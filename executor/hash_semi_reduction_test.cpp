/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// End-to-end tests for Phase 2 "Hash Semi Reduction".
//
// What is under test, in the order the layers own it:
//
//  * plan/cascades.cpp `semi_reduction_pushdown_semi_join` /
//    `..._anti_join` record the probe side's join-key columns on a derived
//    probe group (Group::semi_reduction_probe_keys). Purely logical: which
//    columns may bound the build side, never how the build is shrunk.
//  * plan/implementation_rules.cpp turns that annotation into the physical
//    marker ProductPlan::PreferSemiReduction(), but only for a semi/anti
//    equi-join without a residual whose build side the reduction would
//    actually shrink.
//  * executor/relational_factory.cpp passes the marker to the executor.
//  * executor/hash_join.cpp MaterializeSemiAntiReduced() builds the probe key
//    domain FIRST, drops build rows outside it, keeps one build row per
//    distinct key, and falls back to the unrestricted intake (dedup only)
//    when the domain grows past its cap.
//
// The invariant every case below defends: the reduction changes the hash
// BUILD, never a result. Each executor case therefore runs the identical
// inputs with and without EnableSemiReduction() and compares the rows, and
// each SQL case pins the exact expected output. NULL keys, duplicate keys,
// empty inputs, int64 boundary keys, the outer/mark/single kinds that must
// NOT reduce, a spilling build, a parallel probe scan, unique INTEGER keys
// and ORDER BY are each covered.
#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/constant_executor.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/full_scan.hpp"
#include "executor/hash_join.hpp"
#include "executor/parallel_scan.hpp"
#include "executor/query_memory.hpp"
#include "expression/expression.hpp"
#include "index/index.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/values_plan.hpp"
#include "query/sql_engine.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

constexpr int64_t kMinI64 = std::numeric_limits<int64_t>::min();
constexpr int64_t kMaxI64 = std::numeric_limits<int64_t>::max();

std::vector<Row> Drain(ExecutorBase* executor) {
  std::vector<Row> rows;
  Row row;
  RowPosition position;
  while (executor->Next(&row, &position)) {
    rows.push_back(std::move(row));
    row = Row();
  }
  return rows;
}

// Renders rows as "a|b" strings so a statement can be asserted with a single
// readable comparison. Order preserving: the SQL cases all carry an ORDER BY.
std::vector<std::string> Text(const std::vector<Row>& rows) {
  std::vector<std::string> out;
  out.reserve(rows.size());
  for (const Row& row : rows) {
    std::string text;
    for (size_t i = 0; i < row.values_.size(); ++i) {
      if (i != 0) {
        text.push_back('|');
      }
      std::ostringstream cell;
      cell << row[i];
      text.append(cell.str());
    }
    out.push_back(std::move(text));
  }
  return out;
}

// Order-insensitive rendering. The direct-executor cases have no ORDER BY (a
// hash join emits matches in build-side chain order), so they compare as
// multisets.
std::vector<std::string> SortedText(const std::vector<Row>& rows) {
  std::vector<std::string> out = Text(rows);
  std::ranges::sort(out);
  return out;
}

// RAII: temporarily replaces the process-wide query working-memory budget so a
// case can force the reactive spill path. Always restores the previous limit,
// so a spilled query cannot corrupt later tests.
class ScopedQueryMemory {
 public:
  explicit ScopedQueryMemory(size_t limit)
      : original_(QueryMemoryBudget::Global().Limit()) {
    QueryMemoryBudget::Global().ResetForTest(limit);
  }
  ~ScopedQueryMemory() { QueryMemoryBudget::Global().ResetForTest(original_); }
  ScopedQueryMemory(const ScopedQueryMemory&) = delete;
  ScopedQueryMemory& operator=(const ScopedQueryMemory&) = delete;
  ScopedQueryMemory(ScopedQueryMemory&&) = delete;
  ScopedQueryMemory& operator=(ScopedQueryMemory&&) = delete;

 private:
  size_t original_;
};

// RAII for the semi-reduction domain cap.
class ScopedDomainCap {
 public:
  explicit ScopedDomainCap(size_t keys) { Set(keys); }
  explicit ScopedDomainCap(size_t keys, size_t restore_to)
      : restore_(restore_to) {
    Set(keys);
  }
  ~ScopedDomainCap() {
    if (restore_.has_value()) {
      HashJoin::SetSemiReductionMaxDomainKeysForTest(*restore_);
    }
  }
  ScopedDomainCap(const ScopedDomainCap&) = delete;
  ScopedDomainCap& operator=(const ScopedDomainCap&) = delete;
  ScopedDomainCap(ScopedDomainCap&&) = delete;
  ScopedDomainCap& operator=(ScopedDomainCap&&) = delete;

 private:
  static void Set(size_t keys) {
    HashJoin::SetSemiReductionMaxDomainKeysForTest(keys);
  }
  std::optional<size_t> restore_{};
};

// ---------------------------------------------------------------------------
// SQL path: the results, and the rule/marker that has to appear for them.
// ---------------------------------------------------------------------------

class HashSemiReductionSqlTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "hash_semi_reduction-" + RandomString();
    database_ = Database::Create(prefix_).MoveValue();
    TransactionContext setup = database_->BeginContext();
    // `probe`: the semi-join's outer side. Duplicate keys, a key no build row
    // carries (300, well past `build`'s 0..199), and a NULL key.
    ASSIGN_OR_ASSERT_FAIL(
        Table, probe,
        database_->CreateTable(
            setup, Schema("probe", {Column("k", ValueType::kInt64),
                                    Column("v", ValueType::kInt64)})));
    for (const auto& [key, value] :
         {std::pair{Value(int64_t{1}), Value(int64_t{10})},
          std::pair{Value(int64_t{1}), Value(int64_t{11})},
          std::pair{Value(int64_t{2}), Value(int64_t{20})},
          std::pair{Value(int64_t{300}), Value(int64_t{90})},
          std::pair{Value(), Value(int64_t{99})}}) {
      ASSERT_SUCCESS(probe.Insert(setup.txn_, Row({key, value})).GetStatus());
    }
    // `build`: the huge fact on the BUILD side. Only 0..1 also live in `probe`,
    // so the domain removes ~197 of the 200 rows.
    ASSIGN_OR_ASSERT_FAIL(
        Table, build,
        database_->CreateTable(
            setup, Schema("build", {Column("k", ValueType::kInt64),
                                    Column("pad", ValueType::kInt64)})));
    for (int64_t key = 0; key < 200; ++key) {
      ASSERT_SUCCESS(
          build.Insert(setup.txn_, Row({Value(key), Value(key)})).GetStatus());
    }
    // `build_dup`: a build side whose keys repeat, so the build-key dedup has
    // something to drop.
    ASSIGN_OR_ASSERT_FAIL(
        Table, build_dup,
        database_->CreateTable(
            setup, Schema("build_dup", {Column("k", ValueType::kInt64),
                                        Column("tag", ValueType::kInt64)})));
    for (int64_t key = 0; key < 200; ++key) {
      for (int64_t copy = 0; copy < 3; ++copy) {
        ASSERT_SUCCESS(
            build_dup.Insert(setup.txn_, Row({Value(key), Value(copy)}))
                .GetStatus());
      }
    }
    // `small`: same size as `probe`, so no shape here is profitable.
    ASSIGN_OR_ASSERT_FAIL(
        Table, small,
        database_->CreateTable(
            setup, Schema("small", {Column("k", ValueType::kInt64),
                                    Column("v", ValueType::kInt64)})));
    for (const auto& [key, value] :
         {std::pair{Value(int64_t{1}), Value(int64_t{10})},
          std::pair{Value(int64_t{1}), Value(int64_t{11})},
          std::pair{Value(int64_t{2}), Value(int64_t{20})},
          std::pair{Value(int64_t{300}), Value(int64_t{90})},
          std::pair{Value(), Value(int64_t{99})}}) {
      ASSERT_SUCCESS(small.Insert(setup.txn_, Row({key, value})).GetStatus());
    }
    // `uniq`: a unique INTEGER key on the probe side, the shape where the
    // domain is exactly the probe row count.
    ASSIGN_OR_ASSERT_FAIL(
        Table, uniq,
        database_->CreateTable(
            setup, Schema("uniq", {Column("k", ValueType::kInt64,
                                          Constraint(Constraint::kPrimaryKey)),
                                   Column("v", ValueType::kInt64)})));
    for (int64_t key = 0; key < 4; ++key) {
      ASSERT_SUCCESS(uniq.Insert(setup.txn_, Row({Value(key), Value(key * 3)}))
                         .GetStatus());
    }
    // `edge`: the int64 domain boundaries as join keys, plus a NULL.
    ASSIGN_OR_ASSERT_FAIL(
        Table, edge,
        database_->CreateTable(
            setup, Schema("edge", {Column("k", ValueType::kInt64),
                                   Column("v", ValueType::kInt64)})));
    for (const auto& [key, value] :
         {std::pair{Value(kMinI64), Value(int64_t{1})},
          std::pair{Value(int64_t{0}), Value(int64_t{2})},
          std::pair{Value(kMaxI64), Value(int64_t{3})},
          std::pair{Value(), Value(int64_t{4})}}) {
      ASSERT_SUCCESS(edge.Insert(setup.txn_, Row({key, value})).GetStatus());
    }
    ASSERT_SUCCESS(setup.txn_.PreCommit());
    // Row counts and per-column distinct counts drive the profitability gate;
    // without statistics nothing is ever estimated large enough to reduce.
    std::vector<Row> analyzed;
    ASSERT_SUCCESS(Run("ANALYZE", &analyzed));
  }

  void TearDown() override { database_->DeleteAll(); }

  Status Run(std::string_view sql, std::vector<Row>* rows) {
    TransactionContext context = database_->BeginContext();
    SqlEngine engine(*database_);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    if (!prepared.HasValue()) {
      context.Abort();
      return prepared.GetStatus();
    }
    *rows = Drain(prepared.Value().get());
    if (prepared.Value()->GetStatus() != Status::kSuccess) {
      const Status status = prepared.Value()->GetStatus();
      context.Abort();
      return status;
    }
    return context.PreCommit();
  }

  std::string Explain(std::string_view sql) {
    TransactionContext context = database_->BeginContext();
    SqlEngine engine(*database_);
    StatusOr<Executor> prepared =
        engine.Prepare(context, "EXPLAIN ANALYZE " + std::string(sql));
    std::ostringstream text;
    if (!prepared.HasValue()) {
      context.Abort();
      return "ERROR: " + engine.LastError();
    }
    for (const Row& row : Drain(prepared.Value().get())) {
      text << row[0] << "\n";
    }
    context.Abort();
    return text.str();
  }

  std::string prefix_;
  std::unique_ptr<Database> database_;
};

TEST_F(HashSemiReductionSqlTest, SemiJoinReducesTheHashBuildAndKeepsTheRows) {
  // `probe` is 5 rows and `build` is 200, so the build side shrinks to the
  // distinct probe keys (1, 2, 9) plus nothing else. The semi output is
  // unchanged: k=1 twice, k=2 once; k=9 has no build row and the NULL key never
  // matches.
  std::vector<Row> rows;
  ASSERT_SUCCESS(
      Run("SELECT p.v FROM probe p WHERE p.k IN "
          "(SELECT k FROM build) ORDER BY p.v",
          &rows));
  EXPECT_EQ(Text(rows), (std::vector<std::string>{"10", "11", "20"}));
}

TEST_F(HashSemiReductionSqlTest, ProfitableShapeEngagesTheReduction) {
  const std::string plan =
      Explain("SELECT p.v FROM probe p WHERE p.k IN (SELECT k FROM build)");
  EXPECT_NE(plan.find("SemiReduction"), std::string::npos) << plan;
  EXPECT_NE(plan.find("probe_key_domain"), std::string::npos) << plan;
  // The build side is 200 rows over 200 distinct keys; the probe side carries
  // keys 1, 2 and 300, so 198 build rows can never join.
  EXPECT_NE(plan.find("build_rows=200"), std::string::npos) << plan;
  EXPECT_NE(plan.find("build_rows_kept=2"), std::string::npos) << plan;
  EXPECT_NE(plan.find("build_rows_pruned=198"), std::string::npos) << plan;
}

TEST_F(HashSemiReductionSqlTest, UnprofitableShapeKeepsThePlainSemiJoin) {
  // probe and small hold the same 5 keys, so the build cannot be shrunk: the
  // gate refuses and the plan carries no reduction. Both non-NULL probe keys
  // match, so the result is the plain semi result.
  const std::string plan =
      Explain("SELECT p.v FROM probe p WHERE p.k IN (SELECT k FROM small)");
  EXPECT_EQ(plan.find("SemiReduction"), std::string::npos) << plan;
  std::vector<Row> rows;
  ASSERT_SUCCESS(
      Run("SELECT p.v FROM probe p WHERE p.k IN (SELECT k FROM small) "
          "ORDER BY p.v",
          &rows));
  EXPECT_EQ(Text(rows), (std::vector<std::string>{"10", "11", "20", "90"}));
}

TEST_F(HashSemiReductionSqlTest, DuplicateBuildKeysAreDedupedNotCountedTwice) {
  // build_dup is 600 rows over 200 distinct keys, three copies each. The
  // domain admits only 1 and 2, and the duplicate copies of those two keys are
  // dropped as well.
  std::vector<Row> rows;
  ASSERT_SUCCESS(
      Run("SELECT p.v FROM probe p WHERE p.k IN "
          "(SELECT k FROM build_dup) ORDER BY p.v",
          &rows));
  EXPECT_EQ(Text(rows), (std::vector<std::string>{"10", "11", "20"}));
  const std::string plan =
      Explain("SELECT p.v FROM probe p WHERE p.k IN (SELECT k FROM build_dup)");
  EXPECT_NE(plan.find("SemiReduction"), std::string::npos) << plan;
  EXPECT_NE(plan.find("build_rows=600"), std::string::npos) << plan;
  // key 1 appears 3x and key 2 3x: two survivors and four duplicates.
  EXPECT_NE(plan.find("build_rows_kept=2"), std::string::npos) << plan;
  EXPECT_NE(plan.find("duplicate_keys_pruned=4"), std::string::npos) << plan;
}

TEST_F(HashSemiReductionSqlTest, AntiJoinKeepsTheThreeValuedNotInRules) {
  // NOT IN over a build side with no NULL key: every non-matching probe row
  // survives, and the NULL-key probe row is UNKNOWN so it drops out.
  std::vector<Row> rows;
  ASSERT_SUCCESS(
      Run("SELECT p.v FROM probe p WHERE p.k NOT IN "
          "(SELECT k FROM build) ORDER BY p.v",
          &rows));
  EXPECT_EQ(Text(rows), (std::vector<std::string>{"90"}));
  const std::string plan =
      Explain("SELECT p.v FROM probe p WHERE p.k NOT IN (SELECT k FROM build)");
  EXPECT_NE(plan.find("SemiReduction"), std::string::npos) << plan;
}

TEST_F(HashSemiReductionSqlTest, NotInOverANullBuildKeyStaysUnknown) {
  // A NULL anywhere in the build side makes every non-match UNKNOWN, so the
  // whole result is empty. The reduction must read the RAW build cardinality
  // and the RAW NULL-key flag, not the reduced hash table, or this query would
  // start returning rows.
  std::vector<Row> rows;
  ASSERT_SUCCESS(
      Run("SELECT p.v FROM probe p WHERE p.k NOT IN "
          "(SELECT k FROM edge) ORDER BY p.v",
          &rows));
  EXPECT_TRUE(rows.empty());
}

TEST_F(HashSemiReductionSqlTest, NotExistsAndExistsMatchTheSameRows) {
  std::vector<Row> rows;
  ASSERT_SUCCESS(
      Run("SELECT p.v FROM probe p WHERE EXISTS (SELECT 1 FROM build b "
          "WHERE b.k = p.k) ORDER BY p.v",
          &rows));
  EXPECT_EQ(Text(rows), (std::vector<std::string>{"10", "11", "20"}));
  ASSERT_SUCCESS(
      Run("SELECT p.v FROM probe p WHERE NOT EXISTS (SELECT 1 FROM build b "
          "WHERE b.k = p.k) ORDER BY p.v",
          &rows));
  // NOT EXISTS keeps the NULL-key probe row: it matches nothing, so it is not
  // the "at least one match" case either.
  EXPECT_EQ(Text(rows), (std::vector<std::string>{"90", "99"}));
  // A CORRELATED EXISTS stays on the relational IR route, which owns its own
  // anti join; the reduction is a hash-build contract and does not reach there.
  // The uncorrelated IN / NOT IN forms above do go through the semi/anti hash
  // join and are covered by the other cases.
}

TEST_F(HashSemiReductionSqlTest, EmptyInputsKeepTheAntiAndSemiShortcuts) {
  // An empty probe side is empty for every kind.
  std::vector<Row> rows;
  ASSERT_SUCCESS(
      Run("SELECT p.v FROM probe p WHERE p.k IN "
          "(SELECT k FROM probe WHERE v < 0) ORDER BY p.v",
          &rows));
  EXPECT_TRUE(rows.empty());
  // An empty BUILD side: the semi join matches nothing, and the anti join
  // emits every probe row INCLUDING the NULL-key one -- `x NOT IN {}` is TRUE
  // for every x, even a NULL x. The gate declines the reduction here (there is
  // no build to shrink), so this is the plain empty-build shortcut.
  ASSERT_SUCCESS(
      Run("SELECT p.v FROM probe p WHERE p.k NOT IN "
          "(SELECT k FROM build WHERE k < 0) ORDER BY p.v",
          &rows));
  EXPECT_EQ(Text(rows),
            (std::vector<std::string>{"10", "11", "20", "90", "99"}));
  const std::string plan = Explain(
      "SELECT p.v FROM probe p WHERE p.k NOT IN "
      "(SELECT k FROM build WHERE k < 0)");
  EXPECT_EQ(plan.find("SemiReduction"), std::string::npos) << plan;
  // An empty PROBE side is the one shape where the reduction has maximal value
  // and the executor reports it explicitly; see the executor-level
  // `EmptyProbeSideEmptiesTheBuild` for the `empty_probe_domain` status.
}

TEST_F(HashSemiReductionSqlTest, Int64BoundaryKeysRoundTrip) {
  // `edge` carries INT64_MIN, 0, INT64_MAX and a NULL; the domain is built over
  // the same memcomparable encoding the hash index uses, so the boundaries are
  // ordinary keys and the NULL adds no domain member.
  std::vector<Row> rows;
  ASSERT_SUCCESS(
      Run("SELECT e.v FROM edge e WHERE e.k IN (SELECT k FROM edge) "
          "ORDER BY e.v",
          &rows));
  EXPECT_EQ(Text(rows), (std::vector<std::string>{"1", "2", "3"}));
  ASSERT_SUCCESS(
      Run("SELECT e.v FROM edge e WHERE e.k NOT IN (SELECT k FROM edge) "
          "ORDER BY e.v",
          &rows));
  EXPECT_TRUE(rows.empty());
}

TEST_F(HashSemiReductionSqlTest, UniqueIntegerProbeKeyReducesToItsOwnRows) {
  // `uniq.k` is a PRIMARY KEY, so the probe key domain is exactly the probe
  // rows: nothing to dedup, and the reduction prunes the build purely.
  std::vector<Row> rows;
  ASSERT_SUCCESS(
      Run("SELECT u.v FROM uniq u WHERE u.k IN "
          "(SELECT k FROM build) ORDER BY u.v",
          &rows));
  EXPECT_EQ(Text(rows), (std::vector<std::string>{"0", "3", "6", "9"}));
  const std::string plan =
      Explain("SELECT u.v FROM uniq u WHERE u.k IN (SELECT k FROM build)");
  EXPECT_NE(plan.find("SemiReduction"), std::string::npos) << plan;
  EXPECT_NE(plan.find("probe_keys=4"), std::string::npos) << plan;
  EXPECT_NE(plan.find("build_rows_kept=4"), std::string::npos) << plan;
  EXPECT_NE(plan.find("build_rows_pruned=196"), std::string::npos) << plan;
}

TEST_F(HashSemiReductionSqlTest, OuterJoinsNeverReduce) {
  // The preserved side keeps its unmatched rows, so the build cannot be cut
  // down to the probe domain: a build row outside the domain is exactly a row
  // the NULL padding has to account for. The gate excludes every outer kind.
  const std::string left =
      "SELECT p.v, b.k FROM probe p LEFT JOIN build b ON p.k = b.k "
      "ORDER BY p.v, b.k";
  const std::string plan = Explain(left);
  EXPECT_EQ(plan.find("SemiReduction"), std::string::npos) << plan;
  std::vector<Row> rows;
  ASSERT_SUCCESS(Run(left, &rows));
  // p.v=10 and 11 match build k=1, p.v=20 matches k=2, p.v=90 has no build row
  // and p.v=99 carries a NULL key that matches nothing.
  EXPECT_EQ(Text(rows), (std::vector<std::string>{"10|1", "11|1", "20|2",
                                                  "90|NULL", "99|NULL"}));
  // RIGHT OUTER is normalized to LEFT OUTER; FULL OUTER keeps both sides.
  const std::string full =
      "SELECT p.v, b.k FROM probe p FULL JOIN build b ON p.k = b.k "
      "ORDER BY p.v, b.k";
  EXPECT_EQ(Explain(full).find("SemiReduction"), std::string::npos) << full;
  ASSERT_SUCCESS(Run(full, &rows));
  // Three matched pairs (probe k=1 twice against build k=1, probe k=2 against
  // build k=2), two unmatched probe rows, and 198 build rows the probe never
  // matches -- all of which the reduction would have destroyed.
  EXPECT_EQ(rows.size(), 3U + 2U + 198U);
}

TEST_F(HashSemiReductionSqlTest,
       FilteredSubqueryKeepsItsConjunctThroughTheJoin) {
  // A subquery filter lands in a Selection BELOW the semi join (the
  // decorrelation pushes single-side conjuncts under the join), so the join
  // itself carries no residual and the reduction may engage: dropping build
  // rows the probe cannot match is still sound for an existence test. What
  // must not happen is the filter being lost.
  const std::string sql =
      "SELECT p.v FROM probe p WHERE p.k IN (SELECT k FROM build b "
      "WHERE b.pad >= 0) ORDER BY p.v";
  const std::string plan = Explain(sql);
  // The filter lives in a Selection BELOW the semi join, so it is never traded
  // away for the reduction.
  EXPECT_NE(plan.find("pad"), std::string::npos) << plan;
  EXPECT_NE(plan.find("SemiReduction"), std::string::npos) << plan;
  std::vector<Row> rows;
  ASSERT_SUCCESS(Run(sql, &rows));
  EXPECT_EQ(Text(rows), (std::vector<std::string>{"10", "11", "20"}));
  // And a filter that removes every build row empties the semi join.
  ASSERT_SUCCESS(
      Run("SELECT p.v FROM probe p WHERE p.k IN (SELECT k FROM build b "
          "WHERE b.pad < 0) ORDER BY p.v",
          &rows));
  EXPECT_TRUE(rows.empty());
}

TEST_F(HashSemiReductionSqlTest, LargeSemiAndAntiJoinKeepTheirCounts) {
  // A probe side big enough for the morsel-driven parallel scan, against a
  // BUILD side even larger. `fat` holds i % 2000 for i in 0..19999 (2000
  // distinct keys, 10 copies each) and `vast` holds the 50000 distinct keys
  // 1000..50999, so exactly half of fat's keys are live.
  //
  // At this size the memo selects the relational IR route, which owns its own
  // join construction; the hash semi reduction is a Cascades/decorrelation
  // physical contract and does not reach there (see
  // `ParallelProbeScanAgreesWithTheSerialOne` for the reduced shape over a real
  // ParallelScan probe). What this case pins is that the large shape keeps the
  // exact semi / anti counts either way.
  TransactionContext setup = database_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, fat,
      database_->CreateTable(setup,
                             Schema("fat", {Column("k", ValueType::kInt64),
                                            Column("v", ValueType::kInt64)})));
  for (int64_t i = 0; i < 20000; ++i) {
    ASSERT_SUCCESS(
        fat.Insert(setup.txn_, Row({Value(i % 2000), Value(i)})).GetStatus());
  }
  ASSIGN_OR_ASSERT_FAIL(
      Table, vast,
      database_->CreateTable(setup,
                             Schema("vast", {Column("k", ValueType::kInt64)})));
  for (int64_t i = 1000; i < 51000; ++i) {
    ASSERT_SUCCESS(vast.Insert(setup.txn_, Row({Value(i)})).GetStatus());
  }
  ASSERT_SUCCESS(setup.txn_.PreCommit());
  {
    std::vector<Row> analyzed;
    ASSERT_SUCCESS(Run("ANALYZE", &analyzed));
  }
  std::vector<Row> rows;
  ASSERT_SUCCESS(Run(
      "SELECT COUNT(*) FROM fat f WHERE f.k IN (SELECT k FROM vast)", &rows));
  ASSERT_EQ(rows.size(), 1U);
  // fat's k = 0..1999, vast's k = 1000..50999: the 1000 keys in [1000, 1999]
  // match, 10 copies each.
  EXPECT_EQ(rows[0][0], Value(int64_t{10000}));
  ASSERT_SUCCESS(
      Run("SELECT COUNT(*) FROM fat f WHERE f.k NOT IN (SELECT k FROM vast)",
          &rows));
  ASSERT_EQ(rows.size(), 1U);
  // The other 1000 keys match nothing, so the anti join emits them.
  EXPECT_EQ(rows[0][0], Value(int64_t{10000}));
}

TEST_F(HashSemiReductionSqlTest, ExplainShowsTheRuntimeReductionCounters) {
  // EXPLAIN ANALYZE renders the executor tree, so the reduction's own line --
  // its status and its counters -- is what a plan capture can assert on.
  const std::string plan =
      Explain("SELECT p.v FROM probe p WHERE p.k IN (SELECT k FROM build)");
  EXPECT_NE(plan.find("SemiHashJoin"), std::string::npos) << plan;
  EXPECT_NE(
      plan.find("SemiReduction probe_key_domain probe_keys=3"),  // 1, 2, 300
      std::string::npos)
      << plan;
  EXPECT_NE(plan.find("build_rows_pruned=198"), std::string::npos) << plan;
}

// ---------------------------------------------------------------------------
// Direct executor: the reduction must never change a row, for ANY join kind.
// ---------------------------------------------------------------------------

class HashSemiReductionExecutorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    database_ = Database::Create("hash_semi_reduction_exec-" + RandomString())
                    .MoveValue();
    TransactionContext setup = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, created,
        database_->CreateTable(
            setup, Schema("probe", {Column("k", ValueType::kInt64),
                                    Column("v", ValueType::kInt64)})));
    table_ = std::make_unique<Table>(std::move(created));
    for (const auto& [key, value] :
         {std::pair{Value(int64_t{1}), Value(int64_t{10})},
          std::pair{Value(int64_t{1}), Value(int64_t{11})},
          std::pair{Value(int64_t{2}), Value(int64_t{20})},
          std::pair{Value(int64_t{7}), Value(int64_t{70})},
          std::pair{Value(), Value(int64_t{99})}}) {
      ASSERT_SUCCESS(table_->Insert(setup.txn_, Row({key, value})).GetStatus());
    }
    ASSERT_SUCCESS(setup.txn_.PreCommit());
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  }

  void TearDown() override {
    context_->Abort();
    context_.reset();
    database_->DeleteAll();
  }

  // Build side: the one-column key set {1, 2}. The probe side is a two-column
  // (k, v) relation, so a match row is the probe row alone for semi/anti and
  // (build k, probe k, probe v) for the row-emitting kinds.
  static std::vector<Row> BuildRows() {
    return {Row({Value(int64_t{1})}), Row({Value(int64_t{2})})};
  }
  static std::vector<Row> ProbeRows() {
    return {Row({Value(int64_t{1}), Value(int64_t{10})}),
            Row({Value(int64_t{1}), Value(int64_t{11})}),
            Row({Value(int64_t{2}), Value(int64_t{20})}),
            Row({Value(int64_t{7}), Value(int64_t{70})}),
            Row({Value(), Value(int64_t{99})})};
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<Table> table_;
};

TEST_F(HashSemiReductionExecutorTest, EveryJoinKindProducesTheSameRows) {
  // The contract is not "the reduced path happens to produce N rows" but
  // "enabling the reduction changes nothing": each kind runs twice, with and
  // without EnableSemiReduction(), and the two row multisets must be equal.
  // Existence-only kinds activate the reduction; every other kind must ignore
  // the marker entirely.
  const std::vector<JoinKind> kinds = {
      JoinKind::kInner,         JoinKind::kSemi,      JoinKind::kAnti,
      JoinKind::kNullAwareAnti, JoinKind::kLeftOuter, JoinKind::kRightOuter,
      JoinKind::kFullOuter,     JoinKind::kMark,      JoinKind::kSingle};
  for (const JoinKind kind : kinds) {
    // Existence-only kinds (semi / anti / null-aware anti) activate the
    // reduction; every other kind must ignore the marker entirely.
    const bool existence_only = kind == JoinKind::kSemi ||
                                kind == JoinKind::kAnti ||
                                kind == JoinKind::kNullAwareAnti;
    const auto run = [&](bool enable) {
      auto join = std::make_shared<HashJoin>(
          std::make_shared<ConstantExecutor>(ProbeRows()),
          std::vector<slot_t>{0},
          std::make_shared<ConstantExecutor>(BuildRows()),
          std::vector<slot_t>{0}, HashJoinMode::kInMemory, kind,
          /*right_width=*/1, /*left_width=*/2);
      if (enable) {
        join->EnableSemiReduction();
      }
      const std::vector<Row> rows = Drain(join.get());
      EXPECT_EQ(join->GetStatus(), Status::kSuccess)
          << "kind=" << static_cast<int>(kind);
      EXPECT_EQ(join->SemiReductionActive(), enable && existence_only)
          << "kind=" << static_cast<int>(kind);
      return rows;
    };
    const std::vector<Row> enabled = run(/*enable=*/true);
    const std::vector<Row> plain = run(/*enable=*/false);
    EXPECT_EQ(SortedText(enabled), SortedText(plain))
        << "kind=" << static_cast<int>(kind);
  }
}

TEST_F(HashSemiReductionExecutorTest, SemiJoinReducesTheBuildSide) {
  // Probe keys are {1, 1, 2, 7, NULL}: three distinct, one of which (7) no
  // build row carries. Both build rows survive and no duplicate is dropped.
  auto join = std::make_shared<HashJoin>(
      std::make_shared<ConstantExecutor>(ProbeRows()), std::vector<slot_t>{0},
      std::make_shared<ConstantExecutor>(BuildRows()), std::vector<slot_t>{0},
      HashJoinMode::kInMemory, JoinKind::kSemi, /*right_width=*/1,
      /*left_width=*/2);
  join->EnableSemiReduction();
  EXPECT_EQ(SortedText(Drain(join.get())),
            (std::vector<std::string>{"1|10", "1|11", "2|20"}));
  EXPECT_TRUE(join->SemiReductionActive());
  EXPECT_EQ(join->SemiReductionDescription(), "probe_key_domain");
  EXPECT_EQ(join->SemiReductionProbeKeys(), 3U);
  EXPECT_EQ(join->SemiReductionBuildRows(), 2U);
  EXPECT_EQ(join->SemiReductionBuildRowsKept(), 2U);
  EXPECT_EQ(join->SemiReductionBuildRowsPruned(), 0U);
  EXPECT_EQ(join->SemiReductionDuplicateKeysPruned(), 0U);
}

TEST_F(HashSemiReductionExecutorTest, DuplicateAndUnmatchedBuildKeysArePruned) {
  // A build side with 200 distinct keys plus three copies of each. The probe
  // domain is {1, 2, 7}, so 197 keys are out of domain and the 3 surviving keys
  // keep one row each (six duplicates dropped).
  std::vector<Row> build;
  for (int64_t key = 0; key < 200; ++key) {
    for (int64_t copy = 0; copy < 3; ++copy) {
      build.push_back(Row({Value(key)}));
    }
  }
  auto join = std::make_shared<HashJoin>(
      std::make_shared<ConstantExecutor>(ProbeRows()), std::vector<slot_t>{0},
      std::make_shared<ConstantExecutor>(std::move(build)),
      std::vector<slot_t>{0}, HashJoinMode::kInMemory, JoinKind::kSemi,
      /*right_width=*/1, /*left_width=*/2);
  join->EnableSemiReduction();
  EXPECT_EQ(SortedText(Drain(join.get())),
            (std::vector<std::string>{"1|10", "1|11", "2|20", "7|70"}));
  EXPECT_EQ(join->SemiReductionProbeKeys(), 3U);
  EXPECT_EQ(join->SemiReductionBuildRows(), 600U);
  EXPECT_EQ(join->SemiReductionBuildRowsKept(), 3U);
  EXPECT_EQ(join->SemiReductionBuildRowsPruned(), 597U);
  EXPECT_EQ(join->SemiReductionDuplicateKeysPruned(), 6U);
}

TEST_F(HashSemiReductionExecutorTest, NullSafeKeysMatchNullToNull) {
  // IS NOT DISTINCT FROM makes a NULL probe key join a NULL build key, so the
  // domain must encode NULL as the same presence marker the hash index uses
  // instead of rejecting it. The plan rule does not set the marker on a
  // null-safe key, but the executor is gated on the KIND, so driving it
  // directly here proves the encoding is right rather than the flag.
  std::vector<Row> probe = {Row({Value(), Value(int64_t{90})}),
                            Row({Value(int64_t{7}), Value(int64_t{70})})};
  const auto run = [&](bool enable) {
    auto join = std::make_shared<HashJoin>(
        std::make_shared<ConstantExecutor>(probe), std::vector<slot_t>{0},
        std::make_shared<ConstantExecutor>(
            std::vector<Row>{Row({Value()}), Row({Value(int64_t{2})})}),
        std::vector<slot_t>{0}, HashJoinMode::kInMemory, JoinKind::kSemi,
        /*right_width=*/1, /*left_width=*/2);
    join->SetJoinAnnotations({true}, Expression());
    if (enable) {
      join->EnableSemiReduction();
    }
    return Drain(join.get());
  };
  const std::vector<std::string> enabled = SortedText(run(/*enable=*/true));
  const std::vector<std::string> plain = SortedText(run(/*enable=*/false));
  EXPECT_EQ(enabled, (std::vector<std::string>{"NULL|90"}));
  EXPECT_EQ(enabled, plain);
}

TEST_F(HashSemiReductionExecutorTest, CompositeKeysAreDomainRestrictedToo) {
  // Two key columns: the domain is over the composite memcomparable encoding,
  // so a build row that matches on the first column only still gets pruned.
  const std::vector<Row> probe = {
      Row({Value(int64_t{1}), Value(int64_t{5}), Value(int64_t{10})})};
  const std::vector<Row> build = {Row({Value(int64_t{1}), Value(int64_t{5})}),
                                  Row({Value(int64_t{1}), Value(int64_t{6})}),
                                  Row({Value(int64_t{2}), Value(int64_t{5})})};
  auto join = std::make_shared<HashJoin>(
      std::make_shared<ConstantExecutor>(probe), std::vector<slot_t>{0, 1},
      std::make_shared<ConstantExecutor>(build), std::vector<slot_t>{0, 1},
      HashJoinMode::kInMemory, JoinKind::kSemi, /*right_width=*/2,
      /*left_width=*/3);
  join->EnableSemiReduction();
  EXPECT_EQ(SortedText(Drain(join.get())),
            (std::vector<std::string>{"1|5|10"}));
  EXPECT_EQ(join->SemiReductionBuildRows(), 3U);
  EXPECT_EQ(join->SemiReductionBuildRowsKept(), 1U);
  EXPECT_EQ(join->SemiReductionBuildRowsPruned(), 2U);
}

TEST_F(HashSemiReductionExecutorTest, EmptyProbeSideEmptiesTheBuild) {
  // An empty probe side has an empty key domain, so every build row is pruned
  // and the semi result is empty. Anti keeps its "empty build side" shortcut
  // reading the RAW cardinality, so the probe rows all survive.
  const std::vector<Row> empty;
  for (const auto& [kind, expected] :
       {std::pair{JoinKind::kSemi, std::vector<std::string>{}},
        std::pair{JoinKind::kAnti, std::vector<std::string>{}}}) {
    auto join = std::make_shared<HashJoin>(
        std::make_shared<ConstantExecutor>(empty), std::vector<slot_t>{0},
        std::make_shared<ConstantExecutor>(BuildRows()), std::vector<slot_t>{0},
        HashJoinMode::kInMemory, kind, /*right_width=*/1, /*left_width=*/2);
    join->EnableSemiReduction();
    EXPECT_EQ(SortedText(Drain(join.get())), expected)
        << "kind=" << static_cast<int>(kind);
    EXPECT_EQ(join->SemiReductionDescription(), "empty_probe_domain");
    EXPECT_EQ(join->SemiReductionBuildRows(), 2U);
    EXPECT_EQ(join->SemiReductionBuildRowsKept(), 0U);
  }
}

TEST_F(HashSemiReductionExecutorTest, EmptyBuildSideEmitsEveryProbeRowForAnti) {
  // `x NOT IN {}` is TRUE for every x, including a NULL x, so the anti join
  // emits all five probe rows -- the reduction reads the RAW build cardinality
  // (0) even though it kept no build row at all.
  auto join = std::make_shared<HashJoin>(
      std::make_shared<ConstantExecutor>(ProbeRows()), std::vector<slot_t>{0},
      std::make_shared<ConstantExecutor>(std::vector<Row>{}),
      std::vector<slot_t>{0}, HashJoinMode::kInMemory, JoinKind::kAnti,
      /*right_width=*/1, /*left_width=*/2);
  join->EnableSemiReduction();
  EXPECT_EQ(
      SortedText(Drain(join.get())),
      (std::vector<std::string>{"1|10", "1|11", "2|20", "7|70", "NULL|99"}));
  EXPECT_EQ(join->SemiReductionBuildRows(), 0U);
}

TEST_F(HashSemiReductionExecutorTest, NotInOverANullBuildKeyEmitsNothing) {
  // A NULL anywhere in the build side makes every non-match UNKNOWN, so the
  // null-aware anti join emits nothing. Both the reduced and the plain run must
  // agree, which is only true if the NULL flag is read off the raw build side.
  const auto run = [&](bool enable) {
    auto join = std::make_shared<HashJoin>(
        std::make_shared<ConstantExecutor>(ProbeRows()), std::vector<slot_t>{0},
        std::make_shared<ConstantExecutor>(
            std::vector<Row>{Row({Value()}), Row({Value(int64_t{1})})}),
        std::vector<slot_t>{0}, HashJoinMode::kInMemory,
        JoinKind::kNullAwareAnti, /*right_width=*/1, /*left_width=*/2);
    if (enable) {
      join->EnableSemiReduction();
    }
    return Drain(join.get());
  };
  EXPECT_TRUE(run(/*enable=*/true).empty());
  EXPECT_TRUE(run(/*enable=*/false).empty());
}

TEST_F(HashSemiReductionExecutorTest, DomainCapFallsBackWithoutChangingRows) {
  // The domain cap is a run-time guard: a probe side whose own key domain is
  // huge would cost more memory than the build rows it could prune, so the
  // executor drops the domain and keeps only the (unconditionally sound) build
  // key dedup. Lowering the cap to 2 forces the fallback on five probe rows.
  const size_t restore = size_t{1} << 20;
  {
    ScopedDomainCap cap(2, restore);
    std::vector<Row> build = BuildRows();
    build.push_back(Row({Value(int64_t{1})}));
    build.push_back(Row({Value(int64_t{2})}));
    build.push_back(Row({Value(int64_t{9})}));
    auto join = std::make_shared<HashJoin>(
        std::make_shared<ConstantExecutor>(ProbeRows()), std::vector<slot_t>{0},
        std::make_shared<ConstantExecutor>(std::move(build)),
        std::vector<slot_t>{0}, HashJoinMode::kInMemory, JoinKind::kSemi,
        /*right_width=*/1, /*left_width=*/2);
    join->EnableSemiReduction();
    EXPECT_EQ(SortedText(Drain(join.get())),
              (std::vector<std::string>{"1|10", "1|11", "2|20"}));
    EXPECT_EQ(join->SemiReductionDescription(), "fallback=domain_cap");
    EXPECT_EQ(join->SemiReductionProbeKeys(), 0U);
    // Every build row is still seen; only the two duplicate keys were dropped.
    EXPECT_EQ(join->SemiReductionBuildRows(), 5U);
    EXPECT_EQ(join->SemiReductionBuildRowsKept(), 3U);
    EXPECT_EQ(join->SemiReductionDuplicateKeysPruned(), 2U);
  }
  // With the cap restored the very same inputs take the domain path.
  std::vector<Row> build = BuildRows();
  build.push_back(Row({Value(int64_t{1})}));
  build.push_back(Row({Value(int64_t{2})}));
  build.push_back(Row({Value(int64_t{9})}));
  auto join = std::make_shared<HashJoin>(
      std::make_shared<ConstantExecutor>(ProbeRows()), std::vector<slot_t>{0},
      std::make_shared<ConstantExecutor>(std::move(build)),
      std::vector<slot_t>{0}, HashJoinMode::kInMemory, JoinKind::kSemi,
      /*right_width=*/1, /*left_width=*/2);
  join->EnableSemiReduction();
  EXPECT_EQ(SortedText(Drain(join.get())),
            (std::vector<std::string>{"1|10", "1|11", "2|20"}));
  EXPECT_EQ(join->SemiReductionDescription(), "probe_key_domain");
  EXPECT_EQ(restore, size_t{1} << 20);
}

TEST_F(HashSemiReductionExecutorTest, SpilledBuildSideAgreesWithThePlainPath) {
  // A 64 KiB budget over 400 wide build rows forces the reactive spill, so the
  // reduction has to partition and re-read its reduced build exactly like the
  // plain path does. Build keys 0..399, probe keys {1, 1, 2, 7, NULL}: only 1,
  // 2 and 7 survive the domain.
  std::vector<Row> probe = {Row({Value(int64_t{1}), Value(int64_t{10})}),
                            Row({Value(int64_t{1}), Value(int64_t{11})}),
                            Row({Value(int64_t{2}), Value(int64_t{20})}),
                            Row({Value(int64_t{7}), Value(int64_t{70})}),
                            Row({Value(), Value(int64_t{99})})};
  std::vector<Row> build;
  for (int64_t key = 0; key < 400; ++key) {
    std::string payload = "row-" + std::to_string(key);
    payload.resize(96, 'x');
    build.push_back(
        Row({Value(key), Value(std::string(payload)), Value(int64_t{7})}));
  }
  const auto run = [&](bool enable) {
    ScopedQueryMemory budget(static_cast<size_t>(64) * 1024);
    auto join = std::make_shared<HashJoin>(
        std::make_shared<ConstantExecutor>(probe), std::vector<slot_t>{0},
        std::make_shared<ConstantExecutor>(build), std::vector<slot_t>{0},
        HashJoinMode::kInMemory, JoinKind::kSemi, /*right_width=*/3,
        /*left_width=*/2);
    if (enable) {
      join->EnableSemiReduction();
    }
    const std::vector<Row> rows = Drain(join.get());
    EXPECT_EQ(join->GetStatus(), Status::kSuccess);
    return rows;
  };
  const std::vector<std::string> enabled = SortedText(run(/*enable=*/true));
  EXPECT_EQ(enabled,
            (std::vector<std::string>{"1|10", "1|11", "2|20", "7|70"}));
  EXPECT_EQ(enabled, SortedText(run(/*enable=*/false)));
}

TEST_F(HashSemiReductionExecutorTest, SpilledProbeSideAgreesWithThePlainPath) {
  // The reverse shape: a wide PROBE side forces the probe itself to spill, so
  // the domain has to be built by reading every probe partition back (the
  // domain must be complete before a single build row is judged) and the probe
  // partitions are then re-read for the emission phase. Build keys 0..49, probe
  // keys i % 7 for i in 0..199.
  std::vector<Row> probe;
  for (int64_t i = 0; i < 200; ++i) {
    std::string payload = "row-" + std::to_string(i);
    payload.resize(96, 'x');
    probe.push_back(Row({Value(i % 7), Value(std::string(payload)), Value(i)}));
  }
  std::vector<Row> build;
  build.reserve(50);
  for (int64_t key = 0; key < 50; ++key) {
    build.push_back(Row({Value(key)}));
  }
  // 8 KiB (soft limit 6.5 KiB) against a ~24 KiB probe side pushes every probe
  // row into the spill files, so `s.left.rows` is empty and the domain can only
  // be built by reading the partitions back.
  const auto run = [&](bool enable) {
    ScopedQueryMemory budget(static_cast<size_t>(8) * 1024);
    auto join = std::make_shared<HashJoin>(
        std::make_shared<ConstantExecutor>(probe), std::vector<slot_t>{0},
        std::make_shared<ConstantExecutor>(build), std::vector<slot_t>{0},
        HashJoinMode::kInMemory, JoinKind::kSemi, /*right_width=*/1,
        /*left_width=*/3);
    if (enable) {
      join->EnableSemiReduction();
    }
    const std::vector<Row> rows = Drain(join.get());
    EXPECT_EQ(join->GetStatus(), Status::kSuccess);
    return rows;
  };
  // Every probe key is in 0..6 and the build covers 0..49, so all 200 probe
  // rows match and the semi join is a pass-through. A domain built from only
  // SOME probe partitions would silently drop rows here.
  const std::vector<std::string> enabled = SortedText(run(/*enable=*/true));
  EXPECT_EQ(enabled.size(), 200U);
  EXPECT_EQ(enabled, SortedText(run(/*enable=*/false)));
}

TEST_F(HashSemiReductionExecutorTest, ParallelProbeScanAgreesWithTheSerialOne) {
  // A table wide enough for the morsel-driven ParallelScan on the probe side.
  // The reduction consumes the probe child exactly once, so the parallel
  // consumer must produce the same rows as the serial FullScan.
  std::unique_ptr<Table> fat;
  {
    TransactionContext setup = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, created,
        database_->CreateTable(
            setup, Schema("fat", {Column("k", ValueType::kInt64),
                                  Column("v", ValueType::kInt64)})));
    fat = std::make_unique<Table>(std::move(created));
    for (int64_t i = 0; i < 20000; ++i) {
      ASSERT_SUCCESS(
          fat->Insert(setup.txn_, Row({Value(i % 400), Value(i)})).GetStatus());
    }
    ASSERT_SUCCESS(setup.txn_.PreCommit());
  }
  // A context opened AFTER the commit: the set-up context predates the table
  // and its snapshot cannot see these rows.
  TransactionContext fresh = database_->BeginContext();
  std::vector<Row> build;
  build.reserve(400);
  for (int64_t key = 0; key < 400; ++key) {
    build.push_back(Row({Value(key)}));
  }
  const auto run = [&](bool parallel, bool enable) {
    Executor probe =
        parallel ? std::static_pointer_cast<ExecutorBase>(
                       std::make_shared<ParallelScan>(fresh.txn_, *fat, 4, 8))
                 : std::static_pointer_cast<ExecutorBase>(
                       std::make_shared<FullScan>(fresh.txn_, *fat));
    auto join = std::make_shared<HashJoin>(
        std::move(probe), std::vector<slot_t>{0},
        std::make_shared<ConstantExecutor>(build), std::vector<slot_t>{0},
        HashJoinMode::kInMemory, JoinKind::kSemi, /*right_width=*/1,
        /*left_width=*/2);
    if (enable) {
      join->EnableSemiReduction();
    }
    const std::vector<Row> rows = Drain(join.get());
    EXPECT_EQ(join->GetStatus(), Status::kSuccess);
    return rows;
  };
  // `build` holds k = 0..399 and `fat` carries 50 copies of each, so all
  // 20000 rows match: the semi join is a pass-through here, which is exactly
  // the case a domain that dropped a live key would break.
  const std::vector<std::string> serial =
      SortedText(run(/*parallel=*/false, /*enable=*/false));
  EXPECT_EQ(serial.size(), 20000U);
  EXPECT_EQ(SortedText(run(/*parallel=*/false, /*enable=*/true)), serial);
  EXPECT_EQ(SortedText(run(/*parallel=*/true, /*enable=*/false)), serial);
  EXPECT_EQ(SortedText(run(/*parallel=*/true, /*enable=*/true)), serial);
}

TEST_F(HashSemiReductionExecutorTest, RowPositionsSurviveTheReduction) {
  // A semi/anti join is the DML path's row source: its output row positions are
  // the PROBE side's, so UPDATE/DELETE can address the original row. The
  // reduction drops build rows, never probe rows, so the positions must be the
  // untouched probe ones -- three distinct positions for the three matching
  // rows of `probe`, read through a real FullScan.
  auto join = std::make_shared<HashJoin>(
      std::make_shared<FullScan>(context_->txn_, *table_),
      std::vector<slot_t>{0}, std::make_shared<ConstantExecutor>(BuildRows()),
      std::vector<slot_t>{0}, HashJoinMode::kInMemory, JoinKind::kSemi,
      /*right_width=*/1,
      /*left_width=*/2);
  join->EnableSemiReduction();
  std::vector<RowPosition> positions;
  Row row;
  RowPosition position;
  while (join->Next(&row, &position)) {
    positions.push_back(position);
    row = Row();
  }
  ASSERT_EQ(positions.size(), 3U);
  // Three distinct physical rows: the reduction drops build rows, so it can
  // neither collapse two probe rows into one nor invent a position.
  for (size_t i = 1; i < positions.size(); ++i) {
    EXPECT_TRUE(positions[i].page_id != positions[i - 1].page_id ||
                positions[i].slot != positions[i - 1].slot)
        << "row " << i;
  }
  // The same three rows a plain (unreduced) semi join over the same probe
  // scan emits.
  auto plain = std::make_shared<HashJoin>(
      std::make_shared<FullScan>(context_->txn_, *table_),
      std::vector<slot_t>{0}, std::make_shared<ConstantExecutor>(BuildRows()),
      std::vector<slot_t>{0}, HashJoinMode::kInMemory, JoinKind::kSemi,
      /*right_width=*/1,
      /*left_width=*/2);
  std::vector<RowPosition> plain_positions;
  while (plain->Next(&row, &position)) {
    plain_positions.push_back(position);
    row = Row();
  }
  EXPECT_EQ(positions, plain_positions);
}

TEST_F(HashSemiReductionExecutorTest, BatchPathAgreesWithTheRowPath) {
  // NextBatch re-reads the same materialized output, so the batched and the
  // row-at-a-time consumers must see the identical reduced result.
  auto join = std::make_shared<HashJoin>(
      std::make_shared<ConstantExecutor>(ProbeRows()), std::vector<slot_t>{0},
      std::make_shared<ConstantExecutor>(BuildRows()), std::vector<slot_t>{0},
      HashJoinMode::kInMemory, JoinKind::kSemi, /*right_width=*/1,
      /*left_width=*/2);
  join->EnableSemiReduction();
  DataChunk chunk;
  std::vector<std::string> batched;
  while (join->NextBatch(&chunk) != 0) {
    for (size_t i = 0; i < chunk.Size(); ++i) {
      std::string text;
      for (size_t c = 0; c < chunk.ColumnCount(); ++c) {
        if (c != 0) {
          text.push_back('|');
        }
        std::ostringstream cell;
        cell << chunk.ColumnAt(c).ValueAt(i);
        text.append(cell.str());
      }
      batched.push_back(std::move(text));
    }
  }
  std::ranges::sort(batched);
  EXPECT_EQ(batched, (std::vector<std::string>{"1|10", "1|11", "2|20"}));
  EXPECT_EQ(join->GetStatus(), Status::kSuccess);
  EXPECT_EQ(join->SemiReductionBuildRowsKept(), 2U);
}

TEST_F(HashSemiReductionExecutorTest, ExplainNamesTheReductionAndItsCounters) {
  auto join = std::make_shared<HashJoin>(
      std::make_shared<ConstantExecutor>(ProbeRows()), std::vector<slot_t>{0},
      std::make_shared<ConstantExecutor>(BuildRows()), std::vector<slot_t>{0},
      HashJoinMode::kInMemory, JoinKind::kSemi, /*right_width=*/1,
      /*left_width=*/2);
  join->EnableSemiReduction();
  ASSERT_EQ(Drain(join.get()).size(), 3U);
  std::ostringstream text;
  join->Dump(text, 0);
  const std::string dump = text.str();
  EXPECT_NE(dump.find("SemiHashJoin"), std::string::npos) << dump;
  EXPECT_NE(dump.find("SemiReduction probe_key_domain probe_keys=3"),
            std::string::npos)
      << dump;
  EXPECT_NE(dump.find("build_rows=2 build_rows_kept=2 build_rows_pruned=0"),
            std::string::npos)
      << dump;
  EXPECT_NE(dump.find("duplicate_keys_pruned=0"), std::string::npos) << dump;
}

// ---------------------------------------------------------------------------
// Plan layer: the profitability gate and the physical marker.
// ---------------------------------------------------------------------------

TEST(SemiReductionGateTest, KeptRowsFollowsTheProbeKeyDomain) {
  // Without statistics the probe row count is the honest ceiling: its distinct
  // keys cannot exceed its rows.
  EXPECT_DOUBLE_EQ(SemiReductionKeptRows(/*probe_rows=*/10, /*build_rows=*/100,
                                         /*probe_domain=*/0,
                                         /*build_domain=*/0),
                   10);
  // With statistics the build is scaled by the fraction of its key domain the
  // probe covers.
  EXPECT_DOUBLE_EQ(SemiReductionKeptRows(1000, 1000, 100, 500), 200);
  // A probe domain that covers the WHOLE build domain prunes nothing: every
  // build row is expected to survive, so the reduction has no value.
  EXPECT_DOUBLE_EQ(SemiReductionKeptRows(1000, 1000, 900, 100), 1000);
  // The estimate is a row count, never more than the build side itself.
  EXPECT_DOUBLE_EQ(SemiReductionKeptRows(10, 5, 10, 5), 5);
}

TEST(SemiReductionGateTest, OnlyAShrinkOfAMateriallyLargerBuildPays) {
  // A probe side that is not smaller than the build cannot shrink it.
  EXPECT_FALSE(SemiReductionIsProfitable(/*probe_rows=*/100, /*build_rows=*/100,
                                         3, 100));
  EXPECT_FALSE(SemiReductionIsProfitable(0, 100, 0, 100));
  // An empty build side has nothing to shrink.
  EXPECT_FALSE(SemiReductionIsProfitable(5, 0, 3, 0));
  // Halving is not enough on its own: the absolute floor keeps tiny inputs on
  // the plain pipeline, where the bookkeeping would not pay for itself.
  EXPECT_FALSE(SemiReductionIsProfitable(40, 60, 30, 60));
  // 5 probe rows over a 200-row build: 197 rows go away, so it pays.
  EXPECT_TRUE(SemiReductionIsProfitable(5, 200, 3, 200));
  // The probe domain covers the whole build key domain, so nothing is pruned.
  EXPECT_FALSE(SemiReductionIsProfitable(5, 200, 200, 200));
}

TEST(SemiReductionGateTest, MissingStatisticsOnlyMakeTheEstimateCoarser) {
  const Plan left = std::make_shared<ValuesPlan>(
      Schema("l", {Column("k", ValueType::kInt64)}),
      std::vector<Row>{Row({Value(int64_t{1})})});
  const Plan right = std::make_shared<ValuesPlan>(
      Schema("r", {Column("k", ValueType::kInt64)}),
      std::vector<Row>{Row({Value(int64_t{1})})});
  const std::vector<ColumnName> key = {ColumnName("l", "k")};
  // A one-row probe never dominates a one-row build, whatever the statistics.
  EXPECT_FALSE(SemiReductionProfitable(left, {ColumnName("l", "k")}, right,
                                       {ColumnName("r", "k")}));
  // A null plan is a hard no rather than a guess.
  EXPECT_FALSE(SemiReductionProfitable(left, key, nullptr, key));
  // And the per-key domain estimate is 0 for an unknown column, which the
  // profitability test treats as "no information", never as "infinite".
  EXPECT_DOUBLE_EQ(JoinKeyDomainEstimate(left, ColumnName("l", "k")), 0.0);
  EXPECT_DOUBLE_EQ(JoinKeyDomainEstimate(left, ColumnName("l", "nope")), 0.0);
  EXPECT_DOUBLE_EQ(JoinKeyDomainEstimate(nullptr, key), 0.0);
}

TEST(SemiReductionGateTest, MarkerIsOffByDefault) {
  // A hand-built ProductPlan never has the marker unless a rule sets it, so the
  // executor's plain path is what every other caller gets.
  const Plan left = std::make_shared<ValuesPlan>(
      Schema("l", {Column("k", ValueType::kInt64)}),
      std::vector<Row>{Row({Value(int64_t{1})})});
  const Plan right = std::make_shared<ValuesPlan>(
      Schema("r", {Column("k", ValueType::kInt64)}),
      std::vector<Row>{Row({Value(int64_t{1})})});
  const auto semi = std::make_shared<ProductPlan>(
      left, std::vector<ColumnName>{ColumnName("l", "k")}, right,
      std::vector<ColumnName>{ColumnName("r", "k")}, SemiJoinKind());
  EXPECT_FALSE(semi->PrefersSemiReduction());
  EXPECT_EQ(semi->ToString().find("semi_reduction"), std::string::npos);
  semi->PreferSemiReduction();
  EXPECT_TRUE(semi->PrefersSemiReduction());
  EXPECT_NE(semi->ToString().find("semi_reduction"), std::string::npos);
  // The dynamic filter marker is independent: setting one never sets the other.
  const auto inner = std::make_shared<ProductPlan>(
      left, std::vector<ColumnName>{ColumnName("l", "k")}, right,
      std::vector<ColumnName>{ColumnName("r", "k")}, HashJoinMode::kInMemory,
      std::nullopt);
  inner->PreferSemiReduction();
  EXPECT_TRUE(inner->PrefersSemiReduction());
  EXPECT_FALSE(inner->PrefersDynamicFilter());
  inner->PreferDynamicFilter(/*build_left=*/true);
  EXPECT_TRUE(inner->PrefersDynamicFilter());
  EXPECT_TRUE(inner->PrefersSemiReduction());
}

}  // namespace
}  // namespace tinylamb
