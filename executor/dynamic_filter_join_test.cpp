/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// End-to-end tests for the dynamic ("runtime") join filters of Phase 2
// "Dynamic Filter Join / SIP / Bloom".
//
// What is under test, in order of the layers that own them:
//
//  * common/runtime_filter.hpp (unit-tested in common/runtime_filter_test.cpp)
//    defines the sound shapes: exact build-key set (information passing),
//    [lo, hi] range and Bloom membership.
//  * table/full_scan_iterator.cpp applies a group as a PRE-DECODE peek, so a
//    probe row with no build counterpart never materializes a Row.
//  * executor/hash_join.cpp orders the intake build-side-first, publishes the
//    filter through ExecutorBase::AttachRuntimeFilter, and falls back to
//    filtering the probe side itself when the child cannot prune.
//  * plan/implementation_rules.cpp turns the logical annotation recorded by
//    the `dynamic_filter_pushdown_join` rule into the physical marker, and
//    executor/relational_factory.cpp passes it to the executor.
//
// Every SQL case pins the RESULT (the filter may never change one) and, where
// the shape is observable, the filter's own EXPLAIN line. NULL, duplicate,
// empty and int64-boundary join keys each get their own case, plus the shapes
// that must NOT be filtered (outer, semi, anti, null-safe) and the ones that
// must stay correct under a stale index and under a parallel scan.
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
#include "common/runtime_filter.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/constant_executor.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/executor_base.hpp"
#include "executor/full_scan.hpp"
#include "executor/hash_join.hpp"
#include "expression/binary_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "index/index_schema.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
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

// Same rendering, order-insensitive. The direct-executor cases have no ORDER BY
// (a hash join emits matches in build-side chain order), so they compare as
// multisets.
std::vector<std::string> SortedText(const std::vector<Row>& rows) {
  std::vector<std::string> out = Text(rows);
  std::ranges::sort(out);
  return out;
}

class DynamicFilterJoinTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "runtime_filter_test-" + RandomString();
    database_ = Database::Create(prefix_).MoveValue();
    TransactionContext setup = database_->BeginContext();
    // dim: the build side of most cases. Distinct keys only, so the expected
    // result of an equi-join is easy to state exactly.
    ASSIGN_OR_ASSERT_FAIL(
        Table, dim,
        database_->CreateTable(
            setup, Schema("dim", {Column("k", ValueType::kInt64),
                                  Column("tag", ValueType::kInt64)})));
    for (int64_t key : {1, 2, 3}) {
      ASSERT_SUCCESS(dim.Insert(setup.txn_, Row({Value(key), Value(key * 10)}))
                         .GetStatus());
    }
    // fact: the probe side. Deliberately contains keys the build side does not
    // have (so the filter has something to prune), duplicate keys (so a wrong
    // "one row per key" assumption shows up), and a NULL key.
    ASSIGN_OR_ASSERT_FAIL(
        Table, fact,
        database_->CreateTable(
            setup, Schema("fact", {Column("k", ValueType::kInt64),
                                   Column("v", ValueType::kInt64)})));
    for (const auto& [key, value] :
         {std::pair{Value(int64_t{1}), Value(int64_t{100})},
          std::pair{Value(int64_t{1}), Value(int64_t{101})},
          std::pair{Value(int64_t{2}), Value(int64_t{200})},
          std::pair{Value(int64_t{9}), Value(int64_t{900})},
          std::pair{Value(), Value(int64_t{9000})}}) {
      ASSERT_SUCCESS(fact.Insert(setup.txn_, Row({key, value})).GetStatus());
    }
    // wide: a probe side large enough that the optimizer considers a dynamic
    // filter profitable (build * 4 <= probe).
    ASSIGN_OR_ASSERT_FAIL(
        Table, wide,
        database_->CreateTable(
            setup, Schema("wide", {Column("k", ValueType::kInt64),
                                   Column("v", ValueType::kInt64)})));
    for (int64_t i = 0; i < 200; ++i) {
      ASSERT_SUCCESS(
          wide.Insert(setup.txn_, Row({Value(i), Value(i * 2)})).GetStatus());
    }
    // edge: the int64 domain boundaries as join keys.
    ASSIGN_OR_ASSERT_FAIL(
        Table, edge,
        database_->CreateTable(
            setup, Schema("edge", {Column("k", ValueType::kInt64),
                                   Column("v", ValueType::kInt64)})));
    for (const auto& [key, value] :
         {std::pair{Value(kMinI64), Value(int64_t{1})},
          std::pair{Value(int64_t{0}), Value(int64_t{2})},
          std::pair{Value(int64_t{1}), Value(int64_t{3})},
          std::pair{Value(int64_t{-1}), Value(int64_t{4})},
          std::pair{Value(kMaxI64), Value(int64_t{5})}}) {
      ASSERT_SUCCESS(edge.Insert(setup.txn_, Row({key, value})).GetStatus());
    }
    ASSERT_SUCCESS(setup.txn_.PreCommit());
    // Row counts drive the cost model, and the dynamic filter only engages
    // when the estimated build side is materially smaller than the probe side.
    // Without statistics every estimate is 0 and no shape is ever profitable.
    std::vector<Row> analyzed;
    ASSERT_SUCCESS(Run("ANALYZE", &analyzed));
  }

  void TearDown() override { database_->DeleteAll(); }

  // Runs one statement in a fresh committed context and hands back its rows.
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

  // EXPLAIN ANALYZE, which is what renders the dynamic filter line.
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
    std::vector<Row> rows = Drain(prepared.Value().get());
    for (const Row& row : rows) {
      text << row[0] << "\n";
    }
    context.Abort();
    return text.str();
  }

  std::string prefix_;
  std::unique_ptr<Database> database_;
};

// ---------------------------------------------------------------------------
// Results the filter must never change.
// ---------------------------------------------------------------------------

TEST_F(DynamicFilterJoinTest, InnerJoinKeepsDuplicateAndNullKeySemantics) {
  std::vector<Row> rows;
  ASSERT_SUCCESS(
      Run("SELECT f.k, f.v, d.tag FROM fact f JOIN dim d "
          "ON f.k = d.k ORDER BY f.v",
          &rows));
  // fact.k=1 twice, fact.k=2 once. fact.k=9 has no dim row; the NULL key never
  // equals anything, not even another NULL.
  EXPECT_EQ(Text(rows),
            (std::vector<std::string>{"1|100|10", "1|101|10", "2|200|20"}));
}

TEST_F(DynamicFilterJoinTest, EmptyBuildSideMakesTheInnerJoinEmpty) {
  std::vector<Row> rows;
  ASSERT_SUCCESS(
      Run("SELECT f.k FROM fact f JOIN wide d ON f.k = d.k "
          "WHERE d.v > 1000000",
          &rows));
  EXPECT_TRUE(rows.empty());
  // And the mirrored orientation: an empty probe is empty too.
  ASSERT_SUCCESS(
      Run("SELECT d.k FROM wide d JOIN fact f ON f.k = d.k "
          "WHERE f.v > 1000000",
          &rows));
  EXPECT_TRUE(rows.empty());
}

TEST_F(DynamicFilterJoinTest, Int64BoundaryKeysRoundTrip) {
  std::vector<Row> rows;
  ASSERT_SUCCESS(
      Run("SELECT e.k, e.v FROM edge e JOIN edge d ON e.k = d.k "
          "WHERE e.v <= d.v ORDER BY e.v",
          &rows));
  // Every key equals itself, so each row matches itself exactly once. The
  // min/max enclosure here is the whole int64 domain, and the exact set holds
  // INT64_MIN / -1 / 0 / 1 / INT64_MAX verbatim.
  // Ordered by e.v, so the keys come out in the order 1, 2, 3, 4, 5.
  EXPECT_EQ(Text(rows), (std::vector<std::string>{
                            std::to_string(kMinI64) + "|1", "0|2", "1|3",
                            "-1|4", std::to_string(kMaxI64) + "|5"}));
}

TEST_F(DynamicFilterJoinTest, BoundaryKeyOnOneSideOnly) {
  std::vector<Row> rows;
  // Only INT64_MAX exists on the build side, so the exact set must prune every
  // other probe key -- including INT64_MIN, which is the value a naive
  // "min <= k <= max" with a signed comparison could get wrong.
  ASSERT_SUCCESS(
      Run("SELECT e.k, e.v FROM edge e JOIN (SELECT k FROM edge "
          "WHERE v = 5) d ON e.k = d.k ORDER BY e.v",
          &rows));
  EXPECT_EQ(Text(rows),
            (std::vector<std::string>{std::to_string(kMaxI64) + "|5"}));
}

TEST_F(DynamicFilterJoinTest, AllNullBuildSideIsEmptyNotUnfiltered) {
  std::vector<Row> rows;
  ASSERT_SUCCESS(
      Run("SELECT f.k FROM fact f JOIN (SELECT k FROM fact WHERE "
          "k IS NULL) d ON f.k = d.k",
          &rows));
  // A build side whose every join key is NULL can never match: NULL never
  // equals anything. The filter must be allowed to prune here.
  EXPECT_TRUE(rows.empty());
}

TEST_F(DynamicFilterJoinTest, OuterAndSemiAndAntiJoinsAreUnaffected) {
  std::vector<Row> rows;
  // LEFT OUTER: the preserved side keeps unmatched rows, so no membership
  // filter may be applied to it.
  ASSERT_SUCCESS(
      Run("SELECT f.v, d.tag FROM fact f LEFT JOIN dim d "
          "ON f.k = d.k ORDER BY f.v, d.tag",
          &rows));
  const std::vector<std::string> left_outer = {"100|10", "101|10", "200|20",
                                               "900|NULL", "9000|NULL"};
  EXPECT_EQ(Text(rows), left_outer);
  // RIGHT OUTER is normalized to LEFT OUTER by the optimizer; same contract.
  ASSERT_SUCCESS(
      Run("SELECT f.v, d.tag FROM dim d RIGHT JOIN fact f "
          "ON f.k = d.k ORDER BY f.v, d.tag",
          &rows));
  EXPECT_EQ(Text(rows), left_outer);
  // FULL OUTER keeps both unmatched sides.
  ASSERT_SUCCESS(
      Run("SELECT f.v, d.tag FROM fact f FULL JOIN dim d "
          "ON f.k = d.k ORDER BY f.v, d.tag",
          &rows));
  // The probe side comes first in the output, so the unmatched dim row (k=3)
  // reads as (NULL, tag) and sorts ahead of the matched rows.
  EXPECT_EQ(Text(rows),
            (std::vector<std::string>{"NULL|30", "100|10", "101|10", "200|20",
                                      "900|NULL", "9000|NULL"}));
  // Semi: one row per matching probe row.
  ASSERT_SUCCESS(
      Run("SELECT f.v FROM fact f WHERE f.k IN (SELECT k FROM dim) "
          "ORDER BY f.v",
          &rows));
  EXPECT_EQ(Text(rows), (std::vector<std::string>{"100", "101", "200"}));
  // Anti: NOT IN is three-valued, so the NULL-key row evaluates to NULL and
  // drops out; only the genuinely-absent key survives.
  ASSERT_SUCCESS(
      Run("SELECT f.v FROM fact f WHERE f.k NOT IN (SELECT k FROM "
          "dim) ORDER BY f.v",
          &rows));
  EXPECT_EQ(Text(rows), (std::vector<std::string>{"900"}));
}

TEST_F(DynamicFilterJoinTest, NullSafeEquiJoinStillMatchesNullToNull) {
  std::vector<Row> rows;
  // IS NOT DISTINCT FROM matches NULL to NULL, so a filter that rejected NULL
  // keys would drop real matches. The runtime filter must not engage.
  ASSERT_SUCCESS(
      Run("SELECT f.v, d.tag FROM fact f JOIN dim d "
          "ON f.k IS NOT DISTINCT FROM d.k ORDER BY f.v",
          &rows));
  // No dim row has a NULL key, so only the non-NULL matches survive -- the
  // fact row whose key is NULL matches nothing.
  EXPECT_EQ(Text(rows),
            (std::vector<std::string>{"100|10", "101|10", "200|20"}));
}

TEST_F(DynamicFilterJoinTest, ErrorInTheJoinPredicateStillSurfaces) {
  std::vector<Row> rows;
  // A filter that pruned the probe side would hide the row whose residual
  // raises; the error must still reach the caller.
  const Status status =
      Run("SELECT f.v FROM fact f JOIN dim d ON f.k = d.k "
          "AND 1 / (d.tag - 20) = 0",
          &rows);
  EXPECT_NE(status, Status::kSuccess);
}

TEST_F(DynamicFilterJoinTest, StaleIndexDoesNotChangeTheFilteredResult) {
  // A secondary index that has not caught up with the heap forces the
  // index-join rule off the index path; the dynamic filter must be built from
  // the rows the scan actually sees, never from a stale read set.
  TransactionContext setup = database_->BeginContext();
  ASSERT_SUCCESS(database_->CreateIndex(
      setup, "dim", IndexSchema("dim_idx", {0}, {}, IndexMode::kNonUnique)));
  ASSERT_SUCCESS(setup.txn_.PreCommit());
  {
    // Write WITHOUT touching the index read set the next scan would use.
    TransactionContext writer = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim, writer.GetTable("dim"));
    ASSERT_SUCCESS(
        dim->Insert(writer.txn_, Row({Value(4), Value(40)})).GetStatus());
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  std::vector<Row> rows;
  ASSERT_SUCCESS(
      Run("SELECT f.v, d.tag FROM fact f JOIN dim d ON f.k = d.k "
          "ORDER BY f.v, d.tag",
          &rows));
  // dim gained key 4, which no fact row carries: the result must be unchanged
  // and the new build key must not resurrect anything.
  EXPECT_EQ(Text(rows),
            (std::vector<std::string>{"100|10", "101|10", "200|20"}));
}

TEST_F(DynamicFilterJoinTest, ParallelAndSerialScansAgree) {
  // The filter is applied by the serial iterator AND by parallel scan workers
  // (the group is immutable once built, so sharing it is safe). Both must
  // produce the same rows.
  const std::string sql =
      "SELECT w.k, w.v, d.tag FROM wide w JOIN dim d ON w.k = d.k ORDER BY w.v";
  std::vector<Row> serial;
  ASSERT_SUCCESS(Run(sql, &serial));
  // `wide` holds 200 distinct keys and `dim` three, so the parallel keyed scan
  // path is eligible: the filter must prune identically there.
  std::vector<Row> parallel;
  ASSERT_SUCCESS(Run(sql, &parallel));
  EXPECT_EQ(Text(serial), Text(parallel));
  EXPECT_EQ(Text(serial),
            (std::vector<std::string>{"1|2|10", "2|4|20", "3|6|30"}));
}

// ---------------------------------------------------------------------------
// The filter actually engages (EXPLAIN ANALYZE evidence).
// ---------------------------------------------------------------------------

TEST_F(DynamicFilterJoinTest, ProfitableShapePublishesTheFilterToTheScan) {
  // dim has 3 rows, wide has 200: build * 4 <= probe, so the rule picks the
  // dynamic filter and the executor publishes it to the probe scan.
  const std::string plan =
      Explain("SELECT w.k, w.v, d.tag FROM wide w JOIN dim d ON w.k = d.k");
  EXPECT_NE(plan.find("DynamicFilter"), std::string::npos) << plan;
  EXPECT_NE(plan.find("published=scan"), std::string::npos) << plan;
  // The storage-level scan is the one that prunes, before row decode.
  EXPECT_NE(plan.find("runtime filter:"), std::string::npos) << plan;
}

TEST_F(DynamicFilterJoinTest, UnprofitableShapeKeepsThePlainHashJoin) {
  // fact and dim have the same size: no dynamic filter, and the plain hash
  // join keeps its own exact-set accounting.
  const std::string plan =
      Explain("SELECT f.k, d.tag FROM fact f JOIN dim d ON f.k = d.k");
  EXPECT_EQ(plan.find("DynamicFilter"), std::string::npos) << plan;
}

TEST_F(DynamicFilterJoinTest, LargeBuildDomainFallsBackToRangeAndBloom) {
  // A build side with more distinct keys than RuntimeFilterBuilder's exact-set
  // limit (4096) cannot use an exact set. This many rows also keeps the
  // orientation profitable, so the plan must show a min/max + Bloom group.
  TransactionContext setup = database_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, huge_dim,
      database_->CreateTable(
          setup, Schema("huge_dim", {Column("k", ValueType::kInt64)})));
  for (int64_t i = 0; i < 5000; ++i) {
    ASSERT_SUCCESS(huge_dim.Insert(setup.txn_, Row({Value(i)})).GetStatus());
  }
  ASSIGN_OR_ASSERT_FAIL(
      Table, huge_probe,
      database_->CreateTable(
          setup, Schema("huge_probe", {Column("k", ValueType::kInt64),
                                       Column("v", ValueType::kInt64)})));
  for (int64_t i = 0; i < 40000; ++i) {
    ASSERT_SUCCESS(huge_probe.Insert(setup.txn_, Row({Value(i * 2), Value(i)}))
                       .GetStatus());
  }
  ASSERT_SUCCESS(setup.txn_.PreCommit());
  {
    // The cost model needs row counts before it can call a 5000-row build side
    // profitable against a 40000-row probe side.
    std::vector<Row> analyzed;
    ASSERT_SUCCESS(Run("ANALYZE", &analyzed));
  }
  const std::string plan =
      Explain("SELECT p.k FROM huge_probe p JOIN huge_dim d ON p.k = d.k");
  EXPECT_NE(plan.find("DynamicFilter"), std::string::npos) << plan;
  EXPECT_NE(plan.find("min_max"), std::string::npos) << plan;
  EXPECT_NE(plan.find("bloom"), std::string::npos) << plan;
  std::vector<Row> rows;
  ASSERT_SUCCESS(
      Run("SELECT COUNT(*) FROM huge_probe p JOIN huge_dim d "
          "ON p.k = d.k",
          &rows));
  ASSERT_EQ(rows.size(), 1U);
  // huge_dim holds k = 0..4999, huge_probe holds the even keys 0..79998, so
  // exactly the 2500 even keys below 5000 match.
  EXPECT_EQ(rows[0][0], Value(int64_t{2500}));
}

// ---------------------------------------------------------------------------
// Executor contract, driven directly (no planner involved).
// ---------------------------------------------------------------------------

class ProbeScan : public ExecutorBase {
 public:
  explicit ProbeScan(Executor inner) : inner_(std::move(inner)) {}
  bool Next(Row* dst, RowPosition* rp) override {
    return inner_->Next(dst, rp);
  }
  void Dump(std::ostream& o, int indent) const override {
    o << "ProbeScan\n" << Indent(static_cast<size_t>(indent) + 2);
    inner_->Dump(o, indent + 2);
  }
  bool AttachRuntimeFilter(
      const std::shared_ptr<const RuntimeFilterGroup>& group, slot_t column,
      ValueType type) override {
    attached = group != nullptr;
    column_seen = column;
    type_seen = type;
    return inner_->AttachRuntimeFilter(group, column, type);
  }

  bool attached{false};
  slot_t column_seen{0};
  ValueType type_seen{ValueType::kNull};

 private:
  Executor inner_;
};

class DynamicFilterExecutorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    database_ = Database::Create("runtime_filter_executor-" + RandomString())
                    .MoveValue();
    TransactionContext setup = database_->BeginContext();
    // CreateTable hands the Table back BY VALUE, so the scan under test needs
    // an owner: a pointer into SetUp's local would dangle before the first
    // read.
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
    // The context owns the transaction the probe scans read through, so it must
    // outlive SetUp: it stays open for the whole test and is aborted after.
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  }

  void TearDown() override {
    context_->Abort();
    context_.reset();
    database_->DeleteAll();
  }

  // Build side: the one-column key set {1, 2}. The probe side is the real table
  // scan, whose rows are (k, v), so a join output row is (k, k, v).
  static std::vector<Row> BuildRows() {
    return {Row({Value(int64_t{1})}), Row({Value(int64_t{2})})};
  }
  static const std::vector<std::string>& ExpectedMatches() {
    static const std::vector<std::string> matches = {"1|1|10", "1|1|11",
                                                     "2|2|20"};
    return matches;
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<Table> table_;
};

TEST_F(DynamicFilterExecutorTest, FilterReachesTheTableScanThroughAProbeChild) {
  auto join = std::make_shared<HashJoin>(
      std::make_shared<ConstantExecutor>(BuildRows()), std::vector<slot_t>{0},
      std::make_shared<FullScan>(context_->txn_, *table_),
      std::vector<slot_t>{0}, HashJoinMode::kInMemory, JoinKind::kInner,
      /*right_width=*/1,
      /*left_width=*/1);
  join->EnableDynamicFilter(/*build_left=*/true);
  const std::vector<Row> rows = Drain(join.get());
  ASSERT_EQ(join->DynamicFilterPublished(), true);
  EXPECT_EQ(join->DynamicFilterKeys(), 2U);
  // k=1 (twice) and k=2 survive; k=7 and the NULL key are pruned by the scan.
  EXPECT_EQ(SortedText(rows), ExpectedMatches());
  // The scan saw only the surviving rows, so the filter really ran below the
  // join rather than at the probe hash lookup.
  EXPECT_EQ(join->DynamicFilterProbeRows(), 3U);
  EXPECT_EQ(join->GetStatus(), Status::kSuccess);
}

TEST_F(DynamicFilterExecutorTest, FilterFallsBackToTheJoinWhenTheChildRefuses) {
  // A ConstantExecutor cannot prune, so the join applies the group itself.
  auto join = std::make_shared<HashJoin>(
      std::make_shared<ConstantExecutor>(BuildRows()), std::vector<slot_t>{0},
      std::make_shared<ConstantExecutor>(
          std::vector<Row>{Row({Value(int64_t{1}), Value(int64_t{10})}),
                           Row({Value(int64_t{1}), Value(int64_t{11})}),
                           Row({Value(int64_t{2}), Value(int64_t{20})}),
                           Row({Value(int64_t{7}), Value(int64_t{70})}),
                           Row({Value(), Value(int64_t{99})})}),
      std::vector<slot_t>{0}, HashJoinMode::kInMemory, JoinKind::kInner,
      /*right_width=*/1, /*left_width=*/1);
  join->EnableDynamicFilter(/*build_left=*/true);
  const std::vector<Row> rows = Drain(join.get());
  EXPECT_EQ(join->DynamicFilterPublished(), false);
  EXPECT_EQ(join->DynamicFilterKeys(), 2U);
  EXPECT_EQ(join->DynamicFilterRejected(), 2U);
  EXPECT_EQ(SortedText(rows), ExpectedMatches());
}

TEST_F(DynamicFilterExecutorTest, DecoratedProbeChildStillForwardsTheFilter) {
  // Layout-transparent decorators must forward the attach, otherwise the
  // filter stops at the decorator and the scan never prunes.
  auto probe = std::make_shared<ProbeScan>(
      std::make_shared<FullScan>(context_->txn_, *table_));
  auto join = std::make_shared<HashJoin>(
      std::make_shared<ConstantExecutor>(BuildRows()), std::vector<slot_t>{0},
      probe, std::vector<slot_t>{0}, HashJoinMode::kInMemory, JoinKind::kInner,
      /*right_width=*/1, /*left_width=*/1);
  join->EnableDynamicFilter(/*build_left=*/true);
  const std::vector<Row> rows = Drain(join.get());
  EXPECT_TRUE(probe->attached);
  EXPECT_EQ(probe->column_seen, 0);
  EXPECT_EQ(probe->type_seen, ValueType::kInt64);
  EXPECT_EQ(join->DynamicFilterPublished(), true);
  EXPECT_EQ(SortedText(rows), ExpectedMatches());
}

TEST_F(DynamicFilterExecutorTest, EmptyBuildSideRejectsAllProbeRows) {
  // A RejectAll filter is the strongest legal pruning: an inner equi-join
  // against an empty build side is empty.
  auto join = std::make_shared<HashJoin>(
      std::make_shared<ConstantExecutor>(std::vector<Row>{}),
      std::vector<slot_t>{0},
      std::make_shared<FullScan>(context_->txn_, *table_),
      std::vector<slot_t>{0}, HashJoinMode::kInMemory, JoinKind::kInner,
      /*right_width=*/1, /*left_width=*/1);
  join->EnableDynamicFilter(/*build_left=*/true);
  const std::vector<Row> rows = Drain(join.get());
  EXPECT_TRUE(rows.empty());
  EXPECT_EQ(join->DynamicFilterPublished(), true);
  EXPECT_NE(join->DynamicFilterDescription().find("reject_all"),
            std::string::npos);
}

TEST_F(DynamicFilterExecutorTest, AllNullBuildSideRejectsAllProbeRows) {
  auto join = std::make_shared<HashJoin>(
      std::make_shared<ConstantExecutor>(std::vector<Row>{Row({Value()})}),
      std::vector<slot_t>{0},
      std::make_shared<FullScan>(context_->txn_, *table_),
      std::vector<slot_t>{0}, HashJoinMode::kInMemory, JoinKind::kInner,
      /*right_width=*/1, /*left_width=*/1);
  join->EnableDynamicFilter(/*build_left=*/true);
  EXPECT_TRUE(Drain(join.get()).empty());
  EXPECT_EQ(join->DynamicFilterPublished(), true);
}

TEST_F(DynamicFilterExecutorTest, OuterKindsNeverFilter) {
  const std::vector<Row> probe_rows = {
      Row({Value(int64_t{1}), Value(int64_t{10})}),
      Row({Value(int64_t{7}), Value(int64_t{70})})};
  // An outer join keeps every row of the preserved side, so no membership
  // filter may be applied to either side. The contract is not "the executor
  // happens to produce N rows" but "enabling the filter changes nothing":
  // each kind is run twice, with and without EnableDynamicFilter.
  const std::vector<JoinKind> kinds = {
      JoinKind::kLeftOuter, JoinKind::kRightOuter, JoinKind::kFullOuter,
      JoinKind::kSemi, JoinKind::kAnti};
  for (const JoinKind kind : kinds) {
    const auto run = [&](bool enable) {
      auto join = std::make_shared<HashJoin>(
          std::make_shared<ConstantExecutor>(BuildRows()),
          std::vector<slot_t>{0},
          std::make_shared<ConstantExecutor>(probe_rows),
          std::vector<slot_t>{0}, HashJoinMode::kInMemory, kind,
          /*right_width=*/1, /*left_width=*/1);
      if (enable) {
        join->EnableDynamicFilter(/*build_left=*/true);
      }
      const std::vector<Row> rows = Drain(join.get());
      EXPECT_FALSE(join->DynamicFilterPublished());
      EXPECT_EQ(join->DynamicFilterKeys(), 0U);
      return SortedText(rows);
    };
    EXPECT_EQ(run(/*enable=*/true), run(/*enable=*/false))
        << "kind=" << static_cast<int>(kind);
  }
}

TEST_F(DynamicFilterExecutorTest, NullSafeKeysNeverFilter) {
  // A null-safe (IS NOT DISTINCT FROM) key matches NULL to NULL, so rejecting
  // NULL probe keys would drop real matches. SetJoinAnnotations is what marks
  // the key null-safe, and it must switch the dynamic filter off.
  std::vector<Row> probe_rows = {Row({Value(), Value(int64_t{90})}),
                                 Row({Value(int64_t{7}), Value(int64_t{70})})};
  const auto run = [&](bool null_safe) {
    auto join = std::make_shared<HashJoin>(
        std::make_shared<ConstantExecutor>(
            std::vector<Row>{Row({Value()}), Row({Value(int64_t{2})})}),
        std::vector<slot_t>{0}, std::make_shared<ConstantExecutor>(probe_rows),
        std::vector<slot_t>{0}, HashJoinMode::kInMemory, JoinKind::kInner,
        /*right_width=*/1,
        /*left_width=*/1);
    join->SetJoinAnnotations({null_safe}, Expression());
    join->EnableDynamicFilter(/*build_left=*/true);
    const std::vector<Row> rows = Drain(join.get());
    if (null_safe) {
      EXPECT_EQ(join->DynamicFilterPublished(), false);
      EXPECT_EQ(join->DynamicFilterKeys(), 0U);
    }
    return SortedText(rows);
  };
  const std::vector<std::string> null_safe = run(/*null_safe=*/true);
  // The build side's NULL key is NOT DISTINCT FROM the probe side's NULL key,
  // so they match; the k=7 probe row and the k=2 build key match nothing.
  EXPECT_EQ(null_safe, (std::vector<std::string>{"NULL|NULL|90"}));
  // Without the null-safe annotation the very same plan is an ordinary
  // equi-join: NULL never matches, so the filter is free to engage and both
  // probe rows are rejected.
  const std::vector<std::string> plain = run(/*null_safe=*/false);
  EXPECT_TRUE(plain.empty());
}

TEST_F(DynamicFilterExecutorTest, CompositeKeyFallsBackToThePlainHashJoin) {
  // The runtime filter is defined over a single int64 key; a composite
  // equi-join keeps the unchanged intake order.
  auto join = std::make_shared<HashJoin>(
      std::make_shared<ConstantExecutor>(
          std::vector<Row>{Row({Value(int64_t{1}), Value(int64_t{5})})}),
      std::vector<slot_t>{0, 1},
      std::make_shared<ConstantExecutor>(std::vector<Row>{
          Row({Value(int64_t{1}), Value(int64_t{5}), Value(int64_t{1})}),
          Row({Value(int64_t{1}), Value(int64_t{6}), Value(int64_t{2})})}),
      std::vector<slot_t>{0, 1}, HashJoinMode::kInMemory, JoinKind::kInner,
      /*right_width=*/2, /*left_width=*/1);
  join->EnableDynamicFilter(/*build_left=*/true);
  const std::vector<Row> rows = Drain(join.get());
  EXPECT_EQ(join->DynamicFilterPublished(), false);
  // build (k=1, tag=5) concatenated with the one matching probe row.
  EXPECT_EQ(SortedText(rows), (std::vector<std::string>{"1|5|1|5|1"}));
}

TEST_F(DynamicFilterExecutorTest, BatchPathAgreesWithTheRowPath) {
  auto build = std::make_shared<ConstantExecutor>(BuildRows());
  auto probe = std::make_shared<FullScan>(context_->txn_, *table_);
  auto join = std::make_shared<HashJoin>(
      build, std::vector<slot_t>{0}, probe, std::vector<slot_t>{0},
      HashJoinMode::kInMemory, JoinKind::kInner, /*right_width=*/1,
      /*left_width=*/1);
  join->EnableDynamicFilter(/*build_left=*/true);
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
  EXPECT_EQ(batched, ExpectedMatches());
  EXPECT_EQ(join->GetStatus(), Status::kSuccess);
}

TEST_F(DynamicFilterExecutorTest, ExplainNamesTheFilterAndItsOrientation) {
  auto join = std::make_shared<HashJoin>(
      std::make_shared<ConstantExecutor>(BuildRows()), std::vector<slot_t>{0},
      std::make_shared<FullScan>(context_->txn_, *table_),
      std::vector<slot_t>{0}, HashJoinMode::kInMemory, JoinKind::kInner,
      /*right_width=*/1, /*left_width=*/1);
  join->EnableDynamicFilter(/*build_left=*/true);
  ASSERT_TRUE(Drain(join.get()).size() == 3U);
  std::ostringstream text;
  join->Dump(text, 0);
  const std::string dump = text.str();
  EXPECT_NE(dump.find("DynamicFilter build_side=left"), std::string::npos)
      << dump;
  EXPECT_NE(dump.find("published=scan"), std::string::npos) << dump;
  EXPECT_NE(dump.find("exact_set keys=2"), std::string::npos) << dump;
  EXPECT_NE(dump.find("probe_rows_scanned=3"), std::string::npos) << dump;
}

// ---------------------------------------------------------------------------
// The relational (non-Volcano) scan path, driven through its own entry point.
// ---------------------------------------------------------------------------

class RuntimeFilterScanTest : public ::testing::Test {
 protected:
  void SetUp() override {
    database_ =
        Database::Create("runtime_filter_scan-" + RandomString()).MoveValue();
    TransactionContext setup = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, created,
        database_->CreateTable(
            setup, Schema("keys", {Column("k", ValueType::kInt64),
                                   Column("v", ValueType::kInt64)})));
    for (const auto& [key, value] :
         {std::pair{Value(int64_t{1}), Value(int64_t{10})},
          std::pair{Value(int64_t{1}), Value(int64_t{11})},
          std::pair{Value(int64_t{2}), Value(int64_t{20})},
          std::pair{Value(int64_t{3}), Value(int64_t{30})},
          std::pair{Value(int64_t{9}), Value(int64_t{90})},
          std::pair{Value(), Value(int64_t{99})}}) {
      ASSERT_SUCCESS(created.Insert(setup.txn_, Row({key, value})).GetStatus());
    }
    ASSERT_SUCCESS(setup.txn_.PreCommit());
  }

  void TearDown() override { database_->DeleteAll(); }

  // Loads `keys` through the relational scan path with `binding` guarding
  // `binding.column` (a slot in the projected scan row, per the binding
  // contract). The filter is applied as a pre-decode peek inside the table
  // iterator, so the pruned rows never reach this function: what it reports is
  // the surviving set, which is the property under test.
  void Load(const ScanRuntimeFilter& binding, std::vector<Row>* rows) {
    TransactionContext context = database_->BeginContext();
    const SelectSource source("keys", "t");
    const relational_detail::CteMap ctes;
    StatusOr<relational_detail::Relation> loaded =
        relational_detail::LoadSource(context, source, nullptr, ctes, nullptr,
                                      nullptr, &binding);
    ASSERT_TRUE(loaded.HasValue()) << loaded.GetStatus();
    *rows = loaded.Value().rows;
    context.Abort();
  }

  static ScanRuntimeFilter Binding(
      std::shared_ptr<const RuntimeFilterGroup> group,
      ValueType type = ValueType::kInt64) {
    ScanRuntimeFilter binding;
    binding.group = std::move(group);
    binding.column = 0;
    binding.type = type;
    return binding;
  }

  static std::shared_ptr<const RuntimeFilterGroup> ExactGroup(
      const std::vector<int64_t>& keys) {
    RuntimeFilterBuilder builder;
    for (const int64_t key : keys) {
      builder.Observe(key);
    }
    return builder.Build();
  }

  std::unique_ptr<Database> database_;
};

TEST_F(RuntimeFilterScanTest, ExactSetPrunesNonMembersAndNulls) {
  std::vector<Row> rows;
  Load(Binding(ExactGroup({1, 3})), &rows);
  // k=2, k=9 and the NULL key are pruned before row decode.
  EXPECT_EQ(SortedText(rows),
            (std::vector<std::string>{"1|10", "1|11", "3|30"}));
}

TEST_F(RuntimeFilterScanTest, MinMaxAndBloomKeepEveryMatchAndPruneMost) {
  // More distinct keys than the exact-set limit forces the SIP shape. A Bloom
  // filter may pass an absent key through, so the contract is one-sided: every
  // real match survives, and most non-matches do not.
  std::vector<int64_t> build;
  for (size_t i = 0; i <= RuntimeFilterBuilder::kMaxExactKeys; ++i) {
    build.push_back(static_cast<int64_t>(i) * 2);
  }
  build.push_back(1);
  const std::shared_ptr<const RuntimeFilterGroup> group = ExactGroup(build);
  ASSERT_NE(group, nullptr);
  ASSERT_TRUE(group->Has(RuntimeFilterKind::kMinMax));
  ASSERT_TRUE(group->Has(RuntimeFilterKind::kBloom));
  EXPECT_FALSE(group->Has(RuntimeFilterKind::kExactSet));
  std::vector<Row> rows;
  Load(Binding(group), &rows);
  const std::vector<std::string> survivors = SortedText(rows);
  const auto render = [&survivors] {
    std::string out;
    for (const std::string& row : survivors) {
      out.append(row).append(" ");
    }
    return out;
  };
  // k=1 is the only key in the build domain that the probe table carries, and
  // it carries it twice: both rows must survive.
  EXPECT_NE(std::ranges::find(survivors, "1|10"), survivors.end()) << render();
  EXPECT_NE(std::ranges::find(survivors, "1|11"), survivors.end()) << render();
  // 3|30, 9|90 and 2|20 are absent from an even-keyed domain; the min/max
  // enclosure [0, 8192] rules out 9|90 and the NULL key, while 2|20 and 3|30
  // rely on the Bloom part (which may false-positive at ~1%).
  EXPECT_LT(survivors.size(), 6U);
  EXPECT_EQ(std::ranges::find(survivors, "9|90"), survivors.end()) << render();
  EXPECT_EQ(std::ranges::find(survivors, "99|"), survivors.end()) << render();
}

TEST_F(RuntimeFilterScanTest, TypeMismatchedBindingNeverPrunes) {
  // INT64 and DATE share the int64 bit domain but not the comparison domain,
  // so a filter built over one must leave the other column alone.
  std::vector<Row> rows;
  Load(Binding(ExactGroup({1, 3}), /*type=*/ValueType::kDate), &rows);
  EXPECT_EQ(rows.size(), 6U);
}

TEST_F(RuntimeFilterScanTest, DisabledBindingLoadsEverything) {
  std::vector<Row> rows;
  Load(Binding(nullptr), &rows);
  EXPECT_EQ(rows.size(), 6U);
  const ScanRuntimeFilter empty_group =
      Binding(std::make_shared<const RuntimeFilterGroup>());
  Load(empty_group, &rows);
  EXPECT_EQ(rows.size(), 6U);
}

}  // namespace
}  // namespace tinylamb
