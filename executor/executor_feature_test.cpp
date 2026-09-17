/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/join_kind.hpp"
#include "common/set_operation.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/data_chunk.hpp"
#include "executor/exchange.hpp"
#include "executor/executor_base.hpp"
#include "executor/hash_join.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/merge_join.hpp"
#include "executor/parallel_hash_join.hpp"
#include "executor/parallel_merge_join.hpp"
#include "executor/query_memory.hpp"
#include "executor/set_operation.hpp"
#include "executor/simd_comparison.hpp"
#include "executor/skip_scan_distinct.hpp"
#include "executor/vectorized_expression.hpp"
#include "executor/values.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/unary_expression.hpp"
#include "index/index.hpp"
#include "index/index_schema.hpp"
#include "page/row_position.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

// Executes one statement and returns the materialized rows.  A prepare or
// execution failure surfaces as a runtime_error carrying the diagnostic.
std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    throw std::runtime_error(engine->LastError());
  }
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
    row = Row();
  }
  const Status st = prepared.Value()->GetStatus();
  if (st != Status::kSuccess) {
    throw std::runtime_error(st.GetMessage());
  }
  return rows;
}

// Drains a directly-constructed executor row by row.
std::vector<Row> Drain(ExecutorBase* executor) {
  std::vector<Row> rows;
  Row row;
  RowPosition rp;
  while (executor->Next(&row, &rp)) {
    rows.push_back(row);
    row = Row();
  }
  return rows;
}

Executor Src(std::vector<Row> rows) {
  return std::make_shared<ValuesExecutor>(std::move(rows));
}

Row IRow(int64_t v) { return Row({Value(v)}); }

std::vector<Row> SequenceRows(int64_t begin, int64_t end) {
  std::vector<Row> rows;
  rows.reserve(static_cast<size_t>(end - begin));
  for (int64_t v = begin; v < end; ++v) {
    rows.push_back(IRow(v));
  }
  return rows;
}

// Every row of `expected` is matched exactly once against `actual`.
void ExpectSameMultiset(const std::vector<Row>& actual,
                        const std::vector<Row>& expected) {
  std::vector<Row> remaining = expected;
  for (const Row& row : actual) {
    bool found = false;
    for (size_t i = 0; i < remaining.size(); ++i) {
      if (remaining[i] == row) {
        remaining.erase(remaining.begin() + static_cast<long>(i));
        found = true;
        break;
      }
    }
    EXPECT_TRUE(found);
  }
  EXPECT_TRUE(remaining.empty());
}

// RAII shrink of the global query-memory budget; restores the previous limit
// on scope exit.  Used to force reactive-spill / hybrid-join paths.
class BudgetGuard {
 public:
  explicit BudgetGuard(size_t limit_bytes) {
    previous_ = QueryMemoryBudget::Global().Limit();
    QueryMemoryBudget::Global().ResetForTest(limit_bytes);
  }
  ~BudgetGuard() { QueryMemoryBudget::Global().ResetForTest(previous_); }
  BudgetGuard(const BudgetGuard&) = delete;
  BudgetGuard& operator=(const BudgetGuard&) = delete;

 private:
  size_t previous_{0};
};

// A child executor that yields nothing and latches a failure, for sticky
// error propagation tests.
class FailingSource : public ExecutorBase {
 public:
  bool Next(Row* /*dst*/, RowPosition* /*rp*/) override { return false; }
  void Dump(std::ostream& o, int /*indent*/) const override {
    o << "FailingSource";
  }
  [[nodiscard]] Status GetStatus() const override {
    return StatusError(StatusCode::kRuntimeError, "failing source");
  }
};

// Sorted rows with a single INT64 key plus a payload, each key repeated
// `dup` times.  Merge-join inputs must arrive ordered.
std::vector<Row> SortedKeyRows(int64_t begin, int64_t end, int64_t dup) {
  std::vector<Row> rows;
  for (int64_t k = begin; k < end; ++k) {
    for (int64_t d = 0; d < dup; ++d) {
      rows.push_back(Row({Value(k), Value(k * 10 + d)}));
    }
  }
  return rows;
}

// Deterministic ordering for result-set comparison.  Only for rows without
// NULL values (Value ordering against NULL throws by design).
void SortRowsByValue(std::vector<Row>* rows) {
  std::sort(rows->begin(), rows->end(), [](const Row& a, const Row& b) {
    const size_t n = std::min(a.values_.size(), b.values_.size());
    for (size_t i = 0; i < n; ++i) {
      if (a[i] == b[i]) {
        continue;
      }
      return a[i] < b[i];
    }
    return a.values_.size() < b.values_.size();
  });
}

}  // namespace

// ===========================================================================
// SQL-level coverage: set operations and join shapes through SqlEngine.
// ===========================================================================

class ExecutorFeatureSqlTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "SQL frontend unavailable";
    }
    database_ = Database::Create("executor_feature_test").MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  std::vector<Row> Exec(const std::string& sql) {
    return RunSql(engine_.get(), context_.get(), sql);
  }
  void Run(const std::string& sql) { (void)Exec(sql); }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(ExecutorFeatureSqlTest, Smoke) {
  const auto rows = Exec("SELECT 1");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
}

TEST_F(ExecutorFeatureSqlTest, SetOperationsDistinctAndAll) {
  Run("CREATE TABLE s1(x INT64)");
  Run("CREATE TABLE s2(x INT64)");
  Run("INSERT INTO s1 VALUES (1), (2), (2), (3)");
  Run("INSERT INTO s2 VALUES (2), (3), (5)");

  const auto union_rows =
      Exec("SELECT x FROM s1 UNION DISTINCT SELECT x FROM s2 ORDER BY x");
  ASSERT_EQ(union_rows.size(), 4U);
  EXPECT_EQ(union_rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(union_rows[1][0], Value(int64_t{2}));
  EXPECT_EQ(union_rows[2][0], Value(int64_t{3}));
  EXPECT_EQ(union_rows[3][0], Value(int64_t{5}));

  const auto union_all =
      Exec("SELECT x FROM s1 UNION ALL SELECT x FROM s2 ORDER BY x");
  ASSERT_EQ(union_all.size(), 7U);

  const auto intersect =
      Exec("SELECT x FROM s1 INTERSECT DISTINCT SELECT x FROM s2 ORDER BY x");
  ASSERT_EQ(intersect.size(), 2U);
  EXPECT_EQ(intersect[0][0], Value(int64_t{2}));
  EXPECT_EQ(intersect[1][0], Value(int64_t{3}));

  // INTERSECT ALL keeps min(left, right) multiplicity: 2 appears twice on
  // the left but once on the right.
  const auto intersect_all =
      Exec("SELECT x FROM s1 INTERSECT ALL SELECT x FROM s2 ORDER BY x");
  ASSERT_EQ(intersect_all.size(), 2U);
  EXPECT_EQ(intersect_all[0][0], Value(int64_t{2}));
  EXPECT_EQ(intersect_all[1][0], Value(int64_t{3}));

  const auto except =
      Exec("SELECT x FROM s1 EXCEPT DISTINCT SELECT x FROM s2 ORDER BY x");
  ASSERT_EQ(except.size(), 1U);
  EXPECT_EQ(except[0][0], Value(int64_t{1}));

  // EXCEPT ALL consumes one right-hand copy per left occurrence: the
  // duplicate 2 on the left survives once.
  const auto except_all =
      Exec("SELECT x FROM s1 EXCEPT ALL SELECT x FROM s2 ORDER BY x");
  ASSERT_EQ(except_all.size(), 2U);
  EXPECT_EQ(except_all[0][0], Value(int64_t{1}));
  EXPECT_EQ(except_all[1][0], Value(int64_t{2}));
}

TEST_F(ExecutorFeatureSqlTest, SetOperationsWithNulls) {
  Run("CREATE TABLE n1(x INT64)");
  Run("CREATE TABLE n2(x INT64)");
  Run("INSERT INTO n1 VALUES (1), (NULL), (NULL)");
  Run("INSERT INTO n2 VALUES (1), (NULL)");

  // UNION deduplicates NULLs to a single row.
  const auto union_rows =
      Exec("SELECT x FROM n1 UNION DISTINCT SELECT x FROM n2 ORDER BY x");
  ASSERT_EQ(union_rows.size(), 2U);
  EXPECT_TRUE(union_rows[0][0].IsNull());
  EXPECT_EQ(union_rows[1][0], Value(int64_t{1}));

  // INTERSECT: NULL matches NULL in set operations.
  const auto intersect =
      Exec("SELECT x FROM n1 INTERSECT DISTINCT SELECT x FROM n2");
  ASSERT_EQ(intersect.size(), 2U);

  // EXCEPT removes the matching NULL too.
  const auto except =
      Exec("SELECT x FROM n1 EXCEPT DISTINCT SELECT x FROM n2");
  EXPECT_TRUE(except.empty());

  // EXCEPT ALL: left has two NULLs, right one -> one NULL survives.
  const auto except_all =
      Exec("SELECT x FROM n1 EXCEPT ALL SELECT x FROM n2");
  ASSERT_EQ(except_all.size(), 1U);
  EXPECT_TRUE(except_all[0][0].IsNull());
}

TEST_F(ExecutorFeatureSqlTest, SetOperationTypeCoercion) {
  Run("CREATE TABLE ci(x INT64)");
  Run("CREATE TABLE cd(x DOUBLE)");
  Run("INSERT INTO ci VALUES (1), (2)");
  Run("INSERT INTO cd VALUES (2.5), (0.5)");
  const auto mixed =
      Exec("SELECT x FROM ci UNION DISTINCT SELECT x FROM cd ORDER BY x");
  ASSERT_EQ(mixed.size(), 4U);
  EXPECT_EQ(mixed[0][0].type, ValueType::kDouble);
  EXPECT_EQ(mixed[0][0], Value(0.5));
  EXPECT_EQ(mixed[1][0], Value(1.0));
  EXPECT_EQ(mixed[2][0], Value(2.0));
  EXPECT_EQ(mixed[3][0], Value(2.5));

  // INT64 2 and DOUBLE 2.0 dedup to one row under UNION DISTINCT.
  Run("INSERT INTO cd VALUES (2.0)");
  const auto dedup = Exec("SELECT x FROM ci UNION DISTINCT SELECT x FROM cd");
  EXPECT_EQ(dedup.size(), 4U);
}

TEST_F(ExecutorFeatureSqlTest, MultiSourceUnionAllChains) {
  const auto rows =
      Exec("SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY v");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(rows[1][0], Value(int64_t{2}));
  EXPECT_EQ(rows[2][0], Value(int64_t{3}));
}

TEST_F(ExecutorFeatureSqlTest, OuterJoinShapesWithNullAndDuplicateKeys) {
  Run("CREATE TABLE l(a INT64, lv VARCHAR)");
  Run("CREATE TABLE r(b INT64, rv VARCHAR)");
  Run("INSERT INTO l VALUES (1, 'l1'), (1, 'l1b'), (2, 'l2'), (NULL, 'ln')");
  Run("INSERT INTO r VALUES (1, 'r1'), (1, 'r1b'), (3, 'r3'), (NULL, 'rn')");

  const auto inner =
      Exec("SELECT l.a, lv, b, rv FROM l JOIN r ON l.a = r.b ORDER BY lv, rv");
  ASSERT_EQ(inner.size(), 4U);  // 2 x 2 matches on key 1

  const auto left =
      Exec("SELECT l.a, lv, b, rv FROM l LEFT JOIN r ON l.a = r.b ORDER BY lv");
  ASSERT_EQ(left.size(), 6U);  // 4 matches + l2 + the NULL-keyed row
  // The NULL-keyed left row survives with a NULL-padded right side.
  EXPECT_EQ(left[5][1], Value("ln"));
  EXPECT_TRUE(left[5][2].IsNull());

  const auto right = Exec(
      "SELECT l.a, lv, b, rv FROM l RIGHT JOIN r ON l.a = r.b ORDER BY rv");
  ASSERT_EQ(right.size(), 6U);
  EXPECT_EQ(right[5][3], Value("rn"));
  EXPECT_TRUE(right[5][0].IsNull());

  const auto full = Exec(
      "SELECT l.a, lv, b, rv FROM l FULL JOIN r ON l.a = r.b ORDER BY lv, rv");
  ASSERT_EQ(full.size(), 8U);  // 4 matches + 2 unmatched per side
}

TEST_F(ExecutorFeatureSqlTest, JoinKeysOfEveryType) {
  Run("CREATE TABLE sl(s VARCHAR, v INT64)");
  Run("CREATE TABLE sr(s VARCHAR, w INT64)");
  Run("INSERT INTO sl VALUES ('a', 1), ('b', 2), ('', 3)");
  Run("INSERT INTO sr VALUES ('a', 10), ('', 30), ('c', 40)");
  const auto string_join =
      Exec("SELECT sl.v, sr.w FROM sl JOIN sr ON sl.s = sr.s ORDER BY sl.v");
  ASSERT_EQ(string_join.size(), 2U);
  EXPECT_EQ(string_join[0][0], Value(int64_t{1}));
  EXPECT_EQ(string_join[0][1], Value(int64_t{10}));
  EXPECT_EQ(string_join[1][0], Value(int64_t{3}));
  EXPECT_EQ(string_join[1][1], Value(int64_t{30}));

  Run("CREATE TABLE dl(d DOUBLE, v INT64)");
  Run("CREATE TABLE dr(d DOUBLE, w INT64)");
  Run("INSERT INTO dl VALUES (-0.0, 1), (1.5, 2)");
  Run("INSERT INTO dr VALUES (0.0, 10), (1.5, 20)");
  // -0.0 and 0.0 are the same key in SQL equality.
  const auto double_join =
      Exec("SELECT dl.v, dr.w FROM dl JOIN dr ON dl.d = dr.d ORDER BY dl.v");
  ASSERT_EQ(double_join.size(), 2U);
  EXPECT_EQ(double_join[0][1], Value(int64_t{10}));
  EXPECT_EQ(double_join[1][1], Value(int64_t{20}));

  Run("CREATE TABLE tl(d DATE, v INT64)");
  Run("CREATE TABLE tr(d DATE, w INT64)");
  Run("INSERT INTO tl VALUES (DATE '2020-01-01', 1), (DATE '2021-06-15', 2)");
  Run(
      "INSERT INTO tr VALUES (DATE '2020-01-01', 10), (DATE '1999-12-31', 20)");
  const auto date_join =
      Exec("SELECT tl.v, tr.w FROM tl JOIN tr ON tl.d = tr.d ORDER BY tl.v");
  ASSERT_EQ(date_join.size(), 1U);
  EXPECT_EQ(date_join[0][1], Value(int64_t{10}));
}

TEST_F(ExecutorFeatureSqlTest, MultiKeyJoinAndNullSafeEquiJoin) {
  Run("CREATE TABLE mk1(a INT64, b INT64, v VARCHAR)");
  Run("CREATE TABLE mk2(a INT64, b INT64, w VARCHAR)");
  Run("INSERT INTO mk1 VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z')");
  Run("INSERT INTO mk2 VALUES (1, 1, 'p'), (1, 2, 'q'), (2, 2, 'r')");
  const auto multi = Exec(
      "SELECT mk1.v, mk2.w FROM mk1 JOIN mk2 ON mk1.a = mk2.a AND mk1.b = "
      "mk2.b ORDER BY mk1.v");
  ASSERT_EQ(multi.size(), 2U);
  EXPECT_EQ(multi[0][0], Value("x"));
  EXPECT_EQ(multi[0][1], Value("p"));
  EXPECT_EQ(multi[1][0], Value("y"));
  EXPECT_EQ(multi[1][1], Value("q"));

  Run("CREATE TABLE ns1(a INT64)");
  Run("CREATE TABLE ns2(b INT64)");
  Run("INSERT INTO ns1 VALUES (1), (NULL), (2)");
  Run("INSERT INTO ns2 VALUES (NULL), (2), (3)");
  const auto null_safe = Exec(
      "SELECT ns1.a, ns2.b FROM ns1 JOIN ns2 ON ns1.a IS NOT DISTINCT FROM "
      "ns2.b ORDER BY ns1.a");
  // NULL matches NULL; 2 matches 2; unmatched (1) and (3) drop.
  ASSERT_EQ(null_safe.size(), 2U);
  EXPECT_TRUE(null_safe[0][0].IsNull());
  EXPECT_TRUE(null_safe[0][1].IsNull());
  EXPECT_EQ(null_safe[1][0], Value(int64_t{2}));
}

TEST_F(ExecutorFeatureSqlTest, ScalarSubquerySingleJoin) {
  Run("CREATE TABLE s1(a INT64)");
  Run("CREATE TABLE s2(b INT64)");
  Run("INSERT INTO s1 VALUES (1), (2), (3)");
  Run("INSERT INTO s2 VALUES (2)");
  const auto rows = Exec(
      "SELECT s1.a, (SELECT s2.b FROM s2 WHERE s2.b = s1.a) FROM s1 ORDER BY "
      "s1.a");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_TRUE(rows[0][1].IsNull());
  EXPECT_EQ(rows[1][1], Value(int64_t{2}));
  EXPECT_TRUE(rows[2][1].IsNull());

  // A scalar subquery matching more than one row is a runtime error.
  Run("INSERT INTO s2 VALUES (2)");
  EXPECT_THROW(
      (void)Exec("SELECT s1.a, (SELECT s2.b FROM s2 WHERE s2.b = s1.a) FROM "
                 "s1"),
      std::runtime_error);
}

TEST_F(ExecutorFeatureSqlTest, SemiAntiAndNullAwareAntiSubqueries) {
  Run("CREATE TABLE e1(a INT64)");
  Run("CREATE TABLE e2(b INT64)");
  Run("INSERT INTO e1 VALUES (1), (2), (3), (NULL)");
  Run("INSERT INTO e2 VALUES (2), (NULL)");

  const auto semi = Exec("SELECT a FROM e1 WHERE a IN (SELECT b FROM e2)");
  ASSERT_EQ(semi.size(), 1U);
  EXPECT_EQ(semi[0][0], Value(int64_t{2}));

  // NOT EXISTS: NULL probe keys match nothing, so they survive.  NULLs sort
  // first under the default ascending ordering.
  const auto anti = Exec(
      "SELECT a FROM e1 AS e1x WHERE NOT EXISTS (SELECT 1 FROM e2 WHERE e2.b "
      "= e1x.a) ORDER BY a");
  ASSERT_EQ(anti.size(), 3U);
  EXPECT_TRUE(anti[0][0].IsNull());
  EXPECT_EQ(anti[1][0], Value(int64_t{1}));
  EXPECT_EQ(anti[2][0], Value(int64_t{3}));

  // NOT IN with a NULL in the set is UNKNOWN for non-matching rows: nothing
  // survives.
  const auto not_in_null =
      Exec("SELECT a FROM e1 WHERE a NOT IN (SELECT b FROM e2)");
  EXPECT_TRUE(not_in_null.empty());

  Run("CREATE TABLE e3(b INT64)");
  Run("INSERT INTO e3 VALUES (2)");
  const auto not_in = Exec(
      "SELECT a FROM e1 WHERE a NOT IN (SELECT b FROM e3) ORDER BY a");
  ASSERT_EQ(not_in.size(), 2U);
  EXPECT_EQ(not_in[0][0], Value(int64_t{1}));
  EXPECT_EQ(not_in[1][0], Value(int64_t{3}));
}

TEST_F(ExecutorFeatureSqlTest, EmptySideJoins) {
  Run("CREATE TABLE empty_t(a INT64)");
  Run("CREATE TABLE one(a INT64)");
  Run("INSERT INTO one VALUES (1)");
  EXPECT_TRUE(
      Exec("SELECT * FROM one JOIN empty_t ON one.a = empty_t.a").empty());
  const auto left = Exec(
      "SELECT one.a, empty_t.a FROM one LEFT JOIN empty_t ON one.a = "
      "empty_t.a");
  ASSERT_EQ(left.size(), 1U);
  EXPECT_EQ(left[0][0], Value(int64_t{1}));
  EXPECT_TRUE(left[0][1].IsNull());
  const auto full = Exec(
      "SELECT one.a, empty_t.a FROM one FULL JOIN empty_t ON one.a = "
      "empty_t.a");
  ASSERT_EQ(full.size(), 1U);
  EXPECT_TRUE(
      Exec("SELECT * FROM empty_t JOIN one ON empty_t.a = one.a").empty());
}

TEST_F(ExecutorFeatureSqlTest, CrossJoin) {
  Run("CREATE TABLE cj(a INT64)");
  Run("INSERT INTO cj VALUES (1), (2)");
  const auto cross = Exec("SELECT x.a, y.a FROM cj x, cj y ORDER BY x.a, y.a");
  ASSERT_EQ(cross.size(), 4U);
  EXPECT_EQ(cross[0][0], Value(int64_t{1}));
  EXPECT_EQ(cross[0][1], Value(int64_t{1}));
  EXPECT_EQ(cross[3][0], Value(int64_t{2}));
  EXPECT_EQ(cross[3][1], Value(int64_t{2}));
}

TEST_F(ExecutorFeatureSqlTest, SetOperationSpillsUnderTinyBudget) {
  Run("CREATE TABLE big1(x INT64)");
  Run("CREATE TABLE big2(x INT64)");
  for (int64_t base = 0; base < 4; ++base) {
    std::string values;
    for (int64_t i = 0; i < 40; ++i) {
      if (i != 0) {
        values += ", ";
      }
      values += "(" + std::to_string(base * 10 + i % 10) + ")";
    }
    Run("INSERT INTO big1 VALUES " + values);
    Run("INSERT INTO big2 VALUES " + values);
  }
  BudgetGuard guard(2048);
  const auto union_rows =
      Exec("SELECT x FROM big1 UNION DISTINCT SELECT x FROM big2 ORDER BY x");
  EXPECT_EQ(union_rows.size(), 10U);
  const auto intersect =
      Exec("SELECT x FROM big1 INTERSECT DISTINCT SELECT x FROM big2 ORDER BY x");
  EXPECT_EQ(intersect.size(), 10U);
}

// ===========================================================================
// JoinHashIndex: open-addressing index, growth for both key modes.
// ===========================================================================

TEST(JoinHashIndexTest, InsertFindAndChainInt64Mode) {
  JoinHashIndex index;
  index.Init(JoinHashIndex::KeyMode::kInt64, 4);
  for (int64_t k = 0; k < 30; ++k) {
    index.Insert(JoinHashIndex::HashInt64(k), k, {}, static_cast<size_t>(k));
  }
  // Duplicate key chains in LIFO order.
  index.Insert(JoinHashIndex::HashInt64(7), 7, {}, 1000);
  const size_t head = index.Find(JoinHashIndex::HashInt64(7), 7, {});
  ASSERT_NE(head, JoinHashIndex::kNil);
  EXPECT_EQ(index.RowIndex(head), 1000U);
  const size_t second = index.ChainNext(head);
  EXPECT_EQ(index.RowIndex(second), 7U);
  EXPECT_EQ(index.ChainNext(second), JoinHashIndex::kNil);

  EXPECT_EQ(index.Find(JoinHashIndex::HashInt64(12345), 12345, {}),
            JoinHashIndex::kNil);
  // Every key remains findable after the rehash inside Grow().  Key 7 now
  // chains to the duplicate inserted above, so only its original entry is
  // checked here.
  for (int64_t k = 0; k < 30; ++k) {
    if (k == 7) {
      continue;
    }
    const size_t e = index.Find(JoinHashIndex::HashInt64(k), k, {});
    ASSERT_NE(e, JoinHashIndex::kNil);
    EXPECT_EQ(index.RowIndex(e), static_cast<size_t>(k));
  }
}

TEST(JoinHashIndexTest, InsertFindAndChainBytesMode) {
  JoinHashIndex index;
  index.Init(JoinHashIndex::KeyMode::kBytes, 4);
  const std::string k1 = "alpha";
  const std::string k2 = "beta";
  index.Insert(JoinHashIndex::HashBytes(k1), 0, k1, 0);
  index.Insert(JoinHashIndex::HashBytes(k2), 0, k2, 1);
  index.Insert(JoinHashIndex::HashBytes(k1), 0, k1, 2);  // duplicate chain

  const size_t head = index.Find(JoinHashIndex::HashBytes(k1), 0, k1);
  ASSERT_NE(head, JoinHashIndex::kNil);
  EXPECT_EQ(index.RowIndex(head), 2U);
  const size_t next = index.ChainNext(head);
  EXPECT_EQ(index.RowIndex(next), 0U);
  EXPECT_EQ(index.ChainNext(next), JoinHashIndex::kNil);

  const size_t beta = index.Find(JoinHashIndex::HashBytes(k2), 0, k2);
  ASSERT_NE(beta, JoinHashIndex::kNil);
  EXPECT_EQ(index.RowIndex(beta), 1U);
  EXPECT_EQ(index.Find(JoinHashIndex::HashBytes("missing"), 0, "missing"),
            JoinHashIndex::kNil);
}

// ===========================================================================
// HashJoin kinds that the SQL planner rarely emits: kSingle and kMark.
// ===========================================================================

TEST(HashJoinKindTest, SingleJoinMatchesOnceAndPadsMisses) {
  auto left = Src({Row({Value(1), Value("a")}), Row({Value(2), Value("b")}),
                   Row({Value(3), Value("c")})});
  auto right = Src({Row({Value(2), Value("x")})});
  HashJoin join(left, {0}, right, {0}, HashJoinMode::kInMemory,
                JoinKind::kSingle, /*worker_count=*/2, /*right_width=*/2,
                /*left_width=*/2);
  const std::vector<Row> out = Drain(&join);
  ASSERT_EQ(out.size(), 3U);
  // Left rows 1 and 3 miss -> NULL-padded right side.
  EXPECT_EQ(out[0], (Row({Value(1), Value("a"), Value(), Value()})));
  EXPECT_EQ(out[1], (Row({Value(2), Value("b"), Value(2), Value("x")})));
  EXPECT_EQ(out[2], (Row({Value(3), Value("c"), Value(), Value()})));
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
}

TEST(HashJoinKindTest, SingleJoinRejectsDuplicateMatch) {
  auto left = Src({IRow(1)});
  auto right = Src({Row({Value(1), Value("x")}), Row({Value(1), Value("y")})});
  HashJoin join(left, {0}, right, {0}, HashJoinMode::kInMemory,
                JoinKind::kSingle, 2, 2, 1);
  const std::vector<Row> out = Drain(&join);
  EXPECT_TRUE(out.empty());
  EXPECT_NE(join.GetStatus(), Status::kSuccess);
}

TEST(HashJoinKindTest, SingleJoinReloadsSpilledSides) {
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 120; ++i) {
    left_rows.push_back(IRow(i));
    if (i % 2 == 0) {
      right_rows.push_back(IRow(i));
    }
  }
  BudgetGuard guard(2048);  // forces reactive spill + LoadAll on both sides
  HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                HashJoinMode::kInMemory, JoinKind::kSingle, 2, 1, 1);
  const std::vector<Row> out = Drain(&join);
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  ASSERT_EQ(out.size(), 120U);
  int64_t matched = 0;
  for (const Row& row : out) {
    if (!row[1].IsNull()) {
      ++matched;
      EXPECT_EQ(row[1], row[0]);
    }
  }
  EXPECT_EQ(matched, 60);
}

TEST(HashJoinKindTest, MarkJoinMarkerValues) {
  // Build side without NULLs: match -> TRUE, miss -> FALSE.
  HashJoin join(Src({IRow(1), IRow(2)}), {0}, Src({IRow(2)}), {0},
                HashJoinMode::kInMemory, JoinKind::kMark, 2, 1, 1);
  const std::vector<Row> out = Drain(&join);
  ASSERT_EQ(out.size(), 2U);
  EXPECT_EQ(out[0], (Row({Value(1), Value(false)})));
  EXPECT_EQ(out[1], (Row({Value(2), Value(true)})));

  // A NULL build key turns a miss into UNKNOWN (NULL marker).
  HashJoin join2(Src({IRow(1), IRow(2)}), {0},
                 Src({IRow(2), Row({Value()})}), {0}, HashJoinMode::kInMemory,
                 JoinKind::kMark, 2, 1, 1);
  const std::vector<Row> out2 = Drain(&join2);
  ASSERT_EQ(out2.size(), 2U);
  EXPECT_EQ(out2[0], (Row({Value(1), Value()})));
  EXPECT_EQ(out2[1], (Row({Value(2), Value(true)})));

  // Empty build side: every marker is FALSE.
  HashJoin join3(Src({IRow(1)}), {0}, Src(std::vector<Row>{}), {0},
                 HashJoinMode::kInMemory, JoinKind::kMark, 2, 1, 1);
  const std::vector<Row> out3 = Drain(&join3);
  ASSERT_EQ(out3.size(), 1U);
  EXPECT_EQ(out3[0], (Row({Value(1), Value(false)})));

  // NULL probe key over a non-empty build side is UNKNOWN.
  HashJoin join4(Src({Row({Value()})}), {0}, Src({IRow(2)}), {0},
                 HashJoinMode::kInMemory, JoinKind::kMark, 2, 1, 1);
  const std::vector<Row> out4 = Drain(&join4);
  ASSERT_EQ(out4.size(), 1U);
  EXPECT_TRUE(out4[0][1].IsNull());
}

TEST(HashJoinKindTest, MarkJoinSpilledBuildSideAccumulatesMatches) {
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 200; ++i) {
    left_rows.push_back(IRow(i % 20));
  }
  for (int64_t i = 0; i < 10; ++i) {
    right_rows.push_back(IRow(i * 2));  // even keys 0..18
  }
  BudgetGuard guard(2048);  // spills the build side -> partitioned lookup
  HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                HashJoinMode::kInMemory, JoinKind::kMark, 2, 1, 1);
  const std::vector<Row> out = Drain(&join);
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  ASSERT_EQ(out.size(), 200U);
  size_t true_markers = 0;
  size_t false_markers = 0;
  for (const Row& row : out) {
    if (row[1] == Value(true)) {
      ++true_markers;
    } else if (row[1] == Value(false)) {
      ++false_markers;
    }
  }
  // 200 probe rows over 20 distinct keys, 10 of them matching.
  EXPECT_EQ(true_markers, 100U);
  EXPECT_EQ(false_markers, 100U);
}

// ===========================================================================
// HashJoin outer / semi / anti variants.
// ===========================================================================

TEST(HashJoinOuterTest, LeftRightFullOuterWithNullKeys) {
  const auto left_rows = std::vector<Row>{
      Row({Value(1), Value("l1")}),
      Row({Value(), Value("lnull")}),
      Row({Value(3), Value("l3")}),
  };
  const auto right_rows = std::vector<Row>{
      Row({Value(3), Value("r3")}),
      Row({Value(4), Value("r4")}),
      Row({Value(), Value("rnull")}),
  };
  {
    HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                  HashJoinMode::kInMemory, JoinKind::kLeftOuter, 2, 2, 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 3U);
    bool saw_null_pad = false;
    for (const Row& row : out) {
      EXPECT_EQ(row.values_.size(), 4U);
      if (row[0].IsNull()) {
        saw_null_pad = true;
        EXPECT_TRUE(row[2].IsNull());
      }
      if (row[0] == Value(3)) {
        EXPECT_EQ(row[3], Value("r3"));
      }
    }
    EXPECT_TRUE(saw_null_pad);
  }
  {
    HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                  HashJoinMode::kInMemory, JoinKind::kRightOuter, 2, 2, 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 3U);
    bool saw_right_pad = false;
    for (const Row& row : out) {
      if (row[3] == Value("r4")) {
        saw_right_pad = true;
        EXPECT_TRUE(row[0].IsNull());
      }
    }
    EXPECT_TRUE(saw_right_pad);
  }
  {
    HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                  HashJoinMode::kInMemory, JoinKind::kFullOuter, 2, 2, 2);
    const std::vector<Row> out = Drain(&join);
    // 1 match + 2 unmatched left + 2 unmatched right.
    ASSERT_EQ(out.size(), 5U);
  }
}

TEST(HashJoinOuterTest, OuterJoinsSurviveSpillAndReload) {
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 150; ++i) {
    left_rows.push_back(IRow(i));
    right_rows.push_back(IRow(i % 50));
  }
  BudgetGuard guard(2048);
  HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                HashJoinMode::kInMemory, JoinKind::kFullOuter, 2, 1, 1);
  const std::vector<Row> out = Drain(&join);
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  // Right keys 0..49 appear 3x each: 150 matches; left keys 50..149 add 100
  // left-only padded rows.
  ASSERT_EQ(out.size(), 250U);
  size_t padded = 0;
  for (const Row& row : out) {
    EXPECT_EQ(row.values_.size(), 2U);
    if (row[1].IsNull()) {
      ++padded;
    }
  }
  EXPECT_EQ(padded, 100U);
}

TEST(HashJoinSemiAntiTest, VariantsResidentAndSpilled) {
  const auto probe_rows = std::vector<Row>{
      Row({Value(1)}), Row({Value(2)}), Row({Value()}), Row({Value(4)})};
  const auto build_rows =
      std::vector<Row>{Row({Value(2)}), Row({Value(4)}), Row({Value()})};
  {
    HashJoin semi(Src(probe_rows), {0}, Src(build_rows), {0},
                  HashJoinMode::kInMemory, JoinKind::kSemi, 2, 1, 1);
    const std::vector<Row> out = Drain(&semi);
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(out[0][0], Value(2));
    EXPECT_EQ(out[1][0], Value(4));
  }
  {
    // Anti: NULL probe keys match nothing, so they survive (NOT EXISTS).
    // Survivors stream in probe order: key 1 first, then the NULL key.
    HashJoin anti(Src(probe_rows), {0}, Src(build_rows), {0},
                  HashJoinMode::kInMemory, JoinKind::kAnti, 2, 1, 1);
    const std::vector<Row> out = Drain(&anti);
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(out[0][0], Value(1));
    EXPECT_TRUE(out[1][0].IsNull());
  }
  {
    // NullAwareAnti (NOT IN): the build side carries a NULL, so every
    // non-matching probe is UNKNOWN and filtered; only strict non-NULL
    // matches would be excluded, and here nothing survives at all.
    HashJoin nanti(Src(probe_rows), {0}, Src(build_rows), {0},
                   HashJoinMode::kInMemory, JoinKind::kNullAwareAnti, 2, 1, 1);
    EXPECT_TRUE(Drain(&nanti).empty());
  }
  {
    // NullAwareAnti with a NULL build key: everything is UNKNOWN.
    const auto build_with_null =
        std::vector<Row>{Row({Value(2)}), Row({Value()}), Row({Value(9)})};
    HashJoin nanti(Src(probe_rows), {0}, Src(build_with_null), {0},
                   HashJoinMode::kInMemory, JoinKind::kNullAwareAnti, 2, 1, 1);
    EXPECT_TRUE(Drain(&nanti).empty());
  }
}

TEST(HashJoinSemiAntiTest, AntiJoinWithEmptySpilledBuildEmitsAll) {
  std::vector<Row> probe_rows;
  for (int64_t i = 0; i < 200; ++i) {
    probe_rows.push_back(IRow(i));
  }
  BudgetGuard guard(2048);  // probe side spills; build side stays empty
  HashJoin anti(Src(probe_rows), {0}, Src(std::vector<Row>{}), {0},
                HashJoinMode::kInMemory, JoinKind::kAnti, 2, 1, 1);
  const std::vector<Row> out = Drain(&anti);
  EXPECT_EQ(anti.GetStatus(), Status::kSuccess);
  EXPECT_EQ(out.size(), 200U);
}

TEST(HashJoinSemiAntiTest, SemiJoinResidentProbeOverSpilledBuild) {
  std::vector<Row> probe_rows;
  probe_rows.push_back(Row({Value()}));
  for (int64_t i = 1; i <= 6; ++i) {
    probe_rows.push_back(IRow(i));
  }
  std::vector<Row> build_rows;
  for (int64_t i = 0; i < 300; ++i) {
    build_rows.push_back(IRow(i % 4));  // keys 0..3 only
  }
  BudgetGuard guard(2048);  // build side spills, probe stays resident
  HashJoin semi(Src(probe_rows), {0}, Src(build_rows), {0},
                HashJoinMode::kInMemory, JoinKind::kSemi, 2, 1, 1);
  const std::vector<Row> out = Drain(&semi);
  EXPECT_EQ(semi.GetStatus(), Status::kSuccess);
  // Keys 1..3 match; 4..6 and the NULL key never match.
  ASSERT_EQ(out.size(), 3U);
  EXPECT_EQ(out[0][0], Value(1));
  EXPECT_EQ(out[1][0], Value(2));
  EXPECT_EQ(out[2][0], Value(3));
}

// ===========================================================================
// HashJoin hybrid mode and in-memory pipelined paths.
// ===========================================================================

TEST(HashJoinHybridTest, HybridModeMatchesInnerJoinSemantics) {
  // Variant A: unique keys on both sides -> 1:1 join.
  {
    std::vector<Row> left_rows;
    std::vector<Row> right_rows;
    for (int64_t i = 0; i < 300; ++i) {
      left_rows.push_back(Row({Value(i), Value(i * 2)}));
      right_rows.push_back(Row({Value(i), Value("r")}));
    }
    HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                  HashJoinMode::kHybrid, JoinKind::kInner, /*worker_count=*/2);
    const std::vector<Row> out = Drain(&join);
    EXPECT_EQ(join.GetStatus(), Status::kSuccess);
    ASSERT_EQ(out.size(), 300U);
    for (const Row& row : out) {
      ASSERT_EQ(row.values_.size(), 4U);
      EXPECT_EQ(row[1], Value(row[0].value.int_value * 2));
      EXPECT_EQ(row[3], Value("r"));
    }
  }
  // Variant B: duplicate keys on the right side.
  {
    std::vector<Row> left_rows;
    std::vector<Row> right_rows;
    for (int64_t i = 0; i < 40; ++i) {
      left_rows.push_back(Row({Value(i), Value(i * 2)}));
      right_rows.push_back(Row({Value(i % 10), Value("r")}));
    }
    HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                  HashJoinMode::kHybrid, JoinKind::kInner, /*worker_count=*/2);
    const std::vector<Row> out = Drain(&join);
    EXPECT_EQ(join.GetStatus(), Status::kSuccess);
    // Left keys 0..9 match 4 right rows each; keys 10..39 match nothing.
    EXPECT_EQ(out.size(), 40U);
    for (const Row& row : out) {
      EXPECT_EQ(row[1], Value(row[0].value.int_value * 2));
      EXPECT_EQ(row[3], Value("r"));
    }
  }
}

TEST(HashJoinHybridTest, HybridModeDisjointKeysYieldNothing) {
  auto left = Src(SequenceRows(0, 50));
  auto right = Src(SequenceRows(1000, 1100));
  HashJoin join(left, {0}, right, {0}, HashJoinMode::kHybrid,
                JoinKind::kInner, 2);
  EXPECT_TRUE(Drain(&join).empty());
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("HybridHashJoin"), std::string::npos);
}

TEST(HashJoinAdaptiveTest, StaysInNestedLoopBelowProbeThreshold) {
  // Equal-size sides: the right side becomes the build index and the left
  // (3 rows, below kNestedLoopProbeLimit) stays on the nested-loop phase.
  auto left = Src({IRow(1), IRow(2), IRow(777)});
  auto right = Src({IRow(1), IRow(2), IRow(999)});
  HashJoin join(left, {0}, right, {0}, HashJoinMode::kInMemory,
                JoinKind::kInner, 2);
  const std::vector<Row> out = Drain(&join);
  ASSERT_EQ(out.size(), 2U);
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("AdaptiveJoin initial=NestedLoop final=NestedLoop"),
            std::string::npos);
  // Probe key 777 matched nothing: the exact-set filter counted the reject.
  EXPECT_NE(ss.str().find("probe_rows_rejected=1"), std::string::npos);
}

TEST(HashJoinAdaptiveTest, SwitchesToHashBuildWhenProbeGrows) {
  std::vector<Row> build_rows;
  for (int64_t i = 0; i < 10; ++i) {
    build_rows.push_back(IRow(i));
  }
  // 6 probe rows cross kNestedLoopProbeLimit (4) mid-stream.
  const std::vector<Row> probe_rows = {IRow(0), IRow(1), IRow(2),
                                       IRow(3), IRow(4), IRow(999)};
  HashJoin join(Src(build_rows), {0}, Src(probe_rows), {0},
                HashJoinMode::kInMemory, JoinKind::kInner, 2);
  const std::vector<Row> out = Drain(&join);
  ASSERT_EQ(out.size(), 5U);
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("AdaptiveJoin initial=NestedLoop final=HashJoin"),
            std::string::npos);
}

TEST(HashJoinParallelProbeTest, StripedProbeOverLargeProbeSide) {
  std::vector<Row> build_rows;
  for (int64_t i = 0; i < 100; ++i) {
    build_rows.push_back(IRow(i));
  }
  std::vector<Row> probe_rows;
  probe_rows.reserve(34000);
  for (int64_t i = 0; i < 34000; ++i) {
    probe_rows.push_back(IRow(i % 1000));
  }
  HashJoin join(Src(build_rows), {0}, Src(probe_rows), {0},
                HashJoinMode::kInMemory, JoinKind::kInner,
                /*worker_count=*/8);
  const std::vector<Row> out = Drain(&join);
  // 34 full cycles of keys 0..99 matched within each 1000-block.
  EXPECT_EQ(out.size(), 3400U);
  EXPECT_GT(join.MaterializedBytes(), 0U);

  // The batched consumer drains the striped queues too.
  HashJoin join2(Src(build_rows), {0}, Src(probe_rows), {0},
                 HashJoinMode::kInMemory, JoinKind::kInner, 8);
  DataChunk chunk;
  size_t total = 0;
  for (;;) {
    const size_t got = join2.NextBatch(&chunk, 512);
    if (got == 0) {
      break;
    }
    total += got;
    EXPECT_LE(chunk.Size(), 512U);
  }
  EXPECT_EQ(total, 3400U);
  EXPECT_EQ(join2.GetStatus(), Status::kSuccess);
}

TEST(HashJoinSpillTest, BothSidesSpilledInnerJoinPartitionPairwise) {
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 400; ++i) {
    left_rows.push_back(Row({Value(i % 13)}));
    right_rows.push_back(Row({Value(i % 7)}));
  }
  BudgetGuard guard(2048);
  HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                HashJoinMode::kInMemory, JoinKind::kInner, 2);
  const std::vector<Row> out = Drain(&join);
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  // Shared keys 0..6: total = sum over k of left_count(k) * right_count(k).
  size_t expected = 0;
  for (int64_t k = 0; k < 7; ++k) {
    size_t lc = 0;
    for (int64_t i = k; i < 400; i += 13) {
      ++lc;
    }
    size_t rc = 0;
    for (int64_t i = k; i < 400; i += 7) {
      ++rc;
    }
    expected += lc * rc;
  }
  EXPECT_EQ(out.size(), expected);
  EXPECT_GT(expected, 10000U);  // sanity: the join really produced bulk rows
}

TEST(HashJoinSpillTest, OneSideSpilledKeepsResultExact) {
  // Tiny left side (resident), large right side (spilled).
  auto left = Src({IRow(5), IRow(600)});
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 500; ++i) {
    right_rows.push_back(IRow(i));
  }
  BudgetGuard guard(4096);
  HashJoin join(left, {0}, Src(right_rows), {0}, HashJoinMode::kInMemory,
                JoinKind::kInner, 2);
  const std::vector<Row> out = Drain(&join);
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  ASSERT_EQ(out.size(), 1U);
  EXPECT_EQ(out[0], (Row({Value(5), Value(5)})));
}

TEST(HashJoinSpillTest, MultiKeySpilledJoinWithStringComponent) {
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 200; ++i) {
    left_rows.push_back(Row({Value(i % 10), Value("k")}));
    right_rows.push_back(Row({Value(i % 6), Value("k")}));
  }
  BudgetGuard guard(2048);
  HashJoin join(Src(left_rows), {0, 1}, Src(right_rows), {0, 1},
                HashJoinMode::kInMemory, JoinKind::kInner, 2);
  const std::vector<Row> out = Drain(&join);
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  size_t expected = 0;
  for (int64_t k = 0; k < 6; ++k) {
    size_t lc = 0;
    for (int64_t i = k; i < 200; i += 10) {
      ++lc;
    }
    size_t rc = 0;
    for (int64_t i = k; i < 200; i += 6) {
      ++rc;
    }
    expected += lc * rc;
  }
  EXPECT_EQ(out.size(), expected);
}

// ===========================================================================
// MergeJoin (serial).
// ===========================================================================

TEST(MergeJoinTest, InnerJoinDuplicateRunsCrossProduct) {
  MergeJoin join(Src({IRow(1), IRow(2), IRow(2), IRow(4)}), {0},
                 Src({IRow(2), IRow(2), IRow(3)}), {0}, JoinKind::kInner, 1,
                 1);
  const std::vector<Row> out = Drain(&join);
  ASSERT_EQ(out.size(), 4U);
  for (const Row& row : out) {
    EXPECT_EQ(row[0], Value(2));
    EXPECT_EQ(row[1], Value(2));
  }
}

TEST(MergeJoinTest, OuterVariantsEmitNullPaddedRows) {
  {
    MergeJoin join(Src({IRow(1), IRow(3)}), {0}, Src({IRow(2), IRow(3)}), {0},
                   JoinKind::kLeftOuter, 1, 1);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(out[0], (Row({Value(1), Value()})));
    EXPECT_EQ(out[1], (Row({Value(3), Value(3)})));
  }
  {
    MergeJoin join(Src({IRow(1), IRow(3)}), {0}, Src({IRow(2), IRow(3)}), {0},
                   JoinKind::kRightOuter, 1, 1);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(out[0], (Row({Value(), Value(2)})));
    EXPECT_EQ(out[1], (Row({Value(3), Value(3)})));
  }
  {
    MergeJoin join(Src({IRow(1), IRow(3)}), {0}, Src({IRow(2), IRow(3)}), {0},
                   JoinKind::kFullOuter, 1, 1);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 3U);
    // Merge order: left 1 < right 2 pads left; right 2 is then passed over
    // and padded; the (3,3) match follows.
    EXPECT_EQ(out[0], (Row({Value(1), Value()})));
    EXPECT_EQ(out[1], (Row({Value(), Value(2)})));
    EXPECT_EQ(out[2], (Row({Value(3), Value(3)})));
  }
}

TEST(MergeJoinTest, SemiAndAntiPreserveRowPositions) {
  {
    MergeJoin semi(Src({IRow(1), IRow(2), IRow(5)}), {0},
                   Src({IRow(2), IRow(3)}), {0}, JoinKind::kSemi, 1, 1);
    Row row;
    RowPosition rp;
    size_t count = 0;
    while (semi.Next(&row, &rp)) {
      EXPECT_EQ(row[0], Value(2));
      EXPECT_TRUE(rp.IsValid() || rp.page_id == RowPosition().page_id);
      ++count;
    }
    EXPECT_EQ(count, 1U);
  }
  {
    MergeJoin anti(Src({IRow(1), IRow(2), IRow(5)}), {0},
                   Src({IRow(2), IRow(3)}), {0}, JoinKind::kAnti, 1, 1);
    const std::vector<Row> out = Drain(&anti);
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(out[0][0], Value(1));
    EXPECT_EQ(out[1][0], Value(5));
  }
}

TEST(MergeJoinTest, NullKeysNeverMatchAndOuterKeepsThem) {
  {
    MergeJoin join(Src({Row({Value()}), IRow(1)}), {0},
                   Src({Row({Value()}), IRow(1)}), {0}, JoinKind::kInner, 1, 1);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0], (Row({Value(1), Value(1)})));
  }
  {
    MergeJoin join(Src({Row({Value()}), IRow(1)}), {0},
                   Src({Row({Value()}), IRow(1)}), {0}, JoinKind::kFullOuter, 1,
                   1);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 3U);  // 1 match + 2 NULL-keyed rows preserved
  }
}

TEST(MergeJoinTest, ResidualFiltersPairsAndPadsUnmatchedOuterRows) {
  const Schema lschema(
      "l", {Column("a", ValueType::kInt64), Column("lv", ValueType::kInt64)});
  const Schema rschema(
      "r", {Column("b", ValueType::kInt64), Column("rv", ValueType::kInt64)});
  const Expression residual = BinaryExpressionExp(
      ColumnValueExp(ColumnName("l", "lv")), BinaryOperation::kLessThan,
      ColumnValueExp(ColumnName("r", "rv")));
  const Schema combined = lschema + rschema;
  {
    MergeJoin inner(Src({Row({Value(1), Value(10)}), Row({Value(2), Value(1)})}),
                    {0},
                    Src({Row({Value(1), Value(20)}), Row({Value(2), Value(0)})}),
                    {0}, JoinKind::kInner, 2, 2, residual, combined);
    const std::vector<Row> out = Drain(&inner);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0], (Row({Value(1), Value(10), Value(1), Value(20)})));
  }
  {
    // Left outer: the left row whose only pair fails the residual is
    // NULL-padded.
    MergeJoin outer(Src({Row({Value(1), Value(10)}), Row({Value(2), Value(1)})}),
                    {0},
                    Src({Row({Value(1), Value(20)}), Row({Value(2), Value(0)})}),
                    {0}, JoinKind::kLeftOuter, 2, 2, residual, combined);
    const std::vector<Row> out = Drain(&outer);
    ASSERT_EQ(out.size(), 2U);
    bool saw_pad = false;
    for (const Row& row : out) {
      if (row[0] == Value(2)) {
        saw_pad = true;
        EXPECT_TRUE(row[2].IsNull());
      }
    }
    EXPECT_TRUE(saw_pad);
  }
}

TEST(MergeJoinTest, ResidualDivideByZeroEvaluatesNullNotError) {
  // Divide-by-zero yields SQL NULL in this engine (not an error), so the
  // residual simply rejects the pair and the outer row is NULL-padded.
  const Schema lschema("l", {Column("a", ValueType::kInt64)});
  const Schema rschema("r", {Column("b", ValueType::kInt64)});
  const Expression bad = BinaryExpressionExp(
      ColumnValueExp(ColumnName("l", "a")), BinaryOperation::kDivide,
      ColumnValueExp(ColumnName("r", "b")));
  MergeJoin join(Src({IRow(1)}), {0}, Src({IRow(0)}), {0}, JoinKind::kLeftOuter,
                 1, 1, bad, lschema + rschema);
  const std::vector<Row> out = Drain(&join);
  ASSERT_EQ(out.size(), 1U);
  EXPECT_EQ(out[0], (Row({Value(1), Value()})));
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
}

TEST(MergeJoinTest, MultiColumnKeysAndEmptyInputs) {
  MergeJoin join(Src({Row({Value(1), Value("a")}), Row({Value(1), Value("b")})}),
                 {0, 1}, Src({Row({Value(1), Value("a")})}), {0, 1},
                 JoinKind::kInner, 2, 2);
  const std::vector<Row> out = Drain(&join);
  ASSERT_EQ(out.size(), 1U);
  EXPECT_EQ(out[0], (Row({Value(1), Value("a"), Value(1), Value("a")})));

  MergeJoin empty(Src(std::vector<Row>{}), {0}, Src({IRow(1)}), {0},
                  JoinKind::kInner, 1, 1);
  EXPECT_TRUE(Drain(&empty).empty());
  std::stringstream ss;
  empty.Dump(ss, 0);
  EXPECT_NE(ss.str().find("MergeJoin"), std::string::npos);
}

// ===========================================================================
// ParallelMergeJoin.
// ===========================================================================

TEST(ParallelMergeJoinTest, InnerJoinMultiPartitionMatchesSerialResult) {
  ParallelMergeJoin join(Src(SortedKeyRows(0, 40, 2)), {0},
                         Src(SortedKeyRows(10, 50, 1)), {0},
                         /*worker_count=*/4, JoinKind::kInner, Expression(),
                         Schema(), 2);
  std::vector<Row> out = Drain(&join);
  SortRowsByValue(&out);
  ASSERT_EQ(out.size(), 60U);  // keys 10..39, 2 x 1 pairs each
  EXPECT_EQ(out[0], (Row({Value(10), Value(100), Value(10), Value(100)})));
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("ParallelMergeJoin"), std::string::npos);
}

TEST(ParallelMergeJoinTest, OuterVariantsPadUnmatchedSides) {
  // Keys 0..9 left, 5..14 right: 0..4 left-only, 5..9 matched, 10..14
  // right-only.  Small inputs keep a single steering partition.
  {
    ParallelMergeJoin join(Src(SortedKeyRows(0, 10, 1)), {0},
                           Src(SortedKeyRows(5, 15, 1)), {0}, 2,
                           JoinKind::kLeftOuter, Expression(), Schema(), 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 10U);  // 5 matched + 5 left-padded
    size_t padded = 0;
    for (const Row& row : out) {
      EXPECT_EQ(row.values_.size(), 4U);
      if (row[2].IsNull()) {
        EXPECT_TRUE(row[3].IsNull());
        ++padded;
      }
    }
    EXPECT_EQ(padded, 5U);
  }
  {
    ParallelMergeJoin join(Src(SortedKeyRows(0, 10, 1)), {0},
                           Src(SortedKeyRows(5, 15, 1)), {0}, 2,
                           JoinKind::kRightOuter, Expression(), Schema(), 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 10U);  // 5 matched + 5 right-padded
    size_t padded = 0;
    for (const Row& row : out) {
      EXPECT_EQ(row.values_.size(), 4U);
      if (row[0].IsNull()) {
        EXPECT_TRUE(row[1].IsNull());
        EXPECT_GE(row[2].value.int_value, 10);  // keys 10..14 are right-only
        ++padded;
      }
    }
    EXPECT_EQ(padded, 5U);
  }
  {
    ParallelMergeJoin join(Src(SortedKeyRows(0, 10, 1)), {0},
                           Src(SortedKeyRows(5, 15, 1)), {0}, 2,
                           JoinKind::kFullOuter, Expression(), Schema(), 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 15U);  // 5 matched + 5 + 5 padded
  }
}

TEST(ParallelMergeJoinTest, SemiAntiAndNullKeys) {
  {
    ParallelMergeJoin semi(Src({Row({Value()}), IRow(1), IRow(2), IRow(3)}),
                           {0}, Src({Row({Value()}), IRow(2)}), {0}, 2,
                           JoinKind::kSemi, Expression(), Schema(), 1);
    const std::vector<Row> out = Drain(&semi);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0][0], Value(2));
  }
  {
    // NULL probe keys match nothing and survive anti.
    ParallelMergeJoin anti(Src({Row({Value()}), IRow(1), IRow(2), IRow(3)}),
                           {0}, Src({Row({Value()}), IRow(2)}), {0}, 2,
                           JoinKind::kAnti, Expression(), Schema(), 1);
    const std::vector<Row> out = Drain(&anti);
    ASSERT_EQ(out.size(), 3U);
    EXPECT_TRUE(out[0][0].IsNull());
    EXPECT_EQ(out[1][0], Value(1));
    EXPECT_EQ(out[2][0], Value(3));
  }
}

TEST(ParallelMergeJoinTest, ResidualDecidesMatchesWithOuterFallback) {
  const Schema lschema(
      "l", {Column("a", ValueType::kInt64), Column("lv", ValueType::kInt64)});
  const Schema rschema(
      "r", {Column("b", ValueType::kInt64), Column("rv", ValueType::kInt64)});
  // lv < rv: pairs against rv=20 pass, pairs against rv=5 fail.
  const Expression residual = BinaryExpressionExp(
      ColumnValueExp(ColumnName("l", "lv")), BinaryOperation::kLessThan,
      ColumnValueExp(ColumnName("r", "rv")));
  {
    ParallelMergeJoin join(
        Src({Row({Value(1), Value(10)}), Row({Value(1), Value(11)})}), {0},
        Src({Row({Value(1), Value(20)}), Row({Value(1), Value(5)})}), {0}, 2,
        JoinKind::kLeftOuter, residual, lschema + rschema, 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 2U);
    for (const Row& row : out) {
      EXPECT_EQ(row[3], Value(20));  // only the rv=20 pairs pass
    }
  }
  {
    // An impossible residual rejects every pair: the left rows fall back to
    // NULL-padded output.
    const Expression impossible = BinaryExpressionExp(
        ColumnValueExp(ColumnName("l", "lv")), BinaryOperation::kGreaterThan,
        ConstantValueExp(Value(int64_t{1000})));
    ParallelMergeJoin join(
        Src({Row({Value(1), Value(10)}), Row({Value(1), Value(11)})}), {0},
        Src({Row({Value(1), Value(20)}), Row({Value(1), Value(5)})}), {0}, 2,
        JoinKind::kLeftOuter, impossible, lschema + rschema, 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 2U);
    for (const Row& row : out) {
      EXPECT_TRUE(row[2].IsNull());
      EXPECT_TRUE(row[3].IsNull());
    }
  }
}

TEST(ParallelMergeJoinTest, AllEqualKeysFormOneGiantCluster) {
  // Every row shares one key: the steering partitioner walks past the whole
  // cluster and the trailing partition holds the full 50 x 50 cross product.
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 50; ++i) {
    left_rows.push_back(Row({Value(int64_t{1}), Value(i)}));
    right_rows.push_back(Row({Value(int64_t{1}), Value(i)}));
  }
  ParallelMergeJoin join(Src(left_rows), {0}, Src(right_rows), {0}, 4,
                         JoinKind::kInner, Expression(), Schema(), 2);
  const std::vector<Row> out = Drain(&join);
  EXPECT_EQ(out.size(), 2500U);
}

TEST(ParallelMergeJoinTest, EmptySidesAndBatchPath) {
  {
    ParallelMergeJoin join(Src(std::vector<Row>{}), {0}, Src({IRow(1)}), {0},
                           2, JoinKind::kInner, Expression(), Schema(), 1);
    EXPECT_TRUE(Drain(&join).empty());
    EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  }
  {
    ParallelMergeJoin join(Src(SortedKeyRows(0, 40, 2)), {0},
                           Src(SortedKeyRows(10, 50, 1)), {0}, 4,
                           JoinKind::kInner, Expression(), Schema(), 2);
    DataChunk chunk;
    size_t total = 0;
    for (;;) {
      const size_t got = join.NextBatch(&chunk, 32);
      if (got == 0) {
        break;
      }
      total += got;
    }
    EXPECT_EQ(total, 60U);
  }
}

TEST(ParallelMergeJoinTest, FailedChildLatchesStatusWithoutPartialResult) {
  ParallelMergeJoin join(std::make_shared<FailingSource>(), {0}, Src({IRow(1)}),
                         {0}, 2, JoinKind::kInner, Expression(), Schema(), 1);
  EXPECT_TRUE(Drain(&join).empty());
  EXPECT_NE(join.GetStatus(), Status::kSuccess);
}

// ===========================================================================
// SharedBuildParallelHashJoin.
// ===========================================================================

TEST(SharedBuildParallelHashJoinTest, KindsMatchSerialSemantics) {
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 70; ++i) {
    left_rows.push_back(Row({Value(i % 10), Value("L")}));
  }
  for (int64_t i = 0; i < 70; ++i) {
    right_rows.push_back(Row({Value(i % 5), Value("R")}));
  }
  {
    SharedBuildParallelHashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                                     4, JoinKind::kInner, 2);
    const std::vector<Row> out = Drain(&join);
    // Left keys 0..4 (7 rows each) match 14 right rows each: 5 * 7 * 14.
    EXPECT_EQ(out.size(), 70U * 7U);
  }
  {
    // Semi: left rows with keys 0..4 match (35 rows); keys 5..9 have no
    // right counterpart.
    SharedBuildParallelHashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                                     4, JoinKind::kSemi, 2);
    EXPECT_EQ(Drain(&join).size(), 35U);
  }
  {
    // Anti: only the unmatched left keys 5..9 survive.
    SharedBuildParallelHashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                                     4, JoinKind::kAnti, 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 35U);
    for (const Row& row : out) {
      EXPECT_GE(row[0].value.int_value, 5);
    }
  }
  {
    SharedBuildParallelHashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                                     4, JoinKind::kLeftOuter, 2);
    const std::vector<Row> out = Drain(&join);
    // 490 matched pairs + 35 unmatched left rows padded to width 4.
    EXPECT_EQ(out.size(), 525U);
    for (const Row& row : out) {
      EXPECT_EQ(row.values_.size(), 4U);
    }
  }
}

TEST(SharedBuildParallelHashJoinTest, NullKeysNeverMatchAndPadsOuter) {
  const auto left_rows = std::vector<Row>{
      Row({Value(), Value("ln")}), Row({Value(1), Value("l1")}),
      Row({Value(2), Value("l2")})};
  const auto right_rows = std::vector<Row>{
      Row({Value(), Value("rn")}), Row({Value(2), Value("r1")}),
      Row({Value(2), Value("r2")})};
  {
    SharedBuildParallelHashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                                     2, JoinKind::kInner, 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 2U);
    for (const Row& row : out) {
      EXPECT_EQ(row[0], Value(2));
      EXPECT_EQ(row.values_.size(), 4U);
    }
  }
  {
    SharedBuildParallelHashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                                     2, JoinKind::kLeftOuter, 2);
    const std::vector<Row> out = Drain(&join);
    // 2 matched pairs (left key 2) + both unmatched left rows padded.
    ASSERT_EQ(out.size(), 4U);
    size_t padded = 0;
    for (const Row& row : out) {
      EXPECT_EQ(row.values_.size(), 4U);
      if (row[2].IsNull() && row[3].IsNull()) {
        // Padded rows keep their left payload and NULL right side.
        EXPECT_TRUE(row[0].IsNull() || row[0] == Value(1));
        ++padded;
      }
    }
    EXPECT_EQ(padded, 2U);
  }
}

TEST(SharedBuildParallelHashJoinTest, NullAwareAntiDropsAllOnNullBuildKey) {
  std::vector<Row> left_rows;
  for (int64_t i = 0; i < 40; ++i) {
    left_rows.push_back(IRow(i));
  }
  SharedBuildParallelHashJoin join(Src(left_rows), {0},
                                   Src({IRow(1000), Row({Value()})}), {0}, 2,
                                   JoinKind::kNullAwareAnti, 1);
  EXPECT_TRUE(Drain(&join).empty());

  // Without a NULL build key the NOT IN set behaves like plain anti.
  SharedBuildParallelHashJoin join2(Src(left_rows), {0}, Src({IRow(1000)}),
                                    {0}, 2, JoinKind::kNullAwareAnti, 1);
  EXPECT_EQ(Drain(&join2).size(), 40U);
}

TEST(SharedBuildParallelHashJoinTest, BatchPathAndFailurePropagation) {
  {
    std::vector<Row> left_rows;
    std::vector<Row> right_rows;
    for (int64_t i = 0; i < 40; ++i) {
      left_rows.push_back(IRow(i));
      right_rows.push_back(IRow(i));
    }
    SharedBuildParallelHashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                                     2, JoinKind::kInner, 1);
    DataChunk chunk;
    size_t total = 0;
    for (;;) {
      const size_t got = join.NextBatch(&chunk, 7);
      if (got == 0) {
        break;
      }
      EXPECT_LE(got, 7U);
      total += got;
    }
    EXPECT_EQ(total, 40U);
    std::stringstream ss;
    join.Dump(ss, 0);
    EXPECT_NE(ss.str().find("SharedBuildParallelHashJoin"), std::string::npos);
    EXPECT_GT(join.MaterializedBytes(), 0U);
  }
  {
    SharedBuildParallelHashJoin join(std::make_shared<FailingSource>(), {0},
                                     Src({IRow(1)}), {0}, 2, JoinKind::kInner,
                                     1);
    Row row;
    EXPECT_FALSE(join.Next(&row, nullptr));
    EXPECT_NE(join.GetStatus(), Status::kSuccess);
    // Retried pulls keep reporting the latched failure.
    EXPECT_FALSE(join.Next(&row, nullptr));
    EXPECT_NE(join.GetStatus(), Status::kSuccess);
  }
  {
    SharedBuildParallelHashJoin join(Src({IRow(1)}), {0},
                                     std::make_shared<FailingSource>(), {0}, 2,
                                     JoinKind::kInner, 1);
    EXPECT_TRUE(Drain(&join).empty());
    EXPECT_NE(join.GetStatus(), Status::kSuccess);
  }
}

// ===========================================================================
// ExchangeExecutor: routing, gather, lazy materialization, errors.
// ===========================================================================

TEST(ExchangeExecutorTest, HashExchangeRoutesNullSafeAndGathersBack) {
  std::vector<Row> rows;
  rows.push_back(Row({Value()}));
  for (int64_t i = 0; i < 20; ++i) {
    rows.push_back(IRow(i));
  }
  auto exchange = std::make_shared<ExchangeExecutor>(
      Src(rows), ExchangeType::kHash, 4, std::vector<slot_t>{0});
  EXPECT_FALSE(exchange->IsMaterialized());
  std::vector<Row> collected;
  for (size_t p = 0; p < exchange->PartitionCount(); ++p) {
    Executor part = exchange->GetPartitionExecutor(p);
    for (const Row& row : Drain(part.get())) {
      collected.push_back(row);
    }
  }
  EXPECT_TRUE(exchange->IsMaterialized());
  ASSERT_EQ(collected.size(), rows.size());
  EXPECT_EQ(exchange->MaterializedRowCount(), rows.size());
  // Every input row came back exactly once (order may differ).
  ExpectSameMultiset(collected, rows);
}

TEST(ExchangeExecutorTest, BroadcastCopiesEveryRowToEveryPartition) {
  auto exchange = std::make_shared<ExchangeExecutor>(
      Src({IRow(1), IRow(2), IRow(3)}), ExchangeType::kBroadcast, 3);
  exchange->MaterializePipeline();
  for (size_t p = 0; p < 3; ++p) {
    EXPECT_EQ(exchange->GetPartitionRows(p).size(), 3U);
  }
  EXPECT_EQ(exchange->MaterializedRowCount(), 9U);
  EXPECT_GT(exchange->MaterializedBytes(), 0U);
}

TEST(ExchangeExecutorTest, RangeExchangeRoutesByBounds) {
  std::vector<Row> rows;
  for (int64_t i = 0; i < 36; ++i) {
    rows.push_back(IRow(i));
  }
  auto exchange = std::make_shared<ExchangeExecutor>(
      Src(rows), ExchangeType::kRange, 4, std::vector<slot_t>{0},
      std::vector<Value>{Value(int64_t{10}), Value(int64_t{20}),
                         Value(int64_t{30})});
  exchange->MaterializePipeline();
  // Keys < 10 -> 0, < 20 -> 1, < 30 -> 2, else 3.
  EXPECT_EQ(exchange->GetPartitionRows(0).size(), 10U);
  EXPECT_EQ(exchange->GetPartitionRows(1).size(), 10U);
  EXPECT_EQ(exchange->GetPartitionRows(2).size(), 10U);
  EXPECT_EQ(exchange->GetPartitionRows(3).size(), 6U);

  // NULL keys route to the first partition.
  auto null_exchange = std::make_shared<ExchangeExecutor>(
      Src(std::vector<Row>{Row({Value()}), IRow(5)}), ExchangeType::kRange, 2,
      std::vector<slot_t>{0}, std::vector<Value>{Value(int64_t{10})});
  null_exchange->MaterializePipeline();
  EXPECT_EQ(null_exchange->GetPartitionRows(0).size(), 2U);
}

TEST(ExchangeExecutorTest, RangeExchangeSkipsIncomparableBounds) {
  // An INT64 key against a STRING bound cannot be ordered; the router skips
  // the bound instead of throwing across the partition boundary, then clamps
  // to the last partition.
  auto exchange = std::make_shared<ExchangeExecutor>(
      Src({IRow(5)}), ExchangeType::kRange, 2, std::vector<slot_t>{0},
      std::vector<Value>{Value("zzz")});
  exchange->MaterializePipeline();
  EXPECT_EQ(exchange->GetPartitionRows(0).size(), 0U);
  EXPECT_EQ(exchange->GetPartitionRows(1).size(), 1U);
}

TEST(ExchangeExecutorTest, GatherNextAndBatchDrainAcrossPartitions) {
  std::vector<Row> rows;
  for (int64_t i = 0; i < 10; ++i) {
    rows.push_back(IRow(i));
  }
  auto exchange = std::make_shared<ExchangeExecutor>(
      Src(rows), ExchangeType::kHash, 3, std::vector<slot_t>{0});
  // Lazy materialization: Next() itself triggers distribution.
  std::vector<Row> gathered;
  Row row;
  RowPosition rp;
  while (exchange->Next(&row, &rp)) {
    gathered.push_back(row);
    row = Row();
  }
  ExpectSameMultiset(gathered, rows);
  // Exhausted gather stays exhausted.
  EXPECT_FALSE(exchange->Next(&row, nullptr));

  // Batch path resets the reused chunk.
  const Schema schema("t", {Column("x", ValueType::kInt64)});
  DataChunk chunk(schema, 8);
  auto exchange2 = std::make_shared<ExchangeExecutor>(Src(rows),
                                                      ExchangeType::kGather, 1);
  size_t total = 0;
  for (;;) {
    const size_t got = exchange2->NextBatch(&chunk, 4);
    if (got == 0) {
      break;
    }
    total += got;
    EXPECT_EQ(chunk.Size(), got);
  }
  EXPECT_EQ(total, 10U);
}

TEST(ExchangeExecutorTest, ChildErrorSurfacesAsStickyStatus) {
  auto gather = std::make_shared<ExchangeExecutor>(
      std::make_shared<FailingSource>(), ExchangeType::kGather, 1);
  Row row;
  EXPECT_FALSE(gather->Next(&row, nullptr));
  EXPECT_NE(gather->GetStatus(), Status::kSuccess);

  auto hash = std::make_shared<ExchangeExecutor>(
      std::make_shared<FailingSource>(), ExchangeType::kHash, 2,
      std::vector<slot_t>{0});
  Executor partition = hash->GetPartitionExecutor(0);
  EXPECT_FALSE(partition->Next(&row, nullptr));
  EXPECT_NE(partition->GetStatus(), Status::kSuccess);
}

TEST(ExchangeExecutorTest, DumpShowsTypeAndPartitionCount) {
  auto exchange = std::make_shared<ExchangeExecutor>(Src({IRow(1)}),
                                                     ExchangeType::kBroadcast,
                                                     2);
  std::stringstream ss;
  exchange->Dump(ss, 0);
  EXPECT_NE(ss.str().find("ExchangeExecutor"), std::string::npos);
  EXPECT_NE(ss.str().find("partitions=2"), std::string::npos);
  std::stringstream ss2;
  exchange->Explain(ss2, 0);
  EXPECT_EQ(ss.str(), ss2.str());
}

// ===========================================================================
// SetOperationExecutor direct construction (spill + coercion paths).
// ===========================================================================

TEST(SetOperationExecutorTest, IncompatibleColumnTypesFailSticky) {
  SetOperationExecutor bad({Src({Row({Value::Date("2020-01-01")})}),
                            Src({Row({Value(int64_t{1})})})},
                           SetOperationKind::kUnion);
  Row row;
  EXPECT_FALSE(bad.Next(&row, nullptr));
  EXPECT_NE(bad.GetStatus(), Status::kSuccess);

  SetOperationExecutor width_mismatch(
      {Src({IRow(1)}), Src({Row({Value(1), Value(2)})})},
      SetOperationKind::kUnion);
  EXPECT_FALSE(width_mismatch.Next(&row, nullptr));
  EXPECT_NE(width_mismatch.GetStatus(), Status::kSuccess);

  SetOperationExecutor no_sources({}, SetOperationKind::kUnion);
  EXPECT_FALSE(no_sources.Next(&row, nullptr));
  EXPECT_NE(no_sources.GetStatus(), Status::kSuccess);
}

TEST(SetOperationExecutorTest, DateAndTextUnifyToDateDomain) {
  // A DATE column unioned with a string keeps the DATE domain, and the
  // string side is parsed back into a date.
  SetOperationExecutor op({Src({Row({Value::Date("2020-01-01")})}),
                           Src({Row({Value("2020-01-01")})})},
                          SetOperationKind::kUnion);
  const std::vector<Row> out = Drain(&op);
  ASSERT_EQ(out.size(), 1U);  // same date after coercion
  EXPECT_EQ(out[0][0].type, ValueType::kDate);
  EXPECT_EQ(out[0][0], Value::Date("2020-01-01"));
}

TEST(SetOperationExecutorTest, TextAndNumericUnifyToVarchar) {
  SetOperationExecutor op({Src({Row({Value(int64_t{7})})}),
                           Src({Row({Value("7")})})},
                          SetOperationKind::kUnion);
  const std::vector<Row> out = Drain(&op);
  ASSERT_EQ(out.size(), 1U);
  EXPECT_EQ(out[0][0].type, ValueType::kVarChar);
  EXPECT_EQ(out[0][0], Value("7"));
}

TEST(SetOperationExecutorTest, SpillPathNormalizesIntDoubleAndDedups) {
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 300; ++i) {
    left_rows.push_back(Row({Value(i % 30)}));
    right_rows.push_back(Row({Value(static_cast<double>(i % 30))}));
  }
  BudgetGuard guard(2048);  // non-UNION-ALL operations spill under pressure
  SetOperationExecutor op({Src(left_rows), Src(right_rows)},
                          SetOperationKind::kUnion);
  const std::vector<Row> out = Drain(&op);
  EXPECT_EQ(op.GetStatus(), Status::kSuccess);
  EXPECT_EQ(out.size(), 30U);  // INT64 k and DOUBLE k dedup after coercion
  for (const Row& row : out) {
    EXPECT_EQ(row[0].type, ValueType::kDouble);
  }

  SetOperationExecutor intersect({Src(left_rows), Src(right_rows)},
                                 SetOperationKind::kIntersectAll);
  const std::vector<Row> out2 = Drain(&intersect);
  // Every key appears 10 times per side; INTERSECT ALL keeps min(10, 10).
  EXPECT_EQ(out2.size(), 300U);
}

TEST(SetOperationExecutorTest, SpillPathRejectsWidthMismatch) {
  std::vector<Row> wide_rows;
  std::vector<Row> narrow_rows;
  for (int64_t i = 0; i < 100; ++i) {
    wide_rows.push_back(Row({Value(i), Value(i)}));
    narrow_rows.push_back(Row({Value(i)}));
  }
  BudgetGuard guard(2048);
  SetOperationExecutor op({Src(wide_rows), Src(narrow_rows)},
                          SetOperationKind::kExcept);
  Row row;
  EXPECT_FALSE(op.Next(&row, nullptr));
  EXPECT_NE(op.GetStatus(), Status::kSuccess);
}

TEST(SetOperationExecutorTest, ExceptDistinctKeepsRemovingLaterDuplicates) {
  // 1 appears twice on the left and once on the right: EXCEPT DISTINCT must
  // remove BOTH left copies (the removal stays latched for the whole input).
  SetOperationExecutor op({Src({IRow(1), IRow(1), IRow(2)}), Src({IRow(1)})},
                          SetOperationKind::kExcept);
  const std::vector<Row> out = Drain(&op);
  ASSERT_EQ(out.size(), 1U);
  EXPECT_EQ(out[0][0], Value(2));
}

TEST(SetOperationExecutorTest, DumpListsAllSources) {
  SetOperationExecutor op({Src({IRow(1)}), Src({IRow(2)})},
                          SetOperationKind::kUnionAll);
  std::stringstream ss;
  op.Dump(ss, 0);
  EXPECT_NE(ss.str().find("SetOperation"), std::string::npos);
}

// ===========================================================================
// DataChunk / ColumnVector.
// ===========================================================================

TEST(DataChunkFeatureTest, ColumnVectorInfersTypeAfterLeadingNulls) {
  ColumnVector col;
  col.Append(Value());
  col.Append(Value());
  EXPECT_TRUE(col.IsNull(0));
  col.Append(Value(int64_t{7}));
  col.Append(Value(int64_t{9}));
  ASSERT_EQ(col.Size(), 4U);
  EXPECT_EQ(col.Type(), ValueType::kInt64);
  EXPECT_EQ(col.ValueAt(2), Value(int64_t{7}));
  EXPECT_TRUE(col.IsNull(1));

  col.Reset();
  EXPECT_EQ(col.Size(), 0U);
  col.Append(Value(int64_t{1}));
  EXPECT_EQ(col.ValueAt(0), Value(int64_t{1}));
}

TEST(DataChunkFeatureTest, ColumnVectorSupportsDateDoubleStringArray) {
  ColumnVector dates(ValueType::kDate, 2);
  dates.Append(Value::Date("2021-05-06"));
  dates.Append(Value::Date("1999-12-31"));
  EXPECT_EQ(dates.ValueAt(0), Value::Date("2021-05-06"));

  ColumnVector doubles(ValueType::kDouble, 2);
  doubles.Append(Value(1.5));
  doubles.Append(Value());
  EXPECT_EQ(doubles.ValueAt(0), Value(1.5));
  EXPECT_TRUE(doubles.IsNull(1));

  ColumnVector strings(ValueType::kVarChar, 2);
  strings.Append(Value("hello"));
  strings.Append(Value());
  EXPECT_EQ(strings.ValueAt(0), Value("hello"));

  ColumnVector arrays(ValueType::kArray, 2);
  const Value arr = Value::Array({Value(int64_t{1}), Value(int64_t{2})},
                                 "INT64");
  arrays.Append(arr);
  arrays.Append(Value());
  EXPECT_EQ(arrays.ValueAt(0), arr);
  EXPECT_TRUE(arrays.IsNull(1));

  // AppendFrom copies storage without re-boxing through Value.
  ColumnVector copy(ValueType::kNull, 0);
  copy.AppendFrom(dates, 1);
  copy.AppendFrom(dates, 0);
  EXPECT_EQ(copy.ValueAt(0), Value::Date("1999-12-31"));
  EXPECT_EQ(copy.ValueAt(1), Value::Date("2021-05-06"));
}

TEST(DataChunkFeatureTest, UnsignedIntColumnsRoundTrip) {
  Column unsigned_col("x", ValueType::kInt64);
  unsigned_col.SetUnsigned(true);
  const Schema schema("t", {unsigned_col});
  DataChunk chunk(schema, 4);
  chunk.Append(Row({Value(uint64_t{18446744073709551615ULL}).WithUnsigned()}));
  chunk.Append(Row({Value(int64_t{5}).WithUnsigned()}));
  EXPECT_TRUE(chunk.ColumnAt(0).IsUnsigned());
  EXPECT_TRUE(chunk.ColumnAt(0).ValueAt(0).IsUnsigned());
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(0),
            Value(uint64_t{18446744073709551615ULL}).WithUnsigned());

  // Bitwise aggregates report unsigned results too.
  EXPECT_TRUE(chunk.AggregateBitAnd(0).IsUnsigned());
  EXPECT_EQ(chunk.AggregateBitAnd(0), Value(int64_t{5}).WithUnsigned());
  EXPECT_TRUE(chunk.AggregateBitOr(0).IsUnsigned());
  EXPECT_TRUE(chunk.AggregateBitXor(0).IsUnsigned());
}

TEST(DataChunkFeatureTest, AppendChunkCopiesZoneMapsAcrossTypes) {
  const Schema src_schema(
      "src", {Column("i", ValueType::kInt64), Column("d", ValueType::kDouble),
              Column("s", ValueType::kVarChar), Column("dt", ValueType::kDate),
              Column("arr", ValueType::kArray)});
  DataChunk src(src_schema, 4);
  src.Append(Row({Value(int64_t{1}), Value(0.5), Value("a"),
                  Value::Date("2020-01-01"),
                  Value::Array({Value(int64_t{9})}, "INT64")}));
  src.Append(Row({Value(), Value(), Value(), Value(), Value()}));

  DataChunk dst;
  dst.Append(src, 0);
  dst.Append(src, 1);
  ASSERT_EQ(dst.Size(), 2U);
  EXPECT_EQ(dst.ColumnAt(0).ValueAt(0), Value(int64_t{1}));
  EXPECT_TRUE(dst.ColumnAt(1).IsNull(1));
  EXPECT_TRUE(dst.ColumnAt(4).IsNull(1));
  EXPECT_EQ(dst.PositionAt(0).page_id, src.PositionAt(0).page_id);
  EXPECT_TRUE(dst.HasLayout(src_schema));
}

TEST(DataChunkFeatureTest, AppendRowFromColumnsJoinsAcrossChunks) {
  const Schema schema(
      "t", {Column("a", ValueType::kInt64), Column("b", ValueType::kVarChar)});
  DataChunk c1(schema, 4);
  c1.Append(Row({Value(int64_t{1}), Value("x")}));
  DataChunk c2(schema, 4);
  c2.Append(Row({Value(int64_t{2}), Value("y")}));

  DataChunk out;
  const std::vector<const ColumnVector*> sources = {&c1.ColumnAt(0),
                                                    &c2.ColumnAt(1)};
  out.AppendRowFromColumns(sources, 0, RowPosition());
  ASSERT_EQ(out.Size(), 1U);
  EXPECT_EQ(out.RowAt(0), (Row({Value(int64_t{1}), Value("y")})));
}

TEST(DataChunkFeatureTest, AppendGatherSelectsRowsAndPositions) {
  const Schema schema("t", {Column("a", ValueType::kInt64)});
  DataChunk src(schema, 8);
  src.Append(Row({Value(int64_t{10})}), RowPosition(7, 1));
  src.Append(Row({Value(int64_t{20})}), RowPosition(9, 2));
  src.Append(Row({Value(int64_t{30})}));

  DataChunk dst;
  const std::vector<uint32_t> selection = {2, 0};
  dst.AppendGather(src, selection.data(), selection.size());
  ASSERT_EQ(dst.Size(), 2U);
  EXPECT_EQ(dst.RowAt(0), (Row({Value(int64_t{30})})));
  EXPECT_EQ(dst.RowAt(1), (Row({Value(int64_t{10})})));
  EXPECT_EQ(dst.PositionAt(1).page_id, 7);
  EXPECT_EQ(dst.PositionAt(1).slot, 1);
}

TEST(DataChunkFeatureTest, ResetWithSchemaReinitializesLayout) {
  const Schema schema("t", {Column("a", ValueType::kInt64)});
  DataChunk chunk(schema, 4);
  chunk.Append(IRow(1));
  chunk.Reset(schema, 8);
  EXPECT_EQ(chunk.Size(), 0U);
  EXPECT_EQ(chunk.ColumnCount(), 1U);
  chunk.Append(IRow(2));
  EXPECT_EQ(chunk.RowAt(0), (Row({Value(int64_t{2})})));
  // HasLayout tolerates an inferred kNull-typed column.
  DataChunk inferred;
  inferred.Append(Row({Value(int64_t{3})}));
  EXPECT_TRUE(inferred.HasLayout(schema));
  EXPECT_FALSE(inferred.HasLayout(Schema(
      "u", {Column("a", ValueType::kVarChar), Column("b", ValueType::kInt64)})));
}

TEST(DataChunkFeatureTest, BitAggregatesHandleNullWordsAndSelections) {
  const Schema schema("t", {Column("v", ValueType::kInt64)});
  DataChunk chunk(schema, 70);
  // First word (rows 0..63): all NULL.  Second word: 0b1111, NULL, 0b0011.
  for (int64_t i = 0; i < 64; ++i) {
    chunk.Append(Row({Value()}));
  }
  chunk.Append(Row({Value(int64_t{0b1111})}));
  chunk.Append(Row({Value()}));
  chunk.Append(Row({Value(int64_t{0b0011})}));
  ASSERT_EQ(chunk.Size(), 67U);

  // The all-NULL word is skipped; the mixed word contributes non-NULLs only.
  EXPECT_EQ(chunk.AggregateBitAnd(0), Value(int64_t{0b0011}));
  EXPECT_EQ(chunk.AggregateBitOr(0), Value(int64_t{0b1111}));
  EXPECT_EQ(chunk.AggregateBitXor(0), Value(int64_t{0b1100}));
  // Every value NULL -> NULL aggregate.
  DataChunk all_null(schema, 3);
  all_null.Append(Row({Value()}));
  all_null.Append(Row({Value()}));
  EXPECT_TRUE(all_null.AggregateBitAnd(0).IsNull());
  EXPECT_TRUE(all_null.AggregateBitOr(0).IsNull());
  EXPECT_TRUE(all_null.AggregateBitXor(0).IsNull());
  EXPECT_TRUE(all_null.AggregateLogicalAnd(0).IsNull());
  EXPECT_TRUE(all_null.AggregateLogicalOr(0).IsNull());

  // Selection vector: out-of-range entries are ignored.
  SelectionVector sel(std::vector<uint32_t>{64U, 66U, 1000U});
  EXPECT_EQ(chunk.AggregateBitAnd(0, &sel), Value(int64_t{0b0011}));
  EXPECT_EQ(chunk.AggregateBitOr(0, &sel), Value(int64_t{0b1111}));
  EXPECT_EQ(chunk.AggregateBitXor(0, &sel), Value(int64_t{0b1100}));

  // Logical aggregates: both non-NULL rows (0b1111, 0b0011) are truthy, so
  // AND is TRUE and OR is TRUE.
  EXPECT_EQ(chunk.AggregateLogicalAnd(0), Value(int64_t{1}));
  EXPECT_EQ(chunk.AggregateLogicalOr(0), Value(int64_t{1}));
}

TEST(DataChunkFeatureTest, LogicalAggregatesOnNonIntColumnUseTruthyPath) {
  const Schema schema("t", {Column("d", ValueType::kDouble)});
  DataChunk chunk(schema, 3);
  chunk.Append(Row({Value(0.5)}));  // truthy
  chunk.Append(Row({Value(0.0)}));  // falsy
  chunk.Append(Row({Value()}));     // NULL
  EXPECT_EQ(chunk.AggregateLogicalAnd(0), Value(int64_t{0}));
  EXPECT_EQ(chunk.AggregateLogicalOr(0), Value(int64_t{1}));

  DataChunk all_truthy(schema, 2);
  all_truthy.Append(Row({Value(1.5)}));
  all_truthy.Append(Row({Value(2.5)}));
  EXPECT_EQ(all_truthy.AggregateLogicalAnd(0), Value(int64_t{1}));
  EXPECT_EQ(all_truthy.AggregateLogicalOr(0), Value(int64_t{1}));

  // With a selection vector over the falsy row.
  SelectionVector sel(std::vector<uint32_t>{1U});
  EXPECT_EQ(chunk.AggregateLogicalOr(0, &sel), Value(int64_t{0}));
}

// ===========================================================================
// VectorizedExpression.
// ===========================================================================

namespace {

const Schema kExprSchema("t", {Column("a", ValueType::kInt64),
                               Column("b", ValueType::kInt64),
                               Column("s", ValueType::kVarChar)});

DataChunk MakeExprChunk() {
  DataChunk chunk(kExprSchema, 8);
  chunk.Append(Row({Value(int64_t{0}), Value(int64_t{0}), Value("x0")}));
  chunk.Append(Row({Value(int64_t{1}), Value(), Value("x1")}));
  chunk.Append(Row({Value(), Value(int64_t{3}), Value("x2")}));
  chunk.Append(Row({Value(int64_t{2}), Value(int64_t{4}), Value("x3")}));
  return chunk;
}

}  // namespace

TEST(VectorizedExpressionFeatureTest, NullExpressionAndEmptyChunk) {
  const DataChunk chunk = MakeExprChunk();
  auto empty =
      VectorizedExpression::Evaluate(Expression(), kExprSchema, chunk);
  ASSERT_TRUE(empty.HasValue());
  EXPECT_EQ(empty.Value().Size(), 0U);

  DataChunk nothing(kExprSchema, 4);
  auto on_empty =
      VectorizedExpression::Evaluate(ColumnValueExp("a"), kExprSchema, nothing);
  ASSERT_TRUE(on_empty.HasValue());
  EXPECT_EQ(on_empty.Value().Size(), 0U);

  auto filter = VectorizedExpression::EvaluateFilter(ColumnValueExp("a"),
                                                     kExprSchema, nothing);
  ASSERT_TRUE(filter.HasValue());
  EXPECT_EQ(filter.Value().Size(), 0U);
}

TEST(VectorizedExpressionFeatureTest, ColumnAndConstantWithSelection) {
  const DataChunk chunk = MakeExprChunk();
  auto full =
      VectorizedExpression::Evaluate(ColumnValueExp("a"), kExprSchema, chunk);
  ASSERT_TRUE(full.HasValue());
  ASSERT_EQ(full.Value().Size(), 4U);
  EXPECT_EQ(full.Value().ValueAt(0), Value(int64_t{0}));
  EXPECT_TRUE(full.Value().IsNull(2));

  SelectionVector sel({1U, 3U});
  auto sliced = VectorizedExpression::Evaluate(ColumnValueExp("a"), kExprSchema,
                                               chunk, &sel);
  ASSERT_TRUE(sliced.HasValue());
  ASSERT_EQ(sliced.Value().Size(), 2U);
  EXPECT_EQ(sliced.Value().ValueAt(0), Value(int64_t{1}));
  EXPECT_EQ(sliced.Value().ValueAt(1), Value(int64_t{2}));

  auto constants = VectorizedExpression::Evaluate(
      ConstantValueExp(Value(int64_t{9})), kExprSchema, chunk, &sel);
  ASSERT_TRUE(constants.HasValue());
  EXPECT_EQ(constants.Value().ValueAt(0), Value(int64_t{9}));
  EXPECT_EQ(constants.Value().ValueAt(1), Value(int64_t{9}));
}

TEST(VectorizedExpressionFeatureTest, UnaryEvaluationWithNulls) {
  const DataChunk chunk = MakeExprChunk();
  auto negated = VectorizedExpression::Evaluate(
      UnaryExpressionExp(ColumnValueExp("a"), UnaryOperation::kMinus),
      kExprSchema, chunk);
  ASSERT_TRUE(negated.HasValue());
  ASSERT_EQ(negated.Value().Size(), 4U);
  EXPECT_EQ(negated.Value().ValueAt(0), Value(int64_t{0}));
  EXPECT_EQ(negated.Value().ValueAt(1), Value(int64_t{-1}));
  EXPECT_TRUE(negated.Value().IsNull(2));
  EXPECT_EQ(negated.Value().ValueAt(3), Value(int64_t{-2}));

  auto not_exp = VectorizedExpression::Evaluate(
      UnaryExpressionExp(ColumnValueExp("a"), UnaryOperation::kNot),
      kExprSchema, chunk);
  ASSERT_TRUE(not_exp.HasValue());
  EXPECT_EQ(not_exp.Value().ValueAt(0), Value(true));
  EXPECT_EQ(not_exp.Value().ValueAt(1), Value(false));
  EXPECT_TRUE(not_exp.Value().IsNull(2));
  EXPECT_EQ(not_exp.Value().ValueAt(3), Value(false));
}

TEST(VectorizedExpressionFeatureTest, AndOrShortCircuitSuppressesRhsErrors) {
  const Schema schema(
      "t", {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64)});
  const Expression lhs = BinaryExpressionExp(
      ColumnValueExp("a"), BinaryOperation::kEquals,
      ConstantValueExp(Value(int64_t{1})));
  // 1 / b >= 0: raises for b = 0, TRUE otherwise (integer division).
  const Expression rhs = BinaryExpressionExp(
      BinaryExpressionExp(ConstantValueExp(Value(int64_t{1})),
                          BinaryOperation::kDivide, ColumnValueExp("b")),
      BinaryOperation::kGreaterThanEquals,
      ConstantValueExp(Value(int64_t{0})));

  DataChunk chunk(schema, 4);
  // lhs FALSE decides AND: the 1/0 rhs error on this row is suppressed.
  chunk.Append(Row({Value(int64_t{0}), Value(int64_t{0})}));
  // lhs TRUE: rhs evaluated -> TRUE.
  chunk.Append(Row({Value(int64_t{1}), Value(int64_t{2})}));
  // lhs TRUE with rhs 1/0: the error must propagate.
  chunk.Append(Row({Value(int64_t{1}), Value(int64_t{0})}));

  const Expression both =
      BinaryExpressionExp(lhs, BinaryOperation::kAnd, rhs);
  auto bad = VectorizedExpression::Evaluate(both, schema, chunk);
  EXPECT_FALSE(bad.HasValue());

  // OR mirror: lhs TRUE decides, suppressing the rhs error.
  DataChunk or_chunk(schema, 3);
  or_chunk.Append(Row({Value(int64_t{1}), Value(int64_t{0})}));
  or_chunk.Append(Row({Value(int64_t{0}), Value(int64_t{2})}));
  const Expression or_expr =
      BinaryExpressionExp(lhs, BinaryOperation::kOr, rhs);
  auto good = VectorizedExpression::Evaluate(or_expr, schema, or_chunk);
  ASSERT_TRUE(good.HasValue());
  ASSERT_EQ(good.Value().Size(), 2U);
  EXPECT_EQ(good.Value().ValueAt(0), Value(int64_t{1}));
  EXPECT_EQ(good.Value().ValueAt(1), Value(int64_t{1}));

  // lhs FALSE on OR evaluates rhs: 1/0 errors.
  DataChunk err_chunk(schema, 1);
  err_chunk.Append(Row({Value(int64_t{0}), Value(int64_t{0})}));
  auto err = VectorizedExpression::Evaluate(or_expr, schema, err_chunk);
  EXPECT_FALSE(err.HasValue());
}

TEST(VectorizedExpressionFeatureTest, AndOrWithSelectionVector) {
  const Schema schema(
      "t", {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64)});
  DataChunk chunk(schema, 4);
  chunk.Append(Row({Value(int64_t{1}), Value(int64_t{1})}));  // not selected
  chunk.Append(Row({Value(int64_t{0}), Value(int64_t{0})}));  // FALSE AND ...
  chunk.Append(Row({Value(), Value(int64_t{1})}));            // NULL AND TRUE
  chunk.Append(Row({Value(int64_t{1}), Value(int64_t{1})}));  // TRUE AND TRUE
  SelectionVector sel({1U, 2U, 3U});
  const Expression both = BinaryExpressionExp(
      ColumnValueExp("a"), BinaryOperation::kAnd, ColumnValueExp("b"));
  auto res = VectorizedExpression::Evaluate(both, schema, chunk, &sel);
  ASSERT_TRUE(res.HasValue());
  ASSERT_EQ(res.Value().Size(), 3U);
  EXPECT_EQ(res.Value().ValueAt(0), Value(int64_t{0}));
  EXPECT_TRUE(res.Value().ValueAt(1).IsNull());
  EXPECT_EQ(res.Value().ValueAt(2), Value(int64_t{1}));
}

TEST(VectorizedExpressionFeatureTest, Int64XorFollowsThreeValuedLogic) {
  const DataChunk chunk = MakeExprChunk();
  const Expression xor_expr = BinaryExpressionExp(
      ColumnValueExp("a"), BinaryOperation::kXor, ColumnValueExp("b"));
  auto res =
      VectorizedExpression::Evaluate(xor_expr, kExprSchema, chunk);
  ASSERT_TRUE(res.HasValue());
  ASSERT_EQ(res.Value().Size(), 4U);
  // 0^0 -> FALSE, 1^NULL -> NULL, NULL^3 -> NULL, truthy^truthy -> FALSE.
  EXPECT_EQ(res.Value().ValueAt(0), Value(int64_t{0}));
  EXPECT_TRUE(res.Value().ValueAt(1).IsNull());
  EXPECT_TRUE(res.Value().ValueAt(2).IsNull());
  EXPECT_EQ(res.Value().ValueAt(3), Value(int64_t{0}));
}

TEST(VectorizedExpressionFeatureTest, CastEvaluatesPerRowAndKeepsNulls) {
  const DataChunk chunk = MakeExprChunk();
  auto casted = VectorizedExpression::Evaluate(
      std::make_shared<CastExpression>(ColumnValueExp("a"), "VARCHAR"),
      kExprSchema, chunk);
  ASSERT_TRUE(casted.HasValue());
  ASSERT_EQ(casted.Value().Size(), 4U);
  EXPECT_EQ(casted.Value().ValueAt(0).type, ValueType::kVarChar);
  EXPECT_EQ(casted.Value().ValueAt(0), Value("0"));
  EXPECT_EQ(casted.Value().ValueAt(1), Value("1"));
  EXPECT_TRUE(casted.Value().IsNull(2));

  // Casting NULL stays NULL.
  auto cast_null = VectorizedExpression::Evaluate(
      std::make_shared<CastExpression>(ColumnValueExp("s"), "VARCHAR"),
      kExprSchema, chunk);
  ASSERT_TRUE(cast_null.HasValue());
  EXPECT_EQ(cast_null.Value().ValueAt(0), Value("x0"));
  EXPECT_TRUE(cast_null.Value().IsNull(2));
}

TEST(VectorizedExpressionFeatureTest, CaseExpressionFallsToPerRowPath) {
  const DataChunk chunk = MakeExprChunk();
  const Expression when = BinaryExpressionExp(
      ColumnValueExp("a"), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(int64_t{0})));
  auto res = VectorizedExpression::Evaluate(
      CaseExpressionExp({{when, ConstantValueExp(Value("pos"))}},
                        ConstantValueExp(Value("nonpos"))),
      kExprSchema, chunk);
  ASSERT_TRUE(res.HasValue());
  ASSERT_EQ(res.Value().Size(), 4U);
  EXPECT_EQ(res.Value().ValueAt(0), Value("nonpos"));
  EXPECT_EQ(res.Value().ValueAt(1), Value("pos"));
  EXPECT_EQ(res.Value().ValueAt(2), Value("nonpos"));  // NULL a -> ELSE
  EXPECT_EQ(res.Value().ValueAt(3), Value("pos"));
}

TEST(VectorizedExpressionFeatureTest, UnhandledTagsTakeRowFallback) {
  const DataChunk chunk = MakeExprChunk();
  auto res = VectorizedExpression::Evaluate(
      InExpressionExp(ColumnValueExp("a"),
                      {ConstantValueExp(Value(int64_t{0})),
                       ConstantValueExp(Value(int64_t{2}))}),
      kExprSchema, chunk);
  ASSERT_TRUE(res.HasValue());
  ASSERT_EQ(res.Value().Size(), 4U);
  EXPECT_EQ(res.Value().ValueAt(0), Value(true));
  EXPECT_EQ(res.Value().ValueAt(1), Value(false));
  EXPECT_TRUE(res.Value().ValueAt(2).IsNull());
  EXPECT_EQ(res.Value().ValueAt(3), Value(true));
}

TEST(VectorizedExpressionFeatureTest, EvaluateFilterHonorsSelection) {
  const DataChunk chunk = MakeExprChunk();
  const Expression cmp = BinaryExpressionExp(
      ColumnValueExp("a"), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(int64_t{0})));

  // Full chunk: rows 1 and 3 pass.
  auto full = VectorizedExpression::EvaluateFilter(cmp, kExprSchema, chunk);
  ASSERT_TRUE(full.HasValue());
  ASSERT_EQ(full.Value().Size(), 4U);
  EXPECT_FALSE(full.Value().Get(0));
  EXPECT_TRUE(full.Value().Get(1));
  EXPECT_FALSE(full.Value().Get(2));
  EXPECT_TRUE(full.Value().Get(3));

  // Empty selection short-circuits to an all-false bitmap.
  SelectionVector empty_sel;
  auto none =
      VectorizedExpression::EvaluateFilter(cmp, kExprSchema, chunk, &empty_sel);
  ASSERT_TRUE(none.HasValue());
  ASSERT_EQ(none.Value().Size(), 4U);
  EXPECT_EQ(none.Value().CountTrue(), 0U);

  // Selection restricts both evaluation and the output bitmap.
  SelectionVector sel({0U, 1U});
  auto sliced =
      VectorizedExpression::EvaluateFilter(cmp, kExprSchema, chunk, &sel);
  ASSERT_TRUE(sliced.HasValue());
  EXPECT_FALSE(sliced.Value().Get(0));
  EXPECT_TRUE(sliced.Value().Get(1));
  EXPECT_FALSE(sliced.Value().Get(2));
  EXPECT_FALSE(sliced.Value().Get(3));
}

TEST(VectorizedExpressionFeatureTest, FilterDataChunkChainsSelectionVectors) {
  const DataChunk chunk = MakeExprChunk();
  const Expression cmp = BinaryExpressionExp(
      ColumnValueExp("a"), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(int64_t{0})));

  SelectionVector input({0U, 1U, 2U, 3U});
  SelectionVector output;
  const Status st = VectorizedExpression::FilterDataChunk(
      cmp, kExprSchema, chunk, &output, &input);
  EXPECT_EQ(st, Status::kSuccess);
  ASSERT_EQ(output.Size(), 2U);
  EXPECT_EQ(output[0], 1U);
  EXPECT_EQ(output[1], 3U);

  // No input selection: the bitmap feeds the output directly.
  SelectionVector plain;
  EXPECT_EQ(
      VectorizedExpression::FilterDataChunk(cmp, kExprSchema, chunk, &plain),
      Status::kSuccess);
  ASSERT_EQ(plain.Size(), 2U);
}

TEST(VectorizedExpressionFeatureTest, AggregateDispatchAndWrappers) {
  const Schema schema("t", {Column("v", ValueType::kInt64)});
  DataChunk chunk(schema, 4);
  chunk.Append(Row({Value(int64_t{0b1100})}));
  chunk.Append(Row({Value(int64_t{0b0110})}));
  chunk.Append(Row({Value()}));

  EXPECT_EQ(VectorizedExpression::Aggregate(AggregationType::kBitAnd,
                                            chunk.ColumnAt(0)),
            Value(int64_t{0b0100}));
  EXPECT_EQ(VectorizedExpression::Aggregate(AggregationType::kBitOr,
                                            chunk.ColumnAt(0)),
            Value(int64_t{0b1110}));
  EXPECT_EQ(VectorizedExpression::Aggregate(AggregationType::kBitXor,
                                            chunk.ColumnAt(0)),
            Value(int64_t{0b1010}));
  EXPECT_EQ(VectorizedExpression::Aggregate(AggregationType::kLogicalAnd,
                                            chunk.ColumnAt(0)),
            Value(int64_t{1}));
  EXPECT_EQ(VectorizedExpression::Aggregate(AggregationType::kLogicalOr,
                                            chunk.ColumnAt(0)),
            Value(int64_t{1}));

  SelectionVector sel(std::vector<uint32_t>{0U});
  EXPECT_EQ(VectorizedExpression::AggregateLogicalAnd(chunk.ColumnAt(0), &sel),
            Value(int64_t{1}));
  EXPECT_EQ(VectorizedExpression::AggregateLogicalOr(chunk.ColumnAt(0), &sel),
            Value(int64_t{1}));
  EXPECT_EQ(VectorizedExpression::AggregateBitAnd(chunk.ColumnAt(0), &sel),
            Value(int64_t{0b1100}));
  EXPECT_EQ(VectorizedExpression::AggregateBitOr(chunk.ColumnAt(0), &sel),
            Value(int64_t{0b1100}));
  EXPECT_EQ(VectorizedExpression::AggregateBitXor(chunk.ColumnAt(0), &sel),
            Value(int64_t{0b1100}));
}

// ===========================================================================
// SimdComparisonKernel: scalar/vector comparisons across all operators.
// ===========================================================================

TEST(SimdComparisonKernelTest, Int64ScalarCoversAllOperatorsAcrossWords) {
  std::vector<int64_t> data;
  data.reserve(130);
  for (size_t i = 0; i < 130; ++i) {
    data.push_back(static_cast<int64_t>(i));  // crosses the 64-row word
  }
  const int64_t target = 64;
  struct Case {
    BinaryOperation op;
    size_t expected;
  };
  const std::vector<Case> cases = {
      {BinaryOperation::kEquals, 1},
      {BinaryOperation::kNotEquals, 129},
      {BinaryOperation::kLessThan, 64},
      {BinaryOperation::kLessThanEquals, 65},
      {BinaryOperation::kGreaterThan, 65},
      {BinaryOperation::kGreaterThanEquals, 66},
  };
  for (const Case& c : cases) {
    ValidityBitmap mask;
    SimdComparisonKernel::CompareInt64(data.data(), data.size(), c.op, target,
                                       &mask);
    EXPECT_EQ(mask.CountTrue(), c.expected) << static_cast<int>(c.op);
    EXPECT_EQ(mask.Size(), data.size());
  }
  // Spot-check positions.
  ValidityBitmap mask;
  SimdComparisonKernel::CompareInt64(data.data(), data.size(),
                                     BinaryOperation::kEquals, 70, &mask);
  EXPECT_FALSE(mask.Get(69));
  EXPECT_TRUE(mask.Get(70));
  EXPECT_FALSE(mask.Get(71));

  // An unsupported op clears the mask.
  ValidityBitmap cleared;
  SimdComparisonKernel::CompareInt64(data.data(), data.size(),
                                     BinaryOperation::kAdd, 0, &cleared);
  EXPECT_EQ(cleared.CountTrue(), 0U);

  // count = 0 yields an empty mask.
  ValidityBitmap empty_mask;
  SimdComparisonKernel::CompareInt64(data.data(), 0, BinaryOperation::kEquals,
                                     0, &empty_mask);
  EXPECT_EQ(empty_mask.Size(), 0U);
}

TEST(SimdComparisonKernelTest, DoubleScalarCoversAllOperators) {
  const std::vector<double> data = {1.0, 2.5, -3.0, 2.5, 0.0, 7.0};
  struct Case {
    BinaryOperation op;
    double target;
    size_t expected;
  };
  const std::vector<Case> cases = {
      {BinaryOperation::kEquals, 2.5, 2},
      {BinaryOperation::kNotEquals, 2.5, 4},
      {BinaryOperation::kLessThan, 2.5, 3},
      {BinaryOperation::kLessThanEquals, 2.5, 5},
      {BinaryOperation::kGreaterThan, 2.5, 1},
      {BinaryOperation::kGreaterThanEquals, 2.5, 3},
  };
  for (const Case& c : cases) {
    ValidityBitmap mask;
    SimdComparisonKernel::CompareDouble(data.data(), data.size(), c.op,
                                        c.target, &mask);
    EXPECT_EQ(mask.CountTrue(), c.expected) << static_cast<int>(c.op);
  }
  ValidityBitmap cleared;
  SimdComparisonKernel::CompareDouble(data.data(), data.size(),
                                      BinaryOperation::kMultiply, 0.0,
                                      &cleared);
  EXPECT_EQ(cleared.CountTrue(), 0U);
}

TEST(SimdComparisonKernelTest, StringPrefixCoversAllOperators) {
  const std::vector<std::string_view> data = {"apple", "banana", "cherry",
                                              "banana", "date"};
  struct Case {
    BinaryOperation op;
    size_t expected;
  };
  const std::vector<Case> cases = {
      {BinaryOperation::kEquals, 2},
      {BinaryOperation::kNotEquals, 3},
      {BinaryOperation::kLessThan, 1},
      {BinaryOperation::kLessThanEquals, 3},
      {BinaryOperation::kGreaterThan, 2},
      {BinaryOperation::kGreaterThanEquals, 4},
  };
  for (const Case& c : cases) {
    ValidityBitmap mask;
    SimdComparisonKernel::CompareStringPrefix(data.data(), data.size(), c.op,
                                              "banana", &mask);
    EXPECT_EQ(mask.CountTrue(), c.expected) << static_cast<int>(c.op);
  }
  ValidityBitmap cleared;
  SimdComparisonKernel::CompareStringPrefix(data.data(), data.size(),
                                            BinaryOperation::kAdd, "banana",
                                            &cleared);
  EXPECT_EQ(cleared.CountTrue(), 0U);
}

TEST(SimdComparisonKernelTest, VectorComparisonsMatchPairwiseSemantics) {
  const std::vector<int64_t> lhs = {1, 5, 3, 9, 2};
  const std::vector<int64_t> rhs = {2, 5, 4, 1, 3};
  struct Case {
    BinaryOperation op;
    size_t expected;
  };
  const std::vector<Case> cases = {
      {BinaryOperation::kEquals, 1},
      {BinaryOperation::kNotEquals, 4},
      {BinaryOperation::kLessThan, 3},
      {BinaryOperation::kLessThanEquals, 4},
      {BinaryOperation::kGreaterThan, 1},
      {BinaryOperation::kGreaterThanEquals, 2},
  };
  for (const Case& c : cases) {
    ValidityBitmap mask;
    SimdComparisonKernel::CompareInt64Vectors(lhs.data(), rhs.data(),
                                              lhs.size(), c.op, &mask);
    EXPECT_EQ(mask.CountTrue(), c.expected) << static_cast<int>(c.op);
  }
  ValidityBitmap cleared;
  SimdComparisonKernel::CompareInt64Vectors(lhs.data(), rhs.data(), lhs.size(),
                                            BinaryOperation::kModulo, &cleared);
  EXPECT_EQ(cleared.CountTrue(), 0U);

  const std::vector<double> dl = {1.5, -2.0, 0.0};
  const std::vector<double> dr = {1.5, 2.0, -0.0};
  ValidityBitmap dmask;
  SimdComparisonKernel::CompareDoubleVectors(dl.data(), dr.data(), dl.size(),
                                             BinaryOperation::kEquals, &dmask);
  EXPECT_EQ(dmask.CountTrue(), 2U);  // 1.5 == 1.5 and 0.0 == -0.0
  EXPECT_TRUE(dmask.Get(0));
  EXPECT_TRUE(dmask.Get(2));

  ValidityBitmap dcleared;
  SimdComparisonKernel::CompareDoubleVectors(dl.data(), dr.data(), dl.size(),
                                             BinaryOperation::kDivide,
                                             &dcleared);
  EXPECT_EQ(dcleared.CountTrue(), 0U);
}

// ===========================================================================
// SkipScanDistinct over a real table + index.
// ===========================================================================

class SkipScanDistinctTest : public ::testing::Test {
 protected:
  void SetUp() override {
    database_ = Database::Create("executor_feature_test").MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  }
  void TearDown() override {
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  // Creates the table, inserts rows, backfills an index over them, commits,
  // and opens a fresh transaction for the scan.
  void SetupTable(const Schema& schema, const IndexSchema& index_schema,
                  const std::vector<Row>& rows) {
    Table tbl = database_->CreateTable(*context_, schema).MoveValue();
    for (const Row& row : rows) {
      EXPECT_TRUE(tbl.Insert(context_->txn_, row).HasValue());
    }
    EXPECT_EQ(database_->CreateIndex(*context_, schema.Name(), index_schema),
              Status::kSuccess);
    EXPECT_EQ(context_->PreCommit(), Status::kSuccess);
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
};

TEST_F(SkipScanDistinctTest, DistinctOverIndexedColumnSkipsDuplicates) {
  const Schema schema(
      "t", {Column("a", ValueType::kInt64), Column("b", ValueType::kVarChar)});
  std::vector<Row> rows;
  for (int64_t round = 0; round < 3; ++round) {
    for (int64_t k = 1; k <= 20; ++k) {
      rows.push_back(Row({Value(k), Value("v" + std::to_string(k))}));
    }
  }
  SetupTable(schema, IndexSchema("t_a_idx", {0}, {}, IndexMode::kNonUnique),
             rows);
  Table table = database_->GetTable(*context_, "t").MoveValue();
  ASSERT_GE(table.IndexCount(), 1U);
  const Index& index = table.GetIndex(0);

  SkipScanDistinct scan(context_->txn_, table, index, std::vector<Value>{},
                        std::vector<Value>{}, /*ascending=*/true,
                        ConstantValueExp(Value(true)), table.GetSchema(),
                        /*prefix_cols=*/0);
  const std::vector<Row> scan_rows = Drain(&scan);
  ASSERT_EQ(scan_rows.size(), 20U);
  for (size_t i = 0; i < scan_rows.size(); ++i) {
    EXPECT_EQ(scan_rows[i][0], Value(static_cast<int64_t>(i + 1)));
  }
  EXPECT_EQ(scan.GetStatus(), Status::kSuccess);
  std::stringstream ss;
  scan.Dump(ss, 0);
  EXPECT_NE(ss.str().find("SkipScanDistinct"), std::string::npos);

  // Descending walk yields the reversed distinct set.
  SkipScanDistinct descending(context_->txn_, table, index, std::vector<Value>{},
                              std::vector<Value>{}, false,
                              ConstantValueExp(Value(true)),
                              table.GetSchema(), 0);
  const std::vector<Row> down = Drain(&descending);
  ASSERT_EQ(down.size(), 20U);
  EXPECT_EQ(down[0][0], Value(int64_t{20}));
  EXPECT_EQ(down[19][0], Value(int64_t{1}));

  // A WHERE filter drops some distinct keys.
  const Expression where =
      BinaryExpressionExp(ColumnValueExp(ColumnName("t", "a")),
                          BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(int64_t{15})));
  SkipScanDistinct filtered(context_->txn_, table, index, std::vector<Value>{},
                            std::vector<Value>{}, true, where,
                            table.GetSchema(), 0);
  const std::vector<Row> kept = Drain(&filtered);
  ASSERT_EQ(kept.size(), 5U);
  EXPECT_EQ(kept[0][0], Value(int64_t{16}));

  // Batched consumer agreement.
  SkipScanDistinct batched(context_->txn_, table, index, std::vector<Value>{},
                           std::vector<Value>{}, true,
                           ConstantValueExp(Value(true)), table.GetSchema(), 0);
  DataChunk chunk(table.GetSchema(), 8);
  size_t total = 0;
  for (;;) {
    const size_t got = batched.NextBatch(&chunk, 6);
    if (got == 0) {
      break;
    }
    total += got;
  }
  EXPECT_EQ(total, 20U);
}

TEST_F(SkipScanDistinctTest, PrefixDistinctJumpsCompoundIndexGroups) {
  const Schema schema(
      "pt", {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64)});
  std::vector<Row> rows;
  for (int64_t a = 1; a <= 5; ++a) {
    for (int64_t b = 1; b <= 4; ++b) {
      rows.push_back(Row({Value(a), Value(b)}));
    }
  }
  SetupTable(schema,
             IndexSchema("pt_ab_idx", {0, 1}, {}, IndexMode::kNonUnique),
             rows);
  Table table = database_->GetTable(*context_, "pt").MoveValue();
  const Index& index = table.GetIndex(0);

  SkipScanDistinct ascending(context_->txn_, table, index, std::vector<Value>{},
                             std::vector<Value>{}, true,
                             ConstantValueExp(Value(true)),
                             table.GetSchema(), /*prefix_cols=*/1);
  const std::vector<Row> up = Drain(&ascending);
  ASSERT_EQ(up.size(), 5U);
  for (size_t i = 0; i < up.size(); ++i) {
    EXPECT_EQ(up[i][0], Value(static_cast<int64_t>(i + 1)));
  }

  SkipScanDistinct descending(context_->txn_, table, index,
                              std::vector<Value>{}, std::vector<Value>{}, false,
                              ConstantValueExp(Value(true)),
                              table.GetSchema(), 1);
  const std::vector<Row> down = Drain(&descending);
  ASSERT_EQ(down.size(), 5U);
  EXPECT_EQ(down[0][0], Value(int64_t{5}));
  EXPECT_EQ(down[4][0], Value(int64_t{1}));
}

TEST_F(SkipScanDistinctTest, UniqueIndexNullKeysDecodeAsMultiValue) {
  const Schema schema("ut", {Column("a", ValueType::kInt64)});
  std::vector<Row> rows = {IRow(1), IRow(2), Row({Value()}), Row({Value()})};
  SetupTable(schema, IndexSchema("ut_a_idx", {0}, {}, IndexMode::kUnique),
             rows);
  Table table = database_->GetTable(*context_, "ut").MoveValue();
  const Index& index = table.GetIndex(0);

  SkipScanDistinct scan(context_->txn_, table, index, std::vector<Value>{},
                        std::vector<Value>{}, true, ConstantValueExp(Value(true)),
                        table.GetSchema(), 0);
  const std::vector<Row> out = Drain(&scan);
  EXPECT_EQ(scan.GetStatus(), Status::kSuccess);
  // DISTINCT sees keys {NULL, 1, 2}; how many rows the two NULL entries
  // decode into depends on the multi-value list layout, but the non-NULL
  // keys must appear exactly once each.
  size_t null_rows = 0;
  size_t ones = 0;
  size_t twos = 0;
  for (const Row& row : out) {
    if (row[0].IsNull()) {
      ++null_rows;
    } else if (row[0] == Value(1)) {
      ++ones;
    } else if (row[0] == Value(2)) {
      ++twos;
    }
  }
  EXPECT_EQ(ones, 1U);
  EXPECT_EQ(twos, 1U);
  EXPECT_GE(null_rows, 1U);
  EXPECT_LE(null_rows, 2U);
}

}  // namespace tinylamb
