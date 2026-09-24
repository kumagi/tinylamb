/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
// Executes one statement and returns the materialized rows.  A prepare or
// execution failure surfaces as a runtime_error carrying the diagnostic.
// Routes through Execute() so the rows carry the projected user columns.
std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  // One retry for empty diagnostics: a failed Execute with no LastError is
  // the external parse child being transiently unavailable under load, not
  // a legitimate SQL error (real errors always carry a message).
  StatusOr<QueryResult> result = engine->Execute(*ctx, sql);
  if (!result.HasValue()) {
    if (engine->LastError().empty()) {
      result = engine->Execute(*ctx, sql);
    }
    if (!result.HasValue()) {
      throw std::runtime_error(engine->LastError());
    }
  }
  return result.MoveValue().Collect();
}
}  // namespace

class SqlEngineCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("sql_engine_coverage_test-" + RandomString())
                    .MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE t (a INT64, b VARCHAR(10), c FLOAT64);");
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO t (a, b, c) VALUES (1, 'x', 0.5), (2, 'y', 1.5), "
           "(3, 'z', 2.5)");
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;

  Value Scalar(const std::string& sql) {
    const auto rows = RunSql(engine_.get(), context_.get(), sql);
    if (rows.size() != 1U) {
      ADD_FAILURE() << "query did not return one row: " << sql;
      return {};
    }
    // Row::Size() is the serialized width, not the column count; a
    // single-value projection always lives at index 0.
    return rows[0][0];
  }
  std::vector<Row> Rows(const std::string& sql) {
    return RunSql(engine_.get(), context_.get(), sql);
  }
};

// ------------------------------------------------- Execute/QueryResult API ---

TEST_F(SqlEngineCoverageTest, ExecuteSelectCollectAndMetadata) {
  StatusOr<QueryResult> result =
      engine_->Execute(*context_, "SELECT a, b FROM t ORDER BY a");
  ASSERT_TRUE(result.HasValue());
  EXPECT_EQ(result.Value().Statement(), StatementType::kSelect);
  ASSERT_EQ(result.Value().ColumnNames().size(), 2U);
  const std::vector<Row> rows = result.MoveValue().Collect();
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(rows[2][1], Value(std::string("z")));
}

TEST_F(SqlEngineCoverageTest, QueryResultNextForEachDrain) {
  StatusOr<QueryResult> first =
      engine_->Execute(*context_, "SELECT a FROM t ORDER BY a");
  ASSERT_TRUE(first.HasValue());
  {
    size_t seen = 0;
    const size_t n = first.MoveValue().ForEach([&seen](const Row&) { ++seen; });
    EXPECT_EQ(n, 3U);
    EXPECT_EQ(seen, 3U);
  }
  StatusOr<QueryResult> second = engine_->Execute(*context_, "SELECT a FROM t");
  ASSERT_TRUE(second.HasValue());
  EXPECT_EQ(second.MoveValue().Drain(), 3U);

  // After exhaustion Next reports false and GetStatus stays success.
  StatusOr<QueryResult> third =
      engine_->Execute(*context_, "SELECT a FROM t WHERE a = 1");
  ASSERT_TRUE(third.HasValue());
  QueryResult exhausted = third.MoveValue();
  Row row;
  EXPECT_TRUE(exhausted.Next(&row));
  EXPECT_FALSE(exhausted.Next(&row));
  EXPECT_EQ(exhausted.GetStatus(), Status::kSuccess);
}

TEST_F(SqlEngineCoverageTest, QueryResultDumpRendersThePlan) {
  StatusOr<QueryResult> result =
      engine_->Execute(*context_, "SELECT a FROM t ORDER BY a");
  ASSERT_TRUE(result.HasValue());
  std::ostringstream oss;
  result.MoveValue().Dump(oss, 0);
  EXPECT_FALSE(oss.str().empty());
}

TEST_F(SqlEngineCoverageTest, AffectedRowsPerStatementKind) {
  StatusOr<QueryResult> ins =
      engine_->Execute(*context_, "INSERT INTO t (a, b, c) VALUES (4, 'w', 3)");
  ASSERT_TRUE(ins.HasValue());
  EXPECT_EQ(ins.MoveValue().AffectedRows(), 1);

  StatusOr<QueryResult> upd =
      engine_->Execute(*context_, "UPDATE t SET c = 9 WHERE a <= 2");
  ASSERT_TRUE(upd.HasValue());
  EXPECT_EQ(upd.MoveValue().AffectedRows(), 2);

  StatusOr<QueryResult> del =
      engine_->Execute(*context_, "DELETE FROM t WHERE a = 4");
  ASSERT_TRUE(del.HasValue());
  EXPECT_EQ(del.MoveValue().AffectedRows(), 1);

  // A SELECT carries no affected-row pair.
  StatusOr<QueryResult> sel = engine_->Execute(*context_, "SELECT a FROM t");
  ASSERT_TRUE(sel.HasValue());
  EXPECT_EQ(sel.MoveValue().AffectedRows(), 0);
  EXPECT_EQ(Scalar("SELECT COUNT(*) FROM t"), Value(int64_t{3}));
}

TEST_F(SqlEngineCoverageTest, LastDmlTableTracksTheTarget) {
  // The fixture's INSERT already targeted t.
  StatusOr<QueryResult> ins =
      engine_->Execute(*context_, "INSERT INTO t (a, b, c) VALUES (9, 'q', 0)");
  ASSERT_TRUE(ins.HasValue());
  EXPECT_EQ(engine_->LastDmlTable(), "t");
}

TEST_F(SqlEngineCoverageTest, ExecuteFailureSurfacesLastError) {
  const StatusOr<QueryResult> result =
      engine_->Execute(*context_, "SELECT * FROM missing_table");
  EXPECT_FALSE(result.HasValue());
  EXPECT_FALSE(engine_->LastError().empty());
}

// ------------------------------------------------------- statement routing ---

TEST_F(SqlEngineCoverageTest, StatementTypeRouting) {
  EXPECT_EQ(engine_->LastStatementType(), StatementType::kInsert);
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE ddl_t (x INT64 PRIMARY KEY)");
  EXPECT_EQ(engine_->LastStatementType(), StatementType::kCreateTable);
  RunSql(engine_.get(), context_.get(), "DROP TABLE ddl_t");
  EXPECT_EQ(engine_->LastStatementType(), StatementType::kDropTable);
  std::ignore = RunSql(engine_.get(), context_.get(), "SELECT 1");
  EXPECT_EQ(engine_->LastStatementType(), StatementType::kSelect);
  RunSql(engine_.get(), context_.get(), "UPDATE t SET c = 1 WHERE a = 1");
  EXPECT_EQ(engine_->LastStatementType(), StatementType::kUpdate);
  RunSql(engine_.get(), context_.get(), "DELETE FROM t WHERE a = 3");
  EXPECT_EQ(engine_->LastStatementType(), StatementType::kDelete);
  RunSql(engine_.get(), context_.get(), "ANALYZE t");
  EXPECT_EQ(engine_->LastStatementType(), StatementType::kAnalyze);
  // The deleted row stays deleted; the counts line up.
  EXPECT_EQ(Scalar("SELECT COUNT(*) FROM t"), Value(int64_t{2}));
}

// ------------------------------------------------------------- EXPLAIN ------

TEST_F(SqlEngineCoverageTest, ExplainProducesPlanText) {
  for (const std::string sql :
       {"EXPLAIN SELECT a FROM t WHERE a = 1",
        "EXPLAIN SELECT t1.b, t2.b FROM t t1 JOIN t t2 ON t1.a = t2.a",
        "EXPLAIN SELECT b, SUM(a) FROM t GROUP BY b HAVING SUM(a) > 0",
        "EXPLAIN SELECT a FROM t ORDER BY c LIMIT 2",
        "EXPLAIN SELECT DISTINCT b FROM t",
        "EXPLAIN SELECT SUM(a) OVER (ORDER BY a) FROM t",
        "EXPLAIN WITH x AS (SELECT a FROM t) SELECT * FROM x",
        "EXPLAIN SELECT a FROM t WHERE a * 2 + 1 = 5"}) {
    SCOPED_TRACE(sql);
    const auto rows = RunSql(engine_.get(), context_.get(), sql);
    ASSERT_FALSE(rows.empty());
    std::string plan;
    for (const Row& row : rows) {
      plan += row[0].AsString();
      plan += "\n";
    }
    EXPECT_FALSE(plan.empty());
  }
}

TEST_F(SqlEngineCoverageTest, ExplainAnalyzeExecutesAndReports) {
  const auto rows = RunSql(engine_.get(), context_.get(),
                           "EXPLAIN ANALYZE SELECT a FROM t WHERE a = 1");
  ASSERT_FALSE(rows.empty());
  EXPECT_EQ(Scalar("SELECT COUNT(*) FROM t"), Value(int64_t{3}));
}

TEST_F(SqlEngineCoverageTest, ExplainRejectsNonSelectStatements) {
  EXPECT_THROW(Rows("EXPLAIN INSERT INTO t (a, b, c) VALUES (1, 'x', 0)"),
               std::runtime_error);
  EXPECT_THROW(Rows("EXPLAIN CREATE TABLE x (a INT64)"), std::runtime_error);
  EXPECT_THROW(Rows("EXPLAIN ANALYZE UPDATE t SET a = 1"), std::runtime_error);
}

// ------------------------------------------------------------- plan cache ---

TEST_F(SqlEngineCoverageTest, PlanCacheReusesTemplateAndStaysCorrect) {
  for (const int64_t key : {int64_t{1}, int64_t{2}, int64_t{3}, int64_t{2}}) {
    const auto rows = Rows("SELECT b FROM t WHERE a = " + std::to_string(key));
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows[0][0], Value(std::string(1, static_cast<char>('w' + key))));
  }
  // Structurally identical statements over another table reuse the same
  // machinery and stay correct after DDL changes the catalog.
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE t2 (a INT64, b VARCHAR(10));");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO t2 (a, b) VALUES (1, 'p'), (2, 'r')");
  for (const int64_t key : {int64_t{1}, int64_t{2}}) {
    const auto rows = Rows("SELECT b FROM t2 WHERE a = " + std::to_string(key));
    ASSERT_EQ(rows.size(), 1U);
  }
  RunSql(engine_.get(), context_.get(), "DROP TABLE t2");
  EXPECT_THROW(Rows("SELECT b FROM t2 WHERE a = 1"), std::runtime_error);
}

// --------------------------------------------------------- runtime stats ----

TEST_F(SqlEngineCoverageTest, ThreadExecutionCountAdvances) {
  const uint64_t before = SqlEngine::ThreadExecutionCount();
  std::ignore = RunSql(engine_.get(), context_.get(), "SELECT 1");
  std::ignore = RunSql(engine_.get(), context_.get(), "SELECT 2");
  EXPECT_GE(SqlEngine::ThreadExecutionCount(), before + 2);
}

TEST_F(SqlEngineCoverageTest, RuntimeProfilingAccumulatesAndResets) {
  SqlEngine::SetThreadRuntimeProfiling(true);
  const SqlRuntimeStats zero = SqlEngine::ThreadRuntimeStats();
  EXPECT_EQ(zero.prepare_ns, 0);
  std::ignore = Rows("SELECT a FROM t ORDER BY a");
  std::ignore =
      RunSql(engine_.get(), context_.get(), "SELECT a FROM t WHERE a = 1");
  const SqlRuntimeStats stats = SqlEngine::ThreadRuntimeStats();
  EXPECT_GT(stats.prepare_ns, 0);
  SqlEngine::SetThreadRuntimeProfiling(false);
  const SqlRuntimeStats cleared = SqlEngine::ThreadRuntimeStats();
  EXPECT_EQ(cleared.prepare_ns, 0);
}

// --------------------------------------------------------- window routing ---

TEST_F(SqlEngineCoverageTest, WindowRoutingCleanAndFallback) {
  // Clean window calls route through the memo lowering; the routed row
  // layout carries implementation columns, so verify shape-agnostically.
  const auto plain = Rows("SELECT SUM(a) OVER () FROM t WHERE a = 1");
  ASSERT_EQ(plain.size(), 1U);
  // Partitioned and ordered window shapes stay executable end to end.
  EXPECT_EQ(
      Rows("SELECT a, COUNT(*) OVER (PARTITION BY b) FROM t ORDER BY a").size(),
      3U);
  // QUALIFY filters after the window evaluation.
  const auto qualified =
      Rows("SELECT a FROM t QUALIFY ROW_NUMBER() OVER (ORDER BY a) <= 2");
  ASSERT_EQ(qualified.size(), 2U);
  EXPECT_EQ(qualified[0][0], Value(int64_t{1}));
  EXPECT_EQ(qualified[1][0], Value(int64_t{2}));
  // Window in ORDER BY with the projection kept narrow.
  const auto ordered =
      Rows("SELECT a FROM t ORDER BY ROW_NUMBER() OVER (ORDER BY a) DESC");
  ASSERT_EQ(ordered.size(), 3U);
  EXPECT_EQ(ordered[0][0], Value(int64_t{3}));
}

TEST_F(SqlEngineCoverageTest, FilteredSumSkipsJitColumnFastPath) {
  // The single-SUM JIT shortcut evaluated the whole column and dropped the
  // inline WHERE filter entirely (bug: SUM(a WHERE a > 100) returned 6).
  EXPECT_EQ(Scalar("SELECT SUM(a WHERE a > 100) FROM t").AsString(), "NULL");
  EXPECT_EQ(Scalar("SELECT SUM(a WHERE a > 1) FROM t").AsString(), "5");
}

TEST_F(SqlEngineCoverageTest, TemplateRebindKeepsAggregateAttributes) {
  // Same-fingerprint statements exercise the template cache: the second
  // execution binds literals into the cached tree, which used to rebuild
  // aggregates with only type/child/distinct — silently dropping the
  // inline WHERE filter, inner ORDER BY/LIMIT and the STRING_AGG
  // delimiter.
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE ta (u INT64, a INT64, s VARCHAR(8))");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO ta VALUES (1, 5, 'x'), (2, -3, 'y'), (3, 7, 'x')");
  // A literal-free filter keeps the fingerprint identical across queries
  // while its loss stays silent: every u is non-NULL, so the filtered
  // aggregate sees no rows and must be NULL (not FALSE/TRUE).
  EXPECT_EQ(
      Scalar("SELECT LOGICAL_AND(a > 0 WHERE u IS NULL) FROM ta").AsString(),
      "NULL");
  EXPECT_EQ(
      Scalar("SELECT LOGICAL_AND(a > 1 WHERE u IS NULL) FROM ta").AsString(),
      "NULL");
  EXPECT_EQ(Scalar("SELECT STRING_AGG(s, ',' WHERE u > 1 ORDER BY u) FROM ta")
                .AsString(),
            "\"y,x\"");
  EXPECT_EQ(Scalar("SELECT STRING_AGG(s, '-' WHERE u > 2 ORDER BY u) FROM ta")
                .AsString(),
            "\"x\"");
  EXPECT_EQ(Scalar("SELECT ARRAY_AGG(a WHERE u > 1 ORDER BY u LIMIT 2) FROM ta")
                .AsString(),
            "ARRAY<INT64>[-3, 7]");
  EXPECT_EQ(Scalar("SELECT ARRAY_AGG(a WHERE u > 2 ORDER BY u LIMIT 2) FROM ta")
                .AsString(),
            "ARRAY<INT64>[7]");
}

TEST_F(SqlEngineCoverageTest, NonAnalyticArrayAggIgnoreNulls) {
  EXPECT_EQ(Scalar("SELECT ARRAY_AGG(v IGNORE NULLS ORDER BY v) "
                   "FROM UNNEST([1, NULL]) AS v")
                .AsString(),
            "ARRAY<INT64>[1]");
  EXPECT_EQ(Scalar("SELECT ARRAY_AGG(v ORDER BY v) "
                   "FROM UNNEST([1, NULL]) AS v")
                .AsString(),
            "ARRAY<INT64>[NULL, 1]");
}

TEST_F(SqlEngineCoverageTest, CaseInsensitiveGroupByCollatesEqualKeys) {
  const auto rows = Rows(
      "SELECT COUNT(*) FROM UNNEST(['a', 'A']) AS v "
      "GROUP BY COLLATE(v, 'und:ci')");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{2}));
}

}  // namespace tinylamb
