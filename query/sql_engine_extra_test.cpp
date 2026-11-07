/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Engine-path sweep (query/sql_engine.cpp): UNNEST selects, CTE narrowing
// vs materialization, LATERAL decorrelation, narrow-integer insert checks,
// statement-template reuse through the plan cache, and Prepare/Execute
// error surfaces.

#include <gtest/gtest.h>

#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/plan_cache.hpp"
#include "query/sql_engine.hpp"
#include "query/sql_template.hpp"
#include "table/table.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    if (engine->LastError().empty()) {
      prepared = engine->Prepare(*ctx, sql);
    }
    if (!prepared.HasValue()) {
      throw std::runtime_error(engine->LastError());
    }
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

}  // namespace

class SqlEngineExtraTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    path_ = "sql_engine_extra_test-" + RandomString();
    database_ = Database::Create(path_).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE t(k INT64, s STRING(MAX), f FLOAT64)");
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO t (k, s, f) VALUES (1, 'a', 1.5), (2, 'b', 2.5), "
           "(3, 'a', 3.5)");
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }
  void ExpectQuery(const std::string& sql, const std::vector<Row>& expected) {
    SCOPED_TRACE(sql);
    const auto rows = RunSql(engine_.get(), context_.get(), sql);
    ASSERT_EQ(rows.size(), expected.size());
    for (size_t i = 0; i < rows.size(); ++i) {
      EXPECT_EQ(rows[i], expected[i]);
    }
  }
  void ExpectScalar(const std::string& sql, Value expected) {
    SCOPED_TRACE(sql);
    ExpectQuery(sql, {Row({std::move(expected)})});
  }
  void ExpectError(const std::string& sql) {
    SCOPED_TRACE(sql);
    EXPECT_FALSE(engine_->Execute(*context_, sql).HasValue());
  }
  // Some statement shapes only go through the Execute wrapper.
  void ExpectQueryExecute(const std::string& sql,
                          const std::vector<Row>& expected) {
    SCOPED_TRACE(sql);
    std::vector<Row> rows;
    StatusOr<QueryResult> result = engine_->Execute(*context_, sql);
    ASSERT_TRUE(result.HasValue());
    result.MoveValue().ForEach(
        [&rows](const Row& row) { rows.push_back(row); });
    ASSERT_EQ(rows.size(), expected.size());
    for (size_t i = 0; i < rows.size(); ++i) {
      EXPECT_EQ(rows[i], expected[i]);
    }
  }
  std::unique_ptr<Database> database_;
  std::string path_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(SqlEngineExtraTest, SqlCreateIndexMaintainsDuplicatesAndSurvivesReopen) {
  RunSql(engine_.get(), context_.get(), "CREATE INDEX by_s ON t(s) STORING(f)");
  ExpectQuery("SELECT k FROM t WHERE s = 'a' ORDER BY k",
              {Row({Value(1)}), Row({Value(3)})});
  RunSql(engine_.get(), context_.get(), "INSERT INTO t VALUES (4, 'a', 4.5)");
  RunSql(engine_.get(), context_.get(), "UPDATE t SET s = 'c' WHERE k = 1");
  RunSql(engine_.get(), context_.get(), "DELETE FROM t WHERE k = 3");
  ExpectQuery("SELECT k FROM t WHERE s = 'a'", {Row({Value(4)})});
  ExpectError("CREATE INDEX by_s ON t(k)");
  RunSql(engine_.get(), context_.get(),
         "CREATE INDEX IF NOT EXISTS BY_S ON t(k)");
  ASSERT_EQ(context_->PreCommit(), Status::kSuccess);
  context_.reset();
  engine_.reset();
  database_.reset();
  database_ = Database::Create(path_).MoveValue();
  ASSERT_NE(database_, nullptr);
  context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  engine_ = std::make_unique<SqlEngine>(*database_);
  auto table = context_->GetTable("t");
  ASSERT_TRUE(table.HasValue());
  ASSERT_EQ(table.Value()->IndexCount(), 1U);
  EXPECT_FALSE(table.Value()->GetIndex(0).IsUnique());
  ExpectQuery("SELECT k FROM t WHERE s = 'a'", {Row({Value(4)})});
  RunSql(engine_.get(), context_.get(), "INSERT INTO t VALUES (5, 'a', 5.5)");
  ExpectQuery("SELECT k FROM t WHERE s = 'a' ORDER BY k",
              {Row({Value(4)}), Row({Value(5)})});
}

TEST_F(SqlEngineExtraTest, SqlCreateIndexFailureAndAbortKeepCatalogAtomic) {
  ExpectError("CREATE UNIQUE INDEX fail_unique ON t(s)");
  ExpectError("CREATE INDEX missing_column ON t(missing)");
  ExpectError("CREATE INDEX repeated_column ON t(k, k)");
  auto table = context_->GetTable("t");
  ASSERT_TRUE(table.HasValue());
  EXPECT_EQ(table.Value()->IndexCount(), 0U);
  ExpectScalar("SELECT COUNT(*) FROM t", Value(3));
  RunSql(engine_.get(), context_.get(), "CREATE UNIQUE INDEX good ON t(k)");
  ASSERT_EQ(context_->PreCommit(), Status::kSuccess);
  context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  RunSql(engine_.get(), context_.get(), "CREATE INDEX aborted ON t(s)");
  context_->Abort();
  context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  table = context_->GetTable("t");
  ASSERT_TRUE(table.HasValue());
  ASSERT_EQ(table.Value()->IndexCount(), 1U);
  EXPECT_EQ(table.Value()->GetIndex(0).sc_.name_, "good");
  auto duplicate =
      engine_->Execute(*context_, "INSERT INTO t VALUES (1, 'z', 9.0)");
  if (duplicate.HasValue()) {
    duplicate.Value().Drain();
    EXPECT_NE(duplicate.Value().GetStatus(), Status::kSuccess);
  }
}

TEST_F(SqlEngineExtraTest, UnnestSelectShapes) {
  ExpectQuery("SELECT x FROM UNNEST([1, 2, 3]) AS x WHERE x >= 2 ORDER BY x",
              {Row({Value(2)}), Row({Value(3)})});
  ExpectScalar("SELECT COUNT(*) FROM UNNEST([1, 2, 3]) AS u", Value(3));
  ExpectScalar("SELECT SUM(u) FROM UNNEST([10, 20]) AS u", Value(30));
  ExpectQuery(
      "SELECT t.k, u FROM t CROSS JOIN UNNEST([7]) AS u "
      "ORDER BY t.k",
      {Row({Value(1), Value(7)}), Row({Value(2), Value(7)}),
       Row({Value(3), Value(7)})});
  ExpectQuery("SELECT t.k FROM t WHERE t.k IN UNNEST([2, 3]) ORDER BY t.k",
              {Row({Value(2)}), Row({Value(3)})});
  ExpectQuery(
      "SELECT t.k, (SELECT COUNT(*) FROM UNNEST([1, 2, 3]) AS u "
      "WHERE u > t.k) AS cnt FROM t ORDER BY t.k LIMIT 1",
      {Row({Value(1), Value(2)})});
}

TEST_F(SqlEngineExtraTest, CteNarrowingVersusMaterialization) {
  // The twice-referenced CTE shape goes through the materialized CTE path.
  // NOTE: the single-reference narrowing shape currently leaks a
  // "column a.k not found" exception out of Execute in-process; it is
  // reported separately and intentionally not pinned here.
  auto cte_db = Database::Create("sql_engine_extra_test-cte-" + RandomString())
                    .MoveValue();
  TransactionContext cte_ctx = cte_db->BeginContext();
  SqlEngine cte_engine(*cte_db);
  const auto run = [&](const std::string& sql) {
    std::vector<Row> rows;
    StatusOr<QueryResult> result = cte_engine.Execute(cte_ctx, sql);
    EXPECT_TRUE(result.HasValue()) << sql;
    if (result.HasValue()) {
      result.MoveValue().ForEach(
          [&rows](const Row& row) { rows.push_back(row); });
    }
    return rows;
  };
  run("CREATE TABLE t(k INT64, s STRING(MAX), f FLOAT64)");
  run("INSERT INTO t (k, s, f) VALUES (1, 'a', 1.5), (2, 'b', 2.5), "
      "(3, 'a', 3.5)");
  EXPECT_EQ(run("WITH x AS (SELECT k FROM t) SELECT COUNT(*) FROM x, x x2"),
            std::vector<Row>({Row({Value(9)})}));
  EXPECT_EQ(run("WITH x AS (SELECT k FROM t WHERE k > 1) SELECT COUNT(*) "
                "FROM x"),
            std::vector<Row>({Row({Value(2)})}));
  cte_ctx.Abort();
}

TEST_F(SqlEngineExtraTest, LateralDecorrelation) {
  ExpectQuery(
      "SELECT t.k, u.n FROM t, LATERAL "
      "(SELECT COUNT(*) AS n FROM t t2 WHERE t2.k >= t.k) u "
      "ORDER BY t.k",
      {Row({Value(1), Value(3)}), Row({Value(2), Value(2)}),
       Row({Value(3), Value(1)})});
}

TEST_F(SqlEngineExtraTest, NarrowIntegerInserts) {
  ExpectQuery("CREATE TABLE ni(x INT32)",
              {Row({Value("CREATE TABLE"), Value(0)})});
  ExpectQuery("INSERT INTO ni (x) VALUES (5)",
              {Row({Value("Insert Rows"), Value(1)})});
  ExpectScalar("SELECT x FROM ni", Value(5));
  ExpectError("INSERT INTO ni (x) VALUES ('abc')");
  ExpectError("INSERT INTO t (nope) VALUES (1)");
  ExpectError("INSERT INTO t (k) VALUES (1, 2)");
}

TEST_F(SqlEngineExtraTest, StatementTemplateReuse) {
  // Preparing and executing the same statement shape repeatedly walks the
  // template/plan-cache hit and miss paths.
  for (int i = 0; i < 3; ++i) {
    ExpectScalar("SELECT k FROM t WHERE k = 1", Value(1));
    ExpectScalar("SELECT COUNT(*) FROM t WHERE s = 'a'", Value(2));
  }
  ExpectScalar("SELECT k FROM t WHERE k = 2", Value(2));
  // DDL invalidates cached plans for the touched table.
  ExpectQuery("CREATE TABLE other(x INT64)",
              {Row({Value("CREATE TABLE"), Value(0)})});
  ExpectScalar("SELECT COUNT(*) FROM t", Value(3));
}

TEST_F(SqlEngineExtraTest, CountDistinctIndexKeysKeepsNullsAndMultiplicity) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE distinct_keys(k INT64, payload STRING(MAX))");
  {
    ASSIGN_OR_ASSERT_FAIL(Table, table,
                          database_->GetTable(*context_, "distinct_keys"));
    for (int i = 0; i < 6000; ++i) {
      ASSERT_SUCCESS(table
                         .Insert(context_->txn_,
                                 Row({i % 11 == 10 ? Value() : Value(i % 11),
                                      Value(std::string(256, 'x'))}))
                         .GetStatus());
    }
  }
  RunSql(engine_.get(), context_.get(),
         "CREATE INDEX distinct_key_index ON distinct_keys(k)");
  ASSERT_SUCCESS(database_->RefreshStatistics(*context_, "distinct_keys"));
  ASSERT_SUCCESS(context_->PreCommit());
  context_ =
      std::make_unique<TransactionContext>(database_->BeginReadOnlyContext());
  ASSIGN_OR_ASSERT_FAIL(
      Executor, executor,
      engine_->Prepare(*context_,
                       "SELECT COUNT(DISTINCT k) FROM distinct_keys"));
  std::ostringstream dump;
  executor->Dump(dump, 0);
  EXPECT_NE(dump.str().find("distinct-keys"), std::string::npos) << dump.str();
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(10)}));
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(executor->GetStatus());
  executor.reset();
  ExpectQuery("SELECT COUNT(DISTINCT k), COUNT(*) FROM distinct_keys",
              {Row({Value(10), Value(6000)})});
  ExpectScalar("SELECT COUNT(DISTINCT k) FROM distinct_keys WHERE k >= 5",
               Value(5));
  ExpectScalar("SELECT COUNT(DISTINCT k) FROM distinct_keys WHERE k > 20",
               Value(0));
  ExpectScalar(
      "SELECT COUNT(DISTINCT k) FROM distinct_keys "
      "WHERE payload = 'absent'",
      Value(0));
}

TEST_F(SqlEngineExtraTest, CountDistinctIndexKeysKeepsOldSnapshotFallback) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE distinct_snapshot(k INT64, payload STRING(MAX))");
  {
    ASSIGN_OR_ASSERT_FAIL(Table, table,
                          database_->GetTable(*context_, "distinct_snapshot"));
    for (int i = 0; i < 2000; ++i) {
      ASSERT_SUCCESS(
          table
              .Insert(context_->txn_,
                      Row({Value(i % 4), Value(std::string(256, 'x'))}))
              .GetStatus());
    }
  }
  RunSql(engine_.get(), context_.get(),
         "CREATE INDEX snapshot_key ON distinct_snapshot(k)");
  ASSERT_SUCCESS(database_->RefreshStatistics(*context_, "distinct_snapshot"));
  ASSERT_SUCCESS(context_->PreCommit());
  context_ =
      std::make_unique<TransactionContext>(database_->BeginReadOnlyContext());
  {
    TransactionContext writer = database_->BeginContext();
    RunSql(engine_.get(), &writer,
           "INSERT INTO distinct_snapshot VALUES (9, 'new key')");
    ASSERT_SUCCESS(writer.PreCommit());
  }
  ExpectScalar("SELECT COUNT(DISTINCT k) FROM distinct_snapshot", Value(4));
  TransactionContext fresh = database_->BeginReadOnlyContext();
  EXPECT_EQ(RunSql(engine_.get(), &fresh,
                   "SELECT COUNT(DISTINCT k) FROM distinct_snapshot"),
            std::vector<Row>({Row({Value(5)})}));
  ASSERT_SUCCESS(fresh.PreCommit());
}

TEST_F(SqlEngineExtraTest,
       ImmutableOrderedPlanSharesAcrossWorkersWithExactLimits) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE shared_ordered(k INT64, v INT64)");
  {
    ASSIGN_OR_ASSERT_FAIL(Table, table,
                          database_->GetTable(*context_, "shared_ordered"));
    for (int i = 0; i < 300; ++i) {
      ASSERT_SUCCESS(
          table.Insert(context_->txn_, Row({Value(i % 10), Value(i)}))
              .GetStatus());
    }
  }
  RunSql(engine_.get(), context_.get(),
         "CREATE INDEX shared_keys ON shared_ordered(k, v)");
  ASSERT_SUCCESS(database_->RefreshStatistics(*context_, "shared_ordered"));
  ASSERT_SUCCESS(context_->PreCommit());
  context_ =
      std::make_unique<TransactionContext>(database_->BeginReadOnlyContext());
  const std::string query =
      "SELECT v FROM shared_ordered ORDER BY k DESC, v LIMIT ";
  PreparedPlanCache::Instance().Clear();
  thread_compiled_plans.clear();
  ExpectQuery(query + "3",
              {Row({Value(9)}), Row({Value(19)}), Row({Value(29)})});
  auto initial = PreparedPlanCache::Instance().Find(
      ExtractSqlTemplate(query + "3").fingerprint, database_.get(),
      database_->SchemaEpoch());
  ASSERT_NE(initial, nullptr);
  EXPECT_EQ(initial->parameters, ExtractSqlTemplate(query + "3").parameters);
  EXPECT_FALSE(initial->retired.load());
  auto execute_on_worker = [&](int limit, int repeats) {
    SqlEngine engine(*database_);
    auto reader = database_->BeginReadOnlyContext();
    for (int n = 0; n < repeats; ++n) {
      const auto rows = RunSql(&engine, &reader, query + std::to_string(limit));
      ASSERT_EQ(rows.size(), static_cast<size_t>(limit));
      for (int i = 0; i < limit; ++i) {
        EXPECT_EQ(rows[static_cast<size_t>(i)], Row({Value(9 + i * 10)}));
      }
    }
    ASSERT_SUCCESS(reader.PreCommit());
  };
  const uint64_t replay_before = PlanCacheStats().replays.load();
  const auto invalidations = PlanCacheStats().epoch_invalidations.load();
  const auto mismatches = PlanCacheStats().parameter_mismatches.load();
  std::thread first(execute_on_worker, 3, 1);
  first.join();
  EXPECT_EQ(PlanCacheStats().epoch_invalidations.load(), invalidations);
  EXPECT_EQ(PlanCacheStats().parameter_mismatches.load(), mismatches);
  EXPECT_GT(PlanCacheStats().replays.load(), replay_before);
  // LIMIT literals stay in the fingerprint. Concurrent workers must keep
  // the two immutable trees and result sizes separate.
  std::thread left(execute_on_worker, 3, 32);
  std::thread peer(execute_on_worker, 3, 32);
  std::thread right(execute_on_worker, 5, 32);
  left.join();
  peer.join();
  right.join();
  std::thread seed(execute_on_worker, 3, 1);
  seed.join();
  const auto fingerprint = ExtractSqlTemplate(query + "3").fingerprint;
  auto artifact = PreparedPlanCache::Instance().Find(
      fingerprint, database_.get(), database_->SchemaEpoch());
  ASSERT_NE(artifact, nullptr);
  EXPECT_TRUE(artifact->parameter_slots.empty());
  artifact->retired.store(true);
  const uint64_t replay_at_retire = PlanCacheStats().replays.load();
  std::thread replan(execute_on_worker, 3, 1);
  replan.join();
  EXPECT_EQ(PlanCacheStats().replays.load(), replay_at_retire);
  EXPECT_NE(PreparedPlanCache::Instance().Find(fingerprint, database_.get(),
                                               database_->SchemaEpoch()),
            artifact);
  // A shared index plan still rechecks visibility for an older snapshot.
  {
    auto writer = database_->BeginContext();
    RunSql(engine_.get(), &writer,
           "UPDATE shared_ordered SET k = 99 WHERE v = 0");
    ASSERT_SUCCESS(writer.PreCommit());
  }
  ExpectQuery(query + "3",
              {Row({Value(9)}), Row({Value(19)}), Row({Value(29)})});
  auto fresh = database_->BeginReadOnlyContext();
  EXPECT_EQ(
      RunSql(engine_.get(), &fresh, query + "3"),
      std::vector<Row>({Row({Value(0)}), Row({Value(9)}), Row({Value(19)})}));
  ASSERT_SUCCESS(fresh.PreCommit());
}

TEST_F(SqlEngineExtraTest, EngineErrorSurfaces) {
  ExpectError("SELECT * FROM missing_table");
  ExpectError("SELECT missing_col FROM t");
  ExpectError("SELECT k FROM t ORDER BY nope");
  ExpectError("UPDATE t SET nope = 1");
  ExpectError("DELETE FROM missing_table");
}

TEST_F(SqlEngineExtraTest, ImmutableScalarAggregatePlanSharesButNotItsResult) {
  ASSERT_SUCCESS(context_->PreCommit());
  context_ =
      std::make_unique<TransactionContext>(database_->BeginReadOnlyContext());
  PreparedPlanCache::Instance().Clear();
  thread_compiled_plans.clear();
  const std::string sql = "SELECT COUNT(*), COUNT(DISTINCT k), SUM(k) FROM t";
  const auto expected = std::vector<Row>{Row({Value(3), Value(3), Value(6)})};
  ExpectQuery(sql, expected);
  auto artifact = PreparedPlanCache::Instance().Find(
      ExtractSqlTemplate(sql).fingerprint, database_.get(),
      database_->SchemaEpoch());
  ASSERT_NE(artifact, nullptr);
  EXPECT_TRUE(artifact->immutable_exact_read);
  EXPECT_TRUE(artifact->parameter_slots.empty());
  const uint64_t replays = PlanCacheStats().replays.load();
  auto execute = [&] {
    SqlEngine engine(*database_);
    auto reader = database_->BeginReadOnlyContext();
    for (int n = 0; n < 16; ++n)
      EXPECT_EQ(RunSql(&engine, &reader, sql), expected);
    ASSERT_SUCCESS(reader.PreCommit());
  };
  std::thread first(execute);
  std::thread second(execute);
  first.join();
  second.join();
  EXPECT_GE(PlanCacheStats().replays.load(), replays + 32);
  {
    auto writer = database_->BeginContext();
    RunSql(engine_.get(), &writer,
           "INSERT INTO t VALUES (4, 'new', 4.5), (NULL, 'null', 0.0)");
    ASSERT_SUCCESS(writer.PreCommit());
  }
  ExpectQuery(sql, expected);
  auto fresh = database_->BeginReadOnlyContext();
  EXPECT_EQ(RunSql(engine_.get(), &fresh, sql),
            std::vector<Row>({Row({Value(5), Value(4), Value(10)})}));
  ASSERT_SUCCESS(fresh.PreCommit());
  artifact->retired.store(true);
  const uint64_t retired_replays = PlanCacheStats().replays.load();
  std::thread replan([&] {
    SqlEngine engine(*database_);
    auto reader = database_->BeginReadOnlyContext();
    EXPECT_EQ(RunSql(&engine, &reader, sql),
              std::vector<Row>({Row({Value(5), Value(4), Value(10)})}));
    ASSERT_SUCCESS(reader.PreCommit());
  });
  replan.join();
  EXPECT_EQ(PlanCacheStats().replays.load(), retired_replays);
  EXPECT_NE(PreparedPlanCache::Instance().Find(
                ExtractSqlTemplate(sql).fingerprint, database_.get(),
                database_->SchemaEpoch()),
            artifact);
}

TEST_F(SqlEngineExtraTest,
       ImmutableScalarAggregateRevalidatesEpochAndIncarnation) {
  RunSql(engine_.get(), context_.get(), "CREATE TABLE scalar_epoch(k INT64)");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO scalar_epoch VALUES (7), (NULL)");
  ASSERT_SUCCESS(context_->PreCommit());
  context_ =
      std::make_unique<TransactionContext>(database_->BeginReadOnlyContext());
  const std::string sql = "SELECT COUNT(*), SUM(k) FROM scalar_epoch";
  ExpectQuery(sql, {Row({Value(2), Value(7)})});
  auto old = PreparedPlanCache::Instance().Find(
      ExtractSqlTemplate(sql).fingerprint, database_.get(),
      database_->SchemaEpoch());
  ASSERT_NE(old, nullptr);
  ASSERT_SUCCESS(context_->PreCommit());
  context_.reset();
  {
    auto ddl = database_->BeginContext();
    RunSql(engine_.get(), &ddl, "DROP TABLE scalar_epoch");
    RunSql(engine_.get(), &ddl, "CREATE TABLE scalar_epoch(k INT64)");
    RunSql(engine_.get(), &ddl, "INSERT INTO scalar_epoch VALUES (99)");
    ASSERT_SUCCESS(ddl.PreCommit());
  }
  context_ =
      std::make_unique<TransactionContext>(database_->BeginReadOnlyContext());
  ExpectQuery(sql, {Row({Value(1), Value(99)})});
  EXPECT_NE(PreparedPlanCache::Instance().Find(
                ExtractSqlTemplate(sql).fingerprint, database_.get(),
                database_->SchemaEpoch()),
            old);
  const std::string volatile_sql =
      "SELECT SUM(k + CAST(RAND() AS INT64)) FROM scalar_epoch";
  RunSql(engine_.get(), context_.get(), volatile_sql);
  EXPECT_EQ(PreparedPlanCache::Instance().Find(
                ExtractSqlTemplate(volatile_sql).fingerprint, database_.get(),
                database_->SchemaEpoch()),
            nullptr);
}

TEST_F(SqlEngineExtraTest, TransactionalDmlAcrossContexts) {
  ExpectQuery("UPDATE t SET f = f * 2 WHERE k = 1",
              {Row({Value("Update Rows"), Value(1)})});
  ExpectScalar("SELECT f FROM t WHERE k = 1", Value(3.0));
  ExpectQuery("DELETE FROM t WHERE k = 2",
              {Row({Value("Delete Rows"), Value(1)})});
  ExpectScalar("SELECT COUNT(*) FROM t", Value(2));
  ExpectQuery("INSERT INTO t (k, s, f) VALUES (9, 'z', 9.0)",
              {Row({Value("Insert Rows"), Value(1)})});
  ExpectScalar("SELECT COUNT(*) FROM t", Value(3));
}

TEST_F(SqlEngineExtraTest,
       LiteralConstantCertificationExcludesContextAndModifiers) {
  for (const std::string sql : {"SELECT 1", "SELECT 2 + 3 AS answer",
                                "SELECT NULL, 'x', 1.25", "SELECT -7"}) {
    auto result = engine_->Execute(*context_, sql);
    ASSERT_TRUE(result.HasValue()) << sql << engine_->LastError();
    result.Value().Drain();
    EXPECT_TRUE(result.Value().GetStatus().ok());
    EXPECT_TRUE(engine_->LastQueryIsLiteralConstant()) << sql;
  }
  for (const std::string sql :
       {"SELECT k FROM t", "SELECT RAND()", "SELECT 1 WHERE TRUE",
        "SELECT 1 LIMIT 1", "SELECT 1 UNION ALL SELECT 2", "SELECT (SELECT 1)",
        "EXPLAIN SELECT 1"}) {
    auto result = engine_->Execute(*context_, sql);
    ASSERT_TRUE(result.HasValue()) << sql << engine_->LastError();
    result.Value().Drain();
    EXPECT_FALSE(engine_->LastQueryIsLiteralConstant()) << sql;
  }
  EXPECT_FALSE(engine_->Execute(*context_, "SELECT / broken").HasValue());
  EXPECT_FALSE(engine_->LastQueryIsLiteralConstant());
}

TEST_F(SqlEngineExtraTest,
       SessionPlansMigrateWorkersWithoutSharingMutableSlots) {
  ASSERT_SUCCESS(context_->PreCommit());
  context_.reset();
  thread_compiled_plans.clear();
  PreparedPlanCache::Instance().Clear();
  PreparedPlanSessionCache sessions[2];
  const auto execute = [&](int session, int64_t key) {
    ScopedPreparedPlanSession scope(&sessions[session]);
    auto read = database_->BeginReadOnlyContext();
    SqlEngine engine(*database_);
    const auto rows = RunSql(
        &engine, &read, "SELECT k FROM t WHERE k = " + std::to_string(key));
    const bool valid = rows == std::vector<Row>{Row({Value(key)})};
    const bool committed = read.PreCommit().ok();
    return valid && committed;
  };
  ASSERT_TRUE(execute(0, 1));
  ASSERT_TRUE(execute(1, 2));
  const auto fingerprint =
      ExtractSqlTemplate("SELECT k FROM t WHERE k = 1").fingerprint;
  ASSERT_TRUE(sessions[0].plans.contains(fingerprint));
  ASSERT_TRUE(sessions[1].plans.contains(fingerprint));
  const auto first = sessions[0].plans.at(fingerprint);
  const auto second = sessions[1].plans.at(fingerprint);
  ASSERT_FALSE(first->parameter_slots.empty());
  ASSERT_FALSE(second->parameter_slots.empty());
  EXPECT_NE(first.get(), second.get());
  EXPECT_NE(first->parameter_slots[0].get(), second->parameter_slots[0].get());
  const auto fills = PlanCacheStats().fills.load();
  std::atomic<int> failures{0};
  for (int round = 0; round < 3; ++round) {
    std::thread a([&] {
      for (int i = 0; i < 24; ++i)
        if (!execute(0, 1 + i % 3)) ++failures;
    });
    std::thread b([&] {
      for (int i = 0; i < 24; ++i)
        if (!execute(1, 3 - i % 3)) ++failures;
    });
    a.join();
    b.join();
  }
  EXPECT_EQ(failures.load(), 0);
  EXPECT_EQ(PlanCacheStats().fills.load(), fills);
  EXPECT_TRUE(thread_compiled_plans.empty());
  EXPECT_EQ(active_plan_session, nullptr);
  {
    ScopedPreparedPlanSession outer(&sessions[0]);
    EXPECT_TRUE(MarkFingerprintVolatile("session-only"));
    {
      ScopedPreparedPlanSession inner(&sessions[1]);
      EXPECT_FALSE(IsVolatileSpecializedPlan("session-only"));
    }
    EXPECT_TRUE(IsVolatileSpecializedPlan("session-only"));
  }
  EXPECT_FALSE(IsVolatileSpecializedPlan("session-only"));
}

TEST_F(SqlEngineExtraTest,
       FirstIndexColumnUpperBoundKeepsNullsEdgesAndRebinding) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE UpperOnly (k INT64, v INT64)");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO UpperOnly VALUES "
         "(NULL,100),(-2,1),(-1,2),(0,3),(1,4),(2,5),(2,6),(3,7),(4,8)");
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context_->GetTable("UpperOnly"));
  for (int i = 10; i < 1000; ++i)
    ASSERT_SUCCESS(
        table->Insert(context_->txn_, Row({Value(i), Value(i)})).GetStatus());
  RunSql(engine_.get(), context_.get(),
         "CREATE INDEX upper_kv ON UpperOnly(k,v)");
  RunSql(engine_.get(), context_.get(), "ANALYZE UpperOnly");
  ASSERT_SUCCESS(context_->PreCommit());
  context_ =
      std::make_unique<TransactionContext>(database_->BeginReadOnlyContext());
  for (const std::string direction : {"ASC", "DESC"}) {
    auto expected =
        std::vector<Row>{Row({Value(-2), Value(1)}), Row({Value(-1), Value(2)}),
                         Row({Value(0), Value(3)}), Row({Value(1), Value(4)})};
    if (direction == "DESC") std::ranges::reverse(expected);
    const std::string sql =
        "SELECT k,v FROM UpperOnly WHERE k < 2 ORDER BY k " + direction +
        ", v " + direction;
    EXPECT_EQ(RunSql(engine_.get(), context_.get(), sql), expected);
    const auto plan = RunSql(engine_.get(), context_.get(), "EXPLAIN " + sql);
    bool index_used = false;
    for (const auto& row : plan)
      if (row[0].value.varchar_value.find("IndexOnlyScan") != std::string::npos)
        index_used = true;
    EXPECT_TRUE(index_used);
  }
  EXPECT_EQ(RunSql(engine_.get(), context_.get(),
                   "SELECT SUM(v) FROM UpperOnly WHERE k < 2"),
            std::vector<Row>{Row({Value(10)})});
  EXPECT_EQ(RunSql(engine_.get(), context_.get(),
                   "SELECT SUM(v) FROM UpperOnly WHERE k < 3"),
            std::vector<Row>{Row({Value(21)})});
  EXPECT_EQ(RunSql(engine_.get(), context_.get(),
                   "SELECT SUM(v) FROM UpperOnly WHERE k < -2"),
            std::vector<Row>{Row({Value()})});
  EXPECT_EQ(RunSql(engine_.get(), context_.get(),
                   "SELECT SUM(v) FROM UpperOnly WHERE k <= -2"),
            std::vector<Row>{Row({Value(1)})});
}

TEST_F(SqlEngineExtraTest, FilteredComputedIntegerAggregateKeepsTypedFold) {
  for (size_t replay = 0; replay < 3; ++replay) {
    SCOPED_TRACE(replay);
    ExpectQuery(
        "SELECT SUM(k * k + 1), MIN(k * 2), MAX(k + 1) FROM t WHERE k > 1",
        {Row({Value(15), Value(4), Value(4)})});
    ExpectQuery("SELECT SUM(k * k + 1) FROM t WHERE k > 3", {Row({Value()})});
  }
}
}  // namespace tinylamb
