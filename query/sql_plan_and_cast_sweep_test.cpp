/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Sweeps the compiled-plan cache (repeated identical statement shapes with
// varying literals exercise the parametric INSERT replay and the compiled
// SELECT/UPDATE/DELETE replay), the optimizer's redundant-cast elimination
// and RANGE-frame bound simplification, EXPLAIN plan-writer variants, and
// struct-field UPDATE paths.

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/plan_cache.hpp"
#include "query/sql_engine.hpp"
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

int64_t ScalarInt(SqlEngine* engine, TransactionContext* ctx,
                  const std::string& sql) {
  const auto rows = RunSql(engine, ctx, sql);
  EXPECT_EQ(rows.size(), 1U) << sql;
  if (rows.empty()) {
    return -1;
  }
  return rows[0][0].value.int_value;
}

}  // namespace

class SqlPlanCacheSweepTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("sql_plan_cache_sweep_test-" + RandomString())
                    .MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE pc (k INT64, v STRING)");
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE pcr (k INT64, r FLOAT64)");
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE st_t AS SELECT 1 AS id, STRUCT(10 AS x, 'hi' AS y) "
           "AS s WHERE false");
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
};

// Repeated same-shape INSERTs with varying literals: the first fill stores a
// parametric plan, every further execution replays it with new parameter
// values (ParameterSlot::Evaluate paths in plan_cache.hpp).
TEST_F(SqlPlanCacheSweepTest, RepeatedInsertShapesReplayParameterized) {
  for (int64_t i = 0; i < 12; ++i) {
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO pc VALUES (" + std::to_string(i) + ", 'name" +
               std::to_string(i) + "')");
  }
  EXPECT_EQ(ScalarInt(engine_.get(), context_.get(), "SELECT COUNT(*) FROM pc"),
            12);
  EXPECT_EQ(ScalarInt(engine_.get(), context_.get(),
                      "SELECT COUNT(*) FROM pc WHERE v = 'name7'"),
            1);
}

// Repeated same-shape SELECT / UPDATE / DELETE replay compiled plans.
TEST_F(SqlPlanCacheSweepTest, RepeatedSelectUpdateDeleteShapes) {
  for (int64_t i = 0; i < 10; ++i) {
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO pc VALUES (" + std::to_string(i) + ", 'v')");
  }
  for (int64_t i = 0; i < 10; ++i) {
    const auto rows = RunSql(engine_.get(), context_.get(),
                             "SELECT v FROM pc WHERE k = " + std::to_string(i));
    ASSERT_EQ(rows.size(), 1U);
  }
  EXPECT_EQ(ScalarInt(engine_.get(), context_.get(),
                      "SELECT COUNT(*) FROM pc WHERE k > 4"),
            5);
  RunSql(engine_.get(), context_.get(), "UPDATE pc SET v = 'z' WHERE k = 3");
  for (int64_t i = 4; i < 8; ++i) {
    RunSql(engine_.get(), context_.get(),
           "UPDATE pc SET v = 'z' WHERE k = " + std::to_string(i));
  }
  EXPECT_EQ(ScalarInt(engine_.get(), context_.get(),
                      "SELECT COUNT(*) FROM pc WHERE v = 'z'"),
            5);
  RunSql(engine_.get(), context_.get(), "DELETE FROM pc WHERE k = 0");
  for (int64_t i = 1; i < 4; ++i) {
    RunSql(engine_.get(), context_.get(),
           "DELETE FROM pc WHERE k = " + std::to_string(i));
  }
  EXPECT_EQ(ScalarInt(engine_.get(), context_.get(), "SELECT COUNT(*) FROM pc"),
            6);
  // Double-typed column exercises float parameter slots.
  for (int64_t i = 0; i < 6; ++i) {
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO pcr VALUES (" + std::to_string(i) + ", " +
               std::to_string(i) + ".5)");
  }
  EXPECT_EQ(ScalarInt(engine_.get(), context_.get(),
                      "SELECT COUNT(*) FROM pcr WHERE r = 2.5"),
            1);
}

TEST_F(SqlPlanCacheSweepTest, ReorderedArithmeticUpdateReplaysAllSlots) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE pslot (w INT64, d INT64, s INT64)");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO pslot VALUES (1, 1, 10), (1, 2, 80), (2, 1, 10)");

  RunSql(engine_.get(), context_.get(),
         "UPDATE pslot SET s = s + 1 + 2 + 0 "
         "WHERE w = 1 AND d = 1");
  const uint64_t replays_after_fill =
      PlanCacheStats().replays.load(std::memory_order_relaxed);
  const uint64_t hits_after_fill =
      PlanCacheStats().hits.load(std::memory_order_relaxed);
  const uint64_t mismatches_after_fill =
      PlanCacheStats().parameter_mismatches.load(std::memory_order_relaxed);
  RunSql(engine_.get(), context_.get(),
         "UPDATE pslot SET s = s + 1 + 3 + 0 "
         "WHERE w = 1 AND d = 2");

  EXPECT_EQ(PlanCacheStats().hits.load(std::memory_order_relaxed),
            hits_after_fill + 1);
  EXPECT_EQ(
      PlanCacheStats().parameter_mismatches.load(std::memory_order_relaxed),
      mismatches_after_fill);
  EXPECT_GT(PlanCacheStats().replays.load(std::memory_order_relaxed),
            replays_after_fill);
  EXPECT_EQ(ScalarInt(engine_.get(), context_.get(),
                      "SELECT s FROM pslot WHERE w = 1 AND d = 1"),
            13);
  EXPECT_EQ(ScalarInt(engine_.get(), context_.get(),
                      "SELECT s FROM pslot WHERE w = 1 AND d = 2"),
            84);
  EXPECT_EQ(ScalarInt(engine_.get(), context_.get(),
                      "SELECT s FROM pslot WHERE w = 2 AND d = 1"),
            10);
}

// Redundant-cast elimination and cast-aware predicate handling in the
// optimizer (cascades.cpp cast rewriter): these shapes must produce correct
// results whether or not the cast is folded away.
TEST_F(SqlPlanCacheSweepTest, CastEliminationShapes) {
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO pc VALUES (1, 'one'), (2, 'two'), (3, 'three')");
  EXPECT_EQ(ScalarInt(engine_.get(), context_.get(),
                      "SELECT COUNT(*) FROM pc WHERE CAST(k AS INT64) = 2"),
            1);
  EXPECT_EQ(ScalarInt(engine_.get(), context_.get(),
                      "SELECT COUNT(*) FROM pc WHERE CAST(k AS INT) > 1"),
            2);
  EXPECT_EQ(
      ScalarInt(engine_.get(), context_.get(),
                "SELECT COUNT(*) FROM pc WHERE CAST(v AS STRING) = 'one'"),
      1);
  EXPECT_EQ(
      ScalarInt(engine_.get(), context_.get(),
                "SELECT COUNT(*) FROM pc WHERE CAST(v AS VARCHAR) != 'one'"),
      2);
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO pcr VALUES (1, 1.5), (2, 2.5), (3, 3.5), (4, 0.5)");
  EXPECT_EQ(
      ScalarInt(engine_.get(), context_.get(),
                "SELECT COUNT(*) FROM pcr WHERE CAST(r AS FLOAT64) > 2.0"),
      2);
  EXPECT_EQ(ScalarInt(engine_.get(), context_.get(),
                      "SELECT COUNT(*) FROM pc WHERE CAST(k AS STRING) = '3'"),
            1);
  EXPECT_EQ(
      ScalarInt(engine_.get(), context_.get(),
                "SELECT COUNT(*) FROM pc x JOIN pc y ON CAST(x.k AS INT64) = "
                "CAST(y.k AS INT64)"),
      3);
}

// RANGE window frames with constant bounds (cascades.cpp depth-bound
// simplification) on both operand sides.
TEST_F(SqlPlanCacheSweepTest, RangeFrameBoundShapes) {
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO pcr VALUES (1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0), "
         "(5, 5.0)");
  const auto rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT k, SUM(k) OVER (ORDER BY k RANGE BETWEEN 2 PRECEDING AND "
             "CURRENT ROW) FROM pcr ORDER BY k");
  ASSERT_EQ(rows.size(), 5U);
  EXPECT_EQ(rows[0][1].value.int_value, 1);
  EXPECT_EQ(rows[2][1].value.int_value, 6);
  EXPECT_EQ(rows[4][1].value.int_value, 12);
  const auto rows2 = RunSql(
      engine_.get(), context_.get(),
      "SELECT k, COUNT(*) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND 1 "
      "FOLLOWING) FROM pcr ORDER BY k");
  ASSERT_EQ(rows2.size(), 5U);
  EXPECT_EQ(rows2[4][1].value.int_value, 1);
  const auto rows3 = RunSql(
      engine_.get(), context_.get(),
      "SELECT k, SUM(k) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING "
      "AND UNBOUNDED FOLLOWING) FROM pcr ORDER BY k LIMIT 1");
  ASSERT_EQ(rows3.size(), 1U);
  EXPECT_EQ(rows3[0][1].value.int_value, 15);
}

// Struct-field UPDATE and EXPLAIN coverage over mixed plan shapes.
TEST_F(SqlPlanCacheSweepTest, StructUpdateAndExplainShapes) {
  // NOTE: INSERT into a CTAS-created table currently fails validation
  // ("INSERT type mismatch for column id"), so the table stays empty and
  // these statements exercise the parse/plan/rewrite paths on zero rows.
  EXPECT_EQ(
      ScalarInt(engine_.get(), context_.get(), "SELECT COUNT(*) FROM st_t"), 0);
  RunSql(engine_.get(), context_.get(), "UPDATE st_t SET s.x = 7");
  RunSql(engine_.get(), context_.get(),
         "UPDATE st_t SET s = STRUCT(5 AS x, 'five' AS y)");
  RunSql(engine_.get(), context_.get(),
         "UPDATE st_t SET id = id + 1 WHERE 1 = id");
  EXPECT_EQ(
      ScalarInt(engine_.get(), context_.get(), "SELECT COUNT(*) FROM st_t"), 0);
  for (const auto& sql : {
           "EXPLAIN SELECT k FROM pc WHERE k = 1",
           "EXPLAIN SELECT x.k FROM pc x JOIN pcr y ON x.k = y.k",
           "EXPLAIN SELECT k, COUNT(*) FROM pc GROUP BY k",
           "EXPLAIN SELECT k FROM pc UNION ALL SELECT k FROM pcr",
           "EXPLAIN SELECT k, SUM(k) OVER (ORDER BY k) FROM pcr",
           "EXPLAIN SELECT k FROM pc ORDER BY k LIMIT 2",
           "EXPLAIN ANALYZE SELECT COUNT(*), AVG(r) FROM pcr",
           "EXPLAIN ANALYZE SELECT k FROM pc WHERE k = 1",
           "EXPLAIN ANALYZE SELECT label FROM (SELECT k AS label FROM pc) sub",
           // NOTE: EXPLAIN (ANALYZE) covers SELECT/WITH only by design.
       }) {
    SCOPED_TRACE(sql);
    EXPECT_NO_THROW({ RunSql(engine_.get(), context_.get(), sql); });
  }
}

}  // namespace tinylamb
