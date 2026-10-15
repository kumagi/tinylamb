/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <gtest/gtest.h>

#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "index/index_schema.hpp"
#include "plan/cascades.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

std::vector<Row> RunSql(TransactionContext& context, Database& database,
                        std::string_view sql) {
  SqlEngine engine(database);
  StatusOr<Executor> prepared = engine.Prepare(context, sql);
  EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << sql << "\n"
                                                    << engine.LastError();
  std::vector<Row> rows;
  if (!prepared.HasValue()) {
    return rows;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
    row = Row();
  }
  return rows;
}

class LateMaterializationQueryTest : public ::testing::Test {
 public:
  void SetUp() override {
    database_ =
        Database::Create("late_materialization_query_test-" + RandomString())
            .MoveValue();
  }

  void TearDown() override { database_->DeleteAll(); }

  std::unique_ptr<Database> database_;
};

TEST_F(LateMaterializationQueryTest,
       SqlJoinUsesLateMaterializationAndKeepsResults) {
  TransactionContext setup = database_->BeginContext();
  RunSql(setup, *database_,
         "CREATE TABLE lm_fact (k INT64, fact_value INT64);");
  RunSql(setup, *database_,
         "CREATE TABLE lm_dim (k INT64, a INT64, b INT64, c INT64, d "
         "INT64, e INT64, f INT64);");
  {
    std::string values;
    for (int64_t i = 0; i < 40; ++i) {
      if (!values.empty()) {
        values += ", ";
      }
      values +=
          "(" + std::to_string(i % 10) + ", " + std::to_string(1000 + i) + ")";
    }
    RunSql(setup, *database_,
           "INSERT INTO lm_fact (k, fact_value) VALUES " + values + ";");
  }
  RunSql(setup, *database_,
         "INSERT INTO lm_dim (k, a, b, c, d, e, f) VALUES (1, 10, 20, "
         "30, 40, 50, 60), (2, 11, 21, 31, 41, 51, 61), (3, 12, 22, "
         "32, 42, 52, 62), (4, 13, 23, 33, 43, 53, 63), (NULL, 99, 99, "
         "99, 99, 99, 99);");
  ASSERT_SUCCESS(database_->CreateIndex(
      setup, "lm_dim",
      IndexSchema("lm_dim_k", {0}, {}, IndexMode::kNonUnique)));
  ASSERT_SUCCESS(setup.txn_.PreCommit());

  TransactionContext statistics = database_->BeginContext();
  database_->RefreshStatistics(statistics, "lm_fact");
  database_->RefreshStatistics(statistics, "lm_dim");
  ASSERT_SUCCESS(statistics.txn_.PreCommit());

  TransactionContext context = database_->BeginContext();
  const std::string join_sql =
      "SELECT f.k, d.a FROM lm_fact f JOIN lm_dim d ON f.k = d.k ORDER BY "
      "f.k, d.a;";
  const std::vector<Row> rows = RunSql(context, *database_, join_sql);
  ASSERT_EQ(rows.size(), 16U);
  for (const Row& row : rows) {
    EXPECT_EQ(row[0].value.int_value, row[1].value.int_value - 9);
  }
  EXPECT_EQ(rows.front(), (Row({Value(1), Value(10)})));
  EXPECT_EQ(rows.back(), (Row({Value(4), Value(13)})));

  SqlEngine engine(*database_);
  StatusOr<Executor> explained = engine.Prepare(context, "EXPLAIN " + join_sql);
  ASSERT_TRUE(explained.HasValue()) << engine.LastError();
  std::string plan_text;
  Row plan_row;
  while (explained.Value()->Next(&plan_row, nullptr)) {
    plan_text += plan_row[0].AsString() + "\n";
  }
  EXPECT_NE(plan_text.find("LateMaterializationJoin"), std::string::npos)
      << plan_text;

  RunSql(context, *database_, "CREATE TABLE lm_empty (k INT64, a INT64);");
  ASSERT_SUCCESS(database_->CreateIndex(
      context, "lm_empty",
      IndexSchema("lm_empty_k", {0}, {}, IndexMode::kNonUnique)));
  EXPECT_TRUE(RunSql(context, *database_,
                     "SELECT f.k FROM lm_fact f JOIN lm_empty d ON f.k = "
                     "d.k;")
                  .empty());
  ASSERT_SUCCESS(context.txn_.PreCommit());
}

TEST_F(LateMaterializationQueryTest, OuterJoinFallsBackAndNullKeysNeverMatch) {
  TransactionContext setup = database_->BeginContext();
  RunSql(setup, *database_, "CREATE TABLE lm_left (k INT64, v INT64);");
  RunSql(setup, *database_,
         "CREATE TABLE lm_right (k INT64, a INT64, b INT64, "
         "c INT64, d INT64, e INT64, f INT64);");
  RunSql(setup, *database_,
         "INSERT INTO lm_left (k, v) VALUES (1, 10), (2, 20), (NULL, 30);");
  RunSql(setup, *database_,
         "INSERT INTO lm_right (k, a, b, c, d, e, f) VALUES (1, 11, 12, 13, "
         "14, 15, 16), (NULL, 21, 22, 23, 24, 25, 26);");
  ASSERT_SUCCESS(database_->CreateIndex(
      setup, "lm_right",
      IndexSchema("lm_right_k", {0}, {}, IndexMode::kNonUnique)));
  ASSERT_SUCCESS(setup.txn_.PreCommit());
  TransactionContext statistics = database_->BeginContext();
  database_->RefreshStatistics(statistics, "lm_left");
  database_->RefreshStatistics(statistics, "lm_right");
  ASSERT_SUCCESS(statistics.txn_.PreCommit());

  TransactionContext context = database_->BeginContext();
  const std::vector<Row> inner = RunSql(
      context, *database_,
      "SELECT l.k, r.a FROM lm_left l JOIN lm_right r ON l.k = r.k ORDER BY "
      "l.k;");
  ASSERT_EQ(inner.size(), 1U);
  EXPECT_EQ(inner[0], (Row({Value(1), Value(11)})));

  const std::vector<Row> outer = RunSql(
      context, *database_,
      "SELECT l.k, r.a FROM lm_left l LEFT JOIN lm_right r ON l.k = r.k ORDER "
      "BY l.k;");
  ASSERT_EQ(outer.size(), 3U);
  size_t null_payloads = 0;
  bool found_match = false;
  for (const Row& row : outer) {
    if (row[0].value.int_value == 1 && row[1].value.int_value == 11) {
      found_match = true;
    }
    if (row[1].IsNull()) {
      ++null_payloads;
    }
  }
  EXPECT_TRUE(found_match);
  EXPECT_EQ(null_payloads, 2U);

  SqlEngine engine(*database_);
  StatusOr<Executor> explained = engine.Prepare(
      context,
      "EXPLAIN SELECT l.k, r.a FROM lm_left l LEFT JOIN lm_right r ON l.k = "
      "r.k;");
  ASSERT_TRUE(explained.HasValue()) << engine.LastError();
  std::string plan_text;
  Row plan_row;
  while (explained.Value()->Next(&plan_row, nullptr)) {
    plan_text += plan_row[0].AsString() + "\n";
  }
  EXPECT_EQ(plan_text.find("LateMaterializationJoin"), std::string::npos)
      << plan_text;
  ASSERT_SUCCESS(context.txn_.PreCommit());
}

}  // namespace
}  // namespace tinylamb
