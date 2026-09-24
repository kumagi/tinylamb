#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

std::vector<Row> RunSql(SqlEngine& engine, TransactionContext& context,
                        std::string_view sql) {
  StatusOr<Executor> prepared = engine.Prepare(context, sql);
  EXPECT_TRUE(prepared.HasValue()) << engine.LastError();
  if (!prepared.HasValue()) {
    return {};
  }
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
    row = Row();
  }
  EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess);
  return rows;
}

class TableSampleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP();
    }
    database_ =
        Database::Create("table_sample_test-" + RandomString()).MoveValue();
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

  void Seed() {
    RunSql(*engine_, *context_, "CREATE TABLE sample_t (id INT64, grp INT64)");
    RunSql(*engine_, *context_,
           "INSERT INTO sample_t VALUES (1, 10), (2, 10), (3, 20), "
           "(4, 20), (5, 30)");
    ASSERT_EQ(context_->txn_.PreCommit(), Status::kSuccess);
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(TableSampleTest, ReservoirRepeatableIsStable) {
  Seed();
  const std::string sql =
      "SELECT id FROM sample_t TABLESAMPLE RESERVOIR(2 ROWS) "
      "REPEATABLE(42)";
  const std::vector<Row> first = RunSql(*engine_, *context_, sql);
  const std::vector<Row> second = RunSql(*engine_, *context_, sql);
  ASSERT_EQ(first.size(), 2U);
  EXPECT_EQ(first, second);
}

TEST_F(TableSampleTest, ReservoirPartitionWeight) {
  Seed();
  const std::vector<Row> rows =
      RunSql(*engine_, *context_,
             "SELECT grp, w FROM sample_t TABLESAMPLE RESERVOIR(1 ROWS "
             "PARTITION BY grp) WITH WEIGHT AS w");
  ASSERT_EQ(rows.size(), 3U);
  std::vector<std::pair<int64_t, double>> actual;
  for (const Row& row : rows) {
    ASSERT_EQ(row.values_.size(), 2U);
    actual.emplace_back(row[0].value.int_value, row[1].value.double_value);
  }
  std::ranges::sort(actual);
  const std::vector<std::pair<int64_t, double>> expected{
      {10, 2.0}, {20, 2.0}, {30, 1.0}};
  EXPECT_EQ(actual, expected);
}

TEST_F(TableSampleTest, BernoulliFullWeight) {
  Seed();
  const std::vector<Row> rows =
      RunSql(*engine_, *context_,
             "SELECT id, w FROM sample_t TABLESAMPLE BERNOULLI(100 PERCENT) "
             "WITH WEIGHT AS w");
  ASSERT_EQ(rows.size(), 5U);
  for (const Row& row : rows) {
    ASSERT_EQ(row.values_.size(), 2U);
    EXPECT_EQ(row[1], Value(1.0));
  }
}

TEST_F(TableSampleTest, SystemMethodIsRejected) {
  Seed();
  StatusOr<Executor> prepared = engine_->Prepare(
      *context_, "SELECT id FROM sample_t TABLESAMPLE SYSTEM(10 PERCENT)");
  ASSERT_TRUE(prepared.HasValue()) << engine_->LastError();
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
  }
  EXPECT_EQ(prepared.Value()->GetStatus(), Status::kNotImplemented);
}

TEST_F(TableSampleTest, DerivedRelationUsesRelationalSamplePath) {
  Seed();
  const std::vector<Row> rows = RunSql(
      *engine_, *context_,
      "SELECT id FROM (SELECT id FROM sample_t) TABLESAMPLE RESERVOIR(2 ROWS) "
      "REPEATABLE(42)");
  ASSERT_EQ(rows.size(), 2U);
}

}  // namespace
}  // namespace tinylamb
