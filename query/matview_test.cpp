/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

class MaterializedViewTest : public ::testing::Test {
 protected:
  void SetUp() override {
    database_name_ = "matview_test-" + RandomString(8);
    auto database_or = Database::Create(database_name_);
    ASSERT_TRUE(database_or.HasValue()) << database_or.GetStatus();
    database_ = std::move(database_or.Value());
    // One long-lived context per test: DDL and DML share the session's
    // transaction the way a client session would (the pattern the other
    // frontend test fixtures use).
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

  std::vector<Row> Run(const std::string& sql) {
    StatusOr<Executor> prepared = engine_->Prepare(*context_, sql);
    if (!prepared.HasValue()) {
      last_error_ = engine_->LastError();
      ADD_FAILURE() << sql << " failed: " << last_error_;
      return {};
    }
    std::vector<Row> rows;
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      rows.push_back(row);
      row = Row();
    }
    if (prepared.Value()->GetStatus() != Status::kSuccess) {
      last_error_ = "executor error";
      ADD_FAILURE() << sql << " executor error";
      return {};
    }
    return rows;
  }

  bool Fails(const std::string& sql) {
    StatusOr<Executor> prepared = engine_->Prepare(*context_, sql);
    if (!prepared.HasValue()) {
      last_error_ = engine_->LastError();
      return true;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    if (prepared.Value()->GetStatus() != Status::kSuccess) {
      last_error_ = "executor error";
      return true;
    }
    return false;
  }

  std::string database_name_;
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
  std::string last_error_;
};

}  // namespace

// CREATE MATERIALIZED VIEW materializes the query into a catalog table that
// answers queries like any base relation, persists the defining text, and
// cleans up on DROP.  Staleness is manual: rows only change through an
// explicit recreate (or base-table writes followed by a recreate).
TEST_F(MaterializedViewTest, CreateQueryDropRoundTrip) {
  Run("CREATE TABLE base_t (k INT64, v STRING);");
  Run("INSERT INTO base_t VALUES (1, 'a'), (2, 'b'), (3, 'c');");
  const std::vector<Row> created =
      Run("CREATE MATERIALIZED VIEW mv_agg AS "
          "SELECT v, COUNT(*) AS c, SUM(k) AS sk FROM base_t "
          "GROUP BY v;");
  ASSERT_EQ(created.size(), 1U);
  EXPECT_EQ(std::string_view(created[0][0].value.varchar_value),
            "CREATE MATERIALIZED VIEW");
  EXPECT_EQ(created[0][1].value.int_value, 3);

  const std::vector<Row> scanned =
      Run("SELECT v, c, sk FROM mv_agg ORDER BY v;");
  ASSERT_EQ(scanned.size(), 3U);
  EXPECT_EQ(std::string_view(scanned[0][0].value.varchar_value), "a");
  EXPECT_EQ(scanned[0][1].value.int_value, 1);
  EXPECT_EQ(scanned[0][2].value.int_value, 1);
  EXPECT_EQ(std::string_view(scanned[2][0].value.varchar_value), "c");
  EXPECT_EQ(scanned[2][1].value.int_value, 1);
  EXPECT_EQ(scanned[2][2].value.int_value, 3);

  // The MV joins like a base table.
  const std::vector<Row> joined =
      Run("SELECT b.k, m.c FROM base_t b JOIN mv_agg m "
          "ON b.v = m.v WHERE b.k = 2;");
  ASSERT_EQ(joined.size(), 1U);
  EXPECT_EQ(joined[0][0].value.int_value, 2);
  EXPECT_EQ(joined[0][1].value.int_value, 1);

  // Base-table writes do not flow through (manual staleness).
  Run("INSERT INTO base_t VALUES (4, 'a');");
  const std::vector<Row> stale = Run("SELECT c FROM mv_agg WHERE v = 'a';");
  ASSERT_EQ(stale.size(), 1U);
  EXPECT_EQ(stale[0][0].value.int_value, 1);

  // DROP removes the rows and the definition.
  const std::vector<Row> dropped = Run("DROP MATERIALIZED VIEW mv_agg;");
  ASSERT_EQ(dropped.size(), 1U);
  EXPECT_EQ(std::string_view(dropped[0][0].value.varchar_value),
            "DROP MATERIALIZED VIEW");
  EXPECT_TRUE(Fails("SELECT * FROM mv_agg;"));
  EXPECT_TRUE(Fails("DROP MATERIALIZED VIEW mv_agg;"));
}

TEST_F(MaterializedViewTest, CreateNameRules) {
  Run("CREATE TABLE base_t (k INT64);");
  Run("INSERT INTO base_t VALUES (1);");
  Run("CREATE MATERIALIZED VIEW mv1 AS SELECT k FROM base_t;");
  // Duplicate without IF NOT EXISTS is a conflict...
  EXPECT_TRUE(Fails("CREATE MATERIALIZED VIEW mv1 AS SELECT k FROM base_t;"));
  // ...and IF NOT EXISTS is a silent no-op that keeps the old definition.
  const std::vector<Row> idempotent =
      Run("CREATE MATERIALIZED VIEW IF NOT EXISTS mv1 AS SELECT k + 1 AS k "
          "FROM base_t;");
  ASSERT_EQ(idempotent.size(), 1U);
  const std::vector<Row> kept = Run("SELECT k FROM mv1;");
  ASSERT_EQ(kept.size(), 1U);
  EXPECT_EQ(kept[0][0].value.int_value, 1);
  // A plain table of the same name blocks creation too.
  Run("CREATE TABLE plain_t (k INT64);");
  EXPECT_TRUE(
      Fails("CREATE MATERIALIZED VIEW plain_t AS SELECT k FROM base_t;"));
  // DROP MATERIALIZED VIEW must not accept a non-materialized table.
  EXPECT_TRUE(Fails("DROP MATERIALIZED VIEW plain_t;"));
  EXPECT_TRUE(Fails("DROP MATERIALIZED VIEW no_such_mv;"));
}

TEST_F(MaterializedViewTest, DefinitionPersistsAcrossRestart) {
  Run("CREATE TABLE base_t (k INT64);");
  Run("INSERT INTO base_t VALUES (7);");
  Run("CREATE MATERIALIZED VIEW mv_p AS SELECT k FROM base_t;");
  // Reopen: both the stored rows and the definition survive.  PreCommit the
  // session, then close (a PagePool holds the page file exclusively).
  ASSERT_EQ(context_->PreCommit(), Status::kSuccess);
  engine_.reset();
  context_.reset();
  database_.reset();
  auto reopened = Database::Create(database_name_);
  ASSERT_TRUE(reopened.HasValue()) << reopened.GetStatus();
  database_ = std::move(reopened.Value());
  context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  engine_ = std::make_unique<SqlEngine>(*database_);
  const std::vector<Row> scanned = Run("SELECT k FROM mv_p;");
  ASSERT_EQ(scanned.size(), 1U);
  EXPECT_EQ(scanned[0][0].value.int_value, 7);
  const std::vector<Row> dropped = Run("DROP MATERIALIZED VIEW mv_p;");
  ASSERT_EQ(dropped.size(), 1U);
}

// Recreating after a DROP refreshes the content: the documented manual
// refresh path is DROP + CREATE with the same definition.
TEST_F(MaterializedViewTest, RefreshByRecreateSeesBaseWrites) {
  Run("CREATE TABLE base_t (k INT64);");
  Run("INSERT INTO base_t VALUES (1);");
  Run("CREATE MATERIALIZED VIEW mv_r AS SELECT COUNT(*) AS c FROM base_t;");
  Run("INSERT INTO base_t VALUES (2);");
  Run("DROP MATERIALIZED VIEW mv_r;");
  Run("CREATE MATERIALIZED VIEW mv_r AS SELECT COUNT(*) AS c FROM base_t;");
  const std::vector<Row> refreshed = Run("SELECT c FROM mv_r;");
  ASSERT_EQ(refreshed.size(), 1U);
  EXPECT_EQ(refreshed[0][0].value.int_value, 2);
}

}  // namespace tinylamb
