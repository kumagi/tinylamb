/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// ALTER TABLE: catalog-level actions (RENAME TO, RENAME COLUMN).  Row-layout
// actions (ADD/DROP COLUMN) stay explicit errors until a rebuild path lands.

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

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

std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    throw std::runtime_error(prepared.GetStatus().GetMessage());
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

class AlterTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_name_ = "alter-" + RandomString();
    database_ = Database::Create(database_name_).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE base (k INT64, v STRING(MAX));");
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO base VALUES (1, 'one'), (2, 'two');");
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  void Commit() { ASSERT_EQ(context_->PreCommit(), Status::kSuccess); }

  std::unique_ptr<Database> database_;
  std::string database_name_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(AlterTableTest, RenameTableMovesRowsAndDropsOldName) {
  RunSql(engine_.get(), context_.get(), "ALTER TABLE base RENAME TO moved;");
  const auto rows = RunSql(engine_.get(), context_.get(),
                           "SELECT k, v FROM moved ORDER BY k;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][1], (Value("one")));
  // The old name is gone in the same transaction.
  StatusOr<Executor> stale = engine_->Prepare(*context_, "SELECT * FROM base;");
  EXPECT_FALSE(stale.HasValue());
  Commit();
  // The rename persists across a reopen (close first: WAL single-writer).
  engine_.reset();
  context_.reset();
  database_.reset();
  auto reopened = Database::Create(database_name_);
  ASSERT_TRUE(reopened.HasValue()) << reopened.GetStatus();
  TransactionContext ctx(reopened.Value()->BeginContext());
  SqlEngine engine(*reopened.Value());
  EXPECT_EQ(RunSql(&engine, &ctx, "SELECT COUNT(*) FROM moved;")[0][0],
            (Value(int64_t{2})));
}

TEST_F(AlterTableTest, RenameColumnKeepsDataAndIndexPositions) {
  RunSql(engine_.get(), context_.get(),
         "ALTER TABLE base RENAME COLUMN v TO label;");
  const auto rows = RunSql(engine_.get(), context_.get(),
                           "SELECT k, label FROM base ORDER BY k;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[1][1], (Value("two")));
  // The old column name no longer resolves.
  StatusOr<Executor> stale = engine_->Prepare(*context_, "SELECT v FROM base;");
  EXPECT_FALSE(stale.HasValue());
}

TEST_F(AlterTableTest, RenameTableRejectsForeignKeyParent) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE child (k INT64, fk INT64, FOREIGN KEY (fk) "
         "REFERENCES base(k));");
  try {
    RunSql(engine_.get(), context_.get(), "ALTER TABLE base RENAME TO parent;");
    FAIL() << "expected a foreign key rejection";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("FOREIGN KEY"), std::string::npos)
        << error.what();
  }
}

TEST_F(AlterTableTest, RenameTableRejectsExistingTarget) {
  RunSql(engine_.get(), context_.get(), "CREATE TABLE other (k INT64);");
  try {
    RunSql(engine_.get(), context_.get(), "ALTER TABLE base RENAME TO other;");
    FAIL() << "expected a duplicate-name rejection";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("already exists"),
              std::string::npos)
        << error.what();
  }
}

TEST_F(AlterTableTest, RenameColumnRejectsDuplicateTarget) {
  try {
    RunSql(engine_.get(), context_.get(),
           "ALTER TABLE base RENAME COLUMN v TO k;");
    FAIL() << "expected a duplicate-column rejection";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("already exists"),
              std::string::npos)
        << error.what();
  }
}

TEST_F(AlterTableTest, RowLayoutActionsStayExplicitErrors) {
  for (const char* sql : {"ALTER TABLE base ADD COLUMN extra STRING;",
                          "ALTER TABLE base DROP COLUMN v;"}) {
    SCOPED_TRACE(sql);
    StatusOr<Executor> prepared = engine_->Prepare(*context_, sql);
    ASSERT_FALSE(prepared.HasValue());
    const std::string message = engine_->LastError().empty()
                                    ? prepared.GetStatus().GetMessage()
                                    : engine_->LastError();
    EXPECT_NE(message.find("not supported"), std::string::npos) << message;
  }
}

}  // namespace tinylamb
