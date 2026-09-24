/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/column.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
// Executes one statement and returns the materialized rows.  A prepare or
// execution failure surfaces as a runtime_error carrying the diagnostic.
std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    // Prepare-time DDL failures carry the diagnostic in the Status;
    // LastError is only populated on some paths.
    const std::string& last = engine->LastError();
    throw std::runtime_error(last.empty() ? prepared.GetStatus().GetMessage()
                                          : last);
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

std::string ExpectThrow(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  try {
    RunSql(engine, ctx, sql);
  } catch (const std::runtime_error& e) {
    return e.what();
  }
  return "";
}

void ExpectOk(SqlEngine* engine, TransactionContext* ctx,
              const std::string& sql) {
  EXPECT_EQ(ExpectThrow(engine, ctx, sql), "") << sql;
}

void ExpectViolation(SqlEngine* engine, TransactionContext* ctx,
                     const std::string& sql) {
  const std::string error = ExpectThrow(engine, ctx, sql);
  EXPECT_NE(error.find("foreign key"), std::string::npos)
      << sql << " :: " << error;
}
}  // namespace

class ForeignKeyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    // ctest runs each case as its own process in the same directory and may
    // run them in parallel: the database file name must be unique per test,
    // otherwise concurrent cases stomp on each other's files.
    const ::testing::TestInfo* info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    db_name_ = std::string("sql_foreign_key_test_") +
               (info != nullptr ? info->name() : "unknown");
    database_ = Database::Create(db_name_).MoveValue();
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
  void MakeParentChild() {
    ExpectOk(engine_.get(), context_.get(),
             "CREATE TABLE fk_parent (id INT64)");
    ExpectOk(engine_.get(), context_.get(),
             "CREATE TABLE fk_child (id INT64, pid INT64, FOREIGN KEY (pid) "
             "REFERENCES fk_parent(id))");
    ExpectOk(engine_.get(), context_.get(),
             "INSERT INTO fk_parent VALUES (1), (2)");
  }
  std::string db_name_;
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(ForeignKeyTest, TableLevelDefinitionStoredCanonically) {
  ASSERT_NO_THROW(MakeParentChild());
  StatusOr<std::shared_ptr<Table>> child = context_->GetTable("fk_child");
  ASSERT_TRUE(child.HasValue());
  const Column& pid = child.Value()->GetSchema().GetColumn(1);
  EXPECT_EQ(pid.GetConstraint().ctype, Constraint::kForeign);
  std::string table;
  std::vector<std::string> columns;
  ASSERT_TRUE(ParseForeignKeyReference(pid.GetConstraint(), &table, &columns));
  EXPECT_EQ(table, "fk_parent");
  ASSERT_EQ(columns.size(), 1U);
  EXPECT_EQ(columns[0], "id");
}

TEST_F(ForeignKeyTest, ColumnLevelReferencesAccepted) {
  ExpectOk(engine_.get(), context_.get(), "CREATE TABLE fk_p2 (id INT64)");
  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_c2 (id INT64, pid INT64 REFERENCES fk_p2(id))");
  ExpectOk(engine_.get(), context_.get(), "INSERT INTO fk_p2 VALUES (1)");
  ExpectOk(engine_.get(), context_.get(), "INSERT INTO fk_c2 VALUES (1, 1)");
  ExpectViolation(engine_.get(), context_.get(),
                  "INSERT INTO fk_c2 VALUES (2, 9)");
}

TEST_F(ForeignKeyTest, InsertValidAndNullPass) {
  ASSERT_NO_THROW(MakeParentChild());
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_child VALUES (10, 1), (11, NULL)");
  const std::vector<Row> rows = RunSql(engine_.get(), context_.get(),
                                       "SELECT id FROM fk_child ORDER BY id");
  ASSERT_EQ(rows.size(), 2U);
}

TEST_F(ForeignKeyTest, InsertViolationRejected) {
  ASSERT_NO_THROW(MakeParentChild());
  ExpectViolation(engine_.get(), context_.get(),
                  "INSERT INTO fk_child VALUES (10, 9)");
}

TEST_F(ForeignKeyTest, UpdateChildKeyEnforced) {
  ASSERT_NO_THROW(MakeParentChild());
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_child VALUES (10, 1)");
  ExpectOk(engine_.get(), context_.get(),
           "UPDATE fk_child SET pid = 2 WHERE id = 10");
  ExpectViolation(engine_.get(), context_.get(),
                  "UPDATE fk_child SET pid = 9 WHERE id = 10");
}

TEST_F(ForeignKeyTest, DeleteReferencedParentRejected) {
  ASSERT_NO_THROW(MakeParentChild());
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_child VALUES (10, 1)");
  ExpectViolation(engine_.get(), context_.get(),
                  "DELETE FROM fk_parent WHERE id = 1");
  ExpectOk(engine_.get(), context_.get(), "DELETE FROM fk_parent WHERE id = 2");
}

TEST_F(ForeignKeyTest, UpdateReferencedParentKeyRejected) {
  ASSERT_NO_THROW(MakeParentChild());
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_child VALUES (10, 1)");
  ExpectViolation(engine_.get(), context_.get(),
                  "UPDATE fk_parent SET id = 7 WHERE id = 1");
  ExpectOk(engine_.get(), context_.get(),
           "UPDATE fk_parent SET id = 8 WHERE id = 2");
}

TEST_F(ForeignKeyTest, DropReferencedTableBlocked) {
  ASSERT_NO_THROW(MakeParentChild());
  const std::string error =
      ExpectThrow(engine_.get(), context_.get(), "DROP TABLE fk_parent");
  EXPECT_NE(error.find("FOREIGN KEY"), std::string::npos) << error;
  ExpectOk(engine_.get(), context_.get(), "DROP TABLE fk_child");
  ExpectOk(engine_.get(), context_.get(), "DROP TABLE fk_parent");
}

TEST_F(ForeignKeyTest, MissingParentTableRejected) {
  const std::string error = ExpectThrow(
      engine_.get(), context_.get(),
      "CREATE TABLE fk_orphan (id INT64, pid INT64, FOREIGN KEY (pid) "
      "REFERENCES fk_missing(id))");
  EXPECT_NE(error.find("fk_missing"), std::string::npos) << error;
}

TEST_F(ForeignKeyTest, MissingParentColumnRejected) {
  ExpectOk(engine_.get(), context_.get(), "CREATE TABLE fk_p3 (id INT64)");
  const std::string error =
      ExpectThrow(engine_.get(), context_.get(),
                  "CREATE TABLE fk_c3 (id INT64, pid INT64, FOREIGN KEY (pid) "
                  "REFERENCES fk_p3(nope))");
  EXPECT_NE(error.find("nope"), std::string::npos) << error;
}

TEST_F(ForeignKeyTest, TypeMismatchRejected) {
  ExpectOk(engine_.get(), context_.get(), "CREATE TABLE fk_p4 (id VARCHAR(8))");
  const std::string error =
      ExpectThrow(engine_.get(), context_.get(),
                  "CREATE TABLE fk_c4 (id INT64, pid INT64, FOREIGN KEY (pid) "
                  "REFERENCES fk_p4(id))");
  EXPECT_NE(error.find("type mismatch"), std::string::npos) << error;
}

TEST_F(ForeignKeyTest, UnsupportedActionsRejected) {
  ExpectOk(engine_.get(), context_.get(), "CREATE TABLE fk_p5 (id INT64)");
  EXPECT_NE(ExpectThrow(engine_.get(), context_.get(),
                        "CREATE TABLE fk_c5 (id INT64, pid INT64, FOREIGN KEY "
                        "(pid) REFERENCES fk_p5(id) ON DELETE CASCADE)")
                .find("NO ACTION"),
            std::string::npos);
  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_p6 (x INT64, y INT64)");
  EXPECT_NE(ExpectThrow(engine_.get(), context_.get(),
                        "CREATE TABLE fk_c6 (a INT64, b INT64, FOREIGN KEY "
                        "(a, b) REFERENCES fk_p6(x, y))")
                .find("multi-column"),
            std::string::npos);
}

TEST_F(ForeignKeyTest, SelfReferenceEnforced) {
  ExpectOk(engine_.get(), context_.get(),
           "CREATE TABLE fk_emp (id INT64, mgr INT64, FOREIGN KEY (mgr) "
           "REFERENCES fk_emp(id))");
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_emp VALUES (1, NULL), (2, 1)");
  ExpectViolation(engine_.get(), context_.get(),
                  "INSERT INTO fk_emp VALUES (3, 9)");
  ExpectViolation(engine_.get(), context_.get(),
                  "DELETE FROM fk_emp WHERE id = 1");
}

TEST_F(ForeignKeyTest, ConstraintSurvivesCatalogRoundTrip) {
  ASSERT_NO_THROW(MakeParentChild());
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_child VALUES (10, 1)");
  ASSERT_EQ(context_->PreCommit(), Status::kSuccess);
  engine_.reset();
  context_.reset();
  database_.reset();
  database_ = Database::Create(db_name_).MoveValue();
  context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  engine_ = std::make_unique<SqlEngine>(*database_);
  // The reloaded schema still carries the edge: violations keep failing.
  ExpectViolation(engine_.get(), context_.get(),
                  "INSERT INTO fk_child VALUES (11, 9)");
  ExpectOk(engine_.get(), context_.get(),
           "INSERT INTO fk_child VALUES (11, 2)");
}

}  // namespace tinylamb
