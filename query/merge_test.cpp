/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <cstdint>
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

std::string CellText(const Row& row, size_t column) {
  return row.values_[column].AsString();
}

}  // namespace

class MergeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("merge-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE tgt (k INT64, v STRING);");
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE src (k INT64, nv STRING);");
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO tgt VALUES (1, 'old1'), (2, 'old2'), (3, 'old3');");
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO src VALUES (1, 'new1'), (2, 'new2'), (4, 'new4');");
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  std::vector<std::string> TargetRows() {
    std::vector<std::string> out;
    for (const Row& row : RunSql(engine_.get(), context_.get(),
                                 "SELECT k, v FROM tgt ORDER BY k;")) {
      out.push_back(CellText(row, 0) + ":" + CellText(row, 1));
    }
    return out;
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(MergeTest, UpdatesMatchedAndInsertsUnmatchedSourceRows) {
  const std::vector<Row> affected =
      RunSql(engine_.get(), context_.get(),
             "MERGE INTO tgt USING src ON tgt.k = src.k "
             "WHEN MATCHED THEN UPDATE SET v = src.nv "
             "WHEN NOT MATCHED THEN INSERT (k, v) VALUES (src.k, src.nv);");
  EXPECT_EQ(affected.size(), 1);
  EXPECT_EQ(CellText(affected[0], 1), "3");
  EXPECT_EQ((TargetRows()),
            (std::vector<std::string>{"1:\"new1\"", "2:\"new2\"", "3:\"old3\"",
                                      "4:\"new4\""}));
}

TEST_F(MergeTest, MatchedClausesEvaluateInDeclarationOrder) {
  RunSql(engine_.get(), context_.get(),
         "MERGE INTO tgt USING src ON tgt.k = src.k "
         "WHEN MATCHED AND src.k = 1 THEN DELETE "
         "WHEN MATCHED AND src.k = 2 THEN UPDATE SET v = src.nv;");
  EXPECT_EQ(TargetRows(),
            (std::vector<std::string>{"2:\"new2\"", "3:\"old3\""}));
}

TEST_F(MergeTest, NotMatchedBySourceDeletesTargetOnlyRows) {
  RunSql(engine_.get(), context_.get(),
         "MERGE INTO tgt USING src ON tgt.k = src.k "
         "WHEN NOT MATCHED BY SOURCE THEN DELETE;");
  EXPECT_EQ((TargetRows()),
            (std::vector<std::string>{"1:\"old1\"", "2:\"old2\""}));
}

TEST_F(MergeTest, NotMatchedBySourceUpdateTouchesTargetColumnsOnly) {
  RunSql(engine_.get(), context_.get(),
         "MERGE INTO tgt USING src ON tgt.k = src.k "
         "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 'unmatched';");
  EXPECT_EQ(TargetRows(), (std::vector<std::string>{"1:\"old1\"", "2:\"old2\"",
                                                    "3:\"unmatched\""}));
}

TEST_F(MergeTest, SubquerySourceResolvesAliasReferences) {
  RunSql(engine_.get(), context_.get(),
         "MERGE INTO tgt USING (SELECT 2 AS k, 'sub2' AS nv UNION ALL "
         "SELECT 5, 'sub5') AS s ON tgt.k = s.k "
         "WHEN MATCHED THEN UPDATE SET v = s.nv "
         "WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.nv);");
  EXPECT_EQ((TargetRows()),
            (std::vector<std::string>{"1:\"old1\"", "2:\"sub2\"", "3:\"old3\"",
                                      "5:\"sub5\""}));
}

TEST_F(MergeTest, InsertColumnListReordersAndLeavesMissingCellsNull) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE wide (a INT64, b INT64, c INT64);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO wide VALUES (1, 2, 3);");
  RunSql(engine_.get(), context_.get(),
         "MERGE INTO wide USING (SELECT 99 AS k) AS s ON wide.a = s.k "
         "WHEN NOT MATCHED THEN INSERT (c, a) VALUES (7, s.k);");
  const std::vector<Row> rows = RunSql(
      engine_.get(), context_.get(), "SELECT a, b, c FROM wide WHERE a = 99;");
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(CellText(rows[0], 0), "99");
  EXPECT_TRUE(rows[0].values_[1].IsNull());
  EXPECT_EQ(CellText(rows[0], 2), "7");
}

TEST_F(MergeTest, MultipleMatchingSourceRowsAreRejected) {
  RunSql(engine_.get(), context_.get(), "INSERT INTO src VALUES (2, 'dup2');");
  try {
    RunSql(engine_.get(), context_.get(),
           "MERGE INTO tgt USING src ON tgt.k = src.k "
           "WHEN MATCHED THEN UPDATE SET v = src.nv;");
    FAIL() << "expected a duplicate-match rejection";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("more than once"),
              std::string::npos)
        << error.what();
  }
  // The statement failed: no target row may reflect the rejected merge.
  EXPECT_EQ(TargetRows(), (std::vector<std::string>{"1:\"old1\"", "2:\"old2\"",
                                                    "3:\"old3\""}));
}

TEST_F(MergeTest, AmbiguousBareColumnNameIsRejected) {
  try {
    RunSql(engine_.get(), context_.get(),
           "MERGE INTO tgt USING src ON k = k "
           "WHEN MATCHED THEN UPDATE SET v = src.nv;");
    FAIL() << "expected an ambiguity rejection";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("ambiguous"), std::string::npos)
        << error.what();
  }
}

TEST_F(MergeTest, NullJoinKeysNeverMatch) {
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO tgt VALUES (NULL, 'nulltgt');");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO src VALUES (NULL, 'nullsrc');");
  RunSql(engine_.get(), context_.get(),
         "MERGE INTO tgt USING src ON tgt.k = src.k "
         "WHEN MATCHED THEN UPDATE SET v = src.nv "
         "WHEN NOT MATCHED THEN INSERT (k, v) VALUES (src.k, src.nv);");
  EXPECT_EQ((TargetRows()),
            (std::vector<std::string>{"NULL:\"nulltgt\"", "NULL:\"nullsrc\"",
                                      "1:\"new1\"", "2:\"new2\"", "3:\"old3\"",
                                      "4:\"new4\""}));
}

TEST_F(MergeTest, ForeignKeyEnforcedOnInsertAndUpdate) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE parent (pk INT64, PRIMARY KEY (pk));");
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE child (fk INT64, FOREIGN KEY (fk) REFERENCES "
         "parent (pk));");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO parent VALUES (10), (20);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO child VALUES (10);");
  // Inserting a child key absent from the parent must fail.
  try {
    RunSql(engine_.get(), context_.get(),
           "MERGE INTO child USING (SELECT 99 AS k) AS s ON child.fk = s.k "
           "WHEN NOT MATCHED THEN INSERT (fk) VALUES (s.k);");
    FAIL() << "expected a foreign key rejection";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("foreign key"), std::string::npos)
        << error.what();
  }
  // Deleting the still-referenced parent key must fail.
  try {
    RunSql(engine_.get(), context_.get(),
           "MERGE INTO parent USING (SELECT 10 AS k) AS s ON parent.pk = s.k "
           "WHEN MATCHED THEN DELETE;");
    FAIL() << "expected a foreign key rejection";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("foreign key"), std::string::npos)
        << error.what();
  }
}

TEST_F(MergeTest, TruncateTableDropsRowsAndKeepsSchema) {
  const std::vector<Row> affected =
      RunSql(engine_.get(), context_.get(), "TRUNCATE TABLE tgt;");
  EXPECT_EQ(CellText(affected[0], 1), "3");
  EXPECT_TRUE(
      RunSql(engine_.get(), context_.get(), "SELECT k FROM tgt;").empty());
  // The table definition survives: inserts and merges keep working.
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO tgt VALUES (7, 'post-truncate');");
  RunSql(engine_.get(), context_.get(),
         "MERGE INTO tgt USING src ON tgt.k = src.k "
         "WHEN NOT MATCHED THEN INSERT (k, v) VALUES (src.k, src.nv);");
  EXPECT_EQ((TargetRows()),
            (std::vector<std::string>{"1:\"new1\"", "2:\"new2\"", "4:\"new4\"",
                                      "7:\"post-truncate\""}));
}

TEST_F(MergeTest, UpdateFromSubqueryDesugarsToMerge) {
  // BigQuery UPDATE...FROM: target rows joined with the source; SET values
  // may reference source columns through the source alias.
  RunSql(engine_.get(), context_.get(),
         "UPDATE tgt SET v = src.nv FROM (SELECT k, nv FROM src) AS src "
         "WHERE tgt.k = src.k;");
  EXPECT_EQ((TargetRows()), (std::vector<std::string>{
                                "1:\"new1\"", "2:\"new2\"", "3:\"old3\""}));
}

TEST_F(MergeTest, UpdateFromTableSourceAndWhereResidual) {
  RunSql(engine_.get(), context_.get(),
         "UPDATE tgt SET v = 'x' FROM src WHERE tgt.k = src.k AND src.k = 2;");
  EXPECT_EQ((TargetRows()),
            (std::vector<std::string>{"1:\"old1\"", "2:\"x\"", "3:\"old3\""}));
}

TEST_F(MergeTest, UpdateFromWithoutWhereMatchesEveryJoinedPair) {
  // No WHERE: every (target, source) pair with ON=TRUE matches, so each
  // target row is updated (multi-match = one unconditional action per pair
  // is fine; only multiple ACTIONS on one target row are rejected).
  RunSql(engine_.get(), context_.get(),
         "UPDATE tgt SET v = 'all' FROM (SELECT 1 AS k) AS one "
         "WHERE tgt.k = one.k;");
  EXPECT_EQ((TargetRows()), (std::vector<std::string>{"1:\"all\"", "2:\"old2\"",
                                                      "3:\"old3\""}));
}

TEST_F(MergeTest, UpdateFromMultiMatchRejectsAmbiguousSource) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE dupsrc (k INT64, nv STRING);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO dupsrc VALUES (1, 'a'), (1, 'b');");
  try {
    RunSql(engine_.get(), context_.get(),
           "UPDATE tgt SET v = dupsrc.nv FROM dupsrc "
           "WHERE tgt.k = dupsrc.k;");
    FAIL() << "expected a multi-match rejection";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("once"), std::string::npos)
        << error.what();
  }
}

TEST_F(MergeTest, UpdateFromReportsUpdateCommand) {
  const std::vector<Row> affected =
      RunSql(engine_.get(), context_.get(),
             "UPDATE tgt SET v = src.nv FROM src WHERE tgt.k = src.k;");
  // The statement ran on the MERGE machinery, but the user statement is an
  // UPDATE: the diagnostic and server tags report UPDATE with the count.
  EXPECT_EQ(CellText(affected[0], 0), "\"Update Rows\"");
  EXPECT_EQ(CellText(affected[0], 1), "2");
  EXPECT_EQ(engine_->LastStatementType(), StatementType::kUpdate);
}

}  // namespace tinylamb
