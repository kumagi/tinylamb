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
  return row.values_.at(column).AsString();
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

TEST_F(MergeTest, NotMatchedInsertAppliesDeclaredDefaultsToOmittedColumns) {
  // Omitted columns take the declared DEFAULT, matching the INSERT
  // statement path; a column the clause assigns NULL keeps NULL.
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE dflt (a INT64, b INT64 DEFAULT 42, s STRING);");
  RunSql(engine_.get(), context_.get(),
         "MERGE INTO dflt USING (SELECT 99 AS k, CAST(NULL AS STRING) AS nv) "
         "AS s ON dflt.a = s.k "
         "WHEN NOT MATCHED THEN INSERT (a, s) VALUES (s.k, s.nv);");
  const std::vector<Row> rows = RunSql(
      engine_.get(), context_.get(), "SELECT a, b, s FROM dflt WHERE a = 99;");
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(CellText(rows[0], 1), "42");
  EXPECT_TRUE(rows[0].values_[2].IsNull());
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

TEST_F(MergeTest, ForeignKeyEnforcedOnParentKeyUpdate) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE parent (pk INT64, PRIMARY KEY (pk));");
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE child (fk INT64, FOREIGN KEY (fk) REFERENCES "
         "parent (pk));");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO parent VALUES (10), (20);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO child VALUES (10);");
  // Rekeying a parent row that a child still references must fail on the
  // MERGE path, exactly like a plain UPDATE does in executor/update.cpp.
  try {
    RunSql(engine_.get(), context_.get(),
           "MERGE INTO parent USING (SELECT 10 AS k) AS s ON parent.pk = s.k "
           "WHEN MATCHED THEN UPDATE SET pk = 30;");
    FAIL() << "expected a foreign key rejection";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("referenced by"),
              std::string::npos)
        << error.what();
  }
  // The desugared UPDATE...FROM goes through the same machinery.
  try {
    RunSql(engine_.get(), context_.get(),
           "UPDATE parent SET pk = 30 FROM (SELECT 10 AS k) AS s "
           "WHERE parent.pk = s.k;");
    FAIL() << "expected a foreign key rejection";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("referenced by"),
              std::string::npos)
        << error.what();
  }
  // Both rejections left the parent untouched.
  EXPECT_EQ(RunSql(engine_.get(), context_.get(),
                   "SELECT pk FROM parent ORDER BY pk;")
                .size(),
            2);
  // Rekeying a parent row nobody references is still allowed.
  RunSql(engine_.get(), context_.get(),
         "MERGE INTO parent USING (SELECT 20 AS k) AS s ON parent.pk = s.k "
         "WHEN MATCHED THEN UPDATE SET pk = 40;");
  const std::vector<Row> rows = RunSql(engine_.get(), context_.get(),
                                       "SELECT pk FROM parent ORDER BY pk;");
  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(CellText(rows[0], 0), "10");
  EXPECT_EQ(CellText(rows[1], 0), "40");
}

TEST_F(MergeTest, SelfReferencingForeignKeyAcceptsInStatementForwardKey) {
  // A self-referencing edge (mgr REFERENCES emp(id) on emp itself) reads the
  // multiset the statement is extending. executor/insert.cpp publishes every
  // row it inserts (note_inserted_row), so INSERT INTO emp VALUES (2,1),
  // (3,2) accepts the forward reference; MERGE must not be stricter.
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE emp (id INT64, mgr INT64, FOREIGN KEY (mgr) REFERENCES "
         "emp (id));");
  RunSql(engine_.get(), context_.get(), "INSERT INTO emp VALUES (1, NULL);");
  RunSql(engine_.get(), context_.get(),
         "MERGE INTO emp USING (SELECT 2 AS id, 1 AS mgr UNION ALL "
         "SELECT 3 AS id, 2 AS mgr) AS s ON emp.id = s.id "
         "WHEN NOT MATCHED THEN INSERT (id, mgr) VALUES (s.id, s.mgr);");
  const std::vector<Row> rows =
      RunSql(engine_.get(), context_.get(), "SELECT id, mgr FROM emp;");
  ASSERT_EQ(rows.size(), 3);

  // Plain INSERT keeps the same answer (the asymmetry this pins).
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE emp2 (id INT64, mgr INT64, FOREIGN KEY (mgr) REFERENCES "
         "emp2 (id));");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO emp2 VALUES (1, NULL), (2, 1), (3, 2);");
  EXPECT_EQ(
      RunSql(engine_.get(), context_.get(), "SELECT id FROM emp2;").size(), 3);
  // A key no row publishes is still rejected inside one statement.
  try {
    RunSql(engine_.get(), context_.get(),
           "MERGE INTO emp2 USING (SELECT 4 AS id, 77 AS mgr) AS s "
           "ON emp2.id = s.id "
           "WHEN NOT MATCHED THEN INSERT (id, mgr) VALUES (s.id, s.mgr);");
    FAIL() << "expected a foreign key rejection";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("foreign key"), std::string::npos)
        << error.what();
  }
  EXPECT_EQ(
      RunSql(engine_.get(), context_.get(), "SELECT id FROM emp2;").size(), 3);
}

TEST_F(MergeTest, SelfReferencingForeignKeyFollowsUpdateRekeys) {
  // The parent multiset must be re-seeded after the UPDATE phase: an UPDATE
  // that rekeys this very table removes the old key and publishes the new
  // one, so a later INSERT of the same statement may reference the new key
  // and must not be allowed to reference the removed one.
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE e2 (id INT64, mgr INT64, FOREIGN KEY (mgr) REFERENCES "
         "e2 (id));");
  RunSql(engine_.get(), context_.get(), "INSERT INTO e2 VALUES (1, NULL);");
  RunSql(engine_.get(), context_.get(),
         "MERGE INTO e2 USING (SELECT 1 AS id, 0 AS mgr UNION ALL "
         "SELECT 9 AS id, 5 AS mgr) AS s ON e2.id = s.id "
         "WHEN MATCHED THEN UPDATE SET id = 5 "
         "WHEN NOT MATCHED THEN INSERT (id, mgr) VALUES (s.id, s.mgr);");
  const std::vector<Row> rows =
      RunSql(engine_.get(), context_.get(), "SELECT id, mgr FROM e2;");
  ASSERT_EQ(rows.size(), 2);
  // The UPDATE phase rekeyed row 1 to 5 and the INSERT phase followed it.
  bool saw_five = false;
  bool saw_nine_on_five = false;
  for (const Row& row : rows) {
    saw_five = saw_five || CellText(row, 0) == "5";
    saw_nine_on_five = saw_nine_on_five ||
                       (CellText(row, 0) == "9" && CellText(row, 1) == "5");
  }
  EXPECT_TRUE(saw_five) << "rekeyed parent row missing";
  EXPECT_TRUE(saw_nine_on_five) << "insert must see the rekeyed parent";

  // Same shape, but the INSERT references the key the UPDATE removed.
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE e3 (id INT64, mgr INT64, FOREIGN KEY (mgr) REFERENCES "
         "e3 (id));");
  RunSql(engine_.get(), context_.get(), "INSERT INTO e3 VALUES (1, NULL);");
  try {
    RunSql(engine_.get(), context_.get(),
           "MERGE INTO e3 USING (SELECT 1 AS id, 0 AS mgr UNION ALL "
           "SELECT 9 AS id, 1 AS mgr) AS s ON e3.id = s.id "
           "WHEN MATCHED THEN UPDATE SET id = 5 "
           "WHEN NOT MATCHED THEN INSERT (id, mgr) VALUES (s.id, s.mgr);");
    FAIL() << "expected a foreign key rejection";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("foreign key"), std::string::npos)
        << error.what();
  }
  // The rejected INSERT never landed, whatever the statement-time rollback
  // does with the UPDATE phase that already ran.
  const std::vector<Row> survivors =
      RunSql(engine_.get(), context_.get(), "SELECT id, mgr FROM e3;");
  bool saw_nine = false;
  for (const Row& survivor : survivors) {
    saw_nine = saw_nine || CellText(survivor, 0) == "9";
  }
  EXPECT_FALSE(saw_nine) << "rejected INSERT row must not be visible";
}

TEST_F(MergeTest, SelfReferencingDeleteIgnoresRowsThisStatementRemoves) {
  // NO ACTION is statement-end semantics: the rows the statement itself
  // removes no longer keep a parent key alive. Executor::Delete passes its
  // pending positions to FindReferencedParentKey; this MERGE arm passed none,
  // so deleting a chain in one statement was rejected as "referenced by"
  // itself while the identical plain DELETE succeeded.
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE emp (k INT64, mgr INT64, FOREIGN KEY (mgr) REFERENCES "
         "emp (k))");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO emp VALUES (1, NULL), (2, 1)");
  // Baseline: the plain DELETE of both rows succeeds.
  RunSql(engine_.get(), context_.get(), "DELETE FROM emp WHERE k IN (1, 2)");
  EXPECT_EQ(
      RunSql(engine_.get(), context_.get(), "SELECT COUNT(*) FROM emp;")[0][0],
      Value(0));
  // Same shape through MERGE must not be stricter.
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO emp VALUES (1, NULL), (2, 1)");
  RunSql(engine_.get(), context_.get(),
         "MERGE INTO emp USING (SELECT 1 AS k UNION ALL SELECT 2 AS k) AS s "
         "ON emp.k = s.k WHEN MATCHED THEN DELETE;");
  EXPECT_EQ(
      RunSql(engine_.get(), context_.get(), "SELECT COUNT(*) FROM emp;")[0][0],
      Value(0));
  // Control: deleting only the referenced parent is still rejected, and the
  // surviving child row keeps it that way.
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO emp VALUES (1, NULL), (2, 1)");
  try {
    RunSql(engine_.get(), context_.get(),
           "MERGE INTO emp USING (SELECT 1 AS k) AS s ON emp.k = s.k "
           "WHEN MATCHED THEN DELETE;");
    FAIL() << "expected a foreign key rejection";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("referenced by"),
              std::string::npos)
        << error.what();
  }
  EXPECT_EQ(
      RunSql(engine_.get(), context_.get(), "SELECT COUNT(*) FROM emp;")[0][0],
      Value(2));
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

TEST_F(MergeTest, UpdateFromWhereMatchesOnlyJoinedTarget) {
  // The WHERE equality matches only target key 1 to the single source row.
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
