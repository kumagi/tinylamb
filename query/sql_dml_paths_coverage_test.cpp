/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// SQL coverage for DML execution paths (executor/insert.cpp and the
// UPDATE/DELETE paths): multi-row inserts, PK re-insert semantics, expression
// assignments, filtered updates/deletes, DML followed by reads in one
// transaction, and catalog errors.

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
  StatusOr<QueryResult> result = engine->Execute(*ctx, sql);
  if (!result.HasValue()) {
    if (engine->LastError().empty()) {
      result = engine->Execute(*ctx, sql);
    }
    if (!result.HasValue()) {
      throw std::runtime_error(engine->LastError());
    }
  }
  std::vector<Row> rows;
  QueryResult drained = result.MoveValue();
  Row row;
  while (drained.Next(&row)) {
    rows.push_back(row);
    row = Row();
  }
  if (drained.GetStatus() != Status::kSuccess) {
    throw std::runtime_error(drained.GetStatus().GetMessage());
  }
  return rows;
}
}  // namespace

class SqlDmlPathsCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ =
        Database::Create("sql_dml_paths_test-" + RandomString()).MoveValue();
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

  std::vector<Row> Query(const std::string& sql) {
    // One engine wrapper per statement mirrors production call patterns and
    // keeps cases independent of prepared-plan cache state left by earlier
    // statements in this fixture.
    SqlEngine engine(*database_);
    return RunSql(&engine, context_.get(), sql);
  }
  void ExpectRows(const std::string& sql, const std::vector<Row>& expected) {
    SCOPED_TRACE(sql);
    const auto rows = Query(sql);
    ASSERT_EQ(rows.size(), expected.size());
    for (size_t i = 0; i < rows.size(); ++i) {
      EXPECT_EQ(rows[i], expected[i]);
    }
  }
  void ExpectError(const std::string& sql) {
    SCOPED_TRACE(sql);
    EXPECT_THROW({ Query(sql); }, std::exception);
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(SqlDmlPathsCoverageTest, InsertVariantsAndImmediateReads) {
  Query("CREATE TABLE t (k INT64, v STRING(MAX))");
  Query("INSERT INTO t VALUES (1, 'one')");
  Query("INSERT INTO t VALUES (2, 'two'), (3, 'three')");
  // Explicit column list, in schema order.
  Query("INSERT INTO t (k, v) VALUES (4, 'four')");
  // Reads see the same transaction's writes.
  ExpectRows("SELECT k FROM t ORDER BY k", {Row({Value(1)}), Row({Value(2)}),
                                            Row({Value(3)}), Row({Value(4)})});
  ExpectError("INSERT INTO missing_table VALUES (1)");
}

TEST_F(SqlDmlPathsCoverageTest, PrimaryKeyReinsertSemantics) {
  // A table-level single-column PRIMARY KEY (k) is enforced exactly like the
  // column-level `k INT64 PRIMARY KEY` spelling: re-inserting the key fails.
  Query("CREATE TABLE t (k INT64 NOT NULL, v STRING(MAX), PRIMARY KEY (k))");
  Query("INSERT INTO t VALUES (1, 'first')");
  ExpectError("INSERT INTO t VALUES (1, 'second')");
  ExpectRows("SELECT k, v FROM t ORDER BY v",
             {Row({Value(1), Value(std::string("first"))})});
}

TEST_F(SqlDmlPathsCoverageTest, UpdateWithPredicatesAndExpressions) {
  Query("CREATE TABLE t (k INT64, v STRING(MAX))");
  Query("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
  Query("UPDATE t SET v = 'z' WHERE k >= 2");
  ExpectRows("SELECT k, v FROM t ORDER BY k",
             {Row({Value(1), Value(std::string("a"))}),
              Row({Value(2), Value(std::string("z"))}),
              Row({Value(3), Value(std::string("z"))})});
  // SET with an expression referencing the row's own columns.
  Query("UPDATE t SET k = k * 10 WHERE k = 1");
  ExpectRows("SELECT k FROM t ORDER BY k",
             {Row({Value(2)}), Row({Value(3)}), Row({Value(10)})});
  // A predicate matching nothing changes nothing.
  Query("UPDATE t SET v = 'nope' WHERE k > 1000");
  ExpectRows("SELECT COUNT(*) FROM t WHERE v = 'nope'", {Row({Value(0)})});
}

TEST_F(SqlDmlPathsCoverageTest, DeletePredicatesAndRemainingRows) {
  Query("CREATE TABLE t (k INT64, v STRING(MAX))");
  Query("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");
  Query("DELETE FROM t WHERE k <= 2");
  ExpectRows("SELECT k FROM t ORDER BY k", {Row({Value(3)}), Row({Value(4)})});
  // Deleting a predicate that matches nothing leaves the table intact.
  Query("DELETE FROM t WHERE k > 1000");
  ExpectRows("SELECT COUNT(*) FROM t", {Row({Value(2)})});
  Query("DELETE FROM t");
  ExpectRows("SELECT COUNT(*) FROM t", {Row({Value(0)})});
}

TEST_F(SqlDmlPathsCoverageTest, DmlOnSecondTableKeepsScansIndependent) {
  Query("CREATE TABLE a (k INT64)");
  Query("CREATE TABLE b (k INT64)");
  Query("INSERT INTO a VALUES (1), (2)");
  Query("INSERT INTO b VALUES (3)");
  Query("UPDATE b SET k = (SELECT MAX(k) FROM a)");
  ExpectRows("SELECT k FROM b", {Row({Value(2)})});
  Query("DELETE FROM a WHERE k = (SELECT MIN(k) FROM a)");
  ExpectRows("SELECT k FROM a ORDER BY k", {Row({Value(2)})});
}

// A duplicate SET target names a real column: the per-column loop adopts the
// first entry and leaves the second unconsumed, so the residual check used to
// answer "target not found: a" for a column that exists (and on a single-column
// table it fell into the proto path and blamed the TEXT payload). Both are
// second-order lies about a duplicate assignment.
TEST_F(SqlDmlPathsCoverageTest, DuplicateSetTargetReportsDuplicateNotMissing) {
  Query("CREATE TABLE m (a INT64, b INT64)");
  Query("INSERT INTO m VALUES (1, 2)");
  const auto message_of = [this](const std::string& sql) {
    try {
      Query(sql);
      return std::string("<no error>");
    } catch (const std::runtime_error& error) {
      return std::string(error.what());
    }
  };
  const std::string twice = message_of("UPDATE m SET a = 5, a = 6");
  EXPECT_NE(twice.find("appears more than once"), std::string::npos) << twice;
  EXPECT_EQ(twice.find("not found"), std::string::npos) << twice;
  // A case-variant spelling of the same column is the same duplicate.
  const std::string twice_case = message_of("UPDATE m SET a = 5, A = 6");
  EXPECT_NE(twice_case.find("appears more than once"), std::string::npos)
      << twice_case;
  // Single-column (proto) table: the duplicate must not be re-labelled as a
  // malformed TEXT payload.
  Query("CREATE TABLE s1 (k INT64)");
  Query("INSERT INTO s1 VALUES (1)");
  const std::string twice_single = message_of("UPDATE s1 SET k = 5, k = 6");
  EXPECT_NE(twice_single.find("appears more than once"), std::string::npos)
      << twice_single;
  EXPECT_EQ(twice_single.find("malformed proto"), std::string::npos)
      << twice_single;
  // A genuinely unknown target is still a missing-column diagnostic.
  const std::string missing = message_of("UPDATE m SET zz = 5");
  EXPECT_NE(missing.find("target not found"), std::string::npos) << missing;
  // Nothing was modified by the rejected statements.
  ExpectRows("SELECT a, b FROM m", {Row({Value(1), Value(2)})});
}

// A STRUCT-shaped text INSERT into a multi-column table expands member-wise,
// and its string members must decode the full JSON escape set. The local
// decoder handled only \n \t \r and dropped the backslash of anything else, so
// "caf\u00e9" was silently stored as the literal text "cafu00e9".
TEST_F(SqlDmlPathsCoverageTest, StructTextInsertDecodesJsonEscapes) {
  Query("CREATE TABLE j2 (a STRING(MAX), b INT64)");
  // The SQL literal's doubled backslash reaches the decoder as one backslash.
  Query(R"(INSERT INTO j2 VALUES ('{"caf\\u00e9", 2}'))");
  ExpectRows("SELECT a, b FROM j2",
             {Row({Value(std::string("caf\xC3\xA9")), Value(2)})});
  // The short escapes, including the \b / \f forms that were missing.
  Query("CREATE TABLE j3 (s STRING(MAX))");
  Query(R"(INSERT INTO j3 VALUES ('{"a\\tb\\fc"}'))");
  ExpectRows("SELECT s FROM j3", {Row({Value(std::string("a\tb\fc"))})});
  // A surrogate pair decodes to its astral code point (U+1F600).
  Query(R"(INSERT INTO j3 VALUES ('{"\\ud83d\\ude00"}'))");
  ExpectRows("SELECT s FROM j3",
             {Row({Value(std::string("\xF0\x9F\x98\x80"))})});
}

}  // namespace tinylamb
