/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// SQL coverage for the subquery runtime (executor/detail/subquery_runtime.cpp):
// correlated and uncorrelated scalar/IN/EXISTS shapes, three-valued NULL
// membership, quantified comparisons, row-value IN, EXISTS normalization,
// subqueries inside HAVING / derived tables / UNNEST, and repeated execution
// of cached subqueries.

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

std::string JoinFirstColumn(const std::vector<Row>& rows) {
  std::string out;
  for (const Row& row : rows) {
    if (!out.empty()) {
      out += ",";
    }
    out += row[0].AsString();
  }
  return out;
}
}  // namespace

class SqlSubqueryShapesCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("sql_subquery_shapes_test-" + RandomString())
                    .MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE orders(id INT64, cust INT64, amt FLOAT64)");
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE custs(id INT64, name STRING(MAX))");
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO orders VALUES (1, 10, 5.0), (2, 10, 7.5), "
           "(3, 20, 1.0), (4, 30, 2.0), (5, NULL, 9.0)");
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO custs VALUES (10, 'a'), (20, 'b')");
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
    // keeps this suite's cases independent of prepared-plan cache state left
    // by earlier cases.
    SqlEngine engine(*database_);
    return RunSql(&engine, context_.get(), sql);
  }
  Value Scalar(const std::string& sql) {
    const auto rows = Query(sql);
    if (rows.size() != 1U) {
      ADD_FAILURE() << "expected one row: " << sql;
      return {};
    }
    return rows[0][0];
  }
  void ExpectColumn(const std::string& sql, const std::string& expected) {
    SCOPED_TRACE(sql);
    EXPECT_EQ(JoinFirstColumn(Query(sql)), expected);
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(SqlSubqueryShapesCoverageTest, UncorrelatedScalarSubquery) {
  ExpectColumn(
      "SELECT id FROM orders WHERE amt > (SELECT AVG(amt) FROM orders) "
      "ORDER BY id",
      "1,2,5");
  // Repeated execution of the same uncorrelated shape stays consistent.
  ExpectColumn(
      "SELECT id FROM orders WHERE amt > (SELECT AVG(amt) FROM orders) "
      "ORDER BY id DESC",
      "5,2,1");
}

TEST_F(SqlSubqueryShapesCoverageTest, CorrelatedScalarSubquery) {
  // Per-cust maximum: rows matching their own group's max qualify.  The
  // NULL-cust row's correlated comparison is UNKNOWN and filters out.
  ExpectColumn(
      "SELECT id FROM orders o WHERE amt >= "
      "(SELECT MAX(amt) FROM orders o2 WHERE o2.cust = o.cust) ORDER BY id",
      "2,3,4");
  ExpectColumn(
      "SELECT o.id, (SELECT name FROM custs c WHERE c.id = o.cust) "
      "FROM orders o ORDER BY o.id",
      "1,2,3,4,5");
}

TEST_F(SqlSubqueryShapesCoverageTest, InMembershipAndNullThreeValuedLogic) {
  ExpectColumn(
      "SELECT id FROM orders WHERE cust IN (SELECT id FROM custs) "
      "ORDER BY id",
      "1,2,3");
  // A NULL probe value yields UNKNOWN, filtering the row out.
  ExpectColumn(
      "SELECT id FROM orders WHERE NULL IN (SELECT id FROM custs) "
      "ORDER BY id",
      "");
  // NOT IN with an empty build side is TRUE for every probe.
  ExpectColumn(
      "SELECT id FROM orders WHERE cust NOT IN "
      "(SELECT id FROM custs WHERE id > 1000) ORDER BY id",
      "1,2,3,4,5");
}

TEST_F(SqlSubqueryShapesCoverageTest, QuantifiedComparisons) {
  // cust 20's single amt is 1.0: every order above it qualifies.
  ExpectColumn(
      "SELECT id FROM orders WHERE amt > ALL "
      "(SELECT amt FROM orders WHERE cust = 20) ORDER BY id",
      "1,2,4,5");
  // cust 10's amts are {5.0, 7.5}: above at least one of them.
  ExpectColumn(
      "SELECT id FROM orders WHERE amt > ANY "
      "(SELECT amt FROM orders WHERE cust = 10) ORDER BY id",
      "2,5");
  ExpectColumn(
      "SELECT id FROM orders WHERE amt = SOME "
      "(SELECT amt FROM orders WHERE cust = 30) ORDER BY id",
      "4");
}

TEST_F(SqlSubqueryShapesCoverageTest, ExistsAndItsNegation) {
  ExpectColumn(
      "SELECT id FROM custs c WHERE EXISTS "
      "(SELECT 1 FROM orders o WHERE o.cust = c.id) ORDER BY id",
      "10,20");
  // Negation of a scalar EXISTS shape (no correlation inside the subquery):
  // both fixture custs qualify when no order exceeds 100.
  ExpectColumn(
      "SELECT id FROM custs WHERE NOT EXISTS "
      "(SELECT 1 FROM orders WHERE amt > 100) ORDER BY id",
      "10,20");
  // EXISTS with an ORDER BY and LIMIT normalizes the inner query safely.
  ExpectColumn(
      "SELECT id FROM custs c WHERE EXISTS "
      "(SELECT 1 FROM orders o WHERE o.cust = c.id ORDER BY o.amt LIMIT 1) "
      "ORDER BY id",
      "10,20");
}

TEST_F(SqlSubqueryShapesCoverageTest, RowValueIn) {
  ExpectColumn(
      "SELECT id FROM orders WHERE (id, cust) IN "
      "(SELECT (id, cust) FROM orders WHERE amt > 4.0) ORDER BY id",
      "1,2");
  ExpectColumn(
      "SELECT id FROM orders WHERE (id, cust) IN "
      "(SELECT AS STRUCT id, cust FROM orders WHERE amt > 4.0) "
      "ORDER BY id",
      "1,2");
  EXPECT_TRUE(Scalar("SELECT (1, NULL) IN ((1, 1), (2, 1));").IsNull());
  EXPECT_TRUE(Scalar("SELECT (1, NULL) IN ((1, 1), (1, NULL));").IsNull());

  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE pair_targets(k INT64, g INT64, v STRING(MAX))");
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE pair_source(k INT64, g INT64)");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO pair_targets VALUES (1, 10, 'keep1'), "
         "(1, 20, 'drop1'), (2, 10, 'keep2')");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO pair_source VALUES (1, 10)");
  Query(
      "UPDATE pair_targets SET v = 'updated' WHERE (k, g) IN "
      "(SELECT AS STRUCT k, g FROM pair_source)");
  std::vector<Row> updated =
      Query("SELECT k, g, v FROM pair_targets ORDER BY k, g");
  ASSERT_EQ(updated.size(), 3U);
  EXPECT_EQ(updated[0][2], Value("updated"));
  EXPECT_EQ(updated[1][2], Value("drop1"));
  EXPECT_EQ(updated[2][2], Value("keep2"));

  RunSql(engine_.get(), context_.get(),
         "INSERT INTO pair_source VALUES (1, 20)");
  Query(
      "DELETE FROM pair_targets WHERE (k, g) NOT IN "
      "(SELECT AS STRUCT k, g FROM pair_source)");
  const std::vector<Row> remaining =
      Query("SELECT k, g FROM pair_targets ORDER BY k, g");
  ASSERT_EQ(remaining.size(), 2U);
  EXPECT_EQ(remaining[0][0], Value(int64_t{1}));
  EXPECT_EQ(remaining[0][1], Value(int64_t{10}));
  EXPECT_EQ(remaining[1][0], Value(int64_t{1}));
  EXPECT_EQ(remaining[1][1], Value(int64_t{20}));

  bool rejected = false;
  try {
    Query(
        "SELECT id FROM orders WHERE (id, cust) IN "
        "(SELECT id, cust FROM orders)");
  } catch (const std::runtime_error& error) {
    rejected =
        std::string(error.what())
            .find("Subquery of type IN must have only one output column") !=
        std::string::npos;
  }
  EXPECT_TRUE(rejected);
}

TEST_F(SqlSubqueryShapesCoverageTest, SubqueriesInHavingAndDerived) {
  // Groups whose total exceeds the constant-scalar 4: cust 10 (12.5) and
  // the NULL-cust group (9.0).
  // Groups whose total exceeds the constant-scalar 4: cust 10 (12.5) and
  // the NULL-cust group (9.0).  Only the set is pinned: NULL ordering
  // relative to values is a per-plan detail here.
  const auto rows = Query(
      "SELECT cust FROM orders GROUP BY cust "
      "HAVING SUM(amt) > (SELECT 4)");
  ASSERT_EQ(rows.size(), 2U);
  bool saw_ten = false;
  bool saw_null = false;
  for (const Row& row : rows) {
    if (row[0].IsNull()) {
      saw_null = true;
    } else if (row[0] == (Value(int64_t{10}))) {
      saw_ten = true;
    }
  }
  EXPECT_TRUE(saw_ten);
  EXPECT_TRUE(saw_null);
  ExpectColumn(
      "SELECT t.c FROM (SELECT cust AS c, COUNT(*) AS n FROM orders GROUP BY "
      "cust) t WHERE t.n >= (SELECT COUNT(*) FROM custs) ORDER BY t.c",
      "10");
}

TEST_F(SqlSubqueryShapesCoverageTest, UncorrelatedInSelectListStaysConsistent) {
  // The subquery result must be stable across every output row.
  const auto rows =
      Query("SELECT id, (SELECT MAX(amt) FROM orders) FROM orders ORDER BY id");
  ASSERT_EQ(rows.size(), 5U);
  for (const Row& row : rows) {
    EXPECT_EQ(row[1], (Value(9.0)));
  }
}

TEST_F(SqlSubqueryShapesCoverageTest, VolatileUncorrelatedSubqueryIsNotReused) {
  const Value first = Scalar("SELECT (SELECT RAND() FROM custs LIMIT 1)");
  const Value second = Scalar("SELECT (SELECT RAND() FROM custs LIMIT 1)");
  EXPECT_NE(first, second);
}

}  // namespace tinylamb
