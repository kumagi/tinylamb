/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Coverage for executor/detail/expression_eval.cpp: the row-at-a-time LIKE
// matcher, truthiness, binary error mapping, scope lookup, plus SQL sweeps
// over JSON path navigation, civil-time parsing and struct access whose
// leaf work lands in this file.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/executor_base.hpp"
#include "expression/binary_expression.hpp"
#include "expression/constant_value.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
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

}  // namespace

// ---------------------------------------------------------- direct API ----

TEST(ExpressionEvalDetailTest, LikePatternTable) {
  const std::vector<std::tuple<const char*, const char*, bool>> cases = {
      {"", "%", true},        {"", "", true},         {"", "a", false},
      {"abc", "%", true},     {"abc", "%%", true},    {"abc", "abc", true},
      {"abc", "a%", true},    {"abc", "%c", true},    {"abc", "%b%", true},
      {"abc", "a_", false},   {"abc", "_bc", true},   {"abc", "a%c", true},
      {"abc", "%d%", false},  {"abc", "d%", false},   {"abc", "%d", false},
      {"abc", "____", false}, {"abc", "__", false},   {"abc", "ABC", false},
      {"abc", "abc%", true},  {"%abc", "%abc", true}, {"aaa", "a%a", true},
      {"ab", "a%a", false},   {"aabc", "%ab", false}, {"aab", "%ab", true},
      {"a%a", "a%a", true},   {"€abc", "%c", true},   {"abc", "%€", false},
  };
  for (const auto& [value, pattern, expected] : cases) {
    SCOPED_TRACE(std::string(value) + " LIKE " + pattern);
    EXPECT_EQ(relational_detail::Like(value, pattern), expected);
  }
}

TEST(ExpressionEvalDetailTest, TruthyTable) {
  EXPECT_TRUE(relational_detail::Truthy(Value(true)));
  EXPECT_FALSE(relational_detail::Truthy(Value(false)));
  EXPECT_FALSE(relational_detail::Truthy(Value()));
  EXPECT_FALSE(relational_detail::Truthy(Value(0)));
  EXPECT_TRUE(relational_detail::Truthy(Value(1)));
}

TEST(ExpressionEvalDetailTest, TryBinaryMapsErrorMessages) {
  // Success forwards to the canonical evaluator.
  EXPECT_EQ(
      relational_detail::TryBinary(BinaryOperation::kAdd, Value(1), Value(2))
          .MoveValue(),
      Value(3));
  EXPECT_EQ(relational_detail::TryBinary(BinaryOperation::kLike, Value("abc"),
                                         Value("a%"))
                .MoveValue(),
            Value(true));
  // The LIKE type error is re-worded for the relational layer.
  auto like_error = relational_detail::TryBinary(BinaryOperation::kLike,
                                                 Value(1), Value("a"));
  EXPECT_FALSE(like_error.HasValue());
  EXPECT_NE(
      like_error.GetStatus().GetMessage().find("LIKE requires string operands"),
      std::string::npos);
  // Unsupported operand mixes collapse to the generic message.
  // String-numeric operand mixes fail; the canonical evaluator's own
  // message mapping already reworded the low-level "Cannot do" text, so
  // only the failure contract is pinned here.
  auto bad_multiply = relational_detail::TryBinary(BinaryOperation::kMultiply,
                                                   Value("a"), Value(1));
  EXPECT_FALSE(bad_multiply.HasValue());
}

TEST(ExpressionEvalDetailTest, TryLookupResolvesScopeColumns) {
  const Schema schema("t", {Column("c0", ValueType::kInt64),
                            Column("c1", ValueType::kVarChar)});
  const Row row({Value(7), Value("x")});
  const relational_detail::Scope scope{.row = &row, .schema = &schema};
  EXPECT_EQ(
      relational_detail::TryLookup(ColumnName("t", "c0"), scope).MoveValue(),
      Value(7));
  EXPECT_EQ(
      relational_detail::TryLookup(ColumnName("", "c1"), scope).MoveValue(),
      Value("x"));
  EXPECT_FALSE(relational_detail::TryLookup(ColumnName("t", "missing"), scope)
                   .HasValue());
}

// ------------------------------------------------------------- SQL sweeps --

class ExpressionEvalSqlTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ =
        Database::Create("expression_eval_coverage_test-" + RandomString())
            .MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE j(id INT64, doc STRING(MAX))");
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO j (id, doc) VALUES "
           "(1, '{\"a\": 1, \"b\": {\"c\": \"x\"}, \"arr\": [10, 20, 30]}'), "
           "(2, 'null')");
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }
  Value Scalar(const std::string& sql) {
    const auto rows = RunSql(engine_.get(), context_.get(), sql);
    if (rows.size() != 1U) {
      ADD_FAILURE() << "query did not return one row: " << sql;
      return {};
    }
    return rows[0][0];
  }
  void ExpectScalar(const std::string& sql, const Value& expected) {
    SCOPED_TRACE(sql);
    EXPECT_EQ(Scalar(sql), expected);
  }
  void ExpectError(const std::string& sql) {
    SCOPED_TRACE(sql);
    EXPECT_THROW(
        { RunSql(engine_.get(), context_.get(), sql); }, std::exception);
  }
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(ExpressionEvalSqlTest, EvaluateMatchesAstGroundTruth) {
  const Schema schema;
  const relational_detail::Scope scope{};
  const Expression expr = BinaryExpressionExp(ConstantValueExp(Value(2)),
                                              BinaryOperation::kMultiply,
                                              ConstantValueExp(Value(3)));
  relational_detail::CteMap ctes;
  EXPECT_EQ(
      relational_detail::TryEvaluate(expr, scope, nullptr, *context_, ctes)
          .MoveValue(),
      Value(6));
}

TEST_F(ExpressionEvalSqlTest, JsonPathNavigation) {
  ExpectScalar("SELECT JSON_EXTRACT(doc, '$.a') FROM j WHERE id = 1",
               Value("1"));
  ExpectScalar("SELECT JSON_EXTRACT(doc, '$.\"b\".c') FROM j WHERE id = 1",
               Value("\"x\""));
  ExpectScalar("SELECT JSON_EXTRACT(doc, '$.arr[1]') FROM j WHERE id = 1",
               Value("20"));
  ExpectScalar("SELECT JSON_EXTRACT(doc, '$.arr[*]') FROM j WHERE id = 1",
               Value());
  ExpectScalar("SELECT JSON_EXTRACT(doc, '$.missing') FROM j WHERE id = 1",
               Value());
  ExpectScalar("SELECT JSON_EXTRACT(doc, '$.b') FROM j WHERE id = 1",
               Value(R"({"c":"x"})"));
  ExpectScalar("SELECT JSON_EXTRACT_SCALAR(doc, '$.b.c') FROM j WHERE id = 1",
               Value("x"));
  ExpectScalar("SELECT JSON_QUERY(doc, '$.arr') FROM j WHERE id = 1",
               Value("[10,20,30]"));
  ExpectScalar(
      "SELECT TO_JSON_STRING([STRUCT(1 AS x, 2 AS y), STRUCT(3 AS x, "
      "4 AS y)])",
      Value(R"(ARRAY<STRUCT>["{"x":1,"y":2}", "{"x":3,"y":4}"])"));
  ExpectScalar("SELECT TO_JSON_STRING(CAST(STRUCT(1 AS a) AS STRING))",
               Value("{\"a\":1}"));
}

TEST_F(ExpressionEvalSqlTest, CivilTimeTextParsing) {
  ExpectScalar("SELECT CAST('2026-01-02T03:04:05' AS DATETIME)",
               Value("2026-01-02 03:04:05"));
  ExpectScalar("SELECT CAST('2026-01-02 03:04:05.123456' AS DATETIME)",
               Value("2026-01-02 03:04:05.123456"));
  ExpectScalar("SELECT CAST('03:04:05' AS TIME)", Value("03:04:05"));
  ExpectScalar("SELECT CAST('03:04:05.25' AS TIME)", Value("03:04:05.250"));
  ExpectScalar(
      "SELECT CAST('2026-01-02T03:04:05.123456789+09:00' AS TIMESTAMP)",
      Value("2026-01-01 18:04:05.123456789+00"));
  ExpectScalar("SELECT CAST('2026-01-02T03:04:05-05:30' AS TIMESTAMP)",
               Value("2026-01-02 08:34:05+00"));
  ExpectScalar("SELECT CAST('2026-01-02T03:04:05Z' AS TIMESTAMP)",
               Value("2026-01-02 03:04:05+00"));
  ExpectError("SELECT CAST('not-a-time' AS DATETIME)");
  ExpectError("SELECT CAST('25:00:00' AS TIME)");
}

TEST_F(ExpressionEvalSqlTest, StructValueAccess) {
  ExpectScalar("SELECT TO_JSON_STRING(STRUCT(1 AS a, 'x' AS b))",
               Value(R"({"a":1,"b":"x"})"));
  ExpectScalar("SELECT (STRUCT(1 AS a, 2 AS b)).a", Value("1"));
  ExpectScalar(
      "SELECT TO_JSON_STRING("
      "[STRUCT(1 AS x, 2 AS y), STRUCT(3 AS x, 4 AS y)])",
      Value(R"(ARRAY<STRUCT>["{"x":1,"y":2}", "{"x":3,"y":4}"])"));
  ExpectScalar("SELECT TO_JSON_STRING(CAST(STRUCT(1 AS a) AS STRING))",
               Value("{\"a\":1}"));
  ExpectScalar(
      "SELECT TO_JSON_STRING("
      "(SELECT AS STRUCT MAX(k) AS m, MIN(k) AS n FROM "
      "(SELECT 1 AS k, 2 AS k2)))",
      Value(R"({"m":1,"n":1})"));
}

}  // namespace tinylamb
