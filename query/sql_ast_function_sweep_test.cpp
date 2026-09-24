/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// AST-evaluator function sweep at the SQL layer: every SELECT-list function
// call over a scanned table runs through the projection executor's AST
// evaluator (expression/function_call_expression.cpp), complementing the
// relational-evaluator sweep in executor/expression_eval_function_sweep_test
// .cpp.  Cases execute per-row against a fixture table and pin canonical
// results; argument contracts mirror GoogleSQL semantics.

#include <gtest/gtest.h>

#include <cmath>
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

class SqlAstFunctionSweepTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("sql_ast_function_sweep-" + RandomString())
                    .MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    RunLocal("CREATE TABLE f(id INT64, s STRING(MAX), d FLOAT64)");
    RunLocal(
        "INSERT INTO f VALUES (1, 'Hello World', 2.5), "
        "(2, '  padded  ', -4.0), (3, NULL, NULL)");
  }
  void TearDown() override {
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  // Runs the statement on a private engine wrapper; pass nullptr as the
  // engine to use one.
  std::vector<Row> RunLocal(const std::string& sql) {
    SqlEngine local(*database_);
    return RunSql(&local, context_.get(), sql);
  }

  // Runs `SELECT <expr> FROM f ORDER BY id` and returns the projected first
  // column per row (AST evaluator via the projection executor).
  std::vector<Value> Column(const std::string& expr) {
    const auto rows = RunLocal("SELECT " + expr + " FROM f ORDER BY id");
    std::vector<Value> out;
    out.reserve(rows.size());
    for (const Row& row : rows) {
      out.push_back(row[0]);
    }
    return out;
  }
  void ExpectColumn(const std::string& expr, const std::vector<Value>& want) {
    SCOPED_TRACE(expr);
    const auto got = Column(expr);
    ASSERT_EQ(got.size(), want.size()) << expr;
    for (size_t i = 0; i < got.size(); ++i) {
      EXPECT_EQ(got[i], want[i]) << "row " << i << " of " << expr;
    }
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
};

TEST_F(SqlAstFunctionSweepTest, StringFamilyThroughProjection) {
  ExpectColumn("upper(s)", {Value(std::string("HELLO WORLD")),
                            Value(std::string("  PADDED  ")), Value()});
  ExpectColumn("lower(s)", {Value(std::string("hello world")),
                            Value(std::string("  padded  ")), Value()});
  ExpectColumn("length(s)", {Value(int64_t{11}), Value(int64_t{10}), Value()});
  ExpectColumn("trim(s)", {Value(std::string("Hello World")),
                           Value(std::string("padded")), Value()});
  ExpectColumn("reverse(s)", {Value(std::string("dlroW olleH")),
                              Value(std::string("  deddap  ")), Value()});
  ExpectColumn("concat(s, '!')", {Value(std::string("Hello World!")),
                                  Value(std::string("  padded  !")), Value()});
  ExpectColumn("substr(s, 1, 5)", {Value(std::string("Hello")),
                                   Value(std::string("  pad")), Value()});
  ExpectColumn("replace(s, 'l', 'L')",
               {Value(std::string("HeLLo WorLd")),
                Value(std::string("  padded  ")), Value()});
  ExpectColumn("starts_with(s, 'Hello')", {Value(true), Value(false), Value()});
  ExpectColumn("ends_with(s, 'World')", {Value(true), Value(false), Value()});
  ExpectColumn("ascii(s)", {Value(int64_t{72}), Value(int64_t{32}), Value()});
  ExpectColumn("repeat(s, 2)",
               {Value(std::string("Hello WorldHello World")),
                Value(std::string("  padded    padded  ")), Value()});
  ExpectColumn("lpad(s, 15, '*')",
               {Value(std::string("****Hello World")),
                Value(std::string("*****  padded  ")), Value()});
  ExpectColumn("instr(s, 'World')",
               {Value(int64_t{7}), Value(int64_t{0}), Value()});
}

TEST_F(SqlAstFunctionSweepTest, MathFamilyThroughProjection) {
  ExpectColumn("abs(d)", {Value(2.5), Value(4.0), Value()});
  ExpectColumn("ceil(d)", {Value(3.0), Value(-4.0), Value()});
  ExpectColumn("floor(d)", {Value(2.0), Value(-4.0), Value()});
  ExpectColumn("round(d)", {Value(3.0), Value(-4.0), Value()});
  // SQRT of a negative input raises, so guard with ABS.
  ExpectColumn("sqrt(abs(d))",
               {Value(1.5811388300841898), Value(2.0), Value()});
  ExpectColumn("exp(0)", {Value(1.0), Value(1.0), Value(1.0)});
  ExpectColumn("sign(d)", {Value(1.0), Value(-1.0), Value()});
  ExpectColumn("greatest(d, 0)", {Value(2.5), Value(0.0), Value()});
  ExpectColumn("least(id, 2)",
               {Value(int64_t{1}), Value(int64_t{2}), Value(int64_t{2})});
}

TEST_F(SqlAstFunctionSweepTest, ConditionalAndJsonFamily) {
  ExpectColumn("coalesce(s, 'n/a')",
               {Value(std::string("Hello World")),
                Value(std::string("  padded  ")), Value(std::string("n/a"))});
  ExpectColumn("ifnull(s, 'x')",
               {Value(std::string("Hello World")),
                Value(std::string("  padded  ")), Value(std::string("x"))});
  ExpectColumn("nullif(id, 2)",
               {Value(int64_t{1}), Value(), Value(int64_t{3})});
  ExpectColumn("if(id > 2, 'big', 'small')",
               {Value(std::string("small")), Value(std::string("small")),
                Value(std::string("big"))});
  ExpectColumn("json_extract_scalar('{\"a\": 7}', '$.a')",
               {Value(std::string("7")), Value(std::string("7")),
                Value(std::string("7"))});
}

TEST_F(SqlAstFunctionSweepTest, ArrayAndDateFamily) {
  ExpectColumn("array_length([10, 20, 30])",
               {Value(int64_t{3}), Value(int64_t{3}), Value(int64_t{3})});
  ExpectColumn("array_first([10, 20])",
               {Value(int64_t{10}), Value(int64_t{10}), Value(int64_t{10})});
  ExpectColumn("array_to_string([id, id], '-')",
               {Value(std::string("1-1")), Value(std::string("2-2")),
                Value(std::string("3-3"))});
  ExpectColumn(
      "extract(year from DATE '2026-09-22')",
      {Value(int64_t{2026}), Value(int64_t{2026}), Value(int64_t{2026})});
  ExpectColumn("extract(month from DATE '2026-09-22')",
               {Value(int64_t{9}), Value(int64_t{9}), Value(int64_t{9})});
  ExpectColumn("format_date('%Y', DATE '2026-09-22')",
               {Value(std::string("2026")), Value(std::string("2026")),
                Value(std::string("2026"))});
}

TEST_F(SqlAstFunctionSweepTest, NullArgumentsPropagatePerContract) {
  // d is NULL on row 3: functions either propagate NULL or fail cleanly.
  const auto rows = RunLocal("SELECT ln(d), d + 1, d * 2 FROM f ORDER BY id");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_TRUE(rows[2][0].IsNull());
  EXPECT_TRUE(rows[2][1].IsNull());
  EXPECT_TRUE(rows[2][2].IsNull());
}

}  // namespace tinylamb
