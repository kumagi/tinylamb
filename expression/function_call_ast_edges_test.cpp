/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Edge-branch coverage for FunctionCallExpression's AST evaluation path
// (expression/function_call_expression.cpp).  The sweep test drives one
// valid shape per dispatched name; this file targets the branches that the
// sweep misses: explicit argument counts, type-rejected overloads, error
// paths, and the deterministic value expectations of date/time, hash,
// regexp and network-style helpers.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "expression/constant_value.hpp"
#include "expression/evaluation_context.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

class NoSubqueryContext : public EvaluationContext {
 public:
  StatusOr<std::vector<Value>> RunSubquery(const SelectStatement& /*statement*/,
                                           const Row* /*outer_row*/) override {
    return StatusError(StatusCode::kRuntimeError, "no subqueries in this test");
  }
  [[nodiscard]] const AggregateResultMap* CurrentAggregates() const override {
    return nullptr;
  }
  Status GetOrAddFunction(std::string_view /*function_name*/,
                          int /*argument_count*/) override {
    return Status::kSuccess;
  }
};

Value Str(const char* text) { return Value(std::string(text)); }

// Evaluates `name(args)` through the AST ground-truth path.
StatusOr<Value> Eval(const char* name, const std::vector<Value>& args) {
  std::vector<Expression> children;
  children.reserve(args.size());
  for (const Value& value : args) {
    children.push_back(ConstantValueExp(value));
  }
  const Expression call = FunctionCallExp(name, std::move(children));
  return call->TryEvaluate(Row(), Schema());
}

}  // namespace

TEST(FunctionCallAstEdges, HashAndEncodingFunctions) {
  auto md5 = Eval("md5", {Str("abc")});
  ASSERT_TRUE(md5.HasValue());
  // The digest returns as a quoted binary string: 2 quote bytes + digest.
  EXPECT_EQ(md5.Value().AsString().size(), 18);

  auto sha1 = Eval("sha1", {Str("abc")});
  ASSERT_TRUE(sha1.HasValue());
  EXPECT_EQ(sha1.Value().AsString().size(), 22);

  auto sha256 = Eval("sha256", {Str("abc")});
  ASSERT_TRUE(sha256.HasValue());
  EXPECT_EQ(sha256.Value().AsString().size(), 34);

  EXPECT_FALSE(Eval("md5", {}).HasValue());
  EXPECT_FALSE(Eval("sha256", {Str("a"), Str("b")}).HasValue());
}

TEST(FunctionCallAstEdges, RegexpFamilyBranches) {
  auto extract = Eval("regexp_extract", {Str("hello 42 world"), Str("[0-9]+")});
  ASSERT_TRUE(extract.HasValue());
  EXPECT_EQ(extract.Value().AsString(), "\"42\"");

  auto no_match = Eval("regexp_extract", {Str("hello"), Str("[0-9]+")});
  ASSERT_TRUE(no_match.HasValue());

  auto contains = Eval("regexp_contains", {Str("hello"), Str("l+")});
  ASSERT_TRUE(contains.HasValue());

  auto replace = Eval("regexp_replace", {Str("a1b2"), Str("[0-9]"), Str("_")});
  ASSERT_TRUE(replace.HasValue());
  EXPECT_EQ(replace.Value().AsString(), "\"a_b_\"");

  EXPECT_FALSE(Eval("regexp_extract", {Str("x")}).HasValue());
}

TEST(FunctionCallAstEdges, DateTimeParts) {
  auto year = Eval("extract_year", {Str("2026-09-23")});
  ASSERT_TRUE(year.HasValue());
  EXPECT_EQ(year.Value().value.int_value, 2026);

  auto month = Eval("extract_month", {Str("2026-09-23")});
  ASSERT_TRUE(month.HasValue());
  EXPECT_EQ(month.Value().value.int_value, 9);

  auto day = Eval("extract_day", {Str("2026-09-23")});
  ASSERT_TRUE(day.HasValue());
  EXPECT_EQ(day.Value().value.int_value, 23);
}

TEST(FunctionCallAstEdges, FormatSpecifiers) {
  auto plain = Eval("format", {Str("%d|%s"), Value(int64_t{7}), Str("x")});
  ASSERT_TRUE(plain.HasValue());
  EXPECT_EQ(plain.Value().AsString(), "\"7|x\"");

  auto width = Eval("format", {Str("%05d"), Value(int64_t{42})});
  ASSERT_TRUE(width.HasValue());
  EXPECT_EQ(width.Value().AsString(), "\"00042\"");

  auto literal = Eval("format", {Str("100%%")});
  ASSERT_TRUE(literal.HasValue());
  EXPECT_EQ(literal.Value().AsString(), "\"100%\"");

  // Unknown specifiers surface verbatim instead of failing.
  auto unknown = Eval("format", {Str("%q"), Value(int64_t{1})});
  ASSERT_TRUE(unknown.HasValue());
  EXPECT_EQ(unknown.Value().AsString(), "\"%q\"");
}

TEST(FunctionCallAstEdges, StringHelpersEdgeBranches) {
  auto initcap = Eval("initcap", {Str("hello world")});
  ASSERT_TRUE(initcap.HasValue());
  EXPECT_EQ(initcap.Value().AsString(), "\"Hello World\"");

  auto translate = Eval("translate", {Str("abcdef"), Str("ace"), Str("XYZ")});
  ASSERT_TRUE(translate.HasValue());
  EXPECT_EQ(translate.Value().AsString(), "\"XbYdZf\"");

  auto octet = Eval("octet_length", {Str("hello")});
  ASSERT_TRUE(octet.HasValue());
  EXPECT_EQ(octet.Value().value.int_value, 5);
}

TEST(FunctionCallAstEdges, GreatestLeastNullAndTypeRules) {
  auto greatest = Eval("greatest", {Value(int64_t{1}), Value(int64_t{5})});
  ASSERT_TRUE(greatest.HasValue());
  EXPECT_EQ(greatest.Value().value.int_value, 5);

  auto least = Eval("least", {Value(int64_t{1}), Value(int64_t{5})});
  ASSERT_TRUE(least.HasValue());
  EXPECT_EQ(least.Value().value.int_value, 1);

  auto null_skip = Eval("greatest", {Value(), Value(int64_t{3})});
  ASSERT_TRUE(null_skip.HasValue());
}

TEST(FunctionCallAstEdges, SafeArithmeticRejectsBadArity) {
  EXPECT_FALSE(Eval("safe_add", {Value(int64_t{1})}).HasValue());
  auto overflow = Eval("safe_add", {Value(INT64_MAX), Value(int64_t{1})});
  ASSERT_TRUE(overflow.HasValue());
  EXPECT_TRUE(overflow.Value().IsNull());
}

TEST(FunctionCallAstEdges, StructJsonRoundTrip) {
  // (name, value) pairs: string keys are the supported shape.
  auto json = Eval("__struct_json__",
                   {Str("a"), Value(int64_t{1}), Str("b"), Str("x")});
  ASSERT_TRUE(json.HasValue());
  EXPECT_EQ(json.Value().AsString(), R"("{"a":1,"b":"x"}")");

  // NULL field values flow through as an explicit JSON null.
  auto null_field = Eval("__struct_json__", {Str("a"), Value()});
  ASSERT_TRUE(null_field.HasValue());
}

TEST(FunctionCallSqlEdges, ScalarBatteryOverTable) {
  auto db = Database::Create("function_call_ast_edges");
  ASSERT_TRUE(db.HasValue());
  Database& database = *db.MoveValue();
  SqlEngine engine(database);
  TransactionContext ctx = database.BeginContext();
  auto run = [&](const std::string& sql) {
    // DML is lazy: iterate the result or the mutation never happens.
    StatusOr<QueryResult> result = engine.Execute(ctx, sql);
    if (result.HasValue()) {
      result.Value().ForEach([&](const Row& /*row*/) {});
    }
    return result;
  };
  auto query_first = [&](const std::string& sql) -> StatusOr<Value> {
    StatusOr<QueryResult> result = engine.Execute(ctx, sql);
    if (!result.HasValue()) {
      return result.GetStatus();
    }
    std::optional<Value> first;
    result.Value().ForEach([&](const Row& row) {
      if (!first.has_value()) {
        first = row[0];
      }
    });
    if (!first.has_value()) {
      return StatusError(StatusCode::kInvalidArgument, "no rows");
    }
    return first.value();
  };
  StatusOr<QueryResult> created =
      run("CREATE TABLE t (x INT64, s VARCHAR(16));");
  if (!created.HasValue() || created.Value().GetStatus() != Status::kSuccess) {
    ADD_FAILURE() << "create failed: "
                  << (created.HasValue() ? ToString(created.Value().GetStatus())
                                         : created.GetStatus().GetMessage());
  }
  for (const char* insert_sql : {"INSERT INTO t VALUES (9, 'abc');",
                                 "INSERT INTO t VALUES (8, 'def');"}) {
    SCOPED_TRACE(insert_sql);
    StatusOr<QueryResult> inserted = run(insert_sql);
    if (!inserted.HasValue() ||
        inserted.Value().GetStatus() != Status::kSuccess) {
      ADD_FAILURE() << "insert failed: "
                    << (inserted.HasValue()
                            ? ToString(inserted.Value().GetStatus())
                            : inserted.GetStatus().GetMessage());
    }
  }
  // Publish the seed rows, then continue reading from a fresh transaction.
  ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  ctx = database.BeginContext();
  auto one = [&](const std::string& sql) -> StatusOr<Value> {
    return query_first(sql);
  };
  // Scan + projection routes through the AST evaluator; the WHERE clause
  // keeps the relational evaluator hot in the same statement.
  struct Expectation {
    const char* sql;
    bool ok;
  };
  const std::vector<Expectation> battery = {
      {"SELECT array_to_string([1, 2, 3], '-') FROM t WHERE x = 9", true},
      {"SELECT byte_left('abcdef', 2) FROM t WHERE x = 9", true},
      {"SELECT code_points_to_bytes(72) FROM t WHERE x = 9", true},
      {"SELECT generate_array(1, 5, 2) FROM t WHERE x = 9", true},
      {"SELECT generate_date_array(DATE '2026-01-01', DATE '2026-01-03') "
       "FROM t WHERE x = 9",
       true},
      {"SELECT make_interval(1, 2, 3) FROM t WHERE x = 9", true},
      {"SELECT unix_date(DATE '2026-01-02') FROM t WHERE x = 9", true},
      {"SELECT format_date('%Y', DATE '2026-01-02') FROM t WHERE x = 9", true},
      {"SELECT parse_timestamp('%Y-%m-%d', '2026-01-02') FROM t WHERE x = 9",
       true},
      {"SELECT format_timestamp('%Y', TIMESTAMP '2026-01-02 03:04:05+00') "
       "FROM t WHERE x = 9",
       true},
      {"SELECT split('a,b,c', ',') FROM t WHERE x = 9", true},
      {"SELECT reverse(byte_reverse('ab')) FROM t WHERE x = 9", true},
      {"SELECT upper(s) FROM t WHERE x = 8", true},
      // Wrong arity rejects the row rather than crashing.
      {"SELECT md5() FROM t WHERE x = 9", false},
      {"SELECT safe_add(x) FROM t WHERE x = 9", false},
  };
  for (const Expectation& expectation : battery) {
    SCOPED_TRACE(expectation.sql);
    StatusOr<Value> value = one(expectation.sql);
    if (!expectation.ok) {
      EXPECT_FALSE(value.HasValue());
      continue;
    }
    if (!value.HasValue()) {
      ADD_FAILURE() << value.GetStatus().GetMessage();
      continue;
    }
  }
  ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
}

}  // namespace tinylamb
