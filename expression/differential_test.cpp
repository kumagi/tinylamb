/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Differential test for the expression evaluation engines (improvement3.md
// A6 / S7).  The canonical semantics are the AST evaluators; every fast path
// must agree per docs/expression_evaluation.md:
//   (a) AST      : Expression::Evaluate(Row, Schema)
//   (b) Bytecode : BytecodeCompiler::Compile + EvaluateBatch
//   (c) JIT      : JitInt64Kernels filter/projection kernels (narrow shapes,
//                  non-NULL INT64 inputs only; otherwise skipped)
// relational_detail::Evaluate (scan-filter fallback) is additionally observed
// where it is expected to agree.  Its former divergences (three-valued logic,
// numeric edge cases) were resolved by forwarding its Binary/Unary dispatch to
// the canonical evaluators, and conformance tests at the bottom of this file
// pin that agreement so it cannot drift back.
//
// Matrix: {arithmetic, comparison, logical, LIKE, concat, CASE, IN,
// date_add/date_sub, extract} x {int64, double, varchar, date, bool-as-int64}
// x {non-NULL, left NULL, right NULL, both NULL} plus exception cells
// (division by zero, INT64_MIN/-1, int64 overflow, type mismatch).
#include <cmath>
#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/bytecode.hpp"
// Pulls the production EvaluationContext adapter, which transitively provides
// the concrete TransactionContext needed by the relational_detail driver
// below while keeping this expression-directory TU free of database/
// includes.
#include "executor/data_chunk.hpp"
#include "executor/detail/expression_eval.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/jit.hpp"
#include "gtest/gtest.h"
#include "query/evaluation_context_impl.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

constexpr int64_t kInt64Min = std::numeric_limits<int64_t>::min();
constexpr int64_t kInt64Max = std::numeric_limits<int64_t>::max();

Value TextValue(std::string_view text) { return Value(std::string(text)); }

DataChunk MakeSingleRowChunk(const Schema& schema, const Row& row) {
  Row padded(row);
  while (padded.values_.size() < schema.ColumnCount()) {
    padded.values_.push_back(Value());
  }
  DataChunk chunk(schema);
  chunk.Append(padded);
  return chunk;
}

std::string DescribeValue(const Value& value) {
  return value.IsNull() ? "NULL" : value.AsString();
}

std::string DescribeRow(const Schema& schema, const Row& row) {
  std::string description;
  for (size_t i = 0; i < schema.ColumnCount() && i < row.Size(); ++i) {
    if (!description.empty()) {
      description += ", ";
    }
    description +=
        schema.GetColumn(i).Name().name + "=" + DescribeValue(row[i]);
  }
  return description;
}

// One evaluation outcome on one path.
struct Attempt {
  enum class Kind { kValue, kThrow, kUnsupported };
  Kind kind = Kind::kUnsupported;
  std::string note;  // throw message or unsupported reason
  std::string exception;
  Value value;
};

Attempt Unsupported(std::string reason) {
  Attempt attempt;
  attempt.note = std::move(reason);
  return attempt;
}

Attempt Thrown(const std::exception& error) {
  Attempt attempt;
  attempt.kind = Attempt::Kind::kThrow;
  attempt.exception = dynamic_cast<const std::logic_error*>(&error) != nullptr
                          ? "logic_error"
                          : "runtime_error";
  attempt.note = error.what();
  return attempt;
}

Attempt Evaluated(Value value) {
  Attempt attempt;
  attempt.kind = Attempt::Kind::kValue;
  attempt.value = std::move(value);
  return attempt;
}

Attempt EvaluateAst(const Expression& expression, const Row& row,
                    const Schema& schema) {
  try {
    return Evaluated(expression->Evaluate(row, schema));
  } catch (const std::exception& error) {
    return Thrown(error);
  }
}

Attempt EvaluateBytecode(const std::optional<BytecodeProgram>& program,
                         const Schema& schema, const Row& row) {
  if (!program.has_value()) {
    return Unsupported("bytecode compile failed");
  }
  try {
    return Evaluated(
        program->EvaluateBatch(MakeSingleRowChunk(schema, row)).ValueAt(0));
  } catch (const std::exception& error) {
    return Thrown(error);
  }
}

Attempt EvaluateDetailPath(const Expression& expression, const Row& row,
                           const Schema& schema) {
  TransactionContext context(Transaction{}, nullptr);
  const relational_detail::Scope scope{&row, &schema, nullptr};
  const relational_detail::CteMap ctes;
  try {
    return Evaluated(
        relational_detail::Evaluate(expression, scope, nullptr, context, ctes));
  } catch (const std::exception& error) {
    return Thrown(error);
  }
}

bool SameValue(const Value& left, const Value& right) {
  if (left.IsNull() || right.IsNull()) {
    return left.IsNull() && right.IsNull();
  }
  if (left.type != right.type) {
    return false;
  }
  if (left.type == ValueType::kDouble) {
    // Both paths run identical FP expressions; require bit-identical results
    // so inf/NaN cells compare deterministically.
    const bool left_nan = std::isnan(left.value.double_value);
    if (left_nan != std::isnan(right.value.double_value)) {
      return false;
    }
    return left_nan || left.value.double_value == right.value.double_value;
  }
  return left == right;
}

std::string Describe(const Attempt& attempt) {
  switch (attempt.kind) {
    case Attempt::Kind::kValue:
      return "value=" + DescribeValue(attempt.value);
    case Attempt::Kind::kThrow:
      return attempt.exception + "(" + attempt.note + ")";
    case Attempt::Kind::kUnsupported:
      return "unsupported(" + attempt.note + ")";
  }
  return "?";
}

// Compares every available path for one matrix cell.  Cells with fewer than
// two available paths are counted as skipped (and printed); a mismatch is a
// test failure carrying the cell name and the concrete input row.
class DifferentialTally {
 public:
  void Compare(const std::string& cell, const std::string& input,
               const std::vector<std::pair<std::string, Attempt>>& paths,
               const std::optional<Value>& oracle = std::nullopt) {
    std::vector<std::pair<const std::string*, const Attempt*>> available;
    for (const auto& path : paths) {
      if (path.second.kind == Attempt::Kind::kUnsupported) {
        std::cout << "[differential][skip] " << cell << " path=" << path.first
                  << ": " << path.second.note << std::endl;
        continue;
      }
      available.emplace_back(&path.first, &path.second);
    }
    skipped_ += static_cast<int>(paths.size() - available.size());
    if (!available.empty() &&
        available.front().second->kind == Attempt::Kind::kValue &&
        oracle.has_value()) {
      CheckOracle(cell, input, *available.front().second, *oracle);
    }
    if (available.size() < 2) {
      return;
    }
    ++compared_;
    for (size_t i = 1; i < available.size(); ++i) {
      ComparePair(cell, input, *available[i - 1].second, *available[i].second);
    }
  }

  void Summarize(const std::string& suite) const {
    std::cout << "[differential] " << suite << ": " << compared_
              << " cells compared, " << skipped_ << " path-skips" << std::endl;
  }

 private:
  static void CheckOracle(const std::string& cell, const std::string& input,
                          const Attempt& attempt, const Value& oracle) {
    EXPECT_EQ(attempt.kind, Attempt::Kind::kValue)
        << cell << " [" << input << "] expected a value";
    if (attempt.kind == Attempt::Kind::kValue) {
      EXPECT_TRUE(SameValue(attempt.value, oracle))
          << cell << " [" << input << "] oracle mismatch: got "
          << Describe(attempt);
    }
  }

  static void ComparePair(const std::string& cell, const std::string& input,
                          const Attempt& baseline, const Attempt& other) {
    const bool both_values = baseline.kind == Attempt::Kind::kValue &&
                             other.kind == Attempt::Kind::kValue;
    const bool both_throws = baseline.kind == Attempt::Kind::kThrow &&
                             other.kind == Attempt::Kind::kThrow;
    if (both_values) {
      EXPECT_TRUE(SameValue(baseline.value, other.value))
          << "[differential][MISMATCH] " << cell << " input[" << input
          << "] baseline=" << Describe(baseline)
          << " other=" << Describe(other);
      return;
    }
    if (both_throws) {
      EXPECT_EQ(baseline.exception, other.exception)
          << "[differential][MISMATCH] " << cell << " input[" << input
          << "] exception type differs";
      EXPECT_EQ(baseline.note, other.note)
          << "[differential][MISMATCH] " << cell << " input[" << input
          << "] exception message differs";
      return;
    }
    ADD_FAILURE() << "[differential][MISMATCH] " << cell << " input[" << input
                  << "] one path threw, another returned: baseline="
                  << Describe(baseline) << " other=" << Describe(other);
  }

  int compared_ = 0;
  int skipped_ = 0;
};

Expression GtZero(std::string_view column) {
  return BinaryExpressionExp(ColumnValueExp(column),
                             BinaryOperation::kGreaterThan,
                             ConstantValueExp(Value(int64_t{0})));
}

const Schema& IntSchema() {
  static const Schema schema(
      "ints", {Column("i", ValueType::kInt64), Column("j", ValueType::kInt64)});
  return schema;
}

const Schema& DoubleSchema() {
  static const Schema schema("reals", {Column("x", ValueType::kDouble),
                                       Column("y", ValueType::kDouble)});
  return schema;
}

const Schema& StringSchema() {
  static const Schema schema("strs", {Column("s", ValueType::kVarChar),
                                      Column("t", ValueType::kVarChar)});
  return schema;
}

const Schema& DateSchema() {
  static const Schema schema(
      "dates", {Column("d", ValueType::kDate), Column("e", ValueType::kDate)});
  return schema;
}

struct CaseCell {
  std::string name;
  Expression expression;
  std::vector<std::optional<Value>> oracles;
};

struct InCell {
  std::string name;
  Expression expression;
  std::vector<std::optional<Value>> oracles;
};

std::vector<CaseCell> BuildCaseCells() {
  std::vector<CaseCell> cells;
  cells.push_back({
      "case/hit-miss-null-cond",
      CaseExpressionExp(
          {{BinaryExpressionExp(ColumnValueExp("i"),
                                BinaryOperation::kGreaterThan,
                                ConstantValueExp(Value(int64_t{3}))),
            ConstantValueExp(Value(int64_t{10}))}},
          ConstantValueExp(Value(int64_t{20}))),
      {Value(int64_t{10}), Value(int64_t{20}), Value(int64_t{20})},
  });
  cells.push_back({
      "case/no-else",
      CaseExpressionExp(
          {{BinaryExpressionExp(ColumnValueExp("i"),
                                BinaryOperation::kGreaterThan,
                                ConstantValueExp(Value(int64_t{3}))),
            ConstantValueExp(Value(int64_t{10}))}},
          nullptr),
      {Value(int64_t{10}), Value(), Value()},
  });
  cells.push_back({
      "case/constant-true",
      CaseExpressionExp({{ConstantValueExp(Value(true)),
                          ConstantValueExp(Value(int64_t{1}))}},
                        ConstantValueExp(Value(int64_t{2}))),
      {Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{1})},
  });
  cells.push_back({
      "case/constant-false",
      CaseExpressionExp({{ConstantValueExp(Value(false)),
                          ConstantValueExp(Value(int64_t{1}))}},
                        ConstantValueExp(Value(int64_t{2}))),
      {Value(int64_t{2}), Value(int64_t{2}), Value(int64_t{2})},
  });
  return cells;
}

std::vector<InCell> BuildInCells() {
  std::vector<InCell> cells;
  cells.push_back({
      "in/multi",
      InExpressionExp(ColumnValueExp("i"),
                      {ConstantValueExp(Value(int64_t{1})),
                       ConstantValueExp(Value(int64_t{7}))}),
      {Value(false), Value(false), Value()},
  });
  cells.push_back({
      "in/null-in-list",
      InExpressionExp(
          ColumnValueExp("i"),
          {ConstantValueExp(Value(int64_t{1})), ConstantValueExp(Value()),
           ConstantValueExp(Value(int64_t{2}))}),
      {Value(), Value(true), Value()},
  });
  cells.push_back({
      "in/single-element",
      InExpressionExp(ColumnValueExp("i"),
                      {ConstantValueExp(Value(int64_t{7}))}),
      {Value(false), Value(false), Value()},
  });
  cells.push_back({
      "in/all-constants",
      InExpressionExp(ConstantValueExp(Value(int64_t{7})),
                      {ConstantValueExp(Value(int64_t{1})),
                       ConstantValueExp(Value(int64_t{7}))}),
      {Value(true), Value(true), Value(true)},
  });
  return cells;
}

}  // namespace

TEST(DifferentialTest, Evaluate_ArithmeticMatrix_MatchesAcrossPaths) {
  const std::vector<std::pair<std::string, BinaryOperation>> operations{
      {"add", BinaryOperation::kAdd},      {"sub", BinaryOperation::kSubtract},
      {"mul", BinaryOperation::kMultiply}, {"div", BinaryOperation::kDivide},
      {"mod", BinaryOperation::kModulo},
  };
  const std::vector<Row> int_rows{
      Row({Value(int64_t{7}), Value(int64_t{3})}),
      Row({Value(), Value(int64_t{3})}),
      Row({Value(int64_t{7}), Value()}),
      Row({Value(), Value()}),
      Row({Value(int64_t{5}), Value(int64_t{0})}),
      Row({Value(kInt64Min), Value(int64_t{-1})}),
      Row({Value(kInt64Max), Value(int64_t{1})}),
  };
  const std::vector<Row> double_rows{
      Row({Value(2.5), Value(0.5)}), Row({Value(), Value(0.5)}),
      Row({Value(2.5), Value()}),    Row({Value(), Value()}),
      Row({Value(1.0), Value(0.0)}),
  };
  DifferentialTally tally;
  for (const auto& [op_name, op] : operations) {
    Expression int_expr =
        BinaryExpressionExp(ColumnValueExp("i"), op, ColumnValueExp("j"));
    auto int_program = BytecodeCompiler::Compile(int_expr, IntSchema());
    for (size_t r = 0; r < int_rows.size(); ++r) {
      tally.Compare("int64/" + op_name + "/row" + std::to_string(r),
                    DescribeRow(IntSchema(), int_rows[r]),
                    {{"ast", EvaluateAst(int_expr, int_rows[r], IntSchema())},
                     {"bytecode", EvaluateBytecode(int_program, IntSchema(),
                                                   int_rows[r])}});
    }
    Expression double_expr =
        BinaryExpressionExp(ColumnValueExp("x"), op, ColumnValueExp("y"));
    auto double_program =
        BytecodeCompiler::Compile(double_expr, DoubleSchema());
    for (size_t r = 0; r < double_rows.size(); ++r) {
      tally.Compare(
          "double/" + op_name + "/row" + std::to_string(r),
          DescribeRow(DoubleSchema(), double_rows[r]),
          {{"ast", EvaluateAst(double_expr, double_rows[r], DoubleSchema())},
           {"bytecode",
            EvaluateBytecode(double_program, DoubleSchema(), double_rows[r])}});
    }
  }
  tally.Summarize("arithmetic");
}

TEST(DifferentialTest, Evaluate_ComparisonMatrix_MatchesAcrossPaths) {
  const std::vector<std::pair<std::string, BinaryOperation>> operations{
      {"eq", BinaryOperation::kEquals},
      {"ne", BinaryOperation::kNotEquals},
      {"lt", BinaryOperation::kLessThan},
      {"le", BinaryOperation::kLessThanEquals},
      {"gt", BinaryOperation::kGreaterThan},
      {"ge", BinaryOperation::kGreaterThanEquals},
  };
  struct Family {
    const char* type;
    const Schema* schema;
    std::vector<Row> rows;
  };
  const std::vector<Family> families{
      {"int64",
       &IntSchema(),
       {Row({Value(int64_t{7}), Value(int64_t{3})}),
        Row({Value(int64_t{3}), Value(int64_t{7})}),
        Row({Value(int64_t{7}), Value(int64_t{7})}),
        Row({Value(), Value(int64_t{7})}), Row({Value(int64_t{7}), Value()}),
        Row({Value(), Value()})}},
      {"double",
       &DoubleSchema(),
       {Row({Value(2.5), Value(0.5)}), Row({Value(0.5), Value(2.5)}),
        Row({Value(2.5), Value(2.5)}), Row({Value(), Value(2.5)}),
        Row({Value(), Value()})}},
      {"varchar",
       &StringSchema(),
       {Row({TextValue("abc"), TextValue("abc")}),
        Row({TextValue("abc"), TextValue("abd")}),
        Row({TextValue("abd"), TextValue("abc")}),
        Row({TextValue(""), TextValue("abc")}),
        Row({Value(), TextValue("abc")}), Row({TextValue("abc"), Value()}),
        Row({Value(), Value()})}},
      {"date",
       &DateSchema(),
       {Row({Value::Date("1994-01-01"), Value::Date("1994-01-01")}),
        Row({Value::Date("1994-01-01"), Value::Date("1996-04-05")}),
        Row({Value::Date("1997-11-30"), Value::Date("1996-04-05")}),
        Row({Value(), Value::Date("1996-04-05")}),
        Row({Value::Date("1996-04-05"), Value()}), Row({Value(), Value()})}},
  };
  DifferentialTally tally;
  for (const Family& family : families) {
    for (const auto& [op_name, op] : operations) {
      const Expression predicate = BinaryExpressionExp(
          ColumnValueExp(family.schema->GetColumn(0).Name().name), op,
          ColumnValueExp(family.schema->GetColumn(1).Name().name));
      auto program = BytecodeCompiler::Compile(predicate, *family.schema);
      for (size_t r = 0; r < family.rows.size(); ++r) {
        tally.Compare(
            std::string(family.type) + "/" + op_name + "/row" +
                std::to_string(r),
            DescribeRow(*family.schema, family.rows[r]),
            {{"ast", EvaluateAst(predicate, family.rows[r], *family.schema)},
             {"bytecode",
              EvaluateBytecode(program, *family.schema, family.rows[r])}});
      }
    }
  }
  tally.Summarize("comparison");
}

TEST(DifferentialTest, Evaluate_LogicalThreeValuedLogic_MatchesAcrossPaths) {
  const std::vector<Row> rows{
      Row({Value(int64_t{1}), Value(int64_t{1})}),  // T T
      Row({Value(int64_t{1}), Value(int64_t{0})}),  // T F
      Row({Value(int64_t{0}), Value(int64_t{1})}),  // F T
      Row({Value(int64_t{0}), Value(int64_t{0})}),  // F F
      Row({Value(int64_t{1}), Value()}),            // T N
      Row({Value(), Value(int64_t{1})}),            // N T
      Row({Value(int64_t{0}), Value()}),            // F N
      Row({Value(), Value(int64_t{0})}),            // N F
      Row({Value(), Value()}),                      // N N
  };
  const std::vector<std::pair<std::string, BinaryOperation>> operations{
      {"and", BinaryOperation::kAnd}, {"or", BinaryOperation::kOr}};
  DifferentialTally tally;
  for (const auto& [op_name, op] : operations) {
    Expression expr = BinaryExpressionExp(GtZero("i"), op, GtZero("j"));
    auto program = BytecodeCompiler::Compile(expr, IntSchema());
    for (size_t r = 0; r < rows.size(); ++r) {
      tally.Compare(
          "logic/" + op_name + "/row" + std::to_string(r),
          DescribeRow(IntSchema(), rows[r]),
          {{"ast", EvaluateAst(expr, rows[r], IntSchema())},
           {"bytecode", EvaluateBytecode(program, IntSchema(), rows[r])}});
    }
  }
  // Constant truth table: exercises folding inside the bytecode compiler.
  const std::vector<Value> booleans{Value(true), Value(false), Value()};
  for (const auto& [op_name, op] : operations) {
    for (const Value& left : booleans) {
      for (const Value& right : booleans) {
        const Expression expr = BinaryExpressionExp(ConstantValueExp(left), op,
                                                    ConstantValueExp(right));
        auto program = BytecodeCompiler::Compile(expr, IntSchema());
        tally.Compare(
            "logic-const/" + op_name,
            "lhs=" + DescribeValue(left) + ",rhs=" + DescribeValue(right),
            {{"ast", EvaluateAst(expr, Row(), IntSchema())},
             {"bytecode", EvaluateBytecode(program, IntSchema(), Row())}});
      }
    }
  }
  for (const Value& child : booleans) {
    const Expression expr =
        UnaryExpressionExp(ConstantValueExp(child), UnaryOperation::kNot);
    auto program = BytecodeCompiler::Compile(expr, IntSchema());
    tally.Compare(
        "logic/not", "child=" + DescribeValue(child),
        {{"ast", EvaluateAst(expr, Row(), IntSchema())},
         {"bytecode", EvaluateBytecode(program, IntSchema(), Row())}});
  }
  const std::vector<std::pair<std::string, UnaryOperation>> unary_predicates{
      {"is_null", UnaryOperation::kIsNull},
      {"is_not_null", UnaryOperation::kIsNotNull},
      {"is_true", UnaryOperation::kIsTrue},
      {"is_not_true", UnaryOperation::kIsNotTrue},
      {"is_false", UnaryOperation::kIsFalse},
      {"is_not_false", UnaryOperation::kIsNotFalse}};
  for (const auto& [name, op] : unary_predicates) {
    for (const Value& child : booleans) {
      const Expression expr = UnaryExpressionExp(ConstantValueExp(child), op);
      auto program = BytecodeCompiler::Compile(expr, IntSchema());
      tally.Compare(
          "logic/" + name, "child=" + DescribeValue(child),
          {{"ast", EvaluateAst(expr, Row(), IntSchema())},
           {"bytecode", EvaluateBytecode(program, IntSchema(), Row())}});
    }
  }
  tally.Summarize("logical");
}

// D7 (docs/design.md) acceptance 1 + 2: the Bytecode VM must short-circuit
// AND/OR exactly like the AST, so an error on the right-hand side (division
// by zero) is raised ONLY for the rows where the left operand does not
// already decide the result.  A FALSE AND-guard must suppress the RHS error
// and a TRUE OR-guard must suppress the RHS error.
TEST(DifferentialTest, Evaluate_LogicalShortCircuitErrors_MatchAcrossPaths) {
  const Expression ne0 =
      BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kNotEquals,
                          ConstantValueExp(Value(int64_t{0})));
  const Expression divj =
      BinaryExpressionExp(ConstantValueExp(Value(int64_t{10})),
                          BinaryOperation::kDivide, ColumnValueExp("j"));
  const Expression rhs = BinaryExpressionExp(
      divj, BinaryOperation::kGreaterThan, ConstantValueExp(Value(int64_t{1})));
  const std::vector<Expression> guards{
      BinaryExpressionExp(ne0, BinaryOperation::kAnd, rhs),
      BinaryExpressionExp(ne0, BinaryOperation::kOr, rhs),
  };
  // i=0 decides AND (skip rhs), i≠0 decides OR (skip rhs); j=0 is the
  // throwing RHS cell; NULL i / NULL j exercise the three-valued rows.
  const std::vector<Row> rows{
      Row({Value(int64_t{0}), Value(int64_t{0})}),  // AND skips the throw
      Row({Value(int64_t{5}), Value(int64_t{0})}),  // OR skips the throw
      Row({Value(int64_t{5}), Value(int64_t{2})}),
      Row({Value(int64_t{0}), Value(int64_t{2})}),
      Row({Value(), Value(int64_t{0})}),  // NULL lhs: rhs runs
      Row({Value(), Value(int64_t{2})}),
      Row({Value(int64_t{5}), Value()}),  // rhs NULL compares NULL
      Row({Value(), Value()}),
  };
  DifferentialTally tally;
  for (const Expression& expr : guards) {
    auto program = BytecodeCompiler::Compile(expr, IntSchema());
    for (size_t r = 0; r < rows.size(); ++r) {
      tally.Compare(
          "shortcircuit/row" + std::to_string(r),
          DescribeRow(IntSchema(), rows[r]),
          {{"ast", EvaluateAst(expr, rows[r], IntSchema())},
           {"bytecode", EvaluateBytecode(program, IntSchema(), rows[r])}});
    }
  }
  tally.Summarize("logical short-circuit");
}

TEST(DifferentialTest, Evaluate_LikePatterns_MatchesAcrossPaths) {
  const std::vector<const char*> patterns{"abc", "a%", "%c",  "a%c",
                                          "_bc", "%",  "ab_", "a_c"};
  const std::vector<Value> values{TextValue("abc"), TextValue("abd"),
                                  TextValue(""), Value()};
  DifferentialTally tally;
  size_t cell = 0;
  for (const char* pattern : patterns) {
    const Expression expr =
        BinaryExpressionExp(ColumnValueExp("s"), BinaryOperation::kLike,
                            ConstantValueExp(TextValue(pattern)));
    auto program = BytecodeCompiler::Compile(expr, StringSchema());
    for (const Value& value : values) {
      const Row row({value, TextValue("unused")});
      tally.Compare(
          "like/cell" + std::to_string(cell++),
          "s=" + DescribeValue(value) + ",pattern=" + pattern,
          {{"ast", EvaluateAst(expr, row, StringSchema())},
           {"bytecode", EvaluateBytecode(program, StringSchema(), row)}});
    }
  }
  const std::vector<const char*> not_like_patterns{"abc", "a%", "%c"};
  for (const char* pattern : not_like_patterns) {
    const Expression expr =
        BinaryExpressionExp(ColumnValueExp("s"), BinaryOperation::kNotLike,
                            ConstantValueExp(TextValue(pattern)));
    auto program = BytecodeCompiler::Compile(expr, StringSchema());
    for (const Value& value : values) {
      const Row row({value, TextValue("unused")});
      tally.Compare(
          "not-like/cell" + std::to_string(cell++),
          "s=" + DescribeValue(value) + ",pattern=" + pattern,
          {{"ast", EvaluateAst(expr, row, StringSchema())},
           {"bytecode", EvaluateBytecode(program, StringSchema(), row)}});
    }
  }
  const std::vector<Row> pair_rows{
      Row({TextValue("abc"), TextValue("abc")}),
      Row({TextValue("abc"), Value()}),
      Row({Value(), TextValue("%")}),
      Row({Value(), Value()}),
  };
  const Expression like_column = BinaryExpressionExp(
      ColumnValueExp("s"), BinaryOperation::kLike, ColumnValueExp("t"));
  auto like_program = BytecodeCompiler::Compile(like_column, StringSchema());
  for (size_t r = 0; r < pair_rows.size(); ++r) {
    tally.Compare(
        "like/columns/row" + std::to_string(r),
        DescribeRow(StringSchema(), pair_rows[r]),
        {{"ast", EvaluateAst(like_column, pair_rows[r], StringSchema())},
         {"bytecode",
          EvaluateBytecode(like_program, StringSchema(), pair_rows[r])}});
  }
  // Non-string operand: both paths must raise the unified message.
  const Row bad_row({Value(int64_t{5}), Value()});
  const Expression like_int =
      BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kLike,
                          ConstantValueExp(TextValue("a%")));
  auto bad_program = BytecodeCompiler::Compile(like_int, IntSchema());
  tally.Compare(
      "like/non-string", "i=5,pattern=a%",
      {{"ast", EvaluateAst(like_int, bad_row, IntSchema())},
       {"bytecode", EvaluateBytecode(bad_program, IntSchema(), bad_row)}});
  tally.Summarize("like");
}

TEST(DifferentialTest, Evaluate_StringConcatenation_MatchesAcrossPaths) {
  const std::vector<Row> rows{
      Row({TextValue("foo"), TextValue("bar")}),
      Row({Value(), TextValue("bar")}),
      Row({TextValue("foo"), Value()}),
      Row({Value(), Value()}),
      Row({TextValue(""), TextValue("x")}),
  };
  DifferentialTally tally;
  const Expression concat = BinaryExpressionExp(
      ColumnValueExp("s"), BinaryOperation::kAdd, ColumnValueExp("t"));
  auto program = BytecodeCompiler::Compile(concat, StringSchema());
  for (size_t r = 0; r < rows.size(); ++r) {
    tally.Compare(
        "concat/row" + std::to_string(r), DescribeRow(StringSchema(), rows[r]),
        {{"ast", EvaluateAst(concat, rows[r], StringSchema())},
         {"bytecode", EvaluateBytecode(program, StringSchema(), rows[r])}});
  }
  const Schema mixed_schema("mixed", {Column("i", ValueType::kInt64),
                                      Column("s", ValueType::kVarChar)});
  const Row mixed({Value(int64_t{5}), TextValue("x")});
  const Expression mixed_expr = BinaryExpressionExp(
      ColumnValueExp("i"), BinaryOperation::kAdd, ColumnValueExp("s"));
  auto mixed_program = BytecodeCompiler::Compile(mixed_expr, mixed_schema);
  tally.Compare(
      "concat/mixed-type", DescribeRow(mixed_schema, mixed),
      {{"ast", EvaluateAst(mixed_expr, mixed, mixed_schema)},
       {"bytecode", EvaluateBytecode(mixed_program, mixed_schema, mixed)}});
  tally.Summarize("concat");
}

TEST(DifferentialTest, Evaluate_CaseAndIn_MatchesAcrossPaths) {
  const Schema& schema = IntSchema();
  const std::vector<Row> rows{
      Row({Value(int64_t{9}), Value(int64_t{0})}),
      Row({Value(int64_t{2}), Value(int64_t{0})}),
      Row({Value(), Value(int64_t{0})}),
  };
  DifferentialTally tally;
  for (const CaseCell& cell : BuildCaseCells()) {
    auto program = BytecodeCompiler::Compile(cell.expression, schema);
    for (size_t r = 0; r < rows.size(); ++r) {
      tally.Compare(cell.name + "/row" + std::to_string(r),
                    DescribeRow(schema, rows[r]),
                    {{"ast", EvaluateAst(cell.expression, rows[r], schema)},
                     {"bytecode", EvaluateBytecode(program, schema, rows[r])}},
                    cell.oracles[r]);
    }
  }
  tally.Summarize("case");
  for (const InCell& cell : BuildInCells()) {
    auto program = BytecodeCompiler::Compile(cell.expression, schema);
    for (size_t r = 0; r < rows.size(); ++r) {
      tally.Compare(cell.name + "/row" + std::to_string(r),
                    DescribeRow(schema, rows[r]),
                    {{"ast", EvaluateAst(cell.expression, rows[r], schema)},
                     {"bytecode", EvaluateBytecode(program, schema, rows[r])}},
                    cell.oracles[r]);
    }
  }
  tally.Summarize("case-in");
}

TEST(DifferentialTest, Evaluate_DateFunctions_MatchesAcrossPaths) {
  DifferentialTally tally;
  const Row empty_row;

  // Literal arguments: the bytecode compiler constant-folds these, so the
  // folded result must still match the AST evaluator.
  const std::vector<std::pair<std::string, Expression>> folded{
      {"date_add/literal-year",
       FunctionCallExp("date_add", {ConstantValueExp(TextValue("1994-01-01")),
                                    IntervalExpressionExp(1, "year")})},
      {"date_sub/literal-day",
       FunctionCallExp("date_sub", {ConstantValueExp(TextValue("1998-12-01")),
                                    IntervalExpressionExp(74, "day")})},
      {"date_add/literal-null-date",
       FunctionCallExp("date_add", {ConstantValueExp(Value()),
                                    IntervalExpressionExp(1, "day")})},
      {"extract_year/literal",
       FunctionCallExp("extract_year",
                       {ConstantValueExp(TextValue("1996-07-15"))})},
      {"extract_month/literal",
       FunctionCallExp("extract_month",
                       {ConstantValueExp(TextValue("1996-07-15"))})},
      {"extract_day/literal",
       FunctionCallExp("extract_day",
                       {ConstantValueExp(TextValue("1996-07-15"))})},
      {"extract_year/null-literal",
       FunctionCallExp("extract_year", {ConstantValueExp(Value())})},
  };
  const std::vector<std::optional<Value>> oracles{
      TextValue("1995-01-01"),
      TextValue("1998-09-18"),
      Value(),
      Value(int64_t{1996}),
      Value(int64_t{7}),
      Value(int64_t{15}),
      Value(),
  };
  for (size_t c = 0; c < folded.size(); ++c) {
    auto program = BytecodeCompiler::Compile(folded[c].second, DateSchema());
    tally.Compare(
        folded[c].first, "literal",
        {{"ast", EvaluateAst(folded[c].second, empty_row, DateSchema())},
         {"bytecode", EvaluateBytecode(program, DateSchema(), empty_row)}},
        oracles[c]);
  }

  // Column argument: function shapes are bytecode-unsupported today; the
  // oracle pins the AST behaviour while the skip is displayed.
  const std::vector<Row> date_rows{
      Row({Value::Date("1994-01-01"), Value::Date("1996-04-05")}),
      Row({Value(), Value::Date("1996-04-05")}),
  };
  const std::vector<std::pair<std::string, Expression>> column_forms{
      {"date_add/column-year",
       FunctionCallExp("date_add", {ColumnValueExp("d"),
                                    IntervalExpressionExp(1, "year")})},
      {"date_sub/column-month",
       FunctionCallExp("date_sub", {ColumnValueExp("d"),
                                    IntervalExpressionExp(2, "month")})},
      {"extract_year/column",
       FunctionCallExp("extract_year", {ColumnValueExp("e")})},
      {"extract_day/null-column",
       FunctionCallExp("extract_day", {ColumnValueExp("d")})},
  };
  const std::vector<std::vector<std::optional<Value>>> column_oracles{
      {Value::Date("1995-01-01"), Value()},
      {Value::Date("1993-11-01"), Value()},
      {Value(int64_t{1996}), Value(int64_t{1996})},
      {Value(int64_t{1}), Value()},
  };
  for (size_t c = 0; c < column_forms.size(); ++c) {
    auto program =
        BytecodeCompiler::Compile(column_forms[c].second, DateSchema());
    for (size_t r = 0; r < date_rows.size(); ++r) {
      tally.Compare(
          column_forms[c].first + "/row" + std::to_string(r),
          DescribeRow(DateSchema(), date_rows[r]),
          {{"ast",
            EvaluateAst(column_forms[c].second, date_rows[r], DateSchema())},
           {"bytecode", EvaluateBytecode(program, DateSchema(), date_rows[r])}},
          column_oracles[c][r]);
    }
  }

  // extract over a non-date literal raises on the only supported path.
  const Expression bad_extract =
      FunctionCallExp("extract_year", {ConstantValueExp(Value(int64_t{5}))});
  const Attempt thrown = EvaluateAst(bad_extract, empty_row, DateSchema());
  EXPECT_EQ(thrown.kind, Attempt::Kind::kThrow);
  EXPECT_NE(thrown.note.find("EXTRACT"), std::string::npos);
  tally.Summarize("date-functions");
}

TEST(DifferentialTest,
     CompileFilter_SupportedOperations_MatchesAstAndBytecode) {
  const std::vector<std::pair<std::string, BinaryOperation>> operations{
      {"eq", BinaryOperation::kEquals},
      {"ne", BinaryOperation::kNotEquals},
      {"lt", BinaryOperation::kLessThan},
      {"le", BinaryOperation::kLessThanEquals},
      {"gt", BinaryOperation::kGreaterThan},
      {"ge", BinaryOperation::kGreaterThanEquals},
  };
  constexpr int64_t kConstant = 17;
  const std::vector<std::optional<int64_t>> samples{
      -2048, -17, 0, 17, 18, 1024, std::nullopt};
  const Schema filter_schema("filter", {Column("i", ValueType::kInt64)});
  for (const auto& [op_name, op] : operations) {
    const auto jit = JitInt64Kernels::CompileFilter(op);
    if (!jit.has_value()) {
      std::cout << "[differential][skip] jit-filter/" << op_name
                << ": kernel unavailable (LLVM disabled or op unsupported)"
                << std::endl;
      continue;
    }
    const Expression expr = BinaryExpressionExp(
        ColumnValueExp("i"), op, ConstantValueExp(Value(kConstant)));
    auto program = BytecodeCompiler::Compile(expr, filter_schema);
    ASSERT_TRUE(program.has_value()) << op_name;
    for (const std::optional<int64_t>& sample : samples) {
      if (!sample.has_value()) {
        std::cout << "[differential][skip] jit-filter/" << op_name
                  << ": NULL input excluded by the JIT no-NULL contract"
                  << std::endl;
        continue;
      }
      const Row row({Value(*sample)});
      const Value ast = expr->Evaluate(row, filter_schema);
      const Value bytecode =
          program->EvaluateBatch(MakeSingleRowChunk(filter_schema, row))
              .ValueAt(0);
      uint8_t jit_out = 0;
      jit->Filter(&*sample, &jit_out, 1, kConstant);
      EXPECT_EQ(bytecode, Value(jit_out != 0))
          << "jit-filter/" << op_name << " i=" << *sample;
    }
  }
}

TEST(DifferentialTest,
     CompileProjection_LinearTransformation_MatchesAstAndBytecode) {
  const auto jit = JitInt64Kernels::CompileProjection();
  if (!jit.has_value()) {
    GTEST_SKIP() << "JIT projection kernel unavailable (LLVM disabled)";
  }
  constexpr int64_t kMultiplier = 3;
  constexpr int64_t kAddend = 2;
  const Schema projection_schema("projection",
                                 {Column("i", ValueType::kInt64)});
  const Expression expr = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kMultiply,
                          ConstantValueExp(Value(kMultiplier))),
      BinaryOperation::kAdd, ConstantValueExp(Value(kAddend)));
  auto program = BytecodeCompiler::Compile(expr, projection_schema);
  ASSERT_TRUE(program.has_value());
  for (const int64_t sample :
       {int64_t{0}, int64_t{1}, int64_t{-3}, int64_t{9999999}}) {
    const Row row({Value(sample)});
    const Value ast = expr->Evaluate(row, projection_schema);
    const Value bytecode =
        program->EvaluateBatch(MakeSingleRowChunk(projection_schema, row))
            .ValueAt(0);
    EXPECT_EQ(ast, bytecode) << "jit-projection i=" << sample;
    int64_t jit_out = 0;
    jit->Project(&sample, &jit_out, 1, kMultiplier, kAddend);
    EXPECT_EQ(bytecode, Value(jit_out)) << "jit-projection i=" << sample;
  }
}

// The scan-filter fallback (relational_detail) was aligned with the canonical
// messages/semantics wherever the intent was identical (LIKE message, modulo
// by zero message, unary-minus overflow guard).  These cells assert the newly
// unified behaviour so it cannot drift back.
TEST(DifferentialTest,
     EvaluateDetailPath_CommonExpressions_AgreesWithCanonicalEvaluator) {
  const Schema& schema = IntSchema();
  const Row zero_division({Value(int64_t{5}), Value(int64_t{0})});
  const Expression modulo_zero = BinaryExpressionExp(
      ColumnValueExp("i"), BinaryOperation::kModulo, ColumnValueExp("j"));
  const Attempt ast_mod = EvaluateAst(modulo_zero, zero_division, schema);
  const Attempt detail_mod =
      EvaluateDetailPath(modulo_zero, zero_division, schema);
  EXPECT_EQ(ast_mod.kind, Attempt::Kind::kThrow);
  EXPECT_EQ(detail_mod.exception, ast_mod.exception);
  EXPECT_EQ(detail_mod.note, ast_mod.note) << "modulo-by-zero message";

  const Row min_row({Value(kInt64Min), Value()});
  const Expression negated =
      UnaryExpressionExp(ColumnValueExp("i"), UnaryOperation::kMinus);
  const Attempt ast_neg = EvaluateAst(negated, min_row, schema);
  const Attempt detail_neg = EvaluateDetailPath(negated, min_row, schema);
  EXPECT_EQ(ast_neg.kind, Attempt::Kind::kThrow);
  EXPECT_EQ(detail_neg.exception, ast_neg.exception);
  EXPECT_EQ(detail_neg.note, ast_neg.note) << "unary minus overflow";

  // CASE and date functions agree between the AST evaluator and the
  // relational_detail interpreter.
  const Row case_row({Value(int64_t{9}), Value(int64_t{0})});
  const Expression selected = CaseExpressionExp(
      {{BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kGreaterThan,
                            ConstantValueExp(Value(int64_t{3}))),
        ConstantValueExp(Value(int64_t{10}))}},
      ConstantValueExp(Value(int64_t{20})));
  EXPECT_TRUE(SameValue(EvaluateDetailPath(selected, case_row, schema).value,
                        Value(int64_t{10})));

  const Schema& dates = DateSchema();
  const Row date_row({Value::Date("1994-01-01"), Value::Date("1996-04-05")});
  const Expression add_year = FunctionCallExp(
      "date_add", {ColumnValueExp("d"), IntervalExpressionExp(1, "year")});
  const Attempt detail_date = EvaluateDetailPath(add_year, date_row, dates);
  ASSERT_EQ(detail_date.kind, Attempt::Kind::kValue);
  EXPECT_TRUE(SameValue(detail_date.value, Value::Date("1995-01-01")));

  const Expression extract =
      FunctionCallExp("extract_year", {ColumnValueExp("e")});
  const Attempt detail_extract = EvaluateDetailPath(extract, date_row, dates);
  ASSERT_EQ(detail_extract.kind, Attempt::Kind::kValue);
  EXPECT_TRUE(SameValue(detail_extract.value, Value(int64_t{1996})));
}

// Former XFAIL (improvement3.md A6-1), now resolved: the relational_detail
// interpreter used to collapse SQL three-valued logic to two-valued logic in
// three places (AND with a NULL left operand returned FALSE, OR with a NULL
// left operand ignored a TRUE right operand, NOT NULL returned TRUE).  Its
// Binary()/unary dispatch now forwards to the canonical AST evaluators, so
// the full AND/OR/NOT truth table including UNKNOWN must agree between the
// detail path and the canonical evaluator.
TEST(DifferentialTest, EvaluateDetailPath_ThreeValuedLogic_MatchesCanonical) {
  const Schema& schema = IntSchema();
  const std::vector<Row> rows{
      Row({Value(int64_t{1}), Value(int64_t{1})}),  // T T
      Row({Value(int64_t{1}), Value(int64_t{0})}),  // T F
      Row({Value(int64_t{0}), Value(int64_t{1})}),  // F T
      Row({Value(int64_t{0}), Value(int64_t{0})}),  // F F
      Row({Value(int64_t{1}), Value()}),            // T N
      Row({Value(), Value(int64_t{1})}),            // N T
      Row({Value(int64_t{0}), Value()}),            // F N
      Row({Value(), Value(int64_t{0})}),            // N F
      Row({Value(), Value()}),                      // N N
  };
  const Expression conjunction = BinaryExpressionExp(
      ColumnValueExp("i"), BinaryOperation::kAnd, ColumnValueExp("j"));
  const Expression disjunction = BinaryExpressionExp(
      ColumnValueExp("i"), BinaryOperation::kOr, ColumnValueExp("j"));
  const Expression negation =
      UnaryExpressionExp(ColumnValueExp("i"), UnaryOperation::kNot);
  for (size_t r = 0; r < rows.size(); ++r) {
    const std::string input = DescribeRow(schema, rows[r]);
    for (const auto& [name, expr] :
         {std::pair{"and", conjunction}, {"or", disjunction}}) {
      const Attempt ast = EvaluateAst(expr, rows[r], schema);
      const Attempt detail = EvaluateDetailPath(expr, rows[r], schema);
      EXPECT_EQ(detail.kind, ast.kind)
          << "[differential][MISMATCH] " << name << " [" << input << "]";
      if (detail.kind == ast.kind && ast.kind == Attempt::Kind::kValue) {
        EXPECT_TRUE(SameValue(detail.value, ast.value))
            << "[differential][MISMATCH] " << name << " [" << input
            << "] canonical=" << Describe(ast)
            << " detail=" << Describe(detail);
      }
    }
    const Attempt ast_not = EvaluateAst(negation, rows[r], schema);
    const Attempt detail_not = EvaluateDetailPath(negation, rows[r], schema);
    EXPECT_EQ(detail_not.kind, ast_not.kind)
        << "[differential][MISMATCH] not [" << input << "]";
    if (detail_not.kind == ast_not.kind &&
        ast_not.kind == Attempt::Kind::kValue) {
      EXPECT_TRUE(SameValue(detail_not.value, ast_not.value))
          << "[differential][MISMATCH] not [" << input
          << "] canonical=" << Describe(ast_not)
          << " detail=" << Describe(detail_not);
    }
  }
}

// Former XFAIL (A6 leftover), now resolved: the detail-path numeric core was
// forwarded to the canonical EvaluateBinary, so int/int division truncates to
// INT64 and add/sub/mul overflow plus INT64_MIN % -1 raise the canonical
// errors -- exactly like the AST evaluator.
TEST(DifferentialTest, EvaluateDetailPath_NumericEdgeCases_MatchesCanonical) {
  const Schema& schema = IntSchema();
  const Row seven_three({Value(int64_t{7}), Value(int64_t{3})});
  const Expression division = BinaryExpressionExp(
      ColumnValueExp("i"), BinaryOperation::kDivide, ColumnValueExp("j"));
  const Attempt detail_div = EvaluateDetailPath(division, seven_three, schema);
  const Attempt ast_div = EvaluateAst(division, seven_three, schema);
  EXPECT_EQ(detail_div.kind, Attempt::Kind::kValue);
  EXPECT_EQ(ast_div.kind, Attempt::Kind::kValue);
  EXPECT_EQ(detail_div.value.type, ValueType::kDouble);
  EXPECT_TRUE(SameValue(detail_div.value, ast_div.value))
      << "int/int division must produce double like the canonical evaluator";

  const Row max_one({Value(kInt64Max), Value(int64_t{1})});
  const Expression overflow_add = BinaryExpressionExp(
      ColumnValueExp("i"), BinaryOperation::kAdd, ColumnValueExp("j"));
  const Attempt detail_add = EvaluateDetailPath(overflow_add, max_one, schema);
  const Attempt ast_add = EvaluateAst(overflow_add, max_one, schema);
  EXPECT_EQ(detail_add.kind, Attempt::Kind::kThrow);
  EXPECT_EQ(ast_add.kind, Attempt::Kind::kThrow);
  EXPECT_EQ(detail_add.exception, ast_add.exception);
  EXPECT_EQ(detail_add.note, ast_add.note) << "add overflow message";

  const Row min_minus_one({Value(kInt64Min), Value(int64_t{-1})});
  const Expression extreme_modulo = BinaryExpressionExp(
      ColumnValueExp("i"), BinaryOperation::kModulo, ColumnValueExp("j"));
  const Attempt detail_extreme =
      EvaluateDetailPath(extreme_modulo, min_minus_one, schema);
  const Attempt ast_extreme =
      EvaluateAst(extreme_modulo, min_minus_one, schema);
  EXPECT_EQ(detail_extreme.kind, Attempt::Kind::kThrow);
  EXPECT_EQ(ast_extreme.kind, Attempt::Kind::kThrow);
  EXPECT_EQ(detail_extreme.exception, ast_extreme.exception);
  EXPECT_EQ(detail_extreme.note, ast_extreme.note) << "INT64_MIN % -1 message";
}

TEST(DifferentialTest, CheckedJitKernels_OverflowMatchesAstThrow) {
  // Fixed: the wrapping JIT kernels returned wrapped values where the AST
  // throws. The checked kernels must report overflow on the same inputs.
  const auto sum = JitInt64Kernels::CompileSumChecked();
  if (!sum.has_value()) {
    GTEST_SKIP() << "checked JIT sum kernel unavailable (LLVM disabled)";
  }
  {
    const int64_t inputs[] = {std::numeric_limits<int64_t>::max(), int64_t{1}};
    bool overflowed = false;
    (void)sum->SumChecked(inputs, 2, &overflowed);
    EXPECT_TRUE(overflowed);
  }
  {
    const int64_t inputs[] = {int64_t{1}, int64_t{2}};
    bool overflowed = true;
    (void)sum->SumChecked(inputs, 2, &overflowed);
    EXPECT_FALSE(overflowed);
  }
  const auto proj = JitInt64Kernels::CompileProjectionChecked();
  if (!proj.has_value()) {
    GTEST_SKIP() << "checked JIT projection kernel unavailable";
  }
  {
    const int64_t input = std::numeric_limits<int64_t>::max();
    int64_t output = 0;
    bool mul_of = false;
    bool add_of = false;
    proj->ProjectChecked(&input, &output, 1, int64_t{2}, int64_t{0}, &mul_of,
                         &add_of);
    EXPECT_TRUE(mul_of);
  }
}

TEST(DifferentialTest, EvaluateDetailPath_ConcatNull_MatchesCanonical) {
  const Schema schema("strings", {Column("s", ValueType::kVarChar)});
  const Row row({Value()});
  const Expression concat = FunctionCallExp(
      "concat", {ConstantValueExp(Value("a")), ColumnValueExp("s"),
                 ConstantValueExp(Value("b"))});
  const Attempt ast = EvaluateAst(concat, row, schema);
  const Attempt detail = EvaluateDetailPath(concat, row, schema);
  ASSERT_EQ(ast.kind, Attempt::Kind::kValue);
  ASSERT_EQ(detail.kind, Attempt::Kind::kValue);
  EXPECT_TRUE(ast.value.IsNull());
  EXPECT_TRUE(detail.value.IsNull());
}

}  // namespace tinylamb
