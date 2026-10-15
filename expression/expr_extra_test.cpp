/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// Semantic pins for the AST evaluator (the ground truth every other
// evaluation path mirrors): INT64 division promotes to double, modulo by
// zero fails rather than crashing, NULL propagates through arithmetic and
// three-valued comparisons, and string ordering follows byte order.
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>

#include "common/constants.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

const Schema& MixedSchema() {
  static const Schema schema(
      "mixed", {Column("i", ValueType::kInt64), Column("j", ValueType::kInt64),
                Column("s", ValueType::kVarChar)});
  return schema;
}

Value Eval(const Expression& expression) {
  return expression->Evaluate(
      Row({Value(int64_t{7}), Value(int64_t{2}), Value("abc")}), MixedSchema());
}

}  // namespace

TEST(ExprExtraTest, Int64DividePromotesToDouble) {
  // INT64 `/` follows GoogleSQL semantics: exact division into FLOAT64, so
  // truncating integer division never hides fractional results.
  Expression div = BinaryExpressionExp(
      ColumnValueExp("i"), BinaryOperation::kDivide, ColumnValueExp("j"));
  const Value result = Eval(div);
  EXPECT_EQ(result.type, ValueType::kDouble);
  EXPECT_DOUBLE_EQ(result.value.double_value, 3.5);
}

TEST(ExprExtraTest, Int64ModuloStaysIntegralAndNullPropagates) {
  Expression mod = BinaryExpressionExp(
      ColumnValueExp("i"), BinaryOperation::kModulo, ColumnValueExp("j"));
  const Value result = Eval(mod);
  EXPECT_EQ(result.type, ValueType::kInt64);
  EXPECT_EQ(result.value.int_value, 1);

  // NULL on either side yields NULL, never zero.
  Expression null_side = BinaryExpressionExp(
      ConstantValueExp(Value()), BinaryOperation::kModulo, ColumnValueExp("j"));
  EXPECT_TRUE(Eval(null_side).IsNull());
}

TEST(ExprExtraTest, ModuloByZeroFailsWithoutCrashing) {
  Expression by_zero =
      BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kModulo,
                          ConstantValueExp(Value(int64_t{0})));
  EXPECT_THROW(Eval(by_zero), std::exception);
}

TEST(ExprExtraTest, ThreeValuedComparisonKeepsNullUnknown) {
  // NULL = NULL is UNKNOWN (NULL), not TRUE, and NOT UNKNOWN stays UNKNOWN.
  Expression null_equals_null =
      BinaryExpressionExp(ConstantValueExp(Value()), BinaryOperation::kEquals,
                          ConstantValueExp(Value()));
  EXPECT_TRUE(Eval(null_equals_null).IsNull());

  const Value compared = Eval(
      BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(int64_t{0}))));
  // BOOL folds into kInt64 (0/1) in this engine.
  EXPECT_EQ(compared.type, ValueType::kInt64);
  EXPECT_EQ(compared.value.int_value, 1);
}

TEST(ExprExtraTest, SignedOverflowIsDetectedNotWrapped) {
  // INT64 + INT64 stays INT64 and the evaluator rejects two's-complement
  // overflow with an error instead of silently wrapping.
  Expression wrap = BinaryExpressionExp(
      ConstantValueExp(Value(std::numeric_limits<int64_t>::max())),
      BinaryOperation::kAdd, ConstantValueExp(Value(int64_t{1})));
  EXPECT_THROW(Eval(wrap), std::exception);
  // The extreme in-range sum still evaluates exactly.
  Expression exact = BinaryExpressionExp(
      ConstantValueExp(Value(std::numeric_limits<int64_t>::max())),
      BinaryOperation::kAdd,
      ConstantValueExp(Value(std::numeric_limits<int64_t>::min())));
  EXPECT_EQ(Eval(exact).value.int_value, -1);
}

TEST(ExprExtraTest, VarcharComparisonFollowsByteOrder) {
  Expression less_than =
      BinaryExpressionExp(ColumnValueExp("s"), BinaryOperation::kLessThan,
                          ConstantValueExp(Value("abd")));
  const Value result = Eval(less_than);
  EXPECT_EQ(result.type, ValueType::kInt64);
  EXPECT_EQ(result.value.int_value, 1);

  Expression against_null =
      BinaryExpressionExp(ColumnValueExp("s"), BinaryOperation::kLessThan,
                          ConstantValueExp(Value()));
  EXPECT_TRUE(Eval(against_null).IsNull());
}

}  // namespace tinylamb
