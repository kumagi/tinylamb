/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Direct unit tests for the plan-cache expression plumbing in
// query/plan_cache.hpp: ParameterSlot evaluation metadata and rebinding,
// the bindable-literal classifier, and the CloneWithPreparedValues tree
// walker across every expression node kind.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/unary_expression.hpp"
#include "query/plan_cache.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

std::shared_ptr<const PreparedValues> Vals(std::vector<Value> values) {
  return std::make_shared<const PreparedValues>(std::move(values));
}

}  // namespace

TEST(ParameterSlotTest, EvaluateReturnsSlotValue) {
  auto values = Vals({Value(int64_t{7}), Value(std::string("s"))});
  const ParameterSlot slot(1, ValueType::kVarChar, values);

  Row row({});
  Schema schema("t", {Column("a", ValueType::kInt64)});
  EXPECT_EQ(slot.Type(), TypeTag::kConstantValue);
  EXPECT_EQ(slot.Index(), 1U);
  EXPECT_EQ(slot.Evaluate(row, schema), Value(std::string("s")));
  EXPECT_EQ(slot.Evaluate(&row, schema, nullptr, schema),
            Value(std::string("s")));
  EXPECT_EQ(slot.ResultType(schema).GetType(), TypeTag::kVarChar);
  EXPECT_EQ(slot.ResultType(schema, schema).GetType(), TypeTag::kVarChar);
  EXPECT_TRUE(slot.TouchedColumns().empty());
  EXPECT_EQ(slot.ToString(), "<?1>");
  std::ostringstream dump;
  slot.Dump(dump);
  EXPECT_EQ(dump.str(), "<?1>");
}

TEST(ParameterSlotTest, ResultTypeCoversAllValueTypes) {
  auto values = Vals({});
  const std::vector<std::pair<ValueType, TypeTag>> cases = {
      {ValueType::kInt64, TypeTag::kBigInt},
      {ValueType::kDouble, TypeTag::kDouble},
      {ValueType::kVarChar, TypeTag::kVarChar},
      {ValueType::kDate, TypeTag::kDate},
      {ValueType::kArray, TypeTag::kArray},
      {ValueType::kNull, TypeTag::kInvalid},
  };
  for (const auto& [value_type, tag] : cases) {
    const ParameterSlot slot(0, value_type, values);
    SCOPED_TRACE(static_cast<int>(value_type));
    EXPECT_EQ(slot.ResultType(Schema("t", {})).GetType(), tag);
    EXPECT_EQ(slot.ResultType(Schema("t", {}), Schema("t", {})).GetType(), tag);
  }
}

TEST(ParameterSlotTest, BindUpdatesSniffedValue) {
  ParameterSlot slot(0, Value(int64_t{1}));
  Row row({});
  EXPECT_EQ(slot.Evaluate(row, Schema("t", {})), Value(int64_t{1}));
  EXPECT_EQ(slot.AsConstantValue().GetValue(), Value(int64_t{1}));
  slot.Bind(Value(int64_t{9}));
  EXPECT_EQ(slot.Evaluate(row, Schema("t", {})), Value(int64_t{9}));
  EXPECT_EQ(slot.AsConstantValue().GetValue(), Value(int64_t{9}));
  EXPECT_EQ(slot.ToString(), "9");
}

TEST(SelectReuseThresholdTest, RetiresOnlyExtremeRegressions) {
  EXPECT_FALSE(SelectReuseExceededThreshold(1'000'000, 0));
  EXPECT_FALSE(SelectReuseExceededThreshold(10'000'000, 1'000'000));
  EXPECT_FALSE(SelectReuseExceededThreshold(40'000'000, 5'000'000));
  EXPECT_TRUE(SelectReuseExceededThreshold(80'000'000, 5'000'000));
  EXPECT_FALSE(
      SelectReuseExceededThreshold(6'000'000'000ULL, 5'000'000'000ULL));
}

TEST(ParameterSlotTest, RebindPointsAtNewValues) {
  auto first = Vals({Value(int64_t{1})});
  auto second = Vals({Value(int64_t{2})});
  const ParameterSlot slot(0, ValueType::kInt64, first);
  Expression rebound = slot.Rebind(second);
  ASSERT_EQ(rebound->Type(), TypeTag::kConstantValue);
  Row row({});
  EXPECT_EQ(rebound->Evaluate(row, Schema("t", {})), Value(int64_t{2}));
}

TEST(ParameterSlotTest, IsBindableLiteralClassifiesValueTypes) {
  EXPECT_TRUE(IsBindableLiteral(Value(int64_t{1})));
  EXPECT_TRUE(IsBindableLiteral(Value(1.5)));
  EXPECT_TRUE(IsBindableLiteral(Value(std::string("s"))));
  EXPECT_TRUE(IsBindableLiteral(Value::Date("2026-01-02")));
  EXPECT_FALSE(IsBindableLiteral(Value()));
  EXPECT_FALSE(IsBindableLiteral(Value::Array({Value(int64_t{1})}, "INT64")));
}

TEST(ParameterSlotTest, ContainsParameterSlotWalksEveryNodeKind) {
  auto values = Vals({Value(int64_t{1})});
  Schema schema("t", {Column("a", ValueType::kInt64)});

  Expression plain_slot =
      std::make_shared<ParameterSlot>(0, ValueType::kInt64, values);
  EXPECT_TRUE(ContainsParameterSlot(plain_slot));
  EXPECT_FALSE(ContainsParameterSlot(Expression{}));
  EXPECT_FALSE(ContainsParameterSlot(ConstantValueExp(Value(int64_t{1}))));
  EXPECT_FALSE(ContainsParameterSlot(ColumnValueExp("a")));

  const auto slot = [] {
    return std::make_shared<ParameterSlot>(0, ValueType::kInt64,
                                           Vals({Value(int64_t{1})}));
  };

  EXPECT_TRUE(ContainsParameterSlot(
      BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kAdd, slot())));
  EXPECT_TRUE(
      ContainsParameterSlot(UnaryExpressionExp(slot(), UnaryOperation::kNot)));
  EXPECT_TRUE(ContainsParameterSlot(
      UnaryExpressionExp(slot(), UnaryOperation::kIsNull)));
  EXPECT_TRUE(ContainsParameterSlot(
      AggregateExpressionExp(AggregationType::kSum, slot(), false)));
  EXPECT_TRUE(ContainsParameterSlot(CaseExpressionExp(
      {{slot(), ConstantValueExp(Value(int64_t{1}))}}, ColumnValueExp("a"))));
  EXPECT_TRUE(ContainsParameterSlot(
      InExpressionExp(slot(), {ConstantValueExp(Value(int64_t{1}))})));
  EXPECT_TRUE(ContainsParameterSlot(
      FunctionCallExp("concat", {ColumnValueExp("a"), slot()})));
  // NOTE: the walker does not descend into cast nodes (plan-cache INSERT
  // artifacts never contain them today); pinned so a widening of the walker
  // is a conscious change.
  EXPECT_FALSE(
      ContainsParameterSlot(CastExpressionExp(slot(), "INT64", false)));
  // Nested two levels deep.
  EXPECT_TRUE(ContainsParameterSlot(
      BinaryExpressionExp(UnaryExpressionExp(slot(), UnaryOperation::kNot),
                          BinaryOperation::kAdd, ColumnValueExp("a"))));
}

TEST(ParameterSlotTest, CloneWithPreparedValuesRewritesSlotsInPlace) {
  auto values = Vals({Value(int64_t{42})});
  Schema schema("t", {Column("a", ValueType::kInt64)});
  Row row({Value(int64_t{1})});

  const auto slot = [&values] {
    return std::make_shared<ParameterSlot>(0, ValueType::kInt64, values);
  };

  Expression binary =
      BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kAdd, slot());
  Expression cloned = CloneWithPreparedValues(binary, values);
  EXPECT_EQ(cloned->Evaluate(row, schema), Value(int64_t{43}));

  Expression unary = UnaryExpressionExp(slot(), UnaryOperation::kNot);
  EXPECT_EQ(CloneWithPreparedValues(unary, values)->Evaluate(row, schema),
            Value(int64_t{0}));

  Expression aggregate =
      AggregateExpressionExp(AggregationType::kSum, slot(), false);
  EXPECT_NO_THROW((void)CloneWithPreparedValues(aggregate, values));

  Expression searched = CaseExpressionExp(
      {{slot(), ConstantValueExp(Value(int64_t{9}))}}, slot());
  Expression cloned_case = CloneWithPreparedValues(searched, values);
  EXPECT_EQ(cloned_case->Evaluate(row, schema), Value(int64_t{9}));

  Expression in_list =
      InExpressionExp(slot(), {slot(), ConstantValueExp(Value(int64_t{5}))});
  EXPECT_NO_THROW((void)CloneWithPreparedValues(in_list, values));

  // Trees without slots are returned untouched.
  Expression plain = ConstantValueExp(Value(int64_t{3}));
  EXPECT_EQ(CloneWithPreparedValues(plain, values), plain);
}

}  // namespace tinylamb
