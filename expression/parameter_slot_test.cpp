/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

// ParameterSlot / SlotizeLiterals contracts. The slot index is a POSITION in
// the SQL text; every traversal that numbers slots must sequence its sibling
// visits explicitly. The ordering tests here fail on any compiler that
// evaluates function arguments right-to-left (GCC) if the sequencing is ever
// dropped from SlotizeLiterals.

#include "expression/parameter_slot.hpp"

#include <cstdint>
#include <utility>
#include <vector>

#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/unary_expression.hpp"
#include "gtest/gtest.h"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

// Slotizes `expression` against `parameters` and returns the slot index each
// bindable literal received, in the order the slots were installed.
std::vector<size_t> SlotIndexes(const Expression& expression,
                                const std::vector<Value>& parameters) {
  size_t cursor = 0;
  bool ok = true;
  Expression slotted =
      SlotizeLiterals(expression, &cursor, &ok, &parameters, nullptr);
  EXPECT_TRUE(ok);
  std::vector<Expression> slots;
  AppendParameterSlots(slotted, &slots);
  std::vector<size_t> result;
  result.reserve(slots.size());
  for (const Expression& slot : slots) {
    result.push_back(dynamic_cast<const ParameterSlot&>(*slot).Index());
  }
  return result;
}

// The slot that replaced the literal inside the comparison leaf `leaf`
// (a `column = literal` binary).
size_t LeafSlotIndex(const Expression& leaf) {
  EXPECT_EQ(leaf->Type(), TypeTag::kBinaryExp);
  const Expression& rhs = leaf->AsBinaryExpression().Right();
  const auto* slot = dynamic_cast<const ParameterSlot*>(rhs.get());
  EXPECT_NE(slot, nullptr);
  return slot == nullptr ? static_cast<size_t>(-1) : slot->Index();
}

}  // namespace

TEST(ParameterSlotTest, BinarySlotsNumberInSqlTextOrder) {
  // WHERE d_w_id = 7 AND d_id = 7: equal literals must still number left to
  // right. Right-to-left argument evaluation numbered d_id first and swapped
  // every later replay binding.
  Expression where = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("d_w_id"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(7))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp("d_id"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(7))));

  // Fill-time parameters equal the statement literals (7, 7); replay will
  // substitute different values through the same slots.
  const std::vector<Value> parameters = {Value(7), Value(7)};
  size_t cursor = 0;
  bool ok = true;
  Expression slotted =
      SlotizeLiterals(where, &cursor, &ok, &parameters, nullptr);
  ASSERT_TRUE(ok);
  ASSERT_EQ(cursor, parameters.size());
  // d_w_id (left, first literal) is slot 0; d_id (right) is slot 1.
  EXPECT_EQ(LeafSlotIndex(slotted->AsBinaryExpression().Left()), 0U);
  EXPECT_EQ(LeafSlotIndex(slotted->AsBinaryExpression().Right()), 1U);
}

TEST(ParameterSlotTest, UnequalValuesDoNotMaskTraversalOrder) {
  Expression where = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(1))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp("b"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(2))));
  const std::vector<size_t> indexes =
      SlotIndexes(where, std::vector<Value>{Value(1), Value(2)});
  ASSERT_EQ(indexes.size(), 2U);
  EXPECT_EQ(indexes[0], 0U);
  EXPECT_EQ(indexes[1], 1U);
}

TEST(ParameterSlotTest, CaseNumbersConditionBeforeResultBeforeElse) {
  // CASE WHEN c1 THEN 1 WHEN c2 THEN 2 ELSE 3: the WHEN condition numbers
  // before its result; the ELSE clause trails both branches.
  Expression case_exp = CaseExpressionExp(
      std::vector<std::pair<Expression, Expression>>{
          {BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                               ConstantValueExp(Value(0))),
           ConstantValueExp(Value(1))},
          {BinaryExpressionExp(ColumnValueExp("c2"), BinaryOperation::kEquals,
                               ConstantValueExp(Value(0))),
           ConstantValueExp(Value(2))}},
      ConstantValueExp(Value(3)));

  const std::vector<Value> parameters = {Value(0), Value(1), Value(0), Value(2),
                                         Value(3)};
  size_t cursor = 0;
  bool ok = true;
  Expression slotted =
      SlotizeLiterals(case_exp, &cursor, &ok, &parameters, nullptr);
  ASSERT_TRUE(ok);
  ASSERT_EQ(cursor, 5U);

  size_t position = 0;
  for (const auto& clause : slotted->AsCaseExpression().when_clauses_) {
    // The WHEN condition is a `column = literal` comparison; its slot sits
    // on the right-hand side. The THEN result is the slot itself.
    const auto* condition = dynamic_cast<const ParameterSlot*>(
        clause.first->AsBinaryExpression().Right().get());
    const auto* result =
        dynamic_cast<const ParameterSlot*>(clause.second.get());
    ASSERT_NE(condition, nullptr);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(condition->Index(), position);
    EXPECT_EQ(result->Index(), position + 1);
    position += 2;
  }
  const auto* else_slot = dynamic_cast<const ParameterSlot*>(
      slotted->AsCaseExpression().else_clause_.get());
  ASSERT_NE(else_slot, nullptr);
  EXPECT_EQ(else_slot->Index(), 4U);
}

TEST(ParameterSlotTest, InNumbersChildBeforeListItems) {
  // x IN (1, 2): the child literal binds before the list items.
  Expression in = InExpressionExp(
      BinaryExpressionExp(ColumnValueExp("x"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(9))),
      std::vector<Expression>{ConstantValueExp(Value(1)),
                              ConstantValueExp(Value(2))});
  const std::vector<Value> parameters = {Value(9), Value(1), Value(2)};
  size_t cursor = 0;
  bool ok = true;
  Expression slotted = SlotizeLiterals(in, &cursor, &ok, &parameters, nullptr);
  ASSERT_TRUE(ok);
  ASSERT_EQ(cursor, 3U);
  std::vector<Expression> numbered;
  AppendParameterSlots(slotted, &numbered);
  ASSERT_EQ(numbered.size(), 3U);
  EXPECT_EQ(dynamic_cast<const ParameterSlot&>(*numbered[0]).Index(), 0U);
  EXPECT_EQ(dynamic_cast<const ParameterSlot&>(*numbered[1]).Index(), 1U);
  EXPECT_EQ(dynamic_cast<const ParameterSlot&>(*numbered[2]).Index(), 2U);
}

TEST(ParameterSlotTest, SamePrintedExpressionKeepsEqualValueSlotsDistinct) {
  Expression where = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("d_w_id"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(7))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp("d_id"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(7))));
  const std::vector<Value> parameters = {Value(7), Value(7)};
  size_t cursor = 0;
  bool ok = true;
  Expression slotted =
      SlotizeLiterals(where, &cursor, &ok, &parameters, nullptr);
  ASSERT_TRUE(ok);
  const Expression& left = slotted->AsBinaryExpression().Left();
  const Expression& right = slotted->AsBinaryExpression().Right();
  // Same printed value, different slots: a rewrite must never merge them.
  EXPECT_TRUE(SamePrintedExpression(left->AsBinaryExpression().Right(),
                                    left->AsBinaryExpression().Right()));
  EXPECT_FALSE(SamePrintedExpression(left->AsBinaryExpression().Right(),
                                     right->AsBinaryExpression().Right()));
}

TEST(ParameterSlotTest, NonBindableAndNullLiteralsStayUntouched) {
  // NULL and non-bindable shapes must not consume parameter positions.
  Expression where = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("x"), BinaryOperation::kEquals,
                          ConstantValueExp(Value())),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp("y"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(4))));
  const std::vector<Value> parameters = {Value(4)};
  size_t cursor = 0;
  bool ok = true;
  Expression slotted =
      SlotizeLiterals(where, &cursor, &ok, &parameters, nullptr);
  ASSERT_TRUE(ok);
  ASSERT_EQ(cursor, 1U);
  const Expression& right = slotted->AsBinaryExpression().Right();
  EXPECT_NE(dynamic_cast<const ParameterSlot*>(
                right->AsBinaryExpression().Right().get()),
            nullptr);
  EXPECT_FALSE(IsBindableLiteral(Value()));
}

TEST(ParameterSlotTest, SlotEvaluateReadsPreparedValuesAndRebind) {
  auto values = std::make_shared<PreparedValues>(
      std::vector<Value>{Value(11), Value(22)});
  ParameterSlot insert_slot(1, ValueType::kInt64, values);
  const Schema schema("t", {Column("a", ValueType::kInt64)});
  Row row({Value(0)});
  EXPECT_EQ(insert_slot.Evaluate(row, schema), Value(22));

  // Optimizer-resident slot: starts at its initial value, then Bind() moves it.
  ParameterSlot bound_slot(0, Value(5));
  EXPECT_EQ(bound_slot.Evaluate(row, schema), Value(5));
  bound_slot.Bind(Value(6));
  EXPECT_EQ(bound_slot.Evaluate(row, schema), Value(6));

  // Rebind preserves the index and expected type.
  Expression rebound = bound_slot.Rebind(values);
  const auto* rebound_slot = dynamic_cast<const ParameterSlot*>(rebound.get());
  ASSERT_NE(rebound_slot, nullptr);
  EXPECT_EQ(rebound_slot->Index(), 0U);
  EXPECT_EQ(rebound_slot->Evaluate(row, schema), Value(11));
}

TEST(ParameterSlotTest, ContainsParameterSlotWalksAllContainers) {
  EXPECT_FALSE(ContainsParameterSlot(Expression{}));
  EXPECT_FALSE(ContainsParameterSlot(ConstantValueExp(Value(1))));
  EXPECT_FALSE(ContainsParameterSlot(
      BinaryExpressionExp(ColumnValueExp("x"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(1)))));
  EXPECT_TRUE(ContainsParameterSlot(
      BinaryExpressionExp(ColumnValueExp("x"), BinaryOperation::kEquals,
                          std::make_shared<ParameterSlot>(0, Value(1)))));
  EXPECT_TRUE(ContainsParameterSlot(InExpressionExp(
      ColumnValueExp("x"),
      std::vector<Expression>{std::make_shared<ParameterSlot>(0, Value(1))})));
  EXPECT_TRUE(ContainsParameterSlot(UnaryExpressionExp(
      std::make_shared<ParameterSlot>(0, Value(1)), UnaryOperation::kIsNull)));
}

TEST(ParameterSlotTest, SlotizeRejectsParameterValueMismatch) {
  // The expected parameters must equal the literals in traversal order; a
  // mismatch clears `ok` and the caller falls back to the legacy path.
  Expression where =
      BinaryExpressionExp(ColumnValueExp("x"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(1)));
  std::vector<Value> parameters = {Value(2)};
  size_t cursor = 0;
  bool ok = true;
  SlotizeLiterals(where, &cursor, &ok, &parameters, nullptr);
  EXPECT_FALSE(ok);
}

}  // namespace tinylamb
