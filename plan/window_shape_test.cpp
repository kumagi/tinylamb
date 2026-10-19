/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
// The plan half of the window operator's two memory shapes
// (plan/window_plan.hpp): which shape a row estimate selects, and the ordering
// contract that differs between them.
//
// What the plan layer owns:
//   * the memory policy (budget -> row capacity) and the estimate-driven
//     choice between the buffered and the spill-capable shape;
//   * the ordering claim: a buffered window still appends columns to the rows
//     its child produced, so it keeps the child's order, while a spill-capable
//     window emits one spill file at a time and must claim nothing -- the claim
//     is decided before the runtime row count is known, so a mis-estimate can
//     only be conservative;
//   * the condition that makes a spill split safe at all: every call has to
//     partition by the same expression list, because a row's spill file is
//     chosen from that one key.
//
// The operator half (executor/window_spill_test.cpp) drives what the executor
// does with the policy it was handed.
#include <gtest/gtest.h>

#include <cmath>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "plan/sort_plan.hpp"
#include "plan/values_plan.hpp"
#include "plan/window_plan.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

Schema TwoColumnSchema() {
  return Schema(
      "t", {Column("g", ValueType::kInt64), Column("k", ValueType::kInt64)});
}

// ROW_NUMBER() OVER (PARTITION BY <key> [ORDER BY k]).
std::shared_ptr<WindowFunctionCallExpression> RowNumber(
    const std::string& partition_column, bool with_order) {
  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = "ROW_NUMBER";
  window->partition_by = {ColumnValueExp(ColumnName(partition_column))};
  if (with_order) {
    WindowOrderTerm term;
    term.expression = ColumnValueExp(ColumnName("k"));
    window->order_by = {term};
  }
  return window;
}

NamedExpression Output(const std::string& name,
                       std::shared_ptr<WindowFunctionCallExpression> window) {
  return NamedExpression(name, Expression(std::move(window)));
}

Plan OrderedLeaf() {
  Plan leaf = std::make_shared<ValuesPlan>(
      TwoColumnSchema(),
      std::vector<Row>{Row({Value(int64_t{1}), Value(int64_t{2})})});
  SortKey key;
  key.expression = ColumnValueExp(ColumnName("k"));
  return std::make_shared<SortPlan>(std::move(leaf), std::vector<SortKey>{key});
}

WindowMemoryPolicy SpillingPolicy() {
  WindowMemoryPolicy policy;
  policy.memory_budget_bytes = kWindowDefaultMemoryBudgetBytes;
  return policy;
}

}  // namespace

TEST(WindowShapeTest, PolicyPicksTheShapeFromTheRowEstimate) {
  const size_t capacity = WindowRowCapacity(SpillingPolicy());
  EXPECT_EQ(capacity,
            kWindowDefaultMemoryBudgetBytes / kWindowEstimatedRowBytes);
  // An unbounded policy is "buffered only" and can never be outgrown.
  EXPECT_EQ(WindowRowCapacity(WindowMemoryPolicy{}),
            std::numeric_limits<size_t>::max());
  // At the capacity the input still fits, so the buffered shape (which keeps
  // the child's order) survives; one row past it hands the node a budget.
  EXPECT_EQ(
      WindowPolicyForRows(static_cast<double>(capacity)).memory_budget_bytes,
      0U);
  EXPECT_NE(WindowPolicyForRows(static_cast<double>(capacity) + 1)
                .memory_budget_bytes,
            0U);
  // A NaN (degenerate) estimate must not turn the spilling shape on by
  // accident; an infinite estimate means "too big to fit" and spills.
  EXPECT_NE(WindowPolicyForRows(std::numeric_limits<double>::infinity())
                .memory_budget_bytes,
            0U);
  EXPECT_EQ(WindowPolicyForRows(std::numeric_limits<double>::quiet_NaN())
                .memory_budget_bytes,
            0U);
}

TEST(WindowShapeTest, OnlyTheBufferedShapeKeepsTheChildOrderClaim) {
  const std::vector<Expression> required = {ColumnValueExp(ColumnName("k"))};
  const std::vector<bool> ascending = {true};
  std::vector<NamedExpression> outputs = {
      Output("$win0", RowNumber("g", /*with_order=*/true))};

  const auto buffered = std::make_shared<WindowPlan>(OrderedLeaf(), outputs);
  EXPECT_FALSE(buffered->SpillCapable());
  EXPECT_TRUE(buffered->IsOrderedBy(required, ascending));

  const auto spilling =
      std::make_shared<WindowPlan>(OrderedLeaf(), outputs, SpillingPolicy());
  EXPECT_TRUE(spilling->SpillCapable());
  // The spill shape emits one file at a time, so it cannot promise the child's
  // order.
  EXPECT_FALSE(spilling->IsOrderedBy(required, ascending));
  // A trivial requirement is still satisfied (nothing to deliver).
  EXPECT_TRUE(spilling->IsOrderedBy({}, {}));

  std::ostringstream buffered_dump;
  buffered->Dump(buffered_dump, 0);
  EXPECT_EQ(buffered_dump.str().find("spill"), std::string::npos)
      << buffered_dump.str();
  std::ostringstream spilling_dump;
  spilling->Dump(spilling_dump, 0);
  EXPECT_NE(spilling_dump.str().find("spill: partitions="), std::string::npos)
      << spilling_dump.str();
}

TEST(WindowShapeTest, SpillNeedsOnePartitionKeySharedByEveryCall) {
  std::vector<NamedExpression> same_key = {
      Output("$win0", RowNumber("g", /*with_order=*/true)),
      Output("$win1", RowNumber("g", /*with_order=*/false))};
  const auto shareable =
      std::make_shared<WindowPlan>(OrderedLeaf(), same_key, SpillingPolicy());
  EXPECT_TRUE(shareable->CallsShareOnePartitionKey());
  EXPECT_TRUE(shareable->SpillCapable());

  // Different PARTITION BY lists: a spill file chosen from one key could cut
  // the other call's partition in half, so the shape stays buffered.
  std::vector<NamedExpression> different_keys = {
      Output("$win0", RowNumber("g", /*with_order=*/true)),
      Output("$win1", RowNumber("k", /*with_order=*/false))};
  const auto unshareable = std::make_shared<WindowPlan>(
      OrderedLeaf(), different_keys, SpillingPolicy());
  EXPECT_FALSE(unshareable->CallsShareOnePartitionKey());
  EXPECT_FALSE(unshareable->SpillCapable());
  // No spill means the child's order survives even under a finite budget.
  EXPECT_TRUE(
      unshareable->IsOrderedBy({ColumnValueExp(ColumnName("k"))}, {true}));

  // No call at all is also not spillable: there is nothing to group.
  const auto empty = std::make_shared<WindowPlan>(
      OrderedLeaf(), std::vector<NamedExpression>{}, SpillingPolicy());
  EXPECT_FALSE(empty->CallsShareOnePartitionKey());
  EXPECT_FALSE(empty->SpillCapable());
}

}  // namespace tinylamb
