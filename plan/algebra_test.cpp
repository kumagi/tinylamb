/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/algebra.hpp"

#include <iostream>

#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/rewrite.hpp"
#include "gtest/gtest.h"

namespace tinylamb {

namespace {

Expression ColEqConst(const std::string& table, const std::string& column,
                      const Value& value) {
  return BinaryExpressionExp(ColumnValueExp(ColumnName(table, column)),
                             BinaryOperation::kEquals, ConstantValueExp(value));
}

Expression ColEqCol(const std::string& left_table, const std::string& left,
                    const std::string& right_table, const std::string& right) {
  return BinaryExpressionExp(ColumnValueExp(ColumnName(left_table, left)),
                             BinaryOperation::kEquals,
                             ColumnValueExp(ColumnName(right_table, right)));
}

}  // namespace

TEST(AlgebraTest, BuildAndPrintTree) {
  AlgebraTree tree = AlgebraNode::Filter(
      AlgebraNode::Join(AlgebraNode::Scan("a"), AlgebraNode::Scan("b"),
                        ConstantValueExp(Value(true))),
      CombineConjuncts(
          {ColEqConst("a", "x", Value(1)), ColEqCol("a", "x", "b", "y")}));
  EXPECT_FALSE(tree->ToString().empty());
  std::cout << tree->ToString() << "\n";
}

TEST(AlgebraTest, AbsorbScanFoldsFilterIntoScan) {
  AlgebraTree tree = AlgebraNode::Filter(AlgebraNode::Scan("a"),
                                         ColEqConst("a", "x", Value(1)));
  AlgebraTree rewritten = RewriteAlgebra(tree, AlgebraRuleSet::Default());
  EXPECT_EQ(rewritten->kind, AlgebraNode::Kind::kScan);
  EXPECT_EQ(rewritten->predicates.size(), 1U);
}

TEST(AlgebraTest, TrueFilterIsEliminated) {
  AlgebraTree tree = AlgebraNode::Filter(AlgebraNode::Scan("a"),
                                         ConstantValueExp(Value(true)));
  AlgebraTree rewritten = RewriteAlgebra(tree, AlgebraRuleSet::Default());
  EXPECT_EQ(rewritten->kind, AlgebraNode::Kind::kScan);
  EXPECT_TRUE(rewritten->predicates.empty());
}

TEST(AlgebraTest, PushdownMovesSingleSidePredicatesBelowJoin) {
  AlgebraTree join =
      AlgebraNode::Join(AlgebraNode::Scan("a"), AlgebraNode::Scan("b"),
                        ColEqCol("a", "x", "b", "y"));
  // The unqualified conjunct cannot be assigned to a side and stays above.
  Expression unqualified =
      BinaryExpressionExp(ColumnValueExp("z"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(3)));
  AlgebraTree filter = AlgebraNode::Filter(
      join, CombineConjuncts({ColEqConst("a", "x", Value(1)),
                              ColEqConst("b", "y", Value(2)), unqualified}));
  AlgebraTree rewritten = RewriteAlgebra(filter, AlgebraRuleSet::Default());
  // Both single-side predicates descend below the join and the absorb rule
  // folds them into the scan payloads; only the unqualified conjunct remains
  // at the root.
  EXPECT_EQ(rewritten->kind, AlgebraNode::Kind::kFilter);
  const AlgebraTree& inner_join = rewritten->children.front();
  EXPECT_EQ(inner_join->kind, AlgebraNode::Kind::kJoin);
  EXPECT_EQ(inner_join->children[0]->kind, AlgebraNode::Kind::kScan);
  EXPECT_EQ(inner_join->children[0]->predicates.size(), 1U);
  EXPECT_EQ(inner_join->children[1]->kind, AlgebraNode::Kind::kScan);
  EXPECT_EQ(inner_join->children[1]->predicates.size(), 1U);
}

TEST(AlgebraTest, LoweringSplitsScanPayloadFromResidual) {
  AlgebraTree join =
      AlgebraNode::Join(AlgebraNode::Scan("a"), AlgebraNode::Scan("b"),
                        ColEqCol("a", "x", "b", "y"));
  AlgebraTree filter = AlgebraNode::Filter(
      join, CombineConjuncts({ColEqConst("a", "x", Value(1))}));
  AlgebraTree rewritten = RewriteAlgebra(filter, AlgebraRuleSet::Default());
  const AlgebraLowering lowered = LowerAlgebra(rewritten);
  ASSERT_TRUE(lowered.scan_predicates.contains("a"));
  EXPECT_EQ(lowered.scan_predicates.at("a")->ToString(),
            ColEqConst("a", "x", Value(1))->ToString());
  EXPECT_FALSE(lowered.scan_predicates.contains("b"));
  ASSERT_EQ(lowered.join_predicates.size(), 1U);
  EXPECT_EQ(lowered.residual_predicates.size(), 1U);  // join condition
}

TEST(AlgebraTest, RuleCanBeRemoved) {
  AlgebraRuleSet rules = AlgebraRuleSet::Default();
  ASSERT_TRUE(rules.Remove("filter_absorb_scan"));
  EXPECT_FALSE(rules.Contains("filter_absorb_scan"));
  EXPECT_TRUE(rules.Contains("filter_conjoin"));
}

}  // namespace tinylamb
