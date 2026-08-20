/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/rewrite.hpp"

#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/unary_expression.hpp"
#include "gtest/gtest.h"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

TEST(ExpressionRewriteTest, DefaultRulesFoldAndNormalizeRecursively) {
  Expression expression = BinaryExpressionExp(
      ConstantValueExp(Value(10)), BinaryOperation::kLessThan,
      BinaryExpressionExp(ConstantValueExp(Value(1)), BinaryOperation::kAdd,
                          ConstantValueExp(Value(2))));

  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);

  ASSERT_EQ(rewritten->Type(), TypeTag::kConstantValue);
  EXPECT_FALSE(rewritten->AsConstantValue().GetValue().Truthy());
}

TEST(ExpressionRewriteTest, RuleCanBeRemovedWithoutChangingOtherRules) {
  ExpressionRuleSet rules = ExpressionRuleSet::Default();
  ASSERT_TRUE(rules.Remove("fold_binary"));
  EXPECT_TRUE(rules.Contains("canonicalize_comparison"));
  Expression expression =
      BinaryExpressionExp(ConstantValueExp(Value(10)),
                          BinaryOperation::kLessThan, ColumnValueExp("value"));

  Expression rewritten =
      ExpressionRewriter(std::move(rules)).Rewrite(expression);

  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(),
            BinaryOperation::kGreaterThan);
  EXPECT_EQ(rewritten->AsBinaryExpression().Left()->Type(),
            TypeTag::kColumnValue);
}

TEST(ExpressionRewriteTest, CustomDslRuleIsIndependent) {
  using namespace expression_dsl;
  ExpressionRuleSet rules;
  rules.Add(ExpressionRule(
      "replace_named_column", Is(TypeTag::kColumnValue, "column"),
      [](const Expression&, const ExpressionBindings& bindings) {
        if (bindings.at("column")->AsColumnValue().GetName() != "old") {
          return Expression{};
        }
        return ColumnValueExp("new");
      }));

  Expression rewritten =
      ExpressionRewriter(std::move(rules))
          .Rewrite(BinaryExpressionExp(ColumnValueExp("old"),
                                       BinaryOperation::kEquals,
                                       ConstantValueExp(Value(1))));

  EXPECT_EQ(rewritten->AsBinaryExpression().Left()->AsColumnValue().GetName(),
            "new");
}

TEST(ExpressionRewriteTest, SplitAndCombineConjuncts) {
  Expression expression = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(1))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp("b"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(2))));

  const std::vector<Expression> conjuncts = SplitConjuncts(expression);

  ASSERT_EQ(conjuncts.size(), 2);
  EXPECT_EQ(SplitConjuncts(CombineConjuncts(conjuncts)).size(), 2);
}

TEST(ExpressionRewriteTest, FoldsInAndSimplifiesCase) {
  Expression in =
      InExpressionExp(ConstantValueExp(Value(2)),
                      {ConstantValueExp(Value(1)), ConstantValueExp(Value(2))});
  Expression folded =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(in);
  ASSERT_EQ(folded->Type(), TypeTag::kConstantValue);
  EXPECT_TRUE(folded->AsConstantValue().GetValue().Truthy());

  Expression case_expression = CaseExpressionExp(
      {{ConstantValueExp(Value(false)), ConstantValueExp(Value(1))},
       {ConstantValueExp(Value(true)), ConstantValueExp(Value(2))}},
      ConstantValueExp(Value(3)));
  Expression simplified =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(case_expression);
  ASSERT_EQ(simplified->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(simplified->AsConstantValue().GetValue(), Value(2));
}

TEST(ExpressionRewriteTest, PatternMatchAndBindings) {
  using namespace expression_dsl;
  Expression expression = BinaryExpressionExp(
      ColumnValueExp("a"), BinaryOperation::kAdd, ConstantValueExp(Value(1)));
  ExpressionBindings bindings;
  ExpressionPattern pattern =
      AnyBinary(Any("left"), Is(TypeTag::kConstantValue, "right"), "binary");
  ASSERT_TRUE(pattern.Match(expression, &bindings));
  EXPECT_EQ(bindings.size(), 3);
  EXPECT_EQ(bindings.at("left")->Type(), TypeTag::kColumnValue);
  EXPECT_EQ(bindings.at("right")->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(bindings.at("binary")->Type(), TypeTag::kBinaryExp);
}

TEST(ExpressionRewriteTest, PatternMatchFailures) {
  using namespace expression_dsl;
  Expression expression = BinaryExpressionExp(
      ColumnValueExp("a"), BinaryOperation::kAdd, ConstantValueExp(Value(1)));
  ExpressionBindings bindings;
  EXPECT_FALSE(Binary(BinaryOperation::kSubtract, Any(), Any())
                   .Match(expression, &bindings));
  EXPECT_FALSE(Is(TypeTag::kInExp).Match(expression, &bindings));
  EXPECT_FALSE(Any().Match(nullptr, &bindings));
  EXPECT_FALSE(Unary(UnaryOperation::kNot, Any()).Match(expression, &bindings));
}

TEST(ExpressionRewriteTest, PatternCaptureConflictRejects) {
  using namespace expression_dsl;
  Expression a = ColumnValueExp("a");
  Expression b = ColumnValueExp("b");
  Expression same = BinaryExpressionExp(a, BinaryOperation::kEquals, a);
  Expression different =
      BinaryExpressionExp(a, BinaryOperation::kEquals, b);
  ExpressionBindings bindings;
  ExpressionPattern pattern = AnyBinary(Any("x"), Any("x"));
  EXPECT_TRUE(pattern.Match(same, &bindings));
  bindings.clear();
  EXPECT_FALSE(pattern.Match(different, &bindings));
}

TEST(ExpressionRewriteTest, RuleApplyWithoutMatchReturnsNull) {
  using namespace expression_dsl;
  ExpressionRule rule(
      "only_in", Is(TypeTag::kInExp),
      [](const Expression&, const ExpressionBindings&) {
        return ConstantValueExp(Value(1));
      });
  EXPECT_EQ(rule.Apply(ConstantValueExp(Value(2))).get(), nullptr);
  EXPECT_NE(rule.Apply(InExpressionExp(ConstantValueExp(Value(1)),
                                       {ConstantValueExp(Value(1))}))
                .get(),
            nullptr);
}

TEST(ExpressionRewriteTest, RuleSetAddRemoveContains) {
  using namespace expression_dsl;
  ExpressionRuleSet rules;
  EXPECT_FALSE(rules.Contains("x"));
  rules.Add(ExpressionRule(
      "x", Any(),
      [](const Expression&, const ExpressionBindings&) {
        return Expression{};
      }));
  EXPECT_TRUE(rules.Contains("x"));
  EXPECT_TRUE(rules.Remove("x"));
  EXPECT_FALSE(rules.Remove("x"));
  EXPECT_FALSE(rules.Contains("x"));
}

TEST(ExpressionRewriteTest, FoldUnaryAndFoldIn) {
  Expression unary = UnaryExpressionExp(ConstantValueExp(Value(5)),
                                        UnaryOperation::kMinus);
  Expression rewritten_unary =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(unary);
  ASSERT_EQ(rewritten_unary->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_unary->AsConstantValue().GetValue(), Value(-5));

  Expression in = InExpressionExp(
      ConstantValueExp(Value(1)),
      {ConstantValueExp(Value(1)), ConstantValueExp(Value(2))});
  Expression rewritten_in =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(in);
  ASSERT_EQ(rewritten_in->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_in->AsConstantValue().GetValue(), Value(true));

  // Singleton IN is rewritten to an equality, not folded (column child).
  Expression in_nonconst =
      InExpressionExp(ColumnValueExp("x"), {ConstantValueExp(Value(1))});
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(in_nonconst);
  EXPECT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
}

TEST(ExpressionRewriteTest, CanonicalizeComparison) {
  // Constant on the right is already canonical.
  Expression expr = BinaryExpressionExp(ColumnValueExp("v"),
                                        BinaryOperation::kLessThan,
                                        ConstantValueExp(Value(3)));
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expr);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kLessThan);

  // Constant on the left is moved to the right with the operator flipped.
  Expression reversed = BinaryExpressionExp(ConstantValueExp(Value(3)),
                                            BinaryOperation::kLessThan,
                                            ColumnValueExp("v"));
  Expression rewritten_reversed =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(reversed);
  ASSERT_EQ(rewritten_reversed->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten_reversed->AsBinaryExpression().Op(),
            BinaryOperation::kGreaterThan);
  EXPECT_EQ(rewritten_reversed->AsBinaryExpression().Left()->Type(),
            TypeTag::kColumnValue);
  EXPECT_EQ(rewritten_reversed->AsBinaryExpression().Right()->Type(),
            TypeTag::kConstantValue);
}

TEST(ExpressionRewriteTest, BooleanIdentity) {
  Expression x = ColumnValueExp("x");
  EXPECT_EQ(ExpressionRewriter(ExpressionRuleSet::Default())
                .Rewrite(BinaryExpressionExp(x, BinaryOperation::kAnd,
                                             ConstantValueExp(Value(true))))
                ->Type(),
            TypeTag::kColumnValue);
  Expression rewritten_or =
      ExpressionRewriter(ExpressionRuleSet::Default())
          .Rewrite(BinaryExpressionExp(x, BinaryOperation::kOr,
                                       ConstantValueExp(Value(true))));
  ASSERT_EQ(rewritten_or->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_or->AsConstantValue().GetValue(), Value(true));
  Expression rewritten_and =
      ExpressionRewriter(ExpressionRuleSet::Default())
          .Rewrite(BinaryExpressionExp(x, BinaryOperation::kAnd,
                                       ConstantValueExp(Value(false))));
  ASSERT_EQ(rewritten_and->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_and->AsConstantValue().GetValue(), Value(false));
  EXPECT_EQ(ExpressionRewriter(ExpressionRuleSet::Default())
                .Rewrite(BinaryExpressionExp(x, BinaryOperation::kOr,
                                             ConstantValueExp(Value(false))))
                ->Type(),
            TypeTag::kColumnValue);
}

TEST(ExpressionRewriteTest, DoubleNegationAndDeMorgan) {
  Expression x = ColumnValueExp("x");
  Expression y = ColumnValueExp("y");
  Expression double_not = UnaryExpressionExp(
      UnaryExpressionExp(x, UnaryOperation::kNot), UnaryOperation::kNot);
  EXPECT_EQ(ExpressionRewriter(ExpressionRuleSet::Default())
                .Rewrite(double_not)
                ->Type(),
            TypeTag::kColumnValue);

  Expression not_and =
      UnaryExpressionExp(BinaryExpressionExp(x, BinaryOperation::kAnd, y),
                         UnaryOperation::kNot);
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(not_and);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kOr);
  EXPECT_EQ(rewritten->AsBinaryExpression().Left()->Type(),
            TypeTag::kUnaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Right()->Type(),
            TypeTag::kUnaryExp);
}

TEST(ExpressionRewriteTest, SimplifyCaseVariants) {
  // All-false conditions leave the else clause.
  Expression case_all_false = CaseExpressionExp(
      {{ConstantValueExp(Value(false)), ConstantValueExp(Value(1))}},
      ConstantValueExp(Value(9)));
  Expression simplified2 =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(case_all_false);
  ASSERT_EQ(simplified2->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(simplified2->AsConstantValue().GetValue(), Value(9));

  // Non-constant conditions are preserved.
  Expression case_keep = CaseExpressionExp(
      {{ColumnValueExp("v"), ConstantValueExp(Value(1))}},
      ConstantValueExp(Value(9)));
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(case_keep);
  EXPECT_EQ(rewritten->Type(), TypeTag::kCaseExp);
}

TEST(ExpressionRewriteTest, ExpressionChildrenAndWithChildren) {
  Expression binary = BinaryExpressionExp(ConstantValueExp(Value(1)),
                                          BinaryOperation::kAdd,
                                          ColumnValueExp("x"));
  std::vector<Expression> bin_children = ExpressionChildren(binary);
  ASSERT_EQ(bin_children.size(), 2);
  EXPECT_EQ(WithExpressionChildren(binary, bin_children)->Type(),
            TypeTag::kBinaryExp);

  Expression unary = UnaryExpressionExp(ColumnValueExp("x"),
                                        UnaryOperation::kNot);
  std::vector<Expression> unary_children = ExpressionChildren(unary);
  ASSERT_EQ(unary_children.size(), 1);
  EXPECT_EQ(WithExpressionChildren(unary, unary_children)->Type(),
            TypeTag::kUnaryExp);

  Expression aggregate =
      AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("x"));
  std::vector<Expression> agg_children = ExpressionChildren(aggregate);
  ASSERT_EQ(agg_children.size(), 1);
  EXPECT_EQ(WithExpressionChildren(aggregate, agg_children)->Type(),
            TypeTag::kAggregateExp);

  Expression case_exp = CaseExpressionExp(
      {{ColumnValueExp("a"), ColumnValueExp("b")},
       {ColumnValueExp("c"), ColumnValueExp("d")}},
      ColumnValueExp("e"));
  std::vector<Expression> case_children = ExpressionChildren(case_exp);
  ASSERT_EQ(case_children.size(), 5);
  EXPECT_EQ(WithExpressionChildren(case_exp, case_children)->Type(),
            TypeTag::kCaseExp);

  Expression in = InExpressionExp(
      ColumnValueExp("x"),
      {ConstantValueExp(Value(1)), ConstantValueExp(Value(2))});
  std::vector<Expression> in_children = ExpressionChildren(in);
  ASSERT_EQ(in_children.size(), 3);
  EXPECT_EQ(WithExpressionChildren(in, in_children)->Type(), TypeTag::kInExp);

  Expression func =
      FunctionCallExp("coalesce", {ColumnValueExp("x"), ColumnValueExp("y")});
  std::vector<Expression> func_children = ExpressionChildren(func);
  ASSERT_EQ(func_children.size(), 2);
  EXPECT_EQ(WithExpressionChildren(func, func_children)->Type(),
            TypeTag::kFunctionCallExp);

  Expression query = QueryExpressionExp(std::shared_ptr<SelectStatement>(),
                                        ColumnValueExp("x"));
  std::vector<Expression> query_children = ExpressionChildren(query);
  ASSERT_EQ(query_children.size(), 1);
  EXPECT_EQ(WithExpressionChildren(query, query_children)->Type(),
            TypeTag::kQueryExp);

  EXPECT_TRUE(ExpressionChildren(ConstantValueExp(Value(1))).empty());
  EXPECT_TRUE(ExpressionChildren(ColumnValueExp("x")).empty());
  EXPECT_TRUE(ExpressionChildren(nullptr).empty());

  EXPECT_THROW(WithExpressionChildren(ConstantValueExp(Value(1)),
                                      {ConstantValueExp(Value(2))}),
               std::invalid_argument);
  EXPECT_THROW(WithExpressionChildren(binary, {ColumnValueExp("x")}),
               std::invalid_argument);
  EXPECT_THROW(WithExpressionChildren(in, {}), std::invalid_argument);
  EXPECT_THROW(WithExpressionChildren(query,
                                      {ColumnValueExp("x"),
                                       ColumnValueExp("y")}),
               std::invalid_argument);
}

TEST(ExpressionRewriteTest, SplitCombineEdgeCases) {
  Expression single = BinaryExpressionExp(ColumnValueExp("a"),
                                          BinaryOperation::kEquals,
                                          ConstantValueExp(Value(1)));
  EXPECT_EQ(SplitConjuncts(single).size(), 1);
  EXPECT_TRUE(SplitConjuncts(nullptr).empty());
  Expression deep = BinaryExpressionExp(
      BinaryExpressionExp(
          BinaryExpressionExp(ConstantValueExp(Value(true)),
                              BinaryOperation::kAnd, ColumnValueExp("a")),
          BinaryOperation::kAnd, ColumnValueExp("b")),
      BinaryOperation::kAnd, ColumnValueExp("c"));
  EXPECT_EQ(SplitConjuncts(deep).size(), 4);
  Expression combined_empty = CombineConjuncts({});
  ASSERT_EQ(combined_empty->Type(), TypeTag::kConstantValue);
  EXPECT_TRUE(combined_empty->AsConstantValue().GetValue().Truthy());
  EXPECT_EQ(CombineConjuncts({ColumnValueExp("a")})->Type(),
            TypeTag::kColumnValue);
}

TEST(ExpressionRewriteTest, ReferencesOnly) {
  EXPECT_TRUE(ReferencesOnly(nullptr, {}));
  Expression unqualified = BinaryExpressionExp(
      ColumnValueExp("x"), BinaryOperation::kEquals,
      ConstantValueExp(Value(1)));
  EXPECT_FALSE(ReferencesOnly(unqualified, {}));
  ColumnName qualified("s", "x");
  Expression qualified_expr = BinaryExpressionExp(
      ColumnValueExp(qualified), BinaryOperation::kEquals,
      ConstantValueExp(Value(1)));
  EXPECT_TRUE(ReferencesOnly(qualified_expr, {"s"}));
  EXPECT_FALSE(ReferencesOnly(qualified_expr, {"other"}));
}

TEST(ExpressionRewriteTest, NonConvergingRewriteThrows) {
  using namespace expression_dsl;
  ExpressionRuleSet rules;
  // Toggle the constant between 0 and 1 forever: the expression never
  // stabilizes, so the 32-pass budget is exhausted and Rewrite throws.
  rules.Add(ExpressionRule(
      "toggle", Is(TypeTag::kConstantValue),
      [](const Expression& expression, const ExpressionBindings&) {
        if (expression->AsConstantValue().GetValue().Truthy()) {
          return ConstantValueExp(Value(0));
        }
        return ConstantValueExp(Value(1));
      }));
  EXPECT_THROW(
      (void)ExpressionRewriter(std::move(rules))
          .Rewrite(ConstantValueExp(Value(1))),
      std::runtime_error);
}

TEST(ExpressionRewriteTest, RewriteAppliesSingleRule) {
  using namespace expression_dsl;
  ExpressionRuleSet rules;
  rules.Add(ExpressionRule(
      "fold_one", AnyBinary(Is(TypeTag::kConstantValue, "left"),
                            Is(TypeTag::kConstantValue, "right")),
      [](const Expression&, const ExpressionBindings&) {
        return ConstantValueExp(Value(42));
      }));
  Expression expr = BinaryExpressionExp(ConstantValueExp(Value(1)),
                                        BinaryOperation::kAdd,
                                        ConstantValueExp(Value(2)));
  Expression once = ExpressionRewriter(std::move(rules)).Rewrite(expr);
  ASSERT_EQ(once->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(once->AsConstantValue().GetValue(), Value(42));
}

}  // namespace tinylamb
