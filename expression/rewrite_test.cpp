/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/rewrite.hpp"

#include "common/constants.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "gtest/gtest.h"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

#include <memory>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

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
      ExpressionRewriter(rules).Rewrite(expression);

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
      ExpressionRewriter(rules)
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

  EXPECT_THROW(std::ignore = WithExpressionChildren(
                    ConstantValueExp(Value(1)), {ConstantValueExp(Value(2))}),
                std::invalid_argument);
  EXPECT_THROW(std::ignore = WithExpressionChildren(binary, {ColumnValueExp("x")}),
               std::invalid_argument);
  EXPECT_THROW(std::ignore = WithExpressionChildren(in, {}),
               std::invalid_argument);
  EXPECT_THROW(std::ignore = WithExpressionChildren(
                    query, {ColumnValueExp("x"), ColumnValueExp("y")}),
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
      (void)ExpressionRewriter(rules)
          .Rewrite(ConstantValueExp(Value(1))),
      std::runtime_error);
}

TEST(ExpressionRewriteTest, DeeplyNestedExpressionThrowsInsteadOfCrashing) {
  // Attacker-controlled SQL such as ((((((...1...)))))) must surface as an
  // exception instead of overflowing the stack and killing the process.
  Expression expression = ColumnValueExp("v");
  for (int i = 0; i < 1024; ++i) {
    expression = UnaryExpressionExp(expression, UnaryOperation::kMinus);
  }
  EXPECT_THROW((void)ExpressionRewriter(ExpressionRuleSet::Default())
                   .Rewrite(expression),
               std::runtime_error);
}

TEST(ExpressionRewriteTest, NestedExpressionWithinDepthLimitStillRewrites) {
  Expression expression = ColumnValueExp("v");
  for (int i = 0; i < 400; ++i) {
    expression = UnaryExpressionExp(expression, UnaryOperation::kMinus);
  }
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  // An even number of negations collapses back to the bare column.
  EXPECT_EQ(rewritten->Type(), TypeTag::kColumnValue);
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
  Expression once = ExpressionRewriter(rules).Rewrite(expr);
  ASSERT_EQ(once->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(once->AsConstantValue().GetValue(), Value(42));
}

TEST(ExpressionRewriteTest, NotPushdownRewritesComparisons) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };
  Expression x = ColumnValueExp("x");
  Expression y = ColumnValueExp("y");

  Expression not_less =
      UnaryExpressionExp(BinaryExpressionExp(x, BinaryOperation::kLessThan, y),
                         UnaryOperation::kNot);
  Expression rewritten = rewrite(not_less);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(),
            BinaryOperation::kGreaterThanEquals);

  Expression not_equals = UnaryExpressionExp(
      BinaryExpressionExp(x, BinaryOperation::kEquals, y),
      UnaryOperation::kNot);
  rewritten = rewrite(not_equals);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(),
            BinaryOperation::kNotEquals);

  // NOT over AND stays under De Morgan control, not comparison negation.
  Expression not_and =
      UnaryExpressionExp(BinaryExpressionExp(x, BinaryOperation::kAnd, y),
                         UnaryOperation::kNot);
  rewritten = rewrite(not_and);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kOr);
}

TEST(ExpressionRewriteTest, NotPushdownRewritesLikeAndNullChecks) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };
  Expression name = ColumnValueExp("name");

  Expression not_like = UnaryExpressionExp(
      BinaryExpressionExp(name, BinaryOperation::kLike,
                          ConstantValueExp(Value("a%"))),
      UnaryOperation::kNot);
  Expression rewritten = rewrite(not_like);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kNotLike);

  Expression not_not_like = UnaryExpressionExp(
      BinaryExpressionExp(name, BinaryOperation::kNotLike,
                          ConstantValueExp(Value("a%"))),
      UnaryOperation::kNot);
  rewritten = rewrite(not_not_like);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kLike);

  Expression not_is_null = UnaryExpressionExp(
      UnaryExpressionExp(ColumnValueExp("x"), UnaryOperation::kIsNull),
      UnaryOperation::kNot);
  rewritten = rewrite(not_is_null);
  ASSERT_EQ(rewritten->Type(), TypeTag::kUnaryExp);
  EXPECT_EQ(rewritten->AsUnaryExpression().Op(), UnaryOperation::kIsNotNull);

  Expression not_is_not_null = UnaryExpressionExp(
      UnaryExpressionExp(ColumnValueExp("x"), UnaryOperation::kIsNotNull),
      UnaryOperation::kNot);
  rewritten = rewrite(not_is_not_null);
  ASSERT_EQ(rewritten->Type(), TypeTag::kUnaryExp);
  EXPECT_EQ(rewritten->AsUnaryExpression().Op(), UnaryOperation::kIsNull);
}

TEST(ExpressionRewriteTest, XorBooleanIdentity) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };
  Expression x = ColumnValueExp("x");

  Expression xor_true = BinaryExpressionExp(
      x, BinaryOperation::kXor, ConstantValueExp(Value(true)));
  Expression rewritten = rewrite(xor_true);
  ASSERT_EQ(rewritten->Type(), TypeTag::kUnaryExp);
  EXPECT_EQ(rewritten->AsUnaryExpression().Op(), UnaryOperation::kNot);

  Expression false_xor = BinaryExpressionExp(
      ConstantValueExp(Value(false)), BinaryOperation::kXor, x);
  rewritten = rewrite(false_xor);
  EXPECT_EQ(rewritten->Type(), TypeTag::kColumnValue);

  Expression xor_columns = BinaryExpressionExp(x, BinaryOperation::kXor,
                                               ColumnValueExp("y"));
  EXPECT_EQ(rewrite(xor_columns)->Type(), TypeTag::kBinaryExp);
}

TEST(ExpressionRewriteTest, IdempotenceAndAbsorption) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };
  Expression x = ColumnValueExp("x");
  Expression y = ColumnValueExp("y");

  Expression and_idempotent = BinaryExpressionExp(
      x, BinaryOperation::kAnd, ColumnValueExp("x"));
  EXPECT_EQ(rewrite(and_idempotent)->Type(), TypeTag::kColumnValue);

  Expression or_idempotent = BinaryExpressionExp(
      y, BinaryOperation::kOr, ColumnValueExp("y"));
  EXPECT_EQ(rewrite(or_idempotent)->Type(), TypeTag::kColumnValue);

  Expression absorption = BinaryExpressionExp(
      x, BinaryOperation::kAnd,
      BinaryExpressionExp(x, BinaryOperation::kOr, y));
  EXPECT_EQ(rewrite(absorption)->Type(), TypeTag::kColumnValue);

  Expression absorption_reversed = BinaryExpressionExp(
      BinaryExpressionExp(x, BinaryOperation::kOr, y),
      BinaryOperation::kAnd, x);
  EXPECT_EQ(rewrite(absorption_reversed)->Type(), TypeTag::kColumnValue);

  Expression absorption_or = BinaryExpressionExp(
      x, BinaryOperation::kOr,
      BinaryExpressionExp(x, BinaryOperation::kAnd, y));
  EXPECT_EQ(rewrite(absorption_or)->Type(), TypeTag::kColumnValue);

  // Non-absorption shapes survive unchanged.
  Expression kept = BinaryExpressionExp(
      x, BinaryOperation::kAnd,
      BinaryExpressionExp(y, BinaryOperation::kOr, ColumnValueExp("z")));
  EXPECT_EQ(rewrite(kept)->Type(), TypeTag::kBinaryExp);
}

TEST(ExpressionRewriteTest, ArithmeticIdentitiesAndDoubleNegation) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };
  Expression x = ColumnValueExp("x");

  EXPECT_EQ(rewrite(BinaryExpressionExp(x, BinaryOperation::kAdd,
                                        ConstantValueExp(Value(0))))
                ->Type(),
            TypeTag::kColumnValue);
  EXPECT_EQ(rewrite(BinaryExpressionExp(ConstantValueExp(Value(0)),
                                        BinaryOperation::kAdd, x))
                ->Type(),
            TypeTag::kColumnValue);
  EXPECT_EQ(rewrite(BinaryExpressionExp(x, BinaryOperation::kSubtract,
                                        ConstantValueExp(Value(0))))
                ->Type(),
            TypeTag::kColumnValue);
  EXPECT_EQ(rewrite(BinaryExpressionExp(x, BinaryOperation::kMultiply,
                                        ConstantValueExp(Value(1))))
                ->Type(),
            TypeTag::kColumnValue);
  EXPECT_EQ(rewrite(BinaryExpressionExp(ConstantValueExp(Value(1)),
                                        BinaryOperation::kMultiply, x))
                ->Type(),
            TypeTag::kColumnValue);
  EXPECT_EQ(rewrite(BinaryExpressionExp(x, BinaryOperation::kDivide,
                                        ConstantValueExp(Value(1))))
                ->Type(),
            TypeTag::kColumnValue);

  Expression double_minus = UnaryExpressionExp(
      UnaryExpressionExp(x, UnaryOperation::kMinus), UnaryOperation::kMinus);
  EXPECT_EQ(rewrite(double_minus)->Type(), TypeTag::kColumnValue);

  // Non-identity operands stay untouched.
  Expression kept = BinaryExpressionExp(x, BinaryOperation::kAdd,
                                        ConstantValueExp(Value(2)));
  EXPECT_EQ(rewrite(kept)->Type(), TypeTag::kBinaryExp);
}

TEST(ExpressionRewriteTest, CanonicalizesSimpleArithmeticShapes) {
  const ExpressionRewriter rewriter(ExpressionRuleSet::Default());
  const Expression integer =
      FunctionCallExp("extract_year", {ColumnValueExp("event_date")});

  Expression add_negative = rewriter.Rewrite(BinaryExpressionExp(
      ColumnValueExp("x"), BinaryOperation::kAdd,
      ConstantValueExp(Value(-7))));
  ASSERT_EQ(add_negative->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(add_negative->AsBinaryExpression().Op(),
            BinaryOperation::kSubtract);
  EXPECT_EQ(add_negative->AsBinaryExpression()
                .Right()
                ->AsConstantValue()
                .GetValue(),
            Value(7));

  Expression subtract_negative = rewriter.Rewrite(BinaryExpressionExp(
      ColumnValueExp("x"), BinaryOperation::kSubtract,
      ConstantValueExp(Value(-9))));
  ASSERT_EQ(subtract_negative->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(subtract_negative->AsBinaryExpression().Op(),
            BinaryOperation::kAdd);
  EXPECT_EQ(subtract_negative->AsBinaryExpression()
                .Right()
                ->AsConstantValue()
                .GetValue(),
            Value(9));

  Expression negative_one = rewriter.Rewrite(BinaryExpressionExp(
      integer, BinaryOperation::kMultiply, ConstantValueExp(Value(-1))));
  ASSERT_EQ(negative_one->Type(), TypeTag::kUnaryExp);
  EXPECT_EQ(negative_one->AsUnaryExpression().Op(), UnaryOperation::kMinus);

  Expression repeated = rewriter.Rewrite(BinaryExpressionExp(
      integer, BinaryOperation::kAdd, integer));
  ASSERT_EQ(repeated->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(repeated->AsBinaryExpression().Op(), BinaryOperation::kMultiply);
  EXPECT_EQ(repeated->AsBinaryExpression()
                .Right()
                ->AsConstantValue()
                .GetValue(),
            Value(2));
}

TEST(ExpressionRewriteTest, DoesNotCombineVolatileOrUntypedRepeatedAddends) {
  const ExpressionRewriter rewriter(ExpressionRuleSet::Default());
  Expression volatile_add = BinaryExpressionExp(
      FunctionCallExp("rand", {}), BinaryOperation::kAdd,
      FunctionCallExp("rand", {}));
  Expression rewritten = rewriter.Rewrite(volatile_add);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kAdd);

  Expression string_add = BinaryExpressionExp(
      ColumnValueExp("unknown"), BinaryOperation::kAdd,
      ColumnValueExp("unknown"));
  rewritten = rewriter.Rewrite(string_add);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kAdd);
}

TEST(ExpressionRewriteTest, TypedIntegerMultiplyZeroPreservesNullsAndType) {
  const Schema schema(
      "input", {Column("i", ValueType::kInt64),
                Column("d", ValueType::kDouble)});
  const Expression integer_multiply = BinaryExpressionExp(
      ColumnValueExp("i"), BinaryOperation::kMultiply,
      ConstantValueExp(Value(0)));

  const Expression rewritten =
      RewriteTypedArithmetic(integer_multiply, schema);
  EXPECT_EQ(rewritten->Type(), TypeTag::kCaseExp);
  EXPECT_EQ(rewritten->ResultType(schema).GetType(), TypeTag::kBigInt);
  EXPECT_EQ(rewritten->Evaluate(Row({Value(9), Value(1.5)}), schema),
            Value(0));
  EXPECT_TRUE(rewritten->Evaluate(Row({Value(), Value(1.5)}), schema).IsNull());
  EXPECT_EQ(rewritten->ToString().find("* 0"), std::string::npos);

  // IEEE NaN and infinity make the corresponding DOUBLE rewrite unsafe.
  const Expression double_multiply = BinaryExpressionExp(
      ColumnValueExp("d"), BinaryOperation::kMultiply,
      ConstantValueExp(Value(0)));
  EXPECT_EQ(RewriteTypedArithmetic(double_multiply, schema)->Type(),
            TypeTag::kBinaryExp);
}

TEST(ExpressionRewriteTest, SchemaFreeSelfComparisonKeepsThreeValuedResult) {
  const Expression comparison = BinaryExpressionExp(
      ColumnValueExp("x"), BinaryOperation::kLessThan, ColumnValueExp("x"));
  const Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(comparison);

  // x < x is FALSE for a non-NULL x and UNKNOWN for NULL. A scalar rewrite
  // to either one constant would therefore be incorrect outside WHERE.
  EXPECT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(),
            BinaryOperation::kLessThan);
}

TEST(ExpressionRewriteTest, ReassociateConstantArithmetic) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };
  // A statically int64 inner expression that no rule can constant-fold.
  const auto int64_inner = [] {
    return FunctionCallExp("extract_year", {ColumnValueExp("event_date")});
  };

  Expression nested_add = BinaryExpressionExp(
      BinaryExpressionExp(int64_inner(), BinaryOperation::kAdd,
                          ConstantValueExp(Value(1))),
      BinaryOperation::kAdd, ConstantValueExp(Value(2)));
  Expression rewritten = rewrite(nested_add);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(
      rewritten->AsBinaryExpression().Right()->AsConstantValue().GetValue(),
      Value(3));
  Expression nested_subtract = BinaryExpressionExp(
      BinaryExpressionExp(int64_inner(), BinaryOperation::kSubtract,
                          ConstantValueExp(Value(5))),
      BinaryOperation::kSubtract, ConstantValueExp(Value(3)));
  rewritten = rewrite(nested_subtract);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(
      rewritten->AsBinaryExpression().Right()->AsConstantValue().GetValue(),
      Value(8));
  EXPECT_EQ(rewritten->AsBinaryExpression().Left()->Type(),
            TypeTag::kFunctionCallExp);
}

TEST(ExpressionRewriteTest, ReassociationRestrictedToIntegerExpressions) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };

  // Reassociating floating point arithmetic changes the IEEE rounding order,
  // so statically double-typed inners must be left alone.
  Expression avg =
      AggregateExpressionExp(AggregationType::kAvg, ColumnValueExp("amount"));
  Expression fp_nested = BinaryExpressionExp(
      BinaryExpressionExp(avg, BinaryOperation::kAdd,
                          ConstantValueExp(Value(1))),
      BinaryOperation::kAdd, ConstantValueExp(Value(2)));
  Expression rewritten = rewrite(fp_nested);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(
      rewritten->AsBinaryExpression().Right()->AsConstantValue().GetValue(),
      Value(2));
  ASSERT_EQ(rewritten->AsBinaryExpression().Left()->Type(),
            TypeTag::kBinaryExp);

  // The type of a bare column is unknown without a schema; keep the shape.
  Expression column_nested = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("x"), BinaryOperation::kAdd,
                          ConstantValueExp(Value(1))),
      BinaryOperation::kAdd, ConstantValueExp(Value(2)));
  rewritten = rewrite(column_nested);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Left()->Type(),
            TypeTag::kBinaryExp);
}

TEST(ExpressionRewriteTest, DedupeInListAndUniformCaseResult) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };

  Expression duplicated = InExpressionExp(
      ColumnValueExp("x"),
      {ConstantValueExp(Value(1)), ConstantValueExp(Value(1)),
       ConstantValueExp(Value(2))});
  Expression rewritten = rewrite(duplicated);
  ASSERT_EQ(rewritten->Type(), TypeTag::kInExp);
  EXPECT_EQ(rewritten->AsInExpression().list_.size(), 2U);

  Expression unique = InExpressionExp(
      ColumnValueExp("x"),
      {ConstantValueExp(Value(1)), ConstantValueExp(Value(2))});
  EXPECT_EQ(rewrite(unique)->AsInExpression().list_.size(), 2U);

  Expression uniform = CaseExpressionExp(
      {{ColumnValueExp("a"), ConstantValueExp(Value(7))},
       {ColumnValueExp("b"), ConstantValueExp(Value(7))}},
      ConstantValueExp(Value(7)));
  rewritten = rewrite(uniform);
  ASSERT_EQ(rewritten->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten->AsConstantValue().GetValue(), Value(7));

  Expression mixed = CaseExpressionExp(
      {{ColumnValueExp("a"), ConstantValueExp(Value(7))},
       {ColumnValueExp("b"), ConstantValueExp(Value(8))}},
      ConstantValueExp(Value(7)));
  EXPECT_EQ(rewrite(mixed)->Type(), TypeTag::kCaseExp);
}

TEST(ExpressionRewriteTest, WildcardFreeLikeBecomesEquality) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };

  Expression like_exact =
      BinaryExpressionExp(ColumnValueExp("name"), BinaryOperation::kLike,
                          ConstantValueExp(Value("abc")));
  Expression rewritten = rewrite(like_exact);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kEquals);
  EXPECT_EQ(
      rewritten->AsBinaryExpression().Right()->AsConstantValue().GetValue(),
      Value("abc"));

  Expression not_like_exact =
      BinaryExpressionExp(ColumnValueExp("name"), BinaryOperation::kNotLike,
                          ConstantValueExp(Value("abc")));
  rewritten = rewrite(not_like_exact);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kNotEquals);

  // Wildcards keep the LIKE operator.
  Expression like_percent =
      BinaryExpressionExp(ColumnValueExp("name"), BinaryOperation::kLike,
                          ConstantValueExp(Value("a%")));
  EXPECT_EQ(rewrite(like_percent)->AsBinaryExpression().Op(),
            BinaryOperation::kLike);

  Expression like_underscore =
      BinaryExpressionExp(ColumnValueExp("name"), BinaryOperation::kLike,
                          ConstantValueExp(Value("a_c")));
  EXPECT_EQ(rewrite(like_underscore)->AsBinaryExpression().Op(),
            BinaryOperation::kLike);
}

TEST(ExpressionRewriteTest, NullCheckCompositionCollapsesToConstant) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };

  Expression is_null_of_is_null = UnaryExpressionExp(
      UnaryExpressionExp(ColumnValueExp("x"), UnaryOperation::kIsNull),
      UnaryOperation::kIsNull);
  Expression rewritten = rewrite(is_null_of_is_null);
  ASSERT_EQ(rewritten->Type(), TypeTag::kConstantValue);
  EXPECT_FALSE(rewritten->AsConstantValue().GetValue().Truthy());

  Expression is_not_null_of_is_null = UnaryExpressionExp(
      UnaryExpressionExp(ColumnValueExp("x"), UnaryOperation::kIsNull),
      UnaryOperation::kIsNotNull);
  rewritten = rewrite(is_not_null_of_is_null);
  ASSERT_EQ(rewritten->Type(), TypeTag::kConstantValue);
  EXPECT_TRUE(rewritten->AsConstantValue().GetValue().Truthy());

  Expression is_null_of_is_not_null = UnaryExpressionExp(
      UnaryExpressionExp(ColumnValueExp("x"), UnaryOperation::kIsNotNull),
      UnaryOperation::kIsNull);
  rewritten = rewrite(is_null_of_is_not_null);
  ASSERT_EQ(rewritten->Type(), TypeTag::kConstantValue);
  EXPECT_FALSE(rewritten->AsConstantValue().GetValue().Truthy());

  // NOT over a null check uses the pushdown rules instead of collapsing.
  Expression not_is_null = UnaryExpressionExp(
      UnaryExpressionExp(ColumnValueExp("x"), UnaryOperation::kIsNull),
      UnaryOperation::kNot);
  rewritten = rewrite(not_is_null);
  ASSERT_EQ(rewritten->Type(), TypeTag::kUnaryExp);
  EXPECT_EQ(rewritten->AsUnaryExpression().Op(), UnaryOperation::kIsNotNull);
}

TEST(ExpressionRewriteTest, MixedReassociationOfConstants) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };
  // A statically int64 inner expression that no rule can constant-fold.
  const auto int64_inner = [] {
    return FunctionCallExp("extract_year", {ColumnValueExp("event_date")});
  };

  // (x - 5) + 3 -> x - 2 (negative constants are canonicalized).
  Expression subtract_then_add = BinaryExpressionExp(
      BinaryExpressionExp(int64_inner(), BinaryOperation::kSubtract,
                          ConstantValueExp(Value(5))),
      BinaryOperation::kAdd, ConstantValueExp(Value(3)));
  Expression rewritten = rewrite(subtract_then_add);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(),
            BinaryOperation::kSubtract);
  EXPECT_EQ(
      rewritten->AsBinaryExpression().Right()->AsConstantValue().GetValue(),
      Value(2));

  // (x + 5) - 3 -> x + 2
  Expression add_then_subtract = BinaryExpressionExp(
      BinaryExpressionExp(int64_inner(), BinaryOperation::kAdd,
                          ConstantValueExp(Value(5))),
      BinaryOperation::kSubtract, ConstantValueExp(Value(3)));
  rewritten = rewrite(add_then_subtract);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(
      rewritten->AsBinaryExpression().Right()->AsConstantValue().GetValue(),
      Value(2));
}

TEST(ExpressionRewriteTest, ComplementaryAbsorptionDisabled) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };
  Expression x = ColumnValueExp("x");
  Expression y = ColumnValueExp("y");

  // x AND (NOT x OR y) must NOT be rewritten to x AND y: the identity fails
  // under SQL three-valued logic (x = NULL, y = FALSE yields UNKNOWN on the
  // left side but FALSE on the right). The rule was therefore removed.
  Expression complement = BinaryExpressionExp(
      x, BinaryOperation::kAnd,
      BinaryExpressionExp(UnaryExpressionExp(x, UnaryOperation::kNot),
                          BinaryOperation::kOr, y));
  Expression rewritten = rewrite(complement);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kAnd);
  const Expression& or_clause = rewritten->AsBinaryExpression().Right();
  ASSERT_EQ(or_clause->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(or_clause->AsBinaryExpression().Op(), BinaryOperation::kOr);

  Expression complement_reversed = BinaryExpressionExp(
      BinaryExpressionExp(UnaryExpressionExp(x, UnaryOperation::kNot),
                          BinaryOperation::kOr, y),
      BinaryOperation::kAnd, x);
  rewritten = rewrite(complement_reversed);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kAnd);
  EXPECT_EQ(rewritten->AsBinaryExpression().Right()->Type(),
            TypeTag::kColumnValue);

  // The negation inside must reference the same operand; otherwise keep.
  Expression unrelated = BinaryExpressionExp(
      x, BinaryOperation::kAnd,
      BinaryExpressionExp(UnaryExpressionExp(y, UnaryOperation::kNot),
                          BinaryOperation::kOr, ColumnValueExp("z")));
  EXPECT_EQ(rewrite(unrelated)->Type(), TypeTag::kBinaryExp);
}

TEST(ExpressionRewriteTest, CollapseNestedIdenticalCast) {
  Expression nested = CastExpressionExp(
      CastExpressionExp(ColumnValueExp("x"), "INT64"), "INT64");
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(nested);
  ASSERT_EQ(rewritten->Type(), TypeTag::kCastExp);
  EXPECT_EQ(rewritten->AsCastExpression().Child()->Type(),
            TypeTag::kColumnValue);
}

TEST(ExpressionRewriteTest, FactorCommonAndFromOr) {
  Expression x = ColumnValueExp("x");
  Expression y = ColumnValueExp("y");
  Expression z = ColumnValueExp("z");
  Expression expression = BinaryExpressionExp(
      BinaryExpressionExp(x, BinaryOperation::kAnd, y), BinaryOperation::kOr,
      BinaryExpressionExp(x, BinaryOperation::kAnd, z));
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kAnd);
}

TEST(ExpressionRewriteTest, ExpressionChildrenRewritesCast) {
  Expression cast = CastExpressionExp(ColumnValueExp("x"), "INT64");
  std::vector<Expression> children = ExpressionChildren(cast);
  ASSERT_EQ(children.size(), 1);
  EXPECT_EQ(WithExpressionChildren(cast, children)->Type(), TypeTag::kCastExp);
}

TEST(ExpressionRewriteTest, JsonPathConstantFold) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  // JSON_EXTRACT with constant json and path
  Expression extract = FunctionCallExp(
      "json_extract",
      {ConstantValueExp(Value("{\"a\": 1, \"b\": \"hello\"}")),
       ConstantValueExp(Value("$.a"))});
  Expression rewritten_extract = rewriter.Rewrite(extract);
  ASSERT_EQ(rewritten_extract->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_extract->AsConstantValue().GetValue(), Value("1"));

  // JSON_VALUE extracting scalar string
  Expression val_call = FunctionCallExp(
      "json_value",
      {ConstantValueExp(Value("{\"name\": \"Alice\"}")),
       ConstantValueExp(Value("$.name"))});
  Expression rewritten_val = rewriter.Rewrite(val_call);
  ASSERT_EQ(rewritten_val->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_val->AsConstantValue().GetValue(), Value("Alice"));

  // JSON_QUERY extracting object/array
  Expression query_call = FunctionCallExp(
      "json_query",
      {ConstantValueExp(Value("{\"items\": [10, 20]}")),
       ConstantValueExp(Value("$.items"))});
  Expression rewritten_query = rewriter.Rewrite(query_call);
  ASSERT_EQ(rewritten_query->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_query->AsConstantValue().GetValue(), Value("[10,20]"));

  // Default path $ when omitted
  Expression extract_root = FunctionCallExp(
      "json_extract",
      {ConstantValueExp(Value("{\"x\": 42}"))});
  Expression rewritten_root = rewriter.Rewrite(extract_root);
  ASSERT_EQ(rewritten_root->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_root->AsConstantValue().GetValue(), Value("{\"x\":42}"));
}

TEST(ExpressionRewriteTest, NumericWideningCast) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  Expression x = ColumnValueExp("x");

  // CAST(CAST(x AS INT32) AS INT64) -> CAST(x AS INT64)
  Expression nested_int = CastExpressionExp(
      CastExpressionExp(x, "INT32"), "INT64");
  Expression rewritten_int = rewriter.Rewrite(nested_int);
  ASSERT_EQ(rewritten_int->Type(), TypeTag::kCastExp);
  EXPECT_EQ(rewritten_int->AsCastExpression().TargetTypeName(), "INT64");
  EXPECT_EQ(rewritten_int->AsCastExpression().Child()->Type(), TypeTag::kColumnValue);

  // CAST(CAST(x AS UINT8) AS INT32) -> CAST(x AS INT32)
  Expression nested_uint = CastExpressionExp(
      CastExpressionExp(x, "UINT8"), "INT32");
  Expression rewritten_uint = rewriter.Rewrite(nested_uint);
  ASSERT_EQ(rewritten_uint->Type(), TypeTag::kCastExp);
  EXPECT_EQ(rewritten_uint->AsCastExpression().TargetTypeName(), "INT32");

  // CAST(CAST(x AS FLOAT) AS DOUBLE) -> CAST(x AS DOUBLE)
  Expression nested_float = CastExpressionExp(
      CastExpressionExp(x, "FLOAT"), "DOUBLE");
  Expression rewritten_float = rewriter.Rewrite(nested_float);
  ASSERT_EQ(rewritten_float->Type(), TypeTag::kCastExp);
  EXPECT_EQ(rewritten_float->AsCastExpression().TargetTypeName(), "DOUBLE");

  // Narrowing cast CAST(CAST(x AS INT64) AS INT32) should NOT be collapsed
  Expression narrowing = CastExpressionExp(
      CastExpressionExp(x, "INT64"), "INT32");
  Expression rewritten_narrowing = rewriter.Rewrite(narrowing);
  ASSERT_EQ(rewritten_narrowing->Type(), TypeTag::kCastExp);
  EXPECT_EQ(rewritten_narrowing->AsCastExpression().Child()->Type(), TypeTag::kCastExp);

  // Constant cast folding CAST(123 AS DOUBLE)
  Expression constant_cast = CastExpressionExp(ConstantValueExp(Value(int64_t{123})), "DOUBLE");
  Expression rewritten_const = rewriter.Rewrite(constant_cast);
  ASSERT_EQ(rewritten_const->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_const->AsConstantValue().GetValue(), Value(123.0));
}

TEST(ExpressionRewriteTest, OrOfRangesToIn) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  Expression x = ColumnValueExp("x");

  // (x = 1 OR x = 2 OR x = 3) -> x IN (1, 2, 3)
  Expression or_chain = BinaryExpressionExp(
      BinaryExpressionExp(
          BinaryExpressionExp(x, BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{1}))),
          BinaryOperation::kOr,
          BinaryExpressionExp(x, BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{2})))),
      BinaryOperation::kOr,
      BinaryExpressionExp(x, BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{3}))));

  Expression rewritten = rewriter.Rewrite(or_chain);
  ASSERT_EQ(rewritten->Type(), TypeTag::kInExp);
  const auto& in_exp = rewritten->AsInExpression();
  EXPECT_EQ(in_exp.child_->Type(), TypeTag::kColumnValue);
  EXPECT_EQ(in_exp.list_.size(), 3);

  // (1 = x OR 2 = x) -> x IN (1, 2)
  Expression reversed_eq = BinaryExpressionExp(
      BinaryExpressionExp(ConstantValueExp(Value(int64_t{1})), BinaryOperation::kEquals, x),
      BinaryOperation::kOr,
      BinaryExpressionExp(ConstantValueExp(Value(int64_t{2})), BinaryOperation::kEquals, x));
  Expression rewritten_rev = rewriter.Rewrite(reversed_eq);
  ASSERT_EQ(rewritten_rev->Type(), TypeTag::kInExp);
  EXPECT_EQ(rewritten_rev->AsInExpression().list_.size(), 2);

  // (x IN (1, 2) OR x = 3) -> x IN (1, 2, 3)
  Expression in_or_eq = BinaryExpressionExp(
      InExpressionExp(x, {ConstantValueExp(Value(int64_t{1})), ConstantValueExp(Value(int64_t{2}))}),
      BinaryOperation::kOr,
      BinaryExpressionExp(x, BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{3}))));
  Expression rewritten_in_or = rewriter.Rewrite(in_or_eq);
  ASSERT_EQ(rewritten_in_or->Type(), TypeTag::kInExp);
  EXPECT_EQ(rewritten_in_or->AsInExpression().list_.size(), 3);
}

TEST(ExpressionRewriteTest, IntervalNormalize) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  Expression iv1 = IntervalExpressionExp(1, "day");
  Expression iv2 = IntervalExpressionExp(2, "day");
  Expression iv5 = IntervalExpressionExp(5, "day");

  // INTERVAL '1' DAY + INTERVAL '2' DAY -> INTERVAL '3' DAY
  Expression add_iv = BinaryExpressionExp(iv1, BinaryOperation::kAdd, iv2);
  Expression rewritten_add = rewriter.Rewrite(add_iv);
  ASSERT_EQ(rewritten_add->Type(), TypeTag::kIntervalExp);
  EXPECT_EQ(rewritten_add->AsIntervalExpression().GetIntervalValue().days, 3);

  // INTERVAL '5' DAY - INTERVAL '2' DAY -> INTERVAL '3' DAY
  Expression sub_iv = BinaryExpressionExp(iv5, BinaryOperation::kSubtract, iv2);
  Expression rewritten_sub = rewriter.Rewrite(sub_iv);
  ASSERT_EQ(rewritten_sub->Type(), TypeTag::kIntervalExp);
  EXPECT_EQ(rewritten_sub->AsIntervalExpression().GetIntervalValue().days, 3);

  // INTERVAL '2' DAY * 3 -> INTERVAL '6' DAY
  Expression mul_iv = BinaryExpressionExp(
      iv2, BinaryOperation::kMultiply, ConstantValueExp(Value(int64_t{3})));
  Expression rewritten_mul = rewriter.Rewrite(mul_iv);
  ASSERT_EQ(rewritten_mul->Type(), TypeTag::kIntervalExp);
  EXPECT_EQ(rewritten_mul->AsIntervalExpression().GetIntervalValue().days, 6);

  // 3 * INTERVAL '2' DAY -> INTERVAL '6' DAY
  Expression mul_iv2 = BinaryExpressionExp(
      ConstantValueExp(Value(int64_t{3})), BinaryOperation::kMultiply, iv2);
  Expression rewritten_mul2 = rewriter.Rewrite(mul_iv2);
  ASSERT_EQ(rewritten_mul2->Type(), TypeTag::kIntervalExp);
  EXPECT_EQ(rewritten_mul2->AsIntervalExpression().GetIntervalValue().days, 6);

  // - INTERVAL '5' DAY -> INTERVAL '-5' DAY
  Expression neg_iv = UnaryExpressionExp(iv5, UnaryOperation::kMinus);
  Expression rewritten_neg = rewriter.Rewrite(neg_iv);
  ASSERT_EQ(rewritten_neg->Type(), TypeTag::kIntervalExp);
  EXPECT_EQ(rewritten_neg->AsIntervalExpression().GetIntervalValue().days, -5);

  // JUSTIFY_HOURS(INTERVAL '30' HOUR)
  Expression iv30h = IntervalExpressionExp(30, "hour");
  Expression justify_h = FunctionCallExp("justify_hours", {iv30h});
  Expression rewritten_jh = rewriter.Rewrite(justify_h);
  ASSERT_EQ(rewritten_jh->Type(), TypeTag::kIntervalExp);
  EXPECT_EQ(rewritten_jh->AsIntervalExpression().GetIntervalValue().days, 1);
  EXPECT_EQ(rewritten_jh->AsIntervalExpression().GetIntervalValue().nanos,
            6LL * 3600LL * 1000000000LL);

  // JUSTIFY_DAYS(INTERVAL '35' DAY)
  Expression iv35d = IntervalExpressionExp(35, "day");
  Expression justify_d = FunctionCallExp("justify_days", {iv35d});
  Expression rewritten_jd = rewriter.Rewrite(justify_d);
  ASSERT_EQ(rewritten_jd->Type(), TypeTag::kIntervalExp);
  EXPECT_EQ(rewritten_jd->AsIntervalExpression().GetIntervalValue().months, 1);
  EXPECT_EQ(rewritten_jd->AsIntervalExpression().GetIntervalValue().days, 5);

  // JUSTIFY_INTERVAL(INTERVAL '35' DAY + INTERVAL '30' HOUR)
  Expression iv_combined =
      BinaryExpressionExp(iv35d, BinaryOperation::kAdd, iv30h);
  Expression justify_all = FunctionCallExp("justify_interval", {iv_combined});
  Expression rewritten_ja = rewriter.Rewrite(justify_all);
  ASSERT_EQ(rewritten_ja->Type(), TypeTag::kIntervalExp);
  EXPECT_EQ(rewritten_ja->AsIntervalExpression().GetIntervalValue().months, 1);
  EXPECT_EQ(rewritten_ja->AsIntervalExpression().GetIntervalValue().days, 6);
  EXPECT_EQ(rewritten_ja->AsIntervalExpression().GetIntervalValue().nanos,
            6LL * 3600LL * 1000000000LL);
}

TEST(ExpressionRewriteTest, PredicatePushdownCase) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  Expression c1 = ColumnValueExp("c1");
  Expression v1 = ColumnValueExp("v1");
  Expression v2 = ColumnValueExp("v2");
  Expression case_exp = CaseExpressionExp({{c1, v1}}, v2);

  // Unary IS NULL pushdown
  Expression is_null = UnaryExpressionExp(case_exp, UnaryOperation::kIsNull);
  Expression rewritten_unary = rewriter.Rewrite(is_null);
  ASSERT_EQ(rewritten_unary->Type(), TypeTag::kCaseExp);
  const auto& case_unary = rewritten_unary->AsCaseExpression();
  ASSERT_EQ(case_unary.when_clauses_.size(), 1);
  EXPECT_EQ(case_unary.when_clauses_[0].second->Type(), TypeTag::kUnaryExp);
  EXPECT_EQ(case_unary.when_clauses_[0].second->AsUnaryExpression().Op(),
            UnaryOperation::kIsNull);
  EXPECT_EQ(case_unary.else_clause_->Type(), TypeTag::kUnaryExp);

  // Binary = pushdown
  Expression eq = BinaryExpressionExp(case_exp, BinaryOperation::kEquals,
                                      ConstantValueExp(Value(int64_t{10})));
  Expression rewritten_binary = rewriter.Rewrite(eq);
  ASSERT_EQ(rewritten_binary->Type(), TypeTag::kCaseExp);
  const auto& case_bin = rewritten_binary->AsCaseExpression();
  ASSERT_EQ(case_bin.when_clauses_.size(), 1);
  EXPECT_EQ(case_bin.when_clauses_[0].second->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(case_bin.when_clauses_[0].second->AsBinaryExpression().Op(),
            BinaryOperation::kEquals);

  // IN list pushdown
  Expression in = InExpressionExp(case_exp,
                                  {ConstantValueExp(Value(int64_t{1})),
                                   ConstantValueExp(Value(int64_t{2}))});
  Expression rewritten_in = rewriter.Rewrite(in);
  ASSERT_EQ(rewritten_in->Type(), TypeTag::kCaseExp);
  const auto& case_in = rewritten_in->AsCaseExpression();
  ASSERT_EQ(case_in.when_clauses_.size(), 1);
  EXPECT_EQ(case_in.when_clauses_[0].second->Type(), TypeTag::kInExp);

  // CAST pushdown
  Expression cast = CastExpressionExp(case_exp, "INT64");
  Expression rewritten_cast = rewriter.Rewrite(cast);
  ASSERT_EQ(rewritten_cast->Type(), TypeTag::kCaseExp);
  const auto& case_cast = rewritten_cast->AsCaseExpression();
  ASSERT_EQ(case_cast.when_clauses_.size(), 1);
  EXPECT_EQ(case_cast.when_clauses_[0].second->Type(), TypeTag::kCastExp);
}

TEST(ExpressionRewriteTest, InnerJoinNotNullInference) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  Expression x = ColumnValueExp("x");
  Expression y = ColumnValueExp("y");
  Expression a = ColumnValueExp("a");

  // (x = y AND a > 10) -> ((x = y AND a > 10) AND x IS NOT NULL) AND y IS NOT NULL
  Expression eq = BinaryExpressionExp(
      BinaryExpressionExp(x, BinaryOperation::kEquals, y),
      BinaryOperation::kAnd,
      BinaryExpressionExp(a, BinaryOperation::kGreaterThan, ConstantValueExp(Value(int64_t{10}))));
  Expression rewritten = rewriter.Rewrite(eq);
  std::vector<Expression> conjuncts = SplitConjuncts(rewritten);
  EXPECT_GE(conjuncts.size(), 4);

  bool has_eq = false;
  bool has_x_not_null = false;
  bool has_y_not_null = false;
  for (const auto& c : conjuncts) {
    if (c->Type() == TypeTag::kBinaryExp &&
        c->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
      has_eq = true;
    }
    if (c->Type() == TypeTag::kUnaryExp &&
        c->AsUnaryExpression().Op() == UnaryOperation::kIsNotNull) {
      if (c->AsUnaryExpression().Child()->ToString() == x->ToString()) {
        has_x_not_null = true;
      }
      if (c->AsUnaryExpression().Child()->ToString() == y->ToString()) {
        has_y_not_null = true;
      }
    }
  }
  EXPECT_TRUE(has_eq);
  EXPECT_TRUE(has_x_not_null);
  EXPECT_TRUE(has_y_not_null);
}

TEST(ExpressionRewriteTest, RegexpPrefixExtraction) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  Expression str = ColumnValueExp("str");

  // REGEXP_CONTAINS(str, '^abc.*') -> str LIKE 'abc%'
  Expression fn1 = FunctionCallExp(
      "regexp_contains", {str, ConstantValueExp(Value("^abc.*"))});
  Expression rewritten1 = rewriter.Rewrite(fn1);
  ASSERT_EQ(rewritten1->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten1->AsBinaryExpression().Op(), BinaryOperation::kLike);
  EXPECT_EQ(rewritten1->AsBinaryExpression().Right()->AsConstantValue().GetValue(),
            Value("abc%"));

  // REGEXP_LIKE(str, '^hello$') -> str = 'hello'
  Expression fn2 = FunctionCallExp(
      "regexp_like", {str, ConstantValueExp(Value("^hello$"))});
  Expression rewritten2 = rewriter.Rewrite(fn2);
  ASSERT_EQ(rewritten2->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten2->AsBinaryExpression().Op(), BinaryOperation::kEquals);
  EXPECT_EQ(rewritten2->AsBinaryExpression().Right()->AsConstantValue().GetValue(),
            Value("hello"));

  // REGEXP_MATCH(str, '^test') -> str LIKE 'test%'
  Expression fn3 = FunctionCallExp(
      "regexp_match", {str, ConstantValueExp(Value("^test"))});
  Expression rewritten3 = rewriter.Rewrite(fn3);
  ASSERT_EQ(rewritten3->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten3->AsBinaryExpression().Op(), BinaryOperation::kLike);
  EXPECT_EQ(rewritten3->AsBinaryExpression().Right()->AsConstantValue().GetValue(),
            Value("test%"));
}

TEST(ExpressionRewriteTest, CastPushdownComparison) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  Expression col = ColumnValueExp("col");

  // CAST(col AS DOUBLE) = 10.0 -> col = 10
  Expression cast_double = CastExpressionExp(col, "DOUBLE");
  Expression eq_int = BinaryExpressionExp(
      cast_double, BinaryOperation::kEquals, ConstantValueExp(Value(10.0)));
  Expression rewritten1 = rewriter.Rewrite(eq_int);
  ASSERT_EQ(rewritten1->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten1->AsBinaryExpression().Op(), BinaryOperation::kEquals);
  EXPECT_EQ(rewritten1->AsBinaryExpression().Right()->AsConstantValue().GetValue(),
            Value(int64_t{10}));

  // CAST(col AS DOUBLE) = 3.5 -> false (cannot equal fractional float)
  Expression eq_fraction = BinaryExpressionExp(
      cast_double, BinaryOperation::kEquals, ConstantValueExp(Value(3.5)));
  Expression rewritten2 = rewriter.Rewrite(eq_fraction);
  ASSERT_EQ(rewritten2->Type(), TypeTag::kConstantValue);
  EXPECT_FALSE(rewritten2->AsConstantValue().GetValue().Truthy());

  // CAST(col AS DOUBLE) < 3.5 -> col <= 3
  Expression lt_fraction = BinaryExpressionExp(
      cast_double, BinaryOperation::kLessThan, ConstantValueExp(Value(3.5)));
  Expression rewritten3 = rewriter.Rewrite(lt_fraction);
  ASSERT_EQ(rewritten3->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten3->AsBinaryExpression().Op(),
            BinaryOperation::kLessThanEquals);
  EXPECT_EQ(rewritten3->AsBinaryExpression().Right()->AsConstantValue().GetValue(),
            Value(int64_t{3}));

  // CAST(col AS DOUBLE) > 3.5 -> col >= 4
  Expression gt_fraction = BinaryExpressionExp(
      cast_double, BinaryOperation::kGreaterThan, ConstantValueExp(Value(3.5)));
  Expression rewritten4 = rewriter.Rewrite(gt_fraction);
  ASSERT_EQ(rewritten4->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten4->AsBinaryExpression().Op(),
            BinaryOperation::kGreaterThanEquals);
  EXPECT_EQ(rewritten4->AsBinaryExpression().Right()->AsConstantValue().GetValue(),
            Value(int64_t{4}));

  // CAST(col AS INT64) = 42 -> col = 42
  Expression cast_int = CastExpressionExp(col, "INT64");
  Expression eq_cast_int = BinaryExpressionExp(
      cast_int, BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{42})));
  Expression rewritten5 = rewriter.Rewrite(eq_cast_int);
  ASSERT_EQ(rewritten5->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten5->AsBinaryExpression().Op(), BinaryOperation::kEquals);
  EXPECT_EQ(rewritten5->AsBinaryExpression().Right()->AsConstantValue().GetValue(),
            Value(int64_t{42}));
}

TEST(ExpressionRewriteTest, DeterministicFunctionCse) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  Expression x = ColumnValueExp("x");
  Expression abs_x = FunctionCallExp("abs", {x});
  Expression upper_x = FunctionCallExp("upper", {x});
  Expression sqrt_x = FunctionCallExp("sqrt", {x});

  // f(x) - f(x) -> 0
  Expression sub_self = BinaryExpressionExp(abs_x, BinaryOperation::kSubtract, abs_x);
  Expression rewritten_sub = rewriter.Rewrite(sub_self);
  ASSERT_EQ(rewritten_sub->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_sub->AsConstantValue().GetValue(), Value(int64_t{0}));

  // f(x) = f(x) -> f(x) IS NOT NULL
  Expression eq_self = BinaryExpressionExp(upper_x, BinaryOperation::kEquals, upper_x);
  Expression rewritten_eq = rewriter.Rewrite(eq_self);
  ASSERT_EQ(rewritten_eq->Type(), TypeTag::kUnaryExp);
  EXPECT_EQ(rewritten_eq->AsUnaryExpression().Op(), UnaryOperation::kIsNotNull);

  // f(x) / f(x) -> 1
  Expression div_self = BinaryExpressionExp(sqrt_x, BinaryOperation::kDivide, sqrt_x);
  Expression rewritten_div = rewriter.Rewrite(div_self);
  ASSERT_EQ(rewritten_div->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_div->AsConstantValue().GetValue(), Value(int64_t{1}));

  // COALESCE(upper(x), upper(x)) -> upper(x)
  Expression coalesce_dup = FunctionCallExp("coalesce", {upper_x, upper_x});
  Expression rewritten_coalesce = rewriter.Rewrite(coalesce_dup);
  ASSERT_EQ(rewritten_coalesce->Type(), TypeTag::kFunctionCallExp);
  EXPECT_EQ(rewritten_coalesce->AsFunctionCallExpression().FuncName(), "upper");

  // NULLIF(upper(x), upper(x)) -> NULL
  Expression nullif_dup = FunctionCallExp("nullif", {upper_x, upper_x});
  Expression rewritten_nullif = rewriter.Rewrite(nullif_dup);
  ASSERT_EQ(rewritten_nullif->Type(), TypeTag::kConstantValue);
  EXPECT_TRUE(rewritten_nullif->AsConstantValue().GetValue().IsNull());

  // CASE WHEN c THEN upper(x) ELSE upper(x) END -> upper(x)
  Expression case_dup = CaseExpressionExp({{ColumnValueExp("c"), upper_x}}, upper_x);
  Expression rewritten_case = rewriter.Rewrite(case_dup);
  ASSERT_EQ(rewritten_case->Type(), TypeTag::kFunctionCallExp);
  EXPECT_EQ(rewritten_case->AsFunctionCallExpression().FuncName(), "upper");

  // Volatile rand() - rand() must NOT be eliminated to 0
  Expression rand_call = FunctionCallExp("rand", {});
  Expression sub_rand = BinaryExpressionExp(rand_call, BinaryOperation::kSubtract, rand_call);
  Expression rewritten_rand = rewriter.Rewrite(sub_rand);
  ASSERT_EQ(rewritten_rand->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten_rand->AsBinaryExpression().Op(), BinaryOperation::kSubtract);
}

TEST(ExpressionRewriteTest, FunctionVolatilityClassification) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  EXPECT_EQ(GetFunctionVolatility("rand"), Volatility::kVolatile);
  EXPECT_EQ(GetFunctionVolatility("random"), Volatility::kVolatile);
  EXPECT_EQ(GetFunctionVolatility("uuid"), Volatility::kVolatile);
  EXPECT_EQ(GetFunctionVolatility("generate_uuid"), Volatility::kVolatile);
  EXPECT_EQ(GetFunctionVolatility("now"), Volatility::kStable);
  EXPECT_EQ(GetFunctionVolatility("current_timestamp"), Volatility::kStable);
  EXPECT_EQ(GetFunctionVolatility("current_date"), Volatility::kStable);
  EXPECT_EQ(GetFunctionVolatility("upper"), Volatility::kImmutable);
  EXPECT_EQ(GetFunctionVolatility("abs"), Volatility::kImmutable);
  EXPECT_EQ(GetFunctionVolatility("sqrt"), Volatility::kImmutable);

  // Volatile / stable functions with 0 args are NOT constant folded at compile time
  Expression rand_exp = FunctionCallExp("rand", {});
  Expression rewritten_rand = rewriter.Rewrite(rand_exp);
  ASSERT_EQ(rewritten_rand->Type(), TypeTag::kFunctionCallExp);
  EXPECT_EQ(rewritten_rand->AsFunctionCallExpression().FuncName(), "rand");

  Expression now_exp = FunctionCallExp("now", {});
  Expression rewritten_now = rewriter.Rewrite(now_exp);
  ASSERT_EQ(rewritten_now->Type(), TypeTag::kFunctionCallExp);
  EXPECT_EQ(rewritten_now->AsFunctionCallExpression().FuncName(), "now");

  // Immutable functions with constant args ARE constant folded
  Expression upper_const = FunctionCallExp("upper", {ConstantValueExp(Value("abc"))});
  Expression rewritten_upper = rewriter.Rewrite(upper_const);
  ASSERT_EQ(rewritten_upper->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_upper->AsConstantValue().GetValue(), Value("ABC"));
}

TEST(ExpressionRewriteTest, BooleanFilterPullup) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  Expression a = ColumnValueExp("a");
  Expression b = ColumnValueExp("b");
  Expression c = ColumnValueExp("c");

  // (a = 1 AND b = 2) OR (a = 1 AND c = 3) -> (a = 1) AND ((b = 2) OR (c = 3))
  Expression a_eq_1 = BinaryExpressionExp(a, BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{1})));
  Expression b_eq_2 = BinaryExpressionExp(b, BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{2})));
  Expression c_eq_3 = BinaryExpressionExp(c, BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{3})));

  Expression disj = BinaryExpressionExp(
      BinaryExpressionExp(a_eq_1, BinaryOperation::kAnd, b_eq_2),
      BinaryOperation::kOr,
      BinaryExpressionExp(a_eq_1, BinaryOperation::kAnd, c_eq_3));

  Expression rewritten = rewriter.Rewrite(disj);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kAnd);

  // Absorption: (a = 1 AND b = 2) OR (a = 1) -> a = 1
  Expression absorb = BinaryExpressionExp(
      BinaryExpressionExp(a_eq_1, BinaryOperation::kAnd, b_eq_2),
      BinaryOperation::kOr,
      a_eq_1);
  Expression rewritten_absorb = rewriter.Rewrite(absorb);
  ASSERT_EQ(rewritten_absorb->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten_absorb->AsBinaryExpression().Op(), BinaryOperation::kEquals);
}

TEST(ExpressionRewriteTest, NotInNullSemantics) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  Expression x = ColumnValueExp("x");
  Expression null_val = ConstantValueExp(Value());
  Expression val_1 = ConstantValueExp(Value(int64_t{1}));
  Expression val_2 = ConstantValueExp(Value(int64_t{2}));

  // x NOT IN (1, NULL) -> CASE WHEN x = 1 THEN false ELSE NULL END
  Expression not_in_1_null = UnaryExpressionExp(
      InExpressionExp(x, {val_1, null_val}), UnaryOperation::kNot);
  Expression rewritten_not_in = rewriter.Rewrite(not_in_1_null);
  ASSERT_EQ(rewritten_not_in->Type(), TypeTag::kCaseExp);

  // Evaluate x=1 -> false
  Row row1({Value(int64_t{1})});
  Schema schema("t", {Column("x", ValueType::kInt64)});
  EXPECT_EQ(rewritten_not_in->Evaluate(row1, schema), Value(false));

  // Evaluate x=2 -> NULL
  Row row2({Value(int64_t{2})});
  EXPECT_TRUE(rewritten_not_in->Evaluate(row2, schema).IsNull());

  // Evaluate x=NULL -> NULL
  Row row_null({Value()});
  EXPECT_TRUE(rewritten_not_in->Evaluate(row_null, schema).IsNull());

  // x NOT IN (1, 2, NULL) -> CASE WHEN x IN (1, 2) THEN false ELSE NULL END
  Expression not_in_multi_null = UnaryExpressionExp(
      InExpressionExp(x, {val_1, val_2, null_val}), UnaryOperation::kNot);
  Expression rewritten_multi = rewriter.Rewrite(not_in_multi_null);
  ASSERT_EQ(rewritten_multi->Type(), TypeTag::kCaseExp);
  EXPECT_EQ(rewritten_multi->Evaluate(row1, schema), Value(false));
  EXPECT_EQ(rewritten_multi->Evaluate(row2, schema), Value(false));
  Row row3({Value(int64_t{3})});
  EXPECT_TRUE(rewritten_multi->Evaluate(row3, schema).IsNull());

  // x NOT IN (NULL) -> NULL
  Expression not_in_only_null = UnaryExpressionExp(
      InExpressionExp(x, {null_val}), UnaryOperation::kNot);
  Expression rewritten_only_null = rewriter.Rewrite(not_in_only_null);
  ASSERT_EQ(rewritten_only_null->Type(), TypeTag::kConstantValue);
  EXPECT_TRUE(rewritten_only_null->AsConstantValue().GetValue().IsNull());
}

TEST(ExpressionRewriteTest, ArrayFlattenOptimization) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  Expression a = ColumnValueExp("a");
  Expression b = ColumnValueExp("b");
  Expression c = ColumnValueExp("c");

  // array_concat(array_concat(a, b), c) -> array_concat(a, b, c)
  Expression nested_concat = FunctionCallExp(
      "array_concat", {FunctionCallExp("array_concat", {a, b}), c});
  Expression rewritten_concat = rewriter.Rewrite(nested_concat);
  ASSERT_EQ(rewritten_concat->Type(), TypeTag::kFunctionCallExp);
  EXPECT_EQ(rewritten_concat->AsFunctionCallExpression().FuncName(), "array_concat");
  EXPECT_EQ(rewritten_concat->AsFunctionCallExpression().Args().size(), 3);

  // array_concat([1, 2], [3, 4]) -> [1, 2, 3, 4]
  Expression arr1 = ArrayExpressionExp(
      {ConstantValueExp(Value(int64_t{1})), ConstantValueExp(Value(int64_t{2}))},
      "INT64");
  Expression arr2 = ArrayExpressionExp(
      {ConstantValueExp(Value(int64_t{3})), ConstantValueExp(Value(int64_t{4}))},
      "INT64");
  Expression concat_arrays = FunctionCallExp("array_concat", {arr1, arr2});
  Expression rewritten_arrays = rewriter.Rewrite(concat_arrays);
  ASSERT_EQ(rewritten_arrays->Type(), TypeTag::kArrayExp);
  EXPECT_EQ(rewritten_arrays->AsArrayExpression().Elements().size(), 4);

  // array_concat(a, []) -> a
  Expression empty_arr = ArrayExpressionExp({}, "INT64");
  Expression concat_empty = FunctionCallExp("array_concat", {a, empty_arr});
  Expression rewritten_empty = rewriter.Rewrite(concat_empty);
  EXPECT_EQ(rewritten_empty->Type(), TypeTag::kColumnValue);

  // array_flatten([[1, 2], [3, 4]]) -> [1, 2, 3, 4]
  Expression nested_array = ArrayExpressionExp({arr1, arr2}, "ARRAY<INT64>");
  Expression flatten = FunctionCallExp("array_flatten", {nested_array});
  Expression rewritten_flatten = rewriter.Rewrite(flatten);
  ASSERT_EQ(rewritten_flatten->Type(), TypeTag::kArrayExp);
  EXPECT_EQ(rewritten_flatten->AsArrayExpression().Elements().size(), 4);

  // array_flatten([]) -> []
  Expression flatten_empty = FunctionCallExp("array_flatten", {empty_arr});
  Expression rewritten_flatten_empty = rewriter.Rewrite(flatten_empty);
  ASSERT_EQ(rewritten_flatten_empty->Type(), TypeTag::kArrayExp);
  EXPECT_TRUE(rewritten_flatten_empty->AsArrayExpression().Elements().empty());
}

TEST(ExpressionRewriteTest, DatetimeAndStringFoldExtent) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  // DATE_ADD(DATE '2026-08-27', INTERVAL 3 DAY) -> DATE '2026-08-30'
  Expression date_val = ConstantValueExp(Value(std::string("2026-08-27")));
  Expression interval_3d = IntervalExpressionExp(3, "DAY");
  Expression date_add = FunctionCallExp("date_add", {date_val, interval_3d});
  Expression rewritten_date_add = rewriter.Rewrite(date_add);
  ASSERT_EQ(rewritten_date_add->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_date_add->AsConstantValue().GetValue(), Value(std::string("2026-08-30")));

  // DATE_SUB(DATE '2026-08-27', INTERVAL 2 DAY) -> DATE '2026-08-25'
  Expression interval_2d = IntervalExpressionExp(2, "DAY");
  Expression date_sub = FunctionCallExp("date_sub", {date_val, interval_2d});
  Expression rewritten_date_sub = rewriter.Rewrite(date_sub);
  ASSERT_EQ(rewritten_date_sub->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_date_sub->AsConstantValue().GetValue(), Value(std::string("2026-08-25")));

  // SUBSTRING('Hello World', 1, 5) -> 'Hello'
  Expression str_hello_world = ConstantValueExp(Value(std::string("Hello World")));
  Expression substr = FunctionCallExp(
      "substring", {str_hello_world, ConstantValueExp(Value(int64_t{1})),
                    ConstantValueExp(Value(int64_t{5}))});
  Expression rewritten_substr = rewriter.Rewrite(substr);
  ASSERT_EQ(rewritten_substr->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_substr->AsConstantValue().GetValue(), Value(std::string("Hello")));

  // INSTR('abcdef', 'cd') -> 3
  Expression instr = FunctionCallExp(
      "instr", {ConstantValueExp(Value(std::string("abcdef"))),
                ConstantValueExp(Value(std::string("cd")))});
  Expression rewritten_instr = rewriter.Rewrite(instr);
  ASSERT_EQ(rewritten_instr->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_instr->AsConstantValue().GetValue(), Value(int64_t{3}));

  // LPAD('42', 5, '0') -> '00042'
  Expression lpad = FunctionCallExp(
      "lpad", {ConstantValueExp(Value(std::string("42"))),
               ConstantValueExp(Value(int64_t{5})),
               ConstantValueExp(Value(std::string("0")))});
  Expression rewritten_lpad = rewriter.Rewrite(lpad);
  ASSERT_EQ(rewritten_lpad->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_lpad->AsConstantValue().GetValue(), Value(std::string("00042")));

  // RPAD('hi', 5, '!') -> 'hi!!!'
  Expression rpad = FunctionCallExp(
      "rpad", {ConstantValueExp(Value(std::string("hi"))),
               ConstantValueExp(Value(int64_t{5})),
               ConstantValueExp(Value(std::string("!")))});
  Expression rewritten_rpad = rewriter.Rewrite(rpad);
  ASSERT_EQ(rewritten_rpad->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_rpad->AsConstantValue().GetValue(), Value(std::string("hi!!!")));

  // CONCAT('foo', 'bar', 'baz') -> 'foobarbaz'
  Expression concat = FunctionCallExp(
      "concat", {ConstantValueExp(Value(std::string("foo"))),
                 ConstantValueExp(Value(std::string("bar"))),
                 ConstantValueExp(Value(std::string("baz")))});
  Expression rewritten_concat = rewriter.Rewrite(concat);
  ASSERT_EQ(rewritten_concat->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_concat->AsConstantValue().GetValue(), Value(std::string("foobarbaz")));

  // LENGTH('tinylamb') -> 8
  Expression len = FunctionCallExp(
      "length", {ConstantValueExp(Value(std::string("tinylamb")))});
  Expression rewritten_len = rewriter.Rewrite(len);
  ASSERT_EQ(rewritten_len->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_len->AsConstantValue().GetValue(), Value(int64_t{8}));
}

TEST(ExpressionRewriteTest, CoalesceAndNullifSimplification) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  Expression x = ColumnValueExp("x");
  Expression y = ColumnValueExp("y");
  Expression null_val = ConstantValueExp(Value());
  Expression const_42 = ConstantValueExp(Value(int64_t{42}));
  Expression const_99 = ConstantValueExp(Value(int64_t{99}));

  // COALESCE(NULL, 42, x) -> 42
  Expression c1 = FunctionCallExp("coalesce", {null_val, const_42, x});
  Expression rewritten_c1 = rewriter.Rewrite(c1);
  ASSERT_EQ(rewritten_c1->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_c1->AsConstantValue().GetValue(), Value(int64_t{42}));

  // COALESCE(x, 42, y) -> COALESCE(x, 42) (y is eliminated after first non-null constant)
  Expression c2 = FunctionCallExp("coalesce", {x, const_42, y});
  Expression rewritten_c2 = rewriter.Rewrite(c2);
  ASSERT_EQ(rewritten_c2->Type(), TypeTag::kFunctionCallExp);
  EXPECT_EQ(rewritten_c2->AsFunctionCallExpression().Args().size(), 2);
  EXPECT_EQ(rewritten_c2->AsFunctionCallExpression().Args()[0]->Type(), TypeTag::kColumnValue);
  EXPECT_EQ(rewritten_c2->AsFunctionCallExpression().Args()[1]->Type(), TypeTag::kConstantValue);

  // COALESCE(x, y, NULL) -> COALESCE(x, y) (trailing NULL eliminated)
  Expression c3 = FunctionCallExp("coalesce", {x, y, null_val});
  Expression rewritten_c3 = rewriter.Rewrite(c3);
  ASSERT_EQ(rewritten_c3->Type(), TypeTag::kFunctionCallExp);
  EXPECT_EQ(rewritten_c3->AsFunctionCallExpression().Args().size(), 2);

  // COALESCE(x, NULL) -> x (single non-null arg remaining)
  Expression c4 = FunctionCallExp("coalesce", {x, null_val});
  Expression rewritten_c4 = rewriter.Rewrite(c4);
  EXPECT_EQ(rewritten_c4->Type(), TypeTag::kColumnValue);

  // COALESCE(NULL, NULL) -> NULL
  Expression c5 = FunctionCallExp("coalesce", {null_val, null_val});
  Expression rewritten_c5 = rewriter.Rewrite(c5);
  ASSERT_EQ(rewritten_c5->Type(), TypeTag::kConstantValue);
  EXPECT_TRUE(rewritten_c5->AsConstantValue().GetValue().IsNull());

  // NULLIF(x, x) -> NULL
  Expression n1 = FunctionCallExp("nullif", {x, x});
  Expression rewritten_n1 = rewriter.Rewrite(n1);
  ASSERT_EQ(rewritten_n1->Type(), TypeTag::kConstantValue);
  EXPECT_TRUE(rewritten_n1->AsConstantValue().GetValue().IsNull());

  // NULLIF(42, 42) -> NULL
  Expression n2 = FunctionCallExp("nullif", {const_42, const_42});
  Expression rewritten_n2 = rewriter.Rewrite(n2);
  ASSERT_EQ(rewritten_n2->Type(), TypeTag::kConstantValue);
  EXPECT_TRUE(rewritten_n2->AsConstantValue().GetValue().IsNull());

  // NULLIF(42, 99) -> 42
  Expression n3 = FunctionCallExp("nullif", {const_42, const_99});
  Expression rewritten_n3 = rewriter.Rewrite(n3);
  ASSERT_EQ(rewritten_n3->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_n3->AsConstantValue().GetValue(), Value(int64_t{42}));

  // NULLIF(x, NULL) -> x
  Expression n4 = FunctionCallExp("nullif", {x, null_val});
  Expression rewritten_n4 = rewriter.Rewrite(n4);
  EXPECT_EQ(rewritten_n4->Type(), TypeTag::kColumnValue);
}

}  // namespace tinylamb
