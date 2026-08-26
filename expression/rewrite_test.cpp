/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/rewrite.hpp"

#include <memory>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "gtest/gtest.h"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {

TEST(ExpressionRewriteTest,
     Rewrite_DefaultRules_FoldsAndNormalizesRecursively) {
  Expression expression = BinaryExpressionExp(
      ConstantValueExp(Value(10)), BinaryOperation::kLessThan,
      BinaryExpressionExp(ConstantValueExp(Value(1)), BinaryOperation::kAdd,
                          ConstantValueExp(Value(2))));

  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);

  ASSERT_EQ(rewritten->Type(), TypeTag::kConstantValue);
  EXPECT_FALSE(rewritten->AsConstantValue().GetValue().Truthy());
}

TEST(ExpressionRewriteTest, Rewrite_PullsConstantOffsetOutOfComparison) {
  const Expression expression = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kAdd,
                          ConstantValueExp(Value(1))),
      BinaryOperation::kGreaterThanEquals, ConstantValueExp(Value(5)));

  const Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);

  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  const auto& comparison = rewritten->AsBinaryExpression();
  EXPECT_EQ(comparison.Op(), BinaryOperation::kGreaterThanEquals);
  EXPECT_EQ(comparison.Left()->ToString(), "k");
  ASSERT_EQ(comparison.Right()->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(comparison.Right()->AsConstantValue().GetValue(), Value(4));
}

TEST(ExpressionRewriteTest, Rewrite_SimplifiesSafeScalarFunctionShapes) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());
  const Expression x = ColumnValueExp("x");

  const Expression nullif = rewriter.Rewrite(
      FunctionCallExp("nullif", {x, ConstantValueExp(Value(0))}));
  EXPECT_EQ(nullif->ToString().find("nullif"), std::string::npos);

  const Expression abs =
      rewriter.Rewrite(FunctionCallExp("abs", {FunctionCallExp("abs", {x})}));
  ASSERT_EQ(abs->Type(), TypeTag::kFunctionCallExp);
  EXPECT_EQ(abs->AsFunctionCallExpression().Args().size(), 1U);
  EXPECT_EQ(abs->AsFunctionCallExpression().Args()[0]->ToString(), "x");

  const Expression greatest =
      rewriter.Rewrite(FunctionCallExp("greatest", {x}));
  EXPECT_EQ(greatest->ToString(), "x");
}

// IS [NOT] DISTINCT FROM over a constant collapses into NULL tests and
// plain comparisons while preserving three-valued semantics end-to-end.
TEST(ExpressionRewriteTest, Rewrite_IsDistinctFromConstantCanonicalizes) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());
  const Expression x = ColumnValueExp("x");

  const Expression distinct_null = rewriter.Rewrite(
      FunctionCallExp("__is_distinct_from", {x, ConstantValueExp(Value())}));
  ASSERT_EQ(distinct_null->Type(), TypeTag::kUnaryExp);
  EXPECT_EQ(distinct_null->AsUnaryExpression().Op(),
            UnaryOperation::kIsNotNull);

  const Expression not_distinct_null = rewriter.Rewrite(UnaryExpressionExp(
      FunctionCallExp("__is_distinct_from", {x, ConstantValueExp(Value())}),
      UnaryOperation::kNot));
  ASSERT_EQ(not_distinct_null->Type(), TypeTag::kUnaryExp);
  EXPECT_EQ(not_distinct_null->AsUnaryExpression().Op(),
            UnaryOperation::kIsNull);

  const Expression distinct_const = rewriter.Rewrite(
      FunctionCallExp("__is_distinct_from", {x, ConstantValueExp(Value(7))}));
  // x IS NULL OR x <> 7
  ASSERT_EQ(distinct_const->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(distinct_const->AsBinaryExpression().Op(), BinaryOperation::kOr);

  const Schema schema("t", {Column("x", ValueType::kInt64)});
  EXPECT_EQ(distinct_const->Evaluate(Row({Value()}), schema), Value(true));
  EXPECT_EQ(distinct_const->Evaluate(Row({Value(7)}), schema), Value(false));
  EXPECT_EQ(distinct_const->Evaluate(Row({Value(3)}), schema), Value(true));

  const Expression not_distinct_const = rewriter.Rewrite(UnaryExpressionExp(
      FunctionCallExp("__is_distinct_from", {x, ConstantValueExp(Value(7))}),
      UnaryOperation::kNot));
  // x IS NOT NULL AND x = 7
  ASSERT_EQ(not_distinct_const->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(not_distinct_const->AsBinaryExpression().Op(),
            BinaryOperation::kAnd);
  EXPECT_EQ(not_distinct_const->Evaluate(Row({Value()}), schema), Value(false));
  EXPECT_EQ(not_distinct_const->Evaluate(Row({Value(7)}), schema), Value(true));
  EXPECT_EQ(not_distinct_const->Evaluate(Row({Value(3)}), schema),
            Value(false));
}

// SAFE_DIVIDE against a constant zero is NULL for every input, so the
// rewrite pins the result and drops the runtime guard.
TEST(ExpressionRewriteTest, Rewrite_SafeDivideByConstantZeroIsNull) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());
  const Expression x = ColumnValueExp("x");

  const Expression int_zero = rewriter.Rewrite(
      FunctionCallExp("safe_divide", {x, ConstantValueExp(Value(0))}));
  ASSERT_EQ(int_zero->Type(), TypeTag::kConstantValue);
  EXPECT_TRUE(int_zero->AsConstantValue().GetValue().IsNull());

  const Expression double_zero = rewriter.Rewrite(
      FunctionCallExp("safe_divide", {x, ConstantValueExp(Value(0.0))}));
  ASSERT_EQ(double_zero->Type(), TypeTag::kConstantValue);
  EXPECT_TRUE(double_zero->AsConstantValue().GetValue().IsNull());

  // Non-constant divisors keep the SAFE_DIVIDE call untouched.
  const Expression dynamic = rewriter.Rewrite(
      FunctionCallExp("safe_divide", {x, ColumnValueExp("y")}));
  ASSERT_EQ(dynamic->Type(), TypeTag::kFunctionCallExp);
  EXPECT_EQ(dynamic->AsFunctionCallExpression().FuncName(), "safe_divide");
}

TEST(ExpressionRewriteTest, Rewrite_XorExpansionPreservesThreeValuedLogic) {
  const Expression expression = BinaryExpressionExp(
      ColumnValueExp("a"), BinaryOperation::kXor, ColumnValueExp("b"));
  const Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kAnd);

  const Schema schema(
      "t", {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64)});
  EXPECT_EQ(rewritten->Evaluate(Row({Value(), Value(false)}), schema), Value());
  EXPECT_EQ(rewritten->Evaluate(Row({Value(true), Value(false)}), schema),
            Value(true));
}

TEST(ExpressionRewriteTest, Rewrite_FoldsConstantArrayConstructor) {
  Expression array = ArrayExpressionExp(
      {ConstantValueExp(Value(1)), ConstantValueExp(Value(2))}, "INT64");
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(array);
  ASSERT_EQ(rewritten->Type(), TypeTag::kConstantValue);
  const Value value = rewritten->AsConstantValue().GetValue();
  ASSERT_TRUE(value.IsArray());
  ASSERT_EQ(value.ArrayElements().size(), 2U);
  EXPECT_EQ(value.ArrayElements()[0], Value(1));
  EXPECT_EQ(value.ArrayElements()[1], Value(2));
}

TEST(ExpressionRewriteTest, Rewrite_FlattensNestedCoalesceWithoutReordering) {
  Expression expression = FunctionCallExp(
      "COALESCE",
      {FunctionCallExp("COALESCE", {ColumnValueExp("a"), ColumnValueExp("b")}),
       ColumnValueExp("c")});
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  ASSERT_EQ(rewritten->Type(), TypeTag::kFunctionCallExp);
  const auto& call = rewritten->AsFunctionCallExpression();
  ASSERT_EQ(call.FuncName(), "coalesce");
  ASSERT_EQ(call.Args().size(), 3U);
  EXPECT_EQ(call.Args()[0]->ToString(), "a");
  EXPECT_EQ(call.Args()[1]->ToString(), "b");
  EXPECT_EQ(call.Args()[2]->ToString(), "c");
}

TEST(ExpressionRewriteTest, Rewrite_SimplifiesCoalesceNullTestsForLiterals) {
  const Expression coalesce = FunctionCallExp(
      "coalesce", {ConstantValueExp(Value(42)), ColumnValueExp("later")});
  const Expression is_null =
      UnaryExpressionExp(coalesce, UnaryOperation::kIsNull);
  const Expression is_not_null =
      UnaryExpressionExp(coalesce, UnaryOperation::kIsNotNull);
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());

  const Expression null_result = rewriter.Rewrite(is_null);
  const Expression not_null_result = rewriter.Rewrite(is_not_null);
  ASSERT_EQ(null_result->Type(), TypeTag::kConstantValue);
  ASSERT_EQ(not_null_result->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(null_result->AsConstantValue().GetValue(), Value(false));
  EXPECT_EQ(not_null_result->AsConstantValue().GetValue(), Value(true));
}

TEST(ExpressionRewriteTest, Rewrite_FoldsComparisonWithNullToUnknown) {
  const Expression expression =
      BinaryExpressionExp(ColumnValueExp("value"), BinaryOperation::kEquals,
                          ConstantValueExp(Value()));
  const Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  ASSERT_EQ(rewritten->Type(), TypeTag::kConstantValue);
  EXPECT_TRUE(rewritten->AsConstantValue().GetValue().IsNull());
}

TEST(ExpressionRewriteTest, Remove_SingleRule_PreservesOtherRules) {
  ExpressionRuleSet rules = ExpressionRuleSet::Default();
  ASSERT_TRUE(rules.Remove("fold_binary"));
  EXPECT_TRUE(rules.Contains("canonicalize_comparison"));
  Expression expression =
      BinaryExpressionExp(ConstantValueExp(Value(10)),
                          BinaryOperation::kLessThan, ColumnValueExp("value"));

  Expression rewritten = ExpressionRewriter(rules).Rewrite(expression);

  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(),
            BinaryOperation::kGreaterThan);
  EXPECT_EQ(rewritten->AsBinaryExpression().Left()->Type(),
            TypeTag::kColumnValue);
}

TEST(ExpressionRewriteTest, Rewrite_CustomDslRule_TransformsExpression) {
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

  Expression rewritten = ExpressionRewriter(rules).Rewrite(
      BinaryExpressionExp(ColumnValueExp("old"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(1))));

  EXPECT_EQ(rewritten->AsBinaryExpression().Left()->AsColumnValue().GetName(),
            "new");
}

TEST(ExpressionRewriteTest,
     SplitConjuncts_AndCombineConjuncts_PreservesConjuncts) {
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

TEST(ExpressionRewriteTest, Rewrite_InAndCaseExpressions_FoldsAndSimplifies) {
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

TEST(ExpressionRewriteTest, Match_MatchingPattern_BindsNodesSuccessfully) {
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

TEST(ExpressionRewriteTest, Match_NonMatchingPattern_ReturnsFalse) {
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

TEST(ExpressionRewriteTest, Match_ConflictingCaptures_RejectsMatch) {
  using namespace expression_dsl;
  Expression a = ColumnValueExp("a");
  Expression b = ColumnValueExp("b");
  Expression same = BinaryExpressionExp(a, BinaryOperation::kEquals, a);
  Expression different = BinaryExpressionExp(a, BinaryOperation::kEquals, b);
  ExpressionBindings bindings;
  ExpressionPattern pattern = AnyBinary(Any("x"), Any("x"));
  EXPECT_TRUE(pattern.Match(same, &bindings));
  bindings.clear();
  EXPECT_FALSE(pattern.Match(different, &bindings));
}

TEST(ExpressionRewriteTest, Apply_WhenPatternDoesNotMatch_ReturnsNull) {
  using namespace expression_dsl;
  ExpressionRule rule("only_in", Is(TypeTag::kInExp),
                      [](const Expression&, const ExpressionBindings&) {
                        return ConstantValueExp(Value(1));
                      });
  EXPECT_EQ(rule.Apply(ConstantValueExp(Value(2))).get(), nullptr);
  EXPECT_NE(rule.Apply(InExpressionExp(ConstantValueExp(Value(1)),
                                       {ConstantValueExp(Value(1))}))
                .get(),
            nullptr);
}

TEST(ExpressionRewriteTest, AddAndRemove_CustomRules_ManagesRuleSet) {
  using namespace expression_dsl;
  ExpressionRuleSet rules;
  EXPECT_FALSE(rules.Contains("x"));
  rules.Add(ExpressionRule("x", Any(),
                           [](const Expression&, const ExpressionBindings&) {
                             return Expression{};
                           }));
  EXPECT_TRUE(rules.Contains("x"));
  EXPECT_TRUE(rules.Remove("x"));
  EXPECT_FALSE(rules.Remove("x"));
  EXPECT_FALSE(rules.Contains("x"));
}

TEST(ExpressionRewriteTest, Rewrite_UnaryAndInConstants_FoldsToConstants) {
  Expression unary =
      UnaryExpressionExp(ConstantValueExp(Value(5)), UnaryOperation::kMinus);
  Expression rewritten_unary =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(unary);
  ASSERT_EQ(rewritten_unary->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten_unary->AsConstantValue().GetValue(), Value(-5));

  Expression in =
      InExpressionExp(ConstantValueExp(Value(1)),
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

TEST(ExpressionRewriteTest, Rewrite_ConstantOnLeft_CanonicalizesComparison) {
  // Constant on the right is already canonical.
  Expression expr =
      BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kLessThan,
                          ConstantValueExp(Value(3)));
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expr);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kLessThan);

  // Constant on the left is moved to the right with the operator flipped.
  Expression reversed =
      BinaryExpressionExp(ConstantValueExp(Value(3)),
                          BinaryOperation::kLessThan, ColumnValueExp("v"));
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

TEST(ExpressionRewriteTest, Rewrite_BooleanIdentities_SimplifiesExpressions) {
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

TEST(ExpressionRewriteTest, Rewrite_SameColumnEqualityBecomesNullCheck) {
  const Expression expression = BinaryExpressionExp(
      ColumnValueExp("x"), BinaryOperation::kEquals, ColumnValueExp("x"));
  const Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  ASSERT_EQ(rewritten->Type(), TypeTag::kUnaryExp);
  EXPECT_EQ(rewritten->AsUnaryExpression().Op(), UnaryOperation::kIsNotNull);

  Schema schema("test", {Column("x", ValueType::kInt64)});
  EXPECT_EQ(rewritten->Evaluate(Row({Value(7)}), schema), Value(true));
  EXPECT_EQ(rewritten->Evaluate(Row({Value()}), schema), Value(false));

  const Expression non_equality = BinaryExpressionExp(
      ColumnValueExp("x"), BinaryOperation::kNotEquals, ColumnValueExp("x"));
  EXPECT_EQ(ExpressionRewriter(ExpressionRuleSet::Default())
                .Rewrite(non_equality)
                ->Type(),
            TypeTag::kBinaryExp);
}

TEST(ExpressionRewriteTest, Rewrite_MergesCompatibleRangePredicates) {
  const Expression x = ColumnValueExp("x");
  const Expression merged =
      ExpressionRewriter(ExpressionRuleSet::Default())
          .Rewrite(BinaryExpressionExp(
              BinaryExpressionExp(x, BinaryOperation::kGreaterThan,
                                  ConstantValueExp(Value(1))),
              BinaryOperation::kAnd,
              BinaryExpressionExp(x, BinaryOperation::kGreaterThan,
                                  ConstantValueExp(Value(5)))));
  EXPECT_EQ(merged->ToString(), "(x > 5)");

  const Expression upper =
      ExpressionRewriter(ExpressionRuleSet::Default())
          .Rewrite(BinaryExpressionExp(
              BinaryExpressionExp(x, BinaryOperation::kLessThanEquals,
                                  ConstantValueExp(Value(10))),
              BinaryOperation::kAnd,
              BinaryExpressionExp(x, BinaryOperation::kLessThanEquals,
                                  ConstantValueExp(Value(7)))));
  EXPECT_EQ(upper->ToString(), "(x <= 7)");

  const Expression mixed =
      ExpressionRewriter(ExpressionRuleSet::Default())
          .Rewrite(BinaryExpressionExp(
              BinaryExpressionExp(x, BinaryOperation::kGreaterThan,
                                  ConstantValueExp(Value(1))),
              BinaryOperation::kAnd,
              BinaryExpressionExp(x, BinaryOperation::kLessThan,
                                  ConstantValueExp(Value(5)))));
  EXPECT_EQ(mixed->Type(), TypeTag::kBinaryExp);
}

TEST(ExpressionRewriteTest,
     Rewrite_EmptyAndNullInListsRespectThreeValuedLogic) {
  const Expression empty = InExpressionExp(ColumnValueExp("x"), {});
  const Expression empty_rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(empty);
  ASSERT_EQ(empty_rewritten->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(empty_rewritten->AsConstantValue().GetValue(), Value(false));

  const Expression null_item =
      InExpressionExp(ColumnValueExp("x"), {ConstantValueExp(Value())});
  const Expression null_rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(null_item);
  ASSERT_EQ(null_rewritten->Type(), TypeTag::kConstantValue)
      << null_rewritten->ToString();
  EXPECT_TRUE(null_rewritten->AsConstantValue().GetValue().IsNull());
}

TEST(ExpressionRewriteTest, Rewrite_OrOfEqualitiesBecomesInList) {
  const Expression x = ColumnValueExp("x");
  const Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default())
          .Rewrite(BinaryExpressionExp(
              BinaryExpressionExp(x, BinaryOperation::kEquals,
                                  ConstantValueExp(Value(1))),
              BinaryOperation::kOr,
              BinaryExpressionExp(x, BinaryOperation::kEquals,
                                  ConstantValueExp(Value(2)))));
  ASSERT_EQ(rewritten->Type(), TypeTag::kInExp);
  EXPECT_EQ(rewritten->AsInExpression().list_.size(), 2U);
}

TEST(ExpressionRewriteTest,
     Rewrite_DoubleNegationAndDeMorgan_SimplifiesExpressions) {
  Expression x = ColumnValueExp("x");
  Expression y = ColumnValueExp("y");
  Expression double_not = UnaryExpressionExp(
      UnaryExpressionExp(x, UnaryOperation::kNot), UnaryOperation::kNot);
  EXPECT_EQ(ExpressionRewriter(ExpressionRuleSet::Default())
                .Rewrite(double_not)
                ->Type(),
            TypeTag::kColumnValue);

  Expression not_and = UnaryExpressionExp(
      BinaryExpressionExp(x, BinaryOperation::kAnd, y), UnaryOperation::kNot);
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(not_and);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kOr);
  EXPECT_EQ(rewritten->AsBinaryExpression().Left()->Type(), TypeTag::kUnaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Right()->Type(),
            TypeTag::kUnaryExp);
}

TEST(ExpressionRewriteTest,
     Rewrite_CaseVariants_SimplifiesOrPreservesConditions) {
  // All-false conditions leave the else clause.
  Expression case_all_false = CaseExpressionExp(
      {{ConstantValueExp(Value(false)), ConstantValueExp(Value(1))}},
      ConstantValueExp(Value(9)));
  Expression simplified2 =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(case_all_false);
  ASSERT_EQ(simplified2->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(simplified2->AsConstantValue().GetValue(), Value(9));

  // Non-constant conditions are preserved.
  Expression case_keep =
      CaseExpressionExp({{ColumnValueExp("v"), ConstantValueExp(Value(1))}},
                        ConstantValueExp(Value(9)));
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(case_keep);
  EXPECT_EQ(rewritten->Type(), TypeTag::kFunctionCallExp);
  EXPECT_TRUE(rewritten->AsFunctionCallExpression().IsCanonicalIf());
}

TEST(ExpressionRewriteTest,
     WithExpressionChildren_VariousExpressions_RebuildsOrThrows) {
  Expression binary = BinaryExpressionExp(
      ConstantValueExp(Value(1)), BinaryOperation::kAdd, ColumnValueExp("x"));
  std::vector<Expression> bin_children = ExpressionChildren(binary);
  ASSERT_EQ(bin_children.size(), 2);
  EXPECT_EQ(WithExpressionChildren(binary, bin_children)->Type(),
            TypeTag::kBinaryExp);

  Expression unary =
      UnaryExpressionExp(ColumnValueExp("x"), UnaryOperation::kNot);
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

  Expression case_exp =
      CaseExpressionExp({{ColumnValueExp("a"), ColumnValueExp("b")},
                         {ColumnValueExp("c"), ColumnValueExp("d")}},
                        ColumnValueExp("e"));
  std::vector<Expression> case_children = ExpressionChildren(case_exp);
  ASSERT_EQ(case_children.size(), 5);
  EXPECT_EQ(WithExpressionChildren(case_exp, case_children)->Type(),
            TypeTag::kCaseExp);

  Expression in =
      InExpressionExp(ColumnValueExp("x"),
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
  EXPECT_THROW(
      std::ignore = WithExpressionChildren(binary, {ColumnValueExp("x")}),
      std::invalid_argument);
  EXPECT_THROW(std::ignore = WithExpressionChildren(in, {}),
               std::invalid_argument);
  EXPECT_THROW(std::ignore = WithExpressionChildren(
                   query, {ColumnValueExp("x"), ColumnValueExp("y")}),
               std::invalid_argument);
}

TEST(ExpressionRewriteTest,
     AggregateRewritePreservesFilteringAndOrderingMetadata) {
  auto aggregate = std::make_shared<AggregateExpression>(
      AggregationType::kStringAgg,
      BinaryExpressionExp(ColumnValueExp("value"), BinaryOperation::kAdd,
                          ConstantValueExp(Value("!"))),
      true);
  aggregate->SetWhereFilter(
      BinaryExpressionExp(ColumnValueExp("kind"), BinaryOperation::kEquals,
                          ConstantValueExp(Value("keep"))));
  aggregate->SetHaving(AggregateHavingModifier::kMax,
                       ColumnValueExp("priority"));
  aggregate->SetInnerOrderBy(
      {WindowOrderTerm{ColumnValueExp("priority"), false, std::nullopt}});
  aggregate->SetInnerLimit(3);
  aggregate->SetSecondaryArg(ConstantValueExp(Value("|")));

  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(aggregate);
  const auto& result = rewritten->AsAggregateExpression();
  EXPECT_TRUE(result.Distinct());
  ASSERT_TRUE(result.WhereFilter());
  EXPECT_EQ(result.WhereFilter()->ToString(), "(kind = \"keep\")");
  EXPECT_EQ(result.Having(), AggregateHavingModifier::kMax);
  ASSERT_TRUE(result.HavingCondition());
  EXPECT_EQ(result.HavingCondition()->ToString(), "priority");
  ASSERT_EQ(result.InnerOrderBy().size(), 1U);
  EXPECT_EQ(result.InnerOrderBy()[0].expression->ToString(), "priority");
  EXPECT_FALSE(result.InnerOrderBy()[0].ascending);
  ASSERT_TRUE(result.InnerLimit());
  EXPECT_EQ(*result.InnerLimit(), 3U);
  ASSERT_TRUE(result.SecondaryArg());
  EXPECT_EQ(result.SecondaryArg()->ToString(), "\"|\"");
}

TEST(ExpressionRewriteTest, SplitConjuncts_EdgeCases_HandlesCorrectly) {
  Expression single =
      BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kEquals,
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

TEST(ExpressionRewriteTest,
     ReferencesOnly_VariousExpressions_ReturnsExpectedBoolean) {
  EXPECT_TRUE(ReferencesOnly(nullptr, {}));
  Expression unqualified =
      BinaryExpressionExp(ColumnValueExp("x"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(1)));
  EXPECT_FALSE(ReferencesOnly(unqualified, {}));
  ColumnName qualified("s", "x");
  Expression qualified_expr =
      BinaryExpressionExp(ColumnValueExp(qualified), BinaryOperation::kEquals,
                          ConstantValueExp(Value(1)));
  EXPECT_TRUE(ReferencesOnly(qualified_expr, {"s"}));
  EXPECT_FALSE(ReferencesOnly(qualified_expr, {"other"}));
}

TEST(ExpressionRewriteTest, Rewrite_NonConvergingRules_ThrowsRuntimeError) {
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
      (void)ExpressionRewriter(rules).Rewrite(ConstantValueExp(Value(1))),
      std::runtime_error);
}

TEST(ExpressionRewriteTest, Rewrite_ExceedingDepthLimit_ThrowsRuntimeError) {
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

TEST(ExpressionRewriteTest, Rewrite_WithinDepthLimit_RewritesSuccessfully) {
  Expression expression = ColumnValueExp("v");
  for (int i = 0; i < 400; ++i) {
    expression = UnaryExpressionExp(expression, UnaryOperation::kMinus);
  }
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  // An even number of negations collapses back to the bare column.
  EXPECT_EQ(rewritten->Type(), TypeTag::kColumnValue);
}

TEST(ExpressionRewriteTest, Rewrite_SingleRuleSet_AppliesRuleSuccessfully) {
  using namespace expression_dsl;
  ExpressionRuleSet rules;
  rules.Add(ExpressionRule("fold_one",
                           AnyBinary(Is(TypeTag::kConstantValue, "left"),
                                     Is(TypeTag::kConstantValue, "right")),
                           [](const Expression&, const ExpressionBindings&) {
                             return ConstantValueExp(Value(42));
                           }));
  Expression expr =
      BinaryExpressionExp(ConstantValueExp(Value(1)), BinaryOperation::kAdd,
                          ConstantValueExp(Value(2)));
  Expression once = ExpressionRewriter(rules).Rewrite(expr);
  ASSERT_EQ(once->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(once->AsConstantValue().GetValue(), Value(42));
}

TEST(ExpressionRewriteTest, Rewrite_NotOverComparisons_PushesDownNegation) {
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

  Expression not_equals =
      UnaryExpressionExp(BinaryExpressionExp(x, BinaryOperation::kEquals, y),
                         UnaryOperation::kNot);
  rewritten = rewrite(not_equals);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kNotEquals);

  // NOT over AND stays under De Morgan control, not comparison negation.
  Expression not_and = UnaryExpressionExp(
      BinaryExpressionExp(x, BinaryOperation::kAnd, y), UnaryOperation::kNot);
  rewritten = rewrite(not_and);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kOr);
}

TEST(ExpressionRewriteTest,
     Rewrite_NotOverLikeAndNullChecks_PushesDownNegation) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };
  Expression name = ColumnValueExp("name");

  Expression not_like =
      UnaryExpressionExp(BinaryExpressionExp(name, BinaryOperation::kLike,
                                             ConstantValueExp(Value("a%"))),
                         UnaryOperation::kNot);
  Expression rewritten = rewrite(not_like);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kNotLike);

  Expression not_not_like =
      UnaryExpressionExp(BinaryExpressionExp(name, BinaryOperation::kNotLike,
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

TEST(ExpressionRewriteTest,
     Rewrite_XorBooleanIdentities_SimplifiesExpressions) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };
  Expression x = ColumnValueExp("x");

  Expression xor_true = BinaryExpressionExp(x, BinaryOperation::kXor,
                                            ConstantValueExp(Value(true)));
  Expression rewritten = rewrite(xor_true);
  ASSERT_EQ(rewritten->Type(), TypeTag::kUnaryExp);
  EXPECT_EQ(rewritten->AsUnaryExpression().Op(), UnaryOperation::kNot);

  Expression false_xor = BinaryExpressionExp(ConstantValueExp(Value(false)),
                                             BinaryOperation::kXor, x);
  rewritten = rewrite(false_xor);
  EXPECT_EQ(rewritten->Type(), TypeTag::kColumnValue);

  Expression xor_columns =
      BinaryExpressionExp(x, BinaryOperation::kXor, ColumnValueExp("y"));
  EXPECT_EQ(rewrite(xor_columns)->Type(), TypeTag::kBinaryExp);
}

TEST(ExpressionRewriteTest,
     Rewrite_IdempotenceAndAbsorption_SimplifiesExpressions) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };
  Expression x = ColumnValueExp("x");
  Expression y = ColumnValueExp("y");

  Expression and_idempotent =
      BinaryExpressionExp(x, BinaryOperation::kAnd, ColumnValueExp("x"));
  EXPECT_EQ(rewrite(and_idempotent)->Type(), TypeTag::kColumnValue);

  Expression or_idempotent =
      BinaryExpressionExp(y, BinaryOperation::kOr, ColumnValueExp("y"));
  EXPECT_EQ(rewrite(or_idempotent)->Type(), TypeTag::kColumnValue);

  Expression absorption =
      BinaryExpressionExp(x, BinaryOperation::kAnd,
                          BinaryExpressionExp(x, BinaryOperation::kOr, y));
  EXPECT_EQ(rewrite(absorption)->Type(), TypeTag::kColumnValue);

  Expression absorption_reversed =
      BinaryExpressionExp(BinaryExpressionExp(x, BinaryOperation::kOr, y),
                          BinaryOperation::kAnd, x);
  EXPECT_EQ(rewrite(absorption_reversed)->Type(), TypeTag::kColumnValue);

  Expression absorption_or =
      BinaryExpressionExp(x, BinaryOperation::kOr,
                          BinaryExpressionExp(x, BinaryOperation::kAnd, y));
  EXPECT_EQ(rewrite(absorption_or)->Type(), TypeTag::kColumnValue);

  // Non-absorption shapes survive unchanged.
  Expression kept = BinaryExpressionExp(
      x, BinaryOperation::kAnd,
      BinaryExpressionExp(y, BinaryOperation::kOr, ColumnValueExp("z")));
  EXPECT_EQ(rewrite(kept)->Type(), TypeTag::kBinaryExp);
}

TEST(ExpressionRewriteTest,
     Rewrite_ArithmeticIdentitiesAndDoubleNegation_SimplifiesExpressions) {
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
  Expression kept =
      BinaryExpressionExp(x, BinaryOperation::kAdd, ConstantValueExp(Value(2)));
  EXPECT_EQ(rewrite(kept)->Type(), TypeTag::kBinaryExp);
}

TEST(ExpressionRewriteTest,
     Rewrite_NestedConstantArithmetic_ReassociatesConstants) {
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

TEST(ExpressionRewriteTest,
     Rewrite_FloatingPointAndUnknownTypes_DoesNotReassociate) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };

  // Reassociating floating point arithmetic changes the IEEE rounding order,
  // so statically double-typed inners must be left alone.
  Expression avg =
      AggregateExpressionExp(AggregationType::kAvg, ColumnValueExp("amount"));
  Expression fp_nested =
      BinaryExpressionExp(BinaryExpressionExp(avg, BinaryOperation::kAdd,
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

TEST(ExpressionRewriteTest,
     Rewrite_DuplicateInListAndUniformCase_SimplifiesExpressions) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };

  Expression duplicated =
      InExpressionExp(ColumnValueExp("x"),
                      {ConstantValueExp(Value(1)), ConstantValueExp(Value(1)),
                       ConstantValueExp(Value(2))});
  Expression rewritten = rewrite(duplicated);
  ASSERT_EQ(rewritten->Type(), TypeTag::kInExp);
  EXPECT_EQ(rewritten->AsInExpression().list_.size(), 2U);

  Expression unique =
      InExpressionExp(ColumnValueExp("x"),
                      {ConstantValueExp(Value(1)), ConstantValueExp(Value(2))});
  EXPECT_EQ(rewrite(unique)->AsInExpression().list_.size(), 2U);

  Expression uniform =
      CaseExpressionExp({{ColumnValueExp("a"), ConstantValueExp(Value(7))},
                         {ColumnValueExp("b"), ConstantValueExp(Value(7))}},
                        ConstantValueExp(Value(7)));
  rewritten = rewrite(uniform);
  ASSERT_EQ(rewritten->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(rewritten->AsConstantValue().GetValue(), Value(7));

  Expression mixed =
      CaseExpressionExp({{ColumnValueExp("a"), ConstantValueExp(Value(7))},
                         {ColumnValueExp("b"), ConstantValueExp(Value(8))}},
                        ConstantValueExp(Value(7)));
  EXPECT_EQ(rewrite(mixed)->Type(), TypeTag::kCaseExp);
}

TEST(ExpressionRewriteTest, Rewrite_WildcardFreeLike_TransformsToEquality) {
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

TEST(ExpressionRewriteTest, Rewrite_NestedNullChecks_CollapsesToConstant) {
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

TEST(ExpressionRewriteTest,
     Rewrite_MixedConstantArithmetic_ReassociatesConstants) {
  auto rewrite = [](const Expression& expression) {
    return ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  };
  // A statically int64 inner expression that no rule can constant-fold.
  const auto int64_inner = [] {
    return FunctionCallExp("extract_year", {ColumnValueExp("event_date")});
  };

  // (x - 5) + 3 -> x + (-2)
  Expression subtract_then_add = BinaryExpressionExp(
      BinaryExpressionExp(int64_inner(), BinaryOperation::kSubtract,
                          ConstantValueExp(Value(5))),
      BinaryOperation::kAdd, ConstantValueExp(Value(3)));
  Expression rewritten = rewrite(subtract_then_add);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(
      rewritten->AsBinaryExpression().Right()->AsConstantValue().GetValue(),
      Value(-2));

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

TEST(ExpressionRewriteTest,
     Rewrite_ComplementaryAbsorption_PreservesExpressionForThreeValuedLogic) {
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

TEST(ExpressionRewriteTest, Rewrite_NestedIdenticalCast_CollapsesToSingleCast) {
  Expression nested = CastExpressionExp(
      CastExpressionExp(ColumnValueExp("x"), "INT64"), "INT64");
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(nested);
  ASSERT_EQ(rewritten->Type(), TypeTag::kCastExp);
  EXPECT_EQ(rewritten->AsCastExpression().Child()->Type(),
            TypeTag::kColumnValue);
}

TEST(ExpressionRewriteTest,
     Rewrite_RedundantEqualityFilterRemovesImpliedPredicate) {
  const Expression x = ColumnValueExp("x");
  const Expression expression =
      BinaryExpressionExp(BinaryExpressionExp(x, BinaryOperation::kEquals,
                                              ConstantValueExp(Value(5))),
                          BinaryOperation::kAnd,
                          BinaryExpressionExp(x, BinaryOperation::kGreaterThan,
                                              ConstantValueExp(Value(1))));
  const Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kEquals);
  EXPECT_EQ(
      rewritten->AsBinaryExpression().Right()->AsConstantValue().GetValue(),
      Value(5));
}

TEST(ExpressionRewriteTest, Rewrite_SelfStrictInequalityPreservesUnknown) {
  const Expression expression = BinaryExpressionExp(
      ColumnValueExp("x"), BinaryOperation::kLessThan, ColumnValueExp("x"));
  const Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  ASSERT_EQ(rewritten->Type(), TypeTag::kFunctionCallExp);
  const Schema schema("t", {Column("x", ValueType::kInt64)});
  EXPECT_EQ(rewritten->Evaluate(Row({Value(1)}), schema), Value(false));
  EXPECT_TRUE(rewritten->Evaluate(Row({Value()}), schema).IsNull());
}

TEST(ExpressionRewriteTest, Rewrite_NestedConcatFlattensArgumentsInOrder) {
  const Expression nested = FunctionCallExp(
      "concat", {ConstantValueExp(Value("a")),
                 FunctionCallExp("concat", {ConstantValueExp(Value("b")),
                                            ColumnValueExp("x")})});
  const Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(nested);
  ASSERT_EQ(rewritten->Type(), TypeTag::kFunctionCallExp);
  const auto& args = rewritten->AsFunctionCallExpression().Args();
  ASSERT_EQ(args.size(), 3U);
  EXPECT_EQ(args[0]->AsConstantValue().GetValue(), Value("a"));
  EXPECT_EQ(args[1]->AsConstantValue().GetValue(), Value("b"));
  EXPECT_EQ(args[2]->Type(), TypeTag::kColumnValue);
}

TEST(ExpressionRewriteTest, Rewrite_IfUsesShortCircuitingCaseForm) {
  const Expression expression = FunctionCallExp(
      "if", {ColumnValueExp("condition"), ConstantValueExp(Value(1)),
             ConstantValueExp(Value(2))});
  const Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  ASSERT_EQ(rewritten->Type(), TypeTag::kCaseExp);
  EXPECT_EQ(rewritten->Evaluate(
                Row({Value(1)}),
                Schema("t", {Column("condition", ValueType::kInt64)})),
            Value(1));
}

TEST(ExpressionRewriteTest, Rewrite_CaseWithOneBranchUsesStableCanonicalIf) {
  const Expression expression = CaseExpressionExp(
      {{ColumnValueExp("condition"), ConstantValueExp(Value(1))}},
      ConstantValueExp(Value(2)));
  const Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
  ASSERT_EQ(rewritten->Type(), TypeTag::kFunctionCallExp);
  EXPECT_EQ(rewritten->AsFunctionCallExpression().FuncName(), "if");
  EXPECT_TRUE(rewritten->AsFunctionCallExpression().IsCanonicalIf());
  const Schema schema("t", {Column("condition", ValueType::kInt64)});
  EXPECT_EQ(rewritten->Evaluate(Row({Value(0)}), schema), Value(2));
  EXPECT_EQ(rewritten->Evaluate(Row({Value(1)}), schema), Value(1));
}

TEST(ExpressionRewriteTest, Rewrite_DoesNotFoldNondeterministicFunctions) {
  const Expression timestamp = FunctionCallExp("current_timestamp", {});
  const Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(timestamp);
  EXPECT_EQ(rewritten->Type(), TypeTag::kFunctionCallExp);
}

TEST(ExpressionRewriteTest, Rewrite_FoldsDeterministicDateAndStringFunctions) {
  const Expression date = FunctionCallExp(
      "date_add",
      {ConstantValueExp(Value("2026-01-01")), IntervalExpressionExp(2, "day")});
  const Expression substring = FunctionCallExp(
      "substr", {ConstantValueExp(Value("abcdef")), ConstantValueExp(Value(2)),
                 ConstantValueExp(Value(3))});
  const ExpressionRewriter rewriter(ExpressionRuleSet::Default());
  EXPECT_EQ(rewriter.Rewrite(date)->AsConstantValue().GetValue(),
            Value("2026-01-03"));
  EXPECT_EQ(rewriter.Rewrite(substring)->AsConstantValue().GetValue(),
            Value("bcd"));
}

TEST(ExpressionRewriteTest,
     Rewrite_BooleanComparisonAgainstLiteralPreservesUnknown) {
  const Expression predicate = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("x"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(0))),
      BinaryOperation::kEquals, ConstantValueExp(Value(1)));
  const Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(predicate);
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(),
            BinaryOperation::kGreaterThan);

  const Schema schema("t", {Column("x", ValueType::kInt64)});
  EXPECT_TRUE(rewritten->Evaluate(Row({Value(2)}), schema).Truthy());
  EXPECT_TRUE(rewritten->Evaluate(Row({Value()}), schema).IsNull());
}

TEST(ExpressionRewriteTest, Rewrite_NotBetweenBecomesDisjunctionOfBounds) {
  const Expression x = ColumnValueExp("x");
  const Expression between = BinaryExpressionExp(
      BinaryExpressionExp(x, BinaryOperation::kGreaterThanEquals,
                          ConstantValueExp(Value(10))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(x, BinaryOperation::kLessThanEquals,
                          ConstantValueExp(Value(20))));
  const Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default())
          .Rewrite(UnaryExpressionExp(between, UnaryOperation::kNot));
  ASSERT_EQ(rewritten->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(rewritten->AsBinaryExpression().Op(), BinaryOperation::kOr);
  EXPECT_EQ(rewritten->Evaluate(Row({Value(5)}),
                                Schema("t", {Column("x", ValueType::kInt64)})),
            Value(true));
  EXPECT_TRUE(rewritten
                  ->Evaluate(Row({Value()}),
                             Schema("t", {Column("x", ValueType::kInt64)}))
                  .IsNull());
}

TEST(ExpressionRewriteTest, Rewrite_CommonAndInOr_FactorsCommonExpression) {
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

TEST(ExpressionRewriteTest,
     WithExpressionChildren_CastExpression_RebuildsCast) {
  Expression cast = CastExpressionExp(ColumnValueExp("x"), "INT64");
  std::vector<Expression> children = ExpressionChildren(cast);
  ASSERT_EQ(children.size(), 1);
  EXPECT_EQ(WithExpressionChildren(cast, children)->Type(), TypeTag::kCastExp);
}

// Overflowing constant folds must never wrap into a wrong constant: the
// fold attempt throws, the rule declines, and the expression survives for
// execution to report the overflow loudly.
TEST(ExpressionRewriteTest, Rewrite_OverflowingConstantFoldStaysUnfolded) {
  Expression expr = BinaryExpressionExp(
      ConstantValueExp(Value(std::numeric_limits<int64_t>::max())),
      BinaryOperation::kAdd, ConstantValueExp(Value(1)));
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expr);
  EXPECT_EQ(rewritten->Type(), TypeTag::kBinaryExp)
      << "folded value would silently wrap";
}

// ARRAY_LENGTH over a literal array is deterministic and folds at rewrite
// time; an =0 comparison against the folded length collapses to a boolean.
TEST(ExpressionRewriteTest, Rewrite_ArrayLengthOfLiteralArrayFolds) {
  ExpressionRewriter rewriter(ExpressionRuleSet::Default());
  Expression length = rewriter.Rewrite(FunctionCallExp(
      "array_length",
      {ConstantValueExp(Value::Array({Value(1), Value(2), Value(3)},
                                     std::string("INT64")))}));
  ASSERT_EQ(length->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(length->AsConstantValue().GetValue(), Value(3));

  Expression empty_is_zero = rewriter.Rewrite(BinaryExpressionExp(
      FunctionCallExp("array_length", {ConstantValueExp(Value::Array(
                                          {}, std::string("INT64")))}),
      BinaryOperation::kEquals, ConstantValueExp(Value(0))));
  ASSERT_EQ(empty_is_zero->Type(), TypeTag::kConstantValue);
  EXPECT_EQ(empty_is_zero->AsConstantValue().GetValue().Truthy(), true);
}

}  // namespace tinylamb
