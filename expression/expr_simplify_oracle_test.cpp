/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/expr_simplify_oracle.hpp"

#include <cstdint>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "gtest/gtest.h"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

// Seeded rewrite-equivalence sweep: every generated tree must evaluate
// identically before and after the default rewrite set.
TEST(ExprSimplifyOracle, SeededIterationsPreserveSemantics) {
  constexpr int kIterations = 200;
  int ran = 0;
  for (uint32_t seed = 0; seed < kIterations; ++seed) {
    std::mt19937 rng(seed);
    GeneratedExpr generated = GenerateSimplifyExpr(rng);
    ASSERT_TRUE(generated.expr) << true << (seed != 0U);
    EXPECT_TRUE(generated.sql.empty() == false) << true << (seed != 0U);
    EXPECT_TRUE(generated.sexpr.empty() == false) << true << (seed != 0U);
    EXPECT_EQ(CheckSimplifyEquivalence(generated.expr), "")
        << true << (seed != 0U) << true << generated.sql
        << "\nsexpr: " << generated.sexpr;
    ++ran;
  }
  EXPECT_EQ(ran, kIterations);
}

// The generator is deterministic: reseeding reproduces the triple exactly,
// which is what lets a `.test` file replay from the seed alone.
TEST(ExprSimplifyOracle, SameSeedReproducesSameTriple) {
  for (uint32_t seed : {0U, 1U, 7U, 42U, 12345U}) {
    std::mt19937 first(seed);
    std::mt19937 second(seed);
    GeneratedExpr a = GenerateSimplifyExpr(first);
    GeneratedExpr b = GenerateSimplifyExpr(second);
    EXPECT_EQ(a.sql, b.sql) << "seed=" << seed;
    EXPECT_EQ(a.sexpr, b.sexpr) << "seed=" << seed;
    EXPECT_EQ(a.expr->ToString(), b.expr->ToString()) << "seed=" << seed;
  }
}

// Serializers cover every node kind the generator emits.
TEST(ExprSimplifyOracle, SerializersCoverAllNodeKinds) {
  std::mt19937 rng(99);  // NOLINT(cert-msc32-c,cert-msc51-cpp) deterministic
                         // seed for reproducibility
  bool saw_case = false;
  bool saw_in = false;
  bool saw_cast = false;
  bool saw_function = false;
  for (int i = 0; i < 50; ++i) {
    GeneratedExpr generated = GenerateSimplifyExpr(rng);
    const std::string& sql = generated.sql;
    const std::string& sexpr = generated.sexpr;
    if (sql.find("CASE") != std::string::npos) {
      saw_case = true;
    }
    if (sql.find(" IN (") != std::string::npos) {
      saw_in = true;
    }
    if (sql.find("CAST(") != std::string::npos) {
      saw_cast = true;
    }
    if (sql.find("ABS(") != std::string::npos ||
        sql.find("GREATEST(") != std::string::npos ||
        sql.find("COALESCE(") != std::string::npos) {
      saw_function = true;
    }
    EXPECT_EQ(sexpr.front(), '(') << sexpr;
    EXPECT_EQ(sexpr.back(), ')') << sexpr;
  }
  EXPECT_TRUE(saw_case);
  EXPECT_TRUE(saw_in);
  EXPECT_TRUE(saw_cast);
  EXPECT_TRUE(saw_function);
}

// NaN/NULL/inf-aware equality is the oracle's comparison kernel.
TEST(ExprSimplifyOracle, SimplifyValuesEqualHandlesEdgeCases) {
  EXPECT_TRUE(SimplifyValuesEqual(Value(), Value()));
  EXPECT_FALSE(SimplifyValuesEqual(Value(), Value(int64_t{0})));
  EXPECT_TRUE(
      SimplifyValuesEqual(Value(std::numeric_limits<double>::quiet_NaN()),
                          Value(std::numeric_limits<double>::quiet_NaN())));
  EXPECT_TRUE(
      SimplifyValuesEqual(Value(std::numeric_limits<double>::infinity()),
                          Value(std::numeric_limits<double>::infinity())));
  EXPECT_FALSE(
      SimplifyValuesEqual(Value(std::numeric_limits<double>::infinity()),
                          Value(-std::numeric_limits<double>::infinity())));
  EXPECT_TRUE(SimplifyValuesEqual(Value(int64_t{7}), Value(int64_t{7})));
  EXPECT_FALSE(SimplifyValuesEqual(Value(int64_t{7}), Value(7.0)));
}

// Shrinking a consistent tree is a no-op and always terminates.
TEST(ExprSimplifyOracle, ShrinkPreservesConsistentTrees) {
  std::mt19937 rng(5);  // NOLINT(cert-msc32-c,cert-msc51-cpp) deterministic
                        // seed for reproducibility
  GeneratedExpr generated = GenerateSimplifyExpr(rng);
  ASSERT_EQ(CheckSimplifyEquivalence(generated.expr), "");
  Expression shrunk = ShrinkSimplifyCounterexample(generated.expr);
  EXPECT_EQ(shrunk->ToString(), generated.expr->ToString());
}

// Oracle-found regression pins (the generator + Python cross-check caught
// each of these as a ground-truth divergence; the rewrite-equivalence sweep
// alone cannot see uniformly-wrong evaluation).

Value EvaluateGroundTruth(const Expression& expression) {
  return expression->Evaluate(Row(), Schema());
}

TEST(ExprSimplifyOracle, LikeWildcardMatchesLiteralPercent) {
  // '%' is a wildcard even when the value character is also '%'.
  auto like = [](const char* value, const char* pattern) {
    return EvaluateGroundTruth(BinaryExpressionExp(
        ConstantValueExp(Value(std::string(value))), BinaryOperation::kLike,
        ConstantValueExp(Value(std::string(pattern)))));
  };
  EXPECT_EQ(like("%%", "%"), Value(true));
  EXPECT_EQ(like("%", "%_"), Value(true));
  EXPECT_EQ(like("abc", "a%c"), Value(true));
  EXPECT_EQ(like("abc", "a_d"), Value(false));
  EXPECT_EQ(like("", "%"), Value(true));
  EXPECT_EQ(like("", ""), Value(true));
}

TEST(ExprSimplifyOracle, AbsInt64MinRaisesLikeUnaryMinus) {
  EXPECT_THROW(
      EvaluateGroundTruth(FunctionCallExp(
          "abs",
          {ConstantValueExp(Value(std::numeric_limits<int64_t>::min()))})),
      std::runtime_error);
  EXPECT_EQ(EvaluateGroundTruth(
                FunctionCallExp("abs", {ConstantValueExp(Value(int64_t{-3}))})),
            Value(int64_t{3}));
}

TEST(ExprSimplifyOracle, GroundTruthExecutesNullifGreatestLeastIfnull) {
  EXPECT_TRUE(
      EvaluateGroundTruth(
          FunctionCallExp("nullif", {ConstantValueExp(Value(int64_t{0})),
                                     ConstantValueExp(Value(int64_t{0}))}))
          .IsNull());
  EXPECT_EQ(EvaluateGroundTruth(FunctionCallExp(
                "nullif", {ConstantValueExp(Value(int64_t{1})),
                           ConstantValueExp(Value(int64_t{0}))})),
            Value(int64_t{1}));
  EXPECT_EQ(EvaluateGroundTruth(FunctionCallExp(
                "greatest", {ConstantValueExp(Value(int64_t{1})),
                             ConstantValueExp(Value(2.5)),
                             ConstantValueExp(Value(int64_t{3}))})),
            Value(int64_t{3}));
  EXPECT_TRUE(
      EvaluateGroundTruth(
          FunctionCallExp("least", {ConstantValueExp(Value()),
                                    ConstantValueExp(Value(int64_t{1}))}))
          .IsNull());
  EXPECT_EQ(EvaluateGroundTruth(FunctionCallExp(
                "ifnull", {ConstantValueExp(Value()),
                           ConstantValueExp(Value(int64_t{9}))})),
            Value(int64_t{9}));
}

TEST(ExprSimplifyOracle, CoalesceShortCircuitsLikeRelationalPath) {
  // A throwing branch after the first non-NULL must never surface.
  Expression throwing = BinaryExpressionExp(
      ConstantValueExp(Value(int64_t{1})), BinaryOperation::kDivide,
      ConstantValueExp(Value(int64_t{0})));
  EXPECT_EQ(EvaluateGroundTruth(FunctionCallExp(
                "coalesce", {ConstantValueExp(Value(int64_t{7})), throwing})),
            Value(int64_t{7}));
  EXPECT_EQ(EvaluateGroundTruth(FunctionCallExp(
                "ifnull", {ConstantValueExp(Value(int64_t{7})), throwing})),
            Value(int64_t{7}));
  EXPECT_TRUE(EvaluateGroundTruth(
                  FunctionCallExp("coalesce", {ConstantValueExp(Value()),
                                               ConstantValueExp(Value())}))
                  .IsNull());
  // A NULL prefix still evaluates the next branch, so its error surfaces.
  EXPECT_THROW(EvaluateGroundTruth(FunctionCallExp(
                   "coalesce", {ConstantValueExp(Value()), throwing})),
               std::runtime_error);
}

TEST(ExprSimplifyOracle, DoubleNegationKeepsMinOverflowError) {
  Expression neg_min = UnaryExpressionExp(
      ConstantValueExp(Value(std::numeric_limits<int64_t>::min())),
      UnaryOperation::kMinus);
  ASSERT_THROW(EvaluateGroundTruth(neg_min), std::runtime_error);
  // The rewrite must not turn the throwing negation into a value.
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default())
          .Rewrite(UnaryExpressionExp(neg_min, UnaryOperation::kMinus));
  EXPECT_THROW(rewritten->Evaluate(Row(), Schema()), std::runtime_error);
}

}  // namespace
}  // namespace tinylamb
