/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "executor/detail/planning_heuristics.hpp"

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/unary_expression.hpp"
#include "gtest/gtest.h"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

using relational_detail::AnalyzePredicates;
using relational_detail::ConcatRows;
using relational_detail::EqualityKeys;
using relational_detail::EqualityPairKey;
using relational_detail::EstimateJoinRows;
using relational_detail::EstimateJoinRowsCached;
using relational_detail::IsNullRejectingPredicate;
using relational_detail::JoinKeyFrequencyMaps;
using relational_detail::PredicateInfo;
using relational_detail::Relation;
using relational_detail::SameSingleIntegerJoinKey;
using relational_detail::SynthesizeDisjunctEqualityPredicates;

TEST(PlanningHeuristicsTest, EqualityPairKeyIsSymmetric) {
  ColumnName a("t1", "id");
  ColumnName b("t2", "id");
  EXPECT_EQ(EqualityPairKey(a, b), EqualityPairKey(b, a));
  EXPECT_NE(EqualityPairKey(a, b),
            EqualityPairKey(a, ColumnName("t2", "other")));
}

TEST(PlanningHeuristicsTest, ConcatRowsAndSameSingleIntegerJoinKey) {
  Row left({Value(int64_t{1}), Value("a")});
  Row right({Value(int64_t{2})});
  Row joined = ConcatRows(left, right);
  ASSERT_EQ(joined.values_.size(), 3U);
  EXPECT_EQ(joined[0], Value(int64_t{1}));
  EXPECT_EQ(joined[1], Value("a"));
  EXPECT_EQ(joined[2], Value(int64_t{2}));

  Schema s1("t1", {Column(ColumnName("t1", "k"), ValueType::kInt64),
                   Column(ColumnName("t1", "v"), ValueType::kVarChar)});
  Schema s2("t2", {Column(ColumnName("t2", "k"), ValueType::kInt64)});
  EXPECT_TRUE(SameSingleIntegerJoinKey(s1, {0}, s2, {0}));
  EXPECT_FALSE(SameSingleIntegerJoinKey(s1, {1}, s2, {0}));
}

TEST(PlanningHeuristicsTest, IsNullRejectingPredicate) {
  Expression eq = BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "a")),
                                      BinaryOperation::kEquals,
                                      ConstantValueExp(Value(int64_t{10})));
  EXPECT_TRUE(IsNullRejectingPredicate(eq));

  Expression is_null = UnaryExpressionExp(ColumnValueExp(ColumnName("t1", "a")),
                                          UnaryOperation::kIsNull);
  EXPECT_FALSE(IsNullRejectingPredicate(is_null));
}

TEST(PlanningHeuristicsTest, SynthesizeDisjunctEqualityPredicates) {
  Relation r1;
  r1.schema = Schema("t1", {Column(ColumnName("t1", "k"), ValueType::kInt64),
                            Column(ColumnName("t1", "x"), ValueType::kInt64)});
  Relation r2;
  r2.schema = Schema("t2", {Column(ColumnName("t2", "k"), ValueType::kInt64),
                            Column(ColumnName("t2", "y"), ValueType::kInt64)});
  std::vector<Relation> relations;
  relations.push_back(std::move(r1));
  relations.push_back(std::move(r2));

  Expression eq_k = BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "k")),
                                        BinaryOperation::kEquals,
                                        ColumnValueExp(ColumnName("t2", "k")));
  Expression b1 = BinaryExpressionExp(
      eq_k, BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "x")),
                          BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{1}))));
  Expression b2 = BinaryExpressionExp(
      eq_k, BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("t2", "y")),
                          BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{2}))));
  Expression disj = BinaryExpressionExp(b1, BinaryOperation::kOr, b2);

  std::vector<PredicateInfo> analyzed = AnalyzePredicates(disj, relations);
  std::vector<PredicateInfo> synth =
      SynthesizeDisjunctEqualityPredicates(analyzed, relations);
  ASSERT_EQ(synth.size(), 1U);
  EXPECT_EQ(synth[0].sources.size(), 2U);
}

TEST(PlanningHeuristicsTest, EstimateJoinRowsAndCachedAgree) {
  Relation left;
  left.schema =
      Schema("t1", {Column(ColumnName("t1", "k"), ValueType::kInt64)});
  left.rows = {Row({Value(int64_t{1})}), Row({Value(int64_t{2})}),
               Row({Value(int64_t{2})}), Row({Value(int64_t{3})})};

  Relation right;
  right.schema =
      Schema("t2", {Column(ColumnName("t2", "k"), ValueType::kInt64)});
  right.rows = {Row({Value(int64_t{2})}), Row({Value(int64_t{2})}),
                Row({Value(int64_t{4})})};

  Expression eq = BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "k")),
                                      BinaryOperation::kEquals,
                                      ColumnValueExp(ColumnName("t2", "k")));
  std::vector<Expression> preds = {eq};

  const size_t est = EstimateJoinRows(left, right, preds);
  EXPECT_EQ(est, 4U);

  std::unordered_map<const Relation*, std::vector<JoinKeyFrequencyMaps>> cache;
  EXPECT_EQ(EstimateJoinRowsCached(left, right, preds, &cache), 4U);
  // Second call hits the frequency cache.
  EXPECT_EQ(EstimateJoinRowsCached(left, right, preds, &cache), 4U);
}

}  // namespace
}  // namespace tinylamb
