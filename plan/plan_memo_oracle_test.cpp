/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/plan_memo_oracle.hpp"

#include <cstdint>
#include <random>

#include "expression/expression.hpp"
#include "gtest/gtest.h"
#include "plan/cascades.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

// Seeded join-graph sweep: every generated graph must keep the memo sound
// before/after Explore, deterministic across rebuilds, and monotone in the
// rule set. Mirrors the ExprSimplifyOracle seeded-iteration style so a
// failure reproduces from the seed alone.
TEST(PlanMemoOracle, SeededIterationsPreserveMemoEquivalence) {
  constexpr int kIterations = 200;
  int ran = 0;
  for (uint32_t seed = 0; seed < kIterations; ++seed) {
    std::mt19937 rng(seed);
    const GeneratedJoinGraph graph = GenerateJoinGraph(rng);
    EXPECT_TRUE(CheckExploreEquivalence(graph).empty())
        << true << (seed != 0u) << true
        << CheckExploreEquivalence(graph);
    ++ran;
  }
  EXPECT_EQ(ran, kIterations);
}

// The generator is deterministic: reseeding reproduces the graph exactly,
// which is what lets a failure replay from the seed alone.
TEST(PlanMemoOracle, SameSeedReproducesSameGraph) {
  for (uint32_t seed : {0U, 1U, 7U, 42U, 12345U}) {
    std::mt19937 first(seed);
    std::mt19937 second(seed);
    const GeneratedJoinGraph a = GenerateJoinGraph(first);
    const GeneratedJoinGraph b = GenerateJoinGraph(second);
    ASSERT_EQ(a.relations, b.relations) << "seed=" << seed;
    ASSERT_EQ(a.conjuncts.size(), b.conjuncts.size()) << "seed=" << seed;
    for (size_t i = 0; i < a.conjuncts.size(); ++i) {
      EXPECT_EQ(a.conjuncts[i].conjunct->ToString(),
                b.conjuncts[i].conjunct->ToString())
          << "seed=" << seed;
      EXPECT_EQ(a.conjuncts[i].relations, b.conjuncts[i].relations)
          << "seed=" << seed;
    }
  }
}

// The shrinker never introduces a passing graph and never grows the input:
// shrinking a generated graph keeps the verdict while minimizing it.
TEST(PlanMemoOracle, ShrinkNeverHidesMismatchNorGrows) {
  for (uint32_t seed : {0U, 1U, 7U, 42U, 1234U}) {
    std::mt19937 rng(seed);
    const GeneratedJoinGraph graph = GenerateJoinGraph(rng);
    const GeneratedJoinGraph shrunk = ShrinkJoinGraph(graph);
    EXPECT_LE(shrunk.relations.size(), graph.relations.size())
        << "seed=" << seed;
    EXPECT_LE(shrunk.conjuncts.size(), graph.conjuncts.size())
        << "seed=" << seed;
    // A passing graph shrinks to itself; check both directions agree.
    EXPECT_EQ(CheckExploreEquivalence(graph).empty(),
              CheckExploreEquivalence(shrunk).empty())
        << "seed=" << seed;
  }
}

// Sensitivity probe: a hand-built Join whose predicate diverges from the
// stored conjuncts must be flagged (rules are required to build joins via
// Memo::NewJoin, which sets the canonical condition).
TEST(PlanMemoOracle, FlagsJoinConditionDivergingFromConjuncts) {
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build(
      {"a", "b"},
      {{BinaryExpressionExp(ColumnValueExp("a.x"), BinaryOperation::kEquals,
                            ColumnValueExp("b.y")),
        {"a", "b"}}});
  const cascades::Group& group = memo.Get(root);
  ASSERT_EQ(group.expressions.size(), 1U);
  cascades::LogicalExpression bogus = group.expressions.front();
  bogus.predicate =
      BinaryExpressionExp(ColumnValueExp("a.x"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{999})));
  ASSERT_TRUE(memo.AddExpression(root, bogus));
  EXPECT_FALSE(CheckMemoInvariants(memo, true).empty());
}

}  // namespace
}  // namespace tinylamb
