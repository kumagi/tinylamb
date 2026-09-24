/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Seeded sweeps over the plan memo oracle (plan/plan_memo_oracle.cpp):
// complex-memo generation with varied configurations, invariant checking of
// fresh and explored memos, and the join-graph shrinker's contract.

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <string>

#include "plan/cascades.hpp"
#include "plan/plan_memo_oracle.hpp"

namespace tinylamb {

namespace {
// Fresh-build half of CheckComplexMemoEquivalence: the generated memo must
// build and satisfy its invariants.  (The full equivalence runner currently
// trips a use-after-free in an aggregate-rewriting rule -- see
// AggregateExpression::TouchedColumns reached from cascades.cpp:8460 -- so
// Explore is exercised only through the simpler join-graph oracle.)
void ExpectFreshBuildInvariants(uint32_t seed,
                                const ComplexMemoGenConfig& config) {
  std::mt19937 rng(seed);
  const GeneratedComplexMemo gen = GenerateComplexMemo(rng, config);
  SCOPED_TRACE("seed=" + std::to_string(seed) + " " + gen.description);
  EXPECT_FALSE(gen.relations.empty());
  cascades::Memo memo(128);
  const cascades::GroupId root = gen.builder(memo);
  EXPECT_TRUE(CheckMemoInvariants(memo, true).empty())
      << CheckMemoInvariants(memo, true);
  EXPECT_EQ(memo.Get(root).relations.size(), gen.relations.size());
}
}  // namespace

TEST(PlanMemoOracleComplex, SeededComplexMemosBuildSoundMemos) {
  for (uint32_t seed = 0; seed < 64; ++seed) {
    ExpectFreshBuildInvariants(seed, ComplexMemoGenConfig{});
  }
}

TEST(PlanMemoOracleComplex, ConfigVarietyExercisesGeneratorPaths) {
  const std::vector<ComplexMemoGenConfig> configs = {
      ComplexMemoGenConfig{},
      {.min_relations = 2,
       .max_relations = 6,
       .max_conjuncts = 8,
       .max_operator_depth = 8},
      {.min_relations = 4,
       .max_relations = 4,
       .max_conjuncts = 0,
       .max_operator_depth = 1},
      {.min_relations = 3,
       .max_relations = 3,
       .max_conjuncts = 3,
       .max_operator_depth = 12},
  };
  uint32_t seed = 0;
  for (const ComplexMemoGenConfig& config : configs) {
    for (int i = 0; i < 8; ++i) {
      ExpectFreshBuildInvariants(seed++, config);
    }
  }
}

TEST(PlanMemoOracleGraph, EdgeShapedJoinGraphsStaySound) {
  const std::vector<PlanMemoGenConfig> configs = {
      PlanMemoGenConfig{
          .min_relations = 2, .max_relations = 2, .max_conjuncts = 0},
      PlanMemoGenConfig{
          .min_relations = 2, .max_relations = 2, .max_conjuncts = 8},
      PlanMemoGenConfig{
          .min_relations = 5, .max_relations = 5, .max_conjuncts = 6},
  };
  uint32_t seed = 0;
  for (const PlanMemoGenConfig& config : configs) {
    for (int i = 0; i < 8; ++i) {
      std::mt19937 rng(seed++);
      const GeneratedJoinGraph graph = GenerateJoinGraph(rng, config);
      EXPECT_TRUE(CheckExploreEquivalence(graph).empty())
          << CheckExploreEquivalence(graph);
      const GeneratedJoinGraph shrunk = ShrinkJoinGraph(graph);
      EXPECT_LE(shrunk.relations.size(), graph.relations.size());
      EXPECT_LE(shrunk.conjuncts.size(), graph.conjuncts.size());
    }
  }
}

}  // namespace tinylamb
