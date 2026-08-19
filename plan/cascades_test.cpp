/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/cascades.hpp"

#include "gtest/gtest.h"

namespace tinylamb::cascades {

TEST(CascadesTest, DefaultRulesEnumerateEquivalentJoinTreesInMemo) {
  Memo memo;
  const GroupId root = memo.Build({"a", "b", "c"});
  SearchEngine search(std::move(memo), RuleSet::Default());

  search.Explore(root);

  EXPECT_GE(search.GetMemo().ExpressionCount(root), 4);
  EXPECT_GE(search.GetMemo().GroupCount(), 6);
}

TEST(CascadesTest, RuleCanBeRemovedWithoutOptimizerChanges) {
  Memo memo;
  const GroupId root = memo.Build({"a", "b", "c"});
  RuleSet rules = RuleSet::Default();
  ASSERT_TRUE(rules.Remove("join_enumeration"));
  EXPECT_TRUE(rules.Contains("join_commutativity"));
  SearchEngine search(std::move(memo), std::move(rules));

  search.Explore(root);

  EXPECT_EQ(search.GetMemo().ExpressionCount(root), 2);
}

TEST(CascadesTest, CustomCppDslRuleCapturesChildGroups) {
  using namespace dsl;
  Memo memo;
  const GroupId root = memo.Build({"a", "b"});
  bool invoked = false;
  RuleSet rules;
  rules.Add(Rule(
      "observe_join", Join(Scan("left"), Scan("right")),
      [&](const Bindings& bindings, Memo&, GroupId, const LogicalExpression&) {
        invoked = bindings.contains("left") && bindings.contains("right");
      }));
  SearchEngine search(std::move(memo), std::move(rules));

  search.Explore(root);

  EXPECT_TRUE(invoked);
}

TEST(CascadesTest, ImplementationRulesAreIndependentlyRemovable) {
  using namespace dsl;
  ImplementationRuleSet rules;
  rules.Add(ImplementationRule(
      "full_scan", Scan(),
      [](const Bindings&, const LogicalExpression&,
         const std::vector<BestPlan>&, const PhysicalProperties&) {
        return std::vector<PlanAlternative>{};
      }));

  EXPECT_TRUE(rules.Contains("full_scan"));
  EXPECT_TRUE(rules.Remove("full_scan"));
  EXPECT_FALSE(rules.Contains("full_scan"));
}

}  // namespace tinylamb::cascades
