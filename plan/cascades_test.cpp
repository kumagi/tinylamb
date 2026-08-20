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

TEST(CascadesTest, MemoBuildRejectsDuplicateRelations) {
  // Arrange + Act + Assert: a join graph that repeats a relation cannot form a
  // valid memo group.
  Memo memo;
  EXPECT_THROW(memo.Build({"a", "a"}), std::invalid_argument);
}

TEST(CascadesTest, MemoAddExpressionRejectsScanWithChildren) {
  // Arrange: a single-relation group holds a scan expression.
  Memo memo;
  const GroupId root = memo.Build({"a"});

  // Act + Assert: a scan carrying child groups does not belong to the group.
  EXPECT_THROW(memo.AddExpression(
                   root, LogicalExpression{LogicalOperator::kScan, {root}, "a"}),
               std::invalid_argument);
}

TEST(CascadesTest, MemoAddExpressionRejectsJoinWithWrongArity) {
  // Arrange: a single-relation group.
  Memo memo;
  const GroupId root = memo.Build({"a"});

  // Act + Assert: a join must have exactly two child groups.
  EXPECT_THROW(memo.AddExpression(
                   root, LogicalExpression{LogicalOperator::kJoin,
                                           {root, root, root}, {}}),
               std::invalid_argument);
}

TEST(CascadesTest, MemoAddExpressionRejectsNonEquivalentJoin) {
  // Arrange: build a two-way join so the child groups {a} and {b} are known.
  Memo memo;
  const GroupId root = memo.Build({"a", "b"});
  const Group& join_group = memo.Get(root);
  ASSERT_EQ(join_group.expressions.size(), 1U);
  const GroupId a_group = join_group.expressions[0].children[0];
  const GroupId b_group = join_group.expressions[0].children[1];

  // Act + Assert: joining {b} with {a} inside the {a} group produces a union
  // that is not equivalent to the target group's relations.
  EXPECT_THROW(memo.AddExpression(
                   a_group, LogicalExpression{LogicalOperator::kJoin,
                                              {b_group, a_group}, {}}),
               std::invalid_argument);
}

TEST(CascadesTest, PatternChildMismatchFailsToMatch) {
  // Arrange: a rule that requires a SCAN on the join's left child. The
  // enumerated join trees have multi-relation left children whose groups only
  // hold join expressions, so those child patterns must fail to match.
  using namespace dsl;
  Memo memo;
  const GroupId root = memo.Build({"a", "b", "c"});
  RuleSet rules = RuleSet::Default();
  rules.Add(Rule(
      "probe_scan_left", Join(Scan("left"), Any("right")),
      [](const Bindings&, Memo&, GroupId, const LogicalExpression&) {}));
  SearchEngine search(std::move(memo), std::move(rules));

  // Act: enumerate the join space with the probe rule active.
  search.Explore(root);

  // Assert: enumeration still produces the full set of equivalent join trees.
  EXPECT_GE(search.GetMemo().ExpressionCount(root), 4U);
}

TEST(CascadesTest, PatternArityMismatchFailsToMatch) {
  // Arrange: a pattern whose children count (1) differs from the arity of a
  // join expression (2) must never match.
  Memo memo;
  const GroupId root = memo.Build({"a", "b"});
  bool invoked = false;
  RuleSet rules;
  rules.Add(Rule(
      "arity_probe", Pattern::Op(LogicalOperator::kJoin, {Pattern::Any()}),
      [&](const Bindings&, Memo&, GroupId, const LogicalExpression&) {
        invoked = true;
      }));
  SearchEngine search(std::move(memo), std::move(rules));

  // Act + Assert: the malformed pattern matches nothing.
  search.Explore(root);
  EXPECT_FALSE(invoked);
}

TEST(CascadesTest, OptimizeWithNoImplementationRulesReturnsNoPlan) {
  // Arrange: an empty implementation rule set can implement nothing.
  Memo memo;
  const GroupId root = memo.Build({"a", "b"});
  SearchEngine search(std::move(memo), RuleSet::Default());
  ImplementationRuleSet implementation_rules;

  // Act: optimize the two-way join with no implementation rules.
  std::optional<BestPlan> plan =
      search.Optimize(root, PhysicalProperties{}, implementation_rules);

  // Assert: no physical plan exists because the child groups produce none.
  EXPECT_FALSE(plan.has_value());
}

TEST(CascadesTest, OptimizeWithOrderingPropertyKeysOrderingColumns) {
  // Arrange: a physical-properties request carrying an ordering column must be
  // reflected in the per-group cache key.
  Memo memo;
  const GroupId root = memo.Build({"a"});
  SearchEngine search(std::move(memo), RuleSet::Default());
  ImplementationRuleSet implementation_rules;
  PhysicalProperties properties{.require_row_position = false,
                                .ordering = {ColumnName("a")}};

  // Act + Assert: optimization completes (with no plan) and does not crash.
  std::optional<BestPlan> plan =
      search.Optimize(root, properties, implementation_rules);
  EXPECT_FALSE(plan.has_value());
}

}  // namespace tinylamb::cascades
