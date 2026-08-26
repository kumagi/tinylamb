/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/cascades.hpp"

#include <algorithm>
#include <optional>
#include <cstddef>
#include <ranges>
#include <sstream>
#include <utility>
#include <unordered_set>
#include <vector>
#include <stdexcept>

#include "common/constants.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "gtest/gtest.h"
#include "plan/implementation_rules.hpp"
#include "plan/product_plan.hpp"
#include "plan/relational_plan.hpp"
#include "query/statement.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

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
  ASSERT_TRUE(rules.Remove("join_associativity_left"));
  ASSERT_TRUE(rules.Remove("join_associativity_right"));
  EXPECT_TRUE(rules.Contains("join_commutativity"));
  SearchEngine search(std::move(memo), rules);

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
  SearchEngine search(std::move(memo), rules);

  search.Explore(root);

  EXPECT_TRUE(invoked);
}

TEST(CascadesTest, ImplementationRulesAreIndependentlyRemovable) {
  using namespace dsl;
  ImplementationRuleSet rules;
  rules.Add(ImplementationRule(
      "full_scan", Scan(),
      [](GroupId, const Memo&, const Bindings&, const LogicalExpression&,
         const std::vector<BestPlan>&, const PhysicalProperties&,
         const RuleContext&) {
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
  EXPECT_THROW(memo.AddExpression(root,
                                  LogicalExpression{.operation =
                                                        LogicalOperator::kScan,
                                                    .children = {root},
                                                    .table = "a"}),
               std::invalid_argument);
}

TEST(CascadesTest, MemoAddExpressionRejectsJoinWithWrongArity) {
  // Arrange: a single-relation group.
  Memo memo;
  const GroupId root = memo.Build({"a"});

  // Act + Assert: a join must have exactly two child groups.
  EXPECT_THROW(
      memo.AddExpression(root,
                         LogicalExpression{.operation = LogicalOperator::kJoin,
                                           .children = {root, root, root}}),
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
  EXPECT_THROW(
      memo.AddExpression(a_group,
                         LogicalExpression{.operation = LogicalOperator::kJoin,
                                           .children = {b_group, a_group}}),
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
  SearchEngine search(std::move(memo), rules);

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
  SearchEngine search(std::move(memo), rules);

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

TEST(CascadesTest, AssociativityRulesEnumerateJoinOrdersWithoutEnumeration) {
  // Arrange: with join_enumeration removed, commutativity plus the two
  // associativity rotations must still derive every ordered bipartition of a
  // three-way join.
  Memo memo;
  const GroupId root = memo.Build({"a", "b", "c"});
  RuleSet rules = RuleSet::Default();
  ASSERT_TRUE(rules.Remove("join_enumeration"));
  SearchEngine search(std::move(memo), rules);

  // Act
  search.Explore(root);

  // Assert: all six ordered splits ({a}|{b,c}, {b,c}|{a}, {a,b}|{c},
  // {c}|{a,b}, {a,c}|{b}, {b}|{a,c}) appear in the root group; the mirrored
  // rotation is derivable from commutativity + the left rotation alone.
  EXPECT_GE(search.GetMemo().ExpressionCount(root), 6U);
}


namespace {

Expression EqExp(std::string_view column, const Value& value) {
  return BinaryExpressionExp(ColumnValueExp(column),
                             BinaryOperation::kEquals,
                             ConstantValueExp(value));
}

}  // namespace

TEST(CascadesTest, MemoBuildAttachesConjunctsToScansAndJoinConditions) {
  // Arrange: one single-relation conjunct and one spanning conjunct.
  Memo memo;
  const GroupId root = memo.Build(
      {"a", "b"},
      {{EqExp("a.x", Value(1)), {"a"}},
       {BinaryExpressionExp(ColumnValueExp("a.y"), BinaryOperation::kEquals,
                            ColumnValueExp("b.z")),
        {"a", "b"}}});

  // Act: locate the scan groups below the initial join.
  const Group& join_group = memo.Get(root);
  ASSERT_EQ(join_group.expressions.size(), 1U);
  const LogicalExpression& join = join_group.expressions.front();
  const Group& left = memo.Get(join.children[0]);
  const Group& right = memo.Get(join.children[1]);

  // Assert: the single-relation conjunct becomes the scan filter of its
  // relation and the spanning conjunct becomes the join condition payload.
  EXPECT_TRUE(left.relations == std::vector<std::string>{"a"});
  ASSERT_TRUE(left.filter);
  EXPECT_EQ(left.filter->TouchedColumns().size(), 1U);
  EXPECT_FALSE(right.filter);
  ASSERT_TRUE(join.predicate);
  if (join.predicate) {  // Re-check so static analysis sees the guard.
    EXPECT_EQ((*join.predicate)->TouchedColumns().size(), 2U);
  }
}

TEST(CascadesTest, PayloadAwareFingerprintKeepsDistinctSelections) {
  // Arrange: two Selections over the same child group differing only in the
  // predicate must not collapse into one memo expression (D1).
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId selection = memo.EnsureDerivedGroup({"a"}, "selection");
  LogicalExpression first;
  first.operation = LogicalOperator::kSelection;
  first.children = {scan};
  first.predicate = EqExp("a.x", Value(1));
  LogicalExpression second = first;
  second.predicate = EqExp("a.x", Value(2));

  // Act + Assert: both expressions coexist in the group.
  EXPECT_TRUE(memo.AddExpression(selection, first));
  EXPECT_TRUE(memo.AddExpression(selection, second));
  EXPECT_EQ(memo.ExpressionCount(selection), 2U);
}

TEST(CascadesTest, MemoAddExpressionValidatesSingleChildOperators) {
  // Arrange: a scan group, its derived group, and a two-relation group.
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId join_root = memo.Build({"a", "b"});
  const GroupId derived = memo.EnsureDerivedGroup({"a"}, "selection");

  // Act + Assert: a Selection must carry a predicate...
  LogicalExpression no_predicate;
  no_predicate.operation = LogicalOperator::kSelection;
  no_predicate.children = {scan};
  EXPECT_THROW(memo.AddExpression(derived, no_predicate),
               std::invalid_argument);
  // ...and must preserve the group's relation set.
  LogicalExpression wrong_relations;
  wrong_relations.operation = LogicalOperator::kSelection;
  wrong_relations.children = {join_root};
  wrong_relations.predicate = EqExp("a.x", Value(1));
  EXPECT_THROW(memo.AddExpression(derived, wrong_relations),
               std::invalid_argument);
  // Projections need a target list.
  LogicalExpression no_targets;
  no_targets.operation = LogicalOperator::kProjection;
  no_targets.children = {scan};
  EXPECT_THROW(memo.AddExpression(derived, no_targets),
               std::invalid_argument);
}

TEST(CascadesTest, SelectionWithinPatternMatchesOnlyCoveredPredicates) {
  // Arrange: a Selection over a scan group whose predicate stays inside the
  // child relations, and one that does not.
  using namespace dsl;
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId derived = memo.EnsureDerivedGroup({"a"}, "selection");
  LogicalExpression inside;
  inside.operation = LogicalOperator::kSelection;
  inside.children = {scan};
  inside.predicate = EqExp("a.x", Value(1));
  ASSERT_TRUE(memo.AddExpression(derived, inside));
  LogicalExpression outside;
  outside.operation = LogicalOperator::kSelection;
  outside.children = {scan};
  outside.predicate = EqExp("b.z", Value(1));
  ASSERT_TRUE(memo.AddExpression(derived, outside));

  const Pattern pattern =
      SelectionWithin(0, Scan("scan"), "sel");
  Bindings bindings;

  // Act + Assert: only the covered predicate matches.
  EXPECT_TRUE(pattern.Match(memo, derived, inside, &bindings));
  EXPECT_FALSE(pattern.Match(memo, derived, outside, &bindings));
}

TEST(CascadesTest, PushSelectionIntoScanAnnotatesGroupFilter) {
  // Arrange: a Selection over a scan group; the default rule set contains
  // push_selection_into_scan.
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId selection = memo.EnsureDerivedGroup({"a"}, "selection");
  LogicalExpression expression;
  expression.operation = LogicalOperator::kSelection;
  expression.children = {scan};
  expression.predicate = EqExp("a.x", Value(1));
  ASSERT_TRUE(memo.AddExpression(selection, expression));
  SearchEngine search(std::move(memo), RuleSet::Default());

  // Act
  search.Explore(selection);

  // Assert: the predicate moved into the scan group's filter.
  ASSERT_TRUE(search.GetMemo().Get(scan).filter);
  EXPECT_EQ(search.GetMemo().Get(scan).filter->ToString(),
            EqExp("a.x", Value(1))->ToString());
}

TEST(CascadesTest, MergeSelectionsCollapsesSelectionChain) {
  // Arrange: Selection(p2) over a group whose expression is Selection(p1).
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId inner = memo.EnsureDerivedGroup({"a"}, "selection");
  LogicalExpression first;
  first.operation = LogicalOperator::kSelection;
  first.children = {scan};
  first.predicate = EqExp("a.x", Value(1));
  ASSERT_TRUE(memo.AddExpression(inner, first));
  const GroupId outer = memo.EnsureDerivedGroup({"a"}, "selection2");
  LogicalExpression second;
  second.operation = LogicalOperator::kSelection;
  second.children = {inner};
  second.predicate = EqExp("a.y", Value(2));
  ASSERT_TRUE(memo.AddExpression(outer, second));
  SearchEngine search(std::move(memo), RuleSet::Default());

  // Act
  search.Explore(outer);

  // Assert: the outer group gained a merged Selection over the same child.
  const Group& group = search.GetMemo().Get(outer);
  ASSERT_EQ(group.expressions.size(), 2U);
  EXPECT_TRUE(group.expressions[1].children == std::vector<GroupId>{inner});
}

TEST(CascadesTest, PushSelectionThroughJoinMovesSingleRelationConjuncts) {
  // Arrange: a hand-built memo (no build-time distribution) where a
  // Selection over the join root mixes a single-relation conjunct with a
  // spanning one.
  Memo memo;
  const GroupId root = memo.Build({"a", "b"});
  const GroupId selection = memo.EnsureDerivedGroup({"a", "b"}, "selection");
  LogicalExpression expression;
  expression.operation = LogicalOperator::kSelection;
  expression.children = {root};
  expression.predicate = BinaryExpressionExp(
      EqExp("a.x", Value(1)), BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp("a.y"), BinaryOperation::kEquals,
                          ColumnValueExp("b.z")));
  ASSERT_TRUE(memo.AddExpression(selection, expression));
  SearchEngine search(std::move(memo), RuleSet::Default());

  // Act
  search.Explore(selection);

  // Assert: the single-relation conjunct reached the scan filter of a; the
  // spanning conjunct cannot move and stays in the Selection.
  const Memo& explored = search.GetMemo();
  bool found_filter = false;
  for (size_t group = 0; group < explored.GroupCount(); ++group) {
    const Group& candidate = explored.Get(group);
    if (candidate.relations == std::vector<std::string>{"a"} && candidate.filter) {
      EXPECT_EQ(candidate.filter->ToString(),
                EqExp("a.x", Value(1))->ToString());
      found_filter = true;
    }
  }
  EXPECT_TRUE(found_filter);
}

TEST(CascadesTest, JoinEnumerationPrunesDisconnectedBipartitions) {
  // Arrange: a connected conjunct between a and b only; associativity rules
  // are removed so the root count isolates join_enumeration's pruning.
  std::vector<ConjunctInfo> conjuncts;
  conjuncts.push_back({BinaryExpressionExp(ColumnValueExp("a.x"),
                                           BinaryOperation::kEquals,
                                           ColumnValueExp("b.y")),
                       {"a", "b"}});
  Memo pruned;
  const GroupId pruned_root = pruned.Build({"a", "b", "c"}, conjuncts);
  Memo exhaustive;
  const GroupId exhaustive_root = exhaustive.Build({"a", "b", "c"});
  RuleSet rules = RuleSet::Default();
  ASSERT_TRUE(rules.Remove("join_associativity_left"));
  ASSERT_TRUE(rules.Remove("join_associativity_right"));
  SearchEngine pruned_search(std::move(pruned), rules);
  SearchEngine exhaustive_search(std::move(exhaustive), rules);

  // Act
  pruned_search.Explore(pruned_root);
  exhaustive_search.Explore(exhaustive_root);

  // Assert: the {a,b}|{c} cut carries no conjunct and is pruned; the fully
  // disconnected graph keeps exhaustive enumeration.
  EXPECT_EQ(pruned_search.GetMemo().ExpressionCount(pruned_root), 4U);
  EXPECT_EQ(exhaustive_search.GetMemo().ExpressionCount(exhaustive_root), 6U);
}

TEST(CascadesTest, ExploreConvergesOnWideJoinGraphsWithoutPassCap) {
  // Arrange: an eight-relation join graph exercises the Phase 7 worklist
  // (the old implementation capped exploration at 64 passes).
  Memo memo;
  std::vector<std::string> relations;
  for (char name = 'a'; name < 'i'; ++name) {
    relations.emplace_back(1, name);
  }
  const GroupId root = memo.Build(relations);
  SearchEngine search(std::move(memo), RuleSet::Default());

  // Act + Assert: exploration terminates and populates the root group.
  search.Explore(root);
  EXPECT_GT(search.GetMemo().ExpressionCount(root), 0U);
}

TEST(CascadesTest, ExpressionCapDegradesGracefully) {
  // Arrange: eight relations enumerate more root expressions than a small
  // per-group cap allows.
  Memo memo(64);
  std::vector<std::string> relations;
  for (char name = 'a'; name < 'i'; ++name) {
    relations.emplace_back(1, name);
  }
  const GroupId root = memo.Build(relations);
  SearchEngine search(std::move(memo), RuleSet::Default());

  // Act + Assert: exploration completes without throwing and reports the
  // degradation instead of growing without bound.
  search.Explore(root);
  EXPECT_TRUE(search.GetMemo().Degraded());
}

TEST(CascadesTest, RequiredChildPropertiesDropJoinAndAggregationRequirements) {
  // Arrange: a demanding parent requirement.
  PhysicalProperties required;
  required.require_row_position = true;
  required.ordering = {ColumnName("x")};
  required.limit_hint = 5;

  LogicalExpression join;
  join.operation = LogicalOperator::kJoin;
  LogicalExpression selection;
  selection.operation = LogicalOperator::kSelection;
  LogicalExpression aggregation;
  aggregation.operation = LogicalOperator::kAggregation;
  LogicalExpression scan;
  scan.operation = LogicalOperator::kScan;

  // Act
  const std::vector<PhysicalProperties> join_children =
      SearchEngine::RequiredChildProperties(join, required);
  const std::vector<PhysicalProperties> selection_children =
      SearchEngine::RequiredChildProperties(selection, required);
  const std::vector<PhysicalProperties> aggregation_children =
      SearchEngine::RequiredChildProperties(aggregation, required);
  const std::vector<PhysicalProperties> scan_children =
      SearchEngine::RequiredChildProperties(scan, required);

  // Assert: joins and aggregations drop row-position/ordering requirements;
  // Selection passes them through; scans have no children.
  EXPECT_EQ(join_children.size(), 2U);
  EXPECT_FALSE(join_children[0].require_row_position);
  EXPECT_TRUE(join_children[0].ordering.empty());
  EXPECT_EQ(selection_children.size(), 1U);
  EXPECT_TRUE(selection_children[0].require_row_position);
  EXPECT_EQ(selection_children[0].ordering.size(), 1U);
  EXPECT_EQ(selection_children[0].limit_hint, 5U);
  EXPECT_EQ(aggregation_children.size(), 1U);
  EXPECT_FALSE(aggregation_children[0].require_row_position);
  EXPECT_TRUE(scan_children.empty());
}

TEST(CascadesTest, PropertyKeysIncludeAccessMethodLimitHintAndDistribution) {
  // Arrange: properties that differ only in the reserved Phase 5 fields.
  PhysicalProperties base;
  PhysicalProperties hinted = base;
  hinted.limit_hint = 10;
  PhysicalProperties indexed = base;
  indexed.access_method = AccessMethod::kPreferIndex;
  PhysicalProperties distributed = base;
  distributed.distribution = Distribution::kSingleNode;

  // Act + Assert: every distinguishing field participates in the cache key.
  EXPECT_NE(base.Key(), hinted.Key());
  EXPECT_NE(base.Key(), indexed.Key());
  EXPECT_NE(base.Key(), distributed.Key());
}

TEST(CascadesTest, MemoDumpsGroupsAndExpressions) {
  // Arrange: a small memo with a conjunct.
  Memo memo;
  const GroupId root = memo.Build(
      {"a", "b"},
      {{EqExp("a.x", Value(1)), {"a"}},
       {BinaryExpressionExp(ColumnValueExp("a.y"), BinaryOperation::kEquals,
                            ColumnValueExp("b.z")),
        {"a", "b"}}});

  // Act
  std::ostringstream dump;
  memo.Dump(dump);

  // Assert: groups, the scan filter and expressions are visible.
  const std::string text = dump.str();
  EXPECT_NE(text.find("group 0"), std::string::npos);
  EXPECT_NE(text.find("filter="), std::string::npos);
  EXPECT_NE(text.find("expressions=1"), std::string::npos);
  (void)root;
}

TEST(CascadesTest, MergeProjectionsComposesTargetLists) {
  using namespace dsl;
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId inner = memo.EnsureDerivedGroup({"a"}, "inner-proj");
  LogicalExpression inner_proj;
  inner_proj.operation = LogicalOperator::kProjection;
  inner_proj.children = {scan};
  inner_proj.target_list = {NamedExpression("x", ColumnValueExp("a.x"))};
  ASSERT_TRUE(memo.AddExpression(inner, inner_proj));
  const GroupId outer = memo.EnsureDerivedGroup({"a"}, "outer-proj");
  LogicalExpression outer_proj;
  outer_proj.operation = LogicalOperator::kProjection;
  outer_proj.children = {inner};
  outer_proj.target_list = {NamedExpression("y", ColumnValueExp("x"))};
  ASSERT_TRUE(memo.AddExpression(outer, outer_proj));
  SearchEngine search(std::move(memo), RuleSet::Default());

  search.Explore(outer);

  bool composed = false;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(outer).expressions) {
    if (expression.operation == LogicalOperator::kProjection &&
        expression.children == std::vector<GroupId>{scan} &&
        !expression.target_list.empty() &&
        expression.target_list[0].expression->ToString() ==
            ColumnValueExp("a.x")->ToString()) {
      composed = true;
    }
  }
  EXPECT_TRUE(composed);
}

TEST(CascadesTest, MergeLimitsComposesNestedLimits) {
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId inner = memo.EnsureDerivedGroup({"a"}, "inner-limit");
  LogicalExpression inner_limit;
  inner_limit.operation = LogicalOperator::kLimit;
  inner_limit.children = {scan};
  inner_limit.limit_count = 10;
  inner_limit.limit_offset = 0;
  ASSERT_TRUE(memo.AddExpression(inner, inner_limit));
  const GroupId outer = memo.EnsureDerivedGroup({"a"}, "outer-limit");
  LogicalExpression outer_limit;
  outer_limit.operation = LogicalOperator::kLimit;
  outer_limit.children = {inner};
  outer_limit.limit_count = 3;
  outer_limit.limit_offset = 1;
  ASSERT_TRUE(memo.AddExpression(outer, outer_limit));
  SearchEngine search(std::move(memo), RuleSet::Default());

  search.Explore(outer);

  bool merged = false;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(outer).expressions) {
    if (expression.operation == LogicalOperator::kLimit &&
        expression.children == std::vector<GroupId>{scan} &&
        expression.limit_count == 3 && expression.limit_offset == 1) {
      merged = true;
    }
  }
  EXPECT_TRUE(merged);
}

TEST(CascadesTest, EliminateTrueSelectionCopiesChildScan) {
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId selection = memo.EnsureDerivedGroup({"a"}, "true-sel");
  LogicalExpression tautology;
  tautology.operation = LogicalOperator::kSelection;
  tautology.children = {scan};
  tautology.predicate = ConstantValueExp(Value(true));
  ASSERT_TRUE(memo.AddExpression(selection, tautology));
  SearchEngine search(std::move(memo), RuleSet::Default());

  search.Explore(selection);

  bool has_scan = false;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(selection).expressions) {
    if (expression.operation == LogicalOperator::kScan &&
        expression.table == "a") {
      has_scan = true;
    }
  }
  EXPECT_TRUE(has_scan);
}

TEST(CascadesTest, EliminateFalseSelectionBecomesEmptyLimit) {
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId selection = memo.EnsureDerivedGroup({"a"}, "false-sel");
  LogicalExpression contradiction;
  contradiction.operation = LogicalOperator::kSelection;
  contradiction.children = {scan};
  contradiction.predicate = ConstantValueExp(Value(false));
  ASSERT_TRUE(memo.AddExpression(selection, contradiction));
  SearchEngine search(std::move(memo), RuleSet::Default());

  search.Explore(selection);

  bool has_empty_limit = false;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(selection).expressions) {
    if (expression.operation == LogicalOperator::kLimit &&
        expression.children == std::vector<GroupId>{scan} &&
        expression.limit_count == 0 && expression.limit_offset == 0) {
      has_empty_limit = true;
    }
  }
  EXPECT_TRUE(has_empty_limit);
}

TEST(CascadesTest, DefaultRulesIncludePredicateAndProjectionTransforms) {
  const RuleSet& rules = RuleSet::Default();
  EXPECT_TRUE(rules.Contains("merge_projections"));
  EXPECT_TRUE(rules.Contains("push_selection_through_projection"));
  EXPECT_TRUE(rules.Contains("push_limit_through_projection"));
  EXPECT_TRUE(rules.Contains("push_selection_through_aggregation"));
  EXPECT_TRUE(rules.Contains("infer_join_predicates"));
  EXPECT_TRUE(rules.Contains("merge_limits"));
  EXPECT_TRUE(rules.Contains("eliminate_true_selection"));
  EXPECT_TRUE(rules.Contains("eliminate_false_selection"));
}

TEST(CascadesTest, PushLimitThroughProjectionAddsLimitBelow) {
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId projection = memo.EnsureDerivedGroup({"a"}, "projection");
  LogicalExpression proj;
  proj.operation = LogicalOperator::kProjection;
  proj.children = {scan};
  proj.target_list = {NamedExpression("x", ColumnValueExp("a.x"))};
  ASSERT_TRUE(memo.AddExpression(projection, proj));
  const GroupId limit = memo.EnsureDerivedGroup({"a"}, "limit");
  LogicalExpression lim;
  lim.operation = LogicalOperator::kLimit;
  lim.children = {projection};
  lim.limit_count = 5;
  lim.limit_offset = 0;
  ASSERT_TRUE(memo.AddExpression(limit, lim));
  SearchEngine search(std::move(memo), RuleSet::Default());

  search.Explore(limit);

  bool projected_limit = false;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(limit).expressions) {
    if (expression.operation != LogicalOperator::kProjection) { continue;
}
    const Group& child = search.GetMemo().Get(expression.children[0]);
    projected_limit = std::ranges::any_of(
        child.expressions, [](const LogicalExpression& candidate) {
          return candidate.operation == LogicalOperator::kLimit &&
                 candidate.limit_count == 5;
        });
  }
  EXPECT_TRUE(projected_limit);
}

TEST(CascadesTest, InferJoinPredicatesCopiesConstantsAcrossEquals) {
  Memo memo;
  const GroupId root = memo.Build(
      {"a", "b"},
      {{EqExp("a.x", Value(1)), {"a"}},
       {BinaryExpressionExp(ColumnValueExp("a.x"), BinaryOperation::kEquals,
                            ColumnValueExp("b.y")),
        {"a", "b"}}});
  SearchEngine search(std::move(memo), RuleSet::Default());

  search.Explore(root);

  const Group& b = search.GetMemo().Get(search.GetMemo().EnsureGroup({"b"}));
  ASSERT_TRUE(b.filter);
  EXPECT_NE(b.filter->ToString().find("b.y"), std::string::npos);
  EXPECT_NE(b.filter->ToString().find("1"), std::string::npos);
}

namespace {

using dsl::Scan;

// Schema-carrying opaque plan so join implementation rules can run without
// catalog tables; only GetSchema()/stats are consulted during costing.
Plan StubLeafPlan(ColumnName key) {
  return std::make_shared<RelationalPlan>(
      std::make_shared<SelectStatement>(std::vector<NamedExpression>{},
                                        std::vector<std::string>{},
                                        Expression()),
      Schema("", std::vector<Column>{Column(std::move(key))}));
}

// Implements scan groups "a"/"b" with the given stubs and also includes
// the default semi/anti join implementation rules so that join groups can
// be optimized end-to-end.
ImplementationRuleSet RulesWithStubLeaves(Plan left_stub, Plan right_stub) {
  ImplementationRuleSet rules;
  rules.Add(ImplementationRule(
      "stub_scan", Scan("scan"),
      [left_stub, right_stub](GroupId, const Memo&, const Bindings&,
                              const LogicalExpression& logical,
                              const std::vector<BestPlan>&,
                              const PhysicalProperties&, const RuleContext&) {
        if (logical.table == "a") {
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = left_stub, .local_cost = 1,
                              .estimated_rows = 10}};
        }
        if (logical.table == "b") {
          return std::vector<PlanAlternative>{
              PlanAlternative{.plan = right_stub, .local_cost = 1,
                              .estimated_rows = 10}};
        }
        return std::vector<PlanAlternative>{};
      },
      LogicalOperator::kScan));
  // Include default join-family implementation rules so the optimizer can
  // select hash/semi/anti join strategies.  Scan/projection/aggregation/
  // limit rules are excluded because they require catalog context that
  // these tests do not provide.
  static const std::unordered_set<std::string> kJoinRules = {
      "hash_join",    "semi_hash_join", "anti_hash_join",
      "index_join",   "nested_loop_join"};
  for (const auto& rule : DefaultImplementationRules().Rules()) {
    if (kJoinRules.contains(rule.Name())) {
      rules.Add(rule);
    }
  }
  return rules;
}

// Builds {a} ⋈ {b} on a.x = b.y with the requested membership semantics.
// Uses a single Build call so the relation index stays consistent.
GroupId BuildMembershipJoin(Memo& memo, LogicalJoinKind kind,
                            Expression extra_conjunct = nullptr) {
  const GroupId root = memo.Build({"a", "b"});
  // Extract the two scan child groups from the initial join expression.
  const Group& root_group = memo.Get(root);
  EXPECT_EQ(root_group.expressions.size(), 1U);
  const GroupId left = root_group.expressions[0].children[0];
  const GroupId right = root_group.expressions[0].children[1];
  const GroupId join_group =
      memo.EnsureDerivedGroup({"a", "b"}, "membership-join");
  LogicalExpression join = memo.NewJoin(left, right, kind);
  join.predicate = BinaryExpressionExp(
      ColumnValueExp("a.x"), BinaryOperation::kEquals, ColumnValueExp("b.y"));
  if (extra_conjunct) {
    join.predicate = BinaryExpressionExp(*join.predicate,
                                         BinaryOperation::kAnd,
                                         extra_conjunct);
  }
  EXPECT_TRUE(memo.AddExpression(join_group, join));
  return join_group;
}

}  // namespace

TEST(CascadesTest, SemiHashJoinImplementationRuleChoosesSemiKind) {
  Memo memo;
  const GroupId root = BuildMembershipJoin(memo, LogicalJoinKind::kSemi);
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);

  std::optional<BestPlan> best =
      search.Optimize(root, PhysicalProperties{},
                      RulesWithStubLeaves(StubLeafPlan(ColumnName("a", "x")),
                                          StubLeafPlan(ColumnName("b", "y"))));

  ASSERT_TRUE(best.has_value());
  // Semi joins keep only the probe side's columns.
  EXPECT_NE(best->plan->ToString().find("Semi Join"), std::string::npos);
  EXPECT_EQ(best->plan->GetSchema().ColumnCount(), 1U);
}

TEST(CascadesTest, AntiHashJoinImplementationRuleChoosesAntiKind) {
  Memo memo;
  const GroupId root = BuildMembershipJoin(memo, LogicalJoinKind::kAnti);
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);

  std::optional<BestPlan> best =
      search.Optimize(root, PhysicalProperties{},
                      RulesWithStubLeaves(StubLeafPlan(ColumnName("a", "x")),
                                          StubLeafPlan(ColumnName("b", "y"))));

  ASSERT_TRUE(best.has_value());
  EXPECT_NE(best->plan->ToString().find("Anti Join"), std::string::npos);
  EXPECT_EQ(best->plan->GetSchema().ColumnCount(), 1U);
}

TEST(CascadesTest, MembershipJoinRulesRejectResidualConjuncts) {
  // A non-equality conjunct cannot be re-applied above a membership join
  // without changing semantics, so no physical alternative may be offered.
  Memo memo;
  const GroupId root = BuildMembershipJoin(
      memo, LogicalJoinKind::kSemi,
      BinaryExpressionExp(ColumnValueExp("a.x"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(1))));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);

  std::optional<BestPlan> best =
      search.Optimize(root, PhysicalProperties{},
                      RulesWithStubLeaves(StubLeafPlan(ColumnName("a", "x")),
                                          StubLeafPlan(ColumnName("b", "y"))));
  EXPECT_FALSE(best.has_value());
}

TEST(CascadesTest, InnerRulesSkipMembershipJoins) {
  // Commutativity must not swap semi/anti children: only the left side
  // survives, so the memo must not derive an inner-style swapped expression.
  Memo memo;
  const GroupId root = BuildMembershipJoin(memo, LogicalJoinKind::kSemi);
  SearchEngine search(std::move(memo), RuleSet::Default());

  search.Explore(root);

  for (const LogicalExpression& expression :
       search.GetMemo().Get(root).expressions) {
    if (expression.operation == LogicalOperator::kJoin) {
      EXPECT_EQ(expression.join_kind, LogicalJoinKind::kSemi);
      EXPECT_EQ(expression.children.size(), 2U);
    }
  }
}

TEST(CascadesTest, DefaultImplementationRulesIncludeMembershipKinds) {
  const ImplementationRuleSet& rules = DefaultImplementationRules();
  EXPECT_TRUE(rules.Contains("semi_hash_join"));
  EXPECT_TRUE(rules.Contains("anti_hash_join"));
}

}  // namespace tinylamb::cascades
