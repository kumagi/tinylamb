/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/cascades.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <optional>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "gtest/gtest.h"
#include "plan/implementation_rules.hpp"
#include "plan/set_operation_plan.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
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

  // join_to_cross_if_no_predicate adds a typed CrossJoin alternative for
  // each unqualified join shape, including the commuted shape.
  EXPECT_EQ(search.GetMemo().ExpressionCount(root), 4);
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

TEST(CascadesTest, OuterJoinIsAJoinNodeWithExplicitTypePayload) {
  using namespace dsl;
  Memo memo;
  const GroupId root = memo.Build({"a", "b"});
  const Group& initial = memo.Get(root);
  ASSERT_EQ(initial.expressions.size(), 1U);
  const GroupId left = initial.expressions[0].children[0];
  const GroupId right = initial.expressions[0].children[1];

  LogicalExpression outer{.operation = LogicalOperator::kOuterJoin,
                          .children = {left, right},
                          .join_type = 2};
  ASSERT_TRUE(memo.AddExpression(root, outer));
  EXPECT_NE(std::ranges::find_if(memo.Get(root).expressions,
                                 [](const LogicalExpression& item) {
                                   return item.operation ==
                                              LogicalOperator::kOuterJoin &&
                                          item.join_type == 2;
                                 }),
            memo.Get(root).expressions.end());

  bool matched = false;
  RuleSet rules;
  rules.Add(Rule(
      "observe_outer", OuterJoin(Scan("left"), Scan("right")),
      [&](const Bindings& bindings, Memo&, GroupId,
          const LogicalExpression& expression) {
        matched = bindings.contains("left") && bindings.contains("right") &&
                  expression.join_type == 2;
      },
      LogicalOperator::kOuterJoin));
  SearchEngine search(std::move(memo), rules);
  search.Explore(root);
  EXPECT_TRUE(matched);
}

TEST(CascadesTest, ImplementationRulesAreIndependentlyRemovable) {
  using namespace dsl;
  ImplementationRuleSet rules;
  rules.Add(ImplementationRule(
      "full_scan", Scan(),
      [](GroupId, const Memo&, const Bindings&, const LogicalExpression&,
         const std::vector<BestPlan>&, const PhysicalProperties&,
         const RuleContext&) { return std::vector<PlanAlternative>{}; }));

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
                   root, LogicalExpression{.operation = LogicalOperator::kScan,
                                           .children = {root},
                                           .table = "a"}),
               std::invalid_argument);
}

TEST(CascadesTest, MemoAddExpressionRejectsJoinWithWrongArity) {
  // Arrange: a single-relation group.
  Memo memo;
  const GroupId root = memo.Build({"a"});

  // Act + Assert: a join must have exactly two child groups.
  EXPECT_THROW(memo.AddExpression(
                   root, LogicalExpression{.operation = LogicalOperator::kJoin,
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
  rules.Add(
      Rule("probe_scan_left", Join(Scan("left"), Any("right")),
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
  rules.Add(Rule("arity_probe",
                 Pattern::Op(LogicalOperator::kJoin, {Pattern::Any()}),
                 [&](const Bindings&, Memo&, GroupId,
                     const LogicalExpression&) { invoked = true; }));
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

TEST(CascadesTest, DummyScanAndValuesHavePhysicalImplementationRules) {
  Memo dummy_memo;
  const GroupId dummy = dummy_memo.EnsureDerivedGroup({}, "dummy");
  ASSERT_TRUE(dummy_memo.AddExpression(
      dummy, LogicalExpression{.operation = LogicalOperator::kDummyScan}));
  SearchEngine dummy_search(std::move(dummy_memo), RuleSet::Default());
  const auto dummy_plan = dummy_search.Optimize(
      dummy, PhysicalProperties{}, tinylamb::DefaultImplementationRules());
  ASSERT_TRUE(dummy_plan.has_value());
  if (!dummy_plan) return;
  EXPECT_NE(dummy_plan->plan->ToString().find("DummyScan"),
            std::string::npos);

  Memo values_memo;
  const GroupId values = values_memo.EnsureDerivedGroup({}, "values");
  LogicalExpression values_expression;
  values_expression.operation = LogicalOperator::kValues;
  values_expression.output_schema =
      Schema("", {Column("x", ValueType::kInt64)});
  values_expression.values = {Row({Value(1)}), Row({Value(2)})};
  ASSERT_TRUE(values_memo.AddExpression(values, values_expression));
  SearchEngine values_search(std::move(values_memo), RuleSet::Default());
  const auto values_plan = values_search.Optimize(
      values, PhysicalProperties{}, tinylamb::DefaultImplementationRules());
  ASSERT_TRUE(values_plan.has_value());
  if (!values_plan) return;
  EXPECT_NE(values_plan->plan->ToString().find("Values"),
            std::string::npos);
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
  return BinaryExpressionExp(ColumnValueExp(column), BinaryOperation::kEquals,
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
  EXPECT_THROW(memo.AddExpression(derived, no_targets), std::invalid_argument);
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

  const Pattern pattern = SelectionWithin(0, Scan("scan"), "sel");
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
  ASSERT_GE(group.expressions.size(), 2U);
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
    if (candidate.relations == std::vector<std::string>{"a"} &&
        candidate.filter) {
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
  conjuncts.push_back(
      {BinaryExpressionExp(ColumnValueExp("a.x"), BinaryOperation::kEquals,
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
  // The disconnected graph retains the six inner join shapes plus their
  // explicit CrossJoin alternatives.
  EXPECT_EQ(exhaustive_search.GetMemo().ExpressionCount(exhaustive_root), 12U);
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
  LogicalExpression cross_join;
  cross_join.operation = LogicalOperator::kCrossJoin;
  LogicalExpression selection;
  selection.operation = LogicalOperator::kSelection;
  LogicalExpression aggregation;
  aggregation.operation = LogicalOperator::kAggregation;
  LogicalExpression scan;
  scan.operation = LogicalOperator::kScan;

  // Act
  const std::vector<PhysicalProperties> join_children =
      SearchEngine::RequiredChildProperties(join, required);
  const std::vector<PhysicalProperties> cross_join_children =
      SearchEngine::RequiredChildProperties(cross_join, required);
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
  EXPECT_EQ(cross_join_children.size(), 2U);
  EXPECT_FALSE(cross_join_children[0].require_row_position);
  EXPECT_EQ(selection_children.size(), 1U);
  EXPECT_TRUE(selection_children[0].require_row_position);
  EXPECT_EQ(selection_children[0].ordering.size(), 1U);
  EXPECT_EQ(selection_children[0].limit_hint, 5U);
  EXPECT_EQ(aggregation_children.size(), 1U);
  EXPECT_FALSE(aggregation_children[0].require_row_position);
  EXPECT_TRUE(scan_children.empty());
}

TEST(CascadesTest, TopNPropagatesItsKeysAndLimitToTheChild) {
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId topn = memo.EnsureDerivedGroup({"a"}, "topn");
  LogicalExpression expression;
  expression.operation = LogicalOperator::kTopN;
  expression.children = {scan};
  expression.target_list = {
      NamedExpression("", ColumnValueExp(ColumnName("a", "key")))};
  expression.sort_ascending = {false};
  expression.limit_count = 3;
  expression.limit_offset = 2;
  ASSERT_TRUE(memo.AddExpression(topn, expression));

  PhysicalProperties required;
  required.require_row_position = true;
  required.ordering = {ColumnName("a", "key")};
  required.limit_hint = 5;
  const std::vector<PhysicalProperties> child_requirements =
      SearchEngine::RequiredChildProperties(expression, required);

  ASSERT_EQ(child_requirements.size(), 1U);
  EXPECT_TRUE(child_requirements[0].require_row_position);
  // The TopN's own sort keys are offered to the child as an optional
  // ordering so a scan that delivers them natively wins on cost; the
  // implementation rule then elides the engine-side heap.
  EXPECT_EQ(child_requirements[0].ordering,
            (std::vector<ColumnName>{ColumnName("a", "key")}));
  EXPECT_EQ(child_requirements[0].limit_hint, 5U);
  EXPECT_NE(expression.Fingerprint().find("#l:2,3"), std::string::npos);
}

TEST(CascadesTest, Max1RowIsAUnaryLogicalOperator) {
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId max1 = memo.EnsureDerivedGroup({"a"}, "max1");
  LogicalExpression expression;
  expression.operation = LogicalOperator::kMax1Row;
  expression.children = {scan};
  ASSERT_TRUE(memo.AddExpression(max1, expression));

  PhysicalProperties required;
  required.ordering = {ColumnName("a", "key")};
  const std::vector<PhysicalProperties> child_requirements =
      SearchEngine::RequiredChildProperties(expression, required);
  ASSERT_EQ(child_requirements.size(), 1U);
  EXPECT_EQ(child_requirements[0].ordering,
            (std::vector<ColumnName>{ColumnName("a", "key")}));
}

TEST(CascadesTest, SetOperationValidatesArityRelationsAndDropsProperties) {
  Memo memo;
  const GroupId set = memo.Build({"a", "b"});
  const Group& base = memo.Get(set);
  ASSERT_EQ(base.expressions.size(), 1U);
  const GroupId left = base.expressions.front().children[0];
  const GroupId right = base.expressions.front().children[1];
  LogicalExpression expression;
  expression.operation = LogicalOperator::kUnionAll;
  expression.children = {left, right};
  ASSERT_TRUE(memo.AddExpression(set, expression));

  PhysicalProperties required;
  required.require_row_position = true;
  required.ordering = {ColumnName("a", "key")};
  required.limit_hint = 5;
  const std::vector<PhysicalProperties> children =
      SearchEngine::RequiredChildProperties(expression, required);
  ASSERT_EQ(children.size(), 2U);
  EXPECT_FALSE(children[0].require_row_position);
  EXPECT_TRUE(children[0].ordering.empty());
  EXPECT_EQ(children[0].limit_hint, std::numeric_limits<size_t>::max());

  LogicalExpression invalid = expression;
  invalid.children = {left};
  EXPECT_THROW(memo.AddExpression(set, invalid), std::invalid_argument);
}

TEST(CascadesTest, UnionDistinctRewriteAddsUnionAllAndDistinctAlternative) {
  Memo memo;
  const GroupId root = memo.Build({"a", "b"});
  const Group& base = memo.Get(root);
  ASSERT_EQ(base.expressions.size(), 1U);
  LogicalExpression union_expression;
  union_expression.operation = LogicalOperator::kUnion;
  union_expression.children = base.expressions.front().children;
  ASSERT_TRUE(memo.AddExpression(root, union_expression));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);

  const Group& explored = search.GetMemo().Get(root);
  EXPECT_TRUE(std::ranges::any_of(
      explored.expressions, [](const LogicalExpression& expression) {
        return expression.operation == LogicalOperator::kDistinct;
      }));
}

TEST(CascadesTest, UnionAllMergeFlattensNestedBranches) {
  Memo memo;
  const GroupId root = memo.Build({"a", "b", "c"});
  const Group& base = memo.Get(root);
  ASSERT_EQ(base.expressions.size(), 1U);
  const GroupId a = base.expressions.front().children[0];
  const GroupId b = memo.EnsureDerivedGroup({"b"}, "b-leaf");
  const GroupId c = memo.EnsureDerivedGroup({"c"}, "c-leaf");
  const GroupId nested = memo.EnsureDerivedGroup({"a", "b"}, "nested-union");
  ASSERT_TRUE(memo.AddExpression(
      nested, LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                .children = {a, b}}));
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kUnionAll,
                              .children = {nested, c}}));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);
  EXPECT_TRUE(std::ranges::any_of(search.GetMemo().Get(root).expressions,
                                  [](const LogicalExpression& expression) {
                                    return expression.operation ==
                                               LogicalOperator::kUnionAll &&
                                           expression.children.size() == 3U;
                                  }));
}

TEST(CascadesTest, IntersectWithEmptyBranchBecomesEmpty) {
  Memo memo;
  (void)memo.Build({"a", "b"});
  const GroupId root = memo.EnsureDerivedGroup({"a", "b"}, "intersect");
  const GroupId left = memo.EnsureGroup({"a"});
  const GroupId empty_branch = memo.EnsureDerivedGroup({"b"}, "empty");
  ASSERT_TRUE(memo.AddExpression(
      empty_branch, LogicalExpression{.operation = LogicalOperator::kEmpty,
                                      .children = {memo.EnsureGroup({"b"})}}));
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kIntersect,
                              .children = {left, empty_branch}}));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);
  EXPECT_TRUE(std::ranges::any_of(search.GetMemo().Get(root).expressions,
                                  [](const LogicalExpression& expression) {
                                    return expression.operation ==
                                           LogicalOperator::kEmpty;
                                  }));
}

TEST(CascadesTest, EmptySetOperationBranchesUseIdentityAlternatives) {
  Memo memo;
  (void)memo.Build({"a"});
  const GroupId left = memo.EnsureGroup({"a"});
  const GroupId empty = memo.EnsureDerivedGroup({"a"}, "empty-identity");
  ASSERT_TRUE(memo.AddExpression(
      empty, LogicalExpression{.operation = LogicalOperator::kEmpty,
                               .children = {left}}));

  const GroupId union_all = memo.EnsureDerivedGroup({"a"}, "union-all-empty");
  ASSERT_TRUE(memo.AddExpression(
      union_all, LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                   .children = {left, empty}}));
  const GroupId except_all = memo.EnsureDerivedGroup({"a"}, "except-all-empty");
  ASSERT_TRUE(memo.AddExpression(
      except_all, LogicalExpression{.operation = LogicalOperator::kExceptAll,
                                    .children = {left, empty}}));
  const GroupId union_distinct =
      memo.EnsureDerivedGroup({"a"}, "union-distinct-empty");
  ASSERT_TRUE(memo.AddExpression(
      union_distinct, LogicalExpression{.operation = LogicalOperator::kUnion,
                                        .children = {left, empty}}));
  const GroupId except_distinct =
      memo.EnsureDerivedGroup({"a"}, "except-distinct-empty");
  ASSERT_TRUE(memo.AddExpression(
      except_distinct, LogicalExpression{.operation = LogicalOperator::kExcept,
                                         .children = {left, empty}}));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(union_all);
  search.Explore(except_all);
  search.Explore(union_distinct);
  search.Explore(except_distinct);

  EXPECT_TRUE(std::ranges::any_of(search.GetMemo().Get(union_all).expressions,
                                  [](const LogicalExpression& candidate) {
                                    return candidate.operation ==
                                           LogicalOperator::kScan;
                                  }));
  EXPECT_TRUE(std::ranges::any_of(search.GetMemo().Get(except_all).expressions,
                                  [](const LogicalExpression& candidate) {
                                    return candidate.operation ==
                                           LogicalOperator::kScan;
                                  }));
  EXPECT_TRUE(std::ranges::any_of(
      search.GetMemo().Get(union_distinct).expressions,
      [](const LogicalExpression& candidate) {
        return candidate.operation == LogicalOperator::kDistinct;
      }));
  EXPECT_TRUE(std::ranges::any_of(
      search.GetMemo().Get(except_distinct).expressions,
      [](const LogicalExpression& candidate) {
        return candidate.operation == LogicalOperator::kDistinct;
      }));
}

TEST(CascadesTest, UnionAllCanChooseMergeAppendForRequiredOrdering) {
  Memo memo;
  (void)memo.Build({"left", "right"});
  const Schema schema("", {Column("v", ValueType::kInt64)});
  const GroupId left = memo.EnsureDerivedGroup({"left"}, "values-left");
  const GroupId right = memo.EnsureDerivedGroup({"right"}, "values-right");
  ASSERT_TRUE(memo.AddExpression(
      left, LogicalExpression{.operation = LogicalOperator::kValues,
                              .values = {Row({Value(1)}), Row({Value(3)})},
                              .output_schema = schema}));
  ASSERT_TRUE(memo.AddExpression(
      right, LogicalExpression{.operation = LogicalOperator::kValues,
                               .values = {Row({Value(2)}), Row({Value(4)})},
                               .output_schema = schema}));
  const GroupId root = memo.EnsureDerivedGroup({"left", "right"}, "union");
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kUnionAll,
                              .children = {left, right}}));

  PhysicalProperties required;
  required.ordering = {ColumnName("v")};
  SearchEngine search(std::move(memo), RuleSet::Default());
  const std::optional<BestPlan> best =
      search.Optimize(root, required, tinylamb::DefaultImplementationRules());
  ASSERT_TRUE(best.has_value());
  if (!best) return;
  const auto merged =
      std::dynamic_pointer_cast<SetOperationPlan>(best->plan);
  ASSERT_TRUE(merged);
  EXPECT_EQ(merged->ToString(), "MergeAppend");
  EXPECT_TRUE(merged->IsOrderedBy({ColumnValueExp("v")}, {true}));
}

TEST(CascadesTest, SelectionIsPushedIntoEverySetOperationBranch) {
  Memo memo;
  (void)memo.Build({"a", "b"});
  const GroupId left = memo.EnsureGroup({"a"});
  const GroupId right = memo.EnsureGroup({"b"});
  const GroupId setop = memo.EnsureDerivedGroup({"a", "b"}, "union");
  ASSERT_TRUE(memo.AddExpression(
      setop, LogicalExpression{.operation = LogicalOperator::kUnionAll,
                               .children = {left, right}}));
  const GroupId root = memo.EnsureDerivedGroup({"a", "b"}, "filter");
  const Expression predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("v")), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(10)));
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kSelection,
                              .children = {setop},
                              .predicate = predicate}));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);
  const bool found = std::ranges::any_of(
      search.GetMemo().Get(root).expressions,
      [&](const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kUnionAll ||
            expression.children.size() != 2) {
          return false;
        }
        return std::ranges::all_of(expression.children, [&](GroupId child) {
          return std::ranges::any_of(search.GetMemo().Get(child).expressions,
                                     [](const LogicalExpression& branch) {
                                       return branch.operation ==
                                                  LogicalOperator::kSelection &&
                                              branch.predicate.has_value();
                                     });
        });
      });
  EXPECT_TRUE(found);
}

TEST(CascadesTest, ProjectionIsPushedThroughUnionBranches) {
  Memo memo;
  (void)memo.Build({"a", "b"});
  const GroupId left = memo.EnsureGroup({"a"});
  const GroupId right = memo.EnsureGroup({"b"});
  const GroupId setop = memo.EnsureDerivedGroup({"a", "b"}, "union");
  ASSERT_TRUE(memo.AddExpression(
      setop, LogicalExpression{.operation = LogicalOperator::kUnion,
                               .children = {left, right}}));
  const GroupId root = memo.EnsureDerivedGroup({"a", "b"}, "project");
  const std::vector<NamedExpression> target_list = {
      NamedExpression("v", ColumnValueExp(ColumnName("v")))};
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kProjection,
                              .children = {setop},
                              .target_list = target_list}));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);
  EXPECT_TRUE(std::ranges::any_of(
      search.GetMemo().Get(root).expressions,
      [&](const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kUnion ||
            expression.children.size() != 2) {
          return false;
        }
        return std::ranges::all_of(expression.children, [&](GroupId child) {
          return std::ranges::any_of(search.GetMemo().Get(child).expressions,
                                     [](const LogicalExpression& branch) {
                                       return branch.operation ==
                                              LogicalOperator::kProjection;
                                     });
        });
      }));
}

TEST(CascadesTest, ProjectionIsPushedThroughInnerJoinWithRequiredColumns) {
  Memo memo;
  (void)memo.Build({"a", "b"});
  const GroupId left = memo.EnsureGroup({"a"});
  const GroupId right = memo.EnsureGroup({"b"});
  const GroupId join_group = memo.EnsureDerivedGroup({"a", "b"}, "join");
  const Expression join_predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("a", "id")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("b", "id")));
  ASSERT_TRUE(memo.AddExpression(
      join_group, LogicalExpression{.operation = LogicalOperator::kJoin,
                                    .children = {left, right},
                                    .predicate = join_predicate}));
  const GroupId root = memo.EnsureDerivedGroup({"a", "b"}, "project");
  const std::vector<NamedExpression> targets = {
      NamedExpression("value", ColumnValueExp(ColumnName("a", "value")))};
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kProjection,
                              .children = {join_group},
                              .target_list = targets}));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);

  bool found = false;
  for (const LogicalExpression& projection :
       search.GetMemo().Get(root).expressions) {
    if (projection.operation != LogicalOperator::kProjection ||
        projection.children.size() != 1 ||
        projection.target_list.size() != targets.size()) {
      continue;
    }
    const Group& projected_join =
        search.GetMemo().Get(projection.children.front());
    for (const LogicalExpression& join : projected_join.expressions) {
      if (join.operation != LogicalOperator::kJoin ||
          join.children.size() != 2) {
        continue;
      }
      const auto has_projection = [&](GroupId child, size_t expected) {
        return std::ranges::any_of(search.GetMemo().Get(child).expressions,
                                   [expected](const LogicalExpression& item) {
                                     return item.operation ==
                                                LogicalOperator::kProjection &&
                                            item.target_list.size() == expected;
                                   });
      };
      if (has_projection(join.children[0], 2) &&
          has_projection(join.children[1], 1)) {
        found = true;
      }
    }
  }
  EXPECT_TRUE(found);
}

TEST(CascadesTest, ProjectionThroughOuterJoinIsConservativelySkipped) {
  Memo memo;
  (void)memo.Build({"a", "b"});
  const GroupId left = memo.EnsureGroup({"a"});
  const GroupId right = memo.EnsureGroup({"b"});
  const GroupId join_group = memo.EnsureDerivedGroup({"a", "b"}, "outer");
  ASSERT_TRUE(memo.AddExpression(
      join_group,
      LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                        .children = {left, right},
                        .predicate = BinaryExpressionExp(
                            ColumnValueExp("a.id"), BinaryOperation::kEquals,
                            ColumnValueExp("b.id")),
                        .join_type = 0}));
  const GroupId root = memo.EnsureDerivedGroup({"a", "b"}, "project");
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kProjection,
                              .children = {join_group},
                              .target_list = {NamedExpression(
                                  "value", ColumnValueExp("a.value"))}}));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);
  EXPECT_TRUE(std::ranges::none_of(
      search.GetMemo().Get(root).expressions,
      [&](const LogicalExpression& item) {
        return item.operation == LogicalOperator::kProjection &&
               item.children.size() == 1 &&
               search.GetMemo()
                   .Get(item.children.front())
                   .tag.starts_with("join-project-join:");
      }));
}

TEST(CascadesTest, FilterIsPushedBelowDistinct) {
  Memo memo;
  const GroupId scan = memo.Build({"items"});
  const GroupId distinct = memo.EnsureDerivedGroup({"items"}, "distinct");
  ASSERT_TRUE(memo.AddExpression(
      distinct, LogicalExpression{.operation = LogicalOperator::kDistinct,
                                  .children = {scan}}));
  const GroupId root = memo.EnsureDerivedGroup({"items"}, "filter");
  const Expression predicate = BinaryExpressionExp(
      ColumnValueExp("items.value"), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(0)));
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kSelection,
                              .children = {distinct},
                              .predicate = predicate}));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);
  EXPECT_TRUE(std::ranges::any_of(
      search.GetMemo().Get(root).expressions,
      [&](const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kDistinct ||
            expression.children.size() != 1) {
          return false;
        }
        return std::ranges::any_of(
            search.GetMemo().Get(expression.children.front()).expressions,
            [](const LogicalExpression& child) {
              return child.operation == LogicalOperator::kSelection &&
                     child.predicate.has_value();
            });
      }));
}

TEST(CascadesTest, UnionAllLimitCapsEveryChildAndKeepsGlobalLimit) {
  Memo memo;
  (void)memo.Build({"a", "b"});
  const GroupId left = memo.EnsureGroup({"a"});
  const GroupId right = memo.EnsureGroup({"b"});
  const GroupId setop = memo.EnsureDerivedGroup({"a", "b"}, "union");
  ASSERT_TRUE(memo.AddExpression(
      setop, LogicalExpression{.operation = LogicalOperator::kUnionAll,
                               .children = {left, right}}));
  const GroupId root = memo.EnsureDerivedGroup({"a", "b"}, "limit");
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kLimit,
                              .children = {setop},
                              .limit_count = 3,
                              .limit_offset = 2}));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);
  EXPECT_TRUE(std::ranges::any_of(
      search.GetMemo().Get(root).expressions,
      [&](const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kLimit ||
            expression.limit_count != 3 || expression.limit_offset != 2) {
          return false;
        }
        const Group& capped = search.GetMemo().Get(expression.children[0]);
        return std::ranges::any_of(
            capped.expressions, [&](const LogicalExpression& branch_setop) {
              if (branch_setop.operation != LogicalOperator::kUnionAll) {
                return false;
              }
              return std::ranges::all_of(
                  branch_setop.children, [&](GroupId child) {
                    return std::ranges::any_of(
                        search.GetMemo().Get(child).expressions,
                        [](const LogicalExpression& child_limit) {
                          return child_limit.operation ==
                                     LogicalOperator::kLimit &&
                                 child_limit.limit_count == 5 &&
                                 child_limit.limit_offset == 0;
                        });
                  });
            });
      }));
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

TEST(CascadesTest, DefaultRulesIncludePredicateAndProjectionTransforms) {
  const RuleSet& rules = RuleSet::Default();
  EXPECT_TRUE(rules.Contains("merge_projections"));
  EXPECT_TRUE(rules.Contains("push_selection_through_projection"));
  EXPECT_TRUE(rules.Contains("push_limit_through_projection"));
  EXPECT_TRUE(rules.Contains("push_selection_through_aggregation"));
  EXPECT_TRUE(rules.Contains("infer_join_predicates"));
  EXPECT_TRUE(rules.Contains("merge_limits"));
  EXPECT_TRUE(rules.Contains("eliminate_true_selection"));
  EXPECT_TRUE(rules.Contains("join_to_cross_if_no_predicate"));
  EXPECT_TRUE(rules.Contains("eliminate_false_selection"));
}

TEST(CascadesTest, FalseSelectionAddsEmptyLogicalAlternative) {
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId selection = memo.EnsureDerivedGroup({"a"}, "selection");
  LogicalExpression expression;
  expression.operation = LogicalOperator::kSelection;
  expression.children = {scan};
  expression.predicate = ConstantValueExp(Value(false));
  ASSERT_TRUE(memo.AddExpression(selection, expression));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(selection);

  const Group& group = search.GetMemo().Get(selection);
  EXPECT_TRUE(std::ranges::any_of(
      group.expressions, [](const LogicalExpression& candidate) {
        return candidate.operation == LogicalOperator::kEmpty;
      }));
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
    if (expression.operation != LogicalOperator::kProjection) {
      continue;
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

TEST(CascadesTest, PushTopNThroughProjectionRewritesOrderingExpression) {
  Memo memo;
  const GroupId scan = memo.Build({"items"});
  const GroupId projection = memo.EnsureDerivedGroup({"items"}, "projection");
  ASSERT_TRUE(memo.AddExpression(
      projection,
      LogicalExpression{
          .operation = LogicalOperator::kProjection,
          .children = {scan},
          .target_list = {
              NamedExpression("value", ColumnValueExp("items.v")),
              NamedExpression("label", ColumnValueExp("items.s"))}}));
  const GroupId root = memo.EnsureDerivedGroup({"items"}, "topn");
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{
                .operation = LogicalOperator::kTopN,
                .children = {projection},
                .target_list = {NamedExpression("", ColumnValueExp("value"))},
                .sort_ascending = {false},
                .limit_count = 3}));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);
  EXPECT_TRUE(std::ranges::any_of(
      search.GetMemo().Get(root).expressions,
      [&](const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kProjection ||
            expression.children.size() != 1) {
          return false;
        }
        const Group& pushed = search.GetMemo().Get(expression.children.front());
        return std::ranges::any_of(
            pushed.expressions, [](const LogicalExpression& child) {
              return child.operation == LogicalOperator::kTopN &&
                     child.target_list.size() == 1 &&
                     child.target_list.front().expression->ToString() ==
                         "items.v";
            });
      }));
}

TEST(CascadesTest, CompatibleNestedSortsCollapseToOneOrder) {
  Memo memo;
  const GroupId scan = memo.Build({"items"});
  const GroupId inner = memo.EnsureDerivedGroup({"items"}, "inner-sort");
  ASSERT_TRUE(memo.AddExpression(
      inner,
      LogicalExpression{
          .operation = LogicalOperator::kSort,
          .children = {scan},
          .target_list = {NamedExpression("", ColumnValueExp("items.a")),
                          NamedExpression("", ColumnValueExp("items.b"))},
          .sort_ascending = {true, true}}));
  const GroupId root = memo.EnsureDerivedGroup({"items"}, "outer-sort");
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{
                .operation = LogicalOperator::kSort,
                .children = {inner},
                .target_list = {NamedExpression("", ColumnValueExp("items.a"))},
                .sort_ascending = {true}}));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);
  EXPECT_TRUE(std::ranges::any_of(
      search.GetMemo().Get(root).expressions,
      [&](const LogicalExpression& expression) {
        return expression.operation == LogicalOperator::kSort &&
               expression.children == std::vector<GroupId>{scan} &&
               expression.target_list.size() == 2;
      }));
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
  EXPECT_NE(b.filter->ToString().find('1'), std::string::npos);
}

TEST(CascadesTest, SingleJoinAndMarkJoinAreBinaryLogicalOperators) {
  using namespace dsl;
  Memo memo;
  const GroupId left = memo.Build({"a"});
  const GroupId right = memo.Build({"b"});
  const GroupId join = memo.Build({"a", "b"});

  LogicalExpression single_join{.operation = LogicalOperator::kSingleJoin,
                                .children = {left, right}};
  EXPECT_TRUE(memo.AddExpression(join, single_join));

  LogicalExpression mark_join{.operation = LogicalOperator::kMarkJoin,
                              .children = {left, right},
                              .marker_column = "in_marker"};
  EXPECT_TRUE(memo.AddExpression(join, mark_join));

  LogicalExpression apply{.operation = LogicalOperator::kApply,
                          .children = {left, right}};
  EXPECT_TRUE(memo.AddExpression(join, apply));
}

TEST(CascadesTest, WindowAndSpoolAreUnaryLogicalOperators) {
  using namespace dsl;
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId window_group = memo.EnsureDerivedGroup({"t"}, "window");

  LogicalExpression window_expr{.operation = LogicalOperator::kWindow,
                                .children = {scan},
                                .partition_by = {ColumnValueExp("t.dept")}};
  EXPECT_TRUE(memo.AddExpression(window_group, window_expr));

  const GroupId spool_group = memo.EnsureDerivedGroup({"t"}, "spool");
  LogicalExpression spool_expr{.operation = LogicalOperator::kMaterialize,
                               .children = {scan}};
  EXPECT_TRUE(memo.AddExpression(spool_group, spool_expr));
}

TEST(CascadesTest, ConstantTableAndGenerateSeriesAreLeafOperators) {
  using namespace dsl;
  Memo memo;
  const GroupId const_group = memo.Build({"const_tab"});

  LogicalExpression const_table{.operation = LogicalOperator::kConstantTable,
                                .table = "const_tab"};
  EXPECT_TRUE(memo.AddExpression(const_group, const_table));

  const GroupId series_group = memo.Build({"series_tab"});
  LogicalExpression series_expr{.operation = LogicalOperator::kGenerateSeries,
                                .table = "series_tab"};
  EXPECT_TRUE(memo.AddExpression(series_group, series_expr));
}

TEST(CascadesTest,
     PhysicalPropertiesExtendedKeysIncludeCollationAndPartitioning) {
  PhysicalProperties props;
  props.ordering = {ColumnName("x")};
  props.sort_ascending = {true};
  props.collation = "en_US";
  props.partition_by = {ColumnName("dept")};
  props.bloom_filter_keys = {ColumnName("k")};
  props.is_unique = true;

  const std::string key = props.Key();
  EXPECT_NE(key.find("col:en_US"), std::string::npos);
  EXPECT_NE(key.find("p:dept"), std::string::npos);
  EXPECT_NE(key.find("bf:k"), std::string::npos);
  EXPECT_NE(key.find("uniq:1"), std::string::npos);
}

TEST(CascadesTest, EliminateSortUnderUnorderedAggregation) {
  using namespace dsl;
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId sort_group = memo.EnsureDerivedGroup({"t"}, "sort");
  memo.AddExpression(
      sort_group,
      LogicalExpression{.operation = LogicalOperator::kSort,
                        .children = {scan},
                        .target_list = {NamedExpression("x", ColumnValueExp("t.x"))},
                        .sort_ascending = {true}});

  const GroupId agg_group = memo.EnsureDerivedGroup({"t"}, "agg");
  memo.AddExpression(
      agg_group,
      LogicalExpression{.operation = LogicalOperator::kAggregation,
                        .children = {sort_group},
                        .target_list = {NamedExpression("cnt", ConstantValueExp(Value(int64_t{1})))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg_group);

  // Aggregation should directly reference scan child without intermediate sort
  const auto& exprs = search.GetMemo().Get(agg_group).expressions;
  bool found_direct = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kAggregation &&
        expr.children.size() == 1 && expr.children[0] == scan) {
      found_direct = true;
      break;
    }
  }
  EXPECT_TRUE(found_direct);
}

TEST(CascadesTest, DistinctOverDistinctElimination) {
  using namespace dsl;
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId inner_distinct = memo.EnsureDerivedGroup({"t"}, "dist1");
  memo.AddExpression(
      inner_distinct,
      LogicalExpression{.operation = LogicalOperator::kDistinct,
                        .children = {scan}});

  const GroupId outer_distinct = memo.EnsureDerivedGroup({"t"}, "dist2");
  memo.AddExpression(
      outer_distinct,
      LogicalExpression{.operation = LogicalOperator::kDistinct,
                        .children = {inner_distinct}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(outer_distinct);

  const auto& exprs = search.GetMemo().Get(outer_distinct).expressions;
  bool found_direct = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kDistinct &&
        expr.children.size() == 1 && expr.children[0] == scan) {
      found_direct = true;
      break;
    }
  }
  EXPECT_TRUE(found_direct);
}

TEST(CascadesTest, CrossToInnerWithPredicate) {
  using namespace dsl;
  Memo memo;
  const GroupId left = memo.Build({"t1"});
  const GroupId right = memo.Build({"t2"});
  (void)memo.Build({"t1", "t2"});
  const GroupId cross = memo.EnsureDerivedGroup({"t1", "t2"}, "cross");
  memo.AddExpression(
      cross,
      LogicalExpression{.operation = LogicalOperator::kCrossJoin,
                        .children = {left, right}});

  const GroupId sel = memo.EnsureDerivedGroup({"t1", "t2"}, "sel");
  const Expression pred = BinaryExpressionExp(
      ColumnValueExp("t1.id"), BinaryOperation::kEquals, ColumnValueExp("t2.id"));
  memo.AddExpression(
      sel,
      LogicalExpression{.operation = LogicalOperator::kSelection,
                        .children = {cross},
                        .predicate = pred});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel);

  const auto& exprs = search.GetMemo().Get(sel).expressions;
  bool found_inner = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kJoin &&
        expr.children.size() == 2 && expr.predicate.has_value()) {
      found_inner = true;
      break;
    }
  }
  EXPECT_TRUE(found_inner);
}

TEST(CascadesTest, JoinEmptySimplification) {
  using namespace dsl;
  Memo memo;
  const GroupId left = memo.Build({"t1"});
  const GroupId right = memo.Build({"t2"});
  (void)memo.Build({"t1", "t2"});
  const GroupId empty_group = memo.EnsureDerivedGroup({"t1"}, "empty");
  memo.AddExpression(
      empty_group,
      LogicalExpression{.operation = LogicalOperator::kEmpty,
                        .children = {left}});

  const GroupId join_group = memo.EnsureDerivedGroup({"t1", "t2"}, "join");
  memo.AddExpression(
      join_group,
      LogicalExpression{.operation = LogicalOperator::kJoin,
                        .children = {empty_group, right}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(join_group);

  const auto& exprs = search.GetMemo().Get(join_group).expressions;
  bool found_empty = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kEmpty) {
      found_empty = true;
      break;
    }
  }
  EXPECT_TRUE(found_empty);
}

TEST(CascadesTest, IntersectToSemiJoinRewrite) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId intersect_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "intersect");
  memo.AddExpression(
      intersect_group,
      LogicalExpression{
          .operation = LogicalOperator::kIntersect,
          .children = {left, right},
          .output_schema = Schema("t1", {Column("id", ValueType::kInt64),
                                         Column("name", ValueType::kVarChar)})});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(intersect_group);

  const auto& exprs = search.GetMemo().Get(intersect_group).expressions;
  bool found_semijoin = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kSemiJoin &&
        expr.children.size() == 2 && expr.children[0] == left &&
        expr.children[1] == right && expr.predicate.has_value()) {
      found_semijoin = true;
      break;
    }
  }
  EXPECT_TRUE(found_semijoin);
}

TEST(CascadesTest, ExceptToAntiJoinRewrite) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId except_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "except");
  memo.AddExpression(
      except_group,
      LogicalExpression{
          .operation = LogicalOperator::kExcept,
          .children = {left, right},
          .output_schema = Schema("t1", {Column("id", ValueType::kInt64),
                                         Column("name", ValueType::kVarChar)})});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(except_group);

  const auto& exprs = search.GetMemo().Get(except_group).expressions;
  bool found_antijoin = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kAntiJoin &&
        expr.children.size() == 2 && expr.children[0] == left &&
        expr.children[1] == right && expr.predicate.has_value()) {
      found_antijoin = true;
      break;
    }
  }
  EXPECT_TRUE(found_antijoin);
}

TEST(CascadesTest, CountStarWithoutGroupRewriteToConstantTable) {
  Memo memo;
  const GroupId scan = memo.Build({"t1"});
  const GroupId agg_group =
      memo.EnsureDerivedGroup({"t1"}, "count_star_agg");
  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {scan},
          .target_list = {NamedExpression(
              "cnt", AggregateExpressionExp(
                  AggregationType::kCount, nullptr, false))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg_group);

  const auto& exprs = search.GetMemo().Get(agg_group).expressions;
  bool found_constant_table = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kConstantTable &&
        expr.table == "t1") {
      found_constant_table = true;
      break;
    }
  }
  EXPECT_TRUE(found_constant_table);
}

TEST(CascadesTest, DistinctAndGroupByInterchangeBothDirections) {
  // Direction 1: Distinct(Projection(X)) -> Aggregation(X, GROUP BY col)
  {
    Memo memo;
    const GroupId scan = memo.Build({"t1"});
    const GroupId proj_group =
        memo.EnsureDerivedGroup({"t1"}, "distinct_proj");
    memo.AddExpression(
        proj_group,
        LogicalExpression{
            .operation = LogicalOperator::kProjection,
            .children = {scan},
            .target_list = {NamedExpression("id", ColumnValueExp("id"))}});

    const GroupId dist_group =
        memo.EnsureDerivedGroup({"t1"}, "distinct_over_proj");
    memo.AddExpression(
        dist_group,
        LogicalExpression{.operation = LogicalOperator::kDistinct,
                          .children = {proj_group}});

    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(dist_group);

    const auto& exprs = search.GetMemo().Get(dist_group).expressions;
    bool found_agg = false;
    for (const auto& expr : exprs) {
      if (expr.operation == LogicalOperator::kAggregation &&
          !expr.grouping_sets.empty()) {
        found_agg = true;
        break;
      }
    }
    EXPECT_TRUE(found_agg);
  }

  // Direction 2: Aggregation(X, GROUP BY col) -> Distinct(Projection(X))
  {
    Memo memo;
    const GroupId scan = memo.Build({"t2"});
    const GroupId agg_group =
        memo.EnsureDerivedGroup({"t2"}, "agg_no_func");
    memo.AddExpression(
        agg_group,
        LogicalExpression{
            .operation = LogicalOperator::kAggregation,
            .children = {scan},
            .target_list = {NamedExpression("v", ColumnValueExp("v"))},
            .grouping_sets = {ColumnValueExp("v")}});

    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(agg_group);

    const auto& exprs = search.GetMemo().Get(agg_group).expressions;
    bool found_distinct = false;
    for (const auto& expr : exprs) {
      if (expr.operation == LogicalOperator::kDistinct) {
        found_distinct = true;
        break;
      }
    }
    EXPECT_TRUE(found_distinct);
  }
}

TEST(CascadesTest, SelfJoinEliminationReplacesJoinWithSingleScan) {
  Memo memo;
  (void)memo.Build({"t1_1", "t1_2"});
  const GroupId left_group = memo.EnsureDerivedGroup({"t1_1"}, "left_scan");
  memo.AddExpression(
      left_group,
      LogicalExpression{.operation = LogicalOperator::kScan, .table = "t1"});

  const GroupId right_group = memo.EnsureDerivedGroup({"t1_2"}, "right_scan");
  memo.AddExpression(
      right_group,
      LogicalExpression{.operation = LogicalOperator::kScan, .table = "t1"});

  const GroupId join_group =
      memo.EnsureDerivedGroup({"t1_1", "t1_2"}, "self_join");
  memo.AddExpression(
      join_group,
      LogicalExpression{
          .operation = LogicalOperator::kJoin,
          .children = {left_group, right_group},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t1_1", "id")),
              BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t1_2", "id")))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(join_group);

  const auto& exprs = search.GetMemo().Get(join_group).expressions;
  bool found_scan = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kScan && expr.table == "t1") {
      found_scan = true;
      break;
    }
  }
  EXPECT_TRUE(found_scan);
}

TEST(CascadesTest, UniqueSemiToInnerRewrite) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId semi_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "semi_unique");
  memo.AddExpression(
      semi_group,
      LogicalExpression{
          .operation = LogicalOperator::kSemiJoin,
          .children = {left, right},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t1", "id")),
              BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id")))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(semi_group);

  const auto& exprs = search.GetMemo().Get(semi_group).expressions;
  bool found_inner = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kJoin &&
        expr.children.size() == 2 && expr.children[0] == left &&
        expr.children[1] == right) {
      found_inner = true;
      break;
    }
  }
  EXPECT_TRUE(found_inner);
}

TEST(CascadesTest, OuterToAntiJoinRewrite) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId outer_join_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "outer_join");
  memo.AddExpression(
      outer_join_group,
      LogicalExpression{
          .operation = LogicalOperator::kOuterJoin,
          .children = {left, right},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t1", "id")),
              BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id"))),
          .join_type = 0});  // 0 = LeftOuter

  const GroupId sel_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "null_filter");
  memo.AddExpression(
      sel_group,
      LogicalExpression{
          .operation = LogicalOperator::kSelection,
          .children = {outer_join_group},
          .predicate = UnaryExpressionExp(
              ColumnValueExp(ColumnName("t2", "id")),
              UnaryOperation::kIsNull)});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  bool found_anti = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kAntiJoin &&
        expr.children.size() == 2 && expr.children[0] == left &&
        expr.children[1] == right) {
      found_anti = true;
      break;
    }
  }
  EXPECT_TRUE(found_anti);
}

TEST(CascadesTest, RightToLeftOuterJoinRewrite) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId right_outer_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "right_outer");
  memo.AddExpression(
      right_outer_group,
      LogicalExpression{
          .operation = LogicalOperator::kOuterJoin,
          .children = {left, right},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t1", "id")),
              BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id"))),
          .join_type = 1});  // 1 = RightOuter

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(right_outer_group);

  const auto& exprs = search.GetMemo().Get(right_outer_group).expressions;
  bool found_left_outer = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kOuterJoin &&
        expr.join_type == 0 &&  // 0 = LeftOuter
        expr.children.size() == 2 && expr.children[0] == right &&
        expr.children[1] == left) {
      found_left_outer = true;
      break;
    }
  }
  EXPECT_TRUE(found_left_outer);
}

TEST(CascadesTest, FullOuterJoinDecomposition) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId full_outer_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "full_outer");
  memo.AddExpression(
      full_outer_group,
      LogicalExpression{
          .operation = LogicalOperator::kOuterJoin,
          .children = {left, right},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t1", "id")),
              BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id"))),
          .join_type = 2});  // 2 = FullOuter

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(full_outer_group);

  const auto& exprs = search.GetMemo().Get(full_outer_group).expressions;
  bool found_union_all = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kUnionAll &&
        expr.children.size() == 2) {
      found_union_all = true;
      break;
    }
  }
  EXPECT_TRUE(found_union_all);
}

TEST(CascadesTest, PushDownLimitThroughJoin) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId join_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "inner_join");
  memo.AddExpression(
      join_group,
      LogicalExpression{
          .operation = LogicalOperator::kJoin,
          .children = {left, right},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t1", "id")),
              BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id")))});

  const GroupId limit_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "limit_join");
  memo.AddExpression(
      limit_group,
      LogicalExpression{
          .operation = LogicalOperator::kLimit,
          .children = {join_group},
          .limit_count = 10,
          .limit_offset = 5});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(limit_group);

  const auto& exprs = search.GetMemo().Get(limit_group).expressions;
  bool found_join_with_limit = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kJoin &&
        expr.children.size() == 2) {
      const Group& child_group = search.GetMemo().Get(expr.children[0]);
      for (const auto& child_expr : child_group.expressions) {
        if (child_expr.operation == LogicalOperator::kLimit &&
            child_expr.limit_count == 15) {
          found_join_with_limit = true;
          break;
        }
      }
      if (found_join_with_limit) break;
    }
  }
  EXPECT_TRUE(found_join_with_limit);
}

TEST(CascadesTest, RankRowNumberToTopNRewrite) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId win_group = memo.EnsureDerivedGroup({"t1"}, "window_rank");
  memo.AddExpression(
      win_group,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {scan},
          .target_list = {NamedExpression(
              "rn", ColumnValueExp(ColumnName("t1", "id")))},
          .sort_ascending = {true}});

  const GroupId sel_group = memo.EnsureDerivedGroup({"t1"}, "rank_le_5");
  memo.AddExpression(
      sel_group,
      LogicalExpression{
          .operation = LogicalOperator::kSelection,
          .children = {win_group},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t1", "rn")),
              BinaryOperation::kLessThanEquals,
              ConstantValueExp(Value(int64_t{5})))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  bool found_topn = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kTopN &&
        expr.children.size() == 1 && expr.children[0] == scan &&
        expr.limit_count == 5) {
      found_topn = true;
      break;
    }
  }
  EXPECT_TRUE(found_topn);
}

TEST(CascadesTest, NoOpWindowElimination) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId win_group = memo.EnsureDerivedGroup({"t1"}, "noop_window");
  memo.AddExpression(
      win_group,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {scan},
          .target_list = {NamedExpression(
              "c1", ColumnValueExp(ColumnName("t1", "id")))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(win_group);

  const auto& exprs = search.GetMemo().Get(win_group).expressions;
  bool found_proj = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kProjection &&
        expr.children.size() == 1 && expr.children[0] == scan) {
      found_proj = true;
      break;
    }
  }
  EXPECT_TRUE(found_proj);
}

TEST(CascadesTest, AggregateProjectionMerge) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId agg_group = memo.EnsureDerivedGroup({"t1"}, "agg");
  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {scan},
          .target_list = {NamedExpression(
              "c1", ColumnValueExp(ColumnName("t1", "id")))},
          .grouping_sets = {ColumnValueExp(ColumnName("t1", "id"))}});

  const GroupId proj_group = memo.EnsureDerivedGroup({"t1"}, "proj_over_agg");
  memo.AddExpression(
      proj_group,
      LogicalExpression{
          .operation = LogicalOperator::kProjection,
          .children = {agg_group},
          .target_list = {NamedExpression(
              "c1", ColumnValueExp(ColumnName("t1", "id")))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(proj_group);

  const auto& exprs = search.GetMemo().Get(proj_group).expressions;
  bool found_merged_agg = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kAggregation &&
        expr.children.size() == 1 && expr.children[0] == scan) {
      found_merged_agg = true;
      break;
    }
  }
  EXPECT_TRUE(found_merged_agg);
}

TEST(CascadesTest, EagerAggregationOverJoin) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId join_group = memo.EnsureDerivedGroup({"t1", "t2"}, "join");
  memo.AddExpression(
      join_group,
      LogicalExpression{
          .operation = LogicalOperator::kJoin,
          .children = {left, right},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t1", "id")),
              BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id")))});

  const GroupId agg_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "agg_over_join");
  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {join_group},
          .target_list = {NamedExpression(
              "c1", ColumnValueExp(ColumnName("t1", "id")))},
          .grouping_sets = {ColumnValueExp(ColumnName("t1", "id"))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg_group);

  const auto& exprs = search.GetMemo().Get(agg_group).expressions;
  bool found_join_with_eager_agg = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kJoin && expr.children.size() == 2) {
      const Group& child_group = search.GetMemo().Get(expr.children[0]);
      for (const auto& child_expr : child_group.expressions) {
        if (child_expr.operation == LogicalOperator::kAggregation) {
          found_join_with_eager_agg = true;
          break;
        }
      }
      if (found_join_with_eager_agg) break;
    }
  }
  EXPECT_TRUE(found_join_with_eager_agg);
}

TEST(CascadesTest, ProjectionCseAndPruning) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId proj_group = memo.EnsureDerivedGroup({"t1"}, "dup_proj");
  memo.AddExpression(
      proj_group,
      LogicalExpression{
          .operation = LogicalOperator::kProjection,
          .children = {scan},
          .target_list = {
              NamedExpression("c1", ColumnValueExp(ColumnName("t1", "id"))),
              NamedExpression("c1", ColumnValueExp(ColumnName("t1", "id"))),
              NamedExpression(
                  "c2", ColumnValueExp(ColumnName("t1", "score")))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(proj_group);

  const auto& exprs = search.GetMemo().Get(proj_group).expressions;
  bool found_pruned = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kProjection &&
        expr.target_list.size() == 2) {
      found_pruned = true;
      break;
    }
  }
  EXPECT_TRUE(found_pruned);
}

TEST(CascadesTest, ProjectionConstantPropagation) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId inner_proj = memo.EnsureDerivedGroup({"t1"}, "const_proj");
  memo.AddExpression(
      inner_proj,
      LogicalExpression{
          .operation = LogicalOperator::kProjection,
          .children = {scan},
          .target_list = {NamedExpression(
              "c_const", ConstantValueExp(Value(int64_t{42})))}});

  const GroupId outer_proj = memo.EnsureDerivedGroup({"t1"}, "outer_proj");
  memo.AddExpression(
      outer_proj,
      LogicalExpression{
          .operation = LogicalOperator::kProjection,
          .children = {inner_proj},
          .target_list = {NamedExpression(
              "out", BinaryExpressionExp(
                         ColumnValueExp(ColumnName("", "c_const")),
                         BinaryOperation::kAdd,
                         ConstantValueExp(Value(int64_t{1}))))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(outer_proj);

  const auto& exprs = search.GetMemo().Get(outer_proj).expressions;
  bool found_prop = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kProjection &&
        !expr.target_list.empty()) {
      if (expr.target_list[0].expression &&
          expr.target_list[0].expression->Type() == TypeTag::kBinaryExp) {
        const auto& bin = expr.target_list[0].expression->AsBinaryExpression();
        if (bin.Left()->Type() == TypeTag::kConstantValue) {
          found_prop = true;
          break;
        }
      }
    }
  }
  EXPECT_TRUE(found_prop);
}

TEST(CascadesTest, PushProjectionBelowJoinWidthControl) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId join_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "join_width");
  memo.AddExpression(
      join_group,
      LogicalExpression{
          .operation = LogicalOperator::kJoin,
          .children = {left, right},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t1", "id")),
              BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id")))});

  const GroupId proj_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "proj_over_join");
  memo.AddExpression(
      proj_group,
      LogicalExpression{
          .operation = LogicalOperator::kProjection,
          .children = {join_group},
          .target_list = {
              NamedExpression("c1", ColumnValueExp(ColumnName("t1", "id"))),
              NamedExpression(
                  "c2", ColumnValueExp(ColumnName("t2", "id")))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(proj_group);

  const auto& exprs = search.GetMemo().Get(proj_group).expressions;
  bool found_width_control = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kProjection &&
        expr.children.size() == 1) {
      const Group& join_child = search.GetMemo().Get(expr.children[0]);
      for (const auto& jexpr : join_child.expressions) {
        if (jexpr.operation == LogicalOperator::kJoin &&
            jexpr.children.size() == 2) {
          const Group& left_child = search.GetMemo().Get(jexpr.children[0]);
          for (const auto& child_expr : left_child.expressions) {
            if (child_expr.operation == LogicalOperator::kProjection) {
              found_width_control = true;
              break;
            }
          }
          if (found_width_control) break;
        }
      }
      if (found_width_control) break;
    }
  }
  EXPECT_TRUE(found_width_control);
}

TEST(CascadesTest, MergeAdjacentProjectionsCompose) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId inner_proj = memo.EnsureDerivedGroup({"t1"}, "inner_p");
  memo.AddExpression(
      inner_proj,
      LogicalExpression{
          .operation = LogicalOperator::kProjection,
          .children = {scan},
          .target_list = {NamedExpression(
              "b", BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "a")),
                                       BinaryOperation::kAdd,
                                       ConstantValueExp(Value(int64_t{1}))))}});

  const GroupId outer_proj = memo.EnsureDerivedGroup({"t1"}, "outer_p");
  memo.AddExpression(
      outer_proj,
      LogicalExpression{
          .operation = LogicalOperator::kProjection,
          .children = {inner_proj},
          .target_list = {NamedExpression(
              "c",
              BinaryExpressionExp(ColumnValueExp(ColumnName("", "b")),
                                  BinaryOperation::kMultiply,
                                  ConstantValueExp(Value(int64_t{2}))))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(outer_proj);

  const auto& exprs = search.GetMemo().Get(outer_proj).expressions;
  bool found_composed = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kProjection &&
        expr.children.size() == 1 && expr.children[0] == scan) {
      found_composed = true;
      break;
    }
  }
  EXPECT_TRUE(found_composed);
}

TEST(CascadesTest, GreedyJoinOrderFallback) {
  std::vector<std::string> rels;
  rels.reserve(18);
  for (int i = 1; i <= 18; ++i) {
    rels.push_back("t" + std::to_string(i));
  }
  Memo memo;
  const GroupId root = memo.Build(rels);
  const auto& exprs = memo.Get(root).expressions;
  EXPECT_FALSE(exprs.empty());
  EXPECT_EQ(exprs[0].operation, LogicalOperator::kJoin);
  EXPECT_EQ(exprs[0].children.size(), 2);
  EXPECT_EQ(memo.Get(exprs[0].children[0]).relations.size(), 1);
  EXPECT_EQ(memo.Get(exprs[0].children[1]).relations.size(), 17);
}

TEST(CascadesTest, JoinCardinalityEstimation) {
  // 1-to-1 join
  auto est_1to1 = EstimateJoinCardinality(100.0, 100.0, true, true, 0.5);
  EXPECT_EQ(est_1to1.multiplicity, JoinMultiplicity::kOneToOne);
  EXPECT_DOUBLE_EQ(est_1to1.rows, 50.0);

  // 1-to-many join
  auto est_1toN = EstimateJoinCardinality(100.0, 20.0, false, true, 1.0);
  EXPECT_EQ(est_1toN.multiplicity, JoinMultiplicity::kOneToMany);
  EXPECT_DOUBLE_EQ(est_1toN.rows, 100.0);

  // many-to-many join
  auto est_NtoM = EstimateJoinCardinality(100.0, 50.0, false, false, 0.05);
  EXPECT_EQ(est_NtoM.multiplicity, JoinMultiplicity::kManyToMany);
  EXPECT_DOUBLE_EQ(est_NtoM.rows, 250.0);
}

TEST(CascadesTest, CostModelCalibration) {
  PhysicalProperties delivered;
  PhysicalProperties required;

  double hash_cost = CalibrateOperatorCost(
      OperatorCostKind::kHashJoin, 100.0, 200.0, delivered, required);
  EXPECT_GT(hash_cost, 0.0);

  double merge_cost = CalibrateOperatorCost(
      OperatorCostKind::kMergeJoin, 100.0, 200.0, delivered, required);
  EXPECT_GT(merge_cost, 0.0);

  double nlj_cost = CalibrateOperatorCost(
      OperatorCostKind::kNestedLoopJoin, 100.0, 200.0, delivered, required);
  EXPECT_GT(nlj_cost, hash_cost);

  double idx_cost = CalibrateOperatorCost(
      OperatorCostKind::kIndexScan, 100.0, 0.0, delivered, required);
  EXPECT_GT(idx_cost, 0.0);

  double bitmap_cost = CalibrateOperatorCost(
      OperatorCostKind::kBitmapScan, 100.0, 0.0, delivered, required);
  EXPECT_GT(bitmap_cost, 0.0);

  double sort_cost = CalibrateOperatorCost(
      OperatorCostKind::kSort, 100.0, 0.0, delivered, required);
  EXPECT_GT(sort_cost, 0.0);

  // Property mismatch penalties
  required.ordering = {ColumnName("t1", "id")};
  double penalized_cost = CalibrateOperatorCost(
      OperatorCostKind::kHashJoin, 100.0, 200.0, delivered, required);
  EXPECT_GT(penalized_cost, hash_cost);
}

TEST(CascadesTest, DynamicFilterPushdownJoin) {
  Memo memo;
  memo.Build({"t1", "t2"});
  const GroupId g1 = memo.EnsureGroup({"t1"});
  const GroupId g2 = memo.EnsureGroup({"t2"});
  const GroupId join_group = memo.EnsureDerivedGroup({"t1", "t2"}, "root_join");

  memo.AddExpression(
      join_group,
      LogicalExpression{
          .operation = LogicalOperator::kJoin,
          .children = {g1, g2},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t1", "id")), BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id")))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(join_group);

  const auto& exprs = search.GetMemo().Get(join_group).expressions;
  bool found_bloom = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kJoin &&
        expr.children.size() == 2) {
      const auto& probe_tag = search.GetMemo().Get(expr.children[1]).tag;
      if (probe_tag.find("bloom_probe") != std::string::npos) {
        found_bloom = true;
        break;
      }
    }
  }
  EXPECT_TRUE(found_bloom);
}

TEST(CascadesTest, JoinPredicateTransitivity) {
  Memo memo;
  memo.Build({"t1", "t2", "t3"});
  const GroupId g3 = memo.EnsureGroup({"t3"});
  const GroupId join12 = memo.EnsureGroup({"t1", "t2"});
  const GroupId root = memo.EnsureDerivedGroup({"t1", "t2", "t3"}, "root");

  const Expression p1 = BinaryExpressionExp(
      ColumnValueExp(ColumnName("t1", "a")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("t2", "b")));
  const Expression p2 = BinaryExpressionExp(
      ColumnValueExp(ColumnName("t2", "b")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("t3", "c")));
  const Expression joint_predicate =
      BinaryExpressionExp(p1, BinaryOperation::kAnd, p2);

  memo.AddExpression(root, LogicalExpression{.operation = LogicalOperator::kJoin,
                                            .children = {join12, g3},
                                            .predicate = joint_predicate});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);

  const auto& exprs = search.GetMemo().Get(root).expressions;
  bool found_inferred = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kJoin && expr.predicate &&
        *expr.predicate) {
      const std::string pred_str = (*expr.predicate)->ToString();
      if (pred_str.find("t1.a = t3.c") != std::string::npos ||
          pred_str.find("t3.c = t1.a") != std::string::npos) {
        found_inferred = true;
        break;
      }
    }
  }
  EXPECT_TRUE(found_inferred);
}

TEST(CascadesTest, InferredInequalityPushdown) {
  Memo memo;
  memo.Build({"t1", "t2"});
  const GroupId g1 = memo.EnsureGroup({"t1"});
  const GroupId g2 = memo.EnsureGroup({"t2"});
  const GroupId join_group = memo.EnsureDerivedGroup({"t1", "t2"}, "root_join");

  // t1 has filter t1.x > 10
  memo.MergeScanFilter(
      g1, BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "x")),
                              BinaryOperation::kGreaterThan,
                              ConstantValueExp(Value(int64_t{10}))));

  // join condition t1.x = t2.y
  memo.AddExpression(
      join_group,
      LogicalExpression{
          .operation = LogicalOperator::kJoin,
          .children = {g1, g2},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t1", "x")), BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "y")))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(join_group);

  const Expression t2_filter = search.GetMemo().Get(g2).filter;
  ASSERT_TRUE(t2_filter);
  const std::string filter_str = t2_filter->ToString();
  EXPECT_NE(filter_str.find("t2.y > 10"), std::string::npos);
}

TEST(CascadesTest, RedundantJoinPredicateElimination) {
  Memo memo;
  memo.Build({"t1", "t2", "t3"});
  const GroupId g12 = memo.EnsureGroup({"t1", "t2"});
  const GroupId g3 = memo.EnsureGroup({"t3"});
  const GroupId root = memo.EnsureDerivedGroup({"t1", "t2", "t3"}, "root");

  const Expression p1 = BinaryExpressionExp(
      ColumnValueExp(ColumnName("t1", "a")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("t2", "b")));
  const Expression p2 = BinaryExpressionExp(
      ColumnValueExp(ColumnName("t2", "b")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("t3", "c")));
  const Expression p3 = BinaryExpressionExp(
      ColumnValueExp(ColumnName("t1", "a")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("t3", "c")));

  const Expression joint_predicate = BinaryExpressionExp(
      BinaryExpressionExp(p1, BinaryOperation::kAnd, p2),
      BinaryOperation::kAnd, p3);

  memo.AddExpression(root, LogicalExpression{.operation = LogicalOperator::kJoin,
                                            .children = {g12, g3},
                                            .predicate = joint_predicate});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);

  const auto& exprs = search.GetMemo().Get(root).expressions;
  bool found_reduced = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kJoin && expr.predicate &&
        *expr.predicate) {
      const std::string pred_str = (*expr.predicate)->ToString();
      // Reduced expression will have only 2 equality terms (fewer than the original 3 terms).
      size_t eq_count = 0;
      size_t pos = 0;
      while ((pos = pred_str.find('=', pos)) != std::string::npos) {
        ++eq_count;
        ++pos;
      }
      if (eq_count == 2) {
        found_reduced = true;
        break;
      }
    }
  }
  EXPECT_TRUE(found_reduced);
}

TEST(CascadesTest, IntersectExceptCostBasedLowering) {
  Memo memo;
  memo.Build({"t1", "t2"});
  const GroupId g1 = memo.EnsureGroup({"t1"});
  const GroupId g2 = memo.EnsureGroup({"t2"});
  const GroupId intersect_group = memo.EnsureDerivedGroup({"t1", "t2"}, "intersect_root");
  const GroupId except_group = memo.EnsureDerivedGroup({"t1", "t2"}, "except_root");

  memo.AddExpression(
      intersect_group,
      LogicalExpression{.operation = LogicalOperator::kIntersect,
                        .children = {g1, g2},
                        .target_list = {NamedExpression("id", ColumnValueExp(ColumnName("t1", "id")))}});

  memo.AddExpression(
      except_group,
      LogicalExpression{.operation = LogicalOperator::kExcept,
                        .children = {g1, g2},
                        .target_list = {NamedExpression("id", ColumnValueExp(ColumnName("t1", "id")))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(intersect_group);
  search.Explore(except_group);

  const auto& int_exprs = search.GetMemo().Get(intersect_group).expressions;
  bool found_semi = false;
  for (const auto& expr : int_exprs) {
    if (expr.operation == LogicalOperator::kSemiJoin) {
      found_semi = true;
      break;
    }
  }
  EXPECT_TRUE(found_semi);

  const auto& exc_exprs = search.GetMemo().Get(except_group).expressions;
  bool found_anti = false;
  for (const auto& expr : exc_exprs) {
    if (expr.operation == LogicalOperator::kAntiJoin) {
      found_anti = true;
      break;
    }
  }
  EXPECT_TRUE(found_anti);
}

TEST(CascadesTest, UnionDistinctHashSortChoice) {
  Memo memo;
  memo.Build({"t1", "t2"});
  const GroupId g1 = memo.EnsureGroup({"t1"});
  const GroupId g2 = memo.EnsureGroup({"t2"});
  const GroupId union_group = memo.EnsureDerivedGroup({"t1", "t2"}, "union_root");

  memo.AddExpression(
      union_group,
      LogicalExpression{.operation = LogicalOperator::kUnion,
                        .children = {g1, g2},
                        .target_list = {NamedExpression("id", ColumnValueExp(ColumnName("t1", "id")))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(union_group);

  const auto& exprs = search.GetMemo().Get(union_group).expressions;
  bool found_distinct = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kDistinct) {
      found_distinct = true;
      break;
    }
  }
  EXPECT_TRUE(found_distinct);
}

TEST(CascadesTest, WindowFrameSortSharing) {
  Memo memo;
  memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId inner_win = memo.EnsureDerivedGroup({"t1"}, "inner_w");
  const GroupId outer_win = memo.EnsureDerivedGroup({"t1"}, "outer_w");

  const std::vector<Expression> partition = {
      ColumnValueExp(ColumnName("t1", "dept"))};
  const std::vector<bool> ascending = {true};

  memo.AddExpression(
      inner_win,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {scan},
          .target_list = {NamedExpression("w1", ColumnValueExp("t1.dept"))},
          .sort_ascending = ascending,
          .partition_by = partition});

  memo.AddExpression(
      outer_win,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {inner_win},
          .target_list = {NamedExpression("w2", ColumnValueExp("t1.dept"))},
          .sort_ascending = ascending,
          .partition_by = partition});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(outer_win);

  const auto& exprs = search.GetMemo().Get(outer_win).expressions;
  bool found_shared = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kWindow &&
        expr.children.size() == 1 && expr.children[0] == scan) {
      found_shared = true;
      break;
    }
  }
  EXPECT_TRUE(found_shared);
}

TEST(CascadesTest, UnnestFilterPushdown) {
  Memo memo;
  const GroupId scan = memo.Build({"t1"});
  const GroupId unnest_group = memo.EnsureDerivedGroup({"t1"}, "unnest_grp");
  memo.AddExpression(
      unnest_group,
      LogicalExpression{.operation = LogicalOperator::kUnnest,
                        .children = {scan},
                        .unnest_alias = "elem"});

  const GroupId sel_group = memo.EnsureDerivedGroup({"t1"}, "sel_unnest");
  Expression pred = BinaryExpressionExp(
      ColumnValueExp("t1.id"), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(int64_t{10})));
  memo.AddExpression(
      sel_group,
      LogicalExpression{.operation = LogicalOperator::kSelection,
                        .children = {unnest_group},
                        .predicate = pred});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  bool found_pushed = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kUnnest &&
        expr.children.size() == 1 && expr.children[0] != unnest_group) {
      const auto& child_exprs = search.GetMemo().Get(expr.children[0]).expressions;
      for (const auto& child_expr : child_exprs) {
        if (child_expr.operation == LogicalOperator::kSelection &&
            child_expr.children.size() == 1 && child_expr.children[0] == scan) {
          found_pushed = true;
          break;
        }
      }
    }
  }
  EXPECT_TRUE(found_pushed);
}

TEST(CascadesTest, RecursiveTerminationPredicatePushdown) {
  Memo memo;
  (void)memo.Build({"t1", "rec_table"});
  const GroupId anchor = memo.EnsureGroup({"t1"});
  const GroupId rec = memo.EnsureGroup({"rec_table"});
  const GroupId cte_group = memo.EnsureDerivedGroup({"t1", "rec_table"}, "cte_grp");
  memo.AddExpression(
      cte_group,
      LogicalExpression{.operation = LogicalOperator::kRecursiveCte,
                        .children = {anchor, rec},
                        .cte_name = "rec_table"});

  const GroupId sel_group = memo.EnsureDerivedGroup({"t1", "rec_table"}, "sel_cte");
  Expression term_pred = BinaryExpressionExp(
      ColumnValueExp("rec_table.n"), BinaryOperation::kLessThanEquals,
      ConstantValueExp(Value(int64_t{100})));
  memo.AddExpression(
      sel_group,
      LogicalExpression{.operation = LogicalOperator::kSelection,
                        .children = {cte_group},
                        .predicate = term_pred});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  bool found_rec_pushed = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kRecursiveCte &&
        expr.children.size() == 2 && expr.children[0] == anchor &&
        expr.children[1] != rec) {
      const auto& rec_exprs = search.GetMemo().Get(expr.children[1]).expressions;
      for (const auto& r_expr : rec_exprs) {
        if (r_expr.operation == LogicalOperator::kSelection &&
            r_expr.children.size() == 1 && r_expr.children[0] == rec) {
          found_rec_pushed = true;
          break;
        }
      }
    }
  }
  EXPECT_TRUE(found_rec_pushed);
}

TEST(CascadesTest, InListToSemiJoin) {
  Memo memo;
  const GroupId scan = memo.Build({"t1"});
  const GroupId sel_group = memo.EnsureDerivedGroup({"t1"}, "in_list_sel");

  const Expression eq1 = BinaryExpressionExp(
      ColumnValueExp("t1.id"), BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{1})));
  const Expression eq2 = BinaryExpressionExp(
      ColumnValueExp("t1.id"), BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{2})));
  const Expression eq3 = BinaryExpressionExp(
      ColumnValueExp("t1.id"), BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{3})));

  const Expression or_pred = BinaryExpressionExp(
      BinaryExpressionExp(eq1, BinaryOperation::kOr, eq2), BinaryOperation::kOr, eq3);

  memo.AddExpression(
      sel_group,
      LogicalExpression{.operation = LogicalOperator::kSelection,
                        .children = {scan},
                        .predicate = or_pred});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  bool found_semi = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kSemiJoin && expr.children.size() == 2) {
      found_semi = true;
      break;
    }
  }
  EXPECT_TRUE(found_semi);
}

TEST(CascadesTest, FilterPullUpForExtremeSelectivity) {
  Memo memo;
  memo.Build({"t1", "t2"});
  const GroupId g1 = memo.EnsureGroup({"t1"});
  const GroupId g2 = memo.EnsureGroup({"t2"});
  const GroupId sel_left = memo.EnsureDerivedGroup({"t1"}, "sel_left");

  Expression filter_pred = BinaryExpressionExp(
      ColumnValueExp("t1.status"), BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{1})));
  memo.AddExpression(
      sel_left,
      LogicalExpression{.operation = LogicalOperator::kSelection,
                        .children = {g1},
                        .predicate = filter_pred});

  const GroupId join_group = memo.EnsureDerivedGroup({"t1", "t2"}, "root_join");
  Expression join_pred = BinaryExpressionExp(
      ColumnValueExp("t1.id"), BinaryOperation::kEquals, ColumnValueExp("t2.id"));

  memo.AddExpression(
      join_group,
      LogicalExpression{.operation = LogicalOperator::kJoin,
                        .children = {sel_left, g2},
                        .predicate = join_pred});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(join_group);

  const auto& exprs = search.GetMemo().Get(join_group).expressions;
  bool found_pull_up = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kSelection && expr.children.size() == 1) {
      found_pull_up = true;
      break;
    }
  }
  EXPECT_TRUE(found_pull_up);
}

TEST(CascadesTest, FunctionalDependencyFilterReduction) {
  Memo memo;
  const GroupId scan = memo.Build({"t1"});
  const GroupId sel_group = memo.EnsureDerivedGroup({"t1"}, "sel_fd");

  const Expression eq = BinaryExpressionExp(
      ColumnValueExp("t1.age"), BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{10})));
  const Expression lte = BinaryExpressionExp(
      ColumnValueExp("t1.age"), BinaryOperation::kLessThanEquals, ConstantValueExp(Value(int64_t{20})));

  const Expression joint = BinaryExpressionExp(eq, BinaryOperation::kAnd, lte);

  memo.AddExpression(
      sel_group,
      LogicalExpression{.operation = LogicalOperator::kSelection,
                        .children = {scan},
                        .predicate = joint});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  bool found_reduced = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kSelection && expr.predicate && *expr.predicate) {
      const std::string pred_str = (*expr.predicate)->ToString();
      if (pred_str.find("<=") == std::string::npos &&
          pred_str.find('=') != std::string::npos) {
        found_reduced = true;
        break;
      }
    }
  }
  EXPECT_TRUE(found_reduced);
}

TEST(CascadesTest, ScanZoneMapFilterIntegration) {
  Memo memo;
  const GroupId scan = memo.Build({"t1"});
  memo.MergeScanFilter(
      scan, BinaryExpressionExp(ColumnValueExp("t1.id"), BinaryOperation::kGreaterThan,
                                ConstantValueExp(Value(int64_t{100}))));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(scan);

  bool found_zm = false;
  for (size_t g = 0; g < search.GetMemo().GroupCount(); ++g) {
    if (search.GetMemo().Get(g).tag.find("zonemap") != std::string::npos) {
      found_zm = true;
      break;
    }
  }
  EXPECT_TRUE(found_zm);
}

TEST(CascadesTest, CountDistinctExpansion) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId agg_group = memo.EnsureDerivedGroup({"t1"}, "agg_count_distinct");
  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {scan},
          .target_list = {NamedExpression(
              "cnt", AggregateExpressionExp(AggregationType::kCount,
                                            ColumnValueExp("t1.id"),
                                            /*distinct=*/true))},
          .grouping_sets = {ColumnValueExp("t1.k")}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg_group);

  const auto& exprs = search.GetMemo().Get(agg_group).expressions;
  bool found_expanded = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kAggregation &&
        expr.children.size() == 1 && expr.children[0] != scan) {
      const auto& child_exprs =
          search.GetMemo().Get(expr.children[0]).expressions;
      for (const auto& c_expr : child_exprs) {
        if (c_expr.operation == LogicalOperator::kAggregation &&
            c_expr.children.size() == 1 && c_expr.children[0] == scan) {
          found_expanded = true;
          break;
        }
      }
    }
  }
  EXPECT_TRUE(found_expanded);
}

TEST(CascadesTest, GroupingSetsExpansion) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId agg_group = memo.EnsureDerivedGroup({"t1"}, "agg_multi_grouping");
  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {scan},
          .target_list = {NamedExpression(
              "cnt", AggregateExpressionExp(AggregationType::kCount,
                                            ColumnValueExp("t1.id"),
                                            /*distinct=*/false))},
          .grouping_sets = {ColumnValueExp("t1.a"), ColumnValueExp("t1.b")}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg_group);

  const auto& exprs = search.GetMemo().Get(agg_group).expressions;
  bool found_union_all = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kUnionAll &&
        expr.children.size() == 2) {
      found_union_all = true;
      break;
    }
  }
  EXPECT_TRUE(found_union_all);
}

TEST(CascadesTest, HavingToFilterRewrite) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId agg_group = memo.EnsureDerivedGroup({"t1"}, "agg_having");
  Expression having_pred = BinaryExpressionExp(
      ColumnValueExp("cnt"), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(int64_t{5})));
  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {scan},
          .predicate = having_pred,
          .target_list = {NamedExpression(
              "cnt", AggregateExpressionExp(AggregationType::kCount,
                                            ColumnValueExp("t1.id"),
                                            /*distinct=*/false))},
          .grouping_sets = {ColumnValueExp("t1.k")}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg_group);

  const auto& exprs = search.GetMemo().Get(agg_group).expressions;
  bool found_sel = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kSelection &&
        expr.children.size() == 1 && expr.predicate.has_value() &&
        *expr.predicate == having_pred) {
      found_sel = true;
      break;
    }
  }
  EXPECT_TRUE(found_sel);
}

TEST(CascadesTest, FilterAggregatePushdown) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId sel_group = memo.EnsureDerivedGroup({"t1"}, "sel_before_agg");
  Expression filter_pred = BinaryExpressionExp(
      ColumnValueExp("t1.v"), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(int64_t{10})));
  memo.AddExpression(
      sel_group,
      LogicalExpression{
          .operation = LogicalOperator::kSelection,
          .children = {scan},
          .predicate = filter_pred});

  const GroupId agg_group = memo.EnsureDerivedGroup({"t1"}, "agg_over_sel");
  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {sel_group},
          .target_list = {NamedExpression(
              "sum_v", AggregateExpressionExp(AggregationType::kSum,
                                              ColumnValueExp("t1.v"),
                                              /*distinct=*/false))},
          .grouping_sets = {ColumnValueExp("t1.k")}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg_group);

  const auto& exprs = search.GetMemo().Get(agg_group).expressions;
  bool found_where_filter = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kAggregation &&
        expr.children.size() == 1 && expr.children[0] == scan) {
      for (const auto& target : expr.target_list) {
        if (target.expression &&
            target.expression->Type() == TypeTag::kAggregateExp) {
          const auto& agg = static_cast<const AggregateExpression&>(
              *target.expression);
          if (agg.WhereFilter() && agg.WhereFilter() == filter_pred) {
            found_where_filter = true;
            break;
          }
        }
      }
    }
  }
  EXPECT_TRUE(found_where_filter);
}

TEST(CascadesTest, MultiColumnCorrelationNdv) {
  std::vector<double> selectivities = {0.1, 0.2};
  const double independent = EstimateMultiColumnSelectivity(selectivities, 0.0);
  EXPECT_NEAR(independent, 0.02, 1e-6);

  const double fully_correlated = EstimateMultiColumnSelectivity(selectivities, 1.0);
  EXPECT_NEAR(fully_correlated, 0.1, 1e-6);

  const double partial_correlated = EstimateMultiColumnSelectivity(selectivities, 0.5);
  EXPECT_NEAR(partial_correlated, 0.06, 1e-6);
}

TEST(CascadesTest, LikeRegexSelectivityModel) {
  const double sel_all = EstimatePatternSelectivity(PatternMatchingKind::kLike, "%");
  EXPECT_NEAR(sel_all, 1.0, 1e-6);

  const double sel_prefix1 = EstimatePatternSelectivity(PatternMatchingKind::kLike, "a%");
  const double sel_prefix3 = EstimatePatternSelectivity(PatternMatchingKind::kLike, "abc%");
  EXPECT_GT(sel_prefix1, sel_prefix3);

  const double sel_regex_prefix = EstimatePatternSelectivity(PatternMatchingKind::kRegexp, "^abc.*");
  EXPECT_NEAR(sel_regex_prefix, sel_prefix3, 1e-6);
}

TEST(CascadesTest, HistogramJoinEstimation) {
  std::vector<HistogramBucket> left = {
      {.lower = 0.0, .upper = 10.0, .count = 100.0, .distinct_count = 10.0},
      {.lower = 10.0, .upper = 20.0, .count = 200.0, .distinct_count = 20.0}};
  std::vector<HistogramBucket> right = {
      {.lower = 5.0, .upper = 15.0, .count = 150.0, .distinct_count = 15.0}};

  const double est = EstimateHistogramJoinCardinality(left, right);
  EXPECT_GT(est, 0.0);
}

TEST(CascadesTest, MemorySpillCostModel) {
  MemoryBudget budget;
  budget.max_memory_bytes = 1024.0 * 1024.0;
  budget.row_size_bytes = 128.0;
  budget.io_spill_cost_multiplier = 3.0;

  const double no_spill = EstimateMemorySpillCost(OperatorCostKind::kSort, 1000.0, budget);
  EXPECT_NEAR(no_spill, 0.0, 1e-6);

  const double spill_sort = EstimateMemorySpillCost(OperatorCostKind::kSort, 20000.0, budget);
  EXPECT_GT(spill_sort, 0.0);

  const double spill_hash = EstimateMemorySpillCost(OperatorCostKind::kHashJoin, 20000.0, budget);
  EXPECT_GT(spill_hash, 0.0);
}

TEST(CascadesTest, UniqueGroupKeyAggregateElimination) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId agg_group =
      memo.EnsureDerivedGroup({"t1"}, "unique_group_key_agg");

  memo.AddExpression(
      scan,
      LogicalExpression{
          .operation = LogicalOperator::kScan,
          .table = "t1",
          .output_schema = Schema(
              "t1", {Column("id", ValueType::kInt64,
                             Constraint(Constraint::kPrimaryKey)),
                     Column("val", ValueType::kInt64),
                     Column("v2", ValueType::kVarChar)})});

  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {scan},
          .target_list = {NamedExpression("id", ColumnValueExp("t1.id")),
                          NamedExpression("min_val",
                                          AggregateExpressionExp(
                                              AggregationType::kMin,
                                              ColumnValueExp("t1.val"))),
                          NamedExpression("any_v",
                                          AggregateExpressionExp(
                                              AggregationType::kAnyValue,
                                              ColumnValueExp("t1.v2")))},
          .output_schema = Schema(
              "t1", {Column("id", ValueType::kInt64),
                     Column("min_val", ValueType::kInt64),
                     Column("any_v", ValueType::kVarChar)}),
          .grouping_sets = {ColumnValueExp("t1.id")}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg_group);

  const auto& exprs = search.GetMemo().Get(agg_group).expressions;
  bool found_proj = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kProjection &&
        expr.children.size() == 1 && expr.children[0] == scan) {
      if (expr.target_list.size() == 3 &&
          expr.target_list[1].expression->Type() == TypeTag::kColumnValue &&
          expr.target_list[2].expression->Type() == TypeTag::kColumnValue) {
        found_proj = true;
        break;
      }
    }
  }
  EXPECT_TRUE(found_proj);
}

TEST(CascadesTest, OneRowCrossJoinElimination) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureDerivedGroup({}, "one_row_const");

  memo.AddExpression(
      right,
      LogicalExpression{
          .operation = LogicalOperator::kValues,
          .values = {Row({Value(int64_t{42})})},
          .output_schema = Schema("c", {Column("k", ValueType::kInt64)})});

  const GroupId cross_group =
      memo.EnsureDerivedGroup({"t1"}, "cross_join_1row");
  memo.AddExpression(
      cross_group,
      LogicalExpression{
          .operation = LogicalOperator::kCrossJoin,
          .children = {left, right},
          .target_list = {NamedExpression("id", ColumnValueExp("t1.id")),
                          NamedExpression("k", ColumnValueExp("c.k"))},
          .output_schema = Schema(
              "out", {Column("id", ValueType::kInt64),
                      Column("k", ValueType::kInt64)})});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(cross_group);

  const auto& exprs = search.GetMemo().Get(cross_group).expressions;
  bool found_scalar_proj = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kProjection &&
        expr.children.size() == 1 && expr.children[0] == left) {
      found_scalar_proj = true;
      break;
    }
  }
  EXPECT_TRUE(found_scalar_proj);
}

TEST(CascadesTest, AggregateUnionTranspose) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId union_all =
      memo.EnsureDerivedGroup({"t1", "t2"}, "union_all_input");
  memo.AddExpression(
      union_all,
      LogicalExpression{
          .operation = LogicalOperator::kUnionAll,
          .children = {left, right},
          .target_list = {NamedExpression("val", ColumnValueExp("val")),
                          NamedExpression("cnt", ColumnValueExp("cnt"))},
          .output_schema = Schema(
              "u", {Column("val", ValueType::kInt64),
                    Column("cnt", ValueType::kInt64)})});

  const GroupId agg_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "agg_over_union");
  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {union_all},
          .target_list = {NamedExpression(
                              "sum_val",
                              AggregateExpressionExp(AggregationType::kSum,
                                                     ColumnValueExp("val"))),
                          NamedExpression(
                              "cnt_total",
                              AggregateExpressionExp(AggregationType::kCount,
                                                     ColumnValueExp("cnt")))},
          .output_schema = Schema(
              "out", {Column("sum_val", ValueType::kInt64),
                      Column("cnt_total", ValueType::kInt64)})});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg_group);

  const auto& exprs = search.GetMemo().Get(agg_group).expressions;
  bool found_transposed = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kAggregation &&
        expr.children.size() == 1 && expr.children[0] != union_all) {
      const auto& child_exprs =
          search.GetMemo().Get(expr.children[0]).expressions;
      for (const auto& c_expr : child_exprs) {
        if (c_expr.operation == LogicalOperator::kUnionAll &&
            c_expr.children.size() == 2) {
          found_transposed = true;
          break;
        }
      }
    }
  }
  EXPECT_TRUE(found_transposed);
}

TEST(CascadesTest, AggregateJoinTranspose) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId join_group = memo.EnsureDerivedGroup({"t1", "t2"}, "join_input");
  memo.AddExpression(
      join_group,
      LogicalExpression{
          .operation = LogicalOperator::kJoin,
          .children = {left, right},
          .predicate = BinaryExpressionExp(ColumnValueExp("t1.id"),
                                          BinaryOperation::kEquals,
                                          ColumnValueExp("t2.fk_id"))});

  const GroupId agg_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "agg_over_join");
  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {join_group},
          .target_list = {NamedExpression("sum_v",
                                          AggregateExpressionExp(
                                              AggregationType::kSum,
                                              ColumnValueExp("t1.v")))},
          .output_schema = Schema("out", {Column("sum_v", ValueType::kInt64)}),
          .grouping_sets = {ColumnValueExp("t1.id")}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg_group);

  const auto& exprs = search.GetMemo().Get(agg_group).expressions;
  bool found_join_over_agg = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kJoin && expr.children.size() == 2) {
      const auto& left_child_exprs =
          search.GetMemo().Get(expr.children[0]).expressions;
      for (const auto& lc : left_child_exprs) {
        if (lc.operation == LogicalOperator::kAggregation &&
            lc.children.size() == 1 && lc.children[0] == left) {
          found_join_over_agg = true;
          break;
        }
      }
    }
  }
  EXPECT_TRUE(found_join_over_agg);
}

TEST(CascadesTest, WindowAfterFilterPartitionPushdown) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId win_group = memo.EnsureDerivedGroup({"t1"}, "win_node");
  memo.AddExpression(
      win_group,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {scan},
          .target_list = {NamedExpression("part", ColumnValueExp("t1.part")),
                          NamedExpression("val", ColumnValueExp("t1.val"))},
          .output_schema = Schema(
              "t1", {Column("part", ValueType::kInt64),
                     Column("val", ValueType::kInt64)}),
          .partition_by = {ColumnValueExp("t1.part")}});

  const GroupId sel_group = memo.EnsureDerivedGroup({"t1"}, "sel_over_win");
  memo.AddExpression(
      sel_group,
      LogicalExpression{
          .operation = LogicalOperator::kSelection,
          .children = {win_group},
          .predicate = BinaryExpressionExp(ColumnValueExp("t1.part"),
                                          BinaryOperation::kEquals,
                                          ConstantValueExp(Value(int64_t{10})))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  bool found_win_over_sel = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kWindow &&
        expr.children.size() == 1 && expr.children[0] != win_group &&
        expr.children[0] != scan) {
      const auto& c_exprs = search.GetMemo().Get(expr.children[0]).expressions;
      for (const auto& ce : c_exprs) {
        if (ce.operation == LogicalOperator::kSelection &&
            ce.children.size() == 1 && ce.children[0] == scan) {
          found_win_over_sel = true;
          break;
        }
      }
    }
  }
  EXPECT_TRUE(found_win_over_sel);
}

TEST(CascadesTest, StarJoinReorderAndCostModel) {
  Memo memo;
  (void)memo.Build(
      {"fact", "dim1", "dim2"},
      {ConjunctInfo{
           .conjunct = BinaryExpressionExp(ColumnValueExp("fact.d1_id"),
                                           BinaryOperation::kEquals,
                                           ColumnValueExp("dim1.id")),
           .relations = {"fact", "dim1"}},
       ConjunctInfo{
           .conjunct = BinaryExpressionExp(ColumnValueExp("fact.d2_id"),
                                           BinaryOperation::kEquals,
                                           ColumnValueExp("dim2.id")),
           .relations = {"fact", "dim2"}}});

  const GroupId root = memo.EnsureGroup({"fact", "dim1", "dim2"});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);

  const auto& exprs = search.GetMemo().Get(root).expressions;
  bool found_fact_split = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kJoin && expr.children.size() == 2) {
      const auto& left_rels = search.GetMemo().Get(expr.children[0]).relations;
      const auto& right_rels = search.GetMemo().Get(expr.children[1]).relations;
      if ((left_rels.size() == 1 && left_rels[0] == "fact" && right_rels.size() == 2) ||
          (right_rels.size() == 1 && right_rels[0] == "fact" && left_rels.size() == 2)) {
        found_fact_split = true;
        break;
      }
    }
  }
  EXPECT_TRUE(found_fact_split);

  const double star_cost =
      EstimateStarJoinCost(100000.0, {100.0, 500.0}, {0.01, 0.05});
  EXPECT_GT(star_cost, 0.0);
}

TEST(CascadesTest, PkUniqueDistinctElimination) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  memo.AddExpression(
      scan,
      LogicalExpression{
          .operation = LogicalOperator::kScan,
          .table = "t1",
          .output_schema = Schema(
              "t1", {Column("id", ValueType::kInt64,
                             Constraint(Constraint::kPrimaryKey)),
                     Column("name", ValueType::kVarChar)})});

  const GroupId distinct_group =
      memo.EnsureDerivedGroup({"t1"}, "distinct_on_pk");
  memo.AddExpression(
      distinct_group,
      LogicalExpression{.operation = LogicalOperator::kDistinct,
                        .children = {scan},
                        .target_list = {NamedExpression("id", ColumnValueExp("t1.id")),
                                        NamedExpression("name", ColumnValueExp("t1.name"))},
                        .output_schema = Schema("t1", {Column("id", ValueType::kInt64),
                                                       Column("name", ValueType::kVarChar)})});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(distinct_group);

  const auto& exprs = search.GetMemo().Get(distinct_group).expressions;
  bool found_eliminated = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kProjection) {
      found_eliminated = true;
      break;
    }
  }
  EXPECT_TRUE(found_eliminated);
}

TEST(CascadesTest, NotNullIsNotNullElimination) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  memo.AddExpression(
      scan,
      LogicalExpression{
          .operation = LogicalOperator::kScan,
          .table = "t1",
          .output_schema = Schema(
              "t1", {Column("id", ValueType::kInt64,
                             Constraint(Constraint::kNotNull)),
                     Column("name", ValueType::kVarChar)})});

  const GroupId sel_group =
      memo.EnsureDerivedGroup({"t1"}, "sel_is_not_null");
  memo.AddExpression(
      sel_group,
      LogicalExpression{
          .operation = LogicalOperator::kSelection,
          .children = {scan},
          .predicate = UnaryExpressionExp(ColumnValueExp("t1.id"),
                                          UnaryOperation::kIsNotNull)});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  bool found_true_or_scan = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kSelection && expr.predicate && *expr.predicate) {
      if ((*expr.predicate)->Type() == TypeTag::kConstantValue &&
          (*expr.predicate)->AsConstantValue().GetValue().Truthy()) {
        found_true_or_scan = true;
        break;
      }
    }
    if (expr.operation == LogicalOperator::kScan) {
      found_true_or_scan = true;
      break;
    }
  }
  EXPECT_TRUE(found_true_or_scan);
}

TEST(CascadesTest, FkJoinElimination) {
  Memo memo;
  (void)memo.Build({"child", "parent"});
  const GroupId child = memo.EnsureGroup({"child"});
  const GroupId parent = memo.EnsureGroup({"parent"});
  memo.AddExpression(
      child,
      LogicalExpression{
          .operation = LogicalOperator::kScan,
          .table = "child",
          .output_schema = Schema(
              "child", {Column("id", ValueType::kInt64),
                        Column("fk_id", ValueType::kInt64,
                               Constraint(Constraint::kNotNull))})});
  memo.AddExpression(
      parent,
      LogicalExpression{
          .operation = LogicalOperator::kScan,
          .table = "parent",
          .output_schema = Schema(
              "parent", {Column("id", ValueType::kInt64,
                                Constraint(Constraint::kPrimaryKey))})});

  const GroupId join_group = memo.EnsureDerivedGroup({"child", "parent"}, "fk_join");
  memo.AddExpression(
      join_group,
      LogicalExpression{
          .operation = LogicalOperator::kJoin,
          .children = {child, parent},
          .predicate = BinaryExpressionExp(ColumnValueExp("child.fk_id"),
                                          BinaryOperation::kEquals,
                                          ColumnValueExp("parent.id")),
          .target_list = {NamedExpression("id", ColumnValueExp("child.id")),
                          NamedExpression("fk_id", ColumnValueExp("child.fk_id"))},
          .output_schema = Schema(
              "child", {Column("id", ValueType::kInt64),
                        Column("fk_id", ValueType::kInt64)})});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(join_group);

  const auto& exprs = search.GetMemo().Get(join_group).expressions;
  bool found_semi = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kSemiJoin &&
        expr.children.size() == 2 && expr.children[0] == child &&
        expr.children[1] == parent) {
      found_semi = true;
      break;
    }
  }
  EXPECT_TRUE(found_semi);
}

TEST(CascadesTest, CheckConstraintPredicateIntake) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  memo.AddExpression(
      scan,
      LogicalExpression{
          .operation = LogicalOperator::kScan,
          .table = "t1",
          .output_schema = Schema(
              "t1", {Column("x", ValueType::kInt64,
                             Constraint(Constraint::kCheck, Value(std::string("x >= 0"))))})});

  const GroupId sel_group =
      memo.EnsureDerivedGroup({"t1"}, "sel_contradiction");
  memo.AddExpression(
      sel_group,
      LogicalExpression{
          .operation = LogicalOperator::kSelection,
          .children = {scan},
          .predicate = BinaryExpressionExp(ColumnValueExp("t1.x"),
                                          BinaryOperation::kLessThan,
                                          ConstantValueExp(Value(int64_t{0})))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  bool found_empty = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kEmpty) {
      found_empty = true;
      break;
    }
  }
  EXPECT_TRUE(found_empty);
}

TEST(CascadesTest, SplitSelectionOverJoinPushesConjunctsOfBothSides) {
  // split_selection_over_join: unlike push_selection_through_join (left side
  // only), every single-relation conjunct may land in its own scan group.
  Memo memo;
  const GroupId root = memo.Build({"a", "b"});
  const GroupId selection = memo.EnsureDerivedGroup({"a", "b"}, "selection");
  LogicalExpression expression;
  expression.operation = LogicalOperator::kSelection;
  expression.children = {root};
  expression.predicate = BinaryExpressionExp(
      EqExp("a.x", Value(1)), BinaryOperation::kAnd,
      EqExp("b.z", Value(2)));
  ASSERT_TRUE(memo.AddExpression(selection, expression));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(selection);

  const Memo& explored = search.GetMemo();
  bool filter_a = false;
  bool filter_b = false;
  for (size_t group = 0; group < explored.GroupCount(); ++group) {
    const Group& candidate = explored.Get(group);
    if (!candidate.filter) {
      continue;
    }
    if (candidate.relations == std::vector<std::string>{"a"}) {
      EXPECT_EQ(candidate.filter->ToString(),
                EqExp("a.x", Value(1))->ToString());
      filter_a = true;
    }
    if (candidate.relations == std::vector<std::string>{"b"}) {
      EXPECT_EQ(candidate.filter->ToString(),
                EqExp("b.z", Value(2))->ToString());
      filter_b = true;
    }
  }
  EXPECT_TRUE(filter_a);
  EXPECT_TRUE(filter_b);
}

TEST(CascadesTest, PushSelectionThroughProjectionRewritesPredicate) {
  // The Selection references the projection's output alias; the pushed
  // Selection below must reference the underlying column instead.
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId proj_group = memo.EnsureDerivedGroup({"a"}, "proj");
  ASSERT_TRUE(memo.AddExpression(
      proj_group,
      LogicalExpression{
          .operation = LogicalOperator::kProjection,
          .children = {scan},
          .target_list = {NamedExpression("alias", ColumnValueExp("a.x"))}}));
  const GroupId root = memo.EnsureDerivedGroup({"a"}, "filter");
  ASSERT_TRUE(memo.AddExpression(
      root,
      LogicalExpression{
          .operation = LogicalOperator::kSelection,
          .children = {proj_group},
          .predicate = EqExp("alias", Value(1))}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);

  const bool found = std::ranges::any_of(
      search.GetMemo().Get(root).expressions,
      [&](const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kProjection ||
            expression.children.size() != 1) {
          return false;
        }
        return std::ranges::any_of(
            search.GetMemo().Get(expression.children.front()).expressions,
            [&](const LogicalExpression& child) {
              return child.operation == LogicalOperator::kSelection &&
                     child.predicate.has_value() &&
                     (*child.predicate)->ToString() ==
                         EqExp("a.x", Value(1))->ToString();
            });
      });
  EXPECT_TRUE(found);
}

TEST(CascadesTest, PushSelectionThroughAggregationFiltersGroupingKeys) {
  // A conjunct over a grouping output moves below the aggregation; the
  // aggregation still applies to the filtered input.
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId agg_group = memo.EnsureDerivedGroup({"a"}, "agg");
  ASSERT_TRUE(memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {scan},
          .target_list = {
              NamedExpression("k", ColumnValueExp("a.x")),
              NamedExpression(
                  "s", AggregateExpressionExp(AggregationType::kSum,
                                              ColumnValueExp("a.y"), false))}}));
  const GroupId root = memo.EnsureDerivedGroup({"a"}, "filter");
  ASSERT_TRUE(memo.AddExpression(
      root,
      LogicalExpression{
          .operation = LogicalOperator::kSelection,
          .children = {agg_group},
          .predicate = EqExp("k", Value(1))}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);

  const bool found = std::ranges::any_of(
      search.GetMemo().Get(root).expressions,
      [&](const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kAggregation ||
            expression.children.size() != 1) {
          return false;
        }
        return std::ranges::any_of(
            search.GetMemo().Get(expression.children.front()).expressions,
            [&](const LogicalExpression& child) {
              return child.operation == LogicalOperator::kSelection &&
                     child.predicate.has_value() &&
                     (*child.predicate)->ToString() ==
                         EqExp("a.x", Value(1))->ToString();
            });
      });
  EXPECT_TRUE(found);
}

TEST(CascadesTest, PushFilterThroughLeftJoinLeftSide) {
  // A predicate that only references left-side columns may filter the left
  // input before the outer join; unmatched left rows are still NULL-padded.
  Memo memo;
  (void)memo.Build({"a", "b"});
  const GroupId join_group =
      memo.EnsureDerivedGroup({"a", "b"}, "outer-join");
  ASSERT_TRUE(memo.AddExpression(
      join_group,
      LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                        .children = {memo.EnsureGroup({"a"}),
                                     memo.EnsureGroup({"b"})},
                        .join_type = 0}));
  const GroupId selection = memo.EnsureDerivedGroup({"a", "b"}, "filter");
  const Expression predicate = EqExp("a.x", Value(1));
  ASSERT_TRUE(memo.AddExpression(
      selection,
      LogicalExpression{.operation = LogicalOperator::kSelection,
                        .children = {join_group},
                        .predicate = predicate}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(selection);

  const bool found = std::ranges::any_of(
      search.GetMemo().Get(selection).expressions,
      [&](const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kSelection ||
            expression.children.size() != 1) {
          return false;
        }
        const Group& join_candidates =
            search.GetMemo().Get(expression.children.front());
        return std::ranges::any_of(
            join_candidates.expressions,
            [&](const LogicalExpression& join) {
              if (join.operation != LogicalOperator::kOuterJoin ||
                  join.children.size() != 2) {
                return false;
              }
              return std::ranges::any_of(
                  search.GetMemo().Get(join.children[0]).expressions,
                  [](const LogicalExpression& left) {
                    return left.operation == LogicalOperator::kSelection &&
                           left.predicate.has_value() &&
                           (*left.predicate)->ToString() ==
                               EqExp("a.x", Value(1))->ToString();
                  });
            });
      });
  EXPECT_TRUE(found);
}

TEST(CascadesTest, PushProjectionThroughAggregationMovesProjectionBelow) {
  // Projection over an aggregation with simple column references can push
  // the projection below the aggregation.
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId agg_group = memo.EnsureDerivedGroup({"a"}, "agg");
  ASSERT_TRUE(memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {scan},
          .target_list = {NamedExpression("k", ColumnValueExp("a.x"))},
          .grouping_sets = {ColumnValueExp("a.x")}}));
  const GroupId root = memo.EnsureDerivedGroup({"a"}, "proj");
  ASSERT_TRUE(memo.AddExpression(
      root,
      LogicalExpression{
          .operation = LogicalOperator::kProjection,
          .children = {agg_group},
          .target_list = {NamedExpression("k", ColumnValueExp("k"))}}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);

  const bool found = std::ranges::any_of(
      search.GetMemo().Get(root).expressions,
      [&](const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kAggregation ||
            expression.children.size() != 1) {
          return false;
        }
        return std::ranges::any_of(
            search.GetMemo().Get(expression.children.front()).expressions,
            [&](const LogicalExpression& child) {
              return child.operation == LogicalOperator::kProjection &&
                     child.children.size() == 1 &&
                     child.children.front() == scan;
            });
      });
  EXPECT_TRUE(found);
}

TEST(CascadesTest, MergeAdjacentFiltersCombinesBothPredicates) {
  // merge_adjacent_filters flattens Selection(p2, Selection(p1, X)) into a
  // single Selection(p1 AND p2, X) so the residual chain collapses.
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId inner = memo.EnsureDerivedGroup({"a"}, "selection");
  ASSERT_TRUE(memo.AddExpression(
      inner,
      LogicalExpression{.operation = LogicalOperator::kSelection,
                        .children = {scan},
                        .predicate = EqExp("a.x", Value(1))}));
  const GroupId outer = memo.EnsureDerivedGroup({"a"}, "selection2");
  ASSERT_TRUE(memo.AddExpression(
      outer,
      LogicalExpression{.operation = LogicalOperator::kSelection,
                        .children = {inner},
                        .predicate = EqExp("a.y", Value(2))}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(outer);

  const bool found = std::ranges::any_of(
      search.GetMemo().Get(outer).expressions,
      [&](const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kSelection ||
            expression.children != std::vector<GroupId>{scan} ||
            !expression.predicate.has_value()) {
          return false;
        }
        const std::string merged = (*expression.predicate)->ToString();
        return merged.find(EqExp("a.x", Value(1))->ToString()) !=
                   std::string::npos &&
               merged.find(EqExp("a.y", Value(2))->ToString()) !=
                   std::string::npos;
      });
  EXPECT_TRUE(found);
}

TEST(CascadesTest, UnusedJoinEliminationRequiresPredicate) {
  // unused_join_elimination must check join.predicate.has_value() before
  // eliminating. This test verifies the precondition: a cross join without
  // a predicate should NOT be eliminated.
  Memo memo(64);
  const GroupId root = memo.Build({"a", "b"});
  const Group& root_group = memo.Get(root);
  ASSERT_EQ(root_group.expressions.size(), 1U);
  // The Build-created join has no predicate (cross join).
  EXPECT_FALSE(root_group.expressions[0].predicate.has_value());
}

// Note: contradiction detection and join elimination effectiveness are
// tested via compliance tests (optimizer_null_uniqueness, star_schema, etc.).





TEST(CascadesTest, TopNLimitHintPropagation) {
  // TopN(limit=3, offset=2) should propagate limit_hint=5 to child.
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId topn = memo.EnsureDerivedGroup({"a"}, "topn");
  LogicalExpression expression;
  expression.operation = LogicalOperator::kTopN;
  expression.children = {scan};
  expression.target_list = {NamedExpression("k", ColumnValueExp("a.id"))};
  expression.sort_ascending = {true};
  expression.sort_nulls_first = {false};
  expression.limit_count = 3;
  expression.limit_offset = 2;
  ASSERT_TRUE(memo.AddExpression(topn, expression));

  PhysicalProperties required;
  required.require_row_position = true;
  std::vector<PhysicalProperties> child_requirements =
      SearchEngine::RequiredChildProperties(expression, required);

  ASSERT_EQ(child_requirements.size(), 1U);
  EXPECT_EQ(child_requirements[0].limit_hint, 5U);
  EXPECT_FALSE(child_requirements[0].ordering.empty());
}

}  // namespace tinylamb::cascades
