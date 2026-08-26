/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/cascades.hpp"

#include <algorithm>
#include <limits>
#include <optional>
#include <cstddef>
#include <ranges>
#include <sstream>
#include <utility>
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
  EXPECT_NE(std::ranges::find_if(
                memo.Get(root).expressions, [](const LogicalExpression& item) {
                  return item.operation == LogicalOperator::kOuterJoin &&
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

TEST(CascadesTest, DummyScanAndValuesHavePhysicalImplementationRules) {
  Memo dummy_memo;
  const GroupId dummy = dummy_memo.EnsureDerivedGroup({}, "dummy");
  ASSERT_TRUE(dummy_memo.AddExpression(
      dummy, LogicalExpression{.operation = LogicalOperator::kDummyScan}));
  SearchEngine dummy_search(std::move(dummy_memo), RuleSet::Default());
  const auto dummy_plan = dummy_search.Optimize(
      dummy, PhysicalProperties{}, tinylamb::DefaultImplementationRules());
  ASSERT_TRUE(dummy_plan.has_value());
  EXPECT_NE(dummy_plan->plan->ToString().find("DummyScan"), std::string::npos);

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
  EXPECT_NE(values_plan->plan->ToString().find("Values"), std::string::npos);
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

TEST(CascadesTest, TopNIsALogicalOperatorWithIndependentChildRequirements) {
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
  EXPECT_TRUE(child_requirements[0].ordering.empty());
  EXPECT_EQ(child_requirements[0].limit_hint,
            std::numeric_limits<size_t>::max());
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
  EXPECT_EQ(children[0].limit_hint,
            std::numeric_limits<size_t>::max());

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
  EXPECT_TRUE(std::ranges::any_of(
      search.GetMemo().Get(root).expressions,
      [](const LogicalExpression& expression) {
        return expression.operation == LogicalOperator::kUnionAll &&
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
      empty_branch,
      LogicalExpression{.operation = LogicalOperator::kEmpty,
                        .children = {memo.EnsureGroup({"b"})}}));
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kIntersect,
                              .children = {left, empty_branch}}));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);
  EXPECT_TRUE(std::ranges::any_of(
      search.GetMemo().Get(root).expressions,
      [](const LogicalExpression& expression) {
        return expression.operation == LogicalOperator::kEmpty;
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

  EXPECT_TRUE(std::ranges::any_of(
      search.GetMemo().Get(union_all).expressions,
      [](const LogicalExpression& candidate) {
        return candidate.operation == LogicalOperator::kScan;
      }));
  EXPECT_TRUE(std::ranges::any_of(
      search.GetMemo().Get(except_all).expressions,
      [](const LogicalExpression& candidate) {
        return candidate.operation == LogicalOperator::kScan;
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
  const std::optional<BestPlan> best = search.Optimize(
      root, required, tinylamb::DefaultImplementationRules());
  ASSERT_TRUE(best.has_value());
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
        return std::ranges::all_of(
            expression.children, [&](GroupId child) {
              return std::ranges::any_of(
                  search.GetMemo().Get(child).expressions,
                  [](const LogicalExpression& branch) {
                    return branch.operation == LogicalOperator::kSelection &&
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
        return std::ranges::all_of(
            expression.children, [&](GroupId child) {
              return std::ranges::any_of(
                  search.GetMemo().Get(child).expressions,
                  [](const LogicalExpression& branch) {
                    return branch.operation == LogicalOperator::kProjection;
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
        return std::ranges::any_of(
            search.GetMemo().Get(child).expressions,
            [expected](const LogicalExpression& item) {
              return item.operation == LogicalOperator::kProjection &&
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
      join_group, LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                                    .children = {left, right},
                                    .predicate = BinaryExpressionExp(
                                        ColumnValueExp("a.id"),
                                        BinaryOperation::kEquals,
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
               search.GetMemo().Get(item.children.front()).tag.starts_with(
                   "join-project-join:");
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

TEST(CascadesTest, PushTopNThroughProjectionRewritesOrderingExpression) {
  Memo memo;
  const GroupId scan = memo.Build({"items"});
  const GroupId projection = memo.EnsureDerivedGroup({"items"}, "projection");
  ASSERT_TRUE(memo.AddExpression(
      projection,
      LogicalExpression{
          .operation = LogicalOperator::kProjection,
          .children = {scan},
          .target_list = {NamedExpression("value", ColumnValueExp("items.v")),
                          NamedExpression("label", ColumnValueExp("items.s"))}}));
  const GroupId root = memo.EnsureDerivedGroup({"items"}, "topn");
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kTopN,
                              .children = {projection},
                              .target_list = {NamedExpression(
                                  "", ColumnValueExp("value"))},
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
      LogicalExpression{.operation = LogicalOperator::kSort,
                        .children = {scan},
                        .target_list = {NamedExpression(
                            "", ColumnValueExp("items.a")),
                                        NamedExpression(
                                            "", ColumnValueExp("items.b"))},
                        .sort_ascending = {true, true}}));
  const GroupId root = memo.EnsureDerivedGroup({"items"}, "outer-sort");
  ASSERT_TRUE(memo.AddExpression(
      root,
      LogicalExpression{.operation = LogicalOperator::kSort,
                        .children = {inner},
                        .target_list = {NamedExpression(
                            "", ColumnValueExp("items.a"))},
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
  EXPECT_NE(b.filter->ToString().find("1"), std::string::npos);
}

}  // namespace tinylamb::cascades
