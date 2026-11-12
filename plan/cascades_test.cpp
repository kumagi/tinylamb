/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/cascades.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "gtest/gtest.h"
#include "plan/implementation_rules.hpp"
#include "plan/set_operation_plan.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::cascades {

TEST(CascadesTest, ReconsidersParentAfterChildAlternativeAppears) {
  Memo memo;
  const auto leaf = memo.Build({"t"});
  const auto child = memo.EnsureDerivedGroup({"t"}, "child");
  memo.AddExpression(child,
                     {.operation = LogicalOperator::kScan, .table = "t"});
  const auto root = memo.EnsureDerivedGroup({"t"}, "root");
  memo.AddExpression(
      root, {.operation = LogicalOperator::kProjection,
             .children = {child},
             .target_list = {NamedExpression("x", ColumnValueExp("t.x"))}});
  RuleSet rules;
  rules.Add(Rule(
      "consume", dsl::Projection(dsl::Selection(dsl::Any())),
      [](const Bindings&, Memo& target, GroupId group,
         const LogicalExpression& expr) {
        target.AddExpression(group, {.operation = LogicalOperator::kLimit,
                                     .children = expr.children,
                                     .limit_count = 1});
      },
      LogicalOperator::kProjection));
  rules.Add(Rule(
      "produce", dsl::Projection(),
      [child, leaf](const Bindings&, Memo& target, GroupId,
                    const LogicalExpression&) {
        target.AddExpression(child,
                             {.operation = LogicalOperator::kSelection,
                              .children = {leaf},
                              .predicate = ConstantValueExp(Value(true))});
      },
      LogicalOperator::kProjection));
  SearchEngine search(std::move(memo), rules);
  search.Explore(root);
  EXPECT_TRUE(search.AppliedRuleNames().contains("consume"));
  EXPECT_EQ(search.GetMemo().ExpressionCount(root), 2U);
}

TEST(CascadesTest, LargeJoinSeedUsesFilteredRowsAndPreservesConnectivity) {
  Memo memo;
  std::vector<std::string> relations;
  std::vector<ConjunctInfo> predicates;
  std::unordered_map<std::string, double> rows;
  for (int i = 0; i < 17; ++i) {
    const std::string name = "t" + std::to_string(i);
    relations.push_back(name);
    rows[name] = i == 16 ? 100000.0 : 10.0;
    if (i > 0) {
      const std::string previous = "t" + std::to_string(i - 1);
      predicates.push_back({BinaryExpressionExp(ColumnValueExp(previous + ".k"),
                                                BinaryOperation::kEquals,
                                                ColumnValueExp(name + ".k")),
                            {previous, name}});
    }
  }
  memo.SetRelationRows(std::move(rows));
  const auto root = memo.Build(relations, predicates);
  const auto& join = memo.Get(root).expressions.front();
  EXPECT_EQ(memo.Get(join.children[0]).relations,
            std::vector<std::string>{"t16"});
  EXPECT_TRUE(
      memo.InducedSubgraphConnected(memo.Get(join.children[1]).relation_mask));
}

TEST(CascadesTest, DefaultRulesEnumerateEquivalentJoinTreesInMemo) {
  Memo memo;
  const GroupId root = memo.Build({"a", "b", "c"});
  SearchEngine search(std::move(memo), RuleSet::Default());

  search.Explore(root);

  EXPECT_GE(search.GetMemo().ExpressionCount(root), 4);
  EXPECT_GE(search.GetMemo().GroupCount(), 6);
}

TEST(CascadesTest, JoinCommutativityPreservesInheritedPredicate) {
  Memo memo;
  (void)memo.Build({"a", "b"});
  const GroupId a = memo.EnsureGroup({"a"});
  const GroupId b = memo.EnsureGroup({"b"});
  const GroupId group =
      memo.EnsureDerivedGroup({"a", "b"}, "inherited_join_predicate");
  const Expression predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("a", "id")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("b", "id")));
  memo.AddExpression(group,
                     LogicalExpression{.operation = LogicalOperator::kJoin,
                                       .children = {a, b},
                                       .predicate = predicate});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(group);

  bool found_commuted = false;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(group).expressions) {
    if (expression.operation != LogicalOperator::kJoin ||
        expression.children.size() != 2 || expression.children[0] != b ||
        expression.children[1] != a) {
      continue;
    }
    found_commuted = true;
    ASSERT_TRUE(expression.predicate.has_value());
    // The plain assert teaches the optional-access checker what ASSERT_TRUE
    // already guarantees.
    assert(expression.predicate.has_value());
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    EXPECT_EQ(expression.predicate.value()->ToString(), predicate->ToString());
  }
  EXPECT_TRUE(found_commuted);
}

TEST(CascadesTest, NonCanonicalJoinPredicateDoesNotReassociate) {
  const Expression ab_predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("a", "id")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("b", "id")));
  const Expression bc_predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("b", "id")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("c", "id")));
  Memo memo;
  (void)memo.Build({"a", "b", "c"},
                   {{.conjunct = ab_predicate, .relations = {"a", "b"}},
                    {.conjunct = bc_predicate, .relations = {"b", "c"}}});
  const GroupId a = memo.EnsureGroup({"a"});
  const GroupId b = memo.EnsureGroup({"b"});
  const GroupId c = memo.EnsureGroup({"c"});
  const GroupId ab = memo.EnsureGroup({"a", "b"});
  const GroupId all =
      memo.EnsureDerivedGroup({"a", "b", "c"}, "partial_join_predicate");
  memo.AddExpression(ab, LogicalExpression{.operation = LogicalOperator::kJoin,
                                           .children = {a, b},
                                           .predicate = ab_predicate});
  memo.AddExpression(all, LogicalExpression{.operation = LogicalOperator::kJoin,
                                            .children = {ab, c},
                                            .predicate = ab_predicate});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(all);

  const Memo& explored = search.GetMemo();
  for (const LogicalExpression& expression : explored.Get(all).expressions) {
    if (expression.operation != LogicalOperator::kJoin ||
        expression.children.size() != 2) {
      continue;
    }
    const std::vector<std::string> left =
        explored.Get(expression.children[0]).relations;
    const std::vector<std::string> right =
        explored.Get(expression.children[1]).relations;
    const bool unsafe_a_bc =
        (left.size() == 1 && left[0] == "a" && right.size() == 2) ||
        (right.size() == 1 && right[0] == "a" && left.size() == 2);
    const bool unsafe_b_ac =
        (left.size() == 1 && left[0] == "b" && right.size() == 2) ||
        (right.size() == 1 && right[0] == "b" && left.size() == 2);
    EXPECT_FALSE(unsafe_a_bc || unsafe_b_ac);
  }
}

TEST(CascadesTest, JoinEnumerationBoundsConnectedJoinClosure) {
  const std::vector<std::string> relations{"t0", "t1", "t2", "t3",
                                           "t4", "t5", "t6", "t7"};
  std::vector<ConjunctInfo> conjuncts;
  conjuncts.reserve(relations.size() - 1);
  for (size_t i = 1; i < relations.size(); ++i) {
    conjuncts.push_back(
        ConjunctInfo{.conjunct = BinaryExpressionExp(
                         ColumnValueExp(ColumnName(relations[i - 1], "id")),
                         BinaryOperation::kEquals,
                         ColumnValueExp(ColumnName(relations[i], "id"))),
                     .relations = {relations[i - 1], relations[i]}});
  }
  Memo memo;
  const GroupId root = memo.Build(relations, conjuncts);
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);

  EXPECT_TRUE(search.GetMemo().JoinEnumerationClaimed(root));
  EXPECT_LE(search.GetMemo().ExpressionCount(root), 32U);
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
  EXPECT_FALSE(memo.TryBuild({"a", "a"}).HasValue());
}

TEST(CascadesTest, MemoAddExpressionRejectsScanWithChildren) {
  // Arrange: a single-relation group holds a scan expression.
  Memo memo;
  const GroupId root = memo.Build({"a"});

  // Act + Assert: a scan carrying child groups does not belong to the group.
  EXPECT_DEATH(memo.AddExpression(
                   root, LogicalExpression{.operation = LogicalOperator::kScan,
                                           .children = {root},
                                           .table = "a"}),
               "does not belong");
}

TEST(CascadesTest, MemoAddExpressionRejectsJoinWithWrongArity) {
  // Arrange: a single-relation group.
  Memo memo;
  const GroupId root = memo.Build({"a"});

  // Act + Assert: a join must have exactly two child groups.
  EXPECT_DEATH(memo.AddExpression(
                   root, LogicalExpression{.operation = LogicalOperator::kJoin,
                                           .children = {root, root, root}}),
               "one or two child groups");
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
  EXPECT_DEATH(
      memo.AddExpression(a_group,
                         LogicalExpression{.operation = LogicalOperator::kJoin,
                                           .children = {b_group, a_group}}),
      "not equivalent");
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

TEST(CascadesTest, RuleTargetListPrefiltersOtherOperators) {
  // Arrange: a rule whose body only acts on set-operation expressions, now
  // declaring those roots explicitly. The prefilter must keep it away from
  // scan/join expressions without changing what it does on its roots.
  using namespace dsl;
  Memo memo;
  const GroupId root = memo.Build({"a", "b"});
  bool invoked_on_join = false;
  RuleSet rules;
  rules.Add(Rule("setop_probe", Pattern::Any(),
                 [&](const Bindings&, Memo&, GroupId,
                     const LogicalExpression&) { invoked_on_join = true; },
                 {LogicalOperator::kUnion, LogicalOperator::kUnionAll,
                  LogicalOperator::kIntersect, LogicalOperator::kExcept}));

  // Act: exploring the two-relation join must not invoke the rule (the memo
  // holds only scan and join expressions), while MayApply stays true for
  // every declared root and false for everything else.
  SearchEngine search(std::move(memo), rules);
  search.Explore(root);
  EXPECT_FALSE(invoked_on_join);
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
  if (!dummy_plan) {
    return;
  }
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
  if (!values_plan) {
    return;
  }
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

TEST(CascadesTest, AuditDefaultOrderDirectionIsAscendingInMemoKey) {
  PhysicalProperties implicit;
  implicit.ordering = {ColumnName("t.x")};
  PhysicalProperties ascending = implicit;
  ascending.sort_ascending = {true};
  PhysicalProperties descending = implicit;
  descending.sort_ascending = {false};
  EXPECT_EQ(implicit.Key(), ascending.Key());
  EXPECT_NE(implicit.Key(), descending.Key());
}

TEST(CascadesTest, PhysicalPropertiesKeyIncludesDistinctAndNullPlacement) {
  PhysicalProperties base;
  base.ordering = {ColumnName("a", "k")};
  base.sort_ascending = {true};
  base.sort_nulls_first = {std::nullopt};

  PhysicalProperties distinct = base;
  distinct.distinct = true;
  PhysicalProperties explicit_last = base;
  explicit_last.sort_nulls_first = {std::optional<bool>(false)};
  PhysicalProperties missing = base;
  missing.sort_nulls_first.clear();

  EXPECT_NE(base.Key(), distinct.Key());
  EXPECT_NE(base.Key(), explicit_last.Key());
  EXPECT_NE(base.Key(), missing.Key());
}

TEST(CascadesTest, ExploreRequeuesGroupTouchedAfterItWasPopped) {
  using namespace dsl;
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId root = memo.EnsureDerivedGroup({"a"}, "requeue");
  const auto selection = [&](int64_t value) {
    return LogicalExpression{.operation = LogicalOperator::kSelection,
                             .children = {scan},
                             .predicate = ConstantValueExp(Value(value))};
  };
  ASSERT_TRUE(memo.AddExpression(root, selection(1)));

  bool observed = false;
  RuleSet rules;
  rules.Add(Rule("produce_late_selection",
                 Pattern::Op(LogicalOperator::kSelection, {Pattern::Any()}),
                 [&](const Bindings&, Memo& mutable_memo, GroupId group,
                     const LogicalExpression& expression) {
                   if (expression.predicate && *expression.predicate &&
                       (*expression.predicate)->Type() ==
                           TypeTag::kConstantValue &&
                       (*expression.predicate)
                               ->AsConstantValue()
                               .GetValue()
                               .value.int_value == 1) {
                     (void)mutable_memo.AddExpression(group, selection(2));
                   }
                 }));
  rules.Add(Rule("observe_late_selection",
                 Pattern::Op(LogicalOperator::kSelection, {Pattern::Any()}),
                 [&](const Bindings&, Memo&, GroupId,
                     const LogicalExpression& expression) {
                   if (expression.predicate && *expression.predicate &&
                       (*expression.predicate)->Type() ==
                           TypeTag::kConstantValue &&
                       (*expression.predicate)
                               ->AsConstantValue()
                               .GetValue()
                               .value.int_value == 2) {
                     observed = true;
                   }
                 }));

  SearchEngine search(std::move(memo), rules);
  search.Explore(root);

  EXPECT_TRUE(observed);
  EXPECT_GE(search.GetMemo().ExpressionCount(root), 2U);
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

TEST(CascadesTest, StructuralFingerprintSeparatesAggregateFilter) {
  const Expression plain =
      AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("a.x"));
  auto filtered = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                        ColumnValueExp("a.x"));
  filtered->SetWhereFilter(
      BinaryExpressionExp(ColumnValueExp("a.x"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(int64_t{0}))));
  LogicalExpression first;
  first.operation = LogicalOperator::kProjection;
  first.target_list = {NamedExpression("v", plain)};
  LogicalExpression second = first;
  second.target_list = {NamedExpression("v", filtered)};
  EXPECT_NE(first.Fingerprint(), second.Fingerprint());
}

TEST(CascadesTest, StructuralFingerprintSeparatesTypedConstants) {
  LogicalExpression integer;
  integer.operation = LogicalOperator::kSelection;
  integer.predicate =
      BinaryExpressionExp(ColumnValueExp("a.x"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{1})));
  LogicalExpression real = integer;
  real.predicate =
      BinaryExpressionExp(ColumnValueExp("a.x"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(1.0)));
  EXPECT_NE(integer.Fingerprint(), real.Fingerprint());
}

TEST(CascadesTest, StructuralFingerprintKeepsExactScalarEncoding) {
  const auto identity = [](const Value& value) {
    LogicalExpression expression;
    expression.operation = LogicalOperator::kProjection;
    expression.target_list = {NamedExpression("v", ConstantValueExp(value))};
    return expression.Fingerprint();
  };
  const auto minimum = identity(Value(std::numeric_limits<int64_t>::min()));
  EXPECT_NE(minimum.find("-9223372036854775808"), std::string::npos);
  EXPECT_NE(minimum.find("#t:v=e"), std::string::npos);
  const auto maximum = identity(Value(std::numeric_limits<int64_t>::max()));
  EXPECT_NE(maximum.find("9223372036854775807"), std::string::npos);
  EXPECT_NE(minimum, maximum);
  EXPECT_EQ(minimum, identity(Value(std::numeric_limits<int64_t>::min())));
  const std::string embedded("x:;\0y", 5);
  EXPECT_NE(identity(Value(std::string(embedded))).find(embedded),
            std::string::npos);
  EXPECT_NE(identity(Value(std::string(embedded))), identity(Value("x:;")));
  EXPECT_NE(identity(Value()), identity(Value("")));
  EXPECT_NE(identity(Value(0.0)), identity(Value(-0.0)));
  EXPECT_NE(identity(Value::Array({Value(1), Value()}, "INT64")),
            identity(Value::Array({Value(1), Value()}, "DOUBLE")));
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
  EXPECT_DEATH(memo.AddExpression(derived, no_predicate),
               "selection must carry a predicate");
  // ...and must preserve the group's relation set.
  LogicalExpression wrong_relations;
  wrong_relations.operation = LogicalOperator::kSelection;
  wrong_relations.children = {join_root};
  wrong_relations.predicate = EqExp("a.x", Value(1));
  EXPECT_DEATH(memo.AddExpression(derived, wrong_relations),
               "preserve the group");
  // Projections need a target list.
  LogicalExpression no_targets;
  no_targets.operation = LogicalOperator::kProjection;
  no_targets.children = {scan};
  EXPECT_DEATH(memo.AddExpression(derived, no_targets), "target list");
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

  // Assert: the result owns its scan filter; the shared input stays unchanged.
  EXPECT_FALSE(search.GetMemo().Get(scan).filter);
  ASSERT_TRUE(search.GetMemo().Get(selection).filter);
  EXPECT_EQ(search.GetMemo().Get(selection).filter->ToString(),
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
  EXPECT_TRUE(group.expressions[1].children == std::vector<GroupId>{scan});
}

TEST(CascadesTest, ScanFilterOrdersCheapConjunctsBeforeLike) {
  // Arrange: one relation carries a LIKE (per-row pattern match) and a literal
  // comparison. Canonical order used to be plain ToString() order, which put
  // "a.x LIKE ..." (x < z) ahead of "a.z = 1" and paid the pattern match on
  // rows the comparison would have rejected.
  Memo memo;
  const GroupId root = memo.Build(
      {"a", "b"},
      {{BinaryExpressionExp(ColumnValueExp("a.x"), BinaryOperation::kLike,
                            ConstantValueExp(Value(std::string("abc%")))),
        {"a"}},
       {EqExp("a.z", Value(int64_t{1})), {"a"}},
       {BinaryExpressionExp(ColumnValueExp("a.x"), BinaryOperation::kEquals,
                            ColumnValueExp("b.y")),
        {"a", "b"}}});

  // Act: memo construction merges the single-relation conjuncts into the scan.
  const Group& scan = memo.Get(memo.EnsureGroup({"a"}));

  // Assert: the cheap comparison is evaluated first, LIKE last; the spanning
  // conjunct still belongs to the join above.
  ASSERT_TRUE(scan.filter);
  const std::string text = scan.filter->ToString();
  EXPECT_NE(text.find("a.z = 1"), std::string::npos) << text;
  EXPECT_NE(text.find("a.x LIKE"), std::string::npos) << text;
  EXPECT_LT(text.find("a.z = 1"), text.find("a.x LIKE")) << text;
  EXPECT_FALSE(memo.Get(root).filter) << "spanning conjunct leaked into a scan";
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
  EXPECT_EQ(pruned_search.GetMemo().ExpressionCount(pruned_root), 2U);
  // The disconnected graph retains the six inner join shapes plus their
  // explicit CrossJoin alternatives.
  EXPECT_EQ(exhaustive_search.GetMemo().ExpressionCount(exhaustive_root), 6U);
}

TEST(CascadesTest, JoinEnumerationKeepsChainSplitsConnected) {
  // A chain a-b-c-d-e-f has only n-1 csg-cmp cuts. Enumeration emits those
  // once per group; commutativity mirrors them. Cross-product bipartitions
  // such as {a,c}|{b,d,e,f} stay out of the root.
  std::vector<ConjunctInfo> conjuncts;
  const std::vector<std::string> relations = {"a", "b", "c", "d", "e", "f"};
  for (size_t i = 0; i + 1 < relations.size(); ++i) {
    conjuncts.push_back(
        {BinaryExpressionExp(ColumnValueExp(relations[i] + ".x"),
                             BinaryOperation::kEquals,
                             ColumnValueExp(relations[i + 1] + ".x")),
         {relations[i], relations[i + 1]}});
  }
  Memo memo;
  const GroupId root = memo.Build(relations, conjuncts);
  RuleSet rules = RuleSet::Default();
  ASSERT_TRUE(rules.Remove("join_associativity_left"));
  ASSERT_TRUE(rules.Remove("join_associativity_right"));
  SearchEngine search(std::move(memo), rules);

  search.Explore(root);

  // 5 contiguous cuts, each swapped by commutativity. The initial connected
  // split is one of those cuts.
  EXPECT_LE(search.GetMemo().ExpressionCount(root), 12U);
  EXPECT_GE(search.GetMemo().ExpressionCount(root), 5U);
  bool saw_disconnected = false;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(root).expressions) {
    if (expression.operation != LogicalOperator::kJoin ||
        expression.children.size() != 2) {
      continue;
    }
    const uint64_t left =
        search.GetMemo().Get(expression.children[0]).relation_mask;
    const uint64_t right =
        search.GetMemo().Get(expression.children[1]).relation_mask;
    if (!search.GetMemo().InducedSubgraphConnected(left) ||
        !search.GetMemo().InducedSubgraphConnected(right)) {
      saw_disconnected = true;
    }
  }
  EXPECT_FALSE(saw_disconnected);
}

TEST(CascadesTest, BushyBipartitionsAppearWithAndWithoutEnumeration) {
  // Arrange: a four-relation chain a-b-c-d. The {a,b}|{c,d} split crosses the
  // b-c conjunct and is connected on both sides, so a complete bushy search
  // must derive it; memo construction only produces a 1+3 split, so the shape
  // cannot come from the initial tree alone.
  std::vector<ConjunctInfo> conjuncts;
  const std::vector<std::string> relations = {"a", "b", "c", "d"};
  for (size_t i = 0; i + 1 < relations.size(); ++i) {
    conjuncts.push_back(
        {BinaryExpressionExp(ColumnValueExp(relations[i] + ".x"),
                             BinaryOperation::kEquals,
                             ColumnValueExp(relations[i + 1] + ".x")),
         {relations[i], relations[i + 1]}});
  }
  const auto saw_bushy_split = [](const Memo& memo, GroupId root) {
    return std::ranges::any_of(
        memo.Get(root).expressions, [&](const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kJoin ||
              expression.children.size() != 2) {
            return false;
          }
          return memo.Get(expression.children[0]).relations.size() >= 2 &&
                 memo.Get(expression.children[1]).relations.size() >= 2;
        });
  };

  // Act + Assert: the full rule set, where join_enumeration claims the group
  // and enumerates the csg-cmp bipartitions directly.
  Memo enumerated;
  const GroupId enumerated_root = enumerated.Build(relations, conjuncts);
  SearchEngine enumerated_search(std::move(enumerated), RuleSet::Default());
  enumerated_search.Explore(enumerated_root);
  EXPECT_TRUE(saw_bushy_split(enumerated_search.GetMemo(), enumerated_root));

  // Act + Assert: with enumeration removed, commutativity plus the two
  // associativity rotations must still reach a bushy topology (the right
  // rotation turns a JOIN (b JOIN (c JOIN d)) into (a JOIN b) JOIN (c JOIN d)).
  Memo rotated;
  const GroupId rotated_root = rotated.Build(relations, conjuncts);
  RuleSet rules = RuleSet::Default();
  ASSERT_TRUE(rules.Remove("join_enumeration"));
  SearchEngine rotated_search(std::move(rotated), rules);
  rotated_search.Explore(rotated_root);
  EXPECT_TRUE(saw_bushy_split(rotated_search.GetMemo(), rotated_root));
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
  EXPECT_DEATH(memo.AddExpression(set, invalid), "at least two children");
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
  if (!best) {
    return;
  }
  const auto merged = std::dynamic_pointer_cast<SetOperationPlan>(best->plan);
  ASSERT_TRUE(merged);
  EXPECT_EQ(merged->ToString(), "MergeAppend");
  EXPECT_TRUE(merged->IsOrderedBy({ColumnValueExp("v")}, {true}));
}

TEST(CascadesTest, SelectionIsPushedIntoEverySetOperationBranch) {
  Memo memo;
  (void)memo.Build({"a", "b"});
  // An unqualified predicate column must resolve in EVERY branch before the
  // push; publish the branch schemas proving `v` exists in both.
  memo.SetTableSchemas({{"a", Schema("a", {Column("v", ValueType::kInt64)})},
                        {"b", Schema("b", {Column("v", ValueType::kInt64)})}});
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

TEST(CascadesTest, MergeLimitsProducesEmptyWhenOffsetReachesInnerLimit) {
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId inner = memo.EnsureDerivedGroup({"a"}, "inner-limit-empty");
  ASSERT_TRUE(memo.AddExpression(
      inner, LogicalExpression{.operation = LogicalOperator::kLimit,
                               .children = {scan},
                               .limit_count = 2,
                               .limit_offset = 0}));
  const GroupId outer = memo.EnsureDerivedGroup({"a"}, "outer-limit-empty");
  ASSERT_TRUE(memo.AddExpression(
      outer, LogicalExpression{.operation = LogicalOperator::kLimit,
                               .children = {inner},
                               .limit_count = 0,
                               .limit_offset = 2}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(outer);

  EXPECT_TRUE(std::ranges::any_of(
      search.GetMemo().Get(outer).expressions,
      [scan](const LogicalExpression& expression) {
        return expression.operation == LogicalOperator::kEmpty &&
               expression.children == std::vector<GroupId>{scan};
      }));
}

TEST(CascadesTest, MergeLimitsKeepsTwoUnboundedLimitsUnbounded) {
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId inner = memo.EnsureDerivedGroup({"a"}, "inner-unbounded");
  ASSERT_TRUE(memo.AddExpression(
      inner, LogicalExpression{.operation = LogicalOperator::kLimit,
                               .children = {scan},
                               .limit_count = 0,
                               .limit_offset = 1}));
  const GroupId outer = memo.EnsureDerivedGroup({"a"}, "outer-unbounded");
  ASSERT_TRUE(memo.AddExpression(
      outer, LogicalExpression{.operation = LogicalOperator::kLimit,
                               .children = {inner},
                               .limit_count = 0,
                               .limit_offset = 2}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(outer);

  EXPECT_TRUE(std::ranges::any_of(
      search.GetMemo().Get(outer).expressions,
      [scan](const LogicalExpression& expression) {
        return expression.operation == LogicalOperator::kLimit &&
               expression.children == std::vector<GroupId>{scan} &&
               expression.limit_count == 0 && expression.limit_offset == 3;
      }));
  EXPECT_FALSE(std::ranges::any_of(search.GetMemo().Get(outer).expressions,
                                   [](const LogicalExpression& expression) {
                                     return expression.operation ==
                                            LogicalOperator::kEmpty;
                                   }));
}

TEST(CascadesTest, MergeLimitsSkipsOffsetOverflow) {
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId inner = memo.EnsureDerivedGroup({"a"}, "inner-overflow");
  ASSERT_TRUE(memo.AddExpression(
      inner,
      LogicalExpression{.operation = LogicalOperator::kLimit,
                        .children = {scan},
                        .limit_count = 7,
                        .limit_offset = std::numeric_limits<size_t>::max()}));
  const GroupId outer = memo.EnsureDerivedGroup({"a"}, "outer-overflow");
  ASSERT_TRUE(memo.AddExpression(
      outer, LogicalExpression{.operation = LogicalOperator::kLimit,
                               .children = {inner},
                               .limit_count = 3,
                               .limit_offset = 1}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(outer);

  EXPECT_FALSE(std::ranges::any_of(
      search.GetMemo().Get(outer).expressions,
      [scan](const LogicalExpression& expression) {
        return expression.operation == LogicalOperator::kLimit &&
               expression.children == std::vector<GroupId>{scan};
      }));
}

TEST(CascadesTest, EliminateTrueSelectionCopiesChildScan) {
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId selection = memo.EnsureDerivedGroup({"a"}, "true-sel");
  LogicalExpression tautology;
  tautology.operation = LogicalOperator::kSelection;
  tautology.children = {scan};
  tautology.predicate = ConstantValueExp(Value(1));
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
  EXPECT_TRUE(rules.Contains("push_semi_join_through_inner_join"));
  EXPECT_TRUE(rules.Contains("push_limit_through_left_join"));
  EXPECT_TRUE(rules.Contains("merge_adjacent_windows"));
  EXPECT_TRUE(rules.Contains("push_not_through_expression"));
}

TEST(CascadesTest, FalseSelectionAddsEmptyLogicalAlternative) {
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId selection = memo.EnsureDerivedGroup({"a"}, "selection");
  LogicalExpression expression;
  expression.operation = LogicalOperator::kSelection;
  expression.children = {scan};
  expression.predicate = ConstantValueExp(Value(0));
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
  memo.AddExpression(sort_group,
                     LogicalExpression{.operation = LogicalOperator::kSort,
                                       .children = {scan},
                                       .target_list = {NamedExpression(
                                           "x", ColumnValueExp("t.x"))},
                                       .sort_ascending = {true}});

  const GroupId agg_group = memo.EnsureDerivedGroup({"t"}, "agg");
  memo.AddExpression(
      agg_group,
      LogicalExpression{.operation = LogicalOperator::kAggregation,
                        .children = {sort_group},
                        .target_list = {NamedExpression(
                            "cnt", ConstantValueExp(Value(int64_t{1})))}});

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

TEST(CascadesTest, EliminateSortUnderAggregationKeepsGroupingSets) {
  // The lowered aggregation must carry the original payload: dropping
  // grouping_sets would turn a GROUP BY aggregate into a scalar one.
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId sort_group = memo.EnsureDerivedGroup({"t"}, "sort_grouped");
  memo.AddExpression(sort_group,
                     LogicalExpression{.operation = LogicalOperator::kSort,
                                       .children = {scan},
                                       .target_list = {NamedExpression(
                                           "x", ColumnValueExp("t.x"))},
                                       .sort_ascending = {true}});

  const GroupId agg_group =
      memo.EnsureDerivedGroup({"t"}, "agg_grouped_over_sort");
  memo.AddExpression(
      agg_group, LogicalExpression{
                     .operation = LogicalOperator::kAggregation,
                     .children = {sort_group},
                     .target_list = {NamedExpression(
                         "sum_v", AggregateExpressionExp(AggregationType::kSum,
                                                         ColumnValueExp("t.v"),
                                                         /*distinct=*/false))},
                     .grouping_sets = {ColumnValueExp("t.k")}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg_group);

  bool found_lowered_with_grouping = false;
  for (const auto& expr : search.GetMemo().Get(agg_group).expressions) {
    if (expr.operation == LogicalOperator::kAggregation &&
        expr.children.size() == 1 && expr.children[0] == scan) {
      EXPECT_EQ(expr.grouping_sets.size(), 1U)
          << "lowered aggregation lost its GROUP BY payload";
      found_lowered_with_grouping = true;
    }
  }
  EXPECT_TRUE(found_lowered_with_grouping);
}

TEST(CascadesTest, EliminateSortSkipsOrderSensitiveAggregate) {
  // STRING_AGG without WITHIN GROUP observes the input order produced by
  // the Sort below; removing the Sort could reorder the concatenation.
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId sort_group = memo.EnsureDerivedGroup({"t"}, "sort_string_agg");
  memo.AddExpression(sort_group,
                     LogicalExpression{.operation = LogicalOperator::kSort,
                                       .children = {scan},
                                       .target_list = {NamedExpression(
                                           "x", ColumnValueExp("t.x"))},
                                       .sort_ascending = {true}});

  const GroupId agg_group =
      memo.EnsureDerivedGroup({"t"}, "agg_string_agg_over_sort");
  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {sort_group},
          .target_list = {NamedExpression(
              "joined", AggregateExpressionExp(AggregationType::kStringAgg,
                                               ColumnValueExp("t.v"),
                                               /*distinct=*/false))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg_group);

  for (const auto& expr : search.GetMemo().Get(agg_group).expressions) {
    ASSERT_FALSE(expr.operation == LogicalOperator::kAggregation &&
                 expr.children.size() == 1 && expr.children[0] == scan)
        << "order-sensitive aggregate lost its ordering input";
  }
}

TEST(CascadesTest, DistinctOverDistinctElimination) {
  using namespace dsl;
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId inner_distinct = memo.EnsureDerivedGroup({"t"}, "dist1");
  memo.AddExpression(inner_distinct,
                     LogicalExpression{.operation = LogicalOperator::kDistinct,
                                       .children = {scan}});

  const GroupId outer_distinct = memo.EnsureDerivedGroup({"t"}, "dist2");
  memo.AddExpression(outer_distinct,
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
  memo.AddExpression(cross,
                     LogicalExpression{.operation = LogicalOperator::kCrossJoin,
                                       .children = {left, right}});

  const GroupId sel = memo.EnsureDerivedGroup({"t1", "t2"}, "sel");
  const Expression pred =
      BinaryExpressionExp(ColumnValueExp("t1.id"), BinaryOperation::kEquals,
                          ColumnValueExp("t2.id"));
  memo.AddExpression(sel,
                     LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {cross},
                                       .predicate = pred});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel);

  const auto& exprs = search.GetMemo().Get(sel).expressions;
  bool found_inner = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kJoin && expr.children.size() == 2 &&
        expr.predicate.has_value()) {
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
  memo.AddExpression(empty_group,
                     LogicalExpression{.operation = LogicalOperator::kEmpty,
                                       .children = {left}});

  const GroupId join_group = memo.EnsureDerivedGroup({"t1", "t2"}, "join");
  memo.AddExpression(join_group,
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
      LogicalExpression{.operation = LogicalOperator::kIntersect,
                        .children = {left, right},
                        .output_schema = Schema(
                            "t1", {Column("id", ValueType::kInt64),
                                   Column("name", ValueType::kVarChar)})});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(intersect_group);

  // INTERSECT is DISTINCT: the semijoin must surface through a logical
  // Distinct wrapper so left-side multiplicity is collapsed.
  const auto& exprs = search.GetMemo().Get(intersect_group).expressions;
  bool found_distinct_semijoin = false;
  for (const auto& expr : exprs) {
    if (expr.operation != LogicalOperator::kDistinct ||
        expr.children.size() != 1) {
      continue;
    }
    const Group& join_holder = search.GetMemo().Get(expr.children[0]);
    for (const auto& join_expr : join_holder.expressions) {
      if (join_expr.operation == LogicalOperator::kSemiJoin &&
          join_expr.children.size() == 2 && join_expr.children[0] == left &&
          join_expr.children[1] == right && join_expr.predicate.has_value()) {
        found_distinct_semijoin = true;
        break;
      }
    }
    if (found_distinct_semijoin) {
      break;
    }
  }
  EXPECT_TRUE(found_distinct_semijoin);
}

TEST(CascadesTest, ExceptToAntiJoinRewrite) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId except_group = memo.EnsureDerivedGroup({"t1", "t2"}, "except");
  memo.AddExpression(
      except_group,
      LogicalExpression{.operation = LogicalOperator::kExcept,
                        .children = {left, right},
                        .output_schema = Schema(
                            "t1", {Column("id", ValueType::kInt64),
                                   Column("name", ValueType::kVarChar)})});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(except_group);

  // EXCEPT is DISTINCT: the antijoin must surface through a logical
  // Distinct wrapper.
  const auto& exprs = search.GetMemo().Get(except_group).expressions;
  bool found_distinct_antijoin = false;
  for (const auto& expr : exprs) {
    if (expr.operation != LogicalOperator::kDistinct ||
        expr.children.size() != 1) {
      continue;
    }
    const Group& join_holder = search.GetMemo().Get(expr.children[0]);
    for (const auto& join_expr : join_holder.expressions) {
      if (join_expr.operation == LogicalOperator::kAntiJoin &&
          join_expr.children.size() == 2 && join_expr.children[0] == left &&
          join_expr.children[1] == right && join_expr.predicate.has_value()) {
        found_distinct_antijoin = true;
        break;
      }
    }
    if (found_distinct_antijoin) {
      break;
    }
  }
  EXPECT_TRUE(found_distinct_antijoin);
}

TEST(CascadesTest, SetopEmptySimplificationRequiresEmptyLeftForExcept) {
  // A EXCEPT empty(B) is just A: an Empty alternative must NOT appear, or
  // costing (Empty costs 0) would collapse a non-empty result.
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId except_group = memo.EnsureDerivedGroup({"t1", "t2"}, "except");
  memo.AddExpression(
      except_group,
      LogicalExpression{
          .operation = LogicalOperator::kExcept,
          .children = {left, right},
          .output_schema = Schema("t1", {Column("id", ValueType::kInt64)})});
  const GroupId empty_base_right = memo.EnsureDerivedGroup({"t2"}, "empty_r");
  memo.AddExpression(right,
                     LogicalExpression{.operation = LogicalOperator::kEmpty,
                                       .children = {empty_base_right}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(except_group);

  for (const auto& expr : search.GetMemo().Get(except_group).expressions) {
    ASSERT_FALSE(expr.operation == LogicalOperator::kEmpty);
  }
}

TEST(CascadesTest, SetopEmptySimplificationCollapsesEmptyLeftExcept) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId except_group = memo.EnsureDerivedGroup({"t1", "t2"}, "except");
  memo.AddExpression(
      except_group,
      LogicalExpression{
          .operation = LogicalOperator::kExcept,
          .children = {left, right},
          .output_schema = Schema("t1", {Column("id", ValueType::kInt64)})});
  const GroupId empty_base_left = memo.EnsureDerivedGroup({"t1"}, "empty_l");
  memo.AddExpression(left,
                     LogicalExpression{.operation = LogicalOperator::kEmpty,
                                       .children = {empty_base_left}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(except_group);

  bool found_empty = false;
  for (const auto& expr : search.GetMemo().Get(except_group).expressions) {
    if (expr.operation == LogicalOperator::kEmpty) {
      found_empty = true;
      break;
    }
  }
  EXPECT_TRUE(found_empty);
}

TEST(CascadesTest, DistinctAndGroupByInterchangeBothDirections) {
  // Direction 1: Distinct(Projection(X)) -> Aggregation(X, GROUP BY col)
  {
    Memo memo;
    const GroupId scan = memo.Build({"t1"});
    const GroupId proj_group = memo.EnsureDerivedGroup({"t1"}, "distinct_proj");
    memo.AddExpression(
        proj_group, LogicalExpression{.operation = LogicalOperator::kProjection,
                                      .children = {scan},
                                      .target_list = {NamedExpression(
                                          "id", ColumnValueExp("id"))}});

    const GroupId dist_group =
        memo.EnsureDerivedGroup({"t1"}, "distinct_over_proj");
    memo.AddExpression(
        dist_group, LogicalExpression{.operation = LogicalOperator::kDistinct,
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
    const GroupId agg_group = memo.EnsureDerivedGroup({"t2"}, "agg_no_func");
    memo.AddExpression(
        agg_group, LogicalExpression{.operation = LogicalOperator::kAggregation,
                                     .children = {scan},
                                     .target_list = {NamedExpression(
                                         "v", ColumnValueExp("v"))},
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
  // Join elimination is only sound on a proven key: publish the table
  // schemas with a PRIMARY KEY id column.
  const Schema keyed("t1", {Column("id", ValueType::kInt64,
                                   Constraint(Constraint::kPrimaryKey))});
  memo.SetTableSchemas({{"t1_1", keyed}, {"t1_2", keyed}});
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
      LogicalExpression{.operation = LogicalOperator::kJoin,
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

TEST(CascadesTest, SelfJoinEliminationRequiresProvenUniqueKey) {
  // Without a UNIQUE/PRIMARY KEY constraint the join can multiply rows;
  // eliminating it would change results, so the rule must refuse.
  Memo memo;
  (void)memo.Build({"t1_1", "t1_2"});
  const Schema unkeyed("t1", {Column("id", ValueType::kInt64)});
  memo.SetTableSchemas({{"t1_1", unkeyed}, {"t1_2", unkeyed}});
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
      LogicalExpression{.operation = LogicalOperator::kJoin,
                        .children = {left_group, right_group},
                        .predicate = BinaryExpressionExp(
                            ColumnValueExp(ColumnName("t1_1", "id")),
                            BinaryOperation::kEquals,
                            ColumnValueExp(ColumnName("t1_2", "id")))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(join_group);

  const auto& exprs = search.GetMemo().Get(join_group).expressions;
  const bool found_scan = std::ranges::any_of(exprs, [](const auto& expr) {
    return expr.operation == LogicalOperator::kScan && expr.table == "t1";
  });
  EXPECT_FALSE(found_scan);
}

TEST(CascadesTest, UniqueSemiToInnerRewrite) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  memo.SetTableSchemas(
      {{"t2", Schema("t2", {Column("id", ValueType::kInt64,
                                   Constraint(Constraint::kPrimaryKey))})}});
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
              ColumnValueExp(ColumnName("t1", "id")), BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id")))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(semi_group);

  const auto& exprs = search.GetMemo().Get(semi_group).expressions;
  bool found_inner = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kJoin && expr.children.size() == 2 &&
        expr.children[0] == left && expr.children[1] == right) {
      found_inner = true;
      break;
    }
  }
  EXPECT_TRUE(found_inner);
}

TEST(CascadesTest, OuterToAntiJoinRewrite) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  // The rewrite is only sound when the null-tested column cannot be NULL in
  // a matched row: publish t2.id as a PRIMARY KEY (implies NOT NULL).
  memo.SetTableSchemas(
      {{"t2", Schema("t2", {Column("id", ValueType::kInt64,
                                   Constraint(Constraint::kPrimaryKey))})}});
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
              ColumnValueExp(ColumnName("t1", "id")), BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id"))),
          .join_type = 0});  // 0 = LeftOuter

  const GroupId sel_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "null_filter");
  memo.AddExpression(
      sel_group, LogicalExpression{.operation = LogicalOperator::kSelection,
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

TEST(CascadesTest, OuterToAntiJoinRefusesNullableNullTestedColumn) {
  // A matched row whose t2.id is genuinely NULL satisfies `t2.id IS NULL`
  // but would be dropped by the AntiJoin; without a NOT NULL/PK constraint
  // the rewrite must refuse.
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  memo.SetTableSchemas(
      {{"t2", Schema("t2", {Column("id", ValueType::kInt64)})}});
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
              ColumnValueExp(ColumnName("t1", "id")), BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id"))),
          .join_type = 0});

  const GroupId sel_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "null_filter");
  memo.AddExpression(
      sel_group, LogicalExpression{.operation = LogicalOperator::kSelection,
                                   .children = {outer_join_group},
                                   .predicate = UnaryExpressionExp(
                                       ColumnValueExp(ColumnName("t2", "id")),
                                       UnaryOperation::kIsNull)});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  const bool found_anti2 =
      std::ranges::any_of(exprs, [](const LogicalExpression& expr) {
        return expr.operation == LogicalOperator::kAntiJoin;
      });
  EXPECT_FALSE(found_anti2);
}

TEST(CascadesTest, OuterToAntiJoinRefusesLeftResidualConjunct) {
  // `WHERE t2.id IS NULL AND t1.x = 5`: the AntiJoin alternative keeps only
  // the join condition; converting would drop `t1.x = 5` and return rows
  // that violate it.
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  memo.SetTableSchemas(
      {{"t2", Schema("t2", {Column("id", ValueType::kInt64,
                                   Constraint(Constraint::kPrimaryKey))})}});
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
              ColumnValueExp(ColumnName("t1", "id")), BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id"))),
          .join_type = 0});

  Expression residual = BinaryExpressionExp(
      UnaryExpressionExp(ColumnValueExp(ColumnName("t2", "id")),
                         UnaryOperation::kIsNull),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "x")),
                          BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{5}))));
  const GroupId sel_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "null_filter");
  memo.AddExpression(sel_group,
                     LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {outer_join_group},
                                       .predicate = residual});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  const bool found_anti3 =
      std::ranges::any_of(exprs, [](const LogicalExpression& expr) {
        return expr.operation == LogicalOperator::kAntiJoin;
      });
  EXPECT_FALSE(found_anti3);
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
              ColumnValueExp(ColumnName("t1", "id")), BinaryOperation::kEquals,
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
  memo.SetTableSchemas(
      {{"t1", Schema("t1", {Column("id", ValueType::kInt64)})},
       {"t2", Schema("t2", {Column("id", ValueType::kInt64)})}});
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
              ColumnValueExp(ColumnName("t1", "id")), BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id"))),
          .join_type = 2});  // 2 = FullOuter

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(full_outer_group);

  const auto& exprs = search.GetMemo().Get(full_outer_group).expressions;
  bool found_union_all = false;
  bool found_padded_projection = false;
  for (const auto& expr : exprs) {
    if (expr.operation != LogicalOperator::kUnionAll ||
        expr.children.size() != 2) {
      continue;
    }
    found_union_all = true;
    const Group& padded = search.GetMemo().Get(expr.children[1]);
    for (const LogicalExpression& padded_expr : padded.expressions) {
      if (padded_expr.operation == LogicalOperator::kProjection &&
          padded_expr.output_schema.ColumnCount() == 2) {
        found_padded_projection = true;
      }
    }
  }
  EXPECT_TRUE(found_union_all);
  EXPECT_TRUE(found_padded_projection);
}

TEST(CascadesTest, PushDownLimitThroughJoin) {
  // D5 gate: push_down_limit_through_join is DISABLED because the Memo
  // does not carry uniqueness constraints. Without proving the join key is
  // unique on the pushed side, this rule can return fewer rows.
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
              ColumnValueExp(ColumnName("t1", "id")), BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id")))});

  const GroupId limit_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "limit_join");
  memo.AddExpression(limit_group,
                     LogicalExpression{.operation = LogicalOperator::kLimit,
                                       .children = {join_group},
                                       .limit_count = 10,
                                       .limit_offset = 5});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(limit_group);

  // The rule is disabled: no Limit should be pushed into the join's left.
  const auto& exprs = search.GetMemo().Get(limit_group).expressions;
  bool found_limit_pushed = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kJoin && expr.children.size() == 2) {
      const Group& child_group = search.GetMemo().Get(expr.children[0]);
      for (const auto& child_expr : child_group.expressions) {
        if (child_expr.operation == LogicalOperator::kLimit) {
          found_limit_pushed = true;
          break;
        }
      }
    }
  }
  EXPECT_FALSE(found_limit_pushed);
}

TEST(CascadesTest, NoOpWindowElimination) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId win_group = memo.EnsureDerivedGroup({"t1"}, "noop_window");
  memo.AddExpression(
      win_group,
      LogicalExpression{.operation = LogicalOperator::kWindow,
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

namespace {
std::shared_ptr<WindowFunctionCallExpression> PrefixWindowCall(
    std::string function, std::vector<Expression> args,
    std::vector<Expression> partition_by = {},
    std::vector<WindowOrderTerm> order_by = {}, bool has_frame = false) {
  auto call = std::make_shared<WindowFunctionCallExpression>();
  call->function = std::move(function);
  call->args = std::move(args);
  call->partition_by = std::move(partition_by);
  call->order_by = std::move(order_by);
  call->has_frame = has_frame;
  if (has_frame) {
    call->frame_unit = WindowFrameUnit::kRows;
    call->frame_start.type = WindowFrameBoundType::kUnboundedPreceding;
    call->frame_end.type = WindowFrameBoundType::kCurrentRow;
  }
  return call;
}

// True when some kWindow node in the memo has a child group holding a
// Limit/TopN over the window's input (the limit-prefix shape).  The
// negative tests assert this stays absent.
bool AnyWindowHasLimitedChild(const Memo& memo) {
  for (size_t id = 0; id < memo.GroupCount(); ++id) {
    for (const LogicalExpression& window : memo.Get(GroupId(id)).expressions) {
      if (window.operation != LogicalOperator::kWindow ||
          window.children.size() != 1) {
        continue;
      }
      for (const LogicalExpression& child :
           memo.Get(window.children[0]).expressions) {
        if (child.operation == LogicalOperator::kLimit ||
            child.operation == LogicalOperator::kTopN) {
          return true;
        }
      }
    }
  }
  return false;
}

bool GroupHasLimitWithDifferentChild(const Memo& memo, GroupId limit_group,
                                     GroupId original_child) {
  for (const LogicalExpression& limit : memo.Get(limit_group).expressions) {
    if (limit.operation != LogicalOperator::kLimit ||
        limit.children.size() != 1 || limit.children[0] == original_child) {
      continue;
    }
    for (const LogicalExpression& window :
         memo.Get(limit.children[0]).expressions) {
      if (window.operation == LogicalOperator::kWindow &&
          window.children.size() == 1) {
        return true;
      }
    }
  }
  return false;
}
}  // namespace

TEST(CascadesTest, LimitPrefixThroughUnorderedWindowFires) {
  Memo memo;
  // A values leaf stands in for the base relation: window-row estimates then
  // dominate the cost comparison without a catalog-backed scan. Build only
  // registers the relation in the join graph.
  (void)memo.Build({"t1"});
  std::vector<Row> wide_rows;
  for (int64_t i = 1; i <= 1000; ++i) {
    wide_rows.push_back(Row({Value(i), Value(int64_t{i % 5})}));
  }
  const GroupId scan = memo.EnsureGroup({"t1"});
  memo.AddExpression(
      scan, LogicalExpression{.operation = LogicalOperator::kValues,
                              .values = std::move(wide_rows),
                              .output_schema = Schema(
                                  "t1", {Column("part", ValueType::kInt64),
                                         Column("val", ValueType::kInt64)})});
  const GroupId win_group = memo.EnsureDerivedGroup({"t1"}, "win_node");
  memo.AddExpression(
      win_group,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {scan},
          .target_list = {NamedExpression(
              "rn", Expression(PrefixWindowCall("ROW_NUMBER", {},
                                                {ColumnValueExp("t1.part")})))},
          .output_schema = Schema("t1", {Column("part", ValueType::kInt64),
                                         Column("val", ValueType::kInt64)}),
          .partition_by = {ColumnValueExp("t1.part")}});
  const GroupId limit_group = memo.EnsureDerivedGroup({"t1"}, "limit_over_win");
  memo.AddExpression(limit_group,
                     LogicalExpression{.operation = LogicalOperator::kLimit,
                                       .children = {win_group},
                                       .limit_count = 3,
                                       .limit_offset = 2});

  RuleSet rules;
  for (const auto& rule : RuleSet::Default().Rules()) {
    if (rule.Name() == "limit_prefix_unordered_window") {
      rules.Add(rule);
    }
  }
  SearchEngine search(std::move(memo), rules);
  search.Explore(limit_group);

  const auto& all = search.GetMemo();
  EXPECT_TRUE(GroupHasLimitWithDifferentChild(all, limit_group, win_group));
  EXPECT_TRUE(AnyWindowHasLimitedChild(all));
  // The inner cut under the window is a prefix: it must keep offset+count
  // rows from the start (OFFSET applied once, by the outer Limit). Skipping
  // the OFFSET twice would drop rows and renumber the survivors.
  bool saw_prefix_cut = false;
  for (size_t g = 0; g < all.GroupCount(); ++g) {
    for (const LogicalExpression& expr : all.Get(GroupId(g)).expressions) {
      if (expr.operation == LogicalOperator::kLimit &&
          !expr.children.empty() && expr.children[0] == scan) {
        EXPECT_EQ(expr.limit_count, 5U);
        EXPECT_EQ(expr.limit_offset, 0U);
        saw_prefix_cut = true;
      }
    }
  }
  EXPECT_TRUE(saw_prefix_cut);

  // And costing must prefer the transformed shape once the base relation is
  // non-trivial: the window only materializes the kept prefix. The rendered
  // tree nests the window BETWEEN two limits; the untransformed shape keeps
  // the window above the single limit.
  const RuleSet empty_rules;
  SearchEngine costing(all, empty_rules);
  const std::optional<BestPlan> best = costing.Optimize(
      limit_group, PhysicalProperties{}, DefaultImplementationRules());
  ASSERT_TRUE(best.has_value());
  if (!best.has_value()) {
    return;
  }
  ASSERT_TRUE(best->plan);
  std::ostringstream rendered;
  rendered << best->plan;
  const std::string tree = rendered.str();
  const size_t window_at = tree.find("Window");
  ASSERT_NE(window_at, std::string::npos);
  EXPECT_NE(tree.find("Limit:", window_at + 1), std::string::npos);
}

TEST(CascadesTest, LimitPrefixThroughOrderedWindowFusesTopN) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId sort_group = memo.EnsureDerivedGroup({"t1"}, "sort_below");
  memo.AddExpression(
      sort_group,
      LogicalExpression{.operation = LogicalOperator::kSort,
                        .children = {scan},
                        .target_list = {NamedExpression(
                            "", ColumnValueExp(ColumnName("t1", "id")))},
                        .sort_ascending = {true}});
  const GroupId win_group = memo.EnsureDerivedGroup({"t1"}, "win_node");
  memo.AddExpression(
      win_group,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {sort_group},
          .target_list = {NamedExpression(
              "run", Expression(PrefixWindowCall(
                         "SUM", {ColumnValueExp("t1.val")}, {},
                         {{.expression = ColumnValueExp(ColumnName("t1", "id")),
                           .ascending = true}},
                         true)))},
          .output_schema = Schema("t1", {Column("id", ValueType::kInt64),
                                         Column("val", ValueType::kInt64)})});
  const GroupId limit_group = memo.EnsureDerivedGroup({"t1"}, "limit_over_win");
  memo.AddExpression(limit_group,
                     LogicalExpression{.operation = LogicalOperator::kLimit,
                                       .children = {win_group},
                                       .limit_count = 3});

  RuleSet rules;
  for (const auto& rule : RuleSet::Default().Rules()) {
    if (rule.Name() == "limit_prefix_ordered_window") {
      rules.Add(rule);
    }
  }
  SearchEngine search(std::move(memo), rules);
  search.Explore(limit_group);

  ASSERT_TRUE(AnyWindowHasLimitedChild(search.GetMemo()));
  // Sharpen it: some kWindow's child group holds the fused TopN with the
  // original limit count.
  bool found_fused_topn = false;
  const Memo& after = search.GetMemo();
  for (size_t id = 0; id < after.GroupCount(); ++id) {
    for (const LogicalExpression& window : after.Get(GroupId(id)).expressions) {
      if (window.operation != LogicalOperator::kWindow ||
          window.children.size() != 1) {
        continue;
      }
      for (const LogicalExpression& limited :
           after.Get(window.children[0]).expressions) {
        if (limited.operation == LogicalOperator::kTopN &&
            limited.limit_count == 3) {
          found_fused_topn = true;
        }
      }
    }
  }
  EXPECT_TRUE(found_fused_topn);
}

TEST(CascadesTest, DefaultFrameWindowKeepsLimitAbove) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId win_group = memo.EnsureDerivedGroup({"t1"}, "win_node");
  memo.AddExpression(
      win_group,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {scan},
          .target_list = {NamedExpression(
              "wide",
              Expression(PrefixWindowCall("SUM", {ColumnValueExp("t1.val")},
                                          {ColumnValueExp("t1.part")})))},
          .output_schema = Schema("t1", {Column("part", ValueType::kInt64),
                                         Column("val", ValueType::kInt64)}),
          .partition_by = {ColumnValueExp("t1.part")}});
  const GroupId limit_group = memo.EnsureDerivedGroup({"t1"}, "limit_over_win");
  memo.AddExpression(limit_group,
                     LogicalExpression{.operation = LogicalOperator::kLimit,
                                       .children = {win_group},
                                       .limit_count = 3});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(limit_group);

  // The default frame spans the whole partition, so truncating the input
  // would change every value: the exception must not fire.
  EXPECT_FALSE(AnyWindowHasLimitedChild(search.GetMemo()));
}

TEST(CascadesTest, CumeDistWindowKeepsLimitAbove) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId sort_group = memo.EnsureDerivedGroup({"t1"}, "sort_below");
  memo.AddExpression(
      sort_group,
      LogicalExpression{.operation = LogicalOperator::kSort,
                        .children = {scan},
                        .target_list = {NamedExpression(
                            "", ColumnValueExp(ColumnName("t1", "id")))},
                        .sort_ascending = {true}});
  const GroupId win_group = memo.EnsureDerivedGroup({"t1"}, "win_node");
  memo.AddExpression(
      win_group,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {sort_group},
          .target_list = {NamedExpression(
              "cd", Expression(PrefixWindowCall(
                        "CUME_DIST", {}, {},
                        {{.expression = ColumnValueExp(ColumnName("t1", "id")),
                          .ascending = true}})))},
          .output_schema = Schema("t1", {Column("id", ValueType::kInt64)})});
  const GroupId limit_group = memo.EnsureDerivedGroup({"t1"}, "limit_over_win");
  memo.AddExpression(limit_group,
                     LogicalExpression{.operation = LogicalOperator::kLimit,
                                       .children = {win_group},
                                       .limit_count = 3});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(limit_group);

  // CUME_DIST divides by the partition size, so truncating the input
  // changes the surviving values even though the sort delivers the spec.
  EXPECT_FALSE(AnyWindowHasLimitedChild(search.GetMemo()));
}

TEST(CascadesTest, AuditFrameIgnoringWindowsKeepLimitAbove) {
  for (const std::string function :
       {"CUME_DIST", "PERCENT_RANK", "NTILE", "LEAD", "LAG", "PERCENTILE_CONT",
        "PERCENTILE_DISC"}) {
    SCOPED_TRACE(function);
    Memo memo;
    (void)memo.Build({"t1"});
    const GroupId scan = memo.EnsureGroup({"t1"});
    const GroupId sort_group = memo.EnsureDerivedGroup({"t1"}, "sort_below");
    memo.AddExpression(
        sort_group,
        LogicalExpression{.operation = LogicalOperator::kSort,
                          .children = {scan},
                          .target_list = {NamedExpression(
                              "", ColumnValueExp(ColumnName("t1", "id")))},
                          .sort_ascending = {true}});
    const GroupId win_group = memo.EnsureDerivedGroup({"t1"}, "win_node");
    memo.AddExpression(
        win_group,
        LogicalExpression{
            .operation = LogicalOperator::kWindow,
            .children = {sort_group},
            .target_list = {NamedExpression(
                "cd",
                Expression(PrefixWindowCall(
                    function, {}, {},
                    {{.expression = ColumnValueExp(ColumnName("t1", "id")),
                      .ascending = true}},
                    true)))},
            .output_schema = Schema("t1", {Column("id", ValueType::kInt64)})});
    const GroupId limit_group =
        memo.EnsureDerivedGroup({"t1"}, "limit_over_win");
    memo.AddExpression(limit_group,
                       LogicalExpression{.operation = LogicalOperator::kLimit,
                                         .children = {win_group},
                                         .limit_count = 3});

    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(limit_group);

    // CUME_DIST divides by the partition size, so truncating the input
    // changes the surviving values even though the sort delivers the spec.
    EXPECT_FALSE(AnyWindowHasLimitedChild(search.GetMemo()));
  }
}

TEST(CascadesTest, OrderedWindowRejectsMismatchedSortKeys) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId sort_group = memo.EnsureDerivedGroup({"t1"}, "sort_other");
  memo.AddExpression(
      sort_group,
      LogicalExpression{.operation = LogicalOperator::kSort,
                        .children = {scan},
                        .target_list = {NamedExpression(
                            "", ColumnValueExp(ColumnName("t1", "val")))},
                        .sort_ascending = {true}});
  const GroupId win_group = memo.EnsureDerivedGroup({"t1"}, "win_node");
  memo.AddExpression(
      win_group,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {sort_group},
          .target_list = {NamedExpression(
              "rn", Expression(PrefixWindowCall(
                        "ROW_NUMBER", {}, {},
                        {{.expression = ColumnValueExp(ColumnName("t1", "id")),
                          .ascending = true}})))},
          .output_schema = Schema("t1", {Column("id", ValueType::kInt64),
                                         Column("val", ValueType::kInt64)})});
  const GroupId limit_group = memo.EnsureDerivedGroup({"t1"}, "limit_over_win");
  memo.AddExpression(limit_group,
                     LogicalExpression{.operation = LogicalOperator::kLimit,
                                       .children = {win_group},
                                       .limit_count = 3});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(limit_group);

  // The sort below the window does not deliver the window's order; the
  // truncated stream would not be a partition-order prefix.
  EXPECT_FALSE(AnyWindowHasLimitedChild(search.GetMemo()));
}

TEST(CascadesTest, NestedWindowLayerRefusesLimitPrefix) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId inner_group = memo.EnsureDerivedGroup({"t1"}, "win_inner");
  memo.AddExpression(
      inner_group,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {scan},
          .target_list = {NamedExpression(
              "wide",
              Expression(PrefixWindowCall("SUM", {ColumnValueExp("t1.val")},
                                          {ColumnValueExp("t1.part")})))},
          .output_schema = Schema("t1", {Column("part", ValueType::kInt64),
                                         Column("val", ValueType::kInt64)}),
          .partition_by = {ColumnValueExp("t1.part")}});
  const GroupId outer_group = memo.EnsureDerivedGroup({"t1"}, "win_outer");
  memo.AddExpression(
      outer_group,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {inner_group},
          .target_list = {NamedExpression(
              "rn", Expression(PrefixWindowCall("ROW_NUMBER", {})))},
          .output_schema = Schema("t1", {Column("part", ValueType::kInt64),
                                         Column("val", ValueType::kInt64)})});
  const GroupId limit_group = memo.EnsureDerivedGroup({"t1"}, "limit_over_win");
  memo.AddExpression(limit_group,
                     LogicalExpression{.operation = LogicalOperator::kLimit,
                                       .children = {outer_group},
                                       .limit_count = 3});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(limit_group);

  // The outer call is prefix-stable, but truncating between the layers
  // would also cut the inner whole-partition window; refusing is the safe
  // stance until the gate learns to check the layers recursively.
  EXPECT_FALSE(AnyWindowHasLimitedChild(search.GetMemo()));
}

TEST(CascadesTest, UnorderedRuleRefusesOrderedWindow) {
  // Rule A (the Sort-less route) only covers order-free windows: with order
  // keys, emit order disagrees with partition order and a limit under the
  // window would shift the surviving rows' ranks (M3 guard).
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId win_group = memo.EnsureDerivedGroup({"t1"}, "win_node");
  memo.AddExpression(
      win_group,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {scan},
          .target_list = {NamedExpression(
              "rn", Expression(PrefixWindowCall(
                        "ROW_NUMBER", {},
                        /*partition_by=*/{},
                        /*order_by=*/
                        {{.expression = ColumnValueExp(ColumnName("t1", "v")),
                          .ascending = true}})))},
          .output_schema = Schema("t1", {Column("v", ValueType::kInt64)})});
  const GroupId limit_group = memo.EnsureDerivedGroup({"t1"}, "limit_over_win");
  memo.AddExpression(limit_group,
                     LogicalExpression{.operation = LogicalOperator::kLimit,
                                       .children = {win_group},
                                       .limit_count = 2});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(limit_group);
  EXPECT_FALSE(AnyWindowHasLimitedChild(search.GetMemo()));
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
              NamedExpression("c2",
                              ColumnValueExp(ColumnName("t1", "score")))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(proj_group);

  const auto& exprs = search.GetMemo().Get(proj_group).expressions;
  bool found_reduced_arity = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kProjection &&
        expr.target_list.size() != 3) {
      found_reduced_arity = true;
      break;
    }
  }
  EXPECT_FALSE(found_reduced_arity);
}

TEST(CascadesTest, MergeProjectionsPropagatesConstants) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId inner_proj = memo.EnsureDerivedGroup({"t1"}, "const_proj");
  memo.AddExpression(
      inner_proj,
      LogicalExpression{.operation = LogicalOperator::kProjection,
                        .children = {scan},
                        .target_list = {NamedExpression(
                            "c_const", ConstantValueExp(Value(int64_t{42})))}});

  const GroupId outer_proj = memo.EnsureDerivedGroup({"t1"}, "outer_proj");
  memo.AddExpression(
      outer_proj, LogicalExpression{
                      .operation = LogicalOperator::kProjection,
                      .children = {inner_proj},
                      .target_list = {NamedExpression(
                          "out", BinaryExpressionExp(
                                     ColumnValueExp(ColumnName("", "c_const")),
                                     BinaryOperation::kAdd,
                                     ConstantValueExp(Value(int64_t{1}))))}});

  RuleSet rules;
  for (const auto& rule : RuleSet::Default().Rules()) {
    if (rule.Name() == "merge_projections") {
      rules.Add(rule);
    }
  }
  SearchEngine search(std::move(memo), rules);
  search.Explore(outer_proj);

  const auto& exprs = search.GetMemo().Get(outer_proj).expressions;
  bool found_prop = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kProjection &&
        !expr.target_list.empty() &&
        expr.children == std::vector<GroupId>{scan}) {
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
              ColumnValueExp(ColumnName("t1", "id")), BinaryOperation::kEquals,
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
              NamedExpression("c2", ColumnValueExp(ColumnName("t2", "id")))}});

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
          if (found_width_control) {
            break;
          }
        }
      }
      if (found_width_control) {
        break;
      }
    }
  }
  EXPECT_TRUE(found_width_control);
}

TEST(CascadesTest, MergeProjectionsCompose) {
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
              "c", BinaryExpressionExp(ColumnValueExp(ColumnName("", "b")),
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

TEST(CascadesTest, DynamicFilterPushdownJoin) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
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
    if (expr.operation == LogicalOperator::kJoin && expr.children.size() == 2) {
      const auto& probe_tag = search.GetMemo().Get(expr.children[1]).tag;
      if (probe_tag.find("bloom_probe") != std::string::npos) {
        found_bloom = true;
        break;
      }
    }
  }
  EXPECT_TRUE(found_bloom);
}

// The logical half of the dynamic filter contract: the probe-side group records
// WHICH columns carry the join key, and nothing else. The filter shape (exact
// set / min-max / Bloom) is an executor decision, so no filter information may
// leak into the group.
TEST(CascadesTest, DynamicFilterPushdownJoinAnnotatesTheProbeKeys) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
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

  // The unannotated groups must stay unannotated: the annotation belongs to the
  // derived probe group only, or every join would look filterable.
  EXPECT_TRUE(search.GetMemo().Get(g1).dynamic_filter_keys.empty());
  EXPECT_TRUE(search.GetMemo().Get(g2).dynamic_filter_keys.empty());

  size_t annotated = 0;
  for (const auto& expr : search.GetMemo().Get(join_group).expressions) {
    if (expr.operation != LogicalOperator::kJoin || expr.children.size() != 2) {
      continue;
    }
    const Group& probe = search.GetMemo().Get(expr.children[1]);
    if (probe.tag.find("bloom_probe") == std::string::npos) {
      continue;
    }
    ++annotated;
    ASSERT_EQ(probe.dynamic_filter_keys.size(), 1U) << probe.tag;
    // The annotated key always belongs to the annotated group itself: it is the
    // column the build side's filter would be tested against.
    ASSERT_EQ(probe.relations.size(), 1U) << probe.tag;
    EXPECT_EQ(probe.dynamic_filter_keys.front().schema,
              probe.relations.front());
    EXPECT_EQ(probe.dynamic_filter_keys.front().name, "id");
    // The derived group keeps the probe side's selection (D5), so the
    // alternative stays predicate-equivalent to the original join.
    EXPECT_FALSE(probe.filter);
  }
  // Join commutativity supplies the mirrored expression, so both orientations
  // are annotated and the implementation rules can pick either build side.
  EXPECT_GE(annotated, 2U);
}

// A non-equality join key cannot be expressed as a membership filter, and a
// multi-relation probe group has no single scan to push into: neither is
// annotated.
TEST(CascadesTest, DynamicFilterPushdownJoinSkipsNonEqualityJoins) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId g1 = memo.EnsureGroup({"t1"});
  const GroupId g2 = memo.EnsureGroup({"t2"});
  const GroupId range_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "range_join");
  // A band predicate has no membership domain, so a dynamic filter over the
  // build keys could not be sound.
  memo.AddExpression(
      range_group,
      LogicalExpression{.operation = LogicalOperator::kJoin,
                        .children = {g1, g2},
                        .predicate = BinaryExpressionExp(
                            ColumnValueExp(ColumnName("t1", "id")),
                            BinaryOperation::kLessThan,
                            ColumnValueExp(ColumnName("t2", "id")))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(range_group);
  for (const auto& expr : search.GetMemo().Get(range_group).expressions) {
    if (expr.operation != LogicalOperator::kJoin || expr.children.size() != 2) {
      continue;
    }
    const Group& probe = search.GetMemo().Get(expr.children[1]);
    EXPECT_TRUE(probe.dynamic_filter_keys.empty()) << probe.tag;
  }
  EXPECT_TRUE(search.GetMemo().Get(g1).dynamic_filter_keys.empty());
  EXPECT_TRUE(search.GetMemo().Get(g2).dynamic_filter_keys.empty());
}

// A multi-relation probe group is left alone: the filter would have to be
// pushed through a join, which this rule does not model.
TEST(CascadesTest, DynamicFilterPushdownJoinSkipsMultiRelationProbe) {
  Memo memo;
  (void)memo.Build({"t1", "t2", "t3"});
  const GroupId g1 = memo.EnsureGroup({"t1"});
  const GroupId g2 = memo.EnsureGroup({"t2"});
  const GroupId g34 = memo.EnsureGroup({"t3"});
  const GroupId g23 = memo.EnsureGroup({"t2", "t3"});
  const GroupId root = memo.EnsureDerivedGroup({"t1", "t2", "t3"}, "root");
  memo.AddExpression(
      g23, LogicalExpression{.operation = LogicalOperator::kJoin,
                             .children = {g2, g34},
                             .predicate = BinaryExpressionExp(
                                 ColumnValueExp(ColumnName("t2", "id")),
                                 BinaryOperation::kEquals,
                                 ColumnValueExp(ColumnName("t3", "id")))});
  // root = t1 join (t2 join t3) on two equalities: the probe side is the
  // two-relation group g23.
  memo.AddExpression(
      root,
      LogicalExpression{
          .operation = LogicalOperator::kJoin,
          .children = {g1, g23},
          .predicate = CombineConjuncts(
              {BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "id")),
                                   BinaryOperation::kEquals,
                                   ColumnValueExp(ColumnName("t2", "id"))),
               BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "id")),
                                   BinaryOperation::kEquals,
                                   ColumnValueExp(ColumnName("t3", "id")))})});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);
  for (const auto& expr : search.GetMemo().Get(root).expressions) {
    if (expr.operation != LogicalOperator::kJoin || expr.children.size() != 2) {
      continue;
    }
    const Group& probe = search.GetMemo().Get(expr.children[1]);
    if (probe.relations.size() == 1) {
      continue;  // commutativity-derived single-relation alternative
    }
    EXPECT_TRUE(probe.dynamic_filter_keys.empty()) << probe.tag;
  }
}

// The logical half of the hash semi reduction contract
// (`semi_reduction_pushdown_semi_join` / `..._anti_join`): the probe-side group
// records WHICH columns carry the join key whose distinct values bound the
// build side. Like the dynamic-filter annotation it says nothing about HOW the
// build is shrunk -- dedup, domain restriction and the fallback are all
// executor decisions -- and it must not leak onto the unannotated groups, or
// every semi/anti join would look reducible.
TEST(CascadesTest, SemiReductionPushdownAnnotatesTheProbeKeys) {
  for (const LogicalOperator operation :
       {LogicalOperator::kSemiJoin, LogicalOperator::kAntiJoin}) {
    Memo memo;
    (void)memo.Build({"t1", "t2"});
    const GroupId g1 = memo.EnsureGroup({"t1"});
    const GroupId g2 = memo.EnsureGroup({"t2"});
    const GroupId join_group =
        memo.EnsureDerivedGroup({"t1", "t2"}, "root_semi");
    memo.AddExpression(
        join_group,
        LogicalExpression{.operation = operation,
                          .children = {g1, g2},
                          .predicate = BinaryExpressionExp(
                              ColumnValueExp(ColumnName("t1", "id")),
                              BinaryOperation::kEquals,
                              ColumnValueExp(ColumnName("t2", "id")))});

    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(join_group);

    // The base groups stay unannotated: the annotation belongs to the derived
    // probe group only.
    EXPECT_TRUE(search.GetMemo().Get(g1).semi_reduction_probe_keys.empty());
    EXPECT_TRUE(search.GetMemo().Get(g2).semi_reduction_probe_keys.empty());

    size_t annotated = 0;
    for (const auto& expr : search.GetMemo().Get(join_group).expressions) {
      if (expr.operation != operation || expr.children.size() != 2) {
        continue;
      }
      const Group& probe = search.GetMemo().Get(expr.children[0]);
      if (probe.tag.find("semi_reduce_probe") == std::string::npos) {
        continue;
      }
      ++annotated;
      ASSERT_EQ(probe.semi_reduction_probe_keys.size(), 1U) << probe.tag;
      // The annotated key always belongs to the annotated group itself.
      ASSERT_EQ(probe.relations.size(), 1U) << probe.tag;
      EXPECT_EQ(probe.semi_reduction_probe_keys.front().schema,
                probe.relations.front());
      EXPECT_EQ(probe.semi_reduction_probe_keys.front().name, "id");
      // No filter information may leak into the group: the rule names columns,
      // not filter shapes.
      EXPECT_FALSE(probe.filter);
    }
    EXPECT_GE(annotated, 1U) << "op=" << static_cast<int>(operation);
  }
}

// A band predicate has no membership domain, so there is no key set a build
// side could be cut down to. Neither is annotated.
TEST(CascadesTest, SemiReductionPushdownSkipsNonEqualityJoins) {
  for (const LogicalOperator operation :
       {LogicalOperator::kSemiJoin, LogicalOperator::kAntiJoin}) {
    Memo memo;
    (void)memo.Build({"t1", "t2"});
    const GroupId g1 = memo.EnsureGroup({"t1"});
    const GroupId g2 = memo.EnsureGroup({"t2"});
    const GroupId join_group =
        memo.EnsureDerivedGroup({"t1", "t2"}, "range_semi");
    memo.AddExpression(
        join_group,
        LogicalExpression{.operation = operation,
                          .children = {g1, g2},
                          .predicate = BinaryExpressionExp(
                              ColumnValueExp(ColumnName("t1", "id")),
                              BinaryOperation::kLessThan,
                              ColumnValueExp(ColumnName("t2", "id")))});

    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(join_group);
    for (const auto& expr : search.GetMemo().Get(join_group).expressions) {
      if (expr.operation != operation || expr.children.size() != 2) {
        continue;
      }
      const Group& probe = search.GetMemo().Get(expr.children[0]);
      EXPECT_TRUE(probe.semi_reduction_probe_keys.empty()) << probe.tag;
    }
    EXPECT_TRUE(search.GetMemo().Get(g1).semi_reduction_probe_keys.empty());
    EXPECT_TRUE(search.GetMemo().Get(g2).semi_reduction_probe_keys.empty());
  }
}

// A composite semi key is annotated with EVERY key column, because the
// executor's domain is built over the whole memcomparable encoding.
TEST(CascadesTest, SemiReductionPushdownAnnotatesEveryCompositeKey) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId g1 = memo.EnsureGroup({"t1"});
  const GroupId g2 = memo.EnsureGroup({"t2"});
  const GroupId join_group = memo.EnsureDerivedGroup({"t1", "t2"}, "composite");
  memo.AddExpression(
      join_group,
      LogicalExpression{
          .operation = LogicalOperator::kSemiJoin,
          .children = {g1, g2},
          .predicate = CombineConjuncts(
              {BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "a")),
                                   BinaryOperation::kEquals,
                                   ColumnValueExp(ColumnName("t2", "a"))),
               BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "b")),
                                   BinaryOperation::kEquals,
                                   ColumnValueExp(ColumnName("t2", "b")))})});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(join_group);
  size_t composite_annotations = 0;
  for (const auto& expr : search.GetMemo().Get(join_group).expressions) {
    if (expr.operation != LogicalOperator::kSemiJoin ||
        expr.children.size() != 2) {
      continue;
    }
    const Group& probe = search.GetMemo().Get(expr.children[0]);
    if (probe.semi_reduction_probe_keys.empty()) {
      continue;
    }
    ++composite_annotations;
    EXPECT_EQ(probe.semi_reduction_probe_keys.size(), 2U) << probe.tag;
  }
  EXPECT_GE(composite_annotations, 1U);
}

TEST(CascadesTest, JoinPredicateTransitivity) {
  Memo memo;
  (void)memo.Build({"t1", "t2", "t3"});
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

  memo.AddExpression(root,
                     LogicalExpression{.operation = LogicalOperator::kJoin,
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
  (void)memo.Build({"t1", "t2"});
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
  (void)memo.Build({"t1", "t2", "t3"});
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

  const Expression joint_predicate =
      BinaryExpressionExp(BinaryExpressionExp(p1, BinaryOperation::kAnd, p2),
                          BinaryOperation::kAnd, p3);

  memo.AddExpression(root,
                     LogicalExpression{.operation = LogicalOperator::kJoin,
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
      // Reduced expression will have only 2 equality terms (fewer than the
      // original 3 terms).
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
  (void)memo.Build({"t1", "t2"});
  const GroupId g1 = memo.EnsureGroup({"t1"});
  const GroupId g2 = memo.EnsureGroup({"t2"});
  const GroupId intersect_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "intersect_root");
  const GroupId except_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "except_root");

  memo.AddExpression(
      intersect_group,
      LogicalExpression{.operation = LogicalOperator::kIntersect,
                        .children = {g1, g2},
                        .target_list = {NamedExpression(
                            "id", ColumnValueExp(ColumnName("t1", "id")))}});

  memo.AddExpression(
      except_group,
      LogicalExpression{.operation = LogicalOperator::kExcept,
                        .children = {g1, g2},
                        .target_list = {NamedExpression(
                            "id", ColumnValueExp(ColumnName("t1", "id")))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(intersect_group);
  search.Explore(except_group);

  // Both lowerings publish the join through a Distinct wrapper (the set
  // operations are DISTINCT; the raw joins keep left-side multiplicity).
  auto contains_wrapped_join = [](const Memo& search_memo, const Group& group,
                                  LogicalOperator join_op) {
    for (const auto& expr : group.expressions) {
      if (expr.operation != LogicalOperator::kDistinct ||
          expr.children.size() != 1) {
        continue;
      }
      for (const auto& join_expr :
           search_memo.Get(expr.children[0]).expressions) {
        if (join_expr.operation == join_op) {
          return true;
        }
      }
    }
    return false;
  };
  EXPECT_TRUE(contains_wrapped_join(search.GetMemo(),
                                    search.GetMemo().Get(intersect_group),
                                    LogicalOperator::kSemiJoin));
  EXPECT_TRUE(contains_wrapped_join(search.GetMemo(),
                                    search.GetMemo().Get(except_group),
                                    LogicalOperator::kAntiJoin));
}

// Set-op row comparison treats NULLs as equal ("NULL is a duplicate of
// NULL"), so the lowering must join on IS NOT DISTINCT FROM. Plain `=`
// would drop null-bearing rows from INTERSECT and wrongly keep them in
// EXCEPT, and the Distinct wrapper cannot repair either.
TEST(CascadesTest, IntersectExceptLoweringUsesNullSafeRowEquality) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId g1 = memo.EnsureGroup({"t1"});
  const GroupId g2 = memo.EnsureGroup({"t2"});
  const GroupId intersect_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "intersect_null_root");
  const GroupId except_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "except_null_root");

  memo.AddExpression(
      intersect_group,
      LogicalExpression{.operation = LogicalOperator::kIntersect,
                        .children = {g1, g2},
                        .target_list = {NamedExpression(
                            "id", ColumnValueExp(ColumnName("t1", "id")))}});

  memo.AddExpression(
      except_group,
      LogicalExpression{.operation = LogicalOperator::kExcept,
                        .children = {g1, g2},
                        .target_list = {NamedExpression(
                            "id", ColumnValueExp(ColumnName("t1", "id")))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(intersect_group);
  search.Explore(except_group);

  auto join_predicate_uses_null_safe =
      [](const Memo& search_memo, const Group& group, LogicalOperator join_op) {
        bool found_join = false;
        for (const auto& expr : group.expressions) {
          if (expr.operation != LogicalOperator::kDistinct ||
              expr.children.size() != 1) {
            continue;
          }
          for (const auto& join_expr :
               search_memo.Get(expr.children[0]).expressions) {
            if (join_expr.operation != join_op || !join_expr.predicate ||
                !*join_expr.predicate) {
              continue;
            }
            found_join = true;
            for (const Expression& conjunct :
                 SplitConjuncts(*join_expr.predicate)) {
              EXPECT_EQ(conjunct->Type(), TypeTag::kBinaryExp);
              EXPECT_EQ(conjunct->AsBinaryExpression().Op(),
                        BinaryOperation::kIsNotDistinctFrom);
            }
          }
        }
        return found_join;
      };
  EXPECT_TRUE(join_predicate_uses_null_safe(
      search.GetMemo(), search.GetMemo().Get(intersect_group),
      LogicalOperator::kSemiJoin));
  EXPECT_TRUE(join_predicate_uses_null_safe(search.GetMemo(),
                                            search.GetMemo().Get(except_group),
                                            LogicalOperator::kAntiJoin));
}

TEST(CascadesTest, UnnestFilterPushdown) {
  Memo memo;
  const GroupId scan = memo.Build({"t1"});
  const GroupId unnest_group = memo.EnsureDerivedGroup({"t1"}, "unnest_grp");
  memo.AddExpression(unnest_group,
                     LogicalExpression{.operation = LogicalOperator::kUnnest,
                                       .children = {scan},
                                       .unnest_alias = "elem"});

  const GroupId sel_group = memo.EnsureDerivedGroup({"t1"}, "sel_unnest");
  Expression pred = BinaryExpressionExp(ColumnValueExp("t1.id"),
                                        BinaryOperation::kGreaterThan,
                                        ConstantValueExp(Value(int64_t{10})));
  memo.AddExpression(sel_group,
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
      const auto& child_exprs =
          search.GetMemo().Get(expr.children[0]).expressions;
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

TEST(CascadesTest, ConstantUnnestArrayFoldsToValues) {
  // A stand-alone UNNEST over the one-row dummy leaf with a constant array
  // argument IS the expanded element list: constant_unnest_to_values adds
  // the equivalent kValues node (with WITH OFFSET folded into the counter).
  Memo memo;
  const GroupId dummy = memo.EnsureDerivedGroup({}, "dummy_unnest");
  memo.AddExpression(
      dummy, LogicalExpression{.operation = LogicalOperator::kDummyScan});
  const GroupId unnest_group = memo.EnsureDerivedGroup({}, "unnest_grp");
  memo.AddExpression(
      unnest_group,
      LogicalExpression{
          .operation = LogicalOperator::kUnnest,
          .children = {dummy},
          .predicate = ConstantValueExp(Value::Array(
              {Value(int64_t{9}), Value(int64_t{4}), Value(int64_t{7})},
              "INT64")),
          .unnest_alias = "x",
          .offset_alias = "o"});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(unnest_group);

  const LogicalExpression* folded = nullptr;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(unnest_group).expressions) {
    if (expression.operation == LogicalOperator::kValues) {
      folded = &expression;
    }
  }
  ASSERT_TRUE(folded != nullptr);
  ASSERT_EQ(folded->values.size(), 3U);
  EXPECT_EQ(folded->values[0], Row({Value(9), Value(int64_t{0})}));
  EXPECT_EQ(folded->values[1], Row({Value(4), Value(int64_t{1})}));
  EXPECT_EQ(folded->values[2], Row({Value(7), Value(int64_t{2})}));
  ASSERT_EQ(folded->output_schema.ColumnCount(), 2U);
  auto lower = [](std::string_view text) {
    std::string out(text);
    std::ranges::transform(out, out.begin(), [](unsigned char c) {
      return static_cast<char>(std::tolower(c));
    });
    return out;
  };
  EXPECT_EQ(lower(folded->output_schema.GetColumn(0).Name().name), "x");
  EXPECT_EQ(lower(folded->output_schema.GetColumn(1).Name().name), "o");
  EXPECT_EQ(folded->output_schema.GetColumn(1).Type(), ValueType::kInt64);
}

TEST(CascadesTest, ConstantUnnestKeepsRuntimeWhenNotFoldable) {
  // Two refusals: a non-array constant keeps the runtime UNNEST (the
  // executor produces the UNNEST(scalar) error), and a driving input that is
  // more than the one-row dummy leaf cannot become a plain Values node.
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId scalar_group = memo.EnsureDerivedGroup({}, "unnest_scalar");
  memo.AddExpression(
      scalar_group,
      LogicalExpression{.operation = LogicalOperator::kUnnest,
                        .children = {memo.EnsureDerivedGroup({}, "dummy_two")},
                        .predicate = ConstantValueExp(Value(int64_t{5})),
                        .unnest_alias = "x"});
  const GroupId scan_group = memo.EnsureDerivedGroup({"t1"}, "unnest_scan");
  memo.AddExpression(
      scan_group,
      LogicalExpression{.operation = LogicalOperator::kUnnest,
                        .children = {scan},
                        .predicate = ConstantValueExp(Value::Array(
                            {Value(int64_t{9}), Value(int64_t{7})}, "INT64")),
                        .unnest_alias = "x"});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(scalar_group);
  search.Explore(scan_group);

  for (const GroupId group : {scalar_group, scan_group}) {
    for (const LogicalExpression& expression :
         search.GetMemo().Get(group).expressions) {
      EXPECT_NE(expression.operation, LogicalOperator::kValues);
    }
  }
}

TEST(CascadesTest, RecursiveTerminationPredicatePushdown) {
  // Production shape: an opaque childless kRecursiveCte leaf with a depth
  // column; a depth-window Selection above it narrows the driver's depth
  // bounds (post-filter `depth <= N` equals stopping the fixpoint at N
  // because every row at depth <= N is produced by iteration N).
  Memo memo;
  (void)memo.Build({"rec_table"});
  const GroupId cte_group = memo.EnsureDerivedGroup({"rec_table"}, "cte_grp");
  LogicalExpression cte{.operation = LogicalOperator::kRecursiveCte,
                        .cte_name = "rec_table"};
  cte.depth_spec =
      RecursiveDepthSpec{.column = "n", .lower = 0, .upper = 1 << 20};
  memo.AddExpression(cte_group, cte);

  const GroupId sel_group = memo.EnsureDerivedGroup({"rec_table"}, "sel_cte");
  Expression term_pred = BinaryExpressionExp(
      ColumnValueExp("rec_table.n"), BinaryOperation::kLessThanEquals,
      ConstantValueExp(Value(int64_t{100})));
  memo.AddExpression(sel_group,
                     LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {cte_group},
                                       .predicate = term_pred});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  bool found_narrowed = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kRecursiveCte &&
        expr.children.empty() && expr.depth_spec.has_value() &&
        expr.depth_spec->upper == 100 && expr.depth_spec->lower == 0) {
      found_narrowed = true;
    }
  }
  EXPECT_TRUE(found_narrowed);
}

TEST(CascadesTest, RecursiveTerminationNonDepthPredicateStays) {
  // A predicate over anything but the depth column would change the
  // fixpoint if pushed (the filtered rows feed later iterations), so the
  // rule must keep the Selection.
  Memo memo;
  (void)memo.Build({"rec_table"});
  const GroupId cte_group = memo.EnsureDerivedGroup({"rec_table"}, "cte_grp2");
  LogicalExpression cte{.operation = LogicalOperator::kRecursiveCte,
                        .cte_name = "rec_table"};
  cte.depth_spec =
      RecursiveDepthSpec{.column = "n", .lower = 0, .upper = 1 << 20};
  memo.AddExpression(cte_group, cte);

  const GroupId sel_group = memo.EnsureDerivedGroup({"rec_table"}, "sel_cte2");
  Expression term_pred = BinaryExpressionExp(
      ColumnValueExp("rec_table.v"), BinaryOperation::kLessThan,
      ConstantValueExp(Value(int64_t{100})));
  memo.AddExpression(sel_group,
                     LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {cte_group},
                                       .predicate = term_pred});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);
  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  for (const auto& expr : exprs) {
    EXPECT_NE(expr.operation, LogicalOperator::kRecursiveCte)
        << "non-depth predicate must not fold into the recursive leaf";
  }
}

TEST(CascadesTest, InListToSemiJoin) {
  Memo memo;
  const GroupId scan = memo.Build({"t1"});
  const GroupId sel_group = memo.EnsureDerivedGroup({"t1"}, "in_list_sel");

  const Expression eq1 =
      BinaryExpressionExp(ColumnValueExp("t1.id"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{1})));
  const Expression eq2 =
      BinaryExpressionExp(ColumnValueExp("t1.id"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{2})));
  const Expression eq3 =
      BinaryExpressionExp(ColumnValueExp("t1.id"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{3})));

  const Expression or_pred =
      BinaryExpressionExp(BinaryExpressionExp(eq1, BinaryOperation::kOr, eq2),
                          BinaryOperation::kOr, eq3);

  memo.AddExpression(sel_group,
                     LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {scan},
                                       .predicate = or_pred});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  bool found_semi = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kSemiJoin &&
        expr.children.size() == 2) {
      found_semi = true;
      break;
    }
  }
  EXPECT_TRUE(found_semi);
}

TEST(CascadesTest, FunctionalDependencyFilterReduction) {
  Memo memo;
  const GroupId scan = memo.Build({"t1"});
  const GroupId sel_group = memo.EnsureDerivedGroup({"t1"}, "sel_fd");

  const Expression eq =
      BinaryExpressionExp(ColumnValueExp("t1.age"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{10})));
  const Expression lte = BinaryExpressionExp(
      ColumnValueExp("t1.age"), BinaryOperation::kLessThanEquals,
      ConstantValueExp(Value(int64_t{20})));

  const Expression joint = BinaryExpressionExp(eq, BinaryOperation::kAnd, lte);

  memo.AddExpression(sel_group,
                     LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {scan},
                                       .predicate = joint});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  bool found_reduced = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kSelection && expr.predicate &&
        *expr.predicate) {
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

TEST(CascadesTest, FilterAggregatePushdown) {
  // Scalar aggregation: moving the selection into FILTER clauses preserves
  // the result, so the rewrite must fire.
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId sel_group = memo.EnsureDerivedGroup({"t1"}, "sel_before_agg");
  Expression filter_pred =
      BinaryExpressionExp(ColumnValueExp("t1.v"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(int64_t{10})));
  memo.AddExpression(sel_group,
                     LogicalExpression{.operation = LogicalOperator::kSelection,
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
                                              /*distinct=*/false))}});

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
          const auto& agg =
              dynamic_cast<const AggregateExpression&>(*target.expression);
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

TEST(CascadesTest, FilterAggregatePushdownRefusedForGroupedAggregation) {
  // With GROUP BY, Aggregation(Selection(p)) -> FILTER pushdown is NOT
  // group-membership preserving: groups whose every row fails p vanish in
  // the original but would survive as COUNT=0 phantom groups.
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId sel_group = memo.EnsureDerivedGroup({"t1"}, "sel_before_agg");
  Expression filter_pred =
      BinaryExpressionExp(ColumnValueExp("t1.v"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(int64_t{10})));
  memo.AddExpression(sel_group,
                     LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {scan},
                                       .predicate = filter_pred});

  const GroupId agg_group = memo.EnsureDerivedGroup({"t1"}, "agg_over_sel");
  memo.AddExpression(
      agg_group, LogicalExpression{
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
  for (const auto& expr : exprs) {
    ASSERT_FALSE(expr.operation == LogicalOperator::kAggregation &&
                 expr.children.size() == 1 && expr.children[0] == scan);
  }
}

TEST(CascadesTest, FilterAggregatePushdownRefusedForVolatilePredicate) {
  // The rewrite replicates the selection predicate into every aggregate's
  // FILTER clause. A volatile predicate (RAND()) evaluated once per row in
  // the Selection would be evaluated once per aggregate per row after the
  // rewrite, changing which rows each aggregate sees. It must not fire.
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId sel_group = memo.EnsureDerivedGroup({"t1"}, "sel_before_agg");
  Expression volatile_pred = BinaryExpressionExp(FunctionCallExp("rand", {}),
                                                 BinaryOperation::kLessThan,
                                                 ConstantValueExp(Value(0.5)));
  memo.AddExpression(sel_group,
                     LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {scan},
                                       .predicate = volatile_pred});

  const GroupId agg_group =
      memo.EnsureDerivedGroup({"t1"}, "agg_over_volatile");
  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {sel_group},
          .target_list = {NamedExpression(
              "sum_v", AggregateExpressionExp(AggregationType::kSum,
                                              ColumnValueExp("t1.v"),
                                              /*distinct=*/false))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg_group);

  const auto& exprs = search.GetMemo().Get(agg_group).expressions;
  for (const auto& expr : exprs) {
    ASSERT_FALSE(expr.operation == LogicalOperator::kAggregation &&
                 expr.children.size() == 1 && expr.children[0] == scan);
  }
}

TEST(CascadesTest, EquijoinSelectivityBackoff) {
  EXPECT_NEAR(EstimateEquijoinSelectivity({}), 1.0, 1e-12);
  EXPECT_NEAR(EstimateEquijoinSelectivity({0.01}), 0.01, 1e-12);
  const double two_keys = EstimateEquijoinSelectivity({0.1, 0.01});
  EXPECT_NEAR(two_keys, 0.01 * std::sqrt(0.1), 1e-12);
  EXPECT_LT(two_keys, 0.01);
  EXPECT_GT(two_keys, 0.001);
  const double three_keys = EstimateEquijoinSelectivity({0.5, 0.2, 0.1});
  EXPECT_NEAR(three_keys, 0.1 * std::sqrt(0.2) * std::pow(0.5, 0.25), 1e-12);
}

TEST(CascadesTest, MemorySpillCostModel) {
  MemoryBudget budget;
  budget.max_memory_bytes = 1024.0 * 1024.0;
  budget.row_size_bytes = 128.0;
  budget.io_spill_cost_multiplier = 3.0;

  const double no_spill =
      EstimateMemorySpillCost(OperatorCostKind::kSort, 1000.0, budget);
  EXPECT_NEAR(no_spill, 0.0, 1e-6);

  const double spill_sort =
      EstimateMemorySpillCost(OperatorCostKind::kSort, 20000.0, budget);
  EXPECT_GT(spill_sort, 0.0);

  const double spill_hash =
      EstimateMemorySpillCost(OperatorCostKind::kHashJoin, 20000.0, budget);
  EXPECT_GT(spill_hash, 0.0);
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
          .output_schema = Schema("u", {Column("val", ValueType::kInt64),
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
          .output_schema =
              Schema("out", {Column("sum_val", ValueType::kInt64),
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

TEST(CascadesTest, WindowPartitionFilterPushdownUsesCanonicalRule) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId win_group = memo.EnsureDerivedGroup({"t1"}, "win_node");
  memo.AddExpression(
      win_group,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {scan},
          .target_list = {NamedExpression(
              "rn", WindowFunctionCallExp("row_number", {},
                                          {ColumnValueExp("t1.part")}, {}))},
          .output_schema = Schema("t1", {Column("part", ValueType::kInt64),
                                         Column("val", ValueType::kInt64)}),
          .partition_by = {ColumnValueExp("t1.part")}});

  const GroupId sel_group = memo.EnsureDerivedGroup({"t1"}, "sel_over_win");
  memo.AddExpression(
      sel_group,
      LogicalExpression{.operation = LogicalOperator::kSelection,
                        .children = {win_group},
                        .predicate = BinaryExpressionExp(
                            ColumnValueExp("t1.part"), BinaryOperation::kEquals,
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

TEST(CascadesTest, PkUniqueDistinctElimination) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  memo.AddExpression(
      scan,
      LogicalExpression{.operation = LogicalOperator::kScan,
                        .table = "t1",
                        .output_schema = Schema(
                            "t1", {Column("id", ValueType::kInt64,
                                          Constraint(Constraint::kPrimaryKey)),
                                   Column("name", ValueType::kVarChar)})});

  const GroupId distinct_group =
      memo.EnsureDerivedGroup({"t1"}, "distinct_on_pk");
  memo.AddExpression(
      distinct_group,
      LogicalExpression{
          .operation = LogicalOperator::kDistinct,
          .children = {scan},
          .target_list = {NamedExpression("id", ColumnValueExp("t1.id")),
                          NamedExpression("name", ColumnValueExp("t1.name"))},
          .output_schema =
              Schema("t1", {Column("id", ValueType::kInt64),
                            Column("name", ValueType::kVarChar)})});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(distinct_group);

  const auto& exprs = search.GetMemo().Get(distinct_group).expressions;
  bool found_eliminated = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kScan) {
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
  // SQL publishes the catalog separately; its Scan has no output_schema.
  memo.SetTableSchemas(
      {{"t1", Schema("t1", {Column("id", ValueType::kInt64,
                                   Constraint(Constraint::kNotNull)),
                            Column("name", ValueType::kVarChar)})}});

  const GroupId sel_group = memo.EnsureDerivedGroup({"t1"}, "sel_is_not_null");
  memo.AddExpression(
      sel_group, LogicalExpression{
                     .operation = LogicalOperator::kSelection,
                     .children = {scan},
                     .predicate = UnaryExpressionExp(
                         ColumnValueExp("t1.id"), UnaryOperation::kIsNotNull)});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);
  EXPECT_TRUE(
      search.AppliedRuleNames().contains("not_null_is_not_null_elimination"));

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  bool found_true_or_scan = false;
  for (const auto& expr : exprs) {
    if (expr.operation == LogicalOperator::kSelection && expr.predicate &&
        *expr.predicate) {
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

TEST(CascadesTest, AuditUnknownRelationConjunctRemainsResidual) {
  for (bool disjunction : {false, true}) {
    Memo memo;
    const GroupId input = memo.Build({"a", "b"});
    const GroupId root = memo.EnsureDerivedGroup({"a", "b"}, "audit_selection");
    Expression predicate = EqExp("outside.x", Value(1));
    if (disjunction) {
      predicate = BinaryExpressionExp(
          BinaryExpressionExp(predicate, BinaryOperation::kAnd,
                              EqExp("a.x", Value(1))),
          BinaryOperation::kOr,
          BinaryExpressionExp(EqExp("outside.x", Value(2)),
                              BinaryOperation::kAnd, EqExp("a.x", Value(2))));
    }
    ASSERT_TRUE(
        memo.AddExpression(root, {.operation = LogicalOperator::kSelection,
                                  .children = {input},
                                  .predicate = predicate}));
    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(root);
    EXPECT_FALSE(search.GetMemo().ContainsRelation({"outside"}));
  }
}

TEST(CascadesTest, AuditJoinInferenceDoesNotStrengthenBranchLocalScans) {
  for (bool inequality : {false, true}) {
    Memo memo;
    (void)memo.Build({"a", "b"});
    const auto a = memo.EnsureGroup({"a"});
    const auto b = memo.EnsureGroup({"b"});
    const auto branch = memo.EnsureDerivedGroup({"a", "b"}, "branch");
    memo.Get(branch).branch_local = true;
    memo.MergeScanFilter(
        a, BinaryExpressionExp(ColumnValueExp("a.x"),
                               inequality ? BinaryOperation::kGreaterThan
                                          : BinaryOperation::kEquals,
                               ConstantValueExp(Value(10))));
    ASSERT_TRUE(memo.AddExpression(
        branch, {.operation = LogicalOperator::kJoin,
                 .children = {a, b},
                 .predicate = BinaryExpressionExp(ColumnValueExp("a.x"),
                                                  BinaryOperation::kEquals,
                                                  ColumnValueExp("b.x"))}));
    RuleSet rules;
    for (const auto& rule : RuleSet::Default().Rules()) {
      if (rule.Name() == (inequality ? "inferred_inequality_pushdown"
                                     : "infer_join_predicates")) {
        rules.Add(rule);
      }
    }
    ASSERT_EQ(rules.Rules().size(), 1U);
    SearchEngine search(std::move(memo), rules);
    search.Explore(branch);
    EXPECT_FALSE(search.GetMemo().Get(b).filter);
  }
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
      EqExp("a.x", Value(1)), BinaryOperation::kAnd, EqExp("b.z", Value(2)));
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
      proj_group, LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {scan},
                                    .target_list = {NamedExpression(
                                        "alias", ColumnValueExp("a.x"))}}));
  const GroupId root = memo.EnsureDerivedGroup({"a"}, "filter");
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kSelection,
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
              NamedExpression("s", AggregateExpressionExp(AggregationType::kSum,
                                                          ColumnValueExp("a.y"),
                                                          false))}}));
  const GroupId root = memo.EnsureDerivedGroup({"a"}, "filter");
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kSelection,
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
  const GroupId join_group = memo.EnsureDerivedGroup({"a", "b"}, "outer-join");
  ASSERT_TRUE(memo.AddExpression(
      join_group, LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                                    .children = {memo.EnsureGroup({"a"}),
                                                 memo.EnsureGroup({"b"})},
                                    .join_type = 0}));
  const GroupId selection = memo.EnsureDerivedGroup({"a", "b"}, "filter");
  const Expression predicate = EqExp("a.x", Value(1));
  ASSERT_TRUE(memo.AddExpression(
      selection, LogicalExpression{.operation = LogicalOperator::kSelection,
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
            join_candidates.expressions, [&](const LogicalExpression& join) {
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
  // Prune aggregate inputs while retaining the output projection and aliases.
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId agg_group = memo.EnsureDerivedGroup({"a"}, "agg");
  ASSERT_TRUE(memo.AddExpression(
      agg_group, LogicalExpression{.operation = LogicalOperator::kAggregation,
                                   .children = {scan},
                                   .target_list = {NamedExpression(
                                       "k", ColumnValueExp("a.x"))},
                                   .grouping_sets = {ColumnValueExp("a.x")}}));
  const GroupId root = memo.EnsureDerivedGroup({"a"}, "proj");
  ASSERT_TRUE(memo.AddExpression(
      root, LogicalExpression{
                .operation = LogicalOperator::kProjection,
                .children = {agg_group},
                .target_list = {NamedExpression("k", ColumnValueExp("k"))}}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);

  const bool found = std::ranges::any_of(
      search.GetMemo().Get(root).expressions,
      [&](const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kProjection ||
            expression.children.size() != 1 ||
            expression.children[0] == agg_group) {
          return false;
        }
        for (const auto& aggregate :
             search.GetMemo().Get(expression.children[0]).expressions) {
          if (aggregate.operation != LogicalOperator::kAggregation ||
              aggregate.children.size() != 1) {
            continue;
          }
          for (const auto& child :
               search.GetMemo().Get(aggregate.children[0]).expressions) {
            if (child.operation == LogicalOperator::kProjection &&
                child.children == std::vector<GroupId>{scan}) {
              return true;
            }
          }
        }
        return false;
      });
  EXPECT_TRUE(found);
}

TEST(CascadesTest, MergeSelectionsFlattensBothPredicates) {
  // merge_selections flattens Selection(p2, Selection(p1, X)) into a
  // single Selection(p1 AND p2, X) so the residual chain collapses.
  Memo memo;
  const GroupId scan = memo.Build({"a"});
  const GroupId inner = memo.EnsureDerivedGroup({"a"}, "selection");
  ASSERT_TRUE(memo.AddExpression(
      inner, LogicalExpression{.operation = LogicalOperator::kSelection,
                               .children = {scan},
                               .predicate = EqExp("a.x", Value(1))}));
  const GroupId outer = memo.EnsureDerivedGroup({"a"}, "selection2");
  ASSERT_TRUE(memo.AddExpression(
      outer, LogicalExpression{.operation = LogicalOperator::kSelection,
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

// ====================================================================
// D5 counterexample tests: verify that dangerous rules have proper gate
// conditions and do NOT fire when preconditions are not met.
// ====================================================================

TEST(CascadesTest, PushDownLimitThroughJoinSkipsNonUniqueJoin) {
  // D5 counterexample: push_down_limit_through_join must NOT push LIMIT
  // into a join when the join key is not unique on the inner side.
  // A many-to-many join with LIMIT can return wrong row counts.
  Memo memo;
  (void)memo.Build({"orders", "items"});
  const GroupId left = memo.EnsureGroup({"orders"});
  const GroupId right = memo.EnsureGroup({"items"});
  const GroupId join_group = memo.EnsureDerivedGroup({"orders", "items"}, "j");
  // orders.item_id = items.id — items.id is NOT unique (many orders per item)
  ASSERT_TRUE(memo.AddExpression(
      join_group, LogicalExpression{.operation = LogicalOperator::kJoin,
                                    .children = {left, right},
                                    .predicate = BinaryExpressionExp(
                                        ColumnValueExp("orders.item_id"),
                                        BinaryOperation::kEquals,
                                        ColumnValueExp("items.id"))}));
  const GroupId limit_group = memo.EnsureDerivedGroup({"orders", "items"}, "l");
  ASSERT_TRUE(memo.AddExpression(
      limit_group, LogicalExpression{.operation = LogicalOperator::kLimit,
                                     .children = {join_group},
                                     .limit_count = 5,
                                     .limit_offset = 0}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(limit_group);

  // The rule should NOT have pushed LIMIT into the join's left side.
  // Verify no Limit-over-Scan pattern exists inside the join.
  const bool limit_pushed = std::ranges::any_of(
      search.GetMemo().Get(limit_group).expressions,
      [&](const LogicalExpression& expr) {
        if (expr.operation != LogicalOperator::kJoin) {
          return false;
        }
        for (GroupId child : expr.children) {
          for (const LogicalExpression& cexpr :
               search.GetMemo().Get(child).expressions) {
            if (cexpr.operation == LogicalOperator::kLimit) {
              return true;
            }
          }
        }
        return false;
      });
  EXPECT_FALSE(limit_pushed);
}

TEST(CascadesTest, EliminateDoubleSortRequiresSameKeyExpressions) {
  // D5 counterexample: eliminate_double_sort must NOT eliminate the outer
  // sort when the inner sort uses different key expressions, even if the
  // ascending flags happen to match.
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  // Inner: ORDER BY t.a ASC
  const GroupId inner_sort = memo.EnsureDerivedGroup({"t"}, "inner_sort");
  ASSERT_TRUE(memo.AddExpression(
      inner_sort, LogicalExpression{.operation = LogicalOperator::kSort,
                                    .children = {scan},
                                    .target_list = {NamedExpression(
                                        "a", ColumnValueExp("t.a"))},
                                    .sort_ascending = {true},
                                    .sort_nulls_first = {false}}));
  // Outer: ORDER BY t.b ASC (different key, same direction)
  const GroupId outer_sort = memo.EnsureDerivedGroup({"t"}, "outer_sort");
  ASSERT_TRUE(memo.AddExpression(
      outer_sort, LogicalExpression{.operation = LogicalOperator::kSort,
                                    .children = {inner_sort},
                                    .target_list = {NamedExpression(
                                        "b", ColumnValueExp("t.b"))},
                                    .sort_ascending = {true},
                                    .sort_nulls_first = {false}}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(outer_sort);

  // The outer sort should NOT have been eliminated because the keys differ.
  const bool eliminated = std::ranges::any_of(
      search.GetMemo().Get(outer_sort).expressions,
      [&](const LogicalExpression& expr) {
        if (expr.operation != LogicalOperator::kSort ||
            expr.children.size() != 1) {
          return false;
        }
        // If the child is the scan directly, the sort was eliminated.
        return search.GetMemo().Get(expr.children[0]).relations ==
                   std::vector<std::string>{"t"} &&
               std::ranges::none_of(
                   search.GetMemo().Get(expr.children[0]).expressions,
                   [](const LogicalExpression& e) {
                     return e.operation == LogicalOperator::kSort;
                   });
      });
  EXPECT_FALSE(eliminated);
}

TEST(CascadesTest, EliminateDoubleSortRequiresSameNullsFirst) {
  // D5 counterexample: eliminate_double_sort must NOT eliminate the outer
  // sort when nulls_first differs between inner and outer.
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  // Inner: ORDER BY t.a ASC NULLS FIRST
  const GroupId inner_sort = memo.EnsureDerivedGroup({"t"}, "inner_sort");
  ASSERT_TRUE(memo.AddExpression(
      inner_sort, LogicalExpression{.operation = LogicalOperator::kSort,
                                    .children = {scan},
                                    .target_list = {NamedExpression(
                                        "a", ColumnValueExp("t.a"))},
                                    .sort_ascending = {true},
                                    .sort_nulls_first = {true}}));
  // Outer: ORDER BY t.a ASC NULLS LAST (different nulls order)
  const GroupId outer_sort = memo.EnsureDerivedGroup({"t"}, "outer_sort");
  ASSERT_TRUE(memo.AddExpression(
      outer_sort, LogicalExpression{.operation = LogicalOperator::kSort,
                                    .children = {inner_sort},
                                    .target_list = {NamedExpression(
                                        "a", ColumnValueExp("t.a"))},
                                    .sort_ascending = {true},
                                    .sort_nulls_first = {false}}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(outer_sort);

  // The outer sort should NOT be eliminated because nulls_first differs.
  const bool eliminated = std::ranges::any_of(
      search.GetMemo().Get(outer_sort).expressions,
      [&](const LogicalExpression& expr) {
        if (expr.operation != LogicalOperator::kSort ||
            expr.children.size() != 1) {
          return false;
        }
        return search.GetMemo().Get(expr.children[0]).relations ==
                   std::vector<std::string>{"t"} &&
               std::ranges::none_of(
                   search.GetMemo().Get(expr.children[0]).expressions,
                   [](const LogicalExpression& e) {
                     return e.operation == LogicalOperator::kSort;
                   });
      });
  EXPECT_FALSE(eliminated);
}

TEST(CascadesTest, InListToSemiJoinRejectsMixedColumnOr) {
  // D5 counterexample: in_list_to_semi_join must NOT transform a predicate
  // where OR branches reference different columns into a semi join.
  // `x = 1 OR y = 2` cannot become SemiJoin(x, {1}) because y is lost.
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId sel = memo.EnsureDerivedGroup({"t"}, "sel");
  // WHERE x = 1 OR y = 2
  Expression predicate = BinaryExpressionExp(
      EqExp("t.x", Value(1)), BinaryOperation::kOr, EqExp("t.y", Value(2)));
  ASSERT_TRUE(memo.AddExpression(
      sel, LogicalExpression{.operation = LogicalOperator::kSelection,
                             .children = {scan},
                             .predicate = predicate}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel);

  // The rule should NOT have created a SemiJoin.
  const bool has_semi_join = std::ranges::any_of(
      search.GetMemo().Get(sel).expressions, [](const LogicalExpression& expr) {
        return expr.operation == LogicalOperator::kSemiJoin;
      });
  EXPECT_FALSE(has_semi_join);
}

TEST(CascadesTest, OuterToAntiJoinRequiresNullCheckOnAllRightColumns) {
  // D5 counterexample: outer_to_anti_join must NOT convert when the
  // WHERE clause references right-side columns that are NOT IS NULL checked.
  // SELECT * FROM a LEFT JOIN b ON a.id = b.id
  //   WHERE b.id IS NULL AND b.name = 'foo'
  // The b.name predicate needs actual right-side data, not NULL padding.
  Memo memo;
  (void)memo.Build({"a", "b"});
  const GroupId left = memo.EnsureGroup({"a"});
  const GroupId right = memo.EnsureGroup({"b"});
  const GroupId join_group = memo.EnsureDerivedGroup({"a", "b"}, "oj");
  ASSERT_TRUE(memo.AddExpression(
      join_group,
      LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                        .children = {left, right},
                        .predicate = BinaryExpressionExp(
                            ColumnValueExp("a.id"), BinaryOperation::kEquals,
                            ColumnValueExp("b.id")),
                        .join_type = 0}));
  // WHERE b.id IS NULL AND b.name = 'foo'
  Expression null_check =
      UnaryExpressionExp(ColumnValueExp("b.id"), UnaryOperation::kIsNull);
  Expression name_check = EqExp("b.name", Value("foo"));
  Expression combined =
      BinaryExpressionExp(null_check, BinaryOperation::kAnd, name_check);
  const GroupId sel = memo.EnsureDerivedGroup({"a", "b"}, "sel");
  ASSERT_TRUE(memo.AddExpression(
      sel, LogicalExpression{.operation = LogicalOperator::kSelection,
                             .children = {join_group},
                             .predicate = combined}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel);

  // The rule should NOT have created an AntiJoin because b.name = 'foo'
  // still needs right-side data.
  const bool has_anti_join = std::ranges::any_of(
      search.GetMemo().Get(sel).expressions, [](const LogicalExpression& expr) {
        return expr.operation == LogicalOperator::kAntiJoin;
      });
  EXPECT_FALSE(has_anti_join);
}

TEST(CascadesTest, InListToSemiJoinRejectsUncollectableBranch) {
  // D5 counterexample (収集できなかったOR枝を捨てない): three collectable
  // same-column equalities PLUS one range branch.  The old rule fired on the
  // three collected values and silently dropped `t.x > 9`, losing rows.
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId sel = memo.EnsureDerivedGroup({"t"}, "sel");
  Expression predicate = BinaryExpressionExp(
      BinaryExpressionExp(
          BinaryExpressionExp(EqExp("t.x", Value(1)), BinaryOperation::kOr,
                              EqExp("t.x", Value(2))),
          BinaryOperation::kOr, EqExp("t.x", Value(3))),
      BinaryOperation::kOr,
      BinaryExpressionExp(ColumnValueExp("t.x"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(int64_t{9}))));
  ASSERT_TRUE(memo.AddExpression(
      sel, LogicalExpression{.operation = LogicalOperator::kSelection,
                             .children = {scan},
                             .predicate = predicate}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel);
  const bool has_semi_join = std::ranges::any_of(
      search.GetMemo().Get(sel).expressions, [](const LogicalExpression& expr) {
        return expr.operation == LogicalOperator::kSemiJoin;
      });
  EXPECT_FALSE(has_semi_join);
}

TEST(CascadesTest, DerivedGroupFingerprintSeparatesDifferentInLists) {
  // D5 (docs/design.md) acceptance 2: two IN-lists over the same column
  // with the same SIZE but different values must not share one derived
  // constant group (a count-only tag would collide and mix both value sets).
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId sel_a = memo.EnsureDerivedGroup({"t"}, "sel_a");
  const GroupId sel_b = memo.EnsureDerivedGroup({"t"}, "sel_b");
  const auto eq = [](std::string_view col, int64_t v) {
    return EqExp(col, Value(v));
  };
  Expression pred_a = BinaryExpressionExp(
      BinaryExpressionExp(eq("t.x", 1), BinaryOperation::kOr, eq("t.x", 2)),
      BinaryOperation::kOr, eq("t.x", 3));
  Expression pred_b = BinaryExpressionExp(
      BinaryExpressionExp(eq("t.x", 4), BinaryOperation::kOr, eq("t.x", 5)),
      BinaryOperation::kOr, eq("t.x", 6));
  ASSERT_TRUE(memo.AddExpression(
      sel_a, LogicalExpression{.operation = LogicalOperator::kSelection,
                               .children = {scan},
                               .predicate = pred_a}));
  ASSERT_TRUE(memo.AddExpression(
      sel_b, LogicalExpression{.operation = LogicalOperator::kSelection,
                               .children = {scan},
                               .predicate = pred_b}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_a);
  search.Explore(sel_b);
  const auto& memo_ref = search.GetMemo();
  GroupId const_a = 0;
  GroupId const_b = 0;
  bool found_a = false;
  bool found_b = false;
  for (const auto& expr : memo_ref.Get(sel_a).expressions) {
    if (expr.operation == LogicalOperator::kSemiJoin) {
      const_a = expr.children[1];
      found_a = true;
    }
  }
  for (const auto& expr : memo_ref.Get(sel_b).expressions) {
    if (expr.operation == LogicalOperator::kSemiJoin) {
      const_b = expr.children[1];
      found_b = true;
    }
  }
  ASSERT_TRUE(found_a);
  ASSERT_TRUE(found_b);
  EXPECT_NE(const_a, const_b);
}

TEST(CascadesTest, UniqueSemiToInnerDoesNotFireWithoutKeyEquality) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId semi_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "semi_no_key");
  memo.AddExpression(
      semi_group, LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                    .children = {left, right},
                                    .predicate = BinaryExpressionExp(
                                        ColumnValueExp(ColumnName("t1", "a")),
                                        BinaryOperation::kGreaterThan,
                                        ConstantValueExp(Value(0)))});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(semi_group);
  bool found_inner = false;
  for (const auto& expr : search.GetMemo().Get(semi_group).expressions) {
    if (expr.operation == LogicalOperator::kJoin) {
      found_inner = true;
    }
  }
  EXPECT_FALSE(found_inner);
}

TEST(CascadesTest, PushFilterPastSetopRejectsUnresolvedQualifier) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId union_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "union_branch");
  memo.AddExpression(union_group,
                     LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                       .children = {left, right}});
  const GroupId sel_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "sel_over_union");
  memo.AddExpression(
      sel_group, LogicalExpression{.operation = LogicalOperator::kSelection,
                                   .children = {union_group},
                                   .predicate = BinaryExpressionExp(
                                       ColumnValueExp(ColumnName("t1", "a")),
                                       BinaryOperation::kGreaterThan,
                                       ConstantValueExp(Value(0)))});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);
  // The t1-qualified predicate must not be pushed into the t2 branch.
  bool pushed_into_right = false;
  for (const auto& expr : search.GetMemo().Get(right).expressions) {
    if (expr.operation == LogicalOperator::kSelection && expr.predicate) {
      pushed_into_right = true;
    }
  }
  EXPECT_FALSE(pushed_into_right);
}

TEST(CascadesTest, PushFilterPastSetopRejectsUnqualifiedColumnMissingInBranch) {
  // `WHERE x > 1` over UNION ALL of (a.x) and (b.y): pushing x > 1 into the
  // b branch would filter on a column b does not have.  The published
  // schemas prove the name only resolves in one branch.
  Memo memo;
  (void)memo.Build({"a", "b"});
  memo.SetTableSchemas({{"a", Schema("a", {Column("x", ValueType::kInt64)})},
                        {"b", Schema("b", {Column("y", ValueType::kInt64)})}});
  const GroupId left = memo.EnsureGroup({"a"});
  const GroupId right = memo.EnsureGroup({"b"});
  const GroupId union_group =
      memo.EnsureDerivedGroup({"a", "b"}, "union_branch");
  memo.AddExpression(union_group,
                     LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                       .children = {left, right}});
  const GroupId sel_group =
      memo.EnsureDerivedGroup({"a", "b"}, "sel_over_union");
  memo.AddExpression(sel_group,
                     LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {union_group},
                                       .predicate = BinaryExpressionExp(
                                           ColumnValueExp(ColumnName("x")),
                                           BinaryOperation::kGreaterThan,
                                           ConstantValueExp(Value(0)))});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);
  const bool pushed_into_right = std::ranges::any_of(
      search.GetMemo().Get(right).expressions,
      [](const LogicalExpression& expr) {
        return expr.operation == LogicalOperator::kSelection &&
               expr.predicate.has_value();
      });
  EXPECT_FALSE(pushed_into_right);
}

TEST(CascadesTest, LimitPushThroughSortKeepsOffsetOnTop) {
  Memo memo;
  (void)memo.Build({"t"});
  const GroupId scan = memo.EnsureGroup({"t"});
  memo.AddExpression(
      scan,
      LogicalExpression{.operation = LogicalOperator::kScan, .table = "t"});
  const GroupId sort_group = memo.EnsureDerivedGroup({"t"}, "sort");
  memo.AddExpression(
      sort_group,
      LogicalExpression{
          .operation = LogicalOperator::kSort,
          .children = {scan},
          .target_list = {NamedExpression("a", ColumnValueExp("t.a"))},
          .sort_ascending = {true},
          .sort_nulls_first = {std::optional<bool>{}}});
  const GroupId limit_group = memo.EnsureDerivedGroup({"t"}, "limit_offset");
  memo.AddExpression(limit_group,
                     LogicalExpression{.operation = LogicalOperator::kLimit,
                                       .children = {sort_group},
                                       .limit_count = 10,
                                       .limit_offset = 5});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(limit_group);
  // With OFFSET 5 the rule must not push Limit(10, 5) below the sort (that
  // would skip pre-sort rows). Every Sort alternative in the top group must
  // still read from the sort group, and the offset must survive on a Limit.
  bool offset_preserved = false;
  for (const auto& expr : search.GetMemo().Get(limit_group).expressions) {
    if (expr.operation == LogicalOperator::kLimit && expr.limit_offset == 5) {
      offset_preserved = true;
    }
  }
  EXPECT_TRUE(offset_preserved);
}

TEST(CascadesTest, JoinOnFalseToEmptyPreservesRelationSet) {
  // Fixed: the rule emitted Limit(left), which fails relation validation
  // (and drops the right schema), so it never fired. It must emit Empty
  // over the full relation set instead.
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId join_group = memo.EnsureDerivedGroup({"t1", "t2"}, "jfalse");
  memo.AddExpression(
      join_group, LogicalExpression{.operation = LogicalOperator::kJoin,
                                    .children = {left, right},
                                    .predicate = ConstantValueExp(Value(0))});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(join_group);
  bool found_empty = false;
  for (const auto& expr : search.GetMemo().Get(join_group).expressions) {
    if (expr.operation == LogicalOperator::kEmpty) {
      found_empty = true;
    }
  }
  EXPECT_TRUE(found_empty);
}

TEST(CascadesTest, SelfJoinEliminationRejectsDistinctRealTables) {
  // Fixed: alias-stripping conflated real tables `orders` and `orders_1`.
  Memo memo;
  (void)memo.Build({"orders", "orders_1"});
  const GroupId left = memo.EnsureGroup({"orders"});
  memo.AddExpression(left,
                     LogicalExpression{.operation = LogicalOperator::kScan,
                                       .table = "orders"});
  const GroupId right = memo.EnsureGroup({"orders_1"});
  memo.AddExpression(right,
                     LogicalExpression{.operation = LogicalOperator::kScan,
                                       .table = "orders_1"});
  const GroupId join_group =
      memo.EnsureDerivedGroup({"orders", "orders_1"}, "selfjoin_distinct");
  memo.AddExpression(
      join_group,
      LogicalExpression{.operation = LogicalOperator::kJoin,
                        .children = {left, right},
                        .predicate = BinaryExpressionExp(
                            ColumnValueExp(ColumnName("orders", "id")),
                            BinaryOperation::kEquals,
                            ColumnValueExp(ColumnName("orders_1", "id")))});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(join_group);
  for (const auto& expr : search.GetMemo().Get(join_group).expressions) {
    EXPECT_NE(expr.operation, LogicalOperator::kScan)
        << "distinct real tables must not collapse to a single scan";
  }
}

TEST(CascadesTest, IntersectWithoutEquatableColumnsDoesNotInventIdJoin) {
  // Fixed: BuildEqualityOnAllColumns invented an `id` equality when no
  // columns were known, joining on a possibly nonexistent key.
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId setop = memo.EnsureDerivedGroup({"t1", "t2"}, "isect_nokey");
  memo.AddExpression(setop,
                     LogicalExpression{.operation = LogicalOperator::kIntersect,
                                       .children = {left, right}});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(setop);
  // The rule must simply not add a SemiJoin alternative here.
  bool found_semi = false;
  for (const auto& expr : search.GetMemo().Get(setop).expressions) {
    if (expr.operation == LogicalOperator::kSemiJoin) {
      found_semi = true;
    }
  }
  EXPECT_FALSE(found_semi);
}

TEST(CascadesTest, GroupLogicalPropertiesPropagation) {
  Memo memo;
  const Schema user_schema(
      "users",
      {Column("id", ValueType::kInt64, Constraint(Constraint::kPrimaryKey)),
       Column("email", ValueType::kVarChar, Constraint(Constraint::kUnique)),
       Column("name", ValueType::kVarChar)});
  memo.SetTableSchemas({{"users", user_schema}});
  const GroupId users_group = memo.Build({"users"});
  const auto& props = memo.Get(users_group).logical_properties;
  EXPECT_TRUE(props.IsUniqueOn({"users.id"}));
  EXPECT_TRUE(props.IsUniqueOn({"id"}));
  // A nullable UNIQUE column may repeat NULLs (multi-value key encoding), so
  // it is not a uniqueness proof.
  EXPECT_FALSE(props.IsUniqueOn({"users.email"}));
  EXPECT_TRUE(props.IsNotNull("users.id"));
  EXPECT_FALSE(props.IsUniqueOn({"users.name"}));
}

TEST(CascadesTest, IncrementalPropertiesRefreshChangedChildren) {
  Memo memo;
  const GroupId base = memo.Build({"users"});
  const GroupId child = memo.EnsureDerivedGroup({"users"}, "child");
  const GroupId parent = memo.EnsureDerivedGroup({"users"}, "parent");

  ASSERT_TRUE(memo.AddExpression(
      child, LogicalExpression{.operation = LogicalOperator::kSelection,
                               .children = {base},
                               .predicate = BinaryExpressionExp(
                                   ColumnValueExp(ColumnName("users", "id")),
                                   BinaryOperation::kGreaterThan,
                                   ConstantValueExp(Value(0)))}));
  ASSERT_TRUE(memo.AddExpression(
      parent, LogicalExpression{.operation = LogicalOperator::kSelection,
                                .children = {child},
                                .predicate = BinaryExpressionExp(
                                    ColumnValueExp(ColumnName("users", "id")),
                                    BinaryOperation::kGreaterThan,
                                    ConstantValueExp(Value(0)))}));
  ASSERT_TRUE(memo.AddExpression(
      child, LogicalExpression{
                 .operation = LogicalOperator::kSelection,
                 .children = {base},
                 .predicate = BinaryExpressionExp(
                     ColumnValueExp(ColumnName("users", "id")),
                     BinaryOperation::kEquals,
                     ColumnValueExp(ColumnName("users", "employee_id")))}));
  ASSERT_TRUE(memo.AddExpression(
      parent, LogicalExpression{.operation = LogicalOperator::kSelection,
                                .children = {child},
                                .predicate = BinaryExpressionExp(
                                    ColumnValueExp(ColumnName("users", "id")),
                                    BinaryOperation::kLessThan,
                                    ConstantValueExp(Value(100)))}));

  EXPECT_TRUE(memo.Get(parent).logical_properties.AreEqual(
      "users.id", "users.employee_id"));
}

TEST(CascadesTest, LogicalPropertiesSchemaRefreshIsChildFirst) {
  Memo memo;
  (void)memo.Build({"a", "b"});
  const GroupId left = memo.EnsureGroup({"a"});
  const GroupId right = memo.EnsureGroup({"b"});
  const GroupId parent = memo.EnsureDerivedGroup({"a", "b"}, "join");
  ASSERT_TRUE(memo.AddExpression(
      parent, LogicalExpression{.operation = LogicalOperator::kJoin,
                                .children = {left, right},
                                .predicate = BinaryExpressionExp(
                                    ColumnValueExp(ColumnName("a", "x")),
                                    BinaryOperation::kEquals,
                                    ColumnValueExp(ColumnName("b", "id")))}));

  memo.SetTableSchemas(
      {{"a", Schema("a", {Column("x", ValueType::kInt64,
                                 Constraint(Constraint::kPrimaryKey))})},
       {"b", Schema("b", {Column("id", ValueType::kInt64,
                                 Constraint(Constraint::kPrimaryKey))})}});

  EXPECT_TRUE(memo.Get(parent).logical_properties.IsUniqueOn({"a.x"}));
}

TEST(CascadesTest, PushDownLimitThroughJoinOnUniqueKey) {
  // DISABLED-RULE counterexample (docs/cascades_optimizer.md):
  // push_down_limit_through_join pushed Limit(k) into the left side of an
  // inner join whose right keys are unique.  Uniqueness bounds the match
  // count at one but does NOT prove every left row matches, so an unmatched
  // left row inside the limit prefix silently shrinks the result: with
  // L = {1, 2, 3}, R matching only key 3, `LIMIT 2` must return the single
  // row {3} (first two join outputs), while the pushed plan returned the
  // matches of {1, 2} -- one row fewer.  Re-enabling requires
  // referential-integrity metadata proving ">= 1 match per left row".
  Memo memo;
  const Schema orders_schema(
      "orders",
      {Column("id", ValueType::kInt64, Constraint(Constraint::kPrimaryKey)),
       Column("user_id", ValueType::kInt64)});
  const Schema users_schema(
      "users",
      {Column("id", ValueType::kInt64, Constraint(Constraint::kPrimaryKey)),
       Column("name", ValueType::kVarChar)});
  memo.SetTableSchemas({{"orders", orders_schema}, {"users", users_schema}});
  (void)memo.Build({"orders", "users"});
  const GroupId left = memo.EnsureGroup({"orders"});
  const GroupId right = memo.EnsureGroup({"users"});
  const GroupId join_group =
      memo.EnsureDerivedGroup({"orders", "users"}, "inner_join");
  memo.AddExpression(
      join_group,
      LogicalExpression{.operation = LogicalOperator::kJoin,
                        .children = {left, right},
                        .predicate = BinaryExpressionExp(
                            ColumnValueExp(ColumnName("orders", "user_id")),
                            BinaryOperation::kEquals,
                            ColumnValueExp(ColumnName("users", "id")))});

  const GroupId limit_group =
      memo.EnsureDerivedGroup({"orders", "users"}, "limit_join");
  memo.AddExpression(limit_group,
                     LogicalExpression{.operation = LogicalOperator::kLimit,
                                       .children = {join_group},
                                       .limit_count = 10,
                                       .limit_offset = 0});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(limit_group);

  bool found_limit_pushed = false;
  for (const auto& expr : search.GetMemo().Get(limit_group).expressions) {
    if (expr.operation == LogicalOperator::kJoin && expr.children.size() == 2) {
      const Group& child_group = search.GetMemo().Get(expr.children[0]);
      for (const auto& child_expr : child_group.expressions) {
        if (child_expr.operation == LogicalOperator::kLimit) {
          found_limit_pushed = true;
          break;
        }
      }
    }
  }
  EXPECT_FALSE(found_limit_pushed);
}

TEST(CascadesTest, AuditOuterJoinProofKeepsSuffixedRelationsDistinct) {
  Memo memo;
  (void)memo.Build({"audit_t", "audit_t_1"});
  const GroupId left = memo.EnsureGroup({"audit_t"});
  const GroupId right = memo.EnsureGroup({"audit_t_1"});
  const GroupId joined =
      memo.EnsureDerivedGroup({"audit_t", "audit_t_1"}, "outer");
  memo.AddExpression(
      joined,
      LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                        .children = {left, right},
                        .predicate = BinaryExpressionExp(
                            ColumnValueExp(ColumnName("audit_t", "id")),
                            BinaryOperation::kEquals,
                            ColumnValueExp(ColumnName("audit_t_1", "id"))),
                        .join_type = 0});
  const GroupId selected =
      memo.EnsureDerivedGroup({"audit_t", "audit_t_1"}, "filtered");
  memo.AddExpression(
      selected,
      LogicalExpression{
          .operation = LogicalOperator::kSelection,
          .children = {joined},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("audit_t", "val")),
              BinaryOperation::kGreaterThan, ConstantValueExp(Value(10)))});
  RuleSet rules;
  for (const auto& rule : RuleSet::Default().Rules()) {
    if (rule.Name() == "outer_to_inner_join_on_null_rejecting_filter") {
      rules.Add(rule);
    }
  }
  ASSERT_EQ(rules.Rules().size(), 1U);
  const size_t before = memo.GroupCount();
  SearchEngine search(std::move(memo), rules);
  search.Explore(selected);
  EXPECT_EQ(search.GetMemo().GroupCount(), before);
  EXPECT_EQ(search.GetMemo().Get(selected).expressions.size(), 1U);
}

TEST(CascadesTest, OuterToInnerJoinOnNullRejectingFilter) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId oj_group = memo.EnsureDerivedGroup({"t1", "t2"}, "loj");
  memo.AddExpression(
      oj_group, LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                                  .children = {left, right},
                                  .predicate = BinaryExpressionExp(
                                      ColumnValueExp(ColumnName("t1", "id")),
                                      BinaryOperation::kEquals,
                                      ColumnValueExp(ColumnName("t2", "id"))),
                                  .join_type = 0});

  const GroupId sel_group = memo.EnsureDerivedGroup({"t1", "t2"}, "sel_oj");
  memo.AddExpression(
      sel_group, LogicalExpression{.operation = LogicalOperator::kSelection,
                                   .children = {oj_group},
                                   .predicate = BinaryExpressionExp(
                                       ColumnValueExp(ColumnName("t2", "val")),
                                       BinaryOperation::kGreaterThan,
                                       ConstantValueExp(Value(10L)))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  bool found_inner_join = false;
  for (const auto& expr : search.GetMemo().Get(sel_group).expressions) {
    if (expr.operation == LogicalOperator::kSelection &&
        expr.children.size() == 1) {
      const Group& child_group = search.GetMemo().Get(expr.children[0]);
      for (const auto& child_expr : child_group.expressions) {
        if (child_expr.operation == LogicalOperator::kJoin) {
          found_inner_join = true;
          break;
        }
      }
    }
  }
  EXPECT_TRUE(found_inner_join);
}

TEST(CascadesTest, OuterToInnerJoinOnNullRejectingIsNotNull) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId oj_group = memo.EnsureDerivedGroup({"t1", "t2"}, "loj");
  memo.AddExpression(
      oj_group, LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                                  .children = {left, right},
                                  .predicate = BinaryExpressionExp(
                                      ColumnValueExp(ColumnName("t1", "id")),
                                      BinaryOperation::kEquals,
                                      ColumnValueExp(ColumnName("t2", "id"))),
                                  .join_type = 0});

  const GroupId sel_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "sel_is_not_null");
  memo.AddExpression(
      sel_group, LogicalExpression{.operation = LogicalOperator::kSelection,
                                   .children = {oj_group},
                                   .predicate = UnaryExpressionExp(
                                       ColumnValueExp(ColumnName("t2", "val")),
                                       UnaryOperation::kIsNotNull)});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  bool found_inner_join = false;
  for (const auto& expr : search.GetMemo().Get(sel_group).expressions) {
    if (expr.operation == LogicalOperator::kSelection &&
        expr.children.size() == 1) {
      const Group& child_group = search.GetMemo().Get(expr.children[0]);
      for (const auto& child_expr : child_group.expressions) {
        if (child_expr.operation == LogicalOperator::kJoin) {
          found_inner_join = true;
          break;
        }
      }
    }
  }
  EXPECT_TRUE(found_inner_join);
}

TEST(CascadesTest, OuterJoinAssociativity) {
  Memo memo;
  (void)memo.Build({"a", "b", "c"});
  const GroupId a = memo.EnsureGroup({"a"});
  const GroupId b = memo.EnsureGroup({"b"});
  const GroupId c = memo.EnsureGroup({"c"});
  const GroupId ab = memo.EnsureDerivedGroup({"a", "b"}, "ab_loj");
  memo.AddExpression(
      ab, LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                            .children = {a, b},
                            .predicate = BinaryExpressionExp(
                                ColumnValueExp(ColumnName("a", "id")),
                                BinaryOperation::kEquals,
                                ColumnValueExp(ColumnName("b", "id"))),
                            .join_type = 0});

  const GroupId abc = memo.EnsureDerivedGroup({"a", "b", "c"}, "abc_loj");
  memo.AddExpression(
      abc, LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                             .children = {ab, c},
                             .predicate = BinaryExpressionExp(
                                 ColumnValueExp(ColumnName("b", "c_id")),
                                 BinaryOperation::kEquals,
                                 ColumnValueExp(ColumnName("c", "id"))),
                             .join_type = 0});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(abc);

  bool found_associated = false;
  for (const auto& expr : search.GetMemo().Get(abc).expressions) {
    if (expr.operation == LogicalOperator::kOuterJoin &&
        expr.children.size() == 2) {
      if (expr.children[0] == a) {
        found_associated = true;
        break;
      }
    }
  }
  EXPECT_TRUE(found_associated);
}

TEST(CascadesTest, SemiJoinInnerJoinReorder) {
  Memo memo;
  (void)memo.Build({"r", "s", "t"});
  const GroupId r = memo.EnsureGroup({"r"});
  const GroupId s = memo.EnsureGroup({"s"});
  const GroupId t = memo.EnsureGroup({"t"});
  const GroupId rs = memo.EnsureDerivedGroup({"r", "s"}, "rs_semi");
  memo.AddExpression(
      rs, LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                            .children = {r, s},
                            .predicate = BinaryExpressionExp(
                                ColumnValueExp(ColumnName("r", "s_id")),
                                BinaryOperation::kEquals,
                                ColumnValueExp(ColumnName("s", "id")))});

  const GroupId rst = memo.EnsureDerivedGroup({"r", "s", "t"}, "rst_join");
  memo.AddExpression(
      rst, LogicalExpression{.operation = LogicalOperator::kJoin,
                             .children = {rs, t},
                             .predicate = BinaryExpressionExp(
                                 ColumnValueExp(ColumnName("r", "t_id")),
                                 BinaryOperation::kEquals,
                                 ColumnValueExp(ColumnName("t", "id")))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(rst);

  bool found_reordered = false;
  for (const auto& expr : search.GetMemo().Get(rst).expressions) {
    if (expr.operation == LogicalOperator::kSemiJoin &&
        expr.children.size() == 2) {
      if (expr.children[1] == s) {
        found_reordered = true;
        break;
      }
    }
  }
  EXPECT_TRUE(found_reordered);
}

TEST(CascadesTest, SemiJoinCommutativity) {
  Memo memo;
  (void)memo.Build({"r", "s", "t"});
  const GroupId r = memo.EnsureGroup({"r"});
  const GroupId s = memo.EnsureGroup({"s"});
  const GroupId t = memo.EnsureGroup({"t"});
  const GroupId rs = memo.EnsureDerivedGroup({"r", "s"}, "rs_semi");
  memo.AddExpression(
      rs, LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                            .children = {r, s},
                            .predicate = BinaryExpressionExp(
                                ColumnValueExp(ColumnName("r", "s_id")),
                                BinaryOperation::kEquals,
                                ColumnValueExp(ColumnName("s", "id")))});

  const GroupId rst = memo.EnsureDerivedGroup({"r", "s", "t"}, "rst_semi");
  memo.AddExpression(
      rst, LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                             .children = {rs, t},
                             .predicate = BinaryExpressionExp(
                                 ColumnValueExp(ColumnName("r", "t_id")),
                                 BinaryOperation::kEquals,
                                 ColumnValueExp(ColumnName("t", "id")))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(rst);

  bool found_commuted = false;
  for (const auto& expr : search.GetMemo().Get(rst).expressions) {
    if (expr.operation == LogicalOperator::kSemiJoin &&
        expr.children.size() == 2) {
      if (expr.children[1] == s) {
        found_commuted = true;
        break;
      }
    }
  }
  EXPECT_TRUE(found_commuted);
}

TEST(CascadesTest, OuterToInnerJoinNullAllowingPredicateDoesNotRewrite) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId oj_group = memo.EnsureDerivedGroup({"t1", "t2"}, "loj_null_ok");
  memo.AddExpression(
      oj_group, LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                                  .children = {left, right},
                                  .predicate = BinaryExpressionExp(
                                      ColumnValueExp(ColumnName("t1", "id")),
                                      BinaryOperation::kEquals,
                                      ColumnValueExp(ColumnName("t2", "id"))),
                                  .join_type = 0});

  const GroupId sel_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "sel_is_null");
  memo.AddExpression(
      sel_group, LogicalExpression{.operation = LogicalOperator::kSelection,
                                   .children = {oj_group},
                                   .predicate = UnaryExpressionExp(
                                       ColumnValueExp(ColumnName("t2", "val")),
                                       UnaryOperation::kIsNull)});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  bool found_inner_join = false;
  for (const auto& expr : search.GetMemo().Get(sel_group).expressions) {
    if (expr.operation == LogicalOperator::kSelection &&
        expr.children.size() == 1) {
      const Group& child_group = search.GetMemo().Get(expr.children[0]);
      for (const auto& child_expr : child_group.expressions) {
        if (child_expr.operation == LogicalOperator::kJoin) {
          found_inner_join = true;
          break;
        }
      }
    }
  }
  EXPECT_FALSE(found_inner_join);
}

TEST(CascadesTest, OuterToInnerJoinCoalesceDoesNotRewrite) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId oj_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "loj_coalesce");
  memo.AddExpression(
      oj_group, LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                                  .children = {left, right},
                                  .predicate = BinaryExpressionExp(
                                      ColumnValueExp(ColumnName("t1", "id")),
                                      BinaryOperation::kEquals,
                                      ColumnValueExp(ColumnName("t2", "id"))),
                                  .join_type = 0});

  const GroupId sel_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "sel_coalesce");
  memo.AddExpression(
      sel_group,
      LogicalExpression{
          .operation = LogicalOperator::kSelection,
          .children = {oj_group},
          .predicate = BinaryExpressionExp(
              FunctionCallExp("coalesce",
                              {ColumnValueExp(ColumnName("t2", "val")),
                               ConstantValueExp(Value(0L))}),
              BinaryOperation::kEquals, ConstantValueExp(Value(0L)))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  bool found_inner_join = false;
  for (const auto& expr : search.GetMemo().Get(sel_group).expressions) {
    if (expr.operation == LogicalOperator::kSelection &&
        expr.children.size() == 1) {
      const Group& child_group = search.GetMemo().Get(expr.children[0]);
      for (const auto& child_expr : child_group.expressions) {
        if (child_expr.operation == LogicalOperator::kJoin) {
          found_inner_join = true;
          break;
        }
      }
    }
  }
  EXPECT_FALSE(found_inner_join);
}

TEST(CascadesTest, NonRejectingRightFilterKeepsEveryOuterKind) {
  for (uint8_t kind : {uint8_t{0}, uint8_t{1}, uint8_t{2}}) {
    SCOPED_TRACE(kind);
    Memo memo;
    (void)memo.Build({"t1", "t2"});
    const GroupId left = memo.EnsureGroup({"t1"});
    const GroupId right = memo.EnsureGroup({"t2"});
    const GroupId outer = memo.EnsureDerivedGroup({"t1", "t2"}, "outer");
    memo.AddExpression(
        outer, LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                                 .children = {left, right},
                                 .predicate = BinaryExpressionExp(
                                     ColumnValueExp(ColumnName("t1", "id")),
                                     BinaryOperation::kEquals,
                                     ColumnValueExp(ColumnName("t2", "id"))),
                                 .join_type = kind});
    const GroupId selection =
        memo.EnsureDerivedGroup({"t1", "t2"}, "selection");
    memo.AddExpression(
        selection,
        LogicalExpression{
            .operation = LogicalOperator::kSelection,
            .children = {outer},
            .predicate = BinaryExpressionExp(
                FunctionCallExp("coalesce",
                                {ColumnValueExp(ColumnName("t2", "val")),
                                 ConstantValueExp(Value(int64_t{0}))}),
                BinaryOperation::kEquals,
                ConstantValueExp(Value(int64_t{0})))});
    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(selection);
    bool found_inner = false;
    for (const LogicalExpression& expression :
         search.GetMemo().Get(selection).expressions) {
      if (expression.operation != LogicalOperator::kSelection ||
          expression.children.size() != 1) {
        continue;
      }
      for (const LogicalExpression& child :
           search.GetMemo().Get(expression.children[0]).expressions) {
        found_inner = found_inner || child.operation == LogicalOperator::kJoin;
      }
    }
    EXPECT_FALSE(found_inner);
  }
}

TEST(CascadesTest, OuterJoinAssociativityRejectsInvalidPredicate) {
  Memo memo;
  (void)memo.Build({"a", "b", "c"});
  const GroupId a = memo.EnsureGroup({"a"});
  const GroupId b = memo.EnsureGroup({"b"});
  const GroupId c = memo.EnsureGroup({"c"});
  const GroupId ab = memo.EnsureDerivedGroup({"a", "b"}, "ab_loj_invalid");
  memo.AddExpression(
      ab, LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                            .children = {a, b},
                            .predicate = BinaryExpressionExp(
                                ColumnValueExp(ColumnName("a", "id")),
                                BinaryOperation::kEquals,
                                ColumnValueExp(ColumnName("b", "id"))),
                            .join_type = 0});

  const GroupId abc =
      memo.EnsureDerivedGroup({"a", "b", "c"}, "abc_loj_invalid");
  memo.AddExpression(
      abc, LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                             .children = {ab, c},
                             .predicate = BinaryExpressionExp(
                                 ColumnValueExp(ColumnName("a", "id")),
                                 BinaryOperation::kEquals,
                                 ColumnValueExp(ColumnName("c", "id"))),
                             .join_type = 0});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(abc);

  bool found_associated = false;
  for (const auto& expr : search.GetMemo().Get(abc).expressions) {
    if (expr.operation == LogicalOperator::kOuterJoin &&
        expr.children.size() == 2) {
      if (expr.children[0] == a) {
        found_associated = true;
        break;
      }
    }
  }
  EXPECT_FALSE(found_associated);
}

TEST(CascadesTest, LogicalPropertiesAggregationAndLimitOne) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId t1 = memo.EnsureGroup({"t1"});

  const GroupId agg_group = memo.EnsureDerivedGroup({"t1"}, "agg_grouped");
  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {t1},
          .target_list = {NamedExpression(
              "dept", ColumnValueExp(ColumnName("t1", "dept_id")))},
          .grouping_sets = {ColumnValueExp(ColumnName("t1", "dept_id"))}});
  EXPECT_TRUE(
      memo.Get(agg_group).logical_properties.IsUniqueOn({"t1.dept_id"}));
  EXPECT_FALSE(memo.Get(agg_group).logical_properties.max_1_row);

  const GroupId scalar_agg = memo.EnsureDerivedGroup({"t1"}, "agg_scalar");
  memo.AddExpression(
      scalar_agg,
      LogicalExpression{.operation = LogicalOperator::kAggregation,
                        .children = {t1},
                        .target_list = {NamedExpression(
                            "cnt", ColumnValueExp(ColumnName("t1", "id")))}});
  EXPECT_TRUE(memo.Get(scalar_agg).logical_properties.max_1_row);
  EXPECT_TRUE(
      memo.Get(scalar_agg).logical_properties.IsUniqueOn({"any_column"}));

  const GroupId limit_group = memo.EnsureDerivedGroup({"t1"}, "limit_one");
  memo.AddExpression(limit_group,
                     LogicalExpression{.operation = LogicalOperator::kLimit,
                                       .children = {t1},
                                       .limit_count = 1});
  EXPECT_TRUE(memo.Get(limit_group).logical_properties.max_1_row);
}

TEST(CascadesTest, PushSelectionThroughWindowOnPartitionColumns) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId win_group = memo.EnsureDerivedGroup({"t1"}, "win");
  memo.AddExpression(
      win_group,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {scan},
          .target_list = {NamedExpression(
              "rn", ColumnValueExp(ColumnName("t1", "id")))},
          .partition_by = {ColumnValueExp(ColumnName("t1", "part_col"))}});

  const GroupId sel_group = memo.EnsureDerivedGroup({"t1"}, "sel_win");
  memo.AddExpression(
      sel_group,
      LogicalExpression{
          .operation = LogicalOperator::kSelection,
          .children = {win_group},
          .predicate = BinaryExpressionExp(
              BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "part_col")),
                                  BinaryOperation::kEquals,
                                  ConstantValueExp(Value(int64_t{10}))),
              BinaryOperation::kAnd,
              BinaryExpressionExp(ColumnValueExp(ColumnName("t1", "rn")),
                                  BinaryOperation::kLessThanEquals,
                                  ConstantValueExp(Value(int64_t{5}))))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  bool found_pushed_selection = false;
  for (const auto& expr : search.GetMemo().Get(sel_group).expressions) {
    if (expr.operation == LogicalOperator::kSelection &&
        expr.children.size() == 1) {
      const Group& win_g = search.GetMemo().Get(expr.children[0]);
      for (const auto& win_expr : win_g.expressions) {
        if (win_expr.operation == LogicalOperator::kWindow &&
            win_expr.children.size() == 1) {
          const Group& child_g = search.GetMemo().Get(win_expr.children[0]);
          for (const auto& c_expr : child_g.expressions) {
            if (c_expr.operation == LogicalOperator::kSelection) {
              found_pushed_selection = true;
              break;
            }
          }
        }
      }
    }
  }
  EXPECT_TRUE(found_pushed_selection);
}

TEST(CascadesTest, PushLimitThroughUnionAll) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId t1 = memo.EnsureGroup({"t1"});
  const GroupId t2 = memo.EnsureGroup({"t2"});
  const GroupId union_group = memo.EnsureDerivedGroup({"t1", "t2"}, "union");
  memo.AddExpression(union_group,
                     LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                       .children = {t1, t2}});

  const GroupId limit_group = memo.EnsureDerivedGroup({"t1", "t2"}, "limit");
  memo.AddExpression(limit_group,
                     LogicalExpression{.operation = LogicalOperator::kLimit,
                                       .children = {union_group},
                                       .limit_count = 5,
                                       .limit_offset = 2});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(limit_group);

  bool found_pushed_limit = false;
  for (const auto& expr : search.GetMemo().Get(limit_group).expressions) {
    if (expr.operation == LogicalOperator::kLimit &&
        expr.children.size() == 1) {
      const Group& ug = search.GetMemo().Get(expr.children[0]);
      for (const auto& u_expr : ug.expressions) {
        if (u_expr.operation == LogicalOperator::kUnionAll &&
            u_expr.children.size() == 2) {
          const Group& c1 = search.GetMemo().Get(u_expr.children[0]);
          const Group& c2 = search.GetMemo().Get(u_expr.children[1]);
          bool c1_limited = false;
          bool c2_limited = false;
          for (const auto& e : c1.expressions) {
            if (e.operation == LogicalOperator::kLimit && e.limit_count == 7) {
              c1_limited = true;
              break;
            }
          }
          for (const auto& e : c2.expressions) {
            if (e.operation == LogicalOperator::kLimit && e.limit_count == 7) {
              c2_limited = true;
              break;
            }
          }
          if (c1_limited && c2_limited) {
            found_pushed_limit = true;
            break;
          }
        }
      }
    }
  }
  EXPECT_TRUE(found_pushed_limit);
}

TEST(CascadesTest, InferFilterFromEquivalenceClass) {
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
              ColumnValueExp(ColumnName("t1", "id")), BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id")))});

  const GroupId sel_group = memo.EnsureDerivedGroup({"t1", "t2"}, "sel");
  memo.AddExpression(
      sel_group, LogicalExpression{.operation = LogicalOperator::kSelection,
                                   .children = {join_group},
                                   .predicate = BinaryExpressionExp(
                                       ColumnValueExp(ColumnName("t1", "id")),
                                       BinaryOperation::kGreaterThan,
                                       ConstantValueExp(Value(int64_t{10})))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  bool found_inferred_predicate = false;
  for (const auto& expr : search.GetMemo().Get(sel_group).expressions) {
    if (expr.operation == LogicalOperator::kSelection && expr.predicate &&
        *expr.predicate) {
      const std::string pred_str = (*expr.predicate)->ToString();
      if (pred_str.find("t2.id") != std::string::npos &&
          pred_str.find("t1.id") != std::string::npos) {
        found_inferred_predicate = true;
        break;
      }
    }
  }
  EXPECT_TRUE(found_inferred_predicate);
}

// The recorded classes are pairwise ({a,b} from one join expression, {b,c}
// from another). The inference rule merges them, so a filter on `a` must
// reach `c` through the chain without any direct a=c equality.
TEST(CascadesTest, InferFilterFromEquivalenceClassTransitiveChain) {
  Memo memo;
  (void)memo.Build({"t1", "t2", "t3"});
  const GroupId t1 = memo.EnsureGroup({"t1"});
  const GroupId t2 = memo.EnsureGroup({"t2"});
  const GroupId t3 = memo.EnsureGroup({"t3"});

  const GroupId join_12 = memo.EnsureDerivedGroup({"t1", "t2"}, "join_12");
  memo.AddExpression(
      join_12, LogicalExpression{.operation = LogicalOperator::kJoin,
                                 .children = {t1, t2},
                                 .predicate = BinaryExpressionExp(
                                     ColumnValueExp(ColumnName("t1", "id")),
                                     BinaryOperation::kEquals,
                                     ColumnValueExp(ColumnName("t2", "id")))});

  const GroupId join_group =
      memo.EnsureDerivedGroup({"t1", "t2", "t3"}, "join_all");
  memo.AddExpression(
      join_group,
      LogicalExpression{
          .operation = LogicalOperator::kJoin,
          .children = {join_12, t3},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t2", "id")), BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t3", "id")))});

  const GroupId sel_group = memo.EnsureDerivedGroup({"t1", "t2", "t3"}, "sel");
  memo.AddExpression(
      sel_group, LogicalExpression{.operation = LogicalOperator::kSelection,
                                   .children = {join_group},
                                   .predicate = BinaryExpressionExp(
                                       ColumnValueExp(ColumnName("t1", "id")),
                                       BinaryOperation::kGreaterThan,
                                       ConstantValueExp(Value(int64_t{10})))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  bool found_t3_inferred = false;
  for (const auto& expr : search.GetMemo().Get(sel_group).expressions) {
    if (expr.operation == LogicalOperator::kSelection && expr.predicate &&
        *expr.predicate) {
      const std::string pred_str = (*expr.predicate)->ToString();
      if (pred_str.find("t3.id") != std::string::npos) {
        found_t3_inferred = true;
        break;
      }
    }
  }
  EXPECT_TRUE(found_t3_inferred);
}

TEST(CascadesTest, PushSemiJoinThroughInnerJoin) {
  Memo memo;
  (void)memo.Build({"t1", "t2", "t3"});
  const GroupId t1 = memo.EnsureGroup({"t1"});
  const GroupId t2 = memo.EnsureGroup({"t2"});
  const GroupId t3 = memo.EnsureGroup({"t3"});

  const GroupId inner_join = memo.EnsureDerivedGroup({"t1", "t2"}, "join_12");
  memo.AddExpression(
      inner_join,
      LogicalExpression{
          .operation = LogicalOperator::kJoin,
          .children = {t1, t2},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t1", "id")), BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id")))});

  const GroupId semi_group =
      memo.EnsureDerivedGroup({"t1", "t2", "t3"}, "semi_12_3");
  memo.AddExpression(
      semi_group,
      LogicalExpression{
          .operation = LogicalOperator::kSemiJoin,
          .children = {inner_join, t3},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t1", "id")), BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t3", "id")))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(semi_group);

  bool found_pushed_semi = false;
  for (const auto& expr : search.GetMemo().Get(semi_group).expressions) {
    if (expr.operation == LogicalOperator::kJoin && expr.children.size() == 2) {
      const Group& child_left = search.GetMemo().Get(expr.children[0]);
      for (const auto& child_expr : child_left.expressions) {
        if (child_expr.operation == LogicalOperator::kSemiJoin &&
            child_expr.children.size() == 2 && child_expr.children[0] == t1 &&
            child_expr.children[1] == t3) {
          found_pushed_semi = true;
          break;
        }
      }
    }
  }
  EXPECT_TRUE(found_pushed_semi);
}

TEST(CascadesTest, PushLimitThroughLeftJoinRejectsUnprovenComparisonTypes) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const Schema keyed("t2", {Column("id", ValueType::kInt64,
                                   Constraint(Constraint::kPrimaryKey))});
  memo.SetTableSchemas({{"t2", keyed}});
  memo.MarkProvenOneToOne("t1", "t2");
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});

  const GroupId ojoin_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "left_join");
  memo.AddExpression(
      ojoin_group,
      LogicalExpression{
          .operation = LogicalOperator::kOuterJoin,
          .children = {left, right},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t1", "fk")), BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id"))),
          .join_type = 0});

  const GroupId limit_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "limit_ojoin");
  memo.AddExpression(limit_group,
                     LogicalExpression{.operation = LogicalOperator::kLimit,
                                       .children = {ojoin_group},
                                       .limit_count = 5,
                                       .limit_offset = 0});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(limit_group);

  bool found_pushed_limit = false;
  for (const auto& expr : search.GetMemo().Get(limit_group).expressions) {
    if (expr.operation == LogicalOperator::kLimit &&
        expr.children.size() == 1) {
      const Group& ojoin_child = search.GetMemo().Get(expr.children[0]);
      for (const auto& ojoin_expr : ojoin_child.expressions) {
        if (ojoin_expr.operation == LogicalOperator::kOuterJoin &&
            ojoin_expr.children.size() == 2) {
          const Group& left_child =
              search.GetMemo().Get(ojoin_expr.children[0]);
          for (const auto& left_expr : left_child.expressions) {
            if (left_expr.operation == LogicalOperator::kLimit &&
                left_expr.limit_count == 5 &&
                left_expr.children == std::vector<GroupId>{left}) {
              found_pushed_limit = true;
              break;
            }
          }
        }
      }
    }
  }
  // Schema-free comparison analysis cannot prove compatible operand types.
  // Reducing input evaluation must not erase an error in a later join row.
  EXPECT_FALSE(found_pushed_limit);
}

TEST(CascadesTest, PushLimitThroughLeftJoinRequiresContainmentProof) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const Schema keyed("t2", {Column("id", ValueType::kInt64,
                                   Constraint(Constraint::kPrimaryKey))});
  memo.SetTableSchemas({{"t2", keyed}});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId ojoin_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "left_join_without_proof");
  ASSERT_TRUE(memo.AddExpression(
      ojoin_group,
      LogicalExpression{
          .operation = LogicalOperator::kOuterJoin,
          .children = {left, right},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("t1", "fk")), BinaryOperation::kEquals,
              ColumnValueExp(ColumnName("t2", "id"))),
          .join_type = 0}));
  const GroupId limit_group =
      memo.EnsureDerivedGroup({"t1", "t2"}, "limit_without_proof");
  ASSERT_TRUE(memo.AddExpression(
      limit_group, LogicalExpression{.operation = LogicalOperator::kLimit,
                                     .children = {ojoin_group},
                                     .limit_count = 5,
                                     .limit_offset = 0}));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(limit_group);
  for (const LogicalExpression& limit :
       search.GetMemo().Get(limit_group).expressions) {
    if (limit.operation != LogicalOperator::kLimit ||
        limit.children.size() != 1) {
      continue;
    }
    const Group& child = search.GetMemo().Get(limit.children.front());
    for (const LogicalExpression& outer : child.expressions) {
      if (outer.operation != LogicalOperator::kOuterJoin ||
          outer.children.size() != 2) {
        continue;
      }
      const Group& left_child = search.GetMemo().Get(outer.children[0]);
      EXPECT_TRUE(std::ranges::none_of(
          left_child.expressions, [](const LogicalExpression& item) {
            return item.operation == LogicalOperator::kLimit;
          }));
    }
  }
}

TEST(CascadesTest, MergeAdjacentWindows) {
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId inner_win = memo.EnsureDerivedGroup({"t"}, "inner_win");
  memo.AddExpression(
      inner_win,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {scan},
          .target_list = {NamedExpression(
              "w1", ColumnValueExp(ColumnName("t", "v1")))},
          .sort_ascending = {true},
          .sort_nulls_first = {false},
          .partition_by = {ColumnValueExp(ColumnName("t", "dept"))}});

  const GroupId outer_win = memo.EnsureDerivedGroup({"t"}, "outer_win");
  memo.AddExpression(
      outer_win,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {inner_win},
          .target_list = {NamedExpression(
              "w2", ColumnValueExp(ColumnName("t", "v2")))},
          .sort_ascending = {true},
          .sort_nulls_first = {false},
          .partition_by = {ColumnValueExp(ColumnName("t", "dept"))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(outer_win);

  bool found_merged = false;
  for (const auto& expr : search.GetMemo().Get(outer_win).expressions) {
    if (expr.operation == LogicalOperator::kWindow &&
        expr.children == std::vector<GroupId>{scan} &&
        expr.target_list.size() == 2) {
      found_merged = true;
      break;
    }
  }
  EXPECT_TRUE(found_merged);
}

TEST(CascadesTest, PushNotThroughExpression) {
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId sel_group = memo.EnsureDerivedGroup({"t"}, "sel_not");
  memo.AddExpression(
      sel_group,
      LogicalExpression{
          .operation = LogicalOperator::kSelection,
          .children = {scan},
          .predicate = UnaryExpressionExp(
              BinaryExpressionExp(ColumnValueExp(ColumnName("t", "v")),
                                  BinaryOperation::kEquals,
                                  ConstantValueExp(Value(int64_t{10}))),
              UnaryOperation::kNot)});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  bool found_normalized = false;
  for (const auto& expr : search.GetMemo().Get(sel_group).expressions) {
    if (expr.operation == LogicalOperator::kSelection && expr.predicate &&
        *expr.predicate) {
      if ((*expr.predicate)->Type() == TypeTag::kBinaryExp) {
        const auto& bin = (*expr.predicate)->AsBinaryExpression();
        if (bin.Op() == BinaryOperation::kNotEquals) {
          found_normalized = true;
          break;
        }
      }
    }
  }
  EXPECT_TRUE(found_normalized);
}

TEST(CascadesTest, PushNotThroughExpressionPreservesOrderedComparison) {
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId sel_group = memo.EnsureDerivedGroup({"t"}, "sel_ordered_not");
  memo.AddExpression(
      sel_group,
      LogicalExpression{
          .operation = LogicalOperator::kSelection,
          .children = {scan},
          .predicate = UnaryExpressionExp(
              BinaryExpressionExp(ColumnValueExp(ColumnName("t", "v")),
                                  BinaryOperation::kLessThan,
                                  ConstantValueExp(Value(int64_t{10}))),
              UnaryOperation::kNot)});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  bool found_rewritten = false;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(sel_group).expressions) {
    if (expression.operation == LogicalOperator::kSelection &&
        expression.predicate && *expression.predicate &&
        (*expression.predicate)->Type() == TypeTag::kBinaryExp &&
        (*expression.predicate)->AsBinaryExpression().Op() ==
            BinaryOperation::kGreaterThanEquals) {
      found_rewritten = true;
    }
  }
  EXPECT_FALSE(found_rewritten);
}

TEST(CascadesTest, JoinIdentityDummyEliminatesDummySide) {
  Memo memo;
  (void)memo.Build({"a"});
  const GroupId left = memo.EnsureGroup({"a"});
  const GroupId dummy = memo.EnsureDerivedGroup({}, "dummy");
  ASSERT_TRUE(memo.AddExpression(
      dummy, LogicalExpression{.operation = LogicalOperator::kDummyScan}));
  const GroupId cross = memo.EnsureDerivedGroup({"a"}, "cross_dummy");
  ASSERT_TRUE(memo.AddExpression(
      cross, LogicalExpression{.operation = LogicalOperator::kCrossJoin,
                               .children = {left, dummy}}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(cross);
  // The DummyScan side contributes one row with no columns, so the scan
  // alternatives of {a} must appear in the cross group.
  const bool scan_present = std::ranges::any_of(
      search.GetMemo().Get(cross).expressions,
      [](const LogicalExpression& expression) {
        return expression.operation == LogicalOperator::kScan;
      });
  EXPECT_TRUE(scan_present);
}

TEST(CascadesTest, CountStarRewriteOnNotNullColumn) {
  Memo memo;
  const Schema schema(
      "t", {Column("c", ValueType::kInt64, Constraint(Constraint::kNotNull))});
  memo.SetTableSchemas({{"t", schema}});
  (void)memo.Build({"t"});
  const GroupId input = memo.EnsureGroup({"t"});
  const GroupId agg = memo.EnsureDerivedGroup({"t"}, "agg_count");
  ASSERT_TRUE(memo.AddExpression(
      agg,
      LogicalExpression{.operation = LogicalOperator::kAggregation,
                        .children = {input},
                        .target_list = {NamedExpression(
                            "n", AggregateExpressionExp(
                                     AggregationType::kCount,
                                     ColumnValueExp(ColumnName("t", "c"))))}}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg);
  bool found_star = false;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(agg).expressions) {
    if (expression.operation != LogicalOperator::kAggregation) {
      continue;
    }
    for (const NamedExpression& target : expression.target_list) {
      if (target.expression &&
          target.expression->Type() == TypeTag::kAggregateExp &&
          target.expression->AsAggregateExpression().GetType() ==
              AggregationType::kCount &&
          !target.expression->AsAggregateExpression().Child()) {
        found_star = true;
      }
    }
  }
  EXPECT_TRUE(found_star);
}

TEST(CascadesTest, CountStarRewriteKeepsNullableColumn) {
  Memo memo;
  const Schema schema("t", {Column("c", ValueType::kInt64)});
  memo.SetTableSchemas({{"t", schema}});
  (void)memo.Build({"t"});
  const GroupId input = memo.EnsureGroup({"t"});
  const GroupId agg = memo.EnsureDerivedGroup({"t"}, "agg_count_null");
  ASSERT_TRUE(memo.AddExpression(
      agg,
      LogicalExpression{.operation = LogicalOperator::kAggregation,
                        .children = {input},
                        .target_list = {NamedExpression(
                            "n", AggregateExpressionExp(
                                     AggregationType::kCount,
                                     ColumnValueExp(ColumnName("t", "c"))))}}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg);
  const bool found_star = std::ranges::any_of(
      search.GetMemo().Get(agg).expressions,
      [](const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kAggregation) {
          return false;
        }
        return std::ranges::any_of(
            expression.target_list, [](const NamedExpression& target) {
              return target.expression &&
                     target.expression->Type() == TypeTag::kAggregateExp &&
                     target.expression->AsAggregateExpression().GetType() ==
                         AggregationType::kCount &&
                     !target.expression->AsAggregateExpression().Child();
            });
      });
  EXPECT_FALSE(found_star);
}

TEST(CascadesTest, TopNPushThroughInnerJoinOnForeignKey) {
  // DISABLED-RULE counterexample (docs/cascades_optimizer.md):
  // topn_push_through_inner_join pushed TopN(Join(L, R)) ->
  // Join(TopN(L), R) when the right keys were unique and the left keys NOT
  // NULL.  That gate proves "at most one match per left row", not "exactly
  // one": with L = {2, 3, 4, 5, 6} and R = {5, 6}, TopN(L, a ASC, 1) = {2}
  // joins to zero rows, while the correct first join output is a = 5.
  // The rule must stay disabled until ">= 1 match per left row" is provable
  // from memo metadata.
  Memo memo;
  const Schema orders_schema(
      "orders",
      {Column("id", ValueType::kInt64, Constraint(Constraint::kPrimaryKey)),
       Column("user_id", ValueType::kInt64, Constraint(Constraint::kNotNull))});
  const Schema users_schema(
      "users",
      {Column("id", ValueType::kInt64, Constraint(Constraint::kPrimaryKey))});
  memo.SetTableSchemas({{"orders", orders_schema}, {"users", users_schema}});
  const Expression join_key = BinaryExpressionExp(
      ColumnValueExp(ColumnName("orders", "user_id")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("users", "id")));
  const GroupId root =
      memo.Build({"orders", "users"}, {{join_key, {"orders", "users"}}});
  const GroupId topn = memo.EnsureDerivedGroup({"orders", "users"}, "topn_fk");
  ASSERT_TRUE(memo.AddExpression(
      topn, LogicalExpression{
                .operation = LogicalOperator::kTopN,
                .children = {root},
                .target_list = {NamedExpression(
                    "", ColumnValueExp(ColumnName("orders", "user_id")))},
                .sort_ascending = {true},
                .limit_count = 5,
                .limit_offset = 0}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(topn);
  bool found_pushed = false;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(topn).expressions) {
    if (expression.operation != LogicalOperator::kJoin ||
        expression.children.size() != 2) {
      continue;
    }
    for (const LogicalExpression& child :
         search.GetMemo().Get(expression.children[0]).expressions) {
      if (child.operation == LogicalOperator::kTopN) {
        found_pushed = true;
      }
    }
  }
  EXPECT_FALSE(found_pushed);
}

TEST(CascadesTest, SplitWindowSeparatesPartitionSpecs) {
  Memo memo;
  (void)memo.Build({"t"});
  const GroupId input = memo.EnsureGroup({"t"});
  const Expression first =
      WindowFunctionCallExp("sum", {ColumnValueExp(ColumnName("t", "x"))},
                            {ColumnValueExp(ColumnName("t", "p"))}, {});
  const Expression second =
      WindowFunctionCallExp("sum", {ColumnValueExp(ColumnName("t", "y"))},
                            {ColumnValueExp(ColumnName("t", "q"))}, {});
  const GroupId window = memo.EnsureDerivedGroup({"t"}, "window_split");
  ASSERT_TRUE(memo.AddExpression(
      window, LogicalExpression{
                  .operation = LogicalOperator::kWindow,
                  .children = {input},
                  .target_list = {NamedExpression("s1", first),
                                  NamedExpression("s2", second)},
                  .partition_by = {ColumnValueExp(ColumnName("t", "p"))}}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(window);
  // The outer alternative keeps one spec; the other spec must appear in a
  // stacked window below it.
  bool found_stacked = false;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(window).expressions) {
    if (expression.operation != LogicalOperator::kWindow ||
        expression.children.size() != 1 || expression.target_list.size() != 1) {
      continue;
    }
    for (const LogicalExpression& child :
         search.GetMemo().Get(expression.children[0]).expressions) {
      if (child.operation == LogicalOperator::kWindow &&
          child.target_list.size() == 1) {
        found_stacked = true;
      }
    }
  }
  EXPECT_TRUE(found_stacked);
}

TEST(CascadesTest, ValuesFoldIntoUnionAll) {
  Memo memo;
  (void)memo.Build({"t"});
  const Schema schema("", {Column("x", ValueType::kInt64)});
  const GroupId left = memo.EnsureDerivedGroup({"t"}, "values_left");
  ASSERT_TRUE(memo.AddExpression(
      left, LogicalExpression{.operation = LogicalOperator::kValues,
                              .values = {Row({Value(1)})},
                              .output_schema = schema}));
  const GroupId right = memo.EnsureDerivedGroup({"t"}, "values_right");
  ASSERT_TRUE(memo.AddExpression(
      right, LogicalExpression{.operation = LogicalOperator::kValues,
                               .values = {Row({Value(2)})},
                               .output_schema = schema}));
  const GroupId union_group = memo.EnsureDerivedGroup({"t"}, "union_fold");
  ASSERT_TRUE(memo.AddExpression(
      union_group, LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                     .children = {left, right}}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(union_group);
  bool found_folded = false;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(union_group).expressions) {
    if (expression.operation == LogicalOperator::kValues &&
        expression.values.size() == 2) {
      found_folded = true;
    }
  }
  EXPECT_TRUE(found_folded);
}

TEST(CascadesTest, OuterNestedLoopPlansNonEquiLeftJoin) {
  Memo memo;
  (void)memo.Build({"a", "b"});
  const GroupId left = memo.EnsureGroup({"a"});
  ASSERT_TRUE(memo.AddExpression(
      left, LogicalExpression{
                .operation = LogicalOperator::kValues,
                .values = {Row({Value(1)})},
                .output_schema = Schema(
                    "", {Column(ColumnName("a", "x"), ValueType::kInt64)})}));
  const GroupId right = memo.EnsureGroup({"b"});
  ASSERT_TRUE(memo.AddExpression(
      right, LogicalExpression{
                 .operation = LogicalOperator::kValues,
                 .values = {Row({Value(2)})},
                 .output_schema = Schema(
                     "", {Column(ColumnName("b", "y"), ValueType::kInt64)})}));
  const GroupId outer = memo.EnsureDerivedGroup({"a", "b"}, "outer_nl");
  ASSERT_TRUE(memo.AddExpression(
      outer, LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                               .children = {left, right},
                               .predicate = BinaryExpressionExp(
                                   ColumnValueExp(ColumnName("a", "x")),
                                   BinaryOperation::kLessThan,
                                   ColumnValueExp(ColumnName("b", "y"))),
                               .join_type = 0}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  // Scans need catalog tables; keep only table-free implementations so the
  // single-relation children plan through their Values alternatives.
  ImplementationRuleSet rules;
  for (const ImplementationRule& rule :
       tinylamb::DefaultImplementationRules().Rules()) {
    // No catalog tables here (scans need them), and the hash/merge outer
    // rule cannot serve a non-equi predicate, so the only remaining outer
    // implementation is the nested loop.
    if (rule.Name() != "index_scan" && rule.Name() != "full_scan" &&
        rule.Name() != "outer_hash_join") {
      rules.Add(rule);
    }
  }
  const std::optional<BestPlan> best =
      search.Optimize(outer, PhysicalProperties{}, rules);
  ASSERT_TRUE(best.has_value());
  if (!best) {
    return;
  }
  EXPECT_NE(best->plan->ToString().find("Left Outer Join"), std::string::npos);
}

TEST(CascadesTest, AppliedRuleNamesTrackFiredRules) {
  Memo memo;
  const GroupId root = memo.Build({"a", "b"});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);
  EXPECT_TRUE(search.AppliedRuleNames().contains("join_commutativity"));
}

TEST(CascadesTest, SearchStepBudgetDegradesToBestSoFar) {
  Memo memo;
  const GroupId root = memo.Build({"a", "b", "c"});
  SearchEngine limited(std::move(memo), RuleSet::Default());
  limited.SetStepBudget(3);
  limited.Explore(root);
  EXPECT_TRUE(limited.BudgetExhausted());
  EXPECT_FALSE(limited.ExploreLimitReached());

  Memo full_memo;
  const GroupId full_root = full_memo.Build({"a", "b", "c"});
  SearchEngine full(std::move(full_memo), RuleSet::Default());
  full.Explore(full_root);
  EXPECT_FALSE(full.BudgetExhausted());
  // The budgeted exploration still covers the initial join tree.
  EXPECT_GE(limited.GetMemo().ExpressionCount(root), 1U);
}

TEST(CascadesTest, ExtractYearSargableAddsScanRange) {
  Memo memo;
  const Schema schema("events", {Column("d", ValueType::kDate)});
  memo.SetTableSchemas({{"events", schema}});
  (void)memo.Build({"events"});
  const GroupId input = memo.EnsureGroup({"events"});
  const Expression year_of_d = FunctionCallExp(
      "extract_year", {ColumnValueExp(ColumnName("events", "d"))});
  const GroupId sel = memo.EnsureDerivedGroup({"events"}, "sel_year");
  ASSERT_TRUE(memo.AddExpression(
      sel, LogicalExpression{.operation = LogicalOperator::kSelection,
                             .children = {input},
                             .predicate = BinaryExpressionExp(
                                 year_of_d, BinaryOperation::kEquals,
                                 ConstantValueExp(Value(int64_t{2024})))}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel);
  // The year predicate stays (residual) while the scan filter gains the
  // half-open date range on top of any pushed extract conjunct.
  const Expression filter = search.GetMemo().Get(input).filter;
  ASSERT_TRUE(filter);
  EXPECT_GE(SplitConjuncts(filter).size(), 2U);
  EXPECT_NE(filter->ToString().find("2024"), std::string::npos);
}

TEST(CascadesTest, ComparisonSelfPredicatesFold) {
  Memo memo;
  const Schema schema("t", {Column("c", ValueType::kInt64)});
  memo.SetTableSchemas({{"t", schema}});
  (void)memo.Build({"t"});
  const GroupId input = memo.EnsureGroup({"t"});
  const GroupId sel = memo.EnsureDerivedGroup({"t"}, "sel_self");
  ASSERT_TRUE(memo.AddExpression(
      sel, LogicalExpression{.operation = LogicalOperator::kSelection,
                             .children = {input},
                             .predicate = BinaryExpressionExp(
                                 ColumnValueExp(ColumnName("t", "c")),
                                 BinaryOperation::kEquals,
                                 ColumnValueExp(ColumnName("t", "c")))}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel);
  bool found_null_test = false;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(sel).expressions) {
    if (expression.operation != LogicalOperator::kSelection ||
        !expression.predicate || !*expression.predicate) {
      continue;
    }
    for (const Expression& conjunct : SplitConjuncts(*expression.predicate)) {
      if (conjunct && conjunct->Type() == TypeTag::kUnaryExp &&
          conjunct->AsUnaryExpression().Op() == UnaryOperation::kIsNotNull) {
        found_null_test = true;
      }
    }
  }
  EXPECT_TRUE(found_null_test);
}

TEST(CascadesTest, ComparisonSelfPredicatesSkipFloats) {
  Memo memo;
  const Schema schema("t", {Column("f", ValueType::kDouble)});
  memo.SetTableSchemas({{"t", schema}});
  (void)memo.Build({"t"});
  const GroupId input = memo.EnsureGroup({"t"});
  const GroupId sel = memo.EnsureDerivedGroup({"t"}, "sel_self_float");
  ASSERT_TRUE(memo.AddExpression(
      sel, LogicalExpression{.operation = LogicalOperator::kSelection,
                             .children = {input},
                             .predicate = BinaryExpressionExp(
                                 ColumnValueExp(ColumnName("t", "f")),
                                 BinaryOperation::kEquals,
                                 ColumnValueExp(ColumnName("t", "f")))}));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel);
  // NaN breaks reflexivity, so no IS NOT NULL alternative may appear.
  const bool found_null_test = std::ranges::any_of(
      search.GetMemo().Get(sel).expressions,
      [](const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kSelection ||
            !expression.predicate || !*expression.predicate) {
          return false;
        }
        return std::ranges::any_of(SplitConjuncts(*expression.predicate),
                                   [](const Expression& conjunct) {
                                     return conjunct && conjunct->Type() ==
                                                            TypeTag::kUnaryExp;
                                   });
      });
  EXPECT_FALSE(found_null_test);
}

TEST(CascadesTest, TopNWithTiesCarriesPayload) {
  Memo memo;
  (void)memo.Build({"t"});
  const GroupId input = memo.EnsureGroup({"t"});
  const GroupId topn = memo.EnsureDerivedGroup({"t"}, "topn_ties");
  LogicalExpression with_ties{.operation = LogicalOperator::kTopN,
                              .children = {input},
                              .target_list = {NamedExpression(
                                  "", ColumnValueExp(ColumnName("t", "x")))},
                              .sort_ascending = {true},
                              .limit_count = 2,
                              .limit_offset = 0,
                              .with_ties = true};
  LogicalExpression without_ties = with_ties;
  without_ties.with_ties = false;
  EXPECT_NE(with_ties.Fingerprint(), without_ties.Fingerprint());
  ASSERT_TRUE(memo.AddExpression(topn, std::move(with_ties)));
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(topn);
  const bool ties_kept = std::ranges::any_of(
      search.GetMemo().Get(topn).expressions,
      [](const LogicalExpression& expression) {
        return expression.operation == LogicalOperator::kTopN &&
               expression.with_ties;
      });
  EXPECT_TRUE(ties_kept);
}

TEST(CascadesTest, ExploreTwiceReachesSameFixpoint) {
  Memo memo;
  const GroupId root = memo.Build({"a", "b", "c"});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(root);
  const size_t groups_once = search.GetMemo().GroupCount();
  const size_t root_exprs_once = search.GetMemo().ExpressionCount(root);
  search.Explore(root);
  EXPECT_EQ(search.GetMemo().GroupCount(), groups_once);
  EXPECT_EQ(search.GetMemo().ExpressionCount(root), root_exprs_once);
}

TEST(CascadesTest, SpillCostIsMonotone) {
  const std::vector<double> row_counts = {
      0.0,   0.1,   0.5,    0.9,    1.0,     1.05,    1.1,
      1.5,   2.0,   5.0,    10.0,   20.0,    50.0,    100.0,
      250.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0};

  // 3. Memory spill cost monotonicity.
  MemoryBudget budget{.max_memory_bytes = 64.0 * 1024.0,
                      .row_size_bytes = 64.0,
                      .io_spill_cost_multiplier = 3.5};
  for (OperatorCostKind kind :
       {OperatorCostKind::kSort, OperatorCostKind::kHashJoin}) {
    for (size_t i = 1; i < row_counts.size(); ++i) {
      double spill_prev =
          EstimateMemorySpillCost(kind, row_counts[i - 1], budget);
      double spill_curr = EstimateMemorySpillCost(kind, row_counts[i], budget);
      EXPECT_LE(spill_prev, spill_curr + 1e-9)
          << "Spill cost not monotonic for kind " << static_cast<int>(kind)
          << " between " << row_counts[i - 1] << " and " << row_counts[i];
    }
  }
}

namespace {

// Shared memo shape for the TopN-over-1:1-join tests: fact(dim_id) JOIN
// dim(id) with TopN(fact.id, 5) directly above the join (i.e. the state
// after topn_push_through_projection). The caller decides the proof:
// - fk=true: fact.dim_id carries Constraint::kForeign -> "dim".
// - mark=true: memo.MarkProvenOneToOne("fact", "dim") (snapshot path).
// dim.id is always PRIMARY KEY so the <=1-match side is proven.
struct TopNProvenJoinFixture {
  Memo memo;
  GroupId fact{kInvalidGroup};
  GroupId dim{kInvalidGroup};
  GroupId topn{kInvalidGroup};

  TopNProvenJoinFixture(bool fk, bool mark,
                        std::string fk_reference = "dim(id)",
                        bool filter_null = true, bool residual = false) {
    std::vector<Column> fact_columns = {
        Column("id", ValueType::kInt64, Constraint(Constraint::kPrimaryKey)),
        Column("dim_id", ValueType::kInt64,
               fk ? Constraint(Constraint::kForeign,
                               Value(std::move(fk_reference)))
                  : Constraint(Constraint::kNothing)),
        Column("payload", ValueType::kInt64)};
    const Schema fact_schema("fact", std::move(fact_columns));
    const Schema dim_schema("dim", {Column("id", ValueType::kInt64,
                                           Constraint(Constraint::kPrimaryKey)),
                                    Column("val", ValueType::kInt64)});
    memo.SetTableSchemas({{"fact", fact_schema}, {"dim", dim_schema}});
    (void)memo.Build({"fact", "dim"});
    if (mark) {
      memo.MarkProvenOneToOne("fact", "dim");
    }
    const GroupId fact_base = memo.EnsureGroup({"fact"});
    memo.AddExpression(fact_base,
                       LogicalExpression{.operation = LogicalOperator::kScan,
                                         .table = "fact",
                                         .output_schema = fact_schema});
    if (filter_null) {
      fact = memo.EnsureDerivedGroup({"fact"}, "fact_not_null");
      memo.AddExpression(
          fact,
          LogicalExpression{.operation = LogicalOperator::kSelection,
                            .children = {fact_base},
                            .predicate = UnaryExpressionExp(
                                ColumnValueExp(ColumnName("fact", "dim_id")),
                                UnaryOperation::kIsNotNull),
                            .output_schema = fact_schema});
    } else {
      fact = fact_base;
    }
    dim = memo.EnsureGroup({"dim"});
    memo.AddExpression(dim,
                       LogicalExpression{.operation = LogicalOperator::kScan,
                                         .table = "dim",
                                         .output_schema = dim_schema});
    const GroupId join = memo.EnsureDerivedGroup({"fact", "dim"}, "inner_join");
    const Expression equality = BinaryExpressionExp(
        ColumnValueExp(ColumnName("fact", "dim_id")), BinaryOperation::kEquals,
        ColumnValueExp(ColumnName("dim", "id")));
    memo.AddExpression(
        join,
        LogicalExpression{
            .operation = LogicalOperator::kJoin,
            .children = {fact, dim},
            .predicate =
                residual
                    ? BinaryExpressionExp(
                          equality, BinaryOperation::kAnd,
                          BinaryExpressionExp(
                              ColumnValueExp(ColumnName("fact", "payload")),
                              BinaryOperation::kGreaterThan,
                              ConstantValueExp(Value(int64_t{0}))))
                    : equality});
    topn = memo.EnsureDerivedGroup({"fact", "dim"}, "topn");
    memo.AddExpression(
        topn,
        LogicalExpression{.operation = LogicalOperator::kTopN,
                          .children = {join},
                          .target_list = {NamedExpression(
                              "", ColumnValueExp(ColumnName("fact", "id")))},
                          .sort_ascending = {true},
                          .sort_nulls_first = {std::nullopt},
                          .limit_count = 5,
                          .limit_offset = 0});
  }

  [[nodiscard]] bool HasPushedTopN(const Memo& searched) const {
    for (size_t i = 0; i < searched.GroupCount(); ++i) {
      const Group& group = searched.Get(i);
      if (!group.tag.starts_with("topn_proven_one_to_one:5:fact:")) {
        continue;
      }
      for (const auto& expr : group.expressions) {
        if (expr.operation == LogicalOperator::kTopN && expr.limit_count == 5 &&
            expr.limit_offset == 0 && expr.children.size() == 1 &&
            expr.children[0] == fact) {
          return true;
        }
      }
    }
    return false;
  }
};

}  // namespace

TEST(CascadesTest, TopNPushThroughProvenOneToOneJoinWithForeignKey) {
  TopNProvenJoinFixture fixture(/*fk=*/true, /*mark=*/false);
  const GroupId topn = fixture.topn;
  SearchEngine search(std::move(fixture.memo), RuleSet::Default());
  search.Explore(topn);
  EXPECT_TRUE(search.AppliedRuleNames().contains(
      "topn_push_through_proven_one_to_one_join"));
  EXPECT_TRUE(fixture.HasPushedTopN(search.GetMemo()));
}

TEST(CascadesTest, TopNPushThroughProvenOneToOneJoinRejectsNullableForeignKey) {
  TopNProvenJoinFixture fixture(true, false, "dim(id)", false);
  const GroupId topn = fixture.topn;
  SearchEngine search(std::move(fixture.memo), RuleSet::Default());
  search.Explore(topn);
  EXPECT_FALSE(fixture.HasPushedTopN(search.GetMemo()));
}

TEST(CascadesTest,
     TopNPushThroughProvenOneToOneJoinRejectsWrongForeignKeyColumn) {
  TopNProvenJoinFixture fixture(true, false, "dim(code)");
  const GroupId topn = fixture.topn;
  SearchEngine search(std::move(fixture.memo), RuleSet::Default());
  search.Explore(topn);
  EXPECT_FALSE(fixture.HasPushedTopN(search.GetMemo()));
}

TEST(CascadesTest, TopNPushThroughProvenOneToOneJoinRejectsResidual) {
  TopNProvenJoinFixture fixture(false, true, "dim(id)", true, true);
  const GroupId topn = fixture.topn;
  SearchEngine search(std::move(fixture.memo), RuleSet::Default());
  search.Explore(topn);
  EXPECT_FALSE(fixture.HasPushedTopN(search.GetMemo()));
}

TEST(CascadesTest, TopNPushThroughProvenOneToOneJoinWithSnapshotProof) {
  TopNProvenJoinFixture fixture(/*fk=*/false, /*mark=*/true);
  const GroupId topn = fixture.topn;
  SearchEngine search(std::move(fixture.memo), RuleSet::Default());
  search.Explore(topn);
  EXPECT_TRUE(search.AppliedRuleNames().contains(
      "topn_push_through_proven_one_to_one_join"));
  EXPECT_TRUE(fixture.HasPushedTopN(search.GetMemo()));
}

TEST(CascadesTest, TopNPushThroughProvenOneToOneJoinSkipsWithoutProof) {
  // Uniqueness on dim.id alone must NOT fire the rule (the D5 disabled-rule
  // counterexample: an unmatched fact row inside the TopN prefix).
  TopNProvenJoinFixture fixture(/*fk=*/false, /*mark=*/false);
  const GroupId topn = fixture.topn;
  SearchEngine search(std::move(fixture.memo), RuleSet::Default());
  search.Explore(topn);
  EXPECT_FALSE(search.AppliedRuleNames().contains(
      "topn_push_through_proven_one_to_one_join"));
  EXPECT_FALSE(fixture.HasPushedTopN(search.GetMemo()));
}

TEST(CascadesTest, TopNPushThroughProvenKeepsJoinPredicatesSeparate) {
  // D1: two joins with different predicates over the same relations must not
  // share one pushed-join group, or the best plan could pair a TopN with the
  // wrong predicate's rows.
  TopNProvenJoinFixture fixture(/*fk=*/false, /*mark=*/true);
  Memo memo = std::move(fixture.memo);
  const GroupId join =
      memo.EnsureDerivedGroup({"fact", "dim"}, "two_pred_joins");
  for (const char* outer_col : {"dim_id", "payload"}) {
    memo.AddExpression(
        join,
        LogicalExpression{.operation = LogicalOperator::kJoin,
                          .children = {fixture.fact, fixture.dim},
                          .predicate = BinaryExpressionExp(
                              ColumnValueExp(ColumnName("fact", outer_col)),
                              BinaryOperation::kEquals,
                              ColumnValueExp(ColumnName("dim", "id")))});
  }
  const GroupId topn = memo.EnsureDerivedGroup({"fact", "dim"}, "topn_two");
  memo.AddExpression(
      topn,
      LogicalExpression{.operation = LogicalOperator::kTopN,
                        .children = {join},
                        .target_list = {NamedExpression(
                            "", ColumnValueExp(ColumnName("fact", "id")))},
                        .sort_ascending = {true},
                        .sort_nulls_first = {std::nullopt},
                        .limit_count = 5,
                        .limit_offset = 0});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(topn);
  std::unordered_set<std::string> pushed_join_tags;
  for (size_t i = 0; i < search.GetMemo().GroupCount(); ++i) {
    const Group& group = search.GetMemo().Get(i);
    if (group.tag.starts_with("topn_proven_join:5:fact:")) {
      pushed_join_tags.insert(group.tag);
    }
  }
  // fact.payload is not a foreign key and has no snapshot mark issues here
  // (the mark covers the pair, both predicates share it), so both fire and
  // each keeps its own predicate group.
  EXPECT_EQ(pushed_join_tags.size(), 2U);
}

TEST(CascadesTest, TopNPushThroughProvenOneToOneJoinSkipsNonLocalKeys) {
  // TopN keys spanning both sides cannot be evaluated on one side alone.
  TopNProvenJoinFixture fixture(/*fk=*/true, /*mark=*/false);
  Memo memo = std::move(fixture.memo);
  const GroupId join =
      memo.EnsureDerivedGroup({"fact", "dim"}, "inner_join_wide");
  memo.AddExpression(
      join, LogicalExpression{.operation = LogicalOperator::kJoin,
                              .children = {fixture.fact, fixture.dim},
                              .predicate = BinaryExpressionExp(
                                  ColumnValueExp(ColumnName("fact", "dim_id")),
                                  BinaryOperation::kEquals,
                                  ColumnValueExp(ColumnName("dim", "id")))});
  const GroupId wide_topn =
      memo.EnsureDerivedGroup({"fact", "dim"}, "topn_wide");
  memo.AddExpression(
      wide_topn,
      LogicalExpression{
          .operation = LogicalOperator::kTopN,
          .children = {join},
          .target_list = {NamedExpression(
                              "", ColumnValueExp(ColumnName("fact", "id"))),
                          NamedExpression(
                              "", ColumnValueExp(ColumnName("dim", "val")))},
          .sort_ascending = {true, true},
          .sort_nulls_first = {std::nullopt, std::nullopt},
          .limit_count = 5,
          .limit_offset = 0});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(wide_topn);
  // No pushed TopN over the fact side may appear for the wide key set.
  bool pushed = false;
  for (size_t i = 0; i < search.GetMemo().GroupCount(); ++i) {
    const Group& group = search.GetMemo().Get(i);
    if (!group.tag.starts_with("topn_proven_one_to_one:5:fact:")) {
      continue;
    }
    for (const auto& expr : group.expressions) {
      if (expr.operation == LogicalOperator::kTopN &&
          expr.target_list.size() == 2) {
        pushed = true;
      }
    }
  }
  EXPECT_FALSE(pushed);
}

TEST(CascadesTest, OrToUnionSplitsTwoBranchDisjunction) {
  Memo memo;
  (void)memo.Build({"t"});
  const GroupId base = memo.EnsureGroup({"t"});
  const GroupId sel = memo.EnsureDerivedGroup({"t"}, "or_sel");
  Expression predicate = BinaryExpressionExp(
      UnaryExpressionExp(ColumnValueExp(ColumnName("t", "a")),
                         UnaryOperation::kIsNull),
      BinaryOperation::kOr,
      UnaryExpressionExp(ColumnValueExp(ColumnName("t", "b")),
                         UnaryOperation::kIsNull));
  memo.AddExpression(sel,
                     LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {base},
                                       .predicate = predicate});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel);
  EXPECT_TRUE(search.AppliedRuleNames().contains("or_to_union"));
  bool found_union = false;
  size_t branch_selections = 0;
  for (const auto& expr : search.GetMemo().Get(sel).expressions) {
    if (expr.operation != LogicalOperator::kUnionAll ||
        expr.children.size() != 2) {
      continue;
    }
    found_union = true;
    for (const GroupId branch : expr.children) {
      for (const auto& branch_expr : search.GetMemo().Get(branch).expressions) {
        if (branch_expr.operation == LogicalOperator::kSelection) {
          ++branch_selections;
        }
      }
    }
  }
  EXPECT_TRUE(found_union);
  EXPECT_EQ(branch_selections, 2U);
}

TEST(CascadesTest, OrToUnionSkipsSameColumnEqualities) {
  // Same-column `a=1 OR a=2` folds to IN instead; a deduping union would
  // only add overhead there.
  Memo memo;
  (void)memo.Build({"t"});
  const GroupId base = memo.EnsureGroup({"t"});
  const GroupId sel = memo.EnsureDerivedGroup({"t"}, "or_same_col");
  Expression predicate = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp(ColumnName("t", "a")),
                          BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{1}))),
      BinaryOperation::kOr,
      BinaryExpressionExp(ColumnValueExp(ColumnName("t", "a")),
                          BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{2}))));
  memo.AddExpression(sel,
                     LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {base},
                                       .predicate = predicate});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel);
  EXPECT_FALSE(search.AppliedRuleNames().contains("or_to_union"));
}

TEST(CascadesTest, OrToUnionSplitsThreeBranchDisjunction) {
  Memo memo;
  (void)memo.Build({"t"});
  const GroupId base = memo.EnsureGroup({"t"});
  const GroupId sel = memo.EnsureDerivedGroup({"t"}, "or_three");
  auto is_null = [](const char* column) {
    return UnaryExpressionExp(ColumnValueExp(ColumnName("t", column)),
                              UnaryOperation::kIsNull);
  };
  Expression predicate = BinaryExpressionExp(
      BinaryExpressionExp(is_null("a"), BinaryOperation::kOr, is_null("b")),
      BinaryOperation::kOr, is_null("c"));
  memo.AddExpression(sel,
                     LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {base},
                                       .predicate = predicate});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel);
  EXPECT_TRUE(search.AppliedRuleNames().contains("or_to_union"));
  bool found_union = false;
  size_t branch_selections = 0;
  for (const auto& expr : search.GetMemo().Get(sel).expressions) {
    if (expr.operation != LogicalOperator::kUnionAll ||
        expr.children.size() != 3) {
      continue;
    }
    found_union = true;
    for (const GroupId branch : expr.children) {
      for (const auto& branch_expr : search.GetMemo().Get(branch).expressions) {
        if (branch_expr.operation == LogicalOperator::kSelection) {
          ++branch_selections;
        }
      }
    }
  }
  EXPECT_TRUE(found_union);
  EXPECT_EQ(branch_selections, 3U);
}

TEST(CascadesTest, OrToUnionSplitsMultiRelationDisjunction) {
  Memo memo;
  (void)memo.Build({"left_table", "right_table"});
  const GroupId left = memo.EnsureGroup({"left_table"});
  const GroupId right = memo.EnsureGroup({"right_table"});
  const GroupId join_group =
      memo.EnsureDerivedGroup({"left_table", "right_table"}, "multi_or_join");
  ASSERT_TRUE(memo.AddExpression(
      join_group, LogicalExpression{.operation = LogicalOperator::kJoin,
                                    .children = {left, right}}));
  const GroupId selection =
      memo.EnsureDerivedGroup({"left_table", "right_table"}, "multi_or_filter");
  const Expression predicate = BinaryExpressionExp(
      UnaryExpressionExp(ColumnValueExp(ColumnName("left_table", "a")),
                         UnaryOperation::kIsNull),
      BinaryOperation::kOr,
      UnaryExpressionExp(ColumnValueExp(ColumnName("right_table", "b")),
                         UnaryOperation::kIsNull));
  ASSERT_TRUE(memo.AddExpression(
      selection, LogicalExpression{.operation = LogicalOperator::kSelection,
                                   .children = {join_group},
                                   .predicate = predicate}));

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(selection);
  EXPECT_TRUE(search.AppliedRuleNames().contains("or_to_union"));
  EXPECT_TRUE(std::ranges::any_of(search.GetMemo().Get(selection).expressions,
                                  [](const LogicalExpression& expression) {
                                    return expression.operation ==
                                               LogicalOperator::kUnionAll &&
                                           expression.children.size() == 2;
                                  }));
}

TEST(CascadesTest, DistinctEagerOverJoinPushesDistinctBelowJoin) {
  Memo memo;
  const Schema r_schema("r", {Column("id", ValueType::kInt64,
                                     Constraint(Constraint::kPrimaryKey))});
  memo.SetTableSchemas({{"r", r_schema}});
  (void)memo.Build({"l", "r"});
  const GroupId left = memo.EnsureGroup({"l"});
  const GroupId right = memo.EnsureGroup({"r"});
  memo.AddExpression(right,
                     LogicalExpression{.operation = LogicalOperator::kScan,
                                       .table = "r",
                                       .output_schema = r_schema});
  const GroupId join = memo.EnsureDerivedGroup({"l", "r"}, "inner_join");
  memo.AddExpression(
      join, LogicalExpression{.operation = LogicalOperator::kJoin,
                              .children = {left, right},
                              .predicate = BinaryExpressionExp(
                                  ColumnValueExp(ColumnName("l", "rid")),
                                  BinaryOperation::kEquals,
                                  ColumnValueExp(ColumnName("r", "id")))});
  const GroupId distinct = memo.EnsureDerivedGroup({"l", "r"}, "distinct");
  memo.AddExpression(distinct,
                     LogicalExpression{.operation = LogicalOperator::kDistinct,
                                       .children = {join}});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(distinct);
  EXPECT_TRUE(search.AppliedRuleNames().contains("distinct_eager_over_join"));
  bool found_narrow_join = false;
  for (size_t i = 0; i < search.GetMemo().GroupCount(); ++i) {
    const Group& group = search.GetMemo().Get(i);
    if (!group.tag.starts_with("distinct_eager_join:l:")) {
      continue;
    }
    for (const auto& expr : group.expressions) {
      if (expr.operation != LogicalOperator::kJoin ||
          expr.children.size() != 2) {
        continue;
      }
      const Group& pushed = search.GetMemo().Get(expr.children[0]);
      if (!pushed.tag.starts_with("distinct_eager:l:")) {
        continue;
      }
      for (const auto& pushed_expr : pushed.expressions) {
        if (pushed_expr.operation == LogicalOperator::kDistinct &&
            pushed_expr.children.size() == 1 &&
            pushed_expr.children[0] == left) {
          found_narrow_join = true;
        }
      }
    }
  }
  EXPECT_TRUE(found_narrow_join);
}

TEST(CascadesTest, DistinctEagerOverJoinSkipsNonUniqueSide) {
  Memo memo;
  (void)memo.Build({"l", "r"});
  const GroupId left = memo.EnsureGroup({"l"});
  const GroupId right = memo.EnsureGroup({"r"});
  const GroupId join = memo.EnsureDerivedGroup({"l", "r"}, "inner_join");
  memo.AddExpression(
      join, LogicalExpression{.operation = LogicalOperator::kJoin,
                              .children = {left, right},
                              .predicate = BinaryExpressionExp(
                                  ColumnValueExp(ColumnName("l", "rid")),
                                  BinaryOperation::kEquals,
                                  ColumnValueExp(ColumnName("r", "id")))});
  const GroupId distinct = memo.EnsureDerivedGroup({"l", "r"}, "distinct");
  memo.AddExpression(distinct,
                     LogicalExpression{.operation = LogicalOperator::kDistinct,
                                       .children = {join}});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(distinct);
  EXPECT_FALSE(search.AppliedRuleNames().contains("distinct_eager_over_join"));
}

TEST(CascadesTest, TopNPushThroughUnionAllCapsBranches) {
  Memo memo;
  (void)memo.Build({"t"});
  const GroupId base = memo.EnsureGroup({"t"});
  const GroupId branch0 = memo.EnsureDerivedGroup({"t"}, "branch0");
  memo.AddExpression(
      branch0, LogicalExpression{.operation = LogicalOperator::kSelection,
                                 .children = {base},
                                 .predicate = BinaryExpressionExp(
                                     ColumnValueExp(ColumnName("t", "a")),
                                     BinaryOperation::kEquals,
                                     ConstantValueExp(Value(int64_t{1})))});
  const GroupId branch1 = memo.EnsureDerivedGroup({"t"}, "branch1");
  memo.AddExpression(
      branch1, LogicalExpression{.operation = LogicalOperator::kSelection,
                                 .children = {base},
                                 .predicate = BinaryExpressionExp(
                                     ColumnValueExp(ColumnName("t", "a")),
                                     BinaryOperation::kEquals,
                                     ConstantValueExp(Value(int64_t{2})))});
  const GroupId un = memo.EnsureDerivedGroup({"t"}, "union_all");
  memo.AddExpression(un,
                     LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                       .children = {branch0, branch1}});
  const GroupId topn = memo.EnsureDerivedGroup({"t"}, "topn");
  memo.AddExpression(
      topn, LogicalExpression{.operation = LogicalOperator::kTopN,
                              .children = {un},
                              .target_list = {NamedExpression(
                                  "", ColumnValueExp(ColumnName("t", "a")))},
                              .sort_ascending = {true},
                              .sort_nulls_first = {std::nullopt},
                              .limit_count = 5,
                              .limit_offset = 0});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(topn);
  EXPECT_TRUE(
      search.AppliedRuleNames().contains("topn_push_through_union_all"));
  size_t capped_branches = 0;
  for (size_t i = 0; i < search.GetMemo().GroupCount(); ++i) {
    const Group& group = search.GetMemo().Get(i);
    if (!group.tag.starts_with("topn_union_child:")) {
      continue;
    }
    for (const auto& expr : group.expressions) {
      if (expr.operation == LogicalOperator::kTopN && expr.limit_count == 5 &&
          expr.limit_offset == 0) {
        ++capped_branches;
      }
    }
  }
  EXPECT_EQ(capped_branches, 2U);
  bool found_capped_union = false;
  for (const auto& expr : search.GetMemo().Get(topn).expressions) {
    if (expr.operation == LogicalOperator::kTopN && expr.children.size() == 1) {
      const Group& child = search.GetMemo().Get(expr.children[0]);
      if (child.tag.starts_with("topn_union_pushed:5:")) {
        found_capped_union = true;
      }
    }
  }
  EXPECT_TRUE(found_capped_union);
}

TEST(CascadesTest, TopNPushThroughUnionAllSkipsWithTies) {
  Memo memo;
  (void)memo.Build({"t"});
  const GroupId base = memo.EnsureGroup({"t"});
  const GroupId un = memo.EnsureDerivedGroup({"t"}, "union_all");
  memo.AddExpression(un,
                     LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                       .children = {base, base}});
  const GroupId topn = memo.EnsureDerivedGroup({"t"}, "topn_ties");
  memo.AddExpression(
      topn, LogicalExpression{.operation = LogicalOperator::kTopN,
                              .children = {un},
                              .target_list = {NamedExpression(
                                  "", ColumnValueExp(ColumnName("t", "a")))},
                              .sort_ascending = {true},
                              .sort_nulls_first = {std::nullopt},
                              .limit_count = 5,
                              .limit_offset = 0,
                              .with_ties = true});
  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(topn);
  EXPECT_FALSE(
      search.AppliedRuleNames().contains("topn_push_through_union_all"));
}

}  // namespace tinylamb::cascades
