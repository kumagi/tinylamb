/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <string_view>

#include "expression/parameter_slot.hpp"
#include "plan/cascades_test.hpp"

namespace tinylamb::cascades {

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
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
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
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
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

TEST(CascadesTest, SelfJoinEliminationRejectsNullableUniqueKey) {
  // A nullable UNIQUE column admits duplicate NULLs and NULL-key rows never
  // match `a.k = b.k`, so the INNER join would drop them while the bare scan
  // keeps them: the rule must refuse without a NOT NULL/PK declaration.
  Memo memo;
  (void)memo.Build({"t1_1", "t1_2"});
  const Schema nullable_unique(
      "t1", {Column("id", ValueType::kInt64, Constraint(Constraint::kUnique))});
  memo.SetTableSchemas({{"t1_1", nullable_unique}, {"t1_2", nullable_unique}});
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

TEST(CascadesTest, OuterToAntiJoinFiresOnNullableJoinKeyRejectedByOn) {
  // Lean proof (TinylambOpt/Discovery.lean): Even when t2.id is nullable in the
  // table schema, `ON t1.id = t2.id` is null-rejecting on t2.id, so no matched
  // row can carry t2.id = NULL; `WHERE t2.id IS NULL` still selects strictly
  // the unmatched rows and converts soundly to AntiJoin.
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
  const bool found_anti =
      std::ranges::any_of(exprs, [](const LogicalExpression& expr) {
        return expr.operation == LogicalOperator::kAntiJoin;
      });
  EXPECT_TRUE(found_anti);
}

TEST(CascadesTest, OuterToAntiJoinRefusesNullableNonJoinColumn) {
  // A matched row whose non-join column t2.note is genuinely NULL satisfies
  // `t2.note IS NULL` (`ON t1.id = t2.id` does not reject NULLs on t2.note)
  // and would be dropped by AntiJoin; must refuse.
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  memo.SetTableSchemas(
      {{"t2", Schema("t2", {Column("id", ValueType::kInt64),
                            Column("note", ValueType::kVarChar)})}});
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
                                       ColumnValueExp(ColumnName("t2", "note")),
                                       UnaryOperation::kIsNull)});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_group);

  const auto& exprs = search.GetMemo().Get(sel_group).expressions;
  const bool found_anti =
      std::ranges::any_of(exprs, [](const LogicalExpression& expr) {
        return expr.operation == LogicalOperator::kAntiJoin;
      });
  EXPECT_FALSE(found_anti);
}

TEST(CascadesTest, OuterToAntiJoinRefusesNullSafeOnForNullableColumn) {
  // Null-safe `ON t1.id IS NOT DISTINCT FROM t2.id` can match NULL=NULL, so a
  // matched row can still have t2.id IS NULL; must refuse without NOT NULL.
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
      LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                        .children = {left, right},
                        .predicate = BinaryExpressionExp(
                            ColumnValueExp(ColumnName("t1", "id")),
                            BinaryOperation::kIsNotDistinctFrom,
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
  const bool found_anti =
      std::ranges::any_of(exprs, [](const LogicalExpression& expr) {
        return expr.operation == LogicalOperator::kAntiJoin;
      });
  EXPECT_FALSE(found_anti);
}

TEST(CascadesTest, OuterToAntiJoinPreservesLeftResidualConjunct) {
  // `WHERE t2.id IS NULL AND t1.x = 5`: bare kAntiJoin must NOT replace
  // sel_group directly (which would drop `t1.x = 5`), while Selection(t1.x = 5,
  // AntiJoin) IS generated so the left filter survives and pushes down.
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
  const bool found_bare_anti =
      std::ranges::any_of(exprs, [](const LogicalExpression& expr) {
        return expr.operation == LogicalOperator::kAntiJoin;
      });
  EXPECT_FALSE(found_bare_anti);
  const bool found_filtered_anti =
      std::ranges::any_of(exprs, [&](const LogicalExpression& expr) {
        if (expr.operation != LogicalOperator::kSelection ||
            expr.children.size() != 1) {
          return false;
        }
        return std::ranges::any_of(
            search.GetMemo().Get(expr.children[0]).expressions,
            [](const LogicalExpression& child_expr) {
              return child_expr.operation == LogicalOperator::kAntiJoin;
            });
      });
  EXPECT_TRUE(found_filtered_anti);
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

TEST(CascadesTest, LimitPrefixThroughUnorderedWindowFires) {
  Memo memo;
  // A values leaf stands in for the base relation: window-row estimates then
  // dominate the cost comparison without a catalog-backed scan. Build only
  // registers the relation in the join graph.
  (void)memo.Build({"t1"});
  std::vector<Row> wide_rows;
  wide_rows.reserve(1000);
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
      if (expr.operation == LogicalOperator::kLimit && !expr.children.empty() &&
          expr.children[0] == scan) {
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

TEST(CascadesTest, MixedFrameWindowKeepsLimitAbove) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  // Both calls are individually stable under truncation (backward-only ROWS
  // frames), so only the mixed-spec check can keep the limit above the window.
  // A signature that ignores the frame would treat them as one spec and mix
  // two semantically different windows in a single derived memo group.
  auto call_all = PrefixWindowCall("SUM", {ColumnValueExp("t1.val")},
                                   {ColumnValueExp("t1.part")},
                                   /*order_by=*/{}, /*has_frame=*/true);
  auto call_recent = PrefixWindowCall("SUM", {ColumnValueExp("t1.val")},
                                      {ColumnValueExp("t1.part")},
                                      /*order_by=*/{}, /*has_frame=*/true);
  call_recent->frame_start.type = WindowFrameBoundType::kOffsetPreceding;
  call_recent->frame_start.offset = ConstantValueExp(Value(int64_t{1}));
  const GroupId win_group = memo.EnsureDerivedGroup({"t1"}, "win_node");
  memo.AddExpression(
      win_group,
      LogicalExpression{
          .operation = LogicalOperator::kWindow,
          .children = {scan},
          .target_list = {NamedExpression("all", Expression(call_all)),
                          NamedExpression("recent", Expression(call_recent))},
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
       {"CUME_DIST", "PERCENT_RANK", "NTILE", "LEAD", "PERCENTILE_CONT",
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
// The probe-key fingerprint must be unambiguous: a plain comma join used to
// map ["t1.a", "t1.b"] and ["t1.a,t1.b"] onto the same derived group, so the
// second semi join inherited the first one's probe key set.
TEST(CascadesTest, SemiReductionProbeKeysFingerprintIsUnambiguous) {
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId probe = memo.EnsureGroup({"t1"});
  const GroupId build = memo.EnsureGroup({"t2"});
  const GroupId join_group = memo.EnsureDerivedGroup({"t1", "t2"}, "root_semi");
  const Expression build_id = ColumnValueExp(ColumnName("t2", "id"));
  const auto eq_on = [&](const std::string& name) {
    return BinaryExpressionExp(ColumnValueExp(ColumnName("t1", name)),
                               BinaryOperation::kEquals, build_id);
  };
  memo.AddExpression(
      join_group,
      LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                        .children = {probe, build},
                        .predicate = BinaryExpressionExp(
                            eq_on("a"), BinaryOperation::kAnd, eq_on("b"))});
  memo.AddExpression(join_group,
                     LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                       .children = {probe, build},
                                       .predicate = eq_on("a,t1.b")});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(join_group);

  std::set<std::vector<std::string>> key_sets;
  for (GroupId id = 0; id < search.GetMemo().GroupCount(); ++id) {
    const Group& group = search.GetMemo().Get(id);
    if (group.tag.find("semi_reduce_probe:") == std::string::npos) {
      continue;
    }
    std::vector<std::string> keys;
    keys.reserve(group.semi_reduction_probe_keys.size());
for (const ColumnName& key : group.semi_reduction_probe_keys) {
      keys.push_back(key.ToString());
    }
    if (!keys.empty()) {
      key_sets.insert(std::move(keys));
    }
  }
  EXPECT_EQ(key_sets.size(), 2U)
      << "two different probe key sets must get two different derived groups";
}

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
  memo.SetTableSchemas(
      {{"t1", Schema("t1", {Column("id", ValueType::kInt64)})},
       {"t2", Schema("t2", {Column("id", ValueType::kInt64)})}});
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
  memo.SetTableSchemas(
      {{"t1", Schema("t1", {Column("id", ValueType::kInt64)})},
       {"t2", Schema("t2", {Column("id", ValueType::kInt64)})}});
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

}  // namespace tinylamb::cascades
