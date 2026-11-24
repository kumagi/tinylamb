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
          .target_list = {NamedExpression("k", ColumnValueExp("a.x")),
                          NamedExpression(
                              "s", AggregateExpressionExp(AggregationType::kSum,
                                                          ColumnValueExp("a.y"),
                                                          false))},
          .grouping_sets = {ColumnValueExp("a.x")}}));
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

TEST(CascadesTest, DistinctIdempotentAggregateEliminationStripsMinMaxDistinct) {
  Memo memo;
  (void)memo.Build({"t1"});
  const GroupId scan = memo.EnsureGroup({"t1"});
  const GroupId agg_group = memo.EnsureDerivedGroup({"t1"}, "agg");
  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {scan},
          .target_list = {NamedExpression("mn", AggregateExpressionExp(
                                                    AggregationType::kMin,
                                                    ColumnValueExp("t1.v"),
                                                    /*distinct=*/true)),
                          NamedExpression("sm", AggregateExpressionExp(
                                                    AggregationType::kSum,
                                                    ColumnValueExp("t1.v"),
                                                    /*distinct=*/true))},
          .grouping_sets = {ColumnValueExp("t1.k")}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg_group);

  const bool found_stripped = std::ranges::any_of(
      search.GetMemo().Get(agg_group).expressions,
      [](const LogicalExpression& expr) {
        if (expr.operation != LogicalOperator::kAggregation ||
            expr.target_list.size() != 2) {
          return false;
        }
        const auto& mn =
            expr.target_list[0].expression->AsAggregateExpression();
        const auto& sm =
            expr.target_list[1].expression->AsAggregateExpression();
        return !mn.Distinct() && sm.Distinct();
      });
  EXPECT_TRUE(found_stripped);
}

TEST(CascadesTest, InnerJoinToSemiJoinForIdempotentAggsAndRefusesSum) {
  auto check_for_agg_type = [](AggregationType type, bool distinct) {
    Memo memo;
    (void)memo.Build({"l", "r"});
    const GroupId left = memo.EnsureGroup({"l"});
    const GroupId right = memo.EnsureGroup({"r"});
    const GroupId join_group = memo.EnsureDerivedGroup({"l", "r"}, "j");
    memo.AddExpression(
        join_group,
        LogicalExpression{.operation = LogicalOperator::kJoin,
                          .children = {left, right},
                          .predicate = BinaryExpressionExp(
                              ColumnValueExp("l.id"), BinaryOperation::kEquals,
                              ColumnValueExp("r.id"))});
    const GroupId agg_group = memo.EnsureDerivedGroup({"l", "r"}, "agg");
    memo.AddExpression(
        agg_group,
        LogicalExpression{
            .operation = LogicalOperator::kAggregation,
            .children = {join_group},
            .target_list = {NamedExpression("k", ColumnValueExp("l.dept")),
                            NamedExpression(
                                "a",
                                AggregateExpressionExp(
                                    type, ColumnValueExp("l.v"), distinct))},
            .grouping_sets = {ColumnValueExp("l.dept")}});
    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(agg_group);
    return std::ranges::any_of(
        search.GetMemo().Get(agg_group).expressions,
        [&](const LogicalExpression& expr) {
          if (expr.operation != LogicalOperator::kAggregation ||
              expr.children.size() != 1) {
            return false;
          }
          return std::ranges::any_of(
              search.GetMemo().Get(expr.children[0]).expressions,
              [](const LogicalExpression& c) {
                return c.operation == LogicalOperator::kSemiJoin;
              });
        });
  };
  EXPECT_TRUE(check_for_agg_type(AggregationType::kMax, false));
  EXPECT_TRUE(check_for_agg_type(AggregationType::kCount, true));
  EXPECT_FALSE(check_for_agg_type(AggregationType::kSum, false));
}

TEST(CascadesTest, HavingExtremumBoundPushdownFiresForIntAndRefusesDoubleNaN) {
  auto run_for_type = [](ValueType col_type, const Value& bound_val) {
    Memo memo;
    (void)memo.Build({"t1"});
    memo.SetTableSchemas({{"t1", Schema("t1", {Column("k", ValueType::kInt64),
                                               Column("v", col_type)})}});
    const GroupId scan = memo.EnsureGroup({"t1"});
    const GroupId agg_group = memo.EnsureDerivedGroup({"t1"}, "agg");
    memo.AddExpression(
        agg_group,
        LogicalExpression{
            .operation = LogicalOperator::kAggregation,
            .children = {scan},
            .target_list = {NamedExpression("k", ColumnValueExp("t1.k")),
                            NamedExpression("mx", AggregateExpressionExp(
                                                      AggregationType::kMax,
                                                      ColumnValueExp("t1.v")))},
            .grouping_sets = {ColumnValueExp("t1.k")}});
    const GroupId having_group = memo.EnsureDerivedGroup({"t1"}, "having");
    memo.AddExpression(
        having_group,
        LogicalExpression{
            .operation = LogicalOperator::kSelection,
            .children = {agg_group},
            .predicate = BinaryExpressionExp(
                ColumnValueExp(ColumnName("", "mx")),
                BinaryOperation::kGreaterThan, ConstantValueExp(bound_val))});
    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(having_group);
    return std::ranges::any_of(
        search.GetMemo().Get(having_group).expressions,
        [&](const LogicalExpression& sel) {
          if (sel.operation != LogicalOperator::kSelection ||
              sel.children.size() != 1) {
            return false;
          }
          return std::ranges::any_of(
              search.GetMemo().Get(sel.children[0]).expressions,
              [&](const LogicalExpression& agg) {
                return agg.operation == LogicalOperator::kAggregation &&
                       agg.children.size() == 1 &&
                       search.GetMemo()
                           .Get(agg.children[0])
                           .tag.starts_with("sel-having-extremum:");
              });
        });
  };
  EXPECT_TRUE(run_for_type(ValueType::kInt64, Value(int64_t{100})));
  EXPECT_FALSE(run_for_type(ValueType::kDouble, Value(100.0)));
}

TEST(CascadesTest, OuterToInnerOnHavingDemotesStrictAggAndRefusesCountStar) {
  auto run_test = [](bool include_count_star) {
    Memo memo;
    (void)memo.Build({"l", "r"});
    const GroupId left = memo.EnsureGroup({"l"});
    const GroupId right = memo.EnsureGroup({"r"});
    const GroupId outer_group = memo.EnsureDerivedGroup({"l", "r"}, "outer");
    memo.AddExpression(
        outer_group,
        LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                          .children = {left, right},
                          .predicate = BinaryExpressionExp(
                              ColumnValueExp("l.id"), BinaryOperation::kEquals,
                              ColumnValueExp("r.id")),
                          .join_type = 0});
    std::vector<NamedExpression> targets = {
        NamedExpression("dept", ColumnValueExp("l.dept")),
        NamedExpression("avg_v",
                        AggregateExpressionExp(AggregationType::kAvg,
                                               ColumnValueExp("r.v")))};
    if (include_count_star) {
      targets.emplace_back(
          "cnt", AggregateExpressionExp(AggregationType::kCount, nullptr));
    }
    const GroupId agg_group = memo.EnsureDerivedGroup({"l", "r"}, "agg");
    memo.AddExpression(
        agg_group,
        LogicalExpression{.operation = LogicalOperator::kAggregation,
                          .children = {outer_group},
                          .target_list = std::move(targets),
                          .grouping_sets = {ColumnValueExp("l.dept")}});
    const GroupId having_group = memo.EnsureDerivedGroup({"l", "r"}, "having");
    memo.AddExpression(
        having_group,
        LogicalExpression{
            .operation = LogicalOperator::kSelection,
            .children = {agg_group},
            .predicate = BinaryExpressionExp(
                ColumnValueExp(ColumnName("", "avg_v")),
                BinaryOperation::kGreaterThan, ConstantValueExp(Value(10.0)))});
    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(having_group);
    return std::ranges::any_of(
        search.GetMemo().Get(having_group).expressions,
        [&](const LogicalExpression& sel) {
          if (sel.operation != LogicalOperator::kSelection ||
              sel.children.size() != 1) {
            return false;
          }
          return std::ranges::any_of(
              search.GetMemo().Get(sel.children[0]).expressions,
              [&](const LogicalExpression& agg) {
                return agg.operation == LogicalOperator::kAggregation &&
                       agg.children.size() == 1 &&
                       search.GetMemo()
                           .Get(agg.children[0])
                           .tag.starts_with("demoted_outer_having:");
              });
        });
  };
  EXPECT_TRUE(run_test(/*include_count_star=*/false));
  EXPECT_FALSE(run_test(/*include_count_star=*/true));
}

// NOLINTNEXTLINE(misc-use-anonymous-namespace): test helper is file-local.
static bool OffersEagerAggregation(AggregationType aggregate,
                                   std::string_view grouping_column) {
  Memo memo;
  const GroupId join_root = memo.Build(
      {"sales", "dept"}, {{BinaryExpressionExp(ColumnValueExp("sales.dept_id"),
                                               BinaryOperation::kEquals,
                                               ColumnValueExp("dept.id")),
                           {"sales", "dept"}}});
  memo.SetTableSchemas(
      {{"sales", Schema("sales", {Column("dept_id", ValueType::kInt64),
                                  Column("amt", ValueType::kInt64),
                                  Column("region", ValueType::kVarChar)})},
       {"dept", Schema("dept", {Column("id", ValueType::kInt64,
                                       Constraint(Constraint::kPrimaryKey)),
                                Column("region", ValueType::kVarChar)})}});
  const GroupId agg_group = memo.EnsureDerivedGroup({"sales", "dept"}, "agg");
  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {join_root},
          .target_list =
              {NamedExpression("region", ColumnValueExp(grouping_column)),
               NamedExpression("measure",
                               AggregateExpressionExp(
                                   aggregate, ColumnValueExp("sales.amt"))),
               NamedExpression("n", AggregateExpressionExp(
                                        AggregationType::kCount, nullptr))},
          .grouping_sets = {ColumnValueExp(grouping_column)}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(agg_group);

  const bool found_two_stage = std::ranges::any_of(
      search.GetMemo().Get(agg_group).expressions,
      [&](const LogicalExpression& expr) {
        if (expr.operation != LogicalOperator::kAggregation ||
            expr.children.size() != 1) {
          return false;
        }
        const Group& child_g = search.GetMemo().Get(expr.children[0]);
        if (!child_g.tag.starts_with("eager_agg_join:")) {
          return false;
        }
        return std::ranges::any_of(
            child_g.expressions, [&](const LogicalExpression& j) {
              return j.operation == LogicalOperator::kJoin &&
                     j.children.size() == 2 &&
                     search.GetMemo()
                         .Get(j.children[0])
                         .tag.starts_with("eager_pre_agg:");
            });
      });
  return found_two_stage;
}

TEST(CascadesTest, EagerAggregationOverJoinPushesTwoStagePreAgg) {
  EXPECT_TRUE(OffersEagerAggregation(AggregationType::kMax, "sales.region"));
  EXPECT_FALSE(OffersEagerAggregation(AggregationType::kMax, "dept.region"));
}

TEST(CascadesTest, EagerAggregationSkipsSumEvenWithUniqueJoinPartner) {
  EXPECT_FALSE(OffersEagerAggregation(AggregationType::kSum, "sales.region"));
}

TEST(CascadesTest, OuterJoinRightSelfEqualityDoesNotCertifyLeftCandidateKey) {
  // Lean audit (`cpp_outerJoin_selfEq_candidateKey_bug`): `L LEFT JOIN R ON
  // R.pk = R.col` matches EVERY row of R where `pk = col` for each row of L,
  // multiplying L's rows even though `R.pk` is PRIMARY KEY on R.
  Memo memo;
  (void)memo.Build({"l", "r"});
  const GroupId left_scan = memo.EnsureGroup({"l"});
  const GroupId right_scan = memo.EnsureGroup({"r"});
  memo.SetTableSchemas(
      {{"l", Schema("l", {Column("id", ValueType::kInt64,
                                 Constraint(Constraint::kPrimaryKey))})},
       {"r", Schema("r", {Column("pk", ValueType::kInt64,
                                 Constraint(Constraint::kPrimaryKey)),
                          Column("col", ValueType::kInt64)})}});

  const GroupId oj_group = memo.EnsureDerivedGroup({"l", "r"}, "oj_self_eq");
  memo.AddExpression(
      oj_group,
      LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                        .children = {left_scan, right_scan},
                        .predicate = BinaryExpressionExp(
                            ColumnValueExp("r.pk"), BinaryOperation::kEquals,
                            ColumnValueExp("r.col")),
                        .join_type = 0});
  const GroupId proj_group = memo.EnsureDerivedGroup({"l", "r"}, "proj_l_id");
  memo.AddExpression(
      proj_group, LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {oj_group},
                                    .target_list = {NamedExpression(
                                        "id", ColumnValueExp("l.id"))}});
  const GroupId dist_group = memo.EnsureDerivedGroup({"l", "r"}, "dist_l_id");
  memo.AddExpression(dist_group,
                     LogicalExpression{.operation = LogicalOperator::kDistinct,
                                       .children = {proj_group}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(dist_group);

  EXPECT_TRUE(
      search.GetMemo().Get(oj_group).logical_properties.candidate_keys.empty());
  for (const LogicalExpression& expr :
       search.GetMemo().Get(dist_group).expressions) {
    EXPECT_NE(expr.operation, LogicalOperator::kProjection)
        << "DISTINCT was unsoundly eliminated over fan-out LEFT JOIN";
  }
}

TEST(CascadesTest,
     PushSelectionThroughAggregationKeepsGroupingSetsAndSkipsScalar) {
  // Lean audit (`cpp_push_sel_through_agg_drops_grouping_bug`):
  // 1) Grouped aggregation must retain `grouping_sets` and `predicate` when a
  //    grouping-key selection pushes below it.
  // 2) Ungrouped scalar aggregation must NOT push `Selection` below it, since
  //    scalar aggregation emits 1 row on empty input instead of 0 rows.
  {
    Memo memo;
    const GroupId scan = memo.Build({"t"});
    const GroupId agg_group = memo.EnsureDerivedGroup({"t"}, "grouped_agg");
    memo.AddExpression(
        agg_group,
        LogicalExpression{
            .operation = LogicalOperator::kAggregation,
            .children = {scan},
            .predicate = BinaryExpressionExp(
                ColumnValueExp(ColumnName("", "s")),
                BinaryOperation::kGreaterThan, ConstantValueExp(Value(0))),
            .target_list = {NamedExpression("k", ColumnValueExp("t.k")),
                            NamedExpression("s", AggregateExpressionExp(
                                                     AggregationType::kSum,
                                                     ColumnValueExp("t.v")))},
            .grouping_sets = {ColumnValueExp("t.k")}});
    const GroupId sel_group = memo.EnsureDerivedGroup({"t"}, "sel_over_agg");
    memo.AddExpression(
        sel_group, LogicalExpression{.operation = LogicalOperator::kSelection,
                                     .children = {agg_group},
                                     .predicate = BinaryExpressionExp(
                                         ColumnValueExp(ColumnName("", "k")),
                                         BinaryOperation::kGreaterThan,
                                         ConstantValueExp(Value(10)))});

    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(sel_group);

    bool found_pushed = false;
    for (const LogicalExpression& expr :
         search.GetMemo().Get(sel_group).expressions) {
      if (expr.operation == LogicalOperator::kAggregation &&
          expr.children.size() == 1 &&
          search.GetMemo()
              .Get(expr.children[0])
              .tag.starts_with("sel-below-agg:")) {
        found_pushed = true;
        EXPECT_EQ(expr.grouping_sets.size(), 1U);
        EXPECT_TRUE(expr.predicate.has_value());
      }
    }
    EXPECT_TRUE(found_pushed);
  }
  {
    Memo memo;
    const GroupId scan = memo.Build({"t"});
    const GroupId agg_group = memo.EnsureDerivedGroup({"t"}, "scalar_agg");
    memo.AddExpression(
        agg_group,
        LogicalExpression{
            .operation = LogicalOperator::kAggregation,
            .children = {scan},
            .target_list = {
                NamedExpression("one", ConstantValueExp(Value(int64_t{1}))),
                NamedExpression("c", AggregateExpressionExp(
                                         AggregationType::kCount, nullptr))}});
    const GroupId sel_group =
        memo.EnsureDerivedGroup({"t"}, "sel_over_scalar_agg");
    memo.AddExpression(
        sel_group, LogicalExpression{.operation = LogicalOperator::kSelection,
                                     .children = {agg_group},
                                     .predicate = BinaryExpressionExp(
                                         ColumnValueExp(ColumnName("", "one")),
                                         BinaryOperation::kEquals,
                                         ConstantValueExp(Value(int64_t{2})))});

    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(sel_group);

    for (const LogicalExpression& expr :
         search.GetMemo().Get(sel_group).expressions) {
      if (expr.operation == LogicalOperator::kAggregation &&
          expr.children.size() == 1) {
        EXPECT_FALSE(search.GetMemo()
                         .Get(expr.children[0])
                         .tag.starts_with("sel-below-agg:"))
            << "Selection pushed below ungrouped scalar Aggregation";
      }
    }
  }
}

TEST(CascadesTest, DistinctOverGroupByRefusesWhenComputedGroupingKeyDropped) {
  // Lean audit (`cpp_distinct_over_group_by_skips_expr_key_bug`): when GROUP BY
  // contains an unprojected computed expression `t.b + 1`, two distinct groups
  // can share the projected plain column `t.a`, so DISTINCT must stay.
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId agg_group = memo.EnsureDerivedGroup({"t"}, "agg_expr_key");
  memo.AddExpression(
      agg_group,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {scan},
          .target_list = {NamedExpression("a", ColumnValueExp("t.a"))},
          .grouping_sets = {
              ColumnValueExp("t.a"),
              BinaryExpressionExp(ColumnValueExp("t.b"), BinaryOperation::kAdd,
                                  ConstantValueExp(Value(int64_t{1})))}});
  const GroupId dist_group =
      memo.EnsureDerivedGroup({"t"}, "dist_over_expr_key");
  memo.AddExpression(dist_group,
                     LogicalExpression{.operation = LogicalOperator::kDistinct,
                                       .children = {agg_group}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(dist_group);

  for (const LogicalExpression& expr :
       search.GetMemo().Get(dist_group).expressions) {
    if (expr.operation == LogicalOperator::kAggregation) {
      // `distinct_and_group_by_interchange` may rewrite Distinct(Agg) only if
      // it groups by `a` ABOVE `agg_group`, never directly over `scan`!
      EXPECT_NE(expr.children[0], scan) << "distinct_over_group_by unsoundly "
                                           "dropped DISTINCT over unprojected "
                                           "computed GROUP BY key";
    }
  }
  for (const LogicalExpression& expr :
       search.GetMemo().Get(agg_group).expressions) {
    EXPECT_NE(expr.operation, LogicalOperator::kDistinct)
        << "distinct_and_group_by_interchange unsoundly replaced GROUP BY(a, "
           "b+1) with Distinct(Project(a))";
  }
}

TEST(CascadesTest,
     AggregateUnionTransposeRefusesCompoundAggAndPreservesOuterHaving) {
  // Lean audit (`cpp_agg_union_compound_sum_plus_one_bug` &
  // `cpp_agg_union_having_pushed_to_branch_bug`):
  // 1) `SUM(x) + 1` is NOT a homomorphism over UNION ALL and must not push.
  // 2) `SUM(x)` with outer `HAVING` pushes per-branch `SUM` WITHOUT `HAVING`
  //    and retains `HAVING` on the final outer `Aggregation`.
  const auto run_union_agg = [](const Expression& target_expr,
                                std::optional<Expression> having) {
    Memo memo;
    (void)memo.Build({"t1", "t2"});
    const GroupId s1 = memo.EnsureGroup({"t1"});
    const GroupId s2 = memo.EnsureGroup({"t2"});
    const GroupId u_group = memo.EnsureDerivedGroup({"t1", "t2"}, "u");
    memo.AddExpression(
        u_group, LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                   .children = {s1, s2},
                                   .target_list = {NamedExpression(
                                       "x", ColumnValueExp("x"))}});
    const GroupId agg_group =
        memo.EnsureDerivedGroup({"t1", "t2"}, "agg_over_u");
    memo.AddExpression(
        agg_group,
        LogicalExpression{.operation = LogicalOperator::kAggregation,
                          .children = {u_group},
                          .predicate = std::move(having),
                          .target_list = {NamedExpression("s", target_expr)}});
    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(agg_group);
    return std::make_pair(std::move(search), agg_group);
  };

  {
    const Expression sum_plus_one = BinaryExpressionExp(
        AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("x")),
        BinaryOperation::kAdd, ConstantValueExp(Value(int64_t{1})));
    auto [search, agg_group] = run_union_agg(sum_plus_one, std::nullopt);
    for (const LogicalExpression& expr :
         search.GetMemo().Get(agg_group).expressions) {
      if (expr.operation == LogicalOperator::kAggregation &&
          expr.children.size() == 1) {
        EXPECT_FALSE(search.GetMemo()
                         .Get(expr.children[0])
                         .tag.starts_with("union_all_pushed_aggs:"))
            << "Compound SUM(x)+1 was unsoundly transposed below UNION ALL";
      }
    }
  }
  {
    const Expression plain_sum =
        AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("x"));
    const Expression having_gt_5 = BinaryExpressionExp(
        ColumnValueExp(ColumnName("", "s")), BinaryOperation::kGreaterThan,
        ConstantValueExp(Value(int64_t{5})));
    auto [search, agg_group] = run_union_agg(plain_sum, having_gt_5);
    bool found_pushed = false;
    for (const LogicalExpression& expr :
         search.GetMemo().Get(agg_group).expressions) {
      if (expr.operation == LogicalOperator::kAggregation &&
          expr.children.size() == 1 &&
          search.GetMemo()
              .Get(expr.children[0])
              .tag.starts_with("union_all_pushed_aggs:")) {
        found_pushed = true;
        EXPECT_TRUE(expr.predicate.has_value())
            << "Outer HAVING was dropped on final re-aggregation";
        for (const LogicalExpression& u :
             search.GetMemo().Get(expr.children[0]).expressions) {
          if (u.operation == LogicalOperator::kUnionAll) {
            for (const GroupId branch_g : u.children) {
              for (const LogicalExpression& branch_agg :
                   search.GetMemo().Get(branch_g).expressions) {
                if (branch_agg.operation == LogicalOperator::kAggregation) {
                  EXPECT_FALSE(branch_agg.predicate.has_value())
                      << "HAVING must never be pushed to individual UNION ALL "
                         "branches";
                }
              }
            }
          }
        }
      }
    }
    EXPECT_TRUE(found_pushed);
  }
}

TEST(CascadesTest, IntersectAndExceptRefuseDoubleNaNAndComputedBranchAlias) {
  // Lean audit (`cpp_intersect_except_nan_isNotDistinctFrom_bug` &
  // `cpp_intersect_computed_alias_bug`):
  // 1) SetOperationExecutor treats NaN == NaN, whereas IS NOT DISTINCT FROM
  //    evaluates NaN vs NaN to FALSE; DOUBLE columns must not rewrite to
  //    SemiJoin/AntiJoin.
  // 2) A branch projecting `t1.a + 10 AS a` must not resolve to base `t1.a`.
  {
    Memo memo;
    (void)memo.Build({"t1", "t2"});
    const GroupId left = memo.EnsureGroup({"t1"});
    const GroupId right = memo.EnsureGroup({"t2"});
    const GroupId intersect_double =
        memo.EnsureDerivedGroup({"t1", "t2"}, "intersect_double");
    memo.AddExpression(
        intersect_double,
        LogicalExpression{
            .operation = LogicalOperator::kIntersect,
            .children = {left, right},
            .output_schema = Schema("t1", {Column("d", ValueType::kDouble)})});
    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(intersect_double);
    for (const auto& expr :
         search.GetMemo().Get(intersect_double).expressions) {
      EXPECT_NE(expr.operation, LogicalOperator::kDistinct)
          << "DOUBLE INTERSECT was unsoundly rewritten to IS NOT DISTINCT FROM "
             "SemiJoin (breaks NaN == NaN)";
    }
  }
  {
    Memo memo;
    (void)memo.Build({"t1", "t2"});
    const GroupId s1 = memo.EnsureGroup({"t1"});
    const GroupId s2 = memo.EnsureGroup({"t2"});
    const GroupId left_proj = memo.EnsureDerivedGroup({"t1"}, "left_computed");
    memo.AddExpression(
        left_proj,
        LogicalExpression{
            .operation = LogicalOperator::kProjection,
            .children = {s1},
            .target_list = {NamedExpression(
                "a", BinaryExpressionExp(
                         ColumnValueExp("t1.a"), BinaryOperation::kAdd,
                         ConstantValueExp(Value(int64_t{10}))))},
            .output_schema = Schema("", {Column("a", ValueType::kInt64)})});
    const GroupId intersect_comp =
        memo.EnsureDerivedGroup({"t1", "t2"}, "intersect_computed");
    memo.AddExpression(
        intersect_comp,
        LogicalExpression{
            .operation = LogicalOperator::kIntersect,
            .children = {left_proj, s2},
            .output_schema = Schema("", {Column("a", ValueType::kInt64)})});
    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(intersect_comp);
    for (const auto& expr : search.GetMemo().Get(intersect_comp).expressions) {
      EXPECT_NE(expr.operation, LogicalOperator::kDistinct)
          << "Computed branch projection was falsely equated on base column";
    }
  }
}

TEST(CascadesTest,
     RedundantJoinPredicateEliminationKeepsUnconstrainedSelfEquality) {
  // Lean audit (`cpp_redundant_join_eq_drops_self_eq_null_bug`):
  // `t1.x = t1.x` rejects NULL and NaN on `t1.x`; union-find must not delete it
  // when `t1.x` has not appeared in any kept equality edge.
  Memo memo;
  (void)memo.Build({"t1", "t2"});
  const GroupId left = memo.EnsureGroup({"t1"});
  const GroupId right = memo.EnsureGroup({"t2"});
  const GroupId join_group = memo.EnsureDerivedGroup({"t1", "t2"}, "join_3");
  const Expression pred = CombineConjuncts({
      BinaryExpressionExp(ColumnValueExp("t1.id"), BinaryOperation::kEquals,
                          ColumnValueExp("t2.id")),
      BinaryExpressionExp(ColumnValueExp("t1.v"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(int64_t{0}))),
      BinaryExpressionExp(ColumnValueExp("t1.x"), BinaryOperation::kEquals,
                          ColumnValueExp("t1.x")),
  });
  memo.AddExpression(join_group,
                     LogicalExpression{.operation = LogicalOperator::kJoin,
                                       .children = {left, right},
                                       .predicate = pred});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(join_group);

  for (const LogicalExpression& expr :
       search.GetMemo().Get(join_group).expressions) {
    if (expr.operation == LogicalOperator::kJoin && expr.predicate &&
        *expr.predicate) {
      EXPECT_NE((*expr.predicate)->ToString().find("t1.x = t1.x"),
                std::string::npos)
          << "Unconstrained self-equality `t1.x = t1.x` was unsoundly dropped";
    }
  }
}

TEST(CascadesTest,
     EliminateSortUnderAggregationPreservesHavingAndRefusesNestedStringAgg) {
  // Lean audit (`cpp_elim_sort_under_agg_drops_having_bug`):
  // 1) Lowered Aggregation must retain `.predicate` (`HAVING`).
  // 2) Nested order-sensitive aggregates (`COALESCE(STRING_AGG(v), '')`) must
  //    keep the input `Sort`.
  {
    Memo memo;
    const GroupId scan = memo.Build({"t"});
    const GroupId sort_g = memo.EnsureDerivedGroup({"t"}, "sort_g");
    memo.AddExpression(sort_g,
                       LogicalExpression{.operation = LogicalOperator::kSort,
                                         .children = {scan},
                                         .target_list = {NamedExpression(
                                             "x", ColumnValueExp("t.x"))},
                                         .sort_ascending = {true}});
    const GroupId agg_g = memo.EnsureDerivedGroup({"t"}, "agg_with_having");
    memo.AddExpression(
        agg_g,
        LogicalExpression{
            .operation = LogicalOperator::kAggregation,
            .children = {sort_g},
            .predicate = BinaryExpressionExp(
                ColumnValueExp(ColumnName("", "s")),
                BinaryOperation::kGreaterThan, ConstantValueExp(Value(10))),
            .target_list = {NamedExpression(
                "s", AggregateExpressionExp(AggregationType::kSum,
                                            ColumnValueExp("t.v")))},
            .grouping_sets = {ColumnValueExp("t.k")}});
    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(agg_g);
    bool found_lowered = false;
    for (const auto& expr : search.GetMemo().Get(agg_g).expressions) {
      if (expr.operation == LogicalOperator::kAggregation &&
          expr.children.size() == 1 && expr.children[0] == scan) {
        found_lowered = true;
        EXPECT_TRUE(expr.predicate.has_value())
            << "HAVING predicate was dropped when eliminating Sort";
      }
    }
    EXPECT_TRUE(found_lowered);
  }
  {
    Memo memo;
    const GroupId scan = memo.Build({"t"});
    const GroupId sort_g = memo.EnsureDerivedGroup({"t"}, "sort_nested_str");
    memo.AddExpression(sort_g,
                       LogicalExpression{.operation = LogicalOperator::kSort,
                                         .children = {scan},
                                         .target_list = {NamedExpression(
                                             "x", ColumnValueExp("t.x"))},
                                         .sort_ascending = {true}});
    const GroupId agg_g = memo.EnsureDerivedGroup({"t"}, "agg_nested_str");
    const Expression nested_string_agg = BinaryExpressionExp(
        AggregateExpressionExp(AggregationType::kStringAgg,
                               ColumnValueExp("t.v")),
        BinaryOperation::kEquals, ConstantValueExp(Value("abc")));
    memo.AddExpression(
        agg_g, LogicalExpression{
                   .operation = LogicalOperator::kAggregation,
                   .children = {sort_g},
                   .target_list = {NamedExpression("ok", nested_string_agg)}});
    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(agg_g);
    for (const auto& expr : search.GetMemo().Get(agg_g).expressions) {
      EXPECT_FALSE(expr.operation == LogicalOperator::kAggregation &&
                   expr.children.size() == 1 && expr.children[0] == scan)
          << "Nested STRING_AGG lost its input Sort";
    }
  }
}

TEST(CascadesTest, InListToSemiJoinSeparatesSemicolonConstantFingerprints) {
  // Lean audit (`cpp_in_list_fingerprint_collision_bug`):
  // `['a;b', 'c', 'd']` and `['a', 'b;c', 'd']` must map to distinct Values
  // groups instead of colliding on `'a;b;c;d;'`.
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const auto make_or = [](const char* v1, const char* v2, const char* v3) {
    return BinaryExpressionExp(
        BinaryExpressionExp(
            BinaryExpressionExp(ColumnValueExp("t.s"), BinaryOperation::kEquals,
                                ConstantValueExp(Value(v1))),
            BinaryOperation::kOr,
            BinaryExpressionExp(ColumnValueExp("t.s"), BinaryOperation::kEquals,
                                ConstantValueExp(Value(v2)))),
        BinaryOperation::kOr,
        BinaryExpressionExp(ColumnValueExp("t.s"), BinaryOperation::kEquals,
                            ConstantValueExp(Value(v3))));
  };
  const GroupId sel1 = memo.EnsureDerivedGroup({"t"}, "sel_in_1");
  const GroupId sel2 = memo.EnsureDerivedGroup({"t"}, "sel_in_2");
  memo.AddExpression(sel1,
                     LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {scan},
                                       .predicate = make_or("a;b", "c", "d")});
  memo.AddExpression(sel2,
                     LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {scan},
                                       .predicate = make_or("a", "b;c", "d")});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel1);
  search.Explore(sel2);

  GroupId values1 = kInvalidGroup;
  GroupId values2 = kInvalidGroup;
  for (const auto& e : search.GetMemo().Get(sel1).expressions) {
    if (e.operation == LogicalOperator::kSemiJoin && e.children.size() == 2) {
      values1 = e.children[1];
    }
  }
  for (const auto& e : search.GetMemo().Get(sel2).expressions) {
    if (e.operation == LogicalOperator::kSemiJoin && e.children.size() == 2) {
      values2 = e.children[1];
    }
  }
  ASSERT_NE(values1, kInvalidGroup);
  ASSERT_NE(values2, kInvalidGroup);
  EXPECT_NE(values1, values2)
      << "Distinct string IN-lists collided onto the same Memo Values group";
}

TEST(CascadesTest, WindowRulesRespectMixedPartitionsContiguousOrderAndStars) {
  // M1a: `push_selection_through_window` must NOT push `t.p = 1` below a
  // mixed-spec kWindow node `[SUM(t.x) OVER (PARTITION BY t.p), SUM(t.y) OVER
  // (PARTITION BY t.q)]`.
  {
    Memo memo;
    const GroupId scan = memo.Build({"t"});
    const Expression w_p =
        WindowFunctionCallExp("sum", {ColumnValueExp(ColumnName("t", "x"))},
                              {ColumnValueExp(ColumnName("t", "p"))}, {});
    const Expression w_q =
        WindowFunctionCallExp("sum", {ColumnValueExp(ColumnName("t", "y"))},
                              {ColumnValueExp(ColumnName("t", "q"))}, {});
    const GroupId win = memo.EnsureDerivedGroup({"t"}, "mixed_win");
    memo.AddExpression(
        win, LogicalExpression{
                 .operation = LogicalOperator::kWindow,
                 .children = {scan},
                 .target_list = {NamedExpression("s1", w_p),
                                 NamedExpression("s2", w_q)},
                 .partition_by = {ColumnValueExp(ColumnName("t", "p"))}});
    const GroupId sel = memo.EnsureDerivedGroup({"t"}, "sel_over_mixed_win");
    memo.AddExpression(
        sel, LogicalExpression{
                 .operation = LogicalOperator::kSelection,
                 .children = {win},
                 .predicate = BinaryExpressionExp(
                     ColumnValueExp(ColumnName("t", "p")),
                     BinaryOperation::kEquals, ConstantValueExp(Value(1)))});
    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(sel);
    for (const auto& expr : search.GetMemo().Get(sel).expressions) {
      EXPECT_NE(expr.operation, LogicalOperator::kWindow)
          << "Selection on t.p was pushed below mixed PARTITION BY (t.p, t.q)";
    }
  }

  // M1b: `split_window` on non-contiguous `[w0(p), w1(q), w2(p)]` must split
  // into 3 contiguous levels so output column order `[..., w0, w1, w2]` is
  // preserved (grouping `[w0, w2]` then `[w1]` would swap w1 and w2).
  {
    Memo memo;
    const GroupId scan = memo.Build({"t"});
    const Expression w0 =
        WindowFunctionCallExp("sum", {ColumnValueExp(ColumnName("t", "x"))},
                              {ColumnValueExp(ColumnName("t", "p"))}, {});
    const Expression w1 =
        WindowFunctionCallExp("sum", {ColumnValueExp(ColumnName("t", "y"))},
                              {ColumnValueExp(ColumnName("t", "q"))}, {});
    const Expression w2 =
        WindowFunctionCallExp("count", {ColumnValueExp(ColumnName("t", "x"))},
                              {ColumnValueExp(ColumnName("t", "p"))}, {});
    const GroupId win = memo.EnsureDerivedGroup({"t"}, "noncontig_win");
    memo.AddExpression(
        win, LogicalExpression{
                 .operation = LogicalOperator::kWindow,
                 .children = {scan},
                 .target_list = {NamedExpression("w0", w0),
                                 NamedExpression("w1", w1),
                                 NamedExpression("w2", w2)},
                 .partition_by = {ColumnValueExp(ColumnName("t", "p"))}});
    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(win);
    bool found_w2_outer = false;
    for (const auto& expr : search.GetMemo().Get(win).expressions) {
      if (expr.operation == LogicalOperator::kWindow &&
          expr.target_list.size() == 1) {
        EXPECT_EQ(expr.target_list[0].name, "w2")
            << "Non-contiguous split_window permuted trailing output column";
        found_w2_outer = (expr.target_list[0].name == "w2");
      }
    }
    EXPECT_TRUE(found_w2_outer);
  }

  // M1c: `no_op_window_elimination` must NOT bypass a Window under `SELECT *`.
  {
    Memo memo;
    const GroupId scan = memo.Build({"t"});
    const Expression w0 = WindowFunctionCallExp("row_number", {}, {}, {});
    const GroupId win = memo.EnsureDerivedGroup({"t"}, "win_under_star");
    memo.AddExpression(
        win, LogicalExpression{.operation = LogicalOperator::kWindow,
                               .children = {scan},
                               .target_list = {NamedExpression("w0", w0)}});
    const GroupId proj = memo.EnsureDerivedGroup({"t"}, "proj_star");
    memo.AddExpression(
        proj,
        LogicalExpression{.operation = LogicalOperator::kProjection,
                          .children = {win},
                          .target_list = {NamedExpression(
                              "*", ColumnValueExp(ColumnName("", "*")))}});
    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(proj);
    for (const auto& expr : search.GetMemo().Get(proj).expressions) {
      if (expr.operation == LogicalOperator::kProjection &&
          expr.children.size() == 1) {
        EXPECT_NE(expr.children[0], scan)
            << "SELECT * bypassed Window and dropped window output columns";
      }
    }
  }
}

TEST(CascadesTest, PushProjectionThroughAggregationKeepsHavingColumns) {
  // M2 (`cpp_push_proj_through_agg_prunes_having_col_bug`):
  // `SELECT t.g FROM (SELECT t.g, COUNT(*) FROM t GROUP BY t.g HAVING
  // SUM(t.secret) > 10)` must retain `t.secret` in `proj_below`.
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId agg = memo.EnsureDerivedGroup({"t"}, "agg_with_having");
  const Expression having = BinaryExpressionExp(
      AggregateExpressionExp(AggregationType::kSum,
                             ColumnValueExp(ColumnName("t", "secret")), false),
      BinaryOperation::kGreaterThan, ConstantValueExp(Value(10)));
  memo.AddExpression(
      agg,
      LogicalExpression{
          .operation = LogicalOperator::kAggregation,
          .children = {scan},
          .predicate = having,
          .target_list = {NamedExpression("g",
                                          ColumnValueExp(ColumnName("t", "g"))),
                          NamedExpression("cnt", AggregateExpressionExp(
                                                     AggregationType::kCount,
                                                     nullptr, false))},
          .grouping_sets = {ColumnValueExp(ColumnName("t", "g"))}});
  const GroupId outer_proj = memo.EnsureDerivedGroup({"t"}, "outer_proj_g");
  memo.AddExpression(
      outer_proj,
      LogicalExpression{.operation = LogicalOperator::kProjection,
                        .children = {agg},
                        .target_list = {NamedExpression(
                            "g", ColumnValueExp(ColumnName("", "g")))}});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(outer_proj);

  bool checked_pruned_agg = false;
  for (const auto& expr : search.GetMemo().Get(outer_proj).expressions) {
    if (expr.operation != LogicalOperator::kProjection ||
        expr.children.size() != 1 || expr.children[0] == agg) {
      continue;
    }
    for (const auto& cand_agg :
         search.GetMemo().Get(expr.children[0]).expressions) {
      if (cand_agg.operation != LogicalOperator::kAggregation ||
          cand_agg.children.size() != 1 || cand_agg.children[0] == scan) {
        continue;
      }
      for (const auto& below :
           search.GetMemo().Get(cand_agg.children[0]).expressions) {
        if (below.operation == LogicalOperator::kProjection) {
          checked_pruned_agg = true;
          const bool keeps_secret = std::ranges::any_of(
              below.target_list, [](const NamedExpression& t) {
                return t.expression && t.expression->ToString().find(
                                           "secret") != std::string::npos;
              });
          EXPECT_TRUE(keeps_secret)
              << "push_projection_through_aggregation pruned HAVING column "
                 "t.secret";
        }
      }
    }
  }
  EXPECT_TRUE(checked_pruned_agg);
}

TEST(CascadesTest,
     UnnestFilterPushdownBlocksDefaultAliasAndThrowingPredicates) {
  // M3a & M3b (`cpp_unnest_alias_and_empty_array_throw_bugs`):
  // 1. Default empty `unnest_alias` names its output `"unnest"` and must block
  //    pushing `WHERE unnest > 0`.
  // 2. Potentially-throwing `WHERE 10 / t.x > 1` must not push below UNNEST
  //    (empty arrays on `t.x = 0` would otherwise raise division by zero).
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId unnest_grp = memo.EnsureDerivedGroup({"t"}, "unnest_no_alias");
  memo.AddExpression(
      unnest_grp,
      LogicalExpression{.operation = LogicalOperator::kUnnest,
                        .children = {scan},
                        .predicate = ColumnValueExp(ColumnName("t", "arr")),
                        .unnest_alias = ""});

  const GroupId sel_default_alias =
      memo.EnsureDerivedGroup({"t"}, "sel_default_unnest_col");
  memo.AddExpression(
      sel_default_alias,
      LogicalExpression{
          .operation = LogicalOperator::kSelection,
          .children = {unnest_grp},
          .predicate = BinaryExpressionExp(
              ColumnValueExp(ColumnName("", "unnest")),
              BinaryOperation::kGreaterThan, ConstantValueExp(Value(0)))});

  const GroupId sel_throwing =
      memo.EnsureDerivedGroup({"t"}, "sel_throwing_over_unnest");
  memo.AddExpression(
      sel_throwing,
      LogicalExpression{
          .operation = LogicalOperator::kSelection,
          .children = {unnest_grp},
          .predicate = BinaryExpressionExp(
              BinaryExpressionExp(ConstantValueExp(Value(10)),
                                  BinaryOperation::kDivide,
                                  ColumnValueExp(ColumnName("t", "x"))),
              BinaryOperation::kGreaterThan, ConstantValueExp(Value(1)))});

  SearchEngine search(std::move(memo), RuleSet::Default());
  search.Explore(sel_default_alias);
  search.Explore(sel_throwing);

  for (const auto& e : search.GetMemo().Get(sel_default_alias).expressions) {
    EXPECT_NE(e.operation, LogicalOperator::kUnnest)
        << "Predicate referencing default `unnest` column pushed below Unnest";
  }
  for (const auto& e : search.GetMemo().Get(sel_throwing).expressions) {
    EXPECT_NE(e.operation, LogicalOperator::kUnnest)
        << "Throwing division predicate pushed below Unnest";
  }
}

TEST(CascadesTest, DynamicFilterAndParameterSlotsPreserveSemantics) {
  // M4 (`cpp_dynamic_filter_erases_derived_probe_bug`):
  // `dynamic_filter_pushdown_join` must NOT replace a derived `Limit(Scan(t2))`
  // probe group with a raw `Scan(t2)`.
  {
    Memo memo;
    (void)memo.Build({"t1", "t2"});
    const GroupId g1 = memo.EnsureGroup({"t1"});
    const GroupId g2 = memo.EnsureGroup({"t2"});
    const GroupId lim2 = memo.EnsureDerivedGroup({"t2"}, "limit_t2");
    memo.AddExpression(lim2,
                       LogicalExpression{.operation = LogicalOperator::kLimit,
                                         .children = {g2},
                                         .limit_count = 1,
                                         .limit_offset = 0});
    const GroupId join_grp = memo.EnsureDerivedGroup({"t1", "t2"}, "join_lim");
    memo.AddExpression(
        join_grp,
        LogicalExpression{.operation = LogicalOperator::kJoin,
                          .children = {g1, lim2},
                          .predicate = BinaryExpressionExp(
                              ColumnValueExp(ColumnName("t1", "id")),
                              BinaryOperation::kEquals,
                              ColumnValueExp(ColumnName("t2", "id")))});
    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(join_grp);
    for (const auto& j : search.GetMemo().Get(join_grp).expressions) {
      if (j.operation == LogicalOperator::kJoin && j.children.size() == 2) {
        for (const GroupId child : j.children) {
          if (search.GetMemo().Get(child).relations ==
              std::vector<std::string>{"t2"}) {
            EXPECT_FALSE(
                search.GetMemo().Get(child).tag.starts_with("bloom_probe:"))
                << "Derived Limit(t2) probe was overwritten by raw kScan(t2)";
          }
        }
      }
    }
  }

  // M6 (`cpp_parameter_slot_freeze_bug`):
  // A `Selection` whose predicate is an unbound/false `ParameterSlot` must NOT
  // collapse into `kEmpty`.
  {
    Memo memo;
    const GroupId scan = memo.Build({"t"});
    const Expression slot = std::make_shared<ParameterSlot>(0, Value());
    const GroupId sel = memo.EnsureDerivedGroup({"t"}, "sel_param");
    memo.AddExpression(
        sel, LogicalExpression{.operation = LogicalOperator::kSelection,
                               .children = {scan},
                               .predicate = slot});
    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(sel);
    for (const auto& e : search.GetMemo().Get(sel).expressions) {
      EXPECT_NE(e.operation, LogicalOperator::kEmpty)
          << "ParameterSlot selection froze to kEmpty at compile time";
    }
  }
}

TEST(CascadesTest, TpcdsDisjointUnionGroupByBypassAndNullTagGuard) {
  // 1) Pairwise-distinct non-NULL constant tags ('store', 'web') push full
  //    Aggregation (even AVG + HAVING) directly into branches and emit
  //    kUnionAll at the root without an outer re-aggregation
  //    (`disjoint_union_group_by_bypass_sound`).
  {
    Memo memo;
    (void)memo.Build({"s", "w"});
    const GroupId gs = memo.EnsureGroup({"s"});
    const GroupId gw = memo.EnsureGroup({"w"});
    const GroupId ps = memo.EnsureDerivedGroup({"s"}, "proj_s");
    memo.AddExpression(
        ps,
        LogicalExpression{
            .operation = LogicalOperator::kProjection,
            .children = {gs},
            .target_list = {
                NamedExpression("ch", ConstantValueExp(Value("store"))),
                NamedExpression("k", ColumnValueExp(ColumnName("s", "k"))),
                NamedExpression("v", ColumnValueExp(ColumnName("s", "v")))}});
    const GroupId pw = memo.EnsureDerivedGroup({"w"}, "proj_w");
    memo.AddExpression(
        pw,
        LogicalExpression{
            .operation = LogicalOperator::kProjection,
            .children = {gw},
            .target_list = {
                NamedExpression("ch", ConstantValueExp(Value("web"))),
                NamedExpression("k", ColumnValueExp(ColumnName("w", "k"))),
                NamedExpression("v", ColumnValueExp(ColumnName("w", "v")))}});
    const GroupId u = memo.EnsureDerivedGroup({"s", "w"}, "u_disjoint");
    memo.AddExpression(
        u, LogicalExpression{.operation = LogicalOperator::kUnionAll,
                             .children = {ps, pw}});
    const GroupId agg = memo.EnsureDerivedGroup({"s", "w"}, "agg_disjoint");
    memo.AddExpression(
        agg,
        LogicalExpression{
            .operation = LogicalOperator::kAggregation,
            .children = {u},
            .predicate = BinaryExpressionExp(
                ColumnValueExp(ColumnName("", "avg_v")),
                BinaryOperation::kGreaterThan, ConstantValueExp(Value(10))),
            .target_list =
                {NamedExpression("ch", ColumnValueExp(ColumnName("", "ch"))),
                 NamedExpression("k", ColumnValueExp(ColumnName("", "k"))),
                 NamedExpression(
                     "avg_v", AggregateExpressionExp(
                                  AggregationType::kAvg,
                                  ColumnValueExp(ColumnName("", "v")), false))},
            .grouping_sets = {ColumnValueExp(ColumnName("", "ch")),
                              ColumnValueExp(ColumnName("", "k"))}});

    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(agg);

    bool found_direct_union = false;
    for (const auto& e : search.GetMemo().Get(agg).expressions) {
      if (e.operation == LogicalOperator::kUnionAll && e.children.size() == 2) {
        found_direct_union = true;
        break;
      }
    }
    EXPECT_TRUE(found_direct_union);
  }

  // 2) NULL constant tags (`agg_union_null_tag_collision_bug`) or colliding
  //    tags MUST NOT bypass outer re-aggregation!
  {
    Memo memo;
    (void)memo.Build({"s", "w"});
    const GroupId gs = memo.EnsureGroup({"s"});
    const GroupId gw = memo.EnsureGroup({"w"});
    const GroupId ps = memo.EnsureDerivedGroup({"s"}, "proj_s_null");
    memo.AddExpression(
        ps,
        LogicalExpression{
            .operation = LogicalOperator::kProjection,
            .children = {gs},
            .target_list = {
                NamedExpression("ch", ConstantValueExp(Value())),
                NamedExpression("v", ColumnValueExp(ColumnName("s", "v")))}});
    const GroupId pw = memo.EnsureDerivedGroup({"w"}, "proj_w_null");
    memo.AddExpression(
        pw,
        LogicalExpression{
            .operation = LogicalOperator::kProjection,
            .children = {gw},
            .target_list = {
                NamedExpression("ch", ConstantValueExp(Value())),
                NamedExpression("v", ColumnValueExp(ColumnName("w", "v")))}});
    const GroupId u = memo.EnsureDerivedGroup({"s", "w"}, "u_null_tag");
    memo.AddExpression(
        u, LogicalExpression{.operation = LogicalOperator::kUnionAll,
                             .children = {ps, pw}});
    const GroupId agg = memo.EnsureDerivedGroup({"s", "w"}, "agg_null_tag");
    memo.AddExpression(
        agg,
        LogicalExpression{
            .operation = LogicalOperator::kAggregation,
            .children = {u},
            .target_list =
                {NamedExpression("ch", ColumnValueExp(ColumnName("", "ch"))),
                 NamedExpression(
                     "avg_v", AggregateExpressionExp(
                                  AggregationType::kAvg,
                                  ColumnValueExp(ColumnName("", "v")), false))},
            .grouping_sets = {ColumnValueExp(ColumnName("", "ch"))}});

    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(agg);
    for (const auto& e : search.GetMemo().Get(agg).expressions) {
      EXPECT_NE(e.operation, LogicalOperator::kUnionAll)
          << "NULL tags collided in GROUP BY yet bypassed re-aggregation!";
    }
  }
}

TEST(CascadesTest,
     TpcdsOuterToAntiThroughInnerAndSelfJoinWindowAndEagerDistinct) {
  // 1) `outer_to_anti_join_through_inner_join`:
  //    Selection(Join(LeftOuterJoin(A, B, a.id = b.id), C, a.dt = c.dt), b.id
  //    IS NULL)
  //    -> AntiJoin(Join(A, C, a.dt = c.dt), B, a.id = b.id)
  {
    Memo memo;
    (void)memo.Build({"a", "b", "c"});
    const GroupId ga = memo.EnsureGroup({"a"});
    const GroupId gb = memo.EnsureGroup({"b"});
    const GroupId gc = memo.EnsureGroup({"c"});
    const GroupId ab = memo.EnsureDerivedGroup({"a", "b"}, "loj_ab");
    memo.AddExpression(
        ab, Memo::NewOuterJoin(
                ga, gb,
                BinaryExpressionExp(ColumnValueExp(ColumnName("a", "id")),
                                    BinaryOperation::kEquals,
                                    ColumnValueExp(ColumnName("b", "id"))),
                0));
    const GroupId abc = memo.EnsureDerivedGroup({"a", "b", "c"}, "join_abc");
    memo.AddExpression(
        abc, LogicalExpression{.operation = LogicalOperator::kJoin,
                               .children = {ab, gc},
                               .predicate = BinaryExpressionExp(
                                   ColumnValueExp(ColumnName("a", "dt")),
                                   BinaryOperation::kEquals,
                                   ColumnValueExp(ColumnName("c", "dt")))});
    const GroupId sel = memo.EnsureDerivedGroup({"a", "b", "c"}, "sel_anti");
    memo.AddExpression(
        sel, LogicalExpression{.operation = LogicalOperator::kSelection,
                               .children = {abc},
                               .predicate = UnaryExpressionExp(
                                   ColumnValueExp(ColumnName("b", "id")),
                                   UnaryOperation::kIsNull)});

    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(sel);
    bool found_anti = false;
    for (const auto& e : search.GetMemo().Get(sel).expressions) {
      if (e.operation == LogicalOperator::kAntiJoin && e.children.size() == 2 &&
          e.children[1] == gb) {
        found_anti = true;
        break;
      }
    }
    EXPECT_TRUE(found_anti);
  }

  // 2) `self_join_agg_to_window`:
  //    Projection(Selection(Join(Scan(sales_1), Agg(Scan(sales_2))),
  //    sales_1.v > avg_v)) -> Projection(Selection(sales_1.g IS NOT NULL AND
  //    sales_1.v > $win, Window(Scan(sales_1))))
  {
    Memo memo;
    (void)memo.Build({"sales_1", "sales_2"});
    // The rewrite proves same-table through the catalog-published schemas:
    // both aliases resolve to the physical table `sales`.
    const Schema sales_schema("sales", {Column("g", ValueType::kInt64),
                                        Column("v", ValueType::kInt64),
                                        Column("id", ValueType::kInt64)});
    memo.SetTableSchemas(
        {{"sales_1", sales_schema}, {"sales_2", sales_schema}});
    const GroupId gs = memo.EnsureGroup({"sales_1"});
    const GroupId gb_scan = memo.EnsureGroup({"sales_2"});
    const GroupId gb_agg = memo.EnsureDerivedGroup({"sales_2"}, "b_agg");
    memo.AddExpression(
        gb_agg,
        LogicalExpression{
            .operation = LogicalOperator::kAggregation,
            .children = {gb_scan},
            .target_list = {NamedExpression("g", ColumnValueExp(ColumnName(
                                                     "sales_2", "g"))),
                            NamedExpression(
                                "avg_v",
                                AggregateExpressionExp(
                                    AggregationType::kAvg,
                                    ColumnValueExp(ColumnName("sales_2", "v")),
                                    false))},
            .grouping_sets = {ColumnValueExp(ColumnName("sales_2", "g"))}});
    const GroupId j =
        memo.EnsureDerivedGroup({"sales_1", "sales_2"}, "sb_join");
    memo.AddExpression(
        j, LogicalExpression{.operation = LogicalOperator::kJoin,
                             .children = {gs, gb_agg},
                             .predicate = BinaryExpressionExp(
                                 ColumnValueExp(ColumnName("sales_1", "g")),
                                 BinaryOperation::kEquals,
                                 ColumnValueExp(ColumnName("sales_2", "g")))});
    const GroupId sel =
        memo.EnsureDerivedGroup({"sales_1", "sales_2"}, "sb_sel");
    memo.AddExpression(
        sel, LogicalExpression{.operation = LogicalOperator::kSelection,
                               .children = {j},
                               .predicate = BinaryExpressionExp(
                                   ColumnValueExp(ColumnName("sales_1", "v")),
                                   BinaryOperation::kGreaterThan,
                                   ColumnValueExp(ColumnName("", "avg_v")))});
    const GroupId proj =
        memo.EnsureDerivedGroup({"sales_1", "sales_2"}, "sb_proj");
    memo.AddExpression(
        proj, LogicalExpression{
                  .operation = LogicalOperator::kProjection,
                  .children = {sel},
                  .target_list = {NamedExpression(
                      "id", ColumnValueExp(ColumnName("sales_1", "id")))}});

    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(proj);
    bool found_win_alt = false;
    for (const auto& pe : search.GetMemo().Get(proj).expressions) {
      if (pe.operation == LogicalOperator::kProjection &&
          pe.children.size() == 1 &&
          search.GetMemo()
              .Get(pe.children[0])
              .tag.starts_with("self_join_win_sel:")) {
        found_win_alt = true;
        break;
      }
    }
    EXPECT_TRUE(found_win_alt);
  }

  // 2b) `self_join_agg_to_window` must NOT fire for distinct real tables
  //     whose names differ only by an "_N" suffix: the catalog proof (not
  //     alias-stripping) is what decides same-table, and firing would both
  //     misread `orders_1` rows as `orders` and violate the group's
  //     relation set.
  {
    Memo memo;
    (void)memo.Build({"orders", "orders_1"});
    const Schema orders_schema("orders", {Column("g", ValueType::kInt64),
                                          Column("v", ValueType::kInt64),
                                          Column("id", ValueType::kInt64)});
    const Schema orders1_schema("orders_1", {Column("g", ValueType::kInt64),
                                             Column("v", ValueType::kInt64),
                                             Column("id", ValueType::kInt64)});
    memo.SetTableSchemas(
        {{"orders", orders_schema}, {"orders_1", orders1_schema}});
    const GroupId gs = memo.EnsureGroup({"orders"});
    const GroupId gb_scan = memo.EnsureGroup({"orders_1"});
    const GroupId gb_agg = memo.EnsureDerivedGroup({"orders_1"}, "b_agg");
    memo.AddExpression(
        gb_agg,
        LogicalExpression{
            .operation = LogicalOperator::kAggregation,
            .children = {gb_scan},
            .target_list = {NamedExpression("g", ColumnValueExp(ColumnName(
                                                     "orders_1", "g"))),
                            NamedExpression(
                                "avg_v",
                                AggregateExpressionExp(
                                    AggregationType::kAvg,
                                    ColumnValueExp(ColumnName("orders_1", "v")),
                                    false))},
            .grouping_sets = {ColumnValueExp(ColumnName("orders_1", "g"))}});
    const GroupId j =
        memo.EnsureDerivedGroup({"orders", "orders_1"}, "distinct_selfjoin");
    memo.AddExpression(
        j, LogicalExpression{.operation = LogicalOperator::kJoin,
                             .children = {gs, gb_agg},
                             .predicate = BinaryExpressionExp(
                                 ColumnValueExp(ColumnName("orders", "g")),
                                 BinaryOperation::kEquals,
                                 ColumnValueExp(ColumnName("orders_1", "g")))});
    const GroupId sel = memo.EnsureDerivedGroup({"orders", "orders_1"},
                                                "distinct_selfjoin_sel");
    memo.AddExpression(
        sel, LogicalExpression{.operation = LogicalOperator::kSelection,
                               .children = {j},
                               .predicate = BinaryExpressionExp(
                                   ColumnValueExp(ColumnName("orders", "v")),
                                   BinaryOperation::kGreaterThan,
                                   ColumnValueExp(ColumnName("", "avg_v")))});
    const GroupId proj = memo.EnsureDerivedGroup({"orders", "orders_1"},
                                                 "distinct_selfjoin_proj");
    memo.AddExpression(
        proj, LogicalExpression{
                  .operation = LogicalOperator::kProjection,
                  .children = {sel},
                  .target_list = {NamedExpression(
                      "id", ColumnValueExp(ColumnName("orders", "id")))}});

    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(proj);  // must not abort and must not add the rewrite
    for (const auto& pe : search.GetMemo().Get(proj).expressions) {
      if (pe.operation == LogicalOperator::kProjection &&
          pe.children.size() == 1) {
        EXPECT_FALSE(search.GetMemo()
                         .Get(pe.children[0])
                         .tag.starts_with("self_join_win_sel:"))
            << "distinct real tables must not collapse into a window scan";
      }
    }
  }

  // 3) `eager_distinct_fact_keys_before_join`:
  //    Distinct(Projection_{d1.c, d2.dt}(Join(Join(f, d1), d2)))
  //    adds an alternative with `Distinct(Projection_{f.k1, f.k2}(f))`!
  {
    Memo memo;
    (void)memo.Build({"f", "d1", "d2"});
    const GroupId gf = memo.EnsureGroup({"f"});
    const GroupId gd1 = memo.EnsureGroup({"d1"});
    const GroupId gd2 = memo.EnsureGroup({"d2"});
    const GroupId j1 = memo.EnsureDerivedGroup({"f", "d1"}, "j1");
    memo.AddExpression(
        j1, LogicalExpression{.operation = LogicalOperator::kJoin,
                              .children = {gf, gd1},
                              .predicate = BinaryExpressionExp(
                                  ColumnValueExp(ColumnName("f", "c_sk")),
                                  BinaryOperation::kEquals,
                                  ColumnValueExp(ColumnName("d1", "c_sk")))});
    const GroupId j2 = memo.EnsureDerivedGroup({"f", "d1", "d2"}, "j2");
    memo.AddExpression(
        j2, LogicalExpression{.operation = LogicalOperator::kJoin,
                              .children = {j1, gd2},
                              .predicate = BinaryExpressionExp(
                                  ColumnValueExp(ColumnName("f", "d_sk")),
                                  BinaryOperation::kEquals,
                                  ColumnValueExp(ColumnName("d2", "d_sk")))});
    const GroupId proj = memo.EnsureDerivedGroup({"f", "d1", "d2"}, "proj_dim");
    memo.AddExpression(
        proj, LogicalExpression{
                  .operation = LogicalOperator::kProjection,
                  .children = {j2},
                  .target_list = {
                      NamedExpression("name",
                                      ColumnValueExp(ColumnName("d1", "name"))),
                      NamedExpression(
                          "dt", ColumnValueExp(ColumnName("d2", "dt")))}});
    const GroupId dist = memo.EnsureDerivedGroup({"f", "d1", "d2"}, "dist_dim");
    memo.AddExpression(
        dist, LogicalExpression{.operation = LogicalOperator::kDistinct,
                                .children = {proj}});

    SearchEngine search(std::move(memo), RuleSet::Default());
    search.Explore(dist);
    bool found_eager_dist = false;
    for (size_t gid = 0; gid < search.GetMemo().GroupCount(); ++gid) {
      if (search.GetMemo()
              .Get(static_cast<GroupId>(gid))
              .tag.starts_with("eager_distinct_fact:")) {
        found_eager_dist = true;
        break;
      }
    }
    EXPECT_TRUE(found_eager_dist);
  }
}

}  // namespace tinylamb::cascades
