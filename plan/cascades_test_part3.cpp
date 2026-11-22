/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/cascades_test.hpp"

namespace tinylamb::cascades {

TEST(CascadesTest, TopNProjectionVariantsUseDistinctGroups) {
  Memo memo;
  const GroupId scan = memo.Build({"t"});
  const GroupId projection = memo.EnsureDerivedGroup({"t"}, "projection");
  ASSERT_TRUE(memo.AddExpression(
      projection, LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {scan},
                                    .target_list = {NamedExpression(
                                        "a", ColumnValueExp("t.a"))}}));
  std::vector<GroupId> tops;
  for (int i = 0; i < 4; ++i) {
    const GroupId top =
        memo.EnsureDerivedGroup({"t"}, "top-" + std::to_string(i));
    ASSERT_TRUE(memo.AddExpression(
        top, LogicalExpression{
                 .operation = LogicalOperator::kTopN,
                 .children = {projection},
                 .target_list = {NamedExpression("a", ColumnValueExp("a"))},
                 .sort_ascending = {i != 1},
                 .sort_nulls_first = {i != 2},
                 .limit_count = 5,
                 .with_ties = i == 3}));
    tops.push_back(top);
  }
  SearchEngine search(std::move(memo), RuleSet::Default());
  for (const auto top : tops) {
    search.Explore(top);
  }
  std::vector<GroupId> pushed;
  for (const auto top : tops) {
    for (const auto& expr : search.GetMemo().Get(top).expressions) {
      if (expr.operation == LogicalOperator::kProjection &&
          !expr.children.empty()) {
        pushed.push_back(expr.children[0]);
      }
    }
  }
  ASSERT_EQ(pushed.size(), 4U);
  for (size_t i = 0; i < pushed.size(); ++i) {
    for (size_t j = i + 1; j < pushed.size(); ++j) {
      EXPECT_NE(pushed[i], pushed[j]);
    }
  }
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
  // D5 (docs/design.md, "ルールゲートの必須検証" / derived group): two
  // IN-lists over the same column with the same SIZE but different values
  // must not share one derived constant group (a count-only tag would
  // collide and mix both value sets). The gate requires a fingerprint over
  // semantic content, not a fixed string or a mere count.
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

  // Memory spill cost monotonicity.
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

TEST(CascadesTest, TopNPushThroughProvenOneToOneJoinRejectsFilteredOtherSide) {
  // The FK certificate is scan-free: it cannot see that the other side's
  // WHERE conjunct (`dim.val > 0`) removes exactly the row it guarantees, so
  // a fact row inside the pushed top-(limit+offset) prefix would keep a
  // joined row the plain join filters away.
  TopNProvenJoinFixture fixture(/*fk=*/true, /*mark=*/false,
                                /*fk_reference=*/"dim(id)",
                                /*filter_null=*/true, /*residual=*/false,
                                /*other_side_filter=*/true);
  const GroupId topn = fixture.topn;
  SearchEngine search(std::move(fixture.memo), RuleSet::Default());
  search.Explore(topn);
  EXPECT_FALSE(search.AppliedRuleNames().contains(
      "topn_push_through_proven_one_to_one_join"));
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
  pushed_join_tags.reserve(search.GetMemo().GroupCount());
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

TEST(CascadesTest, EliminateTrueSelectionPreservesInputScanFilter) {
  // Selection(true, X) copies X's alternatives into its own group so costing
  // can skip the residual filter. The input group's pushed-down scan filter
  // must travel with them (the same contract
  // pk_unique_distinct_elimination follows): without the merge the copied
  // bare scan implements with an empty filter and the WHERE conditions are
  // dropped. Run the rule in isolation so push_selection_into_scan cannot
  // mask the missing merge.
  Memo memo;
  (void)memo.Build({"t"});
  const GroupId filtered = memo.EnsureDerivedGroup({"t"}, "filtered_input");
  memo.AddExpression(
      filtered,
      LogicalExpression{.operation = LogicalOperator::kScan, .table = "t"});
  memo.MergeScanFilter(
      filtered, BinaryExpressionExp(ColumnValueExp(ColumnName("t", "v")),
                                    BinaryOperation::kGreaterThan,
                                    ConstantValueExp(Value(int64_t{5}))));
  const GroupId root = memo.EnsureDerivedGroup({"t"}, "true_selection");
  memo.AddExpression(
      root, LogicalExpression{.operation = LogicalOperator::kSelection,
                              .children = {filtered},
                              .predicate = ConstantValueExp(Value(true))});
  RuleSet rules = RuleSet::Default();
  for (const std::string& name : rules.Names()) {
    if (name != "eliminate_true_selection") {
      rules.Remove(name);
    }
  }
  SearchEngine search(std::move(memo), rules);
  search.Explore(root);
  const Group& after = search.GetMemo().Get(root);
  ASSERT_TRUE(after.filter);
  EXPECT_NE(after.filter->ToString().find("t.v"), std::string::npos);
  bool copied_scan = false;
  for (const auto& expr : after.expressions) {
    copied_scan |= expr.operation == LogicalOperator::kScan;
  }
  EXPECT_TRUE(copied_scan);
}

}  // namespace tinylamb::cascades
