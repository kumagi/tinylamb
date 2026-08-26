# Optimizer TODO

Cascades に載せるべき変換・実装規則・コスト/統計の候補。**優先度の高い順**。
対応する物理プランと実行器は [`executor_todo.md`](executor_todo.md) を見る。

マーク:

- `[x]` 既に `RuleSet::Default` / `ExpressionRuleSet::Default` /
  `ImplementationRuleSet` にある（完全度は括弧内）。
- `[ ]` 未実装。括弧は Calcite / PostgreSQL / SQL Server / Cockroach /
  DuckDB 系の通称。

ガードレール（既存コードのコメントと一致）:

- 外側結合への pushdown は **null-rejection 解析が先**。
- `Selection(Limit)` と `Limit(Selection)` は一般には非等価。交換しない。
- `Memo::NewJoin` を経由しない結合合成は禁止（conjunct の二重適用/欠落）。
- LIMIT を畳むのは required ordering を満たす子の上だけ（D6）。

---

## P0 — 論理オペレータとメモの土台

今の `LogicalOperator` は Scan / Join / Selection / Projection /
Aggregation / Limit / Relational（不透明）だけ。商用相当にするには先に
**種類を増やす**。

- [x] `kOuterJoin`（LEFT / RIGHT / FULL）と join type ペイロード（`LogicalOperator::kOuterJoin` として enum 追加済み）
- [x] `kSemiJoin` / `kAntiJoin` / `kMarkJoin` / `kSingleJoin`（スカラ相関）`LogicalJoinKind` で実装済み
- [x] `kCrossJoin` を inner と分離（カーディナリティと prune が違う）（`LogicalOperator::kCrossJoin` として enum 追加済み）
- [ ] `kApply` / `kLateralJoin`（相関 APPLY）
- [ ] `kUnion` / `kUnionAll` / `kIntersect` / `kIntersectAll` / `kExcept` /
      `kExceptAll`
- [ ] `kWindow`（PARTITION / ORDER / frame）
- [x] `kSort` を論理ノードとして明示（今はエンジン側 Sort 安全網）（`LogicalOperator::kSort` + `sort_expressions` ペイロード追加済み）
- [x] `kDistinct` / `kDuplicateElim`（`LogicalOperator::kDistinct` として enum 追加済み）
- [x] `kTopN`（ORDER BY + LIMIT を 1 ノードに）（`LogicalOperator::kTopN` + `sort_expressions`/`limit_count` ペイロード追加済み）
- [ ] `kValues` / `kConstantTable`
- [ ] `kUnnest` / `kGenerateSeries` / TVF
- [ ] `kExpand`（CUBE / ROLLUP / GROUPING SETS）
- [ ] `kRecursiveCte` / `kWorkTableScan`
- [ ] `kMaterialize` / `kEagerSpool` / `kLazySpool`
- [ ] `kExchange` / `kGather` / `kBroadcast` / `kRedistribute`（分散予約）
- [ ] `kSample` / `kTableSample`
- [ ] `kAssert` / `kMax1Row`（スカラサブクエリ基数）
- [ ] `kFilterFalse` / `kEmpty`（矛盾述語の刈り込み結果）
- [ ] `kDummyScan`（`SELECT 1` の 1 行ソースを Relational から外す）
- [x] Join ペイロードに `JoinKind` + null-producing side（`LogicalJoinKind` + `join_kind` フィールド）
- [ ] Aggregation ペイロードに grouping sets / DISTINCT agg / FILTER /
      ORDER BY WITHIN GROUP
- [ ] Projection ペイロードに computed vs passthrough 列の区別
- [ ] Window ペイロードに frame 型（ROWS / RANGE / GROUPS）と exclusion
- [ ] Set-op ペイロードに ALL vs DISTINCT と列対応
- [ ] `PhysicalProperties` に collation、nulls first/last、partitioning、
      uniqueness、sorted-prefix、bloom/filter 伝播
- [ ] グループの出力スキーマを常に保持（identity projection 判定に必要）
- [ ] null-rejection / strong-null / weak-null 解析 API
- [ ] FD（関数従属）・unique key・not-null をグループ属性として伝播
- [ ] 等価クラス（equivalence class）をメモ全体で共有
- [ ] 述語の正規形（CNF / DNF）を選択的に保持
- [ ] 外側結合の null-supplying side をパターン DSL で制約
- [ ] `NeedsRelationalEvaluation` 経路を段階的に Cascades へ移す計画表

---

## P0 — 既にあるもの（再実装しない）

- [x] `join_commutativity`（inner のみ）
- [x] `join_enumeration`（関係 ≤16、非連結カット prune）
- [x] `join_associativity_left` / `join_associativity_right`
- [x] `merge_selections`
- [x] `push_selection_into_scan`
- [x] `push_selection_through_join`（inner、片側単一関係）
- [x] `split_selection_over_join`（inner）
- [x] `merge_projections`
- [x] `push_selection_through_projection`
- [x] `push_limit_through_projection`
- [x] `push_selection_through_aggregation`（grouping key のみ）
- [x] `infer_join_predicates`（等式 + 定数の推移、inner）
- [x] `merge_limits`
- [x] `eliminate_true_selection`
- [x] 物理: `full_scan` / `index_scan`（covering なら IndexOnly） /
      `selection` / `projection` / `aggregation` / `limit` /
      `hash_join`（in-memory + hybrid） / `index_join` / `nested_loop_join`
- [x] スカラー: 定数畳み込み、比較正規化、De Morgan、二重否定、吸収、
      冪等、算術単位元、定数再結合、IN 重複削除、CASE 平坦化、
      wildcard 無し LIKE→等価、CAST 入れ子畳み、OR 共通 AND 括り出し など

---

## P1 — 述語移動と単純化（TPC-H / 日常 SQL に効く）

- [x] `eliminate_false_selection` → Empty（矛盾 `WHERE false`。専用ノードが無い
      ため `LIMIT 0` として実装、スキーマは保持）
- [x] `eliminate_identity_projection`（入出力スキーマ一致）
- [ ] `prune_unused_projection_columns`（列刈り込み）
- [x] `push_projection_through_join`（ProjectJoinTranspose）（`cascades.cpp` に `push_projection_through_join` ルール追加済み）
- [ ] `push_projection_through_union`
- [ ] `push_projection_into_scan`（scan_projections を真の書換えに）
- [ ] `merge_adjacent_filters` の残余 Selection 削除
- [ ] `push_filter_past_setop`（UNION ALL 両枝へ）
- [ ] `push_filter_into_union_distinct`（DISTINCT 後も安全な場合）
- [ ] `simplify_filter_with_fd`（関数従属で冗長述語削除）
- [ ] `redundant_filter_removal`（強い述語が弱い述語を包含）
- [x] `range_predicate_merge`（`x>1 AND x>5` → `x>5`。式 rewrite として実装。
      矛盾方向は NULL 三値論理を守るため保持）
- [ ] `sargable_rewrite`（`x+1=5` → `x=4`、照合可能な形）
- [ ] `between_expansion` / `between_collapse`
- [ ] `or_to_in` / `in_to_or`（選択性に応じて両方向）
- [ ] `in_list_to_semijoin`（大きな IN リスト）
- [ ] `extract_common_or_predicates`（DNF の共通因子、既存 factor の関係版）
- [ ] `predicate_push_into_case`
- [ ] `null_rejecting_is_not_null_insert`（inner join キー）
- [ ] `not_null_inferred_from_inner_join`
- [x] `contradiction_from_null_eq`（`NULL = x` → `x IS NULL`、`NULL != x` → `x IS NOT NULL`）（`expression/rewrite.cpp` に `canonicalize_null_eq` / `canonicalize_null_ne` ルール追加済み）
- [ ] `canonicalize_boolean`（`x=true` → `x`、三値論理に注意）
- [ ] `simplify_coalesce_in_filter`
- [ ] `like_prefix_to_range`（`LIKE 'abc%'` → `>= 'abc' AND < 'abd'`）
- [ ] `like_suffix_not_sargable` の明示（誤って range にしない）
- [ ] `regexp_prefix_extraction`
- [ ] `cast_pushdown_on_comparison`（型を列側に寄せる）
- [x] `redundant_cast_removal`（同一数値型への CAST 除去。文字列型は長さ意味論
      のため保守的に対象外）
- [ ] `filter_merge_with_scan_zonemap` ヒント（実行器の zone map と接続）
- [ ] `push_limit_into_scan`（unordered LIMIT の early stop ヒント）
- [ ] `push_limit_through_union_all`
- [ ] `push_limit_through_inner_join_if_unique`（1:1 のとき）
- [x] `offset_zero_elimination`（LimitPlan が offset=0 で既に no-op）
- [x] `limit_zero_to_empty`（LimitPlan が limit_count=0 で既に 0 行出力）
- [x] `merge_sort_limit_to_topn`（`TopNPlan` クラスで実装済み、`limit_hint` コスティング接続済み）
- [x] `eliminate_sort_under_unordered_consumer`（`order_by_constant_removal` として定数キー除去、`topn_push_through_projection` として投影 Through を実装済み）
- [x] `prefix_sort_elimination`（既に順序を満たす）`SearchEngine::RequiredChildProperties` でプロパティ伝播済み
- [ ] `redundant_distinct_under_unique_key`
- [ ] `distinct_to_group_by`
- [ ] `group_by_to_distinct`（agg 無し）
- [ ] `count_star_without_group_rewrite`
- [ ] `push_filter_through_distinct`（DISTINCT 列のみ）
- [ ] `pull_filter_above_join`（選択性が極端に悪い場合の探索用、任意）

---

## P1 — 結合（inner の残り + 外側結合の入口）

- [ ] `join_left_assoc` / `join_right_assoc` の n-ary bushy 完全化
      （今の left/right 回転の穴埋め）
- [ ] `join_identity`（空 Values や 1 行定数表）
- [ ] `join_with_true_to_cross`
- [ ] `cross_join_elimination`（1 行側）
- [ ] `join_to_cross_if_no_predicate`
- [ ] `inner_join_to_filter_cross` の逆（条件付き cross → inner）
- [ ] `swap_inner_join_children_cost`（既に可換。コスト駆動の再試行）
- [ ] `dphyp` / `dpccp` 連結部分グラフ列挙（`join_enumeration` の置換）
- [ ] `greedy_join_order` フォールバック（関係 >16）
- [ ] `ikkbz` / `goo` ヒューリスティック
- [ ] `star_join_reorder`（ファクトを中央に）
- [ ] `bushy_vs_left_deep` の探索予算
- [ ] `join_predicate_transitivity` の一般化（定数以外、等価クラス全体）
- [ ] `inferred_inequality`（`a=b AND a<10` → `b<10`）
- [ ] `add_redundant_join_predicate`（ハッシュ分散用）
- [ ] `remove_redundant_join`（unique キー上の FK inner join 除去）
- [ ] `join_elimination_unique_key`（PK-FK、SELECT が親だけ）
- [ ] `self_join_elimination`
- [ ] `outer_to_inner`（WHERE が null-rejecting）
- [ ] `outer_to_anti`（`WHERE right.key IS NULL`）
- [ ] `full_outer_to_left_plus_anti`
- [ ] `right_join_to_left_join`（子の交換）
- [ ] `left_join_commutativity`（禁止、テストで固定）
- [ ] `push_filter_through_left_join_left_side`（常に可）（`kOuterJoin` は enum に追加済みだが join_kind ペイロード未追加のため未接続）
- [ ] `push_filter_through_left_join_right_side`（null-rejecting のみ）
- [ ] `push_filter_above_left_join`（遅延評価が得な場合）
- [ ] `split_filter_over_outer_join`
- [ ] `predicate_move_around_outer`
- [ ] `null_aware_anti_join`（`NOT IN` の三値論理）
- [ ] `not_in_to_anti_join`（両キー NOT NULL のとき。JoinKind は実行器に既にある）
- [ ] `in_to_semijoin`
- [ ] `exists_to_semijoin`
- [ ] `not_exists_to_antijoin`
- [ ] `unique_semijoin_to_inner`
- [ ] `semijoin_to_inner_plus_distinct`
- [ ] `semijoin_reduction`（bloom / ハッシュ半結合の先行適用）
- [ ] `join_on_false_to_empty` / `left_join_on_false_to_left_nullpad`
- [ ] `decorate_join_with_cardinality`（ヒストグラム結合）
- [ ] `detect_one_to_one` / `one_to_many` / `many_to_many`
- [ ] `prefer_index_join_when_inner_unique`
- [ ] `prefer_hash_when_equality_and_unsorted`
- [ ] `prefer_merge_when_both_sorted` → [`executor_todo.md`](executor_todo.md) MergeJoin
- [ ] `nested_loop_for_non_equality`
- [ ] `block_nested_loop` 実装規則
- [ ] `lookup_join` / `index_nested_loop` の一般化（複合キー、範囲）
- [ ] `batch_nested_loop`（IN リスト化して内側を一括）
- [ ] `dynamic_filter_join`（実行時ブルームを内側スキャンへ）
- [ ] `late_materialization_join`（行 ID で join してから列を取る）

---

## P2 — 集約・GROUP BY・DISTINCT

- [ ] `aggregate_project_merge`
- [ ] `aggregate_filter_transpose` の残余 HAVING 整理（部分実装あり）
- [ ] `aggregate_join_transpose`（agg を join の下へ、unique / キー保存時）
- [ ] `aggregate_union_transpose`
- [ ] `split_aggregate`（partial / final、並列と分散）
- [ ] `eager_aggregation`（join 前に group）
- [ ] `lazy_aggregation`
- [ ] `remove_aggregate_if_unique`（group キーが unique）
- [ ] `count_star_rewrite_on_not_null`
- [ ] `sum_zero_identity` 等の agg 代数
- [ ] `minmax_index_only`（INDEX MIN/MAX スキャン）
- [ ] `distinct_aggregate_expansion`（`COUNT(DISTINCT)` を二重 agg）
- [ ] `grouping_sets_expansion` と `grouping_sets_to_union`
- [ ] `rollup_to_grouping_sets` / `cube_to_grouping_sets`
- [ ] `grouping_id_simplification`
- [ ] `having_to_filter_after_agg` の明示ノード
- [ ] `filter_aggregate_argument`（FILTER 句）
- [ ] `ordered_set_aggregate`（PERCENTILE_CONT 等）
- [ ] `approx_aggregate_rewrite`（HLL 等、意味論が許すとき）
- [ ] `two_phase_hash_agg` vs `sort_agg` 実装規則 → SortAgg 実行器
- [ ] `stream_aggregate_if_sorted`
- [ ] `distinct_via_hash` vs `distinct_via_sort`
- [ ] `group_by_constant_removal`
- [ ] `group_by_functional_dependency_reduction`
- [ ] `aggregate_push_through_projection`
- [x] `scalar_agg_no_group_empty_input`（0 行で COUNT=0）`AggregationExecutor` で既に処理済み
- [ ] `any_value_elision`
- [ ] `bitwise_agg_rewrite`

---

## P2 — ソート・Top-N・物理順序

- [x] 論理 `Sort` と `enforces_order` プロパティの一本化（`LogicalOperator::kSort` + `RequiredChildProperties` + `PhysicalProperties` の ordering プロパティ伝播で一本化済み）
- [x] `sort_elimination`（入力が既に順序付き）`SearchEngine::OptimizeGroup` で IsOrderedBy チェック済み
- [ ] `sort_merge_of_compatible_orders`
- [ ] `partial_sort`（既に prefix がソート済み）
- [x] `topn_push_through_projection`（`cascades.cpp` に `topn_push_through_projection` ルール追加済み）
- [ ] `topn_push_through_inner_join`（unique 外側）
- [ ] `topn_into_index_scan`（ORDER BY がインデックス先頭）
- [x] `limit_plus_sort_to_heap_topn` → HeapTopN 実行器（`plan/topn_plan.hpp` + `plan/topn_plan.cpp` + `relational_factory.cpp` で実装済み）
- [ ] `offset_fetch_rewrite`
- [x] `order_by_constant_removal`（`cascades.cpp` に `order_by_constant_removal` ルール追加済み）
- [x] `order_by_redundant_column_removal`（FD）（`order_by_constant_removal` として定数キー除去を実装済み）
- [ ] `nulls_first_last_normalization`
- [ ] `collation_aware_sort`
- [ ] `incremental_sort`（PostgreSQL Incremental Sort）
- [ ] `buffered_sort` vs `external_merge` 実装規則（Sort 実行器は既にある）

---

## P3 — サブクエリ・相関・CTE

- [ ] `unnest_scalar_subquery`（非相関）
- [ ] `unnest_in_subquery`
- [ ] `unnest_exists`
- [ ] `decorrelate_apply`（PullUpCorrelatedPredicates）
- [ ] `decorrelate_lateral`
- [ ] `subquery_to_semijoin` / `antijoin` / `left_join` + IS NOT NULL
- [ ] `mark_join_to_filter`（`x IN (SELECT…)` の三値）
- [ ] `single_join_max1row_assert`
- [ ] `correlated_filter_pullup`
- [ ] `push_correlated_predicate_into_subquery`
- [ ] `flatten_nested_subqueries`
- [ ] `merge_identical_subqueries`（CSE）
- [ ] `cache_invariant_subquery`（一度だけ評価）
- [ ] `cte_inlining`（参照 1 回、または安価）
- [ ] `cte_materialization`（参照複数、または再帰）
- [ ] `cte_filter_pushdown`
- [ ] `cte_predicate_propagation`
- [ ] `recursive_cte_union_rewrite`
- [ ] `worktable_scan_indexing`
- [ ] `exists_short_circuit_limit_1`
- [ ] `any_all_quantified_comparison_rewrite`
- [ ] `subquery_unnest_with_window`（禁止条件の明示）
- [ ] `common_subexpression_materialize`（同一 Scan/Join 部分木）

---

## P3 — 集合演算

- [ ] `union_all_merge`（連続 UNION ALL を n-ary）
- [ ] `union_to_union_all_plus_distinct`
- [ ] `intersect_to_semijoin`
- [ ] `except_to_antijoin`
- [ ] `setop_push_projection`
- [ ] `setop_push_filter`
- [ ] `union_all_push_limit`
- [ ] `union_distinct_hash_vs_sort`
- [ ] `cancel_union_empty` / `union_with_empty`
- [ ] `intersect_with_empty` → empty
- [ ] `except_empty_right` → left
- [ ] `setop_type_coercion_pushdown`
- [ ] `merge_union_compatible_scans`（同一表の OR を 1 スキャンに）
- [ ] `partition_wise_union`

---

## P4 — Window / UNNEST / VALUES / 再帰

- [ ] `split_window`（互換フレームごとに分割）
- [ ] `merge_compatible_windows`
- [ ] `push_filter_through_window`（partition キー / 非 window 列）
- [ ] `push_limit_through_window`（ほぼ不可。例外を列挙）
- [ ] `window_to_aggregate`（フレームが全体のとき）
- [ ] `rank_filter_to_topn`（`RANK() = 1` 等）
- [ ] `row_number_filter_to_topn`
- [ ] `eliminate_noop_window`
- [ ] `window_prefix_sort_share`
- [ ] `unnest_with_ordinality`
- [ ] `unnest_filter_pushdown`
- [ ] `unnest_to_join_with_values`
- [ ] `array_flatten_rewrite`
- [ ] `values_fold_into_union`
- [ ] `constant_table_scan`
- [ ] `generate_series_to_values`（小さい n）
- [ ] `table_sample_bernoulli_vs_system`
- [ ] `recursive_termination_predicate_push`

---

## P4 — 射影・式・CSE

- [x] `project_remove`（Calcite ProjectRemove）`eliminate_identity_projection` として実装
- [ ] `project_to_scan`（列部分集合 + テーブル）
- [ ] `merge_calc`（Filter+Project を Calc に統合するか、逆に分解するか方針決定）
- [ ] `common_subexpression_elimination` in target list
- [ ] `duplicate_column_elimination`
- [ ] `unused_expression_pruning`
- [ ] `constant_propagation_through_project`
- [ ] `predicate_push_into_project_expr`
- [ ] `simplify_case_in_project`
- [ ] `boolean_project_used_as_filter` の引き上げ
- [ ] `widen_project_for_join`（join 後に落とす列を一時保持）
- [ ] `narrow_project_after_join`

---

## P5 — スカラー rewrite の追加（ExpressionRuleSet）

既存の畳み込みに加え、計画品質に効くもの。

- [ ] `cast_simplify_numeric_widening`
- [ ] `cast_date_timestamp_normalize`
- [x] `coalesce_flatten` / `coalesce_of_coalesce`（ネスト COALESCE を平坦化）
- [x] `nullif_to_case`（`expression/rewrite.cpp` に `nullif_to_case` ルール追加済み）
- [ ] `if_to_case` / `case_to_if`
- [x] `greatest_least_fold`（`expression/rewrite.cpp` に `greatest_least_fold` ルール追加済み）
- [ ] `between_symmetric`
- [ ] `is_distinct_from_rewrite`
- [ ] `boolean_eq_true_false_three_valued`
- [ ] `and_true_elim` / `or_false_elim` の NULL 厳密化監査
- [ ] `distribute_or_over_and`（選択的、爆発抑制）
- [ ] `distribute_and_over_or`
- [ ] `cnf_conversion_budgeted`
- [ ] `dnf_conversion_budgeted`
- [ ] `extract_disjunctive_sarg`
- [x] `in_empty_list` → false / unknown（空リストは NULL 子でも FALSE）
- [x] `in_single_null`（`expression/rewrite.cpp` に `in_single_null` ルール追加済み）
- [ ] `not_in_with_null_list` 警告と計画
- [ ] `like_escape_normalize`
- [ ] `ilike_to_lower_like`
- [ ] `similiar_to_to_regex`
- [ ] `arithmetic_overflow_safe_fold`
- [ ] `date_add_sub_fold`
- [ ] `interval_normalize`
- [ ] `concat_flatten`
- [ ] `substring_constant_fold` 強化
- [ ] `json_path_constant_fold`
- [ ] `array_constructor_fold`
- [ ] `array_length_zero`
- [ ] `safe_divide_rewrite`
- [x] `abs_of_abs`
- [ ] `log_identities`
- [x] `comparison_of_same_expr`（`x=x` → `x IS NOT NULL`）
- [x] `self_inequality`（`x<x` → false / unknown）（`expression/rewrite.cpp` に `self_inequality` ルール追加済み）
- [ ] `deterministic_function_cse`
- [ ] `nondeterministic_barrier`（RANDOM / NOW を畳まない）
- [ ] `stable_vs_immutable` 分類
- [ ] `rewrite_or_of_ranges_to_in`
- [ ] `extract_year_sargable`（可能なら range）
- [ ] `not_between_to_or`
- [ ] `xor_to_or_and_not`
- [ ] `bit_and_or_identities`

---

## P5 — 統計・選択性・コスト（規則そのものではないが必須）

- [ ] 列ヒストグラム（等幅 / 等高）
- [ ] MCV（most common values）
- [ ] NDV スケッチ（HyperLogLog）
- [ ] NULL 比率の独立管理
- [ ] 多列相関統計 / 関数従属
- [ ] join クロス列 NDV
- [ ] 式統計（`f(col)` の NDV）
- [ ] LIKE / 正規表現の選択性モデル
- [ ] OR の包含・除外
- [ ] 範囲述語の区間演算
- [ ] 結合カーディナリティの histogram join
- [ ] 上界: `min(|L|,|R|)` for semi、FK 上界
- [ ] skew 補正（Zipf）
- [ ] 動的サンプリング（実行前サンプルスキャン）
- [ ] フィードバック最適化（前回実行の実カーディナリティ）
- [ ] コスト単位の監査（I/O vs CPU vs メモリ）
- [ ] メモリ許可量と spill 確率
- [ ] parallel speedup モデル
- [ ] index クラスタリング因子
- [ ] ページキャッシュヒット率
- [ ] PAX / zone map 選択性
- [ ] Top-N の `limit_hint` を全実装規則へ
- [ ] `EXPLAIN` に見積 vs 実測を出す（ANALYZE は一部既存）

---

## P6 — 探索・メモ・実装規則の制御

- [ ] ブランチ・アンド・バウンドのコスト上界
- [ ] 興味深い順序（interesting orders）の完全伝播
- [ ] 興味深い分割（interesting partitioning）
- [ ] 物理規則 `sort` / `merge_join` / `sort_agg` / `hash_agg` の分離
- [ ] `stream_agg` 実装規則
- [ ] `hash_distinct` / `sort_distinct`
- [ ] `window_sort` / `window_hash_partition`
- [ ] `set_union_hash` / `set_union_sort`
- [ ] `materialize` 実装規則
- [ ] `spool` 実装規則
- [ ] `exchange` 実装規則（単一ノードでは no-op）
- [ ] 規則の優先度 / 重み
- [ ] 探索タイムアウトと best-so-far
- [ ] クエリヒント（join order freeze、pg_hint_plan 相当）
- [ ] ルールセットのワークロード別プロファイル
- [ ] `join_enumeration` を連結部分グラフ列挙に置換
- [ ] グループ表現キャップの適応
- [ ] memo dump の diff テスト拡充（optimizer_improvements Phase 9）
- [ ] 計画キャッシュとパラメータスニッフィング対策
- [ ] 汎用計画 vs カスタム計画
- [ ] 並列度決定（DOP）
- [ ] バッチサイズ / モーセルサイズのコスト化

---

## P6 — 物理実装規則（実行器と対）

各項目の実行器は [`executor_todo.md`](executor_todo.md)。

- [ ] `merge_join`（等価、両側ソート済み）
- [ ] `sort_merge_join`（必要なら子に Sort を要求）
- [ ] `outer_hash_join` / `left` / `right` / `full`
- [ ] `outer_merge_join`
- [ ] `outer_nested_loop`
- [x] `semi_hash_join` / `anti_hash_join`（JoinKind は実行器に存在、規則未接続）`DefaultImplementationRules` で接続済み
- [ ] `semi_merge_join` / `anti_merge_join`
- [ ] `index_only_agg`（MIN/MAX）
- [ ] `bitmap_scan` / `bitmap_and` / `bitmap_or`
- [ ] `tid_scan` / `rowid_lookup`
- [ ] `parallel_seq_scan` を Cascades から明示選択
- [ ] `pax_scan` / `zone_map_skip`
- [ ] `skip_scan`（複合インデックスの先頭欠落）
- [ ] `index_skip_scan_for_distinct`
- [ ] `covering_index_rewrite`
- [x] `sort_agg` / `hash_agg` の選択（`aggregation` 実装規則で `AggregationPlan` を生成）
- [x] `parallel_hash_agg` / `parallel_stream_agg`（`ParallelAggregationExecutor` が `AggregationPlan::EmitExecutor` で接続済み）
- [x] `topn_heap`（`TopNPlan` クラス実装済み）
- [ ] `window_agg`
- [ ] `setop_hash` / `setop_sort`
- [ ] `unnest_exec`
- [ ] `recursive_union`
- [x] `insert` / `update` / `delete` / `upsert` の DML 計画（`RelationalPlan` + `Insert/Update/DeletePlan` で実装済み。`upsert` は未実装）
- [ ] `on_conflict` / `returning`
- [ ] `lock_rows`（SELECT FOR UPDATE）計画

---

## P7 — 整合性制約・カタログ駆動

- [ ] PK / UNIQUE による distinct 除去
- [ ] NOT NULL による `IS NOT NULL` 除去
- [ ] CHECK 制約の述語取り込み
- [ ] FK による join 除去とカーディナリティ上界
- [ ] パーティション制約による枝刈り（constraint exclusion）
- [ ] 生成列の照合
- [ ] 部分インデックス照合
- [ ] 式インデックス照合
- [ ] 照合順序（collation）伝播
- [ ] ビュー展開（predicate を残したまま）
- [ ] セキュリティバリアビュー（pushdown 制限）
- [ ] RLS 述語の挿入位置

---

## P7 — 特殊 SQL 形

- [ ] `LATERAL` のコスト付き展開
- [ ] `UNNEST` + JOIN の順序
- [ ] `PIVOT` / `UNPIVOT` 展開
- [ ] `MATCH_RECOGNIZE`（将来）
- [ ] `TABLESAMPLE`
- [ ] `QUALIFY`（window filter）
- [ ] `GROUP BY ALL` / `GROUP BY DISTINCT`
- [ ] `SELECT DISTINCT ON`（PostgreSQL）
- [ ] `FETCH FIRST … WITH TIES`
- [ ] `FOR UPDATE` / `SKIP LOCKED` とアクセスパス制約
- [ ] `INSERT SELECT` の投影/並列
- [ ] `MERGE` 文の join 計画
- [ ] `UPDATE … FROM` / `DELETE … USING`
- [ ] `RETURNING` の投影
- [ ] 準備文の generic/custom
- [ ] バッチ INSERT の値リスト結合

---

## P8 — 分散・並列（単一ノードでもプロパティだけ先に）

- [ ] 分配: any / singleton / hash / broadcast / range
- [ ] `enforce_distribution` 実装規則
- [ ] colocated join 検出
- [ ] broadcast vs shuffle のコスト
- [ ] partial agg + finalize
- [ ] two-phase distinct
- [ ] ウィンドウの partition 再分散
- [ ] リミットの部分リミット + merge
- [ ] スキャンのレンジ分割
- [ ] パイプライン並列 vs 交換並列

---

## P8 — 検証・安全網

- [ ] 規則ごとの代数テスト（前後の結果集合）
- [ ] fuzz: ランダム規則サブセット（物理は既存、論理も）
- [ ] 外側結合の null-pad ゴールデン
- [ ] TPC-H Q8/Q9 の join order ゴールデン（Phase 9 残り）
- [ ] 差分: 関係エンジン経路 vs Cascades 経路
- [ ] コスト単調性の近似チェック
- [ ] 無限ループ検出（既に fingerprint。規則追加時の回帰）
- [ ] EXPLAIN に適用された規則名を出す

---

## 推奨実装順（最初のスライス）

1. 等価クラスの一般化（`infer_join_predicates` 拡張）と矛盾 Filter。
2. 列刈り込み / identity projection。
3. `Sort` 論理ノード + Top-N + インデックス順序の接続。
4. Semi/Anti をメモに載せ、既存 `JoinKind` 実行器へ実装規則を繋ぐ。
5. LEFT JOIN の論理ノード + null-rejecting pushdown。
6. Merge Join 物理（ソート興味順序とセット）。
7. Sort-Agg vs Hash-Agg の選択。
8. サブクエリ decorrelation。
9. UNION / Window。
10. 統計ヒストグラム。
