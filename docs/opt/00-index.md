# 目次 & 進捗トラッカー — tinylamb Cascades オプティマイザ解説

- 執筆方針は [AGENTS.md](AGENTS.md)。各ターン開始時に本表を確認し、
  終了時に更新する。
- 状態: **未執筆 / draft / review / done**
- 基準リビジョン: `3880673` (2026-09-12)。**引用の検証は作業ツリー
  (HEAD `3880673` + 未コミット差分を含む)に対して行われている。**
- 第2部のファイルパスは `part2-rules/<種別>/<rule-name>.md`(表では種別列で省略)。
- 分類(A〜K など)は暫定。執筆時に前後してもよい(AGENTS.md §11)。

進捗サマリ: 全 216 ファイル（基礎編 7/7, 第2部 207/207, 管理文書 2/2）の推敲・文体統一完了（done）。
全ファイルの鹿野氏技術文章規範（SKILL.md）および見出し構成テンプレート（AGENTS.md）への適合、コード引用・三値論理・例外保護の意味論検証を完了。

## 第1部 基礎編(`part1-foundations/`)

| # | 章 | 状態 |
|---|---|---|
| 10 | Cascades とは / tinylamb の全体フロー | draft |
| 20 | Memo / Group / LogicalExpression | draft |
| 30 | Pattern DSL と Bindings(Rule の書き方) | draft |
| 40 | SearchEngine / コスト / 枝刈り / 縮退 | draft |
| 50 | PhysicalProperties と enforcement | draft |
| 60 | Rule 追加・無効化・事前条件ゲート(D5) | draft |
| 70 | 式書き換え: フレームワーク + 自明な Rule の一括解説 | draft |

70 章で一括解説する式書き換え Rule(35 本、1 本も飛ばさない):
`fold_binary` / `fold_unary` / `fold_in` / `fold_function` / `singleton_in` /
`canonicalize_comparison` / `boolean_identity` / `double_negation` /
`simplify_case` / `and_idempotent` / `or_idempotent` / `identity_add_zero` /
`identity_subtract_zero` / `identity_multiply_one` / `identity_divide_one` /
`canonicalize_add_negative_constant` / `canonicalize_subtract_negative_constant` /
`multiply_by_negative_one` / `combine_repeated_addend` /
`double_negation_arithmetic` / `double_bitwise_negation` /
`reassociate_add_constants` / `reassociate_subtract_constants` /
`reassociate_subtract_add_constants` / `reassociate_add_subtract_constants` /
`dedupe_in_list` / `uniform_case_result` / `empty_in_list` / `abs_of_abs` /
`if_to_case` / `concat_flatten` / `array_flatten_optimization` /
`canonicalize_boolean` / `pow_identities` / `greatest_least_fold`

## 第2部 Rule リファレンス(`part2-rules/`)

### 論理 Rule(117 本)

| Rule 名 | 分類 | 状態 |
|---|---|---|
| `join_commutativity` | A 結合順序 | draft |
| `join_enumeration` | A 結合順序 | draft |
| `join_associativity_left` | A 結合順序 | draft |
| `join_associativity_right` | A 結合順序 | draft |
| `join_to_cross_if_no_predicate` | A 結合順序 | draft |
| `cross_to_inner_with_predicate` | A 結合順序 | draft |
| `outer_join_associativity` | A 結合順序 | draft |
| `semi_join_commutativity` | A 結合順序 | draft |
| `semi_join_inner_join_reorder` | A 結合順序 | draft |
| `greedy_join_order_fallback` | A 結合順序 | draft |
| `star_join_reorder` | A 結合順序 | draft |
| `merge_selections` | B フィルタ | draft |
| `merge_adjacent_filters` | B フィルタ | draft |
| `push_selection_into_scan` | B フィルタ | draft |
| `push_selection_through_join` | B フィルタ | draft |
| `split_selection_over_join` | B フィルタ | draft |
| `push_filter_through_distinct` | B フィルタ | draft |
| `push_filter_through_sort` | B フィルタ | draft |
| `push_filter_through_left_join_left_side` | B フィルタ | draft |
| `push_selection_through_projection` | B フィルタ | draft |
| `push_selection_through_aggregation` | B フィルタ | draft |
| `infer_join_predicates` | B フィルタ | draft |
| `infer_filter_from_equivalence_class` | B フィルタ | draft |
| `join_predicate_transitivity` | B フィルタ | draft |
| `inferred_inequality_pushdown` | B フィルタ | draft |
| `dynamic_filter_pushdown_join` | B フィルタ | draft |
| `having_to_filter_rewrite` | B フィルタ | draft |
| `filter_pull_up_for_extreme_selectivity` | B フィルタ | draft |
| `functional_dependency_filter_reduction` | B フィルタ | draft |
| `scan_zone_map_filter_integration` | B フィルタ | draft |
| `check_constraint_predicate_intake` | B フィルタ | draft |
| `unnest_filter_pushdown` | B フィルタ | draft |
| `recursive_termination_predicate_pushdown` | B フィルタ | draft |
| `cast_pushdown_on_comparison` | B フィルタ | draft |
| `extract_year_sargable` | B フィルタ | draft |
| `comparison_self_predicates` | B フィルタ | draft |
| `eliminate_false_selection` | C 削除・簡約 | draft |
| `eliminate_true_selection` | C 削除・簡約 | draft |
| `join_on_false_to_empty` | C 削除・簡約 | draft |
| `join_empty_simplification` | C 削除・簡約 | draft |
| `setop_empty_simplification` | C 削除・簡約 | draft |
| `setop_empty_identity` | C 削除・簡約 | draft |
| `eliminate_identity_projection` | C 削除・簡約 | draft |
| `eliminate_double_sort` | C 削除・簡約 | draft |
| `eliminate_sort_under_unordered_consumer` | C 削除・簡約 | draft |
| `no_op_window_elimination` | C 削除・簡約 | draft |
| `one_row_cross_join_elimination` | C 削除・簡約 | draft |
| `self_join_elimination` | C 削除・簡約 | draft |
| `unused_join_elimination` | C 削除・簡約 | draft |
| `fk_join_elimination` | C 削除・簡約 | draft |
| `foreign_key_outer_join_elimination` | C 削除・簡約 | draft |
| `redundant_join_predicate_elimination` | C 削除・簡約 | draft |
| `pk_unique_distinct_elimination` | C 削除・簡約 | draft |
| `not_null_is_not_null_elimination` | C 削除・簡約 | draft |
| `any_value_elimination` | C 削除・簡約 | draft |
| `join_identity_dummy` | C 削除・簡約 | draft |
| `merge_projections` | D Projection | draft |
| `merge_adjacent_projections` | D Projection | draft |
| `push_projection_through_join` | D Projection | draft |
| `push_projection_through_union` | D Projection | draft |
| `push_projection_through_aggregation` | D Projection | draft |
| `projection_cse_and_pruning` | D Projection | draft |
| `projection_constant_propagation` | D Projection | draft |
| `push_projection_below_join_width_control` | D Projection | draft |
| `aggregate_projection_merge` | D Projection | draft |
| `merge_limits` | E LIMIT/SORT | draft |
| `push_limit_through_projection` | E LIMIT/SORT | draft |
| `topn_push_through_projection` | E LIMIT/SORT | draft |
| `union_all_push_limit` | E LIMIT/SORT | draft |
| `push_limit_through_union_all` | E LIMIT/SORT | draft |
| `push_limit_through_left_join` | E LIMIT/SORT | draft |
| `limit_push_through_sort` | E LIMIT/SORT | draft |
| `sort_merge_of_compatible_orders` | E LIMIT/SORT | draft |
| `rank_row_number_to_topn` | E LIMIT/SORT | draft |
| `distinct_over_group_by` | F 集約 | draft |
| `distinct_over_distinct` | F 集約 | draft |
| `distinct_and_group_by_interchange` | F 集約 | draft |
| `count_star_without_group_rewrite` | F 集約 | draft |
| `count_star_rewrite_on_not_null` | F 集約 | draft |
| `group_by_functional_dependency_reduction` | F 集約 | draft |
| `unique_group_key_aggregate_elimination` | F 集約 | draft |
| `count_distinct_expansion` | F 集約 | draft |
| `grouping_sets_expansion` | F 集約 | draft |
| `eager_aggregation_over_join` | F 集約 | draft |
| `aggregate_join_transpose` | F 集約 | draft |
| `aggregate_union_transpose` | F 集約 | draft |
| `push_aggregation_through_union_all` | F 集約 | draft |
| `filter_aggregate_pushdown` | F 集約 | draft |
| `split_window` | G ウィンドウ | draft |
| `merge_adjacent_windows` | G ウィンドウ | draft |
| `push_selection_through_window` | G ウィンドウ | draft |
| `window_frame_sort_sharing` | G ウィンドウ | draft |
| `window_after_filter_partition_pushdown` | G ウィンドウ | draft |
| `unique_semi_to_inner` | H 結合種別 | draft |
| `outer_to_anti_join` | H 結合種別 | draft |
| `right_to_left_outer_join` | H 結合種別 | draft |
| `full_outer_join_decomposition` | H 結合種別 | draft |
| `outer_to_inner_join_on_null_rejecting_filter` | H 結合種別 | draft |
| `semijoin_to_inner_plus_distinct` | H 結合種別 | draft |
| `mark_join_to_filter` | H 結合種別 | draft |
| `in_list_to_semi_join` | H 結合種別 | draft |
| `intersect_to_semijoin` | H 結合種別 | draft |
| `except_to_antijoin` | H 結合種別 | draft |
| `push_semi_join_through_inner_join` | H 結合種別 | draft |
| `apply_to_join` | I 相関サブクエリ | draft |
| `hoist_correlated_selection_to_apply` | I 相関サブクエリ | draft |
| `push_selection_through_apply` | I 相関サブクエリ | draft |
| `push_apply_through_join` | I 相関サブクエリ | draft |
| `decorrelate_aggregate_apply` | I 相関サブクエリ | draft |
| `push_filter_past_setop` | J 集合演算 | draft |
| `union_to_union_all_plus_distinct` | J 集合演算 | draft |
| `union_all_merge` | J 集合演算 | draft |
| `union_distinct_hash_sort_choice` | J 集合演算 | draft |
| `intersect_except_cost_based_lowering` | J 集合演算 | draft |
| `values_fold_into_union` | J 集合演算 | draft |
| `push_not_through_expression` | K その他 | draft |
| `order_by_redundant_column_removal` | K その他 | draft |

### 実装 Rule(52 本)

| Rule 名 | 分類 | 状態 |
|---|---|---|
| `full_scan` | スキャン | draft |
| `index_scan` | スキャン | draft |
| `empty` | 供給 | draft |
| `dummy_scan` | 供給 | draft |
| `constant_table` | 供給 | draft |
| `values` | 供給 | draft |
| `generate_series` | 供給 | draft |
| `relational_ir` | 供給 | draft |
| `selection` | 選択・射影 | draft |
| `projection` | 選択・射影 | draft |
| `nested_loop_join` | 結合 | draft |
| `hash_join` | 結合 | draft |
| `single_hash_join` | 結合 | draft |
| `merge_join` | 結合 | draft |
| `index_join` | 結合 | draft |
| `cross_join` | 結合 | draft |
| `outer_nested_loop` | 結合 | draft |
| `outer_hash_join` | 結合 | draft |
| `semi_hash_join` | 結合 | draft |
| `semi_merge_join` | 結合 | draft |
| `anti_hash_join` | 結合 | draft |
| `anti_merge_join` | 結合 | draft |
| `mark_hash_join` | 結合 | draft |
| `batch_nested_loop` | 結合 | draft |
| `batch_nested_loop_semi` | 結合 | draft |
| `batch_nested_loop_anti` | 結合 | draft |
| `batch_nested_loop_outer` | 結合 | draft |
| `aggregation` | 集約・DISTINCT | draft |
| `distinct` | 集約・DISTINCT | draft |
| `sort_distinct` | 集約・DISTINCT | draft |
| `skip_scan_distinct` | 集約・DISTINCT | draft |
| `sort` | ソート・行制限 | draft |
| `topn` | ソート・行制限 | draft |
| `limit` | ソート・行制限 | draft |
| `max1_row` | ソート・行制限 | draft |
| `union` | 集合演算 | draft |
| `union_all` | 集合演算 | draft |
| `intersect` | 集合演算 | draft |
| `intersect_all` | 集合演算 | draft |
| `except` | 集合演算 | draft |
| `except_all` | 集合演算 | draft |
| `unnest` | その他 | draft |
| `apply` | その他 | draft |
| `recursive_cte` | その他 | draft |
| `materialize` | その他 | draft |
| `eager_spool` | その他 | draft |
| `lazy_spool` | その他 | draft |
| `window` | その他 | draft |
| `exchange_noop` | 分散 no-op | draft |
| `gather_noop` | 分散 no-op | draft |
| `broadcast_noop` | 分散 no-op | draft |
| `redistribute_noop` | 分散 no-op | draft |

### 式書き換え Rule — 個別章(38 本)

| Rule 名 | 状態 |
|---|---|
| `de_morgan` | draft |
| `not_comparison` | draft |
| `not_like` | draft |
| `not_not_like` | draft |
| `not_is_null` | draft |
| `not_is_not_null` | draft |
| `xor_boolean_identity` | draft |
| `xor_to_or_and_not` | draft |
| `absorption_and` | draft |
| `absorption_and_reversed` | draft |
| `absorption_or` | draft |
| `absorption_or_reversed` | draft |
| `like_equality` | draft |
| `not_like_equality` | draft |
| `is_null_of_null_check` | draft |
| `is_not_null_of_null_check` | draft |
| `collapse_nested_identical_cast` | draft |
| `factor_or_common_and` | draft |
| `nullif_to_case` | draft |
| `contradiction_from_null_eq` | draft |
| `in_single_null` | draft |
| `not_in_null_semantics` | draft |
| `boolean_eq_true_false_three_valued` | draft |
| `nondeterministic_barrier` | draft |
| `safe_divide_rewrite` | draft |
| `coalesce_and_nullif_simplification` | draft |
| `datetime_and_string_fold_extent` | draft |
| `json_path_constant_fold` | draft |
| `numeric_widening_cast` | draft |
| `or_of_ranges_to_in` | draft |
| `interval_normalize` | draft |
| `predicate_pushdown_case` | draft |
| `inner_join_not_null_inference` | draft |
| `regexp_prefix_extraction` | draft |
| `deterministic_function_cse` | draft |
| `function_volatility_classification` | draft |
| `boolean_filter_pullup` | draft |
| `distribute_or_over_and_budgeted` | draft |

### 執筆時に判明した実装の現状メモ(各章に詳述)

- `eliminate_identity_projection` はコード内で丸ごとコメントアウトされて
  無効化済み(理由: 射影の削除が出力スキーマの列名を変え、エイリアス参照を
  含む結合述語を壊す)。D5 監査表には未記載。
- 式書き換えの no-op(宣言のみで常に不発火)が 3 本:
  `nondeterministic_barrier` / `safe_divide_rewrite` /
  `function_volatility_classification`(実際の抑止は `fold_function` の
  volatility guard が担う)。
- `abs_of_abs` は同名で 2 回登録されており、実効登録数は 73 本
  (`built.Add` は 74 回、同名は置き換え)。
- guard がコメントの主張より緩い箇所(例: `eager_aggregation_over_join` の
  「結合キー ⊆ GROUP BY キー」の検査不在、`intersect_to_semijoin` /
  `except_to_antijoin` の多重度 guard 不在、
  `eliminate_sort_under_unordered_consumer` の順序依存性検査不在)は、
  それぞれの章に「現状こうなっている」と記録済み。
