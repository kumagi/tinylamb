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

`LogicalOperator` は Scan / Join / Selection / Projection /
Aggregation / Limit / Relational（不透明）に加えて、等価キーの
SemiJoin / AntiJoin を持つ。商用相当にするには引き続き
**種類を増やす**。

- [x] `kOuterJoin`（LEFT / RIGHT / FULL）と join type ペイロード、outer hash
      実装規則（安全な pushdown 規則は未導入）
- [x] `kSemiJoin` / `kAntiJoin`（等価キーの Cascades 実装規則まで接続）
- [ ] `kMarkJoin`（`IN` の UNKNOWN marker）
- [x] `kSingleJoin`（スカラ相関の基数 assertion は `kMax1Row` / `Max1RowPlan`
      として実装）
- [x] `kCrossJoin` を inner と分離（無条件 join を明示化し、cross-product
      カーディナリティでコスト評価）
- [ ] `kApply` / `kLateralJoin`（相関 APPLY）
- [x] `kUnion` / `kUnionAll` / `kIntersect` / `kIntersectAll` / `kExcept` /
      `kExceptAll`（論理ノードと物理実装規則）
- [ ] `kWindow`（PARTITION / ORDER / frame）
- [x] `kSort` を一般 Cascades 論理ノードとして明示し、`SortPlan` の実装規則と
      出力スキーマ基準の ORDER BY キー正規化を接続
- [x] `kDistinct` / `kDuplicateElim`
- [x] `kTopN`（ORDER BY + LIMIT を 1 ノードに）
- [x] `kValues` / `kConstantTable`（`ValuesPlan` と Cascades 実装規則）
- [ ] `kUnnest` / `kGenerateSeries` / TVF
- [ ] `kExpand`（CUBE / ROLLUP / GROUPING SETS）
- [ ] `kRecursiveCte` / `kWorkTableScan`
- [ ] `kMaterialize` / `kEagerSpool` / `kLazySpool`
- [ ] `kExchange` / `kGather` / `kBroadcast` / `kRedistribute`（分散予約）
- [ ] `kSample` / `kTableSample`
- [ ] `kAssert`（一般的な cardinality assertion）
- [x] `kMax1Row`（スカラサブクエリ基数の専用論理・物理ノード）
- [x] `kFilterFalse` / `kEmpty`（定数 FALSE / NULL Selection から EmptyPlanへ
      論理・物理接続）
- [x] `kDummyScan`（`SELECT 1` の 1 行ソースを Relational から外し、Optimizer
      の no-FROM 経路へ接続）
- [x] Join ペイロードに outer join type（LEFT / RIGHT / FULL）と
      null-producing side（`LogicalExpression::join_type` → `JoinKind`）
- [ ] Aggregation ペイロードに grouping sets
- [x] Aggregation 式の DISTINCT agg / FILTER 相当（GoogleSQL `AGG(x WHERE p)`）/
      ORDER BY WITHIN GROUP メタデータを式・rewrite・plan cache で保持
- [ ] Projection ペイロードに computed vs passthrough 列の区別
- [ ] Window ペイロードに frame 型（ROWS / RANGE / GROUPS）と exclusion
- [x] Set-op ペイロードに ALL vs DISTINCT
- [x] Set-op ペイロードの列対応・共通型（`SetOperationPlan` が列幅を検証し、
      数値共通型を出力スキーマへ反映）
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
      `hash_join`（in-memory + hybrid） / `semi_hash_join` /
      `anti_hash_join` / `outer_hash_join`（LEFT / RIGHT / FULL） /
      `index_join` / `nested_loop_join`
- [x] スカラー: 定数畳み込み、比較正規化、De Morgan、二重否定、吸収、
      冪等、算術単位元、定数再結合、IN 重複削除、CASE 平坦化、
      wildcard 無し LIKE→等価、CAST 入れ子畳み、OR 共通 AND 括り出し など

---

## P1 — 述語移動と単純化（TPC-H / 日常 SQL に効く）

- [x] `eliminate_false_selection` → `EmptyPlan`（定数 FALSE / NULL の Selection）
- [x] `eliminate_identity_projection`（物理実装時に列定義が一致する場合）
- [x] `prune_unused_projection_columns`（述語・出力式の touched columns から
      scan projection を作り、不要な列を物理スキャン直後に除去）
- [x] `push_projection_through_join`（ProjectJoinTranspose; inner join の qualified 列だけを左右へ残し、outer/cross/未修飾列は保守的に対象外）
- [x] `push_projection_through_union`（UNION / UNION ALL の各枝へ分配）
- [x] `push_projection_into_scan`（`Optimizer` の touched-column 刈り込みを
      `scan_projections` と Full/IndexScan の投影へ接続し、TPC-H 回帰で確認）
- [x] `merge_adjacent_filters` の残余 Selection 削除（`merge_selections` が
      合成 predicate を同じ group に追加）
- [x] `push_filter_past_setop`（UNION ALL 両枝へ。全 set-op の枝へ
      predicate を分配）
- [x] `push_filter_into_union_distinct`（DISTINCT 後も安全な場合。set-op の
      重複意味を保ったまま枝へ分配）
- [ ] `simplify_filter_with_fd`（関数従属で冗長述語削除）
- [x] `redundant_filter_removal`（同一列の非 NULL 定数等式が包含する比較を
      三値論理を保ったまま除去）
- [x] `range_predicate_merge`（同一列・同方向の比較に限定し、`x>1 AND x>5`
      → `x>5`／上限側は最小値へ統合。NULL 定数と混在方向は保持）
- [x] `sargable_rewrite`（整数・浮動小数の列 +/− 定数と定数比較を、
      オーバーフロー時は保持したまま列境界へ移送）
- [x] `between_expansion`（GoogleSQL AST visitor が `BETWEEN` を inclusive
      lower/upper の AND に正規化。`between_collapse` は未導入）
- [x] `or_to_in` / `in_to_or`（安全な同一列・定数等式の `OR` → `IN` を実装。
      逆変換は未使用のため、選択性依存の展開は保留）
- [ ] `in_list_to_semijoin`（大きな IN リスト）
- [x] `extract_common_or_predicates`（式レベルの安全な共通 AND 因子抽出を
      `factor_or_common_and` として実装。関係レベルの DNF 展開は抑制）
- [ ] `predicate_push_into_case`
- [ ] `null_rejecting_is_not_null_insert`（inner join キー）
- [ ] `not_null_inferred_from_inner_join`
- [x] `contradiction_from_null_eq`（比較式の片側 NULL を UNKNOWN に定数化し、
      Selection の Empty 化へ接続）
- [ ] `canonicalize_boolean`（`x=true` → `x`、三値論理に注意）
- [x] `simplify_coalesce_in_filter`（先行する NULL リテラルだけを許し、非 NULL
      リテラルが結果を固定する `IS NULL` / `IS NOT NULL` を定数化）
- [x] `like_prefix_to_range`（ASCII の末尾 `%` だけを半開区間へ変換し、LIKE は
      残余述語として再評価。`_` / 中間 `%` / 非 ASCII は対象外）
- [x] `like_suffix_not_sargable` の明示（末尾ワイルドカードのみを prefix range 化し、suffix-only は回帰テストで full scan 固定）
- [ ] `regexp_prefix_extraction`
- [ ] `cast_pushdown_on_comparison`（型を列側に寄せる）
- [x] `redundant_cast_removal`（同一 target/null-on-error の nested CAST を
      `collapse_nested_identical_cast` で除去。異なる型の CAST は保持）
- [ ] `filter_merge_with_scan_zonemap` ヒント（実行器の zone map と接続）
- [x] `push_limit_into_scan`（WHERE / ORDER BY / DISTINCT / 集約がない有限 LIMIT
      に限り、OFFSET + LIMIT の上限を FullScan へ渡す early stop）
- [x] `push_limit_through_union_all`（`union_all_push_limit` として有限 LIMIT の
      offset+count を各枝へ伝播）
- [ ] `push_limit_through_inner_join_if_unique`（1:1 のとき）
- [ ] `offset_zero_elimination`
- [x] `limit_zero_to_empty`（SQL エンジンの明示 LIMIT 0 fast path。QueryData の
      `limit_count_ == 0` は OFFSET-only / 無制限との兼用のため、論理 Cascades
      ノードではまだ区別しない）
- [x] `merge_sort_limit_to_topn`（`SortPlan` + 有限 LIMIT を `TopNPlan` に融合）
- [ ] `eliminate_sort_under_unordered_consumer`
- [x] `prefix_sort_elimination`（Sort/TopN の出力順序を要求キーの prefix として
      `IsOrderedBy` から伝播）
- [ ] `redundant_distinct_under_unique_key`
- [ ] `distinct_to_group_by`
- [ ] `group_by_to_distinct`（agg 無し）
- [ ] `count_star_without_group_rewrite`
- [x] `push_filter_through_distinct`（行値 predicate は DISTINCT と可換なため
      `DISTINCT(Filter(input))` へ移動）
- [ ] `pull_filter_above_join`（選択性が極端に悪い場合の探索用、任意）

---

## P1 — 結合（inner の残り + 外側結合の入口）

- [ ] `join_left_assoc` / `join_right_assoc` の n-ary bushy 完全化
      （今の left/right 回転の穴埋め）
- [ ] `join_identity`（空 Values や 1 行定数表）
- [x] `join_with_true_to_cross`
- [ ] `cross_join_elimination`（1 行側）
- [x] `join_to_cross_if_no_predicate`
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
- [ ] `push_filter_through_left_join_left_side`（常に可）
- [ ] `push_filter_through_left_join_right_side`（null-rejecting のみ）
- [ ] `push_filter_above_left_join`（遅延評価が得な場合）
- [ ] `split_filter_over_outer_join`
- [ ] `predicate_move_around_outer`
- [x] `null_aware_anti_join`（`NOT IN` の三値論理。NULL 制約がない場合は
      `NullAwareAntiJoin` を選択）
- [x] `not_in_to_anti_join`（両キー NOT NULL のとき。V1 の相関なし形）
- [x] `in_to_semijoin`（単一列 IN サブクエリの直接 decorrelation）
- [x] `exists_to_semijoin`（相関キー + inner-only filter の直接 decorrelation）
- [x] `not_exists_to_antijoin`（相関キーの直接 decorrelation）
- [ ] `unique_semijoin_to_inner`
- [ ] `semijoin_to_inner_plus_distinct`
- [ ] `semijoin_reduction`（bloom / ハッシュ半結合の先行適用）
- [ ] `join_on_false_to_empty` / `left_join_on_false_to_left_nullpad`
- [ ] `decorate_join_with_cardinality`（ヒストグラム結合）
- [ ] `detect_one_to_one` / `one_to_many` / `many_to_many`
- [ ] `prefer_index_join_when_inner_unique`
- [ ] `prefer_hash_when_equality_and_unsorted`
- [x] `prefer_merge_when_both_sorted` → [`executor_todo.md`](executor_todo.md) MergeJoin
      （既存順序を再利用し、未ソート側は SortPlan を候補内で補完）
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
- [x] `distinct_via_hash` vs `distinct_via_sort`
- [x] `group_by_constant_removal`（定数 GROUP BY キーを hash key から除去。
      非空入力では同一グループ化し、空入力は SQL の 0 行意味論を保持）
- [ ] `group_by_functional_dependency_reduction`
- [ ] `aggregate_push_through_projection`
- [x] `scalar_agg_no_group_empty_input`（AggregationPlan / 逐次・並列集約の空入力
      回帰で COUNT=0、他の集約は NULL を固定）
- [ ] `any_value_elision`
- [ ] `bitwise_agg_rewrite`

---

## P2 — ソート・Top-N・物理順序

- [x] 論理 `Sort` と `enforces_order` プロパティの一本化
- [x] `sort_elimination`（入力が既に順序付き）
- [x] `sort_merge_of_compatible_orders`（二重 Sort の互換 prefix を 1 つへ統合。
      explicit NULLS 順序が異なる場合は変換しない）
- [ ] `partial_sort`（既に prefix がソート済み）
- [x] `topn_push_through_projection`（sort key を Projection 入力へ書き戻せる場合のみ）
- [ ] `topn_push_through_inner_join`（unique 外側）
- [x] `topn_into_index_scan`（インデックスが required ordering を満たす場合、
      `limit_hint` を適用して IndexScan の遅延反復を LIMIT 境界で停止）
- [x] `limit_plus_sort_to_heap_topn` → `TopNPlan` / `TopNExecutor`（heap 容量に
      OFFSET + LIMIT を含め、`AliasedOrderByLimitFoldsTopK` で回帰）
- [ ] `offset_fetch_rewrite`
- [x] `order_by_constant_removal`（リテラルキーを logical/physical sort 生成前に除去し、
      SQL 経路と直接 Optimizer 経路を回帰テスト）
- [ ] `order_by_redundant_column_removal`（FD）
- [x] `nulls_first_last_normalization`（`QueryData` → logical / physical key の
      optional 指定を保持。Sort / TopN の明示 NULLS FIRST/LAST テスト済み）
- [ ] `collation_aware_sort`（NULLS FIRST/LAST の指定は Sort/TopN に接続済み）
- [ ] `incremental_sort`（PostgreSQL Incremental Sort）
- [ ] `buffered_sort` vs `external_merge` 実装規則（Sort 実行器は既にある）

---

## P3 — サブクエリ・相関・CTE

- [ ] `unnest_scalar_subquery`（非相関）
- [ ] `unnest_in_subquery`
- [ ] `unnest_exists`
- [ ] `decorrelate_apply`（PullUpCorrelatedPredicates）
- [ ] `decorrelate_lateral`
- [x] `subquery_to_semijoin` / `antijoin`（V1 の IN / EXISTS / NOT EXISTS。
      LEFT JOIN + IS NOT NULL と一般相関 APPLY は未実装）
- [ ] `mark_join_to_filter`（`x IN (SELECT…)` の三値）
- [x] `single_join_max1row_assert`（relational evaluator のスカラー経路。
      standalone Cascades ノードは未実装）
- [ ] `correlated_filter_pullup`
- [ ] `push_correlated_predicate_into_subquery`
- [ ] `flatten_nested_subqueries`
- [ ] `merge_identical_subqueries`（CSE）
- [x] `cache_invariant_subquery`（非相関サブクエリを実行時キャッシュし、同一
      statement 内の再評価を `uncorrelated_cache_hits` へ記録）
- [ ] `cte_inlining`（参照 1 回、または安価）
- [ ] `cte_materialization`（参照複数、または再帰）
- [ ] `cte_filter_pushdown`
- [ ] `cte_predicate_propagation`
- [ ] `recursive_cte_union_rewrite`
- [ ] `worktable_scan_indexing`
- [x] `exists_short_circuit_limit_1`（単純な単一表サブクエリでスキャン上限へ pushdown、相関経路は既存 index を維持）
- [ ] `any_all_quantified_comparison_rewrite`
- [ ] `subquery_unnest_with_window`（禁止条件の明示）
- [ ] `common_subexpression_materialize`（同一 Scan/Join 部分木）

---

## P3 — 集合演算

- [x] `union_all_merge`（連続 UNION ALL を n-ary）
- [x] `union_to_union_all_plus_distinct`
- [ ] `intersect_to_semijoin`
- [ ] `except_to_antijoin`
- [x] `setop_push_projection`（UNION / UNION ALL の枝へ分配）
- [x] `setop_push_filter`（全 set-op の枝へ述語を分配）
- [x] `union_all_push_limit`（有限 LIMIT の offset+count を各枝へ伝播し、
      親 LIMIT で全体結果を確定）
- [ ] `union_distinct_hash_vs_sort`
- [ ] `cancel_union_empty` / `union_with_empty`
- [x] `intersect_with_empty` → empty（Cascades の `setop_empty_simplification` と回帰テスト）
- [x] `except_empty_right` → left（DISTINCT は重複排除を保持）
- [x] `cancel_union_empty` / `union_with_empty`（空枝を除去し、DISTINCT の重複排除を保持）
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
- [x] `unnest_with_ordinality`（`WITH OFFSET` を列へ射影し、配列/相関 UNNEST の回帰あり）
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

- [x] `project_remove`（`RemoveIdentityProjection` / `ProjectionPlan` の列定義一致判定）
- [x] `project_to_scan`（列部分集合 + テーブル。scan_projections と各 scan の
      fallback projection へ接続）
- [ ] `merge_calc`（Filter+Project を Calc に統合するか、逆に分解するか方針決定）
- [x] `common_subexpression_elimination` in target list（relational projection が
      式の fingerprint ごとに `projection_cache` を共有）
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
- [x] `coalesce_flatten` / `coalesce_of_coalesce`（nested `COALESCE` を引数列へ
      flatten。評価順を維持する rewrite 回帰付き）
- [x] `nullif_to_case`（副作用のない列＋リテラル形だけを CASE/IF へ変換）
- [x] `if_to_case`（3 引数 IF を短絡評価を保つ CASE へ変換） / [x] `case_to_if`（単一 WHEN の CASE を canonical IF へ変換し、往復を防止）
- [x] `greatest_least_fold`（全リテラルは既存の deterministic function fold、
      1 引数は恒等式へ縮約）
- [ ] `between_symmetric`
- [ ] `is_distinct_from_rewrite`
- [x] `boolean_eq_true_false_three_valued`（構文上の論理式だけを TRUE/FALSE
      リテラル比較から恒等式/NOT へ変換し、NULL は UNKNOWN のまま保持）
- [ ] `and_true_elim` / `or_false_elim` の NULL 厳密化監査
- [ ] `distribute_or_over_and`（選択的、爆発抑制）
- [ ] `distribute_and_over_or`
- [ ] `cnf_conversion_budgeted`
- [ ] `dnf_conversion_budgeted`
- [ ] `extract_disjunctive_sarg`
- [x] `in_empty_list` → false / unknown（空リストは FALSE、NULL 要素だけの
      1 要素リストは UNKNOWN に固定）
- [x] `in_single_null`
- [ ] `not_in_with_null_list` 警告と計画
- [ ] `like_escape_normalize`
- [ ] `ilike_to_lower_like`
- [ ] `similiar_to_to_regex`
- [ ] `arithmetic_overflow_safe_fold`
- [x] `date_add_sub_fold`（決定的なリテラル関数折りたたみで DATE_ADD/SUB を定数化）
- [ ] `interval_normalize`
- [x] `concat_flatten`（評価順を保ったまま nested CONCAT の引数を平坦化）
- [x] `substring_constant_fold` 強化（決定的なリテラル関数折りたたみを回帰）
- [ ] `json_path_constant_fold`
- [x] `array_constructor_fold`（定数要素の `ArrayExpression` を `fold_array` で
      配列 Value へ畳み込み、rewrite 回帰を追加）
- [ ] `array_length_zero`
- [ ] `safe_divide_rewrite`
- [x] `abs_of_abs`（`ABS(ABS(x))` を安定な関数形に縮約）
- [ ] `log_identities`
- [x] `comparison_of_same_expr`（安定した列参照に限定して `x=x` → `x IS NOT NULL`）
- [x] `self_inequality`（`x<x` / `x>x` を NULL 保持 CASE へ変換、三値論理テストあり）
- [ ] `deterministic_function_cse`
- [x] `nondeterministic_barrier`（時刻・乱数・UUID 系を定数畳み込みから除外）
- [ ] `stable_vs_immutable` 分類
- [ ] `rewrite_or_of_ranges_to_in`
- [ ] `extract_year_sargable`（可能なら range）
- [x] `not_between_to_or`（既存の De Morgan / 比較否定書き換えで実装済み、NULL 回帰テストあり）
- [x] `xor_to_or_and_not`（SQL 三値論理を保つ `(a OR b) AND NOT(a AND b)`）
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
- [x] 半結合の上界 `min(|L|,|R|)` と反結合の probe-side 上界を
      物理候補／`ProductPlan` の見積りへ反映（FK 上界は未実装）
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
- [x] `hash_distinct` / `sort_distinct`
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

- [x] `merge_join`（等価、両側ソート済み）
- [x] `sort_merge_join`（必要なら子に SortPlan を要求）
- [x] `outer_hash_join` / `left` / `right` / `full`（`JoinKind` と実装規則を接続）
- [x] `outer_merge_join`（`outer_hash_join` 規則が等価述語の merge alternative と
      子 SortPlan を併せて生成。`MergeJoinPlan` の LEFT/RIGHT/FULL と同じ NULL
      padding 契約を共有）
- [ ] `outer_nested_loop`
- [x] `semi_hash_join` / `anti_hash_join`（等価キー・残余なし。既存
      `HashJoin` の `JoinKind` へ接続）
- [x] `semi_merge_join` / `anti_merge_join`（ソート子の自動挿入と probe 側スキーマを
      保持する MergeJoinPlan）
- [ ] `index_only_agg`（MIN/MAX）
- [ ] `bitmap_scan` / `bitmap_and` / `bitmap_or`
- [ ] `tid_scan` / `rowid_lookup`
- [x] `parallel_seq_scan` を Cascades から明示選択（統計閾値と回帰テスト）
- [ ] `pax_scan` / `zone_map_skip`
- [ ] `skip_scan`（複合インデックスの先頭欠落）
- [ ] `index_skip_scan_for_distinct`
- [ ] `covering_index_rewrite`
- [ ] `sort_agg` / `hash_agg` の選択
- [ ] `parallel_hash_agg` / `parallel_stream_agg`
- [x] `topn_heap`（TopN implementation rule emits `TopNPlan` and preserves
      required NULL placement）
- [ ] `window_agg`
- [ ] `setop_hash` / `setop_sort`
- [x] `unnest_exec`（現行の relational fallback で `Unnest` / OFFSET を実行）
- [ ] `recursive_union`
- [ ] `insert` / `update` / `delete` / `upsert` の DML 計画
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
- [x] `INSERT SELECT` の投影（SQL 実行経路で SELECT の投影結果を target
      列順へ写像して Insert source に接続。回帰は
      `SqlEngineInsertSelectCopiesAndMapsRows`。DML を Cascades で直接最適化する
      並列化は DML 論理ノード導入時に追加）
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
4. LEFT JOIN の論理ノード + null-rejecting pushdown。
5. Merge Join 物理（ソート興味順序とセット）。
6. Mark/Single Join と一般相関 APPLY。
7. Sort-Agg vs Hash-Agg の選択。
8. サブクエリ decorrelation。
9. UNION / Window。
10. 統計ヒストグラム。
