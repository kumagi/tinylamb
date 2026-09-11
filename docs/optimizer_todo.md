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
SemiJoin / AntiJoin を持つ。商用相当の種類は一通り揃い、
残りは構文・カタログ・分散の表面待ちである。

- [x] `kOuterJoin`（LEFT / RIGHT / FULL）と join type ペイロード、outer hash
      実装規則（安全な pushdown 規則は未導入）
- [x] `kSemiJoin` / `kAntiJoin`（等価キーの Cascades 実装規則まで接続）
- [x] `kMarkJoin`（`IN` の UNKNOWN marker。enum・`mark_hash_join` 実装規則に
      加え `mark_join_to_filter` で marker 真偽フィルタを Semi/Anti へ lowering）
- [x] `kSingleJoin`（スカラ相関の基数 assertion は `kMax1Row` / `Max1RowPlan`
      として実装）
- [x] `kCrossJoin` を inner と分離（無条件 join を明示化し、cross-product
      カーディナリティでコスト評価。`join_identity_dummy` で DummyScan 側の
      単位元除去まで接続）
- [x] `kApply`（相関 APPLY。enum・`apply` 実装規則・`apply_to_join` /
      `push_apply_through_join` / `hoist_correlated_selection_to_apply` /
      `decorrelate_aggregate_apply` で型付き lowering。一般 LATERAL の
      コスト付き展開は FROM 派生スキャン葉（M5）待ちで relational 維持）
- [x] `kUnion` / `kUnionAll` / `kIntersect` / `kIntersectAll` / `kExcept` /
      `kExceptAll`（論理ノードと物理実装規則）
- [x] `kWindow`（PARTITION / ORDER ペイロード＋6 論理規則
      ＋`rank_row_number_to_topn`。frame 型・exclusion は
      `WindowFunctionCallExpression` の式レベルで保持。物理実行は
      relational `window_eval` 経由で、window 集約 migration 後に Cascades
      実装規則へ）
- [x] `kSort` を一般 Cascades 論理ノードとして明示し、`SortPlan` の実装規則と
      出力スキーマ基準の ORDER BY キー正規化を接続
- [x] `kDistinct` / `kDuplicateElim`
- [x] `kTopN`（ORDER BY + LIMIT を 1 ノードに。`with_ties` ペイロードと
      `TopNPlan` への透過まで接続。WITH TIES 問い合わせ自体は
      `SelectStatement::complex_` 経由で relational 経路に残る）
- [x] `kValues` / `kConstantTable`（`ValuesPlan` と Cascades 実装規則。
      単一行分岐の `values_fold_into_union` まで接続）
- [x] `kUnnest` / `kGenerateSeries`（enum・`unnest` / `generate_series` 実装規則
      ・`unnest_filter_pushdown` まで接続。一般 TVF は UNNEST・GENERATE_SERIES
      以外の表面がなく将来枠）
- [x] `kExpand`（enum と `grouping_sets_expansion` で GROUPING SETS 系を展開。
      CUBE / ROLLUP 構文自体が parser 未対応のため到達不能）
- [x] `kRecursiveCte` / `kWorkTableScan`（relational 経路で実装:
      `ExecuteRecursiveCte` が作業表反復を担当。Cascades 論理ノードは未導入で、
      WITH RECURSIVE は opaque relational IR として実行される）
- [x] `kMaterialize` / `kEagerSpool` / `kLazySpool`（enum・`MaterializePlan`
      ・`materialize` / `eager_spool` / `lazy_spool` 実装規則まで接続。
      `MaterializeExecutor` が順序保持で再生）
- [x] `kExchange` / `kGather` / `kBroadcast` / `kRedistribute`（enum と単一
      ノード no-op 実装規則 `exchange_noop` 等まで接続。分散コストは将来枠）
- [ ] `kSample` / `kTableSample`（enum のみ。TABLESAMPLE 構文がなく SQL 表面
      がないため Plan・実行器とも未接続。構文追加時に `SamplePlan` から）
- [x] `kAssert`（`kMax1Row` / `Max1RowPlan` がスカラ基数 assertion の実現形。
      一般形の enum は予約）
- [x] `kMax1Row`（スカラサブクエリ基数の専用論理・物理ノード）
- [x] `kFilterFalse` / `kEmpty`（定数 FALSE / NULL Selection から EmptyPlanへ
      論理・物理接続）
- [x] `kDummyScan`（`SELECT 1` の 1 行ソースを Relational から外し、Optimizer
      の no-FROM 経路へ接続）
- [x] Join ペイロードに outer join type（LEFT / RIGHT / FULL）と
      null-producing side（`LogicalExpression::join_type` → `JoinKind`）
- [x] Aggregation ペイロードに grouping sets（`grouping_sets` フィールド＋
      `grouping_sets_expansion`。`aggregate_union_transpose` 等が利用）
- [x] Aggregation 式の DISTINCT agg / FILTER 相当（GoogleSQL `AGG(x WHERE p)`）/
      ORDER BY WITHIN GROUP メタデータを式・rewrite・plan cache で保持
- [x] Projection ペイロードに computed vs passthrough 列の区別（明示マーカー
      ではなく touched-column 解析で等価に実現：`prune_unused_projection_columns`
      ・`push_projection_through_join`・`push_projection_below_join_width_control`
      が列要否を判定。`widen` 方向は保守的デフォルト＝刈り込まない）
- [x] Window ペイロードに frame 型（ROWS / RANGE / GROUPS）と exclusion
      （`WindowFunctionCallExpression::{frame_unit,exclusion}` の式レベルで保持。
      論理ノードは `partition_by` を共有キーとして運ぶ）
- [x] Set-op ペイロードに ALL vs DISTINCT
- [x] Set-op ペイロードの列対応・共通型（`SetOperationPlan` が列幅を検証し、
      数値共通型を出力スキーマへ反映。枝側への型押し下げは
      `setop_push_projection` との合成で等価）
- [x] `PhysicalProperties` に collation、nulls first/last、partitioning、
      uniqueness、sorted-prefix、bloom/filter 伝播（`collation`・
      `sort_nulls_first`・`partition_by`・`is_unique`・`bloom_filter_keys`
      フィールド＋`IsOrderedBy` prefix 判定＋`IncrementalSortPlan`＋
      `dynamic_filter_pushdown_join` まで接続）
- [ ] グループの出力スキーマを常に保持（identity projection 判定に必要。
      意図的に任意のまま：全規則・全テストの式構築を churn する大改修で、
      意味論上の利得がない。`AddExpression` の構造検証＋
      `RemoveIdentityProjection` の物理判定で代替）
- [x] null-rejection / strong-null / weak-null 解析 API（第一版:
      `IsNullRejectingPredicate` の構文的厳格解析。比較 / IS NOT NULL /
      AND / NOT / 定数 IN を strict と判定し、outer join 縮約と押し下げに接続。
      strong-null / weak-null の区別は未導入）
- [x] FD（関数従属）・unique key・not-null をグループ属性として伝播
      （`DeriveLogicalProperties` が `candidate_keys`・`not_null_columns`・
      `equivalence_classes`・`max_1_row` を派生し、
      `group_by_functional_dependency_reduction`
      ・`count_star_rewrite_on_not_null`・`topn_push_through_inner_join` が利用）
- [x] 等価クラス（equivalence class）をメモ全体で共有
      （グループ属性＋`infer_filter_from_equivalence_class`＋
      `join_predicate_transitivity`＋`InferJoinConstants` / `InferJoinInequalities`
      まで接続）
- [x] 述語の正規形（CNF / DNF）を選択的に保持
      （`CanonicalizeConjuncts` のソート・重複除去＋
      `distribute_or_over_and_budgeted` による予算付き CNF 化。
      DNF 方向は `factor_or_common_and` と振動するため意図的に除外。
      詳細は P5 `distribute_*` 項）
- [x] 外側結合の null-supplying side をパターン DSL で制約
      （`PayloadConstraint::outer_join_type`＋`LeftOuterJoin` /
      `RightOuterJoin` / `FullOuterJoin` ヘルパー。`outer_nested_loop` が利用）
- [x] `NeedsRelationalEvaluation` 経路を段階的に Cascades へ移す計画表
      （下の M4〜M8 地図がその計画表。各移管の前提・残件を明記）

  2026-09-04 時点の経路地図（Wave 2 M1〜M3 実施済み、M9 未達）:

  - **Cascades 経路済み**: no-FROM SELECT / 単一表（集計・ORDER BY 計算式・
    エイリアス参照含む） / エイリアス・非エイリアス INNER/CROSS 結合 /
    集合演算（`ExecuteSetOperation`、オペランド再帰 + 実行器レベル折り畳み、
    INTERSECT 優先結合・BY NAME・per-pair 演算子対応） / 複数表 GROUP BY・
    HAVING（`ExecuteGroupedSelect` + `GroupByPlan`。Cascades が FROM+WHERE
    コアを最適化し、グルーピング仕上げは `FinishQuery` 経由）。
    ただし集合演算は `plan_contains` が relational 形式の
    `optimizer_subquery_setop_high_expectations` 等を結果契約のみに緩和済み。
  - **残存する relational 経路（M4〜M8）と各移管要件**:
    1. **CTE / 再帰 CTE**（M4）: Cascades コアが CTE 名を解決できない。
       memo に materialized-scan 葉（派生スキャン）オペレータが必要。
       再帰は `ExecuteRecursiveCte` 相当の物理演算子接続。
    2. **FROM サブクエリ / LATERAL**（M5）: 派生スキャン葉に同じく依存。
       LATERAL は相関パラメータの per-row 再実行が必要（subquery_runtime 相当）。
    3. **OUTER JOIN**（M6）: `GroupByPlan` コアは外部結合条件の WHERE 平坦化が
       意味論を壊すため除外済み。memo `kOuterJoin` に join-type 付き lowering
       （`Memo::Build` の join 木構築拡張）が必要。
       ※ SELECT COUNT(*) FROM a LEFT JOIN b は外部結合ガードで relational 保棄。
    4. **UNNEST / TVF**（M7）: `kUnnest` 論理 op は enum に存在、実装規則未接続。
    5. **相関サブクエリ残部**（M8）: `GroupedSelect` は式内 QueryExp で
       フォールバック済み（`grouped_expressions_correlate` ガード）。
       デコリレーション不能形状は subquery_runtime 依存を維持。
  - **M9（planner 削除）の前提**: 上記 1〜5 の全移管後、
    `executor/relational.cpp`（2,562 行）+ `executor/detail/planning_heuristics.cpp`
    （1,853 行）+ `sql_engine.cpp` の `emit_relational` 3 箇所 +
    `Optimizer::OptimizeRelational` / `kRelational` を削除する。
    実行ヘルパ（expression_eval / window_eval / scan_filter / subquery_runtime の
    評価器部分）は残す。`OptimizerAndRelationalPathsAgree` 監査テストは役目終了。
  - **既知の未復帰修正**（stash 適用で復元済みのはずだが要確認）:
    `sort.cpp SingleKey` DESC 符号混在修正（回帰テスト
    `RelationalSortMixedSignDescendingKeepsOrder`）。
  - **モノトニック索引順序保存**（`ORDER BY` 出力エイリアスが索引列の単調関数の
    場合ソート省略）は optimizer 経路未実装。`optimizer_cse_projection_high_expectations`
    を参照。

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
- [ ] `eliminate_identity_projection`（物理実装時に列定義が一致する場合。Cascades ルールは追加済みだが、出力スキーマのカラム名変更で join プレディケートが壊れるため保留。意図的な保留であり `RemoveIdentityProjection` の物理判定が代替）
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
- [x] `simplify_filter_with_fd`（関数従属で冗長述語削除）
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
- [x] `in_list_to_semijoin`（大きな IN リスト）
- [x] `extract_common_or_predicates`（式レベルの安全な共通 AND 因子抽出を
      `factor_or_common_and` として実装。関係レベルの DNF 展開は抑制）
- [x] `predicate_push_into_case`（`predicate_pushdown_case` として実装済み）
- [x] `null_rejecting_is_not_null_insert` / `not_null_inferred_from_inner_join`
      （`inner_join_not_null_inference` として実装済み。挿入方向・推論方向とも）
- [x] `contradiction_from_null_eq`（比較式の片側 NULL を UNKNOWN に定数化し、
      Selection の Empty 化へ接続）
- [x] `canonicalize_boolean`（`x=true` → `x`、`x=false` → `NOT x`。
      ブール値生成子（AND/OR/NOT/IS 述語/IN）に限定し、三値論理を保持。
      非ブール列の `int_col = TRUE` は比較意味論のまま）
- [x] `simplify_coalesce_in_filter`（先行する NULL リテラルだけを許し、非 NULL
      リテラルが結果を固定する `IS NULL` / `IS NOT NULL` を定数化）
- [x] `like_prefix_to_range`（ASCII の末尾 `%` だけを半開区間へ変換し、LIKE は
      残余述語として再評価。`_` / 中間 `%` / 非 ASCII は対象外）
- [x] `like_suffix_not_sargable` の明示（末尾ワイルドカードのみを prefix range 化し、suffix-only は回帰テストで full scan 固定）
- [x] `regexp_prefix_extraction`（`regexp_prefix_extraction` として実装済み。
      `like_prefix_to_range` と対）
- [x] `cast_pushdown_on_comparison`（`cast_pushdown_on_comparison` 論理規則として
      実装。同一ドメイン冗長 CAST と DATE→TIMESTAMP に限定し、定数の往復
      CAST 証明＋全射性ゲートで健全性を担保。列側へ寄せた形が sargable になり
      index range 抽出へ接続。損失 CAST（INT64→FLOAT64 等）は対象外）
- [x] `redundant_cast_removal`（同一 target/null-on-error の nested CAST を
      `collapse_nested_identical_cast` で除去。異なる型の CAST は保持）
- [x] `filter_merge_with_scan_zonemap` ヒント（実行器の zone map と接続）
- [x] `push_limit_into_scan`（WHERE / ORDER BY / DISTINCT / 集約がない有限 LIMIT
      に限り、OFFSET + LIMIT の上限を FullScan へ渡す early stop）
- [x] `push_limit_through_union_all`（`union_all_push_limit` として有限 LIMIT の
      offset+count を各枝へ伝播）
- [x] `push_limit_through_inner_join_if_unique`（1:1 のとき）
- [x] `offset_zero_elimination`（表現上、OFFSET 0 は既定値と同一の正規化済み
      形であり、専用変換の余地なし。TopN / union branch cap は offset=0 を
      正しく透過）
- [x] `limit_zero_to_empty`（SQL エンジンの明示 LIMIT 0 fast path。QueryData の
      `limit_count_ == 0` は OFFSET-only / 無制限との兼用のため、論理 Cascades
      ノードではまだ区別しない）
- [x] `merge_sort_limit_to_topn`（`SortPlan` + 有限 LIMIT を `TopNPlan` に融合）
- [x] `eliminate_sort_under_unordered_consumer`（実装済み。
      集約・DISTINCT 下の Sort を除去）
- [x] `prefix_sort_elimination`（Sort/TopN の出力順序を要求キーの prefix として
      `IsOrderedBy` から伝播）
- [x] `redundant_distinct_under_unique_key`
      （`pk_unique_distinct_elimination` として実装済み）
- [x] `distinct_to_group_by`
- [x] `group_by_to_distinct`（agg 無し）
- [x] `count_star_without_group_rewrite`
- [x] `push_filter_through_distinct`（行値 predicate は DISTINCT と可換なため
      `DISTINCT(Filter(input))` へ移動）
- [x] `pull_filter_above_join`（選択性が極端に悪い場合の探索用、任意）

---

## P1 — 結合（inner の残り + 外側結合の入口）

- [x] `join_left_assoc` / `join_right_assoc` の n-ary bushy 完全化
      （left/right 回転＋関係 ≤16 の網羅列挙 `join_enumeration` で bushy 形を
      すべて導出。非連結カット prune 付き）
- [x] `join_identity`（空 Values や 1 行定数表。`join_empty_simplification`＋
      `one_row_cross_join_elimination`＋`join_identity_dummy` で被覆）
- [x] `join_with_true_to_cross`
- [x] `cross_join_elimination`（1 行側。上記 identity 群で被覆）
- [x] `join_to_cross_if_no_predicate`
- [x] `inner_join_to_filter_cross` の逆（条件付き cross → inner。
      `cross_to_inner_with_predicate` と対）
- [x] `swap_inner_join_children_cost`（`join_commutativity` で両順序を列挙し
      コストが選択。専用ルール不要）
- [ ] `dphyp` / `dpccp` 連結部分グラフ列挙（`join_enumeration` の置換。
      結果は等価で速度のみの問題のため性能枠。>16 は greedy 継続）
- [x] `greedy_join_order` フォールバック（関係 >16。
      `greedy_join_order_fallback` として実装済み）
- [x] `ikkbz` / `goo` ヒューリスティック（greedy fallback＋`star_join_reorder`
      がヒューリスティック役割を担う。IKKBZ 固有の順序付けは将来枠）
- [x] `star_join_reorder`（ファクトを中央に。実装済み）
- [x] `bushy_vs_left_deep` の探索予算（両形を列挙し
      `search_step_budget` で労力を制限）
- [x] `join_predicate_transitivity` の一般化（定数以外、等価クラス全体。
      `join_predicate_transitivity`＋`infer_filter_from_equivalence_class`）
- [x] `inferred_inequality`（`a=b AND a<10` → `b<10`。
      `inferred_inequality_pushdown` として実装済み）
- [ ] `add_redundant_join_predicate`（ハッシュ分散用。冗長述語の追加は
      メモ膨張 against 微効用のため将来枠）
- [x] `remove_redundant_join` / `join_elimination_unique_key`
      （PK-FK、SELECT が親だけ。 multiplicity 保存形として
      `fk_join_elimination`・`unused_join_elimination`（Semi 化）・
      `self_join_elimination` で被覆。完全除去は包含証明に FK カタログ
      （`Constraint::kForeign` は未実装）が必要なためカタログ待ち）
- [x] `self_join_elimination`
- [x] `outer_to_inner`（WHERE が null-rejecting。relational 経路の
      `ReduceOuterJoinsToInner`: LEFT/RIGHT/FULL を NULL 側を被る厳格述語で
      inner へ縮約し、EXPLAIN 表示にも反映。`IsNullRejectingPredicate` 参照）
- [x] `outer_to_anti`（`WHERE right.key IS NULL`）
- [x] `full_outer_to_left_plus_anti`
- [x] `right_join_to_left_join`（子の交換）
- [x] `left_join_commutativity`（禁止、テストで固定。意図的非実装として確定）
- [x] `push_filter_through_left_join_left_side`（常に可。BuildInput の outer 経路で
      非 NULL 側ソースへの単一関係 conjunct を join 前に適用）
- [x] `push_filter_through_left_join_right_side`（null-rejecting のみ。
      null-rejecting の場合は outer_to_inner 縮約が等価かつそれ以上の効果を
      持つため、縮約経路で実現）
- [ ] `push_filter_above_left_join`（遅延評価が得な場合。コスト比較基盤の将来枠）
- [x] `split_filter_over_outer_join`（左側 push＋null-rejecting 縮約の合成で等価。
      右側単独の無条件 push は意味論上不可のためこの形で確定）
- [ ] `predicate_move_around_outer`（完全な null-rejection フレームワーク待ち）
- [x] `null_aware_anti_join`（`NOT IN` の三値論理。NULL 制約がない場合は
      `NullAwareAntiJoin` を選択）
- [x] `not_in_to_anti_join`（両キー NOT NULL のとき。V1 の相関なし形）
- [x] `in_to_semijoin`（単一列 IN と追加相関述語を複合キー化する直接 decorrelation）
- [x] `exists_to_semijoin`（複合相関キー + inner-only filter の直接 decorrelation）
- [x] `not_exists_to_antijoin`（複合相関キーの直接 decorrelation）
- [x] `unique_semijoin_to_inner`
- [x] `join_on_false_to_empty` / `left_join_on_false_to_left_nullpad`（`cascades.cpp` にルール追加済み — FALSE/NULL プレディケートの Join を LIMIT 0 に短絡）
- [x] `semijoin_to_inner_plus_distinct`（`semijoin_to_inner_plus_distinct` として
      実装。unique 時は `unique_semi_to_inner` の素朴 inner が勝つためコスト選択）
- [x] `semijoin_reduction`（bloom / ハッシュ半結合の先行適用。
      `dynamic_filter_pushdown_join`＝実行時ブルーム半結合削減として実装）
- [x] `decorate_join_with_cardinality`（ヒストグラム結合。
      `EstimateHistogramJoinCardinality`＋列ヒストグラム統計まで接続）
- [x] `detect_one_to_one` / `one_to_many` / `many_to_many`
      （`EstimateJoinCardinality` の multiplicity 判定＋
      `PhysicalProperties::join_multiplicity` まで接続）
- [x] `prefer_index_join_when_inner_unique`
- [x] `prefer_hash_when_equality_and_unsorted`
      （いずれも明示ルールではなくコスト選択で実現：index/hash/merge/NL の
      代替案が競合しコストが選ぶ。ハードな優先付けは将来のヒント枠）
- [x] `prefer_merge_when_both_sorted` → [`executor_todo.md`](executor_todo.md) MergeJoin
      （既存順序を再利用し、未ソート側は SortPlan を候補内で補完）
- [x] `nested_loop_for_non_equality`（`nested_loop_join`＝cross＋完全述語。
      外側版は `outer_nested_loop`）
- [x] `block_nested_loop` 実装規則（`NestedLoopJoin` 自体が block_size 1024 の
      block NL として実装）
- [x] `lookup_join` / `index_nested_loop` の一般化（複合キー、範囲。
      `index_join` が複合キー対応。範囲 INL はコストモデル待ちの将来枠）
- [ ] `batch_nested_loop`（IN リスト化して内側を一括。`BatchNestedLoopJoin`
      実行器はあるが Cascades 配線はコストモデル待ち）
- [x] `dynamic_filter_join`（実行時ブルームを内側スキャンへ。
      `dynamic_filter_pushdown_join`）
- [ ] `late_materialization_join`（行 ID で join してから列を取る。
      rowid 表面がなく TID 経路は DML 専用のため将来枠）

---

## P2 — 集約・GROUP BY・DISTINCT

- [x] `aggregate_project_merge`
- [x] `aggregate_filter_transpose` の残余 HAVING 整理（`having_to_filter_rewrite`
      ＋`filter_aggregate_pushdown` で被覆）
- [x] `aggregate_join_transpose`（agg を join の下へ、unique / キー保存時。
      右キー unique の 1:N 証明ゲート付きで有効。`cascades_optimizer.md` の
      D5 監査表の「Disabled」記載は改訂前のもの）
- [x] `aggregate_union_transpose`
- [x] `split_aggregate`（partial / final。UNION 分岐ごとの部分集約＋最終集約
      として `aggregate_union_transpose`＋`partial_agg_union` 系で被覆。
      並列 partial は `ParallelAggregation` 実行器側のしきい値駆動で、
      明示の論理分割は分散枠に残る）
- [x] `eager_aggregation`（join 前に group）
- [ ] `lazy_aggregation`（集約の遅延配置。eager の双対だが適用条件の定式化が
      未了のため将来枠）
- [x] `remove_aggregate_if_unique`（group キーが unique。
      `unique_group_key_aggregate_elimination` として実装済み）
- [x] `count_star_rewrite_on_not_null`（`count_star_rewrite_on_not_null` として
      実装。NOT NULL 宣言・派生の両証明に対応し COUNT(*) 高速路へ接続）
- [x] `sum_zero_identity` 等の agg 代数（スカラー側の `safe_multiply_zero` 等で
      被覆。SUM 固有の恒等式は空集合 NULL 意味論のため追加なし）
- [x] `minmax_index_only`（非NULLな単一キー列のINDEX MIN/MAXスキャン）
- [x] `distinct_aggregate_expansion`（`COUNT(DISTINCT)` を二重 agg）
- [x] `grouping_sets_expansion` と `grouping_sets_to_union`
- [ ] `rollup_to_grouping_sets` / `cube_to_grouping_sets`（ROLLUP / CUBE 構文が
      parser 未対応のため到達不能。構文追加時に grouping sets 展開へ）
- [ ] `grouping_id_simplification`（GROUPING_ID 関数が未実装のため到達不能）
- [x] `having_to_filter_after_agg` の明示ノード
- [x] `filter_aggregate_argument`（FILTER 句）
- [x] `ordered_set_aggregate`（PERCENTILE_CONT 等。`kPercentileCont` 集約型＋
      WITHIN GROUP メタデータ保持＋汎用集約実装規則で実行。専用の転位規則は
      不要のためこの形で確定）
- [ ] `approx_aggregate_rewrite`（HLL 等、意味論が許すとき。近似への書換えは
      オプトインなしでは意味論変更になるため、ヒント枠とセットで将来対応）
- [x] `two_phase_hash_agg` vs `sort_agg` 実装規則 → SortAgg 実行器
      （`aggregation` 規則が Hash/Sort 両代替をコスト付きで提示。
      部分集約側は `PartialAggregate` 実行器＋UNION 転位で被覆）
- [x] `stream_aggregate_if_sorted`（`aggregation` 規則の第三代替
      `StreamAggregatePlan` として実装。スカラ集約は単一グループのため任意順序で
      正当。グループ集約の順序前提活用は GroupBy migration 後の枠）
- [x] `distinct_via_hash` vs `distinct_via_sort`
- [x] `group_by_constant_removal`（定数 GROUP BY キーを hash key から除去。
      非空入力では同一グループ化し、空入力は SQL の 0 行意味論を保持）
- [x] `group_by_functional_dependency_reduction`
      （`group_by_functional_dependency_reduction` として実装。unique キーを
      含む grouping set の冗長キーを除去）
- [x] `aggregate_push_through_projection`（`aggregate_projection_merge` で被覆）
- [x] `scalar_agg_no_group_empty_input`（AggregationPlan / 逐次・並列集約の空入力
      回帰で COUNT=0、他の集約は NULL を固定）
- [x] `any_value_elision`（`any_value_elimination` として実装。grouping key 上の
      ANY_VALUE をキー参照へ）
- [ ] `bitwise_agg_rewrite`（BIT_AND / BIT_OR 集約関数が未実装のため到達不能）

---

## P2 — ソート・Top-N・物理順序

- [x] 論理 `Sort` と `enforces_order` プロパティの一本化
- [x] `sort_elimination`（入力が既に順序付き）
- [x] `sort_merge_of_compatible_orders`（二重 Sort の互換 prefix を 1 つへ統合。
      explicit NULLS 順序が異なる場合は変換しない）
- [x] `partial_sort`（既に prefix がソート済み。`IncrementalSortPlan` が順序
      prefix を保持し suffix のみソート。実装規則内で `IsOrderedBy` prefix 検出）
- [x] `topn_push_through_projection`（sort key を Projection 入力へ書き戻せる場合のみ）
- [ ] `topn_push_through_inner_join` は**無効化**（docs/cascades_optimizer.md の
      無効化ルール表を参照）。右側一意 + 左側 NOT NULL は「各左行のマッチ数
      ≤ 1」しか証明せず、「必ず 1 行マッチ（1:1）」には FK / 参照整合性
      メタデータが必要。Memo は FK をモデルしていないため、証明可能になる
      まで再有効化不可。反例テスト: `TopNPushThroughInnerJoinOnForeignKey`
- [x] `topn_into_index_scan`（インデックスが required ordering を満たす場合、
      `limit_hint` を適用して IndexScan の遅延反復を LIMIT 境界で停止）
- [x] `limit_plus_sort_to_heap_topn` → `TopNPlan` / `TopNExecutor`（heap 容量に
      OFFSET + LIMIT を含め、`AliasedOrderByLimitFoldsTopK` で回帰。
      `with_ties` ペイロード透過付き）
- [x] `offset_fetch_rewrite`（OFFSET/LIMIT は Limit/TopN ペイロード
      （offset+count）として第一級に保持。専用変換の余地なし）
- [x] `order_by_constant_removal`（リテラルキーを logical/physical sort 生成前に除去し、
      SQL 経路と直接 Optimizer 経路を回帰テスト）
- [x] `order_by_redundant_column_removal`（FD。`order_by_redundant_column_removal`
      として実装。unique prefix 以降のキーを除去）
- [x] `nulls_first_last_normalization`（`QueryData` → logical / physical key の
      optional 指定を保持。Sort / TopN の明示 NULLS FIRST/LAST テスト済み）
- [ ] `collation_aware_sort`（照合順序が式・カタログのどこにもモデル化されて
      おらず、COLLATE 表面もないため将来枠。NULLS 指定は接続済み）
- [x] `incremental_sort`（入力のORDER BY prefixを保持し、suffixをprefixグループ内でソート）
- [x] `buffered_sort` vs `external_merge` 実装規則（Sort 実行器＋`spill_file`
      による実行器内スピルで被覆。明示の物理分岐はコスト差が安定しないため
      この形で確定）

---

## P3 — サブクエリ・相関・CTE

- [x] `unnest_scalar_subquery`（非相関 scalar は InitPlan として一度だけ評価し、
      同一 SELECT 内で再利用回数を EXPLAIN に表示）
- [x] `unnest_in_subquery`（非相関 IN は NULL を除いた hash membership を構築し、
      三値論理のための NULL 検査を残す。struct/array は一般経路を維持）
- [x] `unnest_exists`（非相関 EXISTS は一度だけ materialize、相関単一表は
      equality index と parameterized result cache を使用）
- [x] `decorrelate_apply`（PullUpCorrelatedPredicates。型付き Apply の lowering
      `apply_to_join`＋`push_apply_through_join`＋
      `hoist_correlated_selection_to_apply`＋`decorrelate_aggregate_apply` で
      被覆。不透明 Apply の一般 decorrelation は subquery_runtime 維持）
- [ ] `decorrelate_lateral`（FROM 派生スキャン葉（M5）待ち。relational 維持）
- [x] `subquery_to_semijoin` / `antijoin`（V1 の IN / EXISTS / NOT EXISTS。
      LEFT JOIN + IS NOT NULL と一般相関 APPLY は未実装）
- [x] `mark_join_to_filter`（`x IN (SELECT…)` の三値。`mark_join_to_filter`
      として実装。IS TRUE/=TRUE→Semi、IS FALSE/=FALSE/NOT→Anti）
- [x] `single_join_max1row_assert`（relational evaluator のスカラー経路。
      standalone Cascades ノードは未実装）
- [x] `correlated_filter_pullup`（相関等式を index probe key として subquery
      の単一表 scan より前に適用。複合相関も composite key として保持し、
      非等式は保守的に一般経路へ）
- [x] `push_correlated_predicate_into_subquery`（相関等式の local 側を index
      build、local-only predicate を build 時 filter。NULL key は push しない）
- [x] `flatten_nested_subqueries`（単一表の identity projection と、行数を変えない
      immutable projection の nested derived table。WHERE / ORDER BY の列参照を
      内側式へ再束縛し、GROUP/LIMIT/WINDOW/volatile 式は境界を保持）
- [x] `merge_identical_subqueries`（非相関 QueryExpression を構造 fingerprint と
      継承 CTE の実体識別子で共有。scalar 結果と IN membership の構築を
      statement 内で一度にし、`uncorrelated_cache_hits` へ反映）
- [x] `cache_invariant_subquery`（非相関サブクエリを実行時キャッシュし、同一
      statement 内の再評価を `uncorrelated_cache_hits` へ記録）
- [x] parameterized Apply cache（decorrelate できない相関 scalar を外側キーごとに
      再利用し、`correlated_result_cache_hits` を記録。単一表 equality probe の
      専用 index に加え、derived/opaque source も外側スコープのパラメータ化
      結果を共有し、volatile 関数はキャッシュしない）
- [ ] `cte_inlining`（参照 1 回、または安価。派生スキャン葉（M4）待ち）
- [ ] `cte_materialization`（参照複数、または再帰。物理側は `materialize`
      実装規則まで接続済み。Cascades 側の CTE 葉は M4 待ち）
- [ ] `cte_filter_pushdown`
- [ ] `cte_predicate_propagation`
      （いずれも派生スキャン葉（M4）待ち。relational 維持）
- [x] `recursive_cte_union_rewrite`（`recursive_termination_predicate_pushdown`
      が価値の大きい場合（停止述語）を被覆。一般の和の書換えは不要のため
      この形で確定）
- [ ] `worktable_scan_indexing`（作業表 index 未対応のため将来枠）
- [x] `exists_short_circuit_limit_1`（非相関 EXISTS のキャッシュ結果と相関単一表
      probe 結果へ LIMIT 1 を適用し、EXISTS に不要な内部 ORDER BY も除去。
      明示 LIMIT 0 と集約の意味を保持。物理 scan の iterator early-stop は別 TODO）
- [ ] `any_all_quantified_comparison_rewrite`（配列 ANY/ALL は実行時
      `__quantified__` ヘルパーで被覆。副問い合わせ ANY/ALL は
      subquery_runtime 維持のため将来枠）
- [ ] `subquery_unnest_with_window`（禁止条件の明示。window migration 後に
      整理する将来枠）
- [ ] `common_subexpression_materialize`（同一 Scan/Join 部分木。scalar query の
      statement 内共有と相関 parameter cache は上記で実装済み。関係 CSE の
      materialize 配置は DOP 圧がない単一ノードでは利得が薄いため将来枠。
      物理側 `materialize` は接続済み）

---

## P3 — 集合演算

- [x] `union_all_merge`（連続 UNION ALL を n-ary）
- [x] `union_to_union_all_plus_distinct`
- [x] `intersect_to_semijoin`
- [x] `except_to_antijoin`
- [x] `setop_push_projection`（UNION / UNION ALL の枝へ分配）
- [x] `setop_push_filter`（全 set-op の枝へ述語を分配）
- [x] `union_all_push_limit`（有限 LIMIT の offset+count を各枝へ伝播し、
      親 LIMIT で全体結果を確定）
- [x] `union_distinct_hash_vs_sort`（`union_distinct_hash_sort_choice` として
      実装済み。コスト選択）
- [x] `cancel_union_empty` / `union_with_empty`（空枝を除去し、DISTINCT の重複排除を保持。
      上の重複行と同義。`setop_empty_simplification` / `setop_empty_identity`）
- [x] `intersect_with_empty` → empty（Cascades の `setop_empty_simplification` と回帰テスト）
- [x] `except_empty_right` → left（DISTINCT は重複排除を保持）
- [x] `setop_type_coercion_pushdown`（`SetOperationPlan` が出力スキーマへ数値共通型を
      反映し、枝側の型合わせは `setop_push_projection` との合成で等価）
- [ ] `merge_union_compatible_scans`（同一表の OR を 1 スキャンに。OR 述語は
      relational 経路に残るため、そちら側の将来枠）
- [ ] `partition_wise_union`（パーティションカタログがないため将来枠）

---

## P4 — Window / UNNEST / VALUES / 再帰

- [x] `split_window`（互換フレームごとに分割。`split_window` として実装。
      PARTITION BY 仕様ごとに関数群を同種 Window へ分離し積み上げ）
- [x] `merge_compatible_windows`（`merge_adjacent_windows` として実装済み）
- [x] `push_filter_through_window`（partition キー。`push_selection_through_window`
      として実装済み。非 window 列一般の push は意味論上不可（パーティション内
      集計が変わる）ため、partition 列＝安全部分集合の形で確定）
- [x] `push_limit_through_window`（適用不能として確定。Window は行保存演算子の
      ため LIMIT の押し下げは一般に非等価。no-op window は
      `no_op_window_elimination` で先に除去される）
- [x] `window_to_aggregate`（適用不能として確定。全フレーム集計への置換は行複製
      を伴わないと非等価（Window N 行 vs 集約 1 行）のため、単純置換の余地なし。
      一時集約＋cross 結合の形は将来の decorrelation 枠）
- [x] `rank_filter_to_topn`（`RANK() = 1` 等）
- [x] `row_number_filter_to_topn`
- [x] `eliminate_noop_window`
- [x] `window_prefix_sort_share`（`window_frame_sort_sharing` として実装済み）
- [x] `unnest_with_ordinality`（`WITH OFFSET` を列へ射影し、配列/相関 UNNEST の回帰あり）
- [x] `unnest_filter_pushdown`
- [ ] `unnest_to_join_with_values`（定数配列 UNNEST の Values 化は
      `array_flatten_rewrite` で被覆。一般形の join 化は relational 維持のため将来枠）
- [x] `array_flatten_rewrite`
- [x] `values_fold_into_union`（`values_fold_into_union` として実装。単一行分岐
      に限定し、複数行分岐の MergeAppend 選択を温存）
- [x] `constant_table_scan`（`constant_table` 実装規則まで接続済み）
- [x] `generate_series_to_values`（`generate_series` 実装規則が Values 化。
      論理の事前具体化は不要のためこの形で確定）
- [ ] `table_sample_bernoulli_vs_system`（TABLESAMPLE 構文がなく SQL 表面がない
      ため将来枠。enum `kSample` は予約）
- [x] `recursive_termination_predicate_push`

---

## P4 — 射影・式・CSE

- [x] `project_remove`（`RemoveIdentityProjection` / `ProjectionPlan` の列定義一致判定）
- [x] `project_to_scan`（列部分集合 + テーブル。scan_projections と各 scan の
      fallback projection へ接続）
- [x] `merge_calc`（方針確定：統合 Calc ノードを作らず Filter と Project を分離のまま。
      分離形が `merge_selections` / `merge_projections` / 各 pushdown の適用点を
      最大化し、物理も SelectionPlan＋ProjectionPlan の直結で等価）
- [x] `common_subexpression_elimination` in target list（relational projection が
      式の fingerprint ごとに `projection_cache` を共有）
- [x] `duplicate_column_elimination`
- [x] `unused_expression_pruning`
- [x] `constant_propagation_through_project`
- [ ] `predicate_push_into_project_expr`（式 index 照合が前提。式インデックスの
      カタログがないため将来枠）
- [x] `simplify_case_in_project`（スカラー `simplify_case` で被覆）
- [x] `boolean_project_used_as_filter` の引き上げ（派生表経由は
      `flatten_nested_subqueries` の再束縛でフィルタへ。単独形の余地なし）
- [x] `widen_project_for_join`（join 後に落とす列を一時保持。保守的デフォルト
      ＝証明済み不要列のみ刈る、で等価に実現）
- [x] `narrow_project_after_join`

---

## P5 — スカラー rewrite の追加（ExpressionRuleSet）

既存の畳み込みに加え、計画品質に効くもの。

- [x] `cast_simplify_numeric_widening`（`numeric_widening_cast` として実装済み。
      比較側への押し下げは `cast_pushdown_on_comparison` 論理規則で被覆）
- [x] `cast_date_timestamp_normalize`（同一ドメイン冗長 CAST の除去は
      `collapse_nested_identical_cast`、DATE→TIMESTAMP の比較押し下げは
      `cast_pushdown_on_comparison` で被覆。スキーマなしの単独正規化は
      損失・丸めの危険があるため行わない）
- [x] `coalesce_flatten` / `coalesce_of_coalesce`（nested `COALESCE` を引数列へ
      flatten。評価順を維持する rewrite 回帰付き）
- [x] `nullif_to_case`（副作用のない列＋リテラル形だけを CASE/IF へ変換）
- [x] `if_to_case`（3 引数 IF を短絡評価を保つ CASE へ変換） / [x] `case_to_if`（単一 WHEN の CASE を canonical IF へ変換し、往復を防止）
- [x] `greatest_least_fold`（全リテラルは既存の deterministic function fold、
      1 引数は恒等式へ縮約）
- [ ] `between_symmetric`（到達不能: ピン留め parser が SYMMETRIC 構文を拒否）
- [x] `is_distinct_from_rewrite`（定数側の `__is_distinct_from` を
      IS (NOT) NULL または OR/AND 比較へ正準化。三値論理回帰あり）
- [x] `boolean_eq_true_false_three_valued`（構文上の論理式だけを TRUE/FALSE
      リテラル比較から恒等式/NOT へ変換し、NULL は UNKNOWN のまま保持。
      一般ブール式版は `canonicalize_boolean` で被覆）
- [x] `and_true_elim` / `or_false_elim` の NULL 厳密化監査
      （`boolean_identity` が三値論理で健全であることを確定。
      `x AND TRUE ≡ x`、`x OR FALSE ≡ x` は NULL でも一致）
- [x] `distribute_or_over_and`（選択的、爆発抑制。
      `distribute_or_over_and_budgeted` として実装。片側単一・項数 ≤4 に制限）
- [ ] `distribute_and_over_or`（意図的に非実装：`factor_or_common_and` /
      `boolean_filter_pullup` と振動し、`ComplementaryAbsorptionDisabled` の
      pin 形状を崩す。DNF 方向が必要な箇所は De Morgan 双対で個別対応）
- [x] `cnf_conversion_budgeted`（`distribute_or_over_and_budgeted` の fixpoint が
      予算付き CNF 化として機能）
- [ ] `dnf_conversion_budgeted`（同上、DNF 方向は非実装。bitmap OR 等の選言
      sarg は実装規則側の `or_ranges` 抽出で被覆）
- [x] `extract_disjunctive_sarg`（選言の範囲抽出は index 実装規則の bitmap OR
      経路で被覆）
- [x] `in_empty_list` → false / unknown（空リストは FALSE、NULL 要素だけの
      1 要素リストは UNKNOWN に固定）
- [x] `in_single_null`
- [x] `not_in_with_null_list` 警告と計画（`not_in_null_semantics` として実装済み）
- [ ] `like_escape_normalize`（到達不能: ピン留め parser が ESCAPE 句を拒否）
- [ ] `ilike_to_lower_like`（到達不能: ピン留め parser が ILIKE を拒否）
- [ ] `similiar_to_to_regex`（到達不能: SIMILAR TO 表面がなく parser 未対応）
- [x] `date_add_sub_fold`（決定的なリテラル関数折りたたみで DATE_ADD/SUB を定数化）
- [x] `interval_normalize`（`interval_normalize` として実装済み）
- [x] `concat_flatten`（評価順を保ったまま nested CONCAT の引数を平坦化）
- [x] `substring_constant_fold` 強化（決定的なリテラル関数折りたたみを回帰）
- [x] `json_path_constant_fold`（`json_path_constant_fold` として実装済み）
- [x] `array_constructor_fold`（定数要素の `ArrayExpression` を `fold_array` で
      配列 Value へ畳み込み、rewrite 回帰を追加）
- [x] `array_length_zero`（`ARRAY_LENGTH` を AST 評価パスの
      `ExecuteFunction` に実装し、決定論的定数畳み込み `fold_function` が
      リテラル配列の長さを rewrite 時に確定。`=0` 比較はブール定数まで縮約）
- [x] `safe_divide_rewrite`（定数 0 除数は NULL へ定数化。非定数除数は
      オーバーフロー意味論維持のため保持）
- [x] `abs_of_abs`（`ABS(ABS(x))` を安定な関数形に縮約）
- [x] `log_identities`（`pow_identities`（`pow(x,1)→x`、`pow(x,0)→1）＋定数畳み込み
      で被覆。LOG 恒等式は有効な底の証明が必要で、底が定数なら fold が畳むため
      追加規則の余地なし）
- [x] `comparison_of_same_expr`（安定した列参照に限定して `x=x` → `x IS NOT NULL`。
      `comparison_self_predicates` 論理規則として実装。NaN のため FLOAT 除外を
      カタログドメインでゲート）
- [x] `self_inequality`（`x<x` / `x>x` を NULL 保持 CASE へ変換、三値論理テストあり。
      同上規則で `<=` / `>=` 含め被覆）
- [x] `deterministic_function_cse`（target list の `projection_cache` が
      ToString fingerprint ごとに値を共有 = [`common_subexpression_elimination`] 済み。
      単一式木内の DAG 化は未導入だが実用上の重複評価は発生しない）
- [x] `nondeterministic_barrier`（時刻・乱数・UUID 系を定数畳み込みから除外）
- [x] `stable_vs_immutable` 分類（`function_volatility_classification` として実装済み）
- [x] `rewrite_or_of_ranges_to_in`（`or_of_ranges_to_in` として実装済み）
- [x] `extract_year_sargable`（DATE 列限定でプランニング層が
      `EXTRACT(YEAR FROM col) <op> 年定数` を半開区間へ書き換え。
      残余 WHERE は元述語のまま二重評価で安全性担保。TIMESTAMP / VARCHAR は
      意味論変更のため対象外）
- [x] `not_between_to_or`（既存の De Morgan / 比較否定書き換えで実装済み、NULL 回帰テストあり）
- [x] `xor_to_or_and_not`（SQL 三値論理を保つ `(a OR b) AND NOT(a AND b)`）
- [x] `bit_and_or_identities`（`bit_and_or_identities` として実装。
      `__bit_and` / `__bit_or` は到達可能（既存の `bit_and_zero` 等が対象に
      している内部関数）。`x&x→x`・`x|x→x`・`x&-1`・`x|-1` を INT64 に限定）

---

## P5 — 統計・選択性・コスト（規則そのものではないが必須）

- [x] 列ヒストグラム（等幅 / 等高。16 equi-depth バケット＋`EstimateRange` まで接続）
- [x] MCV（most common values。5 MCV＋`EstimateEqual` まで接続）
- [x] NDV スケッチ（HyperLogLog。`hyper_log_log` まで接続）
- [x] NULL 比率の独立管理（NULL/non-NULL/distinct の独立カウント＋IS NULL 選択性）
- [x] 多列相関統計 / 関数従属（`EstimateMultiColumnSelectivity` の相関因子）
- [ ] join クロス列 NDV（列間 NDV の同時分布がなく、現状は max-NDV 除算で近似。将来枠）
- [ ] 式統計（`f(col)` の NDV。式 index とセットの将来枠）
- [x] LIKE / 正規表現の選択性モデル
- [x] OR の包含・除外（`ReductionFactor` の AND/OR/XOR/IN/IS-NULL 確率規則で被覆）
- [x] 範囲述語の区間演算（`EstimateRange` の区間演算で被覆）
- [x] 結合カーディナリティの histogram join
- [x] 半結合の上界 `min(|L|,|R|)` と反結合の probe-side 上界を
      物理候補／`ProductPlan` の見積りへ反映（FK-PK inner は片側 unique として
      `EstimateJoinCardinality` が子側行数に自動で丸めるため被覆）
- [ ] skew 補正（Zipf。歪み統計がなく一様仮定のため将来枠）
- [ ] 動的サンプリング（実行前サンプルスキャン。計画時追加スキャンの基盤がないため将来枠）
- [x] フィードバック最適化（前回実行の実カーディナリティ。EXPLAIN ANALYZE の
      実測＋`SqlOracleFuzzer.PlanFeedbackObservesPlans` まで接続。自動再計画は将来枠）
- [x] コスト単位の監査（I/O vs CPU vs メモリ。D3 で行アクセス単位に確定。
      spill 罰則・ソート N log N・ハイブリッド切替で three-way を近似）
- [x] メモリ許可量と spill 確率
- [x] parallel speedup モデル（`parallel_thresholds` による実行器内並列の
      しきい値駆動。Cascades の明示 DOP 配分は分散枠に残る）
- [ ] index クラスタリング因子（クラスタリング統計のカタログがないため将来枠）
- [ ] ページキャッシュヒット率（バッファプール統計の露出がないため将来枠）
- [x] PAX / zone map 選択性（zone map は `scan_zone_map_filter_integration`＋
      実行器で被覆。PAX 列形式自体が未実装のため対象外）
- [x] Top-N の `limit_hint` を全実装規則へ（順序提供スキャンが適用。
      hash 系は全入力消費のため早期停止不能で対象外。範囲はこの形で確定）
- [x] `EXPLAIN` に見積 vs 実測を出す（ANALYZE は一部既存。見積は Plan の
      estimated cost 表示＋`dump_memo` の適用規則・選択計画で被覆）

---

## P6 — 探索・メモ・実装規則の制御

- [x] ブランチ・アンド・バウンドのコスト上界（`OptimizeGroup` が子コスト
      時点で incumbent を上回る式を枝刈り。local cost 非負のため健全）
- [x] 興味深い順序（interesting orders）の完全伝播（`RequiredChildProperties`
      の導出＋merge 系の子 Sort 補完＋`limit_hint` の Top-K 伝播で被覆。
      列挙型の要求順序展開は単一ノードではこの形で確定）
- [x] 興味深い分割（interesting partitioning）（`partition_by` プロパティ＋
      単一ノード colocated 自明性で被覆。分散分割は P8 枠）
- [x] 物理規則 `sort` / `merge_join` / `sort_agg` / `hash_agg` の分離
      （`sort`・`merge_join`・`aggregation`（Hash/Sort/Stream の3代替）で分離済み）
- [x] `stream_agg` 実装規則（`aggregation` 規則の第三代替 `StreamAggregatePlan`）
- [x] `hash_distinct` / `sort_distinct`
- [ ] `window_sort` / `window_hash_partition`（window 実行器がなく relational
      `window_eval` のため将来枠。window migration とセット）
- [ ] `set_union_hash` / `set_union_sort`（`SetOperationPlan` 単一実装のため
      将来枠。現状 MergeAppend 選択はある）
- [x] `materialize` 実装規則（`materialize`＋`MaterializePlan` まで接続）
- [x] `spool` 実装規則（`eager_spool` / `lazy_spool`。同上実装を共有し
      ヒント用に分離）
- [x] `exchange` 実装規則（単一ノードでは no-op。`exchange_noop` 等4規則）
- [x] 規則の優先度 / 重み（登録順＝適用順で確定。探索は fixpoint 完備のため
      順序は速度にのみ影響）
- [x] 探索タイムアウトと best-so-far（`OptimizerOptions::search_step_budget`＋
      `SearchEngine::BudgetExhausted`。枯渇時は best-so-far を返し
      `dump_memo` に記録）
- [x] クエリヒント（join order freeze、pg_hint_plan 相当。`disabled_implementation_rules`
      ＋`extra_implementation_rules`＋`access_method`＋`RuleSet::Remove` による
      join 列挙凍結で等価に実現。専用構文は将来枠）
- [x] ルールセットのワークロード別プロファイル（同上メカニズムで構成可能。
      プリセットの配布は将来枠）
- [ ] `join_enumeration` を連結部分グラフ列挙に置換（dphyp。結果等価で速度のみ
      のため性能枠）
- [x] グループ表現キャップの適応（4096＋`Degraded()` の優雅な縮退で確定。
      `search_step_budget` が労力側の適応も担う）
- [x] memo dump の diff テスト拡充（optimizer_improvements Phase 9。
      `plan_memo_oracle`＋`dump_memo` の適用規則記録＋`ExploreTwiceReachesSameFixpoint`
      で被覆）
- [ ] 計画キャッシュとパラメータスニッフィング対策（プリペアド方式が Simple Query
      専用のため表面がなく将来枠。`plan_cache.hpp` は query 側の器）
- [ ] 汎用計画 vs カスタム計画（同上、プロトコル制約のため将来枠）
- [x] 並列度決定（DOP。`kParallelScanMinRows` /
      `kParallelAggregationMinRows` のしきい値駆動で確定。明示 DOP 配分は分散枠）
- [x] バッチサイズ / モーセルサイズのコスト化（8 pages/morsel 固定＋
      `kDefaultVectorSize` 固定で確定。可変化は将来枠）

---

## P6 — 物理実装規則（実行器と対）

各項目の実行器は [`executor_todo.md`](executor_todo.md)。

- [x] `merge_join`（等価、両側ソート済み）
- [x] `sort_merge_join`（必要なら子に SortPlan を要求）
- [x] `outer_hash_join` / `left` / `right` / `full`（`JoinKind` と実装規則を接続）
- [x] `outer_merge_join`（`outer_hash_join` 規則が等価述語の merge alternative と
      子 SortPlan を併せて生成。`MergeJoinPlan` の LEFT/RIGHT/FULL と同じ NULL
      padding 契約を共有）
- [x] `outer_nested_loop`（`outer_nested_loop` として実装。LEFT 非等価結合を
      null-pad 付き `NestedLoopJoin` へ。RIGHT は正規化で LEFT 経由、FULL は
      hash/merge 維持）
- [x] `semi_hash_join` / `anti_hash_join`（等価キー・残余なし。既存
      `HashJoin` の `JoinKind` へ接続）
- [x] `semi_merge_join` / `anti_merge_join`（ソート子の自動挿入と probe 側スキーマを
      保持する MergeJoinPlan）
- [x] `index_only_agg`（非NULLな単一キー列のMIN/MAX）
- [x] `bitmap_scan` / `bitmap_and` / `bitmap_or`（`index_scan` 規則内の
      `BitmapScanPlan` AND/OR 経路として実装済み）
- [ ] `tid_scan` / `rowid_lookup`（`TidScan` 実行器はあるが rowid SQL 表面がなく
      DML 経路専用。Cascades 配線は DML 計画とセットで将来対応）
- [x] `parallel_seq_scan` を Cascades から明示選択（統計閾値と回帰テスト）
- [x] `pax_scan` / `zone_map_skip`（zone map は `scan_zone_map_filter_integration`
      ＋実行器で被覆。PAX 列形式自体が未実装のため対象外）
- [x] `skip_scan`（複合インデックスの先頭欠落。`IndexSkipScanPlan`＋
      `skip_scan_distinct` 経路として実装済み）
- [x] `index_skip_scan_for_distinct`（同上）
- [x] `covering_index_rewrite`（`index_scan` 規則が covering 時に
      `IndexOnlyScanPlan` を提示）
- [x] `sort_agg` / `hash_agg` の選択（`aggregation` 規則が Hash/Sort/Stream の
      3代替をコスト付きで提示。`SortAggregatePlan` まで接続）
- [x] `parallel_hash_agg` / `parallel_stream_agg`（`kParallelAggregationMinRows`
      のしきい値駆動で実行器が選択。Cascades の明示分岐は不要のため確定）
- [x] `topn_heap`（TopN implementation rule emits `TopNPlan` and preserves
      required NULL placement）
- [ ] `window_agg`（window 実行器がなく relational `window_eval` のため将来枠。
      window migration とセット）
- [ ] `setop_hash` / `setop_sort`（`SetOperationPlan` 単一実装（MergeAppend 選択
      あり）のため将来枠）
- [x] `unnest_exec`（現行の relational fallback で `Unnest` / OFFSET を実行。
      Cascades 側 `unnest` 実装規則あり）
- [x] `recursive_union`（`recursive_cte` 実装規則＋`ExecuteRecursiveCte` で被覆。
      和の分別計画は不要のため確定）
- [ ] `insert` / `update` / `delete` / `upsert` の DML 計画（DML 論理ノード自体が
      なく relational 実行。`INSERT SELECT` の投影写像のみ接続済み。DML ノード
      導入時に追加）
- [ ] `on_conflict` / `returning`（parser 表面がなく到達不能。構文追加時に DML 計画とセット）
- [ ] `lock_rows`（SELECT FOR UPDATE）計画（同上、表面なし）

---

## P7 — 整合性制約・カタログ駆動

- [x] PK / UNIQUE による distinct 除去（`pk_unique_distinct_elimination` として実装済み）
- [x] NOT NULL による `IS NOT NULL` 除去（`not_null_is_not_null_elimination`
      として実装済み）
- [x] CHECK 制約の述語取り込み（`check_constraint_predicate_intake` として実装済み。
      矛盾検出の範囲は `>= 0` / `> 0` 系に限定。一般化は `kCheck` 制約自体が DDL
      未実装（"Won't implemented"）のためカタログ待ち）
- [x] FK による join 除去とカーディナリティ上界（multiplicity 保存形の
      `fk_join_elimination`・`unused_join_elimination`＋片側 unique 時の
      `EstimateJoinCardinality` 上界で被覆。完全除去は `kForeign` カタログ待ち）
- [ ] パーティション制約による枝刈り（constraint exclusion。パーティション
      カタログがないため将来枠）
- [ ] 生成列の照合（生成列の表面・カタログがなく将来枠）
- [ ] 部分インデックス照合（部分インデックスのカタログがなく将来枠）
- [ ] 式インデックス照合（同上）
- [ ] 照合順序（collation）伝播（照合順序のモデル・表面がなく将来枠。
      `PhysicalProperties::collation` の器のみ予約）
- [x] ビュー展開（predicate を残したまま。TEMP ビューは parse 時に
      `ViewRegistry` 経由で展開され、展開後の述語に通常の pushdown が効く）
- [ ] セキュリティバリアビュー（pushdown 制限。セキュリティビューの器がなく将来枠）
- [ ] RLS 述語の挿入位置（RLS 基盤がなく将来枠）

---

## P7 — 特殊 SQL 形

- [ ] `LATERAL` のコスト付き展開（派生スキャン葉（M5）待ち。relational 維持）
- [ ] `UNNEST` + JOIN の順序（UNNEST が join グラフ外のため将来枠。relational 維持）
- [x] `PIVOT` / `UNPIVOT` 展開（parse 時に `ExpandPivotSource` で join/agg へ展開。
      オプティマイザ側の追加規則は不要のため確定）
- [ ] `MATCH_RECOGNIZE`（将来。行パターン実行器がなく将来枠）
- [ ] `TABLESAMPLE`（構文がなく SQL 表面なし。`kSample` enum 予約のみ）
- [ ] `QUALIFY`（window filter。relational で実行。window の Cascades migration
      後に Selection-over-Window として移管する将来枠）
- [x] `GROUP BY ALL` / `GROUP BY DISTINCT`（ALL は parse 時に明示キーへ展開、
      DISTINCT は重複除去意味が GROUP BY と一致するためそのまま実行。
      `GroupByAllAndDistinctExecution` で回帰。専用規則の余地なし）
- [ ] `SELECT DISTINCT ON`（PostgreSQL。relational で実行（`DistinctOf`）。
      Cascades 移管はソート＋先頭行保持形の設計後に将来対応）
- [x] `FETCH FIRST … WITH TIES`（`TopNPlan` が `with_ties` を実行し、論理
      `TopN` ペイロード＋実装透過まで接続。`complex_` 問い合わせは当面
      relational 経路に残り、移管後に Cascades が担う）
- [ ] `FOR UPDATE` / `SKIP LOCKED` とアクセスパス制約（parser 表面がなく到達不能）
- [x] `INSERT SELECT` の投影（SQL 実行経路で SELECT の投影結果を target
      列順へ写像して Insert source に接続。回帰は
      `SqlEngineInsertSelectCopiesAndMapsRows`。DML を Cascades で直接最適化する
      並列化は DML 論理ノード導入時に追加）
- [ ] `MERGE` 文の join 計画（MERGE 構文がなく到達不能。`MergeExecutor` の器は
      executor 側にあるが表面なし）
- [ ] `UPDATE … FROM` / `DELETE … USING`（構文がなく到達不能）
- [ ] `RETURNING` の投影（同上）
- [ ] 準備文の generic/custom（Simple Query 専用プロトコルのため表面なし。
      `plan_cache.hpp` は query 側の器）
- [x] バッチ INSERT の値リスト結合（複数行 VALUES は単一 `Values` ノード。
      UNION 側は `values_fold_into_union` で被覆）

---

## P8 — 分散・並列（単一ノードでもプロパティだけ先に）

- [x] 分配: any / singleton / hash / broadcast / range（`Distribution` の器＋
      単一ノードでは全分配が自明に充足。詳細分割は分散枠）
- [x] `enforce_distribution` 実装規則（`exchange_noop` 等の no-op 群で等価に実現）
- [x] colocated join 検出（単一ノードでは全 join が colocated のため自明。分散枠で再訪）
- [x] broadcast vs shuffle のコスト（単一ノードでは等価のため確定）
- [x] partial agg + finalize（UNION 分岐の `aggregate_union_transpose`＋
      `PartialAggregate` 実行器で被覆。並列 partial はしきい値駆動）
- [x] two-phase distinct（`TwoPhaseDistinctAggExecutor`＋`count_distinct_expansion`
      の二重 agg で被覆。Cascades の明示二相分岐は不要のため確定）
- [ ] ウィンドウの partition 再分散（window 実行器がなく将来枠）
- [x] リミットの部分リミット + merge（UNION 分岐の `union_all_push_limit` で被覆。
      並列 partial limit はしきい値駆動の将来枠）
- [x] スキャンのレンジ分割（8 pages/morsel の morsel 駆動で被覆）
- [ ] パイプライン並列 vs 交換並列（分散枠）

---

## P8 — 検証・安全網

- [x] 規則ごとの代数テスト（前後の結果集合。`algebra_test`＋各規則の
      `cascades_test` 回帰（等価・反例の両方向）で被覆）
- [x] fuzz: ランダム規則サブセット（物理は既存、論理も。`rule_fuzzer`＋
      `JoinChainMatchesGoldenResultUnderRuleSubsets` の論理・物理スイープで被覆）
- [ ] 外側結合の null-pad ゴールデン（inner 系は上記スイープで被覆。outer の
      行集合ゴールデンは window/CTE 移管後の Phase 9 残り）
- [ ] TPC-H Q8/Q9 の join order ゴールデン（Phase 9 残り。outer/CTE を含むため
      移管後に回収）
- [x] 差分: 関係エンジン経路 vs Cascades 経路（`OptimizerAndRelationalPathsAgree`
      監査テストで被覆）
- [x] コスト単調性の近似チェック（非負コスト＋B&B の incumbent 単調性＋全スイープの
      行等価で被覆。厳密単調性の形式的検証は将来枠）
- [x] 無限ループ検出（既に fingerprint。`ExploreTwiceReachesSameFixpoint` の
      fixpoint 冪等回帰＋規則追加時の回帰で被覆）
- [x] EXPLAIN に適用された規則名を出す（`SearchEngine::AppliedRuleNames`＋
      `dump_memo` 記録で被覆）

---

## 推奨実装順（最初のスライス：すべて到達済み）

1. 等価クラスの一般化（`infer_join_predicates` 拡張）と矛盾 Filter。→ 到達
2. 列刈り込み / identity projection。→ 到達（identity の Cascades 除去は
   別名破壊のため意図的保留、物理判定で代替）
3. `Sort` 論理ノード + Top-N + インデックス順序の接続。→ 到達
4. LEFT JOIN の論理ノード + null-rejecting pushdown。→ 到達
5. Merge Join 物理（ソート興味順序とセット）。→ 到達
6. Mark/Single Join と一般相関 APPLY。→ 到達
7. Sort-Agg vs Hash-Agg の選択。→ 到達（Stream 含む3択）
8. サブクエリ decorrelation。→ 到達（型付き lowering＋実行時キャッシュ）
9. UNION / Window。→ 到達（論理規則まで。window 物理は migration 枠）
10. 統計ヒストグラム。→ 到達

残件はすべて「構文・カタログ・分散・プロトコルの表面待ち」か「意図的非実装」
（理由を各項に明記）に整理済み。表面が追加されたら対応する `[ ]` 項から着手する。
