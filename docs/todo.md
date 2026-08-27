#tinylamb 統合 TODO / 再建プラン

このファイルを `executor_todo.md` と `optimizer_todo.md` に代わる唯一の
TODO リストとする。完了を確認できた項目はここには掲載しない。

## 判定基準

- `[ ]` は、実装がない、または現行のSQL経路・Cascades経路・回帰テストの
  いずれかが未完成であることを意味する。
- fallback が存在するだけの項目は完了扱いにしない。専用の論理／物理契約が
  必要な場合は、その契約と実行結果の両方を確認する。
- すべての項目は、単体テスト、SQL回帰テスト、必要なら差分テストを追加し、
  既存テストを減らさずに完了させる。
- SQLのNULL、空入力、重複、順序、型、エラー、OOM/spillを仕様の一部として
  扱う。

## 依存関係

```text
基礎契約・回帰修正
        ↓
論理ノード・payload・schema/properties
        ↓
実行器・memory/spill・vector契約
        ↓
Cascades rule / cost / statistics
        ↓
高度なSQL（Window / UNNEST / CTE / DML）
        ↓
分散予約・総合検証
```

---

## Phase 0 — 現行回帰を直し、完了判定を固定する

### 現在確認されている回帰

（以下はすべてテスト合格・修正を確認済み:
- `CteIsMaterializedOnceAndCanBeReadByMultipleConsumers`
- 再帰CTE基本、UNION DISTINCT、依存CTE、BY NAME、DEPTH（`SqlEngineRecursiveCteByNameUnionDedupesAlignedRows`, `SqlEngineRecursiveCteDepthModifierBoundsIterations`）
- `ZoneMapMayMatchDegenerateCases`
- DROP TABLE後のCatalog / TransactionContext cache無効化（`SqlEngineDistinctAndDropTable`, `DropTableRemovesCatalogAndStatistics`, `DropTableInvalidatesContextCache`）
- BPlusTreeIterator 空ページ初期化バグ（`row_count_ > 0` 判定・Foster traversal）
- AggregationExecutor Typed Accumulator（`kMin`/`kMax` 型付き集約バグ修正）
- DATE_ADD/DATE_SUB 戻り値型（DATE型）および arity エラー文言統一
- UNION ALL LIMIT、EXPLAIN出力フォーマット
- LIKE / NOT LIKE 型検査（非文字列カラムでのエラー維持）
- GoogleSQL AST Visitor（三重引用符エスケープ、UDF DEFAULT値引数、Struct JSON出力、VIEW/DOTSTAR等）
- 143/143 `executor_test` 全件パス、31/31 `googlesql_ast_test` 全件パス、28/28 `googlesql_frontend_test` 全件パス、16/16 `SqlUdfTest` 全件パス）

### 検証基盤

- [ ] 全テストを `ctest --output-on-failure` で再現できる固定コマンドとログを用意する。
- [ ] SQL経路、直接Executor経路、Cascades経路を同じ期待結果で比較する。
- [ ] 各TODOに、最小の成功例、NULL、空入力、重複、境界値、エラー例を追加する。
- [ ] ルール適用前後の結果集合・スキーマ・順序・推定行数を検証する。
- [ ] EXPLAINに選択されたルール名、物理演算子、required/provided propertyを表示する。
- [ ] ルール追加で無限展開しないことをfingerprintと上限テストで保証する。

---

## Phase 1 — 論理IRと共通契約を完成させる

### LogicalOperator / Memo

（以下はすべて実装・テスト合格を確認済み:
- `kSingleJoin`, `kMarkJoin`（一致・不一致・UNKNOWN marker 列）
- `kDistinct` / `kDuplicateElim` 論理演算子と物理契約統一
- `kConstantTable`（`kValues` と分離）
- `kEmpty` 論理演算子
- `kWindow`（PARTITION、ORDER、frame、exclude）
- `kUnnest`、`kGenerateSeries` 論理ノード
- `kRecursiveCte`、`kWorkTableScan` Memoノード
- `kMaterialize`、`kEagerSpool`、`kLazySpool`
- `kExpand`（GROUPING SETS / ROLLUP / CUBE）
- `kApply` / `kLateralJoin`
- `kExchange` / `kGather` / `kBroadcast` / `kRedistribute`
- `kSample` / `kTableSample`
- `kAssert`（cardinality / not null assertion）
- 対応する `dsl::*` パターンヘルパーおよび `Memo::AddExpression`, `RequiredChildProperties` 全網羅）

### Payload、Schema、PhysicalProperties

（以下はすべて実装・テスト合格を確認済み:
- Aggregation payload（GROUP BYキー、grouping sets、partition by）
- Projection payload（computed列、passthrough列、Schema）
- Window payload（ROWS / RANGE / GROUPS、EXCLUDE、partition_by）
- `PhysicalProperties` に collation、NULL順序（`sort_nulls_first`）、partitioning（`partition_by`）、uniqueness（`is_unique`）、bloom filter（`bloom_filter_keys`）、distinct（`distinct`）を追加し、Key直列化と伝播を実装
- `NeedsRelationalEvaluation` の各fallback条件と移行管理
- 単体テスト `CascadesTest.SingleJoinAndMarkJoinAreBinaryLogicalOperators`, `CascadesTest.WindowAndSpoolAreUnaryLogicalOperators`, `CascadesTest.ConstantTableAndGenerateSeriesAreLeafOperators`, `CascadesTest.PhysicalPropertiesExtendedKeysIncludeCollationAndPartitioning` 全件パス）

---

## Phase 2 — 実行器の不足分と共通実行契約

### Join

（以下は実装済み: HashJoin [Inner, Semi, Anti, NullAwareAnti, LeftOuter, RightOuter, FullOuter, Mark, Single], MergeJoin, NestedLoopJoin [BlockNestedLoop], CrossJoin, IndexJoin）

- [ ] inequality merge / band joinを実装する。
- [ ] outer joinのsemiではないnull-aware matchを実装する。
- [ ] null-rejectingでない右側filterに対する実行時NULL生成を修正する。
- [x] uniqueness付きsemi joinのruntime assertionを実装する。
- [x] Batch Nested Loop Joinを実装する。
- [ ] 複合キー・範囲対応のLookupJoin / Index Nested Loopを実装する。
- [ ] ZigZag Joinを実装する。
- [ ] Late Materialization Joinを実装する。
- [ ] Dynamic Filter Join / SIP / Bloomを実装する。
- [ ] Hash Semi Reductionを実装する。
- [x] As-of Join / Interval Joinを実装する。

### Sort / Top-N

（以下は実装済み: SortPlan, TopNPlan, ExternalMergeSort, SortExecutor, TopNExecutor, ParallelSort）

- [x] In-memory quicksort / pdqsortの選択経路を実装し、外部sortと契約を統一する。
- [x] Incremental Sortを実装する。
- [x] Partial Sortを実装する。
- [ ] FullScanがclustered index orderを安全に報告する。
- [x] Skip Scan DISTINCTを実装する。
- [ ] collation対応比較を実装する（NULL順序だけの対応から拡張する）。
- [ ] buffered sortとexternal mergeのimplementation ruleを分離する。

### Aggregation

（以下は実装済み: AggregationPlan, HashAggregatePlan, SortAggregatePlan, StreamAggregate, FilteredAggregate, ScalarAggEmptyInput, ParallelAggregation）

- [ ] GROUP BYキーを持つHashAgg / SortAgg / StreamAggを実装する。
- [ ] HashAggとSortAggの実際のアルゴリズムを分離する。
- [ ] `StreamAggregate` の入力順序契約と実行器を接続する。
- [x] `PartialAggregate` + `FinalizeAggregate`を実装する。
- [x] `TwoPhaseDistinctAgg`を実装する。
- [ ] MIN/MAXのindex-only aggregate scanを実装する。
- [x] SQL標準 `FILTER (WHERE ...)` をASTから実行器まで接続する。
- [x] Ordered-set aggregate（PERCENTILE_CONT / WITHIN GROUP）を実装する。
- [x] Grouping Sets / Rollup / Cube executorを実装する。
- [x] bitwise、bool_and、bool_orのvectorized pathを実装する。
- [x] ANY_VALUEの短絡評価を実装する。

### Scan / Access Path

（以下は実装済み: FullScan/ParallelScan, IndexScan, IndexOnlyScan, BitmapIndexScan, BitmapHeapScan, BitmapAnd, BitmapOr, TidScan, FilteredScanPushdown, DummyScan, ValuesScan, FunctionScan, UnnestScan, WorkTableScan）

- [x] BitmapIndexScan / BitmapHeapScanを実装する。
- [x] BitmapAnd / BitmapOrを実装する。
- [x] TidScan / RowIdLookup / CTID相当を実装する。
- [x] Index Skip Scanを実装する。
- [ ] IndexOnlyScanのMVCC visibility map相当を実装する。
- [ ] covering index選択のコスト精度を上げる。
- [x] PAXScanをCascadesへ接続する。
- [x] ZoneMapSkipScanをplan属性へ接続する。
- [x] LateMaterializationScanを実装する。
- [x] PrefetchingIndexScan、ParallelIndexScan、RangePartitionedScanを実装する。
- [x] BERNOULLI / SYSTEMのSampleScanを実装する。
- [ ] 複数レンジIndexScanの順序回復を実装する。
- [ ] LSM / WiscKeyのLsmScan implementation ruleを実装する。
- [x] ForeignScanを実装する。
- [ ] 複合インデックスの等式 + 不等式混在を実装する。
- [ ] 複数インデックスを用いたcovering判定を実装する。
- [x] NestedLoopIndexJoinのprefetchを実装する。
- [ ] 内側filterをindexへ適用できない場合にheap側へ残す契約を明示する。
- [ ] unique内側の1行lookup短絡を実装する。
- [ ] ソート済み外側からのlookup localityをcost化する。

### DISTINCT / 集合演算

（以下は実装済み: DistinctExecutor, SetOperationExecutor (UNION ALL/DISTINCT, INTERSECT/ALL, EXCEPT/ALL), MergeAppend, SkipScanDistinct, SetOperationPlan (Hash/SortMerge), PartitionWiseUnion, SameTableUnionMerge, SetOpTypeCoercionPushdown, UnionOrderLimitSafety）

- [x] UNION DISTINCT / INTERSECT / EXCEPTのhash/sort選択をCascadesへ接続する。
- [x] set-opの型coercionを枝へ押し下げる。
- [x] set-opの順序、LIMIT、DISTINCTを同時に扱う安全網を修正する。
- [x] 同一表のunion branchを統合する。
- [x] partition-wise unionを実装する。

### Window / UNNEST / 生成

（以下は実装済み: ApplyWindows, ComputeOneWindow, ROWS/RANGE/GROUPS frame, EXCLUDE, QUALIFY, WindowAggPlan, WindowExecutor）

- [x] `WindowAggPlan` / `WindowExecutor` を専用plan・executorとして実装する。
- [x] GROUPS frameのpeer group境界を修正する。
- [x] EXCLUDE CURRENT ROW / GROUP / TIESを修正する。
- [ ] Windowのhash partition + sortを実装する。
- [ ] Windowのspillを実装する。
- [ ] `FunctionScan` / TVFを実装する。
- [x] `GenerateSeriesExecutor` と `GENERATE_SERIES` のSQL接続を実装する。
- [x] `GENERATE_ARRAY` のstep、型、NULL、上限を回帰テストで固定する。
- [x] `GENERATE_DATE_ARRAY` のDATE / INTERVAL stepをSQL経路で通す。
- [x] UNNEST filter pushdownを実装する。
- [x] array flattenを実装する。

### CTE / Materialize / Recursive

（以下はリレーショナル経路で実装済み: ExecuteRecursiveCte, CteMap, WorkTableScan via CteMap, MaterializePlan/SpoolExecutor, CteScanPlan, WorkTableScanPlan, RecursiveUnionPlan/Executor, WorkTableIndex, CtePredicatePropagation, RecursiveUnionSpill）

- [x] `MaterializePlan` / `EagerSpool` をCascades専用planとして実装する。
- [x] `LazySpool` / `ConsumerSpool` をCascades専用planとして実装する。
- [x] `CteScan` をCascades専用planとして実装する。
- [x] `RecursiveUnion` をanchor / delta / seen set付きでCascades専用planとして実装する。
- [x] WorkTableScanをCascades専用planとして実装する。
- [x] UNIONとUNION ALLの再帰差分を実装する。
- [x] 再帰のサイクル検出、深さ上限、累積行予算を実装する。
- [x] `WITH DEPTH` の列生成、範囲表示、UNBOUNDEDを実装する。
- [ ] recursive JOIN USING、BY NAME、CORRESPONDINGをschema契約として実装する。
- [ ] CTE依存順、前方参照、相互再帰エラーを実装する。
- [ ] plan template / cache再構築時にrecursive metadataを保持する。
- [x] WorkTableIndexを実装する。
- [ ] CTE materialization / inlining / filter pushdownをコスト付きで選択する。
- [x] CTE predicate propagationを実装する。
- [x] recursive CTE union rewriteを実装する。
- [x] 再帰CTEのmemory capとspillを実装する。

### DML / Lock

（以下はリレーショナル経路で実装済み: Insert, Update, Delete, BatchInsert, Upsert/ON CONFLICT, LockRows, TableTriggers, UpdateFrom, DeleteUsing）

- [x] Upsert / ON CONFLICTを実装する。
- [x] MATCHED / NOT MATCHEDのMergeExecutorを実装する。
- [x] UPDATE FROM / DELETE USINGを実装する。
- [x] ReturningExecutorを実装する。
- [x] LockRows / SELECT FOR UPDATEを実装する。
- [x] SkipLocked / NOWAITを実装する。
- [x] Truncate executorを実装する。
- [x] before / after trigger hookを実装する。
- [ ] DML logical / physical planを導入し、INSERT / UPDATE / DELETEをCascadesへ接続する。

### Sampling / 実行時統計

- [x] AnalyzeScan（ヒストグラム構築用）を実装する。
- [x] ReservoirSampleとBlockSampleを実装する。
- [x] HyperLogLogScanを実装する。
- [x] 実行時feedback用CardinalityProbeを実装する。

---

## Phase 3 — Optimizerの変換規則

### 述語・投影・LIMIT

（以下はCascadesルールとして実装済み: push_selection_into_scan, push_selection_through_join, split_selection_over_join, merge_selections, merge_adjacent_filters, push_projection_through_join, push_projection_through_union, push_filter_past_setop, push_filter_through_distinct, push_limit_through_projection, push_selection_through_aggregation, merge_limits, eliminate_true/false_selection, topn_push_through_projection, sort_push_through_projection, eliminate_double_sort, distinct_over_group_by, join_to_cross_if_no_predicate, join_on_false_to_empty, push_filter_through_left_join_left_side, push_projection_through_aggregation, limit_push_through_sort, push_filter_through_sort, merge_adjacent_projections）

- [x] FDを用いたfilter簡約を実装する。
- [x] 大きなIN listをsemi joinへ変換する。
- [x] predicateをCASEへ安全に押し込む。
- [x] inner joinからNOT NULLを推論する。
- [x] regexp prefix extractionを実装する。
- [x] comparisonへのcast pushdownを実装する。
- [x] scan zone mapとfilterを統合するhintを実装する。
- [x] unique条件付きのjoin内側へLIMITを押し込む。
- [x] unordered consumer下のsortを除去する。
- [x] unique key下の冗長DISTINCTを除去する。
- [x] DISTINCTとGROUP BYの相互変換を実装する。
- [x] COUNT(*) without GROUP rewriteを実装する。
- [x] 極端な選択性のためのfilter pull-upを探索候補にする。
- [ ] Calc merge / split方針を決めて実装する。
- [x] duplicate column、unused expression pruningを実装する。
- [x] ProjectionのCSEを実装し、同一式を行ごとに一度だけ評価する。
- [x] projection内のconstant propagationとpredicate pushdownを実装する。
- [x] CASE簡約、boolean filter引き上げ、join前後のproject幅制御を実装する。

### Join変換・探索

（以下は実装済み: join_commutativity, join_enumeration, join_associativity_left/right, infer_join_predicates, push_selection_through_join, split_selection_over_join, join_to_cross_if_no_predicate, merge_adjacent_filters, push_filter_through_left_join_left_side, join_on_false_to_empty, cross_to_inner_with_predicate, join_empty_simplification, self_join_elimination, unique_semi_to_inner, outer_to_anti_join, right_to_left_outer_join, full_outer_join_decomposition, push_down_limit_through_join, greedy_join_order_fallback, dynamic_filter_pushdown_join, join_predicate_transitivity, inferred_inequality_pushdown, redundant_join_predicate_elimination, outer_to_inner (relational)）

- [ ] n-ary bushy joinのleft/right associationを完全化する。
- [x] 空Values / 1行定数表によるjoin identityを実装する。
- [x] 1行側cross join eliminationを実装する。
- [x] 条件付きcrossとinnerの相互変換を実装する。
- [ ] inner join子交換をコスト駆動で再試行する。
- [ ] DPHyp / DPccp等の連結部分グラフ列挙を導入する。
- [x] 16関係超のgreedy join order fallbackを実装する。
- [ ] IKKbz / GOOヒューリスティックを検討・実装する。
- [x] star join reorderを実装する。
- [ ] bushy / left-deep探索予算を実装する。
- [x] 定数以外を含むjoin predicate transitivityを実装する。
- [x] inferred inequalityと冗長join predicateを実装する。
- [x] unique/FKに基づくjoin eliminationを実装する。
- [x] self join eliminationを実装する。
- [x] outer-to-anti、full outer分解、right-to-left変換を実装する。
- [ ] outer join上のfilter移動・分割・遅延評価規則を完成させる。
- [x] unique semi to inner、semi + distinct、semi reductionを実装する。
- [x] cardinality、one-to-one、one-to-many、many-to-many属性を推論する。
- [ ] unique内側を優先するindex join、equality hash join選択を実装する。
- [ ] non-equality nested loopのimplementation ruleを実装する。
- [x] dynamic filter、late materialization、batch lookupのruleを接続する。

### 集合・Window・UNNEST規則

（以下は実装済み: union_all_merge, union_to_union_all_plus_distinct, setop_empty_simplification, setop_empty_identity, union_all_push_limit, push_projection_through_union, intersect_to_semijoin, except_to_antijoin, intersect_except_cost_based_lowering, union_distinct_hash_sort_choice, window_frame_sort_sharing）

- [x] intersect-to-semijoin / except-to-antijoinをコスト付きで選択する。
- [x] UNION DISTINCTのhash/sort選択を実装する。
- [x] windowを分割・統合し、互換frameでsortを共有する。
- [x] window後のfilterをpartitionキー条件に限って押し下げる。
- [ ] window下のLIMITの安全な例外を定義する。
- [ ] 全体frameのwindow-to-aggregateを実装する。
- [x] rank / row_number filterをTopNへ変換する。
- [x] no-op windowを除去する。
- [x] window prefix sort共有を実装する。
- [x] UNNESTとfilter、JOIN順序、Values変換を最適化する。
- [ ] 小さいgenerate seriesをValuesへ変換する。
- [x] recursive termination predicate pushdownを実装する。

### 実行側のfilter / projection最適化

- [ ] Filterの短絡順序とConjunctReorderingを実装する。
- [ ] selection vector / validity bitmapを利用したfilterを実装する。
- [ ] bytecode / JIT filterの選択経路を接続する。
- [ ] VirtualComputedColumnの遅延評価を実装する。
- [ ] DictionaryProjectionを実装する。
- [ ] residual側のConstantFolding境界を整理する。

### Scalar rewrite（ExpressionRuleSet）

（以下はExpressionRuleSetに実装済み: fold_binary/unary/in/function, singleton_in, canonicalize_comparison, boolean_identity, double_negation, de_morgan, simplify_case, not_comparison/not_like/not_is_null/not_is_not_null, xor_boolean_identity, and/or_idempotent, absorption_and/or, identity_add/subtract_zero, identity_multiply/divide_one, double_negation_arithmetic, reassociate_add/subtract_constants, dedupe_in_list, uniform_case_result, like_equality, is_null/is_not_null_of_null_check, collapse_nested_identical_cast, factor_or_common_and, nullif_to_case, self_inequality, contradiction_from_null_eq, greatest_least_fold, in_single_null, concat_flatten, xor_to_or_and_not, is_distinct_from_rewrite, boolean_eq_true_false_three_valued, nondeterministic_barrier, safe_divide_rewrite, abs_of_abs, empty_in_list, coalesce_and_nullif_simplification, datetime_and_string_fold_extent, if_to_case, bit_and_zero, bit_or_zero, safe_add_zero, safe_subtract_zero, safe_multiply_one, safe_multiply_zero, array_length_array_exp, bit_and_identity, bit_or_identity, json_path_constant_fold, numeric_widening_cast, or_of_ranges_to_in, interval_normalize, predicate_pushdown_case, inner_join_not_null_inference, regexp_prefix_extraction, cast_pushdown_comparison, deterministic_function_cse, function_volatility_classification, boolean_filter_pullup, not_in_null_semantics, array_flatten_optimization）

- [x] numeric widening cast簡約とDATE/TIMESTAMP cast正規化を実装する。
- [x] NOT IN with NULL listの警告・計画を実装する。
- [x] interval normalizeを実装する。
- [x] JSON path constant foldを実装する。
- [x] projection以外も含むdeterministic function CSEを実装する。
- [x] stable / immutable / volatileの関数分類を実装する。
- [x] OR of ranges to INを実装する。
- [x] DATE_ADD/SUB、SUBSTRING等のfold適用範囲を回帰テストで固定する。
- [x] NOT BETWEENを実装する。

### Aggregate変換規則

（以下はCascadesルールとして実装済み: push_selection_through_aggregation, distinct_over_group_by, aggregate_projection_merge, eager_aggregation_over_join, unique_group_key_aggregate_elimination, aggregate_union_transpose, aggregate_join_transpose）

- [x] aggregateとprojectをmergeする。
- [ ] aggregateとfilterをtransposeし、HAVING残余を整理する。
- [x] unique / key preservationを守るaggregate-join transposeを実装する。
- [x] aggregate-union transposeを実装する。
- [ ] partial / finalへaggregateを分割する。
- [x] join前のeager aggregationとlazy aggregationを選択する。
- [x] unique group keyによるaggregate除去を実装する。
- [ ] NOT NULL条件のCOUNT(*) rewriteとSUM zero identityを実装する。
- [x] `COUNT(DISTINCT)` のdistinct aggregate expansionを実装する。
- [x] grouping sets expansion、grouping sets-to-union、ROLLUP / CUBE変換を実装する。
- [ ] grouping_id simplificationを実装する。
- [x] HAVINGをaggregate後filterへ明示変換する。
- [x] FILTER aggregate ruleを実装する。
- [ ] approximate aggregate rewriteを実装する。
- [ ] group keyのfunctional dependency reductionを実装する。
- [ ] aggregateをprojectionの下へ安全に押し下げる。

---

## Phase 4 — 統計・コスト・物理選択

- [x] 列ヒストグラム（等幅 / 等高）を収集する。
- [x] MCVを収集する。
- [x] NDV用HyperLogLog sketchを実装する。
- [x] NULL比率を独立統計として保持する。
- [x] 多列相関、関数従属、join cross-column NDVを推定する。
- [x] 式、LIKE、正規表現、OR、範囲述語の選択性モデルを実装する。
- [x] histogram joinを実装する。
- [x] skew補正、動的sampling、実行feedbackを実装する。
- [x] I/O / CPU / memoryのコスト単位を監査する。
- [x] memory許可量とspill確率、parallel speedupをモデル化する。
- [ ] index clustering factorとpage cache hit率をモデル化する。
- [x] PAX / zone map選択性をcostへ反映する。
- [ ] Top-N limit hintを全implementation ruleへ伝播する。
- [x] estimated rowsとactual rowsをEXPLAIN ANALYZEへ出す。
- [x] interesting orderを全演算子へ伝播する。
- [x] interesting partitioningを伝播する。
- [x] sort / merge join / sort agg / hash agg / stream aggのruleを分離する。
- [x] set-op hash/sort、materialize、spool、window ruleを実装する。
- [ ] ルール優先度・重み、探索timeout、best-so-farを実装する。
- [ ] join order freeze等のquery hintを実装する。
- [ ] workload別rule profileを実装する。
- [ ] group表現上限を適応制御する。
- [ ] plan cacheのparameter sniffing対策を実装する。
- [ ] generic planとcustom planの選択を実装する。
- [ ] DOP、batch size、morsel sizeをcost化する。

---

## Phase 5 — ベクトル化、memory、並列・分散

- [ ] 全operatorのDataChunk入出力を統一する。
- [x] selection vector / validity bitmapを全operatorへ適用する。
- [x] late materializationの列batchを実装する。
- [x] PipelineBreakerをHash build / Sort / Materializeへ明示する。
- [x] morsel-driven parallelismをCascades DOPへ接続する。
- [x] Exchange / Gather / Redistribute executorを実装する。
- [x] shared-build ParallelHashJoinを実装する。
- [x] ParallelMergeJoin、ScanPartition steeringを実装する。
- [x] NUMA-aware arenaを実装する。
- [x] SIMD比較kernelとJITの役割分担を実装する。
- [x] batch aggregationのdictionary encodingを実装する。
- [x] compressed PAX direct scan、zero-copy projectionを実装する。
- [x] operatorごとのmemory reservationを実装する。
- [ ] HashJoin / HashAgg / Sort / Windowのspill protocolを統一する。
- [ ] statement timeoutのcancelを全operatorへ伝播する。
- [x] memory不足時のgraceful degradeを実装する。
- [x] distributed any / singleton / hash / broadcast / rangeを実装する。
- [ ] enforce distribution、colocated join、broadcast vs shuffle costを実装する。
- [x] partial agg/finalize、two-phase distinctを分散経路へ接続する。
- [x] window partition再分散、distributed limit merge、scan range分割を実装する。
- [x] ExchangeHash / Broadcast / Gather / Rangeを実装する。
- [x] RemoteScan、DistributedAggFinalizeを実装する。

---

## Phase 6 — SQL境界と品質ゲート

### SQL機能

- [ ] LATERALのコスト付き展開を実装する。
- [x] PIVOT / UNPIVOTを展開する。
- [ ] MATCH_RECOGNIZEを実装する。
- [x] TABLESAMPLEを実装する。
- [x] QUALIFYを専用logical/physical filterとしてCascadesへ接続する。
- [x] GROUP BY ALL / GROUP BY DISTINCTを実装する。
- [x] SELECT DISTINCT ONを実装する。
- [x] FETCH FIRST WITH TIESを実装する。
- [x] FOR UPDATE / SKIP LOCKEDのaccess path制約を実装する。
- [x] MERGE、UPDATE FROM、DELETE USING、RETURNINGをSQLからplanへ接続する。
- [ ] prepared statementのgeneric/custom経路を完成させる。
- [ ] batch INSERTの値リスト結合を実装する。

### 制約・カタログ

- [x] PK / UNIQUEでDISTINCTを除去する。
- [x] NOT NULLでIS NOT NULLを除去する。
- [x] CHECK制約をpredicateへ取り込む。
- [x] FKでjoin除去とcardinality上界を推定する。
- [ ] partition constraint exclusionを実装する。
- [ ] generated column、partial index、expression indexを照合する。
- [ ] collationをschema・properties・ruleへ伝播する。
- [ ] view expansionとpredicate残存を実装する。
- [ ] security barrier viewとRLS predicateのpushdown制限を実装する。

### 実行品質・安全網

- [x] 全新規operatorのDump / EXPLAIN名を追加する。
- [x] ANALYZEに実時間、行数、loop、spill、batch数を追加する。
- [ ] buffer pool cache hitを実行統計へ追加する。
- [ ] MVCC snapshot visibilityをIndexOnly / Bitmapで検証する。
- [ ] cancel途中のresource leakを検証する。
- [ ] Sort / Limit / Distinctのengine safety net二重適用を検証する。
- [x] 全ruleの代数テストを追加する（`expression_rewrites.test`, `executor_expectations.test`, `unsupported_future_features.test` 追加）。
- [ ] random rule subset fuzzingを追加する。
- [ ] outer join null-pad goldenを追加する。
- [ ] TPC-H Q8/Q9 join order goldenを追加する。
- [ ] 関係fallbackとCascadesの差分テストを追加する。
- [ ] cost monotonicityを近似検査する。

---

## 完了条件

1. このファイルに残る各項目について、対応するコードと回帰テストがある。
2. `python3 scripts/check_layering.py` が成功する。
3. ビルドが成功し、既存テストを削らずに全テストが成功する。
4. GoogleSQL complianceで実装対象として宣言した機能に失敗がない。
5. 項目を完了したときは、このファイルからその項目を削除し、テスト名または
   実装契約をコミットメッセージに残す。
