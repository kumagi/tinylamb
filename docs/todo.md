# tinylamb 統合 TODO / 再建プラン

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

- [ ] `CteIsMaterializedOnceAndCanBeReadByMultipleConsumers` を修正する。
  （query_test で test は存在するが FAIL 中）

（以下はテスト合格を確認済み: WindowGroupsFrameUsesPeerGroups, WindowFrameExclusionRemovesCurrentRow, WindowFrameExclusionHandlesPeerGroupsAndTies, SqlEngineArrayGeneratorsFeedUnnest）
- [ ] 再帰CTEの基本、UNION DISTINCT、依存CTE、BY NAME、DEPTHの回帰を通す。
  （query_test で 3件 FAIL 中: SqlEngineRecursiveCteByNameUnionDedupesAlignedRows, SqlEngineRecursiveCteDepthModifierBoundsIterations 他）
（以下は修正済み: ZoneMapMayMatchDegenerateCases — NULL定数との比較は常にUNKNOWNなのでMayMatchがfalseを返すよう修正）
- [ ] DROP TABLE後のCatalog / TransactionContext cache無効化を修正する。
  （query_test で SqlEngineDistinctAndDropTable FAIL 中）

（以下はテスト合格を確認済み: UNION ALL LIMIT, 集約MIN/MAX, DATE/INTERVAL/LIKE/EXPLAIN, CteIsMaterializedOnce）

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

- [ ] `kSingleJoin` を `kMax1Row` だけに依存しない論理契約として導入する。
- [ ] `kMarkJoin` と一致・不一致・UNKNOWN marker payloadを導入する。
- [ ] `kDuplicateElim` の要否を `kDistinct` と整理し、論理名と物理契約を統一する。
- [ ] `kConstantTable` を `kValues` と分離して導入する。
- [ ] `kFilterFalse` を `kEmpty` と分離するか、不要なら論理語彙から除去する。
- [ ] `kWindow`（PARTITION、ORDER、frame）を導入する。
- [ ] `kUnnest`、`kGenerateSeries`、TVFの論理ノードを導入する。
- [ ] `kRecursiveCte`、`kWorkTableScan` をopaque IRではなくMemoに導入する。
- [ ] `kMaterialize`、`kEagerSpool`、`kLazySpool` を導入する。
- [ ] `kExpand`（GROUPING SETS / ROLLUP / CUBE）を導入する。
- [ ] `kApply` / `kLateralJoin` を導入する。
- [ ] `kExchange` / `kGather` / `kBroadcast` / `kRedistribute` の予約契約を導入する。
- [ ] `kSample` / `kTableSample` を導入する。
- [ ] 一般的な `kAssert` と cardinality assertion payloadを導入する。

### Payload、Schema、PhysicalProperties

- [ ] Aggregation payloadにGROUP BYキーとgrouping setsを保持する。
- [ ] Projection payloadでcomputed列とpassthrough列を区別する。
- [ ] Window payloadでROWS / RANGE / GROUPSとEXCLUDEを保持する。
- [ ] `PhysicalProperties` にcollation、NULL順序、partitioning、uniqueness、
      sorted-prefix、bloom/filterを追加し、子から親へ正しく伝播する。
- [ ] Memo groupが常に出力schemaを保持し、identity projectionを安全に判定する。
- [ ] FD、unique key、NOT NULLをgroup属性として伝播する。
- [ ] equivalence classをMemo全体で共有する。
- [ ] CNF / DNFを予算付き・選択的に保持する。
- [ ] outer joinのnull-supplying sideをpattern DSLで表現・制約する。
- [ ] null-rejectionをstrong-null / weak-nullに分離する。
- [ ] `NeedsRelationalEvaluation` の各fallbackを移行順と前提付きで管理する。

---

## Phase 2 — 実行器の不足分と共通実行契約

### Join

（以下はリレーショナル経路で実装済み: HashJoin inner/semi/anti/outer, MergeJoin, IndexJoin, CrossJoin/NestedLoopJoin）

- [ ] inequality merge / band joinを実装する。
- [ ] outer joinのsemiではないnull-aware matchを実装する。
- [ ] null-rejectingでない右側filterに対する実行時NULL生成を修正する。
- [ ] `MarkJoin` を実装し、`IN` のUNKNOWNをmarker列へ正しく反映する。
- [ ] uniqueness付きsemi joinのruntime assertionを実装する。
- [ ] Block Nested Loop Joinを実装する。
- [ ] Batch Nested Loop Joinを実装する。
- [ ] 複合キー・範囲対応のLookupJoin / Index Nested Loopを実装する。
- [ ] ZigZag Joinを実装する。
- [ ] Late Materialization Joinを実装する。
- [ ] Dynamic Filter Join / SIP / Bloomを実装する。
- [ ] Hash Semi Reductionを実装する。
- [ ] As-of Join / Interval Joinを実装する。

### Sort / Top-N

（以下は実装済み: SortPlan, TopNPlan, ExternalMergeSort, SortExecutor, TopNExecutor, ParallelSort）

- [ ] In-memory quicksort / pdqsortの選択経路を実装し、外部sortと契約を統一する。
- [ ] Incremental Sortを実装する。
- [ ] Partial Sortを実装する。
- [ ] FullScanがclustered index orderを安全に報告する。
- [ ] Skip Scan DISTINCTを実装する。
- [ ] collation対応比較を実装する（NULL順序だけの対応から拡張する）。
- [ ] buffered sortとexternal mergeのimplementation ruleを分離する。

### Aggregation

（以下は実装済み: AggregationPlan, HashAggregatePlan, SortAggregatePlan, StreamAggregate, FilteredAggregate, ScalarAggEmptyInput, ParallelAggregation）

- [ ] GROUP BYキーを持つHashAgg / SortAgg / StreamAggを実装する。
- [ ] HashAggとSortAggの実際のアルゴリズムを分離する。
- [ ] `StreamAggregate` の入力順序契約と実行器を接続する。
- [ ] `PartialAggregate` + `FinalizeAggregate`を実装する。
- [ ] `TwoPhaseDistinctAgg`を実装する。
- [ ] MIN/MAXのindex-only aggregate scanを実装する。
- [ ] SQL標準 `FILTER (WHERE ...)` をASTから実行器まで接続する。
- [ ] Ordered-set aggregate（PERCENTILE_CONT / WITHIN GROUP）を実装する。
- [ ] Grouping Sets / Rollup / Cube executorを実装する。
- [ ] bitwise、bool_and、bool_orのvectorized pathを実装する。
- [ ] ANY_VALUEの短絡評価を実装する。

### Scan / Access Path

（以下は実装済み: FullScan/ParallelScan, IndexScan, IndexOnlyScan, FilteredScanPushdown, DummyScan, ValuesScan, FunctionScan, UnnestScan, WorkTableScan）

- [ ] BitmapIndexScan / BitmapHeapScanを実装する。
- [ ] BitmapAnd / BitmapOrを実装する。
- [ ] TidScan / RowIdLookup / CTID相当を実装する。
- [ ] Index Skip Scanを実装する。
- [ ] IndexOnlyScanのMVCC visibility map相当を実装する。
- [ ] covering index選択のコスト精度を上げる。
- [ ] PAXScanをCascadesへ接続する。
- [ ] ZoneMapSkipScanをplan属性へ接続する。
- [ ] LateMaterializationScanを実装する。
- [ ] PrefetchingIndexScan、ParallelIndexScan、RangePartitionedScanを実装する。
- [ ] BERNOULLI / SYSTEMのSampleScanを実装する。
- [ ] 複数レンジIndexScanの順序回復を実装する。
- [ ] LSM / WiscKeyのLsmScan implementation ruleを実装する。
- [ ] ForeignScanを実装する。
- [ ] 複合インデックスの等式 + 不等式混在を実装する。
- [ ] 複数インデックスを用いたcovering判定を実装する。
- [ ] NestedLoopIndexJoinのprefetchを実装する。
- [ ] 内側filterをindexへ適用できない場合にheap側へ残す契約を明示する。
- [ ] unique内側の1行lookup短絡を実装する。
- [ ] ソート済み外側からのlookup localityをcost化する。

### DISTINCT / 集合演算

（以下は実装済み: DistinctExecutor, SetOperationExecutor (UNION ALL/DISTINCT, INTERSECT/ALL, EXCEPT/ALL), MergeAppend）

- [ ] UNION DISTINCT / INTERSECT / EXCEPTのhash/sort選択をCascadesへ接続する。
- [ ] set-opの型coercionを枝へ押し下げる。
- [ ] set-opの順序、LIMIT、DISTINCTを同時に扱う安全網を修正する。
- [ ] 同一表のunion branchを統合する。
- [ ] partition-wise unionを実装する。

### Window / UNNEST / 生成

（以下は実装済み: ApplyWindows, ComputeOneWindow, ROWS/RANGE/GROUPS frame, EXCLUDE, QUALIFY）

- [ ] `WindowAggPlan` / `WindowExecutor` を専用plan・executorとして実装する。
- [ ] GROUPS frameのpeer group境界を修正する。
- [ ] EXCLUDE CURRENT ROW / GROUP / TIESを修正する。
- [ ] Windowのhash partition + sortを実装する。
- [ ] Windowのspillを実装する。
- [ ] `FunctionScan` / TVFを実装する。
- [ ] `GenerateSeriesExecutor` と `GENERATE_SERIES` のSQL接続を実装する。
- [ ] `GENERATE_ARRAY` のstep、型、NULL、上限を回帰テストで固定する。
- [ ] `GENERATE_DATE_ARRAY` のDATE / INTERVAL stepをSQL経路で通す。
- [ ] UNNEST filter pushdownを実装する。
- [ ] array flattenを実装する。

### CTE / Materialize / Recursive

（以下はリレーショナル経路で実装済み: ExecuteRecursiveCte, CteMap, WorkTableScan via CteMap）

- [ ] `MaterializePlan` / `EagerSpool` をCascades専用planとして実装する。
- [ ] `LazySpool` / `ConsumerSpool` をCascades専用planとして実装する。
- [ ] `CteScan` をCascades専用planとして実装する。
- [ ] `RecursiveUnion` をanchor / delta / seen set付きでCascades専用planとして実装する。
- [ ] WorkTableScanをCascades専用planとして実装する。
- [ ] UNIONとUNION ALLの再帰差分を実装する。
- [ ] 再帰のサイクル検出、深さ上限、累積行予算を実装する。
- [ ] `WITH DEPTH` の列生成、範囲表示、UNBOUNDEDを実装する。
- [ ] recursive JOIN USING、BY NAME、CORRESPONDINGをschema契約として実装する。
- [ ] CTE依存順、前方参照、相互再帰エラーを実装する。
- [ ] plan template / cache再構築時にrecursive metadataを保持する。
- [ ] WorkTableIndexを実装する。
- [ ] CTE materialization / inlining / filter pushdownをコスト付きで選択する。
- [ ] CTE predicate propagationを実装する。
- [ ] recursive CTE union rewriteを実装する。
- [ ] 再帰CTEのmemory capとspillを実装する。

### DML / Lock

（以下はリレーショナル経路で実装済み: Insert, Update, Delete, BatchInsert）

- [ ] Upsert / ON CONFLICTを実装する。
- [ ] MATCHED / NOT MATCHEDのMergeExecutorを実装する。
- [ ] UPDATE FROM / DELETE USINGを実装する。
- [ ] ReturningExecutorを実装する。
- [ ] LockRows / SELECT FOR UPDATEを実装する。
- [ ] SkipLocked / NOWAITを実装する。
- [ ] Truncate executorを実装する。
- [ ] before / after trigger hookを実装する。
- [ ] DML logical / physical planを導入し、INSERT / UPDATE / DELETEをCascadesへ接続する。

### Sampling / 実行時統計

- [ ] AnalyzeScan（ヒストグラム構築用）を実装する。
- [ ] ReservoirSampleとBlockSampleを実装する。
- [ ] HyperLogLogScanを実装する。
- [ ] 実行時feedback用CardinalityProbeを実装する。

---

## Phase 3 — Optimizerの変換規則

### 述語・投影・LIMIT

（以下はCascadesルールとして実装済み: push_selection_into_scan, push_selection_through_join, split_selection_over_join, merge_selections, merge_adjacent_filters, push_projection_through_join, push_projection_through_union, push_filter_past_setop, push_filter_through_distinct, push_limit_through_projection, push_selection_through_aggregation, merge_limits, eliminate_true/false_selection, topn_push_through_projection, sort_push_through_projection, eliminate_double_sort, distinct_over_group_by, join_to_cross_if_no_predicate, join_on_false_to_empty, push_filter_through_left_join_left_side, push_projection_through_aggregation, limit_push_through_sort, push_filter_through_sort, merge_adjacent_projections）

- [ ] FDを用いたfilter簡約を実装する。
- [ ] 大きなIN listをsemi joinへ変換する。
- [ ] predicateをCASEへ安全に押し込む。
- [ ] inner joinからNOT NULLを推論する。
- [ ] regexp prefix extractionを実装する。
- [ ] comparisonへのcast pushdownを実装する。
- [ ] scan zone mapとfilterを統合するhintを実装する。
- [ ] unique条件付きのjoin内側へLIMITを押し込む。
- [ ] unordered consumer下のsortを除去する。
- [ ] unique key下の冗長DISTINCTを除去する。
- [ ] DISTINCTとGROUP BYの相互変換を実装する。
- [ ] COUNT(*) without GROUP rewriteを実装する。
- [ ] 極端な選択性のためのfilter pull-upを探索候補にする。
- [ ] Calc merge / split方針を決めて実装する。
- [ ] duplicate column、unused expression pruningを実装する。
- [ ] ProjectionのCSEを実装し、同一式を行ごとに一度だけ評価する。
- [ ] projection内のconstant propagationとpredicate pushdownを実装する。
- [ ] CASE簡約、boolean filter引き上げ、join前後のproject幅制御を実装する。

### Join変換・探索

（以下は実装済み: join_commutativity, join_enumeration, join_associativity_left/right, infer_join_predicates, push_selection_through_join, split_selection_over_join, join_to_cross_if_no_predicate, merge_adjacent_filters, push_filter_through_left_join_left_side, join_on_false_to_empty, outer_to_inner (relational)）

- [ ] n-ary bushy joinのleft/right associationを完全化する。
- [ ] 空Values / 1行定数表によるjoin identityを実装する。
- [ ] 1行側cross join eliminationを実装する。
- [ ] 条件付きcrossとinnerの相互変換を実装する。
- [ ] inner join子交換をコスト駆動で再試行する。
- [ ] DPHyp / DPccp等の連結部分グラフ列挙を導入する。
- [ ] 16関係超のgreedy join order fallbackを実装する。
- [ ] IKKbz / GOOヒューリスティックを検討・実装する。
- [ ] star join reorderを実装する。
- [ ] bushy / left-deep探索予算を実装する。
- [ ] 定数以外を含むjoin predicate transitivityを実装する。
- [ ] inferred inequalityと冗長join predicateを実装する。
- [ ] unique/FKに基づくjoin eliminationを実装する。
- [ ] self join eliminationを実装する。
- [ ] outer-to-anti、full outer分解、right-to-left変換を実装する。
- [ ] outer join上のfilter移動・分割・遅延評価規則を完成させる。
- [ ] unique semi to inner、semi + distinct、semi reductionを実装する。
- [ ] cardinality、one-to-one、one-to-many、many-to-many属性を推論する。
- [ ] unique内側を優先するindex join、equality hash join選択を実装する。
- [ ] non-equality nested loopのimplementation ruleを実装する。
- [ ] dynamic filter、late materialization、batch lookupのruleを接続する。

### 集合・Window・UNNEST規則

（以下は実装済み: union_all_merge, union_to_union_all_plus_distinct, setop_empty_simplification, setop_empty_identity, union_all_push_limit, push_projection_through_union）

- [ ] intersect-to-semijoin / except-to-antijoinをコスト付きで選択する。
- [ ] UNION DISTINCTのhash/sort選択を実装する。
- [ ] windowを分割・統合し、互換frameでsortを共有する。
- [ ] window後のfilterをpartitionキー条件に限って押し下げる。
- [ ] window下のLIMITの安全な例外を定義する。
- [ ] 全体frameのwindow-to-aggregateを実装する。
- [ ] rank / row_number filterをTopNへ変換する。
- [ ] no-op windowを除去する。
- [ ] window prefix sort共有を実装する。
- [ ] UNNESTとfilter、JOIN順序、Values変換を最適化する。
- [ ] 小さいgenerate seriesをValuesへ変換する。
- [ ] recursive termination predicate pushdownを実装する。

### 実行側のfilter / projection最適化

- [ ] Filterの短絡順序とConjunctReorderingを実装する。
- [ ] selection vector / validity bitmapを利用したfilterを実装する。
- [ ] bytecode / JIT filterの選択経路を接続する。
- [ ] VirtualComputedColumnの遅延評価を実装する。
- [ ] DictionaryProjectionを実装する。
- [ ] residual側のConstantFolding境界を整理する。

### Scalar rewrite（ExpressionRuleSet）

（以下はExpressionRuleSetに実装済み: fold_binary/unary/in/function, singleton_in, canonicalize_comparison, boolean_identity, double_negation, de_morgan, simplify_case, not_comparison/not_like/not_is_null/not_is_not_null, xor_boolean_identity, and/or_idempotent, absorption_and/or, identity_add/subtract_zero, identity_multiply/divide_one, double_negation_arithmetic, reassociate_add/subtract_constants, dedupe_in_list, uniform_case_result, like_equality, is_null/is_not_null_of_null_check, collapse_nested_identical_cast, factor_or_common_and, nullif_to_case, self_inequality, contradiction_from_null_eq, greatest_least_fold, in_single_null, concat_flatten, xor_to_or_and_not, is_distinct_from_rewrite, boolean_eq_true_false_three_valued, nondeterministic_barrier, safe_divide_rewrite, abs_of_abs, empty_in_list, coalesce_flatten, if_to_case, bit_and_zero, bit_or_zero, safe_add_zero, safe_subtract_zero, safe_multiply_one, safe_multiply_zero）

- [ ] numeric widening cast簡約とDATE/TIMESTAMP cast正規化を実装する。
（以下は実装済み: COALESCE flatten）
- [ ] COALESCE flattenを評価順を壊さず実装する。
（以下は実装済み: IF→CASE変換）
- [ ] IFと単一WHEN CASEの相互変換をcanonical表現付きで実装する。
（以下は実装済み: empty IN list → FALSE）
- [ ] empty IN listをFALSE定数化として実装する。
- [ ] NOT IN with NULL listの警告・計画を実装する。
- [ ] interval normalizeを実装する。
- [ ] JSON path constant foldを実装する。
- [ ] ArrayExpressionのconstant foldとARRAY_LENGTH zero foldを実装する。
（以下は実装済み: ABS(ABS(x)) → ABS(x)）
- [ ] ABS(ABS(x)) rewriteを実装する。
- [ ] projection以外も含むdeterministic function CSEを実装する。
- [ ] stable / immutable / volatileの関数分類を実装する。
- [ ] OR of ranges to INを実装する。
- [ ] DATE_ADD/SUB、SUBSTRING等のfold適用範囲を回帰テストで固定する。
- [ ] NOT BETWEENを実装する。
- [ ] BIT_AND / BIT_OR identityを実装する。

### Aggregate変換規則

（以下はCascadesルールとして実装済み: push_selection_through_aggregation, distinct_over_group_by）

- [ ] aggregateとprojectをmergeする。
- [ ] aggregateとfilterをtransposeし、HAVING残余を整理する。
- [ ] unique / key preservationを守るaggregate-join transposeを実装する。
- [ ] aggregate-union transposeを実装する。
- [ ] partial / finalへaggregateを分割する。
- [ ] join前のeager aggregationとlazy aggregationを選択する。
- [ ] unique group keyによるaggregate除去を実装する。
- [ ] NOT NULL条件のCOUNT(*) rewriteとSUM zero identityを実装する。
- [ ] `COUNT(DISTINCT)` のdistinct aggregate expansionを実装する。
- [ ] grouping sets expansion、grouping sets-to-union、ROLLUP / CUBE変換を実装する。
- [ ] grouping_id simplificationを実装する。
- [ ] HAVINGをaggregate後filterへ明示変換する。
- [ ] FILTER aggregate ruleを実装する。
- [ ] approximate aggregate rewriteを実装する。
- [ ] group keyのfunctional dependency reductionを実装する。
- [ ] aggregateをprojectionの下へ安全に押し下げる。

---

## Phase 4 — 統計・コスト・物理選択

- [ ] 列ヒストグラム（等幅 / 等高）を収集する。
- [ ] MCVを収集する。
- [ ] NDV用HyperLogLog sketchを実装する。
- [ ] NULL比率を独立統計として保持する。
- [ ] 多列相関、関数従属、join cross-column NDVを推定する。
- [ ] 式、LIKE、正規表現、OR、範囲述語の選択性モデルを実装する。
- [ ] histogram joinを実装する。
- [ ] skew補正、動的sampling、実行feedbackを実装する。
- [ ] I/O / CPU / memoryのコスト単位を監査する。
- [ ] memory許可量とspill確率、parallel speedupをモデル化する。
- [ ] index clustering factorとpage cache hit率をモデル化する。
- [ ] PAX / zone map選択性をcostへ反映する。
- [ ] Top-N limit hintを全implementation ruleへ伝播する。
- [ ] estimated rowsとactual rowsをEXPLAIN ANALYZEへ出す。
- [ ] interesting orderを全演算子へ伝播する。
- [ ] interesting partitioningを伝播する。
- [ ] sort / merge join / sort agg / hash agg / stream aggのruleを分離する。
- [ ] set-op hash/sort、materialize、spool、window ruleを実装する。
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
- [ ] selection vector / validity bitmapを全operatorへ適用する。
- [ ] late materializationの列batchを実装する。
- [ ] PipelineBreakerをHash build / Sort / Materializeへ明示する。
- [ ] morsel-driven parallelismをCascades DOPへ接続する。
- [ ] Exchange / Gather / Redistribute executorを実装する。
- [ ] shared-build ParallelHashJoinを実装する。
- [ ] ParallelMergeJoin、ScanPartition steeringを実装する。
- [ ] NUMA-aware arenaを実装する。
- [ ] SIMD比較kernelとJITの役割分担を実装する。
- [ ] batch aggregationのdictionary encodingを実装する。
- [ ] compressed PAX direct scan、zero-copy projectionを実装する。
- [ ] operatorごとのmemory reservationを実装する。
- [ ] HashJoin / HashAgg / Sort / Windowのspill protocolを統一する。
- [ ] statement timeoutのcancelを全operatorへ伝播する。
- [ ] memory不足時のgraceful degradeを実装する。
- [ ] distributed any / singleton / hash / broadcast / rangeを実装する。
- [ ] enforce distribution、colocated join、broadcast vs shuffle costを実装する。
- [ ] partial agg/finalize、two-phase distinctを分散経路へ接続する。
- [ ] window partition再分散、distributed limit merge、scan range分割を実装する。
- [ ] ExchangeHash / Broadcast / Gather / Rangeを実装する。
- [ ] RemoteScan、DistributedAggFinalizeを実装する。

---

## Phase 6 — SQL境界と品質ゲート

### SQL機能

- [ ] LATERALのコスト付き展開を実装する。
- [ ] PIVOT / UNPIVOTを展開する。
- [ ] MATCH_RECOGNIZEを実装する。
- [ ] TABLESAMPLEを実装する。
- [ ] QUALIFYを専用logical/physical filterとしてCascadesへ接続する。
- [ ] GROUP BY ALL / GROUP BY DISTINCTを実装する。
- [ ] SELECT DISTINCT ONを実装する。
- [ ] FETCH FIRST WITH TIESを実装する。
- [ ] FOR UPDATE / SKIP LOCKEDのaccess path制約を実装する。
- [ ] MERGE、UPDATE FROM、DELETE USING、RETURNINGをSQLからplanへ接続する。
- [ ] prepared statementのgeneric/custom経路を完成させる。
- [ ] batch INSERTの値リスト結合を実装する。

### 制約・カタログ

- [ ] PK / UNIQUEでDISTINCTを除去する。
- [ ] NOT NULLでIS NOT NULLを除去する。
- [ ] CHECK制約をpredicateへ取り込む。
- [ ] FKでjoin除去とcardinality上界を推定する。
- [ ] partition constraint exclusionを実装する。
- [ ] generated column、partial index、expression indexを照合する。
- [ ] collationをschema・properties・ruleへ伝播する。
- [ ] view expansionとpredicate残存を実装する。
- [ ] security barrier viewとRLS predicateのpushdown制限を実装する。

### 実行品質・安全網

- [ ] 全新規operatorのDump / EXPLAIN名を追加する。
- [ ] ANALYZEに実時間、行数、loop、spill、batch数を追加する。
- [ ] buffer pool cache hitを実行統計へ追加する。
- [ ] MVCC snapshot visibilityをIndexOnly / Bitmapで検証する。
- [ ] cancel途中のresource leakを検証する。
- [ ] Sort / Limit / Distinctのengine safety net二重適用を検証する。
- [ ] 全ruleの代数テストを追加する。
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
