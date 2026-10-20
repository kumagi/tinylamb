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

- [x] 全テストを `ctest --output-on-failure` で再現できる固定コマンドとログを用意する。
      `scripts/verify_all.sh` がRelWithDebInfo build・CTest・layering lintを
      同一ログへ記録する。
- [x] SQL経路、直接Executor経路、Cascades経路を同じ期待結果で比較する。
      `plan/phase0_contract_test.cpp:SqlCascadesAndDirectExecutorShareExpectedRows`
      がNULL・重複・境界値を含む順序付き行多重集合を3経路で比較する。
- [x] 各TODOに、最小の成功例、NULL、空入力、重複、境界値、エラー例を追加する。
      `Phase0ContractTest.ManifestCoversEveryTodoAndSemanticClass` が55–59の
      各項目に6ケースを固定し、SQL fixtureの結果とerror statusを検証する。
- [x] ルール適用前後の結果集合・スキーマ・順序・推定行数を検証する。
      `Phase0ContractTest.RuleRewritePreservesSchemaOrderSetAndEstimate` が
      `push_selection_into_scan` 適用前後を比較する。
- [x] EXPLAINに選択されたルール名、物理演算子、required/provided propertyを表示する。
      `Phase0ContractTest.ExplainShowsRuleOperatorAndPropertyContract` と
      `plan/plan.hpp` の `PlanExplainInfo` がSQL EXPLAIN回帰を固定する。
- [x] ルール追加で無限展開しないことをfingerprintと上限テストで保証する。
      `CascadesNonConvergenceTest.FingerprintsRejectDuplicateRuleExpansion` と
      `SearchStepBudgetStopsUniqueExpansion` が重複fingerprintと探索予算を検証する。

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

（以下は実装済み: HashJoin [Inner, Semi, Anti, NullAwareAnti, LeftOuter, RightOuter, FullOuter, Mark, Single], MergeJoin, NestedLoopJoin [BlockNestedLoop], CrossJoin, LookupJoin [Index Nested Loop; 複合キー等値prefix＋range bound 1個], LateMaterializationJoin [equi/unique winner/rowid; payload遅延], ZigZagJoin [両側Index; 単一署名付きINT64キー; 等値/単一range]）

- [x] inequality merge / band joinを実装する（inner、単一の符号付きINT64/DATE順序比較、NULL・重複・空入力・エラー・順序・行数を固定。複合キー/複数述語/他型とouter/semi/antiのCascades loweringはImplementation倒し）。
      - [x] `join_commutativity` で左右の子が交換されたPlanningにおける band join の行落ちを修正する（`TryBandJoinSpec` の交換分岐で比較演算子を鏡像化し、spec を「`left <op> right`」＝実行器の子の実際の順で表すようにする。候補範囲と残差再検査が食い違い一致行を落としていた。回帰: `band_join_test.cpp` の `RuleMirrorsComparisonForSwappedChildOrder` / `SwappedChildOrderReturnsTheSamePairs`、`optimizer_test.cpp` の `BandJoinSurvivesJoinCommutativity`）。
- [x] outer joinのsemiではないnull-aware matchを実装する。
- [x] null-rejectingでない右側filterに対する実行時NULL生成を修正する。
- [x] uniqueness付きsemi joinのruntime assertionを実装する。
- [x] Batch Nested Loop Joinを実装する。
- [x] 複合キー・範囲対応のLookupJoin / Index Nested Loopを実装する（等値prefix＋1個のrange bound（INT64/DATE）。probe列は索引キー順で導出。複数range・非対応型・非キー列の等値は残差述語へ明示fallback。unique indexの単一probeはshort-circuit。SQL/optimizer/直接実行器の回帰あり）。
- [x] Late Materialization Joinを実装する（equi key / unique winner / 明示rowidの候補RUS表现为`RowPosition`のみで保持し、payloadを上位`LateMaterializePayload`段で`Table::Read`する。inner・符号付きINT64/DATEキー・NULL/重複/空入力/unique/error・row position/順序を固定。outer/semi/anti、NULL-safe、ARRAY、retained index、row-position必須、volatile/subquery、巨大spillはrule拒否またはfactory fallback）。
- [x] Dynamic Filter Join / SIP / Bloomを実装する（`common/runtime_filter.{hpp,cpp}` の3形状: exact_set＝buildキー集合のIP、min_max＝[lo,hi]囲い、bloom＝2 probeのビット集合。`RuntimeFilterGroup` は合算則AND。build側は先にdrainして`ExecutorBase::AttachRuntimeFilter`でprobe scanへpublishし、`FullScanIterator` がrow decode前のpeekで判定する。probe childが拒否すればjoin自身が`JoinState::Consume`で適用。NULLキーは常にreject、型不一致（INT64 vs DATE）はpublish前に拒否、spilled build・複合キー・null-safe・outer/semi/anti・hybrid hash・shared-build parallel hashは明示fallback。論理側は`dynamic_filter_pushdown_join` がprobe groupへ`dynamic_filter_keys`をannotateし、`hash_join` implementation ruleが「inner/非null-safe/単一INT64・DATE等値 かつ build*4 <= probe（または build<=1024 かつ probe>=8192）」のときだけ`ProductPlan::PreferDynamicFilter(build_left)`を立てる。回帰: `common/runtime_filter_test.cpp`、`executor/dynamic_filter_join_test.cpp`、`plan/cascades_test.cpp` の `DynamicFilterPushdownJoinAnnotatesTheProbeKeys` / `DynamicFilterPushdownJoinSkipsNonEqualityJoins` / `DynamicFilterPushdownJoinSkipsMultiRelationProbe`、`plan/optimizer_test.cpp` の `DynamicFilterMarkerFollowsTheAnnotatedProfitableShape` / `DynamicFilterMarkerEngagesOnAProbeHeavyShape`）。
- [x] Hash Semi Reductionを実装する（存在判定だけを使う semi / anti / null-aware anti は build 側を
      「probe 側ドメインに含まれるキーの1行」に縮める。probe の等値キーを先に `JoinHashIndex` で
      ドメイン化し（NULL は照合不能なのでドメインに加えない。null-safe キーではハッシュ索引と同じ
      memcomparable エンコードで NULL も一致させる）、その外側の build 行は intake 中に捨てて
      しまう。build キー重複も1行に畳む）。出力行・順序・行位置は従来 semi と完全に一致し、防御は
      2つ: (1) `kSemiReductionMaxDomainKeys` を超えた domain は解放し非制限 intake に fallback
      して build キー重複除去だけを残す、(2) `NOT IN` の三値論理と「build 側が空」の判定は縮約後
      の hash ではなく *生の* build cardinality と NULL フラグを読む。論理側は
      `semi_reduction_pushdown_semi_join` / `semi_reduction_pushdown_anti_join` が probe 側の
      等値キー列を `Group::semi_reduction_probe_keys` に記す（キー列の fingerprint を tag に持つ
      derived group。derived group は probe の selection を持ち D5 ゲートを満たす）、
      `ProductPlan::PreferSemiReduction` が物理マーカー、`executor/relational_factory.cpp` が
      実行器へ渡す。採否は `SemiReductionIsProfitable`（`plan/product_plan.cpp`。Cascades の hash
      実装規則と decorrelation が同じ判定を共有する）で、build 行数が probe 行数より大きく、
      かつ build の半分以上が消え、かつ絶対下限 64 行を越えるときだけ採用する。inner / outer /
      mark / single、null-safe キー、残余述語付きの semi/anti（そもそも hash 代替を生成しない）、
      収益性不十分な形は従来 semi へ fallback する。spill した build も domain と同じハッシュ規則
      で区画分割されるため、縮約済みの行が区画をまたいでも照合は exhaustive のまま。
      EXPLAIN ANALYZE の `SemiReduction <status> probe_keys= build_rows= build_rows_kept=
      build_rows_pruned= duplicate_keys_pruned=` が証拠。回帰: `executor/hash_semi_reduction_test.cpp`
      （SQL / 直接実行器 / 収益性ゲート。NULL・重複・空入力・外側結合・anti・`NOT IN`・spill・
      並列 probe・一意 INTEGER キー・ORDER BY・行位置・batch 経路）、
      `plan/cascades_test.cpp` の `SemiReductionPushdownAnnotatesTheProbeKeys` /
      `SemiReductionPushdownSkipsNonEqualityJoins` /
      `SemiReductionPushdownAnnotatesEveryCompositeKey`）。
- [x] As-of Join / Interval Joinを実装する。

### Sort / Top-N

（以下は実装済み: SortPlan（base）, BufferedSortPlan, ExternalMergeSortPlan, TopNPlan, ExternalMergeSort, SortExecutor, TopNExecutor, ParallelSort）

- [x] In-memory quicksort / pdqsortの選択経路を実装し、外部sortと契約を統一する。
- [x] Incremental Sortを実装する。
- [x] Partial Sortを実装する。
- [x] FullScanがclustered index orderを安全に報告する。
      ヒープは row-page chain の tail への追記だけで成り、`FullScanIterator` は先頭→末尾・slot 昇順で歩くので、走査順は「挿入順」そのものである。これが SQL の順序になるには「カタログ」「測定」「鮮度」の3条件が揃う必要があり、`plan/full_scan_plan.hpp` の `ClusteredOrderColumn()` が検査する。
      (1) カタログ: その列が UNIQUE 索引のキー全体（CREATE TABLE は PRIMARY KEY/UNIQUE 列ごとに索引を作る）で、列制約が NOT NULL。NULL が挟まらないので、報告する順序は NULLS FIRST でも NULLS LAST でも同じ列になる。
      (2) 測定: ANALYZE がその列の相関を 1.0 として記録済み（走査順で隣接する非 NULL 対がすべて非減少）。
      (3) 鮮度: 統計が `Table::LayoutGeneration()` を持ち、それが表の現在値と一致する。row mutation は layout generation を進め、UPDATE/DELETE はさらに 1 プロセス内で in-place 書き換えの latch を立てる（古い snapshot は pre-image を解決しうるので、現在の画像の測定はその代用にならない）。ANALYZE は自分の走査の前後で世代が動いていた場合、証拠を捨てる（走査中に commit された行は測定に含まれない）。生成直後のプロセスでは世代がプロセスグローバルカウンタから採番されるので、再開前の永続化済み stamp とは一致しない。
      非unique/複合キー/nullable/未ANALYZE/更新/削除/並列閾値超（`ParallelScan` はモーセル完了順にチャンクを出すので保存順ではない）/DML source scan（`lock_rows_`）はいずれも false へ fallback する。`IsOrderedBy` は昇順・先頭キー1個・素の列参照だけに一致し（降順は逆順チェーン走査がない、2個目以降は prefix として `IncrementalSort` 側へ委ねる）、EXPLAIN には `FullScan: t (clustered order: id ASC)` が出る。順序を守るため `FullScanPlan::EmitExecutor` はこの場合 single-threaded なチェーン順 `FullScan` を保つ。
      コストは別に扱う: `PlanBase::OrderIsMeasured()` が「保存順から測定した順序」を表し、構造的な順序（索引・スート）と違い全チェーンを読んだ代償が残る。したがって direct path の「順序付き候補を優先」段も `ApplyLimitHint` の LIMIT 早期打ち切り割引も適用対象外にする（適用しないと `ORDER BY pk LIMIT 1` が全表読みを1行扱いとして索引を追い越してしまう）。
      順序主張は失効しうる（次の行変異で false に戻る）ので、`query/plan_cache.hpp` の契約どおり、その文脈で解決済みテーブルに claim がある間は `sql_engine.cpp` が plan をキャッシュしない。ソート除去が上限の折畳み / `IncrementalSort` として木に焼き込まれた plan は、後の別 snapshot で再生すると壊れるため。
      回帰: `plan/full_scan_plan_test.cpp`（ゲート全分岐と EXPLAIN 描画）、`query/sql_clustered_order_test.cpp`（SQL の順序結果、LIMIT/OFFSET/DISTINCT/NULL 順序/空表/失敗文、変異後の Sort 復帰、キャッシュ非登録）、`plan/optimizer_test.cpp` の `ClusteredFullScanDoesNotOutrankASelectiveIndexPath` / `ClusteredFullScanIsNotPricedAsALimitEarlyStop`（アクセスパス選択とコストの非干渉）、`table/table_statistics_test.cpp` の `*LayoutGeneration*`（stamp の記録・失効・派生統計の破棄・永続化ラウンドトリップ）。
- [x] `IncrementalSort` を入力の既存順序 prefix と suffix ソートへ接続する。
- [x] Skip Scan DISTINCTを実装する。
- [x] collation対応比較を実装する（NULL順序だけの対応から拡張する）。
      比較契約は `type/collation.{hpp,cpp}` に一元化し（`Collation` / `ResolveCollation` /
      `ApplyCollation` / `CompareWithCollation` / `CompareUnderCollation` / `EqualUnderCollation` /
      `SortKeyOrder`）、`Value` のタグ（0=なし・1=binary・2=case-insensitive。3は符号なし整数マーカーで
      照合順序ではない）だけが照合順序の解決点になる。畳み込みは ASCII のみで、`expression/` の
      `FoldCase`・`CompareForOrderBy` の VARCHAR 経路・`CanonicalDistinctValue` がこの一個所を使う。
      ロケール／ICU 照合順序は未実装で、`en_US` 等は case-insensitive として扱う（未対応は binary
      fallback）。UTF-8 のマルチバイト列・`interval` 表現・ENUM 固定順位は byte 順のまま変わらない。
      実装: `SortExecutor` はキー符号化**前**に畳むので、メモリ内経路・spill した run・外部マージの
      merge window が同じ符号化を使う（`Row` 直列化はタグを落としてもマージ側がキー式を再評価するので
      付け直る）。比較経路の `TopNExecutor` ヒープ / `PdqSort` / `MergeAppendExecutor` /
      `PartialSortExecutor` / SQL 関係代数の三つの `ORDER BY` ソータはすべて `SortKeyOrder` を使い、
      NULL 位置・方向・照合順序が一つの契約に集約される。`WITH TIES` の境界行と `IncrementalSort`
      の prefix 判定は `EqualUnderCollation`。Window の ORDER BY はその場限りの関数名判定をやめて同じ契約へ。
      `GROUP BY` / `DISTINCT` は既存の `CanonicalDistinctValue`（同じ畳み込み）と一致。
      `COLLATE(value, spec)` を AST 経路にも実装し（従来はバッチ評価器しか "not yet executable" にしていた）、
      `ColumnVector` が列の collator を保持して値に付け直すので、射影の隠し `$orderN` 経由でも畳みが
      失われない。optimizer は `PhysicalProperties::collation` で伝播し、`PlanBase::OrderCollation()`
      （`SortPlan`/`TopNPlan` が override、projection/selection/limit は転送）と
      `SatisfiesOrderingCollation` が「要求照合順序と同一か」を判定する。case-insensitive な順序は
      binary な順序の代替にならない（`und:ci` で `a` < `B`、binary では逆）ので
      `ORDER BY x COLLATE 'und:ci'` は索引順にも clustered order にも畳まれない。
      `ORDER BY x COLLATE 'spec'` の節構文（AST は `Collate` 子ノード）も関數-call 形と同じ
      `COLLATE(x, 'spec')` に畳む（`query/googlesql_ast_visitor.cpp`）。従来はこの節が無視されて
      binary 順が黙って返っていた。`ORDER BY 1 COLLATE 'spec'` の ordinal は照合順序と対で
      遅延解決する（`AsCollatedOrdinal` / `ApplyCollationSpec` / `ReapplyCollation`）。解決し損なうと
      全行共通の中間キーになり「整列が何もしない」ので binary ではなく明示的に畳む。
      回帰: `type/collation_test.cpp`（タグ往復・解決規則・畳み込み・全順序性・peer 等価・SortKeyOrder の
      NULL 位置/方向）、`executor/sort_collation_test.cpp`（Sort/外部merge spill/TopN/WITH TIES/
      PdqSort/IncrementalSort/PartialSort/MergeAppend、重複・空・境界バイト・interval 形・非ASCII・
      キー評価エラー）、`plan/collation_property_test.cpp`（OrderCollation・requirement 判定・
      Property Key 分離・provided/EXPLAIN 伝播）、`query/sql_collation_test.cpp`（SQL 実行順の回帰:
      base table/subquery/UNNEST/alias/ordinal の一致、NULL 位置、LIMIT/OFFSET/WITH TIES、
      GROUP BY と window RANK との一致、`=` 述語・DISTINCT との一致、非ASCII と interval 形の
      byte 順維持、整数キー・空入力・重複）。
- [x] buffered sortとexternal mergeのimplementation ruleを分離する。
      内部 spill 選択は `SortExecutor::Materialize` のまま残し、**どちらの形を選ぶか**だけを plan 側に出した。
      論理情報は同じ ORDER BY（キー・方向・NULL 位置・照合順序を全共有）なので、分離するのはメモリ契約と
      EXPLAIN の名前だけで、`IsOrderedBy` / `OrderCollation` / `EnforcesLimit` / 行数 /
      ParameterSlot の転送は base `SortPlan` に一本化して両形が報告する。
      - `plan/sort_plan.hpp`: `SortMemoryPolicy{memory_budget_bytes, merge_window_rows}`、
        `SortRowCapacity(policy)`（`budget / kSortEstimatedRowBytes`）、`SortStrategy`、
        base `SortPlan`（互換のためfinalを外し、EXPLAIN名"StrategyName()"は"Sort"のまま）、
        `BufferedSortPlan`（"BufferedSort"）/ `ExternalMergeSortPlan`（"ExternalMergeSort"）、
        選択の単一実装 `MakeSortPlan(child, keys, estimated_rows, policy)`。
      - 実装 rule は `sort`（buffered）と `sort_external_merge`（spill runs）の**排他な2片**で、
        共通部分（キー構築・`SatisfiesOrderingCollation` を伴う省略判定）は `PrepareSortShape` に抽出。
        推定行数 <= `SortRowCapacity` なら buffered のみ、越えたら external merge のみが alternative を
        出す（選択は推定行数の決定関数）。external merge のコストは
        `EstimateMemorySpillCost(kSort, …)` を加えた分だけ高く、run の書き出し/再マージの対価を
        明示している。子が既に順序を配送するなら両 rule とも sort を挿入しない（省略は buffered
        側だけが提供し、同一 plan が2つの rule 名で memo に残らないようにする）。
        merge join / band join / sort-distinct / UNION ALL の内部 sort と
        `OptimizeSingleRelation` の高速経路も同じ `MakeSortPlan` を通す。
      - 専用 factory wiring: `executor/relational_factory.cpp` に `BufferedSortPlan::EmitExecutor` と
        `ExternalMergeSortPlan::EmitExecutor`（`ExecutorSortKeys` / `SortExecutorOptions` 共有）。
        `SortExecutor::Options{spill_mode, memory_budget_bytes, merge_window_rows, spill_threshold_rows}`
        を渡す。既存4引数 ctor は `Options{}`（kAuto）へ委譲するので**既存 Sort API は互換**。
      - operator 側: kBuffered は予算までメモリに保持して**spill を計画しない**（予算/プロセス予算が尽き
        た時だけ executor 内部の選択で spill する = 誤った推定でも答えは変えない安全弁）、
        kExternalMerge は `spill_threshold_rows` 到達で sorted run を書き出し、末尾 run も必ず書いて
        merge window 越しにマージするので作業集合が入力サイズに依存しない。全 mode で内部 spill 選択は
        最終手段として残る。Dump は `ParallelSort (1 workers, spill=external-merge, budget=…, run_rows=…)`。
      回帰: `plan/sort_shape_test.cpp`（両形の順序/collation/NULL/ParameterSlot 契約の一致、メモリ契約と
      RowCapacity、EXPLAIN 名と memory suffix、PlanExplainInfo の rule/physical operator/provided 順序が
      形で変わらないこと、`MakeSortPlan` と2 rule の排他選択契約、順序配送済み子の省略、folding 要求を
      binary な子が満たさないこと、factory が各形の operator 設定を渡すこと、両形が同じ行順を返す
      こと）、`executor/sort_shape_executor_test.cpp`（既定 kAuto の互換、小入力は非 spill、
      予算超過時の正しい fallback、rows threshold 複数 run のマージ、merge window の clamp、
      ORDER BY/NULL/ties・降順+NULLS LAST の一致、キー評価失敗/子 cursor 失敗が両形で error として
      出て部分結果を返さないこと、空入力）。

### Aggregation

（以下は実装済み: AggregationPlan, HashAggregatePlan, SortAggregatePlan, StreamAggregatePlan, HashAggregateExecutor, SortAggregateExecutor, StreamAggregateExecutor, FilteredAggregate, ScalarAggEmptyInput, ParallelAggregation）

- [x] GROUP BYキーを持つHashAgg / SortAgg / StreamAggを実装する。
      `AggregatePayload`{group_keys, aggregates, having, estimated_groups,
      memory} を `plan/aggregation_plan.hpp` に用意し、3つの plan 型と3つの
      実行器（`executor/grouped_aggregate.{hpp,cpp}`）が同じ payload を受け取る。
      出力行は [group key..., aggregate...]、`HAVING` はその出力行に対する述語
      （ノード上の `kSelection` と同じスコープ）。グループキーなしは 1 group
      のまま既存スカラー経路（`AggregationExecutor` /
      `ParallelAggregationExecutor`）へ束ね、既存 API は非互換なし。
      回帰: `executor/grouped_aggregate_test.cpp`（NULL/空/重複キー/複合キー/
      FILTER/DISTINCT/AVG/HAVING/エラー/spill/file error/並列/SQL 差分）、
      `plan/aggregate_shape_test.cpp`（payload・順序契約・EXPLAIN・規則選択・
      factory 配線）。
- [x] HashAggとSortAggの実際のアルゴリズムを分離する。
      HashAgg は group table（予算超過時: key hash で partition spill して
      partition 単位で再集計）、SortAgg は group key の sort run（run 書き込み
      + merge window）→ 同一 key run を 1 パスで畳む。両者は同じ group 集合を
      返し、違うのはメモリ契約と出力順序（SortAgg は group-key 順を約束する）。
      分離は `PeakResidentGroups()`（同時に保持する group 数）で観測可能な
      まま固定した: HashAgg のピークは distinct key 数（spill 後は最大 1
      partition の group 数）、SortAgg は常に 1。factory も strategy ごとに
      設定を分けて引き渡す（worker 数 → HashAgg、run threshold →
      SortAgg、順序検証 → StreamAgg）。
      回帰: `grouped_aggregate_test.cpp` の
      `AllThreeShapesReturnTheSameGroups` /
      `TheHashShapeKeepsAGroupTableTheSortShapeKeepsOneGroup` /
      `TheHashShapeDoesNotReorderItsInputTheSortShapeDoes` /
      `InterleavedKeysFoldIntoTheSameGroupsAsAdjacentOnes` /
      `EachGroupedShapeSpillsOnItsOwnContractOnly` /
      `APartitionedHashBuildHoldsOnePartitionsGroups` /
      `TheHashShapePartitionsAndStillReturnsEveryGroup` /
      `TheSortShapeWritesRunsAndStillReturnsEveryGroup` /
      `TheSpillPathsKeepTheNullAndDuplicateContract` /
      `TheSortShapeFoldsNanAndSignedZeroKeysAcrossItsRuns` /
      `TheEmptyInputReachesNeitherGroupedSpillPath` /
      `OnlyTheHashShapeFoldsItsInputInParallel` /
      `AFailingChildIsTheSameErrorOnBothGroupedAlgorithms`、
      `aggregate_shape_test.cpp` の
      `OnlyTheOrderedShapesPromiseAnOrder` /
      `EveryAlternativeIsExactlyOneStrategy` /
      `TheStrategyChoiceFollowsTheGroupTableMemoryContract` /
      `EachShapeEmitsItsOwnStrategySettings` / `ExplainInfo...`。
- [x] `StreamAggregate` の入力順序契約と実行器を接続する。
      順序契約は「group key を昇順・NULLS FIRST で到着済み」の入力。規則
      `aggregation` はその順序を供給する子の場合にだけ第3代替を提示し
      （`ChildDeliversGroupKeyOrder`）、実行器は `require_ordered_input` で
      同じ契約を自分で検証する（違反は `kInvalidArgument`、部分結果は返さ
      ない）。group key が 0 個なら group が 1 個なので任意順序で valid。
      回帰: `grouped_aggregate_test.cpp` の
      `TheStreamShapeRefusesAnUnorderedInput` /
      `TheStreamShapeAcceptsThePromisedInputOrder` /
      `TheStreamShapeRefusesNullsThatAreNotFirst` /
      `AKeylessPayloadNeedsNoInputOrder`、
      `aggregate_shape_test.cpp` の `TheStreamShapeNeedsAnOrderedInput`。
- [x] `PartialAggregate` + `FinalizeAggregate`を実装する。
- [x] `TwoPhaseDistinctAgg`を実装する。
- [x] MIN/MAXのindex-only aggregate scanを実装する（非NULLな単一キー列を
      順序付きインデックスの先頭1件から取得）。
- [x] SQL標準 `FILTER (WHERE ...)` をASTから実行器まで接続する。
- [x] Ordered-set aggregate（PERCENTILE_CONT / WITHIN GROUP）を実装する。
- [x] Grouping Sets / Rollup / Cube executorを実装する。
- [x] bitwise、bool_and、bool_orのvectorized pathを実装する。
- [x] ANY_VALUEの短絡評価を実装する。

### Scan / Access Path

（以下は実装済み: FullScan/ParallelScan, IndexScan, IndexOnlyScan, BitmapIndexScan, BitmapHeapScan, BitmapAnd, BitmapOr, TidScan, FilteredScanPushdown, DummyScan, ValuesScan, UnnestScan, WorkTableScan）

- [x] BitmapIndexScan / BitmapHeapScanを実装する。
- [x] BitmapAnd / BitmapOrを実装する。
- [x] TidScan / RowIdLookup / CTID相当を実装する。
- [x] Index Skip Scanを実装する。
- [x] PAXScanをCascadesへ接続する。
- [x] ZoneMapSkipScanをplan属性へ接続する。
- [x] LateMaterializationScanを実装する。
- [x] BERNOULLI / SYSTEMのSampleScanを実装する。
- [ ] ForeignScanを実装する。（2026-10-17 保留: 外部ソース形式が未定のため設計待ち。次の具体項目から着手）
- [x] 複数インデックスを用いたcovering判定を実装する。
      `ScanAlternatives`（`plan/implementation_rules.cpp`）が `touched` =
      述語列 ∪ 出力列を **複数インデックス** で被覆できる場合に
      `MultiIndexCoveringScanPlan`（`plan/multi_index_covering_scan_plan.*`）を
      候補に出す。各参加者は先頭キー列の範囲を1つだけ持ち（範囲のない
      インデックスは参加者にしない）、最小被覆は貪欲法で選び、
      他の参加者が既に被覆する列しか持たない参加者は削除する（1本だけは
      index-only 経路の担当なので残さない）。保持削除を行う
      `kVersionedUnique` は参加者にしない（エントリが committed image で
      ない）。`executor/multi_index_covering_scan.*` が行位置で交差を作り、
      エントリの行ページにバージョン鎖が無ければ index image だけで行を
      組み立て（`IndexScanIterator::TryReadIndexOnly` と同じ可視性根拠）、
      鎖があれば heap read に落とす。**計画キャッシュが literal を
      ParameterSlot に書き換えるため、参加者の seek キーは emit 時に
      bound 済み述語から `ReboundIndexKeysFromPredicate` で張り直す**
      （`IndexOnlyScanPlan::EmitExecutor` と同じ契約。これが無いと SQL 経路
      では常に full scan にフォールバックして dead code になる）。先頭キーの
      bound を失った参加者が1つでもあればノード全体を full scan に落とす。
      read guard は全参加者を index root 昇順で一括取得（連結した取得順が
      writer starvation のデッドロックを生まないため）。回帰:
      `executor/multi_index_covering_scan_test.cpp` 9件（EXPLAIN 形状 /
      フルスキャン結果一致 / recheck が交差を狭める向き /
      NULL キー / 単一 covering は index-only のまま / 保持インデックスは
      不参加 / UPDATE 後の新イメージ / 削除済み行 / 直接実行器の交差のみ）。
      既知の残り: 交差表は `BitmapHeapScan` と同じく位置列を全件メモリに
      持つ（両経路共通の予算化は別項目）。
- [x] NestedLoopIndexJoinのprefetchを実装する。
- [x] 内側filterをindexへ適用できない場合にheap側へ残す契約を明示する。
      `LookupJoin` は右の子 plan を完全にバイパスして生テーブルを index で
      probe するため、以前は「右の scan group に filter があるなら index join
      形状を出さない」という保守ゲートで応えていた。契約を改めた: **index が
      表現できない内側 filter は拒否理由ではなく、residual が運ぶ conjunct**。
      `JoinAlternatives`(`plan/implementation_rules.cpp`)で
      (a) ゲートを撤去し (b) residual の供給元を「probe が確定しなかった
      join 述語 conjunct」だけに狭めず、**probe がバイパスする右の子 plan に
      押し込まれた conjunct と、内側 relation の scan group filter** も加えた
      (ToString で重複排除)。
      既に適用済みという印 `pushed` は**左の子だけ**(=この実行器が消費する子)。
      結合結果は右の全カラムを持つので、内側 conjunct はそこで元の意味のまま
      評価される。付け加えるのは元の WHERE の conjunct だけなので、誤って
      含める方向にしか動かない(安全)。
      回帰: `OptimizerTest.FilteredSelfJoinStillPlansThroughTheIndexNestedLoop`
      (hash/NL を無効化して probe 形状を強制し、`b.c3 > 107` が結果を決める
      ことを固定。旧ゲートを戻すと「Index Join なし」で落ちる)。
      既知の残り: 内側 filter の conjunct を probe の seek range へ出す
      (定数 bound を先頭キーへ載せる)最適化は未実装 —— 現状は全部 residual。
- [x] unique内側の1行lookup短絡を実装する。
      短絡の扉 `LookupJoin::at_most_one_` 自体は既にあったが、契約も回帰も無く
      「探せばある」状態だったので、条件・安全性・監査点を確定させた。
      **発火条件**: seek が single-value unique 索引の**完全キー**への点 probe
      であること（`!range_position_` かつ `StoresSingleValue()` かつ
      `left_cols_.size() == index.sc_.key_.size()`）。このとき一致した候補は
      そのキーが指し得る唯一の行（`kUnique` は NULL 以外のキーを単一値で持ち、
      重複挿入は `BPlusTree::Insert` が `kDuplicates` で弾く）なので、索引
      カーソルを進めず外側カーソルを1つ進める。
      **安全性の要件を1つ足した**: 短絡してよいのは**一致した候補だけ**。
      `BuildSeekRange` は使えない bound を前置走査へ広げる契約（「広がるだけで
      誤った行は通さない」）なので、最初の候補で打ち切るとその fallback が行の
      取りこぼしに変わる。一致しなかった候補は通常の走査へ落とし、`Matches` を
      唯一の判定者に保つ（現状の点 probe では未達だが、契約として固定）。
      形状は `AtMostOneProbe()` と実行器 dump の `at-most-one` 表示で監査可能。
      回帰: `ExecutorTest.LookupJoinUniquePointProbeShortCircuits` /
      `...UniquePointProbeHandlesRepeatedOuterKeys`（外側キー重複時に
      hold を捨てて再 probe する状態機械）/ `...NonUniqueInnerKeepsTheFullMatchSet` /
      `...UniqueCompositeKeyNeedsTheWholeKey`（前置だけなら短絡しない）/
      `...RetainedUniqueIndexIsNotASingleValue` の5件、および SQL
      `QueryTest.UniqueInnerJoinEmitsOneRowPerOuterRow`（外側キー重複・内側に
      無いキー・COUNT の3方向）。扉を殺す変異で3件が落ちることを確認。
      既知の残り: `kVersionedUnique` は保持削除でキー1つに複数 position が
      並び得る（挿入は一意性を強制しない）ため短絡しない。連続する重複
      外側キーで probe を作り直さない locality 最適化は次項。
- [x] ソート済み外側からのlookup localityをcost化する。
      `JoinAlternatives` の index nested-loop 局所コストは
      `l_rows * 2.0 + equi`（B+Tree probe 1回 + 行の組み立て1回）で、外側が
      probe キー順に来ていても何も変わらなかった。probe 側だけを
      `DeliversProbeOrder(left.plan, probe->left_columns)` で割り引く:
      `l_rows * (ordered ? 0.75 : 1.0) + l_rows + equi`。外側が probe の
      キー順（索引キー順の左カラム列）を昇順で供給しているとき、連続する
      probe は直前のリーフか隣のリーフに着地するので、根からの下降路と
      リーフページが buffer pool に残る。組み立て側と返る行は順序に依存しない
      ので割り引かない。`IsOrderedBy` は修飾/非修飾の両表記を解決するため、
      probe の left カラム名をそのまま渡せる。
      **measured order（clustered scan の主張）も対象**にする: 「順序を得るのに
      鎖を最後まで歩いた」という `OrderIsMeasured` の理屈は『順序を*得て*
      いない』ことの理由にはならず、probe には実際に昇順キーが届く
      （その主張を含む文はコンパイル済み計画キャッシュから除外済み）。
      回帰: `OptimizerTest.OrderedOuterDiscountsTheIndexProbeIntoAWin`。
      同じ100行の外側を2つ用意し、片方だけ join キーに索引を持つ（=外側が
      索引順を供給できる）。フィルタ後の外側20行・内側37行では、順序ありの
      probe 55 が最良の hash 向き 57 を下回り、順序なしの 60 は負けるので、
      **選ばれる形状そのものが割引の証拠**になる（`Index Join` の有無）。
      両者の出力行は一致（割引は形状だけを変える）。係数を 1.0 に戻す変異で
      落ちる。
      既知の残り: 子グループに「probe キー順」を要求していないので、割引は
      「たまたま最安の外側計画が順序を供給する」ときだけ効く。結合の子に
      interesting order を要求する仕組みは未実装。リーフヒント
      (`BPlusTree::Read` の `hint_leaf`) を read 経路へ通す実装も未着手で、
      現状の効果はキャッシュ局所性のみ。

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
- [x] Windowのhash partition + sortを実装する。
      `BuildWindowOrderLayout` は PARTITION BY のグループを「見つけた順に並んだ
      代表キーを線形走査」で決めていた（行×グループ）。代表キーのタプルを
      `std::unordered_map`（キー `std::vector<Value>`）で引くよう変更:
      グループの生成順は初出順のままなので、`partitions` と出力位置は不変で、
      探索だけが O(1) になる。内側の並べ替え（`std::stable_sort` + 事前評価した
      `order_values` と peer 境界）はそのまま。
      **ハッシュと等価性の契約**を明示: 等価判定は `ValuesEqual`（NULL 同士は
      同一パーティション、NaN は1つの peer group、interval 風テキストは interval
      として比較）で、ハッシュは `std::hash<Value>`（同じく NULL/NaN を
      正規化し、interval テキストを parse してハッシュする）。等しい値が
      等しくハッシュされる方向さえ守ればよく、衝突（INT64 と UINT64 が同じ
      ビット列）は `PartitionKeyEq` が弾くので無害。
      回帰: `WindowSqlTest.InterleavedPartitionsKeepTheirOwnFrames`（400行/50
      グループを非連続に配置し、各区画が自分のフレーム全体を見ることを固定）と
      `WindowSqlTest.NullAndNanPartitionKeysEachFormOneGroup`（NULL 3行が1区画・
      NaN 2行が1区画）。グループを再利用しない変異で両方が落ちる。
      既知の残り: 次項の Window spill（区画とソート状態のメモリ予算化）。
- [x] Windowのspillを実装する。
      `WindowPlan` に `WindowMemoryPolicy{memory_budget_bytes,
      spill_partition_count}` を追加し、`window` 実装ルールが子の行推定から
      `WindowPolicyForRows(rows)` で選ぶ（既定64MB ÷ 64byte/行 ≒ 104万行）。
      推定が容量以下なら従来どおり **buffered**（全行常駐・入力順を保存）。
      超えると **spill 可能**（spill-capable）: 入力の drain 時に PARTITION BY
      タプルのハッシュで `spill_partition_count` 個の `SpillFile` へ振り分け、
      1ファイルずつ常駐で評価・出力する。常駐は最大ファイル1つで、入力全体では
      ない。ハッシュは `detail/window_eval` の区画化と `WindowPartitionKeyHash`
      を共有し（一方向整合 = 等しいキーは等しいハッシュ）、区画全体が必ず
      1ファイルに収まる。単一区画が予算を超える場合、そのファイルは常駐する
      （上限は「区画1つ」ではなく「ファイル1つ」）。
      **順序契約**: buffered は子の行順を保つので子の ordering claim を
      引き継ぐが、spill 形はファイル順に出力するので claim を捨てる。claim は
      実行時の行数を知る前に決まるため、容量を超え得る推定には予算を渡して
      claim を落とし、過小推定でも claim が嘘にならない側にだけ倒す。
      **分割条件**: 全 window 呼び出しの PARTITION BY 式リストが同一のときだけ
      spill 可能。呼び出しごとに違うと、あるキーで選んだファイルが別呼び出しの
      区画を切ってしまうので、その場合は buffered のままにする。
      回帰: `plan/window_shape_test.cpp`（ポリシー選択・claim・分割条件・Dump）と
      `executor/window_spill_test.cpp`（spill と buffered の行・値の一致、
      resident 上限、batch 境界、PARTITION BY 不一致の fallback、
      end-of-stream の粘着性 = 終端後に引き続き Next しても再送しない）。
      「区画を order key で切り直す」変異で2件、「全行を1ファイルへ」変異で
      `SpillFileCount`/`PeakResidentRows`、「終端で `read_offset_` を巻き戻す」
      変異で粘着性テストが落ちることを確認（この巻き戻しは QUALIFY 経路の
      無限ループとして実害が出た）。
      既知の残り: 単一区画が予算を超える場合の区画内 spill（frame 評価の
      ストリーム化が要る）。
- [x] `FunctionScan` / TVFを実装する。
      FROM の組み込みテーブル関数 `GENERATE_SERIES(start, stop[, step])` を受ける。
      エンジンの `GENERATE_SERIES` は ARRAY を返すスカラー関数なので、呼び出しは
      **その配列の暗黙 UNNEST** に落とす（`SelectSource.unnest` を立てる）:
      リレーショナル経路は `LoadSource` が配列を行源化し、Cascades 経路は現在の
      入力グループに対する `kUnnest` になる（専用スキャン演算子は増やさない）。
      したがって専用の `FunctionScanPlan` は持たず、`UnnestPlan` / `kUnnest` が
      実行契約を共有する。列名は BigQuery の TVF 出力に合わせて未別名なら
      `value`、別名があれば UNNEST と同じく別名。名前付き引数
      (`START => 1, STOP => 7, STEP => 3`) は位置引数へ並べ替え、重複・未知の
      引数名・引数不足・過多は文エラー。`FROM a JOIN GENERATE_SERIES(...) ON ...`
      は Join のオペランドとして扱い、引数は先行ソース列を参照できる（暗黙
      UNNEST と同じ lateral 契約）。`TABLESAMPLE` は UNNEST と同様に拒否。
      `CREATE TABLE FUNCTION` の TVF 展開（既存）と同じ `TVF` AST 節から分岐する。
      回帰: `QueryTest.BuiltinTableFunctionBecomesRowSource`（列名・名前付き
      引数・別名・テーブルとの lateral join・`JOIN ... ON`・`INSERT SELECT`・
      EXPLAIN が `Unnest: generate_series(...)` になること・引数エラー5種）。
      付随修正: `QueryTest.UnnestAnonymousOrderByKeyRidesAHiddenColumn`。
      無名 select item を `ORDER BY <番号>` で並べると、ソートキーが射影の上で
      元式を再評価して UNNEST 値列を見失う（`column x not found`）バグを、
      隠し `$order<i>` 射影列に載せる形で修正。
      残り: `EXTERNAL_QUERY` / `ML.*` / `APPENDS` など外部・時系列系 TVF は
      外部ソース基盤が前提なので対象外。
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
- [ ] WorkTableIndexを実装する。
- [ ] CTE materialization / inlining / filter pushdownをコスト付きで選択する。
- [x] CTE predicate propagationを実装する。
- [x] recursive CTE union rewriteを実装する。
- [x] 再帰CTEのmemory capとspillを実装する。

### DML / Lock

（以下はリレーショナル経路で実装済み: Insert, Update, Delete, BatchInsert, Upsert/ON CONFLICT, TableTriggers）

- [x] Upsert / ON CONFLICTを実装する。
- [x] MATCHED / NOT MATCHEDのMergeExecutorを実装する。
- [ ] UPDATE FROM / DELETE USINGを実装する。
- [x] ReturningExecutorを実装する。
- [ ] LockRows / SELECT FOR UPDATEを実装する。
- [ ] SkipLocked / NOWAITを実装する。
- [x] Truncate executorを実装する。
- [x] before / after trigger hookを実装する。
- [ ] DML logical / physical planを導入し、INSERT / UPDATE / DELETEをCascadesへ接続する。

### Sampling / 実行時統計

- [x] AnalyzeScan（ヒストグラム構築用）を実装する。
- [x] ReservoirSampleを実装する。
- [x] HyperLogLogによる統計収集(AnalyzeScan)を実装する。
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

（以下は実装済み: join_commutativity, join_enumeration, join_associativity_left/right, infer_join_predicates, push_selection_through_join, split_selection_over_join, join_to_cross_if_no_predicate, merge_adjacent_filters, push_filter_through_left_join_left_side, join_on_false_to_empty, cross_to_inner_with_predicate, join_empty_simplification, self_join_elimination, unique_semi_to_inner, outer_to_anti_join, right_to_left_outer_join, full_outer_join_decomposition, push_down_limit_through_join, dynamic_filter_pushdown_join, join_predicate_transitivity, inferred_inequality_pushdown, redundant_join_predicate_elimination, outer_to_inner (relational)）

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

（以下はExpressionRuleSetに実装済み: fold_binary/unary/in/function, singleton_in, canonicalize_comparison, boolean_identity, double_negation, de_morgan, simplify_case, not_comparison/not_like/not_is_null/not_is_not_null, xor_boolean_identity, and/or_idempotent, absorption_and/or, identity_add/subtract_zero, identity_multiply/divide_one, double_negation_arithmetic, reassociate_add/subtract_constants, dedupe_in_list, uniform_case_result, like_equality, is_null/is_not_null_of_null_check, collapse_nested_identical_cast, factor_or_common_and, nullif_to_case, self_inequality, contradiction_from_null_eq, greatest_least_fold, in_single_null, concat_flatten, xor_to_or_and_not, is_distinct_from_rewrite, boolean_eq_true_false_three_valued, abs_of_abs, empty_in_list, coalesce_and_nullif_simplification, datetime_and_string_fold_extent, if_to_case, json_path_constant_fold, numeric_widening_cast, or_of_ranges_to_in, interval_normalize, predicate_pushdown_case, inner_join_not_null_inference, regexp_prefix_extraction, cast_pushdown_comparison, deterministic_function_cse, boolean_filter_pullup, not_in_null_semantics, array_flatten_optimization）

- [x] numeric widening cast簡約とDATE/TIMESTAMP cast正規化を実装する。
- [x] NOT IN with NULL listの警告・計画を実装する。
- [x] interval normalizeを実装する。
- [x] JSON path constant foldを実装する。
- [x] projection以外も含むdeterministic function CSEを実装する。
- [x] stable / immutable / volatileの関数分類を実装する。
- [x] OR of ranges to INを実装する（空INクラッシュ回避、非スロー境界、評価順・短絡例外保存）。
- [x] DATE_ADD/SUB、SUBSTRING等のfold適用範囲を回帰テストで固定する。
- [x] NOT BETWEENを実装する。

### Aggregate変換規則

（以下はCascadesルールとして実装済み: push_selection_through_aggregation, distinct_over_group_by, unique_group_key_aggregate_elimination, aggregate_union_transpose）

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
- [x] index clustering factor(correlation)のモデル化は済。
- [ ] page cache hit率のモデル化。
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
      （HashAgg の partition spill は 1 段だけ: 1 partition の group table が
      予算内に収まることを `spill_partition_count` に任せている。偏った key
      分布で収まらない場合の多段 spill は未実装 —
      `executor/grouped_aggregate_test.cpp` の
      `APartitionedHashBuildHoldsOnePartitionsGroups` がその現状を固定。）
- [ ] statement timeoutのcancelを全operatorへ伝播する。
- [x] memory不足時のgraceful degradeを実装する。
- [x] distributed any / singleton / hash / broadcast / rangeを実装する。
- [ ] enforce distribution、colocated join、broadcast vs shuffle costを実装する。
- [x] partial agg/finalize、two-phase distinctを分散経路へ接続する。
- [x] window partition再分散、scan range分割を実装する。
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
- [x] MERGE、RETURNINGをSQLからplanへ接続する。
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
- [x] buffer pool cache hitを実行統計へ追加する。
- [x] MVCC snapshot visibilityをIndexOnly / Bitmapで検証する。
- [x] cancel途中のresource leakを検証する。
- [x] Sort / Limit / Distinctのengine safety net二重適用を検証する。
- [x] 全ruleの代数テストを追加する（`expression_rewrites.test`, `executor_expectations.test`, `unsupported_future_features.test` 追加）。
- [x] random rule subset fuzzingを追加する。
- [x] outer join null-pad goldenを追加する。
- [x] TPC-H Q8/Q9 join order goldenを追加する。
- [x] 関係fallbackとCascadesの差分テストを追加する。
- [x] cost monotonicityを近似検査する。

---

## 完了条件

1. このファイルに残る各項目について、対応するコードと回帰テストがある。
2. `python3 scripts/check_layering.py` が成功する。
3. ビルドが成功し、既存テストを削らずに全テストが成功する。
4. GoogleSQL complianceで実装対象として宣言した機能に失敗がない。
5. 項目を完了したときは、このファイルからその項目を削除し、テスト名または
   実装契約をコミットメッセージに残す。
