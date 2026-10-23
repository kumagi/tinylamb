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
- 再帰CTE基本、UNION DISTINCT、依存CTE、BY NAME、DEPTH（`SqlEngineRecursiveCteByNameUnionDedupesAlignedRows`, `SqlEngineRecursiveCteNameMatchingAlignsEveryUnionMode`, `SqlEngineRecursiveCteDepthModifierBoundsIterations`）
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

（以下はリレーショナル経路で実装済み: ExecuteRecursiveCte, CteMap, WorkTableScan via CteMap, MaterializePlan/SpoolExecutor, CteScanPlan, WorkTableScanPlan, RecursiveUnionPlan/Executor, WorkTableIndex, 共有CTEの制約押し下げ (PushSharedCteRestrictions), RecursiveUnionSpill）

- [x] `MaterializePlan` / `EagerSpool` をCascades専用planとして実装する。
- [x] `LazySpool` / `ConsumerSpool` をCascades専用planとして実装する。
- [x] `CteScan` をCascades専用planとして実装する。
- [x] `RecursiveUnion` をanchor / delta / seen set付きでCascades専用planとして実装する。
- [x] WorkTableScanをCascades専用planとして実装する。
- [x] UNIONとUNION ALLの再帰差分を実装する。
- [x] 再帰のサイクル検出、深さ上限、累積行予算を実装する。
- [x] `WITH DEPTH` の列生成、範囲表示、UNBOUNDEDを実装する。
- [x] recursive JOIN USING、BY NAME、CORRESPONDINGをschema契約として実装する。
      契約: 再帰CTEの列は常に anchor 項が決める（各ラウンドは anchor の列を
      作らなければならない。`SELECT * FROM <cte>` が anchor の名前で固定される
      理由）。集合演算メタデータは項ごとの `SetOperationMatch` を持ち
      （`SelectStatement::MatchFor(term)` で解決。visitor が `BY NAME` /
      `STRICT CORRESPONDING` → `by_name`、`CORRESPONDING` → `corresponding`、
      `ON/BY (col_list)` → `columns` を per-pair で記録する）、名前一致は
      値の並びだけを変える: `BY NAME` / `STRICT CORRESPONDING` は項の列名集合が
      anchor と完全一致することを要求、素の `CORRESPONDING` は同名一致＋項だけの
      列を落とす、`ON/BY (col_list)` は列挙列だけを一致対象にし、working shape を
      覆う必要がある（anchor 列を落とす指定はエラー）。anchor 列を項が作れない /
      重複名 / 無名列は NULL 埋めせずエラー、位置一致の項は anchor 幅を要求。
      `WITH DEPTH` の depth 列は整列後の末尾。回帰:
      `SqlEngineRecursiveCteNameMatchingAlignsEveryUnionMode`,
      `SqlEngineRecursiveCteNameMatchingKeepsDepthLast`,
      `SqlEngineRecursiveCteJoinUsingFeedsNameMatching`,
      `SqlEngineRecursiveCteNameMatchingRejectsMismatchedColumns`。
      既知のギャップ: 非再帰の集合演算（`UNION ALL BY NAME` /
      `CORRESPONDING`）は依然として位置一致（`docs/next-actions.md` 第42報）。
- [x] CTE依存順、前方参照、相互再帰エラーを実装する。
      契約: WITH のエントリは宣言順ではなく依存順に解決する（前方参照可）。
      依存の走査は深い（派生表・スカラーサブクエリ・集合演算の枝・入れ子 WITH の
      本体まで。`executor/detail/subquery_runtime` の
      `CollectStatementTableRefs` / `CollectCteBodyOuterRefs`。従来は直下の
      FROM しか見ず、`WITH b AS (SELECT * FROM (SELECT * FROM a)), a AS (...)`
      が "table a not found" になった）。非再帰エントリは自分の定義内では
      不可視で、自己参照は外側（外側 WITH またはカタログ表）へ解決する
      （コンプライアンス `with_on_multiple_levels` の nested WITH 規則と同じ。
      再帰エントリの自己参照だけが work table を読む）。よって相互参照
      （`a→b→a`）だけが循環で、`ValidateCteDependencies`（`PrepareStatement` の
      kSelect 入口）と実行器の CTE ループが同じ
      "cyclic CTE reference among a, b" を返す。実行器側は
      手組み Statement 向けの backstop。再帰CTEの本体が兄弟 CTE を読む場合は
      兄弟をスコープに持たない Cascades の work-table leaf へ流さず、
      `RecursiveCteLiftable` ゲートで依存順リレーショナル経路へ落とす。
      回帰: `SqlEngineCteDependencyOrderFollowsNestedReferences`,
      `SqlEngineRecursiveCteReadsSiblingCteInDependencyOrder`,
      `SqlEngineCyclicCteReferenceRejected`。
- [x] plan template / cache再構築時にrecursive metadataを保持する。
      契約: パース済みテンプレートキャッシュ（`FindTemplate` /
      `RememberTemplate`、キーはリテラル非依存の fingerprint）の再構築は
      `SqlTemplate::BindSelect` が担い、テキスト順にパラメータを消費しながら
      再帰フラグ（`AddRecursiveWithQuery`）、`RecursiveDepthSpec`、枝ごとの
      `SetOperationKind` / `SetOperationMatch`、`SetOperationTree` を保持する。
      従来はキャッシュに載る前に 2 つの理由でバインドが失敗していた:
      (a) 集合演算は flat な `UnionAll()` と `SetOperationTree` の二表現を
      同じ枝ノードで共有するが、両方をバインドして全リテラルを二重消費し
      "parameter underflow" になっていた（再帰CTEの本体は必ず集合演算なので
      再帰CTEは一切キャッシュされなかった）。flat 側で 1 回だけバインドし、
      tree は同じ枝を共有、`first` はヘッドが消費したパラメータ範囲から複製する。
      (b) `WITH DEPTH ... BETWEEN <lower> AND <upper>` の上下限は式ではなく
      CTE メタデータに載るが、テキスト上は整数リテラルなので抽出器が
      パラメータとして数えていた。`RecursiveDepthSpec` の
      `lower_is_literal` / `upper_is_literal` で区別し、本文の直後
      （メイン SELECT の前）で消費して新しい値を書き戻す。`UNBOUNDED` 側は
      パラメータを消費しない。複数エントリの WITH にリテラルの深さ上限がある
      場合は宣言順を復元できないのでテンプレート化を拒否する（本文リテラルと
      同じ扱い）。キャッシュが実際に効いたかは
      `PlanCacheStats().template_fills` / `template_hits` で観測できる。
      回帰: `TemplateCacheKeepsRecursiveCteMetadata`,
      `SqlTemplateTest.BindsRecursiveWithSetOperationOnce`,
      `SqlTemplateTest.BindsRecursiveWithDepthBounds`。
- [x] WorkTableIndexを実装する。
      契約: 再帰CTEの作業テーブル（累積結果）に対するハッシュ索引
      `WorkTableIndex`（`executor/detail/work_table_index.{hpp,cpp}`）。
      `UNION DISTINCT` 再帰の「前のラウンドまでに同じ行が出たか」を 1 回の
      ハッシュ参照で判定する。従来は既出全行の線形走査で、結果サイズに対して
      二次だった（320001 行を生成する再帰が 3分17秒 → 0.27 秒）。
      ピア規則は DISTINCT と同一（`EqualUnderCollation` +
      `CanonicalDistinctValue`: NaN は 1 ピア、-0.0 は +0.0 と同値、
      大文字小文字非依存の照合は畳み込み、NULL は NULL 同値、幅・型は区別）で、
      `DistinctOf` も同じ実装を共有する。**これは結合アクセス経路ではない**:
      CTE 名は前ラウンドのデルタに束縛される（BigQuery の working-table 契約）
      ため、各ラウンドの結合は join 演算子がそのデルタをハッシュ化し、再帰項は
      累積テーブルを参照できない。ループ自身が索引を必要とするのは
      「どの行が新規か」であり、そこを担う。`WorkTableIndexStats()` の
      `inserts` / `duplicates` でループが索引を引いたことを観測できる。
      回帰: `WorkTableIndexTest.*`（8 件）,
      `SqlEngineRecursiveCteDedupesThroughWorkTableIndex`。
- [x] CTE materialization / inlining / filter pushdownをコスト付きで選択する。
      契約（`SqlEngine::PrepareStatement` の CTE 前処理パイプライン。
      `InlineSingleUseCtes` → `PushSharedCteRestrictions` → `MaterializeCtes`
      → `FlattenDerivedSources`、この順序が意味を持つ）:
      (1) **単一参照の非再帰 CTE はインライン**する（`InlineSingleUseCtes`:
      派生表へ置換し、本体は 1 回しか走らないので materialize より弱く劣化
      しない）。置換後は M5 の派生表フラット化が効くため、利用側の WHERE は
      本体のスキャンまで降りる（`EXPLAIN ANALYZE` の `scan_output_rows` が
      全行走査でなく絞り込み後になる）。
      (2) **複数参照の CTE は 1 回 materialize**する（`MaterializeCtes`:
      共有セル 1 個・`kValues` 葉）。
      (3) **共有 CTE への filter pushdown は「利用側制約の選言」だけ**を押す
      （`PushSharedCteRestrictions`）。単一利用側の述語をそのまま押すのは
      不正（他方の利用側が必要とする行が消える）が、全利用側の制約の選言は
      「どの利用側も出し得る行」を過不足なく残す。ゲートは、
      再帰エントリでない / 参照箇所が 2 以上ですべて「この文または兄弟 CTE
      本文の FROM 直下の平テーブル源」/ 結合種別が CROSS・INNER（NULL 拡張側
      だと除去が padding に化ける）/ 派生表・副問合せ・集合演算分岐に参照が
      無い / 各参照箇所に自分自身の別名だけを見る制約が最低 1 つある（無い
      箇所があると選言が TRUE）/ 本体が「1 平テーブルからの無改名の列投影」
      で GROUP BY・DISTINCT・LIMIT/OFFSET・QUALIFY・window・集合演算・
      副問合せ・USING・ON を持たない（別名を剥がしても本体の列名に戻る）/
      コスト推定 `TableStatistics::EstimateSelectivity` が選言で 1.0 未満に
      なる（統計が無い = Rows()==0 なら押さない）。判定と書き換えは
      `SqlEngine::LastCteRestrictionPushdowns()` で観測でき、押した場合は
      共有セルが実際に縮む（`EXPLAIN ANALYZE` の葉の `rows_evaluated`）。
      回帰: `SqlEnginePushesSharedCteRestrictionsDisjunctively`（選言・
      異なる制約同士・統計ゲート・無制約箇所・副問合せ箇所・LIMIT 本体・
      セル縮小のプラン観測）。
- [x] CTE predicate propagationを実装する。
- [x] recursive CTE union rewriteを実装する。
- [x] 再帰CTEのmemory capとspillを実装する。

### DML / Lock

（以下はリレーショナル経路で実装済み: Insert, Update, UpdateFrom(MERGE脱糖), Delete, BatchInsert, Upsert/ON CONFLICT, TableTriggers）

- [x] Upsert / ON CONFLICTを実装する。
- [x] MATCHED / NOT MATCHEDのMergeExecutorを実装する。
- [x] UPDATE FROM / DELETE USINGを実装する。
      `UPDATE ... FROM` は実装済み（フロントエンドが
      `MERGE INTO t USING src ON <WHERE> WHEN MATCHED THEN UPDATE SET ...` へ
      脱糖し、`DesugaredFromUpdate()` で UPDATE 由来と分かる。診断は
      `Update Rows` / `StatementType::kUpdate`、`EXPLAIN` も UPDATE として出る）。
      契約: SET 値はソース別名経由でソース列を参照でき、FROM 副問合せは別名必須
      （無い場合は AST 段階でエラー）。1 対象行に複数ソース行が一致した場合は
      BigQuery と同じく拒否する（"MERGE attempted to UPDATE or DELETE the same
      target row more than once"）。
      `DELETE ... USING` は **GoogleSQL に構文が無い**（フロントエンドの
      パーサが `USING` で syntax error にする）ため実装対象外とし、等価な
      綴りを契約にする: `DELETE FROM t WHERE EXISTS (SELECT 1 FROM u WHERE
      u.k = t.k)` と `DELETE FROM t WHERE k IN (SELECT k FROM u)`。前者は対象行が
      複数ソース行に一致しても 1 回だけ削除する（UPDATE FROM の重複一致エラーとは
      規則が違う）。なお AST に `FromClause` を持つ DELETE が来た場合は句を
      黙って落とさず「USING は GoogleSQL に無い + 相関サブクエリで書く」と
      明示エラーにする（句を捨てると JOIN の無い WHERE で誤削除になる）。
      回帰: `MergeTest.UpdateFrom*`（5 件）,
      `SqlEngineDeleteJoinsThroughCorrelatedSubqueries`,
      `GoogleSqlAstTest.DeleteRejectsUsingClauseInsteadOfDroppingIt`。
- [x] ReturningExecutorを実装する。
- [x] LockRows / SELECT FOR UPDATEを実装する。
      GoogleSQL 唯一のロック句 `FOR UPDATE`（パーサの `LockMode` ノード）を
      `SelectStatement::ForUpdate()` として取り込み、**DML ソース契約を再利用**して
      実装した: `QueryData::require_row_position_=true` → プラン要求
      `row_position=true` → スキャンが `lock_rows` になり、訪れた行を
      読み出し前に write-intent で予約する（`txn.AddWriteSet`/`TryAddWriteSet`）。
      予約はトランザクション終了（commit/abort）まで保持され、他トランザクションの
      同じ行への予約を拒否する。
      契約:
      - WHERE はロックの**後**に評価される（UPDATE/DELETE のソースと同じ規則）。
        したがってインデックス範囲で絞れない述語ではスキャンが訪れた行
        （フルスキャンならテーブル全体）をロックする。安全側（取りこぼしが無い）
        だが PostgreSQL の「一致行だけ」より広い。索引範囲が取れる形なら
        その範囲に限定される。
      - ロック対象はスキャンが解決した head（最新コミット）版なので、
        「古い版をロックして新しい版を返す」状態にはならない。
      - 単一基底テーブル・JOIN/集計/DISTINCT/GROUP BY/HAVING/window/集合演算/
        CTE/派生表/サンプリング無しの形のみ受理。行位置を持たない形
        （集計・JOIN・集合演算）は**明示エラー**にする（黙ってロック無し実行に
        しない）。relational 経路に落ちる形も row position を運べないのでエラー。
      - `BindSelect` はテンプレート再束縛でもフラグを保持する
        （`SqlTemplateTest.BindSelectPreservesForUpdate`）。
      - `FOR SHARE` / `NOWAIT` / `SKIP LOCKED` は GoogleSQL の文法に無く
        パーサが拒否する（次項の課題を参照）。
      回帰: `QueryTest.SqlEngineForUpdateLocksScannedRows`（ロック取得・
      非ロック読みとの差・abort 解放・再実行でもロック）,
      `QueryTest.SqlEngineForUpdateRejectsUnlockableShapes`（集計/GROUP BY/
      DISTINCT/JOIN/CTE の拒否）,
      `SqlTemplateTest.BindSelectPreservesForUpdate`。
      変異（visitor のフラグ設定削除 / `require_row_position_` 代入削除 /
      形ゲート無効化 / 再束縛のフラグ複写削除）はすべてテストで kill 確認。
- [x] SkipLocked / NOWAITを実装する。
      GoogleSQL には `NOWAIT` / `SKIP LOCKED` の**文法が無い**（パーサが
      `FOR UPDATE` の後で syntax error にする）ので SQL からは指定できない。
      そのため **NOWAIT はエンジン API として実装**した:
      `SqlEngine::SetForUpdateWaitMode(ForUpdateWaitMode::kNoWait)`。
      `kNoWait` は `QueryData::wait_for_write_intent_=false` に落ち、スキャンが
      `TryAddWriteSet` で予約する（DELETE のソーススキャンと同じ非ブロッキング
      方針）。競合した時点で文が即座に `kConflicts` で落ちる（既定の `kWait` は
      5 秒上限のブロッキング）。待機方針はプランに焼き込まれるので、
      **fingerprint に方針を混ぜる**（`SELECT ... FOR UPDATE` のテキスト +
      `kNoWait` のときだけ接尾辞）。これが無いと `kWait` で埋めた
      コンパイル済みプランを `kNoWait` のエンジンが再生して 5 秒待つ。
      回帰: `QueryTest.SqlEngineForUpdateNoWaitFailsFast`（`kWait` の
      プランキャッシュを埋めた後に同一テキストを `kNoWait` で実行し、
      2 秒未満で `kConflicts`、行は返らない）。
      変異（fingerprint の接尾辞削除 / `wait_for_write_intent_` を常に true）は
      いずれも 5 秒待ちになってテストが落ちることを確認。
      **SKIP LOCKED は実装しない**（スキップ判断）。理由: (1) 文法が無く
      エンジン API にしか出せない、(2) 本エンジンのロック取得は DML ソース契約で
      「ロックしてから WHERE」なので、ロック済み行を飛ばすと**述語に一致する行まで
      結果から消える**（部分的に空の結果が返る）。意味を保つには
      「スキャンが述語評価後にロックし、失敗行だけ飛ばす」モード
      （新しいスキャンロックモード + plan property の追加）が必要で、
      SQL 面が無いまま property 配管だけ増えるため見送る。将来 `FOR UPDATE`
      に文法が入ったら `ScanOptions` に三値モードを足すのが出発点
      （`docs/lock_order.md` の 4 番、LockManager 行ロックの層）。
- [x] Truncate executorを実装する。
- [x] before / after trigger hookを実装する。
- [ ] DML logical / physical planを導入し、INSERT / UPDATE / DELETEをCascadesへ接続する。
      **スキップ（判断理由）**: 部分は既に接続済みで、残りは Cascades の
      memo/rule を拡張する大規模リファクタになる。現状:
      - UPDATE / DELETE は対象テーブルを一時的な `QueryData`
        (`require_row_position_=true`, `where_`, 出力列) に落とし、
        **Cascades の `Optimizer::Optimize` でソース側のアクセスパスを選ばせて**
        から `UpdateExecutor` / `DeleteExecutor` で変更を適用する。
        つまりソース選択（index / full scan / filter 順 / row position）は
        Cascades の担当で、変更適用だけが手書き executor。
      - INSERT ... SELECT はソース問合せを materialize し、
        INSERT VALUES は plan を必要としない。
      - MERGE / TRUNCATE / nested array update / struct field update は
        文固有の前処理（sql_engine 内）を通る。
      残っているのは「DML 自体を `LogicalOperator::kInsert/kUpdate/kDelete` +
      物理 `DmlPlan` として memo に入れ、ソースと一緒に探索・コスト比較する」
      部分で、必要な作業は:
      1. `LogicalOperator` に DML 3 種を追加し、`LogicalScan` 側の
         memo グループ化（`InsertLogicalToPhysical` 相当の規則）を書く。
      2. row position と write-intent 方針を `PhysicalProperties` の
         制約として扱い、`DmlPlan` が `require_row_position` を子に要求する形に
         する（現状は QueryData 経由で暗黙に立てている）。
      3. 対象テーブルへの適用（SET 式評価、制約・トリガ・RETURNING・
         ASSERT_ROWS_MODIFIED・値テーブルの別名束縛）を plan ノードの
         データに移す。これは sql_engine の kUpdate/kDelete/kInsert 分岐で
         文あたり 300〜400 行あり、移設は回帰リスクが高い。
      4. コスト比較（index 経由の点更新 vs スキャン + 適用）を定義する。
      SQL 上の観測結果は現状で変わらない（EXPLAIN も UPDATE/DELETE 非対応のまま）
      ため、**この項目はペンディング**とし、`docs/next-actions.md` に
      着手点と分割案を残した。

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
- [x] Calc merge / split方針を決めて実装する。
      **方針: Calc ノードは導入しない**（merge は既存規則 + スキャン側の融合、
      split は転置規則で達成する）。理由:
      - `Selection` も `Projection` も既に `NextBatch` でバッチ単位に評価する
        （Selection は zone-map / SIMD 比較 / JIT filter、Projection は CSE /
        bytecode / JIT）。1 ノードに融合して節約できるのはバッチ境界のコピー
        1 回で、SIMD / JIT パイプラインを二重実装するリスクに見合わない。
      - フィルタは `push_selection_into_scan` でスキャン直上まで降りる
        （`SelectionPlan` が `FullScanPlan` の peek / zone-map と協調する）。
      - 恒等 projection はスキャン側で消える（`fallback_select` が全列なら
        `ProjectionPlan` を作らない）。隣接 projection は `merge_projections`、
        隣接 selection は `merge_selections` が畳む。
      - split 側は `push_selection_through_projection` が計算列へのフィルタを
        投影の下（入力列）へ書き換える。書き換え不能なら上に残す。
      **実装した不足分: 押し下げ済みフィルタの二重評価。** スキャン規則は
      group filter を `SelectionPlan` として巻き、さらに selection ノードの
      `SelectionPlan` がもう一枚載ると、同じ述語を 1 行 2 回評価し、通過行を
      余分なバッチ境界に通していた（`EXPLAIN` に `Selection:` が 2 回、
      `SelectionStats rows_evaluated` も 2 段）。"selection" 実装規則で、
      子プラン（`Rename: t AS x` ラッパは透過）が既に同じ述語の
      `SelectionPlan` なら外側を恒等として畳む。比較は修飾子を無視する
      （`DropQualifiers`）: 押し下げ側は物理テーブル名、論理側はクエリの別名を
      話すため、文字列一致だけでは別名付きの形が畳めない。
      回帰: `QueryTest.SqlEngineCalcMergesDuplicateScanFilters`（非別名 /
      別名 / 残差述語の 3 形で `Selection:` が 1 回、結果と NULL が不変）。
      変異（畳み込み無効化 / 修飾子無視の無効化）はいずれも
      `Selection:` が 2 回になりテストが落ちることを確認。
      再検討条件: プロファイルで Selection→Projection のバッチ境界コピーが
      支配的になったら `CalcPlan`（filter + target list）を検討する。
- [x] duplicate column、unused expression pruningを実装する。
- [x] ProjectionのCSEを実装し、同一式を行ごとに一度だけ評価する。
- [x] projection内のconstant propagationとpredicate pushdownを実装する。
- [x] CASE簡約、boolean filter引き上げ、join前後のproject幅制御を実装する。

### Join変換・探索

（以下は実装済み: join_commutativity, join_enumeration, join_associativity_left/right, infer_join_predicates, push_selection_through_join, split_selection_over_join, join_to_cross_if_no_predicate, merge_adjacent_filters, push_filter_through_left_join_left_side, join_on_false_to_empty, cross_to_inner_with_predicate, join_empty_simplification, self_join_elimination, unique_semi_to_inner, outer_to_anti_join, right_to_left_outer_join, full_outer_join_decomposition, push_down_limit_through_join, dynamic_filter_pushdown_join, join_predicate_transitivity, inferred_inequality_pushdown, redundant_join_predicate_elimination, outer_to_inner (relational)）

- [x] n-ary bushy joinのleft/right associationを完全化する。
      実装は 2 経路:
      (1) 回転規則 `join_associativity_left`
          (`Join(Join(ll,lr), right)` → `Join(ll, Join(lr,right))`) と
          `join_associativity_right` (`Join(left, Join(rl,rr))` →
          `Join(Join(left,rl), rr)`)。回転先の述語は `Memo::NewJoin` が保持済み
          conjunct から再導出し、元の述語が正規化済み conjunct と一致する場合だけ
          回転する (`HasCanonicalStoredJoinPredicate`) ので、述語の無い回転で
          直積を混ぜない。
      (2) 関係数 3..16 では `join_enumeration` が csg-cmp 制限付きで二分割を
          網羅列挙し、bushy 形を直接 memo に登録する
          (`ClaimJoinEnumeration` 後は回転規則を走らせない）。連結グラフでは
          両辺の誘導部分グラフ連結性と切断面の連結性を要求して直積分割を落とす。
          17 関係以上は `GreedyConnectedSplit` で初期木を作り回転規則に委ねる。
      回帰: `CascadesTest.BushyBipartitionsAppearWithAndWithoutEnumeration`
      (4 関係チェーンで `{a,b}|{c,d}` が enumeration 有無どちらでも現れる),
      `CascadesTest.AssociativityRulesEnumerateJoinOrdersWithoutEnumeration`
      (3 関係で 6 分割), `JoinEnumerationKeepsChainSplitsConnected`
      (6 関係チェーンで非連結分割を出さない),
      `OptimizerTest.JoinChainMatchesGoldenResultUnderRuleSubsets`
      (結合順規則の任意部分集合で同一結果)。
- [x] 空Values / 1行定数表によるjoin identityを実装する。
- [x] 1行側cross join eliminationを実装する。
- [x] 条件付きcrossとinnerの相互変換を実装する。
- [x] inner join子交換をコスト駆動で再試行する。
      `join_commutativity` が両順序を memo に登録し、コストが build/probe 側を
      選ぶ（専用の「コスト付き交換」規則は不要。両順序は
      `Memo::AddExpression` の指紋で重複排除される）。回帰:
      `OptimizerTest.HashJoinUsesSmallerInputAsBuildSide`（小さい入力を
      build 側に置く）、`OptimizerTest.JoinChainMatchesGoldenResultUnderRuleSubsets`
      （交換則を外した部分集合でも同一結果）。
- [x] DPHyp / DPccp等の連結部分グラフ列挙を導入する。
      `join_enumeration` → `EnumerateJoinGroup` が、関係 ≤16
      (`kMaxJoinEnumerationRelations`) のグループに対して csg-cmp を満たす
      二分割のみを 1 回だけ発行する（`ClaimJoinEnumeration`）。判定は
      `Memo::CutConnected`（切断面が conjunct で繋がる）と
      `InducedSubgraphConnected`（両辺が連結）。非連結グラフでは従来の二分割を
      維持し、17 関係以上は `GreedyConnectedSplit` + 回転に委ねる。
      回帰: `CascadesTest.JoinEnumerationPrunesDisconnectedBipartitions`
      ({a,b}|{c} の非接続カットを落とす), `JoinEnumerationKeepsChainSplitsConnected`,
      `JoinEnumerationBoundsConnectedJoinClosure`,
      `BushyBipartitionsAppearWithAndWithoutEnumeration`。
- [x] 16関係超のgreedy join order fallbackを実装する。
- [ ] IKKbz / GOOヒューリスティックを検討・実装する。
      **スキップ（検討結果）**: 現状の探索は関係 ≤16 で DP（csg-cmp 列挙）、
      17 以上で `GreedyConnectedSplit`（初期木）+ 回転。IKKBZ はチェーン/スターの
      辺重みから一意な結合順を O(n) で出すヒューリスティックだが、≤16 では
      同じコストモデルで候補を列挙する DP に支配される（列挙した中から最良を
      選ぶため、ヒューリスティックが当てられる保証が無い）。GOO は規則適用順の
      ヒューリスティックで、本実装の worklist は「子が変わった親を再試行」する
      ため順序依存の取りこぼしが無い
      (`CascadesTest.ExploreRequeuesGroupTouchedAfterItWasPopped`)。
      よって追加のヒューリスティックは探索空間を狭めるだけで、測定上の利得が
      無い。17 関係超の greedy 品質が問題になったら、まず
      `GreedyConnectedSplit` の辺重み（選択性・行数）を改善するのが先。
- [x] star join reorderを実装する。
- [x] bushy / left-deep探索予算を実装する。
      bushy と left-deep は同じ memo で列挙され（`join_enumeration` の二分割と
      回転規則の木組み替え）、コストが選ぶ。探索量の上限は 3 段:
      (1) `Memo` の式数上限（`kDefaultExpressionCap`。超過時は `Degraded()` を
          立てて最良案で打ち切る）,
      (2) `SearchEngine::SetStepBudget`（論理規則適用回数の上限。
          `OptimizerOptions::search_step_budget` から設定し、0=無制限。
          枯渇時は `BudgetExhausted()` を立てて best-so-far を返す）,
      (3) 関係数 >16 は DP を行わず greedy 初期木 + 回転。
      回帰: `CascadesTest.ExpressionCapDegradesGracefully`,
      `CascadesTest.ExploreConvergesOnWideJoinGraphsWithoutPassCap`,
      `phase0_contract_test` の step budget ケース（`BudgetExhausted`）。
- [x] 定数以外を含むjoin predicate transitivityを実装する。
- [x] inferred inequalityと冗長join predicateを実装する。
- [x] unique/FKに基づくjoin eliminationを実装する。
- [x] self join eliminationを実装する。
- [x] outer-to-anti、full outer分解、right-to-left変換を実装する。
- [x] outer join上のfilter移動・分割・遅延評価規則を完成させる。
      ケースごとの担当:
      - 左側（保存側）への単一関係 conjunct の押し下げ:
        `push_filter_through_left_join_left_side`（LEFT JOIN は左行を保存し
        左列を書き換えないため常に可）。
      - 右側（NULL 生成側）は null-rejecting のみ:
        `outer_to_inner_join_on_null_rejecting_filter` が
        `ExpressionRejectsNullsOnRelations` で null 除去を証明できた時に
        inner join へ縮約し、その後の押し下げ規則に渡す。証明できなければ
        フィルタは結合の上に残す（NULL パディング行を消さない）。
      - 分割: `split_selection_over_join` が結合をまたぐ選言/連言を分け、
        LEFT JOIN 側は上記 2 規則の合成で等価になる。
      - 右/全外側: `right_to_left_outer_join` で正規化し、
        `full_outer_join_decomposition` で分解してから同じ規則を適用。
      **遅延評価（結合の上へ引き上げる案）は登録しない**（判断）:
      左側フィルタは押し下げても意味が変わらず、結合に入る行数が減るだけで
      あるため、遅延が仕事量を減らすことはない。右側フィルタは null-rejecting
      でない限り上に残す必要があり、それは既に default の配置。よって
      「遅延した方が得」な形状は存在せず、逆方向規則を足すと
      push-down 規則との往復候補が増えるだけになる。
      （`predicate_move_around_outer` の一般形も同じ理由で不要と判断。
      null-rejection の解析自体は `ExpressionRejectsNullsOnRelations` として
      既にあり、`outer_to_inner` と `split_selection_over_join` が使っている。）
      回帰: `CascadesTest.OuterToInnerJoinOnNullRejectingFilter`,
      `OuterToInnerJoinOnNullRejectingIsNotNull`,
      `OptimizerTest.OuterJoinSliceKeepsLeftSideFilterAboveJoin`,
      `OuterJoinSliceKeepsRightSideFilterAboveJoin`（上に残す側）,
      `OuterJoinSliceLowersLeftJoinThroughMemo`。
- [x] unique semi to inner、semi + distinct、semi reductionを実装する。
- [x] cardinality、one-to-one、one-to-many、many-to-many属性を推論する。
- [x] unique内側を優先するindex join、equality hash join選択を実装する。
      ハードな優先付けではなくコスト競争 + 一意性の証明で実現している:
      - `index_join`（`LookupJoin`）/`late_materialization_join` は内側索引が
        `StoresSingleValue()` かつ探索キーが索引キー全体のとき
        `unique_winner`（`LateMaterializationIdentity::kUniqueWinner`）を立て、
        1 点探索で外側カーソルを進める短絡を使う
        (`LookupJoin::AtMostOneProbe`、dump に `at-most-one`)。
      - equality 結合は `hash_join` / `merge_join` / `index_join` /
        `late_materialization_join` / `nested_loop_join` が同じ memo で候補に
        なり、コストが選ぶ（`nested_loop_join` は equi 列対がある場合に自分を
        降りる）。build 側は `HashJoinUsesSmallerInputAsBuildSide` が固定。
      回帰: `ExecutorTest.LookupJoinUniquePointProbeShortCircuits`,
      `ExecutorTest.LookupJoinNonUniqueInnerKeepsTheFullMatchSet`,
      `OptimizerTest.HashJoinUsesSmallerInputAsBuildSide`,
      `OptimizerTest.HashJoinPreferredOverCrossProductForEquiJoin`。
- [x] non-equality nested loopのimplementation ruleを実装する。
      `nested_loop_join` 規則は、述語が無い/non-equi、または equi 列対が
      catalog 型で比較可能でない場合にだけ候補を出す（equi の場合は
      hash/merge/index に譲る）。実行器は block NL（block_size 1024）で
      全述語を組ごとに評価し、外側版は `outer_nested_loop`（LEFT は NULL
      パディング、RIGHT は `right_to_left_outer_join` で正規化、FULL は
      hash/merge 側の担当）。`batch_nested_loop` は半結合/反結合/外側の
      非 equi を tuple 形と同じコストで登録する。
      回帰: `OptimizerTest.BatchNestedLoopExecutesNonEquiInnerJoin`,
      `OptimizerTest.OuterJoinSliceFullNonEquiJoinUsesBatchNestedLoop`,
      `CascadesTest.OuterNestedLoopPlansNonEquiLeftJoin`,
      `CascadesRulesCoverageTest.LeftJoinNonEquiUsesNestedLoop`,
      `OptimizerTest.CrossTableResidualPredicateIsAppliedByTheJoin`。
- [x] dynamic filter、late materialization、batch lookupのruleを接続する。

### 集合・Window・UNNEST規則

（以下は実装済み: union_all_merge, union_to_union_all_plus_distinct, setop_empty_simplification, setop_empty_identity, union_all_push_limit, push_projection_through_union, intersect_to_semijoin, except_to_antijoin, intersect_except_cost_based_lowering, union_distinct_hash_sort_choice, window_frame_sort_sharing）

- [x] intersect-to-semijoin / except-to-antijoinをコスト付きで選択する。
- [x] UNION DISTINCTのhash/sort選択を実装する。
- [x] windowを分割・統合し、互換frameでsortを共有する。
- [x] window後のfilterをpartitionキー条件に限って押し下げる。
- [x] window下のLIMITの安全な例外を定義する。
  - **実装**: Cascades 変換規則 2 本（`plan/cascades.cpp`、
    `limit_push_through_sort` の直後に登録）。
    - `limit_prefix_unordered_window`: `Limit(Window(input))` の被る行が同一
      であることを利用し、Window とその子(input)の間に `Limit` を差し込む。
      input group は derived group (`window-prefix-limit:<signature>`)、新
      Window は `window-prefix-window:<signature>`。signature =
      window spec(partition+order keys, nulls flags) + count + offset。
    - `limit_prefix_ordered_window`: `Limit(Window(Sort(input)))` を TopN
      fusion 形 (`Window(TopN-like Limit)` は Sort を Limit(count+offset)
      へ畳む＝`push_limit_through_sort` と同型) に落とす。projection 越しは
      既存 `push_limit_through_projection` との組合せで到達する。
  - の前置条件 (2 本とも同じ `WindowPrefixGate`):
    (a) **値の安定性** `WindowValueStableUnderTruncation`: 前方参照しない
      call のみ。ranking 白名単 (ROW_NUMBER/RANK/DENSE_RANK) か、明示 ROWS
      frame で start/end とも `UNBOUNDED PRECEDING|OFFSET PRECEDING|
      CURRENT ROW`。default frame・RANGE/GROUPS・NTILE/PERCENT_RANK/
      CUME_DIST・LEAD は不変でないので拒否。
    (b) **emit 順 = partition 順**: 全 call の order_by が空 (rule A のみ)
      か、全 call が同一 spec を共有し子が (partition keys ++ order keys,
      昇降/NULLS 一致) の Sort であること (`SortDeliversWindowSpec`)。
    さらに `with_ties` 拒否、offset は許容 (prefix cut を広げるだけ)、
    window 層の下に別 window がいれば BFS (`ReachableChildHasWindow`) で拒否、
    再発火は derived group tag prefix ガード、冪等は fingerprint 重複排除。
  - 根拠: `executor/window.cpp` はemit 順に rows_ を保持し
    `ComputeOneWindowImpl` が `(*out)[ordered[k]]` で元 index へ散開するた
    め `Window(Limit(X))` と `Limit(Window(X))` の先頭行集合が一致する。
  - テスト: `plan/cascades_test.cpp` 7 件
    (`LimitPrefixThroughUnorderedWindowFires` は発火＋コスト選択まで検証,
    `LimitPrefixThroughOrderedWindowFusesTopN`,
    `DefaultFrameWindowKeepsLimitAbove`, `CumeDistWindowKeepsLimitAbove`,
    `UnorderedRuleRefusesOrderedWindow`, `OrderedWindowRejectsMismatchedSortKeys`,
    `NestedWindowLayerRefusesLimitPrefix`)。
    `query/analytical_features_test.cpp` 3 件
    (`LimitStaysOutsidePartitionWideDefaultFrame`,
    `UnorderedWindowKeepsRanksUnderLimit`, `RunningWindowSurvivesLimitWithRowsFrame`)。
    変異検証 M1-M5: 白名単削除→positive テスト fail / frame gate 削除→負テスト
    2件fail / rule A order gate 削除→`UnorderedRuleRefusesOrderedWindow` fail /
    sort 照合削除→`OrderedWindowRejectsMismatchedSortKeys` fail /
    階層ガード削除→`NestedWindowLayerRefusesLimitPrefix` fail。全て確認済み。
  - 対象外 (将来): 外側 ORDER BY+LIMIT の TopN fusion (window が sort を
    共有するケース)、LEAD/NTILE/PERCENT_RANK/CUME_DIST/default frame。
- [ ] 全体frameのwindow-to-aggregateを実装する。
  - **2026-10-23 判定: 適用不能として確定済み（スキップ）。**
    `docs/optimizer_todo.md` の `window_to_aggregate` 項参照: 全フレーム
    (partition 全体) 集計への置換は、Window が行を保存する (N 行出力) のに
    対し集約は 1 行/グループを返すため、行複製 (一時集約 + グループ結合
    / cross 結合による持ち上げ) を伴わないと非等価。単純置換の余地なし。
    一時集約＋結合の形は将来の decorrelation 枠とし、本項はスキップ。
- [x] rank / row_number filterをTopNへ変換する。
- [x] no-op windowを除去する。
- [x] window prefix sort共有を実装する。
- [x] UNNESTとfilter、JOIN順序、Values変換を最適化する。
- [x] 小さいgenerate seriesをValuesへ変換する。
  - **実装**: Cascades 変換規則 `constant_unnest_to_values`
    （`plan/cascades.cpp`、`unnest_filter_pushdown` の直後）。FROM 系の
    `UNNEST(<定数配列>)` / `UNNEST(GENERATE_SERIES(<定数>))`（TVF lowering は
    kUnnest ノード、alias 未指定なら列名 `value`）を子が 1 行 dummy scan の
    場合に限り kValues ノードへ折りたたむ。1 行 = [要素, WITH OFFSET 時の
    0-based counter]。要素数上限 1024（`kConstantUnnestValuesThreshold`）。
    - 折りたたみ拒否は実行側に委譲: 非配列定数（`UNNEST(スカラー)` は実行時
      エラー）、ネスト配列 / struct・PROTO 展開幅（セクション schema 幅が
      スカラレイアウト(1+offset)と食い違う）、dummy 以外の 1 行リーフでない
      駆動入力（テーブルSources は行倍率未知）、plan キャッシュ凍結リスクの
      ある非決定的生成（GENERATE_SERIES 以外の builtin は pure 評価で折る
      だけでなく関数 gate で GENERATE_SERIES のみ）。
    - **評価の単一化**: `GENERATE_SERIES` はこれまで executor detail evaluator
      のみに実装があり plan 層の純関数評価が不能だった。AST ground truth
      に合わせた純実装を `expression/function_call_expression.cpp`
      `ExecuteFunction`（`ExecuteBuiltin` anon ヘルパーから rename 外出し・
      ヘッダに宣言）へ追加し、`executor/detail/expression_eval.cpp` は
      generate_series をそれへ委譲し、front end / plan fold / 実行 3 経路の
      要素一致を保証（int64 厳密ステップ、2^53 超でも exact、繰り返し加算の
      float 契約、エラーメッセージ一致）。2 引数の降順既定ステップは
      PostgreSQL 流「空配列」（既存挙動維持）。
    - **コスト**: `unnest` implementation rule の local_cost を child 行数 →
      `estimated_rows`（= child × 10 展開係数）へ修正し、kValues（自己行数
      コスト）と公平比較。これが無いと_values ノードが cost 負けして
      折りたたみが採用されない。
  - テスト: `expression/function_call_test.cpp`
    `ScalarFunctionSqlTest.GenerateSeriesSemanticsMatchesRuntime`（昇順/降順/
    既定ステップ/逆Dir空配列/double 反復加算/2^53 超の exact/NULL⇒0 行/
    arity・step0・非数値エラー）; `plan/cascades_test.cpp`
    `ConstantUnnestArrayFoldsToValues`（3 行 + offset 列・schema）、
    `ConstantUnnestKeepsRuntimeWhenNotFoldable`（非配列定数／スキャン駆動は
    折らない）; `query/query_test.cpp` `BuiltinTableFunctionBecomesRowSource`
    ・`CascadesOptimizesUnnestPlans` を新契約へ更新（定数⇒Values、相関⇒
    Unnest を EXPLAIN 断言）; `query/sql_explain_format_coverage_test.cpp`
    `DerivedUnnestAndLimit` 更新。
  - 変異検証: dummy チェックを外す→`ConstantUnnestKeepsRuntimeWhenNotFoldable`
    fail / offset 無し化→`ConstantUnnestArrayFoldsToValues` fail。確認済み。
- [x] recursive termination predicate pushdownを実装する。

### 実行側のfilter / projection最適化

- [x] Filterの短絡順序とConjunctReorderingを実装する。
      - **短絡**: AND/OR は既に左→右で短絡評価し 3 値論理を保つ
        （左が NULL で確定しないときだけ右を評価）。
        `expression/binary_expression.cpp` の
        `EvaluateMaybeShortCircuited`。
      - **並べ替え**: `CanonicalizeConjuncts` が連言/選言を
        **構造コストクラス順**に並べる（`ConjunctCostClass`、低いほど先）:
        定数=0 < 単列リテラル比較/ブール列=1 < 列対比較=2 < IS NULL 系=2 <
        IN/CAST/計算式比較=3-4 < LIKE関数=5 < NOT/AND=6 < OR=7 <
        集約/window/subquery=8。タイブレークは従来の `ToString()` のままなので
        同一クラス内の順序は不変、クラスは純粋に構造的（統計に依存しない）ため
        memo の指紋・正規化済み述語同士の一致判定も安定する。
        スキャンフィルタ / join 条件 / pushdown 先のすべてがこの正規形を通るので、
        実行時の短絡が初手で安い述語を試す（逆順だった例:
        `a.x LIKE 'abc%' AND a.z = 1` は + 行ごとにパターンマッチを実走）。
      - 並べ替えは行集合を変えない（連言/選言は可換）。`WHERE` の行評価は
        `EvaluateConjunctsTolerant`（誤りを記録して次の conjunct を見る）なので、
        「どれかが cleanly false なら破棄」の結果は順序に依存しない。
        違いは短絡で *評価を省けるか*（`a <> 0 AND 1/a > 1` の a=0 行）と、
        列挙順に最初に記録される誤りの種類だけ。
      回帰: `CascadesTest.ScanFilterOrdersCheapConjunctsBeforeLike`（正規化済み
      スキャンフィルタのテキスト順を固定）。変異（コストクラスを外して
      旧来の ToString 順に戻す）はテストが落ちることを確認。
- [x] selection vector / validity bitmapを利用したfilterを実装する。
  - **実装**: `Selection::NextBatch`（`executor/selection.cpp`）は行ループで
    なくバッチ単位で動く。`DataChunk` の型付き `ColumnVector` + 列ごと
    `ZoneMap` を 1 次入力とし、Survivor を selection vector に貯めて
    `AppendGather` で一括コピーする。
    - **zone-map 最適化**: `BatchMayMatch`（`executor/detail/scan_filter.hpp`）
      で確実に全行 reject のバッチは評価もせず skip
      （`SelectionSkipsImpossibleZoneMapBatch`）。
    - **SIMD + ValidityBitmap**: 述語が「int64/date 列 ⟨比較⟩ 同型定数」
      （符号なし列/定数を除く、`SimdIntFilter` gate は ctor で構造判定）なら
      `SimdComparisonKernel::CompareInt64` が raw int64 配列を直接食べて
      `ValidityBitmap` を填め、`ToSelectionVector` → gather。ビット語末尾
      (`inputs.size() % 64`) も SelectionVector 側で処理済み
      （`DataChunkTest.SelectionVectorTest.*`,
      `SimdComparisonKernelTest.*`）。
    - **bytecode バッチ**: `BytecodeProgram::TryEvaluateBatch` が bool
      `ColumnVector` を型付き格納のまま返し、1 行 1 `Value` の boxing 無しで
      bits を読んで selection vector を組む。NULL は non-pass
      （`!IsNull(i) && bits[i] != 0`）。
    - **2 層 conjunct**: 全述語がコンパイル不能なとき（IN over 関数 / LIKE /
      IS NOT NULL 等が 1 つ混ざる）個別 conjunct のコンパイル可否を判定し、
      compile 可能なものはバッチ評価・tier 2 は survivor だけ行評価
      （`EvaluateConjunctsTolerant` で WHERE 可換セマンティクス維持）。
      compile 済みが 0 なら行のみフォールバック。
    - カウンタ `rows_seen_ / rows_selected_ / skipped_batches_ / jit_batches_`
      は EXPLAIN ANALYZE の `SelectionStats` / `ZoneMap blocks_skipped` に出る
      （EXPLAIN 無しは全カウンタ抑止）。
  - 変異検証: bytecode バッチ bits 条件を反転 → `Selection*` 2 件 fail、
    確認済み。
- [x] bytecode / JIT filterの選択経路を接続する。
  - **実装**: Selection の compile 経路選択は ctor で確定し、NextBatch の
    優先順位は zone-map skip → `SimdIntFilter`(SIMD gate) → JIT kernel →
    全述語 bytecode バッチ → 2 層 conjunct → 行評価フォールバック。
    - **JIT 発火（選択経路の本体）**:
      1. ctor: `BytecodeCompiler::Compile(exp, schema, kFilter)` で全体述語の
         bytecode を作る。`BytecodeEnabled()` が差分テストフック
         （無効化すれば JIT も無効、`expression/jit_test.cpp`）。
      2. NextBatch: `jit_attempted_` 未設定で
         `rows_seen_ >= jit_threshold_rows_`（実行側が与える閾値。短い /
         断片的なスキャン向けの累積フォールバック）または
         `rows_seen_ >= kJitEarlyPromotionRows(512K) && 満杯バッチ`
         （スキャン全体の行数が break-even を超える場合の早期昇格、
         `docs/jit_profile.md`）で 1 回だけ JIT compile を試みる。
      3. compile 可否は bytecode 命令列の形で判定
         （`kLoadColumn / kLoadConstant / kBinaryInt64` の 3 命令、定数 /
         列が unsigned でないこと——JIT kernel は signed 比較のため）。
         成立なら `JitInt64Kernels::CompileFilter` が以後のバッチを 0/1
         バイト列で処理（`AppendBoolBatch` で boxing 無し、`++jit_batches_`）。
    - compile 不成立なら bytecode バッチ / 行フォールバックへ静かに落ちる。
      Projection 側の同種昇格は `ProjectionUsesJitForLargeAffineIntegerBatches`。
  - テスト: `ExecutorTest.Selection*`（基本 / zone-map skip / JIT 発火 /
    左定数反転 / OR）、`JitTest.CompileFilter_*`、`SelectionVectorTest.*`、
    `SimdComparisonKernelTest.*`。
  - 変異検証: JIT 昇格の第一臂（`rows_seen_ >= jit_threshold_rows_`）を外す →
    `SelectionUsesJitForLargeIntegerBatches` fail。確認済み。
- [ ] VirtualComputedColumnの遅延評価を実装する。
      **スキップ（判断理由）**: 2 つの読み方があり、片方は実装済み、もう片方
      はストレージ / DataChunk 層の全面再設計が前提。
      - 実行パイプラインの意味（投影が定義する計算列を、消費者が要求するまで
        評価しない）は既存の 3 機構で達成済み:
        (1) `push_selection_through_projection`（FilterProjectTranspose）が
        計算列へ掛るフィルタを入力列式へ書き換えて投影の下へ降ろす
        （非通過行の計算列評価をそもそも消す）;
        (2) `fallback_select` / スキャン narrowing が consumers の要求列だけを
        出力・decode する（`TINYLAMB_SCAN_PROJECTION=1` で decode floor まで
        narrowing）;
        (3) `topn_push_through_projection` が上位 N 行以外の計算列評価を省く。
      - 残る意味（DataChunk 内の列単位 on-demand materialization、ならびに
        DDL `GENERATED ALWAYS AS ... VIRTUAL` のスキーマ仮想列）は DataChunk /
        行形式 / カタログ / WAL の全面再設計（列を Value 配列から遅延スロット
        へ変える、仮想列は行 payload を増やさず read時に合成）を要求する。
        ハイブリッド PAX + morsel 並列の現物理（行単位 materialize 前提）と
        噛み合わず、単独で入れる利得はバッチ境界コピー削除程度に留まる。
        - **将来の着手点**: `DataChunk::ColumnAt` を仮想化し、遅延列は
          (producer 式, 元列参照) で保持、consumer が最初に触った時点でバッチ
          単位評価（`Selection::NextBatch` の SIMD / JIT 契約と同じく
          NULL 反映の selection vector を返す）。スキーマ仮想列はカタログに
          列単位の生成式を置き、全スキャン経路（full / index / LSM /
          morsel 並列）の出力合成から着手。
- [ ] DictionaryProjectionを実装する。
      **スキップ（判断理由）**: 辞書符号化射影（VarChar を dict + code で
      流す）は `ColumnVector` に新表現を足し、比較 / join / sort / 集約 /
      bytecode·JIT すべての消費者が code 域（または展開）を扱える必要が
      ある。辞書の供給源も未決で、スキャン側辞書ページ（ストレージ層の
      新 column セグメント形式）か実行時構築（投影が入力バッチから辞書を
      継ぎ足す; morsel 並列の局所辞書併合が WiscKey 読みと競合）のどちらを
      採るかの設計判断が先。TPC-H 全問の varchar 比較が対数域に落ちる潜在的
      利得は大きいが、表現 1 つ足すだけで executor 差分面が全域に波及する。
      - **将来の着手点**: まず `ColumnVector` に 2 成分（`dictionary` 共有
        ストリングプール + `code` int32）を追加し、`CompareVarCharForOrderBy`
        相当を code 域比較に置換する babysitter 実装から始め、バッチの
        `TryEvaluateBatch` / `SimdComparisonKernel::CompareStringPrefix` を
        code 域で処理できる経路を diff テスト（`differential_test` の述語
        直交 set）で守る。
- [x] residual側のConstantFolding境界を整理する。
  - **現境界の整理（本項の成果）**: 定数畳み込みは 4 層で行われる——
    (1) Scalar rewrite（`fold_binary/unary/in/function` 等、評価は
    `ExpressionRuleSet::Default()`）: 列参照なしの部分式を AST 変換で折る。
    memo 経路はここで恒 TRUE conjunct が AND 簡約によって既に消える
    （`TRUE AND x>1` → `x>1`）。(2) 関数を含む WHERE は
    `NeedsRelationalEvaluation` が課すハッチ（
    `TINYLAMB_SCALAR_FN_CASCADES`=off が既定）のため SQL 全体が relational
    経路に流れ、relational AST は生述語のまま `CompileScanFilter` /
    `FinishQuery` に到達する——ここが従来の穴: 定数 conjunct
    （`UPPER('A')='A'`）が 1 行ごとに評価されていた。
    (3) **本実装**: `CompileScanFilter`
    （`executor/detail/scan_filter.cpp`）に constant-residual 境界を追加
    ——決定的 conjunct（volatile 呼び出し `ContainsVolatileCall`（
    expression 層に新設）なし、パラメータスロットなし、subquery なし、かつ
    空スキーマ `TryEvaluate` が成功＝列参照は空スキーマで失敗するので
    自己ゲート）は compile 時に 1 回評価し、`Truthy()` で
    **TRUE → conjunct 放棄** / **FALSE・NULL →
    `compile_decided_reject`**（結合全体を compile 時確定 + 全行 reject は
    `MatchScanFilter` / morsel 並列の `MatchScanFilterWorker` でも
    ミラー）。行時評価と同一の 3 値規則を 1 回だけ適用するので、
    rejecting conjunct による sibling error 抑制
    （`EvaluateConjunctsTolerant` 契約）も compile 時決定が肩代わりする。
    stable（`CURRENT_TIMESTAMP` 等）は保守的に kImmutable のみ fold
    （文内固定値を保つ）。OR 分岐内は行動単位の評価のまま
    （sibling 意味論が conjunct と異なる）。
    - 新規公開: `expression::ContainsVolatileCall`（rand/random/uuid/
      generate_uuid/newid の到達可能性; plan_cache の
      `ContainsNonDeterministicCall` は query 層に留まり layering 順を守る）。
  - テスト: `executor_extra_test.cpp`
    `ScanFilterTest.CompileTimeResolvesDeterministicConstantConjuncts`
    （TRUE 放棄 / FALSE 全行 reject / NULL / volatile 残留）、
    `query/query_test.cpp`
    `QueryTest.ConstantConjunctsDecidedAtScanFilterCompile`
    （relational 経路の行集合 + volatile でない stable も残留）。
  - 変異検証: `TryDecideConstantConjunct` を常時 nullopt にすると単体テスト
    fail（SQL 行集合は等価なので不変——本境界の価値は評価回数削減）。
    確認済み。

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
