# Executor / Physical Operator TODO

Cascades の実装規則が選ぶ **物理プラン** と、それを駆動する **実行器**。
最適化側の規則一覧は [`optimizer_todo.md`](optimizer_todo.md)。

マーク:

- `[x]` ツリーに実行器またはプランがある（完成度は括弧内）。
- `[ ]` 未実装。プランクラス（`plan/`）と実行器（`executor/`）の両方を書く。

層の約束: 新しい実行器は `executor/`、プランは `plan/`、結合 IR の細部は
`executor/detail/`。下位層への依存を増やさない。

既存の安全網: エンジンが Distinct → Sort → Limit をプランの上に載せる。
新しい物理オペレータは `IsOrderedBy` / `EnforcesLimit` / `EmitRowCount` を
正しく報告すること。

---

## 既にあるもの（再実装しない）

- [x] `FullScan` / `ParallelScan` + `FullScanPlan`
- [x] `IndexScan`（点・範囲・定数 IN の multi-range）+ `IndexScanPlan`
- [x] `IndexOnlyScan` + `IndexOnlyScanPlan`
- [x] `Selection` + `SelectionPlan`
- [x] `Projection` + `ProjectionPlan`
- [x] `Aggregation` + `AggregationPlan`（＋ `ParallelAggregation`）
- [x] `Limit` + `LimitPlan`
- [x] `HashJoin`（in-memory / hybrid spill）+ `JoinKind`: inner / semi / anti /
      null-aware anti
- [x] `IndexJoin` + エイリアス付き自己結合
- [x] `CrossJoin` / nested-loop（完全述語評価）
- [x] `Sort`（外部マージ）※計画ノードとしてはエンジン安全網が主
- [x] `Distinct`
- [x] `Insert` / `Update` / `Delete`
- [x] `ConstantExecutor`
- [x] `EmptyPlan`（定数 FALSE / NULL の結果をスキャンなしで返す）
- [x] `RelationRename` + `RelationRenamePlan`
- [x] `Relational` 不透明 IR + `RelationalPlan`（外側結合・CTE などの避難所）
- [x] `DataChunk` ベクトル化、`SpillFile`、`QueryMemory`、`QueryScheduler`
- [x] `ZoneMap`（スキャン補助）

`JoinKind::kSemi` / `kAnti` はハッシュ結合実行器にあり、等価キーの
論理 Semi/Anti から実装規則まで接続済み。残余述語や Mark/Null-aware
形状は下の未完了項目として扱う。

---

## P0 — 結合の欠け（計画品質の最大ギャップ）

### Merge / sort-merge

- [x] `MergeJoinPlan`（キーと両側スキーマの物理契約）
- [x] required ordering / implementation rule（interesting order まで。Cascades の
      MergeJoinAlternative が子 SortPlan 要求を構築）
- [x] `MergeJoinExecutor`（等価、両側昇順、NULL 非マッチ、重複キー run）
- [x] 複合キー（キー配列と回帰テスト）
- [x] 不等号残余フィルタ（等価キー run の各ペアに残余述語を評価。outer の NULL
      padding と semi/anti の一致判定も残余込みで行う）
- [x] LEFT / RIGHT / FULL outer merge join（NULL キーを非マッチとして左右の
      unmatched 行を NULL padding。空入力の幅は `MergeJoinPlan` から伝播し、
      `MergeJoinSupportsOuterKindsAndNullKeys` で回帰）
- [x] SEMI / ANTI merge join（重複キーで probe を 1 回だけ、NULL キー非一致、物理
      実装規則と回帰テスト付き）
- [x] 片側未ソートなら `SortPlan` を子として要求する実装規則（MergeJoin の
      コストにソート費用を加算し、他方式との比較対象にする）
- [x] 興味深い順序を出力（左入力の join キー昇順を `IsOrderedBy` で伝播）
- [x] many-to-many の交差（重複キー run の直積を出力）
- [ ] 非等価 merge（`a.x < b.y` のバンドジョイン）は後回しでも項目化:
      - [ ] inequality merge / band join

### Outer hash / nested loop

- [x] `LeftHashJoin`（ビルド右、マッチしなければ NULL pad。`HashJoin`
      の `JoinKind::kLeftOuter` として実装）
- [x] `RightHashJoin`（`HashJoin::JoinKind::kRightOuter`。左右の未マッチ行と
      NULL キーを保持）
- [x] `FullHashJoin`（`HashJoin::JoinKind::kFullOuter`。両側未マッチ）
- [x] spill 付き hybrid outer hash（`HashJoin::MaterializeOuter` が両辺を
      budget-aware に取り込み、spill partition を再走査。LEFT JOIN の整数キー・
      文字列キー回帰 `Relational_WithHybridHashLeftJoinUnderBudget_SpillsToDisk` /
      `Relational_WithStringKeyHybridLeftJoinUnderBudget_SpillsToDisk`）
- [x] `LeftNestedLoopJoin` / `Right` / `Full`（非等価外部結合の relational fallback と回帰テストあり）
- [ ] outer の Semi ではない null-aware マッチ
- [ ] 右側フィルタが null-rejecting でないときの実行時 NULL 生成

### Semi / Anti / Mark / Single（実行器は部分的に既存）

- [x] Cascades から `JoinKind::kSemi` / `kAnti` を選ぶ物理規則
      （等価キー・残余なしの HashJoin。半結合は probe 側スキーマを保持）
- [ ] `MarkJoin`（一致フラグ列を追加、`IN` の UNKNOWN）
- [x] `SingleJoin` / `Max1Row` の意味論（スカラサブクエリは 2 行目で
      cardinality error。`Max1RowPlan` / `Max1RowExecutor` も接続）
- [x] `NullAwareAntiJoin`（`NOT IN`、ビルド側 NULL の短絡）
- [ ] uniqueness 付き semi → inner に落とさない実行時 assert
- [x] EXISTS 用 short-circuit nested loop（単一表の安全な LIMIT 1 pushdown、相関 index 経路は維持）

### その他の結合形

- [ ] `BlockNestedLoopJoin`（外側ブロック × 内側全走査）
- [ ] `BatchNestedLoopJoin`（外側キーを IN リスト化）
- [ ] `LookupJoin` の複合キー・範囲（IndexJoin の一般化）
- [ ] `ZigZagJoin`（両側インデックス）
- [ ] `LateMaterializationJoin`（RID 結合 → 列フェッチ）
- [ ] `DynamicFilterJoin` / sideway information passing（ブルームをスキャンへ）
- [ ] `HashSemiReduction`（巨大ファクトの前に次元を縮約）
- [x] `CrossJoinUnnest`（定数/相関配列を通常の cross/lateral 行源として結合、SQL 回帰あり）
- [ ] `AsOfJoin` / `IntervalJoin`（時系列、将来）

---

## P0 — ソート・Top-N・順序

- [x] `SortPlan` を単一リレーションおよび一般 Cascades 最適化経路から明示生成
      （`IsOrderedBy` でエンジン安全網との二重適用を防止）
- [x] `ExternalMergeSort` のプラン接続（`SortPlan` が既存
      `SortExecutor` を駆動。単一キー radix / 複合キー安定ソートも既存実装）
- [ ] `InMemoryQuickSort` / `pdqsort` 経路
- [x] `TopNPlan` / `TopNExecutor`（ヒープで `OFFSET + LIMIT` 件だけ保持）
- [x] `TopNPlusOffset`（TopNの容量と出力位置で対応）
- [x] `WITH TIES` Top-N（TopNExecutor の ties 境界拡張）
- [ ] `IncrementalSort`（入力が prefix ソート済み）
- [ ] `PartialSort`
- [ ] `ClusteredIndexOrder` を FullScan が報告（PK 順ページ）
- [x] `LimitPushdownScan`（unordered LIMIT の OFFSET + LIMIT 上限を FullScan が
     受け取り、スカラー/バッチ双方で early stop）
- [ ] `SkipScanDistinct`（インデックスで DISTINCT キーを飛ばす）
- [x] collation / nulls first/last を比較器に（Sort / TopN と SQL の明示
      `NULLS FIRST` / `NULLS LAST` を接続。collation の多言語拡張は別項目）
- [x] 安定ソート vs 非安定の文書化（SortExecutor は安定ソートを保証）
- [x] 並行ソート（モーセル分割と安定マージを `ParallelSort` 経路で実行）

---

## P0 — 集約

- [x] `HashAggregatePlan` と `SortAggregatePlan` の分離選択（スカラー集約の
      物理 plan 型と `AggregationStrategy` を分離。GROUP BY payload の
      Cascades 接続までは別項目）
- [ ] `StreamAggregate`（入力が group キー順。前提: Cascades 論理ペイロードへの
      GROUP BY キー導入が未着手のため、単体実行器だけでは接続先がない）
- [ ] `PartialAggregate` + `FinalizeAggregate`（並列・分散）
- [x] `DistinctAggregate`（逐次・並列 `Aggregation` が aggregate metadata の
      DISTINCT 集合を保持して `COUNT(DISTINCT x)` を処理）
- [ ] `TwoPhaseDistinctAgg`
- [ ] `MinMaxIndexScan`（INDEX ONLY で MIN/MAX）
- [x] `FilteredAggregate`（GoogleSQL `AGG(x WHERE …)`。直列・並列集約と
      `COUNT(*)` のフィルタ順序を回帰）
- [ ] SQL 標準 `AGG(...) FILTER (WHERE …)` の AST 接続
- [ ] `OrderedSetAggregate`（`PERCENTILE_CONT` / `WITHIN GROUP`）
- [ ] `GroupingSetsExecutor` / `Rollup` / `Cube`（Expand + Agg または複数 Agg）
- [x] `ScalarAggEmptyInput`（0 行で COUNT=0、SUM=NULL。AVG/MIN/MAX の NULL も
      含め、逐次・並列集約で回帰テスト済み）
- [x] spill 付きハッシュ集約の計画接続（Relational fallback の partitioned aggregation、予算超過回帰テストあり）
- [x] `MemoryLimitedAgg` の強制 spill（grouped relational aggregation が
      `QueryMemoryBudget` を監視して hash partition + `SpillFile` に切り替え。
      `Relational_WithPartitionedAggregationUnderBudget_SpillsToDisk` で回帰）
- [ ] bitwise / bool_and / bool_or のベクトル化パス
- [ ] `ANY_VALUE` 短絡

---

## P1 — スキャンとアクセスパス

- [ ] `BitmapIndexScan` + `BitmapHeapScan`（複数インデックス OR/AND）
- [ ] `BitmapAnd` / `BitmapOr` オペレータ
- [ ] `TidScan` / `RowIdLookup` / `CTID` 相当
- [ ] `IndexSkipScan`（先頭列が等式でない複合インデックス）
- [ ] `IndexOnlyScan` の visibility map 相当（MVCC で heap fetch 省略を広げる）
- [ ] `CoveringIndex` 選択のコスト精度
- [ ] `PAXScan` / カラムナ・スキャンの Cascades 接続
- [ ] `ZoneMapSkipScan`（既存 zone map をプラン属性に）
- [ ] `LateMaterializationScan`（まずキー、後でワイド列）
- [ ] `PrefetchingIndexScan`
- [ ] `ParallelIndexScan`
- [ ] `RangePartitionedScan`（ページ範囲の分割）
- [ ] `SampleScan`（BERNOULLI / SYSTEM）
- [x] `DummyScan` / `OneRow`（`DummyScanPlan` + `ValuesExecutor`。SELECT 1 を
      Optimizer 経路で実行）
- [x] `ValuesScan`（多行 VALUES の `ValuesPlan` / `ValuesExecutor`）
- [x] `FunctionScan` / TVF（関数由来の配列を `LoadSource` が行源化）
- [x] `UnnestScan`（配列、STRUCT/PROTO、WITH OFFSET、相関 lateral 経路）
- [x] `WorkTableScan`（再帰 CTE。反復ごとに CteMap の CTE 名を差分 Relation に
      束縛する形で実装。専用インデックスアクセスは `WorkTableIndex` が未実装）
- [ ] `ForeignScan`（将来の外部データ）
- [x] `FilteredScanPushdown` と残余フィルタの EXPLAIN 区別（IndexScan の predicate
      と、非カバー条件を Selection に残す経路を分離）
- [ ] 複数レンジ IndexScan の大域順序保証（1 レンジ以外は順序なし、既存）の
      マージによる順序回復
- [ ] LSM / WiscKey 経路の `LsmScan` 実装規則

---

## P1 — DISTINCT・集合演算

- [x] `HashDistinctPlan` / `HashDistinctExecutor`（`DistinctPlan` + 既存ハッシュ集合実行器）
- [x] `SortDistinct`（ソート済み入力の adjacent unique。Cascades は必要なら
      SortPlan を前置し、ハッシュ方式とコスト比較）
- [x] `UnionAllExecutor`（n-ary append、列数のスキーマ強制）
- [x] `UnionDistinct`（hash による重複排除）
- [x] `Intersect` / `IntersectAll`（行多重度を保持）
- [x] `Except` / `ExceptAll`（行多重度を保持）
- [x] 集合演算の型変換・共通型解決（INT64 / DOUBLE を共通 DOUBLE に昇格し、
      plan schema と executor の行値を一致させる）
- [x] 集合演算の spill（重複判定が必要な UNION DISTINCT / INTERSECT /
      EXCEPT は hash partition を `SpillFile` へ退避し、partition 単位で再実行。
      共通型を確定してから再 partition するため INT64/DOUBLE の重複も保持）
- [x] `MergeAppend`（ソート済み UNION ALL 子供のマージ。未ソート子は
      implementation rule 内で SortPlan を前置し、共通型を出力スキーマへ合わせる）
- [x] `Append`（順序なし UNION ALL）
- [x] 子供への LIMIT 分配（UNION ALL。有限 LIMIT・順序なしの連結で
      branch cap を適用）

---

## P1 — Window

- [x] `WindowAggPlan` / `WindowExecutor` 相当（`ApplyWindows` + `ComputeOneWindow`）
- [x] パーティション切替
- [x] ROWS フレーム（BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 等）
- [x] RANGE フレーム（ORDER BY キーの同値）
- [x] GROUPS フレーム（peer group 単位の preceding/current/following 境界と回帰テスト）
- [x] EXCLUDE CURRENT ROW / GROUP / TIES
- [x] フレーム無しランキング（ROW_NUMBER / RANK / DENSE_RANK / NTILE）
- [x] オフセット（LAG / LEAD / FIRST_VALUE / LAST_VALUE / NTH_VALUE）
- [x] 累積 AGG（SUM/AVG OVER）
- [x] 複数 window のソート共有
- [ ] ハッシュ・パーティション + ソート（メモリ）
- [ ] spill 付き window
- [x] `QUALIFY` を Window の後段 Filter として実行

---

## P2 — UNNEST / 配列 / VALUES / 生成

- [x] `UnnestExecutor` 相当（`LoadSource` の配列行展開）
- [x] `WITH ORDINALITY`（`SelectSource::offset_alias` と 0-based offset 列を
      `Unnest` 出力へ付加。`SqlEngineUnnestExpandsArraysAndEmitsOffsets` で回帰）
- [x] 複数 UNNEST の zip / 直積方針（GoogleSQL の FROM 複数項目を直積として
      実行し、回帰テストで固定）
- [x] `UnnestJoin`（相関 UNNEST。先行行スコープで配列を評価する lateral
      経路と回帰テスト）
- [x] `GenerateSeriesExecutor`（`GENERATE_SERIES` / `GENERATE_ARRAY` を配列値として
      生成し、既存の `Unnest` row source へ接続。要素数上限と step 検証を実装）
- [x] `GenerateDateArray` 等（`GENERATE_DATE_ARRAY` を DATE 配列として生成し、
      `UNNEST` で展開する回帰テスト付き）
- [x] `ValuesExecutor` 多列（`ValuesPlan` のスキーマ幅検証と複数列 `Row` を
      そのままベクトル出力）
- [x] 空配列 UNNEST の 0 行 vs NULL 行

---

## P2 — CTE・再帰・マテリアライズ

- [x] `MaterializePlan` / `EagerSpool`（CTE 複数参照。`ExecuteQuery` が
      `CteMap` に一度だけ materialize）
- [x] `LazySpool` / `ConsumerSpool`（再スキャン可能バッファ。`Relation` の
      `ForEachRow` を複数 consumer から利用）
- [x] `CteScan`（実体化された CTE の読み。`LoadSource` の `CteMap` 経路）
- [x] `RecursiveUnion`（作業表の反復。`ExecuteRecursiveCte`: anchor 項で作業表を
      初期化し、再帰項には前回差分だけを CTE 名に束縛して反復。anchor が空でも
      再帰項内の非再帰分岐から反復を開始する）
- [x] 再帰の UNION vs UNION ALL（UNION ALL は差分そのまま、UNION DISTINCT は
      payload の seen セットで重複排除。循環データは DISTINCT で停止する。
      NaN / ±inf は正規化キーで収束させる）
- [x] `WITH DEPTH BETWEEN lo AND hi` 修飾子（反復上限と depth 列を提供。anchor は
      depth 0、round r の出力は depth r、範囲外の行は非表示。既定列名 depth、
      `AS col` で改名、UNBOUNDED 上限対応）→ サイクル検出オプションの実質代替
- [x] 再帰深さリミット（明示 DEPTH 未指定時は反復上限 1024 + 累積行予算
      1000 万で超過時にエラー）
- [x] JOIN の USING 列合併（`using_columns` を SelectSource に追加し、
      Join/InnerJoin 後に右側の同名列を出力から除去。FULL/RIGHT/INNER の
      USING 結合で無修飾参照の曖昧さを解消）
- [x] anchor 内重複の UNION DISTINCT 排除（Flights 型の全 NULL 重複行が
      出力に二重に入る不具合を修正。anchor も seen セットで dedupe）
- [x] JOIN の USING 句を両辺修飾で生成（無修飾 `a = a` が同名列結合で
      曖昧になる問題を修正。FULL/RIGHT JOIN の CTE オペランドが解決）
- [x] 再帰項の BY NAME / CORRESPONDING アライメント（各ラウンドで蓄積側と項出力を
      列名で整列してから dedupe/追加。出力スキーマが拡張/縮小した場合は既存行も
      再射影。`matches[term_index-1]` で項対応の match を解決）
- [x] CTE 解決の依存順（WITH RECURSIVE 内の前方参照を許可。兄弟定義への参照を
      トポロジカルに解決し、相互再帰はエラー化）
- [x] テンプレート再構築でも再帰メタデータを保持（BindSelect が宣言順
      `WithQueryOrder()` でバインドし、`AddRecursiveWithQuery` /
      `SetRecursiveDepth` を再適用。従来はプランキャッシュ経由で recursive
      フラグが落ちて自己参照が unknown table になっていた）
- [ ] `WorkTableIndex`（再帰結合用）
- [x] `CacheSubquery`（`ExecuteCachedUncorrelated` が非相関サブクエリを 1 回だけ
      materialize。再利用と cache hit を回帰テスト）

---

## P2 — DML・ロック

- [x] `InsertSelectPlan` 相当（`INSERT ... SELECT` を relational executor で
      materialize し、既存 `Insert` に一括接続。対象列順・型変換を含む
      `SqlEngineInsertSelectCopiesAndMapsRows` で回帰。専用 Cascades DML plan は
      DML 論理ノード導入時に置換する）
- [x] `BatchInsert`（`Insert` が複数行 source を最後まで消費して一括挿入。
      `Insert_FromSourceTable_InsertsAllRows` と上記 INSERT SELECT 回帰で確認）
- [ ] `Upsert` / `ON CONFLICT`
- [ ] `MergeExecutor`（MATCHED / NOT MATCHED）
- [ ] `UpdateFrom` / `DeleteUsing`（結合更新）
- [ ] `ReturningExecutor`
- [ ] `LockRows` / `SELECT FOR UPDATE`
- [ ] `SkipLocked` / `NOWAIT`
- [ ] `Truncate` 実行器（カタログ操作との境界）
- [ ] トリガ相当の before/after 行フック（将来）

---

## P3 — ベクトル化・パイプライン・並列

- [ ] 全オペレータの `DataChunk` 入出力統一（まだ行型が残る箇所の洗い出し）
- [ ] Selection vector / validity bitmap の徹底
- [ ] late materialization の列バッチ
- [ ] `PipelineBreaker` 明示（Hash build、Sort、Materialize）
- [ ] morsel-driven 並列の Cascades DOP 接続（`QueryScheduler` 既存）
- [ ] `Exchange` / `Gather` / `Redistribute` 実行器（単一ノードはスレッド間）
- [ ] `ParallelHashJoin`（共有ビルド表）
- [ ] `ParallelMergeJoin`
- [ ] `ScanPartition` ステアリング
- [ ] NUMA 意識のアリーナ
- [ ] SIMD 比較カーネル（INT64 は JIT と役割分担）
- [ ] バッチ集約の辞書エンコード
- [ ] 圧縮カラムの直接スキャン（PAX）
- [ ] ゼロコピー投影（列参照のみ）

---

## P3 — メモリ・spill・キャンセル

- [ ] オペレータごとの reservation（`QueryMemory` 拡張）
- [ ] HashJoin / HashAgg / Sort / Window の統一 spill プロトコル
- [ ] 再帰 CTE のメモリキャップ
- [ ] 実行キャンセル（ステートメントタイムアウト）の全オペレータ貫通
- [ ] 実行時統計（build 行数、spill 回数）を EXPLAIN ANALYZE へ
- [ ] メモリ不足時の graceful degrade（強制 nested-loop 等）

---

## P4 — フィルタ・投影の実行最適化

- [ ] `Filter` の短絡順序（選択性の高い述語先、副作用に注意）
- [ ] `ConjunctReordering` 実行時
- [ ] バイトコード / JIT フィルタの選択（既存 expression 層と接続）
- [x] `Projection` の CSE（同一式を 1 回、行ごとの評価キャッシュ）
- [ ] `VirtualComputedColumn` の遅延評価
- [ ] `DictionaryProjection`
- [ ] `ConstantFolding` はオプティマイザ側。実行器は residual のみ

---

## P4 — インデックス結合・ルックアップの拡張

- [ ] 複合インデックスの不等式 + 等式ミックス
- [ ] covering 判定の複数インデックス
- [ ] `NestedLoopIndexJoin` のプリフェッチ
- [ ] 内側フィルタをインデックスに落とせず heap に残す明示
- [ ] unique 内側の 1 行ルックアップ短絡
- [ ] 外側がソート済みなら内側ルックアップの局所性

---

## P5 — サンプリング・統計収集実行

- [ ] `AnalyzeScan`（ヒストグラム構築用）
- [ ] `ReservoirSample`
- [ ] `BlockSample`
- [ ] `HyperLogLogScan`
- [ ] 実行時フィードバック用 `CardinalityProbe`（デバッグ）

---

## P5 — EXPLAIN / プロファイル

- [ ] 各新オペレータの `Dump` / EXPLAIN 名
- [ ] ANALYZE: 実時間、行数、ループ回数、spill
- [ ] 見積行 vs 実測行
- [ ] ベクトル化バッチ数
- [ ] キャッシュヒット（バッファプール）

---

## P6 — 分散（予約。単一ノード実装は no-op Exchange でも可）

- [ ] `ExchangeHash`
- [ ] `ExchangeBroadcast`
- [ ] `ExchangeGather`
- [ ] `ExchangeRange`
- [ ] `RemoteScan`
- [ ] `DistributedLimitMerge`
- [ ] `DistributedAggFinalize`

---

## P6 — 正しさ・テスト

- [x] 各結合種の NULL キー・空入力・1 行・重複キー（outer hash の回帰テスト）
- [x] outer join の null pad ゴールデン（left / right / full hash）
- [x] `NOT IN` vs anti join の三値論理
- [x] Window フレーム境界（ROWS / RANGE の回帰テスト）
- [x] 集合演算 ALL vs DISTINCT の多重度
- [x] Top-N + OFFSET + ties
- [x] spill 強制（メモリキャップを極端に下げる。HashJoin / HashAgg / Sort の
      回帰テストで結果と spill 統計を確認）
- [x] 並列と直列の結果一致（ParallelAggregation の int64 fast path 回帰）
- [ ] MVCC スナップショット visiblity を IndexOnly / Bitmap で
- [ ] キャンセル途中のリーク無し
- [ ] エンジン安全網（Sort/Limit/Distinct）との二重適用が無いこと

---

## オプティマイザ項目との対応（実装時に同時に動かす）

| オプティマイザ | 必要な実行器 |
|---|---|
| `prefer_merge_when_both_sorted` | MergeJoin |
| `not_in_to_anti_join` / `exists_to_semijoin` | Semi/Anti 規則接続、Mark、NullAwareAnti |
| `outer_to_inner` 失敗時の LEFT | LeftHashJoin / LeftNLJ |
| `merge_sort_limit_to_topn` | HeapTopN |
| `two_phase_hash_agg` / `stream_aggregate_if_sorted` | HashAgg / SortAgg / StreamAgg |
| `rank_filter_to_topn` | Window + Filter または TopN |
| `union_all_merge` | Append / UnionAll |
| `intersect_to_semijoin` | Semi または Intersect |
| `cte_materialization` | Materialize / Spool |
| `unnest_*` | Unnest |
| `bitmap_*` アクセスパス | BitmapIndex + BitmapHeap |
| `minmax_index_only` | MinMax インデックススキャン |
| `dynamic_filter_join` | SIP / Bloom を Scan へ |
| `recursive_cte_*` | RecursiveUnion + WorkTableScan |

---

## 推奨実装順（最初のスライス）

1. `SortPlan` を明示し、Limit/Top-N と二重適用を潰す。
2. `TopNHeap`（ORDER BY LIMIT が TPC-H で効く）。
3. `MergeJoin` + interesting order。
4. `LeftHashJoin` / `RightHashJoin` / `FullHashJoin`（`kOuterJoin` と
   null-producing side の実装を先に固定）。
5. `HashAgg` vs `SortAgg` / `StreamAgg` の分離。
6. `Append` / `UnionAll`。
7. `WindowAgg` の最小セット（ROW_NUMBER + 累積 SUM）。
8. `Unnest`。
9. Bitmap スキャンと Min/Max インデックス。
