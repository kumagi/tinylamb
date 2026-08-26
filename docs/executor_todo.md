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
- [x] `HashJoin`（in-memory / hybrid spill）+ `JoinKind`: inner / semi / anti
- [x] `IndexJoin` + エイリアス付き自己結合
- [x] `CrossJoin` / nested-loop（完全述語評価）
- [x] `Sort`（外部マージ）※計画ノードとしてはエンジン安全網が主
- [x] `Distinct`
- [x] `Insert` / `Update` / `Delete`
- [x] `ConstantExecutor`
- [x] `RelationRename` + `RelationRenamePlan`
- [x] `Relational` 不透明 IR + `RelationalPlan`（外側結合・CTE などの避難所）
- [x] `DataChunk` ベクトル化、`SpillFile`、`QueryMemory`、`QueryScheduler`
- [x] `ZoneMap`（スキャン補助）

`JoinKind::kSemi` / `kAnti` はハッシュ結合実行器にある。**Cascades の
論理 Semi/Anti と実装規則は `DefaultImplementationRules` で接続済み**。

---

## P0 — 結合の欠け（計画品質の最大ギャップ）

### Merge / sort-merge

- [ ] `MergeJoinPlan` / `MergeJoinExecutor`（等価、両側昇順、NULL 処理）
- [ ] 複合キー、不等号残余フィルタ
- [ ] LEFT / RIGHT / FULL outer merge join
- [ ] SEMI / ANTI merge join（重複キーで probe を 1 回だけ）
- [ ] 片側未ソートなら `Sort` を子として要求する実装規則
- [ ] 興味深い順序を出力（join キー順）
- [ ] many-to-many の交差（マーク＆リセット）
- [ ] 非等価 merge（`a.x < b.y` のバンドジョイン）は後回しでも項目化:
      - [ ] inequality merge / band join

### Outer hash / nested loop

- [x] `LeftHashJoin`（ビルド右、マッチしなければ NULL pad）（`JoinKind::kLeftOuter` + `MaterializeLeftOuter()` + `outer_hash_join` 実装規則で実装済み）
- [x] `RightHashJoin`（`JoinKind::kRightOuter` + `MaterializeRightOuter()` + `right_hash_join` 実装規則で実装済み。Cascades で Left/Right 子を入替えて実現）
- [ ] `FullHashJoin`（両側未マッチ）
- [ ] spill 付き hybrid outer hash（既存 hybrid の拡張）
- [ ] `LeftNestedLoopJoin` / `Right` / `Full`
- [ ] outer の Semi ではない null-aware マッチ
- [ ] 右側フィルタが null-rejecting でないときの実行時 NULL 生成

### Semi / Anti / Mark / Single（実行器は部分的に既存）

- [x] Cascades から `JoinKind::kSemi` / `kAnti` を選ぶ物理規則（`semi_hash_join` / `anti_hash_join` in `DefaultImplementationRules`）
- [ ] `MarkJoin`（一致フラグ列を追加、`IN` の UNKNOWN）
- [ ] `SingleJoin` / `Max1Row`（スカラサブクエリ、2 行目でエラー）
- [ ] `NullAwareAntiJoin`（`NOT IN`、ビルド側 NULL の短絡）
- [ ] uniqueness 付き semi → inner に落とさない実行時 assert
- [ ] EXISTS 用 short-circuit nested loop（1 ヒットで内側打ち切り）

### その他の結合形

- [ ] `BlockNestedLoopJoin`（外側ブロック × 内側全走査）
- [ ] `BatchNestedLoopJoin`（外側キーを IN リスト化）
- [ ] `LookupJoin` の複合キー・範囲（IndexJoin の一般化）
- [ ] `ZigZagJoin`（両側インデックス）
- [ ] `LateMaterializationJoin`（RID 結合 → 列フェッチ）
- [ ] `DynamicFilterJoin` / sideway information passing（ブルームをスキャンへ）
- [ ] `HashSemiReduction`（巨大ファクトの前に次元を縮約）
- [ ] `CrossJoinUnnest`（1 行定数 × 表）
- [ ] `AsOfJoin` / `IntervalJoin`（時系列、将来）

---

## P0 — ソート・Top-N・順序

- [x] `SortPlan` を Cascades 実装規則から明示生成（エンジン安全網と二重適用禁止）`plan/sort_plan.hpp` + `plan/sort_plan.cpp` + `relational_factory.cpp` で実装
- [ ] `ExternalMergeSort` のプラン接続（実行器 `sort.cpp` の正式化）
- [ ] `InMemoryQuickSort` / `pdqsort` 経路
- [x] `TopNHeapPlan` / `TopNExecutor`（k 小さい ORDER BY LIMIT）`plan/topn_plan.hpp` + `plan/topn_plan.cpp` で実装
- [ ] `TopNPlusOffset`
- [ ] `WITH TIES` Top-N
- [ ] `IncrementalSort`（入力が prefix ソート済み）
- [ ] `PartialSort`
- [ ] `ClusteredIndexOrder` を FullScan が報告（PK 順ページ）
- [ ] `LimitPushdownScan`（unordered LIMIT の early stop）
- [ ] `SkipScanDistinct`（インデックスで DISTINCT キーを飛ばす）
- [ ] collation / nulls first/last を比較器に
- [ ] 安定ソート vs 非安定の文書化
- [ ] 並行ソート（モーセルマージ）

---

## P0 — 集約

- [x] `HashAggregatePlan` と `SortAggregatePlan` の分離選択（`AggregationPlan` が統一インタフェース）
- [ ] `StreamAggregate`（入力が group キー順）
- [ ] `PartialAggregate` + `FinalizeAggregate`（並列・分散）
- [ ] `DistinctAggregate`（`COUNT(DISTINCT x)` 専用パス）
- [ ] `TwoPhaseDistinctAgg`
- [ ] `MinMaxIndexScan`（INDEX ONLY で MIN/MAX）
- [ ] `FilteredAggregate`（`AGG(...) FILTER (WHERE …)`）
- [ ] `OrderedSetAggregate`（`PERCENTILE_CONT` / `WITHIN GROUP`）
- [ ] `GroupingSetsExecutor` / `Rollup` / `Cube`（Expand + Agg または複数 Agg）
- [ ] `ScalarAggEmptyInput`（0 行で COUNT=0、SUM=NULL）
- [x] spill 付きハッシュ集約の計画接続（並列集約は既存）`ParallelAggregationExecutor` で接続済み
- [ ] `MemoryLimitedAgg` の強制 spill
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
- [ ] `DummyScan` / `OneRow`（`SELECT 1`、既存 Constant の計画化）
- [ ] `ValuesScan`（多行 VALUES）
- [ ] `FunctionScan` / TVF
- [ ] `UnnestScan`
- [ ] `WorkTableScan`（再帰 CTE）
- [ ] `ForeignScan`（将来の外部データ）
- [ ] `FilteredScanPushdown` と残余フィルタの EXPLAIN 区別
- [ ] 複数レンジ IndexScan の大域順序保証（1 レンジ以外は順序なし、既存）の
      マージによる順序回復
- [ ] LSM / WiscKey 経路の `LsmScan` 実装規則

---

## P1 — DISTINCT・集合演算

- [ ] `HashDistinctPlan` / `HashDistinctExecutor`
- [ ] `SortDistinct`（ソート済み unique）
- [ ] `UnionAllExecutor`（n-ary append、スキーマ強制）
- [ ] `UnionDistinct`（hash または sort+unique）
- [ ] `Intersect` / `IntersectAll`
- [ ] `Except` / `ExceptAll`
- [ ] 集合演算の spill
- [ ] `MergeAppend`（ソート済み子供のマージ、PARTITION UNION）
- [ ] `Append`（順序なし UNION ALL）
- [ ] 子供への LIMIT 分配（UNION ALL）

---

## P1 — Window

- [ ] `WindowAggPlan` / `WindowExecutor`
- [ ] パーティション切替
- [ ] ROWS フレーム（BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 等）
- [ ] RANGE フレーム（ORDER BY キーの同値）
- [ ] GROUPS フレーム
- [ ] EXCLUDE CURRENT ROW / GROUP / TIES
- [ ] フレーム無しランキング（ROW_NUMBER / RANK / DENSE_RANK / NTILE）
- [ ] オフセット（LAG / LEAD / FIRST_VALUE / LAST_VALUE / NTH_VALUE）
- [ ] 累積 AGG（SUM/AVG OVER）
- [ ] 複数 window のソート共有
- [ ] ハッシュ・パーティション + ソート（メモリ）
- [ ] spill 付き window
- [ ] `QUALIFY` を Window の後段 Filter として実行

---

## P2 — UNNEST / 配列 / VALUES / 生成

- [ ] `UnnestExecutor`（配列の行展開）
- [ ] `WITH ORDINALITY`
- [ ] 複数 UNNEST の zip / 直積方針（GoogleSQL に合わせる）
- [ ] `UnnestJoin`（相関 UNNEST）
- [ ] `GenerateSeriesExecutor`
- [ ] `GenerateDateArray` 等
- [ ] `ValuesExecutor` 多列
- [ ] 空配列 UNNEST の 0 行 vs NULL 行

---

## P2 — CTE・再帰・マテリアライズ

- [ ] `MaterializePlan` / `EagerSpool`（CTE 複数参照）
- [ ] `LazySpool` / `ConsumerSpool`（再スキャン可能バッファ）
- [ ] `CteScan`（実体化された CTE の読み）
- [ ] `RecursiveUnion`（作業表の反復）
- [ ] 再帰の UNION vs UNION ALL
- [ ] サイクル検出オプション
- [ ] 再帰深さリミット
- [ ] `WorkTableIndex`（再帰結合用）
- [ ] `CacheSubquery`（非相関スカラを 1 回）

---

## P2 — DML・ロック

- [ ] `InsertSelectPlan`（SELECT 結果の一括挿入、計画接続）
- [ ] `BatchInsert`
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
- [ ] `Projection` の CSE（同一式を 1 回）
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

- [ ] 各結合種の NULL キー・空入力・1 行・重複キー
- [ ] outer join の null pad ゴールデン
- [ ] `NOT IN` vs anti join の三値論理
- [ ] Window フレーム境界
- [ ] 集合演算 ALL vs DISTINCT の多重度
- [ ] Top-N + OFFSET + ties
- [ ] spill 強制（メモリキャップを極端に下げる）
- [ ] 並列と直列の結果一致
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

1. Semi/Anti を Cascades 実装規則から既存 `HashJoin`+`JoinKind` へ接続。
2. `SortPlan` を明示し、Limit/Top-N と二重適用を潰す。
3. `TopNHeap`（ORDER BY LIMIT が TPC-H で効く）。
4. `MergeJoin` + interesting order。
5. `LeftHashJoin`（メモに `kOuterJoin` が入った直後）。
6. `HashAgg` vs `SortAgg` / `StreamAgg` の分離。
7. `Append` / `UnionAll`。
8. `WindowAgg` の最小セット（ROW_NUMBER + 累積 SUM）。
9. `Unnest`。
10. Bitmap スキャンと Min/Max インデックス。
