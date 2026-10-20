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
- [x] `LookupJoin`（旧 `IndexJoin`）+ エイリアス付き自己結合
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
- [x] 非等価 merge（`a.x < b.y` のバンドジョイン）を実装:
      - [x] inequality merge / band join（inner、単一の符号付きINT64/DATE順序比較。NULL・重複・空入力・エラー・順序・行数を固定）
      - [x] `join_commutativity` で子の順序が逆になった Planning でも band join の比較を鏡像化する（`TryBandJoinSpec`。これまでは候補範囲が「書かれた演算子」のまま残り、
            残差再検査と食い違い一致行を落としていた）

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
- [x] outer の Semi ではない null-aware マッチ
- [x] 右側フィルタが null-rejecting でないときの実行時 NULL 生成（残余 ON は match 判定内で評価し、全候補不成立の preserved side も pad）

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
- [x] `LookupJoin` の複合キー・範囲（等値prefix＋range bound 1個、INT64/DATE）
- [x] `ZigZagJoin`（両側インデックス、INNER、単一署名付きINT64キー、等値または単一range）
- [x] `LateMaterializationJoin`（equi/unique winnerの索引キーと`RowPosition`だけを先に保持し、payloadは上位段で`Table::Read`。内部結合、符号付きINT64/DATEキー、NULL/重複/空入力/unique/error、row positionと順序を固定。内部key probeは左major、rowid明示形は内部専用でSQL rowid表面は持たない。array/outer/semi/anti/NULL-safe/巨大候補・stale indexは拒否またはfallback）
- [ ] `DynamicFilterJoin` / sideway information passing（ブルームをスキャンへ）
- [x] `HashSemiReduction`（巨大ファクトの前に次元を縮約。`HashJoin::EnableSemiReduction` は
      存在判定だけを使う semi / anti / null-aware anti だけを対象にし、probe 側の等値キーを先に
      `JoinHashIndex` でドメイン化してから build を取り込む。probe 側ドメインの外にある build 行は
      intake 中に捨て、build キー重複も1行に畳む。`NOT IN` の三値論理と「build 側が空」は *生の*
      build cardinality / NULL フラグで判定するので縮約の影響を受けない。domain が
      `kSemiReductionMaxDomainKeys` を超えたら解放して非制限 intake に fallback し、build キー
      重複除去だけを残す。論理側は `semi_reduction_pushdown_semi_join` /
      `semi_reduction_pushdown_anti_join`、`ProductPlan::PreferSemiReduction`、
      `plan/product_plan.cpp` の `SemiReductionIsProfitable` を通る。回帰は
      `executor/hash_semi_reduction_test.cpp` と `plan/cascades_test.cpp` の
      `SemiReductionPushdown*`。詳細は `docs/todo.md` Phase 2 Join。）
- [x] `CrossJoinUnnest`（定数/相関配列を通常の cross/lateral 行源として結合、SQL 回帰あり）
- [x] `AsOfJoin` / `IntervalJoin`（時系列、将来）(2026-10-03 削除: 生成元なし)

---

## P0 — ソート・Top-N・順序

- [x] `SortPlan` を単一リレーションおよび一般 Cascades 最適化経路から明示生成
      （`IsOrderedBy` でエンジン安全網との二重適用を防止）
- [x] `ExternalMergeSort` のプラン接続（`SortPlan` が既存
      `SortExecutor` を駆動。単一キー radix / 複合キー安定ソートも既存実装）
- [x] `InMemoryQuickSort` / `pdqsort` 経路
- [x] `TopNPlan` / `TopNExecutor`（ヒープで `OFFSET + LIMIT` 件だけ保持）
- [x] `TopNPlusOffset`（TopNの容量と出力位置で対応）
- [x] `WITH TIES` Top-N（TopNExecutor の ties 境界拡張）
- [x] `IncrementalSort`（入力が prefix ソート済み）
- [x] `PartialSort`
- [x] `ClusteredIndexOrder` を FullScan が報告（PK 順ページ）。順序は「測定」であり
      保証ではない: `plan/full_scan_plan.hpp` の `ClusteredOrderColumn()` が UNIQUE
      単一キー + NOT NULL + ANALYZE の相関 1.0 + 未変化の `Table::LayoutGeneration()`
      （+ in-place 書き換えの latch が立っていない）をすべて満たすときだけ昇順を
      報告し、`FullScanPlan::EmitExecutor` はその場合 single-threaded な
      chain-ordered `FullScan` を保つ（`ParallelScan` は保存順でない）。EXPLAIN には
      `FullScan: t (clustered order: id ASC)`。詳細は `docs/todo.md` Phase 2 Sort /
      Top-N。残件: モーセル並列のまま順序を保つ scan（`ParallelScan` のモーセル順
      出力）を作れば並列閾値以上の表でも報告できる。
- [x] `LimitPushdownScan`（unordered LIMIT の OFFSET + LIMIT 上限を FullScan が
      受け取り、スカラー/バッチ双方で early stop）
- [x] `SkipScanDistinct`（インデックスで DISTINCT キーを飛ばす）
- [x] collation / nulls first/last を比較器に（Sort / TopN と SQL の明示
      `NULLS FIRST` / `NULLS LAST` を接続。collation の多言語拡張は別項目）
- [x] 安定ソート vs 非安定の文書化（SortExecutor は安定ソートを保証）
- [x] 並行ソート（モーセル分割と安定マージを `ParallelSort` 経路で実行）

---

## P0 — 集約

- [x] `HashAggregatePlan` と `SortAggregatePlan` の分離選択（スカラー集約の
      物理 plan 型と `AggregationStrategy` を分離。GROUP BY payload の
      Cascades 接続は下の `StreamAggregate` と同時に完了）
- [x] HashAgg と SortAgg の**アルゴリズム**分離（state / order / spill /
      parallel / empty / NULL / duplicate / ties / 子エラーを形ごとに固定。
      `PeakResidentGroups()` が state の差（HashAgg = group table の peak、
      SortAgg = 常に 1）、Dump が spill の差（`spill=partitioned,
      partitions=N` vs `run_rows=R, window=W`）、順序の承諾は plan 側の
      `IsOrderedBy`。選択は group 数が `AggregateGroupCapacity()` を超えると
      sort が安くなる memory 契約のコスト。
      `plan/aggregate_shape_test.cpp` / `executor/grouped_aggregate_test.cpp`）
- [x] `StreamAggregate`（入力が group キー順。GROUP BY キーは
      `AggregatePayload.group_keys` として論理ペイロードへ入り、規則
      `aggregation` が順序を供給する子にだけ第3代替を提示する。実行器は
      `require_ordered_input` で契約違反を検出してエラーにする。
      `plan/aggregate_shape_test.cpp` / `executor/grouped_aggregate_test.cpp`）
- [x] `PartialAggregate` + `FinalizeAggregate`（並列・分散）
- [x] `DistinctAggregate`（逐次・並列 `Aggregation` が aggregate metadata の
      DISTINCT 集合を保持して `COUNT(DISTINCT x)` を処理）
- [x] `TwoPhaseDistinctAgg`(2026-10-03 削除: 経路なし。COUNT DISTINCT は並列2相集約が担う)
- [x] `MinMaxIndexScan`（非NULLな単一キー列をINDEX ONLYの先頭1件から取得）
- [x] `FilteredAggregate`（GoogleSQL `AGG(x WHERE …)`。直列・並列集約と
      `COUNT(*)` のフィルタ順序を回帰）
- [ ] SQL 標準 `AGG(...) FILTER (WHERE …)` の AST 接続
- [x] `OrderedSetAggregate`（`PERCENTILE_CONT` / `WITHIN GROUP`）
- [x] `GroupingSetsExecutor` / `Rollup` / `Cube`（Expand + Agg または複数 Agg）
- [x] `ScalarAggEmptyInput`（0 行で COUNT=0、SUM=NULL。AVG/MIN/MAX の NULL も
      含め、逐次・並列集約で回帰テスト済み）
- [x] spill 付きハッシュ集約の計画接続（Relational fallback の partitioned aggregation、予算超過回帰テストあり）
- [x] `MemoryLimitedAgg` の強制 spill（grouped relational aggregation が
      `QueryMemoryBudget` を監視して hash partition + `SpillFile` に切り替え。
      `Relational_WithPartitionedAggregationUnderBudget_SpillsToDisk` で回帰）
- [x] bitwise / bool_and / bool_or のベクトル化パス
- [x] `ANY_VALUE` 短絡

---

## P1 — スキャンとアクセスパス

- [x] `BitmapIndexScan` + `BitmapHeapScan`（単一キーの複数インデックス OR/AND を optimizer に接続）
- [x] `BitmapAnd` / `BitmapOr` オペレータ
- [x] `TidScan` / `RowIdLookup` / `CTID` 相当(2026-10-03 削除: SQL/計画経路からの参照がなく到達不能)
- [ ] `IndexSkipScan`（先頭列が等式でない複合インデックス）
- [x] `IndexOnlyScan` の visibility map 相当（MVCC で heap fetch 省略を広げる）。
      `IndexScanIterator::TryReadIndexOnly` が非保持インデックス＋チェーンフリー頁を
      B+Tree 像（キー＋INCLUDE）から直接返す。回帰:
      `IndexScanIteratorTest.IndexOnlyVisibilityMap*`、
      `ExecutorTest.IndexOnlyScanServesVisibilityMapWithoutHeapFetch`、
      `SqlClusteredOrderTest.CountStarIndexOnlyScanServesVisibilityMap`
- [x] `CoveringIndex` 選択のコスト精度。
      `ScanAlternatives` が covering（IndexOnly）候補をラップ前に検出し、
      クラスタリング罰を適用せず `kCoveringIndexCostFactor`(0.5) を掛ける。
      回帰: `OptimizerTest.CoveringIndexWinsOverNonCoveringIndexForTheSameRange` /
      `CoveringIndexRangeSkipsTheClusteringPenalty`
- [ ] `PAXScan` / カラムナ・スキャンの Cascades 接続
- [ ] `ZoneMapSkipScan`（既存 zone map をプラン属性に）
- [ ] `LateMaterializationScan`（まずキー、後でワイド列）
- [x] `PrefetchingIndexScan`。
      直列 `IndexScan` が `kIndexScanPrefetchDistance`(8) 先のヒープ頁を
      先読みウォームする(先読みカーソル+`index_scan_prefetch_pages` ゲージ、
      hatch `TINYLAMB_DISABLE_INDEX_SCAN_PREFETCH`)。回帰:
      `ExecutorTest.IndexScanPrefetchesHeapPagesAhead`
- [x] `ParallelIndexScan` + `RangePartitionedScan`（リーフ境界でのキー範囲分割）。
      `BPlusTree::ScanPartitionBoundaries` がリーフ鎖の先頭キーを返し、
      `IndexScanIterator::PartitionBoundaries` が復号。`ParallelIndexScan` が
      モーセル順ハンドオフで直列と同じキー順・行・位置を返す。閾値
      `kParallelIndexScanMinRows`(8192)、hatch `TINYLAMB_DISABLE_PARALLEL_INDEX_SCAN`。
      回帰: `ParallelIndexScanTest.*`、`IndexScanIteratorTest.PartitionBoundariesTileTheLeafChain`、
      `OptimizerTest.ParallelIndexScanEmittedForLargeIndexRange`
- [ ] `SampleScan`（BERNOULLI / SYSTEM）
- [x] `DummyScan` / `OneRow`（`DummyScanPlan` + `ValuesExecutor`。SELECT 1 を
      Optimizer 経路で実行）
- [x] `ValuesScan`（多行 VALUES の `ValuesPlan` / `ValuesExecutor`）
- [x] `FunctionScan` / TVF（関数由来の配列を `LoadSource` が行源化）。
      組み込み TVF `GENERATE_SERIES(...)` を FROM で受けると、フロントエンドが
      その配列の暗黙 UNNEST（`SelectSource.unnest`）へ落とすので、専用スキャン
      演算子は持たない: リレーショナル経路は `LoadSource`、Cascades 経路は
      `kUnnest`（＋`UnnestPlan`）が実行する。名前付き引数は位置引数へ並べ替え、
      未別名の値列は `value`。回帰:
      `QueryTest.BuiltinTableFunctionBecomesRowSource`。残り:
      `EXTERNAL_QUERY` / `ML.*` などは外部ソース基盤が前提。
- [x] `UnnestScan`（配列、STRUCT/PROTO、WITH OFFSET、相関 lateral 経路）
- [x] `WorkTableScan`（再帰 CTE。反復ごとに CteMap の CTE 名を差分 Relation に
      束縛する形で実装。作業テーブル側は `WorkTableIndex` が索引する）
- [ ] `ForeignScan`（将来の外部データ）
- [x] `FilteredScanPushdown` と残余フィルタの EXPLAIN 区別（IndexScan の predicate
      と、非カバー条件を Selection に残す経路を分離）
- [x] 複数レンジ IndexScan の大域順序保証（1 レンジ以外は順序なし、既存）の
      マージによる順序回復。point-union と OR-of-prefixes はレンジがソート済み・
      非重複なので、`ScanAlternatives` が全キー列を provided order として提示し、
      ORDER BY が Sort なしで満たされる（マージ不要: 逐次スキャンが既に大域順序）。
      回帰: `OptimizerTest.InListPointUnionKeepsKeyOrderWithoutSort` /
      `DisjunctivePrefixRangesKeepKeyOrderWithoutSort` /
      `SqlClusteredOrderTest.InListPointUnionKeepsKeyOrder`
- [x] LSM / WiscKey 経路の `LsmScan` 実装規則。
      `LogicalOperator::kLsmScan`（葉・ツリー参照＋行数推定）＋ `LsmScanPlan`
      （出力 = key/value の VARCHAR、キー昇順を主張）＋ `LsmScan` 実行器
      （`LSMView` スナップショット、世代最新勝ち・tombstone スキップ）＋
      `lsm_scan` implementation rule。**SQL からの到達経路は未接続**
      （plan-level API/テストから直接 Memo を構築する。ビューは登録済み run
      のみを対象とするため、最新書き込みは `Sync` 後に見える）。
      回帰: `LsmScanTest.*`（キー順の live pair、規則→plan→executor の
      end-to-end、空ツリー、Fingerprint 分離）

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
- [x] RANGE INTERVAL オフセット（DATE キーのカレンダ窓:
      `RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW`。
      MONTH/YEAR は `DATE + INTERVAL` と同じ月末クランプ、
      DATE 以外のキー+INTERVAL は明示エラー。
      `WindowSqlCov.RangeIntervalFramesOnDateKeys`）
- [x] GROUPS フレーム（peer group 単位の preceding/current/following 境界と回帰テスト）
- [x] EXCLUDE CURRENT ROW / GROUP / TIES
- [x] フレーム無しランキング（ROW_NUMBER / RANK / DENSE_RANK / NTILE）
- [x] オフセット（LAG / LEAD / FIRST_VALUE / LAST_VALUE / NTH_VALUE）
- [x] 累積 AGG（SUM/AVG OVER）
- [x] 複数 window のソート共有
- [x] ハッシュ・パーティション + ソート（メモリ）
- [x] spill 付き window
      入力 drain を PARTITION BY ハッシュで `spill_partition_count` 個の
      `SpillFile` に振り分け、1ファイルずつ常駐で評価・出力する（`WindowPlan::
      Memory()` → `WindowExecutor::Options`）。buffered 形は入力順と ordering
      claim を保ち、spill 形は claim を捨てる（`WindowPlan::IsOrderedBy`）。
      PARTITION BY が呼び出し間で異なる場合は buffered に固定。演算子側の回帰は
      `executor/window_spill_test.cpp`、plan 側は `plan/window_shape_test.cpp`。
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
- [x] 再帰項の BY NAME / CORRESPONDING アライメント（各ラウンドで項出力を
      蓄積側の列名へ整列してから dedupe/追加。`SelectStatement::MatchFor(term)`
      で項対応の match を解決し、visitor は `BY NAME` / `STRICT CORRESPONDING`
      / `CORRESPONDING` / `ON/BY (col_list)` を per-pair で記録する。契約は
      「anchor が CTE の形を決める」: 名前一致は値の並びだけを変え、項が
      anchor 列を作れない・重複名・無名列は NULL 埋めせずエラー。素の
      `CORRESPONDING` は項だけの列を落とし、`ON/BY (col_list)` は working
      shape を覆う必要がある。`WITH DEPTH` の depth 列は整列後の末尾。
      回帰: `SqlEngineRecursiveCteNameMatching*`,
      `SqlEngineRecursiveCteJoinUsingFeedsNameMatching`）
- [x] CTE 解決の依存順（WITH RECURSIVE 内の前方参照を許可。兄弟定義への参照を
      トポロジカルに解決し、相互再帰はエラー化）。依存走査は深く行う
      （`executor/detail/subquery_runtime` の `CollectStatementTableRefs`:
      派生表・スカラーサブクエリ・集合演算の枝・入れ子 WITH 本体まで。
      `CollectCteBodyOuterRefs` がエントリごとの依存集合を作り、自分の名前は
      外側へ解決するので依存に数えない）。相互参照は
      `CteCycleStatus` の "cyclic CTE reference among a, b" になり、
      `ValidateCteDependencies` が `PrepareStatement` で先に弾く（実行器の
      CTE ループは手組み Statement 用の backstop）。再帰CTEの本体が兄弟 CTE を
      読む場合は `ExecuteUnnestSelect` の `RecursiveCteLiftable` ゲートで
      Cascades の work-table leaf を諦め、依存順リレーショナル経路へ落とす。
      回帰: `SqlEngineCteDependencyOrderFollowsNestedReferences`,
      `SqlEngineRecursiveCteReadsSiblingCteInDependencyOrder`,
      `SqlEngineCyclicCteReferenceRejected`
- [x] テンプレート再構築でも再帰メタデータを保持（BindSelect が宣言順
      `WithQueryOrder()` でバインドし、`AddRecursiveWithQuery` /
      `SetRecursiveDepth` を再適用。従来はプランキャッシュ経由で recursive
      フラグが落ちて自己参照が unknown table になっていた）。さらに、その
      再構築が実際に走るように 2 つのバインド失敗を修正: 集合演算が flat な
      `UnionAll()` と `SetOperationTree` を同じ枝ノードで共有するため両方を
      バインドして全リテラルを二重消費していた（tree は flat の結果を共有し、
      `first` はヘッドのパラメータ範囲から複製）。`WITH DEPTH` の上下限は
      メタデータに載るがテキスト上は整数リテラルなので、`RecursiveDepthSpec`
      の `*_is_literal` を見て本文の直後に消費し、新しい値を書き戻す
      （`UNBOUNDED` は消費しない）。複数エントリ WITH でリテラル深さ上限が
      ある場合は宣言順を復元できないためテンプレート化を拒否。キャッシュの
      実効性は `PlanCacheStats().template_fills` / `template_hits` で観測。
      回帰: `TemplateCacheKeepsRecursiveCteMetadata`,
      `SqlTemplateTest.BindsRecursiveWithSetOperationOnce`,
      `SqlTemplateTest.BindsRecursiveWithDepthBounds`
- [x] `WorkTableIndex`（作業テーブル索引）。`UNION DISTINCT` 再帰の重複判定を
      作業テーブル（累積結果）のハッシュ索引で行う。従来は既出全行の線形走査で
      結果サイズに対して二次（320k 行の再帰が 3分17秒 → 0.27 秒）。ピア規則は
      DISTINCT と同じ（`EqualUnderCollation` + `CanonicalDistinctValue`: NaN は
      1 ピア、-0.0 == +0.0、大文字小文字非依存は畳み込み、NULL == NULL）で、
      `DistinctOf` と実装を共有する。**結合アクセス経路ではない**: CTE 名は前
      ラウンドのデルタに束縛される（BigQuery の working-table 契約）ので、各
      ラウンドの結合はそのデルタを join 演算子がハッシュ化し、再帰項は累積
      テーブルを読んではならない。ループ自身が必要とする索引は「どの行が新規か」
      であり、それがこの構造。`WorkTableIndexStats()` の `inserts`/`duplicates`
      でループが索引を引いたことをテストから観測できる。
      回帰: `WorkTableIndexTest.*`（8 件）, `SqlEngineRecursiveCteDedupesThroughWorkTableIndex`
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
- [x] `Upsert` / `ON CONFLICT`
- [x] `MergeExecutor`（MATCHED / NOT MATCHED）
- [ ] `UpdateFrom` / `DeleteUsing`（結合更新）
- [x] `ReturningExecutor`
- [ ] `LockRows` / `SELECT FOR UPDATE`
- [ ] `SkipLocked` / `NOWAIT`
- [x] `Truncate` 実行器（カタログ操作との境界）
- [ ] トリガ相当の before/after 行フック（将来）

---

## P3 — ベクトル化・パイプライン・並列

- [ ] 全オペレータの `DataChunk` 入出力統一（まだ行型が残る箇所の洗い出し）
- [ ] Selection vector / validity bitmap の徹底
- [ ] late materialization の列バッチ
- [ ] `PipelineBreaker` 明示（Hash build、Sort、Materialize）
- [ ] morsel-driven 並列の Cascades DOP 接続（`QueryScheduler` 既存）
- [ ] `Exchange` / `Gather` / `Redistribute` 実行器（単一ノードはスレッド間）
- [x] `ParallelHashJoin`（共有ビルド表）
- [x] `ParallelMergeJoin`
- [ ] `ScanPartition` ステアリング
- [x] NUMA 意識のアリーナ
- [x] SIMD 比較カーネル（INT64 は JIT と役割分担）
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

- [x] 複合インデックスの不等式 + 等式ミックス（等式 prefix + 同一キー列の両側境界。
      `LookupSeek::second_range_left_column` / `second_range_operation`、
      `ProductPlan::SetLookupRange` の2回目呼び出し、`FindLookupRanges`、
      `LookupJoin` の区間 seek。`probe:{a, lo < b < hi}` として EXPLAIN に出る。
      同側の2個目・3個目の境界と後続キー列の境界は残差へ明示 fallback）
- [ ] covering 判定の複数インデックス
- [ ] `NestedLoopIndexJoin` のプリフェッチ
- [ ] 内側フィルタをインデックスに落とせず heap に残す明示
- [ ] unique 内側の 1 行ルックアップ短絡
- [ ] 外側がソート済みなら内側ルックアップの局所性

---

## P5 — サンプリング・統計収集実行

- [x] `AnalyzeScan`（ヒストグラム構築用）
- [x] `ReservoirSample`
- [ ] `BlockSample`
- [x] HyperLogLogによる統計収集（`AnalyzeScan` 内部で使用）
- [x] 実行時フィードバック用 `CardinalityProbe`（デバッグ）

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
- [ ] `DistributedAggFinalize`(2026-10-03 削除: 実分散実行は未実装)

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
