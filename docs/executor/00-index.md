# データベース実行エンジン

SQL の問い合わせは、求める結果を宣言する。実行エンジンは物理実行計画を受け取り、処理順、データ構造、メモリ量、並列度を具体的な処理として実現する。以下の文書では、行と列バッチの受け渡しから、走査、変換、結合、整列、集合演算、集約、ウィンドウ、再帰、分散、更新までを説明する。

対象には、物理計画が直接選択する演算子に加えて、計画境界のラッパー、診断用演算子、二段集約の中間演算子、分散実行用の交換、更新文の実行器を含める。演算子の動作を支えるバッチ、メモリ管理、スピルの仕組みも扱う。

## 読む順序

1. [実行モデル](10-execution-model.md) — 要求駆動型（pull 型）の反復子、ベクトル化、パイプライン、物理特性、誤りの伝播
2. [データ表現と実行時資源](20-data-runtime.md) — 列バッチ、選択ベクトル、ゾーンマップ、式評価、メモリ、スピル、スケジューリング
3. [走査と行生成](30-scan-source.md) — 表・索引・ビットマップ・行位置・遠隔入力・定数入力
4. [単項変換と制御](40-unary-control.md) — 選択、射影、制限、実体化、行数検査、計測、UNNEST
5. [結合](50-join.md) — 入れ子ループ、索引、ハッシュ、マージ、相関、時系列・区間結合
6. [整列・重複除去・集合演算](60-sort-distinct-set.md) — `Sort`、Top-N、`Distinct`、集合演算、マージ連結
7. [集約](70-aggregation.md) — 単一・並列・二段・分散・グルーピング集合・辞書集約
8. [ウィンドウ・再帰・交換](80-window-recursion-distribution.md) — ウィンドウフレーム、再帰 CTE、交換、遠隔入力、関係単位の実行境界
9. [データ変更](90-data-modification.md) — 挿入、更新、削除、全行削除、`MERGE`、`RETURNING`

## 各演算子で確認する項目

各演算子は、可能な限り次の項目で説明する。

- **入力契約**: 子が満たすべきスキーマ、順序、索引、行位置など。
- **処理方式**: 1 行ずつ、バッチ単位、全入力の実体化、あるいは複数ワーカーのどれか。
- **出力契約**: 列、順序、重複、`NULL`、物理行位置がどう変わるか。
- **資源特性**: 時間計算量、作業メモリ、スピル、早期終了の可否。
- **失敗契約**: 入力終了と実行失敗の区別、式評価や制約検査の失敗の扱い。

記号として、入力行数を `N`、左右の入力行数を `L`, `R`、グループ数を `G`、保持する上位件数を `K`、ワーカー数を `P` と書く。計算量は主要項だけを示し、式評価、値のコピー、可変長データの比較コストは必要に応じて別記する。

## 演算子網羅表

### 走査・入力

| 演算子 | 役割 | 詳細 |
|---|---|---|
| `FullScan` | 可視な表行を順次走査する | [表の全走査](30-scan-source.md#2-fullscan表の全走査) |
| `ChunkedScan` | ページ範囲または索引範囲をモーセル単位で読む | [チャンク走査](30-scan-source.md#3-chunkedscanモーセル単位の走査) |
| `ParallelScan` | ページ範囲を複数ワーカーで動的分配する | [並列走査](30-scan-source.md#4-parallelscan並列表走査) |
| `IndexScan` | キー範囲を順方向または逆方向に読む | [索引走査](30-scan-source.md#5-indexscan範囲索引走査) |
| `IndexOnlyScan` | 索引キーと付加列から出力を組み立てる | [索引のみ走査](30-scan-source.md#6-indexonlyscan索引のみ走査) |
| `BitmapIndexScan` | 索引条件を物理行位置集合へ変換する | [ビットマップ走査](30-scan-source.md#7-bitmapindexscan-と-bitmapheapscan) |
| `BitmapHeapScan` | 行位置集合を並べ、表から行を再検査する | [ビットマップ走査](30-scan-source.md#7-bitmapindexscan-と-bitmapheapscan) |
| `TidScan` | 明示された物理行位置を直接読む | [行位置走査](30-scan-source.md#8-tidscan行位置の直接走査) |
| `SkipScanDistinct` | 索引上で重複キー区間を飛び越える | [スキップ走査](30-scan-source.md#9-skipscandistinct-と-indexskipscanexecutor) |
| `SkipScanDistinctExecutor` | `SkipScanDistinct` の公開別名 | [スキップ走査](30-scan-source.md#9-skipscandistinct-と-indexskipscanexecutor) |
| `IndexSkipScanExecutor` | スキップ走査を計画表示境界で包む | [スキップ走査](30-scan-source.md#9-skipscandistinct-と-indexskipscanexecutor) |
| `MinMaxIndexExecutor` | 順序付き入力の先頭非 `NULL` 値を返す | [極値走査](30-scan-source.md#10-minmaxindexexecutor) |
| `ValuesExecutor` | 明示された定数行を返す | [定数入力](30-scan-source.md#11-定数空行および数列の入力) |
| `ConstantExecutor` | 保持済み行を診断名付きで返す | [定数入力](30-scan-source.md#11-定数空行および数列の入力) |
| `EmptyResultExecutor` | 0 行を返す | [定数入力](30-scan-source.md#11-定数空行および数列の入力) |
| `GenerateSeriesExecutor` | 等差数列を生成する | [定数入力](30-scan-source.md#11-定数空行および数列の入力) |
| `RemoteScan` | 受信チャネルまたは保持済みバッチを読む | [遠隔走査](30-scan-source.md#12-remotescan遠隔入力) |
| `AnalyzeScan` | 走査しながら列統計を構築する | [統計走査](30-scan-source.md#13-analyzescan統計収集走査) |

### 単項変換・制御

| 演算子 | 役割 | 詳細 |
|---|---|---|
| `Selection` | 三値論理の述語で行を選ぶ | [選択](40-unary-control.md#1-selection) |
| `Projection` | 式から出力列を作る | [射影](40-unary-control.md#2-projection) |
| `LimitExecutor` | オフセット後の行数を制限する | [制限](40-unary-control.md#3-limitexecutor) |
| `MaterializeExecutor` | 入力を保持し、巻き戻し可能にする | [実体化](40-unary-control.md#4-materializeexecutor) |
| `Max1RowExecutor` | スカラー副問い合わせの多行を拒否する | [最大 1 行検査](40-unary-control.md#5-max1rowexecutor) |
| `RelationRenameExecutor` | スキーマ名だけを論理的に付け替える | [透過ラッパー](40-unary-control.md#6-透過ラッパー) |
| `RetainedExecutor` | コンパイル済み計画の寿命を実行ストリームへ結び付ける | [透過ラッパー](40-unary-control.md#6-透過ラッパー) |
| `CardinalityProbe` | 通過行数と経過時間を記録する | [計測](40-unary-control.md#7-cardinalityprobe) |
| `ScheduledExecutor` | 最初の読み出し時に資源の利用許可を取得する | [スケジューリング](40-unary-control.md#8-scheduledexecutor) |
| `UnnestExecutor` | 配列要素を行へ展開する | [配列展開](40-unary-control.md#9-unnestexecutor) |

### 結合

| 演算子 | 役割 | 詳細 |
|---|---|---|
| `CrossJoin` | 直積を作る | [直積](50-join.md#2-crossjoin) |
| `NestedLoopJoin` | 右入力を保持して全候補対を検査する | [入れ子ループ](50-join.md#3-nestedloopjoin) |
| `BatchNestedLoopJoin` | 左入力をブロック化した入れ子ループを行う | [バッチ入れ子ループ](50-join.md#4-batchnestedloopjoin) |
| `IndexJoin` | 左キーごとに右索引を探索する | [索引結合](50-join.md#5-indexjoin) |
| `HashJoin` | ハッシュ表と必要時の分割スピルで結合する | [ハッシュ結合](50-join.md#6-hashjoin) |
| `SharedBuildParallelHashJoin` | 共有ハッシュ表を並列構築・探索する | [並列ハッシュ結合](50-join.md#7-sharedbuildparallelhashjoin) |
| `MergeJoin` | 整列済みキー列を突き合わせる | [マージ結合](50-join.md#8-mergejoin) |
| `ParallelMergeJoin` | キー範囲を分割して並列マージする | [並列マージ結合](50-join.md#9-parallelmergejoin) |
| `ApplyExecutor` | 外側行ごとに相関内側計画を実行する | [Apply](50-join.md#10-applyexecutor) |
| `AsOfJoin` | 時刻条件を満たす直近行を選ぶ | [AS OF 結合](50-join.md#11-asofjoin) |
| `IntervalJoin` | 等値キーと時刻区間で候補を列挙する | [区間結合](50-join.md#12-intervaljoin) |

### 整列・重複除去・集合

| 演算子 | 役割 | 詳細 |
|---|---|---|
| `SortExecutor` | 全体順序を作り、必要なら外部マージする | [全体整列](60-sort-distinct-set.md#2-sortexecutor) |
| `TopNExecutor` | オフセットを含む上位 `K` 行を求める | [Top-N](60-sort-distinct-set.md#3-topnexecutor) |
| `PartialSortExecutor` | 各固定ブロックだけを整列・切り出しする | [部分整列](60-sort-distinct-set.md#4-partialsortexecutor) |
| `IncrementalSortExecutor` | 既整列の接頭辞ごとに残りのキーを整列する | [増分整列](60-sort-distinct-set.md#5-incrementalsortexecutor) |
| `DistinctExecutor` | 最初に現れた異なるキーだけを通す | [重複除去](60-sort-distinct-set.md#7-distinctexecutor) |
| `SortDistinctExecutor` | 隣接する同値行を除く | [整列済み重複除去](60-sort-distinct-set.md#8-sortdistinctexecutor) |
| `SetOperationExecutor` | `UNION`、`INTERSECT`、`EXCEPT` を実行する | [集合演算](60-sort-distinct-set.md#9-setoperationexecutor) |
| `UnionAllExecutor` | `SetOperationExecutor` の別名で全枝を連結する | [集合演算](60-sort-distinct-set.md#9-setoperationexecutor) |
| `MergeAppendExecutor` | 整列済みの複数入力を k 路マージする | [マージ連結](60-sort-distinct-set.md#10-mergeappendexecutor) |

### 集約・ウィンドウ

| 演算子 | 役割 | 詳細 |
|---|---|---|
| `AggregationExecutor` | グループなしの集約結果を 1 行作る | [単一集約](70-aggregation.md#3-aggregationexecutor) |
| `ParallelAggregationExecutor` | ワーカーの部分状態を番号順に併合する | [並列集約](70-aggregation.md#4-parallelaggregationexecutor) |
| `PartialAggregate` | グループ別の可併合中間状態を作る | [二段集約](70-aggregation.md#5-partialaggregate-と-finalizeaggregate) |
| `PartialAggregateExecutor` | `PartialAggregate` の公開別名 | [二段集約](70-aggregation.md#5-partialaggregate-と-finalizeaggregate) |
| `FinalizeAggregate` | 複数の中間状態を最終値へ畳み込む | [二段集約](70-aggregation.md#5-partialaggregate-と-finalizeaggregate) |
| `FinalizeAggregateExecutor` | `FinalizeAggregate` の公開別名 | [二段集約](70-aggregation.md#5-partialaggregate-と-finalizeaggregate) |
| `TwoPhaseDistinctAgg` | グループ別の重複除去用集合と通常状態を保持する | [`DISTINCT` 集約](70-aggregation.md#6-twophasedistinctagg) |
| `TwoPhaseDistinctAggExecutor` | `TwoPhaseDistinctAgg` の公開別名 | [`DISTINCT` 集約](70-aggregation.md#6-twophasedistinctagg) |
| `GroupingSetsExecutor` | 複数のグルーピング集合を独立集約する | [グルーピング集合](70-aggregation.md#7-groupingsetsexecutor) |
| `DistributedAggFinalize` | 複数ストリームの部分集約を最終化する | [分散最終化](70-aggregation.md#8-distributedaggfinalize) |
| `WindowExecutor` | パーティション、順序、フレームごとの値を付加する | [ウィンドウ](80-window-recursion-distribution.md#1-windowexecutor) |

### 再帰・交換・実行境界

| 演算子 | 役割 | 詳細 |
|---|---|---|
| `RecursiveCteExecutor` | 差分ワークテーブルで不動点を求める | [再帰 CTE](80-window-recursion-distribution.md#2-recursivecteexecutor) |
| `ExchangeExecutor` | ハッシュ・範囲・複製・集約で行を再配置する | [交換](80-window-recursion-distribution.md#3-exchangeexecutor) |
| `PartitionScanExecutor` | Exchange の一つの区画を読む内部カーソル | [交換](80-window-recursion-distribution.md#4-partitionscanexecutor) |
| `RelationalExecutor` | 関係単位の評価系を要求駆動型の境界へ接続する | [実行境界](80-window-recursion-distribution.md#6-relationalexecutor) |
| `GroupedFinishExecutor` | 中核計画の結果を実体化し、後段処理を完了する | [実行境界](80-window-recursion-distribution.md#7-groupedfinishexecutor) |

### データ変更

| 演算子 | 役割 | 詳細 |
|---|---|---|
| `Insert` | 挿入、無視、`upsert`、置換を実行する | [挿入](90-data-modification.md#2-insert) |
| `Update` | 行位置に対して新しい行像を適用する | [更新](90-data-modification.md#3-update) |
| `DeleteExecutor` | 行位置で対象行を削除する | [削除](90-data-modification.md#4-deleteexecutor) |
| `TruncateExecutor` | 全行位置を列挙して削除する | [全削除](90-data-modification.md#5-truncateexecutor) |
| `MergeExecutor` | 一致・不一致ごとの更新、削除、挿入を行う | [MERGE](90-data-modification.md#6-mergeexecutor) |
| `ReturningExecutor` | 変更対象行から返却式を計算する | [RETURNING](90-data-modification.md#7-returningexecutor) |

## 演算子ではないが実行を規定する部品

`DataChunk`、`ColumnVector`、`SelectionVector`、`ZoneMap`、`VectorizedExpression`、`SimdComparisonKernel`、`SpillFile`、`QueryMemoryBudget`、`OperatorMemoryReservation`、`NumaArena`、`QueryScheduler`、`PipelineBreaker` は、物理演算子ではない。これらは行の表現、述語の実行方法、メモリ超過時の動作、並列実行時の資源配分を決める。[データ表現と実行時資源](20-data-runtime.md)で個別に説明する。

`DictionaryBatchAggregation` と `PdqSort` も要求駆動型の演算子ではなく、物理演算子から利用される専用カーネルである。それぞれ[集約](70-aggregation.md#9-dictionarybatchaggregation)と[整列](60-sort-distinct-set.md#6-pdqsort)で説明する。
