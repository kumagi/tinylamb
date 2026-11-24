# クラウド・BI ワークロードの命題 42 件（第九波・証明済み）

全 42 件について提出型を変更せず `theorem sound_<宣言名> : <宣言名>.Goal` を証明した（反例なし）。提出モジュールは `TinylambOpt/ResearchProofs/CloudBI/{Quotes,NestedBI,DeleteFiles,PublicBI}.lean`（共通補題は同 `Common.lean`）で、`lake build`（`sorryAx` 検査込み）成功。詳細は [RESULTS.md](RESULTS.md) の第九波の項。

各項目は `TinylambOpt.ResearchGoals.CloudBI.<モジュール名>.<宣言名>.Goal` で参照できる。公開入口の `TinylambOpt` から 4 モジュールを import する。各提出型に比較対象と入力条件を記し、モジュール末尾で具体的な境界例を `#guard` で確認した。[計測条件・図・SQL](../../../benchmark/comparison/reports/cloud-bi/README.md)

既存の証明では、結合ファンアウトの重み伝播、辞書語に対する遅延述語評価、MVCC の読み取り時点と統計の対応を扱っている。今回の提出型は、それらをどの枝・辞書・履歴に適用できるかを具体化する。たとえば `JoinedLeafWeights` は通常分岐と結合分岐を同時に数え、`EqualityHorizon` は delete file のスコープと sequence の境界を扱う。集計に使う行数・非 NULL 数・整数和は既存の `Totals` を再利用した。[結合木の重み伝播](../../TinylambOpt/ResearchProofs/FactorizedJoinTreeCountPropagation.lean)・[辞書述語](../../TinylambOpt/LazyDictionary.lean)・[スナップショット統計](../../TinylambOpt/SnapshotZoneMap.lean)・[共有集計状態](../../TinylambOpt/ResearchGoals/AnalyticsWorkloads/Common.lean)

## Quotes — CostBench の取り込み・維持・読者、10 件

ローカルでは書き込み・集計公開・並行する明細／ダッシュボードを計時した。公開 run14 では維持費と読み取り時間が同時にスコアを左右する。モデルの価格は符号付き整数 tick、データは確定した追記行。OHLC や再クラスタリングは、この負荷から派生する実装候補である。[提出型](../../TinylambOpt/ResearchGoals/CloudBI/Quotes.lean)

| 宣言名 | 比較する結果、削減したい処理 |
| --- | --- |
| StaleRollup | 確定時点までの集計に未反映分を足して任意の時刻範囲を返す。遅着の取引を commit 順で拾う。 |
| SparseRefresh | 完了バッチが飛び飛びでも、公開済みタイルの重複除去と穴の直接走査で全件集計を復元する。 |
| CloseTournament | 走査順が変わっても各 run の最終気配から終値を決める。同時刻は取引所の連番で決着する。 |
| CandleFringe | 窓に完全に含まれる固定幅タイルの状態と、端の半端な時間帯の行だけを足す。 |
| SpreadMoments | bid と ask の両方が有効な行をマスクし、差の列を作らず件数・spread の総和を求める。 |
| FenceRead | 銘柄の上下限から開く run を減らし、疎な銘柄範囲には正確な残余述語を適用する。 |
| ReclusterSwap | 読者の世代で新旧 run の一方を選び、再配置による二重計上と行欠落を防ぐ。 |
| RetryRefresh | 同じ不変バッチの再配送を一度だけ集計する。異なるバッチの中身が一致しても両方を数える。 |
| ReclusterBudget | 同じ再構築費の独立 run では、対象クエリの頻度込みで読み取り行の削減数を評価し、上位 k 本を選ぶ。 |
| MaintenanceBreakEven | 維持費の増分がある配置で、累積クエリ時間をどれだけ削減すれば full-path score が下がるかを判定する。 |

## NestedBI — Snowset/PBench 派生の巨大 SQL、10 件

256 分岐をキー別の重みに畳んだ SQL は同じ 7,332 を返した。一般化には、同じスナップショットと束縛を持つ決定的な枝であること、UNION ALL の多重度、空入力と全 NULL の集約意味論が必要になる。加算・乗算は拡張精度の正確な整数。固定幅のオーバーフロー、浮動小数点の丸め、volatile 式への適用には別途契約が要る。[提出型](../../TinylambOpt/ResearchGoals/CloudBI/NestedBI.lean)

| 宣言名 | 比較する結果、削減したい処理 |
| --- | --- |
| BranchHistogram | ネストした UNION ALL を枝 ID のヒストグラムにし、各部分状態を一度求めて出現回数を掛ける。 |
| JoinedLeafWeights | 通常枝の件数と、結合枝の右側行数・重み和を分け、重複次元や NULL のある袋でも集計する。 |
| FlattenSum | スカラー SUM が空入力で出す NULL 一行を考慮して、複数段の SUM の最終値を保存する。 |
| ProjectionChain | 任意段の射影を一つの列位置表へ合成する。重複列、並べ替え、NULL 拡張を扱う。 |
| RangeMultiplicity | 重なる多数の半開区間枝を始点・終点のソート済み列に置き換え、各値の寄与回数を返す。 |
| MemoMisses | 空結果を含む純粋な枝の部分集計をキャッシュし、計算回数が異なる枝 ID 数になることを問う。 |
| SpoolBudget | 独立な枝の構築・保存・再読費を比較し、最初の消費者が直接読める場合の最小コストを選ぶ。 |
| GroupSignature | ネストした GROUP BY の重複した列位置を落としても、NULL を含むキーの同値類を保つ。 |
| CanonicalFilters | 純粋な三値の連言を正準化し、同じフィルタを共有する。各消費者の行の多重度を残す。 |
| SharedProjection | 合成後の列位置が同じ消費者を一つに畳み、異なる射影から同じ値が出る場合も出現数を保つ。 |

## DeleteFiles — Iceberg Merge-on-Read、11 件

position delete は同じデータ sequence にも適用し、equality delete は厳密に古いデータに適用する。ファイルと物理行番号は安定した入力とし、partition の表現に spec ID を含めた。複数スナップショットのファイル選択はスキャンの前提で、各行に与える delete リストは読み取りスナップショットに属する。[提出型](../../TinylambOpt/ResearchGoals/CloudBI/DeleteFiles.lean)

| 宣言名 | 比較する結果、削減したい処理 |
| --- | --- |
| EqualityHorizon | (scope, key) ごとに最新の equality-delete sequence を一つ保存し、global と local の最大を引く。 |
| PositionMask | ファイル・partition・sequence を確定すると、そのファイルの位置集合を全行で再利用できる。 |
| OverlapCount | 両種の削除マスクの重なりを一度だけ引き、フッター行数から正しい生存数を求める。 |
| FooterCount | 範囲外 ordinal と重複を除いた位置集合だけで、position delete 後の COUNT を返す。 |
| ManifestHorizon | 保守的な partition・sequence 上界から不要な equality-delete マニフェストを開かずに済ませる。 |
| NullAwarePruning | 数値の上下限と NULL の可能性を組み合わせて equality-delete キーを絞る。 |
| FieldIdProbe | スキーマ進化で列位置が変化した場合、削除キーの field ID をファイルごとの物理位置に一括変換する。欠落 optional 列の既定値は NULL。 |
| CompactOrdinal | 削除を適用したファイル内で、生存行の新しい物理位置を旧位置の前の生存数から求める。 |
| DeleteAwareLimit | 削除数の上界を使って順序付き読み取りを打ち切り、LIMIT に入る生存行を保存する。 |
| SkippedOrdinalBase | 行グループを読み飛ばしてもその全行数だけ位置を進め、後の position delete を正しい ordinal に当てる。 |
| BitmapEpoch | 適用可能な削除 sequence の境界を越えない範囲で、equality-delete キー判定を再利用する。 |

## PublicBI — MLB_11 の軸列挙・属性・文字列計算、11 件

Q1・Q2・Q56 は軸ラベルの列挙、Q108 は定数で制約された一群の属性集約を使う。追加の接尾辞 CASE は「分類後の GROUP BY」と「細かい GROUP BY 後の分類」の両方を実測した。文字列は同じ比較・照合規則で扱い、丸めを伴う式には固定小数点の命題を直接適用しない。[提出型](../../TinylambOpt/ResearchGoals/CloudBI/PublicBI.lean)

| 宣言名 | 比較する結果、削減したい処理 |
| --- | --- |
| ConstantGroup | 三つのキーが定数に固定された Q108 を一状態で実行する。該当なしと全測定値 NULL の一群を区別する。 |
| PartialExtrema | 通過行を持つブロックの部分 MIN/MAX を併合し、群の有無を列の NULL 状態から独立に保つ。 |
| OccupiedAxis | 各ブロックで出現した辞書コードだけをデコードし、別ブロックの同名ラベルを統合して軸を整列する。 |
| GroupBeforeSuffix | (league, position) で先に数えてから接尾辞 CASE を適用し、最終ラベルごとに件数を足す。 |
| TripleBitmap | 各リテラルに対応する局所コードをすべて引き、三次元の位置マスクを交差してから測定列を読む。 |
| AttributeWitness | 非 NULL 値の異なりが 0・1・複数のいずれかを、MIN/MAX の状態だけで判定する。 |
| LiftedDecimals | 固定小数点のスケールをブロック内の extrema 計算後に揃える。負値・全 NULL を扱う。 |
| AxisRetraction | 表示範囲を縮めたとき、最後の出現が除去された軸ラベルだけを落とす。 |
| ClassificationCache | 守備位置コードの接尾辞分類を要求時に一度行い、NULL を含むデコード後の行順と呼出し数を比較する。 |
| SliceBounds | 年とブロックで層化した周辺件数から三次元フィルタの上下界を求める。列間の独立性を仮定しない。 |
| CodeExtrema | 選択されたコードから全 NULL・単一値を検出し、値が混在する場合だけ占有辞書値の MAX/MIN を求める。 |
