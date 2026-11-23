# SQL 実行の高速化に向けた未証明命題（第七波）

12 分野、100 件の提出型を登録した。以下の各表の左列に `.Goal` を付けると、リンク先の Lean 名前空間内の宣言名になる。たとえば並列マージの提出型は `TinylambOpt.ResearchGoals.OrderedStreams.MergeDiagonal.Goal`。証明を提出するときは従来どおり別モジュールから import できる。各ファイル末尾の `#guard` は小さい入力でモデルの境界動作を検査する。

### 既存の証明をどこから広げたか

| 証明済みの結果 | 今回、新しく問うこと |
| --- | --- |
| 区間内の最終出現位置から [COUNT DISTINCT](../../../TinylambOpt/ResearchProofs/RangeDistinctIndex.lean) を返す。 | 順序統計量をビットごとの順位で引けるか。フレームの種類に応じ、重なりを許す索引と許さない索引を使い分けられるか。 |
| [外部結合の頻度表](../../../TinylambOpt/ResearchProofs/OuterJoinCounts.lean)で NULL と多重度を復元する。 | 更新前の頻度表と変更バッチだけで、最後の一致を失った親の NULL 補完行まで修正できるか。 |
| [ペア和の第 k 位](../../../TinylambOpt/ResearchProofs/PairRank.lean)を直積の実体化なしで求める。 | 距離総和、異なるユーザーの組数、三表の不等号結合をどの統計量で厳密に集計できるか。 |
| [ビットプレーン SUM](../../../TinylambOpt/ResearchProofs/BitPlaneSum.lean) が符号と NULL マスクを保存する。 | レーンを復号する前に大小比較、順位選択、積和まで評価できるか。 |
| [再帰 CTE の差分展開](../../../TinylambOpt/ResearchProofs/RecursiveDelta.lean)が到達集合を保存する。 | 辺の更新に対する閉包の保守、到達先からの枝刈り、UNION ALL の経路多重度にも使えるか。 |
| [関数従属キー削減の反例](../../../TinylambOpt/ResearchProofs/FunctionalDependentGroupKeyElim.lean)では、算術ガードの定義域と参照評価器のずれが観測できる。 | 式から索引範囲を導く際、負数の丸め・端点・検査付き演算のエラーをどの条件で保存するか。 |

袋の比較には多重度、順序が観測される演算には列の等価性を使った。数値は型に明示された精度で解釈する。特に `Int` による集約の因子化を適用できるのは、途中結果まで正確に計算できる場合。以下の削減対象は実装時の狙いで、実測した性能値ではない。

## ペアと多表の集約（8 件）

[PairAggregates の提出型](../../../TinylambOpt/ResearchGoals/ExecutionStrategies/PairAggregates.lean)。NULL を除いた入力と `COUNT` または `COALESCE(SUM(...),0)` を扱う。

| 提出型 | 省ける処理・適用条件 |
| --- | --- |
| AbsoluteDistance | 自己結合の絶対差総和を、ソート後の件数・累積和で求める。同値の別行の寄与は 0。 |
| SquaredDistance | 二表間の二乗距離総和を各表の件数・一次・二次モーメントから復元する。 |
| ResidualInequality | テナント一致かつユーザー不一致の結合件数を、テナント頻度から複合キー頻度を引いて求める。 |
| SameGroupPairs | 同一グループ内の異値行ペア数を、グループ別と値別の組合せ数の差で求める。 |
| ComplementProbe | 和が定数になる結合は、対向表の補数キーを一度引けば重複分まで数えられる。 |
| IncreasingTriples | 三表の厳密な昇順鎖を中央キーの頻度と両側の順位件数に分解する。 |
| XorDistance | 全ペアのバイト XOR 総和を、各ビットの 0・1 の個数から求める。 |
| LayerCakeMinimum | 非負の小さい値域では、全ペアの最小値の和を閾値別の超過件数の積和に変える。 |

## ウィンドウと区間索引（9 件）

[FrameIndexes の提出型](../../../TinylambOpt/ResearchGoals/ExecutionStrategies/FrameIndexes.lean)。位置は一つのパーティション内の半開区間で表す。

| 提出型 | 省ける処理・適用条件 |
| --- | --- |
| WaveletQuantile | 値幅のビット数だけ wavelet tree を降り、フレームの再ソートなしで重複込みの第 k 位を引く。 |
| SparseMinimum | 非空フレームの MIN を、両端に接する二つの事前集計区間から求める。両区間は重なってよい。 |
| DisjointRangeFold | 順序依存の文字列連結では、端点の最上位相違ビットから重ならない suffix と prefix を選ぶ。 |
| PrefixNullableSum | 累積和と非 NULL 件数の差を併用し、範囲 SUM の NULL と総和 0 を区別する。 |
| ExcludePeerSelect | EXCLUDE GROUP 後の第 k 行を、除いた連続区間の幅で元の位置へ戻して引く。 |
| TileArithmetic | NTILE を行位置から直接計算する。行数よりバケット数が多い場合も対象。 |
| SlidingMinimum | 期限切れと後続行に支配される候補だけを除き、各 ROWS 窓の再走査を省く。 |
| SessionBoundary | 時刻差で区切るセッション数を、隣接するチャンクの境界時刻一組で補正する。 |
| PeerRankRuns | 同順位ランの長さから RANK と CUME_DIST の分子を全行へ配る。 |

## 結果の差分保守（8 件）

[DeltaMaintenance の提出型](../../../TinylambOpt/ResearchGoals/ExecutionStrategies/DeltaMaintenance.lean)。挿入と削除が同じバッチに含まれる場合も扱う。

| 提出型 | 省ける処理・適用条件 |
| --- | --- |
| JoinBatch | 両側同時更新の等値結合件数を、旧頻度と両差分の積から更新する。 |
| ChainBatch | 三表鎖結合の変更量を、新旧の頻度表を混ぜる三項に分解する。 |
| DistinctCrossing | DISTINCT の出力更新を、出現回数が 0 をまたぐキーに絞る。 |
| SemiWitness | EXISTS の対向表更新では、一致が初めて生じるか最後に消えるキーだけ旧左行に影響する。 |
| OuterPadding | LEFT JOIN の右側を更新したとき、一致数 0 と 1 の間の遷移と NULL キーを補正する。 |
| IntersectionDelete | INTERSECT ALL の片側の削除から、実際に減る出力多重度を求める。 |
| TopKRepair | 最大 d 行の削除に備えて古い上位 k+d 行を保持し、挿入を交えて新しい上位 k 行を復元する。 |
| MinimumRepair | 旧最小値の最後の一件が消えた場合だけ残存入力を再集約する。 |

## ビットスライス実行（8 件）

[PlaneKernels の提出型](../../../TinylambOpt/ResearchGoals/ExecutionStrategies/PlaneKernels.lean)。符号なし 8 ビット値を 64 レーンで処理する。比較対象の辞書には共通のコード順序が必要。

| 提出型 | 省ける処理・適用条件 |
| --- | --- |
| ParallelLess | 二つの列の大小関係を、上位ビットで下位の判断を上書きするマスクとして構築する。 |
| PlaneAdd | 行をまたがずに桁上がりを伝え、各行の和とオーバーフロービットを同時に得る。 |
| ArgMinMask | 上位ビットから候補を消し、選択行の最小値と同率の全レーンを残す。 |
| RadixSelect | 各ビットで 0 側の母数を数えて分岐し、第 k 位をソートなしで引く。 |
| PlaneDot | 二列の積和をプレーン同士の共通ビット数に分解する。 |
| MedianOfThree | 三列の中央値を比較マスクから選ぶ。同値の入力を含む。 |
| OccupiedCodes | 空のビット接頭辞を枝刈りし、使用コード数を復号なしで数える。 |
| RunBoundary | 前ランと異なる最初の選択レーンを不一致マスクと末尾ゼロ数で得る。 |

## バージョン付き索引（8 件）

[VersionedIndexes の提出型](../../../TinylambOpt/ResearchGoals/ExecutionStrategies/VersionedIndexes.lean)。確定済みバージョンを扱い、照会時刻・コミット順序・削除マーカーを区別する。

| 提出型 | 省ける処理・適用条件 |
| --- | --- |
| SnapshotAnchor | 最古の pin より古い版は、キーごとに可視性の境界となる一版だけを残して圧縮できる。 |
| SparsePins | 離れた複数の pin に可視な版と最新 pin より新しい版を保持し、中間世代を飛ばす。 |
| RangeTombstone | 半開区間の削除を点データとは別に照会し、新旧を比較する。同一時刻は削除を優先する。 |
| CoveredPredicate | 索引で値述語を先に評価するとき、新しい非該当行や削除による遮蔽を確認する。 |
| BatchedLookup | 同一スナップショットの重複要求を一回のキー検索にまとめ、欠落も含めて要求順に配る。 |
| DominatedRun | 既知の可視版より全エントリが古いランを丸ごと飛ばす。 |
| BottomTombstone | 下層に以前の版が残らない圧縮では、古い削除境界も消して照会結果を保つ。 |
| OverlayAggregate | 新旧ランを先にマージせず、新ランに可視版がないキーだけ旧ランから加算する。 |

## バイナリ文字列と辞書（8 件）

[BinaryText の提出型](../../../TinylambOpt/ResearchGoals/ExecutionStrategies/BinaryText.lean)。バイト順照合が対象。空文字、埋め込み NUL、0xff を許す。

| 提出型 | 省ける処理・適用条件 |
| --- | --- |
| PrefixFence | 接頭辞検索を半開索引区間に変換する。末尾 0xff の桁上がりと上端のない場合を含む。 |
| SharedPrefixCompare | 前の辞書語との比較で得た最長共通接頭辞の長さを、次の語とプローブの比較に再利用する。 |
| AnchorPositions | リテラル中の選んだ一バイトの位置リストから候補開始位置を逆算し、完全一致だけを残す。 |
| DeleteSignature | 一バイト以内の挿入・削除・置換候補には共有する削除署名がある。偽陽性は残差照合で扱う。 |
| PrefixCardinality | 辞書全体の異なる非空接頭辞数を、隣接語との共通接頭辞の長さから求める。 |
| StripSort | 共有するページ接頭辞を外して suffix だけをソートし、重複を含む元の順序を復元する。 |
| AcrossChunks | 部分文字列の出現数を各チャンクの集計と短い境界領域から合成する。重なった一致も数える。 |
| TupleEncoding | ゼロをエスケープした複合キーのバイト比較が二列の辞書式順序を保存する。 |

## 計画探索と実行時の上界（8 件）

[SearchBounds の提出型](../../../TinylambOpt/ResearchGoals/ExecutionStrategies/SearchBounds.lean)。枝刈りに使う上界と、再確認が必要な候補集合を個別に定義した。

| 提出型 | 省ける処理・適用条件 |
| --- | --- |
| CollisionBound | キー別頻度の二次モーメントだけで、等値結合件数の上界を求める。 |
| SortedDegreeBound | 両側の頻度を大きい順に組み合わせ、キー対応が未知でもファンアウトを上から抑える。 |
| TriangleBound | 重複排除済みの三角結合について、三辺の件数から結果数を上から抑える。 |
| FrequentCandidates | Misra–Gries の有界カウンタで、再集計が必要な HAVING 候補を漏れなく残す条件を与える。 |
| CappedJoin | LIMIT の残枠をキー頻度で減らし、必要な結合件数に達したらプローブを打ち切る。 |
| GroupSumFence | 残存ブロックの正の寄与だけを上界に使い、符号付き SUM でも閾値未達を確定する。 |
| UnseenTopK | シャードごとの候補外キーの上界を足し、グローバルな同率候補の取りこぼしを防ぐ。 |
| PredicateExchange | 二述語の短絡順序を、結果の独立性を仮定せず、評価単価と棄却件数から比較する。 |

## 相関サブクエリ（8 件）

[CorrelatedProbes の提出型](../../../TinylambOpt/ResearchGoals/ExecutionStrategies/CorrelatedProbes.lean)。空の右入力、同じ値を持つ複数の行、NULL の証人を区別する。

| 提出型 | 省ける処理・適用条件 |
| --- | --- |
| Division | 二重 NOT EXISTS の「全必須タグあり」を、重複排除した一致タグ数と必須数で判定する。 |
| OffsetScalar | OFFSET d のある相関スカラ照会では、各キーの最初の d+2 行だけで値と複数行エラーが決まる。 |
| ExcludingMinimum | 自分の行だけを除く MIN を、グループ内の値と出現位置の上位二件から引く。 |
| CountDistinctStop | 相関 COUNT(DISTINCT) が閾値以上かの判定を、必要な非 NULL 証人を得た時点で止める。 |
| NeWitnesses | 「<> ANY」の三値結果を、異なる値二個までと NULL の有無で復元する。 |
| OrExists | 二つの EXISTS の選言を排他的な検索に分け、外側の行番号で重複行・順序を復元する。 |
| OptionalSiblings | 同じ親に独立に付く二つの外部結合の FILTER 付き件数を、NULL 補完後の片側件数の積にする。 |
| PrefixAntichain | 複数の接頭辞のいずれかを探す相関検索で、短い接頭辞に包含される候補を除く。 |

## 順序付きストリーム（8 件）

[OrderedStreams の提出型](../../../TinylambOpt/ResearchGoals/ExecutionStrategies/OrderedStreams.lean)。同じキーでも異なるペイロードを持てるので、安定順序の破壊を観測できる。

| 提出型 | 省ける処理・適用条件 |
| --- | --- |
| MergeDiagonal | 並列マージの任意出力位置に対し、左優先の同率規則を守る二入力上の切断点が一意に決まる。 |
| MonotoneProbes | 昇順プローブの検索カーソルを巻き戻さず、同じキーの再要求には同じ最初の行を返す。 |
| WeightedOffset | 結合ランの直積をサイズで飛ばし、目的位置の行ペアを商と余りで取り出す。 |
| ReversePeerRuns | 降順入力から安定昇順へ変換するとき、同順位ラン単位で順序を反転できる。 |
| RangeCoalescing | 近い索引範囲をまとめて seek 回数を減らす。隙間は残差で落とし、各行を一回だけ返す。 |
| RankSlice | 全順序が一意なら、整列ランごとの挿入順位の和で出力ページに入る行を特定できる。 |
| SpliceBlock | 左ブロック末尾が右先頭以下なら、キーを逐次比較せず左ブロックごと出力できる。 |
| RunIntersection | 整列したラン同士を二カーソルで進み、INTERSECT ALL の多重度を展開前に求める。 |

## 圧縮ブロックと範囲更新（8 件）

[EncodedBlocks の提出型](../../../TinylambOpt/ResearchGoals/ExecutionStrategies/EncodedBlocks.lean)。可視行を選んだ後の列、またはその選択マスクを入力とする。

| 提出型 | 省ける処理・適用条件 |
| --- | --- |
| PatchedRange | 基準値との差分を格納した列で、例外レーンだけ実値を比較する。例外は基底コードを上書きする。 |
| SparseGather | 妥当性ビットの prefix rank で、NULL を詰めた列のペイロード位置を求める。 |
| OverlapDot | 区切りの違う二本の RLE 列の積和を、重なるラン区間の長さで重み付けする。 |
| StrideSlice | 等差数列ランの任意区間の和を、最初の項・間隔・有効長から求める。 |
| RangeEdits | 範囲加算のバッチを端点の差分に変換し、一回の累積走査で反映する。 |
| DistinctSeams | 整列ページ列の DISTINCT 件数を局所件数から合成する。空ページを挟んで続く同一値も補正する。 |
| SelectDirectory | 選択ビットの累積個数を二分探索し、詰めた行の順位から元の行位置へ戻す。 |
| SecondDifferences | 二階差分ブロックの総和と位置モーメントで、全値を復号せず終了時の値と一階差分を求める。 |

## 式から索引範囲を求める（9 件）

[IndexPreimages の提出型](../../../TinylambOpt/ResearchGoals/ExecutionStrategies/IndexPreimages.lean)。境界を広い整数型で計算し、SQL 式の丸め方式を個別に指定する。

| 提出型 | 省ける処理・適用条件 |
| --- | --- |
| TruncBucket | ゼロ方向の整数除算の等値条件を一つの閉区間にする。商 0 の幅に注意する。 |
| RoundedBucket | 最近接偶数丸めの等値条件を索引範囲に変換し、両端の採否を出力の偶奇で決める。 |
| ModularLookup | 合同式の係数に逆元があるとき、密な有界キー域の点検索一回に変換する。 |
| DescendingBand | 負の傾きを持つ線形式の半開帯域を、元の整数索引上の閉区間に戻す。 |
| CheckedDistanceBand | ページ範囲から減算と ABS の安全性を確定してから、距離条件を索引の範囲条件にする。 |
| CursorRanges | NULLS LAST 昇順と第二キー降順のカーソル以降を二つの連続範囲として重複なく読む。 |
| DistanceRing | 絶対距離の下限・上限を二つの排他的な seek にし、半径 0 でも中心を二重に読まない。 |
| MortonSquare | ビット交互配置の二次元索引では、整列した正方形が一つの連続キー区間を占める。 |
| PeriodicSeek | 周期条件を満たす最初の位置を求め、負の日番号でも該当日だけを列挙する。 |

## 再帰 CTE とグラフ結合（10 件）

[RecursiveJoins の提出型](../../../TinylambOpt/ResearchGoals/ExecutionStrategies/RecursiveJoins.lean)。到達可能性には長さ 0 の経路を含む。経路数を観測する提出型では、同じ端点を持つ辺も出現回数だけ寄与する。

| 提出型 | 省ける処理・適用条件 |
| --- | --- |
| InsertClosure | 一辺の挿入後の到達集合を、旧閉包での始点側・終点側の到達可能性から更新する。 |
| QuotientClosure | 同じラベル内で相互到達可能なら、縮約後のグラフで再帰 EXISTS を評価できる。 |
| MeetFrontiers | 固定長の経路の存在を、始点からの前方探索と終点からの逆向き探索の交差で判定する。 |
| BinaryLifting | 後続行が高々一つの外部キー鎖を、事前計算した二冪歩のジャンプで辿る。 |
| PathMultiplicity | 深さ制限のある UNION ALL で、全経路の列挙を各段の頂点別件数の伝播に変換する。 |
| SuppressConnector | 問い合わせの両端以外で入出次数がともに 1 の頂点を除去し、二辺を直結する。 |
| TargetCone | 目的地へ到達できない辺を逆引きで除き、固定長の経路件数を保存する。 |
| RedundantEdges | 残す辺だけで代替経路が存在する辺は、複数同時に取り除いても到達性を保存する。 |
| ButterflyCount | 二部グラフの四角形を、左側の頂点ペアの共有隣接点数から数える。 |
| TriangleOrientation | 無向三角形に正準な向きを与えて一度ずつ処理し、六通りの自己結合の件数を復元する。 |
