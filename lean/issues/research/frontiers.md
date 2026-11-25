# DuckDB との性能差から考えた高速化仮説 110 件

結合の中間表、行ごとの文字列照合、削除後の再計算、プラン候補の全探索をどこまで省けるかを調べる。11 分野で各 10 件を `def …Goal : Prop` として登録した。うち 12 件は前提を意図的に弱め、具体的な入力で破れるところまで確認した。各モジュール末尾の `#guard` は有限例を計算する。命題の証明および反証定理は今後の課題として残す。

起点は H2O H5（SF1、tinylamb 162.6 ms / DuckDB 7.7 ms）、LDBC 派生 L1（同 81.9 / 16.0 ms）、TPC-DS Q22（SF10、941.73 / 144.45 ms）、Public BI の suffix CASE（44.376 / 2.413 ms）などの保存済み測定。Snowset/PBench 派生の巨大な式や CostBench の refresh にも同じ発想を広げた。各節にどの処理からの着想か、次に何を測ればその仮説を選別できるかを示す。[測定条件・クエリ別結果](duckdb-gaps.md)・[派生ワークロードの SQL と入力件数](analytics-workloads/README.md)・[Cloud/BI 比較](../../../benchmark/comparison/reports/cloud-bi/README.md)

## H2O H5 で実際に縮んだもの

H5 は `x.id4 = y.id4` で結合し、`x.id1` ごとに `SUM(x.v1)` と `SUM(y.v2)` を返す。SF1・seed 1729 の生成データで、左の群 × キーの度数を 400 × 1,000 の整数行列にした。各度数の下位 1 ビットでガウス消去したところ、独立な行は **400 本**あった。整数のランク分解を 2 で剰余化しても積を保つので、この行列に幅 399 以下の厳密な整数因子を使う案は、この入力では棄却できる。これは `RankRejection` の使いどころであり、同命題の反例ではない。[H5 の提出型](../../TinylambOpt/ResearchGoals/Frontiers/WorkloadBases.lean)

一方、出力する二つの SUM はそれぞれ右側の異なる情報しか使わない。右キー別に `rc = COUNT(*)`、`rs = SUM(y.v2)` を作り、同じ重みを持つキーをまとめた。

| 左の集約単位 | 右の異なる重み | 左の実在する組 | 計算するもの |
| --- | ---: | ---: | --- |
| (id1, id4) | 1,000 キー | 127,405 | 基準となる両 SUM の部分状態 |
| (id1, rc) | 62 | **20,703** | 左の v1 の部分和に rc を掛けて sx を得る |
| (id1, rs) | 977 | 126,252 | 左の行数に rs を掛けて sy を得る |
| (id1, (rc, rs)) | 1,000 | 127,405 | 二つの重みの組を一緒に辞書化 |

元の H5 と二つの集約を分けた SQL を DuckDB Python 1.5.5 で実行し、群 ID で整列した **400 行の両 SUM がすべて一致**した。全群の sx の合計は 11,991,046,910、sy は 12,011,554,875。20,703 は sx 側の積をまとめられる粒度で、元表の読取り、sy の計算、再集約の費用も残る。二案の実行時間は今回未計測。

再現には空の出力ディレクトリを指定して以下を実行する。H2O の manifest の SHA-256 は `2608e983a5ccf961b590b01f536495afc3a05a05bed599d5c4d6542c2dd858dd`。生成される `dataset/h2o/{x,y}.csv` を同名の表として読む。[データ生成・元 SQL](../../../benchmark/comparison/analytics_bench.py)

```bash
python3 benchmark/comparison/analytics_bench.py --generate-only --families h2o \
  --scale 1 --seed 1729 --output /tmp/h5-basis
```

```sql
WITH l AS (SELECT id1, id4, COUNT(*) AS lc, SUM(v1) AS ls FROM x GROUP BY id1, id4),
     r AS (SELECT id4, COUNT(*) AS rc, SUM(v2) AS rs FROM y GROUP BY id4),
     joined AS (SELECT * FROM l JOIN r USING (id4)),
     xw AS (SELECT id1, rc, SUM(ls) AS s FROM joined GROUP BY id1, rc),
     yw AS (SELECT id1, rs, SUM(lc) AS n FROM joined GROUP BY id1, rs),
     a AS (SELECT id1, SUM(rc*s) AS sx FROM xw GROUP BY id1),
     b AS (SELECT id1, SUM(rs*n) AS sy FROM yw GROUP BY id1)
SELECT id1, sx, sy FROM a JOIN b USING (id1) ORDER BY id1
```

## 具体的な反例がある 12 件

| 宣言名 | 前提を満たしても何が破れるか |
| --- | --- |
| QuotientCycles | 無向の三角形 2 個と 6 頂点の環は全頂点の次数が 2。一つのクラスにまとめると区別できず、始点に戻る長さ 3 の経路は 12 対 0。 |
| FanoutOnly | 右側 2 キーの件数がともに 1 でも、右の値が 5 と 8 なら和は 13。前者を代表にすると 10。 |
| ThreeFromTwo | 削除位置 [0,4,5] と [1,2,6] は件数 3、和 9、平方和 41 が一致する。位置 4 を消すのは前者だけ。 |
| MeanHoleBudget | 両軸を 0～8 とし、両端がともに 2 未満の 4 組だけを除く。平均欠落数は 1 以下でも、各軸の先頭 2 要素からは最小の有効スコア 2 を得られない。 |
| TwoWinners | 費用 10n、10+2n、30 の三案。n=0 と 10 の勝者だけを残すと、n=3 で最安の 16 を失い 30 を払う。 |
| ComplementaryViews | 二つのビューが両方そろって初めて 1 回の再計算を省ける場合、二枚目の限界便益は 0 から 1 に増える。 |
| Unseparated | 一文字の二行 [1] と [2] を連結すると、どちらにも含まれない [1,2] が見つかる。 |
| PairStats | 三つの二値属性で偶数パリティの 4 行と奇数パリティの 4 行は、全二属性周辺表が一致する。000 の件数は 1 対 0。 |
| LocalRounding | 二つの 1/2 を各々偶数丸めすると 0+0。合計してから丸めると 1。 |
| NegatedConsumer | FALSE と NULL は WHERE で同じ採否だが、NOT 後に採用されるのは FALSE だった行だけ。 |
| PackingExact | 証人 {0,1}, {1,2}, {0,2} は 2 本を互いに素に選べないが、全部を潰すには 2 行の削除が要る。 |
| AdjacentRuns | run 長 [1,10,1] の最小書込量は、両端を先に混ぜれば 14、隣接ペアだけなら 23。 |

## 分野別の提出型

各宣言の完全名は `TinylambOpt.ResearchGoals.Frontiers.<モジュール>.<宣言名>.Goal`。整数・頻度の要約は任意精度で扱う。SQL への適用時に必要な非 NULL 件数、空群、固定幅算術、照合順、同点、スナップショットは各モデルの観測に合わせて扱う。物理コストを述べる命題は、ページ数・書込レコード数・生存バッファなど比較する単位をコードで定めている。

### 結合の行列分解とグラフの縮約（Contractions）

H2O H5、STATS-CEB C3、LDBC L1 の橋表が起点。部分行列のランク、ゼロでない例外数、クラス別行和の一致を測る。三角結合は L1 からの発展で、閉路の開始頂点と方向を区別して数える。[提出型](../../TinylambOpt/ResearchGoals/Frontiers/Contractions.lean)

- `LatentJoin`　低ランクの橋表を二組の周辺集計に変換して、結合キー対の走査を省く。
- `PivotCertificate`　一つの非零ピボットに対する小行列式の消滅をロード時に検査し、後続の任意の集約を一行一列から復元する。
- `ResidualHaving`　上限付き誤差を許すランク一近似から、厳密な HAVING の不採用を保証する。
- `TriangleFactors`　三角結合で三辺すべてが因子化できるとき、頂点三重ループを三つの内積に落とす。
- `RankUpdate`　橋表のランク一更新に対して三角集計を再評価する量を、変更された二端点間の経路数に限定する。
- `InvisibleDelta`　基底方向に直交する売上差分は全出力群から見えないため、群ごとの更新を全廃できる。
- `QuotientWalk`　同じクラスの頂点が各行先クラスへ同じ総重みを送るなら、任意段のグラフ展開を商グラフで行う。
- `QuotientCycles`　強仮説。クラス別の行和が一致する二つのグラフでは、始点に戻る長さ 3 の経路数も同じとしてよいか。
- `ExceptionJoin`　係数の疎な例外だけを別経路で課金する。例外座標自体が重複しても差分を重ねる。
- `LatentBridge`　二つのランク分解を内部キーで先に縮約し、中間結合を潜在次元だけの行列で共有する。

### H5 の二つの SUM が本当に必要とする情報（WorkloadBases）

H5 の度数行列と重みの分布を上記の fixture で調べた。キー全体の圧縮、単一の集約の圧縮、上位群だけでの第二集約を比較する。LateSecondMeasure は LIMIT がある派生クエリ向け。[提出型](../../TinylambOpt/ResearchGoals/Frontiers/WorkloadBases.lean)

- `RankRejection`　偶奇だけのガウス消去で整数因子の必要ランクを下から押さえ、見込みのない厳密圧縮を構築前に棄却する。
- `QueryBasis`　同じクエリ系列の重みが少数のベクトルの線形結合なら、橋表自体がフルランクでも要約の内積だけで全回答を復元する。
- `Indistinguishable`　二行の要約が識別不能となる条件を、基底ごとの等値で特徴付け、全てのクエリ係数を列挙せず共有する。
- `SeparateMeasures`　H5 の二つの SUM に別々の同値類を使い、右の件数と右の売上の組が一意でも片方の状態を強く圧縮する。
- `FanoutOnly`　強仮説。ファンアウトが同じキーを代表一つにまとめた要約で、右の値に由来する二つ目の SUM も返せるか。
- `NarrowWeights`　右側の重みを下限からの少数ビット面として格納し、各面のマスク付き和だけから正確な群内積を復元する。
- `EnergyBound`　外れ値を含む重みを代表値と残差に分け、二乗ノルムの積で任意のグループの誤差の二乗を保証する。
- `MedianCode`　圧縮誤差の絶対値総和を最小にする辞書代表を、グループごとの再探索なしにバケットの中央値で決定する。
- `LateSecondMeasure`　第一集約が順位を決めるとき、厳密な境界余裕がある勝者だけ第二集約を開く。勝者内部の順序は厳密値で復元する。
- `DirtyWeights`　収集済みの群別内積に対し右表が部分的に更新されても、差分重みがゼロのキーを丸ごと落として修復する。

### 少数の更新・削除を要約から復元する（SparseRecovery）

Iceberg 相当の位置削除と CostBench の訂正を想定。ファイルと版を固定して ordinal を比較する。削除密度、符号化に必要なビット数、積やべき和の更新費まで測る。[提出型](../../TinylambOpt/ResearchGoals/Frontiers/SparseRecovery.lean)

- `BoundedMoments`　最大 d 個の削除 ID の袋を d 次までのべき和で一意に復元できるか。0 次は件数を担う。
- `TwoRootMask`　削除が二行以内のファイルでは候補位置を二次式で判定し、位置ビットマップを省く。
- `SwapDecoder`　件数と一次・二次モーメントから、追加一行と削除一行の位置をログ展開なしで復元する。
- `MonicFingerprint`　同じ件数のファイル内容を、最高次が既知の特性多項式の d 点評価で正確に比較する。
- `RationalDelta`　入出力が巨大でも変更量が疎なら、多項式比の交差積から削除・挿入の正味の差を復元する。
- `UniformBucket`　平方和が分散ゼロを示すバケットなら、全重複を一つの ID と件数から再生する。
- `ThreeFromTwo`　強仮説。三つの削除でも件数・和・平方和だけで正しい位置マスクを共有できるか。
- `IsolatedHash`　大きな ordinal 空間を複数の小さい辞書へ射影し、各候補の混同行数の最小値を削除数の上界にする。
- `PeelingVariance`　バケット単位で既知の削除を剥がすと、新たに純粋になったバケットの decoded ID も正しくなる。
- `DisjointResultant`　小さな二表の積結合でキー一致が存在するかを、片方の特性多項式の他方全キーでの積で判定する。

### 結合結果を展開する前の順位確定（RankedJoins）

TPC-H Q3、ClickBench Q37/Q38、TPC-DS Q22 の上位結果が起点。加算で合成できるスコアや重み付き順位に適用する。切り落とせる候補数と、境界同点・残余述語の偏りを調べる。[提出型](../../TinylambOpt/ResearchGoals/Frontiers/RankedJoins.lean)

- `BoundedProduct`　順序保存的な積の上位 K は両入力の上位 K だけで計算し、巨大な直積を切り詰める。
- `HolesPerAxis`　各行・各列の残余述語による欠落が最大 h 個なら、両入力 K+h 行から正しい上位を得る。
- `MeanHoleBudget`　強仮説。欠落数の平均が h 以下という統計でも、各軸 K+h の境界を使えるか。
- `RankByCut`　閾値以下の組数をソート済み片側の prefix 長から求め、第 K スコアを列挙なしで判定する。
- `WeightedSeek`　重複キーを展開せず、重み付き順位が指す一点を返す。重みゼロのキーも扱う。
- `DistinctOverfetch`　DISTINCT の前で上位を切るため、物理スコアの最大重複度を証明書に使う。
- `BoxCertificate`　既知の勝者より確実に悪い区画を外し、同点を出す区画は開く。証明書には区画の最小スコアのみ必要。
- `TieMass`　K 番目以降に同点が続く WITH TIES を重み付き境界の二項（厳密未満と同値）から復元する。
- `NearPairs`　隣接時刻対の距離が d 以下の組数を、ソート後の左右対称な帯の長さから計算する。
- `IntervalWinners`　誤差区間の上端が落選側の全下端より小さければ、上位集合だけは近似値で確定できる。

### 基数の誤差を許すプラン選択とビュー選択（PlanMarkets）

JOB・CEB の不確かな結合基数と、PBench 派生で再利用される部分式が起点。アフィン費用が成立する基数区間や、複数ビューを同時に要求する消費者の比率を調べる。[提出型](../../TinylambOpt/ResearchGoals/Frontiers/PlanMarkets.lean)

- `EndpointEnvelope`　基数区間の両端で優越する代替案があれば、区間全体のメモから劣る線形プランを消せる。
- `RobustRegret`　各基数について最良の競合が異なる場合でも、固定プランの最悪絶対 regret を両端で評価できる。
- `TwoWinners`　強仮説。メモで両端の勝者だけを残しても、中間基数で最安のプランを失わないか。
- `SlopeSwitch`　基数が増えると最良線形プランの単位費用は単調に下がる。パラメータ順に一方向へ切替可能か。
- `OnlineSpool`　再利用回数が未知のスプールを、累積再計算費が構築費を超える境界で一度だけ構築する。
- `DiminishingReuse`　複数ビューのどれか一つで要求を満たせるとき、既選択集合が増えると追加ビューの限界便益は減る。
- `GreedyViews`　ビューを K 個選ぶ貪欲法の便益を最良 K 個と整数だけで比較する。K=1 では厳密な最適性になる。
- `ComplementaryViews`　強仮説。二つの部分ビューを両方要求する場合にも、限界便益の逓減を再利用できるか。
- `ApproxMemo`　メモを幅方向のバケットに縮約する。起動費・行単価が各々 ε 以内なら、全基数での劣化を制限できる。
- `BoxWorst`　多次元パラメータの独立な区間で、線形コスト差を最悪にする角だけを一回評価する。

### 一つの索引から文字列の一致位置と行集計を得る（TextIndex）

ClickBench Q21–Q24 の文字列フィルタ、Public BI の分類が起点。バイナリ照合のリテラルを扱い、空語と行末を明示する。索引構築費、各区間の出現数と異なる行数を別々に測る。[提出型](../../TinylambOpt/ResearchGoals/Frontiers/TextIndex.lean)

- `Contiguous`　一致する suffix を区間の始点・件数だけで表し、LIKE の一致位置を後から列挙する。
- `BackwardSearch`　FM 索引でパターンの左端を一文字伸ばす。二回の rank と累積頻度から新しい半開区間を得る。
- `LocateExtension`　一致区間内の前置文字だけをプローブし、再ソートせず左拡張の全住所を得る。
- `SampledLocate`　stride の倍数だけ住所を保存した suffix 配列から、LF を高々 stride−1 回辿って元位置を復元する。
- `DocumentSum`　同一文書内の複数一致をまとめ、元の行を読まず一致文書ごとの売上を一度だけ合計する。
- `DistinctMarks`　任意の suffix 区間で直前の同一文書が左端より手前の印だけを数え、行 DISTINCT を返す。
- `Unseparated`　強仮説。複数行を区切りなしに連結して一つの suffix 索引を作り、EXISTS を任意の非空語で共有できるか。
- `SeparatedCount`　各行末の予約記号を一つだけ使い、全行を共有する索引から非空リテラルの総出現数を得る。
- `PositionalIntersect`　部分語ごとの出現位置をオフセットで突き合わせ、連続フレーズの一致を元文字列の再照合なしで得る。
- `PhraseBound`　二つの部分語を別々の辞書で照合し、一方の出現頻度だけでもフレーズ頻度の打切り上限を得る。

### 小さい二値属性集合の変換と差分更新（SetTransforms）

TPC-DS Q22 の ROLLUP や複数チャネルの集約から、低次元のビット属性に広げる。領域が 2 の d 乗に増えるので、実在セル数、繰返し照会数、必要な係数数が選択基準になる。[提出型](../../TinylambOpt/ResearchGoals/Frontiers/SetTransforms.lean)

- `OrJoin`　ビット属性の OR で結合した度数表を、部分集合累積表の点ごとの積として構築する。
- `RecoverCell`　部分集合別のロールアップだけを保存し、要求された完全一致セルを Möbius 反転で復元する。
- `XorJoin`　フラグの対称差で分類する結合を Walsh 変換し、直積の代わりに二つの係数列を乗算する。
- `SpectralDot`　符号付きヒストグラムの等値結合 SUM を、変換領域の内積一回から復元する。
- `InvisibleRefresh`　差分の周波数成分がクエリと交わらないとき、そのクエリのキャッシュ更新を全廃する。
- `TruncatedHaving`　高次係数の絶対値予算から HAVING の上界を得て、残りの復号を省く。
- `SplitButterfly`　一つのフラグで群を分割するとき、二つの子の変換済み状態を加減算だけで合成する。
- `ProjectAxis`　ディメンションを一つ捨てる ROLLUP は、その軸の符号を反転しない係数だけを引き継ぐ。
- `PairStats`　強仮説。三つの二値属性の全二属性周辺表が一致すれば、任意の三属性フィルタも交換できるか。
- `DisjointJoin`　互いに素なフラグ集合だけを通す結合を、相手側の補集合マスクに対する部分集合照会へ変換する。

### 集約の計算幅・丸め・失敗行を制御する（Arithmetic）

H2O H4/H5、TPC-DS の AVG/HAVING、CostBench の数値集計から派生。各群の絶対値予算・残差の幅・指数帯を取り、広いレーンや再走査を省ける割合を測る。[提出型](../../TinylambOpt/ResearchGoals/Frontiers/Arithmetic.lean)

- `SignedCRT`　二本の細い剰余レーンと群の下限から符号付き SUM を復元する。下限を負にも設定できる。
- `ZeroJoin`　大きな等値結合の SUM が 0 かを、二本の剰余内積と全項の絶対値予算だけで判定する。
- `RoundingCertificate`　未読ページの寄与範囲の両端が同じ値に丸まる場合、固定小数の SQL 出力を早期確定する。
- `LocalRounding`　強仮説。各 morsel の丸め誤差が最大半単位なら、局所丸めした和を直接返せるか。
- `BinnedAccumulator`　指数帯ごとに異なる基点で整数化し、積和を一度だけ共通精度にそろえて最後に丸める。
- `FirstOverflow`　並列 SUM の溢れを群単位で検査し、初めて範囲を出たブロックだけを開けば元の失敗行を得る。
- `QuadraticJoin`　二次損失の総和を両入力の件数・和・平方和から計算し、対ごとの乗算と結合を省く。
- `QuantizedAverage`　低精度量子化した列の整数商と非零剰余数から AVG の上界を得て、HAVING を棄却する。
- `CenteredProduct`　行群の幅の上限から必要な乗算精度を決め、積の下位ビットだけで符号付きの群内積を復元する。
- `ShiftedCovariance`　二列の中心化した積和から母共分散の分子を得る。群の基点を各列の平均から遠い値に取っても正確か。

### 巨大な述語・CASE を評価する状態数の上限と下限（DecisionTables）

Snowset/PBench 派生の多重ネストと Public BI の CASE が起点。決定的でエラーのない述語を対象に、許容される入力、NULL、利用クエリごとの観測、変数順で残余表の異なり数を比較する。[提出型](../../TinylambOpt/ResearchGoals/Frontiers/DecisionTables.lean)

- `SharedResidual`　複数の WHERE を同時にコンパイルするとき、残る入力に対する真理値表が等しい節を一状態に畳む。
- `StateLowerBound`　任意の共通決定プログラムが同じ出力を返すために必要な中間状態数の下限を、残余表の異なり数で与える。
- `Coarsening`　後半の入力が辞書制約で減ると、旧状態を分割せず併合だけで決定表を再構築できる。
- `JointWidth`　二つの独立した利用クエリ集合を同じ走査に載せるとき、必要な状態数を個別状態数の積で上から押さえる。
- `WeightedQueries`　NULL を含む相関キーを一度ずつ評価し、物理行の重複度を復元して複数の FILTER 件数を同時に得る。
- `GroupDispatch`　GROUP ごとに定数となる判定は代表行で一度実行し、キー一致群の測度をそのまま加算する。
- `NegatedConsumer`　強仮説。WHERE で一致する式を共有した後、否定を置く消費者にも同じビットを再利用できるか。
- `SignedConsumers`　後続で否定される式にも対応するため、式とその否定の採用ビットを対で保存する。
- `AddressFirst`　CASE の選択列を先に読む順序では、n 個の出力候補に対する途中状態数を n 以下に抑えられる。
- `PayloadFirst`　同じ CASE で値列を先に読むと、選択列が一意でも途中状態が指数個必要になるか。

### どの行が消えても結果が残るかを使った再計算の削減（Provenance）

TPC-H Q4 の EXISTS、Iceberg 相当の anti-join、CostBench の更新で再利用しうる導出の情報を扱う。一つの証人は結果を導ける物理行 ID の組。包含・重複・共有 ID の割合で保持費用が変わる。[提出型](../../TinylambOpt/ResearchGoals/Frontiers/Provenance.lean)

- `Antichain`　包含される導出を一度取り除けば、将来どの物理行を削除しても EXISTS の再評価には極小の証人だけで足りる。
- `ProductCount`　二つの結合枝の証人の直積を展開せず、削除後も残る導出数を局所カウントの積から得る。
- `SignatureWeights`　同一行を何度も使う自己結合でも、削除の判定用には ID を一意化し、同一署名の導出を重みで保存する。
- `IncidentDelta`　同時に削除が数行届いたとき、変更 ID を含み、以前は生きていた導出だけを一回ずつ減算する。
- `ProductCut`　結合した二枝のどちらかを完全に潰す最小削除数を求めれば、直積全体の耐障害性が分かる。
- `PackingCertificate`　ID を共有しない導出が k 本あれば、k 未満の新規削除では EXISTS を再検査しなくてよい。
- `PackingExact`　強仮説。最小削除集合の大きさを、任意の多表結合でも互いに素な導出の最大本数から得られるか。
- `BipartiteCut`　二表間に限れば、最小削除集合を物理行 ID が重ならない一致辺の最大本数で厳密に評価できるか。
- `GuardedAnti`　右枝の削除が多くても、行ごとに耐えられる削除数を証明できれば anti-join の候補を開かず除外する。
- `UniversalImplication`　各結果キーの依存集合を比較し、包含関係が既知ならあらゆる削除に対する EXISTS の含意を静的判定できる。

### spill・バッファ・morsel 割当ての物理コスト（SpillScheduling）

TPC-DS の大規模集約、H5 の偏った結合、L1 の長い隣接列から着想。再参照区間、run の実長、部分木ごとの入出力幅を測定対象とする。元の測定で spill が発生したかは仮定しない。[提出型](../../TinylambOpt/ResearchGoals/Frontiers/SpillScheduling.lean)

- `StackDistance`　spill の読み出し列から再参照距離を一度求め、全キャッシュ容量の LRU ヒット数を再実行なしで予測する。
- `OfflineCache`　読み込み間の常駐を半開区間とみなし、k 回以上ヒットするキャッシュ計画の存在を容量付き区間選択に帰着する。
- `SingleSlot`　バイパスを許す 1 枠キャッシュでは、終了の早い再参照区間を貪欲に採用すればヒット数が最大になる。
- `TreePeak`　可換な実行木の左右を局所で決定し、全体の同時生存バッファの最小値を線形サイズの状態から得る。
- `ExcessFirst`　出力を保持する必要のある枝の実行順を、内部ピークと出力幅の差で決める。
- `MinimumWrites`　任意の二つの run を混ぜられる外部ソートでは、最短二本ずつのマージが総書込行数を最小化する。
- `AdjacentRuns`　強仮説。入力順を保つ隣接 run のマージだけでも、任意ペアを許すときの最小書込量を達成できるか。
- `AdjacentPenalty`　三本の順序制約付き run の最適コストを算出し、任意ペアの下限と一致する条件を planner に渡す。
- `LargestFirst`　重い morsel 順で最も空いた worker に置く割当ての完了時間を、任意の理想的な非分割割当てと比較する。
- `TileImbalance`　各 morsel を所定の上限以下に分割できる場合、貪欲な割当ての負荷差を一片の最大長で制限する。
