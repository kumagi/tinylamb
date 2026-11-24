# DuckDB より遅かったクエリから追加した未証明命題 50 件

11 月 24 日の保存済み結果から、同じ問いで tinylamb の中央値が DuckDB を上回ったものを抽出した。Cloud/BI は比較可能な系列のうち 6 件。3 スイートの after-wave8 測定は結果検証を通った両者の組に限ると、TPC-DS が 98 問中 44 件、ClickBench が 43 問中 17 件、TPC-H が 22 問中 8 件だった。下表には今回の提出型の起点にしたクエリを記す。速度差は候補を選ぶ材料で、個々の演算の寄与は別途プロファイルで確かめる。[Cloud/BI 生データ](../../../benchmark/comparison/reports/cloud-bi/measurements.json)・[3 スイートのローカル測定集計](../../../build/benchmarks/three-db-sf10-20261122/after-wave8-20261124/summary.json)

| 起点 | tinylamb / DuckDB の中央値（ms） | 調べた処理 |
| --- | --- | --- |
| Public BI・CostBench | MLB suffix CASE 44.376 / 2.413、Q108 3.377 / 1.244、refresh 1.929 / 1.085 | 文字列分類、複数列の MIN/MAX、バッチ集計公開 |
| Iceberg を表現した SQL | base 1.190 / 0.501、equality 3.046 / 1.446、both / part 0 3.364 / 2.611 | ファイル結合、削除キー構築、位置削除と等値削除の重なり |
| TPC-DS SF10 | Q22 941.73 / 144.45、Q38 259.91 / 72.41、Q72 293.45 / 87.35、Q75 248.37 / 103.69、Q87 218.47 / 85.69 | ROLLUP、チャネル間集合演算、不等号付き在庫結合 |
| ClickBench 100M | Q20 15.81 / 4.42、Q21 371.56 / 339.33、Q22 455.33 / 340.61、Q23 389.81 / 329.78、Q24 45.90 / 26.02、Q37 98.20 / 15.48、Q38 14.20 / 2.52 | URL・Title の比較、上位グループの確定、遅延取得 |
| TPC-H SF10 | Q3 71.74 / 54.45、Q4 55.29 / 52.27、Q7 59.07 / 54.54、Q8 131.42 / 52.31、Q13 158.97 / 157.61 | 注文経由の多重度伝播、国別分岐、存在判定、注文数ヒストグラム |
| 派生分析の既存記録 | SF10 の C3 66.3 / 8.2、C2 42.6 / 15.8。SF1 の H5 162.6 / 7.7、C4 103.5 / 5.5、L1 81.9 / 16.0 | 結合前の状態圧縮、並行辺・重複 ID・偏りへの対応 |

3 スイートの数値は各 JSONL の `tinylamb.median_ms` と `duckdb.median_ms`。after-wave8 の集計に収録された DuckDB 値を使用した。各スイートは独立した条件で、派生分析の SF1 数値も当時の Release + ANALYZE + TLCS の記録である。[TPC-DS](../../../build/benchmarks/three-db-sf10-20261122/after-wave8-20261124/tpcds/results.jsonl)・[ClickBench](../../../build/benchmarks/three-db-sf10-20261122/after-wave8-20261124/clickbench/results.jsonl)・[TPC-H](../../../build/benchmarks/three-db-sf10-20261122/after-wave8-20261124/tpch/results.jsonl)・[C2/C3 の記録](../../TinylambOpt/ResearchGoals/Wave9/AnalyticsRemaining.lean)・[H5](analytics-losses/bilateral-eager-agg-join-pushdown.md)・[C4](analytics-losses/left-join-right-multiplicity-collapse.md)・[L1](analytics-losses/factorized-join-tree-count-propagation.md)

## 同じ結果を返す SQL での追試

Cloud/BI ランナーのデータ・接続アダプターを再利用し、新しい一時 DB で元 SQL と書き換えの実行順を交互にした。1 回ずつのウォームアップ後、各 7 回の中央値。tinylamb は Release サーバー（SHA-256 `c790124fb70450ae32cafad7c71168689125e725ad02e45fced9e16229cb46bf`）にローカル TCP で接続し、DuckDB 2.0.0 は組み込み。各 2 ワーカー設定で、各 SELECT を同じ Python 参照結果に照合した。下表の「元」も今回の追試の値。[データ生成・元 SQL・参照計算](../../../benchmark/comparison/cloud_bi_bench.py)

| 元 SQL → 書き換え | tinylamb（ms） | DuckDB（ms） | 結果 |
| --- | ---: | ---: | --- |
| Iceberg base → 一意で網羅的なファイル表との結合を除く | 0.730 → **0.056** | 0.601 → **0.262** | COUNT 4,608、SUM 10,614,528 |
| Iceberg equality → 先に DISTINCT (file, key) を作り anti-join | 2.724 → **0.824** | 1.380 → **1.172** | COUNT 4,104、SUM 9,476,856 |
| MLB suffix CASE → 一致した守備位置名を列挙する文字列 IN | 40.533 → 51.520 | 1.804 → 2.074 | 同じ 32 群。語彙取得の初回 SELECT DISTINCT は別途 43.774 / 1.528 ms |

base の書き換えは `SELECT COUNT(*), SUM(rid) FROM ice_rows`。equality の書き換えは下記。fixture のキー領域では -1 を NULL の専用コードにでき、ファイル ID は非 NULL。Lean 側は `Option Int` により NULL と全数値を区別する。

```sql
SELECT COUNT(*), SUM(x.rid)
FROM ice_rows x
LEFT JOIN (
  SELECT DISTINCT f.fid, COALESCE(e.k, -1) AS keyval
  FROM ice_files f JOIN ice_eq e ON f.seq < e.seq
  WHERE f.part = e.scope OR e.scope = -1
) h ON x.fid = h.fid AND COALESCE(x.k, -1) = h.keyval
WHERE h.fid IS NULL
```

MLB の試行は `SELECT DISTINCT pos FROM mlb` のうち末尾が ` Fielder` の語を、元の CASE の `pos IN (...)` に展開した。既報の「先に 144 組に集約してから CASE」を含め、同じ意味を持つ書き換えでも費用は異なる。以下の辞書コード案は、既存の符号化列を直接利用できる場合に検証する。[既報の元 SQL と二段集約の比較](../../../benchmark/comparison/reports/cloud-bi/README.md)

## Lean 提出型

各名前には `TinylambOpt.ResearchGoals.DuckDBGaps.<モジュール>.` を前置し、`.Goal` を付ける。提案は `def …Goal : Prop`。数値集約が比較するのは行数、非 NULL 数、拡張精度の整数和またはその組。実装での固定幅オーバーフロー、FLOAT64 の再結合、文字列照合、MVCC の可視性には各カーネルの適用時に対応する契約が要る。とくに Q22 の平均値の上界・下界を使う場合、実際の丸めと同順位の扱いまで枝刈り条件に含める。

### Slices — MLB Q108 と接尾辞分類、7 件

既存の `GroupBeforeSuffix`、`ClassificationCache`、`PartialExtrema` を参照し、符号の偏り、辞書の追記、選択行での極値の確定を対象にした。実装での照合先はブロックのメタデータから MIN/MAX を返す経路。[提出型](../../TinylambOpt/ResearchGoals/DuckDBGaps/Slices.lean)・[既存モデル](../../TinylambOpt/ResearchGoals/CloudBI/PublicBI.lean)・[集約カーネル](../../../executor/detail/typed_stream_aggregate.cpp)

| 宣言名 | 省ける処理と成立条件 |
| --- | --- |
| MinorityCodes | 分類辞書の真・偽の少ない側だけを保持し、既定ビットとの XOR で復元する。例外コード数は辞書長の半分以下。 |
| AppendDictionary | コードを維持する辞書追記で既存の例外表に差分を継ぎ足し、分類済み語の再評価を省く。範囲外コードは ELSE。 |
| ComplementGroups | league の総数から outfield の件数を引き、もう一方の群を復元する。件数ゼロの群は出力しない。 |
| PairwiseExtrema | 隣接する有効値を一度比較し、大きい側だけを MAX、小さい側だけを MIN に渡す。奇数末尾と全 NULL を含む。 |
| DominatedColumn | 選択行が未確定でも、列の上下限が既得の極値に収まるページはその列をデコードせずに済む。 |
| WitnessedExtrema | 選択行中にページの最大・最小の値を持つ行がそれぞれ見つかれば、残る値の読み取りを省く。 |
| SparsePosting | 疎な転置リストから重複・範囲外を除いて他の二条件と突合し、元の行順でペイロードを一度ずつ取得する。 |

### FileMasks — Iceberg 相当の関係演算、7 件

既存の `EqualityHorizon` はキーごとの最大削除 sequence を扱う。ここではファイルの識別子・共有可能なファイル署名・削除が当たった後の読み取り順を扱う。共通 SQL の速度差はネイティブ Iceberg コネクタの速度とは別に測定した。[提出型](../../TinylambOpt/ResearchGoals/DuckDBGaps/FileMasks.lean)・[既存の削除規則](../../TinylambOpt/ResearchGoals/CloudBI/DeleteFiles.lean)

| 宣言名 | 省ける処理と成立条件 |
| --- | --- |
| ReferencedFiles | 参照される file ID にちょうど一つ対応するファイルがあれば COUNT/SUM から結合を除く。空の SUM は NULL。 |
| FileKeyAntiJoin | ファイル・等値削除から削除対象の (file, key) を先に構成し、各データ行を直接プローブする。 |
| SharedDeleteTable | partition spec・partition 値・データ sequence が等しいファイルで削除辞書を共有する。空辞書も共有可能。 |
| PartitionSpecialization | partition を先に限定し、そのファイル群の最古 sequence 以下の削除を外してからファイル別集合を作る。 |
| IncrementalSurvivors | 既存スナップショットの生存行に、新規削除ファイルが言及するキーの分だけ追加判定する。 |
| LateEqualityKey | 位置削除に当たった行は等値キーを取得しない。生存行の順序とキー取得回数を合わせて規定する。 |
| ArithmeticFooter | 行 ID が等差数列であるファイルでは有効な削除位置だけから件数と ID 和を求める。全削除時は NULL。 |

### Refresh — CostBench の集計公開、6 件

refresh の測定には集計結果の受信と再 INSERT が含まれる。書き込み中に集計状態を得る候補について、読み取り時点、既存バッチへの追記と部分的な再送を明示した。[提出型](../../TinylambOpt/ResearchGoals/DuckDBGaps/Refresh.lean)・[既存の公開・再送モデル](../../TinylambOpt/ResearchGoals/CloudBI/Quotes.lean)

| 宣言名 | 省ける処理と成立条件 |
| --- | --- |
| InsertSideState | 既知の公開状態から INSERT と同時に集計し、すでに行のある batch でも再走査を省く。 |
| ChangedSymbols | バッチ中の訂正で取り除いた寄与と追加分だけを反映し、全 NULL 群と消滅した群を区別する。 |
| OverlapReplay | 既に確定した連続プレフィックスと再送区間が重なる場合、未確定の ordinal から適用する。 |
| LaneOrSpill | 密な銘柄範囲の直接配列と範囲外用ハッシュを併用する。基点より小さい値を先に弾く。 |
| VisibleFlush | 複数の公開単位をまとめるとき、読者の世代までの部分状態を合成して過去スナップショットを保つ。 |
| DirtyFlush | morsel が更新したキーだけを、元の集計に加算した値で一度ずつ出力する。 |

### Retail — TPC-DS、8 件

既存の Q22 の品目事前集約、Q38 の surrogate 重複排除を起点に、整列済み入力、閾値順位、チャネル間の集合の保持方法に着目した。Q72 ではフィルタ伝播を行う既存の結合入力構築の後にも不等号比較が残る場合を扱う。[提出型](../../TinylambOpt/ResearchGoals/DuckDBGaps/Retail.lean)・[既存の ROLLUP・集合演算](../../TinylambOpt/ResearchGoals/Wave9/TpcdsRollupAndSetOps.lean)・[結合入力構築](../../../executor/detail/build_input.cpp)

| 宣言名 | 対象と候補 |
| --- | --- |
| OrderedRollup | Q22。整列済みの各階層でプレフィックスが変わったときだけ状態を出す。空入力時に総計一行を残す。 |
| AverageFence | Q22。残り有効数の上限と値の下限を使い、AVG が厳密に上位 K 圏外になる条件を除算なしで判定する。 |
| SuffixFlush | Q22。隣接キーの共通プレフィックス長から、境界で確定する ROLLUP 階層のみを列挙する。 |
| ThresholdFanout | Q72。同一 item/week/warehouse の非 NULL 在庫量を整列し、販売数量未満の行数を二分探索する。 |
| PromoReturnCounts | Q72。二つの外部結合で promo 有無と returns の多重度を重みにし、未一致の補完一行を保つ。 |
| ShrinkingIntersect | Q38。小さいチャネルの射影キーを起点に残った候補だけを第 3 チャネルに照会する。照会数も規定する。 |
| ExclusionErase | Q87。store の射影集合から catalog/web で見たキーを直接消し、除外側の不要キーを保持しない。 |
| NetPairSet | Q75。群ごとに販売数量と金額の組を重複排除し、新規の組だけ両方の SUM に反映する。 |

### Hits — ClickBench、8 件

既存の Misra–Gries による HAVING 候補と二段 Top-N を起点に、実際の相殺回数を利用した上位キーの確定と、確定後のメンバー数・文字列取得に進んだ。[提出型](../../TinylambOpt/ResearchGoals/DuckDBGaps/Hits.lean)・[既存の候補抽出](../../TinylambOpt/ResearchGoals/ExecutionStrategies/SearchBounds.lean)・[二段集約実装](../../../executor/detail/columnar_tlcs_fast_path.cpp)

| 宣言名 | 対象と候補 |
| --- | --- |
| CancellationDebt | Q22/Q37/Q38。残存カウンタの総和から相殺回数を復元し、各キーの真の件数の誤差上限を得る。 |
| CertifiedWinners | Q22/Q23/Q37/Q38。相殺回数を厳密に上回る候補が K 個あれば候補内の再計数で境界同点を含む順位を確定できる。 |
| OwnerDistinct | Q23。勝者の非 NULL UserID を決まった shard に割り振り、局所 distinct 件数の和から全体を得る。 |
| OrderedCodeMin | Q22/Q23。照合順で並ぶ局所辞書で勝者の最小コードだけデコードし、ページ間の文字列 MIN を取る。 |
| FrontCodedContains | Q21/Q22。辞書の共通 prefix が文字列に一致すれば全語採用。それ以外は境界をまたぎ得る末尾だけ次の断片に足す。 |
| OrderedFetch | Q24。時刻順の行 ID で URL 述語を K 行目のヒットまで試し、広い行を出力分だけ取得する。 |
| DatePostings | Q37/Q38。CounterID とフラグを満たす URL/Title の日付頻度から、完全なバケットを一括加算し端だけ再判定する。 |
| StridedPoint | Q20。等間隔の基準値＋符号なしオフセットで表せるページなら、不合同な UserID はオフセットを読まずに棄却する。 |

### Orders — TPC-H、7 件

結合後に残す列、右側の重複、CASE の NULL 規則まで保存した候補。Q3 の run 境界合成は整列性が利用できる場合に適用する。[提出型](../../TinylambOpt/ResearchGoals/DuckDBGaps/Orders.lean)・[クエリテンプレート](../../../benchmark/tpch_queries.hpp)

| 宣言名 | 対象と候補 |
| --- | --- |
| MarketShareWeights | Q8。供給国の該当数と全数を使って分子・分母の状態を同時更新し、ELSE 0 の非 NULL 寄与を保つ。 |
| YearRoutes | Q8。顧客・地域絞り込み後の経路多重度を (order, year) に持たせ、lineitem から引く。 |
| DirectionBuckets | Q7。対象二国を方向ビットへ変換し、NULL・他国・同国間を除いて方向別・年別に集約する。 |
| LateSlack | Q4。行ごとの日付差の最大が正なら遅延明細が存在する。異なる行の commit と receipt を取り違えない。 |
| RetireWitness | Q4。対象注文を最初の遅延明細で処理済みにし、その注文に対応する priority の多重度を一度だけ足す。 |
| RebucketOrders | Q13。除外コメントがある顧客だけ、注文数ヒストグラムの元の階級から新しい階級に移す。 |
| RevenueRuns | Q3。注文順の morsel 内で収益を畳み、境界をまたぐ一組だけマージする。空 morsel・全 NULL を含む。 |

### BridgeStates — H2O・STATS-CEB・LDBC-SNB、7 件

既存の左右事前集約と二役の posts/users プロファイルを再利用した。実装側には因子化結合と右側多重度の縮約がある。今回の候補は状態そのものの共有、重複する経路の圧縮、パーティションごとの実行方式を扱う。[提出型](../../TinylambOpt/ResearchGoals/DuckDBGaps/BridgeStates.lean)・[既存の H5 モデル](../../TinylambOpt/ResearchGoals/AnalyticsWorkloads/H2o.lean)・[因子化結合実装](../../../executor/detail/analytics_factorized_join.hpp)

| 宣言名 | 対象と候補 |
| --- | --- |
| RightStateClasses | H5。結合キーが異なっても右側の (行数, 非 NULL 数, 和) が一致すれば、左部分状態を共有できる。 |
| ShardedHybrid | H5。radix 分割ごとに hot/cold 戦略を変え、複数 shard の同一出力グループを両側の有効数付きで統合する。 |
| SourceWeights | C3。重複リンクを端点対で数え、到着先の多重度を始点に集めてから score を一度加算する。 |
| EndpointDemand | C3。選択 link type に実際に出現する始点・終点だけ、別々の役割の posts を準備する。 |
| CommentQuotient | C2。重複コメントを (post, user) でまとめ、コメント側・投稿者側のユーザー条件を別の多重度で反映する。 |
| CollisionBucket | C4。ハッシュバケットの中身が同じ綴りなら一致判定を一回に縮め、綴りが混在する場合だけ残余比較する。 |
| CountryProfiles | L1。友人へ入る経路を国の多重集合で分類し、同じプロファイルの友人の重みを国への配賦前にまとめる。 |
