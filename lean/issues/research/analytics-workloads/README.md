# 派生分析ベンチマークの実行計画と物理演算（第八波・未証明）

リポジトリの JOB、STATS-CEB、H2O、LDBC-SNB、BigBench に含まれる **全 26 クエリに対する 33 件**の提出型。各表の宣言名に `.Goal` を付けると、リンク先モジュールの命題になる。たとえば `TinylambOpt.ResearchGoals.AnalyticsWorkloads.SocialGraphJoinWeights.ActiveFriends.Goal` は L1 の国別件数と隣接リストの読取り数を同時に問う。[実際の SQL と生成器](../../../../benchmark/comparison/analytics_bench.py)

### 候補を選んだ根拠

第六波には両側事前集約、結合木の重み伝播、局所辞書の二段集約など 18 件の証明があり、C++ にも `TryCollapsedRightGroupedJoin`、`TryFactorizedJoinTreeAggregate`、`TrySmallDimStarDirectAggregate` がある。今回はその次の選択、たとえば「どのキーだけを事前集約するか」「辞書を何回引くか」「結果群のどの部分をソートするか」を提出型にした。[第六波の計測と証明一覧](../analytics-losses/README.md)・[現在の実行器](../../../../executor/detail/analytics_factorized_join.cpp)

次の件数は `analytics_bench.py --generate-only --scale 1 --seed 1729` で生成した CSV を数えたもの。J4・C4・H5・L1・B2 では、候補を独立した集計で実行し、同じデータを使った DuckDB の元の SQL と結果の多重集合も照合した。この比較が検査するのは結果であり、tinylamb での所要時間は未計測。[生成・計測条件](../../../../benchmark/comparison/README-analytics.md)

| 問 | 入力や中間結果で観測した件数 | 命題で狙う削減 |
| --- | --- | --- |
| J4 | bridge 100,000 行、異なる (映画, キーワード文字列) は 76,345 組、結合後は 732,678 行、出力は 8 群 | 同じ文字列を表す別 ID を早めにまとめ、映画ごとの情報件数を掛ける。 |
| C4 | 通過した投稿は NULL タグ 1,546 行と sql タグ 4,691 行。右表で重複する名前は sql だけ。外部結合後は 1,174,296 行 | 基本重み 1 に、重複する名前だけ追加重みを足す。このデータなら比較対象は一つ。 |
| H2 / H5 | H2 の 37,679 群に対して異なる COUNT は 121 種、最大 237。H5 は右キー 1,000 種、左の (群, 結合キー) は 127,405 組、結合は 23,995,178 行 | H2 は小さい COUNT をバケット出力する。H5 は左側にも集約表を作るキーを選べるようにする。 |
| L1 | knows 40,000 辺のうち、年条件と likes/message を通る 2,446 人に入る辺は 12,959 本。該当経路は 2,124,422 本、出力は 10 か国 | 逆向き隣接索引があれば 12,959 本を一度ずつ読み、likes 側の多重度を加算する。索引構築費は別に見積もる。 |
| B2 / B4 | B2 は訪問 150,000 行・異なる URL 19,646 種、順位条件を通る右 URL は 7,205 種、結合は 1,367,564 行。B4 の非 US 行 120,198 件で検索語は 2,000 種 | B2 は同じローカルコードからの文字列検索を再利用し、B4 は実際に LIKE が必要な語だけ調べる。ページ単位の辞書では同じ語が別ページにも現れ得る。 |

### モデルの適用範囲

- COUNT・SUM・AVG を同時に扱う命題は、グループの行数、非 NULL 件数、正確な整数和を比較する。これで空グループ、全 NULL、真の和 0 を区別できる。AVG の浮動小数点への変換や固定幅での中間加算には実行器側の契約が必要。検査付き加算の適用条件は `RepeatedScore` と `RepeatEnvelope` で個別に扱う。
- 命題が比較する入力段階は表に示した。たとえば L1 の辺は source 人物と場所の結合結果を保持し、active 側は条件を通った p2/like/message の袋である。各カーネルを実行計画全体に接続するときには、その入力段階の多重度とスナップショットを保存する。
- ベンチマークは PK を宣言しない。ユーザー ID などの一意性が必要な命題では、利用時に検証する。合成データに規則的な文字列や連続 ID が現れても、辞書世代・文字列表現・範囲の適用条件は残る。

## JOB（6 件）

[Job の提出型](../../../TinylambOpt/ResearchGoals/AnalyticsWorkloads/MovieJoinFiltersAndFanout.lean)

| 宣言名 | 問 | 選択・削減する処理 |
| --- | --- | --- |
| CompanyPair | J1 | 作品・年を絞った bridge で、会社と会社種別のフィルタ後多重度の積をキー対ごとにキャッシュする。単なる存在ビットにすると重複次元行が消える。 |
| RatingOrder | J2 | バイナリ照合の rating 辞書で ASCII の「数字・点・数字」だけを整数コード化して比較する。他の表記は文字列比較に戻す。たとえば 10.0 は文字列では 7.0 より小さい。 |
| FixedGender | J3 | gender = F の後に残るグループを一つの集約器で処理する。空入力時はグループを出力せず、通過行の年がすべて NULL なら AVG は NULL になる。 |
| LabelQuotient | J4 | keyword ID を投影先のラベルへ写し、(映画, ラベル) ごとの bridge 多重度と、絞り込み後の movie_info 件数を掛ける。 |
| DecimalPrefix | J5 | 辞書作成時に正準の aka-整数と桁幅を確認したエントリでは、aka-1% を数値の桁範囲で判定する。aka-01 や aka-1x は文字列の経路に残せる。 |
| CorrelatedFanout | J1・J2・C1 | 年・種別など二条件の相関が未知でも、同じファンアウトで重み付けしたセルの周辺度数から通過件数の上下界を導く。 |

## STATS-CEB（6 件）

[Ceb の提出型](../../../TinylambOpt/ResearchGoals/AnalyticsWorkloads/UserRolesAndCommentJoins.lean)

| 宣言名 | 問 | 選択・削減する処理 |
| --- | --- | --- |
| RepeatedScore | C1・H5 | 同じ符号付き値をファンアウト回だけ連続加算する場合、除算一回で最後まで安全か、何回目に失敗するかを求める。 |
| RoleRouting | C2 | 一意性確認後に、適格な所有者 ID を post ID に付け、コメント投稿者の適格集合を別に持つ。comments の一走査で正しい所有者へ加算できる。 |
| EndpointProfiles | C3 | posts の同じ ID について、リンク始点用のスコア集約と終点用の post_type = 1 の件数を区別する。自己リンク・重複 ID でも AVG の分母を保つ。 |
| SparseOuterCorrection | C4 | 選択済みの各投稿の基本重みを 1 とし、右で複数回現れるタグだけ例外にする。例外が 0 個なら定数、1 個なら一比較、複数なら例外表を使う。 |
| DemandBadges | C5 | 通過した投稿が参照する所有者だけに users/badges の集計を絞る。適格ユーザーの重複分は badge 件数に掛ける。 |
| YearOnlyOrder | C5 | (年, 種別) で集約した行を年ごとに格納し、年のキーだけを整列する。年が同じグループのデータも順序も保存する。 |

## H2O（6 件）

[H2o の提出型](../../../TinylambOpt/ResearchGoals/AnalyticsWorkloads/HotKeyAndEpochAggregates.lean)

| 宣言名 | 問 | 選択・削減する処理 |
| --- | --- | --- |
| HotRegister | H1 | 頻出の id1 を専用カウンタへ分離し、その出現回数ぶん汎用表への更新が減ることを、出力件数と同時に検証する。 |
| CountTail | H2 | COUNT が閾値以下の群を直接バケット出力し、大きい群だけをソートする。ソート対象数の上界と同順位の順序も含む。 |
| ThreeBaseSums | H3 | 3 列の基準値との差分を集約してから、各列の非 NULL 件数で基準値を戻す。基準値と妥当性はブロック・列ごとに異なってよい。 |
| EpochAverage | H4 | id3 フィルタを通った (id1,id2) だけを、新しい世代番号で初期化する。古い表の全消去を省き、最初の通過値が NULL でも群を記録する。 |
| SkewHybrid | H5 | 一部の id4 だけ両側事前集約し、残りは右側の件数・和を直接プローブする。キーの選び方に依存せず、二つの nullable SUM を同時に保存する。 |
| InRangeSynopsis | H6 | id1 ごとの id2 累積集計を再利用できる場合、IN の異なる各 ID について二端点の差で COUNT と SUM を得る。上端 40 を含む。 |

## LDBC-SNB（5 件）

[Ldbc の提出型](../../../TinylambOpt/ResearchGoals/AnalyticsWorkloads/SocialGraphJoinWeights.lean)

| 宣言名 | 問 | 選択・削減する処理 |
| --- | --- | --- |
| ActiveFriends | L1 | 条件を通る友人だけ逆向き隣接索引を走査し、国別件数をまとめて更新する。読んだ辺数は active な友人に入る辺数と一致する。 |
| ForumRuns | L2 | 隣接する同一 forum のメッセージ間で適格会員多重度を持ち回る。プローブ回数は入力の forum ラン数になり、forum が後で再出現しても正しい。 |
| ForwardReplies | L3 | 親の年が既知なら直ちに加算し、まだ現れていない親には正の返信スコアの集約だけを保留する。一走査で後方・前方・自己参照を扱う。一意な message ID が条件。 |
| LateForumWeight | L4 | 年と parent のフィルタ後にメッセージを forum 単位に集約し、最後に forum 表の件数を掛ける。forum 欠落、重複、全 NULL スコアを区別する。 |
| DirectoryDegree | L5 | CSR の連続するオフセットから人物の出次数を取り、国別の年齢集約に使う。宛先列を読まず、重複辺も非 NULL 年齢の分母も保つ。 |

## BigBench（5 件）

[BigBench の提出型](../../../TinylambOpt/ResearchGoals/AnalyticsWorkloads/DictionaryJoinsAndLateLabels.lean)

| 宣言名 | 問 | 選択・削減する処理 |
| --- | --- | --- |
| UrlGrouping | B1・B2 | 正準の page-N は整数、他の URL は別タグの生文字列として保持する。B1 の GROUP BY の NULL と page-01 / page-1 を区別し、出力時に元の値を復元する。 |
| CodeFanout | B2 | 辞書のローカルコードから page_rank > 40 の一致件数への変換を全ての国で共有する。不一致も記憶し、文字列で引く回数を使われた非 NULL コード数に抑える。 |
| LateLabels | B3 | 適格 customer の件数を掛けた売上を (store ID, item ID, 年) で集約し、存在したセルだけ region/category に展開する。異なる ID が同じラベルを持ってもよい。 |
| ResidualWords | B4 | US 行を採用した後、残る行で要求された非 NULL の検索語だけ LIKE を評価する。偽の結果も記憶し、重複して両条件を満たす行は一度だけ計上する。 |
| PriceSynopsis | B5 | item ID ごとの価格の最小・最大と非 NULL 件数から、price > 100 が全件または 0 件のバケットを確定する。両側の値が混在するときだけ実値を調べる。 |

## 複数の問で使える計画選択・実行カーネル（5 件）

[PhysicalChoices の提出型](../../../TinylambOpt/ResearchGoals/AnalyticsWorkloads/PhysicalChoices.lean)

| 宣言名 | 問 | 選択・削減する処理 |
| --- | --- | --- |
| RefineFanout | J2・C1・H5 | 頻出キーの正確な寄与と、残りのハッシュバケット件数の積から、結合サイズの区間を作る。正確に追うキーを増やすたびに区間が狭まることも問う。 |
| CachedProbeOrder | J1・B3 | 二つの次元検索について、通過した行が要求する異なるキー数から、各順序のキャッシュミス費用を比較する。行単位の選択率が同じでも順序の費用は違い得る。 |
| RepeatEnvelope | H5・L2 | 同じ build 区間が連続して再生される物理順序では、一回分の部分和範囲と総和から全反復のオーバーフロー判定を行う。符号が混在し、総和が 0 でも使える。 |
| FieldBytes | H2・H4・L2・C2 | 頻出する整数の長さ付き PostgreSQL テキストフィールドを再利用し、DataRow 全体のバイト列と十進変換回数を比較する。 |
| EdgeMorsels | L1・L3 | 長い隣接列の内部でも並列作業を分割する。一作業の辺数に上限を設け、空作業、重複処理、欠落がないことと作業数の上界を同時に問う。 |
