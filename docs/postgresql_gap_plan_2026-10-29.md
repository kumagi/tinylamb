# PostgreSQLに10倍以上劣る指標の改善計画 2026年10月29日

実施状況は[A–Hの実施記録](postgresql_gap_results_2026-10-29.md)へ追記する。
以下は改善前の比較値と当初計画として保存する。

対象は[10月29日の再測定](olap_oltp_comparison_2026-10-29.md)。二桁以上の差を「10倍以上不利」と定義して、全スケール・全接続数を抽出した。中央値に基づく対象は35件、15種類の負荷で、共通原因を8計画に整理した。まず非unique索引の重複値リスト再デコードとコスト見積もりを直し、続いてTop Nの早期停止、EXISTS、結合・集計の中間結果、索引なし検索を改善する。書き込みとHTAPは短期測定の揺らぎを分離してから修正箇所を決める。

## 判定方法と対象範囲

小さいほど良い実行時間はtinylamb／PostgreSQL、大きいほど良い成功TPS・分析query/sはPostgreSQL／tinylambを不利倍率とした。丸める前の値で10倍以上を抽出する。OLAPは初回を除いた5回の中央値、OLTPのTPSは3試行の中央値、p50は各試行の成功時p50の中央値であり、全試行の成功サンプルをまとめたp50ではない。

416比較の内訳は、OLAP 152、OLTPのTPS／p50／p95／p99 256、HTAP役割別8。主対象はOLAP 21件、OLTPのTPS・p50 10件、HTAP分析query/s 4件の計35件。p95・p99の「3試行の中央値」にも6件の差があるので、補足表で漏れなく扱う。

ロード時間・索引構築時間・first_msは単発値であり、中央値の対象に入れない。PostgreSQLの中断数が0なので中断率の倍率も定義しない。対象のOLAP結果はすべて一致し、OLTP／HTAPの最終状態の不変条件も成立している。

比較する数値・境界は表の表示桁で丸めている。合否判定には[gaps.json](../benchmark/comparison/results/2026-10-29-gap-plan/gaps.json)の元の値を使い、厳密に10倍未満を目指す。これは最初の受け入れ基準で、改善後の性能予測ではない。

## 10倍以上の差がある全指標

### OLAPの実行時間

単位はms。

| 計画 | 追加索引 | 負荷 | 規模 | 指標 | tinylamb | PostgreSQL | 不利倍率 | 10倍差解消の境界 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C | なし | exists | 1万行 | median_ms | 6.117329 | 0.547515 | 11.17× | <5.475150 |
| D | なし | join_group | 100万行 | median_ms | 888.471151 | 30.228157 | 29.39× | <302.281569 |
| E | なし | left_join | 100万行 | median_ms | 593.712504 | 32.374374 | 18.34× | <323.743740 |
| C | なし | exists | 100万行 | median_ms | 174.310787 | 8.474879 | 20.57× | <84.748790 |
| A | あり | count | 1万行 | median_ms | 4.359754 | 0.212917 | 20.48× | <2.129170 |
| A | あり | sum | 1万行 | median_ms | 9.705751 | 0.355559 | 27.30× | <3.555591 |
| B | あり | top_100 | 1万行 | median_ms | 6.064809 | 0.046218 | 131.22× | <0.462179 |
| C | あり | exists | 1万行 | median_ms | 5.589772 | 0.399423 | 13.99× | <3.994231 |
| A | あり | count | 10万行 | median_ms | 74.878236 | 1.836547 | 40.77× | <18.365470 |
| A | あり | sum | 10万行 | median_ms | 235.215359 | 3.168560 | 74.23× | <31.685600 |
| B | あり | top_100 | 10万行 | median_ms | 56.729033 | 0.051909 | 1092.86× | <0.519090 |
| C | あり | exists | 10万行 | median_ms | 10.286691 | 0.625745 | 16.44× | <6.257449 |
| A | あり | count | 100万行 | median_ms | 4316.349950 | 6.793601 | 635.36× | <67.936011 |
| A | あり | sum | 100万行 | median_ms | 8950.838483 | 10.604006 | 844.10× | <106.040060 |
| A | あり | filter_1pct | 100万行 | median_ms | 45.405763 | 0.257061 | 176.63× | <2.570610 |
| A | あり | distinct | 100万行 | median_ms | 4844.186092 | 23.526269 | 205.91× | <235.262690 |
| B | あり | top_100 | 100万行 | median_ms | 574.413012 | 0.072108 | 7966.01× | <0.721080 |
| D | あり | join_group | 100万行 | median_ms | 884.940355 | 30.550441 | 28.97× | <305.504410 |
| E | あり | left_join | 100万行 | median_ms | 583.665488 | 15.837569 | 36.85× | <158.375689 |
| C | あり | exists | 100万行 | median_ms | 171.801853 | 3.443284 | 49.89× | <34.432840 |
| A | あり | Q06 | SF 0.01 | median_ms | 28.968216 | 0.861846 | 33.61× | <8.618460 |

### OLTPの成功TPSとp50遅延

tpsは成功txn/s（nonkey_readはquery/s）、p50_msはms。p50は中断時の待ち時間を含まない。

| 計画 | 追加索引 | 負荷 | 規模 | 指標 | tinylamb | PostgreSQL | 不利倍率 | 10倍差解消の境界 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| F | なし | nonkey_read | 4接続 | tps | 2518.666667 | 50255.000000 | 19.95× | >5025.500000 |
| F | なし | nonkey_read | 4接続 | p50_ms | 1.524621 | 0.076385 | 19.96× | <0.763851 |
| F | なし | nonkey_read | 16接続 | tps | 3834.000000 | 85471.333333 | 22.29× | >8547.133333 |
| F | なし | nonkey_read | 16接続 | p50_ms | 4.111189 | 0.182358 | 22.54× | <1.823580 |
| G | なし | update | 16接続 | tps | 1943.000000 | 21750.000000 | 11.19× | >2175.000000 |
| G | なし | update | 16接続 | p50_ms | 8.142147 | 0.267100 | 30.48× | <2.670999 |
| G | なし | hot_update | 1接続 | tps | 466.333333 | 7586.000000 | 16.27× | >758.600000 |
| G | なし | hot_update | 1接続 | p50_ms | 2.119847 | 0.124348 | 17.05× | <1.243480 |
| G | あり | transfer | 16接続 | p50_ms | 8.080888 | 0.286046 | 28.25× | <2.860461 |
| G | あり | insert_delete | 16接続 | p50_ms | 7.938176 | 0.272851 | 29.09× | <2.728510 |

### HTAPの分析query/s

混合負荷の合計TPSで分析側の遅さを隠さず、reader役の完了query/sを比べる。

| 計画 | 追加索引 | 負荷 | 規模 | 指標 | tinylamb | PostgreSQL | 不利倍率 | 10倍差解消の境界 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| H | なし | htap | 4接続 | reader_tps | 0.666667 | 32.666667 | 49.00× | >3.266667 |
| H | なし | htap | 16接続 | reader_tps | 0.666667 | 30.333333 | 45.50× | >3.033333 |
| H | あり | htap | 4接続 | reader_tps | 0.666667 | 39.000000 | 58.50× | >3.900000 |
| H | あり | htap | 16接続 | reader_tps | 0.666667 | 37.666667 | 56.50× | >3.766667 |

## A 索引走査の重複デコードとコスト見積もり

対象はCOUNT 3条件、SUM 3条件、filter_1pct、DISTINCT、TPC-H Q06の9件。COUNTは635倍、SUMは844倍、DISTINCTは206倍に達する。

### COUNTの比較

両製品とも索引のみを読む計画を使う。PostgreSQLはParallel Index Only Scan＋部分／最終集計、tinylambは逐次IndexOnlyScan＋scalar集計である。したがって「索引を使ったこと」自体が原因ではない。tinylambの実測は約301万buffer hit、miss 0。非unique索引では前進と状態更新のたびに値リスト全体をデコードしており、同一キーにd件あればデコード仕事量がO(d²)になり得る。1万／10万／100万行ではaccount_idの重複数が1／10／100になり、この軸を独立に測る必要がある。

[tinylamb count あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-count-tinylamb.json)、[postgresql count あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-count-postgresql.json)。実装: [前進処理](../index/index_scan_iterator.cpp#L301)、[状態更新](../index/index_scan_iterator.cpp#L210)、[vectorデコード](../common/decoder.hpp#L43)。デコードが全時間の何割かは未計測。

### SUMと1パーセント選択の比較

SUMではPostgreSQLがParallel Seq Scanを選ぶのに対し、tinylambは無条件のIndexSkipScan／ParallelIndexScanから100万行を取り出す。約803万hit、miss 0だった。無条件索引候補は基本的に行数ベースで、coveringなら一律0.5倍にする。デコード・可視性確認・ランダムheap参照・batch処理の実測差を、この費用が十分表せているかを較正する。

filter_1pctでは両製品とも(category, amount)の範囲を読む。PostgreSQL 0.257 msに対してtinylamb 45.406 msで、こちらは索引の実行経路自体を速くする必要がある。16worker集計へ渡す同期、重複リストのデコード、行から値への復元を分けて計測する。索引なし全走査の10.648 msへ戻すだけでも約41倍差が残る。

[tinylamb sum あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-sum-tinylamb.json)、[postgresql sum あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-sum-postgresql.json)、[tinylamb filter_1pct あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-filter_1pct-tinylamb.json)、[postgresql filter_1pct あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-filter_1pct-postgresql.json)。コスト: [無条件候補](../plan/implementation_rules.cpp#L949)。

### DISTINCTとQ06の比較

DISTINCTは両製品ともaccount_id索引の全走査を使うが、tinylambはCOUNTと同じ重複デコードに加えて16worker集計へ渡す。第1段階は索引／全走査の適切な選択、第2段階は可視行を持つキー単位のDISTINCT入力である。既存SkipScanDistinctの適用対象と、COUNT(DISTINCT)からの到達性を確認する。統計上のNDVを正答に使ったり、可視性を確かめず索引キー数を数えたりしない。

Q06はPostgreSQLがshipdate索引からBitmap Heap Scanへ進む。tinylambはParallelIndexScanで9,495行を評価し1,242行を通した後、上位でも述語を評価する。約76,855hit、miss 0。索引なしのtinylambは1.395 msなので、現在の28.968 msは選択経路の悪化である。bitmap／ページ順アクセスとPAX全走査を較正し、上位述語の重複除去は下位が全条件を適用済みと証明できる場合に限る。

[tinylamb distinct あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-distinct-tinylamb.json)、[postgresql distinct あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-distinct-postgresql.json)、[tinylamb Q06 あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/tpch-0.01-Q06-tinylamb.json)、[postgresql Q06 あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/tpch-0.01-Q06-postgresql.json)。

### 実装順と完了条件

1. キーが変わるまで非uniqueのデコード済みリストを保持する。read gate／snapshot／resolve_headの寿命を整理し、NULLキー、逆順、uniqueのNULLリスト、削除済みエントリ、prefix境界を含むiterator回帰を追加する。ページのraw参照を保持する方式は採らない。
2. IndexOnlyScan等にbatchと型付き値を渡す経路を用意し、COUNT／単純INT集計の低件数では16workerの受け渡しを避ける。まずデコード回数・bytes、可視性確認回数、worker待機を計測する。
3. デコード改善後の実測単価で、索引と全走査の候補を較正する。NDV／リスト長、選択率、coveringの有無、heap page局所性、予測入力行数とLIMITを費用へ入れる。固定の表名・クエリ名・スケールによる分岐は作らない。
4. DISTINCTのキー単位処理とQ06のbitmap候補は共通修正後の残差を測って追加する。

最初の性能基準は各表の境界。100万行のCOUNT <67.936 ms、SUM <106.040 ms、DISTINCT <235.263 ms、filter_1pct <2.571 ms、Q06 <8.618 msが目安になる。COUNT／SUM／DISTINCT／Q06は今ある全走査の実測でも届く余地があり、filter_1pctは高速な索引範囲集計が必要。非unique走査は行数NとNDVを独立に振り、デコード要素総数が同じリストの再読みによってN×重複数へ増えないことも合格条件とする。

## B Top Nの順序特性と早期停止

対象は索引あり1万／10万／100万行の3件で、131倍／1,093倍／7,966倍。SQLはamount DESC, id ASCである。PostgreSQLは逆順Index Only Scan→Incremental Sort→Limitを選ぶ。tinylambはIndexOnlyScan→TopNで、100万行を入力して100行を返す。

[tinylamb top_100 あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-top_100-tinylamb.json)、[postgresql top_100 あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-top_100-postgresql.json)。既に[sort_incremental規則](../plan/implementation_rules.cpp#L4038)と[IncrementalSortExecutor](../executor/incremental_sort.cpp#L55)はあるが、後者は最初にsourceをすべて読み切る。既存規則を選ばせるだけでは早期停止を実現しない。

計画: 先頭キーの順序だけを満たす子候補を探索に残し、混合ASC／DESCをprefixとsuffixに分ける。sort_incrementalの適用入口はLogical Sortなので、Logical TopNへ畳まれた後にも候補が残るかを確認する。IncrementalSortをpeer group単位の読み出しへ変え、Limitが不要になった次のgroupを読まないようにする。全入力を読まない費用をLIMIT候補にも反映する。(amount ASC, id ASC)の逆順をそのまま全体の正しい順序と見なしてはいけない。

完了条件: 3規模でそれぞれ<0.462／0.519／0.721 msを目指す。同時に、このfixtureではchildの消費行を先頭100行＋peer境界／明示したbatch先読み程度に抑え、100万行全走査をなくす。NULLS FIRST／LAST、同値キー、OFFSET、WITH TIES、collation、非常に大きいpeer group、sourceのエラー伝播を回帰させる。大きいgroupがメモリを超える場合のspill／TopK fallbackも定義する。

## C EXISTSの構築前フィルタと小さなキー集合

対象は索引なし1万／100万行、索引あり1万／10万／100万行の5件で11～50倍。PostgreSQLはcategory=7を先に適用し、Hash Semi Join／Hash Right Semi Joinへ渡す。索引ありではcategory索引のBitmap Heap Scanも使う。

tinylambの表示はSemiHashJoinだが、実測はcorrelated_index_builds=1、probes=10,000である。外側1行ごとにsalesを全走査するN+1問題ではない。100万行ではsales全体を2列分デコードし、relation_spills=1になってからフィルタする。[単一ソースの構築](../executor/detail/subquery_runtime.cpp#L907)はLoadSourceへscan_predicates=nullptrを渡すため、内側だけで決まるcategory条件を構築前に使えていない。

[tinylamb exists なし](../benchmark/comparison/results/2026-10-29/unindexed/query-details/micro-1000000-exists-tinylamb.json)、[postgresql exists なし](../benchmark/comparison/results/2026-10-29/unindexed/query-details/micro-1000000-exists-postgresql.json)、[tinylamb exists あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-exists-tinylamb.json)、[postgresql exists あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-exists-postgresql.json)。

計画: 相関等価条件と内側だけの述語を分け、category=7をscan／index accessまで押す。EXISTS専用のhash setへ必要なaccount_idだけをstreamし、同一キーの複数行を保持しない。既存の外側domainフィルタとキャッシュ所有者・serialの契約を保つ。相関を含む残余式はprobe後に評価し、IN／NOT INのNULL契約へこの最適化を無条件に広げない。

完了条件: 全5条件が表の境界未満。100万行では索引なし<84.749 ms、あり<34.433 ms。構築前に1%へ絞り、fixtureのdistinct keyは100個なので保持集合がsales全体へ膨らまず、64 MiBでこの単純EXISTSにspillが出ないことを確認する。NULL、重複、空の内側、全件一致／不一致、別のouter contextでのキャッシュ再利用をテストする。

## D 内部結合と集計のstream融合

対象は100万行join_groupの両構成で約29倍。PostgreSQLは小さいaccountsをhash buildし、salesの走査→join→部分集計をpipeline化する。tinylambも実測では既にbuild=10,000行、hash索引見積もり640,000 bytes、in_memory_hash_joins=1である。表示のHybridHashJoin build~100万行は推定であり、今の実測原因を「大きい側をbuildする」とはしない。

残る実測はspilled_probe_rows=1,000,000、relation_spills=2、aggregate_input_rows=1,000,000、grouped_join_fusions=0。[TryFusedGroupedJoin](../executor/detail/planning_heuristics.cpp#L2594)はどちらかの入力にspillがあるとfallbackする。scan／join／projectの各時間はネストした区間を含み得るため、内訳を単純加算しない。

[tinylamb join_group なし](../benchmark/comparison/results/2026-10-29/unindexed/query-details/micro-1000000-join_group-tinylamb.json)、[postgresql join_group なし](../benchmark/comparison/results/2026-10-29/unindexed/query-details/micro-1000000-join_group-postgresql.json)、[tinylamb join_group あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-join_group-tinylamb.json)。

計画: 1万行側だけを常駐buildし、spillしたprobeをForEachRow／batchで読める融合へ拡張する。join結果をRelationへ100万行書かず、必要列から100グループのworker-local集計器へ直接渡してmergeする。その後、scan自体からのstream供給も可能にしてprobeの事前materializeを減らす。aggregateの押し下げはjoin重複度・COUNT／SUM・NULLの等価性を証明できる場合だけ使う。

完了条件: 両構成で<302.282／305.504 ms。64 MiBを基準に、100万行のjoin出力materializeをなくすこと、融合が実際に発火すること、全件の行がちょうど一度集計されることをカウンタで確認する。16／64／256 MiB、skew、複合キー、重複キー、NULL、空入力を回帰する。256 MiBへの増量だけで完了とはしない。

## E 外部結合の片側述語と保持側の小さなhash build

対象は100万行left_joinの両構成で18～37倍。PostgreSQLはsales側のcategory<10を先に適用し、accountsをbuildするHash Right Join。tinylambはsales 100万行をbuildするHybridHashJoinで、category<10をjoin residualに残し、100万比較・spill 1回を実測している。

[tinylamb left_join なし](../benchmark/comparison/results/2026-10-29/unindexed/query-details/micro-1000000-left_join-tinylamb.json)、[postgresql left_join なし](../benchmark/comparison/results/2026-10-29/unindexed/query-details/micro-1000000-left_join-postgresql.json)、[tinylamb left_join あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-left_join-tinylamb.json)、[postgresql left_join あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-left_join-postgresql.json)。

計画: LEFT JOINのONにある右側だけの述語を右入力へ押し、100万行から10万行にしてからjoinする。次に小さい保持側accountsのhash build＋matched bitmapでprobeし、最後に未一致accountsのNULL拡張行を出す形を候補化する。category索引を使う右入力候補も残す。集計融合はDのstream基盤へ載せるが、ONとWHEREを交換したり、INNER JOIN同様に左右を無条件で入れ替えたりしない。

完了条件: 索引なし<323.744 ms、あり<158.376 ms。右入力のフィルタ通過数とhash build行数を実測し、保持側が消えないことを確認する。fixtureのjoin出力は109,000行（100,000一致行＋9,000未一致行）で、COUNT(s.id)とCOUNT(*)を混同しない。NULLキー、右側全不一致、重複、追加residualを回帰する。

## F 追加索引なしの非キー検索

対象は4／16接続のTPSとp50の4件で約20～23倍。追加二次索引なしという条件を維持し、既存の主キーを使って改善する。bucket,id索引を追加するだけでは対象条件の改善にならない。

元のOLTPでは当該SQLのEXPLAINを保存していないので、同じaccounts 1万行・bucket=7・LIMIT 10でリセット前後の[補足診断](../benchmark/comparison/results/2026-10-29-gap-plan/nonkey-diagnostic-final/plans.json)を採取した。PostgreSQLは主キーIndex Scan＋Filter＋Limitで、898行を除外して10行を返す。tinylambは投入直後のLimitから、UPDATEによるリセット後にParallelScan＋TopNへ変わる。

物理格納順の証明がUPDATEで失効するのは正しい。安全な主キー順と物理順を区別して、索引順＋残余フィルタ＋Limitの候補を探索へ残す必要がある。補足の単発実行は更新後tinylamb 0.481 ms、PostgreSQL 0.251 msで、20倍差自体は再現しない。元の3試行の並列差は安定しているが、plan形だけで原因を確定しない。

計画: 主キー順を使った早期停止候補の生成／ordering／LIMIT費用を確認する。無条件索引候補は[no_filterでゲート](../plan/implementation_rules.cpp#L949)されており、主キー以外のresidualがあるORDER BY／LIMITでも安全な索引順候補を生成する余地がある。1万行に対する4並列scanの起動・handoff・MVCC参照と、共有ワーカ待機を計測し、serial scan／ordered indexとの費用を較正する。1／4／16接続で同じ履歴を作り、queryごとのscan入力数とqueue時間を記録する。追加索引ありの結果へ条件を置き換えない。

完了条件: 4接続>5,025.5 query/s・p50<0.763850 ms、16接続>8,547.133 query/s・p50<1.823580 ms。補足p95／p99も改善する。順序が崩れるUPDATE／DELETE、古いsnapshot、空結果、低選択率、先頭に条件一致行が偏る／ない場合をテストする。

## G 同期commitとワーカ待機の切り分け

対象は索引なしupdate 16接続のTPS・p50、hot_update 1接続のTPS・p50、索引ありtransfer／insert_delete 16接続のp50、計6件。約11～30倍だが、短い試行で両製品とも遅い状態と速い状態を行き来している。

| 条件 | tinylambの3試行TPS | PostgreSQLの3試行TPS | 解釈上の注意 |
| --- | --- | --- | --- |
| なし update 16 | 1,943 / 1,938 / 17,008 | 7,555 / 21,750 / 63,994 | 中央値の差は大きいが定常速度は未確定 |
| なし hot_update 1 | 3,808 / 466 / 460 | 7,586 / 7,706 / 936 | 単一接続なので書き手同士の競合では説明できない |
| あり transfer 16 | 2,075 / 2,083 / 2,028 | 7,698 / 17,162 / 62,501 | p50差とTPS差は別に判定 |
| あり insert_delete 16 | 2,179 / 2,086 / 16,333 | 7,627 / 20,026 / 63,771 | 双方中断0、churn索引構成は両条件で同じ |

既存Loggerにはgroup commit、durable waiterによる即時wake、durable_lsn待機が既にある（[WaitForDurable](../recovery/logger.cpp#L719)、[worker](../recovery/logger.cpp#L1230)）。「group commit未実装」を前提にしない。8共有ワーカがcommit待機中に埋まる仮説、WAL同期自体の遅延、checkpointとのI/O競合、版／GCの履歴を分けて計測する必要がある。

実施順: まず10秒warmup＋60秒測定×5試行で製品順と条件順を変え、fresh instanceと同一履歴の両方を測る。秒ごとの成功・失敗、p50／p95／p99、fsync数／時間／bytes／commit件数、enqueue→SQL開始、SQL本体、commit enqueue→durable、durable→応答を記録する。既存transaction metricsを使い、待機をexclusiveな区間として追加する。

その結果で、group batchの集約・不要なwake、durability待ちによるworker占有、checkpoint帯域のどれを直すか決める。worker占有が支配する場合はdurable完了待ちの継続実行を検討するが、session順序、TransactionContext所有権、切断abort、終了drain、NoWaitによるpoolの進行保証を保つ。単にworker数を増やしてI/O待ちを隠す方式を最終解としない。

完了条件: 全6指標が表の境界を下回る／上回り、長時間負荷でも再現すること。hot_update単一接続>758.6 TPS、update 16接続>2,175 TPS／p50<2.671 ms、transferとinsert_deleteのp50はそれぞれ<2.860／2.729 msが目安。[同期commitと依存LSN](commit_durability.md)、[WAL-before-data](recovery_invariants.md)、[lock順序](lock_order.md)は維持し、durability barrier・I/Oエラー・crash/reopen・切断時の回帰を通す。

## H HTAPの分析側スループット

対象は両索引構成×4／16接続の4件で45.5～58.5倍。tinylambは全条件0.667 query/s、PostgreSQLは30.3～39.0 query/s。3秒窓ではtinylambの分析完了数が2件しかなく、境界をまたぐ要求を成功数から除くルールの影響も大きい。0.667の逆数をそのまま分析クエリの中央値遅延として扱わない。

SQLはSUM(s.amount)のjoin_groupとは異なりSUM(a.balance)であり、更新中のaccounts版を読む。[混合負荷SQLと測定窓](../benchmark/comparison/run.py#L364)を固定し、同じSQLの単独実行→3 writer＋1 reader→15 writer＋1 readerを比べる。D／Eの中間materialize改善と、Gのワーカ待機改善の効果を分けて測る。

計画: まずDの改善を適用し、同じ64 MiB予算で分析のspill・group fusion・所要時間を観測する。その後、60秒×5試行で分析完了数と全分析要求の遅延、境界外完了数、reader／writer別queue、内部scanタスク待ち、最古snapshotとversion chain／GCを採取する。共有pool／内部schedulerの調整はSQLの読み書き役割に応じた公平性を測って決める。

完了条件: 4／16接続、索引なしは>3.267／3.033 query/s、索引ありは>3.900／3.767 query/s。分析だけを優先してwriterの成功TPSを落とさず、残高保存を確認する。同じ境界処理を両製品へ適用し、試行の長さで倍率が変わる場合も併記する。

## 実施順序と共通の合格条件

| 順序 | 計画 | 依存関係と実施単位 |
| --- | --- | --- |
| 0 | 計測の補強 | G／Hの長時間再測定と、scan／decoder／queueの観測を先に設計する |
| 1 | A | iteratorのデコード保持→batch集計→較正。9件の共通要因を先に解く |
| 2 | B | 順序候補とstreaming incremental sortを別変更で検証する |
| 3 | C | 相関build前のlocal filter→EXISTSキー集合。5条件をまとめて検証 |
| 4 | D | spill probeを許す集計融合→scanからのstream供給 |
| 5 | E | ONの右側述語を先に押す→保持側build→Dの集計基盤へ接続 |
| 6 | F | 主キー順の早期停止と小規模scanの並列費用を較正 |
| 7 | G | 計測で支配要因を確定後、同期commitを保って改善 |
| 8 | H | DとG適用後の残差に対し、scheduler／MVCC／GCを調整 |

各変更は結果一致・回帰テスト・対象条件の再測定までを1単位にする。まず同じバイナリ／8 CPU／512 MiB／query 64 MiB／同期commitの条件で、修正前後を順序を入れ替えて測る。OLAPは初回を分離し、5回の反復を1ブロックとして複数ブロックを測定する。統計の乱数、プラン、spill、入力行数、batch同期数も残す。

最初の性能ゲートは対象35件の不利倍率10未満とし、PG自身を同時期に再測定した倍率でも確認する。旧PG値への到達だけで合格としない。他のクエリ・更新・メモリ上限・中断率を悪化させて得た改善は採用しない。p95／p99の補足6件も同時に比較する。SQL／型／NULL／順序／snapshotの正しさを先に守る。

変更層の回帰、全ctest、expression differential、layering、clang-formatを実行する。optimizer／executor／expressionに触る場合はSQL oracle fuzzも実施する。新しい正しさの修正は最小再現を回帰へ残し、実行計画が速そうという理由だけで完了を付けない。

## 補足 p95とp99の差

以下の6件も3試行の分位点の中央値で10倍以上。主対象のp50とは区別し、FとGの受け入れ条件へ加える。

| 計画 | 追加索引 | 負荷 | 規模 | 指標 | tinylamb | PostgreSQL | 不利倍率 | 10倍差解消の境界 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| F | なし | nonkey_read | 4接続 | p95_ms | 2.472621 | 0.095593 | 25.87× | <0.955929 |
| F | なし | nonkey_read | 4接続 | p99_ms | 2.955693 | 0.125048 | 23.64× | <1.250480 |
| F | なし | nonkey_read | 16接続 | p95_ms | 5.440558 | 0.233786 | 23.27× | <2.337859 |
| F | なし | nonkey_read | 16接続 | p99_ms | 6.068868 | 0.298851 | 20.31× | <2.988509 |
| G | なし | hot_update | 1接続 | p95_ms | 2.279552 | 0.153813 | 14.82× | <1.538130 |
| G | なし | hot_update | 1接続 | p99_ms | 2.835964 | 0.192818 | 14.71× | <1.928180 |

## 再現と保存物

抽出コードは[gaps.py](../benchmark/comparison/gaps.py)、方向・10倍境界・中央値と平均の違い・未検証結果・重複試行・HTAP役割を検査する[gaps_test.py](../benchmark/comparison/gaps_test.py)を追加した。既存分を含む17テストが通過した。今回の作業ではDB実装を変更していない。

```bash
python3 benchmark/comparison/gaps.py benchmark/comparison/results/2026-10-29 \
  --output /var/tmp/tinylamb-pg-gap-new
python3 -m unittest discover -s benchmark/comparison -p '*_test.py' -v
```

全416比較と抽出41件は[gaps.json](../benchmark/comparison/results/2026-10-29-gap-plan/gaps.json)。補足検索診断の[環境](../benchmark/comparison/results/2026-10-29-gap-plan/nonkey-diagnostic-final/environment.json)・[実行計画と結果](../benchmark/comparison/results/2026-10-29-gap-plan/nonkey-diagnostic-final/plans.json)も保存した。補足は当時の長い更新履歴と並列負荷を再現する測定ではなく、計画候補を確かめるための単発診断である。
