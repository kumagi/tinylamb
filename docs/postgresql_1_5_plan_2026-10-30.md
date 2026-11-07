# PG比1.5倍以上の条件を順に改善（2026-10-30）

今回同条件で測定したPGを基準に、時間は1.5倍未満、成功TPSはPGの2/3超を目標とする。
単体SF=1とTCP比較を混同せず、SQL結果・分布・同期・索引・接続数をそろえる。

- [x] 既存456比較から34対象を倍率順で固定する。
- [x] TPC-H SF=1を同じDBGENデータ・SQL・共通索引・TCPでPGと比較する（旧Q16の誤答は性能対象外として修正）。
- [x] TPC-Cを既存C++生成器・同じTCPクライアントでPGと比較できるようにする。
- [ ] 全対象の原因を確認し、倍率の大きい順に実装・回帰検証・両製品再測定する。
- [ ] 全種目の退行確認とBenchmarkHistoryへの追記を行う。

2026-10-31時点: 同条件SF=1の変更前はQ02／Q09／Q04／Q10／Q11／Q19／Q20が1.5倍以上。
TPC-Cは全体TPSだけでは隠れるOrder-Status／Delivery／Stock-Levelの遅延が対象。
生データと各指標は`benchmark/comparison/results/2026-10-30-pg15/*-targets.json`に保存。
C4の途中までの再測定ではQ02は277.86→14.88 ms、同時PG 101.57 ms。
Q16は並列走査のコンパイル済みNOT IN等のフィルタ欠落を修正し、18,331グループの値がPGと一致した。
C4測定は追加指示に伴いQ20までで中断されたため、全22問完了とは扱わず、残りと退行確認は未完了。

Oracle Free追加は別の2コア枠で完了した。[4製品レポート](oracle_free_comparison_2026-10-31.md)。PG比1.5倍の判定に従来8／16コアと混ぜない。

C5では、無関係なキーの変更で最新注文の検索が全走査に落ちる問題を、予算付きのキー履歴で軽減した。
C6は短いIN範囲の不要な先読み、C7はページ再配置中の行別メモリ確保を削減した。
C7のTPC-C 30秒×3試行は両製品とも整合性を通過したが、PG比1.5倍の目標は未達。
Order-Status p95はC4の38.3791 msからC7の1.72515 msへ短縮し、今回PG .260948 ms比6.611倍。
Delivery p50は33.0719 ms／PG 3.36056 msで9.841倍、Stock-Level p50は1.78682 ms／PG .724392 msで2.467倍。
時期が異なる測定間の短縮率と、同時期PGとの倍率を区別する。
原データは`2026-10-31-pg15-tpcc-c7`、全20指標は`2026-10-30-pg15/tpcc-c7-targets.json`。
C7でSF=1全22問も再測定し、44/44の全値一致・反復安定を確認した。
今回の未達はQ09 2.738倍、Q11 2.246倍、Q10 2.218倍、Q04 2.191倍、Q20 1.578倍、Q19 1.504倍。
Q02は12.616 ms／PG 92.791 ms、Q16は85.357 ms／PG 83.004 msで通過。
全22問の中央値合計はTiny 5,544.945 ms／PG 5,784.351 msだが、問別目標の代わりにはしない。
全4,192回帰テスト（失敗0、既存skip 3）とSQL oracle 601秒788入力（不一致・クラッシュ0）を通過した。
全種目の最終再測定と、Delivery等の残る原因調査は継続中。

## C8の更新退行修正

単一接続Deliveryが同じ注文で繰り返し失敗する問題は、UPDATEの行移動で古い末尾ヒントを更新せず、
実際の末尾との予約条件が一致しなくなることが原因だった。INSERTと同じowner-visible末尾カーソルから
移動先を探し、辿った公開済みリンクを使ってヒントを更新する。競合する未公開tail・undo・同期の契約は保つ。
元の行からテーブル後半を毎回歩く費用も避ける。小さな65行の再起動回帰は旧実装で予約競合を再現し、
C8はcommit／abort・旧snapshot・再起動で通過した。全4,194テスト失敗0・既存skip 3。
C8は同条件30秒×3試行で両製品を再測定し、全6試行valid。
Delivery p50は33.0719→8.85851 ms、成功TPSは23.4667→65.3に改善した。
今回PG比はp50 2.744倍／p95 2.547倍／p99 2.323倍で、目標はまだ未達。
最大の未達はOrder-Status p95 6.522倍、Stock-Level p99 4.836倍。
TPC-C全20指標のうち9遅延指標が残り、`tpcc-c8-targets.json`に全件を保存した。
SQL oracleも602秒776入力、657新規入力、不一致・クラッシュ0で通過した。

Order-Statusの次の候補は、純粋な列射影で汎用のCSE／rewrite／bytecodeコンパイルを省くこと。
診断用C8の命令数ではProjection構築が25.39%、cache replay全体が53.35%を占めた。
数値は時間・TPSではなく、単一接続・type2だけのCallgrind診断。列順・重複・タグ・位置・fallbackの意味を保つ。

## 共通TPC測定を加えた優先順位

|優先|負荷|変更前の同条件PG比|次に確認する内容|
|---:|---|---:|---|
|1|TPC-C Order-Status|p95 84.16、p99 79.80、p50 6.24|C4を同じv2 fixture・凍結クライアントで再測定し、顧客／最新注文／明細のSQLごとの計画と混合時待機を切り分ける|
|2|TPC-C Delivery|p99 21.97、p95 17.16、p50 10.41、成功TPS 2.09|10 districtを含む各SQLと中断・undo・同期待機を計測する。成功時tailだけで中断率を隠さない|
|3|TPC-C Stock-Level|p99 4.81、p95 4.37、p50 2.76|明細の複合範囲とstockの複合キー＋INの訪問件数を確認する|
|4|TPC-H SF=1 Q02|2.97|C4部分測定では0.147に改善。残り2問を含むフル再測定と退行確認を完了する|
|5|TPC-H SF=1 Q09／Q04／Q10／Q11|2.73／2.34／2.31／2.14|保存済み実行カウンタのscan／join／projection費用を分離して改善する|
|6|既存34指標とSF=1 Q19／Q20|一覧参照／1.73／1.69|同一CPU・索引・反復・PG状態を維持して再測定する|

TPC-Cの3試行は全体TPSがTiny 1,546–4,450、PG 1,333–9,298と変動した。
中央値だけから安定した優劣とは扱わない。上記の遅延倍率と競合中断数も保存し、
改善後は両製品を再測定する。既存の同期commit／可視性・GCの契約は維持する。

|順|構成|負荷|規模/接続|指標|変更前PG比|
|---:|---|---|---:|---|---:|
|1|indexed|Q02|0.01|median_ms|1.938|
|2|indexed|rollback|1|p50_ms|1.928|
|3|indexed|Q20|0.01|median_ms|1.887|
|4|indexed|noop|16|p95_ms|1.845|
|5|unindexed|noop|16|p95_ms|1.784|
|6|indexed|exists|10000|median_ms|1.759|
|7|indexed|noop|16|p99_ms|1.751|
|8|indexed|noop|16|p50_ms|1.749|
|9|indexed|Q19|0.01|median_ms|1.748|
|10|indexed|noop|16|tps|1.747|
|11|unindexed|rollback|1|p95_ms|1.688|
|12|indexed|rollback|1|tps|1.682|
|13|unindexed|rollback|4|p50_ms|1.677|
|14|indexed|Q14|0.01|median_ms|1.667|
|15|indexed|rollback|1|p95_ms|1.659|
|16|unindexed|rollback|4|tps|1.653|
|17|indexed|rollback|1|p99_ms|1.652|
|18|unindexed|noop|16|tps|1.639|
|19|indexed|rollback|4|p50_ms|1.627|
|20|unindexed|noop|16|p50_ms|1.626|
|21|indexed|union_all|1000000|median_ms|1.613|
|22|unindexed|rollback|4|p99_ms|1.605|
|23|indexed|transfer|1|p99_ms|1.582|
|24|unindexed|rollback|4|p95_ms|1.581|
|25|indexed|rollback|4|tps|1.579|
|26|unindexed|rollback|1|tps|1.561|
|27|indexed|filter_10pct|10000|median_ms|1.547|
|28|indexed|top_100|100000|median_ms|1.535|
|29|unindexed|rollback|1|p99_ms|1.530|
|30|indexed|Q13|0.01|median_ms|1.526|
|31|unindexed|noop|16|p99_ms|1.518|
|32|unindexed|rollback|16|p50_ms|1.508|
|33|unindexed|Q13|0.01|median_ms|1.506|
|34|unindexed|rollback|1|p50_ms|1.506|
