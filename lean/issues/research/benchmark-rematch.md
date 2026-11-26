# ベンチマークの敗北に対応する94件の未証明命題

[全94件の対応表](benchmark-rematch-ledger.md)は、各行から Lean の提出型に移れる。速度は「クエリ・対戦相手」、保存サイズは「データセット・保存形式・対戦相手」を1敗とした。同じクエリで両者に負けた場合は、その SQL に使える別々の手段を割り当てた。各行に tinylamb の実測、相手の実測、逆転に必要な差、次回採るカウンタを載せた。

## 速度の85敗

2026年11月25日の waves 1–13 適用後の保存結果を使った。対象は TPC-H SF10 の22問、TPC-DS SF10 の99問、ClickBench 全量の43問、派生分析 SF10 の26問、クラウド・BI の tinylamb 測定あり23ケース。クラウド・BI のケースは SELECT、EXPLAIN、混合処理を元の測定単位で扱う。異なるスケール・スレッド数や以前の最適化ラウンドの値を合算すると同一クエリの勝敗が変わるため、表はこの比較系列に固定した。

| 系列 | 相手のどちらかに負けたクエリ | 対 DuckDB | 対 ClickHouse |
|---|---:|---:|---:|
| TPC-H | 5 | 5 | 0 |
| TPC-DS | 35 | 35 | 2 |
| ClickBench | 17 | 13 | 10 |
| JOB 派生 | 1 | 1 | 1 |
| STATS-CEB 派生 | 2 | 2 | 1 |
| H2O 派生 | 3 | 3 | 0 |
| LDBC SNB 派生 | 3 | 3 | 1 |
| BigBench 派生 | 3 | 3 | 2 |
| Public BI | 1 | 1 | 1 |
| Iceberg SQL | 1 | 1 | 0 |
| CostBench 派生・PBench 派生 | 0 | 0 | 0 |
| **合計** | **71** | **67** | **18** |

タイムは保存された実行サンプルの中央値（通常3回、混合処理は操作別のサンプル列）。tinylamb の中央値が相手を厳密に上回り、結果照合が通った組を採用した。派生分析の照合は行の多重集合とハーネスの数値許容差を使う。TPC-DS Q44 は tinylamb と DuckDB の検証欄が結果不一致なので除外した。Iceberg native の3ケースには tinylamb の測定がない。公開 CostBench の料金比較はローカルの同条件対戦とは別系列。僅差も載せており、統計的な有意差は判定していない。次回は同じ入力・スレッド数・検証方式で相手も測り、正解を保ったうえで中央値が相手を下回るかを判定する。

速度の入力は作業ツリーの以下のファイル。表には測定値そのものも保存した。

- [TPC-H](../../../build/benchmarks/three-db-sf10-20261122/after-waves1-13-20261125/tpch/results.jsonl)、[TPC-DS](../../../build/benchmarks/three-db-sf10-20261122/after-waves1-13-20261125/tpcds/results.jsonl)、[ClickBench](../../../build/benchmarks/three-db-sf10-20261122/after-waves1-13-20261125/clickbench/results.jsonl)
- [派生分析5種の SQL・時間・結果照合](../../../build/benchmarks/analytics-sf10-waves1-13/results.jsonl)
- [クラウド・BI のケース・モード・サンプル](../../../build/benchmarks/cloud-bi-waves1-13/measurements.json)

## 次の計測で切り分けたい差

- **ClickBench Q37** は tinylamb 87.763 ms、DuckDB 18.376 ms、ClickHouse 18.775 ms。同じ上位10 URL でも、対 DuckDB の命題は未走査ページから増え得る件数の上界を使い、既に10候補に届かない URL を落とせる条件を問う。対 ClickHouse の命題はページごとに同じ文字列を指す辞書コードが違っても、通過 RID の局所カウントから最終件数を復元できるかを問う。保持したグループ数と文字列を復元した回数を別々に測れる。[ClickBench の提出型](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean)
- **JOB J3** は tinylamb 23.070 ms、DuckDB 8.885 ms、ClickHouse 21.608 ms。役柄条件を通る出演は999,750行あるが、人物 ID は1～20,000に収まる。対 DuckDB ではこの範囲だけで女性かを引けるビット表、対 ClickHouse では女性・役柄条件を通った出演を映画別に束ねてから制作年を引く案を登録した。後者では出演の多重度が COUNT と AVG の両方に効く。[J3 の分布調査](restart-races.md)・[派生分析の提出型](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean)
- **Public BI MLB Q108** は絞り込み後に同じ4列の最大と最小を取る。対 DuckDB では3条件の RID 集合からグループが存在するかを決める。対 ClickHouse ではページが全件条件を満たす場合に4列の極値を再利用する。空グループの出力有無と、行はあるが全値 NULL の結果を区別する。[クラウド・BI の提出型](../../TinylambOpt/ResearchGoals/QueryRewrites/FileRangesAndExtrema.lean)

各命題は候補演算の正しさを検証する提出型。SQL 全体に適用するときは結合の多重度、NULL、照合順、同順位、数値精度を保つ必要がある。たとえば順位キーが等しい場合の決定規則を含む局所 Top-K、結合キーの一意性を確認した場合の単一行集約を扱う。Lean の整数和は正確な算術であり、FLOAT の再結合を採用する経路には別途、対象型での丸めの検証が要る。カウンタは候補が効いたかを診断するためのもので、時間の逆転は次回の実行で測る。

## 保存サイズの8敗とオフライン試作の1敗

TPC-H・TPC-DS・ClickBench は、双方で行数を照合した保存済み fixture のバイト数を使った。tinylamb は TLCS・DB・ブロック、DuckDB は DB ファイル、ClickHouse はユーザーテーブルの active part の全ファイル。元の測定条件に従い、この3データセットは WAL を除外している。フレーム化した既存の 1 MiB 試作はこの3種で DuckDB より小さく、ClickHouse に負けたのは ClickBench のみ。試作からさらに **703,816,662 B** 削れば初めて逆転する。URL と OriginalURL の共通接頭辞案は、この差をフレーム索引・再圧縮込みでどこまで埋めるかを測る。[DuckDB との全量比較](compression.md)・[ClickHouse との全量比較](clickbench-layout.md)

派生分析では5種が1つの DB に同居する。36表・18,223,012行を対象に、tinylamb は保存セットのコピーを再オープン、DuckDB 2.0.0 は読取専用で開き、各表の行数を manifest と照合した。ClickHouse も各表の非重複 part の行数を照合した。tinylamb は DB・blocks・再起動に必要な WAL とメタデータを含む **1,082,859,325 B**。DuckDB は **190,328,832 B**、ClickHouse の対象 part は **324,908,462 B**。共有カタログや WAL の容量を5種に配賦していない。クラウド・BI の速度結果には同条件で比較できる保存サイズの記録がない。[データセット manifest](../../../build/benchmarks/analytics-sf10-waves1-13/dataset/manifest.json)

この派生分析 DB では **blocks だけ**を1 MiBごとに Zstandard レベル6で圧縮する実験も行った。全1,017フレームで復号したバイトが元と一致した。32 B のファイルヘッダとフレームごとの17 B の索引を課金し、カタログと WAL をそのまま足すと **270,530,617 B**。同じ集計で ClickHouse を **54,377,845 B** 下回る。各フレームで元配列と4・8・16列のバイト転置から短い形式を選ぶと **209,099,213 B**。DuckDB に勝つにはここからもう **18,770,382 B** 要る。いずれもメモリ上で生成した圧縮本体長に索引費用を足した試算で、エンジンでの再起動や読取速度は今後の検証項目。[保存形式の提出型](../../TinylambOpt/ResearchGoals/QueryRewrites/ColumnCodecs.lean)

## 再現と監査

以下をリポジトリ直下で実行する。監査は保存された全測定から厳密な敗北を引き直し、Lean 宣言と対応表の欠落・重複・余剰を検出する。対応表の再生成は `--write`、派生分析の全フレームを再圧縮・復号する実験は `--probe-analytics`（Python 3.14以降）。[監査と実験のスクリプト](fixtures/benchmark_rematch.py)

```sh
python3 lean/issues/research/fixtures/benchmark_rematch.py
python3 lean/issues/research/fixtures/benchmark_rematch.py --probe-analytics
(cd lean && lake build CheckProofs)
```
