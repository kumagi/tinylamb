# DuckDBを追加した5製品比較（2026-11-05）

DuckDB 2.0.0を既存比較器へ追加し、Tinylamb C8・PostgreSQL・MySQL・Oracle Freeと
同じ入力／SQLで再測定した。OLAP 395測定は全行・値・順序がPGと一致し、
OLTP／HTAP 495試行はすべて残高・行数・読み取り検査を通過した。
測定条件の欠落・重複も監査した。前回のOracle結果を流用した表ではない。

測定終了後、容量取得の`duckdb_database_size()`がDuckDB 2.0で存在せず、最終メタデータ保存だけが失敗した。
全395 OLAP・495 OLTPの記録はその前に書き込み済みで、欠落・重複・結果・反復数・fixtureを独立監査した。
元のenvironment／results／終了ログを改変せず、そのSHA-256付き[回復記録](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/completion-recovery.json)を添えた。
終了時のOracle SGA／segment容量、DuckDBファイル容量は未取得として扱い、別実行の値を流用していない。
実行時のソースはそのまま保存し、現行比較器は`PRAGMA database_size`へ修正。
修正後の5製品・100行の別スモーク実行で、最終保存と後始末まで確認した。性能表には混ぜていない。

## 測定条件

- PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2)、MySQL 8.4.13、Oracle AI Database 26ai Free Release 23.26.3.0.0 - Develop, Learn, and Run for Free / Version 23.26.3.0.0。
  Tinyは前回の改善済みC8 binaryを固定し、今回の追加でエンジン本体は変更していない。
- 全サーバーをCPU 0,1へ固定し、負荷は製品ごとに順次実行。
  DuckDBのサーバーthreads=2、クライアント側は各1スレッド。
- sales=[10000, 100000, 1000000]、accounts最大10,000行。共通二次索引キーあり。
  DuckDBはART＋zone map・固有の圧縮を使うため、B-treeや物理配置を同一にした比較ではない。
- Tiny／PGのデータバッファ1,088 MiB、MySQL実測1,536 MiB。
  Oracle実Buffer Cache開始1,088 MiB／終了未取得。
  DuckDB memory_limit指定1,088 MiB（設定表示1.0 GiB）。その適用範囲は他製品のデータバッファと異なり、
  総RSS上限でもない。OracleのSGA／PGAや他製品のクエリ予算も同一ではない。
- OLAPは最初の実行を別記し、その後9反復の中央値。cold I/O性能ではない。
  SQL送信から全結果取得・Python型変換までを含む。
  整数・NULL・文字列・行順を厳密比較し、浮動小数点は相対1e-7／絶対0.01の許容誤差で照合する。
- OLTPは[1, 4, 16]接続、warmup 0.5秒＋計測2.0秒、
  3試行。表は試行中央値、全表にはmin/max・p95/p99・失敗率も残す。
  初回比較の短窓であり、僅差・tail latencyや以前のPG比1.5倍目標の達成判定には使わない。
- TPC-H由来22問はSF=0.01・同一DBGEN入力・binary64。
  TPCの監査を受けたスコアではない。TPC-Cの既存クライアントはPG Simple Query専用で、
  DuckDB／OracleのTPC-C、および今回のDuckDB SF=1は未測定。
- 投入時だけOracleはarray DML、他製品は500行単位のINSERTを使う。
  ロード時間を同じ投入手法のエンジン比較として順位付けしない。
- ビルド・テスト・プロファイラを本測定と重ねていない。ホスト上の通常アプリは停止していない。

## DuckDBの接続と更新競合

公式[Quack](https://duckdb.org/docs/2.0/quack/overview)を使い、localhost HTTP上で
`ATTACH`＋`CONNECT`によりセッション全体をサーバーへ送る。
永続ファイルを開くのは専用サーバープロセスだけで、クライアントのローカル最適化への
部分pushdownを使わない。公式Python clientとQuackの通信・型変換費用も含むため、
組み込みAPIを直接呼ぶDuckDBの速度はこの数値から断定できない。
PG/TinyはSimple Query、MySQLはCOM_QUERY、OracleはThin＋非キャッシュstatement。
OracleのみDockerのlocalhostポート転送が入り、他はホスト内の直接TCPである。

DuckDBは[snapshot isolationと楽観的並行制御](https://duckdb.org/docs/2.0/connect/concurrency)を使う。
PG／MySQL／OracleのREAD COMMITTEDと同一ではなく、同一行の同時更新では競合エラーが起きる。
自動再試行は追加していない。成功したトランザクションだけをTPSへ数え、失敗は別に残す。
暗黙transactionがabort済みのときだけ、後始末ROLLBACKの「transactionなし」を無害として扱う。
元の更新エラーは隠さず計上する。

DuckDBはin-memory DBを使わず、永続ファイルと標準WAL設定を使う。
DuckDBの[耐久化方式](https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions)はcommit時WALとfsync。
PGはfsync／synchronous_commit有効、MySQLはflush_log_at_trx_commit=1・binlog無効、
Tinyは同期commit、OracleはWAIT／IMMEDIATE・FORCE LOGGING・NOARCHIVELOG。
今回の比較では電源断試験を追加していない。

## 読み取り結果

100万行での抜粋。時間は小さいほど速い。

| 処理 | tinylamb ms | PostgreSQL ms | MySQL ms | Oracle Free ms | DuckDB ms |
| --- | --- | --- | --- | --- | --- |
| count | 6.877 | 13.191 | 115.119 | 2.655 | 0.640 |
| sum | 8.173 | 20.007 | 149.638 | 28.632 | 1.376 |
| group_100 | 19.012 | 20.370 | 146.231 | 54.031 | 2.687 |
| group_10000 | 31.992 | 47.371 | 678.267 | 148.884 | 7.198 |
| distinct | 4.030 | 22.622 | 4.786 | 20.613 | 2.744 |
| top_100 | 0.054 | 0.041 | 188.036 | 26.017 | 2.184 |
| join_group | 12.411 | 59.250 | 705.735 | 31.842 | 3.645 |
| exists | 2.302 | 3.435 | 7.941 | 7.836 | 2.187 |
| window | 5.448 | 13.413 | 142.201 | 6.026 | 1.592 |

DuckDBは100万行の19種目中、Tinyより16種目、PGより17種目で速かった。
TinyがDuckDBへ大きく負けた側の時間比は次のとおり。

| 処理 | Tiny / DuckDB 時間比 |
| --- | --- |
| scalar_subquery | 23.28 |
| expression | 14.44 |
| count | 10.75 |
| left_join | 8.98 |
| join | 8.85 |
| group_100 | 7.07 |

これらは共通SQLを送信して全結果を受け取るまでの差であり、個々の実行器の原因を
profileした結果ではない。走査・集約と短い点検索を分けて評価する必要がある。

TPC-H由来22問の各中央値の合計は次のとおり。合計そのものを連続実行して計測した時間ではない。

| 製品 | 22問の中央値合計 ms |
| --- | --- |
| tinylamb | 75.008 |
| PostgreSQL | 68.128 |
| MySQL | 261.387 |
| Oracle Free | 60.083 |
| DuckDB | 52.174 |

DuckDBが速い問はTiny比7/22、PG比8/22。
全22問と全3規模のmicro表は下記の生成表に保存した。

![100万行OLAP](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/analysis/micro_1m.png)

![TPC-H由来22問](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/analysis/tpch.png)

## トランザクション結果

成功TPSの抜粋。失敗した要求を成功数へ含めないため、次の失敗件数と併せて読む。

| 処理 | 接続 | tinylamb 成功TPS | PostgreSQL 成功TPS | MySQL 成功TPS | Oracle Free 成功TPS | DuckDB 成功TPS |
| --- | --- | --- | --- | --- | --- | --- |
| noop | 1 | 141,213.0 | 104,614.0 | 72,969.0 | 37,599.0 | 1,901.5 |
| noop | 16 | 373,755.5 | 222,939.0 | 229,244.0 | 173,413.0 | 5,768.0 |
| point_read | 1 | 65,937.0 | 63,425.5 | 55,460.0 | 2,767.5 | 1,357.5 |
| point_read | 16 | 141,306.0 | 127,929.0 | 162,975.5 | 5,437.0 | 3,640.0 |
| range_read | 1 | 33,240.0 | 28,663.0 | 34,415.5 | 2,480.5 | 1,274.5 |
| range_read | 16 | 74,315.0 | 69,794.0 | 90,636.5 | 5,255.5 | 3,219.5 |
| update | 1 | 6,753.0 | 7,286.0 | 6,870.0 | 1,925.0 | 962.5 |
| update | 16 | 9,098.5 | 7,842.5 | 7,918.0 | 4,507.0 | 428.0 |
| hot_update | 1 | 997.0 | 1,007.0 | 977.5 | 966.5 | 337.5 |
| hot_update | 16 | 9,381.0 | 4,870.0 | 7,906.0 | 7,733.5 | 436.5 |
| transfer | 1 | 924.5 | 953.0 | 880.0 | 560.5 | 212.0 |
| transfer | 16 | 7,941.0 | 8,008.0 | 8,064.0 | 2,374.5 | 333.0 |
| rollback | 1 | 19,147.5 | 31,559.0 | 786.0 | 654.5 | 584.5 |
| rollback | 16 | 43,995.0 | 61,545.5 | 7,516.5 | 3,879.0 | 1,558.5 |

`SELECT 1`だけの1接続p50も、Tiny 0.0065 ms、PG 0.0092 ms、
DuckDB 0.5159 msと差がある。短いSQLの比較には通信・クライアント・実行準備の
固定費も含まれる。今回の数値をエンジン内部の演算時間だけと解釈しない。

| 製品 | 計測中SQL失敗 | warmup失敗 | 失敗を含む試行 |
| --- | --- | --- | --- |
| tinylamb | 1107 | 291 | 27 |
| PostgreSQL | 0 | 0 | 0 |
| MySQL | 0 | 0 | 0 |
| Oracle Free | 0 | 0 | 0 |
| DuckDB | 317 | 84 | 24 |

失敗を含む試行でも、全件の事後不変条件は通過した。整合性の成功とSQL失敗ゼロは
別の項目である。`failures.json`はwarmup中を含むSQL失敗のある試行も収録するため、
SQL失敗を含む試行をそのまま「結果不一致」と解釈しない。今回の終了コード1の直接原因は、上述の終了時容量メタデータ取得である。
HTAPは1接続でreaderのみ、4／16接続では最後の1接続がreaderとなる。

![OLTP scaling](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/analysis/oltp_scaling.png)

## 保存先と再現

- [生データ・実測設定・監査](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/)、[監査結果](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/audit.json)、
  [全測定CSV](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/summary.csv)、[初回実行・全反復・SQL失敗](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/results.jsonl)。
- [1万行](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/analysis/micro_10000.md)、[10万行](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/analysis/micro_100000.md)、
  [100万行](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/analysis/micro_1000000.md)、[TPC-H全22問](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/analysis/tpch.md)。
- [全OLTP TPS・min/max・失敗率](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/analysis/oltp_tps.md)、
  [p95/p99](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/analysis/oltp_latency.md)、[HTAP reader/writer別](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/analysis/htap.md)。
- [実行時の比較器](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/runtime-harness/)、[完全な実行引数](../benchmark/comparison/results/2026-11-05-duckdb-5engine-indexed/environment.json)。
  DuckDB 2.0.0、Quack拡張・エンジンbinaryのSHA-256、Docker image digest、
  frozen Tiny C8 serverのSHA-256、追跡差分を保存。
  実行方法は[比較器README](../benchmark/comparison/README.md)。

事前検証は別ディレクトリ`2026-11-05-duckdb-preflight3`に分離し、本表へ混ぜていない。
初期の起動失敗2回も採用していない。Quackを親プロセスへ先にLOADするとMySQL Cドライバの
static TLS確保が失敗する問題を再現し、拡張の準備を別プロセスへ分離した。
この読み込み順・容量取得・集計監査も含め比較器の51回帰テストが通過した。エンジン本体のコード変更はない。
終了時に比較用サーバー・コンテナ・一時DBを削除し、公式imageと拡張cacheは再測定用に残す。
