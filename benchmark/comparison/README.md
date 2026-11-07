# tinylamb PostgreSQL MySQL Oracle Free DuckDB 比較ベンチマーク

共通SQLをローカルTCP経由で実行し、OLAP、OLTP、読み書き混在の性能と結果の正しさを比較する。データベース本体の変更は不要。測定値はクライアントでのSQL送信から全結果の取得までを含む。

## TPC-Cの3製品比較

[2026-11-06の結果](../../docs/tpcc_three_comparison_2026-11-06.md)は、DuckDB／Oracleを除く
Tinylamb C8・PostgreSQL・MySQLのW=1比較。10接続、warmup5秒＋30秒、3試行。
SFはTPC-Hの指定で、TPC-Cは倉庫数Wを使う。

MySQLの開発ヘッダーとC client library（例: `libmysqlclient-dev`）が必要。
既定のビルドへこの依存を追加せず、必要な場合だけ有効化する。
Pythonは引用された空文字とNULLを区別するCSV読み込みのため3.12以上が必要。

```bash
cmake -S . -B build -DTINYLAMB_TPCC_MYSQL=ON
ninja -C build tinylamb_tpcc_three_benchmark
python benchmark/comparison/tpcc_three.py \
  --output benchmark/comparison/results/my-tpcc-three-run \
  --server build/benchmarks/pg15-bin/server-c8 \
  --client build/tinylamb_tpcc_three_benchmark \
  --fixture build/benchmarks/pg15-bin/tpcc-v2-gold \
  --seconds 30 --warmup 5 --trials 3 --clients 10
```

server／fixtureの例は今回の保存済みC8 binaryとW=1 snapshotを指す。
別の環境では対応する初期化済みW=1 fixtureと互換serverを指定する。
MySQLヘッダーを通常と別の場所へ展開した場合、`TINYLAMB_MYSQL_INCLUDE_DIR`と
`TINYLAMB_MYSQL_LIBRARY`をCMakeへ指定できる。
`tinylamb_tpcc_three_benchmark`は共通SQL生成器を外部接続用にコンパイルし、
DB本体のライブラリをリンクしない。旧PG専用ターゲットも維持する。

3製品で同じC++生成器を使い、MySQLはC APIのCOM_QUERY、Tiny／PGはSimple Query。
初期データを9表すべて全行照合し、各試行前に復元する。文字列照合順はMySQL utf8mb4_bin。
成功TPS・engine abort・意図したrollback・成功処理のp50/p95/p99を別々に保存する。
現在の生成器にはSQLをまとめる箇所があるため、過去の凍結clientの数値とは混ぜない。
TPC認証スコアではなく、think time／keying timeは省略している。

`--cases tinylamb:2 postgresql:2`で、同じ番号の条件だけを再測定できる。
元の記録は上書きせず、ビルド混入を監視で検出した条件のみ置換記録を作って採用する。
レポート生成は`tpcc_three_report.py <結果dir> --output <保存先.md>`。
他のbuild／test／profiler／比較ベンチマークの終了を待ち、計測中の1秒間隔監視ログも残す。
最終採用は`2026-11-06-tpcc-three-quiet`と、そのtrial3の2条件の再測定。旧セットは参考保存する。

## DuckDBの追加枠

`--include-duckdb`でDuckDB 2.0.0を追加する。`--include-oracle`と独立して指定でき、
両方指定すると5製品になる。既定の3製品は維持する。

```bash
python -m pip install -r benchmark/comparison/requirements-duckdb.txt
# Oracleも測る場合は requirements-oracle.txt もインストールする。
python benchmark/comparison/run.py \
  --output benchmark/comparison/results/my-duckdb-run \
  --scratch "$PWD/build/benchmarks" --include-duckdb --include-oracle \
  --server-cpus 0,1 --read-workers 2 --data-cache-mib 1088 \
  --duckdb-threads 2 --duckdb-memory-mib 1088 --secondary-indexes \
  --sizes 10000 100000 1000000 --clients 1 4 16 \
  --repeats 9 --warmup 1 --seconds 5 --trials 3 --tpch-scale .01 \
  --htap-reader-baseline
```

公式[Quackプロトコル](https://duckdb.org/docs/2.0/quack/overview)の
`ATTACH`＋`CONNECT`で、SQLセッション全体をlocalhostの専用DuckDBサーバーへ送る。
永続DBを開くのはサーバーだけで、負荷生成プロセスはファイルを直接開かない。
SQL送信・HTTP・結果の全件取得・型変換を計時に含める。組み込みAPIの直接呼び出しの
数値とは区別する。各クライアント側DuckDBは1スレッド、サーバーの既定スレッド数は
`--server-cpus`で選んだCPU数。認証tokenはランダムで、引数・測定メタデータへ出さない。

Quack拡張は準備時だけ公式配布から取得し、既定では
`build/benchmarks/duckdb-extensions-2.0.0`へキャッシュする。取得にはネット接続が必要。
`--duckdb-extensions`で保存先を変更でき、使用したバイナリのSHA-256を記録する。
実行中のクライアントは取得済み拡張をLOADする。
専用ポートの既定は59494。終了時にサーバーと一時DBを削除し、拡張キャッシュは残す。

DuckDBは永続ファイルと標準WAL設定を使い、in-memory DBや耐久性を省いた設定を使わない。
[snapshot isolationと楽観的並行制御](https://duckdb.org/docs/2.0/connect/concurrency)のため、
同一行の同時更新は競合エラーになる。自動再試行は加えず、成功TPS・失敗数・失敗率と
残高等の事後検査を残す。暗黙txnが自動abort済みのときに限り、後始末のROLLBACKの
「有効なtransactionなし」を無害として扱う。元の更新エラーは記録する。

`--duckdb-memory-mib`は`memory_limit`で、既定は`--data-cache-mib`と同じ値。
バッファや索引を含む予算であり、総RSS上限でも、他製品のデータバッファと同じ範囲でもない。
共通CREATE INDEXキーを使うが、DuckDBの物理索引はARTで、圧縮・zone mapも製品固有。
共通SQL／結果の比較であり、ストレージ構造や分離レベルを同一にはしていない。
18 OLAP＋HTAP reader、11 OLTP／HTAP、TPC-H由来22問が対象。
既存TPC-CクライアントはPG Simple Query専用で、DuckDBのTPC-Cは未対応。

回帰検証: `python -m unittest discover -s benchmark/comparison -p '*_test.py'`。
DuckDB用の実接続テストにはlocalhostでのlisten権限が必要。

初回の[5製品比較レポート](../../docs/duckdb_comparison_2026-11-05.md)には
395 OLAP測定・495 OLTP／HTAP試行を保存している。全測定後の容量メタデータ取得が失敗したため、
元の記録を改変せずSHA-256付きの回復記録と独立監査を添えた。
現行コードは容量取得SQLを修正し、5製品スモークで終了処理まで検証済み。

## Oracle Freeの追加枠

`--include-oracle`で公式Fullコンテナを追加できる。既定は従来どおり3製品。
このフラグを付けた場合、CPU指定の既定は先頭2論理CPU、read-workersは2、
他3製品のキャッシュ指定値は1,088 MiBになる。明示した引数は優先する。
Oracle Freeは2コア・DB RAM 2GB・ユーザーデータ12GBまでなので、4製品比較は
`--server-cpus 0,1 --read-workers 2`の別プロファイルにする。
従来の8／16コア結果へOracleだけを並べて同条件とは扱わない。
他3製品のデータキャッシュは1,088 MiBに設定する。FreeはSGA内の配分を管理しており、
同じイメージでも起動後のBuffer Cache Sizeは1,088／1,120 MiBと変わった。
今回MySQLも1,088 MiBの指定に対して実測は1,536 MiBだった。指定値と実測値を区別する。
厳密な同一キャッシュ容量・同一総メモリの比較ではなく、Oracleの実際のSGAを開始／終了時に保存する。
共有プールやPGA、クエリ予算、プロセス全体のメモリ上限も同値ではない。

```bash
python -m pip install -r benchmark/comparison/requirements-oracle.txt
python benchmark/comparison/run.py \
  --output benchmark/comparison/results/my-oracle-run \
  --scratch "$PWD/build/benchmarks" --include-oracle \
  --oracle-image container-registry.oracle.com/database/free@sha256:f988b0c04c4c386cd306a2a914c0d7a9702d83acc31b064a28ad8eb6278a8fba \
  --server-cpus 0,1 --read-workers 2 --data-cache-mib 1088 --secondary-indexes \
  --sizes 10000 100000 1000000 --clients 1 4 16 \
  --repeats 9 --warmup 2 --seconds 5 --trials 3 --tpch-scale .01
```

上記digestは26ai Free 23.26.3.0.0 Full。PG／MySQLも測定時はdigestを指定する。
取得したコンテナはFree Use Terms配布で、ホストへのOracleインストールや
クラウド契約は不要。専用コンテナ・一時領域とランダムな使い捨てパスワードを使い、
ポートはlocalhostだけへ公開し、終了時に当該コンテナと一時DBを削除する。
Dockerイメージ自体は再測定用に残る。最低限、イメージとDBの数GB以上の空きを用意する。

OracleはPython公式ドライバ`oracledb`のThinモード、statement cache無効で測る。
`NUMBER(19)`／`BINARY_DOUBLE`／`VARCHAR2`、末尾LIMITのFETCH FIRST化、
BEGIN／COMMIT／ROLLBACK、DATE型の返却を変換する。リテラルや結果の正しさ判定は
変更しない。Oracleの空文字＝NULLの規則は残り、このfixtureの非空文字列で照合する。
投入時だけOracleのarray DMLを使うためロード時間を純粋なエンジン比較と解釈しない。
STATISTICSはDBMS_STATS、実行計画はDBMS_XPLANで保存する。
計時には送信・実行・全結果取得・ドライバの型変換を含むため、通信方式とドライバ費用の
差はSELECT 1も併せて見る。

OracleはREAD COMMITTED、COMMIT_WAIT=WAIT、COMMIT_LOGGING=IMMEDIATE。
Fullイメージの既存DBでは環境変数だけでARCHIVELOGが解除されないため、mount中に
NOARCHIVELOGへ明示変更して再起動する。redoとFORCE LOGGINGは維持し、実測設定が
NOARCHIVELOG／FORCE LOGGING YESであることを測定前に検証する。
Freeはdb_cache_sizeの指定値より大きなキャッシュを保持するため、
PDBのパラメータ表示値ではなくv$sgainfoの実サイズを使う。従来3製品の既定512 MiBは維持する。
アーカイブ運用・レプリケーションの比較ではない。

対応範囲は既存18 OLAP＋HTAP reader、11 OLTP／HTAP、TPC-H由来22問。
`tinylamb_tpcc_tcp_benchmark`はPG Simple Query専用で、Oracle TPC-Cは未対応。
Freeの制限とTPC由来の非認証試験であることは結果レポートにも明記する。

公式資料: [配布イメージ](https://container-registry.oracle.com/ords/ocr/ba/database/free)、
[リソース制限](https://docs.oracle.com/en/database/oracle/oracle-database/26/xeinl/licensing-restrictions.html)、
[Free Use Terms](https://www.oracle.com/downloads/licenses/oracle-free-license.html)。

初回の4製品実測は[2026-10-31のレポート](../../docs/oracle_free_comparison_2026-10-31.md)。
2コア、共通二次索引、OLAP 316測定・OLTP／HTAP 396試行を保存している。

## 最新の改善結果

[2026-10-30のPG比2倍以内への改善レポート](../../docs/postgresql_twofold_results_2026-10-30.md)。
C34は全種目の標準比較（OLAP 474測定、Tiny/PG OLTP 396試行）を完了し、
短時間の13残差を同じバイナリの長窓・101反復・PG状態確認で再評価した。
最終の元161対象／全456比較は今回PG比2倍以内。標準短窓の結果と精密再測定を
区別しており、旧PG固定値では149/161である。

UNION ALLではPGの可視性情報とBitmap Heap Scan／Index Only Scanの違いを確認した。
新規DBで反復だけ増やした遅いPG値を使わず、明示VACUUM後の速い状態で判定する。
同期writeも数秒の窓だけで判定せず、同期時間と中断数を残して長窓で再確認する。
生データ・採用元・再現引数はレポートとfinal/provenance内に保存している。

2026-10-30のTPC-C W=1／TPC-H SF=1の単体測定は
[BenchmarkHistory](../../BenchmarkHistory.md)に追記した。
[測定記録](results/2026-10-30-tpc-native/)には3試行の中央値・全22問・競合中断を保存。
これはTinylambの直接API測定であり、このディレクトリのTCP経由3製品比較とは別条件。

## 実行

必要環境はLinux、Docker、`taskset`、Python 3.11以上、GoogleSQL有効のtinylambビルド。Dockerへのアクセスが必要。通常のテストやビルドにはPythonドライバも比較DBも追加しない。

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j8 --target tinylamb_server tinylamb_tpch_benchmark
python3 -m venv /var/tmp/tinylamb-comparison-venv
/var/tmp/tinylamb-comparison-venv/bin/pip install -r benchmark/comparison/requirements.txt
/var/tmp/tinylamb-comparison-venv/bin/python benchmark/comparison/run.py \
  --output benchmark/comparison/results/new-run \
  --sizes 10000 100000 1000000 --clients 1 4 16 \
  --seconds 3 --warmup 1 --trials 3 --repeats 5 --tpch-scale 0.01
```

各実行は新しい出力ディレクトリ、新しいtinylambファイル、新しいPostgreSQL／MySQLコンテナを作る。ポートは55433／55432／53306。既存リスナーがあれば開始前に失敗する。終了時には自分が作成したコンテナとDBデータだけを削除し、測定結果とログを残す。`Ctrl-C`も通常の例外経路で後始末する。ホストの既存DBを接続先として指定する機能は設けていない。

`--output`は既存ディレクトリを上書きしない。長時間測定は例えば`--seconds 60 --warmup 10 --trials 5`。スケールや接続数も任意指定できる。CPUの初期値は実行プロセスに許可された先頭8論理CPUで、`--server-cpus 0,1,2,3`等で変更する。tinylambの子プロセスも同じCPU affinityを継承する。負荷生成プロセスのCPUは制限しない。

再現時に同じDBバイナリを使うには、保存された`environment.json`の`images`にあるdigestを`--postgres-image postgres@sha256:… --mysql-image mysql@sha256:…`で渡す。tinylambのHEADだけでなく保存した差分、ビルド条件、サーバのSHA-256も確認する。`--server`で別ビルドを指定した場合、`build_configuration`は通常の`build/CMakeCache.txt`の参考値なので、指定ビルドの条件は別途記録する。

短い動作確認:

```bash
/var/tmp/tinylamb-comparison-venv/bin/python benchmark/comparison/run.py \
  --output /var/tmp/tinylamb-comparison-smoke-new --sizes 100 \
  --clients 1 4 --seconds 0.5 --warmup 0.1 --trials 1 --repeats 1 --tpch-scale 0
python3 -m unittest discover -s benchmark/comparison -p '*_test.py' -v
python3 scripts/check_layering.py
```

終了コード1は、SQLエラー、結果不一致、不変条件違反、または競合によるトランザクション失敗があったことも表す。`failures.json`と`results.jsonl`で内訳を確認する。競合による中断と、誤った結果の返却は区別する。

## ワークロード

| 分類 | 内容 |
| --- | --- |
| OLAP 18種 | COUNT、SUM、1%／10%フィルタ、数式、100／10,000グループ、DISTINCT、Top 100、結合、結合集計、外部結合、EXISTS、スカラー副問い合わせ、CTE、HAVING、UNION ALL、ウィンドウ関数 |
| OLTP 10種 | SELECT 1、主キー検索、非キー検索、100件以下の主キー範囲集計、ランダム更新、16キーへの集中更新、2行送金、4回検索＋2行送金、INSERT＋DELETE、ROLLBACK |
| HTAP 1種 | 1クライアントがsalesとaccountsを結合集計し、残りが4回検索＋2行送金 |
| TPC-H由来22種 | 既存`tpch_queries.hpp`の全テンプレート、同じDBGENファイルで比較 |

accountsは最大10,000行、各行の初期残高100,000、64文字ペイロード。salesは`--sizes`の行数で、データ生成は固定式。salesのcategoryは100種類、account_idは最大10,000種類。主キーはaccounts.idとchurn.idだけで、sales／TPC-Hには索引を追加しない。SQL経由の`CREATE INDEX`は別途機能検査し、共通条件の測定に成功した製品だけの追加索引を混ぜない。

OLTPは最後に指定したsalesスケールで実施する。既定の全スケールでaccountsは同じ10,000行であるため、OLTPの重複実行を避ける。accountsを10,000行より大きくする設定は現実装にはない。HTAPの読み取り側はこのsalesスケールを使う。

TPC-Hのデータ生成器はCMakeで固定したコミットを利用する。スキーマと全クエリは既存C++ソースから読み取る。価格等は全製品でbinary64（tinylambはFLOAT64、それ以外はDOUBLE PRECISION）に揃える。日付演算はリテラルに限り等価なDATE値へ事前変換する。Q11の係数はSFに追従する。列型と計算精度が公式定義と異なり、小数SF、固定パラメータ、更新関数なしの測定なので、公式TPC-H／TPC-Cのスコアとして扱わない。

## 測定条件

全製品を同じ8論理CPUに制限し、バッファ設定は512 MiB。tinylambはクエリ予算64 MiB、PostgreSQLは`work_mem=64MB`、1 Gather最大4並列ワーカー、JIT有効。メモリ設定は各製品で意味が異なるため、プロセス総メモリや同じクエリ全体のメモリ上限を保証する値ではない。MySQLの他のクエリメモリ設定は既定値。tinylambは読み取りワーカー8、内部スケジューラは既定値を使う。

tinylambは既定の同期commit、PostgreSQLは`fsync=on`・`synchronous_commit=on`、MySQLは`innodb_flush_log_at_trx_commit=1`。MySQLのbinlogは無効で、レプリケーション性能は測らない。PostgreSQL／MySQLはREAD COMMITTED、tinylambは既定のMVCC。完全な分離レベル同値性や停電時の耐久性をこのテストで証明するものではない。

PostgreSQL／tinylambはpsycopg2のSimple Query、MySQLはmysql-connector-pythonのC拡張、非prepared COM_QUERYを使用する。SSLを無効化し、接続を再利用し、SQL文を1文ずつ送る。異なる製品間にはプロトコルとドライバの費用差があるため、SELECT 1を通信・ドライバ・サーバ固定費の参考として併記する。製品ごとのprepared statementの最高性能を比較する試験ではない。

各サイズのロード後にANALYZEする。OLAPはクエリごとに最初の実行を`first_ms`として分離し、その後5回の中央値を使う。全行を取得してから時計を止める。最初の実行もロード直後のOSキャッシュを使用し得るため、コールドI/O性能を表さない。各クエリのEXPLAINは計時後に保存する。統計の乱数やプラン選択の揺らぎは固定していない。

OLTPは接続ごとに独立したPythonプロセスを使用する。全接続の準備後に共通の時計でウォームアップと測定窓を開始する。窓内で開始・終了した成功トランザクションだけをTPSと成功時遅延へ数える。境界を跨いだ成功数を別に記録する。失敗は再試行せず、ROLLBACKして次の要求へ進む。失敗数は窓内開始の要求を数えるため、最後に窓外で失敗した要求を含み得る。p50／p95／p99は各試行の成功サンプルからnearest-rank法で算出する。失敗時の遅延はこれらの分位点に含まない。

製品は同時に負荷をかけず、測定順をseedでシャッフルする。OLAPは各クエリを製品ごとにまとめて反復し、OLTPは各条件を複数試行する。全試行でDBを作り直すわけではなく、同じインスタンスに蓄積したキャッシュ・WAL・MVCC・メンテナンス履歴を含む。accounts残高とchurn行は試行前にリセットする。

## 正しさと保存物

ロード時は行数・ID総和・金額総和を検査する。OLAPはPostgreSQLの全結果行と照合し、繰り返しで結果が安定することも検査する。整数／DECIMALは厳密比較し、浮動小数点の演算順序差には相対誤差1e-7・絶対誤差0.01を許す。順序も比較するので、SQLが同値キー間の順序を完全指定していない場合の不一致は結果ファイルから判断する。フィンガープリントは保存用で、異なる数値表現間の正しさ判定には使わない。

OLTPは各試行後に行数と残高総和を検査する。更新では全成功数（ウォームアップ・境界外完了を含む）だけ残高が増え、送金・ROLLBACKでは残高総和が保存され、INSERT＋DELETEではchurnが空でなければならない。SELECT 1、主キー検索、非キー検索は返却値・順序も検査する。HTAPではsales行数がaccounts行数の整数倍なら結合後の残高総和が保存されることも検査する。値不一致は`validation_failures`にwarmup分も記録して試行を無効にし、通常のSQL競合と分ける。`valid=true`はSQL失敗0を意味しない。これは全種類の分離異常、各行の最終状態、クラッシュリカバリの完全検証ではない。

| ファイル | 内容 |
| --- | --- |
| `environment.json` | バージョン、イメージdigest、CPU／メモリ、設定、HEAD、作業ツリー差分hash、バイナリ／実行スクリプトhash |
| `tracked-changes.patch` | 実行時に存在したtrackedファイルの差分。既存の利用者変更を含む |
| `commands.jsonl` | 起動・データ生成のコマンドとtinylamb環境設定 |
| `results.jsonl` | 試行単位の値、OLAP全計時、エラー例、ワーカーごとの成功・失敗数、不変条件検査 |
| `summary.csv` | 表計算／分析用の平坦な表 |
| `query-details/` | SQL結果の全行とEXPLAIN |
| `failures.json` | 不一致・SQLエラー・中断のある測定 |
| `tpch-data-sha256.json` | 8つの入力ファイルのhash |

待機やネットワーク停止にはDB／ドライバ側タイムアウトと子プロセスの監督期限を設ける。tinylambにはSQLキャンセル機構がないため、子プロセスを停止してもサーバ内部のクエリ終了は保証しない。タイムアウト発生時の後続値はログと負荷を確認して扱う。

最新の全条件再測定は[2026年10月29日・第5巡のレポート](../../docs/benchmark_full_comparison_2026-10-29.md)。EXISTSとOLTP読み取りの改善後、追加索引なし／共通二次索引ありの両方で、3スケールの共通OLAP、TPC-H全22クエリ、全OLTP／HTAPを測定した。第2巡は[前回のフル測定](../../docs/benchmark_full_rerun_2026-10-29.md)、第1巡は[10月29日の旧レポート](../../docs/olap_oltp_comparison_2026-10-29.md)、修正前の基準値と64／256 MiB時のspill等の追測は[10月28日のレポート](../../docs/olap_oltp_comparison_2026-10-28.md)に保存している。未測定の領域は、キャッシュを超えるデータ、長時間定常負荷、分離異常、障害・再起動、レプリケーション、prepared protocol、TPC-C／TPC-DSの3製品共通実装など。ベンチマークの範囲を広く取っているが、これらまで測定済みとは扱わない。

## 第5巡の全種目比較

`results/2026-10-29-full5/{unindexed,indexed}`が最新の全条件。
サーバは`16a67785…`で固定し、OLAPは初回＋9反復、OLTP／HTAPは
2秒warmup＋5秒×3試行とした。各実行引数は`provenance/command-*.json`、
ソース対応・差分・ハーネスは`provenance/`へ保存した。scratchは同じNVMeのext4を使う。
今回から`range_read`の全成功応答も計時中に検証する。解析・比較器は33テスト通過。

両構成でOLAP 474測定の結果が比較先・反復間で一致し、OLTP／HTAP 594試行が検査を満たした。
SQL競合は別集計し、成功TPSへ含めない。PG比10倍以上はp50 1指標とtail 3指標、
MySQL比は0件。非キー検索／範囲集計の12条件はPG比最大1.54倍、旧PG固定比最大1.72倍。
書き込み系120条件中55条件で最大／最小TPSが2倍以上変動したため、短時間窓の観測として扱う。
全表、初回、ロード／索引構築、役割別HTAP、エラー、実行カウンタ、過去値との比較は
`analysis/`に保存し、本文を次で再生成できる。

```bash
python3 benchmark/comparison/results/2026-10-29-full5/analysis/generate_report.py
```

既存の実行記録を上書きしない。再測定は`command-*.json`を参照し、対応バイナリと
新しい`--output`を指定して`run.py`を両構成で実行する。

## 改善後の追測オプション

`--olap-workloads count sum top_100`で共通OLAPの対象を絞れる。
`--tiny-query-memory-mib 256`はtinylambだけの予算を変え、設定を環境情報へ保存する。
`--explain-analyze`は計時後にtinylambの実行カウンタも採取し、
`query-details/`の`actual_plan`に保存する。通常のEXPLAINは推定形状で、
両者は別に読む。診断実行の時間を通常の反復時間へ混ぜない。

`--secondary-indexes`は全3製品へ同じ通常B-treeのキーを作る。
microはaccountsの`(bucket,id)`、salesの`(account_id)`、
`(category,amount)`、`(amount,id)`。TPC-H側の全キーは
`workloads.secondary_indexes`を参照。全DDLと構築時間を保存し、
一製品でも構築に失敗したら実行を失敗させる。既定は従来の追加索引なし。
索引ありの値は別の出力ディレクトリで扱い、元の表に混ぜない。

`profile.py --output NEW_DIR`はtinylamb単体の診断用で、同じfixtureを使い
初回を除いた反復時間、全結果、EXPLAIN ANALYZEを保存する。
`--server`で修正前バイナリ、`--memory-mib 64`／`256`で予算を指定できる。
3製品をその場で照合する比較には`run.py`を使う。

サーバの`--read-workers`は互換名で、改善後は通常DMLとtransaction制御にも
同じプールを使う。TEMP／UDFセッションとDDLはI/O側に保持する。
catalog変更は他の開いたtransaction／workerがある間55006で再試行を要求する。
これはonline index構築ではない。CREATE INDEXは昇順の通常列キー、
UNIQUE、IF NOT EXISTS、GoogleSQLのSTORINGに対応する。式索引、DESC、
SEARCH／VECTOR、OR REPLACE、OPTIONSは明示的に拒否する。索引名は表内で判定する。


## PostgreSQLとの中央値の差を抽出する

```bash
python3 benchmark/comparison/gaps.py benchmark/comparison/results/2026-10-29 \
  --output /var/tmp/tinylamb-pg-gap-new
```

全スケール・全接続数のtinylamb／PostgreSQLを対応付け、実行時間は
`tinylamb / PostgreSQL`、成功TPSは`PostgreSQL / tinylamb`で不利倍率を求める。
OLAPは初回を除く中央値、OLTPのTPSとp50は各試行の値の中央値を使う。
p95／p99はtailとして分け、HTAPはreader／transactionの役割別にも比較する。
混合負荷の役割別遅延を保存した実行では、そのp50／p95／p99も別の指標として抽出する。
既定の10倍以上を丸める前に判定し、全比較・抽出結果を`gaps.json`と表へ保存する。
不一致のOLAP、無効なOLTP、対応のない条件、重複試行は順位付けせず失敗する。
出力先は新規ディレクトリを使い、再集計する場合だけ`--overwrite`を指定する。
`--reference-engine mysql`で同じ基準をMySQLにも適用できる。
既定はPostgreSQL。JSONの参照値・サンプル・エラー・詳細ファイルのキーと
表の製品名も選択した参照先に合わせる。

[10月29日の指標別改善計画](../../docs/postgresql_gap_plan_2026-10-29.md)と
[TODO](../../TODO.md)に、各指標の原因候補・修正順序・合格条件を対応付けた。

### 長時間の差分検証

`--skip-olap`でOLTP/HTAPだけを実行できる。`--oltp-cases hot_update:1 update:16`
は選択済みworkload/clientsの組合せを絞り、`--oltp-engines tinylamb postgresql`
は比較対象を絞る（起動・fixture検証は引き続き3製品）。
`--warmup 10 --seconds 60 --trials 5 --fresh-trials --server-metrics`
で各試行前にfixtureを再作成し、SQLワーカとWALの累積カウンタをサーバログへ保存する。
省略時は同じfixtureの履歴を引き継ぐ。seedで製品順を変更する。

HTAPの1接続は分析reader単独、4/16接続はreader 1＋writer 3/15。
`--htap-reader-baseline`は同じ`SUM(a.balance)` SQLのOLAP単独測定も追加する。
`role_latency`に役割別percentile、workerの`boundary_samples`に測定境界をまたいだ
成功クエリの開始・終了時刻と遅延を記録する。境界外完了はwindow内query/sに含めない。
`measure_start_monotonic`と`seconds`でサーバの`TINYLAMB_METRICS.time_ns`に対応付けられる。
`nonkey_read`はLIMITで返るID列とbalanceもfixtureから検証する。

`gap_followup.py --history fresh` は表をDROP／CREATEしてfixtureを再ロードする。
サーバプロセス、WAL履歴、OSキャッシュは維持する。`continued`は表を再作成せず、
各試行前に残高とchurnだけをリセットする。プロセスやWALを毎回初期化する試験とは区別する。

## A–Hの最終追測と再集計

[実施レポート](../../docs/postgresql_gap_results_2026-10-29.md)に実装、全41指標、
未達の5指標、writer側の後退、測定条件の限界を記載した。
`2026-10-29-gap-post-{unindexed,indexed}-olap`が最終OLAP、
`2026-10-29-gap-final-g-{unindexed,indexed}`がG/Hの60秒×5試行、
`2026-10-29-gap-post-f`がFの並列検索。いずれも`results/`以下に保存した。
長時間G/Hは`b7af77d1…`版、最終OLAPとFは空commit・複文エラー修正後の
`6ce27fb4…`版で、同一バイナリの一括測定ではない。使用版は各`environment.json`で確認する。

```bash
python3 benchmark/comparison/gap_results.py \
  --run unindexed=benchmark/comparison/results/2026-10-29-gap-post-unindexed-olap \
  --run indexed=benchmark/comparison/results/2026-10-29-gap-post-indexed-olap \
  --run unindexed=benchmark/comparison/results/2026-10-29-gap-final-g-unindexed \
  --run indexed=benchmark/comparison/results/2026-10-29-gap-final-g-indexed \
  --run unindexed=benchmark/comparison/results/2026-10-29-gap-post-f \
  --output /var/tmp/tinylamb-gap-summary-new
```

元PG固定値と同時期PG値の**両方**で不利倍率10未満の場合だけ通過とする。
保存済み集計は`results/2026-10-29-gap-final-comparison/`。
図の生成だけは任意依存の`matplotlib`が必要。

## 第2巡のフル再測定と再集計

`results/2026-10-29-full2-before/{unindexed,indexed}`と
`results/2026-10-29-full2-after/{unindexed,indexed}`が変更前後の全条件。
各相でOLAP 474測定、OLTP／HTAP 594試行を含む。
`--server-metrics --explain-analyze --htap-reader-baseline`を有効にし、
冒頭のフル実行を追加索引なし／`--secondary-indexes`ありでそれぞれ実行した。
時刻、製品digest、全引数とハッシュは各`environment.json`に保存している。

変更前は`6ce27fb4…`、変更後は`876da51b…`のサーバを両構成で固定した。
各相の共通`server-source.json`と`server-source.patch`がそのバイナリの
ソース対応情報である。個別の`tracked-changes.patch`は実行開始時のworkspaceを
記録するため、凍結後に準備した変更を含み得る。再ビルド時は共通の差分を使う。

```bash
python3 benchmark/comparison/gaps.py \
  benchmark/comparison/results/2026-10-29-full2-after \
  --reference-engine mysql --output /var/tmp/tinylamb-full2-mysql-new
python3 benchmark/comparison/gap_results.py \
  --baseline benchmark/comparison/results/2026-10-29-full2-gaps-before-mysql/gaps.json \
  --reference-engine mysql \
  --run unindexed=benchmark/comparison/results/2026-10-29-full2-after/unindexed \
  --run indexed=benchmark/comparison/results/2026-10-29-full2-after/indexed \
  --output /var/tmp/tinylamb-full2-mysql-targets-new
```

参照先を省略するとPostgreSQLとの比較になる。各参照先456指標を比較するが、
同一負荷のTPS・p50・tail・HTAP役割別を含むため、456種類の独立負荷ではない。
旧比較先固定値と同時期の比較先の両方で10倍未満になったかを判定する。
全比較・正しさ監査・前後の後退条件は`results/2026-10-29-full2-comparison/`、
最終テストログは`results/2026-10-29-full2-validation/`に保存する。
第1巡の長時間WAL課題と、その古いPG固定目標は別に残す。

## 第3巡のEXISTS改善と交互測定

[EXISTSの追測レポート](../../docs/exists_performance_2026-10-29.md)と
`results/2026-10-29-exists3-comparison/`に、3規模・両構成の前後比較と、
索引あり21反復×3組の交互測定を保存した。
前後は`876da51b…`／`bef16c9b…`で固定し、内側の等値索引prefixから
直接キー集合を構築する改善を比較している。
各組は前後／後前／前後の順とし、MySQLは固定した旧値と同時期の両基準で判定する。
全4,099テストと301秒のSQL oracleを通過した後に交互測定を実行した。
再集計と新しい出力名での再実行は、同ディレクトリのREADMEと`run_pairs.py`を参照。
この追測には全OLTP／HTAPの再測定は含めていない。

```bash
python3 benchmark/comparison/plot_gaps.py \
  /var/tmp/tinylamb-gap-summary-new/comparison.json \
  benchmark/comparison/results/2026-10-29-gap-final-g-unindexed/results.jsonl \
  --output /var/tmp/tinylamb-gap-summary-new
```

`trial_metrics.py --help`はサーバカウンタと試行窓の対応付けに使う。
正しさとテストの監査記録は`results/2026-10-29-gap-validation/`、
値不一致判定器を3製品で確認した短いsmokeは`2026-10-29-gap-validation-smoke/`。
smokeの性能値は最終比較に混ぜていない。


## 第4巡のOLTP読み取り改善

[読み取りの調査・改善レポート](../../docs/read_performance_2026-10-29.md)と
`results/2026-10-29-read4-comparison/`に、非キー検索・100行範囲集計の
追加索引なし／あり・1／4／16接続の比較を保存した。
最終測定はsales 100万行、accounts 1万行、warmup 2秒＋5秒×3試行。
同時期PGだけでなく、第2巡フル測定の速いPG値を固定した比率も判定する。
全体のフル測定の更新ではなく、この2種類の読み取りと周辺7 OLAPの追測である。

`run-final.py`のserverパスとoutput名を新しいものへ変えると再実行できる。
`summarize.py`は3試行のp50／p95／p99／query毎秒を集計する。
計時ハーネスはrange_readのSUMを毎回は照合しないため、`range_audit.py`で
全始点とNULL・値更新後の結果を計時外に完全列挙して検証する。
`results/2026-10-29-read4-version/`にはC++差分と最終バイナリの指紋、
`results/2026-10-29-read4-validation/`には全テストとoracleの記録を置いた。
