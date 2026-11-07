# TPC-Cの3製品比較（2026-11-06・完了）

- [x] DuckDB・Oracleを除き、共通C++生成器へMySQL C APIの接続層を追加する。
- [x] 専用clientターゲットをDB本体から分離し、C8 serverを固定する。
- [x] NULL・数値・更新件数・commit／rollbackと、9表の全行・値一致を3製品で事前検証する。
- [x] W=1・10接続・warmup5秒＋30秒×3試行の全9試行で事後不変条件を確認する。
- [x] 初回のビルド混入を保存し、他ベンチマークも含む監視で全条件を再測定する。
- [x] 最終シリーズのTiny trial3／PG trial3の他TPC実行混入を同一条件で再測定し、採用元を監査する。
- [x] 成功TPS・engine abort・意図したrollback・5種別の分位点・全試行を[レポート](docs/tpcc_three_comparison_2026-11-06.md)と履歴へ記録する。
  中央値TPS: Tiny C8 1,712.30／PG 1,373.27／MySQL 4,376.43。中断率も別記する。
- [x] 比較器60テスト、3製品の実接続検査、layering、図表を検証する。

---

# DuckDBを比較対象に追加（2026-11-05・初回比較完了）

- [x] 公式DuckDB 2.0.0とQuack拡張を取得し、専用永続DB・localhost接続を追加する。
- [x] 独立フラグで既定3製品／DuckDB追加4製品／Oracle込み5製品を選択できるようにする。
- [x] DATE・大きな整数・NULL・rollback・更新競合・別プロセスからの同時更新を実接続で検証する。
- [x] 全205 OLAP事前測定のPG一致とDuckDB全33 OLTP条件の不変条件を確認する。
- [x] 5製品の全3規模OLAP＋TPC-H由来22問、全11 OLTP×1／4／16接続×3試行を完了する（395正答・495整合性通過）。
- [x] 欠落重複・正答・失敗率・実測設定・実行時ソースを監査し、[レポート](docs/duckdb_comparison_2026-11-05.md)とBenchmarkHistoryへ記録する。
- [x] 測定後の容量SQL誤りをPRAGMA database_sizeへ修正し、5製品スモークで終了保存まで通す。
  本測定の生データは改変せず、SHA-256付き回復記録を添える。終了時容量／SGAは未取得のまま明記する。
- [x] 比較器51テスト・layering・5製品グラフを確認する。
- [ ] DuckDB用TPC-C接続層とSF=1／組み込み直接APIの補助測定は別課題として検討する。

---

# Oracle Freeを比較対象に追加（2026-10-31・初回比較完了）

[4製品比較レポート](docs/oracle_free_comparison_2026-10-31.md)。

- [x] 公式Free Use配布・2コア／DB RAM 2GB／ユーザーデータ12GBの制限を確認する。
- [x] 公式26ai Free 23.26.3.0.0のFullコンテナを取得し、digestを固定する。
- [x] Oracle Thin接続・方言・明示トランザクション・統計収集を既存比較器へ追加する。
- [x] 18 OLAP＋TPC-H由来22問の4製品全行一致、Oracle全11 OLTPの不変条件を事前検証する。
- [x] 共通2コア枠で反復比較し、通常の8／16コア測定と分けてBenchmarkHistoryへ記録する。
- [x] OLAP 316測定の全結果一致、OLTP／HTAP 396試行の整合性・欠落重複なしを監査する。
- [x] 実測バッファ容量差（MySQLの指定値丸め、OracleのSGA配分変動）とドライバ／経路差を記録する。
- [ ] OracleのTPC-C用接続層を実装し、既存C++生成器との同値性を検証する（PGプロトコル版の流用不可）。

---

# PG比1.5倍以上の改善（2026-10-30・進行中）

[計画と対象一覧](docs/postgresql_1_5_plan_2026-10-30.md)。

- [x] 既存比較の対象34指標を倍率順で固定する。
- [x] TPC-H SF=1のPG比較、TPC-Cの共通クライアント測定を準備する。
- [ ] 倍率順に改善し、正しさと同条件のPG比を再確認する。
- [ ] 全種目の退行確認とBenchmarkHistoryへの結果追記を完了する。
- [x] C5–C7の全4,192回帰テスト、SQL oracle 601秒788入力、比較器40テスト、layeringを確認する（失敗0、既存skip 3）。
- [x] 単一接続Deliveryで反復するorder_line更新失敗を小さな回帰テストへ落として修正する。
  C7時に診断fixture `build/benchmarks/pg15-c7-delivery-gdb4/database` のDelivery preflightで再現（C8検証で当該fixtureは進行済み）。
  district 10 / order 2370、page 3627 / slot 10。原因はUPDATEの移動先探索が古い末尾ヒントを更新せず、末尾予約で失敗すること。
  C8はINSERTと同じowner-visible末尾カーソルから探索し、古いヒントを更新する。
  65行の再起動・commit／abort・旧snapshot回帰が旧実装で失敗、新実装で通過。保存fixtureの全5事前検査も通過。
- [x] C8の全4,194回帰テストとSQL oracle602秒776入力を通す（失敗0、既存skip3）。
- [x] C8のTPC-Cを30秒×3試行でPGと再比較し、中断率・全20指標を保存する（全6試行valid）。
  Delivery p50 8.859 ms／PG 3.229 ms =2.744倍、p95 2.547倍、p99 2.323倍。
  元の目標は未達。Order-Status／Stock-Level／Deliveryの計9遅延指標が1.5倍以上。
- [ ] Order-Statusの純粋な列射影でCSE探索・式のrewrite・bytecodeコンパイルを省略する。
  C8の命令数診断: cache replay 53.35%、Projection構築25.39%、bytecode compile16.55%。
  並べ替え・重複列・NULL・DATE・unsigned・collation・row positionと未解決列fallbackを検証する。
- [x] C7のTPC-H SF=1全22問をPGと全値照合し、部分C4測定を補完する（44/44正答・安定）。
  Q09／Q11／Q10／Q04／Q20／Q19の6問は今回同条件PG比1.5倍以上で残る。

---

# PostgreSQLが2倍以上速い全指標の改善（2026-10-30・第6巡完了）

[最終レポート](docs/postgresql_twofold_results_2026-10-30.md) / [実装計画と経緯](docs/postgresql_twofold_plan_2026-10-29.md)。
達成判定はユーザー指定の今回同条件PG比2倍以内。標準フル測定だけでは13未達、
同じバイナリの長窓・反復増加・PG状態確認を反映すると元161対象／全456比較が通過。
旧PG固定値では149/161であり、旧基準を全件通過したとは扱わない。

- [x] 第5巡456比較から元161対象を抽出し、旧サーバ・ソース・固定PG値を保存する。
- [x] A: 走査・型付き集計の行構築、batch転送、可視性確認を改善する（14/14）。
- [x] B: 結合・副問い合わせをstream化し、中間materialize/spillと反復走査を減らす（39/39）。
- [x] C: 計画・式・索引カーソルの固定費を減らす（10/10）。
- [x] D: dispatchと短い応答の固定費を減らし、pipeline/half-closeを検証する（35/35）。
- [x] E: 同期WAL・混合負荷を改善し、own/dependency barrierを保つ（63/63）。
- [x] 正確な整数キー件数再利用、列形式のdimension、非負SUMの直接foldを実装する。
- [x] NULL・DATE・符号・重複・snapshot・commit/abort・削除穴・未公開tail・予算・overflow・drop/recreateを検証する。
- [x] 全4,178テスト失敗0、既存skip 3、SQL oracle 602秒771入力・新規追加655件・不一致0、解析34テスト、整形109ファイル、layeringを確認する。
- [x] 同一C34バイナリで両索引構成のOLAP 474測定・Tiny/PG OLTP 396試行を完了し、全件整合性を確認する。
- [x] 短窓の未達13指標を保存し、該当OLTP 4条件を30秒×3試行で再測定する（24試行、条件全体24指標を採用）。
- [x] UNION ALLを101反復で再確認し、PGの可視性・計画をVACUUM前後で比較する。速いIndex Only Scan側のPGを最終判定に使う。
- [x] 元161対象・全456比較、旧PG参考値、採用した測定元を監査する（最終最大1.938倍、旧PG固定比は149/161）。
- [x] 固定長WALの新規server DBへの採用範囲・既存v1維持・旧バイナリ非対応をADRとレポートへ記録する。
- [x] レポート・全比較表・生データ・再現コマンド・検証ログを保存する。

次の測定課題（今回の達成判定とは別）:

- [ ] PGのvisibility map／VACUUM／自動メンテナンス状態をOLAP標準手順に明示する。
- [ ] 同期時間の状態差を検出し、短い窓の偽の倍率差を長窓・交互順で確認する手順を定常化する。
- [ ] fact取り込み・更新とcache無効化／再構築を伴うHTAP、cold時、より大きいSFを追加する。
- [ ] PGの待機とTinyのNoWait中断を、成功latencyに加えて中断率・再試行費用でも比較する。

---

# 全種目の3製品フル比較（2026-10-29・第5巡）

- [x] EXISTS／OLTP読み取り改善後のサーバを固定し、バイナリ・ソース・ハーネスのhashを保存する。
- [x] 範囲集計の各成功応答のSUM検証を追加し、比較器33テストを通す。
- [x] 追加索引なし／共通二次索引ありで、3規模の19 OLAP・TPC-H由来22問・全11 OLTP／HTAPを再測定する。
  OLAP 9反復、OLTP 2秒warmup＋5秒×3試行・1／4／16接続。
- [x] OLAP 474測定の比較先・反復間の結果一致、OLTP／HTAP 594試行の検査通過、条件の欠落・重複なしを監査する。
  SQL競合は成功TPSから除外。測定窓の失敗はtiny 13,087／11,619件、PG／MySQLは0件。
- [x] 中央値・成功TPS・tail・役割別HTAP・エラー率・索引効果・ロード時間・実行カウンタをレポート化する。
  [全種目比較レポート](docs/benchmark_full_comparison_2026-10-29.md)。
  非キー検索／範囲集計の全12条件はPG比最大1.54倍、旧PG固定比最大1.72倍。

PG比10倍以上は索引ありread_write／1接続のp50（10.33倍）と更新系tail 3指標。
MySQL比は0件。これらの書き込み残差は下記Gの長時間検証課題に含める。
結合集計／HTAP readerにも最大7倍台の差が残る。書き込み系120条件中55条件で
最大／最小TPSが2倍以上変動しており、長時間・実行順交互の再確認を残す。
今回DB本体の追加修正は行っていない。

---

# OLTP読み取りをPostgreSQL比2倍以内へ（2026-10-29・第4巡）

- [x] 非キー検索と100行範囲集計を、索引あり／なし・1／4／16接続で修正前に再測定する。
- [x] 行位置の汎用ストリーム復号、葉ページの個別文字列確保、不要列と重複条件評価を削る。
- [x] スナップショットの可視性解決を保ち、連続する同一heapページを所有コピーで再利用する。
- [x] 小さな範囲検索の先読み用カーソル構築を抑える。
- [x] 書き込み停止後も残ったMVCC履歴のGCを進める。古いsnapshot・耐久性の判定は維持する。
- [x] NULL・再束縛・残差・符号・逆順・穴・binary INCLUDE・破損入力・GC再開の回帰を追加する。
- [x] 全ctest・SQL oracle・layering・formatを確認する。
  4,107テスト失敗0（既存skip 3）、SQL oracle 301秒456入力・不一致0。
  解析31テスト・format 11ファイル・layering通過。
- [x] 元の100万行fixtureで独立した再測定を行い、全12条件のp50をPG比2倍以内にする。
  同時期PG比最大1.57倍、前回フル測定の速いPGを固定しても最大1.59倍。
  最終OLTP 72試行・周辺OLAP 42測定はSQLエラー／不一致0。
  範囲検索の全始点をNULL・値変更後も含め40,000クエリで別途検証し、全件一致。
- [x] 原因・全試行・比較表・再現手順をレポートに保存する。
  [第4巡のレポート](docs/read_performance_2026-10-29.md)。
  非キー検索／追加索引なし／1接続は0.483→0.074 ms、範囲集計は0.143→0.035 ms。

---

# EXISTSの退行解消と余裕の確保（2026-10-29・第3巡）

- [x] 修正前の同一バイナリで3製品・3規模・両索引構成を再測定する。
  EXISTSと周辺4クエリ、9反復で各構成45測定。索引あり100万行は14.510 ms。
- [x] 選択的な内側等値条件で索引prefixを走査し、可視行から直接キー集合を構築する。
  残りの局所条件も再評価し、相関処理と内側Relationの構築を避ける。
- [x] 統計の過小推定を吸収する容量余裕と、予算内のメモリ予約を追加する。
  実際のキー数超過・古いsnapshot・予算不足では通常経路へ戻す。
- [x] NULL／NOT EXISTS、複合prefixと残差条件、統計誤差、容量超過、メモリ不足、
  索引キー更新後の古いsnapshotを回帰テストで検証する。
- [x] 修正後も3製品・3規模・両構成を測り、全90測定の結果一致を確認する。
- [x] 全4,099テストを通す（失敗0、既存skip 3件）。
- [x] SQL oracleを300秒実行し、layering・formatも検証する。
  301秒449入力で不一致0。解析31テスト、既存allowlist 65辺、新規違反0。
- [x] 修正前後の順序を交互にし、21反復×3組でMySQLとの差と100万行の退行解消を確認する。
  100万行は16.048→2.614 ms（6.14倍高速化）、1万行は0.964→0.732 ms。
  1万行の旧MySQL固定比5.43倍、同時期MySQL比5.47倍。3組すべて両基準を通過。
- [x] 旧MySQL固定値・同時期の値の両基準と、全計測・再現手順をレポートへ保存する。
  [第3巡のレポート](docs/exists_performance_2026-10-29.md)。焦点比較180測定と交互54測定が全件一致・安定。

---

# フル再測定と追加改善（2026-10-29・第2巡）

- [x] 変更前のバイナリを固定して、追加索引なし／ありの3製品フル測定を行う。
  1万・10万・100万行、TPC-H由来22問、全OLTP／HTAPの1／4／16接続を含める。
- [x] 不利倍率の抽出をPostgreSQL／MySQLから選べるようにし、中央値・TPSの向きと製品名を検証する。
- [x] 新しい測定で10倍以上の条件を抽出し、原因を実行計画・カウンタから確認する。
  PostgreSQLは456指標中0件、MySQLは索引ありEXISTS／1万行と
  DISTINCT／100万行の2件。HTAPの役割別p50／p95／p99も別々に抽出する。
- [x] COUNT(DISTINCT key)で重複した索引キーの全行展開を避ける。
  NULL、snapshotのfallback、複数集計、残差述語、INCLUDE、逆順・batchを回帰で検証する。
- [x] 選択的なEXISTSの単純な等値相関を、一度構築したキー集合へ変換する。
  EXISTS／NOT EXISTSのNULL・空集合を保ち、小規模走査の過剰な並列起動も抑える。
- [x] 局所追測でPG比10.16倍だったTop 100のプラン再利用を改善する。
  安全な実テーブルの読み取りだけを共有し、LIMIT別の識別、同時実行、退役、
  epoch／incarnation／snapshotの契約を回帰で検証する。
- [x] 改善後も同一バイナリで両構成のフル測定を行い、結果一致・中断率・他条件の後退を確認する。
  前後それぞれOLAP 474測定・OLTP／HTAP 594試行を検証。
  PG／MySQL各456指標で10倍差は0件。元MySQL固定値の基準も2件とも通過。
  OLAPの1.5倍超かつ1 ms超の後退、成功TPS中央値の20%超低下は0件。
- [x] 全ctest・SQL oracle・layering・formatを通し、比較結果と残差をレポートへ保存する。
  4,095テスト失敗0（既存skip 3）、oracle 181秒285入力不一致0、解析31テスト通過。

[第2巡のレポート・全表・再生成手順](docs/benchmark_full_rerun_2026-10-29.md)。
EXISTSは2.316→1.335 ms、DISTINCTは64.203→23.688 ms、
索引ありTop 100／10万行は0.455→0.221 ms。

次の改善候補:

- [x] 1万行EXISTSの準備・小規模走査費用をさらに削り、比較先固定値への余裕を増やす。
  第3巡の交互測定で旧MySQL固定比9.90→5.43倍、100万行は2.614 ms。
  3組とも修正前・退行指摘前の値を下回り、索引なしの通常経路も検証した。
- [ ] DISTINCTで残る値リストの全デコードを計測し、安全なキー単位処理を進める。
  NULL・可視性・破損検知・逆順の契約を保つ。100万行は第2巡23.688 ms→第5巡4.359 ms。
  読み取り改善後の短縮を観測したが、個別のデコード費用の切り分けは未実施。
- [ ] HTAP reader（最大PG比8.43倍）とjoin_group（8.16倍）のscan／materialize／集約を分解する。
  同じreader SQL、writer TPS、役割別遅延・サンプル数を保ってstream化の効果を確認する。
- [ ] 下記Gの長時間・同期WAL課題を、旧PG固定目標を使って継続検証する。

---

# PostgreSQLとの10倍以上の差を解消する TODO（2026-10-29・第1巡の記録）

[指標別の比較・原因・改善計画](docs/postgresql_gap_plan_2026-10-29.md)に、
[改善前の再測定](docs/olap_oltp_comparison_2026-10-29.md)の全スケール・接続数から
抽出した主対象35件とp95／p99の補足6件を対応付けた。
以下は次の実施順序。完了は実装・正しさの回帰・同条件での再測定まで確認して付ける。
各目標は性能予測ではなく、PostgreSQLとの差を厳密に10倍未満へ収める最初の基準。

- [ ] **0. 計測と再現条件を補強する。**
  scan入力、キー／リストのdecode回数・bytes、可視性確認、batch同期、
  spill、ワーカqueue、WAL同期とcommit応答までを切り分ける。
  書き込み・HTAPは10秒warmup＋60秒測定×5試行、実行順変更、
  fresh／同一履歴で確認する。HTAPの境界外完了と役割別query/sも記録する。
  SQLワーカqueue／占有、耐久化待機、同期回数・時間、役割別・境界外完了は実装・採取済み。
  decode bytesの詳細と、I/O状態・試行長・履歴を揃えた変更前後の交互測定を残す。
- [x] **A. 非unique索引走査とコストを改善する（主対象9件）。**
  COUNT／SUM各3規模、filter_1pct、DISTINCT、Q06。
  キー切替までデコード済みリストを保持し、型付きbatch集計へ接続。
  その後にNDV・リスト長・heap局所性・選択率・LIMITを費用へ反映する。
  DISTINCTのキー単位処理とQ06のbitmap候補は残差を見て追加する。
  NULL／逆順／snapshotの意味を保ち、100万行COUNT <67.936 ms、
  SUM <106.040 ms、filter_1pct <2.571 ms等の全9境界を確認する。
- [x] **B. Top Nを先頭キー順とgroup単位の整列で早期停止させる（3件）。**
  最大7,966倍差。Logical TopN後の候補到達性を調べ、既存IncrementalSortの
  全入力materializeをgroup単位のstreamへ変える。混合ASC／DESC、NULL、
  OFFSET／WITH TIES、collation、大きいpeer groupを検証。
  100万行で100行を返すのに全件を読まないことと、<0.721 msを確認する。
- [x] **C. EXISTSの内側フィルタを構築前に押し、必要キーだけ保持する（5件）。**
  既存の1回buildを活かし、category条件をLoadSource／索引候補へ渡す。
  outer domain／キャッシュ所有者とNULLの契約を保った専用キー集合にする。
  100万行・64 MiBで不要なspillをなくし、索引なし<84.749 ms、
  あり<34.433 msを含め、全5条件を検証する。
- [x] **D. spillしたprobeでも結合・集計を融合する（2件）。**
  build=1万行は既に達成済み。TryFusedGroupedJoinのspill制約を解き、
  100万行のjoin出力をRelationへ書かず100グループへ直接集計する。
  次にscanからのstream供給を検討。64 MiBのまま<302／306 msを目指す。
- [x] **E. LEFT JOINの右側ON述語を先に適用し、予算内で集計融合する（2件）。**
  salesの100万行をcategory条件で10万行へ絞ってからjoinする。
  右側が予算内に収まり既存のLEFT融合へ接続できたため、保持側build＋
  matched bitmapの新方式は追加しない。未一致行のNULL拡張は既存経路を保つ。
  ON／WHERE、COUNT(s.id)／COUNT(*)を区別する。索引あり<158.376 ms等を確認。
- [x] **F. 追加索引なしの非キー検索で既存主キー順とLIMITを活かす（4件＋tail4件）。**
  no_filterで排除されるordered index＋residual候補を検討し、小規模scanの
  並列起動・MVCC・queue費用を較正。UPDATEで失効する物理順を仮定しない。
  補足の単発診断では20倍差が再現しないため並列負荷を切り分ける。
  16接続>8,547 query/s、p50<1.824 msを含め全指標を確認する。
- [ ] **G. 書き込みの遅い試行を再現し、同期commitとworker待機を改善する（6件＋tail2件）。**
  update／hot_update／transfer／insert_deleteを、既存group commit・
  WAL同期・checkpoint・queueに分けて測る。hot_update単一接続の遅さを
  書き手間競合と決めつけない。計測で支配要因を確定後に修正し、
  同期commit・依存LSN・NoWait・session順序・切断abort／shutdown drainを保つ。
  coordinatorへの耐久化応答移管・集約、証明済みLSNによる待機省略、
  空commit除去、複文エラー前のSELECTへの依存バリアを実装済み。
  G/Hの長時間80試行を完了し、Gは8指標中3件通過。未達は索引なしUPDATE/16の
  p50（3.836 ms、旧PG比14.36倍）とhot_update/1のTPS・p50・p95・p99の4件。
  単一writerはfdatasync待ちが支配し、中央値264.833 TPS／p50 3.652 ms。
- [x] **H. HTAPの分析側を単独／3 writer／15 writerで比較して改善する（4件）。**
  SUM(a.balance)の同じSQLを固定。DとG適用後の残差からscheduler、
  MVCC version chain／GC、spillを調整する。3秒窓の0.667 query/sの
  逆数を中央値遅延とみなさない。分析>3.03～3.90 query/s、writer性能と残高保存を確認。
  readerは5.083～5.183 query/sで4目標を通過し、全試行の残高保存も成立。
  writer全条件の無退行は未確認。索引なし3 writerは旧2,548→今回1,026 TPSで、
  測定窓・履歴・I/Oの差を揃えた前後比較を共通ゲートに残す。

実施記録: [A–Hの変更・最終追測](docs/postgresql_gap_results_2026-10-29.md)。
A〜Fの29指標とHの4指標は回帰と両PG基準を通過。Gを含め主対象32/35件、
補足4/6件、合計36/41件が通過し、未測定はない。今回PG比だけでは全41件が
10倍未満だが、Gの旧PG固定目標を達成扱いにしない。
最終全4,090テストは失敗0（既存skip 3件）、SQL oracle 181秒277入力は不一致0、
解析・値検証27テストとlayeringを通過。OLAPは3製品462測定で結果一致・反復安定。

Gの残作業（上から実施）:

- [ ] 同じバイナリ／長さ／fixture履歴／I/O条件で対照試験を交互実施し、
  単一writerとHTAPの3 writerでコード由来の差と同期I/Oの期間差を分ける。
- [ ] WALの物理予約・再利用と論理終端を分離する設計を詰める。
  KEEP_SIZEの同期実験は安定改善を示さなかった。単なる拡張やfile sizeの
  DurableLSN流用では解決扱いにせず、同期commit保証を維持する。
- [ ] 採用する変更に対し再開・torn tail・ENOSPC/EIO・segment境界・
  checkpoint/recycle・D4応答バリアの回帰を追加し、長時間G/Hを再測定する。
- [ ] UPDATE/16 p50 <2.671 ms、hot_update/1 >758.600 TPS、
  p50 <1.243 ms・p95 <1.538 ms・p99 <1.928 msの旧PG固定目標を再判定する。

追加の残差: Aの原目標9件は通過したが、100万行COUNTは索引なし25.920／あり51.272 ms、
DISTINCTは46.179／63.002 ms。無条件集計の走査費用とキー単位集計には改善余地がある。

共通ゲート: 元データの値と同時期のPostgreSQLの両方で不利倍率10未満、
他のワークロード・メモリ上限・中断率に回帰なし。変更層の回帰、全ctest、
式差分、layering、format、optimizer／executor変更後のSQL oracle fuzzを実行する。

---

# 3製品比較からの改善 TODO（2026-10-28）

[実測レポート](docs/olap_oltp_comparison_2026-10-28.md)から、正確性を先に、
その後は書き込み・結合・走査・SQL機能の順で進める。既存の測定値は保存し、
修正後の結果は別ディレクトリへ記録する。完了は実装だけでなく回帰テストと
対象ワークロードの再測定まで確認してからチェックする。

- [x] **1. TPC-H Q6の結果を一致させる（最優先）。**
  SF 0.01でPostgreSQL／MySQLは`1787210.8440999957`、tinylambはNULL。
  再現を小さくし、式評価・射影・並列worker-filter・ページ走査を切り分ける。
  原因の層に回帰テストを追加し、初回／キャッシュ再利用／並列経路で一致を
  検証する。`differential_test`とTPC-H全22クエリも再確認する。
  完了: DOUBLE列への整数上限を並列走査のraw整数peekにしていた。
  `ScanAlternatives`を既存の型検査付き`BuildIntegerPeeks`へ統一。
  optimizer／SQL並列・キャッシュ再利用の回帰を追加。全4061テスト
  （3件skip）、式差分16件、SQL oracle 180秒270入力、layering通過。
  [修正後の実測](benchmark/comparison/results/2026-10-28-q6-fixed/results.jsonl)
  はQ6含むTPC-H 66条件・共通OLAP 54条件すべて一致。
- [x] **2. INSERT＋DELETEの並列実行で生じる不要な競合・待機を解消する。**
  異なるキーでも16接続・10秒で1.1成功TPS、失敗率88.70%。write intent、
  索引構造変更、undo予約の待機を切り分け、最小の複数セッション回帰を追加。
  1／4／16接続で再測定し、成功TPS・中断率・ロールバック後の状態を確認する。
  完了: NoWait時の索引予約drainも即時判定にし、DML全走査にロック費用を
  加算。自分がintentを持つ完全一致unique点検索は、他キー削除による
  snapshot用全走査fallbackを避ける（read gate下で索引上の位置を確認）。
  undo予約・通常snapshot読取は維持。16セッション回帰と予約容量の回帰通過。
  [10秒×3試行の追測](benchmark/comparison/results/2026-10-28-churn-fixed/results.jsonl)
  は1／4／16接続すべて中断0・不変条件成立、中央値466.7／453.2／435.5 TPS。
  書き込みの単一スレッド制約は次項で扱う。
- [x] **3. SQLサーバの書き込み実行とcommit待機をI/Oループから分離する。**
  セッション内の実行順序とTransactionContextの寿命を保った実行方式にする。
  同期commit、切断、エラー後のtransaction状態、終了時のdrainを検証し、
  更新・送金・検索＋送金のスケーリングを再測定する。永続化は無効化しない。
  完了: 通常DML／transaction制御を共有ワーカへ移し、contextの所有権を
  メッセージ単位で往復。1接続1タスク、切断時abort、終了時drain、
  pipelineの順序を回帰で確認（サーバ既存46件＋追加4件通過）。
  TEMP／UDFとDDLはI/O側に保持。catalog変更は他の開いたblock／workerが
  ある間55006で再試行を要求し、未対応のonline index構築を防ぐ。
  [4負荷×1／4／16接続×3試行](benchmark/comparison/results/2026-10-28-session-workers/results.jsonl)
  の36条件で不変条件成立。16接続の中央値はupdate 1115.2、transfer 1217.8、
  read_write 15275.4、insert_delete 2124.4 TPS。同一行の競合による中断は
  残るため成功TPSと失敗率を併記する。fsync設定は維持。
- [x] **4. 結合集計の構築側とメモリ使用を改善する。**
  100万行salesと1万行accountsの内部結合で大きい側を構築する経路を調べ、
  小さい側の構築と必要列の保持を実装する。外部結合のNULL拡張を保つ。
  64／256 MiBで結果・実行形状・spillを確認し、結合集計／外部結合を再測定。
  完了: relational IRのinner joinで小さい構築側の選択をHybrid判定より前へ。
  probeだけのspillはForEachRowで読み、既存の必要列projectionを維持する。
  hash索引の予算chargeと実測build行数／spill probe行数も追加。
  INT／複合INT／文字列キー・NULL・列順の回帰通過。64／256 MiBの100万行
  [診断](benchmark/comparison/results/2026-10-28-join-64/results.jsonl)で
  build=1万行、probe=100万行を確認。両予算の結合集計／外部結合は
  [PostgreSQL参照結果と一致](benchmark/comparison/results/profile-correctness.json)。
  入力と結合結果のspillは残り、spill下の集計融合は今後の改善余地。
- [x] **5. 単純COUNT／SUMとTop Nのボトルネックを個別に改善する。**
  走査・行の復元・集計・ソート・結果転送の費用を切り分け、該当経路を改善。
  NULL・空入力・同値キー・LIMIT／OFFSETの意味を回帰で固定し、
  1万／10万／100万行の速度と結果を再確認する。
  完了: scalar集計の直接列projectionをscanへ渡し、COUNT／単純INT集計の
  batch受け渡しに不要な16worker同期を省いた。DOUBLEは既存の並列mergeを維持。
  Top Nの直接列キーはbatchで評価し、採用される候補だけ行を復元する。
  NULL／空入力、同値キーの安定性、offset、payload／行位置の回帰通過。
  [3スケールの追測](benchmark/comparison/results/2026-10-28-scalar-final/results.jsonl)
  9条件はPostgreSQL参照結果と一致。100万行の診断中央値はCOUNT 25.8、
  SUM 45.4、Top 100 58.8 ms。別時刻の測定と環境変動を含むため、速度比は参考。
- [x] **6. SQL経由の索引作成と索引ありの比較を追加する。**
  CREATE INDEXを既存Catalog／Tableの索引作成へ接続し、失敗時のatomicityと
  作成後DML・再オープンを検証。3製品に等価な二次索引を作る比較条件を追加し、
  非キー検索とTPC-Hを索引なしの既存結果から分離して再測定する。
  完了: CREATE [UNIQUE] INDEX [IF NOT EXISTS]＋昇順列／STORINGを
  GoogleSQL AST→Statement→Database::CreateIndexへ接続。未対応のDESC／
  式／SEARCH／VECTOR／OPTIONS／OR REPLACEは拒否。重複・不明列、build失敗、
  abort後のcatalog、作成後DML、再オープンを回帰で確認。
  実測から派生したB+Treeの境界バグも修正: UpdateSlotImplの圧縮後の判定が
  既存行にも余分なslotを数え、675番目のキーを失っていた。赤→緑の回帰を追加。
  [索引あり追測](benchmark/comparison/results/2026-10-28-indexed-fixed/results.jsonl)
  は共通OLAP 15条件、TPC-H 66条件、非キー検索6試行すべて一致・中断0。
  失敗した途中実験は保存し、成功した追測と区別する。

共通完了確認: 対象層のテスト、`ctest`、`differential_test`、
`python3 scripts/check_layering.py`、変更C++のclang-format。
optimizer／executor／expressionの変更後はSQL oracle fuzzerも実行する。
今回の最終確認: ctest 4071項目（3件skip、追加テスト修正後の失敗分再実行を含め通過）、
B+Tree 71件、式差分16件、SQL oracle 180秒・256入力、layering／format通過。
[検証ログ](benchmark/comparison/results/2026-10-28-validation/)を保存。
修正後の追加索引なしOLAPも3スケール×18クエリ×3製品の162条件すべて一致した。
既存利用者変更22ファイルは原測定時の差分と完全一致を確認した。
長時間の定常負荷、キャッシュ超過データ、TPC-C／TPC-DSの共通実装は、
上記修正後の比較拡張として別途設計する。

---

# Cascade Optimizer 高速化 TODO（2026-09-16 策定）

前回考察（chat: cascade ルール追加提案）の推奨着手順を、他エージェントへ
引き継げる明細として分解したもの。現状の土台は以下を参照：

- `docs/cascades_optimizer.md`（D5 監査表・探索フロー・健全性不変条件）
- `docs/optimizer_todo.md`（P0〜P8 の到達/未達マップ）
- `docs/optimizer_improvements.md`（Phase 8/9 の経路地図 M4〜M9）
- `docs/executor_todo.md`（物理オペレータ側の未達）
- `docs/opt/part2-rules/logical/*.md`（各 Rule の現状メモ・監査指摘 A〜E）

全体不変条件（破ったら即 revert）：

1. `Memo::NewJoin` を経由しない結合合成は禁止（conjunct の二重適用/欠落）。
2. `LimitPlan`/`TopNPlan` は required ordering を満たす子の上にしか畳まない（D6）。
3. 外側結合への pushdown は null-rejection 解析が先。
4. `Selection(Limit)` と `Limit(Selection)` は一般に非等価。交換しない。
5. 論理規則は memo groups + 論理 operators のみに触れる。
   catalog/executor/storage 型は実装規則 + `RuleContext` の背後に隔離。
6. 変更後は `ctest` + `python3 scripts/check_layering.py`（exit 0）+
   `expression/differential_test` を通す。触った `*.hpp/*.cpp` は
   `clang-format -i`。

---

## 1. FK + clustering factor（実装済み 2026-09-16）（前提工事：封印規則の解禁と誤選択防止）

狙い：D5 監査表で Disabled の `push_down_limit_through_join` /
`topn_push_through_inner_join` を安全に復活させ、index vs full-scan の
誤選択（非クラスタ化 index のランダム I/O 過小評価）を直す。
波及が最大なので最初にやる。

- [x] 1a. `ColumnStats` に物理順序相関 `correlation_`（0..1）を追加
  - 実装済み (2026-09-16): `table/table_statistics.{hpp,cpp}`、
    `common/decoder.hpp` に `AtEnd()` を追加し旧 payload は 1.0 既定で読取。
    `operator==` は既定メンバ比較に追随、`ostream` 表示は不変。
    回帰: `table_statistics_test` の `Correlation_*` 2件
  - `table/table_statistics.hpp`（`ColumnStats` に `double correlation_{1.0}`
    + `Correlation()` アクセサ）
  - `table/table_statistics.cpp`（`Update()` の reservoir sampling 時に
    (physical_pos, sorted_pos) の順序相関を推定。`Encoder/Decoder` 永続化、
    `operator==`・`operator<<`・`Assign`・`Duplicate`・`operator*=` の追随）
  - 互換性注意：永続化フォーマットを変える場合は旧 blob 読取の
    downgrade path（`table/README.md` の legacy min/max upgrade が前例）を
    用意し、`table_statistics_test` に round-trip 回帰を追加
- [x] 1b. range scan の index コストに clustering penalty を適用
  - `plan/implementation_rules.cpp` の `index_scan` 規則内
    （`BuildIndexScan` 候補の `local_cost` 計算箇所。
    先頭キー equality-prefix のみの point lookup は対象外、
    range を含む候補のみ `local_cost *= (1 + k*(1-correlation))`。
    `k` は `CalibrateOperatorCost`（`plan/cascades.cpp:10989`）側の較正係数と
    並べ、テストで full/index 選択が flip することを pin）
  - `optimizer_cardinality_costing_high_expectations.test` 系に期待値追加
- [x] 1c. `topn_push_through_proven_one_to_one_join` 論理規則（kForeign ゲート版）
  - 実装名は FK＋snapshot 両対応のため `topn_push_through_proven_one_to_one_join`
    （`plan/cascades.cpp` の DISABLED コメント直下）。発火条件は D5 表の新規行を参照
  - `plan/cascades.cpp:9347` の DISABLED コメント直下に新規則を追加。
    発火条件：`TopN(Join(L,R))`（inner のみ）＋ 右結合キーが
    `IsUniqueOn` ＋ 左結合列の schema 制約が `Constraint::kForeign` で
    参照先が右 relation と一致（`foreign_key_outer_join_elimination`
    `plan/cascades.cpp:5402` の参照先照合ロジックを再利用）＋
    `!with_ties` ＋ 有限 limit。書換えは `Join(TopN(L'), R)` ではなく
    D5 表の反例を避けるため **1:1 証明が揃う場合のみ**
    左側へ `TopN(limit+offset)` を押す形（`push_limit_through_left_join`
    `plan/cascades.cpp:5758` が前例。右キー uniqueness 証明付き左押込み）。
  - 反例テスト：`TopNPushThroughInnerJoinOnForeignKey`（既存）と対になる
    肯定テスト `TopNPushThroughProvenOneToOneJoinWithForeignKey` を
    `plan/cascades_test.cpp` に追加。D5 監査表へ行追加
- [x] 1d. snapshot による参照整合性証明（DDL なしで今日速くする本体）
  - `plan/cascades.hpp` の `Memo` に `proven_one_to_one_`（relation-pair set）
    + `MarkProvenOneToOne(a,b)` / `IsProvenOneToOne(a,b)` を追加
    （純データ構造の拡張であり層違反ではない）
  - `plan/optimizer.cpp` の QueryData→Memo 翻訳時（`TryEliminateUnusedOuterJoin`
    の `kMaxJoinEliminationProofRows = 1<<16` snapshot 走査が前例
    `plan/optimizer.cpp:406`）に、TopN/LIMIT 直下の単一 inner join について
    左外部キーの全 non-NULL 値が右 unique 列に存在することを証明し、
    通れば `MarkProvenOneToOne`
  - 1c 規則の発火条件を「kForeign **または** `IsProvenOneToOne`」に一般化。
    論理規則は依然 memo のみに触れる（snapshot I/O は optimizer.cpp 側）ため
    「論理パターンは memo のみ」の規律を維持
  - `optimizer_test.cpp` に end-to-end（TPC-H Q3/Q10 型 ORDER BY+LIMIT が
    TopN push で計画されること）を追加

残件（実装済み 2026-09-19）：`FOREIGN KEY` DDL（pinned parser は
`ForeignKey`／`ForeignKeyColumnAttribute` ノードを出すことを確認。
`VisitCreate` で両形式を受理し、単一列 FK を `Constraint(kForeign,
Value("parent(col)"))` 正準形で列スロットに格納＋`CreateTableStatement::
ForeignKeys()` に定義保持。`Database::CreateTable` が親表・親列・型一致を
検証（自己参照は作成中スキーマで解決）。`INSERT`/`UPDATE` 子側・
`DELETE`/`UPDATE` 親側を NO ACTION 強制（`executor/foreign_key.*`＋
`Insert`/`Update`/`DeleteExecutor` 配線、自己参照は文内書込を
追跡）。`DropTable` は被参照表を拒否。CASCADE/SET NULL/SET DEFAULT・
MATCH FULL/PARTIAL・複合 FK は明確に reject。回帰:
`query/foreign_key_test.cpp` 14件）

## 2. CTE predicate pushdown（M4 の行削減）（実装済み 2026-09-16）

狙い：`MaterializeCtes`（`query/sql_engine.cpp:4380`）の共有 eager cell が
外側 WHERE を被らず全行実体化している問題を解消。1024行予算
（`kMaxMaterializedCteRows`）の浪費と cell scan の I/O を削減。

- [x] 2a. 全サイト弱化による cell 絞り込み（単一サイトは特例として包含）
  - 実装済み: `TryNarrowCteBody`（`query/sql_engine.cpp`）。単一参照限定ではなく
    全サイトのフィルタの論理帰結（列毎の下限min・上限max・全会一致の等価）のみ注入。
    外側フィルタは残置。GROUP BY/DISTINCT/LIMIT/QUALIFY/WITH TIES/window/派生・
    star・複数表 body は対象外。矛盾サイトは除外、型不一致等価は列無効化
  - `MaterializeCtes` の body 実行直前（fresh `PrepareStatement` 前）に
    `TryPushCteFilterIntoBody(body, outer_where, cte_alias)` を挿入
  - ゲート：body が単一 plain base table・行保存
    （`InlineSingleUseCtes` `query/sql_engine.cpp:2761` の body 判定を再利用）
    ＋ CTE 参照サイトが単一（`CountCteReferencesInStatement == 1`。
    共有 cell への site-specific filter 混入を防止）＋ 外側 conjunct が
    `alias.out_col OP const`（`OP ∈ {=,<,<=,>,>=}`、const immutable、
    volatile/subquery なし）＋ `out_col` が body SELECT の単一 base 列の
    素通し（`base_col [AS out_col]` のみ。式・集約は不可）
  - 注入は body WHERE への AND のみ。**外側フィルタは残す**
    （residual 再評価＝安全網。単一表 scan filter の exactness 規約
    `docs/cascades_optimizer.md:28` に抵触させない）
  - 三値論理：`NULL` 比較は UNKNOWN で両側とも落とすため等価。
    `IS NULL`/`IS NOT NULL` 形は v1 対象外
- [x] 2b. 回帰テスト（`query/query_test.cpp` 系）
  - `SqlEngineMultiSiteCtePushesWeakenedFilter`（弱化＋全会一致等価＋Values リーフ）、
    `SqlEngineWeakenedCteFilterFitsCellBudget`（発火証明：1024行予算を超過するbodyが
    絞り込みで lift する）、`SqlEngineMultiUseCteCellIgnoresSiteSpecificFilter`
    （共有 cell 汚染防止）、`SqlEngineGroupedCteBodyKeepsGroupsUnderFilter`（集約境界）
  - `CteFilterPushdownNarrowsMaterializedCell`（plan ダンプ/EXPLAIN で
    body 側に範囲が付くこと＋結果一致）
  - 複数参照 CTE では発火しないこと（共有 cell 汚染防止の否条件）
  - `OptimizerAndRelationalPathsAgree` 監査に CTE+フィルタ形を追加

残件：複数サイト共通フィルタの intersection push、CTE推移的多段 push、
`cte_predicate_propagation`（等価クラス経由の join 定数伝播。M4 葉待ち）
—— このうち**複数サイト weakening は実装済み**（`TryNarrowCteBody` が全サイトの
下限min・上限max・全会一致等価を注入）。2026-10-06 に上限方向の融合が
最弱（最大値）でなく最強（最小値）を採る silent wrong result を修正
（狭いサイトの上限が共有 cell を潰し、広いサイトが必要な行を失う。
回帰 `SqlEngineMultiSiteCteUpperBoundStaysWeakest`）。

## 3. LATERAL decorrelation（per-row 再実行の排除）（実装済み 2026-09-16）

狙い：`has_lateral` → `ExecuteUnnestSelect`/relational fallback
（`query/sql_engine.cpp:5558`）の `O(N*M)` を等価相関の hash join 化で
`O(N+M)` に。M5 葉化の最小スライス。

- [x] 3a. 単一表・等価相関 LATERAL の statement-level decorrelation
  - 実装済み: `DecorrelateSingleTableLaterals`（`query/sql_engine.cpp`、
    `FlattenDerivedSources` 直後）。相関等値を ON へ移動（派生別名へ再束縛、
    非出力キーは射影拡張）＋ lateral clear ＋ kCross→kInner 昇格（LEFT 維持）＋
    M5 flatten へ委譲。volatile/集約/非等価/複数表 inner・RIGHT/FULL・star は対象外
  - `FlattenDerivedSources`（`query/sql_engine.cpp:3647`）の直後に
    `DecorrelateSingleTableLateral(outer)` を挿入（routing 前。
    `InlineSingleUseCtes→MaterializeCtes→LiftRecursiveCtes→FlattenDerivedSources`
    の呼出順 `query/sql_engine.cpp:5258` を崩さない）
  - ゲート（`FlattenOneDerivedSource` `query/sql_engine.cpp:3399` の判定を準用）：
    site が `is_lateral` ＋ inner が単一 plain base table・行保存
    （group/agg/distinct/limit/window/setop/star/CTE なし）＋ inner WHERE の
    conjunct にちょうど1つの `outer_col = inner_col` 等価相関があり、
    それ以外の inner 式（SELECT/WHERE/ORDER）に outer 参照なし
    （scope 外参照検出は `DerivedSourceIsLocal` 系ヘルパと
    `CanUseDecorrelatedSubqueryOptimizer` `query/sql_engine.cpp:278` の
    相関キー検査を再利用）＋ outer が grouped/window でない
  - 書換え：相関等式を inner WHERE から outer の `join_condition`
    （`SelectSource::join_type = kInner`、cross からの昇格）へ移動し、
    inner 式を merged scope へ再束縛（`RebindInnerExpression` 前例あり
    `query/sql_engine.cpp:3531`）、`is_lateral` を clear。
    移動（copy ではない）＋ outer 側に residual を残さないのは
    join_condition が等価に評価されるため。非等価相関は対象外
- [x] 3b. 回帰テスト
  - `SqlEngineSingleTableLateralDecorrelatesToJoin`（HashJoin 化＋Apply 消滅＋行一致）、
    `SqlEngineLeftLateralDecorrelatesWithNullPadding`（NULL pad 維持）、
    `SqlEngineNonEquiAndAggregateLateralStayCorrect`（非等価・集約は旧経路で一致）。
    既存 `LateralJoinExpansion` は decorrelated 期待値へ更新（行アサートは等価性の証拠として維持）
  - `LateralSingleTableEqualityDecorrelates`（結果一致＋EXPLAIN が
    hash join になること）
  - 否条件：複数相関・非等価相関・集約 inner・outer 参照残存は
    relational 維持（`PostRewriteNeedsRelational` が true のまま）

残件：複数表 LATERAL、LATERAL + 集約のデコリレーション（旧 `decorrelate_aggregate_apply` は撤去、履歴は docs/optimizer_rule_audit.md）
対応（memo 葉化 M5 待ち。一般形は `docs/optimizer_todo.md:491` のまま）

## 4. 論理規則3点（OR-to-UNION / 集約 duality / TopN-through-Union）（実装済み 2026-09-16）

いずれも `plan/cascades.cpp:RuleSet::Default` への additive rule
（`memo.AddExpression(group, ...)` のみ。既存式の削除・置換なし）＋
`plan/cascades_test.cpp` の肯定/反例テスト＋必要なら D5 行追加。

- [x] 4a. `or_to_union`（選言の union 化）
  - 実装済み: `or_to_union`（`plan/cascades.cpp`）。2選言・単一relation・安定述語のみ
    `kUnion`（DISTINCT）代替を追加。同列定数等式は `or_to_in` に譲る。分岐タグは選言内容で
    アドレス指定（D1）。`OrToUnion*` 3件
  - パターン `Selection(OR(d1,d2))`。ゲート：ちょうど2 disjuncts＋各
    disjunct は conjunct 単位（AND 可、そのまま branch predicate 化）＋
    グループが単一 relation＋両 disjunct 非 volatile・subquery なし
    （`distribute_or_over_and_budgeted`
    `expression/rewrite.cpp:3477` の `ExpressionCannotThrow` /
    `SafeToReduceEvaluationCount` 検査を再利用）＋派生タグ
    `or_union_branch:` での循環防止（`union_all_push_limit`
    `plan/cascades.cpp:2997` の `union-limit-setop:` ガードが前例）
  - 書換え：`EnsureDerivedGroup(rels, "or_union_branch:0/1")` に
    `Selection(d_i)` を載せ、同一 group へ `kUnion`（DISTINCT。
    overlap があっても重複排除で等価。Selection は TRUE のみ保持するので
    三値論理でも `FALSE OR NULL`≡両枝で落とす、で一致）を追加。
    memo 検証（`plan/cascades.cpp:1442`：children relations の union が
    group と一致）は同-relation children で充足
  - v1 は `kUnion` 固定（disjoint 証明が要る `UnionAll` 化は将来。
    同一列の定数等式 OR は既存 `or_to_in` が担当のため重複回避）。
    反例テスト：volatile 述語・複数 relation・3 disjunct で不発
- [x] 4b. 集約 duality：`distinct_eager_over_join`＋lazy 共存保証
  - 実装済み: `distinct_eager_over_join`（inner 両側＋LEFT 保存側、他側キー unique
    ゲート。 containment 不要の証明を D5 表に記録）。純粋 lazy 規則は不要と確定：
    eager が additive なため B&B が自動選択する。`DistinctEagerOverJoin*` 2件＋
    `EagerAndLazyAggregationCoexistForCostChoice`（両形状の共存 pin）で保証
  - 背景：`lazy_aggregation` は eager の双対だが、eager が additive
    （元形状 `Agg(Join)` は memo に残り B&B cost が選ぶ）なため、
    純粋な「遅延配置規則」は空転する。真の不足は (i) DISTINCT 版 eager が
    ないこと (ii) selective-join 時に cost が join-first を選ぶことの保証
  - 規則 `distinct_eager_over_join`：パターン `Distinct(Join(L,R))`、
    ゲート inner/left のみ＋右結合キーが `IsUniqueOn`
    （`RightSideJoinKeysAreUnique` `plan/cascades.cpp:137` を再利用）＋
    DISTINCT 対象列が全て左 relations 由来。書換え
    `Join(Distinct(L'), R)`（`eager_agg_left/eager_agg_join`
    `plan/cascades.cpp:6642` と同型の2段化ではなく DISTINCT は単段。
    outer は LEFT のみ、RIGHT/FULL は対象外）
  - テスト：肯定（`SELECT DISTINCT l.* FROM l JOIN r` で narrow 側 DISTINCT
    が選ばれ得ること）＋反例（右キー非 unique・FULL・右列参照で不発）。
    さらに `LazyAggregationPrefersJoinFirstWhenSelective`（eager 代替が
    存在しても selective join では単段 agg が勝つ cost 順序テスト）で
    lazy 側を pin
- [x] 4c. `topn_push_through_union_all`（順序付き版）
  - 実装済み: `topn_push_through_union_all`（`plan/cascades.cpp`）。各枝に
    `TopN(limit+offset)`＋親 TopN 維持。`!with_ties`・同形枝 cap 済み検査・枝 GroupId
    タグ（D1）。既存 limit 系2規則は不変。`TopNPushThroughUnionAll*` 2件
  - 既存 `union_all_push_limit` / `push_limit_through_union_all`
    （`plan/cascades.cpp:2997/5212`、無順序 LIMIT の branch cap。重複登録の
    統合は `docs/optimizer_todo.md:1054` の監査 C 項として別途）は触らず、
    `TopN(keys,limit,offset)` over `UnionAll` の新規則を追加
  - ゲート：`!with_ties`（ties 境界は行数保証が崩れる）＋有限 limit＋
    各枝が既に `TopN(count<=limit+offset)` を持たないこと
    （`already_limited` 検査 `plan/cascades.cpp:5178` を再利用）＋
    派生タグ `topn_union_child:` / 合流 `topn_union_pushed`
    （既存 limit 系タグと衝突させない）
  - 書換え：各枝に `TopN(keys, limit+offset, 0)`、親に元 `TopN` を維持。
    親 TopN が順序を確立するため D6 適合。各枝 TopN は
    `implement_set_operation`（`plan/implementation_rules.cpp:2428`）の
    `MergeAppend` 経路（順序要求時の子 Sort 強制挿入）と相乗
  - テスト：肯定（各枝 TopN＋親 TopN の形状）＋反例（with_ties・無限・
    二重適用防止）。`union_all_push_limit` との相互作用テスト
    （両規則が共存しても `ExploreTwiceReachesSameFixpoint` が保たれる）

## 5. 並列の実装規則分岐（DOP のコスト化・第一級化）（実装済み 2026-09-16）

狙い：`kParallelScanMinRows/kParallelAggregationMinRows = 8192`
（`plan/parallel_thresholds.hpp`）の閾値駆動は scan/agg のみ。
実行器は `SharedBuildParallelHashJoin`（`executor/parallel_hash_join.*`）、
`ParallelMergeJoin`（`executor/parallel_merge_join.*`）を持ちながら
`ProductPlan::EmitExecutor` / `MergeJoinPlan::EmitExecutor`
（`executor/relational_factory.cpp:453/470`）が常に逐次形を出す。

- [x] 5a. join 並列閾値を `parallel_thresholds.hpp` に追加
  - 実装済み: `kParallelHashJoinMinRows` / `kParallelMergeJoinMinRows`（=8192、scan/agg と同根拠）
  - `kParallelHashJoinMinRows` / `kParallelMergeJoinMinRows`
    （初期値 8192。scan/agg と同一根拠：スレッド起動+morsel handoff を
    ペイする規模。コメントに根拠を明記）
- [x] 5b. factory の自動選択（scan/agg と同型）
  - 実装済み: `executor/relational_factory.cpp`。inner equi hash は
    `SharedBuildParallelHashJoin`（null-safe キー除外—並列側に符号化なし）、
    inner 無 residual merge は `ParallelMergeJoin`。worker は `min(16, hw)`（集約と同型）
  - `ProductPlan::EmitExecutor`：inner equi hash join で
    `left_src_->EmitRowCount() + right_src_->EmitRowCount()` が閾値以上なら
    `SharedBuildParallelHashJoin`（worker 数は
    `relational_factory.cpp:327` の `min(16, hardware_concurrency)` が前例。
    コンストラクタ引数は `parallel_hash_join.hpp:72` に合わせる。
    semi/anti/outer/null-aware・residual 付き・index join は v1 対象外で
    逐次維持）
  - `MergeJoinPlan::EmitExecutor`：両側行数合計が閾値以上なら
    `ParallelMergeJoin`（`parallel_merge_join.hpp:31`。outer/semi/anti・
    residual 付きは対象外）
  - `Dump`/`EXPLAIN` 名が並列形を区別すること
    （`sort.cpp:668` の `"ParallelSort (N workers)"` が前例）
- [x] 5c. コスト側の扱い→意図的に不変（判断記録）
  - scan/agg の前例通り emit 時選択のみとし `local_cost` は変えない。
    並列化は全代替をほぼ一様に速くするため plan 選択は変わらず、コスト変更は
    golden churn のみを生む。DOP 明示配分・Exchange 実装は分散枠に残す（従来通り）
  - `implementation_rules.cpp` の hash/merge join `local_cost` に
    閾値超過時の speedup divisor（`CalibrateOperatorCost`
    `plan/cascades.cpp:10989` の hash `L*1.2+R*1.5` / merge `*1.1` と
    同オーダーで整合。並列効率は Amdahl 的に `1/min(workers, morsels)` の
    保守形）を適用し、逐次/並列の cost 順序が flip することを
    `optimizer_test.cpp` で pin（`ParallelScanEmittedForLargeAnalyzedTable`
    `plan/optimizer_test.cpp:3169` が前例）
  - spill 連携：`EstimateMemorySpillCost`（`plan/cascades.cpp:10973`）の
    hybrid 罰則と二重割引にならないよう、spill 支配時は speedup を適用しない
- [x] 5d. 回帰テスト
  - `executor_test.cpp` 4件：大入力で `SharedBuildParallelHashJoin` /
    `ParallelMergeJoin`（各16000行の結果一致）、小入力で逐次維持
  - `ParallelHashJoinEmittedForLargeInputs` /
    `ParallelMergeJoinEmittedForLargeInputs`（大小両側の閾値テスト＋
    逐列一致）＋ semi/outer が逐次のままの否条件

残件（分散枠）：明示 DOP 配分・`Exchange` 実装（今は no-op 4規則
`plan/implementation_rules.cpp:2973`）・colocated/broadcast の cost 比較は
単一ノードでは等価のため対象外（`docs/optimizer_todo.md:920` の整理通り）

---

## 作業順序（このファイルの上から順に実装する）

1 → 2 → 3 → 4a → 4b → 4c → 5 の順。各項目は
「実装 → 対象層テスト → `ctest` 該当 suite → `check_layering.py`」の
小刻みコミット。`docs/cascades_optimizer.md` の D5 表・
`docs/optimizer_todo.md` の `[ ]→[x]` は同じ変更で更新する
（`docs/AGENTS.md` の「doc と振る舞いの同時更新」規則）。
