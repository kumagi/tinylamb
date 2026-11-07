# PostgreSQLとの2倍以上の差を縮める計画（2026-10-29・第6巡）

**2026-10-30完了**: [最終レポート](postgresql_twofold_results_2026-10-30.md)。
C34の標準フル測定は13残差を残したが、同一バイナリの長窓とPG状態をそろえた
再測定で元161対象・全456比較が今回PG比2倍以内、最大1.938倍。旧PG固定比は
149/161。以下は実装計画と中間候補の時系列であり、過去の通過／未達を最終結果と
混同しない。

第5巡の同一バイナリ（16a67785…）を基準とし、456比較のうちPGが2倍以上速かった161指標を対象とする。対象にはp95/p99、HTAPの役割別、合計の重複した指標も含む。取りこぼしを防ぐため全件を保存し、同じ原因の実装はまとめる。

ユーザー確認済みの目標は時間tiny/PG ≤2、成功TPS PG/tiny ≤2。2026-10-30の追加確認により、達成判定は今回同条件で再測定したPGを基準とする。前回PG固定値との比較も参考として保存し、混同しない。

## 実行順

| 順 | 原因群 | 指標数 | 実装方針と確認 |
| --- | --- | --- | --- |
| A | 走査・型付き集計 | 14 | PAXの列batchとMVCC可視性判定を調べ、行ごとのboxing・queue転送・集計を減らす。COUNT/SUM/数式、NULL、古いsnapshot、途中エラーを検証。 |
| B | 結合・副問い合わせのstream化 | 39 | 1,010,000行のmaterializeとspillを避け、小さい側をbuildしてprobeと集約を融合する。多重度、NULL、outer join、残差、古いsnapshot、予算不足のfallbackを検証。 |
| C | 小規模SQL・索引の固定費 | 10 | 再計画、バインド、索引カーソル、Top Nの停止位置を計測し共有可能な不変成果物を再利用。schema epoch、LIMIT、thread-local状態、cache退役の契約を保つ。 |
| D | SQL dispatch・transaction固定費 | 35 | ネットワーク→worker→event loopの往復とSQL実行・BEGIN/ROLLBACK費用を分離する。短く安全な処理のdispatchを削り、protocol・セッション状態・例外処理を検証。 |
| E | 同期WAL・混合負荷 | 63 | fdatasync本体と周期・group化・返信待ちを分離し、durable replyとWAL-before-dataを保ったまま余分な同期・待機を減らす。短期窓に加え長時間・交互順・同じ履歴で確認。 |

## 合格条件

- 再測定PG比で各対象の不利倍率を判定する。元PG固定値の倍率も参考として残す。中央値とtail、TPSは独立に判定する。
- 結果・反復結果一致、OLTP不変条件、HTAP readerの保存則を維持する。競合は成功へ数えず失敗率を併記する。
- 同期commit・メモリ予算・CPU数・SQL・データを性能達成のために緩めない。設定変更を診断する場合は別条件で保存する。
- 新規の意味論回帰テスト、全ctest、expression differential、SQL oracle、layering、formatを通す。
- 最終同一バイナリで全対象と周辺の既改善条件を再測定する。短期書き込みの大きな変動は長期計測を別に行い、未達を未達として残す。

## 実装と中間検証（最終判定前）

Aの型付き走査と集計、Bの整数結合と集計の融合・scalar subqueryの1回評価、Cの計画再利用・索引上限の再バインド・小さいpeer groupのソート省略、Dのセッション計画のworker間移動と応答処理を実装した。HTAPでは同時要求数に応じてクエリ内並列数を抑える処理も追加し、readerが全CPUを使ってwriterを圧迫する状況を調べている。

中間の同一バイナリによるOLAP全474測定は結果・反復結果が一致した（`2026-10-29-gap6-olap-a`）。これは後続修正前の検証であり、最終達成数には使わない。100万行の単独診断ではCOUNT約25.2→2.2 ms、SUM約46.3→2.6 ms、数式約40.7→15.4 ms、GROUP BY付き結合約246→6.0 ms、HTAP reader約198→6.5 msとなった。

Cの追加修正後は、索引付き1万行Top 100が約0.053 ms、EXISTSが約0.63 ms、100万行Top 100が約0.059 ms。さらにUNION ALL内のCOUNTをbatch単位で数える修正で、100万行は約6.93 msとなった。いずれも診断値であり、最終の3製品・両索引構成の比較と区別する。

Eの調査ではfdatasyncの平均が約0.22 msと約2 msの区間に分かれる。物理ファイルの事前確保は、現行の厳格なCRC／末尾破損判定とそのままでは両立しないため採用していない。事前確保の試作を取り除いた後、commit要求を中心にした同期を追加した。成功応答は引き続きown/dependency LSNの同期完了を待つ。要求のないWALの背景同期は200 msを上限とし、途中のWAL同期に続いてcommit同期を二重に待つ状況の削減を検証する。埋込みAPIの既定周期とWAL形式は変更しない。WALの性能確認は未完了。

I/Oの追加診断では、許可CPU 0–7を保ちmain I/O threadのみCPU 0に固定すると、16接続のliteral queryは独立2試行で約434k／337k query/s（PG約622k／620k）、ROLLBACKは約90.6k／84.4k txn/s（PG約151k／154k）となった。主測定とは分けて`2026-10-29-gap6-c11-affinity-0`へ保存した。この配置を実装し、SQL worker・storage thread・I/O上で同期実行するSQLには元のCPU集合を維持する。最終測定は実際の`io_cpu`も保存する。

索引走査では可視行を所有権移動で渡し、物理順を復元するsortの比較ごとのhash参照を、各行1回のrank取得へ変えた。式cache等のキー生成には、既存stream版とbyte単位で同一のstring encoderを追加して小さい書込みのstream費用を削減する。完了通知は空queueへの遷移でまとめ、WALのbatch対象から耐久化済みの応答を除く。これらは追加の検証・再測定中。

全ctestの初回は4,142件中7件が失敗した（既存skip 3件）。うちUINT64のgrouped MIN/MAXは型付きbatch→Valueとarena→Valueで符号タグが欠落した実装上の不具合であり、両変換を修正した。NULL・UINT64 group key・最大値・UNION ALLでのarena再利用の回帰を追加した。残る計画契約は、小さいCOUNTの投影／index-only経路の保持、generic並列集約の共通閾値、整数のscan内集約の選択、選択的non-equi joinのBandJoin選択、filtered computed aggregateでのCascades経路の保持を整理した。7件の再検証とrelational 37件は通過済み。修正後C13の全ctestは4,143件・失敗0（既存skip 3件）、SQL oracleは301秒432入力・不一致0で完了した。比較器34テストとlayeringも通過。検証ログは`2026-10-30-gap6-c13-provenance`へ保存した。

### C13再測定からの追加修正（2026-10-30）

C13の同一バイナリ比較は全161対象を再測定し、115通過・46未達となった。
OLAPは両構成計474測定が結果一致、対象OLTP/HTAPは192試行が検査通過。
NoWait競合は測定窓で各構成1,050件あり、成功TPSから除外した。
元PG固定値と同時期PGを両方比較した表は
`benchmark/comparison/results/2026-10-30-gap6-c13-comparison/comparison.md`に保存した。
これは追加修正前の中間結果であり、完了報告ではない。

C13の再測定では、Q6のCascades経路を保つ修正が整数のfiltered computed
aggregateまで遅い経路へ戻した。数式集計は100万行で約33msとなり、目標の
約22msを超えたため、Aは最終達成扱いを取り消して修正する。浮動小数点の
filtered aggregateは従来経路を保ち、整数のbytecode式だけ走査内集計を選ぶ。
並列table scanはmorselの担当・結合順を保ち、callerもworker 0として参加する。

更新系には新しい固定長WALファイルの試作を追加した。単純なゼロ埋めではなく、
CRC付きの正の論理終端をセグメント外に記録する。既存WALは変換せず、新規の
サーバ用WALだけを対象とし、両終端の破損時は既定で起動を拒否する。
[ADR 0001](adr/0001-fixed-wal-logical-end.md)に方式・互換性・採用条件を記録した。
この試作は品質・性能の検証が完了するまで採用済みと数えない。

C14の局所比較では数式集計が1万行0.276ms、100万行15.39msに戻り、
同時期PG比はそれぞれ0.75倍・1.33倍となった。一方Q8は3.05msで未達。
次のC15では、通常のrelational table scanにも型を確認したINT64/DATEの早期棄却を
渡し、復号する行を減らす。NULL、UINT64の列と定数、FLOAT64への整数境界、
射影、古いsnapshotの回帰で検証する。

WALはpre-rollで次セグメントのレコード境界を保つ。旧frameのCRC付き終端は
実データ末尾を保ち、走査位置との完全一致と次frameの存在だけをpaddingの証明に
使う。新規領域は先に実体化して未書込みextentの変換をcommitから除く。
またcommitのbatch対象はautocommit DMLとCOMMIT/ENDに限定し、明示block中の
SELECT/UPDATEの将来commitを無駄に待たない。own/dependencyバリアは維持する。

C15の全ctestは4,159件・失敗0（既存skip 3件、287.57秒）、SQL oracleは
301秒441入力・不一致0。WALの同期失敗・終端破損・正の終端より前のゼロ化・
checkpoint/recycle後の回復を含む。C16では追加レビューで見つけたUINT64定数の
signed peek除外を加え、その境界の回帰を通した。最終性能の再測定中。

Q8のCallgrind診断ではLoadSourceが命令数の約54%を占め、row復号とmalloc/freeが
大きかった。小さいfiltered dimensionからのkey domainを次の走査選択へ反映し、
region→nation→customerの条件をordersのmaterializeより前に伝える。
C17の局所比較ではQ8は3.03→1.64ms（PG 1.44ms）、Q7は3.51→1.38ms。
重複とNULLを含む3段joinの結果と、factを先に全materializeしない回帰を追加した。
候補間の整数キー互換性は事前計算し、毎回の比較でschema解決を繰り返さない。

C18ではページ順序metadataの保持も試したが、Q14のwarm中央値に明確な改善が
なく（約0.80ms）、追加stateを残す根拠がないため、この試作だけを取り除く。
全体の最終測定では採用した走査順・同期処理のバイナリを使う。

4接続HTAPの混在p95にはrequest構成比の変化もある。C20でreaderは単独約4.05ms、
同時実行時約6.0ms、writer p99は約4.5msへ改善したが、readerの処理件数が増えた
ため混在p95は約6.0ms（PG約2.29ms）となり未達。元の混在値も残し、役割別の
値と併記する。測定定義を変えて通過扱いにはしない。

build側だけを読む非負SUM/COUNTは、実際に一意で密な整数join keyだけ直接参照し、
workerごとにprobe頻度を数えてから加算する。非負の場合の単調なprefixに限定し、
負値・probe側の引数・重複・疎なdomainは元の経路を保つ。NULL、穴、負のkey、
古いsnapshot、負値への更新、正のoverflowを回帰で確認した。

C21ではdurability/network待ちの要求をCPU占有と同等に扱わず、snapshot reader間で
空きCPUを使う。interactive要求がある時はI/O用の1core分を残し、専用query補助
threadをLinuxでbest-effort nice=5へ下げる。SQL worker/caller/I/O/storage threadの
priorityは変えず、許可CPU集合も変えない。新規threadはoperator終了時に消え、
元threadのpriorityを戻す権限は不要。親thread不変の回帰と実機のthread-local動作を
確認した。性能は再確認中。

## 全対象と固定PG目標

| 原因群 | 構成 | 負荷 | 規模/接続 | 指標 | 元tiny | 固定PG | 不利倍率 | 目標tiny |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| A | unindexed | count | 10000 | median_ms ms | 0.636385 | 0.190724 | 3.34× | ≤0.381448 |
| A | unindexed | expression | 10000 | median_ms ms | 1.234626 | 0.342133 | 3.61× | ≤0.684266 |
| A | unindexed | distinct | 10000 | median_ms ms | 1.678875 | 0.768447 | 2.18× | ≤1.536894 |
| C | unindexed | exists | 10000 | median_ms ms | 1.093388 | 0.534320 | 2.05× | ≤1.068640 |
| C | unindexed | window | 10000 | median_ms ms | 1.103596 | 0.538848 | 2.05× | ≤1.077696 |
| A | unindexed | count | 100000 | median_ms ms | 3.703841 | 1.746002 | 2.12× | ≤3.492004 |
| A | unindexed | count | 1000000 | median_ms ms | 29.315751 | 5.916825 | 4.95× | ≤11.833650 |
| A | unindexed | sum | 1000000 | median_ms ms | 46.013957 | 10.224578 | 4.50× | ≤20.449156 |
| A | unindexed | expression | 1000000 | median_ms ms | 41.769154 | 11.232512 | 3.72× | ≤22.465024 |
| C | unindexed | top_100 | 1000000 | median_ms ms | 59.548354 | 14.576626 | 4.09× | ≤29.153252 |
| B | unindexed | join_group | 1000000 | median_ms ms | 251.244618 | 32.488959 | 7.73× | ≤64.977918 |
| B | unindexed | scalar_subquery | 1000000 | median_ms ms | 129.126914 | 24.104874 | 5.36× | ≤48.209748 |
| B | unindexed | htap_reader | 1000000 | median_ms ms | 206.817860 | 31.733236 | 6.52× | ≤63.466472 |
| B | unindexed | Q09 | 0.01 | median_ms ms | 11.374284 | 5.603607 | 2.03× | ≤11.207214 |
| B | unindexed | Q11 | 0.01 | median_ms ms | 1.265556 | 0.631806 | 2.00× | ≤1.263612 |
| D | unindexed | noop | 1 | tps query/s | 45343.400000 | 101038.600000 | 2.23× | ≥50519.300000 |
| D | unindexed | noop | 1 | p50_ms ms | 0.021400 | 0.009197 | 2.33× | ≤0.018394 |
| D | unindexed | noop | 1 | p95_ms ms | 0.024377 | 0.010931 | 2.23× | ≤0.021862 |
| D | unindexed | noop | 1 | p99_ms ms | 0.035168 | 0.014057 | 2.50× | ≤0.028114 |
| D | unindexed | noop | 4 | p50_ms ms | 0.020399 | 0.010159 | 2.01× | ≤0.020318 |
| D | unindexed | noop | 4 | p95_ms ms | 0.025038 | 0.011913 | 2.10× | ≤0.023826 |
| D | unindexed | noop | 4 | p99_ms ms | 0.031901 | 0.014097 | 2.26× | ≤0.028194 |
| D | unindexed | noop | 16 | tps query/s | 209765.200000 | 613123.800000 | 2.92× | ≥306561.900000 |
| D | unindexed | noop | 16 | p50_ms ms | 0.071406 | 0.023445 | 3.05× | ≤0.046890 |
| D | unindexed | noop | 16 | p95_ms ms | 0.107145 | 0.042792 | 2.50× | ≤0.085584 |
| D | unindexed | noop | 16 | p99_ms ms | 0.121321 | 0.054824 | 2.21× | ≤0.109648 |
| D | unindexed | point_read | 16 | p50_ms ms | 0.075333 | 0.037332 | 2.02× | ≤0.074664 |
| E | unindexed | update | 4 | tps txn/s | 1837.400000 | 17450.200000 | 9.50× | ≥8725.100000 |
| E | unindexed | update | 4 | p50_ms ms | 2.110529 | 0.218126 | 9.68× | ≤0.436252 |
| E | unindexed | update | 4 | p95_ms ms | 2.318626 | 0.269905 | 8.59× | ≤0.539810 |
| E | unindexed | update | 4 | p99_ms ms | 3.938158 | 0.283090 | 13.91× | ≤0.566180 |
| E | unindexed | hot_update | 1 | tps txn/s | 492.000000 | 1025.800000 | 2.08× | ≥512.900000 |
| E | unindexed | hot_update | 1 | p50_ms ms | 1.984308 | 0.964692 | 2.06× | ≤1.929384 |
| E | unindexed | hot_update | 1 | p95_ms ms | 2.167257 | 1.049213 | 2.07× | ≤2.098426 |
| E | unindexed | hot_update | 1 | p99_ms ms | 3.774956 | 1.101513 | 3.43× | ≤2.203026 |
| E | unindexed | transfer | 1 | tps txn/s | 465.600000 | 972.600000 | 2.09× | ≥486.300000 |
| E | unindexed | transfer | 1 | p50_ms ms | 2.114918 | 1.009357 | 2.10× | ≤2.018714 |
| E | unindexed | transfer | 1 | p95_ms ms | 2.320310 | 1.117944 | 2.08× | ≤2.235888 |
| E | unindexed | transfer | 1 | p99_ms ms | 2.760550 | 1.269403 | 2.17× | ≤2.538806 |
| E | unindexed | transfer | 4 | p95_ms ms | 5.366325 | 2.181575 | 2.46× | ≤4.363150 |
| E | unindexed | transfer | 4 | p99_ms ms | 6.504307 | 2.611014 | 2.49× | ≤5.222028 |
| E | unindexed | read_write | 4 | p50_ms ms | 4.174399 | 2.035646 | 2.05× | ≤4.071292 |
| E | unindexed | read_write | 4 | p95_ms ms | 4.553122 | 2.221601 | 2.05× | ≤4.443202 |
| E | unindexed | read_write | 4 | p99_ms ms | 5.815953 | 2.725363 | 2.13× | ≤5.450726 |
| E | unindexed | read_write | 16 | tps txn/s | 3593.000000 | 7660.200000 | 2.13× | ≥3830.100000 |
| E | unindexed | read_write | 16 | p50_ms ms | 4.395953 | 2.056576 | 2.14× | ≤4.113152 |
| E | unindexed | read_write | 16 | p95_ms ms | 4.858185 | 2.231580 | 2.18× | ≤4.463160 |
| E | unindexed | read_write | 16 | p99_ms ms | 6.624987 | 2.842256 | 2.33× | ≤5.684512 |
| E | unindexed | insert_delete | 1 | p50_ms ms | 0.308819 | 0.151749 | 2.04× | ≤0.303498 |
| E | unindexed | insert_delete | 16 | p99_ms ms | 4.801947 | 2.324668 | 2.07× | ≤4.649336 |
| D | unindexed | rollback | 1 | tps txn/s | 16375.400000 | 33566.600000 | 2.05× | ≥16783.300000 |
| D | unindexed | rollback | 1 | p50_ms ms | 0.058702 | 0.027974 | 2.10× | ≤0.055948 |
| D | unindexed | rollback | 1 | p95_ms ms | 0.075494 | 0.035117 | 2.15× | ≤0.070234 |
| D | unindexed | rollback | 1 | p99_ms ms | 0.097095 | 0.045948 | 2.11× | ≤0.091896 |
| D | unindexed | rollback | 16 | tps txn/s | 67883.600000 | 165006.400000 | 2.43× | ≥82503.200000 |
| D | unindexed | rollback | 16 | p50_ms ms | 0.228456 | 0.091314 | 2.50× | ≤0.182628 |
| D | unindexed | rollback | 16 | p95_ms ms | 0.285936 | 0.135639 | 2.11× | ≤0.271278 |
| B | unindexed | htap | 1 | tps txn/s | 4.800000 | 31.200000 | 6.50× | ≥15.600000 |
| B | unindexed | htap | 1 | p50_ms ms | 200.234306 | 31.824565 | 6.29× | ≤63.649130 |
| B | unindexed | htap | 1 | p95_ms ms | 208.769140 | 32.379964 | 6.45× | ≤64.759928 |
| B | unindexed | htap | 1 | p99_ms ms | 209.103007 | 33.822579 | 6.18× | ≤67.645158 |
| B | unindexed | htap | 1 | reader_tps query/s | 4.800000 | 31.200000 | 6.50× | ≥15.600000 |
| E | unindexed | htap | 4 | p50_ms ms | 0.585447 | 0.278512 | 2.10× | ≤0.557024 |
| B | unindexed | htap | 4 | reader_tps query/s | 4.800000 | 30.000000 | 6.25× | ≥15.000000 |
| B | unindexed | htap | 4 | reader_p50_ms ms | 202.948308 | 32.994177 | 6.15× | ≤65.988354 |
| B | unindexed | htap | 4 | reader_p95_ms ms | 211.633077 | 34.080281 | 6.21× | ≤68.160562 |
| B | unindexed | htap | 4 | reader_p99_ms ms | 214.330406 | 34.590405 | 6.20× | ≤69.180810 |
| E | unindexed | htap | 4 | transaction_p50_ms ms | 0.585387 | 0.278341 | 2.10× | ≤0.556682 |
| E | unindexed | htap | 16 | p95_ms ms | 4.604830 | 2.284241 | 2.02× | ≤4.568482 |
| B | unindexed | htap | 16 | reader_tps query/s | 4.600000 | 29.200000 | 6.35× | ≥14.600000 |
| B | unindexed | htap | 16 | reader_p50_ms ms | 213.719881 | 34.068559 | 6.27× | ≤68.137118 |
| B | unindexed | htap | 16 | reader_p95_ms ms | 222.912780 | 34.806778 | 6.40× | ≤69.613556 |
| B | unindexed | htap | 16 | reader_p99_ms ms | 223.734249 | 35.422994 | 6.32× | ≤70.845988 |
| E | unindexed | htap | 16 | transaction_p95_ms ms | 4.603338 | 2.273580 | 2.02× | ≤4.547160 |
| A | indexed | count | 10000 | median_ms ms | 0.763277 | 0.199811 | 3.82× | ≤0.399622 |
| C | indexed | filter_1pct | 10000 | median_ms ms | 0.169864 | 0.032722 | 5.19× | ≤0.065444 |
| C | indexed | filter_10pct | 10000 | median_ms ms | 0.406086 | 0.082487 | 4.92× | ≤0.164974 |
| A | indexed | expression | 10000 | median_ms ms | 1.225780 | 0.341392 | 3.59× | ≤0.682784 |
| C | indexed | top_100 | 10000 | median_ms ms | 0.127113 | 0.044375 | 2.86× | ≤0.088750 |
| C | indexed | exists | 10000 | median_ms ms | 0.805437 | 0.345019 | 2.33× | ≤0.690038 |
| A | indexed | count | 100000 | median_ms ms | 4.163428 | 1.779687 | 2.34× | ≤3.559374 |
| C | indexed | filter_10pct | 100000 | median_ms ms | 1.886121 | 0.703203 | 2.68× | ≤1.406406 |
| C | indexed | top_100 | 100000 | median_ms ms | 0.122494 | 0.047100 | 2.60× | ≤0.094200 |
| A | indexed | count | 1000000 | median_ms ms | 33.398129 | 7.735567 | 4.32× | ≤15.471134 |
| A | indexed | sum | 1000000 | median_ms ms | 46.476917 | 10.961436 | 4.24× | ≤21.922872 |
| A | indexed | expression | 1000000 | median_ms ms | 41.116063 | 11.552374 | 3.56× | ≤23.104748 |
| C | indexed | top_100 | 1000000 | median_ms ms | 0.163422 | 0.050146 | 3.26× | ≤0.100292 |
| B | indexed | join_group | 1000000 | median_ms ms | 252.847427 | 34.216571 | 7.39× | ≤68.433142 |
| B | indexed | scalar_subquery | 1000000 | median_ms ms | 124.598718 | 20.658784 | 6.03× | ≤41.317568 |
| A | indexed | union_all | 1000000 | median_ms ms | 9.047234 | 3.842817 | 2.35× | ≤7.685634 |
| B | indexed | htap_reader | 1000000 | median_ms ms | 201.285587 | 27.121423 | 7.42× | ≤54.242846 |
| B | indexed | Q07 | 0.01 | median_ms ms | 4.147719 | 1.815656 | 2.28× | ≤3.631312 |
| B | indexed | Q08 | 0.01 | median_ms ms | 3.537363 | 1.489794 | 2.37× | ≤2.979588 |
| B | indexed | Q14 | 0.01 | median_ms ms | 1.714453 | 0.413048 | 4.15× | ≤0.826096 |
| B | indexed | Q19 | 0.01 | median_ms ms | 1.249776 | 0.292869 | 4.27× | ≤0.585738 |
| B | indexed | Q20 | 0.01 | median_ms ms | 1.112805 | 0.532026 | 2.09× | ≤1.064052 |
| D | indexed | noop | 1 | tps query/s | 46850.600000 | 101754.600000 | 2.17× | ≥50877.300000 |
| D | indexed | noop | 1 | p50_ms ms | 0.021240 | 0.009157 | 2.32× | ≤0.018314 |
| D | indexed | noop | 1 | p95_ms ms | 0.024888 | 0.011271 | 2.21× | ≤0.022542 |
| D | indexed | noop | 1 | p99_ms ms | 0.048272 | 0.015710 | 3.07× | ≤0.031420 |
| D | indexed | noop | 4 | p95_ms ms | 0.024106 | 0.011723 | 2.06× | ≤0.023446 |
| D | indexed | noop | 4 | p99_ms ms | 0.031029 | 0.014127 | 2.20× | ≤0.028254 |
| D | indexed | noop | 16 | tps query/s | 219531.200000 | 606927.800000 | 2.76× | ≥303463.900000 |
| D | indexed | noop | 16 | p50_ms ms | 0.068982 | 0.023686 | 2.91× | ≤0.047372 |
| D | indexed | noop | 16 | p95_ms ms | 0.103267 | 0.043343 | 2.38× | ≤0.086686 |
| D | indexed | noop | 16 | p99_ms ms | 0.118446 | 0.055426 | 2.14× | ≤0.110852 |
| E | indexed | update | 1 | tps txn/s | 2019.200000 | 7316.800000 | 3.62× | ≥3658.400000 |
| E | indexed | update | 1 | p50_ms ms | 0.266439 | 0.131140 | 2.03× | ≤0.262280 |
| E | indexed | update | 1 | p95_ms ms | 2.082777 | 0.156429 | 13.31× | ≤0.312858 |
| E | indexed | update | 1 | p99_ms ms | 2.304379 | 0.186216 | 12.37× | ≤0.372432 |
| E | indexed | hot_update | 1 | tps txn/s | 458.600000 | 955.800000 | 2.08× | ≥477.900000 |
| E | indexed | hot_update | 1 | p50_ms ms | 2.142931 | 1.032902 | 2.07× | ≤2.065804 |
| E | indexed | hot_update | 1 | p95_ms ms | 2.346079 | 1.116642 | 2.10× | ≤2.233284 |
| E | indexed | hot_update | 1 | p99_ms ms | 2.581959 | 1.174592 | 2.20× | ≤2.349184 |
| E | indexed | transfer | 1 | p50_ms ms | 0.323298 | 0.157932 | 2.05× | ≤0.315864 |
| E | indexed | transfer | 4 | p95_ms ms | 4.451859 | 2.186935 | 2.04× | ≤4.373870 |
| E | indexed | transfer | 4 | p99_ms ms | 6.355052 | 2.963197 | 2.14× | ≤5.926394 |
| E | indexed | read_write | 1 | tps txn/s | 635.800000 | 4403.600000 | 6.93× | ≥2201.800000 |
| E | indexed | read_write | 1 | p50_ms ms | 2.200701 | 0.213137 | 10.33× | ≤0.426274 |
| E | indexed | read_write | 1 | p95_ms ms | 2.397447 | 0.281257 | 8.52× | ≤0.562514 |
| E | indexed | read_write | 1 | p99_ms ms | 2.546061 | 0.366009 | 6.96× | ≤0.732018 |
| E | indexed | read_write | 4 | tps txn/s | 960.600000 | 1941.400000 | 2.02× | ≥970.700000 |
| E | indexed | read_write | 4 | p50_ms ms | 4.166534 | 2.027600 | 2.05× | ≤4.055200 |
| E | indexed | read_write | 4 | p95_ms ms | 4.589572 | 2.218095 | 2.07× | ≤4.436190 |
| E | indexed | read_write | 16 | tps txn/s | 3688.400000 | 7686.600000 | 2.08× | ≥3843.300000 |
| E | indexed | read_write | 16 | p50_ms ms | 4.332351 | 2.043942 | 2.12× | ≤4.087884 |
| E | indexed | read_write | 16 | p95_ms ms | 4.709641 | 2.214759 | 2.13× | ≤4.429518 |
| E | indexed | read_write | 16 | p99_ms ms | 6.125364 | 2.956905 | 2.07× | ≤5.913810 |
| D | indexed | rollback | 1 | tps txn/s | 15117.200000 | 32179.600000 | 2.13× | ≥16089.800000 |
| D | indexed | rollback | 1 | p50_ms ms | 0.066917 | 0.028955 | 2.31× | ≤0.057910 |
| D | indexed | rollback | 1 | p95_ms ms | 0.077688 | 0.037682 | 2.06× | ≤0.075364 |
| D | indexed | rollback | 1 | p99_ms ms | 0.099410 | 0.047330 | 2.10× | ≤0.094660 |
| D | indexed | rollback | 16 | tps txn/s | 71061.600000 | 155753.600000 | 2.19× | ≥77876.800000 |
| D | indexed | rollback | 16 | p50_ms ms | 0.220451 | 0.096404 | 2.29× | ≤0.192808 |
| B | indexed | htap | 1 | tps txn/s | 4.800000 | 35.600000 | 7.42× | ≥17.800000 |
| B | indexed | htap | 1 | p50_ms ms | 201.649911 | 27.738265 | 7.27× | ≤55.476530 |
| B | indexed | htap | 1 | p95_ms ms | 210.079805 | 28.160853 | 7.46× | ≤56.321706 |
| B | indexed | htap | 1 | p99_ms ms | 210.857820 | 28.869636 | 7.30× | ≤57.739272 |
| B | indexed | htap | 1 | reader_tps query/s | 4.800000 | 35.600000 | 7.42× | ≥17.800000 |
| E | indexed | htap | 4 | tps txn/s | 3815.800000 | 8997.200000 | 2.36× | ≥4498.600000 |
| E | indexed | htap | 4 | p95_ms ms | 2.543396 | 0.425833 | 5.97× | ≤0.851666 |
| E | indexed | htap | 4 | p99_ms ms | 3.481306 | 1.588333 | 2.19× | ≤3.176666 |
| B | indexed | htap | 4 | reader_tps query/s | 4.600000 | 34.600000 | 7.52× | ≥17.300000 |
| B | indexed | htap | 4 | reader_p50_ms ms | 202.627969 | 28.660025 | 7.07× | ≤57.320050 |
| B | indexed | htap | 4 | reader_p95_ms ms | 212.524813 | 29.521920 | 7.20× | ≤59.043840 |
| B | indexed | htap | 4 | reader_p99_ms ms | 217.881520 | 30.103822 | 7.24× | ≤60.207644 |
| E | indexed | htap | 4 | transaction_tps txn/s | 3811.200000 | 8962.600000 | 2.35× | ≥4481.300000 |
| E | indexed | htap | 4 | transaction_p95_ms ms | 2.541432 | 0.418098 | 6.08× | ≤0.836196 |
| E | indexed | htap | 4 | transaction_p99_ms ms | 3.134885 | 1.239917 | 2.53× | ≤2.479834 |
| E | indexed | htap | 16 | tps txn/s | 3438.000000 | 10500.200000 | 3.05× | ≥5250.100000 |
| E | indexed | htap | 16 | p50_ms ms | 4.335427 | 1.992734 | 2.18× | ≤3.985468 |
| E | indexed | htap | 16 | p95_ms ms | 5.125204 | 2.234405 | 2.29× | ≤4.468810 |
| E | indexed | htap | 16 | p99_ms ms | 8.953824 | 3.375164 | 2.65× | ≤6.750328 |
| B | indexed | htap | 16 | reader_tps query/s | 4.800000 | 33.400000 | 6.96× | ≥16.700000 |
| B | indexed | htap | 16 | reader_p50_ms ms | 205.192244 | 28.997479 | 7.08× | ≤57.994958 |
| B | indexed | htap | 16 | reader_p95_ms ms | 213.130289 | 33.227815 | 6.41× | ≤66.455630 |
| B | indexed | htap | 16 | reader_p99_ms ms | 215.316733 | 34.446312 | 6.25× | ≤68.892624 |
| E | indexed | htap | 16 | transaction_tps txn/s | 3433.200000 | 10466.800000 | 3.05× | ≥5233.400000 |
| E | indexed | htap | 16 | transaction_p50_ms ms | 4.335056 | 1.991482 | 2.18× | ≤3.982964 |
| E | indexed | htap | 16 | transaction_p95_ms ms | 5.048097 | 2.226390 | 2.27× | ≤4.452780 |
| E | indexed | htap | 16 | transaction_p99_ms ms | 8.744805 | 3.167978 | 2.76× | ≤6.335956 |

C21のCPU借用・補助threadのnice調整は、4接続readerを約4.5msへ縮めた一方、
同時期PG比でwriter TPSが2.60倍、writer p95が7.61倍と悪化したため取り除く。
C20のinteractive要求分のCPUを確保する方式に戻し、採用済みの整数集計・
走査順・頻度集計・WAL変更をまとめたバイナリで全件を再検証する。

C22ではtyped batchの最後の射影列を復号した後、不要な後続列の走査を止める。
先頭固定長列だけの射影、可変長列を跨ぐ射影、NULL bitmap、batch境界、
更新前snapshotを同じfixtureで検証する。全品質検査と全条件再測定は進行中。

C22の全ctestは4,163件・失敗0（既存skip 3件、288.76秒）、SQL oracleは
604秒887入力・不一致0。C23では非負の頻度集計だけworker数分のgroup tableを
確保し、符号付きの一般経路はmorsel順のtableを維持する。多数morselと512KiB
予算の回帰を追加し、追加変更後の全品質検査を再実行してから測定する。

C23の全ctestは4,163件・失敗0（既存skip 3件、289.17秒）、SQL oracleは
510秒766入力のコーパス再生・不一致0。解析34テスト、layering、formatも通過。
バイナリSHA-256 `2a4473acc0b6a3d4b91e84592bb8d01999ee3946caf6b2864cfa8fc0c30fcb7b`
と全C++/比較ハーネスのhashを保存し、全OLTP/HTAP（Tiny/PG）と
全OLAP（3製品）の両索引構成を再測定中。

ユーザー追加確認（C23測定中）: 更新4接続でPG自身が旧値より約9倍遅くなる
区間が観測されたため、達成判定には「今回同じ条件で再測定したPG」を採用する。
旧PG固定値の未達は別列に残す。これはユーザーが明示的に選んだ基準変更であり、
混在HTAPを含む指標の定義・SQL・同期commit条件は変えない。

C23途中の未達はROLLBACKの固定費、HTAPのtailと試行間の同期所要時間差、
索引あり1万行の10%フィルタ（2.08倍）など。索引ありSELECT 1/16接続は、
同じバイナリ・version chain 0でも約14万query/s（PG約61万）となった。
索引なし時の約40万からの差は索引そのものの費用と決めつけず、現在無制限の
負荷生成CPUがサーバ用CPU 0–7と重なる影響を、別の物理core 8–15で切り分ける。
診断は別条件として保存し、C23の全測定中にビルドや追加負荷はかけない。

次の修正候補は、GCの閾値到達ごとの起床（閾値未満の連続passを抑制し、
静かなburstは100msのtimed wakeを維持）、index-only結果Rowのvector再利用、
混合負荷で要求間の並列実行を優先するreader並列数の制御。採否は局所再測定で
決め、採用後は全条件を同一バイナリで確認する。

C23の全測定はOLAP 474件が結果一致、OLTP/HTAP 396試行が検査通過。
今回PG比では元161対象の146件通過（A14/14、B38/39、C9/10、D26/35、E59/63）。
新規13指標を含む全未達28指標を`2026-10-30-gap6-c23-comparison`へ保存した。
旧PG固定比は参考で134/161通過。NoWait中断は索引なし23,405件／あり11,995件、
PGは0件。成功TPSには中断を含めていない。全C++/ハーネスhashとバイナリの
測定前後一致を検証した。これは中間結果であり、完了扱いにはしない。

C23の追加診断では、負荷生成を別の物理core 8–15へ移すとSELECT 1/16接続は
約353k→237k query/sへ悪化した。I/O affinityを外す試行も約295kに留まった。
いずれも採用しない。通常配置の再測定ではPG比2倍以内だったが、フル測定での
大きな変動の原因が解消したとは扱わない。I/O threadは診断中ほぼ1 coreを使い、
schedstatの実行待ちはほぼ0だったため、負荷生成とのCPU競合だけでは説明できない。

Q14/Q15のCallgrindはnativeでロードした同じfixtureを再起動して計測した。
Q15ではRow::TryPeekInteger本体が命令数の21.2%、関連するschema/serdes/skipも
大きい。日付列より前の固定長列を毎行たどる処理を、schemaごとにまとめる候補を
C24に追加する。NULL bitmap行は従来のwalkを保ち、古いsnapshotも検証する。
プロファイラの経過時間は比較ベンチマーク値には使わない。

C24候補はGCの閾値到達時だけの通知、index-only結果Rowの容量再利用、混合負荷で
readerの要求内並列数を1へ抑える制御、可変長列の間の固定長spanをまとめるpeek、
一度SELECTと確認した同一WITH文をidle時のsnapshot workerへ送る分類を含む。
WITHの分類は接続ごとに最大1文・64KiBで、結果やbound planは保持しない。
初回は従来経路、TEMPセッション・明示block・異なるSQLは従来の状態機械を保つ。
新しい3回帰は通過済み。全品質検査と局所比較後に採否を決める。

C24の全ctestは4,166件・失敗0（既存skip 3件、287.13秒）。SQL oracleは
64入力から開始して新規変異も生成し、601秒749入力・不一致0。解析34テスト、
layering、formatも通過。SHA-256
`d86adfc1179732ea0ccd483c01dca8a52ba7aa5800e12c856bfd5c3f991cd235`
の候補を固定して局所比較中。索引あり1万行10%フィルタは0.1354ms
（PG 0.08336ms、1.62倍）。前回0.1675ms／2.08倍から縮まった。
100万行単独readerは3.86ms（PG 25.36ms）で結果一致。

C24局所比較はOLAP 18件すべて一致、OLTP/HTAP 48試行すべて不変条件を通過。
SQL競合は別途失敗として保存した。SELECT 1/16接続はTPS・p50・p95・p99が
いずれもPG比2倍以内、ROLLBACKも今回の全接続条件で2倍以内。
Q15は0.559ms（PG 0.500ms）、Q14は0.843ms（PG 0.409ms、2.06倍）。
4接続HTAPのwriter p95/p99は0.391/0.782ms（PG 0.425/1.161ms）へ縮んだが、
混在p99と16接続のtailが未達。16接続の同期待ちは約1.0–1.25ms/syncで変動し、
並列度だけの効果とは断定しない。元の混在指標も維持する。

100万行10%フィルタはTinyが8.086→7.666msへ少し縮んだ一方、今回PGは
8.098→2.349msとなり、新たな3.26倍差が出た。C25候補では索引の条件を一度
compileし、決定的なキー列だけの条件は同じ重複keyのvalue list内で判定を再利用
する。INCLUDE列や未対応式はこの再利用から除外する。小さい結合集計では1本だけの
補助threadを起動せずcallerで実行し、同じ式scratchのエラー後の再利用も検証する。

C25の局所OLAP 18件は全件一致。索引あり10%フィルタは1万行0.135ms
（PG 0.080ms）、100万行3.824ms（PG 7.814ms、前の速いPG 2.349msに
固定しても1.63倍）、Q14は0.781ms（PG 0.420ms、1.86倍）、Q15は0.545ms。
全件再測定での確認は残る。

C26は短い応答のworker送信と、bytecode fast null-safe比較のVARCHAR/DATEの
型判定を修正した。全ctest 4,171件・失敗0・既存skip 3、SQL oracleは601秒
772入力・新規変異込み不一致0。フル比較は途中のSELECT 1/16接続でp50/p95が
2倍を少し超えたため中断した。保存済みのOLAP 171件は一致しているが、
`2026-10-30-gap6-c26-full`は未完了の測定であり最終結果には使わない。

C27の受信コピー削減ではpipeline＋half-closeの回帰が発生した。テスト終了状態の
確認前に始めた比較は中断し、採否判定から除外した。C28で、worker完了と古い
RDHUP通知が交差した時も未読メッセージをdrainしてから接続を閉じるよう修正した。
server 64テストとpipeline 200反復を通過。局所84試行は全件整合性を通過し、
SELECT 1/16接続の9試行中央値は394k query/s（PG 627k、1.59倍）、p95も1.72倍。
HTAP混合p99など3指標は未達で、全件完了とは扱わない。

C29は要求targetのcacheではなくLoggerの実際の耐久化済み境界で応答可否を判定する。
fdatasyncを止めた回帰で、未耐久targetは通さず、成功後にのみ境界が進むことを確認した。
受信cacheの連続処理も32 packetごとにevent loopへ戻し、50 packet＋half-closeを検証。
重点36試行はすべて結果整合性を通過したが、混合HTAP p99は4条件で2倍を超える。
役割別readerはPGより速く、成功TPSとwriterの中央値も近い一方、writerのtailと
読み取りの構成比が混合p99に影響している。指標定義は維持し、長い処理の実行時間と
耐久化待ちを別に記録する診断へ進む。C29は全品質検査・全件比較前の中間候補である。

C30の混合時2スレッド案は4接続の混合p99とwriter p99を悪化させたため取り除いた。
索引あり測定は途中で停止し、完了結果として採用しない。

C31はunfiltered整数join keyの正確な出現回数を、caller threadごとに最大1MiB・
1エントリだけ保持する。SQL文や結合相手の値、集計結果は保存しない。再利用の度に
table lease、全row-page chain、各page latch下のMVCC chain不在と同じPageLSN、
前後のlayout generationを検証する。証明できない時は従来のsnapshot scanへ戻す。
新しいtable 2回帰と結合集計1回帰で、NULL・負key・domain変更・不完全なpage列・
未公開tail・古いsnapshot・commit/abort・dimension更新・overflow・drop/recreateを検証。
関連148テストと追加境界の再検証は通過。全品質検査はまだ行っていない。

C31重点比較は36試行すべて整合性を通過し、56比較のうち54が2倍以内だった。
HTAP reader中央値は約17msから約1.5msへ縮んだ。混合p99の4残差は今回すべて通過。
一方readerが毎秒約650回へ増え、成功要求中の5%を超えたため、4接続の混合p95に
新たな差が出た。索引なし1.487ms対PG 0.409ms（3.64倍）、索引あり1.498ms対
PG 0.418ms（3.58倍）。元の混合指標を外さず、残る読取固定費をプロファイルする。
NoWait中断はTiny 258件、PG 0件。バイナリとC++/ハーネスhashの前後一致を確認した。

C31のCallgrindでは、10回のwarm readerでLoadSourceが命令数の40.4%、
Relationの破棄が10.1%、BuildScanMorselsが10.6%を占めた。計測は比較と分離し、
プロファイラー上の経過時間は性能値に使わない。C32はbuild側の固定長整数／DATEを
1MiB上限の列形式で読む。NULLと型を保ち、負SUM・重複・疎なdomainはcanonical
Rowへ戻して従来の順序付き集計を使う。上限や予算不足でも通常のLoadSourceへ戻る。
グループの単一整数キーは一時Rowを作らず、NULLグループを別に保持する。
DATE／NULL、小予算、上限超過の追加2回帰と、関連150テストを通過。

C32重点36試行は整合性を通過し、56比較中55が2倍以内。索引なし28比較は通過、
索引あり4接続混合p95だけ1.114ms対PG 0.460ms（2.42倍）で残った。
reader中央値は約1.1ms。TinyのNoWait中断223件、PG 0件。測定前後のsource／binary
一致を確認した。全品質検査とフル比較はまだ実行していない。

C33候補は、保存した完全なpage列からcacheを探し、現在のnext link、終端0、
各PageLSN、MVCC chain不在、layout generationをそのまま確認する。cache missは
従来のmorsel構築・走査へ戻す。cacheがある時のmorsel構築とcache検証の二重の
page-pool走査をなくす候補であり、保存page列だけで鮮度を判定する変更ではない。

C33の削除穴確認で、件数の構築にRowCountを走査終端として使う誤りを発見した。
空きスロットの後ろの生存行を落とすため、RowMaxまで走査しGetRowの空きを飛ばす。
追加した削除回帰は旧ループで失敗し、修正後は通過した。C31/C32はこの一般ケースを
修正する前の中間候補であり、最終版として扱わない。C33の関連150テストは通過。
C33重点36試行は整合性を通過し、56比較中55が2倍以内。索引なし4接続混合p95だけ
0.851ms対PG 0.414ms（2.06倍）で残った。Tiny中断238件／PG 0件、source/binary一致。

C34候補は、検証済み件数がある時にキー→行の対応表と頻度の中間配列を作らず、
dimensionの各行に直接件数を掛ける。正の重複キーも各行を一度ずつ数えるため、
joinの多重度を保つ。cache miss時の走査は引き続き一意な密domainだけ頻度集計する。
グループ作成・実際のkey範囲・SUM引数の非負性の確認も1回にまとめた。
正の重複キーをcache hitで読む回帰と、既存の負値・NULL・DATE・上限・snapshotの
回帰、結合集計44件とserver 64件が通過。全品質検査と全条件比較はまだ行っていない。

C34重点36試行はすべて整合性を通過し、56比較すべてが同時期PG比2倍以内。
最大は索引あり4接続混合p95の1.97倍（0.824ms対PG 0.418ms）。索引なしは
0.829ms対PG 0.428ms（1.94倍）。TinyのNoWait中断276件／PG 0件は保存し、
成功TPSには含めない。旧PG固定値は別の参考比較とし、今回PGを達成判定に使う。

C34の全ctestは4,178件・失敗0（既存skip 3、290.47秒）。SQL oracleは64入力から
新規変異を生成し、602秒771入力・新規追加655件・不一致0。解析34テスト、
整形109ファイル、layering、diff checkも通過。SHA-256
`4567bacc718b1f97381bc5a46ed3dab182b74e123fbe5a01337118bf550d9ecd`
のserverと全C++／ハーネスのhash一致を確認して、全件比較を2026-10-30 08:41 JSTに
開始した。OLAPは3製品474測定、OLTP／HTAPはTiny・PGの396試行を予定する。
この局所通過を全161対象・全456比較の達成として扱わず、フル監査を続ける。


## 最終監査（C34、2026-10-30）

標準フル測定はOLAP 474測定・OLTP/HTAP 396試行が全件整合性通過、
元161対象は153通過、全456比較中13未達。短時間の生データを保存した。
読取＋更新の索引なし1/16接続と索引あり4接続、HTAP索引なし4接続を
30秒×3試行で再測定し、条件全体の24指標を同じキーで照合した。
UNION ALLは101反復でPGが遅い状態へ変わったため、そのまま通過扱いにせず、
可視性情報とVACUUM前後の計画を確認した。0/11,364 visible pageのBitmap Heap Scan
から11,364/11,364のIndex Only Scan（Heap Fetches各0）になり、PGは11.164→3.600ms。
速いPGとの比較でもTiny 5.808ms、1.613倍だった。

最終は元161対象161通過（A14/B39/C10/D35/E63）、全456比較未達0、最大1.938倍。
旧PG固定値にも通るのは149/161。バイナリは全工程で同一、C++/ハーネスhash一致。
結果の採用元、短窓13残差、長窓24試行、PG状態確認、全品質検査は
[最終監査データ](../benchmark/comparison/results/2026-10-30-gap6-c34-final/)へ保存した。
