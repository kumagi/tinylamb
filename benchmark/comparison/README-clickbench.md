# ClickBench / DuckDB and ClickHouse comparison

ClickBenchの全43クエリを、同じParquetからロードしたtinylambとDuckDBまたはClickHouseで比較する。
[`clickbench.py`](clickbench.py)はTPC-DSのmanifestローダーとJSON workerを共用する。
エンジンにはクエリ番号・テーブル名・データセットに依存する分岐を追加していない。

## DuckDB比較（現在の改善目標）

目標は、正答を確認した全43クエリでDuckDBより短時間になること。
`--reference duckdb`の`target_achieved`は全43件の勝利を要求し、合計だけでは達成扱いにしない。
現在は100万行・500万行の開発用部分データで改善中。標準規模の約1億行は未測定。

## 並列予算・分割処理と辞書サイズ判定（opt46、2026-11-11）

| 行数 | 物理コア／最大スレッド | Tinylamb合計 ms | DuckDB合計 ms | Tinylamb勝利数 |
|---|---:|---:|---:|---:|
| 1M | 1 | 357.35–358.88 | 681.81–688.20 | 31–31/43 |
| 1M | 2 | 239.61–241.44 | 407.21–409.25 | 29–29/43 |
| 1M | 4 | 167.52–168.14 | 237.76–241.06 | 31–31/43 |
| 1M | 8 | 130.66–131.83 | 174.83–174.93 | 29–29/43 |
| 1M | 16 | 156.37–184.28 | 190.76–202.46 | 28–29/43 |
| 5M | 2 | 10,289.03–10,404.58 | 3,058.20–3,067.15 | 18–19/43 |
| 5M | 8 | 4,161.88–4,174.01 | 918.50–921.61 | 10–11/43 |

各条件warmup 1＋7回、正順・逆順の範囲。全14実行で43/43一致。500万行・8コアはopt45比72.3%短縮したが、DuckDBの4.53倍の時間がかかり、全43勝利は未達。
CPUモデル固有の閾値を使わず、許可CPU・並列予算・タスク数で制限する。DISTINCTは予算内のソートバッファと外部ランへ分割し、必要列だけをキャッシュする。辞書は固定件数上限ではなくバイト量で選ぶ。
標準約1億行、別CPUモデル、同時クライアント負荷は未測定。初回キャッシュ構築を含まない反復実行の結果。
測定条件・品質検証・制約・図は `build/benchmarks/clickbench-opt46-scaling-20261111/REPORT.md` と `comparison.png` / `.svg` に保存。


```bash
# 既存の旧データは日時のローカルタイムゾーン依存があるため、UTCで新規生成する。
/var/tmp/tinylamb-comparison-venv/bin/python benchmark/comparison/clickbench.py prepare \
  --upstream build/clickbench-upstream \
  --parquet build/benchmarks/clickbench-input/hits_0.parquet \
  --output build/benchmarks/clickbench-1m-utc-dataset
build-tpcds-release/tinylamb_tpcds_compare build/benchmarks/clickbench-1m-utc-native \
  --load build/benchmarks/clickbench-1m-utc-dataset
/var/tmp/tinylamb-comparison-venv/bin/python benchmark/comparison/clickbench.py init-duckdb \
  --dataset build/benchmarks/clickbench-1m-utc-dataset \
  --duckdb build/benchmarks/clickbench-1m.duckdb
/var/tmp/tinylamb-comparison-venv/bin/python benchmark/comparison/clickbench.py run \
  --reference duckdb --duckdb build/benchmarks/clickbench-1m.duckdb \
  --dataset build/benchmarks/clickbench-1m-utc-dataset \
  --native-db build/benchmarks/clickbench-1m-utc-native \
  --binary build-tpcds-release/tinylamb_tpcds_compare \
  --cpus 0,1 --threads 2 --warmups 1 --repeats 7 \
  --output build/benchmarks/clickbench-duck-result
```

DuckDBは独立した読み取り専用プロセスで実行し、`execute`から`fetchall`完了までを測る。
上の実行例では両プロセスをCPU 0・1、最大2スレッドに固定する。文字列は同一Parquetのバイト列からUTF-8へ変換し、
DuckDB側は公式クエリでも使われる`STRLEN`でバイト長を求める。
日時の生成・ロードはUTC固定。opt28以降はnativeの実行セッションもUTCに固定し、
日単位のTIMESTAMP_TRUNCプローブを測定外で検証する。全warmup・測定サンプルの結果を測定区間外で検証する。
整数は厳密比較し、LIMIT同率境界は順位と完全な結果への所属・重複数を検証する。
SQL・実行バイナリ・ソース差分・未追跡の追加C++ソース・ビルド設定・PGO入力・allocatorを保存する。

UTCで再生成した初期値（`build/benchmarks/clickbench-duck-baseline1-utc/`）は
全43件一致、tinylamb 4,790.234 ms、DuckDB 409.954 ms、5/43件でtinylambが高速。
これはwarmup 1回・測定3回の診断値であり、目標は未達。
`clickbench-duck-baseline0/`は旧日時データのため41/43件しか一致せず、性能判定に使用しない。

改善途中の記録（同じ100万行、warmup 1回・測定3回。最終判定ではない）:

| 保存先 | 正答 | tinylamb合計 ms | DuckDB合計 ms | 勝利数 |
| --- | ---: | ---: | ---: | ---: |
| `clickbench-duck-opt3` | 43/43 | 4,374.478 | 409.101 | 6/43 |
| `clickbench-duck-opt4` | 43/43 | 4,119.786 | 411.776 | 6/43 |
| `clickbench-duck-opt5` | 43/43 | 4,332.776 | 415.970 | 7/43 |
| `clickbench-duck-opt6` | 43/43 | 4,176.746 | 408.570 | 7/43 |
| `clickbench-duck-opt7` | 43/43 | 4,131.786 | 408.936 | 6/43 |
| `clickbench-duck-opt8` | 43/43 | 3,961.403 | 416.885 | 7/43 |
| `clickbench-duck-opt9` | 43/43 | 4,033.466 | 406.110 | 7/43 |
| `clickbench-duck-opt10` | 43/43 | 4,055.544 | 408.105 | 7/43 |
| `clickbench-duck-opt11` | 43/43 | 4,021.225 | 408.006 | 7/43 |
| `clickbench-duck-opt12` | 43/43 | 3,777.951 | 407.416 | 11/43 |
| `clickbench-duck-opt13` | 43/43 | 3,724.913 | 408.416 | 11/43 |
| `clickbench-duck-opt14` | 43/43 | 3,362.427 | 405.130 | 11/43 |
| `clickbench-duck-opt15` | 43/43 | 3,355.135 | 404.602 | 12/43 |
| `clickbench-duck-opt16` | 43/43 | 3,278.546 | 406.977 | 14/43 |
| `clickbench-duck-opt17` | 43/43 | 3,178.549 | 408.697 | 16/43 |
| `clickbench-duck-opt18` | 43/43 | 3,187.989 | 409.975 | 17/43 |
| `clickbench-duck-opt19` | 43/43 | 2,235.823 | 407.411 | 18/43 |
| `clickbench-duck-opt20` | 43/43 | 1,445.311 | 407.580 | 17/43 |
| `clickbench-duck-opt21` | 43/43 | 1,439.729 | 410.381 | 18/43 |
| `clickbench-duck-opt22` | 43/43 | 1,434.779 | 409.284 | 18/43 |
| `clickbench-duck-opt23` | 43/43 | 1,030.477 | 408.253 | 19/43 |
| `clickbench-duck-opt24` | 43/43 | 1,019.590 | 408.100 | 20/43 |
| `clickbench-duck-opt25` | 43/43 | 1,015.379 | 406.204 | 20/43 |
| `clickbench-duck-opt26` | 43/43 | 1,021.786 | 408.609 | 20/43 |
| `clickbench-duck-opt27` | 43/43 | 984.815 | 413.025 | 20/43 |
| `clickbench-duck-opt28`（実行TZ統一） | 43/43 | 958.047 | 412.368 | 20/43 |
| `clickbench-duck-opt29` | 43/43 | 977.291 | 415.721 | 20/43 |

`opt3`はBYTE_LENGTHのBytecode対応、列オフセットからの長さ集計、
値域が小さい整数キーの配列集計を追加。Q28は約400 msから5.359 ms
（DuckDB 10.332 ms）に短縮した。整数合計がDOUBLEでも厳密に表現できる
上限を確認し、それを超える場合は通常の集計を使う。
`opt4`は既存の整数GROUP BYキーから決まる算術式をハッシュキーから除き、
各グループの確定時に検算する。Q36は約226 msから23.401 msに短縮した。
LIMITで出力されないグループでもオーバーフローを検出する。

ストレージ形式の後方互換性は改善の制約にしない（ユーザー指定）。
現在の分析用列キャッシュは、既存の圧縮PAXとは別に展開した列を保持しており、
PAXの構築は先頭1024行のサンプルに限られる。
`opt5`では254種類を超える文字列にも共有辞書と16/32ビットコード列を適用した。
これは現時点では列キャッシュの符号化であり、永続ファイル形式の変更ではない。
辞書とコード列で20%以上小さくなる列だけ採用する。
初回測定では合計時間が悪化したため、この圧縮変更を性能改善の達成とは扱わない。
`opt6`では辞書の値に一度だけ述語を評価し、コード列を走査する処理を追加した。
`opt7`では整数列の存在判定用Bloomフィルターを追加した。陽性の場合は行を検証し、
スナップショットとレイアウトの検証を通った列ストアだけで不在判定を使う。
`opt8`ではDATE列と有効な日付文字列定数の比較を直接列走査へ接続した。
日付文字列はASTと同じパーサーで一度だけ解釈し、不正な文字列は従来の評価に戻す。
Q38は119.909 ms（opt7）から16.795 msに短縮したが、DuckDBの3.240 msには届いていない。
`opt9`では辞書の上限を約100万種類まで広げた（64 MiBの辞書サイズ制限と20%の
サイズ削減条件は維持）。COUNT(DISTINCT)は可視な通過行のコードを記録してから
一意の文字列を正規化する。Q6は8.565 ms（opt8）から3.904 msに短縮した。
`opt10`では物理ParallelScanに後から設定されるフィルターも事前判定へ接続した。
Q20は7.497 ms（opt9）から0.069 ms、DuckDBは0.267 ms。
`opt11`では辞書構築時に、異なるコードがSQL上でも異なる値であることを検証する。
証明できた辞書のCOUNT(DISTINCT)は、通過行のコードの出現記録だけで確定する。
同値のINTERVAL表記などがある場合は従来の値比較を使う。
`opt12`ではその証明をGROUP BYのCOUNT(*)にも使い、ワーカーごとのコード別配列を
合算してからTopNを選ぶ。文字列への復元は出力に残るグループだけに限定する。
Q13・Q34・Q35・Q37が新たにDuckDBを上回り、11/43件で高速となった。
Q34は4.490 ms対17.565 ms、Q35は4.557 ms対20.504 ms。
`opt13`では辞書コードをページごとの小さなバッファから列単位の連続領域へ移した。
ページ走査はこの領域の範囲を参照し、他列のフィルターがないCOUNT(*)は連続した
圧縮コード列を直接集計する。元のページ別バッファは解放し、二重保持しない。
Q34は2.913 ms、Q35は2.852 msへ短縮した。いずれもメモリー上の列キャッシュの改善。
`opt14`では単一文字列列に対する一部のスカラーGROUP BY式も辞書コードから集計する。
WHERE通過後のコードだけで式を一度評価し、同じ計算結果になるコードの件数を統合する。
LIMIT外のグループも評価してエラーを隠さない。Q43は106.608 msへ短縮したが、
DuckDBは2.361 msで、まだ大きな差がある。
回帰テストの実行時Status検証を強化した際、既存UNIONの数値昇格後に型情報が更新されず
上位の集計がINT64演算を選ぶ不具合も発見・修正した。
`opt15`ではINT64/DATEにも16/32ビット辞書コード索引を追加し、COUNT(DISTINCT)と
対応するCOUNT集約で利用する。コード索引は最大64 MiB/テーブルで、既存整数列も
保持するため、総メモリー削減ではない。Q5は1.868 ms対3.174 msで新たに上回った。
`opt16`では可視性と世代を検証した無条件スキャンに列ストア全体のfoldを追加した。
COUNT(DISTINCT)は連続するコードを直接走査し、Q5が0.212 ms対3.199 ms、
Q6が0.209 ms対1.683 msになった。フィルター・投影・更新前スナップショット・
消費後のEOF・コールバック失敗を回帰テストする。
`opt17`ではORDER BY集約の解決済みノードを型付き集約の計画にも渡す。
別ASTだった`ORDER BY COUNT(*)`が高速経路から外れる問題を解消し、
Q16は42.084 msから1.237 ms（DuckDB 3.749 ms）、Q17は91.642 msから
33.470 msに短縮した。
`opt18`では複数列の辞書コードをNULL用コード込みで64ビット以内のキーへ詰め、
スレッドごとの整数ハッシュ表でCOUNTを集計する。語彙がSQL上で一意であること、
コード列の長さ、スナップショット、メモリー予算を確認してから使う。
Q17は4.170 ms対5.832 ms、Q18は4.043 ms対5.564 msとなった。
他のクエリの変動により合計時間はopt17よりわずかに増えており、合計短縮の達成とは
扱わない。
`opt19`では複合キーの一部が日時抽出などの式でも、辞書の観測値を一度評価して
結果のコードへ写像する。Q19は1,006.241 msから52.243 ms（DuckDB 13.366 ms）に短縮。
`opt20`では単一文字列入力の辞書コードを先に数え、同じ元文字列の式と集約入力を
一度だけ評価する。COUNT・MIN/MAXと、合計が2^53以下に収まるBYTE_LENGTHのAVGに
限定し、フィルター・HAVING・最初の出現順を保つ。Q29は1,102.003 msから
308.261 ms（DuckDB 138.164 ms）に短縮した。勝利数は他クエリの変動で17件。
`opt21`では8ビット辞書と16/32ビット辞書の混在した複合キー、およびキー列への
定数フィルターもコード上で集計する。Q15は10.234 msから3.560 msへ短縮したが、
DuckDBの2.617 msには届いていない。
`opt22`では8ビットコード列も列ごとの連続領域で所有し、ページはその範囲を参照する。
ページ別コードバッファは二重保持せず、複合キー集約は圧縮列を直接読む。合併時の
ハッシュ表もワーカーが実際に作ったグループ数で確保する。Q15は1.931 ms対2.509 msで
新たに上回ったが、Q7の微小な変動で勝利数は18件のまま。ピークRSSは約5.60 GiBで
前回とほぼ同じだった。UINT64の型情報を失う回帰を検出・修正し、この経路は符号付き
キーに限定した。これは引き続きメモリー上の列キャッシュの変更である。
`opt23`は辞書コード上の複合キーを最大8列（合計64ビット以内）へ広げ、
単一WHENのCASEと、キー以外の列への定数比較もコード上で処理する。
NULL・空文字列・辞書にないELSE定数・OFFSET・符号なし整数のフォールバックを検証した。
Q40は410.953 ms（opt22）から14.658 msに短縮し、DuckDBの28.106 msを上回った。
全43件一致、19/43件で高速。合計は1,030.477 ms対408.253 msで、目標は未達。
通常およびASan/UBSanの集計回帰テスト162件、全テスト3,722件成功・既存3件スキップ。
全体391翻訳単位・396構成と今回の変更2翻訳単位のClang Tidyは指摘0件。

`opt24`では単一キー集計の他列フィルターも連続コード列で処理し、
単一・複合キーの定数IN条件を辞書マスクへ変換する。NULLを含むINやNOT IN、
辞書のない入力は従来の評価へ戻す。Q38は2.080 ms対3.243 msで新たに上回った。
全43件一致、20/43件で高速。合計は1,019.590 ms対408.100 msで、全面勝利は未達。
全テスト3,723件成功・既存3件スキップ、ASan/UBSan回帰163件、変更2翻訳単位の
Clang Tidyは指摘0件。SQL・式・GriffinのASan/UBSan付きファジングは各約10分、
それぞれ576・41,512・3,557ケースをエラーなしで完了した。
`opt25`では整数辞書コードを必要な1〜21ビットへ詰める。NULLは辞書の末尾コードで表し、
末尾には安全な64ビット読み取り用の7バイトを確保する。通常スキャンと集約を共通の
デコーダーへ接続した。整数の広域辞書とコード列は66,871,960→49,714,481バイトへ25.7%減り、
対象列は29→31列になった。RefererHashは18ビット、URLHashは19ビットで採用できた。
592 MBの元の整数配列はまだ保持しており、これは永続ファイルの圧縮率ではない。
ピークRSSは5,869,936→5,870,136 KiBで実質変わらず、プロセス全体のピーク削減は未達。
圧縮量の比較図は`clickbench-duck-opt25/storage.png` / `.svg`に保存した。
Q42は9.516→4.236 msへ短縮した一方、Q5・Q6のCOUNT(DISTINCT)は0.861・1.161 msへ悪化した。
合計1,015.379 ms対406.204 ms、全43件一致・20/43件で高速。全面勝利は未達。
全テスト3,724件成功・既存3件スキップ、ASan/UBSanでスキャン12・Executor226・集計163件成功。
変更4翻訳単位のClang Tidyは新規テストの書式3件を修正後に指摘0件。
ヘッダー変更を含む全体Clang Tidyはopt26で再検証済み。圧縮変更後の長時間ファジングもopt26で完了。
`opt26`では閉じた定数式のINリストを安全に畳み込み、`IN(-1, 6)`も辞書フィルターへ渡す。
列参照・パラメーター・揮発性関数・NULL・符号なし値・評価エラーを含む場合は通常評価へ戻し、
INの短絡によって隠れるエラーを先取りしない。非例外の辞書マスクだけを通過率順に並べ、
全行で真と証明できる条件を除く。既存16/32ビットコードの走査も幅ごとに分岐する形へ戻した。
Q41は13.337→2.379 ms、Q42は4.236→3.145 msへ短縮したが、DuckDBの1.938／1.830 msには未達。
Q6は1.161→0.263 msへ戻った一方、Q5の圧縮整数コード走査は0.861→0.855 msでほぼ変化なし。
全43件一致・20/43件で高速、合計は1,021.786 ms対408.609 msで全面勝利は未達。
クリーンビルド731工程でwarning 0件、全テスト3,725件成功・既存3件スキップ。
ASan/UBSanで型付き集計8・Executor226・集計回帰163件成功。
全体Clang Tidyは391翻訳単位・396構成で指摘0件。SQL・式・GriffinのASan/UBSan付きファジングも各約10分、621・64,384・4,022ケースをエラーなしで完了。
`opt27`では日時の固定幅表記を直接読み取り・整形し、時差がない場合の暦→ナノ秒→暦の
往復を省いた。うるう秒・端数秒・その他の表記は汎用パーサーを維持する。
日時切り詰めの引数も、すでに評価した値を再利用して二重評価を除いた。
年1〜9999・世紀のうるう年・固定時差・端数秒の回帰テストを変更前にも実行して期待値を確認。
関数単体のCallgrind命令数は73,945,387→38,960,387（5,000回評価）だが、これは実時間の比較値ではない。
Q43は104.303→75.738 ms、合計984.815 ms対413.025 msへ短縮した。
全43件一致・20/43件で高速、全面勝利は未達。全テスト3,727件成功・既存3件スキップ、
全ターゲットの増分ビルドwarning 0件。ASan/UBSanの関数5件・辞書集計9件成功。
変更2翻訳単位のClang Tidyは指摘0件。新規指摘21件と途中のテストの型エラーを修正して再検証した。
日時変更後のファジング3本はSQL 591・式54,111・Griffin 3,866ケースをエラーなしで完了。
各601秒前後の壁時計時間から測定用の停止13.156秒を引いた活動時間は約588秒。
`opt28`はベンチマーク条件の修正のみで、エンジンのソースはopt27と同一。
それまでDuckDBはUTC、nativeは既定のAmerica/Los_Angelesで実行していた。
現在の43クエリは両設定で同じ結果になるが、分単位の切り詰めでは時差変換のコストが異なった。
nativeもUTCに設定し、日単位切り詰めのプローブで異なる設定のバイナリを拒否する。
旧バイナリがプローブで失敗することを確認し、Pythonテスト15件・変更TUのClang Tidy指摘0件。
Q43は75.738→56.381 ms、合計958.047 ms対412.368 msとなったが、設定差による短縮であり
Optimizer/Executorの改善としては数えない。全43件一致・20/43件で高速、全面勝利は未達。
この静かな測定のためopt27ファジングを13.156秒停止・再開しており、活動時間の集計から差し引く。
`opt29`では日時切り詰めの単位トークンを行の列名として探索しない。これによりMINUTEなどの
名前から不要な行全体のSTRUCTを作る処理を除いた。複合キー集約はフィルターを通過した行を
先に選び、その行数を上限にハッシュ表を確保する。選択行ベクトルもメモリー予算に計上する。
Q43は56.381→32.707 ms、Q40は12.361→11.037 msへ短縮した一方、Q29は300.297→320.265 ms、
Q19は50.305→57.414 msなどに悪化し、合計は977.291 msへ増えた。合計改善の達成とは扱わない。
DuckDBは415.721 ms、全43件一致・20/43件で高速。全テスト3,728件成功・既存3件スキップ、
通常/ASan/UBSanの関数6件・辞書集計9件成功、増分全ターゲットビルドwarning 0件、
変更4翻訳単位のClang Tidy指摘0件。opt29のASan/UBSan付きファジングも完了し、SQL 567件・式61,039件・Griffin 3,516件を各601〜602秒、エラーなしで実行した。
合計悪化を調べるため、同じUTC設定・warmup 1回・測定7回・実行順反転で保存バイナリを再比較した。
`opt28-recheck7`は959.974 ms対407.669 ms・21勝、`opt29-recheck7`は918.169 ms対406.930 ms・22勝。
両方43/43一致、native合計は4.4%短縮し、先の合計悪化はこの再測定では再現しなかった。
Q29は306.122→298.291 ms、Q43は55.015→32.140 ms。微小差の勝利数は変動しており、全面勝利は未達。
`build/benchmarks/clickbench-duck-opt29/progress.png` / `.svg`には初期値からの
合計時間と勝利数の推移を保存し、opt28の実行タイムゾーン変更を明記した。
`progress.json`と`progress_plot.py`で再描画できる。

## 監査修正後の再測定と関数ディスパッチ改善（opt30 / opt31）

100万行のUTC部分データ、CPU 0・1、両エンジン2スレッド、warmup 1回・測定7回。
`opt30-post-audit`は監査修正後の現在のコードで取り直した基準値であり、
監査前の`opt29`との差を単独の最適化効果として扱わない。
既存のnative DBを別名に複製し、コピー元・先・SHA-256を
`native-database-copy.json`に記録した。入力ParquetとDuckDB DBは従来と同一。

| 保存先 | 正答 | tinylamb合計 ms | DuckDB合計 ms | 勝利数 |
| --- | ---: | ---: | ---: | ---: |
| `clickbench-duck-opt30-post-audit` | 43/43 | 929.614 | 407.772 | 21/43 |
| `clickbench-duck-opt31` | 43/43 | 795.076 | 406.170 | 21/43 |

両行とも`--reverse`、各クエリの中央値の合計。opt31は14.5%短縮したが、
DuckDBより速いのは21件であり、全43件での勝利は未達。
Q29は300.205→194.321 ms、Q43は31.510→20.442 ms。
実行順を変えた`clickbench-duck-opt31-forward`でも43/43一致、796.079 ms対
404.227 ms・21/43勝。`opt31/paired.png`と`.svg`は監査後の基準値との比較、
`paired.json`と`paired_plot.py`で再描画できる。全43件の対DuckDBプロットは
各測定ディレクトリの`comparison.png`と`.svg`に保存する。

`opt30-post-audit/q43.er`と`q43-functions.txt`に、gprofngのCPUサンプリングを保存した。
Q1による初期ロードとQ43の300回実行を含む診断用プロファイルで、比較の測定値には含めない。
関数名と文字列リテラルの比較が、長さを毎回求める`std::string`/`char*`比較を通っていた。
ASTとrelationalの関数ディスパッチを長さ付き`std::string_view`で比較するように変更した。
関数の順序・分岐条件・NULLやエラーの扱いを保ち、クエリ名や入力値の特別扱いは追加していない。

opt31の検証は`clickbench-duck-opt31/validation/summary.json`に保存。
全CTest 3,914項目（成功3,911・既存スキップ3・失敗0）、通常版の関数関連63テスト、
ASan/UBSan版59テスト成功。追加fuzzはASan/UBSan付きでSQL 276回・303秒、
式9,550回・301秒をエラーなしで完走した。変更2翻訳単位のClang-Tidy指摘0、
layeringは既存allowlist 78件のまま。増分の全ターゲットビルドにwarningはなかったが、
最終コードのclean-firstフルビルドとリポジトリ全体のClang-Tidy完了を示すものではない。

## 一意な集約キーと LIMIT の早期適用（opt32）

同じ100万行の部分データ・CPU 0/1・2スレッド・UTC・warmup 1回・測定7回。
クエリ結果キャッシュは無効で、全43件の結果とLIMITの同率境界を検証した。

| 測定 | 正答 | tinylamb合計 ms | DuckDB合計 ms | tinylamb勝利 |
|---|---:|---:|---:|---:|
| `clickbench-duck-opt31`（reverse） | 43/43 | 795.076 | 406.170 | 21/43 |
| `clickbench-duck-opt32`（reverse） | 43/43 | 642.350 | 404.978 | 23/43 |
| `clickbench-duck-opt32-forward` | 43/43 | 639.372 | 406.612 | 23/43 |

各クエリの中央値の合計。reverse同士では19.2%短縮した。
Q32は11.328→0.033 ms、Q33は152.435→0.042 ms。
全43件でDuckDBを上回る目標は未達で、標準の約1億行は未測定。

整数辞書を完全に構築した際、非NULLの異なる値の数が行数と一致するなら、
圧縮に不向きで辞書を破棄する場合も正確な一意性の証明を保持する。
このキーを含むGROUP BYでは各グループが1行になるため、ORDER BYがCOUNT(*)と
定数だけなら全グループが同率になる。スナップショットと列ストアの有効性を確認し、
直接列参照の非例外的な集約・フィルターだけの場合にLIMIT+OFFSETまでで走査を終了する。
計算式、HAVING、WITH TIES、非定数の順位、未対応の集約修飾は既存経路を使う。
集約値は既存のAggregateAccumulatorで計算し、クエリ名やデータ値の特別扱いはしない。

`opt32/validation/summary.json`に検証範囲を記録。
全CTest 3,916項目（成功3,913・既存スキップ3・最終失敗0）。リンク中に起動できなかった
2項目はリンク完了後の再実行で成功した。ASan/UBSan版193テスト成功、SQL fuzz 281回・
302秒、式fuzz 9,895回・301秒がエラーなし。変更4翻訳単位のClang-Tidy指摘0、
layeringの既存allowlistは78件のまま。増分全ターゲットビルドにwarningなし。
最終コードのclean-firstフルビルドとリポジトリ全体のClang-Tidyは引き続き未完了。

全件の対DuckDBプロットは各測定先の`comparison.png` / `.svg`、
opt31との比較は`opt32/paired.png` / `.svg`と再描画用`paired.json` / `paired_plot.py`。
`opt32/q22-profile/`には診断専用CPUプロファイルと次の改善候補を保存した。
検証ジョブと同時に採取したため、その時間を性能比較の値として扱わない。
LIKEの残余式による行復元と式評価が大きく、直接列・辞書上でのフィルターが次の候補。

## 文字列 LIKE の直接列・辞書評価（opt33）

opt32と同じ100万行・CPU 0/1・2スレッド・UTC・warmup 1回・測定7回。

| 測定 | 正答 | tinylamb合計 ms | DuckDB合計 ms | tinylamb勝利 |
|---|---:|---:|---:|---:|
| `clickbench-duck-opt32`（reverse） | 43/43 | 642.350 | 404.978 | 23/43 |
| `clickbench-duck-opt33`（reverse） | 43/43 | 493.458 | 404.817 | 24/43 |
| `clickbench-duck-opt33-forward` | 43/43 | 498.466 | 405.967 | 23/43 |

reverse同士では23.2%短縮。Q22は80.038→12.282 ms、Q23は95.119→23.454 ms。
forwardのQ22は12.678 ms対12.676 msでほぼ同率なので、安定した勝利とは扱わない。
全43件の正答とLIMIT同率境界を検証済みだが、全件勝利は未達。標準の約1億行は未測定。

単純なLIKE/NOT LIKEを、完全一致・前方一致・後方一致・部分一致としてコンパイルし、
直接列と辞書で同じ判定関数を使う。辞書が十分小さい場合はパターンを辞書値ごとに評価し、
各行はコードで判定する。NULLは不採用。途中のワイルドカード、`_`、照合順序付きの
未対応式などは既存の式評価を使う。クエリ固有のパターンや入力値は組み込まない。

ASTとの比較に加え、辞書あり／なし、1／2スレッド、集約・CASE・OR・古いスナップショットを検証。
全CTest 3,918項目（成功3,915・既存スキップ3・失敗0）、ASan/UBSan 187テスト成功。
追加fuzzはSQL 274回・式9,410回、各301秒をエラーなしで完走。
変更4翻訳単位のClang-Tidyは修正後0件、layeringの既存allowlistは78件のまま。
増分全ターゲットビルドにwarningなし。最終clean-firstフルビルドと全体Clang-Tidyは未完了。
詳細は`opt33/validation/summary.json`、全件の比較は`comparison.png` / `.svg`、
opt32との比較は`paired.png` / `.svg`と再描画用`paired.json` / `paired_plot.py`に保存する。
`q19-profile/`は検証と同時に採取した診断専用プロファイルと、残るコストの調査記録。

## 日時抽出の型判定と LIKE 辞書評価の費用判定（opt34）

同じ100万行・CPU 0/1・2スレッド・UTC・warmup 1回・測定7回。

| 測定 | 正答 | tinylamb合計 ms | DuckDB合計 ms | tinylamb勝利 |
|---|---:|---:|---:|---:|
| `clickbench-duck-opt34`（reverse） | 43/43 | 455.205 | 406.398 | 24/43 |
| `clickbench-duck-opt34-forward` | 43/43 | 447.550 | 404.490 | 24/43 |

opt33のreverseから合計7.8%短縮。Q19は47.530→24.915 ms、Q23は23.454→5.811 ms。
Q23は両順序でDuckDBより高速。Q22は両順序でわずかに遅く、全件勝利は引き続き未達。
全43件の結果とLIMIT同率境界を検証済み。標準の約1億行は未測定。

EXTRACTが空白を含む日時文字列をINTERVALと誤認し、日・タイムゾーン・不正値の扱いを
誤る問題を回帰テストで再現して修正した。INTERVALは型タグで判別し、ASTの共通実装へ渡す。
符号付きINTERVALの各成分抽出をASTで修正し、年・月・日のBytecode評価も共通化した。
先行LIKE述語の辞書通過率を後続述語の費用見積もりに使い、少数の行しか残らない場合は
後続の大きな辞書全体を評価せず、行ごとに判定する。推定値で回答を省略することはない。

全CTest 3,921項目（成功3,918・既存スキップ3・失敗0）、ASan/UBSan 258テスト成功。
SQL 270回・式9,698回のfuzzが各301秒で成功。変更7翻訳単位の最終Clang-Tidy指摘0件、
増分全ターゲットビルドwarning 0件、layeringの既存allowlistは78件のまま。
最終clean-firstフルビルドと全体Clang-Tidyは未完了。
`opt34/validation/summary.json`、全件の`comparison.png` / `.svg`、前版比較の
`paired.png` / `.svg`と再描画用データを保存した。`q19-profile/`は検証と重ねて採取した
診断用プロファイルで、比較時間には使わない。残るQ19の主なコストはグループハッシュ表の更新。

## 不採用にしたハッシュ表内の件数格納（opt35）

複合キーCOUNTの件数と最初の行位置をハッシュ表の64ビット値へ格納し、
更新時の別配列アクセスをなくす案を検証した。正答は保ったが、性能が悪化したため
**実装はopt34へ戻した**。標準規模は未測定。以下も100万行・同一条件の7回中央値。

| 測定 | 正答 | tinylamb合計 ms | DuckDB合計 ms | tinylamb勝利 |
|---|---:|---:|---:|---:|
| `clickbench-duck-opt35`（reverse、不採用） | 43/43 | 464.797 | 406.269 | 23/43 |
| `clickbench-duck-opt35-forward`（不採用） | 43/43 | 466.450 | 408.936 | 22/43 |

reverseではQ17が4.528→6.294 ms、Q18が4.445→6.130 ms、Q19が24.915→26.494 ms。
forwardでもこれらの悪化が再現した。統合時に空スロットを含むハッシュ表全体を走査し、
TopN用の配列も別に生成する追加コストが原因候補。改善として採用しない。
ワーカー間の統合、72,000件のCOUNT、NULL、同率順位・OFFSET・空結果を検証する
新しい回帰テストは残した。1・2・4ワーカーで確認している。

候補版は全CTest 3,922項目（成功3,919・既存スキップ3）、ASan/UBSan 180件、
SQL fuzz 268回/302秒、式fuzz 9,546回/301秒が成功。変更2翻訳単位のClang-Tidyは0件。
これらは候補版の検証記録であり、採用版の性能として引用しない。
実装を戻した後の検証は`opt35/validation/restored-summary.json`に別記する。
復元後も全CTest 3,922項目が失敗なし（既存スキップ3）、ASan/UBSanの関連5件が成功。
ReleaseバイナリのSHA-256は採用済みopt34と完全一致し、その比較結果を現状の結果として使える。
復元後の増分全ターゲット・Release・ASanビルドはwarning 0件。
候補のソース差分・バイナリ・比較データ・プロットは履歴として保存。
`q29-profile/`には次の候補の診断資料を保存した（検証と同時に採取、比較時間には不使用）。

## 辞書集約の初出順を走査中に記録（opt36）

採用済みopt34と同じ100万行・CPU 0/1・2スレッド・UTC・warmup 1回・測定7回。
opt35の不採用実装は含まない。

| 測定 | 正答 | tinylamb合計 ms | DuckDB合計 ms | tinylamb勝利 |
|---|---:|---:|---:|---:|
| `clickbench-duck-opt36`（reverse） | 43/43 | 437.707 | 405.055 | 25/43 |
| `clickbench-duck-opt36-forward` | 43/43 | 432.338 | 404.992 | 24/43 |

opt34とのreverse同士で合計3.8%短縮。Q29は195.426→180.309 ms、forward同士でも
189.241→176.273 msへ短縮した。Q22はreverseで勝ち、forwardで負けたため、
安定した勝利とは扱わない。全43件の正答とLIMIT同率境界を検証済みだが、全件勝利は未達。
標準の約1億行は未測定。

`TryFoldDictionaryAggregateRows`は、コード別件数が初めて0から増える時点でコードを
順序配列へ追加する。初出の行位置を別配列に保存して後でソートする処理を省いた。
同じ物理走査の初出順を保ち、作業領域は1コードあたり24→16バイトとなる。
16/32ビット辞書、NULL、重複、通常の行集約との全結果・順序一致、スカラーキーによる
再集約、フィルター・OFFSET、コールバック失敗の伝播を回帰テストした。

全CTest 3,923項目（成功3,920・既存スキップ3・失敗0）、ASan/UBSan 181件成功。
SQL 275回・式9,550回のfuzzは各301秒で成功。変更2翻訳単位のClang-Tidyは指摘0件、
最終増分全ターゲット・Release・ASanビルドwarning 0件。layering allowlistは78件のまま。
最終clean-firstフルビルドと全体Clang-Tidyは未完了。
`opt36/validation/summary.json`、全件の`comparison.png` / `.svg`、前版比較の
`paired.png` / `.svg`と再描画用データを保存した。
`q36-profile/`は検証と重ねて採取した診断専用資料。次の候補は、依存する整数GROUP BY式を
全観測グループで検証しながら辞書コード集約へ接続する改善で、opt36では未実装。

## 依存する整数 GROUP BY 式と辞書集約（opt37）

同じ100万行・CPU 0/1・2スレッド・UTC・warmup 1回・測定7回。

| 測定 | 正答 | tinylamb合計 ms | DuckDB合計 ms | tinylamb勝利 |
|---|---:|---:|---:|---:|
| `clickbench-duck-opt37`（reverse） | 43/43 | 418.379 | 404.059 | 25/43 |
| `clickbench-duck-opt37-forward` | 43/43 | 418.723 | 402.205 | 25/43 |

opt36とのreverse同士で合計4.4%短縮。Q36は23.440→7.374 ms（68.5%短縮）、
forwardでも23.429→7.602 ms。DuckDBの3.8 msには届いておらず、全件勝利は未達。
全43件の正答とLIMIT同率境界を検証済み。標準の約1億行は未測定。

単一の整数辞書キーから決まるGROUP BY式を持つCOUNT集約にも、コード別件数集計を適用した。
WHERE通過後に観測された全コードの依存式をBytecodeで評価し、表示とORDER BYで共用する。
LIMIT外のオーバーフローを隠さず、WHEREで除外されたコードは評価しない。
依存式の計算結果を保持する作業領域はサイズ計算のオーバーフローを検査し、
クエリのメモリー予算へ計上する。クエリをまたぐ結果キャッシュは追加しない。
未対応の型・形は従来処理へ戻す。

NULL、複数の依存式、列投影、隠れたORDER BYキー、同率順位・OFFSET・LIMIT 0、
未出力グループのエラー、空入力、符号なしキーのフォールバック、更新前後のスナップショットを検証。
全CTest 3,924項目（成功3,921・既存スキップ3・失敗0）、ASan/UBSan 182件成功。
SQL 279回・式9,870回のfuzzは各301秒で成功。変更2翻訳単位のClang-Tidy指摘0件、
増分全ターゲット・Release・ASanビルドwarning 0件、layering allowlistは78件のまま。
最終clean-firstフルビルドと全体Clang-Tidyは未完了。
`opt37/validation/summary.json`、全件比較の`comparison.png` / `.svg`、前版比較の
`paired.png` / `.svg`と再描画用データを保存した。
`q9-profile/`は検証と重ねた診断資料と次の候補の記録で、比較時間には使わない。

## 依存する整数式の検算付きカーネル（opt38）

辞書集約で評価する整数式に、プラン生成時にコンパイルした整数カーネルを再利用する。
観測済みの非NULLキーにだけ適用し、NULLまたはカーネル失敗時は従来のBytecodeで評価する。
内部カーネルのオーバーフローStatusとASTのStatusには違いがあるため、失敗を直接返さず、
Bytecode経由で公開APIのエラー分類を保つ。LIMIT外の観測済みキーも検算する。

同じ100万行・CPU 0/1・2スレッド・UTC、warmup 1回・測定7回:

| 保存先 | 正答 | tinylamb合計 ms | DuckDB合計 ms | 勝利数 |
| --- | ---: | ---: | ---: | ---: |
| `clickbench-duck-opt38`（reverse） | 43/43 | 414.322 | 403.710 | 26/43 |
| `clickbench-duck-opt38-forward` | 43/43 | 417.105 | 404.822 | 25/43 |

Q36は前版の7.374 msから2.413 msへ短縮し、DuckDBの3.852 msより高速になった。
順序を変えた再測定でも2.409 ms対3.803 msで勝利した。
一方、Q22はreverseでは勝利、forwardでは12.642 ms対12.535 msで敗れており、
全43件の勝利は未達。標準規模の約1億行は未測定。

整数境界値・複合式・NULLについてAST/Bytecodeとの一致を検証し、
通常CTestは3,922件成功・3件skip・失敗0、ASan/UBSanは211件成功。
SQL/式oracle fuzzを各301秒実行（274/9,503入力）し、異常なし。
変更した4翻訳単位のClang Tidy指摘0、増分の全ターゲットビルドwarning 0。
最終コードのclean-firstフルビルドと全体Clang Tidyは未完了。
検証ログとソースハッシュは`opt38/validation/`、比較プロットは`comparison.png`と
`paired.png`、再生成用データとスクリプトは`paired.json`と`paired_plot.py`に保存した。
`q36-profile/`は検証処理と重ねた診断用プロファイルであり、比較時間には使っていない。

## 辞書コードのビットマップによるグループ別DISTINCT（opt39）

単一辞書キーのGROUP BYと、整数/DATE列のCOUNT(DISTINCT)に、ワーカー専用の
ビットマップを追加した。同じ引数のCOUNT(DISTINCT)とCOUNT(*)を併用できる。
正確な辞書サイズの積をオーバーフロー検査し、ビットマップ合計64 MiB以内かつ
クエリのメモリ予算に収まる場合だけ使用する。それ以外は既存の集計へ戻す。
NULL引数は除外し、NULLキーや引数が全NULLのグループは保持する。
並列ワーカーのビットマップをORしてからpopcountすることで、ワーカー間の重複も除く。
COUNT(*)、最初の出現位置、フィルター、依存するキー式、LIMITの処理は共有する。
これはクエリ中の作業領域であり、永続ストレージ形式や結果キャッシュの変更ではない。

同じ100万行・CPU 0/1・2スレッド・UTC、warmup 1回・測定7回:

| 保存先 | 正答 | tinylamb合計 ms | DuckDB合計 ms | 勝利数 |
| --- | ---: | ---: | ---: | ---: |
| `clickbench-duck-opt39`（reverse） | 43/43 | 400.289 | 403.288 | 27/43 |
| `clickbench-duck-opt39-forward` | 43/43 | 400.598 | 409.720 | 26/43 |

Q9は前版の22.350 msから2.280 msへ短縮し、DuckDBの4.375 msより高速になった。
順序を変えた再測定でも2.293 ms対4.385 msで勝利した。
両測定で合計時間も短くなったが、Q22は勝敗が入れ替わっており、全43件の勝利は未達。
標準規模の約1億行は未測定。SUM/AVGを併用するQ10はこの新経路の対象外で、
26.967 ms対8.492 msという差が残る。

新規回帰テストでは1/2/4ワーカー、論理幅8/16/32ビットの辞書コード、NULL、同順位、
LIMIT/OFFSET、WHERE、旧/新スナップショットを検証した。
大きな辞書で64 MiB制限を超える場合と、1 MiBのクエリ予算でも通常経路と一致した。
通常CTestは3,924件成功・3件skip・失敗0。ASan/UBSanの回帰183件が成功し、
サイズ定数の型修正とテスト追加後に新規回帰2件・式差分19件を再実行して成功した。
SQL/式oracle fuzzは各301秒（270/8,656入力）で異常なし。
変更した2翻訳単位のClang Tidy指摘0、増分全ターゲットビルドwarning 0。
最終コードのclean-firstフルビルドと全体Clang Tidyは未完了。

結果・サンプル・実行バイナリは`opt39/`と`opt39-forward/`、検証ログと今回だけの差分は
`opt39/validation/`、プロットと再生成スクリプトは`comparison.png` / `.svg`および
`paired.png` / `.svg`、`paired.json`、`paired_plot.py`に保存した。
`q43-profile/`は検証と重ねた診断で、比較時間には使っていない。
Q43では日時関数の評価コストが残るため、次の改善候補として記録した。

## 辞書集約への検算済みSUM・AVG融合（opt40）

単一辞書キーの集約に、非NULLの符号付き整数列に対するSUM・AVGを追加した。
COUNT(*)、COUNT(DISTINCT)と同じ走査で、列バッファからワーカー専用の合計へ加算する。
全列の厳密な最小・最大値と行数から、どの部分集合・グループ・途中結果も
SUMではINT64範囲、AVGでは正確なDOUBLE整数表現範囲内になることを確認する。
証明できない値、NULL、符号なし列、未対応の型や式は従来経路へ戻す。
AVGの整数加算はこの証明がある場合だけ行い、引数をDOUBLEへ変換する際の丸めも保つ。
追加の集約状態は既存のクエリメモリ予算に算入する。

同じ100万行・CPU 0/1・2スレッド・UTC、warmup 1回・測定7回:

| 保存先 | 正答 | tinylamb合計 ms | DuckDB合計 ms | 勝利数 |
| --- | ---: | ---: | ---: | ---: |
| `clickbench-duck-opt40`（reverse） | 43/43 | 379.240 | 410.876 | 27/43 |
| `clickbench-duck-opt40-forward` | 43/43 | 394.809 | 412.865 | 27/43 |

Q10は前版の26.967 msから2.988 msへ短縮し、DuckDBの8.828 msより高速になった。
再測定でも3.146 ms対8.879 msで勝利した。Q9・Q36の勝利も維持している。
Q22は今回は両測定で敗北。合計の測定間差にはQ29（183.519→192.907 ms）や
Q19（22.112→26.069 ms）の変動が含まれる。全43件の勝利は未達で、
標準規模の約1億行も未測定。

新規回帰で1/2/4ワーカーの混合集約、SUM/AVG順のTopN、フィルター、OFFSET、
CAST付きAVG、NULLと旧/新スナップショットを検証した。
別のappend-onlyテーブルで、AVGのDOUBLE丸めと、LIMIT外のグループで発生する
SUMオーバーフローもそれぞれ独立して検証した。最初のテストはDELETE後のテーブルで
列ストアを要求して失敗したため、列ストアが実際に使用可能な新しいテーブルへ修正した。
本体コードはその修正前後で変更していない。

最終CTestは3,925件成功・3件skip・失敗0。ASan/UBSanは既存回帰184件に加え、
修正後の新規回帰1件と式差分19件が成功。SQL/式oracle fuzzも302/301秒、
261/9,567入力で異常なし。変更した2翻訳単位のClang Tidy指摘0、
増分全ターゲットビルドwarning 0。最終clean-firstフルビルドと全体Clang Tidyは未完了。

結果と実行バイナリは`opt40/`・`opt40-forward/`、検証履歴・今回だけの差分は
`opt40/validation/`、比較プロットは`comparison.png` / `.svg`と`paired.png` / `.svg`。
前版比較は`paired.json`と`paired_plot.py`から再生成できる。
日時関数・複合キー集約・選択性の高いフィルターの次候補は
`validation/follow-up.json`に記録している。

## UTC日時の集約キーを整数化（opt41）

`TIMESTAMP_TRUNC`の秒・分・時・日単位について、UTCかつ検証済みの固定長日時表記を
`YYYYMMDDhhmmss`の整数キーへ変換する。辞書の観測済みエントリを整数で再集約し、
TopNで保持した`OFFSET + LIMIT`件のキーだけを出力用文字列に戻す。
年0001〜9999で元のUTC文字列の等値・順序を保ち、NULLは独立したグループとして扱う。
非対応の表記・小数秒・UTC以外・うるう秒は通常評価へ戻す。
WHEREを通過した全キーをLIMIT前に検査するため、落選するグループのエラーも隠さない。

引き続き**100万行の開発用サブセット**。CPU 0/1・2スレッド・UTC、
ウォームアップ1回＋測定7回の中央値で比較した。約1億行の標準規模は未測定。

| 実行 | 正答 | tinylamb合計 ms | DuckDB合計 ms | tinylamb勝利数 |
|---|---:|---:|---:|---:|
| `clickbench-duck-opt41`（reverse） | 43/43 | 369.635 | 410.958 | 28/43 |
| `clickbench-duck-opt41-forward` | 43/43 | 367.424 | 417.310 | 27/43 |

Q43は前版21.767/21.438 msから**4.719/4.692 ms**へ約78%短縮した。
DuckDBは2.318/2.283 msで、Q43ではまだ負けている。
全件合計は前版から2.5%/6.9%短縮、DuckDB比10.1%/12.0%短縮。
Q22の勝敗は実行間で変わり、全43件での勝利目標は未達。
Q29など未変更のクエリにも測定変動があり、合計の改善率すべてを日時処理へ帰属させない。

通常CTestは3,930件中3,927件成功・3件skip・失敗0。
ASan/UBSanは日時関数12件・式差分19件・relational 186件の計217件成功。
SQL/式oracle fuzzは302/301秒、278/9,493入力で異常なし。
Clang Tidyは変更した4翻訳単位の最終指摘0件。新規テストのbraces指摘1件を修正し、
その翻訳単位を再解析、対象回帰テストを再ビルド・再実行した。
増分全ターゲット・Release・ASanビルドのwarningは0。
最終clean-firstフルビルドと全体Clang Tidyは未完了。

実行バイナリ・回答・各サンプルは`opt41/`と`opt41-forward/`、
検証ログ・今回だけの差分・ソースハッシュは`opt41/validation/`に保存。
`comparison.png` / `.svg`はDuckDB比較、`paired.png` / `.svg`は前版比較で、
後者は`paired.json`と`paired_plot.py`から再生成できる。
`profile/`のサンプリングは検証と並行実行した診断専用で、比較時間には使用していない。
残る辞書走査・再集約・日時解析の費用と次候補は`profile/next-steps.json`に記録。

## 複合辞書キーへのSUM・AVG融合（opt42b）

複数列の辞書コードをまとめてGROUP BYする経路にも、SUM・AVGを融合した。
単一キーと共通の厳密な列範囲検査を使い、INT64の全中間和が安全で、AVGでは
DOUBLEへの昇格後の加算と一致する場合だけ整数で蓄積する。
NULLを含む集約入力、unsigned、範囲を証明できない値などは通常経路へ戻す。
ワーカー間の合算と追加領域をメモリ予算へ含め、最初の出現位置による同率順序を保持する。

初版`opt42`はQ31を短縮した一方、COUNTのみのQ19・Q40に約7〜12%の遅延が出た。
保存済みバイナリをABBA順で交互に実行して再現したため、初版は不採用。
COUNT専用経路から数値集約の更新・分岐をコンパイル時に除く`opt42b`へ修正した。
再確認の21サンプル中央値はQ19が旧21.22/23.35 ms・新22.51/21.58 ms、
Q40が旧10.37/10.54 ms・新10.15/10.04 msだった。

採用版の全43件比較も**100万行の開発用サブセット**で、CPU 0/1・2スレッド・UTC、
ウォームアップ1回＋測定7回の中央値。約1億行の標準規模は未測定。

| 実行 | 正答 | tinylamb合計 ms | DuckDB合計 ms | tinylamb勝利数 |
|---|---:|---:|---:|---:|
| `clickbench-duck-opt42b`（reverse） | 43/43 | 357.982 | 412.601 | 30/43 |
| `clickbench-duck-opt42b-forward` | 43/43 | 385.122 | 429.889 | 29/43 |

Q31は前版9.524/9.653 msから**2.082/2.000 ms**へ約78〜79%短縮。
DuckDBの3.062/3.099 msより両測定で高速になった。
合計はDuckDB比13.2%/10.4%短縮。ただし前版比ではreverseが3.2%短縮、
forwardが4.8%増加しており、全体合計の改善が一貫して再現したとはしない。
forwardのQ21は19.966→34.088 ms、Q29は183.347→191.605 msと変動した。
これらも別途ABBA順・21サンプルで確認し、Q21は旧18.24/17.70 ms・新17.98/17.61 ms、
Q29は旧189.27/184.41 ms・新190.09/186.86 msだった。
Q24の勝敗は実行間で変わる。全43件勝利の目標は未達。

最終ソースで通常CTestは3,931件中3,928件成功・3件skip・失敗0。
ASan/UBSanはrelational 187件＋式差分19件の計206件成功。
SQL/式oracle fuzzは302/301秒、270/9,852入力で異常なし。
変更した2翻訳単位のClang Tidy指摘0、増分全ターゲット・Release・ASanビルドwarning 0。
最終clean-firstフルビルドと全体Clang Tidyは未完了。

採用版の回答・各サンプル・実行バイナリは`opt42b/`と`opt42b-forward/`、
検証ログ・差分・ソースハッシュは`opt42b/validation/`に保存。
追加測定は`count-check-summary.json`・`unmodified-check-summary.json`と再実行スクリプト、
初版の性能退行は`opt42/validation/rejected-summary.json`に記録した。
比較プロットは`comparison.png` / `.svg`、前版比較は`paired.png` / `.svg`で、
後者は`paired.json`と`paired_plot.py`から再生成できる。
`profile-pre-specialization/`はCOUNT分離前のQ29診断で、検証と並行実行したもの。
比較時間には使用せず、文字列コピー・正規表現評価などの次候補を記録している。

## 正規表現の置換処理を再利用し、VARCHAR引数を参照渡し（opt43）

`ReplaceCheckedRegex`をstring_view入力へ変更し、パターンと置換文字列の組に対する
検証・置換形式の変換・コンパイル結果をスレッドごとに最大8件保持する。
4096バイトを超えるパターン・置換文字列は一時的なプログラムで処理し、キャッシュを肥大化させない。
キャッシュはキーを所有し、入力文字列やクエリ結果は保持しない。
ASTとrelationalの両評価器はVARCHAR引数の余分なコピーを避け、他の型の文字列変換は維持する。
既存の危険な正規表現の拒否、置換エスケープ・参照番号の検査、NULLの扱い、
RE2のLatin1処理と対応外パターンのstd::regexへの切り替えを保つ。

測定は引き続き**100万行の開発用サブセット**、CPU 0/1・2スレッド・UTC、
ウォームアップ1回＋測定7回の中央値。約1億行の標準規模は未測定。

| 実行 | 正答 | tinylamb合計 ms | DuckDB合計 ms | tinylamb勝利数 |
|---|---:|---:|---:|---:|
| `clickbench-duck-opt43`（reverse） | 43/43 | 331.555 | 408.750 | 30/43 |
| `clickbench-duck-opt43-forward` | 43/43 | 338.582 | 410.841 | 30/43 |

Q29は前版185.555/191.605 msから**165.006/167.115 ms**へ11.1%/12.8%短縮。
ただしDuckDBは137.410/137.608 msで、Q29の勝利にはまだ届かない。
全件合計はDuckDB比18.9%/17.6%短縮した。
前版forwardにはQ21などの大きな変動があったため、前版比の合計短縮をすべて本変更の効果とはしない。
Q29単独の21サンプル・ABBA交互測定でも旧183.05/187.19 ms、新162.83/164.95 msと短縮した。
Q22・Q24は勝敗境界に近く、特にforwardの差は約0.006/0.037 msにとどまる。
全43件勝利の目標は未達。

最終ソースの通常CTestは3,935件中3,932件成功・3件skip・失敗0。
ASan/UBSanのstd::regex構成で261件、RE2を有効にした単独の正規表現テストで追加10件成功。
両方式の共通APIで埋め込みNUL、終端なしの部分参照、キャッシュキーの所有、入れ替え、
サイズ上限、空文字列マッチを確認し、AST・relationalのテストではNULLと型変換も確認した。
SQL/式oracle fuzzはstd::regex構成で302/301秒、259/9,035入力で異常なし。
変更した6翻訳単位とRE2有効時の共通実装、計7コンパイル構成のClang Tidy最終指摘0。
optional参照・テストのvector予約の初回指摘を修正し、対象を再解析した。
増分全ターゲット・Release・ASan・RE2単独ビルドwarning 0。
最終clean-firstフルビルドと全体Clang Tidyは未完了。

回答・各サンプル・実行バイナリは`opt43/`と`opt43-forward/`、
検証ログ・差分・ソースハッシュ・RE2用サニタイザーのコンパイルコマンドは`opt43/validation/`。
`q29-check-summary.json`と`check_q29.py`には追加の交互測定を保存した。
この追加測定はoptional参照の調整前で、そのバイナリも`tinylamb_pre_tidy_compare`として保持する。
比較プロットは`comparison.png` / `.svg`、前版比較は`paired.png` / `.svg`。
`profile-before-optional-fix/`は検証と並行した診断専用で、比較時間には使用していない。
残る引数Valueのコピーを減らす候補と必要な条件を同ディレクトリの`next-steps.json`に記録した。

## 引き継いだ変更と疎な辞書DISTINCTのradix集計（opt44c）

TPC-H Q5の結合列取り込み修正、正規表現VM、ストレージ等の未コミット変更を
全体ビルド・全テストで検証して引き継いだ。Q5は修正前の保存バイナリで
「正解5行に対して0行」を再現し、修正後のSF1三者比較で22/22件の一致を確認した。
TPC-Hの採用結果は `build/benchmarks/tpch-three-20261111/FINAL-REPORT.md` と
`final-comparison.png`。その測定は以下の新規DISTINCT改善より前の固定バイナリを使用する。

今回の変更は、辞書GROUP BYとCOUNT(DISTINCT)の直積ビットマップが64 MiBを
超える場合に、実際に現れたグループ・引数のコード対だけを64ビット整数として保持する。
各ワーカーで定数バイトを除くradix sortと重複排除を行い、ソート済み集合を厳密に併合する。
通常の小さなビットマップ経路はテンプレートで分離し、行ごとの追加分岐を避ける。
値の復元は出力に残るグループだけ。クエリ結果のキャッシュやクエリ番号による分岐はない。

保持・ソート・併合の領域は全入力行数の2倍のコード対で上限を見積もり、64 MiBと
クエリのメモリ予算を超える場合は通常経路へ戻す。選択率の推測は使わない。
NULL引数、全NULLのグループ、NULLキー、OFFSET、LIMIT同率、フィルターと
スナップショットの扱いを保つ。辞書構造のフィールド配置も整理し、616から584バイトへ縮小した。
これはメモリ上の配置変更で、永続フォーマットの変更ではない。

以下は100万行の開発用部分データ、CPU 0/1・2スレッド・UTC、warmup 1＋測定7回。
ビルド・検査・fuzzを終了してから測定した。標準規模の約1億行は未測定。

| 実行 | 正答 | tinylamb合計 ms | DuckDB合計 ms | tinylamb勝利数 |
|---|---:|---:|---:|---:|
| `clickbench-duck-inherited-20261111`（reverse、引継ぎ時） | 43/43 | 295.861 | 405.913 | 29/43 |
| `clickbench-duck-inherited-20261111-forward` | 43/43 | 307.564 | 408.025 | 28/43 |
| `clickbench-duck-opt44c`（reverse、採用） | 43/43 | 296.940 | 406.048 | 29/43 |
| `clickbench-duck-opt44c-forward`（採用） | 43/43 | 295.934 | 408.433 | 30/43 |

Q14は引継ぎ時の10.074/9.901 msから1.602/1.595 msへ約84%短縮し、
DuckDBの3.103/3.119 msを上回った。旧版・radix版・配置整理後の版をABC/CBA順で
21サンプルずつ追加測定しても、Q14は旧9.928/10.152 msに対し新1.597/1.594 ms。
Q9は旧2.521/2.505 ms、新2.493/2.490 ms。Q19の本測定reverseでの増加は
追加測定では再現せず、旧24.655/24.930 ms、新23.602/22.348 msだった。
Q29は追加測定でも新123.365〜128.825 msと変動があり、合計時間の差をすべて
本変更の効果とはしない。最終合計はDuckDB比26.9%/27.5%短いが、全43件勝利は未達。
両実行で負けるのはQ2・3・4・19・21・22・24・25・26・27・41・42・43。

全体CTestは3,940件中3,937成功、3 skip、失敗0。ASan/UBSanの関連19件と
配置整理後のDISTINCT 3件が成功。SQL/式oracle fuzzは各301秒、271/9,089入力で異常なし。
1/2/4ワーカー間の重複、フィルター、低メモリ時の通常経路、NULL、空入力、LIMIT 0、
MVCCを専用の18万行テストで確認した。実装・回帰テストの2翻訳単位の最終Clang Tidy指摘0。
初回の構造体パディングとテスト文字列結合の指摘は修正し、再解析した。
通常全ターゲット・Release・ASanの増分ビルドwarning 0、layering/diff検査成功。
最終clean-firstフルビルドと全体Clang Tidyは未完了。

採用結果・固定バイナリ・ソース差分・回答・全サンプルは `opt44c/` と
`opt44c-forward/`、比較画像は `opt44c/paired-final.png` / `.svg`。
検証・交互測定・今回だけの差分は `build/quality/clickbench-opt44-validation/` に保存した。
初期候補の `opt44/` と `opt44-forward/` はQ9に追加分岐の悪影響があったため不採用で、
`SUPERSEDED.md` に明記した。採用版は `opt44c` のみ。

## コア数を増やした再測定（opt44c、2026-11-11）

同じ100万行・固定バイナリで1/2/4/8/16物理コアを測定したところ、
**合計時間は4コアからDuckDB優位に逆転した**。各条件warmup 1＋測定7回、
順方向・逆方向の両方を実施し、全10実行で43/43件の回答が一致した。

| 物理コア／最大スレッド数 | tinylamb合計 ms | DuckDB合計 ms |
|---|---:|---:|
| 1 | 365.2–366.5 | 682.6–687.0 |
| 2 | 301.3–305.6 | 409.7–412.2 |
| 4 | 252.2–255.7 | 238.4–238.6 |
| 8 | 223.4–224.8 | 174.5–175.6 |
| 16 | 263.7–269.1 | 197.1–197.4 |

幅は2実行の範囲。Ryzen 9 9950X3DでCPU 0..N-1を指定しSMTは使わない。
16コアではL3共有ドメインを跨ぎ、両エンジンとも8コアより遅くなった。
Q29はtinylambが1〜8コアで約127〜132 msのままなのに対して、
DuckDBは約256から約51 msへ短縮しており、合計逆転の主要因になっている。

このopt44c測定時点ではデータ増量は未測定。疎なDISTINCTには全入力行数×16バイト／64 MiBの上限があり、
直積ビットマップも収まらない場合、4,194,304行超では今回の高速経路から外れる。
辞書の種類数・容量にも上限があるため、100万行・2コアの勝利を大規模性能へ一般化しない。
測定・実装上の制限・次の課題は
`build/benchmarks/clickbench-scaling-20261111/REPORT.md`、図は同ディレクトリの
`scaling.png` / `.svg`、全コマンドは `run_scaling.py` に保存した。

## 動的並列化と必要列キャッシュ（opt45、2026-11-11）

タスク数・CPU affinity・同時クエリ予算でワーカー数を決め、文字列キー計算と部分集約を動的に分配する。
疎なDISTINCTはメモリー予算内のバッファと外部ランに分割する。
全列キャッシュの容量上限に達する表では必要列だけを構築し、未使用列のメタデータも省く。
全列と部分列のキャッシュは分離し、世代・MVCCの条件を維持する。

| 行数 | 物理コア／最大スレッド数 | Tinylamb合計 ms | DuckDB合計 ms | Tinylamb勝利数 |
|---|---:|---:|---:|---:|
| 1M | 1 | 360.74–360.81 | 685.10–686.70 | 31–31/43 |
| 1M | 2 | 244.40–245.55 | 407.22–407.59 | 29–30/43 |
| 1M | 4 | 167.22–167.55 | 234.63–236.78 | 31–32/43 |
| 1M | 8 | 128.59–129.85 | 173.46–175.01 | 29–31/43 |
| 1M | 16 | 164.64–171.99 | 191.49–197.32 | 29–29/43 |
| 5M | 2 | 23,433.80–25,183.99 | 3,069.10–3,071.59 | 4–4/43 |
| 5M | 8 | 15,046.51–15,069.05 | 921.87–923.53 | 4–4/43 |

100万行・8コアはopt44c比42.3%短縮、DuckDB比25.8%短縮。
各条件warmup 1＋7回、正順・逆順の範囲。全14実行で43/43件一致。
500万行は公式の別パーティションを追加しており、行の複製ではない。
全43クエリ勝利は未達。初回のキャッシュ構築を含まない反復実行時間で、標準約1億行も未測定。

全3,948テスト中3,945成功・3スキップ、ASan/UBSan重点52件成功。
SQL・式ファジングを各約5分、異常なし。増分ビルドwarning 0、変更14翻訳単位のClang-Tidy最終指摘0。
全体のメモリー使用量や高カーディナリティの経路には制限が残る。
測定・図・制約・再現手順は`build/benchmarks/clickbench-opt45-scaling-20261111/REPORT.md`、
`comparison.png` / `.svg`、`STRATEGY.md`に記録した。

## 入力と比較条件

- [公式ClickBench](https://github.com/ClickHouse/ClickBench)の
  `0b1ff23f94d68b9790e910a4340e4ec020930ca8`を固定する。
- 標準規模は約1億行。`hits_0.parquet`だけの場合は100万行の**部分データ**であり、
  EventDateが2013-07-15に偏る。標準ClickBenchの結果として扱わない。
- 元の43 SQL、両エンジン向けSQL、入力ParquetのSHA-256を保存する。
  整数・文字列・日付を保ち、文字列はhexでロードして制御文字も保持する。
- AVGの入力を両方でFLOAT64にする。ClickHouseの整数AVGはUserIDの合計が
  INT64を超えてオーバーフローするため。元SQLは`original/`に残す。
- ClickHouseの`length(String)`はバイト長なのでGoogleSQLでは`BYTE_LENGTH`を使う。
  正規表現のdotはClickHouse同様に改行も含むよう、両SQLで明示的な文字クラスにする。
- 同じCPU集合・最大スレッド数。query result cacheは無効。
  warmup後の各クエリの中央値を合計する。全43件の結果と失敗を残し、
  都合のよいクエリだけを合計しない。
- tinylambは`SqlEngine::Execute`から結果の`Collect`まで、ClickHouseはサーバーの
  `statistics.elapsed`を測る。どちらも計画を含み、データロード・HTTP/worker起動は除く。
  実行順序を交互にし、`--reverse`で逆順の独立した再測定ができる。
- 出力列の型に従ってFLOAT64の丸めとUTCの時刻表記を正規化する。
  整数は厳密比較。LIMITの同率境界では順位キーと、LIMIT/OFFSETを外した完全な
  クエリ結果への所属・重複数を検証する。隠れたORDER BY列も別の検証クエリで確認する。
  この検証は測定時間に含めない。誤答のタイミングは勝利に数えない。

## 再現手順

```bash
python3 -m venv /var/tmp/tinylamb-comparison-venv
/var/tmp/tinylamb-comparison-venv/bin/pip install -r benchmark/comparison/requirements-tpcds.txt
git clone https://github.com/ClickHouse/ClickBench.git build/clickbench-upstream
git -C build/clickbench-upstream checkout 0b1ff23f94d68b9790e910a4340e4ec020930ca8
mkdir -p build/benchmarks/clickbench-input
curl -fL https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet \
  -o build/benchmarks/clickbench-input/hits_0.parquet
```

標準規模には`hits_0.parquet`〜`hits_99.parquet`をすべて指定する。
以下は100万行の開発用比較。標準規模はメモリーとディスク容量を別途確保する必要がある。

```bash
/var/tmp/tinylamb-comparison-venv/bin/python benchmark/comparison/clickbench.py prepare \
  --upstream build/clickbench-upstream \
  --parquet build/benchmarks/clickbench-input/hits_0.parquet \
  --output build/benchmarks/clickbench-data
cmake -S . -B build-clickbench -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_EXE_LINKER_FLAGS=-fuse-ld=lld \
  -DTINYLAMB_ARCH=native -DTINYLAMB_ENABLE_LTO=ON
cmake --build build-clickbench -j8 --target tinylamb_tpcds_compare
build-clickbench/tinylamb_tpcds_compare build/benchmarks/clickbench-native \
  --load build/benchmarks/clickbench-data
```

正規表現置換は、インストール済みRE2を検出すれば利用する。
`TINYLAMB_ENABLE_RE2=OFF`、または未インストール時は標準実装を使う。
独自prefixの場合は`TINYLAMB_RE2_INCLUDE_DIR`と`TINYLAMB_RE2_LIBRARY`を指定できる。
Latin1で既存のバイト単位の意味を保ち、空文字マッチやエスケープクラスなど
標準実装と動作が異なる構文は標準実装へ戻す。両バックエンドを回帰テストする。

[ClickHouse公式リリース](https://github.com/ClickHouse/ClickHouse/releases/tag/v26.10.2.103-stable)
の`clickhouse-common-static-26.10.2.103-amd64.tgz`を展開し、そのbinaryを指定する。
セットアップは新しい作業ディレクトリとlocalhost専用サーバーを作り、公式DDLでロードする。
既存のディレクトリ・データベースを上書きしない。

```bash
/var/tmp/tinylamb-comparison-venv/bin/python benchmark/comparison/clickbench.py init-clickhouse \
  --binary /absolute/path/to/clickhouse \
  --root build/benchmarks/clickhouse-local \
  --dataset build/benchmarks/clickbench-data --database clickbench
/var/tmp/tinylamb-comparison-venv/bin/python benchmark/comparison/clickbench.py run \
  --dataset build/benchmarks/clickbench-data \
  --native-db build/benchmarks/clickbench-native \
  --binary build-clickbench/tinylamb_tpcds_compare \
  --database clickbench --server-pid "$(cat build/benchmarks/clickhouse-local/server.pid)" \
  --cpus 0,1 --threads 2 --warmups 1 --repeats 7 \
  --output build/benchmarks/clickbench-result
```

終了後はセットアップが表示した自分のサーバーPIDに`kill`を送る。
`run.json`に環境・SQL・binary・作業ツリーの識別情報、`results.jsonl`に全サンプルと回答、
`summary.json`に合計と達成判定、`comparison.png`/`.svg`にプロットを保存する。
比較中はビルド・テスト・静的解析・ファジングを同時に走らせない。

## 実装した汎用改善

- COUNT(*)の空のProjectionを省き、可視性を確認した列スキャンで件数を数える。
- SUM(x + 定数)を共通の列走査へまとめる。NULL・符号・中間overflow・MVCCを保つ。
- 文字列の比較とLIKEを列ブロック上で実行する。
- TopNでは列の参照と行位置だけをヒープに入れ、勝った行を最後に展開する。
- COUNT(DISTINCT)は整数集合／文字列ビューで先に重複を除き、文字列の正規化を一意値ごとに行う。
- DATEのMIN/MAXでブロックの極値を使い、DATE型とNULLを保つ。
- AVGの数値列へのFLOAT64 castを既存の数値列カーネルへ結び付ける。
- 集約のハッシュ表は所有済みのグループキーをIDで参照し、重複したRowの割当を省く。
  TopNで落ちるグループの出力文字列は生成しない。定数GROUP BYも型付き集約へ流す。
- 並列集約はクエリのスレッド予算を守る。
- 通常の文字列をINTERVALと誤認する問題とREGEXP_REPLACEの後方参照を修正する。

## 過去のClickHouse比較（2026-11-10）

**以下はUTCローダー修正前の過去の値。** 日時表現にローカルタイムゾーンへの依存があり、現在のDuckDB比較とは混ぜない。ClickHouseより短い合計時間という当時の目標も未達。 以下は100万行の部分データで、
標準の約1億行は未測定。Ryzen 9 9950X3D、CPU 0・1、両エンジン2スレッド、
warmup 1回・測定7回。tinylambはRelease / native / ThinLTO / RE2有効、
ClickHouseは26.10.2.103。時間は全43クエリそれぞれの中央値を合計したもの。

| 測定 | tinylamb | ClickHouse | 結果一致 | tinylambが速いクエリ |
| --- | ---: | ---: | ---: | ---: |
| 交互順 | 5,921.686 ms | 379.451 ms | 43/43 | 6/43 |
| 逆順 | 5,977.048 ms | 372.998 ms | 43/43 | 7/43 |

生データとプロットは`build/benchmarks/clickbench-final/`と
`build/benchmarks/clickbench-final-reverse/`に保存した。
標準規模・元SQLそのままのClickBenchスコアではない。
初期の診断測定は比較相手のCPU固定やSQL正規化が最終条件と異なるため、
上表との厳密な改善率の計算には使わない。

残る主なコストはQ19（日時抽出を含む複合キー）、Q29（正規表現を含む集約）、
Q40（CASEを含む集約）、Q43（日時切り詰め）、Q28（文字列長の平均）、
Q36（算術式を含む複合キー）。この6件でtinylambの合計時間の約77%を占める。
これらの計算式を列ブロック上の集約へ取り込む実装が残っている。
100万行でのnative workerのピークRSSは5.25 GiBだった。
標準規模への拡張にはメモリー消費も別途検証する必要がある。

品質確認は`build/quality/clickbench-verification.json`に記録した。
全748工程のクリーンビルドと最終修正の再ビルドでwarning 0件、
Clang-Tidyは389翻訳単位・394構成を解析し、修正箇所の再解析後に指摘0件。
RE2有効構成も別途解析した。C++テスト3,592件成功・既存3件スキップ、
Pythonテスト76件成功、layering違反の追加0件。
SQL・式・GriffinのUBSan付きファジングは各601秒、計84,115ケースを
エラーなしで完了した。
