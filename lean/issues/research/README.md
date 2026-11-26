# 次の SQL 最適化の未証明課題

[現行 RDBMS の正しさを問う107件](database-correctness.md)。再監査は証明104件・反証3件。MERGE の順序で成功とエラーが変わる実測、ソート再試行の前提、108組の SQL 照合を収録。

[JIT 高速化の境界を問う281件](jit-kernels.md)。30モデルの索引と、31件について登録済みの式を直接評価した有限探索。

[性能・ストレージ・長時間運転に関する224件の研究命題](resource-policies.md)。メモリ不足、部分書き込み、回収、並列実行の境界を22モデルで扱う。

[最新 TLCS の負け89クエリに対応する未証明命題](fresh-tlcs-query-losses.md)。演算別の提出型、クエリごとの実測値、Q28・Q35 の入力調査。

証明を書く前に [既存の補題を探す](../../README.md#既存の補題を探す) を参照してください。

[11月26日の敗北83クエリの未証明命題](november26-queries.md)。最新のクエリ別中央値と次回の計測項目。

[速度・保存サイズの各敗北に対応する94件の未証明命題](benchmark-rematch.md)。各クエリと対戦相手ごとの逆転目標、候補演算、観測量。

[リカバリ・並行制御・DDL 競合と JOB J3 の20題](restart-races.md)。WAL の選択再生、オンライン UNIQUE、キーの引渡し、JOB での結合順とキャッシュに関する仮説と追試。

[ClickHouse との保存サイズ差から調べた圧縮・実行の12題](clickbench-layout.md)。ClickBench・TPC-H・TPC-DS の全量比較と、同じ98,304行での時刻・文字列の実験。

[Optimizer・MVCC・Executor の実装判断を問う23題](engine-decisions.md)。適用条件、境界反例、実装に接続する箇所を掲載。

[データ表現・SQL 実行・復旧の境界に関する 1,000 題](layer-contracts.md)。ページ配置、名前解決、参照制約、再帰経路など 100 モデルの索引と反例。

[ストレージ・トランザクション・SQL 実行にまたがる 100 題](engine-boundaries.md)。WAL・待機グラフ・spill・統計・ウィンドウ関数など 10 分野の未証明命題と反例。

[JIT コンパイルの境界条件を問う 1,000 件](jit-frontiers.md)。エラー順序、SIMD の別名参照、ガード失敗後の再開など、100 モデルの索引と反例を収録。

[圧縮と SQL 実行の未証明命題 1,000 件](storage-execution.md)。100 モデルの索引と、ClickBench の選択ビットマップの実測を収録。

[可逆圧縮の研究課題 228 件と DuckDB との保存サイズ比較](compression.md)。ClickBench・TPC-H・TPC-DS の全列ファイルで試作形式の復元を検証。

[SQL エンジンの高速化仮説 500 件](atlas.md)。提出型、具体的な反例、H5・C1 の基数上界を収録。

[大胆な高速化仮説 110 件と反例 12 件](frontiers.md)。H2O H5 で行列のランクと集約ごとの圧縮粒度も調べました。

[DuckDB より遅かったクエリから登録した命題 50 件](duckdb-gaps.md)に、比較値、SQL の追試、適用条件をまとめています。7 モジュールすべてで `theorem sound_* : Goal` を証明済み（[RESULTS.md](RESULTS.md)）。

[クラウド・BI の命題 42 件](cloud-bi.md)（第九波）は、CostBench、Snowset/PBench、Iceberg、Public BI の比較結果から、書き換え条件と実行方式の検証課題を定めています。42 件すべて `theorem sound_* : *.Goal` を証明済み（[RESULTS.md](RESULTS.md)）。

各モデルの主定理と元の SQL・実装との対応は、[接続命題と監査後の状況](sql-refinement.md)を参照。

各 Issue は独立した研究課題。課題の作成側で参照計算・入力条件・観測結果を定め、証明担当者へ渡す前に提出型を固定する。計算量の結論にはコストを数える評価器を用いる。

証明・反例の登録結果は [RESULTS.md](RESULTS.md) を参照（初回 26 件＋型検査済み提出型 1 件、差し戻し後・新規の 7 件（第二波）、追加第三波 6 件、第四波 36 件（jit-skip 16 + engine-opt 20）、第五波 27 件、第六波 18 件、第二波からの複合提出型 1 件、第九波（クラウド・BI）42 件、DuckDB ギャップ 50 件をすべて証明または反例登録し、`lake build`（CheckProofs の `sorryAx` 検査込み）成功）。

## DuckDB ギャップの命題（50 件・証明済み）

[50 件の Issue と提出型](duckdb-gaps.md)。Cloud/BI 6 件、TPC-DS 8 件、ClickBench 8 件、
TPC-H 7 件、Slices/FileMasks/Refresh/BridgeStates の派生 21 件。全 50 件
`theorem sound_* : *.Goal` 証明済み（反例なし）。対応する証明は
`ResearchProofs/AnalyticalOperators/` の 7 モジュール。

## クラウド・BI ワークロードの命題（第九波・証明済み）

[42 件の Issue と提出型](cloud-bi.md)。CostBench（Quotes 10 件）、Snowset/PBench 派生（NestedBI 10 件）、Iceberg Merge-on-Read（DeleteFiles 11 件）、Public BI MLB_11（PublicBI 11 件）。全 42 件 `theorem sound_* : *.Goal` 証明済み（反例なし）。共通補題ライブラリ `ResearchProofs/CloudBI/Common.lean` を併設。

## 派生分析ベンチマークの実行計画と物理演算（第八波・未証明）

[全 26 クエリに対する 33 件の提出型](analytics-workloads/README.md)。同一 seed のデータで結合件数・キー頻度を調べ、適用条件と演算ごとの削減対象を記した。

## SQL 実行の高速化候補（第七波・未証明）

[12 分野・100 件の提出型と適用条件](execution-strategies/README.md)。各命題は独立した `def Goal : Prop` として登録し、既存証明との対応を一覧の冒頭に記した。

## JIT とブロック読み飛ばしの課題（第四波・証明済み）

[16件の Issue と提出型](jit-skip/README.md)。ビット演算による分岐削減が9件、ページや payload の読取りを省く課題が7件。全 16 件 `theorem sound : Goal` 証明済み。

## 起動ロード・融合実行・Bytecode・キャッシュの課題（第四波・証明済み）

[20件の Issue と提出型](engine-opt/README.md)。永続 TLCS v3 遅延キャッシュ・起動ロード（5件）、TPC-DS/TPC-H 融合結合・圧縮走査（6件）、ClickBench 列指向実行・文字列/集約（5件）、Bytecode 側表・Limit/Cache 合成（4件）。全 20 件 `theorem sound : Goal` 証明済み。

## 3 ベンチマーク敗北クエリ全 66 件からの課題（第五波・証明済み）

[27件の Issue と提出型](benchmark-losses/README.md)（全 66/66 敗北クエリ対応表を含む）。TPC-H SF=10（7 敗）、TPC-DS SF=10（41 敗）、ClickBench 100M（18 敗）の全敗北クエリを網羅する結合逆伝播・左外部結合簡約・ピボット・分散統計・文字列表現・Offset/Limit 最適化の命題群。26 件を証明、1 件（functional-dependent-group-key-elim）は反例により `¬ Goal` を登録。

## 派生分析ベンチマーク 5 種敗北クエリ全 15 件からの課題（第六波・証明済み）

[18件の Issue と提出型](analytics-losses/README.md)（全 15/15 敗北クエリ対応表を含む）。`benchmark/comparison/analytics_bench.py`（JOB / STATS-CEB / H2O / LDBC-SNB / BigBench, SF=1, Release + `ANALYZE` + TLCS）での全敗北クエリ（`J4, J5, C1, C2, C4, C5, H2, H4, H5, L1, L2, L5, B1, B2, B3`）を網羅する両側／因子化事前集約、バイナリ照合文字列キー残差除去・ファンアウト分割・短文字列パック、ブッシー／自己結合共有、2 キー直接配列・局所辞書 2 段集約・符号反転基数ソート・PostgreSQL ワイヤ直列化の命題群。全 18 件 `theorem sound : Goal` 証明済み。

## SQL 実行を改善する追加の課題（第三波・証明済み）

第三波の 6 件はすべて証明済み（`research/RESULTS.md` を参照）。

- [重なり区間数を端点の走査で求める](interval-count-sweep.md)
- [再帰 UNION DISTINCT で新しく発見した頂点だけを展開する](recursive-delta.md)
- [EXCEPT ALL の差を spill 部分木間で相殺する](except-all-debt.md) 
- [NTH_VALUE IGNORE NULLS と行除外を順位索引で評価する](nth-non-null-frame.md)
- [最終出現位置の永続索引で任意区間の COUNT DISTINCT を返す](range-distinct-index.md)
- [相関 ANY・ALL をグループごとの最小・最大と NULL 情報に変換する](quantified-range-summary.md)

## 差し戻し後と新規の Issue（第二波・証明済み）

次の 7 件は差し戻し時に未解決だった課題で、第二波としてすべて `theorem sound : Goal` を証明した
（提出型は未変更。登録は [RESULTS.md](RESULTS.md) の第二波の項）。各 Issue に提出型、境界例、適用範囲を記した。LLVM の課題には実生成 IR との接続を含む。

- [分散 FETCH WITH TIES で開くブロックを確定する](distributed-page-fence.md)
- [spill ファイルをまたぐ run のバッファ上限とページ出力](streaming-run-buffer.md)
- [radix 再分割・再送の実行履歴から一回消費を導く](radix-retry-exactly-once.md)
- [マスク付きロードの全アドレスとページ間一致を照合する](masked-page-load-trace.md)
- [入力途中で履歴を落としても勝者のキャプチャを保存する](ordered-tag-frontier.md)
- [実生成 LLVM フィルタの本体・ベクトル余り・末尾を検証する](emitted-filter-loop.md)
- [二次元の相関 NOT EXISTS を一度のソートと走査に変換する](skyline-sweep.md)

## 証明担当者への引き渡し

[並列 SUM](checked-aggregate-tree.md) には型検査済みの提出型 `CheckedAggregateTree.Goal` がある。初回の他の19件は、この一覧では仕様案として扱う。下の追加6件にもそれぞれ型検査済みの提出型がある。既に書かれた証明を採用するときも、元の課題の提出型にその証明項が入るか確認する。

1. Issue で比較する観測を決める。例えば順序付き出力と bag、NULL と欠落行、エラーの有無と最初のエラー位置は、それぞれ異なる命題になる。
2. 課題側の Lean ファイルに参照意味論と `def Goal : Prop := ...` を置き、反例の期待値を具体的に検査する。適用条件を発見する課題は、拒否すべき例と適用に成功すべき入力族の両方を含める。性能上の性質は実行トレースやコストを返す評価器に結びつける。
3. 証明は別ファイルから仕様を import し、`theorem sound : Goal` として提出する。提出差分に仕様変更があれば、その変更は元の Issue に照らして先にレビューする。型検査と公理依存の検査は別々に行う。

## 提出型を固定した追加課題

- [直積を展開せずに加算キーの第 k 位を求める](pair-rank.md)
- [FULL OUTER JOIN と GROUP BY を二つの頻度表に変換する](outer-join-counts.md)
- [CASE 内の相関スカラサブクエリを遅延エラー付き索引に変換する](demand-scalar.md)
- [符号付き nullable INT64 の安定降順ソートをビット分配に落とす](stable-radix.md)
- [定数の符号付き INT64 商と余りを補正付き逆数乗算で計算する](reciprocal-divmod.md)
- [リセットと NULL を含む XOR 差分列を並列 prefix scan で復号する](segmented-xor.md)

## 結合探索と適応的な実行

- [再最適化時に処理済みの結合結果を再利用する](adaptive-memo.md)
- [NULL を含む循環結合を trie の交差で列挙する](null-aware-hyperjoin.md)
- [spill 再分割中の Bloom を probe に公開する](spill-bloom-epoch.md)
- [外部結合の未マッチ確定を並列化する](outer-probe-cancel.md)
- [辞書世代が異なる圧縮列を復号せず等値結合する](compressed-join-dict.md)
- [時系列 ASOF JOIN を安全な水位で打ち切る](asof-snapshot-frontier.md)

## 集約・順序・スナップショット

- [分散 Top-N に同率を失わない停止水位を付ける](tie-watermark.md)
- [部分整列キーを使う spill 対応 FETCH](incremental-sort-page.md)
- [部分和の順序情報を持つ並列 SUM](checked-aggregate-tree.md)
- [移動窓の DISTINCT 集約を差分更新する](window-distinct-inverse.md)
- [radix 分割数の途中変更を許す集約](adaptive-radix-replay.md)
- [MVCC 索引の積集合から遅延して列を読む](snapshot-index-intersect.md)

## JIT とパターン検索

- [複数フィルタの共通部分式を遅延評価する](runtime-predicate-dag.md)
- [優先順位付きキャプチャを保持する DFA 化](regex-tagged-determinize.md)
- [複数 LIKE を一つの多パターン探索で評価する](like-multi-prefilter.md)
- [ページ境界を越える検索を二つのマスクで処理する](vector-tail-page.md)

## 今回の証明を実装へ接続する課題

- [生成済み LLVM IR に対する翻訳検証](ir-translation-validation.md)
- [パイプライン融合で LIMIT の需要を下流から伝える](pull-demand-fusion.md)
- [検証済みパターンコードを異なる行・実行間で共有する](regex-cache-version.md)
- [pin 中の版を保つ compaction で安全に列を遅延読取りする](mvcc-compaction-pin.md)
