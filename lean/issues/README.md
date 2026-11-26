# SQL 最適化の証明課題

[現行 RDBMS の正しさを問う107件](research/database-correctness.md)。再監査は証明104件・反証3件。MERGE の順序で成功とエラーが変わる実測、ソート再試行の前提、108組の SQL 照合を収録。

[JIT 高速化の境界を問う281件](research/jit-kernels.md)。30モデルの索引と、31件について登録済みの式を直接評価した有限探索。

[性能・ストレージ・長時間運転に関する224件の研究命題](research/resource-policies.md)。メモリ不足、部分書き込み、回収、並列実行の境界を22モデルで扱う。

[最新 TLCS の負け89クエリに対応する未証明命題](research/fresh-tlcs-query-losses.md)。演算別の提出型、クエリごとの実測値、Q28・Q35 の入力調査。

証明を書く前に [既存の補題を探す](../README.md#既存の補題を探す) を参照してください。

[11月26日の敗北83クエリの未証明命題](research/november26-queries.md)。最新のクエリ別中央値と次回の計測項目。

[速度・保存サイズの各敗北に対応する94件の未証明命題](research/benchmark-rematch.md)。各クエリと対戦相手ごとの逆転目標、候補演算、観測量。

[リカバリ・並行制御・DDL 競合と JOB J3 の20題](research/restart-races.md)。WAL の選択再生、オンライン UNIQUE、キーの引渡し、JOB での結合順とキャッシュに関する仮説と追試。

[ClickHouse との保存サイズ差から調べた圧縮・実行の12題](research/clickbench-layout.md)。ClickBench・TPC-H・TPC-DS の全量比較と、同じ98,304行での時刻・文字列の実験。

[Optimizer・MVCC・Executor の実装判断を問う23題](research/engine-decisions.md)。適用条件、境界反例、実装に接続する箇所を掲載。

[データ表現・SQL 実行・復旧の境界に関する 1,000 題](research/layer-contracts.md)。ページ配置、名前解決、参照制約、再帰経路など 100 モデルの索引と反例。

[ストレージ・トランザクション・SQL 実行にまたがる 100 題](research/engine-boundaries.md)。WAL・待機グラフ・spill・統計・ウィンドウ関数など 10 分野の未証明命題と反例。

[JIT コンパイルの境界条件を問う 1,000 件](research/jit-frontiers.md)。エラー順序、SIMD の別名参照、ガード失敗後の再開など、100 モデルの索引と反例を収録。

[圧縮と SQL 実行の未証明命題 1,000 件](research/storage-execution.md)。100 モデルの索引と、ClickBench の選択ビットマップの実測を収録。

[可逆圧縮の研究課題 228 件と DuckDB との保存サイズ比較](research/compression.md)。ClickBench・TPC-H・TPC-DS の全列ファイルで試作形式の復元を検証。

[SQL エンジンの高速化仮説 500 件](research/atlas.md)。提出型、具体的な反例、H5・C1 の基数上界を収録。

[大胆な高速化仮説 110 件と反例 12 件](research/frontiers.md)。H2O H5 で行列のランクと集約ごとの圧縮粒度も調べました。

[DuckDB より遅かったクエリから登録した命題 50 件](research/duckdb-gaps.md)に、比較値、SQL の追試、適用条件をまとめています。7 モジュールの全命題を証明済み（[RESULTS.md](research/RESULTS.md)）。

[クラウド・BI ワークロードの未証明命題 42 件](research/cloud-bi.md)を、4 系統の実測と対応づけました。

[第八波の未証明命題 33 件](research/analytics-workloads/README.md) を、JOB・STATS-CEB・H2O・LDBC-SNB・BigBench の全 26 クエリに対応づけました。

[第七波の未証明命題 100 件](research/execution-strategies/README.md) には、提出型と各最適化の狙いを分野別にまとめています。

## 初回 30 件の検証結果

このディレクトリに置かれていた 30 件の未証明問題について、個別の証明または反例を
Lean 4 で登録しました。対応する結果とモデル前提は [RESULTS.md](RESULTS.md) を参照してください。

以前未完了の `sorry` を含んでいた `ResearchProofs/FirstKeyBlock.lean` と
`ResearchProofs/PackedRadix2x32BatchAgg.lean` も証明が完了し、現在 `lake build`
（CheckProofs によるパッケージ全体の `sorryAx` 検査）は成功する。

各定理の達成範囲と適用条件は [仕様監査](AUDIT.md)、追加研究の課題一覧は [研究 Issue](research/README.md)、新規提案 8 件は [提案 Issue](proposals/README.md) にまとめています。

## SQL 書き換えと集約

- [検査付き SUM を結合前に移せる値域](../TinylambOpt/CheckedEagerSum.lean) — `checked_eager_sum_sound`
- [相関 NOT IN を NULL 対応のマーク結合にする](../TinylambOpt/NullableMarkJoin.lean) — `nullable_mark_join_sound`
- [LEFT JOIN の OR 条件を排他的な等値結合に分解する](../TinylambOpt/OuterOrBranches.lean) — `outer_or_branches_sound`
- [有界 ROWS フレームのための前後行だけを残す](../TinylambOpt/WindowHalo.lean) — `window_halo_sound`
- [WITH TIES の候補を結合前に絞る](../TinylambOpt/OuterFetchTies.lean) — `outer_fetch_ties_sound`
- [エラーを保って安いフィルタを前に移す](../TinylambOpt/FilterEvalOrder.lean) — `filter_eval_order_sound`

## 行選択と遅延デコード

- [選択ビットから連続区間だけをデコードする](../TinylambOpt/MaskToRanges.lean) — `mask_to_ranges_sound`
- [疎な行番号と密なビットマスクをバッチ途中で切り替える](../TinylambOpt/AdaptiveSelection.lean) — `adaptive_selection_sound`
- [参照された辞書値だけで述語を一度ずつ評価する](../TinylambOpt/LazyDictionary.lean) — `lazy_dictionary_sound`
- [遅延射影の再読時に同じ行バージョンを取得する](../TinylambOpt/LateGatherVersions.lean) — `late_gather_versions_sound`
- [LIMIT を満たした後の SIMD レーンのエラーを抑える](../TinylambOpt/LimitErrorFrontier.lean) — `limit_error_frontier_sound`

## ブロック単位の読み飛ばし

- [更新差分を含むスナップショットでのブロック除外](../TinylambOpt/SnapshotZoneMap.lean) — `snapshot_zone_map_sound`
- [三値とエラーを含む複合述語のブロック判定](../TinylambOpt/BlockKleene.lean) — `block_kleene_sound`
- [必須 trigram がないブロックを LIKE 検索から外す](../TinylambOpt/TrigramSkip.lean) — `trigram_skip_sound`
- [複合順序キーの上界で Top-N の残りブロックを飛ばす](../TinylambOpt/TopNBlockFenceExplicitTerms.lean) — `topn_block_fence_sound`
- [二つの Bloom ビット集合からマッチ不能なブロックを判定する](../TinylambOpt/BloomBlockIntersection.lean) — `bloom_block_intersection_sound`
- [RLE のランと可視行マスクを交差して走査を省く](../TinylambOpt/RleRunSkip.lean) — `rle_run_skip_sound`

## 分岐を減らす JIT・SIMD

- [popcount と prefix rank による安定 SIMD 圧縮](../TinylambOpt/RankScatter.lean) — `rank_scatter_sound`
- [複数のエラーマスクから逐次評価と同じ最初の失敗を返す](../TinylambOpt/FirstErrorBallot.lean) — `first_error_ballot_sound`
- [符号付き区間の二比較を unsigned の一比較にする](../TinylambOpt/SimdInRange.lean) — `simd_in_range_sound`
- [同じグループを指す SIMD レーンの加算をまとめる](../TinylambOpt/SimdGroupConflicts.lean) — `simd_group_conflicts_sound`
- [JIT の途中フォールバックで集約を二重計上しない](../TinylambOpt/DeoptResume.lean) — `deopt_resume_sound`
- [マスク付き末尾ロードの値とアクセス範囲を同時に証明する](../TinylambOpt/MaskedLoadTail.lean) — `masked_load_tail_sound`

## LIKE・正規表現のネイティブ化

- [任意個の % 区切りリテラルを順次検索にコンパイルする](../TinylambOpt/LikeSegmentChain.lean) — `like_segment_chain_sound`
- [UTF-8 の _ を含む LIKE の ASCII 高速経路](../TinylambOpt/LikeUnderscore.lean) — `like_underscore_sound`
- [ワードをまたぐ Shift-And による %literal% 検索](../TinylambOpt/LikeShiftAnd.lean) — `like_shift_and_sound`
- [正規表現から必須リテラルを抽出して行を先に落とす](../TinylambOpt/RegexMandatoryLiteral.lean) — `regex_mandatory_literal_sound`
- [小さな正規表現 NFA を一語のビット状態で実行する](../TinylambOpt/RegexBitsetNfa.lean) — `regex_bitset_nfa_sound`
- [キャプチャの開始・終了位置と選択優先順位を保存する](../TinylambOpt/RegexCapturePriority.lean) — `regex_capture_priority_sound`
- [長い一行の正規表現検索をブロック要約で合成する](../TinylambOpt/DfaBlockSummary.lean) — `dfa_block_summary_sound`

各モジュールには参照評価器・最適化後の計算・十分条件の仮定・C++ 側の確認箇所が
`/-- ... -/` コメントとして記録されています。性能の評価は正当性とは別に行ってください。
