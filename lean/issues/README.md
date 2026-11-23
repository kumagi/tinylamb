# SQL 最適化の証明課題

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
