# Lean モジュールの検証記録

元の課題に対する達成範囲と修正後の定理は [30 件の仕様監査](AUDIT.md) を参照。

`issues/README.md` の 30 件について、証明または反例（素朴な lowering が壊れる具体例）を Lean で登録した。
個々の issue 記述は対応する Lean モジュールの doc コメントへ統合され、個別 `.md` ファイルは削除済み（[README.md](README.md) が一覧）。

これはパッケージ全体の `sorryAx` 非依存を意味しない。現在 `TinylambOpt.lean` が import する
`ResearchProofs/FirstKeyBlock.lean` と `ResearchProofs/PackedRadix2x32BatchAgg.lean` には未完了の
`sorry` があるため、全体の証明検証状態は別途確認が必要である。

```bash
cd lean && ~/.elan/bin/lake build
```

## 仕様監査で追加した回帰テストと定理

| 課題 | 検証した観測結果 |
| --- | --- |
| Thompson NFA | `alternation_keeps_both_singletons` は両枝の受理を検査する。`alternation_does_not_accept_left_prefix` は `(ab または c)` が単独の a、b を拒否し、abx の途中で受理することを検査する。`compiled_search_iff` は全 AST と全入力で元の言語の部分一致まで証明する。 |
| 貪欲な探索 | `greedy_backtracks_before_final_literal` は候補が [2, 1, 0] で、ab 上の .*b が位置 2 で終わることを検査する。候補列の被覆と降順も全長で証明した。 |
| 検査付き SUM | `sql_checked_sum_reaggregation` は実在キーから作る部分・最終グループの bag を比較する。`sql_sum_null_unmatched_regression` と `sql_sum_unmatched_overflow_regression` は全 NULL のグループ、消えるグループ、範囲条件を外したときの早すぎる失敗を区別する。 |
| LEFT JOIN と WITH TIES | `sql_outer_fetch_ties_sound` は三値 ON の leftOuterJoin の出力列を比較する。`mixed_boundary_regression` と `unmatched_null_ties_regression` は未マッチの補完、右の重複、NULL の境界を固定する。 |

## 初回に登録した 8 件

| issue | Lean ファイル | 主定理 | 反例 / 境界定理 |
| --- | --- | --- | --- |
| `deopt-resume` | `TinylambOpt/DeoptResume.lean` | `deopt_resume_sound` | `restart_from_batch_head_unsound` |
| `first-error-ballot` | `TinylambOpt/FirstErrorBallot.lean` | `first_error_ballot_sound` | `op_first_is_wrong` |
| `like-shift-and` | `TinylambOpt/LikeShiftAnd.lean` | `like_shift_and_sound` | `no_reset_spurious_match`, `boundary_widths` |
| `limit-error-frontier` | `TinylambOpt/LimitErrorFrontier.lean` | `limit_error_frontier_sound`, `limit_error_frontier_batches_sound` | `limit_zero_evaluates_nothing`, `pass_then_error_at_k1`, `error_then_pass_at_k1` |
| `masked-load-tail` | `TinylambOpt/MaskedLoadTail.lean` | `masked_load_tail_sound` | `full_width_load_reads_unassigned`, `full_width_load_violates_tail_access` |
| `regex-bitset-nfa` | `TinylambOpt/RegexBitsetNfa.lean` | `regex_bitset_nfa_sound`, `regex_bitset_nfa_accepts` | `start_injection_once_is_wrong` |
| `simd-group-conflicts` | `TinylambOpt/SimdGroupConflicts.lean` | `simd_group_conflicts_sound` | `naive_gather_add_scatter_loses_increments`, `all_null_group_sum_is_not_zero` |
| `topn-block-fence` | `TinylambOpt/TopNBlockFence.lean`（別証明: `TinylambOpt/TopNBlockFenceExplicitTerms.lean`） | `topn_block_fence_sound`（別証明は `TopNBlockFenceExplicitTerms` 名前空間） | `mixed_order_boundary_example`, `with_ties_equal_boundary_kept_example`, `nulls_placement_example`（別証明側は `mixed_direction_row_improves`, `nulls_first_and_last_are_opposite`, `conservative_bound_need_not_be_a_row`, `ties_keep_equal_block`） |

### 各証明のモデルと仮定

- **deopt-resume**: 状態に `output` / 検査付き `SUM` / 件数 / NULL フラグを明示。ガード失敗時に途中書き込みを `save`/`restore` で巻き戻し、最後に成功した行の次をカーソルとしてスカラ経路を再開する。`refRun` と再開後の結果（エラー一致を含む）が一致することを証明。反例 `[1, 2, INT64_MAX]` では先頭から再開すると先頭 2 行を二重加算して SUM が 6 になる（正しくは 3）。
- **first-error-ballot**: 参照器は行優先・演算次要の逐次評価で行内短絡。SIMD 側は `mask = live && err.isSome` の投票から辞書式最小（行→演算）を選ぶ。追加の `packed_error_ballot` は非ゼロガードと BitVec.ctz を検証する。演算番号を先に比較すると先頭行の後段オーバーフローより次行の先頭除算を誤って選ぶ反例を固定。
- **like-shift-and**: 非空 needle・バイト照合。理想の真偽ベクトル更新（各位置で開始ビットを注入し、文字マスクを掛ける）を先に定義し、k ワードの shift+carry 機械が理想状態と一致することを証明（needle 長 ≤ w*k、未使用上位ビットは消去、行頭で状態リセット）。63/64/65 境界と同一バイト反復を収録。行間で state を持ち越すと隣接行をまたいだ誤一致が生じる反例を固定。UTF-8 / 照合順は対象外。
- **limit-error-frontier**: レーンを `.skip` / `.pass` / `.error` に分類。参照器は行順で K 件通過または最初のエラーで停止し、SIMD 判定は最初のエラー位置と K 件目通過位置の前後で同じ結果を出す。K=0 はレーンを一切評価しない。複数バッチにまたがる残り需要の引き継ぎも証明。停止点より後は副作用のないマスク計算のみであることを前提とする。
- **masked-load-tail**: メモリを `Nat → α`、割当てを `[0, alloc)`、実際の読み取りをイベント列として記録。選んだ lowering は有効レーンのみアクセスする短い masked load（全幅ロード＋select ではない）。`base + r ≤ alloc` のもとで有効レーンの値がスカラ読取りと等しく、記録された全アドレスが割当て内であることを証明。r=0 はアクセスなし。全幅ロード＋select は `base + W > total` のとき無効レーンをゼロにしても割当て外を読むことを反例として固定。
- **regex-bitset-nfa**: 参照側は ε 遷移を含む NFA の到達状態集合（有限状態上の述語）、機械側は `BitVec W` のビット集合。ε 閉包を最小不動点として定義し、ε サイクル（star）でも停止することを示す。`N.states ≤ W` と上位ビットゼロを仮定して、各ステップ後・各入力位置での両者の一致と受理一致性を証明。`(ab|b)c` を `xbc` に適用したとき開始状態を初回しか注入しない実装は誤る反例を固定。キャプチャ・フラグ・アンカー・UTF-8 は対象外。
- **simd-group-conflicts** は代表キーの一意性と全キーを合算した COUNT/SUM の一致を証明した。追加の `representatives_per_key` と `checked_batch_scatter` が各キーを引いた結果と安全な INT64 加算を扱う。
- **topn-block-fence**: 順序項を `(値, ASC/DESC, NULLS FIRST/LAST)` として辞書順序を定義。項ごとの最良値を取った辞書式上界は、実在しない組合せでも保守的に K 番目より悪いことを証明。参照 Top-N は同率保持の順位ベースで定義し、fence が全残ブロックに成立するとき `refTopN(pre ++ remaining) = refTopN(pre)` を示す。混在順序 `a DESC, b ASC`、WITH TIES の等値境界、NULL 配置の具体例を収録。閾値は K 行確定後に発行する前提。

## 登録した反例（素朴な最適化が壊れる具体例）

1. `DeoptResume.restart_from_batch_head_unsound` — 再開位置をバッチ先頭にすると接頭辞を二重加算する。
2. `FirstErrorBallot.op_first_is_wrong` — 演算番号を先に比較すると別行のエラー種別を返す。
3. `LikeShiftAnd.no_reset_spurious_match` — 行頭で状態をリセットしないと隣接行をまたぐ誤一致が出る。
4. `MaskedLoadTail.full_width_load_violates_tail_access` / `full_width_load_reads_unassigned` — 全幅ロード＋select は割当て外を読み取る。
5. `RegexBitsetNfa.start_injection_once_is_wrong` — 開始状態を初回しか注入しないと `(ab|b)c` の `xbc` での一致を取りこぼす。
6. `SimdGroupConflicts.naive_gather_add_scatter_loses_increments` — `[A,B,A,A]` で A の増分を失う。
7. `SimdGroupConflicts.all_null_group_sum_is_not_zero` — 全 NULL グループの SUM は 0 ではなく NULL。
8. `TopNBlockFence` の各具体例 — 混在順序・WITH TIES 等値境界・NULL 配置で境界判定を誤らないこと（および誤った境界の扱い）を固定。

これらは「証明が失敗した issue」ではなく、**証明された主定理の前提条件が実際に必要であることを示す反例**として登録した。

## 既に証明済みだった 21 件 + 1 件の再検証

以下の 22 ファイルは個別コンパイルと `CheckProofs` を通ることを再確認した。
うち `OuterFetchTies.lean` はコンパイルエラー（未完了の補題 4 件）があったため修復し、`outer_fetch_ties_sound` を完全証明に戻した。

| issue | Lean ファイル | 主定理 |
| --- | --- | --- |
| adaptive-selection | `AdaptiveSelection.lean` | `adaptive_selection_sound` |
| block-kleene | `BlockKleene.lean` | `block_kleene_sound` |
| bloom-block-intersection | `BloomBlockIntersection.lean` | `bloom_block_intersection_sound` |
| checked-eager-sum | `CheckedEagerSum.lean` | `sql_checked_sum_reaggregation`, `sql_checked_eager_sum_sound` |
| dfa-block-summary | `DfaBlockSummary.lean` | `dfa_block_summary_sound` |
| filter-eval-order | `FilterEvalOrder.lean` | `filter_eval_order_sound` |
| late-gather-versions | `LateGatherVersions.lean` | `late_gather_versions_sound` |
| lazy-dictionary | `LazyDictionary.lean` | `lazy_dictionary_sound` |
| like-segment-chain | `LikeSegmentChain.lean` | `greedyChain_sound` 等 |
| like-underscore | `LikeUnderscore.lean` | `like_underscore_sound` |
| mask-to-ranges | `MaskToRanges.lean` | `mask_to_ranges_sound` |
| nullable-mark-join | `NullableMarkJoin.lean` | `nullable_mark_join_sound` |
| outer-fetch-ties | `OuterFetchTies.lean` | `sql_outer_fetch_ties_sound` |
| outer-or-branches | `OuterOrBranches.lean` | `outer_or_branches_sound` |
| rank-scatter | `RankScatter.lean` | `rank_scatter_sound` |
| regex-capture-priority | `RegexCapturePriority.lean` | `regex_capture_priority_sound` |
| regex-mandatory-literal | `RegexMandatoryLiteral.lean` | `regex_mandatory_literal_sound` |
| rle-run-skip | `RleRunSkip.lean` | `rle_run_skip_sound` |
| simd-in-range | `SimdInRange.lean` | `simd_in_range_sound` |
| snapshot-zone-map | `SnapshotZoneMap.lean` | `snapshot_zone_map_sound` |
| trigram-skip | `TrigramSkip.lean` | `trigram_skip_sound` |
| window-halo | `WindowHalo.lean` | `window_halo_sound` |

## 検証コマンド

```bash
# 全定理・反例定理が sorryAx に依存しないことの確認（CheckProofs）
cd lean && ~/.elan/bin/lake build

# 個別ファイルのコンパイル確認
cd lean && ~/.elan/bin/lake env lean TinylambOpt/<Name>.lean
```
