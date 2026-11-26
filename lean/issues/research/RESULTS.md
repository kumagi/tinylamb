# 研究 Issue の検証結果

各モデルの主定理と元の SQL・実装との対応は、[接続命題と監査後の状況](sql-refinement.md)を参照。

[research/README.md](README.md) の 26 件（初回 20 件 ＋ 提出型固定 6 件）と、
初回 20 件のうち型検査済み提出型が用意された `checked-aggregate-tree` の 1 件を合わせた 27 の提出物を
すべて証明した。反例は「素朴な最適化が壊れる具体例」として各モジュールに定理化して登録した
（主定理が偽だったものはない）。全モジュールは `TinylambOpt.lean` から import され、
`cd lean && lake build`（CheckProofs）が成功する。

```bash
cd lean && ~/.elan/bin/lake build
# 個別検証では lakefile.toml のオプションが効かないため、厳格モードを明示する：
~/.elan/bin/lake env lean -DrelaxedAutoImplicit=false TinylambOpt/Research/<Name>.lean
```

## 初回 20 件（参照計算と最適化計算を別定義した証明モデル）

| issue | Lean モジュール | 主定理 | 回帰反例 |
| --- | --- | --- | --- |
| adaptive-memo | `TinylambOpt/Research/AdaptiveMemo.lean` | `adaptive_memo_sound` | `key_level_done_set_counterexample` |
| null-aware-hyperjoin | `Research/NullAwareHyperjoin.lean` | `null_aware_hyperjoin_sound` | `global_null_filter_counterexample` |
| spill-bloom-epoch | `Research/SpillBloomEpoch.lean` | `spill_bloom_epoch_sound` | `parent_empty_child_deferred_counterexample` |
| outer-probe-cancel | `Research/OuterProbeCancel.lean` | `outer_probe_cancel_sound` | `early_unknown_then_true_counterexample` |
| compressed-join-dict | `Research/CompressedJoinDict.lean` | `compressed_join_dict_sound` | `same_code_different_generation_unsound` 等 4 件 |
| asof-snapshot-frontier | `Research/AsofSnapshotFrontier.lean` | `asof_snapshot_frontier_sound` | `deleted_newest_frontier_counterexample` 等 3 件 |
| tie-watermark | `Research/TieWatermark.lean` | `tie_watermark_sound` | `local_take_k_loses_tied_block_counterexample` |
| incremental-sort-page | `Research/IncrementalSortPage.lean` | `incremental_sort_page_sound` | `file_boundary_run_split_counterexample` |
| checked-aggregate-tree | `Research/CheckedAggregateTree.lean` | `checked_aggregate_tree_sound` | `reordered_partial_merge_counterexample` 等 3 件 |
| window-distinct-inverse | `Research/WindowDistinctInverse.lean` | `window_distinct_inverse_sound` | `set_deletion_loses_second_copy_counterexample` |
| adaptive-radix-replay | `Research/AdaptiveRadixReplay.lean` | `adaptive_radix_replay_sound` | `whole_parent_replay_double_adds_counterexample` |
| snapshot-index-intersect | `Research/SnapshotIndexIntersect.lean` | `snapshot_index_intersect_sound` | `rid_only_intersection_false_positive_counterexample` |
| runtime-predicate-dag | `Research/RuntimePredicateDag.lean` | `runtime_predicate_dag_sound` | `naive_all_lanes_is_unsound` |
| regex-tagged-determinize | `Research/RegexTaggedDeterminize.lean` | `regex_tagged_determinize_sound` | `state_set_merge_loses_capture` |
| like-multi-prefilter | `Research/LikeMultiPrefilter.lean` | `like_multi_prefilter_sound` | `overlapping_occurrences_not_shared`, `not_predicate_inverts_filter` |
| vector-tail-page | `Research/VectorTailPage.lean` | `vector_tail_page_sound` | `full_width_load_touches_guard`, `dropped_carry_misses_match` |
| ir-translation-validation | `Research/IrTranslationValidation.lean` | `ir_translation_validation_sound` | `nsw_sub_counterexample`, `full_load_select_counterexample` 等 |
| pull-demand-fusion | `Research/PullDemandFusion.lean` | `pull_demand_fusion_sound` | `demand_through_cast_counterexample`, `limit_zero_counterexample` |
| regex-cache-version | `Research/RegexCacheVersion.lean` | `regex_cache_version_sound` | `unmatched_capture_sharing_counterexample` 等 3 件 |
| mvcc-compaction-pin | `Research/MvccCompactionPin.lean` | `mvcc_compaction_pin_sound` | `rid_reuse_counterexample`, `tombstone_vs_unmatched_counterexample` |

各モジュールには doc コメントとして参照計算、最適化計算、十分条件、C++ 側の照合先を記載している。
計算量の結論はイベント／ステップ数を返す評価器（例: `extra_probes_le_candidates`, `sealedIO`,
`evalCost`, `update_count_bound`, `reread_bound`, `copyCost`, `batchedSteps`）として定式化し証明した。

## 提出型が固定された 7 件（`ResearchGoals` の `Goal` に対する `sound`）

| issue | 仕様モジュール | 提出モジュール | 結果 |
| --- | --- | --- | --- |
| pair-rank | `ResearchGoals/PairRank.lean` | `ResearchProofs/PairRank.lean` | `theorem sound : Goal` 証明 |
| outer-join-counts | `ResearchGoals/OuterJoinCounts.lean` | `ResearchProofs/OuterJoinCounts.lean` | `theorem sound : Goal` 証明 |
| demand-scalar | `ResearchGoals/DemandScalar.lean` | `ResearchProofs/DemandScalar.lean` | `theorem sound : Goal` 証明 |
| stable-radix | `ResearchGoals/StableRadix.lean` | `ResearchProofs/StableRadix.lean` | `theorem sound : Goal` 証明 |
| reciprocal-divmod | `ResearchGoals/ReciprocalDivMod.lean` | `ResearchProofs/ReciprocalDivMod.lean` | `theorem sound : Goal` 証明 |
| segmented-xor | `ResearchGoals/SegmentedXor.lean` | `ResearchProofs/SegmentedXor.lean` | `theorem sound : Goal` 証明 |
| checked-aggregate-tree | `ResearchGoals/CheckedAggregateTree.lean` | `ResearchProofs/CheckedAggregateTree.lean` | `theorem sound : Goal` 証明 |

主な証明の要点：

- **pair-rank**: 昇順左／降順右の階段 `countLE` が `sum ≤ t` の組数に等しいことと、9 段の二分探索が
  `k < count t` となる最小 t を返すことを証明し、完全積のソート結果に接続した。
- **outer-join-counts**: `histogram.getD = List.count` を示し、NULL は照合しないが集約では一グループに
  入ること、両側に存在するキーは出現数の積になることを全キーで証明。
- **demand-scalar**: 索引の `Hit` と相関フィルタ結果の対応、および `for` ループの出力列・最初の失敗
  外行番号を再帰評価器に翻訳して参照器と一致させた。
- **stable-radix**: 65 桁の変換キーが `DESC NULLS LAST` と同順であることをビットベクトルで証明し、
  各 pass の安定分配が下位桁からの整列を一段進めることを帰納した。
- **reciprocal-divmod**: `ceil(2^64/m)` による推定商が真の商の 1 つ上までに収まり、128 ビット積による
  補正で正確な商になることを証明。`tdiv`/`tmod` と符号復元、NULL・ゼロ除算・`INT64_MIN/-1` も照合。
- **segmented-xor**: 要約の合成則 `applySummary (combine a b) x = applySummary b (applySummary a x)`
  を示し、任意の分割木・初期 carry でストリーム復号と並列復号が一致することを帰納した。
- **checked-aggregate-tree**: 部分和の min/max が全 prefix を被覆すること、skip ガードが
  「全 prefix が INT64 内」と同値であることを含め、結果とイベント列の両方を証明した。

## 差し戻し後と新規の 7 件（第二波・`ResearchGoals` の `Goal` に対する `sound`）

初回登録後に差し戻された 6 件と新規 1 件。各課題は `lean/issues/research/` の提出型
（`ResearchGoals/<Name>.lean` の `Goal`）を一切変更せず、`ResearchProofs/<Name>.lean` に
`theorem sound : Goal` を構成した。

| issue | 仕様モジュール | 提出モジュール | 結果 |
| --- | --- | --- | --- |
| streaming-run-buffer | `ResearchGoals/StreamingRunPage.lean` | `ResearchProofs/StreamingRunPage.lean` | `theorem sound : Goal` 証明 |
| radix-retry-exactly-once | `ResearchGoals/RadixReplayOwnership.lean` | `ResearchProofs/RadixReplayOwnership.lean` | `theorem sound : Goal` 証明 |
| masked-page-load-trace | `ResearchGoals/PageLoadTrace.lean` | `ResearchProofs/PageLoadTrace.lean` | `theorem sound : Goal` 証明 |
| ordered-tag-frontier | `ResearchGoals/OrderedTagFrontier.lean` | `ResearchProofs/OrderedTagFrontier.lean` | `theorem sound : Goal` 証明 |
| emitted-filter-loop | `ResearchGoals/FilterLoopFootprint.lean` | `ResearchProofs/FilterLoopFootprint.lean` | `theorem sound : Goal` 証明 |
| skyline-sweep | `ResearchGoals/SkylineAntiJoin.lean` | `ResearchProofs/SkylineAntiJoin.lean` | `theorem sound : Goal` 証明 |
| distributed-page-fence | `ResearchGoals/DistributedPageFence.lean` | `ResearchProofs/DistributedPageFence.lean` | `theorem sound : Goal` 証明 |

主な証明の要点：

- **streaming-run-buffer**: 先頭キー単調・出現 ID 増加の入力に対し、run を終端で閉じず先頭キー
  替わりでのみ flush するストリームが、全体ソートの `SqlFetchWithTies.pageSQL` と同一のページを
  返すことを示した。ピークバッファが `maxRunLen`（同率ブロックの最大長）を超えないことも、
  ページ選択を `pageSel` 経由で `fetch` に接続して証明。
- **radix-retry-exactly-once**: 実行イベント履歴（drain/spill/repartition/forward）から
  `consumedIds` を定義し、drain 時にのみ消費が起き、履歴末尾の消費済み集合が状態の台帳と一致し、
  各出現 ID がちょうど一度消費される（履歴中の消費済み多重度 = 1）ことをイベント帰納で証明。
- **masked-page-load-trace**: マスク付きロードを (アドレス, 値) のチャンク列としてモデル化し、
  不連続ページ列・空ページ・任意の正ベクトル幅でも、チャンクの全アドレスがそのページの
  許可範囲内に収まること、一致候補がページ跨ぎの needle 照合（ページ間は比較しない）を
  正しい carry（needle 長決定的な末尾保持）で再現することを証明。
- **ordered-tag-frontier**: `firstStates` の畳み込みが、辺順・ε 展開順の最初の受理履歴を
  落とさない不変条件（訪れた状態の先頭記録が参照器の探索順と一致）を保つことを示し、
  履歴を先頭状態に圧縮しても勝者キャプチャが保存されることを証明。分岐がタグ内容を
  見ない仮定の下で同一状態から伸びる辺の集合が一致する点を用いる。
- **emitted-filter-loop**: 提出型 `FilterLoopFootprint.Goal` は、32 行本体・8 行余り・
  スカラー末尾のスケジュールが抽象行番号 `[0, n)` を重複なく被覆し、読み取り位置が
  バッファ長内に収まることを証明する。これはスケジュールの抽象モデルの証明であり、
  実生成 LLVM (`fixtures/filter-eq.ll`) の翻訳検証までは行っていない。実ファイルの受理・
  整形・検証器への入力処理は [未完了 Issue](emitted-filter-loop.md) に残る。
- **skyline-sweep**: `a.x < b.x ∧ a.y < b.y` の相関 NOT EXISTS に対し、x 昇順（タイは y 降順）の
  1 回ソート＋y 最小水位の走査が、参照側の二重ループと厳密に同じ出力多重度を返すことを証明。
  自己比較・NULL・同一座標の複数出現を fold の可換性 (`step_comm`) で処理した。
- **distributed-page-fence**: `fetch` を「カットオフ行 `cutoff` を使い `take n` 上の
  `filter (¬ltb cutoff) ++ [cutoff]`」と特徴づける補題 (`page_eq_filter`) と、ディレクトリ先頭キーの
  単調性（`kLe_trans`/`kLt_kLe_trans`）を軸に、block リスト帰納 (`sound_aux`) で
  `ordered spec seen` が `ordered spec allRows` の先頭 `off+k+1` 行と一致することを示した。
  これによりページ選択 `page spec off k` が scan 前後で同一であり、かつ `opened` は
  「`k != 0` かつ全体行に対する `eligible` を満たすブロック列」と一致する。`k = 0` は
  左右とも空列として分離処理。二つ目の連言（`opened` の特徴づけ）も同じカットオフ単調性で
  接頭部分と接尾部分を判定し `filter_eq_take_of_prefix` で閉じた。

## 追加第三波（6/6 件 証明済み）

第三波の 6 件すべてについて提出型を変更せず `theorem sound : Goal` を構成した。

| issue | 仕様モジュール | 提出モジュール | 結果 |
| --- | --- | --- | --- |
| except-all-debt | `ResearchGoals/ExceptAllDebt.lean` | `ResearchProofs/ExceptAllDebt.lean` | `theorem sound : Goal` 証明 |
| interval-count-sweep | `ResearchGoals/IntervalCountSweep.lean` | `ResearchProofs/IntervalCountSweep.lean` | `theorem sound : Goal` 証明 |
| recursive-delta | `ResearchGoals/RecursiveDelta.lean` | `ResearchProofs/RecursiveDelta.lean` | `theorem sound : Goal` 証明 |
| nth-non-null-frame | `ResearchGoals/NthNonNullFrame.lean` | `ResearchProofs/NthNonNullFrame.lean` | `theorem sound : Goal` 証明 |
| range-distinct-index | `ResearchGoals/RangeDistinctIndex.lean` | `ResearchProofs/RangeDistinctIndex.lean` | `theorem sound : Goal` 証明 |
| quantified-range-summary | `ResearchGoals/QuantifiedRangeSummary.lean` | `ResearchProofs/QuantifiedRangeSummary.lean` | `theorem sound : Goal` 証明 |

証明の要点：各 `Part` の `balance`（`Std.HashMap` の符号付き行数）が
「左側の行数 − 右側の行数」に等しいことを `(rows.foldl insert).getD` の帰納
（`countRows_fold_getD` / `getD_foldl_entries`）と `Std.HashMap.fold_eq_foldl_toList`
＋`distinct_keys_toList` から導いた。`output` の各行の出現回数は
`(balance.getD r 0).toNat` であり、`reference`（右側を順に `List.erase` した多重集合）の
出現回数は `left.count r − min (left.count r) (right.count r)` に等しい。
両者を `List.perm_iff_count` で照合した。非 NULL 行の重複・片側だけの行・
`merge` の入れ子・符号が打ち消されて 0 になるエントリをすべて含む。

各証明の要点：

- **interval-count-sweep**: イベントを `4*time + phase` の線形キーとしてソート順序を特徴づけ、
  `Id.run`/`for` ループを `foldl` と構造再帰へ書き換えた。開始・停止イベントの個数を
  対応する区間の個数に結び、問い合わせ時刻 `t` 以前のイベントだけが先行することから
  `active = #(lo ≤ t) − #(hi ≤ t) = #contains` を導いた。
- **recursive-delta**: `eraseDups` の重複除去と「非空 frontier ならラウンドごとに新しい辺が
  1 本必要」という鳩の巣原理から `edges.length+1` ラウンドでの停止を示した。
  `seen = expanded ++ frontier` 不変条件と隣接索引の等価性から `Reach` との一致を証明。
- **nth-non-null-frame**: `ranks[i] = (xs.take i).filterMap id |>.length` と
  `values = xs.filterMap id` をsnoc帰納で示し、フレームの非 NULL 列をランク区間として
  特徴づけた。現在行の除外はランクの 1 インクリメントとして表され、末尾の境界检查と
  合わせて `selectNth = reference` を導いた。
- **range-distinct-index**: 木がマーク位置の集合を正確に表す関係 `WF` を導入し、
  `mark` が 1 位置の消去・挿入であることと `suffix` が `≥ lo` のマーク数を返すことを証明。
  `n < 2^(n.log2+1)` から深さが十分であることを示し、最後尾出現リストと
  区間の DISTINCT 個数を接続した。
- **quantified-range-summary**: `index rows` の各キーが `push` の畳み込みにより
  min/max/NULL 有無を保持することを示し、`foldl SqlBool.or/and` を TRUE/FALSE 項の
  存在で特徴づけた。空集合と全 NULL 群、NULL の外部キー・外部値の場合分けを網羅した。

反例の登録はない。中途の証明案はリポジトリ外に退避してある。

## 第四波（jit-skip 16 件 + engine-opt 20 件・全 36 件証明済み）

`engine-opt` の 20 件と `jit-skip` の 16 件、計 36 件すべてについて提出型を変更せず
`theorem sound : Goal` を構成した。反例はすべて連言（`∃` 節）として提出型側に固定済みで、
`sound` の一部として証明している。まず指定された 5 件。

| issue | 提出モジュール | 結果 |
| --- | --- | --- |
| bytecode-side-table-equiv | `ResearchProofs/BytecodeSideTableEquiv.lean` | `theorem sound : Goal` 証明 |
| exact-int-total-overflow-guard | `ResearchProofs/ExactIntTotalOverflowGuard.lean` | `theorem sound : Goal` 証明 |
| gathered-small-dict-fast-path | `ResearchProofs/GatheredSmallDictFastPath.lean` | `theorem sound : Goal` 証明 |
| pair-bitmap-dense-remap | `ResearchProofs/PairBitmapDenseRemap.lean` | `theorem sound : Goal` 証明 |
| plan-cache-toggle-invalidation | `ResearchProofs/PlanCacheToggleInvalidation.lean` | `theorem sound : Goal` 証明 |

証明の要点：

- **bytecode-side-table-equiv**: `sameFullProgram` が構造的等価であることを使い、
  `assignSlots` が同じスロットに異なるプログラムを割り当てないことと、
  `uniqueProgs` が各スロットにちょうど一つの代表を持つことを示した。
  これによりスロット参照評価が `evalRefSlots` と一致する。`resultTypeTag` だけを
  比べるバグ版（TPC-DS Q90 の別名バグ）は反例として提出型側に固定されている。
- **exact-int-total-overflow-guard**: ブロック每行の絶対値和が `blockAbsBound` で
  押さえられることと、部分和の `natAbs` が残り予算以下であることを同時帰納で示し、
  `≤ int64Max.toNat`（2^63−1）なら全部分和が `[-2^63, 2^63−1]` に収まることを
  証明した。境界が等値でも安全で、`refCheckedSum` が `.ok` を返す。
- **gathered-small-dict-fast-path**: `findIdx?` による密 ID と `compactDict` の
  対応（`dict.Nodup` と `eraseDups` の重複除去を使用）を証明し、`sel` 上の畳み込みが
  参照のグループ和と同じ不変条件を保つことを示した。NULL バケットはセンチネルと
  分離され、未参照コードと混同されない。
- **pair-bitmap-dense-remap**: `bitIndex` が有効な (外側添字, 内側値) に対して
  単射であること（内側オフセットが `span` より小さいことから）を示し、
  マスクのビットが立つことと元のペアが存在することを双方向に結んだ。
  `mn/mx` は格納ペアの実 min/max なので範囲外の値は生じない。
- **plan-cache-toggle-invalidation**: 消費済み接頭辞に対する強い不変条件
  （現在 cfg = `cfgBefore`、キャッシュキー = (sqlId, 実行時 cfg)、
  キー存在 ⇔ 同じ構成で以前にクエリあり）を `ops` 帰納で示し、
  再コンパイル旗が `expectedRecompiles` に等しいことを接続した。

残り 15 件（`engine-opt`）：

| issue | 提出モジュール | 結果 |
| --- | --- | --- |
| lazy-block-cache-reuse | `ResearchProofs/LazyBlockCacheReuse.lean` | `theorem sound : Goal` 証明 |
| block-lazy-zone-prune | `ResearchProofs/BlockLazyZonePrune.lean` | `theorem sound : Goal` 証明 |
| footer-only-open-equiv | `ResearchProofs/FooterOnlyOpenEquiv.lean` | `theorem sound : Goal` 証明 |
| column-mmap-evict-epoch | `ResearchProofs/ColumnMmapEvictEpoch.lean` | `theorem sound : Goal` 証明 |
| wal-checkpoint-tlcs-boundary | `ResearchProofs/WalCheckpointTlcsBoundary.lean` | `theorem sound : Goal` 証明 |
| two-probe-packed-bloom | `ResearchProofs/TwoProbePackedBloom.lean` | `theorem sound : Goal` 証明 |
| mono-contiguous-fact-slice | `ResearchProofs/MonoContiguousFactSlice.lean` | `theorem sound : Goal` 証明 |
| packed-radix-2x32-batch-agg | `ResearchProofs/PackedRadix2x32BatchAgg.lean` | `theorem sound : Goal` 証明 |
| two-level-partitioned-agg-merge | `ResearchProofs/TwoLevelPartitionedAggMerge.lean` | `theorem sound : Goal` 証明 |
| dict-prefix-monotonic-range | `ResearchProofs/DictPrefixMonotonicRange.lean` | `theorem sound : Goal` 証明 |
| bounded-heap-topn-merge | `ResearchProofs/BoundedHeapTopNMerge.lean` | `theorem sound : Goal` 証明 |
| substr-ascii-byte-slice-equiv | `ResearchProofs/SubstrAsciiByteSliceEquiv.lean` | `theorem sound : Goal` 証明 |
| count-distinct-adaptive-switch | `ResearchProofs/CountDistinctAdaptiveSwitch.lean` | `theorem sound : Goal` 証明 |
| limit-offset-prefix-composition | `ResearchProofs/LimitOffsetPrefixComposition.lean` | `theorem sound : Goal` 証明 |
| cte-single-pass-split-filter | `ResearchProofs/CteSinglePassSplitFilter.lean` | `theorem sound : Goal` 証明 |

`jit-skip` の 16 件：

| issue | 提出モジュール | 結果 |
| --- | --- | --- |
| swar-digit-ballot | `ResearchProofs/SwarDigitBallot.lean` | `theorem sound : Goal` 証明 |
| checked-lane-add | `ResearchProofs/CheckedLaneAdd.lean` | `theorem sound : Goal` 証明 |
| row-in-mask | `ResearchProofs/RowInMask.lean` | `theorem sound : Goal` 証明 |
| bit-plane-sum | `ResearchProofs/BitPlaneSum.lean` | `theorem sound : Goal` 証明 |
| selection-mask-deposit | `ResearchProofs/SelectionMaskDeposit.lean` | `theorem sound : Goal` 証明 |
| quoted-delimiter-mask | `ResearchProofs/QuotedDelimiterMask.lean` | `theorem sound : Goal` 証明 |
| packed-order-key | `ResearchProofs/PackedOrderKey.lean` | `theorem sound : Goal` 証明 |
| hamming-one-search | `ResearchProofs/HammingOneSearch.lean` | `theorem sound : Goal` 証明 |
| threshold-bit-counter | `ResearchProofs/ThresholdBitCounter.lean` | `theorem sound : Goal` 証明 |
| bit-signature-count | `ResearchProofs/BitSignatureCount.lean` | `theorem sound : Goal` 証明 |
| congruence-count | `ResearchProofs/CongruenceCount.lean` | `theorem sound : Goal` 証明 |
| bigram-chain-skip | `ResearchProofs/BigramChainSkip.lean` | `theorem sound : Goal` 証明 |
| first-key-block | `ResearchProofs/FirstKeyBlock.lean` | `theorem sound : Goal` 証明 |
| overlap-pair-count | `ResearchProofs/OverlapPairCount.lean` | `theorem sound : Goal` 証明 |
| offset-bitmap-gather | `ResearchProofs/OffsetBitmapGather.lean` | `theorem sound : Goal` 証明 |
| checked-abs-fence | `ResearchProofs/CheckedAbsFence.lean` | `theorem sound : Goal` 証明 |

第四波の主な証明の要点：

- **lazy-block-cache-reuse**: 世代番号付きキャッシュ状態の不変条件（キャッシュ済みスタンプ ≤ 現在世代、現世代エントリはストレージと一致、ヒット判定 = 参照フラグ列）を操作列の帰納で保ち、`optRun` を世代・退避・変更に場合分けして `refRun` とデコードフラグ列の双方で一致させた。
- **wal-checkpoint-tlcs-boundary**: チェックポイント待機（`WaitForDurable`）と TLCS 境界・MVCC 書込み意図の順序交換を、観測履歴の接頭辞帰納で復旧同値として証明。do ループの継続 (`runK.match_1`) を明示的に参照してループを畳んだ。
- **packed-radix-2x32-batch-agg**: `tryPack` 成功キーは fast 表、失敗キーは fallback 表に、という有効性不変条件をワーカー直列・マージで保存し、`queryMergedSum` の `ite` 分岐がフォールバックキーと衝突しないことを矛盾帰納で示して参照畳み込みと接続した。
- **two-level-partitioned-agg-merge**: `(count, sum)` 対の和を可換モノイドとして、パーティション帰属（`hash k % numParts`）がキーごとに一意であることを区間排他性の補題にまとめ、1 段目チャンク集約のフラット化と単一スレッド参照の逐語一致を証明した。
- **count-distinct-adaptive-switch**: 4 状態（empty/single/small/large）の不変条件（エントリ互相異なる・NULL 除外・参照集合との等価）を挿入・マージの全経路で保存し、`Nodup` 2 列の membership 同値から長さ一致（`nodup_same_length`）で `finalize` と `eraseDups.length` を結んだ。
- **first-key-block**: ヘッダビットマップの補集合条件（`header &&& ~~~seen = 0`）とブロック内全キー既出を結び、ブロック単位スキップが重複排除フルスキャンと同一の行列・同一の「開いた」ブロック列を作ることを事前語帰納で示した。
- **checked-abs-fence**: `canSkip` 済みページには `INT64_MIN` も `|x| < limit` も存在しないことを極値境界から示すフェンス補題と、do/match 機械のループを手書き `go` に写像する橋渡し（`run.match_1` を明示参照）で、スキップ後の開-page 列と参照を開いた。
- **hamming-one-search**: `exact`/`one` の 2 シフトレジスタ不変条件を前方ウィンドウ一致として snoc 帰納で強化し、`forIn ... pure (ForInStep.yield)` 列を `List.forIn_pure_yield_eq_foldl` で畳み込みに同一化、誤り ≤1 の全開始位置（昇順）を `List.range` フィルタとして特徴づけた。
- **column-mmap-evict-epoch / footer-only-open-equiv / block-lazy-zone-prune**: ピン保護 LRU の退避順序上界、フッタ限定オープン＋オンデマンド列スライス、ZoneMap 判定＋キーデコード＋一致ブロック限定デコードの各経路を、共通のページ評価器上でフル読取りと逐語一致させた。
- **two-probe-packed-bloom / mono-contiguous-fact-slice / dict-prefix-monotonic-range / bounded-heap-topn-merge / substr-ascii-byte-slice-equiv / limit-offset-prefix-composition / cte-single-pass-split-filter**: いずれもオフセット加算済みキーの 2 プローブ無偽陰性、単調キー二分探索区間、`LIKE 'prefix%'` の `0xFF` 繰り上げ区間変換、Top-(offset+limit) 2 段マージ、ASCII `SUBSTR` の O(1) バイトスライス、`prior_offset` 前置スキップ合成、共有 CTE 3 値フィルタ 1 パス集約を、参照計算との逐語一致で証明した。
- **swar-digit-ballot / checked-lane-add / row-in-mask / bit-plane-sum / selection-mask-deposit / quoted-delimiter-mask / packed-order-key / threshold-bit-counter**: `BitVec` の `getLsbD_and/or/xor`・`signedOverflow`・PDEP 型deposit・6 段 prefix XOR・符号反転 xor 順序キー（`xor_flip_int`）などビットレベル補題を土台に、各マスク計算が逐行参照と一致することを Lane ごとの場合分け＋`omega` で証明した。
- **bit-signature-count / congruence-count / bigram-chain-skip / overlap-pair-count / offset-bitmap-gather**: 共通ページ評価器 `BlockCountModel` 上で、AND/OR 署名の三分類、GCD 剰余判定、隣接バイト対除外、区間 JOIN ページ対の三分割（`histogram_fold_getD` の HashMap 畳み込み）、OFFSET 捨棄ページの payload 非読取りを、それぞれ件数・読取り集合の一致として証明した。

## 第二波からの残存・複合提出型（1 件 証明済み）

| issue | 仕様モジュール | 提出モジュール | 結果 |
| --- | --- | --- | --- |
| tie-watermark + incremental-sort-page | `ResearchGoals/SqlFetchWithTies.lean` | `ResearchProofs/SqlFetchWithTies.lean` | `theorem sound : Goal` 証明 |

分散 FETCH WITH TIES の 2 エンジンを跨ぐ複合提出型で、連言第 1 部はパディング比較と
`keyLe`/`keyLt` の橋（`bridge_le`/`bridge_lt`）から `wmRead` の takeWhile 出力を SQL
rank フィルタと一致させ、第 2 部は連結済み走の `mergeSort` 特徴づけと境界キー前後の
tie 共有順位計算で `incPage` をページ SQL と一致させた。`k = 0` は仕様側 `zero_sql` を使用。

## 第五波（ベンチマーク敗北 27 件・完了）

第五波の 27 件（`issues/research/benchmark-losses/`）について、26 件の証明と 1 件の反例を登録した。
提出型は変更せず、各提出モジュールは `TinylambOpt.lean` から import されている。
`lake build`（CheckProofs を含む）が成功することを確認済み。

### 反例（主定理が偽だった課題）

- **functional-dependent-group-key-elim**: `TinylambOpt/ResearchProofs/FunctionalDependentGroupKeyElim.lean` に
  `theorem disproof : ¬ Goal` を登録。`lim = 1`、`rows = [int64Max + 1]` のとき、
  参照側 `refGroupedTopK` は `some [((int64Max+1, int64Max, int64Max-1, int64Max-2), 1)]` を返すが、
  最適化側 `optKeyOnlyTopK` のガード `int64Min + 3 ≤ k ∧ k ≤ int64Max` がこれを拒否して `none` を返す。
  `checkedSub (int64Max+1) d` は `d = 1,2,3` いずれも安全域に収まるため、ガードの上限は
  `int64Max + 1` まで緩める必要がある（あるいは入力を妥当な INT64 値に制限する）。
  第二連言（`badUncheckedDeferTopK` のエラー隠蔽反例）は真だが、連言全体は偽。

証明済み（26 件）：

| issue | 提出モジュール | 結果 |
| --- | --- | --- |
| unconditional-block-sum-avg-reuse | `ResearchProofs/UnconditionalBlockSumAvgReuse.lean` | `theorem sound : Goal` 証明 |
| immutable-tlcs-zonemap-row-bound | `ResearchProofs/ImmutableTlcsZoneMapRowBound.lean` | `theorem sound : Goal` 証明 |
| single-pass-multi-consumer-fact-scan | `ResearchProofs/SinglePassMultiConsumerFactScan.lean` | `theorem sound : Goal` 証明 |
| left-join-preserved-to-nullable-filter-transfer | `ResearchProofs/LeftJoinPreservedToNullableFilterTransfer.lean` | `theorem sound : Goal` 証明 |
| chan-welford-shared-variance-state | `ResearchProofs/ChanWelfordSharedVarianceState.lean` | `theorem sound : Goal` 証明 |
| correlated-agg-partition-key-filter-pushdown | `ResearchProofs/CorrelatedAggPartitionKeyFilterPushdown.lean` | `theorem sound : Goal` 証明 |
| unique-key-left-join-elision-and-mark | `ResearchProofs/UniqueKeyLeftJoinElisionAndMark.lean` | `theorem sound : Goal` 証明 |
| disjunctive-exists-cascading-bitset | `ResearchProofs/DisjunctiveExistsCascadingBitset.lean` | `theorem sound : Goal` 証明 |
| selective-filter-late-slice-group | `ResearchProofs/SelectiveFilterLateSliceGroup.lean` | `theorem sound : Goal` 証明 |
| cross-table-disjunctive-branch-mask | `ResearchProofs/CrossTableDisjunctiveBranchMask.lean` | `theorem sound : Goal` 証明 |
| bidirectional-equijoin-semijoin | `ResearchProofs/BidirectionalEquiJoinSemijoin.lean` | `theorem sound : Goal` 証明 |
| nonequi-self-exists-minmax-witness | `ResearchProofs/NonEquiSelfExistsMinMaxWitness.lean` | `theorem sound : Goal` 証明 |
| float-fubini-having-band | `ResearchProofs/FloatFubiniHavingBand.lean` | `theorem sound : Goal` 証明 |
| canonical-equiv-id-set-algebra | `ResearchProofs/CanonicalEquivIdSetAlgebra.lean` | `theorem sound : Goal` 証明 |
| varchar-offset-only-length-sum | `ResearchProofs/VarcharOffsetOnlyLengthSum.lean` | `theorem sound : Goal` 証明 |
| capped-group-early-freeze | `ResearchProofs/CappedGroupEarlyFreeze.lean` | `theorem sound : Goal` 証明 |
| surrogate-key-channel-pivot-late-dim | `ResearchProofs/SurrogateKeyChannelPivotLateDim.lean` | `theorem sound : Goal` 証明 |
| full-outer-indicator-bitset-complement | `ResearchProofs/FullOuterIndicatorBitsetComplement.lean` | `theorem sound : Goal` 証明 |
| identity-fk-left-join-histogram | `ResearchProofs/IdentityFkLeftJoinHistogram.lean` | `theorem sound : Goal` 証明 |
| disjoint-case-pivot-bucket-dispatch | `ResearchProofs/DisjointCasePivotBucketDispatch.lean` | `theorem sound : Goal` 証明 |
| offset-heavy-reverse-topn-and-quotient-bucket | `ResearchProofs/OffsetHeavyReverseTopNAndQuotientBucket.lean` | `theorem sound : Goal` 証明 |
| packed-gid-distinct-sort-dedup | `ResearchProofs/PackedGidDistinctSortDedup.lean` | `theorem sound : Goal` 証明 |
| empty-string-key-partition-split | `ResearchProofs/EmptyStringKeyPartitionSplit.lean` | `theorem sound : Goal` 証明 |
| rank-self-join-to-lag-lead-and-hash-window | `ResearchProofs/RankSelfJoinToLagLeadAndHashWindow.lean` | `theorem sound : Goal` 証明 |
| late-wide-row-topn-block-fence | `ResearchProofs/LateWideRowTopNBlockFence.lean` | `theorem sound : Goal` 証明 |
| monotonic-key-stream-topk | `ResearchProofs/MonotonicKeyStreamTopK.lean` | `theorem sound : Goal` 証明 |

## 第六波（派生分析ベンチマーク 18 件・完了）

`issues/research/analytics-losses/` の 18 件すべてについて提出型を変更せず
`theorem sound : Goal` を構成した。反例はない。`lake build`（CheckProofs を含む）が成功。

| issue | 提出モジュール | 結果 |
| --- | --- | --- |
| binary-collation-string-equi-join-elision | `ResearchProofs/BinaryCollationStringEquiJoinElision.lean` | `theorem sound : Goal` 証明 |
| bushy-join-associativity-with-dead-key-prune | `ResearchProofs/BushyJoinAssociativityWithDeadKeyPrune.lean` | `theorem sound : Goal` 証明 |
| bounded-count-radix-desc-sort-equiv | `ResearchProofs/BoundedCountRadixDescSortEquiv.lean` | `theorem sound : Goal` 証明 |
| compressed-string-prefix-hash-equiv | `ResearchProofs/CompressedStringPrefixHashEquiv.lean` | `theorem sound : Goal` 証明 |
| factorized-join-tree-count-propagation | `ResearchProofs/FactorizedJoinTreeCountPropagation.lean` | `theorem sound : Goal` 証明 |
| columnar-to-pg-wire-chunked-framing-equiv | `ResearchProofs/ColumnarToPgWireChunkedFramingEquiv.lean` | `theorem sound : Goal` 証明 |
| inner-join-unreferenced-side-weight-pushdown | `ResearchProofs/InnerJoinUnreferencedSideWeightPushdown.lean` | `theorem sound : Goal` 証明 |
| left-join-right-multiplicity-collapse | `ResearchProofs/LeftJoinRightMultiplicityCollapse.lean` | `theorem sound : Goal` 証明 |
| self-join-shared-scan-dual-filter-split | `ResearchProofs/SelfJoinSharedScanDualFilterSplit.lean` | `theorem sound : Goal` 証明 |
| transitive-equiv-predicate-pushdown-and-dup-key-drop | `ResearchProofs/TransitiveEquivPredicatePushdownAndDupKeyDrop.lean` | `theorem sound : Goal` 証明 |
| null-or-like-dict-bitmap-prefilter | `ResearchProofs/NullOrLikeDictBitmapPrefilter.lean` | `theorem sound : Goal` 証明 |
| fanout-weighted-bucket-work-split | `ResearchProofs/FanoutWeightedBucketWorkSplit.lean` | `theorem sound : Goal` 証明 |
| bounded-multi-key-direct-array-aggregate | `ResearchProofs/BoundedMultiKeyDirectArrayAggregate.lean` | `theorem sound : Goal` 証明 |
| small-dim-dense-array-direct-lookup | `ResearchProofs/SmallDimDenseArrayDirectLookup.lean` | `theorem sound : Goal` 証明 |
| star-hub-multiway-weighted-product | `ResearchProofs/StarHubMultiwayWeightedProduct.lean` | `theorem sound : Goal` 証明 |
| semijoin-guarded-checked-eager-sum | `ResearchProofs/SemijoinGuardedCheckedEagerSum.lean` | `theorem sound : Goal` 証明 |
| multi-block-local-dict-two-stage-aggregate | `ResearchProofs/MultiBlockLocalDictTwoStageAggregate.lean` | `theorem sound : Goal` 証明 |
| bilateral-eager-agg-join-pushdown | `ResearchProofs/BilateralEagerAggJoinPushdown.lean` | `theorem sound : Goal` 証明 |

## 第九波（クラウド・BI の命題 42 件・完了）

[cloud-bi.md](cloud-bi.md) の 42 件（Quotes 10 / NestedBI 10 / DeleteFiles 11 / PublicBI 11）
すべてについて、`ResearchGoals/CloudBI/<モジュール>.lean` の提出型を一切変更せず、
`ResearchProofs/CloudBI/<モジュール>.lean` に `theorem sound_<宣言名> : <宣言名>.Goal` を構成した。
反例はなく、全命題が真だった。共通補題ライブラリ `ResearchProofs/CloudBI/Common.lean`
（namespace `…CloudBI.Helpers`、`Totals`/`summarize` の和算、`countP`・`mergeSort`・
`eraseDups` 補題群）を新設し、4 モジュールから import している。4 モジュールとも
`TinylambOpt.lean` から import 済みで、`cd lean && ~/.elan/bin/lake build`（CheckProofs の
`sorryAx` 検査込み）が成功すること、および全 42 定理の `#print axioms` が
`propext` / `Classical.choice` / `Quot.sound` のみ（`sorryAx` なし）であることを確認した。

| 仕様モジュール | 提出モジュール | 結果 |
| --- | --- | --- |
| `ResearchGoals/CloudBI/Quotes.lean` | `ResearchProofs/CloudBI/Quotes.lean` | 10/10 `theorem sound_* : *.Goal` 証明 |
| `ResearchGoals/CloudBI/NestedBI.lean` | `ResearchProofs/CloudBI/NestedBI.lean` | 10/10 証明 |
| `ResearchGoals/CloudBI/DeleteFiles.lean` | `ResearchProofs/CloudBI/DeleteFiles.lean` | 11/11 証明 |
| `ResearchGoals/CloudBI/PublicBI.lean` | `ResearchProofs/CloudBI/PublicBI.lean` | 11/11 証明 |

証明の要点（宣言名ごと）：

### Quotes（CostBench の取り込み・維持・読者）

- **StaleRollup**: 水位線で切った半開区間が hit-set の相互非交な被覆になることを
  `filter` の論理合成で示し、`summarize_append` ＋ `summarize_perm` で部分和の和を
  全体集計へ接続した。
- **SparseRefresh**: `foldl` の不変条件をアキュムレータまで一般化して帰納し、稀疏キー列が
  `Nodup` を保つこと・部分集計が到着順の `plus` 畳み込みと一致することを示した。
- **CloseTournament**: `latest` が (ts, tie) の辞書式最大を返すことを折りの支配不変条件
  （帰属＋全元素支配）として特徴づけ、関数従属性 `(同 ts・同 tie ⇒ 同 bid)` から勝者を
  一通りに決めて、flatten 順列を候補集合への移動で transfer した。
- **CandleFringe**: `whole` タイル id 列（`eraseDups` 由来で `Nodup`）のfoldと fringe 行の
  `filter` に分割し、`p ∨ q` の被覆と `p ∧ q = false` の非交から和の再接合を証明した。
- **SpreadMoments**: `filterMap` による bid/ask ペア抽出を列ごとの和へ書き換える補題
  （`sum_getD_eq_filterMap`）を示し、Common の `summarize_cnt`/`summarize_nn`/
  `summarize_sum` と組み合わせて件数・総和の一致を揃えた。
- **FenceRead**: run 列帰納と `List.flatten_cons` ＋ `List.filter_append` で残余述語付き
  読取りをフル読取りに接続。ヒット行は必ず fence を開くことを min/max の所属補題で示した。
- **ReclusterSwap**: `pin < cut` の場合分けで新旧 run の選択を特徴づけ、flatten の
  並べ替えと単調な被覆述語により二重計上・行欠落がないことを示した。
- **RetryRefresh**: 配送列帰納で `seen = v.reverse ++ u`・`v.Nodup`・メンバシップ同値の
  不変条件を保ち、最終的に重複なしキー列の並べ替え (`perm_of_nodup_mem`) と
  flatMap 畳み込みの融合で「不変バッチの再配送は一度だけ集計」を証明した。
- **ReclusterBudget**: `mergeSort` の降順性（pairwise）に対する帰納と先頭交換
  （`perm_move_front` ＋ `map_sum_perm`）で「同一コストなら頻度上位 k 本選びが最大削減」
  を示した。
- **MaintenanceBreakEven**: `s ≤ time` を `time = s + d` に分解し、分配法則の
  `simp` と `Nat.lt_of_add_lt_add_left` で score 減少条件へ閉じた。

### NestedBI（Snowset/PBench 派生の巨大 SQL）

- **BranchHistogram**: ネストした UNION ALL を枝 ID のヒストグラムに畳み、
  `TSum (replicate n a) = scaleT a n` と部分集計の掛算で逐語一致を示した。
- **JoinedLeafWeights**: 通常枝件数と結合枝重みを分離した私有集約（一キーに閉じた
  fold 融合）を組み、重複次元・NULL 入りの袋でも `Nodup` 単一折りの補題で閉じた。
- **FlattenSum**: 空入力のスカラー SUM が返す NULL 一行の規則を明示した畳み込みで、
  複数列 flatten 後の総和とネストした SUM の最終値の一致を示した。
- **ProjectionChain**: 射影の合成を列位置表へ落とし、`List.map` の交換と getD 境界で
  重複列・並べ替え・NULL 拡張を処理した。
- **RangeMultiplicity**: 重なり半開区間列を境界点のソート列へ置き換える写像を作り、
  各値の寄与回数が `weighted` の畳み込みと一致することを接頭・接尾の分解で示した。
- **MemoMisses**: 部分集計キャッシュ折りの四成分不変条件（出力・定義域・値・呼出し数）を
  帰納で保ち、計算回数が相異なる枝 ID 数（`eraseDups.length`）に等しいことを証明した。
- **SpoolBudget**: 直接読み・保存再読のコスト式の場合分けを `Nat.succ_mul` ＋ `omega` で
  整理し、最小コスト選択が正しく下界になることを示した。
- **GroupSignature**: GROUP BY キー列の重複位置を落としても NULL を含む同値類の写像が
  保存されることを、キー比較の決定性と snoc 帰納で証明した。
- **CanonicalFilters**: 正準化キー列のヒストグラムが `mergeSort` で不変であることと
  多重度保存を示した（`histogram_foldl_add` の橋渡し経由）。
- **SharedProjection**: 合成後位置表が同一の消費者を統合しても、異なる射影から同じ値が
  出る場合を含め出現数（`filterMap` 列）が変わらないことを示した。

### DeleteFiles（Iceberg Merge-on-Read）

- **EqualityHorizon**: `eqIndex` HashMap 折りのステップ補題（(scope, key) の最新
  sequence は global/local 最大）を delete 列帰納で示した。
- **PositionMask**: `HashSet` 構築折りの `contains` 特徴づけ＋点ごとの Bool 書換えで、
  ファイル確定済み位置集合の全行再利用を証明した。
- **OverlapCount**: 2 種マスクの重なりを含む・引く の包除（`countP_add_split` ＋
  `filter_not_countP`）で生存数をフッター行数から復元した。
- **FooterCount**: `eraseDups` 経由の `filter` と `range` 経由の `filter` の並べ替えを
  構成し、範囲外 ordinal・重複を除いた位置集合だけで COUNT が一致することを示した。
- **ManifestHorizon**: synopsis が正しければヒットは必ず `mayOpen` を誘発することを示し、
  マニフェスト折りへ帰納で持ち上げた。
- **NullAwarePruning**: `any_filter` と「行にヒットする delete はその key を正確に運ぶ」
  ことで、上下限＋NULL 可能性による絞り込みの安全性を証明した。
- **FieldIdProbe**: `idxOf`/`find?` の zip 逐点一致を帰納で示し、field ID → 物理位置の
  一括変換と欠落 optional 列の NULL 既定値を `map_map` で接続した。
- **CompactOrdinal**: 削除適用後の生存行の新位置が、旧位置より前の生存数
  （`countP keep` の接頭辞）に等しいことを行列帰納で証明した。
- **DeleteAwareLimit**: 削除数上界での overfetch が `filter` と可換であることを直接の
  補助ループ帰納で示し、打ち切り後の LIMIT 結果の保存を得た。
- **SkippedOrdinalBase**: スキャン折りが `zipIdx` グループ flatten への単一 `filterMap` に
  等しいことを示し、読み飛ばし後の ordinal が正しい位置に当たることを証明した。
- **BitmapEpoch**: 関係ない delete は sequence を上げても不活性（ヒット集合不変）である
  ことを示し、キー判定再利用の安全境界を導いた。

### PublicBI（MLB_11 の軸列挙・属性・文字列計算）

- **PartialExtrema**: ブロック部分 MIN/MAX（Option 極値）の併合結合と群の有無を NULL 列
  状態から分離した不変条件で証明した。
- **LiftedDecimals**: extrema 計算後の固定小数スケール揃えについて、負値・全 NULL を
  含め場合分けで一致を示した。
- **ConstantGroup**: 3 キー定数固定の一群実行が、該当なし（空）と全測定値 NULL の一群を
  区別しつつ同一結果を返すことを示した。
- **AttributeWitness**: MIN/MAX の状態だけで非 NULL 値の相違個数 0・1・複数 を判定する
  規則を、極値と NULL 有無の場合分けで網羅した。
- **OccupiedAxis**: 各ブロックで出現した辞書コードのみをデコードし、別ブロックの同名
  ラベルを統合する軸の整列が完全一致であることを示した。
- **AxisRetraction**: 表示範囲縮小時、最終出現が除去されたラベルだけを落とす写像が、
  残すラベルの順序・内容を保存することを証明した。
- **TripleBitmap**: 各リテラルの局所コード集合をすべて引いた位置マスクの交差が、
  測定列読取り後に一致行集合と等しいことを示した。
- **SliceBounds**: 年・ブロックの層別件数から三次元フィルタの上下界を作り、列間の独立性を
  仮定せずに正しい包含を証明した。
- **CodeExtrema**: 選択コード集合から全 NULL・単一値を検出し、混在時のみ占有辞書値の
  MIN/MAX を使う規則を場合分けで示した。
- **GroupBeforeSuffix**: (league, position) で先に数えてから接尾辞 CASE を適用する順序
  交換が、最終ラベル別の件数袋を保存することを `countP` 系補題で証明した。
- **ClassificationCache**: メモ折りを `(cache, out, calls)` 状態の 4 分不変条件
  （出力＝復号済みラベル列・定義域＝処理済み前缀・キャッシュ値一致・
  `calls = 前缀.eraseDups.length`）として帰納し、ヒット時 +0・ミス時 +1 を
  `eraseDups` snoc 長補題で示して、`classify` の呼出し数比較を接続した。

## DuckDB ギャップの 50 件（証明済み）

[DuckDB との比較で遅かったクエリから登録した 50 件](duckdb-gaps.md)は、7 モジュールすべてで
`theorem <Name>_sound : <Name>.Goal` を証明した。反例はなく、提出型・既存証明は変更していない。

| 系統 | 証明モジュール | 主定理（50 件） |
| --- | --- | --- |
| Slices（7） | `ResearchProofs/AnalyticalOperators/DictionarySlicesAndExtrema.lean` | `sound_MinorityCodes`, `sound_AppendDictionary`, `sound_ComplementGroups`, `sound_PairwiseExtrema`, `sound_DominatedColumn`, `sound_WitnessedExtrema`, `sound_SparsePosting` |
| FileMasks（7） | `ResearchProofs/AnalyticalOperators/FilePruningAndDeleteMasks.lean` | `ReferencedFiles_sound`, `FileKeyAntiJoin_sound`, `SharedDeleteTable_sound`, `PartitionSpecialization_sound`, `IncrementalSurvivors_sound`, `LateEqualityKey_sound`, `ArithmeticFooter_sound` |
| Refresh（6） | `ResearchProofs/AnalyticalOperators/IncrementalRefreshAndFlush.lean` | `InsertSideState_sound`, `ChangedSymbols_sound`, `OverlapReplay_sound`, `LaneOrSpill_sound`, `VisibleFlush_sound`, `DirtyFlush_sound` |
| Retail（8） | `ResearchProofs/AnalyticalOperators/RollupsAndChannelSetCounts.lean` | `OrderedRollup_sound`, `AverageFence_sound`, `SuffixFlush_sound`, `ThresholdFanout_sound`, `PromoReturnCounts_sound`, `ShrinkingIntersect_sound`, `ExclusionErase_sound`, `NetPairSet_sound` |
| Hits（8） | `ResearchProofs/AnalyticalOperators/FrequencyBoundsAndOrderedFetch.lean` | `CancellationDebt_sound`, `CertifiedWinners_sound`, `OwnerDistinct_sound`, `OrderedCodeMin_sound`, `FrontCodedContains_sound`, `OrderedFetch_sound`, `DatePostings_sound`, `StridedPoint_sound` |
| Orders（7） | `ResearchProofs/AnalyticalOperators/RevenueAggregatesAndJoinWitnesses.lean` | `MarketShareWeights_sound`, `YearRoutes_sound`, `DirectionBuckets_sound`, `LateSlack_sound`, `RetireWitness_sound`, `RebucketOrders_sound`, `RevenueRuns_sound` |
| BridgeStates（7） | `ResearchProofs/AnalyticalOperators/SharedJoinStatesAndWeights.lean` | `RightStateClasses_sound`, `ShardedHybrid_sound`, `SourceWeights_sound`, `EndpointDemand_sound`, `CommentQuotient_sound`, `CollisionBucket_sound`, `CountryProfiles_sound` |

証明の要点：

- **FileMasks**: 一意ファイル結合の `countP = 1` 特徴づけ、`keysByFile` の
  `mem_flatMap`/`mem_eraseDups` による membership 同値、等差数列フッターの
  `List.sum_range'` と線形和補題。
- **Refresh**: `plus`/`summarize` の畳み込み代数、`replaceState` の非 NULL 群と空群の分離、
  ordinal 区間の重なり除去。`lane` の境界は `base ≤ sym < base+width` で場合分け。
- **Slices**: 辞書コードの `range` 分割による少数側の長さ上限、隣接ペア帰納による
  MAX/MIN 候補、`eraseDups` を経由した posting スキャンの置換性。
- **Retail**: ROLLUP の prefix run 帰納、`lowerRank` 二分探索の完全性、HashSet の
  erase による除外集合、`NetPairSet` の組単位 dedup。
- **Hits**: Misra–Gries の投票保存不変条件、非居住キーの頻度上界、ソート辞書の
  最小コード、front coding の境界横断、`seekHits` の最初の k ヒット不変条件。
- **Orders**: CASE ELSE 0 の非 NULL 寄与、方向ビットの二国対応、行ごとの
  paired slack の最大、`mergeBoundary` の snoc 帰納。
- **BridgeStates**: HashMap の再グループ化補題（`pfold_insert_add` / `regroup_cells`）、
  `refGroup` のシャード分割恒等式と `finish` の加法性。H5 の状態共有と
  radix 別戦略切替が同一結果を与えることを示した。

全モジュールは `TinylambOpt.lean` から import され、`cd lean && lake build`
（CheckProofs の `sorryAx` 検査込み）が成功する。

## 検証範囲の注記

- 提出型付き 6 件と `checked-aggregate-tree` は `ResearchGoals` の定義を変更せず、そのまま import して
  `sound` を構成している（仕様変更なし）。
- 初回 20 件の証明は独立した研究モデル上の命題であり、実 C++ 実装の全命令・全挙動を覆うものではない。
  実装接続の残課題は [AUDIT.md](../AUDIT.md) の「実装への適用境界」を参照。
- `lakefile.toml` の `relaxedAutoImplicit = false` は `lake build` 時のみ効く。個別ファイルを
  `lake env lean <file>` で検証すると typo が暗黙変数として許されるため、上の厳格オプション付き
  コマンド（または `lake build`）で最終確認すること。
