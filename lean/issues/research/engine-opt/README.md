# 起動ロード・融合実行・Bytecode・キャッシュの未証明課題（20 件）

**更新: 以下 20 件はすべて `theorem sound : Goal` を証明済み**（登録は [../RESULTS.md](../RESULTS.md) の第四波の項）。

TPC-DS / TPC-H / ClickBench の最適化実装、TLCS v3 永続起動ロード改善・列単位遅延ブロックキャッシュ、および実測中に発見したバグ再発防止から抽出した 20 件の未証明命題です。
各課題は `TinylambOpt.ResearchGoals.<Name>.Goal` を提出型として定義しています。証明は別モジュールでそのファイルを import し、`theorem sound : TinylambOpt.ResearchGoals.<Name>.Goal` として提出してください。

## 1. 起動ロード・TLCS v3・列単位遅延ブロックキャッシュ（5 件）

- [世代番号付き列単位ブロックキャッシュの遅延無効化と再読込回避](lazy-block-cache-reuse.md) (`LazyBlockCacheReuse`)
- [ZoneMap ヘッダ判定＋キーデコード＋一致ブロック限定 payload 遅延デコード](block-lazy-zone-prune.md) (`BlockLazyZonePrune`)
- [TLCS v3 フッタ限定オープンとオンデマンド列スライス読取りの等価性](footer-only-open-equiv.md) (`FooterOnlyOpenEquiv`)
- [pin 保護付き LRU 退避による列キャッシュ解放の上界・順序保証](column-mmap-evict-epoch.md) (`ColumnMmapEvictEpoch`)
- [WAL チェックポイント・TLCS 境界と MVCC 書込み意図順序での復旧同値性](wal-checkpoint-tlcs-boundary.md) (`WalCheckpointTlcsBoundary`)

## 2. TPC-DS / TPC-H 融合結合・多キープローブ・単調ファクト（6 件）

- [2D ペアビットマップの密外側キーリマップ＋軸反転の完全一致](pair-bitmap-dense-remap.md) (`PairBitmapDenseRemap`)
- [オフセット加算済み 2x32 ビットキー＋2 プローブ Bloom 等値結合の無偽陰性](two-probe-packed-bloom.md) (`TwoProbePackedBloom`)
- [キー単調圧縮ファクトの二分探索区間切り出し](mono-contiguous-fact-slice.md) (`MonoContiguousFactSlice`)
- [`packed_2x32` ワーカー局所集約＋NULL・範囲外フォールバック統合](packed-radix-2x32-batch-agg.md) (`PackedRadix2x32BatchAgg`)
- [`left_pack` 小辞書圧縮 ID リマップでの NULL・未参照センチネル分離](gathered-small-dict-fast-path.md) (`GatheredSmallDictFastPath`)
- [ブロック絶対値上界和による無検査並列 INT64 SUM ＆定数ブロック O(1) 畳込み](exact-int-total-overflow-guard.md) (`ExactIntTotalOverflowGuard`)

## 3. ClickBench 100M 列指向・集約・文字列/Top-N 最適化（5 件）

- [2 段基数パーティション並列集約 `(count, sum)` 統合の重複排除・同値性](two-level-partitioned-agg-merge.md) (`TwoLevelPartitionedAggMerge`)
- [ソート済み辞書の `LIKE 'prefix%'` 単調区間・末尾 `0xFF` 繰り上がり変換](dict-prefix-monotonic-range.md) (`DictPrefixMonotonicRange`)
- [ワーカー局所 Top-(offset+limit) 2 段並列マージの正当性](bounded-heap-topn-merge.md) (`BoundedHeapTopNMerge`)
- [負開始・0 開始・境界外を含む ASCII `SUBSTR` O(1) バイトスライス等価性](substr-ascii-byte-slice-equiv.md) (`SubstrAsciiByteSliceEquiv`)
- [適応 4 状態 `COUNT(DISTINCT)`（empty/single/small/large）の状態不変量・NULL 除外](count-distinct-adaptive-switch.md) (`CountDistinctAdaptiveSwitch`)

## 4. オプティマイザ・Bytecode CSE・プランキャッシュ・Limit 合成（4 件）

- [Bytecode 命令列だけでなく側表・型を揃えた集約スロット重複排除](bytecode-side-table-equiv.md) (`BytecodeSideTableEquiv`)
- [`prior_offset` を伴う `Limit`/`TopN` 前置スキップ合成と射影可換性](limit-offset-prefix-composition.md) (`LimitOffsetPrefixComposition`)
- [最適化トグルビットをキーに含むプランキャッシュ無効化の完全再現性](plan-cache-toggle-invalidation.md) (`PlanCacheToggleInvalidation`)
- [共有 CTE 多分岐 3 値フィルタの 1 パス並列集約還元](cte-single-pass-split-filter.md) (`CteSinglePassSplitFilter`)
