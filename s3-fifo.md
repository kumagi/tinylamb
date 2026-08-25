# S3-FIFO 段階的移行計画

バッファプール（`page/page_pool.cpp`、LRU＋グローバルラッチ）を mmap 安定VA空間＋S3-FIFO の
VM Cache（`common/vm_cache_impl.cpp`）へ収束させるための作業リスト。

設計方針の要約:

- Pointer Swizzling は実装しない。ページNが常に `base + N * kPageSize` に存在する世界では
  swizzling はポインタ演算に帰着し、管理ビットが不要になる（Umbra 方式）。
- 現行ベンチはRAM内に収まるため eviction policy の差は誤差。優先度はヒット経路の定数倍と
  並行性の除去が先。ポリシーが効くのは RSS > RAM の大規模SFから。
- アルゴリズムは S3-FIFO を維持（TPC-Cのホットセット × TPC-Hの大スキャンという
  scan-resistance 要求に合致）。課題は政策ではなく実装品質（シャーディング・境界付き操作）。

## Stage 0: 二重実装の統合と既知バグ修正

- [x] `vm_cache_impl.cpp` の `EnqueueToSmallFifo`/`EnqueueToMainFifo` の全pin時無限回転を
      境界付きスキャンに置換（`cache.cpp` の `scanned_locked` 方式を移植、2026-08-22）
- [x] 小キュー満杯判定を `==` → `>=` に変更（一時オーバーフロー後も eviction が再開する自己修復。
      `cache.cpp` 側にはこの潜在バグが残っている、2026-08-22）
- [ ] `index/lsm_detail/cache.cpp` と `common/vm_cache_impl.cpp` を単一実装に統合する
      - [ ] ブロックサイズをテンプレート／コンストラクタ引数化（4KiB vs 可変）
      - [ ] `cache.cpp` の zero-copy `ReadAt(offset, length, string_view&)` + RAII `Locks`
            API を共通版へ取り込む
      - [ ] `cache_fuzzer_replay` 系テストが CMakeLists.txt:498-520 で無効化されているので、
            統合後に再有効化または削除する
- [ ] ghost FIFO を deque ポインタ列（8B/entry）からコンパクトなリングバッファに圧縮する

## Stage 1: PagePool 内部刷新（PageRef API 変更なし）

- [x] eviction書き戻し vs 同一pid同時miss読み取りの stale-read レースを修正
      （`flushing_` セット導入、page_pool.cpp 参照、2026-08-22）
      - [ ] 検証: 10クライアントTPC-C で `invalid page type` 零を複数シードで確認済み（3回連続ゼロ）
      - [ ] 要確認: 1クライアント実行で tps 16 / `payment customer-name read returned no rows`
            を一度観測（履歴ベースライン tps 821）。負荷残留の可能性があるため再測が必要
- [ ] `std::fstream` を廃止し `pread`/`pwrite` に置換（将来の O_DIRECT / io_uring への布石）
- [ ] dirty ビット導入: クリーンページの書き戻しを省略（現状は全evictionが32KiB無条件ライト）
- [ ] `pin_count` を atomic 化し、ヒット経路からグローバル `pool_latch` を排除する
      - [ ] pid→エントリの open-addressing 表（キャッシュフレンドリな配置）
      - [ ] `Touch()` の list erase+push_back をコア間 ping-pong なしの構造へ
- [ ] Entry ごとの heap 確保 `std::shared_mutex` をインラインラッチに置換
      （過去のUAF修復跡 page_pool.cpp:63-67 のイテレータ再解決が不要になる）
- [ ] バックグラウンドライタ: eviction同期書き戻しから分離
      - [ ] WAL-before-data 順序: eviction時に `page_lsn` までログをフラッシュしてから pwrite
            （`rec_lsn = MAX` はクリーンの規約は既存、page_pool.cpp ReadFrom 参照）
      - [ ] `CheckpointManager` / `RecoveryManager` は friend 済みなのでフック追加のみ

## Stage 2: 楽観的バージョン検証（HyPer/LeanStore方式）

- [ ] ページ状態語にバージョンカウンタを追加（seqlock 方式の before/after 検証）
- [ ] `OptimisticGuard`: 読み取り専用降下での shared_mutex 取得を撤去
      - [ ] `BPlusTree::FindLeafReadOnly`（b_plus_tree.cpp:183）
      - [ ] `FullScanIterator` / `IndexScanIterator` / index-only scan
- [ ] 書き込みと構造変更操作（SMO: split/foster吸収）は悲観的排他を維持
- [ ] ラッチカップリング（親shared保持で子取得、b_plus_tree.cpp:198-212）を読み取り経路から除去
- [ ] 検証: b_plus_tree_concurrent_test を実ワークロード規模に拡充
      （現状 InsertInsert 1ケースのみ・11ms）

## Stage 3: mmap VA空間モデルへの完全移行

- [ ] ファイル全体を `MAP_NORESERVE` 匿名mmapし `Page* == base + pid * kPageSize` を恒久化
- [ ] Activate = pread、Release = `madvise(MADV_DONTNEED)`（現VMCacheImplと同一）
- [ ] `pool_lru_` / `pool_` マップを削除（pid→ポインタ変換がシフト演算になる）
- [ ] S3-FIFO キューのシャーディング（pidハッシュ分割、queue_lock_ の分散）
- [ ] LSM側（SortedRun の VMCache<Entry>）とバッファプールが同じ基盤を共有
- [ ] メモリ予算配分: row/leaf/branch ページプールと LSM インデックスキャッシュの制御

## Stage 4: 発展（任意）

- [ ] io_uring によるスキャン先読み（full scan / index range scan の非同期prefetch）
- [ ] zero-copy アクセサを executor まで通す（ピン中ページへの `string_view`。
      TPC-H スキャンフロア ~12s は decode 支配のため、こちらが効率的に効く）
- [ ] NUMA 対応（マルチソケット環境が出てきたら）

## 検証方針（各ステージ共通）

- 単体: `vm_cache_test`(40), `cache_test`(39), `cache_concurrent_test`(2),
  `lsm_tree_test`(28), `page_pool_test`(24) を緑維持
- 並行ストレス: `tinylamb_tpcc_benchmark --clients 10 --seconds 10` を複数シードで回し
  `first_error` 非出力を確認（2026-08-22 時点: 3シード連続でエラー0・tps 10〜11.3）
- 性能基準線: 1クライアント TPC-C tps≈821 / sql_qps≈27466（BenchmarkHistory.md 2026-08-20）
- 各ステージ完了時に BenchmarkHistory.md へ計測行を追記する
