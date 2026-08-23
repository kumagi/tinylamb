# page_pool_sharding.md レビュー指摘事項

## サマリー

記載された方式の骨格(ヒットパスの `shared_lock` + atomic `pin_count++`、`try_lock` によるベストエフォート `Touch`、ミス/eviction の unique lock、`pread`/`pwrite` 化)は page_pool.cpp の現行実装と一致し、improvement.md §4.2 option (c) / M3.x への参照も正確である。一方、題名に「concurrency」と掲げながら重要な直列化点が欠落している:(1) 全 PageRef 解放が `Unpin` 経由で exclusive `pool_latch` を取る、(2) `pread`/`pwrite` が単一の `file_latch_` で全局直列化される、(3) flushing 待機が yield スピンである。さらに M3.3 の「load under exclusive」は現行コード(ラッチを一切保持せずロード)と乖離しており、s3-fifo.md の移行計画(mmap 安定VA + S3-FIFO)との関係も未整理のまま残置されている。

## 指摘一覧

### S-1: Unpin が全解放経路で exclusive pool_latch を要求する直列化点が未記載
- 区分: 粒度不足
- 対象: docs/page_pool_sharding.md:11-16「**Hit path uses `std::shared_mutex` shared lock**」および :17-18「unblocks read scaling without a full shard redesign」
- 問題: GetPage 1 回ごとに対応する Unpin が存在し、それは **unique_lock(pool_latch)** を取る。つまり読み取りスレッドは pin(共有ラッチ下)→unpin(排他ラッチ)で必ずグローバルラッチに再接触し、「ヒットパスは共有ロックで走る」記述だけだと読み取りスケールの上限構造が見えない。実測でも 16 clients で ~893 qps と頭打ちである。並行性設計を文書化する以上、この対称性(pin=共有/unpin=排他)は必須情報。
- 根拠:
  - page/page_ref.cpp:28-38 — `PageRef::PageUnlock`(デストラクタから呼ばれる)が常時 `pool_->Unpin(...)` を呼ぶ。
  - page/page_pool.cpp:204-214 — `void PagePool::Unpin(...) { std::unique_lock latch(pool_latch); ... }`(排他ロック)。
  - BenchmarkHistory.md:76-78 — 「plateaus near 8 workers (~893 qps at 16 clients)」。
- 提案: 「pin は共有ラッチ+atomic 増分だが、unpin は exclusive pool_latch を必要とする。これが残る直列化点であり、shard 化または atomic 参照カウント管理が次の改善候補」と明記する。

### S-2: ミスパス記述「load under exclusive, then return shared page latch (done in M3.3)」は現行実装と乖離
- 区分: 実態との乖離
- 対象: docs/page_pool_sharding.md:24-25「**Miss path**: load under exclusive, then return shared page latch when `shared=true` was requested (done in M3.3).」
- 問題: 現行コードはミス時にページラッチを一切保持せず I/O を行う。pool_latch を解放した後、未 install の新 Page を file_latch_ の下で ReadFrom し、その後 pool_ に install してから要求されたモードの PageRef を返す。「exclusive でロード→shared へ降格」という M3.3 時点の方式は既に過去のものであり、どちらの解釈(pool_latch か page_latch か)でも「under exclusive」は現行と一致しない。
- 根拠:
  - page/page_pool.cpp:147-149 — 新 Page 生成後 `latch.unlock()` してからロードへ。
  - page/page_pool.cpp:161-164 — `ReadFrom` は `file_latch_` のみ保持(page_latch は未獲得)。:182-183 に「Page content was loaded without holding page_latch」と明記。
  - page/page_pool.cpp:176-184 — install 後に `{this, raw_page, raw_latch, shared}` を返す(PageRef ctor が要求モードのラッチを取得、page/page_ref.hpp:37-44)。
  - improvement.md:365 — M3.3 当時は「ロード後ダウングレードする形」だった(履歴としての記述は誤りではないが、現状説明としては陳腐)。
- 提案: 「現行: ミス時は未 install ページに対しラッチ無しでロード(file_latch_ + flushing_ プロトコルで保護)、install 後に要求モードのラッチで返す。二重ロードは排他区間での recheck(:166)で回収」と現行方式に書き換える。

### S-3: file_latch_ による I/O 全局直列化と flushing_ 待機プロトコルが未記載
- 区分: 粒度不足
- 対象: docs/page_pool_sharding.md 全体(特に :20-31 Follow-ups)
- 問題: (a) すべての pread/pwrite は単一の `file_latch_` mutex の下で実行され、キャッシュミス時の I/O はスレッド数に関係なく直列化する。(b) eviction 書き戻し中の同一 pid ミスは `flushing_` セットへの登録を `std::this_thread::yield()` スピンで待機する。(c) eviction は DetachVictim 後 pool_latch を解放して書き戻す(構造操作と I/O の分離)。(d) `GetPageForRecovery` が validate=false で検証を回避しつつ常に exclusive ラッチを返す。この文書が扱うべき並行性の核心要素がいずれも記載されず、読者は「ミスパスも含めてスケールする」と誤解しかねない。
- 根拠:
  - page/page_pool.hpp:142(`flushing_`)、:146-148(`file_latch_` コメント「Serializes pread/pwrite; held independently of pool_latch」)、:79-82(GetPageForRecovery コメント)。
  - page/page_pool.cpp:124-137 — DetachVictim → `flushing_.insert` → latch 解放 → `scoped_lock file(file_latch_)` WriteBack。
  - page/page_pool.cpp:150-160 — `flushing_.contains(page_id)` の間 `std::this_thread::yield()` スピン。
  - page/page_pool.cpp:161-163 — ReadFrom も `file_latch_` 下。:73-75 — GetPageForRecovery は `(page_id, cache_hit, false /*shared*/, false /*validate*/)`.
- 提案: ロック階層(pool_latch / file_latch_ / page_latch)と各取得箇所の一節を追加し、miss I/O は file_latch_ で直列化されること、eviction と同一 pid ミスの順序保証が flushing_ + スピン待機であることを記載する。

### S-4: 「Sharding (`page_id % N` pools)」フォローアップと s3-fifo.md 移行計画の関係が未整理
- 区分: 不明瞭
- 対象: docs/page_pool_sharding.md:22-23「**Sharding** (`page_id % N` pools) if shared_mutex still contends on the map/list structure.」
- 問題: リポジトリ直下の s3-fifo.md は PagePool を mmap 安定VA空間 + S3-FIFO VMCache へ収束させる計画(Stage 3 で pid→ポインタ変換の map 自体を削除、キューのシャーディング)を描いており、本文書の「PagePool を page_id%N でシャードする」フォローアップは方向性として競合/包含される。どちらが現行方針で、この文書の follow-up がまだ有効なのか読み取れない。加えて s3-fifo.md Stage 1 の「pin_count を atomic 化し、ヒット経路からグローバル pool_latch を排除する」は未完了チェック([ ])だが、実際には本実装で達成済み(page_pool.hpp:47 atomic pin_count、:82 shared_lock hit path)であり、両文書の相互参照は現在陳腐化している。
- 根拠:
  - s3-fifo.md:3-4(収束先は VMCache)、:61-66(Stage 3: mmap モデル、`pool_lru_`/`pool_` 削除、S3-FIFO キューのシャーディング)。
  - s3-fifo.md:38「- [ ] `pin_count` を atomic 化し...」 vs page/page_pool.hpp:47 `std::atomic<uint32_t> pin_count{0};` + page_pool.cpp:82-101。
  - improvement.md:91 — §4.2 の選択肢 (a) シャード/(b) lock-free/(c) shared_mutex 読取り。採用は (c)(本文書 :17 の主張通り)。
- 提案: フォローアップ節に「単純 shard 化は s3-fifo.md Stage 3(mmap+S3-FIFO)へ統合・繰り込み済みの方針」等の位置づけを一行追記し、s3-fifo.md 側の完了項目も同期することを促す。

### S-5: Measurement 節が履行済みで陳腐化しており、before データが不存在という事実も未反映
- 区分: 粒度不足
- 対象: docs/page_pool_sharding.md:33-36「Record `pg_read_benchmark` ... before/after in `BenchmarkHistory.md` when that binary is next run.」
- 問題: 計測は既に実施・記録済み(improvement.md M3.5 [x])。本節は「次回実行時に記録せよ」という未履行の指示のように読めるが、実際は BenchmarkHistory.md 2026-08-22 に qps 125→242→465→852→893(clients 1/2/4/8/16)の結果表がある。また同記録には「No pre-M3 baseline captured; treat as post-Phase-3a reference」とある通り、文書が想定した before/after 比較はそもそも成立していない。指示文のまま残すと進捗判断を誤る。
- 根拠:
  - improvement.md:367(M3.5 [x])、:421(2026-08-22 計測行)。
  - BenchmarkHistory.md:59-79(pg_read_benchmark の結果表と plateau の注記)。
  - benchmark/pg_read_benchmark.cpp / CMakeLists.txt:494-497 — バイナリ自体は存在。
- 提案: 本節を実績参照(「M3.5 分として BenchmarkHistory.md 2026-08-22 に記録済み。before 比較は未取得」)に置き換える。

## 未検証事項

- :7「made Relational morsel parallelism hang / regress」は問題発生当時の挙動の主張であり、現ツリーからは検証できない(改善後の配線は scan_filter.cpp:282 `TryParallelTableScan` と improvement.md:422 で確認)。
- :27-28「Re-enable Relational morsel parallel fill only after `ConcurrentEvictionAcrossThreads` is fixed/green (enabled in M3.6)」について、テスト自体は無効化接頭辞なしで存在し改善ログにも有効化記録があるが(page_pool_test.cpp:528、improvement.md:422)、テスト直上のコメント(:523-527)は依然「Disabled because it deterministically aborts」と述べており、コメント・テスト名・ドキュメント間の整合までは確認できていない(flushing_ 修正後のコメント更新漏れの疑い)。
