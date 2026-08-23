# page_checksum.md レビュー指摘事項

## サマリー
アルゴリズム選定(CRC-32C)・主要API名(`SetChecksum`/`IsValid`)・エラーメッセージ(`corrupt page checksum`)は実装と一致しており、方向性は正しい。ただし最大の問題として、ダイジェストの計算範囲が文書と実装で食い違う。実装は `recovery_lsn` の8バイトをダイジェストから**除外**しているが、文書は「全 `kPageSize` バイトを対象」と記載するため、文書通りに実装した者は互換性のないチェックサムを作ってしまう。また検証が `GetPageForRecovery` 経由では意図的にバイパスされること、全ゼロ領域の特殊扱いなど、ReadFrom の分岐が過少記述である。加えて、検証 API が排他アクセスを前提とする非アトミック実装であること(共有ラッチ下で競合し得る)、ソフトウェア bit-by-bit 実装の計測上無視できないコスト、の2点も文書化されていない。

## 指摘一覧

### C-1: ダイジェスト範囲 — `recovery_lsn` が除外されていることを文書が記載していない
- 区分: 実態との乖離
- 対象: docs/page_checksum.md:7-9「computed by `common/crc32c.hpp` over the full `kPageSize` bytes with the checksum field itself forced to zero during the digest」
- 問題: 実際のダイジェストはページ全体ではなく、`recovery_lsn`(オフセット16〜24)の8バイトを**スキップ**して計算される。文書通り「フルサイズ+checksumフィールドのみ0」と実装すると、本実装とは異なるオンディスク値になり相互運用できない。除外には理由(読み込み後に `recovery_lsn = kMaxLsn` に書き換わるため、これをハッシュに含めるとロード→ライトバックでチェックサムが壊れる)があり、それも文書化されていない。
- 根拠:
  - page/page.cpp:292-305 — `StoredChecksum` が `skip_begin = &p.recovery_lsn - base`、`kSkipWidth = sizeof(uint64_t)` として `[skip_begin + kSkipWidth, kPageSize)` を連結し、`recovery_lsn` の8バイトを計算対象から外す。コメント(293-295行)に "covers the whole page except runtime-only metadata ... and recovery_lsn" と明記。
  - page/page_pool.cpp:311-312 — 読み込み後に `target->recovery_lsn = std::numeric_limits<lsn_t>::max()` を設定("RecLSN = MAX means a clean page")。
  - 実測(本リポジトリのヘッダで確認): `offsetof(Page, recovery_lsn)=16`, `off(checksum)=32`, `sizeof(Page)=32768`。
- 提案: 「ダイジェスト対象は `recovery_lsn` の8バイトを除く領域。checksumスロット自体は0としてダイジェストに参加する。これは `PagePool::ReadFrom` がロード後に `recovery_lsn` を `kMaxLsn` に正規化しても検証が壊れないようにするため」と明記する。

### C-2: チェックサム検証は無条件ではない — リカバリ経路は意図的にバイパスされる
- 区分: 粒度不足
- 対象: docs/page_checksum.md:13-14「`PagePool::ReadFrom` verifies after a successful full-page read and throws `std::runtime_error` ... on mismatch」および :24-25「will fail to open once a page is read (`corrupt page checksum`)」
- 問題: 検証は `validate=true` のときのみ行われる。`GetPageForRecovery()` は `validate=false` で ReadFrom を呼び、破損画像をそのまま返す(RecoveryManager が Single Page Recovery に使う)。文書は検証を無条件と読める書き方で、「破損ページでもリカバリ経由なら起動できる」という重要な例外が欠落している。「fail to open once a page is read」も、実際は validate=true 経路(通常アクセス)で当該ページを読んだ時点で失敗するという条件付きの挙動。
- 根拠:
  - page/page_pool.hpp:76-82 — `GetPageForRecovery` のコメント「a corrupt on-disk image is returned verbatim instead of rejected. RecoveryManager needs the raw bytes to run Single Page Recovery」。
  - page/page_pool.cpp:73-75 — `GetPageImpl(page_id, cache_hit, false /*shared*/, false /*validate*/)`:301-308 — `validate` 偽の場合は破損画像をそのまま返す分岐。
  - recovery/recovery_manager.cpp:458-467 — コメント「this loop bypasses PagePool checksum enforcement」の上で `GetPageForRecovery` → `!page->IsValid()` なら SPR へ。
- 提案: 「通常の `GetPage` は不一致で throw。`GetPageForRecovery` は検証をスキップし破損画像を返す(RecoveryManager が Single Page Recovery で再構築)。よって起動時のメタページ(id=0, page_manager.cpp:56-65)以外の破損は、当該ページへの初回アクセスまで顕在化しない」ことを追記する。

### C-3: 全ゼロ領域・EINVAL・CRC==0 の境界条件が未記載
- 区分: 粒度不足
- 対象: docs/page_checksum.md:16-17「Short reads / past-EOF materialize an in-memory free page without a stored checksum」
- 問題: short read / past-EOF 以外にも free ページ化される経路が2つある。(1) フルリード成功だが `type == kUnknown && checksum == 0`(ファイル拡張で生じた全ゼロ領域)は未初期化として扱われ破損扱いしない。(2) pread が EINVAL(ページID×32KiB が off_t 範囲外)でも free ページ化。さらに逆に、正当なページでも CRC-32C が偶然 0 になった場合は (1) の特例に引っかからず破損として拒否される確率的境界がある(コードは「Real pages always carry a non-zero CRC」という仮定に依存)。いずれも実装者が誤解しうる挙動。
- 根拠:
  - page/page_pool.cpp:292-295 — short/past-EOF で `PageInit(pid, kFreePage)`:296-300 — 全ゼロ領域特例(`type == kUnknown && checksum == 0` → free ページ化、コメントに "Real pages always carry a non-zero CRC after WriteBack"):283-288 — EINVAL 時も `PageInit(pid, kFreePage)` + `recovery_lsn = max`。
- 提案: ReadFrom の4分岐(short read/EOF、EINVAL、全ゼロ領域、不一致)を表にして列挙し、「checksum==0 かつ type==kUnknown は新規拡張領域とみなす」「CRC が理論上 0 になるページは誤判定され得る(確率 2^-32)」ことを注記する。

### C-4: `uint64_t` フィールドに実効幅 32bit の CRC を格納している旨の注記がない
- 区分: 粒度不足
- 対象: docs/page_checksum.md:5「On-disk pages carry a `uint64_t checksum` field in the page header」
- 問題: フィールドは 64bit 幅だが格納されるのは CRC-32C の 32bit 値のみで、上位 32bit は常に 0。誤り訂正・検出強度を実装者が 64bit と誤認しうる。将来 xxHash64 等へ差し替える場合の拡張余地についても言及がない。
- 根拠:
  - page/page.hpp:173 — `mutable uint64_t checksum = 0;`
  - page/page.cpp:304 — `return static_cast<uint64_t>(crc ^ 0xffffffffu);`(`crc` は `uint32_t`)
  - common/crc32c.hpp:41-45 — Castagnoli 反射多項式 0x82F63B78 のソフトウェア実装、既知ベクトル `Crc32C("123456789") == 0xe3069283`(common/crc32c_test.cpp:27-30 でも検証)。
- 提案: 「フィールドは uint64_t だが現行フォーマットは下位 32bit に CRC-32C、上位 32bit は常に 0(将来の 64bit ハッシュ置換のための予約)」と明記する。

### C-5: WriteBack の呼び出し元とエラー時挙動・fsync 方針が未記載
- 区分: 粒度不足
- 対象: docs/page_checksum.md:12「`PagePool::WriteBack` always refreshes the checksum before I/O.」
- 問題: 正しいが粒度が粗い。WriteBack は (a) eviction 経路(pool_latch の外・file_latch_ 保持)、(b) `FlushPageForTest`、(c) デストラクタ(全常駐ページを書き戻し後 fdatasync/F_FULLFSYNC)から呼ばれ、pwrite 失敗・部分書き込み時は `std::runtime_error` を投げる。チェックサムの永続性(durability)を論じる文書として、どのタイミングでディスクに到達し、失敗時にどうなるかの記述があるべき。
- 根拠:
  - page/page_pool.cpp:260-275 — SetChecksum 後 pwrite、`written < 0` / 部分書き込みで throw:119-138 — eviction は DetachVictim 後 latch 解放して WriteBack:242-258 — デストラクタで全ページ WriteBack + SyncFile(fd_):58-62 — Linux=fdatasync / macOS=F_FULLFSYNC。
- 提案: 「checksum 更新は WriteBack(eviction/デストラクタ/テスト用明示フラッシュ)の先頭で必ず行われ、書き込み失敗・部分書き込みは runtime_error。デストラクタ時に fdatasync(macOS は F_FULLFSYNC)で永続化する」ことを追記する。

### C-6: `SetChecksum`/`IsValid` の並行性契約(排他前提)が文書化されておらず、共有ラッチ下で競合し得る
- 区分: 粒度不足
- 対象: docs/page_checksum.md:11-14(`SetChecksum`/`IsValid`/`WriteBack`/`ReadFrom` のライフサイクル記述全体)
- 問題: `IsValid()` は `mutable checksum` を一時的に 0 に書き換えて戻す非アトミック操作であり、ページ全体の読み出しも伴う。ところが `PageManager::GetPage` は **shared=true(共有ラッチ)** で取得した PageRef に対して `ref->IsValid()` を呼ぶため、同一ページを並行フルスキャンする2スレッドが同じチェックサムフィールドを同時に読み書きし得る(データ競合、torn 値による誤破壊検知)。また `WriteBack`(=`SetChecksum`)は eviction・デストラクタ経路で pin 残ページに対しても走り得る。「検証はどのラッチモードを前提とするか」という契約の記述が皆無で、共有ラッチ読み取りが安全であるかのように読める。
- 根拠:
  - page/page.cpp:313-318 — `IsValid()` 内で `checksum = 0` へ代入して復元。フィールド定義は page/page.hpp:173 `mutable uint64_t checksum = 0;`(非アトミック)。
  - page/page_manager.cpp:36-44 — `pool_.GetPage(page_id, &cache_hit, shared)` の後 `ref->IsValid()` を呼ぶ。shared=true 時は PageRef が共有ラッチを保持(page/page_ref.hpp:37-44)。
  - table/full_scan_iterator.cpp:50,70,79,151 / table/table.cpp:309 — `GetPage(pos_.page_id, /*shared=*/true)` の実呼び出し箇所。
  - page/CODE_REVIEW.md:24,98 — 同一競合の指摘(「共有ラッチ保持の読み取りスレッドと並行するとデータ競合」「~PagePool がピン残ページへ WriteBack」)。
- 提案: 「`SetChecksum`/`IsValid` はページ排他ラッチ保持を前提とする」ことを明記する。現行の `PageManager::GetPage(shared=true)` 経路(:39)は契約違反なので、修正するか既知問題として注記する。

### C-7: ソフトウェア bit-by-bit CRC の計算コストが無視できないのに文書に記載がない
- 区分: 粒度不足
- 対象: docs/page_checksum.md:32-37(Why CRC-32C)
- 問題: 「Hardware CRC32C can replace the software loop later」とある通り現行はソフトウェア実装だが、1バイト8ビットループ(~8サイクル/バイト)を 32KiB ページ毎回計算するため安価ではない。実際 BenchmarkHistory.md に「fresh CRC32C pages add ~2× scan time on hot scans (Q1 scan_ms 1739→3525)」という退行記録がある。文書だけ読むと「ほぼ無料で堅牢性を得られる」と誤解する。
- 根拠:
  - common/crc32c.hpp:28-38 — ビット毎ループのソフトウェア実装。
  - common/CODE_REVIEW.md:28 — 「約 8 サイクル/バイト。テーブル駆動または SSE4.2 `_mm_crc32_u64` への切替を検討」。
  - BenchmarkHistory.md:51-54 — 「fresh CRC32C pages add ~2× scan time on hot scans (Q1 scan_ms 1739→3525)」。
- 提案: 「現行はソフトウェア実装でホットスキャン約2×の退行が記録済み。テーブル駆動/SSE4.2 置換がフォローアップ」を Why CRC-32C 節に追記する。

## 未検証事項
- 「Regenerate fixtures(TPC-H `--reuse-database` は新規ロード後のみ等)」(docs/page_checksum.md:26-27)は運用手順の主張であり、ソースだけでは検証していない(benchmark/tpch_benchmark.cpp:164-165 に該当フラグ自体は存在)。
- 「旧 `std::hash` 形式の `.db` が必ず open 時に失敗する」(:24-25)は、旧形式の実際の格納値に依存するため現ツリーからは断言できない(メタページが validate=true でロードされるため失敗する見込みは妥当)。なお `std::hash<Page>` 特殊化自体は page/page.hpp:203-207 / page.cpp:577-596 に残存しており、永続化には使用されていない。
