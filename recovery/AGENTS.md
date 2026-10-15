# `recovery/` — Layer 3 storage: ARIES WAL + checkpoint + recovery

Write-ahead log, group commit, checkpoints, crash recovery. Normative docs:
`docs/wal_format.md`, `docs/recovery_invariants.md`.

## Key files

- `logger.{hpp,cpp}` — group commit: 8 MiB buffer + 1 ms worker thread
  (`write` on an `O_APPEND` fd + `fdatasync`), over a segmented WAL:
  `<db>.log.<index>` files of `kDefaultLogSegmentSize` (16 MiB;
  `TINYLAMB_LOG_SEGMENT_BYTES` overrides), `lsn = index * size + offset`.
  Three-LSN split is the core idea: BufferedLSN / CommittedLSN(written) /
  DurableLSN(fsynced). **`AddLog`'s return LSN alone guarantees nothing —
  callers must `WaitForDurable`; after `Finish`, an unreached durability
  target fails fast.** `kMaxRecordSize` (16 MiB−64 KiB) enforced; ENOSPC/EIO
  handling + page-cache advice. Rolls fsync the old segment before close.
  Wired to `PagePool` via `SetDurabilityGate`; recycling via `RecycleUpTo`
  (+ `SetRecycleBoundaryValidator`) — the recycle scan starts from the
  tracked `lowest_segment_` cursor, so checkpoint cost stays proportional to
  the live segment window, never to the total history ever recycled away.
  Reopen fsyncs the existing current segment before seeding `DurableLSN`; a
  file-size seed is not itself a durability proof. `TryCopyBuffered(lsn, dst,
  len)` serves the volatile tail `[max(ring_floor_lsn_, buffered - capacity),
  buffered)` from the ring without blocking (`ring_floor_lsn_` is seeded to
  the open-time WAL end, so a reopened database's zeroed ring never stands in
  for file bytes); `RecoveryManager` holds an optional `SetLogger` link and
  reads that tail first, which is what lets a runtime undo walk run without a
  pre-undo durability wait. A producer parking on a full ring notifies the
  worker and the idle predicate counts full-buffer waiters — occupancy
  predicates cannot see a FULL ring (every byte written, none drained).
- `log_record.{hpp,cpp}` — ~39 `LogType`s (`kBegin`, `kInsertRow|Leaf|Branch`,
  `kUpdate*`, `kDelete*`, fence/foster ops, matching `kCompensate*`,
  `kCommit`, `kBegin|EndCheckpoint`, `kSystemAlloc|DestroyPage`,
  `kFullPageImage`). On-disk version 6 (trailing CRC32C; readers accept
  v1-v5, writers emit current). `{prev_lsn, txn, pid, slot/key, redo, undo}`;
  per-txn backward chain is the abort/undo path. `slot`/`page_id = max()` is
  the invalid sentinel.
- `recovery_manager.{hpp,cpp}` — `RecoverFrom(checkpoint_lsn)`: ARIES
  Analysis (active txns + dirty pages) → REDO (repeating history) →
  `UndoLoserChains`. `UndoneRecorder` prevents double-undo under parallel
  per-page replay; CRC mismatch at tail = torn tail (`ValidLogEnd`);
  truncation only with `--force` (`SetTornTailTruncationAllowed`), via
  `Logger::TruncateTo` (resizes the tail segment, drops segments above).
  Segment-aware reads (`ReadAt` straddles boundaries); `LogStart()` is the
  retained start after recycling — never read below it. Every scan phase
  (torn-tail / analysis / redo collection / torn-page sweep) emits
  rate-limited progress lines, the final torn-page sweep classifies pages in
  parallel (`TINYLAMB_RECOVERY_WORKERS` caps it), and recovery ends with a
  one-line summary (WAL window, committed/loser counts, rebuilt pages).
  **REDO is streamed in bounded batches** (~64 MiB / 16k records): the
  analysis pass finishes first, so the committed set is complete before the
  first batch replays, and `PageReplay` is idempotent per page (page_lsn
  guards), so a page spanning batches is simply replayed once per batch — a
  restart's memory profile is bounded by the batch, not by the WAL window (a
  checkpoint that kept failing for hours no longer replays its window into
  one in-memory map). Broken (torn) pages share one WAL pass
  (`RecoverBrokenPages`, ~5,400 pages in 8 s vs 2.3 s PER page for the
  per-page scans it replaced) that trims to each page's last
  re-initialization record (alloc/destroy/FPW — recycled pids mix older
  incarnations into the window), and the streaming REDO filters records by
  each page's DPT recLSN (a destroyed page's zeroed image defeats the
  page_lsn guard). Stale-incarnation CLRs (`IsCompensation`) skip their
  physical half on replay instead of failing the open; the loser undo's
  re-inserts skip (loud WARN, CLR kept) on a full page rather than wedge
  the reopen forever.
- `checkpoint_manager.{hpp,cpp}` — 5 s worker: fuzzy checkpoint + bounded
  dirty-page flush — pin resident entries → per-page `RecoveryLSN` into the
  DPT → ATT snapshot → **flush stale dirty pages oldest-recLSN-first within
  `TINYLAMB_CHECKPOINT_FLUSH_MS` (default 1000, 0 disables)** → re-snapshot
  the DPT (flushed pages are clean and drop out) → CHECKPOINT_BEGIN/END (DPT
  + `ActiveTransactionEntry{txn_id, status, last_lsn}`) → fsync → master
  record → **fdatasync the data file → recycle WAL segments below
  `min(begin_lsn, min DPT recLSN, oldest active txn first LSN, oldest
  unsynced full-page image)`**. **No TM pause**; redo starts from the DPT's
  per-page `RecoveryLSN`. The flush pass is what keeps the recycle floor
  advancing under sustained load: a purely fuzzy checkpoint let a
  continuously rewritten hot page keep its setup-era recLSN forever and
  pinned the floor (WAL grew at the full write rate). The meta page is
  excluded from the DPT but is synchronously flushed after
  `kBeginCheckpoint`, outside the bounded dirty-page budget; failure stops
  the checkpoint before master publication or WAL recycling. `SyncDataFile`
  retires only images whose pwrite completed before the sync began; emitting,
  in-flight, and failed writes remain pending until a later successful pwrite
  and sync. The "oldest unsynced full-page image" term of the floor is the
  record's LAST byte (`snapshot_end - 1`): `RecycleUpTo` drops a segment as
  soon as `(index + 1) * segment_size <= floor`, so the exclusive end would
  unlink an FPW finishing exactly on a segment boundary. Retained WAL above
  `TINYLAMB_WAL_RETENTION_WARN_BYTES` (default 1 GiB) logs a rate-limited
  warning naming the pin — the concrete floor term (oldest active txn, oldest
  dirty page, or unsynced full-page image) with its id, not just the floor.
  Txn status snapshotted via relaxed atomics, lock-free.
  **`PageStorage::Create` runs one end-of-recovery checkpoint after a
  successful `RecoverFrom` (skipped when the WAL is empty)**: the replayed
  state is anchored by a fresh master record at open time, so a crash right
  after a long recovery re-pays only the post-checkpoint WAL and recycling
  can start immediately; failure is non-fatal (the periodic worker retries;
  `TINYLAMB_DISABLE_EOR_CHECKPOINT=1` opts out for A/B tests).

## Before reading code here

LSN = byte offset into a *virtual* stream (absolute; never renumbered).
`page_lsn`/`recovery_lsn` on pages vs WAL LSNs are the same numbering.
**Allocator invariant restored by `RecoverFrom`: every free-list member must
be ≤ `max_page_count`.** A fresh allocation issues `max+1`, so a free page
above the high-water lets that id be double-issued; the rebuilt list and the
preserved free chain are folded into `required_allocator_max` before
`RestoreMaxPageCount`. The fresh-loser-allocation undo therefore no longer
leaves max BELOW the freed page — the id stays immediately reusable through
the free list instead.
v6 allocation records snapshot allocator fields in a fixed 16-byte payload and
use exact kind-specific undo payloads; `kCompensateAllocPage` restores them
without serializing in-memory page structs. The startup page-range scan remains
the legacy/free-list repair authority.
Torn data-file pages rebuild from retained `kFullPageImage` records
emitted by `PagePool::WriteBack` (FPW); a page whose image is
checkpoint-fsynced and whose snapshot was recycled is beyond WAL repair
(backups territory). Recycling rules: `docs/wal_format.md`.
Torn-page detection is a checksum sweep over the whole allocated range
(`1..scan_high_water`), NOT dirty-page-table membership: a page the checkpoint
already flushed drops out of the DPT and would otherwise reach readers as
broken -- `BPlusTree::Open` then bootstraps an empty tree and the catalog is
silently lost. `RecoverFrom` rebuilds every non-free page whose stored CRC does
not match its body and fails the open when the retained WAL cannot rebuild it.

Test: `./build/logger_test`, `log_record_test`, `recovery_manager_test`,
`checkpoint_manager_test`, `recovery_extra_test` (segmentation, recycling,
FPW torn-page rebuild, legacy migration).
Fuzz: `recovery/*_fuzzer.cpp` (+ `_replay`, needs `TINYLAMB_ENABLE_FUZZ`).
