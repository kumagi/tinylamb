# `recovery/` — Layer 3 storage: ARIES WAL + checkpoint + recovery

Write-ahead log, group commit, checkpoints, crash recovery. Normative docs:
`docs/wal_format.md`, `docs/recovery_invariants.md`.

## Key files

- `logger.{hpp,cpp}` — group commit: 8 MiB buffer + 1 ms worker thread
  (`pwrite`+`fdatasync`). Three-LSN split is the core idea:
  BufferedLSN / CommittedLSN (written) / DurableLSN (fsynced).
  **`AddLog`'s return LSN alone guarantees nothing — callers must
  `WaitForDurable`.** `kMaxRecordSize` (16 MiB−64 KiB) enforced;
  ENOSPC/EIO handling + page-cache advice. Wired to `PagePool` via
  `SetDurabilityGate`.
- `log_record.{hpp,cpp}` — ~30 `LogType`s (`kBegin`, `kInsertRow|Leaf|Branch`,
  `kUpdate*`, `kDelete*`, fence/foster ops, matching `kCompensate*`,
  `kCommit`, `kBegin|EndCheckpoint`, `kSystemAlloc|DestroyPage`). On-disk
  version 3 (trailing CRC32C; readers accept older, writers emit current).
  `{prev_lsn, txn, pid, slot/key, redo, undo}`; per-txn backward chain is the
  abort/undo path. `slot`/`page_id = max()` is the invalid sentinel.
- `recovery_manager.{hpp,cpp}` — `RecoverFrom(checkpoint_lsn)`: ARIES
  Analysis (active txns + dirty pages) → REDO (repeating history) →
  `UndoLoserChains`. `UndoneRecorder` prevents double-undo under parallel
  per-page replay; CRC mismatch at tail = torn tail (`ValidLogEnd`);
  truncation only with `--force` (`SetTornTailTruncationAllowed`).
- `checkpoint_manager.{hpp,cpp}` — 5 s worker: ATT snapshot →
  CHECKPOINT_BEGIN → dirty flush → END with `ActiveTransactionEntry{txn_id,
  status, last_lsn}`; txn status snapshotted via relaxed atomics, lock-free.

## Before reading code here

LSN = byte offset (`lsn_t`). `page_lsn`/`recovery_lsn` on pages vs WAL LSNs
are the same numbering. Meta-page state is WAL-exempt — startup rebuild is
mandatory, not optional.

Test: `./build/logger_test`, `log_record_test`, `recovery_manager_test`,
`checkpoint_manager_test`, `recovery_extra_test`.
Fuzz: `recovery/*_fuzzer.cpp` (+ `_replay`, needs `TINYLAMB_ENABLE_FUZZ`).
