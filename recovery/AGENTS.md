# `recovery/` — Layer 3 storage: Logical WAL + Fuzzy Checkpoint + Fuzzy Recovery

Logical write-ahead log, group commit, lock-free fuzzy checkpoints, and
commit-filtered fuzzy crash recovery.

## Key files

- `logger.{hpp,cpp}` — group commit: 8 MiB buffer + a flush worker thread over
  a segmented WAL (`<db>.log.<index>`). 1 ms is only the initial `sync_interval_`
  (the tight group-commit cadence); when idle the worker doubles its sleep up
  to `kIdleWaitCap` (100 ms), so 1 ms is not the steady-state worker period.
  Synchronous commits always notify the worker, so their durability latency is
  unaffected. Three-LSN split: `BufferedLSN` / `CommittedLSN` / `DurableLSN`.
- `log_record.{hpp,cpp}` — compact logical WAL records (`kBegin`, `kCommit`,
  `kAbort`, `kBeginCheckpoint`, `kEndCheckpoint`, `kInsertRow`, `kUpdateRow`,
  `kDeleteRow`, `kCompensateUpdate`, `kSetRowPageNext`, `kSetRowPagePrev`,
  `kSystemAllocPage`, `kSystemDestroyPage`, `kLsmPut`, `kLsmDelete`,
  `kLsmDestroy`) protected by trailing CRC32C.
- `recovery_manager.{hpp,cpp}` — `RecoverFrom(checkpoint_lsn)`: loads the
  latest fuzzy checkpoint via `PageManager::LoadFuzzyCheckpoint` and runs a
  2-pass WAL scan (using a memory-bounded `CompactTxnBitmap` in Pass 1) from
  `max(anchor_lsn, log_start)`. `anchor_lsn` starts as `checkpoint_lsn` and is
  narrowed using the checkpoint's replay LSN only when a checkpoint was
  actually loaded (`ckpt_loaded`): then `anchor_lsn = checkpoint_lsn == 0 ?
  ckpt_replay_lsn : min(checkpoint_lsn, ckpt_replay_lsn)` (`ckpt_replay_lsn` is
  the replay LSN loaded from the checkpoint). When no checkpoint was loaded
  (`ckpt_loaded == false`), `anchor_lsn` stays `checkpoint_lsn` regardless of
  any reported replay LSN. The final `max(anchor_lsn, log_start)` clamps the
  scan start to the oldest WAL byte still available; an anchor below
  `log_start`, or a zero anchor with a recycled prefix, is rejected as corrupt
  rather than silently advanced. The scan identifies committed transactions
  (`kCommit`) and replays only committed records in LSN order
  (`PageManager::ReplayCommittedRecord`). Uncommitted loser transactions are
  naturally ignored without undo passes or CLRs. Also provides `ReadLog` for
  large-transaction WAL re-read abort/commit scans (P6a, ADR 0008).
- `checkpoint_manager.{hpp,cpp}` — compaction-driven persistence: triggered by
  LSM-Tree flush/compaction or committed mutation threshold (no idle periodic
  WAL writes or `.last_checkpoint` sidecar), captures `oldest_active_first_lsn`,
  persists compacted committed block/LSM state and `replay_start_lsn` to
  `<db>.db` via `PageManager::WriteFuzzyCheckpoint`, and recycles WAL segments
  below `replay_start_lsn`.

Test: `./build/logger_test`, `logger_segment_test`, `recovery_manager_test`,
`checkpoint_manager_test`.
