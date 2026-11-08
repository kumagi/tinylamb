# `recovery/` — Layer 3 storage: Logical WAL + Fuzzy Checkpoint + Fuzzy Recovery

Logical write-ahead log, group commit, lock-free fuzzy checkpoints, and
commit-filtered fuzzy crash recovery.

## Key files

- `logger.{hpp,cpp}` — group commit: 8 MiB buffer + 1 ms worker thread over a
  segmented WAL (`<db>.log.<index>`). Three-LSN split: `BufferedLSN` /
  `CommittedLSN` / `DurableLSN`.
- `log_record.{hpp,cpp}` — compact logical WAL records (`kBegin`, `kCommit`,
  `kAbort`, `kBeginCheckpoint`, `kEndCheckpoint`, `kInsertRow`, `kUpdateRow`,
  `kDeleteRow`, `kCompensateUpdate`, `kSetRowPageNext`, `kSetRowPagePrev`,
  `kSystemAllocPage`, `kSystemDestroyPage`, `kLsmPut`, `kLsmDelete`,
  `kLsmDestroy`) protected by trailing CRC32C.
- `recovery_manager.{hpp,cpp}` — `RecoverFrom(checkpoint_lsn)`: loads the
  latest fuzzy checkpoint via `PageManager::LoadFuzzyCheckpoint` and runs a
  2-pass WAL scan from `min(checkpoint_lsn, ckpt_replay_start_lsn)` that
  identifies committed transactions (`kCommit`) and replays only committed
  records in LSN order (`PageManager::ReplayCommittedRecord`). Uncommitted
  loser transactions are naturally ignored without undo passes or CLRs.
- `checkpoint_manager.{hpp,cpp}` — compaction-driven persistence: triggered by
  LSM-Tree flush/compaction or committed mutation threshold (no idle periodic
  WAL writes or `.last_checkpoint` sidecar), captures `oldest_active_first_lsn`,
  persists compacted committed block/LSM state and `replay_start_lsn` to
  `<db>.db` via `PageManager::WriteFuzzyCheckpoint`, and recycles WAL segments
  below `replay_start_lsn`.

Test: `./build/logger_test`, `logger_segment_test`, `recovery_manager_test`,
`checkpoint_manager_test`.
