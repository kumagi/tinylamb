# Recovery invariants

Current implementation, verified against the source on 2026-11-11.

## Persistence and visibility

1. Restart loads committed snapshot state and replays records only for owners
   with a valid `kCommit` in the retained WAL scan. Aborted and unfinished
   transactions are not replayed. A synchronous successful commit must survive;
   asynchronous commits can be lost before their WAL becomes durable.
2. `BufferedLSN` is the appended end, `CommittedLSN` is the written end, and
   `DurableLSN` is the synced end. Only `WaitForDurable` / `DurableLSN` proves
   durability. Publishing MVCC versions and acknowledging a commit are separate
   steps; see [commit durability](commit_durability.md).
3. `CheckpointManager::WriteCheckpoint` captures the current buffered end and
   the oldest active transaction's first LSN under the transaction registry and
   per-transaction chain locks. Its replay anchor is their minimum (the buffered
   end when no active transaction has logged).
4. Before writing the snapshot, the checkpoint manager waits for the WAL's
   captured durable target. `PageManager::WriteFuzzyCheckpoint` serializes
   committed block slots/link stamps, free page IDs, committed LSM entries,
   allocator high-water mark, transaction-ID high-water mark and replay anchor.
   There is no persisted dirty-page table or active-transaction table.
5. `<db>.db` starts with `TLSMCKP1`, snapshot version 2, and ends with CRC32C.
   Loading checks the checksum, field bounds and complete consumption. Corrupt
   input must fail with a status rather than exposing partially decoded data to
   queries. Snapshot writes use a temporary file, data sync and atomic rename.
6. WAL recycling follows successful snapshot publication and only removes whole
   segments below the replay anchor. The logger also checks retained segment
   boundaries so a legacy straddling record does not lose its head. Active
   transactions can pin WAL indefinitely. No DPT, full-page-image retention
   term, or `TINYLAMB_WAL_RETENTION_WARN_BYTES` warning is implemented.
7. Catalog and row/index state are recovered through the same committed-record
   filter. A snapshot with missing WAL, an anchor below the retained start, or a
   recycled WAL prefix without an anchor is rejected as corrupt.

## Runtime abort and replay

`TransactionManager::Abort` reverses `staged_records_` in memory through
`PageManager::ApplyTransactionAbort`, removes pending MVCC versions and appends
one `kAbort` when the transaction has logged. It does not read the WAL chain,
write a CLR for every operation, append a `kCommit`, or wait for abort durability.
A failure during rollback/append is surfaced and can fail-stop the transaction
engine. Restart needs no loser undo because it never replays those transactions.

`kCompensateUpdate` still exists for `RowPage::RestorePhysicalUpdate`; it is not
a restart undo-next chain protocol. Alloc/destroy records carry a page ID (the
allocation type is in flags), not a page-body or allocator undo image. Replay
uses committed slot/link/entry LSNs and allocation state to avoid overwriting
newer snapshot data. Destruction removes committed state and tracks free IDs;
there is no `kFreePage` scan or B+Tree leaf/branch redo path.

Rollback restoration must restore the previous row even when another write has
consumed the byte budget released by a shrink/delete. That admission budget is
for new writes, not a reason to discard an undo image.

## Recovery pipeline

1. Load and validate the fuzzy checkpoint, if present.
2. Validate the retained WAL window from the selected replay anchor, classifying
   any damaged tail according to [WAL format](wal_format.md).
3. Scan once to collect committed transaction IDs and the transaction-ID high
   water mark. Seed the next transaction ID from that mark.
4. Scan again in LSN order and apply only records whose owner committed. Both
   passes are single-threaded and skip recognized segment padding.
5. `PageStorage::Create` attempts an end-of-recovery checkpoint unless
   `TINYLAMB_DISABLE_EOR_CHECKPOINT` is present. Failure logs a warning and keeps
   the retained WAL; subsequent compaction/explicit checkpoints can retry.

The current implementation has no restart UNDO phase, full-page-image repair,
allocated-page checksum sweep, parallel page replay, or `.last_checkpoint`
master-record publication. Snapshot corruption requires recovery from an
appropriate backup; the code does not reconstruct a damaged snapshot from a
retained full-page-image log.

## Testing

The following names exist in the current tree (2026-11-11). This inventory
is not a claim that every legacy recovery failure mode has a replacement test.

- Record serialization and committed/loser recovery:
  `LogRecordTest.RoundTripSerialization` and
  `RecoveryManagerTest.ReplaysCommittedAndIgnoresUncommittedLosers`.
- Snapshot state and replay anchors:
  `PageManagerTest.UnboundedRowBlockInsertUpdateDeleteAndFuzzyCheckpoint`,
  `CheckpointManagerTest.FuzzyCheckpointWithInFlightAndPostCheckpointTxns`,
  `CheckpointManagerTest.CompactionShortensWalReplayWithoutCheckpointSidecar`.
- Concurrent checkpoint visibility:
  `DatabaseExtraTest.CommittedRowsSurviveCrashDuringCheckpointStorm`.
- Catalog restart/drop: `CatalogTest.TwoCleanRecoveryCyclesKeepTable` and
  `CatalogTest.DropTableCrashRecoveryReclaimsPages`.
- Logger durability, segmentation, fixed logical ends and damaged metadata:
  `recovery/logger_test.cpp`, including
  `LoggerTest.WaitForDurableObservesFsync`,
  `LoggerTest.FixedSegmentsRejectEitherDamagedSlotAndForcedScanKeepsBody`,
  `LoggerTest.FixedSegmentsSyncFailureNeverAdvancesDurableEnd`, and
  `LoggerTest.D1NoRecordsInterleavedAcrossProducers`.
- Worker shutdown: `CheckpointManagerTest.RapidConstructAndStopDoesNotLoseWakeup`.
- Random DML/restart model checking: `table/table_fuzzer.hpp` and
  `table/table_fuzzer_replay.cpp`.

The former `recovery_extra_test`, B+Tree page/allocator undo tests, and
legacy-format CRC test names were removed with their implementations. Do
not count them as passing or present coverage. New changes to corruption
handling or replay idempotence need current regression tests of their own.

## Related docs

- WAL layout: `docs/wal_format.md`
- Page header: `docs/page_format.md`
- Durability policy: `docs/commit_durability.md`
