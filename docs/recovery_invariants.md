# Recovery invariants

After crash or `Database::EmulateCrash()` + reopen:

1. **Committed transactions** are visible; **aborted** or **in-flight** work is
   rolled back.
   Abort chains carry the same terminator record recovery classifies commits
   by (`LogType::kCommit`), so `TransactionManager::Abort` appends it only
   after the undo walk compensated the whole chain. Every CLR records the
   original record's `prev_lsn` as its undo-next pointer, so a crash during a
   multi-step abort resumes at the first uncompensated predecessor. A partially compensated
   chain stays un-terminated and is finished by the restart loser-undo
   (idempotent through the `page_lsn` guard in `UndoLoserChains`); appending
   the terminator unconditionally would resurrect the uncompensated tail as
   committed data.
2. **Page LSN** on disk ≤ **CommittedLSN** for durable commits.
3. **Checksum** validates every page read from disk; corrupt pages fail closed.
4. **Checkpoint** (`*.last_checkpoint`) points at a recoverable LSN; replay starts
   from the minimum `rec_lsn` among active pages or the checkpoint, whichever is
   earlier.
5. **Catalog** table/index metadata matches row pages after redo completes.
6. **Recycled WAL never holds a needed record**: after a durable checkpoint
    the log is truncated by whole segments (see `docs/wal_format.md`), and
    every consumer -- redo from the DPT, loser-chain undo, torn-tail
    truncation, runtime abort walks -- stays at or above the retained start
    because the recycle floor is the minimum over (checkpoint begin LSN,
    oldest DPT recLSN, oldest active transaction's first LSN, oldest
    unsynced full-page image). Each checkpoint also flushes stale dirty
    pages (oldest recLSN first, time-budgeted) so the oldest-DPT-recLSN
    component actually advances under sustained load. The allocator meta page
    is synchronously flushed outside that budget before the master record is
    published; a failed meta flush aborts the checkpoint without recycling.
    `SyncDataFile` retires only images whose pwrite completed before the
    sync began: emitting, in-flight, and failed writes remain pending, and a
    later successful pwrite plus sync is required for retirement. A torn
    data-file page rebuilds from its retained `kFullPageImage` record plus
    the deltas above it.

## Idempotent redo/undo (D2, `docs/design.md`)

Recovery may apply the same log more than once: a loser UNDO rewinds
`page_lsn`, so a second `RecoverFrom` re-runs both REDO and UNDO.  Therefore
the page-level apply/undo operations are logically idempotent -- an
already-applied or already-undone mutation is a SUCCESSFUL NO-OP, not an
error:

- Row delete / insert undo on an empty, absent or out-of-range slot does not
  decrement `row_count_` a second time (the underflow-to-65535 symptom).
- Leaf insert re-apply overwrites the existing key in place (no duplicate
  entry, no row-count growth); leaf delete/update of a missing key is a no-op
  and never drops a neighbouring row.
- Branch insert re-apply keeps ONE separator per key (child id overwritten in
  place).  Branch UPDATE re-apply of a missing key never overwrites the
  landing neighbour or the foster slot.  Branch DELETE re-apply
  (`Page::DeleteBranchImpl`, the REDO/UNDO-only entry) verifies the recorded
  separator still exists; the forward path keeps its promote-lowest special
  case.
- Page LSN remains the FAST filter (an already-newer page skips the record),
  but it is never the sole correctness basis -- the page operations are
  self-guarding, so replay order changes (CLR, original record, restart)
  cannot corrupt the page.
- Format/structure corruption is NOT hidden as a no-op: ordinary page APIs
  still reject invalid types, while recovery validates replay target types
  and returns `kCorrupt` before advancing page LSN. The page checksum still
  gates reads, so genuine damage fails loudly while a merely-absent apply
  target succeeds quietly. Replay payload-size and page-capacity failures
  likewise return an error before advancing the page LSN; a rejected
  full-page image cannot be mistaken for an applied image.
- The UNDO side has one deliberate exception to the hard type check: when the
  target page's type no longer matches the record because the page was
  destroyed or re-issued AFTER the record was written (a newer incarnation),
  the operation's effects no longer exist, so `LogUndo` logs the CLR (the
  loser chain must terminate on every future replay), skips the physical
  mutation, and leaves the page's own stamps untouched. A mismatch on a page
  whose current incarnation still predates the record remains `kCorrupt`.

`kSystemDestroyPage` (D3): REDO re-initializes the target page as
`kFreePage`; the free list is rebuilt by a `[1, max_page_count]` scan at the
end of `RecoverFrom` (only pages that ENDED recovery as free pages are
linked; live and lossy-undone images are preserved).  Destroy records carry
the old page type and body image, so UNDO of an aborted destroy (e.g. a
rolled-back DROP TABLE) restores the page exactly and pops it from the
allocator stack.  Re-applying the same destroy redo or re-running the whole
recovery is idempotent.

`kSystemAllocPage` v6 is transactional. Every record carries a fixed 16-byte
big-endian allocator snapshot: pre-allocation `first_free_page` and
`max_page_count`. Fresh allocation has no undo bytes, recycled allocation has
a fixed 16-byte big-endian `next_free_page`/`destroyer` snapshot, and in-place
page-type changes retain the complete 32 KiB old target body. No payload reads
`MetaPage` or `FreePage` object representation. Runtime abort and loser recovery
therefore restore the allocator without relying on a restart-time free-list
scan. `kCompensateAllocPage` is appended before either page changes; recovery
validates exact payload sizes and allocator/free-list invariants, then treats
the CLR as the idempotent redo image. Fresh undo makes the target free and
restores the old high-water mark. Recycled redo pops the captured link and
repairs the high-water mark to `max(old_max_page_count, page_id)`. Recycled undo
restores the exact old free state and allocator fields, including a free-list
head ahead of a stale high-water mark; when later allocator activity forces a
safe merge rather than an exact snapshot restore, the merged head and monotonic
high-water mark are retained. Legacy v1-v5 allocation records have no undo
payload and keep the legacy redo-only behavior.
Recycled-page MVCC chains are cleaned only after the allocation commits; the
cleanup preserves that transaction's pending versions on the new incarnation,
while abort leaves the old incarnation's chains intact.

`AddLog` and visibility (D4): a transaction's versions become visible only
AFTER its commit record's `AddLog` returns; durability of the commit (own
and observed dependencies) is a separate barrier before results reach the
user -- see `docs/commit_durability.md`.

## Recovery pipeline

```
Startup → read checkpoint LSN
       → scan WAL from checkpoint (rate-limited progress lines per phase:
         torn-tail scan / analysis / redo collection / torn-page sweep)
       → REDO page writes + row ops (parallel per-page replay)
       → UNDO uncommitted transactions
       → torn-page checksum sweep over the whole allocated range
         (parallel classification; single-page recovery stays serial)
       → END-OF-RECOVERY CHECKPOINT: publish a fresh master record so the
         replayed state is durable immediately -- a crash right after a
         long recovery re-pays only the post-checkpoint WAL, and the
         recycled-prefix becomes reclaimable without waiting for the
         periodic timer
       → open for queries (one-line summary: WAL window, committed/loser
         counts, pages rebuilt, elapsed)
```

The end-of-recovery checkpoint is best-effort: a failure logs a warning and
the periodic checkpoint retries; the recovered pages stay protected by the
retained WAL either way.

## Testing

- Unit: `recovery_manager_test`, `checkpoint_manager_test`, `logger_test`.
- Segmented WAL / recycling / FPW: `recovery_extra_test`
  (`WalRollsAcrossSegmentsAndReadsBack`, `LegacySingleFileLogMigratesToSegments`,
  `CheckpointRecyclesOldSegmentsAndRestarts`,
  `TornPageRebuildsFromFullPageImageAfterRecycle`,
  `TornWalTailAcrossSegmentsTruncatesWithForce`,
  `WholeWalReplaysWhenNoCheckpointExisted`).
- Torn-tail auto-truncation (safe classes) and `--force`:
  `HalfWrittenRecordAutoTruncatesWithoutForce`,
  `ZeroFilledTailAutoTruncatesWithoutForce`,
  `CorruptTailAbortsByDefault`, `ForceRecoversFromCorruptTail`.
- Commit visibility under concurrent checkpoints (ATT-classification race
  regression): `DatabaseExtraTest.CommittedRowsSurviveCrashDuringCheckpointStorm`.
- Checkpoint worker lifecycle (`Start()` gating, interval 0 disabled):
  `CheckpointTest.WorkerStaysParkedUntilStart`,
  `CheckpointTest.ZeroIntervalDisablesPeriodicCheckpointing`.
- Allocation undo: `PageManagerTest.RuntimeFreshAllocationAbortReusesId`,
  `PageManagerTest.RuntimeRecycledAllocationAbortRestoresFreeBodyAndReusesId`,
  `RecoveryManagerTest.FreshLoserAllocationUndoRestoresIdForImmediateReuse`,
  `RecoveryManagerTest.RecycledLoserAllocationUndoRestoresBodyForImmediateReuse`,
  `RecoveryManagerTest.VersionSixCommittedAllocationSurvivesRecovery`.
- Idempotence / destroy (D2/D3): `LoserUndoSurvivesRepeatedRecoveryPasses`,
  `DestroyPageRedoInitializesFreePageAndRebuildsList`,
  `RowPageLinkRedoAndLoserUndo`,
  `Update_CommittedRelocationSurvivesCrashReplay`,
  `DestroyPageRedoTwiceOnSamePageIsIdempotent`,
  `DestroyPageUndoRestoresRowContent`, `DeleteRowReappliedIsIdempotent`,
  `LeafPageTest.ImplOperationsAreIdempotent`,
  `BranchPageTest.ImplOperationsAreIdempotent`,
  `CatalogTest.DropTableCrashRecoveryReclaimsPages`,
  `BPlusTreeTest.NodeReclaimWALReplaysAfterCrash`.
- WAL CRC (D9): `MidRecordBitFlipStopsScanAtThatRecord`,
  `MixedLegacyAndCurrentVersionLogsScanCleanly`,
  `LegacyV1DestroyRecordStillParses`.
- Logger D1: `D1NoRecordsInterleavedAcrossProducers`,
  `D1RejectsRecordOverMaxSize`.
- Fuzz: `table_fuzzer` performs random DML, calls `EmulateCrash()`, reopens the
  database, and verifies the row model (`table/table_fuzzer.hpp`).
- End-to-end: `table/table_fuzzer_replay.cpp` replays libFuzzer corpora.

## Related docs

- WAL layout: `docs/wal_format.md`
- Page header: `docs/page_format.md`
- Durability policy: `docs/commit_durability.md`
