# Recovery invariants

After crash or `Database::EmulateCrash()` + reopen:

1. **Committed transactions** are visible; **aborted** or **in-flight** work is
   rolled back.
2. **Page LSN** on disk ≤ **CommittedLSN** for durable commits.
3. **Checksum** validates every page read from disk; corrupt pages fail closed.
4. **Checkpoint** (`*.last_checkpoint`) points at a recoverable LSN; replay starts
   from the minimum `rec_lsn` among active pages or the checkpoint, whichever is
   earlier.
5. **Catalog** table/index metadata matches row pages after redo completes.

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
- Format/structure corruption is NOT hidden as a no-op: `ASSERT_PAGE_TYPE`
  still throws and the page checksum still gates reads, so genuine damage
  fails loudly while a merely-absent apply target succeeds quietly.

`kSystemDestroyPage` (D3): REDO re-initializes the target page as
`kFreePage`; the free list is rebuilt by a `[1, max_page_count]` scan at the
end of `RecoverFrom` (only pages that ENDED recovery as free pages are
linked; live and lossy-undone images are preserved).  Destroy records carry
the old page type and body image, so UNDO of an aborted destroy (e.g. a
rolled-back DROP TABLE) restores the page exactly and pops it from the
allocator stack.  Re-applying the same destroy redo or re-running the whole
recovery is idempotent.

`AddLog` and visibility (D4): a transaction's versions become visible only
AFTER its commit record's `AddLog` returns; durability of the commit (own
and observed dependencies) is a separate barrier before results reach the
user -- see `docs/commit_durability.md`.

## Recovery pipeline

```
Startup → read checkpoint LSN
       → scan WAL from checkpoint
       → REDO page writes + row ops
       → UNDO uncommitted transactions
       → open for queries
```

## Testing

- Unit: `recovery_manager_test`, `checkpoint_manager_test`, `logger_test`.
- Idempotence / destroy (D2/D3): `LoserUndoSurvivesRepeatedRecoveryPasses`,
  `DestroyPageRedoInitializesFreePageAndRebuildsList`,
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
