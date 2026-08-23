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
- Fuzz: `table_fuzzer` performs random DML, calls `EmulateCrash()`, reopens the
  database, and verifies the row model (`table/table_fuzzer.hpp`).
- End-to-end: `table/table_fuzzer_replay.cpp` replays libFuzzer corpora.

## Related docs

- WAL layout: `docs/wal_format.md`
- Page header: `docs/page_format.md`
- Durability policy: `docs/commit_durability.md`
