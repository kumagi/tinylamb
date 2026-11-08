#WAL format

Write-ahead logging uses append-only segment files managed by
`tinylamb::Logger` (`recovery/logger.hpp`).  See `docs/design.md` decisions D3 and D9.

## Segmentation and recycling

The WAL is cut into fixed-size segment files named `<dbname>.log.<index>`
(zero-padded 20-digit index; `kDefaultLogSegmentSize` = 16 MiB, test-tunable
via `TINYLAMB_LOG_SEGMENT_BYTES`). An absolute LSN maps to
`index = lsn / segment_size`, `in-segment offset = lsn % segment_size`;
renumbering never happens, so page stamps and prev_lsn chains stay valid across
                               recycling.A record may straddle a segment
                               boundary(its bytes are split across two files);
the writer caps each `write()` at the boundary and
rolls to a fresh O_APPEND fd, fsyncing the old segment before closing it.

The effective segment size is persisted in `<dbname>.log.meta` (big-endian
magic/version/segment-size/CRC32C). Once numbered segments exist, this
metadata is authoritative and a conflicting `TINYLAMB_LOG_SEGMENT_BYTES`
override is ignored. A base-only legacy log may still be migrated to a newly
requested size;
a numbered series without metadata fails closed rather than reinterpreting or
    truncating existing segments. If the base file is still larger than one
    segment, migration is resumed even when a partial numbered segment already
    exists. The sidecar is written with a temporary file,
    file fsync, atomic rename,
    and directory fsync
                .

Database storage rejects an effective or persisted segment geometry that
cannot hold the serialized full-page-image record; a legacy database using a
smaller geometry must be migrated before it can open.

`CheckpointManager::WriteCheckpoint` persists the in-memory `RowPage` blocks,
LSM-Tree runs, and active transaction table to `<dbname>.db` (`kFuzzyCheckpointMagic`,
`TLCP`) when LSM compaction completes (or on clean shutdown / explicit request)
and recycles old WAL segments. Because `<dbname>.db` is written atomically via
`<dbname>.db.tmp` + `fsync` + `rename` after `Logger::WaitForDurable(durable_target)`,
WAL replay on startup begins at `min(begin_lsn, min_rec_lsn, min_att_first_lsn)`
stored directly in `<dbname>.db`.
Segments are removed only when ALL of the following hold:

            1. The segment's records all sit below the recycle floor
   `min(checkpoint begin LSN, oldest DPT recLSN,
            oldest active
                transaction's first LSN, oldest not-yet-fsynced full-page image)`. 2. The new master
                    record(anchoring this checkpoint)
                        is already durable-- a crash right before the rename
                            keeps every segment the OLD anchor needs,
            and a crash right after it never needs the removed ones.3. The data
                    fdatasync'd past the protected writes, so the full -
                page images guarding those page writes are no
                    needed (SyncDataFile retires only images whose pwrite
    completed before the sync began; emitting, in-flight, and failed writes
    remain pending until a later successful pwrite and sync).
4. The next segment's first byte starts a record (`RecycleUpTo` consults
   `RecoveryManager::ReadLog`)
     : a record
      straddling the boundary keeps its head segment alive until the record's tail is gone from the WAL too.
   A segment is dropped only when `(index + 1) * segment_size <= floor`, so
   the "oldest not-yet-fsynced full-page image" term of that floor is the
   **last byte** of the `kFullPageImage` record (`Page::UnsyncedSnapshotFloor`
   reports `snapshot_end - 1`), never its exclusive end. Using the end would
   let a record that finishes exactly on a segment boundary be unlinked with
   that segment while its page write is still unsynced -- destroying the very
   image a torn page needs to rebuild.

If the retained WAL (BufferedLSN minus the lowest retained segment start)
exceeds `TINYLAMB_WAL_RETENTION_WARN_BYTES` (default 1 GiB), every
checkpoint logs a rate-limited warning naming the recycle floor AND the
concrete term that pins it (oldest active transaction with its id, the
oldest dirty page's recLSN with its page id, or the oldest unsynced
full-page image). The WAL grows without bound until that constraint clears;
the named term says which of the three remediations applies.

      Consequences
    : restart recovery always replays from the durable master
      record's checkpoint and never reads below the retained start; `--force` torn
            - tail truncation only resizes the tail segment
        and drops segments above it;
and a pre-segmentation single-file `<dbname>.log` is split into segments
automatically on first open (`Logger::MigrateLegacyLog`). Media corruption
of a *data-file page* is recoverable from the WAL while a full-page image of
that page is retained (see kFullPageImage below);
a page whose image was already checkpoint -
    fsynced and whose WAL snapshot was recycled cannot be rebuilt from the WAL
    alone(standard single - node trade - off-- use backups)
        .

Torn pages are detected by **checksum over the whole allocated page range**,
not by dirty-page-table membership: a page the checkpoint already flushed
drops out of the DPT, so if the crash tore it there is no redo entry for it.
`RecoverFrom` therefore sweeps `1..scan_high_water` before the free-list
rebuild and runs Single Page Recovery for every non-free page whose stored CRC
does not match its body, failing the open when the retained WAL can no longer
rebuild it (free pages and never-written holes -- CRC 0 -- keep the historical
skip behaviour). A page recovery itself rewrote is marked *repaired in memory*
(`Page::MarkRepairedInMemory`): `IsValid()` then reports it whole, so no reader
mistakes a repair for a torn page, while `ChecksumMatches()` keeps reporting it
dirty so the repair is still written back.

    ## #Torn tail policy

    The first unparseable record ends the log;
recovery classifies the damage(`RecoveryManager::ClassifyTailDamage`) and
    truncates automatically when the bytes are provably never -
        acknowledged :

    -**kTornRecord* *
    --a record whose header decodes but whose tail is
      cut(crash mid - append) at the end of the newest segment
          .Truncated automatically : the record was never reported durable.-
        **kZeroTail* *
            --only zero bytes follow the last valid
              record(blocks the filesystem lost before the append reached disk)
                  .Truncated automatically : zeros can never encode a record,
    and nothing follows.- **kSuspect* * --mid - file garbage,
    or damage outside the newest segment(e.g.a missing middle segment)
            .Fails closed with `LOG(FATAL)`;
truncating could silently drop committed history behind the corruption point
    .Only `--force` (`RecoveryManager::SetTornTailTruncationAllowed`)
        truncates these
    .

    This keeps an
    unattended restart from wedging on the previous life's last append
    while still refusing to boot over damage that may hide committed
    transactions.

    ##Record header

    Each log record is typed(`recovery / log_record.hpp`); its byte offset is its
LSN. All fixed-width fields use big-endian order:

| Field | Size | Notes |
|-------|------|-------|
| magic | 4 | `kSerdesMagic` (`TYB1`) |
| version | 4 | `kWalRecordVersion` (currently 6) |
| type | 2 | `LogType` enum |
| `prev_lsn` | 8 | Previous record for same transaction |
| `txn_id` | 8 | Owning transaction |
| presence flags | 1 | Optional page id, slot, and key fields |
| payload | variable | Type-specific body |
| CRC32C | 4 | v3+;
covers all preceding record bytes |

    ##Version history

        - **v1**(`kLegacyWalRecordVersion`)
    : original layout. `kSystemDestroyPage` carried only the page id; records had no trailing checksum.
- **v2** (`kDestroyPayloadWalVersion`, D3): `kSystemDestroyPage` additionally
  carries the destroyed page's type and, when the page held rows, its full
  body image so undo of an aborted destroy restores the page exactly. Redo
  re-initialises the page as a free page and recovery rebuilds the free list
  by scanning the page range.
- **v3** (`kCrcWalRecordVersion`, D9): every record ends with a CRC32C over the
  complete record byte sequence excluding the CRC field itself.
- **v4**: adds a distinct `kCompensateSetLowestValue` CLR type so a
  SetLowestValue compensation is never mistaken for a forward update during
  repeated recovery.
- **v5**: adds `kSetRowPageNext`, `kSetRowPagePrev`, and
  their compensation records for persistent row-page chain links.
- **v6** (`kWalRecordVersion`): `kSystemAllocPage` carries the new page type,
  allocation kind, and a fixed 16-byte `redo_data` payload containing the
  big-endian pre-allocation `first_free_page` and `max_page_count`. Its
  `undo_data` is kind-specific: fresh allocation has no undo bytes; recycled
  allocation has a fixed 16-byte payload containing the big-endian old
  `next_free_page` and `destroyer`; in-place page-type changes retain the
  complete 32 KiB old target body. No payload aliases `MetaPage` or `FreePage`
  storage, so struct padding is not part of the wire format. The new
  `kCompensateAllocPage` CLR has the same kind-specific payload and restores
  both the target and allocator state. Fresh allocation undo returns the target
  to `kFreePage` and restores the old allocator fields. Recycled redo pops the
  captured link and sets the high-water mark to
  `max(old_max_page_count, allocated_page_id)`; its undo restores the captured
  free-page state and exact old allocator fields, including a legacy state where
  the free-list head is ahead of a stale high-water mark. In-place allocation
  does not mutate the allocator.

## Log types (selected)

See `LogType` in `recovery/log_record.hpp` for the full list.

- **kBegin / kCommit / kAbort** — transaction lifecycle markers. Recovery
  treats any transaction without a `kCommit` or `kAbort` terminator as a loser
  and rolls back its mutations during `RecoverFrom`.
- **kInsertRow / kUpdateRow / kDeleteRow** (`kCompensateInsert` /
  `kCompensateUpdate` / `kCompensateDelete`) — row-level changes on `RowPage`
  blocks with `(page_id, slot)` and row payload.
- **kSetRowPageNext / kSetRowPagePrev** (`kCompensateRowPageNext` /
  `kCompensateRowPagePrev`) — persistent `RowPage` doubly-linked chain-link
  assignments and their compensation records.
- **kLsmPut / kLsmDelete / kLsmDestroy** (`kCompensateLsmPut` /
  `kCompensateLsmDelete` / `kCompensateLsmDestroy`) — LSM-Tree index key-value
  inserts/updates, key deletions, and tree destruction, carrying previous value
  information for runtime abort and crash undo.
- **kSystemAllocPage / kSystemDestroyPage** (`kCompensateAllocPage` /
  `kCompensateDestroyPage`) — `RowPage` block allocation and destruction.
- **kBeginCheckpoint / kEndCheckpoint** — legacy WAL record enumerators retained
  for backward-compatible log scanning; current LSM compaction persistence writes
  the recovery anchor (`begin_lsn`, DPT, ATT) directly into `<dbname>.db`
  without emitting checkpoint records into the WAL stream.
- **kFullPageImage** — legacy full-page image enumerator retained in `LogType`.

## Durability

Committed transactions wait for `CommittedLSN` to be durable when
`synchronous_commit` is on (default). See `docs/commit_durability.md`.

## Checksum (D9)

Each v3+ record carries a trailing CRC32C (`common/crc32c.hpp`) computed over
all record bytes before the CRC field. `RecoveryManager::ReadLog` validates
magic, version, type and payload first, then the CRC;
a mismatch is treated as the valid end of the log so nothing after a corrupted
        record is replayed.Legacy v1 /
    v2 records are read without requiring a CRC(they predate it),
    so existing logs stay readable
        .

    ##Compatibility

    Writers always emit `kWalRecordVersion`.Readers accept
    v1(no CRC, pid - only destroy),
    v2(typed destroy payload, no CRC), v3(typed destroy payload + CRC),
    v4(the distinct SetLowestValue CLR), v5(row-page chain links), and
    v6(allocation undo payloads and `kCompensateAllocPage`);
a version outside this range is treated as corrupt(or a torn tail). Re-encoding
a decoded v1-v5 allocation preserves its original wire version because the
missing undo payload cannot be synthesized safely.


## Fixed-file prototype (metadata version 2)

C14 introduces an opt-in fixed-file series; see
[ADR 0001](adr/0001-fixed-wal-logical-end.md) for its format and adoption gates.
Its S-byte LSN data area is followed by two 32-byte CRC32C logical-end slots.
The physical S+64 bytes never mean S+64 bytes of WAL records. A successful
fdatasync covers both data and the new logical end before DurableLSN advances.
Both slots and physical length must validate; the older valid slot is never
an automatic fallback after damage. A damaged frame requires explicit force,
which scans the available data area for a CRC-valid prefix before truncating.
Every byte below a framed logical end is data: zero tails there cannot use
the legacy never-written-region exemption. Existing version-1 series retain
their format, and older binaries reject version 2.
