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
    truncating existing segments.The sidecar is written with a temporary file,
    file fsync, atomic rename,
    and directory fsync
                .

`CheckpointManager::WriteCheckpoint` recycles the log after every durable
                    checkpoint.Segments are removed only when ALL of the
                        following hold :

            1. The segment's records all sit below the recycle floor
   `min(checkpoint begin LSN, oldest DPT recLSN,
            oldest active
                transaction's first LSN, oldest not-yet-fsynced full-page image)`. 2. The new master
                    record(anchoring this checkpoint)
                        is already durable-- a crash right before the rename
                            keeps every segment the OLD anchor needs,
            and a crash right after it never needs the removed ones.3. The data
                    file was
                        fdatasync'd past the protected writes, so the full -
                page images guarding those page writes are no longer
                    needed.4. The next segment's first byte starts a record (`RecycleUpTo` consults
   `RecoveryManager::ReadLog`)
    : a record
      straddling the boundary keeps its head segment alive until the record's tail is gone from the WAL too.

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
| version | 4 | `kWalRecordVersion` (currently 4) |
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
- **v4** (`kWalRecordVersion`): adds a distinct `kCompensateSetLowestValue` CLR
  type so a SetLowestValue compensation is never mistaken for a forward update
  during repeated recovery.

## Log types (selected)

See `LogType` in `recovery/log_record.hpp` for the full list.

- **kInsertRow / kUpdateRow / kDeleteRow** — row-level changes with table id
  and row payload or position;
kInsertLeaf / kInsertBranch(and the kUpdate* / kDelete * twins) are the B
    + tree structural counterparts.-
    **kSetLowFence / kSetHighFence / kSetFoster** — B
    + tree navigation metadata updates; each has a kCompensate* twin for undo.
- **kCommit** — transaction commit. Recovery treats a transaction whose
  newest LSN has no matching kCommit as a loser and undoes it. A runtime
  `TransactionManager::Abort` that completed its undo walk also writes a
  kCommit-typed marker: every compensation on the chain is already durable
  at that point, and the marker reclassifies the aborted transaction as
  committed so the next recovery neither redoes nor re-undoes it (the page
  ops are idempotent; the marker just keeps the loser set exact).
- **kBeginCheckpoint / kEndCheckpoint** — fuzzy checkpoint bracket; recovery
  truncates or replays from the last completed checkpoint (see
  `docs/recovery_invariants.md`).
- **kSystemAllocPage / kSystemDestroyPage** — page allocation/destruction.
- **kCompensateDestroyPage** — CLR for a destroy undo. Carries the same
  payload shape as `kSystemDestroyPage` (restored page type + full body
  image); its REDO re-installs that image.  Without it a crash after an
  aborted `DROP TABLE` would replay only the destroy and leave the
  resurrected catalog entry pointing at a free page.
- **kFullPageImage** — full-page write (FPW) protection, txn_id 0 (system
  record, never undone). `PagePool::WriteBack` emits it before a dirty page
  image whose on-disk copy is not known durable reaches the data file: the
  record carries the page type plus the complete 32 KiB body, and REDO
  restores that image verbatim. This is what keeps a torn (or partially
  lost) data-file page rebuildable from the WAL alone once the WAL prefix
  has been recycled: recovery replays the snapshot, then every delta above
  it. The page's snapshot becomes stale when recycling (or the checkpoint's
  data-file fdatasync watermark) moves past its record, and the next flush
  of that page emits a fresh one.

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
    and v4(the distinct SetLowestValue CLR);
a version outside this range is treated as corrupt(or a torn tail).
