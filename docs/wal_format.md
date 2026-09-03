# WAL format

Write-ahead logging uses append-only segment files (`*.log`) managed by
`recovery::Logger`.  See `docs/design.md` decisions D3 and D9.

## Record header

Each log record is typed (`recovery/log_record.hpp`); its byte offset is its
LSN. All fixed-width fields use big-endian order:

| Field | Size | Notes |
|-------|------|-------|
| magic | 4 | `kSerdesMagic` (`TYB1`) |
| version | 4 | `kWalRecordVersion` (currently 3) |
| type | 2 | `LogType` enum |
| `prev_lsn` | 8 | Previous record for same transaction |
| `txn_id` | 8 | Owning transaction |
| presence flags | 1 | Optional page id, slot, and key fields |
| payload | variable | Type-specific body |
| CRC32C | 4 | v3 only; covers all preceding record bytes |

## Version history

- **v1** (`kLegacyWalRecordVersion`): original layout. `kSystemDestroyPage`
  carried only the page id; records had no trailing checksum.
- **v2** (`kDestroyPayloadWalVersion`, D3): `kSystemDestroyPage` additionally
  carries the destroyed page's type and, when the page held rows, its full
  body image so undo of an aborted destroy restores the page exactly. Redo
  re-initialises the page as a free page and recovery rebuilds the free list
  by scanning the page range.
- **v3** (`kWalRecordVersion`, D9): every record ends with a CRC32C over the
  complete record byte sequence excluding the CRC field itself.

## Log types (selected)

- **kInsert / kUpdate / kDelete** — row-level changes with table id and row
  payload or position.
- **kPageWrite** — full page image after modification (`page_id`, bytes).
- **kCommit / kAbort** — transaction terminal states.
- **kCheckpoint** — fuzzy checkpoint marker; recovery truncates or replays from
  the last completed checkpoint (see `docs/recovery_invariants.md`).

## Durability

Committed transactions wait for `CommittedLSN` to be durable when
`synchronous_commit` is on (default). See `docs/commit_durability.md`.

## Checksum (D9)

Each v3 record carries a trailing CRC32C (`common/crc32c.hpp`) computed over
all record bytes before the CRC field. `RecoveryManager::ReadLog` validates
magic, version, type and payload first, then the CRC; a mismatch is treated
as the valid end of the log so nothing after a corrupted record is replayed.
Legacy v1/v2 records are read without requiring a CRC (they predate it), so
existing logs stay readable.

## Compatibility

Writers always emit `kWalRecordVersion`. Readers accept v1 (no CRC, pid-only
destroy), v2 (typed destroy payload, no CRC) and v3 (typed destroy payload +
CRC); a version outside this range is treated as corrupt (or a torn tail).

