# WAL format

Write-ahead logging uses append-only segment files (`*.log`) managed by
`recovery::Logger`.

## Record header

Each log record is typed (`recovery/log_record.hpp`); its byte offset is its
LSN. All fixed-width fields use big-endian order:

| Field | Size | Notes |
|-------|------|-------|
| magic | 4 | `kSerdesMagic` (`TYB1`) |
| version | 4 | `kSerdesVersion` (= 1) |
| type | 2 | `LogType` enum |
| `prev_lsn` | 8 | Previous record for same transaction |
| `txn_id` | 8 | Owning transaction |
| presence flags | 1 | Optional page id, slot, and key fields |
| payload | variable | Type-specific body |

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

## Checksum

Page images embedded in WAL reuse the page checksum described in
`docs/page_format.md` / `docs/page_checksum.md`.

## Compatibility

This is an intentional destructive v1 format bump. Headerless v0 WAL and
master checkpoint records are not accepted; unknown versions are corrupt.
