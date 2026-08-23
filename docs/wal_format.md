# WAL format

Write-ahead logging uses append-only segment files (`*.log`) managed by
`recovery::Logger`.

## Record header

Each log record is length-prefixed and typed (`recovery/log_record.hpp`):

| Field | Size | Notes |
|-------|------|-------|
| `lsn` | 8 | Monotonic log sequence number |
| `prev_lsn` | 8 | Previous record for same transaction |
| `txn_id` | 8 | Owning transaction |
| `type` | 2 | `LogType` enum |
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
