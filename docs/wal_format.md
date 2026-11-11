# WAL format

Current implementation, verified against `recovery/log_record.cpp`,
`logger.cpp`, and `recovery_manager.cpp` on 2026-11-11.

## Segments and checkpoints

`Logger` writes `<db>.log.<index>` with a zero-padded 20-digit index. The
ordinary segment size is 16 MiB; `TINYLAMB_LOG_SEGMENT_BYTES` can request another
size. Persisted `<db>.log.meta` geometry is authoritative once a numbered series
exists. Absolute LSNs map to `index = lsn / segment_size` and
`offset = lsn % segment_size`; recycling never renumbers them. Metadata uses
big-endian fields and CRC32C and is published through a synced temporary file
and rename. A numbered series without metadata fails closed. Base-file legacy
migration concerns segment layout; it does not add support for old record
versions.

New records never straddle segment boundaries. If a record does not fit, the
writer waits for pending data, pre-rolls to the next segment and places the
record at its start. Legacy segments use zero padding; fixed-file segments use
the framed logical end. Readers retain boundary-spanning reads for migrated
logs, and recycling validates the next retained record boundary.

`CheckpointManager::WriteCheckpoint` chooses
`min(buffered_end_at_capture, oldest_active_first_lsn)`, waits for WAL durability,
and persists committed row/LSM state with that replay anchor in `<db>.db`.
The snapshot magic is `TLSMCKP1`, version 2. It has no DPT/ATT and no checkpoint
WAL records or `.last_checkpoint` sidecar are emitted. Successful publication
allows whole WAL segments below the anchor to be recycled. There is no
full-page-image geometry requirement or full-page-image retention term.
See [recovery invariants](recovery_invariants.md) and
[page format](page_format.md).

## Record version 7

Only `LogRecord::kCurrentVersion = 7` is accepted. Versions 1–6 from the retired
storage implementation are rejected, not transparently upgraded.

| Offset | Field | Bytes | Encoding |
|---:|---|---:|---|
| 0 | magic | 2 | native-endian integer `0x4C53` |
| 2 | version | 2 | native-endian integer 7 |
| 4 | total length, including CRC | 4 | native-endian unsigned |
| 8 | previous transaction-record LSN | 8 | big-endian |
| 16 | owner transaction ID | 8 | big-endian |
| 24 | type | 1 | `LogType` |
| 25 | flags | 1 | bit 0: `lsm_had_prev`; remaining bits: allocated page type |
| 26 | payload | variable | type-specific, below |
| total − 4 | CRC32C | 4 | native-endian standard finalized CRC32C |

The minimum record length is 30 bytes. Page IDs and slots use the big-endian
serdes helpers. Strings use a native-endian uint32 length followed by bytes.
Thus version 7 is **not an architecture-independent interchange format**: its
header, string lengths and CRC retain host byte order. Do not copy WAL between
hosts of different endianness. A portable encoding needs a new version and an
explicit migration; changing readers alone would reinterpret existing records.

`Deserialize` validates magic/version and total bounds, checks CRC over all
preceding bytes, then decodes type and payload with bounds checks. Failure
returns zero consumed bytes and is handled by recovery's tail policy.

## Record payloads

| Types | Payload |
|---|---|
| `kBegin`, `kCommit`, `kAbort`, `kBeginCheckpoint`, `kEndCheckpoint` | none |
| `kSystemAllocPage`, `kSystemDestroyPage`, `kLsmDestroy` | page/tree ID |
| `kSetRowPageNext`, `kSetRowPagePrev` | page ID, new page ID, old page ID |
| `kInsertRow`, `kCompensateUpdate` | page ID, slot, new row string |
| `kDeleteRow` | page ID, slot, old row string |
| `kUpdateRow` | page ID, slot, new row string, old row string |
| `kLsmPut` | tree ID, key, new value, old value |
| `kLsmDelete` | tree ID, key, old value |

`kUnknown` is not a valid decoded type. Checkpoint enumerators remain but the
current checkpoint writer does not emit them. `kCompensateUpdate` is the only
compensation enumerator; `kFullPageImage`, allocation CLRs, B+Tree and other
legacy compensation types no longer exist. Runtime abort reverses staged
in-memory records and appends `kAbort`; restart replays only `kCommit` owners.

## Damaged tails

The first unparseable record ends the valid prefix. Recovery returns `kCorrupt`
for suspect damage unless explicit force truncation is enabled; it does not
terminate the process with `LOG(FATAL)`.

- A short trailing record in the newest legacy segment is classified
  `kTornRecord` and can be truncated automatically.
- An all-zero newest legacy tail, including one reaching the segment boundary,
  is `kZeroTail`. Legacy rolling pads before creating the next file; a crash
  between those steps therefore needs no force flag. A later segment beyond a
  missing successor still proves a hole and returns an I/O error.
- Interior corruption, missing segments and other ambiguous damage are
  `kSuspect`. Fixed-file bytes below the framed logical end are always data;
  damaged bytes there cannot claim the legacy never-written-tail exemption.

Only recognized damage is automatically truncated. Explicit force can discard
history and does not constitute proof that no acknowledged commit was lost.
There is no data-page reconstruction from full-page-image WAL in this format.

## Durability

Acknowledgement uses `WaitForDurable` on a record's exclusive end LSN, not the
record start or `CommittedLSN`. See [commit durability](commit_durability.md)
for own-commit and observed-dependency barriers.

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
