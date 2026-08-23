# Page format

Fixed **32 KiB** pages (`kPageSize` in `common/constants.hpp`).

## Header (32 bytes)

| Offset | Field | Type | Purpose |
|--------|-------|------|---------|
| 0 | `page_id` | u64 | Identity |
| 8 | `page_lsn` | u64 | Last WAL LSN applied |
| 16 | `rec_lsn` | u64 | Recovery start LSN |
| 24 | `page_type` | u64 | `PageType` discriminator |
| 28 | `checksum` | u32 | CRC32C over body + header prefix |

Body size = `kPageSize - kPageHeaderSize` (32 KiB − 32 B).

## Page types

- **RowPage** — slotted row store (default heap table).
- **BranchPage / LeafPage** — B+ tree nodes.
- **Meta / checkpoint** pages — catalog and recovery metadata.

Row and index layouts are validated on read; checksum mismatch returns
`Status::kCorrupt` from `PagePool::ReadFrom` (see `docs/page_checksum.md`).

## PAX variant

Columnar PAX blocks for analytics experiments are documented in
`docs/pax_page_format.md`.
