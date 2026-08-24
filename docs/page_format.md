# Page format

Fixed **32 KiB** pages (`kPageSize` in `common/constants.hpp`).

## Header v1 (48 bytes, big-endian)

| Offset | Field | Type | Purpose |
|--------|-------|------|---------|
| 0 | magic | u32 | `kSerdesMagic` (`TYB1`) |
| 4 | version | u32 | `kSerdesVersion` (= 1) |
| 8 | `page_id` | u64 | Identity |
| 16 | `page_lsn` | u64 | Last WAL LSN applied |
| 24 | `recovery_lsn` | u64 | Runtime recovery start LSN; excluded from the persistent checksum |
| 32 | `page_type` | u64 | `PageType` discriminator |
| 40 | `checksum` | u64 | CRC32C stored as a zero-extended 64-bit value |

Body size = `kPageSize - kPageHeaderSize` (32 KiB − 48 B). The fixed header,
PAX metadata/value arrays, and values encoded through `common/serdes` use
network byte order. Unknown magic/version values are rejected as corrupt.

## Page types

- **RowPage** — slotted row store (default heap table).
- **BranchPage / LeafPage** — B+ tree nodes.
- **PaxPage** — persistent column-group page for `DataChunk` values.
- **Meta / checkpoint** pages — catalog and recovery metadata.

Row and index layouts are validated on read; checksum mismatch returns
`Status::kCorrupt` from `PagePool::ReadFrom` (see `docs/page_checksum.md`).

## PAX variant

Columnar PAX blocks for analytics experiments are documented in
`docs/pax_page_format.md`.

## Compatibility

This is an intentional destructive v1 format bump. Headerless v0 database
files are not accepted and there is no dual reader or downgrade path.
