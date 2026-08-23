# Page checksum (CRC-32C)

## Algorithm

On-disk pages carry a `uint64_t checksum` field in the page header
(`page/page.hpp`). As of the Phase-2 change in `improvement.md` §M2, the
value is **CRC-32C (Castagnoli)**, computed by `common/crc32c.hpp` over the
full `kPageSize` bytes with the checksum field itself forced to zero during
the digest.

`Page::SetChecksum()` writes the digest; `Page::IsValid()` recomputes and
compares. `PagePool::WriteBack` always refreshes the checksum before I/O.
`PagePool::ReadFrom` verifies after a successful full-page read and throws
`std::runtime_error` (logged with `Status::kCorrupt`) on mismatch.

Short reads / past-EOF materialize an in-memory free page without a stored
checksum (checksum stays 0 until the first write-back).

## Format compatibility

This **replaces** the previous `std::hash<Page>` digest, which was weak and
implementation-defined. There is **no automatic migration**:

- Existing `.db` files written with `std::hash` checksums will fail to open
  once a page is read (`corrupt page checksum`).
- Regenerate fixtures (TPC-H `--reuse-database` only after a fresh load, TPC-C
  reload, tests that create temp DBs).
- Treat this as an on-disk format bump for engineering builds; no separate
  magic/version field was added yet (follow-up: optional page format version
  in meta page).

## Why CRC-32C

- Stable across compilers and platforms (unlike `std::hash`).
- Detects torn writes and random bit flips better than additive header hashes.
- Hardware CRC32C can replace the software loop later without changing the
  on-disk value for the same bytes.
