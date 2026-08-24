# Page checksum (CRC-32C)

## Algorithm

On-disk pages carry a `uint64_t checksum` field in the page header
(`page/page.hpp`). As of the Phase-2 change in `improvement.md` §M2, the
value is **CRC-32C (Castagnoli)**, computed by `common/crc32c.hpp` over the
page image excluding the runtime-only `recovery_lsn` and `checksum` slots.
The 32-bit digest is stored zero-extended in the 64-bit checksum field.

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

## 実装上の注意(2026-08-24 レビュー反映)

- ダイジェスト範囲に recovery_lsn を含まない。リカバリ経路(GetPageForRecovery)は意図的に検証をバイパス。
- 全ゼロ領域/EINVAL/CRC==0 の境界挙動、uint64 フィールドに実効32bit値を格納している点、WriteBack 呼び出し元とエラー時挙動、bit-by-bit CRC の計算コスト、`SetChecksum`/`IsValid` の排他ラッチ前提契約——詳細は page.cpp/hpp のコメントと page_pool 実装を参照。
