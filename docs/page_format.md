# Page and persistence format

`tinylamb` keeps table rows in variable-capacity in-memory `RowPage` blocks
(`page/row_page.hpp`) and secondary/catalog indexes in `LsmIndex` trees
(`index/lsm_index.hpp`) managed by `PageManager` (`page/page_manager.hpp`).

## `PageType` (`page/page_type.hpp`)

- **`PageType::kUnknown` (0)** — uninitialized placeholder.
- **`PageType::kRowPage` (1)** — in-memory slotted row block (`RowPage`), linked
  into per-table doubly-linked chains (`prev_page_id_`, `next_page_id_`) with
  a `page_lsn` stamp and per-row commit stamps (`committed_link_lsn_`,
  `committed_lsns_`).
- Retired B+Tree page types (`BranchPage`, `LeafPage`, `MetaPage`, `FreePage`)
  have been removed; all indexes and catalog maps are stored as LSM-Tree runs
  in `PageManager`.

## `<dbname>.db` snapshot format (`page/page_manager.cpp`)

When LSM compaction completes or an explicit `WriteCheckpoint` runs (shutdown does not write a final snapshot; the WAL is replayed on next open),
`PageManager::WriteFuzzyCheckpoint` atomically writes `<dbname>.db` via a
temporary file + `fsync` + `rename` with the following layout:

| Section | Fields | Purpose |
|---------|--------|---------|
| Header | In order: `magic` (8 bytes, `TLSMCKP1`), `version` (u32, 4), `replay_start_lsn` (u64), `max_committed_txn_id` (u64), `has_blocks_flag` (u32), `target_blocks_gen` (u64), `has_lsm_flag` (u32), `target_lsm_gen` (u64) | Snapshot identity, WAL replay anchor, transaction-ID high-water mark, and presence/generation of the blocks and LSM sidecar files |
| Pages | `max_pid`, `free_range_count` followed by coalesced `[lo, hi]` free ID ranges, then `[chunk_count > 0 ... 0]` streaming page batches encoding `kDiskBackedSlotTag` (referencing `<dbname>.blocks.<gen>` `{disk_offset, disk_length, file_gen}`), `kColumnarStubSlotTag`, or inline row-page state | Committed state of all live and evicted `RowPage` blocks bounded by chunk batching without holding a full-DB snapshot vector in RAM |
| Trees | `num_trees` followed by per-tree header (`tree_id`, persisted `<dbname>.lsm.<gen>` SSTable runs of ~32 KiB CRC-32C-protected blocks + sparse `LsmRunBlockMeta` directory, plus any unflushed inline committed/immutable entries) | Compacted and multi-run committed state of all LSM-Tree indexes and catalog trees |
| Footer | `crc32c` | Whole-payload standard CRC-32C (`Crc32C`: initial and final XOR `0xffffffff`) |

The header is 52 bytes; integers are written in native byte order. Older
versions are rejected with `Status::kCorrupt`. The transaction-ID high-water
mark can include aborted transactions and seeds the next ID. Each sidecar flag
is 0 or 1; when set, its generation identifies `<dbname>.blocks.<gen>` or
`<dbname>.lsm.<gen>`, respectively. The writer emits generation 0 when the
corresponding flag is clear; the loader limits the blocks generation to u32.

On startup, `RecoveryManager::RecoverFrom` loads `<dbname>.db` via
`PageManager::LoadFuzzyCheckpoint` and replays WAL segments starting from
`min(checkpoint_lsn, replay_start_lsn)` — the caller's checkpoint LSN and the
snapshot header's replay anchor, whichever is smaller (a zero caller value
takes the snapshot anchor).

## PAX chunk checksums

PAX chunks (`table/pax_chunk/pax_chunk_scan.cpp`) use a separate footer with
`crc_data` and `crc_footer`. These use `Crc32cOf`, whose result is the raw
Castagnoli register (initial `0xffffffff`, **no final XOR**), the complement
of standard CRC-32C. Do not use the snapshot's `Crc32C` convention for these
fields. `Crc32CTest.AuditPaxChecksumUsesRawRegisterConvention` pins the
known vector and incremental update behavior.

## TLCS v3 columnar table file format (`table/tlcs_file.{hpp,cpp}`)

`<db>.tlcs.<first_pid>` uses magic `TLCS0001` and `kFormatVersion = 3`. The
trailing `0001` in the magic is a historical label; the authoritative format
version is the `version` field in the header. `TlcsFile::Open` accepts v3 and
rejects older versions, including v2, per ADR 0011:

- **Fixed 72-byte Header (offset `0`)**: magic (`8B`), `version = 3`,
  `flags = 0`, `layout_generation`, `first_pid`, `total_rows`, `block_count`,
  `column_count`, `meta_dir_offset`, `meta_dir_length`, `meta_dir_crc32c`,
  and `hdr_crc32c`.
- **Interleaved Column Data & Paged Directory Groups**: every up-to-128 block
  batch writes column streams (`kFor`, `kRawDouble`, `kNarrowDict`,
  `kVarChar`, `kConstNull`) followed by an aligned `DirGroup` containing
  `[block_count * uint64_t stub_pids]` (patched during `FinalizeTlcsFile`)
  and `block_count` serialized `BlockDirEntry` payloads (`row_count`,
  `has_any_null`, per-column `ColumnMeta`).
- **Compact Meta Directory (at `meta_dir_offset`)**: stores column types,
  small dictionaries (`<= 254` entries), `dir_group_size (= 128)`,
  `group_count`, fixed-size `DirGroupMeta` summaries (`group_offset`,
  `block_count`, `pid_crc32c`, `body_size`, `body_crc32c`, `first_pid`,
  `last_pid`, `min_pid`, `max_pid`, `pids_monotonic`), and serialized column
  statistics, allowing large tables to open with `O(N / 128)` directory index
  metadata in RAM and fault `DirGroup` slices through a bounded LRU cache.
- **Column Synopsis Trailer (v3, at the end of the compact meta directory)**:
  for each column, `has_null`, `is_monotonic`, `is_unique_nonnull`,
  `is_identity_1_to_n` (one byte each, 0/1) followed by `min_int` and `max_int`
  (`int64_t` each). The reader gates this trailer on the header version
  (`version >= 3`), but `Open` currently accepts only v3, so every readable
  file carries it.
