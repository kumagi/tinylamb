# Page and persistence format

`tinylamb` keeps table rows in variable-capacity in-memory `RowPage` blocks
(`page/row_page.hpp`) and secondary/catalog indexes in `LsmIndex` trees
(`index/lsm_index.hpp`) managed by `PageManager` (`page/page_manager.hpp`).

## `PageType` (`page/page_type.hpp`)

- **`PageType::kUnknown` (0)** — uninitialized placeholder.
- **`PageType::kRowPage` (1)** — in-memory slotted row block (`RowPage`), linked
  into per-table doubly-linked chains (`prev_page_id_`, `next_page_id_`) with
  `page_lsn` and `rec_lsn` stamps.
- Retired B+Tree page types (`BranchPage`, `LeafPage`, `MetaPage`, `FreePage`)
  have been removed; all indexes and catalog maps are stored as LSM-Tree runs
  in `PageManager`.

## `<dbname>.db` snapshot format (`page/page_manager.cpp`)

When LSM compaction completes (or on explicit `WriteCheckpoint` / shutdown),
`PageManager::WriteFuzzyCheckpoint` atomically writes `<dbname>.db` via a
temporary file + `fsync` + `rename` with the following layout:

| Section | Fields | Purpose |
|---------|--------|---------|
| Header | `magic` (`kFuzzyCheckpointMagic` = `TLSMCKP1`), `version` (= 2), `replay_start_lsn`, `max_committed_txn_id` | Snapshot identity, WAL replay anchor LSN, and highest committed transaction ID |
| Pages | `max_pid`, `free_pages`, `num_pages` followed by `(page_id, prev_pid, next_pid, link_lsn, slots, slot_lsns)` | Committed state of all live `RowPage` blocks |
| Trees | `num_trees` followed by `(tree_id, compacted_entries...)` | Compacted committed state of all LSM-Tree indexes and catalog trees |
| Footer | `crc32c` | Whole-payload CRC-32C integrity check |

On startup, `RecoveryManager::RecoverFrom` loads `<dbname>.db` via
`PageManager::LoadFuzzyCheckpoint` and replays WAL segments starting from
`min(begin_lsn, min_dpt_rec_lsn, min_att_first_lsn)`.
