# `page/` — Layer 3 storage: Unbounded LSM Block & Tree Store

Unbounded row blocks + LSM-Tree engines (`tinylamb_page`, shared with
`recovery/` and `transaction/`).

## Key files

- `page.{hpp,cpp}` — `Page` facade over `RowPage` with per-block `shared_mutex`
  content latch (`PageRef` RAII).
- `page_manager.{hpp,cpp}` — manages unbounded row blocks (`blocks_`) and
  LSM-Tree instances (`lsm_trees_`, each with `MemTable` + `committed_mem` +
  immutable `LsmSortedRun`s and background k-way merge compaction), plus
  transaction commit baseline promotion (`ApplyTransactionCommit`), in-memory
  abort rollback (`ApplyTransactionAbort`), lock-free fuzzy checkpoints
  (`WriteFuzzyCheckpoint` / `LoadFuzzyCheckpoint`), and commit-filtered fuzzy
  crash recovery (`ReplayCommittedRecord`).
- `page_ref.{hpp,cpp}` — move-only RAII: latches `Page` on construct and
  unlocks on destroy/`PageUnlock`.
- `row_page.{hpp,cpp}` — unbounded row block (`std::vector<std::string> slots_`
  + `committed_slots_` + `committed_lsns_`, up to `kMaxSlotsPerBlock = 4096`
  slots per block; dynamic strings with a byte admission budget for new writes).
- `row_position.hpp` — `RowPosition{page_id, slot}` row-lock and MVCC
  granularity unit.

Test: `./build/page_manager_test`.
