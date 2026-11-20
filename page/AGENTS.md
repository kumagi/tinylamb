# `page/` — Layer 3 storage: Unbounded LSM Block & Tree Store

Unbounded row blocks + LSM-Tree engines (`tinylamb_page`, shared with
`recovery/` and `transaction/`).

## Key files

- `page.{hpp,cpp}` — `Page` facade over `RowPage` with per-block `shared_mutex`
  content latch (`PageRef` RAII).
- `page_manager.{hpp,cpp}` — manages row blocks (resident `blocks_` +
  evicted descriptors in `EvictedPageTable`) and LSM-Tree instances (`lsm_trees_`,
  each with `MemTable` + `committed_mem` + immutable disk-backed SSTable runs in
  `<db>.lsm.<gen>`), cold-page eviction to `<db>.blocks.<gen>` (`TLBK` records)
  with uncommitted overlays (`TUPO`) so dirty uncommitted pages can still evict,
  transaction commit baseline promotion (`ApplyTransactionCommit`), in-memory
  abort rollback (`ApplyTransactionAbort`), lock-free fuzzy checkpoints
  (`kFuzzyCheckpointVersion = 4`, `WriteFuzzyCheckpoint` / `LoadFuzzyCheckpoint`),
  and commit-filtered fuzzy crash recovery (`ReplayCommittedRecord`).
- `page_lsm_store.{hpp,cpp}` — OOC page/LSM primitives: `EvictedPageTable`
  (16-byte sparse two-level chunk descriptor), `BoundedFreeIdSet`
  (range-coalesced reusable page IDs), `LsmBlockCache` (byte-bounded LRU over
  decoded SSTable blocks under `kStorage`), `LsmRunCursor` (`~32 KiB` streaming
  cursor over `TLRB` SSTable blocks), and `MergeLsmSourcesToFd` (streaming K-way
  merge). See ADRs 0007 and 0009/0010.
- `page_ref.{hpp,cpp}` — move-only RAII: latches `Page` on construct and
  unlocks on destroy/`PageUnlock`.
- `row_page.{hpp,cpp}` — unbounded row block (`std::vector<std::string> slots_`
  + `committed_slots_` + `committed_lsns_`, up to `kMaxSlotsPerBlock = 4096`
  slots per block; dynamic strings with a byte admission budget for new writes).
- `row_position.hpp` — `RowPosition{page_id, slot}` row-lock and MVCC
  granularity unit.

Test: `./build/page_manager_test`.
