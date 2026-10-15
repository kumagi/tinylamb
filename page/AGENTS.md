# `page/` — Layer 3 storage: pages + buffer pool

Fixed 32 KiB pages (`kPageSize`; 48 B header; `kPageBodySize` body).
One rank with `recovery/`+`transaction/` (single CMake target
`tinylamb_page`). Page-format changes also update `docs/page_format.md`
(and consider magic+version).

## Key files

- `page.{hpp,cpp}` — `Page{page_id,type,page_lsn,recovery_lsn,checksum +
  union body}` facade; dispatches Read/Insert/Update/Delete to Row/Leaf/Branch.
- `page_manager.{hpp,cpp}` — sole allocation API; page 0 is `MetaPage`
  (`meta_page.hpp`: `first_free_page`, `max_page_count`; `FreePage` freelist).
   Meta/free-list allocation undo is captured by v6 WAL; startup range-scan
   repair remains authoritative for legacy logs and committed destroys.
- `page_pool.{hpp,cpp}` — buffer pool: **LRU + global latch + per-stripe
  fast maps** (S3-FIFO is a *planned* migration, see `s3-fifo.md` — not the
  current policy). `GetPage(shared=false)` takes the exclusive latch.
  `WriteBack` invokes the WAL durability gate **outside** the pool latch
  (WAL-before-data); a successful write-back also raises the page's
  `recovery_lsn` (`ResetRecLSN`) so hot pages cannot pin WAL recycling.
  `WriteBackStaged(pid)` is the checkpoint-flush variant: the page latch is
  held only for the content snapshot (FPW emit + image encode), the gate and
  the pwrite run latch-free, and the end phase seals the page (checksum +
  `ResetRecLSN`) only when its `page_lsn` is unchanged, or re-anchors
  `recovery_lsn` at the staged LSN otherwise (`SetRecLSN` is sticky-min, so
  skipping the re-anchor would pin the recycle floor to a pre-flush record
  forever). A per-entry `written_page_lsn` counter under the IO latch keeps
  an older staged image from overwriting a newer completed one. This is what
  keeps a flush pass from blocking every transaction that needs a page being
  flushed (the latch-across-fsync wait manufactured write-intent
  escape-hatch timeouts in bursts under WAL write pressure).
  Full-page-image (FPW) emission: before a dirty page whose WAL snapshot is
  stale reaches the data file, `EmitSnapshotIfNeeded` logs a
  `kFullPageImage` (see `SetWalSnapshotSink`; rules in
  `docs/wal_format.md`). `SyncDataFile` retires exactly the images whose
  pwrite completed before the sync began; emitting, in-flight, and failed
  pwrites retain their images until a later successful pwrite and sync.
  `sync_state_mu_` is not held across the fallible data-file sync.
  `Page::IsValid()` ("this image is whole") and `Page::ChecksumMatches()`
  ("matches what the last write-back persisted", i.e. dirty) are deliberately
  different: recovery marks a page it rewrote with `MarkRepairedInMemory()`,
  so the two disagree until the page is actually written out. Treating a
  repaired page as torn makes `GetPage` return an empty ref and
  `BPlusTree::Open` silently bootstrap an empty tree.
  `GetPageForRecovery` returns corrupt images verbatim — recovery only, every
  other caller uses `GetPage`; a valid image carrying a different page id is
  converted to an invalid recovery sentinel.
- `page_ref.{hpp,cpp}` — move-only RAII: pin+latch on construct,
  unpin+unlock on destroy/`PageUnlock`. Never hold a raw `Page*`.
- `row_page.{hpp,cpp}` — slotted page: forward `rows_[0]` slot array of
  `RowPointer{offset,size}` (`row_pointer.hpp`), tuples grow from the back.
  `DeFragment`/`ReclaimUntilContiguous` heal fragmentation. All mutating ops
  take `Transaction&` (WAL linkage). Do not touch the `rows_[0]` indexing
  trick — `sizeof` shifts silently corrupt. Delete-space contract: a delete
  credits its bytes back to `free_size_` immediately but the hole KEEPS the
  row size, and `RowPage::Insert`'s admission subtracts the bytes of holes
  whose deleting transaction still holds the write intent — the deleter's
  abort undo must always be able to put its row back (a concurrent insert
  eating exactly those bytes inside the intent window used to kNoSpace the
  undo, fail-stop the engine, and wedge the reopen). `GetRow` reports holes
  as empty regardless of the stored size.
- `pax_page.{hpp,cpp}`, `pax_block.{hpp,cpp}`, `pax_layout.hpp` —
  non-transactional analytic path (`Store(DataChunk)`/`Load()` only;
  `kPlain/kDictionary/kBitPacked` column blocks). Row = OLTP writes,
  PAX = scan workload.
- `leaf_page`/`branch_page` + `foster_pair.hpp`/`index_key.hpp` — Foster
  B+Tree page layer (see `index/AGENTS.md`). `row_position.hpp`
  (`RowPosition{page_id,slot}`) is the row-lock granularity unit.

## Lock order (strict)

Page content latch → IO latch → pool_latch (short, shared) → shard mutex.
Never invert. `DropAllPages` waits for active writebacks, writes nothing back,
and retires entries to avoid use-after-free.

Test: `./build/page_pool_test`, `row_page_test`, `pax_page_test`,
`row_page_concurrent_test`, `page_manager_test`.
