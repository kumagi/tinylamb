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
  Meta/free-list updates are **outside WAL** → rebuilt at startup.
- `page_pool.{hpp,cpp}` — buffer pool: **LRU + global latch + per-stripe
  fast maps** (S3-FIFO is a *planned* migration, see `s3-fifo.md` — not the
  current policy). `GetPage(shared=false)` takes the exclusive latch.
  `WriteBack` invokes the WAL durability gate **outside** the pool latch
  (WAL-before-data). `GetPageForRecovery` returns corrupt images verbatim —
  recovery only, every other caller uses `GetPage`.
- `page_ref.{hpp,cpp}` — move-only RAII: pin+latch on construct,
  unpin+unlock on destroy/`PageUnlock`. Never hold a raw `Page*`.
- `row_page.{hpp,cpp}` — slotted page: forward `rows_[0]` slot array of
  `RowPointer{offset,size}` (`row_pointer.hpp`), tuples grow from the back.
  `DeFragment`/`ReclaimUntilContiguous` heal fragmentation. All mutating ops
  take `Transaction&` (WAL linkage). Do not touch the `rows_[0]` indexing
  trick — `sizeof` shifts silently corrupt.
- `pax_page.{hpp,cpp}`, `pax_block.{hpp,cpp}`, `pax_layout.hpp` —
  non-transactional analytic path (`Store(DataChunk)`/`Load()` only;
  `kPlain/kDictionary/kBitPacked` column blocks). Row = OLTP writes,
  PAX = scan workload.
- `leaf_page`/`branch_page` + `foster_pair.hpp`/`index_key.hpp` — Foster
  B+Tree page layer (see `index/AGENTS.md`). `row_position.hpp`
  (`RowPosition{page_id,slot}`) is the row-lock granularity unit.

## Lock order (strict)

IO latch → pool_latch (short, shared) → shard mutex. Never invert.
`DropAllPages` writes nothing back; retires entries to avoid use-after-free.

Test: `./build/page_pool_test`, `row_page_test`, `pax_page_test`,
`row_page_concurrent_test`, `page_manager_test`.
