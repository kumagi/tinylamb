# Lock order

Acquire locks in **increasing** layer order. Never hold an inner lock while
waiting on an outer one.

```
1. Database / catalog latch (schema changes)
2. PagePool target-page content latch → IO latch → pool_latch (short, shared)
   → shard mutex
3. Index mutation gate(s) → B+ tree page latches (root → leaf, consistent with tree descent)
4. LockManager row locks (auxiliary, test-facing; primary conflict control is
   MVCC write-intent via `TransactionManager::AcquireWriteIntent`)
5. LSM memtable / run mutex (single writer per tree)
6. Logger enqueue latch → Logger work mutex (D1, see below)
7. MVCC version shard mutex → transaction table lock (Wound-Wait preemption
   and the deadlock detector's victim wounding only). Never the reverse:
   no path may acquire a version shard mutex while holding the transaction
   table lock.
7a. MVCC version shard mutex → page-chain shard mutex (leaf, see
   `TransactionManager::page_chain_shards_`): chain create/erase updates the
   per-page map under the storage shard's mutex. The page-chain shard mutex
   is never held while acquiring any other lock; lookups
   (`HasVersionChain`, `HasPageVersionChains`) take it alone.
8. MVCC version shard mutex → wait-for graph mutex (AddWaitForEdge under the
   shard lock; the detector snapshots the graph under the graph mutex alone
   and re-locks the table only to wound).
9. Transaction table lock → per-transaction `Transaction::chain_mu_` (the
   checkpoint's active-transaction snapshot iterates the registry and takes
   each transaction's chain mutex below it). WAL chain maintenance
   (`AppendLog`, `PreCommit`'s commit record, `Abort`'s terminator) takes
   ONLY the transaction's own chain mutex — never the table lock while
   holding it (registry cleanup happens after the mutex is released). This
   mutex also makes {record append, `prev_lsn_`, status flip} atomic with
   respect to that snapshot: a snapshot that read `kRunning` while the
   commit record sat below the checkpoint's begin LSN made restart recovery
   roll back a durably committed transaction.
```

## Rules

- **Page before row**: pin the page, then take row-level locks if needed.
- **Index gate before tree pages**: DDL takes all affected index-gate shards
  in sorted order before collecting or destroying B+Tree pages; readers hold a
  shared gate for their cursor lifetime.
- **Logger record atomicity (D1, `docs/design.md`)**: `AddLog` holds the
  enqueue latch until the whole record has landed in the ring buffer.  When
  the buffer is full the producer waits on the work condition variable **while
  keeping the latch**, ordering enqueue latch → work mutex.  The flush worker
  never acquires the enqueue latch, so the wait can never form a cycle.  A
  record's byte stream is never fragmented by another producer.
- **No lock while blocking on I/O**: release page latches before `pread`/`pwrite`
  miss paths where possible (PagePool miss path loads outside shard lock).
- **PagePool durability bookkeeping mutexes are below the IO latch**: the
  write-back path takes page content latch → IO latch (`io_latches_[shard].mu`)
  → `sync_state_mu_` (the `unsynced_pages_` / pwrite-serial map) and, nested
  only under it, `snapshot_mu_` (the retained FPW LSN map). Both are released
  before the durability gate or the WAL snapshot sink runs (they block on
  fsync/logging); no path takes `pool_latch`, a page latch, or an IO latch
  while holding either. `writeback_wait_mu_`/`writeback_cv_`
  (`DropAllPages`/`~PagePool`) wait for the active writeback/eviction/load
  counters with no other lock held.
- **Table-tail ownership is a leaf lock**: `PageManager` may inspect or update
  table-tail state while holding a page content latch, but it must release that
  page latch before waiting on `table_tails_cv_`. Never hold `table_tails_mu_`
  while taking a page, meta, transaction, index, or catalog latch. A pending
  tail owner is resolved only after commit or complete runtime undo.
- **LockManager timeout**: waits use `ExclusiveWaitTimeout()` (5 s, extended
  toward the 60 s durability floor during WAL stalls); on expiry the wait
  gives up and the acquire returns `false` (see `docs/lock_timeout.md`).

## Page allocation and the pool latch (two-phase by design)

`PageManager::AllocateNewPage` (`page/page_manager.cpp`) never holds the meta
latch across pool operations.  Allocation is two-phase via
`MetaPage::PeekAllocationCandidate` / `AllocateCandidate` / `AllocateFreshId`
(`page/meta_page.hpp:60-74`): the candidate is picked under one meta latch
acquisition, the latch is released when the scoped `PageRef` ends, then the
candidate page is latched with `pool_->GetPage`, and a second meta latch
acquisition revalidates and pops the candidate.  Quoting the rationale
comment in `page_manager.cpp`: "the runtime undo of an aborted destroy
latches the candidate page first and takes the meta latch second
(`LogUndoWithPage` -> `PopFreePageHead`), so holding meta across GetPage here
would give the two paths opposite orders and let an allocator deadlock
against an aborting destructor."  A loser of a race just retries after
re-validation under the re-taken latch.  Because the meta latch is never held
while acquiring a pool or per-page latch, no ordering violation exists on
these paths.

## TSAN / deadlock tests

`lock_manager_test` and `page_pool` concurrency tests cover cross-thread
ordering; extend them when adding a new global latch.

### Recovery restart: phase-serialized latching (known TSAN graph cycle)

Restart recovery mixes two latch disciplines across sequential phases inside
`RecoveryManager::RecoverFrom`:

1. The dirty-page batch (pin every DPT page, meta last) is latched, analyzed,
   and released (`pages.clear()`) *before* parallel replay starts.
2. `ReplayPagesInParallel` workers latch their target page first and the meta
   page second (`PageManager::ReplayAllocation -> GetMetaPage`).
3. The torn-page sweep and the free-list rebuild walk pages 1..high_water
   transiently *after* the workers are joined.

Because each phase releases everything before the next begins, no two of
these orders can be active at once, so there is no runtime deadlock. TSAN's
process-wide lock-order graph, however, combines the edges across phases and
reports `lock-order-inversion` against `PageRef::PageRef`
(`postgres_session_fuzzer_replay_test` under TSAN, exit code 66). Any change
that lets two phases overlap — or keeps a PageRef alive across
`pages.clear()` — turns the graph cycle into a real one; restructure toward a
single global order (target -> meta everywhere) if recovery latching is ever
revisited.
