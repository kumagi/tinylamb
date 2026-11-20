# Lock order

Acquire locks in **increasing** layer order. Never hold an inner lock while
waiting on an outer one.

```
1. Database / catalog latch (schema changes)
2. PageManager `blocks_mu_` (short map lookup/insert) → `Page` content latch
   (`PageRef`)
3. Index mutation gate(s) (`TransactionManager::AcquireIndexReadGuard` /
   `AcquireIndexWriteGuard`) → `PageManager` `trees_mu_` → per-tree
   `LsmTreeState::mutex`
4. Table-tail ownership mutex (`PageManager::table_tails_mu_`, leaf lock)
5. Logger enqueue latch → Logger work mutex (D1, see below)
6. MVCC version shard mutex (`VersionShard::mutex`) → transaction table lock
   (Wound-Wait preemption and the deadlock detector's victim wounding and the diagnostics listed below).
   Never the reverse: no path may acquire a version shard mutex while holding
   the transaction table lock.
6a. MVCC version shard mutex → page-chain shard mutex (leaf, see
    `TransactionManager::page_chain_shards_`): chain create/erase updates the
    per-page map under the storage shard's mutex. The page-chain shard mutex
    is never held while acquiring any other lock; lookups
    (`HasVersionChain`, `HasPageVersionChains`) take it alone.
7. MVCC version shard mutex → wait-for graph mutex (`AddWaitForEdge` under the
   shard lock; the detector snapshots the graph under the graph mutex alone
   and re-locks the table only to wound).
8. Transaction table lock → per-transaction `Transaction::chain_mu_` (the
   checkpoint's active-transaction snapshot iterates the registry and takes
   each transaction's chain mutex below it). WAL chain maintenance
   (`AppendLog`, `PreCommit`'s commit record, `Abort`'s terminator) takes
   ONLY the transaction's own chain mutex — never the table lock while
   holding it (registry cleanup happens after the mutex is released).
```

Timeout diagnostics (`RecordIntentWaitTimeout`) and reservation diagnostics
(`DebugDumpAllIndexReservations`) also take the transaction table lock below
a version/index reservation shard mutex. They follow the same direction as
rule 6; the reverse acquisition remains forbidden.

## Rules

- **Write-intent before page latch on updates/deletes**: `Table::Update` and
  `Table::Delete` pre-acquire the MVCC write intent (`AddWriteSet`) before
  latching the target `RowPage` so contended row waits park latch-free.
  `RowPage::Insert` probes free slots under the page latch with non-blocking
  `TryAddWriteSet` and unlocks the page before `IndexInsert`.
- **Index gate before LSM-Tree mutex**: DDL takes all affected index-gate shards
  in sorted order before destroying LSM-Trees; readers hold a shared gate for
  their cursor lifetime.
- **Logger record atomicity (D1, `docs/design.md`)**: `AddLog` holds the
  enqueue latch until the whole record has landed in the ring buffer.  When
  the buffer is full the producer waits on the work condition variable **while
  keeping the latch**, ordering enqueue latch → work mutex.  The flush worker
  never acquires the enqueue latch, so the wait can never form a cycle.  A
  record's byte stream is never fragmented by another producer.
- **No page/tree lock while blocking on disk I/O**: `CheckpointManager` and
  `PageManager` avoid holding page latches or `blocks_mu_` across
  `WaitForDurable` and large sequential companion-file I/O (`<db>.blocks.*`,
  `<db>.lsm.*`).
- **`blocks_mu_` vs companion-file / spill mutexes**:
  `PageManager::eviction_mu_` sits outside `blocks_file_mu_` / `lsm_file_mu_` /
  `blocks_mu_` / `trees_mu_` (`LoadFuzzyCheckpoint` acquires them in that order;
  never acquire `eviction_mu_` while holding any of those inner locks).
  `PageManager::blocks_file_mu_`, `PageManager::lsm_file_mu_`,
  `PageManager::uncommitted_spill_store_mu_`, `SpillStore::mu_`,
  `LsmBlockCache::mu_`, `TransactionManager::write_set_spill_mu_`, and
  `TransactionManager::version_spill_mu_` are leaf mutexes; never acquire
  `blocks_mu_` or any `PageRef` latch while holding them, and never acquire
  `Page` latches while holding `blocks_mu_`.
- **Uncommitted page overlay spill (`ADR 0007`)**: `EvictBatch` snapshots live
  page state under `Page::Latch()` (`try_to_lock`) + `blocks_mu_`, then writes
  to `uncommitted_spill_store_` after releasing both latches; final eviction
  commit re-acquires `Page::Latch()` (`try_to_lock`) -> `blocks_mu_` and
  verifies `live_seq_` / `committed_seq_`.
- **Table-tail ownership is a leaf lock**: `PageManager` may inspect or update
  table-tail state while holding a page content latch, but it must release that
  page latch before waiting on `table_tails_cv_`. Never hold `table_tails_mu_`
  while taking a page, transaction, index, or catalog latch. A pending
  tail owner is resolved only after commit or complete runtime undo.
- **Write-intent timeout**: waits use `GetWriteIntentWaitLimit()` (default 5 s,
  extended toward the 60 s durability floor during WAL stalls); on expiry the
  wait gives up and returns false / `Status::kConflicts` (see
  `docs/lock_timeout.md`).

## TSAN / deadlock tests

`transaction_test`, `transaction_extra_test`, and `page_manager_test` exercise
cross-thread concurrency; extend them when adding a new latch.

The default TSAN build runs with `detect_deadlocks=0`, so it finds data races
only and does **not** verify the acquisition order above.  Lock-order (ABBA)
inversions are checked only by configuring with
`-DTINYLAMB_ENABLE_TSAN=ON -DTINYLAMB_TSAN_DETECT_DEADLOCKS=ON`; the `tsan`
workflow's `lock-order` job does this (advisory until its baseline is clean).

## Memory Reservation & Non-Blocking Reclaim Rules (ADR 0003)

- Callers invoke `ProcessMemoryBudget::TryReserveWithReclaim` only while holding
  no lower-layer latches (`PageRef`, `PageManager::blocks_mu_`,
  `TableIncarnation::columnar_cache_->mu`, `TlcsFile::cache_mu_`,
  `TlcsFile::dir_cache_mu_`, `TlcsFile::resident_mu_`). Inside
  `blocks_mu_` or a `PageRef` latch, use non-reclaiming `TryReserve` (or
  `ReserveEmergency` for unavoidable small metadata).
- Reclaim callbacks registered on `ProcessMemoryBudget` (`ReclaimStage::kColumnar`
  on the `TlcsFile` decoded-block cache, `ReclaimStage::kCleanStorage` on
  `PageManager`'s evicted-page table; `ReclaimStage::kSpillableQuery` exists in
  the enum but currently registers no hook -- query-side release goes through
  the `QueryMemoryCoordinator` grant path) **must never block**: `ProcessMemoryBudget::hooks_mu_`
  snapshots hooks before invoking callbacks outside the lock, and `QueryMemoryCoordinator::mu_`
  coordinates grant waits without holding page or version locks. Every mutex/latch
  acquisition inside a reclaim hook must use `std::try_to_lock` (`try_lock`) and
  skip the target immediately if contended. Reclaim hooks must not re-enter
  `TryReserveWithReclaim`.
