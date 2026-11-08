# Lock order

Acquire locks in **increasing** layer order. Never hold an inner lock while
waiting on an outer one.

```
1. Database / catalog latch (schema changes)
2. PageManager `pages_mu_` (short map lookup/insert) → `Page` content latch
   (`PageRef`)
3. Index mutation gate(s) (`TransactionManager::AcquireIndexReadGuard` /
   `AcquireIndexWriteGuard`) → `PageManager` `trees_mu_` → per-tree
   `LsmTreeState::mutex`
4. Table-tail ownership mutex (`PageManager::table_tails_mu_`, leaf lock)
5. Logger enqueue latch → Logger work mutex (D1, see below)
6. MVCC version shard mutex (`VersionShard::mutex`) → transaction table lock
   (Wound-Wait preemption and the deadlock detector's victim wounding only).
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

## Rules

- **Write-intent before page latch on updates/deletes**: `Table::Update` and
  `Table::Delete` pre-acquire the MVCC write intent (`AddWriteSet`) before
  latching the target `RowPage` so contended row waits park latch-free.
  `RowPage::Insert` probes free slots under the page latch with non-blocking
  `TryAddWriteSet` and unlocks the page before `IndexInsert`.
- **Index gate before LSM tree mutex**: DDL takes all affected index-gate shards
  in sorted order before destroying LSM trees; readers hold a shared gate for
  their cursor lifetime.
- **Logger record atomicity (D1, `docs/design.md`)**: `AddLog` holds the
  enqueue latch until the whole record has landed in the ring buffer.  When
  the buffer is full the producer waits on the work condition variable **while
  keeping the latch**, ordering enqueue latch → work mutex.  The flush worker
  never acquires the enqueue latch, so the wait can never form a cycle.  A
  record's byte stream is never fragmented by another producer.
- **No page/tree lock while blocking on disk I/O**: `CheckpointManager` copies
  an in-memory snapshot of pages and LSM runs before writing `<db>.db` and
  waiting on WAL durability.
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

`transaction_test`, `transaction_extra_test`, and `page_manager_test` cover
cross-thread concurrency and lock ordering; extend them when adding a new latch.
