# Lock order

Acquire locks in **increasing** layer order. Never hold an inner lock while
waiting on an outer one.

```
1. Database / catalog latch (schema changes)
2. PagePool shard latch → per-page latch
3. B+ tree page latches (root → leaf, consistent with tree descent)
4. LockManager row/table locks (txn order: lower table id first on multi-table)
5. LSM memtable / run mutex (single writer per tree)
6. Logger enqueue latch → Logger work mutex (D1, see below)
```

## Rules

- **Page before row**: pin the page, then take row-level locks if needed.
- **Logger record atomicity (D1, `docs/design.md`)**: `AddLog` holds the
  enqueue latch until the whole record has landed in the ring buffer.  When
  the buffer is full the producer waits on the work condition variable **while
  keeping the latch**, ordering enqueue latch → work mutex.  The flush worker
  never acquires the enqueue latch, so the wait can never form a cycle.  A
  record's byte stream is never fragmented by another producer.
- **No lock while blocking on I/O**: release page latches before `pread`/`pwrite`
  miss paths where possible (PagePool miss path loads outside shard lock).
- **LockManager timeout**: waits use `kLockWaitTimeoutMs`; failure returns
  `Status::kTimeout` (see `docs/lock_timeout.md`).

## Known TSAN static lock-order reports (not deadlocks)

`page_pool_test` / `recovery_manager_test` under TSAN report
"lock-order-inversion" cycles that pair the PagePool **pool latch (M0)** with a
**per-page latch (M1)**:

1. `MetaPage::AllocateNewPage` (`meta_page.cpp:42`) is invoked by
   `PageManager::AllocateNewPage` while the CALLER holds the **meta page's own
   per-page latch (M1)**; inside, `pool.GetPage(new_page_id)` installs a fresh
   entry under the **pool latch (M0)** and constructs the new `PageRef`, whose
   constructor takes the new entry's per-page latch while M0 is still held
   (M1' under M0).
2. Conversely, a plain `GetPage` hit path pins under the **shard mutex** and
   then constructs `PageRef` (page latch) before any pool-latch acquisition.

These are **false positives by construction**: `GetPageImpl` releases
`pool_latch` (`latch.unlock()`) BEFORE constructing the returned `PageRef` on
every install path, so M0 and M1 are never held simultaneously by one thread.
TSAN observes the constructor stack inside the `GetPage` call frame and cannot
see the unlock.  Do not "fix" this by reordering the unlock later; keep the
invariant explicit:

- **Invariant**: `GetPageImpl` must unlock `pool_latch` before constructing
  the returned `PageRef` (or taking any per-page latch).  New install paths
  must keep this ordering.
- Pin counts use release/acquire (`ReleasePin` release fetch_sub, evictor
  recheck acquire load) so Entry destruction cannot race a concurrent unpin.

## TSAN / deadlock tests

`lock_manager_test` and `page_pool` concurrency tests cover cross-thread
ordering; extend them when adding a new global latch.
