# Lock order

Acquire locks in **increasing** layer order. Never hold an inner lock while
waiting on an outer one.

```
1. Database / catalog latch (schema changes)
2. PagePool shard latch → per-page latch
3. B+ tree page latches (root → leaf, consistent with tree descent)
4. LockManager row/table locks (txn order: lower table id first on multi-table)
5. LSM memtable / run mutex (single writer per tree)
6. Logger buffer mutex (short critical sections only)
```

## Rules

- **Page before row**: pin the page, then take row-level locks if needed.
- **No lock while blocking on I/O**: release page latches before `pread`/`pwrite`
  miss paths where possible (PagePool miss path loads outside shard lock).
- **LockManager timeout**: waits use `kLockWaitTimeoutMs`; failure returns
  `Status::kTimeout` (see `docs/lock_timeout.md`).

## TSAN / deadlock tests

`lock_manager_test` and `page_pool` concurrency tests cover cross-thread
ordering; extend them when adding a new global latch.
