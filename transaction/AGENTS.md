# `transaction/` — Layer 3 storage: MVCC + locks + txn lifecycle

Strict-2PL + MVCC version chains + write intents. Normative doc:
`docs/lock_order.md` (+ `docs/commit_durability.md`, `docs/lock_timeout.md`).

## Key files

- `transaction.{hpp,cpp}` — `Transaction{txn_id, snapshot, read/write sets,
  prev_lsn chain, status}`. Emits Insert/Update/Delete + CLR log records;
  visibility and durability are separate (dependent durable-LSN barrier
  before commit).
- `transaction_manager.{hpp,cpp}` — `Begin(read_only)` / `PreCommit` /
  commit (`COMMIT` record → `WaitForDurable` → unlock) / abort (backward
  chain walk + CLR, idempotent). All locks held to commit/abort (Strict 2PL).
  `synchronous_commit_` switch; deadlock policy selectable
  (`kLegacy/kWaitDie/kWoundWait/kDeadlockDetect`).
- `lock_manager.{hpp,cpp}` — **row locks on `RowPosition{page_id,slot}`**,
  64 shards, one shard mutex per op (no cross-shard ordering hazard).
  Asymmetric by design: `GetSharedLock` never waits (caller retries on
  false); `GetExclusiveLock` waits up to `kExclusiveWaitTimeout{5s}`
  (raised to `kDurabilityWaitFloor{60s}` while a durability wait is in
  flight). Owner-verified release/upgrade — one txn can never drop or
  promote another's lock.

## Three things you must not conflate

Row **latch** (`PagePool`) vs row **lock** (`LockManager`) vs MVCC
**write-intent** (version chain). Canonical flow:
`Begin → lock → WAL → PreCommit/Abort → unlock`.

Test: `./build/transaction_test`, `lock_manager_test`,
`transaction_extra_test`.
