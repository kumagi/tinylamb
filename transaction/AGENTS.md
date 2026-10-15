# `transaction/` — Layer 3 storage: MVCC + locks + txn lifecycle

Strict-2PL + MVCC version chains + write intents. Normative doc:
`docs/lock_order.md` (+ `docs/commit_durability.md`, `docs/lock_timeout.md`).

## Key files

- `transaction.{hpp,cpp}` — `Transaction{txn_id, snapshot, read/write sets,
  prev_lsn chain, status}`. Emits Insert/Update/Delete + CLR log records;
  visibility and durability are separate (dependent durable-LSN barrier
  before commit). The leaf/branch log emitters
  (`DeleteLeafLog`/`DeleteBranchLog`/`UpdateLeafLog`/`Set{Low,High}Fence`/
  `SetFoster`) also **register an index-undo space reservation**
  (`ReserveIndexUndoBytes`): the bytes the record's UNDO will re-consume stay
  out of OTHER transactions' admissions until this transaction resolves
  (the Leaf/Branch twin of the RowPage hole reservation — without it a
  concurrent insert admitted on the freed bytes makes the abort undo
  physically impossible, fail-stops the runtime abort, and wedges the
  restart's loser undo against the same page). Own reservations never block
  own operations (the reverse-chain undo frees a later op's bytes before an
  earlier op's undo re-claims them).
- `transaction_manager.{hpp,cpp}` — `Begin(read_only)` / `PreCommit` /
  commit (`COMMIT` record → `WaitForDurable` → unlock) / abort (backward
  chain walk + CLR, idempotent). All locks held to commit/abort (Strict 2PL).
  `synchronous_commit_` switch; deadlock policy selectable
  (`kLegacy/kWaitDie/kWoundWait/kDeadlockDetect`; **default
  `kDeadlockDetect`** — event-driven wait-for detector, spawns with the
  manager, wounds the youngest cycle participant; `kLegacy` is the old
  5ms-timeout behavior kept for benchmark comparability). The write-intent
  wait's latch-cycle escape hatch (a waiter that holds a page latch its
  blocked holder needs) gives up after
  `TINYLAMB_WRITE_INTENT_WAIT_MS` (default 5000); every give-up bumps
  `RuntimeStats().write_intent_wait_timeouts` — surfaced by the TPC-C
  benchmark as `mvcc.write_intent.wait_timeouts` — and logs the row
  rate-limited, so a climbing gauge exposes a latch-cycle hot spot.
  **The wait deadline (and the 250ms holder-abort grace) restart on every
  intent handover**: on a contended hot row the waiters queue, and each
  handover is progress — only a single holder that never moves rides the
  hatch to its limit. `TINYLAMB_INTENT_NO_PROGRESS_RESET=1` restores the
  entry-time deadline for A/B comparisons.
  **The escape hatch is durability-aware**: while a WAL durability barrier
  is in flight (commit fsync, write-back gate — counted by
  `DurabilityBarrierGuard` around `WaitForWALDurable`) or the group-commit
  backlog exceeds 1 MiB, an expired hatch deadline extends toward
  `kDurabilityIntentWaitFloor` (60 s) instead of firing, the twin of
  `LockManager::kDurabilityWaitFloor`. A stalled fsync slows holders
  without deadlocking them, and timing their waiters out manufactured abort
  cascades whenever a checkpoint's data-file sync stretched every fsync
  (measured on TPC-C: bursts of 5 s escapes and a 42% throughput crash
  exactly while a checkpoint flushed; after the fix 71 timeouts in 4
  minutes dropped to 6 — the genuine latch cycles).
  MVCC version chains live
  in 256 shards; the background GC worker sweeps small version spaces in
  one full pass and large ones in rotating shard windows (adaptive to
  `kGcFullScanChainLimit`), so GC's shard-lock interference stays bounded
  no matter how many chains accumulate over a long uptime. `RuntimeStats()`
  carries long-uptime gauges (active transactions, retained version chains,
  oldest-snapshot lag); `GarbageCollectVersions` rate-limits a WARN when a
  live snapshot pins a large chain backlog.
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
