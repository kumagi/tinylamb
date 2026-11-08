# Write-intent timeouts & deadlock handling

Row-level write serialization uses MVCC write intents
(`TransactionManager::AcquireWriteIntent` and
`TransactionManager::WaitForRowWriteIntent` in
`transaction/transaction_manager.hpp`, sharded across 256 `VersionShard`s;
`LockManager` is retired). Waiting mode (`wait=true`) blocks up to
`TINYLAMB_WRITE_INTENT_WAIT_MS` (default **5 seconds**, stretched toward
`kDurabilityIntentWaitFloor` = 60 seconds while a WAL durability barrier is in
flight) for a conflicting intent holder to commit or abort. Non-waiting mode
(`wait=false`, used by `TryAddWriteSet`) is a try-lock.

`DeadlockPolicy::kNoWait` also makes the transaction manager's page-specific
and all-index undo-reservation drains immediate checks. The byte reservation
and page admission checks remain active: operations that fit may proceed;
operations requiring a foreign reservation to drain return a retryable conflict.

Snapshot reads never acquire row locks or block behind write intents: they
resolve the version visible at `txn.SnapshotTimestamp()` through the MVCC
version chain.

Write-intent acquisition (`TransactionManager::AcquireWriteIntent`) supports
four deadlock policies (`DeadlockPolicy`: `kLegacy` 5ms abort,
`kWaitDie`/`kWoundWait` timestamp ordering, `kDeadlockDetect` background
wait-for graph with youngest-victim wounding); see
`transaction/transaction_manager.hpp` and `docs/lock_order.md`.
**The default is `kDeadlockDetect`**: the wait-for detector is
event-driven (sleeps until a wait-for edge exists) and resolves a cycle by
wounding the youngest participant within one notification. `kLegacy` remains
selectable explicitly for benchmarks.

## Write-intent wait failure classification

Every failed write-intent wait records *why* it gave up —
`Transaction::IntentWaitFailure`: `kConflict` (try-mode contention),
`kWounded` (wounded by the deadlock detector or a waiter-die timestamp
rule), `kHolderAborting` (the conflicting holder started aborting),
`kTimeout` (the 5-second wait ceiling), or `kFailStopped` (the manager is
shutting down). `executor/index_scan.cpp` surfaces these as distinct
messages.

When the conflicting holder is itself aborting, waiters give up after
`kHolderAbortGiveupDelay` (250 ms, `transaction/transaction_manager.cpp`)
instead of waiting out the full 5 seconds: the holder's undo walk may need
the very page latch a waiter holds, so a shorter give-up breaks the latch
cycle. A `kDeadlockDetect` give-up also removes its own wait-for edge so a
stale edge cannot fabricate a phantom cycle.

## Implementation notes

- On write-intent conflict or timeout, the caller unwinds and aborts the
  transaction (or compensates the in-flight statement).
- The 5-second wait ceiling (`TINYLAMB_WRITE_INTENT_WAIT_MS`) resets whenever
  the intent hands over to a new holder (`check_holder_progress`), so deep
  queues that make forward progress do not time out.
- Version chains and write intents are sharded across 256 `VersionShard`s by
  `(page_id * 131 + slot) % 256`.
