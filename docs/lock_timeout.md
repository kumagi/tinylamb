# Lock manager timeouts

`LockManager::GetExclusiveLock(row, wait=true)` waits up to **5 seconds** for
conflicting shared/exclusive holders to release, then returns `false`
(caller should abort / retry). Non-waiting mode (`wait=false`) is try-lock.

`GetSharedLock` remains non-blocking: if an exclusive lock is held it returns
`false` immediately (readers do not queue behind writers).

This prevents permanent hangs on simple 2-transaction exclusive conflicts.
It is **not** full deadlock detection (wait-for graph); nested multi-lock
cycles can still time out piecemeal.

Write-intent acquisition (`TransactionManager::AcquireWriteIntent`) has its
own deadlock policies (`DeadlockPolicy`: `kLegacy` 5ms abort,
`kWaitDie`/`kWoundWait` timestamp ordering, `kDeadlockDetect` background
wait-for graph with youngest-victim wounding); see
`transaction/transaction_manager.hpp` and `docs/lock_order.md` §7-8.
**The default is `kDeadlockDetect`** (2026-09-28): the wait-for detector is
event-driven (sleeps until a wait-for edge exists) and resolves a cycle by
wounding the youngest participant within one notification. TPC-C W=2
measurements: at 32 clients the previous `kLegacy` default accumulated
12x more engine aborts (each a full undo walk with CLR WAL records) and
~30% less throughput; at 64 clients legacy reached 48,533 aborts vs 1,435
under detection. `kLegacy` remains selectable explicitly for benchmarks.

## Write-intent wait failure classification (2026-10-01)

Every failed write-intent wait records *why* it gave up —
`Transaction::IntentWaitFailure`: `kConflict` (try-mode contention),
`kWounded` (wounded by the deadlock detector or a waiter-die timestamp
rule), `kHolderAborting` (the conflicting holder started aborting),
`kTimeout` (the 5-second wait ceiling), or `kFailStopped` (the manager is
shutting down). `executor/index_scan.cpp` surfaces these as distinct
messages; the pre-classification blanket "write intent wait timed out"
text was misleading — real 5-second timeouts were rare, with try-mode
conflicts and wounds dominating.

When the conflicting holder is itself aborting, waiters give up after
`kHolderAbortGiveupDelay` (250 ms, `transaction/transaction_manager.cpp`)
instead of waiting out the full 5 seconds: the holder's undo walk may need
the very page latch a waiter holds, so a shorter give-up breaks the latch
cycle. The 250 ms grace period is load-bearing — without it, the strict-2PL
"wait completes before the holder's intent disappears" contract breaks and
`DeadlockDetectorBreaksThreeTransactionCycle` flakes. A `kDeadlockDetect`
give-up must also remove its own wait-for edge, or the stale edge fabricates
a phantom cycle.

## 実装上の注意(2026-08-24 レビュー反映)

- タイムアウト後は abort すること。Insert 経路は物理挿入がロック取得に先行するため単純 retry は不可(ghost slot を作る)。
- 読み出し経路(MV2PL)は共有ロックを取得しない(AddReadSet のみ)。GetSharedLock は補助 API。
- 5秒は個々の取得試行の上限。継続読者トラフィック下では排他取得が繰り返し失敗し得る(待機者優先制御なし、wait-for graph なし)。
- 任意タイムアウトのオーバーロードあり。TryUpgradeLock は「共有保持者=自分1人かつ排他なし」でのみ成功。
- LockManager は RowPosition ハッシュで64 shard に分割済み(操作は単一 shard mutex、ネストなし)。release_epoch_ は進捗検出のため全局。
