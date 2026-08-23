# Lock manager timeouts

`LockManager::GetExclusiveLock(row, wait=true)` waits up to **5 seconds** for
conflicting shared/exclusive holders to release, then returns `false`
(caller should abort / retry). Non-waiting mode (`wait=false`) is try-lock.

`GetSharedLock` remains non-blocking: if an exclusive lock is held it returns
`false` immediately (readers do not queue behind writers).

This prevents permanent hangs on simple 2-transaction exclusive conflicts.
It is **not** full deadlock detection (wait-for graph); nested multi-lock
cycles can still time out piecemeal.

See `improvement.md` §7.1 / §M2.5.
