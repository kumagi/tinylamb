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

## 実装上の注意(2026-08-24 レビュー反映)

- タイムアウト後は abort すること。Insert 経路は物理挿入がロック取得に先行するため単純 retry は不可(ghost slot を作る)。
- 読み出し経路(MV2PL)は共有ロックを取得しない(AddReadSet のみ)。GetSharedLock は補助 API。
- 5秒は個々の取得試行の上限。継続読者トラフィック下では排他取得が繰り返し失敗し得る(待機者優先制御なし、wait-for graph なし)。
- 任意タイムアウトのオーバーロードあり。TryUpgradeLock は「共有保持者=自分1人かつ排他なし」でのみ成功。
- LockManager は RowPosition ハッシュで64 shard に分割済み(操作は単一 shard mutex、ネストなし)。release_epoch_ は進捗検出のため全局。
- 参照 improvement.md §7.1/M2.5 は git 履歴参照。
