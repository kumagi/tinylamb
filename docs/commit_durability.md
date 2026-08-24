# Commit durability

## Policy

`TransactionManager::PreCommit` appends a commit log record via `Logger::AddLog`,
then by default **waits until that LSN is durable** (`Logger::WaitForDurable`).

- **Default (`synchronous_commit = true`)**: commit success means the commit
  record has been written and `fdatasync`'d (group commit: the logger worker
  batches syncs for the configured interval (1ms by default), then advances
  `DurableLSN` and wakes waiters).
- **`synchronous_commit = false`**: PreCommit returns after enqueueing the
  commit record (previous behavior). A crash within the sync interval can lose
  “committed” transactions. Use only for benchmarks that explicitly accept
  that trade-off.

Set via `TransactionManager::SetSynchronousCommit(bool)` /
`Logger` is shared; the wait uses `Logger::WaitForDurable(lsn)`.

## Trade-off

| Mode | Crash safety | Commit latency |
|------|--------------|----------------|
| sync on (default) | Commit survives process crash after success | Up to ~group-commit interval |
| sync off | May lose recent commits | Lower p99 / higher TPC-C tps |

TPC-H read-mostly workloads are barely affected; TPC-C write throughput will
drop when enabling sync if the previous baseline assumed async commit.

See also `improvement.md` §6.1 / §M2.3–M2.4.

## 実装上の注意(2026-08-24 レビュー反映)

- 待機点: `WaitForDurable(BufferedLSN())` — commit レコード終端が durable になるまで待つ(AddLog 戻り値=先頭LSNではない)。Abort 経路も同様に終端待ちとし、ReadLog 失敗時は undo 打ち切り(WARNING)。
- read-only txn: ログ書込も durability 待ちも発生しない。
- 可視化はメモリ上で即時(CommitVersions)、durability 確定はその後(既定で最大約1ms)。クラッシュ時は未flushコミット消失=可視状態と整合。
- 設定: `TransactionManager::SetSynchronousCommit(bool)` は TM インスタンス(DB)単位。リポジトリ内に async 化の呼び出し箇所は現状なし。
- チェックポイント: 周期実行は `CheckpointManager::Start()` 呼び出しが現行起動パスになく無効。マスターレコード(.last_checkpoint)は boot 時に読み取り+妥当性検証(probe)して利用。リカバリ基本は WAL 全スキャン。
- sync間隔は `Logger` の `every_ms` で指定し、既定値は1ms（0は1msへclamp）。macOS は F_FULLFSYNC。
- LSN 3種: CommittedLSN=write済み(fsync未)、DurableLSN=fdatasync済み、BufferedLSN=バッファ末尾。durable 判定に使うのは DurableLSN/WaitForDurable のみ。
- 参照 improvement.md §M2.x は git 履歴参照(本書は当該対策の設計記録)。
