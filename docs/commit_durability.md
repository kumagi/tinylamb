# Commit durability

## Policy

`TransactionManager::PreCommit` appends a commit log record via `Logger::AddLog`,
then by default **waits until that LSN is durable** (`Logger::WaitForDurable`).

- **Default (`synchronous_commit = true`)**: commit success means the commit
  record has been written and `fdatasync`'d (group commit: the logger worker
  batches syncs up to ~10ms, then advances `DurableLSN` and wakes waiters).
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
