# Transaction

This directory is responsible for managing transactions, which are the fundamental units of work in the database. It ensures that all operations within a transaction are atomic, consistent, isolated, and durable (ACID), which are the core guarantees of a reliable database system. This is achieved through a combination of concurrency control and logging mechanisms.

## Core Components

- **`Transaction`**: Represents a single transaction in the system. It encapsulates the state of a transaction, including its unique `txn_id`, its current `status` (`kRunning`, `kCommitted`, `kAborted`), and the sets of rows that have been read or written to (`read_set_` and `write_set_`).
  - **Lifecycle**: A transaction is created in the `kRunning` state. It can then be either committed by calling `PreCommit()` or rolled back by calling `Abort()`.
  - **Logging**: The `Transaction` class is responsible for generating log records for all its operations. It provides methods like `InsertLog`, `UpdateLog`, and `DeleteLog` that create the appropriate `LogRecord` and pass it to the `Logger`. Each log record is linked to the previous record from the same transaction, forming a backward chain that is essential for rollbacks.

- **`TransactionManager`**: The central component that manages the lifecycle of all transactions in the system.
  - **Transaction Creation**: It is responsible for creating new `Transaction` objects via its `Begin()` method, assigning them a unique `txn_id`.
  - **Active Transaction Table**: It maintains a table of all currently active transactions, which is crucial for recovery and for managing concurrent operations.
  - **Commit and Abort**: It orchestrates the commit and abort processes. When a transaction is committed, it writes a `COMMIT` record to the log. When a transaction is aborted, it walks the same chain to compensate every change with a CLR and then appends a `COMMIT`-typed terminator (there is no `ABORT` record type) so restart recovery classifies the transaction as finished.
  - **Compensation Log Records (CLRs)**: During an abort, for each change that is undone, the `TransactionManager` writes a Compensation Log Record (CLR) to the log. CLRs describe the undo operation and are essential for ensuring that the rollback process itself is idempotent and can survive a crash.

- **`LockManager`**: An auxiliary row-lock table (Strict 2PL semantics: shared and exclusive locks per `RowPosition`). It is **not** the primary isolation mechanism — conflict control between writers is handled by the MVCC version store and per-row write intents in the `TransactionManager` (see `transaction/AGENTS.md` and `docs/lock_order.md`). The `LockManager` exists for test-facing locking semantics and for callers that need explicit row locks; the `TransactionManager` itself never takes or releases `LockManager` locks (unlocking is the caller's job).

## Workflow

The typical lifecycle of a transaction is as follows:

1.  A client requests a new transaction from the `TransactionManager` by calling `Begin()`.
2.  The `TransactionManager` creates a new `Transaction` object and adds it to the active transaction table.
3.  The client then performs a series of read and write operations. For each operation:
    -   Reads go through the MVCC version store, which selects the version visible to the transaction's snapshot.
    -   Writes first acquire a per-row write intent (`AddWriteSet`); conflicts are resolved by the configured deadlock policy (wait-die, wound-wait, detection) and the executor unwinds the transaction on a lost race.
    -   The transaction generates a log record for the operation and appends it to the log.
4.  When the client is finished, it calls either `PreCommit()` or `Abort()` on the `Transaction` object.
    -   **Commit**: The `TransactionManager` writes a `COMMIT` record to the log, publishes the staged versions, and releases the write intents. `LockManager` locks, if any were taken, are released by the caller.
    -   **Abort**: The `TransactionManager` walks the log chain, writes a CLR for each change, releases the write intents, marks the transaction aborted, and appends a `COMMIT`-typed terminator.

This combination of locking for concurrency control and logging for durability and atomicity ensures that the database operates reliably and maintains data consistency even in the presence of concurrent access and system failures.