# Transaction

This directory is responsible for managing transactions, which are the fundamental units of work in the database. It ensures that all operations within a transaction are atomic, consistent, isolated, and durable (ACID), which are the core guarantees of a reliable database system. This is achieved through a combination of concurrency control and logging mechanisms.

## Core Components

- **`Transaction`**: Represents a single transaction in the system. It encapsulates the state of a transaction, including its unique `txn_id`, its current `status` (`kRunning`, `kCommitted`, `kAborted`), and the sets of rows that have been read or written to (`read_set_` and `write_set_`).
  - **Lifecycle**: A transaction is created in the `kRunning` state. It can then be either committed by calling `PreCommit()` or rolled back by calling `Abort()`.
  - **Logging**: The `Transaction` class is responsible for generating log records for all its operations. It provides methods like `InsertLog`, `UpdateLog`, and `DeleteLog` that create the appropriate `LogRecord` and pass it to the `Logger`. Each log record is linked to the previous record from the same transaction, forming a backward chain that is essential for rollbacks.

- **`TransactionManager`**: The central component that manages the lifecycle of all transactions in the system.
  - **Transaction Creation**: It is responsible for creating new `Transaction` objects via its `Begin()` method, assigning them a unique `txn_id`.
  - **Active Transaction Table**: It maintains a table of all currently active transactions, which is crucial for recovery and for managing concurrent operations.
  - **Commit and Abort**: It orchestrates the commit and abort processes. When a transaction is committed, it writes a `COMMIT` record to the log. When a transaction is aborted, it uses the chain of log records to undo all the changes made by that transaction.
  - **Compensation Log Records (CLRs)**: During an abort, for each change that is undone, the `TransactionManager` writes a Compensation Log Record (CLR) to the log. CLRs describe the undo operation and are essential for ensuring that the rollback process itself is idempotent and can survive a crash.

- **`LockManager`**: Implements the concurrency control mechanism, which is responsible for ensuring that concurrent transactions do not interfere with each other, thus providing isolation.
  - **Locking Protocol**: It uses a locking protocol (likely Strict Two-Phase Locking, or Strict 2PL) to manage access to data. Before a transaction can read or write to a row, it must first acquire the appropriate lock.
    - **Shared Locks (S-locks)**: Multiple transactions can hold a shared lock on the same data item simultaneously, allowing for concurrent reads.
    - **Exclusive Locks (X-locks)**: Only one transaction can hold an exclusive lock on a data item at a time. An exclusive lock is required for write operations.
  - **Deadlock Detection/Prevention**: While not explicitly detailed in the headers, a complete `LockManager` would also be responsible for handling deadlocks, which can occur when two or more transactions are waiting for each other to release locks.

## Workflow

The typical lifecycle of a transaction is as follows:

1.  A client requests a new transaction from the `TransactionManager` by calling `Begin()`.
2.  The `TransactionManager` creates a new `Transaction` object and adds it to the active transaction table.
3.  The client then performs a series of read and write operations. For each operation:
    -   The transaction requests the appropriate lock (shared or exclusive) from the `LockManager`.
    -   If the lock is granted, the operation proceeds.
    -   The transaction generates a log record for the operation and appends it to the log.
4.  When the client is finished, it calls either `PreCommit()` or `Abort()` on the `Transaction` object.
    -   **Commit**: The `TransactionManager` writes a `COMMIT` record to the log and then releases all the locks held by the transaction.
    -   **Abort**: The `TransactionManager` uses the log to undo all the changes made by the transaction and then releases all its locks.

This combination of locking for concurrency control and logging for durability and atomicity ensures that the database operates reliably and maintains data consistency even in the presence of concurrent access and system failures.