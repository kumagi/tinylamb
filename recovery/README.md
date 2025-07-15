# Recovery

This directory is responsible for ensuring the durability and consistency of the database in the face of failures, such as system crashes or power outages. It implements a recovery system based on the principles of the ARIES (Algorithm for Recovery and Isolation Exploiting Semantics) protocol, which is a standard in modern database systems. The core idea is to use a write-ahead log (WAL) to record all changes to the database before they are written to the main data files.

## Core Components

- **`Logger` and `LogRecord`**: These components form the foundation of the write-ahead logging mechanism.
  - **`LogRecord`**: Represents a single entry in the log file. Each log record describes a specific change to the database, such as an `INSERT`, `UPDATE`, or `DELETE` operation on a page. It also includes records for transaction management, such as `BEGIN`, `COMMIT`, and `ABORT`. Each record is assigned a unique Log Sequence Number (LSN), which is a monotonically increasing value.
  - **`Logger`**: Manages the log file on disk. It provides an interface for appending log records to the log file in a sequential and durable manner. Before any changes are made to a page in the buffer pool, a corresponding log record is first written to the log file, ensuring that the change can be reconstructed in the event of a failure.

- **`CheckpointManager`**: This component is responsible for periodically creating checkpoints to speed up the recovery process. A checkpoint is a snapshot of the system's state at a particular point in time.
  - **Process**: To create a checkpoint, the `CheckpointManager` temporarily pauses the transaction manager, writes a `CHECKPOINT_BEGIN` record to the log, flushes all dirty pages from the buffer pool to disk, and then writes a `CHECKPOINT_END` record. This `END` record contains the information needed to identify the starting point for the REDO phase of recovery.
  - **Benefit**: By ensuring that all changes up to a certain point have been written to disk, checkpoints limit the amount of the log that needs to be scanned during recovery, significantly reducing the recovery time.

- **`RecoveryManager`**: This is the central component that orchestrates the recovery process when the database is started after a crash. It reads the log file and restores the database to a consistent state by performing the following three phases:
  1.  **Analysis Phase**: The `RecoveryManager` scans the log from the last checkpoint to the end to identify all the transactions that were active at the time of the crash (the "loser" transactions) and all the pages that were dirty in the buffer pool.
  2.  **REDO Phase**: It scans the log forward from the earliest log record of a dirty page (the "redo point") and reapplies all the changes described in the log records. This ensures that all committed changes are restored to the database, even if they were not written to disk before the crash. This is known as the "repeating history" principle.
  3.  **UNDO Phase**: After the REDO phase, the `RecoveryManager` scans the log backward and reverts all the changes made by the "loser" transactions (those that were not committed before the crash). This is done by applying the "undo" information stored in the log records, ensuring that the database is left in a consistent state with no partial transactions.

## Overall Workflow

Together, these components provide a robust and efficient recovery mechanism. The `Logger` ensures that all changes are durably recorded, the `CheckpointManager` minimizes the amount of work that needs to be done during recovery, and the `RecoveryManager` uses the log to restore the database to a consistent state after a failure. This adherence to the ARIES principles guarantees the atomicity and durability properties of transactions, which are essential for a reliable database system.