# Database

This directory is the heart of the database system, orchestrating the interactions between various low-level components to provide a unified and transactional data management layer. It is responsible for managing the lifecycle of database objects, ensuring data integrity, and providing a clean interface for all database operations.

## Core Components

- **`Database`**: The central class that serves as the primary entry point for all interactions with the database. It encapsulates the entire system and provides a cohesive API for managing tables, indexes, and transactions.
  - **Catalog Management**: It maintains a system catalog, which is a special-purpose B+Tree that stores metadata about all the tables in the database. This includes table schemas, index information, and other critical metadata. The `CreateTable` and `GetTable` methods interact with the catalog to manage table definitions.
  - **Statistics Management**: To support query optimization, the `Database` class manages a persistent B+Tree for storing table statistics. These statistics, such as row counts and data distribution, are crucial for the query optimizer to generate efficient execution plans.
  - **Transaction Initiation**: It provides the `BeginContext` method, which creates a `TransactionContext` to start a new transaction, ensuring that all subsequent operations are performed within a transactional scope.

- **`PageStorage`**: This component acts as an intermediary layer that brings together all the essential storage-related services. It integrates the `PageManager`, `LockManager`, `Logger`, `RecoveryManager`, and `TransactionManager` to provide a complete storage solution.
  - **Lifecycle Management**: It is responsible for initializing and managing the lifecycle of all the core storage components, ensuring they work together seamlessly.
  - **Transaction Creation**: The `Begin` method on `PageStorage` is responsible for creating a new `Transaction` object, which is then wrapped in a `TransactionContext`.

- **`TransactionContext`**: A crucial abstraction that bundles a `Transaction` object with a reference to the `Database` instance. This context object is passed around to various methods to ensure that all database operations are performed within the scope of the correct transaction.
  - **Transactional State**: It holds the active transaction and provides a convenient way to access transactional methods like `PreCommit` and `Abort`.
  - **Object Caching**: To improve performance, it caches pointers to `Table` and `TableStatistics` objects that have been accessed within the transaction. This avoids redundant lookups and deserialization, ensuring that subsequent accesses to the same objects are fast.
  - **High-Level API**: It exposes high-level methods like `GetTable` and `GetStats`, which transparently handle the interaction with the underlying database catalog and statistics manager, providing a simplified view to the application layer.

## Workflow

A typical interaction with the `Database` layer involves the following steps:
1. A client requests a new transaction by calling `Database::BeginContext`.
2. The `Database` class delegates this to `PageStorage`, which creates a new `Transaction` object.
3. A `TransactionContext` is created, bundling the new transaction with a pointer to the `Database`.
4. The client then uses this context to perform various database operations, such as creating tables, inserting data, or executing queries.
5. All operations are performed within the transactional guarantees provided by the underlying managers (e.g., locking, logging).
6. Finally, the client calls `PreCommit` or `Abort` on the `TransactionContext` to either make the changes permanent or roll them back.