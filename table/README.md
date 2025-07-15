# Table

This directory provides the core abstraction for a database table, which is the fundamental structure for storing user data. It encapsulates the logical schema of the table, its physical storage, and the indexes defined on it. This layer provides a clean and high-level interface for data manipulation and retrieval.

## Core Components

- **`Table`**: The central class that represents a single table in the database. It is the primary entry point for all table-level operations and orchestrates the interactions between the schema, the data pages, and the indexes.
  - **Schema Management**: Each `Table` object is associated with a `Schema`, which defines the names and data types of its columns.
  - **Data Storage**: The actual data of the table is stored in a linked list of `RowPage`s. The `Table` class manages the allocation of new pages as the table grows.
  - **Data Manipulation**: It provides high-level methods for `Insert`, `Update`, and `Delete` operations. These methods handle the low-level details of finding the correct `RowPage` and `slot_t` for a given row and then performing the modification.
  - **Index Management**: The `Table` class is responsible for maintaining all the indexes defined on it. When a row is inserted, updated, or deleted, the `Table` class ensures that all associated indexes are updated accordingly to keep them consistent with the data.

- **Iterators (`Iterator`, `IteratorBase`, `FullScanIterator`)**: The `table` directory provides a powerful and flexible iterator system for accessing the data in a table.
  - **`IteratorBase`**: An abstract base class that defines the common interface for all iterators. This includes methods like `IsValid()`, `operator++()`, and `operator*()`, providing a standard way to traverse a sequence of rows.
  - **`Iterator`**: A concrete, user-facing iterator class that acts as a wrapper around a pointer to an `IteratorBase` implementation. This allows for different types of iterators (e.g., `FullScanIterator`, `IndexScanIterator`) to be used interchangeably.
  - **`FullScanIterator`**: An iterator that performs a full sequential scan of the table. It works by traversing the linked list of `RowPage`s from beginning to end and iterating through all the rows on each page.

- **`TableStatistics`**: A data structure that stores statistical information about the data in a table. This includes metrics such as the total number of rows and the number of distinct values (cardinality) for each column.
  - **Query Optimization**: These statistics are not used for data retrieval directly but are crucial for the query optimizer. The optimizer uses this information to estimate the cost of different query plans and choose the most efficient one. For example, knowing the selectivity of a predicate allows the optimizer to make a more informed decision about whether to use an index or perform a full table scan.

## Workflow

A typical interaction with the `Table` layer involves the following:

1.  **Table Creation/Retrieval**: A `Table` object is obtained from the `Database` catalog.
2.  **Data Manipulation**: Rows are inserted, updated, or deleted using the `Table`'s methods. The `Table` class ensures that these operations are transactional and that all indexes are kept in sync.
3.  **Data Retrieval**: To read data from the table, a client requests an iterator from the `Table` object.
    -   For a sequential scan, `BeginFullScan()` is called to get a `FullScanIterator`.
    -   For an indexed scan, `BeginIndexScan()` is called with the desired index and key range to get an `IndexScanIterator`.
4.  The client then uses the returned `Iterator` to loop through the rows, processing them as needed. The iterator abstraction hides the complexity of whether the data is being read from the base table directly or through an index.