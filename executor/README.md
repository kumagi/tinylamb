# Executor

This directory contains the query execution engine of the database. It is responsible for processing the logical query plans generated by the query optimizer and turning them into physical operations that retrieve and manipulate data. The execution model is based on the Volcano/Iterator model, where each executor is an iterator that exposes a `Next()` method. This design allows for a clean and composable way to build complex query execution pipelines.

## Core Concepts

- **`ExecutorBase`**: An abstract base class that defines the common interface for all executors. The central method is `Next()`, which retrieves the next row from the executor's output. This iterator-based approach allows for pipelined execution, where data flows from one executor to the next without being fully materialized in memory, leading to efficient query processing.

- **Executors**: Each executor implements a specific data manipulation or retrieval operation. They can be chained together to form a tree-like execution plan.

### Scan Executors

These executors are responsible for reading data from the database tables and indexes.

- **`FullScan`**: Performs a complete scan of a table, iterating through all the rows sequentially. This is the most basic way to access data and is typically used when no suitable index is available.

- **`IndexScan`**: Utilizes an index to efficiently retrieve rows that match a specific key range. It takes a begin and end key and iterates through the rows within that range, making it much faster than a full scan for selective queries.

- **`IndexOnlyScan`**: A highly optimized version of `IndexScan` that retrieves data directly from the index without accessing the base table. This is possible when all the required columns are present in the index itself, which can significantly reduce I/O and improve query performance.

### Join Executors

These executors are responsible for combining rows from two or more tables based on a join condition.

- **`CrossJoin`**: Implements a cross product of two tables, producing a result set where each row from the left table is combined with every row from the right table. This is generally an expensive operation and is used when no specific join condition is provided.

- **`HashJoin`**: A more efficient join algorithm that builds a hash table on the smaller of the two tables (the build side) and then probes the hash table with rows from the larger table (the probe side). This is very effective for equi-joins.

- **`IndexJoin`** (also known as Index Nested Loop Join): An efficient join strategy that iterates through the rows of the outer table and uses an index on the inner table to quickly find matching rows. This is particularly effective when the outer table is small and there is a selective index on the inner table's join key.

### Other Executors

- **`Projection`**: Transforms the rows from its child executor by applying a set of expressions. This is used to select specific columns, compute new values, or rename columns in the output.

- **`Selection`**: Filters the rows from its child executor based on a given predicate (a `WHERE` clause). Only rows that evaluate to true for the predicate are passed on to the next executor.

- **`Insert`**: Takes rows from a source executor and inserts them into a target table. It returns the number of rows that were successfully inserted.

- **`Update`**: Modifies existing rows in a target table. It takes a source executor that provides the new values for the rows to be updated.

## Execution Flow

A query execution plan is a tree of executors. The root of the tree is the final output of the query. To execute the query, the client repeatedly calls `Next()` on the root executor. This call is then propagated down the tree, with each executor calling `Next()` on its children to retrieve the data it needs to perform its operation. This pipelined execution model is highly efficient and is a cornerstone of modern database systems.