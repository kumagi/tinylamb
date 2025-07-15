# Index

This directory contains the implementations of various index structures that are crucial for efficient data retrieval. Indexes are fundamental to database performance, as they allow the system to quickly locate specific rows without having to scan the entire table. This directory provides a flexible framework for different types of indexes, each with its own strengths and use cases.

## Core Components

- **`Index` and `IndexSchema`**: These classes define the structure and metadata of an index.
  - **`IndexSchema`**: Describes the logical properties of an index, including its name, the columns that make up the key, any included (covering) columns, and whether the index is unique or non-unique.
  - **`Index`**: Represents a physical instance of an index. It holds the `IndexSchema` and the `page_id_t` of the root page of the underlying data structure (e.g., the B+Tree). It also provides methods for generating index keys from a given row.

- **`BPlusTree`**: A robust and highly optimized implementation of the B+Tree data structure. The B+Tree is a self-balancing tree that maintains sorted data and allows for efficient insertion, deletion, and search operations. It is the workhorse for most indexing in relational databases.
  - **Concurrency Control**: The implementation is designed to be fully transactional, with fine-grained locking to ensure high concurrency.
  - **Operations**: It supports standard operations like `Insert`, `Update`, `Delete`, and `Read`.
  - **`BPlusTreeIterator`**: Provides an iterator for traversing the keys in the B+Tree in sorted order, which is essential for range scans.

- **`LSMTree` (Log-Structured Merge-Tree)**: An alternative index structure that is optimized for write-heavy workloads. LSM-Trees are designed to provide very high write throughput by buffering writes in memory and then sequentially writing them to disk in sorted runs.
  - **MemTable**: An in-memory balanced tree that stores the most recent writes.
  - **Sorted Runs**: On-disk files that store sorted key-value pairs. These are created by flushing the MemTable to disk.
  - **Compaction**: A background process that merges smaller sorted runs into larger ones to reduce read amplification and keep the number of files manageable.
  - **`LSMView`**: Provides a consistent snapshot of the LSM-Tree at a particular point in time, allowing for stable reads while the tree is being modified.

- **`IndexScanIterator`**: A specialized iterator that provides a unified interface for scanning different types of indexes. It abstracts away the details of the underlying index structure (e.g., B+Tree or LSM-Tree) and provides a simple way to iterate over the indexed data.
  - **Range Scans**: It supports scanning a specific key range, both in ascending and descending order.
  - **Predicate Pushdown**: It can be combined with a selection predicate to filter rows at the index level, further improving query performance.

## How It Works

When a query needs to access data through an index, the following steps typically occur:

1.  The query planner identifies the most suitable index for the query based on the `WHERE` clause and `JOIN` conditions.
2.  An `IndexScanIterator` is created for the chosen index, with the appropriate key range and direction.
3.  The executor then uses this iterator to retrieve the matching rows. The iterator, in turn, interacts with the underlying index structure (`BPlusTree` or `LSMTree`) to perform the actual data retrieval.
4.  For `IndexOnlyScan`, the data is returned directly from the index. For a regular `IndexScan`, the iterator returns the `RowPosition` of the base table row, which is then used to fetch the full row.

This modular design allows for new index types to be added to the system with minimal changes to the rest of the codebase, providing a flexible and extensible indexing framework.