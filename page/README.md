# Page

This directory is responsible for the low-level management of data on disk. It abstracts the database file into a collection of fixed-size pages and provides the necessary components to read, write, and manage these pages efficiently. This layer is fundamental to the entire storage engine, as all higher-level components, such as tables and indexes, are built on top of it.

## Core Components

- **`Page`**: The central class representing a single, fixed-size block of data in the database file. Each page has a header containing metadata such as its `page_id`, `PageType`, and a `page_lsn` (Log Sequence Number) for recovery purposes. The body of the page is a union of different page types, allowing it to be interpreted in various ways depending on its role.

- **Page Types**:
  - **`MetaPage`**: A special page (always at `page_id = 0`) that stores global metadata about the database, such as the location of the first free page and the total number of pages in the file.
  - **`RowPage`**: The primary page type for storing table data. It uses a slotted page layout, which allows for efficient storage and retrieval of variable-length rows. It maintains a directory of row pointers at the beginning of the page, with the actual row data stored at the end, growing towards the middle.
  - **`LeafPage`** and **`BranchPage`**: These are specialized page types used for implementing B+Tree indexes.
    - `LeafPage`: Stores the actual key-value pairs of the index. The keys are sorted, and the pages are linked together to allow for efficient sequential scans.
    - `BranchPage`: An internal node in the B+Tree that stores separator keys and pointers to child pages (either other branch pages or leaf pages).
  - **`FreePage`**: A page that is not currently in use and is part of a free list. When a new page is needed, the system can quickly allocate one from this list.

- **`PagePool`**: A buffer pool manager that is responsible for caching pages in memory. It maintains an in-memory cache of recently used pages to minimize disk I/O.
  - **LRU Eviction**: It uses a Least Recently Used (LRU) policy to decide which pages to evict from the cache when it is full.
  - **Pinning**: It allows pages to be "pinned," which prevents them from being evicted while they are in use by a transaction. This is crucial for ensuring data consistency.

- **`PageManager`**: The main interface for interacting with the page layer. It coordinates with the `PagePool` to provide a clean API for allocating, retrieving, and destroying pages. It abstracts away the details of whether a page is in memory or needs to be fetched from disk.

- **`PageRef`**: A smart pointer-like class that provides safe and convenient access to a page in the `PagePool`.
  - **RAII (Resource Acquisition Is Initialization)**: It automatically handles the pinning and unpinning of pages. When a `PageRef` is created, it pins the corresponding page in the `PagePool`. When the `PageRef` goes out of scope, it automatically unpins the page, making it eligible for eviction.
  - **Thread Safety**: It ensures that the page is properly locked before being accessed, preventing race conditions in a multi-threaded environment.

## Workflow

When a higher-level component needs to access a page, it goes through the following steps:
1. It requests the page from the `PageManager` using its `page_id`.
2. The `PageManager` asks the `PagePool` for the page.
3. The `PagePool` checks if the page is already in its cache.
   - If it is, the page is pinned, and a `PageRef` is returned.
   - If not, the `PagePool` finds a free frame in the cache (evicting a page if necessary), reads the page from disk into the frame, pins it, and then returns a `PageRef`.
4. The client then uses the `PageRef` to safely access and modify the page's content.
5. When the `PageRef` is destroyed, the page is automatically unpinned in the `PagePool`, and if it was modified (marked as "dirty"), it will be scheduled to be written back to disk.