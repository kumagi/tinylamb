# Working with the `tinylamb` Project

This document provides a set of useful tips and a high-level overview of the `tinylamb` project's architecture and development workflow. It is intended for developers looking to contribute to or understand the codebase.

## 1. Core Principle: A Layered Architecture

The project is organized into a clean, layered architecture. Understanding the responsibility of each layer is crucial for navigating the codebase and knowing where to make changes. The layers are organized from the lowest level (closest to disk) to the highest level (query processing).

-   **`page` (Physical Storage Layer):** This is the foundation. It abstracts the database file into fixed-size pages.
    -   **Key Components:** `Page` (the raw page structure), `PagePool` (the buffer pool manager that caches pages in memory), and `PageManager` (the interface for page allocation/retrieval).
    -   **Core Abstraction:** `PageRef` is a critical smart pointer that provides safe, RAII-style access to pages, automatically handling pinning, unpinning, and locking.

-   **`transaction` & `recovery` (ACID Guarantees):** These directories ensure the database is reliable.
    -   **Key Components:** `Transaction` (manages the state of a single transaction), `LockManager` (provides concurrency control via locking), `Logger` (implements the write-ahead log), and `RecoveryManager` (implements the ARIES-based recovery protocol).
    -   **Workflow:** All database changes are first written to a log to ensure durability. The `LockManager` prevents concurrent transactions from interfering with each other.

-   **`table` & `index` (Data Abstractions):** These layers build on the page layer to create meaningful data structures.
    -   **Key Components:** `Table` (manages data stored in `RowPage`s), `Index` (manages index metadata), `BPlusTree` (the primary index implementation), and `LSMTree` (a write-optimized index).
    -   **Functionality:** This layer handles the logic for inserting, updating, and deleting rows and ensures that all associated indexes are kept consistent.

-   **`expression` (SQL Expression Evaluation):** This directory provides the framework for representing and evaluating SQL expressions (e.g., `price * 1.1` or `id > 100`).
    -   **Key Components:** `ExpressionBase` (the abstract base class), `ConstantValue`, `ColumnValue`, and `BinaryExpression`.
    -   **Structure:** Expressions are represented as trees, which allows for complex, nested logic to be evaluated cleanly.

-   **`plan` & `executor` (Query Processing Engine):** This is the highest level, responsible for turning a SQL query into a result.
    -   **`plan`:** Contains the logical plan nodes (`FullScanPlan`, `SelectionPlan`, etc.) and the `Optimizer`. The optimizer is a cost-based engine that transforms a simple logical plan into an efficient physical plan by exploring alternatives like index scans and different join orders.
    -   **`executor`:** Contains the physical operators (`FullScan`, `HashJoin`, `IndexScan`) that execute the plan. It uses the **Volcano/Iterator Model**, where each executor has a `Next()` method that pulls data from its children, allowing for efficient, pipelined execution.

## 2. Development Workflow and Tooling

The project relies on a test-driven development process and standardized tooling to maintain quality.

-   **Test-Driven Development:** The large number of `_test.cpp` and `_fuzzer.cpp` files highlights that testing is fundamental.
    -   **Rule:** Any new feature or bug fix should be accompanied by a corresponding test.
    -   **Utilities:** `common/test_util.hpp` provides helpful macros like `ASSERT_SUCCESS` to make test writing easier.
    -   **Running Tests:** The project uses CTest, which is automated via the `run-ctest.yml` GitHub workflow.

-   **Code Style:** The project enforces a uniform code style using `clang-format`.
    -   **Rule:** Always format your code before committing. The configuration is defined in the `.clang-format` file in the root directory.
    -   **CI Check:** The `clang-format.yml` workflow will automatically check for style compliance.

-   **Building the Project:** The recommended way to build the project is with `cmake` and `ninja`. Ninja is a small build system with a focus on speed, which can lead to faster build times, especially for large projects.
    -   **Command:** From a build directory (e.g., `build/`), run the following commands:
        ```bash
        # First, configure the project with CMake, telling it to generate Ninja build files.
        cmake -G Ninja ..

        # Then, run the build using Ninja.
        ninja
        ```
    - **Important:** You shouldn't run this command other than build directory.

## 3. Key Abstractions and Patterns

Mastering these core abstractions is key to being productive in this codebase.

-   **`PageRef` (RAII for Pages):** This is a smart pointer for a page. It automatically handles the pinning of a page in the `PagePool` on creation and unpinning on destruction. This prevents the page from being evicted from memory while it's in use and is crucial for stability.

-   **`TransactionContext`:** This object is passed to nearly every function that performs a database operation. It bundles the active `Transaction` with a reference to the `Database` instance, providing a clean and consistent way to access transactional and catalog information.

-   **The Volcano/Iterator Model (`ExecutorBase`, `Iterator`):** The query execution engine is built on this model. Every physical operator is an iterator with a `Next()` method. To get the result of a query, you simply call `Next()` on the root executor repeatedly. This call propagates down the execution tree, with each node pulling rows from its children as needed.

-   **`StatusOr<T>` (Error Handling):** Instead of using exceptions, functions that can fail return a `StatusOr<T>`. This object contains either a value of type `T` on success or a `Status` object on failure.
    -   **Usage:** Use the `ASSIGN_OR_RETURN` macro to cleanly handle the result. This macro will assign the value to your variable if the operation was successful or return the error status from the current function if it failed.

## 4. How to Contribute: A General Guide

Here is a general process for adding a new feature:

1.  **Identify the Right Layer:** Determine where your change fits in the architecture. For a new SQL function, you'd start in `expression`. For a new join algorithm, you'd work in `plan` (for the logic) and `executor` (for the implementation).

2.  **Implement the Core Logic:** Write the new class or function. Adhere to existing patterns and use the core abstractions described above.

3.  **Integrate with Other Layers:** Connect your new code to the rest of the system. For example, a new executor would need to be recognized and chosen by the `Optimizer`.

4.  **Write Comprehensive Tests:** Add unit tests for your new component. The existing tests in the same directory are your best guide.

5.  **Format and Verify:** Run `clang-format` on your changes and run all tests using CTest to ensure you haven't introduced any regressions.

## 5. Debugging and Logging

The `common/` directory provides utilities to help with debugging:

-   **`log_message.hpp`:** Use the logging framework (`INFO`, `WARN`, `FATAL`) for detailed, contextual logging. It automatically includes the file, line, and function name.
-   **`debug.hpp`:** Contains useful helper functions like `Hex()` for printing data in a readable hexadecimal format.
