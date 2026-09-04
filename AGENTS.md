# Working with the `tinylamb` Project

This document provides a comprehensive guide to the `tinylamb` codebase, its layered architecture, core design principles, build and test workflows, and development guidelines for AI agents and human contributors.

---

## 1. Project Overview

`tinylamb` is an educational yet production-grade Relational Database Management System (RDBMS) implemented in **C++20**. It integrates modern database engine concepts:

- **SQL Frontend & PostgreSQL Server**: Direct GoogleSQL AST parsing & visitor (`query/`), supporting all 22 TPC-H queries and full TPC-C transaction shapes. PostgreSQL v3 wire-protocol compatible TCP server (`server/`, `tinylamb_server`).
- **Cascades Query Optimizer**: Memo-based cost-driven optimizer with modular rule sets (scalar rewrites, logical equivalences, physical implementations) in `plan/`.
- **Three-Tier Expression Engine**: Canonical AST evaluation (ground truth), Bytecode VM (batch/vectorized IR), and optional LLVM ORC JIT kernels (`expression/`).
- **Hybrid Storage & Buffer Management**: Slotted-page (`RowPage`) and columnar PAX layout (`PAXPage`/`PAXBlock`), S3-FIFO buffer pool manager (`PagePool`), and RAII page pinning (`PageRef`).
- **ACID, Recovery & Transactions**: ARIES-style write-ahead logging (WAL) and recovery (`recovery/`), MVCC version chains with strict write intent locking (`transaction/`).
- **Index Structures**: Foster B+ Tree (`index/b_plus_tree.hpp`) and WiscKey-style LSM Tree with Blob separation (`index/lsm_tree.hpp`).
- **Vectorized & Morsel-driven Execution**: Volcano + pipeline execution model using `DataChunk`, parallel scans/aggregations, disk spilling (external merge sort, hybrid hash join).

---

## 2. Layered Architecture & Dependency Rules

The codebase is organized into a strict Directed Acyclic Graph (DAG) of layers. **Upper layers may include lower layers, but lower layers MUST NOT depend on or include upper layers.**

Dependency direction is mechanically enforced by `scripts/check_layering.py` (include-lint).

```
[Layer 1]  common       (common/)
   ↓
[Layer 2]  type         (type/)
   ↓
[Layer 3]  storage      (page/ + recovery/ + transaction/)  [CMake: tinylamb_page]
   ↓
[Layer 4]  index        (index/)
   ↓
[Layer 5]  table        (table/)
   ↓
[Layer 6]  database     (database/)
   ↓
[Layer 7]  expression   (expression/)
   ↓
[Layer 8]  relational   (executor/detail/)  [Relational IR & execution helpers]
   ↓
[Layer 9]  plan         (plan/)             [Cascades memo, plan nodes, rules]
   ↓
[Layer 10] executor     (executor/)         [Physical operators, chunk pipelines]
   ↓
[Layer 11] sql          (query/)            [GoogleSQL AST visitor, SqlEngine]
   ↓
[Layer 12] server       (server/)           [PostgreSQL wire protocol server]

[Top-level] main.cpp, benchmark/, tests (*_test.cpp), fuzzers (*_fuzzer.cpp)
```

### Layer Map & Responsibilities

| Layer | Directory | Primary Responsibilities & Key Files |
|---|---|---|
| **common** | `common/` | `status_or.hpp`, `log_message.hpp`, `vm_cache.hpp`, `crc32c.hpp`, encoders/decoders |
| **type** | `type/` | `value.hpp` (tagged union/memory management), `row.hpp`, `schema.hpp`, `date.hpp` |
| **storage** | `page/`<br>`recovery/`<br>`transaction/` | `page_pool.hpp` (S3-FIFO), `row_page.hpp`, `pax_page.hpp`, `page_ref.hpp`<br>`logger.hpp` (WAL), `log_record.hpp`, `recovery_manager.hpp`<br>`transaction_manager.hpp`, `lock_manager.hpp` (MVCC + write intents) |
| **index** | `index/` | `b_plus_tree.hpp` (Foster B-Tree), `lsm_tree.hpp` (SortedRun, BlobFile, cache) |
| **table** | `table/` | `table.hpp`, `full_scan_iterator.hpp` (MVCC fast-path), `table_statistics.hpp` |
| **database** | `database/` | `database.hpp` (Catalog, DDL), `catalog_reader.hpp`, `transaction_context.hpp` |
| **expression** | `expression/` | `expression.hpp` (AST), `bytecode.hpp` (VM), `jit.hpp` (LLVM), `rewrite.hpp` |
| **relational** | `executor/detail/` | `relation.hpp`, `scan_filter.hpp`, `subquery_runtime.hpp`, `planning_heuristics.hpp` |
| **plan** | `plan/` | `optimizer.hpp`, `cascades.hpp` (Memo/Search), `plan.hpp`, `implementation_rules.hpp` |
| **executor** | `executor/` | `hash_join.hpp`, `sort.hpp`, `parallel_scan.hpp`, `parallel_aggregation.hpp`, `data_chunk.hpp` |
| **sql** | `query/` | `googlesql_frontend.hpp`, `googlesql_ast_visitor.cpp`, `sql_engine.hpp`, `statement.hpp` |
| **server** | `server/` | `postgres_server.hpp`, `postgres_protocol.hpp` |

---

## 3. Core Subsystems & Invariants

### 3.1 SQL Frontend & Engine (`query/`, `server/`)
- **GoogleSQL AST**: Pinned GoogleSQL `execute_query` binary generates the AST. `query/googlesql_ast_visitor.cpp` converts it into tinylamb expressions and relational operators.
- **`SqlEngine` Facade**: `SqlEngine::Execute` and `QueryResult` provide a unified interface for CLI (`tinylamb`), benchmarks, and `tinylamb_server`.
- **`EXPLAIN` / `EXPLAIN ANALYZE`**: Built-in support for inspecting selected physical plans and live execution profiles (scan/filter/join timings, row counts, cache hits).

### 3.2 Cascades Optimizer (`plan/`)
- **Memo Structure**: Relational query plans are transformed and explored inside a Cascades memo (`plan/cascades.cpp`).
- **Rule Sets**:
  - `ExpressionRuleSet`: Rewriting scalar expressions (constant folding, De Morgan, NULL normalization).
  - `cascades::RuleSet`: Logical equivalence rules (join commutativity, associativity, predicate pushdown).
  - `cascades::ImplementationRuleSet`: Physical implementation choices (IndexScan, IndexOnlyScan, HashJoin, IndexJoin, NestedLoopJoin).
- See [`docs/cascades_optimizer.md`](docs/cascades_optimizer.md) for details.

### 3.3 Expression Evaluation Policy (`expression/`)
- **Ground Truth**: `Expression::Evaluate` on AST is the **semantic reference**.
- **Bytecode**: `BytecodeCompiler` converts scalar AST trees into stack-based `BytecodeProgram` for batch execution over `DataChunk`.
- **LLVM JIT**: Optional native code kernels for INT64 comparisons/projections/SUM.
- **Rule**: Never introduce divergent behavior across AST, Bytecode, and JIT. Any new scalar expression must be implemented in AST first, then Bytecode, and differential tests (`expression/differential_test.cpp`) must pass.
- See [`docs/expression_evaluation.md`](docs/expression_evaluation.md).

### 3.4 Concurrency, WAL & Recovery (`storage`)
- **MVCC & Transactions**: Strict 2PL/MVCC integration using version chains and write intents. Concurrent writers coordinate via row version chains.
- **Lock Ordering**: Strict locking order must be maintained to prevent deadlocks (see [`docs/lock_order.md`](docs/lock_order.md)).
- **WAL & ARIES Recovery**: All mutations write WAL records (`recovery/logger.hpp`) before pages are flushed. Checkpoint management adheres to ARIES invariants (see [`docs/recovery_invariants.md`](docs/recovery_invariants.md) and [`docs/wal_format.md`](docs/wal_format.md)).

---

## 4. Key Abstractions & Patterns

1. **`StatusOr<T>` (Error Handling)**:
   - Avoid exceptions for database logic. Functions return `StatusOr<T>` or `Status`.
   - Use the `ASSIGN_OR_RETURN(var, expr)` macro for ergonomic error propagation.
2. **`PageRef` (RAII Page Access)**:
   - Pins a page in `PagePool` on creation and unpins/unlocks on destruction.
   - Never hold raw page pointers across pool operations to avoid eviction hazards.
3. **`TransactionContext` & `CatalogReader`**:
   - Encapsulates active transaction state, catalog metadata readers, and runtime execution context.
4. **`DataChunk` (Vectorized Processing)**:
   - Block of column vectors used by batch operators for cache-friendly execution.

---

## 5. Development Workflow & Tooling

### Building the Project
CMake and Ninja are recommended:
```bash
# Configure with Ninja (Release or Debug)
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo

# Build all canonical targets
cmake --build build -j
```

### Running Tests
CTest and GoogleTest are used across all layers:
```bash
# Run all unit and integration tests
ctest --test-dir build --output-on-failure -j$(nproc)

# Run a specific test binary
./build/sql_engine_tpch_test
./build/differential_test
```

### Static Analysis & Layer Linting
```bash
# Verify architectural layer constraints (include-lint)
python3 scripts/check_layering.py

# Format code with clang-format
clang-format -i $(find common type page recovery transaction index table database expression plan executor query server -name '*.hpp' -o -name '*.cpp')
```

### Benchmarks
- **TPC-H Benchmark** (All 22 queries):
  ```bash
  cmake --build build -j --target tinylamb_tpch_benchmark
  ./build/tinylamb_tpch_benchmark /tmp/tpch.db --scale-factor 1 --data-dir /tmp/tpch-data
  ```
- **TPC-C Benchmark**:
  ```bash
  cmake --build build -j --target tinylamb_tpcc_benchmark
  ./build/tinylamb_tpcc_benchmark /tmp/tpcc.db --scale-factor 1 --clients 10 --seconds 30
  ```
- **PostgreSQL Server Benchmark**:
  ```bash
  ./build/tinylamb_server /tmp/test.db --port 54321 &
  ./build/tinylamb_pg_read_benchmark --port 54321 --clients 8 --seconds 10
  ```

---

## 6. Guidelines for Making Changes

1. **Check Layer Constraints First**: Place new classes/functions in the lowest appropriate layer. Run `python3 scripts/check_layering.py` before committing.
2. **Preserve Semantic Equivalence**: If changing expressions or operators, verify with `expression/differential_test.cpp`.
3. **Write Unit Tests**: Every bugfix or new feature must be accompanied by unit tests in the corresponding layer directory (`*_test.cpp`).
4. **Refer to Architectural Decision Records (ADRs)**: Check [`docs/review/`](docs/review/), [`docs/adr-proposal.md`](docs/adr-proposal.md), and [`ARCHITECTURE.md`](ARCHITECTURE.md) for rationale behind existing design decisions.
