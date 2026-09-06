# Working with the `tinylamb` Project

`tinylamb` is a C++20 RDBMS: GoogleSQL frontend, Cascades optimizer,
three-tier expression engine (AST / Bytecode / LLVM JIT), hybrid
row+PAX storage, ARIES WAL, MVCC, Foster B+Tree + WiscKey LSM,
morsel-driven execution, PostgreSQL Simple-Query server.

Each layer directory has its own `AGENTS.md`. **Before touching code in
a layer, read that directory's `AGENTS.md` first.** This file covers only
project-wide invariants, layering, and workflows.

---

## 1. Layered Architecture & Dependency Rules

Upper layers may include lower layers, never the reverse. Enforced by
`scripts/check_layering.py` (include-lint). The DAG is **not strict**:
accepted debt lives in `DEFAULT_ALLOWLIST` inside that script
(V1/V3'/V4 markers). shrinking that list is how layering debt gets paid
off — do not add new allowlist entries without rethinking placement.

```
common -> type -> storage(page/ + recovery/ + transaction/) -> index
       -> table -> database -> expression -> relational(executor/detail/)
       -> plan -> executor -> sql(query/) -> server
[above all] main.cpp, benchmark/, *_test*, *_fuzzer*, *_benchmark*
```

CMake mapping (`CMakeLists.txt`): `tinylamb_common` … `tinylamb_executor` are
declared with `tinylamb_add_layer`: `tinylamb_common`, `tinylamb_type`,
`tinylamb_page` (page+recovery+transaction, one rank), `tinylamb_index`,
`tinylamb_table` (**includes `index/index_scan_iterator.cpp`** to avoid a
table<->index cycle), `tinylamb_database`, `tinylamb_expression`,
`tinylamb_executor` (**plan/ + executor/ + executor/detail/ +
`query/query_data.cpp`**); `tinylamb_sql` and `tinylamb_postgres_server` are
plain `add_library` targets.

判定に迷ったら: **下位に置けるものは下位に置く**。詳しくは
[`ARCHITECTURE.md`](ARCHITECTURE.md) (§3 判定表, §4 lint運用, §5 既知違反一覧)。

## 2. Project-Wide Invariants (must-know before reading code)

1. **`StatusOr<T>` error handling** (`common/status_or.hpp`): no `throw`
   in DB logic. Macro form is 3-arg:
   `ASSIGN_OR_RETURN(type, value, expr)` (plus `ASSIGN_OR_ASSERT_FAIL`,
   `ASSIGN_OR_CRASH`, `COERCE`).
2. **`PageRef` RAII** (`page/page_ref.hpp`): pins + latches on construct,
   unpins/unlocks on destroy. Never hold a raw `Page*` across pool ops.
3. **Expression ground truth**: AST `Expression::Evaluate` is the semantic
   reference. New operators go AST-first, then Bytecode; JIT only mirrors
   narrow INT64 kernels. `expression/differential_test.cpp` must pass.
4. **WAL-before-data**: `PagePool` consults a durability gate before
   write-back; never bypass it. ARIES invariants:
   [`docs/recovery_invariants.md`](docs/recovery_invariants.md),
   [`docs/wal_format.md`](docs/wal_format.md).
5. **Lock ordering**: `docs/lock_order.md` is normative. Row latch
   (`PagePool`) vs row lock (`LockManager`) vs MVCC write-intent are three
   different things — do not conflate them.
6. **Plan/executor split**: `plan/` holds logical info only; concrete
   `EmitExecutor` implementations live in `executor/relational_factory.cpp`.
7. **SQL entry points**: `SqlEngine::Execute` / `Prepare`
   (`query/sql_engine.hpp`) over `TransactionContext`. `EXPLAIN [ANALYZE]`
   is a `Prepare`-level string prefix for SELECT/WITH only — there is no
   `kExplain` statement type.
8. **Legacy parser is archived**: `parser/*.hpp` + `legacy/parser/` serve
   historical unit tests only and are off the SQL execution path (canonical
   frontend is `query/` + external `execute_query --mode=parse` child
   process). Do not build on it.
9. **Server speaks Simple Query only**: extended protocol messages
   (Parse/Bind/Describe/Execute) are rejected with an ErrorResponse; Sync and
   Flush are tolerated as no-ops. Text format,
   int8/float8/date-centric.

## 3. Development Workflow

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j
ctest --test-dir build --output-on-failure -j$(nproc)
python3 scripts/check_layering.py   # must exit 0
```

- Single test: `./build/<name>_test` (e.g. `differential_test`,
  `sql_engine_tpch_test`). Tests link `tinylamb::sql` + `tinylamb::test_util`.
- Format: `clang-format -i` on touched `*.hpp`/`*.cpp`.
- Unit test per bugfix/feature in the same layer dir (`*_test.cpp`).
- Benchmarks: TPC-H (`tinylamb_tpch_benchmark`) and
  `tinylamb_expression_jit_benchmark` are `EXCLUDE_FROM_ALL` — build by
  explicit target. TPC-H pulls an external `dbgen` build dependency
  (pinned commit; not fetched for normal builds).

## 4. Guidelines for Making Changes

1. Read the target layer's `AGENTS.md`, then place new code in the lowest
   fitting layer and re-run `check_layering.py`.
2. Preserve semantic equivalence (`expression/differential_test.cpp`).
3. Check ADRs: [`docs/review/`](docs/review/),
   [`docs/adr-proposal.md`](docs/adr-proposal.md),
   [`ARCHITECTURE.md`](ARCHITECTURE.md). Improvement backlog:
   [`docs/next-actions.md`](docs/next-actions.md).
