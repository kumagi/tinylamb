# Working with the `tinylamb` Project

`tinylamb` is a C++20 RDBMS: GoogleSQL frontend, Cascades optimizer,
three-tier expression engine (AST / Bytecode / LLVM JIT), hybrid
row+PAX storage, logical WAL, MVCC, LSM-Tree indexes,
morsel-driven execution, PostgreSQL Simple-Query server.

Each layer directory has its own `AGENTS.md`. **Before touching code in
a layer, read that directory's `AGENTS.md` first.** This file covers only
project-wide invariants, layering, and workflows.

If `AGENTS.local.md` exists in the repository root, read it as well — it
contains machine-specific rules and local workflows for this environment.

---

## 1. Layered Architecture & Dependency Rules

Upper layers may include lower layers, never the reverse. Enforced by
`scripts/check_layering.py` (include-lint). The DAG is **not strict**:
accepted debt lives in `DEFAULT_ALLOWLIST` inside that script
(V3'/V4 markers). shrinking that list is how layering debt gets paid
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

1. **`StatusOr<T>` error handling** (`common/status_or.hpp`): public storage,
   catalog, executor, and query APIs return `Status` / `StatusOr<T>` and must
   not let exceptions escape across `Status` boundaries (internal codec bounds
   checks such as `Row::DeserializeBounded` are caught and translated to
   `Status::kCorrupt` / `false` at boundary callers; legacy `exc_shim.hpp`
   helpers are transitional). Macro form is 3-arg:
   `ASSIGN_OR_RETURN(type, value, expr)` (plus `ASSIGN_OR_ASSERT_FAIL`,
   `ASSIGN_OR_CRASH`, `COERCE`).
2. **`PageRef` RAII** (`page/page_ref.hpp`): latches the in-memory `Page` on
   construct and unlocks on destroy (`PageUnlock()` releases early). Never hold
   a raw `Page*` across latch release (`PagePool` is retired; pages live in RAM
   under `PageManager` and persist to `<db>.db` on LSM compaction or explicit `WriteCheckpoint` (no final snapshot is written at shutdown; the WAL is replayed on next open)).
3. **Expression ground truth**: AST `Expression::Evaluate` is the semantic
   reference. New operators go AST-first, then Bytecode; JIT only mirrors
   narrow INT64 kernels. `expression/differential_test.cpp` must pass.
4. **WAL-before-data**: `CheckpointManager::WriteCheckpoint` waits for
   `Logger::WaitForDurable(durable_target)` before writing the `<db>.db`
   snapshot and recycling WAL segments; never bypass it. Recovery invariants:
   [`docs/recovery_invariants.md`](docs/recovery_invariants.md),
   [`docs/wal_format.md`](docs/wal_format.md).
5. **Lock ordering**: `docs/lock_order.md` is normative. Page content latch
   (`PageRef`) vs MVCC write-intent (`TransactionManager::AcquireWriteIntent`)
   are two different things — do not conflate them (`LockManager` is retired).
6. **Plan/executor split**: `plan/` holds logical info only; concrete
   `EmitExecutor` implementations live in `executor/relational_factory.cpp`.
7. **SQL entry points**: `SqlEngine::Execute` / `Prepare`
   (`query/sql_engine.hpp`) over `TransactionContext`. `EXPLAIN [ANALYZE]`
   is a `Prepare`-level string prefix for SELECT/WITH only — there is no
   `kExplain` statement type.
8. **Legacy parser is retired**: the hand-written `parser/` frontend and its `parser/` directory are retired and removed (`legacy/` was also deleted). Nothing may include it or add it to CMake. The only SQL frontend
   is `query/` + external `execute_query --mode=parse` child process.
9. **Server speaks Simple Query only**: extended protocol messages
   (Parse/Bind/Describe/Execute) are rejected with an ErrorResponse; Flush is
   a silent no-op and Sync replies with ReadyForQuery (transaction status
   'I'/'T'/'E'). Text format,
   int8/float8/date-centric.

## 3. Development Workflow

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j
ctest --test-dir build --output-on-failure -j$(nproc)
python3 scripts/check_layering.py       # must exit 0
python3 scripts/check_ooc_invariants.py # must exit 0
```

- **Unit-test concurrency & duration contract**:
  - Every `TEST`/`TEST_F` unit must be safe to run concurrently alongside any other test under `ctest --test-dir <dir> --output-on-failure -j$(nproc)` (no shared mutable filesystem paths, ports, or process-global fixtures across tests — derive unique filenames per test, e.g. `RandomString()`).
  - Every individual test unit must finish in **at most 60 seconds** (`add_simple_test` enforces `TIMEOUT 60` in `CMakeLists.txt`). Do not raise test timeouts in CMake; shard heavy suites into independent `TEST`/`TEST_F` cases or keep per-case input sizes bounded so every unit completes well under 60s even in Debug builds.
  - Exception: `scale`-labeled tests (`add_scale_test`, `TIMEOUT 600`, e.g. GiB-scale spill/sort baselines that cannot fit 60s) are opt-in via `ctest -L scale` and are excluded from default runs (`ctest -LE scale`; CI uses the exclusion). Do not add new scale tests without rethinking sharding first.
  - Always invoke tests with `-j` (`ctest --test-dir <dir> --output-on-failure -j$(nproc)`, adding `-R <regex>` when filtering). Avoid serial full-suite runs.

- Throwaway `.db`/`.log` files: every test binary links
  `database/test_workspace_cleanup_env.cpp`, a gtest environment that
  removes database artifacts newly created in the working directory at
  exit (pre-existing files are untouched; `TINYLAMB_KEEP_TEST_DATABASES=1`
  keeps the run's files for post-mortem). Benchmarks, fuzzers and servers
  outlive or outspawn that hook: sweep with
  `python3 scripts/clean_root_artifacts.py [--dry-run]` (CI runs it after
  ctest/tsan/fuzz jobs). Fuzz harnesses must open databases through
  `query/fuzz_scoped_db.hpp` (`ScopedDb`), which deletes its files per
  iteration — the disk-quota guard that keeps long sweeps from turning
  `Database::Create` failures into bogus crashes.

- Filtered test run: `ctest --test-dir build --output-on-failure -j$(nproc) -R 'Suite\.Case'` (always keep `-j`; avoid running full unsharded binaries serially). Most tests link `tinylamb::sql` + `tinylamb::test_util`; the fuzz replay
tests (added via `add_replay_test`) instead link `tinylamb::sql` /
`tinylamb::postgres_server` directly and do not use `tinylamb::test_util`.
- Format: `clang-format -i` on touched `*.hpp`/`*.cpp`.
- Unit test per bugfix/feature in the same layer dir (`*_test.cpp`).
- Benchmarks: TPC-H (`tinylamb_tpch_benchmark`) and
  `tinylamb_expression_jit_benchmark` are `EXCLUDE_FROM_ALL` — build by
  explicit target, e.g. `cmake --build build -j --target
  tinylamb_tpch_benchmark` (as in `README.md`); the plain `cmake --build build -j`
  shown first in the README does not produce them. TPC-H pulls an external
  `dbgen` build dependency (pinned commit; not fetched for normal builds).
- Bug hunting with SQL oracle fuzzers: `sql_oracle_fuzzer_libfuzzer`
  (TLP/NoREC/PQS/DQE/index-independence/txn-splitting oracles over the whole
  engine), `expr_oracle_fuzzer_libfuzzer` (scalar rewrite + engine + Python
  cross-check), `griffin_fuzzer_libfuzzer` (catalog-guided SQL sessions,
  AST-vs-bytecode/JIT differential), `sql_session_fuzzer_libfuzzer`
  (state-tracking sessions: a C++ row-level mirror is compared against a
  full-table dump after every statement, across PreCommit boundaries),
  `sql_ooc_fuzzer_libfuzzer` (budget/eviction/spill-invariant oracle across
  queries and memory limits).
  Build with
  `cmake -S . -B build-fuzz -DTINYLAMB_ENABLE_FUZZ=ON` and run e.g.
  `./build-fuzz/sql_oracle_fuzzer_libfuzzer -max_total_time=3600` in a scratch
  dir. On a mismatch each writes a self-contained
  `*_fuzz-repro-<seed>.test` (already re-verified by the harness) next to an
  `abort()`. Keep the file as a regression guard:
  `SqlOracleFuzzer.ReplayCommittedRegressionFiles` replays every `.test` in
  `TINYLAMB_ORACLE_REGRESSION_DIR` (expr/griffin traces replay through
  `ReplayExprOracleTrace` / `ReplayGriffinTrace` in their `*_fuzzer_test`;
  session traces through `SqlSessionFuzzer.ReplayCommittedRegressionFilesShard0..3`
  over `TINYLAMB_SESSION_REGRESSION_DIR`, wired in CMake to
  `query/testdata/session_regressions/`; OOC traces through
  `SqlOocFuzzer.ReplayCommittedRegressionFilesShard0..3` over
  `TINYLAMB_OOC_REGRESSION_DIR`, wired in CMake to
  `query/testdata/ooc_regressions/`).
  Logic bugs (wrong results), not just crashes, fail the run; run them long
  after touching optimizer/executor/expression code.
- `.fuzz_findings/` (and `.fuzz_findings_small/`) is an add-only store:
  `scripts/oci_fuzz.sh collect|deploy` and `scripts/oci_small_fuzz.sh collect`
  merge remote findings into it and never delete or overwrite a local file.
  Remove a finding only after you have verified the bug is absent or turned
  it into a committed regression test, and only via
  `python3 scripts/fuzz_collector.py --findings-dir .fuzz_findings --retire <id>...
  --reason {bug-absent,regression-test} --evidence "<test name / replay cmd>"`
  (`--retire-bug-id` retires every record of a bug_id); it logs to
  `retired.jsonl`. Never `rm` files there by hand.
- Out-of-Core bug hunting: `sql_ooc_fuzzer` (`query/sql_ooc_fuzzer.*`,
  `query/sql_ooc_query_gen.*`) shrinks the memory knobs so a few MiB of data
  is many times the buffer pool, then holds the engine to exact results with
  a C++ mirror, a budget-invariance oracle (the same random query with the
  knobs relaxed vs. tiny budgets / always-spill / early-closed cursors),
  exhaustive failure-injection sweeps and leak checks. Run campaigns
  out-of-process so crashes and hangs are captured and minimized too:
  `ninja -C build sql_ooc_fuzz && scripts/ooc_fuzz.py run --bin
  build/sql_ooc_fuzz --out /tmp/ooc --minutes 30 --minimize` (findings are
  bucketed by signature under `<out>/findings/`; `--ignore` /
  `TINYLAMB_OOC_FUZZ_SKIP_OPS` step over known ones). After touching
  `executor/detail/ooc/`, `executor/detail/pipeline/ooc_*`, `common/spill/`,
  `common/memory_budget.*` or `page/page_manager.*`, run it long. Workflow
  (signatures, filing a finding, promoting a fixed one to a regression test):
  [`docs/ooc_fuzzing.md`](docs/ooc_fuzzing.md).
- Pipeline & OOC execution default: `SqlEngine` lowers SELECT queries through `PipelineLowering` (`executor/pipeline_lowering.hpp`) + OOC spillable sinks (`executor/detail/pipeline/`) unless `TINYLAMB_EXEC_ENGINE=legacy`; grouped queries also use `TryLowerGroupedStatementToPipeline` (`query/grouped_pipeline_lowering.hpp`). See [`docs/environment_variables.md`](docs/environment_variables.md) and [`ooca-v2.md`](ooca-v2.md).

## 4. Guidelines for Making Changes

1. Read the target layer's `AGENTS.md`, then place new code in the lowest
   fitting layer and re-run `check_layering.py` and `check_ooc_invariants.py`
   (and `python3 scripts/run_clang_tidy.py` when checking static analysis).
2. Preserve semantic equivalence (`expression/differential_test.cpp`).
3. Check ADRs and design/plan records: [`docs/design.md`](docs/design.md),
   [`docs/review/`](docs/review/), [`docs/adr/`](docs/adr/),
   [`ARCHITECTURE.md`](ARCHITECTURE.md). Improvement plans:
   [`TODO.md`](TODO.md) and [`ooca-v2.md`](ooca-v2.md).
