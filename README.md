<!-- CI status (master) -->
[![ctest](https://github.com/kumagi/tinylamb/actions/workflows/run-ctest.yml/badge.svg?branch=master)](https://github.com/kumagi/tinylamb/actions/workflows/run-ctest.yml?query=branch%3Amaster)
[![build](https://github.com/kumagi/tinylamb/actions/workflows/build-matrix.yml/badge.svg?branch=master)](https://github.com/kumagi/tinylamb/actions/workflows/build-matrix.yml?query=branch%3Amaster)
[![tsan](https://github.com/kumagi/tinylamb/actions/workflows/tsan.yml/badge.svg?branch=master)](https://github.com/kumagi/tinylamb/actions/workflows/tsan.yml?query=branch%3Amaster)
[![fuzzer-nightly](https://github.com/kumagi/tinylamb/actions/workflows/fuzzer-nightly.yml/badge.svg)](https://github.com/kumagi/tinylamb/actions/workflows/fuzzer-nightly.yml)
[![Lean proofs](https://github.com/kumagi/tinylamb/actions/workflows/lean.yml/badge.svg?branch=master)](https://github.com/kumagi/tinylamb/actions/workflows/lean.yml?query=branch%3Amaster)
[![benchmark](https://github.com/kumagi/tinylamb/actions/workflows/benchmark.yml/badge.svg)](https://github.com/kumagi/tinylamb/actions/workflows/benchmark.yml)

<!-- Code quality / security -->
[![clang-format](https://github.com/kumagi/tinylamb/actions/workflows/clang-format.yml/badge.svg?branch=master)](https://github.com/kumagi/tinylamb/actions/workflows/clang-format.yml?query=branch%3Amaster)
[![clang-tidy](https://github.com/kumagi/tinylamb/actions/workflows/clang-tidy.yml/badge.svg?branch=master)](https://github.com/kumagi/tinylamb/actions/workflows/clang-tidy.yml?query=branch%3Amaster)
[![CodeQL](https://github.com/kumagi/tinylamb/actions/workflows/codeql.yml/badge.svg?branch=master)](https://github.com/kumagi/tinylamb/actions/workflows/codeql.yml?query=branch%3Amaster)
[![codecov](https://codecov.io/gh/kumagi/tinylamb/graph/badge.svg?branch=master)](https://codecov.io/gh/kumagi/tinylamb)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/kumagi/tinylamb/badge)](https://scorecard.dev/viewer/?uri=github.com/kumagi/tinylamb)

<!-- Project facts -->
[![C++20](https://img.shields.io/badge/C%2B%2B-20-00599C?logo=cplusplus&logoColor=white)](https://en.cppreference.com/w/cpp/20)
[![Platforms](https://img.shields.io/badge/platform-Linux%20%7C%20macOS-lightgrey?logo=linux&logoColor=white)](#continuous-integration)
[![TPC-H](https://img.shields.io/badge/TPC--H-Q1%E2%80%93Q22-success)](#sql-frontend)
[![Built with CMake](https://img.shields.io/badge/build-CMake%20%2B%20Ninja-064F8C?logo=cmake)](https://cmake.org)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE.txt)
[![GitHub last commit](https://img.shields.io/github/last-commit/kumagi/tinylamb)](https://github.com/kumagi/tinylamb/commits/master)

tinylamb
===========

A simple implementation of an RDBMS.

SQL frontend
============

The `tinylamb` executable reads a SQL script from standard input and executes
every statement as one implicit transaction. The pinned GoogleSQL parser
produces its parser AST, a visitor converts that tree directly to tinylamb
expressions and relational query nodes, and the resulting plan is executed by
tinylamb.

```console
cmake -S . -B build
cmake --build build -j
printf 'CREATE TABLE t (k INT64, v VARCHAR); INSERT INTO t VALUES (1, %s);\nSELECT * FROM t WHERE k = 1;\n' "'hello'" | ./build/tinylamb /tmp/demo.db
```

`tinylamb` creates the database file on first use; it bootstraps only the
catalog, so create your tables with DDL before querying.

CMake prefers a locally staged GoogleSQL `execute_query` (via
`~/.local/share/googlesql-zygote/` or `/google/bin/releases/`) without checksum
verification; otherwise it downloads a pinned release and verifies its SHA-256
checksum. Bazel is not required. GoogleSQL AST mode is required by the
SQL executable; configuring with `-DTINYLAMB_ENABLE_GOOGLESQL=OFF` still
builds every library, executable and test binary, but the SQL frontend
becomes unavailable at runtime (statements fail to parse). The frontend-driven
GoogleSQL compliance cases are not compiled into the test binary in this
configuration; parser-only compliance-file tests remain available. Keep the
default ON when running frontend integration coverage.

The SQL execution path supports the TPC-C transaction query shapes (excluding
stored procedures) and all 22 TPC-H queries. This includes inner and left
joins, derived tables, CTEs, correlated scalar/`IN`/`EXISTS` subqueries,
`GROUP BY`/`HAVING`, nested and distinct aggregates, `CASE`, `LIKE`, `BETWEEN`,
date intervals and extraction, `ORDER BY`, `LIMIT`/`OFFSET`, `INSERT`, `UPDATE`,
and `DELETE`. End-to-end coverage lives in `query/sql_engine_tpcc_test.cpp` and
`query/sql_engine_tpch_test.cpp`.

`EXPLAIN` returns the selected physical execution strategy without running the
query. `EXPLAIN ANALYZE` executes it and adds the actual row count plus total
planning and execution time to the plan dump. Both forms work through standard
input and PostgreSQL simple-query clients such as `psql`.

```sql
EXPLAIN SELECT * FROM lineitem WHERE l_orderkey = 1;
EXPLAIN ANALYZE SELECT l_returnflag, COUNT(*) FROM lineitem
  GROUP BY l_returnflag;
```

Storage engine
--------------

Tables use a hybrid row + columnar PAX/zone-map storage engine
(`table/pax_chunk/`, `table/lstable_bridge.*`, and persistent columnar
`TlcsFile` storage under `table/tlcs_file.*`, governed by `ProcessMemoryBudget`
under `common/memory_budget.*`). Full and parallel table scans use per-page
min/max zone maps and zero-allocation columnar batch decoding when the table's
layout generation is clean (refreshed by `ANALYZE` or first read-only analytical
scan), while OLTP point lookups and in-place mutations use the underlying MVCC
row pages (`page/page_manager.*`, `page/page_lsm_store.*`) and secondary
LSM-Tree indexes (`index/lsm_index.hpp`) without rebuilding the columnar store on writes.

Explicit `CREATE TABLE` engine clauses naming `lstable` (`lstable`/`lst`/`columnar`/`pax`,
case-insensitive) are accepted:

```sql
CREATE TABLE lineitem_lst (l_orderkey INT64, l_returnflag STRING)
  OPTIONS (engine = 'lstable');

CREATE TABLE orders_lst (o_orderkey INT64, o_Orderstatus STRING)
  USING lstable;

CREATE TABLE part_lst (p_partkey INT64, p_name STRING)
  ENGINE = lstable;

CREATE TABLE supplier_lst (s_suppkey INT64, s_name STRING)
  WITH (engine = 'lstable');
```

The TPC-H benchmark takes a scale factor, builds the pinned TPC-H DBGEN tool,
generates all eight official-distribution `.tbl` files, validates their
cardinalities, loads them with their SQL types, and executes all 22 queries. It
prints each runtime profile followed by a slowest-first summary. DBGEN is only
downloaded when this explicit benchmark target is built.

```console
cmake --build build -j --target tinylamb_tpch_benchmark
./build/tinylamb_tpch_benchmark /var/tmp/tinylamb-tpch-sf1/database \
  --scale-factor 1 --data-dir /var/tmp/tinylamb-tpch-sf1/data
```

`--reuse-database` skips schema creation and loading for a previously loaded
database, and `--query N` runs one query while investigating a plan. `--force`
only removes the tinylamb database artifacts at the exact path supplied
(`<db>.db`, the base WAL `<db>.log`, and `<db>.log.meta` / `<db>.log.meta.tmp`;
an obsolete `.last_checkpoint` sidecar is removed if present). Numbered WAL
segments `<db>.log.<20-digit index>` are **not** removed — use a fresh path
when a numbered series exists.
Scale factors outside the official TPC-H set are accepted for quick engineering
smoke tests but are reported as non-official.

PostgreSQL-compatible TCP server
================================

`tinylamb_server` exposes the same SQL engine through PostgreSQL's version 3
wire protocol. It uses a non-blocking `epoll` event loop and accepts multiple
TCP clients. The standard-input `tinylamb` executable remains available for
scripts and debugging.

```console
cmake --build build -j --target tinylamb_server
./build/tinylamb_server warehouse.db --host 127.0.0.1 --port 54321 \
  --read-workers 8

# In another terminal:
psql -X "host=127.0.0.1 port=54321 user=tinylamb dbname=warehouse sslmode=prefer"
```

The server supports PostgreSQL startup negotiation, trust authentication,
simple queries, text result rows and NULLs, command tags, error responses,
and `BEGIN`/`COMMIT`/`ROLLBACK`. Read-only `SELECT` messages and ordinary
session statements (`INSERT`/`UPDATE`/`DELETE`/`MERGE`, transaction control,
and `SET`) run on a shared worker pool, with one in-flight task per session,
so independent clients do not block the epoll I/O loop. `WITH`/`EXPLAIN`
that can wrap writes, DDL/catalog changes, and sessions that created TEMP
objects stay on the event-loop path. The pool is configured with the
historical `--read-workers` flag (also called `read_worker_threads`): the
default is the detected hardware concurrency capped at 16. The flag accepts 1..1024;
values above the server cap of 16 are clamped to 16
with a warning at startup. TLS/GSS encryption
requests are declined and
clients continue over plain TCP. The default bind address is
`127.0.0.1`; binding to a non-loopback address should only be done on a trusted
network. PostgreSQL extended-query messages, `COPY`, catalog compatibility for
psql backslash commands such as `\dt`, and PostgreSQL user/password management
are not implemented yet.

Read scaling can be measured with the bundled PostgreSQL-protocol benchmark
client after creating a table named `read_scale` with an integer `id` column:

```console
cmake --build build -j --target tinylamb_pg_read_benchmark
./build/tinylamb_pg_read_benchmark --host 127.0.0.1 --port 54321 \
  --clients 8 --warmup 3 --seconds 15 --database warehouse
```

Cascades optimizer
==================

Logical joins are explored in a Cascades-style memo rather than by the old
subset dynamic-programming loop. Scalar normalization, logical
transformations, and physical implementations are independent named rule
sets, each with a small C++ pattern DSL and add/remove APIs. See
[`docs/cascades_optimizer.md`](docs/cascades_optimizer.md) for the architecture
and extension examples.

**Call path:** `SqlEngine` builds `QueryData`, runs `Rewrite`, then calls
`Optimizer::Optimize` (`plan/optimizer.cpp`). That entry point normalizes
predicates, constructs a Cascades `SearchEngine`, and delegates join/order
enumeration to `plan/cascades.cpp` via `RuleSet` / `ImplementationRuleSet`.
There is no second standalone DP optimizer; `optimizer.cpp` is the façade and
post-processing layer (aggregates, residual predicates). By default (`TINYLAMB_EXEC_ENGINE`
unset or `!= legacy`), `SqlEngine` lowers physical plans into a DAG of
Out-of-Core spillable pipeline stages via `PipelineLowering`
(`executor/pipeline_lowering.hpp`, sinks in `executor/detail/pipeline/`) or the
direct `ColumnarTlcsScanExecutor` fast path; grouped queries also route through
`TryLowerGroupedStatementToPipeline` (`query/grouped_pipeline_lowering.hpp`). See
[`ooca-v2.md`](ooca-v2.md) and
[`docs/environment_variables.md`](docs/environment_variables.md).

**Statement AST:** SQL DML/SELECT shapes live in `query/statement.hpp`
(executor-side code includes it directly).

**Layer libraries:** `tinylamb_core` is an INTERFACE aggregate over
`tinylamb_common` → … → `tinylamb_executor` (see `CMakeLists.txt`). The
layer map, ownership table, and include rules are documented in
[`ARCHITECTURE.md`](ARCHITECTURE.md); dependency direction is enforced by
`python3 scripts/check_layering.py`.

**Docs index:** WAL/page format, lock order, recovery, checksum, durability, and
Value storage notes are under `docs/`. See [`CONTRIBUTING.md`](CONTRIBUTING.md)
for error-handling policy.

TPC-C workload benchmark
========================

The benchmark client takes a TPC-C scale factor `W` (warehouse count, default
1) and loads the Clause 4.3 population: 10 districts per warehouse, 3,000
customers per district, 100,000 items, and 3,000 initial orders per district
(the newest 900 stay in `NEW-ORDER`). It then runs 10 terminals per warehouse
with the standard 45/43/4/4/4 mix, NURand customer/item/last-name selection,
1% New-Order unused-item rollback, and 15% remote Payment.

```console
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j --target tinylamb_tpcc_benchmark
./build/tinylamb_tpcc_benchmark /tmp/tinylamb-tpcc.db --scale-factor 1
./build/tinylamb_tpcc_benchmark /tmp/tinylamb-tpcc.db --sf 1 \
  --clients 10 --warmup 2 --seconds 10 --seed 20260819
```

Each SQL statement goes through the GoogleSQL frontend, tinylamb optimizer,
executor, and transaction commit path. The client reports committed
transactions per second (`tps`), SQL statements per second (`sql_qps`), and
committed New-Order transactions per minute (`new_order_tpm`). Preflight checks
all five transaction types, and the run fails unless every measured transaction
executes at least one statement through `SqlEngine` (`sql_path_gate=PASS`).
Think/keying time is omitted, so the number is not an audited TPC-C `tpmC`
score. Scale factor 1 is a large load (on the order of 100k items and ~300k
order lines). Unit tests use a reduced `TpccScale::ForTest()` population, not
SF=1.

Cross-engine OLAP and OLTP comparison
======================================

[`benchmark/comparison`](benchmark/comparison/README.md) runs portable OLAP,
OLTP, HTAP, and all 22 TPC-H-derived queries against isolated tinylamb,
PostgreSQL, and MySQL servers, with optional Oracle Free and DuckDB (official
Quack) backends, as well as TPC-DS and ClickBench comparison suites
([`benchmark/comparison/README-tpcds.md`](benchmark/comparison/README-tpcds.md),
[`benchmark/comparison/README-clickbench.md`](benchmark/comparison/README-clickbench.md)).
It saves latency, throughput, transaction errors, result validation, query
plans, and reproduction metadata; historical benchmark snapshots are tracked in
[`BenchmarkHistory.md`](BenchmarkHistory.md). These are engineering benchmarks,
not audited TPC scores.

Remote SQL Fuzzing (`ssh oci`)
==============================

Continuous SQL pattern fuzzing (`sql_oracle`, `sql_session`, `sql_join`,
`sql_aggregate`, `sql_plancache`, `griffin`, `expr_oracle`) runs on the remote
server (`ssh oci`) via `scripts/oci_fuzz.sh` and `scripts/remote_fuzz_supervisor.sh`,
using `tmpfs` (`/tmp/tinylamb-fuzz`) for DB/WAL/corpus/log I/O and collecting
deduplicated failing queries and reproduction `.test` files.

```bash
# 稼働状況・各ワーカーの進捗・リモート上の失敗クエリサマリを表示
./scripts/oci_fuzz.sh status

# リモートで集まった失敗クエリ(.sql / .test / .report.txt / SUMMARY.md)をローカルの .fuzz_findings/ に回収
./scripts/oci_fuzz.sh collect

# ローカルのコード変更を oci に同期してリビルド＆Fuzzer再起動
./scripts/oci_fuzz.sh deploy

# ワーカーログの確認 / 停止 / 再起動
./scripts/oci_fuzz.sh logs
./scripts/oci_fuzz.sh stop
./scripts/oci_fuzz.sh restart
```

Continuous integration
======================

Every workflow lives in `.github/workflows/` and shares the toolchain setup in
`.github/actions/setup-toolchain` (LLVM/Clang 21 from apt.llvm.org pinned to
match the dev box, Ninja, mold, ccache).

| Workflow | Trigger | What it checks |
|---|---|---|
| `ctest` | push / PR | layering + OOC invariant lint, then the full suite (`-LE scale`) under ASan + UBSan; GoogleSQL compliance (informational) |
| `build` | push / PR | Linux clang-21 Release, Linux gcc-14 RelWithDebInfo, macOS Apple Clang Debug (build + tests) |
| `tsan` | push / PR | concurrency suites under ThreadSanitizer, plus advisory lock-order detection |
| `clang-format` / `clang-tidy` | push / PR | formatting (clang-format 22; a fix-up patch is uploaded on failure) and tidy on changed lines |
| `CodeQL` / `flawfinder` / `Scorecard` | push / weekly | static security analysis and supply-chain posture → Security tab |
| `coverage` | push to master / weekly | gcov line coverage of production sources → Codecov + job summary |
| `fuzzer-nightly` | daily | every `add_fuzzer_test()` target for 10 min each with a persisted corpus; findings are uploaded as artifacts and filed as a `fuzz` issue |
| `benchmark` | weekly | TPC-H SF=1 regression gate against `BenchmarkHistory.md` (GitHub runners are noisy; authoritative numbers come from evo-x2) |
| `Lean proofs` | `lean/**` changes | `lake build` of the optimizer proofs |
| `workflow-lint` | `.github/**` changes | actionlint + ShellCheck on the workflows themselves |

Test results, slowest tests, coverage and TPC-H timings are rendered into each
run's job summary. Dependabot keeps the pinned actions current.

License
==========
See ./LICENSE.txt

Copyright (c) 2023 KUMAZAKI Hiroki <rintyo@gmail.com>
