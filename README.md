![ctest](https://github.com/kumagi/tinylamb/actions/workflows/run-ctest.yml/badge.svg)
![format](https://github.com/kumagi/tinylamb/actions/workflows/clang-format.yml/badge.svg)

tinylamb
===========

A simple implementation of RDBMS.

SQL frontend
============

The `tinylamb` executable reads one SQL statement from standard input. The
pinned GoogleSQL parser produces its parser AST, a visitor converts that tree
directly to tinylamb expressions and relational query nodes, and the resulting
plan is executed by tinylamb. SQL text is not parsed again by tinylamb's legacy
Tokenizer, Parser, or PrattParser.

```console
cmake -S . -B build
cmake --build build -j
echo "SELECT * FROM warehouse WHERE w_id = 1;" | ./build/tinylamb tpcc
```

CMake downloads the pinned GoogleSQL `execute_query` release and verifies its
SHA-256 checksum. Bazel is not required. GoogleSQL AST mode is required by the
SQL executable; configuring with `-DTINYLAMB_ENABLE_GOOGLESQL=OFF` leaves only
the lower-level tinylamb libraries available.

The SQL execution path supports the TPC-C transaction query shapes (excluding
stored procedures) and all 22 TPC-H queries. This includes inner and left
joins, derived tables, CTEs, correlated scalar/`IN`/`EXISTS` subqueries,
`GROUP BY`/`HAVING`, nested and distinct aggregates, `CASE`, `LIKE`, `BETWEEN`,
date intervals and extraction, `ORDER BY`, `LIMIT`/`OFFSET`, `INSERT`, `UPDATE`,
and `DELETE`. End-to-end coverage lives in `query/sql_engine_tpcc_test.cpp` and
`query/sql_engine_tpch_test.cpp`.

`EXPLAIN` returns the selected physical execution strategy without running the
query. `EXPLAIN ANALYZE` executes it and adds actual row count, planning and
execution time, scan/filter/join/project/sort timings, join comparison counts,
peak intermediate rows, and subquery/index cache counters. Both forms work
through standard input and PostgreSQL simple-query clients such as `psql`.

```sql
EXPLAIN SELECT * FROM lineitem WHERE l_orderkey = 1;
EXPLAIN ANALYZE SELECT l_returnflag, COUNT(*) FROM lineitem
  GROUP BY l_returnflag;
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
only replaces the three tinylamb database files at the exact path supplied.
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
and `BEGIN`/`COMMIT`/`ROLLBACK`. Autocommit `SELECT`/`WITH` queries run on a
worker pool, so independent clients execute read-only transactions in parallel
without blocking the epoll I/O loop. The default worker count is the detected
hardware concurrency and can be set with `--read-workers`. TLS/GSS encryption
requests are declined and
clients continue over plain TCP. The default bind address is therefore
`127.0.0.1`; binding to a non-loopback address should only be done on a trusted
network. PostgreSQL extended-query messages, `COPY`, catalog compatibility for
psql backslash commands such as `\\dt`, and PostgreSQL user/password management
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
predicates, constructs a Cascades `Search`, and delegates join/order
enumeration to `plan/cascades.cpp` via `RuleSet` / `ImplementationRuleSet`.
There is no second standalone DP optimizer; `optimizer.cpp` is the façade and
post-processing layer (aggregates, residual predicates).

**Statement AST:** SQL DML/SELECT shapes live in `query/statement.hpp`
(executor-side code includes it directly; the `parser/ast.hpp` shim remains
only for the legacy parser headers under `parser/` and is slated for removal).
The legacy Tokenizer/Parser/Pratt implementations under `legacy/parser/` are
an archive used by historical tests only (see `legacy/parser/README.md`) and
are not part of the GoogleSQL path.

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

License
==========
See ./LICENSE.txt

Copyright (c) 2023 KUMAZAKI Hiroki <rintyo@gmail.com>
