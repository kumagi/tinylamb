# `benchmark/` — above all layers, may include anything

Standalone binaries only; never linked into the DB or tests. TPC-H DBGEN is
an executable build dependency of the TPC-H benchmark alone (pinned commit,
not fetched for normal builds).

## Key files

- `tpch_benchmark.cpp` + `tpch_queries.hpp` (all 22 queries) →
  `tinylamb_tpch_benchmark` (**`EXCLUDE_FROM_ALL`**: `cmake --build build
  -j --target tinylamb_tpch_benchmark`; usage
  `./build/tinylamb_tpch_benchmark /tmp/tpch.db --scale-factor 1 --data-dir …`).
- `tpcc_benchmark.cpp` + `tpcc_workload.{hpp,cpp}` →
  `tinylamb_tpcc_benchmark` / `tinylamb_tpcc` lib (multi-client OLTP, TPS +
  per-type latency; callgrind-capable).
- `pg_read_benchmark.cpp` → `tinylamb_pg_read_benchmark`: raw-socket parallel
  read load generator, links `Threads` only (no DB link).
- `expression_jit_benchmark.cpp` →
  `tinylamb_expression_jit_benchmark` (**`EXCLUDE_FROM_ALL`**): scalar-vs-JIT
  on INT64 filters, reports compile ms + break-even row count (LLVM
  availability probe).
- `lsm_tree_bench.cpp` lives in `index/lsm_detail/` (same explicit-target rule).

Gate: `scripts/bench_gate.py` compares against `BenchmarkHistory.md`.
