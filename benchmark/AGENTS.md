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
- `ooc_baseline_measure.cpp` →
  `tinylamb_ooc_baseline_measure` (**`EXCLUDE_FROM_ALL`**): measures pooled
  tracked memory (`ProcessMemoryBudget`) and unaccounted RSS across load and
  self-join execution.
- `tpcds_benchmark.cpp`, `tpcds_compare.cpp`, `tpcc_tcp_benchmark.cpp`,
  `spsc_aggregation_benchmark.cpp`, `wasm_spike/` — TPC-DS, external-client
  TPC-C, SPSC aggregation, and WASM execution spike runners/benchmarks.

Gate: `../scripts/bench_gate.py` compares against `../BenchmarkHistory.md`
(paths relative to this directory; both files live at the repository root).
