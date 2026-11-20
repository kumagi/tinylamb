# TPC-H sequential comparison

`tpch.py` runs Q1–Q22 through the same embedded comparison harness as
`tpcds.py`. With `--max-ratio 2`, it stops at the first execution error,
answer mismatch, or tinylamb median above twice DuckDB's median. Later
queries are left unattempted until that failure is fixed.

The input is an existing DBGEN dataset containing `dataset.json`,
`manifest.tsv`, table TSVs, `reference.duckdb`, and both engines' SQL files.
The harness validates table and query hashes before running. A native fixture
must have matching dataset provenance; omit `--fixture` and use
`--prepare-only` to load a fresh one with the current binary.

```sh
/var/tmp/tinylamb-comparison-venv/bin/python benchmark/comparison/tpch.py \
  --scale 1 --dataset build/benchmarks/tpch-three-20261111/dataset \
  --fixture build/quality/tpch-performance-20261117/fixture/tinylamb \
  --binary build-tpcds-release/tinylamb_tpcds_compare \
  --output build/benchmarks/tpch-sf1-next-check \
  --threads 8 --cpus 8,9,10,11,12,13,14,15 \
  --warmup 1 --repeats 7 --max-ratio 2
```

The SF 1 comparison uses the existing DBGEN input (8,661,245 rows),
binary64 numeric columns, DuckDB 2.0.0, and eight threads on CPUs 8–15.
Each query has one warmup and seven measured executions. Timing includes
execution and full result collection, excluding load, transaction setup,
and serialization. Answer comparison preserves duplicate counts, checks
integers and NULLs exactly, and uses relative `1e-9` / absolute `1e-7`
tolerance for floating point. Output order is not checked. This is a warm
embedded-engine comparison, not an official TPC-H score.

## Scan optimization (2026-11-17)

The first gate stopped at Q12: 11.83 ms versus DuckDB's 5.83 ms (2.027×).
Most time was spent dispatching predicates for every lineitem row. The scan
now intersects integer/date bounds and, when a range is selective, selects
candidate row positions in a vectorizable loop before evaluating dictionary
and column-to-column predicates. Every survivor still receives the complete
predicate. NULLs and signed integer endpoints retain their original semantics.

The regression covers NULLs, both INT64 endpoints, an empty interval,
residual predicates, and row/column output with one and three workers.
Run it with:

```sh
ctest --test-dir build-tpcds-release --output-on-failure -j$(nproc) \
  -R 'RelationalRegressionTest.RangePrefilterKeepsResidualsNullsAndBounds'
```

The final binary passed all 22 queries in order twice, with identical bags
of answers and every median within the 2× limit:

| Run | Maximum ratio | Q12 tinylamb / DuckDB | Sum of medians tinylamb / DuckDB |
|---|---:|---:|---:|
| gate04 | 1.777× (Q9) | 9.14 / 5.94 ms (1.539×) | 220.03 / 311.37 ms |
| gate05 | 1.709× (Q8) | 9.01 / 5.88 ms (1.533×) | 220.05 / 309.67 ms |

[gate04](../../build/quality/tpch-performance-20261117/gate04/summary.json)
and [gate05](../../build/quality/tpch-performance-20261117/gate05/summary.json)
retain all samples, answers, input hashes, the binary, and source snapshots.
The related 306 C++ tests, 10 Python tests, layering check, and OOC invariant
check passed.

The same binary also passed the TPC-DS SF 1 Q1–Q99 gate with all answers
matching and a maximum ratio of 1.891× (Q54), under the same measurement
conditions. See the [TPC-DS regression run](../../build/quality/tpch-performance-20261117/tpcds-regression/summary.json).

The final 10-minute OOC campaign completed 1,293 sessions and
160,876 checks with no findings and no ignored findings.
[Verification record](../../build/quality/tpch-performance-20261117/verification.json).

## SF 2 / 5 / 10 completion check (2026-11-17)

With eight threads on CPUs 8–15 and default memory budgets, all 22 queries
completed three times at each scale, including the initial execution. Every
answer matched DuckDB under the comparison rules above. The per-execution
watchdog was 60 seconds; there were no timeouts. Data generation and native
load were also successful. SQL uses the project's same templates, with Q11's
scale-dependent threshold adjusted by `workloads.tpch_queries(scale)`.

| SF | Input rows | Matching queries | Longest measured execution | Longest median |
|---|---:|---:|---:|---:|
| 2 | 17,318,026 | 22 / 22 | 2.317 s (Q1) | 0.092 s (Q6) |
| 5 | 43,299,825 | 22 / 22 | 8.896 s (Q9) | 7.081 s (Q12) |
| 10 | 86,586,082 | 22 / 22 | 21.413 s (Q4) | 19.771 s (Q4) |

Completion does not imply the SF 1 performance target: some queries exceed
2× DuckDB at every larger scale. The largest median ratios were 16.75× at
SF 2 (Q6), 280.66× at SF 5 (Q12), and 404.59× at SF 10 (Q17). Larger-scale
performance optimization remains outstanding.

The initial SF 2 run exposed a Q18 correctness bug (zero rows instead of
100). A direct aggregate scan can leave a no-copy marker in the base-relation
cache. `LoadSource` treated that marker as a materialized empty relation when
the persisted columnar row count was below its size threshold. It now rescans
an empty marker without an associated row arena independently of backing
format and size. `ScanReloadsAggregateCacheMarkerBelowBackingThreshold`
reproduces the issue with three rows, including a residual filter, NULL,
and both output modes. Its setup permits asynchronous background flushing.
The pre-fix failure and successful retest are preserved in the run directory.

The 306 existing related C++ tests and the new regression passed; layering
and OOC invariant checks also passed. An OOC campaign completed 513 sessions
and 67,534 checks with zero findings and zero ignored findings. The final
binary retained the SF 1 gates: all 22 TPC-H queries within 1.709× and all
99 TPC-DS queries within 1.891×, with matching answers.

[Scaling verification](../../build/quality/tpch-scaling-20261117/verification.json)
contains per-scale measurements, outstanding performance ratios, artifact
locations, and source/binary hashes. The directory also retains `prepare.py`,
DBGEN data and provenance, native load logs, resource measurements, and
`task-changes.patch` relative to the start of this check. Each scale's
`run01` directory (SF 2: `run02`) contains all measured samples and answers.
