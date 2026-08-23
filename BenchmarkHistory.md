# Benchmark history

Measurements of tinylamb's bundled TPC-C and TPC-H drivers. These are
engineering numbers, not audited TPC results (no think/keying time, no
auditor, no 2-hour TPC-C window, no TPC-H power/throughput mix).

## 2026-08-23 — SF=1 clean `--force` reload (post M5–M8 + test fixes)

- **When:** 2026-08-23
- **Build:** `CMAKE_BUILD_TYPE=Release` (`build-rel/`), current working tree
- **Command:** `./tinylamb_tpch_benchmark ./tpch-run/sf1 --scale-factor 1 \
  --data-dir ./tpch-run/sf1.tpch-data --force`
- **Fixture:** fresh CRC32C pages at `./tpch-run/sf1` (`.db`/`.log`/`.last_checkpoint`
  replaced; `.tbl` data reused)
- **Load:** 126.39 s total (`lineitem` 126.3 s, `orders` 33.5 s, 8 workers)
- **Analyze:** 50.40 s (8 tables)
- **Query sum (Q1–Q22):** **81.30 s** (`build-rel/tpch-run/sf1-fresh-20260823-bench.log`)
- **Slowest:** Q21 **21.05 s**, Q19 **9.59 s**, Q18 **6.04 s**, Q7 **4.00 s**, Q1 **3.76 s**
- **End-to-end wall (load + analyze + queries):** ~258 s
- **Compare:** prior `--force` **85.05 s** (2026-08-22 M4.5–8); plan-start reuse baseline
  **45.34 s** (`sf1-opt16c-bench.log`, pre-CRC32C fixture — not apples-to-apples)

## 2026-08-22 — M4.5–M4.8 LSM tiered merge, cache padding, HashJoin pipeline, TPC-H heuristics

- **When:** 2026-08-22 (post M4.4)
- **Changes:**
  - LSM `Sync()` swaps under lock, `SortedRun::Construct` outside; `frozen_mem_tree_`
    stays visible until flush completes. `MergeAll()` tiered: merge two oldest runs
    only when `index_.size() > 4` (no full compaction on every pair).
  - LSM cache `PageMeta` with `alignas(64)` per page state (false-sharing mitigation).
  - `HashJoin` in-memory: build right hash table, stream left in `Next`/`NextBatch`
    (spill path unchanged).
  - TPC-H: `CompileScanFilter` OR branches; `part` before `lineitem` in load order;
    `stream_agg` skips row cache when `GROUP BY` is present (Q21 subqueries).
- **lsm_tree_bench** (`-k -s -m`, 500k keys): write **4587 w/ms**, find **1142 r/ms**,
  post-merge find **2232 r/ms**. Tiered merge avoids rewriting all runs when only
  two levels exist (write amplification note: background merger merges 2 oldest
  when depth exceeds 4).
- **SF=1 `--force` reload (CRC32C pages):** query sum **85.05 s**
  (`build-rel/tpch-run/sf1-opt18-m48-bench.log`). Compare opt16c **45.34 s**
  (`--reuse-database`, pre-CRC32C fixture). Q19 **9.81 s**, Q21 **23.03 s**.
- **Note:** apples-to-apples TPC-H comparison requires `--reuse-database` on a DB
  opened cleanly once after `--force`; interrupted runs trigger slow SPR recovery.

## 2026-08-22 — M3.6 Relational parallel morsel (gated)

- **When:** 2026-08-22 (post M3.4 PagePool I/O)
- **Change:** `TryParallelTableScan` wired into `LoadSource`; parallel only for
  read-only scans without pushed filters, integer peeks, or key filters (TPC-H
  keeps serial peek path).
- **SF=1 `--force` reload:** query sum **78.68 s**
  (`build-rel/tpch-run/sf1-opt17-m36b-bench.log`). opt16c baseline **45.34 s**
  used `--reuse-database` on a pre-CRC32C fixture; fresh CRC32C pages add
  ~2× scan time on hot scans (Q1 scan_ms 1739→3525).
- **Note:** reopening the `--force` DB with `--reuse-database` currently triggers
  slow per-page SPR recovery; separate durability follow-up.
- **Correctness:** `ExecutesAllTwentyTwoQueries` PASS

## 2026-08-22 — PagePool concurrency + pread (pg_read_benchmark)

- **When:** 2026-08-22 (post M3.1–M3.4: shared_mutex hit path, miss shared latch,
  `pread`/`pwrite` I/O)
- **Build:** `CMAKE_BUILD_TYPE=Release` (`build-rel/`)
- **Fixture:** `read_scale` table, 50,000 `INT64` rows (`/tmp/pg_read_bench/read_scale.db`)
- **Server:** `tinylamb_server --read-workers 8`, port 54322
- **Query:** `SELECT SUM(id) FROM read_scale;` (10 s measurement, 2 s warmup)

| clients | completed_queries | qps |
| --- | ---: | ---: |
| 1 | 1247 | 124.7 |
| 2 | 2420 | 242.0 |
| 4 | 4648 | 464.8 |
| 8 | 8524 | 852.4 |
| 16 | 8930 | 893.0 |

- **Scaling:** ~1.9× at 2 clients, ~3.7× at 4, ~6.8× at 8 vs 1 client; plateaus
  near 8 workers (~893 qps at 16 clients). No pre-M3 baseline captured; treat as
  post-Phase-3a reference for future regressions.
- **Gate:** `page_pool_test` 26 PASS, TPC-H Q1–22 PASS

## 2026-08-22 — improvement.md plan baseline (SF=1 TPC-H)

- **When:** 2026-08-22 (opt16c after decode-before-filter peeks + filtered
  reusable cache)
- **Tree:** post-TPC-H optimization work on top of `f072a22`
- **Build:** `CMAKE_BUILD_TYPE=Release` (`build-rel/`)
- **Result:** query execution sum **45.34 s** (Q1–Q22 `TPCH_PROFILE`), wall
  ~90 s including ANALYZE. Log: `build-rel/tpch-run/sf1-opt16c-bench.log`
- **Correctness:** `sql_engine_tpch_test.ExecutesAllTwentyTwoQueries` PASS
- **Note:** This is the §19 M0 baseline. Do not regress below this sum while
  landing PagePool / durability / structure work from `improvement.md`.

## 2026-08-20 — SF=1 snapshot

- **When:** 2026-08-20 00:58–01:10 JST
- **Tree:** `31b479e` (`optimize query cache`) plus uncommitted TPC-C scale-factor
  driver work on `master`
- **Build:** `CMAKE_BUILD_TYPE=Release`, sanitizers off
- **Host:** AMD Ryzen 9 9950X3D (16 cores / 32 threads, 128 MiB L3), 59 GiB RAM,
  NVMe
- **Binaries:** `build/tinylamb_tpcc_benchmark`, `build/tinylamb_tpch_benchmark`

### TPC-C (scale factor 1)

Official Clause 4.3 population: 1 warehouse, 10 districts, 3,000 customers per
district, 100,000 items, 3,000 orders per district (900 in `NEW-ORDER`).
Think/keying time omitted (`tpmc_compliant=false`).

Load of the SF=1 fixture completed in about 12 s. Histogram blobs for wide
tables do not fit in a 32 KiB B+tree leaf, so optimizer statistics were
skipped after load. Five-transaction verification all passed (New-Order 38
statements, Payment 7, Order-Status 3, Delivery 70, Stock-Level 2).

Default 10 terminals (`10 × W`) aborted during the measurement window:

```
terminate called after throwing an instance of 'std::runtime_error'
  what():  Cannot parse without type.
```

An earlier 10-client attempt also aborted with `data chunk row width mismatch`.
Both look like concurrent executor/value-decode races, not mix logic.

**Successful run** (2026-08-20 00:58 JST): `--clients 1 --warmup 2 --seconds 60 --seed 20260819`

| Metric | Value |
| --- | --- |
| `tps` | 0.283 |
| `sql_qps` | 8.15 |
| `new_order_tpm` | **9.0** |
| New-Order latency | 6307 ms (9 committed) |
| Payment latency | 5.1 ms (7 committed) |
| Stock-Level latency | 857 ms (1 committed) |
| Order-Status / Delivery | 0 in this 60 s sample |
| Mix check (Clause 5.4.2) | `short_interval` (17 transactions) |
| RSS | 570 MiB |
| Wall time including load | 75.5 s |

**Notes**

- `new_order_tpm ≈ 9` is not a tpmC rating. A single terminal with ~6.3 s
  New-Order latency cannot approach the 45/43/4/4/4 mix or the 90th-percentile
  RT limits. Payment at 5 ms shows primary-key point lookups are fine; New-Order
  is dominated by multi-statement stock/item work without table stats.
- 10 terminals per warehouse is the regulation default, but the engine currently
  does not survive that concurrency on SF=1.
- Skipping statistics at SF=1 likely makes range/join cardinality worse and
  should be treated as a known limiter, not as the intended plan quality.

### TPC-H (scale factor 1)

Official DBGEN cardinalities (6,001,215 `lineitem` rows). Generate 6.58 s, load
11.1 s (lineitem 7.87 s). Peak RSS during the first query batch ~4.6 GiB.

First pass ran Q1–Q7 then segfaulted while dumping the Q7 plan. Remaining
queries were run with `--reuse-database --query N` (timeout 180 s).

| Query | Time (ms) | Result rows | Status |
| --- | ---: | ---: | --- |
| Q1 | 11880 | 0 | ran; empty result (TPC-H Q1 should be 4 groups) |
| Q2 | 4048 | 100 | ran (`LIMIT 100`) |
| Q3 | 22773 | 0 | ran; empty |
| Q4 | 3839 | 0 | ran; empty |
| Q5 | 35013 | 0 | ran; empty |
| Q6 | 12573 | 1 | ran (single aggregate row) |
| Q7 | 20225 | 0 | ran on retry; first all-query run crashed in plan dump |
| Q8 | — | — | segfault (exit 139) |
| Q9 | — | — | segfault |
| Q10 | 17955 | 0 | ran; empty |
| Q11 | 4268 | 741 | ran |
| Q12 | — | — | segfault |
| Q13 | — | — | segfault |
| Q14 | 12620 | 1 | ran |
| Q15 | 12193 | 0 | ran; empty |
| Q16 | 3184 | 18331 | ran |
| Q17 | 31358 | 1 | ran |
| Q18 | 34128 | 100 | ran (`LIMIT 100`) |
| Q19 | 18514 | 1 | ran |
| Q20 | 25052 | 0 | ran; empty |
| Q21 | 31558 | 0 | ran; empty |
| Q22 | — | — | segfault |

Completed queries: **17 / 22**. Crashes: Q8, Q9, Q12, Q13, Q22.

**Notes**

- Scan cost for a 6 M-row `lineitem` pass is about 12–20 s (Q1, Q6, Q14, Q15,
  Q19). That is the current sequential-scan floor at SF=1.
- Q1 scanned 6,001,215 rows and emitted **0** `scan_output_rows`. The date
  predicate is not matching stored `DATE` values, so several “scan then filter”
  queries (Q1, Q3–Q5, Q7, Q10, Q15, Q20, Q21) are timing a wrong empty plan, not
  a correct TPC-H answer. Do not compare these times to published TPC-H QphH.
- Queries that produced plausible cardinalities: Q2 (100), Q6 (1), Q11 (741),
  Q16 (18331), Q17 (1), Q18 (100), Q19 (1).
- Q18 was the slowest successful query (~34 s) with a 6 M-row peak hash table
  and ~6 M aggregate inputs. Q5 was similar wall time with an empty result.
- Q8/Q9/Q12/Q13/Q22 abort in the executor (nested/hash-join or plan dump). Q7
  previously died in `Dump()` after finishing execution.

### What to fix next (from this snapshot)

1. TPC-C: persist table statistics larger than one leaf; survive 10 concurrent
   terminals (`Cannot parse without type` / row-width mismatch).
2. TPC-C: bring New-Order well below a second at SF=1 (indexes are present;
   stats and scan/decode path are the suspects).
3. TPC-H: DATE filters on `lineitem`/`orders` (Q1 empty after a full scan).
4. TPC-H: crashes on Q8, Q9, Q12, Q13, Q22; plan `Dump()` must not crash after
   a successful query.

## 2026-08-20 — after SF=2 OLTP backlog

- **When:** 2026-08-20 04:52–04:58 JST
- **Tree:** `cc204ea` plus uncommitted TPC-C work (statistics split across
  B+tree keys, IndexScan no longer gated on pending writers, Stock-Level
  rewritten onto `order_line_pk`/`stock_pk`, read-only Order-Status and
  Stock-Level, DataChunk schema reset, per-warehouse load, ~2 GiB page pool,
  sharded SQL template cache, hoisted Cascades defaults)
- **Build:** `build-rel`, `CMAKE_BUILD_TYPE=Release`, sanitizers off
- **Host:** AMD Ryzen 9 9950X3D (16 cores / 32 threads), 59 GiB RAM, NVMe
- **Binary:** `build-rel/tinylamb_tpcc_benchmark`
- **Seed:** `20260819` (same as the morning SF=1 snapshot)

Think/keying time omitted (`tpmc_compliant=false`). Official Clause 4.3
population. Driver exit status 3 means `engine_aborted_transactions > 0`, not
a crash.

### TPC-C SF=1, 1 client, warmup 2 s, 60 s

Direct repeat of the 00:58 JST run (`--clients 1 --warmup 2 --seconds 60`).

Load+verify finished well inside the 67 s wall (fixture is no longer one giant
transaction). Verification: New-Order 38, Payment 7, Order-Status 3, Delivery
70, Stock-Level **193** (was 2: `IN (subquery)` became last-20 `order_line`
plus per-item `stock` probes). Peak RSS 444 MiB. Files: `.db` 224 MiB, WAL 469
MiB.

| Metric | 00:58 JST | this run |
| --- | ---: | ---: |
| `tps` | 0.283 | **821.1** |
| `sql_qps` | 8.15 | **27466** |
| `new_order_tpm` | 9.0 | **23047** |
| New-Order latency | 6307 ms (9 committed) | **1.10 ms** (23047 committed, 210 user rollback) |
| Payment latency | 5.1 ms (7) | **0.26 ms** (22143) |
| Order-Status latency | 0 in sample | **8.72 ms** (2003) |
| Delivery latency | 0 in sample | 1.10 ms (**0 committed / 2080 attempted**) |
| Stock-Level latency | 857 ms (1) | **4.30 ms** (2073) |
| Mix | `short_interval` (17 txns) | `short_interval` (payment 43.0%, order-status 3.90%) |
| RSS | 570 MiB | 444 MiB |
| Wall including load | 75.5 s | 67.5 s |

`new_order_tpm` is still not tpmC (no think/keying, 1 terminal, mix not
Clause 5.4.2). The ~2500× New-Order jump matches the intended work: table
stats are used, IndexScan stays an index scan for a lone writer, and
Stock-Level is no longer a 100 k-row `stock` full scan.

Delivery is the remaining 1-client abort: every measurement Delivery fails
with `delivery queue delete affected too few rows`. Verify-only Delivery
passes, so the queue is readable just after load. The mix then does tens of
thousands of New-Orders and zero successful Deliveries, which points at
SELECT-then-DELETE on `new_order` not agreeing (plan/visibility), not at an
empty queue after drain.

### TPC-C SF=1, 10 clients (regulation default), warmup 2 s, 10 s

Previously this aborted (`Cannot parse without type` / `data chunk row width
mismatch`). This run **completed**. Peak RSS 506 MiB. CPU 354%. Exit 3.

| Metric | Value |
| --- | ---: |
| `tps` | 6.90 |
| `sql_qps` | 232.9 |
| `new_order_tpm` | **198** |
| New-Order | 1101 ms (33/33 committed) |
| Payment | 1.12 ms (31/31) |
| Order-Status | 3.67 ms (2/2) |
| Delivery | 719 ms (1/3 committed) |
| Stock-Level | 25.2 ms (2/2) |
| engine aborts | 2 |
| `first_error` | `invalid page type` |

Ten terminals no longer fall over on chunk typing, but New-Order is ~1000×
slower than the 1-client path (1.1 s vs 1.1 ms). `invalid page type` is a
buffer-pool / latch bug under concurrent pins, not mix logic. Throughput does
not scale with clients.

### TPC-C SF=2, 1 client, warmup 2 s, 60 s

Same seed. Verification identical in statement counts. Peak RSS 582 MiB.
Files: `.db` 355 MiB, WAL 701 MiB. Wall 77.2 s.

| Metric | SF=1 1c | SF=2 1c |
| --- | ---: | ---: |
| `tps` | 821.1 | **781.3** |
| `sql_qps` | 27466 | 26716 |
| `new_order_tpm` | 23047 | **22057** |
| New-Order latency | 1.10 ms | **1.12 ms** |
| Payment | 0.26 ms | 0.27 ms |
| Order-Status | 8.72 ms | 8.39 ms |
| Stock-Level | 4.30 ms | 4.45 ms |
| Delivery committed | 0/2080 | 0/2004 |
| engine aborts | 2137 | 2069 |

SF=2 is essentially the SF=1 1-client curve. Warehouse-local point lookups
are not scanning the extra warehouse. Delivery still commits nothing.

### TPC-C SF=2, 20 clients (milestone shape), warmup 2 s, 60 s

Did not crash. Peak RSS 1011 MiB. CPU 1034% (user 849 s + sys 209 s in 102 s
wall). Voluntary context switches 28.3 M. Exit 3.

| Metric | Value |
| --- | ---: |
| `tps` | 3.40 |
| `sql_qps` | 108.6 |
| `new_order_tpm` | **99** |
| New-Order | 8604 ms (99 committed / 105 attempted) |
| Payment | 2.33 ms (88/88) |
| Order-Status | 541 ms (10/10) |
| Delivery | 4177 ms (1/5 committed) |
| Stock-Level | 22.1 ms (6/6) |
| engine aborts | 10 |
| `first_error` | `new-order stock update affected too few rows` |

The SF=2 / 20-terminal / 60 s / zero engine-abort milestone is **not** met
(10 engine aborts, New-Order ~8.6 s). Payment stays ~2 ms, so unique PK
updates on `warehouse`/`district`/`customer` are fine; New-Order and
Order-Status blow up once many writers share the pool and row locks. The
syscall/context-switch ratio is a latch or lock convoy, not SQL parse.

### Notes

- 1-client New-Order/Stock-Level are in the right order of magnitude for an
  in-process OLTP prototype (1 ms / 4 ms). Do not treat 23000 `new_order_tpm`
  as tpmC.
- Stock-Level statement count 193 at verify is expected: 1 district + 1
  `order_line` range + ~191 `stock` probes for distinct items in the last 20
  orders. Average runtime 4 ms says those probes hit `stock_pk`.
- Multi-client survival is new; multi-client speed is not. Next limiter is
  concurrent page validity (`invalid page type`) and New-Order/Delivery row
  updates that report 0 rows under contention, plus the 1-client Delivery
  DELETE mismatch.
- WAL is still ~2× the `.db` file after a 60 s run (group commit helps fsync
  count, not log volume).

### What to fix next (from this snapshot)

1. TPC-C Delivery: `new_order` SELECT MIN vs DELETE must affect the same row
   on a single terminal (0/2000+ committed in the 1-client mix).
2. TPC-C multi-client: `invalid page type` and New-Order latency (1 ms → 1–8 s
   at 10–20 terminals). Page latch / pin / eviction under the 2 GiB pool.
3. TPC-C: engine abort on `stock update affected too few rows` at 20
   terminals; lock wait vs lost update.
4. TPC-H leftovers from the morning snapshot (DATE filters, Q8/Q9/Q12/Q13/Q22
   crashes) were not remeasured here.

