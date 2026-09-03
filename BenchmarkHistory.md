# Benchmark history

Measurements of tinylamb's bundled TPC-C and TPC-H drivers. These are
engineering numbers, not audited TPC results (no think/keying time, no
auditor, no 2-hour TPC-C window, no TPC-H power/throughput mix).

## 2026-08-24 — TPC-C SQL frontend restored and enforced

- The standard TPC-C workload again sends every measured DML statement through
  `SqlEngine::Execute`, including GoogleSQL parsing, planning, execution, and
  transaction processing. The typed `Table`/`IndexScanIterator` transaction
  implementations were removed from this driver.
- Preflight now rejects any of the five transaction types if it executes zero
  SQL statements. Runtime accounting also rejects the run if any individual
  measured transaction reports zero statements (`sql_path_gate=FAIL`, exit 4).
- **SF=1 preflight:** New-Order 54 statements, Payment 7, Order-Status 3,
  Delivery 70, Stock-Level 210; every `sql_path` check passed.
- **SF=1 integration diagnostic:** default build, one client, 0 s warmup, 1 s
  measure, no-sync commit: 114 attempts, 113 commits, one expected rollback,
  zero engine aborts, **3,647 executed SQL statements**, `sql_path_gate=PASS`,
  113 TPS. This short run validates wiring only; it is not a performance result.
- Delivery SQL now supplies the complete `ol_number` key bounds for its
  order-line update and aggregate. Without the bounds, repeated SF=1 Delivery
  transactions could miss rows through the composite-prefix scan path.
- The 18,548/45,963 TPS figures below bypassed the SQL frontend. They remain
  useful only as typed storage-engine upper bounds and must not be compared as
  SQL end-to-end TPC-C throughput.

## 2026-08-24 — TPC-C typed-API upper bound (not SQL end-to-end; superseded)

- **Build:** default CMake `build/` (assertions enabled), current working tree.
- **Host:** AMD Ryzen 9 9950X3D, 16 cores / 32 threads, 59 GiB RAM.
- **Shape:** official SF=1 population, 10 clients, 1 s warmup, 5 s measure,
  seed `20260824`, Clause 5.2 mix, no think/keying time.
- **Upper bound (`--no-sync-commit --profile-waits`):** **18,548 committed
  TPS**, 19,548 attempted TPS, **481,440 new-order TPM**. 92,740 commits,
  422 expected New-Order rollbacks, 4,580 engine aborts. Average latency:
  New-Order 0.516 ms, Payment 0.399 ms, Order-Status 0.062 ms, Delivery
  0.455 ms, Stock-Level 2.178 ms. MVCC intent conflict rate: **0.372%**.
- **Release confirmation (`build-rel/`, same shape):** **45,963 committed
  TPS**, 48,454 attempted TPS, 1,182,804 new-order TPM. New-Order 0.213 ms,
  Payment 0.190 ms, Order-Status 0.020 ms, Delivery 0.095 ms, Stock-Level
  0.589 ms; MVCC intent conflicts 0.386%.
- **Durable 1 ms group commit:** **4,370.6 committed TPS**, 167,832
  new-order TPM. WAL wait averaged 1.086 ms over 22,779 waits; this is now the
  dominant limit. Payment admission is disabled for this mode so the gate
  does not serialize fsync/group commit.
- **Release durable confirmation:** **5,001.2 committed TPS**, 190,464
  new-order TPM; WAL wait averaged 1.264 ms and MVCC intent conflicts were
  2.538%.
- **Previous upper-bound observation:** about 4,295 TPS after typed TPC-C and
  MVCC GC work. The final no-sync result is about **4.3x** that figure and
  exceeds the 18k engineering target.

**Bottlenecks removed**

1. MVCC GC retained the newest version of every loaded row and rescanned the
   entire map every ~10 ms. The heap is authoritative for the latest image;
   chains no active snapshot needs are now erased completely. One-client
   intent-mutex time fell from ~1.25 s to ~4 ms in a 3 s sample.
2. `Table::last_pid_` came from stale catalog bytes in every transaction, so
   each append walked the row-page chain from its first page. A PageManager
   tail cache now advances monotonically across independently decoded Table
   objects. New-Order latency fell below 0.6 ms.
3. Delivery performed one B+Tree descent per order line. A bounded exact-key
   range (`line 1..o_ol_cnt`) replaces those probes. Queue deletes and
   New-Order index inserts are deferred until after conflict-prone row writes,
   avoiding unsafe structural WAL undo after an ordinary MVCC abort.
4. The unused `order_line_item_idx` doubled every order-line index write but
   was not read by any of the five TPC-C transactions, so the fixture no
   longer creates it.
5. SF=1 Payment snapshots repeatedly lost on the single W_YTD row. For the
   no-sync upper-bound run, per-warehouse admission prevents complete work on
   snapshots guaranteed to abort; the durable run leaves it disabled to keep
   group commit parallel.

These remain engineering throughput figures, not audited tpmC. The durable
result shows that getting 18k with synchronous commit requires a different
commit publication/grouping design; lock-table lock-freedom is not the next
limiter.

## 2026-08-24 — TPC-C OLTP fast paths and MVCC sharding

- **Build:** `CMAKE_BUILD_TYPE=Release` (`build-rel/`), current working tree.
- **Durability:** synchronous commit enabled, WAL group-commit interval 1 ms.
- **Command shape:** `tinylamb_tpcc_benchmark <fresh-db> --warmup 2
  --seconds 5 --seed 20260824 --profile-waits`. These short samples are for
  engineering direction, not audited tpmC or a stable release gate.
- **SF=1, 10 clients:** **993.6 TPS**, **29,796 new-order TPM**, 4,968 commits
  from 6,045 attempts; 29 expected New-Order rollbacks and 1,048 engine aborts.
  Average latency: New-Order 12.87 ms, Payment 1.10 ms, Order-Status 0.04 ms,
  Delivery 23.14 ms, Stock-Level 16.52 ms.
- **SF=2, 10 clients:** **433.0 TPS**, **12,828 new-order TPM**, 2,165 commits
  from 2,513 attempts; 12 expected rollbacks and 336 engine aborts. Average
  latency: New-Order 30.83 ms, Payment 2.25 ms, Order-Status 0.57 ms, Delivery
  55.54 ms, Stock-Level 69.51 ms.
- **Conclusion on scale factor:** SF=2 did not improve throughput at a fixed
  client count; it was 56% slower than SF=1. The SF=1 result is therefore not
  being held back by too few warehouses. The larger B+Tree/data footprint is
  currently more expensive than the reduction in warehouse contention.
- **Implemented:** typed index/MVCC paths for Delivery and Order-Status;
  typed customer-name lookup in Payment; one multi-row order-line INSERT;
  composite equality-prefix + `IN` point unions; per-index stale-key epochs;
  exact full-key bounds for TPC-C composite scans; per-warehouse Delivery
  admission; configurable 1 ms WAL group commit; wait/abort instrumentation;
  MVCC version shards increased from 64 to 256.
- **Observed waits at SF=1/10:** WAL 4.17 s cumulative, write-intent mutex
  3.38 s, commit-shard mutex 0.12 s across ten workers. Write-intent conflicts
  were 2.18%. The remaining dominant correctness/performance loss is retry-free
  first-updater-wins aborts on warehouse/stock rows, not the scheduler
  (`wait.scheduler.acquires=0`). A lock-free table may reduce mutex time, but
  transaction retry and warehouse hot-row redesign have higher expected value.
- **Caveat:** repeated fresh-load attempts can still terminate with exit 139
  for some seed/B+Tree shapes. Successful samples above completed all five
  preflight transaction checks, but fixture-load stability needs a separate
  sanitizer/fuzzer investigation before using this as a release gate.

## 2026-08-24 — TPC-C SF=1, 10 clients, 60 seconds, MVCC v1

- **Build:** `CMAKE_BUILD_TYPE=Release` (`build-rel/`), current working tree
- **Command:** `./tinylamb_tpcc_benchmark ./tpcc-run/sf1-v1-20260824-final \
  --scale-factor 1 --clients 10 --warmup 2 --seconds 60 --seed 20260819`
- **Fixture:** freshly loaded incompatible big-endian v1 database. All five
  transaction-shape verifications passed before warmup.
- **Result:** **50.533 TPS**, **1,851.433 SQL QPS**, **1,395 new-order TPM**.
  This is an engineering throughput number, not audited tpmC (think/keying
  time omitted).
- **Outcomes:** 3,180 attempted, 3,032 committed, 15 expected New-Order user
  rollbacks, and 133 engine aborts (**4.18%**). Warmup had 18 engine aborts.
- **Average latency:** New-Order **126.254 ms** (1,395/1,456 committed),
  Payment **11.008 ms** (1,333/1,344), Order-Status **1.233 ms** (133/134),
  Delivery **1,713.804 ms** (35/110), Stock-Level **756.091 ms** (136/136).
- **Mix:** New-Order 45.786%, Payment 42.264%, Order-Status 4.214%, Delivery
  3.459%, Stock-Level 4.277%; `mix_clause_542=short_interval` because Payment
  and Delivery fell below their minimum percentages.
- **Resources:** wall **87.63 s** including load/verification and worker drain,
  CPU user/system **179.88/15.97 s**, peak RSS **463,192 KiB**.
- **Comparison:** the previous SF=1/10-client sample was only 10 seconds, so it
  is not strictly apples-to-apples. Against that run, TPS rose 6.90 -> 50.533
  (+632%) and new-order TPM rose 198 -> 1,395 (+605%); New-Order latency fell
  1,101 -> 126 ms. Payment, Delivery, and Stock-Level latency regressed.
- **Correctness fixes required to measure:** update now reads its indexed old
  row before reserving an invisible unstaged intent; TPC-C catches executor
  conflicts and aborts the transaction; insertion skips holes reserved by a
  concurrent delete while WAL replay preserves the selected slot.
- **Log:** `build-rel/tpcc-run/sf1-v1-20260824-10c60s-final.log`.

## 2026-08-24 — SF=1 big-endian v1 format remeasurement

- **Build:** `CMAKE_BUILD_TYPE=Release` (`build-rel/`), current working tree
- **Fresh command:** `./tinylamb_tpch_benchmark ./tpch-run/sf1 --scale-factor 1 \
  --data-dir ./tpch-run/sf1.tpch-data --force`
- **Fixture:** DBGEN `.tbl` files reused; database, WAL, and checkpoint files
  recreated in the incompatible big-endian v1 format.
- **Load:** **112.57 s** total (`lineitem` 112.47 s, `orders` 23.62 s, 8 workers),
  down 10.9% from 126.39 s.
- **Analyze:** **8.17 s**, down 83.8% from 50.40 s.
- **Query sum (Q1–Q22):** **58.40 s**, down 28.2% from 81.30 s
  (`build-rel/tpch-run/sf1-v1-20260824-bench.log`). All 22 queries completed
  with their expected row counts.
- **Slowest:** Q22 **16.07 s**, Q21 **8.67 s**, Q1 **3.88 s**, Q18 **3.20 s**,
  Q9 **2.85 s**.
- **End-to-end wall:** **207.89 s**; peak RSS **9,539,152 KiB**.
- **Warm/reopen check:** query sum **50.29 s**, database open **33.00 s**,
  ANALYZE **5.56 s**, wall **97.08 s**
  (`build-rel/tpch-run/sf1-v1-20260824-reuse.log`).
- **Regression:** Q22 repeatedly took **16.07–17.68 s** versus **1.00 s** in the
  2026-08-23 run; its `filter_ms` rose to 15.81–17.51 s. Q17 also rose from
  **0.94 s** to **2.05–2.36 s**. These are follow-up optimization targets even
  though the total improved.

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

## 2026-08-24 — SQL-only TPC-C regulation pass and OLTP tuning

- **Build:** `build-rel`, Release, sanitizers off
- **Shape:** SF=1, 10 terminals, official population and 45/43/4/4/4 input mix
- **SQL integrity:** every measured transaction enters `SqlEngine::Execute`;
  preflight statement counts are New-Order 10, Payment 7, Order-Status 3,
  Delivery 70, Stock-Level 3. All five `sql_path` checks and the aggregate
  `sql_path_gate` pass. No typed-table benchmark path is present.
- **Regulation correction:** New-Order, Payment and Order-Status now choose
  D_ID randomly in 1..10 per transaction. Only Stock-Level retains the fixed,
  unique terminal `(W_ID,D_ID)` pair. NURand, official cardinalities, 1%
  invalid-item rollback and mix generation remain enabled.
- **Not an audited tpmC result:** keying/think time, remote terminal emulation,
  screens, 120-minute run and durability audit are absent. The binary prints
  `tpmc_compliant=false`.

### No-sync CPU upper bound, warmup 3 s, measurement 10 s

Command: `tinylamb_tpcc_benchmark <db> --scale-factor 1 --clients 10
--warmup 3 --seconds 10 --seed 20260824 --no-sync-commit --profile-waits`.

| Metric | Value |
| --- | ---: |
| committed throughput | **13,218.7 TPS** |
| SQL throughput | **110,680.4 statements/s** |
| New-Order | 61,524 / 62,462 committed, 0.728 ms average |
| Payment | 59,276 / 59,537 committed, 0.372 ms average |
| Order-Status | 5,522 / 5,577 committed, 0.384 ms average |
| Delivery | 389 / 5,562 committed, 3.774 ms average |
| Stock-Level | 5,476 / 5,476 committed, 1.684 ms average |
| engine aborts / user rollbacks | 5,794 / 633 |
| New-Order diagnostic rate | 369,144/min |
| prepare / collect CPU across 10 workers | 50.40 s / 41.64 s |
| MVCC intent conflicts | 6,678 / 2,112,542 (0.316%) |

The 10-second mix landed at 45.062/42.952/4.023/4.013/3.951%; the Stock-Level
share missed the 4% lower bound by 0.049 point, so the driver correctly reports
`short_interval`. This is random short-window variance, not a substituted mix;
use the documented 60-second gate for a mix decision.

### Synchronous commit, warmup 2 s, measurement 5 s

The same shape with the default 1 ms group-commit interval produced
**6,384.4 TPS**, 55,135 SQL statements/s and 176,820 diagnostic New-Orders/min.
WAL durability wait averaged 837.9 us over 37,354 waits. This is the durable
number to compare with future WAL/group-commit work; the 13.2k value above is
the CPU/concurrency upper bound.

### Changes retained from the tuning loop

- versioned-unique index mode retains key→row-position history and lets heap
  MVCC decide visibility; TPC-C `orders`, `new_order`, and `order_line` primary
  indexes use it so rollback/delete cannot poison later point scans;
- strict write-intent waiting reads the predecessor's newest committed image,
  matching the TPC-C isolation-test wait semantics; DELETE remains no-wait to
  prevent ten concurrent Delivery inputs from forming a convoy;
- composite equality-prefix costing and OR/IN point-union ranges keep OLTP
  queries on the correct composite indexes;
- SQL template/compiled-plan caches are worker-local on churn and expose real
  replay vs parameter-mismatch counters; high-cardinality replacement queues
  are suppressed;
- statistics are reused as immutable, epoch-keyed thread-local metadata;
- buffer-pool LRU touches are sampled at 1/64 resident hits and PageRef keeps
  its atomic pin counter, removing a second stripe-map lookup on every unpin;
- point-union scans advance a cursor instead of repeatedly erasing the front
  of a vector; benchmark diagnostics truncate giant first-error SQL strings.

The 30k TPS target is **not reached**. At the current point, prepare plus
executor collection consume about 92 of the available 100 worker-seconds per
10-second run. Delivery is deliberately not serialized by a benchmark-only
warehouse mutex and therefore aborts heavily under a no-think-time input
storm. The next compliant step is a real deferred Delivery/RTE queue plus
generic parameterized index-bound plans; simply skipping SQL/parser work or
adding a warehouse gate would make the number incomparable.

## 2026-08-27 — TPC-C optimization toward 40k TPS (checkpoint + adaptive group commit)

- **Measure:** SF=1, 10 clients, 15 s, sync commit, seed 20260819.
- **Result:** **10,664.9 TPS**, 296,880 new-order TPM, 157,267 WAL waits at **9.75 µs** average (down from ~791 µs / 6,315 TPS baseline).
- **Root cause of original bottleneck:** `CheckpointManager::Start()` was never invoked in the main database startup path (`database/page_storage.cpp`), so the WAL (`.log`) grew unbounded (1.3 GB after 30 s). `fdatasync` on a multi-GB dirty file dominated commit latency (~80% of worker time).
- **Changes applied:**
  1. `PageStorage` constructor now calls `cm_.Start()` (periodic checkpoint enabled).
  2. `CheckpointManager` default interval reduced 60 s → 10 s (`checkpoint_manager.hpp`).
  3. `Logger::AdviseOldBytesDurable()` calls `posix_fadvise(DONTNEED)` after a durable checkpoint, preventing the kernel from re-flushing already-checkpointed pages on each barrier (`recovery/logger.*`).
  4. `LoggerWork()` now fsyncs immediately when `pending_durable_waiters_ > 0`, removing the timer-only delay (`recovery/logger.cpp` adaptive group commit).
  5. `AcquireWriteIntent()` wait extended 1 ms → 5 ms to reduce hot-row aborts on district/new_order.
- **Remaining to reach 40k:** engine abort rate still ~4.6% (mostly `district` / `new_order` write-intent timeouts under extreme contention). Further gains likely from stricter admission control or deferred Delivery queue redesign (see `docs/next-actions.md`).
- Not an audited tpmC result (no think/keying, 15 s window). Engineering figure only.

## 2026-09-03 — TPC-H SF=0.01 PeekCompare predicate pushdown

- **Build:** `CMAKE_BUILD_TYPE=Release`, current working tree.
- **Host:** AMD Ryzen 9 9950X3D (16 cores / 32 threads, 128 MiB L3), 59 GiB RAM.
- **Command:** `./tinylamb_tpch_benchmark /tmp/tpch_baseline/tpch --scale-factor 0.01`
- **Fixture:** SF=0.01 (60,175 lineitem rows, 15,000 orders, 2,000 parts).

### Changes applied
1. **PeekCompare predicate pushdown in Cascades scan path:** FullScanPlan now accepts
   `IntegerPeekCompare` predicates extracted from Selection filters. ParallelScan forwards
   these to `BeginMorselScan`, enabling raw-byte-level integer/date rejection before
   full row deserialization.
2. **Fixed `LogCompensationFailure` forward declaration** in `table/table.cpp`.

### SF=0.01 results (cold first run)

| Query | Time (ms) | scan_ms | filter_ms | join_ms | Rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q1 | 51.8 | 41.6 | 41.6 | 0 | 4 |
| Q2 | 12.1 | 1.1 | 2.2 | 0.6 | 3 |
| Q3 | 15.4 | 5.8 | 5.8 | 0.2 | 10 |
| Q4 | 30.6 | 8.3 | 21.1 | 0 | 5 |
| Q5 | 25.7 | 7.2 | 0.7 | 2.6 | 5 |
| Q6 | 12.7 | 3.4 | 3.4 | 0 | 1 |
| Q7 | 24.7 | 4.1 | 3.1 | 3.1 | 4 |
| Q8 | 24.2 | 8.0 | 1.7 | 2.8 | 5 |
| Q9 | 119.2 | 16.7 | 0.4 | 40.3 | 175 |
| Q10 | 16.3 | 6.0 | 5.6 | 1.0 | 20 |
| Q11 | 11.3 | 1.5 | 0.0 | 0.3 | 5 |
| Q12 | 13.8 | 4.7 | 3.7 | 0.1 | 2 |
| Q13 | 21.4 | 2.3 | 0 | 6.6 | 32 |
| Q14 | 12.1 | 1.5 | 1.3 | 0.2 | 1 |
| Q15 | 12.5 | 3.3 | 3.4 | 0.004 | 1 |
| Q16 | 12.0 | 1.5 | 1.5 | 0.3 | 278 |
| Q17 | 30.8 | 10.3 | 16.5 | 0.04 | 1 |
| Q18 | 32.7 | 17.4 | 17.3 | 0.4 | 100 |
| Q19 | 16.6 | 6.5 | 6.5 | 0.01 | 1 |
| Q20 | 79.8 | 10.2 | 48.7 | 19.5 | 1 |
| Q21 | FAIL | spill write failed (pre-existing) | | | |
| Q22 | 394.3 | 2.8 | 12.4 | 0 | 7 |

**Query sum (Q1–Q22, Q21 excluded):** **1,060 ms**

### Analysis
- Q22 (394ms) dominates due to cold page reads (warm=22ms).
- Q9 (119ms) second slowest: 6-way join with 70K comparisons.
- Q20 (80ms): expensive filter evaluation (48ms).
- PeekCompare does not help Q1 because the date predicate passes 99% of rows.

### Correctness
- `sql_engine_tpch_test.ExecutesAllTwentyTwoQueries` PASS
- `optimizer_test` 81 PASS
- `executor_test` 158 PASS
