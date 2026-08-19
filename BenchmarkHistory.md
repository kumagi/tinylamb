# Benchmark history

Measurements of tinylamb's bundled TPC-C and TPC-H drivers. These are
engineering numbers, not audited TPC results (no think/keying time, no
auditor, no 2-hour TPC-C window, no TPC-H power/throughput mix).

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
