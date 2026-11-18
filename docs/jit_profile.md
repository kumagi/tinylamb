# LLVM JIT target profile

2026-08-19に`tinylamb_expression_jit_benchmark`を実行した。対象はbytecode化後も反復回数の多いINT64比較filterで、LLVM ORC compileは約2.36msだった。

| 評価回数 | scalar | JIT kernel |
|---:|---:|---:|
| 1,310,720 | 0.69ms | 0.27ms |
| 5,242,880 | 2.76ms | 1.07ms |
| 20,971,520 | 11.09ms | 4.32ms |

コンパイル費込みの損益分岐は約2,097万評価だった。このため通常の短時間クエリはbytecodeのまま実行し、Selectionは累積2,000万行からJITへ昇格する。JIT対象はINT64 filter、線形projection、SUM aggregate kernelに限定し、複雑式・NULLを含むbatchはbytecodeへフォールバックする。

方針の詳細は [`expression_evaluation.md`](expression_evaluation.md) を参照（AST が意味論の正本、bytecode は batch 実行用 IR、JIT はその compiler）。

## 実装上の注意(2026-08-24 レビュー反映)

- 本稿は測定記録。昇格判定は静的ヒューリスティクス(プロファイラによる動的判定は未実装)。
- 適用対象と閾値: Selection/Projection/Aggregation 各エグゼキュータ単位。累積2000万行に加え「512K行×満幅バッチ」での早期昇格条件を追加(selection.cpp kJitEarlyPromotionRows)。
- filter JIT の形状: 単一列 vs INT64定数の二項比較(bytecode 3命令)のみ。列対列・複合式は対象外。
- SUM kernel は非並列 AggregationExecutor 専用(推定8192行以上は ParallelAggregation へ迂回し JIT 非対応)。
- フォールバック階層: JIT → bytecode → 式ツリー評価。JIT/bytecode以前にゾーンマップ pruning(BatchMayMatch, NULL count==0 判定)が効く。
- break_even 約2097万は測定グリッド最大点(LLVM ORC compile 約2.36ms、測定環境依存)。

## 2026-11-13: optimized code versus fused execution

The ORC kernels previously went directly from generated IR to machine code.
They now run the target-aware LLVM O3 middle-end before code generation, with
host CPU/features and aggressive backend optimization. The kernel shape cache
still owns code for the process lifetime; kernels contain no captured table
addresses or query results. Changed input buffers can use the same code.
`TINYLAMB_JIT_DUMP_DIR` saves IR and object files, and
`TINYLAMB_JIT_DISABLE_OPT` provides a fresh-process A/B comparison.

The benchmark uses identical buffers, call-level result observation, full
result checks before timing, and a memory compiler barrier between iterations.
The former volatile update inside only the native row loop was not comparable.
Measurements below pin one physical CPU on the Ryzen 9950X3D; each value is the
median of seven measured samples after warmup. Compilation is excluded and
reported separately. Artifacts: `build/benchmarks/clickbench-jit-20261113/`.

| Kernel, 1,048,576 elements | Previous JIT ms | O3 JIT ms | Native ms (O3 run) |
| --- | ---: | ---: | ---: |
| Filter | 0.226918 | 0.064095 | 0.063339 |
| Projection | 0.277533 | 0.101394 | 0.106883 |
| Sum | 0.192116 | 0.049988 | 0.050095 |

The dumped filter object contains AVX-512 comparisons and mask-to-byte stores,
with no per-element function calls. This establishes efficient individual
kernels; it does **not** establish fused SQL execution.

A separate diagnostic evaluates `SUM(IF(x > threshold, x * multiplier + addend,
0))` over bounded signed inputs. Bounds make both the expression and every sum
prefix safe. The split path calls filter, projection, then masked reduction;
its JIT column uses the JIT filter/projection and the same native masked
reduction as the AOT column. The fused reference is one native loop. There is
no fused JIT implementation in this diagnostic, so its JIT result is `nan`,
not a substituted native timing.

| Elements | Split AOT ms | Split JIT/native ms | Fused AOT ms |
| --- | ---: | ---: | ---: |
| 1,048,576 | 0.220245 | 0.220589 | 0.057594 |
| 10,000,000 | 6.881780 | 6.790140 | 0.763064 |

At ten million elements the fused reference is 8.90 times faster than the
split JIT path. Materializing the mask and projected column is consequently a
major remaining design issue. Separate ORC functions cannot be fused by
running O3 independently on each function. Pipeline generation must emit
filter, expression, and reduction in the same module/loop, while preserving
NULL handling, selected-row evaluation, intermediate overflow, and aggregation
boundaries. This ratio is a microbenchmark result, not a ClickBench speedup.

ClickBench Q29 currently runs through native dictionary aggregation, not these
integer JIT kernels. Its owner partitions and borrowed regex capture views
remove serial merging and string allocations, but must not be described as a
fully JIT-compiled pipeline. Hash partitioning also introduces a real exchange
boundary: it cannot simply be removed by marking functions `always_inline`.


## 2026-11-13: eight-core ClickBench integration

| Comparison | Cores | Tinylamb ms | Reference ms | Reference / Tinylamb | Individual wins |
|---|---:|---:|---:|---:|---:|
| ClickHouse | 8 | 742.468 | 1038.330 | 1.398x | 21/43 |
| ClickHouse, reversed order | 8 | 740.689 | 1034.845 | 1.397x | 22/43 |
| DuckDB | 8 | 735.071 | 1501.914 | 2.043x | 22/43 |
| DuckDB | 1 | 3272.270 | 8621.261 | 2.635x | 21/43 |

All four comparisons verify 43/43 answers. The same candidate9 binary
beats ClickHouse in both execution orders (1.398x and
1.397x). The prior implementation took 2790.978 ms;
candidate7 took 915.571 ms. Per-query geometric means are 1.454x
against ClickHouse, 1.288x against eight-core DuckDB and
1.901x against one-core DuckDB. One-to-eight-core scaling is
4.452x. Every-query or overwhelming eight-core victory is not achieved.
Full samples, frozen binaries, perf captures and graphs live under
`build/benchmarks/clickbench-jit-20261113/` (`report/`, `report-comparison/`).

## What changed

- Target-aware LLVM O3 generates vectorized standalone INT64 kernels. Dumped
  IR and machine code show AVX-512 filter comparisons and no per-element calls.
  Compiled code is cached for the process lifetime and embeds no snapshot data
  pointers; changing input buffers does not require recompilation.
- Dictionary transformation, hashing and recent-key partial aggregation share
  a worker loop. Borrowed regex captures avoid copies. Owner partitions remove
  the serial second hash build and finish local TopN before global materialization.
  Q29 fell from roughly 1.1 seconds to 107 ms.
- DISTINCT intake computes radix histograms together, removes adjacent duplicate
  pairs and merges disjoint value ranges in parallel. Q9/Q10 reach about 13/14 ms.
- Dependent integer keys and dense counter merges run in parallel; padded
  per-worker statistics avoid false sharing. Q22/Q23 fell from about 74/60 ms
  to 36/25 ms. All observed expression keys are checked before LIMIT.
- Selective one-key COUNT chooses owner-partitioned state instead of allocating
  dense per-worker state for the entire dictionary. Sampling chooses the layout
  only: every qualifying row is still processed. Buffers are first written by
  workers instead of being zero-filled serially. In the final paired run Q37
  takes 9.934 ms (preceding candidate 40.16 ms), Q38 8.175 ms, Q39 4.207 ms.
  Q37's earlier profile attributed 42.79% of cycles to vector initialization;
  that function disappears from the new profile's >=0.5% self-cost entries.
- Bounded, exact per-code frequencies and first encounter positions are built
  with string **and integer** dictionary encoding. Validated unfiltered
  COUNT DISTINCT reuses those frequencies. Q5/Q6 take 0.517/0.476 ms;
  Q16/Q36 take 8.191/7.653 ms instead of roughly 32/32 ms.

These are generic execution paths, without query-number or benchmark-table
branches. The physical metadata is independent of SQL expressions and belongs
to immutable generation-validated stores. Tests cover NULL, filtered COUNT
DISTINCT, old snapshots, append invalidation, rebuilt stores, 1/2/8 workers,
LIMIT/OFFSET, checked overflow and memory-budget fallback.

## Inlining is not full pipeline fusion

The integer JIT kernels are still separate functions. A ten-million-element
diagnostic takes 6.790 ms through split JIT/native kernels and 0.763 ms through
a fused **AOT** reference, an 8.90x difference. There is **no fused JIT** in that
diagnostic. It identifies intermediate-array overhead, not an achieved SQL
speedup. The ClickBench dictionary improvements above are native C++ paths.

See `build/benchmarks/clickbench-jit-20261113/report/jit-fusion.png`.

SQL-wide fused JIT/WASM and TODO S0–S10 remain incomplete. Pipeline generation
must preserve selected-row evaluation, NULL, checked intermediate overflow,
collation, memory fallback and MVCC. Hash exchange remains a real boundary.
Q19 averages about 7.49 active cores in the diagnostic profile, so that path's
remaining work is not explained by idle workers alone.

## Measurement boundaries and memory

No SQL result cache. First-use column-store construction, exact dictionary
frequency/uniqueness preparation and compilation are outside warm medians.
ClickHouse query caching is disabled. Both sessions use UTC and the same CPU set.
Tinylamb measures Execute+Collect, DuckDB execute+fetchall, and ClickHouse server
statistics.elapsed. Planning is included, but transport boundaries differ.

This is **not an equal-memory experiment**: Tinylamb query working memory is
8 GiB with separate storage/column caches, ClickHouse has a 32 GiB server limit,
and DuckDB uses its automatic default. Tinylamb's peak RSS in the primary
ClickHouse run was 50.82 GiB. Cold preparation and 100-million-row
performance have not been established by these measurements.

## Sanitizer boundary

Clang's function-type sanitizer probes eight bytes before an indirect call
target for its type header. LLVM ORC functions do not carry that header and may
start at the beginning of an executable mapping. Disable only the `function`
sanitizer on the five JIT invocation wrappers; other ASan/UBSan checks and
checked arithmetic remain enabled. The former invalid header probe is absent
in the saved disassembly; all eight JIT and 233 executor tests pass under
ASan/UBSan. `final-validation.json` records the complete final validation.

## Final validation (2026-11-14 JST)

- Full Release build succeeded. CTest: 3,991 tests, zero failures, three existing skips (480.5 s).
- SQL metamorphic oracle: 1,024 seeded iterations, passed (897.0 s).
- ASan/UBSan: executor 233, related regression 24, JIT 8, integer-dictionary scan 1, all passed.
- TSan: one executor and four parallel-regression tests, all passed.
- Python comparison/report tests: 18 passed. Layering: 79 existing allowlisted edges, zero additions.
- `git diff --check`: passed. See `build/benchmarks/clickbench-jit-20261113/final-validation.json` and its adjacent logs.

## 2026-11-14: TPC-DS SF1, largest DuckDB gaps

Artifacts: `build/benchmarks/tpcds-opt-20261114/` (`REPORT.md`,
`summary.json`, raw samples, source manifest, `perf` data and validation logs).
The frozen before/after binaries were both measured again on CPUs 0–7,
with one warmup and seven measured executions per query, in forward and reverse
query order. All 99 answers match the DuckDB reference, including every measured
iteration. The figures below average the two passes' per-query medians.

| Query | Before ms | After ms | Reduction |
| --- | ---: | ---: | ---: |
| Q78 | 230.764 | 214.023 | 7.3% |
| Q14 | 208.316 | 151.716 | 27.2% |
| Q4 | 185.139 | 148.295 | 19.9% |
| Q11 | 108.724 | 85.136 | 21.7% |
| All 99 | 3070.793 | 2810.982 | 8.5% |

DuckDB totals 972.449 ms and ClickHouse 3584.499 ms under the same CPU pinning.
Tinylamb still takes **2.89 times DuckDB's time**; it is 1.28 times faster than
ClickHouse by the sum of medians. Q51 remains a major window/FULL JOIN gap, and
Q47 is approximately 2% slower in this comparison. This is a non-certified
engineering comparison using binary64 numeric columns, fixed query parameters,
unequal memory limits, and warm data/code caches without a query-result cache.

The changes target measured executor costs: directly constructing raw aggregate
outputs, isolating scan counters on cache lines, parallel actual-value UNION type
inference/promotion, compact existence keys for unmatched LEFT joins, parallel
filter/project of shared CTE consumers, and parallel reclamation of large row
intermediates while an explicit query admission scope is alive. Outside that
scope and during exception unwinding reclamation stays serial. Wide join keys,
spill paths, unsigned retagging, and unsupported predicates retain their general
paths. No result-dependent shortcut or new fused SQL JIT is involved.

Diagnostic `perf` runs show why these are partly parallel-execution improvements:
Q14's average active cores increase from 2.60 to 3.74, Q4 from 2.73 to 3.51,
and Q78 from 3.89 to 4.25. These are process-CPU/request-wall ratios, including
profiling overhead, not the timings used in the performance chart. CPU work is
not reduced proportionally to wall time; distributing type checks, CTE copies, and
row reclamation across workers accounts for part of the gain. Q51 remains
near three active cores and needs a separate structural change.

Validation of the final build: CTest 3,995 cases with zero failures and three
existing skips; 1,024 seeded SQL-oracle iterations; 33 ASan/UBSan regression
cases; six TSan cases; layering and diff checks all passed. The benchmark binary
SHA-256 is `9340c7e6f967dbfc19b7e14162ce3cf02ee78d4096afd2e93358f95ccff46d50`.

## 2026-11-14: JOIN push audit

This is an architecture investigation, **not an implemented push executor or
a new performance win**. No production C++ was changed for this audit.
Artifacts: `build/benchmarks/join-push-audit-20261114/`; `measure.py` reproduces
the comparison, `ablation.py` reproduces the DuckDB optimizer experiments
(`--reverse` repeats the eight-core experiments in reverse order).
Use `/var/tmp/tinylamb-comparison-venv/bin/python` from the repository root.
The native binary is the frozen `9340c7e6…` build above. DuckDB reports v2.0.0,
source ID `d54cbbe3df`; source inspection uses that commit, not current main.
Both engines use the same SF1 fixture and CPUs 0 or 0–7, one warmup and seven
measured runs. Every warmup and measured result was compared with the saved
reference, including optimizer experiments. Profiling is a separate execution.
These are narrow diagnostic reruns, not a replacement all-99 score; the earlier
numeric, memory, cache and measurement-boundary caveats still apply.

| Query | Native 1 core ms | DuckDB 1 core ms | Native 8 cores ms | DuckDB 8 cores ms | Native scaling | DuckDB scaling |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q78 | 387.442 | 112.522 | 208.636 | 33.650 | 1.86x | 3.34x |
| Q51 | 300.904 | 196.902 | 189.312 | 48.999 | 1.59x | 4.02x |

### What the code and actual plans establish

- Tinylamb's general API is batch pull, not tuple-at-a-time Volcano. However,
  `SharedBuildParallelHashJoin::BuildSharedHashTable` drains **both** children
  through `RowAt`, and `ParallelProbe` builds complete worker output vectors,
  then merges them before `NextBatch` returns anything. Reversing API call
  direction alone would retain these costs. This operator is an additional
  structural example, **not** the active Q78/Q51 operator.
- The actual Q78/Q51 route is `RelationalExecutor` → `ExecuteQuery` /
  `BuildInput` with hand-written fusion. It reports three / two grouped join
  fusions and zero streamed grouped joins. `TryFusedGroupedJoin` avoids a
  joined-row intermediate within supported shapes but emits aggregate results
  into `Relation` as `Row` / `Value` objects. WITH evaluation also completes
  CTE Relations before their consumers. These are real remaining boundaries,
  despite the existing native fusion and typed scan kernels.
- DuckDB separates a join's build pipeline from the probe pipeline; build
  completion is a dependency. The probe operator stays in the pipeline with
  downstream operations. RIGHT/FULL or external execution can add a subsequent
  source pipeline. Build-side dependencies may deliberately serialize large
  branches that can already occupy all workers; push does not mean assigning
  a separate thread and queue to every operator.
  [Pinned pipeline construction](https://github.com/duckdb/duckdb/blob/d54cbbe3df/src/execution/operator/join/physical_join.cpp).
- Build ingestion uses local hash-table state and chunk key/payload inputs;
  combine and finalize prepare the shared table. Probe produces bounded chunks
  and can request another invocation for the same input. This is a reusable
  execution protocol, not a compiled whole-query loop.
  [Pinned hash join](https://github.com/duckdb/duckdb/blob/d54cbbe3df/src/execution/operator/join/physical_hash_join.cpp).
- DuckDB's build state also uses tuple storage; claiming it never stores rows
  would be wrong. Its probe-side output columns can reference/slice input
  vectors, while needed build-side output is gathered into vectors. Tinylamb's
  owning `DataChunk::AppendGather` is useful but is not a reference/slice view.
  [Pinned hash table](https://github.com/duckdb/duckdb/blob/d54cbbe3df/src/execution/join_hashtable.cpp).
- DuckDB's pipeline driver sends each chunk through its operator sequence to
  the sink and tracks operators with remaining output or blocked execution.
  This supplies the continuation required by one-to-many joins without storing
  the complete joined relation.
  [Pinned driver](https://github.com/duckdb/duckdb/blob/d54cbbe3df/src/parallel/pipeline_executor.cpp).

### Q78: fewer final probes, still much slower

Both engines produce 492,986 store groups, 128,130 web groups and 258,740
catalog groups: **879,856 aggregate output rows**. Tinylamb's late match-domain
Bloom reduction shrinks the store input from 492,986 to 14,010 rows before the
final LEFT joins. DuckDB's two final LEFT joins each emit 492,986 rows, and its
filter keeps 223 before TopN returns 100. Tinylamb therefore does not lose
because it uniquely fails to prune those final probes. Its reduction happens
after those large grouped Relations have already been constructed.

The prior final-build perf sample, not used for this audit's timing table,
attributes 18.02% self cycles to `mi_free_block_delayed_mt` and 3.22% to
`_mi_free_delayed_block`; `FusedGroup` vector insertion and `Value` vector
growth are also prominent. This supports investigating ownership, representation
and materialization, but does not assign the entire inter-engine gap to them.
Native `join_ms` includes fused aggregation work and nested `build_input_ms`
overlaps child work; neither is directly comparable to DuckDB operator timing.
DuckDB operator times sum work across workers, not disjoint wall-clock stages.

### Controlled changes to DuckDB's plan

Disable `cte_inlining`, `build_side_probe_side`, or both; retain all other
settings. Profiles confirm real CTE/CTE_SCAN operators when requested and
RIGHT_ANTI → ANTI in all three Q78 branches when build/probe swapping is disabled.
The switches affect the whole plan, so these are not isolated measurements of
one hash-table function. Seven-repeat medians, forward / reverse experiment order:

| DuckDB 8-core mode | Q78 ms | Q51 ms |
| --- | ---: | ---: |
| Default | 36.022 / 35.882 | 48.731 / 50.213 |
| Disable CTE inlining | 34.901 / 34.902 | 49.503 / 49.305 |
| Disable build/probe swap | 23.380 / 23.856 | 55.724 / 55.491 |
| Disable both | 23.880 / 23.433 | 53.610 / 53.599 |

The small CTE differences should not be interpreted as a general benefit from
materialization. They do rule out CTE inlining alone as an explanation for this
six-fold Q78 gap. Copying DuckDB's RIGHT_ANTI orientation is not the answer
either: its ordinary ANTI variant is faster here. This strengthens the case for
changing the execution/data-transfer substrate rather than adding another
query-shaped rewrite. Push versus pull alone has **not** been isolated by an
otherwise identical implementation, so no causal speedup factor is claimed.

Q51 has genuine grouping/window barriers: 138,313 web and 499,071 store groups,
then 610,617 FULL JOIN output rows. DuckDB's FULL JOIN accounts for about
4.99 ms of accumulated operator time in this profile, while windows account for
293.25 ms out of 336.61 ms accumulated work. Its total wall time is 48.76 ms.
Tinylamb's Q51 perf also shows prefix-window/layout work prominently. Removing
JOIN result materialization should feed a partitioned window sink directly;
it cannot remove the window ordering requirement or by itself explain all Q51
scaling. A FULL JOIN must finish probing before scanning unmatched build rows.

### Concrete migration target

Start with Q78 on native Tier 0, using the same pipeline IR intended for later
WASM generation. Existing SQL/plan semantics and the AST reference remain the
authority. The pipeline structure should be:

```text
date scan/filter ─────────────────────────────> date hash-build / finalize
returns scan (each channel) ──────────────────> existence hash-build / finalize
sales scan -> date probe -> anti probe ───────> channel group-aggregate sink

web group scan ───────────────────────────────> web hash-build / finalize
catalog group scan ───────────────────────────> catalog hash-build / finalize
store group scan -> web LEFT probe -> catalog LEFT probe -> filter -> TopN sink
TopN source -> result
```

Arrows are chunk flow. Probe pipelines depend on the corresponding finalized
build state, and aggregate scans depend on aggregation completion. All three
channels still group their inputs; a group boundary is not erased by the drawing.
The key change is that an aggregate's output is scanned directly into its
consumer, without an intervening all-row Relation. The scheduler admits tasks
for an entire scan-to-sink pipeline under the query's worker/memory budget.

Use typed chunk views plus selection indexes for transient probe outputs,
packed keys/payload for retained hash state, and worker-local aggregate/build
state followed by partitioned finalize. Reference lifetimes must cover resumed
one-to-many output and blocked sinks; strings need pinned owners. Shared CTEs
can retain immutable chunk collections, whereas single-use CTEs connect producer
and consumer pipelines. Spill, cancellation and errors must release every owner
and budget charge; unsupported SQL shapes keep an explicit fallback.

For FULL JOIN, track matches per build row, including duplicate keys and residual
predicates, and scan unmatched rows only after all probe tasks finish. Use
race-free matched-state updates; copying an implementation's unsynchronized
same-value writes would violate this project's TSan requirements. NULL keys,
null-safe equality, signed/unsigned and collation semantics need distinct gates.
Do not push a build-key rejection into an arbitrary LEFT/ANTI probe, or through
an aggregate/window without proving group completeness and error semantics.

The acceptance gates and implementation order are now in `TODO.md` S2.1.
Track Row boxing, gathered/copied bytes, retained probe output, and pipeline
waiting separately from task CPU and wall time. The first goal is eliminating
unnecessary Relation boundaries in a real SQL execution; WASM/JIT comes after
that measurement. An API rename or an isolated microbenchmark is not completion.

## 2026-11-14: Direct grouped-output consumers

The first production connection now exists: `GroupedOutputSink` receives up to
1,024 rows of borrowed cells at a time from raw grouped-join emission.
`TryPushGroupedCteJoins` connects eligible single-use grouped CTEs or derived
tables to numeric build/probe consumers. The SQL frontend already inlines the
Q78 CTEs into derived sources, so supporting that actual shape was necessary.

`GroupedNumericHash` retains numeric tuples without per-row `Row` or `Value`
allocations. Build workers fill disjoint payload ranges and publish bucket heads
atomically; probes start only after all build pushes complete. Duplicate keys
retain every row. The probe consumer composes two to four inputs, retains one
scratch row per worker, and sends only WHERE survivors to the ordinary finish
path. A one-to-many match is consumed immediately, including combinations across
multiple build tables; there is no complete intermediate join-output vector.

The current gates require LEFT equality joins with signed INT64 keys, numeric
payloads, and a restricted total numeric predicate language. NULL keys never
match. No-match rows are NULL-extended unless a proven all-unmatched-rejecting
predicate permits omission. Unsupported domains/shapes retain the general
route; no query names, fixture values or known answers are matched. Aggregate
overflow flags are checked even for aggregates not selected into the output.
Build state and batch buffers reserve query memory; a declined reservation
keeps the ordinary spill-capable emission. Sink errors propagate after the
worker barrier, and retained filtered output can still spill.

This is **not** completion of the pipeline IR, scheduler or SQL-wide JIT.
Transient batches still contain `Value` cells. Scan, anti-join and grouping
inputs still use existing execution, and semantic aggregate barriers remain.
The new hash table does not yet publish a Bloom filter. General runtime filter
publication and Q78's old late composite Bloom reduction are separate existing
mechanisms, not evidence of scan-level filter transfer in this new path.

`EXPLAIN ANALYZE` reports `pushed_group_rows` (rows delivered through grouped
sinks, including any attempted path later declined by its consumer) and
`pushed_group_joins` (successfully executed grouped LEFT-join chains).
`TINYLAMB_DISABLE_GROUP_OUTPUT_PUSH=1` disables the connection for same-binary
A/B measurements. Artifact root: `build/benchmarks/group-push-20261114/`.

### Final same-binary measurements and validation

The frozen native binary is SHA-256
`8e51e5be1990f33962a09ab05cfc93ef32b846e9092fcd830f4349fc259eb4de`.
`measure_final.py` pins CPU 0 for one worker and CPUs 0–7 for eight workers.
Each result has one warmup and seven timed executions; native numbers below
average the medians from forward and reverse query/mode orders. DuckDB 2.0.0
(`d54cbbe3df`) was measured again on the same SF1 fixture. No concurrent build,
test or fuzzer ran during these measurements. This measures warm execution,
not a cached result.

| Query | Cores | Native connection OFF (ms) | Native connection ON (ms) | DuckDB (ms) |
|---|---:|---:|---:|---:|
| Q78 | 1 | 391.177 | 421.959 | 112.882 |
| Q78 | 8 | 217.797 | 180.707 | 35.554 |
| Q51 | 1 | 300.813 | 298.597 | 196.973 |
| Q51 | 8 | 197.313 | 197.325 | 49.950 |

Q78 improves **17.03% at eight cores**, but regresses **7.87% at one core**.
The new route avoids grouped Row materialization yet probes all left groups;
the old route uses a late composite Bloom reduction. This is an unresolved
tradeoff, not evidence that push execution alone eliminates the gap. The new
Q78 is still 5.08 times DuckDB's elapsed time at eight cores. Q51 does not enter
this LEFT-join route and its eight-core timing is unchanged.

The sum of all 99 eight-core query medians decreases from **2,820.001 ms to
2,784.581 ms (1.26%)**. Peak process RSS is effectively unchanged at **11.210 GiB**;
this fixture and upstream materialization still dominate. No new all-query
DuckDB/ClickHouse total was collected in this run. The chart is
`build/benchmarks/group-push-20261114/comparison.png` (also SVG).

All 408 median records, including their warmup and seven timed executions,
passed answer comparison against the DuckDB reference. Validation also passed:

- Full CTest: 3,999 cases, zero failures, three existing skips.
- SQL oracle: 1,024 seeded iterations.
- ASan/UBSan: 37 targeted cases; TSan: 10 targeted cases.
- Generated SQL shapes: 64 queries against DuckDB with the connection OFF/ON
  (128 comparisons); 14 shapes actually entered the new route.
- Layering lint and `git diff --check`.

`final-validation.json`, `code-manifest.json`, `final/manifest.json`,
`final/timings.json`, `shape-oracle/validation.json` and the individual logs
retain validation commands, source/binary identities and raw measurements.

A separate eight-core Q78 `perf record` comparison (`perf-legacy/`, `perf-push/`)
used seven executions per mode and verified all 14 answers. These diagnostic
runs are excluded from the timing chart. Median process CPU decreased from
960 ms to 860 ms; average active cores were 4.19 and 4.75 respectively, so eight
cores are still not continuously occupied. Sampled user cycles were about
27.84 billion versus 24.70 billion over the seven executions.

The new run still attributes 16.77% of sampled self cycles to
`mi_free_block_delayed_mt`, 6.38% to growing `vector<Value>`, and 5.39% to
`ClearRelationRows`; the chained probe consumer itself accounts for 8.11%.
These are sampled attribution, not exact phase wall times. Both routes decode
7,893,930 scan values: the new grouped-output connection has not reduced scan
work. `pushed_group_rows=879856` and `pushed_group_joins=1` confirm the intended
route ran. Its `peak_intermediate_rows=223` counts retained result Relations,
not the entire working set or grouped/hash state; do not interpret it as a
492,986-to-223 reduction in total resident rows.

The next structural target remains scan → date/return filtering → grouped
update, with build-domain filtering transferred as early as SQL semantics
permit. The one-core regression also needs a controlled Bloom-on/off comparison;
this measurement does not isolate how much of that regression is due to lost
Bloom pruning versus consumer/conversion overhead.

## 2026-11-14: Bidirectional predicate transfer before grouped inputs

Eligible grouped LEFT-join chains now transfer composite INT64 key domains
across their GROUP BY boundaries into immutable base scans. The two stages are:

1. Read only the preserved fact's direct group-key columns to build a Bloom
   superset of its possible output keys. Worker-local bitsets are combined after
   their build barrier and shared by the nullable-side scan consumers.
2. Execute those reduced build inputs and finish their aggregates. Project the
   completed hash-build keys into a union Bloom domain, then use it to prune the
   preserved fact before Row decoding, anti joins and grouping.

The seed pass includes unfiltered base keys deliberately: extra candidates and
Bloom collisions only retain unnecessary work. The original joins and WHERE
still decide results. Composite key order is remapped separately for each edge;
only key components common to all nullable sides and direct preserved-base
GROUP BY columns cross the boundary. For example, Q78 can transfer item/customer
without year while retaining the existing date filter. It does not substitute
independent per-column domains for correlated tuples.

The parent must have a total, right-only conjunct that rejects all-NULL nullable
sides. This licenses dropping a complete preserved-side group absent from every
build. Ordinary LEFT joins without that proof retain unmatched rows. Reverse
pruning drops only build groups that cannot match any preserved raw key.
`ProveGroupedTransfer` independently validates each pruned body's computation:
base sources only, no right/full join, direct numeric grouping/aggregate inputs,
total numeric predicates, and no aggregate modifiers. Signed integer SUM uses
actual immutable min/max and a conservative Cartesian join-cardinality bound;
a complete same-domain unique-key edge can tighten that bound. Floating SUM
also rejects non-finite domains and requires enough headroom for rounding in
any merge tree. Unproved bounds keep the original execution, including errors
in groups that the parent would otherwise discard. Statistics estimates do not
justify semantic pruning.

`ScanTupleFilter` owns its bitmap and pins the scan image used by the proof.
`BuildInput` hands it explicitly through `LoadSource`; it is not a global table
filter or a cache entry. `TryParallelTableScan` applies it only to that same
validated immutable image. Unsupported/index/fallback scans may ignore the
optimization. Filtered scans bypass reusable base-Relation cache fills.
Uncommitted writes/version chains, incompatible domains, or memory reservation
failures decline transfer. Seed bitsets and retained filters reserve query
memory. Reverse transfer also declines when total eligible build scans have
fewer than one eighth as many rows as the preserved seed input.

`EXPLAIN ANALYZE` separates `transfer_seed_rows` (extra key-only input pass),
`transfer_scan_tested` (candidates after existing scan filters) and
`transfer_scan_rejected` (Bloom/NULL misses before payload decoding).
`pushed_group_rows` still measures completed groups delivered to consumers.
Reducing decoded values or grouped rows is **not** a claim that all physical
reads vanished: seed key reads and Bloom key probes are additional work and are
included in query timing. This remains a conservative grouped-chain rule, not a
general predicate-transfer graph planner, full scan-to-sink fusion, or a JIT
change.

A/B controls are `TINYLAMB_DISABLE_PREDICATE_TRANSFER` (both directions off)
and `TINYLAMB_DISABLE_REVERSE_TRANSFER` (forward-only). Artifacts and scripts
are under `build/benchmarks/predicate-transfer-20261114/`. Final measurements
and validation follow below.

### Frozen binary and validation

Native SHA-256:
`447a5713512c6d5cb537b356bcf51c351d427e90a083edbe8642c01f01e42dd7`.
All final builds and checks passed, with source hashes retained in
`code-manifest.json` and commands/results in `final-validation.json`:

- Full CTest: 4,005 cases, zero failures, three existing skips.
- ASan/UBSan: 43 targeted cases; TSan: 16 targeted cases.
- SQL oracle: 1,024 seeded iterations.
- Physical-table oracle: 36 Q78 variants against DuckDB under OFF,
  forward-only and bidirectional modes (108 successful comparisons).
  Forward and bidirectional transfer each ran in 12 variants; variants with
  unmatched-preserving predicates, residual join conditions, arithmetic
  aggregate inputs or an INNER parent exercised conservative fallback.
- Layering lint (79 existing allowlisted edges, no new edge), formatting and
  `git diff --check`.

The six new regression tests cover composite-key order, duplicate build
fanout, complete groups with multiple rows, NULLs, empty builds, later writes,
integer and floating SUM overflow on discarded probe groups, overflow on a
reverse-pruned build group, and throwing aggregate arguments.

The timing script uses CPU 0 or CPUs 0–7 (distinct physical cores), one warmup
and seven timed executions per query. Native numbers average forward/reverse
query and mode order medians from the same frozen binary. All answers,
including warmups, are compared with the SF1 DuckDB reference. No project
build, test or fuzzer runs during timing; the seed pass and Bloom construction
are included on every execution, with no result or cross-query Bloom cache.

### Final SF1 comparison

| Q78 mode | 1 physical core (ms) | 8 physical cores (ms) |
|---|---:|---:|
| Native, transfer OFF | 412.381 | 180.810 |
| Native, forward-only | 190.271 | 97.953 |
| Native, bidirectional | **79.943** | **37.535** |
| DuckDB 2.0.0 (`d54cbbe3df`) | 113.927 | 35.466 |

Bidirectional transfer reduces Q78 elapsed time by **80.61% / 79.24%** at
one/eight cores (5.16x / 4.82x speedup over the preceding grouped-output route).
At one core it takes 29.83% less time than DuckDB; at eight cores it still takes
5.84% more time. The earlier standalone 34 ms diagnostic must not replace this
forward/reverse benchmark result to claim an eight-core win.

The eight-core sum of all 99 query medians decreases from **2,767.989 ms to
2,630.349 ms (4.97%)**. Q51 does not enter this rule: OFF/ON is 296.310/300.536 ms
at one core and 195.789/200.193 ms at eight cores; no Q51 speedup is claimed.
Peak process RSS remains approximately **11.21 GiB**, dominated by the fixture
and other retained data. No new all-query DuckDB or ClickHouse total was
collected here. All **412 median records** (3,296 executions including warmups)
passed answer comparison. Raw records are in `final/timings.json`; the rendered
chart is `comparison.png` / `comparison.svg` in the artifact directory.

### Work reduction and remaining profile

Separate eight-core diagnostic runs used `perf record` at 499 Hz for seven
executions in each mode. All 21 profiled answers matched the reference. Their
timings are excluded from the chart; counter snapshots and CPU samples are in
`profile-counters.json` and `profile-validation.json`.

| Q78 work counter | OFF | Forward-only | Bidirectional |
|---|---:|---:|---:|
| Extra key-only seed rows | 0 | 0 | 2,880,404 |
| Transfer-tested scan candidates | 0 | 553,861 | 983,578 |
| Candidates rejected before decoding | 0 | 538,288 | 962,304 |
| Groups delivered to build/probe consumers | 879,856 | 400,824 | 19,119 |
| Scan values decoded | 7,893,930 | 4,125,914 | 1,157,802 |
| Median process CPU (ms) | 860 | 350 | 110 |

The bidirectional rule rejects **97.84%** of its scan candidates, reduces
completed group delivery by **97.83%**, and reduces decoded Values by **85.33%**.
These metrics exclude raw seed/key probes; their costs are included in the
measured elapsed and CPU times. Existing date filters run before the transfer
candidate count, and returns scans still contribute to the remaining work.

The bidirectional profile's largest sampled self-cycle buckets are the scan
worker (~38%) and reverse seed construction (~32%); `vector<Value>` growth is
~1.5%. These are coarse sampled attribution over a short run, not exact phase
wall times. Median active cores fall from about 4.77 to 3.18 while CPU work
falls sharply. One-to-eight-core scaling is still only 2.13x for Q78, compared
with DuckDB's 3.21x in this run. Reducing/parallelizing the remaining seed and
scan work is the next target; eliminating grouped output materialization alone
was not enough to get here.


## 2026-11-14: compressed execution for inner aggregate stages

The dictionary aggregate kernels were restricted to queries with `LIMIT`,
although inner stages of derived tables and CTEs normally need every group.
`TryDictionaryAggregate`, `TryCompositeDictionaryAggregate`, and
`TryDictionaryExpressionAggregate` now accept unbounded output. Both global
and owner-local TopN trimming are conditional on an actual LIMIT. Encounter
order, NULL ordering, and the outer stage's HAVING/LIMIT remain intact.

A validated immutable LSTable snapshot can also feed these kernels when the
base table is shared by sibling queries. Each consumer applies its own
predicates; the fast path neither publishes filtered base rows nor reuses a
filtered cache entry. Snapshot/MVCC validity remains a prerequisite.

Direct aggregates of the sole group key (`COUNT(key)`, `MIN/MAX(key)`, signed
integer `SUM/AVG(key)`) use the dictionary value and group frequency. SUM has
a conservative absolute-value bound over the entire snapshot; AVG additionally
requires an exactly representable integer total (at most 2^53). Unsafe bounds,
casts, unsupported arguments, inner HAVING, and runtime membership filters
retain the ordinary evaluator. NULL keys form a group; COUNT(key) is zero and
SUM/AVG/MIN/MAX are NULL in that group. Small 8-bit dictionaries are also
accepted when their value domain and SQL equality proof match the kernel.

Three EXPLAIN ANALYZE counters distinguish this from merely avoiding Row objects:

- `dictionary_aggregations`: successful dictionary aggregate kernels.
- `dictionary_summary_aggregations`: kernels using snapshot-owned frequencies.
- `dictionary_aggregate_rows`: row positions visited by these kernels; zero
  for a frequency-only fold. `scan_rows` still reports logical input size.

These are storage summaries, not cached SQL results. Changes to the snapshot
invalidate eligibility under the existing snapshot checks. Output groups are
still materialized for the next stage; the entire query is not kept compressed.
For composite keys, individual column frequencies cannot prove joint
frequencies, so actual code tuples are read and partitioned. COUNT(DISTINCT)
also reads actual (group, argument) code pairs. Bounded SUM/AVG of other
integer columns still reads their numeric values.

An unbounded, unfiltered numeric composite whose dictionary-domain product
exceeds the input row count keeps ordinary typed grouping. In a diagnostic
customer/item query, packing/partitioning nearly unique keys made latency
360.9 -> 469.7 ms in the initial diagnostic run; the final gate selects the old
path. This is a cost decision, not a correctness proof or a claim that the
product equals the actual number of groups. Strings, transformed keys, and
selective inputs retain the encoded candidate.

### Measurements

Artifacts: `build/benchmarks/compressed-first-aggregate-20261114/`.
Final binary SHA-256:
`92836430683a2d137127bc840dbefe6485e6a0decce9c8ed0d3f543db71a0107`.
`measure.py`, `sql/*.sql`, `current-micro.json`, `summary.json`,
`profile-counters.json`, and `comparison.png` retain the queries and evidence.

These are **eight additional multi-stage queries on the TPC-DS SF=1 tables,
not the standard TPC-DS query set**. store_sales contains 2,880,404 rows.
CPU affinity is 0 for one core and 0..7 for eight distinct physical cores.
One warm-up plus seven measured executions; tinylamb uses forward/reverse
query and mode orders, and the table averages their per-run medians. DuckDB
2.0.0 uses one warm-up plus seven executions per thread setting, in fresh
processes pinned before interpreter startup. Every worker thread affinity
was checked after every measured query (`duckdb-pinned-*.json`). Native
latency includes Execute/Collect; DuckDB includes execute/fetch/normalization.
Profiling is separate from these timings. All 640 executions were checked
against DuckDB (exact integer/NULL, existing float tolerance). No ClickHouse
comparison was run for this change.

The A/B hatch `TINYLAMB_DISABLE_UNBOUNDED_DICTIONARY_AGG=1` disables the
unbounded dictionary kernels in the same binary. Existing LIMIT kernels stay
enabled. Times below are milliseconds, lower is faster.

| Additional query | Cores | Disabled | Enabled | DuckDB | Disabled / enabled |
|---|---:|---:|---:|---:|---:|
| Item count histogram | 1 | 51.776 | 2.377 | 11.735 | 21.78x |
| Store/item count histogram | 1 | 144.915 | 53.561 | 31.214 | 2.71x |
| Customer/item (fallback) | 1 | 501.511 | 529.570 | 76.978 | 0.95x |
| Group-key aggregates, then aggregate | 1 | 193.628 | 3.073 | 17.782 | 63.01x |
| Shared filtered stages | 1 | 55.595 | 11.860 | 23.299 | 4.69x |
| Quantity count histogram | 1 | 54.814 | 0.063 | 11.935 | 866.27x |
| Distinct customers per item | 1 | 287.486 | 27.099 | 71.067 | 10.61x |
| Scalar dictionary key | 1 | 2.995 | 0.052 | 0.500 | 57.45x |
| Item count histogram | 8 | 11.854 | 2.215 | 3.294 | 5.35x |
| Store/item count histogram | 8 | 66.460 | 22.762 | 10.265 | 2.92x |
| Customer/item (fallback) | 8 | 373.122 | 385.095 | 13.754 | 0.97x |
| Group-key aggregates, then aggregate | 8 | 144.022 | 3.079 | 4.420 | 46.77x |
| Shared filtered stages | 8 | 14.866 | 11.829 | 4.103 | 1.26x |
| Quantity count histogram | 8 | 9.872 | 0.058 | 2.052 | 170.60x |
| Distinct customers per item | 8 | 161.384 | 8.637 | 14.273 | 18.69x |
| Scalar dictionary key | 8 | 0.636 | 0.059 | 0.582 | 10.85x |

For the item, quantity and key-argument summary cases, the initial aggregate
visits **zero row positions and decodes zero input Values**, rather than
2,880,404 input rows. The two shared filtered stages also use summaries.
The store/item and distinct-customer cases visit 2,880,404 encoded positions
but decode zero input Values. This does not eliminate output group creation,
and the physical sidecars currently coexist with uncompressed storage columns.
The customer/item fallback still loses badly to DuckDB; its final enabled
measurements are 3.2% slower at eight cores and 5.6% slower at one core despite
selecting the same grouping path. Do not present this case as a speedup.

### Standard TPC-DS coverage and remaining opportunities

All 99 standard SF=1 queries were run twice and matched the saved DuckDB
answers (198 checked executions). **None invoked a dictionary aggregate
kernel.** Their initial aggregations commonly contain joins, HAVING, floating
payloads, or ROLLUP; those take other paths. `current-official.json` was
collected with diagnostic profiling and is not a new comparative timing
claim. There is no demonstrated standard TPC-DS speedup from this change.

The measured standard-query counters locate further work:

| Query | Decoded scan Values | Relevant current behavior |
|---|---:|---|
| Q23 | 3,915,091 | Five grouped join fusions; COUNT HAVING and subsequent stages remain outside dictionary aggregate selection. |
| Q44 | 7,332,352 | Single-table AVG with HAVING/scalar subquery and nullable floating payload; current bounded integer/count kernels cannot serve it. |
| Q65 | 4,496,240 | Two grouped join fusions; 60,871 intermediate groups feed outer AVG/join processing. |
| Q67 | 2,861,135 | ROLLUP/window path; 4,866,786 logical aggregate input rows and 505,037 aggregate groups. |
| Q97 | 2,525,652 | Grouped join plus FULL JOIN; high-cardinality composite group output still becomes intermediate rows. |

For Q65, the next useful extension is a dictionary-code lookup from fact-side
foreign keys into the filtered unique dimension, followed by key-code
aggregation without first producing joined Rows. Multiple independent
code frequencies cannot replace the actual joint key stream; non-key SUM
payloads still need reading. Q67 additionally needs group-set identity/NULL
separation for ROLLUP, and Q97 needs a compressed/typed intermediate boundary
through FULL JOIN. These are identified but **not implemented** here.

### Validation

See `validation.json` and the associated build/test logs for the final full
suite, sanitizer, SQL-oracle and layering results. The new regressions cover
nested aggregates without LIMIT, parallel composite grouping, transformed
keys, NULLs, empty selection, ordering/offsets, shared filtered inputs,
read snapshots across insertions, and an inner SUM overflow that an outer
LIMIT must not hide.

Final checks for this implementation: full release build and CTest **4,007
cases, zero failures, three existing skips**; ASan/UBSan **36 cases**; TSAN
**9 cases**; seeded SQL oracle **1,024 iterations**; physical-shape oracle
**41 queries × two thread counts × ON/OFF = 164 checked executions**.
All passed. Layering retains the same 79 allowlisted edges with no additions.
