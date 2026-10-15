# Benchmark history

## 2026-10-10 — TPC-H SF=1: bulk raw-count fusion (one map probe replaces the join bucket walk, Q13), single-pread page reads (the 32 KiB body copy per page read removed, suite-wide scan floor), hoisted per-group maps in the fast grouped emit (Q11), morsel-parallel membership fills (Q22) — −5.3% (min-of-8) clean, all rows and all 19,689 value lines identical; the ~20% target was NOT reached — eleventh consecutive TPC-H session. TPC-C untouched (owned by the concurrent txn session, on round 15).

- **Measurement headline:** interleaved paired passes, gold-restore per
  pass, `TINYLAMB_CHECKPOINT_SECONDS=0 TINYLAMB_SKIP_ANALYZE=1`, taskset
  16-31, both sides the SAME snapshot binary with three env hatches
  flipping the levers (`BASE_DISABLE=TINYLAMB_DISABLE_BULK_COUNT=1
  TINYLAMB_DISABLE_FAST_PAGE_READ=1 TINYLAMB_DISABLE_PARALLEL_MEMBERSHIP=1`;
  the emit hoist has no hatch). Set ab3 (min-of-8, quiet machine, load
  3-7, the measuring session running nothing else): **3,485.7 → 3,300.8
  ms, −5.3%**, rows identical in all 16 passes. Full value check: **all
  19,689 output lines identical** (CLI on gold copies, floats rounded to
  4 decimals; the base-vs-base control is also 0 diff lines). An earlier
  set (ab2, min-of-6) measured only −1.6% under the SMT-overlap trap
  below — ab3 is the number of record. Change-certain anchors: Q22
  137.2 → 108.1 ms (−21.2%) and Q13 −5.3% reproduced across sets;
  consistent wins Q2 −22.1%, Q19 −15.1%, Q12 −14.8%, Q15 −12.5%,
  Q18 −12.3%, Q14 −9.5%, Q20 −8.0%, Q17 −7.8%, Q11 −5.9%, Q10 −4.8%;
  Q5 +8.0%/Q4 +6.2% sit on untouched paths inside the round-to-round
  spread.
- **Bulk raw-count fusion (Q13)** (`executor/detail/planning_heuristics.cpp`,
  hatch `TINYLAMB_DISABLE_BULK_COUNT=1`). Once every residual is hoisted
  off the pair path (the 10-09 build-residual hoist) and the group key is
  one probe-side INT64/DATE column, a count-only fusion needs no per-pair
  work at all: each surviving bucket entry contributes exactly +1, so the
  build now produces a `FlatIntKeyMap<BulkCountValue>` (duplicate count +
  the most recently inserted row per key — the row the bucket chain's
  head would have filled a new group's representative from) and the probe
  collapses to one read-only `Find`, one group-map probe and one integer
  add per outer row. The per-pair scratch fills, flat-map probes and
  build-row dereferences disappear; `FlatIntKeyMap::Find` (probe without
  insert) was added for this. Clean interleaved Q13-only alternations: on
  460-481 ms vs off 483-524 ms (−5~12%). The remaining Q13 wall is the
  hoisted residual eval (1.5M × NOT LIKE over the build), the 906K-entry
  build itself, and the 150K representative Row copies.
- **Single-pread page reads** (`page/page_pool.cpp` + `Page::
  DecodeDiskInPlace` in page.cpp/page.hpp, hatch
  `TINYLAMB_DISABLE_FAST_PAGE_READ=1`). `ReadFrom` pread the page into a
  stack buffer and `DecodeDisk` then copied the 32 KiB body into the page
  object — one extra full body copy per page read, sitting next to the
  syscall in scan profiles. The fast path preads straight into the `Page`
  object and fixes the big-endian header scalars up in place. Layout
  contract: the members occupy the same leading offsets as the serialized
  header except that `type` is a full u64 on disk while the member is 1
  byte + 7 padding bytes, and `StoredChecksum` hashes bytes [0,24) ∪
  [32,40) ∪ raw body — so the in-place fixup must restore the exact
  write-time image (native-endian scalars, zeroed padding).
  **The first version hardcoded a 4-byte memset after `type`, but
  `sizeof(enum PageType) == 1`, so bytes [37,40) kept the BE type's high
  half and every clean page read failed its checksum —
  `PagePoolTest.PersistencyWithReset` caught it on the first run.** The
  memset length is now derived from the `type`/`checksum` member
  addresses. On the changed tree: page_pool 43/43, recovery_extra 20/20,
  recovery_manager 85/85, differential 15/15.
- **Hoisted per-group maps in the fast grouped emit**
  (`executor/relational.cpp`, no hatch). The fast emit loop built an
  `AggregateResultMap` (bucket allocation) and a finished-values vector
  per group; with Q11's 31,587 groups that dominated `project_ms`. Both
  are hoisted and re-filled per group (the map keys are the same
  aggregate nodes every iteration, so `insert_or_assign` reuses the
  buckets); the evaluator, the nodes and the three-valued semantics are
  unchanged. Q11 −5.9%.
- **Morsel-parallel membership fills** (`executor/detail/scan_filter.cpp`,
  hatch `TINYLAMB_DISABLE_PARALLEL_MEMBERSHIP=1`). The 10-09 direct-scan
  membership fill boxed one Row per member on one thread — Q22's 1.5M-row
  orders build cost ~90 ms of the filter phase. The fill now splits
  `BuildScanMorsels` chunks across up to 16 workers that collect raw int
  payloads, then merges into the pre-reserved `FlatIntSet` under the same
  distinct-bound guard, so the stale-stats fallback decision stays
  identical to the serial path (overflow verdict included). Fires only
  for the trivial single-table no-WHERE single-column shape at ≥65,536
  estimated rows and ≥4 morsels. Q22 −21.2%.
- **Measurement trap (cost half the session's signal):** running test
  suites (taskset 0-15) while benchmark passes (taskset 16-31) are in
  flight is NOT safe on this box — logical CPUs 16-23 are the SMT
  siblings of physical cores 0-7, so "disjoint" tasksets share physical
  cores and the load-average gate cannot see the overlap. ab2 measured
  −1.6% under exactly this overlap while single probes swung 3× (Q13 "on"
  1,317 ms polluted vs ~470 ms clean). Rule: during rounds the measuring
  session runs NOTHING — not even tests on the other cores. Also:
  rebuilding the benchmark binary mid-harness invalidates the set (ab1
  was discarded for this).
- **Where the remaining time is (after-state, ab3 after-side):** Q13
  (419 ms — orders scan ~95 ms + hoisted residual eval + 906K-entry build
  + 150K representative Row copies); Q9 (394 ms — SIP loads are the
  page-read/decode floor, then four small materializing joins); Q8/Q3/
  Q10/Q20's materializing join chains re-box the SIP-pruned fact stream
  2-7× while the typed fold proves the same volume aggregates at ~6
  ns/row — streaming/fusing the chain or late materialization is the
  structural lever; Q21 (225 ms — lineitem still scanned twice: the fused
  sibling aggregate and l1 share the SIP domain but not the scan); the
  ~70 ms grouped-emit machinery remainder in Q11/Q22. These are the same
  structural levers the 10-08/10-09 entries named; this session's
  micro-levers did not reach them, and 11 sessions of micro-levers have
  now yielded ~-70% cumulative without landing the 20% single-session
  target.

Reproduce (same binary both sides, hatches flip the levers):

```bash
BENCH_BASE=~/develop/tpchopt-1010/build/tinylamb_tpch_benchmark \
BENCH_AFTER=~/develop/tpchopt-1010/build/tinylamb_tpch_benchmark \
BASE_DISABLE="TINYLAMB_DISABLE_BULK_COUNT=1 TINYLAMB_DISABLE_FAST_PAGE_READ=1 TINYLAMB_DISABLE_PARALLEL_MEMBERSHIP=1" \
~/develop/tpch-perf-1010/harness.sh 8 tag
```

## 2026-10-09 — TPC-H SF=1: transitive key-filter propagation through join-key equality classes (Q9 partsupp 800K→63K, −31%), direct-scan membership fill (Q22 −45%), HAVING uncorrelated-subquery fold (Q11 −13%), build-side residual hoist + raw-count count-only fusions (Q13; hoist −45 ms paired) — two paired sets −11.3% (min-of-8) / −12.2% (min-of-6) pooled, all rows and all values identical; the ~20% target was NOT reached — tenth consecutive TPC-H session. TPC-C untouched (owned by the concurrent txn session).

- **Measurement headline:** interleaved paired passes, gold-restore per
  pass, `TINYLAMB_CHECKPOINT_SECONDS=0 TINYLAMB_SKIP_ANALYZE=1`,
  taskset 16-31, both sides the SAME snapshot binary with five env hatches
  flipping the levers (`BASE_DISABLE=TINYLAMB_DISABLE_TRANSITIVE_KEY_FILTER=1
  TINYLAMB_DISABLE_BUILD_RESIDUAL_HOIST=1 TINYLAMB_DISABLE_HAVING_FOLD=1
  TINYLAMB_DISABLE_ARENA_MEMBERSHIP=1 TINYLAMB_DISABLE_COUNT_STAR_FUSION=1`).
  Set ab4 (min-of-8, window load 2.6–7.2): **3,688.7 → 3,271.3 ms,
  −11.3%**; set ab5 (min-of-6, load 3.7–10.5, adds the raw-count fusion):
  **3,882.8 → 3,409.0 ms, −12.2%**; USER-time medians agree (ab5 11.39 →
  10.36 s, −9.0%). Change-certain anchors: Q9 586.0 → 401.5 (−31.5%) and
  Q22 270.7 → 148.8 (−45.0%) reproduced in both sets; consistent wins
  Q10/Q11/Q12/Q14/Q15/Q17/Q19 (−12…−18%), Q18 −8%, Q20/Q21/Q4/Q2 −5…−20%.
  The machine was co-tenant-loaded (sibling tinylamb txn session + an
  LSTable health sweep, load 30–70) for most of the session; only the
  quiet windows are recorded here.
- **Transitive key-filter propagation (the Q9 headline)**
  (`executor/detail/planning_heuristics.cpp`, BuildInput; hatch
  `TINYLAMB_DISABLE_TRANSITIVE_KEY_FILTER=1`). The SIP key-filter driver
  loop only considered *directly loaded* join partners, so a relation whose
  only filter source loads later went unpruned: Q9's partsupp (loaded 4th
  by size) has `ps_partkey = l_partkey` / `ps_suppkey = l_suppkey`, both
  referencing lineitem which loads 5th — it materialized all 800K rows
  (165 ms load) and then paid a 122 ms fused-join build over them. The fix
  builds a union-find over (relation, column) nodes from resolved
  two-source integer equality predicates — every such predicate is applied
  by the greedy join loop, so a class's columns are value-identical in the
  final result and pruning a not-yet-loaded member to a loaded member's
  domain only removes rows no surviving row could match; ambiguous
  pairings (a name resolving in both relations) never enter the classes.
  When no direct partner is loaded, the virtual-driver pass walks the
  partner column's class for a loaded member and applies the same
  soundness rules (type match, projection mapping, near-full-domain skip).
  Driver *selection* adds a coverage heuristic — distinct keys over the
  observed min/max span — because Q9's supplier (10K rows, full suppkey
  span, coverage 1.0) otherwise beat part (11K LIKE-filtered rows,
  coverage 0.056, prunes 95%) on the old "fewer driver rows" tiebreak.
  partsupp 800,000 → 63,184 rows; statement `scan_rows` 2,121,210 →
  1,384,394; the fused join's build moved to orders (222K).
- **Direct-scan membership fill (Q22)** (`executor/detail/scan_filter.cpp`,
  `TryCompilePrebuiltMembership`; hatch
  `TINYLAMB_DISABLE_ARENA_MEMBERSHIP=1`, name kept from the first
  attempt). A trivial uncorrelated membership subquery — one base-table
  source, no WHERE, one bare INT64/DATE column — executed the whole
  relational pipeline (Row boxing per member) inside filter compile:
  Q22's `c_custkey NOT IN (SELECT o_custkey FROM orders)` paid ~190 ms of
  filter phase for 1.5M members. The peel now reads the column straight
  through `BeginFullScan` with a one-column projection into the FlatIntSet.
  The set cannot grow, so the reserve upper-bounds the distinct keys:
  ANALYZE row count, with a running guard (`Size() >= reserve`) that
  rebuilds generically on the stale-stats case. An earlier variant looked
  the column up in the typed-fill `base_relation_arenas` instead — dead:
  single-consumer tables never get a published arena (the reusability
  gate), and building one for them re-runs the 2026-10-04 cross-query
  cache negative. Q22 270.7 → 148.8 ms; **FlatIntSet without a covering
  `Reserve` SEGVs in `Insert`** (build-once, no growth) — the fallback
  path must keep its `Reserve(members->TotalRows())`.
- **HAVING uncorrelated-subquery fold (Q11)**
  (`executor/relational.cpp`, grouped emit; hatch
  `TINYLAMB_DISABLE_HAVING_FOLD=1`). The scan-filter fold never reached
  HAVING, so every group re-probed its uncorrelated scalar through the
  cache: Q11's `HAVING sum(...) > (SELECT sum(...)) * 0.0001` = 31,587
  groups × AST-with-subquery walk (`project_ms` 84 ms). The fold runs once
  before the per-group emit under the same contract as
  `FoldRowIndependentSubqueries` (uncorrelated volatile-free nodes are
  statement-invariant; raising nodes keep their original form). Q11
  181.2 → 157.4 ms. The remaining ~70 ms of `project_ms` is the group-emit
  machinery itself (accumulator finish + scope + select-list eval per
  group) — the next structural lever there.
- **Build-side residual hoist (Q13)**
  (`planning_heuristics.cpp`, TryFusedGroupedJoin; hatch
  `TINYLAMB_DISABLE_BUILD_RESIDUAL_HOIST=1`). A residual conjunct loading
  only right-side cells (Q13's `o_comment NOT LIKE ...` under customer
  LEFT JOIN orders) is row-invariant on the probe side; the fused join now
  evaluates it once per build row into a keep-vector and drops failing
  rows from the build pointer list (throw-safe gate
  `ExpressionCannotThrowForPlanner`; compiled kFilter residuals cannot
  reference outer scope). **The first version pre-filtered via
  `TryParallelFilterRows` and was net-negative (+58 ms on Q13's join_ms):
  the filtered relation re-copies ~906K surviving rows, the same trap the
  2026-10-06 build-prefilter entry recorded.** The pointer-list version
  (no copy) measured −45 ms paired (496/510 → 452/462 ms, same window).
  Pooled effect on Q13 stayed inside window noise (ab5 showed Q13 +5.1%,
  its base/after mins landing in different load pockets; the paired
  isolation above is the reliable signal).
- **Raw-count count-only fusions** (`planning_heuristics.cpp`, hatch
  `TINYLAMB_DISABLE_COUNT_STAR_FUSION=1`). A fusion whose aggregates are
  all `COUNT(*)` — or all `COUNT(bare not-null column)` (kNotNull /
  kPrimaryKey constraint, no FILTER/ORDER BY inside the aggregate) —
  accumulates one `int64_t` per group instead of driving the accumulator
  machinery: no per-pair `Value(1)` boxing, no per-group state
  construction, integer-add merge. The two shapes differ on unmatched LEFT
  rows: COUNT(*) counts the null-extended row (+1), COUNT(not-null column)
  counts nothing (+0). Verified value-identical on Q13 (full result diff
  vs all-hatches-off base).
- **Correctness:** all 22 queries row-identical in every pass of all
  three paired sets (ab3, ab4, ab5). Full value-level check: all 22
  queries through the `tinylamb` CLI on gold copies, after vs
  all-hatches-off base — **19,689 output lines identical after rounding
  floats to 4 decimals** (the raw diff is the known last-ULP sum-order
  jitter of parallel sums). Suites on the merged tree: differential 15/15,
  query 178/178, relational_extra 3/3, sql_engine_tpch 8/8,
  sql_subquery_shapes_coverage 11/11, executor_feature 91/91,
  join_reduction 12/12, expression_eval_coverage 8/8. clang-format
  applied to the three touched files.
- **Concurrent-session protocol:** the txn/recovery lane
  (recovery/, transaction/, query/sql_engine.cpp, benchmark/tpcc_*) was
  owned by a sibling session throughout (its round-14 TPC-C work + a
  main-tree `relational.cpp` one-liner at 05:35 and a mid-flight
  `Row::Count()` rename touching `scan_filter.cpp`/`type/row.hpp` at
  08:33/08:48). All work ran in an rsync snapshot
  (`~/develop/tpchopt-1009`, clang RelWithDebInfo); merge-back re-applied
  my hunks onto the sibling's latest scan_filter.cpp / relational.cpp
  (both files compile and pass the suites above in the snapshot with the
  sibling's current `type/row.hpp`).
- **Where the remaining time is (after-state):** Q13 (428 ms — orders scan
  ~95 ms + fused pair loop ~210 ms at ~230 ns/pair + derived
  materialization ~29 ms); Q9 (401 ms — lineitem 889K and orders 222K SIP
  loads are the page-read/decode floor, then four small materializing
  joins ~30 ms each); Q11/Q22 (~70 ms `project_ms` each = the grouped
  emit machinery); Q21 (280 ms — the EXISTS/NOT EXISTS pair still builds
  lineitem twice; the canonical-layout sibling share from the 2026-10-08
  note remains unbuilt).

Reproduce (same binary both sides, hatches flip the levers):

```bash
BENCH_BASE=~/develop/tpchopt-1009/build/tinylamb_tpch_benchmark \
BENCH_AFTER=~/develop/tpchopt-1009/build/tinylamb_tpch_benchmark \
~/develop/tpch-perf-1009/harness.sh 6 tag   # BASE_DISABLE carries the 5 hatches
```

## 2026-10-08 — TPC-H SF=1: correlation-domain pruning for multi-source correlated builds (Q2 −80%+), single-LIKE pair-loop specialization, flat membership sets, fused-join single-int group keys (three paired sets −14.3% / −9.9% / −6.7% min-of-N pooled under a loaded machine; the ~20% target was NOT reached — ninth consecutive TPC-H session)

- **Q2 lever (the headline): correlation-domain pruning for MULTI-source
  correlated builds** (`executor/detail/subquery_runtime.cpp`). The shape
  `ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier,
  nation, region WHERE p_partkey = ps_partkey AND ...)` builds its
  `CorrelatedIndex` through `BuildInput`, which re-executed the whole
  4-table join once per statement — ~800K partsupp rows materialized inside
  the filter phase (Q2 filter_ms ≈ 348 ms; callgrind showed memset+memcpy
  dominating the statement). The multi-source build branch now: (1) erases
  any `table_key_filters` entries owned by the subquery statement, (2) finds
  the first correlation equality (`local col = outer col`, local resolving
  in exactly one base-table source via alias-qualified schema peek), and
  (3) when the outer relation is registered (`ScopedCorrelatedOuter` — the
  same soundness contract the single-source domain filter uses), extracts
  the outer key domain with the existing `CorrelationDomainFilter` and
  stashes it as a `TableKeyFilter` (owner = the subquery statement) so
  `BuildInput`'s own scan of the correlation table rejects foreign keys
  pre-decode: 800K → ~16K rows. The cache entry records
  `outer_filter_relation/serial`, so a probe from any other context rebuilds
  (same reuse guard the single-source path applies); self-joined correlation
  tables are not stashed (one column's domain cannot prune both sides); the
  shared `TINYLAMB_DISABLE_CORRELATED_DOMAIN_FILTER=1` hatch disables the
  new path too (the stash routes through the same filter builder). Q2 wall
  398.8 / 387.3 / 370.6 → 81.8 / 68.7 / 50.3 ms (min-of-5/5/4 across the
  three sets; filter_ms 348 → 16 ms single pass) — ≈ −8 points of the suite
  and the change-certain anchor.
- **Single-`col [NOT] LIKE 'const'` pair-loop specialization**
  (`executor/detail/planning_heuristics.cpp`): a 3-instruction residual
  program (kLoadColumn / kLoadConstant / kBinaryVarchar(kLike|kNotLike),
  constant collation zero) classifies into a direct predicate — the fused
  join's pair loop reads the cell and calls the segment matcher, skipping
  the interpreter round-trip and `TryEvaluateBinary`'s dispatch chain. NULL
  or collated cells fall back to the program paths, mirroring the fast
  slot's refusal contract (the canonical verdict for those inputs is
  unchanged). Q13 join_ms 301 → 258 ms isolated; Q13 in-suite
  −3.1 / −3.8 / −11.8% across the sets.
- **Flat membership sets** (`executor/detail/scan_filter.{hpp,cpp}` +
  `FlatIntSet` in `executor/detail/flat_int_key_map.hpp`): the
  `col [NOT] IN (uncorrelated subquery)` prebuild now backs its member set
  with an open-addressing build-once int table (no tombstones, no growth)
  instead of `unordered_set<Value>` — the peel is restricted to INT64/DATE
  domains whose type and unsigned tag the probe statically pins, so the raw
  int payload is the exact key (DATE day counts included). Q22's 950K-row
  orders build and Q20's 800K partsupp build drop the per-element node
  allocation; the runtime per-row membership build (`uncorrelated_membership
  _by_node`) is untouched and still node-based.
- **Fused-join single-int group keys** (`planning_heuristics.cpp`): when the
  fusion groups by exactly one plain INT64 (non-unsigned) or DATE column
  (Q13's `c_custkey`), per-worker accumulation probes a per-worker
  `FlatIntKeyMap<size_t>` with the raw cell instead of hashing a canonical
  Value vector per pair. NULL keys keep the generic map (which groups
  NULLs), the two key spaces are disjoint, only the group insert pays the
  canonical-key construction, and the stored group key row matches the
  generic form so the post-join merge dedups unchanged.
- **Measurement:** interleaved paired passes on the gold SF=1 fixture
  (`~/develop/tpch-perf-1008/harness.sh`, fresh gold copy per pass,
  `TINYLAMB_CHECKPOINT_SECONDS=0 TINYLAMB_SKIP_ANALYZE=1`, taskset 16-31),
  base = pristine pre-session tree binary, after = this stack, alternating
  pass order, min-of-N per query. The machine never went quiet: the
  sibling session's libfuzzer campaign (load 7–16) ran through set 1
  (min-of-4: **3,870.6 → 3,315.6 ms, −14.3%**), then its ctest sweep +
  `ironlamb-tpch-bench --sf1` through sets 2–3 (min-of-4: 4,290.3 →
  4,000.8, −6.7%; min-of-5: 4,172.0 → 3,759.8, **−9.9%**). Rows identical
  on all 22 queries in every pass of all three sets. Reliable per-query
  signal: Q2 −79.5/−82.3/−86.4% (all three sets) and Q13
  −3.1/−3.8/−11.8%; the swing regressions (Q15 +30.3%, Q20 +11.1%,
  Q18 +9.6% in set 3; Q1 +17.4% in set 2) sit on untouched code paths and
  inside the co-tenant window spread — single-table queries moved −13/+30%
  between sets with no code change touching them.
- **Values:** all 22 queries byte-identical to the pre-session binary after
  rounding floats to 4 decimals (19,689 output lines; the raw diff shows
  only the known Q9 last-ULP sum-order jitter, reproduced base-vs-base on
  fresh copies).
- **Tests:** executor 198/198, query 178/178, differential 15/15,
  sql_engine_tpch 8/8, sql_subquery_shapes 10/10, flat_int_key_map 3/3,
  expression_eval coverage+sweep 8/3, join_reduction 12/12,
  late_materialization 2/2, analytical_features 29/29 — on the merged
  tree; clang-tidy clean on subquery_runtime.cpp; clang-format applied.
- **Concurrent-session protocol:** the recovery lane (recovery/,
  index/) was owned by a sibling session throughout (recovery_manager.cpp
  edited at session start; fuzzer campaign validation). `subquery_runtime.
  {cpp,hpp}` and `flat_int_key_map.hpp` were ALSO edited mid-session by the
  sibling (sibling-EXISTS build sharing + a `Prefetch` empty-map guard):
  merged 3-way with `git merge-file` (their regions taken verbatim — all
  three conflicts sat inside their exists-sharing restructure), plus the
  header they added fields to taken wholesale. Merged tree builds warning-
  clean and passes the suites above; one merge artifact
  (`index->int_rows.end()` left next to their `exist_rows.begin()`) was
  caught by the main-tree build and fixed before merge-back.
- **Where the remaining time is:** Q9 (~526 ms; 4-hop materializing join
  chain ahead of the fused grouped join — scan 247 + join 198 + the
  unattributed materialization; the known derived-table flattening hatch is
  1.7× slower pending bridge join-order quality) is the next structural
  lever; Q13's residual loop is now ~220 ns/pair (fill + direct LIKE +
  flat-key accumulate — mostly floor); Q22's filter still pays the
  membership-build execution (~orders scan + flat set) inside the filter
  phase; Q18/Q21's fused paths are within noise of their 10-07 state.

Reproduce:

```bash
BENCH_BASE=.../tpch-perf-1008/bin-base/tinylamb_tpch_benchmark \
BENCH_AFTER=.../tpch-perf-1008/tinylamb_tpch_benchmark.after2 \
~/develop/tpch-perf-1008/harness.sh 4 tag
```

## 2026-10-07 — TPC-H SF=1: relational route becomes the grouped-route default
(route-agnostic audit completed) + fused grouped join LEFT-join row-loss fix +
uncorrelated membership peel + VARCHAR/DATE compare fast interpreter (suite
−6.0% min-of-4 pooled, all 22 rows identical; the ~20% target was NOT reached
— eighth consecutive TPC-H session; a correlated-scalar pre-aggregation peel
was built, measured net-negative for Q2 as shipped, and left opt-in)

- **Route flip (the headline lever).** The 2026-10-06 route decision record
  left the flip "unblocked but pending a route-agnostic test audit". This
  session ran the audit: the full ctest suite (3802 tests) under
  `TINYLAMB_GROUPED_ROUTE=relational` came back 3797/3799, with the two
  failures decomposed below. The default in `query/sql_engine.cpp` now
  prefers the relational route; `TINYLAMB_GROUPED_ROUTE=cascades` restores
  the old Cascades-first order. Quiet-machine directional passes:
  Cascades-first 5,538 ms vs relational 4,911 ms (−11.3%, single cold passes,
  taskset 16-31). The two shape-pin tests that previously skipped under the
  hatch env (`CascadesRulesCoverageTest.GroupedSqlSelectsHashAndStreamAlternatives`,
  `SqlEngineCoverageTest.NullableUniqueDoesNotProveGroupDependency`) now skip
  unless the cascades hatch is set — under the flip their EXPLAIN pins only
  hold there.
- **Route-attributable audit failure, root-fixed:**
  `SqlJoinFuzzer.SeededJoinsMatchMirror` seed 4 lost one null-extended row
  through `LEFT JOIN ... ON f.k = d.k AND d.tag = 'yy'` when every bucket
  pair for an outer row was residual-rejected. Root cause: all three
  `TryFusedGroupedJoin` probe loops (integer/composite/string keys) set
  `matched = true` on bucket entry, BEFORE the residual verdict — a
  residual-rejected pair is not a match, so `unmatched_left` never fired and
  the outer row silently vanished from fused aggregates (`COUNT(*)` lost the
  row; row-level SELECT was correct because the plain LEFT join path was
  fine). `process_pair` now returns whether the pair accumulated, and
  `matched` ORs that in. The same fusion runs under the old default (it is
  shared code), so the fix benefits both routes.
- **Prebuilt membership peel** (`executor/detail/scan_filter.cpp`,
  `TINYLAMB_DISABLE_PREBUILT_MEMBERSHIP=1` restores the old path):
  `col [NOT] IN (uncorrelated single-column subquery)` compiles, at
  filter-compile time, to one immutable `unordered_set<Value>` built through
  the runtime's own uncorrelated-execution cache — the conjunct leaves the
  residual entirely (matching the 3VL exactly: NULL members, NULL probes,
  and the decisive empty set). Compile-time env plumbing
  (`FilterCompileEnv`) reaches the LoadSource, FilterRelation and
  FinishQuery filter sites. Q22 wall 341 → 252 ms (quiet single pass);
  min-of-4 pooled −4.8%. Honest decomposition: the previous "serial
  contextful filter" cost was mostly the orders membership BUILD (same
  execution either way), not per-row dispatch — the runtime already only
  probed the ~19K rows that survived the leading simple compares, so the
  parallel-filter win is modest, not the −100–150 ms the 2026-10-06 entry
  estimated.
- **kBinaryVarchar / kBinaryDate in the boxing-free interpreter**
  (`expression/bytecode.cpp`): `FastSlot` carries a string_view (collated
  strings never enter it; they stay canonical), and the two opcodes delegate
  to the same `TryEvaluateBinary` the canonical VM calls, so compare/LIKE
  semantics are instruction-for-instruction identical. Differential runs
  both evaluators on every compiled cell as before.
- **Negative result (measured, kept opt-in):
  `PrebuiltScalarComparePredicate`** (`TINYLAMB_ENABLE_PREBUILT_SCALAR=1`
  enables): `col op (correlated scalar min/max/count/sum/avg subquery)` with
  exactly one INT64/DATE correlation equality peels to a key->value map by
  executing the subquery rewritten as GROUP BY correlation column. The peel
  is semantically correct (executor tests pin the three-valued edge cases)
  and kills the per-key correlated applies (Q2 probes 561 → 0), but the
  grouped rewrite re-pays the 4-table join the correlated index had already
  materialized once, so Q2 measured +170 ms net as shipped (589 vs ~420 ms)
  — the correlated-apply machinery beats one extra full grouped execution
  until the build can share the statement's own join. Default OFF.
- **Measurement:** interleaved paired passes on the gold SF=1 fixture
  (`~/develop/tpch-perf-1006/harness.sh`, fresh gold copy per pass,
  `TINYLAMB_CHECKPOINT_SECONDS=0 TINYLAMB_SKIP_ANALYZE=1`, taskset 16-31),
  base = pre-session binary (Cascades default), after = this stack,
  alternating pass order, min-of-4 per query under co-tenant load 3–20:
  **3,997.9 → 3,757.2 ms (−6.0%)**, all 22 row counts identical in all 8
  passes. Reliable per-query wins: Q3 −43.5%, Q19 −24.1%, Q16 −19.1%,
  Q8 −14.4%, Q20 −13.3%, Q12 −8.8%, Q14 −9.4%, Q4 −10.2%, Q22 −4.8%;
  Q7/Q11/Q15 "+9–10%" swings sit inside their ±40% round-to-round spread
  (co-tenant noise). A quiet single cold pass of the merged tree totals
  3,987 ms vs the base binary's 5,538 ms — the min-of-N protocol erases the
  base's cold-page-in penalty and is the number of record.
- **Not this session's failures:** `RecoveryExtraTest.
  PartialLoserUndoThenAnchorRestoreKeepsIndexConsistent` fails identically
  without the route hatch (pre-existing on the working tree; the concurrent
  session owns the recovery lane and was editing recovery_manager.cpp
  during this session), and `SqlStatementSweepTest.ErrorTable`'s
  `TRUNCATE TABLE tiny` expectation went stale when the concurrent session's
  TRUNCATE support landed (the statement no longer raises). Both are outside
  this session's files.
- **Concurrent-session protocol note:** the tree was edited throughout by a
  second session (MERGE INTO in query/, recovery/page fixes). All executor /
  expression work ran in an rsync snapshot (`~/develop/tpchopt-1007`,
  clang, RelWithDebInfo) and merged back file-by-file after mtime checks;
  `query/sql_engine.cpp` was re-applied as a hunk, not copied. A GCC-only
  designated-initializer order warning in transaction_manager.cpp was fixed
  in the snapshot only (clang does not flag it; left for the owning lane).

Reproduce:

```bash
BENCH_BASE=.../pre-session/tinylamb_tpch_benchmark \
BENCH_AFTER=.../tpchopt-1007/build/tinylamb_tpch_benchmark \
~/develop/tpch-perf-1006/harness.sh 4 tag
# cascades escape hatch for the old default:
TINYLAMB_GROUPED_ROUTE=cascades <benchmark> ...
```


Measurements of tinylamb's bundled TPC-C and TPC-H drivers. These are
engineering numbers, not audited TPC results (no think/keying time, no
auditor, no 2-hour TPC-C window, no TPC-H power/throughput mix).

## 2026-10-06 (2nd session) — TPC-H SF=1: row-independent subquery fold at filter-compile time + typed byte-prefix `substr(...) IN` peel + same-table-only cache snapshot (suite −13.2% min-of-8 pooled, per-query rows identical in all 22 passes; Q22 −44% reproduced in all four A/B sets; the ~20% target was NOT reached — seventh consecutive TPC-H session; a fused-join build-residual prefilter was measured and reverted)

- **Changes (all default-on in `executor/detail/scan_filter.cpp`, each with
  an env hatch; measured as one paired set):**
  1. **`FoldRowIndependentSubqueries`** (`TINYLAMB_DISABLE_SUBQUERY_FOLD=1`
     restores the old path): `LoadSource` and `FilterRelation` fold every
     uncorrelated, volatile-free scalar and EXISTS subquery in scan
     predicates to the constant one probe returns, before
     `CompileScanFilter`. The value is row-invariant by the same contract
     the per-row caches rely on (`uncorrelated_scalar_by_node` /
     `uncorrelated_membership_by_node`); a node whose evaluation raises
     keeps its original form, so per-row error behavior — including
     conjunct-tolerant suppression — is unchanged. Correlated nodes never
     fold (`ContainsOnlyUncorrelatedQueries` gate). Q22's
     `c_acctbal > (SELECT avg(c_acctbal) ...)` becomes a typed double
     compare; its 61,128 per-row scalar probes drop to ~19K membership
     probes. Q2's `ps_supplycost = (SELECT min(...))` stays per-row (its
     subquery references outer `p_partkey` — correlated, correctly not
     folded).
  2. **`PrefixInPredicate`** (`TINYLAMB_DISABLE_PREFIX_IN=1`):
     `substr(col, 1, k) IN (c1..cn)` compiles to a byte-prefix membership
     test in the simple-predicate family (evaluated by `MatchScanFilter`,
     `MatchScanFilterWorker`, and inside OR branches). Soundness: every
     prefix must be plain ASCII — a leading multi-byte code point can then
     never match, and behind an ASCII first byte one code point is one
     byte, so the byte-prefix verdict equals the AST code-point substr
     verdict for every input, NULL and short strings included. Start
     offsets ≠ 1, non-int constants, mixed prefix lengths, and multi-byte
     prefixes stay on the residual path. Q22's 7-country filter leaves the
     residual entirely (filter_ms 349 → 241 ms).
  3. **Cached-relation snapshot narrowed to same-table re-entrancy** (no
     hatch; pure guard narrowing): the cache-hit filter path snapshotted
     the whole cached relation whenever any residual contained a query;
     only queries that re-enter `LoadSource` *for the same cache_key*
     break a spill-file read position. `ContainsReentrantQueryIn` checks
     `CountStatementTables` of each subquery against `source.table`, so an
     uncorrelated membership probe over *another* table (Q22's
     `c_custkey NOT IN (SELECT o_custkey FROM orders)`) no longer forces a
     150K-row copy per scan (filter_ms 241 → 200 ms, Q22 −15 ms).
- **Negative result (measured, reverted):** pushing build-only fused-join
  ON-residuals into a pre-build filter (`TryFusedGroupedJoin`). The
  mechanics worked (`[fusion] build prefilter: 1 conjuncts, 915657 ->
  905814 rows`) but Q13's `o_comment NOT LIKE '%special%requests%'`
  passes ~99% of orders, so the extra full shard-copy pass cost ~+190 ms
  against ~90 ms of per-pair residual it removed. Q13's real lever is a
  cheaper per-pair predicate (kLike in the boxing-free VM) or an
  earlier-pruning ON plan, not relocation.
- **Measurement:** interleaved paired passes on the gold SF=1 fixture
  (`~/develop/tpch-perf-1006`, fresh gold copy per pass,
  `TINYLAMB_CHECKPOINT_SECONDS=0 TINYLAMB_SKIP_ANALYZE=1`, taskset 16-31),
  same binary both sides with the env hatches flipping the levers,
  alternating pass order per round. Four A/B sets (3+3+3+2 rounds, 11
  rounds per side) under co-tenant load 4–40 (the second session's TPC-C
  soak was running throughout): set deltas −6.8 / −3.6 / −13.8 / −10.3%
  tracking window load; pooled **min-of-8 per query: 4,285.7 → 3,718.5 ms
  (−13.2%)**, all 22 queries improved, row counts identical in all 22
  passes. The change-certain anchor is Q22: −39.2 / −37.0 / −44.1 / −44.0%
  across the four sets (416 → 233 ms, ≈ −4 points of the suite);
  single-round deltas on untouched queries (Q13 −0.2%, Q7 +0.5%, Q5 −0.2%
  in the quiet-window set) bound the window bias — the headline −13%
  includes a favorable-window component the pooled mins cannot fully
  remove. Single-query sanity along the way: Q22 411.7 → 281 → 263 ms as
  the three levers landed (TPCH_PROFILE: filter_ms 441 → 349 → 241 → 200).
- **Tests:** executor 195/195 (4 new: scalar fold, membership+EXISTS fold,
  correlated-stays-per-row, prefix-IN peel vs substr verdicts incl.
  multi-byte/NULL/short), differential 15/15, sql_engine_tpch 8/8, query
  175/175.
- **Where the remaining time is (updated):**
  - **Q22 (233 ms)**: filter_ms ~200 of it — the residual NOT-IN
    membership probes (19K rows) still pay contextful `TryEvaluate`
    dispatch per row, and the whole filter stays serial because the
    residual bytecode cannot compile kQueryExp. The sketched-but-unbuilt
    lever is a prebuilt-cache worker mode: fold/prebuild all row-side
    caches at compile time, make the hit-path counters atomic, and let
    `TryParallelFilterRows` run the contextful `MatchScanFilter` in
    workers with per-row defer on raise (est. −100–150 ms here, and it
    would generalize to Q2/Q17-class filters).
  - **Q2 (~340 ms)**: 561 correlated-apply executions of the 4-table min
    subquery, ~0.5 ms each behind per-key string-keyed cache probes. The
    structural fix is the classic min-correlated-subquery →
    pre-aggregated semi-join rewrite (planner project, executor lane).
  - **Q13 (~430 ms)**: 915K per-pair ON-residual LIKE evaluations in the
    fused LEFT join (99% pass rate) — per-pair cost is VM dispatch, not
    the LIKE matcher (already a memcmp/memchr segment scan).
  - **Q9 (~490 ms)**: unchanged diagnosis — 4-hop materializing join
    chain ahead of the fused grouped join.

Reproduce (base = the same binary with both hatches set):

```bash
BENCH_BASE=.../build/tinylamb_tpch_benchmark \
BENCH_AFTER=.../build/tinylamb_tpch_benchmark \
BASE_DISABLE="TINYLAMB_DISABLE_SUBQUERY_FOLD=1 TINYLAMB_DISABLE_PREFIX_IN=1" \
~/develop/tpch-perf-1006/harness.sh 3 tag
```

## 2026-10-06 — OR-branch common-equijoin edges in the relational planner (TPC-DS relational route 48.0s → 7.26s, back at parity with the Cascades default 7.07s; TPC-H route A/B on the same gold DB: relational 12.9s vs default 20.8s with Q3 4.0s → 0.26s and Q19 3.9s → 0.31s)

- **Change:** `SynthesizeDisjunctEqualityPredicates`
  (`executor/detail/planning_heuristics.cpp`) turns every `col = col`
  equality implied by ALL branches of a deterministic disjunction into a
  synthetic two-source equi-join predicate. The disjunction implies the
  equality, so the pairs feed the greedy connectivity graph, the runtime
  key-filter (SIP) loop and the per-step `EqualityKeys` hash extraction
  soundly; the original disjunction stays as residual and re-verifies every
  matched pair. TPC-DS q13 (`ss_hdemo_sk = hd_demo_sk` /
  `cd_demo_sk = ss_cdemo_sk` shared by all 3 OR branches) stops degrading to
  nested loops under `TINYLAMB_GROUPED_ROUTE=relational`.
- **Measurement:** TPC-DS SF=1 in-process fixture, single runs (co-tenant
  load), `TINYLAMB_SKIP_ANALYZE=1 TINYLAMB_CHECKPOINT_SECONDS=0`, all 94
  queries row-count identical on both routes and consistent with history.
  TPC-H SF=1 on the same gold DB (loaded 2026-10-06 from the pinned
  `/tmp/tpch-gold/sf1.db.tpch-data` .tbl dump), both routes sequential —
  not interleaved pairs, so treat deltas as directional; per-query rows
  matched across routes on all 22 queries.
- **Route decision record:** with q13 no longer a relational-route blocker,
  the grouped-route default (Cascades-first) is TPC-H-unfavorable (−38%
  suite) while TPC-DS is at parity. Flipping the default needs a full ctest
  pass under `TINYLAMB_GROUPED_ROUTE=relational` plus a per-query audit of
  where the Cascades route still wins (2026-10-06 ctest-under-hatch run is
  recorded in docs/next-actions.md 第18報).

## 2026-10-05 — TPC-H SF=1: parallel relation-filter lowered to 32K rows (+ deterministic chunk rotation), page-read zero-scan pre-check (suite −3.0% min-of-4; the ~20% target was NOT reached — sixth consecutive TPC-H session; entry records two landed executor/page fixes plus a recalibrated map of where the remaining wall time sits)

- **Changes (both default-on, measured as one paired set):**
  1. `TryParallelFilterRows` (executor/detail/scan_filter.cpp): the
     relation-filter path refused to parallelize below **262,144 rows** and
     scaled workers at rows/65,536, so every base relation in the
     32K–262K band (part 200K, customer 150K — the LHS of most TPC-H
     joins/filters) ran its post-cache filter **serially**. Threshold →
     32,768, workers → rows/8,192. The per-chunk atomic counter also handed
     chunks to whichever worker grabbed them, so filter output row order
     changed run to run; chunks are now **static rotation** (worker w takes
     chunks w, w+W, …), the same determinism fix the parallel table scan
     got in 2026-10-01 — shard contents become a pure function of the
     input, which FP-sensitive aggregates read as exact bits.
  2. `PagePool::ReadFrom` (page/page_pool.cpp): every page read paid a full
     256 KiB `all_of` zero-scan before decode (fresh-sparse-region
     detection). A page whose 48-byte header holds any non-zero byte cannot
     be all-zero, and every readable page carries a non-zero magic there —
     the full scan now runs only for the (rare) zero-header case.
     Classification semantics unchanged (torn/corrupt/free-page paths
     identical).
- **Measurement:** interleaved paired passes on the gold SF=1 fixture
  (`~/develop/tpch-perf-1005`, fresh dir copy per pass,
  `TINYLAMB_CHECKPOINT_SECONDS=0 TINYLAMB_SKIP_ANALYZE=1`, taskset 16-31,
  both binaries built back-to-back from one tree differing only in the two
  hunks). min-of-4 per query, load mostly 2–17: suite total
  **4,763 → 4,619 ms (−3.0%)**, all 22 row counts identical in all 8 passes.
  Per-query: Q13 −12.1% (495→439 ms), Q19 −8.9%, Q22 −7.9%, Q20 −7.8%,
  Q8 −7.2%, Q11 −6.9%, Q16 −5.3%, Q14 −5.1%, Q18 −4.0%, Q21 −3.2%;
  single-table scans (Q1/Q6/Q15/Q17) within ±6% noise, as expected — they
  never touch the relation-filter path. Tests on the merged tree:
  differential 15/15, executor 191/191, page_pool 42/42, sql_engine_tpch
  8/8, query 175/175.
- **Where the remaining time actually is (phase-level, this session's
  profiling):**
  - **Q22/Q2 are Drain-bound, not prepare-bound.** Q22 wall ≈ 674 ms of
    which `SqlEngine::Execute` is ~85 ms and `QueryResult::Drain()` (the
    actual parallel execution) is ~590 ms; a second execution in the same
    process replans in 0.1 ms via the compiled-plan cache but still pays
    the full Drain. The benchmark runs each query once per process, so
    plan-cache wins are invisible to it by construction.
  - **Q22's customer filter is still serial**: its WHERE carries scalar/
    membership subqueries, `CompileWorkerFilter` returns incomplete
    programs, and the whole 150K-row pipeline falls back to the contextful
    per-row evaluator (61K uncorrelated memo hits are cheap; the AST walk
    around them is not). The next targeted lever is eager constant-folding
    of *uncorrelated* subqueries at scan-filter compile time (sound: same
    statement, same snapshot ⇒ one value), which would also make the filter
    worker-parallelizable. Est. −1.5–2.5% suite.
  - **Page-in is a per-pass tax**: every pass is a fresh process with a
    cold pool, so every touched page pays pread → stack buffer → `all_of`
    (now skipped) → `DecodeDisk` body copy → CRC. The body copy is the next
    chunk (pread directly into `Page::body` kills one 256 KiB memcpy per
    page); CRC verification stays (correctness contract).
  - **Every pass also replays ~1.2 GB of retained WAL at open** (recycle
    floor pinned by the oldest dirty-page recLSN) — excluded from per-query
    times but it inflates every pass and every user-time pairing by
    seconds. Recovery/txn lane.
- **Traps hit this session (all cost real time; recorded so the next
  session skips them):**
  - **The machine's clock jumped mid-session** (ninja: "manifest still
    dirty … system time is not set"; file mtimes in the future). An
    incremental rebuild across the jump silently linked a **stale
    benchmark object**, and a "clean" A/B then compared an instrumented
    binary against a non-instrumented one, reporting −36.9% — an artifact
    of min(cold,warm) extraction vs a single cold run. Rule: after any
    clock anomaly, `touch` the touched sources and verify the artifact
    (e.g. TPCH_PROFILE line count per pass) before believing an A/B.
  - **Do not hand-inject objects into `.a` archives** (ar r + relink) in a
    tree another session is editing: the SEGV that results
    (ExecutionRuntime field at offset 0x7d) costs an hour. Use a properly
    configured scratch build dir; copying `build/` across paths does NOT
    reconfigure (CMakeCache holds absolute paths).
  - Whole-process perf profiles of one-query runs are dominated by
    WAL-replay-at-open; phase slicing needs `perf report --time a,b`
    (relative seconds, comma) — and the driver must skip close
    (checkpoint flush) or the slice catches the checkpoint instead of the
    query.
- TPC-C was not touched this session (a concurrent session owns the
  page/txn lane; this session's page_pool.cpp hunk is confined to the
  read path and predates no conflict).

Reproduce (gold fixture + harness as in the 2026-10-02 entry):

```bash
BENCH_BASE=.../bin-base2/tinylamb_tpch_benchmark \
BENCH_AFTER=.../bin-after2/tinylamb_tpch_benchmark \
~/develop/tpch-perf-1005/harness.sh 4 tag
```

## 2026-10-02 20:45 JST — TPC-H SF=1 verification snapshot

- Current uncommitted working tree after the full unit/integration test run.
  AMD Ryzen 9 9950X3D, 32 logical CPUs; Clang, RelWithDebInfo, system
  allocator, LTO off. No CPU affinity or cache eviction; host 1-minute load
  was 3.57–4.29 during the three measured runs.
- Fresh database loaded from the existing SF=1 DBGEN fixture
  (`/tmp/tltpch.tpch-data`, pinned DBGEN commit
  `32f1c1b92d1664dba542e927d23d86ffa57aa253`): 6,001,215 lineitem rows.
  Initial load **26.917 s**, ANALYZE **6.919 s**; initial Q1–Q22 run excluded
  from the three-run summary.
- Each measured round is a fresh process using `--reuse-database`, with
  ANALYZE enabled. The **sum of Q1–Q22 elapsed times** was
  **13.493 / 13.846 / 13.816 s**; median **13.816 s**.
  These query times include planning and result draining, but exclude
  database open, ANALYZE and shutdown. This is a snapshot, not a paired
  performance comparison against the earlier optimization measurements.
- All 22 queries succeeded in all four runs; per-query row counts matched
  across runs (row-count consistency, not a separate result-value oracle).
- Raw plans, timings, host load, binary SHA-256, tracked working-tree diff,
  metadata and machine-readable results are saved locally under
  `build/benchmarks/tpch-20261002-2045/` (ignored build output).

Reproduce after loading `/tmp/tinylamb-tpch-20261002-2045`:

```bash
cmake --build build -j8 --target tinylamb_tpch_benchmark
./build/tinylamb_tpch_benchmark /tmp/tinylamb-tpch-20261002-2045 \
  --scale-factor 1 --data-dir /tmp/tltpch.tpch-data --reuse-database
```

| Query | Median ms (3 runs) | Rows |
| --- | ---: | ---: |
| Q1 | 293.028 | 4 |
| Q2 | 436.416 | 100 |
| Q3 | 1911.420 | 10 |
| Q4 | 286.934 | 5 |
| Q5 | 424.216 | 5 |
| Q6 | 162.705 | 1 |
| Q7 | 632.148 | 4 |
| Q8 | 403.152 | 5 |
| Q9 | 2317.390 | 175 |
| Q10 | 490.316 | 20 |
| Q11 | 265.983 | 741 |
| Q12 | 276.324 | 2 |
| Q13 | 682.931 | 42 |
| Q14 | 318.427 | 1 |
| Q15 | 201.044 | 1 |
| Q16 | 264.699 | 18,331 |
| Q17 | 239.530 | 1 |
| Q18 | 946.042 | 100 |
| Q19 | 293.181 | 1 |
| Q20 | 284.258 | 164 |
| Q21 | 1919.710 | 100 |
| Q22 | 698.606 | 7 |

## 2026-10-02 — TPC-DS SF=1 (94q): grouping-set branches stop re-executing CTE bodies (snapshot A/B −14% suite under co-tenant load; q5 −66%, q82 582→37 ms, q80 487→148 ms)

- **Root cause:** the visitor's GROUPING SETS / ROLLUP / CUBE expansion
  (`grouping_sets_to_union`) copies the whole statement — WITH clauses
  included — onto every UNION ALL branch. Relational-route execution re-ran
  each CTE body once per branch (`ExecuteQuery` re-executes a branch's own
  WITH entries over the inherited `ctes` map), and the head copy re-ran them
  once more. q5 evidence: `scan_rows=474,472` ≈ 4 body passes (3 CTE bodies
  ≈ 118k rows); ROLLUP arity A/B showed near-linear cost (1 branch 0.15 s →
  3 branches 0.50 s user).
- **Fix:** branches drop their WITH copies (`SelectStatement::ClearWithQueries`,
  both the plain and the window×grouping-sets expansion); the owning
  statement keeps the declarations and branch references resolve through the
  inherited CTE map. The UNION ALL fold clears the head copy too
  (`head.ClearWithQueries()`). Post-fix q5 `scan_rows=118,618` = one body
  pass; cold single-run 940→283 ms, warm min-of-2 133 ms.
- **A/B (same-minute snapshots, with/without the fix, co-tenant 1-min load
  20-35):** all 94 queries identical row counts; suite total 10,701→9,201 ms
  (−14%). Per-query wins: q5 562→191, q82 582→37, q80 487→148, q78 326→210,
  q47 306→222, q77 122→50 ms. The swing regressions (q16 +171, q67 +118,
  q23a +97 ms) are load noise; rows match.
- **Regression:**
  `SqlGroupingSetsTest.CteBodyExecutesOncePerGroupingSetExpansion` pins the
  results and a `scan_rows <= 8` bound (one pass over the fixture).

## 2026-10-02 — TPC-C W=2/32cl: ring-read undo (abort without pre-undo group-commit flush), detector backstop 25 ms, delete-undo space reservation, logger full-park wakeup fix

- **tpc (paired, alternating, taskset 0-15, tmpfs, 60 s):** two pairing
  sessions. Session A: base 3,833 / 3,370 / 2,949 vs new 3,328 / 2,905
  (a third "new" round fail-stopped mid-run — that fail-stop was the
  pre-existing delete-undo kNoSpace bug, fixed in this round). Session B
  (with the fix on both sides): base 3,139 / 3,850 / 2,304 vs new
  2,681 / 2,885 / 3,087. Alternating pair deltas flip sign (+34% to
  −25%); co-tenant load 10-20 and the 5 s write-intent escape hatch
  (24-48 gauge firings per round × 5 s across 32 clients) put the noise
  floor far above any effect, so tps shows no significant delta in either
  direction. The round's wins are mechanism-level and pinned by tests
  (the abort path provably performs no durability wait; abort survives
  the delete-undo collision that used to kill the engine).
- **Mechanism evidence:** `RuntimeAbortUndoDoesNotWaitForWalFlush` (60 s
  sync interval, DurableLSN provably unmoved across an abort),
  `PendingDeleteReservesSpaceForAbortUndo` (fails on the pre-fix binary,
  verified), `LoggerTest.TryCopyBuffered*`, dead-WAL abort contract in
  `AbortSurvivesUnreadableTailAndDeadWal`.
- **Fail-stop postmortem (found by the benchmark, fixed same round):**
  delivery hit the 5 s write-intent escape hatch → abort → the undo
  re-insert failed kNoSpace (a concurrent new_order insert had consumed
  the bytes the delete freed inside the intent window; the intent only
  locked the SLOT, not the BYTES) → `fail_stop_`. A restart would replay
  the identical undo against the same full page and fail the open. Fix:
  holes keep their row size and RowPage::Insert admission reserves
  intent-locked holes' bytes for the deleter's undo.

## 2026-10-02 (2nd session) — TPC-H SF=1: boxing-free bytecode interpreter + fused-probe shape specialization (min-of-12 wall −5.2%, user −4.1%; quiet-window reversed-order pairs −1.1/−6.2/−6.4% user; per-query min Q21 −7.1%, Q18 −5.1%, Q19 −4.6%, Q20 −4.2%; the ~20% target was NOT reached — fifth consecutive TPC-H session)

- **Status:** four changes landed (all default-on, semantics-guarded).
  Suite-level the result is −4 to −5% on robust estimators and a wash on
  naive ones; per-query it is a real −4..−7% on the Q18/Q19/Q20/Q21 block
  and noise (±3-8%) everywhere else. A controlled Q1 A/B (base vs a binary
  with the fold-path changes removed) showed parity within noise, and the
  queries that "regressed" in min-of-N are untouched code paths — the
  +3-8% cluster is co-tenant noise, not a real loss. All row counts
  identical on all 22 queries across 26 rounds.
- **Changes:**
  1. **Boxing-free interpreter** (`BytecodeProgram::TryEvaluateRowFast`,
     expression/bytecode.cpp): runs compiled programs straight on row
     cells (16 B stack slots, one final Value) for shapes the compiler
     flags `FastInterpretable` — columns/constants, int64+double
     arithmetic, comparisons, unary, EXTRACT date parts. Semantics mirror
     the canonical VM instruction-for-instruction, including the trap the
     differential suite caught on first run: **kDivide always divides in
     double** (the canonical kDivide-numeric shortcut precedes any integer
     kernel), so `INT64_MIN / -1` is a large finite double, not an
     overflow error. `TryEvaluateRowAuto` = fast with canonical fallback;
     differential_test now runs the fast interpreter against the canonical
     VM on every compiled cell of every suite.
  2. **Fused grouped-join probe specialization** (`TryFusedGroupedJoin`):
     per-program shape classification (single-`kLoadColumn` → direct cell
     read; fast-shaped → boxing-free interpreter; else canonical), plus a
     **scratch-mask pair fill** — the pair loop copies only the cells the
     residual/key/child programs actually load (group representatives are
     re-filled completely on the rare new-group path; unmatched LEFT rows
     keep the full copy).
  3. **Scan-filter + semi-join residuals** via `TryEvaluateRowAuto`
     (`MatchScanFilter` main/branch paths, subquery-runtime EXISTS
     residual).
  4. **Typed fold** filter programs and aggregate children via
     `TryEvaluateRowAuto`.
- **Measurement:** interleaved paired rounds on the gold fixture
  (12 pairs = 26 rounds; bursts discarded: p2-base, p8-mod). The
  forward-order pairs are order-biased under co-tenant load (whoever runs
  in the load valley wins), so the decisive numbers are the
  **reversed-order quiet-window pairs** (load 5-10): user time
  49.61 vs 50.18 / 50.95 vs 54.29 / 51.75 vs 55.30 = −1.1 / −6.2 / −6.4%.
  Suite min-of-12: wall 13,157 → 12,467 ms (−5.2%), user 50.18 → 48.13 s
  (−4.1%). Per-query min-of-12: Q21 1921.8 → 1784.7 ms, Q18 907.1 → 860.6,
  Q19 346.8 → 330.9, Q20 292.7 → 280.4.
- **Where it did not move:** Q9's fused probe — `join_ms` medians
  (base ~870 ms vs mod ~855 ms) are inside noise; the pair-loop work
  engaged but its win is smaller than the environment can resolve. The
  Q21 profile says the remaining fat is the **scan decode floor**
  (scan_ms 1310 of 2130; 49.5 M cells decoded through Value boxing +
  byte-swapped serdes) — a raw-cell fill path (decode into the arena
  mirror directly, Rows only for cache publish) is the next structural
  lever, and it is invasive (table/ + executor/).
- **Isolation:** all work ran in an rsync snapshot
  (`~/develop/tpchopt-1002`) while the concurrent TPC-C session edited
  `executor/relational.cpp` mid-session (their CTE re-run fix and two
  cosmetic hunks landed at 04:39); the merge back took the main-tree
  relational.cpp plus three one-line `TryEvaluateRowAuto` call-site edits,
  leaving their hunks intact. Tests green on the merged tree: differential
  15/15, executor 191/191 + 50/50 + 16/16 + 98/98 + 27/13/8/17/10/1,
  sql_engine 21/21 + 7/7.

## 2026-10-01 — TPC-H SF=1: inner-join build/probe flip + int64-arena cached base relations (clean-round paired −2.0%, sum-of-mins −5.9% over 3 rounds; per-query Q9 −15%, Q18 −17%, Q7 −10%, Q21 −7%; the ~20% target was NOT reached — fourth consecutive session)
## 2026-10-01 (2nd session) — TPC-H SF=1: typed-fold constant/IN-list peel, fused-slot FlatIntKeyMap (+probe prefetch), block-reserved slim group state, allocation-free int-key hit path (clean-window wall −4.5%; paired user-time −5.1% (mod6, 3 pairs) and −6.0% (mod8, 3 clean of 4 pairs); the 10% target was NOT reached — machine never went quiet after ~08:30)

- **Status:** five micro-optimizations landed, one reverted. Verified
  numbers: base 11,126/11,222 ms (quiet window, 2 rounds) vs W1+W2 binary
  10,593/10,728 ms = **−4.5% wall** in the same window; later in the day the
  co-tenant farm never dropped below 1-min load ≈ 15 (TPC-DS test suite +
  ironlamb-tpch bursts at load 20-46), so suite wall became unusable and the
  protocol switched to interleaved paired **user time** (`/usr/bin/time %U`):
  mod6 vs base 48.93 s vs 51.54 s = **−5.1%** (3 pairs), mod8 vs base
  −3.6/−9.7/−4.6% = **−6.0%** (3 clean pairs of 4, one burst pair
  discarded). Row counts identical on all 22 queries on both sides of every
  round (24+ rounds).
- **Isolation:** the concurrent TPC-DS session was editing
  `executor/relational.cpp` at session start, so all work ran in an rsync
  snapshot (`~/develop/tpchopt-1001b`) and merged back two files:
  `executor/relational.cpp`, `executor/detail/flat_int_key_map.hpp`
  (planning_heuristics.cpp net-unchanged after a revert). One mid-session
  mishap: the first fused-probe-pipelining edit landed on the main tree by
  mistake; it was reverted there before merging.
- **Changes:**
  1. **Typed-fold constant-compare peel** (`PlanTypedAggregation`):
     `col op const` conjuncts are constant-folded (ExpressionRewriter with
     the NOT-of-ordered-double suppression) and peeled into positional
     compares for int64/date/double and byte-exact VARCHAR =/≠; also
     **constant IN-list peel** (`col IN (c1..cn)`, ≤64 plain same-type
     items) into `TypedInListCompare`. Removes per-row bytecode dispatch and
     re-enables the arena fast path (which requires `filter_programs.empty()`)
     for folds whose WHERE was all constant compares (Q1 −20%, Q14 −12%).
  2. **Fused-slot FlatIntKeyMap**: key/value/occupancy merged into one 24 B
     slot (one cache line per probe instead of three across parallel arrays)
     + `Prefetch()` + **×4 growth** (rehash total 1.33× final size instead of
     2×). callgrind: TryEmplace was 6.1% of the Q21 fold.
  3. **Block-reserved slim group state**: `ReserveSlimBlock()` grows the four
     SoA arrays 128 groups at a time instead of one tail-insert per group
     (callgrind: `vector::_M_fill_insert` family ≈ 7% of the fold). Capacity
     derives from the array sizes so the merge path's std::move adoption
     carries padding for free.
  4. **Int-key hit path with no Value construction** (`accumulate_row`):
     probes `RawIntSlot` with the raw cell and builds canonical keys only
     when a new group opens; group-slot prefetch hoisted above the filter
     programs; `AppendFillRow` arena buffers grow geometrically with a
     publish-time exact-length cut (keeps rows·stride == cells.size).
  5. **Generic grouped loop single-int-key map** swapped from
     `unordered_map<int64_t,size_t>` to `FlatIntKeyMap` (node chase per probe
     → one line).
  - **Reverted:** software-pipelined bucket prefetch in the fused grouped
    join probe — Q9 measured −6% *worse* paired; the probe's bucket walk was
    already hidden behind per-pair residual/accumulate bytecode, so the
    pipelining was pure overhead there (ParallelHashProbe keeps its own,
    which predates this session).
- **Bug caught by tests before merge:** the first VARCHAR equality peel was
  shadowed by the constant-int branch of `DecideTypedFilterCompare` (read
  `int_value` out of a string cell), which made `name != 'hello'` keep every
  row — `ExecutorTest.RelationalNumericAndVarcharBinaryOperators` caught it;
  branch order fixed, 191/191 executor tests green on the merged tree.
- **Measurement notes:** exec of the 269 MB RelWithDebInfo binaries failed
  intermittently (rc=126, `env: execvp failed`) under the co-tenant suite's
  memory pressure — round.sh now strips debug info (54 MB) and retries;
  user-time pairing held a stable signal (±2%) while wall swung ±40% under
  bursts.
- **Where the remaining time is** (callgrind on the post-change binary,
  Q21): the fused probe's per-pair bytecode (`process_pair`/`accumulate_into`
  — residual programs, group-key programs, child programs are still
  bytecode on the concatenated schema), the Row-copy side of the cache fill
  (`AppendFillRow` ~4%), and the structural columnar batch decode in
  FullScanIterator (per-cell Value boxing on non-arena scans).


## 2026-10-04 06:30 JST — TPC-H SF=1: cross-query scan cache investigated and reverted (3 architectures, all net-negative); uncorrelated-subquery probe reorder landed (mechanism-level, suite effect within noise); ~20% target NOT reached

- **Summary:** this session attacked the biggest structural line item the
  2026-10-03 entry left open — every statement re-decodes its base tables
  (`ExecutionRuntime` caches are per-query, so lineitem's 6M rows are
  re-decoded by all ~15 queries that touch them). Three cross-query cache
  architectures were built, measured, and reverted: none beats a 16-worker
  parallel decode. One probe-path fix landed behind an env gate
  (`TINYLAMB_LEGACY_SUBQUERY_PROBE=1` restores the old order). The ~20%
  target was not reached; this entry records the negative results and the
  sharpened map of where the suite's remaining wall time sits.
- **Environment hazard first:** the main working tree's uncommitted evening
  WIP (2026-10-03 20:07–23:23: `sql_engine.cpp`, `relational.cpp`,
  `subquery_runtime.*`, `parallel_scan.*`, `relational_factory.cpp`,
  `full_scan_plan.hpp`, `expression_eval.cpp`) returns **Q18 rows=0 and Q21
  rows=0 at SF=1 (correct: 100 each) and then segfaults before Q22**, while
  the tiny-fixture `ExecutesAllTwentyTwoQueries` still passes. A mid-suite
  segfault also corrupts the WAL tail (damage class 3 fail-closed on next
  open). All work here ran on an rsync snapshot of the verified 10-03 06:37
  state (`~/develop/tpchopt-1004`); do not benchmark the main tree until
  that WIP is fixed.
- **Diagnosis that drove the session:** per-query profiles
  (`TPCH_PROFILE ... planning_ms/scan_ms/join_ms`) show `planning_ms`
  double-counts the join-loop's embedded `InnerJoin` executions; Q9's 2.3 s
  is ≈1.0 s parallel scan (41M cells boxed through serdes) + ≈0.9 s join
  chain (four materializing hash joins feeding the fused grouped join) —
  not planner exploration. Suite-level: scan+filter ≈4.4 s of 11.8 s wall.
- **Architecture 1 — rows cache keyed (table, projection):** publishes are
  correctly gated off by runtime join filters (the stored result would be
  SIP-pruned), so almost nothing publishes (2 events/suite). Entries are
  fat (456 MB for orders 1.5M×3) and the publish refill + hit narrowing are
  serial full-row copies. Suite +8% user. Reverted.
- **Architecture 2 — arena mirror inside `TryParallelTableScan`:** dead on
  arrival: keyed scans (runtime filters handed to `BeginMorselScan`) reject
  rows inside the iterator, so a mirror taken in the worker loop captures
  only the SIP-pruned subset — not a table image. Reverted.
- **Architecture 3 — per-column stride-1 arenas published by the typed fold
  fill** (unfiltered by construction — the mirror precedes the fold's
  peeled compares; Find requires full column coverage with equal row
  counts; `AssembleColumnsArena` re-interleaves for streaming folds; null
  bitmaps must be pre-sized per column — two crash bugs found by the suite
  on the way). Works mechanically (all 22 row counts correct, 17 column
  publishes/round), but the ordered per-morsel publish drain + split costs
  the publisher +400–650 ms (Q1 331→968 ms, Q6 143→594 ms) while a hit
  saves only ~100–250 ms — and Q9/Q21's projected column sets are never
  published by any fold, so the heaviest queries can never hit. Suite
  13.4 s vs 11.78 s baseline. Reverted; lesson recorded in memory.
- **Landed — uncorrelated-subquery probe reorder** (`subquery_runtime.cpp`):
  the per-row probe checked `uncorrelated_results_by_fingerprint` first,
  hashing the long structural key every row (Q22: ~61K probes, filter_ms
  639 ms of its 656 ms wall). The address-keyed `uncorrelated_results` map
  is now consulted first under the existing `CacheEntryIsCurrent` guard,
  and `cache_fingerprints` stores a pointer into `fingerprint_memo`'s
  stable node storage so the guard short-circuits instead of memcmp-ing two
  long equal strings per probe. `TINYLAMB_LEGACY_SUBQUERY_PROBE=1` restores
  the old order for A/B.
- **Measurement:** same-binary interleaved pairs, gold 2026-10-01 SF=1
  fixture, `TINYLAMB_SKIP_ANALYZE=1 TINYLAMB_CHECKPOINT_SECONDS=0
  TINYLAMB_GROUPED_ROUTE=relational`, taskset 16-31, fresh process per
  round via `--reuse-database`. Only two pairs landed in quiet windows
  (load 2–4): user 40.81/39.90 vs 41.61/41.53 s (−1.8/−3.9%), suite wall
  11758/10828 vs 11748/11125 ms (−0.0/−2.7%); the day's remaining pairs ran
  under co-tenant load 10–25 and are discarded. Per-query min-of-2 for the
  affected queries is inside noise (Q22 633.5 legacy vs 634.9 new), so the
  honest claim is mechanism-level: strictly less work per probe, pinned by
  construction and by the counters, with a suite effect below the machine's
  ±3% noise floor. All 22 row counts identical in all 10 rounds, both gate
  settings. Tests on the final tree: sql_engine_tpch 6/6, differential
  15/15, executor 191/191, query 174/174.
- **Where the remaining time is (updated):** Q9 2.3 s (scan decode+boxing
  ~1.0 s; a 4-hop materializing join chain ~0.9 s ahead of the fused grouped
  join — a star-join fusion that streams the fact table once against all
  dimension builds, or at minimum a build/probe flip so the 480K-row
  intermediates never materialize, is the next structural lever); Q21 2.0 s
  (lineitem scanned twice with disjoint projections, so the per-query
  self-join cache misses both times — within-query column dedup would
  halve its scan); Q18 1.0 s, Q13/Q22 0.66 s each. Cross-statement scan
  caching as tried here cannot pay for itself; any future attempt needs a
  publisher whose amortization is guaranteed within the statement.

## 2026-10-10 — completeness pass baselines (JSON family + column DEFAULT + persistent views landed)

- **TPC-H SF=1** (gold copy at ~/base1010/tpch.db, taskset 16-31,
  TINYLAMB_SKIP_ANALYZE=1, single run, quiet-ish window): suite ≈10.9 s.
  Q9 2208 / Q21 2074 / Q18 1145 / Q7 724 / Q13 598 / Q10 469 / Q5 388 ms.
  All 22 row counts unchanged from the 10-09 records. Q21's profile confirms
  the round-12 sibling aggregate fusion stuck: scan_rows 7.5M (~1.25 lineitem
  scans), aggregate_input_rows 6.0M — the round-11 "shared EXISTS canonical
  layout" lever is obsolete.
- **TPC-DS SF=1** (94 queries, --runs 2, failed=0): TOTAL 3435.6 ms.
- Remaining structural levers unchanged: Q9's 4-hop materializing join chain
  (star-join fusion), Q18's 6M-row GROUP BY, scan-projection default flip
  behind the Q19 plan-feedback interaction.
