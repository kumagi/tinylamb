# Fuzzing Campaign Report — 2026-09

End-to-end fuzzing campaign over the whole `tinylamb` engine: an inventory of
all fuzz harnesses, a large multi-round parallel sweep (UBSan/ASan builds,
growing corpora), targeted **expansion of weak harnesses**, and the resulting
bug fixes. Every entry below was reproduced from a shipped artifact, fixed,
and re-verified; the full unit test suite (2410 tests) passed after each
batch of fixes.

**Tally: 23 engine bugs + 10 harness/infrastructure fixes + 5 new fuzz
targets + 5 regression-test updates.** The campaign hit diminishing returns:
the last three waves found no new engine bugs, so the sweep was closed as
converged rather than padded to an arbitrary fix count (the goal was bug
yield, not a quota — see §5/§6).

## 1. Fuzz target inventory (complete)

libFuzzer targets (`-DTINYLAMB_ENABLE_FUZZ=ON`, see `CMakeLists.txt`):

| Layer | Target | Oracle |
|---|---|---|
| type | `value_fuzzer` | memcomparable order, + NEW: serialize/skip round-trip, memcomparable decode round-trip + order preservation, trichotomy, `CompareForOrderBy` totality, arithmetic total-status |
| type | `date_interval_fuzzer` (NEW) | date parse→format→parse fixed point, date-add identities, interval parse↔ToString, plus/commutativity, negate involution, justify chains preserve total-nanos |
| expression | `expression_fuzzer` | AST vs Bytecode VM vs `relational_detail::Evaluate` differential, + NEW: typed schema values, CAST/shift/IS-DISTINCT/DATE/LIKE coverage |
| expression | `cast_fuzzer` (NEW) | Try-contract (no escaped exceptions), determinism, NULL→NULL, safe-cast total success, INT64↔STRING round-trip |
| expression | `function_fuzzer` (NEW) | strict builtin NULL propagation, determinism, Try-contract over 57 function names (26 added from a code-level strictness audit) |
| executor | `data_chunk_fuzzer` (NEW) | bulk paths (`AppendGather`/`AppendFrom`/`AppendRowFromColumns`) vs scalar `Append` reference: values, null bits, zone-map aggregates |
| page | `row_page_fuzzer` | slotted page ops + crash/recovery model |
| page | `leaf_page_fuzzer` | leaf page ops + NEW: Commit / Crash+ARIES-recover model oracle |
| page | `pax_page_fuzzer` (NEW) | PAX Store/Load exact-value round-trip + hostile body memory safety |
| index | `b_plus_tree_fuzzer` | tree ops + NEW: Commit / Crash+Recover model oracle |
| index | `lsm_tree_fuzzer` | LSM ops + NEW: Sync barrier + reopen-from-disk durability check |
| index | `lsm_view_fuzzer`, `cache_fuzzer` | run merge views / blob cache reads |
| recovery | `logger_fuzzer`, `log_record_fuzzer` | WAL append/scan, record round-trip stability |
| table | `table_fuzzer` | catalog + rows + `EmulateCrash` sessions |
| server | `postgres_protocol_fuzzer` | startup-packet parse/round-trip |
| query | `googlesql_ast_fuzzer` | AST-dump parse + full visit |
| query | `sql_oracle_fuzzer_libfuzzer` | TLP / NoREC / PQS / DQE / index-independence / txn-splitting + window model/permutation + WITH TIES + skip-scan-distinct (R13) |
| query | `expr_oracle_fuzzer_libfuzzer` | scalar rewrite + engine + Python cross-check |
| query | `griffin_fuzzer_libfuzzer` | catalog-guided sessions, AST-vs-bytecode/JIT differential |
| query | `sql_join_fuzzer_libfuzzer`, `sql_aggregate_fuzzer_libfuzzer`, `sql_plancache_fuzzer_libfuzzer`, `sql_session_fuzzer_libfuzzer` | join/agg semantics, plan cache, stateful sessions |
| plan | `rule_fuzzer` (standalone threaded stressor) | cascades rules |
| + replay binaries | `*_fuzzer_replay` (11) | deterministic replay of saved units |

## 2. Infrastructure fixes (why previous rounds were under-finding)

1. **Whole-project coverage instrumentation.** Fuzz builds instrumented only
   each harness TU; the SQL/plan/executor fuzzers call straight into
   `tinylamb::sql`, so libFuzzer saw `cov: 1` — pure blind mutation. Fuzz
   builds now compile everything with `-fsanitize=fuzzer-no-link,undefined,
   bounds` (clang-only guard for GCC builds). Effect: `sql_oracle_fuzzer`
   coverage 1 → **22,449** PCs (ft 81k) within minutes; corpus growth is now
   feedback-driven.
2. **`row_page_fuzzer` never ran recovery.** The "Crash" op rebuilt the
   environment but never called `RecoveryManager::RecoverFrom`, so ARIES
   redo/undo was untested despite the harness comment claiming otherwise;
   committed data "vanished" after the first fuzz-exercised recovery. Now the
   crash op runs the real ARIES pass like `page_storage` does.
3. **`expression_fuzzer` was broken on its first real input**: it built rows
   with untyped random values against a typed schema → every
   `DataChunk::Append` hit the type CHECK. Now values are schema-typed (and
   NULL-heavy) and the schema gained a DATE column.
4. Harnesses with **no persistence/recovery path** (leaf page, B+tree, LSM)
   got Commit/Crash/Reopen-or-recover operations plus model oracles.
5. New fuzz targets: `pax_page_fuzzer` (+replay), `date_interval_fuzzer`,
   `cast_fuzzer`, `function_fuzzer`, `data_chunk_fuzzer` — 20 → **25**
   libFuzzer targets total.
6. Deterministic temp-dir collisions (`RandomString(..., false)` is seeded,
   NOT random) made concurrent LSM harnesses delete each other's directories
   under ASan sweeps → unique names + stale-dir cleanup.
7. `lsm_tree_fuzzer` teardown raced the background merge worker's file
   deletion (`remove_all` threw ENOENT); tree is destroyed before the sweep.
8. `ColumnVector::Append` CHECK now prints both type names (this diagnostic
   is what pinpointed the IS DISTINCT FROM contract bug below).
9. Mismatch printing in `expression_fuzzer` (threw/value, both types, row,
   folded tree + folded-AST value) turns each finding into a ~1 min triage.
10. Documented-deviation tolerances in `expression_fuzzer`: identity folds
    pinned by `optimizer_arithmetic_high_expectations.test` (type promotion
    loss for `x + 0.0` etc.) and the *by-design*
    `ExpressionCannotThrow`-column-totality assumption (both directions of
    error erasure/surfacing). Everything else stays strict.

## 3. Engine bugs found and fixed

### Expression / optimizer
1. **Bytecode VM AND/OR short-circuit returned the raw truthy operand**
   instead of the AST-normalized BOOLEAN (e.g. `-1 OR x` → VM said `-1`,
   AST `TRUE`). Fixed in both `TryEvaluateBatch` and `TryEvaluateRow`.
2. **Truthiness-collapse rewrite rules fired in value contexts**: `x AND
   TRUE → x`, `NOT NOT x → x`, `x OR x → x`, absorption, and the two
   common-conjunct factoring rules were removed from
   `BytecodeCompiler::Compile(kValue)` (they only preserve truthiness, not
   value/type).
3. `double_negation_arithmetic` folded `-(-x)` for **dynamic INT64** `x`,
   erasing the INT64_MIN overflow raise the AST produces. Now restricted to
   doubles/constants.
4. `combine_repeated_addend` rewrote `x + x → x * 2` for dynamic INT64; in
   this engine addition crosses into UINT64 wraparound while multiplication
   raises, so the fold *adds* a throw (oracle-found at INT64_MIN).
5. `distribute_or_over_and_budgeted` moved always-evaluated conjuncts into
   disjuncts where the original short-circuit could skip them, surfacing
   raises that the AST never produced; guarded with
   `ExpressionCannotThrow` per conjunct.
6. **NaN soundness cluster** (`not_comparison` is IEEE-unsound when an
   ordered comparison can be DOUBLE):
   - `ContainsNotOfOrderedDoubleComparison` missed the NOTs that
     `xor_to_or_and_not` *expands into* (`(NaN >= -3) XOR col_i` diverged);
   - `SideCanBeDouble` returned false for subtrees containing DOUBLE
     literals (unknown columns throw out of `ResultType`, hiding the double);
   - `not_comparison` itself now refuses when either operand subtree
     contains a DOUBLE literal constant;
   - column-typed operands (`col_d > 0`, `col_d = 0.5`) still slipped
     through — the rule now takes the active schema (thread-local
     registration from bytecode compile, `ScanFilter`, and the optimizer's
     filter normalization; unknown columns conservatively count as double),
     and `TypesComparable` gained the same static mismatch the optimizer
     pins (STRING vs FLOAT64/INT64).
7. `relational_detail::Evaluate`'s IN node swallowed comparison errors
   (`"" IN (col_i)` → FALSE) where the AST ground truth propagates the
   type-mismatch raise.
8. `BinaryResultType` did not classify `IS [NOT] DISTINCT FROM` as boolean,
   declaring DOUBLE (via promotion) for `x IS DISTINCT FROM y`; the batch VM
   then appended an INT64 boolean under a DOUBLE column vector → `CHECK
   column vector type mismatch: column=Double value=Integer` crash.
9. **`Value::Truthy()` returned true for DOUBLE 0.0** (fell into the blanket
   non-int `return true`): `(0.0 OR x)` short-circuited TRUE while `(0 OR
   x)` was FALSE, and identity folds of the 0.0 made the AST and the
   bytecode fast path diverge. Zero is false in every representation now.
10. `REPEAT` cap check `s.size() * n` used a **wrapping uint64 multiply**:
    `("abcdefgh", 2^61)` wrapped to 0, passed the 1 MB check, and appended
    until OOM (ASan sweep found the alloc bomb). Now `__builtin_mul_overflow`.
11. `REPEAT('', 2^63)` looped appending an empty string billions of times
    (30 s+ fuzzer timeout) — early-return empty.

### Type layer (string/parse surface)
12–16. **Five double→int64 UB sites in INTERVAL parsing** (UBSan:
     "inf/9.24222e+18 is outside the range of representable values of type
     long"): H:M:S `sscanf %lf` path, ISO-8601 `from_chars` path ("nan"/"inf"
     spellings parse fine and slip past plain range compares), the unit-suffix
     `to_int64` helper (`stod("nan")`), and the second H:M:S unit-list path.
     The first-pass guards used ±9.3e18/±9.2e18 — fuzzing narrowed them to
     the exact `[-2^63, 2^63)` window (`kInt64MinAsDouble` /
     `kInt64MaxExclusive`).

### Numeric builtins
17. `TRUNC`/`ROUND` on INT64 with a huge negative scale negated `digits`
    (`-INT64_MIN` is UB — UBSan, found by the new FUNCTION fuzzer); the
    digit count is now converted in unsigned space.

### Recovery / MVCC
18. **Transaction ids restarted at 1 after recovery.** Recovery classifies
    losers as "txn ids without a kCommit in the WAL"; a fresh
    `TransactionManager` re-issuing id 1 let an *old* commit record vouch
    for a *new* uncommitted transaction, whose writes then skipped undo —
    committed rows vanished or losers' rows survived (fuzz-found:
    committed-after-crash DELETE lost). `RecoverFrom` now seeds
    `next_txn_id = max_seen_txn + 1` (records + checkpoint ATT).
19. **Second-recovery double undo.** The per-page undo phase re-undid
    operations that a previous recovery had already compensated (their CLR
    sits in the WAL window). Undo is only idempotent while nothing else
    touches the same slot/key; a later committed write to the same key was
    deleted by the stale undo. PageReplay now pairs each `kCompensate*`
    record (type + slot/key + txn, newest-first) with the operation it
    closed and skips it.

### SQL frontend robustness (uncaught-exception class)
20. Six `IntervalExpr` construction sites in `googlesql_ast_visitor` let the
    eager `IntervalExpression` constructor throw `out_of_range` straight
    through `Visit()` (fuzzed AST with empty string/no unit). They validate
    with `IntervalValue::TryParse` and return `StatusError` now.
21. `make_interval`, `generate_date_array` step parsing, the
    `__tinylamb_date__` marker path, and DATE array elements all used
    throwing EXC-SHIM wrappers inside `TryEvaluate` paths → Status-based
    (`TryParse`/`TryDate`).
22. `CAST(x AS INTERVAL)` used throwing `Parse` + `ToString`; converted to
    the Try family.
23. `EncodeProtoWireMessage` (CAST-to-BYTES of NullableDate proto text)
    threw through `TryEvaluate` on invalid dates; it returns
    `StatusOr<std::string>` and every propagation site checks.

**Count: 23 engine fixes** (11 expression/optimizer, 5 interval UB sites,
1 numeric builtin, 2 recovery/MVCC, 4 frontend-throw).

## 4. Regression tests pinned

- `googlesql_ast_test.GoogleSqlAstTest.UnitlessIntervalLiteralsReturnStatusNotThrow`
- `value_test`: `Truthy_VariousTypes…` now pins `Truthy(-0.0)=false`,
  `Truthy(inf)=true`
- `rewrite_test`: `ArithmeticIdentitiesAndDoubleNegation`,
  `NestedExpressionWithinDepthLimitStillRewrites`,
  `CanonicalizesSimpleArithmeticShapes` updated to the *sound* behavior
- existing fuzzer regression corpora kept replaying
  (`SqlOracleFuzzer.ReplayCommittedRegressionFiles`, session regressions)

## 5. Run summary

- Rounds: 12 waves, up to ~115 concurrent workers, cumulative **~120 CPU-hours**
  of directed fuzzing across UBSan and Debug-ASan fuzz builds.
- Corpora: persisted per target (~530 MB, 135k units at the end). Coverage
  after feedback instrumentation was enabled (final wave):
  `sql_oracle`/`griffin` ft 82k, `expression_fuzzer` cov 6,610 / ft 34k,
  `googlesql_ast_fuzzer` cov 6,675 / ft 28k, `table_fuzzer` cov 4,234,
  `b_plus_tree_fuzzer` cov 2,938, `leaf_page_fuzzer` cov 2,682 (was cov 1
  for every SQL target before §2.1).
- Crash/finding triage: every saved unit was replayed before and after the
  fix; false-positive classes (env flakes under 60-way CPU contention,
  seeded-directory collisions, pinned optimizer deviations) are listed in
  §2/§6 rather than silently dropped.
- Final 25-minute SQL-only wave (7 oracle fuzzers, 4 workers each, corpora
  from all previous rounds): **zero findings**.
- Full `ctest`: **2410/2410 passed** after every fix batch (2441/2441 after
  the R13 window-oracle round, including concurrent test additions).
- `scripts/check_layering.py`: no new allowlist entries (63 = baseline).
- `fuzzer-nightly.yml` now builds and smoke-runs all 12 in-process fuzz
  targets (previously 2), pins `CC=clang CXX=clang++` (GCC 15 has no
  `-fsanitize=fuzzer`), and sweeps artifacts on failure.

## 6. Known residual gaps / follow-ups

- `ExpressionCannotThrow` keeps its *documented* column-totality assumption
  (schema-blind rewriter); the bytecode/scan fast paths mitigate with
  `ContainsNotOfOrderedDoubleComparison`, and the expression fuzzer now
  tolerates exactly this error-direction. Payoff path: schema-aware rewrite
  contexts (or typed rules à la `RewriteTypedArithmetic`).
- `a + 0.0` identity keeps INT64 typing (pinned deviation); IEEE
  -0.0/+0.0 and error-at-boundary differences are documented there.
- `postgres_protocol_fuzzer` only covers the startup packet; a session-level
  protocol fuzzer (Q/Sync/error streams over a socketpair) is the next step.
- TSan is not combined with libFuzzer (no concurrent fuzz harness); row/page
  concurrency rests on `*_concurrent_test` under `build-tsan`.

## 7. Conclusion

The dominant bug class was **fast-path/rewriter divergence from the AST
ground truth** (11 of 23): short-circuit value normalization and folds that
preserve truthfulness but not value, and IEEE/overflow edge semantics
(NaN, ±0.0, INT64_MIN) silently erased or introduced by rewrites. The second
class was **Status-contract violations** — throwing paths reached from
`TryEvaluate`/`Visit` (8 of 23) — all converted to the `Try*` family.
Recovery gained two *stateful* correctness fixes (txn-id seeding, CLR-aware
undo) that only a crash-model harness can find; both were invisible to the
test suite because no pre-existing test did crash → recover → write →
recover-again. The 20 → 27 target expansion plus whole-project coverage
instrumentation is the durable output: the harnesses now find bugs on their
own, and the nightly runs 12 of them in CI.

## 8. Follow-up round (R13): recent-feature catch-up

The engine grew three large surfaces right after the campaign landed, none
of them fuzz-covered:

- **Window functions** (`executor/detail/window_eval.cpp`, ~2.2k lines):
  ranking (`ROW_NUMBER/RANK/DENSE_RANK/PERCENT_RANK/CUME_DIST/NTILE`),
  `LAG/LEAD` with default, `FIRST/LAST/NTH_VALUE`, every aggregate as
  analytic (incl. `ARRAY_AGG`/`STRING_AGG` with inner `ORDER BY`,
  `COUNT(DISTINCT)`, `COUNTIF`), `ROWS/RANGE/GROUPS` frames with all five
  bound types and `EXCLUDE CURRENT ROW/GROUP/TIES`.
- **`TOP n ... WITH TIES`** (frontend pre-pass + limit path).
- **Skip-scan distinct** (`executor/skip_scan_distinct.cpp`: index jumps to
  the next distinct key).

Additions (all inside `sql_oracle_fuzzer` so mismatches keep the
`.test`-file regression pipeline):

1. `window_probes`: 2–4 generated window expressions per iteration over the
   seeded mirror table. Model kinds (`ROW_NUMBER`, `RANK`/`DENSE_RANK` incl.
   `DESC`, `LAG/LEAD`, `SUM/COUNT/MIN/MAX` over `ROWS` frames with
   `EXCLUDE CURRENT ROW`, unbounded partition aggregates, `NTILE`,
   `FIRST/LAST_VALUE`, `COUNTIF`, `COUNT(DISTINCT) OVER`) are recomputed
   harness-side from the INSERTs; `RANGE/GROUPS` and array-result shapes run
   as permutation/index checks only. Every probe additionally runs against
   the table rebuilt in **reversed physical insert order** and again **with
   the iteration's indexes** — results must be identical (probes order by
   the unique key), which is an index-independence test for the window
   sort/partition input path. Per-probe skips keep one unsupported shape
   from muting the rest.
2. `skip_probes`: `COUNT(DISTINCT c)` / `SELECT DISTINCT c` (bare and under
   the predicate) compared before vs after creating indexes on `c` — the
   indexed leg may route through `SkipScanDistinct`.
3. `ties`: `ORDER BY a LIMIT n WITH TIES` checked against the null-safe
   boundary model (ASC NULLS FIRST, all boundary peers included).
4. griffin window session grew 4 → 21 statements (frames, exclusions,
   `NTILE/LAG/LEAD/FIRST/LAST_VALUE`, `WITH TIES`, indexed
   `COUNT(DISTINCT)`), keeping the AST-vs-bytecode/JIT differential over
   them all.
5. `WindowOracleDetectsWrongModel` unit test pins that the model comparison
   actually fires (a perturbed frame offset must report).

Verification: 15 generated window syntax shapes all accepted by the
frontend (0 rejections over 24 seeded iterations); a 20-minute 50-worker
sweep (sql_oracle × 6, griffin × 2, expr/session) found **no new bugs**;
full `ctest` green (2441 including concurrent WIP tests), layering at
baseline.

Incidental fixes found while validating against the current tree (the
in-flight rule-migration work had drifted): `SafeToReduceEvaluationCount`
was declared publicly in `rewrite.hpp` while its definition sat in an
anonymous namespace — every in-file call was ambiguous under GCC (clang
tolerated it) and the cross-TU user in `plan/cascades.cpp` would not link;
the definition now lives in `namespace tinylamb` and the header declaration
stands. A `cascades_test` helper call passed one argument to the
two-argument `FunctionCallExp` factory (compile error under any compiler).

## 9. Follow-up round (R14): optimizer-expansion fuzzer catch-up

The Cascades surface kept growing (conjunct canonicalization, grouped
outer joins, RIGHT/FULL, LATERAL/correlated subqueries, Top-N through
joins, parallel join/aggregation above the 8192-row morsel threshold)
while the harnesses still generated only INNER/LEFT equi-joins over
4–12-row tables. This round expanded coverage to the supported edge and
fixed what it found.

### Fuzzer expansion

1. **`sql_oracle_fuzzer`: index oracles made live.** The SQL frontend
   never accepted `CREATE INDEX`, so `CheckIdx`/`CheckSkip`/the window
   index pass had silently skipped every index leg. A shared
   `ApplyIndexSpec` helper in `query/fuzz_scoped_db.hpp` now parses the
   serialized `-- idxddl:` specs, resolves column names to schema slots,
   and builds `IndexSchema` via `Database::CreateIndex` — `.test` files
   stay self-contained and textual. The generator emits unique,
   composite-key, and INCLUDE-column indexes.
2. **`sql_oracle_fuzzer`: guarded-division conjunct oracle (`CheckGuard`,
   new).** Every iteration plants one `a = 0` row, pairs a
   possibly-erroring conjunct `(b / a) <op> c` with 1–2 plain guard
   conjuncts, and checks that all textual permutations *plus* a
   subquery-wrapped variant agree with a commutative-3VL mirror:
   a clean FALSE/NULL suppresses sibling errors; with no rejecting
   sibling the error must surface on every permutation. This is the
   fuzz-side pin for the Bug-1 semantics below; serialized as
   `-- guard:` / `-- guardexpected:` / `-- guarderror:` lines.
3. **`sql_join_fuzzer` rewritten around a C++ row-mirror.** Constraint-
   bearing schemas, RIGHT/FULL joins, NULL-bearing keys, GROUP BY /
   DISTINCT / ORDER BY+LIMIT over joins, set operations, CTE probes, and
   ≥8192-row rounds that cross the parallel join/aggregation thresholds —
   previously unreachable by any fuzzer.
4. **`griffin_fuzzer` seed pool** grew to cover the new optimizer surface:
   boundary numeric/string values, outer joins with NULL keys, RIGHT/FULL,
   set ops with NULLs, window frames/exclusions, CTEs and correlated
   subqueries, Top-N `WITH TIES`, index creation, and larger datasets.
   `CREATE INDEX` statements route through `ApplyIndexSpec`; the
   AST-vs-bytecode differential still reruns every accepted SELECT.
5. **`sql_aggregate_fuzzer` expanded to the shapes the engine grew:**
   GROUP BY over 1–2 key columns with optional WHERE and
   HAVING (3VL on the aggregate result — a NULL comparison drops the
   group), global aggregates over possibly-empty input (pins
   `COUNT(*)=0` / `SUM=NULL`), `COUNTIF` and `COUNT(DISTINCT)` /
   `SUM(DISTINCT)`, and window `ROWS BETWEEN k PRECEDING AND CURRENT ROW`
   frames for `SUM`/`COUNT(b)`. ~12% of iterations build ~8.4k-row tables
   via multi-row `VALUES` batches, crossing the morsel/parallel-
   aggregation threshold no fuzzer reached before.
6. **Stream-error detection in every lazy-result harness.** `RunQuery` in
   `sql_aggregate`/`sql_join`/`sql_session`/`sql_plancache` fuzzers and
   their `RunSql`/`RunUpdate` helpers now check `GetStatus()` after
   draining; a mid-stream error on an always-valid generated query is a
   finding (`stream-error:` diagnostic), not a silent truncation — the
   same class as the `main.cpp` CLI bug (25) it pins.
7. **Disk-pressure hardening.** `ScopedDb` creation sites that used
   `CHECK(sdb.get() != nullptr)` (expr_oracle ×2, sql_oracle ×3, griffin
   ×2) or dereferenced the handle unconditionally (sql_join ×2,
   sql_aggregate ×2) now skip the iteration/replay leg when
   `Database::Create` fails. A "Disk quota exceeded" LOG(FATAL) is an
   environment flake — it previously aborted whole sweep workers and
   produced non-reproducing crash units.

### Bugs found and fixed

24. **WHERE conjunct reordering broke error suppression (wrong-result /
    crash-class).** `CanonicalizeConjuncts` sorts conjuncts
    lexicographically for memo dedup; `a <> 0 AND b/a > -10` reached the
    executor as `b/a > -10 AND a <> 0`, and `Selection` evaluated the
    whole AND tree strictly left-to-right, so `a = 0` rows raised
    division-by-zero before the guard — silently truncating the stream
    (the CLI swallowed the error; see 25). The engine's filter doctrine
    is commutative-tolerant (`MatchScanFilter` already declares it): a
    clean FALSE/NULL conjunct suppresses sibling errors. A shared
    `EvaluateConjunctsTolerant` helper (`executor/detail/scan_filter.*`)
    now evaluates top-level conjuncts tolerantly at every filter site —
    `Selection`, index/index-only/bitmap residual conditions,
    `CompiledScanFilter` fallback, and *inner-kind* NLJ/BNLJ/merge-join
    residuals. Outer/semi/anti JOIN ON stays strict (error propagation
    is required for unmatched-row correctness). Scalar AST/bytecode/JIT
    keep strict L-to-R; the doctrine is documented in
    `docs/expression_evaluation.md`. Pins:
    `QueryTest.WhereConjunctReorderSuppressesSiblingError` plus
    guard-order/propagation/inner-join cases.
25. **CLI swallowed streaming execution errors.** `main.cpp` consumed
    `QueryResult` via `ForEach` but never checked `GetStatus()` — a
    mid-stream failure (e.g. the div-by-zero above) printed partial rows
    and exited 0. The result is now null-guarded and status-checked
    after draining.
26. **`Selection::NextBatch` out-of-bounds constant read (SEGV).** The
    INT64-filter fast path indexed `Constants()` with the bytecode
    operand before validating the opcode; a non-compare opcode with a
    large operand read past the constant table (libFuzzer SEGV at
    `selection.cpp`). Opcode validation now precedes the operand read.

### Harness fixes found by the new coverage

- Window mirror modelled `COUNT(...)` over an empty/all-NULL frame as
  NULL; SQL `COUNT` returns 0. Corrected in `WindowModel`.
- Join mirror `"<>"` comparison fell into the `'<'` branch — comparator
  dispatch fixed.
- Troc probes now run against a shadow table (`tab + "b"`) so the
  transaction-splitting oracle cannot mutate the rows other oracles
  depend on.

### Verification

- `sql_oracle_fuzzer_test`, `sql_join_fuzzer_test`,
  `griffin_fuzzer_test`: all pass (incl. seeded-iteration oracles and
  `.test` round-trip replay).
- Sweeps: `sql_join_fuzzer` 8 × 240 s + 2 × 240 s clean;
  `sql_oracle_fuzzer` 4 × 240 s (~2,140 iterations, all oracles incl.
  `CheckGuard` active) clean; expanded `sql_aggregate_fuzzer` 6 × 240 s
  (~3,650 iterations incl. ~8.4k-row parallel-aggregation rounds),
  `sql_session_fuzzer` 2 × 240 s (~2,770), `sql_plancache_fuzzer`
  2 × 240 s (~27.7k), `griffin_fuzzer` 2 × 240 s (~6.3k),
  `expr_oracle_fuzzer` 4 × 200 s (~70.7k, after the ScopedDb
  disk-pressure hardening — the previous run's three crash units were
  all the documented quota-exceeded CHECK, not engine bugs) — all clean,
  no crashes, no mismatches.
- Full `ctest` 2450/2450 and `check_layering.py` clean after the Bug-1
  batch.

## 10. Follow-up round (R15): DML write-path coverage — Bug 27

### Fuzzer expansion

`sql_session_fuzzer` generated only constant-assignment DML and never
exercised transactions rolling back. This round pushed the state-tracking
mirror to the DML expression/subquery edge:

1. **Expression `UPDATE ... SET`.** SET right-hand sides are now full
   scalar expressions — column references, arithmetic, `COALESCE`,
   `CASE`, and string concat — evaluated by the mirror against the
   **pre-update row** (every RHS sees the old row).
2. **Multi-column SET incl. swap.** `SET c1 = c2, c2 = c1` verifies the
   engine evaluates all RHS before assigning (a sequential-assign engine
   loses the swap).
3. **`INSERT ... SELECT` with projection.** The mirror materializes
   source rows/projections before appending (an early draft iterated the
   target row vector while pushing into it — a mirror bug, not engine).
4. **Transaction abort ops.** `-- abort` steps roll the C++ mirror back
   to the pre-transaction snapshot and re-BeginContext; DDL (CREATE/DROP
   TABLE, index builds) is transactional and unwinds too. This exercises
   ARIES undo/CLR paths no fuzzer reached before.
5. **`IN`/`NOT IN` subquery predicates in DML WHERE.** `t.u [NOT] IN
   (SELECT s.k FROM s)` — the QueryData decorrelation path (semi/anti
   join), with the mirror implementing SQL three-valued logic (NULL LHS
   or NULL member yields NULL). Generation respects the documented
   supported boundary (next-actions D-8):
   - LHS is **qualified** (`t.u`): an unqualified name that also exists
     in the subquery's table is treated as correlated and rejected.
   - The subquery never references the DML target (self-reference is
     not decorrelatable).
   - `x IN (subq)` appears only as a top-level conjunct — never under
     `NOT` or `OR` (the decorrelator cannot see through them);
     `INSERT ... SELECT` uses the relational path and additionally
     permits OR-nested and self-referencing subqueries (`SubqMode::
     kAnywhere`).
6. **`sql_oracle_fuzzer` transaction-splitting updates** moved from
   constant SETs to expression SETs, exercising the expression engine
   across commit boundaries.

### Bugs found and fixed

27. **`UPDATE ... WHERE u IN (SELECT ...)` crashed in
    `BuildKeyOffsets`.** `TryDecorrelate` wraps the core plan in a
    `ProductPlan` semi-join whose keys carry qualified names (`t.u`).
    Because UPDATE forces a non-identity projection, the core plan's
    output schema used unqualified names and the join key no longer
    resolved (`CHECK failed: ProductPlan: join key column not found:
    t.u` — SELECT and DELETE took different plan shapes and did not
    fire).  The decorrelation now keeps the key-resolvable columns.

28. **DML decorrelation double-applied SET expressions.** The same
    `TryDecorrelate` trim re-evaluated the original select expressions
    to drop the hidden key columns — `SET a = a + 1` ran twice
    (10 → 12).  The optimizer now trims hidden decorrelation keys with
    a **positional** projection over the already-produced visible
    columns instead of re-evaluating expressions.  Verified across
    `IN`, `NOT IN` (NULL-aware 3VL), `UPDATE`, `DELETE`, and multiple
    semi+anti joins in one WHERE.
    Pin: `QueryTest.UpdateWithInSubqueryDecorrelatesOnce`.

### Generator-boundary notes (not engine bugs)

- `NOT (x IN (subq))` / `NOT (x NOT IN (subq))` in DML WHERE is rejected
  by design (`RuntimeError: query expression requires relational
  evaluation`) — the decorrelator only fires on top-level conjuncts.
- `x IN (SELECT ... FROM x)` self-reference in DML WHERE is rejected by
  design (inner/outer scope collision; D-8).
- Literal `NULL` arithmetic operands (`NULL * c1`) are rejected by the
  AST layer by design; the SET generator avoids them.

### Verification

- `sql_session_fuzzer_test` all pass (24 seeded sessions incl.
  abort/subquery/swap coverage, .test round-trip).
- `QueryTest.UpdateWithInSubqueryDecorrelatesOnce` +
  `WhereConjunctReorderSuppressesSiblingError` pass.
- Sweep: `sql_session_fuzzer_libfuzzer` 6 × 240 s — see below.

## 11. Follow-up round (R16): abort-mismatch hunt — Bugs 29–30

The first abort-enabled sweep produced several `post-abort state
mismatch` reports.  Most were a **mirror bug** (empty-set `NOT IN`: the
mirror evaluated `x NOT IN ∅` as NULL for a NULL LHS; SQL says TRUE —
`IN`/`NOT IN` over an empty set is false/true regardless of the LHS).
One report was a genuine storage bug that deterministic replay could not
reproduce; seed-repeat (`TINYLAMB_SESSION_SEED`/`_REPEAT`) plus a
page-chain dump (`TINYLAMB_SESSION_DEBUG_PAGES`) cornered it:

- tbl3's row page: `row_count=0, row_max=2, slots=[{0,0}{0,0}]` — undo had
  correctly cleared every slot — yet `BeginFullScan` still returned two
  rows whose payloads decoded as the **dropped predecessor table's**
  contents (plus uninitialized-pool bytes past the real record).

### Bugs found and fixed

29. **Stale MVCC version chains survive page-id recycling.**
    `TransactionManager` keys version chains by `RowPosition{page_id,
    slot}` only.  `DROP TABLE` destroys the pages (free-list push) without
    touching their chains; the LIFO free list then hands the same id to a
    later `CREATE TABLE`.  Scans probing a physically vacant slot on the
    recycled page went through `ReadVersion(rp, nullopt)` and were served
    the *dropped table's* committed row image — phantom rows under the
    new table's schema.  Fix: `PageManager::AllocateNewPage` calls
    `TransactionManager::InvalidatePageVersions(pid)` whenever the
    candidate comes off the free list, erasing every `{pid,*}` chain and
    scrubbing the reclaiming transaction's own write set (an
    insert→drop→create-in-one-txn sequence otherwise reuses a stale
    write-set entry and hits `RegisterVersionWrite without reserved
    write intent`).  Pin:
    `FullScanMvccFastPathTest.BeginFullScan_RecycledPageNeverServesDroppedTableRows`.

30. **Uncommitted destroys made page ids reissuable.**
    `MetaPage::DestroyPage` pushes the page onto the free list
    immediately; a *different* transaction could then allocate the id,
    and when the destroyer aborted, the catalog delete undo resurrected
    the dropped table pointing at a page owned by the new table — two
    tables sharing one row page.  Fix: the free page now records its
    `destroyer` txn id; `AllocateNewPage` refuses a free-list head whose
    destroyer is still active and grows past the high-water mark instead
    (`MetaPage::AllocateFreshId`).  Pin:
    `FullScanMvccFastPathTest.AbortedDestroyMustNotClobberReallocatedPage`.

31. **Same-transaction reuse of a pending-destroy page hid restored
    rows.**  The first cut of the Bug-30 fix exempted the destroying
    transaction itself from the reissue block.  A
    drop→create→insert→abort sequence in one txn then recycled the page:
    the reuse-time `InvalidatePageVersions` erased the dropped table's
    chains, and when the abort restored the old page image (slots
    populated again) the resurrected table scanned **empty** — the rows
    were physically present but invisible without their version chains.
    Nondeterministic in the sweep (page-id reuse timing), caught via a
    deterministic `.test` replay.  The reissue block now covers *any*
    active destroyer.  Pin:
    `FullScanMvccFastPathTest.SameTransactionCannotReusePageOfOwnPendingDestroy`.

32. **Destroy undo wrote no CLR, so loser undo did not survive
    crash/recovery.**  The `kSystemDestroyPage` undo arm restored the
    page (`PageInit` + body `memcpy`) but never emitted a compensation
    record — every other undo arm calls a `Compensate*Log`.  After a
    crash, REDO re-applied the original destroy (page back on the free
    list) while the catalog row — whose delete undo *did* log a CLR —
    resurrected the table pointing at a free page.  Post-crash scans of
    the resurrected table then returned zero rows against a committed
    mirror.  Fix: new log type `kCompensateDestroyPage` carrying the
    same payload shape as `kSystemDestroyPage` (restored page type +
    full body image), appended by both the runtime-abort
    (`LogUndoWithPage`) and restart loser-undo (`UndoLoserChains`)
    paths; `LogUndo` treats it like every other CLR (cannot itself be
    undone).  `docs/wal_format.md` updated.  Pin:
    `FullScanMvccFastPathTest.AbortedDestroyRestoreSurvivesCrash`
    (verified to fail when the CLR emission is disabled).

33. **Compiled-plan replays swallowed every executor error.**  The
    dual-transaction session generator flagged a `SET c1 = c1` no-op
    UPDATE that "succeeded" while another transaction held the row's
    write intent — but the intent layer was blameless: the inner
    `Update` executor did latch `kConflicts`.  The error was eaten by
    `RetainedExecutor` (`query/sql_engine.cpp`), the plan-cache
    decorator that forwards `Next`/`NextBatch` but not the
    **non-virtual** `ExecutorBase::GetStatus()` — so the outer cursor
    always reported its own `status_` (kSuccess) and a conflicting or
    failing cached-plan statement replayed as a silent zero-row
    success.  `IndexSkipScanExecutor` (`executor/index_scan.hpp`) had
    the same hole for its inner scan.  Fix: `GetStatus()` is now
    virtual and both decorators forward to `inner_`.  Any runtime
    failure under a plan-cache hit — write-intent conflicts, duplicate
    keys, expression errors — previously surfaced as success, so this
    bug also masked oracle-fuzzer findings on the cached path.  Pins:
    `SqlSessionFuzzer.ConcurrentUpdateSameRowLosesIntentRace`
    (identical statement text forces the RetainedExecutor path) and
    `ExecutorTest.DecoratorForwardsInnerFailure`.

34. **Row-slot reuse masked a snapshot-visible deleted row.**  Txn A
    deletes row u=4 and commits; txn B (snapshot from before the
    commit) inserts into the same table.  `RowPage::Insert` picked the
    first physical hole — u=4's freed slot — and staged B's pending
    version at the same `RowPosition`.  `ReadVersion` serves the
    inserter's own staged pending first, so B's snapshot — still
    entitled to the deleted row — saw u=4 vanish (mirror expected
    `[0,4,5]`, engine returned `[0,5]`).  Fix: the hole loop now
    reserves the intent, then consults
    `TransactionManager::SnapshotSeesRow`; a hole whose committed
    chain still serves a row under the inserter's snapshot is
    disqualified and the just-acquired unstaged intent is dropped via
    `ReleaseWriteIntent` (which also scrubs the `write_set_` entry so a
    later write cannot skip `AcquireWriteIntent`).  One subtlety:
    `SnapshotSeesRow` may only treat an *already staged* own pending
    as superseding — the intent the probe itself just reserved is
    unstaged and must not mask the committed history it is checking.
    Other snapshots are unaffected either way: they read the covering
    committed entry past a foreign pending.  Pin:
    `SqlSessionFuzzer.SnapshotOutlivesConcurrentDeleteCommit` (fails
    without the visibility check, passes with it).

### Mirror/harness fixes (not engine bugs)

- `x NOT IN ∅` / `x IN ∅` three-valued logic in the session mirror
  (empty member set short-circuits before the NULL-LHS check).
- `sql_session_fuzzer` gained `TINYLAMB_SESSION_SEED`/`_REPEAT` and
  `TINYLAMB_SESSION_DEBUG_PAGES` env knobs for nondeterministic-finding
  hunts; `Table` exposes `FirstPageId`/`LastPageId` for diagnostics.
- `-- crash` session step: `Database::EmulateCrash()`, destroy the old
  instance (skipping shutdown write-back), reopen via
  `Database::Create`, then compare the full table dump against the
  committed mirror — exercises ARIES redo + loser undo end to end.
- `-- checkpoint` session step: `Database::WriteCheckpoint()` (new
  forwarder to `CheckpointManager`) forces a fuzzy checkpoint
  mid-transaction, so later crashes recover from the DPT/ATT snapshot
  instead of LSN 0.  The post-step check also verifies checkpointing
  never perturbs visible state.
- `ANALYZE <t>;` op: refreshes persisted statistics + bumps the schema
  epoch inside the open transaction — invisible to the row mirror, but
  abort/crash must roll the stats-B+Tree writes back cleanly.
- `.test` serializer now emits only the first line of the failure
  summary after `-- failure:` and the parser tolerates continuation
  lines, keeping the line-oriented format parseable.

### Verification

- `full_scan_mvcc_fastpath_test`: 6/6 incl. all new pins.
- Full `ctest`: 2465 discovered, all pass (1 intentionally skipped
  nondeterminism test); `check_layering.py` baseline.
- Seed `5523799140708077511` repeated 60× post-fix: clean.
- `ReplayCommittedRegressionFiles` replays every committed `.test`
  including `crash_loser_undo_destroy_9.test`.
- Sweep: `sql_session_fuzzer_libfuzzer` 6 × 300 s — see below.
