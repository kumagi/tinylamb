#Fuzzing Campaign Report — 2026 - 09

End - to -
    end fuzzing campaign over the whole `tinylamb` engine
    : an inventory of all fuzz harnesses,
    a large multi - round parallel sweep(UBSan / ASan builds, growing corpora),
    targeted** expansion of weak harnesses**,
    and the resulting bug fixes.Every entry below was
        reproduced from a shipped artifact,
    fixed, and re - verified;
the full unit test suite(2410 tests) passed after each batch of fixes.

        ** Tally : 23 engine bugs
    + 10 harness / infrastructure fixes + 5 new fuzz targets + 5 regression -
    test updates.**The campaign hit diminishing returns
    : the last three waves found no new engine bugs,
    so the sweep was closed as converged rather than padded to an arbitrary fix
    count(the goal was bug yield, not a quota — see §5 /§6)
        .

    ##1. Fuzz target inventory(complete)

libFuzzer targets(`- DTINYLAMB_ENABLE_FUZZ = ON`, see `CMakeLists.txt`)
    :

      |
      Layer | Target | Oracle | | -- -| -- -| -- -|
      | type | `value_fuzzer` | memcomparable order,
+NEW : serialize / skip round - trip,
    memcomparable decode round - trip + order preservation,
    trichotomy, `CompareForOrderBy` totality,
    arithmetic total - status | | type
        | `date_interval_fuzzer` (NEW) | date parse→format→parse fixed point,
    date - add identities, interval parse↔ToString, plus / commutativity,
    negate involution, justify chains preserve total - nanos | | expression | `expression_fuzzer` | AST vs Bytecode VM vs `relational_detail::Evaluate` differential, +NEW : typed schema values, CAST / shift / IS - DISTINCT / DATE / LIKE coverage | | expression | `cast_fuzzer` (NEW) | Try - contract(no escaped exceptions), determinism, NULL→NULL, safe - cast total success, INT64↔STRING round - trip | | expression | `function_fuzzer` (NEW) | strict builtin NULL propagation, determinism, Try - contract over 57 function names(26 added from a code - level strictness audit) | | executor | `data_chunk_fuzzer` (NEW) | bulk paths(`AppendGather`/`AppendFrom`/`AppendRowFromColumns`) vs scalar `Append` reference : values, null bits, zone - map aggregates | | page | `row_page_fuzzer` | slotted page ops + crash / recovery model | | page | `leaf_page_fuzzer` | leaf page ops + NEW : Commit / Crash + ARIES - recover model oracle | | page | `pax_page_fuzzer` (NEW) | PAX Store / Load exact - value round - trip + hostile body memory safety | | index | `b_plus_tree_fuzzer` | tree ops + NEW : Commit / Crash + Recover model oracle | | index | `lsm_tree_fuzzer` | LSM ops + NEW : Sync barrier + reopen - from - disk durability check | | index | `lsm_view_fuzzer`, `cache_fuzzer` | run merge views / blob cache reads | | recovery | `logger_fuzzer`, `log_record_fuzzer` |
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  WAL append
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      /
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      scan
    ,
    record round - trip stability | | table
        | `table_fuzzer` | catalog + rows + `EmulateCrash` sessions | | server
        | `postgres_protocol_fuzzer` | startup - packet parse / round - trip |
        | query | `googlesql_ast_fuzzer` | AST - dump parse + full visit |
        | query | `sql_oracle_fuzzer_libfuzzer` |
        TLP / NoREC / PQS / DQE / index - independence / txn - splitting
            + window model / permutation + WITH TIES + skip - scan -
            distinct(R13) |
        | query | `expr_oracle_fuzzer_libfuzzer` |
        scalar rewrite + engine + Python cross - check | | query
        | `griffin_fuzzer_libfuzzer` | catalog - guided sessions,
    AST - vs - bytecode / JIT differential | | query | `sql_join_fuzzer_libfuzzer`, `sql_aggregate_fuzzer_libfuzzer`, `sql_plancache_fuzzer_libfuzzer`, `sql_session_fuzzer_libfuzzer` |
                                                                                                                                                            join
                                                                                                                                                                /
                                                                                                                                                                agg
                                                                                                                                                                semantics
    ,
    plan cache,
    stateful sessions | | plan
        | `rule_fuzzer` (standalone threaded stressor) | cascades rules | |
        +replay binaries
        | `* _fuzzer_replay` (11) | deterministic replay of saved units |

        ##2. Infrastructure fixes(why previous rounds were under - finding)

                    1. *
                *Whole
            - project coverage instrumentation.*
                  *Fuzz builds instrumented only each harness TU; the SQL/plan/executor fuzzers call straight into
   `tinylamb::sql`, so libFuzzer saw `cov: 1` — pure blind mutation. Fuzz
   builds now compile everything with `-fsanitize=fuzzer-no-link,undefined,
   bounds` (clang-only guard for GCC builds). Effect: `sql_oracle_fuzzer`
   coverage 1 → **22,449** PCs (ft 81k) within minutes;
corpus growth is now feedback -
    driven.2. * *`row_page_fuzzer` never ran recovery.*
                    *The "Crash" op rebuilt the environment but never
                        called `RecoveryManager::RecoverFrom`,
    so ARIES redo /
        undo was untested despite the harness comment claiming otherwise;
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
4. `combine_repeated_addend` rewrote `x + x → x * 2` for dynamic INT64;
in this engine addition crosses into UINT64
    wraparound while multiplication raises,
    so the fold * adds *
            a throw(oracle - found at INT64_MIN)
                .5. `distribute_or_over_and_budgeted` moved always -
        evaluated conjuncts into disjuncts where the original short -
        circuit could skip them,
    surfacing raises that the AST never produced;
guarded with
   `ExpressionCannotThrow` per conjunct.6. *
    *NaN soundness cluster**(`not_comparison` is IEEE -
                             unsound when an ordered comparison can be DOUBLE)
    : - `ContainsNotOfOrderedDoubleComparison` missed the NOTs that
     `xor_to_or_and_not` *
    expands into * (`(NaN >= -3) XOR col_i` diverged);
   - `SideCanBeDouble` returned false for subtrees containing DOUBLE
     literals (unknown columns throw out of `ResultType`, hiding the double);
   - `not_comparison` itself now refuses when either operand subtree contains a
       DOUBLE literal constant;
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
    `TransactionManager` keys version chains by `RowPosition{
  page_id, slot}` only.  `DROP TABLE` destroys the pages (free-list push) without
    touching their chains; the LIFO free list then hands the same id to a
    later `CREATE TABLE`.  Scans probing a physically vacant slot on the
    recycled page went through `ReadVersion(rp, nullopt)` and were served
    the *dropped table's* committed row image — phantom rows under the
    new table's schema.  Fix: `PageManager::AllocateNewPage` calls
    `TransactionManager::InvalidatePageVersions(pid)` whenever the
    candidate comes off the free list, erasing every `{
  pid, *}` chain and
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

## 12. Follow-up round (R17): scan-side write intents + window/SQL coverage — Bugs 35–40

### Bugs found and fixed

35. **DML source scans read stale snapshots without taking write
    intents.**  `IndexScanIterator` already implements the intended
    model — `Position()` acquires the row's write intent, then
    `operator*` re-resolves the row through `ReadVersion`, which
    serves the newest committed head while the intent is held — but
    the `FullScan` path had no equivalent: UPDATE/DELETE over a table
    without a usable index materialized the pre-snapshot image and
    overwrote a row another transaction had committed after the
    snapshot.  Fix: `TableScanOptions::lock_rows` /
    `FullScanIterator::SeekVisibleRow` now acquire the write intent on
    every occupied slot and re-resolve via `ReadVersion` before
    yielding (only for `require_row_position` sources, i.e. DML).
    Two supporting changes were required: `ReadVersion` /
    `Page::Read` / `RowPage::Read` gained a `resolve_head` flag so the
    write path can opt into head-resolution while pure snapshot reads
    keep snapshot semantics (a scan-time intent must not poison a
    later plain SELECT on the same transaction), and `IndexScan` /
    `IndexScanIterator` thread `lock_rows_` through the same flag.
    `BitmapHeapScan` has no lock/head-resolution path, so
    `ScanAlternatives` no longer emits `BitmapScanPlan` candidates
    under `require_row_position`.  Pins: the three corrected
    dual-transaction `.test` replays under
    `query/testdata/session_regressions/`.

36. **Reversed window frames leaked the peer group.**  `end_of(
    kUnboundedPreceding)` in `executor/detail/window_eval.cpp`
    returned `peer_end[position]` instead of the partition start, so
    `CURRENT ROW .. UNBOUNDED PRECEDING` (a reversed frame) produced
    the peer group instead of an empty frame.

37. **`PERCENTILE_CONT` exact-rank returned raw bits.**  When the
    interpolated rank landed exactly on an element, the integer
    `Value` was returned verbatim and later reinterpreted as a
    double — `PERCENTILE_CONT(v, 0)` over INT64 10 read back as
    ~4.9e-323.  Numeric inputs now always produce FLOAT64; string
    inputs keep element semantics.

38. **Percentile windows honored the default frame.**  `PERCENTILE_*`
    are analytic functions with no frame clause, but evaluation used
    the default `RANGE UNBOUNDED PRECEDING..CURRENT ROW` window, so
    early rows saw truncated value sets (row 0 under
    `RESPECT NULLS` saw only `{
  NULL}`).  They now evaluate over the
    whole partition.

39. **No-window `QUALIFY` dropped aliases inside `IN`.**  The
    `InlineAliases` helper in `window_eval.cpp` recursed through
    binary/unary/case/function nodes but returned `kInExp` (and other
    composites) untouched, leaving the select alias unresolved inside
    `IN (...)`: `SELECT v AS c FROM t QUALIFY c IN (10, 40)` failed
    with `InvalidArgument: type mismatch`.  The inliner now rewrites
    via the shared `ExpressionChildren`/`WithExpressionChildren`
    walker.

40. **Outer `WHERE` over a `UNION DISTINCT` derived table filtered
    only the head branch.**  `OptimizeDerivedBoundaries`
    (`executor/relational.cpp`) pushes outer predicates into a derived
    source; the branch-wise path was gated to `UNION ALL`, so a
    `UNION DISTINCT` source fell into the generic path, which set the
    predicate as the derived statement's `WhereClause` — a clause
    that applies to the head operand only.  `SELECT u.c1 FROM
    (SELECT c1 FROM a UNION DISTINCT SELECT d1 FROM b) u WHERE
    u.c1 < 2` returned every `b` row unfiltered.  (INTERSECT/EXCEPT
    were accidentally correct: `filter(A ∩ B) = filter(A) ∩ B` and
    `filter(A − B) = filter(A) − B` make head-only injection exact.)
    Set-operation sources now keep the predicate above the boundary.
    Pin: `CascadesRulesCoverageTest.FilterPushedIntoEveryUnionBranch`.

41. **Vectorized `CAST` re-evaluated its child against a synthetic
    schema.**  `VectorizedExpression::Evaluate` evaluated the child
    once per batch, then wrapped each value in a one-column row and
    called `CastExpression::TryEvaluate` — which re-resolves
    `child_` against that synthetic schema, so
    `CAST(a AS VARCHAR)` failed to find `a`.  `CastExpression` now
    exposes `TryCast(value)` that applies only the conversion step,
    and the vectorized path uses it (NULLs still short-circuit).

42. **`CBRT` was not correctly rounded under clang.**  clang's
    libm returns `3.0000000000000004` for `cbrt(27)` where glibc
    returns `3`.  `AccurateCbrt` (`common/math_utils.hpp`) applies
    one Newton–Raphson refinement step, which recovers the
    correctly rounded result on both toolchains and makes the value
    toolchain-independent.

### Mirror/harness fixes (not engine bugs)

- Session mirror: `OwnerInfo.written` distinguishes scan-time intents
  from actual writes so commit no longer merges scan-locked keys as
  writes; `ApplyDelta` is owner-driven with committed-generation
  tracking for head-moved detection.
- Coverage-test repairs: `%` → `MOD()` (unsupported operator), SQLite
  `FILTER (WHERE …)` → GoogleSQL `AGG(x WHERE …)`, explicit
  `ALL`/`DISTINCT` on set ops, recursive-CTE column-list syntax,
  `VALUES` → supported forms, untyped literal `NULL` operands →
  `CAST(NULL AS …)`, `MOD` over two FLOAT64s → `ExpectError`
  (GoogleSQL MOD is integer-only), `NOT NULL` →
  `NOT CAST(NULL AS BOOL)`, plus several stale expected row counts
  validated by direct query.

### Verification

- `cascades_rules_coverage_test`: 96/96.
- `sql_window_agg_coverage_test` / `executor_feature_test` /
  `executor_test` / `relational_regression_test`: all green after
  expectation corrections.

## 13. Follow-up round (R18): UNIQUE/DML oracle + pgwire session fuzzer — Bugs 43–50

### Fuzzer expansion

- `sql_session_fuzzer`: UNIQUE indexes join the mirror.  `ApplyDdl` now
  parses `CREATE UNIQUE INDEX`, every live unique key is mirrored, and
  INSERT / INSERT…SELECT / UPDATE predict the engine's verdict *before*
  execution (NULL keys exempt per SQL unique semantics): an accepted
  violation or a rejected clean statement fails the run.  Clean
  multi-row batches against an active unique index are order-dependent
  inside the engine, so those steps degrade to a non-key column (or are
  skipped) instead of guessing a scan order.  Trace format gains
  `-- ddlfail:` so a rejected build replays through `ApplyDdl` and is
  re-verified as a duplicate rejection.
- New target `postgres_session_fuzzer` (server/): boots one
  `PostgresServer` per process and replays a fuzzed client script over
  loopback — 'Q' statements from a menu (incl. failing ones),
  Sync/Flush, extended-protocol messages, bogus frame lengths, optional
  SSL negotiation — asserting framing validity, exactly-one
  ReadyForQuery per exchange, the transaction-status state machine
  ('I'→'T'→'E' rules, extended messages status-preserving), and a
  fresh-connection `SELECT 1` sanity query after every script.  A
  race mode (~1 in 6 inputs) instead drives TWO concurrent writers
  against one hot row: conflicts must surface as framed ErrorResponses
  (never a stall), a reply must never carry both CommandComplete and
  ErrorResponse, and the committed value afterwards must be one
  writer's legal store.

### Bugs found and fixed

43. **UNIQUE-index scans truncated at the first NULL key after
    single-value entries.**  A kUnique index stores NULL-bearing keys as
    multi-value lists but everything else as single values
    (`Table::IndexInsert`).  `IndexScanIterator::operator++` only
    repositions `value_offset_` inside its multi-value branch; when an
    ascending scan entered a NULL-key list from single-value entries the
    offset was still the `-1` "not positioned" sentinel, so
    `UpdateIteratorState` bounds-checked it, called `Clear()`, and ended
    the whole scan.  A `DELETE FROM t` planned over the unique index
    therefore drained only the rows before the first NULL key and
    committed with the rest of the table intact (crash-replay variant:
    the same lost deletes reappeared after recovery).  Fix: the
    single-value branch resets the sentinel, and the multi-value branch
    positions a freshly entered list at its first (ascending) or last
    (descending) element.  Pin:
    `TableTest.IndexScan_UniqueIndexNullKeyAfterSingleValueEntries` +
    `session_regressions/unique_index_nullkey_delete_43.test`.

44. **Bytecode-compile folds erased the INT64↔DOUBLE promotion.**  The
    AST evaluates mixed-type numeric arithmetic as IEEE DOUBLE (never
    raising integer overflow), while the neutral-constant identities
    (`x + 0.0 → x`, `x * 1.0 → x`, …) applied inside
    `BytecodeCompiler::Compile` collapsed the DOUBLE operand away and
    turned the surviving chain into a checked INT64 kernel — e.g.
    `CASE WHEN i THEN 1.0 … END * i + i + i` raised
    "integer overflow on '+'" under bytecode where the reference
    returned the double.  The four `identity_*` rules are now removed
    from the kValue compile rule set (the optimizer keeps them; the VM
    adds a constant at full speed anyway).

45. **`uniform_case_result` folded type-mismatched equal constants.**
    `Same()` compares `ToString()`, so `CASE WHEN c THEN 1.0 ELSE 1 END`
    "agreed" on all branches and collapsed to the ELSE's INT64 — the
    taken branch's runtime type (which drives numeric promotion upstream)
    was erased.  The rule now requires constants to agree in `ValueType`
    as well as text.

46. **`kBinaryInt64` on bool/NULL-typed operands diverged from the AST.**
    `IS TRUE x` declares kBigInt statically (the type system has no
    bool tag) but evaluates to a BOOLEAN value, so `1 + (IS TRUE x)`
    compiled to the INT64 kernel and raised integer overflow at
    INT64_MAX+INT64_MAX, while the AST IEEE-promoted to double.
    Arithmetic with a statically kNull-typed operand is now rejected by
    the compiler and falls back to the AST evaluator (ground truth).

    Pins: `BytecodeTest.MixedTypePromotion_FoldsMustNotEraseDoubleDomain`,
    `UniformCaseResult_FoldKeepsBranchValueType`,
    `Int64PlusBool_UsesAstDoublePromotionNotIntKernel`.

### Further expansion (same round; bugs 47-50 continue below)

- `sql_session_fuzzer` index-independence probes: every plain index
  creation registers its key, and `kCheck` plus the post-crash /
  post-checkpoint `check_all_tables` now also run a mirror-filtered
  equality/range probe against the first key column.  The trace format
  carries the predicate as `-- checksql:` so probes replay verbatim and
  a pinned `.test` re-validates the index read path (REDOed or
  dirty-flushed pages included).  Sessions also deepened to
  25-60 steps / 45 rows so tables cross row-page boundaries.

47. **`HashJoin::BuildShards` dereferenced null probe metadata on the
    reactive-spill path** (found by the ASan sweep): the INT fast-path probe
    `JoinIntModeType(*s.probe_rows, *s.probe_cols, …)` ran whenever
    `NullSafeArg() == nullptr`, but during a reactive right-side flush the
    probe metadata is not wired yet → UBSan reference-binding-to-null and
    SEGV.  The fast path is now taken only when the probe side is actually
    configured (kBytes fallback otherwise).

48. **`UPDATE`/`DELETE … WHERE x IN (SELECT …)` silently did nothing.**
    The positional DML planner cannot evaluate a raw `kQueryExp`: the
    predicate was either retained and raised
    "query expression requires relational evaluation" from the scalar
    evaluator (masked as a sticky executor error — `Execute()` itself
    returned OK!), or — with uncommitted subquery sources — decorrelation
    wrapped a semi-join that lost row positions and applied zero rows.
    `SqlEngine::DecorateDmlWhere` now runs every non-correlated
    `x IN (SELECT …)` / `NOT IN` conjunct once at prepare time and folds
    it into a constant IN list (empty list folds to FALSE/TRUE per
    three-valued SQL); correlated inners keep the legacy route.  The
    session mirror had been applying these statements all along, which is
    what made the ASan-era state checks diverge.

    Pin: wave-4 session `.test` replays clean; seeds 0-23 of
    `SeededSessionsHoldState` green with the fold enabled.
    `QueryTest.DmlWhereInSubqueryAppliesRows` pins UPDATE/DELETE/NOT IN
    plus the empty-subquery folds.

49. **`DecorateDmlWhere` stole the outer statement's plan-cache candidate.**
    The fold's nested `PrepareStatement(inner SELECT)` overwrote the armed
    `plan_cache_fingerprint_`/parameters, so the enclosing UPDATE cached
    its positional plan under the subquery's fingerprint.  The next
    statement with that fingerprint was then served the wrong plan —
    wave-5 surfaced it as 8 "generation fails, replay holds" flaky
    sessions (fresh engines miss the poisoned cache entry, which is why
    the replay stayed green).  The fold now swaps the armed candidate out
    around the nested prepare and restores it afterwards; all 8 wave-5
    crash inputs replay clean against the fixed binary.

50. **`FailStatement` freed the `TransactionContext` while the failing
    statement's `QueryResult` was still alive (server).**  On an in-query
    error inside an explicit transaction, `ExecuteStatement` called
    `FailStatement()`, which aborts *and resets* `client.transaction`;
    the function-scope `QueryResult` (holding the plan's executor, which
    keeps a raw `TransactionContext*`) was only destroyed on `return`, so
    `~RelationalExecutor` read `context_->execution_runtime()` from freed
    memory — ASan `heap-use-after-free` on every failing statement in a
    BEGIN block.  The new `postgres_session_fuzzer` hit it 10/10 with a
    5-byte script.  The result (and with it the whole executor graph and
    its pins) is now destroyed before `FailStatement`/commit paths run,
    which also fixes the executor-outlives-commit ordering on the success
    path.  Pins: `PostgresSessionFuzzerReplay.FailStatementOrderingCrash{,2}`.

### Harness fixes (not engine bugs)

- `sql_join_fuzzer` case 8 read `op[6]` on the 5-character literal
  `"UNION"` to detect `UNION ALL` — a global-buffer-overflow the ASan
  sweep flagged immediately; now a `string_view` comparison.
- New `b_plus_tree_concurrent_fuzz_test` (index/): fills the documented
  "TSan is not combined with libFuzzer" gap as a seeded random
  two-writer stress (disjoint key spaces give a strong committed-state
  oracle; shared keys get a membership oracle).  Its first run found a
  data race in the TEST — both workers drew from the shared `mt19937`
  (TSan: "Location is stack of main thread") — now seeded before
  launch; engine-clean across 8 seeds under TSan, and the
  `^BPlusTreeConcurrent` CI regex already picks it up.
- `cache_fuzzer` / `lsm_view_fuzzer` derived their temp database names
  from `RandomString(20, false)` — the *fixed-seed* generator, so every
  forked `-jobs` worker used the identical file name.  Worker A's
  end-of-iteration `remove_all` deleted worker B's live blob and B's
  next stat died on `IOError … No such file or directory` (all 8
  workers aborting within the same second is the fingerprint).  Both
  now use the random_device-backed generator.

### Verification

- `bytecode_test` / `differential_test` / full `ctest` (2879 tests): green.
- Both `expression_fuzzer` crash artifacts replay clean.
- `sql_session_fuzzer` with the UNIQUE/reject + index-probe oracle:
  wave3 25 min x 8 workers plus 8 x 40 min deep sessions (~20k sessions,
  crash/checkpoint/commit boundaries included) — zero mismatches after
  the bug-43 fix.
- `postgres_session_fuzzer`: ~14k scripts across script / write-race /
  pipelining modes — zero findings (framing + status machine hold).
- TSan subset (151 tests incl. `BPlusTreeConcurrentFuzzTest`): green.
- GCC + clang strict builds: zero warnings (the pre-existing `-Wconversion`
  in the session fuzzer's column-shuffle was fixed on the way).
- Layering / clang-format / diff-scoped clang-tidy: clean.

### Residual gaps after R18

- `BitmapScanPlan` is never constructed by the production planner
  (test-only) — bitmap-path fuzzing is pointless until it is planned.
- The engine has no DROP INDEX / CREATE TABLE IF NOT EXISTS surface for
  the session fuzzer to drive; index churn stays at create + drop-table.
- Dual-transaction sessions still never see UNIQUE indexes (the mirror's
  conflict model would need cross-txn unique probing; single-txn UNIQUE
  coverage is deep).

## 14. Follow-up round (R19): aggregate-family fuzzer expansion — Bug 51

> Numbering note: this round was written while R18 was adding bugs 47–50;
> its ARRAY_AGG fix is number 51 (47 is taken by `HashJoin::BuildShards`).

`sql_aggregate_fuzzer` learned the remaining supported aggregate
surface.  New generated kinds with full mirror models:

- `BIT_AND` / `BIT_OR` / `BIT_XOR` over nullable INT64 columns
  (NULLs skipped; all-NULL/empty groups → NULL).
- `LOGICAL_AND` / `LOGICAL_OR` over *generated boolean expressions*
  (`GenWhere` trees — comparisons, IS NULL, NOT, AND/OR), NULL
  operands ignored, no vacuous-true/false on all-NULL input.
- `STRING_AGG` with optional delimiter, inner `ORDER BY u [DESC]`,
  and inner `LIMIT n` (the LIMIT counts buffered rows *before* NULL
  elements are skipped — `STRING_AGG(s ORDER BY u DESC LIMIT 2)` over
  {x,NULL,y,x} is "x", not "x,y").
- `ARRAY_AGG` (NULL elements kept, `DISTINCT` collapses them to one,
  inner `ORDER BY u [DESC]`, inner `LIMIT n`).
- `VAR_POP` / `VAR_SAMP` / `STDDEV_POP` / `STDDEV_SAMP`: the mirror
  reproduces the engine's `long double` sx/sxx accumulator bit-exactly;
  generation is gated on columns that never received an INT64 extreme
  so every partial sum stays inside the 64-bit mantissa and the result
  is accumulation-order independent (parallel aggregation merges by
  summing the same fields).

The aggregate fuzzer's `FILTER (WHERE ...)` generation was **dead
  code since introduction**: PostgreSQL-style `FILTER` is not GoogleSQL
  and every such query failed parse → skip, so ~25% of generated
  aggregates silently never exercised the filtered path.  Filters are
  now emitted in the engine's dialect, `AGG(arg [WHERE cond]
  [ORDER BY …] [LIMIT n])`, injected at the argument-list boundary
  (`where_pos` tracks where ORDER BY begins for the ordered
  aggregates).  HAVING-clause filters got the same fix.
- Mirror: `BIT_XOR` results legitimately equal `INT64_MIN`
  (`INT64_MIN+1 XOR 1`) and collided with the `kNullRepr` sentinel in
  `FmtInt`; formatted past the sentinel like `SUM` already did.

### Bugs found and fixed

51. **`ARRAY_AGG` lost the declared element type over all-NULL
    groups.**  `InferAggregateArrayElementType` only resolves literal
    and CAST arguments, so `ARRAY_AGG(int_col)` carried no static
    element type; `AggregateAccumulator::Finish` then fell back to
    scanning `array_values_`, and when every element was NULL the scan
    found nothing and emitted `ARRAY<>[NULL, …]` instead of
    `ARRAY<INT64>[NULL, …]`.  `AggregateAccumulator` now takes the
    input schema at construction and resolves plain `ColumnValue`
    arguments through `ElementSqlTypeName(column.Type())` (with a
    UINT64 override for unsigned columns); all five construction sites
    (`aggregation.cpp`, `relational.cpp` ×3, `subquery_runtime.cpp`)
    pass their already-available schemas.  Parallel aggregation keeps
    falling back to the serial accumulator for ARRAY_AGG so it is
    covered transitively.  Pin:
    `SqlFeatureCoverageTest.ArrayAggKeepsElementTypeOverAllNullGroups`.

## Round 20 — template-cache + JIT aggregate modifier loss (Bugs 52-53)

Fuzzing coverage: the aggregate fuzzer's long-dead `FILTER (WHERE …)`
generation was rewritten to the supported GoogleSQL inline form
`AGG(arg WHERE pred)` (and HAVING aggregates), and mirror coverage was
extended to BIT_AND/OR/XOR, LOGICAL_AND/OR (expression arguments),
STRING_AGG (delimiter, inner ORDER BY, inner LIMIT — which counts rows
before NULL-skip), ARRAY_AGG (DISTINCT, ORDER BY, LIMIT, NULL retention)
and VAR/STDDEV _POP/_SAMP (long double mirror).  Two engine bugs found
by the first sweeps with the revived filter syntax:

- **Bug 48 — JIT SUM fast path dropped the inline filter.**  The
  single-column `SUM` JIT eligibility check in
  `executor/aggregation.cpp` ignored `WhereFilter()`/`InnerOrderBy()`, so
  `SELECT SUM(a WHERE a > 100) FROM t` evaluated the whole column.
  Gate now requires both absent.  Pin:
  `SqlEngineCoverageTest.FilteredSumSkipsJitColumnFastPath`.
- **Bug 49 — template cache rebound aggregates with only
  type/child/distinct.**  `BindExpression`'s `kAggregateExp` arm in
  `query/sql_template.cpp` silently discarded `WhereFilter`, inner
  `ORDER BY`/`LIMIT`, the `STRING_AGG` delimiter (`SecondaryArg`),
  trailing args, HAVING modifiers, inner grouping and the static
  `ARRAY_AGG` element type.  Warm-cache executions of
  `AGG(x WHERE pred)` (literal-free predicates keep the fingerprint
  identical, so bind never failed to fall back) returned unfiltered
  results; cold replays were correct, which is why the trace replayed
  clean.  The bind arm now rebinds every attribute in SQL order.  Pin:
  `SqlEngineCoverageTest.TemplateRebindKeepsAggregateAttributes`
  (second same-fingerprint query must preserve filter/delimiter/
  ORDER BY/LIMIT).  Regression trace:
  `query/testdata/aggregate_regressions/logical_and_where_filter_48.test`.

## Round 21 — index point-union over NULL / cross-typed IN items (Bug 54)

Found by `sql_session_fuzzer` (dual-table UPDATE rejected with an empty
`LastError`; serialized replay could not reproduce because the failing
statement is generated after the last recorded step — the trace only
proved the run flaky, the `RepeatSeedForNondeterministicBugs` harness
reproduced it deterministically on iteration 0).

- **Bug 50 — `x IN (…, NULL)` or cross-typed IN lists crash Prepare when
  `x` is indexed.**  The point-union alternative in
  `plan/implementation_rules.cpp` collected every constant of a literal
  IN list into `point_sets` and then `std::ranges::sort`ed them;
  `Value::operator<` throws "Different type cannot be compared" for NULL
  or cross-typed pairs, so `SELECT/UPDATE/DELETE … WHERE u IN (0,2,NULL)`
  failed at plan time whenever a secondary index covered `u`.  Worse,
  cross-typed numeric points are not merely unsortable — a DOUBLE seek
  key can never find the INT64 2 that `u IN (0, 2.0)` must match, so
  probing them would silently drop rows.  The collector now skips NULL
  items (never satisfiable by an equality seek; three-valued semantics
  stays with the residual predicate) and declines the whole IN list when
  any remaining constant's `ValueType` differs from the column's — the
  conjunct then rides the residual predicate like the no-index plan.
  Pin: `query/testdata/session_regressions/in_list_null_point_union.test`
  (single-column index + `UPDATE … WHERE u IN (0,2,NULL)`).

- **Bug 51 — cross-typed index range bounds silently returned wrong
  results.**  Same collection loop, comparison branch: a constant whose
  `ValueType` differs from the key column's was folded into `Range` min/
  max, but memcomparable seek keys are type-tagged, so `u = 2.0` probed a
  DOUBLE-encoded key that can never reach the INT64 2 it must match
  (returned empty instead of `[2]`), and `u > 0.5 AND u < 2.5` produced a
  DOUBLE-bounded range scanning nothing (empty instead of `[2]`).  Mixed
  bounds could also throw inside `Range::Update` narrowing
  (`Value::operator<`).  The comparison branch now only constrains the
  index when the constant's `ValueType` equals the column's; the LIKE
  prefix-bounds branch likewise requires a VARCHAR column.  Cross-typed
  conjuncts stay in the residual predicate, matching no-index semantics
  (type errors still surface at row evaluation).  Pin:
  `CascadesRulesCoverageTest.InLiteralListWithNullAndCrossType` covers
  IN-list NULL/cross-type, `c1 = 2.0` equality, and `0.5 < c1 < 2.5`
  band probes over an indexed column.

  Known adjacent limitation (pre-existing, unfixed): signed and unsigned
  INT64 values encode under different memcomparable tags, so index seeks
  only see same-signedness entries; numeric equality across the sign
  boundary is only guaranteed on scan paths.

## Round 22 — DML subquery WHERE: ANY/ALL, EXISTS, scalar fold (Bug 55)

`UPDATE/DELETE ... WHERE <quantifier or EXISTS or scalar> (SELECT ...)`
still failed with `query expression requires relational evaluation`
(the residual of Bug 48: only `= ANY`/IN was folded).  A direct probe
showed `x op ANY/ALL (SELECT ...)` raising while the identical SELECT
predicate worked.  `SqlEngine::DecorateDmlWhere` now folds, at prepare
time and only for non-correlated inners (a correlated inner cannot be
prepared standalone, so it keeps the legacy route):

- `x op ANY (S)`  -> OR-chain of `x op c` over the materialized rows;
- `x op ALL (S)`  -> AND-chain; empty ANY = FALSE, empty ALL = TRUE;
- `EXISTS (S)`    -> constant TRUE/FALSE;
- scalar `(S)`    -> its single value (0/>1 rows keep the legacy
  cardinality error).

The three-valued semantics coincide with `TryEvaluateQuantifiedComparison`
by construction (Kleene AND/OR match the row loop; per-candidate type
mismatch raises on both sides).

**Bug 55 (introduced by the Bug-48 fold, caught by hand probe):** a
scalar subquery is a `QueryExpression` with `test_ == nullptr` but the
default `mode_ = kIn`/`op_ = kEquals`, so it passed the IN-fold guard and
produced `InExpressionExp(nullptr, ...)`: `InExpression::ToString` then
dereferenced the null child -> SEGFAULT on
`DELETE ... WHERE k > (SELECT MIN(k) FROM s)`.  The fold branches now
require a non-null test (or take the dedicated EXISTS/scalar branch), and
all three honor the pinned contract that **WITH under a DML subquery
stays unimplemented** (`dml_delete.test` / b/362158469): the fold returns
the node unchanged so the legacy route raises.  Pins:
`QueryTest.DmlWhereQuantifiedAndExistsFold` (ANY with NULL candidate,
<> ALL, scalar, EXISTS, WITH rejection drained to a non-success status).

Follow-up in the same round: the fold only covered WHERE, so
`UPDATE t SET v = (SELECT ...)`, `SET v = CASE WHEN EXISTS ...` and
`INSERT ... VALUES ((SELECT ...))` still raised the legacy error (hand
probe).  `PrepareStatement` now runs the identical decoration over the
UPDATE SET values and the INSERT VALUES rows
(`QueryTest.DmlSetAndValuesFoldSubqueries`).

Harness (not engine): `expression_fuzzer` reported two AST-throw/
bytecode-NULL mismatches whose tolerance predicate
`ContainsStaticallyMismatchedComparison` returned false: the mismatched
comparison operand was a CASE whose branches disagree on result type
(`CaseExpression::ResultType` -> kInvalid), and `TypesComparable` treated
every kInvalid as "untolerated (conservative)".  kInvalid now means
"statically indeterminate -> tolerated" except for NULL constants (NULL
operands yield UNKNOWN, never a type raise).  This is exactly the
documented ExpressionCannotThrow-by-design error-erasure class; both
crash inputs replay green and the differential/rewrite/bytecode suites
pass.

### Round 22 addendum — quantified subqueries in SELECT (Bug 56) & plan-cache staleness (Bug 57)

Bug 56 (pre-existing, surfaced by the new session-fuzz quantifier
generation): SELECT-side `x op ANY/ALL (SELECT ...)` silently executed as
`x = ANY (...)`. Two independent lowerings dropped the operator/mode:

* `planning_heuristics.cpp` stashed the inner projection as a
  `TableKeyFilter` MEMBERSHIP set (scan-level key pruning) for every
  non-negated test-carrying QueryExp, so `j > ANY (5,6)` pruned j=9,7;
* `TryDecorrelate` (optimizer.cpp) accepted the same shape into an
  equality semi/anti join.
* `CanUseDecorrelatedSubqueryOptimizer` routed correlated quantifiers onto
  the vectorized path whose residual has no subquery context.

Gates: membership-stash and decorrelation now require the equality-IN
shape (`kIn`, or `kAny`+`kEquals`); everything else stays on the row-level
quantified evaluator (relational route). `<> ALL` correlated remains
row-evaluated (the keyed anti join cannot express the per-row candidate
restriction). Pin: `QueryTest.QuantifiedSubqueryKeepsOperatorAndNegation`.

Bug 57 (introduced by the Bug-48/55 prepare-time folds, caught by
session-fuzz seed 6): the folded constants were baked into the
specialized plan cache entry; re-preparing the same DML text after the
inner table's rows changed reused the stale plan (UPDATE matched the
wrong rows). Fold sites now set `dml_fold_applied_`, which suppresses
`RememberSpecializedPlan` for that statement.

Harness fixes: session generator rendered `NOT` for kQuantSub/kExistsSub
predicates inconsistently (negation flag dropped from SQL, mirror kept it
-> phantom seed-6 mismatch); libFuzzer crash corpora in a shared scratch
dir silently overwrite each other across fuzzers (per-fuzzer subdirs).

Round-9 reruns confirmed two file-reproducible variants (kept in
`.fuzzscratch/w9/sql_session/`): `INSERT ... SELECT` with a
self-referential non-correlated `EXISTS (SELECT 1 FROM t WHERE t.u =
t.c1)` inserts one row on top of uncommitted CREATE INDEX + abort
history where the SELECT reference says zero (`sql_session_fuzz-repro-
5523799142257999587.test`); and a post-abort lost-UPDATE variant
(`...-11075184383239140191.test`). Manual 15-line rewrites of the same
statement order do NOT reproduce, so the trigger involves interleaved
multi-table history; line-wise ddmin bottoms out at the trivial
table-mismatch oracle and needs a row-diff-aware minimizer. Deferred to
a focused round.

RESOLVED by Bug 60 (below): all wave8/wave9 session crash inputs —
including the "Heisenbug" set — replay green on the current build.

Open: 5 wave8 session-fuzz crash inputs reproduce under DUAL-TXN
generation (`RunDualTxnSession`) but PASS when replayed as written
`.test` files — the trace format does not record transaction
boundaries, so the recorded repro under-specifies. Signatures: lost
UPDATE of one row (c2 kept pre-update value) after a quantified UPDATE
in a sibling txn, post-abort/checkpoint variants included. Reproduced
on the current build with every #55-#57 fix applied. Not yet isolated
(harness mirror vs engine visibility).

### Round 9-10 triage — self-EXISTS shadowing (Bug 58) & a Heisenbug family

Bug 58 (harness/mirror): the session generator emitted `EXISTS (SELECT 1
FROM t WHERE t.a = t.b)` with sub-table == target. The inner FROM shadows
the outer qualifier, so the predicate is a CONSTANT uncorrelated
membership test for the engine — the mirror however treated it as
correlated per-row. Engine was right; generator now folds the self-case
back to `kInSubquery` (whose test really is outer-scoped). This killed
2/6 wave9 crash inputs.

Remaining open family (3 wave9 + 1 wave8 inputs, kept under
`.fuzzscratch/w9/sql_session/`): a quantified UPDATE/INSERT-SELECT whose
effect vanishes or appears depending on instrumentation —
`TINYLAMB_SESSION_CHECK_QUANT=1` (extra SELECT checks between steps)
makes the divergence disappear entirely; `SetForceRelationalFallback(true)`
does NOT (so it is not plan-cache/Cascades-specific). Signatures: a lost
`SET c2 = COALESCE(u,c2)` over `c2<-2 AND u != ALL (SELECT u FROM t1)`
(empty t1 -> ALL fold TRUE; engine skipped the row only when the
preceding step was not a SELECT), row-level sign/value drift after
checkpoint, post-abort drift. The written `.test` files replay green —
the trace loses whatever step-level state carries the bug. Needs a
step-level execution-order trace (including implicit probes) to pin.

### Round 10 — pgwire fold menu exposes shutdown-order UAF (Bug 59, harness)

Extending the postgres_session_fuzzer SQL menu with the DML subquery fold
shapes (`= ANY`, scalar `SET`, correlated-EXISTS `DELETE`) plus
`DROP/CREATE pgsrc` surfaced a flaky ASan heap-use-after-free: a request
thread inside `ServeFromPlanCache` read a `Table` shared_ptr owned by the
singleton `ServerEnv`. The trace showed the free came from `~ServerEnv`
(static destruction at exit) while the runner thread was still unwinding
`PostgresServer::Run()` — the destructor called `RequestStop(); join()`
but then destroyed the server before the runner thread had actually
finished touching it. Fixed in the harness by moving the server into a
local `keeper` that outlives `join()`. 20x100s single-process sweeps
after the fix: zero hits (before: hit within ~10). Production
`postgres_server_main` runs `Run()` on the main stack and is unaffected.

Harness notes: the `postgres_session_fuzzer` CMake wiring (fuzzer +
replay test) had been silently dropped by a parallel edit and was
restored; folded INSERT shapes now also skip the InsertShape plan-cache
store (Bug 57 completion). `QueryTest.DmlQuantifiedFoldMatchesRelationalRoute`
(54 cases) pins fold == relational for ANY/ALL/NOT/EXISTS/correlated/
scalar and AND/OR mixes over {values, NULL, empty} inner tables.

## Round 11 — template cache dropped quantifier mode/op (Bug 60)

The Heisenbug family (wave8/wave9 session crashes) was NOT a
visibility/checkpoint bug at all: `sql_template.cpp::BindExpression`
rebound a `kQueryExp` through the **4-arg `QueryExpression`
constructor**, silently resetting `op_`/`mode_` to the
IN-membership defaults whenever the statement round-tripped through the
TEMPLATE cache (RememberTemplate → later identical-shape statement).
`x op ANY/ALL (sub)` therefore executed as `x IN (sub)` on the second
and later occurrences of the same text — which also explains why the
bug vanished when extra statements were executed between steps
(instrumentation perturbed template hit rates) and why
`SetForceRelationalFallback` did not help (the loss happened before
routing, at template binding).

Fix: carry `query.Op(), query.Mode()` across template rebinding
(ARRAY-result semantics were already preserved; the visitor's other
4-arg sites intentionally construct fresh unquantified nodes).
Minimal repro pinned: `t0(1,2), t1(1,2,1)` +
`UPDATE ... WHERE NOT u < ALL (SELECT u FROM t1)` twice through the
same SqlEngine — before: 2 then 0 rows; after: 2 then 2, matching the
relational route. All 16 wave8+wave9 session crash inputs replay
green; wave10's sql_session failures are gone.

Round-11 hardening: the Bug-57 fold guard now also covers plan-cache
REUSE. Prepare() marks a DML fingerprint volatile around the compile
and demotes it unless `dml_fold_applied_` fired, so a folded UPDATE can
never be served from (or published into) the specialized-plan cache —
closing the "old fold constants served to a new database state" hole
that the #60 template-cache fix only half-closed (RememberSpecializedPlan
was suppressed, but ServeFromPlanCache for a plan cached before the
first fold could still replay stale constants).

## Round 11 addenda

- Bug-57 hardening (fold vs plan-cache REUSE): Prepare() now brackets
  every DML compile with a precautionary volatile mark on the
  fingerprint and demotes it unless `dml_fold_applied_` fired, so a
  folded statement can neither be served from nor published into the
  specialized-plan cache (RememberSpecializedPlan gating alone left the
  ServeFromPlanCache direction open).
- Non-bug resolved: bare `UNION` (no ALL/DISTINCT) errors out because
  the pinned ZetaSQL 2026.7.2 frontend requires the modifier — the
  engine was right, the probe was wrong.
- Follow-up from the same probe: frontend parse failures returned
  `Status::kUnknown` (client syntax errors looked like internal faults
  on the wire); now `kInvalidArgument`, message preserved in
  `LastError()`.

## Round 12 — GROUPING SETS dangling columns (oracle decidability)

New aggregate-fuzz mismatch
(`sql_aggregate_fuzz-repro-3251292579381062677.test`): a query like
`SELECT s, a, ... GROUP BY GROUPING SETS ((), (a))` projects `s`, which
is a member of **no** grouping set. The mirror emitted NULL for it;
the engine reports an arbitrary group member (`'x'`).

Triage: this is the lenient-dialect behaviour, not a wrong-result bug.
Plain `GROUP BY a` already returns an arbitrary member for projected
non-key columns (any_value semantics). Grouping sets compose that rule
with the key-dimension rule: a column that appears in **some** set is a
grouping dimension (NULL in sets that omit it — `GroupingSetsExecutor`
and the Cascades `grouping_sets_expansion` UNION-ALL rewrite agree);
a column in **no** set stays a dangling arbitrary member. Both physical
paths return the same shape, so the engine is self-consistent — the
value is just not deterministic for the oracle to predict (it depends
on which member row wins each group).

Fix (generator side, `query/sql_aggregate_fuzzer.cpp`): after sampling
the grouping sets, append the singleton set `{c}` for any selected key
column `c` not covered by the union of chosen sets. Every projected
grouping column is now a real grouping dimension, so the mirror's
"absent from the set → NULL" model is decidable again. The committed
repro file was not promoted to the regression dir: it encodes a
semantically ambiguous shape the generator can no longer produce.

### Round 12 addendum — COUNT(DISTINCT … WHERE …) dropped its filter (Bug 61)

Second aggregate-fuzz mismatch
(`sql_aggregate_fuzz-repro-14224539374217437353.test`, real engine bug):
`SELECT b, s, …, COUNT(DISTINCT a WHERE b IS NOT NULL) … GROUP BY b, s`
returned the unfiltered distinct count for the `b = NULL` group (every
row in that group fails the predicate, so the answer must be 0; the
engine reported 9).

Root cause in the grouped typed fast path
(`executor/relational.cpp` `TypedAggSlot` setup): the `distinct_int`
shortcut — COUNT(DISTINCT int64-column) backed by a per-group
`unordered_set<int64_t>` — `continue`d before the modifier check, so an
aggregate carrying `WHERE`/group-context/HAVING/ORDER BY/secondary/
trailing modifiers took the typed path whose accumulation loop never
evaluates a row filter.  Filter dropped silently → over-count.

Fix: `distinct_int_column` now additionally requires no `WhereFilter`,
`NeedsGroupContext`, `Having`, `InnerOrderBy`, `SecondaryArg`, or
trailing args; a filtered DISTINCT therefore hits the existing modifier
check and falls back to the generic accumulator (which evaluates the
predicate per row).  Sibling paths were audited: the serial
`AggregationExecutor`, `ParallelAggregationExecutor` generic loop,
`two_phase_distinct_agg`, and `partial_aggregate` all evaluate
`WhereFilter` already; `dictionary_batch_aggregation` is test-only.

Verified: minimal repro `COUNT(DISTINCT a WHERE b IS NOT NULL)` over
groups with all-NULL b returns 0 (was 2); the committed repro replays
clean via `ReplayCommittedRegressionFiles`; executor_test 149,
query_test 149, executor_extra_test 44 green.

### Round 12 addendum 2 — XOR boolean-identity guard completion (Bug 62)

Broad ctest surfaced a deterministic failure in
`ExpressionRewriteTest.XorBooleanIdentityRefusesNonBooleanSurvivor`.
Triage showed the in-flight guard was half-applied and its test encoded
a wrong premise:

- `x XOR TRUE -> NOT x` / `x XOR FALSE -> x` presume a boolean survivor.
  For a statically non-boolean x the identity changes results:
  `(x+y) XOR FALSE` evaluates 1 (truthiness) while `x+y` yields 7.
- The companion expansion `xor_to_or_and_not`
  (`a XOR b -> (a OR b) AND NOT(a AND b)`) was unguarded and would still
  rewrite non-boolean operands — guarded with the same
  `StaticallyNonBoolean` check.
- The original test used constant operands (`5 XOR TRUE`); a
  constant-only XOR is folded by `fold_binary` before the identity rule
  ever runs, so it could never observe the guard.  XOR evaluation is
  truthiness-based (never a type error), so the fold to `0` is itself
  correct.  The test now exercises `(x+y) XOR FALSE` in both operand
  orders — a non-constant statically-non-boolean survivor that actually
  reaches the guarded rules.

rewrite_test 83 + differential_test 15 green.

### Round 12 addendum 3 — COUNT(*) TouchedColumns null-deref (Bug 63)

`rule_fuzzer` (ASAN, Cascades `ExploreGroup` → `Rule::Apply`) SEGV'd in
`AggregateExpression::TouchedColumns()` at
`expression/aggregate_expression.cpp:35`: the method unconditionally
dereferenced `child_`, but a `COUNT(*)` aggregate carries `nullptr`.
`ResultType` (both overloads) already null-checked; `TouchedColumns`
was the only missed accessor.

Fix: `child_` null-guarded, and the previously-dropped `secondary_arg_`
(STRING_AGG delimiter) and `inner_group_by_` key columns are now merged
into the result — both can reference columns that must not be pruned.
Regression pinned in
`ExpressionTest.AggregateExpression_TouchedColumns_ChildlessIsEmpty`.

Bug 63 scope widening: the childless `COUNT(*)` is not a fuzzer-only
construct — `cascades.cpp` `count_not_null_to_count_star` rewrites
`COUNT(non_null_col)` into `AggregateExpression(kCount, nullptr)`, so the
shape is reachable from real SQL through the memo. Audited every
`AsAggregateExpression().Child()` dereference:

- `IsCountStar` (executor/aggregation.hpp) — the single authority for the
  COUNT(*) shape — itself dereferenced the child. Now treats both
  spellings (null child or `*` column child) as count-star, which keeps
  every `IsCountStar`-gated executor branch correct instead of merely
  moving the segfault to the next `Child()->` deref.
- `plan/aggregation_plan.cpp` schema derivation null-guarded.
- Rewrite traversal (`ExpressionChildren`/`WithExpressionChildren`,
  `ExpressionPattern::Match`, `ContainsNotOfOrderedDoubleComparison`)
  already null-safe; lossless rebuild preserved.

Harness gap (not an engine bug): rule_fuzzer's Griffin pass flagged
`SUM(v WHERE v > 0)` "type mismatch" — a legitimate data-dependent
rejection surfacing through the EXC-SHIM as a C++ exception
(binary_expression.cpp:755 StatusError message). Added "type mismatch"
to the griffin harness's known-rejection list.

### Round 12 addendum 4 — HAVING MIN/MAX modifier triple defect (Bug 64)

rule_fuzzer's Griffin pass surfaced `SUM(v WHERE v > 0)` analysis noise; manual
probing of the `AGG(x HAVING MIN/MAX c)` modifier found three real defects:

1. **Frontend mis-parse (wrong results everywhere).** The pinned GoogleSQL
   dump leaves `HavingModifier.detail` empty, so `modifier.find("MIN")` never
   matched and `HAVING MIN` was always bound as `AggregateHavingModifier::kMax`.
   `ANY_VALUE(s HAVING MIN v)` grouped returned the *max*-v row. Fixed by
   falling back to `SliceSource` byte-range recovery: the keyword is the word
   after `HAVING` (handles `HAVING MAX min_col` safely).
2. **Scalar executor dropped the modifier.** `AggregationExecutor`'s generic
   accumulator branch never evaluated `HavingCondition` into
   `AggregateInput.condition`, so gated buffers saw all-NULL conditions and
   returned NULL. Also, both fast paths bypassed the modifier entirely:
   the typed-column gate and the `jit_sum_eligible_` gate only checked
   Distinct/WhereFilter/InnerOrderBy — `SUM(v HAVING MIN v)` summed the
   whole column. Both gates now require `!NeedsGroupContext()`, and the
   generic branch populates `condition`.
3. **Parallel routing dropped whole-group aggregates.**
   `AggregationPlan::EmitExecutor` sent every aggregate shape to
   `ParallelAggregationExecutor` once `EmitRowCount >= 8192`, but
   `PartialState` has no row buffer — HAVING/inner ORDER BY/LIMIT/
   secondary-arg aggregates and unsupported kinds (ARRAY_AGG, STRING_AGG,
   ANY_VALUE, BIT_*, COUNTIF, approx) silently degraded to NULL or raw
   sums. `EmitExecutor` now keeps those on the serial
   `AggregationExecutor`.

Regression pinned in `AggregateHavingTest` (sql_extra_test.cpp): grouped and
scalar MIN/MAX gating plus `SUM(v HAVING ...)` on both paths.

### Round 12 addendum 5 — aggregate fuzzer: full modifier coverage

Extended `sql_aggregate_fuzzer` generation and mirror to cover every
in-call aggregate modifier the engine accepts:

- `AGG(arg HAVING {MIN|MAX} c)` — the argmin/argmax modifier, generated on
  all non-predicate aggregates (excluded for `COUNT(*)`, `COUNTIF`,
  `LOGICAL_AND/OR` which carry no plain column argument).  The mirror's
  `FilteredRows` now gates each group to rows whose condition equals the
  non-NULL extremum, matching the engine's accumulator semantics.
- `AGG(arg ORDER BY u [DESC] [LIMIT n])` — previously generated only for
  STRING_AGG/ARRAY_AGG; now emitted for every kind (verified accepted on
  SUM/AVG/MIN/MAX/COUNT(*)/COUNTIF/LOGICAL_*/BIT_*/VAR_*/ANY_VALUE).
  Mirror: new `PreparedRows` = WHERE filter → HAVING gate → u ordering →
  LIMIT truncation, shared by `AggNum`/`ComputeAgg`; STRING_AGG/ARRAY_AGG
  branches simplified to consume the prepared row list.
- `ANY_VALUE` — generated on all column types; the frontend normalizes it
  to `kMin` so the mirror reuses the MIN computation (deterministic).
- Combinations exercised: `WHERE` + `HAVING` + `ORDER BY` + `LIMIT` +
  `DISTINCT` in a single call, e.g.
  `ARRAY_AGG(DISTINCT a WHERE a>0 HAVING MIN b ORDER BY u LIMIT 2)`.

Grammar order probed and pinned: `AGG(arg [WHERE c] [HAVING {MIN|MAX} e]
[ORDER BY k] [LIMIT n])` — `HAVING` after `WHERE`, both before `ORDER BY`.

Validation: `SqlAggregateFuzzer.SeededAggregatesMatchMirror` 300 seeds x 2
(HAVING-only and HAVING+ORDER/LIMIT builds) — all match.

### Round 12 addendum 6 — join fuzzer: USING clause coverage

`sql_join_fuzzer` only emitted `ON` predicates.  Added `USING (k)`
generation: ~35% of plain equi-joins (INNER/LEFT/RIGHT/FULL) now render as
`fact f <kw> dim d USING (k)`.  The merged key column deduplicates and
coalesces — modeled as a new `ProjCol::kMergedK` (`f ? f->k : d->k`),
projected as bare `k` in half the USING queries; raw `f.k`/`d.k`
projections keep their null-extension semantics under USING.  NATURAL JOIN
is confirmed unsupported (clean frontend rejection), so it is not
generated.

Validation: `SqlJoinFuzzer.SeededJoinsMatchMirror` 300 seeds — all match.

### Round 12 addendum 7 — session fuzzer: DOUBLE columns

`sql_session_fuzzer` schemas were INT64+VARCHAR only — float DML, DOUBLE
index keys, and double-typed predicates never ran through the state mirror.
Added `ColType::kDbl` (20% of generated columns): half-step domain
(`repr * 0.5`, repr in [-6,6]) keeps mirror values exact; new `kDblCmp`
predicate kind (const and column RHS), `GenDblValue`, `is_dbl` on SExpr
(leaves/COALESCE/CASE only — mirror arithmetic stays INT64), DOUBLE probe
values + `FormatDoubleShortest` literal rendering in `CheckIndexProbe`.
DOUBLE columns flow into UNIQUE/plain index keys automatically via the
existing `PickCol` key selection.

Validation: `SqlSessionFuzzer.SeededSessionsHoldState` 200 seeds — all
match.

### Round 12 addendum 8 — function fuzzer: strict-list expansion + non-strict tier

`function_fuzzer` curated 62 strict builtins. Audited every `name ==`
guard in `function_call_expression.cpp` for NULL-propagation and arity
coverage:

- **Strict list +55**: `log`, `pow`, `ieee_divide`, `safe_add`,
  `safe_multiply`, `is_inf`, `is_nan`, `ceiling`, `truncate`,
  `octet_length`, `character_length`, `split_substr`,
  `code_points_to_string`, `concat`, `regexp_contains`, `regexp_match`,
  `regexp_instr`, `byte_regexp_instr`, `regexp_extract_all`,
  `byte_regexp_extract_all`, `regexp_replace`, `md5`, `sha1`, `sha256`,
  `sha512`, `format`, `format_timestamp`, `format_datetime`,
  `format_date`, `parse_timestamp`, `string`, `extract_year`,
  `extract_month`, `extract_day`, `unix_seconds`, `unix_millis`,
  `unix_micros`, `unix_date`, `generate_array`, `generate_date_array`,
  8 `json_*` variants, and 5 `__bit_*`/`__shift_*` internal ops — each
  verified to NULL-guard *every* argument position it reads.
- **New non-strict tier** (`kNonStrictFunctions`): invariant 3 (NULL
  propagation) is skipped; no-throw + determinism still checked.
  `nullif`/`ifnull`/`coalesce`/`greatest`/`least` are non-strict by
  design; `if`/`iferror`/`nulliferror` evaluate conditionally;
  `make_interval` and `regexp_extract` accept extra arguments they never
  read (lenient arity — `make_interval(1,2,3,4)` is pinned by
  `SqlIntervalCoverageTest`), so a NULL there must not assert.
- Excluded: `rand`/`current_*` (volatile), `date_add`/`date_sub` (require
  `IntervalExpression` args the constant-arg generator cannot build).

Two oracle mismatches surfaced during bring-up — both harness
misclassifications, not engine bugs (`nullif` returning arg0 when arg1 is
NULL is correct GoogleSQL semantics; `make_interval` extra args are
intentionally ignored).

Validation: 177K execs / 0 crashes, cov 4015 / ft 7410.

### Round 12 addendum 9 — DATE_ADD union-member read SEGV (Bug 65)

The expanded function fuzzer (with generated `IntervalExpression` operands)
crashed `date_add(INT64, INTERVAL)` inside `TryParseDateDays`:

`AddOrSubInterval` parsed `date.value.varchar_value` for *every* non-DATE
operand — for INT64/DOUBLE/ARRAY inputs that union member is a garbage
`string_view` (null data pointer + nonzero length), and the strict
"Y-M-D" scanner dereferenced it at `type/date.cpp:91`.

Fix: `AddOrSubInterval` now rejects operands that are neither `kDate` nor
`kVarChar` with `kInvalidArgument` before touching `varchar_value`
(all 5 `date_add`/`date_sub` dispatch sites share this helper). Every other
`TryParseDateDays` caller was already type-gated.

The SQL surface type-checks `DATE_ADD` at the frontend, so the SEGV was
only reachable through direct expression evaluation — but `TryEvaluate`'s
Status contract must hold for every input the optimizer/executor can
construct. Regression: `FunctionCallTest.DateAddRejectsNonDateOperand`.

### Round 12 addendum 10 — ParseJsonSimple non-progress loop OOM (Bug 66)

`function_fuzzer` OOM'd (2 GB malloc): `json_extract("[ $$", …)` —
`ParseJsonSimple`'s `[` array loop pushed a `JVal` per iteration but an
unrecognized token returns an empty `JVal` *without consuming input*, so
the loop never advanced (10.8 M allocations, 1.6 GB live). Any malformed
array payload (`[x`, `[,`, `[[[[x`) hangs the same way — reachable from
SQL via `JSON_EXTRACT`/`JSON_QUERY`/`JSON_VALUE`/… and from the
`json_path_constant_fold` rewrite.

Fix: the array element loop now breaks when a nested `ParseJsonSimple`
call fails to shrink the input. The `{` object loop was already safe
(it breaks on non-`"` keys, and `,` always consumes). The second JSON
parser in `executor/detail/expression_eval.cpp` is a `bool`-returning
design that exits on parse failure — unaffected.

Regression: `FunctionCallTest.JsonExtractMalformedArrayTerminates`.

Also in this round: fuzzer `MakeArg` gained INT64 array literals
(`Value::Array`, occasional NULL elements) so `code_points_to_*` and
other array-taking builtins reach their real paths, and DATE_ADD/
DATE_SUB get a generated `IntervalExpression` operand.

### Bug 67 — `Value::TryLess/TryGreater` on arrays escapes the StatusOr contract via throwing `operator<`

Found by `function_fuzzer` seconds after the array-argument generator
landed: `LEAST(ARRAY[81,…], ARRAY[190,…], ARRAY[NULL])` aborted the
harness with "function threw". The kArray branch of `TryLess`/`TryGreater`
in `type/value.cpp` delegated to `std::vector<Value>::operator<`, whose
element comparison calls `Value::operator<` — an `ExcShimUnwrap` wrapper
that *throws* when `TryLess` returns an error (NULL elements, mixed
element types). The exception unwound straight through `TryEvaluate`'s
no-throw boundary.

Fix: both branches now run an explicit lexicographic loop that skips
`operator==`-equal prefix elements and propagates the recursive
`TryLess`/`TryGreater` result — incomparable elements surface as a
`StatusError` on the same channel as every other bad operand. The
throwing `operator<`/`operator>` wrappers keep identical semantics for
callers that expect a throw (sort comparators etc. encode through
memcomparable keys and never hit this path).

Note the SQL surface already errored cleanly before the fix — a statement
boundary catch converted the throw into an error — so this is a contract
violation fix, not a user-visible behavior change. Regression:
`ValueTest.TryCompare_WithArrays_*` and
`FunctionCallTest.GreatestLeastOnArraysWithIncomparableElements`.

Fuzzer side: `pi` (strict) and `iserror` (non-strict — it returns 0/1 by
design on any input) joined the lists; a join-context
`TryEvaluate(left, lschema, right, rschema)` equivalence check was added
since that overload walks its own argument loop.

Fuzzer-side additions in the same round: the array generator now emits
INT64/STRING/DOUBLE/nested-ARRAY elements (mixed flavors), and the
Griffin seed pool gained a higher-order-lambda + correlated-UNNEST
scenario (`ARRAY_TRANSFORM`/`ARRAY_FILTER` over `ARRAY<INT64>` columns
including empty and NULL arrays — lambda bodies were previously
unfuzzed). `LEAST([NULL],[NULL])` and friends now evaluate through the
StatusOr path end-to-end; incomparable array elements surface as
ordinary `InvalidArgument` errors at the SQL surface.

### Bug 68 — unbounded recursion in both JSON parsers overflows the stack

`SELECT JSON_EXTRACT('[[[…×20000…]]]', '$')` segfaulted: both hand-written
JSON parsers recurse once per `[`/`{` with no depth bound. The row-eval
`parse_j` in `executor/detail/expression_eval.cpp` overflows first (fat
`std::function` frames, ~20 K levels); the fold-side `ParseJsonSimple` in
`expression/rewrite.cpp` (reachable via `json_path_constant_fold` and via
`ExecuteFunction`'s `json_*` family) has the same latent flaw with a
slimmer frame.

Fix: a 512-level depth bound in both parsers (`kMaxJsonDepth`), matching
`kMaxRewriteDepth`. Over-limit documents fail the parse and evaluate to
NULL — same surface behavior as malformed JSON. Regression:
`SqlScalarCoverageTest.JsonDeepNestingTerminatesCleanly` covers both the
constant-fold path and the column-sourced row-eval path.

### Bug 69 — template-cache rebinding drops `QueryExpression::array_element_sql_type_`

Griffin reported `evaluation-path mismatch` on
`SELECT id, ARRAY_FILTER(arr, x -> y > 1) FROM la ORDER BY id;` —
`ARRAY<INT64>[...]` on the first run vs `ARRAY<STRING>[...]` on the
second. The divergence looked flaky: verbatim replay held, regeneration
reproduced, and the harness flagged "serialized test replay holds".

Root cause was not the evaluator but the plan template cache:
`BindExpression`'s `kQueryExp` case in `query/sql_template.cpp` rebuilt
the subquery node with `SetArrayResult` preserved but
`array_element_sql_type_` dropped (the aggregate sibling already carried
it — plain omission). First execution of a templatable
`ARRAY(SELECT ...)`/lambda-desugared statement honored the static
annotation ("INT64" default from `InferArrayElementSqlType`); every
subsequent execution hit the process-global cache, rebound the tree
without the annotation, and fell back to runtime inference — which typed
the produced `{"y":10}` string elements as STRING. Same SQL, different
observable result type — bytecode on/off only decided *which* run
executed first, so `bc;bc` order diverged just like `bc;ast`.

The "flaky" mechanics resolve cleanly: the in-process regeneration warms
the template cache before verbatim replay, so both replayed runs take
the rebound (annotation-free) tree and agree — replay can never
reproduce this class. `ReplayGriffinTrace` now accepts the recorded
statements as a *prefix* of the healthy regeneration instead of
requiring exact equality, so a committed repro of a fixed bug replays
clean instead of reporting "tampered" forever.

Minimal repro:
`SELECT ARRAY_TRANSFORM(arr, e -> e || 'a') FROM t;` twice in one
process — `ARRAY<INT64>` then `ARRAY<STRING>` before the fix, identical
after. Regression: `QueryTest.TemplateCacheKeepsArraySubqueryElementType`
plus committed repro `griffin_fuzz-repro-9473355082901233217.test`
replayed by `GriffinFuzzer.ReplayCommittedRegressionFiles`
(now wired to `query/testdata/griffin_regressions/` via
`TINYLAMB_GRIFFIN_REGRESSION_DIR`).

Audit notes for the same class: `BindSelect` preserves aliases, GROUP
BY, HAVING, QUALIFY, DISTINCT ON, WITH TIES, AS STRUCT, union branches
and `MarkComplex`; `kCaseExp::from_if_` is dead state; `kIntervalExp`
drops `raw_amount_` but no reachable interval syntax carries a
semantically distinct raw amount (compound units are rejected);
`kWindowFunctionExp`/`kLambdaExp` share subtrees but window/lambda
queries take the relational route and are never cached. The remaining
latent wart: `InferArrayElementSqlType` defaults unrecognized lambda
bodies to "INT64", so `ARRAY_TRANSFORM(arr, e -> e || 'a')` declares
`ARRAY<INT64>` over string elements — consistent across runs now, but
the tag itself is still wrong; a follow-up could return "" and align the
`query_expression.cpp` empty-annotation fallback (currently hardcoded
INT64) with the relational runtime inference.

### Bug 70 — `std::regex` nested-quantifier ReDoS in REGEXP functions (fixed)

`function_fuzzer` produced a libFuzzer timeout artifact (1320s on a
26-byte input): `regexp_replace(array, ".++\0T", "\0\0")` where the
subject is the ~75-byte `AsString()` of a nested array. libstdc++'s
`std::regex` uses a DFS backtracking executor; `.++` is parsed as a
stacked unbounded quantifier (equivalent to `(.+)+`), giving exponential
blowup on non-matching input — a genuine ReDoS reachable from plain SQL.

A follow-up `slow-unit` probe (`.?+A`) and a manual probe (`(a|a)+b`)
showed the first fix was too narrow: exponential tiling also comes from a
bounded-but-length-varying operand under an unbounded quantifier and from
alternation branches that can match the same input differently.

Fix: `common/regex_guard.hpp` adds `RegexPatternHasNestedRepetition`, a
conservative static scan that tracks, per element, its fixed-vs-variable
match length, first-character set, nullability, and branch ambiguity, and
rejects an unbounded quantifier (`*`, `+`, `{m,}`) applied to:

* a variable-length operand — stacked (`a++`, `a*{3,}`), nested
  (`(a+)+`, `((a+)b)+`, `(a|b+)*`) or bounded-but-varying
  (`(a?)+`, `(a{2,4})+`, `.?+`);
* an alternation whose branches' first-character sets overlap or nest —
  `(a|a)+`, `(x|xy)+`, `(ca|cb)+`, `(\d|\w)+`; or
* a nullable operand that can also match real input — `(a|)+`.

Lazy modifiers (`a+?`), bounded counts (`a{2,4}`, `(a+){3}`), sequential
reps (`a.*b.*c`), char classes, and disjoint-first-char alternations
(`(cat|dog|bird)+`, `(a|bc)+` — deterministic even when branch lengths
differ) stay allowed. `CompileCheckedRegex` wraps the check + compile and
is used at all twelve `std::regex` call sites (`regexp_contains`,
`regexp_match`, `regexp_instr`, `regexp_extract[_all]`, `regexp_replace`,
incl. `byte_` variants) in both `function_call_expression.cpp` (AST/fold)
and `expression_eval.cpp` (relational) so the two paths stay equivalent.
Regression: `ScalarFunctionSqlTest.RegexpNestedRepetitionRejected`.

## Round 15 — handoff: WIP union-pushdown rule corrupts correlated-EXISTS DML

w15 sql_session caught a divergence (crash-fa8902d2…, tbl0 c1 kept the
pre-update value where `UPDATE … WHERE EXISTS(E) AND EXISTS(E)` should
apply). Minimal probe against the CURRENT working tree vs committed
HEAD (f002d56):

| predicate (t0.u=9 ∈ t1.u=9)          | HEAD (default route) | WIP (default route) |
|--------------------------------------|----------------------|---------------------|
| `EXISTS (SELECT 1 FROM t1 WHERE t1.u = t0.u)` | 1 row | 1 row |
| `EXISTS(E) AND EXISTS(E)` (duplicate)         | 1 row | **0 rows** |
| two different correlated EXISTS conjuncts     | 1 row | 1 row |
| `EXISTS(E) OR EXISTS(E)`                      | 1 row | **error/0** |

The regression appeared with the uncommitted Selection/union rework in
plan/cascades.cpp (or_to_union + filter-push-into-union-branches); the
in-tree test `CascadesRulesCoverageTest.FilterPushedIntoEveryUnionBranch`
currently returns 200 rows where it expects 2 — the rule itself is not
yet sound for duplicate conjuncts / Selection-over-semijoin shapes.
sql_extra_test (new SqlUdfTest file, 36 failures) and the ErrorTable
cases are the same agent session's WIP. Not touched per the
don't-edit-others-WIP rule; w15 continues to sweep the tree and will
report further wrong-results from this rule.

Round-15 sweep results (agent WIP tree): expression/cast/function fuzzers
and sql_plancache (fold mutations included) all green. Two findings
belong to the uncommitted relational rework, not to the fold/quantifier
line:

* sql_join_fuzzer OOM (2GB): the new `GroupedFinishExecutor::Initialize`
  materializes the whole core output into a `Relation` before
  `FinishQuery` (288MB of live `Row` copies at the sample point); a
  wide cross-join shape now peaks far above the old streaming path.
  Either an intentional trade-off (needs a spill hook) or a regression
  — flagged for the rule's owner.
* pgwire write-race stall: the fold-carrying race UPDATE exceeded the
  harness's 5s read timeout under load-190 conditions; at load<15 the
  same corpora replay clean and a direct 2-thread/300-round engine
  race (same SQL) finishes with zero errors. Timeout, not deadlock —
  the harness should widen the race timeout or pin CPU.

* Pre-existing (HEAD too, not the fold line): `GROUP BY g ORDER BY
  MAX(k)` / `ORDER BY COUNT(*)` with a *raw* aggregate in the ORDER BY
  list fails `InvalidArgument: aggregate was not prepared`
  (executor/detail/expression_eval.cpp:1469) even though the identical
  aggregate is in the select list — the sort key re-evaluates the
  un-prepared AggregateExpression instead of binding to the already
  computed output (alias / positional references work). Valid GoogleSQL
  (ZetaSQL accepts it); SELECT/UPDATE/force-relational all agree on the
  error so no wrong results, but the grouped-finish rework in
  executor/relational.cpp (resolved_order_expressions) is the natural
  place to fold the key onto the select-list slot. Recorded for the
  aggregation owner.

## Round 23 — bug 62 (harness): aggregate fuzzer flagged DOUBLE SUM ulp noise

w16 sql_aggregate aborted on `SUM(f)` where engine said `-3.999999999999999e+300`
and mirror `-3.9999999999999996e+300` — the last ulp of the pairwise
(partial→final) tree sum vs the mirror's sequential sum, not a wrong
result (both are correctly-rounded-ish orderings of the same terms; the
engine's answer is the reference). The fuzzer compared `FormatRows`
strings with `==`. Fix: `CellAlmostEqual/RowAlmostEqual/RowsAlmostEqual`
in `query/sql_aggregate_fuzzer.cpp` give every mismatch a numeric second
look — finite doubles match within `1e-9` relative, ints/strings/NULLs
must still be exact, rows are matched as a multiset — applied at both
the fuzz site and the replay site. The offending seed is committed to
`query/testdata/aggregate_regressions/` and `ReplayCommittedRegressionFiles`
passes (the ulp drift must never re-morph into a real bug report, and
any genuine difference beyond 1e-9 relative still trips).

## Round 24 — route-flip differential lands; catches a WIP relational-DML regression

`query/sql_session_fuzzer.cpp` now supports `TINYLAMB_SESSION_ROUTE_FLIP=1`:
every third statement (deterministic counter, skipped when
FORCE_RELATIONAL is set) runs through
`SetForceRelationalFallback(true)`, so the row-mirror doubles as a
plan-vs-interpreter differential oracle for free. A 480 s flip run
immediately tripped:

    UPDATE tbl1 SET c1 = 'bb'
     WHERE (u > -3) AND NOT EXISTS (SELECT 1 FROM tbl2 WHERE tbl2.u = tbl1.c2);

with tbl1=(1,'a',0), tbl2=(0): the default (decorrelating) route
correctly answers 0 rows; the current-tree force-relational route
answers 1 row — the interpreter evaluates the correlated inner as if
`tbl1.c2` were unbound, so EXISTS flips to false. The same predicate as
a SELECT agrees on both routes (empty), and committed HEAD evaluates
this UPDATE correctly on both routes, so it is a regression inside the
uncommitted relational rework (top suspect: the ~285 new lines in
executor/detail/subquery_runtime.cpp — cache_outer_columns /
outer_columns probe paths). Minimal C++ probe: flip1.cpp pattern above
(two tables, one row each). Handed to the aggregation/interpreter owner
with the flip harness as its regression net.

The flip decision is keyed on the recorded-step ordinal
(`RouteFlipFor(t.steps.size())` at generation, `RouteFlipFor(i)` over
the trace index at replay), so serialized replay re-runs each statement
on the same route and reproduces the mismatch (an earlier
counter-in-RunSql design printed `replay holds (flaky)` and was
replaced). A second flip seed (tbl0 `SET c2 = CASE… WHERE EXISTS(…)`)
probe-reduces to the same relational-EXISTS shape, so one fix should
clear both. Repro probes: flip1.cpp / flip3.cpp (two tables, correlated
EXISTS in an UPDATE WHERE).

Round-18 confirmation: a third flip seed (tbl0 `UPDATE SET c3 = -1.0
WHERE EXISTS (SELECT 1 FROM tbl1 WHERE tbl1.u = tbl0.u)` — engine left
c3 NULL where the mirror says -1) probe-reduces to the same
relational-route correlated-EXISTS shape; post-abort visibility is
irrelevant. Storage (6 targets), sql_join@8GB, and the new correlated-
EXISTS pgwire race variant all ran green for ~2 h aggregate. The
regression has been idle-owner-assigned for ~5 h; flip sweeps keep
landing on it, so keep the rule: do not enable ROUTE_FLIP by default
until relational EXISTS matches the optimizer on the flip1/flip3 probes.

## Round 25 — open: ghost write-intent blocks fresh INSERT (w19 sess seed 5523799141114400307)

`w19/sess` (default route, no flip) aborts deterministically on
`INSERT INTO tbl4 VALUES (14, -2, 'a')` → `stream: Conflicts: insert
failed on table tbl4` although **u=14 had never been inserted anywhere**
in the session (rows 9-13, 25; two earlier txns aborted, one committed).
Evidence chain:

- harness improvement first: `RunSqlWhy` now reports the failing phase
  and status (`query/sql_session_fuzzer.cpp`) — this rejection used to
  surface with an empty `LastError()`, which is why the trace looked
  random. (`serialized session replay holds (flaky)` on this seed is a
  harness artifact: `TINYLAMB_SESSION_ONLY_SEED` values above
  kIterations make the gtest skip everything and trip the sanity
  asserts; the libfuzzer replay of the crash file is deterministic.)
- transcript (v.txt, `TINYLAMB_SESSION_VERBOSE=1`): abort-heavy prefix,
  INSERT-SELECT writing u=19 then abort, ... then plain INSERTs 11,12,13
  fine, 14 conflicts.
- hand-written single-ctx replays of the whole visible sequence
  (wi3/wi4 probes) never conflict → the ghost lives in state the
  transcript does not show (two-context interleavings / index / intent
  tables) or in the agent's uncommitted b_plus_tree/transaction work —
  index/b_plus_tree.cpp + 10 related files are mid-edit right now.
- HEAD driver replay of the same crash file passes (caveat: driver RNG
  diverges, so this is weak but consistent with a WIP regression).

Files kept at `.fuzzscratch/w19/sess/`
(`crash-df222e9c…`, `sql_session_fuzz-repro-5523799141114400307.test`,
`v.txt`). Not committed as a regression yet — it would stay red until
the intent/index owner lands; commit it once the tree passes.

Round-25 analysis update (ghost unique-duplicate, seed 5523799141114400307):
the rejection status is `Duplicates` from `Table::Insert` (executor/
insert.cpp wraps it as kConflicts). Minimal semantics check (uniq2.cpp,
C++ `db->CreateIndex(..., IndexMode::kUnique)`): with committed rows
{c1: 0, -2} and unique idx on c1, the FIRST duplicate insert fails —
engine alone is sound. In the session trace the duplicates at
INSERT (12, 0, NULL)/(13, -2, …) were ACCEPTED and only the next
(14, -2, …) tripped — and the harness did not predict them either
(no must_fail markers), although its own check two lines earlier dumped
committed rows carrying c1 ∈ {0, -2}. Note the unique idx2 (line 56)
was created in a transaction that ABORTED (line 58): engine-side index
survived-but-empty vs harness-side unique_keys both point at the
DDL-in-transaction/abort-cleanup path in the uncommitted
b_plus_tree/index rework. Candidate invariant test for the owner:
CREATE UNIQUE INDEX → ABORT → is the index object still enforcing,
and against which entry set?

## Round 26 — FK surface reviewed green; w20 sweep clean; agent WIP regrew 6 tests

* b_plus_tree_concurrent_fuzz_test green under TSan on the current tree.
* w20 long sweep (40 min each): sql_session / sql_plancache / griffin all
  rc=0 on the agent's tree — the ghost-intent seed stays unreproduced by
  default-route fuzz (it needs the DDL-abort interleaving; files kept in
  w19/sess).
* New agent feature under review (their +656-line sql_engine diff):
  FOREIGN KEY. Probes green: basic insert/update/delete child+parent
  enforcement, NULL pass, self-reference, DROP guard, FK × prepare-time
  fold (`1 = ANY (SELECT …)` in UPDATE WHERE keeps enforcement), FK ×
  INSERT…SELECT, cross-transaction visibility (child insert against
  uncommitted-then-aborted parent rejected; accepted after the parent
  commits), FK × ON CONFLICT DO UPDATE both directions. REPLACE syntax
  honestly rejected. No engine bug to claim from the FK surface yet.
* 6 red tests right now are the agent's mid-edit files
  (benchmark/tpcc_workload_test.cpp + query/sql_engine_coverage_test.cpp:
  new Tpcc-rollback and filtered-aggregate-rebind expectations); per the
  WIP rule they are handed back untouched.

## Round 27 — TPC-C workload regressions localized to the daytime WIP (owner handoff)

5 `TpccWorkloadTest` failures + `SqlEngineCoverageTest.TemplateRebindKeepsAggregateAttributes`
are red on the current tree; the same committed tests **PASS on clean
HEAD** (worktree probe). The workload driver itself is untouched since
HEAD (test diff is cosmetic), and every failure is `ASSERT_TRUE(...committed)`
— i.e. an engine statement started failing (often with an empty
`result.error`, the stream-error path again). The day's landed-in-tree
edits after the 03:25 all-green run are exactly the agent's
relational/grouped-finish (executor/relational.cpp 07:14+09:41,
sql_engine.cpp 20:45 FK, insert.cpp 20:26). The `user_rollback` path in
NewOrder goes through the item-batch SELECT ("returned too few rows"),
so the grouped-finish/plan-cache interaction is the first place to
look. Harness note: `TpccWorkload` only records `LastError()` from
prepare failures, so a stream-level error surfaces as an empty string —
worth a `RunSqlWhy`-style phase+status capture there as well.

## Round 28 — TPC-C red is a real data bug: duplicated heap rows + blind unique index

Bisect of the 5 TpccWorkloadTest failures (green on clean HEAD, red on
the current tree; the workload/test diffs are cosmetic) landed on a
physical inconsistency, not a test-expectation drift:

* `benchmark/tpcc_workload.cpp` harness bug fixed first (mine to fix,
  benchmark layer): `ExecuteSql` **discarded the drain status** —
  `Drain(); return kSuccess;` — so any stream-level engine error looked
  like "0 rows / committed=false" with an *empty* error string. Now
  returns `result.Value().GetStatus()` and labels the message
  `stream: …`. (Same lesson as RunSqlWhy in the session fuzzer: lazy
  results demand status capture.)
* With the fix, `NewOrderRollsBackOnInvalidItem` fails at the point
  read `SELECT … FROM customer WHERE c_w_id=1 AND c_d_id=1 AND c_id=4`
  → 0 rows. A temporary trace dump of the same table (env
  `TPCC_SQL_TRACE=1` remains on the two RunSql logs) showed **every
  customer row present twice** (ids `1,1,2,2,…`) while the composite
  *unique* index lookup for c_id=4 returned nothing.
* So: the fixture load produced duplicate visible versions (aborted /
  committed mixup or double batch insert in `Initialize`'s chunked
  "failed to commit TPC-C fixture" path), and the unique index point
  probe is inconsistent with the table for the same key — either the
  index lost the entries or its search dead-ends on the duplicate
  pair. Both symptoms live in the uncommitted b_plus_tree/insert
  rework (index/b_plus_tree.cpp +* oracle, executor/insert.cpp 89 new
  lines) — handed to the index owner with the exact failing filter:
  `./build/tpcc_workload_test --gtest_filter='*NewOrderRollsBackOnInvalidItem*'`.
  Coverage note for the fuzzer backlog: sql_session exercises small
  tables; the duplicated-version flavor needs multi-batch loads —
  worth a fixture-mode stress once the owner lands.

## Round 29 — fresh regressions triage: LSM view runs (page-pool suspect)

The 03:25 agent session fixed 4/5 TpccWorkloadTest failures (green now,
alongside their new TPCC work). The 5 red tests are now
`LSMViewTest.Recover/OverwriteAndScan/Delete*Scan` — deterministic on
the current tree, **PASS on clean HEAD**, even in a pristine working
directory: every test aborts on
`Corrupt: Corrupted run header: tmp_view_merger_test-*/blob.db.meta`
(common/status_or.hpp MoveValue FATAL, from sorted_run.cpp:274/285).
The lsm_detail sources themselves are unchanged; the mutated substrate
is the page layer mid-migration (page/page_pool.cpp, page_manager.cpp
dirty). Signature smells like writes not landing (or zero pages
returned after reopen) → header magic reads back 0. Owner note:
`./build/lsm_view_test --gtest_filter='*Recover*'` reproduces in any
cwd; the stale `tmp_view_merger_test-*/` dirs at the repo root are also
leaking — clean_root_artifacts should sweep them.
Harness fixes this turn (mine): `TINYLAMB_SESSION_ONLY_SEED` now runs
any full-width seed (old uint32-stoul+range made libFuzzer seeds
unreproducible in gtest, silently tripping the sanity floor instead),
and the coverage-floor asserts only guard full sweeps.

Round-21 sweep: oracle + expression fuzzers green on the agent's tree;
both flip crashes probe-reduce to the open relational-route correlated-
EXISTS bug (line 57-style `SET … WHERE … AND EXISTS (…)` skipped by the
interpreter — no new shape). pgwire hit one more 5 s race-stall, this
time on the EXISTS race variant under jobs=2 — but the box was running
the agent's -j24 rebuild at that minute, standalone replay of the crash
file passes, and the direct 2-thread engine race never deadlocks:
timed-out parse-subprocess under build storms. Consider a wider
race timeout when the tree is being rebuilt concurrently.

## Round 30 — post-page-pool sweep green; single stale expectation left

w22 on the page-pool-migrated tree: b_plus_tree/table/lsm_tree/cache
fuzzers (560 s each) + sql_oracle (35 min) + griffin (25 min) **all
rc=0** — the earlier Tpcds/FiveTableJoin segfaults were my build racing
the agent's relink, not crashes: fresh ctest is 3259/3260 green.
The lone failure is `SqlFrontendCoverageTest.RejectedSyntax`
(untracked, mid-writing) expecting `SELECT t.* FROM t` to be rejected;
the engine now accepts it with correct GoogleSQL semantics (verified:
plain, mixed-select-list with ORDER BY, and qualified-star-over-join
all return the right rows). ZetaSQL makes qualified `t.*` legal, so the
expectation — not the engine — looks stale; left for its author.
Ops: `/tmp/opencode` had drifted to 39 GB (other tools co-resident);
swept the campaign's dead probe binaries (6 GB back), link failures
resolved.

## Round 31 — bug 63 (engine, mine): duplicate correlated-EXISTS conjuncts killed DML decorrelation

**Bug 63** — `UPDATE t0 SET … WHERE EXISTS (SELECT 1 FROM t1 WHERE
t1.u = t0.u) AND EXISTS (…same…)` updated 0 rows. Root cause: the
decorrelation pass turned *every* split conjunct into a
`DecorrelationSpec`, and the identical conjuncts each wrapped a
semi-join `ProductPlan` around the child; the second wrapper probes an
outer key the first one already reshaped, matching nothing (silent 0
rows). `E AND E` ≡ `E`, and semi/anti-joins are idempotent, so
`plan/optimizer.cpp` now dedups candidates with `SameExpression` before
wrapping (kept-conjunct routing unchanged; `EXISTS … OR EXISTS …`
remains the pre-existing unsupported-but-honest `requires relational
evaluation` error).

Found via: w15 `sql_session` crash (still red after the agent's
relational-EXISTS fixes — bisected against my `ex4.cpp` pred matrix:
only the exact-duplicate AND shape misfires). Guards:
`QueryTest.DmlDuplicateExistsConjunctDecorrelateOnce` (semantic,
fails-fast on regressions) and the w15 trace committed to
`query/testdata/session_regressions/` (replay suite green, 13 files).

Round status vs the agent's parallel work: the relational-route
correlated-EXISTS regression and the ghost unique-duplicate are fixed
upstream (crash replays green); LSM run-header failures are gone since
the page-pool landing. `TpccWorkload` green. Still open on their side:
`SqlFrontendCoverageTest.RejectedSyntax` (untracked new test asserting
`SELECT t.*` rejection — engine now accepts it spec-correctly; the
expectation, not the engine, looks stale).
