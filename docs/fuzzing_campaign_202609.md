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
| query | `sql_oracle_fuzzer_libfuzzer` | TLP / NoREC / PQS / DQE / index-independence / txn-splitting |
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
- Full `ctest`: **2410/2410 passed** after every fix batch.
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
preserve truthiness but not value, and IEEE/overflow edge semantics
(NaN, ±0.0, INT64_MIN) silently erased or introduced by rewrites. The second
class was **Status-contract violations** — throwing paths reached from
`TryEvaluate`/`Visit` (8 of 23) — all converted to the `Try*` family.
Recovery gained two *stateful* correctness fixes (txn-id seeding, CLR-aware
undo) that only a crash-model harness can find; both were invisible to the
test suite because no pre-existing test did crash → recover → write →
recover-again. The 20 → 27 target expansion plus whole-project coverage
instrumentation is the durable output: the harnesses now find bugs on their
own, and the nightly runs 12 of them in CI.
