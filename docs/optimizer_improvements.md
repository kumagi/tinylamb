# Cascades Optimizer Improvements

This document tracks the incremental work required to turn tinylamb's Cascades
framework into a production-grade query optimizer, replacing the remaining
hand-written portions of `plan/optimizer.cpp`.

> Revision 2026-08 (handover): audited against the working tree after the
> previous implementer departed. Phases 1–7 are complete, with two correctness
> defects found and fixed during the handover (see Handover Log). Remaining
> work concentrates on Phase 8 coverage gaps and Phase 9 hardening.

## Handover Log (2026-08)

Audit found the plan-level LIMIT work half-wired, producing wrong results:

- **Double application**: the optimizer folded LIMIT/OFFSET into the plan
  (`LimitPlan`) while `sql_engine.cpp` unconditionally wrapped another
  `LimitExecutor`. `SELECT ... ORDER BY a DESC LIMIT 2 OFFSET 1` returned one
  row instead of two. Fixed by adding `PlanBase::EnforcesLimit()`; the engine
  skips its wrapper when the plan already enforces the exact limit/offset.
- **Truncation before sort**: the `limit` implementation rule folded a
  truncating `LimitPlan` even when the child did not deliver a required
  ordering; the engine would then sort the truncated stream (wrong top-N).
  The rule now passes the child through unchanged in that case, leaving
  Sort+Limit to the engine (D6 soundness).
- **DISTINCT interaction**: DISTINCT is applied by the engine *above* the
  plan output, so a plan-level limit would truncate before dedup. The engine
  therefore withholds LIMIT from `QueryData` when `select->Distinct()` is
  set; Distinct → Sort → Limit stays ordered correctly above the plan.

Regression guards added to `query/query_test.cpp`
(`SqlEngineSelectOrderByLimitOffset` covers unordered ORDER BY+LIMIT+OFFSET,
plain LIMIT+OFFSET single application, and DISTINCT+LIMIT).

## Optimizer Capability Follow-ups (2026-08, post-Phase 8)

Three improvements landed after auditing the aliased/self-join path:

1. **Index nested-loop join for renamed (aliased) inputs**
   (`implementation_rules.cpp`, `product_plan.hpp`). The index-join branch
   now locates the owning relation key for statistics, translates join-key
   qualifiers to physical names, and declares a renamed full-width output
   schema via a new `ProductPlan` constructor — previously the alias-
   qualified keys never matched the physical schema, silently disabling
   index joins for every self-join. A soundness guard skips index joins when
   the right scan group carries a pushed filter: the IndexJoin executor
   probes the raw table through its index and bypasses the child plan, so a
   filter implemented by that child would otherwise vanish.
2. **Ordering visibility across renames** (`plan/relation_rename_plan.*`).
   The rename is now a dedicated positional pass-through plan whose
   `IsOrderedBy` translates requests back to physical names and whose
   executor surfaces a `Rename:` node in EXPLAIN. Aliased ORDER BY + LIMIT
   queries regain index-provided-order credit and Top-K folding
   (`AliasedOrderByLimitFoldsTopK`).
3. **Constant IN lists drive point-union index access**
   (`executor/index_scan.*`, `plan/index_scan_plan.*`). A constant IN list
   on a leading index key becomes one point range per distinct value,
   offered as an alternative against full/range scans; the IN conjunct stays
   in the scan predicate so correctness never depends on bound tightness.
   Multi-range scans keep global order only when a single range remains.

Routing note: plain multi-table joins with table aliases now go to the
cost-based optimizer; the relational-engine EXPLAIN/spill tests that pin
relational internals use a semantically neutral `EXISTS` marker to stay on
that path.

---

## Current Status

**Phases 1–7: complete.** Summary of what exists:

- Logical operators `kScan`, `kJoin`, `kSelection`, `kProjection`,
  `kAggregation`, `kLimit` with payload storage (`plan/cascades.hpp`);
  payload-aware `Fingerprint()`; arity validation in `Memo::AddExpression`;
  derived root-layer groups (`EnsureDerivedGroup`) tagged
  selection/aggregation/projection/limit.
- Predicate pushdown: WHERE decomposition into `ConjunctInfo` at memo build;
  rules `push_selection_through_join`, `split_selection_over_join`,
  `push_selection_into_scan` (scan-group filters), `merge_selections`.
  Join conditions attach to their deepest covering join (`Memo::NewJoin`),
  so every conjunct applies exactly once per root-to-leaf path.
- Projection handling: required-column computation flows into scans via
  `RuleContext::scan_projections` (global touched-set approximation, adequate
  for conjunctive queries); multi-table `SELECT *` expansion works.
- Implementation rules moved out of the optimizer core into
  `plan/implementation_rules.cpp`: `index_scan`, `full_scan`, `selection`,
  `projection`, `aggregation`, `limit`, `hash_join` (in-memory/hybrid),
  `index_join`, `nested_loop_join`. No thread-local context; explicit
  immutable `RuleContext` instead.
- Physical properties: `RequiredChildProperties` derivation (joins and
  aggregation drop ordering/row-position requirements), ordering modeled as a
  property with N·log N sort penalties for non-delivering alternatives,
  `access_method` hint, reserved `distribution` field, `limit_hint` enabling
  Top-K costing on order-delivering scans.
- Cost model: selectivity from `TableStatistics::EstimateSelectivity`, join
  cardinality `|L|·|R| / max NDV` capped by cross product, hybrid hash-spill
  penalty, estimated rows threaded through `PlanAlternative`/`BestPlan`.
- Exploration: worklist-based (`DrainTouchedGroups`, expression cursors — no
  pass cap), per-rule operator hints (`Rule::MayApply`), disconnected-cut
  pruning in `join_enumeration` (`Memo::CutConnected`), graceful degradation
  via per-group expression caps (`Memo::Degraded()`).
- Diagnostics: `OptimizerOptions::dump_memo` logs Memo contents plus chosen
  plan.

`plan/optimizer.cpp` is now a thin QueryData→Memo translation plus options
plumbing; the DP-era search logic is gone.

## Design Decisions (implemented as specified)

- **D1 Group identity**: Selection/Projection/Aggregation/Limit live as
  expressions inside groups sharing their child's relation set; derived
  root-layer groups disambiguate via tags. Fingerprints include payloads
  (canonicalized conjuncts).
- **D2 Property derivation**: `SearchEngine::RequiredChildProperties`.
- **D3 Cost units**: rows accessed; estimates as above.
- **D4 Context plumbing**: `RuleContext` replaces the former thread-local.
- **D5 Join-graph metadata**: conjunct masks on the Memo
  (`CutConnected`, `JoinGraphDisconnected`).
- **D6 Ordering/LIMIT boundary**: optimizer folds LIMIT only over
  order-delivering children (Top-K); the engine remains the safety net for
  sort and limit, skipping wrappers only when the plan already satisfies
  them (`EnforcesLimit`, `IsOrderedBy`).

## Phase 8 — Query Coverage Gaps

Completed 2026-08 unless noted. Routing contract after this phase:

- **Optimizer path (cost-based)**: single-relation queries; multi-relation
  queries whose sources carry table aliases — including self-joins of one
  physical table; plain INNER/CROSS `JOIN ... ON` (the engine folds the ON
  conjunction into WHERE before planning).
- **Relational path (heuristic)**: unaliased multi-table joins, LEFT JOIN,
  FROM-subqueries, CTEs/WITH, GROUP BY/HAVING, OR/subquery expressions
  (`NeedsRelationalEvaluation`), and unqualified column references across
  several relations (precise ambiguity diagnostics live there).

Implemented in this phase:

- [x] Table aliases and self-joins. Relation identity = alias when given,
      else table name (`QueryData.from_` + `aliases_`); scan implementations
      rename their output schemas to the relation identity
      (`RenameToRelation`, `plan/implementation_rules.cpp`) while translating
      pushed conjuncts down to physical names (`QualifyDown`). Memo groups,
      join conditions, and root projections all speak the alias-qualified
      names, so `FROM t AS a JOIN t AS b ON a.k = b.k` plans and executes
      correctly through Cascades, including index-backed range selection on
      one side.
- [x] INNER/CROSS `JOIN ... ON` through SQL: conditions fold into WHERE and
      attach to their deepest covering join (`Memo::NewJoin`); non-equality
      residuals apply above the cross product.
- [x] Disjunctions: verified safe by construction — `NeedsRelationalEvaluation`
      routes any OR to the relational engine, so opaque predicates never
      enter pushdown reasoning. Covered by existing relational tests.
- [x] LEFT JOIN end-to-end correctness guarded by a dedicated test
      (`SqlEngineLeftJoinStaysCorrect`): stays on the relational path, which
      null-pads unmatched rows.
- [x] Ambiguity diagnostics preserved: unqualified columns across aliased
      relations keep the relational resolver's "ambiguous column" error.

Deliberately not migrated to cost-based planning (functional via the
relational executor; each needs its own design note):

- Outer joins *inside* the memo: requires a kOuterJoin logical operator,
  null-rejection analysis gating every pushdown rule, and commutativity
  restrictions. Guard-rail comments already mark the rule sites in
  `plan/cascades.cpp`.
- Subquery decorrelation into SemiJoin/AntiJoin: IN/EXISTS evaluate via
  `subquery_runtime` today; decorrelation changes cardinality estimation and
  needs new implementation rules.
- Projection pushdown as true rewrite rules (`push_projection_through_join`
  etc.): the global touched-set approximation remains sound for the shapes
  that reach the optimizer now that disjunctive/subquery shapes route to the
  relational engine; revisit if those shapes ever move over.
- RIGHT/FULL OUTER joins: not parsed at all (frontend supports kCross/kInner/
  kLeft only).

Known routing limitation (performance, not correctness): ORDER BY credit is
not yet propagated across renamed schemas — aliased queries lose the
index-provides-ordering bonus and always sort engine-side.

## Phase 9 — Hardening (partially complete)

- [x] Plan-dump diagnostics (`dump_memo`).
- [x] End-to-end suites: `SqlEngineTpchTest` / `SqlEngineTpccTest` exercise
      the Cascades path and pass.
- [ ] Widen golden-result comparisons for join-order-sensitive queries
      (TPC-H Q8/Q9-style chains) against captured outputs.
- [ ] Fuzz the memo under randomized rule subsets
      (`OptimizerOptions::disabled_implementation_rules`) to catch
      unsound-rule interactions like the LIMIT ones fixed in this revision.

## Known Unrelated Failures (out of optimizer scope)

`executor_test` fails 4 cases (`Aggregation`,
`VectorizedScanFilterProjectAggregatePipeline`,
`RelationalAggregateSumEmptyAndDouble`, `DistinctNullValueThrowsFromHash`).
They construct executors directly without the optimizer; symptoms are
floating-point bit mismatches on printed-equal doubles and a missing throw on
NULL distinct hashing. Likely introduced by the concurrent
executor/data_chunk workstream in this tree; tracked separately.

---

## Notes for Other Agents

- All logical operators and rules live in the `tinylamb::cascades` namespace:
  small, pure data structures plus rule lambdas operating only on Memo groups
  and expressions. Predicates are manipulated via expression utilities
  (`TouchedColumns`, `SplitConjuncts`); storage/catalog/executor types are
  confined to implementation rules behind the `RuleContext` object.
- Soundness invariants that must not regress:
  1. Every WHERE conjunct is applied exactly once per root-to-leaf path
     (`Memo::NewJoin` canonicalization; never bypass it when synthesizing
     joins).
  2. A plan may only truncate (`LimitPlan`) when it delivers any required
     ordering; otherwise pass through.
  3. The engine owns Distinct → Sort → Limit above the plan and skips each
     wrapper only on proof from the plan (`EnforcesLimit`, `IsOrderedBy`).
- If reality diverges from this document, update this document in the same
  change.
- Tests: `plan/cascades_test.cpp` for framework checks,
  `plan/optimizer_test.cpp` for end-to-end integration, `query/query_test.cpp`
  for SqlEngine-level behavior.
