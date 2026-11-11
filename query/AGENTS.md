# `query/` — Layer 11 SQL frontend (`sql` rank)

GoogleSQL AST → `Statement` IR → `SqlEngine` routing → Cascades + executors.
Entry points for CLI, server and benchmarks are `SqlEngine::Execute(ctx, sql)`
(`StatusOr<QueryResult>`) and `Prepare(ctx, sql)` (`StatusOr<Executor>`).

## Key files

- `sql_engine.{hpp,cpp}` — facade. `QueryResult` unifies `Next`, `ForEach`,
  `Drain`, `Collect`, `AffectedRows` and `Dump`. Routes set operations
  (INTERSECT binds tighter), GROUP BY/HAVING and UNNEST/lateral queries.
  Exposes `LastError`, `LastStatementType`, `LastDmlTable`,
  `ResultColumnNames`, `ThreadExecutionCount`, `RuntimeStats` and
  `CompliancePrimaryKeyMode`.
- `googlesql_frontend.{hpp,cpp}` — spawns the pinned external
  `TINYLAMB_GOOGLESQL_EXECUTABLE --mode=parse -`, feeds stdin and enforces a
  timeout. Uses a 16-shard parse cache (capacity 1024) and one-second negative
  cache. Literal/comment-masked prepasses rewrite GROUP BY DISTINCT,
  DISTINCT ON and FETCH WITH TIES syntax.
- `googlesql_ast.{hpp,cpp}` + `googlesql_ast_visitor.{hpp,cpp}` — AST dump to
  Statement IR. Missing set-operation text is recovered from source byte ranges.
- `statement.hpp` — statement types and `SelectStatement`, `SelectSource`,
  `JoinType`, `SetOperationTree`. No `kExplain`: **EXPLAIN [ANALYZE] is a
  Prepare-level string prefix for SELECT/WITH only**, rejected for other
  statements without executing them.
- `plan_cache.hpp`, `sql_template.{hpp,cpp}` — fingerprint/literal binding.
  SELECT/UPDATE/DELETE retain parameter slots and rebind on replay. A SELECT
  replay slower than 50 ms and eight times its first replay is replanned.
  INSERT uses a per-execution slot clone in the process-global cache.
  EXPLAIN and non-templatable SQL do not participate. Plans based on measured
  clustered order (`AnyResolvedTableHasClusteredOrder`) must not be cached:
  row mutation can invalidate the order proof and any folded LIMIT/sort.
- `query_data.{hpp,cpp}` — optimizer input, compiled into `tinylamb_executor`
  for the plan → query_data edge. `join_reduction.*` simplifies joins.
- **Template-cache contract.** The parsed-tree cache (`FindTemplate` /
  `RememberTemplate` in `sql_engine.cpp`, keyed by the literal-free
  fingerprint) replays through `SqlTemplate::BindSelect`, which must rebuild
  the tree in *text order* and carry every piece of metadata a route depends
  on: `WITH RECURSIVE` markers, `RecursiveDepthSpec` (including its bounds,
  which are metadata rather than expressions, so the binder consumes their
  literals from the parameter list itself), per-branch
  `SetOperationMatch`/`SetOperationKind`, and the `SetOperationTree`. A set
  operation keeps two views of the same operands (the flat `UnionAll()`
  vectors and the tree, sharing the branch nodes) — bind the flat vectors
  once and rebuild the tree from them, or every literal is consumed twice and
  the bind dies with a parameter underflow. Tests can distinguish "the cache
  served this statement" from "it parsed verbatim" with
  `PlanCacheStats().template_fills` / `template_hits`.
  Regressions: `QueryTest.TemplateCacheKeepsRecursiveCteMetadata`,
  `SqlTemplateTest.BindsRecursiveWithSetOperationOnce`,
  `SqlTemplateTest.BindsRecursiveWithDepthBounds`.
- `testdata/googlesql_compliance/` + `googlesql_compliance_{file,test}.*` —
  corpus-driven compliance (`[mode=explain]` / `[mode=explain_analyze]`
  fragments). `sql_oracle_fuzzer.*`, `expr_oracle_fuzzer.*`,
  `griffin_fuzzer.*` (+ `scripts/expr_oracle.py` Python oracle).

## Before adding syntax

New syntax = frontend parse + `statement.hpp` shape + engine route (+ plan
node if it needs optimizing). Watch the V3' debt: readers of
`query/statement.hpp` from `plan/`+`executor/` are allowlisted, not a pattern
to copy.

## DML contracts

- Column DEFAULT uses `Constraint::kDefault` with a constant payload frozen at
  CREATE time; volatile expressions are rejected. INSERT substitutes defaults
  for omitted columns and the `__column_default` VALUES marker. UPDATE SET
  col=DEFAULT resolves the same way. Undeclared defaults are NULL. The single
  constraint slot prevents combining DEFAULT/FOREIGN KEY/PRIMARY KEY on a
  column; conflicting constraints must be rejected explicitly.
- PRIMARY KEY in CREATE TABLE is metadata only: INSERT does not enforce its
  uniqueness. `sql_dml_paths_coverage_test` pins this contract. Compliance mode
  can tag the first column as a key for optimizer rules; that is not write-time
  uniqueness enforcement either. Changing this is a semantic decision.
- MERGE materializes its source, classifies WHEN actions, then applies deletes,
  updates and inserts. An extracted ON equality key hashes the source;
  otherwise it uses linear matching. Multiple source matches for target
  UPDATE/DELETE are rejected. At most one NOT MATCHED BY TARGET and one BY
  SOURCE clause is supported. Ambiguous bare names fail, NULL keys do not
  match, and FK NO ACTION edges are enforced. MATCHED may read source columns;
  BY SOURCE may read only target columns. Tests: `query/merge_test.cpp`.
- TRUNCATE TABLE lowers to unconditional DELETE, retaining MVCC/WAL behavior.
  A dedicated physical chain reset is future work.
  Test: `MergeTest.TruncateTableDropsRowsAndKeepsSchema`.
- CREATE MATERIALIZED VIEW materializes into a catalog table and stores its
  defining SELECT under `Database::kMaterializedViewKeyPrefix` in the functions
  tree. Base writes do not refresh it. DROP removes rows/definition and rejects
  plain tables. The pinned parser has no REFRESH; use DROP + CREATE.
  Test: `query/matview_test.cpp`.
- CREATE TABLE FUNCTION stores an AST clone. FROM calls substitute
  statement-constant arguments (`SubstituteInSelect`); volatile arguments are
  rejected and trailing DEFAULT binding follows scalar UDF rules.
  Test: `QueryTest.TableValuedFunctionInvokesStoredBody`.
  Builtin GENERATE_SERIES lowers through the same TVF node to implicit UNNEST
  of an ARRAY. Its unaliased column is `value`; START/STOP/STEP named arguments
  reorder to the positional signature. TABLESAMPLE is rejected and JOIN accepts
  TVF operands. Test: `QueryTest.BuiltinTableFunctionBecomesRowSource`.
- `FuseSiblingDerivedAggregates` combines sibling aggregates over the same base
  table and GROUP BY keys. Member WHERE clauses become CASE expressions;
  HAVING lifts to an outer conjunction. Filtered members require a
  `COUNT(CASE WHEN p THEN 1 END) > 0` witness to preserve group membership.
  The rewrite is all-or-nothing: bare member references, outer stars and outer
  subqueries prevent it. ORDER BY member references map to SELECT output names.
  `TINYLAMB_FUSE_SIBLING_AGG=0` disables it; `LastFusedSiblingAggregates()` counts
  firings. Tests: `sibling_aggregate_fusion_test` and
  `QueryTest.SiblingDerivedAggregatesFuseIntoSingleScan`.

`expression/collation.hpp` reaches the frontend: an `ORDER BY x COLLATE 'spec'`
clause (a `Collate` child of `OrderingExpression`) is rewritten into the same
`COLLATE(x, 'spec')` call the function form builds, and a collated SELECT-list
ordinal (`ORDER BY 1 COLLATE 'spec'`) keeps its collator across the deferred
ordinal resolution. Without that the clause parsed but silently ordered binary,
and an unresolved ordinal left a constant key that ordered nothing.

`query/statement.hpp` records set-operation column matching per pair: the
visitor turns `BY NAME` / `STRICT CORRESPONDING` into `SetOperationMatch::by_name`,
plain `CORRESPONDING` into `corresponding`, and an `ON/BY (col_list)` into
`columns`, then hands the match to the `AddSetOperation` that introduces the
branch. `SelectStatement::MatchFor(pair_index)` resolves it per term (falling
back to the head-level `union_by_name_` flag for hand-built statements).
`ExecuteRecursiveCte` is the consumer that binds values by name; **non-recursive
set operations still match positionally and silently ignore the mode** — that
gap is recorded in `docs/next-actions.md` (第42報), so no test should pin the
positional answer for them.

WITH lists are resolved in dependency order, not declaration order, and the
check runs before any rewrite: `SqlEngine::PrepareStatement` calls
`relational_detail::ValidateCteDependencies` (living in
`executor/detail/subquery_runtime`, the lowest place both layers can include)
and reports "cyclic CTE reference among a, b" for a mutual reference. A
non-recursive entry is **not** visible inside its own definition — its
self-named reference resolves to the enclosing WITH or to a catalog table
(the compliance corpus's `with_on_multiple_levels` pins this for nested
WITHs), so it is not a cycle. Only a `WITH RECURSIVE` entry reads its own
name (as the work table). A recursive body that reads a sibling CTE must keep
the dependency-ordered relational route, which is why
`ExecuteUnnestSelect` consults `RecursiveCteLiftable` before minting a
work-table leaf.

CTE execution strategy is decided before the plan is built, in
`PrepareStatement`, and the order is load-bearing: `InlineSingleUseCtes`
(non-recursive entry with exactly one reference becomes a derived source, so
the body runs once and M5 flattening pushes the consumer's WHERE into the
body's scan) → `PushSharedCteRestrictions` (an entry that stays shared
materializes once, and only the **disjunction** of the per-site restrictions
may be pushed into its body: one site's predicate alone would delete rows
another site needs) → `MaterializeCtes` (one eager cell, shared `kValues`
leaf) → `FlattenDerivedSources`. The pushdown is gated on plain FROM sites
joined by CROSS/INNER, a self-only predicate at every site, a single-table
unnamed projection body with no GROUP BY/DISTINCT/LIMIT/QUALIFY/window/
set-op/subquery, and an `EstimateSelectivity < 1` estimate from the base
table's statistics (no statistics ⇒ no pushdown). `LastCteRestrictionPushdowns()`
reports it; `SqlEnginePushesSharedCteRestrictionsDisjunctively` pins the
disjunction, the gates, and the shrunken cell (leaf `rows_evaluated`).

## Locking selects

`SELECT ... FOR UPDATE` is GoogleSQL's only lock clause (the AST's `LockMode`
node; `FOR SHARE`/`NOWAIT`/`SKIP LOCKED` are parser errors). It is not a new
lock layer: the statement takes the **DML source contract**, i.e.
`QueryData::require_row_position_ = true` makes the access path `lock_rows`,
so the scan reserves a write intent (`Transaction::AddWriteSet`) on every row
it visits *before* the WHERE drops it, and the intent lives until
commit/abort. Consequences to keep in mind when touching the select route:

- The predicate is applied after the lock, exactly like UPDATE/DELETE sources;
  a full scan reserves the whole scanned table (safe, wider than PostgreSQL's
  matched-rows-only rule). An index range still bounds the visited rows.
- Shapes with no single row position per output row (JOIN, aggregate, DISTINCT,
  GROUP BY, HAVING, window, set operation, CTE, derived source, table sample)
  are refused with an explicit diagnostic, as is the relational route (it does
  not carry positions). Never fall back to an unlocked read.
- `BindSelect` must re-apply `ForUpdate()`; otherwise a template-cache hit
  replays the statement as an unlocked read.
- Rows come from the locking scan's head-version resolution, so the returned
  image and the locked image are the same version.

The wait policy is an embedder knob, not syntax: GoogleSQL has no
`NOWAIT`/`SKIP LOCKED` (the parser stops after `FOR UPDATE`).
`SqlEngine::SetForUpdateWaitMode(kNoWait)` makes the statement use the
try-lock path (`QueryData::wait_for_write_intent_ = false`, the same policy
DELETE's source scan uses) so a conflicting writer fails the statement at once
with `kConflicts` instead of parking for the 5s cap. The mode is part of the
plan-cache fingerprint for `FOR UPDATE` text; without that, a plan filled under
`kWait` would be replayed by a `kNoWait` engine.

Regressions: `QueryTest.SqlEngineForUpdate*`,
`SqlTemplateTest.BindSelectPreservesForUpdate`.

Test: `./build/query_test`, `googlesql_frontend_test`, `googlesql_ast_test`,
`sql_template_test`, `join_reduction_test`, `sql_clustered_order_test`,
`sql_collation_test`,
TPC-H/TPC-C suites (`sql_engine_tpch_test`, `sql_engine_tpcc_test`),
`sql_extra_test`.

CREATE INDEX SQL now routes through `CreateIndexStatement` to
`Database::CreateIndex`: plain ascending column keys, UNIQUE, IF NOT EXISTS,
and GoogleSQL STORING are supported. Names are checked within the target
table. Expression/DESC/SEARCH/VECTOR/OPTIONS/OR REPLACE spellings are rejected
explicitly. The pinned AST dump omits IF NOT EXISTS / OR REPLACE, so the
visitor validates the source keyword prefix (including comments); source-less
CREATE INDEX dumps are refused. DDL never enters the literal template cache.
Regressions: `GoogleSqlAstTest.CreateIndex*`, `SqlEngineExtraTest.SqlCreateIndex*`.
