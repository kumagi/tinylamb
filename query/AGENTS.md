# `query /` — Layer 11 SQL frontend(`sql` rank)

GoogleSQL AST → `Statement` IR → `SqlEngine` routing → Cascades +
executors. Entry points for CLI, server, benchmarks:
`SqlEngine::Execute(ctx, sql) -> StatusOr<QueryResult>` (one-shot) and
`Prepare(ctx, sql) -> StatusOr<Executor>` (plan/inspect).

## Key files

- `sql_engine.{
  hpp, cpp
}
` — facade. `QueryResult {
  Next / ForEach / Drain / Collect / AffectedRows / Dump
}
` unifies streaming.Routes set - ops(INTERSECT binds tighter),
    GROUP BY / HAVING, UNNEST / lateral through Cascades;
exposes
  `LastError / LastStatementType / LastDmlTable / ResultColumnNames`,
  `ThreadExecutionCount / RuntimeStats`, `CompliancePrimaryKeyMode`
                                                  .- `googlesql_frontend.{
  hpp, cpp
}
` — parse = spawn pinned external
  `TINYLAMB_GOOGLESQL_EXECUTABLE-- mode = parse -` (`fork`+`poll`, timeout),
    stdin - fed;
16 - shard parse cache(cap 1024) + 1 s negative cache.Pre
    - pass rewrites on literal / comment -
    masked text(e.g. `GROUP BY DISTINCT` →
  `GROUP BY`, `DISTINCT ON`, `FETCH … WITH TIES` → LIMIT / OFFSET)
        .- `googlesql_ast.{
  hpp, cpp
}
` + `googlesql_ast_visitor.{ hpp, cpp }
` — AST dump →
  `Statement` IR.Missing set -
    op text is recovered from source byte
        ranges.- `statement.hpp` — `StatementType {
  kCreateTable / kDropTable / kSelect / kInsert / kUpdate / kDelete / kAnalyze
}` + `SelectStatement/SelectSource/JoinType/
  SetOperationTree/…`. No `kExplain`: **`EXPLAIN [ANALYZE]` is a `Prepare`-
  level string prefix for SELECT/WITH only** (`ParseExplain` in
  `sql_engine.cpp`), rejected otherwise without executing anything.
- `plan_cache.hpp`, `sql_template.{
  hpp, cpp
}
` — fingerprint + literal - binding compiled -
    plan cache.SELECT / UPDATE /
        DELETE keep parameter slots and rebind them on replay;
a SELECT replay slower than 50 ms and
    8× its first replay is planned again.INSERT stays a per -
        execution slot clone in the process - global cache.EXPLAIN and non -
        templatable SQL never participate.A statement planned while a table
                carried a *
            measured *
            clustered index
            order(`AnyResolvedTableHasClusteredOrder`) is never stored
    : the proof dies with the next row mutation,
    and the plan may have folded a LIMIT
        or an incremental sort on it.- `query_data.{
  hpp, cpp
}` — optimizer input (compiled into `tinylamb_executor`
  for the `plan/ → query_data` edge). `join_reduction.*` — join simplification.
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

- Column `DEFAULT` clauses ride the column's reserved
  `Constraint::kDefault` slot (constant value payload, frozen at CREATE
  time; volatile expressions are rejected). The INSERT executor substitutes
  the default for columns omitted from the target list and for the DEFAULT
  keyword in VALUES (marker `__column_default`, resolved against the target
  schema);
`UPDATE SET col =
    DEFAULT` resolves the same way
        .A column without a declared default has the implicit default NULL — no
        error(BigQuery semantics, pinned by the `dml_*` compliance cases and
  `QueryTest.ColumnDefault*`)
        .DEFAULT cannot combine with PRIMARY KEY or
    FOREIGN KEY on the same column(
        single constraint slot; the pinned parser rejects the adjacency anyway)
            .- `PRIMARY KEY` in `CREATE TABLE` is metadata only
    : **INSERT does not enforce its uniqueness *
      *(duplicates are stored).The contract is pinned by
  `sql_dml_paths_coverage_test`; changing it is a semantic decision, not a
  bugfix. `SqlEngine::CompliancePrimaryKeyMode` additionally tags the first
  column with a `kPrimaryKey` constraint for compliance-corpus runs — that
  tag feeds the optimizer (e.g. pk_unique_distinct_elimination), it is not
  write-time enforcement either.
- `MERGE INTO` (`query/merge_test.cpp` pins the shapes) materializes the
  source as a relation in `SqlEngine::ExecuteMerge`, classifies target rows
  (and unmatched source rows) against the WHEN clauses, and applies the
  pending DML in apply order deletes → updates → inserts. An equality key
  extracted from the ON conjuncts hashes the source side (no extractable key
  falls back to a linear matcher). Clause semantics follow BigQuery: one
  action per target row (a target matching several source rows under
  UPDATE/DELETE is rejected), at most one NOT MATCHED [BY TARGET] and one BY
  SOURCE clause, bare names present in both relations are ambiguous errors,
  NULL keys never match, and FK NO ACTION edges are enforced.  MATCHED
  conditions may reference source columns;
BY SOURCE actions may only reference target
    columns(source references surface as evaluation errors)
        .- `TRUNCATE TABLE` desugars to an
               unconditional `DELETE` (visitor - level) so MVCC
               / WAL semantics stay uniform;
a physical page -
        chain reset with a dedicated WAL record remains a future transaction -
        layer optimization.Regression
    : `MergeTest.TruncateTableDropsRowsAndKeepsSchema`.- `CREATE MATERIALIZED
            VIEW[IF NOT EXISTS] name AS SELECT...` materializes the query into a
                catalog table of the same
                name(CTAS schema derivation) and
    persists the defining SELECT text under a reserved key prefix in the
    functions tree(`Database::kMaterializedViewKeyPrefix`)
        .Queries answer from the stored
    rows(manual staleness — base writes do not flow through);
`DROP MATERIALIZED VIEW` removes rows +
    definition and refuses plain tables.There is no REFRESH statement in the
        pinned parser;
the manual refresh path is DROP +
    CREATE.Regression : `query / matview_test.cpp`.- `CREATE TABLE FUNCTION
    name(args)
AS SELECT...` bodies are stored as AST clones in the UDF registry,
    and `FROM name(arg, ...)` expands the stored query with the call -
        site arguments(`SubstituteInSelect` substitutes parameter names with
                constants)
            .Arguments must be statement
        - constant(volatile calls are rejected before evaluation);
trailing DEFAULT padding follows scalar -
    UDF binding rules.Regression :
  `QueryTest.TableValuedFunctionInvokesStoredBody`
  Builtin table functions reach the same `TVF` AST node: `FROM
  GENERATE_SERIES(start, stop[, step])` lowers to an implicit UNNEST of the
  scalar generator's ARRAY value, so `LoadSource` (relational) and `kUnnest`
  (Cascades) row-ify it without a dedicated scan operator. Unaliased value
  column is `value`, `START`/`STOP`/`STEP` named arguments reorder into the
  positional signature, `TABLESAMPLE` is rejected, and join operands accept
  the `TVF` node. Regression:
  `QueryTest.BuiltinTableFunctionBecomesRowSource`.
        .- `FuseSiblingDerivedAggregates` (kSelect rewrite chain)
               fuses sibling derived tables that aggregate the SAME base table
           under the SAME GROUP BY key list into one aggregation(
               TPC -
                   H Q21's `multi`/`only_late`): each member's WHERE becomes a CASE inside its own aggregates, its HAVING lifts to
                       an outer conjunct over the fused columns,
               and a member with a WHERE adds a `COUNT(CASE WHEN p THEN 1 END) >
                       0` witness because group membership is not carried by the
                               wrapped aggregates.The rewrite is all -
                   or -nothing;
               bare member references, outer star items,
               and subqueries in the outer scope leave the statement unfused
                       .  `ORDER BY` member references map to the matching
                           SELECT item's output name (ApplyOrderBy resolves
                               against the post -
                   projection schema)
               .
  `TINYLAMB_FUSE_SIBLING_AGG = 0` opts out;
`SqlEngine::LastFusedSiblingAggregates()`
  counts firings.  Regressions: `sibling_aggregate_fusion_test` +
  `QueryTest.SiblingDerivedAggregatesFuseIntoSingleScan`.

`expression/collation.hpp` reaches the frontend: an `ORDER BY x COLLATE 'spec'`
clause (a `Collate` child of `OrderingExpression`) is rewritten into the same
`COLLATE(x, 'spec')` call the function form builds, and a collated SELECT-list
ordinal (`ORDER BY 1 COLLATE 'spec'`) keeps its collator across the deferred
ordinal resolution. Without that the clause parsed but silently ordered binary,
and an unresolved ordinal left a constant key that ordered nothing.

Test: `./build/query_test`, `googlesql_frontend_test`, `googlesql_ast_test`,
`sql_template_test`, `join_reduction_test`, `sql_clustered_order_test`,
`sql_collation_test`,
TPC-H/TPC-C suites (`sql_engine_tpch_test`, `sql_engine_tpcc_test`),
`sql_extra_test`.
