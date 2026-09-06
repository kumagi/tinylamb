# `query/` — Layer 11 SQL frontend (`sql` rank)

GoogleSQL AST → `Statement` IR → `SqlEngine` routing → Cascades +
executors. Entry points for CLI, server, benchmarks:
`SqlEngine::Execute(ctx, sql) -> StatusOr<QueryResult>` (one-shot) and
`Prepare(ctx, sql) -> StatusOr<Executor>` (plan/inspect).

## Key files

- `sql_engine.{hpp,cpp}` — facade. `QueryResult{Next/ForEach/Drain/Collect/
  AffectedRows/Dump}` unifies streaming. Routes set-ops (INTERSECT binds
  tighter), GROUP BY/HAVING, UNNEST/lateral through Cascades; exposes
  `LastError/LastStatementType/LastDmlTable/ResultColumnNames`,
  `ThreadExecutionCount/RuntimeStats`, `CompliancePrimaryKeyMode`.
- `googlesql_frontend.{hpp,cpp}` — parse = spawn pinned external
  `TINYLAMB_GOOGLESQL_EXECUTABLE --mode=parse -` (`fork`+`poll`, timeout),
  stdin-fed; 16-shard parse cache (cap 1024) + 1 s negative cache. Pre-pass
  rewrites on literal/comment-masked text (e.g. `GROUP BY DISTINCT` →
  `GROUP BY`, `DISTINCT ON`, `FETCH … WITH TIES` → LIMIT/OFFSET).
- `googlesql_ast.{hpp,cpp}` + `googlesql_ast_visitor.{hpp,cpp}` — AST dump →
  `Statement` IR. Missing set-op text is recovered from source byte ranges.
- `statement.hpp` — `StatementType{kCreateTable/kDropTable/kSelect/kInsert/
  kUpdate/kDelete/kAnalyze}` + `SelectStatement/SelectSource/JoinType/
  SetOperationTree/…`. No `kExplain`: **`EXPLAIN [ANALYZE]` is a `Prepare`-
  level string prefix for SELECT/WITH only** (`ParseExplain` in
  `sql_engine.cpp`), rejected otherwise without executing anything.
- `plan_cache.hpp`, `sql_template.{hpp,cpp}` — fingerprint + literal-binding
  compiled-plan cache. EXPLAIN and non-templatable SQL never participate.
- `query_data.{hpp,cpp}` — optimizer input (compiled into `tinylamb_executor`
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

Test: `./build/query_test`, `googlesql_frontend_test`, `googlesql_ast_test`,
`sql_template_test`, `join_reduction_test`, TPC-H/TPC-C suites
(`sql_engine_tpch_test`, `sql_engine_tpcc_test`), `sql_extra_test`.
