# Environment variables

Every `TINYLAMB_*` environment variable read by the code, in one place.  The
`TINYLAMB_DISABLE_*` family are A/B hatches: each turns off one default-on
fast path so a suspected optimization can be isolated without a rebuild.
They exist for debugging and benchmarking only — none of them changes
documented SQL semantics; flipping one must not change results, only speed
(and error timing).  Bool hatches are on when set to any non-empty value.

## Storage / buffer pool / WAL

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_PAGE_POOL_BYTES` | Buffer-pool byte budget override (default derives from machine memory; `tpch_benchmark` only pins its default when this is unset). |
| `TINYLAMB_LOG_SEGMENT_BYTES` | WAL segment file size override. |
| `TINYLAMB_CHECKPOINT_SECONDS` | Fuzzy-checkpoint interval override. |
| `TINYLAMB_CHECKPOINT_FLUSH_MS` | Pacing for the checkpoint flush phase. |
| `TINYLAMB_DISABLE_EOR_CHECKPOINT` | Skip the end-of-recovery checkpoint. |
| `TINYLAMB_WAL_RETENTION_WARN_BYTES` | Warn once WAL volume exceeds this many bytes. |
| `TINYLAMB_WAL_FREE_WARN_BYTES` | Warn when recyclable-but-unrecycled WAL exceeds this many bytes. |
| `TINYLAMB_RECOVERY_TRACE` | Verbose per-record recovery tracing. |
| `TINYLAMB_RECOVERY_WORKERS` | Parallel recovery worker count override. |
| `TINYLAMB_CORRUPT_TAIL_PROBE` | Fault-injection probe: corrupt the WAL tail (recovery tests). |
| `TINYLAMB_CRC_BITFLIP_PROBE` | Fault-injection probe: flip CRC bits (checksum tests). |
| `TINYLAMB_DISABLE_READBUF_NOINIT` | Zero-initialize page read buffers (default skips the memset). |
| `TINYLAMB_DISABLE_CRC_LANES` | Disable the 4-lane CRC32C kernel. |

## Transactions

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_WRITE_INTENT_WAIT_MS` | Write-intent wait bound override. |
| `TINYLAMB_DEADLOCK_POLICY` | Deadlock handling selection for benchmarks/tests (`abort`- vs `wait`-first). |
| `TINYLAMB_DISABLE_INDEX_RESERVATION` | Undo-space reservation for index pages (`0`=on, `1`=off, `2`=off with no shared map; default on). |
| `TINYLAMB_INTENT_NO_PROGRESS_RESET` | `=1` restores the entry-time write-intent deadline instead of the default no-progress reset. |
| `TINYLAMB_TEST_UNDO_STOP_AFTER` | Test hook: stop the recovery loser-undo walk after N applications (deterministic mid-undo crash for `recovery_extra_test`). Unset = full walk. |
| `TINYLAMB_DISABLE_ANCHOR_VOID` | `=1` disables the body-restore-anchor stale-undo evidence voiding in `UndoLoserChains` (A/B for the stale-separator fix). |
| `TINYLAMB_DISABLE_SEPARATOR_SWEEP` | `=1` disables the post-undo stale-separator sweep in `RecoverFrom` (A/B). |
| `TINYLAMB_DISABLE_REDO_REINSERT_FLOOR` | `=1` makes compensation re-insert redo failures fatal again instead of the bounded-loss skip (A/B). |

## Executor / planner A-B hatches (aggregation)

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_DISABLE_TYPED_AGG` | Typed parallel stream aggregate; falls back to the generic loop. |
| `TINYLAMB_DISABLE_SLIM_AGG` | Slim SoA per-group aggregate states. |
| `TINYLAMB_DISABLE_AGG_ARENA` | int64 arena mirror of cached base relations. |
| `TINYLAMB_DISABLE_FILL_COPY` | Move-append cache fill (copy-append fallback). |
| `TINYLAMB_DISABLE_FLAT_GROUP_FOLD` | Flat-map cache-hit fold. |
| `TINYLAMB_DISABLE_HAVING_FAST` | Positional HAVING fast path. |
| `TINYLAMB_DISABLE_AGG_FILTER_PEEK` | Constant-compare peel in the typed fold. |
| `TINYLAMB_DISABLE_TYPED_CONST_PEEK` | Constant IN-list peel in the typed fold. |
| `TINYLAMB_DISABLE_REUSABLE_TYPED_AGG` | Pre-change gate: shared-table statements on the generic stream loop. |
| `TINYLAMB_DISABLE_CHUNK_COPY_APPEND` | Chunk copy-append scan path. |
| `TINYLAMB_DISABLE_FINISH_PARALLEL_FILTER` | Worker-side filter in the grouped-aggregate finish phase. |
| `TINYLAMB_DISABLE_PARALLEL_EXPRESSION_KEYS` | Bytecode-computed GROUP BY keys / aggregate args in the parallel grouped fold (plain columns only when set). |
| `TINYLAMB_DISABLE_PARALLEL_COUNT_DISTINCT` | Parallel COUNT(DISTINCT) build. |
| `TINYLAMB_DISABLE_FLAT_GROUP_MAP` | Flat open-addressing int64 group map (subquery runtime); falls back to `unordered_map`. |
| `TINYLAMB_DISABLE_AGG_COLUMN_PEEK` | Integer peek compares inside aggregation input. |

## Executor / planner A-B hatches (scan / filter / join)

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_DISABLE_COLCOL_COMPARE` | Column-vs-column positional compare peel in scan filters. |
| `TINYLAMB_DISABLE_PARALLEL_RELATION_FILTER` | Parallel relation filter. |
| `TINYLAMB_DISABLE_WORKER_FILTER` | Worker-side residual filter. |
| `TINYLAMB_DISABLE_WORKER_SCAN_FILTER` | Worker-side scan filter (relational factory). |
| `TINYLAMB_DISABLE_PARALLEL_KEYED_SCAN` | Parallel keyed (index) scan. |
| `TINYLAMB_DISABLE_INT_KEY_JOIN` | Fixed-width int-key fast path in the parallel hash join. |
| `TINYLAMB_DISABLE_JOIN_FLIP` | Hash-join build/probe flip. |
| `TINYLAMB_DISABLE_JOIN_REORDER` | Ordered-path inner-run reorder + WHERE-to-inner-ON placement. |
| `TINYLAMB_DISABLE_GROUPED_JOIN_FUSION` | Grouped-join fusion. |
| `TINYLAMB_DISABLE_PARALLEL_PROBE` | Parallel hash-join probe. |
| `TINYLAMB_DISABLE_CORRELATED_DOMAIN_FILTER` | Correlated-subquery domain filter. |
| `TINYLAMB_DISABLE_PREFIX_IN` | Prefix-IN predicate compilation in scan filters (falls back to the generic IN evaluation). |
| `TINYLAMB_DISABLE_SUBQUERY_FOLD` | Folding of row-independent subqueries into scan-filter constants. |
| `TINYLAMB_DISABLE_PREBUILT_MEMBERSHIP` | Prebuilt `col [NOT] IN (uncorrelated subquery)` membership set in scan filters (falls back to per-row evaluation). |
| `TINYLAMB_PREBUILT_MEMBERSHIP_DEBUG` | Trace why a membership predicate was not peeled to the prebuilt set. |
| `TINYLAMB_ENABLE_PREBUILT_SCALAR` | Opt-in prebuilt scalar apply (compile-time GROUP BY join for per-key applies); off by default because the build only pays for itself when reused. |
| `TINYLAMB_PREBUILT_SCALAR_DEBUG` | Trace why a scalar apply predicate was not peeled to the prebuilt map. |
| `TINYLAMB_DISABLE_SHARED_EXISTS_BUILD` | Sibling EXISTS/NOT EXISTS build sharing in the subquery runtime (each correlated EXISTS re-scans its base table when set). |
| `TINYLAMB_SCAN_PROJECTION` | Opt-in Cascades scan-column narrowing at the iterator (`=1` enables projected `FullScanPlan`/scan pushdown); default keeps full-width scans because plan feedback flipped TPC-H Q19 to a slower plan. |
| `TINYLAMB_DISABLE_TYPED_FILL` | Typed scan fill (arena publish for cache consumers); falls back to Row-shaped cache fills. |

## Bytecode / expression

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_DISABLE_BYTECODE` | Disable bytecode evaluation globally (AST fallback; also flips fuzz oracles). |

## Planner diagnostics

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_LEGACY_PLANNER` | Pre-Cascades planning heuristics. |
| `TINYLAMB_LEGACY_ESTIMATE` | Pre-blend cardinality estimation. |
| `TINYLAMB_GROUPED_ROUTE` | `=relational` prefers the hand-rolled relational route (scan projections, runtime filters, typed folds, grouped-join fusion) for grouped statements; the default keeps the Cascades-first order because the Cascades core infers OR-branch equijoins the relational heuristic does not (TPC-DS q13 degrades to nested loops under the relational route). |
| `TINYLAMB_RULE_STATS` | Print Cascades rule application counters. |
| `TINYLAMB_PLAN_PROFILE` | `=1` prints plan-time profiling (rule wall time table, parse-subprocess and prepare-statement timings). |
| `TINYLAMB_JOIN_CARD_DEBUG` | Prints per-key selectivity inputs and hash-join orientation candidates so a collapsed estimate or a wrong build side is visible. |
| `TINYLAMB_FUSION_DEBUG` | Trace grouped-join fusion decisions. |
| `TINYLAMB_DEBUG_ORDERED` | Trace ordered-path join decisions. |
| `TINYLAMB_DEBUG_GS` | Trace GROUPING SETS routing. |
| `TINYLAMB_FOLD_TRACE` | Trace expression fold decisions in the engine. |
| `TINYLAMB_TRACE_ROUTE` | Log why `PostRewriteNeedsRelational` kept a statement on the relational route (source shape). |
| `TINYLAMB_SCALAR_FN_CASCADES` | A/B hatch (default off): ordinary scalar functions cross the Cascades bridge instead of forcing the relational route, and the M5+ join-core derived-table flattener fires (TPC-H Q9 reaches `GroupByFinish`). Known-incomplete: TPC-DS rank-boundary determinism and cross-product avoidance fail under it, and Q9 runs ~1.7x slower than the relational route (bridge join-order quality), so keep it off outside A/B runs. |
| `TINYLAMB_FUSE_SIBLING_AGG` | `=0` disables sibling derived-aggregate fusion in `sql_engine.cpp` (same-group derived tables aggregated in separate members are rewritten into one fused scan). Unlike the `DISABLE_*` hatches this one is on by default and opted out with `=0`. |

## Memory / temp

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_QUERY_MEMORY_BYTES` | Per-query memory budget override. |
| `TINYLAMB_TEMP` | Directory for spill files (default `std::filesystem::temp_directory_path()`). |

## Server / frontend

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_MAX_CONNECTIONS` | Postgres server connection cap. |
| `TINYLAMB_PARSE_TIMEOUT_SEC` | External GoogleSQL parse child timeout. |

## Tests / fuzzers

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_KEEP_TEST_DATABASES` | Keep the run's database artifacts for post-mortem (see `AGENTS.md`). |
| `TINYLAMB_ORACLE_REGRESSION_DIR` | Directory of `*.test` files replayed by `SqlOracleFuzzer.ReplayCommittedRegressionFiles`. |
| `TINYLAMB_SESSION_REGRESSION_DIR` | Session-trace regression directory (wired to `query/testdata/session_regressions/`). |
| `TINYLAMB_GRIFFIN_REGRESSION_DIR` | Griffin-trace regression directory. |
| `TINYLAMB_AGGREGATE_REGRESSION_DIR` | Aggregate-fuzzer regression directory. |
| `TINYLAMB_SESSION_SEED` / `TINYLAMB_SESSION_ONLY_SEED` / `TINYLAMB_SESSION_REPEAT` | Session fuzzer seed selection and replay repetition. |
| `TINYLAMB_SESSION_VERBOSE` / `TINYLAMB_SESSION_DEBUG_PAGES` / `TINYLAMB_SESSION_PRED_DEBUG` / `TINYLAMB_SESSION_CHECK_QUANT` | Session fuzzer diagnostics. |
| `TINYLAMB_SESSION_ROUTE_FLIP` / `TINYLAMB_SESSION_FORCE_RELATIONAL` | Force AST/relational evaluator routing in session fuzzing. |
| `TINYLAMB_EXPR_FUZZ_PASSES` / `TINYLAMB_EXPR_FUZZ_VERBOSE` | Expression fuzzer rewrite-pass control and tracing. |
| `TINYLAMB_FUNC_FUZZ_TRACE` | Function fuzzer tracing. |
| `TINYLAMB_FUZZ_ITERS` | In-process fuzz iteration override. |
| `TINYLAMB_FUZZ_FORCE_SCOPED_DB_CREATE_FAILURE` | Fault-injection: make `ScopedDb` create fail (quota-guard tests). |
| `TINYLAMB_BPT_CONC_FUZZ_SEED` | B+Tree concurrency fuzzer seed. |
| `TINYLAMB_CALLGRIND_MEASUREMENT` | Benchmark mode tuned for callgrind (skips wall-time assertions). |
| `TINYLAMB_SKIP_ANALYZE` | `=1` skips the post-load ANALYZE pass in the TPC-H benchmark (profiling-only; statistics stay stale). |
