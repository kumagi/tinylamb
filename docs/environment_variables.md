# Environment variables

Every `TINYLAMB_*` environment variable read by the code, in one place.  The
`TINYLAMB_DISABLE_*` family are A/B hatches: each turns off one default-on
fast path so a suspected optimization can be isolated without a rebuild.
They exist for debugging and benchmarking only — none of them changes
documented SQL semantics; flipping one must not change results, only speed
(and error timing).  Most bool hatches test presence (`getenv != nullptr`), so even an empty value or `0` enables them; knobs with value parsing document their own rules.

## Storage / buffer pool / WAL

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_PAGE_POOL_BYTES` | PageManager capacity hint in bytes, divided by `kPageSize`; unset/empty or less than one page uses the default. The legacy name remains active. |
| `TINYLAMB_LOG_SEGMENT_BYTES` | WAL segment file size override. |
| `TINYLAMB_CHECKPOINT_SECONDS` | Periodic checkpoint interval in seconds (default 10); `0` disables the timer. Compaction and explicit checkpoints remain separate triggers. |
| `TINYLAMB_DISABLE_EOR_CHECKPOINT` | Skip the end-of-recovery checkpoint. |
| `TINYLAMB_DISABLE_CRC_LANES` | Disable the 8-lane CRC32C kernel. |

## Transactions

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_WRITE_INTENT_WAIT_MS` | Write-intent wait bound override. |
| `TINYLAMB_DEADLOCK_POLICY` | Deadlock handling selection for benchmarks/tests (`abort`- vs `wait`-first). |
| `TINYLAMB_DISABLE_INDEX_RESERVATION` | Index undo-space diagnostic modes: `0`=on (default), `1`=off, `2`=no shared map, `3`=no per-transaction vector, `4`=registration on with admission discount off, `5`=discount on with tree drain retry off. |
| `TINYLAMB_INTENT_NO_PROGRESS_RESET` | `=1` restores the entry-time write-intent deadline instead of the default no-progress reset. |

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
| `TINYLAMB_DISABLE_INDEX_ONLY_VISIBILITY_MAP` | Index-only visibility-map fast path: a non-retained entry on a chain-free page is served from the B+Tree image (key + INCLUDE) instead of the heap. |
| `TINYLAMB_DISABLE_INDEX_SCAN_PREFETCH` | Index-scan look-ahead heap-page prefetcher (a second cursor warms the pages the serial `IndexScan` is about to fetch). |
| `TINYLAMB_DISABLE_PARALLEL_INDEX_SCAN` | ParallelIndexScan for large ascending single-range index scans (falls back to the serial `IndexScan`). |
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
| `TINYLAMB_SCAN_PROJECTION` | Opt-in Cascades scan-column narrowing at the iterator (any set value, including `0` or an empty string, enables projected `FullScanPlan`/scan pushdown); default keeps full-width scans because plan feedback flipped TPC-H Q19 to a slower plan. |
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
| `TINYLAMB_GROUPED_ROUTE` | Unset/default prefers the relational route for grouped statements; `=cascades` selects Cascades first. |
| `TINYLAMB_RULE_STATS` | Print Cascades rule application counters. |
| `TINYLAMB_DEBUG_DUMP_MEMO` | Dump every Cascades memo group (tag, scan filter, expression fingerprints) to stderr after exploration. Diagnostic for D1 (one group, one meaning) violations. |
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
| `TINYLAMB_CALLGRIND_MEASUREMENT` | Benchmark mode tuned for callgrind (skips wall-time assertions). |
| `TINYLAMB_SKIP_ANALYZE` | `=1` skips the post-load ANALYZE pass in the TPC-H benchmark (profiling-only; statistics stay stale). |

## Server diagnostics

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_DISABLE_IO_AFFINITY` | When present, leave the PostgreSQL server I/O thread unpinned. Normally Run pins that thread to the first CPU in its inherited allowed set. SQL workers and storage threads retain the full set; synchronous SQL on I/O temporarily restores it, and Run restores the caller mask on exit. Best effort on systems that allow affinity calls. |
| `TINYLAMB_SERVER_METRICS` | When present, emit cumulative JSON counters to stderr at most once per second: monotonic timestamp, selected I/O CPU (`io_cpu`, -1 if unpinned), worker tasks/queue/execution/completion handoff ns, pending tasks, own-commit WAL waits, WAL sync calls/ns and buffered/durable bytes, active transactions and MVCC chain/snapshot gauges. Enables transaction timing. Blocking execution includes WAL wait; the deferred single-statement path reports separate durability_commands/wait_ns/batches after SQL execution. Compare deltas and subtract nested spans instead of adding them. Sync statistics count runtime segment syncs, including segment rolls, but exclude open-time sync and final shutdown fsync. |

## Additional active controls (source audit, 2026-11-11)

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_DBG_CNT` | Retired diagnostic probe: currently read by the optimizer but its body is empty. |
| `TINYLAMB_DISABLE_ARENA_MEMBERSHIP` | Disable membership construction from a cached base-relation arena. |
| `TINYLAMB_DISABLE_BUILD_RESIDUAL_HOIST` | Disable build-side residual hoisting in fused grouped joins. |
| `TINYLAMB_DISABLE_BULK_COUNT` | Disable bulk count updates in fused grouped joins. |
| `TINYLAMB_DISABLE_COUNT_STAR_FUSION` | Disable COUNT(*) join fusion. |
| `TINYLAMB_DISABLE_HAVING_FOLD` | Disable HAVING folding in relational aggregation. |
| `TINYLAMB_DISABLE_PARALLEL_MEMBERSHIP` | Disable parallel membership set construction. |
| `TINYLAMB_DISABLE_PK_ABSORPTION` | Disable primary-key dimension absorption in input building. |
| `TINYLAMB_DISABLE_STREAM_GROUPED_JOIN` | Disable streaming grouped joins. |
| `TINYLAMB_DISABLE_TRANSITIVE_KEY_FILTER` | Disable transitive join-key filters. |
| `TINYLAMB_EXPR_ROW_ORACLE` | Row-expression oracle selection: `0` disables, another present value enables; unset samples one quarter of seeds. |
| `TINYLAMB_FORCE_PLAIN` | Force the PAX encoder to choose plain encoding (presence). |
| `TINYLAMB_FORCE_SCHEME` | Force a named PAX encoding where supported by the chooser. |
| `TINYLAMB_FUZZ_CONTINUE` | Continue after saving a finding when nonempty and not `0`; default aborts. |
| `TINYLAMB_FUZZ_DB_DIR` | Directory for temporary scoped fuzzer databases. |
| `TINYLAMB_FUZZ_FINDINGS_DIR` | Additional directory receiving reproducer and report copies. |
| `TINYLAMB_FUZZ_TRACE_SQL` | Trace fuzzer SQL and SQL-engine execution diagnostics. |
| `TINYLAMB_FUZZ_VERBOSE` | Verbose oracle-fuzzer diagnostics. |
| `TINYLAMB_GOOGLESQL_BIN` | Parser executable fallback override (see frontend executable resolution). |
| `TINYLAMB_GOOGLESQL_ZYGOTE_SOCK` | Unix socket for the GoogleSQL parser zygote. |
| `TINYLAMB_JOIN_REGRESSION_DIR` | Override the join-oracle regression corpus directory. |
| `TINYLAMB_NEGATIVE_CACHE_TTL_SEC` | Positive integer parse-failure cache lifetime in seconds; default 1. |
| `TINYLAMB_NO_CRC` | Skip the PAX chunk CRC calculation for diagnostics. |
| `TINYLAMB_NO_HASH_IDX` | Omit the PAX hash index; reads fall back to scans. |
| `TINYLAMB_NO_INV_IDX` | Omit PAX dictionary inverted indexes. |
| `TINYLAMB_NO_NEW_SCHEMES` | Disable newer adaptive PAX encoding candidates. |
| `TINYLAMB_NO_XOR8` | Omit PAX XOR8 row-group filters. |
| `TINYLAMB_ORACLE_ITERS` | Override SQL oracle test iteration count. |
| `TINYLAMB_PARSE_CACHE_SIZE` | Total parse-cache capacity, divided across 16 shards; values below 16 are ignored. |
| `TINYLAMB_SMALL_HASH_IDX` | Use two hash slots per PAX row instead of four, rounded up to a power of two. |
| `TINYLAMB_VALDBG` | PAX validation diagnostics. |

Retired names removed from the active inventory: `TINYLAMB_BPT_CONC_FUZZ_SEED`, `TINYLAMB_CHECKPOINT_FLUSH_MS`, `TINYLAMB_CORRUPT_TAIL_PROBE`, `TINYLAMB_CRC_BITFLIP_PROBE`, `TINYLAMB_DISABLE_ANCHOR_VOID`, `TINYLAMB_DISABLE_READBUF_NOINIT`, `TINYLAMB_DISABLE_REDO_REINSERT_FLOOR`, `TINYLAMB_DISABLE_SEPARATOR_SWEEP`, `TINYLAMB_RECOVERY_TRACE`, `TINYLAMB_RECOVERY_WORKERS`, `TINYLAMB_TEST_UNDO_STOP_AFTER`, `TINYLAMB_WAL_FREE_WARN_BYTES`, `TINYLAMB_WAL_RETENTION_WARN_BYTES`. They have no current C++ readers.
