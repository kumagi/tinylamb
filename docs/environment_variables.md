# Environment variables

Every `TINYLAMB_*` environment variable read by the code, in one place.  The
`TINYLAMB_DISABLE_*` family are A/B hatches: each turns off one default-on
fast path so a suspected optimization can be isolated without a rebuild.
They exist for debugging and benchmarking only; most change only speed (and
error timing), but a few opt-in knobs are known-incomplete and can change
results (see `TINYLAMB_SCALAR_FN_CASCADES` below). Check each entry before
using a knob in an A/B comparison or production.  Boolean hatches use different conventions: some test presence (`getenv != nullptr`), while others parse specific values such as `1` or `0`. An empty value or `0` therefore does not have one global meaning; check each entry. A few defaults are opt-out instead: `TINYLAMB_FUSE_SIBLING_AGG=0` disables that rewrite. Check the individual entry before assuming `=0` is a no-op.

## Storage / buffer pool / WAL

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_PAGE_POOL_BYTES` | Retired (ignored no-op): replaced by `TINYLAMB_MEMORY_LIMIT_BYTES` / `TINYLAMB_BUFFER_POOL_BYTES`. |
| `TINYLAMB_TLCS_MMAP_LIMIT_BYTES` | Override byte threshold above which `TlcsFile` uses paged `platform::Pread` + bounded LRU cache instead of mmap resident storage (default `min(ColumnarPoolLimit() / 2, 1 GiB)`). |
| `TINYLAMB_DISABLE_ADAPTIVE_TLCS_READ` | Use coalesced windows by default for paged TLCS projections. Explicit direct reads still use their requested mode. |
| `TINYLAMB_AUTO_TLCS` | Automatic TLCS enablement (`database/database.cpp`, `AutoTlcsEnabled`): on by default when unset/empty; values `0`, `false`, `FALSE`, `off`, `OFF`, `no`, `NO` turn it off. |
| `TINYLAMB_ANALYZE_AUTO_TLCS` | Allow `ANALYZE` to auto-compact a table to TLCS when nonempty and not starting with `0`; `TINYLAMB_AUTO_TLCS` must also be enabled. |
| `TINYLAMB_LOG_SEGMENT_BYTES` | WAL segment size override in bytes; values below the 32 KiB page size or invalid values use the 16 MiB default. |
| `TINYLAMB_CHECKPOINT_SECONDS` | Legacy interval parameter passed from `PageStorage` to `CheckpointManager` (default `10`, parsed integer); checkpoints are actually driven by LSM compaction / committed-mutation thresholds, not a periodic timer. |
| `TINYLAMB_WAL_RETENTION_WARN_BYTES` | Warn threshold (bytes; default `64 MiB`, `0` disables) for WAL segments pinned by long-running active transactions (`recovery/checkpoint_manager.cpp`). |
| `TINYLAMB_DISABLE_EOR_CHECKPOINT` | Skip the end-of-recovery checkpoint. |
| `TINYLAMB_DISABLE_CRC_LANES` | Disable the 8-lane CRC32C kernel. |

## Transactions

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_WRITE_INTENT_WAIT_MS` | Write-intent wait bound override. |
| `TINYLAMB_DDL_QUIESCENCE_TIMEOUT_MS` | Max milliseconds a DDL drop waits for table-lease quiescence before retrying as a conflict (`common/incarnation.hpp`, default `60000`; non-numeric values fall back to the default). |
| `TINYLAMB_STATEMENT_TIMEOUT_MS` | Default per-statement timeout in milliseconds (default `0` = disabled; also configurable via `Transaction::SetStatementTimeout`). |
| `TINYLAMB_DEADLOCK_POLICY` | Benchmark/test deadlock policy: `wait_die`, `wound_wait`, `deadlock_detect`/`detect`, or unset for engine default. |
| `TINYLAMB_DISABLE_INDEX_RESERVATION` | Index undo-space diagnostic modes: `0`=on (default), `1`=off, `2`=no shared map, `3`=no per-transaction vector, `4`=registration on with admission discount off, `5`=discount on with tree drain retry off. |
| `TINYLAMB_INTENT_NO_PROGRESS_RESET` | Presence-based (any value, including `=0`, enables): restores the entry-time write-intent deadline instead of the default no-progress reset. |

## Executor / planner A-B hatches (aggregation)

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_DISABLE_PARTITIONED_DICTIONARY_FOLD` | Disable owner-partitioned dictionary expression aggregation and use bounded preparation waves. |
| `TINYLAMB_DISABLE_QUOTIENT_FUSION` | Disable the fused quotient scan for grouped top-count queries, including the sampled numeric-pair route. |
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
| `TINYLAMB_DISABLE_PARALLEL_SCAN` | Morsel `ParallelScan` for large full scans (falls back to the serial `FullScan`). Read per `EmitExecutor` call so tests can toggle it. |
| `TINYLAMB_DISABLE_INDEX_ONLY_VISIBILITY_MAP` | Index-only visibility-map fast path: a non-retained entry on a chain-free page is served from the index image (key + INCLUDE) instead of the heap. |
| `TINYLAMB_DISABLE_INDEX_SCAN_PREFETCH` | Index-scan look-ahead heap-page prefetcher (a second cursor warms the pages the serial `IndexScan` is about to fetch). |
| `TINYLAMB_DISABLE_PARALLEL_INDEX_SCAN` | ParallelIndexScan for large ascending single-range index scans (falls back to the serial `IndexScan`). |
| `TINYLAMB_DISABLE_INT_KEY_JOIN` | Fixed-width int-key fast path in the parallel hash join. |
| `TINYLAMB_DISABLE_PACKED2X32_HJ` | Disable the `kPacked2x32` two-INT64 hash-join key mode in `executor/detail/planning_heuristics.cpp`; two-int joins then use the multi-INT64 composite key path. |
| `TINYLAMB_DISABLE_JOIN_FLIP` | Hash-join build/probe flip. |
| `TINYLAMB_DISABLE_ORDERED_INNER_FLIP` | Presence-based A/B hatch: disable smaller-build inner join flip in `planning_heuristics.cpp`. |
| `TINYLAMB_DISABLE_DISTINCT_ON_TOP1` | Disable the ordered-winner fast path for `DISTINCT ON`; enabled when the value starts with `1`. |
| `TINYLAMB_DISABLE_BLOCK_KLEENE` | Presence-based A/B hatch: disable block Kleene predicate evaluation in parallel scans. |
| `TINYLAMB_DISABLE_FAST_LIKE` | Disable the literal-pattern LIKE fast path when the value starts with `1`; re-read by the explicit runtime reload helper. |
| `TINYLAMB_DISABLE_FAST_REGEX` | Disable the fast regex search path when the value starts with `1`; re-read by the explicit runtime reload helper. |
| `TINYLAMB_DISABLE_OUTER_JOIN_COUNTS` | Disable the grouped outer-join count-to-histogram rewrite; enabled when the value is nonempty and does not start with `0`. |
| `TINYLAMB_DISABLE_OUTER_OR_BRANCHES` | Disable outer-join OR-branch expansion when the value starts with `1`. |
| `TINYLAMB_DISABLE_QUALIFY_PUSHDOWN` | Disable partition-only QUALIFY-to-WHERE pushdown; enabled when the value is nonempty and does not start with `0`. |
| `TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE` | Disable the sliding DISTINCT window inverse path when the value starts with `1`. |
| `TINYLAMB_DISABLE_OUTER_FETCH_TIES` | Disable streaming `FETCH ... WITH TIES` when set to a nonempty value not starting with `0`; unset, empty, or `0`-prefixed values keep the streaming path enabled. |
| `TINYLAMB_DISABLE_WINDOW_HALO` | Disable bounded window halo optimization. The executor disables it when the value starts with `1`; the planner disables its halo rule for any nonempty value not starting with `0`. Use `1` to disable both. |
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
| `TINYLAMB_JIT_DISABLE_OPT` | Presence-based diagnostic: set to any value to omit LLVM O3 middle-end optimization for newly compiled JIT kernels. Process-wide kernel cache remains active. |
| `TINYLAMB_DISABLE_FUSED_SUM_JIT` | Use separate Filter, Project and scalar aggregate stages for eligible signed INT64 pipeline SUMs. |
| `TINYLAMB_FILTER_HASH_PROBE` | Resident signed INT64 inner join probe routing. Default `auto` samples eligible batches after 65,536 input rows. `off` uses the ordinary filter/operator path. Diagnostic `direct` and `compact` force either eligible kernel from the first batch. |
| `TINYLAMB_DISABLE_LITERAL_SEARCH_JIT` | Set to any value to use the native TLCS block precheck and row filter for literal substring predicates. |
| `TINYLAMB_DISABLE_FULL_LITERAL_MASK_JIT` | Keep the first/last byte SIMD precheck and scalar verification for short literals on AVX-512BW hosts. |
| `TINYLAMB_DISABLE_DELIMITED_CAPTURE_JIT` | Use the checked regex evaluator for anchored group keys that can be lowered to a delimited capture. |
| `TINYLAMB_DISABLE_FUSED_CAPTURE_HAVING` | Keep the ordinary two-stage aggregate for delimited capture group keys and do not collect reusable bucket proposals. |
| `TINYLAMB_DISABLE_GROUP_HASH_JIT` | Use scalar hashing and the shared bitmap for near-unique TLCS numeric-key groups. |
| `TINYLAMB_DISABLE_JIT_DENSE_SPLIT` | Disable the JIT dense-split path for fused grouped joins (`executor/detail/fused_grouped_join.cpp`); falls back to the non-dense grouped-join execution. |
| `TINYLAMB_DISABLE_JIT_PACKED_DEDUP` | Disable the JIT packed 2x32 radix dedup kernels for streaming grouped joins (`executor/detail/fused_grouped_join_stream.cpp`). |
| `TINYLAMB_DISABLE_JIT_RADIX_AGG` | Disable the JIT radix-key aggregate kernel in `fused_grouped_join.cpp`. Distinct from `TINYLAMB_DISABLE_RADIX_AGG`, which gates the non-JIT radix aggregation path. |
| `TINYLAMB_DISABLE_SHARDED_DUPLICATE_SIEVE` | Keep the shared duplicate bitmap and full-key replay when the ordinal-based sharded path is eligible. |
| `TINYLAMB_DISABLE_DATE_PART_HASH_JIT` | Use ordinary timestamp extraction and mixed-key hashing for eligible UTC group keys. |
| `TINYLAMB_DISABLE_STRING_FINGERPRINT_JIT` | Use full-string hashes for eligible TLCS single-string Top-N count sweeps. |
| `TINYLAMB_DISABLE_TLCS_BOUNDARY_AGG` | Use ordinary execution for aggregates eligible for exact TLCS boundary frequencies or extrema. |
| `TINYLAMB_DISABLE_TLCS_DUPLICATE_SIEVE` | Disable duplicate screening for nearly unique numeric keys in a TLCS `ORDER BY COUNT(*) DESC LIMIT` query. |
| `TINYLAMB_JIT_DUMP_DIR` | Write optimized LLVM IR and native object files for newly compiled JIT kernels into this directory; failed diagnostic writes do not affect execution. |
| `TINYLAMB_DISABLE_BYTECODE` | Set to `1` to disable bytecode evaluation globally (AST fallback; also flips fuzz oracles). |
| `TINYLAMB_DISABLE_FILTER_ORDER` | Disable residual scan-filter predicate reordering only when the value starts with `1`; unset or other values keep the default ordering optimization. |

## Planner diagnostics

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_EXEC_ENGINE` | Select execution engine: default (unset or `!= legacy`) lowers SELECT queries through `PipelineLowering` (`executor/pipeline_lowering.hpp`) with OOC spillable sinks; `=legacy` selects the historical `EmitExecutor` tree. |
| `TINYLAMB_LEGACY_PLANNER` | Pre-Cascades planning heuristics. |
| `TINYLAMB_LEGACY_ESTIMATE` | Pre-blend cardinality estimation. |
| `TINYLAMB_GROUPED_ROUTE` | Unset/default prefers the relational/grouped pipeline route for grouped statements; `=cascades` selects Cascades first. |
| `TINYLAMB_RULE_STATS` | Presence-based: print Cascades rule application counters. |
| `TINYLAMB_DEBUG_DUMP_MEMO` | Dump every Cascades memo group (tag, scan filter, expression fingerprints) to stderr after exploration. Diagnostic for D1 (one group, one meaning) violations. |
| `TINYLAMB_PLAN_PROFILE` | Presence-based (any value, including `=0`, enables): prints plan-time profiling (rule wall time table, parse-subprocess and prepare-statement timings). |
| `TINYLAMB_JOIN_CARD_DEBUG` | Prints per-key selectivity inputs and hash-join orientation candidates so a collapsed estimate or a wrong build side is visible. |
| `TINYLAMB_FUSION_DEBUG` | Trace grouped-join fusion decisions. |
| `TINYLAMB_DEBUG_TLCS` | Presence-based: print TLCS columnar fast-path / partitioned-aggregate diagnostics. |
| `TINYLAMB_DEBUG_GS` | Trace GROUPING SETS routing. |
| `TINYLAMB_FOLD_TRACE` | Trace expression fold decisions in the engine. |
| `TINYLAMB_TRACE_ROUTE` | Log why `PostRewriteNeedsRelational` kept a statement on the relational route (source shape). |
| `TINYLAMB_SCALAR_FN_CASCADES` | A/B hatch (default off; requires the literal value `1`): ordinary scalar functions cross the Cascades bridge instead of forcing the relational route, and the M5+ join-core derived-table flattener fires (TPC-H Q9 reaches `GroupByFinish`). Known-incomplete: TPC-DS rank-boundary determinism and cross-product avoidance fail under it, and Q9 runs ~1.7x slower than the relational route (bridge join-order quality), so keep it off outside A/B runs. |
| `TINYLAMB_FUSE_SIBLING_AGG` | `=0` disables sibling derived-aggregate fusion in `query/select_rewrites.cpp` (`FuseSiblingDerivedAggregates`; same-group derived tables aggregated in separate members are rewritten into one fused scan). The rewrite is invoked from `query/sql_engine.cpp`. Unlike the `DISABLE_*` hatches this one is on by default and opted out with `=0`. |

## Memory / temp

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_QUERY_MEMORY_BYTES` | Per-query memory budget override (legacy path only; pipelined/OOC queries use query grants and the process pool). |
| `TINYLAMB_TEMP` | Directory for legacy `SpillFile` / `ProcessMemoryBudget::ResolveTempDirectory()` (default `std::filesystem::temp_directory_path()` unless registered DB directory overrides). |
| `TINYLAMB_TEMP_DIR` | Override directory specifically checked by `SpillStore` (`common/spill/spill_store.cpp`). |
| `TINYLAMB_MEMORY_LIMIT_BYTES` | Process-wide limit for tracked storage + columnar + query + tx memory (`ProcessMemoryBudget`). Default 16 GiB (min supported 1 GiB); `0` disables the limit; `auto` scales to half of physical memory (min 16 GiB) and lifts the fixed 4 GiB storage / 8 GiB columnar cache caps. See [ADR 0003](adr/0003-memory-budget-thresholds.md). |
| `TINYLAMB_BUFFER_POOL_BYTES` | Cap for the storage pool (resident row pages). Default `min(4 GiB, limit / 4)`. |
| `TINYLAMB_COLUMNAR_CACHE_BYTES` | Cap for the columnar cache pool. Default `min(8 GiB, limit / 2)`; the shared soft limit and other pools' floors further constrain reservations. |
| `TINYLAMB_STORAGE_FLOOR_PCT` | Minimum `M_t` percentage guaranteed for `storage` pool (default `25`). |
| `TINYLAMB_QUERY_FLOOR_PCT` | Minimum `M_t` percentage guaranteed for `query` pool (default `25`). |
| `TINYLAMB_TX_FLOOR_PCT` | Minimum `M_t` percentage guaranteed for `tx` pool (default `5`). |
| `TINYLAMB_COLUMNAR_FLOOR_PCT` | Minimum `M_t` percentage guaranteed for `columnar` pool (default `0`). |
| `TINYLAMB_GRANT_WAIT_TIMEOUT_MS` | Admission wait timeout in milliseconds for `QueryMemoryCoordinator::AcquireGrant` before failing with `kMemoryLimitExceeded` (default `30000`; `0` means no wait / fail immediately). |
| `TINYLAMB_TEMP_LIMIT_BYTES` | Maximum temporary disk bytes allowed for spill files/stores (`0` = unlimited). Exceeding it fails with `kNoSpace` ([ADR 0005](adr/0005-spill-store-placement.md)). |
| `TINYLAMB_OP_MEMORY_BYTES` | Test/diagnostic override for per-operator working memory budget `B_op` (`0` = automatic). |
| `TINYLAMB_ALWAYS_SPILL` | Any non-empty value other than exactly "0" forces out-of-core spill paths even on small inputs (string comparison, not numeric) (testing/diagnostic knob). |
| `TINYLAMB_INJECT_MEMORY_FAIL` | Countdown (`> 0`): fail the N-th memory request across TryReserve, pool reserve, and grant paths for fault-injection testing. |
| `TINYLAMB_INJECT_SPILL_IO_FAIL` | Countdown (`> 0`): fail the N-th spill I/O call for fault-injection testing. |

## Server / frontend

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_MAX_CONNECTIONS` | PostgreSQL server connection cap. |
| `TINYLAMB_PARSE_TIMEOUT_SEC` | External GoogleSQL parse child timeout in seconds (default 5). |
| `TINYLAMB_DISABLE_AUTO_ZYGOTE` | Disable automatic GoogleSQL parser-zygote startup only when the value is exactly `1`; explicit socket or binary overrides also skip auto-start. |

## Tests / fuzzers

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_KEEP_TEST_DATABASES` | Keep the run's database artifacts for post-mortem only when the value is exactly `1`, `true`, `yes`, or `on` (case-sensitive; see `database/test_workspace_cleanup_env.cpp`). Other values, including `0`/`false`, keep cleanup enabled. |
| `TINYLAMB_ORACLE_REGRESSION_DIR` | Directory of `*.test` files replayed by `SqlOracleFuzzer.ReplayCommittedRegressionFiles`. |
| `TINYLAMB_SESSION_REGRESSION_DIR` | Session-trace regression directory (wired to `query/testdata/session_regressions/`). |
| `TINYLAMB_OOC_REGRESSION_DIR` | OOC fuzzer regression directory (wired to `query/testdata/ooc_regressions/`). |
| `TINYLAMB_GRIFFIN_REGRESSION_DIR` | Griffin-trace regression directory. |
| `TINYLAMB_AGGREGATE_REGRESSION_DIR` | Aggregate-fuzzer regression directory. |
| `TINYLAMB_SESSION_SEED` / `TINYLAMB_SESSION_ONLY_SEED` / `TINYLAMB_SESSION_REPEAT` | Session fuzzer seed selection and replay repetition. |
| `TINYLAMB_SESSION_VERBOSE` / `TINYLAMB_SESSION_DEBUG_PAGES` / `TINYLAMB_SESSION_PRED_DEBUG` / `TINYLAMB_SESSION_CHECK_QUANT` | Session fuzzer diagnostics. |
| `TINYLAMB_SESSION_ROUTE_FLIP` / `TINYLAMB_SESSION_FORCE_RELATIONAL` | Force AST/relational evaluator routing in session fuzzing. |
| `TINYLAMB_OOC_FUZZ_ONLY_SEED` / `TINYLAMB_OOC_FUZZ_SCALE` / `TINYLAMB_OOC_FUZZ_SECONDS` / `TINYLAMB_OOC_FUZZ_MINIMIZE_SECONDS` / `TINYLAMB_OOC_FUZZ_VERBOSE` / `TINYLAMB_OOC_FUZZ_EVICT_THREAD` / `TINYLAMB_OOC_FUZZ_NO_LEAK_CHECK` / `TINYLAMB_OOC_FUZZ_IGNORE` / `TINYLAMB_OOC_FUZZ_SKIP_OPS` / `TINYLAMB_OOC_FUZZ_ABORT_AFTER_FAILED_READ` / `TINYLAMB_OOC_FUZZ_OP_TIMEOUT` | OOC SQL fuzzer knobs (seed pin, iteration/time scaling, minimization timeout, verbose trace, background eviction thread toggle, leak check bypass, `;`-separated signature filter for known findings, `,`-separated op kinds the generator must not emit, roll the transaction back after a tolerated read failure, seconds one op may run in `sql_ooc_fuzzer_test` before the case fails with its trace; default 45, `0` disables). Workflow: [`ooc_fuzzing.md`](ooc_fuzzing.md). |
| `TINYLAMB_FUZZ_OOC_JOBS` / `TINYLAMB_FUZZ_OOC_SLICE_SECONDS` / `TINYLAMB_FUZZ_OOC_SESSION_SECONDS` / `TINYLAMB_FUZZ_OOC_OP_TIMEOUT` / `TINYLAMB_FUZZ_OOC_MAX_TRACES` / `TINYLAMB_FUZZ_OOC_MINIMIZE_SECONDS` / `TINYLAMB_FUZZ_OOC_KNOWN_DIR` | Read by `scripts/remote_fuzz_supervisor.sh` only: the out-of-core campaign worker on `ssh oci` (parallel sessions with 0 = off, slice length, per-session generation time, op timeout, traces kept per bucket, minimization budget, known-findings directory). Table in [`ooc_fuzzing.md`](ooc_fuzzing.md). |
| `TINYLAMB_EXPR_FUZZ_PASSES` / `TINYLAMB_EXPR_FUZZ_VERBOSE` | Expression fuzzer rewrite-pass control and tracing. |
| `TINYLAMB_FUNC_FUZZ_TRACE` | Function fuzzer tracing. |
| `TINYLAMB_FUZZ_ITERS` | In-process fuzz iteration override. |
| `TINYLAMB_FUZZ_FORCE_SCOPED_DB_CREATE_FAILURE` | Fault-injection: make `ScopedDb` create fail (quota-guard tests). |
| `TINYLAMB_CALLGRIND_MEASUREMENT` | Benchmark mode tuned for callgrind (skips wall-time assertions). |
| `TINYLAMB_SKIP_ANALYZE` | Presence-based (any value, including `=0`, enables): skips the post-load ANALYZE pass in the TPC-H benchmark (profiling-only; statistics stay stale). |

## Server diagnostics

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_DISABLE_IO_AFFINITY` | When present, leave the PostgreSQL server I/O thread unpinned. Normally Run pins that thread to the first CPU in its inherited allowed set. SQL workers and storage threads retain the full set; synchronous SQL on I/O temporarily restores it, and Run restores the caller mask on exit. Best effort on systems that allow affinity calls. |
| `TINYLAMB_SERVER_METRICS` | When present, emit cumulative JSON counters to stderr at most once per second: monotonic timestamp, selected I/O CPU (`io_cpu`, -1 if unpinned), worker tasks/queue/execution/completion handoff ns, pending tasks, own-commit WAL waits, WAL sync calls/ns and buffered/durable bytes, active transactions and MVCC chain/snapshot gauges. Enables transaction timing. Blocking execution includes WAL wait; the deferred single-statement path reports separate durability_commands/wait_ns/batches after SQL execution. Compare deltas and subtract nested spans instead of adding them. Sync statistics count runtime segment syncs, including segment rolls, but exclude open-time sync and final shutdown fsync. |

## Additional active controls (source audit, 2026-11-11)

| Variable | Effect |
| --- | --- |
| `TINYLAMB_DISABLE_COLUMNAR_TLCS_AGG` | If present, disables columnar TLCS aggregation for A/B comparisons. |
| `TINYLAMB_EXPR_FUZZ_REGRESSION_DIR` | Directory of expression fuzzer regression cases to replay. |
| `TINYLAMB_FORCE_COLOR` | If present, enables colored log output even when stderr is redirected. |
| `TINYLAMB_LOG_LEVEL` | Minimum log level at process startup (default: TRACE). Accepts TRACE, DEBUG, USER, INFO, NOTICE, WARN, ERROR, ALERT, and FATAL, case-insensitively. |
| `TINYLAMB_PIPELINE_METRICS` | If present, enables optional pipeline timing metrics. |
| `TINYLAMB_PLANCACHE_REGRESSION_DIR` | Directory of SQL plan-cache fuzzer regression cases to replay. |

| Variable | Meaning |
| --- | --- |
| `TINYLAMB_AUDIT_SPAWN_ENV` | Test fixture for `SpawnOverridesEnvironmentOnlyInChild` (`platform_test`): set in the parent, verified overridden only inside the spawned child. |
| `TINYLAMB_DEBUG_ORDERED` | Presence-based diagnostic: dumps the ordered join visit order and match-domain row counts to stderr (`executor/detail/build_input{,_joins}.cpp`). |
| `TINYLAMB_DISABLE_ARENA_MEMBERSHIP` | Disable membership construction from a cached base-relation arena. |
| `TINYLAMB_DISABLE_BUILD_RESIDUAL_HOIST` | Disable build-side residual hoisting in fused grouped joins. |
| `TINYLAMB_DISABLE_BULK_COUNT` | Disable bulk count updates in fused grouped joins. |
| `TINYLAMB_DISABLE_COMPACT_COUNT_LOG` | Disable the compact count log in fused grouped joins. |
| `TINYLAMB_DISABLE_COMPACT_PARTITION_AGG` | Disable compact per-partition aggregate state in the typed stream aggregate. |
| `TINYLAMB_DISABLE_COMPACT_STATISTICS` | Disable compact statistics collection in the typed stream aggregate. |
| `TINYLAMB_DISABLE_COUNT_PREFILTER` | Disable the count prefilter in fused grouped joins. |
| `TINYLAMB_DISABLE_COUNT_STAR_FUSION` | Disable COUNT(*) join fusion. |
| `TINYLAMB_DISABLE_DEFERRED_DIMENSION_PRELOAD` | Presence-based A/B hatch: disable deferred dimension-key preload ahead of a single large fact scan (`executor/detail/build_input.cpp`). |
| `TINYLAMB_DISABLE_DEFERRED_MULTI_PAYLOAD` | Presence-based A/B hatch: disable the deferred multi-payload join phase (`executor/detail/build_input.cpp`). |
| `TINYLAMB_DISABLE_EAGER_CTE_COST_GUARD` | Presence-based A/B hatch: disable speculative eager CTE materialization cost guard in `query/select_rewrites.cpp`. |
| `TINYLAMB_DISABLE_EXACT_ROLLUP` | Disable the exact integer grouping-sets rollup plan in typed column eval. |
| `TINYLAMB_DISABLE_FUSED_PARTITION_MERGE` | Disable the fused grouped-join partition merge. |
| `TINYLAMB_DISABLE_GROUPING_SET_EMIT_SPLIT` | Disable splitting grouped-set emission across contiguous group ranges in the typed stream aggregate. |
| `TINYLAMB_DISABLE_HAVING_FOLD` | Disable HAVING folding in relational aggregation. |
| `TINYLAMB_DISABLE_JOIN_REORDER` | Presence-based A/B hatch: restore strict syntactic FROM visit order instead of cost-based join reordering (`executor/detail/build_input.cpp`); results must be identical either way. |
| `TINYLAMB_DISABLE_ORDERED_COLUMN_PRUNE` | Presence-based A/B hatch: disable pruning of already-consumed columns between ordered join steps (`executor/detail/build_input.cpp`). |
| `TINYLAMB_DISABLE_PARALLEL_FUSED_EMIT` | Disable parallel fused-join emit for raw count/sum fusion. |
| `TINYLAMB_DISABLE_GROUP_OUTPUT_PUSH` | Any set value disables direct grouped-output batches into numeric hash-build/probe consumers. A/B switch for the bounded grouped-result transfer path; ordinary relational execution and spilling remain available. |
| `TINYLAMB_DISABLE_PACKED_GROUPING_DICT` | Presence-based A/B hatch: disable packed grouping dictionary optimization in `typed_stream_aggregate_compact`. |
| `TINYLAMB_DISABLE_PARALLEL_ANTI_PROBE` | Presence-based A/B hatch: force the anti-join probe to a single worker (`executor/detail/build_input_joins.cpp`). |
| `TINYLAMB_DISABLE_PARALLEL_BRANCHES` | Presence-based A/B hatch: disable parallel UNION/branch execution in `executor/relational.cpp`. |
| `TINYLAMB_DISABLE_PUSHED_SCALAR_AGG` | Presence-based A/B hatch: disable pushed scalar aggregation in `executor/relational.cpp`. |
| `TINYLAMB_DISABLE_RADIX_AGG` | Presence-based A/B hatch: disable radix aggregation in `fused_grouped_join.cpp`. |
| `TINYLAMB_BENCH_DUCKDB_EXTENSIONS` / `TINYLAMB_BENCH_DUCKDB_TOKEN` / `TINYLAMB_BENCH_ORACLE_PASSWORD` | Benchmark comparison suite (`benchmark/comparison/`) credentials and DuckDB extension path overrides. |
| `TINYLAMB_DISABLE_PREDICATE_TRANSFER` | Any set value disables both directions of grouped-key Bloom transfer across safe aggregate boundaries into base scans. Ordinary joins and grouped-output push remain active. |
| `TINYLAMB_DISABLE_REVERSE_TRANSFER` | Any set value disables the raw preserved-key seed pass into grouped build inputs; forward transfer from completed builds to the probe scan remains enabled. A/B switch for the second direction. |
| `TINYLAMB_DISABLE_PARALLEL_JOIN_RESIDUAL` | Disable bytecode-compiled probe residuals in the parallel hash join. |
| `TINYLAMB_DISABLE_PARALLEL_MEMBERSHIP` | Disable parallel membership set construction. |
| `TINYLAMB_DISABLE_PARALLEL_PROBE_RELEASE` | Disable early release of probe-side rows in the parallel hash join. |
| `TINYLAMB_DISABLE_PARTITIONED_GROUP_MERGE` | Disable the partitioned parallel group merge in relational aggregation. |
| `TINYLAMB_DISABLE_PK_ABSORPTION` | Presence-based A/B hatch: disable absorbing small PK-joined dimension sources into the ordered join build (`executor/detail/build_input.cpp`). |
| `TINYLAMB_DISABLE_RANK_BOUND` | Disable the window rank-bound early-exit optimization. |
| `TINYLAMB_DISABLE_RAW_PARTITION_MERGE` | Disable raw partition merge for fused grouped joins. |
| `TINYLAMB_DISABLE_RELATIVE_ORDERED_FILTERS` | Presence-based A/B hatch: disable materializing selective preserved dimensions before the fact scan in ordered joins (`executor/detail/build_input.cpp`). |
| `TINYLAMB_DISABLE_FUSED_STREAM_GROUPED_JOIN` | Disable streaming grouped joins. |
| `TINYLAMB_DISABLE_TRANSITIVE_KEY_FILTER` | Presence-based A/B hatch: disable transitive equality-key filter propagation across join inputs (`executor/detail/build_input.cpp`). |
| `TINYLAMB_EXPR_ROW_ORACLE` | Row-expression oracle selection: `0` disables, another present value enables; unset samples one quarter of seeds. |
| `TINYLAMB_FORCE_PLAIN` | Force the PAX encoder to choose plain encoding (presence). |
| `TINYLAMB_FORCE_SCHEME` | Force a named PAX encoding where supported by the chooser; unknown or unsupported names are ignored with a warning. |
| `TINYLAMB_FUZZ_CONTINUE` | Continue after saving a finding when nonempty and not `0`; default aborts. |
| `TINYLAMB_FUZZ_DB_DIR` | Directory for temporary scoped fuzzer databases. |
| `TINYLAMB_FUZZ_FINDINGS_DIR` | Additional directory receiving reproducer and report copies. |
| `TINYLAMB_FUZZ_TRACE_SQL` | Trace fuzzer SQL and SQL-engine execution diagnostics. |
| `TINYLAMB_FUZZ_VERBOSE` | Verbose oracle-fuzzer diagnostics. |
| `TINYLAMB_GOOGLESQL_BIN` | Parser executable fallback override (see frontend executable resolution). |
| `TINYLAMB_GOOGLESQL_ZYGOTE_SOCK` | Unix socket for the GoogleSQL parser zygote. |
| `TINYLAMB_GROUP_PHASES` | Presence-based diagnostic: prints per-phase timings (`[group]`, `[anti]`, `[typed-aggregate]`, `[partitioned-numeric]`) for grouping and anti-joins; numeric grouping separates partition, owner aggregation/local TopN, and finish. |
| `TINYLAMB_DISABLE_UNBOUNDED_DICTIONARY_AGG` | Presence-based A/B hatch: disable dictionary-code/frequency aggregation for GROUP BY without LIMIT, including derived-table/CTE producers. Existing LIMIT kernels remain enabled. |
| `TINYLAMB_DISABLE_PARTITIONED_NUMERIC_AGG` | Presence-based A/B hatch: retains the existing worker-local grouping and global merge instead of partition-owned numeric grouping/TopN. |
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

`TINYLAMB_DBG_CNT` was a retired no-op probe (formerly an empty conditional in
`plan/optimizer.cpp`); the reader was removed in #1701, so it now has no C++
reader at all and should be treated as a fully removed name.

Retired names removed from the active inventory: `TINYLAMB_BPT_CONC_FUZZ_SEED`, `TINYLAMB_CHECKPOINT_FLUSH_MS`, `TINYLAMB_CORRUPT_TAIL_PROBE`, `TINYLAMB_CRC_BITFLIP_PROBE`, `TINYLAMB_DISABLE_ANCHOR_VOID`, `TINYLAMB_DISABLE_READBUF_NOINIT`, `TINYLAMB_DISABLE_REDO_REINSERT_FLOOR`, `TINYLAMB_DISABLE_SEPARATOR_SWEEP`, `TINYLAMB_RECOVERY_TRACE`, `TINYLAMB_RECOVERY_WORKERS`, `TINYLAMB_TEST_UNDO_STOP_AFTER`, `TINYLAMB_WAL_FREE_WARN_BYTES`. They have no current C++ readers. `TINYLAMB_PAGE_POOL_BYTES` is listed above as retired-but-tolerated: no C++ code reads it.
