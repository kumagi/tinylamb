# `executor/` — Layer 10 (with `plan/`) physical operators

Volcano `Next(Row*, RowPosition*)` + batched `NextBatch(DataChunk*,
max_rows = kDefaultVectorSize = 1024)` (`executor_base.hpp`;
`Executor = shared_ptr<ExecutorBase>`). Morsel-driven parallel scans/joins/
aggregations with disk spilling. CMake target `tinylamb_executor` also covers
`plan/`, `executor/detail/`, and `query/query_data.cpp` — cross-include
inside that set is build-free, but `check_layering.py` still ranks them.

## Key files

- `relational_factory.cpp` + `relational.{hpp,cpp}` — **the only place that
  implements `Plan::EmitExecutor` bodies** (V4 split). New physical operator
  = new `Plan` node + implementation rule + factory wiring here.
- Scans: `full_scan`, `chunked_scan`, `parallel_scan`, `index_scan`,
  `index_only_scan`, `parallel_index_scan` (morsel-driven parallel index
  scan over B+Tree-leaf-partitioned key ranges, morsel-ordered handoff),
  `bitmap_scan`, `tid_scan`, `minmax_index`, `zone_map`, `remote_scan`,
  `analyze_scan`, `cardinality_probe`, `constant_executor`, `lsm_scan`
  (streams an LSM/WiscKey tree's live key/value pairs; `LSMTree::GetView()`
  snapshot taken at construction).
  The serial `IndexScan` prefetches its upcoming heap pages with a look-ahead
  cursor (`TINYLAMB_DISABLE_INDEX_SCAN_PREFETCH`); the index-only visibility
  map is `IndexScanIterator::TryReadIndexOnly`.
  `multi_index_covering_scan` intersects two or more index ranges BY ROW
  POSITION and assembles each output row from the index images alone
  (`TryReadIndexOnly`) when the entry's page carries no version chains — the
  case no single index covers, since it needs two indexes' stored columns at
  once. A position on a chain-bearing page, or absent from any participant's
  range, is dropped; the surviving rows are rechecked against the predicate
  on the covered columns, so the intersection is a superset filter and never a
  shortcut. Like `BitmapHeapScan` it materializes the position set, and the
  plan orders participants narrowest-first so the table starts from the
  smallest side.
- Joins: `hash_join` (+ `hash_join_mode.hpp` cost helper; `EnableDynamicFilter`
  drains the build side first and publishes a `RuntimeFilterGroup` through
  `ExecutorBase::AttachRuntimeFilter`, falling back to filtering the probe side
  itself when the child cannot prune; `EnableSemiReduction` builds the probe
  side's key domain first and cuts an existence-only build down to one row per
  distinct key inside it — semi/anti/null-aware-anti only, with a domain-cap
  fallback to the unrestricted intake),
  `parallel_hash_join`,
  `merge_join`, `parallel_merge_join`, `nested_loop_join`,
  `batch_nested_loop_join`, `cross_join`, `lookup_join`,
  `late_materialization_join`, `interval_join`, `as_of_join`, `apply`
  (correlated), `join_kind.hpp`.
- Sort/agg: `sort`, `topn`, `partial_sort`, `incremental_sort`, `pdqsort`,
  (every sort-shaped operator resolves its keys through
  `type/collation.hpp`'s `SortKeyOrder` / `EqualUnderCollation`; `sort.cpp`
  folds a collated key *before* the memcomparable encoding so the in-memory
  permutation, the spilled runs and the external merge windows share one
  encoding, and `ColumnVector` keeps a VARCHAR column's collator so a
  collated value survives the batch path),
  `spill_file`, `aggregation`, `grouped_aggregate`, `parallel_aggregation`,
  `partial_aggregate`,
  `grouping_sets`, `two_phase_distinct_agg`, `distributed_agg_finalize`,
  `dictionary_batch_aggregation`, `distinct`, `skip_scan_distinct`.
  `grouped_aggregate.{hpp,cpp}` is the physical half of the three GROUP BY
  shapes (`plan/aggregation_plan.hpp`): `HashAggregateExecutor` (one hash table
  over the group keys; when the table outgrows `GroupedAggregateOptions::
  memory_budget_bytes` the rows read so far are split across
  `spill_partition_count` files by key hash and re-grouped one partition at a
  time, so every group stays inside one partition), `SortAggregateExecutor`
  (a `SortExecutor` over the group keys, then one pass over the equal runs, so
  the working set is one group plus the sort's window), and
  `StreamAggregateExecutor` (one group resident, no table and no sort — only
  correct over an input that already arrives in group-key order, which
  `require_ordered_input` makes it verify instead of assuming). The two grouped
  algorithms are separated by state as well as by order, and both expose it as
  `PeakResidentGroups()`: the hash shape's peak is the most groups one of its
  group tables held (distinct keys when resident, the largest single partition
  after a spill), the sort shape's is always 1 (the run being folded). Their
  knobs are disjoint too — the factory hands the worker count to the hash shape
  only, the run threshold to the sort shape only, and the order check to the
  stream shape only. All three share
  `AggregateGroupTable` (canonicalized group key -> slot, one accumulator per
  (group, aggregate)), the "keys then aggregates" output row, and HAVING as a
  predicate over that row; `AggregateAccumulator` is the ground truth for the
  payload, so the shapes agree with the relational path row for row. The hash
  shape folds its input in parallel only for a payload every worker can read
  positionally (plain column keys and arguments, no FILTER/HAVING modifier/
  ORDER BY/trailing args) and refuses the worker count otherwise; a worker build
  unions private tables, so `GroupCapacity()` bounds the serial build and the
  partition spill, not the worker build. A failure
  anywhere is the shape's own `Status` with no partial group set. A keyless
  payload keeps the historical scalar operators (`AggregationExecutor` /
  `ParallelAggregationExecutor`) — the factory routes all three shapes there.
  `SortExecutor::Options{spill_mode, memory_budget_bytes, merge_window_rows,
  spill_threshold_rows}` is the physical form of the plan's `SortMemoryPolicy`
  (`BufferedSortPlan` / `ExternalMergeSortPlan`, see `plan/sort_plan.hpp`):
  `kBuffered` never *plans* a spill and only falls back to the internal budget
  selection, `kExternalMerge` writes a sorted run once `spill_threshold_rows`
  is reached (the tail run too) and merges through `merge_window_rows`-row
  windows, and the default 4-argument constructor stays `kAuto` for the
  historical behavior. A mis-estimated shape must never change the answer, so
  every mode keeps the internal spill selection as the last resort.
- Window: `window` (physical operator wired from `WindowPlan::EmitExecutor`)
  over `detail/window_eval` partition/frame evaluation.
- DML/other: `insert`, `update`, `delete`, `truncate`, `merge`, `returning`,
  `values`, `generate_series`, `unnest`, `recursive_cte`, `set_operation`,
  `merge_append`, `limit`, `max1_row`, `minmax_index`, `materialize`,
  `exchange`, `pipeline_breaker.hpp`.
- Infra: `data_chunk` (+ `selection_vector`), `query_scheduler`,
  `query_memory` (+ `operator_memory`, `numa_arena`), `vectorized_expression`,
  `simd_comparison`.

## Rules for new operators

1. Add the `Plan` node in `plan/` (logical info only) + implementation rule.
2. Implement the executor + wire `EmitExecutor` in `relational_factory.cpp`.
3. Batch-first: implement `NextBatch`; keep row-at-a-time `Next` consistent.
4. `executor/{apply,recursive_cte,unnest}.cpp + relational.cpp ->
   query/statement.hpp` is V3' debt — read-only, do not widen.

Test: `./build/executor_test`, `relational_regression_test`,
`data_chunk_test`, `distinct_test`, `merge_append_test`, `sort_collation_test`,
`sort_shape_executor_test`, `grouped_aggregate_test`,
`late_materialization_join_test`, `hash_semi_reduction_test`,
`multi_index_covering_scan_test`,
`query_scheduler_test`, `query_memory_test`, `spill_file_test`,
`executor_extra_test`.
