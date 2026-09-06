# `executor/` — Layer 10 physical operators

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
  `index_only_scan`, `bitmap_scan`, `tid_scan`, `minmax_index`, `zone_map`,
  `remote_scan`, `analyze_scan`, `cardinality_probe`, `constant_executor`.
- Joins: `hash_join` (+ `hash_join_mode.hpp` cost helper), `parallel_hash_join`,
  `merge_join`, `parallel_merge_join`, `nested_loop_join`,
  `batch_nested_loop_join`, `cross_join`, `index_join`, `interval_join`,
  `as_of_join`, `apply` (correlated), `join_kind.hpp`.
- Sort/agg: `sort`, `topn`, `partial_sort`, `incremental_sort`, `pdqsort`,
  `spill_file`, `aggregation`, `parallel_aggregation`, `partial_aggregate`,
  `grouping_sets`, `two_phase_distinct_agg`, `distributed_agg_finalize`,
  `dictionary_batch_aggregation`, `distinct`, `skip_scan_distinct`.
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
`data_chunk_test`, `distinct_test`, `merge_append_test`,
`query_scheduler_test`, `query_memory_test`, `spill_file_test`,
`executor_extra_test`.
