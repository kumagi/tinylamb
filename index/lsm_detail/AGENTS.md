# `index/lsm_detail/` — WiscKey LSM internals

Index-separated keys + blob-separated values. Tuned for TPC-C hot sets ×
TPC-H large scans (scan resistance).

## Key files

- `sorted_run.{hpp,cpp}` — immutable sorted run (index only): `Entry`,
  `Iterator`, `Construct`/`FlushInternal` (write + fsync, delete on failure).
  Small values inline; over-threshold values become blob references.
- `blob_file.{hpp,cpp}` — append-only value log. **`Sync` (flush +
  `WaitForDurable`) must complete before the referencing run is registered**,
  else recovery quarantines the run.
- `lsm_view.{hpp,cpp}` — generation-descending snapshot; newest generation
  wins, tombstones skipped during iteration.
- `cache.{hpp,cpp}` — read cache (fuzzed; see `cache_fuzzer*`).
- `lsm_tree_bench.cpp` — micro-benchmark (explicit target only).

## Concurrency rules

`mem_tree_lock_` → `file_tree_lock_` fixed order; `sync_lock_` serializes
whole `Sync` (prevents double-flush and delete-resurrection from a value
landing in a newer run than its tombstone). `RestoreRuns` recovers in
numeric generation order; corrupt/duplicated runs are quarantined, not
trusted.

Test: `./build/blob_file_test`, `lsm_view_test`, `cache_test`,
`sorted_run_test`, `cache_concurrent_test`.
