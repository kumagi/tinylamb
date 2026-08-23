# PagePool concurrency (Phase 3a)

## Problem

`PagePool::GetPage` held a single `std::mutex pool_latch` across **cache hits**,
so concurrent readers of hot pages serialized on one lock. That blocks
multi-core scan scale (and made Relational morsel parallelism hang / regress).

## Chosen approach (first step)

**Hit path uses `std::shared_mutex` shared lock**:

1. `shared_lock` → lookup → `pin_count++` → unlock → acquire page latch.
2. LRU `Touch` is **best-effort**: `try_lock` unique; if contended, skip
   reordering (approximate LRU). Misses / eviction still take a unique lock.

This matches `improvement.md` §4.2 option (c) and unblocks read scaling without
a full shard redesign.

## Follow-ups

- **Sharding** (`page_id % N` pools) if shared_mutex still contends on the
  map/list structure.
- **Miss path**: load under exclusive, then return shared page latch when
  `shared=true` was requested (done in M3.3).
- **I/O**: `pread`/`pwrite` on a dedicated fd (M3.4).
- Re-enable Relational morsel parallel fill only after
  `ConcurrentEvictionAcrossThreads` is fixed/green (enabled in M3.6).
- **Relational `TryParallelTableScan`** (M3.6): active only for read-only,
  unfiltered scans without integer peeks or key filters. TPC-H predicates stay
  on the serial peek path.

## Measurement

Record `pg_read_benchmark` (or equivalent multi-client read) clients=1..N
before/after in `BenchmarkHistory.md` when that binary is next run.
