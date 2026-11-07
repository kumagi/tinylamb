# `index/` — Layer 4 LSM-Tree Index

Persistent unbounded `{string => string}` LSM-Tree index structures over
`page/`. Depends only on storage and below. Note the cycle-avoidance split:
`index_scan_iterator.*` lives here but compiles into `tinylamb_table`.

## Key files

- `lsm_index.{hpp,cpp}` — `LsmIndex` and `LsmIndexIterator` (with `BPlusTree` /
  `BPlusTreeIterator` aliases). Backed by `PageManager`'s LSM-Tree store
  (`MemTable` + immutable `LsmSortedRun`s + k-way merge compaction), supporting
  arbitrary key and value sizes without page-size or key-size limits.
- `index_schema.{hpp,cpp}` (`IndexMode{kNonUnique/kUnique/kVersionedUnique}`),
  `index.{hpp,cpp}` (`Index{schema, root pid}`).
- `index_scan_iterator.{hpp,cpp}` — unified scan bound to `Table+Index+Txn`:
  fwd/rev, composite `vector<Value>` keys, non-unique offsets, `IndexOnly`
  key/include access (`TryReadIndexOnly`).

Test: `./build/lsm_index_test`, `index_scan_iterator_test`.
