# `index/` — Layer 4 Foster B+Tree + WiscKey LSM

Persistent `{string => string}` structures over `page/`. Depends only on
storage and below. Note the cycle-avoidance split: `index_scan_iterator.*`
lives here but compiles into `tinylamb_table` (see `table/AGENTS.md`).

## Key files

- `b_plus_tree.{hpp,cpp}` — Foster B+Tree. Splits link the new child as a
  foster chain instead of updating the parent avalanche-style; readers follow
  the chain, only writers absorb it. `hint_leaf` skips the root-to-leaf walk
  when the previous landing leaf still routes the key (verified against root
  + fences). Reclaim via `ReclaimIfOrphaned` (row-less + foster-less only).
- `b_plus_tree_iterator.{hpp,cpp}` — range scan cursor (asc/desc,
  `PositionAtOrAbove`/`PositionBelow`).
- `index_schema.{hpp,cpp}` (`IndexMode{kNonUnique/kUnique/kVersionedUnique}`),
  `index.{hpp,cpp}` (`Index{schema, root pid}` only — no row data).
  `IndexValueType{RowPosition | Row include}` lives here; `Table::IndexValueType`
  is a compat alias, disk encoding unchanged.
- `index_scan_iterator.{hpp,cpp}` — unified scan bound to `Table+Index+Txn`:
  fwd/rev, composite `vector<Value>` keys, non-unique offsets, `IndexOnly`
  key/include access. **CMake builds it into `tinylamb_table`** to keep the
  `table → index` edge one-directional.
- `lsm_tree.{hpp,cpp}` + `lsm_detail/` — WiscKey-style LSM (see
  `lsm_detail/AGENTS.md`).

Test: `./build/b_plus_tree_test`, `b_plus_tree_iterator_test`,
`lsm_tree_test`, `index_scan_iterator_test`, `b_plus_tree_concurrent_test`.
Fuzz: `index/*_fuzzer.cpp` (+ `_replay`, needs `TINYLAMB_ENABLE_FUZZ`).
