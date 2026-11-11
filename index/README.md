# Index

`IndexSchema` describes key and included columns plus uniqueness/versioning.
`Index` stores that schema and the identifier of its `PageManager` LSM tree.

`LsmIndex` implements string-key Read/Insert/Update/Delete and range scans.
`BPlusTree` is a compatibility alias, not a separate B+Tree implementation.
`PageManager` owns mutable/committed maps, immutable runs, compaction, and
snapshot/WAL persistence. These runs are not independently reopened by
scanning BlobFile filenames; see [page format](../docs/page_format.md).

`LsmIndexIterator` (`BPlusTreeIterator` alias) materializes an ordered range
snapshot. Partition boundaries are key-count based virtual leaf boundaries,
currently 128 keys per slice, not physical leaf-page boundaries. There is no
public `LSMView` class.

`IndexScanIterator` combines index keys with table/MVCC visibility and optional
included columns. Ordinary scans resolve row positions through the heap;
index-only reads require the guarded visibility/coverage conditions documented
in its header. This file is compiled into the table layer to avoid an index ↔
table dependency cycle.

Tests: `lsm_index_test`, `index_scan_iterator_test`.
