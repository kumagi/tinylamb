# Page storage

`PageManager` owns in-memory row blocks and LSM trees. `RowPage` stores
variable-length serialized rows in logical slots, with committed baseline
images and per-slot LSNs. A block has at most 4096 slots. New writes also obey
the block byte admission budget (`RowPage::kBlockBudgetBytes`); row storage
uses dynamic strings rather than a fixed on-disk page image.

`PageRef` holds a page content latch through RAII. `PageUnlock` releases it;
raw page pointers must not be used across release. This is distinct from MVCC
write intents owned by transactions. There is no PagePool eviction/pinning
protocol and no current B+Tree BranchPage/LeafPage/MetaPage/FreePage family.

`WriteFuzzyCheckpoint` persists committed row blocks and LSM tree state into
one checksummed `<db>.db` snapshot. Startup loads it and replays committed
WAL records. See [page format](../docs/page_format.md),
[recovery invariants](../docs/recovery_invariants.md), and
[lock ordering](../docs/lock_order.md).

Abort restores the previously stored physical image even when another
committed write has consumed space freed by a shrink/delete. The normal
new-write byte budget cannot reject that restoration; it is an admission
limit, not a limit on the rollback image.

Tests: `page_manager_test` and `recovery/checkpoint_manager_test.cpp`.
