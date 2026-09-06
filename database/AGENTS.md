# `database/` — Layer 6 catalog + storage bundle + txn context

Facade over everything below: DDL/catalog, statistics persistence, and the
`TransactionContext` handle that upper layers pass around.

## Key files

- `database.{hpp,cpp}` (`Database : CatalogReader`) —
  `CreateTable/DropTable/CreateIndex/GetTable/ListTables`,
  `GetOrAddFunction`, `Get/Update/RefreshStatistics`, `BeginContext` /
  `BeginReadOnlyContext`, `EmulateCrash`, `DeleteAll`. Owns 3 B+Trees
  (catalog/statistics/functions) + `PageStorage` + `catalog_mu_` +
  `schema_epoch_`. DDL and stats updates `BumpSchemaEpoch` (plan-cache
  staleness; per-DB disjoint epoch ranges so a new DB never replays a
  destroyed predecessor's plans). `catalog_mu_` is held across the
  read→insert chain to prevent orphan page allocation on double registration.
- `page_storage.{hpp,cpp}` — owns
  `Logger/PageManager/RecoveryManager/TransactionManager/CheckpointManager`.
  **Member declaration order is load-bearing**: logger first, checkpoint
  manager last (background worker joins before members it touches die).
- `transaction_context.{hpp,cpp}` — `{Transaction, CatalogReader*,
  table/stats cache, evaluation_context_, execution_runtime_}`. Caches
  `shared_ptr<Table/Stats>` per txn; move-assign drops the cache.
- `catalog_reader.hpp` — minimal capability interface
  (`CatalogEpoch/GetTable/GetStatistics/GetOrAddFunction`) so executor state
  cannot reach DDL/crash/filesystem. `EvaluationContext` is forward-declared
  only — **no include edge to `expression/`** (allowlist V1 depends on this).

Test: `./build/catalog_test`, `database_extra_test`.
