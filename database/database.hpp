/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TINYLAMB_DATABASE_HPP
#define TINYLAMB_DATABASE_HPP

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "database/catalog_reader.hpp"
#include "database/page_storage.hpp"
#include "database/transaction_context.hpp"
#include "index/lsm_index.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

class Table;
class Transaction;
class Schema;
class IndexSchema;
class TableStatistics;
class PageStorage;
class Function;

class Database final : public CatalogReader {
 public:
  // Opens (creating when needed) the database files and initializes the
  // system relations. Startup failures surface as Status.
  static StatusOr<std::unique_ptr<Database>> Create(
      std::string_view dbname, size_t wal_sync_ms = 1,
      bool preallocate_new_wal = false);

  // Filesystem bookkeeping: Create() records the database's base name after
  // successful initialization; a failed Create() must not leave a name that
  // could make a later process's same-named files eligible for this sweep.
  // RemoveCreatedDatabaseFiles() unlinks <name>.db (plus companion block/tlcs
  // files), the WAL segment series, and legacy .last_checkpoint side-cars.
  // The checkpoint state itself is aggregated into the <name>.db snapshot;
  // the side-cars are only removed to clean up files from older versions
  // (see recovery/AGENTS.md). Missing files are ignored. The test-workspace
  // cleanup environment calls it at binary exit so throwaway databases never
  // outlive the run; benchmarks and servers keep files unless they call it
  // themselves.
  // Names are never unregistered: a reopen cycle (EmulateCrash + Create)
  // keeps the same base name and stays covered.
  static void RemoveCreatedDatabaseFiles();
  // Test cleanup variant: bases present before the test binary started must
  // survive even if the process reopened them through Create().
  static void RemoveCreatedDatabaseFiles(
      const std::unordered_set<std::string>& preserve_bases);
  Database(const Database&) = delete;
  Database& operator=(const Database&) = delete;
  Database(Database&&) = delete;
  Database& operator=(Database&&) = delete;
  // Drops the columnar caches of every table incarnation: compiled plans may
  // outlive the database while still holding Table copies.
  ~Database() override;

  TransactionContext BeginContext() { return {storage_->Begin(), this}; }
  TransactionContext BeginReadOnlyContext() {
    return {storage_->BeginReadOnly(), this};
  }
  void SetSynchronousCommit(bool enabled) {
    storage_->tm_->SetSynchronousCommit(enabled);
  }
  void SetCommitDrivenWalSync(bool enabled) {
    storage_->logger_->SetCommitDrivenSync(enabled);
  }
  [[nodiscard]] bool FixedSizeWal() const {
    return storage_->logger_->FixedSizeSegments();
  }
  Status WaitForCommitDurable(lsn_t target) {
    return storage_->tm_->WaitForWalDurable(target);
  }
  // Successful fdatasync frontier, including bytes beyond a waiter's target.
  // BufferedLSN / CommittedLSN are not durability proofs.
  [[nodiscard]] lsn_t DurableWalLSN() const {
    return storage_->logger_->DurableLSN();
  }
  void SetTransactionMetricsEnabled(bool enabled) {
    storage_->tm_->SetMetricsEnabled(enabled);
  }
  // Deadlock policy for write-intent acquisition. See
  // TransactionManager::DeadlockPolicy for the trade-offs.
  using DeadlockPolicy = TransactionManager::DeadlockPolicy;
  void SetDeadlockPolicy(DeadlockPolicy policy) {
    storage_->tm_->SetDeadlockPolicy(policy);
  }
  [[nodiscard]] DeadlockPolicy GetDeadlockPolicy() const {
    return storage_->tm_->GetDeadlockPolicy();
  }
  [[nodiscard]] TransactionRuntimeStats TransactionStats() const {
    return storage_->tm_->RuntimeStats();
  }
  [[nodiscard]] bool FailStopped() const {
    return storage_->tm_->FailStopped();
  }
  [[nodiscard]] uint64_t CacheHits() const noexcept;
  [[nodiscard]] uint64_t CacheMisses() const noexcept;
  [[nodiscard]] size_t PinnedPageCount() const noexcept;

  StatusOr<Table> CreateTable(TransactionContext& ctx, const Schema& schema);

  Status DropTable(TransactionContext& ctx, std::string_view schema_name);

  // Materialized-view definition records, persisted in the functions tree
  // under a reserved key prefix.  The stored value is the defining SELECT
  // text; `Put` overwrites (refresh/recreate), `Get` reports kNotExists for
  // anything that is not a materialized view.
  static constexpr std::string_view kMaterializedViewKeyPrefix =
      "__matview\x1e";
  Status PutMaterializedViewDefinition(TransactionContext& ctx,
                                       std::string_view name,
                                       std::string_view query_text);
  StatusOr<std::string> GetMaterializedViewDefinition(TransactionContext& ctx,
                                                      std::string_view name);
  Status DeleteMaterializedViewDefinition(TransactionContext& ctx,
                                          std::string_view name);

  // (Non-materialized) view definition records, persisted like the
  // materialized ones under their own reserved prefix, plus a newline-joined
  // name index under `kViewIndexKey` so a reopen can re-register every view
  // without a range-scan API.  Definitions store the defining SELECT text;
  // expansion happens at FROM-reference time (session ViewRegistry).
  static constexpr std::string_view kViewKeyPrefix = "__view\x1e";
  static constexpr std::string_view kViewIndexKey = "__view\x1e\x01__index";
  Status PutViewDefinition(TransactionContext& ctx, std::string_view name,
                           std::string_view query_text,
                           bool replace_existing = false);
  StatusOr<std::string> GetViewDefinition(TransactionContext& ctx,
                                          std::string_view name);
  Status DeleteViewDefinition(TransactionContext& ctx, std::string_view name);
  StatusOr<std::vector<std::string>> ListViewDefinitions(
      TransactionContext& ctx);

  Status CreateIndex(TransactionContext& ctx, std::string_view schema_name,
                     const IndexSchema& idx);

  // ALTER TABLE ... RENAME TO: a catalog-level operation (the Table value
  // is re-serialized under the new canonical name; data and index pages are
  // untouched).  Refuses when another table's FOREIGN KEY references the
  // old name (the constraint stores the parent table name as a string) or
  // when the target name already exists (case-insensitive).
  Status RenameTable(TransactionContext& ctx, std::string_view from_name,
                     std::string_view to_name);

  // ALTER TABLE ... RENAME COLUMN: rewrites the column name inside the
  // catalog-serialized schema.  Statistics entries are column-INDEX keyed
  // and stay valid; cached plans are invalidated by the schema epoch bump.
  Status RenameColumn(TransactionContext& ctx, std::string_view table_name,
                      std::string_view old_column, std::string_view new_column);

  StatusOr<Function> GetOrAddFunction(TransactionContext& ctx,
                                      std::string_view function_name,
                                      int argument_count) override;

  [[maybe_unused]] void DebugDump(Transaction& txn, std::ostream& o);

  friend std::ostream& operator<<(std::ostream& o, const Database& db);

  StatusOr<TableStatistics> GetStatistics(
      TransactionContext& ctx, std::string_view schema_name) override;

  Status UpdateStatistics(TransactionContext& ctx, std::string_view schema_name,
                          const TableStatistics& ts);

  Status RefreshStatistics(TransactionContext& ctx,
                           std::string_view schema_name);

  StatusOr<bool> TableExists(TransactionContext& ctx,
                             std::string_view name) override;
  StatusOr<Table> GetTable(TransactionContext& ctx,
                           std::string_view schema_name) override;

  // #1100: lightweight identity probe for compiled-plan reuse.  True while
  // `state` is still the incarnation object bound to `canonical_name` in the
  // per-database map and no drop has begun.  Replaces a
  // catalog GetTable (LSM read + full Table deserialization) per retained
  // table per plan-cache hit; DDL swaps or retires the map binding, and
  // epoch-keyed cache lookups cover the catalog-only rewrites.
  [[nodiscard]] bool IsCurrentIncarnation(
      std::string_view canonical_name,
      const std::shared_ptr<TableIncarnation>& state) const;

  // Catalog table names in ascending key order.
  std::vector<std::string> ListTables(TransactionContext& ctx);

  void EmulateCrash();

  // Forces a fuzzy checkpoint now (the automatic compaction/checkpoint worker
  // is far too slow to matter in a test/fuzz iteration).  No page latch may be
  // held on this thread.  Returns the begin-checkpoint LSN recovery will resume
  // from.
  StatusOr<lsn_t> WriteCheckpoint(bool allow_auto_compaction = true);

  // Compacts `schema_name`'s committed RowPage chain into an immutable `.tlcs`
  // columnar file alongside `<db>.db` and replaces the in-memory pages with
  // columnar stub pages bound to the backing `TlcsFile`.
  Status CompactTableToTlcs(TransactionContext& ctx,
                            std::string_view schema_name,
                            bool write_checkpoint = true,
                            bool refresh_statistics = true,
                            bool non_blocking_quiescence = false);

  Status CompactAllTablesToTlcs();

  bool AutoCompactTableForAnalyze(TransactionContext& ctx,
                                  std::string_view schema_name);

  void DeleteAll();

  // Monotonic catalog/statistics generation used by the SQL engine's
  // compiled-plan cache (Phase 2-1). Every successful DDL or statistics
  // refresh advances it; cached plans stamped with an older epoch are stale.
  [[nodiscard]] uint64_t SchemaEpoch() const noexcept {
    return schema_epoch_.load(std::memory_order_acquire);
  }
  [[nodiscard]] uint64_t CatalogEpoch() const noexcept override {
    return SchemaEpoch();
  }
  uint64_t BumpSchemaEpoch() noexcept {
    return schema_epoch_.fetch_add(1, std::memory_order_acq_rel) + 1;
  }
  void BumpCatalogEpoch() noexcept override { BumpSchemaEpoch(); }

 private:
  friend class TransactionContext;

  // Process-global epoch base so every Database instance owns a disjoint
  // epoch range (see schema_epoch_ below). Exhausting the 64-bit space is a
  // recoverable Status now: aborting here would kill the embedding process
  // from a library constructor boundary. Called by Create() before the
  // database is published to the caller.
  [[nodiscard]] static StatusOr<uint64_t> TryNextDatabaseEpochBase();

  // Persistent { Name => Table } storage.
  BPlusTree catalog_;

  // Persistent statistics: table name → row-count meta, and
  // table name + NUL + column index → ColumnStats.  Splitting by column
  // keeps each catalog value small (LsmIndex has no value-size limit).
  BPlusTree statistics_;

  // Persistent { Name => Function } storage.
  BPlusTree functions_;

  // Guards the Read→Insert sequences against catalog entries (CreateTable,
  // GetOrAddFunction). Held across page allocation so a concurrent duplicate
  // registration can no longer allocate pages it would never reclaim.
  std::mutex catalog_mu_;

  mutable std::mutex table_incarnations_mu_;
  // T4634: transparent key lookup lets lookups probe with a string_view,
  // so hot paths (GetTable, IsCurrentIncarnation) do not materialize a
  // std::string while holding table_incarnations_mu_; only inserts
  // allocate. (Same root cause as T4327, which covered the other site.)
  struct StringViewNameHash {
    using is_transparent = void;
    [[nodiscard]] size_t operator()(std::string_view sv) const noexcept {
      return std::hash<std::string_view>{}(sv);
    }
  };
  mutable std::unordered_map<std::string, std::shared_ptr<TableIncarnation>,
                             StringViewNameHash, std::equal_to<>>
      table_incarnations_;
  mutable std::mutex tlcs_attach_mu_;
  std::unordered_set<page_id_t> tlcs_probed_pids_;

  StatusOr<std::string> ReadTableCatalogEntry(TransactionContext& ctx,
                                              std::string_view name);
  void MaybeAttachBackingTlcs(Table& table);

  // Compiled-plan invalidation epoch; see SchemaEpoch()/BumpSchemaEpoch().
  // Starts from a process-global counter so two Database instances never
  // share an epoch: plan caches stamp entries with the epoch and a fresh
  // database starting at 0 would otherwise replay artifacts that reference
  // the destroyed predecessor's tables.
  // Assigned by Create() from TryNextDatabaseEpochBase(); a default-constructed
  // Database never observes an uninitialized epoch.
  std::atomic<uint64_t> schema_epoch_{0};
  std::atomic<bool> auto_tlcs_in_progress_{false};

  bool TryAutoCompactTable(TransactionContext& ctx,
                           std::string_view schema_name);

  std::unique_ptr<PageStorage> storage_;

  Database(std::string_view dbname, std::unique_ptr<PageStorage> storage);
};

}  // namespace tinylamb

#endif  // TINYLAMB_DATABASE_HPP
