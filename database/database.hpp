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
#include <mutex>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "database/catalog_reader.hpp"
#include "database/page_storage.hpp"
#include "database/transaction_context.hpp"
#include "index/b_plus_tree.hpp"
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
  explicit Database(std::string_view dbname, size_t wal_sync_ms = 1);

  // Transaction Begin() { return storage_.Begin(); }
  TransactionContext BeginContext() { return {storage_.Begin(), this}; }
  TransactionContext BeginReadOnlyContext() {
    return {storage_.BeginReadOnly(), this};
  }
  void SetSynchronousCommit(bool enabled) {
    storage_.tm_.SetSynchronousCommit(enabled);
  }
  void SetTransactionMetricsEnabled(bool enabled) {
    storage_.tm_.SetMetricsEnabled(enabled);
  }
  [[nodiscard]] TransactionRuntimeStats TransactionStats() const {
    return storage_.tm_.RuntimeStats();
  }

  StatusOr<Table> CreateTable(TransactionContext& ctx, const Schema& schema);

  Status DropTable(TransactionContext& ctx, std::string_view schema_name);

  Status CreateIndex(TransactionContext& ctx, std::string_view schema_name,
                     const IndexSchema& idx);

  StatusOr<Function> GetOrAddFunction(TransactionContext& ctx,
                                      std::string_view function_name,
                                      int argument_count) override;

  [[maybe_unused]] void DebugDump(Transaction& txn, std::ostream& o);

  friend std::ostream& operator<<(std::ostream& o, const Database& db);

  StatusOr<TableStatistics> GetStatistics(TransactionContext& ctx,
                                          std::string_view schema_name) override;

  Status UpdateStatistics(TransactionContext& ctx, std::string_view schema_name,
                          const TableStatistics& ts);

  Status RefreshStatistics(TransactionContext& ctx,
                           std::string_view schema_name);

  StatusOr<Table> GetTable(TransactionContext& ctx,
                           std::string_view schema_name) override;

  // Catalog table names in ascending key order.
  std::vector<std::string> ListTables(TransactionContext& ctx);

  void EmulateCrash();

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

 private:
  friend class TransactionContext;

  // Process-global epoch base so every Database instance owns a disjoint
  // epoch range (see schema_epoch_ below).
  static uint64_t next_database_epoch_base() noexcept {
    static std::atomic<uint64_t> base{0};
    return base.fetch_add(1ULL << 32, std::memory_order_acq_rel) + 1;
  }

  // Persistent { Name => Table } storage.
  BPlusTree catalog_;

  // Persistent statistics: table name → row-count meta, and
  // table name + NUL + column index → ColumnStats.  Splitting by column
  // keeps each B+tree value under the leaf entry size limit.
  BPlusTree statistics_;

  // Persistent { Name => Function } storage.
  BPlusTree functions_;

  // Guards the Read→Insert sequences against catalog entries (CreateTable,
  // GetOrAddFunction). Held across page allocation so a concurrent duplicate
  // registration can no longer allocate pages it would never reclaim.
  std::mutex catalog_mu_;

  // Compiled-plan invalidation epoch; see SchemaEpoch()/BumpSchemaEpoch().
  // Starts from a process-global counter so two Database instances never
  // share an epoch: plan caches stamp entries with the epoch and a fresh
  // database starting at 0 would otherwise replay artifacts that reference
  // the destroyed predecessor's tables.
  std::atomic<uint64_t> schema_epoch_{next_database_epoch_base()};

  PageStorage storage_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_DATABASE_HPP
