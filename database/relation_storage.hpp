#ifndef TINYLAMB_RELATION_STORAGE_HPP
#define TINYLAMB_RELATION_STORAGE_HPP

#include <bits/unique_ptr.h>

#include <cstdint>
#include <ostream>
#include <string_view>
#include <unordered_map>

#include "common/constants.hpp"
#include "database/page_storage.hpp"
#include "database/transaction_context.hpp"
#include "index/b_plus_tree.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

class PageManager;
class Table;
class Transaction;
class Schema;
class IndexSchema;
class TableStatistics;
class PageStorage;

class RelationStorage {
 public:
  explicit RelationStorage(std::string_view dbname);

  // Transaction Begin() { return storage_.Begin(); }
  TransactionContext BeginContext() { return {storage_.Begin(), this}; }

  StatusOr<Table> CreateTable(TransactionContext& ctx, const Schema& schema);

  Status CreateIndex(TransactionContext& ctx, std::string_view schema_name,
                     const IndexSchema& idx);

  [[maybe_unused]] void DebugDump(Transaction& txn, std::ostream& o);

  StatusOr<TableStatistics> GetStatistics(TransactionContext& txn,
                                          std::string_view schema_name);

  Status UpdateStatistics(TransactionContext& txn, std::string_view schema_name,
                          const TableStatistics& ts);
  Status RefreshStatistics(TransactionContext& txn,
                           std::string_view schema_name);

  PageStorage* GetPageStorage() { return &storage_; }

 private:
  friend class TransactionContext;
  StatusOr<Table> GetTable(TransactionContext& ctx,
                           std::string_view schema_name);

  // Persistent { Name => Table } storage.
  BPlusTree catalog_;

  // Persistent { Name => TableStatistics } storage.
  BPlusTree statistics_;

  PageStorage storage_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_RELATION_STORAGE_HPP
