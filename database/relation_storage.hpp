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
