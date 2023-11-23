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

#include <functional>

#include "database/page_storage.hpp"
#include "relation_storage.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class IndexSchema;

class Database {
 public:
  explicit Database(std::string_view dbname) : relations_(dbname) {}
  TransactionContext BeginContext();

  StatusOr<Table> CreateTable(TransactionContext& ctx, const Schema& schema);
  Status CreateIndex(TransactionContext& ctx, std::string_view schema_name,
                     const IndexSchema& idx);

  StatusOr<Table> GetTable(TransactionContext& ctx,
                           std::string_view schema_name);

  [[maybe_unused]] void DebugDump(TransactionContext& ctx, std::ostream& o);

  StatusOr<TableStatistics> GetStatistics(TransactionContext& ctx,
                                          std::string_view schema_name);
  Status UpdateStatistics(TransactionContext& txn, std::string_view schema_name,
                          const TableStatistics& ts);
  Status RefreshStatistics(TransactionContext& txn,
                           std::string_view schema_name);

  PageStorage& Storage() { return *relations_.GetPageStorage(); }

 private:
  RelationStorage relations_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_DATABASE_HPP
