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


#include "database.hpp"

#include "index/index_schema.hpp"

namespace tinylamb {

TransactionContext Database::BeginContext() {
  return relations_.BeginContext();
}

StatusOr<Table> Database::CreateTable(TransactionContext& ctx,
                                      const Schema& schema) {
  return relations_.CreateTable(ctx, schema);
}

Status Database::CreateIndex(TransactionContext& ctx,
                             std::string_view schema_name,
                             const IndexSchema& idx) {
  return relations_.CreateIndex(ctx, schema_name, idx);
}

StatusOr<Table> Database::GetTable(TransactionContext& ctx,
                                   std::string_view schema_name) {
  ASSIGN_OR_RETURN(std::shared_ptr<Table>, table, ctx.GetTable(schema_name));
  return *table;
}

void Database::DebugDump(TransactionContext& ctx, std::ostream& o) {
  relations_.DebugDump(ctx.txn_, o);
}

StatusOr<TableStatistics> Database::GetStatistics(
    TransactionContext& ctx, std::string_view schema_name) {
  return relations_.GetStatistics(ctx, schema_name);
}

Status Database::UpdateStatistics(TransactionContext& ctx,
                                  std::string_view schema_name,
                                  const TableStatistics& ts) {
  return relations_.UpdateStatistics(ctx, schema_name, ts);
}

Status Database::RefreshStatistics(TransactionContext& ctx,
                                   std::string_view schema_name) {
  return relations_.RefreshStatistics(ctx, schema_name);
}

}  // namespace tinylamb