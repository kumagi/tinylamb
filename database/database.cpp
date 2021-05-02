//
// Created by kumagi on 2019/09/04.
//

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