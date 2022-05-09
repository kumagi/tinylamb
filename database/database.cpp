//
// Created by kumagi on 2019/09/04.
//

#include "database.hpp"

#include "index/index_schema.hpp"

namespace tinylamb {

Transaction Database::Begin() { return relations_.Begin(); }

TransactionContext Database::BeginContext() {
  return {relations_.Begin(), &relations_};
}

Status Database::CreateTable(Transaction& txn, const Schema& schema) {
  return relations_.CreateTable(txn, schema);
}

Status Database::CreateIndex(Transaction& txn, std::string_view schema_name,
                             const IndexSchema& idx) {
  return relations_.CreateIndex(txn, schema_name, idx);
}

StatusOr<Table> Database::GetTable(Transaction& txn,
                                   std::string_view schema_name) {
  return relations_.GetTable(txn, schema_name);
}

void Database::DebugDump(Transaction& txn, std::ostream& o) {
  relations_.DebugDump(txn, o);
}

StatusOr<TableStatistics> Database::GetStatistics(
    Transaction& txn, std::string_view schema_name) {
  return relations_.GetStatistics(txn, schema_name);
}

Status Database::UpdateStatistics(Transaction& txn,
                                  std::string_view schema_name,
                                  const TableStatistics& ts) {
  return relations_.UpdateStatistics(txn, schema_name, ts);
}

Status Database::RefreshStatistics(Transaction& txn,
                                   std::string_view schema_name) {
  return relations_.RefreshStatistics(txn, schema_name);
}

}  // namespace tinylamb