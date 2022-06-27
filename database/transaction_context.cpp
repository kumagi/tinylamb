//
// Created by kumagi on 22/05/09.
//

#include "database/transaction_context.hpp"

#include "database/relation_storage.hpp"

namespace tinylamb {

StatusOr<std::shared_ptr<Table>> TransactionContext::GetTable(
    std::string_view table_name) {
  auto it = tables_.find(std::string(table_name));
  if (it != tables_.end()) {
    return it->second;
  }
  ASSIGN_OR_RETURN(Table, tbl, rs_->GetTable(*this, table_name));
  auto result =
      tables_.emplace(table_name, std::make_shared<Table>(std::move(tbl)));
  return result.first->second;
}

StatusOr<std::shared_ptr<TableStatistics>> TransactionContext::GetStats(
    std::string_view table_name) {
  auto it = stats_.find(std::string(table_name));
  if (it != stats_.end()) {
    return it->second;
  }
  ASSIGN_OR_RETURN(TableStatistics, tbl, rs_->GetStatistics(*this, table_name));
  auto result = stats_.emplace(
      table_name, std::make_shared<TableStatistics>(std::move(tbl)));
  return result.first->second;
}

}  // namespace tinylamb
