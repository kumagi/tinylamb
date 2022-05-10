//
// Created by kumagi on 22/05/09.
//

#include "database/transaction_context.hpp"

#include "database/relation_storage.hpp"

namespace tinylamb {

Table* TransactionContext::GetTable(std::string_view table_name) {
  auto it = tables_.find(std::string(table_name));
  if (it != tables_.end()) {
    return &it->second;
  }
  ASSIGN_OR_CRASH(Table, tbl, rs_->GetTable(txn_, table_name));
  auto result = tables_.emplace(table_name, tbl);
  return &result.first->second;
}

}  // namespace tinylamb
