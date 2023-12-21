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

//
// Created by kumagi on 22/05/09.
//

#include "database/transaction_context.hpp"

#include "database/database.hpp"

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
