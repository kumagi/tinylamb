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

#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "common/status_or.hpp"
#include "database/catalog_reader.hpp"

namespace tinylamb {
StatusOr<std::shared_ptr<Table>> TransactionContext::GetTable(
    std::string_view table_name) {
  auto it = tables_.find(std::string(table_name));
  if (it != tables_.end()) {
    return it->second;
  }
  if (catalog_ == nullptr) {
    return Status::kNotExists;
  }
  ASSIGN_OR_RETURN(Table, tbl, catalog_->GetTable(*this, table_name));
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
  if (catalog_ == nullptr) {
    return Status::kNotExists;
  }
  struct ThreadStatsCache {
    CatalogReader* owner{nullptr};
    uint64_t epoch{0};
    std::unordered_map<std::string, std::shared_ptr<TableStatistics>> entries;
  };
  thread_local ThreadStatsCache cache;
  const uint64_t epoch = catalog_->CatalogEpoch();
  if (cache.owner != catalog_ || cache.epoch != epoch) {
    cache.owner = catalog_;
    cache.epoch = epoch;
    cache.entries.clear();
  }
  const std::string name(table_name);
  if (const auto cached = cache.entries.find(name);
      cached != cache.entries.end()) {
    stats_.emplace(name, cached->second);
    return cached->second;
  }
  ASSIGN_OR_RETURN(TableStatistics, tbl,
                   catalog_->GetStatistics(*this, table_name));
  auto shared = std::make_shared<TableStatistics>(std::move(tbl));
  cache.entries.emplace(name, shared);
  auto result = stats_.emplace(table_name, std::move(shared));
  return result.first->second;
}

std::ostream& operator<<(std::ostream& o, const TransactionContext& ctx) {
  o << "TransactionContext(txn=" << ctx.txn_ << ", tables=[";
  for (const auto& [name, t] : ctx.tables_) {
    o << name << ": " << t << ", ";
  }
  o << "], stats=[";
  for (const auto& [name, s] : ctx.stats_) {
    o << name << ": " << s << ", ";
  }
  o << "])";
  return o;
}
}  // namespace tinylamb
