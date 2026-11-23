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
#include "common/string_util.hpp"

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "database/catalog_reader.hpp"

namespace tinylamb {
namespace {
thread_local const TransactionContext* tl_runtime_owner = nullptr;
thread_local relational_detail::ExecutionRuntime* tl_runtime_ptr = nullptr;
}  // namespace

TransactionContext::ScopedRuntimeOverride::ScopedRuntimeOverride(
    const TransactionContext& owner,
    relational_detail::ExecutionRuntime* runtime)
    : owner_(owner),
      prev_owner_(tl_runtime_owner),
      prev_runtime_(tl_runtime_ptr) {
  owner_.runtime_override_depth_.fetch_add(1, std::memory_order_acq_rel);
  tl_runtime_owner = &owner_;
  tl_runtime_ptr = runtime;
}

TransactionContext::ScopedRuntimeOverride::~ScopedRuntimeOverride() {
  tl_runtime_owner = prev_owner_;
  tl_runtime_ptr = prev_runtime_;
  owner_.runtime_override_depth_.fetch_sub(1, std::memory_order_release);
}

bool TransactionContext::LookupThreadRuntimeOverride(
    const TransactionContext* owner,
    relational_detail::ExecutionRuntime** out) {
  if (tl_runtime_owner == owner) {
    *out = tl_runtime_ptr;
    return true;
  }
  return false;
}

StatusOr<bool> TransactionContext::TableExists(std::string_view name) {
  {
    std::scoped_lock lock(cache_mutex_);
    if (tables_.contains(AsciiToLower(name))) { return true;
}
  }
  if (catalog_ == nullptr) { return false;
}
  return catalog_->TableExists(*this, name);
}

StatusOr<std::shared_ptr<Table>> TransactionContext::GetTable(
    std::string_view table_name) {
  {
    std::scoped_lock lock(cache_mutex_);
    auto it = tables_.find(AsciiToLower(table_name));
    if (it != tables_.end()) {
      return it->second;
    }
  }
  if (catalog_ == nullptr) {
    return Status::kNotExists;
  }
  ASSIGN_OR_RETURN(Table, tbl, catalog_->GetTable(*this, table_name));
  auto shared = std::make_shared<Table>(std::move(tbl));
  std::scoped_lock lock(cache_mutex_);
  auto result =
      tables_.emplace(AsciiToLower(table_name), std::move(shared));
  return result.first->second;
}

StatusOr<std::shared_ptr<TableStatistics>> TransactionContext::GetStats(
    std::string_view table_name) {
  // Use the same key as GetTable so callers can join the two caches.
  const std::string name = AsciiToLower(table_name);
  {
    std::scoped_lock lock(cache_mutex_);
    auto it = stats_.find(name);
    if (it != stats_.end()) {
      return it->second;
    }
  }
  if (catalog_ == nullptr) {
    return Status::kNotExists;
  }
  struct ThreadStatsCache {
    CatalogReader* owner{nullptr};
    uint64_t epoch{0};
    uint64_t snapshot{0};
    std::unordered_map<std::string, std::shared_ptr<TableStatistics>> entries;
  };
  thread_local ThreadStatsCache cache;
  const uint64_t epoch = catalog_->CatalogEpoch();
  const uint64_t snapshot = txn_.SnapshotTimestamp();
  // The live epoch can advance while this transaction still sees older
  // catalog rows. Cache each read under the snapshot that produced it.
  if (cache.owner != catalog_ || cache.epoch != epoch ||
      cache.snapshot != snapshot) {
    cache.owner = catalog_;
    cache.epoch = epoch;
    cache.snapshot = snapshot;
    cache.entries.clear();
  }
  if (const auto cached = cache.entries.find(name);
      cached != cache.entries.end()) {
    std::scoped_lock lock(cache_mutex_);
    auto result = stats_.emplace(name, cached->second);
    return result.first->second;
  }
  ASSIGN_OR_RETURN(TableStatistics, tbl,
                   catalog_->GetStatistics(*this, table_name));
  auto shared = std::make_shared<TableStatistics>(std::move(tbl));
  cache.entries.emplace(name, shared);
  std::scoped_lock lock(cache_mutex_);
  auto result = stats_.emplace(name, std::move(shared));
  return result.first->second;
}

std::ostream& operator<<(std::ostream& o, const TransactionContext& ctx) {
  std::scoped_lock lock(ctx.cache_mutex_);
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
