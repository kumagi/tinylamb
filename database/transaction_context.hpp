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

#ifndef TINYLAMB_TRANSACTION_CONTEXT_HPP
#define TINYLAMB_TRANSACTION_CONTEXT_HPP

#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>

#include "database/catalog_reader.hpp"
#include "transaction/transaction.hpp"
// #include "transaction/transaction_manager.hpp"

namespace tinylamb {

class PageManager;
class CatalogReader;
class Table;
class TableStatistics;
namespace relational_detail {
struct ExecutionRuntime;
}
// expression/evaluation_context.hpp; forward declared so the database layer
// keeps no include edge towards the expression layer.
class EvaluationContext;

class TransactionContext {
 public:
  ~TransactionContext() = default;
  TransactionContext(Transaction&& txn, CatalogReader* catalog)
      : txn_(std::move(txn)), catalog_(catalog) {}
  TransactionContext(const TransactionContext&) = delete;
  TransactionContext& operator=(const TransactionContext&) = delete;
  TransactionContext(TransactionContext&& o) noexcept
      : txn_(std::move(o.txn_)),
        catalog_(o.catalog_),
        tables_(std::move(o.tables_)),
        stats_(std::move(o.stats_)),
        evaluation_context_(o.evaluation_context_),
        execution_runtime_(o.execution_runtime_) {
    o.evaluation_context_ = nullptr;
    o.execution_runtime_ = nullptr;
  }
  TransactionContext& operator=(TransactionContext&& o) noexcept {
    if (this == &o) {
      return *this;
    }
    txn_ = std::move(o.txn_);
    catalog_ = o.catalog_;
    // Caches belong to the old context; the moved-in txn must not reuse them.
    // Clear the source too so a moved-from handle cannot observe stale
    // snapshot-bound Table/Stats shared_ptrs (the move ctor moves them).
    tables_.clear();
    stats_.clear();
    o.tables_.clear();
    o.stats_.clear();
    evaluation_context_ = nullptr;
    execution_runtime_ = nullptr;
    return *this;
  }
  CatalogReader* GetCatalog() const { return catalog_; }
  StatusOr<std::shared_ptr<Table>> GetTable(std::string_view table_name);
  StatusOr<std::shared_ptr<TableStatistics>> GetStats(
      std::string_view table_name);

  Status PreCommit() { return txn_.PreCommit(); }
  StatusOr<lsn_t> PrepareCommitForDurableReply() {
    return txn_.PrepareCommitForDurableReply();
  }
  Status Abort() {
    const bool catalog_dirty = txn_.IsCatalogDirty();
    Status status = txn_.Abort();
    // T485: the rollback revives (or drops) uncommitted catalog entries,
    // but the epoch counter never unwinds -- without the bump, a stamp
    // taken from the transaction-local view would keep claiming that
    // vanished state.
    if (catalog_dirty && catalog_ != nullptr) {
      catalog_->BumpCatalogEpoch();
    }
    return status;
  }
  // True once the underlying transaction committed or aborted.
  [[nodiscard]] bool IsFinished() const { return txn_.IsFinished(); }

  // A1/S3 implementation hook: upper layers (query/) attach the
  // EvaluationContext that carries subquery execution and function
  // registration for this transaction.  Non-owning; the owner must outlive
  // the context.  Expression nodes only ever see the abstract
  // EvaluationContext interface, never this type.
  void set_evaluation_context(EvaluationContext* context) {
    evaluation_context_ = context;
  }
  [[nodiscard]] EvaluationContext* evaluation_context() const {
    return evaluation_context_;
  }

  class ScopedRuntimeOverride {
   public:
    ScopedRuntimeOverride(const TransactionContext& owner,
                          relational_detail::ExecutionRuntime* runtime);
    ScopedRuntimeOverride(const ScopedRuntimeOverride&) = delete;
    ScopedRuntimeOverride& operator=(const ScopedRuntimeOverride&) = delete;
    ScopedRuntimeOverride(ScopedRuntimeOverride&&) = delete;
    ScopedRuntimeOverride& operator=(ScopedRuntimeOverride&&) = delete;
    ~ScopedRuntimeOverride();

   private:
    const TransactionContext& owner_;
    const TransactionContext* prev_owner_;
    relational_detail::ExecutionRuntime* prev_runtime_;
  };

  void set_execution_runtime(relational_detail::ExecutionRuntime* runtime) {
    execution_runtime_ = runtime;
  }
  [[nodiscard]] relational_detail::ExecutionRuntime* raw_execution_runtime()
      const {
    return execution_runtime_;
  }
  [[nodiscard]] relational_detail::ExecutionRuntime* execution_runtime() const {
    if (runtime_override_depth_.load(std::memory_order_acquire) != 0) {
      relational_detail::ExecutionRuntime* overridden = nullptr;
      if (LookupThreadRuntimeOverride(this, &overridden)) {
        return overridden;
      }
    }
    return execution_runtime_;
  }

  friend std::ostream& operator<<(std::ostream& o,
                                  const TransactionContext& ctx);

  Transaction txn_;
  CatalogReader* catalog_;
  std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
  std::unordered_map<std::string, std::shared_ptr<TableStatistics>> stats_;

 private:
  static bool LookupThreadRuntimeOverride(
      const TransactionContext* owner,
      relational_detail::ExecutionRuntime** out);

  mutable std::mutex cache_mutex_;
  mutable std::atomic<uint32_t> runtime_override_depth_{0};
  EvaluationContext* evaluation_context_{nullptr};
  relational_detail::ExecutionRuntime* execution_runtime_{nullptr};
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_CONTEXT_HPP
