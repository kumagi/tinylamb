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

#include <string>
#include <unordered_map>

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
// keeps no include edge towards the expression layer (improvement3.md A1/S3).
class EvaluationContext;

class TransactionContext {
 public:
  TransactionContext(Transaction&& txn, CatalogReader* catalog)
      : txn_(std::move(txn)), catalog_(catalog) {}
  TransactionContext(const TransactionContext&) = delete;
  TransactionContext& operator=(const TransactionContext&) = delete;
  TransactionContext(TransactionContext&&) = default;
  TransactionContext& operator=(TransactionContext&& o) noexcept {
    txn_ = std::move(o.txn_);
    catalog_ = o.catalog_;
    // Caches belong to the old context; the moved-in txn must not reuse them.
    tables_.clear();
    stats_.clear();
    evaluation_context_ = nullptr;
    execution_runtime_ = nullptr;
    return *this;
  }
  CatalogReader* GetCatalog() { return catalog_; }
  StatusOr<std::shared_ptr<Table>> GetTable(std::string_view table_name);
  StatusOr<std::shared_ptr<TableStatistics>> GetStats(
      std::string_view table_name);

  Status PreCommit() { return txn_.PreCommit(); }
  void Abort() { txn_.Abort(); }
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

  void set_execution_runtime(relational_detail::ExecutionRuntime* runtime) {
    execution_runtime_ = runtime;
  }
  [[nodiscard]] relational_detail::ExecutionRuntime* execution_runtime() const {
    return execution_runtime_;
  }

  friend std::ostream& operator<<(std::ostream& o,
                                  const TransactionContext& ctx);

  Transaction txn_;
  CatalogReader* catalog_;
  std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
  std::unordered_map<std::string, std::shared_ptr<TableStatistics>> stats_;

 private:
  EvaluationContext* evaluation_context_{nullptr};
  relational_detail::ExecutionRuntime* execution_runtime_{nullptr};
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_CONTEXT_HPP
