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

#include "transaction/transaction.hpp"
// #include "transaction/transaction_manager.hpp"

namespace tinylamb {

class PageManager;
class RelationStorage;
class Table;
class TableStatistics;

class TransactionContext {
 public:
  TransactionContext(Transaction&& txn, RelationStorage* rs)
      : txn_(std::move(txn)), rs_(rs) {}
  TransactionContext(const TransactionContext&) = delete;
  TransactionContext& operator=(const TransactionContext&) = delete;
  TransactionContext(TransactionContext&&) = default;
  TransactionContext& operator=(TransactionContext&& o) noexcept {
    txn_ = std::move(o.txn_);
    rs_ = o.rs_;
    return *this;
  }
  StatusOr<std::shared_ptr<Table>> GetTable(std::string_view table_name);
  StatusOr<std::shared_ptr<TableStatistics>> GetStats(
      std::string_view table_name);

  Status PreCommit() { return txn_.PreCommit(); }
  void Abort() { txn_.Abort(); }

  Transaction txn_;
  RelationStorage* rs_;
  std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
  std::unordered_map<std::string, std::shared_ptr<TableStatistics>> stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_CONTEXT_HPP
