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

#ifndef TINYLAMB_TRANSACTION_HPP
#define TINYLAMB_TRANSACTION_HPP

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "page/page.hpp"
#include "page/row_position.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class TransactionManager;
class CheckpointManager;
class PageManager;
class Logger;
class LockManager;
struct FosterPair;
struct LogRecord;
struct Row;

enum class TransactionStatus : uint_fast8_t {
  kUnknown,
  kRunning,
  kCommitted,
  kAborted,
};

std::ostream& operator<<(std::ostream& o, const TransactionStatus& t);

class Transaction final {
 public:
  Transaction() = default;  // For test purpose only.
  Transaction(txn_id_t txn_id, TransactionManager* tm);
  Transaction(const Transaction& o) = delete;
  Transaction(Transaction&& o) = default;
  Transaction& operator=(const Transaction& o) = delete;
  Transaction& operator=(Transaction&& o) noexcept {
    txn_id_ = o.txn_id_;
    read_set_ = std::move(o.read_set_);
    write_set_ = std::move(o.write_set_);
    prev_lsn_ = o.prev_lsn_;
    status_ = o.status_;
    transaction_manager_ = o.transaction_manager_;
    return *this;
  }
  ~Transaction() = default;

  void SetStatus(TransactionStatus status);
  bool IsFinished() const {
    return status_ == TransactionStatus::kCommitted ||
           status_ == TransactionStatus::kAborted;
  }
  lsn_t PrevLSN() const { return prev_lsn_; }

  bool AddReadSet(const RowPosition& rp);
  bool AddWriteSet(const RowPosition& rp);

  Status PreCommit();
  void Abort();

  // Log the action. Returns LSN.
  lsn_t InsertLog(page_id_t pid, slot_t slot, std::string_view redo);
  lsn_t InsertLeafLog(page_id_t pid, std::string_view key,
                      std::string_view redo);
  lsn_t InsertBranchLog(page_id_t pid, std::string_view key, page_id_t redo);

  lsn_t UpdateLog(page_id_t pid, slot_t slot, std::string_view redo,
                  std::string_view undo);
  lsn_t UpdateLeafLog(page_id_t pid, std::string_view key,
                      std::string_view redo, std::string_view undo);
  lsn_t UpdateBranchLog(page_id_t pid, std::string_view key, page_id_t redo,
                        page_id_t undo);

  lsn_t DeleteLog(page_id_t pid, slot_t key, std::string_view prev);
  lsn_t DeleteLeafLog(page_id_t pid, std::string_view key,
                      std::string_view prev);
  lsn_t DeleteBranchLog(page_id_t pid, std::string_view key, page_id_t undo);

  lsn_t SetLowestLog(page_id_t pid, page_id_t redo, page_id_t undo);

  lsn_t SetLowFence(page_id_t pid, const IndexKey& redo, const IndexKey& undo);
  lsn_t SetHighFence(page_id_t pid, const IndexKey& redo, const IndexKey& undo);
  lsn_t SetFoster(page_id_t pid, const FosterPair& redo,
                  const FosterPair& undo);

  lsn_t AllocatePageLog(page_id_t page_id, PageType new_page_type);

  lsn_t DestroyPageLog(page_id_t page_id);

  // Prepared mainly for testing.
  // Using this function is discouraged to get performance of flush pipelining.
  void CommitWait() const;

  PageManager* PageManager() { return transaction_manager_->GetPageManager(); }

  // Transaction is not a value object. Never try to compare by its attributes.
  bool operator==(const Transaction& rhs) const = delete;

 private:
  friend class TransactionManager;
  friend class CheckpointManager;

  txn_id_t txn_id_{static_cast<txn_id_t>(-1)};

  std::unordered_set<RowPosition> read_set_{};
  std::unordered_set<RowPosition> write_set_{};
  lsn_t prev_lsn_{};
  TransactionStatus status_ = TransactionStatus::kUnknown;

  // Not owned by this class.
  TransactionManager* transaction_manager_{nullptr};
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_HPP
