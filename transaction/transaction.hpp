//
// Created by kumagi on 2021/05/29.
//

#ifndef TINYLAMB_TRANSACTION_HPP
#define TINYLAMB_TRANSACTION_HPP

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "page/page.hpp"
#include "page/row_position.hpp"

namespace tinylamb {

class TransactionManager;

class CheckpointManager;
class PageManager;
class Logger;
class LockManager;
class Row;
struct LogRecord;

enum class TransactionStatus : uint_fast8_t {
  kUnknown,
  kRunning,
  kCommitted,
  kAborted,
};

std::ostream& operator<<(std::ostream& o, const TransactionStatus& t);

class Transaction {
  friend class TransactionManager;

 public:
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

  // Returns LSN
  lsn_t InsertLog(page_id_t pid, slot_t slot, std::string_view redo);
  lsn_t InsertLeafLog(page_id_t pid, std::string_view key,
                      std::string_view redo);
  lsn_t InsertInternalLog(page_id_t pid, std::string_view key, page_id_t redo);

  lsn_t UpdateLog(page_id_t pid, slot_t slot, std::string_view prev,
                  std::string_view value);
  lsn_t UpdateLeafLog(page_id_t pid, std::string_view key,
                      std::string_view prev, std::string_view value);
  lsn_t UpdateInternalLog(page_id_t pid, std::string_view key, page_id_t prev,
                          page_id_t value);

  lsn_t DeleteLog(page_id_t pid, slot_t key, std::string_view prev);
  lsn_t DeleteLeafLog(page_id_t pid, std::string_view key,
                      std::string_view prev);
  lsn_t DeleteInternalLog(page_id_t pid, std::string_view key, page_id_t prev);

  lsn_t SetLowestLog(page_id_t pid, page_id_t lowest_value);

  lsn_t AllocatePageLog(page_id_t page_id, PageType new_page_type);

  lsn_t DestroyPageLog(page_id_t page_id);

  // Prepared mainly for testing.
  // Using this function is discouraged to get performance of flush pipelining.
  void CommitWait() const;

 private:
  friend class TransactionManager;
  friend class CheckpointManager;

 private:
  txn_id_t txn_id_;

  std::unordered_set<RowPosition> read_set_{};
  std::unordered_set<RowPosition> write_set_{};
  lsn_t prev_lsn_;
  TransactionStatus status_ = TransactionStatus::kUnknown;

  // Not owned by this class.
  TransactionManager* transaction_manager_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_HPP
