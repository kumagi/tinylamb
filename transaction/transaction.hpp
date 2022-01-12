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

  void SetStatus(TransactionStatus status);
  bool IsFinished() const {
    return status_ == TransactionStatus::kCommitted ||
           status_ == TransactionStatus::kAborted;
  }
  lsn_t PrevLSN() const { return lsns_.back(); }

  bool AddReadSet(const RowPosition& rp);
  bool AddWriteSet(const RowPosition& rp);

  bool PreCommit();
  void Abort();

  // Returns LSN
  lsn_t InsertLog(const RowPosition& pos, std::string_view redo);
  lsn_t UpdateLog(const RowPosition& pos, std::string_view undo,
                     std::string_view redo);
  lsn_t DeleteLog(const RowPosition& pos, std::string_view undo);

  lsn_t AllocatePageLog(page_id_t page_id, PageType new_page_type);

  lsn_t DestroyPageLog(page_id_t page_id);

  // Prepared mainly for testing.
  // Using this function is discouraged to get performance of flush pipelining.
  void CommitWait() const;

 private:
  friend class TransactionManager;
  friend class CheckpointManager;

 private:
  const txn_id_t txn_id_;

  std::unordered_set<RowPosition> read_set_{};
  std::unordered_set<RowPosition> write_set_{};
  std::vector<lsn_t> lsns_;
  TransactionStatus status_ = TransactionStatus::kUnknown;

  // Not owned by this class.
  TransactionManager* transaction_manager_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_HPP
