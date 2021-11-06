//
// Created by kumagi on 2021/05/29.
//

#ifndef TINYLAMB_TRANSACTION_HPP
#define TINYLAMB_TRANSACTION_HPP

#include <unordered_map>
#include <unordered_set>

#include "page/page.hpp"
#include "page/row_position.hpp"

namespace tinylamb {

class TransactionManager;

class PageManager;
class Logger;
class LockManager;
class Row;
struct LogRecord;

enum class TransactionStatus : uint_fast8_t {
  kUnknown,
  kRunning,
  kCommitted,
  kAborted
};

class Transaction {
  friend class TransactionManager;

 public:
  Transaction(uint64_t txn_id, TransactionManager* tm, LockManager* lm,
              PageManager* pm, Logger* l);
  Transaction SpawnSystemTransaction();
  void SetStatus(TransactionStatus status);
  bool IsFinished() const {
    return status_ == TransactionStatus::kCommitted ||
           status_ == TransactionStatus::kAborted;
  }
  uint64_t PrevLSN() const { return prev_lsn_; }

  bool AddReadSet(const RowPosition& rs);
  bool AddWriteSet(const RowPosition& rs);

  bool PreCommit();
  void Abort();

  // Returns LSN
  uint64_t InsertLog(const RowPosition& pos, std::string_view redo);
  uint64_t UpdateLog(const RowPosition& pos, std::string_view undo,
                     std::string_view redo);
  uint64_t DeleteLog(const RowPosition& pos, std::string_view undo);
  uint64_t CompensateInsertLog(const RowPosition& pos, uint64_t undo_next_Lsn);
  uint64_t CompensateUpdateLog(const RowPosition& pos, uint64_t undo_next_lsn,
                               std::string_view redo);
  uint64_t CompensateDeleteLog(const RowPosition& pos, uint64_t undo_next_lsn,
                               std::string_view redo);

  uint64_t AllocatePageLog(uint64_t page_id, PageType new_page_type);

  uint64_t DestroyPageLog(uint64_t page_id);
  // Using this function is discouraged to get performance of flush pipelining.
  void CommitWait() const;

 private:
  friend class TransactionManager;

 private:
  const uint64_t txn_id_;

  enum class WriteType {
    kInsert,
    kUpdate,
    kDelete,
  };
  struct WriteEntry {
    WriteType entry_type;
    std::string payload;
    uint64_t lsn;
  };

  uint64_t prev_lsn_ = 0;
  std::unordered_set<RowPosition> read_set_{};
  std::unordered_set<RowPosition> write_set_{};
  std::unordered_map<RowPosition, WriteEntry> prev_record_{};
  TransactionStatus status_ = TransactionStatus::kUnknown;

  // Not owned by this class.
  TransactionManager* transaction_manager_;
  LockManager* lock_manager_;
  PageManager* page_manager_;
  Logger* logger_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_HPP
