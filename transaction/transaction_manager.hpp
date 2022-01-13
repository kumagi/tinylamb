//
// Created by kumagi on 2021/05/29.
//

#ifndef TINYLAMB_TRANSACTION_MANAGER_HPP
#define TINYLAMB_TRANSACTION_MANAGER_HPP

#include <atomic>
#include <shared_mutex>
#include <unordered_map>

#include "constants.hpp"

namespace tinylamb {

class LockManager;
class Logger;
class LogRecord;
class PageManager;
class Transaction;
class Recovery;
class RowPosition;
enum class TransactionStatus : uint_fast8_t;

class TransactionManager {
 public:
  TransactionManager(LockManager* lm, Logger* l, Recovery* r)
      : lock_manager_(lm), logger_(l), recovery_(r) {}

  Transaction Begin();

  bool PreCommit(Transaction& txn);

  void Abort(Transaction& txn);

  void CompensateInsertLog(txn_id_t txn_id, const RowPosition& pos);
  void CompensateUpdateLog(txn_id_t txn_id, const RowPosition& pos,
                           std::string_view redo);
  void CompensateDeleteLog(txn_id_t txn_id, const RowPosition& pos,
                           std::string_view redo);

  bool GetExclusiveLock(const RowPosition& rp);
  bool GetSharedLock(const RowPosition& rp);

  lsn_t AddLog(const LogRecord& lr);
  lsn_t CommittedLSN() const;

 private:
  friend class Recovery;
  friend class CheckpointManager;

  std::unordered_map<txn_id_t, Transaction*> active_transactions_;
  std::atomic<txn_id_t> next_txn_id_ = 1;
  LockManager* const lock_manager_;
  Logger* const logger_;
  Recovery* const recovery_;
  std::mutex transaction_table_lock;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_MANAGER_HPP
