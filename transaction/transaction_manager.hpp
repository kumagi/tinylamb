//
// Created by kumagi on 2021/05/29.
//

#ifndef TINYLAMB_TRANSACTION_MANAGER_HPP
#define TINYLAMB_TRANSACTION_MANAGER_HPP

#include <atomic>
#include <shared_mutex>
#include <unordered_map>

#include "common/constants.hpp"

namespace tinylamb {

class LockManager;
class Logger;
class PageManager;
class Transaction;
class RecoveryManager;
enum class TransactionStatus : uint_fast8_t;
struct LogRecord;
struct RowPosition;

class TransactionManager {
 public:
  TransactionManager(LockManager* lm, Logger* l, RecoveryManager* r)
      : lock_manager_(lm), logger_(l), recovery_(r) {}

  Transaction Begin();

  Status PreCommit(Transaction& txn);

  void Abort(Transaction& txn);

  void CompensateInsertLog(txn_id_t txn_id, page_id_t pid, slot_t slot);
  void CompensateInsertLog(txn_id_t txn_id, page_id_t pid,
                           std::string_view key);
  void CompensateInsertInternalLog(txn_id_t txn_id, page_id_t pid,
                                   std::string_view key);
  void CompensateUpdateLog(txn_id_t txn_id, page_id_t pid, slot_t slot,
                           std::string_view redo);
  void CompensateUpdateLog(txn_id_t txn_id, page_id_t pid, std::string_view key,
                           std::string_view redo);
  void CompensateUpdateInternalLog(txn_id_t txn_id, page_id_t pid,
                                   std::string_view key, page_id_t redo);
  void CompensateDeleteLog(txn_id_t txn_id, page_id_t pid, slot_t slot,
                           std::string_view redo);
  void CompensateDeleteLog(txn_id_t txn_id, page_id_t pid, std::string_view key,
                           std::string_view redo);
  void CompensateDeleteInternalLog(txn_id_t txn_id, page_id_t pid,
                                   std::string_view key, page_id_t redo);

  bool GetExclusiveLock(const RowPosition& rp);
  bool GetSharedLock(const RowPosition& rp);

  lsn_t AddLog(const LogRecord& lr);
  lsn_t CommittedLSN() const;

 private:
  friend class RecoveryManager;
  friend class CheckpointManager;

  std::unordered_map<txn_id_t, Transaction*> active_transactions_;
  std::atomic<txn_id_t> next_txn_id_ = 1;
  LockManager* const lock_manager_;
  Logger* const logger_;
  RecoveryManager* const recovery_;
  std::mutex transaction_table_lock;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_MANAGER_HPP
