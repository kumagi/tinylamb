//
// Created by kumagi on 2022/01/09.
//

#ifndef TINYLAMB_CHECKPOINT_MANAGER_HPP
#define TINYLAMB_CHECKPOINT_MANAGER_HPP
#include <atomic>
#include <cstdint>
#include <thread>
#include <functional>

#include "transaction/transaction.hpp"

namespace tinylamb {

class TransactionManager;
class PagePool;

class CheckpointManager {
 public:
  CheckpointManager(std::string_view path, TransactionManager* tm, PagePool* pp,
                    size_t interval = 60)
      : master_record_path(path),
        tm_(tm),
        pp_(pp),
        interval_seconds_(interval),
        checkpoint_worker_(
            [this] { WorkerThreadTask(); }) {}
  ~CheckpointManager();

  void Start() {
    start_.store(true, std::memory_order_release);
  }

  void WorkerThreadTask();
  struct ActiveTransactionEntry {
    ActiveTransactionEntry(uint64_t id, TransactionStatus st, uint64_t lsn)
        : txn_id(id), status(st), last_lsn(lsn) {}
    friend std::ostream& operator<<(std::ostream& o,
                                    const ActiveTransactionEntry& a) {
      o << "ID: " << a.txn_id << ", status: " << a.status
        << ", lastLSN: " << a.last_lsn;
      return o;
    }
    bool operator==(const ActiveTransactionEntry& rhs) const {
      return txn_id == rhs.txn_id && status == rhs.status &&
             last_lsn == rhs.last_lsn;
    }
    uint64_t txn_id;
    TransactionStatus status;
    uint64_t last_lsn;
  };

  // This function is intentionally public for test.
  uint64_t WriteCheckpoint(const std::function<void()>& func_for_test = []() {
  });

 private:
  std::atomic<bool> start_ = false;
  std::atomic<bool> stop_ = false;
  std::string master_record_path;
  TransactionManager* const tm_;
  PagePool* const pp_;
  size_t interval_seconds_;
  std::thread checkpoint_worker_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CHECKPOINT_MANAGER_HPP
