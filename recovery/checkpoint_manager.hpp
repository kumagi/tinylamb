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

#ifndef TINYLAMB_CHECKPOINT_MANAGER_HPP
#define TINYLAMB_CHECKPOINT_MANAGER_HPP

#include <atomic>
#include <cstdint>
#include <functional>
#include <thread>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
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
        checkpoint_worker_([this] { WorkerThreadTask(); }) {}
  ~CheckpointManager();

  void Start() { start_.store(true, std::memory_order_release); }

  void WorkerThreadTask();
  struct ActiveTransactionEntry {
    ActiveTransactionEntry() = default;
    ActiveTransactionEntry(uint64_t id, TransactionStatus st, lsn_t lsn)
        : txn_id(id), status(st), last_lsn(lsn) {}
    friend std::ostream& operator<<(std::ostream& o,
                                    const ActiveTransactionEntry& a) {
      o << "ID: " << a.txn_id << ", status: " << a.status
        << ", lastLSN: " << a.last_lsn;
      return o;
    }

    friend Encoder& operator<<(Encoder& e, const ActiveTransactionEntry& t) {
      e << t.txn_id << (uint_fast8_t)t.status << t.last_lsn;
      return e;
    }
    friend Decoder& operator>>(Decoder& d, ActiveTransactionEntry& t) {
      d >> t.txn_id >> (uint_fast8_t&)t.status >> t.last_lsn;
      return d;
    }
    static size_t Size() {
      return sizeof(txn_id_t) + sizeof(TransactionStatus) + sizeof(lsn_t);
    }
    bool operator==(const ActiveTransactionEntry& rhs) const = default;
    txn_id_t txn_id = 0;
    TransactionStatus status = TransactionStatus::kRunning;
    lsn_t last_lsn = 0;
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
