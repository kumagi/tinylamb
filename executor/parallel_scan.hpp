/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_PARALLEL_SCAN_HPP
#define TINYLAMB_EXECUTOR_PARALLEL_SCAN_HPP

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <exception>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "executor/executor_base.hpp"
#include "table/table.hpp"

namespace tinylamb {

class Transaction;

// Morsel-driven scan: workers dynamically claim small page groups rather than
// statically owning a table partition, which balances uneven page occupancy.
class ParallelScan final : public ExecutorBase {
 public:
  ParallelScan(Transaction& txn, const Table& table,
               size_t worker_count = std::thread::hardware_concurrency(),
               size_t pages_per_morsel = 8,
               std::optional<std::vector<slot_t>> projection = std::nullopt);
  ~ParallelScan() override;

  bool Next(Row* destination, RowPosition* position) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& out, int indent) const override;

  [[nodiscard]] size_t MorselCount() const { return morsels_.size(); }
  [[nodiscard]] size_t WorkerCount() const { return worker_count_; }

 private:
  void Start(size_t batch_size);
  void RunWorker(size_t batch_size);
  bool Enqueue(DataChunk chunk);

  Transaction* txn_;
  const Table* table_;
  std::optional<std::vector<slot_t>> projection_;
  std::vector<Table::ScanMorsel> morsels_;
  size_t worker_count_;
  size_t max_ready_chunks_;

  std::atomic<size_t> next_morsel_{0};
  std::vector<std::jthread> workers_;
  std::mutex mutex_;
  std::condition_variable ready_cv_;
  std::condition_variable space_cv_;
  std::deque<DataChunk> ready_;
  size_t active_workers_{0};
  bool started_{false};
  bool cancelled_{false};
  std::exception_ptr worker_error_;

  std::optional<DataChunk> pending_;
  size_t pending_offset_{0};
  DataChunk scalar_batch_;
  size_t scalar_offset_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_PARALLEL_SCAN_HPP
