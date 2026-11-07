/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_PARALLEL_INDEX_SCAN_HPP
#define TINYLAMB_EXECUTOR_PARALLEL_INDEX_SCAN_HPP

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "common/incarnation.hpp"
#include "executor/executor_base.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
class Index;
class Table;
class Transaction;

// Morsel-driven parallel index scan (RangePartitionedScan consumer): the
// requested key range is partitioned at B+Tree leaf boundaries
// (BPlusTree::ScanPartitionBoundaries) and workers scan one key sub-range
// each, so the random heap I/O of a large index range scan overlaps across
// workers.  Workers hand chunks off in morsel order, so the output is the
// same key order the serial IndexScan promises and the plan's ordering
// claims stay valid.
//
// Scope: ascending single-range scans.  DML source scans (row-position
// locking) and point-union shapes stay on the serial IndexScan; residual
// predicates ride a Selection above this executor because AST evaluation is
// single-threaded (same contract as ParallelScan's row filter).
class ParallelIndexScan final : public ExecutorBase {
 public:
  ParallelIndexScan(const ParallelIndexScan&) = delete;
  ParallelIndexScan& operator=(const ParallelIndexScan&) = delete;
  ParallelIndexScan(ParallelIndexScan&&) = delete;
  ParallelIndexScan& operator=(ParallelIndexScan&&) = delete;
  ParallelIndexScan(Transaction& txn, const Table& table, const Index& index,
                    const std::vector<Value>& begin_key,
                    const std::vector<Value>& end_key, bool ascending,
                    size_t worker_count, size_t leaves_per_morsel,
                    TransactionManager::IndexReadGuard index_read_guard = {});
  ~ParallelIndexScan() override;

  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& out, int indent) const override;

  [[nodiscard]] size_t MorselCount() const { return morsels_.size(); }
  [[nodiscard]] size_t WorkerCount() const { return worker_count_; }

 private:
  struct Morsel {
    std::vector<Value> begin_key;
    std::vector<Value> end_key;
    // True when |end_key| is an interior partition boundary: entries whose
    // key equals it belong to the next morsel, so this one stops before them.
    bool exclusive_end{false};
  };

  void Start(size_t batch_size);
  void RunWorker(size_t batch_size, size_t worker_index);
  // Deposits a full chunk for |morsel_index|.  One slot per morsel and one
  // owner per morsel (static rotation), so the owner never races itself;
  // the deposit blocks while the consumer has not drained the previous chunk
  // of that morsel (bounded memory, consumer-driven backpressure).
  bool Deposit(size_t morsel_index, DataChunk chunk);
  [[nodiscard]] bool KeyEqualsBoundary(
      const Row& row, const std::vector<Value>& boundary) const;

  Transaction* txn_;
  const Table* table_;
  const Index* index_;
  bool ascending_;
  Schema schema_;
  // Holds the table lease for the whole parallel scan; per-morsel serial
  // scans acquire (and release) their own on top of it.
  TableLease table_lease_;
  // Index key column slots, used for the interior-boundary check.
  std::vector<slot_t> key_slots_;
  TransactionManager::IndexReadGuard index_read_guard_;
  std::vector<Morsel> morsels_;
  size_t worker_count_;

  std::vector<std::jthread> workers_;
  std::mutex mutex_;
  std::condition_variable ready_cv_;
  std::condition_variable space_cv_;
  // One in-flight chunk slot per morsel plus a done flag; guarded by mutex_.
  std::vector<std::optional<DataChunk>> morsel_chunk_;
  std::vector<bool> morsels_done_;
  size_t next_out_morsel_{0};
  size_t active_workers_{0};
  bool started_{false};
  std::atomic<bool> cancelled_{false};
  std::exception_ptr worker_error_;
  Status worker_status_{Status::kSuccess};

  std::optional<DataChunk> pending_;
  size_t pending_offset_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_PARALLEL_INDEX_SCAN_HPP
