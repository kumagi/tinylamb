/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_PARALLEL_SCAN_HPP
#define TINYLAMB_EXECUTOR_PARALLEL_SCAN_HPP

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "executor/detail/scan_filter.hpp"
#include "executor/executor_base.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/table.hpp"

namespace tinylamb {

class Transaction;
struct LSTableStore;

// Morsel-driven scan: workers dynamically claim small page groups rather than
// statically owning a table partition, which balances uneven page occupancy.
class ParallelScan final : public ExecutorBase {
 public:
  // Non-copyable by design: instances are owned by smart pointers and
  // referenced by raw pointers throughout the executor/graph object web.
  ParallelScan(const ParallelScan&) = delete;
  ParallelScan& operator=(const ParallelScan&) = delete;
  ParallelScan(ParallelScan&&) = delete;
  ParallelScan& operator=(ParallelScan&&) = delete;
  ParallelScan(Transaction& txn, const Table& table,
               size_t worker_count = std::thread::hardware_concurrency(),
               size_t pages_per_morsel = 8,
               std::optional<std::vector<slot_t>> projection = std::nullopt,
               std::vector<IntegerPeekCompare> peek_compares = {},
               bool ordered = false);
  ~ParallelScan() override;

  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& out, int indent) const override;

  [[nodiscard]] size_t MorselCount() const { return morsels_.size(); }
  [[nodiscard]] size_t WorkerCount() const { return worker_count_; }
  [[nodiscard]] size_t OutputColumnCount() const {
    return projection_ ? projection_->size()
                       : table_->GetSchema().ColumnCount();
  }
  [[nodiscard]] slot_t TableColumnSlot(size_t output_column) const {
    return projection_ ? (*projection_)[output_column]
                       : static_cast<slot_t>(output_column);
  }

  // A scalar fold can consume disjoint morsels in the decode workers, without
  // boxing rows or handing chunks through the ordered output queue. The
  // callback owns one state per morsel; batches within a morsel stay ordered.
  // Unsupported layouts and row predicates decline before consuming input.
  StatusOr<bool> TryFoldTypedMorsels(
      const std::function<Status(size_t, const TypedCellBatch&)>& consume,
      bool capture_positions = false);
  StatusOr<bool> TryFoldLSTableBlocks(
      size_t worker_count,
      const std::function<Status(size_t, const LSTablePageBlock&,
                                 const uint32_t*, size_t)>& consume);
  [[nodiscard]] bool WasTypedFolded() const { return typed_folded_; }

  // Attach a full row predicate to be decided inside the decode workers
  // (before chunk append), replacing the Selection executor that would
  // otherwise evaluate the same predicate on the single consumer thread.
  // Must be called before the first NextBatch/Next (i.e. before Start()).
  // Rows the worker programs cannot decide are parked and re-checked on the
  // consumer thread at end-of-scan with the context-free AST evaluator, so
  // error semantics match the serial Selection exactly.
  void SetRowFilter(std::vector<Expression> conjuncts,
                    relational_detail::CompiledScanFilter filter,
                    relational_detail::WorkerScanFilterPrograms programs);

  // Row-filter pushdown keeps the unprojected table schema (deferred rows
  // re-materialize into chunks under table_->GetSchema()).
  // An ordered scan (D-9: the consumer relies on the morsel-ordered handoff
  // to deliver stored-chain order) must not take a row filter: rows the
  // worker programs cannot decide are parked and appended at end-of-scan,
  // which would move them out of chain order.
  [[nodiscard]] bool CanTakeRowFilter() const {
    return !ordered_ && !projection_.has_value();
  }

  // True when this scan's consumer relies on stored-chain output order.
  [[nodiscard]] bool IsOrdered() const { return ordered_; }

  [[nodiscard]] bool HasRowFilter() const { return row_filter_ != nullptr; }
  [[nodiscard]] bool HasSignedIntegerColumn(size_t offset) const;
  [[nodiscard]] const LSTableStore* GetLSTableStore() const {
    return lstable_store_.get();
  }

 private:
  void Start(size_t batch_size);
  void RunWorker(size_t batch_size, size_t worker_index);
  bool Enqueue(size_t morsel_index, DataChunk chunk);
  // Re-checks parked rows (programs raised in the workers) and appends the
  // survivors to `destination`.  Runs on the consumer thread at end-of-scan.
  size_t DrainDeferred(DataChunk* destination, size_t max_rows);

  Transaction* txn_;
  const Table* table_;
  TableLease table_lease_;
  std::shared_ptr<const LSTableStore> lstable_store_;
  std::optional<std::vector<slot_t>> projection_;
  std::vector<IntegerPeekCompare> peek_compares_;
  bool ordered_{false};
  std::vector<Table::ScanMorsel> morsels_;
  size_t worker_count_;
  size_t max_ready_chunks_;

  // Row predicate decided in the decode workers (see SetRowFilter).  Set
  // once before Start(); read-only afterwards.
  std::vector<Expression> row_filter_conjuncts_;
  std::unique_ptr<relational_detail::CompiledScanFilter> row_filter_;
  std::unique_ptr<relational_detail::WorkerScanFilterPrograms>
      row_filter_programs_;
  // Rows whose worker program raised, waiting for the context-free AST
  // re-check on the consumer thread.  Rare: guarded by deferred_mutex_.
  std::mutex deferred_mutex_;
  std::vector<std::pair<Row, RowPosition>> deferred_;
  size_t deferred_offset_{0};
  bool deferred_drained_{false};

  // Morsel-ordered handoff.  Workers claim morsels by static rotation
  // (worker w owns morsels w, w+W, w+2W, ...) and enqueue decoded chunks
  // under their morsel index; the consumer drains morsel 0's chunks before
  // morsel 1's and so on.  A morsel with no surviving rows still records
  // morsels_done_ so the consumer can advance past it.  The output order is
  // therefore the morsel (page-range) order — a deterministic function of
  // the table state — instead of the worker-completion order, which made
  // every downstream accumulation order (join build side, per-group FP
  // partial sums) scheduling-dependent.  All fields below are guarded by
  // mutex_.
  std::map<size_t, std::deque<DataChunk>> inflight_;
  size_t inflight_chunks_{0};
  std::vector<bool> morsels_done_;
  size_t next_out_morsel_{0};

  std::vector<std::jthread> workers_;
  std::mutex mutex_;
  std::condition_variable ready_cv_;
  std::condition_variable space_cv_;
  size_t active_workers_{0};
  bool started_{false};
  bool typed_folded_{false};
  std::atomic<bool> cancelled_{false};
  std::exception_ptr worker_error_;
  Status worker_status_{Status::kSuccess};

  std::optional<DataChunk> pending_;
  size_t pending_offset_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_PARALLEL_SCAN_HPP
