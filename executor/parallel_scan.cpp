/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/parallel_scan.hpp"

#include <algorithm>
#include <ostream>
#include <utility>

#include "table/iterator.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

ParallelScan::ParallelScan(
    Transaction& txn, const Table& table, size_t worker_count,
    size_t pages_per_morsel,
    std::optional<std::vector<slot_t>> projection)
    : txn_(&txn),
      table_(&table),
      projection_(std::move(projection)),
      morsels_(table.BuildScanMorsels(txn, pages_per_morsel)),
      worker_count_(std::min(std::max<size_t>(1, worker_count),
                             std::max<size_t>(1, morsels_.size()))),
      max_ready_chunks_(std::max<size_t>(2, worker_count_ * 2)) {}

ParallelScan::~ParallelScan() {
  {
    std::scoped_lock lock(mutex_);
    cancelled_ = true;
  }
  ready_cv_.notify_all();
  space_cv_.notify_all();
  // jthread joins in its destructor.  Join while the mutexes and condition
  // variables used by workers are still alive.
  workers_.clear();
}

void ParallelScan::Start(size_t batch_size) {
  std::scoped_lock lock(mutex_);
  if (started_) return;
  started_ = true;
  if (morsels_.empty()) return;
  active_workers_ = worker_count_;
  workers_.reserve(worker_count_);
  for (size_t worker = 0; worker < worker_count_; ++worker) {
    workers_.emplace_back(
        [this, batch_size] { RunWorker(batch_size); });
  }
}

void ParallelScan::RunWorker(size_t batch_size) {
  try {
    while (true) {
      const size_t morsel_index = next_morsel_.fetch_add(1);
      if (morsel_index >= morsels_.size()) break;
      Iterator iterator = table_->BeginMorselScan(
          *txn_, morsels_[morsel_index], projection_);
      DataChunk chunk(projection_ ? std::vector<ValueType>{}
                                  : std::vector<ValueType>{},
                      batch_size);
      if (projection_) {
        std::vector<ValueType> types;
        types.reserve(projection_->size());
        for (slot_t slot : *projection_) {
          types.push_back(table_->GetSchema().GetColumn(slot).Type());
        }
        chunk.Initialize(std::move(types), batch_size);
      } else {
        chunk.Initialize(table_->GetSchema(), batch_size);
      }
      while (iterator.IsValid()) {
        chunk.Append(*iterator, iterator.Position());
        ++iterator;
        if (chunk.Size() == batch_size) {
          if (!Enqueue(std::move(chunk))) return;
          if (projection_) {
            std::vector<ValueType> types;
            types.reserve(projection_->size());
            for (slot_t slot : *projection_) {
              types.push_back(table_->GetSchema().GetColumn(slot).Type());
            }
            chunk.Initialize(std::move(types), batch_size);
          } else {
            chunk.Reset(table_->GetSchema(), batch_size);
          }
        }
      }
      if (!chunk.Empty() && !Enqueue(std::move(chunk))) return;
    }
  } catch (...) {
    std::scoped_lock lock(mutex_);
    if (!worker_error_) worker_error_ = std::current_exception();
    cancelled_ = true;
  }
  {
    std::scoped_lock lock(mutex_);
    --active_workers_;
  }
  ready_cv_.notify_all();
  space_cv_.notify_all();
}

bool ParallelScan::Enqueue(DataChunk chunk) {
  std::unique_lock lock(mutex_);
  space_cv_.wait(lock, [this] {
    return cancelled_ || ready_.size() < max_ready_chunks_;
  });
  if (cancelled_) return false;
  ready_.push_back(std::move(chunk));
  lock.unlock();
  ready_cv_.notify_one();
  return true;
}

size_t ParallelScan::NextBatch(DataChunk* destination, size_t max_rows) {
  max_rows = std::max<size_t>(1, max_rows);
  destination->Reset();
  if (pending_) {
    while (pending_offset_ < pending_->Size() &&
           destination->Size() < max_rows) {
      destination->Append(*pending_, pending_offset_++);
    }
    if (pending_offset_ == pending_->Size()) {
      pending_.reset();
      pending_offset_ = 0;
    }
    return destination->Size();
  }

  Start(max_rows);
  std::unique_lock lock(mutex_);
  ready_cv_.wait(lock, [this] {
    return !ready_.empty() || active_workers_ == 0 || worker_error_;
  });
  if (worker_error_) std::rethrow_exception(worker_error_);
  if (ready_.empty()) return 0;
  DataChunk chunk = std::move(ready_.front());
  ready_.pop_front();
  lock.unlock();
  space_cv_.notify_one();

  if (chunk.Size() <= max_rows) {
    *destination = std::move(chunk);
    return destination->Size();
  }
  pending_ = std::move(chunk);
  pending_offset_ = 0;
  while (pending_offset_ < pending_->Size() &&
         destination->Size() < max_rows) {
    destination->Append(*pending_, pending_offset_++);
  }
  return destination->Size();
}

bool ParallelScan::Next(Row* destination, RowPosition* position) {
  if (scalar_offset_ == scalar_batch_.Size()) {
    if (NextBatch(&scalar_batch_) == 0) return false;
    scalar_offset_ = 0;
  }
  *destination = scalar_batch_.RowAt(scalar_offset_);
  if (position) *position = scalar_batch_.PositionAt(scalar_offset_);
  ++scalar_offset_;
  return true;
}

void ParallelScan::Dump(std::ostream& out, int /*indent*/) const {
  out << "ParallelScan: " << table_->GetSchema().Name() << " ("
      << worker_count_ << " workers, " << morsels_.size() << " morsels)";
}

}  // namespace tinylamb
