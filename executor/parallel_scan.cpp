/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/parallel_scan.hpp"

#include <algorithm>
#include <cstddef>
#include <optional>
#include <mutex>
#include <exception>
#include <ostream>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "page/row_position.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

ParallelScan::ParallelScan(
    Transaction& txn, const Table& table, size_t worker_count,
    size_t pages_per_morsel,
    std::optional<std::vector<slot_t>> projection,
    std::vector<IntegerPeekCompare> peek_compares)
    : txn_(&txn),
      table_(&table),
      projection_(std::move(projection)),
      peek_compares_(std::move(peek_compares)),
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
  if (started_) { return;
}
  started_ = true;
  if (morsels_.empty()) { return;
}
  workers_.reserve(worker_count_);
  size_t launched = 0;
  for (size_t worker = 0; worker < worker_count_; ++worker) {
    try {
      workers_.emplace_back(
          [this, batch_size] { RunWorker(batch_size); });
      ++launched;
    } catch (...) {
      // Thread creation can fail (e.g. resource_unavailable). Account only
      // for the workers that actually started and wake the consumer via
      // worker_error_ instead of blocking on active_workers_ forever.
      if (!worker_error_) { worker_error_ = std::current_exception();
}
      break;
    }
  }
  active_workers_ = launched;
  ready_cv_.notify_all();
}

void ParallelScan::RunWorker(size_t batch_size) {
  try {
    // The projection layout never changes during the scan, so build the type
    // vector once and reuse it for every chunk refill.
    std::vector<ValueType> projection_types;
    if (projection_) {
      projection_types.reserve(projection_->size());
      for (slot_t slot : *projection_) {
        projection_types.push_back(table_->GetSchema().GetColumn(slot).Type());
      }
    }
    while (true) {
      const size_t morsel_index = next_morsel_.fetch_add(1);
      if (morsel_index >= morsels_.size()) { break;
}
      const std::vector<IntegerPeekCompare>* peek_ptr =
          peek_compares_.empty() ? nullptr : &peek_compares_;
      Iterator iterator = table_->BeginMorselScan(
          *txn_, morsels_[morsel_index], projection_, nullptr,
          std::nullopt, peek_ptr);
      // The layout never changes across refill, so Initialize is the single
      // reset path for both the first and subsequent chunks.
      DataChunk chunk;
      if (projection_) {
        chunk.Initialize(projection_types, batch_size);
      } else {
        chunk.Initialize(table_->GetSchema(), batch_size);
      }
      while (iterator.IsValid()) {
        chunk.Append(*iterator, iterator.Position());
        ++iterator;
        if (chunk.Size() == batch_size) {
          // Never block on a full output queue while holding a page latch:
          // a writer waiting for that latch could be the very consumer that
          // would otherwise drain the queue.
          iterator.DropPageLatch();
          if (!Enqueue(std::move(chunk))) { return;
}
          // The chunk above was handed off; start the refill from a
          // well-defined state instead of re-initializing a moved-from one.
          chunk = DataChunk{};
          if (projection_) {
            chunk.Initialize(projection_types, batch_size);
          } else {
            chunk.Initialize(table_->GetSchema(), batch_size);
          }
        }
      }
      iterator.DropPageLatch();
      if (!chunk.Empty()) {
        if (!Enqueue(std::move(chunk))) { return;
}
        // Handing off moved out of `chunk`; restore a well-defined state so
        // the next morsel iteration starts from a fresh object.
        chunk = DataChunk{};
      }
    }
  } catch (...) {
    std::scoped_lock lock(mutex_);
    if (!worker_error_) { worker_error_ = std::current_exception();
}
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
  if (cancelled_) { return false;
}
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

  // Workers use a fixed chunk size; per-consumer max_rows is absorbed by the
  // pending_ split below so the first caller cannot dictate global capacity.
  Start(kDefaultVectorSize);
  std::unique_lock lock(mutex_);
  ready_cv_.wait(lock, [this] {
    return !ready_.empty() || active_workers_ == 0 || worker_error_;
  });
  if (worker_error_) { std::rethrow_exception(worker_error_);
}
  if (ready_.empty()) { return 0;
}
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
    if (NextBatch(&scalar_batch_) == 0) { return false;
}
    scalar_offset_ = 0;
  }
  *destination = scalar_batch_.RowAt(scalar_offset_);
  if (position != nullptr) { *position = scalar_batch_.PositionAt(scalar_offset_);
}
  ++scalar_offset_;
  return true;
}

void ParallelScan::Dump(std::ostream& out, int /*indent*/) const {
  out << "ParallelScan: " << table_->GetSchema().Name() << " ("
      << worker_count_ << " workers, " << morsels_.size() << " morsels)";
}

}  // namespace tinylamb
