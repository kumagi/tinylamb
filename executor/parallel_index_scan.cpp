/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/parallel_index_scan.hpp"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <ostream>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "executor/index_scan.hpp"
#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"
#include "index/index_schema.hpp"
#include "page/row_position.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"

namespace tinylamb {

ParallelIndexScan::ParallelIndexScan(
    Transaction& txn, const Table& table, const Index& index,
    const std::vector<Value>& begin_key, const std::vector<Value>& end_key,
    bool ascending, size_t worker_count, size_t leaves_per_morsel,
    TransactionManager::IndexReadGuard index_read_guard)
    : txn_(&txn),
      table_(&table),
      index_(&index),
      ascending_(ascending),
      schema_(table.GetSchema()),
      index_read_guard_(index_read_guard.HoldsLock()
                            ? std::move(index_read_guard)
                            : txn.AcquireIndexReadGuard(index.Root())),
      worker_count_(1) {
  StatusOr<TableLease> lease = table.AcquireLease(txn.ID());
  if (!lease.HasValue()) {
    worker_status_ = lease.GetStatus();
    return;
  }
  table_lease_ = lease.MoveValue();
  key_slots_.assign(index.sc_.key_.begin(), index.sc_.key_.end());
  // RangePartitionedScan: leaf-aligned boundaries turn the key range into
  // half-open morsels [b_i, b_{i+1}); the first morsel starts at the scan's
  // own begin and the last ends at its own end.  A navigation failure falls
  // back to a single morsel (the serial shape); the worker's own scan
  // surfaces the failure with a status.
  std::vector<std::vector<Value>> splits;
  StatusOr<std::vector<Row>> boundaries =
      IndexScanIterator::PartitionBoundaries(index, txn, begin_key, end_key,
                                             leaves_per_morsel);
  if (boundaries.HasValue()) {
    splits.reserve(boundaries.Value().size());
    for (Row& row : boundaries.Value()) {
      splits.push_back(std::move(row.values_));
    }
  }
  std::vector<Value> cursor = begin_key;
  for (std::vector<Value>& split : splits) {
    if (split == cursor) {
      continue;  // Duplicate boundary: the morsel would be empty.
    }
    Morsel morsel;
    morsel.begin_key = cursor;
    morsel.end_key = split;
    morsel.exclusive_end = true;
    morsels_.push_back(std::move(morsel));
    cursor = std::move(split);
  }
  Morsel tail;
  tail.begin_key = cursor;
  tail.end_key = end_key;
  tail.exclusive_end = false;
  morsels_.push_back(std::move(tail));
  morsel_chunk_.resize(morsels_.size());
  morsels_done_.assign(morsels_.size(), false);
  worker_count_ = std::min(std::max<size_t>(1, worker_count),
                           std::max<size_t>(1, morsels_.size()));
}

ParallelIndexScan::~ParallelIndexScan() {
  {
    std::scoped_lock lock(mutex_);
    cancelled_.store(true, std::memory_order_release);
  }
  ready_cv_.notify_all();
  space_cv_.notify_all();
  // jthread joins in its destructor.  Join while the mutexes and condition
  // variables used by workers are still alive.
  workers_.clear();
}

void ParallelIndexScan::Start(size_t batch_size) {
  std::scoped_lock lock(mutex_);
  if (started_) {
    return;
  }
  started_ = true;
  workers_.reserve(worker_count_);
  size_t launched = 0;
  for (size_t worker = 0; worker < worker_count_; ++worker) {
    try {
      workers_.emplace_back(
          [this, batch_size, worker] { RunWorker(batch_size, worker); });
      ++launched;
    } catch (...) {
      // Thread creation can fail (e.g. resource_unavailable).  Account only
      // for the workers that actually started and wake the consumer via
      // worker_error_ instead of blocking on active_workers_ forever.
      if (!worker_error_) {
        worker_error_ = std::current_exception();
      }
      break;
    }
  }
  active_workers_ = launched;
  ready_cv_.notify_all();
}

bool ParallelIndexScan::KeyEqualsBoundary(
    const Row& row, const std::vector<Value>& boundary) const {
  if (boundary.size() > key_slots_.size()) {
    return false;
  }
  for (size_t key = 0; key < boundary.size(); ++key) {
    if (!(row[key_slots_[key]] == boundary[key])) {
      return false;
    }
  }
  return true;
}

void ParallelIndexScan::RunWorker(size_t batch_size, size_t worker_index) {
  try {
    // Static rotation: worker w scans morsels w, w+W, w+2W, ... .  Fixed
    // ownership is what makes the ordered handoff deadlock-free: the owner of
    // the morsel the consumer waits for is never blocked behind a later one
    // it has not claimed yet.
    for (size_t morsel_index = worker_index; morsel_index < morsels_.size();
         morsel_index += worker_count_) {
      if (cancelled_.load(std::memory_order_relaxed)) {
        return;
      }
      const Morsel& morsel = morsels_[morsel_index];
      IndexScan scan(*txn_, *table_, *index_, morsel.begin_key, morsel.end_key,
                     ascending_, Expression(), schema_);
      DataChunk chunk;
      chunk.Initialize(table_->GetSchema(), batch_size);
      Row row;
      RowPosition position;
      bool scan_failed = false;
      while (scan.Next(&row, &position)) {
        if (cancelled_.load(std::memory_order_relaxed)) {
          return;
        }
        if (morsel.exclusive_end && KeyEqualsBoundary(row, morsel.end_key)) {
          // The next morsel owns this key (half-open partition): stop here
          // instead of emitting a duplicate.
          break;
        }
        chunk.Append(std::move(row), position);
        row = Row();
        if (chunk.Size() == batch_size) {
          if (!Deposit(morsel_index, std::move(chunk))) {
            return;
          }
          // The chunk above was handed off; start the refill from a
          // well-defined state instead of re-initializing a moved-from one.
          chunk = DataChunk{};
          chunk.Initialize(table_->GetSchema(), batch_size);
        }
      }
      if (scan.GetStatus() != Status::kSuccess) {
        std::scoped_lock lock(mutex_);
        if (worker_status_ == Status::kSuccess) {
          worker_status_ = scan.GetStatus();
        }
        cancelled_.store(true, std::memory_order_release);
        scan_failed = true;
      }
      if (!scan_failed && !chunk.Empty()) {
        if (!Deposit(morsel_index, std::move(chunk))) {
          return;
        }
        chunk = DataChunk{};
      }
      {
        // The morsel is fully decoded and all of its chunks are queued; the
        // consumer may now advance past it once it drained them (an empty
        // morsel records only this flag).
        std::scoped_lock lock(mutex_);
        morsels_done_[morsel_index] = true;
      }
      ready_cv_.notify_all();
    }
  } catch (...) {
    std::scoped_lock lock(mutex_);
    if (!worker_error_) {
      worker_error_ = std::current_exception();
    }
    cancelled_.store(true, std::memory_order_release);
  }
  {
    std::scoped_lock lock(mutex_);
    --active_workers_;
  }
  ready_cv_.notify_all();
  space_cv_.notify_all();
}

bool ParallelIndexScan::Deposit(size_t morsel_index, DataChunk chunk) {
  std::unique_lock lock(mutex_);
  // Backpressure: the deposit waits while the consumer has not drained the
  // previous chunk of this morsel.  Progress is unconditional -- the owner of
  // the morsel the consumer waits for is exactly this thread, and it only
  // waits for a slot the consumer will drain in morsel order.
  space_cv_.wait(lock, [&] {
    return cancelled_.load(std::memory_order_relaxed) ||
           !morsel_chunk_[morsel_index].has_value();
  });
  if (cancelled_.load(std::memory_order_relaxed)) {
    return false;
  }
  morsel_chunk_[morsel_index] = std::move(chunk);
  lock.unlock();
  ready_cv_.notify_one();
  return true;
}

size_t ParallelIndexScan::NextBatch(DataChunk* destination, size_t max_rows) {
  max_rows = std::max<size_t>(1, max_rows);
  destination->Reset();

  // Workers use a fixed chunk size; per-consumer max_rows is absorbed by the
  // pending_ split below so the first caller cannot dictate global capacity.
  Start(kDefaultVectorSize);
  while (destination->Size() < max_rows) {
    if (pending_) {
      while (pending_offset_ < pending_->Size() &&
             destination->Size() < max_rows) {
        destination->Append(*pending_, pending_offset_++);
      }
      if (pending_offset_ == pending_->Size()) {
        pending_.reset();
        pending_offset_ = 0;
      } else {
        return destination->Size();
      }
    }

    std::unique_lock lock(mutex_);
    ready_cv_.wait(lock, [&] {
      return cancelled_.load(std::memory_order_relaxed) || worker_error_ ||
             worker_status_ != Status::kSuccess ||
             next_out_morsel_ >= morsels_.size() ||
             morsels_done_[next_out_morsel_] ||
             morsel_chunk_[next_out_morsel_].has_value();
    });
    if (worker_error_) {
      // Surface the worker failure as a sticky Status instead of unwinding
      // through NextBatch (no-exception executor contract).
      try {
        std::rethrow_exception(worker_error_);
      } catch (const std::exception& error) {
        FailWith(StatusError(StatusCode::kRuntimeError, error.what()));
      } catch (...) {
        FailWith(StatusError(StatusCode::kRuntimeError,
                             "parallel index scan worker failed"));
      }
      return 0;
    }
    if (worker_status_ != Status::kSuccess) {
      FailWith(worker_status_);
      return 0;
    }
    if (cancelled_.load(std::memory_order_relaxed)) {
      return destination->Size();
    }
    if (next_out_morsel_ >= morsels_.size()) {
      return destination->Size();
    }
    if (morsels_done_[next_out_morsel_] &&
        !morsel_chunk_[next_out_morsel_].has_value()) {
      // The morsel finished with nothing queued (all rows filtered out or
      // the range was empty): advance past it instead of reporting a false
      // end-of-scan.
      ++next_out_morsel_;
      space_cv_.notify_all();
      continue;
    }
    DataChunk chunk = std::move(*morsel_chunk_[next_out_morsel_]);
    morsel_chunk_[next_out_morsel_].reset();
    lock.unlock();
    // Whichever producer is parked on this slot must observe the freed one;
    // one random wakeup cannot guarantee that.
    space_cv_.notify_all();

    if (destination->Size() == 0 && chunk.Size() <= max_rows) {
      // Whole-chunk handover keeps the chunk's zone maps intact (move
      // instead of a row-wise re-append).
      *destination = std::move(chunk);
      return destination->Size();
    }
    const size_t room = max_rows - destination->Size();
    if (chunk.Size() > room) {
      pending_ = std::move(chunk);
      pending_offset_ = 0;
      while (destination->Size() < max_rows) {
        destination->Append(*pending_, pending_offset_++);
      }
      return destination->Size();
    }
    for (size_t index = 0; index < chunk.Size(); ++index) {
      destination->Append(chunk, index);
    }
  }
  return destination->Size();
}

bool ParallelIndexScan::Next(Row* destination, RowPosition* position) {
  if (scalar_offset_ == scalar_batch_.Size()) {
    if (NextBatch(&scalar_batch_) == 0) {
      return false;
    }
    scalar_offset_ = 0;
  }
  *destination = scalar_batch_.RowAt(scalar_offset_);
  if (position != nullptr) {
    *position = scalar_batch_.PositionAt(scalar_offset_);
  }
  ++scalar_offset_;
  return true;
}

void ParallelIndexScan::Dump(std::ostream& out, int /*indent*/) const {
  out << "ParallelIndexScan: " << table_->GetSchema().Name() << " with "
      << index_->sc_.name_ << " (" << worker_count_ << " workers, "
      << morsels_.size() << " morsels)";
  if (!ascending_) {
    out << " reverse";
  }
}

}  // namespace tinylamb
