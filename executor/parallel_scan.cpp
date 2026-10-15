/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/parallel_scan.hpp"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <mutex>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/scan_filter.hpp"
#include "page/row_position.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

void ParallelScan::SetRowFilter(
    std::vector<Expression> conjuncts,
    relational_detail::CompiledScanFilter filter,
    relational_detail::WorkerScanFilterPrograms programs) {
  row_filter_conjuncts_ = std::move(conjuncts);
  row_filter_ = std::make_unique<relational_detail::CompiledScanFilter>(
      std::move(filter));
  row_filter_programs_ =
      std::make_unique<relational_detail::WorkerScanFilterPrograms>(
          std::move(programs));
}

ParallelScan::ParallelScan(Transaction& txn, const Table& table,
                           size_t worker_count, size_t pages_per_morsel,
                           std::optional<std::vector<slot_t>> projection,
                           std::vector<IntegerPeekCompare> peek_compares,
                           bool ordered)
    : txn_(&txn),
      table_(&table),
      projection_(std::move(projection)),
      peek_compares_(std::move(peek_compares)),
      ordered_(ordered),
      worker_count_(1),
      max_ready_chunks_(2) {
  StatusOr<TableLease> lease = table_->AcquireLease(txn_->ID());
  if (!lease.HasValue()) {
    worker_status_ = lease.GetStatus();
    return;
  }
  table_lease_ = lease.MoveValue();
  morsels_ = table_->BuildScanMorsels(*txn_, pages_per_morsel);
  morsels_done_.assign(morsels_.size(), false);
  worker_count_ = std::min(std::max<size_t>(1, worker_count),
                           std::max<size_t>(1, morsels_.size()));
  max_ready_chunks_ = std::max<size_t>(2, worker_count_ * 2);
}

ParallelScan::~ParallelScan() {
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

void ParallelScan::Start(size_t batch_size) {
  std::scoped_lock lock(mutex_);
  if (started_) {
    return;
  }
  started_ = true;
  if (morsels_.empty()) {
    return;
  }
  workers_.reserve(worker_count_);
  size_t launched = 0;
  for (size_t worker = 0; worker < worker_count_; ++worker) {
    try {
      workers_.emplace_back(
          [this, batch_size, worker] { RunWorker(batch_size, worker); });
      ++launched;
    } catch (...) {
      // Thread creation can fail (e.g. resource_unavailable). Account only
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

void ParallelScan::RunWorker(size_t batch_size, size_t worker_index) {
  try {
    // The projection layout never changes during the scan, so build the type
    // vector once and reuse it for every chunk refill.
    std::vector<ValueType> projection_types;
    if (projection_) {
      projection_types.reserve(projection_->size());
      for (slot_t slot : *projection_) {
        projection_types.push_back(table_->GetSchema().GetColumn(slot).Type());
      }
    } else {
      projection_types.reserve(table_->GetSchema().ColumnCount());
      for (size_t i = 0; i < table_->GetSchema().ColumnCount(); ++i) {
        projection_types.push_back(table_->GetSchema().GetColumn(i).Type());
      }
    }
    // A/B hatch: TINYLAMB_DISABLE_CHUNK_COPY_APPEND=1 restores move-append.
    static const bool copy_append_disabled =
        std::getenv("TINYLAMB_DISABLE_CHUNK_COPY_APPEND") != nullptr;
    // When the chunk layout holds only fixed-width cells (INT64/DATE/DOUBLE,
    // NULLs), appending by const reference is strictly cheaper than moving:
    // the copy is a scalar push per column, and the iterator keeps reusing
    // its decode buffer instead of re-allocating a fresh values vector (plus
    // fresh string cells) after every accepted row hands its old one to the
    // chunk.  VARCHAR/ARRAY layouts keep the move so long text still changes
    // hands without a deep copy.
    bool copy_append = !copy_append_disabled;
    for (const ValueType type : projection_types) {
      if (type == ValueType::kVarChar || type == ValueType::kArray) {
        copy_append = false;
        break;
      }
    }
    // Static rotation: worker w decodes morsels w, w+W, w+2W, ... .  The
    // atomic counter this replaced handed morsels to whichever worker grabbed
    // them; combined with the ordered drain below it would even deadlock (a
    // worker blocked on a full queue holds a later morsel while the morsel
    // the consumer waits for is still unclaimed).  Fixed ownership keeps
    // every morsel in flight under its owner from the start.
    for (size_t morsel_index = worker_index; morsel_index < morsels_.size();
         morsel_index += worker_count_) {
      if (cancelled_.load(std::memory_order_relaxed)) {
        return;
      }
      const std::vector<IntegerPeekCompare>* peek_ptr =
          peek_compares_.empty() ? nullptr : &peek_compares_;
      Iterator iterator =
          table_->BeginMorselScan(*txn_, morsels_[morsel_index], projection_,
                                  nullptr, std::nullopt, peek_ptr);
      // The layout never changes across refill, so Initialize is the single
      // reset path for both the first and subsequent chunks.
      DataChunk chunk;
      if (projection_) {
        chunk.Initialize(projection_types, batch_size);
      } else {
        chunk.Initialize(table_->GetSchema(), batch_size);
      }
      while (iterator.IsValid()) {
        if (cancelled_.load(std::memory_order_relaxed)) {
          iterator.DropPageLatch();
          return;
        }
        const RowPosition position = iterator.Position();
        if (row_filter_ != nullptr) {
          // Decide the row inside the decode worker: rejected rows never
          // reach a chunk, so the consumer thread pays nothing for them.
          const int decision = relational_detail::MatchScanFilterWorker(
              *iterator, table_->GetSchema(), *row_filter_,
              *row_filter_programs_);
          if (decision == 0) {
            ++iterator;
            continue;
          }
          if (decision == 2) {
            // The program raised; park the row for the context-free AST
            // re-check on the consumer thread (error semantics must match
            // the serial Selection, and AST evaluation is not worker-safe).
            std::scoped_lock deferred_lock(deferred_mutex_);
            deferred_.emplace_back(*iterator, position);
            ++iterator;
            continue;
          }
        }
        if (copy_append) {
          chunk.Append(static_cast<const Row&>(*iterator), position);
        } else {
          chunk.Append(std::move(*iterator), position);
        }
        ++iterator;
        if (chunk.Size() == batch_size) {
          // Never block on a full output queue while holding a page latch:
          // a writer waiting for that latch could be the very consumer that
          // would otherwise drain the queue.
          iterator.DropPageLatch();
          if (!Enqueue(morsel_index, std::move(chunk))) {
            return;
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
      bool scan_failed = false;
      if (iterator.GetStatus() != Status::kSuccess) {
        std::scoped_lock lock(mutex_);
        if (worker_status_ == Status::kSuccess) {
          worker_status_ = iterator.GetStatus();
        }
        cancelled_.store(true, std::memory_order_release);
        scan_failed = true;
      }
      iterator.DropPageLatch();

      if (!scan_failed && !chunk.Empty()) {
        if (!Enqueue(morsel_index, std::move(chunk))) {
          return;
        }
        // Handing off moved out of `chunk`; restore a well-defined state so
        // the next morsel iteration starts from a fresh object.
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

bool ParallelScan::Enqueue(size_t morsel_index, DataChunk chunk) {
  std::unique_lock lock(mutex_);
  // Backpressure with an escape hatch: the queue cap applies to chunks of
  // morsels ahead of the consumer, but a chunk the consumer is currently
  // waiting for (morsel_index <= next_out_morsel_) is always admitted.  With
  // static morsel ownership this is what makes progress unconditional: the
  // owner of the waited-for morsel is the only worker that works on morsels
  // <= next_out_morsel_ before it, so it can never be the one blocked.
  space_cv_.wait(lock, [&] {
    return cancelled_.load(std::memory_order_relaxed) ||
           inflight_chunks_ < max_ready_chunks_ ||
           morsel_index <= next_out_morsel_;
  });
  if (cancelled_.load(std::memory_order_relaxed)) {
    return false;
  }
  ++inflight_chunks_;
  inflight_[morsel_index].push_back(std::move(chunk));
  lock.unlock();
  ready_cv_.notify_one();
  return true;
}

size_t ParallelScan::NextBatch(DataChunk* destination, size_t max_rows) {
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
             !inflight_[next_out_morsel_].empty();
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
                             "parallel scan worker failed"));
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
      lock.unlock();
      // Every morsel is decoded and drained; rows parked by the worker
      // filter (programs raised) are re-checked here before reporting
      // end-of-scan, so their survivors are not silently dropped.
      DrainDeferred(destination, max_rows);
      return destination->Size();
    }
    std::deque<DataChunk>& queue = inflight_[next_out_morsel_];
    if (queue.empty()) {
      // The morsel finished with nothing queued (all rows filtered out):
      // advance past it instead of reporting a false end-of-scan.
      inflight_.erase(next_out_morsel_);
      ++next_out_morsel_;
      // Advancing the admission boundary can turn a parked producer's
      // Enqueue predicate (morsel_index <= next_out_morsel_) true.  Without
      // a wakeup here the consumer sleeps on ready_cv_ waiting for chunks
      // only that producer may push while the producer sleeps on space_cv_
      // with a stale predicate evaluation — a permanent deadlock.  notify_
      // all, not one: a single wakeup can be consumed by a producer of a
      // later morsel whose predicate stays false, sending it straight back
      // to sleep while the admissible producer keeps waiting.
      space_cv_.notify_all();
      continue;
    }
    DataChunk chunk = std::move(queue.front());
    queue.pop_front();
    --inflight_chunks_;
    lock.unlock();
    // Same reasoning as above: whichever parked producer is admissible (the
    // freed slot or the admission hatch) must actually observe the state
    // change, and one random wakeup cannot guarantee that.
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

bool ParallelScan::Next(Row* destination, RowPosition* position) {
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

size_t ParallelScan::DrainDeferred(DataChunk* destination, size_t max_rows) {
  if (row_filter_ == nullptr || deferred_drained_) {
    return 0;
  }
  if (deferred_offset_ >= deferred_.size()) {
    deferred_drained_ = true;
    return 0;
  }
  // All workers are done, so deferred_ is stable; take the lock once for
  // the happens-before edge with the worker pushes.  Park order was racy
  // across workers; drain in physical row order so a raised program cannot
  // make the surviving row order scheduling-dependent.
  {
    std::scoped_lock deferred_lock(deferred_mutex_);
    std::sort(deferred_.begin() + static_cast<std::ptrdiff_t>(deferred_offset_),
              deferred_.end(), [](const auto& lhs, const auto& rhs) {
                return std::tie(lhs.second.page_id, lhs.second.slot) <
                       std::tie(rhs.second.page_id, rhs.second.slot);
              });
  }
  if (destination->Size() == 0) {
    destination->Reset(table_->GetSchema(), max_rows);
  }
  while (deferred_offset_ < deferred_.size() &&
         destination->Size() < max_rows) {
    const auto& [row, position] = deferred_[deferred_offset_++];
    StatusOr<bool> pass = relational_detail::EvaluateConjunctsTolerant(
        row_filter_conjuncts_, [&](const Expression& conjunct) {
          return conjunct->TryEvaluate(row, table_->GetSchema());
        });
    if (!pass.HasValue()) {
      deferred_drained_ = true;
      FailWith(pass.GetStatus());
      return destination->Size();
    }
    if (pass.Value()) {
      destination->Append(row, position);
    }
  }
  if (deferred_offset_ >= deferred_.size()) {
    deferred_drained_ = true;
  }
  return destination->Size();
}

void ParallelScan::Dump(std::ostream& out, int /*indent*/) const {
  out << "ParallelScan: " << table_->GetSchema().Name() << " (" << worker_count_
      << " workers, " << morsels_.size() << " morsels)";
  if (projection_.has_value()) {
    out << " columns=" << projection_->size() << "/"
        << table_->GetSchema().ColumnCount();
  }
  if (row_filter_ != nullptr) {
    out << " worker-filter";
  }
}

}  // namespace tinylamb
