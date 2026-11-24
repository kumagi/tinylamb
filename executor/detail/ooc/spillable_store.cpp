/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/ooc/spillable_store.hpp"

#include <algorithm>
#include <limits>
#include <new>
#include <stdexcept>
#include <utility>

#include "common/memory_budget.hpp"
#include "executor/detail/ooc/chunk_codec.hpp"
#include "type/row_block_codec.hpp"

namespace tinylamb {

SpillableStore::SpillableStore(Options options)
    : options_(options),
      spill_store_alive_(options_.spill_store != nullptr
                             ? options_.spill_store->LifetimeToken()
                             : std::weak_ptr<const bool>{}) {
  if (options_.grant != nullptr) {
    consumer_guard_.emplace(options_.grant);
    const size_t refill =
        options_.memory_budget_bytes > 0
            ? std::min<size_t>(
                  QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
                  std::max<size_t>(65536, options_.memory_budget_bytes / 4))
            : QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes;
    grant_cache_.emplace(options_.grant, refill);
  }
}

SpillableStore::~SpillableStore() { Reset(); }

size_t SpillableStore::EffectiveBudgetBytes() const noexcept {
  size_t b = options_.memory_budget_bytes;
  if (options_.grant != nullptr) {
    const size_t grant_op = options_.grant->OperatorBudgetBytes();
    if (b == 0 || grant_op < b) {
      b = grant_op;
    }
  }
  return b;
}

size_t SpillableStore::EffectiveIoBufferBytes() const noexcept {
  const size_t budget = EffectiveBudgetBytes();
  // EffectiveBudgetBytes() already folds in the grant's operator budget. When
  // there is a budget, clamp the buffer to a quarter of it (4 KiB floor) so it
  // cannot grow the reserved footprint. With no budget the caller has
  // explicitly opted out of reservation accounting, so honor io_buffer_bytes
  // as given: silently capping an explicit tuning value would only regress
  // sequential spill throughput.
  if (budget > 0) {
    return std::min(options_.io_buffer_bytes,
                    std::max<size_t>(4096, budget / 4));
  }
  return options_.io_buffer_bytes;
}

size_t SpillableStore::StagingFlushBytes() const noexcept {
  const size_t budget = EffectiveBudgetBytes();
  if (budget == 0) {
    return 0;
  }
  return std::max<size_t>(4096, std::min<size_t>(budget / 4, 256ULL << 10));
}

Status SpillableStore::FlushStagingChunk() {
  if (staging_chunk_.Empty()) {
    if (staging_reserved_bytes_ > 0 && grant_cache_.has_value()) {
      grant_cache_->Release(staging_reserved_bytes_);
      staging_reserved_bytes_ = 0;
    }
    return Status::kSuccess;
  }
  // Transfer the existing charge along with the staged rows. On I/O failure
  // both still belong to staging and an independent consumer cannot take it.
  RETURN_IF_FAIL(AppendReadyChunk(std::move(staging_chunk_), staging_reserved_bytes_));
  staging_chunk_.Reset();
  staging_bytes_ = 0;
  staging_reserved_bytes_ = 0;
  return Status::kSuccess;
}

Status SpillableStore::SpillChunk(const DataChunk& chunk) {
  if (writer_ == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "SpillableStore missing SpillSegmentWriter");
  }
  const uint64_t before = writer_->BytesWritten();
  Status st = WriteDataChunk(writer_.get(), chunk, &scratch_codec_,
                                   StagingFlushBytes());
  // Trim even when the write fails: a partially encoded oversized frame still
  // grew scratch_codec_, so leaving that capacity behind would keep it charged
  // against the operator budget on every retry.
  TrimCodecScratch();
  if (st != Status::kSuccess) {
    return st;
  }
  spilled_bytes_ += writer_->BytesWritten() - before;
  return Status::kSuccess;
}

void SpillableStore::TrimCodecScratch() {
  const size_t threshold =
      std::max<size_t>(64ULL << 10, StagingFlushBytes() * 4);
  if (scratch_codec_.capacity() > threshold) {
    std::string empty;
    scratch_codec_.swap(empty);
  }
}

Status SpillableStore::TransitionToSpill() {
  if (spilled_) {
    return Status::kSuccess;
  }
  if (options_.spill_store == nullptr) {
    return StatusError(
        StatusCode::kMemoryLimitExceeded,
        "SpillableStore exceeded memory budget with no SpillStore configured");
  }
  auto writer = std::make_unique<SpillSegmentWriter>(options_.spill_store,
                                                     EffectiveIoBufferBytes());
  // Every failure path below leaves mem_chunks_ intact and spilled_ false, so
  // the caller may retry. Abort the half-built segment explicitly on each of
  // them instead of relying on the local unique_ptr's destructor: the writer
  // drops the same segment either way, but making it explicit keeps the
  // cleanup contract true for every I/O boundary, not just Flush().
  Status opened = writer->EnsureOpen();
  if (opened != Status::kSuccess) {
    writer->Abort();
    return opened;
  }
  for (const DataChunk& chunk : mem_chunks_) {
    const Status st = WriteDataChunk(writer.get(), chunk, &scratch_codec_,
                                     StagingFlushBytes());
    if (st != Status::kSuccess) {
      writer->Abort();
      return st;
    }
    TrimCodecScratch();
  }
  Status flush = writer->Flush();
  if (flush != Status::kSuccess) {
    writer->Abort();
    return flush;
  }
  spilled_bytes_ += writer->BytesWritten();
  writer_ = std::move(writer);
  // Swap with an empty vector to release capacity without allocating.
  std::vector<DataChunk>().swap(mem_chunks_);
  if (grant_cache_.has_value()) {
    if (staging_reserved_bytes_ == 0) { grant_cache_->ReleaseAll();
    } else { grant_cache_->Release(mem_bytes_);
}
  }
  mem_bytes_ = 0;
  spilled_ = true;
  return Status::kSuccess;
}

template <typename ChunkRef>
Status SpillableStore::AppendChunkImpl(ChunkRef&& chunk) {
  if (failure_status_ != Status::kSuccess) {
    return failure_status_;
  }
  if (sealed_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "cannot append to sealed SpillableStore");
  }
  if (chunk.Empty()) {
    // A zero-row batch carries no rows: do not flush staging or trigger a
    // spill just because the pipeline produced an empty chunk after a filter.
    return Status::kSuccess;
  }
  RETURN_IF_FAIL(FlushStagingChunk());

  return AppendReadyChunk(std::forward<ChunkRef>(chunk), 0);
}

template <typename ChunkRef>
Status SpillableStore::AppendReadyChunk(ChunkRef&& chunk, size_t reserved) {
  const size_t chunk_rows = chunk.Size();
  if (spilled_) {
    RETURN_IF_FAIL(SpillChunk(chunk));
    if (grant_cache_) { grant_cache_->Release(reserved);
}
    total_rows_ += chunk_rows;
    return Status::kSuccess;
  }

  const size_t chunk_bytes = EstimateChunkMemoryBytes(chunk);
  const size_t budget = EffectiveBudgetBytes();
  bool need_spill = options_.force_spill ||
                    ProcessMemoryBudget::Global().AlwaysSpillEnabled();
  const size_t resident_limit =
      budget == 0 ? std::numeric_limits<size_t>::max() : budget;
  if (!need_spill &&
      chunk_bytes > resident_limit - std::min(mem_bytes_, resident_limit)) {
    need_spill = true;
  }
  const size_t extra = chunk_bytes - std::min(reserved, chunk_bytes);
  if (!need_spill && grant_cache_.has_value()) {
    if (!grant_cache_->Reserve(extra)) {
      need_spill = true;
    }
  }

  if (need_spill) {
    RETURN_IF_FAIL(TransitionToSpill());
    RETURN_IF_FAIL(SpillChunk(chunk));
    if (grant_cache_) { grant_cache_->Release(reserved);
}
    total_rows_ += chunk_rows;
    return Status::kSuccess;
  }

  try {
    mem_chunks_.push_back(std::forward<ChunkRef>(chunk));
  } catch (const std::bad_alloc&) {
    if (grant_cache_) { grant_cache_->Release(extra);
}
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "spillable store chunk buffer allocation failed");
  }
  if (grant_cache_ && reserved > chunk_bytes) { grant_cache_->Release(reserved - chunk_bytes);
}
  mem_bytes_ += chunk_bytes;
  peak_mem_bytes_ = std::max(peak_mem_bytes_, mem_bytes_);
  total_rows_ += chunk_rows;
  return Status::kSuccess;
}

Status SpillableStore::AppendChunk(const DataChunk& chunk) {
  return AppendChunkImpl(chunk);
}

Status SpillableStore::AppendChunk(DataChunk&& chunk) {
  return AppendChunkImpl(std::move(chunk));
}

Status SpillableStore::AppendRow(const Row& row, RowPosition pos) {
  if (failure_status_ != Status::kSuccess) {
    return failure_status_;
  }
  try {
    if (sealed_) {
      return StatusError(StatusCode::kInvalidArgument,
                         "cannot append to sealed SpillableStore");
    }
    const size_t content_bytes = EstimateRowMemoryBytes(row);
    if (content_bytes > std::numeric_limits<size_t>::max() - sizeof(RowPosition)) {
      return StatusError(StatusCode::kTooBigData,
                         "row memory exceeds SpillableStore size range");
    }
    const size_t row_bytes = content_bytes + sizeof(RowPosition);
    if (row_bytes > std::numeric_limits<size_t>::max() - staging_bytes_) {
      RETURN_IF_FAIL(FlushStagingChunk());
    }
    if (grant_cache_.has_value()) {
      auto& grant_cache = grant_cache_.value();
      if (!grant_cache.Reserve(row_bytes)) {
        if (!staging_chunk_.Empty()) {
          RETURN_IF_FAIL(FlushStagingChunk());
        }
        if (!grant_cache.Reserve(row_bytes)) {
          if (!spilled_) {
            RETURN_IF_FAIL(TransitionToSpill());
          }
          // A row larger than the available grant must still be spillable.
          // Do not retain it uncharged in the staging buffer between calls.
          DataChunk single;
          single.Append(row, pos);
          RETURN_IF_FAIL(SpillChunk(single));
          ++total_rows_;
          return Status::kSuccess;
        }
      }
      staging_reserved_bytes_ += row_bytes;
    }
    staging_chunk_.Append(row, pos);
    staging_bytes_ += row_bytes;
    const size_t budget = EffectiveBudgetBytes();
    const size_t staging_cap =
        budget > 0
            ? std::max<size_t>(
              4096, std::min<size_t>(budget / 4, 256ULL << 10))
            : (256ULL << 10);
    if (staging_chunk_.Size() >= kDefaultVectorSize ||
        staging_bytes_ >= staging_cap ||
        (!spilled_ && budget > 0 &&
         staging_bytes_ >= budget - std::min(mem_bytes_, budget))) {
      RETURN_IF_FAIL(FlushStagingChunk());
    }
  return Status::kSuccess;
  } catch (const std::bad_alloc&) {
    failure_status_ = Status::kMemoryLimitExceeded;
    return failure_status_;
  } catch (const std::length_error&) {
    failure_status_ = Status::kMemoryLimitExceeded;
    return failure_status_;
  }
}

Status SpillableStore::Seal() {
  if (failure_status_ != Status::kSuccess) {
    return failure_status_;
  }
  if (sealed_) {
    return Status::kSuccess;
  }
  RETURN_IF_FAIL(FlushStagingChunk());
  if (spilled_ && writer_ != nullptr) {
    ASSIGN_OR_RETURN(SpillSegmentId, id, writer_->Finish(true));
    segment_id_ = id;
    writer_.reset();
    scratch_codec_.clear();
    // T3462: shrink_to_fit can throw bad_alloc across the Status boundary;
    // swap-with-empty frees the capacity without allocating (T2060 idiom).
    std::string().swap(scratch_codec_);
  }
  sealed_ = true;
  mem_cursor_idx_ = 0;
  active_read_chunk_.Reset();
  active_read_row_ = 0;
  return Status::kSuccess;
}

Status SpillableStore::ResetCursor() {
  if (failure_status_ != Status::kSuccess) {
    return failure_status_;
  }
  if (!sealed_) {
    RETURN_IF_FAIL(Seal());
  }
  mem_cursor_idx_ = 0;
  active_read_chunk_.Reset();
  active_read_row_ = 0;
  reader_.reset();
  if (spilled_ && segment_id_ != kInvalidSpillSegmentId) {
    // The reader stores a raw SpillStore pointer. Check the liveness token
    // captured at construction before handing it that pointer, otherwise a
    // SpillStore destroyed ahead of this SpillableStore would be dereferenced.
    if (options_.spill_store == nullptr || spill_store_alive_.expired()) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "SpillStore is no longer alive; cannot rewind SpillableStore");
    }
    reader_ = std::make_unique<SpillSegmentReader>(
        options_.spill_store, segment_id_, EffectiveIoBufferBytes());
    RETURN_IF_FAIL(reader_->Open());
  }
  return Status::kSuccess;
}

StatusOr<bool> SpillableStore::NextChunk(DataChunk* chunk_out) {
  if (failure_status_ != Status::kSuccess) {
    return failure_status_;
  }
  if (chunk_out == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null chunk_out in SpillableStore::NextChunk");
  }
  if (!sealed_) {
    RETURN_IF_FAIL(ResetCursor());
  }
  if (!spilled_) {
    if (mem_cursor_idx_ >= mem_chunks_.size()) {
      chunk_out->Reset();
      return false;
    }
    // Copy, not move: ResetCursor() explicitly supports repeated sequential
    // scans over the in-memory chunks, so the store must retain its own copy
    // of every batch. The spilled path can reuse a decode buffer instead.
    *chunk_out = mem_chunks_[mem_cursor_idx_++];
    return true;
  }
  if (reader_ == nullptr) {
    RETURN_IF_FAIL(ResetCursor());
    if (reader_ == nullptr) {
      chunk_out->Reset();
      return false;
    }
  }
  ASSIGN_OR_RETURN(bool, got,
                   ReadDataChunk(reader_.get(), chunk_out, &scratch_codec_));
  TrimCodecScratch();
  return got;
}

StatusOr<bool> SpillableStore::NextRow(Row* row_out, RowPosition* pos_out) {
  if (failure_status_ != Status::kSuccess) {
    return failure_status_;
  }
  if (row_out == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null row_out in SpillableStore::NextRow");
  }
  while (active_read_row_ >= active_read_chunk_.Size()) {
    ASSIGN_OR_RETURN(bool, has_chunk, NextChunk(&active_read_chunk_));
    if (!has_chunk) {
      return false;
    }
    active_read_row_ = 0;
  }
  *row_out = active_read_chunk_.RowAt(active_read_row_);
  if (pos_out != nullptr) {
    *pos_out = active_read_chunk_.PositionAt(active_read_row_);
  }
  ++active_read_row_;
  return true;
}

void SpillableStore::Reset() noexcept {
  reader_.reset();
  // Ownership of the spill segment lives in exactly one place: the in-flight
  // writer before Seal(), or segment_id_ afterwards (Seal() transfers the id
  // and resets writer_). Capture and clear the member id first so the release
  // below cannot target the same segment twice even if a Finish()-ed writer
  // is still around -- Abort() is a no-op once finished_ is set.
  const SpillSegmentId owned = segment_id_;
  segment_id_ = kInvalidSpillSegmentId;
  if (writer_ != nullptr) {
    writer_->Abort();
    writer_.reset();
  }
  SpillStore::DropSegmentIfAlive(options_.spill_store, spill_store_alive_,
                                 owned);
  // clear() alone keeps the high-water capacity resident (and uncharged once
  // the grant is released below). Swap with an empty container so a later
  // reuse under a small budget starts from a genuinely small footprint.
  std::vector<DataChunk>().swap(mem_chunks_);
  staging_chunk_.Reset();
  active_read_chunk_.Reset();
  staging_bytes_ = 0;
  staging_reserved_bytes_ = 0;
  mem_bytes_ = 0;
  spilled_bytes_ = 0;
  total_rows_ = 0;
  mem_cursor_idx_ = 0;
  active_read_row_ = 0;
  spilled_ = false;
  sealed_ = false;
  failure_status_ = Status::kSuccess;
  // Same high-water argument as mem_chunks_: a single oversized frame must not
  // leave scratch_codec_'s capacity pinned across Reset()/reuse cycles.
  std::string().swap(scratch_codec_);
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
}

}  // namespace tinylamb
