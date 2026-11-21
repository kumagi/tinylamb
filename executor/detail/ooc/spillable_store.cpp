/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/ooc/spillable_store.hpp"

#include <algorithm>
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
  size_t cap = options_.io_buffer_bytes;
  if (budget > 0) {
    cap = std::min(cap, std::max<size_t>(4096, budget / 4));
  }
  return cap;
}

size_t SpillableStore::StagingFlushBytes() const noexcept {
  const size_t budget = EffectiveBudgetBytes();
  if (budget == 0) {
    return 0;
  }
  return std::max<size_t>(4096, std::min<size_t>(budget / 4, 256ULL << 10));
}

Status SpillableStore::FlushStagingChunk() {
  if (staging_reserved_bytes_ > 0 && grant_cache_.has_value()) {
    grant_cache_->Release(staging_reserved_bytes_);
    staging_reserved_bytes_ = 0;
  }
  if (staging_chunk_.Empty()) {
    return Status::kSuccess;
  }
  DataChunk to_flush = std::move(staging_chunk_);
  staging_chunk_.Reset();
  staging_bytes_ = 0;
  return AppendChunk(std::move(to_flush));
}

Status SpillableStore::SpillChunk(const DataChunk& chunk) {
  if (writer_ == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "SpillableStore missing SpillSegmentWriter");
  }
  const uint64_t before = writer_->BytesWritten();
  RETURN_IF_FAIL(WriteDataChunk(writer_.get(), chunk, &scratch_codec_,
                                StagingFlushBytes()));
  spilled_bytes_ += writer_->BytesWritten() - before;
  TrimCodecScratch();
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
  auto writer = std::make_unique<SpillSegmentWriter>(
      options_.spill_store, EffectiveIoBufferBytes());
  RETURN_IF_FAIL(writer->EnsureOpen());
  for (const DataChunk& chunk : mem_chunks_) {
    RETURN_IF_FAIL(WriteDataChunk(writer.get(), chunk, &scratch_codec_,
                                  StagingFlushBytes()));
    TrimCodecScratch();
  }
  RETURN_IF_FAIL(writer->Flush());
  spilled_bytes_ += writer->BytesWritten();
  writer_ = std::move(writer);
  // T3462: shrink_to_fit may throw bad_alloc out of this Status path.
  std::vector<DataChunk>().swap(mem_chunks_);
  mem_bytes_ = 0;
  staging_reserved_bytes_ = 0;
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
  spilled_ = true;
  return Status::kSuccess;
}

template <typename ChunkRef>
Status SpillableStore::AppendChunkImpl(ChunkRef&& chunk) {
  if (sealed_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "cannot append to sealed SpillableStore");
  }
  RETURN_IF_FAIL(FlushStagingChunk());
  if (chunk.Empty()) {
    return Status::kSuccess;
  }

  const size_t chunk_rows = chunk.Size();
  if (spilled_) {
    RETURN_IF_FAIL(SpillChunk(chunk));
    total_rows_ += chunk_rows;
    return Status::kSuccess;
  }

  const size_t chunk_bytes = EstimateChunkMemoryBytes(chunk);
  const size_t budget = EffectiveBudgetBytes();
  bool need_spill = options_.force_spill ||
                    ProcessMemoryBudget::Global().AlwaysSpillEnabled();
  if (!need_spill && budget > 0) {
    if (chunk_bytes > budget || mem_bytes_ + chunk_bytes > budget) {
      need_spill = true;
    }
  }
  if (!need_spill && grant_cache_.has_value()) {
    if (!grant_cache_->Reserve(chunk_bytes)) {
      need_spill = true;
    }
  }

  if (need_spill) {
    RETURN_IF_FAIL(TransitionToSpill());
    RETURN_IF_FAIL(SpillChunk(chunk));
    total_rows_ += chunk_rows;
    return Status::kSuccess;
  }

  mem_chunks_.push_back(std::forward<ChunkRef>(chunk));
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
  if (sealed_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "cannot append to sealed SpillableStore");
  }
  const size_t row_bytes = EstimateRowMemoryBytes(row) + sizeof(RowPosition);
  if (!spilled_ && grant_cache_.has_value()) {
    if (!grant_cache_->Reserve(row_bytes)) {
      if (!staging_chunk_.Empty()) {
        RETURN_IF_FAIL(FlushStagingChunk());
      }
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes
      // success
      if (!spilled_ && !grant_cache_->Reserve(row_bytes)) {  // NOLINT(bugprone-unchecked-optional-access)
        RETURN_IF_FAIL(TransitionToSpill());
      }
    }
    if (!spilled_) {
      staging_reserved_bytes_ += row_bytes;
    }
  }
  staging_chunk_.Append(row, pos);
  staging_bytes_ += row_bytes;
  const size_t budget = EffectiveBudgetBytes();
  const size_t staging_cap =
      budget > 0
          ? std::max<size_t>(4096, std::min<size_t>(budget / 4, 256ULL << 10))
          : (256ULL << 10);
  if (staging_chunk_.Size() >= kDefaultVectorSize ||
      staging_bytes_ >= staging_cap ||
      (!spilled_ && budget > 0 && mem_bytes_ + staging_bytes_ >= budget)) {
    RETURN_IF_FAIL(FlushStagingChunk());
  }
  return Status::kSuccess;
}

Status SpillableStore::Seal() {
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
  if (!sealed_) {
    RETURN_IF_FAIL(Seal());
  }
  mem_cursor_idx_ = 0;
  active_read_chunk_.Reset();
  active_read_row_ = 0;
  reader_.reset();
  if (spilled_ && segment_id_ != kInvalidSpillSegmentId) {
    reader_ = std::make_unique<SpillSegmentReader>(
        options_.spill_store, segment_id_, EffectiveIoBufferBytes());
    RETURN_IF_FAIL(reader_->Open());
  }
  return Status::kSuccess;
}

StatusOr<bool> SpillableStore::NextChunk(DataChunk* chunk_out) {
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
  if (writer_ != nullptr) {
    writer_->Abort();
    writer_.reset();
  }
  SpillStore::DropSegmentIfAlive(options_.spill_store, spill_store_alive_,
                                 segment_id_);
  segment_id_ = kInvalidSpillSegmentId;
  mem_chunks_.clear();
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
  scratch_codec_.clear();
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
}

}  // namespace tinylamb
