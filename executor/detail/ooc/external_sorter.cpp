/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/ooc/external_sorter.hpp"

#include <algorithm>
#include <cmath>
#include <utility>

#include "common/memory_budget.hpp"
#include "type/collation.hpp"
#include "type/row_block_codec.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

constexpr size_t kWriteBatchRows = 256;
constexpr size_t kWriteBatchMaxBytes = 16ULL
                                       << 10;  // 16 KiB bounded decode unit

}  // namespace

ExternalSorter::ExternalSorter(Options options)
    : options_(std::move(options)),
      spill_store_alive_(options_.spill_store != nullptr
                             ? options_.spill_store->LifetimeToken()
                             : std::weak_ptr<const bool>{}) {
  key_orders_.reserve(options_.keys.size());
  for (const OocSortKey& k : options_.keys) {
    key_orders_.push_back(MakeSortKeyOrder(k));
  }
  if (options_.grant != nullptr) {
    const size_t refill =
        options_.memory_budget_bytes > 0
            ? std::min<size_t>(
                  QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
                  std::max<size_t>(65536, options_.memory_budget_bytes / 4))
            : QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes;
    grant_cache_.emplace(options_.grant, refill);
  }
}

ExternalSorter::~ExternalSorter() { Reset(); }

size_t ExternalSorter::EffectiveBudgetBytes() const noexcept {
  size_t b = options_.memory_budget_bytes;
  if (options_.grant != nullptr) {
    const size_t grant_op = options_.grant->OperatorBudgetBytes();
    if (b == 0 || grant_op < b) {
      b = grant_op;
    }
  }
  return b;
}

int ExternalSorter::CompareRows(const Row& lhs, const Row& rhs) const noexcept {
  for (size_t i = 0; i < key_orders_.size(); ++i) {
    const size_t col_idx = options_.keys[i].column_index;
    const bool lhs_ok = col_idx < lhs.Count();
    const bool rhs_ok = col_idx < rhs.Count();
    if (!lhs_ok || !rhs_ok) {
      if (lhs_ok != rhs_ok) {
        return lhs_ok ? 1 : -1;
      }
      continue;
    }
    const int cmp = key_orders_[i].Compare(lhs[col_idx], rhs[col_idx]);
    if (cmp != 0) {
      return cmp;
    }
  }
  return 0;
}

bool ExternalSorter::ShouldSkipTopKChunkRow(
    const DataChunk& chunk, size_t row_idx,
    size_t sql_key_count) const noexcept {
  if (!in_limit_heap_ || spilled_ || run_rows_.empty() ||
      options_.force_spill ||
      ProcessMemoryBudget::Global().AlwaysSpillEnabled() ||
      sql_key_count == 0 || sql_key_count >= key_orders_.size() ||
      sql_key_count >= options_.keys.size() || row_idx >= chunk.Size()) {
    return false;
  }
  if (!key_orders_[sql_key_count].Ascending()) {
    return false;
  }
  const Row& heap_root = run_rows_.front();
  for (size_t i = 0; i < sql_key_count; ++i) {
    const size_t col_idx = options_.keys[i].column_index;
    const bool lhs_ok = col_idx < chunk.ColumnCount();
    const bool rhs_ok = col_idx < heap_root.Count();
    if (!lhs_ok || !rhs_ok) {
      if (lhs_ok != rhs_ok) {
        return lhs_ok;
      }
      continue;
    }
    const ColumnVector& col = chunk.ColumnAt(col_idx);
    const Value& rhs = heap_root[col_idx];
    const SortKeyOrder& order = key_orders_[i];
    const bool lhs_null = col.IsNull(row_idx);
    const bool rhs_null = rhs.IsNull();
    if (lhs_null || rhs_null) {
      if (lhs_null && rhs_null) {
        continue;
      }
      const int null_side = lhs_null ? -1 : 1;
      const int cmp = order.NullsFirst() ? null_side : -null_side;
      return cmp > 0;
    }
    int cmp = 0;
    if (col.IsHeterogeneous() || col.Type() != rhs.type) {
      cmp = order.Compare(col.ValueAt(row_idx), rhs);
    } else {
      switch (col.Type()) {
        case ValueType::kInt64: {
          if (col.IsUnsigned() || rhs.IsUnsigned()) {
            const bool mixed = col.IsUnsigned() != rhs.IsUnsigned();
            const int64_t lv = col.IntegerData()[row_idx];
            const int64_t rv = rhs.value.int_value;
            const auto ulv = static_cast<uint64_t>(lv);
            const auto urv = static_cast<uint64_t>(rv);
            int c = 0;
            if (mixed && !col.IsUnsigned() && lv < 0) {
              c = -1;
            } else if (mixed && !rhs.IsUnsigned() && rv < 0) {
              c = 1;
            } else {
              c = ulv < urv ? -1 : (urv < ulv ? 1 : 0);
            }
            cmp = order.Ascending() ? c : -c;
          } else {
            const int64_t lv = col.IntegerData()[row_idx];
            const int64_t rv = rhs.value.int_value;
            const int c = lv < rv ? -1 : (rv < lv ? 1 : 0);
            cmp = order.Ascending() ? c : -c;
          }
          break;
        }
        case ValueType::kDate: {
          const int64_t lv = col.IntegerData()[row_idx];
          const int64_t rv = rhs.value.int_value;
          const int c = lv < rv ? -1 : (rv < lv ? 1 : 0);
          cmp = order.Ascending() ? c : -c;
          break;
        }
        case ValueType::kDouble: {
          const double lv = col.DoubleData()[row_idx];
          const double rv = rhs.value.double_value;
          const bool nl = std::isnan(lv);
          const bool nr = std::isnan(rv);
          int c = 0;
          if (nl || nr) {
            c = (nl && nr) ? 0 : (nl ? -1 : 1);
          } else {
            c = lv < rv ? -1 : (rv < lv ? 1 : 0);
          }
          cmp = order.Ascending() ? c : -c;
          break;
        }
        case ValueType::kVarChar: {
          if (col.HasMixedCollation() || rhs.IsInterval()) {
            cmp = order.Compare(col.ValueAt(row_idx), rhs);
          } else {
            const Collation resolved = ResolveCollation(
                order.KeyCollation(),
                ResolveCollation(col.ColumnCollation(), CollationOf(rhs)));
            const std::string_view lv = col.StringData()[row_idx];
            const std::string_view rv = rhs.value.varchar_value;
            const int c = CompareVarCharForOrderBy(
                lv, rv, resolved == Collation::kCaseInsensitive);
            cmp = order.Ascending() ? c : -c;
          }
          break;
        }
        default:
          cmp = order.Compare(col.ValueAt(row_idx), rhs);
          break;
      }
    }
    if (cmp != 0) {
      return cmp > 0;
    }
  }
  return true;
}

void ExternalSorter::ObservePeakMemory(size_t bytes) noexcept {
  if (bytes > peak_mem_bytes_) {
    peak_mem_bytes_ = bytes;
    ProcessMemoryBudget::Global().RecordOperatorPeakBytes(bytes);
  }
}

void ExternalSorter::DropRunSegments() noexcept {
  for (const SpillSegmentId id : run_segments_) {
    SpillStore::DropSegmentIfAlive(options_.spill_store, spill_store_alive_,
                                   id);
  }
  run_segments_.clear();
}

void ExternalSorter::Reset() noexcept {
  final_heap_.clear();
  final_streams_.clear();
  DropRunSegments();
  run_rows_.clear();
  run_rows_.shrink_to_fit();
  run_bytes_ = 0;
  in_limit_heap_ = false;
  in_mem_cursor_ = 0;
  total_rows_ = 0;
  emitted_rows_ = 0;
  spilled_bytes_ = 0;
  run_count_ = 0;
  merge_passes_ = 0;
  spilled_ = false;
  finished_ = false;
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
}

Status ExternalSorter::WriteSortedRun(std::vector<Row>* rows,
                                      SpillSegmentId* seg_out) {
  if (options_.spill_store == nullptr) {
    return StatusError(
        StatusCode::kMemoryLimitExceeded,
        "ExternalSorter exceeded memory budget with no SpillStore");
  }
  std::stable_sort(
      rows->begin(), rows->end(),
      [this](const Row& a, const Row& b) { return RowPrecedes(a, b); });
  if (options_.limit.has_value() && rows->size() > *options_.limit) {
    rows->resize(*options_.limit);
  }

  const size_t budget = EffectiveBudgetBytes();
  const size_t writer_buf =
      budget > 0 ? std::min<size_t>(SpillStore::kBlockSize,
                                    std::max<size_t>(4096, budget / 8))
                 : SpillStore::kBlockSize;
  SpillSegmentWriter writer(options_.spill_store, writer_buf);
  RETURN_IF_FAIL(writer.EnsureOpen());

  std::string scratch;
  size_t idx = 0;
  while (idx < rows->size()) {
    size_t batch_end = idx;
    size_t batch_bytes = 0;
    while (batch_end < rows->size() && (batch_end - idx) < kWriteBatchRows &&
           batch_bytes < kWriteBatchMaxBytes) {
      batch_bytes += EstimateRowMemoryBytes((*rows)[batch_end]);
      ++batch_end;
    }
    RETURN_IF_FAIL(WriteRowBlock(
        &writer, std::span<const Row>(rows->data() + idx, batch_end - idx),
        &scratch));
    idx = batch_end;
  }
  ASSIGN_OR_RETURN(SpillSegmentId, id, writer.Finish(true));
  spilled_bytes_ += writer.BytesWritten();
  *seg_out = id;
  return Status::kSuccess;
}

Status ExternalSorter::FlushCurrentRun() {
  if (run_rows_.empty()) {
    return Status::kSuccess;
  }
  if (in_limit_heap_) {
    in_limit_heap_ = false;
  }
  SpillSegmentId seg = kInvalidSpillSegmentId;
  RETURN_IF_FAIL(WriteSortedRun(&run_rows_, &seg));
  run_segments_.push_back(seg);
  ++run_count_;
  run_rows_.clear();
  run_bytes_ = 0;
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
  spilled_ = true;
  return Status::kSuccess;
}

Status ExternalSorter::AddRow(const Row& row) {
  Row copy = row;
  return AddRow(std::move(copy));
}

Status ExternalSorter::AddRow(Row&& row) {
  if (finished_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "cannot AddRow to finished ExternalSorter");
  }
  if (options_.limit.has_value() && *options_.limit == 0) {
    return Status::kSuccess;
  }
  ++total_rows_;

  const size_t row_bytes = EstimateRowMemoryBytes(row);
  const size_t budget = EffectiveBudgetBytes();
  // ADR-0004 rule 9: a single row may never exceed 1/4 of the operator
  // budget. Check before any retention (including the first row, which the
  // empty-run guards below would otherwise skip).
  if (budget > 0 && row_bytes > budget / 4) {
    return StatusError(StatusCode::kTooBigData,
                       "row exceeds operator budget quarter");
  }
  const bool always_spill = options_.force_spill ||
                            ProcessMemoryBudget::Global().AlwaysSpillEnabled();

  auto less_fn = [this](const Row& a, const Row& b) {
    return RowPrecedes(a, b);
  };

  // Bounded Top-N fast path: keep at most `*limit` rows in a max-heap when they
  // fit in budget and forced-spill is disabled.
  if (!spilled_ && !always_spill && options_.limit.has_value() &&
      *options_.limit > 0) {
    const size_t k = *options_.limit;
    if (!in_limit_heap_ && run_rows_.size() == k &&
        (budget == 0 || run_bytes_ <= budget)) {
      std::make_heap(run_rows_.begin(), run_rows_.end(), less_fn);
      in_limit_heap_ = true;
    }
    if (in_limit_heap_) {
      if (!RowPrecedes(row, run_rows_.front())) {
        return Status::kSuccess;  // Larger than current K-th smallest.
      }
      const size_t evicted_bytes = EstimateRowMemoryBytes(run_rows_.front());
      const size_t next_bytes = run_bytes_ > evicted_bytes
                                    ? run_bytes_ - evicted_bytes + row_bytes
                                    : row_bytes;
      bool can_replace_in_heap = (budget == 0 || next_bytes <= budget);
      if (can_replace_in_heap && grant_cache_.has_value() &&
          row_bytes > evicted_bytes) {
        if (!grant_cache_->Reserve(row_bytes - evicted_bytes)) {
          can_replace_in_heap = false;
        }
      } else if (can_replace_in_heap && grant_cache_.has_value() &&
                 evicted_bytes > row_bytes) {
        grant_cache_->Release(evicted_bytes - row_bytes);
      }
      if (can_replace_in_heap) {
        std::pop_heap(run_rows_.begin(), run_rows_.end(), less_fn);
        run_rows_.back() = std::move(row);
        std::push_heap(run_rows_.begin(), run_rows_.end(), less_fn);
        run_bytes_ = next_bytes;
        ObservePeakMemory(run_bytes_);
        return Status::kSuccess;
      }
      // Top-N payload outgrew memory budget; spill heap as run and fall
      // through.
      RETURN_IF_FAIL(FlushCurrentRun());
    }
  }

  const bool need_flush =
      (always_spill && !run_rows_.empty() && run_bytes_ >= 16384) ||
      (!run_rows_.empty() && budget > 0 && run_bytes_ + row_bytes > budget) ||
      (grant_cache_.has_value() && !grant_cache_->Reserve(row_bytes));

  if (need_flush) {
    RETURN_IF_FAIL(FlushCurrentRun());
    if (grant_cache_.has_value() && !grant_cache_->Reserve(row_bytes)) {
      if (options_.spill_store == nullptr) {
        return StatusError(
            StatusCode::kMemoryLimitExceeded,
            "ExternalSorter exceeded grant budget with no SpillStore "
            "configured");
      }
      ObservePeakMemory(row_bytes);
      std::vector<Row> single;
      single.push_back(std::move(row));
      SpillSegmentId seg = kInvalidSpillSegmentId;
      RETURN_IF_FAIL(WriteSortedRun(&single, &seg));
      run_segments_.push_back(seg);
      ++run_count_;
      spilled_ = true;
      return Status::kSuccess;
    }
  }

  run_rows_.push_back(std::move(row));
  run_bytes_ += row_bytes;
  ObservePeakMemory(run_bytes_);

  if (always_spill && run_bytes_ >= 16384) {
    RETURN_IF_FAIL(FlushCurrentRun());
  }
  return Status::kSuccess;
}

Status ExternalSorter::AddChunk(const DataChunk& chunk) {
  for (size_t r = 0; r < chunk.Size(); ++r) {
    RETURN_IF_FAIL(AddRow(chunk.RowAt(r)));
  }
  return Status::kSuccess;
}

Status ExternalSorter::AdvanceMergeStream(MergeStream* stream) {
  if (stream->eof) {
    return Status::kSuccess;
  }
  while (stream->row_idx >= stream->block_rows.size()) {
    stream->block_rows.clear();
    stream->row_idx = 0;
    ASSIGN_OR_RETURN(bool, has_more,
                     ReadRowBlock(stream->reader.get(), &stream->block_rows,
                                  &stream->scratch));
    if (!has_more) {
      stream->eof = true;
      stream->scratch.clear();
      stream->scratch.shrink_to_fit();
      return Status::kSuccess;
    }
  }
  return Status::kSuccess;
}

void ExternalSorter::ComputeMergeGeometry(
    size_t* merge_fan_in_out, size_t* reader_buf_out,
    size_t* writer_buf_out) const noexcept {
  const size_t budget = EffectiveBudgetBytes();
  if (budget == 0) {
    *reader_buf_out = SpillStore::kBlockSize;
    *writer_buf_out = SpillStore::kBlockSize;
    *merge_fan_in_out = 16;
    return;
  }
  const size_t rbuf =
      std::min<size_t>(16384, std::max<size_t>(4096, budget / 32));
  const size_t wbuf =
      std::min<size_t>(32768, std::max<size_t>(4096, budget / 16));
  const size_t usable_for_readers =
      budget > wbuf ? (budget - wbuf) : (budget / 2);
  // Each merge stream holds `rbuf` plus a decoded row block: a spill block is
  // up to SpillStore::kBlockSize (64 KiB), not the 16 KiB write-batch size.
  // Under-estimating this over-provisions fan-in and lets the merge exceed the
  // operator budget.
  const size_t per_stream = rbuf + SpillStore::kBlockSize;
  const size_t fan_in = std::clamp<size_t>(
      usable_for_readers / std::max<size_t>(per_stream, 8192), 2, 32);
  *reader_buf_out = rbuf;
  *writer_buf_out = wbuf;
  *merge_fan_in_out = fan_in;
}

Status ExternalSorter::MergeRunGroup(const std::vector<SpillSegmentId>& inputs,
                                     size_t reader_buf_bytes,
                                     size_t writer_buf_bytes,
                                     SpillSegmentId* merged_out) {
  std::vector<MergeStream> streams(inputs.size());
  std::vector<size_t> heap;
  heap.reserve(inputs.size());

  auto heap_cmp = [this, &streams](size_t a, size_t b) {
    // std::priority_queue / heap max-element -> return true when `a` comes
    // *after* `b` so smallest row sits at `heap.front()`.
    return CompareRows(streams[a].block_rows[streams[a].row_idx],
                       streams[b].block_rows[streams[b].row_idx]) > 0;
  };

  for (size_t i = 0; i < inputs.size(); ++i) {
    streams[i].reader = std::make_unique<SpillSegmentReader>(
        options_.spill_store, inputs[i], reader_buf_bytes);
    RETURN_IF_FAIL(AdvanceMergeStream(&streams[i]));
    if (!streams[i].eof) {
      heap.push_back(i);
    }
  }
  std::make_heap(heap.begin(), heap.end(), heap_cmp);

  SpillSegmentWriter writer(options_.spill_store, writer_buf_bytes);
  RETURN_IF_FAIL(writer.EnsureOpen());

  std::vector<Row> batch;
  batch.reserve(kWriteBatchRows);
  size_t batch_bytes = 0;
  std::string write_scratch;
  size_t written = 0;

  while (!heap.empty()) {
    if (options_.limit.has_value() && written >= *options_.limit) {
      break;
    }
    std::pop_heap(heap.begin(), heap.end(), heap_cmp);
    const size_t idx = heap.back();
    heap.pop_back();

    Row row = std::move(streams[idx].block_rows[streams[idx].row_idx]);
    ++streams[idx].row_idx;
    batch_bytes += EstimateRowMemoryBytes(row);
    batch.push_back(std::move(row));
    ++written;

    if (batch.size() >= kWriteBatchRows || batch_bytes >= kWriteBatchMaxBytes) {
      RETURN_IF_FAIL(WriteRowBlock(&writer, batch, &write_scratch));
      batch.clear();
      batch_bytes = 0;
    }

    RETURN_IF_FAIL(AdvanceMergeStream(&streams[idx]));
    if (!streams[idx].eof) {
      heap.push_back(idx);
      std::push_heap(heap.begin(), heap.end(), heap_cmp);
    }
  }

  if (!batch.empty()) {
    RETURN_IF_FAIL(WriteRowBlock(&writer, batch, &write_scratch));
    batch.clear();
  }
  ASSIGN_OR_RETURN(SpillSegmentId, out_id, writer.Finish(true));
  spilled_bytes_ += writer.BytesWritten();
  *merged_out = out_id;
  return Status::kSuccess;
}

Status ExternalSorter::OpenFinalMergeStreams(size_t reader_buf_bytes) {
  final_streams_.clear();
  final_heap_.clear();
  final_streams_.resize(run_segments_.size());

  for (size_t i = 0; i < run_segments_.size(); ++i) {
    final_streams_[i].reader = std::make_unique<SpillSegmentReader>(
        options_.spill_store, run_segments_[i], reader_buf_bytes);
    RETURN_IF_FAIL(AdvanceMergeStream(&final_streams_[i]));
    if (!final_streams_[i].eof) {
      final_heap_.push_back(i);
    }
  }
  auto heap_cmp = [this](size_t a, size_t b) {
    return CompareRows(
               final_streams_[a].block_rows[final_streams_[a].row_idx],
               final_streams_[b].block_rows[final_streams_[b].row_idx]) > 0;
  };
  std::make_heap(final_heap_.begin(), final_heap_.end(), heap_cmp);
  return Status::kSuccess;
}

Status ExternalSorter::Finish() {
  if (finished_) {
    return Status::kSuccess;
  }
  if (!spilled_) {
    if (in_limit_heap_) {
      auto less_fn = [this](const Row& a, const Row& b) {
        return RowPrecedes(a, b);
      };
      std::sort_heap(run_rows_.begin(), run_rows_.end(), less_fn);
      in_limit_heap_ = false;
    } else {
      std::stable_sort(
          run_rows_.begin(), run_rows_.end(),
          [this](const Row& a, const Row& b) { return RowPrecedes(a, b); });
    }
    if (options_.limit.has_value() && run_rows_.size() > *options_.limit) {
      run_rows_.resize(*options_.limit);
    }
    in_mem_cursor_ = 0;
    emitted_rows_ = 0;
    finished_ = true;
    return Status::kSuccess;
  }

  if (!run_rows_.empty()) {
    RETURN_IF_FAIL(FlushCurrentRun());
  }

  size_t merge_fan_in = 16;
  size_t reader_buf = 16384;
  size_t writer_buf = 16384;
  ComputeMergeGeometry(&merge_fan_in, &reader_buf, &writer_buf);

  while (run_segments_.size() > merge_fan_in) {
    std::vector<SpillSegmentId> next_round;
    next_round.reserve((run_segments_.size() + merge_fan_in - 1) /
                       merge_fan_in);
    for (size_t i = 0; i < run_segments_.size(); i += merge_fan_in) {
      const size_t end = std::min(i + merge_fan_in, run_segments_.size());
      std::vector<SpillSegmentId> group(
          run_segments_.begin() + static_cast<ptrdiff_t>(i),
          run_segments_.begin() + static_cast<ptrdiff_t>(end));
      if (group.size() == 1) {
        next_round.push_back(group.front());
        continue;
      }
      SpillSegmentId merged = kInvalidSpillSegmentId;
      RETURN_IF_FAIL(MergeRunGroup(group, reader_buf, writer_buf, &merged));
      // Immediately release merged input runs so SpillStore recycles blocks.
      // Use the shared liveness token: the store may already have been
      // dropped by a sibling consumer, and a raw DropSegment would reuse a
      // recycled id.
      for (const SpillSegmentId consumed : group) {
        SpillStore::DropSegmentIfAlive(options_.spill_store,
                                       spill_store_alive_, consumed);
      }
      next_round.push_back(merged);
    }
    run_segments_ = std::move(next_round);
    ++merge_passes_;
  }

  if (!run_segments_.empty()) {
    ++merge_passes_;
    RETURN_IF_FAIL(OpenFinalMergeStreams(reader_buf));
  }
  emitted_rows_ = 0;
  finished_ = true;
  return Status::kSuccess;
}

StatusOr<bool> ExternalSorter::NextRow(Row* row_out) {
  if (row_out == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null row_out in ExternalSorter::NextRow");
  }
  if (!finished_) {
    RETURN_IF_FAIL(Finish());
  }
  if (options_.limit.has_value() && emitted_rows_ >= *options_.limit) {
    return false;
  }

  if (!spilled_) {
    if (in_mem_cursor_ >= run_rows_.size()) {
      return false;
    }
    *row_out = std::move(run_rows_[in_mem_cursor_++]);
    ++emitted_rows_;
    if (in_mem_cursor_ == run_rows_.size()) {
      run_rows_.clear();
      run_rows_.shrink_to_fit();
      run_bytes_ = 0;
      if (grant_cache_.has_value()) {
        grant_cache_->ReleaseAll();
      }
    }
    return true;
  }

  if (final_heap_.empty()) {
    final_streams_.clear();
    DropRunSegments();
    return false;
  }

  auto heap_cmp = [this](size_t a, size_t b) {
    return CompareRows(
               final_streams_[a].block_rows[final_streams_[a].row_idx],
               final_streams_[b].block_rows[final_streams_[b].row_idx]) > 0;
  };
  std::pop_heap(final_heap_.begin(), final_heap_.end(), heap_cmp);
  const size_t idx = final_heap_.back();
  final_heap_.pop_back();

  *row_out =
      std::move(final_streams_[idx].block_rows[final_streams_[idx].row_idx]);
  ++final_streams_[idx].row_idx;
  ++emitted_rows_;

  RETURN_IF_FAIL(AdvanceMergeStream(&final_streams_[idx]));
  if (!final_streams_[idx].eof) {
    final_heap_.push_back(idx);
    std::push_heap(final_heap_.begin(), final_heap_.end(), heap_cmp);
  }
  if (final_heap_.empty()) {
    final_streams_.clear();
    DropRunSegments();
  }
  return true;
}

StatusOr<bool> ExternalSorter::NextChunk(DataChunk* chunk_out,
                                         size_t max_rows) {
  if (chunk_out == nullptr || max_rows == 0) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid arguments in ExternalSorter::NextChunk");
  }
  chunk_out->Reset();
  Row row;
  while (chunk_out->Size() < max_rows) {
    ASSIGN_OR_RETURN(bool, has_row, NextRow(&row));
    if (!has_row) {
      break;
    }
    chunk_out->Append(std::move(row));
  }
  return !chunk_out->Empty();
}

}  // namespace tinylamb
