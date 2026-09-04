/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/chunked_scan.hpp"

#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/selection_vector.hpp"
#include "executor/vectorized_expression.hpp"
#include "index/index.hpp"
#include "page/row_position.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

ChunkedScan::ChunkedScan(Transaction& txn, Table& table, Schema schema,
                         std::vector<slot_t> projection,
                         std::optional<Expression> filter,
                         size_t pages_per_morsel)
    : txn_(txn),
      table_(table),
      schema_(std::move(schema)),
      projection_(std::move(projection)),
      filter_(std::move(filter)),
      pages_per_morsel_(pages_per_morsel),
      is_index_scan_(false) {
  InitializeTableMorsels();
}

ChunkedScan::ChunkedScan(Transaction& txn, Table& table, const Index& index,
                         Schema schema, const Value& begin, const Value& end,
                         bool ascending, std::vector<slot_t> projection,
                         std::optional<Expression> filter)
    : txn_(txn),
      table_(table),
      schema_(std::move(schema)),
      projection_(std::move(projection)),
      filter_(std::move(filter)),
      index_(&index),
      index_begin_(begin),
      index_end_(end),
      ascending_(ascending),
      is_index_scan_(true) {
  index_iter_ = table_.BeginIndexScan(txn_, *index_, index_begin_, index_end_,
                                      ascending_);
}

void ChunkedScan::InitializeTableMorsels() {
  morsels_ = table_.BuildScanMorsels(txn_, pages_per_morsel_);
  current_morsel_idx_ = 0;
  table_scan_exhausted_ = morsels_.empty();
}

size_t ChunkedScan::FillFromIndex(DataChunk* destination, size_t max_rows) {
  assert(destination != nullptr);
  if (!index_iter_ || !index_iter_->IsValid()) {
    return 0;
  }
  DataChunk raw_batch(schema_, max_rows);
  while (raw_batch.Size() < max_rows && index_iter_->IsValid()) {
    raw_batch.Append(**index_iter_, index_iter_->Position());
    ++(*index_iter_);
  }
  if (raw_batch.Empty()) {
    return 0;
  }

  if (filter_) {
    SelectionVector sel;
    VectorizedExpression::FilterDataChunk(*filter_, schema_, raw_batch, &sel);
    destination->AppendGather(raw_batch, sel.Data(), sel.Size());
    return sel.Size();
  }

  for (size_t i = 0; i < raw_batch.Size(); ++i) {
    destination->Append(raw_batch, i);
  }
  return raw_batch.Size();
}

size_t ChunkedScan::FillFromTableMorsels(DataChunk* destination,
                                         size_t max_rows) {
  assert(destination != nullptr);
  if (table_scan_exhausted_) {
    return 0;
  }

  const size_t initial_dest_size = destination->Size();
  while (destination->Size() - initial_dest_size < max_rows &&
         current_morsel_idx_ < morsels_.size()) {
    if (!current_iter_ || !current_iter_->IsValid()) {
      std::optional<std::vector<slot_t>> opt_proj =
          projection_.empty() ? std::nullopt : std::make_optional(projection_);
      current_iter_ = table_.BeginMorselScan(
          txn_, morsels_[current_morsel_idx_], std::move(opt_proj));
    }

    const size_t remaining_needed =
        max_rows - (destination->Size() - initial_dest_size);
    const size_t morsel_batch_capacity =
        std::min(remaining_needed, kDefaultVectorSize);
    DataChunk raw_batch(schema_, morsel_batch_capacity);

    while (raw_batch.Size() < morsel_batch_capacity &&
           current_iter_->IsValid()) {
      raw_batch.Append(**current_iter_, current_iter_->Position());
      ++(*current_iter_);
    }

    if (!current_iter_->IsValid()) {
      ++current_morsel_idx_;
      current_iter_.reset();
    }

    if (!raw_batch.Empty()) {
      if (filter_) {
        SelectionVector sel;
        VectorizedExpression::FilterDataChunk(*filter_, schema_, raw_batch,
                                              &sel);
        destination->AppendGather(raw_batch, sel.Data(), sel.Size());
      } else {
        for (size_t i = 0; i < raw_batch.Size(); ++i) {
          destination->Append(raw_batch, i);
        }
      }
    }
  }

  if (current_morsel_idx_ >= morsels_.size()) {
    table_scan_exhausted_ = true;
  }
  return destination->Size() - initial_dest_size;
}

size_t ChunkedScan::NextBatch(DataChunk* destination, size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  // The batch contract is "append the next batch to the given chunk";
  // callers that reuse a destination would otherwise mix rows from two
  // batches, so reset before filling.
  destination->Reset(schema_, max_rows);
  if (is_index_scan_) {
    return FillFromIndex(destination, max_rows);
  }
  return FillFromTableMorsels(destination, max_rows);
}

bool ChunkedScan::Next(Row* dst, RowPosition* rp) {
  assert(dst != nullptr);
  if (buffer_offset_ >= buffer_.Size()) {
    buffer_.Reset(schema_, kDefaultVectorSize);
    buffer_offset_ = 0;
    const size_t fetched = NextBatch(&buffer_, kDefaultVectorSize);
    if (fetched == 0) {
      return false;
    }
  }
  *dst = buffer_.RowAt(buffer_offset_);
  if (rp != nullptr) {
    *rp = buffer_.PositionAt(buffer_offset_);
  }
  ++buffer_offset_;
  return true;
}

void ChunkedScan::Dump(std::ostream& o, int /*indent*/) const {
  o << "ChunkedScan(";
  if (is_index_scan_) {
    o << "index=" << (index_ ? index_->sc_.name_ : "")
      << ", begin=" << index_begin_ << ", end=" << index_end_;
  } else {
    o << "table=" << schema_.Name() << ", morsels=" << morsels_.size();
  }
  if (filter_) {
    o << ", filter=" << *filter_;
  }
  o << ")";
}

void ChunkedScan::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
