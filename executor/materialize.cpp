/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/materialize.hpp"

#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/query_memory.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

MaterializeExecutor::MaterializeExecutor(Executor child, Schema schema)
    : child_(std::move(child)), schema_(std::move(schema)) {}

void MaterializeExecutor::MaterializePipeline() { EnsureMaterialized(); }

void MaterializeExecutor::EnsureMaterialized() {
  if (materialized_) {
    return;
  }
  materialized_ = true;
  rows_.clear();
  read_offset_ = 0;

  Row row;
  RowPosition rp;
  size_t est_bytes = 0;
  while (child_ && child_->Next(&row, &rp)) {
    est_bytes += EstimateRowBytes(row) + sizeof(RowPosition) + sizeof(void*);
    rows_.emplace_back(std::move(row), rp);
  }
  charge_.Add(est_bytes);
}

void MaterializeExecutor::Rewind() {
  EnsureMaterialized();
  read_offset_ = 0;
}

bool MaterializeExecutor::Next(Row* dst, RowPosition* rp) {
  assert(dst != nullptr);
  EnsureMaterialized();
  if (read_offset_ >= rows_.size()) {
    return false;
  }
  *dst = rows_[read_offset_].first;
  if (rp != nullptr) {
    *rp = rows_[read_offset_].second;
  }
  ++read_offset_;
  return true;
}

size_t MaterializeExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  EnsureMaterialized();
  if (read_offset_ >= rows_.size()) {
    return 0;
  }
  const size_t count = std::min(max_rows, rows_.size() - read_offset_);
  destination->Reset(schema_, count);
  for (size_t i = 0; i < count; ++i) {
    destination->Append(rows_[read_offset_ + i].first,
                        rows_[read_offset_ + i].second);
  }
  read_offset_ += count;
  return count;
}

void MaterializeExecutor::Dump(std::ostream& o, int indent) const {
  (void)indent;
  o << "MaterializeExecutor(rows=" << rows_.size()
    << ", materialized=" << (materialized_ ? "true" : "false") << ")";
}

void MaterializeExecutor::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
