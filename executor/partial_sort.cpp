/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/partial_sort.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/query_memory.hpp"
#include "executor/sort.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

int CompareRowKeys(const Row& lhs, const Row& rhs, const Schema& schema,
                   const std::vector<SortExecutor::Key>& keys) {
  for (const auto& key : keys) {
    Value lv = key.expression->Evaluate(lhs, schema);
    Value rv = key.expression->Evaluate(rhs, schema);
    if (lv.IsNull() && rv.IsNull()) continue;
    if (lv.IsNull()) {
      bool nulls_first = key.nulls_first.value_or(!key.ascending);
      return nulls_first ? -1 : 1;
    }
    if (rv.IsNull()) {
      bool nulls_first = key.nulls_first.value_or(!key.ascending);
      return nulls_first ? 1 : -1;
    }
    if (lv < rv) return key.ascending ? -1 : 1;
    if (rv < lv) return key.ascending ? 1 : -1;
  }
  return 0;
}

}  // namespace

PartialSortExecutor::PartialSortExecutor(
    Executor source, Schema schema, std::vector<SortExecutor::Key> keys,
    size_t top_k, size_t offset, size_t block_size)
    : source_(std::move(source)),
      schema_(std::move(schema)),
      keys_(std::move(keys)),
      top_k_(top_k),
      offset_(offset),
      block_size_(block_size) {}

void PartialSortExecutor::ExecutePartialSort() {
  output_.clear();
  output_offset_ = 0;

  auto comp = [&](const std::pair<Row, RowPosition>& a,
                  const std::pair<Row, RowPosition>& b) {
    return CompareRowKeys(a.first, b.first, schema_, keys_) < 0;
  };

  Row row;
  RowPosition rp;
  size_t total_bytes = 0;

  if (block_size_ > 0) {
    // Partitioned block mode: read in chunks of block_size_, sort each block,
    // and emit without sorting across block boundaries.
    std::vector<std::pair<Row, RowPosition>> current_block;
    current_block.reserve(block_size_);

    while (source_ && source_->Next(&row, &rp)) {
      total_bytes += EstimateRowBytes(row) + sizeof(RowPosition);
      current_block.emplace_back(std::move(row), rp);
      if (current_block.size() >= block_size_) {
        if (top_k_ > 0 && top_k_ < current_block.size()) {
          std::partial_sort(current_block.begin(),
                            current_block.begin() + top_k_,
                            current_block.end(), comp);
          current_block.resize(top_k_);
        } else {
          std::sort(current_block.begin(), current_block.end(), comp);
        }
        for (auto& item : current_block) {
          output_.push_back(std::move(item));
        }
        current_block.clear();
      }
    }
    if (!current_block.empty()) {
      if (top_k_ > 0 && top_k_ < current_block.size()) {
        std::partial_sort(current_block.begin(),
                          current_block.begin() + top_k_,
                          current_block.end(), comp);
        current_block.resize(top_k_);
      } else {
        std::sort(current_block.begin(), current_block.end(), comp);
      }
      for (auto& item : current_block) {
        output_.push_back(std::move(item));
      }
    }
  } else {
    // Top-K mode: sort only the top-K elements of the full input.
    std::vector<std::pair<Row, RowPosition>> input_rows;
    while (source_ && source_->Next(&row, &rp)) {
      total_bytes += EstimateRowBytes(row) + sizeof(RowPosition);
      input_rows.emplace_back(std::move(row), rp);
    }
    if (input_rows.empty()) {
      return;
    }
    const size_t limit_k = top_k_ > 0 ? (offset_ + top_k_) : input_rows.size();
    if (limit_k < input_rows.size()) {
      std::partial_sort(input_rows.begin(), input_rows.begin() + limit_k,
                        input_rows.end(), comp);
      const size_t start = std::min(offset_, input_rows.size());
      const size_t end = std::min(limit_k, input_rows.size());
      for (size_t i = start; i < end; ++i) {
        output_.push_back(std::move(input_rows[i]));
      }
    } else {
      std::sort(input_rows.begin(), input_rows.end(), comp);
      const size_t start = std::min(offset_, input_rows.size());
      for (size_t i = start; i < input_rows.size(); ++i) {
        output_.push_back(std::move(input_rows[i]));
      }
    }
  }

  charge_.Add(total_bytes);
}

void PartialSortExecutor::EnsureMaterialized() {
  if (materialized_) {
    return;
  }
  materialized_ = true;
  ExecutePartialSort();
}

void PartialSortExecutor::MaterializePipeline() {
  EnsureMaterialized();
}

bool PartialSortExecutor::Next(Row* dst, RowPosition* rp) {
  assert(dst != nullptr);
  EnsureMaterialized();
  if (output_offset_ >= output_.size()) {
    return false;
  }
  *dst = output_[output_offset_].first;
  if (rp != nullptr) {
    *rp = output_[output_offset_].second;
  }
  ++output_offset_;
  return true;
}

size_t PartialSortExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  EnsureMaterialized();
  if (output_offset_ >= output_.size()) {
    return 0;
  }
  const size_t count = std::min(max_rows, output_.size() - output_offset_);
  for (size_t i = 0; i < count; ++i) {
    destination->Append(output_[output_offset_ + i].first,
                        output_[output_offset_ + i].second);
  }
  output_offset_ += count;
  return count;
}

void PartialSortExecutor::Dump(std::ostream& o, int /*indent*/) const {
  o << "PartialSort(top_k=" << top_k_ << ", offset=" << offset_
    << ", block_size=" << block_size_ << ", keys=" << keys_.size() << ")";
}

void PartialSortExecutor::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
