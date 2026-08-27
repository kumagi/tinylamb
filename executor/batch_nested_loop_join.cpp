/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/batch_nested_loop_join.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/join_kind.hpp"
#include "executor/query_memory.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

BatchNestedLoopJoin::BatchNestedLoopJoin(
    Executor left, Schema left_schema, Executor right, Schema right_schema,
    Expression predicate, JoinKind kind, size_t block_size)
    : left_(std::move(left)),
      left_schema_(std::move(left_schema)),
      right_(std::move(right)),
      right_schema_(std::move(right_schema)),
      predicate_(std::move(predicate)),
      kind_(kind),
      block_size_(std::max<size_t>(16, block_size)),
      combined_schema_(left_schema_ + right_schema_) {}

bool BatchNestedLoopJoin::EvaluatePredicate(const Row& left,
                                            const Row& right) const {
  if (!predicate_) {
    return true;
  }
  Row combined = left + right;
  try {
    Value res = predicate_->Evaluate(combined, combined_schema_);
    return !res.IsNull() && res.Truthy();
  } catch (...) {
    return false;
  }
}

void BatchNestedLoopJoin::ExecuteBlockJoin() {
  output_.clear();
  output_offset_ = 0;

  // Materialize right side rows
  std::vector<std::pair<Row, RowPosition>> right_rows;
  Row r_row;
  RowPosition r_pos;
  while (right_ && right_->Next(&r_row, &r_pos)) {
    right_rows.emplace_back(std::move(r_row), r_pos);
  }

  std::vector<std::pair<Row, RowPosition>> left_block;
  left_block.reserve(block_size_);

  bool left_exhausted = false;
  size_t total_bytes = 0;

  while (!left_exhausted) {
    left_block.clear();
    Row l_row;
    RowPosition l_pos;
    while (left_block.size() < block_size_ && left_ &&
           left_->Next(&l_row, &l_pos)) {
      left_block.emplace_back(std::move(l_row), l_pos);
    }
    if (left_block.empty()) {
      break;
    }
    if (left_block.size() < block_size_) {
      left_exhausted = true;
    }

    std::vector<size_t> left_match_counts(left_block.size(), 0);

    for (const auto& r_item : right_rows) {
      for (size_t i = 0; i < left_block.size(); ++i) {
        if (EvaluatePredicate(left_block[i].first, r_item.first)) {
          ++left_match_counts[i];
          if (kind_ == JoinKind::kInner || kind_ == JoinKind::kLeftOuter) {
            output_.emplace_back(left_block[i].first + r_item.first,
                                 left_block[i].second);
            total_bytes += EstimateRowBytes(output_.back().first);
          }
        }
      }
    }

    if (kind_ == JoinKind::kLeftOuter) {
      for (size_t i = 0; i < left_block.size(); ++i) {
        if (left_match_counts[i] == 0) {
          output_.emplace_back(
              left_block[i].first +
                  Row(std::vector<Value>(right_schema_.ColumnCount())),
              left_block[i].second);
          total_bytes += EstimateRowBytes(output_.back().first);
        }
      }
    } else if (kind_ == JoinKind::kSemi) {
      for (size_t i = 0; i < left_block.size(); ++i) {
        if (left_match_counts[i] > 0) {
          output_.emplace_back(left_block[i].first, left_block[i].second);
          total_bytes += EstimateRowBytes(output_.back().first);
        }
      }
    } else if (kind_ == JoinKind::kAnti) {
      for (size_t i = 0; i < left_block.size(); ++i) {
        if (left_match_counts[i] == 0) {
          output_.emplace_back(left_block[i].first, left_block[i].second);
          total_bytes += EstimateRowBytes(output_.back().first);
        }
      }
    }
  }

  charge_.Add(total_bytes);
}

void BatchNestedLoopJoin::EnsureMaterialized() {
  if (materialized_) {
    return;
  }
  materialized_ = true;
  ExecuteBlockJoin();
}

void BatchNestedLoopJoin::MaterializePipeline() {
  EnsureMaterialized();
}

bool BatchNestedLoopJoin::Next(Row* dst, RowPosition* rp) {
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

size_t BatchNestedLoopJoin::NextBatch(DataChunk* destination, size_t max_rows) {
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

void BatchNestedLoopJoin::Dump(std::ostream& o, int /*indent*/) const {
  o << "BatchNestedLoopJoin(block_size=" << block_size_
    << ", kind=" << static_cast<int>(kind_) << ")";
}

void BatchNestedLoopJoin::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
