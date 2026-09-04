/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/nested_loop_join.hpp"

#include <cstddef>
#include <ostream>
#include <stdexcept>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

NestedLoopJoin::NestedLoopJoin(Executor left, Schema left_schema,
                               Executor right, Schema right_schema,
                               Expression predicate, JoinKind kind,
                               size_t block_size, bool assert_unique)
    : left_(std::move(left)),
      left_schema_(std::move(left_schema)),
      right_(std::move(right)),
      right_schema_(std::move(right_schema)),
      predicate_(std::move(predicate)),
      kind_(kind),
      block_size_(block_size == 0 ? 1024 : block_size),
      assert_unique_(assert_unique),
      combined_schema_(left_schema_ + right_schema_) {}

bool NestedLoopJoin::EvaluatePredicate(const Row& left,
                                       const Row& right) const {
  if (!predicate_) {
    return true;
  }
  // Predicate errors must propagate (see BatchNestedLoopJoin): swallowing
  // them as FALSE corrupts Anti-join output.
  Row combined = left + right;
  Value res = predicate_->Evaluate(combined, combined_schema_);
  return !res.IsNull() && res.Truthy();
}

void NestedLoopJoin::Materialize() {
  if (materialize_error_ != nullptr) {
    std::rethrow_exception(materialize_error_);
  }
  output_.clear();
  output_offset_ = 0;

  // Materialize the right side into memory so it can be re-scanned across
  // blocks.
  std::vector<std::pair<Row, RowPosition>> right_rows;
  Row r_row;
  RowPosition r_pos;
  while (right_->Next(&r_row, &r_pos)) {
    right_rows.emplace_back(std::move(r_row), r_pos);
  }

  // Stream left side in blocks.
  std::vector<std::pair<Row, RowPosition>> left_block;
  left_block.reserve(block_size_);

  bool left_exhausted = false;
  while (!left_exhausted) {
    left_block.clear();
    Row l_row;
    RowPosition l_pos;
    while (left_block.size() < block_size_ && left_->Next(&l_row, &l_pos)) {
      left_block.emplace_back(std::move(l_row), l_pos);
    }
    if (left_block.empty()) {
      break;
    }
    if (left_block.size() < block_size_) {
      left_exhausted = true;
    }

    std::vector<size_t> left_match_count(left_block.size(), 0);
    std::vector<std::pair<Row, RowPosition>> single_matches(left_block.size());

    for (const auto& r_item : right_rows) {
      for (size_t i = 0; i < left_block.size(); ++i) {
        if (EvaluatePredicate(left_block[i].first, r_item.first)) {
          ++left_match_count[i];
          if (kind_ == JoinKind::kSingle && left_match_count[i] > 1) {
            throw std::runtime_error(
                "Single join violation: more than one match found for outer "
                "row");
          }
          if (assert_unique_ && kind_ == JoinKind::kSemi &&
              left_match_count[i] > 1) {
            throw std::runtime_error(
                "Semi join uniqueness assertion failed: multiple matches for "
                "outer row");
          }
          if (kind_ == JoinKind::kInner || kind_ == JoinKind::kLeftOuter) {
            output_.emplace_back(left_block[i].first + r_item.first,
                                 left_block[i].second);
          } else if (kind_ == JoinKind::kSingle) {
            single_matches[i] = std::make_pair(
                left_block[i].first + r_item.first, left_block[i].second);
          }
        }
      }
    }

    if (kind_ == JoinKind::kLeftOuter) {
      for (size_t i = 0; i < left_block.size(); ++i) {
        if (left_match_count[i] == 0) {
          output_.emplace_back(
              left_block[i].first +
                  Row(std::vector<Value>(right_schema_.ColumnCount())),
              left_block[i].second);
        }
      }
    } else if (kind_ == JoinKind::kSingle) {
      for (size_t i = 0; i < left_block.size(); ++i) {
        if (left_match_count[i] == 1) {
          output_.push_back(std::move(single_matches[i]));
        } else if (left_match_count[i] == 0) {
          output_.emplace_back(
              left_block[i].first +
                  Row(std::vector<Value>(right_schema_.ColumnCount())),
              left_block[i].second);
        }
      }
    } else if (kind_ == JoinKind::kSemi) {
      for (size_t i = 0; i < left_block.size(); ++i) {
        if (left_match_count[i] > 0) {
          output_.emplace_back(left_block[i].first, left_block[i].second);
        }
      }
    } else if (kind_ == JoinKind::kAnti) {
      for (size_t i = 0; i < left_block.size(); ++i) {
        if (left_match_count[i] == 0) {
          output_.emplace_back(left_block[i].first, left_block[i].second);
        }
      }
    } else if (kind_ == JoinKind::kMark) {
      for (size_t i = 0; i < left_block.size(); ++i) {
        Value marker = left_match_count[i] > 0 ? Value(true) : Value(false);
        output_.emplace_back(left_block[i].first + Row({marker}),
                             left_block[i].second);
      }
    }
  }
}

bool NestedLoopJoin::Next(Row* dst, RowPosition* rp) {
  if (!materialized_) {
    try {
      Materialize();
      materialized_ = true;
    } catch (...) {
      materialize_error_ = std::current_exception();
      materialized_ = true;  // Prevent a retry that would see drained inputs.
      throw;
    }
  }
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

void NestedLoopJoin::Dump(std::ostream& o, int indent) const {
  o << "NestedLoopJoin: \n" << Indent(indent + 2);
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(indent + 2);
  right_->Dump(o, indent + 2);
}

}  // namespace tinylamb
