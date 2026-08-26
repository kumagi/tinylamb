/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/merge_join.hpp"

#include <algorithm>
#include <ostream>
#include <stdexcept>
#include <string>

#include "common/constants.hpp"
#include "executor/detail/expression_eval.hpp"
#include "type/value.hpp"

namespace tinylamb {

MergeJoin::MergeJoin(Executor left, std::vector<slot_t> left_columns,
                     Executor right, std::vector<slot_t> right_columns,
                     JoinKind kind, size_t left_width, size_t right_width,
                     Expression residual, Schema residual_schema)
    : left_(std::move(left)),
      right_(std::move(right)),
      left_columns_(std::move(left_columns)),
      right_columns_(std::move(right_columns)),
      kind_(kind),
      left_width_(left_width),
      right_width_(right_width),
      residual_(std::move(residual)),
      residual_schema_(std::move(residual_schema)) {
  if (left_columns_.empty() || left_columns_.size() != right_columns_.size()) {
    throw std::invalid_argument(
        "MergeJoin requires equally-sized non-empty keys");
  }
}

bool MergeJoin::KeyIsNull(const Row& row,
                          const std::vector<slot_t>& columns) const {
  return std::ranges::any_of(
      columns, [&](slot_t column) { return row[column].IsNull(); });
}

int MergeJoin::CompareKeys(const Row& left, const Row& right) const {
  for (size_t i = 0; i < left_columns_.size(); ++i) {
    const Value& lhs = left[left_columns_[i]];
    const Value& rhs = right[right_columns_[i]];
    if (lhs == rhs) {
      continue;
    }
    return lhs < rhs ? -1 : 1;
  }
  return 0;
}

Row MergeJoin::Concatenate(size_t i, size_t j) const {
  return left_rows_[i] + right_rows_[j];
}

bool MergeJoin::PairPasses(size_t i, size_t j) const {
  if (!residual_) {
    return true;
  }
  const Row combined = Concatenate(i, j);
  return residual_->Evaluate(combined, residual_schema_).Truthy();
}

void MergeJoin::Materialize() {
  if (materialized_) {
    return;
  }
  materialized_ = true;
  Row row;
  RowPosition position;
  while (left_->Next(&row, &position)) {
    left_rows_.push_back(row);
    left_positions_.push_back(position);
  }
  while (right_->Next(&row, nullptr)) {
    right_rows_.push_back(row);
  }

  if (left_width_ == 0 && !left_rows_.empty()) {
    left_width_ = left_rows_.front().values_.size();
  }
  if (right_width_ == 0 && !right_rows_.empty()) {
    right_width_ = right_rows_.front().values_.size();
  }

  const bool semi = kind_ == JoinKind::kSemi;
  const bool anti = kind_ == JoinKind::kAnti;
  auto emit_left = [&](size_t index) {
    output_.push_back(left_rows_[index]);
    output_positions_.push_back(left_positions_[index]);
  };

  if (semi || anti) {
    size_t left_index = 0;
    size_t right_index = 0;
    while (left_index < left_rows_.size() && right_index < right_rows_.size()) {
      if (KeyIsNull(left_rows_[left_index], left_columns_)) {
        if (anti) {
          emit_left(left_index);
        }
        ++left_index;
        continue;
      }
      if (KeyIsNull(right_rows_[right_index], right_columns_)) {
        ++right_index;
        continue;
      }
      const int comparison =
          CompareKeys(left_rows_[left_index], right_rows_[right_index]);
      if (comparison < 0) {
        if (anti) {
          emit_left(left_index);
        }
        ++left_index;
        continue;
      }
      if (comparison > 0) {
        ++right_index;
        continue;
      }

      // Equal-key runs: the residual decides which pairs count as matches.
      const size_t run_left_begin = left_index;
      size_t left_end = left_index;
      while (left_end < left_rows_.size() &&
             !KeyIsNull(left_rows_[left_end], left_columns_) &&
             CompareKeys(left_rows_[left_end], right_rows_[right_index]) == 0) {
        ++left_end;
      }
      size_t right_end = right_index;
      while (right_end < right_rows_.size() &&
             !KeyIsNull(right_rows_[right_end], right_columns_) &&
             CompareKeys(left_rows_[run_left_begin], right_rows_[right_end]) ==
                 0) {
        ++right_end;
      }
      std::vector<char> matched_left(left_end - run_left_begin, 0);
      for (size_t i = run_left_begin; i < left_end; ++i) {
        for (size_t j = right_index; j < right_end; ++j) {
          if (!PairPasses(i, j)) {
            continue;
          }
          matched_left[i - run_left_begin] = 1;
          break;
        }
      }
      for (size_t i = run_left_begin; i < left_end; ++i) {
        const bool matched = matched_left[i - run_left_begin] != 0;
        if (semi == matched) {
          emit_left(i);
        }
      }
      left_index = left_end;
      right_index = right_end;
    }
    if (anti) {
      while (left_index < left_rows_.size()) {
        emit_left(left_index++);
      }
    }
    return;
  }

  const bool left_outer =
      kind_ == JoinKind::kLeftOuter || kind_ == JoinKind::kFullOuter;
  const bool right_outer =
      kind_ == JoinKind::kRightOuter || kind_ == JoinKind::kFullOuter;
  const Row null_left{std::vector<Value>(left_width_)};
  const Row null_right{std::vector<Value>(right_width_)};
  auto emit_padded_left = [&](size_t index) {
    output_.push_back(left_rows_[index] + null_right);
  };
  auto emit_padded_right = [&](const Row& right_row) {
    output_.push_back(null_left + right_row);
  };
  size_t left_index = 0;
  size_t right_index = 0;
  while (left_index < left_rows_.size() && right_index < right_rows_.size()) {
    if (KeyIsNull(left_rows_[left_index], left_columns_)) {
      if (left_outer) {
        emit_padded_left(left_index);
      }
      ++left_index;
      continue;
    }
    if (KeyIsNull(right_rows_[right_index], right_columns_)) {
      if (right_outer) {
        emit_padded_right(right_rows_[right_index]);
      }
      ++right_index;
      continue;
    }
    const int comparison =
        CompareKeys(left_rows_[left_index], right_rows_[right_index]);
    if (comparison < 0) {
      if (left_outer) {
        emit_padded_left(left_index);
      }
      ++left_index;
      continue;
    }
    if (comparison > 0) {
      if (right_outer) {
        emit_padded_right(right_rows_[right_index]);
      }
      ++right_index;
      continue;
    }

    // Equal-key runs crossed through the residual filter; sides without a
    // single surviving pair are unmatched and get NULL-padded.
    size_t left_end = left_index;
    while (left_end < left_rows_.size() &&
           !KeyIsNull(left_rows_[left_end], left_columns_) &&
           CompareKeys(left_rows_[left_end], right_rows_[right_index]) == 0) {
      ++left_end;
    }
    size_t right_end = right_index;
    while (right_end < right_rows_.size() &&
           !KeyIsNull(right_rows_[right_end], right_columns_) &&
           CompareKeys(left_rows_[left_index], right_rows_[right_end]) == 0) {
      ++right_end;
    }
    std::vector<char> matched_left(left_end - left_index, 0);
    std::vector<char> matched_right(right_end - right_index, 0);
    for (size_t i = left_index; i < left_end; ++i) {
      for (size_t j = right_index; j < right_end; ++j) {
        if (!PairPasses(i, j)) {
          continue;
        }
        matched_left[i - left_index] = 1;
        matched_right[j - right_index] = 1;
        output_.push_back(left_rows_[i] + right_rows_[j]);
      }
    }
    for (size_t i = left_index; i < left_end; ++i) {
      if (left_outer && matched_left[i - left_index] == 0) {
        emit_padded_left(i);
      }
    }
    for (size_t j = right_index; j < right_end; ++j) {
      if (right_outer && matched_right[j - right_index] == 0) {
        emit_padded_right(right_rows_[j]);
      }
    }
    left_index = left_end;
    right_index = right_end;
  }
  if (left_outer) {
    while (left_index < left_rows_.size()) {
      emit_padded_left(left_index++);
    }
  }
  if (right_outer) {
    while (right_index < right_rows_.size()) {
      emit_padded_right(right_rows_[right_index++]);
    }
  }
}

bool MergeJoin::Next(Row* dst, RowPosition* rp) {
  Materialize();
  if (output_index_ >= output_.size()) {
    return false;
  }
  *dst = output_[output_index_];
  if (rp != nullptr && (kind_ == JoinKind::kSemi || kind_ == JoinKind::kAnti)) {
    *rp = output_positions_[output_index_];
  }
  ++output_index_;
  return true;
}

void MergeJoin::Dump(std::ostream& o, int /*indent*/) const {
  o << "MergeJoin (sorted keys)";
}

}  // namespace tinylamb
