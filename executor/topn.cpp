/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/topn.hpp"

#include <algorithm>
#include <cstddef>
#include <ostream>
#include <queue>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
Status TopNExecutor::Materialize() {
  if (limit_ == 0) {
    output_end_ = 0;
    materialized_ = true;
    return Status::kSuccess;
  }
  const size_t capacity = offset_ > static_cast<size_t>(-1) - limit_
                              ? static_cast<size_t>(-1)
                              : offset_ + limit_;
  heap_capacity_ = capacity;
  auto precedes = [this](const Candidate& left, const Candidate& right) {
    for (size_t i = 0; i < left.keys.size(); ++i) {
      const Value& lhs = left.keys[i];
      const Value& rhs = right.keys[i];
      if (lhs.IsNull() || rhs.IsNull()) {
        if (lhs.IsNull() != rhs.IsNull()) {
          const bool nulls_first =
              keys_[i].nulls_first.value_or(keys_[i].ascending);
          return lhs.IsNull() == nulls_first;
        }
        continue;
      }
      // GoogleSQL ordering treats every NaN as a single value above +inf;
      // the plain operator< returns false in both directions, which used to
      // leave NaN rows in arbitrary arrival order and made ORDER BY + LIMIT
      // (TopN) disagree with an unbounded SortExecutor.
      if (lhs.type == ValueType::kDouble && rhs.type == ValueType::kDouble) {
        const int cmp = CompareForOrderBy(lhs, rhs);
        if (cmp == 0) {
          continue;
        }
        return cmp < 0 ? keys_[i].ascending : !keys_[i].ascending;
      }
      if (lhs == rhs) {
        continue;
      }
      return keys_[i].ascending ? lhs < rhs : rhs < lhs;
    }
    return left.sequence < right.sequence;
  };
  auto worse_first = [&precedes](const Candidate& left,
                                 const Candidate& right) {
    return precedes(left, right);
  };
  Row row;
  RowPosition position;
  size_t sequence = 0;
  if (with_ties_) {
    while (source_->Next(&row, &position)) {
      Candidate candidate{.row = std::move(row),
                          .position = position,
                          .keys = {},
                          .sequence = sequence++};
      candidate.keys.reserve(keys_.size());
      for (const Key& key : keys_) {
        StatusOr<Value> key_value =
            key.expression->TryEvaluate(candidate.row, schema_);
        if (!key_value.HasValue()) {
          const Status key_error = key_value.GetStatus();
          FailWith(key_error);
          return key_error;
        }
        candidate.keys.push_back(key_value.MoveValue());
      }
      rows_.push_back(std::move(candidate));
    }
    FailWithChildOf(*source_);
    std::ranges::sort(rows_, precedes);
    output_index_ = std::min(offset_, rows_.size());
    output_end_ = output_index_;
    if (output_index_ < rows_.size()) {
      const size_t boundary = limit_ > rows_.size() - output_index_
                                  ? rows_.size()
                                  : output_index_ + limit_;
      output_end_ = boundary;
      if (boundary != 0 && boundary <= rows_.size()) {
        const Candidate& last = rows_[boundary - 1];
        while (output_end_ < rows_.size()) {
          bool tied = true;
          for (size_t i = 0; i < last.keys.size(); ++i) {
            if (!(last.keys[i] == rows_[output_end_].keys[i])) {
              tied = false;
              break;
            }
          }
          if (!tied) {
            break;
          }
          ++output_end_;
        }
      }
    }
    materialized_ = true;
    return GetStatus();
  }

  std::priority_queue<Candidate, std::vector<Candidate>, decltype(worse_first)>
      heap(worse_first);

  while (source_->Next(&row, &position)) {
    ++input_rows_;
    Candidate candidate{.row = std::move(row),
                        .position = position,
                        .keys = {},
                        .sequence = sequence++};
    candidate.keys.reserve(keys_.size());
    for (const Key& key : keys_) {
      StatusOr<Value> key_value =
          key.expression->TryEvaluate(candidate.row, schema_);
      if (!key_value.HasValue()) {
        const Status key_error = key_value.GetStatus();
        FailWith(key_error);
        return key_error;
      }
      candidate.keys.push_back(key_value.MoveValue());
    }
    if (heap.size() < capacity) {
      heap.push(std::move(candidate));
    } else if (capacity != 0 && precedes(candidate, heap.top())) {
      heap.pop();
      heap.push(std::move(candidate));
    }
  }
  FailWithChildOf(*source_);
  rows_.reserve(heap.size());
  while (!heap.empty()) {
    rows_.push_back(std::move(const_cast<Candidate&>(heap.top())));
    heap.pop();
  }
  std::ranges::sort(rows_, precedes);
  output_index_ = std::min(offset_, rows_.size());
  output_end_ = rows_.size();
  materialized_ = true;
  return GetStatus();
}

bool TopNExecutor::Next(Row* dst, RowPosition* position) {
  if (!materialized_) {
    if (Materialize() != Status::kSuccess) {
      return false;
    }
  }
  if (output_index_ >= output_end_) {
    return false;
  }
  *dst = std::move(rows_[output_index_].row);
  if (position != nullptr) {
    *position = rows_[output_index_].position;
  }
  ++output_index_;
  return true;
}

void TopNExecutor::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << "TopN (limit=" << limit_
         << ", offset=" << offset_ << (with_ties_ ? ", with ties" : "") << ")\n"
         << Indent(static_cast<size_t>(indent) + 2);
  source_->Dump(output, indent + 2);
  if (materialized_) {
    output << "\n"
           << Indent(static_cast<size_t>(indent))
           << "TopN heap_capacity=" << heap_capacity_
           << " input_rows=" << input_rows_ << " output_rows="
           << (output_end_ - std::min(offset_, rows_.size()));
  }
}

}  // namespace tinylamb
