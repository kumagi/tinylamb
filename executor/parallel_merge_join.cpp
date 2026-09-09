/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/parallel_merge_join.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <exception>
#include <iostream>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/query_memory.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

ParallelMergeJoin::ParallelMergeJoin(
    Executor left, std::vector<slot_t> left_cols, Executor right,
    std::vector<slot_t> right_cols, size_t worker_count, JoinKind kind,
    Expression residual, Schema residual_schema, size_t right_width)
    : left_(std::move(left)),
      left_cols_(std::move(left_cols)),
      right_(std::move(right)),
      right_cols_(std::move(right_cols)),
      worker_count_(std::max<size_t>(1, worker_count)),
      kind_(kind),
      residual_(std::move(residual)),
      residual_schema_(std::move(residual_schema)),
      right_width_(right_width) {}

bool ParallelMergeJoin::KeyIsNull(const Row& row,
                                  const std::vector<slot_t>& cols) {
  return std::ranges::any_of(cols, [&row](slot_t col) {
    return col < row.Size() && row[col].IsNull();
  });
}

bool ParallelMergeJoin::PairPasses(const Row& left, const Row& right) const {
  if (!residual_) {
    return true;
  }
  const Row combined = left + right;
  StatusOr<Value> res = residual_->TryEvaluate(combined, residual_schema_);
  if (!res.HasValue()) {
    residual_error_ = res.GetStatus();
    return false;
  }
  return res.Value().Truthy();
}

int ParallelMergeJoin::CompareKeys(const Row& left, const Row& right) const {
  const size_t n = std::min(left_cols_.size(), right_cols_.size());
  for (size_t i = 0; i < n; ++i) {
    const Value& lv = left[left_cols_[i]];
    const Value& rv = right[right_cols_[i]];
    if (lv < rv) {
      return -1;
    }
    if (rv < lv) {
      return 1;
    }
  }
  return 0;
}

void ParallelMergeJoin::ComputeSteeringPartitions() {
  partitions_.clear();
  const size_t left_n = left_rows_.size();
  const size_t right_n = right_rows_.size();

  if (left_n == 0 || right_n == 0) {
    partitions_.push_back(PartitionRange{.left_start = 0,
                                         .left_end = left_n,
                                         .right_start = 0,
                                         .right_end = right_n});
    return;
  }

  const size_t num_partitions =
      std::min(worker_count_, std::max<size_t>(1, left_n / 16));

  if (num_partitions <= 1) {
    partitions_.push_back(PartitionRange{.left_start = 0,
                                         .left_end = left_n,
                                         .right_start = 0,
                                         .right_end = right_n});
    return;
  }

  size_t prev_l = 0;
  size_t prev_r = 0;

  for (size_t p = 1; p < num_partitions; ++p) {
    size_t target_l = (left_n * p) / num_partitions;
    // Advance target_l past equal-key cluster boundary
    while (target_l < left_n &&
           CompareKeys(left_rows_[target_l].first,
                       left_rows_[target_l - 1].first) == 0) {
      ++target_l;
    }
    if (target_l >= left_n) {
      break;
    }

    // Binary search on right_rows_ for first right row >= left_rows_[target_l]
    const Row& bound_row = left_rows_[target_l].first;
    size_t low = prev_r;
    size_t high = right_n;
    while (low < high) {
      size_t mid = low + ((high - low) / 2);
      if (CompareKeys(bound_row, right_rows_[mid].first) > 0) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    size_t split_r = low;

    partitions_.push_back(PartitionRange{.left_start = prev_l,
                                         .left_end = target_l,
                                         .right_start = prev_r,
                                         .right_end = split_r});
    prev_l = target_l;
    prev_r = split_r;
  }

  partitions_.push_back(PartitionRange{.left_start = prev_l,
                                       .left_end = left_n,
                                       .right_start = prev_r,
                                       .right_end = right_n});
}

void ParallelMergeJoin::ExecuteParallelMerge() {
  std::vector<std::vector<std::pair<Row, RowPosition>>> part_outputs(
      partitions_.size());

  auto worker_func = [&](size_t part_idx) {
    const auto& range = partitions_[part_idx];
    auto& out = part_outputs[part_idx];

    // Left-outer survivors need a NULL-padded right side with the FULL right
    // width: padding only the key columns would mix two row widths in one
    // output and trip "data chunk row width mismatch" downstream.
    auto emit_padded_left = [&](size_t li) {
      const Row& left_row = left_rows_[li].first;
      std::vector<Value> vals = left_row.values_;
      // Row::Size() is the serialized byte size, not the column count; the
      // NULL padding must widen to the right row's COLUMN count.
      vals.resize(left_row.values_.size() + right_width_);
      out.emplace_back(Row(std::move(vals)), left_rows_[li].second);
    };
    // Right-outer mirror: NULL-padded left side with the full left width.
    auto emit_padded_right = [&](size_t ri) {
      const Row& right_row = right_rows_[ri].first;
      std::vector<Value> vals;
      vals.reserve(right_row.values_.size() + left_width_);
      vals.resize(left_width_);
      vals.insert(vals.end(), right_row.values_.begin(),
                  right_row.values_.end());
      out.emplace_back(Row(std::move(vals)), right_rows_[ri].second);
    };
    const bool emits_matched =
        kind_ == JoinKind::kInner || kind_ == JoinKind::kLeftOuter ||
        kind_ == JoinKind::kRightOuter || kind_ == JoinKind::kFullOuter;
    const bool pads_left =
        kind_ == JoinKind::kLeftOuter || kind_ == JoinKind::kFullOuter;
    const bool pads_right =
        kind_ == JoinKind::kRightOuter || kind_ == JoinKind::kFullOuter;

    size_t l = range.left_start;
    size_t r = range.right_start;

    while (l < range.left_end && r < range.right_end) {
      if (KeyIsNull(left_rows_[l].first, left_cols_)) {
        if (kind_ == JoinKind::kAnti) {
          out.emplace_back(left_rows_[l].first, left_rows_[l].second);
        } else if (pads_left) {
          emit_padded_left(l);
        }
        ++l;
        continue;
      }
      if (KeyIsNull(right_rows_[r].first, right_cols_)) {
        // NULL keys never match; RIGHT/FULL outer still keep the row.
        if (pads_right) {
          emit_padded_right(r);
        }
        ++r;
        continue;
      }

      int cmp = CompareKeys(left_rows_[l].first, right_rows_[r].first);
      if (cmp < 0) {
        if (kind_ == JoinKind::kAnti) {
          out.emplace_back(left_rows_[l].first, left_rows_[l].second);
        } else if (pads_left) {
          emit_padded_left(l);
        }
        ++l;
      } else if (cmp > 0) {
        if (pads_right) {
          emit_padded_right(r);
        }
        ++r;
      } else {
        // Equal key cluster match.  The residual decides which pairs count
        // as matches; unmatched left rows fall back to the outer/anti
        // semantics (mirrors the serial MergeJoin).
        size_t l_end = l + 1;
        while (l_end < range.left_end &&
               CompareKeys(left_rows_[l].first, left_rows_[l_end].first) == 0) {
          ++l_end;
        }
        size_t r_end = r + 1;
        while (r_end < range.right_end &&
               CompareKeys(right_rows_[r].first, right_rows_[r_end].first) ==
                   0) {
          ++r_end;
        }

        std::vector<char> matched_left(l_end - l, 0);
        std::vector<char> matched_right(r_end - r, 0);
        for (size_t li = l; li < l_end; ++li) {
          for (size_t ri = r; ri < r_end; ++ri) {
            if (!PairPasses(left_rows_[li].first, right_rows_[ri].first)) {
              continue;
            }
            matched_left[li - l] = 1;
            matched_right[ri - r] = 1;
            if (emits_matched) {
              const std::vector<Value>& left_values =
                  left_rows_[li].first.values_;
              const std::vector<Value>& right_values =
                  right_rows_[ri].first.values_;
              std::vector<Value> vals;
              vals.reserve(left_values.size() + right_values.size());
              vals.insert(vals.end(), left_values.begin(), left_values.end());
              vals.insert(vals.end(), right_values.begin(), right_values.end());
              out.emplace_back(Row(std::move(vals)), left_rows_[li].second);
            }
          }
        }
        switch (kind_) {
          case JoinKind::kLeftOuter:
          case JoinKind::kFullOuter:
            for (size_t li = l; li < l_end; ++li) {
              if (matched_left[li - l] == 0) {
                emit_padded_left(li);
              }
            }
            if (kind_ == JoinKind::kFullOuter) {
              for (size_t ri = r; ri < r_end; ++ri) {
                if (matched_right[ri - r] == 0) {
                  emit_padded_right(ri);
                }
              }
            }
            break;
          case JoinKind::kRightOuter:
            for (size_t ri = r; ri < r_end; ++ri) {
              if (matched_right[ri - r] == 0) {
                emit_padded_right(ri);
              }
            }
            break;
          case JoinKind::kSemi:
            for (size_t li = l; li < l_end; ++li) {
              if (matched_left[li - l] != 0) {
                out.emplace_back(left_rows_[li].first, left_rows_[li].second);
              }
            }
            break;
          case JoinKind::kAnti:
            for (size_t li = l; li < l_end; ++li) {
              if (matched_left[li - l] == 0) {
                out.emplace_back(left_rows_[li].first, left_rows_[li].second);
              }
            }
            break;
          default:
            break;
        }

        l = l_end;
        r = r_end;
      }
    }

    // Trailing left rows
    while (l < range.left_end) {
      if (kind_ == JoinKind::kAnti) {
        out.emplace_back(left_rows_[l].first, left_rows_[l].second);
      } else if (pads_left) {
        emit_padded_left(l);
      }
      ++l;
    }
    // Trailing right rows (RIGHT/FULL outer keep unmatched right rows).
    while (r < range.right_end) {
      if (pads_right) {
        emit_padded_right(r);
      }
      ++r;
    }
  };

  if (partitions_.size() <= 1) {
    worker_func(0);
  } else {
    std::vector<std::thread> workers;
    std::exception_ptr worker_failure;
    std::mutex failure_mutex;
    workers.reserve(partitions_.size());
    for (size_t p = 0; p < partitions_.size(); ++p) {
      workers.emplace_back([&, p]() {
        try {
          worker_func(p);
        } catch (...) {
          // An exception escaping a raw std::thread terminates the process;
          // latch the first failure and rethrow on the joining thread.
          std::scoped_lock guard(failure_mutex);
          if (!worker_failure) {
            worker_failure = std::current_exception();
          }
        }
      });
    }
    for (auto& w : workers) {
      w.join();
    }
    if (worker_failure) {
      std::rethrow_exception(worker_failure);
    }
  }

  size_t total_out = 0;
  for (const auto& out : part_outputs) {
    total_out += out.size();
  }
  output_.reserve(total_out);
  for (auto& out : part_outputs) {
    for (auto& item : out) {
      output_.push_back(std::move(item));
    }
  }

  size_t bytes = output_.size() * sizeof(std::pair<Row, RowPosition>);
  charge_.Add(bytes);
}

void ParallelMergeJoin::EnsureMaterialized() {
  if (materialized_) {
    return;
  }
  left_rows_.clear();
  right_rows_.clear();
  output_.clear();
  output_offset_ = 0;

  Row row;
  RowPosition rp;
  while (left_ && left_->Next(&row, &rp)) {
    left_rows_.emplace_back(std::move(row), rp);
  }
  while (right_ && right_->Next(&row, &rp)) {
    right_rows_.emplace_back(std::move(row), rp);
  }
  // NULL-padding needs the full right row width; rows can only supply it
  // when the right side is non-empty, so constructors may pass it explicitly
  // (mirrors the serial MergeJoin's width contract).
  if (right_width_ == 0 && !right_rows_.empty()) {
    right_width_ = right_rows_.front().first.values_.size();
  }
  if (left_width_ == 0 && !left_rows_.empty()) {
    left_width_ = left_rows_.front().first.values_.size();
  }
  FailWithChildOf(*left_);
  FailWithChildOf(*right_);

  // Latch only after the work completes so a failed materialization can be
  // retried instead of silently yielding an empty output.
  ComputeSteeringPartitions();
  ExecuteParallelMerge();
  materialized_ = true;
}

void ParallelMergeJoin::MaterializePipeline() { EnsureMaterialized(); }

bool ParallelMergeJoin::Next(Row* dst, RowPosition* rp) {
  assert(dst != nullptr);
  EnsureMaterialized();
  if (residual_error_ != Status::kSuccess) {
    return FailWith(residual_error_);
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

size_t ParallelMergeJoin::NextBatch(DataChunk* destination, size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  destination->Reset();
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

void ParallelMergeJoin::Dump(std::ostream& o, int /*indent*/) const {
  o << "ParallelMergeJoin(workers=" << worker_count_
    << ", partitions=" << partitions_.size()
    << ", kind=" << static_cast<int>(kind_) << ")";
}

void ParallelMergeJoin::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
