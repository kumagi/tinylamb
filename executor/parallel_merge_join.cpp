/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/parallel_merge_join.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <thread>
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

ParallelMergeJoin::ParallelMergeJoin(
    Executor left, std::vector<slot_t> left_cols, Executor right,
    std::vector<slot_t> right_cols, size_t worker_count, JoinKind kind,
    Expression residual, Schema residual_schema)
    : left_(std::move(left)),
      left_cols_(std::move(left_cols)),
      right_(std::move(right)),
      right_cols_(std::move(right_cols)),
      worker_count_(std::max<size_t>(1, worker_count)),
      kind_(kind),
      residual_(std::move(residual)),
      residual_schema_(std::move(residual_schema)) {}

bool ParallelMergeJoin::KeyIsNull(const Row& row,
                                  const std::vector<slot_t>& cols) const {
  for (slot_t col : cols) {
    if (col < row.Size() && row[col].IsNull()) {
      return true;
    }
  }
  return false;
}

int ParallelMergeJoin::CompareKeys(const Row& left, const Row& right) const {
  const size_t n = std::min(left_cols_.size(), right_cols_.size());
  for (size_t i = 0; i < n; ++i) {
    const Value& lv = left[left_cols_[i]];
    const Value& rv = right[right_cols_[i]];
    if (lv < rv) return -1;
    if (rv < lv) return 1;
  }
  return 0;
}

void ParallelMergeJoin::ComputeSteeringPartitions() {
  partitions_.clear();
  const size_t left_n = left_rows_.size();
  const size_t right_n = right_rows_.size();

  if (left_n == 0 || right_n == 0) {
    partitions_.push_back(PartitionRange{0, left_n, 0, right_n});
    return;
  }

  const size_t num_partitions =
      std::min(worker_count_, std::max<size_t>(1, left_n / 16));

  if (num_partitions <= 1) {
    partitions_.push_back(PartitionRange{0, left_n, 0, right_n});
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
      size_t mid = low + (high - low) / 2;
      if (CompareKeys(bound_row, right_rows_[mid].first) > 0) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    size_t split_r = low;

    partitions_.push_back(PartitionRange{prev_l, target_l, prev_r, split_r});
    prev_l = target_l;
    prev_r = split_r;
  }

  partitions_.push_back(PartitionRange{prev_l, left_n, prev_r, right_n});
}

void ParallelMergeJoin::ExecuteParallelMerge() {
  std::vector<std::vector<std::pair<Row, RowPosition>>> part_outputs(
      partitions_.size());

  auto worker_func = [&](size_t part_idx) {
    const auto& range = partitions_[part_idx];
    auto& out = part_outputs[part_idx];

    size_t l = range.left_start;
    size_t r = range.right_start;

    while (l < range.left_end && r < range.right_end) {
      if (KeyIsNull(left_rows_[l].first, left_cols_)) {
        if (kind_ == JoinKind::kAnti || kind_ == JoinKind::kLeftOuter) {
          out.emplace_back(left_rows_[l].first, left_rows_[l].second);
        }
        ++l;
        continue;
      }
      if (KeyIsNull(right_rows_[r].first, right_cols_)) {
        ++r;
        continue;
      }

      int cmp = CompareKeys(left_rows_[l].first, right_rows_[r].first);
      if (cmp < 0) {
        if (kind_ == JoinKind::kAnti || kind_ == JoinKind::kLeftOuter) {
          if (kind_ == JoinKind::kLeftOuter) {
            std::vector<Value> vals = left_rows_[l].first.values_;
            for (size_t c = 0; c < right_cols_.size(); ++c) {
              vals.push_back(Value());
            }
            out.emplace_back(Row(std::move(vals)), left_rows_[l].second);
          } else {
            out.emplace_back(left_rows_[l].first, left_rows_[l].second);
          }
        }
        ++l;
      } else if (cmp > 0) {
        ++r;
      } else {
        // Equal key cluster match
        size_t l_end = l + 1;
        while (l_end < range.left_end &&
               CompareKeys(left_rows_[l].first, left_rows_[l_end].first) == 0) {
          ++l_end;
        }
        size_t r_end = r + 1;
        while (r_end < range.right_end &&
               CompareKeys(right_rows_[r].first, right_rows_[r_end].first) == 0) {
          ++r_end;
        }

        switch (kind_) {
          case JoinKind::kInner:
          case JoinKind::kLeftOuter:
            for (size_t li = l; li < l_end; ++li) {
              for (size_t ri = r; ri < r_end; ++ri) {
                std::vector<Value> vals;
                vals.reserve(left_rows_[li].first.Size() +
                             right_rows_[ri].first.Size());
                for (size_t c = 0; c < left_rows_[li].first.Size(); ++c) {
                  vals.push_back(left_rows_[li].first[c]);
                }
                for (size_t c = 0; c < right_rows_[ri].first.Size(); ++c) {
                  vals.push_back(right_rows_[ri].first[c]);
                }
                out.emplace_back(Row(std::move(vals)), left_rows_[li].second);
              }
            }
            break;
          case JoinKind::kSemi:
            for (size_t li = l; li < l_end; ++li) {
              out.emplace_back(left_rows_[li].first, left_rows_[li].second);
            }
            break;
          case JoinKind::kAnti:
            // Matches exist, anti emits nothing
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
      if (kind_ == JoinKind::kAnti || kind_ == JoinKind::kLeftOuter) {
        if (kind_ == JoinKind::kLeftOuter) {
          std::vector<Value> vals = left_rows_[l].first.values_;
          for (size_t c = 0; c < right_cols_.size(); ++c) {
            vals.push_back(Value());
          }
          out.emplace_back(Row(std::move(vals)), left_rows_[l].second);
        } else {
          out.emplace_back(left_rows_[l].first, left_rows_[l].second);
        }
      }
      ++l;
    }
  };

  if (partitions_.size() <= 1) {
    worker_func(0);
  } else {
    std::vector<std::thread> workers;
    workers.reserve(partitions_.size());
    for (size_t p = 0; p < partitions_.size(); ++p) {
      workers.emplace_back(worker_func, p);
    }
    for (auto& w : workers) {
      w.join();
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
  materialized_ = true;
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

  ComputeSteeringPartitions();
  ExecuteParallelMerge();
}

void ParallelMergeJoin::MaterializePipeline() {
  EnsureMaterialized();
}

bool ParallelMergeJoin::Next(Row* dst, RowPosition* rp) {
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

size_t ParallelMergeJoin::NextBatch(DataChunk* destination, size_t max_rows) {
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

void ParallelMergeJoin::Dump(std::ostream& o, int /*indent*/) const {
  o << "ParallelMergeJoin(workers=" << worker_count_
    << ", partitions=" << partitions_.size()
    << ", kind=" << static_cast<int>(kind_) << ")";
}

void ParallelMergeJoin::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
