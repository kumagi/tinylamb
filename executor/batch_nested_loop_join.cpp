/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/batch_nested_loop_join.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/join_kind.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/disjunctive_join_branches.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/executor_base.hpp"
#include "executor/join_kind.hpp"
#include "executor/query_memory.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

BatchNestedLoopJoin::BatchNestedLoopJoin(Executor left, Schema left_schema,
                                         Executor right, Schema right_schema,
                                         Expression predicate, JoinKind kind,
                                         size_t block_size)
    : left_(std::move(left)),
      left_schema_(std::move(left_schema)),
      right_(std::move(right)),
      right_schema_(std::move(right_schema)),
      predicate_(std::move(predicate)),
      predicate_conjuncts_(SplitConjuncts(predicate_)),
      kind_(kind),
      block_size_(std::max<size_t>(16, block_size)),
      combined_schema_(left_schema_ + right_schema_) {}

StatusOr<bool> BatchNestedLoopJoin::EvaluatePredicate(const Row& left,
                                                      const Row& right) {
  if (!predicate_) {
    return true;
  }
  // Predicate errors (e.g. division by zero) must propagate to the caller:
  // swallowing them as FALSE corrupts Anti-join output (a failed match would
  // emit the outer row instead of erroring).
  predicate_scratch_.values_.clear();
  predicate_scratch_.values_.reserve(left.values_.size() +
                                     right.values_.size());
  predicate_scratch_.values_.insert(predicate_scratch_.values_.end(),
                                    left.values_.begin(), left.values_.end());
  predicate_scratch_.values_.insert(predicate_scratch_.values_.end(),
                                    right.values_.begin(), right.values_.end());
  const Row& combined = predicate_scratch_;
  if (kind_ == JoinKind::kInner) {
    // An inner-join predicate is a WHERE-level filter: conjuncts may have
    // been reordered by canonicalization, so a sibling that cleanly rejects
    // the pair suppresses errors from the rest (commutative semantics).
    return relational_detail::EvaluateConjunctsTolerant(
        predicate_conjuncts_, [&](const Expression& conjunct) {
          return conjunct->TryEvaluate(combined, combined_schema_);
        });
  }
  StatusOr<Value> res = predicate_->TryEvaluate(combined, combined_schema_);
  if (!res.HasValue()) {
    return res.GetStatus();
  }
  return !res.Value().IsNull() && res.Value().Truthy();
}

bool BatchNestedLoopJoin::TryExecuteDisjunctiveHashJoin(
    const std::vector<std::pair<Row, RowPosition>>& right_rows) {
  if (kind_ != JoinKind::kInner && kind_ != JoinKind::kLeftOuter) {
    return false;
  }
  if (right_rows.empty() ||
      right_rows.size() > std::numeric_limits<uint32_t>::max()) {
    return false;
  }
  auto specs = relational_detail::ExtractDisjunctiveJoinBranches(
      predicate_, left_schema_, right_schema_, combined_schema_);
  if (!specs.has_value()) {
    return false;
  }

  // Lean proof: lean/TinylambOpt/OuterOrBranches.lean (outer_or_branches_sound)
  const size_t branch_count = specs->size();
  std::vector<relational_detail::DisjunctiveBranchBuckets> buckets;
  buckets.reserve(branch_count);
  for (size_t b = 0; b < branch_count; ++b) {
    buckets.emplace_back(std::move((*specs)[b]));
    buckets.back().Reserve(right_rows.size());
  }
  for (size_t r = 0; r < right_rows.size(); ++r) {
    const Row& r_row = right_rows[r].first;
    for (size_t b = 0; b < branch_count; ++b) {
      if (!buckets[b].AddRightRow(static_cast<uint32_t>(r), r_row)) {
        return false;
      }
    }
  }

  const Row null_right(std::vector<Value>(right_schema_.ColumnCount()));
  size_t total_bytes = 0;
  DataChunk left_batch;

  while (left_ != nullptr && left_->NextBatch(&left_batch, block_size_) != 0) {
    for (size_t li = 0; li < left_batch.Size(); ++li) {
      const Row l_row = left_batch.RowAt(li);
      const RowPosition l_pos = left_batch.PositionAt(li);
      bool matched_any = false;

      auto eval_candidate = [&](size_t branch_idx, uint32_t r_idx) -> Status {
        const Row& r_row = right_rows[r_idx].first;
        predicate_scratch_.values_.clear();
        predicate_scratch_.values_.reserve(l_row.values_.size() +
                                           r_row.values_.size());
        predicate_scratch_.values_.insert(predicate_scratch_.values_.end(),
                                          l_row.values_.begin(),
                                          l_row.values_.end());
        predicate_scratch_.values_.insert(predicate_scratch_.values_.end(),
                                          r_row.values_.begin(),
                                          r_row.values_.end());

        StatusOr<Value> res = buckets[branch_idx].Spec().expr->TryEvaluate(
            predicate_scratch_, combined_schema_);
        if (!res.HasValue()) {
          return res.GetStatus();
        }
        if (res.Value().IsNull() || !res.Value().Truthy()) {
          return Status::kSuccess;
        }
        // Exclusive branch guard (Lean `orBranch1`): emit on branch_idx iff
        // IS_TRUE(d_{branch_idx}) AND NOT IS_TRUE(d_j) for every j <
        // branch_idx.
        for (size_t j = 0; j < branch_idx; ++j) {
          StatusOr<Value> prev = buckets[j].Spec().expr->TryEvaluate(
              predicate_scratch_, combined_schema_);
          if (!prev.HasValue()) {
            return prev.GetStatus();
          }
          if (!prev.Value().IsNull() && prev.Value().Truthy()) {
            return Status::kSuccess;
          }
        }
        StatusOr<bool> exact_match = EvaluatePredicate(l_row, r_row);
        if (!exact_match.HasValue()) {
          return exact_match.GetStatus();
        }
        if (!exact_match.Value()) {
          return Status::kSuccess;
        }
        matched_any = true;
        output_.emplace_back(Row(predicate_scratch_), l_pos);
        total_bytes += EstimateRowBytes(output_.back().first);
        return Status::kSuccess;
      };

      for (size_t b = 0; b < branch_count; ++b) {
        std::span<const uint32_t> cands;
        if (buckets[b].Probe(l_row, &cands)) {
          for (const uint32_t r_idx : cands) {
            const Status st = eval_candidate(b, r_idx);
            if (!st.ok()) {
              output_.clear();
              FailWith(st);
              return true;
            }
          }
        } else {
          for (size_t r_idx = 0; r_idx < right_rows.size(); ++r_idx) {
            const Status st = eval_candidate(b, static_cast<uint32_t>(r_idx));
            if (!st.ok()) {
              output_.clear();
              FailWith(st);
              return true;
            }
          }
        }
      }
      if (!matched_any && kind_ == JoinKind::kLeftOuter) {
        output_.emplace_back(l_row + null_right, l_pos);
        total_bytes += EstimateRowBytes(output_.back().first);
      }
    }
  }
  if (left_ != nullptr && FailWithChildOf(*left_)) {
    output_.clear();
    return true;
  }
  charge_.Add(total_bytes);
  return true;
}

void BatchNestedLoopJoin::ExecuteBlockJoin() {
  output_.clear();
  output_offset_ = 0;

  // Materialize right side rows
  std::vector<std::pair<Row, RowPosition>> right_rows;
  DataChunk batch;
  while (right_ && right_->NextBatch(&batch) != 0) {
    // No reserve: reserve(size()+batch) fills capacity exactly, forcing a
    // full reallocation+copy of every materialized Row on the next batch.
    // push_back's geometric growth is already amortized.
    for (size_t i = 0; i < batch.Size(); ++i) {
      right_rows.emplace_back(batch.RowAt(i), batch.PositionAt(i));
    }
  }
  if (right_ != nullptr && FailWithChildOf(*right_)) {
    return;
  }
  if (TryExecuteDisjunctiveHashJoin(right_rows)) {
    return;
  }

  std::vector<std::pair<Row, RowPosition>> left_block;
  left_block.reserve(block_size_);
  std::vector<bool> right_matched(right_rows.size(), false);

  bool left_exhausted = false;
  size_t total_bytes = 0;
  DataChunk left_batch;
  size_t left_batch_idx = 0;

  while (!left_exhausted) {
    left_block.clear();
    while (left_block.size() < block_size_ && left_) {
      if (left_batch_idx >= left_batch.Size()) {
        left_batch_idx = 0;
        if (left_->NextBatch(&left_batch, block_size_ - left_block.size()) ==
            0) {
          break;
        }
      }
      left_block.emplace_back(left_batch.RowAt(left_batch_idx),
                              left_batch.PositionAt(left_batch_idx));
      ++left_batch_idx;
    }
    if (left_ != nullptr && FailWithChildOf(*left_)) {
      output_.clear();
      return;
    }
    if (left_block.empty()) {
      break;
    }
    if (left_block.size() < block_size_) {
      left_exhausted = true;
    }

    std::vector<size_t> left_match_counts(left_block.size(), 0);

    for (size_t r = 0; r < right_rows.size(); ++r) {
      const auto& r_item = right_rows[r];
      for (size_t i = 0; i < left_block.size(); ++i) {
        const auto matches =
            EvaluatePredicate(left_block[i].first, r_item.first);
        if (!matches.HasValue()) {
          output_.clear();
          FailWith(matches.GetStatus());
          return;
        }
        if (matches.Value()) {
          ++left_match_counts[i];
          right_matched[r] = true;
          if (kind_ == JoinKind::kInner || kind_ == JoinKind::kLeftOuter ||
              kind_ == JoinKind::kRightOuter || kind_ == JoinKind::kFullOuter) {
            output_.emplace_back(left_block[i].first + r_item.first,
                                 left_block[i].second);
            total_bytes += EstimateRowBytes(output_.back().first);
          }
        }
      }
    }

    if (kind_ == JoinKind::kLeftOuter || kind_ == JoinKind::kFullOuter) {
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
    } else if (kind_ != JoinKind::kInner && kind_ != JoinKind::kRightOuter) {
      CHECK_MSG(false,
                std::string("BatchNestedLoopJoin: unsupported join kind ") +
                    std::string(JoinKindName(kind_)));
    }
  }
  if (kind_ == JoinKind::kRightOuter || kind_ == JoinKind::kFullOuter) {
    const Row null_left(std::vector<Value>(left_schema_.ColumnCount()));
    for (size_t r = 0; r < right_rows.size(); ++r) {
      if (!right_matched[r]) {
        output_.emplace_back(null_left + right_rows[r].first,
                             right_rows[r].second);
        total_bytes += EstimateRowBytes(output_.back().first);
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

void BatchNestedLoopJoin::MaterializePipeline() { EnsureMaterialized(); }

size_t BatchNestedLoopJoin::NextBatch(DataChunk* destination, size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  destination->Reset();
  EnsureMaterialized();
  if (!ok()) {
    return 0;
  }
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

void BatchNestedLoopJoin::Dump(std::ostream& o, int indent) const {
  o << "BatchNestedLoopJoin(block_size=" << block_size_
    << ", kind=" << static_cast<int>(kind_) << ")";
  o << "\n" << Indent(static_cast<size_t>(indent) + 2);
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(static_cast<size_t>(indent) + 2);
  right_->Dump(o, indent + 2);
}

void BatchNestedLoopJoin::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
