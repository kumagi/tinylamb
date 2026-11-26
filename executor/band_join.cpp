/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/band_join.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <ostream>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "common/status_or.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/executor_base.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

bool IsOrderedComparison(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kLessThan:
    case BinaryOperation::kLessThanEquals:
    case BinaryOperation::kGreaterThan:
    case BinaryOperation::kGreaterThanEquals:
      return true;
    default:
      return false;
  }
}

bool IsOuter(JoinKind kind) {
  return kind == JoinKind::kLeftOuter || kind == JoinKind::kRightOuter ||
         kind == JoinKind::kFullOuter;
}

}  // namespace

BandJoin::BandJoin(Executor left, std::vector<slot_t> left_columns,
                   Executor right, std::vector<slot_t> right_columns,
                   BinaryOperation comparison, Expression predicate,
                   Schema residual_schema, JoinKind kind, size_t left_width,
                   size_t right_width, Schema output_schema)
    : left_(std::move(left)),
      left_columns_(std::move(left_columns)),
      right_(std::move(right)),
      right_columns_(std::move(right_columns)),
      comparison_(comparison),
      predicate_(std::move(predicate)),
      predicate_conjuncts_(SplitConjuncts(predicate_)),
      residual_schema_(std::move(residual_schema)),
      output_schema_(std::move(output_schema)),
      kind_(kind),
      left_width_(left_width),
      right_width_(right_width) {
  CHECK_MSG(left_ != nullptr && right_ != nullptr,
            "BandJoin requires non-null inputs");
  CHECK_MSG(left_columns_.size() == 1,
            "BandJoin requires one key column per input");
  CHECK_MSG(right_columns_.size() == 1,
            "BandJoin requires one key column per input");
  CHECK_MSG(IsOrderedComparison(comparison_) && predicate_ != nullptr,
            "BandJoin requires an ordered comparison predicate");
  const bool unsupported_kind =
      kind_ == JoinKind::kMark || kind_ == JoinKind::kNullAwareAnti;
  CHECK_MSG(!unsupported_kind, "BandJoin does not support this join kind");
  // PairPasses evaluates `predicate_` against the combined left+right row for
  // every join kind (not just kInner). A hand-built join that omits the
  // residual schema must get the same fallback as every other kind; leaving
  // it empty makes column references in the predicate unresolvable for
  // semi/anti joins.
  if (residual_schema_.ColumnCount() == 0 && output_schema_.ColumnCount() > 0) {
    residual_schema_ = output_schema_;
  }
}

StatusOr<std::optional<int64_t>> BandJoin::ReadKey(const Row& row,
                                                   slot_t column) {
  if (column >= row.values_.size()) {
    return StatusError(StatusCode::kCorrupt,
                       "BandJoin key column is outside the row");
  }
  const Value& value = row[column];
  if (value.IsNull()) {
    return std::optional<int64_t>{};
  }
  if (value.type != ValueType::kInt64 && value.type != ValueType::kDate) {
    return StatusError(StatusCode::kInvalidArgument,
                       "BandJoin supports only INT64 and DATE keys");
  }
  if (value.IsUnsigned()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "BandJoin does not support unsigned keys");
  }
  return value.value.int_value;
}

StatusOr<bool> BandJoin::PairPasses(const Row& left, const Row& right) const {
  if (!predicate_) {
    return true;
  }
  pair_scratch_.values_.clear();
  pair_scratch_.values_.reserve(left.values_.size() + right.values_.size());
  pair_scratch_.values_.insert(pair_scratch_.values_.end(),
                               left.values_.begin(), left.values_.end());
  pair_scratch_.values_.insert(pair_scratch_.values_.end(),
                               right.values_.begin(), right.values_.end());
  const Row& combined = pair_scratch_;
  if (kind_ == JoinKind::kInner) {
    return relational_detail::EvaluateConjunctsTolerant(
        predicate_conjuncts_, [&](const Expression& conjunct) {
          return conjunct->TryEvaluate(combined, residual_schema_);
        });
  }
  StatusOr<Value> result = predicate_->TryEvaluate(combined, residual_schema_);
  if (!result.HasValue()) {
    return result.GetStatus();
  }
  return !result.Value().IsNull() && result.Value().Truthy();
}

BandJoin::PostingBandSpec BandJoin::CompilePostingBandSpec(
    std::optional<ValueType> key_type) const {
  PostingBandSpec spec;
  if (residual_schema_.ColumnCount() < left_width_ + right_width_ ||
      left_columns_.empty() || right_columns_.empty()) {
    return spec;
  }
  const slot_t left_slot = left_columns_.front();
  const auto right_slot =
      static_cast<slot_t>(left_width_ + right_columns_.front());
  if (left_slot >= residual_schema_.ColumnCount() ||
      right_slot >= residual_schema_.ColumnCount()) {
    return spec;
  }

  auto resolve_col = [&](const Expression& e) -> std::optional<slot_t> {
    if (!e || e->Type() != TypeTag::kColumnValue) {
      return std::nullopt;
    }
    const auto off = relational_detail::LocalColumnOffset(
        residual_schema_, e->AsColumnValue().GetColumnName());
    if (!off.has_value()) {
      return std::nullopt;
    }
    return static_cast<slot_t>(*off);
  };

  auto extract_shifted_left = [&](const Expression& e,
                                  int64_t* offset) -> bool {
    if (const auto col = resolve_col(e); col.has_value() && *col == left_slot) {
      *offset = 0;
      return true;
    }
    // Non-zero offsets use INT64 arithmetic; keep DATE offsets restricted to 0.
    if (key_type.has_value() && *key_type != ValueType::kInt64) {
      return false;
    }
    if (!e || e->Type() != TypeTag::kBinaryExp) {
      return false;
    }
    const auto& bin = e->AsBinaryExpression();
    if (bin.Op() != BinaryOperation::kAdd &&
        bin.Op() != BinaryOperation::kSubtract) {
      return false;
    }
    const auto lcol = resolve_col(bin.Left());
    if (!lcol.has_value() || *lcol != left_slot || !bin.Right() ||
        bin.Right()->Type() != TypeTag::kConstantValue) {
      return false;
    }
    const Value& c = bin.Right()->AsConstantValue().GetValue();
    if (c.IsNull() || c.type != ValueType::kInt64 || c.IsUnsigned()) {
      return false;
    }
    if (bin.Op() == BinaryOperation::kAdd) {
      *offset = c.value.int_value;
      return true;
    }
    if (c.value.int_value == std::numeric_limits<int64_t>::min()) {
      return false;
    }
    *offset = -c.value.int_value;
    return true;
  };

  auto tighten_lower = [&](int64_t offset, bool inclusive) {
    if (!spec.has_lower || offset > spec.lower_offset ||
        (offset == spec.lower_offset && !inclusive)) {
      spec.has_lower = true;
      spec.lower_offset = offset;
      spec.lower_inclusive = inclusive;
    }
  };
  auto tighten_upper = [&](int64_t offset, bool inclusive) {
    if (!spec.has_upper || offset < spec.upper_offset ||
        (offset == spec.upper_offset && !inclusive)) {
      spec.has_upper = true;
      spec.upper_offset = offset;
      spec.upper_inclusive = inclusive;
    }
  };

  bool all_covered = !predicate_conjuncts_.empty();
  bool any_nonzero_offset = false;
  for (const Expression& conjunct : predicate_conjuncts_) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
      all_covered = false;
      continue;
    }
    const auto& bin = conjunct->AsBinaryExpression();
    if (!IsOrderedComparison(bin.Op())) {
      all_covered = false;
      continue;
    }
    int64_t offset = 0;
    const auto lcol = resolve_col(bin.Left());
    const auto rcol = resolve_col(bin.Right());
    if (rcol.has_value() && *rcol == right_slot &&
        extract_shifted_left(bin.Left(), &offset)) {
      // (left + offset) <op> right
      any_nonzero_offset = any_nonzero_offset || (offset != 0);
      switch (bin.Op()) {
        case BinaryOperation::kLessThan:
          tighten_lower(offset, false);
          break;
        case BinaryOperation::kLessThanEquals:
          tighten_lower(offset, true);
          break;
        case BinaryOperation::kGreaterThan:
          tighten_upper(offset, false);
          break;
        case BinaryOperation::kGreaterThanEquals:
          tighten_upper(offset, true);
          break;
        default:
          all_covered = false;
          break;
      }
    } else if (lcol.has_value() && *lcol == right_slot &&
               extract_shifted_left(bin.Right(), &offset)) {
      // right <op> (left + offset)
      any_nonzero_offset = any_nonzero_offset || (offset != 0);
      switch (bin.Op()) {
        case BinaryOperation::kGreaterThan:
          tighten_lower(offset, false);
          break;
        case BinaryOperation::kGreaterThanEquals:
          tighten_lower(offset, true);
          break;
        case BinaryOperation::kLessThan:
          tighten_upper(offset, false);
          break;
        case BinaryOperation::kLessThanEquals:
          tighten_upper(offset, true);
          break;
        default:
          all_covered = false;
          break;
      }
    } else {
      all_covered = false;
    }
  }
  // If any conjunct used `left + offset` with non-zero offset, ensure no left
  // key can overflow int64_t when adding offset (otherwise SQL raises overflow).
  if (all_covered && any_nonzero_offset) {
    constexpr auto kMinI64 =
        static_cast<__int128>(std::numeric_limits<int64_t>::min());
    constexpr auto kMaxI64 =
        static_cast<__int128>(std::numeric_limits<int64_t>::max());
    for (const auto& left_item : left_rows_) {
      const Value& v = left_item.first[left_slot];
      if (v.IsNull() || v.type != ValueType::kInt64) {
        continue;
      }
      const auto lv = static_cast<__int128>(v.value.int_value);
      if ((spec.has_lower &&
           (lv + spec.lower_offset < kMinI64 ||
            lv + spec.lower_offset > kMaxI64)) ||
          (spec.has_upper &&
           (lv + spec.upper_offset < kMinI64 ||
            lv + spec.upper_offset > kMaxI64))) {
        all_covered = false;
        break;
      }
    }
  }
  spec.covers_all_conjuncts = all_covered;
  return spec;
}

std::pair<size_t, size_t> BandJoin::CandidateRange(
    int64_t value, const PostingBandSpec& spec) const {
  if (right_keys_.empty()) {
    return {0, 0};
  }
  // Lean proof:
  //   lean/TinylambOpt/ResearchProofs/StorageExecution/PostingJoinsA.lean
  //     (Following.OrderedProbe, Following.PruneExtent)
  if (spec.has_lower || spec.has_upper) {
    const auto lv = static_cast<__int128>(value);
    const auto r_min = static_cast<__int128>(right_keys_.front().value);
    const auto r_max = static_cast<__int128>(right_keys_.back().value);
    size_t begin = 0;
    size_t end = right_keys_.size();
    if (spec.has_lower) {
      const __int128 lo = lv + static_cast<__int128>(spec.lower_offset);
      if (spec.lower_inclusive ? (r_max < lo) : (r_max <= lo)) {
        return {0, 0};
      }
      if (spec.lower_inclusive ? (r_min < lo) : (r_min <= lo)) {
        const auto it = std::lower_bound(
            right_keys_.begin(), right_keys_.end(), lo,
            [inclusive = spec.lower_inclusive](const RightKey& key,
                                               __int128 bound) {
              const auto kv = static_cast<__int128>(key.value);
              return inclusive ? (kv < bound) : (kv <= bound);
            });
        begin = static_cast<size_t>(it - right_keys_.begin());
      }
    }
    if (spec.has_upper) {
      const __int128 hi = lv + static_cast<__int128>(spec.upper_offset);
      if (spec.upper_inclusive ? (r_min > hi) : (r_min >= hi)) {
        return {0, 0};
      }
      if (spec.upper_inclusive ? (r_max > hi) : (r_max >= hi)) {
        const auto it = std::lower_bound(
            right_keys_.begin() + static_cast<std::ptrdiff_t>(begin),
            right_keys_.end(), hi,
            [inclusive = spec.upper_inclusive](const RightKey& key,
                                               __int128 bound) {
              const auto kv = static_cast<__int128>(key.value);
              return inclusive ? (kv <= bound) : (kv < bound);
            });
        end = static_cast<size_t>(it - right_keys_.begin());
      }
    }
    return begin < end ? std::pair<size_t, size_t>{begin, end}
                       : std::pair<size_t, size_t>{0, 0};
  }
  const auto lower = std::lower_bound(
      right_keys_.begin(), right_keys_.end(), value,
      [](const RightKey& key, int64_t bound) { return key.value < bound; });
  const auto upper = std::upper_bound(
      right_keys_.begin(), right_keys_.end(), value,
      [](int64_t bound, const RightKey& key) { return bound < key.value; });
  const size_t lower_index = static_cast<size_t>(lower - right_keys_.begin());
  const size_t upper_index = static_cast<size_t>(upper - right_keys_.begin());
  switch (comparison_) {
    case BinaryOperation::kLessThan:
      return {upper_index, right_keys_.size()};
    case BinaryOperation::kLessThanEquals:
      return {lower_index, right_keys_.size()};
    case BinaryOperation::kGreaterThan:
      return {0, lower_index};
    case BinaryOperation::kGreaterThanEquals:
      return {0, upper_index};
    default:
      return {0, 0};
  }
}

Status BandJoin::Materialize() {
  left_rows_.clear();
  right_rows_.clear();
  right_keys_.clear();
  output_.clear();
  output_offset_ = 0;

  auto append = [&](std::vector<PositionedRow>& target, Row row,
                    RowPosition pos, size_t metadata = 0) -> Status {
    if (!materialized_charge_.TryAdd(EstimateRowBytesFast(row) +
                                     sizeof(RowPosition) + metadata)) {
      return StatusError(
          StatusCode::kMemoryLimitExceeded,
          "BandJoin materialization exceeds query memory budget");
    }
    target.emplace_back(std::move(row), pos);
    return Status::kSuccess;
  };
  DataChunk batch;
  while (left_->NextBatch(&batch) != 0) {
    if (const Status st = Transaction::CheckThreadStatementTimeout();
        st != Status::kSuccess) {
      FailWith(st);
      return st;
    }
    for (size_t i = 0; i < batch.Size(); ++i) {
      RETURN_IF_FAIL(append(left_rows_, batch.RowAt(i), batch.PositionAt(i)));
    }
  }
  if (left_->GetStatus() != Status::kSuccess) {
    return left_->GetStatus();
  }
  while (right_->NextBatch(&batch) != 0) {
    if (const Status st = Transaction::CheckThreadStatementTimeout();
        st != Status::kSuccess) {
      FailWith(st);
      return st;
    }
    for (size_t i = 0; i < batch.Size(); ++i) {
      // EstimateRowBytesFast(row) already includes the join key's Value. The
      // metadata pays for the *separate* right_keys_ entry (key copy plus row
      // index) and the one-byte right_matched flag; those allocations live in
      // their own vectors and are not otherwise charged. Dropping this charge
      // would undercount them, not remove a duplicate.
      RETURN_IF_FAIL(append(right_rows_, batch.RowAt(i), batch.PositionAt(i),
                            sizeof(RightKey) + 1));
    }
  }
  if (right_->GetStatus() != Status::kSuccess) {
    return right_->GetStatus();
  }

  if (left_width_ == 0 && !left_rows_.empty()) {
    left_width_ = left_rows_.front().first.values_.size();
  }
  if (right_width_ == 0 && !right_rows_.empty()) {
    right_width_ = right_rows_.front().first.values_.size();
  }
  // When one side is empty its row width cannot be inferred from data. Outer
  // join output_schema still describes both sides, so recover the missing
  // width before constructing the NULL-padded row.
  if (IsOuter(kind_) && output_schema_.ColumnCount() > 0) {
    const size_t output_width = output_schema_.ColumnCount();
    if (left_width_ == 0 && right_width_ <= output_width) {
      left_width_ = output_width - right_width_;
    } else if (right_width_ == 0 && left_width_ <= output_width) {
      right_width_ = output_width - left_width_;
    }
  }

  std::optional<ValueType> key_type;
  const auto validate_key_type = [&](const Row& key_row,
                                     slot_t column) -> Status {
    if (column >= key_row.values_.size()) {
      return StatusError(StatusCode::kCorrupt,
                         "BandJoin key column is outside the row");
    }
    const Value& value = key_row[column];
    if (value.IsNull()) {
      return Status::kSuccess;
    }
    if ((value.type != ValueType::kInt64 && value.type != ValueType::kDate) ||
        value.IsUnsigned()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "BandJoin supports only signed INT64 and DATE keys");
    }
    if (!key_type.has_value()) {
      key_type = value.type;
    } else if (*key_type != value.type) {
      return StatusError(StatusCode::kInvalidArgument,
                         "BandJoin key columns must have the same type");
    }
    return Status::kSuccess;
  };

  int64_t previous_key = 0;
  bool have_previous_key = false;
  for (size_t i = 0; i < right_rows_.size(); ++i) {
    Status type_status =
        validate_key_type(right_rows_[i].first, right_columns_.front());
    if (type_status != Status::kSuccess) {
      return type_status;
    }
    StatusOr<std::optional<int64_t>> key =
        ReadKey(right_rows_[i].first, right_columns_.front());
    if (!key.HasValue()) {
      return key.GetStatus();
    }
    const auto& maybe_key = key.Value();
    if (!maybe_key.has_value()) {
      continue;
    }
    const int64_t key_value = maybe_key.value();
    if (have_previous_key && key_value < previous_key) {
      return StatusError(StatusCode::kInvalidArgument,
                         "BandJoin right input is not ordered");
    }
    previous_key = key_value;
    have_previous_key = true;
    right_keys_.push_back(RightKey{.value = key_value, .row_index = i});
  }

  std::vector<char> right_matched(right_rows_.size(), 0);
  const Row null_left{std::vector<Value>(left_width_)};
  const Row null_right{std::vector<Value>(right_width_)};
  const auto emit_combined = [&](const PositionedRow& left_item,
                                 const PositionedRow& right_item) {
    return append(output_, left_item.first + right_item.first,
                  left_item.second);
  };
  const auto emit_left_only = [&](const PositionedRow& left_item) {
    return append(output_, left_item.first, left_item.second);
  };
  const auto emit_left_padded = [&](const PositionedRow& left_item) {
    return append(output_, left_item.first + null_right, left_item.second);
  };
  const auto emit_right_padded = [&](const PositionedRow& right_item) {
    return append(output_, null_left + right_item.first, right_item.second);
  };

  const PostingBandSpec posting_spec = CompilePostingBandSpec(key_type);
  have_previous_key = false;
  for (const auto& left_item : left_rows_) {
    Status type_status =
        validate_key_type(left_item.first, left_columns_.front());
    if (type_status != Status::kSuccess) {
      return type_status;
    }
    StatusOr<std::optional<int64_t>> key =
        ReadKey(left_item.first, left_columns_.front());
    if (!key.HasValue()) {
      return key.GetStatus();
    }
    const auto& maybe_key = key.Value();
    if (maybe_key.has_value()) {
      const int64_t key_value = maybe_key.value();
      if (have_previous_key && key_value < previous_key) {
        return StatusError(StatusCode::kInvalidArgument,
                           "BandJoin left input is not ordered");
      }
      previous_key = key_value;
      have_previous_key = true;
    }

    bool matched = false;
    size_t match_count = 0;
    if (maybe_key.has_value()) {
      const auto [begin, end] = CandidateRange(maybe_key.value(), posting_spec);
      // Lean proof:
      //   lean/TinylambOpt/ResearchProofs/StorageExecution/PostingJoinsA.lean
      //     (Following.OrderedProbe, Following.OffsetCount)
      if (posting_spec.covers_all_conjuncts &&
          (kind_ == JoinKind::kSemi || kind_ == JoinKind::kAnti)) {
        matched = begin < end;
      } else {
        for (size_t right_index = begin; right_index < end; ++right_index) {
          const size_t row_index = right_keys_[right_index].row_index;
          if (!posting_spec.covers_all_conjuncts) {
            StatusOr<bool> passes =
                PairPasses(left_item.first, right_rows_[row_index].first);
            if (!passes.HasValue()) {
              return passes.GetStatus();
            }
            if (!passes.Value()) {
              continue;
            }
          }
          matched = true;
          ++match_count;
          right_matched[row_index] = 1;
          if (kind_ == JoinKind::kInner || IsOuter(kind_)) {
            RETURN_IF_FAIL(emit_combined(left_item, right_rows_[row_index]));
          } else if (kind_ == JoinKind::kSingle) {
            if (match_count > 1) {
              return StatusError(StatusCode::kInvalidArgument,
                                 "BandJoin single join matched more than once");
            }
            RETURN_IF_FAIL(emit_combined(left_item, right_rows_[row_index]));
          }
        }
      }
    }

    const bool emits_left_only = (kind_ == JoinKind::kSemi && matched) ||
                                 (kind_ == JoinKind::kAnti && !matched);
    const bool emits_left_padded = !matched && (kind_ == JoinKind::kLeftOuter ||
                                                kind_ == JoinKind::kFullOuter ||
                                                kind_ == JoinKind::kSingle);
    if (emits_left_only) {
      RETURN_IF_FAIL(emit_left_only(left_item));
    } else if (emits_left_padded) {
      RETURN_IF_FAIL(emit_left_padded(left_item));
    }
  }

  if (kind_ == JoinKind::kRightOuter || kind_ == JoinKind::kFullOuter) {
    for (size_t i = 0; i < right_rows_.size(); ++i) {
      if (right_matched[i] == 0) {
        RETURN_IF_FAIL(emit_right_padded(right_rows_[i]));
      }
    }
  }
  return Status::kSuccess;
}

void BandJoin::EnsureMaterialized() {
  if (materialized_) {
    return;
  }
  materialized_ = true;
  materialize_error_ = Materialize();
  if (!materialize_error_.ok()) {
    decltype(left_rows_){}.swap(left_rows_);
    decltype(right_rows_){}.swap(right_rows_);
    decltype(right_keys_){}.swap(right_keys_);
    decltype(output_){}.swap(output_);
    materialized_charge_.ReleaseAll();
  }
}

size_t BandJoin::NextBatch(DataChunk* destination, size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  if (output_schema_.ColumnCount() == 0) {
    destination->Reset();
    if (max_rows > kDefaultVectorSize) {
      destination->Reserve(max_rows);
    }
  } else {
    destination->Reset(output_schema_, max_rows);
  }
  EnsureMaterialized();
  if (materialize_error_ != Status::kSuccess) {
    FailWith(materialize_error_);
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

void BandJoin::Dump(std::ostream& o, int /*indent*/) const {
  o << "BandJoin (comparison=" << ToString(comparison_)
    << ", kind=" << static_cast<int>(kind_) << ")";
}

}  // namespace tinylamb
