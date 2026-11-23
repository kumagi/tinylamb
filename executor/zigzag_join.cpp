/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/zigzag_join.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "executor/detail/scan_filter.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

slot_t SingleSlot(const std::vector<slot_t>& slots) {
  CHECK_MSG(slots.size() == 1, "ZigZagJoin requires one key per input");
  return slots.front();
}

bool SupportedOperation(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kEquals:
    case BinaryOperation::kLessThan:
    case BinaryOperation::kLessThanEquals:
    case BinaryOperation::kGreaterThan:
    case BinaryOperation::kGreaterThanEquals:
      return true;
    default:
      return false;
  }
}

}  // namespace

ZigZagJoin::ZigZagJoin(Executor left, slot_t left_key, Executor right,
                       slot_t right_key, BinaryOperation operation,
                       Expression predicate, Schema residual_schema,
                       Schema output_schema)
    : left_(std::move(left)),
      right_(std::move(right)),
      left_key_(left_key),
      right_key_(right_key),
      operation_(operation),
      predicate_(std::move(predicate)),
      predicate_conjuncts_(SplitConjuncts(predicate_)),
      residual_schema_(std::move(residual_schema)),
      output_schema_(std::move(output_schema)) {
  CHECK_MSG(left_ != nullptr && right_ != nullptr,
            "ZigZagJoin requires non-null inputs");
  CHECK_MSG(SupportedOperation(operation_),
            "ZigZagJoin requires an ordered comparison");
  CHECK_MSG(predicate_ != nullptr, "ZigZagJoin requires a predicate");
  if (residual_schema_.ColumnCount() == 0 && output_schema_.ColumnCount() > 0) {
    residual_schema_ = output_schema_;
  }
}

ZigZagJoin::ZigZagJoin(Executor left, const std::vector<slot_t>& left_keys,
                       Executor right, const std::vector<slot_t>& right_keys,
                       BinaryOperation operation, Expression predicate,
                       Schema residual_schema, Schema output_schema)
    : ZigZagJoin(std::move(left), SingleSlot(left_keys), std::move(right),
                 SingleSlot(right_keys), operation, std::move(predicate),
                 std::move(residual_schema), std::move(output_schema)) {}

bool ZigZagJoin::LoadLeft() {
  while (true) {
    if (left_batch_pos_ >= left_batch_.Size()) {
      left_batch_.Reset();
      left_batch_pos_ = 0;
      if (left_->NextBatch(&left_batch_) == 0) {
        break;
      }
    }
    PositionedRow candidate{
        .row = left_batch_.RowAt(left_batch_pos_),
        .position = left_batch_.PositionAt(left_batch_pos_)};
    ++left_batch_pos_;
    if (!candidate.row.IsValid()) {
      return FailWith(
          StatusError(StatusCode::kCorrupt,
                      "ZigZagJoin left input returned an invalid row"));
    }
    if (left_key_ >= candidate.row.values_.size()) {
      return FailWith(
          StatusError(StatusCode::kCorrupt,
                      "ZigZagJoin left key column is outside the row"));
    }
    const Value& value = candidate.row[left_key_];
    if (value.IsNull()) {
      // NULL keys never participate in the merge. Ignore them for monotonic
      // validation so either NULLS FIRST or NULLS LAST input is accepted.
      continue;
    }
    if (value.type != ValueType::kInt64 || value.IsUnsigned()) {
      return FailWith(
          StatusError(StatusCode::kInvalidArgument,
                      "ZigZagJoin supports only signed INT64 keys"));
    }
    const int64_t key = value.value.int_value;
    if (left_order_.previous.has_value() && key < *left_order_.previous) {
      return FailWith(StatusError(StatusCode::kInvalidArgument,
                                  "ZigZagJoin left input is not ordered"));
    }
    left_order_.previous = key;
    current_left_ = std::move(candidate);
    left_value_ = key;
    have_left_ = true;
    return true;
  }
  FailWithChildOf(*left_);
  return false;
}

bool ZigZagJoin::LoadRight() {
  if (right_loaded_) {
    return GetStatus() == Status::kSuccess;
  }
  DataChunk batch;
  while (right_->NextBatch(&batch) != 0) {
    if (const Status st = Transaction::CheckThreadStatementTimeout();
        st != Status::kSuccess) {
      FailWith(st);
      return false;
    }
    for (size_t i = 0; i < batch.Size(); ++i) {
      PositionedRow candidate{.row = batch.RowAt(i),
                              .position = batch.PositionAt(i)};
      if (!candidate.row.IsValid()) {
        return FailWith(
            StatusError(StatusCode::kCorrupt,
                        "ZigZagJoin right input returned an invalid row"));
      }
      if (right_key_ >= candidate.row.values_.size()) {
        return FailWith(
            StatusError(StatusCode::kCorrupt,
                        "ZigZagJoin right key column is outside the row"));
      }
      const Value& value = candidate.row[right_key_];
      if (value.IsNull()) {
        // NULL keys are never materialized into the searchable right side.
        // Their placement must not affect ordering of non-NULL keys.
        continue;
      }
      if (value.type != ValueType::kInt64 || value.IsUnsigned()) {
        return FailWith(
            StatusError(StatusCode::kInvalidArgument,
                        "ZigZagJoin supports only signed INT64 keys"));
      }
      const int64_t key = value.value.int_value;
      if (right_order_.previous.has_value() && key < *right_order_.previous) {
        return FailWith(StatusError(StatusCode::kInvalidArgument,
                                    "ZigZagJoin right input is not ordered"));
      }
      right_order_.previous = key;
      // The whole ordered right side stays resident for binary search, so it
      // must be budget-charged like BandJoin's materialization: force-charging
      // with Add() used to let a huge right input grow until the process OOMed
      // instead of failing cleanly at the query memory limit.
      if (!right_charge_.TryAdd(EstimateRowBytes(candidate.row) +
                                sizeof(RowPosition))) {
        return FailWith(StatusError(
            StatusCode::kMemoryLimitExceeded,
            "ZigZagJoin right input exceeds query memory budget"));
      }
      right_rows_.push_back(std::move(candidate));
    }
  }
  if (FailWithChildOf(*right_)) {
    return false;
  }
  right_loaded_ = true;
  return true;
}

void ZigZagJoin::ClearLeft() {
  have_left_ = false;
  current_left_ = PositionedRow();
  left_value_.reset();
  right_end_ = 0;
  right_cursor_ = 0;
}

StatusOr<bool> ZigZagJoin::BuildAndTest(const Row& left, const Row& right,
                                        Row* combined) const {
  combined->values_.clear();
  combined->values_.reserve(left.values_.size() + right.values_.size());
  combined->values_.insert(combined->values_.end(), left.values_.begin(),
                           left.values_.end());
  combined->values_.insert(combined->values_.end(), right.values_.begin(),
                           right.values_.end());
  return relational_detail::EvaluateConjunctsTolerant(
      predicate_conjuncts_, [&](const Expression& conjunct) {
        return conjunct->TryEvaluate(*combined, residual_schema_);
      });
}

std::pair<size_t, size_t> ZigZagJoin::CandidateRange(int64_t left) const {
  const auto lower =
      std::lower_bound(right_rows_.begin(), right_rows_.end(), left,
                       [this](const PositionedRow& row, int64_t value) {
                         return row.row[right_key_].value.int_value < value;
                       });
  const auto upper =
      std::upper_bound(right_rows_.begin(), right_rows_.end(), left,
                       [this](int64_t value, const PositionedRow& row) {
                         return value < row.row[right_key_].value.int_value;
                       });
  const size_t lower_index = static_cast<size_t>(lower - right_rows_.begin());
  const size_t upper_index = static_cast<size_t>(upper - right_rows_.begin());
  switch (operation_) {
    case BinaryOperation::kEquals:
      return {lower_index, upper_index};
    case BinaryOperation::kLessThan:
      // left < right needs strictly greater keys; skip the equal-key run.
      return {upper_index, right_rows_.size()};
    case BinaryOperation::kLessThanEquals:
      return {lower_index, right_rows_.size()};
    case BinaryOperation::kGreaterThan:
      return {0, lower_index};
    case BinaryOperation::kGreaterThanEquals:
      return {0, upper_index};
    default:
      return {0, 0};
  }
}

bool ZigZagJoin::NextRow(Row* destination, RowPosition* position) {
  if (GetStatus() != Status::kSuccess) {
    return false;
  }
  if (!LoadRight()) {
    return false;
  }
  for (;;) {
    if (have_left_ && right_cursor_ < right_end_) {
      const PositionedRow& right = right_rows_[right_cursor_++];
      // The residual predicate already needs a concatenated row; build it
      // straight into the output buffer and move it on a match, rather than
      // building one for the test and a second copy for the result.
      StatusOr<bool> passes =
          BuildAndTest(current_left_.row, right.row, destination);
      if (!passes.HasValue()) {
        return FailWith(passes.GetStatus());
      }
      if (passes.Value()) {
        if (position != nullptr) {
          *position = current_left_.position;
        }
        return true;
      }
      continue;
    }
    ClearLeft();
    if (!LoadLeft()) {
      return false;
    }
    if (!left_value_.has_value()) {
      return FailWith(StatusError(StatusCode::kCorrupt,
                                  "ZigZagJoin left load produced no key"));
    }
    const auto [begin, end] = CandidateRange(*left_value_);
    right_end_ = end;
    right_cursor_ = begin;
  }
}

size_t ZigZagJoin::NextBatch(DataChunk* destination, size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  // An unspecified output schema means the join emits no projected columns;
  // Reset(schema, n) would build a width-0 chunk that downstream width checks
  // reject. Mirror BandJoin's empty-schema path.
  if (output_schema_.ColumnCount() == 0) {
    destination->Reset();
    if (max_rows > kDefaultVectorSize) {
      destination->Reserve(max_rows);
    }
  } else {
    destination->Reset(output_schema_, max_rows);
  }
  Row row;
  while (destination->Size() < max_rows) {
    RowPosition position;
    if (!NextRow(&row, &position)) {
      break;
    }
    destination->Append(std::move(row), position);
  }
  return destination->Size();
}

void ZigZagJoin::Dump(std::ostream& output, int /*indent*/) const {
  output << "ZigZagJoin (comparison=" << tinylamb::ToString(operation_)
         << ", keys=1)";
}

}  // namespace tinylamb
