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
                       Expression predicate, Schema output_schema)
    : left_(std::move(left)),
      right_(std::move(right)),
      left_key_(left_key),
      right_key_(right_key),
      operation_(operation),
      predicate_(std::move(predicate)),
      predicate_conjuncts_(SplitConjuncts(predicate_)),
      output_schema_(std::move(output_schema)) {
  CHECK_MSG(left_ != nullptr && right_ != nullptr,
            "ZigZagJoin requires non-null inputs");
  CHECK_MSG(SupportedOperation(operation_),
            "ZigZagJoin requires an ordered comparison");
  CHECK_MSG(predicate_ != nullptr, "ZigZagJoin requires a predicate");
}

ZigZagJoin::ZigZagJoin(Executor left, const std::vector<slot_t>& left_keys,
                       Executor right, const std::vector<slot_t>& right_keys,
                       BinaryOperation operation, Expression predicate,
                       Schema output_schema)
    : ZigZagJoin(std::move(left), SingleSlot(left_keys), std::move(right),
                 SingleSlot(right_keys), operation, std::move(predicate),
                 std::move(output_schema)) {}

bool ZigZagJoin::LoadLeft() {
  while (true) {
    PositionedRow candidate;
    if (!left_->Next(&candidate.row, &candidate.position)) {
      break;
    }
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
      if (left_order_.previous.has_value()) {
        return FailWith(StatusError(StatusCode::kInvalidArgument,
                                    "ZigZagJoin left input is not ordered"));
      }
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
  while (true) {
    PositionedRow candidate;
    if (!right_->Next(&candidate.row, &candidate.position)) {
      break;
    }
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
      if (right_order_.previous.has_value()) {
        return FailWith(StatusError(StatusCode::kInvalidArgument,
                                    "ZigZagJoin right input is not ordered"));
      }
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
    right_rows_.push_back(std::move(candidate));
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

StatusOr<bool> ZigZagJoin::PairPasses(const Row& left, const Row& right) const {
  const Row combined = left + right;
  return relational_detail::EvaluateConjunctsTolerant(
      predicate_conjuncts_, [&](const Expression& conjunct) {
        return conjunct->TryEvaluate(combined, output_schema_);
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

bool ZigZagJoin::Next(Row* destination, RowPosition* position) {
  if (GetStatus() != Status::kSuccess) {
    return false;
  }
  if (!LoadRight()) {
    return false;
  }
  for (;;) {
    if (have_left_ && right_cursor_ < right_end_) {
      const PositionedRow& right = right_rows_[right_cursor_++];
      StatusOr<bool> passes = PairPasses(current_left_.row, right.row);
      if (!passes.HasValue()) {
        return FailWith(passes.GetStatus());
      }
      if (passes.Value()) {
        *destination = current_left_.row + right.row;
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
  destination->Reset(output_schema_, max_rows);
  Row row;
  while (destination->Size() < max_rows) {
    RowPosition position;
    if (!Next(&row, &position)) {
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
