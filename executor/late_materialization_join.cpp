/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/late_materialization_join.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/late_materialization.hpp"
#include "common/status_or.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/executor_base.hpp"
#include "expression/binary_expression.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"
#include "page/row_position.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

constexpr size_t kMaxCandidatesPerLeftRow = 1'000'000;

bool JoinValueEqual(const Value& left, const Value& right) {
  if (left.IsNull() || right.IsNull()) {
    return false;
  }
  const StatusOr<Value> result =
      TryEvaluateBinary(BinaryOperation::kEquals, left, right);
  return result.HasValue() && !result.Value().IsNull() &&
         result.Value().Truthy();
}

bool IsUnmatchable(const Value& value) { return value.IsNull(); }

TransactionManager::IndexReadGuard AcquireReadGuard(
    Transaction& txn, const Index* index,
    TransactionManager::IndexReadGuard guard) {
  if (guard.HoldsLock() || index == nullptr) {
    return guard;
  }
  return txn.AcquireIndexReadGuard(index->Root());
}

}  // namespace

bool LateMaterializationJoin::SupportsKeyType(ValueType type) {
  return type == ValueType::kInt64 || type == ValueType::kDate;
}

LateMaterializationJoin::LateMaterializationJoin(
    Transaction& txn, Executor left, std::vector<slot_t> left_columns,
    const Table& right_table, const Index& right_index,
    std::vector<slot_t> right_columns, Schema output_schema,
    Expression residual, bool unique_winner,
    std::vector<slot_t> right_projection,
    TransactionManager::IndexReadGuard index_read_guard)
    : txn_(txn),
      left_(std::move(left)),
      right_table_(&right_table),
      right_index_(&right_index),
      left_columns_(std::move(left_columns)),
      right_columns_(std::move(right_columns)),
      right_projection_(std::move(right_projection)),
      output_schema_(std::move(output_schema)),
      residual_(std::move(residual)),
      residual_conjuncts_(SplitConjuncts(residual_)),
      identity_(unique_winner ? LateMaterializationIdentity::kUniqueWinner
                              : LateMaterializationIdentity::kEquiKey),
      index_read_guard_(
          AcquireReadGuard(txn_, right_index_, std::move(index_read_guard))),
      unique_winner_(unique_winner) {
  if (!Validate() && initial_status_ == Status::kSuccess) {
    initial_status_ = StatusError(StatusCode::kInvalidArgument,
                                  "invalid late materialization equi-join "
                                  "contract");
  }
}

LateMaterializationJoin::LateMaterializationJoin(
    Transaction& txn, Executor left, const Table& right_table,
    Schema output_schema, LateMaterializationRowIdBinding rowid,
    std::vector<slot_t> right_projection, Expression residual)
    : txn_(txn),
      left_(std::move(left)),
      right_table_(&right_table),
      right_projection_(std::move(right_projection)),
      output_schema_(std::move(output_schema)),
      residual_(std::move(residual)),
      residual_conjuncts_(SplitConjuncts(residual_)),
      identity_(LateMaterializationIdentity::kRowId),
      rowid_(rowid) {
  if (!Validate() && initial_status_ == Status::kSuccess) {
    initial_status_ = StatusError(StatusCode::kInvalidArgument,
                                  "invalid late materialization rowid-join "
                                  "contract");
  }
}

LateMaterializationJoin::~LateMaterializationJoin() = default;

bool LateMaterializationJoin::Validate() {
  const auto reject = [this](std::string message) {
    initial_status_ =
        StatusError(StatusCode::kInvalidArgument, std::move(message));
    return false;
  };
  if (left_ == nullptr || right_table_ == nullptr) {
    return reject("late materialization join requires both inputs");
  }
  if (right_projection_.empty()) {
    right_projection_.resize(right_table_->GetSchema().ColumnCount());
    for (size_t i = 0; i < right_projection_.size(); ++i) {
      right_projection_[i] = static_cast<slot_t>(i);
    }
  }
  for (size_t i = 0; i < right_table_->GetSchema().ColumnCount(); ++i) {
    if (right_table_->GetSchema().GetColumn(i).Type() == ValueType::kArray) {
      return reject("late materialization does not support ARRAY tables");
    }
  }
  for (const slot_t column : right_projection_) {
    if (column >= right_table_->GetSchema().ColumnCount()) {
      return reject("late materialization payload column is outside the table");
    }
  }
  if (identity_ == LateMaterializationIdentity::kRowId) {
    if (rowid_.page_column == rowid_.slot_column) {
      return reject("rowid page and slot columns must be distinct");
    }
    return true;
  }
  if (right_index_ == nullptr || left_columns_.empty() ||
      left_columns_.size() != right_columns_.size()) {
    return reject("equi late materialization requires aligned non-empty keys");
  }
  if (right_columns_.size() > right_index_->sc_.key_.size()) {
    return reject("late materialization probe exceeds the index key");
  }
  if (right_index_->RetainsDeletedEntries()) {
    return reject(
        "late materialization does not support retained index entries");
  }
  for (size_t i = 0; i < right_columns_.size(); ++i) {
    const slot_t column = right_columns_[i];
    if (column >= right_table_->GetSchema().ColumnCount() ||
        column != right_index_->sc_.key_[i]) {
      return reject("late materialization probe is not in index key order");
    }
    const Column& index_column =
        right_table_->GetSchema().GetColumn(right_index_->sc_.key_[i]);
    if (!SupportsKeyType(index_column.Type()) || index_column.IsUnsigned()) {
      return reject(
          "late materialization supports only signed INT64/DATE keys");
    }
  }
  if (unique_winner_ && !right_index_->StoresSingleValue()) {
    unique_winner_ = false;
    identity_ = LateMaterializationIdentity::kEquiKey;
  }
  return true;
}

std::optional<Row> LateMaterializationJoin::ExtractLeftKey(
    const Row& left_row) const {
  if (left_columns_.empty()) {
    return std::nullopt;
  }
  for (const slot_t column : left_columns_) {
    if (column >= left_row.values_.size()) {
      return std::nullopt;
    }
  }
  return left_row.Extract(left_columns_);
}

bool LateMaterializationJoin::IsLeftRowValid(const Row& left_row) const {
  if (!left_row.IsValid()) {
    FailWith(StatusError(StatusCode::kCorrupt,
                         "late materialization received an invalid left row"));
    return false;
  }
  if (identity_ == LateMaterializationIdentity::kRowId) {
    if (rowid_.page_column >= left_row.values_.size() ||
        rowid_.slot_column >= left_row.values_.size()) {
      FailWith(StatusError(StatusCode::kCorrupt,
                           "rowid column is outside the left row"));
      return false;
    }
    return true;
  }
  const bool keys_in_range = std::ranges::all_of(
      left_columns_,
      [&](const slot_t column) { return column < left_row.values_.size(); });
  if (!keys_in_range) {
    FailWith(StatusError(StatusCode::kCorrupt,
                         "equi key column is outside the left row"));
    return false;
  }
  return true;
}

std::optional<RowPosition> LateMaterializationJoin::ExtractRowId(
    const Row& left_row) const {
  if (identity_ != LateMaterializationIdentity::kRowId ||
      rowid_.page_column >= left_row.values_.size() ||
      rowid_.slot_column >= left_row.values_.size()) {
    return std::nullopt;
  }
  const Value& page_value = left_row[rowid_.page_column];
  const Value& slot_value = left_row[rowid_.slot_column];
  if (page_value.IsNull() || slot_value.IsNull()) {
    return std::nullopt;
  }
  if (page_value.type != ValueType::kInt64 ||
      slot_value.type != ValueType::kInt64 || page_value.IsUnsigned() ||
      slot_value.IsUnsigned() || page_value.value.int_value < 0 ||
      slot_value.value.int_value < 0 ||
      std::cmp_greater(slot_value.value.int_value,
                       std::numeric_limits<slot_t>::max())) {
    FailWith(StatusError(StatusCode::kInvalidArgument,
                         "rowid identity is not a valid RowPosition"));
    return std::nullopt;
  }
  return RowPosition(static_cast<page_id_t>(page_value.value.int_value),
                     static_cast<slot_t>(slot_value.value.int_value));
}

bool LateMaterializationJoin::KeyMatches(const Row& left_key,
                                         const Row& index_key) const {
  if (index_key.values_.size() < left_key.values_.size() ||
      left_key.values_.size() != right_columns_.size()) {
    return false;
  }
  for (size_t i = 0; i < left_key.values_.size(); ++i) {
    if (!JoinValueEqual(left_key[i], index_key[i])) {
      return false;
    }
  }
  return true;
}

bool LateMaterializationJoin::LoadEquiCandidates(const Row& left_row) {
  const std::optional<Row> extracted = ExtractLeftKey(left_row);
  if (!extracted.has_value()) {
    return false;
  }
  const Row& left_key = *extracted;
  for (const Value& value : left_key.values_) {
    if (IsUnmatchable(value)) {
      return true;
    }
    if ((value.type != ValueType::kInt64 && value.type != ValueType::kDate) ||
        value.IsUnsigned()) {
      return FailWith(StatusError(
          StatusCode::kInvalidArgument,
          "late materialization supports only signed INT64/DATE keys"));
    }
  }
  right_iterator_ = std::make_unique<IndexScanIterator>(
      *right_table_, *right_index_, txn_, left_key.values_, left_key.values_,
      true, false, &index_read_guard_);
  if (right_iterator_->GetStatus() != Status::kSuccess) {
    return FailWith(right_iterator_->GetStatus());
  }
  while (right_iterator_->IsValid()) {
    const Row index_key = right_iterator_->GetKey();
    if (KeyMatches(left_key, index_key)) {
      if (candidates_.size() >= kMaxCandidatesPerLeftRow) {
        return FailWith(StatusError(
            StatusCode::kNotImplemented,
            "late materialization refuses an unbounded duplicate probe"));
      }
      candidates_.push_back(
          Candidate{.position = right_iterator_->Position(), .key = index_key});
      ++candidate_count_;
      if (unique_winner_ && right_index_->StoresSingleValue() &&
          std::ranges::none_of(left_key.values_, [](const Value& value) {
            return value.IsNull();
          })) {
        break;
      }
    }
    ++*right_iterator_;
    if (!right_iterator_->IsValid() &&
        right_iterator_->GetStatus() != Status::kSuccess) {
      return FailWith(right_iterator_->GetStatus());
    }
  }
  if (!right_iterator_->IsValid() &&
      right_iterator_->GetStatus() != Status::kSuccess) {
    return FailWith(right_iterator_->GetStatus());
  }
  return true;
}

bool LateMaterializationJoin::LoadRowIdCandidate(const Row& left_row) {
  const std::optional<RowPosition> position = ExtractRowId(left_row);
  if (!position.has_value()) {
    return true;
  }
  candidates_.push_back(Candidate{.position = *position, .key = Row()});
  ++candidate_count_;
  return true;
}

bool LateMaterializationJoin::LoadNextIdentity() {
  if (initial_status_ != Status::kSuccess) {
    return FailWith(initial_status_);
  }
  for (;;) {
    if (!left_->Next(&hold_left_, &hold_left_position_)) {
      hold_left_.Clear();
      hold_left_position_ = RowPosition();
      have_left_ = false;
      if (left_->GetStatus() != Status::kSuccess) {
        return FailWith(left_->GetStatus());
      }
      return false;
    }
    if (!IsLeftRowValid(hold_left_)) {
      return false;
    }
    candidates_.clear();
    candidate_offset_ = 0;
    const bool loaded = identity_ == LateMaterializationIdentity::kRowId
                            ? LoadRowIdCandidate(hold_left_)
                            : LoadEquiCandidates(hold_left_);
    if (!loaded || GetStatus() != Status::kSuccess) {
      return false;
    }
    if (!candidates_.empty()) {
      have_left_ = true;
      return true;
    }
  }
}

StatusOr<Row> LateMaterializationJoin::MaterializePayload(
    const Candidate& candidate, const Row& left_row) const {
  StatusOr<Row> full = right_table_->Read(txn_, candidate.position);
  if (!full.HasValue()) {
    return full.GetStatus();
  }
  if (right_index_ != nullptr && right_index_->RetainsDeletedEntries() &&
      !candidate.key.values_.empty() &&
      right_index_->GenerateKey(full.Value()) !=
          candidate.key.EncodeMemcomparableFormat()) {
    return Status::kNotExists;
  }
  if (right_index_ != nullptr) {
    for (size_t i = 0; i < right_columns_.size(); ++i) {
      if (right_columns_[i] >= full.Value().values_.size() ||
          !JoinValueEqual(left_row[left_columns_[i]],
                          full.Value()[right_columns_[i]])) {
        return Status::kNotExists;
      }
    }
  }
  ++payload_fetch_count_;
  if (right_projection_.empty()) {
    return full.MoveValue();
  }
  return full.Value().Extract(right_projection_);
}

bool LateMaterializationJoin::Next(Row* destination, RowPosition* position) {
  if (GetStatus() != Status::kSuccess) {
    return false;
  }
  if (initial_status_ != Status::kSuccess) {
    return FailWith(initial_status_);
  }
  for (;;) {
    if (!have_left_) {
      if (!LoadNextIdentity()) {
        return false;
      }
      continue;
    }
    if (candidate_offset_ < candidates_.size()) {
      const Candidate candidate = std::move(candidates_[candidate_offset_++]);
      StatusOr<Row> payload = MaterializePayload(candidate, hold_left_);
      if (!payload.HasValue()) {
        if (payload.GetStatus() == Status::kNotExists ||
            payload.GetStatus() == Status::kDeleted) {
          continue;
        }
        return FailWith(payload.GetStatus());
      }
      Row combined = hold_left_ + payload.Value();
      if (combined.values_.size() != output_schema_.ColumnCount()) {
        return FailWith(StatusError(
            StatusCode::kCorrupt,
            "late materialization output width does not match its schema"));
      }
      if (!residual_conjuncts_.empty()) {
        const StatusOr<bool> pass =
            relational_detail::EvaluateConjunctsTolerant(
                residual_conjuncts_, [&](const Expression& conjunct) {
                  return conjunct->TryEvaluate(combined, output_schema_);
                });
        if (!pass.HasValue()) {
          return FailWith(pass.GetStatus());
        }
        if (!pass.Value()) {
          continue;
        }
      }
      *destination = std::move(combined);
      if (position != nullptr) {
        *position = hold_left_position_;
      }
      return true;
    }
    have_left_ = false;
    hold_left_.Clear();
    hold_left_position_ = RowPosition();
    candidates_.clear();
  }
}

size_t LateMaterializationJoin::NextBatch(DataChunk* destination,
                                          size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  destination->Reset(output_schema_, max_rows);
  while (destination->Size() < max_rows) {
    Row row;
    RowPosition position;
    if (!Next(&row, &position)) {
      break;
    }
    destination->Append(std::move(row), position);
  }
  return destination->Size();
}

void LateMaterializationJoin::Dump(std::ostream& output, int indent) const {
  output << "LateMaterializationJoin (identity=" << ToString(identity_)
         << ", payload=late, keys="
         << (identity_ == LateMaterializationIdentity::kRowId
                 ? size_t{1}
                 : right_columns_.size())
         << ")";
  if (right_index_ != nullptr) {
    output << " index=" << right_index_->sc_.name_;
  }
  output << "\n" << Indent(static_cast<size_t>(indent) + 2);
  if (left_ != nullptr) {
    left_->Dump(output, indent + 2);
  }
  if (right_table_ != nullptr) {
    output << "\n"
           << Indent(static_cast<size_t>(indent) + 2)
           << "LateMaterializePayload: " << right_table_->GetSchema().Name();
    if (right_index_ != nullptr) {
      output << " via " << right_index_->sc_.name_;
    }
  }
}

}  // namespace tinylamb
