/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "merge_append.hpp"

#include <cstddef>
#include <ostream>
#include <stdexcept>
#include <utility>

#include "common/constants.hpp"
#include "executor/executor_base.hpp"
#include "executor/sort.hpp"
#include "expression/column_value.hpp"
#include "page/row_position.hpp"
#include "type/collation.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

StatusOr<Value> CoerceTo(const Value& value, ValueType type) {
  if (value.IsNull() || value.type == type) {
    return value;
  }
  if (type == ValueType::kDouble && value.type == ValueType::kInt64) {
    return Value(static_cast<double>(value.value.int_value));
  }
  return StatusError(StatusCode::kInvalidArgument,
                     "merge append key types are incompatible");
}

}  // namespace

StatusOr<Value> MergeAppendExecutor::KeyValue(
    const Head& head, const SortExecutor::Key& key) const {
  Value value;
  if (key.expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = key.expression->AsColumnValue().GetColumnName();
    const int child_offset = schemas_[head.source].Offset(column);
    if (child_offset >= 0 &&
        static_cast<size_t>(child_offset) < head.row.values_.size()) {
      value = head.row.values_[static_cast<size_t>(child_offset)];
    } else {
      const int output_offset = output_schema_.Offset(column);
      if (output_offset < 0 ||
          static_cast<size_t>(output_offset) >= head.row.values_.size()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "merge append ordering column not found");
      }
      value = head.row.values_[static_cast<size_t>(output_offset)];
    }
  } else {
    ASSIGN_OR_RETURN(
        Value, computed,
        (key.expression->TryEvaluate(head.row, schemas_[head.source])));
    value = std::move(computed);
  }
  const int output_offset =
      key.expression->Type() == TypeTag::kColumnValue
          ? output_schema_.Offset(
                key.expression->AsColumnValue().GetColumnName())
          : -1;
  if (output_offset >= 0 &&
      std::cmp_less(output_offset, output_schema_.ColumnCount())) {
    ASSIGN_OR_RETURN(
        Value, coerced,
        (CoerceTo(value,
                  output_schema_.GetColumn(static_cast<size_t>(output_offset))
                      .Type())));
    value = std::move(coerced);
  }
  // Same collation contract as the sort operators: a case-insensitive key
  // folds its text once, before the merge compares it.
  return ApplyKeyCollation(value, key.collation);
}

bool MergeAppendExecutor::Before(const Head& left, const Head& right) const {
  for (const SortExecutor::Key& key : keys_) {
    StatusOr<Value> lhs = KeyValue(left, key);
    if (!lhs.HasValue()) {
      FailWith(lhs.GetStatus());
      return false;
    }
    StatusOr<Value> rhs = KeyValue(right, key);
    if (!rhs.HasValue()) {
      FailWith(rhs.GetStatus());
      return false;
    }
    if (lhs.Value().IsNull() || rhs.Value().IsNull()) {
      if (lhs.Value().IsNull() != rhs.Value().IsNull()) {
        const bool nulls_first = key.nulls_first.value_or(key.ascending);
        return lhs.Value().IsNull() == nulls_first;
      }
    } else if (lhs.Value() != rhs.Value()) {
      return key.ascending ? lhs.Value() < rhs.Value()
                           : rhs.Value() < lhs.Value();
    }
  }
  return left.source < right.source;
}

bool MergeAppendExecutor::PullNextFromSource(size_t source, Row* row,
                                             RowPosition* position) {
  DataChunk& batch = source_batches_[source];
  size_t& offset = source_offsets_[source];
  if (offset >= batch.Size()) {
    offset = 0;
    if (sources_[source]->NextBatch(&batch) == 0) {
      return false;
    }
  }
  *row = batch.RowAt(offset);
  *position = batch.PositionAt(offset);
  ++offset;
  return true;
}

Status MergeAppendExecutor::Initialize() {
  if (initialized_) {
    return Status::kSuccess;
  }
  if (sources_.size() != schemas_.size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "merge append source/schema count mismatch");
  }
  source_batches_.resize(sources_.size());
  source_offsets_.assign(sources_.size(), 0);
  for (size_t source = 0; source < sources_.size(); ++source) {
    Head head;
    head.source = source;
    if (PullNextFromSource(source, &head.row, &head.position)) {
      heads_.push_back(std::move(head));
    } else if (sources_[source]->GetStatus() != Status::kSuccess) {
      return sources_[source]->GetStatus();
    }
  }
  initialized_ = true;
  return Status::kSuccess;
}

size_t MergeAppendExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset(output_schema_, max_rows);
  if (Status st = Initialize(); st != Status::kSuccess) {
    FailWith(st);
    return 0;
  }
  if (heads_.empty()) {
    if (!sources_.empty()) {
      FailWithChildOf(*sources_.front());
    }
    return 0;
  }
  while (destination->Size() < max_rows && !heads_.empty()) {
    size_t best = 0;
    for (size_t index = 1; index < heads_.size(); ++index) {
      if (Before(heads_[index], heads_[best])) {
        best = index;
      }
    }
    if (!ok()) {
      return destination->Size();
    }
    Head emitted = std::move(heads_[best]);
    if (PullNextFromSource(emitted.source, &heads_[best].row,
                           &heads_[best].position)) {
      heads_[best].source = emitted.source;
    } else {
      if (FailWithChildOf(*sources_[emitted.source])) {
        return destination->Size();
      }
      heads_.erase(heads_.begin() + static_cast<std::ptrdiff_t>(best));
    }
    if (emitted.row.values_.size() != output_schema_.ColumnCount()) {
      FailWith(StatusError(StatusCode::kInvalidArgument,
                           "merge append row/schema width mismatch"));
      return destination->Size();
    }
    for (size_t column = 0; column < emitted.row.values_.size(); ++column) {
      StatusOr<Value> coerced = CoerceTo(
          emitted.row.values_[column], output_schema_.GetColumn(column).Type());
      if (!coerced.HasValue()) {
        FailWith(coerced.GetStatus());
        return destination->Size();
      }
      emitted.row.values_[column] = coerced.MoveValue();
    }
    destination->Append(std::move(emitted.row), emitted.position);
  }
  return destination->Size();
}

void MergeAppendExecutor::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << "MergeAppend: [";
  for (size_t i = 0; i < keys_.size(); ++i) {
    if (i != 0) {
      output << ", ";
    }
    output << keys_[i].expression->ToString()
           << (keys_[i].ascending ? " ASC" : " DESC");
  }
  output << "]\n";
  for (const Executor& source : sources_) {
    source->Dump(output, indent + 2);
  }
}

}  // namespace tinylamb
