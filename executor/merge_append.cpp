/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "merge_append.hpp"

#include <ostream>
#include <stdexcept>
#include <string>

#include "common/constants.hpp"
#include "expression/column_value.hpp"
#include "type/column_name.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

Value CoerceTo(const Value& value, ValueType type) {
  if (value.IsNull() || value.type == type) {
    return value;
  }
  if (type == ValueType::kDouble && value.type == ValueType::kInt64) {
    return Value(static_cast<double>(value.value.int_value));
  }
  throw std::invalid_argument("merge append key types are incompatible");
}

}  // namespace

Value MergeAppendExecutor::KeyValue(const Head& head,
                                    const SortExecutor::Key& key) const {
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
        throw std::runtime_error("merge append ordering column not found");
      }
      value = head.row.values_[static_cast<size_t>(output_offset)];
    }
  } else {
    value = key.expression->Evaluate(head.row, schemas_[head.source]);
  }
  const int output_offset =
      key.expression->Type() == TypeTag::kColumnValue
          ? output_schema_.Offset(
                key.expression->AsColumnValue().GetColumnName())
          : -1;
  if (output_offset >= 0 &&
      static_cast<size_t>(output_offset) < output_schema_.ColumnCount()) {
    value = CoerceTo(
        value,
        output_schema_.GetColumn(static_cast<size_t>(output_offset)).Type());
  }
  return value;
}

bool MergeAppendExecutor::Before(const Head& left, const Head& right) const {
  for (const SortExecutor::Key& key : keys_) {
    const Value lhs = KeyValue(left, key);
    const Value rhs = KeyValue(right, key);
    if (lhs.IsNull() || rhs.IsNull()) {
      if (lhs.IsNull() != rhs.IsNull()) {
        const bool nulls_first = key.nulls_first.value_or(key.ascending);
        return lhs.IsNull() == nulls_first;
      }
    } else if (lhs != rhs) {
      return key.ascending ? lhs < rhs : rhs < lhs;
    }
  }
  return left.source < right.source;
}

void MergeAppendExecutor::Initialize() {
  if (initialized_) {
    return;
  }
  if (sources_.size() != schemas_.size()) {
    throw std::invalid_argument("merge append source/schema count mismatch");
  }
  for (size_t source = 0; source < sources_.size(); ++source) {
    Head head;
    head.source = source;
    if (sources_[source]->Next(&head.row, &head.position)) {
      heads_.push_back(std::move(head));
    }
  }
  initialized_ = true;
}

bool MergeAppendExecutor::Next(Row* destination, RowPosition* position) {
  Initialize();
  if (heads_.empty()) {
    return false;
  }
  size_t best = 0;
  for (size_t index = 1; index < heads_.size(); ++index) {
    if (Before(heads_[index], heads_[best])) {
      best = index;
    }
  }
  Head emitted = std::move(heads_[best]);
  if (sources_[emitted.source]->Next(&heads_[best].row,
                                     &heads_[best].position)) {
    heads_[best].source = emitted.source;
  } else {
    heads_.erase(heads_.begin() + static_cast<std::ptrdiff_t>(best));
  }
  if (emitted.row.values_.size() != output_schema_.ColumnCount()) {
    throw std::invalid_argument("merge append row/schema width mismatch");
  }
  for (size_t column = 0; column < emitted.row.values_.size(); ++column) {
    emitted.row.values_[column] = CoerceTo(
        emitted.row.values_[column], output_schema_.GetColumn(column).Type());
  }
  *destination = std::move(emitted.row);
  if (position != nullptr) {
    *position = emitted.position;
  }
  return true;
}

void MergeAppendExecutor::Dump(std::ostream& output, int indent) const {
  output << Indent(indent) << "MergeAppend: [";
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
