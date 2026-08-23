/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "expression/column_value.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <ostream>
#include <stdexcept>
#include <string_view>
#include <unordered_set>

#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

bool IdentifierEquals(std::string_view left, std::string_view right) {
  return left.size() == right.size() &&
         std::equal(left.begin(), left.end(), right.begin(),
                    [](char lhs, char rhs) {
                      return std::tolower(static_cast<unsigned char>(lhs)) ==
                             std::tolower(static_cast<unsigned char>(rhs));
                    });
}

int ResolveOffset(const Schema& schema, const ColumnName& name) {
  const int exact = schema.Offset(name);
  if (exact >= 0) { return exact;
}

  // SQL identifiers are case-insensitive unless quoted. ColumnName currently
  // does not retain quotedness, so use case-insensitive lookup as the fallback.
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const ColumnName& candidate = schema.GetColumn(i).Name();
    if (!IdentifierEquals(candidate.name, name.name)) { continue;
}
    if (name.schema.empty() ||
        IdentifierEquals(candidate.schema, name.schema) ||
        IdentifierEquals(schema.Name(), name.schema)) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

Type ColumnType(const Column& column) {
  switch (column.Type()) {
    case ValueType::kInt64:
      return {TypeTag::kBigInt};
    case ValueType::kDouble:
      return {TypeTag::kDouble};
    case ValueType::kVarChar:
      return {TypeTag::kVarChar};
    case ValueType::kDate:
      return {TypeTag::kDate};
    case ValueType::kNull:
      return {TypeTag::kInvalid};
  }
  return {TypeTag::kInvalid};
}

}  // namespace

std::unordered_set<ColumnName> ColumnValue::TouchedColumns() const {
  return {col_name_};
}

Value ColumnValue::Evaluate(const Row& row, const Schema& schema) const {
  const int offset = ResolveOffset(schema, col_name_);
  if (offset >= 0) { return row[static_cast<size_t>(offset)];
}
  throw std::runtime_error("column " + col_name_.ToString() + " not found");
}

Value ColumnValue::Evaluate(const Row* left, const Schema& left_schema,
                            const Row* right,
                            const Schema& right_schema) const {
  const int left_offset = ResolveOffset(left_schema, col_name_);
  if (left != nullptr && left_offset >= 0) {
    return (*left)[static_cast<size_t>(left_offset)];
  }
  const int right_offset = ResolveOffset(right_schema, col_name_);
  if (right != nullptr && right_offset >= 0) {
    return (*right)[static_cast<size_t>(right_offset)];
  }
  throw std::runtime_error("column " + col_name_.ToString() + " not found");
}

Type ColumnValue::ResultType(const Schema& schema) const {
  const int offset = ResolveOffset(schema, col_name_);
  if (offset < 0) { throw std::runtime_error("column type not found");
}
  return ColumnType(schema.GetColumn(static_cast<size_t>(offset)));
}

Type ColumnValue::ResultType(const Schema& left, const Schema& right) const {
  const int left_offset = ResolveOffset(left, col_name_);
  if (left_offset >= 0) {
    return ColumnType(left.GetColumn(static_cast<size_t>(left_offset)));
  }
  return ResultType(right);
}

void ColumnValue::Dump(std::ostream& o) const { o << col_name_; }

}  // namespace tinylamb
