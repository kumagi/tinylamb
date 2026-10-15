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

#ifndef TINYLAMB_TYPE_VALUE_TYPE_HPP
#define TINYLAMB_TYPE_VALUE_TYPE_HPP

#include <cstdint>
#include <functional>
#include <string_view>

namespace tinylamb {

enum class ValueType : uint8_t {
  kNull,
  kInt64,
  kVarChar,
  kDouble,
  kDate,
  kArray,
};

inline std::string_view ValueTypeToString(ValueType type) {
  switch (type) {
    case ValueType::kNull:
      return "(null)";
    case ValueType::kInt64:
      return "Integer";
    case ValueType::kVarChar:
      return "Varchar";
    case ValueType::kDouble:
      return "Double";
    case ValueType::kDate:
      return "Date";
    case ValueType::kArray:
      return "Array";
  }
  return "unknown value type";
}

// SQL type name for a stored value's tag, used to infer an array's declared
// element type from its runtime elements. BOOL folds into kInt64, so a
// predicate-produced element still reports INT64 here — the frontend tags
// those arrays statically. Returns "" for tags with no scalar SQL name.
inline std::string_view ElementSqlTypeName(ValueType type) {
  switch (type) {
    case ValueType::kInt64:
      return "INT64";
    case ValueType::kDouble:
      return "FLOAT64";
    case ValueType::kVarChar:
      return "STRING";
    case ValueType::kDate:
      return "DATE";
    default:
      return {};
  }
}

}  // namespace tinylamb

template <>
class std::hash<tinylamb::ValueType> {
 public:
  uint64_t operator()(const tinylamb::ValueType& c) const {
    return std::hash<uint8_t>()(static_cast<uint8_t>(c));
  }
};

#endif  // TINYLAMB_TYPE_VALUE_TYPE_HPP
