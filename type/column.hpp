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

#ifndef TINYLAMB_COLUMN_HPP
#define TINYLAMB_COLUMN_HPP

#include <memory>
#include <string_view>
#include <vector>

#include "page/row_position.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "value_type.hpp"

namespace tinylamb {
class Encoder;
class Decoder;

class Column {
 public:
  Column() = default;
  explicit Column(ColumnName name, ValueType type = ValueType::kNull,
                  Constraint cst = Constraint());

  explicit Column(std::string_view name, ValueType type = ValueType::kNull,
                  Constraint cst = Constraint());

  [[nodiscard]] const ColumnName& Name() const { return col_name_; }
  ColumnName& Name() { return col_name_; }
  [[nodiscard]] ValueType Type() const { return type_; }
  [[nodiscard]] Constraint GetConstraint() const { return constraint_; }

  bool operator==(const Column& rhs) const = default;
  friend std::ostream& operator<<(std::ostream& o, const Column& c);
  friend Encoder& operator<<(Encoder& a, const Column& c);
  friend Decoder& operator>>(Decoder& e, Column& c);

 private:
  ColumnName col_name_;
  ValueType type_{};
  Constraint constraint_{Constraint::kNothing};
};

}  // namespace tinylamb

template <>
struct std::hash<tinylamb::Column> {
 public:
  uint64_t operator()(const tinylamb::Column& c) const noexcept;
};  // namespace std

#endif  // TINYLAMB_COLUMN_HPP
