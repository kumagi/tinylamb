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

#ifndef TINYLAMB_COLUMN_VALUE_HPP
#define TINYLAMB_COLUMN_VALUE_HPP

#include <utility>

#include "expression/expression.hpp"
#include "type/column_name.hpp"

namespace tinylamb {

class ColumnValue : public ExpressionBase {
 public:
  explicit ColumnValue(ColumnName col_name) : col_name_(std::move(col_name)) {}
  ColumnValue(const ColumnValue&) = delete;
  ColumnValue(ColumnValue&&) = delete;
  ColumnValue& operator=(const ColumnValue&) = delete;
  ColumnValue& operator=(ColumnValue&&) = delete;
  ~ColumnValue() override = default;
  [[nodiscard]] TypeTag Type() const override { return TypeTag::kColumnValue; }
  [[nodiscard]] Value Evaluate(const Row& row,
                               const Schema& schema) const override;
  void Dump(std::ostream& o) const override;
  [[nodiscard]] const ColumnName& GetColumnName() const { return col_name_; }
  void SetSchemaName(std::string_view s) { col_name_.schema = s; }
  [[nodiscard]] std::string GetName() const { return col_name_.name; }
  [[nodiscard]] std::string ToString() const override {
    return col_name_.ToString();
  }

 private:
  friend class ProjectionPlan;

  struct ColumnName col_name_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_COLUMN_VALUE_HPP
