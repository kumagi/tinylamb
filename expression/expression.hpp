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


#ifndef TINYLAMB_EXPRESSION_HPP
#define TINYLAMB_EXPRESSION_HPP

#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_set>

#include "type/column_name.hpp"
#include "type/value.hpp"

namespace tinylamb {
class Schema;
struct Row;
class ColumnValue;
class BinaryExpression;
class ConstantValue;

enum class TypeTag : int {
  kBinaryExp,
  kColumnValue,
  kConstantValue,
};

class ExpressionBase {
 public:
  ExpressionBase() = default;
  virtual ~ExpressionBase() = default;
  ExpressionBase(const ExpressionBase&) = delete;
  ExpressionBase(ExpressionBase&&) = delete;
  ExpressionBase& operator=(const ExpressionBase&) = delete;
  ExpressionBase& operator=(ExpressionBase&&) = delete;
  [[nodiscard]] virtual TypeTag Type() const = 0;
  [[nodiscard]] const ColumnValue& AsColumnValue() const;
  [[nodiscard]] ColumnValue& AsColumnValue();
  [[nodiscard]] const BinaryExpression& AsBinaryExpression() const;
  [[nodiscard]] const ConstantValue& AsConstantValue() const;
  [[nodiscard]] std::unordered_set<ColumnName> TouchedColumns() const;
  [[nodiscard]] virtual Value Evaluate(const Row& row,
                                       const Schema& schema) const = 0;
  [[nodiscard]] virtual std::string ToString() const = 0;
  virtual void Dump(std::ostream& o) const = 0;
  friend std::ostream& operator<<(std::ostream& o, const ExpressionBase& e) {
    e.Dump(o);
    return o;
  }
};

typedef std::shared_ptr<ExpressionBase> Expression;
Expression ColumnValueExp(const ColumnName& col_name);
Expression ColumnValueExp(const std::string_view& col_name);
Expression ConstantValueExp(const Value& v);
Expression BinaryExpressionExp(Expression left, BinaryOperation op,
                               Expression right);

}  // namespace tinylamb

#endif  // TINYLAMB_EXPRESSION_HPP
