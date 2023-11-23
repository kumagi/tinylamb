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

#ifndef TINYLAMB_NAMED_EXPRESSION_HPP
#define TINYLAMB_NAMED_EXPRESSION_HPP

#include <string>
#include <utility>

#include "common/log_message.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "type/column_name.hpp"

namespace tinylamb {

struct NamedExpression {
  explicit NamedExpression(ColumnName name_)
      : name(), expression(ColumnValueExp(name_)) {}
  explicit NamedExpression(std::string_view name_)
      : name(), expression(ColumnValueExp(name_)) {}
  NamedExpression(std::string_view name_, ColumnName column_name)
      : name(name_), expression(ColumnValueExp(column_name)) {}
  NamedExpression(std::string_view name_, Expression exp)
      : name(name_), expression(std::move(exp)) {}

  friend std::ostream& operator<<(std::ostream& o, const NamedExpression& ne);

  std::string name;
  Expression expression;
};

}  // namespace tinylamb

#endif  // TINYLAMB_NAMED_EXPRESSION_HPP
