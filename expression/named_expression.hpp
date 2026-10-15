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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/log_message.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "type/column_name.hpp"

namespace tinylamb {

// `SELECT * REPLACE (expr AS col, ...)` substitution: `col` keeps its output
// position and name while `expr` (evaluated over the star target's base
// columns) supplies its values.
struct StarReplaceItem {
  std::string column;
  Expression expression;
};

// Modifiers of a `SELECT * [EXCEPT (...)] [REPLACE (...)]` select item.  The
// item's expression stays the plain star (`ColumnValueExp("*")`, optionally
// qualified), so every existing star detection keeps working; consumers that
// expand the star into concrete columns read this payload and must honor it
// (or fail loudly) — silently ignoring it would widen the output.
struct StarModifiers {
  // EXCEPT names to drop from the expansion, as spelled in the query
  // (matching against expanded columns is case-insensitive; a qualified
  // spelling `t.col` matches the star target).
  std::vector<std::string> except_columns;
  // REPLACE substitutions, in query order.  Duplicate targets are rejected at
  // visit time, and a target must name a column of the star target.
  std::vector<StarReplaceItem> replace_items;
};

struct NamedExpression {
  explicit NamedExpression(const ColumnName& name_)
      : name(), expression(ColumnValueExp(name_)) {}
  explicit NamedExpression(std::string_view name_)
      : name(), expression(ColumnValueExp(name_)) {}
  NamedExpression(std::string_view name_, const ColumnName& column_name)
      : name(name_), expression(ColumnValueExp(column_name)) {}
  NamedExpression(std::string_view name_, Expression exp)
      : name(name_), expression(std::move(exp)) {}

  friend std::ostream& operator<<(std::ostream& o, const NamedExpression& ne);

  std::string name;
  Expression expression;
  // Non-null only on `*` / `rel.*` select items carrying EXCEPT / REPLACE.
  std::shared_ptr<const StarModifiers> star_modifiers;
};

}  // namespace tinylamb

#endif  // TINYLAMB_NAMED_EXPRESSION_HPP
