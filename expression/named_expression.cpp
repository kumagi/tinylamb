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

//
// Created by kumagi on 22/07/10.
//

#include "expression/named_expression.hpp"

#include <ostream>
#include <string>

namespace tinylamb {

std::ostream& operator<<(std::ostream& o, const NamedExpression& ne) {
  // T2108: expression is legitimately null for placeholder / star-modifier
  // carriers -- every consumer loop (query_data.cpp select_ walks,
  // BindFieldPaths, InlineSelectAliases, ORDER BY resolution) guards it, and
  // dereferencing here crashed EXPLAIN/dump paths on a null item.
  const std::string expression_text =
      ne.expression ? ne.expression->ToString() : std::string("<null>");
  o << expression_text;
  if (!ne.name.empty() && ne.name != expression_text) {
    o << " AS " << ne.name;
  }
  // T2107: SELECT * EXCEPT / REPLACE would otherwise dump exactly like a bare
  // star, hiding which columns the projection actually drops or rewrites.
  if (ne.star_modifiers) {
    if (!ne.star_modifiers->except_columns.empty()) {
      o << " EXCEPT (";
      for (size_t i = 0; i < ne.star_modifiers->except_columns.size(); ++i) {
        if (i != 0) {
          o << ", ";
        }
        o << ne.star_modifiers->except_columns[i];
      }
      o << ")";
    }
    if (!ne.star_modifiers->replace_items.empty()) {
      o << " REPLACE (";
      for (size_t i = 0; i < ne.star_modifiers->replace_items.size(); ++i) {
        if (i != 0) {
          o << ", ";
        }
        const StarReplaceItem& item = ne.star_modifiers->replace_items[i];
        o << (item.expression ? item.expression->ToString()
                              : std::string("<null>"))
          << " AS " << item.column;
      }
      o << ")";
    }
  }
  return o;
}

}  // namespace tinylamb