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

#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

Value ColumnValue::Evaluate(const Row& row, const Schema& schema) const {
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const Column& c = schema.GetColumn(i);
    if (c.Name().name == col_name_.name) {
      return row[i];
    }
  }
  throw std::runtime_error("column " + col_name_.ToString() + " not found");
}

void ColumnValue::Dump(std::ostream& o) const { o << col_name_; }

}  // namespace tinylamb