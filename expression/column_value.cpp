//
// Created by kumagi on 2022/02/22.
//

#include "expression/column_value.hpp"

#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

Value ColumnValue::Evaluate(const Row& row, const Schema& schema) const {
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const Column& c = schema.GetColumn(i);
    if (c.Name() == col_name_) {
      return row[i];
    }
  }
  throw std::runtime_error("column " + col_name_ + " not found");
}

void ColumnValue::Dump(std::ostream& o) const { o << col_name_; }

}  // namespace tinylamb