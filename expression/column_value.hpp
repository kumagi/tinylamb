//
// Created by kumagi on 2022/02/22.
//

#ifndef TINYLAMB_COLUMN_VALUE_HPP
#define TINYLAMB_COLUMN_VALUE_HPP

#include "expression/expression_base.hpp"

namespace tinylamb {

class ColumnValue : public ExpressionBase {
  explicit ColumnValue(std::string_view col_name) : col_name_(col_name) {}

 public:
  ~ColumnValue() override = default;
  Value Evaluate(const Row& row, Schema* schema) const override;
  void Dump(std::ostream& o) const override;

 private:
  friend class Expression;

  std::string col_name_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_COLUMN_VALUE_HPP
