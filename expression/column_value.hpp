//
// Created by kumagi on 2022/02/22.
//

#ifndef TINYLAMB_COLUMN_VALUE_HPP
#define TINYLAMB_COLUMN_VALUE_HPP

#include "expression/expression.hpp"

namespace tinylamb {

class ColumnValue : public ExpressionBase {
 public:
  explicit ColumnValue(std::string_view col_name) : col_name_(col_name) {}
  ~ColumnValue() override = default;
  [[nodiscard]] TypeTag Type() const override { return TypeTag::kColumnValue; }
  Value Evaluate(const Row& row, Schema* schema) const override;
  void Dump(std::ostream& o) const override;
  [[nodiscard]] std::string ColumnName() const { return col_name_; }

 private:
  friend class ProjectionPlan;

  std::string col_name_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_COLUMN_VALUE_HPP
