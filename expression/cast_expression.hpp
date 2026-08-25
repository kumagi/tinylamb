/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_CAST_EXPRESSION_HPP
#define TINYLAMB_CAST_EXPRESSION_HPP

#include <string>
#include <utility>
#include <vector>

#include "expression/expression.hpp"
#include "type/type.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class CastExpression : public ExpressionBase {
 public:
  CastExpression(Expression child, std::string target_type_name,
                 bool return_null_on_error = false);

  [[nodiscard]] Value Evaluate(const Row& row,
                               const Schema& schema) const override;
  [[nodiscard]] Value Evaluate(const Row* left, const Schema& left_schema,
                               const Row* right,
                               const Schema& right_schema) const override;
  [[nodiscard]] Value Evaluate(const Row& row, const Schema& schema,
                               EvaluationContext& context) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema& schema) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema& left,
                                          const Schema& right) const override;
  [[nodiscard]] const Expression& Child() const { return child_; }
  [[nodiscard]] const std::string& TargetTypeName() const {
    return target_type_name_;
  }
  [[nodiscard]] ValueType TargetValueType() const { return target_value_type_; }
  [[nodiscard]] bool ReturnNullOnError() const { return return_null_on_error_; }
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& o) const override;
  [[nodiscard]] TypeTag Type() const override { return TypeTag::kCastExp; }
  [[nodiscard]] std::unordered_set<ColumnName> TouchedColumns() const override;

 private:
  Expression child_;
  std::string target_type_name_;
  ValueType target_value_type_{ValueType::kNull};
  TypeTag target_type_tag_{TypeTag::kInvalid};
  bool return_null_on_error_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_CAST_EXPRESSION_HPP
