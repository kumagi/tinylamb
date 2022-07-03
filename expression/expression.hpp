//
// Created by kumagi on 2022/02/22.
//

#ifndef TINYLAMB_EXPRESSION_HPP
#define TINYLAMB_EXPRESSION_HPP

#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_set>

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
  [[nodiscard]] const BinaryExpression& AsBinaryExpression() const;
  [[nodiscard]] const ConstantValue& AsConstantValue() const;
  [[nodiscard]] std::unordered_set<std::string> TouchedColumns() const;
  [[nodiscard]] virtual Value Evaluate(const Row& row,
                                       const Schema& schema) const = 0;
  virtual void Dump(std::ostream& o) const = 0;
  friend std::ostream& operator<<(std::ostream& o, const ExpressionBase& e) {
    e.Dump(o);
    return o;
  }
};

typedef std::shared_ptr<ExpressionBase> Expression;
Expression ColumnValueExp(std::string_view col_name);
Expression ConstantValueExp(const Value& v);
Expression BinaryExpressionExp(Expression left, BinaryOperation op,
                               Expression right);

}  // namespace tinylamb

#endif  // TINYLAMB_EXPRESSION_HPP
