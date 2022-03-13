//
// Created by kumagi on 2022/02/22.
//

#ifndef TINYLAMB_EXPRESSION_HPP
#define TINYLAMB_EXPRESSION_HPP

#include <iosfwd>
#include <memory>

#include "type/value.hpp"

namespace tinylamb {
class Schema;
struct Row;

enum class TypeTag : int {
  kBinaryExp,
  kColumnValue,
  kConstant,
};

class ExpressionBase {
 public:
  virtual ~ExpressionBase() = default;
  [[nodiscard]] virtual TypeTag Type() const = 0;
  virtual Value Evaluate(const Row& row, Schema* schema) const = 0;
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
