//
// Created by kumagi on 2022/02/22.
//

#ifndef TINYLAMB_EXPRESSION_BASE_HPP
#define TINYLAMB_EXPRESSION_BASE_HPP

#include <vector>

#include "type/value.hpp"

namespace tinylamb {
class Row;
class Schema;

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
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXPRESSION_BASE_HPP
