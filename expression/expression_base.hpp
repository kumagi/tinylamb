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

class ExpressionBase {
 public:
  virtual ~ExpressionBase() = default;
  virtual Value Evaluate(const Row& row, Schema* schema) const = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXPRESSION_BASE_HPP
