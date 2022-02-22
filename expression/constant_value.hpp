//
// Created by kumagi on 2022/02/22.
//

#ifndef TINYLAMB_CONSTANT_VALUE_HPP
#define TINYLAMB_CONSTANT_VALUE_HPP

#include "expression/expression_base.hpp"

namespace tinylamb {

class ConstantValue : public ExpressionBase {
 public:
  explicit ConstantValue(const Value& v) : val_(v) {}

  Value Evaluate(const Row& row, Schema* schema) const override { return val_; }

  friend std::ostream& operator<<(std::ostream& o, const ConstantValue& c) {
    o << c.val_;
    return o;
  }

 private:
  Value val_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CONSTANT_VALUE_HPP
