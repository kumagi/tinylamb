//
// Created by kumagi on 2022/02/22.
//

#ifndef TINYLAMB_CONSTANT_VALUE_HPP
#define TINYLAMB_CONSTANT_VALUE_HPP

#include "expression/expression_base.hpp"

namespace tinylamb {

class ConstantValue : public ExpressionBase {
  explicit ConstantValue(const Value& v) : val_(v) {}

 public:
  [[nodiscard]] TypeTag Type() const override { return TypeTag::kConstant; }
  Value Evaluate(const Row& row, Schema* schema) const override { return val_; }

  friend std::ostream& operator<<(std::ostream& o, const ConstantValue& c) {
    o << c.val_;
    return o;
  }
  void Dump(std::ostream& o) const override { o << val_; }

 private:
  friend class Expression;

  Value val_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CONSTANT_VALUE_HPP
