//
// Created by kumagi on 2022/02/22.
//

#ifndef TINYLAMB_CONSTANT_VALUE_HPP
#define TINYLAMB_CONSTANT_VALUE_HPP

#include "expression/expression.hpp"

namespace tinylamb {

class ConstantValue : public ExpressionBase {
 public:
  explicit ConstantValue(const Value& v) : val_(v) {}
  [[nodiscard]] TypeTag Type() const override {
    return TypeTag::kConstantValue;
  }
  [[nodiscard]] Value Evaluate(const Row& /*row*/,
                               const Schema& /*schema*/) const override {
    return val_;
  }
  [[nodiscard]] Value GetValue() const { return val_; }

  friend std::ostream& operator<<(std::ostream& o, const ConstantValue& c) {
    o << c.val_;
    return o;
  }

  [[nodiscard]] std::string ToString() const override {
    return val_.AsString();
  }
  void Dump(std::ostream& o) const override { o << val_; }

 private:
  Value val_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CONSTANT_VALUE_HPP
