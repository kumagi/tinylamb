/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
