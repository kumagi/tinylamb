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

#include <utility>

#include "expression/binary_expression.hpp"
#include "expression/expression.hpp"
#include "type/column.hpp"

namespace tinylamb {

// A rebound parameter carries only a Value, so its logical text domain has to
// be recovered from the value itself. Only VARCHAR can be TIMESTAMP/STRING;
// every other type is domain-free. Mirrors the shape check used by the string
// comparison fast path.
[[nodiscard]] inline TextDomain TextDomainForBoundValue(const Value& value) {
  if (value.type != ValueType::kVarChar) {
    return TextDomain::kUnspecified;
  }
  return LooksLikeTimestampText(value.AsString()) ? TextDomain::kTimestamp
                                                  : TextDomain::kString;
}

class ConstantValue : public ExpressionBase {
 public:
  explicit ConstantValue(Value v,
                         TextDomain text_domain = TextDomain::kUnspecified)
      : val_(std::move(v)), text_domain_(text_domain) {}
  [[nodiscard]] TypeTag NodeKind() const override {
    return TypeTag::kConstantValue;
  }
  [[nodiscard]] Value Evaluate(const Row& /*row*/,
                               const Schema& /*schema*/) const override {
    return val_;
  }
  [[nodiscard]] Value Evaluate(const Row* /*left*/,
                               const Schema& /*left_schema*/,
                               const Row* /*right*/,
                               const Schema& /*right_schema*/) const override {
    return val_;
  }
  [[nodiscard]] const Value& GetValue() const { return val_; }
  [[nodiscard]] TextDomain GetTextDomain() const { return text_domain_; }
  [[nodiscard]] tinylamb::Type ResultType(
      const Schema& /*unused*/) const override {
    return TypeForValue(val_.type);
  }
  [[nodiscard]] tinylamb::Type ResultType(
      const Schema& /*unused*/, const Schema& /*unused*/) const override {
    return TypeForValue(val_.type);
  }

  friend std::ostream& operator<<(std::ostream& o, const ConstantValue& c) {
    // T2104: Value::operator<< is a bare AsString(), which silently drops the
    // INTERVAL tag (kVarChar + collation 4) -- the stream form printed a
    // quoted plain string where ToString()/Dump() printed "INTERVAL ...".
    // Route through ToString() so every debug rendering of one constant
    // agrees.
    o << c.ToString();
    return o;
  }

  [[nodiscard]] std::string ToString() const override {
    // Intentionally renders the value only: text_domain_ is *not* part of the
    // printed SQL literal. Expression identity must distinguish domains, and
    // that is done by SamePrintedExpression (rewrite) and EncodeCompileTree
    // (bytecode cache key), not by this human-readable form.
    return val_.IsInterval() ? "INTERVAL " + val_.AsString() : val_.AsString();
  }
  void Dump(std::ostream& o) const override { o << ToString(); }

 protected:
  // ParameterSlot rebinds the sniffed literal in place. The stored value is
  // what AsConstantValue().GetValue() and constant-folding observe. Keep the
  // domain in sync with the new value: a slot sniffed as TIMESTAMP and later
  // bound to a plain STRING (or a non-VARCHAR) must not keep the stale domain,
  // otherwise CTAS column inference and DML write conversion take the wrong
  // type-interpretation path.
  void ResetValue(Value value) {
    text_domain_ = TextDomainForBoundValue(value);
    val_ = std::move(value);
  }

 public:
  [[nodiscard]] static tinylamb::Type TypeForValue(ValueType type) {
    switch (type) {
      case ValueType::kInt64:
        return {TypeTag::kBigInt};
      case ValueType::kDouble:
        return {TypeTag::kDouble};
      case ValueType::kVarChar:
        return {TypeTag::kVarChar};
      case ValueType::kDate:
        return {TypeTag::kDate};
      case ValueType::kArray:
        return {TypeTag::kArray};
      case ValueType::kNull:
        return {TypeTag::kInvalid};
    }
    return {TypeTag::kInvalid};
  }

 private:
  Value val_;
  TextDomain text_domain_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CONSTANT_VALUE_HPP
