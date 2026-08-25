/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_INTERVAL_EXPRESSION_HPP
#define TINYLAMB_INTERVAL_EXPRESSION_HPP

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>

#include "expression/expression.hpp"
#include "type/interval.hpp"

namespace tinylamb {

class IntervalExpression : public ExpressionBase {
 public:
  IntervalExpression(int64_t amount, std::string unit, std::string raw_amount = "")
      : amount_(amount), unit_(std::move(unit)), raw_amount_(std::move(raw_amount)) {
    for (char& c : unit_) { c = static_cast<char>(std::tolower(static_cast<unsigned char>(c))); }
    if (!raw_amount_.empty()) {
      value_ = IntervalValue::Parse(raw_amount_, unit_);
    } else {
      value_ = IntervalValue::Parse(std::to_string(amount_), unit_);
    }
    text_ = value_.ToString();
  }
  [[nodiscard]] TypeTag Type() const override { return TypeTag::kIntervalExp; }
  [[nodiscard]] Value Evaluate(const Row&, const Schema&) const override;
  [[nodiscard]] Value Evaluate(const Row*, const Schema&, const Row*,
                               const Schema&) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema&) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema&,
                                          const Schema&) const override;
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& output) const override;
  [[nodiscard]] int64_t Amount() const { return amount_; }
  [[nodiscard]] const std::string& Unit() const { return unit_; }
  [[nodiscard]] const std::string& RawAmount() const { return raw_amount_; }
  [[nodiscard]] const IntervalValue& GetIntervalValue() const { return value_; }

 private:
  int64_t amount_;
  std::string unit_;
  std::string raw_amount_;
  IntervalValue value_;
  std::string text_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INTERVAL_EXPRESSION_HPP
