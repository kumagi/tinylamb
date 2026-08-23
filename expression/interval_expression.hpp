/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_INTERVAL_EXPRESSION_HPP
#define TINYLAMB_INTERVAL_EXPRESSION_HPP

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>

#include "expression/expression.hpp"

namespace tinylamb {

class IntervalExpression : public ExpressionBase {
 public:
  IntervalExpression(int64_t amount, std::string unit)
      : amount_(amount), unit_(std::move(unit)) {
    // Validate eagerly so malformed units fail at construction instead of
    // leaking out of date_add/date_sub evaluation.
    static const char* kUnits[] = {"day",   "days",   "month", "months",
                                   "year",  "years"};
    bool known = false;
    for (const char* candidate : kUnits) {
      if (unit_ == candidate) {
        known = true;
        break;
      }
    }
    if (!known) {
      throw std::runtime_error("unsupported interval unit " + unit_);
    }
    text_ = std::to_string(amount_) + " " + unit_;
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

 private:
  int64_t amount_;
  std::string unit_;
  std::string text_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INTERVAL_EXPRESSION_HPP
