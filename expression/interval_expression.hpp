/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_INTERVAL_EXPRESSION_HPP
#define TINYLAMB_INTERVAL_EXPRESSION_HPP

#include <cstdint>
#include <string>
#include <utility>

#include "expression/expression.hpp"

namespace tinylamb {

class IntervalExpression : public ExpressionBase {
 public:
  IntervalExpression(int64_t amount, std::string unit)
      : amount_(amount), unit_(std::move(unit)) {}
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
};

}  // namespace tinylamb

#endif  // TINYLAMB_INTERVAL_EXPRESSION_HPP
