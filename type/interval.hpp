/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TYPE_INTERVAL_HPP
#define TINYLAMB_TYPE_INTERVAL_HPP

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include "common/exc_shim.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

struct IntervalValue {
  int64_t months{0};
  int64_t days{0};
  int64_t nanos{0};

  // Total nanoseconds across all fields; kInvalidArgument when the
  // intermediate computation overflows int64_t instead of silently wrapping
  // (which made distinct intervals compare equal).
  [[nodiscard]] StatusOr<int64_t> TryTotalNanos() const {
    constexpr int64_t kDayNanos = 24LL * 3600LL * 1000000000LL;
    constexpr int64_t kMonthNanos = 30LL * kDayNanos;
    int64_t months_part = 0;
    int64_t days_part = 0;
    int64_t total = nanos;
    if (__builtin_mul_overflow(months, kMonthNanos, &months_part) ||
        __builtin_mul_overflow(days, kDayNanos, &days_part) ||
        __builtin_add_overflow(total, months_part, &total) ||
        __builtin_add_overflow(total, days_part, &total)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "INTERVAL computation out of range");
    }
    return total;
  }

  // Field-wise comparison (months, then days, then time).  Deliberately NOT
  // via TryTotalNanos(): that errors for perfectly representable intervals
  // beyond ~3558 years and conflates calendar months with 30-day months.
  [[nodiscard]] bool operator==(const IntervalValue& o) const {
    return nanos == o.nanos && days == o.days && months == o.months;
  }
  [[nodiscard]] auto operator<=>(const IntervalValue& o) const {
    if (auto c = months <=> o.months; c != std::strong_ordering::equal) {
      return c;
    }
    if (auto c = days <=> o.days; c != std::strong_ordering::equal) {
      return c;
    }
    return nanos <=> o.nanos;
  }

  [[nodiscard]] StatusOr<IntervalValue> TryPlus(const IntervalValue& o) const {
    IntervalValue result{};
    if (__builtin_add_overflow(months, o.months, &result.months) ||
        __builtin_add_overflow(days, o.days, &result.days) ||
        __builtin_add_overflow(nanos, o.nanos, &result.nanos)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "INTERVAL computation out of range");
    }
    return AddChecked(result);
  }
  [[nodiscard]] StatusOr<IntervalValue> TryMinus(const IntervalValue& o) const {
    IntervalValue result{};
    if (__builtin_sub_overflow(months, o.months, &result.months) ||
        __builtin_sub_overflow(days, o.days, &result.days) ||
        __builtin_sub_overflow(nanos, o.nanos, &result.nanos)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "INTERVAL computation out of range");
    }
    return AddChecked(result);
  }
  [[nodiscard]] StatusOr<IntervalValue> TryNegate() const {
    IntervalValue result{};
    if (__builtin_sub_overflow(int64_t{0}, months, &result.months) ||
        __builtin_sub_overflow(int64_t{0}, days, &result.days) ||
        __builtin_sub_overflow(int64_t{0}, nanos, &result.nanos)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "INTERVAL computation out of range");
    }
    return AddChecked(result);
  }
  [[nodiscard]] StatusOr<IntervalValue> TryMultiply(int64_t k) const {
    IntervalValue result{};
    if (__builtin_mul_overflow(months, k, &result.months) ||
        __builtin_mul_overflow(days, k, &result.days) ||
        __builtin_mul_overflow(nanos, k, &result.nanos)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "INTERVAL computation out of range");
    }
    return AddChecked(result);
  }

  [[nodiscard]] StatusOr<IntervalValue> TryJustifyHours() const;
  [[nodiscard]] StatusOr<IntervalValue> TryJustifyDays() const;
  [[nodiscard]] StatusOr<IntervalValue> TryJustifyInterval() const;

  [[nodiscard]] StatusOr<std::string> TryToString() const;
  [[nodiscard]] static StatusOr<IntervalValue> TryParse(
      std::string_view text, std::string_view unit = "");

  // EXC-SHIM: deprecated throwing wrappers (removed with the expression/query
  // conversion, see common/exc_shim.hpp).
  [[nodiscard]] int64_t TotalNanos() const {
    return ExcShimUnwrap(TryTotalNanos(), "IntervalValue::TotalNanos");
  }
  [[nodiscard]] IntervalValue operator+(const IntervalValue& o) const {
    return ExcShimUnwrap(TryPlus(o), "IntervalValue::operator+");
  }
  [[nodiscard]] IntervalValue operator-(const IntervalValue& o) const {
    return ExcShimUnwrap(TryMinus(o), "IntervalValue::operator-");
  }
  [[nodiscard]] IntervalValue operator-() const {
    return ExcShimUnwrap(TryNegate(), "IntervalValue::operator-");
  }
  [[nodiscard]] IntervalValue operator*(int64_t k) const {
    return ExcShimUnwrap(TryMultiply(k), "IntervalValue::operator*");
  }
  [[nodiscard]] IntervalValue JustifyHours() const {
    return ExcShimUnwrap(TryJustifyHours(), "IntervalValue::JustifyHours");
  }
  [[nodiscard]] IntervalValue JustifyDays() const {
    return ExcShimUnwrap(TryJustifyDays(), "IntervalValue::JustifyDays");
  }
  [[nodiscard]] IntervalValue JustifyInterval() const {
    return ExcShimUnwrap(TryJustifyInterval(),
                         "IntervalValue::JustifyInterval");
  }
  [[nodiscard]] std::string ToString() const {
    return ExcShimUnwrap(TryToString(), "IntervalValue::ToString");
  }
  static IntervalValue Parse(std::string_view text,
                             std::string_view unit = "") {
    return ExcShimUnwrap(TryParse(text, unit), "IntervalValue::Parse");
  }

 private:
  // Keeps a composed result representable (same check TotalNanos does).
  [[nodiscard]] static StatusOr<IntervalValue> AddChecked(
      const IntervalValue& result) {
    ASSIGN_OR_RETURN(int64_t, unused, result.TryTotalNanos());
    std::ignore = unused;
    return result;
  }
};

void SetSessionConstant(std::string_view name, std::string_view value);
[[nodiscard]] std::string GetSessionConstant(std::string_view name);
[[nodiscard]] bool HasSessionConstant(std::string_view name);

}  // namespace tinylamb

#endif  // TINYLAMB_TYPE_INTERVAL_HPP
