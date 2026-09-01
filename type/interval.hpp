/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TYPE_INTERVAL_HPP
#define TINYLAMB_TYPE_INTERVAL_HPP

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace tinylamb {

struct IntervalValue {
  int64_t months{0};
  int64_t days{0};
  int64_t nanos{0};

  // Total nanoseconds across all fields; throws when the intermediate
  // computation overflows int64_t instead of silently wrapping (which made
  // distinct intervals compare equal).
  [[nodiscard]] int64_t TotalNanos() const {
    constexpr int64_t kDayNanos = 24LL * 3600LL * 1000000000LL;
    constexpr int64_t kMonthNanos = 30LL * kDayNanos;
    int64_t months_part = 0;
    int64_t days_part = 0;
    int64_t total = nanos;
    if (__builtin_mul_overflow(months, kMonthNanos, &months_part) ||
        __builtin_mul_overflow(days, kDayNanos, &days_part) ||
        __builtin_add_overflow(total, months_part, &total) ||
        __builtin_add_overflow(total, days_part, &total)) {
      throw std::runtime_error("INTERVAL computation out of range");
    }
    return total;
  }

  [[nodiscard]] bool operator==(const IntervalValue& o) const {
    return TotalNanos() == o.TotalNanos();
  }
  [[nodiscard]] auto operator<=>(const IntervalValue& o) const {
    return TotalNanos() <=> o.TotalNanos();
  }

  [[nodiscard]] IntervalValue operator+(const IntervalValue& o) const {
    IntervalValue result{};
    if (__builtin_add_overflow(months, o.months, &result.months) ||
        __builtin_add_overflow(days, o.days, &result.days) ||
        __builtin_add_overflow(nanos, o.nanos, &result.nanos)) {
      throw std::runtime_error("INTERVAL computation out of range");
    }
    std::ignore = result.TotalNanos();  // keeps the result representable
    return result;
  }
  [[nodiscard]] IntervalValue operator-(const IntervalValue& o) const {
    IntervalValue result{};
    if (__builtin_sub_overflow(months, o.months, &result.months) ||
        __builtin_sub_overflow(days, o.days, &result.days) ||
        __builtin_sub_overflow(nanos, o.nanos, &result.nanos)) {
      throw std::runtime_error("INTERVAL computation out of range");
    }
    std::ignore = result.TotalNanos();
    return result;
  }
  [[nodiscard]] IntervalValue operator-() const {
    IntervalValue result{};
    if (__builtin_sub_overflow(int64_t{0}, months, &result.months) ||
        __builtin_sub_overflow(int64_t{0}, days, &result.days) ||
        __builtin_sub_overflow(int64_t{0}, nanos, &result.nanos)) {
      throw std::runtime_error("INTERVAL computation out of range");
    }
    std::ignore = result.TotalNanos();
    return result;
  }
  [[nodiscard]] IntervalValue operator*(int64_t k) const {
    IntervalValue result{};
    if (__builtin_mul_overflow(months, k, &result.months) ||
        __builtin_mul_overflow(days, k, &result.days) ||
        __builtin_mul_overflow(nanos, k, &result.nanos)) {
      throw std::runtime_error("INTERVAL computation out of range");
    }
    std::ignore = result.TotalNanos();
    return result;
  }

  [[nodiscard]] IntervalValue JustifyHours() const;
  [[nodiscard]] IntervalValue JustifyDays() const;
  [[nodiscard]] IntervalValue JustifyInterval() const;

  [[nodiscard]] std::string ToString() const;
  static IntervalValue Parse(std::string_view text, std::string_view unit = "");
};

void SetSessionConstant(std::string_view name, std::string_view value);
[[nodiscard]] std::string GetSessionConstant(std::string_view name);
[[nodiscard]] bool HasSessionConstant(std::string_view name);

}  // namespace tinylamb

#endif  // TINYLAMB_TYPE_INTERVAL_HPP
