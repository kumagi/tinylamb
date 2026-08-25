/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TYPE_INTERVAL_HPP
#define TINYLAMB_TYPE_INTERVAL_HPP

#include <cstdint>
#include <string>
#include <string_view>

namespace tinylamb {

struct IntervalValue {
  int64_t months{0};
  int64_t days{0};
  int64_t nanos{0};

  [[nodiscard]] bool operator==(const IntervalValue& o) const {
    constexpr int64_t kDayNanos = 24LL * 3600LL * 1000000000LL;
    constexpr int64_t kMonthNanos = 30LL * kDayNanos;
    int64_t total1 = months * kMonthNanos + days * kDayNanos + nanos;
    int64_t total2 = o.months * kMonthNanos + o.days * kDayNanos + o.nanos;
    return total1 == total2;
  }
  [[nodiscard]] auto operator<=>(const IntervalValue& o) const {
    constexpr int64_t kDayNanos = 24LL * 3600LL * 1000000000LL;
    constexpr int64_t kMonthNanos = 30LL * kDayNanos;
    int64_t total1 = months * kMonthNanos + days * kDayNanos + nanos;
    int64_t total2 = o.months * kMonthNanos + o.days * kDayNanos + o.nanos;
    return total1 <=> total2;
  }

  [[nodiscard]] IntervalValue operator+(const IntervalValue& o) const {
    return {months + o.months, days + o.days, nanos + o.nanos};
  }
  [[nodiscard]] IntervalValue operator-(const IntervalValue& o) const {
    return {months - o.months, days - o.days, nanos - o.nanos};
  }
  [[nodiscard]] IntervalValue operator-() const {
    return {-months, -days, -nanos};
  }
  [[nodiscard]] IntervalValue operator*(int64_t k) const {
    return {months * k, days * k, nanos * k};
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
