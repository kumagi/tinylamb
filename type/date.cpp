/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "type/date.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <string_view>

namespace tinylamb {
namespace {

using std::chrono::day;
using std::chrono::month_day_last;
using std::chrono::sys_days;
using std::chrono::year;
using std::chrono::year_month_day;
using std::chrono::year_month_day_last;

// sys_days counts days in a 32-bit representation and chrono::year saturates
// at ±32767; keep every day count well inside both limits.
constexpr int64_t kMinSupportedDays = -11000000;
constexpr int64_t kMaxSupportedDays = 11000000;

sys_days ToSysDays(int64_t days) {
  if (days < kMinSupportedDays || kMaxSupportedDays < days) {
    throw std::runtime_error("DATE value out of range");
  }
  return sys_days{std::chrono::days{static_cast<int>(days)}};
}

// Floor division/modulo so negative offsets round the way calendars do
// (truncated C++ division would map January -1 month into the wrong year).
int64_t FloorDiv(int64_t a, int64_t b) {
  const int64_t q = a / b;
  return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
}

int64_t FloorMod(int64_t a, int64_t b) { return a - (FloorDiv(a, b) * b); }

// Shift `base` by whole years plus extra months, computed in int64 because
// chrono::months/chrono::years truncate to their internal int representation.
// Day-of-month overflows are clamped to the last day of the target month,
// matching SQL interval semantics.
sys_days ShiftYMDClamped(const year_month_day& base, int64_t add_years,
                         int64_t add_months) {
  const int64_t base_year = static_cast<int>(base.year());
  const int64_t base_month =
      static_cast<int64_t>(static_cast<unsigned>(base.month())) - 1;
  // add_months stays small (see callers), so this cannot overflow.
  int64_t month_index = base_month + add_months;
  const int64_t carry_years = FloorDiv(month_index, 12);
  month_index -= carry_years * 12;
  int64_t stepped_year = 0;
  int64_t new_year = 0;
  if (__builtin_add_overflow(base_year, add_years, &stepped_year) ||
      __builtin_add_overflow(stepped_year, carry_years, &new_year)) {
    throw std::runtime_error("DATE computation out of range");
  }
  if (new_year < static_cast<int>(year::min()) ||
      static_cast<int>(year::max()) < new_year) {
    throw std::runtime_error("DATE computation out of range");
  }
  const year y{static_cast<int>(new_year)};
  const std::chrono::month m{static_cast<unsigned>(month_index + 1)};
  year_month_day ymd{y, m, day{static_cast<unsigned>(base.day())}};
  if (!ymd.ok()) {
    // e.g. 2024-01-31 + 1 month: clamp Feb 31 down to the month end.
    ymd = year_month_day{year_month_day_last{y, month_day_last{m}}};
  }
  return sys_days{ymd};
}

}  // namespace

int64_t ParseDateDays(std::string_view date) {
  // Strict "Y-M-D" scan: decimal fields separated by '-', nothing else. Unlike
  // sscanf this rejects surrounding whitespace and trailing garbage
  // ("2024-01-01xyz"), while still accepting single-digit fields ("2024-1-1").
  size_t pos = 0;
  const auto read_field = [&](int64_t* out) {
    if (pos >= date.size() || date[pos] < '0' || '9' < date[pos]) {
      throw std::runtime_error("invalid DATE value");
    }
    int64_t value = 0;
    while (pos < date.size() && '0' <= date[pos] && date[pos] <= '9') {
      if (value > 99999999) {  // Cap digits so `value` cannot overflow.
        throw std::runtime_error("invalid DATE value");
      }
      value = (value * 10) + (date[pos++] - '0');
    }
    *out = value;
  };
  const auto expect = [&](char c) {
    if (pos >= date.size() || date[pos] != c) {
      throw std::runtime_error("invalid DATE value");
    }
    ++pos;
  };
  int64_t y = 0;
  int64_t m = 0;
  int64_t d = 0;
  read_field(&y);
  expect('-');
  read_field(&m);
  expect('-');
  read_field(&d);
  if (pos != date.size()) {
    throw std::runtime_error("invalid DATE value");
  }
  if (y < static_cast<int>(year::min()) || static_cast<int>(year::max()) < y) {
    throw std::runtime_error("invalid DATE value");
  }
  // Range-check before constructing month/day: their constructors truncate to
  // unsigned char, so an out-of-range value like month 257 would silently
  // wrap into a valid-looking month instead of failing validation.
  if (m < 1 || 12 < m || d < 1 || 31 < d) {
    throw std::runtime_error("invalid DATE value");
  }
  const year_month_day ymd{year{static_cast<int>(y)},
                           std::chrono::month{static_cast<unsigned>(m)},
                           day{static_cast<unsigned>(d)}};
  // ok() validates month 1..12, day 1..31 and leap-year February days.
  if (!ymd.ok()) {
    throw std::runtime_error("invalid DATE value");
  }
  return sys_days{ymd}.time_since_epoch().count();
}

std::string FormatDateDays(int64_t days) {
  std::array<char, 16> buffer{};
  const year_month_day ymd{ToSysDays(days)};
  const int written = std::snprintf(buffer.data(), buffer.size(),
                                    "%04d-%02u-%02u", int(ymd.year()),
                                    unsigned(ymd.month()), unsigned(ymd.day()));
  if (written < 0 || static_cast<size_t>(written) >= buffer.size()) {
    throw std::runtime_error("DATE value out of representable range");
  }
  return buffer.data();
}

int64_t AddDateIntervalDays(int64_t days, int64_t amount,
                            std::string_view unit) {
  using namespace std::chrono;
  ToSysDays(days);  // Reject inputs chrono cannot represent up front.
  if (unit == "day" || unit == "days") {
    int64_t result = 0;
    if (__builtin_add_overflow(days, amount, &result)) {
      throw std::runtime_error("DATE computation out of range");
    }
    return ToSysDays(result).time_since_epoch().count();
  }
  if (unit == "month" || unit == "months") {
    const year_month_day ymd{ToSysDays(days)};
    // Split so both components stay small and no intermediate overflows.
    return ShiftYMDClamped(ymd, FloorDiv(amount, 12), FloorMod(amount, 12))
        .time_since_epoch()
        .count();
  }
  if (unit == "year" || unit == "years") {
    const year_month_day ymd{ToSysDays(days)};
    return ShiftYMDClamped(ymd, amount, 0).time_since_epoch().count();
  }
  throw std::runtime_error("unsupported interval unit " + std::string(unit));
}

namespace {
thread_local std::string tls_default_time_zone = "America/Los_Angeles";
}  // namespace

void SetDefaultTimeZone(std::string_view tz) {
  if (tz.empty()) {
    tls_default_time_zone = "America/Los_Angeles";
  } else {
    tls_default_time_zone = std::string(tz);
  }
}

std::string GetDefaultTimeZone() {
  return tls_default_time_zone.empty() ? "America/Los_Angeles"
                                       : tls_default_time_zone;
}

}  // namespace tinylamb
