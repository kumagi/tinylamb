/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "type/date.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>

#include "common/status_or.hpp"

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

StatusOr<sys_days> ToSysDays(int64_t days) {
  if (days < kMinSupportedDays || kMaxSupportedDays < days) {
    return StatusError(StatusCode::kInvalidArgument, "DATE value out of range");
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
StatusOr<sys_days> ShiftYMDClamped(const year_month_day& base,
                                   int64_t add_years, int64_t add_months) {
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
    return StatusError(StatusCode::kInvalidArgument,
                       "DATE computation out of range");
  }
  if (new_year < static_cast<int>(year::min()) ||
      static_cast<int>(year::max()) < new_year) {
    return StatusError(StatusCode::kInvalidArgument,
                       "DATE computation out of range");
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

Status InvalidDate() {
  return StatusError(StatusCode::kInvalidArgument, "invalid DATE value");
}

}  // namespace

StatusOr<int64_t> TryParseDateDays(std::string_view date) {
  // Strict "Y-M-D" scan: decimal fields separated by '-', nothing else. Unlike
  // sscanf this rejects surrounding whitespace and trailing garbage
  // ("2024-01-01xyz"), while still accepting single-digit fields ("2024-1-1").
  size_t pos = 0;
  const auto read_field = [&](int64_t* out) -> Status {
    if (pos >= date.size() || date[pos] < '0' || '9' < date[pos]) {
      return InvalidDate();
    }
    int64_t value = 0;
    while (pos < date.size() && '0' <= date[pos] && date[pos] <= '9') {
      if (value > 99999999) {  // Cap digits so `value` cannot overflow.
        return InvalidDate();
      }
      value = (value * 10) + (date[pos++] - '0');
    }
    *out = value;
    return Status::kSuccess;
  };
  const auto expect = [&](char c) -> Status {
    if (pos >= date.size() || date[pos] != c) {
      return InvalidDate();
    }
    ++pos;
    return Status::kSuccess;
  };
  int64_t y = 0;
  int64_t m = 0;
  int64_t d = 0;
  RETURN_IF_FAIL(read_field(&y));
  RETURN_IF_FAIL(expect('-'));
  RETURN_IF_FAIL(read_field(&m));
  RETURN_IF_FAIL(expect('-'));
  RETURN_IF_FAIL(read_field(&d));
  if (pos != date.size()) {
    return InvalidDate();
  }
  if (y < static_cast<int>(year::min()) || static_cast<int>(year::max()) < y) {
    return InvalidDate();
  }
  // Range-check before constructing month/day: their constructors truncate to
  // unsigned char, so an out-of-range value like month 257 would silently
  // wrap into a valid-looking month instead of failing validation.
  if (m < 1 || 12 < m || d < 1 || 31 < d) {
    return InvalidDate();
  }
  const year_month_day ymd{year{static_cast<int>(y)},
                           std::chrono::month{static_cast<unsigned>(m)},
                           day{static_cast<unsigned>(d)}};
  // ok() validates month 1..12, day 1..31 and leap-year February days.
  if (!ymd.ok()) {
    return InvalidDate();
  }
  return sys_days{ymd}.time_since_epoch().count();
}

StatusOr<std::string> TryFormatDateDays(int64_t days) {
  std::array<char, 16> buffer{};
  ASSIGN_OR_RETURN(sys_days, sd, ToSysDays(days));
  const year_month_day ymd{sd};
  const int written = std::snprintf(buffer.data(), buffer.size(),
                                    "%04d-%02u-%02u", int(ymd.year()),
                                    unsigned(ymd.month()), unsigned(ymd.day()));
  if (written < 0 || static_cast<size_t>(written) >= buffer.size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "DATE value out of representable range");
  }
  return std::string(buffer.data());
}

StatusOr<int64_t> TryAddDateIntervalDays(int64_t days, int64_t amount,
                                         std::string_view unit) {
  using namespace std::chrono;
  ASSIGN_OR_RETURN(sys_days, head, ToSysDays(days));  // Reject unrepresentable
  std::ignore = head;                                 // inputs up front.
  if (unit == "day" || unit == "days") {
    int64_t result = 0;
    if (__builtin_add_overflow(days, amount, &result)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "DATE computation out of range");
    }
    ASSIGN_OR_RETURN(sys_days, shifted, ToSysDays(result));
    return shifted.time_since_epoch().count();
  }
  if (unit == "month" || unit == "months") {
    const year_month_day ymd{head};
    // Split so both components stay small and no intermediate overflows.
    ASSIGN_OR_RETURN(
        sys_days, shifted,
        ShiftYMDClamped(ymd, FloorDiv(amount, 12), FloorMod(amount, 12)));
    return shifted.time_since_epoch().count();
  }
  if (unit == "year" || unit == "years") {
    const year_month_day ymd{head};
    ASSIGN_OR_RETURN(sys_days, shifted, ShiftYMDClamped(ymd, amount, 0));
    return shifted.time_since_epoch().count();
  }
  return StatusError(StatusCode::kInvalidArgument,
                     "unsupported interval unit " + std::string(unit));
}

namespace {
// std::string allocation at thread-local init is the accepted failure mode;
// a bad_alloc here is fatal for the engine regardless of how it surfaces.
// NOLINTNEXTLINE(cert-err58-cpp)
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
