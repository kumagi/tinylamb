/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "type/interval.hpp"

#include <array>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/log_message.hpp"

namespace tinylamb {

namespace {

thread_local std::unordered_map<std::string, std::string> tls_session_constants;

// snprintf whose result is enforced: a negative return means an encoding
// failure that would otherwise surface as stale buffer contents. Truncation
// keeps the historical silently-clipped behavior. The C-style ellipsis is
// required to forward to vsnprintf; the format attribute guards it.
int CheckedSnprintf(char* buf, size_t size, const char* fmt, ...)
    __attribute__((format(printf, 3, 4)));
// NOLINTNEXTLINE(cert-dcl50-cpp)
int CheckedSnprintf(char* buf, size_t size, const char* fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  const int n = std::vsnprintf(buf, size, fmt, ap);
  va_end(ap);
  CHECK_MSG(n >= 0, "INTERVAL formatting failed");
  return n;
}

std::string Trim(std::string_view s) {
  while (!s.empty() &&
         (std::isspace(static_cast<unsigned char>(s.front())) != 0)) {
    s.remove_prefix(1);
  }
  while (!s.empty() &&
         (std::isspace(static_cast<unsigned char>(s.back())) != 0)) {
    s.remove_suffix(1);
  }
  return std::string(s);
}

std::string ToLower(std::string_view s) {
  std::string out(s);
  for (char& c : out) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return out;
}

}  // namespace

void SetSessionConstant(std::string_view name, std::string_view value) {
  tls_session_constants[std::string(name)] = std::string(value);
}

std::string GetSessionConstant(std::string_view name) {
  auto it = tls_session_constants.find(std::string(name));
  if (it != tls_session_constants.end()) {
    return it->second;
  }
  return "";
}

bool HasSessionConstant(std::string_view name) {
  return tls_session_constants.contains(std::string(name));
}

StatusOr<IntervalValue> IntervalValue::TryJustifyHours() const {
  constexpr int64_t kDayNanos = 24LL * 3600LL * 1000000000LL;
  // Overflow-checked: days * kDayNanos previously wrapped silently on large
  // parseable intervals (e.g. P4000000000D) and produced garbage results.
  int64_t days_part = 0;
  int64_t total_nanos = nanos;
  if (__builtin_mul_overflow(days, kDayNanos, &days_part) ||
      __builtin_add_overflow(total_nanos, days_part, &total_nanos)) {
    return StatusError(StatusCode::kInvalidArgument,
                       "INTERVAL computation out of range");
  }
  bool neg = (total_nanos < 0);
  int64_t abs_nanos = std::abs(total_nanos);

  int64_t res_days = abs_nanos / kDayNanos;
  int64_t res_nanos = abs_nanos % kDayNanos;

  if (neg) {
    return IntervalValue{
        .months = months, .days = -res_days, .nanos = -res_nanos};
  }
  return IntervalValue{.months = months, .days = res_days, .nanos = res_nanos};
}

StatusOr<IntervalValue> IntervalValue::TryJustifyDays() const {
  int64_t total_days = 0;
  if (__builtin_mul_overflow(months, int64_t{30}, &total_days) ||
      __builtin_add_overflow(total_days, days, &total_days)) {
    return StatusError(StatusCode::kInvalidArgument,
                       "INTERVAL computation out of range");
  }
  bool neg = (total_days < 0);
  int64_t abs_days = std::abs(total_days);

  int64_t res_months = abs_days / 30;
  int64_t res_days = abs_days % 30;

  if (neg) {
    return IntervalValue{
        .months = -res_months, .days = -res_days, .nanos = nanos};
  }
  return IntervalValue{.months = res_months, .days = res_days, .nanos = nanos};
}

StatusOr<IntervalValue> IntervalValue::TryJustifyInterval() const {
  constexpr int64_t kDayNanos = 24LL * 3600LL * 1000000000LL;
  constexpr int64_t kMonthNanos = 30LL * kDayNanos;

  // Overflow-checked via TryTotalNanos().
  ASSIGN_OR_RETURN(int64_t, total_nanos, TryTotalNanos());
  bool neg = (total_nanos < 0);
  int64_t abs_nanos = std::abs(total_nanos);

  int64_t res_months = abs_nanos / kMonthNanos;
  int64_t rem_after_months = abs_nanos % kMonthNanos;

  int64_t res_days = rem_after_months / kDayNanos;
  int64_t res_nanos = rem_after_months % kDayNanos;

  if (neg) {
    return IntervalValue{
        .months = -res_months, .days = -res_days, .nanos = -res_nanos};
  }
  return IntervalValue{
      .months = res_months, .days = res_days, .nanos = res_nanos};
}

StatusOr<std::string> IntervalValue::TryToString() const {
  int64_t y = months / 12;
  int64_t m = months % 12;
  int64_t total_sec = nanos / 1000000000LL;
  int64_t sub_ns = nanos % 1000000000LL;
  int64_t h = total_sec / 3600;
  int64_t min = (total_sec % 3600) / 60;
  int64_t s = total_sec % 60;

  std::array<char, 64> ym_buf{};
  if (months < 0) {
    CheckedSnprintf(ym_buf.data(), ym_buf.size(), "-%ld-%ld", std::abs(y),
                    std::abs(m));
  } else {
    CheckedSnprintf(ym_buf.data(), ym_buf.size(), "%ld-%ld", y, m);
  }

  std::array<char, 32> d_buf{};
  CheckedSnprintf(d_buf.data(), d_buf.size(), "%ld", days);

  std::array<char, 64> t_buf{};
  bool neg_time = (nanos < 0);
  int64_t abs_h = std::abs(h);
  int64_t abs_min = std::abs(min);
  int64_t abs_s = std::abs(s);
  int64_t abs_sub_ns = std::abs(sub_ns);

  if (abs_sub_ns != 0) {
    std::array<char, 16> frac{};
    CheckedSnprintf(frac.data(), frac.size(), "%09ld", abs_sub_ns);
    size_t end = 9;
    while (end > 0 && frac[end - 1] == '0') {
      --end;
    }
    frac[end] = '\0';
    CheckedSnprintf(t_buf.data(), t_buf.size(), "%s%ld:%ld:%ld.%s",
                    neg_time ? "-" : "", abs_h, abs_min, abs_s, frac.data());
  } else {
    CheckedSnprintf(t_buf.data(), t_buf.size(), "%s%ld:%ld:%ld",
                    neg_time ? "-" : "", abs_h, abs_min, abs_s);
  }

  return std::string(ym_buf.data()) + " " + d_buf.data() + " " + t_buf.data();
}

StatusOr<IntervalValue> IntervalValue::TryParse(std::string_view text,
                                                std::string_view unit_str) {
  // Error text shared by every rejection path: GoogleSQL surfaces parse and
  // range failures under one generic::out_of_range prefix.
  const auto fail = [](const std::string& message) -> Status {
    return StatusError(StatusCode::kInvalidArgument,
                       "generic::out_of_range: " + message);
  };
  const auto fail_range = [&]() -> Status {
    return fail("INTERVAL value out of range");
  };
  std::string s = Trim(text);
  std::string u = ToLower(Trim(unit_str));
  if (s.empty()) {
    return IntervalValue{};
  }

  // Saturate-with-error instead of wrapping: plain int64 arithmetic on
  // stoll-parsed literals wraps silently (e.g. INTERVAL '922337203685477580
  // 0 0'), yielding a wrong interval where every sibling path reports
  // out-of-range.  Shared by the ISO branch below and the composite paths.
  // The checked helpers saturate-and-flag instead of unwinding: composite
  // paths keep accumulating into iv and the flag turns the eventual return
  // into the same out-of-range error the old throw produced.
  bool overflowed = false;
  const auto checked_mul = [&](int64_t a, int64_t b) -> int64_t {
    int64_t result = 0;
    if (__builtin_mul_overflow(a, b, &result)) {
      overflowed = true;
    }
    return result;
  };
  const auto checked_add = [&](int64_t a, int64_t b) -> int64_t {
    int64_t result = 0;
    if (__builtin_add_overflow(a, b, &result)) {
      overflowed = true;
    }
    return result;
  };
  const auto checked_return =
      [&](const IntervalValue& v) -> StatusOr<IntervalValue> {
    if (overflowed) {
      return fail_range();
    }
    return v;
  };

  // ISO 8601 (e.g. "P1Y2M3DT4H5M6.789S")
  if (s.front() == 'P' || s.front() == 'p') {
    size_t pos = 1;
    bool in_time = false;
    int64_t parsed_y = 0, parsed_m = 0, parsed_d;
    int64_t parsed_h = 0, parsed_min = 0;
    double parsed_sec = 0.0;
    int64_t sign = 1;
    // A bare 'P'/'PT' is a valid zero interval; 'P-' is malformed.
    if (pos < s.size() && (s[pos] == '-' || s[pos] == '+')) {
      const std::string rest(s.substr(pos + 1));
      const bool digit_follows =
          !rest.empty() &&
          std::isdigit(static_cast<unsigned char>(rest[0])) != 0;
      if (!digit_follows) {
        return fail("'" + s + "': missing components");
      }
      if (s[pos] == '-') {
        sign = -1;
      }
      ++pos;
    }
    while (pos < s.size()) {
      if (s[pos] == 'T' || s[pos] == 't') {
        in_time = true;
        ++pos;
        continue;
      }
      if (s[pos] == '-' || s[pos] == '+') {
        sign = (s[pos] == '-') ? -1 : 1;
        ++pos;
      }
      size_t num_start = pos;
      while (pos < s.size() &&
             ((std::isdigit(static_cast<unsigned char>(s[pos])) != 0) ||
              s[pos] == '.')) {
        ++pos;
      }
      if (num_start == pos) {
        if (pos < s.size()) {
          return fail("'" + s + "': Expected number at '" + s.substr(pos) +
                      "'");
        }
        return fail("'" + s + "': Expected number at end of string");
      }
      std::string num_str = s.substr(num_start, pos - num_start);
      if (pos >= s.size()) {
        return fail("'" + s + "': Expected designator at end of string");
      }
      char desig =
          static_cast<char>(std::toupper(static_cast<unsigned char>(s[pos++])));
      double val = std::stod(num_str) * static_cast<double>(sign);
      // C++20 makes an out-of-range double-to-integral cast undefined; a
      // literal like P9e300Y must be rejected, not cast to garbage.
      if (val < -9.3e18 || val > 9.3e18) {
        return fail_range();
      }
      sign = 1;
      if (!in_time) {
        if (desig == 'Y') {
          parsed_y = static_cast<int64_t>(val);
        } else if (desig == 'M') {
          parsed_m = static_cast<int64_t>(val);
        } else if (desig == 'D') {
          parsed_d = static_cast<int64_t>(val);
        } else {
          return fail("'" + s + "': Unexpected '" + std::string(1, desig) +
                      "'");
        }
      } else {
        if (desig == 'H') {
          parsed_h = static_cast<int64_t>(val);
        } else if (desig == 'M') {
          parsed_min = static_cast<int64_t>(val);
        } else if (desig == 'S') {
          parsed_sec = val;
        } else {
          return fail("'" + s + "': Unexpected '" + std::string(1, desig) +
                      "'");
        }
      }
    }
    // The checked_mul/checked_add helpers defined at function scope cover
    // the ISO-8601 designator path: a plain int64 multiply here used to
    // wrap a literal like P999999999999Y into a negative interval instead
    // of reporting an out-of-range value.
    int64_t tot_months = checked_add(checked_mul(parsed_y, 12), parsed_m);
    const int64_t hour_nanos = checked_mul(
        checked_add(checked_mul(parsed_h, 3600), checked_mul(parsed_min, 60)),
        1000000000LL);
    const double sec_nanos = std::round(parsed_sec * 1000000000.0);
    if (sec_nanos < -9.2e18 || sec_nanos > 9.2e18) {
      return fail_range();
    }
    int64_t tot_nanos =
        checked_add(hour_nanos, static_cast<int64_t>(sec_nanos));
    return checked_return(IntervalValue{
        .months = tot_months, .days = parsed_d, .nanos = tot_nanos});
  }

  // Single unit conversions
  if (!u.empty() && u.find(" to ") == std::string::npos) {
    // Parse integral amounts exactly (std::stod loses precision beyond
    // 2^53 and its double -> int64 cast wraps at 2^63, turning
    // INTERVAL '9223372036854775806' DAY into a negative interval).
    int64_t ival = 0;
    const bool integral = [&] {
      const char* begin = s.data();
      const char* end = s.data() + s.size();
      if (begin == end) {
        return false;
      }
      size_t digits = 0;
      for (const char c : s) {
        if (std::isdigit(static_cast<unsigned char>(c)) != 0) {
          ++digits;
        }
      }
      const auto [ptr, ec] = std::from_chars(begin, end, ival);
      return ec == std::errc() && ptr == end &&
             std::cmp_equal(digits, end - begin);
    }();
    if (!integral) {
      double dval = 0.0;
      try {
        dval = std::stod(s);
      } catch (...) {
        dval = 0.0;
      }
      const auto to_int64 = [&](double v) -> int64_t {
        const double rounded = std::round(v);
        if (rounded >= 9223372036854775808.0 ||
            rounded < -9223372036854775808.0) {
          overflowed = true;
          return 0;
        }
        return static_cast<int64_t>(rounded);
      };
      if (u == "year" || u == "years") {
        return checked_return(IntervalValue{
            .months = to_int64(dval * 12), .days = 0, .nanos = 0});
      }
      if (u == "quarter" || u == "quarters") {
        return checked_return(
            IntervalValue{.months = to_int64(dval * 3), .days = 0, .nanos = 0});
      }
      if (u == "month" || u == "months") {
        return checked_return(
            IntervalValue{.months = to_int64(dval), .days = 0, .nanos = 0});
      }
      if (u == "week" || u == "weeks") {
        return checked_return(
            IntervalValue{.months = 0, .days = to_int64(dval * 7), .nanos = 0});
      }
      if (u == "day" || u == "days") {
        return checked_return(
            IntervalValue{.months = 0, .days = to_int64(dval), .nanos = 0});
      }
      if (u == "hour" || u == "hours") {
        return checked_return(IntervalValue{
            .months = 0, .days = 0, .nanos = to_int64(dval * 3600.0 * 1e9)});
      }
      if (u == "minute" || u == "minutes") {
        return checked_return(IntervalValue{
            .months = 0, .days = 0, .nanos = to_int64(dval * 60.0 * 1e9)});
      }
      if (u == "second" || u == "seconds") {
        return checked_return(IntervalValue{
            .months = 0, .days = 0, .nanos = to_int64(dval * 1e9)});
      }
      if (u == "millisecond" || u == "milliseconds") {
        return checked_return(IntervalValue{
            .months = 0, .days = 0, .nanos = to_int64(dval * 1e6)});
      }
      if (u == "microsecond" || u == "microseconds") {
        return checked_return(IntervalValue{
            .months = 0, .days = 0, .nanos = to_int64(dval * 1e3)});
      }
      if (u == "nanosecond" || u == "nanoseconds") {
        return checked_return(
            IntervalValue{.months = 0, .days = 0, .nanos = to_int64(dval)});
      }
    }
    // Each unit conversion checks its multiply up front; checked_return
    // turns a flagged multiply into the same out-of-range error the old
    // ternary-throw form produced.
    const auto mul = [&](int64_t factor) -> int64_t {
      int64_t scaled = 0;
      overflowed |= __builtin_mul_overflow(ival, factor, &scaled);
      return scaled;
    };
    if (u == "year" || u == "years") {
      return checked_return(
          IntervalValue{.months = mul(12), .days = 0, .nanos = 0});
    }
    if (u == "quarter" || u == "quarters") {
      return checked_return(
          IntervalValue{.months = mul(3), .days = 0, .nanos = 0});
    }
    if (u == "month" || u == "months") {
      return IntervalValue{.months = ival, .days = 0, .nanos = 0};
    }
    if (u == "week" || u == "weeks") {
      return checked_return(
          IntervalValue{.months = 0, .days = mul(7), .nanos = 0});
    }
    if (u == "day" || u == "days") {
      return IntervalValue{.months = 0, .days = ival, .nanos = 0};
    }
    if (u == "hour" || u == "hours") {
      return checked_return(IntervalValue{
          .months = 0, .days = 0, .nanos = mul(int64_t{3600} * 1000000000LL)});
    }
    if (u == "minute" || u == "minutes") {
      return checked_return(IntervalValue{
          .months = 0, .days = 0, .nanos = mul(int64_t{60} * 1000000000LL)});
    }
    if (u == "second" || u == "seconds") {
      return checked_return(IntervalValue{
          .months = 0, .days = 0, .nanos = mul(int64_t{1000000000LL})});
    }
    if (u == "millisecond" || u == "milliseconds") {
      return checked_return(IntervalValue{
          .months = 0, .days = 0, .nanos = mul(int64_t{1000000})});
    }
    if (u == "microsecond" || u == "microseconds") {
      return checked_return(
          IntervalValue{.months = 0, .days = 0, .nanos = mul(int64_t{1000})});
    }
    if (u == "nanosecond" || u == "nanoseconds") {
      return IntervalValue{.months = 0, .days = 0, .nanos = ival};
    }
  }

  // Composite string (e.g. "1-2 3 4:5:6.789", "0-0 29 24:0:0", "-20 30",
  // "-4:5:6.789", etc.)
  if (u.empty() && s.find(':') == std::string::npos &&
      s.find('-') == std::string::npos && s.find(' ') == std::string::npos) {
    // A bare number carries no unit information.
    return fail("'" + s + "'");
  }
  std::istringstream iss(s);
  std::vector<std::string> parts;
  std::string tok;
  while (iss >> tok) {
    parts.push_back(tok);
  }

  IntervalValue iv;
  // Explicit unit ranges ("month to hour", "day to second", ...) assign one
  // component per whitespace-separated number starting at the low unit.
  if (u.find(" to ") != std::string::npos) {
    static constexpr std::array<std::string_view, 6> kUnitNames = {
        "year", "month", "day", "hour", "minute", "second"};
    const std::string& range = u;
    const size_t split = range.find(" to ");
    const std::string lo_unit = range.substr(0, split);
    const std::string hi_unit = range.substr(split + 4);
    auto unit_index = [&](const std::string& name) -> int {
      std::string base = name;
      while (!base.empty() && base.back() == 's') {
        base.pop_back();
      }
      for (int i = 5; i >= 0; --i) {
        if (base.starts_with(kUnitNames[static_cast<size_t>(i)])) {
          return i;
        }
      }
      return -1;
    };
    const int fi = unit_index(lo_unit);
    const int ti = unit_index(hi_unit);
    if (fi >= 0 && ti >= fi &&
        static_cast<int>(parts.size()) == (ti - fi + 1)) {
      bool ok = true;
      for (size_t idx = 0; idx < parts.size(); ++idx) {
        const std::string& p = parts[idx];
        const int unit = fi + static_cast<int>(idx);
        if (p.find(':') != std::string::npos) {
          int64_t th = 0, tm = 0;
          double ts = 0.0;
          // The integer fields cannot report conversion failures, but a
          // partially parsed time component is rejected by the != 3 check.
          // NOLINTNEXTLINE(cert-err34-c)
          if (sscanf(p.c_str(), "%ld:%ld:%lf", &th, &tm, &ts) != 3) {
            ok = false;
            break;
          }
          iv.nanos += static_cast<int64_t>(std::round(
              (static_cast<double>((th * 3600) + (tm * 60)) + ts) * 1e9));
          continue;
        }
        int64_t val = 0;
        try {
          val = std::stoll(p);
        } catch (...) {
          ok = false;
          break;
        }
        switch (unit) {
          case 0: {
            int64_t years = 0;
            if (__builtin_mul_overflow(val, 12, &years) ||
                __builtin_add_overflow(iv.months, years, &iv.months)) {
              ok = false;
              break;
            }
            break;
          }
          case 1:
            if (__builtin_add_overflow(iv.months, val, &iv.months)) {
              ok = false;
              break;
            }
            break;
          case 2:
            if (__builtin_add_overflow(iv.days, val, &iv.days)) {
              ok = false;
              break;
            }
            break;
          case 3: {
            int64_t scaled = 0;
            if (__builtin_mul_overflow(val, 3600LL * 1000000000LL, &scaled) ||
                __builtin_add_overflow(iv.nanos, scaled, &iv.nanos)) {
              ok = false;
              break;
            }
            break;
          }
          case 4: {
            int64_t scaled = 0;
            if (__builtin_mul_overflow(val, 60LL * 1000000000LL, &scaled) ||
                __builtin_add_overflow(iv.nanos, scaled, &iv.nanos)) {
              ok = false;
              break;
            }
            break;
          }
          case 5: {
            int64_t scaled = 0;
            if (__builtin_mul_overflow(val, 1000000000LL, &scaled) ||
                __builtin_add_overflow(iv.nanos, scaled, &iv.nanos)) {
              ok = false;
              break;
            }
            break;
          }
          default:
            ok = false;
            break;
        }
      }
      if (ok) {
        return iv;
      }
      iv = IntervalValue{};
    }
  }
  for (size_t idx = 0; idx < parts.size(); ++idx) {
    const auto& p = parts[idx];
    if (p.find(':') != std::string::npos) {
      // H:M:S[.frac]
      int64_t sign = 1;
      std::string time_part = p;
      if (time_part.front() == '-') {
        sign = -1;
        time_part.erase(0, 1);
      }
      int64_t th = 0, tm = 0;
      double ts = 0.0;
      // Failed conversions leave the zero-initialized fields, which is the
      // accepted parse result for malformed composite parts here.
      sscanf(  // NOLINT(cert-err33-c,cert-err34-c)
          time_part.c_str(), "%ld:%ld:%lf", &th, &tm, &ts);
      auto s_int = static_cast<int64_t>(ts);
      double s_frac = ts - static_cast<double>(s_int);
      auto sub_ns = static_cast<int64_t>(std::round(s_frac * 1e9));
      const int64_t day_seconds = checked_add(
          checked_mul(th, 3600), checked_add(checked_mul(tm, 60), s_int));
      iv.nanos = checked_add(
          iv.nanos,
          checked_mul(
              checked_add(checked_mul(day_seconds, 1000000000LL), sub_ns),
              sign));
    } else if (p.find('-') != std::string::npos &&
               (p.size() > 1 && p.find_last_of('-') != 0)) {
      // Y-M
      int64_t sign = 1;
      std::string ym_part = p;
      if (ym_part.front() == '-') {
        sign = -1;
        ym_part.erase(0, 1);
      }
      size_t dash = ym_part.find('-');
      int64_t y = 0, m = 0;
      if (dash != std::string::npos && dash > 0 && dash + 1 < ym_part.size()) {
        try {
          y = std::stoll(ym_part.substr(0, dash));
          m = std::stoll(ym_part.substr(dash + 1));
        } catch (...) {
          y = 0;
          m = 0;
        }
      }
      iv.months = checked_add(
          iv.months, checked_mul(checked_add(checked_mul(y, 12), m), sign));
    } else {
      // Numerical part
      int64_t val = 0;
      try {
        val = std::stoll(p);
      } catch (...) {
        val = 0;
      }
      if (parts.size() == 3) {
        if (idx == 1) {
          iv.days = checked_add(iv.days, val);
        } else if (idx == 0) {
          iv.months = checked_add(iv.months, checked_mul(val, 12));
        }
      } else if (parts.size() == 2) {
        if (u == "month to day") {  // NOLINT(bugprone-branch-clone)
          if (idx == 0) {
            iv.months = checked_add(iv.months, val);
          } else {
            iv.days = checked_add(iv.days, val);
          }
        } else if (u == "year to month") {
          if (idx == 0) {
            iv.months = checked_add(iv.months, checked_mul(val, 12));
          } else {
            iv.months = checked_add(iv.months, val);
          }
        } else if (parts[0].find('-') != std::string::npos ||
                   parts[1].find(':') != std::string::npos) {
          iv.days = checked_add(iv.days, val);
        } else {
          if (idx == 0) {
            iv.months = checked_add(iv.months, val);
          } else {
            iv.days = checked_add(iv.days, val);
          }
        }
      } else {
        // The unit prefixes intentionally share the same accumulation shape.
        // NOLINTNEXTLINE(bugprone-branch-clone)
        if (u.starts_with("year")) {
          iv.months = checked_add(iv.months, checked_mul(val, 12));
        } else if (u.starts_with("month")) {
          iv.months = checked_add(iv.months, val);
        } else if (u.starts_with("hour")) {
          iv.nanos =
              checked_add(iv.nanos, checked_mul(val, 3600LL * 1000000000LL));
        } else if (u.starts_with("minute")) {
          iv.nanos =
              checked_add(iv.nanos, checked_mul(val, 60LL * 1000000000LL));
        } else {
          iv.days = checked_add(iv.days, val);
        }
      }
    }
  }
  return checked_return(iv);
}

}  // namespace tinylamb
