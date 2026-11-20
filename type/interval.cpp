/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "type/interval.hpp"

#include <array>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <cinttypes>
#include <cmath>
#include <cstdlib>
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
#include "common/string_util.hpp"

namespace tinylamb {

namespace {
// Smallest/largest doubles that still convert to int64_t without undefined
// behaviour.  The old ±9.3e18 guards let values in (INT64_MAX, 9.3e18] reach
// the cast (UBSan: "9.24222e+18 is outside the range of representable values
// of type 'long'", found by INTERVAL fuzzing).
constexpr double kInt64MinAsDouble = -9223372036854775808.0;  // -2^63 exact
constexpr double kInt64MaxExclusive = 9223372036854775808.0;  // 2^63

// Index of a unit name inside a composite "X to Y" range (0=year ..
// 5=second), or -1. Trailing 's' letters are stripped for the plural
// spelling, then the name must match EXACTLY: a prefix match would let
// "yearxyz to month" parse as a year-month range.
int RangeUnitIndex(std::string_view name) {
  std::string base(name);
  while (!base.empty() && base.back() == 's') {
    base.pop_back();
  }
  static constexpr std::array<std::string_view, 6> kNames = {
      "year", "month", "day", "hour", "minute", "second"};
  for (int i = 5; i >= 0; --i) {
    if (base == kNames[static_cast<size_t>(i)]) {
      return i;
    }
  }
  return -1;
}
}  // namespace

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
  // Key on the ASCII-folded name, matching SessionConstantExpressions()
  // (which keys on Lower(name)): the two ledgers must agree about case or a
  // mixed-case CREATE CONSTANT resolves differently depending on which one
  // answers first.
  tls_session_constants[AsciiToLower(name)] = std::string(value);
}

std::string GetSessionConstant(std::string_view name) {
  auto it = tls_session_constants.find(AsciiToLower(name));
  if (it != tls_session_constants.end()) {
    return it->second;
  }
  return "";
}

bool HasSessionConstant(std::string_view name) {
  return tls_session_constants.contains(AsciiToLower(name));
}

void ClearSessionConstants() { tls_session_constants.clear(); }

StatusOr<IntervalValue> IntervalValue::TryJustifyHours() const {
  // GoogleSQL JUSTIFY_HOURS normalizes hours into [-23,23], carrying whole
  // days into the `days` component (months/years unchanged).
  constexpr int64_t kDayNanos = 24LL * 3600LL * 1000000000LL;
  int64_t days_part = 0;
  int64_t total_nanos = nanos;
  if (__builtin_mul_overflow(days, kDayNanos, &days_part) ||
      __builtin_add_overflow(total_nanos, days_part, &total_nanos)) {
    return StatusError(StatusCode::kInvalidArgument,
                       "INTERVAL computation out of range");
  }
  return IntervalValue{.months = months,
                       .days = total_nanos / kDayNanos,
                       .nanos = total_nanos % kDayNanos};
}

StatusOr<IntervalValue> IntervalValue::TryJustifyDays() const {
  // GoogleSQL JUSTIFY_DAYS normalizes days into [-29,29], carrying whole
  // months (30 days) into the `months` component.
  int64_t total_days = 0;
  if (__builtin_mul_overflow(months, int64_t{30}, &total_days) ||
      __builtin_add_overflow(total_days, days, &total_days)) {
    return StatusError(StatusCode::kInvalidArgument,
                       "INTERVAL computation out of range");
  }
  return IntervalValue{
      .months = total_days / 30, .days = total_days % 30, .nanos = nanos};
}

StatusOr<IntervalValue> IntervalValue::TryJustifyInterval() const {
  constexpr int64_t kDayNanos = 24LL * 3600LL * 1000000000LL;
  constexpr int64_t kMonthNanos = 30LL * kDayNanos;

  // Overflow-checked via TryTotalNanos().
  ASSIGN_OR_RETURN(int64_t, total_nanos, TryTotalNanos());
  const int64_t rem_after_months = total_nanos % kMonthNanos;
  return IntervalValue{.months = total_nanos / kMonthNanos,
                       .days = rem_after_months / kDayNanos,
                       .nanos = rem_after_months % kDayNanos};
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
    CheckedSnprintf(ym_buf.data(), ym_buf.size(), "-%" PRId64 "-%" PRId64,
                    static_cast<int64_t>(std::abs(y)),
                    static_cast<int64_t>(std::abs(m)));
  } else {
    CheckedSnprintf(ym_buf.data(), ym_buf.size(), "%" PRId64 "-%" PRId64, y, m);
  }

  std::array<char, 32> d_buf{};
  CheckedSnprintf(d_buf.data(), d_buf.size(), "%" PRId64, days);

  std::array<char, 64> t_buf{};
  bool neg_time = (nanos < 0);
  int64_t abs_h = std::abs(h);
  int64_t abs_min = std::abs(min);
  int64_t abs_s = std::abs(s);
  int64_t abs_sub_ns = std::abs(sub_ns);

  if (abs_sub_ns != 0) {
    std::array<char, 16> frac{};
    CheckedSnprintf(frac.data(), frac.size(), "%09" PRId64, abs_sub_ns);
    size_t end = 9;
    while (end > 0 && frac[end - 1] == '0') {
      --end;
    }
    frac[end] = '\0';
    CheckedSnprintf(t_buf.data(), t_buf.size(),
                    "%s%" PRId64 ":%" PRId64 ":%" PRId64 ".%s",
                    neg_time ? "-" : "", abs_h, abs_min, abs_s, frac.data());
  } else {
    CheckedSnprintf(t_buf.data(), t_buf.size(),
                    "%s%" PRId64 ":%" PRId64 ":%" PRId64, neg_time ? "-" : "",
                    abs_h, abs_min, abs_s);
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
    if (u.empty()) {
      return IntervalValue{};
    }
    // An empty amount still owes the unit string a validation pass:
    // TryParse("", "BOGUS") must not silently become a zero interval.
    // Parsing "0" routes through every unit check below and yields the
    // same zero result for a well-formed unit.
    s = "0";
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
    int64_t parsed_y = 0, parsed_m = 0, parsed_d = 0;
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
      // std::from_chars reports failure (errc::invalid_argument on "." alone,
      // errc::result_out_of_range on 400-digit runs) for scanner-accepted
      // input; reject through fail instead of unwinding.
      double parsed = 0;
      const char* num_begin = num_str.data();
      const char* num_end = num_begin + num_str.size();
      const auto [parse_end, parse_ec] =
          std::from_chars(num_begin, num_end, parsed);
      if (parse_ec != std::errc() || parse_end != num_end) {
        std::string message = "'" + s;
        message += "': Invalid numeric component '";
        message += num_str;
        message += "'";
        return fail(message);
      }
      double val = parsed * static_cast<double>(sign);
      // C++20 makes an out-of-range double-to-integral cast undefined; a
      // literal like P9e300Y must be rejected, not cast to garbage.  from_
      // chars also happily parses "nan"/"inf" spellings, which slip past
      // plain range comparisons.
      if (!std::isfinite(val) || val < kInt64MinAsDouble ||
          val >= kInt64MaxExclusive) {
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
    if (sec_nanos < kInt64MinAsDouble || sec_nanos >= kInt64MaxExclusive) {
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
      // Allow an optional leading sign: a negative integral literal must
      // take this exact path too, otherwise stod rounds beyond 2^53.
      const size_t sign = (begin < end && (*begin == '-' || *begin == '+')) ? 1 : 0;
      return ec == std::errc() && ptr == end &&
             std::cmp_equal(digits + sign, end - begin);
    }();
    if (!integral) {
      // Validate the WHOLE token: std::stod(s) accepts trailing garbage
      // ("3abc" -> 3) and throws on a non-numeric token, which used to be
      // swallowed as 0. A malformed amount must be an error, never a value.
      char* parse_end = nullptr;
      errno = 0;
      const double dval = std::strtod(s.c_str(), &parse_end);
      if (parse_end == s.c_str() ||
          parse_end != s.c_str() + s.size() || errno == ERANGE) {
        return fail("invalid INTERVAL amount: " + s);
      }
      const auto to_int64 = [&](double v) -> int64_t {
        const double rounded = std::round(v);
        if (!std::isfinite(rounded) || rounded >= 9223372036854775808.0 ||
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
  const bool bare_negative =
      !s.empty() && s[0] == '-' && s.find('-', 1) == std::string::npos;
  if (u.empty() && s.find(':') == std::string::npos &&
      s.find(' ') == std::string::npos &&
      (s.find('-') == std::string::npos || bare_negative)) {
    // A bare number (positive or just signed) carries no unit information.
    return fail("'" + s + "'");
  }
  std::istringstream iss(s);
  std::vector<std::string> parts;
  std::string tok;
  while (iss >> tok) {
    parts.push_back(tok);
  }

  // A single explicit unit scales one numeric value; a composite literal
  // (H:M:S or Y-M fields) must not silently ignore it and be read with a
  // different meaning (e.g. '1:2:3' DAY becoming one day). Only a full unit
  // range ("hour to second") is valid for a composite shape.
  // NOLINTNEXTLINE(performance-inefficient-string-concatenation) short fixed concatenations
  if (!u.empty() && u.find(" to ") == std::string::npos) {
    for (const std::string& part : parts) {
      const bool time_shaped = part.find(':') != std::string::npos;
      const bool ym_shaped =
          part.find('-') != std::string::npos && !part.starts_with('-');
      if (time_shaped || ym_shaped) {
        return fail("'" + s + "': Explicit unit '" + u +  // NOLINT(performance-inefficient-string-concatenation)
                    "' is not valid for a composite INTERVAL literal");
      }
    }
  }
  IntervalValue iv;
  // Explicit unit ranges ("month to hour", "day to second", ...) assign one
  // component per whitespace-separated number starting at the low unit.
  if (u.find(" to ") != std::string::npos) {
    const std::string& range = u;
    const size_t split = range.find(" to ");
    const std::string lo_unit = range.substr(0, split);
    const std::string hi_unit = range.substr(split + 4);
    const int fi = RangeUnitIndex(lo_unit);
    const int ti = RangeUnitIndex(hi_unit);
    if (fi >= 0 && ti >= fi &&
        static_cast<int>(parts.size()) == (ti - fi + 1)) {
      bool ok = true;
      for (size_t idx = 0; idx < parts.size(); ++idx) {
        const std::string& p = parts[idx];
        const int unit = fi + static_cast<int>(idx);
        if (p.find(':') != std::string::npos) {
          int64_t th = 0, tm = 0;
          double ts = 0.0;
          // The sign belongs to the whole H:M:S component: "-0:30:00" is
          // minus thirty minutes, but sscanf reads "-0" as plain 0. Strip it
          // and apply it below, as the generic composite branch does.
          const bool negative = !p.empty() && p.front() == '-';
          const std::string magnitude = negative ? p.substr(1) : p;
          // sscanf returns 3 even with trailing garbage; %n records the
          // consumed length so a partially parsed time component is rejected.
          int consumed = 0;
          // NOLINTNEXTLINE(cert-err34-c)
          const int converted =
              sscanf(magnitude.c_str(),  // NOLINT(cert-err34-c)
                     "%" SCNd64 ":%" SCNd64 ":%lf%n", &th, &tm, &ts,
                     &consumed);
          if (converted != 3 || consumed <= 0 ||
              static_cast<size_t>(consumed) != magnitude.size()) {
            ok = false;
            break;
          }
          // The nanoseconds product must stay representable: "nan"/"inf"
          // spellings and huge magnitudes would turn the cast below into
          // undefined behavior (UBSan-found via INTERVAL fuzzing).
          if (!std::isfinite(ts) || ts > 9.2e9 || ts < -9.2e9) {
            ok = false;
            break;
          }
          const auto seconds = static_cast<int64_t>(ts);
          const auto fraction = static_cast<int64_t>(
              std::round((ts - static_cast<double>(seconds)) * 1e9));
          const int64_t whole_seconds = checked_add(
              checked_mul(th, 3600), checked_add(checked_mul(tm, 60), seconds));
          const int64_t component_nanos =
              checked_add(checked_mul(whole_seconds, 1000000000LL), fraction);
          iv.nanos =
              checked_add(iv.nanos, negative ? checked_mul(component_nanos, -1)
                                             : component_nanos);
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
        return checked_return(iv);
      }
      iv = IntervalValue{};
    }
  }
  // A two-field H:M time component is complete only when the unit context
  // allows it: a bare composite literal, or a range ending at minutes
  // ("HOUR TO MINUTE"). "HOUR TO SECOND" still owes its seconds field.
  bool allow_two_field_time = u.empty();
  if (!allow_two_field_time) {
    if (const size_t sp = u.find(" to "); sp != std::string::npos) {
      allow_two_field_time = RangeUnitIndex(u.substr(sp + 4)) == 4;
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
      // sscanf returns a short count on partial input ("1:2" yields 2) and
      // happily stops before trailing garbage; %n records the consumed
      // length so a partially parsed time component is rejected instead of
      // silently becoming a shorter interval (same contract as the
      // unit-range arm above). %n is not reached on a short conversion, so
      // the two-field H:M spelling gets its own pass.
      int consumed = 0;
      const int converted =
          sscanf(time_part.c_str(),  // NOLINT(cert-err34-c)
                 "%" SCNd64 ":%" SCNd64 ":%lf%n", &th, &tm, &ts,
                 &consumed);
      bool time_ok = converted == 3 && consumed > 0 &&
                     static_cast<size_t>(consumed) == time_part.size();
      if (!time_ok && allow_two_field_time) {
        int consumed_hm = 0;
        int64_t hm_th = 0;
        int64_t hm_tm = 0;
        const int converted_hm =
            sscanf(time_part.c_str(),  // NOLINT(cert-err34-c)
                   "%" SCNd64 ":%" SCNd64 "%n", &hm_th, &hm_tm,
                   &consumed_hm);
        if (converted_hm == 2 && consumed_hm > 0 &&
            static_cast<size_t>(consumed_hm) == time_part.size()) {
          th = hm_th;
          tm = hm_tm;
          ts = 0.0;
          time_ok = true;
        }
      }
      if (!time_ok) {
        return fail("'" + s + "': malformed time component '" + p + "'");
      }
      // sscanf happily yields inf/1e300 for e.g. "0:0:1e999"; converting a
      // value outside int64 range (or NaN) is UB, not a wrap (fuzz-found via
      // UBSan: "inf is outside the range of representable values of long").
      if (!std::isfinite(ts) || ts < kInt64MinAsDouble ||
          ts >= kInt64MaxExclusive) {
        return fail_range();
      }
      auto s_int = static_cast<int64_t>(ts);
      double s_frac = ts - static_cast<double>(s_int);
      auto sub_ns = static_cast<int64_t>(std::round(s_frac * 1e9));
      const int64_t day_seconds = checked_add(
          checked_mul(th, 3600), checked_add(checked_mul(tm, 60), s_int));
      // Apply sign before combining whole-second nanos and sub-second nanos so
      // INT64_MIN (-2562047:47:16.854775808) does not overflow INT64_MAX on the
      // positive magnitude before negation.
      const int64_t signed_sec_nanos =
          checked_mul(checked_mul(day_seconds, sign), 1000000000LL);
      const int64_t signed_sub_ns = checked_mul(sub_ns, sign);
      iv.nanos =
          checked_add(iv.nanos, checked_add(signed_sec_nanos, signed_sub_ns));
    } else if (p.find('-') != std::string::npos &&
               (p.size() > 1 && p.find_last_of('-') != 0)) {
      // Y-M
      int64_t sign = 1;
      std::string ym_part = p;
      if (ym_part.front() == '-') {
        sign = -1;
        ym_part.erase(0, 1);
      }
      const size_t dash = ym_part.find('-');
      // Both sides must be plain unsigned integers consumed in full:
      // std::stoll used to accept "1-2abc" as y=1,m=2 (trailing garbage
      // dropped) and its catch-all turned total garbage into 0.
      const auto parse_component = [](std::string_view text,
                                      int64_t* out) -> bool {
        if (text.empty()) {
          return false;
        }
        for (const char c : text) {
          if (c < '0' || c > '9') {
            return false;
          }
        }
        const auto [end, ec] =
            std::from_chars(text.data(), text.data() + text.size(), *out);
        return ec == std::errc() && end == text.data() + text.size();
      };
      int64_t y = 0;
      int64_t m = 0;
      const std::string_view ym = ym_part;
      if (dash == std::string::npos ||
          !parse_component(ym.substr(0, dash), &y) ||
          !parse_component(ym.substr(dash + 1), &m)) {
        return fail("'" + s + "': malformed year-month component '" + p +
                    "'");
      }
      const int64_t signed_y_months = checked_mul(checked_mul(y, sign), 12);
      const int64_t signed_m = checked_mul(m, sign);
      iv.months =
          checked_add(iv.months, checked_add(signed_y_months, signed_m));
    } else {
      // Numerical part: the whole token must be one integer. std::stoll
      // used to accept "3junk" as 3 (prefix match) and its catch-all
      // turned total garbage into a silent 0.
      int64_t val = 0;
      char* num_end = nullptr;
      errno = 0;
      const long long parsed = std::strtoll(p.c_str(), &num_end, 10);
      if (num_end == p.c_str() || *num_end != '\0' || errno == ERANGE) {
        return fail("'" + s + "': invalid numeric component '" + p + "'");
      }
      val = static_cast<int64_t>(parsed);
      if (parts.size() == 3) {
        if (idx == 1) {
          iv.days = checked_add(iv.days, val);
        } else if (idx == 0) {
          iv.months = checked_add(iv.months, checked_mul(val, 12));
        } else {
          // The third composite field is H:M:S; a bare number there has no
          // place to go (it used to be dropped silently).
          std::string msg = "'";
          msg += s;
          msg += "': Unexpected component '";
          msg += p;
          msg += "'";
          return fail(msg);
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
        // A range unit names its accumulation by the LEADING unit only (the
        // span itself is validated after the loop); match it exactly --
        // starts_with used to let "yearxyz to month" accumulate as years.
        const int uidx =
            u.empty() ? 2 : RangeUnitIndex(u.substr(0, u.find(" to ")));
        // NOLINTNEXTLINE(bugprone-branch-clone)
        if (uidx == 0) {
          iv.months = checked_add(iv.months, checked_mul(val, 12));
        } else if (uidx == 1) {
          iv.months = checked_add(iv.months, val);
        } else if (uidx == 3) {
          iv.nanos =
              checked_add(iv.nanos, checked_mul(val, 3600LL * 1000000000LL));
        } else if (uidx == 4) {
          iv.nanos =
              checked_add(iv.nanos, checked_mul(val, 60LL * 1000000000LL));
        } else if (uidx == 2) {
          iv.days = checked_add(iv.days, val);
        } else {
          // An unknown unit must not silently become a day count.
          std::string msg = "'";
          msg += s;
          msg += "': Unsupported INTERVAL unit '";
          msg += u;
          msg += "'";
          return fail(msg);
        }
      }
    }
  }
  // A unit range ("hour to second") admits only components inside it; the
  // generic composite reading above can place a mismatched literal into
  // months/days ('1 2' HOUR TO SECOND became 1 month 2 days).
  if (const size_t split = u.find(" to "); split != std::string::npos) {
    const int lo = RangeUnitIndex(u.substr(0, split));
    const int hi = RangeUnitIndex(u.substr(split + 4));
    if (lo < 0 || hi < lo) {
      return fail("'" + s + "': Invalid INTERVAL unit range '" + u + "'");
    }
    const bool months_ok = lo <= 1;
    const bool days_ok = lo <= 2 && hi >= 2;
    const bool time_ok = hi >= 3;
    if ((iv.months != 0 && !months_ok) || (iv.days != 0 && !days_ok) ||
        (iv.nanos != 0 && !time_ok)) {
      return fail("'" + s + "': Value does not match INTERVAL " + u);
    }
  }
  return checked_return(iv);
}

}  // namespace tinylamb
