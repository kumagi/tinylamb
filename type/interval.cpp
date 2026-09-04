/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "type/interval.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace tinylamb {

namespace {

thread_local std::unordered_map<std::string, std::string> tls_session_constants;

std::string Trim(std::string_view s) {
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
    s.remove_prefix(1);
  }
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
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

IntervalValue IntervalValue::JustifyHours() const {
  constexpr int64_t kDayNanos = 24LL * 3600LL * 1000000000LL;
  // Overflow-checked: days * kDayNanos previously wrapped silently on large
  // parseable intervals (e.g. P4000000000D) and produced garbage results.
  int64_t days_part = 0;
  int64_t total_nanos = nanos;
  if (__builtin_mul_overflow(days, kDayNanos, &days_part) ||
      __builtin_add_overflow(total_nanos, days_part, &total_nanos)) {
    throw std::runtime_error("INTERVAL computation out of range");
  }
  bool neg = (total_nanos < 0);
  int64_t abs_nanos = std::abs(total_nanos);

  int64_t res_days = abs_nanos / kDayNanos;
  int64_t res_nanos = abs_nanos % kDayNanos;

  if (neg) {
    return {months, -res_days, -res_nanos};
  }
  return {months, res_days, res_nanos};
}

IntervalValue IntervalValue::JustifyDays() const {
  int64_t total_days = 0;
  if (__builtin_mul_overflow(months, int64_t{30}, &total_days) ||
      __builtin_add_overflow(total_days, days, &total_days)) {
    throw std::runtime_error("INTERVAL computation out of range");
  }
  bool neg = (total_days < 0);
  int64_t abs_days = std::abs(total_days);

  int64_t res_months = abs_days / 30;
  int64_t res_days = abs_days % 30;

  if (neg) {
    return {-res_months, -res_days, nanos};
  }
  return {res_months, res_days, nanos};
}

IntervalValue IntervalValue::JustifyInterval() const {
  constexpr int64_t kDayNanos = 24LL * 3600LL * 1000000000LL;
  constexpr int64_t kMonthNanos = 30LL * kDayNanos;

  // Overflow-checked via TotalNanos().
  int64_t total_nanos = TotalNanos();
  bool neg = (total_nanos < 0);
  int64_t abs_nanos = std::abs(total_nanos);

  int64_t res_months = abs_nanos / kMonthNanos;
  int64_t rem_after_months = abs_nanos % kMonthNanos;

  int64_t res_days = rem_after_months / kDayNanos;
  int64_t res_nanos = rem_after_months % kDayNanos;

  if (neg) {
    return {-res_months, -res_days, -res_nanos};
  }
  return {res_months, res_days, res_nanos};
}

std::string IntervalValue::ToString() const {
  int64_t y = months / 12;
  int64_t m = months % 12;
  int64_t total_sec = nanos / 1000000000LL;
  int64_t sub_ns = nanos % 1000000000LL;
  int64_t h = total_sec / 3600;
  int64_t min = (total_sec % 3600) / 60;
  int64_t s = total_sec % 60;

  char ym_buf[64];
  if (months < 0) {
    snprintf(ym_buf, sizeof(ym_buf), "-%ld-%ld", std::abs(y), std::abs(m));
  } else {
    snprintf(ym_buf, sizeof(ym_buf), "%ld-%ld", y, m);
  }

  char d_buf[32];
  snprintf(d_buf, sizeof(d_buf), "%ld", days);

  char t_buf[64];
  bool neg_time = (nanos < 0);
  int64_t abs_h = std::abs(h);
  int64_t abs_min = std::abs(min);
  int64_t abs_s = std::abs(s);
  int64_t abs_sub_ns = std::abs(sub_ns);

  if (abs_sub_ns != 0) {
    char frac[16];
    snprintf(frac, sizeof(frac), "%09ld", abs_sub_ns);
    size_t end = 9;
    while (end > 0 && frac[end - 1] == '0') {
      --end;
    }
    frac[end] = '\0';
    snprintf(t_buf, sizeof(t_buf), "%s%ld:%ld:%ld.%s", neg_time ? "-" : "",
             abs_h, abs_min, abs_s, frac);
  } else {
    snprintf(t_buf, sizeof(t_buf), "%s%ld:%ld:%ld", neg_time ? "-" : "", abs_h,
             abs_min, abs_s);
  }

  return std::string(ym_buf) + " " + d_buf + " " + t_buf;
}

IntervalValue IntervalValue::Parse(std::string_view text,
                                   std::string_view unit_str) {
  std::string s = Trim(text);
  std::string u = ToLower(Trim(unit_str));
  if (s.empty()) {
    return {};
  }

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
        throw std::runtime_error(
            "generic::out_of_range: Invalid INTERVAL value '" + s +
            "': missing components");
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
      while (
          pos < s.size() &&
          (std::isdigit(static_cast<unsigned char>(s[pos])) || s[pos] == '.')) {
        ++pos;
      }
      if (num_start == pos) {
        if (pos < s.size()) {
          throw std::runtime_error(
              "generic::out_of_range: Invalid INTERVAL value '" + s +
              "': Expected number at '" + s.substr(pos) + "'");
        }
        throw std::runtime_error(
            "generic::out_of_range: Invalid INTERVAL value '" + s +
            "': Expected number at end of string");
      }
      std::string num_str = s.substr(num_start, pos - num_start);
      if (pos >= s.size()) {
        throw std::runtime_error(
            "generic::out_of_range: Invalid INTERVAL value '" + s +
            "': Expected designator at end of string");
      }
      char desig =
          static_cast<char>(std::toupper(static_cast<unsigned char>(s[pos++])));
      double val = std::stod(num_str) * sign;
      // C++20 makes an out-of-range double-to-integral cast undefined; a
      // literal like P9e300Y must be rejected, not cast to garbage.
      if (!(val >= -9.3e18 && val <= 9.3e18)) {
        throw std::runtime_error(
            "generic::out_of_range: INTERVAL value out of range");
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
          throw std::runtime_error(
              "generic::out_of_range: Invalid INTERVAL value '" + s +
              "': Unexpected '" + std::string(1, desig) + "'");
        }
      } else {
        if (desig == 'H') {
          parsed_h = static_cast<int64_t>(val);
        } else if (desig == 'M') {
          parsed_min = static_cast<int64_t>(val);
        } else if (desig == 'S') {
          parsed_sec = val;
        } else {
          throw std::runtime_error(
              "generic::out_of_range: Invalid INTERVAL value '" + s +
              "': Unexpected '" + std::string(1, desig) + "'");
        }
      }
    }
    // Saturate instead of overflowing: the ISO-8601 designator path used a
    // plain int64 multiply, so a literal like P999999999999Y wrapped into a
    // negative interval rather than reporting an out-of-range value.
    const auto checked_mul = [](int64_t a, int64_t b) -> int64_t {
      int64_t result = 0;
      if (__builtin_mul_overflow(a, b, &result)) {
        throw std::runtime_error(
            "generic::out_of_range: INTERVAL value out of range");
      }
      return result;
    };
    const auto checked_add = [](int64_t a, int64_t b) -> int64_t {
      int64_t result = 0;
      if (__builtin_add_overflow(a, b, &result)) {
        throw std::runtime_error(
            "generic::out_of_range: INTERVAL value out of range");
      }
      return result;
    };
    int64_t tot_months = checked_add(checked_mul(parsed_y, 12), parsed_m);
    const int64_t hour_nanos = checked_mul(
        checked_add(checked_mul(parsed_h, 3600), checked_mul(parsed_min, 60)),
        1000000000LL);
    const double sec_nanos = std::round(parsed_sec * 1000000000.0);
    if (!(sec_nanos >= -9.2e18 && sec_nanos <= 9.2e18)) {
      throw std::runtime_error(
          "generic::out_of_range: INTERVAL value out of range");
    }
    int64_t tot_nanos =
        checked_add(hour_nanos, static_cast<int64_t>(sec_nanos));
    return {tot_months, parsed_d, tot_nanos};
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
        if (std::isdigit(static_cast<unsigned char>(c))) {
          ++digits;
        }
      }
      const auto [ptr, ec] = std::from_chars(begin, end, ival);
      return ec == std::errc() && ptr == end &&
             digits == static_cast<size_t>(end - begin);
    }();
    if (!integral) {
      double dval = 0.0;
      try {
        dval = std::stod(s);
      } catch (...) {
        dval = 0.0;
      }
      auto to_int64 = [](double v) -> int64_t {
        const double rounded = std::round(v);
        if (rounded >= 9223372036854775808.0 ||
            rounded < -9223372036854775808.0) {
          throw std::runtime_error(
              "generic::out_of_range: INTERVAL value out of range");
        }
        return static_cast<int64_t>(rounded);
      };
      if (u == "year" || u == "years") {
        return {to_int64(dval * 12), 0, 0};
      }
      if (u == "quarter" || u == "quarters") {
        return {to_int64(dval * 3), 0, 0};
      }
      if (u == "month" || u == "months") {
        return {to_int64(dval), 0, 0};
      }
      if (u == "week" || u == "weeks") {
        return {0, to_int64(dval * 7), 0};
      }
      if (u == "day" || u == "days") {
        return {0, to_int64(dval), 0};
      }
      if (u == "hour" || u == "hours") {
        return {0, 0, to_int64(dval * 3600.0 * 1e9)};
      }
      if (u == "minute" || u == "minutes") {
        return {0, 0, to_int64(dval * 60.0 * 1e9)};
      }
      if (u == "second" || u == "seconds") {
        return {0, 0, to_int64(dval * 1e9)};
      }
      if (u == "millisecond" || u == "milliseconds") {
        return {0, 0, to_int64(dval * 1e6)};
      }
      if (u == "microsecond" || u == "microseconds") {
        return {0, 0, to_int64(dval * 1e3)};
      }
      if (u == "nanosecond" || u == "nanoseconds") {
        return {0, 0, to_int64(dval)};
      }
    }
    if (u == "year" || u == "years") {
      return {__builtin_mul_overflow(ival, int64_t{12}, &ival)
                  ? throw std::runtime_error(
                        "generic::out_of_range: INTERVAL value out of range")
                  : ival,
              0, 0};
    }
    if (u == "quarter" || u == "quarters") {
      return {__builtin_mul_overflow(ival, int64_t{3}, &ival)
                  ? throw std::runtime_error(
                        "generic::out_of_range: INTERVAL value out of range")
                  : ival,
              0, 0};
    }
    if (u == "month" || u == "months") {
      return {ival, 0, 0};
    }
    if (u == "week" || u == "weeks") {
      return {0,
              __builtin_mul_overflow(ival, int64_t{7}, &ival)
                  ? throw std::runtime_error(
                        "generic::out_of_range: INTERVAL value out of range")
                  : ival,
              0};
    }
    if (u == "day" || u == "days") {
      return {0, ival, 0};
    }
    if (u == "hour" || u == "hours") {
      return {0, 0,
              __builtin_mul_overflow(ival, int64_t{3600} * 1000000000LL, &ival)
                  ? throw std::runtime_error(
                        "generic::out_of_range: INTERVAL value out of "
                        "range")
                  : ival};
    }
    if (u == "minute" || u == "minutes") {
      return {0, 0,
              __builtin_mul_overflow(ival, int64_t{60} * 1000000000LL, &ival)
                  ? throw std::runtime_error(
                        "generic::out_of_range: INTERVAL value out of "
                        "range")
                  : ival};
    }
    if (u == "second" || u == "seconds") {
      return {0, 0,
              __builtin_mul_overflow(ival, int64_t{1000000000LL}, &ival)
                  ? throw std::runtime_error(
                        "generic::out_of_range: INTERVAL value out of "
                        "range")
                  : ival};
    }
    if (u == "millisecond" || u == "milliseconds") {
      return {0, 0,
              __builtin_mul_overflow(ival, int64_t{1000000}, &ival)
                  ? throw std::runtime_error(
                        "generic::out_of_range: INTERVAL value out of "
                        "range")
                  : ival};
    }
    if (u == "microsecond" || u == "microseconds") {
      return {0, 0,
              __builtin_mul_overflow(ival, int64_t{1000}, &ival)
                  ? throw std::runtime_error(
                        "generic::out_of_range: INTERVAL value out of "
                        "range")
                  : ival};
    }
    if (u == "nanosecond" || u == "nanoseconds") {
      return {0, 0, ival};
    }
  }

  // Composite string (e.g. "1-2 3 4:5:6.789", "0-0 29 24:0:0", "-20 30",
  // "-4:5:6.789", etc.)
  if (u.empty() && s.find(':') == std::string::npos &&
      s.find('-') == std::string::npos && s.find(' ') == std::string::npos) {
    // A bare number carries no unit information.
    throw std::runtime_error("generic::out_of_range: Invalid INTERVAL value '" +
                             s + "'");
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
    static const char* kUnitNames[] = {"year", "month",  "day",
                                       "hour", "minute", "second"};
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
        if (base.rfind(kUnitNames[i], 0) == 0) {
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
          sscanf(p.c_str(), "%ld:%ld:%lf", &th, &tm, &ts);
          iv.nanos += static_cast<int64_t>(
              std::round((th * 3600 + tm * 60 + ts) * 1e9));
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
            if (__builtin_mul_overflow(val, 12, &years)) {
              ok = false;
              break;
            }
            iv.months += years;
            break;
          }
          case 1:
            iv.months += val;
            break;
          case 2:
            iv.days += val;
            break;
          case 3: {
            int64_t scaled = 0;
            if (__builtin_mul_overflow(val, 3600LL * 1000000000LL, &scaled)) {
              ok = false;
              break;
            }
            iv.nanos += scaled;
            break;
          }
          case 4: {
            int64_t scaled = 0;
            if (__builtin_mul_overflow(val, 60LL * 1000000000LL, &scaled)) {
              ok = false;
              break;
            }
            iv.nanos += scaled;
            break;
          }
          case 5: {
            int64_t scaled = 0;
            if (__builtin_mul_overflow(val, 1000000000LL, &scaled)) {
              ok = false;
              break;
            }
            iv.nanos += scaled;
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
      sscanf(time_part.c_str(), "%ld:%ld:%lf", &th, &tm, &ts);
      int64_t s_int = static_cast<int64_t>(ts);
      double s_frac = ts - s_int;
      int64_t sub_ns = static_cast<int64_t>(std::round(s_frac * 1e9));
      iv.nanos +=
          ((th * 3600 + tm * 60 + s_int) * 1000000000LL + sub_ns) * sign;
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
      iv.months += (y * 12 + m) * sign;
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
          iv.days += val;
        } else if (idx == 0) {
          iv.months += val * 12;
        }
      } else if (parts.size() == 2) {
        if (u == "month to day") {  // NOLINT(bugprone-branch-clone)
          if (idx == 0) {
            iv.months += val;
          } else {
            iv.days += val;
          }
        } else if (u == "year to month") {
          if (idx == 0) {
            iv.months += val * 12;
          } else {
            iv.months += val;
          }
        } else if (parts[0].find('-') != std::string::npos ||
                   parts[1].find(':') != std::string::npos) {
          iv.days += val;
        } else {
          if (idx == 0) {
            iv.months += val;
          } else {
            iv.days += val;
          }
        }
      } else {
        // The unit prefixes intentionally share the same accumulation shape.
        // NOLINTNEXTLINE(bugprone-branch-clone)
        if (u.starts_with("year")) {
          iv.months += val * 12;
        } else if (u.starts_with("month")) {
          iv.months += val;
        } else if (u.starts_with("hour")) {
          iv.nanos += val * 3600LL * 1000000000LL;
        } else if (u.starts_with("minute")) {
          iv.nanos += val * 60LL * 1000000000LL;
        } else {
          iv.days += val;
        }
      }
    }
  }
  return iv;
}

}  // namespace tinylamb
