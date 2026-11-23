/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/expression_eval.hpp"

// The regexp_* branches below drive libstdc++'s <regex>, and under
// sanitizer-enabled -O2 inlining GCC 15 reports -Wmaybe-uninitialized for
// std::function state inside <regex_automaton.h>/<std_function.h> even though
// every std::regex object here is fully constructed before use. The false
// positive lives in the standard library, not in this file; suppress it for
// this translation unit only. The pragma must precede the includes it covers:
// the diagnostics are attributed to code instantiated from those headers.
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

#include <algorithm>
#include <array>
#include <bit>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <exception>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <ranges>
#if defined(__GNUC__) && !defined(__clang__)
// GCC 15 reports -Wmaybe-uninitialized inside <regex>'s NFA construction when
// instantiating std::regex in sanitizer builds; the library code is fine.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <regex>
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/digest.hpp"
#include "common/math_utils.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/platform/time_format.hpp"
#include "common/regex_guard.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/timestamp_trunc_kernel.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/json_doc.hpp"
#include "expression/json_text.hpp"
#include "expression/named_expression.hpp"
#include "expression/proto_text.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "type/collation.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
#include "type/interval.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {

// T2234/T2238: code-point split + Levenshtein for the batch evaluator;
// semantics (valid UTF-8 -> code points, invalid bytes pass through as the
// engine's BYTES spelling) are mirrored exactly from the canonical
// evaluator's CodePointsOf/LevenshteinCodePoints in
// expression/function_call_expression.cpp so differential evaluation of
// TO_CODE_POINTS / EDIT_DISTANCE cannot drift between lanes.
[[nodiscard]] std::optional<std::pair<uint32_t, size_t>> DecodeUtf8Pair(
    std::string_view value, size_t offset) {
  if (offset >= value.size()) {
    return std::nullopt;
  }
  const auto byte_at = [&](size_t index) {
    return static_cast<unsigned char>(value[index]);
  };
  const auto first = byte_at(offset);
  if (first <= 0x7F) {
    return std::pair<uint32_t, size_t>{first, 1};
  }
  if (first >= 0xC2 && first <= 0xDF && offset + 1 < value.size() &&
      (byte_at(offset + 1) & 0xC0) == 0x80) {
    return std::pair<uint32_t, size_t>{
        static_cast<uint32_t>(((first & 0x1F) << 6) |
                              (byte_at(offset + 1) & 0x3F)),
        2};
  }
  if (first >= 0xE0 && first <= 0xEF && offset + 2 < value.size() &&
      (byte_at(offset + 1) & 0xC0) == 0x80 &&
      (byte_at(offset + 2) & 0xC0) == 0x80) {
    const auto cp = static_cast<uint32_t>(((first & 0x0F) << 12) |
                                          ((byte_at(offset + 1) & 0x3F) << 6) |
                                          (byte_at(offset + 2) & 0x3F));
    if (cp < 0x800 || (cp >= 0xD800 && cp <= 0xDFFF)) {
      return std::nullopt;
    }
    return std::pair<uint32_t, size_t>{cp, 3};
  }
  if (first >= 0xF0 && first <= 0xF4 && offset + 3 < value.size() &&
      (byte_at(offset + 1) & 0xC0) == 0x80 &&
      (byte_at(offset + 2) & 0xC0) == 0x80 &&
      (byte_at(offset + 3) & 0xC0) == 0x80) {
    const auto cp = static_cast<uint32_t>(
        ((first & 0x07) << 18) | ((byte_at(offset + 1) & 0x3F) << 12) |
        ((byte_at(offset + 2) & 0x3F) << 6) | (byte_at(offset + 3) & 0x3F));
    if (cp < 0x10000 || cp > 0x10FFFF) {
      return std::nullopt;
    }
    return std::pair<uint32_t, size_t>{cp, 4};
  }
  return std::nullopt;
}

[[nodiscard]] std::vector<uint32_t> CodePointsOfBatch(std::string_view value) {
  std::vector<uint32_t> out;
  out.reserve(value.size());
  size_t offset = 0;
  while (offset < value.size()) {
    const auto decoded = DecodeUtf8Pair(value, offset);
    if (decoded.has_value()) {
      out.push_back(decoded->first);
      offset += decoded->second;
    } else {
      out.push_back(static_cast<unsigned char>(value[offset]));
      ++offset;
    }
  }
  return out;
}

[[nodiscard]] int64_t LevenshteinCodePointsBatch(std::string_view left,
                                                 std::string_view right) {
  const std::vector<uint32_t> a = CodePointsOfBatch(left);
  const std::vector<uint32_t> b = CodePointsOfBatch(right);
  std::vector<int64_t> prev(b.size() + 1);
  std::vector<int64_t> cur(b.size() + 1);
  for (size_t j = 0; j <= b.size(); ++j) {
    prev[j] = static_cast<int64_t>(j);
  }
  for (size_t i = 1; i <= a.size(); ++i) {
    cur[0] = static_cast<int64_t>(i);
    for (size_t j = 1; j <= b.size(); ++j) {
      const int64_t substitution = prev[j - 1] + (a[i - 1] == b[j - 1] ? 0 : 1);
      cur[j] = std::min({prev[j] + 1, cur[j - 1] + 1, substitution});
    }
    prev.swap(cur);
  }
  return prev[b.size()];
}

// T1653: the per-arm to_i64 conversion lambdas were copy-pasted verbatim
// across the string-function arms; give each distinct contract one home so
// a fix lands everywhere at once.
//
// The strict variant accepts integer arguments. Unsigned indices exceeding
// INT64_MAX saturate so they still behave as large positive lengths/positions.
[[nodiscard]] int64_t ToInt64Exact(const Value& v, int64_t def) {
  if (v.type != ValueType::kInt64) {
    return def;
  }
  return v.IsUnsigned() ? SaturatingUnsignedToInt64(
                              static_cast<uint64_t>(v.value.int_value))
                        : v.value.int_value;
}

// Lenient variant: INT64 passes through, DOUBLE saturates, and canonical
// INT64 text parses in place; anything else yields `def` ('2junk' does NOT
// read as 2).
[[nodiscard]] int64_t ToInt64WithText(const Value& v, int64_t def) {
  if (v.type == ValueType::kInt64) {
    return ToInt64Exact(v, def);
  }
  if (v.type == ValueType::kDouble) {
    return SaturatingDoubleToInt64(v.value.double_value);
  }
  if (v.type == ValueType::kVarChar) {
    // from_chars parses the string_view in place (no per-row heap copy)
    // and requiring end-of-token keeps '2junk' from reading as 2.
    const std::string_view text = v.value.varchar_value;
    int64_t parsed = 0;
    const auto [end, ec] =
        std::from_chars(text.data(), text.data() + text.size(), parsed);
    if (ec == std::errc() && end == text.data() + text.size()) {
      return parsed;
    }
  }
  return def;
}

struct CivilTime {
  int year{1970};
  int month{1};
  int day{1};
  int hour{0};
  int minute{0};
  int second{0};
  int64_t subsecond_nanos{0};
};

bool ParseFixedCivilTime(std::string_view s, CivilTime* ct) noexcept {
  // Fixed-width civil text is common in stored columns. Recognize only a
  // canonical subset here; the general parser below retains all other input
  // forms, fractional seconds and leap-second normalization.
  const bool date_only = s.size() == 10;
  const bool fixed_datetime =
      s.size() == 19 || (s.size() == 22 && s.substr(19) == "+00");
  if ((date_only || fixed_datetime) && s[4] == '-' && s[7] == '-' &&
      (date_only ||
       ((s[10] == ' ' || s[10] == 'T') && s[13] == ':' && s[16] == ':'))) {
    const auto digits = [&](size_t offset, size_t count) {
      int value = 0;
      for (size_t i = offset; i < offset + count; ++i) {
        if (s[i] < '0' || s[i] > '9') {
          return -1;
        }
        value = (value * 10) + (s[i] - '0');
      }
      return value;
    };
    const int year = digits(0, 4);
    const int month = digits(5, 2);
    const int day = digits(8, 2);
    const int hour = date_only ? 0 : digits(11, 2);
    const int minute = date_only ? 0 : digits(14, 2);
    const int second = date_only ? 0 : digits(17, 2);
    if (year >= 1 && month >= 1 && month <= 12 && day >= 1 && day <= 31 &&
        hour >= 0 && hour <= 23 && minute >= 0 && minute <= 59 && second >= 0 &&
        second <= 59 &&
        std::chrono::year_month_day{
            std::chrono::year{year},
            std::chrono::month{static_cast<unsigned>(month)},
            std::chrono::day{static_cast<unsigned>(day)}}
            .ok()) {
      *ct = {.year = year,
             .month = month,
             .day = day,
             .hour = hour,
             .minute = minute,
             .second = second,
             .subsecond_nanos = 0};
      return true;
    }
  }
  return false;
}

bool ParseCivilTime(std::string_view s, CivilTime* ct) {
  while (!s.empty() &&
         (std::isspace(static_cast<unsigned char>(s.front())) != 0)) {
    s.remove_prefix(1);
  }
  while (!s.empty() &&
         (std::isspace(static_cast<unsigned char>(s.back())) != 0)) {
    s.remove_suffix(1);
  }
  if (s.empty()) {
    return false;
  }
  if (ParseFixedCivilTime(s, ct)) {
    return true;
  }
  int Y = 0, M = 0, D = 0;
  if (s.size() == 10 &&
      sscanf(  // NOLINT(cert-err34-c) - conversion count is checked.
          std::string(s).c_str(), "%d-%d-%d", &Y, &M, &D) == 3) {
    if (Y < 1 || Y > 9999 || M < 1 || M > 12 || D < 1 || D > 31) {
      return false;
    }
    const std::chrono::year_month_day ymd{
        std::chrono::year{Y}, std::chrono::month{static_cast<unsigned>(M)},
        std::chrono::day{static_cast<unsigned>(D)}};
    if (!ymd.ok()) {
      return false;
    }
    ct->year = Y;
    ct->month = M;
    ct->day = D;
    ct->hour = 0;
    ct->minute = 0;
    ct->second = 0;
    ct->subsecond_nanos = 0;
    return true;
  }
  int h = 0, m = 0, sec = 0;
  char sep = ' ';
  bool matched = false;
  if (sscanf(  // NOLINT(cert-err34-c) - conversion count is checked.
          std::string(s).c_str(), "%d-%d-%d%c%d:%d:%d", &Y, &M, &D, &sep, &h,
          &m, &sec) >= 6) {
    ct->year = Y;
    ct->month = M;
    ct->day = D;
    ct->hour = h;
    ct->minute = m;
    ct->second = sec;
    ct->subsecond_nanos = 0;
    size_t dot = s.find('.', 11);
    if (dot != std::string_view::npos) {
      size_t end_digits = dot + 1;
      while (end_digits < s.size() && s[end_digits] >= '0' &&
             s[end_digits] <= '9') {
        ++end_digits;
      }
      std::string frac_str(s.substr(dot + 1, end_digits - (dot + 1)));
      while (frac_str.size() < 9) {
        frac_str.push_back('0');
      }
      if (frac_str.size() > 9) {
        frac_str = frac_str.substr(0, 9);
      }
      ct->subsecond_nanos = std::stoll(frac_str);
    }
    matched = true;
  } else if (sscanf(  // NOLINT(cert-err34-c) - conversion count is checked.
                 std::string(s).c_str(), "%d:%d:%d", &h, &m, &sec) >= 3) {
    ct->year = 1970;
    ct->month = 1;
    ct->day = 1;
    ct->hour = h;
    ct->minute = m;
    ct->second = sec;
    ct->subsecond_nanos = 0;
    size_t dot = s.find('.');
    if (dot != std::string_view::npos) {
      size_t end_digits = dot + 1;
      while (end_digits < s.size() && s[end_digits] >= '0' &&
             s[end_digits] <= '9') {
        ++end_digits;
      }
      std::string frac_str(s.substr(dot + 1, end_digits - (dot + 1)));
      while (frac_str.size() < 9) {
        frac_str.push_back('0');
      }
      if (frac_str.size() > 9) {
        frac_str = frac_str.substr(0, 9);
      }
      ct->subsecond_nanos = std::stoll(frac_str);
    }
    matched = true;
  }
  if (matched) {
    if (ct->year < 1 || ct->year > 9999 || ct->month < 1 || ct->month > 12 ||
        ct->day < 1 || ct->day > 31 || ct->hour < 0 || ct->hour > 23 ||
        ct->minute < 0 || ct->minute > 59 || ct->second < 0 ||
        ct->second > 60 || ct->subsecond_nanos < 0 ||
        ct->subsecond_nanos > 999999999) {
      return false;
    }
    const std::chrono::year_month_day ymd{
        std::chrono::year{ct->year},
        std::chrono::month{static_cast<unsigned>(ct->month)},
        std::chrono::day{static_cast<unsigned>(ct->day)}};
    if (!ymd.ok()) {
      return false;
    }
    if (ct->second == 60) {
      ct->second = 0;
      ct->subsecond_nanos = 0;
      ct->minute += 1;
      ct->hour += ct->minute / 60;
      ct->minute %= 60;
      if (ct->hour >= 24) {
        int extra_days = ct->hour / 24;
        ct->hour %= 24;
        int64_t days =
            std::chrono::sys_days{ymd}.time_since_epoch().count() + extra_days;
        std::chrono::sys_days new_sd{std::chrono::days{days}};
        std::chrono::year_month_day new_ymd{new_sd};
        if (int(new_ymd.year()) < 1 || int(new_ymd.year()) > 9999) {
          return false;
        }
        ct->year = int(new_ymd.year());
        ct->month = static_cast<int>(static_cast<unsigned>(new_ymd.month()));
        ct->day = static_cast<int>(static_cast<unsigned>(new_ymd.day()));
      }
    }
    return true;
  }
  return false;
}

CivilTime ShiftCivilTimeHours(const CivilTime& ct, int offset_hours) {
  CivilTime res = ct;
  int total_hours = res.hour + offset_hours;
  int day_diff = 0;
  if (total_hours >= 0) {
    day_diff = total_hours / 24;
    res.hour = total_hours % 24;
  } else {
    day_diff = (total_hours - 23) / 24;
    res.hour = (total_hours % 24 + 24) % 24;
  }
  if (day_diff != 0) {
    std::chrono::year_month_day ymd{
        std::chrono::year{res.year},
        std::chrono::month{static_cast<unsigned>(res.month)},
        std::chrono::day{static_cast<unsigned>(res.day)}};
    int64_t days =
        std::chrono::sys_days{ymd}.time_since_epoch().count() + day_diff;
    std::chrono::sys_days new_sd{std::chrono::days{days}};
    std::chrono::year_month_day new_ymd{new_sd};
    res.year = int(new_ymd.year());
    res.month = static_cast<int>(static_cast<unsigned>(new_ymd.month()));
    res.day = static_cast<int>(static_cast<unsigned>(new_ymd.day()));
  }
  return res;
}

// Static-audit triage (2026-11-22 "unchecked-decode" findings): parse/decode
// results in this file are *not* discarded. `ASSIGN_OR_RETURN(dst,
// (ParseX(...)))` passes the StatusOr as the macro's parenthesized third
// argument (the `(expr));` text the audit matched), and the decoders returning
// an optional/pointer (DecodeUtf8Pair, ParseTimestampTextNanos,
// platform::ParseTime) assign to a value explicitly tested with has_value() or
// nullptr before use. A lambda's `return ParseI64Strict(...)` propagates the
// StatusOr to an ASSIGN_OR_RETURN at its call site. Keep this note so the
// pattern scan is not re-reported against these already-checked call sites.
StatusOr<int> ParseTimeZoneOffset(std::string_view zone,
                                  const CivilTime* ct = nullptr,
                                  bool at_utc = false) {
  // Pass the real year through: the tz database carries the full historical
  // transition table (wartime DST, pre-1966 US standard time, LMT), so a
  // pre-epoch civil time must not be re-dated to 1970.
  return TryTimeZoneOffsetAtLocalTime(
      zone, ct != nullptr ? ct->year : 2000,
      ct != nullptr ? static_cast<unsigned>(ct->month) : 1,
      ct != nullptr ? static_cast<unsigned>(ct->day) : 1,
      ct != nullptr ? ct->hour : 0, ct != nullptr ? ct->minute : 0,
      ct != nullptr ? ct->second : 0, at_utc);
}

// Shifts a civil time by `delta_seconds` (day/month/year carries included).
// Mirrors the AST evaluator's helper of the same name.
CivilTime ShiftCivilTimeSeconds(const CivilTime& ct, int64_t delta_seconds) {
  const std::chrono::year_month_day ymd{
      std::chrono::year{ct.year},
      std::chrono::month{static_cast<unsigned>(ct.month)},
      std::chrono::day{static_cast<unsigned>(ct.day)}};
  const int64_t days = std::chrono::sys_days{ymd}.time_since_epoch().count();
  const int64_t secs = (days * 86400) + (int64_t{ct.hour} * 3600) +
                       (int64_t{ct.minute} * 60) + ct.second + delta_seconds;
  int64_t out_days = secs / 86400;
  int64_t day_secs = secs % 86400;
  if (day_secs < 0) {
    day_secs += 86400;
    --out_days;
  }
  const std::chrono::year_month_day out_ymd{
      std::chrono::sys_days{std::chrono::days{static_cast<int>(out_days)}}};
  CivilTime out = ct;
  out.year = int(out_ymd.year());
  out.month = static_cast<int>(static_cast<unsigned>(out_ymd.month()));
  out.day = static_cast<int>(static_cast<unsigned>(out_ymd.day()));
  out.hour = static_cast<int>(day_secs / 3600);
  out.minute = static_cast<int>((day_secs % 3600) / 60);
  out.second = static_cast<int>(day_secs % 60);
  return out;
}

std::string FormatTimeZoneOffset(int tz_offset_sec) {
  std::array<char, 16> buf{};
  // Widen before negating: std::abs(INT_MIN) is undefined behaviour.
  const int64_t wide_sec = tz_offset_sec;
  const int64_t abs_sec = wide_sec < 0 ? -wide_sec : wide_sec;
  const int h = static_cast<int>(abs_sec / 3600);
  const int m = static_cast<int>((abs_sec % 3600) / 60);
  // Historical (LMT) offsets carry a non-zero second component; match the
  // AST evaluator's ±HH:MM:SS rendering instead of dropping it.
  const int s = static_cast<int>(abs_sec % 60);
  char sign = tz_offset_sec < 0 ? '-' : '+';
  if (m == 0 && s == 0) {
    (void)snprintf(buf.data(), buf.size(), "%c%02d", sign, h);
  } else if (s == 0) {
    (void)snprintf(buf.data(), buf.size(), "%c%02d:%02d", sign, h, m);
  } else {
    (void)snprintf(buf.data(), buf.size(), "%c%02d:%02d:%02d", sign, h, m, s);
  }
  return {buf.data()};
}

void ExpandNumericTimeZone(std::string& format, int offset_seconds) {
  const int64_t wide_seconds = offset_seconds;
  const int64_t magnitude =
      wide_seconds < 0 ? -wide_seconds : wide_seconds;
  const char sign = offset_seconds < 0 ? '-' : '+';
  for (size_t i = format.find('%');
       i != std::string::npos && i + 1 < format.size();
       i = format.find('%', i + 1)) {
    const char spec = format[i + 1];
    if (spec == '%') {
      ++i;
      continue;
    }
    const bool modified = (spec == 'E' || spec == 'O') &&
                          i + 2 < format.size() && format[i + 2] == 'z';
    if (spec != 'z' && !modified) {
      continue;
    }
    std::array<char, 16> offset{};
    (void)snprintf(offset.data(), offset.size(), "%c%02lld%s%02lld", sign,
                   static_cast<long long>(magnitude / 3600),
                   spec == 'E' ? ":" : "",
                   static_cast<long long>((magnitude / 60) % 60));
    format.replace(i, modified ? 3 : 2, offset.data());
  }
}

// True when `fmt` can only produce empty output through platform::FormatTime:
// the format is empty or consists solely of zone-name directives
// (%Z/%EZ/%OZ), which expand to nothing for the zone-less struct tm these
// call sites pass. Every other directive or literal yields output, so a zero
// return there means strftime itself failed (e.g. buffer too small).
bool FormatTimeYieldsEmpty(std::string_view fmt) {
  for (size_t i = 0; i < fmt.size();) {
    if (fmt[i] != '%') {
      return false;
    }
    if (i + 1 >= fmt.size()) {
      return false;  // A trailing '%' survives as a literal.
    }
    const char spec = fmt[i + 1];
    if (spec == 'E' || spec == 'O') {
      if (i + 2 >= fmt.size() || fmt[i + 2] != 'Z') {
        return false;
      }
      i += 3;
      continue;
    }
    if (spec != 'Z') {
      return false;
    }
    i += 2;
  }
  return true;
}

// Skip the two date separators before looking for a numeric UTC offset.
std::string_view ExplicitZoneSuffix(std::string_view text) {
  while (!text.empty() &&
         (std::isspace(static_cast<unsigned char>(text.front())) != 0)) {
    text.remove_prefix(1);
  }
  while (!text.empty() &&
         (std::isspace(static_cast<unsigned char>(text.back())) != 0)) {
    text.remove_suffix(1);
  }
  const size_t first_delimiter = text.find_first_of("-:");
  if (first_delimiter == std::string_view::npos) {
    return {};
  }
  size_t search_from = first_delimiter + 1;
  if (text[first_delimiter] == '-') {
    const size_t month_end = text.find('-', search_from);
    if (month_end == std::string_view::npos) {
      return {};
    }
    search_from = month_end + 1;
  }
  const size_t offset = text.find_first_of("+-", search_from);
  if (offset != std::string_view::npos) {
    return text.substr(offset);
  }
  for (std::string_view suffix : {" UTC", " utc", " GMT", " gmt", "Z", "z"}) {
    if (text.ends_with(suffix)) {
      return text.substr(text.size() - suffix.size() +
                         (suffix.front() == ' ' ? 1 : 0));
    }
  }
  return {};
}

// ParseCivilTime reads the wall time. Removing its explicit offset yields UTC.
StatusOr<CivilTime> ExplicitZoneTextToUtcCivil(std::string_view text,
                                               CivilTime ct) {
  const std::string_view zone = ExplicitZoneSuffix(text);
  if (zone.empty()) {
    return ct;
  }
  ASSIGN_OR_RETURN(int, offset, (ParseTimeZoneOffset(zone, &ct)));
  return ShiftCivilTimeSeconds(ct, -int64_t{offset});
}

StatusOr<CivilTime> TryValueToCivilTime(const Value& val) {
  CivilTime ct;
  if (val.type == ValueType::kDate) {
    std::chrono::sys_days sys_d{std::chrono::days{val.DateDays()}};
    std::chrono::year_month_day ymd{sys_d};
    ct.year = int(ymd.year());
    ct.month = static_cast<int>(static_cast<unsigned>(ymd.month()));
    ct.day = static_cast<int>(static_cast<unsigned>(ymd.day()));
    return ct;
  }
  if (val.type != ValueType::kVarChar) {
    // The AST evaluator rejects other types outright; re-parsing AsString()
    // here accepted INT64/DOUBLE/ARRAY display text and diverged in both
    // accepted inputs and error messages.
    return StatusError(StatusCode::kInvalidArgument,
                       "requires DATE, DATETIME or TIMESTAMP");
  }
  if (ParseCivilTime(val.value.varchar_value, &ct)) {
    return ExplicitZoneTextToUtcCivil(val.value.varchar_value, ct);
  }
  return StatusError(StatusCode::kInvalidArgument,
                     "invalid DATE, DATETIME or TIMESTAMP value: " +
                         std::string(val.value.varchar_value));
}

platform::Int128 CivilTimeToNanos(const CivilTime& ct) {
  std::chrono::year_month_day ymd{
      std::chrono::year{ct.year},
      std::chrono::month{static_cast<unsigned>(ct.month)},
      std::chrono::day{static_cast<unsigned>(ct.day)}};
  int64_t days = std::chrono::sys_days{ymd}.time_since_epoch().count();
  platform::Int128 secs = (static_cast<platform::Int128>(days) * 86400) +
                          (static_cast<platform::Int128>(ct.hour) * 3600) +
                          (static_cast<platform::Int128>(ct.minute) * 60) +
                          ct.second;
  return (secs * 1000000000) + ct.subsecond_nanos;
}

CivilTime NanosToCivilTime(platform::Int128 nanos) {
  auto floor_div = [](platform::Int128 a,
                      platform::Int128 b) -> platform::Int128 {
    const platform::Int128 q = a / b;
    return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
  };
  platform::Int128 secs = floor_div(nanos, 1000000000);
  auto sub_ns = static_cast<int64_t>(nanos - (secs * 1000000000));
  auto days = static_cast<int64_t>(floor_div(secs, 86400));
  auto day_secs = static_cast<int64_t>(
      secs - (static_cast<platform::Int128>(days) * 86400));

  std::chrono::sys_days sys_d{std::chrono::days{days}};
  std::chrono::year_month_day ymd{sys_d};

  CivilTime ct;
  ct.year = int(ymd.year());
  ct.month = static_cast<int>(static_cast<unsigned>(ymd.month()));
  ct.day = static_cast<int>(static_cast<unsigned>(ymd.day()));
  ct.hour = static_cast<int>(day_secs / 3600);
  ct.minute = static_cast<int>((day_secs % 3600) / 60);
  ct.second = static_cast<int>(day_secs % 60);
  ct.subsecond_nanos = sub_ns;
  return ct;
}

// TryValueToCivilTime has already converted explicitly zoned text to UTC.
StatusOr<CivilTime> ExplicitZoneTextToDefaultCivil(std::string_view text,
                                                   CivilTime utc_ct) {
  if (ExplicitZoneSuffix(text).empty()) {
    return utc_ct;
  }
  ASSIGN_OR_RETURN(int, default_sec,
                   (ParseTimeZoneOffset(GetDefaultTimeZone(), &utc_ct,
                                        /*at_utc=*/true)));
  return ShiftCivilTimeSeconds(utc_ct, default_sec);
}

std::string FormatCivilTime(const CivilTime& ct, bool include_subsecond = true,
                            bool is_date_only = false) {
  // Valid civil fields have fixed decimal widths. Avoid printf's format
  // interpreter without changing its padding or the 3/6/9-digit fractions.
  if (ct.year >= 1 && ct.year <= 9999 && ct.month >= 1 && ct.month <= 12 &&
      ct.day >= 1 && ct.day <= 31 && ct.hour >= 0 && ct.hour <= 23 &&
      ct.minute >= 0 && ct.minute <= 59 && ct.second >= 0 && ct.second <= 60 &&
      ct.subsecond_nanos >= 0 && ct.subsecond_nanos <= 999999999) {
    std::array<char, 29> text{};
    const auto pair = [&](size_t offset, int value) {
      text[offset] = static_cast<char>('0' + (value / 10));
      text[offset + 1] = static_cast<char>('0' + (value % 10));
    };
    pair(0, ct.year / 100);
    pair(2, ct.year % 100);
    text[4] = '-';
    pair(5, ct.month);
    text[7] = '-';
    pair(8, ct.day);
    if (is_date_only) {
      return {text.data(), 10};
    }
    text[10] = ' ';
    pair(11, ct.hour);
    text[13] = ':';
    pair(14, ct.minute);
    text[16] = ':';
    pair(17, ct.second);
    size_t size = 19;
    if (include_subsecond && ct.subsecond_nanos != 0) {
      text[size++] = '.';
      const size_t digits = ct.subsecond_nanos % 1000000 == 0 ? 3
                            : ct.subsecond_nanos % 1000 == 0  ? 6
                                                              : 9;
      int64_t divisor = 100000000;
      for (size_t i = 0; i < digits; ++i) {
        text[size++] =
            static_cast<char>('0' + ((ct.subsecond_nanos / divisor) % 10));
        divisor /= 10;
      }
    }
    return {text.data(), size};
  }
  std::array<char, 64> buf{};
  if (is_date_only) {
    (void)snprintf(buf.data(), buf.size(), "%04d-%02d-%02d", ct.year, ct.month,
                   ct.day);
    return {buf.data()};
  }
  if (include_subsecond && ct.subsecond_nanos != 0) {
    if (ct.subsecond_nanos % 1000000 == 0) {
      (void)snprintf(buf.data(), buf.size(),
                     "%04d-%02d-%02d %02d:%02d:%02d.%03ld", ct.year, ct.month,
                     ct.day, ct.hour, ct.minute, ct.second,
                     static_cast<long>(ct.subsecond_nanos / 1000000));
    } else if (ct.subsecond_nanos % 1000 == 0) {
      (void)snprintf(buf.data(), buf.size(),
                     "%04d-%02d-%02d %02d:%02d:%02d.%06ld", ct.year, ct.month,
                     ct.day, ct.hour, ct.minute, ct.second,
                     static_cast<long>(ct.subsecond_nanos / 1000));
    } else {
      (void)snprintf(buf.data(), buf.size(),
                     "%04d-%02d-%02d %02d:%02d:%02d.%09ld", ct.year, ct.month,
                     ct.day, ct.hour, ct.minute, ct.second,
                     static_cast<long>(ct.subsecond_nanos));
    }
    return {buf.data()};
  }
  (void)snprintf(buf.data(), buf.size(), "%04d-%02d-%02d %02d:%02d:%02d",
                 ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second);
  return {buf.data()};
}

// Mutual recursion with Evaluate above; expression trees are the intended
// shape here.
// Error bridge: subquery,
// aggregate and lambda internals may still throw via throwing wrappers;
// error-free functions (IFERROR/ISERROR/NULLIFERROR) must observe those
// failures as Status instead of unwinding past them.
StatusOr<Value> TryEvaluateProtected(  // NOLINT(misc-no-recursion)
    const Expression& expression, const Scope& scope,
    const AggregateResultMap* aggregates, TransactionContext& context,
    const CteMap& ctes) {
  try {
    return TryEvaluate(expression, scope, aggregates, context, ctes);
  } catch (const std::exception& error) {
    return StatusError(StatusCode::kInvalidArgument, error.what());
  }
}

// std::sto* raises std::invalid_argument/out_of_range on user-supplied text;
// DB logic must surface parse failures as Status instead of unwinding past
// the evaluation boundary. Both helpers consume the whole string.
StatusOr<int64_t> ParseI64Strict(const std::string& text) {
  const char* begin = text.data();
  const char* end = begin + text.size();
  int64_t out = 0;
  const auto [parse_end, ec] = std::from_chars(begin, end, out);
  if (ec != std::errc() || parse_end != end) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid integer literal '" + text + "'");
  }
  return out;
}

StatusOr<double> ParseDoubleStrict(const std::string& text) {
  const char* begin = text.data();
  const char* end = begin + text.size();
  double out = 0;
  const auto [parse_end, ec] = std::from_chars(begin, end, out);
  if (ec != std::errc() || parse_end != end) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid numeric literal '" + text + "'");
  }
  return out;
}

}  // namespace

std::optional<UtcTimestampTruncUnit> CompileUtcTimestampTrunc(
    const FunctionCallExpression& call) {
  if (!IdentifierEquals(call.FuncName(), "timestamp_trunc") ||
      (call.Args().size() != 2 && call.Args().size() != 3) ||
      GetDefaultTimeZone() != "UTC" || !call.Args()[0] ||
      call.Args()[0]->Type() != TypeTag::kColumnValue || !call.Args()[1]) {
    return std::nullopt;
  }
  if (call.Args().size() == 3) {
    const auto& zone = call.Args()[2];
    if (!zone || zone->Type() != TypeTag::kConstantValue) {
      return std::nullopt;
    }
    const Value& value = zone->AsConstantValue().GetValue();
    if (value.type != ValueType::kVarChar || value.IsInterval() ||
        value.AsStringView() != "UTC") {
      return std::nullopt;
    }
  }
  const auto& argument = call.Args()[1];
  std::string_view unit;
  if (argument->Type() == TypeTag::kColumnValue) {
    unit = argument->AsColumnValue().GetColumnName().name;
  } else if (argument->Type() == TypeTag::kConstantValue) {
    const Value& value = argument->AsConstantValue().GetValue();
    if (value.type != ValueType::kVarChar || value.IsInterval()) {
      return std::nullopt;
    }
    unit = value.AsStringView();
  } else {
    return std::nullopt;
  }
  if (IdentifierEquals(unit, "second") || IdentifierEquals(unit, "seconds")) {
    return UtcTimestampTruncUnit::kSecond;
  }
  if (IdentifierEquals(unit, "minute") || IdentifierEquals(unit, "minutes")) {
    return UtcTimestampTruncUnit::kMinute;
  }
  if (IdentifierEquals(unit, "hour") || IdentifierEquals(unit, "hours")) {
    return UtcTimestampTruncUnit::kHour;
  }
  if (IdentifierEquals(unit, "day") || IdentifierEquals(unit, "days")) {
    return UtcTimestampTruncUnit::kDay;
  }
  return std::nullopt;
}

std::optional<int64_t> TryUtcTimestampTruncKey(
    std::string_view text, UtcTimestampTruncUnit unit) noexcept {
  if (text.size() != 19 && (text.size() != 22 || text.substr(19) != "+00")) {
    return std::nullopt;
  }
  CivilTime civil;
  if (!ParseFixedCivilTime(text, &civil)) {
    return std::nullopt;
  }
  switch (unit) {
    case UtcTimestampTruncUnit::kDay:
      civil.hour = 0;
      [[fallthrough]];
    case UtcTimestampTruncUnit::kHour:
      civil.minute = 0;
      [[fallthrough]];
    case UtcTimestampTruncUnit::kMinute:
      civil.second = 0;
      [[fallthrough]];
    case UtcTimestampTruncUnit::kSecond:
      break;
    default:
      return std::nullopt;
  }
  int64_t key = civil.year;
  for (const int part :
       {civil.month, civil.day, civil.hour, civil.minute, civil.second}) {
    key = (key * 100) + part;
  }
  return key;
}

std::optional<UtcTimestampTruncUnit> CompileUtcTimestampExtract(
    const FunctionCallExpression& call) {
  if (call.Args().size() != 1 || GetDefaultTimeZone() != "UTC" ||
      !call.Args()[0] || call.Args()[0]->Type() != TypeTag::kColumnValue) {
    return std::nullopt;
  }
  if (IdentifierEquals(call.FuncName(), "extract_minute")) {
    return UtcTimestampTruncUnit::kMinute;
  }
  if (IdentifierEquals(call.FuncName(), "extract_hour")) {
    return UtcTimestampTruncUnit::kHour;
  }
  return std::nullopt;
}

std::optional<int64_t> TryUtcTimestampExtractKey(
    std::string_view text, UtcTimestampTruncUnit unit) noexcept {
  const auto key =
      TryUtcTimestampTruncKey(text, UtcTimestampTruncUnit::kSecond);
  if (!key) {
    return std::nullopt;
  }
  if (unit == UtcTimestampTruncUnit::kMinute) {
    return (*key / 100) % 100;
  }
  if (unit == UtcTimestampTruncUnit::kHour) {
    return (*key / 10000) % 100;
  }
  return std::nullopt;
}

StatusOr<Value> FormatUtcTimestampTruncKey(int64_t key) try {
  if (key < 10101000000LL || key > 99991231235959LL) {
    return Status::kInvalidArgument;
  }
  CivilTime civil;
  civil.second = static_cast<int>(key % 100);
  key /= 100;
  civil.minute = static_cast<int>(key % 100);
  key /= 100;
  civil.hour = static_cast<int>(key % 100);
  key /= 100;
  civil.day = static_cast<int>(key % 100);
  key /= 100;
  civil.month = static_cast<int>(key % 100);
  civil.year = static_cast<int>(key / 100);
  if (civil.hour > 23 || civil.minute > 59 || civil.second > 59 ||
      !std::chrono::year_month_day{
          std::chrono::year{civil.year},
          std::chrono::month{static_cast<unsigned>(civil.month)},
          std::chrono::day{static_cast<unsigned>(civil.day)}}
           .ok()) {
    return Status::kInvalidArgument;
  }
  return Value(FormatCivilTime(civil) + "+00");
} catch (const std::exception& error) {
  return StatusError(StatusCode::kRuntimeError, error.what());
} catch (...) {
  return StatusError(StatusCode::kRuntimeError,
                     "timestamp key formatting failed");
}

StatusOr<Value> TryEvaluateFunction(  // NOLINT(misc-no-recursion)
    const FunctionCallExpression& call, const Scope& scope,
    const AggregateResultMap* aggregates, TransactionContext& context,
    const CteMap& ctes) {
  auto raw_str = [](const Value& val) -> std::string {
    if (val.type == ValueType::kVarChar) {
      return std::string(val.value.varchar_value);
    }
    return val.AsString();
  };
  // Read-only view of a string argument. VARCHAR -- by far the common case in
  // the string functions below -- borrows the Value's storage with zero
  // allocation/copy; other types still need AsString()'s conversion, owned
  // here. Keep the wrapper as a NAMED local while using `view` (a temporary
  // would destroy `owned` and dangle the view).
  struct StringArgView {
    std::string owned;
    std::string_view view;
    explicit StringArgView(const Value& val) {
      if (val.type == ValueType::kVarChar) {
        view = val.value.varchar_value;
      } else {
        owned = val.AsString();
        view = owned;
      }
    }
  };
  // FunctionCallExpression lowercases its name at construction, so this
  // binds the stored copy directly: the per-row re-copy + re-lowercase ran
  // inside the residual-predicate loop of every AST-evaluated scan filter.
  const std::string& name = call.FuncName();
  // Compare with known-length literals without out-of-line string/char*
  // comparisons, which otherwise dominate scalar calls in dictionary folds.
  const std::string_view name_view(name);
  // Proto-field guards emitted by the GoogleSQL frontend: NEW constructors
  // and SELECT AS <proto> route non-constant repeated-field arrays and enum
  // values through them so invalid data fails execution instead of being
  // silently dropped from the text-format representation.
  if (name_view == "$proto_repeated_guard") {
    const auto& args = call.Args();
    if (args.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "$proto_repeated_guard requires 2 arguments");
    }
    ASSIGN_OR_RETURN(Value, arr,
                     (TryEvaluate(args[0], scope, aggregates, context, ctes)));
    if (arr.IsArray()) {
      for (const Value& element : arr.ArrayElements()) {
        if (element.IsNull()) {
          return StatusError(
              StatusCode::kInvalidArgument,
              "Cannot encode a null value in a repeated protocol message "
              "field");
        }
      }
    } else if (!arr.IsNull()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "repeated proto field requires an array");
    }
    return TryEvaluate(args[1], scope, aggregates, context, ctes);
  }
  if (name_view == "$proto_field_guard" || name_view == "$proto_enum_guard") {
    const auto& args = call.Args();
    const size_t expected = name_view == "$proto_field_guard" ? 3 : 2;
    if (args.size() != expected) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " argument count mismatch");
    }
    ASSIGN_OR_RETURN(Value, value,
                     (TryEvaluate(args[0], scope, aggregates, context, ctes)));
    if (!value.IsNull()) {
      ASSIGN_OR_RETURN(
          Value, type_value,
          (TryEvaluate(args[1], scope, aggregates, context, ctes)));

      const std::string enum_type =
          type_value.type == ValueType::kVarChar
              ? std::string(type_value.value.varchar_value)
              : (type_value.IsNull() ? std::string()
                                     : std::string(type_value.AsString()));
      // Full CAST validation against the enum registry; throws on unknown
      // members or out-of-range ordinals.
      Row dummy_row;
      Schema dummy_schema;
      Expression checked =
          CastExpressionExp(ConstantValueExp(value), enum_type, false);
      // Match the AST canonical path: an invalid enum member/out-of-range
      // ordinal must fail the guard instead of being silently discarded.
      ASSIGN_OR_RETURN(Value, validated,
                       (checked->TryEvaluate(dummy_row, dummy_schema)));
      (void)validated;
    }
    if (expected == 3) {
      return TryEvaluate(args[2], scope, aggregates, context, ctes);
    }
    return value;
  }
  if (name_view == "date_add" || name_view == "date_sub" ||
      name_view == "datetime_add" || name_view == "datetime_sub" ||
      name_view == "timestamp_add" || name_view == "timestamp_sub") {
    if (call.Args().size() != 2 ||
        call.Args()[1]->Type() != TypeTag::kIntervalExp) {
      return StatusError(StatusCode::kInvalidArgument,
                         "DATE_ADD/DATE_SUB arity");
    }
    ASSIGN_OR_RETURN(
        Value, base,
        (TryEvaluate(call.Args()[0], scope, aggregates, context, ctes)));

    if (base.IsNull()) {
      return Value();
    }
    const auto& interval = call.Args()[1]->AsIntervalExpression();
    const bool is_sub = name.ends_with("_sub");
    const int64_t raw_amount = interval.Amount();
    std::string unit = std::string(interval.Unit());
    for (char& c : unit) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }

    ASSIGN_OR_RETURN(CivilTime, ct, (TryValueToCivilTime(base)));
    std::string base_s = raw_str(base);
    bool is_timestamp = (base_s.find('+') != std::string::npos ||
                         base_s.find('Z') != std::string::npos ||
                         base_s.find('z') != std::string::npos);
    int timestamp_offset_sec = 0;
    if (is_timestamp) {
      {
        ASSIGN_OR_RETURN(int, parsed_offset,
                         (ParseTimeZoneOffset(GetDefaultTimeZone(), &ct)));
        timestamp_offset_sec = parsed_offset;
      }
      platform::Int128 ns =
          CivilTimeToNanos(ct) +
          (static_cast<platform::Int128>(timestamp_offset_sec) * 1000000000);
      ct = NanosToCivilTime(ns);
    }
    const bool is_date = (name_view == "date_add" || name_view == "date_sub" ||
                          base.type == ValueType::kDate);

    if (unit == "year" || unit == "years" || unit == "quarter" ||
        unit == "quarters" || unit == "month" || unit == "months") {
      const platform::Int128 amount =
          is_sub ? -static_cast<platform::Int128>(raw_amount) : raw_amount;
      platform::Int128 add_m = amount;
      if (unit.starts_with("year")) {
        add_m = amount * 12;
      } else if (unit.starts_with("quarter")) {
        add_m = amount * 3;
      }
      const platform::Int128 total_m =
          ((static_cast<platform::Int128>(ct.year) * 12) + (ct.month - 1)) +
          add_m;
      if (total_m < 12 || total_m >= 120000) {
        return StatusError(StatusCode::kIsInfinity, "DATETIME out of range");
      }
      int target_y = static_cast<int>(total_m / 12);
      int target_m = static_cast<int>((total_m % 12) + 1);
      if (target_m <= 0) {
        target_m += 12;
        target_y -= 1;
      }
      using std::chrono::month;
      using std::chrono::month_day_last;
      using std::chrono::year;
      using std::chrono::year_month_day_last;
      year_month_day_last last_of_target{
          year{target_y},
          month_day_last{month{static_cast<unsigned>(target_m)}}};
      auto target_d = static_cast<unsigned>(ct.day);
      target_d =
          std::min(target_d, static_cast<unsigned>(last_of_target.day()));
      ct.year = target_y;
      ct.month = target_m;
      ct.day = static_cast<int>(target_d);
      if (ct.year < 1 || ct.year > 9999) {
        return StatusError(StatusCode::kIsInfinity, "DATETIME out of range");
      }
      if (is_date) {
        std::chrono::year_month_day ymd{
            year{ct.year}, month{static_cast<unsigned>(ct.month)},
            std::chrono::day{static_cast<unsigned>(ct.day)}};
        return Value::DateFromDays(
            std::chrono::sys_days{ymd}.time_since_epoch().count());
      }
      if (is_timestamp) {
        ASSIGN_OR_RETURN(int, tz_offset_sec,
                         (ParseTimeZoneOffset(GetDefaultTimeZone(), &ct)));
        platform::Int128 ns =
            CivilTimeToNanos(ct) -
            (static_cast<platform::Int128>(tz_offset_sec) * 1000000000);
        CivilTime utc_ct = NanosToCivilTime(ns);
        return Value(FormatCivilTime(utc_ct) + "+00");
      }
      return Value(FormatCivilTime(ct));
    }

    int64_t delta_days = 0;
    int64_t delta_sub_ns = 0;
    if (raw_amount == std::numeric_limits<int64_t>::min()) {
      uint64_t uamount = 9223372036854775808ULL;
      if (unit == "nanosecond" || unit == "nanoseconds") {
        delta_days = static_cast<int64_t>(uamount / (86400ULL * 1000000000ULL));
        delta_sub_ns =
            static_cast<int64_t>(uamount % (86400ULL * 1000000000ULL));
      } else {
        return StatusError(StatusCode::kIsInfinity, "DATETIME out of range");
      }
      if (!is_sub) {
        delta_days = -delta_days;
        delta_sub_ns = -delta_sub_ns;
      }
    } else {
      int64_t amount = is_sub ? -raw_amount : raw_amount;
      if (unit == "day" || unit == "days") {
        delta_days = amount;
      } else if (unit == "week" || unit == "weeks") {
        if (__builtin_mul_overflow(amount, int64_t{7}, &delta_days)) {
          return StatusError(StatusCode::kIsInfinity, "DATETIME out of range");
        }
      } else if (unit == "hour" || unit == "hours") {
        delta_days = amount / 24;
        delta_sub_ns = (amount % 24) * 3600LL * 1000000000LL;
      } else if (unit == "minute" || unit == "minutes") {
        delta_days = amount / (24LL * 60LL);
        delta_sub_ns = (amount % (24LL * 60LL)) * 60LL * 1000000000LL;
      } else if (unit == "second" || unit == "seconds") {
        delta_days = amount / 86400LL;
        delta_sub_ns = (amount % 86400LL) * 1000000000LL;
      } else if (unit == "millisecond" || unit == "milliseconds") {
        delta_days = amount / (86400LL * 1000LL);
        delta_sub_ns = (amount % (86400LL * 1000LL)) * 1000000LL;
      } else if (unit == "microsecond" || unit == "microseconds") {
        delta_days = amount / (86400LL * 1000000LL);
        delta_sub_ns = (amount % (86400LL * 1000000LL)) * 1000LL;
      } else if (unit == "nanosecond" || unit == "nanoseconds") {
        delta_days = amount / (86400LL * 1000000000LL);
        delta_sub_ns = amount % (86400LL * 1000000000LL);
      } else {
        return StatusError(StatusCode::kInvalidArgument,
                           "unsupported interval unit " + unit);
      }
    }

    int64_t day_nanos =
        ((ct.hour * 3600LL + ct.minute * 60LL + ct.second) * 1000000000LL) +
        ct.subsecond_nanos + delta_sub_ns;
    int64_t total_day_nanos = 86400LL * 1000000000LL;
    auto floor_div = [](int64_t a, int64_t b) -> int64_t {
      const int64_t q = a / b;
      return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
    };
    int64_t extra_days = floor_div(day_nanos, total_day_nanos);
    int64_t rem_day_nanos = day_nanos - (extra_days * total_day_nanos);

    std::chrono::year_month_day cur_ymd{
        std::chrono::year{ct.year},
        std::chrono::month{static_cast<unsigned>(ct.month)},
        std::chrono::day{static_cast<unsigned>(ct.day)}};
    int64_t cur_days =
        std::chrono::sys_days{cur_ymd}.time_since_epoch().count();
    const platform::Int128 target_days =
        static_cast<platform::Int128>(cur_days) + delta_days + extra_days;
    const auto min_days = std::chrono::sys_days{std::chrono::year{1} / 1 / 1}
                              .time_since_epoch()
                              .count();
    const auto max_days =
        std::chrono::sys_days{std::chrono::year{9999} / 12 / 31}
            .time_since_epoch()
            .count();
    if (target_days < min_days || target_days > max_days) {
      return StatusError(StatusCode::kIsInfinity, "DATETIME out of range");
    }
    std::chrono::sys_days target_sys_days{
        std::chrono::days{static_cast<int64_t>(target_days)}};
    std::chrono::year_month_day target_ymd{target_sys_days};

    CivilTime res_ct;
    res_ct.year = int(target_ymd.year());
    res_ct.month = static_cast<int>(static_cast<unsigned>(target_ymd.month()));
    res_ct.day = static_cast<int>(static_cast<unsigned>(target_ymd.day()));
    int64_t rem_secs = rem_day_nanos / 1000000000LL;
    res_ct.subsecond_nanos = rem_day_nanos % 1000000000LL;
    res_ct.hour = static_cast<int>(rem_secs / 3600);
    res_ct.minute = static_cast<int>((rem_secs % 3600) / 60);
    res_ct.second = static_cast<int>(rem_secs % 60);

    if (res_ct.year < 1 || res_ct.year > 9999) {
      return StatusError(StatusCode::kIsInfinity, "DATETIME out of range");
    }
    if (is_date) {
      std::chrono::year_month_day ymd{
          std::chrono::year{res_ct.year},
          std::chrono::month{static_cast<unsigned>(res_ct.month)},
          std::chrono::day{static_cast<unsigned>(res_ct.day)}};
      if (base.type == ValueType::kDate) {
        return Value::DateFromDays(
            std::chrono::sys_days{ymd}.time_since_epoch().count());
      }
      std::array<char, 32> buf{};
      (void)snprintf(buf.data(), buf.size(), "%04d-%02d-%02d", res_ct.year,
                     res_ct.month, res_ct.day);
      return Value(std::string(buf.data()));
    }
    if (is_timestamp) {
      platform::Int128 ns =
          CivilTimeToNanos(res_ct) -
          (static_cast<platform::Int128>(timestamp_offset_sec) * 1000000000);
      CivilTime utc_ct = NanosToCivilTime(ns);
      return Value(FormatCivilTime(utc_ct) + "+00");
    }
    return Value(FormatCivilTime(res_ct));
  }
  // Control flow functions must observe conditional-evaluation semantics:
  // untaken branches (and error-handled operands) are never evaluated, so a
  // division by zero or an invalid cast inside them cannot surface.
  // Branch results are normalized to the common supertype of every branch
  // (GoogleSQL coerces INT64 branch arms to FLOAT64 when any arm is a
  // float), keeping downstream comparisons type-consistent.
  auto promotes_to_double = [&](const std::vector<Expression>& exprs) {
    return std::ranges::any_of(exprs, [&](const Expression& branch) {
      if (scope.schema == nullptr) {
        return false;
      }
      try {
        return branch->ResultType(*scope.schema).GetType() == TypeTag::kDouble;
      } catch (const std::exception&) {
        // Static types are unavailable for subqueries/aggregates; the
        // runtime value types then stand on their own.
        return false;
      }
    });
  };
  auto normalize = [](Value value, bool to_double) {
    if (to_double && !value.IsNull() && value.type == ValueType::kInt64) {
      return Value(value.IsUnsigned()
                       ? static_cast<double>(
                             static_cast<uint64_t>(value.value.int_value))
                       : static_cast<double>(value.value.int_value));
    }
    return value;
  };
  // COALESCE stops at the first non-NULL argument.
  if (name_view == "coalesce" || name_view == "ifnull") {
    if (name_view == "ifnull" && call.Args().size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "IFNULL requires 2 arguments");
    }
    TypeTag common = TypeTag::kInvalid;
    if (name_view == "coalesce") {
      static const Schema empty_schema;
      const Schema& schema = (scope.schema != nullptr) ? *scope.schema : empty_schema;
      const Schema* outer = (scope.outer != nullptr) ? scope.outer->schema : nullptr;
      ASSIGN_OR_RETURN(TypeTag, resolved, (call.CoalesceType(schema, outer)));
      common = resolved;
    }
    for (const Expression& argument : call.Args()) {
      ASSIGN_OR_RETURN(
          Value, value,
          (TryEvaluate(argument, scope, aggregates, context, ctes)));
      if (!value.IsNull()) {
        return normalize(std::move(value), common == TypeTag::kDouble);
      }
    }
    return Value();
  }
  if (name_view == "if") {
    if (call.Args().size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "IF requires 3 arguments");
    }
    const bool as_double = promotes_to_double({call.Args()[1], call.Args()[2]});
    ASSIGN_OR_RETURN(
        Value, condition,
        (TryEvaluate(call.Args()[0], scope, aggregates, context, ctes)));

    const bool take_then = !condition.IsNull() && Truthy(condition);
    ASSIGN_OR_RETURN(Value, taken,
                     (TryEvaluate(call.Args()[take_then ? 1 : 2], scope,
                                  aggregates, context, ctes)));
    return normalize(std::move(taken), as_double);
  }
  if (name_view == "iferror") {
    if (call.Args().size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "IFERROR requires 2 arguments");
    }
    const bool as_double = promotes_to_double(call.Args());
    StatusOr<Value> attempt =
        TryEvaluateProtected(call.Args()[0], scope, aggregates, context, ctes);
    if (attempt.HasValue()) {
      return normalize(attempt.MoveValue(), as_double);
    }
    ASSIGN_OR_RETURN(Value, fallback,
                     (TryEvaluateProtected(call.Args()[1], scope, aggregates,
                                           context, ctes)));
    return normalize(std::move(fallback), as_double);
  }
  if (name_view == "iserror") {
    if (call.Args().size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ISERROR requires 1 argument");
    }
    return Value(
        !TryEvaluateProtected(call.Args()[0], scope, aggregates, context, ctes)
             .HasValue());
  }
  if (name_view == "nulliferror") {
    if (call.Args().size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "NULLIFERROR requires 1 argument");
    }
    StatusOr<Value> attempt =
        TryEvaluateProtected(call.Args()[0], scope, aggregates, context, ctes);
    if (!attempt.HasValue()) {
      return Value();
    }
    return attempt;
  }
  std::vector<Value> arguments;
  arguments.reserve(call.Args().size());
  const bool truncates_civil_time =
      name_view == "date_trunc" || name_view == "datetime_trunc" ||
      name_view == "timestamp_trunc" || name_view == "last_day";
  const bool diffs_civil_time = name_view == "date_diff" ||
                                name_view == "datetime_diff" ||
                                name_view == "timestamp_diff";
  for (const Expression& argument : call.Args()) {
    // Convert date-part identifiers to their literal names before evaluation.
    if (((truncates_civil_time && arguments.size() == 1) ||
         (diffs_civil_time && arguments.size() == 2)) &&
        argument->Type() == TypeTag::kColumnValue) {
      arguments.emplace_back(
          std::string(argument->AsColumnValue().GetColumnName().name));
      continue;
    }
    ASSIGN_OR_RETURN(Value, argument_value,
                     (TryEvaluate(argument, scope, aggregates, context, ctes)));
    arguments.push_back(std::move(argument_value));
  }

  if (name_view == "round" || name_view == "__between") {
    return ExecuteFunction(name, arguments);
  }
  if (name_view == "__pipe_concat") {
    if (arguments.size() != 2 || !arguments[0].IsArray()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__pipe_concat requires an array and separator");
    }
    struct Pair {
      std::string a;
      std::string b;
    };
    std::vector<Pair> pairs;
    pairs.reserve(arguments[0].ArrayElements().size());
    for (const Value& element : arguments[0].ArrayElements()) {
      if (element.IsNull()) {
        continue;
      }
      const std::string text = raw_str(element);
      Value a;
      Value b;
      // Same contract as the AST arm (invariant 3): a non-`{...}` element is
      // parsed leniently as its whole body (proto-text shapes like
      // `a: 1 b: 2`), and member keys match case-insensitively.
      const std::string body = text.size() >= 2 && text.front() == '{'
                                   ? text.substr(1, text.size() - 2)
                                   : text;
      for (const auto& [key, member] : SplitJsonObjectMembers(body)) {
        Value parsed;
        if (!JsonTextToValue(member, &parsed)) {
          continue;
        }
        if (IdentifierEquals(key, "a")) {
          a = std::move(parsed);
        } else if (IdentifierEquals(key, "b")) {
          b = std::move(parsed);
        }
      }
      pairs.push_back({raw_str(a), raw_str(b)});
    }
    std::ranges::sort(pairs, [](const Pair& left, const Pair& right) {
      return left.b == right.b ? left.a < right.a : left.b < right.b;
    });
    const std::string separator = raw_str(arguments[1]);
    std::string result;
    for (const Pair& pair : pairs) {
      if (!result.empty()) {
        result += separator;
      }
      result.append(pair.a).append(1, ',').append(pair.b);
    }
    return Value(std::move(result));
  }

  if (name_view == "__struct_json__") {
    // Runtime struct constructor installed by the visitor when struct
    // fields could not be folded at plan time (aggregates, subqueries).
    // Arguments normally alternate name / value / quoted-flag.  Accept the
    // older two-argument form as well because optimizer-bound expressions
    // use it for already typed values.
    bool triple_form = arguments.size() % 3 == 0;
    for (size_t i = 2; triple_form && i < arguments.size(); i += 3) {
      triple_form =
          arguments[i].IsNull() || arguments[i].type == ValueType::kInt64;
    }
    const size_t stride = triple_form ? 3 : 2;
    if (arguments.size() % stride != 0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "struct constructor arity");
    }
    return ExecuteFunction(name, arguments);
  }

  auto to_lower = [](std::string_view s) -> std::string {
    std::string out(s);
    for (char& c : out) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return out;
  };

  if (name.starts_with("extract_")) {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "EXTRACT requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments[0].IsInterval()) {
      return ExecuteFunction(name, arguments);
    }
    ASSIGN_OR_RETURN(CivilTime, ct, (TryValueToCivilTime(arguments[0])));
    if (arguments.size() == 2) {
      if (arguments[1].IsNull()) {
        return Value();
      }
      std::string tz_str = raw_str(arguments[1]);
      // T4097: mirror of the AST arm -- the two-argument form renders a
      // TIMESTAMP instant, so resolve the zone's offset AT that instant
      // (FORMAT_TIMESTAMP semantics), not as local wall-clock fields.
      ASSIGN_OR_RETURN(int, tz_offset_sec,
                       (ParseTimeZoneOffset(tz_str, &ct, /*at_utc=*/true)));
      // Apply the whole offset (minute *and* second residues) in one go so
      // historic LMT zones do not lose their sub-minute component; this is
      // the same rule the AST evaluator uses.
      ct = ShiftCivilTimeSeconds(ct, tz_offset_sec);
    }
    if (name_view == "extract_date") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      return Value::DateFromDays(
          std::chrono::sys_days{ymd}.time_since_epoch().count());
    }
    if (name_view == "extract_time") {
      std::array<char, 64> buf{};
      if (ct.subsecond_nanos != 0) {
        if (ct.subsecond_nanos % 1000000 == 0) {
          (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%03ld",
                         ct.hour, ct.minute, ct.second,
                         static_cast<long>(ct.subsecond_nanos / 1000000));
        } else if (ct.subsecond_nanos % 1000 == 0) {
          (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%06ld",
                         ct.hour, ct.minute, ct.second,
                         static_cast<long>(ct.subsecond_nanos / 1000));
        } else {
          (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%09ld",
                         ct.hour, ct.minute, ct.second,
                         static_cast<long>(ct.subsecond_nanos));
        }
      } else {
        (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d", ct.hour,
                       ct.minute, ct.second);
      }
      return Value(std::string(buf.data()));
    }
    if (name_view == "extract_datetime") {
      return Value(FormatCivilTime(ct));
    }
    if (name_view == "extract_year") {
      return Value(static_cast<int64_t>(ct.year));
    }
    if (name_view == "extract_quarter") {
      return Value(static_cast<int64_t>(((ct.month - 1) / 3) + 1));
    }
    if (name_view == "extract_month") {
      return Value(static_cast<int64_t>(ct.month));
    }
    if (name_view == "extract_day") {
      return Value(static_cast<int64_t>(ct.day));
    }
    if (name_view == "extract_hour") {
      return Value(static_cast<int64_t>(ct.hour));
    }
    if (name_view == "extract_minute") {
      return Value(static_cast<int64_t>(ct.minute));
    }
    if (name_view == "extract_second") {
      return Value(static_cast<int64_t>(ct.second));
    }
    if (name_view == "extract_millisecond") {
      return Value(static_cast<int64_t>(ct.subsecond_nanos / 1000000LL));
    }
    if (name_view == "extract_microsecond") {
      return Value(static_cast<int64_t>(ct.subsecond_nanos / 1000LL));
    }
    if (name_view == "extract_nanosecond") {
      return Value(ct.subsecond_nanos);
    }
    if (name_view == "extract_week") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      std::chrono::sys_days sd{ymd};
      std::chrono::year_month_day year_start{std::chrono::year{ct.year},
                                             std::chrono::month{1},
                                             std::chrono::day{1}};
      int start_wd = static_cast<int>(
          std::chrono::weekday{std::chrono::sys_days{year_start}}.c_encoding());
      int yday =
          static_cast<int>((sd - std::chrono::sys_days{year_start}).count());
      // GoogleSQL WEEK: days before the first Sunday are week 0.
      const int first_sunday = (7 - start_wd) % 7;
      int week_num =
          yday < first_sunday
              ? 0
              : ((yday -
                 first_sunday) /  // NOLINT(readability-math-missing-parentheses)
                        7) +  // NOLINT(readability-math-missing-parentheses)
                    1;       // NOLINT(readability-math-missing-parentheses)
      return Value(static_cast<int64_t>(week_num));
    }
    if (name_view.starts_with("extract_week(")) {
      // EXTRACT(WEEK(<weekday>) FROM d): the week number with weeks
      // starting on <weekday>. The first partial week of the year is week
      // 0, matching the plain extract_week formula above (which is the
      // Sunday-start spelling: offset folds to Jan 1's c_encoding there).
      static const std::unordered_map<std::string_view, int> kIsoWeekdayByName =
          {{"sunday", 7},   {"monday", 1}, {"tuesday", 2}, {"wednesday", 3},
           {"thursday", 4}, {"friday", 5}, {"saturday", 6}};
      const std::string weekday(
          name_view.substr(sizeof("extract_week(") - 1,
                           name_view.size() - sizeof("extract_week(")));
      const auto iso = kIsoWeekdayByName.find(weekday);
      if (iso == kIsoWeekdayByName.end()) {
        return StatusError(
            StatusCode::kInvalidArgument,
            "unsupported EXTRACT part " + std::string(name_view));
      }
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      std::chrono::sys_days sd{ymd};
      std::chrono::year_month_day year_start{std::chrono::year{ct.year},
                                             std::chrono::month{1},
                                             std::chrono::day{1}};
      const int jan1_iso = static_cast<int>(std::chrono::weekday{
          std::chrono::sys_days{year_start}}.iso_encoding());
      const int offset = (jan1_iso - iso->second + 7) % 7;
      const int yday =
          static_cast<int>((sd - std::chrono::sys_days{year_start}).count());
      return Value(static_cast<int64_t>((yday + offset) / 7));
    }
    if (name_view == "extract_dayofweek") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      int wd = static_cast<int>(
          std::chrono::weekday{std::chrono::sys_days{ymd}}.c_encoding());
      return Value(static_cast<int64_t>(wd + 1));
    }
    if (name_view == "extract_weekday") {
      // WEEKDAY: Monday = 1 ... Sunday = 7 (ISO encoding). DAYOFWEEK above is
      // the Sunday = 1 form; c_encoding() here returned Sunday = 0.
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      int wd = static_cast<int>(
          std::chrono::weekday{std::chrono::sys_days{ymd}}.iso_encoding());
      return Value(static_cast<int64_t>(wd));
    }
    if (name_view == "extract_dayofyear") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      std::chrono::year_month_day year_start{std::chrono::year{ct.year},
                                             std::chrono::month{1},
                                             std::chrono::day{1}};
      int yday = static_cast<int>((std::chrono::sys_days{ymd} -
                                   std::chrono::sys_days{year_start})
                                      .count()) +
                 1;
      return Value(static_cast<int64_t>(yday));
    }
    if (name_view == "extract_isoyear" || name_view == "extract_isoweek") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      std::chrono::sys_days sd{ymd};
      int iso_wd = static_cast<int>(std::chrono::weekday{sd}.iso_encoding());
      std::chrono::sys_days thu = sd + std::chrono::days{4 - iso_wd};
      std::chrono::year_month_day thu_ymd{thu};
      int isoyear = int(thu_ymd.year());
      if (name_view == "extract_isoyear") {
        return Value(static_cast<int64_t>(isoyear));
      }
      std::chrono::year_month_day iso_start{std::chrono::year{isoyear},
                                            std::chrono::month{1},
                                            std::chrono::day{4}};
      int start_iso_wd = static_cast<int>(std::chrono::weekday{
          std::chrono::sys_days{iso_start}}.iso_encoding());
      std::chrono::sys_days first_mon = std::chrono::sys_days{iso_start} -
                                        std::chrono::days{start_iso_wd - 1};
      int isoweek = (static_cast<int>((sd - first_mon).count()) / 7) + 1;
      return Value(static_cast<int64_t>(isoweek));
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "unsupported extract field: " + name);
  }
  if (name_view == "__struct_set") {
    if (arguments.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__struct_set requires 3 arguments");
    }
    // A NULL struct or field name must propagate as SQL NULL, exactly like
    // the AST evaluator (function_call_expression.cpp __struct_set arm) and
    // get_field: raw_str(NULL) would otherwise create a literal "NULL"
    // member. A NULL *value* is a legitimate JSON null member.
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    // T2398: call the StatusOr route directly; the throwing
    // StructSetField shim leaked std::runtime_error across the
    // StatusOr evaluation boundary (invariant #1).
    return TryStructSetField(arguments[0], raw_str(arguments[1]), arguments[2]);
  }
  if (name_view == "error") {
    // Unreachable in valid plans unless evaluated: raise the requested
    // runtime error (the message text is informational only).
    return StatusError(StatusCode::kInvalidArgument,
                       arguments.empty() || arguments[0].IsNull()
                           ? std::string("ERROR: user-raised")
                           : "generic::out_of_range: " + raw_str(arguments[0]));
  }
  if (name_view == "nullif") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "NULLIF requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return arguments[0];
    }
    ASSIGN_OR_RETURN(Value, eq,
                     TryEvaluateBinary(BinaryOperation::kEquals, arguments[0],
                                       arguments[1]));
    if (eq.Truthy()) {
      return Value();
    }
    return arguments[0];
  }

  auto parse_date_val = [](const Value& val) -> StatusOr<int64_t> {
    if (val.type == ValueType::kDate) {
      return val.DateDays();
    }
    if (val.type == ValueType::kInt64) {
      return val.value.int_value;
    }
    if (val.type == ValueType::kVarChar) {
      std::string_view s = val.value.varchar_value;
      if (s.size() == 10 && s[4] == '-' && s[7] == '-') {
        return TryParseDateDays(s.substr(0, 10));
      }
      // Only a bare YYYY-MM-DD is a DATE; a timestamp spelling must fall
      // through to the caller's TIMESTAMP handling instead of being silently
      // truncated at the date part.
      return TryParseDateDays(s);
    }
    return StatusError(StatusCode::kInvalidArgument, "requires DATE");
  };

  if (name_view == "unix_date") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "UNIX_DATE requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    // Only a DATE value or a bare `YYYY-MM-DD` (exactly 10 chars) can take
    // this fast path. TIMESTAMP text with a time or timezone must fall through
    // to the canonical TIMESTAMP parser below, which applies the offset and
    // can move the UTC date by a day. INT64 is likewise not a DATE argument.
    if (arguments[0].type == ValueType::kDate) {
      return Value(arguments[0].DateDays());
    }
    if (arguments[0].type == ValueType::kVarChar) {
      const std::string_view s = arguments[0].value.varchar_value;
      if (s.size() == 10 && s[4] == '-' && s[7] == '-') {
        ASSIGN_OR_RETURN(int64_t, days, (TryParseDateDays(s)));
        return Value(days);
      }
    }
  }
  if (name_view == "date_from_unix_date") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "DATE_FROM_UNIX_DATE requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(int64_t, days, (parse_date_val(arguments[0])));
    if (days < -11000000 || days > 11000000) {
      return StatusError(StatusCode::kInvalidArgument,
                         "DATE_FROM_UNIX_DATE: day count out of range");
    }
    return Value::DateFromDays(days);
  }
  if (name_view == "current_time") {
    if (arguments.size() > 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CURRENT_TIME takes at most 1 argument");
    }
    if (arguments.size() == 1 && arguments[0].IsNull()) {
      return Value();
    }
    // T3882: resolve the zone offset at the current instant, not at the
    // one-argument ParseTimeZoneOffset default of 2000-01-01 (whose DST
    // state is fixed and wrong inside DST by an hour); same fix
    // CURRENT_DATETIME received under T1490.
    const time_t utc_now = time(nullptr);
    int tz_offset_sec = 0;
    if (arguments.size() == 1 && !arguments[0].IsNull()) {
      struct tm utc_tm = {};
      platform::GmTime(static_cast<int64_t>(utc_now), &utc_tm);
      const CivilTime utc_ct{.year = utc_tm.tm_year + 1900,
                             .month = utc_tm.tm_mon + 1,
                             .day = utc_tm.tm_mday,
                             .hour = utc_tm.tm_hour,
                             .minute = utc_tm.tm_min,
                             .second = utc_tm.tm_sec,
                             .subsecond_nanos = 0};
      ASSIGN_OR_RETURN(int, parsed_offset,
                       (ParseTimeZoneOffset(raw_str(arguments[0]), &utc_ct,
                                            /*at_utc=*/true)));
      tz_offset_sec = parsed_offset;
    }
    time_t now = utc_now + tz_offset_sec;
    struct tm t = {};
    platform::GmTime(static_cast<int64_t>(now), &t);
    std::array<char, 32> buf{};
    (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d", t.tm_hour,
                   t.tm_min, t.tm_sec);
    return Value(std::string(buf.data()));
  }
  if (name_view == "user" || name_view == "session_user" ||
      name_view == "current_user") {
    if (!arguments.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "USER takes no arguments");
    }
    // No multi-user session layer exists yet; report the OS account, which
    // is the identity a tinylamb server process runs as.
    const char* os_user = std::getenv("USER");
    if (os_user == nullptr || *os_user == '\0') {
      os_user = std::getenv("LOGNAME");
    }
    return Value(std::string(os_user != nullptr ? os_user : "tinylamb"));
  }
  if (name_view == "current_datetime") {
    if (arguments.size() > 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CURRENT_DATETIME takes at most 1 argument");
    }
    if (arguments.size() == 1 && arguments[0].IsNull()) {
      return Value();
    }
    // Resolve the zone offset at the current instant, not at 2000-01-01.
    const time_t utc_now = time(nullptr);
    struct tm utc_tm = {};
    platform::GmTime(static_cast<int64_t>(utc_now), &utc_tm);
    const CivilTime utc_ct{.year = utc_tm.tm_year + 1900,
                           .month = utc_tm.tm_mon + 1,
                           .day = utc_tm.tm_mday,
                           .hour = utc_tm.tm_hour,
                           .minute = utc_tm.tm_min,
                           .second = utc_tm.tm_sec,
                           .subsecond_nanos = 0};
    int tz_offset_sec = 0;
    if (arguments.size() == 1 && !arguments[0].IsNull()) {
      ASSIGN_OR_RETURN(int, parsed_offset,
                       (ParseTimeZoneOffset(raw_str(arguments[0]), &utc_ct,
                                            /*at_utc=*/true)));
      tz_offset_sec = parsed_offset;
    } else {
      ASSIGN_OR_RETURN(int, parsed_offset,
                       (ParseTimeZoneOffset(GetDefaultTimeZone(), &utc_ct,
                                            /*at_utc=*/true)));
      tz_offset_sec = parsed_offset;
    }
    time_t now = utc_now + tz_offset_sec;
    struct tm t = {};
    platform::GmTime(static_cast<int64_t>(now), &t);
    CivilTime current{.year = t.tm_year + 1900,
                      .month = t.tm_mon + 1,
                      .day = t.tm_mday,
                      .hour = t.tm_hour,
                      .minute = t.tm_min,
                      .second = t.tm_sec,
                      .subsecond_nanos = 0};
    return Value(FormatCivilTime(current));
  }
  if (name_view == "current_date") {
    if (arguments.size() > 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CURRENT_DATE takes at most 1 argument");
    }
    if (arguments.size() == 1 && arguments[0].IsNull()) {
      return Value();
    }
    // Resolve the zone offset at the current instant (see current_datetime).
    const time_t utc_now = time(nullptr);
    struct tm utc_tm = {};
    platform::GmTime(static_cast<int64_t>(utc_now), &utc_tm);
    const CivilTime utc_ct{.year = utc_tm.tm_year + 1900,
                           .month = utc_tm.tm_mon + 1,
                           .day = utc_tm.tm_mday,
                           .hour = utc_tm.tm_hour,
                           .minute = utc_tm.tm_min,
                           .second = utc_tm.tm_sec,
                           .subsecond_nanos = 0};
    int tz_offset_sec = 0;
    if (arguments.size() == 1 && !arguments[0].IsNull()) {
      ASSIGN_OR_RETURN(int, parsed_offset,
                       (ParseTimeZoneOffset(raw_str(arguments[0]), &utc_ct,
                                            /*at_utc=*/true)));
      tz_offset_sec = parsed_offset;
    } else {
      ASSIGN_OR_RETURN(int, parsed_offset,
                       (ParseTimeZoneOffset(GetDefaultTimeZone(), &utc_ct,
                                            /*at_utc=*/true)));
      tz_offset_sec = parsed_offset;
    }
    time_t now = utc_now + tz_offset_sec;
    struct tm t = {};
    platform::GmTime(static_cast<int64_t>(now), &t);
    std::chrono::year_month_day ymd{
        std::chrono::year{t.tm_year + 1900},
        std::chrono::month{static_cast<unsigned>(t.tm_mon + 1)},
        std::chrono::day{static_cast<unsigned>(t.tm_mday)}};
    return Value::DateFromDays(
        std::chrono::sys_days{ymd}.time_since_epoch().count());
  }
  if (name_view == "string") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "STRING requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    if (arguments.size() == 2) {
      ASSIGN_OR_RETURN(CivilTime, ct, (TryValueToCivilTime(arguments[0])));
      std::string tz_str = raw_str(arguments[1]);
      // T4097: mirror of the AST arm -- the two-argument form renders a
      // TIMESTAMP instant, so resolve the zone's offset AT that instant
      // (FORMAT_TIMESTAMP semantics), not as local wall-clock fields.
      ASSIGN_OR_RETURN(int, tz_offset_sec,
                       (ParseTimeZoneOffset(tz_str, &ct, /*at_utc=*/true)));
      // Apply the whole offset (minute *and* second residues) in one go so
      // historic LMT zones do not lose their sub-minute component; this is
      // the same rule the AST evaluator uses.
      ct = ShiftCivilTimeSeconds(ct, tz_offset_sec);
      return Value(FormatCivilTime(ct) + FormatTimeZoneOffset(tz_offset_sec));
    }
    if (arguments[0].type == ValueType::kDate) {
      ASSIGN_OR_RETURN(std::string, text,
                       TryFormatDateDays(arguments[0].DateDays()));
      return Value(std::move(text));
    }
    return Value(raw_str(arguments[0]));
  }
  if (name_view == "date") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "DATE takes 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    if (arguments[0].type == ValueType::kDate && arguments.size() == 1) {
      return arguments[0];
    }
    std::string s = raw_str(arguments[0]);
    if (s.size() >= 10 && s[4] == '-' && s[7] == '-') {
      // A bare YYYY-MM-DD with a timezone argument is still a zoned instant
      // (UTC midnight rendered in that zone), so it takes the same
      // conversion as the timestamp form instead of dropping arguments[1].
      // Only the 1-argument call keeps the fast path below.
      if (s.size() > 10 || arguments.size() == 2) {
        int Y = 0, M = 0, D = 0, h = 0, m = 0;
        double sec = 0;
        const int fields =
            s.size() > 10
                ? sscanf(  // NOLINT(cert-err34-c) - conversion count is
                           // checked.
                      s.c_str(), "%d-%d-%d %d:%d:%lf", &Y, &M, &D, &h, &m, &sec)
                : sscanf(  // NOLINT(cert-err34-c) - conversion count is
                           // checked.
                      s.c_str(), "%d-%d-%d", &Y, &M, &D);
        if (fields >= (s.size() > 10 ? 5 : 3)) {
          const CivilTime ct_parsed{.year = Y,
                                    .month = M,
                                    .day = D,
                                    .hour = h,
                                    .minute = m,
                                    .second = static_cast<int>(sec)};
          const std::string_view zone = ExplicitZoneSuffix(s);
          const bool has_tz = !zone.empty();
          int tz_offset_sec = 0;
          if (has_tz) {
            ASSIGN_OR_RETURN(int, parsed_tz,
                             (ParseTimeZoneOffset(zone, &ct_parsed)));
            tz_offset_sec = parsed_tz;
          }
          if (arguments.size() == 2) {
            std::string tz2 = raw_str(arguments[1]);
            ASSIGN_OR_RETURN(int, tz2_sec,
                             (ParseTimeZoneOffset(tz2, &ct_parsed)));
            struct tm t = {};
            t.tm_year = Y - 1900;
            t.tm_mon = M - 1;
            t.tm_mday = D;
            t.tm_hour = h;
            t.tm_min = m;
            t.tm_sec = static_cast<int>(sec);
            const int64_t epoch =
                platform::TimeGm(&t) - tz_offset_sec + tz2_sec;
            struct tm target = {};
            platform::GmTime(epoch, &target);
            std::chrono::year_month_day ymd{
                std::chrono::year{target.tm_year + 1900},
                std::chrono::month{static_cast<unsigned>(target.tm_mon + 1)},
                std::chrono::day{static_cast<unsigned>(target.tm_mday)}};
            return Value::DateFromDays(
                std::chrono::sys_days{ymd}.time_since_epoch().count());
          }
          if (has_tz) {
            ASSIGN_OR_RETURN(
                int, default_tz_sec,
                (ParseTimeZoneOffset(GetDefaultTimeZone(), &ct_parsed)));
            struct tm t = {};
            t.tm_year = Y - 1900;
            t.tm_mon = M - 1;
            t.tm_mday = D;
            t.tm_hour = h;
            t.tm_min = m;
            t.tm_sec = static_cast<int>(sec);
            const int64_t epoch =
                platform::TimeGm(&t) - tz_offset_sec + default_tz_sec;
            struct tm target = {};
            platform::GmTime(epoch, &target);
            std::chrono::year_month_day ymd{
                std::chrono::year{target.tm_year + 1900},
                std::chrono::month{static_cast<unsigned>(target.tm_mon + 1)},
                std::chrono::day{static_cast<unsigned>(target.tm_mday)}};
            return Value::DateFromDays(
                std::chrono::sys_days{ymd}.time_since_epoch().count());
          }
        }
      }
      return Value::TryDate(s.substr(0, 10));
    }
    return Value::TryDate(s);
  }
  if (name_view == "datetime") {
    if (arguments.empty() || arguments.size() > 7) {
      return StatusError(StatusCode::kInvalidArgument,
                         "DATETIME takes 1 to 7 arguments");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments.size() == 1) {
      if (arguments[0].type == ValueType::kDate) {
        return Value(FormatDateDays(arguments[0].DateDays()) + " 00:00:00");
      }
      ASSIGN_OR_RETURN(CivilTime, parsed, (TryValueToCivilTime(arguments[0])));
      // DATETIME(timestamp) == DATETIME(timestamp, <session default zone>).
      ASSIGN_OR_RETURN(
          CivilTime, ct,
          (ExplicitZoneTextToDefaultCivil(raw_str(arguments[0]), parsed)));
      return Value(FormatCivilTime(ct));
    }
    if (arguments.size() == 2) {
      std::string arg1_str = raw_str(arguments[1]);
      if (arg1_str.find(':') != std::string::npos && arg1_str.size() <= 8 &&
          arg1_str[0] != '+' && arg1_str[0] != '-') {
        std::string d = raw_str(arguments[0]);
        if (d.size() > 10) {
          d = d.substr(0, 10);
        }
        return Value(d + " " + arg1_str);
      }
      ASSIGN_OR_RETURN(CivilTime, ct, (TryValueToCivilTime(arguments[0])));
      // T5030: DATETIME(TIMESTAMP, zone) renders an instant, so resolve the
      // zone's offset AT that UTC instant (T4097 / FORMAT_TIMESTAMP family),
      // not against the UTC wall-clock fields as if they were local time.
      ASSIGN_OR_RETURN(int, tz_offset_sec,
                       (ParseTimeZoneOffset(arg1_str, &ct, /*at_utc=*/true)));
      if (tz_offset_sec != 0) {
        platform::Int128 ns =
            CivilTimeToNanos(ct) +
            (static_cast<platform::Int128>(tz_offset_sec) * 1000000000);
        ct = NanosToCivilTime(ns);
      }
      return Value(FormatCivilTime(ct));
    }
    if (arguments.size() == 3) {
      std::string components = raw_str(arguments[0]);
      components.resize(std::min<size_t>(components.size(), 10));
      components.push_back(' ');
      components.append(raw_str(arguments[1]));
      CivilTime ct;
      if (!ParseCivilTime(components, &ct)) {
        return StatusError(StatusCode::kInvalidArgument,
                           "invalid DATETIME components: " + components);
      }
      return Value(FormatCivilTime(ct));
    }
    if (arguments.size() >= 6) {
      const auto int_arg = [&](const Value& argument) -> StatusOr<int64_t> {
        if (argument.type == ValueType::kInt64) {
          return argument.value.int_value;
        }
        return ParseI64Strict(raw_str(argument));
      };
      ASSIGN_OR_RETURN(int64_t, year_val, int_arg(arguments[0]));
      ASSIGN_OR_RETURN(int64_t, month_val, int_arg(arguments[1]));
      ASSIGN_OR_RETURN(int64_t, day_val, int_arg(arguments[2]));
      ASSIGN_OR_RETURN(int64_t, hour_val, int_arg(arguments[3]));
      ASSIGN_OR_RETURN(int64_t, minute_val, int_arg(arguments[4]));
      ASSIGN_OR_RETURN(int64_t, second_val, int_arg(arguments[5]));
      CivilTime ct;
      ct.year = static_cast<int>(year_val);
      ct.month = static_cast<int>(month_val);
      ct.day = static_cast<int>(day_val);
      ct.hour = static_cast<int>(hour_val);
      ct.minute = static_cast<int>(minute_val);
      ct.second = static_cast<int>(second_val);
      if (arguments.size() == 7) {
        if (arguments[6].type == ValueType::kInt64) {
          ct.subsecond_nanos = arguments[6].value.int_value;
        } else {
          ASSIGN_OR_RETURN(int64_t, sub, ParseI64Strict(raw_str(arguments[6])));
          ct.subsecond_nanos = sub;
        }
      }
      return Value(FormatCivilTime(ct));
    }
  }
  if (name_view == "timestamp") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TIMESTAMP takes 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    ASSIGN_OR_RETURN(CivilTime, ct, (TryValueToCivilTime(arguments[0])));
    const bool has_explicit_zone =
        arguments[0].type == ValueType::kVarChar &&
        !ExplicitZoneSuffix(arguments[0].value.varchar_value).empty();
    CivilTime utc_ct = ct;
    if (!has_explicit_zone || arguments.size() == 2) {
      const std::string tz_str = (arguments.size() == 2) ? raw_str(arguments[1])
                                                         : GetDefaultTimeZone();
      ASSIGN_OR_RETURN(int, offset, (ParseTimeZoneOffset(tz_str, &ct)));
      if (!has_explicit_zone) {
        utc_ct = ShiftCivilTimeSeconds(ct, -int64_t{offset});
      }
    }
    if (utc_ct.year < 1 || utc_ct.year > 9999) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TIMESTAMP result is outside the valid year range");
    }
    return Value(FormatCivilTime(utc_ct) + "+00");
  }
  if (name_view == "time") {
    if (arguments.empty() || arguments.size() > 4) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TIME takes 1 to 4 arguments");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments.size() == 1) {
      std::string s = raw_str(arguments[0]);
      CivilTime ct;
      if (ParseCivilTime(s, &ct)) {
        ASSIGN_OR_RETURN(CivilTime, utc_ct,
                         (ExplicitZoneTextToUtcCivil(s, ct)));
        ASSIGN_OR_RETURN(CivilTime, local_ct,
                         (ExplicitZoneTextToDefaultCivil(s, utc_ct)));
        ct = local_ct;
        std::array<char, 64> buf{};
        if (ct.subsecond_nanos != 0) {
          if (ct.subsecond_nanos % 1000000 == 0) {
            (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%03ld",
                           ct.hour, ct.minute, ct.second,
                           static_cast<long>(ct.subsecond_nanos / 1000000));
          } else if (ct.subsecond_nanos % 1000 == 0) {
            (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%06ld",
                           ct.hour, ct.minute, ct.second,
                           static_cast<long>(ct.subsecond_nanos / 1000));
          } else {
            (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%09ld",
                           ct.hour, ct.minute, ct.second,
                           static_cast<long>(ct.subsecond_nanos));
          }
        } else {
          (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d", ct.hour,
                         ct.minute, ct.second);
        }
        return Value(std::string(buf.data()));
      }
      size_t space = s.find(' ');
      if (space == std::string::npos) {
        space = s.find('T');
      }
      if (space != std::string::npos) {
        s.erase(0, space + 1);
      }
      const size_t tz_pos = s.find_first_of("+-Zz");
      if (tz_pos != std::string::npos) {
        s.resize(tz_pos);
      }
      return Value(std::move(s));
    }
    if (arguments.size() == 2) {
      ASSIGN_OR_RETURN(CivilTime, ct, (TryValueToCivilTime(arguments[0])));
      std::string tz_str = raw_str(arguments[1]);
      // T5030: TIME(TIMESTAMP, zone) renders an instant -- resolve the zone
      // offset at that UTC instant (T4097 / STRING(TIMESTAMP, zone) family).
      ASSIGN_OR_RETURN(int, tz_offset_sec,
                       (ParseTimeZoneOffset(tz_str, &ct, /*at_utc=*/true)));
      if (tz_offset_sec != 0) {
        platform::Int128 ns =
            CivilTimeToNanos(ct) +
            (static_cast<platform::Int128>(tz_offset_sec) * 1000000000);
        ct = NanosToCivilTime(ns);
      }
      std::array<char, 64> buf{};
      if (ct.subsecond_nanos != 0) {
        if (ct.subsecond_nanos % 1000000 == 0) {
          (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%03ld",
                         ct.hour, ct.minute, ct.second,
                         static_cast<long>(ct.subsecond_nanos / 1000000));
        } else if (ct.subsecond_nanos % 1000 == 0) {
          (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%06ld",
                         ct.hour, ct.minute, ct.second,
                         static_cast<long>(ct.subsecond_nanos / 1000));
        } else {
          (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%09ld",
                         ct.hour, ct.minute, ct.second,
                         static_cast<long>(ct.subsecond_nanos));
        }
      } else {
        (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d", ct.hour,
                       ct.minute, ct.second);
      }
      return Value(std::string(buf.data()));
    }
    if (arguments.size() >= 3) {
      const auto int_arg = [&](const Value& argument) -> StatusOr<int64_t> {
        if (argument.type == ValueType::kInt64) {
          return argument.value.int_value;
        }
        return ParseI64Strict(raw_str(argument));
      };
      ASSIGN_OR_RETURN(int64_t, hour_val, int_arg(arguments[0]));
      ASSIGN_OR_RETURN(int64_t, minute_val, int_arg(arguments[1]));
      ASSIGN_OR_RETURN(int64_t, second_val, int_arg(arguments[2]));
      const int h = static_cast<int>(hour_val);
      const int m = static_cast<int>(minute_val);
      const int s = static_cast<int>(second_val);
      std::array<char, 64> buf{};
      if (arguments.size() == 4) {
        int sub = 0;
        if (arguments[3].type == ValueType::kInt64) {
          sub = static_cast<int>(arguments[3].value.int_value);
        } else {
          ASSIGN_OR_RETURN(int64_t, sub_val,
                           ParseI64Strict(raw_str(arguments[3])));
          sub = static_cast<int>(sub_val);
        }
        // The 4th TIME() argument is nanoseconds, not microseconds.
        (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%09d", h, m, s,
                       sub);
      } else {
        (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d", h, m, s);
      }
      return Value(std::string(buf.data()));
    }
  }
  if (name_view == "__quantified__") {
    // __quantified__(lhs, array, op, ANY|ALL): three-valued SQL semantics.
    if (arguments.size() != 4) {
      return StatusError(StatusCode::kInvalidArgument,
                         "quantified comparison requires 4 arguments");
    }
    const Value& lhs = arguments[0];
    Value arr = arguments[1];
    const std::string op = raw_str(arguments[2]);
    const std::string mode = raw_str(arguments[3]);
    // UNNEST over a NULL array produces zero rows: the comparison is vacuous
    // (ALL -> TRUE, ANY -> FALSE), not UNKNOWN.
    if (arr.IsNull()) {
      if (mode == "ANY" || mode == "SOME") {
        return Value(int64_t{0});
      }
      return Value(int64_t{1});
    }
    if (!arr.IsArray()) {
      if (arr.IsNull()) {
        // Handled above; unreachable, kept for clarity.
        return StatusError(StatusCode::kInvalidArgument,
                           "quantified comparison requires an array");
      }
      // Field traversals collapse a single-occurrence repeated field to its
      // scalar; UNNEST iterates it as a one-element array.
      const std::string element_type(ElementSqlTypeName(arr.type));
      arr = Value::Array({std::move(arr)},
                         element_type.empty() ? "INT64" : element_type);
    }
    struct OpEntry {
      const char* text;
      BinaryOperation op;
    };
    static constexpr std::array<OpEntry, 7> kOps = {{
        {.text = "=", .op = BinaryOperation::kEquals},
        {.text = "!=", .op = BinaryOperation::kNotEquals},
        {.text = "<>", .op = BinaryOperation::kNotEquals},
        {.text = "<", .op = BinaryOperation::kLessThan},
        {.text = "<=", .op = BinaryOperation::kLessThanEquals},
        {.text = ">", .op = BinaryOperation::kGreaterThan},
        {.text = ">=", .op = BinaryOperation::kGreaterThanEquals},
    }};
    BinaryOperation operation = BinaryOperation::kEquals;
    bool found = false;
    for (const auto& entry : kOps) {
      if (op == entry.text) {
        operation = entry.op;
        found = true;
        break;
      }
    }
    if (!found) {
      return StatusError(StatusCode::kInvalidArgument,
                         "quantified comparison: unsupported operator " + op);
    }
    const bool is_any = mode == "ANY" || mode == "SOME";
    // Collation resolution: an explicit case-insensitive collator on any
    // element applies to the whole comparison set.
    bool any_ci = lhs.IsCaseInsensitive();
    for (const Value& element : arr.ArrayElements()) {
      any_ci = any_ci || element.IsCaseInsensitive();
    }
    Value test_value = lhs;
    if (any_ci && lhs.type == ValueType::kVarChar) {
      std::vector<Value> folded;
      folded.reserve(arr.ArrayElements().size());
      for (const Value& element : arr.ArrayElements()) {
        if (element.type == ValueType::kVarChar) {
          folded.push_back(Value(FoldCase(element.value.varchar_value))
                               .WithCollation(element.Collation()));
        } else {
          folded.push_back(element);
        }
      }
      arr = Value::Array(std::move(folded), arr.ArrayElementSqlType());
      test_value = Value(FoldCase(lhs.value.varchar_value))
                       .WithCollation(lhs.Collation());
    }
    bool saw_true = false;
    bool saw_null = false;
    // Collated LIKE rejects the '_' wildcard outright (validation error, not
    // a per-row UNKNOWN).
    if (operation == BinaryOperation::kLike ||
        operation == BinaryOperation::kNotLike) {
      if (any_ci) {
        for (const Value& element : arr.ArrayElements()) {
          if (!element.IsNull() && element.type == ValueType::kVarChar &&
              std::string_view(element.value.varchar_value).find('_') !=
                  std::string_view::npos) {
            return StatusError(
                StatusCode::kInvalidArgument,
                "LIKE pattern has '_' which is not allowed when its operands "
                "have collation: " +
                    std::string(element.value.varchar_value));
          }
        }
      }
    }
    for (const Value& element : arr.ArrayElements()) {
      if (operation == BinaryOperation::kEquals ||
          operation == BinaryOperation::kNotEquals) {
        // Struct-encoded operands need positional three-valued equality;
        // raw string comparison would turn NULL fields into FALSE.
        const int tri = ThreeValuedEquals(test_value, element);
        const bool is_null = tri < 0;
        const bool equal =
            operation == BinaryOperation::kEquals ? tri == 1 : tri == 0;
        if (is_null) {
          saw_null = true;
          continue;
        }
        if (equal) {
          saw_true = true;
          if (is_any) {
            return Value(int64_t{1});
          }
        } else if (!is_any) {
          return Value(int64_t{0});
        }
        continue;
      }
      Value result;
      StatusOr<Value> cmp = TryBinary(operation, test_value, element);
      if (!cmp.HasValue()) {
        result = Value();
      } else {
        result = cmp.MoveValue();
      }
      if (result.IsNull()) {
        saw_null = true;
        continue;
      }
      if (Truthy(result)) {
        saw_true = true;
        if (is_any) {
          return Value(int64_t{1});
        }
      } else if (!is_any) {
        return Value(int64_t{0});
      }
    }
    if (is_any) {
      if (saw_true) {
        return Value(int64_t{1});
      }
      return saw_null ? Value() : Value(int64_t{0});
    }
    // ALL: true when nothing was false and no NULL blocked certainty.
    if (saw_null) {
      return Value();
    }
    return Value(int64_t{1});
  }
  if (name_view == "array_element_offset" ||
      name_view == "array_element_ordinal" ||
      name_view == "array_element_safe_offset" ||
      name_view == "array_element_safe_ordinal") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "array element access requires 2 arguments");
    }
    const bool safe = name.find("safe") != std::string::npos;
    const Value& arr = arguments[0];
    if (arr.IsNull()) {
      return Value();
    }
    if (!arr.IsArray()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "array element access requires an array");
    }
    // A NULL index yields NULL rather than an error (even for non-SAFE).
    if (arguments[1].IsNull()) {
      return Value();
    }
    if (arguments[1].type != ValueType::kInt64 || arguments[1].IsUnsigned()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "array index requires INT64");
    }
    const auto& elements = arr.ArrayElements();
    const int64_t requested_index = arguments[1].value.int_value;
    int64_t index = requested_index;
    if (name.find("ordinal") != std::string::npos && index > 0) {
      --index;
    } else if (name.find("ordinal") != std::string::npos) {
      index = -1;
    }
    if (index < 0 || std::cmp_greater_equal(index, elements.size())) {
      // Out-of-range plain accesses are errors in GoogleSQL; only the SAFE
      // variants yield NULL.
      if (!safe) {
        return StatusError(StatusCode::kInvalidArgument,
                           "Array index " + std::to_string(requested_index) +
                               " is out of bounds");
      }
      return Value();
    }
    return elements[static_cast<size_t>(index)];
  }
  if (name_view == "__bit_and" || name_view == "__bit_or" ||
      name_view == "__bit_xor" || name_view == "__shift_left" ||
      name_view == "__shift_right") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    if (arguments[0].type != ValueType::kInt64 ||
        arguments[1].type != ValueType::kInt64) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires integer arguments");
    }
    const int64_t lhs = arguments[0].value.int_value;
    const int64_t rhs = arguments[1].value.int_value;
    const bool is_u = arguments[0].IsUnsigned() || arguments[1].IsUnsigned();
    auto with_u = [&](const Value& v) { return is_u ? v.WithUnsigned() : v; };
    if (name_view == "__bit_and") {
      return with_u(Value(lhs & rhs));
    }
    if (name_view == "__bit_or") {
      return with_u(Value(lhs | rhs));
    }
    if (name_view == "__bit_xor") {
      return with_u(Value(lhs ^ rhs));
    }
    // An UNSIGNED shift count is a huge positive number, not a negative
    // offset: counts >= 64 yield 0 just like the signed path.
    if (arguments[1].IsUnsigned()) {
      const auto shift = static_cast<uint64_t>(arguments[1].value.int_value);
      if (shift >= 64) {
        return with_u(Value(static_cast<int64_t>(0)));
      }
      const auto ulhs = static_cast<uint64_t>(lhs);
      const uint64_t shifted =
          name_view == "__shift_left" ? ulhs << shift : ulhs >> shift;
      return with_u(Value(static_cast<int64_t>(shifted)));
    }
    if (rhs < 0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "Bitwise shift by negative offset.");
    }
    if (rhs >= 64) {
      return with_u(Value(static_cast<int64_t>(0)));
    }
    const auto ulhs = static_cast<uint64_t>(lhs);
    const uint64_t shifted =
        name_view == "__shift_left" ? ulhs << rhs : ulhs >> rhs;
    return with_u(Value(static_cast<int64_t>(shifted)));
  }
  if (name_view == "__proto_new") {
    // NEW ProtoType(v1 AS f1, ...) / SELECT AS ProtoType: argument layout is
    // (type_name, value1, field1, value2, field2, ...).  Builds the proto
    // TEXT payload; required-field and enum-member violations throw.
    if (arguments.empty() || arguments.size() % 2 != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__proto_new requires (type, v, f, ...)");
    }
    const std::string type_name = raw_str(arguments[0]);
    std::vector<std::pair<std::string, Value>> fields;
    fields.reserve(arguments.size() / 2);
    for (size_t i = 1; i < arguments.size(); i += 2) {
      fields.emplace_back(raw_str(arguments[i + 1]), arguments[i]);
    }
    return Value(ConstructProtoText(type_name, fields));
  }
  if (name_view == "__value_table_value") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__value_table_value requires one argument");
    }
    return arguments.front();
  }
  if (name_view == "__value_table_proto") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__value_table_proto requires two arguments");
    }
    if (arguments[1].IsNull()) {
      return Value();
    }
    const std::string type_name = raw_str(arguments[0]);
    if (type_name.find("TestExtraPB") == std::string::npos) {
      return arguments[1];
    }
    std::vector<std::pair<std::string, Value>> fields;
    fields.reserve(3);
    for (const char* field : {"int32_val1", "int32_val2", "str_value"}) {
      Value value;
      if (TryProtoTextGetField(raw_str(arguments[1]), field, &value)) {
        fields.emplace_back(field, std::move(value));
      }
    }
    return Value(ConstructProtoText(type_name, fields));
  }
  if (name_view == "__value_table_proto_existing") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__value_table_proto_existing requires two arguments");
    }
    if (arguments[1].IsNull()) {
      return Value();
    }
    const std::string type_name = raw_str(arguments[0]);
    if (type_name.find("TestExtraPB") == std::string::npos) {
      return arguments[1];
    }
    std::vector<std::pair<std::string, Value>> fields;
    fields.reserve(3);
    const std::string payload = raw_str(arguments[1]);
    for (const char* field : {"int32_val1", "int32_val2", "str_value"}) {
      if (!ProtoTextHasField(payload, field)) {
        continue;
      }
      Value value;
      if (TryProtoTextGetField(payload, field, &value)) {
        fields.emplace_back(field, std::move(value));
      }
    }
    return Value(ConstructProtoText(type_name, fields));
  }
  if (name_view == "__proto_set") {
    // Dotted SET targets over proto TEXT columns: (payload, path, new_value).
    // NULL payloads have no field structure to update.
    if (arguments.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__proto_set requires 3 arguments");
    }
    std::vector<std::string> path;
    {
      const std::string joined = raw_str(arguments[1]);
      size_t start = 0;
      while (true) {
        const size_t dot = joined.find('.', start);
        if (dot == std::string::npos) {
          path.push_back(joined.substr(start));
          break;
        }
        path.push_back(joined.substr(start, dot - start));
        start = dot + 1;
      }
    }
    // Resolve the type before the NULL check so error messages can name it.
    const std::string type_name = InferProtoTypeName(
        arguments[0].IsNull() ? std::string_view() : raw_str(arguments[0]),
        path);
    if (arguments[0].IsNull()) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "Cannot set field of NULL `" +
              (type_name.empty() ? std::string("PROTO") : type_name) + "`");
    }
    const std::string payload = raw_str(arguments[0]);
    // Use the Try* variant: the throwing EXC-SHIM wrappers would let a
    // validation Status escape across this Status boundary (AST parity).
    ASSIGN_OR_RETURN(
        std::optional<std::string>, rewritten,
        TryProtoTextSetField(payload, path, arguments[2], type_name));
    return Value(rewritten.value_or(payload));
  }
  if (name_view == "__get_extension") {
    // value.(pkg.Ext.field): reads the bracketed extension entry from a
    // proto TEXT payload; NULL bases yield NULL.
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__get_extension requires 2 arguments");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const StringArgView base_arg(arguments[0]);
    const std::string_view base = base_arg.view;
    const StringArgView ext_arg(arguments[1]);
    const std::string_view extension_name = ext_arg.view;
    // The bracketed key can be built as a small stack buffer for the common
    // short extension names; TryProtoTextGetField takes string_view.
    std::string key_storage;
    key_storage.reserve(extension_name.size() + 2);
    key_storage.push_back('[');
    key_storage.append(extension_name);
    key_storage.push_back(']');
    Value out;
    if (!TryProtoTextGetField(base, key_storage, &out)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "extension " + std::string(name) + " not found");
    }
    return out;
  }
  if (name_view == "unix_seconds" || name_view == "unix_millis" ||
      name_view == "unix_micros" || name_view == "unix_date") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires one TIMESTAMP argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const std::optional<platform::Int128> nanos =
        ParseTimestampTextNanos(raw_str(arguments[0]));
    if (!nanos.has_value()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "invalid TIMESTAMP: " + raw_str(arguments[0]));
    }
    auto floor_div_unix = [](platform::Int128 a,
                             platform::Int128 b) -> int64_t {
      platform::Int128 q = a / b;
      if ((a % b) != 0 && ((a < 0) != (b < 0))) {
        --q;
      }
      // Proto-text timestamps can land far outside the SQL literal range;
      // the quotient then exceeds int64. Saturate instead of narrowing.
      if (q >
          static_cast<platform::Int128>(std::numeric_limits<int64_t>::max())) {
        return std::numeric_limits<int64_t>::max();
      }
      if (q <
          static_cast<platform::Int128>(std::numeric_limits<int64_t>::min())) {
        return std::numeric_limits<int64_t>::min();
      }
      return static_cast<int64_t>(q);
    };
    if (name_view == "unix_date") {
      return Value(floor_div_unix(*nanos, 86400000000000LL));
    }
    if (name_view == "unix_seconds") {
      return Value(floor_div_unix(*nanos, 1000000000LL));
    }
    if (name_view == "unix_millis") {
      return Value(floor_div_unix(*nanos, 1000000LL));
    }
    return Value(floor_div_unix(*nanos, 1000LL));
  }
  if (name_view == "__get_field_safe") {
    // Field access tolerating NULL bases / missing members (returns NULL);
    // used for dotted struct references inside DML predicates.
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__get_field_safe requires 2 arguments");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    std::string object = raw_str(arguments[0]);
    const std::string field_name = raw_str(arguments[1]);
    Value proto_field;
    if (object.size() >= 2 && object.front() == '{' && object.back() == '}') {
      const auto members =
          SplitJsonObjectMembers(object.substr(1, object.size() - 2));
      for (const auto& [key, text] : members) {
        if (key.size() == field_name.size() &&
            std::equal(key.begin(), key.end(), field_name.begin(),
                       [](char lhs, char rhs) {
                         return std::tolower(static_cast<unsigned char>(lhs)) ==
                                std::tolower(static_cast<unsigned char>(rhs));
                       })) {
          Value parsed;
          if (!JsonTextToValue(text, &parsed)) {
            return Value();
          }
          constexpr std::string_view kDateMarker = "__tinylamb_date__:";
          if (parsed.type == ValueType::kVarChar &&
              parsed.value.varchar_value.starts_with(kDateMarker)) {
            ASSIGN_OR_RETURN(
                Value, date_decoded,
                (Value::TryDate(std::string(parsed.value.varchar_value)
                                    .substr(kDateMarker.size()))));
            parsed = std::move(date_decoded);
          }
          if (parsed.type == ValueType::kVarChar &&
              (field_name == "amin" || field_name == "amax") &&
              object.find("ARRAY<DATE>") != std::string::npos) {
            ASSIGN_OR_RETURN(Value, date_decoded,
                             (Value::TryDate(parsed.value.varchar_value)));
            parsed = std::move(date_decoded);
          }
          return parsed;
        }
      }
      // Anonymous struct members (`STRUCT(2)` stores {"f1":2}) are still
      // addressable by any field reference when unambiguous: a single-member
      // object exposes its only value positionally.
      if (members.size() == 1 && members.front().first.empty()) {
        Value parsed;
        if (!JsonTextToValue(members.front().second, &parsed)) {
          return Value();
        }
        constexpr std::string_view kDateMarker = "__tinylamb_date__:";
        if (parsed.type == ValueType::kVarChar &&
            parsed.value.varchar_value.starts_with(kDateMarker)) {
          ASSIGN_OR_RETURN(
              Value, date_decoded,
              (Value::TryDate(std::string(parsed.value.varchar_value)
                                  .substr(kDateMarker.size()))));
          parsed = std::move(date_decoded);
        }
        return parsed;
      }
    }
    // Proto text-format cells (`value: 1`) carry the same field semantics.
    if (ProtoTextExtractField(object, field_name, &proto_field)) {
      return proto_field;
    }
    return Value();
  }
  if (name_view == "get_field") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "get_field requires 2 arguments");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    std::string s0 = raw_str(arguments[0]);
    if (s0.find("\x18\x00") != std::string::npos ||
        (s0.size() >= 2 && s0[0] == '\x18')) {
      return StatusError(StatusCode::kInvalidArgument,
                         "invalid datetime_micros in proto");
    }
    std::string field_name = raw_str(arguments[1]);
    // Proto TEXT payloads (non-JSON) resolve through the shared extractor,
    // which also supplies scalar defaults / has_ bits for absent fields.
    Value proto_value;
    if (TryProtoTextGetField(s0, field_name, &proto_value)) {
      return proto_value;
    }
    if (s0.starts_with("{") && s0.ends_with("}")) {
      // Use the same quote-aware member splitter as the AST evaluator: a
      // hand-rolled find_first_of(",}") truncated values containing commas
      // inside quoted strings and left the surrounding quotes behind.
      const auto members = SplitJsonObjectMembers(s0.substr(1, s0.size() - 2));
      for (const auto& [key, text] : members) {
        if (!IdentifierEquals(key, field_name)) {
          continue;
        }
        Value parsed;
        if (!JsonTextToValue(text, &parsed)) {
          // T1647: match the AST evaluator's strict contract — a present
          // member whose value fails to decode is an error, not NULL.
          return StatusError(StatusCode::kInvalidArgument,
                             "get_field: malformed member value");
        }
        return parsed;
      }
      if (members.empty()) {
        // Positional fallback: unnamed two-field struct literals (the
        // APPROX_TOP_* payload shape) resolve value / count / sum by
        // position.
        std::string inner = s0.substr(1, s0.size() - 2);
        std::vector<std::string> fields;
        fields.reserve(inner.size());
        int depth = 0;
        bool in_str = false;
        char quote = '\0';
        std::string current;
        current.reserve(inner.size());
        for (const char c : inner) {
          if (in_str) {
            current.push_back(c);
            if (c == quote) {
              in_str = false;
            }
            continue;
          }
          if (c == '"' || c == '\'') {
            in_str = true;
            quote = c;
            current.push_back(c);
            continue;
          }
          if (c == '{' || c == '[' || c == '(') {
            ++depth;
          }
          if ((c == '}' || c == ']' || c == ')') && depth > 0) {
            --depth;
          }
          if (c == ',' && depth == 0) {
            fields.push_back(current);
            current.clear();
            continue;
          }
          current.push_back(c);
        }
        fields.push_back(current);
        auto trim_copy = [](const std::string& v) {
          size_t b = v.find_first_not_of(" \t\n\r");
          size_t e = v.find_last_not_of(" \t\n\r");
          return b == std::string::npos ? std::string()
                                        : v.substr(b, e - b + 1);
        };
        for (std::string& field : fields) {
          field = trim_copy(field);
        }
        const bool two_fields = fields.size() == 2;
        if (two_fields && field_name == "value") {
          return Value(std::move(fields[0]));
        }
        if (two_fields && (field_name == "count" || field_name == "sum")) {
          return Value(std::move(fields[1]));
        }
      }
      // T1647: AST semantics are the ground truth — a missing member is a
      // "field not found" error, not NULL.  NULL-tolerant consumers use
      // __get_field_safe (the JSON-path/lambda route).
      return StatusError(StatusCode::kInvalidArgument,
                         "field not found: " + field_name);
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "get_field requires a STRUCT");
  }
  if (diffs_civil_time) {
    if (arguments.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 3 arguments");
    }
    const Value& d1 = arguments[0];
    const Value& d2 = arguments[1];
    if (d1.IsNull() || d2.IsNull()) {
      return Value();
    }
    const std::string unit =
        arguments[2].IsNull() ? "day" : to_lower(raw_str(arguments[2]));
    ASSIGN_OR_RETURN(CivilTime, parsed1, (TryValueToCivilTime(d1)));
    ASSIGN_OR_RETURN(CivilTime, parsed2, (TryValueToCivilTime(d2)));
    // Zoned (TIMESTAMP) operands are compared as civil time in the session
    // default zone (UTC by default), exactly like DATETIME(ts) would render
    // them; zone-less DATETIME/DATE operands are already civil time.
    ASSIGN_OR_RETURN(CivilTime, ct1,
                     (ExplicitZoneTextToDefaultCivil(raw_str(d1), parsed1)));
    ASSIGN_OR_RETURN(CivilTime, ct2,
                     (ExplicitZoneTextToDefaultCivil(raw_str(d2), parsed2)));
    if (unit == "year" || unit == "years") {
      return Value(static_cast<int64_t>(ct1.year - ct2.year));
    }
    if (unit == "quarter" || unit == "quarters") {
      return Value(
          static_cast<int64_t>(((ct1.year - ct2.year) * 4) +
                               ((ct1.month - 1) / 3 - (ct2.month - 1) / 3)));
    }
    if (unit == "month" || unit == "months") {
      return Value(static_cast<int64_t>(((ct1.year - ct2.year) * 12) +
                                        (ct1.month - ct2.month)));
    }
    if (unit == "isoyear" || unit == "isoyears") {
      // The ISO year is the calendar year of the Thursday of the date's ISO
      // week (the extract_isoyear implementation uses the same rule).
      const auto iso_year_of = [](const CivilTime& ct) {
        std::chrono::year_month_day ymd{
            std::chrono::year{ct.year},
            std::chrono::month{static_cast<unsigned>(ct.month)},
            std::chrono::day{static_cast<unsigned>(ct.day)}};
        const std::chrono::sys_days sd{ymd};
        const int wd =
            static_cast<int>(std::chrono::weekday{sd}.iso_encoding());
        const std::chrono::sys_days thu = sd + std::chrono::days{4 - wd};
        return static_cast<int>(std::chrono::year_month_day{thu}.year());
      };
      return Value(static_cast<int64_t>(iso_year_of(ct1) - iso_year_of(ct2)));
    }

    auto trunc_to_unit = [](CivilTime ct, std::string_view u) -> CivilTime {
      if (u == "week" || u == "weeks") {
        std::chrono::year_month_day ymd{
            std::chrono::year{ct.year},
            std::chrono::month{static_cast<unsigned>(ct.month)},
            std::chrono::day{static_cast<unsigned>(ct.day)}};
        std::chrono::sys_days sd{ymd};
        int wd = static_cast<int>(std::chrono::weekday{sd}.iso_encoding());
        int days_back = (wd == 7) ? 0 : wd;
        std::chrono::sys_days week_start = sd - std::chrono::days{days_back};
        std::chrono::year_month_day res_ymd{week_start};
        ct.year = int(res_ymd.year());
        ct.month = static_cast<int>(static_cast<unsigned>(res_ymd.month()));
        ct.day = static_cast<int>(static_cast<unsigned>(res_ymd.day()));
        ct.hour = 0;
        ct.minute = 0;
        ct.second = 0;
        ct.subsecond_nanos = 0;
      } else if (u == "isoweek" || u == "isoweeks" ||
                 (u.starts_with("week(") && u.ends_with(")"))) {
        // ISO weeks start Monday; WEEK(<weekday>) starts on the named day and
        // a bare WEEK starts Sunday. Truncate both operands to their week's
        // first day, then the day-difference below yields whole weeks.
        int start_iso = 7;  // Sunday
        if (u == "isoweek" || u == "isoweeks" || u == "week(monday)") {
          start_iso = 1;
        } else if (u == "week(tuesday)") {
          start_iso = 2;
        } else if (u == "week(wednesday)") {
          start_iso = 3;
        } else if (u == "week(thursday)") {
          start_iso = 4;
        } else if (u == "week(friday)") {
          start_iso = 5;
        } else if (u == "week(saturday)") {
          start_iso = 6;
        }
        std::chrono::year_month_day ymd{
            std::chrono::year{ct.year},
            std::chrono::month{static_cast<unsigned>(ct.month)},
            std::chrono::day{static_cast<unsigned>(ct.day)}};
        const std::chrono::sys_days sd{ymd};
        const int cur_iso =
            static_cast<int>(std::chrono::weekday{sd}.iso_encoding());
        const int days_back = (cur_iso - start_iso + 7) % 7;
        const std::chrono::sys_days week_start =
            sd - std::chrono::days{days_back};
        const std::chrono::year_month_day res_ymd{week_start};
        ct.year = int(res_ymd.year());
        ct.month = static_cast<int>(static_cast<unsigned>(res_ymd.month()));
        ct.day = static_cast<int>(static_cast<unsigned>(res_ymd.day()));
        ct.hour = 0;
        ct.minute = 0;
        ct.second = 0;
        ct.subsecond_nanos = 0;
      } else if (u == "day" || u == "days") {
        ct.hour = 0;
        ct.minute = 0;
        ct.second = 0;
        ct.subsecond_nanos = 0;
      } else if (u == "hour" || u == "hours") {
        ct.minute = 0;
        ct.second = 0;
        ct.subsecond_nanos = 0;
      } else if (u == "minute" || u == "minutes") {
        ct.second = 0;
        ct.subsecond_nanos = 0;
      } else if (u == "second" || u == "seconds") {
        ct.subsecond_nanos = 0;
      } else if (u == "millisecond" || u == "milliseconds") {
        ct.subsecond_nanos = (ct.subsecond_nanos / 1000000LL) * 1000000LL;
      } else if (u == "microsecond" || u == "microseconds") {
        ct.subsecond_nanos = (ct.subsecond_nanos / 1000LL) * 1000LL;
      }
      return ct;
    };

    CivilTime t1 = trunc_to_unit(ct1, unit);
    CivilTime t2 = trunc_to_unit(ct2, unit);
    platform::Int128 ns1 = CivilTimeToNanos(t1);
    platform::Int128 ns2 = CivilTimeToNanos(t2);
    platform::Int128 diff_ns = ns1 - ns2;
    if (unit == "week" || unit == "weeks" || unit == "isoweek" ||
        unit == "isoweeks" ||
        (unit.starts_with("week(") && unit.ends_with(")"))) {
      return Value(
          static_cast<int64_t>(diff_ns / (static_cast<platform::Int128>(7) *
                                          86400LL * 1000000000LL)));
    }
    if (unit == "day" || unit == "days") {
      return Value(static_cast<int64_t>(
          diff_ns / (static_cast<platform::Int128>(86400) * 1000000000LL)));
    }
    if (unit == "hour" || unit == "hours") {
      return Value(static_cast<int64_t>(
          diff_ns / (static_cast<platform::Int128>(3600) * 1000000000LL)));
    }
    if (unit == "minute" || unit == "minutes") {
      return Value(static_cast<int64_t>(
          diff_ns / (static_cast<platform::Int128>(60) * 1000000000LL)));
    }
    if (unit == "second" || unit == "seconds") {
      return Value(static_cast<int64_t>(diff_ns / 1000000000LL));
    }
    if (unit == "millisecond" || unit == "milliseconds") {
      return Value(static_cast<int64_t>(diff_ns / 1000000LL));
    }
    if (unit == "microsecond" || unit == "microseconds") {
      return Value(static_cast<int64_t>(diff_ns / 1000LL));
    }
    if (unit == "nanosecond" || unit == "nanoseconds") {
      if (diff_ns < std::numeric_limits<int64_t>::min() ||
          diff_ns > std::numeric_limits<int64_t>::max()) {
        return StatusError(StatusCode::kIsInfinity,
                           "nanosecond diff out of int64 range");
      }
      return Value(static_cast<int64_t>(diff_ns));
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "unsupported unit in " + name + ": " + unit);
  }
  if (name_view == "date_trunc" || name_view == "datetime_trunc" ||
      name_view == "timestamp_trunc") {
    if (call.Args().size() < 2 || call.Args().size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 2 or 3 arguments");
    }
    // Ordinary arguments were evaluated once above, in source order.
    const Value& d = arguments[0];
    if (d.IsNull()) {
      return Value();
    }
    std::string unit = "day";
    if (call.Args()[1]->Type() == TypeTag::kColumnValue) {
      unit = to_lower(call.Args()[1]->AsColumnValue().GetColumnName().name);
    } else {
      const Value& uval = arguments[1];

      if (!uval.IsNull()) {
        unit = to_lower(raw_str(uval));
      }
    }
    const bool has_explicit_tz = !ExplicitZoneSuffix(raw_str(d)).empty();
    bool is_timestamp = (name_view == "timestamp_trunc" || has_explicit_tz);
    std::string trunc_tz = GetDefaultTimeZone();
    if (call.Args().size() == 3) {
      const Value& tz_val = arguments[2];

      if (tz_val.IsNull()) {
        return Value();
      }
      std::string tz_str = raw_str(tz_val);
      trunc_tz = tz_str;
    }

    ASSIGN_OR_RETURN(CivilTime, ct, (TryValueToCivilTime(d)));
    if (is_timestamp) {
      int parse_tz_sec = 0;
      if (!has_explicit_tz) {
        {
          ASSIGN_OR_RETURN(int, parsed_offset,
                           (ParseTimeZoneOffset(GetDefaultTimeZone(), &ct)));
          parse_tz_sec = parsed_offset;
        }
      }
      ASSIGN_OR_RETURN(int, trunc_tz_sec, (ParseTimeZoneOffset(trunc_tz, &ct)));
      CivilTime trunc_local_ct = ct;
      if (parse_tz_sec != trunc_tz_sec) {
        const platform::Int128 shift =
            (static_cast<platform::Int128>(trunc_tz_sec) - parse_tz_sec) *
            1000000000;
        trunc_local_ct = NanosToCivilTime(CivilTimeToNanos(ct) + shift);
      }
      if (unit == "year" || unit == "years") {
        trunc_local_ct.month = 1;
        trunc_local_ct.day = 1;
        trunc_local_ct.hour = 0;
        trunc_local_ct.minute = 0;
        trunc_local_ct.second = 0;
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "quarter" || unit == "quarters") {
        trunc_local_ct.month = (((trunc_local_ct.month - 1) / 3) * 3) + 1;
        trunc_local_ct.day = 1;
        trunc_local_ct.hour = 0;
        trunc_local_ct.minute = 0;
        trunc_local_ct.second = 0;
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "month" || unit == "months") {
        trunc_local_ct.day = 1;
        trunc_local_ct.hour = 0;
        trunc_local_ct.minute = 0;
        trunc_local_ct.second = 0;
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "week" || unit == "weeks") {
        std::chrono::year_month_day ymd{
            std::chrono::year{trunc_local_ct.year},
            std::chrono::month{static_cast<unsigned>(trunc_local_ct.month)},
            std::chrono::day{static_cast<unsigned>(trunc_local_ct.day)}};
        std::chrono::sys_days sd{ymd};
        int wd = static_cast<int>(std::chrono::weekday{sd}.iso_encoding());
        int days_back = (wd == 7) ? 0 : wd;
        std::chrono::sys_days week_start = sd - std::chrono::days{days_back};
        std::chrono::year_month_day res_ymd{week_start};
        trunc_local_ct.year = int(res_ymd.year());
        trunc_local_ct.month =
            static_cast<int>(static_cast<unsigned>(res_ymd.month()));
        trunc_local_ct.day =
            static_cast<int>(static_cast<unsigned>(res_ymd.day()));
        trunc_local_ct.hour = 0;
        trunc_local_ct.minute = 0;
        trunc_local_ct.second = 0;
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "day" || unit == "days") {
        trunc_local_ct.hour = 0;
        trunc_local_ct.minute = 0;
        trunc_local_ct.second = 0;
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "hour" || unit == "hours") {
        trunc_local_ct.minute = 0;
        trunc_local_ct.second = 0;
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "minute" || unit == "minutes") {
        trunc_local_ct.second = 0;
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "second" || unit == "seconds") {
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "millisecond" || unit == "milliseconds") {
        trunc_local_ct.subsecond_nanos =
            (trunc_local_ct.subsecond_nanos / 1000000LL) * 1000000LL;
      } else if (unit == "microsecond" || unit == "microseconds") {
        trunc_local_ct.subsecond_nanos =
            (trunc_local_ct.subsecond_nanos / 1000LL) * 1000LL;
      } else if (unit == "nanosecond" || unit == "nanoseconds") {
        // no change
      } else {
        return StatusError(StatusCode::kInvalidArgument,
                           "unsupported unit in " + name + ": " + unit);
      }
      ASSIGN_OR_RETURN(int, new_trunc_tz_sec,
                       (ParseTimeZoneOffset(trunc_tz, &trunc_local_ct)));
      CivilTime res_utc_ct = trunc_local_ct;
      if (new_trunc_tz_sec != 0) {
        const platform::Int128 res_utc_ns =
            CivilTimeToNanos(trunc_local_ct) -
            (static_cast<platform::Int128>(new_trunc_tz_sec) * 1000000000);
        res_utc_ct = NanosToCivilTime(res_utc_ns);
      }
      return Value(FormatCivilTime(res_utc_ct) + "+00");
    }
    if (unit == "year" || unit == "years") {
      ct.month = 1;
      ct.day = 1;
      ct.hour = 0;
      ct.minute = 0;
      ct.second = 0;
      ct.subsecond_nanos = 0;
    } else if (unit == "quarter" || unit == "quarters") {
      ct.month = (((ct.month - 1) / 3) * 3) + 1;
      ct.day = 1;
      ct.hour = 0;
      ct.minute = 0;
      ct.second = 0;
      ct.subsecond_nanos = 0;
    } else if (unit == "month" || unit == "months") {
      ct.day = 1;
      ct.hour = 0;
      ct.minute = 0;
      ct.second = 0;
      ct.subsecond_nanos = 0;
    } else if (unit == "week" || unit == "weeks") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      std::chrono::sys_days sd{ymd};
      int wd = static_cast<int>(std::chrono::weekday{sd}.iso_encoding());
      int days_back = (wd == 7) ? 0 : wd;
      std::chrono::sys_days week_start = sd - std::chrono::days{days_back};
      std::chrono::year_month_day res_ymd{week_start};
      ct.year = int(res_ymd.year());
      ct.month = static_cast<int>(static_cast<unsigned>(res_ymd.month()));
      ct.day = static_cast<int>(static_cast<unsigned>(res_ymd.day()));
      ct.hour = 0;
      ct.minute = 0;
      ct.second = 0;
      ct.subsecond_nanos = 0;
    } else if (unit == "day" || unit == "days") {
      ct.hour = 0;
      ct.minute = 0;
      ct.second = 0;
      ct.subsecond_nanos = 0;
    } else if (unit == "hour" || unit == "hours") {
      ct.minute = 0;
      ct.second = 0;
      ct.subsecond_nanos = 0;
    } else if (unit == "minute" || unit == "minutes") {
      ct.second = 0;
      ct.subsecond_nanos = 0;
    } else if (unit == "second" || unit == "seconds") {
      ct.subsecond_nanos = 0;
    } else if (unit == "millisecond" || unit == "milliseconds") {
      ct.subsecond_nanos = (ct.subsecond_nanos / 1000000LL) * 1000000LL;
    } else if (unit == "microsecond" || unit == "microseconds") {
      ct.subsecond_nanos = (ct.subsecond_nanos / 1000LL) * 1000LL;
    } else if (unit == "nanosecond" || unit == "nanoseconds") {
      // no change
    } else {
      return StatusError(StatusCode::kInvalidArgument,
                         "unsupported unit in " + name + ": " + unit);
    }
    if (d.type == ValueType::kDate && name_view == "date_trunc") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      return Value::DateFromDays(
          std::chrono::sys_days{ymd}.time_since_epoch().count());
    }
    return Value(FormatCivilTime(ct));
  }
  if (name_view == "format_date" || name_view == "format_datetime" ||
      name_view == "format_timestamp") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " takes 2 or 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    std::string fmt = raw_str(arguments[0]);
    ASSIGN_OR_RETURN(CivilTime, ct, (TryValueToCivilTime(arguments[1])));
    int tz_offset_sec = 0;
    if (arguments.size() == 3) {
      if (arguments[2].IsNull()) {
        return Value();
      }
      std::string tz_str = raw_str(arguments[2]);
      {
        // FORMAT_TIMESTAMP renders an instant: `ct` is the UTC reading, so
        // the zone's offset must be resolved AT that instant (at_utc), not
        // as though the UTC fields were local wall time — the two differ by
        // up to a day and can straddle a DST transition. DATETIME/DATE args
        // are civil fields and keep the local-time reading.
        ASSIGN_OR_RETURN(int, parsed_offset,
                         (ParseTimeZoneOffset(
                             tz_str, &ct, name_view == "format_timestamp")));
        tz_offset_sec = parsed_offset;
      }
    } else if (name_view == "format_datetime" ||
               name_view == "format_timestamp") {
      {
        ASSIGN_OR_RETURN(
            int, parsed_offset,
            (ParseTimeZoneOffset(GetDefaultTimeZone(), &ct,
                                 name_view == "format_timestamp")));
        tz_offset_sec = parsed_offset;
      }
    }
    // Apply the full offset (including sub-minute LMT seconds) in one go,
    // matching the AST FORMAT_* arm.
    ct = ShiftCivilTimeSeconds(ct, tz_offset_sec);
    struct tm tm = {};
    tm.tm_year = ct.year - 1900;
    tm.tm_mon = ct.month - 1;
    tm.tm_mday = ct.day;
    tm.tm_hour = ct.hour;
    tm.tm_min = ct.minute;
    tm.tm_sec = ct.second;
    platform::TimeGm(&tm);
    std::array<char, 128> buf{};
    // The civil fields are already shifted into the requested zone. Supply
    // its numeric offset explicitly because TimeGm calibrated the tm in UTC.
    ExpandNumericTimeZone(fmt, tz_offset_sec);
    const size_t formatted =
        platform::FormatTime(buf.data(), buf.size(), fmt.c_str(), &tm);
    // strftime's 0 means both "expansion failed" and "legitimately empty
    // output" (e.g. FORMAT_TIMESTAMP('', ts)); only the former is an error.
    if (formatted == 0 && !FormatTimeYieldsEmpty(fmt)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TIMESTAMP format produced no output: " + fmt);
    }
    return Value(std::string(buf.data()));
  }
  if (name_view == "parse_timestamp") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "PARSE_TIMESTAMP requires 2 or 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    if (arguments.size() == 3 && arguments[2].IsNull()) {
      return Value();
    }
    std::string fmt = raw_str(arguments[0]);
    // T3929: fold the glibc-only "%Ez" alias into "%z" before parsing so
    // the input-offset precedence does not depend on the platform's
    // strptime (the portable fallback parser does not know "%Ez").
    {
      std::string normalized;
      normalized.reserve(fmt.size());
      for (size_t i = 0; i < fmt.size(); ++i) {
        if (fmt[i] != '%') {
          normalized += fmt[i];
          continue;
        }
        if (i + 1 < fmt.size() && fmt[i + 1] == '%') {
          normalized += "%%";
          ++i;
          continue;
        }
        if (i + 2 < fmt.size() && fmt[i + 1] == 'E' && fmt[i + 2] == 'z') {
          normalized += "%z";
          i += 2;
          continue;
        }
        normalized += fmt[i];
      }
      fmt = std::move(normalized);
    }
    std::string input = raw_str(arguments[1]);
    struct tm tm = {};
    tm.tm_year = 70;  // BigQuery: unspecified fields start at 1970-01-01.
    tm.tm_mon = 0;
    tm.tm_mday = 1;
    const char* parsed_end =
        platform::ParseTime(input.c_str(), fmt.c_str(), &tm);
    // Every input byte must have a matching format element; trailing garbage
    // is an error, not a silently accepted parse (matches PARSE_DATE).
    if (parsed_end == nullptr || *parsed_end != '\0') {
      return StatusError(StatusCode::kInvalidArgument,
                         "PARSE_TIMESTAMP failed for: " + input);
    }
    CivilTime ct;
    ct.year = tm.tm_year + 1900;
    ct.month = tm.tm_mon + 1;
    ct.day = tm.tm_mday;
    ct.hour = tm.tm_hour;
    ct.minute = tm.tm_min;
    ct.second = tm.tm_sec;
    // Resolve the zone offset AT the parsed civil time: the nullptr overload
    // assumed 2000-01-01, applying standard time to a summer DST date.
    int tz_offset_sec = 0;
    // "%z" (and the "%Ez" alias, folded above by T3929) records the input's
    // offset in tm_gmtoff: prefer it over the default/third-argument zone
    // (AST parity, GoogleSQL).
    if (fmt.find("%z") != std::string::npos) {
      tz_offset_sec = static_cast<int>(platform::TmGmtoff(&tm));
    } else if (arguments.size() == 3) {
      ASSIGN_OR_RETURN(int, parsed_zone_offset,
                       (ParseTimeZoneOffset(raw_str(arguments[2]), &ct)));
      tz_offset_sec = parsed_zone_offset;
    } else {
      ASSIGN_OR_RETURN(int, parsed_zone_offset,
                       (ParseTimeZoneOffset(GetDefaultTimeZone(), &ct)));
      tz_offset_sec = parsed_zone_offset;
    }
    ct = ShiftCivilTimeHours(ct, -tz_offset_sec / 3600);
    int rem_mins = (tz_offset_sec % 3600) / 60;
    if (rem_mins != 0) {
      int total_m = ct.minute - rem_mins;
      if (total_m >= 60) {
        ct.minute = total_m - 60;
        ct = ShiftCivilTimeHours(ct, 1);
      } else if (total_m < 0) {
        ct.minute = total_m + 60;
        ct = ShiftCivilTimeHours(ct, -1);
      } else {
        ct.minute = total_m;
      }
    }
    return Value(FormatCivilTime(ct) + "+00");
  }
  if (name_view == "date_bucket" || name_view == "timestamp_bucket" ||
      name_view == "datetime_bucket") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " takes 2 or 3 arguments");
    }
    const Value& d = arguments[0];
    if (d.IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(CivilTime, d_ct, (TryValueToCivilTime(d)));
    const bool d_has_tz = !ExplicitZoneSuffix(raw_str(d)).empty();
    bool is_ts = (name_view == "timestamp_bucket" || d_has_tz);

    CivilTime orig_ct{.year = 1970,
                      .month = 1,
                      .day = 1,
                      .hour = 0,
                      .minute = 0,
                      .second = 0,
                      .subsecond_nanos = 0};
    bool has_origin = false;
    bool orig_has_tz = false;
    if (arguments.size() == 3) {
      const Value& orig = arguments[2];
      if (orig.IsNull()) {
        return Value();
      }
      ASSIGN_OR_RETURN(CivilTime, orig_ct_next, (TryValueToCivilTime(orig)));
      orig_ct = orig_ct_next;
      orig_has_tz = !ExplicitZoneSuffix(raw_str(orig)).empty();
      if (orig_has_tz) {
        is_ts = true;
      }
      has_origin = true;
    }
    if (call.Args()[1]->Type() != TypeTag::kIntervalExp) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires an INTERVAL width");
    }
    {
      const auto& interval = call.Args()[1]->AsIntervalExpression();
      int64_t amount = interval.Amount();
      std::string unit = to_lower(std::string(interval.Unit()));
      double sec_amount = 0.0;
      bool is_fractional_second = false;
      if (unit == "second" || unit == "seconds") {
        std::string raw_amount = interval.RawAmount();
        if (!raw_amount.empty() && raw_amount.find('.') != std::string::npos) {
          // Exception-free strict parse:
          // unlike the previous std::stod prefix parse, whitespace-padded
          // or trailing-garbage inputs are now rejected -- intended.
          ASSIGN_OR_RETURN(double, parsed_amount,
                           ParseDoubleStrict(raw_amount));
          sec_amount = parsed_amount;
          is_fractional_second = true;
        }
      }
      auto floor_div = [](int64_t a, int64_t b) -> int64_t {
        const int64_t q = a / b;
        return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
      };
      // A zero bucket width divides by zero (SIGFPE); GoogleSQL rejects it.
      if (amount == 0) {
        return StatusError(StatusCode::kInvalidArgument,
                           name + ": interval must not be zero");
      }
      if (unit == "month" || unit == "months" || unit == "quarter" ||
          unit == "quarters" || unit == "year" || unit == "years") {
        const int64_t month_scale = unit.starts_with("year")
                                        ? 12
                                        : (unit.starts_with("quarter") ? 3 : 1);
        int64_t step_m = 0;
        if (__builtin_mul_overflow(amount, month_scale, &step_m)) {
          return StatusError(StatusCode::kInvalidArgument,
                             name + ": interval width out of range");
        }
        const int64_t origin_month =
            (static_cast<int64_t>(orig_ct.year) * 12) + orig_ct.month - 1;
        const int64_t m_diff = (static_cast<int64_t>(d_ct.year) * 12) +
                               d_ct.month - 1 - origin_month;
        const auto total_m =
            origin_month +
            (static_cast<platform::Int128>(floor_div(m_diff, step_m)) * step_m);
        if (total_m < 12 || total_m >= static_cast<platform::Int128>(10000 * 12)) {
          return StatusError(StatusCode::kInvalidArgument,
                             name + ": bucket start out of range");
        }
        const auto month_index = static_cast<int>(total_m);
        const int bucket_y = month_index / 12;
        const int bucket_mon = (month_index % 12) + 1;
        CivilTime res_ct = orig_ct;
        res_ct.year = bucket_y;
        res_ct.month = bucket_mon;
        if (d.type == ValueType::kDate && name_view == "date_bucket") {
          std::chrono::year_month_day ymd{
              std::chrono::year{res_ct.year},
              std::chrono::month{static_cast<unsigned>(res_ct.month)},
              std::chrono::day{static_cast<unsigned>(res_ct.day)}};
          return Value::DateFromDays(
              std::chrono::sys_days{ymd}.time_since_epoch().count());
        }
        if (is_ts) {
          return Value(FormatCivilTime(res_ct) + "+00");
        }
        return Value(FormatCivilTime(res_ct));
      }

      int64_t step_ns = 0;
      if (is_fractional_second) {
        const double rounded = std::round(sec_amount * 1000000000.0);
        // The upper bound is exclusive: double(INT64_MAX) rounds to 2^63.
        if (!std::isfinite(rounded) || rounded < -0x1p63 || rounded >= 0x1p63) {
          return StatusError(StatusCode::kInvalidArgument,
                             name + ": interval width out of range");
        }
        step_ns = static_cast<int64_t>(rounded);
      } else if (unit == "nanosecond" || unit == "nanoseconds") {
        step_ns = amount;
      } else if (unit == "microsecond" || unit == "microseconds") {
        if (__builtin_mul_overflow(amount, 1000LL, &step_ns)) {
          return StatusError(StatusCode::kInvalidArgument,
                             name + ": interval width out of range");
        }
      } else if (unit == "millisecond" || unit == "milliseconds") {
        if (__builtin_mul_overflow(amount, 1000000LL, &step_ns)) {
          return StatusError(StatusCode::kInvalidArgument,
                             name + ": interval width out of range");
        }
      } else if (unit == "second" || unit == "seconds") {
        if (__builtin_mul_overflow(amount, 1000000000LL, &step_ns)) {
          return StatusError(StatusCode::kInvalidArgument,
                             name + ": interval width out of range");
        }
      } else if (unit == "minute" || unit == "minutes") {
        if (__builtin_mul_overflow(amount, 60000000000LL, &step_ns)) {
          return StatusError(StatusCode::kInvalidArgument,
                             name + ": interval width out of range");
        }
      } else if (unit == "hour" || unit == "hours") {
        if (__builtin_mul_overflow(amount, 3600000000000LL, &step_ns)) {
          return StatusError(StatusCode::kInvalidArgument,
                             name + ": interval width out of range");
        }
      } else if (unit == "day" || unit == "days") {
        if (__builtin_mul_overflow(amount, 86400000000000LL, &step_ns)) {
          return StatusError(StatusCode::kInvalidArgument,
                             name + ": interval width out of range");
        }
        if (!has_origin && amount % 7 == 0) {
          orig_ct.year = 1970;
          orig_ct.month = 1;
          orig_ct.day = 4;  // Sunday
        }
      } else if (unit == "week" || unit == "weeks") {
        if (__builtin_mul_overflow(amount, 604800000000000LL, &step_ns)) {
          return StatusError(StatusCode::kInvalidArgument,
                             name + ": interval width out of range");
        }
        if (!has_origin) {
          orig_ct.year = 1970;
          orig_ct.month = 1;
          orig_ct.day = 4;  // Sunday
        }
      } else {
        return StatusError(
            StatusCode::kInvalidArgument,
            "unsupported interval unit in " + name + ": " + unit);
      }

      if (step_ns == 0) {
        return StatusError(StatusCode::kInvalidArgument,
                           name + ": interval must not be zero");
      }
      platform::Int128 d_ns = CivilTimeToNanos(d_ct);
      if (is_ts && !d_has_tz) {
        ASSIGN_OR_RETURN(int, tz_sec,
                         (ParseTimeZoneOffset(GetDefaultTimeZone(), &d_ct)));
        d_ns -= static_cast<platform::Int128>(tz_sec) * 1000000000;
      }
      platform::Int128 orig_ns = CivilTimeToNanos(orig_ct);
      if (is_ts && (!has_origin || !orig_has_tz)) {
        ASSIGN_OR_RETURN(int, tz_sec,
                         (ParseTimeZoneOffset(GetDefaultTimeZone(), &orig_ct)));
        orig_ns -= static_cast<platform::Int128>(tz_sec) * 1000000000;
      }
      platform::Int128 diff_ns = d_ns - orig_ns;
      auto floor_div_128 = [](platform::Int128 a,
                              platform::Int128 b) -> platform::Int128 {
        const platform::Int128 q = a / b;
        return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
      };
      platform::Int128 bucket_ns =
          orig_ns + (floor_div_128(diff_ns, step_ns) * step_ns);
      CivilTime res_ct = NanosToCivilTime(bucket_ns);

      if (d.type == ValueType::kDate && name_view == "date_bucket") {
        std::chrono::year_month_day ymd{
            std::chrono::year{res_ct.year},
            std::chrono::month{static_cast<unsigned>(res_ct.month)},
            std::chrono::day{static_cast<unsigned>(res_ct.day)}};
        return Value::DateFromDays(
            std::chrono::sys_days{ymd}.time_since_epoch().count());
      }
      if (is_ts) {
        return Value(FormatCivilTime(res_ct) + "+00");
      }
      return Value(FormatCivilTime(res_ct));
    }
    return Value();
  }
  if (name_view == "parse_date") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "PARSE_DATE requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    std::string fmt = raw_str(arguments[0]);
    std::string val = raw_str(arguments[1]);
    for (char c : val) {
      if (c == '\r' || c == '\n' || c == '\0') {
        return StatusError(StatusCode::kInvalidArgument,
                           "invalid character in PARSE_DATE: " + val);
      }
    }
    if (fmt.find(' ') == std::string::npos &&
        val.find(' ') != std::string::npos) {
      return StatusError(StatusCode::kInvalidArgument,
                         "unexpected whitespace in PARSE_DATE: " + val);
    }
    if (fmt.find("%C") != std::string::npos ||
        fmt.find("%g") != std::string::npos) {
      return StatusError(StatusCode::kInvalidArgument,
                         "incomplete date format in PARSE_DATE");
    }
    if (fmt == "%W%y") {
      if (val.size() < 3 || val.size() > 4 ||
          !std::ranges::all_of(val,
                               [](char c) { return c >= '0' && c <= '9'; })) {
        return StatusError(
            StatusCode::kInvalidArgument,
            "Failed to parse input string " + Value(std::move(val)).AsString());
      }
      const int w = (10 * (val[0] - '0')) + (val[1] - '0');
      const int y =
          val.size() == 3 ? val[2] - '0' : (10 * (val[2] - '0')) + (val[3] - '0');
      if (w > 53) {
        return StatusError(StatusCode::kIsInfinity,
                           "week out of range in PARSE_DATE: " + val);
      }
      int full_year = y < 69 ? 2000 + y : 1900 + y;
      std::chrono::year_month_day jan1{std::chrono::year{full_year},
                                       std::chrono::month{1},
                                       std::chrono::day{1}};
      std::chrono::sys_days jan1_days{jan1};
      std::chrono::weekday wd{jan1_days};
      int jan1_wd =
          static_cast<int>(wd.iso_encoding());  // 1=Monday .. 7=Sunday
      int days_to_first_monday = (jan1_wd == 1) ? 0 : (8 - jan1_wd);
      std::chrono::sys_days target_days =
          jan1_days + std::chrono::days{days_to_first_monday + ((w - 1) * 7)};
      return Value::DateFromDays(target_days.time_since_epoch().count());
    }
    struct tm tm = {};
    tm.tm_year = 70;
    tm.tm_mday = 1;
    const char* res = platform::ParseTime(val.c_str(), fmt.c_str(), &tm);
    if (res == nullptr || *res != '\0') {
      return StatusError(StatusCode::kInvalidArgument,
                         "failed to parse date: " + val);
    }
    std::array<char, 32> buf{};
    (void)snprintf(buf.data(), buf.size(), "%04d-%02d-%02d", tm.tm_year + 1900,
                   tm.tm_mon + 1, tm.tm_mday);
    return Value::TryDate(std::string(buf.data()));
  }
  if (name_view == "last_day") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "LAST_DAY takes 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    ASSIGN_OR_RETURN(int64_t, days, (parse_date_val(arguments[0])));
    std::string part = "month";
    if (arguments.size() == 2) {
      part = to_lower(raw_str(arguments[1]));
    }
    using std::chrono::day;
    using std::chrono::month;
    using std::chrono::month_day_last;
    using std::chrono::sys_days;
    using std::chrono::year;
    using std::chrono::year_month_day;
    using std::chrono::year_month_day_last;

    year_month_day ymd{sys_days{std::chrono::days{days}}};
    int y = int(ymd.year());
    unsigned m = unsigned(ymd.month());

    if (part == "year" || part == "years") {
      year_month_day last_ymd{year{y}, month{12}, day{31}};
      return Value::DateFromDays(sys_days{last_ymd}.time_since_epoch().count());
    }
    if (part == "month" || part == "months") {
      year_month_day last_ymd{
          year_month_day_last{year{y}, month_day_last{month{m}}}};
      return Value::DateFromDays(sys_days{last_ymd}.time_since_epoch().count());
    }
    if (part == "quarter" || part == "quarters") {
      unsigned q_last_month = ((m - 1) / 3 + 1) * 3;
      year_month_day last_ymd{
          year_month_day_last{year{y}, month_day_last{month{q_last_month}}}};
      return Value::DateFromDays(sys_days{last_ymd}.time_since_epoch().count());
    }
    if (part == "week" || part == "weeks" || part.starts_with("week(")) {
      int last_day_iso = 6;
      if (part == "week(monday)") {
        last_day_iso = 7;
      } else if (part == "week(tuesday)") {
        last_day_iso = 1;
      } else if (part == "week(wednesday)") {
        last_day_iso = 2;
      } else if (part == "week(thursday)") {
        last_day_iso = 3;
      } else if (part == "week(friday)") {
        last_day_iso = 4;
      } else if (part == "week(saturday)") {
        last_day_iso = 5;
      } else if (part == "week(sunday)") {
        last_day_iso = 6;
      }
      std::chrono::sys_days cur_days{std::chrono::days{days}};
      int cur_iso =
          static_cast<int>(std::chrono::weekday{cur_days}.iso_encoding());
      int add_days = (last_day_iso >= cur_iso) ? (last_day_iso - cur_iso)
                                               : (7 + last_day_iso - cur_iso);
      return Value::DateFromDays(days + add_days);
    }
    if (part == "isoyear") {
      year_month_day dec31{year{y}, month{12}, day{31}};
      sys_days dec31_days{dec31};
      int iso_wd =
          static_cast<int>(std::chrono::weekday{dec31_days}.iso_encoding());
      int days_to_sun =
          (iso_wd == 7) ? 0 : ((iso_wd >= 4) ? (7 - iso_wd) : -(iso_wd));
      return Value::DateFromDays((dec31_days + std::chrono::days{days_to_sun})
                                     .time_since_epoch()
                                     .count());
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "unsupported LAST_DAY date_part " + part);
  }
  if (name_view == "week" && arguments.size() == 1) {
    if (arguments[0].IsNull()) {
      return Value();  // NULL propagates, like every other date function here
    }
    return Value("week(" + to_lower(raw_str(arguments[0])) + ")");
  }
  if (name_view == "add_months") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ADD_MONTHS requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(int64_t, days, (parse_date_val(arguments[0])));
    int64_t n = 0;
    if (arguments[1].type == ValueType::kInt64) {
      n = arguments[1].value.int_value;
    } else {
      ASSIGN_OR_RETURN(int64_t, parsed_n,
                       ParseI64Strict(raw_str(arguments[1])));
      n = parsed_n;
    }
    using std::chrono::day;
    using std::chrono::month;
    using std::chrono::month_day_last;
    using std::chrono::sys_days;
    using std::chrono::year;
    using std::chrono::year_month_day;
    using std::chrono::year_month_day_last;

    year_month_day ymd{sys_days{std::chrono::days{days}}};
    year_month_day_last last_of_orig_month{ymd.year(),
                                           month_day_last{ymd.month()}};
    bool is_last_day_of_month = (ymd.day() == last_of_orig_month.day());

    const auto floor_div = [](int64_t a, int64_t b) -> int64_t {
      const int64_t q = a / b;
      return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
    };
    int64_t total_m = (static_cast<int64_t>(int(ymd.year())) * 12) +
                      unsigned(ymd.month()) - 1 + n;
    // Check the year while it is still int64_t: narrowing first could wrap a
    // huge month offset into the accepted [1, 9999] range.
    const int64_t target_y_64 = floor_div(total_m, 12);
    if (target_y_64 < 1 || target_y_64 > 9999) {
      return StatusError(StatusCode::kIsInfinity, "DATE value out of range");
    }
    const int target_y = static_cast<int>(target_y_64);
    int target_m =
        static_cast<int>(total_m - (static_cast<int64_t>(target_y) * 12) + 1);

    year_month_day_last last_of_target{
        year{target_y}, month_day_last{month{static_cast<unsigned>(target_m)}}};
    unsigned target_day = unsigned(ymd.day());
    if (is_last_day_of_month || target_day > unsigned(last_of_target.day())) {
      target_day = unsigned(last_of_target.day());
    }
    year_month_day target_ymd{year{target_y},
                              month{static_cast<unsigned>(target_m)},
                              day{target_day}};
    int64_t res_days = sys_days{target_ymd}.time_since_epoch().count();
    if (res_days < -719162 || res_days > 2932896) {
      return StatusError(StatusCode::kIsInfinity, "DATE value out of range");
    }

    if (arguments[0].type == ValueType::kDate) {
      return Value::DateFromDays(res_days);
    }
    std::string s0 = raw_str(arguments[0]);
    if (s0.size() > 10) {
      return Value(FormatDateDays(res_days) + s0.substr(10));
    }
    return Value::DateFromDays(res_days);
  }
  if (name_view == "next_day") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "NEXT_DAY requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(int64_t, days, (parse_date_val(arguments[0])));
    std::string target_day_str = to_lower(raw_str(arguments[1]));
    int target_iso = 0;
    static const std::unordered_map<std::string, int> kWeekdays = {
        {"monday", 1}, {"tuesday", 2},  {"wednesday", 3}, {"thursday", 4},
        {"friday", 5}, {"saturday", 6}, {"sunday", 7},    {"mon", 1},
        {"tue", 2},    {"wed", 3},      {"thu", 4},       {"fri", 5},
        {"sat", 6},    {"sun", 7}};
    const auto wd = kWeekdays.find(target_day_str);
    if (wd != kWeekdays.end()) {
      target_iso = wd->second;
    } else {
      return StatusError(StatusCode::kInvalidArgument,
                         "invalid day name in NEXT_DAY: " + target_day_str);
    }

    std::chrono::sys_days cur_days{std::chrono::days{days}};
    int cur_iso =
        static_cast<int>(std::chrono::weekday{cur_days}.iso_encoding());
    int diff = target_iso - cur_iso;
    int add_days = (diff > 0) ? diff : (diff + 7);
    int64_t res_days = days + add_days;
    if (res_days < -719162 || res_days > 2932896) {
      return StatusError(StatusCode::kIsInfinity, "DATE value out of range");
    }

    return Value::DateFromDays(res_days);
  }
  if (name_view == "months_between") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "MONTHS_BETWEEN requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(int64_t, days1, (parse_date_val(arguments[0])));
    ASSIGN_OR_RETURN(int64_t, days2, (parse_date_val(arguments[1])));
    using std::chrono::month_day_last;
    using std::chrono::sys_days;
    using std::chrono::year_month_day;
    using std::chrono::year_month_day_last;

    year_month_day ymd1{sys_days{std::chrono::days{days1}}};
    year_month_day ymd2{sys_days{std::chrono::days{days2}}};
    year_month_day_last last1{ymd1.year(), month_day_last{ymd1.month()}};
    year_month_day_last last2{ymd2.year(), month_day_last{ymd2.month()}};

    int y1 = int(ymd1.year()), y2 = int(ymd2.year());
    // year_month_day built from sys_days always yields month in [1,12] and
    // day in [1,31], so these narrowing casts are value-preserving.
    int m1 = static_cast<int>(unsigned(ymd1.month()));
    int m2 = static_cast<int>(unsigned(ymd2.month()));
    int d1 = static_cast<int>(unsigned(ymd1.day()));
    int d2 = static_cast<int>(unsigned(ymd2.day()));

    auto full_months = static_cast<double>(((y1 - y2) * 12) + (m1 - m2));

    bool is_last1 = (std::cmp_equal(d1, unsigned(last1.day())));
    bool is_last2 = (std::cmp_equal(d2, unsigned(last2.day())));

    if ((d1 == d2) || (is_last1 && is_last2)) {
      return Value(full_months);
    }

    std::string s1 = raw_str(arguments[0]);
    std::string s2 = raw_str(arguments[1]);
    double time_frac1 = 0.0;
    double time_frac2 = 0.0;
    int h = 0, m = 0, s = 0;
    if (s1.size() > 11 &&
        sscanf(  // NOLINT(cert-err34-c) - conversion count is checked.
            s1.c_str() + 11, "%d:%d:%d", &h, &m, &s) >= 2) {
      time_frac1 = (h * 3600 + m * 60 + s) / 86400.0;
    }
    if (s2.size() > 11 &&
        sscanf(  // NOLINT(cert-err34-c) - conversion count is checked.
            s2.c_str() + 11, "%d:%d:%d", &h, &m, &s) >= 2) {
      time_frac2 = (h * 3600 + m * 60 + s) / 86400.0;
    }

    double day_diff = (d1 + time_frac1) - (d2 + time_frac2);
    return Value(full_months + (day_diff / 31.0));
  }

  auto utf8_offsets = [](std::string_view s) -> std::vector<size_t> {
    std::vector<size_t> offsets;
    offsets.reserve(s.size() + 1);
    for (size_t i = 0; i < s.size();) {
      const auto c = static_cast<unsigned char>(s[i]);
      if ((c & 0xC0) == 0x80) {
        // Orphan continuation byte: LENGTH/CHAR_LENGTH skip these, so they
        // must not count as a character here either.  Keep utf8_len and the
        // SUBSTR/LPAD/RIGHT offsets consistent with LENGTH.
        i += 1;
        continue;
      }
      offsets.push_back(i);
      if ((c & 0xE0) == 0xC0) {
        i += static_cast<size_t>((i + 1 < s.size()) ? 2 : 1);
      } else if ((c & 0xF0) == 0xE0) {
        i += static_cast<size_t>(
            (i + 2 < s.size()) ? 3 : (i + 1 < s.size() ? 2 : 1));
      } else if ((c & 0xF8) == 0xF0) {
        i += static_cast<size_t>(
            (i + 3 < s.size())
                ? 4
                : (i + 2 < s.size() ? 3 : (i + 1 < s.size() ? 2 : 1)));
      } else {
        // ASCII byte or invalid lead byte: advance by one.
        i += 1;
      }
    }
    offsets.push_back(s.size());
    return offsets;
  };

  auto utf8_len = [](std::string_view s) -> size_t {
    // P497: allocation-free character count -- the same walk utf8_offsets
    // performs (orphan continuation bytes skipped, lead lengths clamped at
    // the end so a truncated lead consumes the tail) minus the per-row
    // offset vector.  A plain "count non-continuation bytes" loop would
    // disagree on malformed input like "\xC3\x41": the lead swallows 0x41.
    size_t chars = 0;
    for (size_t i = 0; i < s.size();) {
      const auto c = static_cast<unsigned char>(s[i]);
      if ((c & 0xC0) == 0x80) {
        i += 1;
        continue;
      }
      ++chars;
      if ((c & 0xE0) == 0xC0) {
        i += static_cast<size_t>((i + 1 < s.size()) ? 2 : 1);
      } else if ((c & 0xF0) == 0xE0) {
        i += static_cast<size_t>(
            (i + 2 < s.size()) ? 3 : (i + 1 < s.size() ? 2 : 1));
      } else if ((c & 0xF8) == 0xF0) {
        i += static_cast<size_t>(
            (i + 3 < s.size())
                ? 4
                : (i + 2 < s.size() ? 3 : (i + 1 < s.size() ? 2 : 1)));
      } else {
        // ASCII byte or invalid lead byte: advance by one.
        i += 1;
      }
    }
    return chars;
  };

  auto utf8_substr = [&](std::string_view s, size_t start_cp,
                         size_t count_cp) -> std::string {
    const auto offsets = utf8_offsets(s);
    const size_t total_cps = offsets.size() - 1;
    if (start_cp >= total_cps) {
      return "";
    }
    const size_t end_cp = std::min(start_cp + count_cp, total_cps);
    const size_t byte_start = offsets[start_cp];
    const size_t byte_end = offsets[end_cp];
    return std::string(s.substr(byte_start, byte_end - byte_start));
  };

  // String functions
  if (name_view == "concat") {
    std::string result;
    result.reserve(arguments.size());
    for (const Value& value : arguments) {
      if (value.IsNull()) {
        return Value();
      }
      if (value.type != ValueType::kVarChar) {
        result.append(value.AsString());
      } else {
        result.append(value.value.varchar_value);
      }
    }
    return Value(std::move(result));
  }
  if (name_view == "byte_length" || name_view == "octet_length") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments[0].type == ValueType::kDate) {
      RETURN_IF_FAIL(TryFormatDateDays(arguments[0].DateDays()).GetStatus());
    }
    const StringArgView arg(arguments[0]);
    return Value(static_cast<int64_t>(arg.view.size()));
  }
  if (name_view == "length" || name_view == "char_length" ||
      name_view == "character_length") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    // GoogleSQL LENGTH/CHAR_LENGTH count characters (code points), while
    // BYTE_LENGTH/OCTET_LENGTH count bytes. Same rule as the AST reference:
    // every byte that is not a UTF-8 continuation byte (0x80-0xBF) is one
    // character, so malformed input skips orphans rather than counting each
    // as a character.
    if (arguments[0].type == ValueType::kDate) {
      RETURN_IF_FAIL(TryFormatDateDays(arguments[0].DateDays()).GetStatus());
    }
    const StringArgView arg(arguments[0]);
    size_t code_points = 0;
    for (const char ch : arg.view) {
      if ((static_cast<unsigned char>(ch) & 0xC0) != 0x80) {
        ++code_points;
      }
    }
    return Value(static_cast<int64_t>(code_points));
  }
  if (name_view == "upper") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "UPPER requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments[0].type != ValueType::kVarChar) {
      return StatusError(StatusCode::kInvalidArgument,
                         "UPPER requires a STRING argument");
    }
    return Value(UnicodeUpper(raw_str(arguments[0])));
  }
  if (name_view == "lower") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "LOWER requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments[0].type != ValueType::kVarChar) {
      return StatusError(StatusCode::kInvalidArgument,
                         "LOWER requires a STRING argument");
    }
    return Value(UnicodeLower(raw_str(arguments[0])));
  }
  if (name_view == "bit_count") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "BIT_COUNT requires 1 argument");
    }
    const Value& input = arguments[0];
    if (input.IsNull()) {
      return Value();
    }
    if (input.type != ValueType::kInt64) {
      return StatusError(StatusCode::kInvalidArgument,
                         "BIT_COUNT requires an INT64 argument");
    }
    // Storage-width popcount: the runtime value model keeps every integer
    // in an INT64 slot (INT32 declarations are width-validated at cast time
    // only), identical to GoogleSQL for non-negative inputs.
    return Value(static_cast<int64_t>(
        std::popcount(static_cast<uint64_t>(input.value.int_value))));
  }
  if (name_view == "normalize_and_casefold" || name_view == "normalize") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments[0].type != ValueType::kVarChar) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires a STRING argument");
    }
    // Mirrors the AST ground truth (function_call_expression.cpp): text
    // stays in composed form, so NFC/NFKC are accepted; decomposed input
    // (combining diacriticals) is rejected instead of silently
    // mis-normalized, and the decomposed normalization modes are rejected.
    if (arguments.size() == 2) {
      if (arguments[1].type != ValueType::kVarChar) {
        return StatusError(StatusCode::kInvalidArgument,
                           name + " normalization mode must be a STRING");
      }
      std::string mode;
      mode.reserve(raw_str(arguments[1]).size());
      for (char c : raw_str(arguments[1])) {
        mode.push_back(
            static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
      }
      if (mode != "NFC" && mode != "NFKC") {
        return StatusError(StatusCode::kNotImplemented,
                           name + " supports NFC/NFKC only, got " + mode);
      }
    }
    if (UnicodeContainsCombiningDiacritical(raw_str(arguments[0]))) {
      return StatusError(
          StatusCode::kNotImplemented,
          name + " cannot compose decomposed input (combining marks)");
    }
    if (name_view == "normalize") {
      return arguments[0];
    }
    return Value(UnicodeCaseFold(raw_str(arguments[0])));
  }
  if (name_view == "trim") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TRIM requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    const std::string cutset =
        arguments.size() == 2 ? raw_str(arguments[1]) : kDefaultTrimCutset;
    // Code-point matching (see TrimCodePoints): byte-set matching cuts
    // multi-byte characters apart.
    return Value(std::string(TrimCodePoints(s, cutset, /*leading=*/true,
                                            /*trailing=*/true)));
  }
  if (name_view == "ltrim") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "LTRIM requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    const std::string cutset =
        arguments.size() == 2 ? raw_str(arguments[1]) : kDefaultTrimCutset;
    // Code-point matching (see TrimCodePoints): byte-set matching cuts
    // multi-byte characters apart.
    return Value(std::string(TrimCodePoints(s, cutset, /*leading=*/true,
                                            /*trailing=*/false)));
  }
  if (name_view == "rtrim") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "RTRIM requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    const std::string cutset =
        arguments.size() == 2 ? raw_str(arguments[1]) : kDefaultTrimCutset;
    // Code-point matching (see TrimCodePoints): byte-set matching cuts
    // multi-byte characters apart.
    return Value(std::string(TrimCodePoints(s, cutset, /*leading=*/false,
                                            /*trailing=*/true)));
  }
  if (name_view == "starts_with") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "STARTS_WITH requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    for (const auto& arg : arguments) {
      if (arg.type != ValueType::kVarChar) {
        return StatusError(StatusCode::kInvalidArgument,
                           "STARTS_WITH requires STRING arguments");
      }
    }
    const StringArgView text(arguments[0]);
    const StringArgView prefix(arguments[1]);
    return Value(text.view.starts_with(prefix.view));
  }
  if (name_view == "ends_with") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ENDS_WITH requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    for (const auto& arg : arguments) {
      if (arg.type != ValueType::kVarChar) {
        return StatusError(StatusCode::kInvalidArgument,
                           "ENDS_WITH requires STRING arguments");
      }
    }
    const StringArgView text(arguments[0]);
    const StringArgView suffix(arguments[1]);
    return Value(text.view.ends_with(suffix.view));
  }
  if (name_view == "strpos") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "STRPOS requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    const StringArgView text(arguments[0]);
    const StringArgView needle(arguments[1]);
    if (needle.view.empty()) {
      return Value(int64_t{1});
    }
    const size_t pos = text.view.find(needle.view);
    if (pos == std::string_view::npos) {
      return Value(int64_t{0});
    }
    // Code points before the byte match, counted like the AST reference:
    // continuation bytes (0x80-0xBF) do not start a character.
    size_t code_points = 0;
    for (size_t i = 0; i < pos; ++i) {
      if ((static_cast<unsigned char>(text.view[i]) & 0xC0) != 0x80) {
        ++code_points;
      }
    }
    return Value(static_cast<int64_t>(code_points + 1));
  }
  if (name_view == "instr") {
    if (arguments.size() < 2 || arguments.size() > 4) {
      return StatusError(StatusCode::kInvalidArgument,
                         "INSTR requires 2 to 4 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() >= 3 && arguments[2].IsNull()) ||
        (arguments.size() == 4 && arguments[3].IsNull())) {
      return Value();
    }
    const std::string source = raw_str(arguments[0]);
    const std::string target = raw_str(arguments[1]);
    auto to_i64 = [](const Value& v) -> StatusOr<int64_t> {
      if (v.type != ValueType::kInt64) {
        return StatusError(
            StatusCode::kInvalidArgument,
            "INSTR position and occurrence require INT64 arguments");
      }
      return ToInt64Exact(v, 1);
    };
    int64_t position = 1;
    if (arguments.size() >= 3) {
      ASSIGN_OR_RETURN(int64_t, parsed_position, to_i64(arguments[2]));
      position = parsed_position;
    }
    int64_t occurrence = 1;
    if (arguments.size() == 4) {
      ASSIGN_OR_RETURN(int64_t, parsed_occurrence, to_i64(arguments[3]));
      occurrence = parsed_occurrence;
    }

    if (position == 0 || occurrence <= 0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "position and occurrence must be positive");
    }

    // P497: a single monotonic walk with a (target_cps + 1)-wide sliding
    // window of code-point boundary offsets replaces the per-row
    // utf8_offsets(source) vector; boundary derivation follows exactly the
    // same rules (orphan continuation bytes skipped, lead lengths clamped
    // at the end).
    const size_t t_cps = utf8_len(target);
    if (t_cps == 0) {
      return Value(
          InstrEmptyTargetPosition(utf8_len(source), position, occurrence));
    }
    std::array<size_t, 33> small_bounds{};
    std::vector<size_t> heap_bounds;
    const size_t window = t_cps + 1;
    size_t* bounds = nullptr;
    if (window <= small_bounds.size()) {
      bounds = small_bounds.data();
    } else {
      heap_bounds.resize(window);
      bounds = heap_bounds.data();
    }
    std::vector<int64_t> matches;  // every match cp+1, ascending
    matches.reserve(source.size());
    size_t b_count = 0;
    size_t i = 0;
    for (;;) {
      const bool at_end = i == source.size();
      if (!at_end && (static_cast<unsigned char>(source[i]) & 0xC0) == 0x80) {
        i += 1;
        continue;
      }
      if (b_count >= t_cps) {
        const size_t b_start = bounds[(b_count - t_cps) % window];
        if (source.compare(b_start, i - b_start, target) == 0) {
          matches.push_back(static_cast<int64_t>(b_count - t_cps) + 1);
        }
      }
      bounds[b_count % window] = i;
      if (at_end) {
        break;
      }
      ++b_count;
      const auto c = static_cast<unsigned char>(source[i]);
      if ((c & 0xE0) == 0xC0) {
        i += static_cast<size_t>((i + 1 < source.size()) ? 2 : 1);
      } else if ((c & 0xF0) == 0xE0) {
        i += static_cast<size_t>(
            (i + 2 < source.size()) ? 3 : (i + 1 < source.size() ? 2 : 1));
      } else if ((c & 0xF8) == 0xF0) {
        i += static_cast<size_t>((i + 3 < source.size())
                                     ? 4
                                     : (i + 2 < source.size()
                                            ? 3
                                            : (i + 1 < source.size() ? 2 : 1)));
      } else {
        i += 1;
      }
    }
    const size_t s_cps = b_count;

    std::vector<int64_t> ordered;
    ordered.reserve(matches.size());
    if (position > 0) {
      const int64_t start_cp = position - 1;
      for (const int64_t cp1 : matches) {
        if (cp1 > start_cp) {
          ordered.push_back(cp1);
        }
      }
    } else {
      const int64_t target_cp = static_cast<int64_t>(s_cps) + position;
      if (target_cp >= 0) {
        for (const auto match : std::views::reverse(matches)) {
          if (match - 1 <= target_cp) {
            ordered.push_back(match);
          }
        }
      }
    }
    if (static_cast<size_t>(occurrence) <= ordered.size()) {
      return Value(ordered[static_cast<size_t>(occurrence) - 1]);
    }
    return Value(int64_t{0});
  }
  if (name_view == "replace") {
    if (arguments.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "REPLACE requires 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        arguments[2].IsNull()) {
      return Value();
    }
    for (const auto& arg : arguments) {
      if (arg.type != ValueType::kVarChar) {
        return StatusError(StatusCode::kInvalidArgument,
                           "REPLACE requires STRING arguments");
      }
    }
    std::string s = raw_str(arguments[0]);
    const std::string from = raw_str(arguments[1]);
    const std::string to = raw_str(arguments[2]);
    if (from.empty()) {
      return Value(std::move(s));
    }
    size_t pos = 0;
    while ((pos = s.find(from, pos)) != std::string::npos) {
      s.replace(pos, from.size(), to);
      pos += to.size();
    }
    return Value(std::move(s));
  }
  if (name_view == "repeat") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "REPEAT requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      return ToInt64WithText(v, def);
    };
    const int64_t n = to_i64(arguments[1], 0);
    if (n < 0) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "Second argument (repeat count) for REPEAT cannot be negative");
    }
    if (n == 0 || s.empty()) {
      // REPEAT('', k) is '' for every k; otherwise the loop below would run k
      // times appending nothing (fuzz-found 30s+ timeout for '' x 2^63).
      return Value(std::string());
    }
    // A plain uint64 multiply wraps (e.g. 8 bytes x 2^61 == 0) and would slip
    // past the 1MB cap into an unbounded append loop.
    uint64_t total_bytes = 0;
    if (__builtin_mul_overflow(static_cast<uint64_t>(s.size()),
                               static_cast<uint64_t>(n), &total_bytes) ||
        total_bytes > 1000000) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "Output of REPEAT exceeds max allowed output size of 1MB");
    }
    std::string res;
    res.reserve(static_cast<size_t>(total_bytes));
    for (int64_t i = 0; i < n; ++i) {
      res += s;
    }
    return Value(std::move(res));
  }

  if (name_view == "reverse") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "REVERSE requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    const auto offsets = utf8_offsets(s);
    const size_t total_cps = offsets.size() - 1;
    std::string res;
    res.reserve(s.size());
    for (size_t i = total_cps; i > 0; --i) {
      const size_t start = offsets[i - 1];
      const size_t len = offsets[i] - start;
      res.append(s, start, len);
    }
    return Value(std::move(res));
  }
  if (name_view == "substr" || name_view == "substring") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SUBSTR requires two or three arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() == 3 && arguments[2].IsNull())) {
      return Value();
    }
    // Same strict signature as the AST reference: SUBSTR(STRING, INT64[,
    // INT64]); a FLOAT64 / STRING offset is a type error, not a coercion.
    if (arguments[0].type != ValueType::kVarChar ||
        arguments[1].type != ValueType::kInt64 ||
        (arguments.size() == 3 && arguments[2].type != ValueType::kInt64)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SUBSTR argument type mismatch");
    }
    const std::string input = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      return ToInt64Exact(v, def);
    };
    // The AST keeps the signed bit pattern for INT64 positions, including
    // UINT64 values used as end-relative offsets; keep the batch path aligned.
    const int64_t start = arguments[1].value.int_value;
    if (arguments.size() == 3) {
      const int64_t len = to_i64(arguments[2], 0);
      if (len < 0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "SUBSTR length cannot be negative");
      }
      if (len == 0) {
        return Value(std::string());
      }
    }

    const size_t total_cps = utf8_len(input);
    int64_t actual_start = 0;
    if (start > 0) {
      actual_start = start - 1;
    } else if (start < 0) {
      actual_start = static_cast<int64_t>(total_cps) + start;
    } else {
      actual_start = 0;
    }
    actual_start = std::max<int64_t>(actual_start, 0);
    if (std::cmp_greater_equal(actual_start, total_cps)) {
      return Value(std::string());
    }

    const size_t length =
        arguments.size() == 3
            ? static_cast<size_t>(std::max(int64_t{0}, to_i64(arguments[2], 0)))
            : total_cps;
    return Value(utf8_substr(input, static_cast<size_t>(actual_start), length));
  }

  if (name_view == "byte_substr") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SUBSTR requires two or three arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() == 3 && arguments[2].IsNull())) {
      return Value();
    }
    if (arguments[0].type != ValueType::kVarChar ||
        arguments[1].type != ValueType::kInt64 ||
        (arguments.size() == 3 && arguments[2].type != ValueType::kInt64)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "BYTE_SUBSTR argument type mismatch");
    }
    const std::string input = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      return ToInt64Exact(v, def);
    };
    // Match SUBSTR's AST semantics for the shared INT64 storage of UINT64.
    const int64_t start = arguments[1].value.int_value;
    if (arguments.size() == 3) {
      const int64_t len = to_i64(arguments[2], 0);
      if (len < 0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "SUBSTR length cannot be negative");
      }
      if (len == 0) {
        return Value(std::string());
      }
    }

    const size_t total_bytes = input.size();
    int64_t actual_start = 0;
    if (start > 0) {
      actual_start = start - 1;
    } else if (start < 0) {
      actual_start = static_cast<int64_t>(total_bytes) + start;
    } else {
      actual_start = 0;
    }
    actual_start = std::max<int64_t>(actual_start, 0);
    if (std::cmp_greater_equal(actual_start, total_bytes)) {
      return Value(std::string());
    }

    const size_t length =
        arguments.size() == 3
            ? static_cast<size_t>(std::max(int64_t{0}, to_i64(arguments[2], 0)))
            : total_bytes;
    return Value(input.substr(static_cast<size_t>(actual_start), length));
  }

  if (name_view == "byte_reverse") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "REVERSE requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    std::string s = raw_str(arguments[0]);
    std::reverse(s.begin(), s.end());
    return Value(std::move(s));
  }

  if (name_view == "left" || name_view == "byte_left") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "LEFT requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      return ToInt64WithText(v, def);
    };
    const int64_t len = to_i64(arguments[1], 0);
    if (len < 0) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "Second argument (length) for LEFT cannot be negative");
    }
    if (len == 0) {
      return Value(std::string());
    }
    if (name_view == "byte_left") {
      return Value(s.substr(0, static_cast<size_t>(len)));
    }
    return Value(utf8_substr(s, 0, static_cast<size_t>(len)));
  }

  if (name_view == "right" || name_view == "byte_right") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "RIGHT requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      return ToInt64WithText(v, def);
    };
    const int64_t len = to_i64(arguments[1], 0);
    if (len < 0) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "Second argument (length) for RIGHT cannot be negative");
    }
    if (len == 0) {
      return Value(std::string());
    }
    if (name_view == "byte_right") {
      const size_t start = s.size() >= static_cast<size_t>(len)
                               ? s.size() - static_cast<size_t>(len)
                               : 0;
      return Value(s.substr(start));
    }
    const size_t total_cps = utf8_len(s);
    const size_t start_cp =
        std::cmp_greater_equal(total_cps, static_cast<size_t>(len))
            ? total_cps - static_cast<size_t>(len)
            : 0;
    return Value(utf8_substr(s, start_cp, static_cast<size_t>(len)));
  }

  if (name_view == "lpad" || name_view == "rpad") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 2 or 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() == 3 && arguments[2].IsNull())) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      return ToInt64WithText(v, def);
    };
    const int64_t target_len = to_i64(arguments[1], 0);
    if (target_len < 0) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "Second argument (output size) for LPAD/RPAD cannot be negative");
    }
    if (target_len > 1000000) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "Output of LPAD/RPAD exceeds max allowed output size of 1MB");
    }
    if (target_len == 0) {
      return Value(std::string());
    }
    const std::string pad = arguments.size() == 3 ? raw_str(arguments[2]) : " ";
    if (pad.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "Pattern in LPAD/RPAD cannot be empty");
    }
    const size_t total_cps = utf8_len(s);
    if (std::cmp_less_equal(target_len, total_cps)) {
      return Value(utf8_substr(s, 0, static_cast<size_t>(target_len)));
    }
    const size_t pad_cps = utf8_len(pad);
    if (pad_cps == 0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "Pattern in LPAD/RPAD contains no characters");
    }
    const size_t needed = static_cast<size_t>(target_len) - total_cps;
    std::string pad_str;
    pad_str.reserve((needed / pad_cps + 1) * pad.size());
    for (size_t i = 0; i < needed / pad_cps; ++i) {
      pad_str += pad;
    }
    if (needed % pad_cps != 0) {
      pad_str += utf8_substr(pad, 0, needed % pad_cps);
    }
    // The 1MB cap is in bytes (REPEAT parity): pad code points can expand
    // to ~4x that in UTF-8, so check the real output size.
    if (s.size() + pad_str.size() > 1000000) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "Output of LPAD/RPAD exceeds max allowed output size of 1MB");
    }
    return Value(name_view == "lpad" ? pad_str + s : s + pad_str);
  }

  if (name_view == "ascii") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ASCII requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments[0].type != ValueType::kVarChar) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ASCII requires a STRING argument");
    }
    const std::string s = raw_str(arguments[0]);
    if (s.empty()) {
      return Value(int64_t{0});
    }
    // ASCII is the first Unicode code point (same as UNICODE), not the first
    // UTF-8 byte. Mirror the UNICODE arm's decoding below.
    const auto offsets = utf8_offsets(s);
    if (offsets.size() < 2) {
      return Value(int64_t{0});
    }
    const size_t char_start = offsets[0];
    const size_t first_len = offsets[1] - char_start;
    uint32_t code = 0;
    if (first_len == 1) {
      code = static_cast<unsigned char>(s[char_start]);
    } else if (first_len == 2) {
      code = static_cast<uint32_t>(
          ((static_cast<unsigned char>(s[char_start]) & 0x1F) << 6) |
          (static_cast<unsigned char>(s[char_start + 1]) & 0x3F));
    } else if (first_len == 3) {
      code = static_cast<uint32_t>(
          ((static_cast<unsigned char>(s[char_start]) & 0x0F) << 12) |
          ((static_cast<unsigned char>(s[char_start + 1]) & 0x3F) << 6) |
          (static_cast<unsigned char>(s[char_start + 2]) & 0x3F));
    } else if (first_len == 4) {
      code = static_cast<uint32_t>(
          ((static_cast<unsigned char>(s[char_start]) & 0x07) << 18) |
          ((static_cast<unsigned char>(s[char_start + 1]) & 0x3F) << 12) |
          ((static_cast<unsigned char>(s[char_start + 2]) & 0x3F) << 6) |
          (static_cast<unsigned char>(s[char_start + 3]) & 0x3F));
    }
    return Value(static_cast<int64_t>(code));
  }

  if (name_view == "unicode") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "UNICODE requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments[0].type != ValueType::kVarChar) {
      return StatusError(StatusCode::kInvalidArgument,
                         "UNICODE requires a STRING argument");
    }
    const std::string s = raw_str(arguments[0]);
    if (s.empty()) {
      return Value(int64_t{0});
    }
    const auto offsets = utf8_offsets(s);
    // utf8_offsets skips orphan continuation bytes, so the first character
    // can start past byte 0 and an all-orphan string has no characters.
    if (offsets.size() < 2) {
      return Value(int64_t{0});
    }
    const size_t char_start = offsets[0];
    const size_t first_len = offsets[1] - char_start;
    uint32_t code = 0;
    if (first_len == 1) {
      code = static_cast<unsigned char>(s[char_start]);
    } else if (first_len == 2) {
      code = static_cast<uint32_t>(
          ((static_cast<unsigned char>(s[char_start]) & 0x1F) << 6) |
          (static_cast<unsigned char>(s[char_start + 1]) & 0x3F));
    } else if (first_len == 3) {
      code = static_cast<uint32_t>(
          ((static_cast<unsigned char>(s[char_start]) & 0x0F) << 12) |
          ((static_cast<unsigned char>(s[char_start + 1]) & 0x3F) << 6) |
          (static_cast<unsigned char>(s[char_start + 2]) & 0x3F));
    } else if (first_len == 4) {
      code = static_cast<uint32_t>(
          ((static_cast<unsigned char>(s[char_start]) & 0x07) << 18) |
          ((static_cast<unsigned char>(s[char_start + 1]) & 0x3F) << 12) |
          ((static_cast<unsigned char>(s[char_start + 2]) & 0x3F) << 6) |
          (static_cast<unsigned char>(s[char_start + 3]) & 0x3F));
    }
    return Value(static_cast<int64_t>(code));
  }

  if (name_view == "chr") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CHR requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments[0].type != ValueType::kInt64) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CHR requires an integer argument");
    }
    int64_t code = arguments[0].value.int_value;
    if (arguments[0].IsUnsigned()) {
      // UINT64 keeps the kInt64 tag; the bit pattern is the true value and
      // >= 2^63 would otherwise read back as a negative int64.
      const auto u = static_cast<uint64_t>(code);
      if (u > 0x10FFFF || (u >= 0xD800 && u <= 0xDFFF)) {
        return StatusError(StatusCode::kIsInfinity,
                           "CHR argument out of range");
      }
      code = static_cast<int64_t>(u);
    }
    std::string res;
    if (code < 0 || code > 0x10FFFF || (code >= 0xD800 && code <= 0xDFFF)) {
      return StatusError(StatusCode::kIsInfinity, "CHR argument out of range");
    }
    if (code <= 0x7F) {
      res.push_back(static_cast<char>(code));
    } else if (code <= 0x7FF) {
      res.push_back(static_cast<char>(0xC0 | ((code >> 6) & 0x1F)));
      res.push_back(static_cast<char>(0x80 | (code & 0x3F)));
    } else if (code <= 0xFFFF) {
      res.push_back(static_cast<char>(0xE0 | ((code >> 12) & 0x0F)));
      res.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3F)));
      res.push_back(static_cast<char>(0x80 | (code & 0x3F)));
    } else {
      res.push_back(static_cast<char>(0xF0 | ((code >> 18) & 0x07)));
      res.push_back(static_cast<char>(0x80 | ((code >> 12) & 0x3F)));
      res.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3F)));
      res.push_back(static_cast<char>(0x80 | (code & 0x3F)));
    }
    return Value(std::move(res));
  }

  // T2234: inverse of CODE_POINTS_TO_STRING/BYTES (GoogleSQL standard).
  if (name_view == "to_code_points") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TO_CODE_POINTS requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments[0].type != ValueType::kVarChar) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TO_CODE_POINTS requires a STRING or BYTES argument");
    }
    std::vector<Value> elements;
    elements.reserve(arguments[0].value.varchar_value.size());
    for (const uint32_t cp :
         CodePointsOfBatch(arguments[0].value.varchar_value)) {
      elements.emplace_back(static_cast<int64_t>(cp));  // NOLINT(modernize-use-emplace)
    }
    return Value::Array(std::move(elements), "INT64");
  }
  // T2238: EDIT_DISTANCE(value1, value2[, max_distance]); distance above
  // max_distance reports max_distance + 1 per the GoogleSQL spec.
  if (name_view == "edit_distance") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "EDIT_DISTANCE requires 2 or 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() == 3 && arguments[2].IsNull())) {
      return Value();
    }
    if (arguments[0].type != ValueType::kVarChar ||
        arguments[1].type != ValueType::kVarChar) {
      return StatusError(StatusCode::kInvalidArgument,
                         "EDIT_DISTANCE requires STRING arguments");
    }
    int64_t max_distance = -1;
    if (arguments.size() == 3) {
      if (arguments[2].type != ValueType::kInt64) {
        return StatusError(StatusCode::kInvalidArgument,
                           "EDIT_DISTANCE max_distance must be an integer");
      }
      max_distance = arguments[2].value.int_value;
      if (max_distance < 0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "EDIT_DISTANCE max_distance cannot be negative");
      }
    }
    const int64_t distance = LevenshteinCodePointsBatch(
        arguments[0].value.varchar_value, arguments[1].value.varchar_value);
    if (max_distance >= 0 && distance > max_distance) {
      return Value(max_distance + 1);
    }
    return Value(distance);
  }
  if (name_view == "code_points_to_string") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CODE_POINTS_TO_STRING requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const Value& arr = arguments[0];
    if (arr.type != ValueType::kArray) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CODE_POINTS_TO_STRING requires an ARRAY<INT64>");
    }
    std::string res;
    res.reserve(arr.ArrayElements().size());
    for (const Value& elem : arr.ArrayElements()) {
      if (elem.IsNull()) {
        return Value();
      }
      if (elem.type != ValueType::kInt64) {
        return StatusError(StatusCode::kInvalidArgument,
                           "CODE_POINTS_TO_STRING requires INT64 elements");
      }
      int64_t cp = elem.value.int_value;
      if (elem.IsUnsigned()) {
        // UINT64 elements keep the kInt64 tag; diagnose by the true value
        // instead of letting >= 2^63 surface as a negative code point.
        const auto u = static_cast<uint64_t>(cp);
        if (u > 0x10FFFF || (u >= 0xD800 && u <= 0xDFFF)) {
          return StatusError(StatusCode::kInvalidArgument,
                             "invalid code point for CODE_POINTS_TO_STRING: " +
                                 std::to_string(u));
        }
        cp = static_cast<int64_t>(u);
      }
      if (cp < 0 || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) {
        return StatusError(StatusCode::kInvalidArgument,
                           "invalid code point for CODE_POINTS_TO_STRING: " +
                               std::to_string(cp));
      }
      if (cp <= 0x7F) {
        res.push_back(static_cast<char>(cp));
      } else if (cp <= 0x7FF) {
        res.push_back(static_cast<char>(0xC0 | ((cp >> 6) & 0x1F)));
        res.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
      } else if (cp <= 0xFFFF) {
        res.push_back(static_cast<char>(0xE0 | ((cp >> 12) & 0x0F)));
        res.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        res.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
      } else {
        res.push_back(static_cast<char>(0xF0 | ((cp >> 18) & 0x07)));
        res.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
        res.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        res.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
      }
    }
    return Value(std::move(res));
  }

  if (name_view == "code_points_to_bytes") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CODE_POINTS_TO_BYTES requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const Value& arr = arguments[0];
    if (arr.type != ValueType::kArray) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CODE_POINTS_TO_BYTES requires an ARRAY<INT64>");
    }
    std::string res;
    res.reserve(arr.ArrayElements().size());
    for (const Value& elem : arr.ArrayElements()) {
      if (elem.IsNull()) {
        return Value();
      }
      if (elem.type != ValueType::kInt64) {
        return StatusError(StatusCode::kInvalidArgument,
                           "CODE_POINTS_TO_BYTES requires INT64 elements");
      }
      int64_t b = elem.value.int_value;
      if (elem.IsUnsigned()) {
        // UINT64 elements keep the kInt64 tag; diagnose by the true value
        // instead of letting >= 2^63 surface as a negative byte.
        const auto u = static_cast<uint64_t>(b);
        if (u > 255) {
          return StatusError(
              StatusCode::kInvalidArgument,
              "invalid byte for CODE_POINTS_TO_BYTES: " + std::to_string(u));
        }
        b = static_cast<int64_t>(u);
      }
      if (b < 0 || b > 255) {
        return StatusError(
            StatusCode::kInvalidArgument,
            "invalid byte for CODE_POINTS_TO_BYTES: " + std::to_string(b));
      }
      res.push_back(static_cast<char>(b));
    }
    return Value(std::move(res));
  }

  if (name_view == "initcap") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "INITCAP requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    const bool has_delim = arguments.size() == 2;
    const std::string s = raw_str(arguments[0]);
    const std::string delims = has_delim ? raw_str(arguments[1]) : "";
    return Value(UnicodeInitcap(s, delims, has_delim));
  }

  if (name_view == "regexp_contains") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "REGEXP_CONTAINS requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    std::string s_owned;
    std::string pat_owned;
    const std::string_view s_view =
        arguments[0].type == ValueType::kVarChar
            ? arguments[0].value.varchar_value
            : std::string_view(s_owned = raw_str(arguments[0]));
    const std::string_view pat_view =
        arguments[1].type == ValueType::kVarChar
            ? arguments[1].value.varchar_value
            : std::string_view(pat_owned = raw_str(arguments[1]));
    ASSIGN_OR_RETURN(bool, found, CheckedRegexContains(s_view, pat_view));
    return Value(found);
  }

  if (name_view == "regexp_match") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "REGEXP_MATCH requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    const std::string pat = raw_str(arguments[1]);
    try {
      ASSIGN_OR_RETURN(std::regex, re, CompileCheckedRegex(pat));
      return Value(std::regex_match(s, re));
    } catch (...) {
      return StatusError(StatusCode::kInvalidArgument,
                         "invalid regular expression: " + pat);
    }
  }

  if (name_view == "regexp_instr" || name_view == "byte_regexp_instr") {
    if (arguments.size() < 2 || arguments.size() > 5) {
      return StatusError(StatusCode::kInvalidArgument,
                         "REGEXP_INSTR requires 2 to 5 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() >= 3 && arguments[2].IsNull()) ||
        (arguments.size() >= 4 && arguments[3].IsNull()) ||
        (arguments.size() == 5 && arguments[4].IsNull())) {
      return Value();
    }
    const int64_t position =
        arguments.size() >= 3 ? ToInt64WithText(arguments[2], 1) : 1;
    const int64_t occurrence =
        arguments.size() >= 4 ? ToInt64WithText(arguments[3], 1) : 1;
    const int64_t occurrence_position =
        arguments.size() == 5 ? ToInt64WithText(arguments[4], 0) : 0;
    ASSIGN_OR_RETURN(
        int64_t, match,
        CheckedRegexPosition(raw_str(arguments[0]), raw_str(arguments[1]),
                             position, occurrence, occurrence_position,
                             name_view == "byte_regexp_instr"));
    return Value(match);
  }

  if (name_view == "regexp_extract_all" ||
      name_view == "byte_regexp_extract_all") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "REGEXP_EXTRACT_ALL requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    const std::string pat = raw_str(arguments[1]);
    try {
      ASSIGN_OR_RETURN(std::regex, re, CompileCheckedRegex(pat));
      std::vector<Value> results;
      std::sregex_iterator it(s.begin(), s.end(), re);
      std::sregex_iterator end;
      while (it != end) {
        // RE2 does not return the terminal empty match for BYTES/STRING
        // REGEXP_EXTRACT_ALL; std::regex_iterator does, so discard it.
        // match_results::empty() reports "no submatches", not "empty match";
        // test the matched length instead.
        if (it->length(0) == 0 &&
            it->position() == static_cast<ptrdiff_t>(s.size())) {
          break;
        }
        if (it->size() > 1) {
          results.emplace_back(it->str(1));
        } else {
          results.emplace_back(it->str(0));
        }
        ++it;
      }
      return Value::Array(
          std::move(results),
          name_view == "byte_regexp_extract_all" ? "BYTES" : "STRING");
    } catch (...) {
      return StatusError(StatusCode::kInvalidArgument,
                         "invalid regular expression: " + pat);
    }
  }

  if (name_view == "regexp_extract" || name_view == "regexp_substr") {
    // T2235: REGEXP_SUBSTR is the GoogleSQL synonym of REGEXP_EXTRACT.
    if (arguments.size() < 2 || arguments.size() > 4) {
      return StatusError(StatusCode::kInvalidArgument,
                         "REGEXP_EXTRACT requires 2 to 4 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() >= 3 && arguments[2].IsNull()) ||
        (arguments.size() == 4 && arguments[3].IsNull())) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    const std::string pat = raw_str(arguments[1]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      return ToInt64WithText(v, def);
    };
    const int64_t pos_arg = arguments.size() >= 3 ? to_i64(arguments[2], 1) : 1;
    const int64_t occ_arg = arguments.size() == 4 ? to_i64(arguments[3], 1) : 1;
    if (pos_arg <= 0 || occ_arg <= 0) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "REGEXP_EXTRACT position and occurrence must be positive");
    }
    const auto offsets = utf8_offsets(s);
    const size_t total_cps = offsets.size() - 1;
    if (static_cast<uint64_t>(pos_arg) > total_cps + 1) {
      return Value();
    }
    const size_t byte_start = offsets[static_cast<size_t>(pos_arg - 1)];
    try {
      ASSIGN_OR_RETURN(std::regex, re, CompileCheckedRegex(pat));
      const char* const search_begin = s.data() + byte_start;
      const char* const search_end = s.data() + s.size();
      std::cregex_iterator it(search_begin, search_end, re);
      std::cregex_iterator end;
      for (int64_t current_occ = 1; it != end; ++it, ++current_occ) {
        if (current_occ == occ_arg) {
          if (it->size() > 1) {
            // GoogleSQL: a capture group that did not participate in the
            // match is NULL, not an empty string.
            if (!(*it)[1].matched) {
              return Value();
            }
            return Value(it->str(1));
          }
          return Value(it->str(0));
        }
      }
      return Value();
    } catch (...) {
      return StatusError(StatusCode::kInvalidArgument,
                         "invalid regular expression: " + pat);
    }
  }

  if (name_view == "regexp_replace") {
    if (arguments.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "REGEXP_REPLACE requires 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        arguments[2].IsNull()) {
      return Value();
    }
    std::array<std::string, 3> owned;
    std::array<std::string_view, 3> views;
    for (size_t i = 0; i < views.size(); ++i) {
      if (arguments[i].type == ValueType::kVarChar) {
        views[i] = arguments[i].AsStringView();
      } else {
        owned[i] = raw_str(arguments[i]);
        views[i] = owned[i];
      }
    }
    try {
      ASSIGN_OR_RETURN(std::string, replaced,
                       ReplaceCheckedRegex(views[0], views[1], views[2]));
      return Value(std::move(replaced));
    } catch (...) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "invalid regular expression: " + std::string(views[1]));
    }
  }

  if (name_view == "split_substr") {
    if (arguments.size() < 2 || arguments.size() > 4) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SPLIT_SUBSTR requires 2 to 4 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() >= 3 && arguments[2].IsNull()) ||
        (arguments.size() == 4 && arguments[3].IsNull())) {
      return Value();
    }
    const std::string str = raw_str(arguments[0]);
    const std::string delim = raw_str(arguments[1]);
    // T1653: was a stoll-based copy of the lenient converter; unified onto
    // ToInt64WithText (same whole-token requirement, no exception cost).
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      return ToInt64WithText(v, def);
    };
    const int64_t occ = arguments.size() >= 3 ? to_i64(arguments[2], 1) : 1;

    std::vector<std::string> parts;
    parts.reserve(delim.empty() ? 1U : (str.size() / delim.size()) + 1U);
    if (delim.empty()) {
      parts.push_back(str);
    } else {
      size_t start = 0;
      while (true) {
        size_t pos = str.find(delim, start);
        if (pos == std::string::npos) {
          parts.push_back(str.substr(start));
          break;
        }
        parts.push_back(str.substr(start, pos - start));
        start = pos + delim.size();
      }
    }

    const int64_t len =
        arguments.size() == 4
            ? to_i64(arguments[3], 1)
            : ((occ == 0 || occ == 1) ? static_cast<int64_t>(parts.size()) : 1);
    if (len <= 0) {
      return Value(std::string());
    }

    int64_t start_idx = 0;
    if (occ > 0) {
      start_idx = occ - 1;
      if (std::cmp_greater_equal(start_idx, parts.size())) {
        return Value(std::string());
      }
    } else if (occ < 0) {
      start_idx = static_cast<int64_t>(parts.size()) + occ;
      start_idx = std::max<int64_t>(start_idx, 0);
    } else {
      start_idx = 0;
    }

    const int64_t remaining = static_cast<int64_t>(parts.size()) - start_idx;
    const int64_t end_idx = start_idx + std::min(len, remaining);
    std::string res = parts[static_cast<size_t>(start_idx)];
    size_t res_bytes = res.size();
    for (int64_t i = start_idx + 1; i < end_idx; ++i) {
      res_bytes += delim.size() + parts[static_cast<size_t>(i)].size();
    }
    res.reserve(res_bytes);
    for (int64_t i = start_idx + 1; i < end_idx; ++i) {
      res += delim + parts[static_cast<size_t>(i)];
    }
    return Value(std::move(res));
  }

  if (name_view == "split") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SPLIT requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    const std::string delim =
        arguments.size() == 2 ? raw_str(arguments[1]) : ",";
    std::vector<Value> elems;
    elems.reserve(s.size());
    if (delim.empty()) {
      if (s.empty()) {
        elems.emplace_back(std::string());
        return Value::Array(std::move(elems), "STRING");
      }
      const auto offsets = utf8_offsets(s);
      for (size_t i = 1; i < offsets.size(); ++i) {
        elems.emplace_back(
            s.substr(offsets[i - 1], offsets[i] - offsets[i - 1]));
      }
      return Value::Array(std::move(elems), "STRING");
    }
    if (s.empty()) {
      elems.emplace_back(std::string());
      return Value::Array(std::move(elems), "STRING");
    }
    size_t start = 0;
    while (true) {
      size_t pos = s.find(delim, start);
      if (pos == std::string::npos) {
        elems.emplace_back(s.substr(start));
        break;
      }
      elems.emplace_back(s.substr(start, pos - start));
      start = pos + delim.size();
    }
    return Value::Array(std::move(elems), "STRING");
  }

  if (name_view == "soundex") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SOUNDEX requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    std::string s = raw_str(arguments[0]);
    size_t first_char = 0;
    while (first_char < s.size() &&
           (std::isspace(static_cast<unsigned char>(s[first_char])) != 0)) {
      ++first_char;
    }
    if (first_char >= s.size() ||
        (std::isalpha(static_cast<unsigned char>(s[first_char])) == 0)) {
      return Value(std::string());
    }
    std::string res;
    res.reserve(4);
    char first_letter = static_cast<char>(
        std::toupper(static_cast<unsigned char>(s[first_char])));
    res.push_back(first_letter);

    auto soundex_code = [](char c) -> char {
      c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
      if (c == 'B' || c == 'F' || c == 'P' || c == 'V') {
        return '1';
      }
      if (c == 'C' || c == 'G' || c == 'J' || c == 'K' || c == 'Q' ||
          c == 'S' || c == 'X' || c == 'Z') {
        return '2';
      }
      if (c == 'D' || c == 'T') {
        return '3';
      }
      if (c == 'L') {
        return '4';
      }
      if (c == 'M' || c == 'N') {
        return '5';
      }
      if (c == 'R') {
        return '6';
      }
      if (c == 'A' || c == 'E' || c == 'I' || c == 'O' || c == 'U' ||
          c == 'Y') {
        return 'V';
      }
      if (c == 'H' || c == 'W') {
        return 'H';
      }
      return '0';
    };

    char prev_code = soundex_code(first_letter);
    if (prev_code == 'V' || prev_code == 'H') {
      prev_code = '0';
    }
    for (size_t i = first_char + 1; i < s.size() && res.size() < 4; ++i) {
      if (std::isalpha(static_cast<unsigned char>(s[i])) == 0) {
        continue;
      }
      char code = soundex_code(s[i]);
      if (code == 'H') {
        continue;
      }
      if (code == 'V' || code == '0') {
        prev_code = '0';
        continue;
      }
      if (code != prev_code) {
        res.push_back(code);
        prev_code = code;
      }
    }
    while (res.size() < 4) {
      res.push_back('0');
    }
    return Value(std::move(res));
  }

  if (name_view == "translate") {
    if (arguments.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TRANSLATE requires 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        arguments[2].IsNull()) {
      return Value();
    }
    const StringArgView text(arguments[0]);
    const StringArgView src(arguments[1]);
    const StringArgView dst(arguments[2]);
    // P630: src/dst are constant across the rows of a batch, so decompose
    // them once and let the row loop probe a string_view table (ReplaceStr's
    // hoisted-pattern design) instead of rebuilding code-point vectors and
    // per-character std::string keys per row.
    struct TranslatePlan {
      std::string src;  // backing storage for the lookup keys
      std::string dst;  // backing storage for the replacement views
      std::vector<std::string_view> dst_chars;
      std::unordered_map<std::string_view, size_t> lookup;
      bool valid{false};  // false before the first build and after a
                          // duplicate-source error (which must re-raise)
    };
    static thread_local TranslatePlan plan;
    if (!plan.valid || plan.src != src.view || plan.dst != dst.view) {
      plan = TranslatePlan{};
      plan.src = std::string(src.view);
      plan.dst = std::string(dst.view);
      const auto src_offsets = utf8_offsets(plan.src);
      const auto dst_offsets = utf8_offsets(plan.dst);
      const size_t src_len = src_offsets.size() - 1;
      const size_t dst_len = dst_offsets.size() - 1;
      plan.dst_chars.reserve(dst_len);
      for (size_t i = 0; i < dst_len; ++i) {
        plan.dst_chars.emplace_back(plan.dst.data() + dst_offsets[i],
                                    dst_offsets[i + 1] - dst_offsets[i]);
      }
      plan.lookup.reserve(src_len);
      for (size_t i = 0; i < src_len; ++i) {
        const std::string_view source_char{plan.src.data() + src_offsets[i],
                                           src_offsets[i + 1] - src_offsets[i]};
        if (!plan.lookup.emplace(source_char, i).second) {
          // Leave `valid` false so the next row re-checks (and re-raises)
          // exactly like the per-row implementation did.
          return StatusError(StatusCode::kInvalidArgument,
                             "TRANSLATE source contains duplicate characters");
        }
      }
      plan.valid = true;
    }

    // Single monotonic walk of the row text with the same boundary rules as
    // utf8_offsets (orphan continuation bytes are dropped at the head and
    // fold into the preceding character elsewhere -- no separate vector).
    std::string res;
    res.reserve(text.view.size());
    size_t char_begin = 0;
    bool have_char = false;
    auto append_char = [&](size_t end) {
      if (!have_char) {
        return;
      }
      const std::string_view ch{text.view.data() + char_begin,
                                end - char_begin};
      const auto found = plan.lookup.find(ch);
      if (found == plan.lookup.end()) {
        res.append(ch);
      } else if (found->second < plan.dst_chars.size()) {
        res.append(plan.dst_chars[found->second]);
      }
    };
    for (size_t p = 0; p <= text.view.size();) {
      const bool at_end = p == text.view.size();
      const auto c = at_end ? 0U : static_cast<unsigned char>(text.view[p]);
      if (!at_end && (c & 0xC0U) == 0x80U) {
        p += 1;
        continue;  // orphan continuation byte: extends the pending character
      }
      append_char(p);
      if (at_end) {
        break;
      }
      char_begin = p;
      have_char = true;
      if ((c & 0xE0U) == 0xC0U) {
        p += (p + 1 < text.view.size()) ? 2 : 1;
      } else if ((c & 0xF0U) == 0xE0U) {
        p +=
            (p + 2 < text.view.size()) ? 3 : (p + 1 < text.view.size() ? 2 : 1);
      } else if ((c & 0xF8U) == 0xF0U) {
        p += (p + 3 < text.view.size())
                 ? 4
                 : (p + 2 < text.view.size()
                        ? 3
                        : (p + 1 < text.view.size() ? 2 : 1));
      } else {
        p += 1;
      }
    }
    return Value(std::move(res));
  }

  // The JSON document family (JSON_TYPE/KEYS/ARRAY_LENGTH/SET/INSERT/REPLACE/
  // REMOVE/ARRAY/OBJECT, LAX_* coercions): one shared implementation with the
  // AST evaluator and the constant-fold rewrite, via expression/json_doc.hpp.
  if (json_doc::family::IsJsonFamilyFunction(name)) {
    return json_doc::family::Evaluate(name, arguments);
  }

  // JSON_EXTRACT family: delegate to the single canonical implementation
  // shared with the AST evaluator and the constant-fold rewrite. A local
  // JVal parser once diverged from it on invalid JSON, number canonicalization,
  // surrogate pairs and NULL array elements; this path no longer has one.
  if (name_view == "json_extract" || name_view == "json_query" ||
      name_view == "json_value" || name_view == "json_extract_scalar" ||
      name_view == "json_extract_array" || name_view == "json_query_array" ||
      name_view == "json_value_array" ||
      name_view == "json_extract_string_array") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    const std::string path =
        arguments.size() == 2 ? raw_str(arguments[1]) : "$";
    return EvaluateJsonFunctionCall(name, raw_str(arguments[0]), path);
  }

  if (name_view == "to_json_string") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TO_JSON_STRING requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    return Value(std::string(s));
  }

  // Math functions
  auto to_numeric_double = [](const Value& val,
                              const std::string& fname) -> StatusOr<double> {
    if (val.type == ValueType::kInt64) {
      if (val.IsUnsigned()) {
        return static_cast<double>(static_cast<uint64_t>(val.value.int_value));
      }
      return static_cast<double>(val.value.int_value);
    }
    if (val.type == ValueType::kDouble) {
      return val.value.double_value;
    }
    return StatusError(StatusCode::kInvalidArgument,
                       fname + " requires numeric argument");
  };
  if (name_view == "abs") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ABS requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments[0].type == ValueType::kInt64) {
      if (arguments[0].IsUnsigned()) {
        return arguments[0];
      }
      // std::abs(INT64_MIN) is UB (returns INT64_MIN); GoogleSQL raises
      // "overflow" for ABS of the most-negative integer.
      if (arguments[0].value.int_value == std::numeric_limits<int64_t>::min()) {
        return StatusError(StatusCode::kIsInfinity, "integer overflow in ABS");
      }
      return Value(std::abs(arguments[0].value.int_value));
    }
    if (arguments[0].type == ValueType::kDouble) {
      return Value(std::fabs(arguments[0].value.double_value));
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "ABS requires numeric argument");
  }
  if (name_view == "sign") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SIGN requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments[0].type == ValueType::kInt64) {
      if (arguments[0].IsUnsigned()) {
        const auto v = static_cast<uint64_t>(arguments[0].value.int_value);
        return Value(v > 0 ? int64_t{1} : int64_t{0});
      }
      const int64_t v = arguments[0].value.int_value;
      return Value(v > 0 ? int64_t{1} : (v < 0 ? int64_t{-1} : int64_t{0}));
    }
    if (arguments[0].type == ValueType::kDouble) {
      const double v = arguments[0].value.double_value;
      if (std::isnan(v)) {
        return Value(v);
      }
      return Value(v > 0.0 ? 1.0 : (v < 0.0 ? -1.0 : 0.0));
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "SIGN requires numeric argument");
  }
  if (name_view == "trunc" || name_view == "truncate") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    int64_t digits = 0;
    if (arguments.size() == 2) {
      if (arguments[1].type != ValueType::kInt64) {
        return StatusError(StatusCode::kInvalidArgument,
                           name + " digits must be an integer");
      }
      digits = arguments[1].IsUnsigned() && arguments[1].value.int_value < 0
                   ? std::numeric_limits<int64_t>::max()
                   : arguments[1].value.int_value;
    }
    // INT64 input: truncation never needs a double round-trip (see ROUND).
    if (arguments[0].type == ValueType::kInt64) {
      if (arguments[0].IsUnsigned()) {
        const auto v = static_cast<uint64_t>(arguments[0].value.int_value);
        if (digits >= 0) {
          return arguments[0];
        }
        uint64_t scale = 1;
        // Negate in unsigned space: -digits overflows for INT64_MIN.
        const uint64_t trunc_digits =
            uint64_t{0} - static_cast<uint64_t>(digits);
        for (uint64_t i = 0; i < trunc_digits; ++i) {
          if (scale > std::numeric_limits<uint64_t>::max() / 10) {
            return Value(int64_t{0}).WithUnsigned();
          }
          scale *= 10;
        }
        return Value(static_cast<int64_t>(v / scale * scale)).WithUnsigned();
      }
      const int64_t v = arguments[0].value.int_value;
      if (digits >= 0) {
        return Value(v);
      }
      int64_t scale = 1;
      const uint64_t trunc_digits = uint64_t{0} - static_cast<uint64_t>(digits);
      for (uint64_t i = 0; i < trunc_digits; ++i) {
        if (scale > std::numeric_limits<int64_t>::max() / 10) {
          return Value(int64_t{0});
        }
        scale *= 10;
      }
      // (v / scale) * scale never overflows: its magnitude is <= |v|.
      return Value(v / scale * scale);
    }
    if (arguments[0].type == ValueType::kDouble) {
      const double val = arguments[0].value.double_value;
      if (std::isnan(val)) {
        return Value(val);
      }
      return Value(ScaleRoundDouble(val, digits, /*truncate=*/true));
    }
    return StatusError(StatusCode::kInvalidArgument,
                       name + " requires numeric argument");
  }
  if (name_view == "ceil" || name_view == "ceiling") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments[0].type == ValueType::kInt64) {
      return arguments[0];
    }
    if (arguments[0].type == ValueType::kDouble) {
      return Value(std::ceil(arguments[0].value.double_value));
    }
    return StatusError(StatusCode::kInvalidArgument,
                       name + " requires numeric argument");
  }
  if (name_view == "floor") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "FLOOR requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments[0].type == ValueType::kInt64) {
      return arguments[0];
    }
    if (arguments[0].type == ValueType::kDouble) {
      return Value(std::floor(arguments[0].value.double_value));
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "FLOOR requires numeric argument");
  }
  if (name_view == "mod") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "MOD requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    if (arguments[0].type == ValueType::kInt64 &&
        arguments[1].type == ValueType::kInt64) {
      if (arguments[0].IsUnsigned() || arguments[1].IsUnsigned()) {
        // Same rule as the AST reference (FunctionCallExpression): a signed
        // negative divisor against UINT64 has no valid result type.
        if (!arguments[1].IsUnsigned() && arguments[1].value.int_value < 0) {
          return StatusError(
              StatusCode::kInvalidArgument,
              "MOD with a negative divisor and UINT64 is invalid");
        }
        const auto r = static_cast<uint64_t>(arguments[1].value.int_value);
        if (r == 0) {
          return StatusError(StatusCode::kIsInfinity,
                             "division by zero in MOD");
        }
        return arguments[0].TryArithmetic(arguments[1],
                                          BinaryOperation::kModulo);
      }
      if (arguments[1].value.int_value == 0) {
        return StatusError(StatusCode::kIsInfinity, "division by zero in MOD");
      }
      // The AST reference (Value::operator%) raises on INT64_MIN % -1; the
      // fast path must mirror it, not quietly return the mathematical 0
      // (fuzzer-found: engine NULL vs AST throw through CASE).
      if (arguments[0].value.int_value == std::numeric_limits<int64_t>::min() &&
          arguments[1].value.int_value == -1) {
        return StatusError(StatusCode::kIsInfinity, "integer overflow on '%'");
      }
      return Value(arguments[0].value.int_value % arguments[1].value.int_value);
    }
    // GoogleSQL MOD is integer-only; the AST reference rejects two doubles
    // ("unsupported binary operation") while an int/double mix promotes to a
    // floating fmod (raising on a zero divisor). Mirror both exactly
    // (fuzzer-found: engine fmod'd two doubles the AST rejects).
    const bool left_double = arguments[0].type == ValueType::kDouble;
    const bool right_double = arguments[1].type == ValueType::kDouble;
    if (left_double && right_double) {
      return StatusError(StatusCode::kInvalidArgument,
                         "unsupported binary operation");
    }
    // Anything else (VARCHAR, DATE, ...) must raise, not fall through into
    // an out-of-type read of the Value union: reinterpretting a
    // std::string_view as an integer fmod'd into silent garbage.
    if (!left_double && arguments[0].type != ValueType::kInt64) {
      return StatusError(StatusCode::kInvalidArgument,
                         "unsupported argument type for MOD");
    }
    if (!right_double && arguments[1].type != ValueType::kInt64) {
      return StatusError(StatusCode::kInvalidArgument,
                         "unsupported argument type for MOD");
    }
    const double l =
        left_double ? arguments[0].value.double_value
                    : (arguments[0].IsUnsigned()
                           ? static_cast<double>(static_cast<uint64_t>(
                                 arguments[0].value.int_value))
                           : static_cast<double>(arguments[0].value.int_value));
    const double r =
        right_double
            ? arguments[1].value.double_value
            : (arguments[1].IsUnsigned()
                   ? static_cast<double>(
                         static_cast<uint64_t>(arguments[1].value.int_value))
                   : static_cast<double>(arguments[1].value.int_value));
    if (r == 0.0) {
      return StatusError(StatusCode::kIsInfinity, "division by zero");
    }
    return Value(std::fmod(l, r));
  }
  if (name_view == "pow" || name_view == "power") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    // Guard the int-or-double assumption: any other type would be read out
    // of the Value union (a std::string_view reinterpreted as a double and
    // pow'd into silent garbage) instead of raising.
    for (const Value& argument : arguments) {
      if (argument.type != ValueType::kInt64 &&
          argument.type != ValueType::kDouble) {
        return StatusError(StatusCode::kInvalidArgument,
                           "unsupported argument type for " + name);
      }
    }
    const double l =
        arguments[0].type == ValueType::kInt64
            ? (arguments[0].IsUnsigned()
                   ? static_cast<double>(
                         static_cast<uint64_t>(arguments[0].value.int_value))
                   : static_cast<double>(arguments[0].value.int_value))
            : arguments[0].value.double_value;
    const double r =
        arguments[1].type == ValueType::kInt64
            ? (arguments[1].IsUnsigned()
                   ? static_cast<double>(
                         static_cast<uint64_t>(arguments[1].value.int_value))
                   : static_cast<double>(arguments[1].value.int_value))
            : arguments[1].value.double_value;
    if (l < 0.0 && !std::isinf(l) && !std::isnan(r) && std::floor(r) != r) {
      return StatusError(StatusCode::kInvalidArgument,
                         "Floating point error in function: POW");
    }
    if (l == 0.0 && r < 0.0) {
      return StatusError(StatusCode::kIsInfinity, "division by zero in POW");
    }
    const double res = std::pow(l, r);
    if (std::isinf(res) && !std::isinf(l) && !std::isinf(r)) {
      return StatusError(StatusCode::kIsInfinity,
                         "Floating point overflow in function: POW");
    }
    return Value(res);
  }
  if (name_view == "sqrt") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SQRT requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], "SQRT"));
    if (v < 0.0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SQRT of negative number");
    }
    return Value(std::sqrt(v));
  }
  if (name_view == "cbrt") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CBRT requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], "CBRT"));
    return Value(AccurateCbrt(v));
  }
  if (name_view == "greatest" || name_view == "least") {
    if (arguments.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires at least 1 argument");
    }
    for (const auto& v : arguments) {
      if (v.IsNull()) {
        return Value();
      }
    }
    for (const auto& v : arguments) {
      if (v.type == ValueType::kDouble && std::isnan(v.value.double_value)) {
        return Value(std::numeric_limits<double>::quiet_NaN());
      }
    }
    Value best = arguments[0];
    // Mixed INT64/DOUBLE arguments promote to DOUBLE (GoogleSQL); the raw
    // Value::operator< throws across types, so compare numerically.
    const auto numeric = [](const Value& v) {
      return v.type == ValueType::kInt64 || v.type == ValueType::kDouble;
    };
    const auto as_double = [](const Value& v) {
      if (v.type == ValueType::kDouble) {
        return v.value.double_value;
      }
      if (v.IsUnsigned()) {
        return static_cast<double>(static_cast<uint64_t>(v.value.int_value));
      }
      return static_cast<double>(v.value.int_value);
    };
    for (size_t i = 1; i < arguments.size(); ++i) {
      if (best.type == ValueType::kInt64 &&
          arguments[i].type == ValueType::kInt64) {
        const bool takes =
            name_view == "greatest" ? arguments[i] > best : arguments[i] < best;
        if (takes) {
          best = arguments[i];
        }
      } else if (numeric(best) && numeric(arguments[i])) {
        const bool takes = name_view == "greatest"
                               ? as_double(arguments[i]) > as_double(best)
                               : as_double(arguments[i]) < as_double(best);
        if (takes) {
          best = arguments[i];
        }
      } else if (name_view == "greatest") {
        ASSIGN_OR_RETURN(bool, takes, arguments[i].TryGreater(best));
        if (takes) {
          best = arguments[i];
        }
      } else {
        ASSIGN_OR_RETURN(bool, takes, arguments[i].TryLess(best));
        if (takes) {
          best = arguments[i];
        }
      }
    }
    bool has_double = false;
    for (const auto& v : arguments) {
      if (v.type == ValueType::kDouble) {
        has_double = true;
        break;
      }
    }
    if (has_double && best.type == ValueType::kInt64) {
      best = Value(as_double(best));
    }
    return best;
  }
  if (name_view == "ln") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "LN requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], "LN"));
    if (v <= 0.0) {
      // T3885: GoogleSQL LN fails for x <= 0 (SAFE_LN returns NULL);
      // std::log would silently hand back -inf / NaN instead. Error
      // discipline mirrors the SQRT negative arm above.
      return StatusError(StatusCode::kInvalidArgument,
                         "LN of non-positive number");
    }
    return Value(std::log(v));
  }
  if (name_view == "log" || name_view == "log10") {
    if (name_view == "log10" ? arguments.size() != 1
                             : (arguments.empty() || arguments.size() > 2)) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " argument count mismatch");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], name));
    if (name_view == "log10") {
      // Mirror the LN guard: x <= 0 is a domain error, not -inf/NaN.
      if (v <= 0.0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "LOG10 of non-positive number");
      }
      return Value(std::log10(v));
    }
    if (arguments.size() == 1) {
      if (v <= 0.0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "LOG of non-positive number");
      }
      return Value(std::log(v));
    }
    ASSIGN_OR_RETURN(double, base, to_numeric_double(arguments[1], "LOG base"));
    if (base == 1.0) {
      // log(x)/log(1) divides by zero.  For a non-finite x the reference
      // engine yields NaN (compliance goldens: LOG(-inf, 1)); a finite x is
      // a domain error.
      if (!std::isfinite(v)) {
        return Value(std::numeric_limits<double>::quiet_NaN());
      }
      return StatusError(StatusCode::kInvalidArgument,
                         "LOG base must not be 1");
    }
    if (v == 1.0 && std::isinf(base)) {
      return Value(std::numeric_limits<double>::quiet_NaN());
    }
    // Domain: x > 0 and base > 0 (base == 1 handled above).  Without these
    // guards LOG(x, 0) returned +-0 and LOG(-1, b) / LOG(x, -b) returned
    // NaN instead of erroring like LN/SQRT.
    if (v <= 0.0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "LOG of non-positive number");
    }
    if (base <= 0.0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "LOG base must be positive");
    }
    return Value(std::log(v) / std::log(base));
  }
  if (name_view == "exp") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "EXP requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], "EXP"));
    const double res = std::exp(v);
    if (std::isinf(res) && !std::isinf(v)) {
      return StatusError(StatusCode::kIsInfinity,
                         "Floating point overflow in function: EXP");
    }
    return Value(res);
  }
  if (name_view == "cos") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "COS requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], "COS"));
    return Value(std::cos(v));
  }
  if (name_view == "sin") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SIN requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], "SIN"));
    return Value(std::sin(v));
  }
  if (name_view == "tan") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TAN requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], "TAN"));
    return Value(std::tan(v));
  }
  if (name_view == "acos") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ACOS requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], "ACOS"));
    if (v < -1.0 || v > 1.0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ACOS argument out of domain [-1, 1]");
    }
    return Value(std::acos(v));
  }
  if (name_view == "asin") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ASIN requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], "ASIN"));
    if (v < -1.0 || v > 1.0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ASIN argument out of domain [-1, 1]");
    }
    return Value(std::asin(v));
  }
  if (name_view == "atan") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ATAN requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], "ATAN"));
    return Value(std::atan(v));
  }
  if (name_view == "atan2") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ATAN2 requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, y, to_numeric_double(arguments[0], "ATAN2"));
    ASSIGN_OR_RETURN(double, x, to_numeric_double(arguments[1], "ATAN2"));
    return Value(std::atan2(y, x));
  }
  if (name_view == "pi") {
    if (!arguments.empty()) {
      return StatusError(StatusCode::kInvalidArgument, "PI takes no arguments");
    }
    return Value(M_PI);
  }
  if (name_view == "radians") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "RADIANS requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, r0, to_numeric_double(arguments[0], "RADIANS"));
    return Value(r0 * (M_PI / 180.0));
  }
  if (name_view == "degrees") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "DEGREES requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, r0, to_numeric_double(arguments[0], "DEGREES"));
    return Value(r0 * (180.0 / M_PI));
  }

  if (name_view == "cosh") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "COSH requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], "COSH"));
    const double res = std::cosh(v);
    if (std::isinf(res) && !std::isinf(v)) {
      return StatusError(StatusCode::kIsInfinity,
                         "Floating point overflow in function: COSH");
    }
    return Value(res);
  }
  if (name_view == "sinh") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SINH requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], "SINH"));
    const double res = std::sinh(v);
    if (std::isinf(res) && !std::isinf(v)) {
      return StatusError(StatusCode::kIsInfinity,
                         "Floating point overflow in function: SINH");
    }
    return Value(res);
  }
  // T2245: reciprocal trig family, mirrored from the canonical evaluator.
  if (name_view == "csc" || name_view == "sec" || name_view == "cot") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         std::string(name_view) + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v,
                     to_numeric_double(arguments[0], std::string(name_view)));
    if (name_view == "csc") {
      const double s = std::sin(v);
      if (s == 0.0) {
        return StatusError(StatusCode::kIsInfinity, "division by zero");
      }
      return Value(1.0 / s);
    }
    if (name_view == "sec") {
      const double c = std::cos(v);
      if (c == 0.0) {
        return StatusError(StatusCode::kIsInfinity, "division by zero");
      }
      return Value(1.0 / c);
    }
    const double s = std::sin(v);
    if (s == 0.0) {
      return StatusError(StatusCode::kIsInfinity, "division by zero");
    }
    return Value(std::cos(v) / s);
  }
  if (name_view == "sech" || name_view == "csch" || name_view == "coth") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         std::string(name_view) + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v,
                     to_numeric_double(arguments[0], std::string(name_view)));
    const double h = name_view == "sech" ? std::cosh(v) : std::sinh(v);
    if (std::isinf(h) && !std::isinf(v)) {
      return StatusError(
          StatusCode::kIsInfinity,
          "Floating point overflow in function: " + std::string(name_view));
    }
    if (h == 0.0) {
      return StatusError(StatusCode::kIsInfinity, "division by zero");
    }
    return Value(name_view == "coth" ? std::cosh(v) / h : 1.0 / h);
  }
  if (name_view == "tanh") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TANH requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], "TANH"));
    return Value(std::tanh(v));
  }
  if (name_view == "div") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "DIV requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    if (arguments[0].type == ValueType::kInt64 &&
        arguments[1].type == ValueType::kInt64) {
      if (arguments[0].IsUnsigned() || arguments[1].IsUnsigned()) {
        // Same rule as the AST reference (FunctionCallExpression): a signed
        // negative divisor against UINT64 has no valid result type.
        if (!arguments[1].IsUnsigned() && arguments[1].value.int_value < 0) {
          return StatusError(
              StatusCode::kInvalidArgument,
              "DIV with a negative divisor and UINT64 is invalid");
        }
        const auto r = static_cast<uint64_t>(arguments[1].value.int_value);
        if (r == 0) {
          return StatusError(StatusCode::kIsInfinity,
                             "division by zero in DIV");
        }
        return arguments[0].TryArithmetic(arguments[1],
                                          BinaryOperation::kDivide);
      }
      if (arguments[1].value.int_value == 0) {
        return StatusError(StatusCode::kIsInfinity, "division by zero in DIV");
      }
      if (arguments[0].value.int_value == std::numeric_limits<int64_t>::min() &&
          arguments[1].value.int_value == -1) {
        return StatusError(StatusCode::kIsInfinity, "integer overflow in DIV");
      }
      return Value(arguments[0].value.int_value / arguments[1].value.int_value);
    }
    ASSIGN_OR_RETURN(double, l, to_numeric_double(arguments[0], "DIV"));
    ASSIGN_OR_RETURN(double, r, to_numeric_double(arguments[1], "DIV"));
    if (r == 0.0) {
      return StatusError(StatusCode::kIsInfinity, "division by zero in DIV");
    }
    const double quotient = std::trunc(l / r);
    // static_cast<int64_t> of an out-of-range double is UB; DIV must raise.
    if (std::isnan(quotient) || std::isinf(quotient) ||
        quotient < static_cast<double>(std::numeric_limits<int64_t>::min()) ||
        quotient >= -static_cast<double>(std::numeric_limits<int64_t>::min())) {
      return StatusError(StatusCode::kIsInfinity,
                         "DIV result out of range for INT64");
    }
    return Value(static_cast<int64_t>(quotient));
  }
  if (name_view == "ieee_divide" || name_view == "safe_divide") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, l, to_numeric_double(arguments[0], name));
    ASSIGN_OR_RETURN(double, r, to_numeric_double(arguments[1], name));
    if (r == 0.0) {
      return name_view == "safe_divide" ? Value() : Value(l / r);
    }
    const double res = l / r;
    // Only overflow of finite operands is suppressed; non-finite inputs keep
    // their IEEE result (SAFE_DIVIDE(0, NaN) is NaN), matching GoogleSQL and
    // the AST evaluator.
    if (name_view == "safe_divide" && !std::isfinite(res) && std::isfinite(l) &&
        std::isfinite(r)) {
      return Value();
    }
    return Value(res);
  }

  if (name_view == "safe_add" || name_view == "safe_subtract" ||
      name_view == "safe_multiply") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    if ((arguments[0].type != ValueType::kInt64 &&
         arguments[0].type != ValueType::kDouble) ||
        (arguments[1].type != ValueType::kInt64 &&
         arguments[1].type != ValueType::kDouble)) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires numeric arguments");
    }
    if (arguments[0].type == ValueType::kInt64 &&
        arguments[1].type == ValueType::kInt64) {
      if (arguments[0].IsUnsigned() || arguments[1].IsUnsigned()) {
        const auto l = static_cast<uint64_t>(arguments[0].value.int_value);
        const auto r = static_cast<uint64_t>(arguments[1].value.int_value);
        uint64_t res = 0;
        if (name_view == "safe_add") {
          if (__builtin_add_overflow(l, r, &res)) {
            return Value();
          }
          return Value(static_cast<int64_t>(res)).WithUnsigned();
        }
        if (name_view == "safe_subtract") {
          if (__builtin_sub_overflow(l, r, &res)) {
            return Value();
          }
          return Value(static_cast<int64_t>(res)).WithUnsigned();
        }
        if (name_view == "safe_multiply") {
          if (__builtin_mul_overflow(l, r, &res)) {
            return Value();
          }
          return Value(static_cast<int64_t>(res)).WithUnsigned();
        }
      }
      const int64_t l = arguments[0].value.int_value;
      const int64_t r = arguments[1].value.int_value;
      if (name_view == "safe_add") {
        int64_t res = 0;
        if (__builtin_add_overflow(l, r, &res)) {
          return Value();
        }
        return Value(res);
      }
      if (name_view == "safe_subtract") {
        int64_t res = 0;
        if (__builtin_sub_overflow(l, r, &res)) {
          return Value();
        }
        return Value(res);
      }
      if (name_view == "safe_multiply") {
        int64_t res = 0;
        if (__builtin_mul_overflow(l, r, &res)) {
          return Value();
        }
        return Value(res);
      }
    }
    const double l =
        arguments[0].type == ValueType::kInt64
            ? (arguments[0].IsUnsigned()
                   ? static_cast<double>(
                         static_cast<uint64_t>(arguments[0].value.int_value))
                   : static_cast<double>(arguments[0].value.int_value))
            : arguments[0].value.double_value;
    const double r =
        arguments[1].type == ValueType::kInt64
            ? (arguments[1].IsUnsigned()
                   ? static_cast<double>(
                         static_cast<uint64_t>(arguments[1].value.int_value))
                   : static_cast<double>(arguments[1].value.int_value))
            : arguments[1].value.double_value;
    // SAFE_* suppresses the overflow error (finite inputs producing inf), but
    // must not swallow a NaN/Inf already present in an input: IEEE propagation
    // is the answer, not an error to mask. Keep the finite-inputs +
    // non-finite-result gate in sync with the AST evaluator and SAFE_DIVIDE.
    const double checked = name_view == "safe_add"
                               ? l + r
                               : (name_view == "safe_subtract" ? l - r : l * r);
    if (std::isfinite(l) && std::isfinite(r) && !std::isfinite(checked)) {
      return Value();
    }
    return Value(checked);
  }
  if (name_view == "safe_negate") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SAFE_NEGATE requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (arguments[0].type != ValueType::kInt64 &&
        arguments[0].type != ValueType::kDouble) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SAFE_NEGATE requires numeric argument");
    }
    if (arguments[0].type == ValueType::kInt64) {
      if (arguments[0].IsUnsigned()) {
        if (arguments[0].value.int_value == 0) {
          return arguments[0];
        }
        return Value();
      }
      if (arguments[0].value.int_value == std::numeric_limits<int64_t>::min()) {
        return Value();
      }
      return Value(-arguments[0].value.int_value);
    }
    return Value(-arguments[0].value.double_value);
  }
  if (name_view == "format") {
    return ExecuteFunction(name, arguments);
  }

  if (name_view == "current_timestamp") {
    if (!arguments.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CURRENT_TIMESTAMP takes no arguments");
    }
    const std::time_t now = std::time(nullptr);
    std::tm tm{};
    platform::GmTime(static_cast<int64_t>(now), &tm);
    std::array<char, 32> buffer{};
    if (std::strftime(buffer.data(), buffer.size(), "%Y-%m-%d %H:%M:%S", &tm) ==
        0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "timestamp formatting failed");
    }
    // Same canonical TIMESTAMP text as every other generator: UTC civil time
    // with an explicit "+00" zone, so comparisons never guess a zone.
    return Value(std::string(buffer.data()) + "+00");
  }

  if (name_view == "make_interval") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "make_interval requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    std::string val_str = raw_str(arguments[0]);
    std::string unit_str = raw_str(arguments[1]);
    ASSIGN_OR_RETURN(IntervalValue, iv,
                     IntervalValue::TryParse(val_str, unit_str));
    ASSIGN_OR_RETURN(std::string, text, iv.TryToString());
    return Value(std::move(text)).WithInterval();
  }

  if (name_view == "justify_hours") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "JUSTIFY_HOURS requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(IntervalValue, iv,
                     IntervalValue::TryParse(raw_str(arguments[0])));
    ASSIGN_OR_RETURN(IntervalValue, justified, iv.TryJustifyHours());
    ASSIGN_OR_RETURN(std::string, text, justified.TryToString());
    return Value(std::move(text)).WithInterval();
  }

  if (name_view == "justify_days") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "JUSTIFY_DAYS requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(IntervalValue, iv,
                     IntervalValue::TryParse(raw_str(arguments[0])));
    ASSIGN_OR_RETURN(IntervalValue, justified, iv.TryJustifyDays());
    ASSIGN_OR_RETURN(std::string, text, justified.TryToString());
    return Value(std::move(text)).WithInterval();
  }

  if (name_view == "justify_interval") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "JUSTIFY_INTERVAL requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(IntervalValue, iv,
                     IntervalValue::TryParse(raw_str(arguments[0])));
    ASSIGN_OR_RETURN(IntervalValue, justified, iv.TryJustifyInterval());
    ASSIGN_OR_RETURN(std::string, text, justified.TryToString());
    return Value(std::move(text)).WithInterval();
  }

  if (name_view == "collate") {
    // COLLATE(value, spec): attaches the collator to the value; the value
    // itself is unchanged.  The spec rule lives in type/collation.hpp so the
    // AST evaluator (expression/function_call_expression.cpp) resolves the
    // same tag for the same spec.
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "COLLATE requires 2 arguments");
    }
    const Value& v = arguments[0];
    if (v.IsNull()) {
      return Value();
    }
    const std::optional<Collation> spec =
        CollationOfSpec(raw_str(arguments[1]));
    if (!spec.has_value()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "COLLATE unsupported collation spec: " +
                             std::string(raw_str(arguments[1])));
    }
    return v.WithCollation(static_cast<uint8_t>(*spec));
  }

  if (name_view == "array_length") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_LENGTH requires 1 argument");
    }
    const Value& array = arguments[0];
    if (array.IsNull()) {
      return Value();
    }
    if (!array.IsArray()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_LENGTH requires an array");
    }
    return Value(static_cast<int64_t>(array.ArrayElements().size()));
  }

  if (name_view == "array_to_string") {
    if (arguments.size() != 2 && arguments.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_TO_STRING requires 2 or 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    if (!arguments[0].IsArray()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_TO_STRING requires an array");
    }
    const std::string delimiter = raw_str(arguments[1]);
    const bool has_null_text = arguments.size() == 3 && !arguments[2].IsNull();
    const std::string null_text =
        has_null_text ? raw_str(arguments[2]) : std::string();
    // Element text mirrors the AST evaluator: CAST-to-STRING rendering,
    // strings unquoted; nested arrays have no text form and error.
    const auto element_text =
        [](const Value& element) -> StatusOr<std::string> {
      switch (element.type) {
        case ValueType::kInt64:
          return element.IsUnsigned() ? std::to_string(static_cast<uint64_t>(
                                            element.value.int_value))
                                      : std::to_string(element.value.int_value);
        case ValueType::kDouble: {
          if (std::isnan(element.value.double_value)) {
            return std::string("nan");
          }
          if (std::isinf(element.value.double_value)) {
            return std::string(element.value.double_value > 0 ? "inf" : "-inf");
          }
          std::array<char, 64> buffer{};
          const auto [ptr, ec] =
              std::to_chars(buffer.data(), buffer.data() + buffer.size(),
                            element.value.double_value);
          if (ec != std::errc()) {
            return StatusError(StatusCode::kInvalidArgument,
                               "cannot format DOUBLE array element");
          }
          return std::string(buffer.data(),
                             static_cast<size_t>(ptr - buffer.data()));
        }
        case ValueType::kVarChar:
          return std::string(element.value.varchar_value);
        case ValueType::kDate:
          return FormatDateDays(element.value.int_value);
        case ValueType::kArray:
          return StatusError(StatusCode::kInvalidArgument,
                             "ARRAY_TO_STRING cannot format nested arrays");
        default:
          break;
      }
      return StatusError(StatusCode::kInvalidArgument,
                         "cannot format array element");
    };
    std::string out;
    out.reserve(arguments[0].ArrayElements().size());
    bool first = true;
    for (const Value& element : arguments[0].ArrayElements()) {
      if (element.IsNull()) {
        if (!has_null_text) {
          continue;
        }
        if (!first) {
          out += delimiter;
        }
        out += null_text;
        first = false;
        continue;
      }
      ASSIGN_OR_RETURN(std::string, text, element_text(element));
      if (!first) {
        out += delimiter;
      }
      out += text;
      first = false;
    }
    return Value(std::move(out));
  }

  // OFFSET(n) / ORDINAL(n): index accessors; both yield their argument with
  // the convention checked by the caller's bounds.
  if (name_view == "offset" || name_view == "ordinal") {
    if (arguments.size() != 1 || arguments[0].IsNull()) {
      return Value();
    }
    const Value& index = arguments[0];
    if (index.type == ValueType::kInt64) {
      return index;
    }
    if (index.type == ValueType::kDouble) {
      return Value(SaturatingDoubleToInt64(index.value.double_value));
    }
    // Reading double_value of a VARCHAR/DATE/ARRAY cell type-puns the union.
    return StatusError(StatusCode::kInvalidArgument,
                       std::string(name_view) + " requires an INT64 index");
  }

  if (name_view == "is_inf" || name_view == "is_nan") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         std::string(name_view) + " requires 1 argument");
    }
    const Value& arg = arguments[0];
    if (arg.IsNull()) {
      return Value();
    }
    if (arg.type != ValueType::kDouble) {
      return Value(int64_t{0});
    }
    const double v = arg.value.double_value;
    if (name_view == "is_inf") {
      return Value(std::isinf(v) ? int64_t{1} : int64_t{0});
    }
    return Value(std::isnan(v) ? int64_t{1} : int64_t{0});
  }

  if (name_view == "hll_count.extract") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "HLL_COUNT.EXTRACT requires 1 argument");
    }
    const Value& sketch = arguments[0];
    if (sketch.IsNull()) {
      return Value(
          int64_t{0});  // GoogleSQL defines a NULL sketch's estimate as zero.
    }
    int typecode = 0;
    int64_t precision = 0;
    std::vector<std::string> entries;
    if (!DecodeSketch(SketchBytesOf(sketch), &typecode, &precision, &entries)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "Invalid sketch in HLL_COUNT.EXTRACT");
    }
    return Value(static_cast<int64_t>(entries.size()));
  }

  if (name.starts_with("kll_quantiles.extract_")) {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "KLL_QUANTILES.EXTRACT requires 2 arguments");
    }
    const Value& sketch = arguments[0];
    const Value& number = arguments[1];
    if (number.IsNull()) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "The second argument to KLL_QUANTILES.EXTRACT must be positive");
    }
    if (number.type != ValueType::kInt64) {
      // Reading int_value of a DOUBLE/VARCHAR cell type-puns the union.
      return StatusError(StatusCode::kInvalidArgument,
                         "The second argument to KLL_QUANTILES.EXTRACT "
                         "must be an INT64");
    }
    if (number.value.int_value < 1) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "The second argument to KLL_QUANTILES.EXTRACT must be positive");
    }
    if (sketch.IsNull()) {
      return Value();
    }
    return TryExtractSketchQuantilesStatic(sketch, number.value.int_value);
  }

  if (name_view == "array_concat") {
    if (arguments.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_CONCAT requires at least 1 argument");
    }
    std::vector<Value> merged;
    merged.reserve(arguments.size());
    std::string element_type;
    for (const Value& arr : arguments) {
      if (arr.IsNull()) {
        return Value();
      }
      if (!arr.IsArray()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "ARRAY_CONCAT requires ARRAY arguments");
      }
      if (element_type.empty()) {
        element_type = arr.ArrayElementSqlType();
      }
      const auto& elements = arr.ArrayElements();
      merged.insert(merged.end(), elements.begin(), elements.end());
    }
    return Value::Array(std::move(merged), std::move(element_type));
  }

  if (name_view == "array_first" || name_view == "array_last") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 argument");
    }
    const Value& arr = arguments[0];
    if (arr.IsNull()) {
      return Value();
    }
    if (!arr.IsArray()) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires an array");
    }
    const auto& elements = arr.ArrayElements();
    if (elements.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         name_view == "array_first"
                             ? "ARRAY_FIRST cannot get the first element "
                               "of an empty array"
                             : "ARRAY_LAST cannot get the last element of "
                               "an empty array");
    }
    return name_view == "array_first" ? elements.front() : elements.back();
  }

  if (name_view == "array_slice") {
    // ARRAY_SLICE(arr, offset[, size]): 1-based ORDINAL-style bounds; a
    // negative offset counts from the end. Out-of-range clamps to [].
    if (arguments.size() < 2 || arguments.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_SLICE requires 2 or 3 arguments");
    }
    const Value& arr = arguments[0];
    if (arr.IsNull()) {
      return Value();
    }
    if (!arr.IsArray()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_SLICE requires an array");
    }
    if (arguments[1].IsNull() ||
        (arguments.size() == 3 && arguments[2].IsNull())) {
      return Value();
    }
    for (size_t i = 1; i < arguments.size(); ++i) {
      if (arguments[i].type != ValueType::kInt64 || arguments[i].IsUnsigned()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "ARRAY_SLICE bounds require INT64");
      }
    }
    const auto& elements = arr.ArrayElements();
    const auto n = static_cast<int64_t>(elements.size());
    int64_t offset = arguments[1].value.int_value;
    int64_t size = arguments.size() == 3 ? arguments[2].value.int_value : n;
    if (size <= 0 || n == 0) {
      return Value::Array({}, arr.ArrayElementSqlType());
    }
    if (offset == 0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_SLICE offset must be non-zero (1-based)");
    }
    int64_t begin = offset > 0 ? offset - 1 : n + offset;
    const platform::Int128 wide_end =
        static_cast<platform::Int128>(begin) + size;
    int64_t end = static_cast<int64_t>(std::min<platform::Int128>(wide_end, n));
    begin = std::max<int64_t>(begin, 0);
    end = std::min<int64_t>(end, n);
    std::vector<Value> picked;
    picked.reserve(static_cast<size_t>(std::max<int64_t>(0, end - begin)));
    for (int64_t i = begin; i < end; ++i) {
      picked.push_back(elements[static_cast<size_t>(i)]);
    }
    return Value::Array(std::move(picked), arr.ArrayElementSqlType());
  }

  if (name_view == "array_is_distinct") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_IS_DISTINCT requires 1 argument");
    }
    const Value& arr = arguments[0];
    if (arr.IsNull()) {
      return Value();
    }
    if (!arr.IsArray()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_IS_DISTINCT requires an array");
    }
    const auto& elements = arr.ArrayElements();
    auto same = [](const Value& a, const Value& b) {
      if (a.IsNull() && b.IsNull()) {
        return true;
      }
      if (a.IsNull() || b.IsNull()) {
        return false;
      }
      // ARRAY equality (Value::EqualsSlow) folds every NaN into one peer
      // while TryBinary's IEEE '=' does not; match the array-equality
      // reading so [NaN, NaN] is not reported distinct.
      if (a.type == ValueType::kDouble && b.type == ValueType::kDouble &&
          std::isnan(a.value.double_value) &&
          std::isnan(b.value.double_value)) {
        return true;
      }
      try {
        StatusOr<Value> cmp = TryBinary(BinaryOperation::kEquals, a, b);
        return cmp.HasValue() && cmp.Value().Truthy();
      } catch (...) {
        return false;
      }
    };
    for (size_t i = 0; i < elements.size(); ++i) {
      for (size_t j = i + 1; j < elements.size(); ++j) {
        if (same(elements[i], elements[j])) {
          return Value(int64_t{0});
        }
      }
    }
    return Value(int64_t{1});
  }

  if (name_view == "generate_array") {
    return ExecuteFunction(name, arguments);
  }

  // GENERATE_SERIES is the PostgreSQL-compatible spelling of the numeric
  // array generator.  It deliberately returns an array so the existing
  // UNNEST source path can consume it without introducing a second scan
  // operator.  The implementation lives in the expression layer
  // (ExecuteFunction): AST evaluation is the semantic ground truth and the
  // plan-layer constant folds share this exact code path, so delegating
  // here keeps the two engines element-wise identical (integer stepping
  // stays exact near INT64 boundaries).
  if (name_view == "generate_series") {
    return ExecuteFunction(name, arguments);
  }

  // T2242: GENERATE_TIMESTAMP_ARRAY keeps its single implementation in the
  // expression layer; delegating (like generate_series above) keeps the
  // batch engine element-wise identical to the AST ground truth.
  if (name_view == "generate_timestamp_array") {
    return ExecuteFunction(name, arguments);
  }

  // T2241: PARSE_DATETIME is implemented AST-first (expression layer); the
  // batch delegates so the format parsing, the 1970 field defaults and the
  // canonical zone-less rendering cannot diverge between the engines.
  if (name_view == "parse_datetime") {
    return ExecuteFunction(name, arguments);
  }

  // T2236: same delegation rationale for REGEXP_EXTRACT_GROUPS (the pattern
  // rewriting and match walking must not diverge between the engines).
  if (name_view == "regexp_extract_groups") {
    return ExecuteFunction(name, arguments);
  }

  // T2237: CONTAINS_SUBSTR likewise stays single-sourced in the expression
  // layer so case folding and array walking are provably identical.
  if (name_view == "contains_substr") {
    return ExecuteFunction(name, arguments);
  }

  if (name_view == "generate_date_array") {
    if (call.Args().size() < 2 || call.Args().size() > 3 ||
        arguments.size() < 2 || arguments.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "GENERATE_DATE_ARRAY requires 2 or 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() == 3 && arguments[2].IsNull())) {
      return Value();
    }
    auto to_days = [](const Value& v) -> StatusOr<int64_t> {
      if (v.type == ValueType::kDate) {
        return v.DateDays();
      }
      if (v.type == ValueType::kVarChar) {
        auto parsed = TryParseDateDays(v.value.varchar_value);
        if (parsed.HasValue()) {
          return parsed.Value();
        }
      }
      return StatusError(StatusCode::kInvalidArgument, "DATE value required");
    };
    auto start_days_or = to_days(arguments[0]);
    if (!start_days_or.HasValue()) {
      return StatusError(StatusCode::kInvalidArgument, "DATE value required");
    }
    auto end_days_or = to_days(arguments[1]);
    if (!end_days_or.HasValue()) {
      return StatusError(StatusCode::kInvalidArgument, "DATE value required");
    }
    const int64_t start_days = start_days_or.Value();
    const int64_t end_days = end_days_or.Value();
    if (start_days < -11000000 || start_days > 11000000 ||
        end_days < -11000000 || end_days > 11000000) {
      return StatusError(StatusCode::kInvalidArgument,
                         "DATE value out of range");
    }
    int64_t step_days = 1;
    int64_t step_months = 0;
    if (call.Args().size() == 3) {
      // Decode the step from the evaluated argument value, exactly what the
      // AST ground truth does. A literal INTERVAL argument evaluates to the
      // canonical interval text (the evaluator preserves the AST's interval
      // domain through folding, batches and spills), and column-valued
      // steps arrive pre-evaluated as the encoded text of a make_interval
      // call (INTERVAL col DAY). Do not decode the expression-side
      // Amount()/Unit() here: with composite intervals the leading amount
      // drops the sub-fields ("1-6" YEAR TO MONTH became a 12-month step
      // instead of 18, "2 3" DAY TO HOUR became 2 days) and skips the
      // sub-field validation the AST applies.
      const Value& step = arguments[2];
      if (step.type == ValueType::kInt64) {
        step_days = step.value.int_value;
      } else {
        const std::string text = raw_str(step);
        ASSIGN_OR_RETURN(IntervalValue, parsed,
                         text.empty() ? StatusOr<IntervalValue>{IntervalValue{}}
                                      : IntervalValue::TryParse(text));
        if (parsed.nanos != 0 || (parsed.months != 0 && parsed.days != 0)) {
          return StatusError(
              StatusCode::kInvalidArgument,
              "unsupported GENERATE_DATE_ARRAY step unit: the step must "
              "be a whole number of days or of months");
        }
        if (parsed.months != 0) {
          step_months = parsed.months;
        } else {
          step_days = parsed.days;
        }
      }
    }
    if (step_days == 0 && step_months == 0) {
      return StatusError(StatusCode::kIsInfinity, "Sequence step cannot be 0.");
    }
    if ((step_days >= 0 && step_months >= 0 && start_days > end_days) ||
        (step_days <= 0 && step_months <= 0 && start_days < end_days)) {
      return Value::Array({}, "DATE");
    }
    std::vector<Value> elements;
    constexpr size_t kMaxGeneratedDates = 1'000'000;
    if (step_months != 0) {
      int64_t d = start_days;
      while (step_months > 0 ? d <= end_days : d >= end_days) {
        if (elements.size() == kMaxGeneratedDates) {
          return StatusError(StatusCode::kInvalidArgument,
                             "GENERATE_DATE_ARRAY generated too many elements");
        }
        if (d < -11000000 || d > 11000000) {
          return StatusError(StatusCode::kInvalidArgument,
                             "DATE value out of range");
        }
        elements.push_back(Value::DateFromDays(d));
        ASSIGN_OR_RETURN(int64_t, next,
                         (TryAddDateIntervalDays(d, step_months, "month")));
        if (next == d) {
          break;
        }
        d = next;
      }
      return Value::Array(std::move(elements), "DATE");
    }
    for (int64_t d = start_days; step_days > 0 ? d <= end_days : d >= end_days;
         d += step_days) {
      if (elements.size() == kMaxGeneratedDates) {
        return StatusError(StatusCode::kInvalidArgument,
                           "GENERATE_DATE_ARRAY generated too many elements");
      }
      if (d < -11000000 || d > 11000000) {
        return StatusError(StatusCode::kInvalidArgument,
                           "DATE value out of range");
      }
      elements.push_back(Value::DateFromDays(d));
      int64_t next = 0;
      if (__builtin_add_overflow(d, step_days, &next)) {
        break;
      }
    }
    return Value::Array(std::move(elements), "DATE");
  }

  if (name_view == "array_includes" || name_view == "array_includes_any" ||
      name_view == "array_includes_all") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 2 arguments");
    }
    const Value& input = arguments[0];
    const Value& target = arguments[1];
    // GoogleSQL: a NULL array or NULL target yields NULL.
    if (input.IsNull() || target.IsNull()) {
      return Value();
    }
    if (!input.IsArray() ||
        (name_view != "array_includes" && !target.IsArray())) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires ARRAY arguments");
    }
    auto equals = [](const Value& left, const Value& right) {
      try {
        StatusOr<Value> cmp = TryBinary(BinaryOperation::kEquals, left, right);
        return cmp.HasValue() && cmp.Value().Truthy();
      } catch (...) {
        return false;
      }
    };
    const auto& haystack = input.ArrayElements();
    bool matched = false;
    if (name_view == "array_includes") {
      matched = false;
      for (const Value& element : haystack) {
        if (!element.IsNull() && equals(element, target)) {
          matched = true;
          break;
        }
      }
    } else {
      const auto& targets = target.ArrayElements();
      matched = name_view != "array_includes_any";
      for (const Value& wanted : targets) {
        if (wanted.IsNull()) {
          // NULL target elements match nothing: ANY cannot use them and ALL
          // fails outright.
          if (name_view == "array_includes_all") {
            matched = false;
          }
          continue;
        }
        bool found = false;
        for (const Value& element : haystack) {
          if (!element.IsNull() && equals(element, wanted)) {
            found = true;
            break;
          }
        }
        if (name_view == "array_includes_any") {
          if (found) {
            matched = true;
            break;
          }
        } else if (!found) {
          matched = false;
          break;
        }
      }
    }
    return Value(matched);
  }

  if (name_view == "rand") {
    if (!arguments.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "RAND requires no arguments");
    }
    static thread_local std::mt19937_64 rng(
        std::random_device{}() ^
        static_cast<uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count()));
    static thread_local std::uniform_real_distribution<double> uniform(0.0,
                                                                       1.0);
    return Value(uniform(rng));
  }

  if (name_view == "generate_uuid") {
    if (!arguments.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "GENERATE_UUID requires no arguments");
    }
    static thread_local std::mt19937_64 rng(
        std::random_device{}() ^
        static_cast<uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count()));
    const uint64_t r1 = rng();
    const uint64_t r2 = rng();
    std::string uuid;
    uuid.reserve(36);
    auto append_hex = [&uuid](uint64_t value, int nibbles) {
      static constexpr std::string_view kDigits = "0123456789abcdef";
      for (int i = nibbles - 1; i >= 0; --i) {
        uuid.push_back(kDigits[(value >> (4 * i)) & 0xF]);
      }
    };
    append_hex(r1 >> 32, 8);
    uuid.push_back('-');
    append_hex((r1 >> 16) & 0xFFFF, 4);
    uuid.push_back('-');
    uuid.push_back('4');  // RFC 4122 version 4 (random)
    append_hex(r1 & 0x0FFF, 3);
    uuid.push_back('-');
    append_hex(0x8 + (r2 >> 62), 1);  // variant 10xx
    append_hex((r2 >> 46) & 0x3FFF, 3);
    uuid.push_back('-');
    append_hex(r2 & 0xFFFFFFFFFFFFULL, 12);
    return Value(std::move(uuid));
  }

  if (name_view == "md5" || name_view == "sha1" || name_view == "sha256" ||
      name_view == "sha512" || name_view == "farm_fingerprint") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const std::string input = raw_str(arguments[0]);
    if (name_view == "md5") {
      return Value(digest::Md5Digest(input));
    }
    if (name_view == "sha1") {
      return Value(digest::Sha1Digest(input));
    }
    if (name_view == "sha256") {
      return Value(digest::Sha256Digest(input));
    }
    if (name_view == "sha512") {
      return Value(digest::Sha512Digest(input));
    }
    return Value(static_cast<int64_t>(digest::FarmFingerprint64(input)));
  }

  // PARSE_JSON validates strict JSON text and hands it back in the engine's
  // string-based JSON representation; TO_JSON renders a scalar/struct value
  // as JSON text (same shape as the AST evaluator's branch).
  if (name_view == "parse_json" || name_view == "to_json") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (name_view == "parse_json") {
      if (arguments[0].type != ValueType::kVarChar) {
        return StatusError(StatusCode::kInvalidArgument,
                           "PARSE_JSON requires a string argument");
      }
      const std::string_view text = arguments[0].value.varchar_value;
      if (!json_text::IsValid(text)) {
        return StatusError(StatusCode::kInvalidArgument,
                           "Parsed JSON is invalid");
      }
      return Value(std::string(text));
    }
    return Value(json_text::ToJsonText(arguments[0]));
  }

  if (name_view == "to_hex") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TO_HEX requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    return Value(digest::ToHex(raw_str(arguments[0])));
  }

  if (name_view == "to_base64") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TO_BASE64 requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    static constexpr std::string_view kAlphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    const std::string input = raw_str(arguments[0]);
    std::string out;
    out.reserve((input.size() + 2) / 3 * 4);
    size_t i = 0;
    while (i + 3 <= input.size()) {
      const auto triple = static_cast<uint32_t>(
          (static_cast<unsigned char>(input[i]) << 16) |
          (static_cast<unsigned char>(input[i + 1]) << 8) |
          static_cast<unsigned char>(input[i + 2]));
      out.push_back(kAlphabet[(triple >> 18) & 0x3F]);
      out.push_back(kAlphabet[(triple >> 12) & 0x3F]);
      out.push_back(kAlphabet[(triple >> 6) & 0x3F]);
      out.push_back(kAlphabet[triple & 0x3F]);
      i += 3;
    }
    const size_t remainder = input.size() - i;
    if (remainder == 1) {
      const auto byte =
          static_cast<uint32_t>(static_cast<unsigned char>(input[i]));
      out.push_back(kAlphabet[(byte >> 2) & 0x3F]);
      out.push_back(kAlphabet[(byte & 0x03) << 4]);
      out += "==";
    } else if (remainder == 2) {
      const auto pair =
          static_cast<uint32_t>((static_cast<unsigned char>(input[i]) << 8) |
                                static_cast<unsigned char>(input[i + 1]));
      out.push_back(kAlphabet[(pair >> 10) & 0x3F]);
      out.push_back(kAlphabet[(pair >> 4) & 0x3F]);
      out.push_back(kAlphabet[(pair & 0x0F) << 2]);
      out.push_back('=');
    }
    return Value(std::move(out));
  }

  if (name_view == "from_base64") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "FROM_BASE64 requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const std::string input = raw_str(arguments[0]);
    auto value_of = [](char c) -> int {
      if (c >= 'A' && c <= 'Z') {
        return c - 'A';
      }
      if (c >= 'a' && c <= 'z') {
        return c - 'a' + 26;
      }
      if (c >= '0' && c <= '9') {
        return c - '0' + 52;
      }
      if (c == '+') {
        return 62;
      }
      if (c == '/') {
        return 63;
      }
      return -1;
    };
    std::string out;
    out.reserve(input.size() / 4 * 3);
    uint32_t buffer = 0;
    int bits = 0;
    for (const char c : input) {
      if (std::isspace(static_cast<unsigned char>(c)) != 0) {
        continue;
      }
      if (c == '=') {
        break;
      }
      const int decoded = value_of(c);
      if (decoded < 0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "FROM_BASE64: invalid character in input");
      }
      buffer = (buffer << 6) | static_cast<uint32_t>(decoded);
      bits += 6;
      if (bits >= 8) {
        bits -= 8;
        out.push_back(static_cast<char>((buffer >> bits) & 0xFF));
      }
    }
    return Value(std::move(out));
  }

  // T2239 / T2240: RFC 4648 base32 pair, mirroring the base64 arms'
  // leniency (whitespace skipped, padding ends the payload, lowercase
  // accepted) so FROM_/TO_ round-trip for any encoding we accept.
  if (name_view == "to_base32") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TO_BASE32 requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    static constexpr std::string_view kAlphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    static constexpr size_t kPadChars[] = {
        0, 6, 4, 3, 1};  // NOLINT(modernize-avoid-c-arrays)
    const std::string input = raw_str(arguments[0]);
    std::string out;
    out.reserve((input.size() + 4) / 5 * 8);
    uint64_t buffer = 0;
    int bits = 0;
    for (const char c : input) {
      buffer = (buffer << 8) | static_cast<unsigned char>(c);
      bits += 8;
      while (bits >= 5) {
        bits -= 5;
        out.push_back(kAlphabet[(buffer >> bits) & 0x1F]);
      }
    }
    if (bits > 0) {
      out.push_back(kAlphabet[(buffer << (5 - bits)) & 0x1F]);
    }
    out.append(kPadChars[input.size() % 5], '=');
    return Value(std::move(out));
  }

  if (name_view == "from_base32") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "FROM_BASE32 requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const std::string input = raw_str(arguments[0]);
    auto value_of = [](char c) -> int {
      if (c >= 'A' && c <= 'Z') {
        return c - 'A';
      }
      if (c >= 'a' && c <= 'z') {
        return c - 'a';
      }
      if (c >= '2' && c <= '7') {
        return c - '2' + 26;
      }
      return -1;
    };
    std::string out;
    out.reserve(input.size() / 8 * 5);
    uint32_t buffer = 0;
    int bits = 0;
    for (const char c : input) {
      if (std::isspace(static_cast<unsigned char>(c)) != 0) {
        continue;
      }
      if (c == '=') {
        break;
      }
      const int decoded = value_of(c);
      if (decoded < 0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "FROM_BASE32: invalid character in input");
      }
      buffer = (buffer << 5) | static_cast<uint32_t>(decoded);
      bits += 5;
      if (bits >= 8) {
        bits -= 8;
        out.push_back(static_cast<char>((buffer >> bits) & 0xFF));
      }
    }
    return Value(std::move(out));
  }

  if (name_view == "from_hex") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "FROM_HEX requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    std::string text = raw_str(arguments[0]);
    std::string out;
    out.reserve(text.size() / 2);
    auto nibble = [](char c) -> int {
      if (c >= '0' && c <= '9') {
        return c - '0';
      }
      if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
      }
      if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
      }
      return -1;
    };
    size_t i = 0;
    if (text.size() % 2 != 0) {
      const int high = nibble(text[0]);
      if (high < 0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "FROM_HEX: invalid hex string");
      }
      out.push_back(static_cast<char>(high));
      i = 1;
    }
    for (; i + 1 < text.size(); i += 2) {
      const int hi = nibble(text[i]);
      const int lo = nibble(text[i + 1]);
      if (hi < 0 || lo < 0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "FROM_HEX: invalid hex string");
      }
      out.push_back(static_cast<char>((hi * 16) + lo));
    }
    return Value(std::move(out));
  }

  if (name_view == "safe_convert_bytes_to_string") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SAFE_CONVERT_BYTES_TO_STRING requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    // The payload is raw bytes: a `\xHH`-shaped byte run is data, not an
    // escape to decode (string-literal escapes are the visitor's job —
    // DecodeStringEscapesImpl already expanded b'\x..' literals). Decoding
    // here mangled valid UTF-8 data that merely contained that text, so the
    // conversion below only replaces invalid UTF-8 with U+FFFD.
    const std::string input = raw_str(arguments[0]);
    std::string out;
    out.reserve(input.size());
    const std::string replacement = "\xEF\xBF\xBD";
    for (size_t i = 0; i < input.size();) {
      const auto lead = static_cast<unsigned char>(input[i]);
      size_t length = 0;
      unsigned char min_cont = 0x80;
      unsigned char max_cont = 0xBF;
      if (lead < 0x80) {
        length = 1;
      } else if (lead >= 0xC2 && lead <= 0xDF) {
        length = 2;
      } else if (lead >= 0xE0 && lead <= 0xEF) {
        length = 3;
        if (lead == 0xE0) {
          min_cont = 0xA0;
        }
        if (lead == 0xED) { /* ED 9F BF is the last code point */
          max_cont = 0x9F;
        }
      } else if (lead >= 0xF0 && lead <= 0xF4) {
        length = 4;
        if (lead == 0xF0) {
          min_cont = 0x90;
        }
        if (lead == 0xF4) { /* F4 8F BF BF is U+10FFFF, the last code point */
          max_cont = 0x8F;
        }
      }
      bool valid = length > 0 && i + length <= input.size();
      for (size_t j = 1; valid && j < length; ++j) {
        const auto cont = static_cast<unsigned char>(input[i + j]);
        if (cont < (j == 1 ? min_cont : 0x80) ||
            cont > (j == 1 ? max_cont : 0xBF)) {
          valid = false;
        }
      }
      if (valid && length == 1) {
        out.push_back(input[i]);
        ++i;
      } else if (valid) {
        out.append(input, i, length);
        i += length;
      } else {
        out += replacement;
        ++i;
      }
    }
    return Value(std::move(out));
  }

  if (name_view == "bit_cast_to_int64" || name_view == "bit_cast_to_uint64") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const Value& input = arguments[0];
    uint64_t bits = 0;
    if (input.type == ValueType::kInt64) {
      bits = static_cast<uint64_t>(input.value.int_value);
    } else if (input.type == ValueType::kDouble) {
      std::memcpy(&bits, &input.value.double_value, sizeof(bits));
    } else if (input.type == ValueType::kVarChar) {
      // BYTES payloads carry little-endian 64-bit patterns.
      const std::string bytes = raw_str(input);
      if (bytes.size() > 8) {
        return StatusError(StatusCode::kInvalidArgument,
                           "BIT_CAST requires at most 8 bytes");
      }
      for (size_t i = 0; i < bytes.size(); ++i) {
        bits |= static_cast<uint64_t>(static_cast<unsigned char>(bytes[i]))
                << (8 * i);
      }
    } else {
      return StatusError(StatusCode::kInvalidArgument,
                         "unsupported BIT_CAST input type");
    }
    if (name_view == "bit_cast_to_int64") {
      return Value(static_cast<int64_t>(bits));
    }
    // UINT64 is exactly representable through the Value unsigned tag
    // (collation_ == 3): keep the full 64-bit pattern so values >= 2^63
    // stay precise and order above every signed INT64 instead of widening
    // to double.
    return Value(static_cast<int64_t>(bits)).WithUnsigned();
  }

  if (name_view == "array_reverse") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_REVERSE requires 1 argument");
    }
    const Value& array = arguments[0];
    if (array.IsNull()) {
      return Value();
    }
    if (!array.IsArray()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_REVERSE requires an ARRAY argument");
    }
    std::vector<Value> reversed(array.ArrayElements().rbegin(),
                                array.ArrayElements().rend());
    return Value::Array(std::move(reversed), array.ArrayElementSqlType());
  }

  if (name_view == "euclidean_distance") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "EUCLIDEAN_DISTANCE requires 2 arguments");
    }
    const Value& left = arguments[0];
    const Value& right = arguments[1];
    if (left.IsNull() || right.IsNull()) {
      return Value();
    }
    if (!left.IsArray() || !right.IsArray()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "EUCLIDEAN_DISTANCE requires ARRAY arguments");
    }
    const auto& a = left.ArrayElements();
    const auto& b = right.ArrayElements();
    if (a.size() != b.size()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "EUCLIDEAN_DISTANCE arrays must have equal sizes");
    }
    auto coordinate = [](const Value& v) -> StatusOr<double> {
      if (v.IsNull()) {
        return 0.0;
      }
      if (v.type == ValueType::kDouble) {
        return v.value.double_value;
      }
      if (v.type == ValueType::kInt64) {
        return v.IsUnsigned() ? static_cast<double>(
                                    static_cast<uint64_t>(v.value.int_value))
                              : static_cast<double>(v.value.int_value);
      }
      // ARRAY<STRUCT<key, value>> elements stay JSON-encoded: the distance
      // runs over the numeric payload (last member); the leading member is
      // only the pair key.
      if (v.type == ValueType::kVarChar) {
        const std::string text(v.value.varchar_value);
        if (text.size() >= 2 && text.front() == '{' && text.back() == '}') {
          const auto members =
              SplitJsonObjectMembers(text.substr(1, text.size() - 2));
          if (!members.empty()) {
            Value parsed;
            if (JsonTextToValue(members.back().second, &parsed)) {
              if (parsed.type == ValueType::kDouble) {
                return parsed.value.double_value;
              }
              if (parsed.type == ValueType::kInt64) {
                return static_cast<double>(parsed.value.int_value);
              }
            }
          }
        }
      }
      return StatusError(StatusCode::kInvalidArgument,
                         "EUCLIDEAN_DISTANCE requires numeric array elements");
    };
    double total = 0.0;
    for (size_t i = 0; i < a.size(); ++i) {
      ASSIGN_OR_RETURN(double, ca, (coordinate(a[i])));
      ASSIGN_OR_RETURN(double, cb, (coordinate(b[i])));
      const double diff = ca - cb;
      total += diff * diff;
    }
    return Value(std::sqrt(total));
  }

  if (name_view == "element") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ELEMENT requires 1 argument");
    }
    const Value& array = arguments[0];
    if (array.IsNull()) {
      return Value();
    }
    if (!array.IsArray()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ELEMENT requires an ARRAY argument");
    }
    if (array.ArrayElements().empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ELEMENT requires a non-empty array");
    }
    if (array.ArrayElements().size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ELEMENT requires exactly one array element");
    }
    return array.ArrayElements().front();
  }

  return StatusError(StatusCode::kInvalidArgument,
                     "unsupported function " + name);
}

}  // namespace tinylamb::relational_detail
