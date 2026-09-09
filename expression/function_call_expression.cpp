/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "expression/function_call_expression.hpp"

// NOLINTNEXTLINE(modernize-deprecated-headers) POSIX gmtime_r/timegm
#include <time.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <iomanip>
#include <limits>
#include <optional>
#include <ostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/digest.hpp"
#include "expression/constant_value.hpp"
#include "expression/evaluation_context.hpp"
#include "expression/expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/proto_text.hpp"
#include "expression/rewrite.hpp"
#include "expression/sql_udf.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
#include "type/interval.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

bool IdentifierEquals(std::string_view left, std::string_view right);

struct CivilTime {
  int year{1970};
  int month{1};
  int day{1};
  int hour{0};
  int minute{0};
  int second{0};
  int64_t subsecond_nanos{0};
};

bool ParseCivilTime(std::string_view s, CivilTime* ct) {
  if (s.empty()) {
    return false;
  }
  const std::string input(s);
  int Y = 0, M = 0, D = 0, h = 0, m = 0, sec = 0;
  bool matched = false;
  // Conversion errors are tolerated: unparsed fields fall through to the
  // alternative formats below and remain zero-initialized on total failure.
  // NOLINTNEXTLINE(cert-err34-c)
  if (sscanf(input.c_str(), "%d-%d-%d %d:%d:%d", &Y, &M, &D, &h, &m, &sec) >=
          3 ||
      // NOLINTNEXTLINE(cert-err34-c)
      sscanf(input.c_str(), "%d-%d-%dT%d:%d:%d", &Y, &M, &D, &h, &m, &sec) >=
          3) {
    ct->year = Y;
    ct->month = M;
    ct->day = D;
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
  // NOLINTNEXTLINE(cert-err34-c)
  else if (sscanf(input.c_str(), "%d:%d:%d", &h, &m, &sec) >= 3) {
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
    if (ct->second == 60) {
      ct->second = 0;
      ct->subsecond_nanos = 0;
      ct->minute += 1;
      ct->hour += ct->minute / 60;
      ct->minute %= 60;
      if (ct->hour >= 24) {
        int extra_days = ct->hour / 24;
        ct->hour %= 24;
        std::chrono::year_month_day ymd{
            std::chrono::year{ct->year},
            std::chrono::month{static_cast<unsigned>(ct->month)},
            std::chrono::day{static_cast<unsigned>(ct->day)}};
        int64_t days =
            std::chrono::sys_days{ymd}.time_since_epoch().count() + extra_days;
        std::chrono::sys_days new_sd{std::chrono::days{days}};
        std::chrono::year_month_day new_ymd{new_sd};
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

int ParseTimeZoneOffset(std::string_view tz_str, const CivilTime* ct = nullptr,
                        int default_offset = 0) {
  if (tz_str.empty()) {
    return default_offset;
  }
  if (tz_str == "UTC" || tz_str == "GMT" || tz_str == "utc" ||
      tz_str == "gmt" || tz_str == "Z" || tz_str == "z" ||
      tz_str == "Etc/Greenwich" || tz_str == "Etc/UTC" || tz_str == "Etc/GMT") {
    return 0;
  }
  if (tz_str.starts_with("UTC+") || tz_str.starts_with("UTC-") ||
      tz_str.starts_with("GMT+") || tz_str.starts_with("GMT-")) {
    char sign = tz_str[3];
    int h = 0, m = 0;
    std::string rem(tz_str.substr(4));
    // Unparsed tokens leave h/m at 0 (UTC); conversion errors are tolerated
    // by design for malformed zone strings.
    if (rem.find(':') != std::string::npos) {
      // NOLINTNEXTLINE(cert-err33-c, cert-err34-c)
      sscanf(rem.c_str(), "%d:%d", &h, &m);
    } else if (rem.size() == 4) {
      // NOLINTNEXTLINE(cert-err33-c, cert-err34-c)
      sscanf(rem.c_str(), "%2d%2d", &h, &m);
    } else {
      // NOLINTNEXTLINE(cert-err33-c, cert-err34-c)
      sscanf(rem.c_str(), "%d", &h);
    }
    return (h * 3600 + m * 60) * (sign == '-' ? -1 : 1);
  }
  if (tz_str[0] == '+' || tz_str[0] == '-') {
    char sign = tz_str[0];
    int h = 0, m = 0;
    std::string rem(tz_str.substr(1));
    // Unparsed tokens leave h/m at 0 (UTC); conversion errors are tolerated
    // by design for malformed zone strings.
    if (rem.find(':') != std::string::npos) {
      // NOLINTNEXTLINE(cert-err33-c, cert-err34-c)
      sscanf(rem.c_str(), "%d:%d", &h, &m);
    } else if (rem.size() == 4) {
      // NOLINTNEXTLINE(cert-err33-c, cert-err34-c)
      sscanf(rem.c_str(), "%2d%2d", &h, &m);
    } else {
      // NOLINTNEXTLINE(cert-err33-c, cert-err34-c)
      sscanf(rem.c_str(), "%d", &h);
    }
    return (h * 3600 + m * 60) * (sign == '-' ? -1 : 1);
  }
  std::string zone_name(tz_str);
  if (zone_name == "NZ-CHAT") {
    zone_name = "Pacific/Chatham";
  }
  try {
    const auto* zone = std::chrono::locate_zone(zone_name);
    if (zone != nullptr) {
      int y = (ct != nullptr) ? ct->year : 2000;
      int mon = (ct != nullptr) ? ct->month : 1;
      int d = (ct != nullptr) ? ct->day : 1;
      int h = (ct != nullptr) ? ct->hour : 0;
      int min = (ct != nullptr) ? ct->minute : 0;
      int s = (ct != nullptr) ? ct->second : 0;
      y = std::max(y, 1970);
      std::chrono::year_month_day ymd{
          std::chrono::year{y}, std::chrono::month{static_cast<unsigned>(mon)},
          std::chrono::day{static_cast<unsigned>(d)}};
      std::chrono::local_days loc_d{ymd};
      auto loc_tp = loc_d + std::chrono::hours{h} + std::chrono::minutes{min} +
                    std::chrono::seconds{s};
      auto loc_info = zone->get_info(loc_tp);
      return static_cast<int>(loc_info.first.offset.count());
    }
  } catch (...) {
    return default_offset;
  }
  return default_offset;
}

CivilTime ValueToCivilTime(const Value& val) {
  CivilTime ct;
  if (val.type == ValueType::kDate) {
    std::chrono::sys_days sys_d{std::chrono::days{val.DateDays()}};
    std::chrono::year_month_day ymd{sys_d};
    ct.year = int(ymd.year());
    ct.month = static_cast<int>(static_cast<unsigned>(ymd.month()));
    ct.day = static_cast<int>(static_cast<unsigned>(ymd.day()));
    return ct;
  }
  if (val.type == ValueType::kVarChar) {
    std::string_view s(val.value.varchar_value);
    ParseCivilTime(s, &ct);
    return ct;
  }
  return ct;
}

std::string FormatCivilTime(const CivilTime& ct) {
  std::array<char, 64> buf{};
  if (ct.subsecond_nanos != 0) {
    if (ct.subsecond_nanos % 1000000 == 0) {
      // Formatting into a fixed buffer cannot fail.
      // NOLINTNEXTLINE(cert-err33-c)
      snprintf(buf.data(), buf.size(), "%04d-%02d-%02d %02d:%02d:%02d.%03ld",
               ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second,
               ct.subsecond_nanos / 1000000);
    } else if (ct.subsecond_nanos % 1000 == 0) {
      // NOLINTNEXTLINE(cert-err33-c)
      snprintf(buf.data(), buf.size(), "%04d-%02d-%02d %02d:%02d:%02d.%06ld",
               ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second,
               ct.subsecond_nanos / 1000);
    } else {
      // NOLINTNEXTLINE(cert-err33-c)
      snprintf(buf.data(), buf.size(), "%04d-%02d-%02d %02d:%02d:%02d.%09ld",
               ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second,
               ct.subsecond_nanos);
    }
  } else {
    // NOLINTNEXTLINE(cert-err33-c)
    snprintf(buf.data(), buf.size(), "%04d-%02d-%02d %02d:%02d:%02d", ct.year,
             ct.month, ct.day, ct.hour, ct.minute, ct.second);
  }
  return std::string{buf.data()};
}

std::string FormatTimeZoneOffset(int tz_offset_sec) {
  std::array<char, 16> buf{};
  int abs_sec = std::abs(tz_offset_sec);
  int h = abs_sec / 3600;
  int m = (abs_sec % 3600) / 60;
  char sign = tz_offset_sec < 0 ? '-' : '+';
  if (m == 0) {
    // NOLINTNEXTLINE(cert-err33-c)
    snprintf(buf.data(), buf.size(), "%c%02d", sign, h);
  } else {
    // NOLINTNEXTLINE(cert-err33-c)
    snprintf(buf.data(), buf.size(), "%c%02d:%02d", sign, h, m);
  }
  return std::string{buf.data()};
}

StatusOr<Value> AddOrSubInterval(const std::string& func_name,
                                 const Value& date,
                                 const IntervalExpression& interval) {
  const int64_t amount =
      func_name == "date_sub" ? -interval.Amount() : interval.Amount();
  ASSIGN_OR_RETURN(int64_t, days,
                   date.type == ValueType::kDate
                       ? StatusOr<int64_t>(date.DateDays())
                       : TryParseDateDays(date.value.varchar_value));
  ASSIGN_OR_RETURN(int64_t, result,
                   TryAddDateIntervalDays(days, amount, interval.Unit()));
  if (date.type == ValueType::kDate) {
    return Value::DateFromDays(result);
  }
  ASSIGN_OR_RETURN(std::string, text, TryFormatDateDays(result));
  return Value(std::move(text));
}

// Decodes one proto-text scalar token (`5`, `1.5`, `true`, `"str"`).
StatusOr<Value> TryProtoTextScalar(std::string_view raw) {
  while (!raw.empty() &&
         (std::isspace(static_cast<unsigned char>(raw.front())) != 0)) {
    raw.remove_prefix(1);
  }
  while (!raw.empty() &&
         (std::isspace(static_cast<unsigned char>(raw.back())) != 0)) {
    raw.remove_suffix(1);
  }
  if (raw.empty() || raw == "null") {
    return Value();
  }
  if (raw == "true") {
    return Value(int64_t{1});
  }
  if (raw == "false") {
    return Value(int64_t{0});
  }
  if (raw.size() >= 2 && raw.front() == '"' && raw.back() == '"') {
    return Value(std::string(raw.substr(1, raw.size() - 2)));
  }
  std::string token(raw);
  try {
    return Value(static_cast<int64_t>(std::stoll(token)));
  } catch (const std::exception& error) {
    (void)error;
  }
  try {
    return Value(std::stod(token));
  } catch (...) {
    return Value(std::move(token));
  }
}

// Minimal proto text-format field extraction: repeated `field: value`
// entries and `field { ... }` message blocks; multiple matches become an
// array.  Mirrors the interpreter-side extractor for plan-executor use.
StatusOr<bool> ProtoTextExtractFieldShim(std::string_view text,
                                         std::string_view key, Value* out) {
  // Proto presence fields (`has_xxx`) report whether `xxx` occurs.
  if (key.size() > 4 && key.starts_with("has_")) {
    Value probe;
    ASSIGN_OR_RETURN(bool, present,
                     ProtoTextExtractFieldShim(text, key.substr(4), &probe));
    if (!present) {
      *out = Value(int64_t{0});
      return true;
    }
    *out = Value(int64_t{1});
    return true;
  }
  std::vector<Value> matches;
  size_t i = 0;
  while (i < text.size()) {
    while (i < text.size() &&
           (std::isspace(static_cast<unsigned char>(text[i])) != 0)) {
      ++i;
    }
    if (i >= text.size()) {
      break;
    }
    if (text[i] == '#') {
      // Comment token: skip through end of line.
      while (i < text.size() && text[i] != '\n') {
        ++i;
      }
      continue;
    }
    if (text[i] == '{' || text[i] == '}') {
      ++i;
      continue;
    }
    const size_t name_start = i;
    while (i < text.size() && text[i] != ':' && text[i] != '{' &&
           (std::isspace(static_cast<unsigned char>(text[i])) == 0)) {
      ++i;
    }
    const std::string_view field_name = text.substr(name_start, i - name_start);
    while (i < text.size() &&
           (std::isspace(static_cast<unsigned char>(text[i])) != 0)) {
      ++i;
    }
    if (i < text.size() && text[i] == '{') {
      int nest = 1;
      bool str = false;
      size_t j = i + 1;
      for (; j < text.size(); ++j) {
        const char c = text[j];
        if (str) {
          if (c == '\\' && j + 1 < text.size()) {
            ++j;
          } else if (c == '"') {
            str = false;
          }
          continue;
        }
        if (c == '"') {
          str = true;
        } else if (c == '{') {
          ++nest;
        } else if (c == '}') {
          if (--nest == 0) {
            break;
          }
        }
      }
      std::string_view body = text.substr(i + 1, j > i + 1 ? j - i - 1 : 0);
      while (!body.empty() &&
             (std::isspace(static_cast<unsigned char>(body.front())) != 0)) {
        body.remove_prefix(1);
      }
      while (!body.empty() &&
             (std::isspace(static_cast<unsigned char>(body.back())) != 0)) {
        body.remove_suffix(1);
      }
      i = j < text.size() ? j + 1 : text.size();
      if (field_name.size() == key.size() &&
          std::equal(key.begin(), key.end(), field_name.begin(),
                     [](char a, char b) {
                       return std::tolower(static_cast<unsigned char>(a)) ==
                              std::tolower(static_cast<unsigned char>(b));
                     })) {
        matches.emplace_back(std::string(body));
      }
      continue;
    }
    if (i >= text.size() || text[i] != ':') {
      break;
    }
    ++i;
    while (i < text.size() &&
           (std::isspace(static_cast<unsigned char>(text[i])) != 0)) {
      ++i;
    }
    const size_t value_begin = i;
    size_t value_end = 0;
    if (i < text.size() && text[i] == '"') {
      ++i;
      while (i < text.size() && text[i] != '"') {
        if (text[i] == '\\' && i + 1 < text.size()) {
          ++i;
        }
        ++i;
      }
      value_end = std::min(text.size(), i + 1);
      i = value_end;
    } else {
      while (i < text.size() &&
             (std::isspace(static_cast<unsigned char>(text[i])) == 0)) {
        ++i;
      }
      value_end = i;
    }
    if (field_name.size() == key.size() &&
        std::equal(key.begin(), key.end(), field_name.begin(),
                   [](char a, char b) {
                     return std::tolower(static_cast<unsigned char>(a)) ==
                            std::tolower(static_cast<unsigned char>(b));
                   })) {
      ASSIGN_OR_RETURN(Value, field_value,
                       TryProtoTextScalar(
                           text.substr(value_begin, value_end - value_begin)));
      matches.push_back(std::move(field_value));
    }
  }
  if (matches.empty()) {
    return false;
  }
  if (matches.size() == 1) {
    *out = std::move(matches[0]);
  } else {
    *out = Value::Array(std::move(matches), "INT64");
  }
  return true;
}

// FORMAT(fmt, ...): GoogleSQL printf-style rendering.  Ported from the
// scope-based relational evaluator (executor/detail/expression_eval.cpp) so
// the AST ground truth stays semantically identical: NULL propagates from the
// format string, any value argument, or a dynamic width/precision argument;
// %t/%T render values in STRING form (strings quoted); %d/%i/%u/%x/%X/%o
// take integers; %f/%g/%e/%E take numerics.  `%*` / `%.*` consume
// width/precision from the argument list.
StatusOr<Value> FormatFunction(const std::string& name,
                               const std::vector<Value>& arguments) {
  (void)name;
  auto raw_str = [](const Value& val) -> std::string {
    if (val.type == ValueType::kVarChar) {
      return std::string(val.value.varchar_value);
    }
    return val.AsString();
  };
  if (arguments.empty()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "FORMAT requires at least 1 argument");
  }
  if (arguments[0].IsNull()) {
    return Value();
  }
  // FORMAT propagates NULL from a value or a dynamic width/precision
  // argument; rendering it as the literal text "NULL" changes the result
  // from SQL NULL to a non-null STRING.
  for (size_t i = 1; i < arguments.size(); ++i) {
    if (arguments[i].IsNull()) {
      return Value();
    }
  }
  const std::string fmt = raw_str(arguments[0]);
  std::string result;
  size_t arg_idx = 1;
  for (size_t i = 0; i < fmt.size(); ++i) {
    if (fmt[i] == '%' && i + 1 < fmt.size()) {
      ++i;
      if (fmt[i] == '%') {
        result.push_back('%');
        continue;
      }
      bool hash_flag = false;
      bool zero_pad = false;
      if (fmt[i] == '#') {
        hash_flag = true;
        ++i;
      }
      if (i < fmt.size() && fmt[i] == '0') {
        zero_pad = true;
        ++i;
      }
      int width = 0;
      if (i < fmt.size() && fmt[i] == '*') {
        if (arg_idx >= arguments.size()) {
          return StatusError(StatusCode::kInvalidArgument,
                             "FORMAT: not enough arguments for format string");
        }
        const Value& width_arg = arguments[arg_idx++];
        if (width_arg.IsNull()) {
          return Value();
        }
        if (width_arg.type != ValueType::kInt64) {
          return StatusError(StatusCode::kInvalidArgument,
                             "FORMAT: dynamic width must be an integer");
        }
        width = static_cast<int>(width_arg.value.int_value);
        ++i;
      }
      while (i < fmt.size() &&
             (std::isdigit(static_cast<unsigned char>(fmt[i])) != 0)) {
        width = (width * 10) + (fmt[i] - '0');
        ++i;
      }
      int precision = -1;
      if (i < fmt.size() && fmt[i] == '.') {
        ++i;
        if (i < fmt.size() && fmt[i] == '*') {
          if (arg_idx >= arguments.size()) {
            return StatusError(
                StatusCode::kInvalidArgument,
                "FORMAT: not enough arguments for format string");
          }
          const Value& precision_arg = arguments[arg_idx++];
          if (precision_arg.IsNull()) {
            return Value();
          }
          if (precision_arg.type != ValueType::kInt64) {
            return StatusError(StatusCode::kInvalidArgument,
                               "FORMAT: dynamic precision must be an integer");
          }
          precision = static_cast<int>(precision_arg.value.int_value);
          ++i;
        } else {
          precision = 0;
        }
        while (i < fmt.size() &&
               (std::isdigit(static_cast<unsigned char>(fmt[i])) != 0)) {
          precision = (precision * 10) + (fmt[i] - '0');
          ++i;
        }
      }
      if (i >= fmt.size()) {
        break;
      }
      char spec = fmt[i];

      if (arg_idx >= arguments.size()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "FORMAT: not enough arguments for format string");
      }
      const Value& arg = arguments[arg_idx++];
      std::string formatted_item;
      if (spec == 'T' || spec == 't') {
        if (arg.IsNull()) {
          formatted_item = "NULL";
        } else if (arg.type == ValueType::kVarChar) {
          std::string s = raw_str(arg);
          formatted_item += "\"";
          for (char c : s) {
            if (c == '"') {
              formatted_item += "\\\"";
            } else if (c == '\\') {
              formatted_item += "\\\\";
            } else if (c == '\n') {
              formatted_item += "\\n";
            } else if (c == '\r') {
              formatted_item += "\\r";
            } else if (c == '\t') {
              formatted_item += "\\t";
            } else {
              formatted_item.push_back(c);
            }
          }
          formatted_item += "\"";
        } else if (arg.type == ValueType::kInt64) {
          formatted_item = std::to_string(arg.value.int_value);
        } else if (arg.type == ValueType::kDouble) {
          formatted_item = std::to_string(arg.value.double_value);
        } else {
          formatted_item = arg.AsString();
        }
      } else if (spec == 's') {
        if (arg.IsNull()) {
          formatted_item = "NULL";
        } else {
          formatted_item = raw_str(arg);
        }
      } else if (spec == 'd' || spec == 'i' || spec == 'u' || spec == 'x' ||
                 spec == 'X' || spec == 'o') {
        if (arg.IsNull()) {
          formatted_item = "NULL";
        } else if (arg.type == ValueType::kInt64) {
          if (spec == 'x') {
            std::array<char, 32> buf{};
            // NOLINTNEXTLINE(cert-err33-c)
            snprintf(buf.data(), buf.size(), hash_flag ? "0x%lx" : "%lx",
                     static_cast<unsigned long>(arg.value.int_value));
            formatted_item = buf.data();
          } else if (spec == 'X') {
            std::array<char, 32> buf{};
            // NOLINTNEXTLINE(cert-err33-c)
            snprintf(buf.data(), buf.size(), hash_flag ? "0X%lX" : "%lX",
                     static_cast<unsigned long>(arg.value.int_value));
            formatted_item = buf.data();
          } else if (spec == 'o') {
            std::array<char, 32> buf{};
            // NOLINTNEXTLINE(cert-err33-c)
            snprintf(buf.data(), buf.size(), hash_flag ? "0%lo" : "%lo",
                     static_cast<unsigned long>(arg.value.int_value));
            formatted_item = buf.data();
          } else {
            formatted_item = std::to_string(arg.value.int_value);
          }
        } else if (arg.type == ValueType::kDouble) {
          formatted_item =
              std::to_string(static_cast<int64_t>(arg.value.double_value));
        } else {
          return StatusError(
              StatusCode::kInvalidArgument,
              "FORMAT: invalid argument type for integer specifier");
        }
      } else if (spec == 'f' || spec == 'g' || spec == 'e' || spec == 'E') {
        if (arg.IsNull()) {
          formatted_item = "NULL";
        } else if (arg.type == ValueType::kDouble) {
          formatted_item = std::to_string(arg.value.double_value);
        } else if (arg.type == ValueType::kInt64) {
          formatted_item =
              std::to_string(static_cast<double>(arg.value.int_value));
        } else {
          return StatusError(
              StatusCode::kInvalidArgument,
              "FORMAT: invalid argument type for float specifier");
        }
      } else {
        result.push_back('%');
        result.push_back(spec);
        continue;
      }

      if (precision >= 0 && (spec == 't' || spec == 'T' || spec == 's')) {
        if (formatted_item.size() > static_cast<size_t>(precision)) {
          formatted_item =
              formatted_item.substr(0, static_cast<size_t>(precision));
        }
      }
      if (precision >= 0 &&
          (spec == 'd' || spec == 'i' || spec == 'u' || spec == 'x' ||
           spec == 'X' || spec == 'o') &&
          formatted_item != "NULL" &&
          formatted_item.size() < static_cast<size_t>(precision)) {
        formatted_item.insert(
            0, static_cast<size_t>(precision) - formatted_item.size(), '0');
      }
      if (width > 0 && formatted_item.size() < static_cast<size_t>(width)) {
        const size_t pad_len =
            static_cast<size_t>(width) - formatted_item.size();
        char pad_char = zero_pad ? '0' : ' ';
        formatted_item.insert(0, pad_len, pad_char);
      }
      result += formatted_item;
    } else {
      result.push_back(fmt[i]);
    }
  }
  if (arg_idx < arguments.size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "FORMAT: too many arguments for format string");
  }
  return Value(std::move(result));
}

StatusOr<Value> ExecuteFunction(const std::string& name,
                                const std::vector<Value>& values) {
  auto raw_str = [](const Value& val) -> std::string {
    if (val.type == ValueType::kVarChar) {
      return std::string(val.value.varchar_value);
    }
    return val.AsString();
  };
  // Proto-field guards emitted by the GoogleSQL frontend: NEW constructors
  // and SELECT AS <proto> route non-constant repeated-field arrays and enum
  // values through them so invalid data fails execution instead of being
  // silently dropped from the text-format representation.
  if (name == "$proto_repeated_guard") {
    if (values.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "$proto_repeated_guard requires 2 arguments");
    }
    if (values[0].IsArray()) {
      for (const Value& element : values[0].ArrayElements()) {
        if (element.IsNull()) {
          return StatusError(
              StatusCode::kInvalidArgument,
              "Cannot encode a null value in a repeated protocol message "
              "field");
        }
      }
    } else if (!values[0].IsNull()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "repeated proto field requires an array");
    }
    return values[1];
  }
  if (name == "$proto_field_guard" || name == "$proto_enum_guard") {
    const size_t expected = name == "$proto_field_guard" ? 3 : 2;
    if (values.size() != expected) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " argument count mismatch");
    }
    if (!values[0].IsNull()) {
      Row dummy_row;
      Schema dummy_schema;
      Expression checked = CastExpressionExp(ConstantValueExp(values[0]),
                                             raw_str(values[1]), false);
      // Full CAST validation against the enum registry; raises on unknown
      // members or out-of-range ordinals.
      RETURN_IF_FAIL(checked->TryEvaluate(dummy_row, dummy_schema).GetStatus());
    }
    return expected == 3 ? values[2] : values[0];
  }
  if (name == "__pipe_concat") {
    if (values.size() != 2 || !values[0].IsArray()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__pipe_concat requires an array and separator");
    }
    const std::string separator = raw_str(values[1]);
    struct Pair {
      std::string a;
      std::string b;
    };
    std::vector<Pair> pairs;
    for (const Value& element : values[0].ArrayElements()) {
      if (element.IsNull()) {
        continue;
      }
      const std::string text = raw_str(element);
      Value a;
      Value b;
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
    std::string result;
    for (const Pair& pair : pairs) {
      if (!result.empty()) {
        result += separator;
      }
      result += pair.a + "," + pair.b;
    }
    return Value(std::move(result));
  }
  if (name == "__struct_set") {
    if (values.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__struct_set requires 3 arguments");
    }
    return TryStructSetField(values[0], raw_str(values[1]), values[2]);
  }
  if (name == "get_field") {
    if (values.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "get_field requires 2 arguments");
    }
    if (values[0].IsNull()) {
      return Value();
    }
    const std::string object = raw_str(values[0]);
    const std::string field = raw_str(values[1]);
    // Proto TEXT payloads resolve through the shared extractor (defaults,
    // has_ bits, repeated arrays) instead of the JSON member scan.
    Value proto_value;
    auto proto_hit = TryReadProtoTextField(object, field, &proto_value);
    if (!proto_hit.HasValue()) {
      return proto_hit.GetStatus();
    }
    if (proto_hit.Value()) {
      return proto_value;
    }
    if (object.size() < 2 || object.front() != '{' || object.back() != '}') {
      return StatusError(StatusCode::kInvalidArgument,
                         "get_field requires a STRUCT");
    }
    for (const auto& [key, text] :
         SplitJsonObjectMembers(object.substr(1, object.size() - 2))) {
      if (IdentifierEquals(key, field)) {
        Value parsed;
        if (!JsonTextToValue(text, &parsed)) {
          return StatusError(StatusCode::kInvalidArgument,
                             "get_field: malformed member value");
        }
        return parsed;
      }
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "field not found: " + field);
  }
  if (name == "__get_field_safe") {
    // Field access that tolerates NULL bases and missing members by
    // returning NULL; used for dotted references in DML predicates.
    if (values.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__get_field_safe requires 2 arguments");
    }
    if (values[0].IsNull()) {
      return Value();
    }
    const std::string object = raw_str(values[0]);
    const std::string field = raw_str(values[1]);
    // Proto field reads need scalar defaults even when an empty proto is
    // represented by an empty text payload.
    Value proto_field;
    auto proto_field_hit = TryReadProtoTextField(object, field, &proto_field);
    if (!proto_field_hit.HasValue()) {
      return proto_field_hit.GetStatus();
    }
    if (proto_field_hit.Value()) {
      return proto_field;
    }
    if (object.size() >= 2 && object.front() == '{' && object.back() == '}') {
      const auto members =
          SplitJsonObjectMembers(object.substr(1, object.size() - 2));
      for (const auto& [key, text] : members) {
        if (IdentifierEquals(key, field)) {
          Value parsed;
          if (!JsonTextToValue(text, &parsed)) {
            return Value();
          }
          return parsed;
        }
      }
      // Anonymous struct members (`STRUCT(2)` stores {"f1":2}) are still
      // addressable by any field reference when unambiguous: a single-member
      // object exposes its only value positionally.
      if (members.size() == 1) {
        Value parsed;
        if (!JsonTextToValue(members.front().second, &parsed)) {
          return Value();
        }
        return parsed;
      }
    }
    // Proto text-format cells (`i1: 5 i2: 5`) carry the same field
    // semantics: extract the first (or repeated) occurrence of `field`.
    ASSIGN_OR_RETURN(bool, extracted,
                     ProtoTextExtractFieldShim(object, field, &proto_field));
    if (extracted) {
      return proto_field;
    }
    return Value();
  }

  // NOTE: `__struct_set` / `get_field` are handled by the earlier blocks
  // above, which return on every path; the copies that used to sit here were
  // unreachable merge artifacts and have been removed.
  if (name == "rand") {
    if (!values.empty()) {
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
  if (name == "coalesce") {
    for (const auto& val : values) {
      if (!val.IsNull()) {
        return val;
      }
    }
    return Value();
  }
  // NULLIF / IFNULL / GREATEST / LEAST: the AST ground truth must implement
  // every scalar the scope-based relational evaluator supports
  // (executor/detail/expression_eval.cpp), or a plan-shape change that routes
  // the predicate through this evaluator turns a working query into "not yet
  // executable". Semantics mirror the relational path exactly: NULLIF uses
  // Value::operator== (NULL == NULL is true, cross-type is false, so a NULL
  // first argument still yields NULL); GREATEST/LEAST propagate NULL and
  // promote mixed INT64/DOUBLE numerically.
  if (name == "nullif") {
    if (values.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "NULLIF requires 2 arguments");
    }
    if (values[0] == values[1]) {
      return Value();
    }
    return values[0];
  }
  if (name == "ifnull") {
    if (values.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "IFNULL requires 2 arguments");
    }
    return !values[0].IsNull() ? values[0] : values[1];
  }
  if (name == "greatest" || name == "least") {
    if (values.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires at least 1 argument");
    }
    for (const auto& val : values) {
      if (val.IsNull()) {
        return Value();
      }
    }
    Value best = values[0];
    const auto numeric = [](const Value& val) {
      return val.type == ValueType::kInt64 || val.type == ValueType::kDouble;
    };
    const auto as_double = [](const Value& val) {
      return val.type == ValueType::kDouble
                 ? val.value.double_value
                 : static_cast<double>(val.value.int_value);
    };
    for (size_t i = 1; i < values.size(); ++i) {
      if (numeric(best) && numeric(values[i])) {
        const bool takes = name == "greatest"
                               ? as_double(values[i]) > as_double(best)
                               : as_double(values[i]) < as_double(best);
        if (takes) {
          best = values[i];
        }
      } else if (name == "greatest") {
        if (values[i] > best) {
          best = values[i];
        }
      } else if (values[i] < best) {
        best = values[i];
      }
    }
    return best;
  }
  if (name == "concat") {
    std::string result;
    for (const auto& value : values) {
      if (value.IsNull()) {
        return Value();
      }
      if (value.type != ValueType::kVarChar) {
        return StatusError(StatusCode::kInvalidArgument,
                           "CONCAT currently requires string arguments");
      }
      result.append(value.value.varchar_value);
    }
    return Value(std::move(result));
  }
  if (name == "upper") {
    if (values.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "UPPER requires 1 argument");
    }
    if (values[0].IsNull()) {
      return Value();
    }
    std::string s = raw_str(values[0]);
    for (char& c : s) {
      c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return Value(std::move(s));
  }
  if (name == "lower") {
    if (values.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "LOWER requires 1 argument");
    }
    if (values[0].IsNull()) {
      return Value();
    }
    std::string s = raw_str(values[0]);
    for (char& c : s) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return Value(std::move(s));
  }
  if (name == "abs") {
    if (values.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ABS requires 1 argument");
    }
    if (values[0].IsNull()) {
      return Value();
    }
    if (values[0].type == ValueType::kInt64) {
      // std::abs(INT64_MIN) is UB (wraps to INT64_MIN); the relational
      // evaluator raises instead, so the ground truth must agree.
      if (values[0].value.int_value == std::numeric_limits<int64_t>::min()) {
        return StatusError(StatusCode::kIsInfinity, "integer overflow in ABS");
      }
      return Value(std::abs(values[0].value.int_value));
    }
    if (values[0].type == ValueType::kDouble) {
      return Value(std::abs(values[0].value.double_value));
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "ABS requires numeric argument");
  }
  if (name == "sqrt") {
    if (values.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SQRT requires 1 argument");
    }
    if (values[0].IsNull()) {
      return Value();
    }
    double val = values[0].type == ValueType::kInt64
                     ? static_cast<double>(values[0].value.int_value)
                     : (values[0].type == ValueType::kDouble
                            ? values[0].value.double_value
                            : 0.0);
    return Value(std::sqrt(val));
  }
  if (name == "substr" || name == "substring") {
    if (values.size() < 2 || values.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SUBSTR requires two or three arguments");
    }
    if (values[0].IsNull() || values[1].IsNull() ||
        (values.size() == 3 && values[2].IsNull())) {
      return Value();
    }
    if (values[0].type != ValueType::kVarChar ||
        values[1].type != ValueType::kInt64 ||
        (values.size() == 3 && values[2].type != ValueType::kInt64)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SUBSTR argument type mismatch");
    }
    const std::string input(values[0].value.varchar_value);
    const int64_t start = values[1].value.int_value;
    if (values.size() == 3 && values[2].type == ValueType::kInt64) {
      if (values[2].value.int_value < 0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "SUBSTR length cannot be negative");
      }
      if (values[2].value.int_value == 0) {
        return Value(std::string());
      }
    }
    // GoogleSQL semantics: a negative start counts back from the end of the
    // string; start == 0 behaves like start == 1. When the computed start
    // lands before the first byte, the result starts at the first byte
    // (SUBSTR('abcde', -10) == 'abcde'), matching SUBSTR('abcde', 0).
    const size_t size = input.size();
    size_t begin = 0;
    if (start < 0) {
      const uint64_t back = static_cast<uint64_t>(-(start + 1)) + 1;
      begin = back >= size ? 0 : size - static_cast<size_t>(back);
    } else {
      begin = start <= 1 ? 0 : static_cast<size_t>(start - 1);
    }
    const size_t length = values.size() == 3
                              ? static_cast<size_t>(values[2].value.int_value)
                              : std::string::npos;

    if (begin >= size) {
      return Value(std::string());
    }
    return Value(input.substr(begin, length));
  }
  if (name == "length" || name == "char_length" || name == "character_length" ||
      name == "octet_length" || name == "byte_length") {
    if (values.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 argument");
    }
    if (values[0].IsNull()) {
      return Value();
    }
    const std::string input = raw_str(values[0]);
    if (name == "char_length" || name == "character_length") {
      // SQL CHAR_LENGTH counts CHARACTER CODE POINTS; the relational
      // evaluator (expression_eval.cpp utf8_len) and the compliance golden
      // (CHAR_LENGTH("€") = 1) already do.  Byte-counting here made the
      // result depend on which evaluator the plan happened to pick.
      size_t code_points = 0;
      for (char i : input) {
        const auto byte = static_cast<unsigned char>(i);
        if ((byte & 0xC0) != 0x80) {  // skip UTF-8 continuation bytes
          ++code_points;
        }
      }
      return Value(static_cast<int64_t>(code_points));
    }
    // length / octet_length / byte_length stay byte-oriented.
    return Value(static_cast<int64_t>(input.size()));
  }
  if (name == "instr" || name == "strpos") {
    if (values.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 2 arguments");
    }
    if (values[0].IsNull() || values[1].IsNull()) {
      return Value();
    }
    const std::string hay = raw_str(values[0]);
    const std::string needle = raw_str(values[1]);
    const size_t pos = hay.find(needle);
    if (pos == std::string::npos) {
      return Value(int64_t{0});
    }
    return Value(static_cast<int64_t>(pos + 1));
  }
  if (name == "lpad" || name == "rpad") {
    if (values.size() < 2 || values.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 2 or 3 arguments");
    }
    if (values[0].IsNull() || values[1].IsNull() ||
        (values.size() == 3 && values[2].IsNull())) {
      return Value();
    }
    const std::string input = raw_str(values[0]);
    int64_t target_len = values[1].type == ValueType::kInt64
                             ? values[1].value.int_value
                             : std::stoll(raw_str(values[1]));
    if (target_len < 0) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " target length cannot be negative");
    }
    const auto target_size = static_cast<size_t>(target_len);
    if (target_size == 0) {
      return Value(std::string());
    }
    if (input.size() >= target_size) {
      return Value(input.substr(0, target_size));
    }
    const std::string pad = values.size() == 3 ? raw_str(values[2]) : " ";
    if (pad.empty()) {
      return Value(input.substr(0, target_size));
    }
    const size_t pad_needed = target_size - input.size();
    std::string padding;
    padding.reserve(pad_needed + pad.size());
    while (padding.size() < pad_needed) {
      padding.append(pad);
    }
    padding.resize(pad_needed);
    if (name == "lpad") {
      return Value(padding + input);
    }
    return Value(input + padding);
  }
  if (name == "extract_year" || name == "extract_month" ||
      name == "extract_day") {
    if (values.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "EXTRACT requires one argument");
    }
    if (values[0].IsNull()) {
      return Value();
    }
    if (values[0].type != ValueType::kDate &&
        values[0].type != ValueType::kVarChar) {
      return StatusError(StatusCode::kInvalidArgument,
                         "EXTRACT requires DATE or STRING");
    }
    const std::string date = values[0].type == ValueType::kDate
                                 ? values[0].AsString()
                                 : std::string(values[0].value.varchar_value);
    if (date.size() < 10) {
      return StatusError(StatusCode::kInvalidArgument, "invalid DATE value");
    }
    int64_t part = 0;
    try {
      if (name == "extract_year") {
        part = std::stoll(date.substr(0, 4));
      } else if (name == "extract_month") {
        part = std::stoll(date.substr(5, 2));
      } else {
        part = std::stoll(date.substr(8, 2));
      }
    } catch (const std::logic_error&) {
      return StatusError(StatusCode::kInvalidArgument,
                         "invalid DATE value: " + date);
    }
    return Value(part);
  }
  if (name == "current_timestamp") {
    if (!values.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CURRENT_TIMESTAMP takes no arguments");
    }
    const std::time_t now =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::tm utc{};
    gmtime_r(&now, &utc);
    std::ostringstream output;
    output << std::put_time(&utc, "%Y-%m-%d %H:%M:%S");
    return Value(output.str());
  }
  if (name == "current_datetime") {
    if (values.size() > 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CURRENT_DATETIME takes at most 1 argument");
    }
    if (values.size() == 1 && values[0].IsNull()) {
      return Value();
    }
    int tz_offset_sec = ParseTimeZoneOffset(GetDefaultTimeZone());
    if (values.size() == 1 && !values[0].IsNull()) {
      std::string tz_str = raw_str(values[0]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        return StatusError(StatusCode::kInvalidArgument,
                           "invalid timezone: " + tz_str);
      }
      tz_offset_sec = ParseTimeZoneOffset(tz_str);
    }
    time_t now = time(nullptr) + tz_offset_sec;
    struct tm t = {};
    gmtime_r(&now, &t);
    CivilTime current{.year = t.tm_year + 1900,
                      .month = t.tm_mon + 1,
                      .day = t.tm_mday,
                      .hour = t.tm_hour,
                      .minute = t.tm_min,
                      .second = t.tm_sec,
                      .subsecond_nanos = 0};
    return Value(FormatCivilTime(current));
  }
  if (name == "current_date") {
    if (values.size() > 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CURRENT_DATE takes at most 1 argument");
    }
    if (values.size() == 1 && values[0].IsNull()) {
      return Value();
    }
    int tz_offset_sec = ParseTimeZoneOffset(GetDefaultTimeZone());
    if (values.size() == 1 && !values[0].IsNull()) {
      std::string tz_str = raw_str(values[0]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        return StatusError(StatusCode::kInvalidArgument,
                           "invalid timezone: " + tz_str);
      }
      tz_offset_sec = ParseTimeZoneOffset(tz_str);
    }
    time_t now = time(nullptr) + tz_offset_sec;
    struct tm t = {};
    gmtime_r(&now, &t);
    std::chrono::year_month_day ymd{
        std::chrono::year{t.tm_year + 1900},
        std::chrono::month{static_cast<unsigned>(t.tm_mon + 1)},
        std::chrono::day{static_cast<unsigned>(t.tm_mday)}};
    return Value::DateFromDays(
        std::chrono::sys_days{ymd}.time_since_epoch().count());
  }
  if (name == "string") {
    if (values.empty() || values.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "STRING requires 1 or 2 arguments");
    }
    if (values[0].IsNull() || (values.size() == 2 && values[1].IsNull())) {
      return Value();
    }
    if (values.size() == 2) {
      CivilTime ct = ValueToCivilTime(values[0]);
      std::string tz_str = raw_str(values[1]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        return StatusError(StatusCode::kInvalidArgument,
                           "invalid timezone: " + tz_str);
      }
      int tz_offset_sec = ParseTimeZoneOffset(tz_str, &ct);
      ct = ShiftCivilTimeHours(ct, tz_offset_sec / 3600);
      int rem_mins = (tz_offset_sec % 3600) / 60;
      if (rem_mins != 0) {
        int total_m = ct.minute + rem_mins;
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
      return Value(FormatCivilTime(ct) + FormatTimeZoneOffset(tz_offset_sec));
    }
    if (values[0].type == ValueType::kDate) {
      return Value(FormatDateDays(values[0].DateDays()));
    }
    return Value(raw_str(values[0]));
  }
  if (name == "format_timestamp" || name == "format_datetime" ||
      name == "format_date") {
    if (values.size() < 2 || values.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " takes 2 or 3 arguments");
    }
    if (values[0].IsNull() || values[1].IsNull()) {
      return Value();
    }
    std::string fmt = raw_str(values[0]);
    CivilTime ct = ValueToCivilTime(values[1]);
    int tz_offset_sec = 0;
    if (values.size() == 3) {
      if (values[2].IsNull()) {
        return Value();
      }
      std::string tz_str = raw_str(values[2]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        return StatusError(StatusCode::kInvalidArgument,
                           "invalid timezone: " + tz_str);
      }
      tz_offset_sec = ParseTimeZoneOffset(tz_str, &ct);
    } else if (name == "format_datetime" || name == "format_timestamp") {
      tz_offset_sec = ParseTimeZoneOffset(GetDefaultTimeZone(), &ct, -8 * 3600);
    }
    ct = ShiftCivilTimeHours(ct, tz_offset_sec / 3600);
    int rem_mins = (tz_offset_sec % 3600) / 60;
    if (rem_mins != 0) {
      int total_m = ct.minute + rem_mins;
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
    struct tm tm = {};
    tm.tm_year = ct.year - 1900;
    tm.tm_mon = ct.month - 1;
    tm.tm_mday = ct.day;
    tm.tm_hour = ct.hour;
    tm.tm_min = ct.minute;
    tm.tm_sec = ct.second;
    timegm(&tm);
    std::array<char, 128> buf{};
    const auto format_time = static_cast<size_t (*)(
        char*, size_t, const char*, const struct tm*) noexcept>(&std::strftime);
    const size_t formatted =
        format_time(buf.data(), buf.size(), fmt.c_str(), &tm);
    if (formatted == 0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TIMESTAMP format produced no output: " + fmt);
    }
    return Value(std::string{buf.data()});
  }
  if (name == "parse_timestamp") {
    if (values.size() < 2 || values.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "PARSE_TIMESTAMP requires 2 or 3 arguments");
    }
    if (values[0].IsNull() || values[1].IsNull()) {
      return Value();
    }
    if (values.size() == 3 && values[2].IsNull()) {
      return Value();
    }
    std::string fmt = raw_str(values[0]);
    std::string input = raw_str(values[1]);
    int tz_offset_sec =
        ParseTimeZoneOffset(GetDefaultTimeZone(), nullptr, -8 * 3600);
    if (values.size() == 3) {
      std::string tz_str = raw_str(values[2]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        return StatusError(StatusCode::kInvalidArgument,
                           "invalid timezone: " + tz_str);
      }
      tz_offset_sec = ParseTimeZoneOffset(tz_str);
    }
    struct tm tm = {};
    tm.tm_year = 100;
    tm.tm_mon = 0;
    tm.tm_mday = 1;
    char* parsed_end = strptime(input.c_str(), fmt.c_str(), &tm);
    if (parsed_end == nullptr) {
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

  // Deferred STRUCT(...) construction: arguments alternate field name and
  // value; encoding is shared with the relational interpreter.
  if (name == "__struct_json__") {
    bool triple_form = values.size() % 3 == 0;
    for (size_t i = 2; triple_form && i < values.size(); i += 3) {
      triple_form = values[i].IsNull() || values[i].type == ValueType::kInt64;
    }
    const size_t stride = triple_form ? 3 : 2;
    std::vector<std::pair<std::string, Value>> fields;
    fields.reserve(values.size() / stride);
    for (size_t i = 0; i + 1 < values.size(); i += stride) {
      Value value = values[i + 1];
      if (triple_form && !values[i + 2].IsNull() && !values[i + 2].Truthy() &&
          value.type == ValueType::kVarChar &&
          value.value.varchar_value == "null") {
        value = Value();
      }
      fields.emplace_back(values[i].IsNull()
                              ? std::string()
                              : std::string(values[i].value.varchar_value),
                          std::move(value));
    }
    return Value(EncodeStructJson(fields));
  }

  if (name == "__proto_new") {
    // NEW ProtoType(v1 AS f1, ...) / SELECT AS ProtoType: argument layout is
    // (type_name, value1, field1, value2, field2, ...).  Builds the proto
    // TEXT payload; required-field and enum-member violations throw.
    if (values.size() % 2 != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__proto_new requires (type, v, f, ...)");
    }
    const std::string type_name = raw_str(values[0]);
    std::vector<std::pair<std::string, Value>> fields;
    fields.reserve(values.size() / 2);
    for (size_t i = 1; i < values.size(); i += 2) {
      fields.emplace_back(raw_str(values[i + 1]), values[i]);
    }
    ASSIGN_OR_RETURN(std::string, proto_text,
                     TryConstructProtoText(type_name, fields));
    return Value(std::move(proto_text));
  }
  if (name == "__value_table_value") {
    if (values.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__value_table_value requires one argument");
    }
    return values.front();
  }
  if (name == "__value_table_proto") {
    if (values.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__value_table_proto requires two arguments");
    }
    if (values[1].IsNull()) {
      return Value();
    }
    const std::string type_name = raw_str(values[0]);
    if (type_name.find("TestExtraPB") == std::string::npos) {
      return values[1];
    }
    std::vector<std::pair<std::string, Value>> fields;
    for (const char* field : {"int32_val1", "int32_val2", "str_value"}) {
      Value value;
      auto proto_hit = TryReadProtoTextField(raw_str(values[1]), field, &value);
      if (!proto_hit.HasValue()) {
        return proto_hit.GetStatus();
      }
      if (proto_hit.Value()) {
        fields.emplace_back(field, std::move(value));
      }
    }
    ASSIGN_OR_RETURN(std::string, proto_text,
                     TryConstructProtoText(type_name, fields));
    return Value(std::move(proto_text));
  }
  if (name == "__value_table_proto_existing") {
    if (values.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__value_table_proto_existing requires two arguments");
    }
    if (values[1].IsNull()) {
      return Value();
    }
    const std::string type_name = raw_str(values[0]);
    if (type_name.find("TestExtraPB") == std::string::npos) {
      return values[1];
    }
    const std::string payload = raw_str(values[1]);
    std::vector<std::pair<std::string, Value>> fields;
    for (const char* field : {"int32_val1", "int32_val2", "str_value"}) {
      if (!ProtoTextHasField(payload, field)) {
        continue;
      }
      Value value;
      auto proto_hit = TryReadProtoTextField(payload, field, &value);
      if (!proto_hit.HasValue()) {
        return proto_hit.GetStatus();
      }
      if (proto_hit.Value()) {
        fields.emplace_back(field, std::move(value));
      }
    }
    ASSIGN_OR_RETURN(std::string, proto_text,
                     TryConstructProtoText(type_name, fields));
    return Value(std::move(proto_text));
  }
  if (name == "__proto_set") {
    // Dotted SET targets over proto TEXT columns: (payload, path, new_value).
    if (values.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__proto_set requires 3 arguments");
    }
    std::vector<std::string> path;
    {
      const std::string joined = raw_str(values[1]);
      size_t start = 0;
      while (true) {
        const size_t dot = joined.find('.', start);
        if (dot == std::string_view::npos) {
          path.emplace_back(joined.substr(start));
          break;
        }
        path.emplace_back(joined.substr(start, dot - start));
        start = dot + 1;
      }
    }
    const std::string type_name = InferProtoTypeName(
        values[0].IsNull() ? std::string_view() : raw_str(values[0]), path);
    if (values[0].IsNull()) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "Cannot set field of NULL `" +
              (type_name.empty() ? std::string("PROTO") : type_name) + "`");
    }
    const std::string payload = raw_str(values[0]);
    ASSIGN_OR_RETURN(std::optional<std::string>, rewritten,
                     TryProtoTextSetField(payload, path, values[2], type_name));
    return Value(rewritten.value_or(payload));
  }
  if (name == "__get_extension") {
    // value.(pkg.Ext.field): reads the bracketed extension entry from a
    // proto TEXT payload; NULL bases yield NULL.
    if (values.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__get_extension requires 2 arguments");
    }
    if (values[0].IsNull()) {
      return Value();
    }
    const std::string base = raw_str(values[0]);
    const std::string key = "[" + raw_str(values[1]) + "]";
    Value out;
    ASSIGN_OR_RETURN(bool, ext_found, TryReadProtoTextField(base, key, &out));
    if (!ext_found) {
      return StatusError(StatusCode::kInvalidArgument,
                         "extension " + raw_str(values[1]) + " not found");
    }
    return out;
  }
  if (name == "unix_seconds" || name == "unix_millis" ||
      name == "unix_micros" || name == "unix_date") {
    if (values.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires one TIMESTAMP argument");
    }
    if (values[0].IsNull()) {
      return Value();
    }
    const std::optional<int64_t> nanos =
        ParseTimestampTextNanos(raw_str(values[0]));
    if (!nanos.has_value()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "invalid TIMESTAMP: " + raw_str(values[0]));
    }
    auto floor_div = [](int64_t a, int64_t b) {
      const int64_t q = a / b;
      return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
    };
    if (name == "unix_date") {
      return Value(floor_div(*nanos, 86400000000000LL));
    }
    if (name == "unix_seconds") {
      return Value(floor_div(*nanos, 1000000000LL));
    }
    if (name == "unix_millis") {
      return Value(floor_div(*nanos, 1000000LL));
    }
    return Value(floor_div(*nanos, 1000LL));
  }

  // IS_NAN / IS_INF: the AST ground truth must implement every scalar the
  // scope-based relational evaluator supports (expression_eval.cpp), or a
  // plan-shape change that routes the predicate through this evaluator turns
  // a working query into "not yet executable".
  if (name == "is_inf" || name == "is_nan") {
    if (values.size() != 1) {
      return Value();
    }
    const Value& arg = values[0];
    if (arg.IsNull()) {
      return Value();
    }
    if (arg.type != ValueType::kDouble) {
      return Value(int64_t{0});
    }
    const double v = arg.value.double_value;
    if (name == "is_inf") {
      return Value(std::isinf(v) ? int64_t{1} : int64_t{0});
    }
    return Value(std::isnan(v) ? int64_t{1} : int64_t{0});
  }

  // FORMAT with non-constant arguments must render per row; the constant
  // shape folds at rewrite time, so this is the executable counterpart.
  if (name == "format") {
    return FormatFunction(name, values);
  }

  // Hashing family: raw digest bytes (BYTES semantics), hex via TO_HEX.
  if (name == "md5" || name == "sha1" || name == "sha256" || name == "sha512") {
    if (values.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 argument");
    }
    if (values[0].IsNull()) {
      return Value();
    }
    const std::string input = raw_str(values[0]);
    if (name == "md5") {
      return Value(digest::Md5Digest(input));
    }
    if (name == "sha1") {
      return Value(digest::Sha1Digest(input));
    }
    if (name == "sha256") {
      return Value(digest::Sha256Digest(input));
    }
    return Value(digest::Sha512Digest(input));
  }

  // JSON accessor family.  The same evaluator backs the json_path_constant_fold
  // rewrite (fold) and this execution path, so constant and row-wise inputs
  // cannot diverge.
  if (name == "json_extract" || name == "json_query" || name == "json_value" ||
      name == "json_extract_scalar" || name == "json_extract_array" ||
      name == "json_query_array" || name == "json_value_array" ||
      name == "json_extract_string_array") {
    if (values.empty() || values.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 or 2 arguments");
    }
    if (values[0].IsNull() || (values.size() == 2 && values[1].IsNull())) {
      return Value();
    }
    const std::string path = values.size() == 2 ? raw_str(values[1]) : "$";
    return EvaluateJsonFunctionCall(name, raw_str(values[0]), path);
  }

  // INTERVAL construction from runtime parts (the visitor emits this for
  // `INTERVAL <column> DAY`); the result is the encoded interval text that
  // generate_date_array and interval arithmetic consume.
  if (name == "make_interval") {
    if (values.size() < 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "make_interval requires at least 2 arguments");
    }
    if (values[0].IsNull() || values[1].IsNull()) {
      return Value();
    }
    const std::string val_str = raw_str(values[0]);
    const std::string unit_str = raw_str(values[1]);
    const IntervalValue iv = IntervalValue::Parse(val_str, unit_str);
    return Value(iv.ToString());
  }

  if (name == "generate_date_array") {
    if (values.size() < 2 || values.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "GENERATE_DATE_ARRAY requires 2 or 3 arguments");
    }
    const Value& start = values[0];
    const Value& end = values[1];
    auto as_date = [](const Value& v) -> Value {
      if (v.type == ValueType::kVarChar) {
        return Value::Date(std::string_view(v.value.varchar_value));
      }
      return v;
    };
    const Value start_date = as_date(start);
    const Value end_date = as_date(end);
    if (start_date.IsNull() || end_date.IsNull() ||
        start_date.type != ValueType::kDate ||
        end_date.type != ValueType::kDate) {
      return StatusError(StatusCode::kInvalidArgument, "DATE value required");
    }
    int64_t step_days = 1;
    if (values.size() == 3) {
      const Value& step = values[2];
      if (step.IsNull()) {
        return Value();
      }
      if (step.type == ValueType::kInt64) {
        step_days = step.value.int_value;
      } else {
        // Column-valued INTERVAL steps arrive as the encoded text of a
        // make_interval call (INTERVAL col DAY) or an evaluated INTERVAL
        // expression ("Y-M D H:M:S"); only whole-day counts are supported.
        const std::string text = raw_str(step);
        const IntervalValue parsed =
            text.empty() ? IntervalValue{} : IntervalValue::Parse(text);
        if (parsed.months != 0 || parsed.nanos != 0) {
          return StatusError(StatusCode::kInvalidArgument,
                             "unsupported GENERATE_DATE_ARRAY step unit");
        }
        step_days = parsed.days;
      }
    }
    if (step_days == 0) {
      return StatusError(StatusCode::kIsInfinity, "Sequence step cannot be 0.");
    }
    const int64_t start_days = start_date.DateDays();
    const int64_t end_days = end_date.DateDays();
    std::vector<Value> elements;
    for (int64_t d = start_days; step_days > 0 ? d <= end_days : d >= end_days;
         d += step_days) {
      elements.push_back(Value::DateFromDays(d));
    }
    return Value::Array(std::move(elements), "DATE");
  }

  // SQL scalar UDFs registered by CREATE FUNCTION: evaluate the body against
  // a synthetic single-row scope holding the argument values.
  if (std::optional<SqlScalarFunction> udf = FindSqlScalarFunction(name)) {
    ASSIGN_OR_RETURN(SqlUdfBinding, binding, BindSqlUdfArguments(*udf, values));
    RETURN_IF_FAIL(SqlUdfDepthGuard::CheckAvailable());
    SqlUdfDepthGuard depth_guard;
    return udf->body->TryEvaluate(binding.row, binding.schema);
  }
  return StatusError(StatusCode::kInvalidArgument,
                     "Function calls are not yet executable: " + name);
}

}  // namespace

// ---- Struct (JSON text) helpers shared by the evaluators and the DML
// mapping. Struct values are stored as flat JSON objects:
//   {"field":value,"nested":{"x":1}}
// ---------------------------------------------------------------------------

namespace {

bool IsJsonSpace(char c) {
  return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

bool IdentifierEquals(std::string_view left, std::string_view right) {
  return left.size() == right.size() &&
         std::equal(left.begin(), left.end(), right.begin(),
                    [](char lhs, char rhs) {
                      return std::tolower(static_cast<unsigned char>(lhs)) ==
                             std::tolower(static_cast<unsigned char>(rhs));
                    });
}

std::string TrimJson(std::string s) {
  size_t b = 0;
  while (b < s.size() && IsJsonSpace(s[b])) {
    ++b;
  }
  size_t e = s.size();
  while (e > b && IsJsonSpace(s[e - 1])) {
    --e;
  }
  return s.substr(b, e - b);
}

std::string EscapeJsonText(std::string_view text) {
  std::string escaped;
  escaped.reserve(text.size());
  for (const char c : text) {
    switch (c) {
      case '"':
        escaped += "\\\"";
        break;
      case '\\':
        escaped += "\\\\";
        break;
      case '\n':
        escaped += "\\n";
        break;
      case '\t':
        escaped += "\\t";
        break;
      case '\r':
        escaped += "\\r";
        break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          std::array<char, 8> buf{};
          // Fixed-size escape output can neither fail nor truncate.
          // NOLINTNEXTLINE(cert-err33-c)
          snprintf(buf.data(), buf.size(), "\\u%04x",
                   static_cast<unsigned char>(c));
          escaped += buf.data();
        } else {
          escaped.push_back(c);
        }
    }
  }
  return escaped;
}

}  // namespace

// Splits a JSON object body into top-level key / raw-value-text pairs.
std::vector<std::pair<std::string, std::string>> SplitJsonObjectMembers(
    const std::string& body) {
  std::vector<std::pair<std::string, std::string>> members;
  int depth = 0;
  bool in_str = false;
  char quote = '\0';
  std::string current;
  auto flush = [&]() {
    const std::string member = TrimJson(current);
    if (member.empty()) {
      return;
    }
    size_t colon = std::string::npos;
    int d2 = 0;
    bool s2 = false;
    char q2 = '\0';
    for (size_t i = 0; i < member.size(); ++i) {
      const char c = member[i];
      if (s2) {
        if (c == '\\' && i + 1 < member.size()) {
          ++i;
        } else if (c == q2) {
          s2 = false;
        }
        continue;
      }
      if (c == '"' || c == '\'') {
        s2 = true;
        q2 = c;
      } else if (c == '{' || c == '[') {
        ++d2;
      } else if (c == '}' || c == ']') {
        --d2;
      } else if (c == ':' && d2 == 0) {
        colon = i;
        break;
      }
    }
    std::string key =
        colon == std::string::npos ? member : member.substr(0, colon);
    std::string value_text =
        colon == std::string::npos ? "" : member.substr(colon + 1);
    key = TrimJson(key);
    if (key.size() >= 2 && key.front() == '"' && key.back() == '"') {
      key = key.substr(1, key.size() - 2);
    }
    members.emplace_back(std::move(key), TrimJson(value_text));
    current.clear();
  };
  for (size_t i = 0; i < body.size(); ++i) {
    const char c = body[i];
    if (in_str) {
      current.push_back(c);
      if (c == '\\' && i + 1 < body.size()) {
        current.push_back(body[++i]);
      } else if (c == quote) {
        in_str = false;
      }
      continue;
    }
    if (c == '"' || c == '\'') {
      in_str = true;
      quote = c;
      current.push_back(c);
    } else if (c == '{' || c == '[') {
      ++depth;
      current.push_back(c);
    } else if (c == '}' || c == ']') {
      --depth;
      current.push_back(c);
    } else if (c == ',' && depth == 0) {
      flush();
    } else {
      current.push_back(c);
    }
  }
  flush();
  return members;
}

bool JsonTextToValue(const std::string& text, Value* parsed) {
  const std::string trimmed = TrimJson(text);
  if (trimmed == "null") {
    *parsed = Value();
    return true;
  }
  if (trimmed == "true") {
    *parsed = Value(int64_t{1});
    return true;
  }
  if (trimmed == "false") {
    *parsed = Value(int64_t{0});
    return true;
  }
  if (trimmed.size() >= 2 && trimmed.front() == '"' && trimmed.back() == '"') {
    std::string unescaped;
    unescaped.reserve(trimmed.size());
    for (size_t i = 1; i + 1 < trimmed.size(); ++i) {
      if (trimmed[i] == '\\' && i + 2 < trimmed.size()) {
        ++i;
        switch (trimmed[i]) {
          case 'n':
            unescaped.push_back('\n');
            break;
          case 't':
            unescaped.push_back('\t');
            break;
          case 'r':
            unescaped.push_back('\r');
            break;
          default:
            unescaped.push_back(trimmed[i]);
            break;
        }
      } else {
        unescaped.push_back(trimmed[i]);
      }
    }
    constexpr std::string_view kDateMarker = "__tinylamb_date__:";
    if (unescaped.starts_with(kDateMarker)) {
      *parsed = Value::Date(unescaped.substr(kDateMarker.size()));
    } else {
      *parsed = Value(std::move(unescaped));
    }
    return true;
  }
  if (!trimmed.empty()) {
    size_t consumed = 0;
    try {
      const int64_t as_int = std::stoll(trimmed, &consumed);
      if (consumed == trimmed.size()) {
        *parsed = Value(as_int);
        return true;
      }
      const double as_double = std::stod(trimmed, &consumed);
      if (consumed == trimmed.size()) {
        *parsed = Value(as_double);
        return true;
      }
    } catch (const std::exception&) {
      consumed = 0;
    }
  }
  // Struct values encode nested arrays using Value::AsString(), for example
  // `ARRAY<INT64>[3, 5]`.  Decode that representation here rather than
  // leaving it as a STRING: field traversal followed by UNNEST must observe
  // the original array value.
  if (trimmed.starts_with("ARRAY<") && trimmed.back() == ']') {
    const size_t type_end = trimmed.find(">[");
    if (type_end != std::string::npos) {
      const std::string element_type = trimmed.substr(6, type_end - 6);
      const std::string body =
          trimmed.substr(type_end + 2, trimmed.size() - type_end - 3);
      std::vector<Value> elements;
      size_t start = 0;
      int depth = 0;
      bool quoted = false;
      for (size_t i = 0; i <= body.size(); ++i) {
        const bool at_end = i == body.size();
        const char c = at_end ? ',' : body[i];
        if (!at_end && c == '"') {
          quoted = !quoted;
        } else if (!quoted && !at_end && (c == '[' || c == '{' || c == '(')) {
          ++depth;
        } else if (!quoted && !at_end && (c == ']' || c == '}' || c == ')')) {
          --depth;
        }
        if (at_end || (!quoted && depth == 0 && c == ',')) {
          const std::string item = TrimJson(body.substr(start, i - start));
          if (item.empty() || item == "NULL" || item == "null") {
            elements.emplace_back();
          } else if (item.front() == '"' && item.back() == '"' &&
                     item.size() >= 2) {
            elements.emplace_back(item.substr(1, item.size() - 2));
          } else if (element_type == "DATE") {
            elements.emplace_back(Value::Date(item));
          } else {
            try {
              size_t consumed = 0;
              const int64_t integer = std::stoll(item, &consumed);
              if (consumed == item.size()) {
                elements.emplace_back(integer);
              } else {
                const double real = std::stod(item, &consumed);
                elements.emplace_back(real);
              }
            } catch (const std::exception&) {
              elements.emplace_back(std::string(item));
            }
          }
          start = i + 1;
        }
      }
      *parsed = Value::Array(std::move(elements), element_type);
      return true;
    }
  }
  std::string lowered_head;
  if (!trimmed.empty()) {
    lowered_head = trimmed.substr(0, 5);
    for (char& c : lowered_head) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
  }
  if (!trimmed.empty() && ((trimmed.front() == '{' && trimmed.back() == '}') ||
                           (trimmed.front() == '[' && trimmed.back() == ']') ||
                           lowered_head == "array")) {
    // Objects / bare arrays / canonical ARRAY<T>[...] tokens stay as raw
    // struct-member text.
    *parsed = Value(std::string(trimmed));
    return true;
  }
  return false;
}

StatusOr<std::string> TryEncodeStructMemberJson(const Value& value) {
  if (value.IsNull()) {
    return std::string("null");
  }
  switch (value.type) {
    case ValueType::kInt64:
      return std::to_string(value.value.int_value);
    case ValueType::kDouble: {
      std::array<char, 64> buffer{};
      auto [ptr, ec] =
          std::to_chars(buffer.data(), buffer.data() + buffer.size(),
                        value.value.double_value);
      (void)ec;
      return std::string{buffer.data(),
                         static_cast<size_t>(ptr - buffer.data())};
    }
    case ValueType::kDate:
      return "\"" + EscapeJsonText(FormatDateDays(value.DateDays())) + "\"";
    case ValueType::kVarChar: {
      std::string text(value.value.varchar_value);
      // Nested structs and arrays are already JSON-shaped; embed verbatim.
      if (text.size() >= 2 && ((text.front() == '{' && text.back() == '}') ||
                               (text.front() == '[' && text.back() == ']'))) {
        return text;
      }
      return "\"" + EscapeJsonText(text) + "\"";
    }
    case ValueType::kArray: {
      // Match the canonical struct-constructor storage text, which embeds
      // arrays via Value::AsString(): "ARRAY<INT64>[50, NULL, 52]".
      return value.AsString();
    }
    default:
      break;
  }
  return StatusError(StatusCode::kInvalidArgument,
                     "cannot encode struct member");
}

StatusOr<Value> TryStructSetField(const Value& json, const std::string& path,
                                  const Value& new_value) {
  if (json.IsNull()) {
    return json;
  }
  if (json.type != ValueType::kVarChar) {
    return StatusError(StatusCode::kInvalidArgument,
                       "struct field assignment requires a STRUCT");
  }
  const std::string text(json.value.varchar_value);
  if (text.size() < 2 || text.front() != '{' || text.back() != '}') {
    return StatusError(StatusCode::kInvalidArgument,
                       "struct field assignment requires a STRUCT");
  }
  size_t dot = path.find('.');
  const std::string head =
      dot == std::string::npos ? path : path.substr(0, dot);
  const std::string rest =
      dot == std::string::npos ? std::string() : path.substr(dot + 1);
  const auto members = SplitJsonObjectMembers(text.substr(1, text.size() - 2));
  std::string rebuilt = "{";
  bool first = true;
  bool replaced = false;
  for (const auto& [key, value_text] : members) {
    if (!first) {
      rebuilt += ",";
    }
    first = false;
    if (IdentifierEquals(key, head)) {
      replaced = true;
      rebuilt += "\"" + EscapeJsonText(key) + "\":";
      if (rest.empty()) {
        ASSIGN_OR_RETURN(std::string, encoded_new,
                         TryEncodeStructMemberJson(new_value));
        rebuilt += encoded_new;
      } else {
        Value nested;
        JsonTextToValue(value_text, &nested);
        ASSIGN_OR_RETURN(Value, sub,
                         TryStructSetField(nested, rest, new_value));
        ASSIGN_OR_RETURN(std::string, encoded_sub,
                         TryEncodeStructMemberJson(sub));
        rebuilt += encoded_sub;
      }
    } else {
      rebuilt += "\"" + EscapeJsonText(key) + "\":" + value_text;
    }
  }
  if (!replaced) {
    if (!first) {
      rebuilt += ",";
    }
    rebuilt += "\"" + EscapeJsonText(head) + "\":";
    if (rest.empty()) {
      ASSIGN_OR_RETURN(std::string, encoded_new,
                       TryEncodeStructMemberJson(new_value));
      rebuilt += encoded_new;
    } else {
      ASSIGN_OR_RETURN(
          Value, sub,
          TryStructSetField(Value(std::string("{}")), rest, new_value));
      ASSIGN_OR_RETURN(std::string, encoded_sub,
                       TryEncodeStructMemberJson(sub));
      rebuilt += encoded_sub;
    }
  }
  rebuilt += "}";
  return Value(std::move(rebuilt));
}

// EXC-SHIM: deprecated throwing wrappers (common/exc_shim.hpp); the
// query/executor callers switch to the Try* forms in Phase 6/7.
Value StructSetField(const Value& json, const std::string& path,
                     const Value& new_value) {
  return ExcShimUnwrap(TryStructSetField(json, path, new_value),
                       "StructSetField");
}

std::string EncodeStructMemberJson(const Value& value) {
  return ExcShimUnwrap(TryEncodeStructMemberJson(value),
                       "EncodeStructMemberJson");
}

std::unordered_set<ColumnName> FunctionCallExpression::TouchedColumns() const {
  std::unordered_set<ColumnName> result;
  for (const auto& arg : args_) {
    result.merge(arg->TouchedColumns());
  }
  return result;
}

namespace {
// IF/IFERROR branch results are normalized to the common supertype of every
// branch (int64 results promote to double when any branch is double) so
// downstream comparisons and sort keys stay type-consistent.  Shared by all
// three TryEvaluate overloads so the join/context forms cannot diverge from
// the plain one.
bool IfBranchesPromoteToDouble(const std::vector<Expression>& args,
                               const Schema& schema, const size_t from) {
  for (size_t i = from; i < args.size(); ++i) {
    try {
      if (args[i]->ResultType(schema).GetType() == TypeTag::kDouble) {
        return true;
      }
    } catch (const std::exception&) {
      continue;
    }
  }
  return false;
}

bool IfBranchesPromoteToDouble(const std::vector<Expression>& args,
                               const Schema& left, const Schema& right,
                               const size_t from) {
  for (size_t i = from; i < args.size(); ++i) {
    try {
      if (args[i]->ResultType(left, right).GetType() == TypeTag::kDouble) {
        return true;
      }
    } catch (const std::exception&) {
      continue;
    }
  }
  return false;
}

Value NormalizeIfBranch(Value value, const bool to_double) {
  if (to_double && !value.IsNull() && value.type == ValueType::kInt64) {
    return Value(static_cast<double>(value.value.int_value));
  }
  return value;
}
}  // namespace

StatusOr<Value> FunctionCallExpression::TryEvaluate(
    const Row& row, const Schema& schema) const {
  if (func_name_ == "__row_struct") {
    // Bare alias row reference ("SELECT s FROM t s"): encodes the columns
    // qualified by the given alias as a struct JSON object.  Evaluated with
    // the scope's full row so multi-source queries pick their own columns.
    if (args_.size() != 1 || args_[0]->Type() != TypeTag::kConstantValue) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__row_struct requires an alias literal");
    }
    std::string alias;
    const Value& alias_value = args_[0]->AsConstantValue().GetValue();
    if (!alias_value.IsNull()) {
      alias = alias_value.type == ValueType::kVarChar
                  ? std::string(alias_value.value.varchar_value)
                  : alias_value.AsString();
    }
    std::vector<std::pair<std::string, Value>> fields;
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      const ColumnName& column = schema.GetColumn(i).Name();
      if (!alias.empty() && !IdentifierEquals(column.schema, alias)) {
        continue;
      }
      fields.emplace_back(column.name, row.values_[i]);
    }
    return Value(EncodeStructJson(fields));
  }
  if (func_name_ == "date_add" || func_name_ == "date_sub") {
    if (args_.size() != 2 || args_[1]->Type() != TypeTag::kIntervalExp) {
      return StatusError(StatusCode::kInvalidArgument,
                         "DATE_ADD/DATE_SUB requires DATE and INTERVAL");
    }
    ASSIGN_OR_RETURN(Value, date, args_[0]->TryEvaluate(row, schema));
    if (date.IsNull()) {
      return Value();
    }
    return AddOrSubInterval(func_name_, date, args_[1]->AsIntervalExpression());
  }
  // Conditional-evaluation semantics: only the taken (or error-handled)
  // branch is evaluated, so errors inside untaken branches never surface.
  // Branch results are normalized via NormalizeIfBranch (see above).
  if (func_name_ == "if") {
    if (args_.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "IF requires 3 arguments");
    }
    const bool as_double = IfBranchesPromoteToDouble(args_, schema, 1);
    ASSIGN_OR_RETURN(Value, condition, args_[0]->TryEvaluate(row, schema));
    ASSIGN_OR_RETURN(
        Value, taken,
        args_[condition.Truthy() ? 1 : 2]->TryEvaluate(row, schema));
    return NormalizeIfBranch(std::move(taken), as_double);
  }
  if (func_name_ == "iferror") {
    if (args_.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "IFERROR requires 2 arguments");
    }
    const bool as_double = IfBranchesPromoteToDouble(args_, schema, 0);
    auto primary = args_[0]->TryEvaluate(row, schema);
    ASSIGN_OR_RETURN(Value, taken,
                     primary.HasValue() ? std::move(primary)
                                        : args_[1]->TryEvaluate(row, schema));
    return NormalizeIfBranch(std::move(taken), as_double);
  }
  if (func_name_ == "iserror") {
    if (args_.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ISERROR requires 1 argument");
    }
    auto evaluated = args_[0]->TryEvaluate(row, schema);
    return Value(evaluated.HasValue() ? int64_t{0} : int64_t{1});
  }
  if (func_name_ == "nulliferror") {
    if (args_.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "NULLIFERROR requires 1 argument");
    }
    auto result = args_[0]->TryEvaluate(row, schema);
    if (!result.HasValue()) {
      return Value();
    }
    return result;
  }
  // COALESCE / IFNULL short-circuit left to right like the relational
  // evaluator: errors inside unevaluated branches never surface. Evaluating
  // every argument eagerly here turned working queries (e.g. a throwing
  // third branch after a non-NULL first) into spurious failures.
  if (func_name_ == "coalesce") {
    for (const auto& arg : args_) {
      ASSIGN_OR_RETURN(Value, value, arg->TryEvaluate(row, schema));
      if (!value.IsNull()) {
        return value;
      }
    }
    return Value();
  }
  if (func_name_ == "ifnull") {
    if (args_.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "IFNULL requires 2 arguments");
    }
    ASSIGN_OR_RETURN(Value, first, args_[0]->TryEvaluate(row, schema));
    if (!first.IsNull()) {
      return first;
    }
    return args_[1]->TryEvaluate(row, schema);
  }
  std::vector<Value> values;
  values.reserve(args_.size());
  for (const auto& arg : args_) {
    ASSIGN_OR_RETURN(Value, value, arg->TryEvaluate(row, schema));
    values.push_back(std::move(value));
  }
  return ExecuteFunction(func_name_, values);
}

std::string FunctionCallExpression::ToString() const {
  std::stringstream ss;
  ss << func_name_ << "(";
  for (size_t i = 0; i < args_.size(); ++i) {
    ss << *args_[i];
    if (i < args_.size() - 1) {
      ss << ", ";
    }
  }
  ss << ")";
  return ss.str();
}

void FunctionCallExpression::Dump(std::ostream& o) const { o << ToString(); }

StatusOr<Value> FunctionCallExpression::TryEvaluate(
    const Row* left, const Schema& left_schema, const Row* right,
    const Schema& right_schema) const {
  if (func_name_ == "date_add" || func_name_ == "date_sub") {
    if (args_.size() != 2 || args_[1]->Type() != TypeTag::kIntervalExp) {
      return StatusError(StatusCode::kInvalidArgument,
                         "DATE_ADD/DATE_SUB requires DATE and INTERVAL");
    }
    ASSIGN_OR_RETURN(
        Value, date,
        (args_[0]->TryEvaluate(left, left_schema, right, right_schema)));
    if (date.IsNull()) {
      return Value();
    }
    return AddOrSubInterval(func_name_, date, args_[1]->AsIntervalExpression());
  }
  // Lazy conditional-evaluation semantics (mirrors the plain overload,
  // including the branch-type normalization).
  if (func_name_ == "if") {
    if (args_.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "IF requires 3 arguments");
    }
    const bool as_double =
        IfBranchesPromoteToDouble(args_, left_schema, right_schema, 1);
    ASSIGN_OR_RETURN(
        Value, condition,
        (args_[0]->TryEvaluate(left, left_schema, right, right_schema)));
    ASSIGN_OR_RETURN(Value, branch,
                     (args_[condition.Truthy() ? 1 : 2]->TryEvaluate(
                         left, left_schema, right, right_schema)));
    return NormalizeIfBranch(std::move(branch), as_double);
  }
  if (func_name_ == "iferror") {
    if (args_.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "IFERROR requires 2 arguments");
    }
    if (auto attempt =
            args_[0]->TryEvaluate(left, left_schema, right, right_schema);
        attempt.HasValue()) {
      return attempt;
    }
    return args_[1]->TryEvaluate(left, left_schema, right, right_schema);
  }
  if (func_name_ == "iserror") {
    if (args_.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ISERROR requires 1 argument");
    }
    auto evaluated =
        args_[0]->TryEvaluate(left, left_schema, right, right_schema);
    return Value(evaluated.HasValue() ? int64_t{0} : int64_t{1});
  }
  if (func_name_ == "nulliferror") {
    if (args_.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "NULLIFERROR requires 1 argument");
    }
    auto result = args_[0]->TryEvaluate(left, left_schema, right, right_schema);
    if (!result.HasValue()) {
      return Value();
    }
    return result;
  }
  // COALESCE / IFNULL short-circuit left to right like the relational
  // evaluator (see the plain overload): errors inside unevaluated branches
  // never surface.
  if (func_name_ == "coalesce") {
    for (const auto& arg : args_) {
      ASSIGN_OR_RETURN(
          Value, value,
          arg->TryEvaluate(left, left_schema, right, right_schema));
      if (!value.IsNull()) {
        return value;
      }
    }
    return Value();
  }
  if (func_name_ == "ifnull") {
    if (args_.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "IFNULL requires 2 arguments");
    }
    ASSIGN_OR_RETURN(
        Value, first,
        (args_[0]->TryEvaluate(left, left_schema, right, right_schema)));
    if (!first.IsNull()) {
      return first;
    }
    return args_[1]->TryEvaluate(left, left_schema, right, right_schema);
  }
  std::vector<Value> values;
  values.reserve(args_.size());
  for (const auto& arg : args_) {
    ASSIGN_OR_RETURN(Value, value,
                     arg->TryEvaluate(left, left_schema, right, right_schema));
    values.push_back(std::move(value));
  }
  return ExecuteFunction(func_name_, values);
}

// Context-aware form: same dispatch as the plain evaluator with the context
// threaded into every argument (A1 stage 3).
StatusOr<Value> FunctionCallExpression::TryEvaluate(
    const Row& row, const Schema& schema, EvaluationContext& context) const {
  if (func_name_ == "date_add" || func_name_ == "date_sub") {
    if (args_.size() != 2 || args_[1]->Type() != TypeTag::kIntervalExp) {
      return StatusError(StatusCode::kInvalidArgument,
                         "DATE_ADD/DATE_SUB requires DATE and INTERVAL");
    }
    ASSIGN_OR_RETURN(Value, date, args_[0]->TryEvaluate(row, schema, context));
    if (date.IsNull()) {
      return Value();
    }
    return AddOrSubInterval(func_name_, date, args_[1]->AsIntervalExpression());
  }
  // Lazy conditional-evaluation semantics (mirrors the plain overload,
  // including the branch-type normalization).
  if (func_name_ == "if") {
    if (args_.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "IF requires 3 arguments");
    }
    const bool as_double = IfBranchesPromoteToDouble(args_, schema, 1);
    ASSIGN_OR_RETURN(Value, condition,
                     (args_[0]->TryEvaluate(row, schema, context)));
    ASSIGN_OR_RETURN(
        Value, branch,
        (args_[condition.Truthy() ? 1 : 2]->TryEvaluate(row, schema, context)));
    return NormalizeIfBranch(std::move(branch), as_double);
  }
  if (func_name_ == "iferror") {
    if (args_.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "IFERROR requires 2 arguments");
    }
    if (auto attempt = args_[0]->TryEvaluate(row, schema, context);
        attempt.HasValue()) {
      return attempt;
    }
    return args_[1]->TryEvaluate(row, schema, context);
  }
  if (func_name_ == "iserror") {
    if (args_.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ISERROR requires 1 argument");
    }
    auto evaluated = args_[0]->TryEvaluate(row, schema, context);
    return Value(evaluated.HasValue() ? int64_t{0} : int64_t{1});
  }
  if (func_name_ == "nulliferror") {
    if (args_.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "NULLIFERROR requires 1 argument");
    }
    auto result = args_[0]->TryEvaluate(row, schema, context);
    if (!result.HasValue()) {
      return Value();
    }
    return result;
  }
  // COALESCE / IFNULL short-circuit left to right like the relational
  // evaluator (see the plain overload): errors inside unevaluated branches
  // never surface.
  if (func_name_ == "coalesce") {
    for (const auto& arg : args_) {
      ASSIGN_OR_RETURN(Value, value, arg->TryEvaluate(row, schema, context));
      if (!value.IsNull()) {
        return value;
      }
    }
    return Value();
  }
  if (func_name_ == "ifnull") {
    if (args_.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "IFNULL requires 2 arguments");
    }
    ASSIGN_OR_RETURN(Value, first, args_[0]->TryEvaluate(row, schema, context));
    if (!first.IsNull()) {
      return first;
    }
    return args_[1]->TryEvaluate(row, schema, context);
  }
  std::vector<Value> values;
  values.reserve(args_.size());
  for (const auto& arg : args_) {
    ASSIGN_OR_RETURN(Value, value, arg->TryEvaluate(row, schema, context));
    values.push_back(std::move(value));
  }
  return ExecuteFunction(func_name_, values);
}

// EXC-SHIM: deprecated throwing wrappers (common/exc_shim.hpp).
Value FunctionCallExpression::Evaluate(const Row& row,
                                       const Schema& schema) const {
  return ExcShimUnwrap(TryEvaluate(row, schema),
                       "FunctionCallExpression::Evaluate");
}

Value FunctionCallExpression::Evaluate(const Row* left,
                                       const Schema& left_schema,
                                       const Row* right,
                                       const Schema& right_schema) const {
  return ExcShimUnwrap(TryEvaluate(left, left_schema, right, right_schema),
                       "FunctionCallExpression::Evaluate");
}

Value FunctionCallExpression::Evaluate(const Row& row, const Schema& schema,
                                       EvaluationContext& context) const {
  return ExcShimUnwrap(TryEvaluate(row, schema, context),
                       "FunctionCallExpression::Evaluate");
}

Type FunctionCallExpression::ResultType(const Schema& schema) const {
  if (func_name_ == "__get_field_safe" || func_name_ == "get_field") {
    if (args_.size() > 1 && args_[1]->Type() == TypeTag::kConstantValue) {
      const Value& field = args_[1]->AsConstantValue().GetValue();
      if (field.type == ValueType::kVarChar) {
        const std::string name(field.value.varchar_value);
        if (name == "str_value") {
          return {TypeTag::kArray};
        }
        if (name.starts_with("int") || name.starts_with("uint") ||
            name.starts_with("fixed") || name.starts_with("sfixed") ||
            name.starts_with("sint") || name == "bool_val") {
          return {TypeTag::kBigInt};
        }
      }
    }
    return {TypeTag::kVarChar};
  }
  if (func_name_ == "coalesce" || func_name_ == "nullif" ||
      func_name_ == "ifnull" || func_name_ == "greatest" ||
      func_name_ == "least") {
    if (args_.empty()) {
      return {TypeTag::kInvalid};
    }
    return args_[0]->ResultType(schema);
  }
  if (func_name_ == "if") {
    if (args_.size() < 2) {
      return {TypeTag::kInvalid};
    }
    return args_[1]->ResultType(schema);
  }
  if (func_name_ == "iferror" || func_name_ == "nulliferror") {
    if (args_.empty()) {
      return {TypeTag::kInvalid};
    }
    return args_[0]->ResultType(schema);
  }
  if (func_name_ == "iserror") {
    return {TypeTag::kBigInt};
  }
  if (func_name_ == "split" || func_name_ == "regexp_extract_all" ||
      func_name_.ends_with("_array")) {
    return {TypeTag::kArray};
  }
  if (func_name_ == "concat" || func_name_ == "current_timestamp" ||
      func_name_ == "format" || func_name_ == "substr" ||
      func_name_ == "substring" || func_name_ == "upper" ||
      func_name_ == "lower" || func_name_ == "trim" || func_name_ == "ltrim" ||
      func_name_ == "rtrim" || func_name_ == "replace" ||
      func_name_ == "repeat" || func_name_ == "reverse" ||
      func_name_ == "split_substr" || func_name_ == "byte_substr" ||
      func_name_ == "byte_reverse" || func_name_ == "code_points_to_string" ||
      func_name_ == "code_points_to_bytes" || func_name_ == "octet_length" ||
      func_name_ == "left" || func_name_ == "right" || func_name_ == "lpad" ||
      func_name_ == "rpad" || func_name_ == "initcap" || func_name_ == "chr" ||
      func_name_ == "soundex" || func_name_ == "translate" ||
      func_name_ == "regexp_extract" || func_name_ == "regexp_replace" ||
      func_name_.starts_with("json_") || func_name_ == "to_json_string") {
    return {TypeTag::kVarChar};
  }

  if (func_name_ == "length" || func_name_ == "char_length" ||
      func_name_ == "character_length" || func_name_ == "byte_length" ||
      func_name_ == "strpos" || func_name_ == "instr" ||
      func_name_ == "starts_with" || func_name_ == "ends_with" ||
      func_name_ == "ascii" || func_name_ == "unicode" ||
      func_name_ == "regexp_contains" || func_name_ == "regexp_match" ||
      func_name_ == "regexp_instr" || func_name_ == "div" ||
      func_name_.starts_with("extract_")) {
    return {TypeTag::kBigInt};
  }

  if (func_name_ == "abs" || func_name_ == "sign" || func_name_ == "round" ||
      func_name_ == "trunc" || func_name_ == "truncate" ||
      func_name_ == "ceil" || func_name_ == "ceiling" ||
      func_name_ == "floor" || func_name_ == "mod" ||
      func_name_ == "safe_add" || func_name_ == "safe_subtract" ||
      func_name_ == "safe_multiply" || func_name_ == "safe_negate") {
    if (args_.empty()) {
      return {TypeTag::kBigInt};
    }
    return args_[0]->ResultType(schema);
  }
  if (func_name_ == "pow" || func_name_ == "power" || func_name_ == "sqrt" ||
      func_name_ == "cbrt" || func_name_ == "ln" || func_name_ == "log" ||
      func_name_ == "log10" || func_name_ == "exp" || func_name_ == "cos" ||
      func_name_ == "sin" || func_name_ == "tan" || func_name_ == "acos" ||
      func_name_ == "asin" || func_name_ == "atan" || func_name_ == "atan2" ||
      func_name_ == "cosh" || func_name_ == "sinh" || func_name_ == "tanh" ||
      func_name_ == "radians" || func_name_ == "degrees" ||
      func_name_ == "pi" || func_name_ == "ieee_divide" ||
      func_name_ == "safe_divide") {
    return {TypeTag::kDouble};
  }
  if (func_name_ == "date_add" || func_name_ == "date_sub") {
    return args_[0]->ResultType(schema);
  }
  return {TypeTag::kVarChar};
}

Type FunctionCallExpression::ResultType(const Schema& left,
                                        const Schema& right) const {
  if (func_name_ == "__get_field_safe" || func_name_ == "get_field") {
    if (args_.size() > 1 && args_[1]->Type() == TypeTag::kConstantValue) {
      const Value& field = args_[1]->AsConstantValue().GetValue();
      if (field.type == ValueType::kVarChar) {
        const std::string name(field.value.varchar_value);
        if (name == "str_value") {
          return {TypeTag::kArray};
        }
        if (name.starts_with("int") || name.starts_with("uint") ||
            name.starts_with("fixed") || name.starts_with("sfixed") ||
            name.starts_with("sint") || name == "bool_val") {
          return {TypeTag::kBigInt};
        }
      }
    }
    return {TypeTag::kVarChar};
  }
  if (func_name_ == "coalesce" || func_name_ == "nullif" ||
      func_name_ == "ifnull" || func_name_ == "greatest" ||
      func_name_ == "least") {
    if (args_.empty()) {
      return {TypeTag::kInvalid};
    }
    return args_[0]->ResultType(left, right);
  }
  if (func_name_ == "if") {
    if (args_.size() < 2) {
      return {TypeTag::kInvalid};
    }
    return args_[1]->ResultType(left, right);
  }
  if (func_name_ == "iferror" || func_name_ == "nulliferror") {
    if (args_.empty()) {
      return {TypeTag::kInvalid};
    }
    return args_[0]->ResultType(left, right);
  }
  if (func_name_ == "iserror") {
    return {TypeTag::kBigInt};
  }
  if (func_name_ == "split" || func_name_ == "regexp_extract_all" ||
      func_name_.ends_with("_array")) {
    return {TypeTag::kArray};
  }
  if (func_name_ == "concat" || func_name_ == "current_timestamp" ||
      func_name_ == "format" || func_name_ == "substr" ||
      func_name_ == "substring" || func_name_ == "upper" ||
      func_name_ == "lower" || func_name_ == "trim" || func_name_ == "ltrim" ||
      func_name_ == "rtrim" || func_name_ == "replace" ||
      func_name_ == "repeat" || func_name_ == "reverse" ||
      func_name_ == "split_substr" || func_name_ == "byte_substr" ||
      func_name_ == "byte_reverse" || func_name_ == "code_points_to_string" ||
      func_name_ == "code_points_to_bytes" || func_name_ == "octet_length" ||
      func_name_ == "left" || func_name_ == "right" || func_name_ == "lpad" ||
      func_name_ == "rpad" || func_name_ == "initcap" || func_name_ == "chr" ||
      func_name_ == "soundex" || func_name_ == "translate" ||
      func_name_ == "regexp_extract" || func_name_ == "regexp_replace" ||
      func_name_.starts_with("json_") || func_name_ == "to_json_string") {
    return {TypeTag::kVarChar};
  }

  if (func_name_ == "length" || func_name_ == "char_length" ||
      func_name_ == "character_length" || func_name_ == "byte_length" ||
      func_name_ == "strpos" || func_name_ == "instr" ||
      func_name_ == "starts_with" || func_name_ == "ends_with" ||
      func_name_ == "ascii" || func_name_ == "unicode" ||
      func_name_ == "regexp_contains" || func_name_ == "regexp_match" ||
      func_name_ == "regexp_instr" || func_name_ == "div" ||
      func_name_.starts_with("extract_")) {
    return {TypeTag::kBigInt};
  }

  if (func_name_ == "abs" || func_name_ == "sign" || func_name_ == "round" ||
      func_name_ == "trunc" || func_name_ == "truncate" ||
      func_name_ == "ceil" || func_name_ == "ceiling" ||
      func_name_ == "floor" || func_name_ == "mod" ||
      func_name_ == "safe_add" || func_name_ == "safe_subtract" ||
      func_name_ == "safe_multiply" || func_name_ == "safe_negate") {
    if (args_.empty()) {
      return {TypeTag::kBigInt};
    }
    return args_[0]->ResultType(left, right);
  }
  if (func_name_ == "pow" || func_name_ == "power" || func_name_ == "sqrt" ||
      func_name_ == "cbrt" || func_name_ == "ln" || func_name_ == "log" ||
      func_name_ == "log10" || func_name_ == "exp" || func_name_ == "cos" ||
      func_name_ == "sin" || func_name_ == "tan" || func_name_ == "acos" ||
      func_name_ == "asin" || func_name_ == "atan" || func_name_ == "atan2" ||
      func_name_ == "cosh" || func_name_ == "sinh" || func_name_ == "tanh" ||
      func_name_ == "radians" || func_name_ == "degrees" ||
      func_name_ == "pi" || func_name_ == "ieee_divide" ||
      func_name_ == "safe_divide") {
    return {TypeTag::kDouble};
  }
  if (func_name_ == "date_add" || func_name_ == "date_sub") {
    return args_[0]->ResultType(left, right);
  }
  return {TypeTag::kVarChar};
}

Status FunctionCallExpression::Validate(EvaluationContext& context,
                                        const Schema& schema) const {
  for (const auto& arg : args_) {
    Status s = arg->Validate(context, schema);
    if (s != Status::kSuccess) {
      return s;
    }
  }
  // Function registration goes through the abstract context; the production
  // implementation forwards to Database::GetOrAddFunction (improvement3.md
  // A1).  Type check is still TODO.
  return context.GetOrAddFunction(func_name_, static_cast<int>(args_.size()));
}

}  // namespace tinylamb
