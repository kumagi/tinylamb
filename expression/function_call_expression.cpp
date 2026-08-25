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

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <iomanip>
#include <ostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>
#include <utility>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "expression/evaluation_context.hpp"
#include "expression/interval_expression.hpp"
#include "type/column_name.hpp"
#include "type/function.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/date.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

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
  if (s.empty()) { return false; }
  int Y = 0, M = 0, D = 0, h = 0, m = 0, sec = 0;
  bool matched = false;
  if (sscanf(s.data(), "%d-%d-%d %d:%d:%d", &Y, &M, &D, &h, &m, &sec) >= 3 ||
      sscanf(s.data(), "%d-%d-%dT%d:%d:%d", &Y, &M, &D, &h, &m, &sec) >= 3) {
    ct->year = Y; ct->month = M; ct->day = D;
    ct->hour = h; ct->minute = m; ct->second = sec;
    ct->subsecond_nanos = 0;
    size_t dot = s.find('.');
    if (dot != std::string_view::npos) {
      size_t end_digits = dot + 1;
      while (end_digits < s.size() && s[end_digits] >= '0' && s[end_digits] <= '9') {
        ++end_digits;
      }
      std::string frac_str(s.substr(dot + 1, end_digits - (dot + 1)));
      while (frac_str.size() < 9) { frac_str.push_back('0'); }
      if (frac_str.size() > 9) { frac_str = frac_str.substr(0, 9); }
      ct->subsecond_nanos = std::stoll(frac_str);
    }
    matched = true;
  } else if (sscanf(s.data(), "%d:%d:%d", &h, &m, &sec) >= 3) {
    ct->hour = h; ct->minute = m; ct->second = sec;
    ct->subsecond_nanos = 0;
    size_t dot = s.find('.');
    if (dot != std::string_view::npos) {
      size_t end_digits = dot + 1;
      while (end_digits < s.size() && s[end_digits] >= '0' && s[end_digits] <= '9') {
        ++end_digits;
      }
      std::string frac_str(s.substr(dot + 1, end_digits - (dot + 1)));
      while (frac_str.size() < 9) { frac_str.push_back('0'); }
      if (frac_str.size() > 9) { frac_str = frac_str.substr(0, 9); }
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
        std::chrono::year_month_day ymd{std::chrono::year{ct->year},
                                        std::chrono::month{static_cast<unsigned>(ct->month)},
                                        std::chrono::day{static_cast<unsigned>(ct->day)}};
        int64_t days = std::chrono::sys_days{ymd}.time_since_epoch().count() + extra_days;
        std::chrono::sys_days new_sd{std::chrono::days{days}};
        std::chrono::year_month_day new_ymd{new_sd};
        ct->year = int(new_ymd.year());
        ct->month = unsigned(new_ymd.month());
        ct->day = unsigned(new_ymd.day());
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
    std::chrono::year_month_day ymd{std::chrono::year{res.year},
                                    std::chrono::month{static_cast<unsigned>(res.month)},
                                    std::chrono::day{static_cast<unsigned>(res.day)}};
    int64_t days = std::chrono::sys_days{ymd}.time_since_epoch().count() + day_diff;
    std::chrono::sys_days new_sd{std::chrono::days{days}};
    std::chrono::year_month_day new_ymd{new_sd};
    res.year = int(new_ymd.year());
    res.month = unsigned(new_ymd.month());
    res.day = unsigned(new_ymd.day());
  }
  return res;
}

int ParseTimeZoneOffset(std::string_view tz_str, const CivilTime* ct = nullptr, int default_offset = 0) {
  if (tz_str.empty()) { return default_offset; }
  if (tz_str == "UTC" || tz_str == "GMT" || tz_str == "utc" || tz_str == "gmt" ||
      tz_str == "Z" || tz_str == "z" || tz_str == "Etc/Greenwich" || tz_str == "Etc/UTC" || tz_str == "Etc/GMT") {
    return 0;
  }
  if (tz_str.starts_with("UTC+") || tz_str.starts_with("UTC-") ||
      tz_str.starts_with("GMT+") || tz_str.starts_with("GMT-")) {
    char sign = tz_str[3];
    int h = 0, m = 0;
    std::string rem(tz_str.substr(4));
    if (rem.find(':') != std::string::npos) {
      sscanf(rem.c_str(), "%d:%d", &h, &m);
    } else if (rem.size() == 4) {
      sscanf(rem.c_str(), "%2d%2d", &h, &m);
    } else {
      sscanf(rem.c_str(), "%d", &h);
    }
    return (h * 3600 + m * 60) * (sign == '-' ? -1 : 1);
  }
  if (tz_str[0] == '+' || tz_str[0] == '-') {
    char sign = tz_str[0];
    int h = 0, m = 0;
    std::string rem(tz_str.substr(1));
    if (rem.find(':') != std::string::npos) {
      sscanf(rem.c_str(), "%d:%d", &h, &m);
    } else if (rem.size() == 4) {
      sscanf(rem.c_str(), "%2d%2d", &h, &m);
    } else {
      sscanf(rem.c_str(), "%d", &h);
    }
    return (h * 3600 + m * 60) * (sign == '-' ? -1 : 1);
  }
  std::string zone_name(tz_str);
  if (zone_name == "NZ-CHAT") { zone_name = "Pacific/Chatham"; }
  try {
    const auto* zone = std::chrono::locate_zone(zone_name);
    if (zone) {
      int y = ct ? ct->year : 2000;
      int mon = ct ? ct->month : 1;
      int d = ct ? ct->day : 1;
      int h = ct ? ct->hour : 0;
      int min = ct ? ct->minute : 0;
      int s = ct ? ct->second : 0;
      if (y < 1970) { y = 1970; }
      std::chrono::year_month_day ymd{std::chrono::year{y},
                                      std::chrono::month{static_cast<unsigned>(mon)},
                                      std::chrono::day{static_cast<unsigned>(d)}};
      std::chrono::local_days loc_d{ymd};
      auto loc_tp = loc_d + std::chrono::hours{h} +
                    std::chrono::minutes{min} + std::chrono::seconds{s};
      auto loc_info = zone->get_info(loc_tp);
      return static_cast<int>(loc_info.first.offset.count());
    }
  } catch (...) {}
  return default_offset;
}

CivilTime ValueToCivilTime(const Value& val) {
  CivilTime ct;
  if (val.type == ValueType::kDate) {
    std::chrono::sys_days sys_d{std::chrono::days{val.DateDays()}};
    std::chrono::year_month_day ymd{sys_d};
    ct.year = int(ymd.year());
    ct.month = unsigned(ymd.month());
    ct.day = unsigned(ymd.day());
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
  char buf[64];
  if (ct.subsecond_nanos != 0) {
    if (ct.subsecond_nanos % 1000000 == 0) {
      snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%03ld",
               ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second, ct.subsecond_nanos / 1000000);
    } else if (ct.subsecond_nanos % 1000 == 0) {
      snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%06ld",
               ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second, ct.subsecond_nanos / 1000);
    } else {
      snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%09ld",
               ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second, ct.subsecond_nanos);
    }
  } else {
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
             ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second);
  }
  return std::string(buf);
}

std::string FormatTimeZoneOffset(int tz_offset_sec) {
  char buf[16];
  int abs_sec = std::abs(tz_offset_sec);
  int h = abs_sec / 3600;
  int m = (abs_sec % 3600) / 60;
  char sign = tz_offset_sec < 0 ? '-' : '+';
  if (m == 0) {
    snprintf(buf, sizeof(buf), "%c%02d", sign, h);
  } else {
    snprintf(buf, sizeof(buf), "%c%02d:%02d", sign, h, m);
  }
  return std::string(buf);
}

Value AddOrSubInterval(const std::string& func_name, const Value& date,
                       const IntervalExpression& interval) {
  const int64_t amount =
      func_name == "date_sub" ? -interval.Amount() : interval.Amount();
  const int64_t days = date.type == ValueType::kDate
                           ? date.DateDays()
                           : ParseDateDays(date.value.varchar_value);
  const int64_t result = AddDateIntervalDays(days, amount, interval.Unit());
  return date.type == ValueType::kDate ? Value::DateFromDays(result)
                                       : Value(FormatDateDays(result));
}

Value ExecuteFunction(const std::string& name,
                      const std::vector<Value>& values) {
  auto raw_str = [](const Value& val) -> std::string {
    if (val.type == ValueType::kVarChar) {
      return std::string(val.value.varchar_value);
    }
    return val.AsString();
  };

  if (name == "rand") {
    if (!values.empty()) {
      throw std::runtime_error("RAND requires no arguments");
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
      if (!val.IsNull()) { return val; }
    }
    return {};
  }
  if (name == "concat") {
    std::string result;
    for (const auto& value : values) {
      if (value.IsNull()) { return {}; }
      if (value.type != ValueType::kVarChar) {
        throw std::runtime_error("CONCAT currently requires string arguments");
      }
      result.append(value.value.varchar_value);
    }
    return Value(std::move(result));
  }
  if (name == "substr" || name == "substring") {
    if (values.size() < 2 || values.size() > 3) {
      throw std::runtime_error("SUBSTR requires two or three arguments");
    }
    if (values[0].IsNull() || values[1].IsNull() ||
        (values.size() == 3 && values[2].IsNull())) {
      return {};
    }
    if (values[0].type != ValueType::kVarChar ||
        values[1].type != ValueType::kInt64 ||
        (values.size() == 3 && values[2].type != ValueType::kInt64)) {
      throw std::runtime_error("SUBSTR argument type mismatch");
    }
    const std::string input(values[0].value.varchar_value);
    const int64_t start = values[1].value.int_value;
    if (values.size() == 3 && values[2].type == ValueType::kInt64) {
      if (values[2].value.int_value < 0) {
        throw std::runtime_error("SUBSTR length cannot be negative");
      }
      if (values[2].value.int_value == 0) {
        return Value(std::string());
      }
    }
    const size_t begin = start <= 1 ? 0 : static_cast<size_t>(start - 1);
    const size_t length = values.size() == 3
                              ? static_cast<size_t>(values[2].value.int_value)
                              : std::string::npos;

    if (begin >= input.size()) { return Value(std::string()); }
    return Value(input.substr(begin, length));
  }
  if (name == "extract_year" || name == "extract_month" ||
      name == "extract_day") {
    if (values.size() != 1) {
      throw std::runtime_error("EXTRACT requires one argument");
    }
    if (values[0].IsNull()) { return {}; }
    if (values[0].type != ValueType::kDate &&
        values[0].type != ValueType::kVarChar) {
      throw std::runtime_error("EXTRACT requires DATE or STRING");
    }
    const std::string date = values[0].type == ValueType::kDate
                                 ? values[0].AsString()
                                 : std::string(values[0].value.varchar_value);
    if (date.size() < 10) { throw std::runtime_error("invalid DATE value"); }
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
      throw std::runtime_error("invalid DATE value: " + date);
    }
    return Value(part);
  }
  if (name == "current_timestamp") {
    if (!values.empty()) {
      throw std::runtime_error("CURRENT_TIMESTAMP takes no arguments");
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
    if (values.size() > 1) { throw std::runtime_error("CURRENT_DATETIME takes at most 1 argument"); }
    if (values.size() == 1 && values[0].IsNull()) { return {}; }
    int tz_offset_sec = ParseTimeZoneOffset(GetDefaultTimeZone());
    if (values.size() == 1 && !values[0].IsNull()) {
      std::string tz_str = raw_str(values[0]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        throw std::runtime_error("invalid timezone: " + tz_str);
      }
      tz_offset_sec = ParseTimeZoneOffset(tz_str);
    }
    time_t now = time(nullptr) + tz_offset_sec;
    struct tm t = {};
    gmtime_r(&now, &t);
    char buf[32];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
             t.tm_year + 1900, t.tm_mon + 1, t.tm_mday,
             t.tm_hour, t.tm_min, t.tm_sec);
    return Value(std::string(buf));
  }
  if (name == "current_date") {
    if (values.size() > 1) { throw std::runtime_error("CURRENT_DATE takes at most 1 argument"); }
    if (values.size() == 1 && values[0].IsNull()) { return {}; }
    int tz_offset_sec = ParseTimeZoneOffset(GetDefaultTimeZone());
    if (values.size() == 1 && !values[0].IsNull()) {
      std::string tz_str = raw_str(values[0]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        throw std::runtime_error("invalid timezone: " + tz_str);
      }
      tz_offset_sec = ParseTimeZoneOffset(tz_str);
    }
    time_t now = time(nullptr) + tz_offset_sec;
    struct tm t = {};
    gmtime_r(&now, &t);
    std::chrono::year_month_day ymd{std::chrono::year{t.tm_year + 1900},
                                    std::chrono::month{static_cast<unsigned>(t.tm_mon + 1)},
                                    std::chrono::day{static_cast<unsigned>(t.tm_mday)}};
    return Value::DateFromDays(std::chrono::sys_days{ymd}.time_since_epoch().count());
  }
  if (name == "string") {
    if (values.empty() || values.size() > 2) { throw std::runtime_error("STRING requires 1 or 2 arguments"); }
    if (values[0].IsNull() || (values.size() == 2 && values[1].IsNull())) { return {}; }
    if (values.size() == 2) {
      CivilTime ct = ValueToCivilTime(values[0]);
      std::string tz_str = raw_str(values[1]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        throw std::runtime_error("invalid timezone: " + tz_str);
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
  if (name == "format_timestamp" || name == "format_datetime" || name == "format_date") {
    if (values.size() < 2 || values.size() > 3) {
      throw std::runtime_error(name + " takes 2 or 3 arguments");
    }
    if (values[0].IsNull() || values[1].IsNull()) { return {}; }
    std::string fmt = raw_str(values[0]);
    CivilTime ct = ValueToCivilTime(values[1]);
    int tz_offset_sec = 0;
    if (values.size() == 3) {
      if (values[2].IsNull()) { return {}; }
      std::string tz_str = raw_str(values[2]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        throw std::runtime_error("invalid timezone: " + tz_str);
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
    char buf[128];
    strftime(buf, sizeof(buf), fmt.c_str(), &tm);
    return Value(std::string(buf));
  }
  if (name == "parse_timestamp") {
    if (values.size() < 2 || values.size() > 3) {
      throw std::runtime_error("PARSE_TIMESTAMP requires 2 or 3 arguments");
    }
    if (values[0].IsNull() || values[1].IsNull()) { return {}; }
    if (values.size() == 3 && values[2].IsNull()) { return {}; }
    std::string fmt = raw_str(values[0]);
    std::string input = raw_str(values[1]);
    int tz_offset_sec = ParseTimeZoneOffset(GetDefaultTimeZone(), nullptr, -8 * 3600);
    if (values.size() == 3) {
      std::string tz_str = raw_str(values[2]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        throw std::runtime_error("invalid timezone: " + tz_str);
      }
      tz_offset_sec = ParseTimeZoneOffset(tz_str);
    }
    struct tm tm = {};
    tm.tm_year = 100;
    tm.tm_mon = 0; tm.tm_mday = 1;
    char* parsed_end = strptime(input.c_str(), fmt.c_str(), &tm);
    if (parsed_end == nullptr) {
      throw std::runtime_error("PARSE_TIMESTAMP failed for: " + input);
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
  throw std::runtime_error("Function calls are not yet executable: " + name);
}
}  // namespace

std::unordered_set<ColumnName> FunctionCallExpression::TouchedColumns() const {
  std::unordered_set<ColumnName> result;
  for (const auto& arg : args_) {
    result.merge(arg->TouchedColumns());
  }
  return result;
}

Value FunctionCallExpression::Evaluate(const Row& row,
                                       const Schema& schema) const {
  if (func_name_ == "date_add" || func_name_ == "date_sub") {
    if (args_.size() != 2 || args_[1]->Type() != TypeTag::kIntervalExp) {
      throw std::runtime_error("DATE_ADD/DATE_SUB requires DATE and INTERVAL");
    }
    const Value date = args_[0]->Evaluate(row, schema);
    if (date.IsNull()) { return {};
}
    return AddOrSubInterval(func_name_, date,
                            args_[1]->AsIntervalExpression());
  }
  std::vector<Value> values;
  values.reserve(args_.size());
  for (const auto& arg : args_) {
    values.emplace_back(arg->Evaluate(row, schema));
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

Value FunctionCallExpression::Evaluate(const Row* left,
                                       const Schema& left_schema,
                                       const Row* right,
                                       const Schema& right_schema) const {
  if (func_name_ == "date_add" || func_name_ == "date_sub") {
    if (args_.size() != 2 || args_[1]->Type() != TypeTag::kIntervalExp) {
      throw std::runtime_error("DATE_ADD/DATE_SUB requires DATE and INTERVAL");
    }
    const Value date =
        args_[0]->Evaluate(left, left_schema, right, right_schema);
    if (date.IsNull()) { return {};
}
    return AddOrSubInterval(func_name_, date,
                            args_[1]->AsIntervalExpression());
  }
  std::vector<Value> values;
  values.reserve(args_.size());
  for (const auto& arg : args_) {
    values.emplace_back(arg->Evaluate(left, left_schema, right, right_schema));
  }
  return ExecuteFunction(func_name_, values);
}

// Context-aware form: same dispatch as the plain evaluator with the context
// threaded into every argument (A1 stage 3).
Value FunctionCallExpression::Evaluate(const Row& row, const Schema& schema,
                                       EvaluationContext& context) const {
  if (func_name_ == "date_add" || func_name_ == "date_sub") {
    if (args_.size() != 2 || args_[1]->Type() != TypeTag::kIntervalExp) {
      throw std::runtime_error("DATE_ADD/DATE_SUB requires DATE and INTERVAL");
    }
    const Value date = args_[0]->Evaluate(row, schema, context);
    if (date.IsNull()) { return {};
}
    return AddOrSubInterval(func_name_, date,
                            args_[1]->AsIntervalExpression());
  }
  std::vector<Value> values;
  values.reserve(args_.size());
  for (const auto& arg : args_) {
    values.emplace_back(arg->Evaluate(row, schema, context));
  }
  return ExecuteFunction(func_name_, values);
}

Type FunctionCallExpression::ResultType(const Schema& schema) const {
  if (func_name_ == "coalesce" || func_name_ == "nullif" ||
      func_name_ == "ifnull" || func_name_ == "greatest" ||
      func_name_ == "least") {
    if (args_.empty()) { return {TypeTag::kInvalid}; }
    return args_[0]->ResultType(schema);
  }
  if (func_name_ == "if") {
    if (args_.size() < 2) { return {TypeTag::kInvalid}; }
    return args_[1]->ResultType(schema);
  }
  if (func_name_ == "split" || func_name_ == "regexp_extract_all" || func_name_.ends_with("_array")) {
    return {TypeTag::kArray};
  }
  if (func_name_ == "concat" || func_name_ == "current_timestamp" ||
      func_name_ == "format" ||
      func_name_ == "substr" || func_name_ == "substring" ||
      func_name_ == "upper" || func_name_ == "lower" ||
      func_name_ == "trim" || func_name_ == "ltrim" || func_name_ == "rtrim" ||
      func_name_ == "replace" || func_name_ == "repeat" ||
      func_name_ == "reverse" || func_name_ == "split_substr" ||
      func_name_ == "byte_substr" || func_name_ == "byte_reverse" ||
      func_name_ == "code_points_to_string" || func_name_ == "code_points_to_bytes" ||
      func_name_ == "octet_length" ||
      func_name_ == "left" || func_name_ == "right" ||
      func_name_ == "lpad" || func_name_ == "rpad" ||
      func_name_ == "initcap" || func_name_ == "chr" ||
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
      func_name_ == "regexp_instr" ||
      func_name_ == "div" ||
      func_name_.starts_with("extract_")) {
    return {TypeTag::kBigInt};
  }

  if (func_name_ == "abs" || func_name_ == "sign" || func_name_ == "round" ||
      func_name_ == "trunc" || func_name_ == "truncate" ||
      func_name_ == "ceil" || func_name_ == "ceiling" ||
      func_name_ == "floor" || func_name_ == "mod" ||
      func_name_ == "safe_add" || func_name_ == "safe_subtract" ||
      func_name_ == "safe_multiply" || func_name_ == "safe_negate") {
    if (args_.empty()) { return {TypeTag::kBigInt}; }
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
  if (func_name_ == "coalesce" || func_name_ == "nullif" ||
      func_name_ == "ifnull" || func_name_ == "greatest" ||
      func_name_ == "least") {
    if (args_.empty()) { return {TypeTag::kInvalid}; }
    return args_[0]->ResultType(left, right);
  }
  if (func_name_ == "if") {
    if (args_.size() < 2) { return {TypeTag::kInvalid}; }
    return args_[1]->ResultType(left, right);
  }
  if (func_name_ == "split" || func_name_ == "regexp_extract_all" || func_name_.ends_with("_array")) {
    return {TypeTag::kArray};
  }
  if (func_name_ == "concat" || func_name_ == "current_timestamp" ||
      func_name_ == "format" ||
      func_name_ == "substr" || func_name_ == "substring" ||
      func_name_ == "upper" || func_name_ == "lower" ||
      func_name_ == "trim" || func_name_ == "ltrim" || func_name_ == "rtrim" ||
      func_name_ == "replace" || func_name_ == "repeat" ||
      func_name_ == "reverse" || func_name_ == "split_substr" ||
      func_name_ == "byte_substr" || func_name_ == "byte_reverse" ||
      func_name_ == "code_points_to_string" || func_name_ == "code_points_to_bytes" ||
      func_name_ == "octet_length" ||
      func_name_ == "left" || func_name_ == "right" ||
      func_name_ == "lpad" || func_name_ == "rpad" ||
      func_name_ == "initcap" || func_name_ == "chr" ||
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
      func_name_ == "regexp_instr" ||
      func_name_ == "div" ||
      func_name_.starts_with("extract_")) {
    return {TypeTag::kBigInt};
  }

  if (func_name_ == "abs" || func_name_ == "sign" || func_name_ == "round" ||
      func_name_ == "trunc" || func_name_ == "truncate" ||
      func_name_ == "ceil" || func_name_ == "ceiling" ||
      func_name_ == "floor" || func_name_ == "mod" ||
      func_name_ == "safe_add" || func_name_ == "safe_subtract" ||
      func_name_ == "safe_multiply" || func_name_ == "safe_negate") {
    if (args_.empty()) { return {TypeTag::kBigInt}; }
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
  return context.GetOrAddFunction(func_name_,
                                  static_cast<int>(args_.size()));
}

}  // namespace tinylamb
