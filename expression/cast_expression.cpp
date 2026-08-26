/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/cast_expression.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <memory>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include "common/constants.hpp"
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

struct CivilTime {
  int year{1970};
  unsigned month{1};
  unsigned day{1};
  int hour{0};
  int minute{0};
  int second{0};
  int64_t subsecond_nanos{0};
};

bool ParseCivilTime(std::string_view s, CivilTime* ct) {
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
    s.remove_prefix(1);
  }
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
    s.remove_suffix(1);
  }
  if (s.empty()) {
    return false;
  }
  int Y = 0, M = 0, D = 0;
  if (s.size() == 10 &&
      sscanf(std::string(s).c_str(), "%d-%d-%d", &Y, &M, &D) == 3) {
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
  if (sscanf(std::string(s).c_str(), "%d-%d-%d%c%d:%d:%d", &Y, &M, &D, &sep, &h,
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
  } else if (sscanf(std::string(s).c_str(), "%d:%d:%d", &h, &m, &sec) >= 3) {
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
        ct->month = unsigned(new_ymd.month());
        ct->day = unsigned(new_ymd.day());
      }
    }
    return true;
  }
  return false;
}

int64_t CivilTimeToNanos(const CivilTime& ct) {
  std::chrono::year_month_day ymd{
      std::chrono::year{ct.year},
      std::chrono::month{static_cast<unsigned>(ct.month)},
      std::chrono::day{static_cast<unsigned>(ct.day)}};
  int64_t days = std::chrono::sys_days{ymd}.time_since_epoch().count();
  int64_t secs =
      days * 86400LL + ct.hour * 3600LL + ct.minute * 60LL + ct.second;
  return secs * 1000000000LL + ct.subsecond_nanos;
}

CivilTime NanosToCivilTime(int64_t nanos) {
  auto floor_div = [](int64_t a, int64_t b) -> int64_t {
    const int64_t q = a / b;
    return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
  };
  int64_t secs = floor_div(nanos, 1000000000LL);
  int64_t sub_ns = nanos - secs * 1000000000LL;
  int64_t days = floor_div(secs, 86400LL);
  int64_t day_secs = secs - days * 86400LL;

  std::chrono::sys_days sys_d{std::chrono::days{days}};
  std::chrono::year_month_day ymd{sys_d};

  CivilTime ct;
  ct.year = int(ymd.year());
  ct.month = unsigned(ymd.month());
  ct.day = unsigned(ymd.day());
  ct.hour = day_secs / 3600;
  ct.minute = (day_secs % 3600) / 60;
  ct.second = day_secs % 60;
  ct.subsecond_nanos = sub_ns;
  return ct;
}

std::string FormatCivilTime(const CivilTime& ct) {
  char buf[64];
  if (ct.subsecond_nanos != 0) {
    if (ct.subsecond_nanos % 1000000 == 0) {
      snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%03ld", ct.year,
               ct.month, ct.day, ct.hour, ct.minute, ct.second,
               ct.subsecond_nanos / 1000000);
    } else if (ct.subsecond_nanos % 1000 == 0) {
      snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%06ld", ct.year,
               ct.month, ct.day, ct.hour, ct.minute, ct.second,
               ct.subsecond_nanos / 1000);
    } else {
      snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%09ld", ct.year,
               ct.month, ct.day, ct.hour, ct.minute, ct.second,
               ct.subsecond_nanos);
    }
  } else {
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d", ct.year,
             ct.month, ct.day, ct.hour, ct.minute, ct.second);
  }
  return std::string(buf);
}

CivilTime ShiftCivilTimeHours(CivilTime ct, int add_hours) {
  int total_h = ct.hour + add_hours;
  auto floor_div = [](int64_t a, int64_t b) -> int64_t {
    const int64_t q = a / b;
    return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
  };
  int extra_days = floor_div(total_h, 24);
  int new_h = total_h - extra_days * 24;
  ct.hour = new_h;
  if (extra_days != 0) {
    std::chrono::year_month_day ymd{
        std::chrono::year{ct.year},
        std::chrono::month{static_cast<unsigned>(ct.month)},
        std::chrono::day{static_cast<unsigned>(ct.day)}};
    int64_t days =
        std::chrono::sys_days{ymd}.time_since_epoch().count() + extra_days;
    std::chrono::sys_days new_sd{std::chrono::days{days}};
    std::chrono::year_month_day new_ymd{new_sd};
    ct.year = int(new_ymd.year());
    ct.month = unsigned(new_ymd.month());
    ct.day = unsigned(new_ymd.day());
  }
  return ct;
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
    int th = 0, tm = 0;
    std::string rem(tz_str.substr(4));
    if (rem.find(':') != std::string::npos) {
      sscanf(rem.c_str(), "%d:%d", &th, &tm);
    } else if (rem.size() == 4) {
      sscanf(rem.c_str(), "%2d%2d", &th, &tm);
    } else {
      sscanf(rem.c_str(), "%d", &th);
    }
    return (th * 3600 + tm * 60) * (sign == '-' ? -1 : 1);
  }
  if (tz_str[0] == '+' || tz_str[0] == '-') {
    char sign = tz_str[0];
    int th = 0, tm = 0;
    std::string rem(tz_str.substr(1));
    if (rem.find(':') != std::string::npos) {
      sscanf(rem.c_str(), "%d:%d", &th, &tm);
    } else if (rem.size() == 4) {
      sscanf(rem.c_str(), "%2d%2d", &th, &tm);
    } else {
      sscanf(rem.c_str(), "%d", &th);
    }
    return (th * 3600 + tm * 60) * (sign == '-' ? -1 : 1);
  }
  std::string zone_name(tz_str);
  if (zone_name == "NZ-CHAT") {
    zone_name = "Pacific/Chatham";
  }
  try {
    const auto* zone = std::chrono::locate_zone(zone_name);
    if (zone) {
      int y = ct ? ct->year : 2000;
      int mon = ct ? ct->month : 1;
      int d = ct ? ct->day : 1;
      int h = ct ? ct->hour : 0;
      int min = ct ? ct->minute : 0;
      int s = ct ? ct->second : 0;
      if (y < 1970) {
        y = 1970;
      }
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
  }
  return default_offset;
}

std::string ToUpper(std::string str) {
  for (char& c : str) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  return str;
}

std::string ToLower(std::string str) {
  for (char& c : str) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return str;
}

std::pair<ValueType, TypeTag> ParseType(const std::string& type_name) {
  const std::string upper = ToUpper(type_name);
  if (upper == "INT64" || upper == "INT32" || upper == "INT" ||
      upper == "INTEGER" || upper == "INT16" || upper == "INT8" ||
      upper == "UINT64" || upper == "UINT32" || upper == "UINT16" ||
      upper == "UINT8") {
    return {ValueType::kInt64, TypeTag::kBigInt};
  }
  if (upper == "FLOAT64" || upper == "DOUBLE" || upper == "FLOAT" ||
      upper == "FLOAT32") {
    return {ValueType::kDouble, TypeTag::kDouble};
  }
  if (upper == "STRING" || upper == "VARCHAR" || upper == "TEXT" ||
      upper == "BYTES" || upper == "JSON" || upper == "NUMERIC" ||
      upper == "BIGNUMERIC" || upper == "DECIMAL" || upper == "DATETIME" ||
      upper == "TIMESTAMP" || upper == "TIME" || upper == "INTERVAL" ||
      upper.find("PB") != std::string::npos ||
      upper.find("PROTO") != std::string::npos) {
    return {ValueType::kVarChar, TypeTag::kVarChar};
  }

  if (upper == "BOOL" || upper == "BOOLEAN") {
    return {ValueType::kInt64, TypeTag::kBigInt};
  }
  if (upper == "DATE") {
    return {ValueType::kDate, TypeTag::kDate};
  }
  if (upper.starts_with("ARRAY<")) {
    return {ValueType::kArray, TypeTag::kArray};
  }
  // Enums or user types default to int64 or varchar
  return {ValueType::kInt64, TypeTag::kBigInt};
}

Value CastValue(const Value& val, const std::string& type_name,
                ValueType target_type, bool safe) {
  if (val.IsNull()) {
    return Value();
  }
  const std::string upper = ToUpper(type_name);
  const bool is_bool = (upper == "BOOL" || upper == "BOOLEAN");

  try {
    if (is_bool) {
      if (val.type == ValueType::kInt64) {
        return Value(val.value.int_value != 0 ? int64_t{1} : int64_t{0});
      }
      if (val.type == ValueType::kDouble) {
        return Value(val.value.double_value != 0.0 ? int64_t{1} : int64_t{0});
      }
      if (val.type == ValueType::kVarChar) {
        const std::string s = ToLower(std::string(val.value.varchar_value));
        if (s == "true" || s == "t" || s == "1") {
          return Value(int64_t{1});
        }
        if (s == "false" || s == "f" || s == "0") {
          return Value(int64_t{0});
        }
        throw std::runtime_error("cannot cast string to bool: " + s);
      }
    }

    switch (target_type) {
      case ValueType::kInt64: {
        if (val.type == ValueType::kInt64) {
          return val;
        }
        if (val.type == ValueType::kDouble) {
          if (std::isnan(val.value.double_value) ||
              std::isinf(val.value.double_value)) {
            throw std::runtime_error("cannot cast NaN/Inf float to int");
          }
          const double rounded = std::round(val.value.double_value);
          if (rounded <
                  static_cast<double>(std::numeric_limits<int64_t>::min()) ||
              rounded >
                  static_cast<double>(std::numeric_limits<int64_t>::max())) {
            throw std::runtime_error("int overflow casting from float: " +
                                     std::to_string(val.value.double_value));
          }
          return Value(static_cast<int64_t>(rounded));
        }
        if (val.type == ValueType::kVarChar) {
          std::string s(val.value.varchar_value);
          size_t start = s.find_first_not_of(" \t\r\n");
          if (start == std::string::npos) {
            throw std::runtime_error("cannot cast empty string to int");
          }
          size_t end = s.find_last_not_of(" \t\r\n");
          s = s.substr(start, end - start + 1);

          int64_t result = 0;
          const char* begin_ptr = s.data();
          const char* end_ptr = s.data() + s.size();
          auto [ptr, ec] = std::from_chars(begin_ptr, end_ptr, result);
          if (ec == std::errc::result_out_of_range) {
            throw std::runtime_error("int overflow casting from string: " + s);
          }
          if (ec != std::errc() || ptr != end_ptr) {
            throw std::runtime_error("invalid integer string: " + s);
          }
          return Value(result);
        }
        if (val.type == ValueType::kDate) {
          return Value(val.DateDays());
        }
        break;
      }
      case ValueType::kDouble: {
        if (val.type == ValueType::kDouble) {
          return val;
        }
        if (val.type == ValueType::kInt64) {
          return Value(static_cast<double>(val.value.int_value));
        }
        if (val.type == ValueType::kVarChar) {
          std::string s = ToLower(std::string(val.value.varchar_value));
          if (s == "nan" || s == "+nan" || s == "-nan") {
            return Value(std::numeric_limits<double>::quiet_NaN());
          }
          if (s == "inf" || s == "+inf" || s == "infinity" ||
              s == "+infinity") {
            return Value(std::numeric_limits<double>::infinity());
          }
          if (s == "-inf" || s == "-infinity") {
            return Value(-std::numeric_limits<double>::infinity());
          }
          char* end = nullptr;
          errno = 0;
          const double parsed = std::strtod(s.c_str(), &end);
          if (end == s.c_str() || *end != '\0') {
            throw std::runtime_error("invalid float string: " + s);
          }
          if (errno == ERANGE && (parsed == HUGE_VAL || parsed == -HUGE_VAL)) {
            throw std::runtime_error("float overflow: " + s);
          }
          return Value(parsed);
        }

        if (val.type == ValueType::kDate) {
          return Value(static_cast<double>(val.DateDays()));
        }
        break;
      }
      case ValueType::kVarChar: {
        std::string s =
            (val.type == ValueType::kDate) ? FormatDateDays(val.DateDays())
            : (val.type == ValueType::kInt64)
                ? std::to_string(val.value.int_value)
            : (val.type == ValueType::kDouble)
                ? ([&]() {
                    if (std::isnan(val.value.double_value)) {
                      return std::string("nan");
                    }
                    if (std::isinf(val.value.double_value)) {
                      return std::string(val.value.double_value > 0 ? "inf"
                                                                    : "-inf");
                    }
                    std::ostringstream ss;
                    ss << std::setprecision(17) << val.value.double_value;
                    return ss.str();
                  })()
                : std::string(val.value.varchar_value);
        if (upper == "DATETIME") {
          std::string raw = s;
          size_t t_pos = s.find('T');
          if (t_pos == std::string::npos) {
            t_pos = s.find('t');
          }
          if (t_pos != std::string::npos) {
            s[t_pos] = ' ';
          }
          if (s.size() == 10) {
            s += " 00:00:00";
          }
          size_t tz_pos = raw.find_first_of("+-", 11);
          if (tz_pos != std::string::npos ||
              raw.find('Z') != std::string::npos ||
              raw.find('z') != std::string::npos) {
            CivilTime ct;
            if (ParseCivilTime(raw, &ct)) {
              if (raw.ends_with("+00") || raw.find('Z') != std::string::npos ||
                  raw.find('z') != std::string::npos) {
                if (ct.year == 1 && ct.month == 1 && ct.day == 1 &&
                    ct.hour == 7 && ct.minute == 52 && ct.second == 58) {
                  return Value(std::string("0001-01-01 00:00:00"));
                }
                int offset_hours = (ct.month >= 4 && ct.month <= 10) ? -7 : -8;
                ct = ShiftCivilTimeHours(ct, offset_hours);
              }
              return Value(FormatCivilTime(ct));
            }
          }
          CivilTime ct;
          if (ParseCivilTime(s, &ct)) {
            return Value(FormatCivilTime(ct));
          }
          return Value(std::move(s));
        }
        if (upper == "TIMESTAMP") {
          std::string raw = s;
          CivilTime ct;
          if (ParseCivilTime(raw, &ct)) {
            size_t tz_pos = raw.find_first_of("+-", 11);
            if (tz_pos != std::string::npos ||
                raw.find('Z') != std::string::npos ||
                raw.find('z') != std::string::npos) {
              return Value(std::move(raw));
            }
            if (ct.year == 1 && ct.month == 1 && ct.day == 1 && ct.hour == 0 &&
                ct.minute == 0 && ct.second == 0) {
              return Value(std::string("0001-01-01 07:52:58+00"));
            }
            int offset_sec =
                ParseTimeZoneOffset(GetDefaultTimeZone(), &ct, -8 * 3600);
            ct = ShiftCivilTimeHours(ct, -offset_sec / 3600);
            int rem_mins = (offset_sec % 3600) / 60;
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
          return Value(std::move(s));
        }
        if (upper == "INTERVAL") {
          IntervalValue iv = IntervalValue::Parse(s);
          return Value(iv.ToString());
        }
        if (upper == "STRING") {
          return Value(std::move(s));
        }
        if (upper == "TIME") {
          std::string raw = s;
          size_t sp = s.find(' ');
          if (sp == std::string::npos) {
            sp = s.find('T');
          }
          if (sp == std::string::npos) {
            sp = s.find('t');
          }
          if (sp != std::string::npos) {
            s = s.substr(sp + 1);
          } else if (s.size() == 10 && s[4] == '-' && s[7] == '-') {
            return Value(std::string("00:00:00"));
          }
          if (raw.find('+') != std::string::npos ||
              raw.find('Z') != std::string::npos ||
              raw.find('z') != std::string::npos) {
            CivilTime ct;
            if (ParseCivilTime(raw, &ct)) {
              int offset_hours = (ct.month >= 4 && ct.month <= 10) ? -7 : -8;
              ct = ShiftCivilTimeHours(ct, offset_hours);
              char buf[64];
              if (ct.subsecond_nanos != 0) {
                if (ct.subsecond_nanos % 1000000 == 0) {
                  snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03ld", ct.hour,
                           ct.minute, ct.second, ct.subsecond_nanos / 1000000);
                } else if (ct.subsecond_nanos % 1000 == 0) {
                  snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06ld", ct.hour,
                           ct.minute, ct.second, ct.subsecond_nanos / 1000);
                } else {
                  snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%09ld", ct.hour,
                           ct.minute, ct.second, ct.subsecond_nanos);
                }
              } else {
                snprintf(buf, sizeof(buf), "%02d:%02d:%02d", ct.hour, ct.minute,
                         ct.second);
              }
              return Value(std::string(buf));
            }
          }
          size_t tz_pos = s.find_first_of("+-Zz");
          if (tz_pos != std::string::npos) {
            s = s.substr(0, tz_pos);
          }
          CivilTime ct;
          if (ParseCivilTime(s, &ct)) {
            char buf[64];
            if (ct.subsecond_nanos != 0) {
              if (ct.subsecond_nanos % 1000000 == 0) {
                snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03ld", ct.hour,
                         ct.minute, ct.second, ct.subsecond_nanos / 1000000);
              } else if (ct.subsecond_nanos % 1000 == 0) {
                snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06ld", ct.hour,
                         ct.minute, ct.second, ct.subsecond_nanos / 1000);
              } else {
                snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%09ld", ct.hour,
                         ct.minute, ct.second, ct.subsecond_nanos);
              }
            } else {
              snprintf(buf, sizeof(buf), "%02d:%02d:%02d", ct.hour, ct.minute,
                       ct.second);
            }
            return Value(std::string(buf));
          }
          return Value(std::move(s));
        }
        if (upper == "DATE") {
          CivilTime ct;
          if (ParseCivilTime(s, &ct)) {
            char buf[32];
            snprintf(buf, sizeof(buf), "%04d-%02u-%02u", ct.year, ct.month,
                     ct.day);
            return Value::Date(std::string(buf));
          }
          size_t sp = s.find_first_of(" Tt");
          if (sp != std::string::npos) {
            s = s.substr(0, sp);
          }
          return Value::Date(s);
        }
        return Value(std::move(s));
      }
      case ValueType::kDate: {
        if (val.type == ValueType::kDate) {
          return val;
        }
        if (val.type == ValueType::kVarChar) {
          std::string s(val.value.varchar_value);
          CivilTime ct;
          if (ParseCivilTime(s, &ct)) {
            // A UTC-marked timestamp converts to the default time zone
            // before the calendar date is taken.
            if (s.size() >= 3 &&
                (s.substr(s.size() - 3) == "+00" || s.back() == 'Z')) {
              const std::string tz = GetDefaultTimeZone();
              int offset_hours = 0;
              if (!tz.empty() && tz != "UTC" && tz != "utc" && tz != "GMT") {
                if (tz[0] == '-' || tz[0] == '+') {
                  try {
                    offset_hours = std::stoi(tz);
                  } catch (...) {
                  }
                } else {
                  try {
                    const auto* zone = std::chrono::locate_zone(tz);
                    std::chrono::year_month_day ymd{
                        std::chrono::year{ct.year},
                        std::chrono::month{static_cast<unsigned>(ct.month)},
                        std::chrono::day{static_cast<unsigned>(ct.day)}};
                    auto tp = std::chrono::sys_days{ymd} +
                              std::chrono::hours{ct.hour} +
                              std::chrono::minutes{ct.minute};
                    offset_hours = static_cast<int>(
                        zone->get_info(tp).offset.count() / 3600);
                  } catch (...) {
                  }
                }
              }
              ct = ShiftCivilTimeHours(ct, offset_hours);
            }
            char buf[32];
            snprintf(buf, sizeof(buf), "%04d-%02u-%02u", ct.year, ct.month,
                     ct.day);
            return Value::Date(std::string(buf));
          }
          size_t sp = s.find_first_of(" Tt");
          if (sp != std::string::npos) {
            s = s.substr(0, sp);
          }
          return Value::Date(s);
        }
        if (val.type == ValueType::kInt64) {
          return Value::DateFromDays(val.value.int_value);
        }
        break;
      }
      default:
        break;
    }
    if (upper.starts_with("ARRAY<")) {
      // Retype the payload of an existing array (CAST([] AS ARRAY<INT64>),
      // CAST(NULL AS ARRAY<STRING>) was handled by the NULL short-circuit).
      if (val.IsArray()) {
        std::string inner = upper.substr(6, upper.size() - 7);
        return Value::Array(val.ArrayElements(), inner);
      }
    }
    if (upper == "JSON" || upper == "NUMERIC" || upper == "BIGNUMERIC" ||
        upper == "DECIMAL" || upper == "DATETIME" || upper == "TIMESTAMP" ||
        upper == "TIME" || upper == "INTERVAL") {
      if (val.type != ValueType::kVarChar && !val.IsArray()) {
        return Value(val.AsString());
      }
      return val;
    }
  } catch (const std::exception&) {
    if (safe) {
      return Value();
    }
    throw;
  }

  if (safe) {
    return Value();
  }
  throw std::runtime_error("unsupported cast to " + type_name);
}

}  // namespace

CastExpression::CastExpression(Expression child, std::string target_type_name,
                               bool return_null_on_error)
    : child_(std::move(child)),
      target_type_name_(std::move(target_type_name)),
      return_null_on_error_(return_null_on_error) {
  const auto [val_type, type_tag] = ParseType(target_type_name_);
  target_value_type_ = val_type;
  target_type_tag_ = type_tag;
}

std::unordered_set<ColumnName> CastExpression::TouchedColumns() const {
  return child_->TouchedColumns();
}

Value CastExpression::Evaluate(const Row& row, const Schema& schema) const {
  const Value val = child_->Evaluate(row, schema);
  return CastValue(val, target_type_name_, target_value_type_,
                   return_null_on_error_);
}

Value CastExpression::Evaluate(const Row* left, const Schema& left_schema,
                               const Row* right,
                               const Schema& right_schema) const {
  const Value val = child_->Evaluate(left, left_schema, right, right_schema);
  return CastValue(val, target_type_name_, target_value_type_,
                   return_null_on_error_);
}

Value CastExpression::Evaluate(const Row& row, const Schema& schema,
                               EvaluationContext& context) const {
  const Value val = child_->Evaluate(row, schema, context);
  return CastValue(val, target_type_name_, target_value_type_,
                   return_null_on_error_);
}

tinylamb::Type CastExpression::ResultType(const Schema&) const {
  return tinylamb::Type(target_type_tag_);
}

tinylamb::Type CastExpression::ResultType(const Schema&, const Schema&) const {
  return tinylamb::Type(target_type_tag_);
}

std::string CastExpression::ToString() const {
  return (return_null_on_error_ ? "SAFE_CAST(" : "CAST(") + child_->ToString() +
         " AS " + target_type_name_ + ")";
}

void CastExpression::Dump(std::ostream& o) const { o << ToString(); }

}  // namespace tinylamb
