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
#include "common/regex_guard.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
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
    if (rem.find(':') != std::string::npos) {
      sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback is intended.
          rem.c_str(), "%d:%d", &h, &m);
    } else if (rem.size() == 4) {
      sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback is intended.
          rem.c_str(), "%2d%2d", &h, &m);
    } else {
      sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback is intended.
          rem.c_str(), "%d", &h);
    }
    return (h * 3600 + m * 60) * (sign == '-' ? -1 : 1);
  }
  if (tz_str[0] == '+' || tz_str[0] == '-') {
    char sign = tz_str[0];
    int h = 0, m = 0;
    std::string rem(tz_str.substr(1));
    if (rem.find(':') != std::string::npos) {
      sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback is intended.
          rem.c_str(), "%d:%d", &h, &m);
    } else if (rem.size() == 4) {
      sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback is intended.
          rem.c_str(), "%2d%2d", &h, &m);
    } else {
      sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback is intended.
          rem.c_str(), "%d", &h);
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

// CURRENT_DATE('foo') and friends must reject unknown zone names the way
// GoogleSQL does instead of silently falling back to the session default.
Status TryValidateTimeZoneName(std::string_view tz_str) {
  if (tz_str.empty()) {
    return StatusError(StatusCode::kInvalidArgument, "invalid timezone: empty");
  }
  if (tz_str == "UTC" || tz_str == "GMT" || tz_str == "utc" ||
      tz_str == "gmt" || tz_str == "Z" || tz_str == "z" ||
      tz_str == "Etc/Greenwich" || tz_str == "Etc/UTC" || tz_str == "Etc/GMT") {
    return Status::kSuccess;
  }
  if (tz_str.starts_with("UTC+") || tz_str.starts_with("UTC-") ||
      tz_str.starts_with("GMT+") || tz_str.starts_with("GMT-")) {
    return Status::kSuccess;
  }
  if (tz_str[0] == '+' || tz_str[0] == '-') {
    return Status::kSuccess;
  }
  std::string zone_name(tz_str);
  if (zone_name == "NZ-CHAT") {
    zone_name = "Pacific/Chatham";
  }
  try {
    static_cast<void>(std::chrono::locate_zone(zone_name));
    return Status::kSuccess;
  } catch (...) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid timezone: " + std::string(tz_str));
  }

  return Status::kSuccess;
}

std::string FormatTimeZoneOffset(int tz_offset_sec) {
  std::array<char, 16> buf{};
  int abs_sec = std::abs(tz_offset_sec);
  int h = abs_sec / 3600;
  int m = (abs_sec % 3600) / 60;
  char sign = tz_offset_sec < 0 ? '-' : '+';
  if (m == 0) {
    (void)snprintf(buf.data(), buf.size(), "%c%02d", sign, h);
  } else {
    (void)snprintf(buf.data(), buf.size(), "%c%02d:%02d", sign, h, m);
  }
  return {buf.data()};
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
  if (val.type == ValueType::kVarChar) {
    if (ParseCivilTime(val.value.varchar_value, &ct)) {
      return ct;
    }
  }
  std::string s = val.AsString();
  if (ParseCivilTime(s, &ct)) {
    return ct;
  }
  return StatusError(StatusCode::kInvalidArgument,
                     "requires DATE, DATETIME or TIMESTAMP");
}

int64_t CivilTimeToNanos(const CivilTime& ct) {
  std::chrono::year_month_day ymd{
      std::chrono::year{ct.year},
      std::chrono::month{static_cast<unsigned>(ct.month)},
      std::chrono::day{static_cast<unsigned>(ct.day)}};
  int64_t days = std::chrono::sys_days{ymd}.time_since_epoch().count();
  int64_t secs =
      (days * 86400LL) + (ct.hour * 3600LL) + (ct.minute * 60LL) + ct.second;
  return (secs * 1000000000LL) + ct.subsecond_nanos;
}

CivilTime NanosToCivilTime(int64_t nanos) {
  auto floor_div = [](int64_t a, int64_t b) -> int64_t {
    const int64_t q = a / b;
    return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
  };
  int64_t secs = floor_div(nanos, 1000000000LL);
  int64_t sub_ns = nanos - (secs * 1000000000LL);
  int64_t days = floor_div(secs, 86400LL);
  int64_t day_secs = secs - (days * 86400LL);

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

std::string FormatCivilTime(const CivilTime& ct, bool include_subsecond = true,
                            bool is_date_only = false) {
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
                     ct.subsecond_nanos / 1000000);
    } else if (ct.subsecond_nanos % 1000 == 0) {
      (void)snprintf(buf.data(), buf.size(),
                     "%04d-%02d-%02d %02d:%02d:%02d.%06ld", ct.year, ct.month,
                     ct.day, ct.hour, ct.minute, ct.second,
                     ct.subsecond_nanos / 1000);
    } else {
      (void)snprintf(buf.data(), buf.size(),
                     "%04d-%02d-%02d %02d:%02d:%02d.%09ld", ct.year, ct.month,
                     ct.day, ct.hour, ct.minute, ct.second, ct.subsecond_nanos);
    }
    return {buf.data()};
  }
  (void)snprintf(buf.data(), buf.size(), "%04d-%02d-%02d %02d:%02d:%02d",
                 ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second);
  return {buf.data()};
}

// Mutual recursion with Evaluate above; expression trees are the intended
// shape here.
// EXC-SHIM bridge (no-exception-rule-migration.md Phase 6/7): subquery,
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
  // FunctionCallExpression lowercases its name at construction, so this
  // binds the stored copy directly: the per-row re-copy + re-lowercase ran
  // inside the residual-predicate loop of every AST-evaluated scan filter.
  const std::string& name = call.FuncName();
  // IF evaluates only the chosen branch: the untaken branch must not raise
  // (e.g. guarded out-of-range array accesses under IF guards).
  if (name == "if") {
    if (call.Args().size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "IF requires 3 arguments");
    }
    ASSIGN_OR_RETURN(
        Value, condition,
        (TryEvaluate(call.Args()[0], scope, aggregates, context, ctes)));

    const size_t taken = condition.Truthy() ? 1 : 2;
    return TryEvaluate(call.Args()[taken], scope, aggregates, context, ctes);
  }
  // COALESCE stops at the first non-NULL argument.
  if (name == "coalesce") {
    for (const Expression& argument : call.Args()) {
      ASSIGN_OR_RETURN(
          Value, value,
          (TryEvaluate(argument, scope, aggregates, context, ctes)));
      if (!value.IsNull()) {
        return value;
      }
    }
    return Value();
  }
  // Proto-field guards emitted by the GoogleSQL frontend: NEW constructors
  // and SELECT AS <proto> route non-constant repeated-field arrays and enum
  // values through them so invalid data fails execution instead of being
  // silently dropped from the text-format representation.
  if (name == "$proto_repeated_guard") {
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
  if (name == "$proto_field_guard" || name == "$proto_enum_guard") {
    const auto& args = call.Args();
    const size_t expected = name == "$proto_field_guard" ? 3 : 2;
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
      static_cast<void>(checked->TryEvaluate(dummy_row, dummy_schema));
    }
    if (expected == 3) {
      return TryEvaluate(args[2], scope, aggregates, context, ctes);
    }
    return value;
  }
  if (name == "date_add" || name == "date_sub" || name == "datetime_add" ||
      name == "datetime_sub" || name == "timestamp_add" ||
      name == "timestamp_sub") {
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
    if (is_timestamp) {
      int tz_offset_sec =
          ParseTimeZoneOffset(GetDefaultTimeZone(), &ct, -8 * 3600);
      int64_t ns = CivilTimeToNanos(ct) + (tz_offset_sec * 1000000000LL);
      ct = NanosToCivilTime(ns);
    }
    const bool is_date = (name == "date_add" || name == "date_sub" ||
                          base.type == ValueType::kDate);

    if (unit == "year" || unit == "years" || unit == "quarter" ||
        unit == "quarters" || unit == "month" || unit == "months") {
      int64_t amount = is_sub ? -raw_amount : raw_amount;
      int64_t add_m = amount;
      if (unit.starts_with("year")) {
        add_m = amount * 12;
      } else if (unit.starts_with("quarter")) {
        add_m = amount * 3;
      }
      int64_t total_m = (ct.year * 12 + (ct.month - 1)) + add_m;
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
        int tz_offset_sec =
            ParseTimeZoneOffset(GetDefaultTimeZone(), &ct, -8 * 3600);
        int64_t ns = CivilTimeToNanos(ct) - (tz_offset_sec * 1000000000LL);
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
        delta_days = amount * 7LL;
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
    int64_t target_days = cur_days + delta_days + extra_days;
    std::chrono::sys_days target_sys_days{std::chrono::days{target_days}};
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
      int64_t ns = CivilTimeToNanos(res_ct) + ((8 * 3600LL) * 1000000000LL);
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
      return Value(static_cast<double>(value.value.int_value));
    }
    return value;
  };
  if (name == "if") {
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
  if (name == "iferror") {
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
  if (name == "iserror") {
    if (call.Args().size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ISERROR requires 1 argument");
    }
    return Value(
        !TryEvaluateProtected(call.Args()[0], scope, aggregates, context, ctes)
             .HasValue());
  }
  if (name == "nulliferror") {
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
  for (const Expression& argument : call.Args()) {
    ASSIGN_OR_RETURN(Value, argument_value,
                     (TryEvaluate(argument, scope, aggregates, context, ctes)));
    arguments.push_back(std::move(argument_value));
  }

  if (name == "__pipe_concat") {
    if (arguments.size() != 2 || !arguments[0].IsArray()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__pipe_concat requires an array and separator");
    }
    struct Pair {
      std::string a;
      std::string b;
    };
    std::vector<Pair> pairs;
    for (const Value& element : arguments[0].ArrayElements()) {
      if (element.IsNull()) {
        continue;
      }
      const std::string text = raw_str(element);
      if (text.size() < 2 || text.front() != '{' || text.back() != '}') {
        return StatusError(StatusCode::kInvalidArgument,
                           "__pipe_concat requires STRUCT elements");
      }
      Value a;
      Value b;
      const std::string body = text.substr(1, text.size() - 2);
      const std::vector<std::pair<std::string, std::string>> members =
          SplitJsonObjectMembers(body);
      for (const auto& [key, member] : members) {
        Value parsed;
        if (!JsonTextToValue(member, &parsed)) {
          continue;
        }
        if (key == "a") {
          a = std::move(parsed);
        } else if (key == "b") {
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
      result += pair.a + "," + pair.b;
    }
    return Value(std::move(result));
  }

  if (name == "__struct_json__") {
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
    std::string json = "{";
    for (size_t i = 0; i < arguments.size(); i += stride) {
      if (i != 0) {
        json += ",";
      }
      std::string escaped;
      const std::string field_name = raw_str(arguments[i]);
      for (char c : field_name) {
        if (c == '"' || c == '\\') {
          escaped.push_back('\\');
        }
        escaped.push_back(c);
      }
      json += "\"" + escaped + "\":";
      const Value& field_value = arguments[i + 1];
      if (field_value.IsNull()) {
        json += "null";
        continue;
      }
      const bool quoted = triple_form && Truthy(arguments[i + 2]);
      if (!quoted && field_value.type == ValueType::kVarChar &&
          field_value.value.varchar_value == "null") {
        json += "null";
        continue;
      }
      if (field_value.type == ValueType::kDate) {
        json += "\"" + FormatDateDays(field_value.DateDays()) + "\"";
        continue;
      }
      const auto is_struct = [](std::string_view sv) {
        return sv.size() >= 2 && sv.front() == '{' && sv.back() == '}';
      };
      if (!quoted && field_value.type == ValueType::kVarChar &&
          LooksLikeProtoText(field_value.value.varchar_value)) {
        json +=
            "{" +
            NormalizeProtoText(field_value.value.varchar_value).value_or("") +
            "}";
        continue;
      }
      if (quoted || (field_value.type == ValueType::kVarChar &&
                     !is_struct(field_value.value.varchar_value))) {
        std::string text = raw_str(field_value);
        std::string escaped_text;
        for (char c : text) {
          if (c == '"' || c == '\\') {
            escaped_text.push_back('\\');
          }
          escaped_text.push_back(c);
        }
        json += "\"" + escaped_text + "\"";
        continue;
      }
      if (field_value.type == ValueType::kInt64) {
        json += std::to_string(field_value.value.int_value);
      } else if (field_value.type == ValueType::kDouble) {
        const double d = field_value.value.double_value;
        std::array<char, 64> buffer{};
        auto [ptr, ec] =
            std::to_chars(buffer.data(), buffer.data() + buffer.size(), d);
        (void)ec;
        json.append(buffer.data(), static_cast<size_t>(ptr - buffer.data()));
      } else if (field_value.type == ValueType::kVarChar) {
        // Flag said unquoted: emit the text without JSON quoting.
        json += raw_str(field_value);
      } else {
        json += field_value.AsString();
      }
    }
    json += "}";
    return Value(std::move(json));
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
    std::string arg0_str = raw_str(arguments[0]);
    if (arguments[0].type == ValueType::kVarChar &&
        arg0_str.find('-') != std::string::npos &&
        arg0_str.find(' ') != std::string::npos &&
        arg0_str.find('-') < arg0_str.find(' ')) {
      IntervalValue iv = IntervalValue::Parse(arg0_str);
      int64_t y = iv.months / 12;
      int64_t m = iv.months % 12;
      int64_t total_sec = iv.nanos / 1000000000LL;
      int64_t sub_ns = iv.nanos % 1000000000LL;
      int64_t h = total_sec / 3600;
      int64_t min = (total_sec % 3600) / 60;
      int64_t s = total_sec % 60;
      if (name == "extract_year") {
        return Value(y);
      }
      if (name == "extract_month") {
        return Value(m);
      }
      if (name == "extract_day") {
        return Value(iv.days);
      }
      if (name == "extract_hour") {
        return Value(h);
      }
      if (name == "extract_minute") {
        return Value(min);
      }
      if (name == "extract_second") {
        return Value(s);
      }
      if (name == "extract_millisecond") {
        return Value(sub_ns / 1000000LL);
      }
      if (name == "extract_microsecond") {
        return Value(sub_ns / 1000LL);
      }
      if (name == "extract_nanosecond") {
        return Value(sub_ns);
      }
    }
    ASSIGN_OR_RETURN(CivilTime, ct, (TryValueToCivilTime(arguments[0])));
    if (arguments.size() == 2) {
      if (arguments[1].IsNull()) {
        return Value();
      }
      std::string tz_str = raw_str(arguments[1]);
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
    }
    if (name == "extract_date") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      return Value::DateFromDays(
          std::chrono::sys_days{ymd}.time_since_epoch().count());
    }
    if (name == "extract_time") {
      std::array<char, 64> buf{};
      if (ct.subsecond_nanos != 0) {
        if (ct.subsecond_nanos % 1000000 == 0) {
          (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%03ld",
                         ct.hour, ct.minute, ct.second,
                         ct.subsecond_nanos / 1000000);
        } else if (ct.subsecond_nanos % 1000 == 0) {
          (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%06ld",
                         ct.hour, ct.minute, ct.second,
                         ct.subsecond_nanos / 1000);
        } else {
          (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%09ld",
                         ct.hour, ct.minute, ct.second, ct.subsecond_nanos);
        }
      } else {
        (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d", ct.hour,
                       ct.minute, ct.second);
      }
      return Value(std::string(buf.data()));
    }
    if (name == "extract_datetime") {
      return Value(FormatCivilTime(ct));
    }
    if (name == "extract_year") {
      return Value(static_cast<int64_t>(ct.year));
    }
    if (name == "extract_quarter") {
      return Value(static_cast<int64_t>(((ct.month - 1) / 3) + 1));
    }
    if (name == "extract_month") {
      return Value(static_cast<int64_t>(ct.month));
    }
    if (name == "extract_day") {
      return Value(static_cast<int64_t>(ct.day));
    }
    if (name == "extract_hour") {
      return Value(static_cast<int64_t>(ct.hour));
    }
    if (name == "extract_minute") {
      return Value(static_cast<int64_t>(ct.minute));
    }
    if (name == "extract_second") {
      return Value(static_cast<int64_t>(ct.second));
    }
    if (name == "extract_millisecond") {
      return Value(static_cast<int64_t>(ct.subsecond_nanos / 1000000LL));
    }
    if (name == "extract_microsecond") {
      return Value(static_cast<int64_t>(ct.subsecond_nanos / 1000LL));
    }
    if (name == "extract_nanosecond") {
      return Value(ct.subsecond_nanos);
    }
    if (name == "extract_week") {
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
      int week_num = (yday + start_wd) / 7;
      return Value(static_cast<int64_t>(week_num));
    }
    if (name == "extract_dayofweek") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      int wd = static_cast<int>(
          std::chrono::weekday{std::chrono::sys_days{ymd}}.c_encoding());
      return Value(static_cast<int64_t>(wd + 1));
    }
    if (name == "extract_weekday") {
      // BigQuery WEEKDAY: Monday = 1 ... Sunday = 7 (ISO encoding).
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      int wd = static_cast<int>(
          std::chrono::weekday{std::chrono::sys_days{ymd}}.iso_encoding());
      return Value(static_cast<int64_t>(wd));
    }
    if (name == "extract_dayofyear") {
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
    if (name == "extract_isoyear" || name == "extract_isoweek") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      std::chrono::sys_days sd{ymd};
      int iso_wd = static_cast<int>(std::chrono::weekday{sd}.iso_encoding());
      std::chrono::sys_days thu = sd + std::chrono::days{4 - iso_wd};
      std::chrono::year_month_day thu_ymd{thu};
      int isoyear = int(thu_ymd.year());
      if (name == "extract_isoyear") {
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
  if (name == "__struct_set") {
    if (arguments.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__struct_set requires 3 arguments");
    }
    return StructSetField(arguments[0], raw_str(arguments[1]), arguments[2]);
  }
  if (name == "error") {
    // Unreachable in valid plans unless evaluated: raise the requested
    // runtime error (the message text is informational only).
    return StatusError(StatusCode::kInvalidArgument,
                       arguments.empty() || arguments[0].IsNull()
                           ? std::string("ERROR: user-raised")
                           : "generic::out_of_range: " + raw_str(arguments[0]));
  }
  if (name == "coalesce") {
    for (Value& value : arguments) {
      if (!value.IsNull()) {
        return value;
      }
    }
    return Value();
  }
  if (name == "nullif") {
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
  if (name == "ifnull") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "IFNULL requires 2 arguments");
    }
    return !arguments[0].IsNull() ? arguments[0] : arguments[1];
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
      if (s.size() >= 10 && s[4] == '-' && s[7] == '-') {
        return TryParseDateDays(s.substr(0, 10));
      }
      return TryParseDateDays(s);
    }
    return StatusError(StatusCode::kInvalidArgument, "requires DATE");
  };

  if (name == "unix_date") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "UNIX_DATE requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(int64_t, days, (parse_date_val(arguments[0])));
    return Value(days);
  }
  if (name == "date_from_unix_date") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "DATE_FROM_UNIX_DATE requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(int64_t, days, (parse_date_val(arguments[0])));
    return Value::DateFromDays(days);
  }
  if (name == "current_time") {
    if (arguments.size() > 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CURRENT_TIME takes at most 1 argument");
    }
    if (arguments.size() == 1 && arguments[0].IsNull()) {
      return Value();
    }
    int tz_offset_sec = 0;
    if (arguments.size() == 1 && !arguments[0].IsNull()) {
      std::string tz_str = raw_str(arguments[0]);
      if (tz_str == "UTC" || tz_str == "utc" || tz_str == "Z" ||
          tz_str == "z") {
        tz_offset_sec = 0;
      } else if (!tz_str.empty() && (tz_str[0] == '+' || tz_str[0] == '-')) {
        char sign = tz_str[0];
        int h = 0, m = 0;
        if (tz_str.find(':') != std::string::npos) {
          if (sscanf(  // NOLINT(cert-err34-c) - conversion count is checked.
                  tz_str.c_str() + 1, "%d:%d", &h, &m) < 1) {
            return StatusError(StatusCode::kInvalidArgument,
                               "invalid timezone: " + tz_str);
          }
        } else {
          if (sscanf(  // NOLINT(cert-err34-c) - conversion count is checked.
                  tz_str.c_str() + 1, "%d", &h) < 1) {
            return StatusError(StatusCode::kInvalidArgument,
                               "invalid timezone: " + tz_str);
          }
        }
        tz_offset_sec = (h * 3600 + m * 60) * (sign == '-' ? -1 : 1);
      } else {
        return StatusError(StatusCode::kInvalidArgument,
                           "invalid timezone: " + tz_str);
      }
    }
    time_t now = time(nullptr) + tz_offset_sec;
    struct tm t = {};
    gmtime_r(&now, &t);
    std::array<char, 32> buf{};
    (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d", t.tm_hour,
                   t.tm_min, t.tm_sec);
    return Value(std::string(buf.data()));
  }
  if (name == "user" || name == "session_user") {
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
  if (name == "current_datetime") {
    if (arguments.size() > 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CURRENT_DATETIME takes at most 1 argument");
    }
    if (arguments.size() == 1 && arguments[0].IsNull()) {
      return Value();
    }
    int tz_offset_sec = ParseTimeZoneOffset(GetDefaultTimeZone());
    if (arguments.size() == 1 && !arguments[0].IsNull()) {
      std::string tz_str = raw_str(arguments[0]);
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
    if (arguments.size() > 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CURRENT_DATE takes at most 1 argument");
    }
    if (arguments.size() == 1 && arguments[0].IsNull()) {
      return Value();
    }
    int tz_offset_sec = ParseTimeZoneOffset(GetDefaultTimeZone());
    if (arguments.size() == 1 && !arguments[0].IsNull()) {
      std::string tz_str = raw_str(arguments[0]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        return StatusError(StatusCode::kInvalidArgument,
                           "invalid timezone: " + tz_str);
      }
      RETURN_IF_FAIL(TryValidateTimeZoneName(tz_str));
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
    if (arguments[0].type == ValueType::kDate) {
      return Value(FormatDateDays(arguments[0].DateDays()));
    }
    return Value(raw_str(arguments[0]));
  }
  if (name == "date") {
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
      if (s.size() > 10) {
        int Y = 0, M = 0, D = 0, h = 0, m = 0;
        double sec = 0;
        if (sscanf(  // NOLINT(cert-err34-c) - conversion count is checked.
                s.c_str(), "%d-%d-%d %d:%d:%lf", &Y, &M, &D, &h, &m, &sec) >=
            5) {
          int tz_offset_sec = 0;
          bool has_tz = false;
          size_t tz_pos = s.find_first_of("+-", 10);
          if (tz_pos != std::string::npos) {
            char sign = s[tz_pos];
            int tz_h = 0, tz_m = 0;
            if (s.find(':', tz_pos) != std::string::npos) {
              sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback.
                  s.c_str() + tz_pos + 1, "%d:%d", &tz_h, &tz_m);
            } else {
              sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback.
                  s.c_str() + tz_pos + 1, "%d", &tz_h);
            }
            tz_offset_sec = (tz_h * 3600 + tz_m * 60) * (sign == '-' ? -1 : 1);
            has_tz = true;
          } else if (s.ends_with('Z') || s.ends_with('z')) {
            has_tz = true;
          }
          if (arguments.size() == 2) {
            std::string tz2 = raw_str(arguments[1]);
            int tz2_sec = 0;
            if (!tz2.empty() && (tz2[0] == '+' || tz2[0] == '-')) {
              char sign = tz2[0];
              int tz_h = 0, tz_m = 0;
              if (tz2.find(':') != std::string::npos) {
                sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback.
                    tz2.c_str() + 1, "%d:%d", &tz_h, &tz_m);
              } else {
                sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback.
                    tz2.c_str() + 1, "%d", &tz_h);
              }
              tz2_sec = (tz_h * 3600 + tz_m * 60) * (sign == '-' ? -1 : 1);
            }
            struct tm t = {};
            t.tm_year = Y - 1900;
            t.tm_mon = M - 1;
            t.tm_mday = D;
            t.tm_hour = h;
            t.tm_min = m;
            t.tm_sec = static_cast<int>(sec);
            time_t epoch = timegm(&t) - tz_offset_sec + tz2_sec;
            struct tm target = {};
            gmtime_r(&epoch, &target);
            std::chrono::year_month_day ymd{
                std::chrono::year{target.tm_year + 1900},
                std::chrono::month{static_cast<unsigned>(target.tm_mon + 1)},
                std::chrono::day{static_cast<unsigned>(target.tm_mday)}};
            return Value::DateFromDays(
                std::chrono::sys_days{ymd}.time_since_epoch().count());
          }
          if (has_tz) {
            CivilTime ct_tmp{.year = Y,
                             .month = M,
                             .day = D,
                             .hour = h,
                             .minute = m,
                             .second = static_cast<int>(sec)};
            int default_tz_sec =
                ParseTimeZoneOffset(GetDefaultTimeZone(), &ct_tmp, -8 * 3600);
            struct tm t = {};
            t.tm_year = Y - 1900;
            t.tm_mon = M - 1;
            t.tm_mday = D;
            t.tm_hour = h;
            t.tm_min = m;
            t.tm_sec = static_cast<int>(sec);
            time_t epoch = timegm(&t) - tz_offset_sec + default_tz_sec;
            struct tm target = {};
            gmtime_r(&epoch, &target);
            std::chrono::year_month_day ymd{
                std::chrono::year{target.tm_year + 1900},
                std::chrono::month{static_cast<unsigned>(target.tm_mon + 1)},
                std::chrono::day{static_cast<unsigned>(target.tm_mday)}};
            return Value::DateFromDays(
                std::chrono::sys_days{ymd}.time_since_epoch().count());
          }
        }
      }
      return Value::Date(s.substr(0, 10));
    }
    return Value::Date(s);
  }
  if (name == "datetime") {
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
      ASSIGN_OR_RETURN(CivilTime, ct, (TryValueToCivilTime(arguments[0])));
      std::string s = raw_str(arguments[0]);
      if (s.find('+') != std::string::npos ||
          s.find('Z') != std::string::npos ||
          s.find('z') != std::string::npos) {
        int64_t ns = CivilTimeToNanos(ct) + ((-8 * 3600LL) * 1000000000LL);
        ct = NanosToCivilTime(ns);
      }
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
      int tz_offset_sec = ParseTimeZoneOffset(arg1_str, &ct, 0);
      if (tz_offset_sec != 0) {
        int64_t ns = CivilTimeToNanos(ct) + (tz_offset_sec * 1000000000LL);
        ct = NanosToCivilTime(ns);
      }
      return Value(FormatCivilTime(ct));
    }
    if (arguments.size() == 3) {
      std::string d = raw_str(arguments[0]);
      std::string t = raw_str(arguments[1]);
      if (d.size() > 10) {
        d = d.substr(0, 10);
      }
      CivilTime ct;
      ParseCivilTime(d + " " + t, &ct);
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
  if (name == "timestamp") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TIMESTAMP takes 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    ASSIGN_OR_RETURN(CivilTime, ct, (TryValueToCivilTime(arguments[0])));
    std::string tz_str =
        (arguments.size() == 2) ? raw_str(arguments[1]) : GetDefaultTimeZone();
    int tz_offset_sec = ParseTimeZoneOffset(tz_str, &ct, -8 * 3600);
    // An explicit zone inside the timestamp text (" UTC", "Z", "+HH:MM")
    // fixes the instant; the session default must not shift it again.
    if (arguments[0].type == ValueType::kVarChar) {
      const std::string_view text = arguments[0].value.varchar_value;
      if (text.size() > 10) {
        bool has_explicit_zone = false;
        int explicit_sec = 0;
        for (size_t i = 10; i < text.size(); ++i) {
          if ((text[i] == '+' || text[i] == '-') &&
              std::isdigit(static_cast<unsigned char>(
                  i + 1 < text.size() ? text[i + 1] : '0')) != 0) {
            has_explicit_zone = true;
            explicit_sec =
                ParseTimeZoneOffset(std::string(text.substr(i)), &ct, 0);
            break;
          }
        }
        if (!has_explicit_zone) {
          for (const std::string_view marker :
               {" UTC", " utc", " GMT", " gmt", "Z", "z"}) {
            if (text.size() > marker.size() &&
                text.substr(text.size() - marker.size()) == marker) {
              has_explicit_zone = true;
              explicit_sec = 0;
              break;
            }
          }
        }
        if (has_explicit_zone) {
          tz_offset_sec = explicit_sec;
        }
      }
    }
    int64_t ns = CivilTimeToNanos(ct) - (tz_offset_sec * 1000000000LL);
    CivilTime utc_ct = NanosToCivilTime(ns);
    return Value(FormatCivilTime(utc_ct) + "+00");
  }
  if (name == "time") {
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
        size_t tz_pos = s.find_first_of("+-", 11);
        if (tz_pos != std::string::npos || s.find('Z') != std::string::npos ||
            s.find('z') != std::string::npos) {
          int tz_offset_sec = 0;
          if (tz_pos != std::string::npos) {
            tz_offset_sec = ParseTimeZoneOffset(s.substr(tz_pos), &ct, 0);
          }
          int64_t ns = CivilTimeToNanos(ct) - (tz_offset_sec * 1000000000LL) +
                       ((-8 * 3600LL) * 1000000000LL);
          ct = NanosToCivilTime(ns);
        }
        std::array<char, 64> buf{};
        if (ct.subsecond_nanos != 0) {
          if (ct.subsecond_nanos % 1000000 == 0) {
            (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%03ld",
                           ct.hour, ct.minute, ct.second,
                           ct.subsecond_nanos / 1000000);
          } else if (ct.subsecond_nanos % 1000 == 0) {
            (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%06ld",
                           ct.hour, ct.minute, ct.second,
                           ct.subsecond_nanos / 1000);
          } else {
            (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%09ld",
                           ct.hour, ct.minute, ct.second, ct.subsecond_nanos);
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
        s = s.substr(space + 1);
      }
      size_t tz_pos = s.find_first_of("+-Zz");
      if (tz_pos != std::string::npos) {
        s = s.substr(0, tz_pos);
      }
      return Value(std::move(s));
    }
    if (arguments.size() == 2) {
      ASSIGN_OR_RETURN(CivilTime, ct, (TryValueToCivilTime(arguments[0])));
      std::string tz_str = raw_str(arguments[1]);
      int tz_offset_sec = ParseTimeZoneOffset(tz_str, &ct, 0);
      if (tz_offset_sec != 0) {
        int64_t ns = CivilTimeToNanos(ct) + (tz_offset_sec * 1000000000LL);
        ct = NanosToCivilTime(ns);
      }
      std::array<char, 64> buf{};
      if (ct.subsecond_nanos != 0) {
        if (ct.subsecond_nanos % 1000000 == 0) {
          (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%03ld",
                         ct.hour, ct.minute, ct.second,
                         ct.subsecond_nanos / 1000000);
        } else if (ct.subsecond_nanos % 1000 == 0) {
          (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%06ld",
                         ct.hour, ct.minute, ct.second,
                         ct.subsecond_nanos / 1000);
        } else {
          (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%09ld",
                         ct.hour, ct.minute, ct.second, ct.subsecond_nanos);
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
        (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d.%06d", h, m, s,
                       sub);
      } else {
        (void)snprintf(buf.data(), buf.size(), "%02d:%02d:%02d", h, m, s);
      }
      return Value(std::string(buf.data()));
    }
  }
  if (name == "__quantified__") {
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
  if (name == "array_element_offset" || name == "array_element_ordinal" ||
      name == "array_element_safe_offset" ||
      name == "array_element_safe_ordinal") {
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
    const auto& elements = arr.ArrayElements();
    int64_t index = arguments[1].value.int_value;
    if (name.find("ordinal") != std::string::npos) {
      --index;
    }
    if (index < 0 || std::cmp_greater_equal(index, elements.size())) {
      // Out-of-range plain accesses are errors in GoogleSQL; only the SAFE
      // variants yield NULL.
      if (!safe) {
        return StatusError(
            StatusCode::kInvalidArgument,
            "Array index " + std::to_string(index) + " is out of bounds");
      }
      return Value();
    }
    return elements[static_cast<size_t>(index)];
  }
  if (name == "__bit_and" || name == "__bit_or" || name == "__bit_xor" ||
      name == "__shift_left" || name == "__shift_right") {
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
    if (name == "__bit_and") {
      return with_u(Value(lhs & rhs));
    }
    if (name == "__bit_or") {
      return with_u(Value(lhs | rhs));
    }
    if (name == "__bit_xor") {
      return with_u(Value(lhs ^ rhs));
    }
    if (rhs < 0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "Bitwise shift by negative offset.");
    }
    if (rhs >= 64) {
      return with_u(Value(static_cast<int64_t>(0)));
    }
    const auto ulhs = static_cast<uint64_t>(lhs);
    const uint64_t shifted = name == "__shift_left" ? ulhs << rhs : ulhs >> rhs;
    return with_u(Value(static_cast<int64_t>(shifted)));
  }
  if (name == "__proto_new") {
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
  if (name == "__value_table_value") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__value_table_value requires one argument");
    }
    return arguments.front();
  }
  if (name == "__value_table_proto") {
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
    for (const char* field : {"int32_val1", "int32_val2", "str_value"}) {
      Value value;
      if (TryProtoTextGetField(raw_str(arguments[1]), field, &value)) {
        fields.emplace_back(field, std::move(value));
      }
    }
    return Value(ConstructProtoText(type_name, fields));
  }
  if (name == "__value_table_proto_existing") {
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
  if (name == "__proto_set") {
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
    ValidateEnumFieldValue(
        type_name, path.empty() ? std::string() : path.back(), arguments[2]);
    auto rewritten = ProtoTextSetField(payload, path, arguments[2], type_name);
    return Value(rewritten.value_or(payload));
  }
  if (name == "__get_extension") {
    // value.(pkg.Ext.field): reads the bracketed extension entry from a
    // proto TEXT payload; NULL bases yield NULL.
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "__get_extension requires 2 arguments");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const std::string base = raw_str(arguments[0]);
    const std::string key = "[" + raw_str(arguments[1]) + "]";
    Value out;
    if (!TryProtoTextGetField(base, key, &out)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "extension " + raw_str(arguments[1]) + " not found");
    }
    return out;
  }
  if (name == "unix_seconds" || name == "unix_millis" ||
      name == "unix_micros" || name == "unix_date") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires one TIMESTAMP argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const std::optional<int64_t> nanos =
        ParseTimestampTextNanos(raw_str(arguments[0]));
    if (!nanos.has_value()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "invalid TIMESTAMP: " + raw_str(arguments[0]));
    }
    auto floor_div_unix = [](int64_t a, int64_t b) {
      const int64_t q = a / b;
      return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
    };
    if (name == "unix_date") {
      return Value(floor_div_unix(*nanos, 86400000000000LL));
    }
    if (name == "unix_seconds") {
      return Value(floor_div_unix(*nanos, 1000000000LL));
    }
    if (name == "unix_millis") {
      return Value(floor_div_unix(*nanos, 1000000LL));
    }
    return Value(floor_div_unix(*nanos, 1000LL));
  }
  if (name == "__get_field_safe") {
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
            parsed = Value::Date(std::string(parsed.value.varchar_value)
                                     .substr(kDateMarker.size()));
          }
          if (parsed.type == ValueType::kVarChar &&
              (field_name == "amin" || field_name == "amax") &&
              object.find("ARRAY<DATE>") != std::string::npos) {
            parsed = Value::Date(parsed.value.varchar_value);
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
        constexpr std::string_view kDateMarker = "__tinylamb_date__:";
        if (parsed.type == ValueType::kVarChar &&
            parsed.value.varchar_value.starts_with(kDateMarker)) {
          parsed = Value::Date(std::string(parsed.value.varchar_value)
                                   .substr(kDateMarker.size()));
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
  if (name == "get_field") {
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
      std::string search_key = "\"" + field_name + "\":";
      size_t pos = s0.find(search_key);
      if (pos == std::string::npos) {
        // Positional fallback: unnamed two-field struct literals (the
        // APPROX_TOP_* payload shape) resolve value / count / sum by
        // position.
        if (s0.find("\":") == std::string::npos) {
          std::string inner = s0.substr(1, s0.size() - 2);
          std::vector<std::string> fields;
          int depth = 0;
          bool in_str = false;
          char quote = '\0';
          std::string current;
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
        return Value();
      }
      size_t val_start = pos + search_key.size();
      size_t val_end = s0.find_first_of(",}", val_start);
      std::string val_str = s0.substr(val_start, val_end - val_start);
      if (val_str == "null") {
        return Value();
      }
      if (val_str.size() >= 2 && val_str.front() == '"' &&
          val_str.back() == '"') {
        val_str = val_str.substr(1, val_str.size() - 2);
      }
      return Value(std::move(val_str));
    }
    return Value();
  }
  if (name == "date_diff" || name == "datetime_diff" ||
      name == "timestamp_diff") {
    if (call.Args().size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 3 arguments");
    }
    ASSIGN_OR_RETURN(
        Value, d1,
        (TryEvaluate(call.Args()[0], scope, aggregates, context, ctes)));
    ASSIGN_OR_RETURN(
        Value, d2,
        (TryEvaluate(call.Args()[1], scope, aggregates, context, ctes)));
    if (d1.IsNull() || d2.IsNull()) {
      return Value();
    }
    std::string unit = "day";
    if (call.Args()[2]->Type() == TypeTag::kColumnValue) {
      unit = to_lower(call.Args()[2]->AsColumnValue().GetColumnName().name);
    } else {
      ASSIGN_OR_RETURN(
          Value, uval,
          (TryEvaluate(call.Args()[2], scope, aggregates, context, ctes)));

      if (!uval.IsNull()) {
        unit = to_lower(raw_str(uval));
      }
    }
    ASSIGN_OR_RETURN(CivilTime, ct1, (TryValueToCivilTime(d1)));
    ASSIGN_OR_RETURN(CivilTime, ct2, (TryValueToCivilTime(d2)));
    std::string s1 = raw_str(d1);
    std::string s2 = raw_str(d2);
    if (s1.find('+') != std::string::npos ||
        s1.find('Z') != std::string::npos ||
        s1.find('z') != std::string::npos) {
      int64_t ns = CivilTimeToNanos(ct1) + ((-8 * 3600LL) * 1000000000LL);
      ct1 = NanosToCivilTime(ns);
    }
    if (s2.find('+') != std::string::npos ||
        s2.find('Z') != std::string::npos ||
        s2.find('z') != std::string::npos) {
      int64_t ns = CivilTimeToNanos(ct2) + ((-8 * 3600LL) * 1000000000LL);
      ct2 = NanosToCivilTime(ns);
    }
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
    int64_t ns1 = CivilTimeToNanos(t1);
    int64_t ns2 = CivilTimeToNanos(t2);
    int64_t diff_ns = ns1 - ns2;
    if (unit == "week" || unit == "weeks") {
      return Value(diff_ns / (7LL * 86400LL * 1000000000LL));
    }
    if (unit == "day" || unit == "days") {
      return Value(diff_ns / (86400LL * 1000000000LL));
    }
    if (unit == "hour" || unit == "hours") {
      return Value(diff_ns / (3600LL * 1000000000LL));
    }
    if (unit == "minute" || unit == "minutes") {
      return Value(diff_ns / (60LL * 1000000000LL));
    }
    if (unit == "second" || unit == "seconds") {
      return Value(diff_ns / 1000000000LL);
    }
    if (unit == "millisecond" || unit == "milliseconds") {
      return Value(diff_ns / 1000000LL);
    }
    if (unit == "microsecond" || unit == "microseconds") {
      return Value(diff_ns / 1000LL);
    }
    if (unit == "nanosecond" || unit == "nanoseconds") {
      return Value(diff_ns);
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "unsupported unit in " + name + ": " + unit);
  }
  if (name == "date_trunc" || name == "datetime_trunc" ||
      name == "timestamp_trunc") {
    if (call.Args().size() < 2 || call.Args().size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 2 or 3 arguments");
    }
    ASSIGN_OR_RETURN(
        Value, d,
        (TryEvaluate(call.Args()[0], scope, aggregates, context, ctes)));
    if (d.IsNull()) {
      return Value();
    }
    std::string unit = "day";
    if (call.Args()[1]->Type() == TypeTag::kColumnValue) {
      unit = to_lower(call.Args()[1]->AsColumnValue().GetColumnName().name);
    } else {
      ASSIGN_OR_RETURN(
          Value, uval,
          (TryEvaluate(call.Args()[1], scope, aggregates, context, ctes)));

      if (!uval.IsNull()) {
        unit = to_lower(raw_str(uval));
      }
    }
    std::string s_orig = raw_str(d);
    bool has_explicit_tz = (s_orig.find('+') != std::string::npos ||
                            s_orig.find('Z') != std::string::npos ||
                            s_orig.find('z') != std::string::npos);
    if (!has_explicit_tz) {
      for (size_t i = 10; i < s_orig.size(); ++i) {
        if (s_orig[i] == '+' || s_orig[i] == '-') {
          has_explicit_tz = true;
          break;
        }
      }
    }
    bool is_timestamp = (name == "timestamp_trunc" || has_explicit_tz);
    std::string trunc_tz = GetDefaultTimeZone();
    if (call.Args().size() == 3) {
      ASSIGN_OR_RETURN(
          Value, tz_val,
          (TryEvaluate(call.Args()[2], scope, aggregates, context, ctes)));

      if (tz_val.IsNull()) {
        return Value();
      }
      std::string tz_str = raw_str(tz_val);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        return StatusError(StatusCode::kInvalidArgument,
                           "invalid timezone: " + tz_str);
      }
      trunc_tz = tz_str;
    }

    ASSIGN_OR_RETURN(CivilTime, ct, (TryValueToCivilTime(d)));
    if (is_timestamp) {
      int64_t d_utc_ns = CivilTimeToNanos(ct);
      if (!has_explicit_tz) {
        int parse_tz_sec =
            ParseTimeZoneOffset(GetDefaultTimeZone(), &ct, -8 * 3600);
        d_utc_ns -= parse_tz_sec * 1000000000LL;
      }
      int trunc_tz_sec = ParseTimeZoneOffset(trunc_tz, &ct, -8 * 3600);
      CivilTime trunc_local_ct =
          NanosToCivilTime(d_utc_ns + (trunc_tz_sec * 1000000000LL));
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
      int new_trunc_tz_sec =
          ParseTimeZoneOffset(trunc_tz, &trunc_local_ct, -8 * 3600);
      int64_t res_utc_ns =
          CivilTimeToNanos(trunc_local_ct) - (new_trunc_tz_sec * 1000000000LL);
      CivilTime res_utc_ct = NanosToCivilTime(res_utc_ns);
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
    if (d.type == ValueType::kDate && name == "date_trunc") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      return Value::DateFromDays(
          std::chrono::sys_days{ymd}.time_since_epoch().count());
    }
    return Value(FormatCivilTime(ct));
  }
  if (name == "format_date" || name == "format_datetime" ||
      name == "format_timestamp") {
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
    format_time(buf.data(), buf.size(), fmt.c_str(), &tm);
    return Value(std::string(buf.data()));
  }
  if (name == "parse_timestamp") {
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
    std::string input = raw_str(arguments[1]);
    int tz_offset_sec =
        ParseTimeZoneOffset(GetDefaultTimeZone(), nullptr, -8 * 3600);
    if (arguments.size() == 3) {
      std::string tz_str = raw_str(arguments[2]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        return StatusError(StatusCode::kInvalidArgument,
                           "invalid timezone: " + tz_str);
      }
      tz_offset_sec = ParseTimeZoneOffset(tz_str);
    }
    struct tm tm = {};
    tm.tm_year = 100;  // 2000 default
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
  if (name == "date_bucket" || name == "timestamp_bucket" ||
      name == "datetime_bucket") {
    if (call.Args().size() < 2 || call.Args().size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " takes 2 or 3 arguments");
    }
    ASSIGN_OR_RETURN(
        Value, d,
        (TryEvaluate(call.Args()[0], scope, aggregates, context, ctes)));
    if (d.IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(CivilTime, d_ct, (TryValueToCivilTime(d)));
    std::string d_s = raw_str(d);
    bool d_has_tz = (d_s.find('+') != std::string::npos ||
                     d_s.find('Z') != std::string::npos ||
                     d_s.find('z') != std::string::npos);
    if (!d_has_tz) {
      for (size_t i = 10; i < d_s.size(); ++i) {
        if (d_s[i] == '+' || d_s[i] == '-') {
          d_has_tz = true;
          break;
        }
      }
    }
    bool is_ts = (name == "timestamp_bucket" || d_has_tz);

    CivilTime orig_ct{.year = 1970,
                      .month = 1,
                      .day = 1,
                      .hour = 0,
                      .minute = 0,
                      .second = 0,
                      .subsecond_nanos = 0};
    bool has_origin = false;
    bool orig_has_tz = false;
    if (call.Args().size() == 3) {
      ASSIGN_OR_RETURN(
          Value, orig,
          (TryEvaluate(call.Args()[2], scope, aggregates, context, ctes)));

      if (orig.IsNull()) {
        return Value();
      }
      ASSIGN_OR_RETURN(CivilTime, orig_ct_next, (TryValueToCivilTime(orig)));
      orig_ct = orig_ct_next;
      std::string orig_s = raw_str(orig);
      orig_has_tz = (orig_s.find('+') != std::string::npos ||
                     orig_s.find('Z') != std::string::npos ||
                     orig_s.find('z') != std::string::npos);
      if (!orig_has_tz) {
        for (size_t i = 10; i < orig_s.size(); ++i) {
          if (orig_s[i] == '+' || orig_s[i] == '-') {
            orig_has_tz = true;
            break;
          }
        }
      }
      if (orig_has_tz) {
        is_ts = true;
      }
      has_origin = true;
    }
    if (call.Args()[1]->Type() == TypeTag::kIntervalExp) {
      const auto& interval = call.Args()[1]->AsIntervalExpression();
      int64_t amount = interval.Amount();
      std::string unit = to_lower(std::string(interval.Unit()));
      double sec_amount = 0.0;
      bool is_fractional_second = false;
      if (unit == "second" || unit == "seconds") {
        std::string raw_amount = interval.RawAmount();
        if (!raw_amount.empty() && raw_amount.find('.') != std::string::npos) {
          // Exception-free strict parse (no-exception-rule-migration.md):
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
        int64_t step_m = amount;
        if (unit.starts_with("year")) {
          step_m = amount * 12;
        } else if (unit.starts_with("quarter")) {
          step_m = amount * 3;
        }
        if (step_m == 0) {
          return StatusError(StatusCode::kInvalidArgument,
                             name + ": interval must not be zero");
        }
        int64_t m_diff = (static_cast<int64_t>(d_ct.year - orig_ct.year) * 12) +
                         (d_ct.month - orig_ct.month);
        int64_t bucket_m = floor_div(m_diff, step_m) * step_m;
        int64_t total_m = (orig_ct.year * 12 + orig_ct.month - 1) + bucket_m;
        int bucket_y = static_cast<int>(floor_div(total_m, 12));
        int bucket_mon = static_cast<int>(
            total_m - (static_cast<int64_t>(bucket_y) * 12) + 1);
        CivilTime res_ct = orig_ct;
        res_ct.year = bucket_y;
        res_ct.month = bucket_mon;
        if (d.type == ValueType::kDate && name == "date_bucket") {
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
        step_ns = static_cast<int64_t>(std::round(sec_amount * 1000000000.0));
      } else if (unit == "nanosecond" || unit == "nanoseconds") {
        step_ns = amount;
      } else if (unit == "microsecond" || unit == "microseconds") {
        step_ns = amount * 1000LL;
      } else if (unit == "millisecond" || unit == "milliseconds") {
        step_ns = amount * 1000000LL;
      } else if (unit == "second" || unit == "seconds") {
        step_ns = amount * 1000000000LL;
      } else if (unit == "minute" || unit == "minutes") {
        step_ns = amount * 60LL * 1000000000LL;
      } else if (unit == "hour" || unit == "hours") {
        step_ns = amount * 3600LL * 1000000000LL;
      } else if (unit == "day" || unit == "days") {
        step_ns = amount * 86400LL * 1000000000LL;
        if (!has_origin && amount % 7 == 0) {
          orig_ct.year = 1970;
          orig_ct.month = 1;
          orig_ct.day = 4;  // Sunday
        }
      } else if (unit == "week" || unit == "weeks") {
        step_ns = amount * 7LL * 86400LL * 1000000000LL;
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

      int64_t d_ns = CivilTimeToNanos(d_ct);
      if (is_ts && !d_has_tz) {
        int tz_sec =
            ParseTimeZoneOffset(GetDefaultTimeZone(), &d_ct, -8 * 3600);
        d_ns -= tz_sec * 1000000000LL;
      }
      int64_t orig_ns = CivilTimeToNanos(orig_ct);
      if (is_ts && (!has_origin || !orig_has_tz)) {
        int tz_sec =
            ParseTimeZoneOffset(GetDefaultTimeZone(), &orig_ct, -8 * 3600);
        orig_ns -= tz_sec * 1000000000LL;
      }
      int64_t diff_ns = d_ns - orig_ns;
      int64_t bucket_ns = orig_ns + (floor_div(diff_ns, step_ns) * step_ns);
      CivilTime res_ct = NanosToCivilTime(bucket_ns);

      if (d.type == ValueType::kDate && name == "date_bucket") {
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
  if (name == "parse_date") {
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
      if (val.size() < 3 || val.size() > 4) {
        return StatusError(StatusCode::kInvalidArgument,
                           "invalid format for %W%y: " + val);
      }
      for (char c : val) {
        if (c < '0' || c > '9') {
          return StatusError(StatusCode::kInvalidArgument,
                             "invalid digit in PARSE_DATE: " + val);
        }
      }
      int w = 0, y = 0;
      if (val.size() == 3) {
        w = std::stoi(val.substr(0, 2));
        y = std::stoi(val.substr(2, 1));
      } else {
        w = std::stoi(val.substr(0, 2));
        y = std::stoi(val.substr(2, 2));
      }
      if (w < 0 || w > 53) {
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
    char* res = strptime(val.c_str(), fmt.c_str(), &tm);
    if (res == nullptr || *res != '\0') {
      return StatusError(StatusCode::kInvalidArgument,
                         "failed to parse date: " + val);
    }
    std::array<char, 32> buf{};
    (void)snprintf(buf.data(), buf.size(), "%04d-%02d-%02d", tm.tm_year + 1900,
                   tm.tm_mon + 1, tm.tm_mday);
    return Value::Date(std::string(buf.data()));
  }
  if (name == "last_day") {
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
  if (name == "week" && arguments.size() == 1) {
    return Value("week(" + to_lower(raw_str(arguments[0])) + ")");
  }
  if (name == "add_months") {
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
    int target_y = static_cast<int>(floor_div(total_m, 12));
    if (target_y < 1 || target_y > 9999) {
      return StatusError(StatusCode::kIsInfinity, "DATE value out of range");
    }
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
  if (name == "next_day") {
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
    if (target_day_str.starts_with("mon")) {
      target_iso = 1;
    } else if (target_day_str.starts_with("tue")) {
      target_iso = 2;
    } else if (target_day_str.starts_with("wed")) {
      target_iso = 3;
    } else if (target_day_str.starts_with("thu")) {
      target_iso = 4;
    } else if (target_day_str.starts_with("fri")) {
      target_iso = 5;
    } else if (target_day_str.starts_with("sat")) {
      target_iso = 6;
    } else if (target_day_str.starts_with("sun")) {
      target_iso = 7;
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
  if (name == "months_between") {
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

  auto utf8_offsets = [](std::string_view s)
      -> std::vector<size_t> {  // NOLINT(bugprone-branch-clone)
    std::vector<size_t> offsets;
    offsets.reserve(s.size() + 1);
    for (size_t i = 0; i < s.size();) {
      offsets.push_back(i);
      const auto c = static_cast<unsigned char>(s[i]);
      if ((c & 0x80) == 0) {  // NOLINT(bugprone-branch-clone)
        i += 1;
      } else if ((c & 0xE0) == 0xC0) {
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
        i += 1;
      }
    }
    offsets.push_back(s.size());
    return offsets;
  };

  auto utf8_len = [&](std::string_view s) -> size_t {
    return utf8_offsets(s).size() - 1;
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
  if (name == "concat") {
    std::string result;
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
  if (name == "byte_length" || name == "octet_length") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    return Value(static_cast<int64_t>(raw_str(arguments[0]).size()));
  }
  if (name == "length" || name == "char_length" || name == "character_length") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    return Value(static_cast<int64_t>(utf8_len(raw_str(arguments[0]))));
  }
  if (name == "upper") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "UPPER requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    return Value(UnicodeUpper(raw_str(arguments[0])));
  }
  if (name == "lower") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "LOWER requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    return Value(UnicodeLower(raw_str(arguments[0])));
  }
  if (name == "bit_count") {
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
                         "BIT_COUNT requires an INT32/INT64 argument");
    }
    // Storage-width popcount: the runtime value model keeps every integer
    // in an INT64 slot (INT32 declarations are width-validated at cast time
    // only), identical to GoogleSQL for non-negative inputs.
    return Value(static_cast<int64_t>(
        std::popcount(static_cast<uint64_t>(input.value.int_value))));
  }
  if (name == "normalize_and_casefold" || name == "normalize") {
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
    // stays in composed form, so NFC/NFKC are accepted and the decomposed
    // forms are rejected instead of silently mis-normalized.
    if (arguments.size() == 2) {
      if (arguments[1].type != ValueType::kVarChar) {
        return StatusError(StatusCode::kInvalidArgument,
                           name + " normalization mode must be a STRING");
      }
      std::string mode;
      for (char c : raw_str(arguments[1])) {
        mode.push_back(
            static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
      }
      if (mode != "NFC" && mode != "NFKC") {
        return StatusError(StatusCode::kNotImplemented,
                           name + " supports NFC/NFKC only, got " + mode);
      }
    }
    if (name == "normalize") {
      return arguments[0];
    }
    return Value(UnicodeCaseFold(raw_str(arguments[0])));
  }
  if (name == "trim") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TRIM requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    std::string s = raw_str(arguments[0]);
    const std::string cutset =
        arguments.size() == 2 ? raw_str(arguments[1]) : " \t\n\r\f\v";
    const size_t start = s.find_first_not_of(cutset);
    if (start == std::string::npos) {
      return Value(std::string());
    }
    const size_t end = s.find_last_not_of(cutset);
    return Value(s.substr(start, end - start + 1));
  }
  if (name == "ltrim") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "LTRIM requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    std::string s = raw_str(arguments[0]);
    const std::string cutset =
        arguments.size() == 2 ? raw_str(arguments[1]) : " \t\n\r\f\v";
    const size_t start = s.find_first_not_of(cutset);
    if (start == std::string::npos) {
      return Value(std::string());
    }
    return Value(s.substr(start));
  }
  if (name == "rtrim") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "RTRIM requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    std::string s = raw_str(arguments[0]);
    const std::string cutset =
        arguments.size() == 2 ? raw_str(arguments[1]) : " \t\n\r\f\v";
    const size_t end = s.find_last_not_of(cutset);
    if (end == std::string::npos) {
      return Value(std::string());
    }
    return Value(s.substr(0, end + 1));
  }
  if (name == "starts_with") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "STARTS_WITH requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    return Value(raw_str(arguments[0]).starts_with(raw_str(arguments[1])));
  }
  if (name == "ends_with") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ENDS_WITH requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    return Value(raw_str(arguments[0]).ends_with(raw_str(arguments[1])));
  }
  if (name == "strpos") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "STRPOS requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    const std::string sub = raw_str(arguments[1]);
    if (sub.empty()) {
      return Value(int64_t{1});
    }
    const size_t pos = s.find(sub);
    if (pos == std::string::npos) {
      return Value(int64_t{0});
    }
    return Value(
        static_cast<int64_t>(utf8_len(std::string_view(s.data(), pos)) + 1));
  }
  if (name == "instr") {
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
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {  // NOLINT(bugprone-empty-catch) - fallback.
        }
      }
      return def;
    };
    const int64_t position =
        arguments.size() >= 3 ? to_i64(arguments[2], 1) : 1;
    const int64_t occurrence =
        arguments.size() == 4 ? to_i64(arguments[3], 1) : 1;

    if (position == 0 || occurrence <= 0) {
      return Value(int64_t{0});
    }

    const auto s_offsets = utf8_offsets(source);
    const size_t s_cps = s_offsets.size() > 1 ? s_offsets.size() - 1 : 0;
    const auto t_offsets = utf8_offsets(target);
    const size_t t_cps = t_offsets.size() > 1 ? t_offsets.size() - 1 : 0;

    if (t_cps == 0) {
      if (position > 0 && static_cast<size_t>(position) <= s_cps + 1) {
        return Value(position);
      }
      return Value(int64_t{0});
    }

    std::vector<size_t> matches;
    if (position > 0) {
      auto start_cp = static_cast<size_t>(position - 1);
      for (size_t cp = start_cp; cp + t_cps <= s_cps; ++cp) {
        size_t b_start = s_offsets[cp];
        size_t b_end = s_offsets[cp + t_cps];
        if (source.substr(b_start, b_end - b_start) == target) {
          matches.push_back(cp + 1);
        }
      }
    } else {
      int64_t target_cp = static_cast<int64_t>(s_cps) + position;
      if (target_cp >= 0) {
        for (int64_t cp = target_cp; cp >= 0; --cp) {
          if (static_cast<size_t>(cp) + t_cps <= s_cps) {
            size_t b_start = s_offsets[static_cast<size_t>(cp)];
            size_t b_end = s_offsets[static_cast<size_t>(cp) + t_cps];
            if (source.substr(b_start, b_end - b_start) == target) {
              matches.push_back(static_cast<size_t>(cp) + 1);
            }
          }
        }
      }
    }

    if (static_cast<size_t>(occurrence) <= matches.size()) {
      return Value(
          static_cast<int64_t>(matches[static_cast<size_t>(occurrence) - 1]));
    }
    return Value(int64_t{0});
  }
  if (name == "replace") {
    if (arguments.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "REPLACE requires 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        arguments[2].IsNull()) {
      return Value();
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
  if (name == "repeat") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "REPEAT requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {  // NOLINT(bugprone-empty-catch) - fallback.
        }
      }
      return def;
    };
    const int64_t n = to_i64(arguments[1], 0);
    if (n < 0) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "Second argument (repeat count) for REPEAT cannot be negative");
    }
    if (n == 0) {
      return Value(std::string());
    }
    if (static_cast<uint64_t>(s.size()) * static_cast<uint64_t>(n) > 1000000) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "Output of REPEAT exceeds max allowed output size of 1MB");
    }
    std::string res;
    res.reserve(s.size() * static_cast<size_t>(n));
    for (int64_t i = 0; i < n; ++i) {
      res += s;
    }
    return Value(std::move(res));
  }

  if (name == "reverse") {
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
  if (name == "substr" || name == "substring") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SUBSTR requires two or three arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() == 3 && arguments[2].IsNull())) {
      return Value();
    }
    const std::string input = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {  // NOLINT(bugprone-empty-catch) - fallback.
        }
      }
      return def;
    };
    const int64_t start = to_i64(arguments[1], 1);
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

  if (name == "byte_substr") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SUBSTR requires two or three arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() == 3 && arguments[2].IsNull())) {
      return Value();
    }
    const std::string input = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {  // NOLINT(bugprone-empty-catch) - fallback.
        }
      }
      return def;
    };
    const int64_t start = to_i64(arguments[1], 1);
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

  if (name == "byte_reverse") {
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

  if (name == "left" || name == "byte_left") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "LEFT requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {  // NOLINT(bugprone-empty-catch) - fallback.
        }
      }
      return def;
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
    if (name == "byte_left") {
      return Value(s.substr(0, static_cast<size_t>(len)));
    }
    return Value(utf8_substr(s, 0, static_cast<size_t>(len)));
  }

  if (name == "right" || name == "byte_right") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "RIGHT requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {  // NOLINT(bugprone-empty-catch) - fallback.
        }
      }
      return def;
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
    if (name == "byte_right") {
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

  if (name == "lpad" || name == "rpad") {
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
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {  // NOLINT(bugprone-empty-catch) - fallback.
        }
      }
      return def;
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
    const size_t needed = static_cast<size_t>(target_len) - total_cps;
    std::string pad_str;
    for (size_t i = 0; i < needed / pad_cps; ++i) {
      pad_str += pad;
    }
    if (needed % pad_cps != 0) {
      pad_str += utf8_substr(pad, 0, needed % pad_cps);
    }
    return Value(name == "lpad" ? pad_str + s : s + pad_str);
  }

  if (name == "ascii") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ASCII requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    if (s.empty()) {
      return Value(int64_t{0});
    }
    return Value(static_cast<int64_t>(static_cast<unsigned char>(s[0])));
  }

  if (name == "unicode") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "UNICODE requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    if (s.empty()) {
      return Value(int64_t{0});
    }
    const auto offsets = utf8_offsets(s);
    const size_t first_len = offsets[1];
    uint32_t code = 0;
    if (first_len == 1) {
      code = static_cast<unsigned char>(s[0]);
    } else if (first_len == 2) {
      code = static_cast<uint32_t>(
          ((static_cast<unsigned char>(s[0]) & 0x1F) << 6) |
          (static_cast<unsigned char>(s[1]) & 0x3F));
    } else if (first_len == 3) {
      code = static_cast<uint32_t>(
          ((static_cast<unsigned char>(s[0]) & 0x0F) << 12) |
          ((static_cast<unsigned char>(s[1]) & 0x3F) << 6) |
          (static_cast<unsigned char>(s[2]) & 0x3F));
    } else if (first_len == 4) {
      code = static_cast<uint32_t>(
          ((static_cast<unsigned char>(s[0]) & 0x07) << 18) |
          ((static_cast<unsigned char>(s[1]) & 0x3F) << 12) |
          ((static_cast<unsigned char>(s[2]) & 0x3F) << 6) |
          (static_cast<unsigned char>(s[3]) & 0x3F));
    }
    return Value(static_cast<int64_t>(code));
  }

  if (name == "chr") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CHR requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const int64_t code = arguments[0].type == ValueType::kInt64
                             ? arguments[0].value.int_value
                             : 0;
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

  if (name == "code_points_to_string") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CODE_POINTS_TO_STRING requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const Value& arr = arguments[0];
    if (arr.type != ValueType::kArray) {
      return Value();
    }
    std::string res;
    for (const Value& elem : arr.ArrayElements()) {
      if (elem.IsNull()) {
        return Value();
      }
      int64_t cp = elem.type == ValueType::kInt64 ? elem.value.int_value : 0;
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

  if (name == "code_points_to_bytes") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "CODE_POINTS_TO_BYTES requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const Value& arr = arguments[0];
    if (arr.type != ValueType::kArray) {
      return Value();
    }
    std::string res;
    for (const Value& elem : arr.ArrayElements()) {
      if (elem.IsNull()) {
        return Value();
      }
      int64_t b = elem.type == ValueType::kInt64 ? elem.value.int_value : 0;
      if (b < 0 || b > 255) {
        return StatusError(
            StatusCode::kInvalidArgument,
            "invalid byte for CODE_POINTS_TO_BYTES: " + std::to_string(b));
      }
      res.push_back(static_cast<char>(b));
    }
    return Value(std::move(res));
  }

  if (name == "initcap") {
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

  if (name == "regexp_contains") {
    if (arguments.size() != 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "REGEXP_CONTAINS requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    const std::string pat = raw_str(arguments[1]);
    try {
      ASSIGN_OR_RETURN(std::regex, re, CompileCheckedRegex(pat));
      return Value(std::regex_search(s, re));
    } catch (...) {
      return StatusError(StatusCode::kInvalidArgument,
                         "invalid regular expression: " + pat);
    }
  }

  if (name == "regexp_match") {
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

  if (name == "regexp_instr" || name == "byte_regexp_instr") {
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
    const std::string s = raw_str(arguments[0]);
    const std::string pat = raw_str(arguments[1]);
    if (pat.empty()) {
      return Value(int64_t{0});
    }
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {  // NOLINT(bugprone-empty-catch) - fallback.
        }
      }
      return def;
    };
    const int64_t pos_arg = arguments.size() >= 3 ? to_i64(arguments[2], 1) : 1;
    const int64_t occ_arg = arguments.size() >= 4 ? to_i64(arguments[3], 1) : 1;
    const int64_t return_pos =
        arguments.size() == 5 ? to_i64(arguments[4], 0) : 0;
    if (pos_arg <= 0 || occ_arg <= 0) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "REGEXP_INSTR position and occurrence must be positive");
    }
    const bool byte_mode = name == "byte_regexp_instr";
    const auto offsets = utf8_offsets(s);
    const size_t total_cps = offsets.size() - 1;
    if (static_cast<size_t>(pos_arg) > (byte_mode ? s.size() : total_cps) + 1) {
      return Value(int64_t{0});
    }
    const size_t byte_start = byte_mode
                                  ? static_cast<size_t>(pos_arg - 1)
                                  : offsets[static_cast<size_t>(pos_arg - 1)];
    try {
      ASSIGN_OR_RETURN(std::regex, re, CompileCheckedRegex(pat));
      std::string search_str = s.substr(byte_start);
      std::sregex_iterator it(search_str.begin(), search_str.end(), re);
      std::sregex_iterator end;
      int64_t current_occ = 1;
      while (it != end) {
        if (current_occ == occ_arg) {
          const auto& match = *it;
          const bool has_capture = match.size() > 1 && match[1].matched;
          const size_t target_start =
              has_capture && return_pos == 0
                  ? byte_start + static_cast<size_t>(match.position(1))
                  : byte_start + static_cast<size_t>(match.position());
          const size_t target_end = byte_start +
                                    static_cast<size_t>(match.position()) +
                                    static_cast<size_t>(match.length());
          if (byte_mode) {
            // `target_end` is already the one-based end position because it
            // is an exclusive byte offset; starts need the usual +1.
            return Value(static_cast<int64_t>(
                return_pos == 1 ? target_end : target_start + 1));
          }
          size_t match_cp_start = 1;
          size_t match_cp_end = 1;
          for (size_t i = 0; i + 1 < offsets.size(); ++i) {
            if (offsets[i] <= target_start && target_start < offsets[i + 1]) {
              match_cp_start = i + 1;
            }
            if (offsets[i] < target_end && target_end <= offsets[i + 1]) {
              match_cp_end = i + 1;
            }
          }
          if (target_end >= s.size()) {
            match_cp_end = total_cps;
          }
          return Value(static_cast<int64_t>(return_pos == 1 ? match_cp_end
                                                            : match_cp_start));
        }
        ++current_occ;
        ++it;
      }
      return Value(int64_t{0});
    } catch (...) {
      return StatusError(StatusCode::kInvalidArgument,
                         "invalid regular expression: " + pat);
    }
  }

  if (name == "regexp_extract_all" || name == "byte_regexp_extract_all") {
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
      return Value::Array(std::move(results), name == "byte_regexp_extract_all"
                                                  ? "BYTES"
                                                  : "STRING");
    } catch (...) {
      return StatusError(StatusCode::kInvalidArgument,
                         "invalid regular expression: " + pat);
    }
  }

  if (name == "regexp_extract") {
    if (arguments.size() < 2 || arguments.size() > 4) {
      return StatusError(StatusCode::kInvalidArgument,
                         "REGEXP_EXTRACT requires 2 to 4 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    const std::string pat = raw_str(arguments[1]);
    try {
      ASSIGN_OR_RETURN(std::regex, re, CompileCheckedRegex(pat));
      std::smatch match;
      if (std::regex_search(s, match, re)) {
        if (match.size() > 1) {
          return Value(match[1].str());
        }
        return Value(match[0].str());
      }
      return Value();
    } catch (...) {
      return StatusError(StatusCode::kInvalidArgument,
                         "invalid regular expression: " + pat);
    }
  }

  if (name == "regexp_replace") {
    if (arguments.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "REGEXP_REPLACE requires 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        arguments[2].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    const std::string pat = raw_str(arguments[1]);
    const std::string rep = raw_str(arguments[2]);
    try {
      ASSIGN_OR_RETURN(std::regex, re, CompileCheckedRegex(pat));
      return Value(std::regex_replace(s, re, rep));
    } catch (...) {
      return StatusError(StatusCode::kInvalidArgument,
                         "invalid regular expression: " + pat);
    }
  }

  if (name == "split_substr") {
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
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
          // NOLINTNEXTLINE(bugprone-empty-catch)
        } catch (...) {
        }
      }
      return def;
    };
    const int64_t occ = arguments.size() >= 3 ? to_i64(arguments[2], 1) : 1;

    std::vector<std::string> parts;
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

    int64_t end_idx =
        std::min(start_idx + len, static_cast<int64_t>(parts.size()));
    std::string res = parts[static_cast<size_t>(start_idx)];
    for (int64_t i = start_idx + 1; i < end_idx; ++i) {
      res += delim + parts[static_cast<size_t>(i)];
    }
    return Value(std::move(res));
  }

  if (name == "split") {
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

  if (name == "soundex") {
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

  if (name == "translate") {
    if (arguments.size() != 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TRANSLATE requires 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        arguments[2].IsNull()) {
      return Value();
    }
    const std::string s = raw_str(arguments[0]);
    const std::string src = raw_str(arguments[1]);
    const std::string dst = raw_str(arguments[2]);
    const auto s_offsets = utf8_offsets(s);
    const auto src_offsets = utf8_offsets(src);
    const auto dst_offsets = utf8_offsets(dst);
    const size_t s_len = s_offsets.size() - 1;
    const size_t src_len = src_offsets.size() - 1;
    const size_t dst_len = dst_offsets.size() - 1;

    std::vector<std::string> src_chars;
    src_chars.reserve(src_len);
    std::unordered_set<std::string> source_seen;
    source_seen.reserve(src_len);
    for (size_t i = 0; i < src_len; ++i) {
      std::string source_char =
          src.substr(src_offsets[i], src_offsets[i + 1] - src_offsets[i]);
      if (!source_seen.insert(source_char).second) {
        return StatusError(StatusCode::kInvalidArgument,
                           "TRANSLATE source contains duplicate characters");
      }
      src_chars.push_back(std::move(source_char));
    }
    std::vector<std::string> dst_chars;
    dst_chars.reserve(dst_len);
    for (size_t i = 0; i < dst_len; ++i) {
      dst_chars.push_back(
          dst.substr(dst_offsets[i], dst_offsets[i + 1] - dst_offsets[i]));
    }

    std::string res;
    for (size_t i = 0; i < s_len; ++i) {
      std::string ch = s.substr(s_offsets[i], s_offsets[i + 1] - s_offsets[i]);
      bool replaced = false;
      for (size_t j = 0; j < src_chars.size(); ++j) {
        if (ch == src_chars[j]) {
          if (j < dst_chars.size()) {
            res += dst_chars[j];
          }
          replaced = true;
          break;
        }
      }
      if (!replaced) {
        res += ch;
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

  // JSON functions
  if (name == "json_extract" || name == "json_query" || name == "json_value" ||
      name == "json_extract_scalar" || name == "json_extract_array" ||
      name == "json_query_array" || name == "json_value_array" ||
      name == "json_extract_string_array") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    const std::string json_str = raw_str(arguments[0]);
    const std::string path =
        arguments.size() == 2 ? raw_str(arguments[1]) : "$";
    struct JVal {
      enum T {  // NOLINT(performance-enum-size) - local recursive JSON tag.
        kNull,
        kBool,
        kNum,
        kStr,
        kArr,
        kObj
      } type{kNull};
      std::string raw;
      std::string str;
      std::vector<std::unique_ptr<JVal>> arr;
      std::vector<std::pair<std::string, std::unique_ptr<JVal>>> obj;

      [[nodiscard]] std::string canonical() const {
        if (type == kNull) {
          return "null";
        }
        if (type == kBool) {
          return str;
        }
        if (type == kNum) {
          return raw;
        }
        if (type == kStr) {
          std::string res = "\"";
          for (char c : str) {
            if (c == '"') {
              res += "\\\"";
            } else if (c == '\\') {
              res += "\\\\";
            } else if (c == '\b') {
              res += "\\b";
            } else if (c == '\f') {
              res += "\\f";
            } else if (c == '\n') {
              res += "\\n";
            } else if (c == '\r') {
              res += "\\r";
            } else if (c == '\t') {
              res += "\\t";
            } else if (static_cast<unsigned char>(c) < 0x20) {
              std::array<char, 8> buf{};
              (void)snprintf(buf.data(), buf.size(), "\\u%04x",
                             static_cast<unsigned char>(c));
              res += buf.data();
            } else {
              res.push_back(c);
            }
          }
          res += "\"";
          return res;
        }
        if (type == kArr) {
          std::string res = "[";
          for (size_t i = 0; i < arr.size(); ++i) {
            if (i > 0) {
              res += ",";
            }
            res += arr[i]->canonical();
          }
          res += "]";
          return res;
        }
        if (type == kObj) {
          std::string res = "{";
          for (size_t i = 0; i < obj.size(); ++i) {
            if (i > 0) {
              res += ",";
            }
            res += "\"";
            res += obj[i].first;
            res += "\":";
            res += obj[i].second->canonical();
          }
          res += "}";
          return res;
        }
        return raw;
      }
    };
    auto skip_ws = [](std::string_view s, size_t& pos) {
      while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t' ||
                                s[pos] == '\n' || s[pos] == '\r')) {
        ++pos;
      }
    };
    auto parse_str = [](std::string_view s, size_t& pos, std::string& out) {
      if (pos >= s.size() || s[pos] != '"') {
        return false;
      }
      ++pos;
      out.clear();
      while (pos < s.size()) {
        if (s[pos] == '\\' && pos + 1 < s.size()) {
          char next = s[++pos];
          if (next == '"') {
            out.push_back('"');
            ++pos;
          } else if (next == '\\') {
            out.push_back('\\');
            ++pos;
          } else if (next == '/') {
            out.push_back('/');
            ++pos;
          } else if (next == 'b') {
            out.push_back('\b');
            ++pos;
          } else if (next == 'f') {
            out.push_back('\f');
            ++pos;
          } else if (next == 'n') {
            out.push_back('\n');
            ++pos;
          } else if (next == 'r') {
            out.push_back('\r');
            ++pos;
          } else if (next == 't') {
            out.push_back('\t');
            ++pos;
          } else if (next == 'u' && pos + 4 < s.size()) {
            std::string hex_str = std::string(s.substr(pos + 1, 4));
            try {
              auto cp = static_cast<uint32_t>(std::stoul(hex_str, nullptr, 16));
              if (cp <= 0x7F) {
                out.push_back(static_cast<char>(cp));
              } else if (cp <= 0x7FF) {
                out.push_back(static_cast<char>(0xC0 | ((cp >> 6) & 0x1F)));
                out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
              } else if (cp <= 0xFFFF) {
                out.push_back(static_cast<char>(0xE0 | ((cp >> 12) & 0x0F)));
                out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
              } else {
                out.push_back(static_cast<char>(0xF0 | ((cp >> 18) & 0x07)));
                out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
              }
              pos += 5;
            } catch (...) {
              out.push_back(next);
              ++pos;
            }
          } else {
            out.push_back(next);
            ++pos;
          }
        } else if (s[pos] == '"') {
          ++pos;
          return true;
        } else {
          out.push_back(s[pos++]);
        }
      }
      return false;
    };
    std::function<bool(std::string_view, size_t&, JVal&, size_t)> parse_j =
        [&](std::string_view s, size_t& pos, JVal& out, size_t depth) -> bool {
      // Bound recursion: a deeply nested document would otherwise blow the
      // stack (Bug 68; mirrored in rewrite.cpp's ParseJsonSimple).
      constexpr size_t kMaxJsonDepth = 512;
      if (depth > kMaxJsonDepth) {
        return false;
      }
      skip_ws(s, pos);
      if (pos >= s.size()) {
        return false;
      }
      const size_t st = pos;
      if (s[pos] == '"') {
        out.type = JVal::kStr;
        if (!parse_str(s, pos, out.str)) {
          return false;
        }
        out.raw = std::string(s.substr(st, pos - st));
        return true;
      }
      if (s[pos] == '{') {
        out.type = JVal::kObj;
        ++pos;
        skip_ws(s, pos);
        if (pos < s.size() && s[pos] == '}') {
          ++pos;
          out.raw = std::string(s.substr(st, pos - st));
          return true;
        }
        while (pos < s.size()) {
          skip_ws(s, pos);
          std::string k;
          if (!parse_str(s, pos, k)) {
            return false;
          }
          skip_ws(s, pos);
          if (pos >= s.size() || s[pos] != ':') {
            return false;
          }
          ++pos;
          JVal v;
          if (!parse_j(s, pos, v, depth + 1)) {
            return false;
          }
          out.obj.emplace_back(std::move(k),
                               std::make_unique<JVal>(std::move(v)));
          skip_ws(s, pos);
          if (pos < s.size() && s[pos] == ',') {
            ++pos;
          } else if (pos < s.size() && s[pos] == '}') {
            ++pos;
            out.raw = std::string(s.substr(st, pos - st));
            return true;
          } else {
            return false;
          }
        }
        return false;
      }
      if (s[pos] == '[') {
        out.type = JVal::kArr;
        ++pos;
        skip_ws(s, pos);
        if (pos < s.size() && s[pos] == ']') {
          ++pos;
          out.raw = std::string(s.substr(st, pos - st));
          return true;
        }
        while (pos < s.size()) {
          JVal elem;
          if (!parse_j(s, pos, elem, depth + 1)) {
            return false;
          }
          out.arr.push_back(std::make_unique<JVal>(std::move(elem)));
          skip_ws(s, pos);
          if (pos < s.size() && s[pos] == ',') {
            ++pos;
          } else if (pos < s.size() && s[pos] == ']') {
            ++pos;
            out.raw = std::string(s.substr(st, pos - st));
            return true;
          } else {
            return false;
          }
        }
        return false;
      }
      if (s.substr(pos, 4) == "true") {
        out.type = JVal::kBool;
        out.str = "true";
        out.raw = "true";
        pos += 4;
        return true;
      }
      if (s.substr(pos, 5) == "false") {
        out.type = JVal::kBool;
        out.str = "false";
        out.raw = "false";
        pos += 5;
        return true;
      }
      if (s.substr(pos, 4) == "null") {
        out.type = JVal::kNull;
        out.raw = "null";
        pos += 4;
        return true;
      }
      while (pos < s.size() &&
             (std::isdigit(static_cast<unsigned char>(s[pos])) ||
              s[pos] == '-' || s[pos] == '+' || s[pos] == '.' ||
              s[pos] == 'e' || s[pos] == 'E')) {
        ++pos;
      }
      out.type = JVal::kNum;
      out.raw = std::string(s.substr(st, pos - st));
      out.str = out.raw;
      return true;
    };

    size_t p = 0;
    JVal root;
    if (!parse_j(json_str, p, root, 0)) {
      return Value();
    }
    const JVal* cur = &root;
    size_t path_pos = 0;
    if (path_pos < path.size() && path[path_pos] == '$') {
      ++path_pos;
    }
    while (path_pos < path.size() && cur != nullptr) {
      if (path[path_pos] == '.') {
        ++path_pos;
        std::string key;
        if (path_pos < path.size() && path[path_pos] == '"') {
          if (!parse_str(path, path_pos, key)) {
            cur = nullptr;
            break;
          }
        } else {
          size_t end = path_pos;
          while (end < path.size() && path[end] != '.' && path[end] != '[') {
            ++end;
          }
          key = std::string(path.substr(path_pos, end - path_pos));
          path_pos = end;
        }
        if (cur->type != JVal::kObj) {
          cur = nullptr;
          break;
        }
        const JVal* next = nullptr;
        for (const auto& [k, v] : cur->obj) {
          if (k == key) {
            next = v.get();
            break;
          }
        }
        cur = next;
      } else if (path[path_pos] == '[') {
        ++path_pos;
        size_t end = path_pos;
        while (end < path.size() && path[end] != ']') {
          ++end;
        }
        if (end >= path.size()) {
          cur = nullptr;
          break;
        }
        const std::string idx_str =
            std::string(path.substr(path_pos, end - path_pos));
        path_pos = end + 1;
        int64_t idx = 0;
        try {
          idx = std::stoll(idx_str);
        } catch (...) {  // NOLINT(bugprone-empty-catch) - incomparable values
                         // are ignored.
          cur = nullptr;
          break;
        }
        if (cur->type != JVal::kArr || idx < 0 ||
            std::cmp_greater_equal(idx, cur->arr.size())) {
          cur = nullptr;
          break;
        }
        cur = cur->arr[static_cast<size_t>(idx)].get();
      } else {
        cur = nullptr;
        break;
      }
    }
    if (cur == nullptr) {
      return Value();
    }
    const bool is_array_func = name.ends_with("_array");
    if (is_array_func) {
      if (cur->type != JVal::kArr) {
        return Value();
      }
      std::vector<Value> elems;
      elems.reserve(cur->arr.size());
      for (const auto& item : cur->arr) {
        if (name == "json_value_array" || name == "json_extract_string_array") {
          if (item->type == JVal::kNull) {
            elems.emplace_back();
          } else if (item->type == JVal::kStr) {
            elems.emplace_back(std::string(item->str));
          } else {
            elems.emplace_back(item->canonical());
          }
        } else {
          elems.emplace_back(item->canonical());
        }
      }
      return Value::Array(std::move(elems), "STRING");
    }
    if (cur->type == JVal::kNull) {
      return Value();
    }
    if (name == "json_value" || name == "json_extract_scalar") {
      if (cur->type == JVal::kArr || cur->type == JVal::kObj) {
        return Value();
      }
      return Value(std::string(cur->type == JVal::kStr ? cur->str : cur->raw));
    }
    return Value(cur->canonical());
  }

  if (name == "to_json_string") {
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
                       fname + " requires a numeric argument");
  };
  if (name == "abs") {
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
                       "ABS requires a numeric argument");
  }
  if (name == "sign") {
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
                       "SIGN requires a numeric argument");
  }
  if (name == "round") {
    if (arguments.empty() || arguments.size() > 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ROUND requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    int64_t digits = 0;
    if (arguments.size() == 2) {
      if (arguments[1].type != ValueType::kInt64) {
        return StatusError(StatusCode::kInvalidArgument,
                           "ROUND digits must be an integer");
      }
      digits = arguments[1].value.int_value;
    }
    // INT64 input must not round-trip through double: |v| > 2^53 loses
    // precision and INT64_MAX (== 2^63 as a double) casts back to INT64_MIN
    // (UB). Round on integers directly.
    if (arguments[0].type == ValueType::kInt64) {
      if (arguments[0].IsUnsigned()) {
        const auto v = static_cast<uint64_t>(arguments[0].value.int_value);
        if (digits >= 0) {
          return arguments[0];
        }
        uint64_t scale = 1;
        for (int64_t i = 0; i < -digits; ++i) {
          if (scale > std::numeric_limits<uint64_t>::max() / 10) {
            return Value(int64_t{0}).WithUnsigned();
          }
          scale *= 10;
        }
        uint64_t q = v / scale;
        const uint64_t rem = v % scale;
        const uint64_t half = scale / 2;
        if (rem >= half) {
          q += 1;
        }
        uint64_t result = 0;
        if (__builtin_mul_overflow(q, scale, &result)) {
          return StatusError(StatusCode::kIsInfinity,
                             "integer overflow in ROUND");
        }
        return Value(static_cast<int64_t>(result)).WithUnsigned();
      }
      const int64_t v = arguments[0].value.int_value;
      if (digits >= 0) {
        return Value(v);  // an integer is already rounded to >=0 places
      }
      int64_t scale = 1;
      for (int64_t i = 0; i < -digits; ++i) {
        if (scale > std::numeric_limits<int64_t>::max() / 10) {
          return Value(int64_t{0});  // scale overflow => result rounds to 0
        }
        scale *= 10;
      }
      int64_t q = v / scale;
      const int64_t rem = v % scale;
      const int64_t half = scale / 2;
      // Round half away from zero (GoogleSQL ROUND semantics).
      const bool negative = v < 0;
      if ((negative ? -rem : rem) >= half) {
        q += negative ? -1 : 1;
      }
      int64_t result = 0;
      if (__builtin_mul_overflow(q, scale, &result)) {
        return StatusError(StatusCode::kIsInfinity,
                           "integer overflow in ROUND");
      }
      return Value(result);
    }
    if (arguments[0].type == ValueType::kDouble) {
      const double val = arguments[0].value.double_value;
      if (std::isnan(val)) {
        return Value(val);
      }
      const double factor = std::pow(10.0, digits);
      const double res = std::round(val * factor) / factor;
      return Value(res);
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "ROUND requires a numeric argument");
  }
  if (name == "trunc" || name == "truncate") {
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
      digits = arguments[1].value.int_value;
    }
    // INT64 input: truncation never needs a double round-trip (see ROUND).
    if (arguments[0].type == ValueType::kInt64) {
      if (arguments[0].IsUnsigned()) {
        const auto v = static_cast<uint64_t>(arguments[0].value.int_value);
        if (digits >= 0) {
          return arguments[0];
        }
        uint64_t scale = 1;
        for (int64_t i = 0; i < -digits; ++i) {
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
      for (int64_t i = 0; i < -digits; ++i) {
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
      const double factor = std::pow(10.0, digits);
      const double res = std::trunc(val * factor) / factor;
      return Value(res);
    }
    return StatusError(StatusCode::kInvalidArgument,
                       name + " requires a numeric argument");
  }
  if (name == "ceil" || name == "ceiling") {
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
                       name + " requires a numeric argument");
  }
  if (name == "floor") {
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
                       "FLOOR requires a numeric argument");
  }
  if (name == "mod") {
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
        const auto r = static_cast<uint64_t>(arguments[1].value.int_value);
        if (r == 0) {
          return StatusError(StatusCode::kIsInfinity,
                             "division by zero in MOD");
        }
        const auto l = static_cast<uint64_t>(arguments[0].value.int_value);
        return Value(static_cast<int64_t>(l % r)).WithUnsigned();
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
  if (name == "pow" || name == "power") {
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
    if (l == 0.0 && r < 0.0 && !std::isinf(r)) {
      return StatusError(StatusCode::kIsInfinity, "division by zero in POW");
    }
    const double res = std::pow(l, r);
    if (std::isinf(res) && !std::isinf(l) && !std::isinf(r)) {
      return StatusError(StatusCode::kIsInfinity,
                         "Floating point overflow in function: POW");
    }
    return Value(res);
  }
  if (name == "sqrt") {
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
  if (name == "cbrt") {
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
  if (name == "greatest" || name == "least") {
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
            name == "greatest" ? arguments[i] > best : arguments[i] < best;
        if (takes) {
          best = arguments[i];
        }
      } else if (numeric(best) && numeric(arguments[i])) {
        const bool takes = name == "greatest"
                               ? as_double(arguments[i]) > as_double(best)
                               : as_double(arguments[i]) < as_double(best);
        if (takes) {
          best = arguments[i];
        }
      } else if (name == "greatest") {
        if (arguments[i] > best) {
          best = arguments[i];
        }
      } else if (arguments[i] < best) {
        best = arguments[i];
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
  if (name == "ln") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "LN requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], "LN"));
    return Value(std::log(v));
  }
  if (name == "log" || name == "log10") {
    if (name == "log10" ? arguments.size() != 1
                        : (arguments.empty() || arguments.size() > 2)) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " argument count mismatch");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    ASSIGN_OR_RETURN(double, v, to_numeric_double(arguments[0], name));
    if (name == "log10") {
      return Value(std::log10(v));
    }
    if (arguments.size() == 1) {
      return Value(std::log(v));
    }
    ASSIGN_OR_RETURN(double, base, to_numeric_double(arguments[1], "LOG base"));
    if (v == 1.0 && std::isinf(base)) {
      return Value(std::numeric_limits<double>::quiet_NaN());
    }
    return Value(std::log(v) / std::log(base));
  }
  if (name == "exp") {
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
  if (name == "cos") {
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
  if (name == "sin") {
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
  if (name == "tan") {
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
  if (name == "acos") {
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
  if (name == "asin") {
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
  if (name == "atan") {
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
  if (name == "atan2") {
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
  if (name == "pi") {
    if (!arguments.empty()) {
      return StatusError(StatusCode::kInvalidArgument, "PI takes no arguments");
    }
    return Value(M_PI);
  }
  if (name == "radians") {
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
  if (name == "degrees") {
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

  if (name == "cosh") {
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
  if (name == "sinh") {
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
  if (name == "tanh") {
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
  if (name == "div") {
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
        const auto r = static_cast<uint64_t>(arguments[1].value.int_value);
        if (r == 0) {
          return StatusError(StatusCode::kIsInfinity,
                             "division by zero in DIV");
        }
        const auto l = static_cast<uint64_t>(arguments[0].value.int_value);
        return Value(static_cast<int64_t>(l / r)).WithUnsigned();
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
  if (name == "ieee_divide" || name == "safe_divide") {
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
      return name == "safe_divide" ? Value() : Value(l / r);
    }
    const double res = l / r;
    if (name == "safe_divide" && (std::isinf(res) || std::isnan(res))) {
      return Value();
    }
    return Value(res);
  }

  if (name == "safe_add" || name == "safe_subtract" ||
      name == "safe_multiply") {
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
        if (name == "safe_add") {
          if (__builtin_add_overflow(l, r, &res)) {
            return Value();
          }
          return Value(static_cast<int64_t>(res)).WithUnsigned();
        }
        if (name == "safe_subtract") {
          if (__builtin_sub_overflow(l, r, &res)) {
            return Value();
          }
          return Value(static_cast<int64_t>(res)).WithUnsigned();
        }
        if (name == "safe_multiply") {
          if (__builtin_mul_overflow(l, r, &res)) {
            return Value();
          }
          return Value(static_cast<int64_t>(res)).WithUnsigned();
        }
      }
      const int64_t l = arguments[0].value.int_value;
      const int64_t r = arguments[1].value.int_value;
      if (name == "safe_add") {
        int64_t res = 0;
        if (__builtin_add_overflow(l, r, &res)) {
          return Value();
        }
        return Value(res);
      }
      if (name == "safe_subtract") {
        int64_t res = 0;
        if (__builtin_sub_overflow(l, r, &res)) {
          return Value();
        }
        return Value(res);
      }
      if (name == "safe_multiply") {
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
    // SAFE_* return NULL on overflow; the double path must not leak +/-inf.
    const double checked =
        name == "safe_add" ? l + r : (name == "safe_subtract" ? l - r : l * r);
    if (std::isinf(checked) || std::isnan(checked)) {
      return Value();
    }
    return Value(checked);
  }
  if (name == "safe_negate") {
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
  if (name == "format") {
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
    if (std::any_of(arguments.begin() + 1, arguments.end(),
                    [](const Value& argument) { return argument.IsNull(); })) {
      return Value();
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
            return StatusError(
                StatusCode::kInvalidArgument,
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
              return StatusError(
                  StatusCode::kInvalidArgument,
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
              (void)snprintf(buf.data(), buf.size(),
                             hash_flag ? "0x%lx" : "%lx",
                             static_cast<unsigned long>(arg.value.int_value));
              formatted_item = buf.data();
            } else if (spec == 'X') {
              std::array<char, 32> buf{};
              (void)snprintf(buf.data(), buf.size(),
                             hash_flag ? "0X%lX" : "%lX",
                             static_cast<unsigned long>(arg.value.int_value));
              formatted_item = buf.data();
            } else if (spec == 'o') {
              std::array<char, 32> buf{};
              (void)snprintf(buf.data(), buf.size(), hash_flag ? "0%lo" : "%lo",
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
            formatted_item = FormatFloatingForPrintf(arg.value.double_value,
                                                     spec, precision);
          } else if (arg.type == ValueType::kInt64) {
            formatted_item = FormatFloatingForPrintf(
                static_cast<double>(arg.value.int_value), spec, precision);
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

  if (name == "current_timestamp") {
    const std::time_t now = std::time(nullptr);
    std::tm tm{};
    gmtime_r(&now, &tm);
    std::array<char, 32> buffer{};
    if (std::strftime(buffer.data(), buffer.size(), "%Y-%m-%d %H:%M:%S", &tm) ==
        0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "timestamp formatting failed");
    }
    return Value(std::string(buffer.data()));
  }

  if (name == "make_interval") {
    if (arguments.size() < 2) {
      return StatusError(StatusCode::kInvalidArgument,
                         "make_interval requires at least 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return Value();
    }
    std::string val_str = raw_str(arguments[0]);
    std::string unit_str = raw_str(arguments[1]);
    IntervalValue iv = IntervalValue::Parse(val_str, unit_str);
    return Value(iv.ToString());
  }

  if (name == "justify_hours") {
    if (arguments.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "JUSTIFY_HOURS requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    IntervalValue iv = IntervalValue::Parse(raw_str(arguments[0]));
    return Value(iv.JustifyHours().ToString());
  }

  if (name == "justify_days") {
    if (arguments.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "JUSTIFY_DAYS requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    IntervalValue iv = IntervalValue::Parse(raw_str(arguments[0]));
    return Value(iv.JustifyDays().ToString());
  }

  if (name == "justify_interval") {
    if (arguments.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "JUSTIFY_INTERVAL requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    IntervalValue iv = IntervalValue::Parse(raw_str(arguments[0]));
    return Value(iv.JustifyInterval().ToString());
  }

  if (name == "collate") {
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
    return v.WithCollation(
        static_cast<uint8_t>(CollationOfSpec(raw_str(arguments[1]))));
  }

  if (name == "array_length") {
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

  if (name == "array_to_string") {
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
  if (name == "offset" || name == "ordinal") {
    if (arguments.size() != 1 || arguments[0].IsNull()) {
      return Value();
    }
    const Value& index = arguments[0];
    if (index.type == ValueType::kInt64) {
      return index;
    }
    return Value(static_cast<int64_t>(index.value.double_value));
  }

  if (name == "is_inf" || name == "is_nan") {
    if (arguments.size() != 1) {
      return Value();
    }
    const Value& arg = arguments[0];
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

  if (name == "hll_count.extract") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "HLL_COUNT.EXTRACT requires 1 argument");
    }
    const Value& sketch = arguments[0];
    if (sketch.IsNull()) {
      return Value(int64_t{0});
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
    if (number.IsNull() || number.value.int_value < 1) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "The second argument to KLL_QUANTILES.EXTRACT must be positive");
    }
    if (sketch.IsNull()) {
      return Value();
    }
    return TryExtractSketchQuantilesStatic(sketch, number.value.int_value);
  }

  if (name == "array_concat") {
    if (arguments.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_CONCAT requires at least 1 argument");
    }
    std::vector<Value> merged;
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
    return Value::Array(std::move(merged), element_type);
  }

  if (name == "array_first" || name == "array_last") {
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
                         name == "array_first"
                             ? "ARRAY_FIRST cannot get the first element "
                               "of an empty array"
                             : "ARRAY_LAST cannot get the last element of "
                               "an empty array");
    }
    return name == "array_first" ? elements.front() : elements.back();
  }

  if (name == "array_slice") {
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
    const auto& elements = arr.ArrayElements();
    const auto n = static_cast<int64_t>(elements.size());
    int64_t offset = arguments[1].value.int_value;
    int64_t size = arguments.size() == 3 ? arguments[2].value.int_value : n;
    if (size <= 0 || n == 0) {
      return Value::Array({}, arr.ArrayElementSqlType());
    }
    int64_t begin = offset >= 0 ? offset - 1 : n + offset;
    int64_t end = begin + size;  // exclusive
    begin = std::max<int64_t>(begin, 0);
    end = std::min<int64_t>(end, n);
    std::vector<Value> picked;
    for (int64_t i = begin; i < end; ++i) {
      picked.push_back(elements[static_cast<size_t>(i)]);
    }
    return Value::Array(std::move(picked), arr.ArrayElementSqlType());
  }

  if (name == "array_is_distinct") {
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

  if (name == "generate_array") {
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
  if (name == "generate_series") {
    return ExecuteFunction(name, arguments);
  }

  if (name == "generate_date_array") {
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
      if (call.Args()[2]->Type() == TypeTag::kIntervalExp) {
        const auto& interval = call.Args()[2]->AsIntervalExpression();
        const int64_t amount = interval.Amount();
        std::string unit = to_lower(std::string(interval.Unit()));
        if (unit.starts_with("week")) {
          step_days = amount * 7;
        } else if (unit.starts_with("day")) {
          step_days = amount;
        } else if (unit.starts_with("month") || unit.starts_with("quarter") ||
                   unit.starts_with("year")) {
          // Calendar steps advance through DATE_ADD's clamped month
          // arithmetic, not fixed day counts.
          step_months =
              unit.starts_with("month")
                  ? amount
                  : (unit.starts_with("quarter") ? amount * 3 : amount * 12);
        } else {
          return StatusError(
              StatusCode::kInvalidArgument,
              "unsupported GENERATE_DATE_ARRAY step unit: " + unit);
        }
      } else {
        const Value& step = arguments[2];
        if (step.type == ValueType::kInt64) {
          step_days = step.value.int_value;
        } else {
          // Column-valued INTERVAL steps arrive pre-evaluated as the
          // encoded text of a make_interval call (INTERVAL col DAY).
          const std::string text = raw_str(step);
          const IntervalValue parsed =
              text.empty() ? IntervalValue{} : IntervalValue::Parse(text);
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

  if (name == "array_includes" || name == "array_includes_any" ||
      name == "array_includes_all") {
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
    if (!input.IsArray() || (name != "array_includes" && !target.IsArray())) {
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
    bool result = false;
    if (name == "array_includes") {
      result = false;
      for (const Value& element : haystack) {
        if (!element.IsNull() && equals(element, target)) {
          result = true;
          break;
        }
      }
    } else {
      const auto& targets = target.ArrayElements();
      result = name != "array_includes_any";
      for (const Value& wanted : targets) {
        if (wanted.IsNull()) {
          // NULL target elements match nothing: ANY cannot use them and ALL
          // fails outright.
          if (name == "array_includes_all") {
            result = false;
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
        if (name == "array_includes_any") {
          if (found) {
            result = true;
            break;
          }
        } else if (!found) {
          result = false;
          break;
        }
      }
    }
    return Value(result);
  }

  if (name == "rand") {
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

  if (name == "session_user" || name == "current_user") {
    if (!arguments.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires no arguments");
    }
    return Value(std::string("tinylamb"));
  }

  if (name == "generate_uuid") {
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

  if (name == "md5" || name == "sha1" || name == "sha256" || name == "sha512" ||
      name == "farm_fingerprint") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const std::string input = raw_str(arguments[0]);
    if (name == "md5") {
      return Value(digest::Md5Digest(input));
    }
    if (name == "sha1") {
      return Value(digest::Sha1Digest(input));
    }
    if (name == "sha256") {
      return Value(digest::Sha256Digest(input));
    }
    if (name == "sha512") {
      return Value(digest::Sha512Digest(input));
    }
    return Value(static_cast<int64_t>(digest::FarmFingerprint64(input)));
  }

  // PARSE_JSON validates strict JSON text and hands it back in the engine's
  // string-based JSON representation; TO_JSON renders a scalar/struct value
  // as JSON text (same shape as the AST evaluator's branch).
  if (name == "parse_json" || name == "to_json") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    if (name == "parse_json") {
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

  if (name == "to_hex") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "TO_HEX requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    return Value(digest::ToHex(raw_str(arguments[0])));
  }

  if (name == "to_base64") {
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

  if (name == "from_base64") {
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

  if (name == "from_hex") {
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

  if (name == "safe_convert_bytes_to_string") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SAFE_CONVERT_BYTES_TO_STRING requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const std::string raw_input = raw_str(arguments[0]);
    std::string input;
    input.reserve(raw_input.size());
    auto hex_digit = [](char c) -> int {
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
    for (size_t i = 0; i < raw_input.size(); ++i) {
      if (raw_input[i] == '\\' && i + 3 < raw_input.size() &&
          (raw_input[i + 1] == 'x' || raw_input[i + 1] == 'X')) {
        const int high = hex_digit(raw_input[i + 2]);
        const int low = hex_digit(raw_input[i + 3]);
        if (high >= 0 && low >= 0) {
          input.push_back(static_cast<char>((high << 4) | low));
          i += 3;
          continue;
        }
      }
      input.push_back(raw_input[i]);
    }
    std::string out;
    out.reserve(input.size());
    const std::string replacement = "\xEF\xBF\xBD";
    for (size_t i = 0; i < input.size();) {
      const auto lead = static_cast<unsigned char>(input[i]);
      size_t length = 0;
      unsigned char min_cont = 0x80;
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
        }
      } else if (lead >= 0xF0 && lead <= 0xF4) {
        length = 4;
        if (lead == 0xF0) {
          min_cont = 0x90;
        }
      }
      bool valid = length > 0 && i + length <= input.size();
      for (size_t j = 1; valid && j < length; ++j) {
        const auto cont = static_cast<unsigned char>(input[i + j]);
        if (cont < (j == 1 ? min_cont : 0x80) || cont > 0xBF) {
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

  if (name == "bit_cast_to_int64" || name == "bit_cast_to_uint64") {
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
    if (name == "bit_cast_to_int64") {
      return Value(static_cast<int64_t>(bits));
    }
    // UINT64 results above INT64_MAX have no exact in-engine representation:
    // widen to double so they compare distinct from any signed value.
    if (bits >= 0x8000000000000000ULL) {
      return Value(static_cast<double>(bits));
    }
    return Value(static_cast<int64_t>(bits));
  }

  if (name == "error") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ERROR requires 1 argument");
    }
    return StatusError(
        StatusCode::kInvalidArgument,
        arguments[0].IsNull() ? "ERROR()" : raw_str(arguments[0]));
  }

  if (name == "array_reverse") {
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

  if (name == "array_is_distinct") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_IS_DISTINCT requires 1 argument");
    }
    const Value& array = arguments[0];
    if (array.IsNull()) {
      return Value();
    }
    if (!array.IsArray()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_IS_DISTINCT requires an ARRAY argument");
    }
    const auto& elements = array.ArrayElements();
    bool saw_null = false;
    for (size_t i = 0; i < elements.size(); ++i) {
      if (elements[i].IsNull()) {
        if (saw_null) {
          return Value(int64_t{0});
        }
        saw_null = true;
        continue;
      }
      for (size_t j = 0; j < i; ++j) {
        if (elements[j].IsNull()) {
          continue;
        }
        StatusOr<Value> eq =
            TryBinary(BinaryOperation::kEquals, elements[j], elements[i]);
        if (eq.HasValue() && eq.Value().Truthy()) {
          return Value(int64_t{0});
        }
      }
    }
    return Value(int64_t{1});
  }

  if (name == "array_first" || name == "array_last") {
    if (arguments.size() != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires 1 argument");
    }
    const Value& array = arguments[0];
    if (array.IsNull()) {
      return Value();
    }
    if (!array.IsArray()) {
      return StatusError(StatusCode::kInvalidArgument,
                         name + " requires an ARRAY argument");
    }
    if (array.ArrayElements().empty()) {
      return StatusError(
          StatusCode::kInvalidArgument,
          name == "array_first"
              ? "ARRAY_FIRST cannot get the first element of an empty array"
              : "ARRAY_LAST cannot get the last element of an empty array");
    }
    if (name == "array_first") {
      return array.ArrayElements().front();
    }
    return array.ArrayElements().back();
  }

  if (name == "array_slice") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_SLICE requires two or three arguments");
    }
    const Value& array = arguments[0];
    if (array.IsNull()) {
      return Value();
    }
    if (!array.IsArray()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY_SLICE requires an ARRAY argument");
    }
    auto as_index = [&](const Value& v) -> StatusOr<int64_t> {
      if (v.IsNull() ||
          (v.type != ValueType::kInt64 && v.type != ValueType::kDouble)) {
        return StatusError(StatusCode::kInvalidArgument,
                           "ARRAY_SLICE offsets must be integers");
      }
      return v.type == ValueType::kInt64
                 ? v.value.int_value
                 : static_cast<int64_t>(v.value.double_value);
    };
    const auto& elements = array.ArrayElements();
    const auto n = static_cast<int64_t>(elements.size());
    ASSIGN_OR_RETURN(int64_t, start_raw, (as_index(arguments[1])));
    ASSIGN_OR_RETURN(int64_t, end_raw,
                     (arguments.size() == 3 ? as_index(arguments[2])
                                            : StatusOr<int64_t>(n)));
    const auto slice_normalize = [n](int64_t offset) -> int64_t {
      if (offset < 0) {
        offset += n;
      }
      if (offset < 0) {
        return 0;
      }
      if (offset > n) {
        return n;
      }
      return offset;
    };
    int64_t start = slice_normalize(start_raw);
    int64_t end = slice_normalize(end_raw);
    start = std::min(start, end);
    std::vector<Value> sliced(
        elements.begin() + static_cast<std::ptrdiff_t>(start),
        elements.begin() + static_cast<std::ptrdiff_t>(end));
    return Value::Array(std::move(sliced), array.ArrayElementSqlType());
  }

  if (name == "euclidean_distance") {
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
        return static_cast<double>(v.value.int_value);
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

  if (name == "element") {
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
    if (array.ArrayElements().size() != 1) {
      return StatusError(StatusCode::kInvalidArgument, "More than one element");
    }
    return array.ArrayElements().front();
  }

  return StatusError(StatusCode::kInvalidArgument,
                     "unsupported function " + name);
}

}  // namespace tinylamb::relational_detail
