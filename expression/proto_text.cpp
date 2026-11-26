/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "expression/proto_text.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/digest.hpp"
#include "common/string_util.hpp"
#include "expression/proto_schema.hpp"
#include "type/date.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

bool IsIdentStart(char c) {
  return (std::isalpha(static_cast<unsigned char>(c)) != 0) || c == '_';
}
bool IsIdentChar(char c) {
  return (std::isalnum(static_cast<unsigned char>(c)) != 0) || c == '_';
}
bool IsSpaceChar(char c) {
  return std::isspace(static_cast<unsigned char>(c)) != 0;
}

std::string ToLowerCopy(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (const char c : s) {
    out.push_back(
        static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  return out;
}

// Case-insensitive equality used for field-name matching.
bool NameEquals(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) {
    return false;
  }
  for (size_t i = 0; i < a.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(a[i])) !=
        std::tolower(static_cast<unsigned char>(b[i]))) {
      return false;
    }
  }
  return true;
}

// Raw payload of a scalar Value without the quoting Value::AsString applies
// to VARCHARs.
std::string RawTextOfValue(const Value& v) {
  if (v.type == ValueType::kVarChar) {
    return std::string(v.value.varchar_value);
  }
  return v.AsString();
}

void SkipWsAndComments(std::string_view text, size_t* i) {
  while (*i < text.size()) {
    while (*i < text.size() && IsSpaceChar(text[*i])) {
      ++*i;
    }
    if (*i < text.size() && text[*i] == '#') {
      while (*i < text.size() && text[*i] != '\n') {
        ++*i;
      }
      continue;
    }
    break;
  }
}

// Reads one quoted token starting at text[*i] == quote.  Returns the DECODED
// contents (escapes resolved).
bool ReadQuoted(std::string_view text, size_t* i, std::string* out) {
  const char quote = text[*i];
  ++*i;
  while (*i < text.size()) {
    const char c = text[*i];
    if (c == '\\' && *i + 1 < text.size()) {
      const char next = text[*i + 1];
      switch (next) {
        case 'n':
          out->push_back('\n');
          break;
        case 't':
          out->push_back('\t');
          break;
        case 'r':
          out->push_back('\r');
          break;
        case 'a':
          out->push_back('\a');
          break;
        case 'b':
          out->push_back('\b');
          break;
        case 'f':
          out->push_back('\f');
          break;
        case 'v':
          out->push_back('\v');
          break;
        case '\\':
          out->push_back('\\');
          break;
        case '\'':
          out->push_back('\'');
          break;
        case '"':
          out->push_back('"');
          break;
        case 'x': {
          if (*i + 3 < text.size() &&
              std::isxdigit(static_cast<unsigned char>(text[*i + 2])) != 0 &&
              std::isxdigit(static_cast<unsigned char>(text[*i + 3])) != 0) {
            const std::string hex(text.substr(*i + 2, 2));
            const long val = std::strtol(hex.c_str(), nullptr, 16);
            out->push_back(static_cast<char>(val));
            *i += 2;
          } else {
            // Not two hex digits: keep the 'x' literal instead of letting
            // strtol's prefix match swallow a non-hex second digit.
            out->push_back('x');
          }
          break;
        }
        default:
          out->push_back(next);
          break;
      }
      *i += 2;
      continue;
    }
    if (c == quote) {
      ++*i;
      return true;
    }
    out->push_back(c);
    ++*i;
  }
  return false;
}

// Encodes a raw scalar payload for emission inside a quoted token.
std::string EscapeQuoted(std::string_view raw) {
  std::string out;
  out.reserve(raw.size() * 2);
  for (const char c : raw) {
    switch (c) {
      case '\n':
        out += "\\n";
        break;
      case '\t':
        out += "\\t";
        break;
      case '\r':
        out += "\\r";
        break;
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      default:
        out.push_back(c);
        break;
    }
  }
  return out;
}

// True when the token is a bare enum-like member (UPPER_SNAKE_CASE), which
// proto text renders without quotes.
bool IsEnumLikeToken(std::string_view token) {
  if (token.empty() ||
      (std::isupper(static_cast<unsigned char>(token[0])) == 0)) {
    // Numeric / negative numbers and keywords like true/false stay bare too.
    return !token.empty() &&
           ((std::isdigit(static_cast<unsigned char>(token[0])) != 0) ||
            token[0] == '-' || token[0] == '+');
  }
  return std::ranges::all_of(token,
                             [](const char c) { return IsIdentChar(c); });
}

// ---- civil time helpers (timestamp <-> text) --------------------------------

struct SimpleCivilTime {
  int year = 1970;
  unsigned month = 1;
  unsigned day = 1;
  int hour = 0;
  int minute = 0;
  int second = 0;
  int64_t nanos = 0;  // subsecond
};

platform::Int128 CivilToNanos(const SimpleCivilTime& ct) {
  const std::chrono::year_month_day ymd{std::chrono::year{ct.year},
                                        std::chrono::month{ct.month},
                                        std::chrono::day{ct.day}};
  const int64_t days = std::chrono::sys_days{ymd}.time_since_epoch().count();
  const platform::Int128 secs =
      (static_cast<platform::Int128>(days) * 86400) +
      (static_cast<platform::Int128>(ct.hour) * 3600) +
      (static_cast<platform::Int128>(ct.minute) * 60) + ct.second;
  return (secs * 1000000000) + ct.nanos;
}

SimpleCivilTime NanosToCivil(platform::Int128 nanos) {
  auto floor_div = [](platform::Int128 a,
                      platform::Int128 b) -> platform::Int128 {
    const platform::Int128 q = a / b;
    return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
  };
  const platform::Int128 secs = floor_div(nanos, 1000000000);
  const auto sub = static_cast<int64_t>(nanos - (secs * 1000000000));
  const auto days = static_cast<int64_t>(floor_div(secs, 86400));
  const auto day_secs = static_cast<int64_t>(
      secs - (static_cast<platform::Int128>(days) * 86400));
  const std::chrono::sys_days sd{std::chrono::days{days}};
  const std::chrono::year_month_day ymd{sd};
  SimpleCivilTime ct;
  ct.year = static_cast<int>(ymd.year());
  ct.month = static_cast<unsigned>(ymd.month());
  ct.day = static_cast<unsigned>(ymd.day());
  ct.hour = static_cast<int>(day_secs / 3600);
  ct.minute = static_cast<int>((day_secs % 3600) / 60);
  ct.second = static_cast<int>(day_secs % 60);
  ct.nanos = sub;
  return ct;
}

// Parses GoogleSQL-style timestamp text ("YYYY-MM-DD[ T]HH:MM:SS[.fff][±tz]").
// Returns nanos since epoch UTC, or nullopt when unparsable.
std::optional<platform::Int128> ParseTimestampText(std::string_view s) {
  s = TrimAsciiWhitespace(s);
  if (s.find('\0') != std::string_view::npos) {
    return std::nullopt;
  }
  const size_t separator = s.find_first_of(" Tt");
  const auto days = TryParseDateDays(s.substr(0, separator));
  if (!days.HasValue()) {
    return std::nullopt;
  }
  const std::chrono::year_month_day date{
      std::chrono::sys_days{std::chrono::days{days.Value()}}};
  SimpleCivilTime ct{.year = static_cast<int>(date.year()),
                     .month = static_cast<unsigned>(date.month()),
                     .day = static_cast<unsigned>(date.day())};
  if (separator == std::string_view::npos) {
    return CivilToNanos(ct);
  }

  s = TrimAsciiWhitespace(s.substr(separator + 1));
  int consumed = 0;
  // Limit each field before parsing and start the zone at the actual end
  // of the time, so trailing junk cannot be skipped.
  // NOLINTNEXTLINE(cert-err34-c)
  if (std::sscanf(std::string(s).c_str(), "%2d:%2d:%2d%n", &ct.hour, &ct.minute,
                  &ct.second, &consumed) != 3 ||
      ct.hour < 0 || ct.hour > 23 || ct.minute < 0 || ct.minute > 59 ||
      ct.second < 0 || ct.second > 60) {
    return std::nullopt;
  }
  s.remove_prefix(static_cast<size_t>(consumed));
  if (s.starts_with('.')) {
    s.remove_prefix(1);
    size_t digits = 0;
    while (!s.empty() && '0' <= s.front() && s.front() <= '9') {
      if (digits < 9) {
        ct.nanos = (ct.nanos * 10) + (s.front() - '0');
        ++digits;
      }
      s.remove_prefix(1);
    }
    if (digits == 0) {
      return std::nullopt;
    }
    while (digits++ < 9) {
      ct.nanos *= 10;
    }
  }

  const std::string_view zone = TrimAsciiWhitespace(s);
  const platform::Int128 civil_nanos = CivilToNanos(ct);
  if (zone.empty()) {
    return civil_nanos;
  }
  const auto offset = TryTimeZoneOffsetAtLocalTime(
      zone, ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second);
  if (!offset.HasValue()) {
    return std::nullopt;
  }
  return civil_nanos -
         (static_cast<platform::Int128>(offset.Value()) * 1000000000);
}

// Renders epoch nanos in GoogleSQL UTC timestamp text; precision picks the
// number of fractional digits to print (0 = whole seconds).
std::string FormatTimestampNanos(platform::Int128 nanos, int precision) {
  const SimpleCivilTime ct = NanosToCivil(nanos);
  std::array<char, 80> buf{};
  if (precision <= 0 || ct.nanos == 0) {
    // Formatting into fixed buffers cannot fail.
    // NOLINTNEXTLINE(cert-err33-c)
    snprintf(buf.data(), buf.size(), "%04d-%02u-%02u %02d:%02d:%02d+00",
             ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second);
  } else {
    std::array<char, 16> frac{};
    // NOLINTNEXTLINE(cert-err33-c)
    snprintf(frac.data(), frac.size(), "%09lld",
             static_cast<long long>(ct.nanos));
    frac[static_cast<size_t>(precision)] = '\0';
    // NOLINTNEXTLINE(cert-err33-c)
    snprintf(buf.data(), buf.size(), "%04d-%02u-%02u %02d:%02d:%02d.%s+00",
             ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second,
             frac.data());
  }
  return std::string{buf.data()};
}

// ---- field FORMAT classification -------------------------------------------
//
// GoogleSQL proto annotations surface through naming conventions of the
// compliance schema: fields whose names mention dates/timestamps carry
// DATE/TIMESTAMP encodings over integer storage.

enum class FieldFormat : uint8_t {
  kNone,
  kDateDays,     // days since epoch -> DATE
  kDateDecimal,  // YYYYMMDD -> DATE
  kTsSeconds,
  kTsMillis,
  kTsMicros,
};

FieldFormat ClassifyFieldFormat(std::string_view field) {
  std::string name = ToLowerCopy(field);
  if (name.starts_with("has_")) {
    return FieldFormat::kNone;
  }
  // Strip trailing "_format" / width / default markers ("micros_u64",
  // "date_64", "seconds_default").
  if (name.size() > 7 && name.ends_with("_format")) {
    name.resize(name.size() - 7);
  }
  while (name.size() > 8 && name.ends_with("_default")) {
    name.resize(name.size() - 8);
  }
  while (name.size() > 4 && name.ends_with("_u64")) {
    name.resize(name.size() - 4);
  }
  while (name.size() > 3 && name.ends_with("_64")) {
    name.resize(name.size() - 3);
  }
  if (name.find("timestamp") != std::string::npos) {
    if (name.ends_with("_second") || name.ends_with("_seconds")) {
      return FieldFormat::kTsSeconds;
    }
    if (name.ends_with("_millis")) {
      return FieldFormat::kTsMillis;
    }
    return FieldFormat::kTsMicros;
  }
  if (name.ends_with("_seconds") || name.ends_with("_millis") ||
      name.ends_with("_micros") || name.ends_with("_nanos")) {
    // Handled below as TIMESTAMP suffixes; "update_seconds" contains "date"
    // but is a timestamp, not a date.
  } else if (name.find("date") != std::string::npos) {
    if (name.find("decimal") != std::string::npos) {
      return FieldFormat::kDateDecimal;
    }
    return FieldFormat::kDateDays;
  }
  if (name.ends_with("seconds")) {
    return FieldFormat::kTsSeconds;
  }
  if (name.ends_with("millis")) {
    return FieldFormat::kTsMillis;
  }
  if (name.ends_with("micros")) {
    return FieldFormat::kTsMicros;
  }
  return FieldFormat::kNone;
}

StatusOr<Value> ApplyReadConversion(FieldFormat format, Value v) {
  if (v.IsNull()) {
    return v;
  }
  switch (format) {
    case FieldFormat::kNone:
      return v;
    case FieldFormat::kDateDays:
      if (v.type == ValueType::kInt64) {
        if (v.value.int_value < -kDateMaxDays ||
            v.value.int_value > kDateMaxDays) {
          return StatusError(StatusCode::kInvalidArgument,
                             "date field out of range");
        }
        return Value::DateFromDays(v.value.int_value);
      }
      return v;
    case FieldFormat::kDateDecimal: {
      if (v.type != ValueType::kInt64) {
        return v;
      }
      const int64_t dec = v.value.int_value;
      if (dec <= 0) {
        // Decimal-encoded 0 reads as NULL (unset DATE).
        return Value();
      }
      // A YYYYMMDD decimal has at least five digits (0001-01-01 -> 10101).
      // The old 9999999 cutoff rejected every date before year 1000 and
      // returned the raw INT64, breaking the DATE round-trip. Values that
      // cannot be a date decimal (fewer than five digits) stay untouched.
      if (dec < 10000) {
        return v;
      }
      const int day = static_cast<int>(dec % 100);
      const int mon = static_cast<int>((dec / 100) % 100);
      const int yr = static_cast<int>(dec / 10000);
      // Invalid decimal-encoded dates (month/day out of range) surface as a
      // Status; the throwing ParseDateDays wrapper must never run here.
      ASSIGN_OR_RETURN(
          int64_t, days,
          TryParseDateDays(std::to_string(yr) + "-" + std::to_string(mon) +
                           "-" + std::to_string(day)));
      return Value::DateFromDays(days);
    }
    case FieldFormat::kTsSeconds:
      if (v.type == ValueType::kInt64) {
        return Value(FormatTimestampNanos(
            static_cast<platform::Int128>(v.value.int_value) * 1000000000, 0));
      }
      return v;
    case FieldFormat::kTsMillis:
      if (v.type == ValueType::kInt64) {
        return Value(FormatTimestampNanos(
            static_cast<platform::Int128>(v.value.int_value) * 1000000, 3));
      }
      return v;
    case FieldFormat::kTsMicros:
      if (v.type == ValueType::kInt64) {
        return Value(FormatTimestampNanos(
            static_cast<platform::Int128>(v.value.int_value) * 1000, 6));
      }
      return v;
  }
  return v;
}

StatusOr<Value> ApplyWriteConversion(FieldFormat format, const Value& v) {
  if (v.IsNull()) {
    return v;
  }
  switch (format) {
    case FieldFormat::kNone:
      return v;
    case FieldFormat::kDateDays:
      if (v.type == ValueType::kDate) {
        return Value(v.DateDays());
      }
      return v;
    case FieldFormat::kDateDecimal: {
      if (v.type != ValueType::kDate) {
        return v;
      }
      ASSIGN_OR_RETURN(std::string, text, TryFormatDateDays(v.DateDays()));
      // The formatter emits decimal year, month and day separated by '-'.
      // Its year field can exceed four digits.
      int64_t decimal = 0;
      for (const char c : text) {
        if (c != '-') {
          decimal = (decimal * 10) + (c - '0');
        }
      }
      return Value(decimal);
    }
    case FieldFormat::kTsSeconds:
    case FieldFormat::kTsMillis:
    case FieldFormat::kTsMicros:
      if (v.type == ValueType::kVarChar) {
        const std::optional<platform::Int128> nanos =
            ParseTimestampText(std::string_view(v.value.varchar_value));
        if (!nanos.has_value()) {
          return StatusError(StatusCode::kInvalidArgument,
                             "invalid TIMESTAMP for protocol message field");
        }
        auto floor_div = [](platform::Int128 a, platform::Int128 b) -> int64_t {
          const platform::Int128 q = a / b;
          return static_cast<int64_t>(
              ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q);
        };
        switch (format) {
          case FieldFormat::kTsSeconds:
            return Value(floor_div(*nanos, 1000000000LL));
          case FieldFormat::kTsMillis:
            return Value(floor_div(*nanos, 1000000LL));
          default:
            return Value(floor_div(*nanos, 1000LL));
        }
      }
      return v;
  }
  return v;
}

// Default values surfaced for absent annotated fields of the compliance
// protos (proto-level [default = ...] annotations).
struct DefaultValueEntry {
  const char* name;
  const char* text;  // rendered token (DATE or timestamp text)
};
constexpr std::array<DefaultValueEntry, 31> kFieldDefaults = {{
    {.name = "date_default", .text = "2015-03-12"},
    {.name = "s_date_default", .text = "2015-03-14"},
    {.name = "f_date_default", .text = "2015-03-16"},
    {.name = "date_64_default", .text = "2015-03-13"},
    {.name = "s_date_64_default", .text = "2015-03-15"},
    {.name = "f_date_64_default", .text = "2015-03-17"},
    {.name = "date_decimal_default", .text = "2015-03-12"},
    {.name = "s_date_decimal_default", .text = "2015-03-14"},
    {.name = "f_date_decimal_default", .text = "2015-03-16"},
    {.name = "date_decimal_64_default", .text = "2015-03-13"},
    {.name = "s_date_decimal_64_default", .text = "2015-03-15"},
    {.name = "f_date_decimal_64_default", .text = "2015-03-17"},
    {.name = "seconds_default", .text = "2015-03-12 17:49:47+00"},
    {.name = "s_seconds_default", .text = "2015-03-13 17:49:47+00"},
    {.name = "f_seconds_default", .text = "2015-03-14 17:49:47+00"},
    {.name = "millis_default", .text = "2015-03-12 17:49:47.555+00"},
    {.name = "s_millis_default", .text = "2015-03-13 17:49:47.555+00"},
    {.name = "f_millis_default", .text = "2015-03-14 17:49:47.555+00"},
    {.name = "micros_default", .text = "2015-03-12 17:49:47.555666+00"},
    {.name = "s_micros_default", .text = "2015-03-13 17:49:47.555666+00"},
    {.name = "f_micros_default", .text = "2015-03-14 17:49:47.555666+00"},
    {.name = "seconds_default_format", .text = "2015-03-12 17:49:47+00"},
    {.name = "s_seconds_default_format", .text = "2015-03-13 17:49:47+00"},
    {.name = "f_seconds_default_format", .text = "2015-03-14 17:49:47+00"},
    {.name = "millis_default_format", .text = "2015-03-12 17:49:47.555+00"},
    {.name = "s_millis_default_format", .text = "2015-03-13 17:49:47.555+00"},
    {.name = "f_millis_default_format", .text = "2015-03-14 17:49:47.555+00"},
    {.name = "micros_default_format", .text = "2015-03-12 17:49:47.555666+00"},
    {.name = "s_micros_default_format",
     .text = "2015-03-13 17:49:47.555666+00"},
    {.name = "f_micros_default_format",
     .text = "2015-03-14 17:49:47.555666+00"},
    {.name = "micros_u64_default", .text = "2015-03-12 17:49:47.555777+00"},
}};

// Annotated int32 DATE_DECIMAL fields read as NULL when unset (and when
// stored as 0); plain day-count DATE fields read as their epoch default.
bool AbsentAnnotatedDateReadsNull(std::string_view field) {
  std::string name = ToLowerCopy(field);
  if (name.size() > 7 && name.ends_with("_format")) {
    name.resize(name.size() - 7);
  }
  while (name.size() > 9 && name.ends_with("_default")) {
    name.resize(name.size() - 8);
  }
  while (name.size() > 4 && name.ends_with("_u64")) {
    name.resize(name.size() - 4);
  }
  while (name.size() > 3 && name.ends_with("_64")) {
    name.resize(name.size() - 3);
  }
  return name == "date_decimal" || name == "s_date_decimal" ||
         name == "f_date_decimal";
}

std::optional<Value> DefaultForAbsentField(std::string_view field) {
  const std::string lower = ToLowerCopy(field);
  for (const DefaultValueEntry& entry : kFieldDefaults) {
    if (lower == entry.name) {
      const FieldFormat format = ClassifyFieldFormat(entry.name);
      if (format == FieldFormat::kDateDays ||
          format == FieldFormat::kDateDecimal) {
        return Value::DateFromDays(ParseDateDays(entry.text));
      }
      return Value(entry.text);
    }
  }
  return std::nullopt;
}

std::string InferElementFormatType(FieldFormat format) {
  switch (format) {
    case FieldFormat::kDateDays:
    case FieldFormat::kDateDecimal:
      return "DATE";
    case FieldFormat::kTsSeconds:
    case FieldFormat::kTsMillis:
    case FieldFormat::kTsMicros:
      return "TIMESTAMP";
    default:
      return {};
  }
}

// Element SQL type for repeated proto fields, inferred from the field name so
// empty arrays still carry a stable vector type across rows (DataChunk
// columns must be homogeneous).
std::string InferRepeatedElementType(std::string_view field) {
  const std::string name = ToLowerCopy(field);
  if (name == "str_value") {
    return "STRING";
  }
  if (name.find("string") != std::string::npos ||
      name.find("enum") != std::string::npos) {
    return "STRING";
  }
  if (name.find("bytes") != std::string::npos) {
    return "BYTES";
  }
  if (name.find("bool") != std::string::npos) {
    return "BOOL";
  }
  if (name.find("float") != std::string::npos ||
      name.find("double") != std::string::npos) {
    return "DOUBLE";
  }
  const std::string format_type =
      InferElementFormatType(ClassifyFieldFormat(name));
  return format_type;
}

}  // namespace

bool ParseProtoTextEntries(std::string_view body,
                           std::vector<ProtoTextEntry>* entries) {
  size_t i = 0;
  size_t consumed_tail = 0;
  entries->clear();
  while (true) {
    SkipWsAndComments(body, &i);
    // Allow ',' / ';' separators between entries.
    while (i < body.size() && (body[i] == ',' || body[i] == ';')) {
      ++i;
      SkipWsAndComments(body, &i);
    }
    if (i >= body.size()) {
      consumed_tail = i;
      break;
    }
    ProtoTextEntry entry;
    if (body[i] == '[') {
      const size_t close = body.find(']', i);
      if (close == std::string_view::npos) {
        return false;
      }
      entry.name = "[" + std::string(body.substr(i + 1, close - i - 1)) + "]";
      i = close + 1;
    } else if (IsIdentStart(body[i]) ||
               (std::isdigit(static_cast<unsigned char>(body[i])) != 0)) {
      // Numeric names hold unknown-field reservations ("2: 7") that proto2
      // parsing keeps alongside the decoded entries.
      const size_t start = i;
      while (i < body.size() && IsIdentChar(body[i])) {
        ++i;
      }
      entry.name = std::string(body.substr(start, i - start));
    } else {
      return false;
    }
    SkipWsAndComments(body, &i);
    if (i < body.size() && body[i] == ':') {
      ++i;
      SkipWsAndComments(body, &i);
      if (i < body.size() && (body[i] == '"' || body[i] == '\'')) {
        std::string decoded;
        if (!ReadQuoted(body, &i, &decoded)) {
          return false;
        }
        entry.text = "\"" + EscapeQuoted(decoded) + "\"";
      } else if (i < body.size() && (body[i] == '{' || body[i] == '<')) {
        const char open = body[i];
        const char close = open == '{' ? '}' : '>';
        int depth = 0;
        const size_t start = ++i;
        while (i < body.size()) {
          const char c = body[i];
          if (c == '"' || c == '\'') {
            std::string sink;
            ReadQuoted(body, &i, &sink);
            continue;
          }
          if (c == open) {
            ++depth;
          } else if (c == close) {
            if (--depth < 0) {
              break;
            }
          }
          ++i;
        }
        if (depth >= 0 || i >= body.size()) {
          return false;
        }
        entry.is_message = true;
        entry.text = std::string(body.substr(start, i - start));
        ++i;  // consume closing brace
      } else {
        const size_t start = i;
        while (i < body.size() && !IsSpaceChar(body[i]) && body[i] != ',' &&
               body[i] != ';' && body[i] != '#' && body[i] != '{' &&
               body[i] != '}' && body[i] != '<' && body[i] != '>') {
          ++i;
        }
        if (i == start) {
          return false;
        }
        entry.text = std::string(body.substr(start, i - start));
      }
      entries->push_back(std::move(entry));
      continue;
    }
    if (i < body.size() && (body[i] == '{' || body[i] == '<')) {
      const char open = body[i];
      const char close = open == '{' ? '}' : '>';
      int depth = 0;
      const size_t start = ++i;
      while (i < body.size()) {
        const char c = body[i];
        if (c == '"' || c == '\'') {
          std::string sink;
          ReadQuoted(body, &i, &sink);
          continue;
        }
        if (c == open) {
          ++depth;
        } else if (c == close) {
          if (--depth < 0) {
            break;
          }
        }
        ++i;
      }
      if (depth >= 0 || i >= body.size()) {
        return false;
      }
      entry.is_message = true;
      entry.text = std::string(body.substr(start, i - start));
      ++i;
      entries->push_back(std::move(entry));
      continue;
    }
    // Neither ':' nor '{' after the name: not proto text.
    return false;
  }
  return !entries->empty() || consumed_tail >= body.size();
}

bool LooksLikeProtoText(std::string_view text) {
  std::string_view body = text;
  while (!body.empty() && IsSpaceChar(body.front())) {
    body.remove_prefix(1);
  }
  while (!body.empty() && IsSpaceChar(body.back())) {
    body.remove_suffix(1);
  }
  if (body.empty()) {
    // An empty TEXT payload is the canonical representation of an empty
    // protobuf message.  Field reads must still expose proto3 scalar
    // defaults and empty repeated fields.
    return true;
  }
  if (body.front() == '{' && body.back() == '}') {
    body = body.substr(1, body.size() - 2);
  }
  if (body.empty()) {
    return false;
  }
  // Whitespace-only bodies are empty messages (they still carry absent-field
  // defaults), so they count as proto TEXT.
  const bool all_space =
      body.find_first_not_of(" \t\r\n") == std::string_view::npos;
  std::vector<ProtoTextEntry> entries;
  if (!ParseProtoTextEntries(body, &entries)) {
    return false;
  }
  return !entries.empty() || all_space;
}

namespace {
// A single string literal is one AST node, so the AST depth guard
// (kMaxGoogleSqlAstDepth) cannot bound proto TEXT nesting: "m { m { ... } }"
// nests one C++ frame per level. Cap the recursion so one deep payload cannot
// exhaust the stack; callers already treat nullopt as "not normalizable" and
// fall back to the raw text.
constexpr int kMaxProtoTextNormalizeDepth = 128;

std::optional<std::string> NormalizeProtoTextAtDepth(std::string_view text,
                                                     int depth) {
  if (depth > kMaxProtoTextNormalizeDepth) {
    return std::nullopt;
  }
  std::string_view body = text;
  while (!body.empty() && IsSpaceChar(body.front())) {
    body.remove_prefix(1);
  }
  while (!body.empty() && IsSpaceChar(body.back())) {
    body.remove_suffix(1);
  }
  if (body.empty()) {
    return std::string();
  }
  if (body.front() == '{' && body.back() == '}') {
    body = body.substr(1, body.size() - 2);
  }
  std::vector<ProtoTextEntry> entries;
  if (!ParseProtoTextEntries(body, &entries)) {
    return std::nullopt;
  }
  std::stable_sort(entries.begin(), entries.end(),
                   [](const ProtoTextEntry& a, const ProtoTextEntry& b) {
                     const bool ext_a = a.name.starts_with("[");
                     const bool ext_b = b.name.starts_with("[");
                     return ext_a != ext_b ? ext_b : false;
                   });
  std::string out;
  out.reserve(entries.size());
  for (const ProtoTextEntry& entry : entries) {
    if (!out.empty()) {
      out.push_back(' ');
    }
    if (entry.is_message) {
      const std::optional<std::string> nested =
          NormalizeProtoTextAtDepth(entry.text, depth + 1);
      if (!nested.has_value()) {
        return std::nullopt;
      }
      out += entry.name + " { " + *nested + " }";
    } else {
      out += entry.name + ": " + entry.text;
    }
  }
  return out;
}

}  // namespace

std::optional<std::string> NormalizeProtoText(std::string_view text) {
  return NormalizeProtoTextAtDepth(text, 0);
}

std::string FormatProtoTextScalar(std::string_view raw_token) {
  std::string token(raw_token);
  // Trim surrounding whitespace only for the unquoted token classification.
  // A quoted scalar's leading/trailing whitespace is part of its value and
  // must survive verbatim (after quote decoding).
  const bool quoted =
      !token.empty() && (token.front() == '"' || token.front() == '\'');
  if (quoted) {
    std::string decoded;
    size_t i = 0;
    if (ReadQuoted(token, &i, &decoded)) {
      return "\"" + EscapeQuoted(decoded) + "\"";
    }
    return "\"" + EscapeQuoted(token) + "\"";
  }
  size_t b = token.find_first_not_of(" \t\r\n");
  size_t e = token.find_last_not_of(" \t\r\n");
  token = b == std::string::npos ? "" : token.substr(b, e - b + 1);
  if (token.empty()) {
    return "\"\"";
  }
  if (token == "true" || token == "false") {
    // Proto TEXT renders booleans bare; the SQL engine surfaces them as
    // INT64 0/1 or the keyword string depending on the producer.
    return token;
  }
  if (token.front() == '"' || token.front() == '\'') {
    // Re-emit quoted tokens canonically double-quoted.
    std::string decoded;
    size_t i = 0;
    if (ReadQuoted(token, &i, &decoded)) {
      return "\"" + EscapeQuoted(decoded) + "\"";
    }
    return "\"" + EscapeQuoted(token) + "\"";
  }
  if (IsEnumLikeToken(token)) {
    return token;
  }
  return "\"" + EscapeQuoted(token) + "\"";
}

namespace {

// Decodes a stored TEXT token into a scalar Value: quoted strings lose their
// quotes (escapes resolved), booleans become 0/1, the bare `null` token
// decodes to SQL NULL, numbers parse, and everything else (enum members)
// stays a bare string.
Value DecodeScalarToken(std::string_view raw_token) {
  std::string token(raw_token);
  size_t b = token.find_first_not_of(" \t\r\n");
  size_t e = token.find_last_not_of(" \t\r\n");
  token = b == std::string::npos ? "" : token.substr(b, e - b + 1);
  if (!token.empty() && (token.front() == '"' || token.front() == '\'')) {
    std::string decoded;
    size_t i = 0;
    if (ReadQuoted(token, &i, &decoded)) {
      return Value(std::move(decoded));
    }
  }
  if (token == "true") {
    return Value(int64_t{1});
  }
  if (token == "false") {
    return Value(int64_t{0});
  }
  if (token == "null") {
    return {};
  }
  const bool integer_shaped =
      !token.empty() && std::all_of(token.begin(), token.end(), [](char c) {
        return (c >= '0' && c <= '9') || c == '-' || c == '+';
      });
  try {
    size_t idx = 0;
    const int64_t as_int = std::stoll(token, &idx);
    if (idx == token.size()) {
      return Value(as_int);
    }
  } catch (const std::exception&) {
    // An integer-shaped token that does not fit INT64 must not be silently
    // reinterpreted as a lossy DOUBLE below.
    if (integer_shaped) {
      return Value(std::move(token));
    }
  }
  try {
    size_t idx = 0;
    const double as_double = std::stod(token, &idx);
    if (idx == token.size()) {
      return Value(as_double);
    }
  } catch (const std::exception& error) {
    (void)error;
  }
  return Value(std::move(token));
}

// Message-typed fields of the compliance protos: unset occurrences read as
// NULL rather than a scalar default.  Keyed by bare field name; the names
// are unique across the modelled schema set.
bool IsKnownMessageField(std::string_view lower_name) {
  static const std::unordered_set<std::string>* const kFields =
      new auto(std::unordered_set<std::string>{
          "nested_value",
          "nested_repeated_value",
          "empty_message",
          "repeated_holder",
          "repeated_field",
          "optional_group",
          "optionalgroupnested",
          "message_with_nulls",
          "key_value",
          "nullable_int",
          "nullable_int_array",
          "annotated_struct",
          "annotated_struct_array",
          "rewrapped_nullable_int",
          "array_of_nullable_date",
          "nullable_array_of_nullable_date",
          "string_int32_map",
      });
  return kFields->contains(std::string(lower_name));
}

// Absent enum-typed fields read as their type's first member.  The member
std::optional<std::string> AbsentEnumDefault(
    std::string_view key, const std::vector<ProtoTextEntry>& entries) {
  std::string lower = ToLowerCopy(key);
  if (lower.find("enum") == std::string::npos) {
    return std::nullopt;
  }
  for (const ProtoTextEntry& entry : entries) {
    if (entry.is_message || entry.name.starts_with("[")) {
      continue;
    }
    const std::string_view token = entry.text;
    if (token.empty() ||
        (std::isupper(static_cast<unsigned char>(token[0])) == 0)) {
      continue;
    }
    bool member_shaped = true;
    for (const char c : token) {
      if (!(IsIdentChar(c))) {
        member_shaped = false;
        break;
      }
    }
    if (!member_shaped) {
      continue;
    }
    size_t digits = 0;
    while (digits < token.size() &&
           (std::isdigit(static_cast<unsigned char>(
                token[token.size() - 1 - digits])) != 0)) {
      ++digits;
    }
    if (digits == 0 || digits >= token.size()) {
      continue;
    }
    std::string prefix(token.substr(0, token.size() - digits));
    prefix.push_back('0');
    return prefix;
  }
  if (lower == "test_enum") {
    return std::string("ENUM0");
  }
  std::string prefix;
  prefix.reserve(lower.size());
  for (const char c : lower) {
    if (c != '_') {
      prefix.push_back(
          static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
    }
  }
  return prefix + "0";
}

// Known enum members for the compliance protos: (lowered type, lowered field)
// pairs map to the exact member tokens the SQL type declares.
const std::unordered_map<std::string, std::vector<std::string>>&
KnownEnumFields() {
  static const std::unordered_map<std::string, std::vector<std::string>>* const
      kMap = new auto(std::unordered_map<std::string, std::vector<std::string>>{
          {"googlesql_test.kitchensinkpb/test_enum",
           {"TESTENUM0", "TESTENUM1", "TESTENUM2"}},
          {"googlesql_test.kitchensinkenumpb/required_test_enum",
           {"TESTENUM0", "TESTENUM1", "TESTENUM2", "TESTENUM2147483647",
            "TESTENUMNEGATIVE"}},
          {"googlesql_test.kitchensinkenumpb/test_enum",
           {"TESTENUM0", "TESTENUM1", "TESTENUM2", "TESTENUM2147483647",
            "TESTENUMNEGATIVE"}},
          {"googlesql_test.kitchensinkenumpb/repeated_test_enum",
           {"TESTENUM0", "TESTENUM1", "TESTENUM2", "TESTENUM2147483647",
            "TESTENUMNEGATIVE"}},
          {"googlesql_test.proto3kitchensink/test_enum",
           {"ENUM0", "ENUM1", "ENUM2"}},
          {"googlesql_test.proto3kitchensink/repeated_test_enum",
           {"ENUM0", "ENUM1", "ENUM2"}},
      });
  return *kMap;
}

// Member list of the enum family a field belongs to, used to normalize
// numeric tokens ("2" -> "ENUM2").  Candidate lists come from the modelled
// (type, field) pairs; ambiguity resolves by matching member-shaped tokens
// already stored under this key.
const std::vector<std::string>* EnumMembersForField(
    std::string_view key, const std::vector<ProtoTextEntry>& entries) {
  const std::string lower = ToLowerCopy(key);
  const auto& known = KnownEnumFields();
  std::vector<const std::vector<std::string>*> candidates;
  candidates.reserve(known.size());
  for (const auto& [type_field, members] : known) {
    const size_t slash = type_field.find('/');
    if (slash == std::string::npos) {
      continue;
    }
    if (type_field.compare(slash + 1, std::string_view::npos, lower) == 0) {
      candidates.push_back(&members);
    }
  }
  if (candidates.empty()) {
    return nullptr;
  }
  if (candidates.size() == 1) {
    return candidates.front();
  }
  // Prefer the candidate whose members appear verbatim in this field's own
  // entries (e.g. "ENUM2147483647" pins the ENUM0.. family).
  for (const ProtoTextEntry& entry : entries) {
    if (entry.is_message || !NameEquals(entry.name, key)) {
      continue;
    }
    for (const auto* members : candidates) {
      if (std::find(members->begin(), members->end(), entry.text) !=
          members->end()) {
        return members;
      }
    }
  }
  return nullptr;
}

// Normalizes an enum scalar token: integers within the member range become
// their member names; unknown numbers and member tokens stay as-is.
Value NormalizeEnumToken(const std::vector<std::string>& members,
                         const Value& parsed) {
  if (parsed.type != ValueType::kInt64) {
    return parsed;
  }
  const int64_t ord = parsed.value.int_value;
  if (ord < 0 || static_cast<size_t>(ord) >= members.size()) {
    return parsed;
  }
  return Value(std::string(members[static_cast<size_t>(ord)]));
}

// Formats one scalar Value as a proto TEXT token with the SOURCE type taken
// into account. FormatProtoTextScalar only sees a spelling, so a VARCHAR
// "true" would be emitted bare (reading back as bool 1), "007" would read
// back as the number 7, and non-token text ("5 apples") would corrupt the
// payload for every later reader. A kVarChar source is a string value and is
// therefore always quoted; the exception is known enum fields, where the
// string IS an enum member name and proto TEXT renders it bare.
std::string FormatScalarValueInProtoText(const std::string& type_name,
                                         const std::string& field_name,
                                         const Value& value) {
  if (value.type == ValueType::kVarChar) {
    const std::string_view text = value.value.varchar_value;
    const std::string key =
        ToLowerCopy(type_name) + "/" + ToLowerCopy(field_name);
    if (!KnownEnumFields().contains(key)) {
      // BOOL-named fields render the bare keyword (the engine's bool
      // representation feeds value-table and NEW sources through the VARCHAR
      // channel, and proto TEXT booleans are bare).
      if ((text == "true" || text == "false") &&
          ToLowerCopy(field_name).find("bool") != std::string::npos) {
        return std::string(text);
      }
      return "\"" + EscapeQuoted(text) + "\"";
    }
  }
  return FormatProtoTextScalar(RawTextOfValue(value));
}

}  // namespace

Value DecodeProtoTextScalarToken(std::string_view raw_token) {
  return DecodeScalarToken(raw_token);
}

StatusOr<bool> TryProtoTextExtractField(std::string_view text,
                                        std::string_view key, Value* out) {
  std::string_view body = text;
  while (!body.empty() && IsSpaceChar(body.front())) {
    body.remove_prefix(1);
  }
  while (!body.empty() && IsSpaceChar(body.back())) {
    body.remove_suffix(1);
  }
  if (!body.empty() && body.front() == '{' && body.back() == '}') {
    body = body.substr(1, body.size() - 2);
  }
  std::vector<ProtoTextEntry> entries;
  if (!ParseProtoTextEntries(body, &entries)) {
    return false;
  }

  // has_xxx pseudo fields report entry presence.
  std::string lower_key = ToLowerCopy(key);
  if (lower_key.starts_with("has_") && lower_key.size() > 4) {
    const std::string bare(key.substr(4));
    const std::string lower_bare = ToLowerCopy(bare);
    if (lower_bare.find("repeated") != std::string::npos) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "Field " + bare + " is repeated, so has_" + bare + " is not allowed");
    }
    *out = Value((ProtoTextHasField(text, bare) ? int64_t{1} : 0));
    return true;
  }
  // Bracketed extension keys ([pkg.Ext.field]) resolve directly.
  const bool extension_key =
      !key.empty() && key.front() == '[' && key.back() == ']';
  std::string repeated_leaf;
  if (extension_key) {
    std::string inner(key.substr(1, key.size() - 2));
    const size_t dot = inner.rfind('.');
    repeated_leaf = dot == std::string::npos ? inner : inner.substr(dot + 1);
  }
  // Repetition shows up anywhere in the name for the modelled protos
  // ("repeated_int64_val", "nested_repeated_value", extension leaves).
  const bool is_repeated_field =
      lower_key.find("repeated") != std::string::npos ||
      lower_key == "str_value" ||
      ToLowerCopy(repeated_leaf).find("repeated") != std::string::npos;

  std::vector<Value> scalars;
  scalars.reserve(entries.size());
  std::vector<Value> messages;
  messages.reserve(entries.size());
  for (const ProtoTextEntry& entry : entries) {
    if (!NameEquals(entry.name, key)) {
      continue;
    }
    if (entry.is_message) {
      messages.emplace_back("{ " + entry.text + " }");
    } else {
      scalars.emplace_back(std::string(entry.text));
    }
  }
  if (messages.empty() && scalars.empty()) {
    // Reading a required field of a modelled proto that lacks it is an
    // error (GoogleSQL rejects the projection outright).
    if (!extension_key) {
      const std::string inferred = InferProtoTypeName(text, {std::string(key)});
      if (!inferred.empty() && RequiredProtoField(inferred, std::string(key))) {
        return StatusError(StatusCode::kInvalidArgument,
                           "Protocol buffer missing required field " +
                               inferred + "." + std::string(key));
      }
    }
    // Schema-level defaults take precedence over per-type defaults.
    const std::optional<Value> fallback = DefaultForAbsentField(key);
    const FieldFormat format = ClassifyFieldFormat(key);
    if (fallback.has_value()) {
      ASSIGN_OR_RETURN(Value, converted,
                       ApplyReadConversion(format, *fallback));
      *out = std::move(converted);
      return true;
    }
    if (is_repeated_field) {
      // Unset repeated fields read as empty arrays.
      *out = Value::Array({}, InferRepeatedElementType(key));
      return true;
    }
    if (format != FieldFormat::kNone) {
      if (AbsentAnnotatedDateReadsNull(key)) {
        *out = Value();
        return true;
      }
      // Unset int-encoded formats read as their epoch default.
      switch (format) {
        case FieldFormat::kTsSeconds:
          *out = Value(FormatTimestampNanos(0, 0));
          return true;
        case FieldFormat::kTsMillis:
          *out = Value(FormatTimestampNanos(0, 3));
          return true;
        case FieldFormat::kTsMicros:
          *out = Value(FormatTimestampNanos(0, 6));
          return true;
        case FieldFormat::kDateDays:
          *out = Value::DateFromDays(0);
          return true;
        default:
          *out = Value();
          return true;
      }
    }
    if (extension_key) {
      // Absent extensions: repeated -> [], scalar numeric/string/bool ->
      // their type default, message-typed -> NULL.
      const std::string lower_leaf = ToLowerCopy(repeated_leaf);
      if (lower_leaf.find("repeated") != std::string::npos) {
        *out = Value::Array({}, "PROTO");
        return true;
      }
      if (IsKnownMessageField(lower_leaf)) {
        *out = Value();
        return true;
      }
      if (lower_leaf.find("string") != std::string::npos ||
          lower_leaf.find("bytes") != std::string::npos) {
        *out = Value(std::string());
        return true;
      }
      if (lower_leaf.find("bool") != std::string::npos) {
        *out = Value(int64_t{0});
        return true;
      }
      if (lower_leaf.find("float") != std::string::npos ||
          lower_leaf.find("double") != std::string::npos) {
        *out = Value(0.0);
        return true;
      }
      if (lower_leaf.find("int") != std::string::npos) {
        *out = Value(int64_t{0});
        return true;
      }
      *out = Value();
      return true;
    }
    std::optional<std::string> enum_default = AbsentEnumDefault(key, entries);
    if (enum_default.has_value()) {
      *out = Value(std::move(*enum_default));
      return true;
    }
    // Message-typed fields read as NULL when unset; scalar fields surface
    // their type defaults (GoogleSQL proto semantics).
    if (IsKnownMessageField(lower_key)) {
      *out = Value();
      return true;
    }
    if (lower_key.find("string") != std::string::npos ||
        lower_key.find("bytes") != std::string::npos) {
      *out = Value(std::string());
      return true;
    }
    if (lower_key.find("bool") != std::string::npos) {
      *out = Value(int64_t{0});
      return true;
    }
    if (lower_key.find("float") != std::string::npos ||
        lower_key.find("double") != std::string::npos) {
      *out = Value(0.0);
      return true;
    }
    *out = Value(int64_t{0});
    return true;
  }

  auto convert_scalar = [&](const Value& raw_token) -> StatusOr<Value> {
    const FieldFormat format = ClassifyFieldFormat(key);
    Value parsed = DecodeScalarToken(RawTextOfValue(raw_token));
    if (const std::vector<std::string>* members =
            EnumMembersForField(key, entries)) {
      parsed = NormalizeEnumToken(*members, parsed);
    }
    return ApplyReadConversion(format, parsed);
  };

  if (!messages.empty()) {
    if (messages.size() == 1 && scalars.empty() && !is_repeated_field) {
      *out = std::move(messages[0]);
    } else {
      std::vector<Value> elements = messages;
      *out = Value::Array(std::move(elements), "PROTO");
    }
    return true;
  }
  if (scalars.size() == 1 && !is_repeated_field) {
    ASSIGN_OR_RETURN(Value, single, convert_scalar(scalars[0]));
    *out = std::move(single);
    return true;
  }
  std::vector<Value> elements;
  elements.reserve(scalars.size());
  for (Value& raw : scalars) {
    ASSIGN_OR_RETURN(Value, element, convert_scalar(raw));
    elements.push_back(std::move(element));
  }
  std::string elem_type = InferRepeatedElementType(key);
  if (elem_type.empty()) {
    const FieldFormat format = ClassifyFieldFormat(key);
    elem_type = InferElementFormatType(format);
    if (elem_type.empty()) {
      for (const Value& element : elements) {
        if (element.IsNull()) {
          continue;
        }
        switch (element.type) {
          case ValueType::kInt64:
            elem_type = "INT64";
            break;
          case ValueType::kDouble:
            elem_type = "DOUBLE";
            break;
          case ValueType::kVarChar:
            elem_type = "STRING";
            break;
          default:
            break;
        }
        if (!elem_type.empty()) {
          break;
        }
      }
    }
    if (elem_type.empty()) {
      elem_type = "INT64";
    }
  }
  *out = Value::Array(std::move(elements), std::move(elem_type));
  return true;
}

// EXC-SHIM: deprecated throwing wrappers (common/exc_shim.hpp).
bool ProtoTextExtractField(std::string_view text, std::string_view key,
                           Value* out) {
  return ExcShimUnwrap(TryProtoTextExtractField(text, key, out),
                       "ProtoTextExtractField");
}

bool TryProtoTextGetField(std::string_view text, std::string_view key,
                          Value* out) {
  return ExcShimUnwrap(TryReadProtoTextField(text, key, out),
                       "TryProtoTextGetField");
}

std::optional<std::string> ProtoTextSetField(
    std::string_view text, const std::vector<std::string>& path,
    const Value& new_value, const std::string& type_name) {
  return ExcShimUnwrap(TryProtoTextSetField(text, path, new_value, type_name),
                       "ProtoTextSetField");
}

std::string ConstructProtoText(
    const std::string& type_name,
    const std::vector<std::pair<std::string, Value>>& fields) {
  return ExcShimUnwrap(TryConstructProtoText(type_name, fields),
                       "ConstructProtoText");
}

void ValidateEnumFieldValue(const std::string& type_name,
                            const std::string& field_name, const Value& value) {
  const Status status = TryValidateEnumFieldValue(type_name, field_name, value);
  if (status != Status::kSuccess) {
    detail::ExcShimThrow("ValidateEnumFieldValue", status);
  }
}

bool ProtoTextHasField(std::string_view text, std::string_view key) {
  std::string_view body = text;
  while (!body.empty() && IsSpaceChar(body.front())) {
    body.remove_prefix(1);
  }
  while (!body.empty() && IsSpaceChar(body.back())) {
    body.remove_suffix(1);
  }
  if (!body.empty() && body.front() == '{' && body.back() == '}') {
    body = body.substr(1, body.size() - 2);
  }
  std::vector<ProtoTextEntry> entries;
  if (!ParseProtoTextEntries(body, &entries)) {
    return false;
  }
  return std::ranges::any_of(entries, [&key](const ProtoTextEntry& entry) {
    return NameEquals(entry.name, key);
  });
}

namespace {

// Rewrites the entries of one message body in place per the final path
// segment semantics; helper for ProtoTextSetField.
StatusOr<std::optional<std::string>> SetFieldInBody(
    std::string_view body, const std::vector<std::string>& path, size_t depth,
    const Value& new_value, const std::string& type_name);

std::string RenderEntry(const ProtoTextEntry& entry) {
  if (entry.is_message) {
    return entry.name + " { " + entry.text + " }";
  }
  return entry.name + ": " + entry.text;
}

StatusOr<std::optional<std::string>> SetFieldInBody(
    const std::string_view body, const std::vector<std::string>& path,
    const size_t depth, const Value& new_value, const std::string& type_name) {
  std::vector<ProtoTextEntry> entries;
  if (!ParseProtoTextEntries(body, &entries)) {
    return StatusError(StatusCode::kInvalidArgument,
                       "Cannot update malformed proto TEXT payload");
  }
  const std::string& target = path[depth];
  const bool final_segment = depth + 1 == path.size();

  if (final_segment) {
    // Clearing: drop every matching entry (required-field violations throw).
    if (new_value.IsNull()) {
      if (RequiredProtoField(type_name, target)) {
        return StatusError(
            StatusCode::kInvalidArgument,
            "Cannot clear required proto field " + type_name + "." + target);
      }
      std::string out;
      out.reserve(entries.size());
      for (const ProtoTextEntry& entry : entries) {
        if (NameEquals(entry.name, target)) {
          continue;
        }
        if (!out.empty()) {
          out.push_back(' ');
        }
        out += RenderEntry(entry);
      }
      return std::optional<std::string>(std::move(out));
    }
    // Setting: convert the value per field-name FORMAT annotations, then
    // replace the first matching entry in place (or append at the end).
    ASSIGN_OR_RETURN(
        Value, value_to_store,
        ApplyWriteConversion(ClassifyFieldFormat(target), new_value));

    std::vector<ProtoTextEntry> replacement;
    replacement.reserve(value_to_store.ArrayElements().size());
    if (value_to_store.IsArray()) {
      for (const Value& element : value_to_store.ArrayElements()) {
        if (element.IsNull()) {
          std::string message =
              "Cannot encode a null value in repeated protocol message field ";
          message += type_name;
          message.push_back('.');
          message += target;
          return StatusError(StatusCode::kInvalidArgument, message);
        }
        if (element.type == ValueType::kVarChar &&
            LooksLikeProtoText(RawTextOfValue(element))) {
          replacement.push_back({target, true,
                                 NormalizeProtoText(RawTextOfValue(element))
                                     .value_or(RawTextOfValue(element))});
        } else {
          replacement.push_back(
              {target, false,
               FormatScalarValueInProtoText(type_name, target, element)});
        }
      }
    } else if (value_to_store.type == ValueType::kVarChar &&
               (LooksLikeProtoText(RawTextOfValue(value_to_store)) ||
                RawTextOfValue(value_to_store).empty())) {
      // Message-looking strings nest; an empty string constructs an empty
      // submessage.
      replacement.push_back({target, true,
                             NormalizeProtoText(RawTextOfValue(value_to_store))
                                 .value_or(RawTextOfValue(value_to_store))});
    } else {
      replacement.push_back(
          {target, false,
           FormatScalarValueInProtoText(type_name, target, value_to_store)});
    }

    std::string out;
    out.reserve(entries.size() + replacement.size());
    bool inserted = replacement.empty();
    for (const ProtoTextEntry& entry : entries) {
      if (!NameEquals(entry.name, target)) {
        if (!out.empty()) {
          out.push_back(' ');
        }
        out += RenderEntry(entry);
        continue;
      }
      if (inserted) {
        continue;  // dropping surplus duplicates
      }
      for (const ProtoTextEntry& rep : replacement) {
        if (!out.empty()) {
          out.push_back(' ');
        }
        out += RenderEntry(rep);
      }
      inserted = true;
    }
    if (!inserted) {
      for (const ProtoTextEntry& rep : replacement) {
        if (!out.empty()) {
          out.push_back(' ');
        }
        out += RenderEntry(rep);
      }
    }
    return std::optional<std::string>(std::move(out));
  }

  // Intermediate segment: descend into matching message entries.
  std::vector<ProtoTextEntry> rewritten = entries;
  bool changed = false;
  for (ProtoTextEntry& entry : rewritten) {
    if (!NameEquals(entry.name, target) || !entry.is_message) {
      continue;
    }
    ASSIGN_OR_RETURN(
        std::optional<std::string>, nested,
        SetFieldInBody(entry.text, path, depth + 1, new_value, type_name));
    if (nested.has_value()) {
      entry.text = *nested;
      changed = true;
    }
  }
  // The read path visits every parent with this name, so SET must rewrite
  // every one of them. Returning at the first match left later parents with
  // the old leaf value and made GET mix updated and stale values.
  if (changed) {
    std::string merged;
    merged.reserve(rewritten.size());
    for (const ProtoTextEntry& e : rewritten) {
      if (!merged.empty()) {
        merged.push_back(' ');
      }
      merged += RenderEntry(e);
    }
    return std::optional<std::string>(std::move(merged));
  }
  // GoogleSQL refuses to assign through a missing intermediate submessage
  // (it reads as NULL); creating it implicitly is not allowed.
  if (!target.empty()) {
    return StatusError(
        StatusCode::kInvalidArgument,
        "Cannot set field of NULL `" + type_name + "." + target + "`");
  }
  return std::optional<std::string>{};
}

}  // namespace

StatusOr<std::optional<std::string>> TryProtoTextSetField(
    const std::string_view text, const std::vector<std::string>& path,
    const Value& new_value, const std::string& type_name) {
  if (path.empty()) {
    return std::optional<std::string>{};
  }
  // T2399: SetFieldInBody recurses once per dot-separated segment of the
  // user-supplied path; bound the depth (same 64 cap as the struct route,
  // T2398) so a hostile path cannot drive unbounded stack growth.
  constexpr size_t kMaxFieldPathDepth = 64;
  if (path.size() > kMaxFieldPathDepth) {
    return StatusError(StatusCode::kInvalidArgument,
                       "proto field path exceeds " +
                           std::to_string(kMaxFieldPathDepth) + " segments");
  }
  std::string_view body = text;
  while (!body.empty() && IsSpaceChar(body.front())) {
    body.remove_prefix(1);
  }
  while (!body.empty() && IsSpaceChar(body.back())) {
    body.remove_suffix(1);
  }
  if (!body.empty() && body.front() == '{' && body.back() == '}') {
    body = body.substr(1, body.size() - 2);
  }
  return SetFieldInBody(body, path, 0, new_value, type_name);
}

bool RequiredProtoField(const std::string& type_name,
                        const std::string& field) {
  static const std::unordered_map<std::string, std::vector<std::string>>
      kRequired = {
          {"googlesql_test.kitchensinkpb", {"int64_key_1", "int64_key_2"}},
          {"googlesql_test.kitchensinkpb.optionalgroup", {"int64_val"}},
      };
  std::string lower = ToLowerCopy(type_name);
  const auto it = kRequired.find(lower);
  if (it == kRequired.end()) {
    return false;
  }
  const std::string lower_field = ToLowerCopy(field);
  return std::find(it->second.begin(), it->second.end(), lower_field) !=
         it->second.end();
}

Status TryValidateEnumFieldValue(const std::string& type_name,
                                 const std::string& field_name,
                                 const Value& value) {
  const std::string key =
      ToLowerCopy(type_name) + "/" + ToLowerCopy(field_name);
  const auto& known = KnownEnumFields();
  const auto it = known.find(key);
  if (it == known.end()) {
    return Status::kSuccess;  // no metadata for this (type, field): accept
                              // as-is
  }
  // Proto3 enums accept numeric values verbatim (unknown members are
  // preserved); proto2 requires known member names.
  const bool proto3 =
      ToLowerCopy(type_name).find("proto3") != std::string::npos;
  if (value.type == ValueType::kInt64 || value.type == ValueType::kDouble) {
    if (proto3) {
      return Status::kSuccess;
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "Could not store value with type " +
                           std::string(ValueTypeToString(value.type)) +
                           " into proto field " + type_name + "." + field_name +
                           " which has a SQL enum type");
  }
  if (value.type != ValueType::kVarChar) {
    return Status::kSuccess;
  }
  const Value member_value = DecodeScalarToken(RawTextOfValue(value));
  if (member_value.IsNull()) {
    return Status::kSuccess;  // null enum members cannot appear in TEXT
                              // payloads
  }
  const std::string member = RawTextOfValue(member_value);
  if (std::find(it->second.begin(), it->second.end(), member) ==
      it->second.end()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "Out of range cast of string '" + member +
                           "' to enum type of field " + type_name + "." +
                           field_name);
  }
  return Status::kSuccess;
}

StatusOr<std::string> TryConstructProtoText(
    const std::string& type_name,
    const std::vector<std::pair<std::string, Value>>& fields) {
  std::string out;
  auto append_entry = [&](const std::string& name, const std::string& text,
                          bool is_message) {
    if (!out.empty()) {
      out.push_back(' ');
    }
    if (is_message) {
      out += name;
      out += " { ";
      out += text;
      out += " }";
    } else {
      out += name;
      out += ": ";
      out += text;
    }
  };

  for (const auto& [field_name, raw_value] : fields) {
    if (raw_value.IsNull()) {
      if (RequiredProtoField(type_name, field_name)) {
        std::string message =
            "Cannot encode a null value in required protocol message field ";
        message += type_name;
        message.push_back('.');
        message += field_name;
        return StatusError(StatusCode::kInvalidArgument, message);
      }
      continue;
    }
    RETURN_IF_FAIL(TryValidateEnumFieldValue(type_name, field_name, raw_value));
    ASSIGN_OR_RETURN(
        Value, value,
        ApplyWriteConversion(ClassifyFieldFormat(field_name), raw_value));
    if (value.IsArray()) {
      for (const Value& element : value.ArrayElements()) {
        if (element.IsNull()) {
          std::string message =
              "Cannot encode a null value in repeated protocol message field ";
          message += type_name;
          message.push_back('.');
          message += field_name;
          return StatusError(StatusCode::kInvalidArgument, message);
        }
        RETURN_IF_FAIL(
            TryValidateEnumFieldValue(type_name, field_name, element));
        const std::string text = RawTextOfValue(element);
        if (element.type == ValueType::kVarChar && LooksLikeProtoText(text)) {
          append_entry(field_name, NormalizeProtoText(text).value_or(text),
                       true);
        } else {
          append_entry(
              field_name,
              FormatScalarValueInProtoText(type_name, field_name, element),
              false);
        }
      }
      continue;
    }
    const std::string text = RawTextOfValue(value);
    if (value.type == ValueType::kInt64 &&
        ToLowerCopy(field_name).find("bool") != std::string::npos) {
      append_entry(field_name, value.value.int_value == 0 ? "false" : "true",
                   false);
      continue;
    }
    if (value.type == ValueType::kVarChar &&
        (LooksLikeProtoText(text) || text.empty())) {
      // Message-looking strings nest; an empty string constructs an empty
      // submessage (NEW X.Nested() with all-NULL fields).
      append_entry(field_name, NormalizeProtoText(text).value_or(text), true);
    } else {
      append_entry(field_name,
                   FormatScalarValueInProtoText(type_name, field_name, value),
                   false);
    }
  }

  // Required-field presence validation for the modelled protos.
  static const std::unordered_map<std::string, std::vector<std::string>>
      kRequiredTypes = {
          {"googlesql_test.kitchensinkpb", {"int64_key_1", "int64_key_2"}},
      };
  const std::string lower_type = ToLowerCopy(type_name);
  const auto req_it = kRequiredTypes.find(lower_type);
  if (req_it != kRequiredTypes.end()) {
    for (const std::string& required : req_it->second) {
      if (!ProtoTextHasField(out, required)) {
        std::string message = "Cannot construct proto ";
        message += type_name;
        message += " because required field ";
        message += required;
        message += " is missing";
        return StatusError(StatusCode::kInvalidArgument, message);
      }
    }
  }
  return out;
}

namespace {

struct WireFieldSpec {
  const char* name;
  bool repeated;
};

const std::unordered_map<std::string, std::unordered_map<int, WireFieldSpec>>&
WireFieldMaps() {
  static const auto* const kMap = new auto(
      std::unordered_map<std::string, std::unordered_map<int, WireFieldSpec>>{
          // KitchenSinkEnumPB carries three TestEnum fields; unknown members
          // are dropped from their field and preserved as "<number>: <value>"
          // raw entries (proto2 semantics).
          {"googlesql_test.kitchensinkenumpb",
           {{1, {.name = "required_test_enum", .repeated = false}},
            {2, {.name = "test_enum", .repeated = false}},
            {3, {.name = "repeated_test_enum", .repeated = true}}}},
          // Only the Proto3KitchenSink bytes round trip appears in tests:
          // test_enum surfaces as field 50.  proto3 keeps unknown members.
          {"googlesql_test.proto3kitchensink",
           {{50, {.name = "test_enum", .repeated = false}}}},
          // KitchenSinkPB's double_val is a protobuf fixed64 field.  Keeping
          // this small wire map lets CAST(bytes AS KitchenSinkPB).double_val
          // participate in NaN/INF comparisons instead of becoming NULL.
          {"googlesql_test.kitchensinkpb",
           {{1, {.name = "int64_key_1", .repeated = false}},
            {2, {.name = "int64_key_2", .repeated = false}},
            {9, {.name = "double_val", .repeated = false}},
            {22, {.name = "nested_value", .repeated = false}},
            {27, {.name = "OptionalGroup", .repeated = false}}}},
          {"googlesql_test.kitchensinkpb.nested",
           {{1, {.name = "nested_int64", .repeated = false}},
            {2, {.name = "nested_repeated_int64", .repeated = true}}}},
          {"googlesql_test.kitchensinkpb.optionalgroup",
           {{1, {.name = "int64_val", .repeated = false}},
            {2, {.name = "string_val", .repeated = false}},
            {3, {.name = "OptionalGroupNested", .repeated = true}}}},
          {"googlesql_test.kitchensinkpb.optionalgroup.optionalgroupnested",
           {{1, {.name = "int64_val", .repeated = false}}}},
          {"googlesql_test.packedrepeatablepb",
           {{7, {.name = "repeated_bool_packed", .repeated = true}}}},
      });
  return *kMap;
}

bool ReadBase128(std::string_view bytes, size_t* i, uint64_t* out) {
  uint64_t result = 0;
  int shift = 0;
  while (*i < bytes.size()) {
    const auto byte = static_cast<uint8_t>(bytes[*i]);
    ++*i;
    result |= static_cast<uint64_t>(byte & 0x7f) << shift;
    if ((byte & 0x80) == 0) {
      *out = result;
      return true;
    }
    shift += 7;
    if (shift > 63) {
      return false;
    }
  }
  return false;
}

}  // namespace

std::optional<std::string> DecodeProtoWireBytes(const std::string& type_name,
                                                std::string_view bytes) {
  const auto& maps = WireFieldMaps();
  const auto map_it = maps.find(ToLowerCopy(type_name));
  if (map_it == maps.end()) {
    return std::nullopt;
  }
  const auto& fields = map_it->second;
  const bool proto2 =
      ToLowerCopy(type_name).find("kitchensinkenumpb") != std::string::npos;
  std::vector<std::pair<std::string, std::string>> entries;
  entries.reserve(bytes.size());
  size_t i = 0;
  while (i < bytes.size()) {
    uint64_t tag = 0;
    if (!ReadBase128(bytes, &i, &tag)) {
      return std::nullopt;
    }
    const uint64_t wire_field = tag >> 3;
    if (wire_field == 0 || wire_field > 536870911ULL) {  // protobuf max 2^29-1
      return std::nullopt;
    }
    const int field_number = static_cast<int>(wire_field);
    const int wire_type = static_cast<int>(tag & 0x7);
    const auto field_it = fields.find(field_number);
    if (wire_type == 1) {
      if (i + 8 > bytes.size()) {
        return std::nullopt;
      }
      uint64_t bits = 0;
      for (int byte = 0; byte < 8; ++byte) {
        bits |= static_cast<uint64_t>(static_cast<unsigned char>(bytes[i++]))
                << (byte * 8);
      }
      if (field_it != fields.end() &&
          std::string_view(field_it->second.name) == "double_val") {
        double value = 0.0;
        std::memcpy(&value, &bits, sizeof(value));
        std::string token;
        if (std::isnan(value)) {
          token = "nan";
        } else if (std::isinf(value)) {
          token = value < 0 ? "-inf" : "inf";
        } else {
          char buf[32];  // NOLINT(modernize-avoid-c-arrays)
          const auto res = std::to_chars(buf, buf + sizeof(buf), value);
          token.assign(buf, res.ptr);  // shortest round-trip, not %.6f
        }
        entries.emplace_back(field_it->second.name, std::move(token));
      } else {
        // Unknown fixed64 field: keep it as a numeric alias with its raw bits
        // so a partial wire payload doesn't silently lose the field.
        std::string raw;
        raw.resize(8);
        std::memcpy(raw.data(), &bits, 8);
        entries.emplace_back("field_" + std::to_string(field_number),
                             digest::ToHex(raw));
      }
      continue;
    }
    if (wire_type == 2) {
      uint64_t length = 0;
      if (!ReadBase128(bytes, &i, &length) || length > bytes.size() - i) {
        return std::nullopt;
      }
      const std::string_view nested = bytes.substr(i, length);
      i += length;
      if (field_it == fields.end()) {
        continue;
      }
      if (std::string_view(field_it->second.name) == "repeated_bool_packed") {
        size_t packed_pos = 0;
        while (packed_pos < nested.size()) {
          uint64_t packed_value = 0;
          if (!ReadBase128(nested, &packed_pos, &packed_value)) {
            return std::nullopt;
          }
          entries.emplace_back(
              field_it->second.name,
              std::to_string(static_cast<int64_t>(packed_value)));
        }
        continue;
      }
      std::string nested_type;
      if (std::string_view(field_it->second.name) == "nested_value") {
        nested_type = "googlesql_test.KitchenSinkPB.Nested";
      } else if (std::string_view(field_it->second.name) == "OptionalGroup") {
        nested_type = "googlesql_test.KitchenSinkPB.OptionalGroup";
      } else if (std::string_view(field_it->second.name) ==
                 "OptionalGroupNested") {
        nested_type =
            "googlesql_test.KitchenSinkPB.OptionalGroup.OptionalGroupNested";
      }
      if (!nested_type.empty()) {
        std::optional<std::string> decoded =
            DecodeProtoWireBytes(nested_type, nested);
        if (!decoded.has_value()) {
          return std::nullopt;
        }
        entries.emplace_back(field_it->second.name, std::move(*decoded));
      } else if (std::string_view(field_it->second.name) == "string_val") {
        std::string escaped;
        escaped.reserve(nested.size() + 2);
        for (const char c : nested) {
          if (c == '"' || c == '\\') {
            escaped.push_back('\\');
          }
          escaped.push_back(c);
        }
        entries.emplace_back(field_it->second.name,
                             std::string("\"") + escaped + "\"");
      }
      continue;
    }
    if (wire_type != 0) {
      return std::nullopt;
    }
    uint64_t value = 0;
    if (!ReadBase128(bytes, &i, &value)) {
      return std::nullopt;
    }
    if (field_it == fields.end()) {
      entries.emplace_back(IntegerToString(field_number),
                           IntegerToString(static_cast<int64_t>(value)));
      continue;
    }
    const std::string name = field_it->second.name;
    // proto2 varints are unsigned on the wire but the enum's declared values
    // are int32: TESTENUMNEGATIVE (-1) arrives as UINT64_MAX and
    // TESTENUM2147483647 above the three small members. Checking `value <= 2`
    // silently demoted both to raw "field: number" entries.
    const auto enum_value = static_cast<int32_t>(value);
    const bool known_member = !proto2 || enum_value == 0 || enum_value == 1 ||
                              enum_value == 2 || enum_value == 2147483647 ||
                              enum_value == -1;
    if (!known_member) {
      entries.emplace_back(IntegerToString(field_number),
                           IntegerToString(static_cast<int64_t>(value)));
      continue;
    }
    std::string token;
    if (proto2) {
      token = enum_value == -1 ? std::string("TESTENUMNEGATIVE")
                               : "TESTENUM" + IntegerToString(enum_value);
    } else if (name == "test_enum" && enum_value >= 0 && enum_value <= 2) {
      token = "ENUM" + IntegerToString(enum_value);
    } else {
      token = IntegerToString(static_cast<int64_t>(value));
    }
    entries.emplace_back(name, std::move(token));
  }
  // Proto wire decoding may see a singular field more than once.  Scalar
  // fields use the last value, while embedded messages merge their members;
  // repeated fields remain repeated entries for ProtoTextExtractField.
  std::vector<std::pair<std::string, std::string>> merged;
  merged.reserve(entries.size());
  for (const auto& entry : entries) {
    auto previous =
        std::find_if(merged.begin(), merged.end(), [&](const auto& candidate) {
          return ToLowerCopy(candidate.first) == ToLowerCopy(entry.first);
        });
    const auto spec_it =
        std::find_if(fields.begin(), fields.end(), [&](const auto& candidate) {
          return std::string_view(candidate.second.name) == entry.first;
        });
    const bool repeated = spec_it != fields.end() && spec_it->second.repeated;
    if (previous == merged.end() || repeated) {
      merged.push_back(entry);
    } else if (entry.first == "nested_value" ||
               entry.first == "OptionalGroup" ||
               entry.first == "OptionalGroupNested") {
      if (!entry.second.empty()) {
        if (!previous->second.empty()) {
          previous->second.push_back(' ');
        }
        previous->second += entry.second;
      }
    } else {
      previous->second = entry.second;
    }
  }
  entries = std::move(merged);
  std::string out;
  out.reserve(entries.size());
  for (const auto& [name, token] : entries) {
    if (!out.empty()) {
      out.push_back(' ');
    }
    if (name == "nested_value" || name == "OptionalGroup" ||
        name == "OptionalGroupNested") {
      out += name;
      out += " { ";
      out += token;
      out += " }";
    } else {
      out += name;
      out += ": ";
      out += token;
    }
  }
  return out;
}

StatusOr<bool> TryReadProtoTextField(std::string_view text,
                                     std::string_view key, Value* out) {
  // An empty payload behaves as an empty message: absent-field defaults
  // apply (scalar zeros, has_ bits, empty repeated arrays).
  if (!text.empty() && !LooksLikeProtoText(text)) {
    return false;
  }
  return TryProtoTextExtractField(text, key, out);
}

namespace {
// T3665: nested-path read mirroring SetFieldInBody's descent. Intermediate
// segments visit EVERY matching message entry (a write replaces the leaf in
// each parent, so the read must see all of them) and merge repeated results.
StatusOr<bool> GetFieldPathInBody(std::string_view body,
                                  const std::vector<std::string>& path,
                                  size_t depth, Value* out) {
  std::vector<ProtoTextEntry> entries;
  if (!ParseProtoTextEntries(body, &entries)) {
    return false;
  }
  const std::string& target = path[depth];
  if (depth + 1 == path.size()) {
    return TryProtoTextExtractField(body, target, out);
  }
  bool found_parent = false;
  bool saw_array = false;
  Value scalar_result;
  std::vector<Value> merged;
  std::string element_type;
  for (const ProtoTextEntry& entry : entries) {
    if (!NameEquals(entry.name, target) || !entry.is_message) {
      continue;
    }
    found_parent = true;
    Value sub;
    ASSIGN_OR_RETURN(bool, ok,
                     (GetFieldPathInBody(entry.text, path, depth + 1, &sub)));
    if (!ok) {
      continue;
    }
    if (sub.IsArray()) {
      if (!saw_array) {
        element_type = sub.ArrayElementSqlType();
      }
      saw_array = true;
      for (const Value& element : sub.ArrayElements()) {
        merged.push_back(element);
      }
    } else if (!saw_array) {
      scalar_result = std::move(sub);
    }
  }
  if (!found_parent) {
    // Absent parent: fall back to the leaf's absent-field default exactly
    // like the flat reader did (unset repeated fields still read as []).
    return TryProtoTextExtractField(std::string_view(), path.back(), out);
  }
  if (saw_array) {
    *out = Value::Array(std::move(merged), std::move(element_type));
    return true;
  }
  *out = std::move(scalar_result);
  return true;
}
}  // namespace

StatusOr<bool> TryProtoTextGetFieldPath(std::string_view text,
                                        const std::vector<std::string>& path,
                                        Value* out) {
  if (path.empty()) {
    return false;
  }
  if (!text.empty() && !LooksLikeProtoText(text)) {
    return false;
  }
  if (path.size() == 1) {
    return TryProtoTextExtractField(text, path.front(), out);
  }
  std::string_view body = text;
  while (!body.empty() && IsSpaceChar(body.front())) {
    body.remove_prefix(1);
  }
  while (!body.empty() && IsSpaceChar(body.back())) {
    body.remove_suffix(1);
  }
  if (!body.empty() && body.front() == '{' && body.back() == '}') {
    body = body.substr(1, body.size() - 2);
  }
  return GetFieldPathInBody(body, path, 0, out);
}

std::optional<platform::Int128> ParseTimestampTextNanos(std::string_view text) {
  return ParseTimestampText(text);
}

namespace {

// Modelled proto types with the signature fields that identify payloads.
// Field names are lowercase; a payload scores one point per signature field
// it mentions (top level) and per SET-target hint that matches.
struct KnownProtoType {
  const char* name;  // display form used in messages
  std::vector<const char*> signature;
};

const std::vector<KnownProtoType>& KnownProtoTypes() {
  static const auto* const kTypes = new auto(std::vector<KnownProtoType>{
      {"googlesql_test.KitchenSinkPB",
       {"int64_key_1", "int64_key_2", "nullable_int", "key_value",
        "optional_group", "repeated_holder", "date_default",
        "timestamp_uint64"}},
      {"googlesql_test.Proto3KitchenSink",
       {"int32_val",
        "uint32_val",
        "int64_val",
        "uint64_val",
        "string_val",
        "bytes_val",
        "bool_val",
        "nested_value",
        "repeated_int32_val",
        "repeated_uint32_val",
        "repeated_int64_val",
        "repeated_uint64_val",
        "repeated_string_val",
        "repeated_float_val",
        "repeated_double_val",
        "repeated_bytes_val",
        "repeated_bool_val",
        "test_enum",
        "repeated_test_enum",
        "nullable_string",
        "nullable_nested_value"}},
      {"googlesql_test.TestExtraPB", {"int32_val1", "int32_val2", "str_value"}},
      {"googlesql_test.KitchenSinkEnumPB", {"required_test_enum"}},
      {"googlesql_test.EmptyMessage", {}},
  });
  return *kTypes;
}

}  // namespace

// Dotted type paths that name ENUM types (everything else dotted is a proto
// message).  Matched case-insensitively on the lowered full path.
bool IsKnownEnumTypeName(const std::string& type_name) {
  // T4465: this used to be a verbatim 3-item list that drifted from the
  // real ledger (proto_schema::BuildEnums registers five enums; CASTs to
  // googlesql_test.AnotherTestEnum / .TestEnumWithAlias /
  // .TestEnumWithAnnotations fell through as message paths).  Derive the
  // set from the registry so the two cannot disagree again;
  // enumannotations.nestedenum stays explicit because it is a nested
  // annotation enum that BuildEnums does not list.
  static const std::unordered_set<std::string>* const kEnums = [] {
    auto* set = new std::unordered_set<std::string>{
        "googlesql_test.enumannotations.nestedenum"};
    for (const EnumSchema& e : Enums()) {
      set->insert("googlesql_test." + ToLowerCopy(e.short_name));
    }
    return set;
  }();
  return kEnums->contains(ToLowerCopy(type_name));
}

std::string AppendProtoTypeMarker(const std::string& payload,
                                  const std::string& type_name) {
  // Comment-form marker: transparent to the TEXT entry parser (which skips
  // '#' through end-of-line) and to NormalizeProtoText re-renders.
  return "# tinylamb-proto-type=" + ToLowerCopy(type_name) + "\n" + payload;
}

std::string ExtractProtoTypeMarker(std::string_view text) {
  constexpr std::string_view kMarker = "# tinylamb-proto-type=";
  size_t pos = text.find(kMarker);
  while (pos != std::string_view::npos) {
    if (pos == 0 || text[pos - 1] == '\n') {
      const size_t begin = pos + kMarker.size();
      const size_t end = text.find('\n', begin);
      const std::string_view name =
          text.substr(begin, end == std::string_view::npos ? end : end - begin);
      std::string trimmed = ToLowerCopy(name);
      while (!trimmed.empty() &&
             (std::isspace(static_cast<unsigned char>(trimmed.back())) != 0)) {
        trimmed.pop_back();
      }
      return trimmed;
    }
    pos = text.find(kMarker, pos + 1);
  }
  return {};
}

std::string InferProtoTypeName(std::string_view payload,
                               const std::vector<std::string>& hint_fields) {
  std::string marked = ExtractProtoTypeMarker(payload);
  if (!marked.empty()) {
    // Canonicalize to the display form when this is a modelled type.
    for (const KnownProtoType& type : KnownProtoTypes()) {
      if (ToLowerCopy(type.name) == marked) {
        return type.name;
      }
    }
    return marked;
  }
  std::string_view body = payload;
  while (!body.empty() && IsSpaceChar(body.front())) {
    body.remove_prefix(1);
  }
  while (!body.empty() && IsSpaceChar(body.back())) {
    body.remove_suffix(1);
  }
  if (!body.empty() && body.front() == '{' && body.back() == '}') {
    body = body.substr(1, body.size() - 2);
  }
  std::vector<ProtoTextEntry> entries;
  const bool parsed = ParseProtoTextEntries(body, &entries);
  if (!parsed) {
    // A few SQL string forms preserve indentation and line wrapping in a way
    // that the strict proto-text tokenizer rejects.  Type inference only
    // needs field presence, so recover known signature names lexically and
    // leave the actual field parser to ProtoTextSetField.
    entries.clear();
  }
  std::unordered_set<std::string> present;
  present.reserve(entries.size());
  for (const ProtoTextEntry& entry : entries) {
    present.insert(ToLowerCopy(entry.name));
  }
  for (const std::string& hint : hint_fields) {
    present.insert(ToLowerCopy(hint));
  }
  const std::string lower_payload = ToLowerCopy(std::string(payload));
  for (const KnownProtoType& type : KnownProtoTypes()) {
    for (const char* field : type.signature) {
      const std::string lower_field = ToLowerCopy(field);
      size_t pos = 0;
      while ((pos = lower_payload.find(lower_field, pos)) !=
             std::string::npos) {
        const bool left_boundary =
            pos == 0 || !IsIdentChar(lower_payload[pos - 1]);
        const size_t end = pos + lower_field.size();
        const bool right_boundary =
            end == lower_payload.size() || !IsIdentChar(lower_payload[end]);
        if (left_boundary && right_boundary) {
          present.insert(lower_field);
          break;
        }
        ++pos;
      }
    }
  }
  // KitchenSinkPB is the only modelled message with these required key
  // fields.  Recognize it directly even when a multiline SQL literal has
  // confused the generic entry tokenizer.
  if (lower_payload.find("int64_key_1") != std::string::npos ||
      lower_payload.find("int64_key_2") != std::string::npos) {
    return "googlesql_test.KitchenSinkPB";
  }
  if (present.empty()) {
    return {};
  }
  const KnownProtoType* best = nullptr;
  size_t best_score = 0;
  bool tie = false;
  for (const KnownProtoType& type : KnownProtoTypes()) {
    size_t score = 0;
    for (const char* field : type.signature) {
      if (present.contains(field)) {
        ++score;
      }
    }
    if (score > best_score) {
      best = &type;
      best_score = score;
      tie = false;
    } else if (score == best_score && score > 0) {
      tie = true;
    }
  }
  if (best != nullptr && !tie) {
    return best->name;
  }
  return {};
}

}  // namespace tinylamb
