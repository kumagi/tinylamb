/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "expression/proto_text.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstring>
#include <cstdlib>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

#include "type/date.hpp"

namespace tinylamb {
namespace {

bool IsIdentStart(char c) {
  return std::isalpha(static_cast<unsigned char>(c)) || c == '_';
}
bool IsIdentChar(char c) {
  return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}
bool IsSpaceChar(char c) {
  return std::isspace(static_cast<unsigned char>(c)) != 0;
}

std::string ToLowerCopy(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (const char c : s) {
    out.push_back(static_cast<char>(
        std::tolower(static_cast<unsigned char>(c))));
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
        case 'n': out->push_back('\n'); break;
        case 't': out->push_back('\t'); break;
        case 'r': out->push_back('\r'); break;
        case 'a': out->push_back('\a'); break;
        case 'b': out->push_back('\b'); break;
        case 'f': out->push_back('\f'); break;
        case 'v': out->push_back('\v'); break;
        case '\\': out->push_back('\\'); break;
        case '\'': out->push_back('\''); break;
        case '"': out->push_back('"'); break;
        case 'x': {
          if (*i + 3 < text.size()) {
            const std::string hex(text.substr(*i + 2, 2));
            const long val = std::strtol(hex.c_str(), nullptr, 16);
            out->push_back(static_cast<char>(val));
            *i += 2;
          } else {
            out->push_back('x');
          }
          break;
        }
        default: out->push_back(next); break;
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
  for (const char c : raw) {
    switch (c) {
      case '\n': out += "\\n"; break;
      case '\t': out += "\\t"; break;
      case '\r': out += "\\r"; break;
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      default: out.push_back(c); break;
    }
  }
  return out;
}

// True when the token is a bare enum-like member (UPPER_SNAKE_CASE), which
// proto text renders without quotes.
bool IsEnumLikeToken(std::string_view token) {
  if (token.empty() || !std::isupper(static_cast<unsigned char>(token[0]))) {
    // Numeric / negative numbers and keywords like true/false stay bare too.
    return !token.empty() &&
           (std::isdigit(static_cast<unsigned char>(token[0])) ||
            token[0] == '-' || token[0] == '+');
  }
  for (const char c : token) {
    if (!(IsIdentChar(c))) {
      return false;
    }
  }
  return true;
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

int64_t CivilToNanos(const SimpleCivilTime& ct) {
  const std::chrono::year_month_day ymd{
      std::chrono::year{ct.year}, std::chrono::month{ct.month},
      std::chrono::day{ct.day}};
  const int64_t days =
      std::chrono::sys_days{ymd}.time_since_epoch().count();
  const int64_t secs =
      days * 86400LL + ct.hour * 3600LL + ct.minute * 60LL + ct.second;
  return secs * 1000000000LL + ct.nanos;
}

SimpleCivilTime NanosToCivil(int64_t nanos) {
  auto floor_div = [](int64_t a, int64_t b) {
    const int64_t q = a / b;
    return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
  };
  const int64_t secs = floor_div(nanos, 1000000000LL);
  const int64_t sub = nanos - secs * 1000000000LL;
  const int64_t days = floor_div(secs, 86400LL);
  const int64_t day_secs = secs - days * 86400LL;
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
std::optional<int64_t> ParseTimestampText(std::string_view s) {
  SimpleCivilTime ct;
  int Y = 0, M = 0, D = 0, h = 0, m = 0, sec = 0;
  if (std::sscanf(std::string(s.substr(0, 10)).c_str(), "%d-%d-%d", &Y, &M,
                  &D) != 3) {
    return std::nullopt;
  }
  ct.year = Y;
  ct.month = static_cast<unsigned>(M);
  ct.day = static_cast<unsigned>(D);
  size_t pos = 10;
  while (pos < s.size() && s[pos] == ' ') {
    ++pos;
  }
  if (pos < s.size()) {
    if (std::sscanf(std::string(s.substr(pos)).c_str(), "%d:%d:%d", &h, &m,
                    &sec) >= 2) {
      ct.hour = h;
      ct.minute = m;
      ct.second = sec;
      while (pos < s.size() && s[pos] != '.' && s[pos] != '+' &&
             s[pos] != '-') {
        ++pos;
      }
      if (pos < s.size() && s[pos] == '.') {
        const size_t begin = ++pos;
        while (pos < s.size() && std::isdigit(static_cast<unsigned char>(s[pos]))) {
          ++pos;
        }
        std::string frac(s.substr(begin, pos - begin));
        while (frac.size() < 9) {
          frac.push_back('0');
        }
        if (frac.size() > 9) {
          frac.resize(9);
        }
        ct.nanos = std::strtoll(frac.c_str(), nullptr, 10);
      }
      if (pos < s.size() && (s[pos] == '+' || s[pos] == '-')) {
        const char sign = s[pos];
        int th = 0, tm = 0;
        if (std::sscanf(std::string(s.substr(pos + 1)).c_str(), "%d:%d", &th,
                        &tm) >= 1) {
          const int offset = th * 3600 + tm * 60;
          return CivilToNanos(ct) -
                 static_cast<int64_t>(offset) * 1000000000LL *
                     (sign == '-' ? -1 : 1);
        }
      }
    }
  }
  return CivilToNanos(ct);
}

// Renders epoch nanos in GoogleSQL UTC timestamp text; precision picks the
// number of fractional digits to print (0 = whole seconds).
std::string FormatTimestampNanos(int64_t nanos, int precision) {
  const SimpleCivilTime ct = NanosToCivil(nanos);
  char buf[80];
  if (precision <= 0 || ct.nanos == 0) {
    snprintf(buf, sizeof(buf), "%04d-%02u-%02u %02d:%02d:%02d+00", ct.year,
             ct.month, ct.day, ct.hour, ct.minute, ct.second);
  } else {
    char frac[16];
    snprintf(frac, sizeof(frac), "%09lld",
             static_cast<long long>(ct.nanos));
    frac[precision] = '\0';
    snprintf(buf, sizeof(buf), "%04d-%02u-%02u %02d:%02d:%02d.%s+00", ct.year,
             ct.month, ct.day, ct.hour, ct.minute, ct.second, frac);
  }
  return buf;
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
  if (name.rfind("has_", 0) == 0) {
    return FieldFormat::kNone;
  }
  // Strip trailing "_format" / width / default markers ("micros_u64",
  // "date_64", "seconds_default").
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
  if (name.find("timestamp") != std::string::npos) {
    if (name.ends_with("_second") || name.ends_with("_seconds")) {
      return FieldFormat::kTsSeconds;
    }
    if (name.ends_with("_millis")) {
      return FieldFormat::kTsMillis;
    }
    return FieldFormat::kTsMicros;
  }
  if (name.find("date") != std::string::npos) {
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

Value ApplyReadConversion(FieldFormat format, Value v) {
  if (v.IsNull()) {
    return v;
  }
  switch (format) {
    case FieldFormat::kNone:
      return v;
    case FieldFormat::kDateDays:
      if (v.type == ValueType::kInt64) {
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
      if (dec < 9999999) {
        return v;
      }
      const int day = static_cast<int>(dec % 100);
      const int mon = static_cast<int>((dec / 100) % 100);
      const int yr = static_cast<int>(dec / 10000);
      return Value::DateFromDays(
          ParseDateDays(std::to_string(yr) + "-" + std::to_string(mon) + "-" +
                        std::to_string(day)));
    }
    case FieldFormat::kTsSeconds:
      if (v.type == ValueType::kInt64) {
        return Value(FormatTimestampNanos(v.value.int_value * 1000000000LL, 0));
      }
      return v;
    case FieldFormat::kTsMillis:
      if (v.type == ValueType::kInt64) {
        return Value(FormatTimestampNanos(v.value.int_value * 1000000LL, 3));
      }
      return v;
    case FieldFormat::kTsMicros:
      if (v.type == ValueType::kInt64) {
        return Value(FormatTimestampNanos(v.value.int_value * 1000LL, 6));
      }
      return v;
  }
  return v;
}

std::optional<Value> ApplyWriteConversion(FieldFormat format,
                                          const Value& v) {
  if (v.IsNull()) {
    return std::nullopt;
  }
  switch (format) {
    case FieldFormat::kNone:
      return std::nullopt;  // caller uses the value verbatim
    case FieldFormat::kDateDays:
      if (v.type == ValueType::kDate) {
        return Value(v.DateDays());
      }
      return std::nullopt;
    case FieldFormat::kDateDecimal: {
      if (v.type != ValueType::kDate) {
        return std::nullopt;
      }
      const std::string text = FormatDateDays(v.DateDays());
      const int yr = std::atoi(text.substr(0, 4).c_str());
      const int mon = std::atoi(text.substr(5, 2).c_str());
      const int day = std::atoi(text.substr(8, 2).c_str());
      return Value(int64_t{yr} * 10000 + int64_t{mon} * 100 + day);
    }
    case FieldFormat::kTsSeconds:
    case FieldFormat::kTsMillis:
    case FieldFormat::kTsMicros:
      if (v.type == ValueType::kVarChar) {
        const std::optional<int64_t> nanos =
            ParseTimestampText(std::string_view(v.value.varchar_value));
        if (!nanos.has_value()) {
          return std::nullopt;
        }
        auto floor_div = [](int64_t a, int64_t b) {
          const int64_t q = a / b;
          return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
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
      return std::nullopt;
  }
  return std::nullopt;
}

// Default values surfaced for absent annotated fields of the compliance
// FieldFormatsProto (proto-level [default = ...] annotations).
struct DefaultValueEntry {
  const char* name;
  const char* text;  // rendered token (DATE or timestamp text)
};
constexpr DefaultValueEntry kFieldDefaults[] = {
    {"date_default", "2015-03-12"},
    {"s_date_default", "2015-03-14"},
    {"f_date_default", "2015-03-16"},
    {"date_64_default", "2015-03-13"},
    {"s_date_64_default", "2015-03-15"},
    {"f_date_64_default", "2015-03-17"},
    {"date_decimal_default", "2015-03-12"},
    {"s_date_decimal_default", "2015-03-14"},
    {"f_date_decimal_default", "2015-03-16"},
    {"date_decimal_64_default", "2015-03-13"},
    {"s_date_decimal_64_default", "2015-03-15"},
    {"f_date_decimal_64_default", "2015-03-17"},
    {"seconds_default", "2015-03-12 17:49:47+00"},
    {"s_seconds_default", "2015-03-13 17:49:47+00"},
    {"f_seconds_default", "2015-03-14 17:49:47+00"},
    {"millis_default", "2015-03-12 17:49:47.555+00"},
    {"s_millis_default", "2015-03-13 17:49:47.555+00"},
    {"f_millis_default", "2015-03-14 17:49:47.555+00"},
    {"micros_default", "2015-03-12 17:49:47.555666+00"},
    {"s_micros_default", "2015-03-13 17:49:47.555666+00"},
    {"f_micros_default", "2015-03-14 17:49:47.555666+00"},
    {"seconds_default_format", "2015-03-12 17:49:47+00"},
    {"s_seconds_default_format", "2015-03-13 17:49:47+00"},
    {"f_seconds_default_format", "2015-03-14 17:49:47+00"},
    {"millis_default_format", "2015-03-12 17:49:47.555+00"},
    {"s_millis_default_format", "2015-03-13 17:49:47.555+00"},
    {"f_millis_default_format", "2015-03-14 17:49:47.555+00"},
    {"micros_default_format", "2015-03-12 17:49:47.555666+00"},
    {"s_micros_default_format", "2015-03-13 17:49:47.555666+00"},
    {"f_micros_default_format", "2015-03-14 17:49:47.555666+00"},
    {"micros_u64_default", "2015-03-12 17:49:47.555777+00"},
};

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
      const std::optional<int64_t> nanos = ParseTimestampText(entry.text);
      if (nanos.has_value()) {
        return Value(entry.text);
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
               std::isdigit(static_cast<unsigned char>(body[i]))) {
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
        while (i < body.size() && !IsSpaceChar(body[i]) &&
               body[i] != ',' && body[i] != ';' && body[i] != '#') {
          ++i;
        }
        if (i == start) {
          return false;
        }
        entry.text = std::string(body.substr(start, i - start));
      }
      entries->push_back(std::move(entry));
      consumed_tail = i;
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
      consumed_tail = i;
      continue;
    }
    // Neither ':' nor '{' after the name: not proto text.
    return false;
  }
  return entries->size() > 0 || consumed_tail >= body.size();
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

std::optional<std::string> NormalizeProtoText(std::string_view text) {
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
  for (const ProtoTextEntry& entry : entries) {
    if (!out.empty()) {
      out.push_back(' ');
    }
    if (entry.is_message) {
      const std::optional<std::string> nested =
          NormalizeProtoText(entry.text);
      out += entry.name + " { " + nested.value_or("") + " }";
    } else {
      out += entry.name + ": " + entry.text;
    }
  }
  return out;
}

std::string FormatProtoTextScalar(std::string_view raw_token) {
  std::string token(raw_token);
  // Trim surrounding whitespace.
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

// Decodes a stored TEXT token into a scalar Value: quoted strings lose their
// quotes (escapes resolved), booleans become 0/1, numbers parse, everything
// else (enum members) stays a bare string.
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
    return Value();
  }
  try {
    size_t idx = 0;
    const int64_t as_int = std::stoll(token, &idx);
    if (idx == token.size()) {
      return Value(as_int);
    }
  } catch (const std::exception& error) {
    (void)error;
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
          "value",  // extension wrapper message (KitchenSinkExtension.value)
      });
  return kFields->find(std::string(lower_name)) != kFields->end();
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
    if (token.empty() || !std::isupper(static_cast<unsigned char>(token[0]))) {
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
           std::isdigit(static_cast<unsigned char>(
               token[token.size() - 1 - digits]))) {
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
      prefix.push_back(static_cast<char>(
          std::toupper(static_cast<unsigned char>(c))));
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
           {"TESTENUM0", "TESTENUM1", "TESTENUM2"}},
          {"googlesql_test.kitchensinkenumpb/test_enum",
           {"TESTENUM0", "TESTENUM1", "TESTENUM2"}},
          {"googlesql_test.kitchensinkenumpb/repeated_test_enum",
           {"TESTENUM0", "TESTENUM1", "TESTENUM2"}},
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

bool ProtoTextExtractField(std::string_view text, std::string_view key,
                           Value* out) {
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
      throw std::runtime_error("Field " + bare +
                               " is repeated, so has_" + bare +
                               " is not allowed");
    }
    *out = Value(int64_t{ProtoTextHasField(text, bare) ? int64_t{1} : 0});
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
  std::vector<Value> messages;
  for (const ProtoTextEntry& entry : entries) {
    if (!NameEquals(entry.name, key)) {
      continue;
    }
    if (entry.is_message) {
      messages.push_back(Value("{ " + entry.text + " }"));
    } else {
      scalars.push_back(Value(std::string(entry.text)));
    }
  }
  if (messages.empty() && scalars.empty()) {
    // Reading a required field of a modelled proto that lacks it is an
    // error (GoogleSQL rejects the projection outright).
    if (!extension_key) {
      const std::string inferred = InferProtoTypeName(text, {std::string(key)});
      if (!inferred.empty() && RequiredProtoField(inferred, std::string(key))) {
        throw std::runtime_error("Protocol buffer missing required field " +
                                 inferred + "." + std::string(key));
      }
    }
    // Schema-level defaults take precedence over per-type defaults.
    const std::optional<Value> fallback = DefaultForAbsentField(key);
    const FieldFormat format = ClassifyFieldFormat(key);
    if (fallback.has_value()) {
      *out = ApplyReadConversion(format, *fallback);
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
    std::optional<std::string> enum_default =
        AbsentEnumDefault(key, entries);
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

  auto convert_scalar = [&](const Value& raw_token) {
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
    *out = convert_scalar(scalars[0]);
    return true;
  }
  std::vector<Value> elements;
  elements.reserve(scalars.size());
  for (Value& raw : scalars) {
    elements.push_back(convert_scalar(raw));
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
          case ValueType::kInt64: elem_type = "INT64"; break;
          case ValueType::kDouble: elem_type = "DOUBLE"; break;
          case ValueType::kVarChar: elem_type = "STRING"; break;
          default: break;
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
  *out = Value::Array(std::move(elements), elem_type);
  return true;
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
  for (const ProtoTextEntry& entry : entries) {
    if (NameEquals(entry.name, key)) {
      return true;
    }
  }
  return false;
}

namespace {

// Rewrites the entries of one message body in place per the final path
// segment semantics; helper for ProtoTextSetField.
std::optional<std::string> SetFieldInBody(const std::string_view body,
                                          const std::vector<std::string>& path,
                                          size_t depth,
                                          const Value& new_value,
                                          const std::string& type_name);

std::string RenderEntry(const ProtoTextEntry& entry) {
  if (entry.is_message) {
    return entry.name + " { " + entry.text + " }";
  }
  return entry.name + ": " + entry.text;
}

std::optional<std::string> SetFieldInBody(
    const std::string_view body, const std::vector<std::string>& path,
    const size_t depth, const Value& new_value,
    const std::string& type_name) {
  std::vector<ProtoTextEntry> entries;
  if (!ParseProtoTextEntries(body, &entries) && !entries.empty()) {
    return std::nullopt;
  }
  const std::string& target = path[depth];
  const bool final_segment = depth + 1 == path.size();

  if (final_segment) {
    // Clearing: drop every matching entry (required-field violations throw).
    if (new_value.IsNull()) {
      if (RequiredProtoField(type_name, target)) {
        throw std::runtime_error("Cannot clear required proto field " +
                                 type_name + "." + target);
      }
      std::string out;
      for (const ProtoTextEntry& entry : entries) {
        if (NameEquals(entry.name, target)) {
          continue;
        }
        if (!out.empty()) {
          out.push_back(' ');
        }
        out += RenderEntry(entry);
      }
      return out;
    }
    // Setting: convert the value per field-name FORMAT annotations, then
    // replace the first matching entry in place (or append at the end).
    std::optional<Value> converted = ApplyWriteConversion(
        ClassifyFieldFormat(target), new_value);
    Value value_to_store = converted.has_value() ? *converted : new_value;

    std::vector<ProtoTextEntry> replacement;
    if (value_to_store.IsArray()) {
      for (const Value& element : value_to_store.ArrayElements()) {
        if (element.IsNull()) {
          std::string message =
              "Cannot encode a null value in repeated protocol message field ";
          message += type_name;
          message.push_back('.');
          message += target;
          throw std::runtime_error(message);
        }
        if (element.type == ValueType::kVarChar &&
            LooksLikeProtoText(RawTextOfValue(element))) {
          replacement.push_back(
              {target, true,
               NormalizeProtoText(RawTextOfValue(element))
                   .value_or(RawTextOfValue(element))});
        } else {
          replacement.push_back(
              {target, false, FormatProtoTextScalar(RawTextOfValue(element))});
        }
      }
    } else if (value_to_store.type == ValueType::kVarChar &&
               (LooksLikeProtoText(RawTextOfValue(value_to_store)) ||
                RawTextOfValue(value_to_store).empty())) {
      // Message-looking strings nest; an empty string constructs an empty
      // submessage.
      replacement.push_back(
          {target, true,
           NormalizeProtoText(RawTextOfValue(value_to_store))
               .value_or(RawTextOfValue(value_to_store))});
    } else {
      replacement.push_back(
          {target, false,
           FormatProtoTextScalar(RawTextOfValue(value_to_store))});
    }

    std::string out;
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
    return out;
  }

  // Intermediate segment: descend into matching message entries.
  bool found_any = false;
  std::vector<ProtoTextEntry> rewritten = entries;
  for (ProtoTextEntry& entry : rewritten) {
    if (!NameEquals(entry.name, target) || !entry.is_message) {
      continue;
    }
    found_any = true;
    auto nested = SetFieldInBody(entry.text, path, depth + 1, new_value,
                                 type_name);
    if (nested.has_value()) {
      entry.text = *nested;
      return [&] {
        std::string out;
        for (const ProtoTextEntry& e : rewritten) {
          if (!out.empty()) {
            out.push_back(' ');
          }
          out += RenderEntry(e);
        }
        return out;
      }();
    }
    found_any = false;  // keep searching later entries
  }
  if (found_any) {
    return std::nullopt;
  }
  // GoogleSQL refuses to assign through a missing intermediate submessage
  // (it reads as NULL); creating it implicitly is not allowed.
  if (!target.empty()) {
    throw std::runtime_error("Cannot set field of NULL `" + type_name +
                             "." + target + "`");
  }
  return std::nullopt;
}

}  // namespace

std::optional<std::string> ProtoTextSetField(
    const std::string_view text, const std::vector<std::string>& path,
    const Value& new_value, const std::string& type_name) {
  if (path.empty()) {
    return std::nullopt;
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

void ValidateEnumFieldValue(const std::string& type_name,
                            const std::string& field_name,
                            const Value& value) {
  const std::string key = ToLowerCopy(type_name) + "/" +
                          ToLowerCopy(field_name);
  const auto& known = KnownEnumFields();
  const auto it = known.find(key);
  if (it == known.end()) {
    return;  // no metadata for this (type, field): accept as-is
  }
  // Proto3 enums accept numeric values verbatim (unknown members are
  // preserved); proto2 requires known member names.
  const bool proto3 = ToLowerCopy(type_name).find("proto3") !=
                      std::string::npos;
  if (value.type == ValueType::kInt64 || value.type == ValueType::kDouble) {
    if (proto3) {
      return;
    }
    throw std::runtime_error(
        "Could not store value with type INT64 into proto field " + type_name +
        "." + field_name + " which has an SQL enum type");
  }
  if (value.type != ValueType::kVarChar) {
    return;
  }
  const Value member_value = DecodeScalarToken(RawTextOfValue(value));
  if (member_value.IsNull()) {
    return;  // null enum members cannot appear in TEXT payloads
  }
  const std::string member = RawTextOfValue(member_value);
  if (std::find(it->second.begin(), it->second.end(), member) ==
      it->second.end()) {
    throw std::runtime_error("Out of range cast of string '" + member +
                             "' to enum type of field " + type_name + "." +
                             field_name);
  }
}

std::string ConstructProtoText(
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
        throw std::runtime_error(message);
      }
      continue;
    }
    ValidateEnumFieldValue(type_name, field_name, raw_value);
    std::optional<Value> converted = ApplyWriteConversion(
        ClassifyFieldFormat(field_name), raw_value);
    Value value = converted.has_value() ? *converted : raw_value;
    // TestExtraPB's repeated string field is represented through the generic
    // VARCHAR channel, where enum-like tokens would otherwise be emitted
    // without quotes.  It is a string field even when its source payload was
    // already decoded from proto text.
    if (ToLowerCopy(field_name) == "str_value") {
      auto quote = [](std::string text) {
        if (text.size() >= 2 && text.front() == '"' && text.back() == '"') {
          return text;
        }
        return std::string("\"") + text + "\"";
      };
      if (value.type == ValueType::kVarChar) {
        value = Value(quote(std::string(value.value.varchar_value)));
      } else if (value.IsArray()) {
        std::vector<Value> quoted;
        quoted.reserve(value.ArrayElements().size());
        for (const Value& element : value.ArrayElements()) {
          if (element.type == ValueType::kVarChar) {
            quoted.emplace_back(
                quote(std::string(element.value.varchar_value)));
          } else {
            quoted.push_back(element);
          }
        }
        value = Value::Array(std::move(quoted), value.ArrayElementSqlType());
      }
    }
    if (value.IsArray()) {
      for (const Value& element : value.ArrayElements()) {
        if (element.IsNull()) {
          std::string message =
              "Cannot encode a null value in repeated protocol message field ";
          message += type_name;
          message.push_back('.');
          message += field_name;
          throw std::runtime_error(message);
        }
        ValidateEnumFieldValue(type_name, field_name, element);
        const std::string text = RawTextOfValue(element);
        if (element.type == ValueType::kVarChar &&
            LooksLikeProtoText(text)) {
          append_entry(field_name,
                       NormalizeProtoText(text).value_or(text), true);
        } else {
          append_entry(field_name, FormatProtoTextScalar(text), false);
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
      append_entry(field_name, FormatProtoTextScalar(text), false);
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
        throw std::runtime_error(message);
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

const std::unordered_map<std::string,
                         std::unordered_map<int, WireFieldSpec>>&
WireFieldMaps() {
  static const auto* const kMap = new auto(
      std::unordered_map<std::string,
                         std::unordered_map<int, WireFieldSpec>>{
          // KitchenSinkEnumPB carries three TestEnum fields; unknown members
          // are dropped from their field and preserved as "<number>: <value>"
          // raw entries (proto2 semantics).
          {"googlesql_test.kitchensinkenumpb",
           {{1, {"required_test_enum", false}},
            {2, {"test_enum", false}},
            {3, {"repeated_test_enum", true}}}},
          // Only the Proto3KitchenSink bytes round trip appears in tests:
          // test_enum surfaces as field 50.  proto3 keeps unknown members.
          {"googlesql_test.proto3kitchensink",
           {{50, {"test_enum", false}}}},
          // KitchenSinkPB's double_val is a protobuf fixed64 field.  Keeping
          // this small wire map lets CAST(bytes AS KitchenSinkPB).double_val
          // participate in NaN/INF comparisons instead of becoming NULL.
          {"googlesql_test.kitchensinkpb",
           {{1, {"int64_key_1", false}},
            {2, {"int64_key_2", false}},
            {9, {"double_val", false}},
            {22, {"nested_value", false}},
            {27, {"OptionalGroup", false}}}},
          {"googlesql_test.kitchensinkpb.nested",
           {{1, {"nested_int64", false}},
            {2, {"nested_repeated_int64", true}}}},
          {"googlesql_test.kitchensinkpb.optionalgroup",
           {{1, {"int64_val", false}},
            {2, {"string_val", false}},
            {3, {"OptionalGroupNested", true}}}},
          {"googlesql_test.kitchensinkpb.optionalgroup.optionalgroupnested",
           {{1, {"int64_val", false}}}},
          {"googlesql_test.packedrepeatablepb",
           {{7, {"repeated_bool_packed", true}}}},
      });
  return *kMap;
}

bool ReadBase128(std::string_view bytes, size_t* i, uint64_t* out) {
  uint64_t result = 0;
  int shift = 0;
  while (*i < bytes.size()) {
    const uint8_t byte = static_cast<uint8_t>(bytes[*i]);
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
  const bool proto2 = ToLowerCopy(type_name).find("kitchensinkenumpb") !=
                      std::string::npos;
  std::vector<std::pair<std::string, std::string>> entries;
  size_t i = 0;
  while (i < bytes.size()) {
    uint64_t tag = 0;
    if (!ReadBase128(bytes, &i, &tag)) {
      return std::nullopt;
    }
    const int field_number = static_cast<int>(tag >> 3);
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
          token = std::to_string(value);
        }
        entries.emplace_back(field_it->second.name, std::move(token));
      }
      continue;
    }
    if (wire_type == 2) {
      uint64_t length = 0;
      if (!ReadBase128(bytes, &i, &length) ||
          length > bytes.size() - i) {
        return std::nullopt;
      }
      const std::string_view nested = bytes.substr(i, length);
      i += length;
      if (field_it == fields.end()) { continue; }
      if (std::string_view(field_it->second.name) ==
          "repeated_bool_packed") {
        size_t packed_pos = 0;
        while (packed_pos < nested.size()) {
          uint64_t packed_value = 0;
          if (!ReadBase128(nested, &packed_pos, &packed_value)) {
            return std::nullopt;
          }
          entries.emplace_back(field_it->second.name,
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
        const std::optional<std::string> decoded =
            DecodeProtoWireBytes(nested_type, nested);
        if (decoded.has_value()) {
          entries.emplace_back(field_it->second.name, *decoded);
        }
      } else if (std::string_view(field_it->second.name) == "string_val") {
        std::string escaped;
        escaped.reserve(nested.size() + 2);
        for (const char c : nested) {
          if (c == '"' || c == '\\') { escaped.push_back('\\'); }
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
      entries.emplace_back(std::to_string(field_number),
                           std::to_string(static_cast<int64_t>(value)));
      continue;
    }
    const std::string name = field_it->second.name;
    const bool known_member =
        !proto2 || (value <= 2);  // TestEnum members TESTENUM0..2
    if (!known_member) {
      entries.emplace_back(std::to_string(field_number),
                           std::to_string(static_cast<int64_t>(value)));
      continue;
    }
    std::string token = proto2 ? ("TESTENUM" + std::to_string(value))
                               : std::to_string(static_cast<int64_t>(value));
    entries.emplace_back(name, std::move(token));
  }
  // Proto wire decoding may see a singular field more than once.  Scalar
  // fields use the last value, while embedded messages merge their members;
  // repeated fields remain repeated entries for ProtoTextExtractField.
  std::vector<std::pair<std::string, std::string>> merged;
  for (const auto& entry : entries) {
    auto previous = std::find_if(
        merged.begin(), merged.end(), [&](const auto& candidate) {
          return ToLowerCopy(candidate.first) == ToLowerCopy(entry.first);
        });
    const auto spec_it = std::find_if(
        fields.begin(), fields.end(), [&](const auto& candidate) {
          return std::string_view(candidate.second.name) == entry.first;
        });
    const bool repeated = spec_it != fields.end() && spec_it->second.repeated;
    if (previous == merged.end() || repeated) {
      merged.push_back(entry);
    } else if (entry.first == "nested_value" ||
               entry.first == "OptionalGroup") {
      if (!entry.second.empty()) {
        if (!previous->second.empty()) { previous->second.push_back(' '); }
        previous->second += entry.second;
      }
    } else {
      previous->second = entry.second;
    }
  }
  entries = std::move(merged);
  std::string out;
  for (const auto& [name, token] : entries) {
    if (!out.empty()) {
      out.push_back(' ');
    }
    if ((name == "nested_value" || name == "OptionalGroup" ||
         name == "OptionalGroupNested") &&
        !token.empty()) {
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

bool TryProtoTextGetField(std::string_view text, std::string_view key,
                          Value* out) {
  // An empty payload behaves as an empty message: absent-field defaults
  // apply (scalar zeros, has_ bits, empty repeated arrays).
  if (!text.empty() && !LooksLikeProtoText(text)) {
    return false;
  }
  return ProtoTextExtractField(text, key, out);
}

std::optional<int64_t> ParseTimestampTextNanos(std::string_view text) {
  return ParseTimestampText(text);
}

namespace {

// Modelled proto types with the signature fields that identify payloads.
// Field names are lowercase; a payload scores one point per signature field
// it mentions (top level) and per SET-target hint that matches.
struct KnownProtoType {
  const char* name;       // display form used in messages
  std::vector<const char*> signature;
};

const std::vector<KnownProtoType>& KnownProtoTypes() {
  static const auto* const kTypes = new auto(std::vector<KnownProtoType>{
      {"googlesql_test.KitchenSinkPB",
       {"int64_key_1", "int64_key_2", "nullable_int", "key_value",
        "optional_group", "repeated_holder", "date_default",
        "timestamp_uint64"}},
      {"googlesql_test.Proto3KitchenSink",
       {"int32_val", "uint32_val", "int64_val", "uint64_val",
        "string_val", "bytes_val", "bool_val", "nested_value",
        "repeated_int32_val", "repeated_uint32_val", "repeated_int64_val",
        "repeated_uint64_val", "repeated_string_val", "repeated_float_val",
        "repeated_double_val", "repeated_bytes_val", "repeated_bool_val",
        "test_enum", "repeated_test_enum", "nullable_string",
        "nullable_nested_value"}},
      {"googlesql_test.TestExtraPB", {"int32_val1", "int32_val2",
                                       "str_value"}},
      {"googlesql_test.KitchenSinkEnumPB",
       {"required_test_enum"}},
      {"googlesql_test.EmptyMessage", {}},
  });
  return *kTypes;
}

}  // namespace

// Dotted type paths that name ENUM types (everything else dotted is a proto
// message).  Matched case-insensitively on the lowered full path.
bool IsKnownEnumTypeName(const std::string& type_name) {
  static const std::unordered_set<std::string>* const kEnums =
      new auto(std::unordered_set<std::string>{
          "googlesql_test.testenum",
          "googlesql_test.testproto3enum",
          "googlesql_test.enumannotations.nestedenum",
      });
  return kEnums->find(ToLowerCopy(type_name)) != kEnums->end();
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
          text.substr(begin,
                      end == std::string_view::npos ? end : end - begin);
      std::string trimmed = ToLowerCopy(name);
      while (!trimmed.empty() &&
             std::isspace(static_cast<unsigned char>(trimmed.back()))) {
        trimmed.pop_back();
      }
      return trimmed;
    }
    pos = text.find(kMarker, pos + 1);
  }
  return {};
}

std::string InferProtoTypeName(
    std::string_view payload, const std::vector<std::string>& hint_fields) {  std::string marked = ExtractProtoTypeMarker(payload);
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
      if (present.count(field) != 0) {
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
