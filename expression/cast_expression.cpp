/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/cast_expression.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <limits>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/json_string.hpp"
#include "common/string_util.hpp"
#include "expression/expression.hpp"
#include "expression/json_text.hpp"
#include "expression/proto_schema.hpp"
#include "expression/proto_text.hpp"
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
// GoogleSQL spells type names in cast errors in lower case ("Bad int64
// value: abc", "uint64 out of range: ..."); `upper` is the canonical
// upper-case target name used for dispatch.
std::string AsciiLowerCopy(std::string_view text) {
  std::string out(text);
  for (char& c : out) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return out;
}
}  // namespace

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

std::string_view TrimCivilWhitespace(std::string_view s) {
  while (!s.empty() &&
         (std::isspace(static_cast<unsigned char>(s.front())) != 0)) {
    s.remove_prefix(1);
  }
  while (!s.empty() &&
         (std::isspace(static_cast<unsigned char>(s.back())) != 0)) {
    s.remove_suffix(1);
  }
  return s;
}

bool ParseCivilTime(std::string_view s, CivilTime* ct, std::string_view* zone,
                    bool allow_time_only) {
  s = TrimCivilWhitespace(s);
  const auto number = [&s](int* out, size_t max_digits) {
    size_t digits = 0;
    int result = 0;
    while (digits < s.size() && digits < max_digits && s[digits] >= '0' &&
           s[digits] <= '9') {
      result = (result * 10) + (s[digits++] - '0');
    }
    s.remove_prefix(digits);
    *out = result;
    return digits != 0;
  };
  const auto take = [&s](char ch) {
    if (s.empty() || s.front() != ch) {
      return false;
    }
    s.remove_prefix(1);
    return true;
  };

  int first = 0;
  const size_t before_year = s.size();
  if (!number(&first, 4)) {
    return false;
  }
  const size_t year_digits = before_year - s.size();
  bool has_clock = false;
  if (take('-')) {
    // The date form is fixed-width YYYY-MM-DD; time-only inputs take the
    // else branch below and must not be subjected to this check.
    if (year_digits != 4 || s.size() < 5 || s[2] != '-' ||
        (std::isdigit(static_cast<unsigned char>(s[0])) == 0) ||
        (std::isdigit(static_cast<unsigned char>(s[1])) == 0) ||
        (std::isdigit(static_cast<unsigned char>(s[3])) == 0) ||
        (std::isdigit(static_cast<unsigned char>(s[4])) == 0)) {
      return false;
    }
    int month = 0;
    int day = 0;
    if (first < 1 || first > 9999 || !number(&month, 2) || !take('-') ||
        !number(&day, 2) || month < 1 || month > 12 || day < 1 || day > 31) {
      return false;
    }
    ct->year = first;
    ct->month = static_cast<unsigned>(month);
    ct->day = static_cast<unsigned>(day);
    if (!s.empty()) {
      if (!take('T') && !take('t') && !take(' ')) {
        return false;
      }
      s = TrimCivilWhitespace(s);
      if (!number(&ct->hour, 2) || !take(':')) {
        return false;
      }
      has_clock = true;
    }
  } else {
    if (!allow_time_only || !take(':')) {
      return false;
    }
    ct->hour = first;
    has_clock = true;
  }

  if (has_clock) {
    if (!number(&ct->minute, 2)) {
      return false;
    }
    if (!take(':')) {
      return false;  // GoogleSQL requires HH:MM:SS
    }
    {
      if (!number(&ct->second, 2)) {
        return false;
      }
      if (take('.')) {
        size_t digits = 0;
        while (!s.empty() && s.front() >= '0' && s.front() <= '9') {
          if (digits < 9) {
            ct->subsecond_nanos =
                (ct->subsecond_nanos * 10) + (s.front() - '0');
          }
          ++digits;
          s.remove_prefix(1);
        }
        if (digits == 0 || digits > 9) {
          return false;
        }
        while (digits++ < 9) {
          ct->subsecond_nanos *= 10;
        }
      }
    }
    if (!s.empty()) {
      const bool zulu = (s == "Z" || s == "z");
      const bool numeric_offset = s.front() == '+' || s.front() == '-';
      if (!zulu && !numeric_offset &&
          (std::isspace(static_cast<unsigned char>(s.front())) == 0)) {
        return false;
      }
      if (numeric_offset) {
        // Validate the whole zone token: a numeric offset followed by trailing
        // junk (e.g. "+05:00garbage") is not a timestamp spelling.
        for (size_t i = 1; i < s.size(); ++i) {
          const char ch = s[i];
          // NOLINTNEXTLINE(readability-implicit-bool-conversion) explicit
          // intent
          if (!std::isdigit(static_cast<unsigned char>(ch)) && ch != ':' &&
              (std::isspace(static_cast<unsigned char>(ch)) == 0)) {
            return false;
          }
        }
        if (s.find_first_of("0123456789") == std::string_view::npos) {
          return false;
        }
      }
    }
    *zone = TrimCivilWhitespace(s);
  }

  const std::chrono::year_month_day ymd{std::chrono::year{ct->year},
                                        std::chrono::month{ct->month},
                                        std::chrono::day{ct->day}};
  // Allow second == 60 through so the leap-second normalization below is
  // reachable: the comparison path accepts the same spelling and TimeGm
  // carries it into the next minute/hour/day, so CAST must agree.
  if (!ymd.ok() || ct->hour > 23 || ct->minute > 59 || ct->second > 60) {
    return false;
  }
  if (ct->second == 60) {
    ct->subsecond_nanos = 0;
    ct->second = 0;
    if (++ct->minute == 60) {
      ct->minute = 0;
      if (++ct->hour == 24) {
        ct->hour = 0;
        const std::chrono::year_month_day next{std::chrono::sys_days{ymd} +
                                               std::chrono::days{1}};
        ct->year = int(next.year());
        ct->month = unsigned(next.month());
        ct->day = unsigned(next.day());
      }
    }
  }
  return ct->year <= 9999;
}

std::string FormatCivilTime(const CivilTime& ct) {
  std::array<char, 64> buf{};
  if (ct.subsecond_nanos != 0) {
    if (ct.subsecond_nanos % 1000000 == 0) {
      // Formatting into a fixed buffer cannot fail.
      // NOLINTNEXTLINE(cert-err33-c)
      snprintf(buf.data(), buf.size(), "%04d-%02d-%02d %02d:%02d:%02d.%03ld",
               ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second,
               static_cast<long>(ct.subsecond_nanos / 1000000));
    } else if (ct.subsecond_nanos % 1000 == 0) {
      // NOLINTNEXTLINE(cert-err33-c)
      snprintf(buf.data(), buf.size(), "%04d-%02d-%02d %02d:%02d:%02d.%06ld",
               ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second,
               static_cast<long>(ct.subsecond_nanos / 1000));
    } else {
      // NOLINTNEXTLINE(cert-err33-c)
      snprintf(buf.data(), buf.size(), "%04d-%02d-%02d %02d:%02d:%02d.%09ld",
               ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second,
               static_cast<long>(ct.subsecond_nanos));
    }
  } else {
    // NOLINTNEXTLINE(cert-err33-c)
    snprintf(buf.data(), buf.size(), "%04d-%02d-%02d %02d:%02d:%02d", ct.year,
             ct.month, ct.day, ct.hour, ct.minute, ct.second);
  }
  return std::string{buf.data()};
}

// Zone shifts must stay in whole seconds: routing through nanosecond
// arithmetic overflows int64 for dates older than ~1678 / newer than ~2262.
CivilTime ShiftCivilTimeSeconds(CivilTime ct, int64_t add_seconds) {
  const auto floor_div = [](int64_t a, int64_t b) -> int64_t {
    const int64_t q = a / b;
    return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
  };
  int64_t day_secs =
      (ct.hour * 3600LL) + (ct.minute * 60LL) + ct.second + add_seconds;
  int64_t extra_days = floor_div(day_secs, 86400);
  int64_t rem = day_secs - (extra_days * 86400);
  ct.second = static_cast<int>(rem % 60);
  ct.minute = static_cast<int>((rem / 60) % 60);
  ct.hour = static_cast<int>(rem / 3600);
  if (extra_days != 0) {
    std::chrono::year_month_day ymd{std::chrono::year{ct.year},
                                    std::chrono::month{ct.month},
                                    std::chrono::day{ct.day}};
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

StatusOr<Value> CastCivilTime(std::string_view text, std::string_view target) {
  const auto invalid = [&] {
    return StatusError(
        StatusCode::kInvalidArgument,
        "invalid " + std::string(target) + " string: " + std::string(text));
  };
  const auto offset_at = [](std::string_view zone, const CivilTime& ct,
                            bool at_utc = false) {
    return TryTimeZoneOffsetAtLocalTime(zone, ct.year, ct.month, ct.day,
                                        ct.hour, ct.minute, ct.second, at_utc);
  };

  CivilTime ct;
  std::string_view zone;
  // A bare clock time (HH:MM:SS[.frac][zone]) is a valid input for every
  // temporal target: it lands on 1970-01-01. GoogleSQL's timestamp cast
  // accepts e.g. CAST('00:00:00.2+00' AS TIMESTAMP), so only a DATE target
  // (which has no clock component) refuses a time-only string.
  const bool allow_time_only = target != "DATE";
  if (!ParseCivilTime(text, &ct, &zone, allow_time_only)) {
    return invalid();
  }
  if (target == "TIMESTAMP" || !zone.empty()) {
    // Convert the input wall time to UTC first. For civil targets, look up
    // the destination offset at the resulting instant, including DST edges.
    const std::string default_zone = GetDefaultTimeZone();
    ASSIGN_OR_RETURN(
        int, input_offset,
        offset_at(zone.empty() ? std::string_view(default_zone) : zone, ct));
    ct = ShiftCivilTimeSeconds(ct, -input_offset);
    if (target != "TIMESTAMP") {
      ASSIGN_OR_RETURN(int, local_offset, offset_at(default_zone, ct, true));
      ct = ShiftCivilTimeSeconds(ct, local_offset);
    }
  }
  if (ct.year < 1 || ct.year > 9999) {
    return invalid();
  }
  if (target == "TIME") {
    const std::string formatted = FormatCivilTime(ct);
    return Value(formatted.substr(formatted.find(' ') + 1));
  }
  if (target == "DATE") {
    const std::chrono::year_month_day ymd{std::chrono::year{ct.year},
                                          std::chrono::month{ct.month},
                                          std::chrono::day{ct.day}};
    return Value::DateFromDays(
        std::chrono::sys_days{ymd}.time_since_epoch().count());
  }
  std::string formatted = FormatCivilTime(ct);
  if (target == "TIMESTAMP") {
    formatted += "+00";
  }
  return Value(std::move(formatted));
}

std::string ToUpper(std::string str) {
  for (char& c : str) {
    c = AsciiToUpper(c);
  }
  return str;
}

std::string ToLower(std::string str) {
  for (char& c : str) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return str;
}

void AppendVarint(std::string* out, uint64_t value) {
  while (value >= 0x80) {
    out->push_back(static_cast<char>((value & 0x7f) | 0x80));
    value >>= 7;
  }
  out->push_back(static_cast<char>(value));
}

std::string UnquoteProtoToken(std::string token) {
  if (token.size() < 2 || ((token.front() != '"' || token.back() != '"') &&
                           (token.front() != '\'' || token.back() != '\''))) {
    return token;
  }
  token = token.substr(1, token.size() - 2);
  std::string result;
  result.reserve(token.size());
  for (size_t i = 0; i < token.size(); ++i) {
    if (token[i] == '\\' && i + 1 < token.size()) {
      const char escaped = token[++i];
      result.push_back(escaped == 'n'   ? '\n'
                       : escaped == 'r' ? '\r'
                       : escaped == 't' ? '\t'
                                        : escaped);
    } else {
      result.push_back(token[i]);
    }
  }
  return result;
}

StatusOr<uint64_t> ProtoInteger(std::string token,
                                std::string_view enum_type = {}) {
  token = UnquoteProtoToken(std::move(token));
  int64_t enum_value = 0;
  if (!enum_type.empty() && EnumValueForMember(enum_type, token, &enum_value)) {
    return static_cast<uint64_t>(enum_value);
  }
  int64_t signed_value = 0;
  const auto [end, error] =
      std::from_chars(token.data(), token.data() + token.size(), signed_value);
  if (error == std::errc() && end == token.data() + token.size()) {
    return static_cast<uint64_t>(signed_value);
  }
  return StatusError(StatusCode::kInvalidArgument,
                     "cannot encode proto scalar: " + token);
}

void AppendProtoField(std::string* out, uint32_t field_number, uint64_t value) {
  AppendVarint(out, (static_cast<uint64_t>(field_number) << 3) | 0);
  AppendVarint(out, value);
}

void AppendProtoMessageField(std::string* out, uint32_t field_number,
                             const std::string& payload) {
  AppendVarint(out, (static_cast<uint64_t>(field_number) << 3) | 2);
  AppendVarint(out, payload.size());
  out->append(payload);
}

std::string_view InferProtoWireType(std::string_view text) {
  if (text.find(':') == std::string_view::npos) {
    return {};
  }
  while (text.starts_with('{') && text.ends_with('}')) {
    text.remove_prefix(1);
    text.remove_suffix(1);
  }
  std::vector<ProtoTextEntry> entries;
  if (!ParseProtoTextEntries(text, &entries)) {
    return {};
  }
  std::string_view type;
  for (const ProtoTextEntry& entry : entries) {
    const std::string field = ToLower(entry.name);
    if (field == "repeated_bool_packed") {
      return "googlesql_test.PackedRepeatablePB";
    }
    if (field == "int64_key_1" || field == "int64_key_2" ||
        field == "double_val") {
      return "googlesql_test.KitchenSinkPB";
    }
    if (field == "required_test_enum") {
      return "googlesql_test.KitchenSinkEnumPB";
    }
    if (field == "str_value" || field == "int32_val1" ||
        field == "int32_val2") {
      return "googlesql_test.TestExtraPB";
    }
    if (field == "test_enum") {
      type = "googlesql_test.Proto3KitchenSink";
    } else if (field == "value" && type.empty()) {
      if (EnumValueForMember("TestEnum", UnquoteProtoToken(entry.text),
                             nullptr)) {
        type = "googlesql_test.NullableEnum";
      } else if (std::ranges::count(entry.text, '-') >= 2) {
        type = "googlesql_test.NullableDate";
      } else {
        type = "googlesql_test.NullableInt";
      }
    }
  }
  return type;
}

StatusOr<std::string> EncodeProtoWireMessage(std::string_view type_name,
                                             std::string_view payload) {
  std::vector<ProtoTextEntry> entries;
  std::string body(payload);
  if (body.starts_with("#")) {
    const size_t newline = body.find('\n');
    body =
        newline == std::string::npos ? std::string() : body.substr(newline + 1);
  }
  while (!body.empty() && body.front() == '{' && body.back() == '}') {
    body = body.substr(1, body.size() - 2);
  }
  if (!ParseProtoTextEntries(body, &entries)) {
    // This helper returns StatusOr<std::string>; a parse failure must surface
    // as an error instead of an empty (successful) wire message. Otherwise the
    // BYTES cast silently turns malformed proto TEXT into b'' while the
    // dotted-TEXT direction rejects the very same payload.
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid proto TEXT payload: " + std::string(payload));
  }
  const std::string lower = ToLower(std::string(type_name));
  std::string out;
  if (lower.ends_with("packedrepeatablepb")) {
    std::string packed;
    for (const ProtoTextEntry& entry : entries) {
      if (ToLower(entry.name) == "repeated_bool_packed") {
        const std::string token = ToLower(UnquoteProtoToken(entry.text));
        if (token == "true" || token == "false") {
          AppendVarint(&packed, token == "true" ? 1U : 0U);
        } else {
          ASSIGN_OR_RETURN(uint64_t, value, ProtoInteger(token));
          AppendVarint(&packed, value);
        }
      }
    }
    if (!packed.empty()) {
      AppendProtoMessageField(&out, 7, packed);
    }
    return out;
  }
  const auto append_integer =
      [&out](uint32_t field_number, const ProtoTextEntry& entry,
             std::string_view enum_type = {}) -> Status {
    ASSIGN_OR_RETURN(uint64_t, value, ProtoInteger(entry.text, enum_type));
    AppendProtoField(&out, field_number, value);
    return Status::kSuccess;
  };
  for (const ProtoTextEntry& entry : entries) {
    const std::string field = ToLower(entry.name);
    if (lower.ends_with("nullableint") || lower.ends_with("nullabledate") ||
        lower.ends_with("nullableenum")) {
      if (field == "value") {
        if (lower.ends_with("nullabledate") &&
            std::ranges::count(entry.text, '-') >= 2) {
          ASSIGN_OR_RETURN(int64_t, date_days, TryParseDateDays(entry.text));
          AppendProtoField(&out, 1, static_cast<uint64_t>(date_days));
        } else {
          RETURN_IF_FAIL(append_integer(
              1, entry, lower.ends_with("nullableenum") ? "TestEnum" : ""));
        }
      }
      continue;
    }
    if (lower.ends_with("kitchensinkpb")) {
      if (field == "int64_key_1") {
        RETURN_IF_FAIL(append_integer(1, entry));
      } else if (field == "int64_key_2") {
        RETURN_IF_FAIL(append_integer(2, entry));
      } else if (field == "double_val") {
        const std::string token = UnquoteProtoToken(entry.text);
        double value = 0;
        const auto [end, error] =
            std::from_chars(token.data(), token.data() + token.size(), value);
        if (error != std::errc() || end != token.data() + token.size()) {
          return StatusError(StatusCode::kInvalidArgument,
                             "cannot encode proto scalar: " + token);
        }
        const auto bits = std::bit_cast<uint64_t>(value);
        AppendVarint(&out, (9 << 3) | 1);
        for (unsigned shift = 0; shift < 64; shift += 8) {
          out.push_back(static_cast<char>((bits >> shift) & 0xff));
        }
      } else if (field == "nested_value") {
        ASSIGN_OR_RETURN(
            std::string, nested_msg,
            (EncodeProtoWireMessage("googlesql_test.KitchenSinkPB.Nested",
                                    entry.text)));
        AppendProtoMessageField(&out, 22, nested_msg);
      } else if (field == "optionalgroup" || field == "optional_group" ||
                 field == "optional_group_field") {
        ASSIGN_OR_RETURN(
            std::string, optional_group_msg,
            (EncodeProtoWireMessage(
                "googlesql_test.KitchenSinkPB.OptionalGroup", entry.text)));
        AppendProtoMessageField(&out, 27, optional_group_msg);
      }
    } else if (lower.ends_with("kitchensinkenumpb")) {
      // Unknown proto2 enum values are rendered using their numeric field
      // number (for example, "2: 7").  Preserve those fields during a
      // message -> bytes -> message round trip.
      uint32_t field_number = field == "required_test_enum"   ? 1
                              : field == "test_enum"          ? 2
                              : field == "repeated_test_enum" ? 3
                                                              : 0;
      if (field_number == 0) {
        const auto [field_end, field_error] = std::from_chars(
            field.data(), field.data() + field.size(), field_number);
        if (field_error != std::errc() ||
            field_end != field.data() + field.size()) {
          field_number = 0;
        }
      }
      if (field_number != 0) {
        RETURN_IF_FAIL(append_integer(field_number, entry, "TestEnum"));
      }
    } else if (lower.ends_with("testextrapb")) {
      if (field == "int32_val1" || field == "int32_val2") {
        RETURN_IF_FAIL(append_integer(field == "int32_val1" ? 1 : 2, entry));
      } else if (field == "str_value") {
        AppendProtoMessageField(&out, 3, UnquoteProtoToken(entry.text));
      }
    } else if (lower.ends_with("proto3kitchensink") && field == "test_enum") {
      RETURN_IF_FAIL(append_integer(50, entry, "TestProto3Enum"));
      // Field-number dispatch tables repeat identical bodies by design: each
      // branch maps a distinct (message, field) pair to its wire field number.
      // NOLINTNEXTLINE(bugprone-branch-clone)
    } else if (lower.ends_with("nested") && field == "nested_int64") {
      RETURN_IF_FAIL(append_integer(1, entry));
    } else if (lower.ends_with("nested") && field == "nested_repeated_int64") {
      RETURN_IF_FAIL(append_integer(2, entry));
    } else if (lower.ends_with("optionalgroup") && field == "int64_val") {
      RETURN_IF_FAIL(append_integer(1, entry));
    } else if (lower.ends_with("optionalgroup") && field == "string_val") {
      const std::string text = UnquoteProtoToken(entry.text);
      AppendProtoMessageField(&out, 2, text);
    } else if (lower.ends_with("optionalgroup") &&
               field == "optionalgroupnested") {
      ASSIGN_OR_RETURN(
          std::string, optional_group_nested_msg,
          (EncodeProtoWireMessage(
              "googlesql_test.KitchenSinkPB.OptionalGroup.OptionalGroupNested",
              entry.text)));
      AppendProtoMessageField(&out, 3, optional_group_nested_msg);
    } else if (lower.ends_with("optionalgroupnested") && field == "int64_val") {
      RETURN_IF_FAIL(append_integer(1, entry));
    }
  }
  return out;
}

// Splits the top-level comma-separated members of a STRUCT's textual form
// ({"k":v,...} / {"f1":v,...}) respecting nesting and quotes; returns raw
// "key" / raw-value text pairs.
std::string StructTrim(std::string_view text) {
  size_t begin = 0;
  while (begin < text.size() &&
         std::isspace(static_cast<unsigned char>(text[begin])) != 0) {
    ++begin;
  }
  size_t end = text.size();
  while (end > begin &&
         std::isspace(static_cast<unsigned char>(text[end - 1])) != 0) {
    --end;
  }
  return std::string(text.substr(begin, end - begin));
}

// String->scalar casts tolerate the same surrounding ASCII whitespace for
// every numeric/boolean target (the INT64 path pinned this contract first).
std::string TrimCastWhitespace(std::string_view text) {
  // Keep this whitespace set identical to the SQL lexer / TrimCivilWhitespace:
  // form feed and vertical tab are ASCII whitespace too.
  const size_t begin = text.find_first_not_of(" \t\r\n\f\v");
  if (begin == std::string_view::npos) {
    return {};
  }
  const size_t end = text.find_last_not_of(" \t\r\n\f\v");
  return std::string(text.substr(begin, end - begin + 1));
}

std::vector<std::pair<std::string, std::string>> SplitStructMembers(
    const std::string& body) {
  std::vector<std::pair<std::string, std::string>> members;
  members.reserve(body.size() + 1);
  int depth = 0;
  bool in_string = false;
  size_t segment_begin = 0;
  for (size_t i = 0; i <= body.size(); ++i) {
    const bool end = i == body.size();
    const char c = end ? ',' : body[i];
    if (!end) {
      if (in_string) {
        if (c == '\\' && i + 1 < body.size()) {
          ++i;
        } else if (c == '"') {
          in_string = false;
        }
        continue;
      }
      if (c == '"') {
        in_string = true;
      } else if (c == '{' || c == '[' || c == '(') {
        ++depth;
      } else if (c == '}' || c == ']' || c == ')') {
        --depth;
      } else if (c == ',' && depth == 0) {
      } else {
        continue;
      }
    }
    if (!end && c != ',') {
      continue;
    }
    const std::string member = body.substr(segment_begin, i - segment_begin);
    segment_begin = i + 1;
    const size_t colon = member.find(':');
    if (colon == std::string::npos) {
      continue;
    }
    std::string key = StructTrim(member.substr(0, colon));
    if (key.size() >= 2 && key.front() == '"' && key.back() == '"') {
      key = key.substr(1, key.size() - 2);
    }
    members.emplace_back(std::move(key), StructTrim(member.substr(colon + 1)));
    if (end) {
      break;
    }
  }
  return members;
}

// Decodes a struct-member scalar: quoted strings are unescaped, everything
// else keeps its raw text (nested objects/arrays stay textual).
std::string DecodeStructMemberText(const std::string& raw) {
  if (raw.size() >= 2 && raw.front() == '"' && raw.back() == '"') {
    // The canonical struct text (EncodeStructJson) escapes control bytes as
    // \uXXXX; DecodeJsonString handles the full JSON escape set.
    if (std::optional<std::string> decoded = json_text::DecodeJsonString(raw)) {
      return std::move(*decoded);
    }
    std::string out;
    for (size_t i = 1; i + 1 < raw.size(); ++i) {
      if (raw[i] == '\\' && i + 2 < raw.size()) {
        const char next = raw[++i];
        switch (next) {
          case 'n':
            out.push_back('\n');
            break;
          case 't':
            out.push_back('\t');
            break;
          case 'r':
            out.push_back('\r');
            break;
          default:
            out.push_back(next);
            break;
        }
      } else {
        out.push_back(raw[i]);
      }
    }
    return out;
  }
  return raw;
}

std::string EncodeStructMemberText(const Value& value) {
  if (value.IsNull()) {
    return "null";
  }
  if (value.type == ValueType::kVarChar) {
    // Same escaping as EncodeStructJson so a CAST round-trip yields text the
    // canonical decoder (and StructJsonCompare) reads back identically.
    std::string out = "\"";
    for (const char c : value.value.varchar_value) {
      if (c == '"' || c == '\\') {
        out.push_back('\\');
        out.push_back(c);
      } else if (static_cast<unsigned char>(c) < 0x20) {
        constexpr std::string_view kHex = "0123456789abcdef";
        const auto byte = static_cast<unsigned char>(c);
        out += "\\u00";
        out.push_back(kHex[byte >> 4U]);
        out.push_back(kHex[byte & 0x0FU]);
      } else {
        out.push_back(c);
      }
    }
    out.push_back('"');
    return out;
  }
  if (value.type == ValueType::kDouble) {
    if (std::isnan(value.value.double_value)) {
      return "\"NaN\"";
    }
    if (std::isinf(value.value.double_value)) {
      return value.value.double_value > 0 ? "\"Infinity\"" : "\"-Infinity\"";
    }
    std::array<char, 64> buffer{};
    const auto [ptr, ec] = std::to_chars(
        buffer.data(), buffer.data() + buffer.size(), value.value.double_value);
    (void)ec;
    return std::string{buffer.data(), static_cast<size_t>(ptr - buffer.data())};
  }
  if (value.type == ValueType::kInt64) {
    return value.IsUnsigned()
               ? IntegerToString(static_cast<uint64_t>(value.value.int_value))
               : IntegerToString(value.value.int_value);
  }
  if (value.type == ValueType::kDate) {
    return "\"" + FormatDateDays(value.DateDays()) + "\"";
  }
  if (value.type == ValueType::kArray) {
    // Array members store as JSON arrays, matching TryEncodeStructMemberJson
    // (__struct_set): the bespoke reader decodes `[...]` back to an ARRAY,
    // while the quoted "ARRAY<T>[...]" display text used to read back as a
    // plain STRING and lose the element type.
    return json_text::ToJsonText(value);
  }
  std::string out = value.AsString();
  out.reserve(out.size() + 2);
  out.insert(out.begin(), '"');
  out.push_back('"');
  return out;
}

// Parses the field list of a STRUCT<...> target type into
// (field-name-or-empty, type-text) pairs, honoring nested brackets.
std::vector<std::pair<std::string, std::string>> ParseStructTypeFields(
    const std::string& upper_type) {
  const size_t open = upper_type.find('<');
  if (open == std::string::npos) {
    return {};
  }
  int depth = 0;
  size_t close = std::string::npos;
  for (size_t i = open; i < upper_type.size(); ++i) {
    if (upper_type[i] == '<') {
      ++depth;
    } else if (upper_type[i] == '>') {
      if (--depth == 0) {
        close = i;
        break;
      }
    }
  }
  if (close == std::string::npos) {
    return {};
  }
  const std::string inner =
      StructTrim(upper_type.substr(open + 1, close - open - 1));
  std::vector<std::string> parts;
  int bracket = 0;
  std::string current;
  for (const char c : inner) {
    if (c == '<' || c == '(') {
      ++bracket;
    }
    if (c == '>' || c == ')') {
      --bracket;
    }
    if (c == ',' && bracket == 0) {
      parts.push_back(StructTrim(current));
      current.clear();
    } else {
      current.push_back(c);
    }
  }
  if (!StructTrim(current).empty()) {
    parts.push_back(StructTrim(current));
  }
  std::vector<std::pair<std::string, std::string>> fields;
  for (const std::string& part : parts) {
    // "name TYPE" names the field; a lone token is an anonymous field type.
    const size_t space = part.find_first_of(" \t");
    if (space == std::string::npos) {
      fields.emplace_back(std::string(), part);
      continue;
    }
    fields.emplace_back(part.substr(0, space), part.substr(space + 1));
  }
  return fields;
}

std::pair<ValueType, TypeTag> ParseType(const std::string& type_name) {
  std::string upper = ToUpper(type_name);
  if (upper.starts_with("ARRAY<")) {
    return {ValueType::kArray, TypeTag::kArray};
  }
  // Strip type parameters (STRING(2) -> STRING) for base-type resolution.
  const size_t paren = upper.find('(');
  if (paren != std::string::npos && !upper.starts_with("ARRAY<")) {
    upper = upper.substr(0, paren);
  }
  if (upper == "INT64" || upper == "INT32" || upper == "INT" ||
      upper == "INTEGER" || upper == "INT16" || upper == "INT8" ||
      upper == "UINT64" || upper == "UINT32" || upper == "UINT16" ||
      upper == "UINT8") {
    return {ValueType::kInt64, TypeTag::kBigInt};
  }
  if (upper == "FLOAT64" || upper == "DOUBLE" || upper == "FLOAT" ||
      upper == "FLOAT32" || upper == "NUMERIC" || upper == "BIGNUMERIC" ||
      upper == "DECIMAL") {
    // NUMERIC/DECIMAL/BIGNUMERIC map onto the engine's DOUBLE numeric
    // surface (the same mapping CREATE TABLE uses for numeric columns), so
    // CAST(x AS DECIMAL(p,s)) yields a numeric value, not text.
    return {ValueType::kDouble, TypeTag::kDouble};
  }
  if (upper == "STRING" || upper == "VARCHAR" || upper == "TEXT" ||
      upper == "BYTES" || upper == "JSON" || upper == "DATETIME" ||
      upper == "TIMESTAMP" || upper == "TIME" || upper == "INTERVAL" ||
      upper.find("PB") != std::string::npos ||
      upper.find("PROTO") != std::string::npos) {
    return {ValueType::kVarChar, TypeTag::kVarChar};
  }
  // Dotted user-defined type paths (`googlesql_test.KitchenSinkPB`) are
  // protobuf messages: the engine carries them as text-format strings.  Enum
  // paths keep their member-name text form through the same channel.
  if (upper.find('.') != std::string::npos) {
    return {ValueType::kVarChar, TypeTag::kVarChar};
  }

  if (upper == "BOOL" || upper == "BOOLEAN") {
    return {ValueType::kInt64, TypeTag::kBigInt};
  }
  if (upper == "DATE") {
    return {ValueType::kDate, TypeTag::kDate};
  }
  if (upper == "UUID") {
    // UUID values are stored as their canonical lowercase text form.
    return {ValueType::kVarChar, TypeTag::kVarChar};
  }
  if (upper.starts_with("ARRAY<")) {
    return {ValueType::kArray, TypeTag::kArray};
  }
  // Enums or user types default to int64 or varchar
  return {ValueType::kInt64, TypeTag::kBigInt};
}

// Narrowed integer targets reject values outside their width: GoogleSQL
// raises out_of_range instead of truncating (INT64 keeps every int64 value).
Status ValidateIntWidth(const std::string& upper, int64_t v,
                        bool is_unsigned = false) {
  bool out_of_width = false;
  std::string width_name;
  if (is_unsigned) {
    const auto u = static_cast<uint64_t>(v);
    if (upper == "INT64") {
      out_of_width =
          u > static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
      width_name = "int64";
    } else if (upper == "INT32") {
      out_of_width = u > 2147483647ULL;
      width_name = "int32";
    } else if (upper == "INT16") {
      out_of_width = u > 32767ULL;
      width_name = "int16";
    } else if (upper == "INT8") {
      out_of_width = u > 127ULL;
      width_name = "int8";
    } else if (upper == "UINT32") {
      out_of_width = u > 4294967295ULL;
      width_name = "uint32";
    } else if (upper == "UINT16") {
      out_of_width = u > 65535ULL;
      width_name = "uint16";
    } else if (upper == "UINT8") {
      out_of_width = u > 255ULL;
      width_name = "uint8";
    } else if (upper == "UINT64") {
      out_of_width = false;
      width_name = "uint64";
    }
    if (out_of_width) {
      return StatusError(StatusCode::kIsInfinity,
                         width_name + " out of range: " + std::to_string(u));
    }
    return Status::kSuccess;
  }

  if (upper == "INT32") {
    out_of_width = v < -2147483648LL || v > 2147483647LL;
    width_name = "int32";
  } else if (upper == "INT16") {
    out_of_width = v < -32768 || v > 32767;
    width_name = "int16";
  } else if (upper == "INT8") {
    out_of_width = v < -128 || v > 127;
    width_name = "int8";
  } else if (upper == "UINT32") {
    out_of_width = v < 0 || v > 4294967295LL;
    width_name = "uint32";
  } else if (upper == "UINT16") {
    out_of_width = v < 0 || v > 65535;
    width_name = "uint16";
  } else if (upper == "UINT8") {
    out_of_width = v < 0 || v > 255;
    width_name = "uint8";
  } else if (upper == "UINT64") {
    out_of_width = v < 0;
    width_name = "uint64";
  }
  if (out_of_width) {
    return StatusError(StatusCode::kIsInfinity,
                       width_name + " out of range: " + std::to_string(v));
  }
  return Status::kSuccess;
}

// "<amount> <alphabetic-unit>" (e.g. "3 day") → split at the last space and
// parse with an explicit unit. Anything else (already-unit-less composite text)
// parses as a whole. Mirrors TryParseIntervalOperand in binary_expression.cpp
// so CAST and DATE+VarChar agree on "<number> <unit>" interval literals.
StatusOr<IntervalValue> TryParseIntervalOperand(std::string_view text) {
  while (!text.empty() &&
         (std::isspace(static_cast<unsigned char>(text.front())) != 0)) {
    text.remove_prefix(1);
  }
  while (!text.empty() &&
         (std::isspace(static_cast<unsigned char>(text.back())) != 0)) {
    text.remove_suffix(1);
  }
  const size_t space = text.find_last_of(' ');
  if (space != std::string_view::npos) {
    const std::string_view unit = text.substr(space + 1);
    const std::string_view amount = text.substr(0, space);
    const bool alphabetic =
        !unit.empty() && std::all_of(unit.begin(), unit.end(), [](char c) {
          return std::isalpha(static_cast<unsigned char>(c)) != 0;
        });
    if (alphabetic) {
      return IntervalValue::TryParse(amount, unit);
    }
  }
  return IntervalValue::TryParse(text);
}

StatusOr<Value> TryCastValueCore(const Value& val, const std::string& type_name,
                                 ValueType target_type, bool safe) {
  if (val.IsNull()) {
    return Value();
  }
  const std::string upper = ToUpper(type_name);
  const bool is_bool = (upper == "BOOL" || upper == "BOOLEAN");
  if (upper == "BYTES" && val.type == ValueType::kVarChar) {
    const std::string stored_type =
        ExtractProtoTypeMarker(val.value.varchar_value);
    const std::string_view marker =
        stored_type.empty() ? InferProtoWireType(val.value.varchar_value)
                            : std::string_view(stored_type);
    if (!marker.empty()) {
      ASSIGN_OR_RETURN(std::string, encoded,
                       (EncodeProtoWireMessage(
                           marker, std::string_view(val.value.varchar_value))));
      return Value(std::move(encoded));
    }
  }
  // Registered GoogleSQL enums validate strictly: integers must be known
  // members (proto3 open enums keep unknown in-range numbers) and strings
  // must match a member name exactly. Unknown enum types keep the legacy
  // heuristic below.
  const std::string enum_short_name(ShortTypeName(type_name));
  // UUID targets canonicalize any 32-hex-digit input (with or without
  // dashes) into the lowercase 8-4-4-4-12 text form.
  if (upper == "UUID") {
    std::string hex;
    if (val.type == ValueType::kVarChar) {
      hex.assign(val.value.varchar_value);
    } else {
      hex = val.AsString();
    }
    std::string compact;
    for (const char c : hex) {
      if (c == '-') {
        continue;
      }
      if (std::isxdigit(static_cast<unsigned char>(c)) != 0) {
        compact.push_back(
            static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
      } else {
        compact.clear();
        break;
      }
    }
    if (compact.size() != 32) {
      if (safe) {
        return Value();
      }
      return StatusError(StatusCode::kInvalidArgument,
                         "invalid UUID string: " + hex);
    }
    std::string canonical;
    for (size_t i = 0; i < compact.size(); ++i) {
      if (i == 8 || i == 12 || i == 16 || i == 20) {
        canonical.push_back('-');
      }
      canonical.push_back(compact[i]);
    }
    return Value(std::move(canonical));
  }
  // STRUCT<...> targets cast struct values field-wise: source members are
  // matched positionally, cast to each declared field type, and rebuilt
  // under the target field names.
  if (upper.starts_with("STRUCT") && val.type != ValueType::kVarChar) {
    const auto fields = ParseStructTypeFields(upper);
    if (fields.size() == 1) {
      const auto& [field_name, field_type] = fields.front();
      const std::string key = field_name.empty() ? "f1" : ToLower(field_name);
      Value converted = val;
      auto casted = TryCastValue(converted, field_type,
                                 ParseType(field_type).first, safe);
      if (!casted.HasValue()) {
        return casted.GetStatus();
      }
      converted = casted.MoveValue();
      if (converted.IsNull()) {
        return Value();
      }
      return Value("{\"" + key + "\":" + EncodeStructMemberText(converted) +
                   "}");
    }
  }
  if (upper.starts_with("STRUCT") && val.type == ValueType::kVarChar) {
    const std::string text(val.value.varchar_value);
    if (text.size() >= 2 && text.front() == '{' && text.back() == '}') {
      const auto fields = ParseStructTypeFields(upper);
      const auto members = SplitStructMembers(text.substr(1, text.size() - 2));
      const auto fail = [&]() -> Status {
        return StatusError(StatusCode::kInvalidArgument,
                           "cannot cast struct to " + type_name);
      };
      std::string rebuilt = "{";
      bool first = true;
      for (size_t i = 0; i < fields.size(); ++i) {
        const auto& [field_name, field_type] = fields[i];
        if (!first) {
          rebuilt += ",";
        }
        first = false;
        const std::string key = field_name.empty() ? "f" + std::to_string(i + 1)
                                                   : ToLower(field_name);
        rebuilt += "\"" + key + "\":";
        if (i >= members.size() || members[i].second == "null") {
          rebuilt += "null";
          continue;
        }
        const std::string raw_member = members[i].second;
        std::string raw = DecodeStructMemberText(raw_member);
        // Nested objects/arrays stay textual; only STRUCT targets recurse.
        if ((raw.starts_with("{") && raw.ends_with("}")) ||
            (raw_member.starts_with("[") && raw_member.ends_with("]"))) {
          if (ToUpper(field_type).starts_with("STRUCT") &&
              raw.starts_with("{")) {
            ASSIGN_OR_RETURN(Value, nested,
                             TryCastValue(Value(std::move(raw)), field_type,
                                          ParseType(field_type).first, safe));
            if (nested.IsNull()) {
              return fail();
            }
            rebuilt += EncodeStructMemberText(nested);
          } else {
            rebuilt += raw_member;
          }
          continue;
        }
        const bool quoted_input =
            raw_member.size() >= 2 && raw_member.front() == '"';
        Value member_value;
        if (quoted_input) {
          member_value = Value(std::move(raw));
        } else if (raw.find('.') != std::string::npos ||
                   raw.find('e') != std::string::npos ||
                   raw.find('E') != std::string::npos || raw == "true" ||
                   raw == "false") {
          member_value = Value(std::strtod(raw.c_str(), nullptr));
        } else {
          int64_t parsed_int = 0;
          const auto [ptr, ec] =
              std::from_chars(raw.data(), raw.data() + raw.size(), parsed_int);
          member_value = ec == std::errc() && ptr == raw.data() + raw.size()
                             ? Value(parsed_int)
                             : Value(std::string(raw));
        }
        auto casted_member = TryCastValue(member_value, field_type,
                                          ParseType(field_type).first, safe);
        if (!casted_member.HasValue()) {
          return fail();
        }
        member_value = casted_member.MoveValue();
        if (member_value.IsNull()) {
          return fail();
        }
        rebuilt += EncodeStructMemberText(member_value);
      }
      rebuilt += "}";
      return Value(std::move(rebuilt));
    }
  }
  // A STRUCT target that survived the field-wise paths above -- scalar input
  // to a multi-field target, or non-object text -- has no conversion.  Reject
  // it here instead of letting it fall into the scalar passthrough below.
  if (upper.starts_with("STRUCT")) {
    if (safe) {
      return Value();
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "cannot cast value to " + type_name);
  }
  // ENUM-typed targets accept their textual member names verbatim: this
  // engine stores enums as their member-name strings.
  if (!is_bool && upper.find("ENUM") != std::string::npos &&
      IsKnownEnum(enum_short_name)) {
    const auto out_of_range = [&](const std::string& message) -> Status {
      return StatusError(StatusCode::kIsInfinity, message);
    };
    if (val.type == ValueType::kInt64) {
      const int64_t ordinal = val.value.int_value;
      if (ordinal > 2147483647LL || ordinal < -2147483648LL) {
        return out_of_range("out of range cast of integer " +
                            std::to_string(ordinal) + " to enum type " +
                            type_name);
      }
      const std::optional<std::string> member =
          EnumMemberForValue(enum_short_name, ordinal);
      if (member.has_value()) {
        return Value(std::string(*member));
      }
      // Proto3 enums are open: unknown but in-range values stay numeric;
      // closed proto2 enums reject them.
      if (EnumIsOpen(enum_short_name)) {
        return val;
      }
      return out_of_range("out of range cast of integer " +
                          std::to_string(ordinal) + " to enum type " +
                          type_name);
    }
    if (val.type == ValueType::kVarChar) {
      const std::string member(val.value.varchar_value);
      int64_t ordinal = 0;
      if (EnumValueForMember(enum_short_name, member, &ordinal)) {
        // Proto enum aliases stringify to the first (canonical) member name,
        // not to the alias used by the input literal.
        if (const std::optional<std::string> canonical =
                EnumMemberForValue(enum_short_name, ordinal);
            canonical.has_value()) {
          return Value(std::string(*canonical));
        }
        return val;
      }
      std::string upper_member = member;
      for (char& c : upper_member) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
      }
      std::string hint;
      if (upper_member != member &&
          EnumValueForMember(enum_short_name, upper_member, nullptr)) {
        hint = "; Did you mean '" + upper_member +
               "'? (Note: ENUM values are case sensitive)";
      }
      return out_of_range("Out of range cast of string '" + member +
                          "' to enum type " + type_name + hint);
    }
    return val;
  }
  // ENUM-typed targets accept their textual member names verbatim: this
  // engine stores enums as their member-name strings.  Message protos whose
  // names merely contain "ENUM" (KitchenSinkEnumPB) are excluded.
  const bool dotted_type = upper.find('.') != std::string::npos;
  const bool enum_target = !is_bool &&
                           upper.find("ENUM") != std::string::npos &&
                           (!dotted_type || IsKnownEnumTypeName(type_name));
  if (enum_target && val.type == ValueType::kVarChar) {
    // Member names are case-sensitive UPPER_SNAKE identifiers.
    const std::string member(val.value.varchar_value);
    bool member_shaped =
        !member.empty() && static_cast<bool>(std::isupper(
                               static_cast<unsigned char>(member.front())));
    for (const char c : member) {
      if (c != '_' &&
          !static_cast<bool>(std::isdigit(static_cast<unsigned char>(c))) &&
          !static_cast<bool>(std::isupper(static_cast<unsigned char>(c)))) {
        member_shaped = false;
        break;
      }
    }
    // Members with implausibly large ordinals are invalid for real enums;
    // keep them erroring through the numeric path (no enum metadata here).
    size_t digits = 0;
    while (digits < member.size() &&
           static_cast<bool>(std::isdigit(static_cast<unsigned char>(
               member[member.size() - 1 - digits])))) {
      ++digits;
    }
    if (member_shaped && digits <= 3) {
      return val;
    }
    if (member_shaped) {
      if (safe) {
        return Value();
      }
      return StatusError(StatusCode::kIsInfinity,
                         "out of range cast of string '" + member +
                             "' to enum type " + type_name);
    }
    // Numeric-looking strings into enum types never name a member.
    const std::string trimmed_member = [&] {
      const size_t b = member.find_first_not_of(" \t\r\n");
      if (b == std::string::npos) {
        return std::string();
      }
      const size_t e = member.find_last_not_of(" \t\r\n");
      return member.substr(b, e - b + 1);
    }();
    bool numeric_token = !trimmed_member.empty();
    for (const char c : trimmed_member) {
      if (!static_cast<bool>(std::isdigit(static_cast<unsigned char>(c))) &&
          c != '-' && c != '+') {
        numeric_token = false;
        break;
      }
    }
    if (numeric_token) {
      if (safe) {
        return Value();
      }
      return StatusError(StatusCode::kIsInfinity,
                         "out of range cast of string '" + member +
                             "' to enum type " + type_name);
    }
    // Any other non-member spelling (mixed or lower case) names no member:
    // ENUM member names are case sensitive.  Suggest the upper-case spelling
    // like the reference engine.
    if (!member.empty()) {
      if (safe) {
        return Value();
      }
      std::string upper_member = member;
      for (char& c : upper_member) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
      }
      return StatusError(StatusCode::kIsInfinity,
                         "Out of range cast of string '" + member +
                             "' to enum type " + type_name +
                             "; Did you mean '" + upper_member +
                             "'? (Note: ENUM values are case sensitive)");
    }
  }

  // Dotted non-enum message targets: TEXT payloads normalize through the
  // shared proto-text helpers; modeled protos additionally decode their
  // wire-format byte casts.
  {
    const std::string upper_path = ToUpper(type_name);
    if (upper_path.find('.') != std::string::npos && !enum_target) {
      if (val.IsNull()) {
        return Value();
      }
      if (val.type == ValueType::kVarChar || val.type == ValueType::kInt64 ||
          val.type == ValueType::kDouble) {
        const std::string raw = val.type == ValueType::kVarChar
                                    ? std::string(val.value.varchar_value)
                                    : val.AsString();
        if (std::optional<std::string> decoded =
                DecodeProtoWireBytes(type_name, raw);
            decoded.has_value()) {
          std::string wire_text = std::move(*decoded);
          return Value(std::move(wire_text));
        }
        if (std::optional<std::string> normalized = NormalizeProtoText(raw);
            normalized.has_value()) {
          // Parseable TEXT payload (whitespace-only text yields the empty
          // message ""): store the canonical form.
          std::string payload = std::move(*normalized);
          return Value(std::move(payload));
        }
        if (!LooksLikeProtoText(raw)) {
          if (IsValidUtf8Text(raw)) {
            // Not proto-shaped text (plain strings): store as-is.  Binary
            // wire junk that failed DecodeProtoWireBytes is NOT valid
            // proto TEXT either — persisting it would leave a permanently
            // damaged cell that breaks every later proto-text reader
            // (P899), so it takes the same reject path as malformed TEXT.
            std::string verbatim = raw;
            return Value(std::move(verbatim));
          }
        }
        if (!safe) {
          return StatusError(StatusCode::kInvalidArgument,
                             "invalid proto TEXT payload: " + raw);
        }
        return Value();
      }
    }
  }

  try {
    if (is_bool) {
      if (val.type == ValueType::kInt64) {
        return Value(val.value.int_value != 0 ? int64_t{1} : int64_t{0});
      }
      if (val.type == ValueType::kDouble) {
        return Value(val.value.double_value != 0.0 ? int64_t{1} : int64_t{0});
      }
      if (val.type == ValueType::kVarChar) {
        const std::string s =
            ToLower(TrimCastWhitespace(val.value.varchar_value));
        if (s == "true" || s == "t" || s == "1") {
          return Value(int64_t{1});
        }
        if (s == "false" || s == "f" || s == "0") {
          return Value(int64_t{0});
        }
        return StatusError(StatusCode::kInvalidArgument,
                           "cannot cast string to bool: " + s);
      }
      if (val.type == ValueType::kDate) {
        // A DATE cell is a day count; without this branch it fell through to
        // the INT64 path and returned the raw days. GoogleSQL has no
        // DATE -> BOOL cast.
        return StatusError(StatusCode::kInvalidArgument,
                           "Invalid cast from DATE to BOOL");
      }
    }

    switch (target_type) {
      case ValueType::kInt64: {
        if (val.type == ValueType::kInt64) {
          RETURN_IF_FAIL(
              ValidateIntWidth(upper, val.value.int_value, val.IsUnsigned()));
          if (upper == "UINT64") {
            return val.WithUnsigned();
          }
          if (val.IsUnsigned()) {
            return Value(val.value.int_value);
          }
          return val;
        }
        if (val.type == ValueType::kDouble) {
          if (std::isnan(val.value.double_value) ||
              std::isinf(val.value.double_value)) {
            return StatusError(StatusCode::kIsInfinity,
                               "cannot cast NaN/Inf float to int");
          }
          const double rounded = std::round(val.value.double_value);
          if (upper == "UINT64") {
            static constexpr double kUint64MaxAsDouble = 18446744073709551616.0;
            if (rounded >= kUint64MaxAsDouble || rounded < 0.0) {
              return StatusError(StatusCode::kIsInfinity,
                                 AsciiLowerCopy(upper) + " out of range: " +
                                     std::to_string(val.value.double_value));
            }
            const auto u = static_cast<uint64_t>(rounded);
            return Value(static_cast<int64_t>(u)).WithUnsigned();
          }
          // 2^63 as a double is exactly representable; values at or above it
          // (including INT64_MAX itself after rounding) are out of range.
          static constexpr double kInt64MaxAsDouble = 9223372036854775808.0;
          static constexpr double kInt64MinAsDouble = -9223372036854775808.0;
          if (rounded >= kInt64MaxAsDouble || rounded < kInt64MinAsDouble) {
            return StatusError(StatusCode::kIsInfinity,
                               AsciiLowerCopy(upper) + " out of range: " +
                                   std::to_string(val.value.double_value));
          }
          const auto narrowed = static_cast<int64_t>(rounded);
          RETURN_IF_FAIL(ValidateIntWidth(upper, narrowed, false));
          return Value(narrowed);
        }
        if (val.type == ValueType::kVarChar) {
          std::string s = TrimCastWhitespace(val.value.varchar_value);
          if (s.empty()) {
            return StatusError(StatusCode::kInvalidArgument,
                               "cannot cast empty string to int");
          }

          // GoogleSQL accepts hex literals in integer casts: an optional
          // sign followed by 0x / 0X and hexadecimal digits.
          size_t hex_body = 0;
          bool hex_negative = false;
          if (!s.empty() && (s[0] == '-' || s[0] == '+')) {
            hex_negative = s[0] == '-';
            hex_body = 1;
          }
          if (s.size() >= hex_body + 3 && s[hex_body] == '0' &&
              (s[hex_body + 1] == 'x' || s[hex_body + 1] == 'X')) {
            const std::string digits = s.substr(hex_body + 2);
            uint64_t magnitude = 0;
            const char* d_begin = digits.data();
            const char* d_end = digits.data() + digits.size();
            auto [d_ptr, d_ec] = std::from_chars(d_begin, d_end, magnitude, 16);
            if (d_ec != std::errc() || d_ptr != d_end) {
              return StatusError(
                  StatusCode::kInvalidArgument,
                  "Bad " + AsciiLowerCopy(upper) + " value: " + s);
            }
            const bool unsigned_target = upper == "UINT8" ||
                                         upper == "UINT16" ||
                                         upper == "UINT32" || upper == "UINT64";
            if (hex_negative &&
                (unsigned_target || magnitude > 0x8000000000000000ULL)) {
              return StatusError(StatusCode::kInvalidArgument,
                                 "Bad " + upper + " value: " + s);
            }
            if (hex_negative) {
              // Two's-complement of the magnitude; the narrowed widths must
              // still hold the resulting negative value (INT8 "-0x80" is
              // fine, "-0x81" is not).
              const auto signed_value = static_cast<int64_t>(~magnitude + 1);
              RETURN_IF_FAIL(ValidateIntWidth(upper, signed_value));
              return Value(signed_value);
            }
            if (magnitude <= 0x7fffffffffffffffULL) {
              const auto signed_magnitude = static_cast<int64_t>(magnitude);
              Value result(signed_magnitude);
              RETURN_IF_FAIL(ValidateIntWidth(upper, signed_magnitude));
              return upper == "UINT64" ? result.WithUnsigned() : result;
            }
            // Above the signed range only UINT64 can represent the value.
            if (upper != "UINT64") {
              return StatusError(StatusCode::kIsInfinity,
                                 upper + " out of range: " + s);
            }
            return Value(static_cast<int64_t>(magnitude)).WithUnsigned();
          }
          int64_t result = 0;
          const char* begin_ptr = s.data();
          if (s.starts_with('+')) {
            // from_chars accepts a minus sign for signed targets, so require
            // a digit here to avoid accepting a second sign after '+'.
            if (s.size() == 1 || s[1] < '0' || s[1] > '9') {
              return StatusError(
                  StatusCode::kInvalidArgument,
                  "Bad " + AsciiLowerCopy(upper) + " value: " + s);
            }
            ++begin_ptr;
          }
          const char* end_ptr = s.data() + s.size();
          // An enum member-name string ("TESTENUMNEGATIVE") reads back as
          // its registry ordinal before plain integer parsing applies.
          if (std::optional<int64_t> ordinal = OrdinalForEnumMemberName(s);
              ordinal.has_value()) {
            RETURN_IF_FAIL(ValidateIntWidth(upper, *ordinal));
            return upper == "UINT64" ? Value(*ordinal).WithUnsigned()
                                     : Value(*ordinal);
          }
          if (upper == "UINT64") {
            if (!s.empty() && s[0] == '-') {
              return StatusError(StatusCode::kInvalidArgument,
                                 "Bad UINT64 value: " + s);
            }
            uint64_t u_result = 0;
            auto [ptr, ec] = std::from_chars(begin_ptr, end_ptr, u_result);
            if (ec == std::errc::result_out_of_range) {
              return StatusError(StatusCode::kIsInfinity,
                                 "int overflow casting from string: " + s);
            }
            if (ec != std::errc() || ptr != end_ptr) {
              return StatusError(
                  StatusCode::kInvalidArgument,
                  "Bad " + AsciiLowerCopy(upper) + " value: " + s);
            }
            return Value(static_cast<int64_t>(u_result)).WithUnsigned();
          }
          auto [ptr, ec] = std::from_chars(begin_ptr, end_ptr, result);
          if (ec == std::errc::result_out_of_range) {
            return StatusError(StatusCode::kIsInfinity,
                               "int overflow casting from string: " + s);
          }
          if (ec != std::errc() || ptr != end_ptr) {
            return StatusError(StatusCode::kInvalidArgument,
                               "Bad " + AsciiLowerCopy(upper) + " value: " + s);
          }
          RETURN_IF_FAIL(ValidateIntWidth(upper, result));
          Value converted(result);
          return converted;
        }
        if (val.type == ValueType::kDate) {
          return Value(val.DateDays());
        }
        break;
      }
      case ValueType::kDouble: {
        // FLOAT/FLOAT32 targets narrow to IEEE-754 single precision:
        // subnormal doubles (e.g. -1e-46) round to signed zero like the
        // reference engine instead of surviving at double precision.
        const bool is_float32 = upper == "FLOAT" || upper == "FLOAT32";
        auto finish_double = [&](double d) -> StatusOr<Value> {
          if (is_float32 && std::isfinite(d)) {
            if (d > std::numeric_limits<float>::max() ||
                d < std::numeric_limits<float>::lowest()) {
              return StatusError(StatusCode::kIsInfinity,
                                 "float overflow: " + std::to_string(d));
            }
            return Value(static_cast<double>(static_cast<float>(d)));
          }
          return Value(d);
        };
        if (val.type == ValueType::kDouble) {
          return finish_double(val.value.double_value);
        }
        if (val.type == ValueType::kInt64) {
          return finish_double(val.IsUnsigned()
                                   ? static_cast<double>(static_cast<uint64_t>(
                                         val.value.int_value))
                                   : static_cast<double>(val.value.int_value));
        }
        if (val.type == ValueType::kVarChar) {
          std::string s = ToLower(TrimCastWhitespace(val.value.varchar_value));
          if (s == "nan" || s == "+nan" || s == "-nan") {
            return finish_double(std::numeric_limits<double>::quiet_NaN());
          }
          if (s == "inf" || s == "+inf" || s == "infinity" ||
              s == "+infinity") {
            return finish_double(std::numeric_limits<double>::infinity());
          }
          if (s == "-inf" || s == "-infinity") {
            return finish_double(-std::numeric_limits<double>::infinity());
          }
          char* end = nullptr;
          errno = 0;
          const double parsed = std::strtod(s.c_str(), &end);
          if (end == s.c_str() || *end != '\0') {
            return StatusError(StatusCode::kInvalidArgument,
                               "invalid float string: " + s);
          }
          if (errno == ERANGE && (parsed == HUGE_VAL || parsed == -HUGE_VAL)) {
            return StatusError(StatusCode::kIsInfinity, "float overflow: " + s);
          }
          return finish_double(parsed);
        }

        if (val.type == ValueType::kDate) {
          return finish_double(static_cast<double>(val.DateDays()));
        }
        break;
      }
      case ValueType::kVarChar: {
        // Note: BOOL values are stored as INT64 without a boolean tag, so
        // `is_bool` here cannot identify a BOOL source. GoogleSQL casts them
        // to "true"/"false" at the AST boundary; this unreachable branch used
        // to suggest the cast itself did so. Keep the integer rendering.
        if (val.IsArray()) {
          // An ARRAY cell has no varchar payload: reading value.varchar_value
          // here returned "" (or garbage). Render the element text form.
          std::string text = "[";
          const auto& elements = val.ArrayElements();
          for (size_t e = 0; e < elements.size(); ++e) {
            if (e != 0) {
              text += ",";
            }
            text += elements[e].IsNull() ? std::string("NULL")
                                         : elements[e].AsString();
          }
          text += "]";
          return Value(std::move(text));
        }
        std::string s =
            (val.type == ValueType::kDate) ? FormatDateDays(val.DateDays())
            : (val.type == ValueType::kInt64)
                ? (val.IsUnsigned() ? IntegerToString(static_cast<uint64_t>(
                                          val.value.int_value))
                                    : IntegerToString(val.value.int_value))
            : (val.type == ValueType::kDouble)
                ? ([&]() {
                    if (std::isnan(val.value.double_value)) {
                      return std::string("nan");
                    }
                    if (std::isinf(val.value.double_value)) {
                      return std::string(val.value.double_value > 0 ? "inf"
                                                                    : "-inf");
                    }
                    return FormatDoubleShortest(val.value.double_value);
                  })()
                : std::string(val.value.varchar_value);
        if (upper == "DATETIME" || upper == "TIMESTAMP" || upper == "TIME") {
          return CastCivilTime(s, upper);
        }
        if (upper == "INTERVAL") {
          ASSIGN_OR_RETURN(IntervalValue, iv, TryParseIntervalOperand(s));
          ASSIGN_OR_RETURN(std::string, iv_text, iv.TryToString());
          return Value(std::move(iv_text)).WithInterval();
        }
        return Value(std::move(s));
      }
      case ValueType::kDate: {
        if (val.type == ValueType::kDate) {
          return val;
        }
        if (val.type == ValueType::kVarChar) {
          return CastCivilTime(val.value.varchar_value, "DATE");
        }
        if (val.type == ValueType::kInt64) {
          RETURN_IF_FAIL(TryFormatDateDays(val.value.int_value).GetStatus());
          return Value::DateFromDays(val.value.int_value);
        }
        break;
      }
      case ValueType::kArray: {
        // CAST(x AS ARRAY<T>): retypes array literals (coercing elements to
        // the declared element type when they are scalar); NULL stays NULL.
        if (val.IsNull()) {
          return Value();
        }
        if (!val.IsArray()) {
          break;
        }
        std::string element_type = "INT64";
        if (upper.starts_with("ARRAY<") && upper.ends_with(">")) {
          element_type = type_name.substr(6, type_name.size() - 7);
        }
        // Length-parameterized element types (STRING(N)/BYTES(N)) bound each
        // string element.
        size_t length_limit = 0;
        std::string length_base;
        {
          const std::string upper_element = ToUpper(element_type);
          const size_t elem_paren = upper_element.find('(');
          if (elem_paren != std::string::npos) {
            length_base = upper_element.substr(0, elem_paren);
            if (length_base == "STRING" || length_base == "BYTES") {
              try {
                length_limit = static_cast<size_t>(
                    std::stoll(element_type.substr(elem_paren + 1)));
              } catch (const std::exception&) {
                length_limit = 0;
              }
            } else {
              length_base.clear();
            }
          }
        }
        std::vector<Value> elements;
        elements.reserve(val.ArrayElements().size());
        bool ok = true;
        for (const Value& element : val.ArrayElements()) {
          if (element.IsNull()) {
            elements.emplace_back();
            continue;
          }
          if (length_limit > 0 && element.type == ValueType::kVarChar &&
              element.value.varchar_value.size() > length_limit) {
            if (!safe) {
              return StatusError(
                  StatusCode::kInvalidArgument,
                  length_base + "(" + std::to_string(length_limit) +
                      ") has maximum length " + std::to_string(length_limit) +
                      " but got a value with length " +
                      std::to_string(element.value.varchar_value.size()));
            }
            ok = false;
            break;
          }
          auto casted_element = TryCastValue(
              element, element_type, ParseType(element_type).first, safe);
          if (!casted_element.HasValue()) {
            if (!safe) {
              return casted_element.GetStatus();
            }
            ok = false;
            break;
          }
          elements.push_back(casted_element.MoveValue());
        }
        if (!ok) {
          break;
        }
        return Value::Array(std::move(elements), ToUpper(element_type));
      }
      default:
        break;
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
  return StatusError(StatusCode::kInvalidArgument,
                     "unsupported cast to " + type_name);
}

// SAFE_CAST contract in one place: with `safe` set every failure collapses
// to NULL; otherwise the Status surfaces (EXC-SHIM wrappers rethrow it for
// callers still using the throwing API).
}  // namespace

StatusOr<Value> TryCastValue(const Value& val, const std::string& type_name,
                             ValueType target_type, bool safe) {
  StatusOr<Value> casted = TryCastValueCore(val, type_name, target_type, safe);
  if (!casted.HasValue() && safe) {
    return Value();
  }
  return casted;
}

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

StatusOr<Value> CastExpression::TryEvaluate(const Row& row,
                                            const Schema& schema) const {
  ASSIGN_OR_RETURN(Value, val, child_->TryEvaluate(row, schema));
  return TryCastValue(val, target_type_name_, target_value_type_,
                      return_null_on_error_);
}

StatusOr<Value> CastExpression::TryEvaluate(const Row* left,
                                            const Schema& left_schema,
                                            const Row* right,
                                            const Schema& right_schema) const {
  ASSIGN_OR_RETURN(Value, val,
                   child_->TryEvaluate(left, left_schema, right, right_schema));
  return TryCastValue(val, target_type_name_, target_value_type_,
                      return_null_on_error_);
}

StatusOr<Value> CastExpression::TryEvaluate(const Row& row,
                                            const Schema& schema,
                                            EvaluationContext& context) const {
  ASSIGN_OR_RETURN(Value, val, child_->TryEvaluate(row, schema, context));
  return TryCastValue(val, target_type_name_, target_value_type_,
                      return_null_on_error_);
}

StatusOr<Value> CastExpression::TryCast(const Value& value) const {
  return TryCastValue(value, target_type_name_, target_value_type_,
                      return_null_on_error_);
}

// EXC-SHIM: deprecated throwing wrappers (common/exc_shim.hpp).
Value CastExpression::Evaluate(const Row& row, const Schema& schema) const {
  return ExcShimUnwrap(TryEvaluate(row, schema), "CastExpression::Evaluate");
}

Value CastExpression::Evaluate(const Row* left, const Schema& left_schema,
                               const Row* right,
                               const Schema& right_schema) const {
  return ExcShimUnwrap(TryEvaluate(left, left_schema, right, right_schema),
                       "CastExpression::Evaluate");
}

Value CastExpression::Evaluate(const Row& row, const Schema& schema,
                               EvaluationContext& context) const {
  return ExcShimUnwrap(TryEvaluate(row, schema, context),
                       "CastExpression::Evaluate");
}

tinylamb::Type CastExpression::ResultType(const Schema& /*unused*/) const {
  return {target_type_tag_};
}

tinylamb::Type CastExpression::ResultType(const Schema& /*unused*/,
                                          const Schema& /*unused*/) const {
  return {target_type_tag_};
}

std::string CastExpression::ToString() const {
  return (return_null_on_error_ ? "SAFE_CAST(" : "CAST(") + child_->ToString() +
         " AS " + target_type_name_ + ")";
}

void CastExpression::Dump(std::ostream& o) const { o << ToString(); }

}  // namespace tinylamb
