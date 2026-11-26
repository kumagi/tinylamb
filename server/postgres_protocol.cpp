/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "server/postgres_protocol.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/string_util.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::pgwire {
namespace {

void AppendCString(std::string* output, std::string_view value) {
  output->append(value);
  output->push_back('\0');
}

std::string Message(char type, const std::string& payload) {
  std::string result;
  result.reserve(payload.size() + 5);
  result.push_back(type);
  AppendUint32(&result, static_cast<uint32_t>(payload.size() + 4));
  result += payload;
  return result;
}

std::string SanitizeField(std::string_view value) {
  if (value.find('\0') == std::string_view::npos) {
    return std::string(value);  // common case: no copy-and-scan round trip
  }
  std::string result(value);
  std::ranges::replace(result, '\0', ' ');
  return result;
}

std::string ValueText(const Value& value) {
  switch (value.type) {
    case ValueType::kNull:
      return {};
    case ValueType::kInt64:
      return value.AsString();
    case ValueType::kVarChar:
      return std::string(value.value.varchar_value);
    case ValueType::kDate:
      return value.AsString();
    case ValueType::kDouble:
      // PostgreSQL emits the shortest round-tripping representation (0.1,
      // not 0.10000000000000001); max_digits10 with the default float format
      // always printed 17 digits.  Non-finite values use PostgreSQL's wire
      // spelling (NaN / Infinity / -Infinity): strict clients (JDBC's
      // Double.parseDouble) reject the C++ ostream "nan"/"inf" forms.
      if (std::isnan(value.value.double_value)) {
        return "NaN";
      }
      if (value.value.double_value == std::numeric_limits<double>::infinity()) {
        return "Infinity";
      }
      if (value.value.double_value ==
          -std::numeric_limits<double>::infinity()) {
        return "-Infinity";
      }
      return FormatDoubleShortest(value.value.double_value);
    case ValueType::kArray:
      return value.AsString();
  }
  return {};
}

struct PgType {
  uint32_t oid;
  int16_t length;
};

PgType ToPgType(ValueType type) {
  switch (type) {
    case ValueType::kInt64:
      return {.oid = 20, .length = 8};  // int8
    case ValueType::kDouble:
      return {.oid = 701, .length = 8};  // float8
    case ValueType::kDate:
      return {.oid = 1082, .length = 4};  // date
    case ValueType::kNull:
    case ValueType::kVarChar:
    case ValueType::kArray:
      return {.oid = 25, .length = -1};  // text
  }
  return {.oid = 25, .length = -1};
}

std::string_view Trim(std::string_view value) {
  const size_t begin = value.find_first_not_of(" \t\r\n");
  if (begin == std::string_view::npos) {
    return {};
  }
  const size_t end = value.find_last_not_of(" \t\r\n");
  return value.substr(begin, end - begin + 1);
}

size_t Utf8CharacterCount(std::string_view text) {
  return static_cast<size_t>(std::ranges::count_if(
      text, [](unsigned char c) { return (c & 0xC0) != 0x80; }));
}

}  // namespace

StatusOr<uint16_t> ReadUint16(std::string_view bytes, size_t offset) {
  if (offset > bytes.size() || bytes.size() - offset < sizeof(uint16_t)) {
    return Status(Status::kCorrupt, "short PostgreSQL protocol field");
  }
  const auto* data = reinterpret_cast<const unsigned char*>(bytes.data());
  return static_cast<uint16_t>((static_cast<uint16_t>(data[offset]) << 8U) |
                               static_cast<uint16_t>(data[offset + 1]));
}

StatusOr<uint32_t> ReadUint32(std::string_view bytes, size_t offset) {
  if (offset > bytes.size() || bytes.size() - offset < sizeof(uint32_t)) {
    return Status(Status::kCorrupt, "short PostgreSQL protocol field");
  }
  const auto* data = reinterpret_cast<const unsigned char*>(bytes.data());
  return (static_cast<uint32_t>(data[offset]) << 24U) |
         (static_cast<uint32_t>(data[offset + 1]) << 16U) |
         (static_cast<uint32_t>(data[offset + 2]) << 8U) |
         static_cast<uint32_t>(data[offset + 3]);
}

void AppendUint16(std::string* output, uint16_t value) {
  output->push_back(static_cast<char>((value >> 8U) & 0xffU));
  output->push_back(static_cast<char>(value & 0xffU));
}

void AppendUint32(std::string* output, uint32_t value) {
  output->push_back(static_cast<char>((value >> 24U) & 0xffU));
  output->push_back(static_cast<char>((value >> 16U) & 0xffU));
  output->push_back(static_cast<char>((value >> 8U) & 0xffU));
  output->push_back(static_cast<char>(value & 0xffU));
}

namespace {

constexpr int64_t kPgDateEpochOffsetDays = 10957;  // 2000-01-01 - 1970-01-01.

inline void WriteBe16(char* dst, uint16_t value) noexcept {
  dst[0] = static_cast<char>((value >> 8U) & 0xffU);
  dst[1] = static_cast<char>(value & 0xffU);
}

inline void WriteBe32(char* dst, uint32_t value) noexcept {
  dst[0] = static_cast<char>((value >> 24U) & 0xffU);
  dst[1] = static_cast<char>((value >> 16U) & 0xffU);
  dst[2] = static_cast<char>((value >> 8U) & 0xffU);
  dst[3] = static_cast<char>(value & 0xffU);
}

inline void WriteBe64(char* dst, uint64_t value) noexcept {
  dst[0] = static_cast<char>((value >> 56U) & 0xffU);
  dst[1] = static_cast<char>((value >> 48U) & 0xffU);
  dst[2] = static_cast<char>((value >> 40U) & 0xffU);
  dst[3] = static_cast<char>((value >> 32U) & 0xffU);
  dst[4] = static_cast<char>((value >> 24U) & 0xffU);
  dst[5] = static_cast<char>((value >> 16U) & 0xffU);
  dst[6] = static_cast<char>((value >> 8U) & 0xffU);
  dst[7] = static_cast<char>(value & 0xffU);
}

inline void AppendUint64(std::string* output, uint64_t value) {
  std::array<char, 8> buf{};
  WriteBe64(buf.data(), value);
  output->append(buf.data(), buf.size());
}

inline uint16_t ResolveColumnFormat(const std::vector<uint16_t>& result_formats,
                                    size_t col_idx) noexcept {
  if (result_formats.empty()) {
    return 0;
  }
  if (result_formats.size() == 1) {
    return result_formats[0];
  }
  return col_idx < result_formats.size() ? result_formats[col_idx] : 0;
}

StatusOr<std::string> ReadCString(std::string_view payload, size_t* cursor) {
  const size_t nul = payload.find('\0', *cursor);
  if (nul == std::string_view::npos) {
    return StatusError(StatusCode::kInvalidArgument,
                       "unterminated C-string in PostgreSQL message");
  }
  std::string out(payload.substr(*cursor, nul - *cursor));
  *cursor = nul + 1;
  return out;
}

StatusOr<std::string> RenderParamAsSqlLiteral(
    uint32_t param_oid, uint16_t format_code,
    const std::optional<std::string>& raw) {
  if (!raw.has_value()) {
    return std::string("NULL");
  }
  const std::string& bytes = *raw;
  if (format_code == 1) {
    // Binary parameter decoding for standard PostgreSQL OIDs.
    if (param_oid == 20 && bytes.size() == 8) {  // int8
      const auto* p = reinterpret_cast<const unsigned char*>(bytes.data());
      const uint64_t u = (static_cast<uint64_t>(p[0]) << 56U) |
                         (static_cast<uint64_t>(p[1]) << 48U) |
                         (static_cast<uint64_t>(p[2]) << 40U) |
                         (static_cast<uint64_t>(p[3]) << 32U) |
                         (static_cast<uint64_t>(p[4]) << 24U) |
                         (static_cast<uint64_t>(p[5]) << 16U) |
                         (static_cast<uint64_t>(p[6]) << 8U) |
                         static_cast<uint64_t>(p[7]);
      return std::to_string(std::bit_cast<int64_t>(u));
    }
    if (param_oid == 23 && bytes.size() == 4) {  // int4
      ASSIGN_OR_RETURN(uint32_t, u, ReadUint32(bytes, 0));
      return std::to_string(std::bit_cast<int32_t>(u));
    }
    if (param_oid == 21 && bytes.size() == 2) {  // int2
      ASSIGN_OR_RETURN(uint16_t, u, ReadUint16(bytes, 0));
      return std::to_string(std::bit_cast<int16_t>(u));
    }
    if (param_oid == 701 && bytes.size() == 8) {  // float8
      const auto* p = reinterpret_cast<const unsigned char*>(bytes.data());
      const uint64_t u = (static_cast<uint64_t>(p[0]) << 56U) |
                         (static_cast<uint64_t>(p[1]) << 48U) |
                         (static_cast<uint64_t>(p[2]) << 40U) |
                         (static_cast<uint64_t>(p[3]) << 32U) |
                         (static_cast<uint64_t>(p[4]) << 24U) |
                         (static_cast<uint64_t>(p[5]) << 16U) |
                         (static_cast<uint64_t>(p[6]) << 8U) |
                         static_cast<uint64_t>(p[7]);
      const auto d = std::bit_cast<double>(u);
      if (std::isnan(d)) {
        return std::string("CAST('NaN' AS FLOAT64)");
      }
      if (d == std::numeric_limits<double>::infinity()) {
        return std::string("CAST('Infinity' AS FLOAT64)");
      }
      if (d == -std::numeric_limits<double>::infinity()) {
        return std::string("CAST('-Infinity' AS FLOAT64)");
      }
      return FormatDoubleShortest(d);
    }
    if (param_oid == 1082 && bytes.size() == 4) {  // date
      ASSIGN_OR_RETURN(uint32_t, u, ReadUint32(bytes, 0));
      const int64_t unix_days =
          static_cast<int64_t>(std::bit_cast<int32_t>(u)) +
          kPgDateEpochOffsetDays;
      return "DATE '" + Value::DateFromDays(unix_days).AsString() + "'";
    }
    if (param_oid == 16 && bytes.size() == 1) {  // bool
      return bytes[0] != 0 ? std::string("TRUE") : std::string("FALSE");
    }
  }
  // Numeric text OIDs render unquoted when syntactically valid.
  if (format_code == 0 &&
      (param_oid == 20 || param_oid == 21 || param_oid == 23)) {
    int64_t parsed = 0;
    const auto res =
        std::from_chars(bytes.data(), bytes.data() + bytes.size(), parsed);
    if (res.ec == std::errc{} && res.ptr == bytes.data() + bytes.size()) {
      return bytes;
    }
  }
  if (format_code == 0 && (param_oid == 700 || param_oid == 701)) {
    double parsed = 0.0;
    const auto res =
        std::from_chars(bytes.data(), bytes.data() + bytes.size(), parsed);
    if (res.ec == std::errc{} && res.ptr == bytes.data() + bytes.size() &&
        std::isfinite(parsed)) {
      return bytes;
    }
  }
  // Default: single-quoted SQL string literal (escaping quotes and backslashes
  // because GoogleSQL interprets backslash escapes).
  std::string out;
  out.reserve(bytes.size() + 2);
  out.push_back('\'');
  for (const char ch : bytes) {
    if (ch == '\'' || ch == '\\') {
      out.push_back('\\');
    }
    out.push_back(ch);
  }
  out.push_back('\'');
  return out;
}

void AppendBinaryValue(std::string* wire_out, const Value& value) {
  if (value.IsNull()) {
    AppendUint32(wire_out, std::numeric_limits<uint32_t>::max());
    return;
  }
  switch (value.type) {
    case ValueType::kInt64:
      AppendUint32(wire_out, 8);
      AppendUint64(wire_out, std::bit_cast<uint64_t>(value.value.int_value));
      return;
    case ValueType::kDouble:
      AppendUint32(wire_out, 8);
      AppendUint64(wire_out, std::bit_cast<uint64_t>(value.value.double_value));
      return;
    case ValueType::kDate: {
      const auto pg_days =
          static_cast<int32_t>(value.DateDays() - kPgDateEpochOffsetDays);
      AppendUint32(wire_out, 4);
      AppendUint32(wire_out, std::bit_cast<uint32_t>(pg_days));
      return;
    }
    case ValueType::kVarChar: {
      const std::string_view sv = value.value.varchar_value;
      AppendUint32(wire_out, static_cast<uint32_t>(sv.size()));
      wire_out->append(sv);
      return;
    }
    case ValueType::kNull:
    case ValueType::kArray: {
      const std::string text = value.AsString();
      AppendUint32(wire_out, static_cast<uint32_t>(text.size()));
      wire_out->append(text);
      return;
    }
  }
}

}  // namespace

StatusOr<ParseMessage> ParseParseMessage(std::string_view payload) {
  size_t cursor = 0;
  ASSIGN_OR_RETURN(std::string, stmt_name, ReadCString(payload, &cursor));
  ASSIGN_OR_RETURN(std::string, query, ReadCString(payload, &cursor));
  ASSIGN_OR_RETURN(uint16_t, param_count, ReadUint16(payload, cursor));
  cursor += 2;
  if (payload.size() - cursor != static_cast<size_t>(param_count) * 4) {
    return StatusError(StatusCode::kInvalidArgument,
                       "malformed PostgreSQL Parse message length");
  }
  ParseMessage msg;
  msg.statement_name = std::move(stmt_name);
  msg.query = std::move(query);
  msg.param_types.reserve(param_count);
  for (uint16_t i = 0; i < param_count; ++i) {
    ASSIGN_OR_RETURN(uint32_t, oid, ReadUint32(payload, cursor));
    cursor += 4;
    msg.param_types.push_back(oid);
  }
  return msg;
}

StatusOr<BindMessage> ParseBindMessage(std::string_view payload) {
  size_t cursor = 0;
  ASSIGN_OR_RETURN(std::string, portal_name, ReadCString(payload, &cursor));
  ASSIGN_OR_RETURN(std::string, stmt_name, ReadCString(payload, &cursor));
  ASSIGN_OR_RETURN(uint16_t, param_fmt_count, ReadUint16(payload, cursor));
  cursor += 2;
  if (payload.size() - cursor < static_cast<size_t>(param_fmt_count) * 2) {
    return StatusError(StatusCode::kInvalidArgument,
                       "truncated parameter formats in PostgreSQL Bind");
  }
  BindMessage msg;
  msg.portal_name = std::move(portal_name);
  msg.statement_name = std::move(stmt_name);
  msg.param_formats.reserve(param_fmt_count);
  for (uint16_t i = 0; i < param_fmt_count; ++i) {
    ASSIGN_OR_RETURN(uint16_t, fmt, ReadUint16(payload, cursor));
    cursor += 2;
    if (fmt != 0 && fmt != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "unsupported parameter format code in Bind");
    }
    msg.param_formats.push_back(fmt);
  }
  ASSIGN_OR_RETURN(uint16_t, param_count, ReadUint16(payload, cursor));
  cursor += 2;
  if (param_fmt_count > 1 && param_fmt_count != param_count) {
    return StatusError(StatusCode::kInvalidArgument,
                       "Bind parameter format count mismatch");
  }
  msg.params.reserve(param_count);
  for (uint16_t i = 0; i < param_count; ++i) {
    ASSIGN_OR_RETURN(uint32_t, raw_len, ReadUint32(payload, cursor));
    cursor += 4;
    const auto len = std::bit_cast<int32_t>(raw_len);
    if (len == -1) {
      msg.params.emplace_back(std::nullopt);
      continue;
    }
    if (len < 0 || payload.size() - cursor < static_cast<size_t>(len)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "truncated parameter value in PostgreSQL Bind");
    }
    msg.params.emplace_back(
        std::string(payload.substr(cursor, static_cast<size_t>(len))));
    cursor += static_cast<size_t>(len);
  }
  ASSIGN_OR_RETURN(uint16_t, result_fmt_count, ReadUint16(payload, cursor));
  cursor += 2;
  if (payload.size() - cursor != static_cast<size_t>(result_fmt_count) * 2) {
    return StatusError(StatusCode::kInvalidArgument,
                       "malformed result format codes in PostgreSQL Bind");
  }
  msg.result_formats.reserve(result_fmt_count);
  for (uint16_t i = 0; i < result_fmt_count; ++i) {
    ASSIGN_OR_RETURN(uint16_t, fmt, ReadUint16(payload, cursor));
    cursor += 2;
    if (fmt != 0 && fmt != 1) {
      return StatusError(StatusCode::kInvalidArgument,
                         "unsupported result format code in Bind");
    }
    msg.result_formats.push_back(fmt);
  }
  return msg;
}

StatusOr<DescribeMessage> ParseDescribeMessage(std::string_view payload) {
  if (payload.size() < 2) {
    return StatusError(StatusCode::kInvalidArgument,
                       "truncated PostgreSQL Describe message");
  }
  const char target = payload[0];
  if (target != 'S' && target != 'P') {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid PostgreSQL Describe target");
  }
  size_t cursor = 1;
  ASSIGN_OR_RETURN(std::string, name, ReadCString(payload, &cursor));
  if (cursor != payload.size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "trailing bytes in PostgreSQL Describe message");
  }
  return DescribeMessage{.target = target, .name = std::move(name)};
}

StatusOr<ExecuteMessage> ParseExecuteMessage(std::string_view payload) {
  size_t cursor = 0;
  ASSIGN_OR_RETURN(std::string, portal_name, ReadCString(payload, &cursor));
  if (payload.size() - cursor != 4) {
    return StatusError(StatusCode::kInvalidArgument,
                       "malformed PostgreSQL Execute message");
  }
  ASSIGN_OR_RETURN(uint32_t, max_rows, ReadUint32(payload, cursor));
  return ExecuteMessage{.portal_name = std::move(portal_name),
                        .max_rows = max_rows};
}

StatusOr<CloseMessage> ParseCloseMessage(std::string_view payload) {
  if (payload.size() < 2) {
    return StatusError(StatusCode::kInvalidArgument,
                       "truncated PostgreSQL Close message");
  }
  const char target = payload[0];
  if (target != 'S' && target != 'P') {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid PostgreSQL Close target");
  }
  size_t cursor = 1;
  ASSIGN_OR_RETURN(std::string, name, ReadCString(payload, &cursor));
  if (cursor != payload.size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "trailing bytes in PostgreSQL Close message");
  }
  return CloseMessage{.target = target, .name = std::move(name)};
}

StatusOr<std::string> BindSqlParameters(
    std::string_view sql, const std::vector<uint32_t>& param_types,
    const std::vector<uint16_t>& param_formats,
    const std::vector<std::optional<std::string>>& params) {
  if (params.empty()) {
    return std::string(sql);
  }
  std::vector<std::string> rendered;
  rendered.reserve(params.size());
  for (size_t i = 0; i < params.size(); ++i) {
    const uint32_t oid = i < param_types.size() ? param_types[i] : 0;
    const uint16_t fmt = ResolveColumnFormat(param_formats, i);
    ASSIGN_OR_RETURN(std::string, lit,
                     RenderParamAsSqlLiteral(oid, fmt, params[i]));
    rendered.push_back(std::move(lit));
  }
  std::string out;
  out.reserve(sql.size() + (params.size() * 8));
  bool single_quote = false;
  bool double_quote = false;
  bool line_comment = false;
  bool block_comment = false;
  for (size_t i = 0; i < sql.size(); ++i) {
    const char ch = sql[i];
    if (line_comment) {
      out.push_back(ch);
      if (ch == '\n') {
        line_comment = false;
      }
      continue;
    }
    if (block_comment) {
      out.push_back(ch);
      if (ch == '*' && i + 1 < sql.size() && sql[i + 1] == '/') {
        out.push_back('/');
        ++i;
        block_comment = false;
      }
      continue;
    }
    if (single_quote) {
      out.push_back(ch);
      if (ch == '\\' && i + 1 < sql.size()) {
        out.push_back(sql[++i]);
      } else if (ch == '\'') {
        single_quote = false;
      }
      continue;
    }
    if (double_quote) {
      out.push_back(ch);
      if (ch == '"') {
        double_quote = false;
      }
      continue;
    }
    if (ch == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
      out.append("--");
      ++i;
      line_comment = true;
      continue;
    }
    if (ch == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
      out.append("/*");
      ++i;
      block_comment = true;
      continue;
    }
    if (ch == '\'') {
      out.push_back(ch);
      single_quote = true;
      continue;
    }
    if (ch == '"') {
      out.push_back(ch);
      double_quote = true;
      continue;
    }
    if (ch == '$' && i + 1 < sql.size() &&
        std::isdigit(static_cast<unsigned char>(sql[i + 1])) != 0) {
      size_t j = i + 1;
      size_t idx = 0;
      while (j < sql.size() &&
             std::isdigit(static_cast<unsigned char>(sql[j])) != 0) {
        idx = (idx * 10) + static_cast<size_t>(sql[j] - '0');
        ++j;
      }
      if (idx == 0 || idx > rendered.size()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "bind parameter index out of range");
      }
      out.append(rendered[idx - 1]);
      i = j - 1;
      continue;
    }
    out.push_back(ch);
  }
  return out;
}

std::optional<StartupPacket> ParseStartupPacket(std::string_view packet,
                                                std::string* error) {
  const StatusOr<uint32_t> declared = ReadUint32(packet, 0);
  if (packet.size() < 8 || !declared.HasValue() ||
      declared.Value() != packet.size()) {
    *error = "malformed PostgreSQL startup packet";
    return std::nullopt;
  }
  StartupPacket result;
  const StatusOr<uint32_t> version = ReadUint32(packet, 4);
  if (!version.HasValue()) {
    *error = "malformed PostgreSQL startup packet";
    return std::nullopt;
  }
  result.protocol_version = version.Value();
  size_t cursor = 8;
  while (cursor < packet.size()) {
    const size_t name_end = packet.find('\0', cursor);
    if (name_end == std::string_view::npos) {
      *error = "unterminated startup parameter name";
      return std::nullopt;
    }
    if (name_end == cursor) {
      if (name_end + 1 != packet.size()) {
        *error = "data after startup packet terminator";
        return std::nullopt;
      }
      return result;
    }
    const size_t value_begin = name_end + 1;
    const size_t value_end = packet.find('\0', value_begin);
    if (value_end == std::string_view::npos) {
      *error = "unterminated startup parameter value";
      return std::nullopt;
    }
    result.parameters.insert_or_assign(
        std::string(packet.substr(cursor, name_end - cursor)),
        std::string(packet.substr(value_begin, value_end - value_begin)));
    cursor = value_end + 1;
  }
  *error = "startup packet has no terminator";
  return std::nullopt;
}

void DiscardQueuedReply(std::string& output, size_t sent_prefix,
                        size_t earlier_pending_bytes) {
  // Guard the precondition (sent_prefix <= output.size()): a miscomputed
  // prefix must not wrap `output.size() - sent_prefix` through size_t and
  // request a ~2^64 resize, which aborts the whole process. Clamping keeps
  // the caller's earlier-bytes intact instead.
  if (sent_prefix > output.size()) {  // NOLINT(readability-use-std-min-max)
    sent_prefix = output.size();
  }
  const size_t pending = output.size() - sent_prefix;
  output.resize(sent_prefix + std::min(pending, earlier_pending_bytes));
}

std::string AuthenticationOk() {
  std::string payload;
  AppendUint32(&payload, 0);
  return Message('R', payload);
}

std::string ParameterStatus(std::string_view name, std::string_view value) {
  std::string payload;
  AppendCString(&payload, name);
  AppendCString(&payload, value);
  return Message('S', payload);
}

void AppendParameterStatus(std::string* output, std::string_view name,
                           std::string_view value) {
  // Same wire bytes as ParameterStatus() but appended in place: batching
  // the startup parameter set avoids one temporary string per message.
  const size_t start = output->size();
  output->push_back('S');
  AppendUint32(output, 0);  // length placeholder, patched below
  AppendCString(output, name);
  AppendCString(output, value);
  const uint32_t length =  // NOLINT(modernize-use-auto)
      static_cast<uint32_t>(output->size() - start - 1);  // = payload + 4
  (*output)[start + 1] = static_cast<char>((length >> 24U) & 0xffU);
  (*output)[start + 2] = static_cast<char>((length >> 16U) & 0xffU);
  (*output)[start + 3] = static_cast<char>((length >> 8U) & 0xffU);
  (*output)[start + 4] = static_cast<char>(length & 0xffU);
}

std::string BackendKeyData(uint32_t process_id, uint32_t secret_key) {
  std::string payload;
  AppendUint32(&payload, process_id);
  AppendUint32(&payload, secret_key);
  return Message('K', payload);
}

std::string ReadyForQuery(char transaction_status) {
  return Message('Z', std::string(1, transaction_status));
}

std::string ErrorResponse(std::string_view message, std::string_view sqlstate,
                          std::string_view severity,
                          std::optional<size_t> position) {
  std::string payload;
  payload.push_back('S');
  AppendCString(&payload, severity);
  payload.push_back('V');
  AppendCString(&payload, severity);
  payload.push_back('C');
  AppendCString(&payload, SanitizeField(sqlstate));
  payload.push_back('M');
  AppendCString(&payload, SanitizeField(message));
  if (position.has_value()) {
    payload.push_back('P');
    AppendCString(&payload, std::to_string(*position));
  }
  payload.push_back('\0');
  return Message('E', payload);
}

std::optional<size_t> SqlErrorPosition(std::string_view message,
                                       const SqlStatement& statement,
                                       size_t parsed_byte_offset) {
  const std::string_view diagnostic = message.substr(0, message.find('\n'));
  const size_t at = diagnostic.rfind("[at ");
  // The "[at L:C]" tag is matched by content, not by an "ERROR:" prefix or a
  // terminal ']' — diagnostics from non-parse sources and future formats
  // with trailing hint text must keep their position.
  if (at == std::string_view::npos ||
      parsed_byte_offset > statement.text.size()) {
    return std::nullopt;
  }
  const size_t close = diagnostic.find(']', at + 4);
  if (close == std::string_view::npos) {
    return std::nullopt;
  }
  const auto location = diagnostic.substr(at + 4, close - at - 4);
  const size_t colon = location.find(':');
  if (colon == std::string_view::npos) {
    return std::nullopt;
  }
  const char* const split = location.data() + colon;
  const char* const end = location.data() + location.size();
  size_t line = 0;
  size_t column = 0;
  // from_chars receives the explicit end pointer.
  // NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
  const auto parsed_line = std::from_chars(location.data(), split, line);
  const auto parsed_col = std::from_chars(split + 1, end, column);
  if (parsed_line.ptr != split || parsed_line.ec != std::errc{} || line == 0 ||
      parsed_col.ptr != end || parsed_col.ec != std::errc{} || column == 0) {
    return std::nullopt;
  }

  const std::string_view sql = statement.text;
  if (!IsValidUtf8Text(sql)) {
    return std::nullopt;
  }
  size_t offset = parsed_byte_offset;
  for (size_t row = 1; row < line; ++row) {
    offset = sql.find_first_of("\r\n", offset);
    if (offset == std::string_view::npos) {
      return std::nullopt;
    }
    offset += sql.substr(offset, 2) == "\r\n" ? 2 : 1;
  }
  size_t display_column = 1;
  while (display_column < column && offset < sql.size() &&
         sql[offset] != '\n' && sql[offset] != '\r') {
    if (sql[offset] == '\t') {
      // GoogleSQL reports columns after expansion to eight-column tab stops.
      display_column += 8 - ((display_column - 1) % 8);
      ++offset;
    } else {
      offset += Utf8SequenceLength(static_cast<unsigned char>(sql[offset]));
      ++display_column;
    }
  }
  if (display_column != column) {
    return std::nullopt;
  }
  return statement.position + Utf8CharacterCount(sql.substr(0, offset));
}

std::string NoticeResponse(std::string_view message, std::string_view sqlstate,
                           std::string_view severity) {
  std::string payload;
  payload.push_back('S');
  AppendCString(&payload, severity);
  payload.push_back('V');
  AppendCString(&payload, severity);
  payload.push_back('C');
  AppendCString(&payload, SanitizeField(sqlstate));
  payload.push_back('M');
  AppendCString(&payload, SanitizeField(message));
  payload.push_back('\0');
  return Message('N', payload);
}

std::string EmptyQueryResponse() { return Message('I', {}); }
std::string ParseComplete() { return Message('1', {}); }
std::string BindComplete() { return Message('2', {}); }
std::string CloseComplete() { return Message('3', {}); }
std::string NoData() { return Message('n', {}); }
std::string PortalSuspended() { return Message('s', {}); }

std::string ParameterDescription(const std::vector<uint32_t>& param_oids) {
  std::string payload;
  payload.reserve(2 + (param_oids.size() * 4));
  AppendUint16(&payload, static_cast<uint16_t>(param_oids.size()));
  for (const uint32_t oid : param_oids) {
    AppendUint32(&payload, oid);
  }
  return Message('t', payload);
}

std::string RowDescription(const std::vector<ColumnDescription>& columns,
                           const std::vector<uint16_t>& result_formats) {
  if (columns.size() > std::numeric_limits<uint16_t>::max()) {
    // The wire format declares the column count as uint16; truncating would
    // desynchronize the protocol stream.
    CHECK_MSG(false, "too many columns for PostgreSQL protocol");
  }
  std::string payload;
  AppendUint16(&payload, static_cast<uint16_t>(columns.size()));
  for (size_t i = 0; i < columns.size(); ++i) {
    const ColumnDescription& column = columns[i];
    const std::string safe_name =
        column.name.find('\0') == std::string::npos && !column.name.empty()
            ? column.name
            : "?column?";
    AppendCString(&payload, safe_name);
    AppendUint32(&payload, 0);  // Not tied to a pg_catalog table OID.
    AppendUint16(&payload, 0);
    const PgType type = ToPgType(column.type);
    AppendUint32(&payload, type.oid);
    AppendUint16(&payload, std::bit_cast<uint16_t>(type.length));
    AppendUint32(&payload, std::numeric_limits<uint32_t>::max());
    AppendUint16(&payload, ResolveColumnFormat(result_formats, i));
  }
  return Message('T', payload);
}

std::string DataRow(const Row& row,
                    const std::vector<uint16_t>& result_formats) {
  CHECK_MSG(row.values_.size() <= std::numeric_limits<uint16_t>::max(),
            "too many values for PostgreSQL protocol");
  std::string payload;
  AppendUint16(&payload, static_cast<uint16_t>(row.values_.size()));
  for (size_t i = 0; i < row.values_.size(); ++i) {
    const Value& value = row.values_[i];
    if (value.IsNull()) {
      AppendUint32(&payload, std::numeric_limits<uint32_t>::max());
      continue;
    }
    if (ResolveColumnFormat(result_formats, i) == 1) {
      AppendBinaryValue(&payload, value);
      continue;
    }
    // PostgreSQL text values cannot contain NUL: clients read PQgetvalue as a
    // NUL-terminated C string, so an embedded NUL would silently truncate the
    // cell. Match ErrorResponse's SanitizeField policy (NUL -> space).
    const std::string text = SanitizeField(ValueText(value));
    CHECK_MSG(text.size() <= std::numeric_limits<uint32_t>::max(),
              "PostgreSQL DataRow cell exceeds uint32 length");
    AppendUint32(&payload, static_cast<uint32_t>(text.size()));
    payload.append(text);
  }
  return Message('D', payload);
}

// Lean proof:
// lean/TinylambOpt/ResearchProofs/ColumnarToPgWireChunkedFramingEquiv.lean
// (sound)
Status AppendDataRowFromChunk(std::string* wire_out, const DataChunk& chunk,
                              size_t row_idx,
                              const std::vector<uint16_t>& result_formats) {
  if (wire_out == nullptr || row_idx >= chunk.Size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid DataChunk row index for PostgreSQL DataRow");
  }
  const size_t col_count = chunk.ColumnCount();
  if (col_count > std::numeric_limits<uint16_t>::max()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "too many values for PostgreSQL protocol");
  }
  if (result_formats.size() > 1 && result_formats.size() != col_count) {
    return StatusError(StatusCode::kInvalidArgument,
                       "result format count does not match column count");
  }
  for (size_t c = 0; c < col_count; ++c) {
    const ColumnVector& col = chunk.ColumnAt(c);
    if (!col.IsNull(row_idx) && !col.IsHeterogeneous() &&
        col.Type() == ValueType::kVarChar) {
      if (col.StringData()[row_idx].size() >
          static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
        return StatusError(StatusCode::kTooBigData,
                           "PostgreSQL DataRow cell exceeds uint32 length");
      }
    }
  }
  const size_t frame_start = wire_out->size();
  wire_out->push_back('D');
  AppendUint32(wire_out, 0);  // Patched below with final frame length.
  AppendUint16(wire_out, static_cast<uint16_t>(col_count));
  std::array<char, 64> num_buf{};
  for (size_t c = 0; c < col_count; ++c) {
    const ColumnVector& col = chunk.ColumnAt(c);
    if (col.IsNull(row_idx)) {
      AppendUint32(wire_out, std::numeric_limits<uint32_t>::max());
      continue;
    }
    const uint16_t fmt = ResolveColumnFormat(result_formats, c);
    if (fmt == 1) {
      if (!col.IsHeterogeneous()) {
        switch (col.Type()) {
          case ValueType::kInt64: {
            std::array<char, 12> cell{};
            WriteBe32(cell.data(), 8);
            WriteBe64(cell.data() + 4,
                      std::bit_cast<uint64_t>(col.IntegerData()[row_idx]));
            wire_out->append(cell.data(), cell.size());
            continue;
          }
          case ValueType::kDouble: {
            std::array<char, 12> cell{};
            WriteBe32(cell.data(), 8);
            WriteBe64(cell.data() + 4,
                      std::bit_cast<uint64_t>(col.DoubleData()[row_idx]));
            wire_out->append(cell.data(), cell.size());
            continue;
          }
          case ValueType::kDate: {
            const auto pg_days = static_cast<int32_t>(
                col.IntegerData()[row_idx] - kPgDateEpochOffsetDays);
            std::array<char, 8> cell{};
            WriteBe32(cell.data(), 4);
            WriteBe32(cell.data() + 4, std::bit_cast<uint32_t>(pg_days));
            wire_out->append(cell.data(), cell.size());
            continue;
          }
          case ValueType::kVarChar: {
            const std::string& s = col.StringData()[row_idx];
            AppendUint32(wire_out, static_cast<uint32_t>(s.size()));
            wire_out->append(s);
            continue;
          }
          default:
            break;
        }
      }
      AppendBinaryValue(wire_out, col.ValueAt(row_idx));
      continue;
    }
    if (!col.IsHeterogeneous()) {
      switch (col.Type()) {
        case ValueType::kInt64: {
          // Lean proof:
          // lean/TinylambOpt/ResearchProofs/AnalyticsWorkloads/PhysicalChoices.lean
          // (FieldBytes_sound)
          // Pre-encoded length-prefixed wire bytes for hot integers in
          // [-1024, 65535], preserving exact DataRow framing bytes.
          struct HotIntWireEntry {
            std::array<char, 15> bytes{};
            uint8_t len{0};
          };
          static constexpr int64_t kHotIntMin = -1024;
          static constexpr int64_t kHotIntMax = 65535;
          static const std::vector<HotIntWireEntry> kHotIntTable = [] {
            std::vector<HotIntWireEntry> table(
                static_cast<size_t>(kHotIntMax - kHotIntMin + 1));
            std::array<char, 32> tmp{};
            for (int64_t v = kHotIntMin; v <= kHotIntMax; ++v) {
              const auto r =
                  std::to_chars(tmp.data(), tmp.data() + tmp.size(), v);
              const auto dlen = static_cast<uint32_t>(r.ptr - tmp.data());
              HotIntWireEntry& e = table[static_cast<size_t>(v - kHotIntMin)];
              e.bytes[0] = static_cast<char>((dlen >> 24U) & 0xffU);
              e.bytes[1] = static_cast<char>((dlen >> 16U) & 0xffU);
              e.bytes[2] = static_cast<char>((dlen >> 8U) & 0xffU);
              e.bytes[3] = static_cast<char>(dlen & 0xffU);
              std::memcpy(e.bytes.data() + 4, tmp.data(), dlen);
              e.len = static_cast<uint8_t>(4U + dlen);
            }
            return table;
          }();
          const int64_t raw_v = col.IntegerData()[row_idx];
          if (!col.IsUnsigned()) {
            if (raw_v >= kHotIntMin && raw_v <= kHotIntMax) {
              const HotIntWireEntry& e =
                  kHotIntTable[static_cast<size_t>(raw_v - kHotIntMin)];
              wire_out->append(e.bytes.data(), e.len);
              continue;
            }
            const auto res = std::to_chars(
                num_buf.data(), num_buf.data() + num_buf.size(), raw_v);
            const auto len = static_cast<uint32_t>(res.ptr - num_buf.data());
            AppendUint32(wire_out, len);
            wire_out->append(num_buf.data(), len);
            continue;
          }
          const auto uv = static_cast<uint64_t>(raw_v);
          if (uv <= static_cast<uint64_t>(kHotIntMax)) {
            const HotIntWireEntry& e = kHotIntTable[static_cast<size_t>(
                static_cast<int64_t>(uv) - kHotIntMin)];
            wire_out->append(e.bytes.data(), e.len);
            continue;
          }
          const auto res = std::to_chars(num_buf.data(),
                                         num_buf.data() + num_buf.size(), uv);
          const auto len = static_cast<uint32_t>(res.ptr - num_buf.data());
          AppendUint32(wire_out, len);
          wire_out->append(num_buf.data(), len);
          continue;
        }
        case ValueType::kDouble: {
          const double d = col.DoubleData()[row_idx];
          if (std::isnan(d)) {
            AppendUint32(wire_out, 3);
            wire_out->append("NaN", 3);
            continue;
          }
          if (d == std::numeric_limits<double>::infinity()) {
            AppendUint32(wire_out, 8);
            wire_out->append("Infinity", 8);
            continue;
          }
          if (d == -std::numeric_limits<double>::infinity()) {
            AppendUint32(wire_out, 9);
            wire_out->append("-Infinity", 9);
            continue;
          }
          const auto res =
              std::to_chars(num_buf.data(), num_buf.data() + num_buf.size(), d);
          if (res.ec == std::errc{}) {
            const auto len = static_cast<uint32_t>(res.ptr - num_buf.data());
            AppendUint32(wire_out, len);
            wire_out->append(num_buf.data(), len);
            continue;
          }
          break;
        }
        case ValueType::kVarChar: {
          const std::string& s = col.StringData()[row_idx];
          AppendUint32(wire_out, static_cast<uint32_t>(s.size()));
          if (s.find('\0') == std::string::npos) {
            wire_out->append(s);
          } else {
            const size_t pos = wire_out->size();
            wire_out->append(s);
            std::replace(wire_out->begin() + static_cast<std::ptrdiff_t>(pos),
                         wire_out->end(), '\0', ' ');
          }
          continue;
        }
        default:
          break;
      }
    }
    const std::string text = SanitizeField(ValueText(col.ValueAt(row_idx)));
    if (text.size() >
        static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
      wire_out->resize(frame_start);
      return StatusError(StatusCode::kTooBigData,
                         "PostgreSQL DataRow cell exceeds uint32 length");
    }
    AppendUint32(wire_out, static_cast<uint32_t>(text.size()));
    wire_out->append(text);
  }
  const size_t msg_len = wire_out->size() - (frame_start + 1);
  if (msg_len > static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
    wire_out->resize(frame_start);
    return StatusError(StatusCode::kTooBigData,
                       "PostgreSQL DataRow frame exceeds uint32 length");
  }
  const auto len32 = static_cast<uint32_t>(msg_len);
  (*wire_out)[frame_start + 1] = static_cast<char>((len32 >> 24U) & 0xffU);
  (*wire_out)[frame_start + 2] = static_cast<char>((len32 >> 16U) & 0xffU);
  (*wire_out)[frame_start + 3] = static_cast<char>((len32 >> 8U) & 0xffU);
  (*wire_out)[frame_start + 4] = static_cast<char>(len32 & 0xffU);
  return Status::kSuccess;
}

Status AppendDataRowsFromChunk(std::string* wire_out, const DataChunk& chunk,
                               size_t row_begin, size_t row_end,
                               size_t max_bytes,
                               const std::vector<uint16_t>& result_formats,
                               size_t* rows_written) {
  if (wire_out == nullptr || rows_written == nullptr || row_begin > row_end ||
      row_end > chunk.Size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid DataChunk row range for PostgreSQL DataRow");
  }
  *rows_written = 0;
  if (row_begin == row_end) {
    return Status::kSuccess;
  }
  const size_t col_count = chunk.ColumnCount();
  if (col_count > std::numeric_limits<uint16_t>::max()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "too many values for PostgreSQL protocol");
  }
  if (result_formats.size() > 1 && result_formats.size() != col_count) {
    return StatusError(StatusCode::kInvalidArgument,
                       "result format count does not match column count");
  }

  // Specialized zero-allocation fast path: all columns binary + fixed-width
  // scalar (int8 / float8 / date) + no NULLs in the chunk.
  bool all_fixed_binary = (col_count > 0);
  size_t fixed_row_bytes = 7;  // 'D' + uint32 len + uint16 col_count.
  for (size_t c = 0; c < col_count && all_fixed_binary; ++c) {
    if (ResolveColumnFormat(result_formats, c) != 1) {
      all_fixed_binary = false;
      break;
    }
    const ColumnVector& col = chunk.ColumnAt(c);
    if (col.IsHeterogeneous()) {
      all_fixed_binary = false;
      break;
    }
    for (const uint64_t word : col.NullBitmap()) {
      if (word != 0) {
        all_fixed_binary = false;
        break;
      }
    }
    if (!all_fixed_binary) {
      break;
    }
    switch (col.Type()) {
      case ValueType::kInt64:
      case ValueType::kDouble:
        fixed_row_bytes += 12;  // uint32(8) + 8 bytes.
        break;
      case ValueType::kDate:
        fixed_row_bytes += 8;  // uint32(4) + 4 bytes.
        break;
      default:
        all_fixed_binary = false;
        break;
    }
  }
  if (all_fixed_binary) {
    const size_t avail = row_end - row_begin;
    size_t count = avail;
    if (max_bytes > 0) {
      const size_t rem_bytes =
          wire_out->size() < max_bytes ? (max_bytes - wire_out->size()) : 0;
      const size_t budget_rows = std::max<size_t>(
          1, (rem_bytes + fixed_row_bytes - 1) / fixed_row_bytes);
      count = std::min(avail, budget_rows);
    }
    const size_t base = wire_out->size();
    wire_out->resize(base + (count * fixed_row_bytes));
    char* dst = wire_out->data() + base;
    const auto frame_len = static_cast<uint32_t>(fixed_row_bytes - 1);
    const auto ncols = static_cast<uint16_t>(col_count);
    for (size_t r = row_begin; r < row_begin + count; ++r) {
      *dst++ = 'D';
      WriteBe32(dst, frame_len);
      dst += 4;
      WriteBe16(dst, ncols);
      dst += 2;
      for (size_t c = 0; c < col_count; ++c) {
        const ColumnVector& col = chunk.ColumnAt(c);
        switch (col.Type()) {
          case ValueType::kInt64:
            WriteBe32(dst, 8);
            WriteBe64(dst + 4, std::bit_cast<uint64_t>(col.IntegerData()[r]));
            dst += 12;
            break;
          case ValueType::kDouble:
            WriteBe32(dst, 8);
            WriteBe64(dst + 4, std::bit_cast<uint64_t>(col.DoubleData()[r]));
            dst += 12;
            break;
          case ValueType::kDate: {
            const auto pg_days = static_cast<int32_t>(col.IntegerData()[r] -
                                                      kPgDateEpochOffsetDays);
            WriteBe32(dst, 4);
            WriteBe32(dst + 4, std::bit_cast<uint32_t>(pg_days));
            dst += 8;
            break;
          }
          default:
            break;
        }
      }
    }
    *rows_written = count;
    return Status::kSuccess;
  }

  for (size_t r = row_begin; r < row_end; ++r) {
    if (*rows_written > 0 && max_bytes > 0 && wire_out->size() >= max_bytes) {
      break;
    }
    RETURN_IF_FAIL(AppendDataRowFromChunk(wire_out, chunk, r, result_formats));
    ++(*rows_written);
  }
  return Status::kSuccess;
}

std::string CommandComplete(std::string_view tag) {
  std::string payload;
  AppendCString(&payload, tag);
  return Message('C', payload);
}

std::string NegotiateProtocolVersion(
    uint32_t newest_minor,
    const std::vector<std::string>& unsupported_parameters) {
  std::string payload;
  AppendUint32(&payload, newest_minor);
  AppendUint32(&payload, static_cast<uint32_t>(unsupported_parameters.size()));
  for (const std::string& parameter : unsupported_parameters) {
    AppendCString(&payload, parameter);
  }
  return Message('v', payload);
}

StatusOr<std::vector<SqlStatement>> SplitSqlStatementsWithPositions(
    std::string_view sql) {
  std::vector<SqlStatement> statements;
  bool overflowed = false;
  std::string current;
  size_t segment_start = 0;
  size_t counted_bytes = 0;
  size_t characters = 0;
  // Length of `current` up to the last character that is neither whitespace
  // nor comment bytes. A trailing comment (or trailing whitespace/comment
  // run) after the final real token of a statement is dropped, so a client
  // sending "select 1; -- note" does not get a bogus second statement.
  size_t meaningful = 0;
  bool single_quote = false;
  bool double_quote = false;
  bool backtick_quote = false;
  bool dollar_quote = false;
  std::string dollar_tag;
  bool line_comment = false;
  bool block_comment = false;
  int block_comment_depth = 0;
  const auto Append = [&](char c) {
    current.push_back(c);
    if (!std::isspace(static_cast<unsigned char>(c))) {
      meaningful = current.size();
    }
  };
  const auto FlushCurrent = [&](size_t segment_end) {
    current.resize(meaningful);
    const std::string_view text = Trim(current);
    if (!text.empty()) {
      if (statements.size() >= kMaxSqlStatements) {
        overflowed = true;
      } else {
        const size_t start =
            segment_start + static_cast<size_t>(text.data() - current.data());
        characters += Utf8CharacterCount(
            sql.substr(counted_bytes, start - counted_bytes));
        counted_bytes = start;
        statements.push_back({std::string(text), characters + 1});
      }
    }
    current.clear();
    meaningful = 0;
    segment_start = segment_end + 1;
  };
  for (size_t i = 0; i < sql.size(); ++i) {
    const char current_char = sql[i];
    const char next = i + 1 < sql.size() ? sql[i + 1] : '\0';
    if (line_comment) {
      // Comment bytes are buffered verbatim but do not count as meaningful;
      // a trailing comment with no real content after it is dropped at
      // flush time (see `meaningful`).
      current.push_back(current_char);
      if (current_char == '\n' || current_char == '\r') {
        line_comment = false;
      }
      continue;
    }
    if (block_comment) {
      current.push_back(current_char);
      // PostgreSQL nests block comments: track depth so `/* outer /* inner */`
      // does not end the comment (and splice the rest of the message into a
      // bogus statement).
      if (current_char == '/' && next == '*') {
        ++block_comment_depth;
        ++i;
        current.push_back('*');
        continue;
      }
      if (current_char == '*' && next == '/') {
        --block_comment_depth;
        if (block_comment_depth <= 0) {
          block_comment = false;
        }
        ++i;
        current.push_back('/');
      }
      continue;
    }
    if (!single_quote && !double_quote && !backtick_quote && !dollar_quote) {
      if (current_char == '-' && next == '-') {
        line_comment = true;
        ++i;
        current.push_back(current_char);
        current.push_back('-');
        continue;
      }
      // GoogleSQL also accepts '#' as a line comment; the frontend skips it,
      // so the statement splitter must not treat a ';' inside it as a boundary.
      if (current_char == '#') {
        line_comment = true;
        current.push_back(current_char);
        continue;
      }
      if (current_char == '/' && next == '*') {
        block_comment = true;
        block_comment_depth = 1;
        ++i;
        current.push_back(current_char);
        current.push_back('*');
        continue;
      }
      if (current_char == '$') {
        // Dollar-quoted string ($tag$...$tag$, tag optional): a ';' inside
        // it must not split statements.  Scan the optional identifier tag
        // and require the closing '$' of the opener; otherwise the '$' is a
        // placeholder and stays ordinary text.
        size_t j = i + 1;
        // PostgreSQL dollar-quote tags follow unquoted-identifier rules:
        // the first character cannot be a digit ($1$ is a placeholder).
        if (j < sql.size() &&
            std::isdigit(static_cast<unsigned char>(sql[j])) != 0) {
          Append(current_char);
          continue;
        }
        while (j < sql.size() &&
               (std::isalnum(static_cast<unsigned char>(sql[j])) != 0 ||
                sql[j] == '_')) {
          ++j;
        }
        if (j < sql.size() && sql[j] == '$') {
          dollar_tag = std::string(sql.substr(i, j - i + 1));
          dollar_quote = true;
          for (const char tag_char : dollar_tag) {
            Append(tag_char);
          }
          i = j;
          continue;
        }
      }
    }
    if (dollar_quote) {
      if (sql.substr(i, dollar_tag.size()) == dollar_tag) {
        for (const char tag_char : dollar_tag) {
          Append(tag_char);
        }
        i += dollar_tag.size() - 1;
        dollar_quote = false;
        continue;
      }
      Append(current_char);
      continue;
    }
    // Backslash escapes the next character inside a quoted string
    // (the parser decodes \', \" and \\): swallowing the pair here keeps
    // `SELECT 'It\'s'; SELECT 2;` in two statements and prevents a `;`
    // inside a string from splitting statements.  GoogleSQL double quotes
    // are STRINGS too, so `"` must escape there as well -- restricting this
    // to single quotes left the splitter "inside" a string after `"a\""`
    // and silently merged every following statement into one parse error.
    if ((single_quote || double_quote || backtick_quote) &&
        current_char == '\\' && i + 1 < sql.size()) {
      Append(current_char);
      ++i;
      Append(sql[i]);
      continue;
    }
    if (!double_quote && !backtick_quote && current_char == '\'') {
      if (single_quote && next == '\'') {
        ++i;
        Append(current_char);
        Append(next);
        continue;
      }
      single_quote = !single_quote;
      Append(current_char);
      continue;
    }
    if (!single_quote && !backtick_quote && current_char == '"') {
      if (double_quote && next == '"') {
        ++i;
        Append(current_char);
        Append(next);
        continue;
      }
      double_quote = !double_quote;
      Append(current_char);
      continue;
    }
    if (!single_quote && !double_quote && current_char == '`') {
      backtick_quote = !backtick_quote;
      Append(current_char);
      continue;
    }
    if (current_char == ';' && !single_quote && !double_quote &&
        !backtick_quote) {
      FlushCurrent(i);
      continue;
    }
    Append(current_char);
  }
  FlushCurrent(sql.size());
  if (overflowed) {
    return StatusError(StatusCode::kInvalidArgument,
                       "too many SQL statements in one message (limit " +
                           std::to_string(kMaxSqlStatements) + ")");
  }
  return statements;
}

StatusOr<std::vector<std::string>> SplitSqlStatements(std::string_view sql) {
  ASSIGN_OR_RETURN(std::vector<SqlStatement>, with_positions,
                   SplitSqlStatementsWithPositions(sql));
  std::vector<std::string> statements;
  statements.reserve(with_positions.size());
  for (SqlStatement& statement : with_positions) {
    statements.push_back(std::move(statement.text));
  }
  return statements;
}

}  // namespace tinylamb::pgwire
