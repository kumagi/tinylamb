/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "server/postgres_protocol.hpp"

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <cctype>
#include <string>
#include <string_view>
#include "type/value.hpp"
#include "type/value_type.hpp"
#include <vector>
#include "type/row.hpp"

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
  std::string result(value);
  std::ranges::replace(result, '\0', ' ');
  return result;
}

std::string ValueText(const Value& value) {
  switch (value.type) {
    case ValueType::kNull:
      return {};
    case ValueType::kInt64:
      return std::to_string(value.value.int_value);
    case ValueType::kVarChar:
      return std::string(value.value.varchar_value);
    case ValueType::kDate:
      return value.AsString();
    case ValueType::kDouble: {
      std::ostringstream output;
      output << std::setprecision(std::numeric_limits<double>::max_digits10)
             << value.value.double_value;
      return output.str();
    }
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
      return {.oid=20, .length=8};  // int8
    case ValueType::kDouble:
      return {.oid=701, .length=8};  // float8
    case ValueType::kDate:
      return {.oid=1082, .length=4};  // date
    case ValueType::kNull:
    case ValueType::kVarChar:
      return {.oid=25, .length=-1};  // text
  }
  return {.oid=25, .length=-1};
}

std::string Trim(std::string_view value) {
  const size_t begin = value.find_first_not_of(" \t\r\n");
  if (begin == std::string_view::npos) { return {};
}
  const size_t end = value.find_last_not_of(" \t\r\n");
  return std::string(value.substr(begin, end - begin + 1));
}

}  // namespace

uint16_t ReadUint16(std::string_view bytes, size_t offset) {
  if (offset + sizeof(uint16_t) > bytes.size()) {
    throw std::out_of_range("short PostgreSQL protocol field");
  }
  const auto* data = reinterpret_cast<const unsigned char*>(bytes.data());
  return static_cast<uint16_t>((static_cast<uint16_t>(data[offset]) << 8U) |
                               static_cast<uint16_t>(data[offset + 1]));
}

uint32_t ReadUint32(std::string_view bytes, size_t offset) {
  if (offset + sizeof(uint32_t) > bytes.size()) {
    throw std::out_of_range("short PostgreSQL protocol field");
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

std::optional<StartupPacket> ParseStartupPacket(std::string_view packet,
                                                std::string* error) {
  if (packet.size() < 8 || ReadUint32(packet, 0) != packet.size()) {
    *error = "malformed PostgreSQL startup packet";
    return std::nullopt;
  }
  StartupPacket result;
  result.protocol_version = ReadUint32(packet, 4);
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

std::string BackendKeyData(uint32_t process_id, uint32_t secret_key) {
  std::string payload;
  AppendUint32(&payload, process_id);
  AppendUint32(&payload, secret_key);
  return Message('K', payload);
}

std::string ReadyForQuery(char transaction_status) {
  return Message('Z', std::string(1, transaction_status));
}

std::string ErrorResponse(std::string_view message, std::string_view sqlstate) {
  std::string payload;
  payload.push_back('S');
  AppendCString(&payload, "ERROR");
  payload.push_back('V');
  AppendCString(&payload, "ERROR");
  payload.push_back('C');
  AppendCString(&payload, SanitizeField(sqlstate));
  payload.push_back('M');
  AppendCString(&payload, SanitizeField(message));
  payload.push_back('\0');
  return Message('E', payload);
}

std::string EmptyQueryResponse() { return Message('I', {}); }

std::string RowDescription(const std::vector<ColumnDescription>& columns) {
  if (columns.size() > std::numeric_limits<uint16_t>::max()) {
    // The wire format declares the column count as uint16; truncating would
    // desynchronize the protocol stream.
    throw std::runtime_error("too many columns for PostgreSQL protocol");
  }
  std::string payload;
  AppendUint16(&payload, static_cast<uint16_t>(columns.size()));
  for (const ColumnDescription& column : columns) {
    AppendCString(&payload, column.name.empty() ? "?column?" : column.name);
    AppendUint32(&payload, 0);  // Not tied to a pg_catalog table OID.
    AppendUint16(&payload, 0);
    const PgType type = ToPgType(column.type);
    AppendUint32(&payload, type.oid);
    AppendUint16(&payload, std::bit_cast<uint16_t>(type.length));
    AppendUint32(&payload, std::numeric_limits<uint32_t>::max());
    AppendUint16(&payload, 0);  // Text format.
  }
  return Message('T', payload);
}

std::string DataRow(const Row& row) {
  if (row.values_.size() > std::numeric_limits<uint16_t>::max()) {
    throw std::runtime_error("too many values for PostgreSQL protocol");
  }
  std::string payload;
  AppendUint16(&payload, static_cast<uint16_t>(row.values_.size()));
  for (const Value& value : row.values_) {
    if (value.IsNull()) {
      AppendUint32(&payload, std::numeric_limits<uint32_t>::max());
      continue;
    }
    const std::string text = ValueText(value);
    AppendUint32(&payload, static_cast<uint32_t>(text.size()));
    payload.append(text);
  }
  return Message('D', payload);
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

std::vector<std::string> SplitSqlStatements(std::string_view sql) {
  std::vector<std::string> statements;
  std::string current;
  // Length of `current` up to the last character that is neither whitespace
  // nor comment bytes. A trailing comment (or trailing whitespace/comment
  // run) after the final real token of a statement is dropped, so a client
  // sending "select 1; -- note" does not get a bogus second statement.
  size_t meaningful = 0;
  bool single_quote = false;
  bool double_quote = false;
  bool backtick_quote = false;
  bool line_comment = false;
  bool block_comment = false;
  const auto Append = [&](char c) {
    current.push_back(c);
    if (!std::isspace(static_cast<unsigned char>(c))) {
      meaningful = current.size();
    }
  };
  const auto FlushCurrent = [&]() {
    current.resize(meaningful);
    current = Trim(current);
    if (!current.empty()) {
      statements.push_back(current);
    }
    current.clear();
    meaningful = 0;
  };
  for (size_t i = 0; i < sql.size(); ++i) {
    const char current_char = sql[i];
    const char next = i + 1 < sql.size() ? sql[i + 1] : '\0';
    if (line_comment) {
      // Comment bytes are buffered verbatim but do not count as meaningful;
      // a trailing comment with no real content after it is dropped at
      // flush time (see `meaningful`).
      current.push_back(current_char);
      if (current_char == '\n') {
        line_comment = false;
      }
      continue;
    }
    if (block_comment) {
      current.push_back(current_char);
      if (current_char == '*' && next == '/') {
        block_comment = false;
        ++i;
        current.push_back('/');
      }
      continue;
    }
    if (!single_quote && !double_quote && !backtick_quote) {
      if (current_char == '-' && next == '-') {
        line_comment = true;
        ++i;
        current.push_back(current_char);
        current.push_back('-');
        continue;
      }
      if (current_char == '/' && next == '*') {
        block_comment = true;
        ++i;
        current.push_back(current_char);
        current.push_back('*');
        continue;
      }
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
      FlushCurrent();
      continue;
    }
    Append(current_char);
  }
  FlushCurrent();
  return statements;
}

}  // namespace tinylamb::pgwire
