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

std::string RowDescription(const std::vector<ColumnDescription>& columns) {
  if (columns.size() > std::numeric_limits<uint16_t>::max()) {
    // The wire format declares the column count as uint16; truncating would
    // desynchronize the protocol stream.
    CHECK_MSG(false, "too many columns for PostgreSQL protocol");
  }
  std::string payload;
  AppendUint16(&payload, static_cast<uint16_t>(columns.size()));
  for (const ColumnDescription& column : columns) {
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
    AppendUint16(&payload, 0);  // Text format.
  }
  return Message('T', payload);
}

std::string DataRow(const Row& row) {
  CHECK_MSG(row.values_.size() <= std::numeric_limits<uint16_t>::max(),
            "too many values for PostgreSQL protocol");
  std::string payload;
  AppendUint16(&payload, static_cast<uint16_t>(row.values_.size()));
  for (const Value& value : row.values_) {
    if (value.IsNull()) {
      AppendUint32(&payload, std::numeric_limits<uint32_t>::max());
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
                              size_t row_idx) {
  if (wire_out == nullptr || row_idx >= chunk.Size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid DataChunk row index for PostgreSQL DataRow");
  }
  const size_t col_count = chunk.ColumnCount();
  if (col_count > std::numeric_limits<uint16_t>::max()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "too many values for PostgreSQL protocol");
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
  std::array<char, 32> int_buf{};
  for (size_t c = 0; c < col_count; ++c) {
    const ColumnVector& col = chunk.ColumnAt(c);
    if (col.IsNull(row_idx)) {
      AppendUint32(wire_out, std::numeric_limits<uint32_t>::max());
      continue;
    }
    if (!col.IsHeterogeneous()) {
      switch (col.Type()) {
        case ValueType::kInt64: {
          // Lean proof:
          // lean/TinylambOpt/ResearchProofs/Wave8PhysicalChoices.lean
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
                int_buf.data(), int_buf.data() + int_buf.size(), raw_v);
            const auto len = static_cast<uint32_t>(res.ptr - int_buf.data());
            AppendUint32(wire_out, len);
            wire_out->append(int_buf.data(), len);
            continue;
          }
          const auto uv = static_cast<uint64_t>(raw_v);
          if (uv <= static_cast<uint64_t>(kHotIntMax)) {
            const HotIntWireEntry& e = kHotIntTable[static_cast<size_t>(
                static_cast<int64_t>(uv) - kHotIntMin)];
            wire_out->append(e.bytes.data(), e.len);
            continue;
          }
          const auto res = std::to_chars(int_buf.data(),
                                         int_buf.data() + int_buf.size(), uv);
          const auto len = static_cast<uint32_t>(res.ptr - int_buf.data());
          AppendUint32(wire_out, len);
          wire_out->append(int_buf.data(), len);
          continue;
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
