/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "server/postgres_protocol.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::pgwire {
namespace {

TEST(PostgresProtocolTest, ParsesStartupPacket) {
  std::string packet;
  AppendUint32(&packet, 0);
  AppendUint32(&packet, kProtocolVersion30);
  packet += "user";
  packet.push_back('\0');
  packet += "alice";
  packet.push_back('\0');
  packet += "database";
  packet.push_back('\0');
  packet += "warehouse";
  packet.push_back('\0');
  packet.push_back('\0');
  const auto size = static_cast<uint32_t>(packet.size());
  packet[0] = static_cast<char>((size >> 24U) & 0xffU);
  packet[1] = static_cast<char>((size >> 16U) & 0xffU);
  packet[2] = static_cast<char>((size >> 8U) & 0xffU);
  packet[3] = static_cast<char>(size & 0xffU);

  std::string error;
  const std::optional<StartupPacket> parsed =
      ParseStartupPacket(packet, &error);
  ASSERT_TRUE(parsed) << error;
  if (!parsed.has_value()) { return; }
  EXPECT_EQ(parsed->protocol_version, kProtocolVersion30);
  EXPECT_EQ(parsed->parameters.at("user"), "alice");
  EXPECT_EQ(parsed->parameters.at("database"), "warehouse");
}

namespace {
// Builds a startup-packet shaped blob whose 4-byte length prefix equals the
// total payload size, so the caller only controls what follows the version.
std::string StartupWithPayload(const std::string& body) {
  std::string packet;
  AppendUint32(&packet, 0);
  AppendUint32(&packet, kProtocolVersion30);
  packet.append(body);
  const auto size = static_cast<uint32_t>(packet.size());
  packet[0] = static_cast<char>((size >> 24U) & 0xffU);
  packet[1] = static_cast<char>((size >> 16U) & 0xffU);
  packet[2] = static_cast<char>((size >> 8U) & 0xffU);
  packet[3] = static_cast<char>(size & 0xffU);
  return packet;
}
}  // namespace

TEST(PostgresProtocolTest, RejectsMalformedStartupPackets) {
  {
    std::string error;
    // Act -- a packet whose declared length does not match its real size
    // Assert -- the parser reports malformed rather than reading garbage
    // (explicit length: a plain literal would truncate at the first NUL)
    std::string packet = StartupWithPayload(std::string("user\0alice\0\0", 12));
    packet[3] = static_cast<char>(packet[3] + 1);
    EXPECT_FALSE(ParseStartupPacket(packet, &error));
    EXPECT_NE(error.find("malformed"), std::string::npos);
  }
  {
    std::string error;
    // Act -- an unterminated parameter name
    EXPECT_FALSE(ParseStartupPacket(StartupWithPayload("user"), &error));
    EXPECT_NE(error.find("unterminated startup parameter name"),
              std::string::npos);
  }
  {
    std::string error;
    // Act -- a value without its terminating NUL
    EXPECT_FALSE(ParseStartupPacket(StartupWithPayload(
                                     std::string("user\0alice", 10)),
                                    &error));
    EXPECT_NE(error.find("unterminated startup parameter value"),
              std::string::npos);
  }
  {
    std::string error;
    // Act -- trailing garbage after the double-NUL terminator
    EXPECT_FALSE(ParseStartupPacket(
                     StartupWithPayload(std::string("user\0alice\0\0junk", 16)),
                     &error));
    EXPECT_NE(error.find("data after startup packet terminator"),
              std::string::npos);
  }
  {
    std::string error;
    // Act -- all parameters present but no empty-string terminator
    EXPECT_FALSE(ParseStartupPacket(
                     StartupWithPayload(std::string("user\0alice\0", 11)),
                     &error));
    EXPECT_NE(error.find("no terminator"), std::string::npos);
  }
  {
    std::string error;
    // Act -- a packet too short to even carry the length + version fields
    EXPECT_FALSE(ParseStartupPacket("ABCDEF", &error));
    EXPECT_NE(error.find("malformed"), std::string::npos);
  }
}

TEST(PostgresProtocolTest, ReadUintEndiannessAndBounds) {
  // Act -- read a big-endian uint16/uint32 and parse them back
  std::string bytes(6, '\0');
  bytes[0] = static_cast<char>(0x12);
  bytes[1] = static_cast<char>(0x34);
  bytes[2] = static_cast<char>(0xAB);
  bytes[3] = static_cast<char>(0xCD);
  bytes[4] = static_cast<char>(0xEF);
  bytes[5] = static_cast<char>(0x01);
  EXPECT_EQ(ReadUint16(bytes, 0), 0x1234);
  EXPECT_EQ(ReadUint32(bytes, 2), 0xABCDEF01);

  // Assert -- out-of-range reads throw rather than returning garbage
  EXPECT_THROW(ReadUint16(bytes, 5), std::out_of_range);
  EXPECT_THROW(ReadUint16(bytes, 6), std::out_of_range);
  EXPECT_THROW(ReadUint32(bytes, 3), std::out_of_range);
  EXPECT_THROW(ReadUint32(bytes, 6), std::out_of_range);
}

TEST(PostgresProtocolTest, EncodesDoubleAndDateValues) {
  // Arrange -- a row mixing a double and a date with the existing int/text/null
  const Row row({Value(3.5), Value::DateFromDays(20000), Value(int64_t{7}),
                 Value(std::string("x")), Value()});
  // Act -- encode a data row and a matching row description
  const std::string message = DataRow(row);
  const std::string description = RowDescription(
      {{"d", ValueType::kDouble}, {"dt", ValueType::kDate}});

  // Assert -- the double prints with full precision
  EXPECT_NE(message.find("3.5"), std::string::npos);
  // float8 OID 701 then date OID 1082 in the row description
  EXPECT_NE(description.find(std::string("\x00\x00\x02\xbd", 4)),
            std::string::npos);
  EXPECT_NE(description.find(std::string("\x00\x00\x04\x3a", 4)),
            std::string::npos);
}

TEST(PostgresProtocolTest, EmptyQueryAndNegotiateProtocolVersion) {
  // Assert -- an empty query response is an 'I' message
  EXPECT_EQ(EmptyQueryResponse()[0], 'I');

  // Act -- negotiate protocol version with unsupported parameters
  const std::string negotiated =
      NegotiateProtocolVersion(3, {"_pq_.foo", "_pq_.bar"});
  // Assert -- 'v' type byte, minor version, count, then the names
  EXPECT_EQ(negotiated[0], 'v');
  EXPECT_EQ(ReadUint32(negotiated, 1), negotiated.size() - 1);
  EXPECT_EQ(ReadUint32(negotiated, 5), 3);
  EXPECT_EQ(ReadUint32(negotiated, 9), 2);
  EXPECT_NE(negotiated.find("_pq_.foo"), std::string::npos);
  EXPECT_NE(negotiated.find("_pq_.bar"), std::string::npos);
}

TEST(PostgresProtocolTest, ErrorResponseSanitizesFields) {
  // Act -- build an error response whose message and SQLSTATE carry NUL bytes
  const std::string message = ErrorResponse(std::string("bad\0query", 9),
                                            std::string("42\0P01", 6));
  // Assert -- the type byte and severity fields are present
  EXPECT_EQ(message[0], 'E');
  EXPECT_NE(message.find("ERROR"), std::string::npos);
  // Assert -- embedded NULs were replaced by spaces
  EXPECT_NE(message.find("bad query"), std::string::npos);
  EXPECT_NE(message.find("42 P01"), std::string::npos);
  // Assert -- no raw NUL remains inside the field bodies
  EXPECT_EQ(message.find(std::string("42\0P01", 6)), std::string::npos);
}

TEST(PostgresProtocolTest, SplitSqlStatementsHandlesAllQuoting) {
  // Act -- split SQL mixing single, double, and backtick quotes, plus comments
  const std::vector<std::string> statements = SplitSqlStatements(
      "SELECT 'it''s;ok'; /* block ; comment */ UPDATE t SET a = \"x;y\" "
      "WHERE b = `s;t`; -- trailing ; comment\nSELECT 2;");

  // Assert -- each statement is trimmed and split at real semicolons only.
  // Line/block comments stay inside their statement (SplitSqlStatements only
  // uses them to decide where semicolons may split), so the trailing comment
  // remains attached to the final statement.
  ASSERT_EQ(statements.size(), 3U);
  EXPECT_EQ(statements[0], "SELECT 'it''s;ok'");
  EXPECT_EQ(statements[1],
            "/* block ; comment */ UPDATE t SET a = \"x;y\" WHERE b = `s;t`");
  EXPECT_EQ(statements[2], "-- trailing ; comment\nSELECT 2");
}

TEST(PostgresProtocolTest, SplitSqlStatementsDropsEmptyStatements) {
  // Act -- split a string of bare semicolons and whitespace
  const std::vector<std::string> statements = SplitSqlStatements(" ; ;  ; ");
  // Assert -- empty statements are omitted entirely
  EXPECT_TRUE(statements.empty());
}

TEST(PostgresProtocolTest, RowDescriptionDefaultNameAndTypes) {
  // Act -- describe columns with empty names and every wire type
  const std::string description = RowDescription(
      {{"", ValueType::kVarChar},
       {"i", ValueType::kInt64},
       {"d", ValueType::kDouble},
       {"t", ValueType::kDate},
       {"n", ValueType::kNull}});
  // Assert -- anonymous columns are labelled ?column?
  EXPECT_NE(description.find("?column?"), std::string::npos);
  // int8 OID 20
  EXPECT_NE(description.find(std::string("\x00\x00\x00\x14", 4)),
            std::string::npos);
}

}  // namespace

TEST(PostgresProtocolTest, EncodesTextAndNullDataRows) {
  const std::string message =
      DataRow(Row({Value(int64_t{42}), Value(std::string("hello")), Value()}));
  ASSERT_GE(message.size(), 5U);
  EXPECT_EQ(message[0], 'D');
  EXPECT_EQ(ReadUint32(message, 1), message.size() - 1);
  EXPECT_EQ(ReadUint16(message, 5), 3);
  EXPECT_NE(message.find("hello"), std::string::npos);
  const std::string null_length(4, static_cast<char>(0xff));
  EXPECT_NE(message.find(null_length), std::string::npos);
}

TEST(PostgresProtocolTest, SplitsOnlyUnquotedSemicolons) {
  const std::vector<std::string> statements = SplitSqlStatements(
      "SELECT 'a;b'; -- ignored ;\n INSERT INTO t VALUES (1); ");
  ASSERT_EQ(statements.size(), 2U);
  EXPECT_EQ(statements[0], "SELECT 'a;b'");
  EXPECT_EQ(statements[1], "-- ignored ;\n INSERT INTO t VALUES (1)");
}

TEST(PostgresProtocolTest, EncodesExpectedBackendMessageTypes) {
  EXPECT_EQ(AuthenticationOk()[0], 'R');
  EXPECT_EQ(ParameterStatus("client_encoding", "UTF8")[0], 'S');
  EXPECT_EQ(ReadyForQuery('I')[0], 'Z');
  EXPECT_EQ(ErrorResponse("bad SQL", "42601")[0], 'E');
  EXPECT_EQ(RowDescription({{"answer", ValueType::kInt64}})[0], 'T');
  EXPECT_EQ(CommandComplete("SELECT 1")[0], 'C');
}

// The wire format stores the column count as uint16; a silent truncation
// would desynchronize RowDescription/DataRow from the actual payload.
TEST(PostgresProtocolTest, RejectsMoreThanUint16Columns) {
  std::vector<ColumnDescription> columns(
      static_cast<size_t>(std::numeric_limits<uint16_t>::max()) + 1);
  EXPECT_THROW(RowDescription(columns), std::runtime_error);
}

TEST(PostgresProtocolTest, RejectsMoreThanUint16Values) {
  Row row(std::vector<Value>(
      static_cast<size_t>(std::numeric_limits<uint16_t>::max()) + 1));
  EXPECT_THROW(DataRow(row), std::runtime_error);
}

// Tests derived from postgres_protocol_fuzzer corpus analysis.  The fuzzer
// showed SplitSqlStatements keeps a trailing line comment as its own
// "statement" (the trim only strips whitespace, not the comment text), so a
// client sending "select 1; -- note" gets a bogus second statement that can
// only fail at parse time.
TEST(PostgresProtocolTest, TrailingLineCommentIsNotAStatement) {
  // Expected behaviour: the trailing comment produces no statement.
  const auto statements = SplitSqlStatements("select 1; -- hi");
  ASSERT_EQ(statements.size(), 1U);
  EXPECT_EQ(statements[0], "select 1");
}

TEST(PostgresProtocolTest, TrailingLineCommentWithNewlineIsNotAStatement) {
  const auto statements = SplitSqlStatements("select 1; -- hi\n");
  ASSERT_EQ(statements.size(), 1U);
  EXPECT_EQ(statements[0], "select 1");
}

TEST(PostgresProtocolTest, CommentOnlyInputYieldsNoStatements) {
  const auto statements = SplitSqlStatements("-- just a note");
  EXPECT_TRUE(statements.empty());
}

TEST(PostgresProtocolTest, UnterminatedQuotesAndCommentsDoNotHang) {
  // Robustness pin (passing): unterminated quoting must neither throw nor
  // split; everything up to EOF stays one statement.
  EXPECT_EQ(SplitSqlStatements("select /* 1; 2").size(), 1U);
  EXPECT_EQ(SplitSqlStatements("select 'a; b").size(), 1U);
}

TEST(PostgresProtocolTest, DuplicateStartupParametersKeepLastValue) {
  // Pin (passing): insert_or_assign semantics - the last occurrence of a
  // duplicated parameter name wins.  Derived from a fuzzer-oracle lesson:
  // map range-construction keeps the FIRST duplicate instead.
  std::string body;
  body.append("user\0first\0", 11);
  body.append("user\0second\0", 12);
  body.push_back('\0');
  const std::string packet = StartupWithPayload(body);

  std::string error;
  const std::optional<StartupPacket> parsed =
      ParseStartupPacket(packet, &error);
  ASSERT_TRUE(parsed) << error;
  if (!parsed.has_value()) { return; }
  ASSERT_EQ(parsed->parameters.size(), 1U);
  EXPECT_EQ(parsed->parameters.at("user"), "second");
}

TEST(PostgresProtocolTest, SplitHandlesBacktickQuotedSemicolons) {
  // Pin (passing) from the fuzzer corpus: backticks quote like double quotes,
  // so semicolons inside them do not split.
  const auto statements = SplitSqlStatements("select `a;b` as c; select 2");
  ASSERT_EQ(statements.size(), 2U);
  EXPECT_EQ(statements[0], "select `a;b` as c");
  EXPECT_EQ(statements[1], "select 2");
}

}  // namespace tinylamb::pgwire
