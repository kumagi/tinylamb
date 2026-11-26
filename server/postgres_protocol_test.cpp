/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "server/postgres_protocol.hpp"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "executor/data_chunk.hpp"
#include "gtest/gtest.h"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::pgwire {
namespace {

// Test-side unwrap for the capped splitter (T1664): every existing case
// asserts on the vector and must stay far below kMaxSqlStatements.
std::vector<std::string> Split(std::string_view sql) {
  auto split = SplitSqlStatements(sql);
  EXPECT_TRUE(split.HasValue()) << ToString(split.GetStatus());
  return std::move(split).MoveValue();
}

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
  if (!parsed.has_value()) {
    return;
  }
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
    EXPECT_FALSE(ParseStartupPacket(
        StartupWithPayload(std::string("user\0alice", 10)), &error));
    EXPECT_NE(error.find("unterminated startup parameter value"),
              std::string::npos);
  }
  {
    std::string error;
    // Act -- trailing garbage after the double-NUL terminator
    EXPECT_FALSE(ParseStartupPacket(
        StartupWithPayload(std::string("user\0alice\0\0junk", 16)), &error));
    EXPECT_NE(error.find("data after startup packet terminator"),
              std::string::npos);
  }
  {
    std::string error;
    // Act -- all parameters present but no empty-string terminator
    EXPECT_FALSE(ParseStartupPacket(
        StartupWithPayload(std::string("user\0alice\0", 11)), &error));
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
  EXPECT_EQ(ReadUint16(bytes, 0).Value(), 0x1234);
  EXPECT_EQ(ReadUint32(bytes, 2).Value(), 0xABCDEF01U);

  // Assert -- out-of-range reads return kCorrupt rather than garbage
  EXPECT_FALSE(ReadUint16(bytes, 5).HasValue());
  EXPECT_FALSE(ReadUint16(bytes, 6).HasValue());
  EXPECT_FALSE(ReadUint32(bytes, 3).HasValue());
  EXPECT_FALSE(ReadUint32(bytes, 6).HasValue());
}

TEST(PostgresProtocolTest, EncodesDoubleAndDateValues) {
  // Arrange -- a row mixing a double and a date with the existing int/text/null
  const Row row({Value(3.5), Value::DateFromDays(20000), Value(int64_t{7}),
                 Value(std::string("x")), Value()});
  // Act -- encode a data row and a matching row description
  const std::string message = DataRow(row);
  const std::string description =
      RowDescription({{"d", ValueType::kDouble}, {"dt", ValueType::kDate}});

  // Assert -- the double prints with full precision
  EXPECT_NE(message.find("3.5"), std::string::npos);

  // PRODUCTION FIX: the shortest round-tripping representation must be
  // emitted (PostgreSQL behavior); max_digits10 printed 17 digits and turned
  // 0.1 into "0.10000000000000001".
  const std::string short_row = DataRow(Row({Value(0.1), Value(1e20)}));
  EXPECT_NE(short_row.find(std::string("0.1\x00", 4), 0), std::string::npos)
      << short_row;
  EXPECT_EQ(short_row.find("0.10000000000000001"), std::string::npos);

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
  EXPECT_EQ(ReadUint32(negotiated, 1).Value(), negotiated.size() - 1);
  EXPECT_EQ(ReadUint32(negotiated, 5).Value(), 3U);
  EXPECT_EQ(ReadUint32(negotiated, 9).Value(), 2U);
  EXPECT_NE(negotiated.find("_pq_.foo"), std::string::npos);
  EXPECT_NE(negotiated.find("_pq_.bar"), std::string::npos);
}

TEST(PostgresProtocolTest, ErrorResponseUsesPositionInOriginalQuery) {
  const auto statements =
      SplitSqlStatementsWithPositions("SELECT '雪';\r\n \tSELECT\n\t)");
  ASSERT_TRUE(statements.HasValue());
  ASSERT_EQ(statements.Value().size(), 2U);
  const SqlStatement& statement = statements.Value().back();
  constexpr std::string_view diagnostic = "ERROR: syntax error [at 2:9]";
  const auto position = SqlErrorPosition(diagnostic, statement, 0);
  EXPECT_EQ(position, 24U);
  const std::string response =
      ErrorResponse(diagnostic, "42601", "ERROR", position);
  EXPECT_NE(response.find(std::string("P24\0", 4)), std::string::npos);
  EXPECT_EQ(response.find(std::string("L2\0", 3)), std::string::npos);

  const auto no_position = ErrorResponse(diagnostic, "42601");
  const size_t message_start = no_position.find(diagnostic);
  ASSERT_NE(message_start, std::string::npos);
  const size_t message_end = no_position.find('\0', message_start);
  ASSERT_NE(message_end, std::string::npos);
  // The message field and the field list each have a NUL terminator.
  EXPECT_EQ(no_position.substr(message_end), std::string(2, '\0'));
  EXPECT_EQ(SqlErrorPosition("ERROR: syntax error [at x:y]", statement, 0),
            std::nullopt);
  EXPECT_EQ(SqlErrorPosition("ERROR: syntax error [at 2:7]", statement, 0),
            std::nullopt);
}

TEST(PostgresProtocolTest, ErrorResponseSanitizesFields) {
  // Act -- build an error response whose message and SQLSTATE carry NUL bytes
  const std::string message =
      ErrorResponse(std::string("bad\0query", 9), std::string("42\0P01", 6));
  // Assert -- the type byte and severity fields are present
  EXPECT_EQ(message[0], 'E');
  EXPECT_NE(message.find("ERROR"), std::string::npos);
  // Assert -- embedded NULs were replaced by spaces
  EXPECT_NE(message.find("bad query"), std::string::npos);
  EXPECT_NE(message.find("42 P01"), std::string::npos);
  // Assert -- no raw NUL remains inside the field bodies
  EXPECT_EQ(message.find(std::string("42\0P01", 6)), std::string::npos);
}

// T692: the severity is a parameter; connection-tearing sites send FATAL so
// clients classify the frame per the PG wire spec.
TEST(PostgresProtocolTest, ErrorResponseCarriesExplicitSeverity) {
  const std::string fatal =
      ErrorResponse("too many connections", "53300", "FATAL");
  EXPECT_EQ(fatal[0], 'E');
  EXPECT_NE(fatal.find(std::string("SFATAL\0", 7)), std::string::npos);
  EXPECT_NE(fatal.find(std::string("VFATAL\0", 7)), std::string::npos);
  EXPECT_NE(fatal.find("53300"), std::string::npos);
  EXPECT_EQ(fatal.find("ERROR"), std::string::npos);
  // The default keeps the historical wording byte-identical.
  EXPECT_NE(ErrorResponse("plain", "XX000").find(std::string("SERROR\0", 7)),
            std::string::npos);
}

// T1693: NoticeResponse ('N') mirrors the ErrorResponse field layout with
// severity NOTICE.
TEST(PostgresProtocolTest, NoticeResponseEncodesNoticeFrame) {
  const std::string notice =
      NoticeResponse("there is already a transaction in progress", "25001");
  EXPECT_EQ(notice[0], 'N');
  EXPECT_NE(notice.find(std::string("SNOTICE\0", 8)), std::string::npos);
  EXPECT_NE(notice.find("25001"), std::string::npos);
  EXPECT_NE(notice.find("there is already a transaction in progress"),
            std::string::npos);
  EXPECT_EQ(notice.back(), '\0');  // field-list terminator
}

TEST(PostgresProtocolTest, SplitSqlStatementsHandlesAllQuoting) {
  // Act -- split SQL mixing single, double, and backtick quotes, plus comments
  const std::vector<std::string> statements = Split(
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

// PostgreSQL nests block comments; a `;` inside an inner comment must not
// split statements.
TEST(PostgresProtocolTest, SplitSqlStatementsHandlesNestedBlockComments) {
  const std::vector<std::string> statements = Split(
      "SELECT 1; /* outer /* inner ; comment */ still ; outer */ SELECT 2;");
  ASSERT_EQ(statements.size(), 2U);
  EXPECT_EQ(statements[0], "SELECT 1");
  EXPECT_EQ(statements[1],
            "/* outer /* inner ; comment */ still ; outer */ SELECT 2");
}

TEST(PostgresProtocolTest, SplitSqlStatementsDropsEmptyStatements) {
  // Act -- split a string of bare semicolons and whitespace
  const std::vector<std::string> statements = Split(" ; ;  ; ");
  // Assert -- empty statements are omitted entirely
  EXPECT_TRUE(statements.empty());
}

TEST(PostgresProtocolTest, SplitSqlStatementsRespectsBackslashEscapes) {
  // PRODUCTION BUG (fixed): a backslash inside a single-quoted string was a
  // normal character to the splitter, so `\'` toggled the quote state early
  // and a semicolon inside the string split statements (classic
  // statement-split injection: `SELECT 'a\', 2; DROP TABLE t; --'`).
  const std::vector<std::string> one =
      Split("SELECT 'a\\', 2; DROP TABLE t; --'");
  ASSERT_EQ(one.size(), 1U);
  EXPECT_EQ(one[0], "SELECT 'a\\', 2; DROP TABLE t; --'");

  const std::vector<std::string> two = Split("SELECT 'It\\'s'; SELECT 2;");
  ASSERT_EQ(two.size(), 2U);
  EXPECT_EQ(two[0], "SELECT 'It\\'s'");
  EXPECT_EQ(two[1], "SELECT 2");
}

// A ';' inside a dollar-quoted string ($$...$$ / $tag$...$tag$) must not
// split statements; the rest of the stack parses dollar quotes, so the
// splitter ignoring them sent valid SQL as two parse errors.
TEST(PostgresProtocolTest, SplitSqlStatementsHandlesDollarQuotes) {
  const std::vector<std::string> plain = Split("SELECT $$a;b$$;");
  ASSERT_EQ(plain.size(), 1U);
  EXPECT_EQ(plain[0], "SELECT $$a;b$$");

  const std::vector<std::string> tagged =
      Split("SELECT $fn$ a;b $$;c$fn$; SELECT 2;");
  ASSERT_EQ(tagged.size(), 2U);
  EXPECT_EQ(tagged[0], "SELECT $fn$ a;b $$;c$fn$");
  EXPECT_EQ(tagged[1], "SELECT 2");

  // A bare $1 placeholder is not a dollar quote.
  const std::vector<std::string> placeholder = Split("SELECT $1; SELECT 2;");
  ASSERT_EQ(placeholder.size(), 2U);
  EXPECT_EQ(placeholder[0], "SELECT $1");
  EXPECT_EQ(placeholder[1], "SELECT 2");
}

TEST(PostgresProtocolTest, RowDescriptionDefaultNameAndTypes) {
  // Act -- describe columns with empty names and every wire type
  const std::string description = RowDescription({{"", ValueType::kVarChar},
                                                  {"i", ValueType::kInt64},
                                                  {"d", ValueType::kDouble},
                                                  {"t", ValueType::kDate},
                                                  {"n", ValueType::kNull}});
  ASSERT_TRUE(ReadUint16(description, 5).HasValue());
  EXPECT_EQ(ReadUint16(description, 5).Value(), 5);
  // Assert -- anonymous columns are labeled ?column?
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
  EXPECT_EQ(ReadUint32(message, 1).Value(), message.size() - 1);
  EXPECT_EQ(ReadUint16(message, 5).Value(), 3);
  EXPECT_NE(message.find("hello"), std::string::npos);
  const std::string null_length(4, static_cast<char>(0xff));
  EXPECT_NE(message.find(null_length), std::string::npos);
}

TEST(PostgresProtocolTest, EncodesUnsignedIntegersAboveTheSignedRange) {
  const std::string message =
      DataRow(Row({Value(int64_t{-1}).WithUnsigned(), Value(int64_t{-1})}));
  ASSERT_EQ(ReadUint32(message, 7).Value(), 20);
  EXPECT_EQ(message.substr(11, 20), "18446744073709551615");
  ASSERT_EQ(ReadUint32(message, 31).Value(), 2);
  EXPECT_EQ(message.substr(35), "-1");
}

TEST(PostgresProtocolTest, DiscardingWithheldReplyPreservesEarlierWireBytes) {
  const std::string previous = DataRow(Row({Value("earlier query")}));
  const std::string withheld = CommandComplete("COMMIT");
  constexpr size_t sent_prefix = 8;  // The previous DataRow is partly sent.
  const std::string prefix = previous.substr(0, sent_prefix);
  const std::string expected_pending = previous.substr(sent_prefix);

  for (const bool compacted : {false, true}) {
    std::string queued = (compacted ? expected_pending : previous) + withheld;
    const size_t offset = compacted ? 0 : sent_prefix;
    DiscardQueuedReply(queued, offset, expected_pending.size());
    EXPECT_EQ(prefix + queued.substr(offset), previous);
  }
  // The durability wait can also fail before the new query queues anything.
  std::string queued = previous;
  DiscardQueuedReply(queued, sent_prefix, expected_pending.size());
  EXPECT_EQ(queued, previous);
}

TEST(PostgresProtocolTest, SplitsOnlyUnquotedSemicolons) {
  const std::vector<std::string> statements =
      Split("SELECT 'a;b'; -- ignored ;\n INSERT INTO t VALUES (1); ");
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
  EXPECT_DEATH(RowDescription(columns), "too many columns");
}

TEST(PostgresProtocolTest, RejectsMoreThanUint16Values) {
  Row row(std::vector<Value>(
      static_cast<size_t>(std::numeric_limits<uint16_t>::max()) + 1));
  EXPECT_DEATH(DataRow(row), "too many values");
}

// Regression from postgres_protocol_fuzzer corpus: a trailing line comment
// must not be emitted as a second "statement" (the old bug made "select 1;
// -- note" produce a bogus parse-time failure).
TEST(PostgresProtocolTest, TrailingLineCommentIsNotAStatement) {
  // Expected behaviour: the trailing comment produces no statement.
  const auto statements = Split("select 1; -- hi");
  ASSERT_EQ(statements.size(), 1U);
  EXPECT_EQ(statements[0], "select 1");
}

TEST(PostgresProtocolTest, TrailingLineCommentWithNewlineIsNotAStatement) {
  const auto statements = Split("select 1; -- hi\n");
  ASSERT_EQ(statements.size(), 1U);
  EXPECT_EQ(statements[0], "select 1");
}

TEST(PostgresProtocolTest, CommentOnlyInputYieldsNoStatements) {
  const auto statements = Split("-- just a note");
  EXPECT_TRUE(statements.empty());
}

TEST(PostgresProtocolTest, UnterminatedQuotesAndCommentsDoNotHang) {
  // Robustness pin (passing): unterminated quoting must neither throw nor
  // split; everything up to EOF stays one statement.
  EXPECT_EQ(Split("select /* 1; 2").size(), 1U);
  EXPECT_EQ(Split("select 'a; b").size(), 1U);
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
  if (!parsed.has_value()) {
    return;
  }
  ASSERT_EQ(parsed->parameters.size(), 1U);
  EXPECT_EQ(parsed->parameters.at("user"), "second");
}

TEST(PostgresProtocolTest, SplitHandlesBacktickQuotedSemicolons) {
  // Pin (passing) from the fuzzer corpus: backticks quote like double quotes,
  // so semicolons inside them do not split.
  const auto statements = Split("select `a;b` as c; select 2");
  ASSERT_EQ(statements.size(), 2U);
  EXPECT_EQ(statements[0], "select `a;b` as c");
  EXPECT_EQ(statements[1], "select 2");
}

// DiscardQueuedReply must not wrap `output.size() - sent_prefix` through
// size_t when a caller miscomputes the prefix: a clamped call keeps the
// queued bytes instead of requesting a ~2^64 resize (bad_alloc -> abort).
TEST(PostgresProtocolTest, DiscardQueuedReplyClampsOversizedSentPrefix) {
  std::string output = "0123456789";
  DiscardQueuedReply(output, /*sent_prefix=*/16, /*earlier_pending_bytes=*/3);
  EXPECT_EQ(output, "0123456789");

  // The normal roll-back contract is unchanged: keep the sent prefix plus
  // min(pending, earlier_pending) bytes.
  std::string queued = "keepdropdata";
  DiscardQueuedReply(queued, /*sent_prefix=*/4, /*earlier_pending_bytes=*/2);
  EXPECT_EQ(queued, "keepdr");
}

// The zero-copy batch builder must produce exactly the bytes of the
// per-message builder, frame for frame.
TEST(PostgresProtocolTest, AppendParameterStatusMatchesParameterStatus) {
  std::string batch;
  AppendParameterStatus(&batch, "server_version", "16.0-tinylamb");
  AppendParameterStatus(&batch, "TimeZone", "UTC");
  EXPECT_EQ(batch, ParameterStatus("server_version", "16.0-tinylamb") +
                       ParameterStatus("TimeZone", "UTC"));
}

// T1664: a message packed with separators must not make the splitter
// allocate millions of strings; it errors out at the cap instead, and the
// cap boundary itself still splits cleanly.
TEST(PostgresProtocolTest, SplitSqlStatementsCapsStatementCount) {
  std::string at_cap;
  at_cap.reserve(kMaxSqlStatements * 2);
  for (size_t i = 0; i < kMaxSqlStatements; ++i) {
    at_cap += "a;";
  }
  const auto ok = SplitSqlStatements(at_cap);
  ASSERT_TRUE(ok.HasValue());
  EXPECT_EQ(ok.Value().size(), kMaxSqlStatements);

  const auto overflow = SplitSqlStatements(at_cap + "b;");
  ASSERT_FALSE(overflow.HasValue());
  EXPECT_NE(overflow.GetStatus().GetMessage().find("too many SQL statements"),
            std::string::npos);
}

TEST(PostgresProtocolTest, AppendDataRowFromChunkMatchesDataRow) {
  DataChunk chunk;
  chunk.Append(
      Row({Value(int64_t{42}), Value(int64_t{-1}).WithUnsigned(), Value(3.125),
           Value::DateFromDays(20000), Value("ascii"), Value()}));
  chunk.Append(Row({Value(std::numeric_limits<int64_t>::min()),
                    Value(int64_t{0}).WithUnsigned(), Value(-0.0),
                    Value::DateFromDays(0), Value("utf8-\xC3\xA9"), Value()}));
  chunk.Append(Row({Value(std::numeric_limits<int64_t>::max()),
                    Value(int64_t{123}).WithUnsigned(),
                    Value(std::numeric_limits<double>::infinity()), Value(),
                    Value(""), Value()}));
  chunk.Append(
      Row({Value(), Value(), Value(std::numeric_limits<double>::quiet_NaN()),
           Value::DateFromDays(19000), Value(), Value()}));

  std::string batched;
  std::string expected_batched;
  for (size_t r = 0; r < chunk.Size(); ++r) {
    std::string single;
    ASSERT_EQ(AppendDataRowFromChunk(&single, chunk, r), Status::kSuccess);
    const std::string row_bytes = DataRow(chunk.RowAt(r));
    EXPECT_EQ(single, row_bytes) << "mismatch at row " << r;

    ASSERT_EQ(AppendDataRowFromChunk(&batched, chunk, r), Status::kSuccess);
    expected_batched += row_bytes;
  }
  EXPECT_EQ(batched, expected_batched);
}

TEST(PostgresProtocolTest, AppendDataRowFromChunkHotIntBoundaries) {
  DataChunk chunk;
  const std::vector<int64_t> signed_vals = {-1025, -1024, -1,    0,
                                            1,     42,    65535, 65536};
  const std::vector<uint64_t> unsigned_vals = {
      0, 1, 65535, 65536, std::numeric_limits<uint64_t>::max()};
  for (size_t i = 0; i < signed_vals.size(); ++i) {
    const uint64_t uv = unsigned_vals[i % unsigned_vals.size()];
    chunk.Append(Row({Value(signed_vals[i]),
                      Value(static_cast<int64_t>(uv)).WithUnsigned()}));
  }
  for (size_t r = 0; r < chunk.Size(); ++r) {
    std::string got;
    ASSERT_EQ(AppendDataRowFromChunk(&got, chunk, r), Status::kSuccess);
    EXPECT_EQ(got, DataRow(chunk.RowAt(r))) << "row " << r;
  }
}

TEST(PostgresProtocolTest, ExtendedQueryParsersAndBinaryDataRows) {
  // Parse message with 1 int8 parameter OID (20).
  std::string parse_payload;
  parse_payload.append("s1", 3);
  parse_payload.append("SELECT $1 + 1", 14);
  AppendUint16(&parse_payload, 1);
  AppendUint32(&parse_payload, 20);
  auto parsed = ParseParseMessage(parse_payload);
  ASSERT_TRUE(parsed.HasValue());
  EXPECT_EQ(parsed.Value().statement_name, "s1");
  EXPECT_EQ(parsed.Value().query, "SELECT $1 + 1");
  ASSERT_EQ(parsed.Value().param_types.size(), 1U);
  EXPECT_EQ(parsed.Value().param_types[0], 20U);

  // Bind message with 1 text param ("41") and broadcast binary result format.
  std::string bind_payload;
  bind_payload.append("p1", 3);
  bind_payload.append("s1", 3);
  AppendUint16(&bind_payload, 1);
  AppendUint16(&bind_payload, 0);  // param format = text
  AppendUint16(&bind_payload, 1);
  AppendUint32(&bind_payload, 2);
  bind_payload.append("41", 2);
  AppendUint16(&bind_payload, 1);
  AppendUint16(&bind_payload, 1);  // result format = binary
  auto bound = ParseBindMessage(bind_payload);
  ASSERT_TRUE(bound.HasValue());
  EXPECT_EQ(bound.Value().portal_name, "p1");
  EXPECT_EQ(bound.Value().statement_name, "s1");
  ASSERT_EQ(bound.Value().result_formats.size(), 1U);
  EXPECT_EQ(bound.Value().result_formats[0], 1U);

  auto sql = BindSqlParameters(parsed.Value().query, parsed.Value().param_types,
                               bound.Value().param_formats,
                               bound.Value().params);
  ASSERT_TRUE(sql.HasValue());
  EXPECT_EQ(sql.Value(), "SELECT 41 + 1");

  // Fixed-width binary fast path: (INT64, FLOAT64, DATE) with no NULLs.
  DataChunk fixed_chunk;
  fixed_chunk.Initialize(
      {ValueType::kInt64, ValueType::kDouble, ValueType::kDate});
  fixed_chunk.Append(Row({Value(int64_t{42}), Value(3.25),
                          Value::DateFromDays(10957 + 10)}));
  fixed_chunk.Append(Row({Value(int64_t{-7}), Value(-0.5),
                          Value::DateFromDays(10957 - 5)}));
  const std::vector<uint16_t> all_bin = {1};
  std::string batch_wire;
  size_t written = 0;
  ASSERT_EQ(AppendDataRowsFromChunk(&batch_wire, fixed_chunk, 0,
                                    fixed_chunk.Size(), 0, all_bin, &written),
            Status::kSuccess);
  EXPECT_EQ(written, 2U);
  std::string expected_wire;
  for (size_t r = 0; r < fixed_chunk.Size(); ++r) {
    expected_wire += DataRow(fixed_chunk.RowAt(r), all_bin);
  }
  EXPECT_EQ(batch_wire, expected_wire);

  // Mixed binary + NULL + VARCHAR + text double fast path parity.
  DataChunk mixed_chunk;
  mixed_chunk.Initialize(
      {ValueType::kInt64, ValueType::kDouble, ValueType::kVarChar});
  mixed_chunk.Append(Row({Value(int64_t{100}), Value(12.5), Value("ok")}));
  mixed_chunk.Append(Row({Value(), Value(0.0), Value()}));
  for (const auto& fmts : std::vector<std::vector<uint16_t>>{
           {}, {1}, {1, 0, 1}}) {
    std::string got;
    size_t n = 0;
    ASSERT_EQ(AppendDataRowsFromChunk(&got, mixed_chunk, 0, mixed_chunk.Size(),
                                      0, fmts, &n),
              Status::kSuccess);
    EXPECT_EQ(n, mixed_chunk.Size());
    std::string ref;
    for (size_t r = 0; r < mixed_chunk.Size(); ++r) {
      ref += DataRow(mixed_chunk.RowAt(r), fmts);
    }
    EXPECT_EQ(got, ref);
  }
}

}  // namespace tinylamb::pgwire
