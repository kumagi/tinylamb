/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "server/postgres_protocol.hpp"

#include <cstdint>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb::pgwire {
namespace {

TEST(PostgresProtocolTest, ParsesStartupPacket) {
  std::string packet;
  AppendUint32(&packet, 0);
  AppendUint32(&packet, kProtocolVersion30);
  packet.append("user\0alice\0database\0warehouse\0\0", 31);
  const uint32_t size = static_cast<uint32_t>(packet.size());
  packet[0] = static_cast<char>((size >> 24U) & 0xffU);
  packet[1] = static_cast<char>((size >> 16U) & 0xffU);
  packet[2] = static_cast<char>((size >> 8U) & 0xffU);
  packet[3] = static_cast<char>(size & 0xffU);

  std::string error;
  const std::optional<StartupPacket> parsed =
      ParseStartupPacket(packet, &error);
  ASSERT_TRUE(parsed) << error;
  EXPECT_EQ(parsed->protocol_version, kProtocolVersion30);
  EXPECT_EQ(parsed->parameters.at("user"), "alice");
  EXPECT_EQ(parsed->parameters.at("database"), "warehouse");
}

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

}  // namespace
}  // namespace tinylamb::pgwire
