/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/string_util.hpp"

#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace tinylamb {
namespace {

TEST(StringUtilTest, AsciiCaseConversion) {
  EXPECT_EQ(AsciiToLower('A'), 'a');
  EXPECT_EQ(AsciiToLower('Z'), 'z');
  EXPECT_EQ(AsciiToLower('a'), 'a');
  EXPECT_EQ(AsciiToLower('5'), '5');
  EXPECT_EQ(AsciiToUpper('a'), 'A');
  EXPECT_EQ(AsciiToUpper('z'), 'Z');
  EXPECT_EQ(AsciiToUpper('A'), 'A');
  EXPECT_EQ(AsciiToUpper('_'), '_');

  EXPECT_EQ(AsciiToLower("Select_123_From"), "select_123_from");
  EXPECT_EQ(AsciiToUpper("Select_123_From"), "SELECT_123_FROM");
  EXPECT_EQ(AsciiToLower(""), "");
  EXPECT_EQ(AsciiToUpper(""), "");
}

TEST(StringUtilTest, IdentifierEquals) {
  EXPECT_TRUE(IdentifierEquals("", ""));
  EXPECT_TRUE(IdentifierEquals("l_orderkey", "l_orderkey"));
  EXPECT_TRUE(IdentifierEquals("L_ORDERKEY", "l_orderkey"));
  EXPECT_TRUE(IdentifierEquals("Customer_ID", "customer_id"));
  EXPECT_FALSE(IdentifierEquals("l_orderkey", "o_orderkey"));
  EXPECT_FALSE(IdentifierEquals("col", "col1"));
  EXPECT_FALSE(IdentifierEquals("a_b", "a-b"));
}

TEST(StringUtilTest, ContainsIdentifier) {
  const std::vector<std::string> cols = {"L_OrderKey", "o_custkey", "c_name"};
  EXPECT_TRUE(ContainsIdentifier(cols, "l_orderkey"));
  EXPECT_TRUE(ContainsIdentifier(cols, "O_CUSTKEY"));
  EXPECT_TRUE(ContainsIdentifier(cols, "C_Name"));
  EXPECT_FALSE(ContainsIdentifier(cols, "p_partkey"));
  EXPECT_FALSE(ContainsIdentifier({}, "l_orderkey"));
}

TEST(StringUtilTest, TrimAsciiWhitespace) {
  EXPECT_EQ(TrimAsciiWhitespace("  hello \t\n\r "), "hello");
  EXPECT_EQ(TrimAsciiWhitespace("no_spaces"), "no_spaces");
  EXPECT_EQ(TrimAsciiWhitespace("   \t\n\r"), "");
  EXPECT_EQ(TrimAsciiWhitespace(""), "");
}

}  // namespace
}  // namespace tinylamb
