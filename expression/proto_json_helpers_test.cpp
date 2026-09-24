/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Direct tests for the proto-text / struct-JSON helper layer shared by
// function_call_expression.cpp and proto_text.cpp: object splitting, scalar
// parsing, member encoding, and struct field assignment.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/status_or.hpp"
#include "expression/function_call_expression.hpp"
#include "type/value.hpp"

namespace tinylamb {

TEST(JsonStructHelpersTest, SplitJsonObjectMembersParsesFlatBodies) {
  // Struct members are stored without the surrounding braces.
  const auto members = SplitJsonObjectMembers(R"("a": 1, "b": "x")");
  ASSERT_EQ(members.size(), 2U);
  EXPECT_EQ(members[0].first, "a");
  EXPECT_EQ(members[1].first, "b");
}

TEST(JsonStructHelpersTest, SplitJsonObjectMembersHandlesNesting) {
  const auto members =
      SplitJsonObjectMembers(R"("outer": {"inner": [1, 2]}, "n": null)");
  ASSERT_FALSE(members.empty());
  EXPECT_EQ(members[0].first, "outer");
}

TEST(JsonStructHelpersTest, SplitJsonObjectMembersKeepsBracesInStrings) {
  const auto members = SplitJsonObjectMembers(R"("s": "}{", "t": 1)");
  ASSERT_EQ(members.size(), 2U);
  EXPECT_EQ(members[0].first, "s");
  EXPECT_EQ(members[1].first, "t");
}

TEST(JsonStructHelpersTest, JsonTextToValueParsesScalars) {
  Value parsed;
  EXPECT_TRUE(JsonTextToValue("42", &parsed));
  EXPECT_EQ(parsed, Value(int64_t{42}));
  EXPECT_TRUE(JsonTextToValue("1.5", &parsed));
  EXPECT_EQ(parsed, Value(1.5));
  EXPECT_TRUE(JsonTextToValue("true", &parsed));
  // "null" parses successfully into a null Value.
  EXPECT_TRUE(JsonTextToValue("null", &parsed));
  EXPECT_TRUE(parsed.IsNull());
  ASSERT_TRUE(JsonTextToValue("\"text\"", &parsed));
  EXPECT_EQ(parsed, Value(std::string("text")));
  EXPECT_TRUE(JsonTextToValue("{\"a\": 1}", &parsed));
  EXPECT_FALSE(JsonTextToValue("not json", &parsed));
}

TEST(JsonStructHelpersTest, EncodeStructMemberJsonCoversValueTypes) {
  auto as_int = TryEncodeStructMemberJson(Value(int64_t{5}));
  ASSERT_TRUE(as_int.HasValue());
  EXPECT_EQ(as_int.MoveValue(), "5");
  auto as_double = TryEncodeStructMemberJson(Value(1.5));
  ASSERT_TRUE(as_double.HasValue());
  EXPECT_EQ(as_double.MoveValue(), "1.5");
  auto quoted = TryEncodeStructMemberJson(Value(std::string("a\"b")));
  ASSERT_TRUE(quoted.HasValue());
  EXPECT_EQ(quoted.MoveValue(), "\"a\\\"b\"");
  auto null_encoded = TryEncodeStructMemberJson(Value());
  ASSERT_TRUE(null_encoded.HasValue());
  auto nested =
      TryEncodeStructMemberJson(Value::Array({Value(int64_t{1})}, "INT64"));
  ASSERT_TRUE(nested.HasValue());
  EXPECT_EQ(nested.MoveValue(), "ARRAY<INT64>[1]");
}

TEST(JsonStructHelpersTest, TryStructSetFieldCreatesAndUpdates) {
  Value base;
  ASSERT_TRUE(JsonTextToValue("{\"a\": 1}", &base));
  auto updated = TryStructSetField(base, "b", Value(int64_t{2}));
  ASSERT_TRUE(updated.HasValue());
  const std::string rewritten =
      std::string(updated.MoveValue().value.varchar_value);
  // The rewrite carries both the original member and the new one.
  EXPECT_NE(rewritten.find('a'), std::string::npos);
  EXPECT_NE(rewritten.find("\"b\""), std::string::npos);
  EXPECT_NE(rewritten.find('2'), std::string::npos);

  // A NULL base stays NULL.
  Value null_base;
  auto created = TryStructSetField(null_base, "x.y", Value(int64_t{1}));
  EXPECT_TRUE(created.HasValue());
  EXPECT_TRUE(created.MoveValue().IsNull());
}

TEST(JsonStructHelpersTest, StructSetFieldThrowsShimMatchesStatus) {
  Value base;
  ASSERT_TRUE(JsonTextToValue("{\"a\": 1}", &base));
  const std::string once = std::string(
      StructSetField(base, "z", Value(int64_t{3})).value.varchar_value);
  const std::string twice = std::string(
      StructSetField(base, "z", Value(int64_t{3})).value.varchar_value);
  EXPECT_EQ(once, twice);
  EXPECT_EQ(EncodeStructMemberJson(Value(int64_t{8})), "8");
}

}  // namespace tinylamb
