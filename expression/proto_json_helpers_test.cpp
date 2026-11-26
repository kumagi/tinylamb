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
#include "expression/proto_text.hpp"
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
  EXPECT_EQ(parsed, Value(true));
  // "null" parses successfully into a null Value.
  EXPECT_TRUE(JsonTextToValue("null", &parsed));
  EXPECT_TRUE(parsed.IsNull());
  ASSERT_TRUE(JsonTextToValue("\"text\"", &parsed));
  EXPECT_EQ(parsed, Value(std::string("text")));
  EXPECT_TRUE(JsonTextToValue("{\"a\": 1}", &parsed));
  EXPECT_FALSE(JsonTextToValue("not json", &parsed));
  EXPECT_FALSE(JsonTextToValue("", &parsed));
  EXPECT_FALSE(JsonTextToValue("  \t\n", &parsed));
  // Rejection needs more than one unrelated token: truncated JSON, trailing
  // garbage after an otherwise valid number, and a bad numeric spelling are
  // the cases that actually break struct-JSON round trips. A failed parse
  // must leave `parsed` untouched so callers never observe a half-written
  // value.
  const Value before_invalid = parsed;
  for (const char* invalid :
       {"{\"a\": 1", "[1, 2", "42abc", "1.2.3", "\"unterminated"}) {
    SCOPED_TRACE(invalid);
    EXPECT_FALSE(JsonTextToValue(invalid, &parsed));
    EXPECT_EQ(parsed, before_invalid);
  }
}

TEST(JsonStructHelpersTest, AuditArrayStringsRoundTripInsideStructMembers) {
  const std::vector<Value> elements{Value("a\"b,c"),
                                    Value("slash\\tail"),
                                    Value("line\nbreak\ttab"),
                                    Value(std::string("nul\0byte", 8)),
                                    Value(""),
                                    Value("NULL"),
                                    Value()};
  const Value source = Value::Array(elements, "STRING");
  auto encoded = TryEncodeStructMemberJson(source);
  ASSERT_TRUE(encoded.HasValue());
  Value decoded;
  ASSERT_TRUE(JsonTextToValue(encoded.Value(), &decoded));
  ASSERT_EQ(decoded.type, ValueType::kArray);
  EXPECT_EQ(decoded.ArrayElements(), elements);
  auto empty = TryEncodeStructMemberJson(Value::Array({}, "STRING"));
  ASSERT_TRUE(empty.HasValue());
  ASSERT_TRUE(JsonTextToValue(empty.Value(), &decoded));
  EXPECT_TRUE(decoded.ArrayElements().empty());
  EXPECT_FALSE(JsonTextToValue(R"(ARRAY<STRING>["unterminated])", &decoded));
  ASSERT_TRUE(JsonTextToValue(R"("\uD83D\uDE00")", &decoded));
  EXPECT_EQ(decoded, Value("😀"));
  EXPECT_FALSE(JsonTextToValue(R"("\uD800")", &decoded));
  EXPECT_FALSE(JsonTextToValue(R"("\q")", &decoded));
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
  EXPECT_EQ(null_encoded.Value(), "null");
  auto nested =
      TryEncodeStructMemberJson(Value::Array({Value(int64_t{1})}, "INT64"));
  ASSERT_TRUE(nested.HasValue());
  // Array members encode as JSON arrays (T2819): the bespoke struct reader
  // decodes `[...]` back to an ARRAY instead of the lossy display text.
  EXPECT_EQ(nested.MoveValue(), "[1]");
  Value round_tripped;
  ASSERT_TRUE(JsonTextToValue("[1, null, 3]", &round_tripped));
  ASSERT_TRUE(round_tripped.IsArray());
  EXPECT_EQ(round_tripped.ArrayElements().size(), 3);
  EXPECT_EQ(round_tripped.ArrayElements()[0].value.int_value, 1);
  EXPECT_TRUE(round_tripped.ArrayElements()[1].IsNull());
  EXPECT_EQ(round_tripped.ArrayElements()[2].value.int_value, 3);
}

TEST(JsonStructHelpersTest, TryStructSetFieldCreatesAndUpdates) {
  Value base;
  ASSERT_TRUE(JsonTextToValue("{\"a\": 1}", &base));
  auto updated = TryStructSetField(base, "b", Value(int64_t{2}));
  ASSERT_TRUE(updated.HasValue());
  const std::string rewritten =
      std::string(updated.MoveValue().value.varchar_value);
  // Substring checks would pass even if the value landed in the wrong slot
  // or stray text were appended. Pin the complete object text and read it
  // back structurally: exactly two members, a=1 and b=2.
  EXPECT_EQ(rewritten, R"({"a":1,"b":2})");
  Value reread;
  ASSERT_TRUE(JsonTextToValue(rewritten, &reread));
  const std::string object(reread.value.varchar_value);
  ASSERT_GE(object.size(), 2U);
  const auto members =
      SplitJsonObjectMembers(object.substr(1, object.size() - 2));
  ASSERT_EQ(members.size(), 2U);
  EXPECT_EQ(members[0].first, "a");
  EXPECT_EQ(members[0].second, "1");
  EXPECT_EQ(members[1].first, "b");
  EXPECT_EQ(members[1].second, "2");

  // A NULL base stays NULL.
  Value null_base;
  auto created = TryStructSetField(null_base, "x.y", Value(int64_t{1}));
  ASSERT_TRUE(created.HasValue());
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

TEST(ProtoTextHelpersTest,
     NormalizeProtoTextRejectsBracesInBareScalarAndIsIdempotent) {
  // Fuzz-found (json_proto_fuzzer): bare scalar tokens inside `< ... >`
  // previously absorbed `}` / `{`, so NormalizeProtoText rewrote `a < b: } >`
  // to `a { b: } }` which failed on the second pass.
  EXPECT_FALSE(NormalizeProtoText("a < b: } >").has_value());
  EXPECT_FALSE(NormalizeProtoText("a < b: { >").has_value());
  const auto norm1 = NormalizeProtoText("a < b: 1 >");
  ASSERT_TRUE(norm1.has_value());
  EXPECT_EQ(*norm1, "a { b: 1 }");
  const auto norm2 = NormalizeProtoText(*norm1);
  ASSERT_TRUE(norm2.has_value());
  EXPECT_EQ(*norm2, *norm1);
}

}  // namespace tinylamb
