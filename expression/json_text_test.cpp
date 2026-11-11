/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Unit tests for the shared strict-JSON validator and renderer
// (expression/json_text.hpp). PARSE_JSON / TO_JSON route through the same
// code from both evaluators, so the acceptance boundary is pinned here at
// the byte level: whatever a test rejects here must never reach an
// evaluator-specific opinion.

#include "expression/json_text.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>

namespace tinylamb {

namespace {

bool Valid(std::string_view text) { return json_text::IsValid(text); }

}  // namespace

TEST(JsonTextValidator, AcceptsTheCoreGrammar) {
  EXPECT_TRUE(Valid("{}"));
  EXPECT_TRUE(Valid("[]"));
  EXPECT_TRUE(Valid("0"));
  EXPECT_TRUE(Valid("-0"));
  EXPECT_TRUE(Valid("42"));
  EXPECT_TRUE(Valid("-1.25e-3"));
  EXPECT_TRUE(Valid("1E+2"));
  EXPECT_TRUE(Valid("\"text\""));
  EXPECT_TRUE(Valid("true"));
  EXPECT_TRUE(Valid("false"));
  EXPECT_TRUE(Valid("null"));
  // Nesting, whitespace and every escape letter.
  EXPECT_TRUE(Valid(" { \"a\" : [ 1 , 2 , { \"b\" : null } ] } "));
  EXPECT_TRUE(Valid("\"\\\"\\\\\\/\\b\\f\\n\\r\\t\""));
  EXPECT_TRUE(Valid("\"\\u0041\\u00e9\\ufffd\""));
  EXPECT_TRUE(Valid("\"\\uD83D\\uDE00\""));  // Valid surrogate pair.
}

TEST(JsonTextValidator, RejectsMalformedNumbers) {
  EXPECT_FALSE(Valid("+1"));
  EXPECT_FALSE(Valid("01"));
  EXPECT_FALSE(Valid("-"));
  EXPECT_FALSE(Valid(".5"));
  EXPECT_FALSE(Valid("1."));
  EXPECT_FALSE(Valid("1e"));
  EXPECT_FALSE(Valid("1e+"));
  EXPECT_FALSE(Valid("1.2.3"));
  EXPECT_FALSE(Valid("0x10"));
  EXPECT_FALSE(Valid("Infinity"));
  EXPECT_FALSE(Valid("NaN"));
  EXPECT_FALSE(Valid("--1"));
}

TEST(JsonTextValidator, RejectsMalformedContainers) {
  EXPECT_FALSE(Valid("{"));
  EXPECT_FALSE(Valid("["));
  EXPECT_FALSE(Valid("{]"));
  EXPECT_FALSE(Valid("[}"));
  EXPECT_FALSE(Valid("{,}"));
  EXPECT_FALSE(Valid("[,]"));
  EXPECT_FALSE(Valid("{\"a\" 1}"));    // Missing colon.
  EXPECT_FALSE(Valid("{a: 1}"));       // Unquoted key.
  EXPECT_FALSE(Valid("{\"a\": 1,}"));  // Trailing comma.
  EXPECT_FALSE(Valid("[1, ]"));        // Trailing comma.
  EXPECT_FALSE(Valid("{\"a\": }"));    // Missing value.
  EXPECT_FALSE(Valid(""));             // Empty input.
  EXPECT_FALSE(Valid("   "));          // Whitespace only.
}

TEST(JsonTextValidator, RejectsTrailingGarbage) {
  EXPECT_FALSE(Valid("{} {}"));
  EXPECT_FALSE(Valid("1 2"));
  EXPECT_FALSE(Valid("true x"));
  EXPECT_FALSE(Valid("\"a\" \"b\""));
}

TEST(JsonTextValidator, RejectsMalformedStrings) {
  EXPECT_FALSE(Valid("\"unterminated"));
  EXPECT_FALSE(Valid("\"bad\\escape\""));
  EXPECT_FALSE(Valid("\"\\u12\""));           // Short escape.
  EXPECT_FALSE(Valid("\"\\uzzzz\""));         // Non-hex.
  EXPECT_FALSE(Valid("\"\\uD800\""));         // Lone high surrogate.
  EXPECT_FALSE(Valid("\"\\uDC00\""));         // Lone low surrogate.
  EXPECT_FALSE(Valid("\"\\uD800\\u0041\""));  // High not followed by low.
  EXPECT_FALSE(Valid("\"\\uD800\\uD800\""));  // High followed by high.
  EXPECT_FALSE(Valid("\"\\uDBFF\\uDBFF\""));  // High followed by high.
  EXPECT_FALSE(Valid("\"ab\ncd\""));          // Raw newline.
  EXPECT_FALSE(Valid("\"\\\""));              // Escaped quote, no close.
}

TEST(JsonTextValidator, AcceptsControlFreeHighBytes) {
  // UTF-8 text above 0x7F is passed through verbatim.
  EXPECT_TRUE(Valid("\"日本語\""));
  EXPECT_TRUE(Valid("\"\x7f\""));  // DEL is a legal raw code point.
}

TEST(JsonTextValidator, BoundsNestingDepth) {
  // The validator runs on untrusted PARSE_JSON arguments; an unbounded
  // descent would crash the process instead of reporting invalid input.
  // 255 levels stay valid, the depth cap itself (256) is the last valid
  // level, and one past it is rejected.
  std::string deep_ok;
  for (int i = 0; i < 255; ++i) {
    deep_ok += "[";
  }
  std::string close_ok(255, ']');
  EXPECT_TRUE(Valid(deep_ok + "1" + close_ok));

  std::string deep_rejected;
  for (int i = 0; i < 257; ++i) {
    deep_rejected += "[";
  }
  std::string close_rejected(257, ']');
  EXPECT_FALSE(Valid(deep_rejected + "1" + close_rejected));
}

TEST(JsonTextStringRenderer, EscapesControlAndQuotes) {
  EXPECT_EQ(json_text::EncodeJsonString("plain"), "\"plain\"");
  EXPECT_EQ(json_text::EncodeJsonString("a\"b"), "\"a\\\"b\"");
  EXPECT_EQ(json_text::EncodeJsonString("a\\b"), "\"a\\\\b\"");
  EXPECT_EQ(json_text::EncodeJsonString("a\nb"), "\"a\\nb\"");
  EXPECT_EQ(json_text::EncodeJsonString("a\tb"), "\"a\\tb\"");
  EXPECT_EQ(json_text::EncodeJsonString(std::string("\x01\x1f")),
            "\"\\u0001\\u001f\"");
  EXPECT_EQ(json_text::EncodeJsonString(std::string("\0\x0e\x10\x1e", 4)),
            "\"\\u0000\\u000e\\u0010\\u001e\"");
}

TEST(JsonTextRenderer, RendersValuesAndArrays) {
  EXPECT_EQ(json_text::ToJsonText(Value(int64_t{42})), "42");
  EXPECT_EQ(json_text::ToJsonText(Value(1.5)), "1.5");
  EXPECT_EQ(json_text::ToJsonText(Value(std::string("hi"))), "\"hi\"");
  const Value array =
      Value::Array({Value(int64_t{1}), Value(std::string("a"))}, "INT64");
  EXPECT_EQ(json_text::ToJsonText(array), "[1,\"a\"]");
  // A struct-shaped VARCHAR payload passes through verbatim.
  EXPECT_EQ(json_text::ToJsonText(Value(std::string("{\"a\":1}"))),
            "{\"a\":1}");
}

TEST(JsonTextRenderer, RendersUnsignedMagnitudeWithoutWrapping) {
  const uint64_t above_int64 = uint64_t{1} << 63U;
  Value unsigned_value = Value(static_cast<int64_t>(above_int64));
  unsigned_value.SetUnsigned();
  EXPECT_EQ(json_text::ToJsonText(unsigned_value), "9223372036854775808");
}

}  // namespace tinylamb
