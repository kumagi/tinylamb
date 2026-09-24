/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Unit sweep over the proto TEXT-format parser (expression/proto_text.cpp):
// entry scanning, plausibility checks, canonical normalization, scalar
// formatting, field extraction (including has_xxx pseudo fields, repeated
// fields and extensions), presence tests, and dotted-path field mutation.

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/status_or.hpp"
#include "expression/proto_text.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

std::vector<ProtoTextEntry> ParseOrDie(std::string_view body) {
  std::vector<ProtoTextEntry> entries;
  const bool ok = ParseProtoTextEntries(body, &entries);
  if (!ok) {
    ADD_FAILURE() << "parse failed: " << body;
    return {};
  }
  return entries;
}

}  // namespace

TEST(ProtoTextParseTest, ScalarsMessagesAndSeparators) {
  const auto entries = ParseOrDie("a: 1 b: \"x\", c: 2.5; d: true");
  ASSERT_EQ(entries.size(), 4U);
  EXPECT_EQ(entries[0].name, "a");
  EXPECT_FALSE(entries[0].is_message);
  EXPECT_EQ(entries[0].text, "1");
  EXPECT_EQ(entries[1].name, "b");
  EXPECT_EQ(entries[1].text, "\"x\"");
  EXPECT_EQ(entries[2].text, "2.5");
  EXPECT_EQ(entries[3].text, "true");
}

TEST(ProtoTextParseTest, CommentsAndWhitespaceAreSkipped) {
  const auto entries =
      ParseOrDie("# leading comment\n  a: 1,  # trailing\n  b: 2");
  ASSERT_EQ(entries.size(), 2U);
  EXPECT_EQ(entries[0].name, "a");
  EXPECT_EQ(entries[1].name, "b");
}

TEST(ProtoTextParseTest, ExtensionEntriesKeepBracketedName) {
  const auto entries = ParseOrDie("[pkg.ext.field]: 7 a: 1");
  ASSERT_EQ(entries.size(), 2U);
  EXPECT_EQ(entries[0].name, "[pkg.ext.field]");
  EXPECT_EQ(entries[0].text, "7");
  EXPECT_EQ(entries[1].name, "a");
}

TEST(ProtoTextParseTest, MessageBodiesAcceptBracesAndAngleBrackets) {
  const auto braced = ParseOrDie("m { a: 1 b \"x\" } c: 2");
  ASSERT_EQ(braced.size(), 2U);
  EXPECT_TRUE(braced[0].is_message);
  EXPECT_EQ(braced[0].text, " a: 1 b \"x\" ");
  const auto angled = ParseOrDie("m < a: 1 > c: 2");
  ASSERT_EQ(angled.size(), 2U);
  EXPECT_TRUE(angled[0].is_message);
  EXPECT_EQ(angled[0].text, " a: 1 ");
}

TEST(ProtoTextParseTest, QuotedStringsMayContainBracesAndQuotes) {
  const auto entries = ParseOrDie(R"(s: "a } b" t: 'it\'s' u: "{ x: 1 }")");
  ASSERT_EQ(entries.size(), 3U);
  EXPECT_EQ(entries[0].text, "\"a } b\"");
  EXPECT_EQ(entries[2].text, "\"{ x: 1 }\"");
}

TEST(ProtoTextParseTest, NumericFieldNamesAreReserved) {
  const auto entries = ParseOrDie("2: 7 a: 1");
  ASSERT_EQ(entries.size(), 2U);
  EXPECT_EQ(entries[0].name, "2");
  EXPECT_EQ(entries[0].text, "7");
}

TEST(ProtoTextParseTest, MalformedBodiesAreRejected) {
  const std::vector<std::string> bad = {
      "a:",                 // missing value
      ": 1",                // missing name
      "a: \"unterminated",  // unterminated string
      "m { a: 1",           // unterminated message
      "[ext.field: 1",      // unterminated extension
      "a: 1 @: 2",          // invalid name start
  };
  for (const std::string& body : bad) {
    SCOPED_TRACE(body);
    std::vector<ProtoTextEntry> entries;
    EXPECT_FALSE(ParseProtoTextEntries(body, &entries)) << body;
  }
}

TEST(ProtoTextParseTest, EmptyBodyParsesToNoEntries) {
  std::vector<ProtoTextEntry> entries;
  ASSERT_TRUE(ParseProtoTextEntries("", &entries));
  EXPECT_TRUE(entries.empty());
  ASSERT_TRUE(ParseProtoTextEntries("  \n\t ", &entries));
  EXPECT_TRUE(entries.empty());
}

TEST(ProtoTextPlausibilityTest, LooksLikeProtoTextClassifiesBodies) {
  EXPECT_TRUE(LooksLikeProtoText(""));
  EXPECT_TRUE(LooksLikeProtoText("   "));
  EXPECT_TRUE(LooksLikeProtoText("{ a: 1 }"));
  EXPECT_TRUE(LooksLikeProtoText("a: 1"));
  // Braces around nothing still denote an (empty) message body.
  EXPECT_TRUE(LooksLikeProtoText("{ }"));
  EXPECT_FALSE(LooksLikeProtoText("plain string"));
  EXPECT_FALSE(LooksLikeProtoText("12345"));
}

TEST(ProtoTextNormalizeTest, CanonicalizesSpacingAndOrdersExtensions) {
  const auto normalized = NormalizeProtoText("b: 2 [x.y]: 1 a: 0");
  ASSERT_TRUE(normalized.has_value());
  EXPECT_EQ(normalized.value(), "b: 2 a: 0 [x.y]: 1");  // NOLINT(bugprone-unchecked-optional-access)

  // Normalization preserves field order inside message bodies (only
  // extension entries move after plain fields); it re-nests recursively.
  const auto nested = NormalizeProtoText("m { b: 2 a: 1 }");
  ASSERT_TRUE(nested.has_value());
  EXPECT_EQ(nested.value(), "m { b: 2 a: 1 }");  // NOLINT(bugprone-unchecked-optional-access)

  const auto empty = NormalizeProtoText("   ");
  ASSERT_TRUE(empty.has_value());
  EXPECT_EQ(empty.value(), "");  // NOLINT(bugprone-unchecked-optional-access)

  EXPECT_FALSE(NormalizeProtoText("a:").has_value());
}

TEST(ProtoTextFormatTest, FormatScalarRendersCanonicalTokens) {
  EXPECT_EQ(FormatProtoTextScalar("true"), "true");
  EXPECT_EQ(FormatProtoTextScalar("false"), "false");
  EXPECT_EQ(FormatProtoTextScalar("  42  "), "42");
  EXPECT_EQ(FormatProtoTextScalar("'hi'"), "\"hi\"");
  EXPECT_EQ(FormatProtoTextScalar("\"a\\nb\""), "\"a\\nb\"");
  EXPECT_EQ(FormatProtoTextScalar(""), "\"\"");
}

TEST(ProtoTextExtractTest, ScalarsMessagesAndRepeatedReads) {
  Value out;
  ASSERT_TRUE(TryProtoTextExtractField("a: 1 b: \"x\"", "a", &out).MoveValue());
  EXPECT_EQ(out, (Value(int64_t{1})));

  ASSERT_TRUE(TryProtoTextExtractField("a: 1 b: \"x\"", "b", &out).MoveValue());
  EXPECT_EQ(out, (Value(std::string("x"))));

  // Repeated occurrences of one scalar field collapse into an array.
  ASSERT_TRUE(
      TryProtoTextExtractField("a: 1 a: 2 a: 3", "a", &out).MoveValue());
  ASSERT_TRUE(out.IsArray());
  EXPECT_EQ(out.ArrayElements().size(), 3U);

  // Message-valued entries project as text with braces; the inner body keeps
  // its original spacing.
  ASSERT_TRUE(
      TryProtoTextExtractField("m { a: 1 } c: 2", "m", &out).MoveValue());
  EXPECT_EQ(out, (Value(std::string("{  a: 1  }"))));

  // Case-insensitive key matching.
  ASSERT_TRUE(TryProtoTextExtractField("aB: 7", "ab", &out).MoveValue());
  EXPECT_EQ(out, (Value(int64_t{7})));
}

TEST(ProtoTextExtractTest, AbsentFieldsUseDefaultsOrNulls) {
  Value out;
  // Unset repeated fields read as empty arrays.
  ASSERT_TRUE(
      TryProtoTextExtractField("a: 1", "repeated_int64_val", &out).MoveValue());
  ASSERT_TRUE(out.IsArray());
  EXPECT_TRUE(out.ArrayElements().empty());

  // Unknown scalar fields without a known format annotation still count as
  // handled reads (true); the projected default depends on the name-based
  // format annotations, so only the handling contract is pinned here.
  auto absent = TryProtoTextExtractField("a: 1", "no_such_field", &out);
  ASSERT_TRUE(absent.HasValue());
  EXPECT_TRUE(absent.MoveValue());
}

TEST(ProtoTextExtractTest, HasPrefixReportsPresence) {
  Value out;
  ASSERT_TRUE(TryProtoTextExtractField("a: 1", "has_a", &out).MoveValue());
  EXPECT_EQ(out, (Value(int64_t{1})));
  ASSERT_TRUE(TryProtoTextExtractField("a: 1", "has_b", &out).MoveValue());
  EXPECT_EQ(out, (Value(int64_t{0})));
  // has_ on a repeated field name is rejected.
  EXPECT_FALSE(TryProtoTextExtractField("repeated_int64_val: 1",
                                        "has_repeated_int64_val", &out)
                   .HasValue());
}

TEST(ProtoTextExtractTest, ExtensionKeysResolveLeafNames) {
  Value out;
  ASSERT_TRUE(
      TryProtoTextExtractField("[p.e.f]: 9", "[p.e.f]", &out).MoveValue());
  EXPECT_EQ(out, (Value(int64_t{9})));
}

TEST(ProtoTextHasFieldTest, PresenceIsCaseInsensitive) {
  EXPECT_TRUE(ProtoTextHasField("alpha: 1", "alpha"));
  EXPECT_TRUE(ProtoTextHasField("ALPHA: 1", "alpha"));
  EXPECT_FALSE(ProtoTextHasField("alpha: 1", "beta"));
  EXPECT_TRUE(ProtoTextHasField("{ alpha: 1 }", "alpha"));
  EXPECT_FALSE(ProtoTextHasField("alpha: 1", ""));
}

TEST(ProtoTextSetFieldTest, UpdatesCreatesAndClears) {
  // Update an existing scalar in place.
  auto updated = TryProtoTextSetField("a: 1 b: 2", {"b"}, Value(int64_t{9}), "")
                     .MoveValue();
  ASSERT_TRUE(updated.has_value());
  EXPECT_EQ(updated.value(), "a: 1 b: 9");  // NOLINT(bugprone-unchecked-optional-access)

  // Append a new field at the end.
  auto appended =
      TryProtoTextSetField("a: 1", {"c"}, Value(int64_t{3}), "").MoveValue();
  ASSERT_TRUE(appended.has_value());
  EXPECT_EQ(appended.value(), "a: 1 c: 3");  // NOLINT(bugprone-unchecked-optional-access)

  // A NULL clears the field.
  auto cleared =
      TryProtoTextSetField("a: 1 b: 2", {"a"}, Value(), "").MoveValue();
  ASSERT_TRUE(cleared.has_value());
  EXPECT_EQ(cleared.value(), "b: 2");  // NOLINT(bugprone-unchecked-optional-access)

  // Dotted paths reach into nested messages.
  auto nested = TryProtoTextSetField("m { inner: 1 }", {"m", "inner"},
                                     Value(int64_t{5}), "")
                    .MoveValue();
  ASSERT_TRUE(nested.has_value());
  EXPECT_NE(nested.value().find("inner: 5"), std::string::npos);  // NOLINT(bugprone-unchecked-optional-access)
  Value out;
  ASSERT_TRUE(TryProtoTextExtractField(*nested, "m", &out).MoveValue());  // NOLINT(bugprone-unchecked-optional-access)
  EXPECT_NE(out.AsString().find("inner: 5"), std::string::npos);

  // Empty path is a no-op.
  auto noop =
      TryProtoTextSetField("a: 1", {}, Value(int64_t{2}), "").MoveValue();
  EXPECT_FALSE(noop.has_value());
}

TEST(ProtoTextSetFieldTest, OuterBracesAreOptional) {
  auto updated = TryProtoTextSetField("{ a: 1 }", {"a"}, Value(int64_t{4}), "")
                     .MoveValue();
  ASSERT_TRUE(updated.has_value());
  EXPECT_EQ(updated.value(), "a: 4");  // NOLINT(bugprone-unchecked-optional-access)
}

}  // namespace tinylamb
