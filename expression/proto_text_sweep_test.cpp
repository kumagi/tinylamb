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
#include "type/date.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

// Fatal on a parse failure so a single parser regression does not also
// produce a confusing secondary "wrong entry count" failure at the caller.
// ASSERT_TRUE is only legal in a void function, hence the out-parameter.
void ParseOrDie(std::string_view body, std::vector<ProtoTextEntry>* entries) {
  ASSERT_TRUE(ParseProtoTextEntries(body, entries)) << "parse failed: " << body;
}

}  // namespace

TEST(ProtoTextParseTest, ScalarsMessagesAndSeparators) {
  std::vector<ProtoTextEntry> entries;
  ParseOrDie("a: 1 b: \"x\", c: 2.5; d: true", &entries);
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
  std::vector<ProtoTextEntry> entries;
  ParseOrDie("# leading comment\n  a: 1,  # trailing\n  b: 2", &entries);
  ASSERT_EQ(entries.size(), 2U);
  EXPECT_EQ(entries[0].name, "a");
  EXPECT_EQ(entries[1].name, "b");
}

TEST(ProtoTextParseTest, ExtensionEntriesKeepBracketedName) {
  std::vector<ProtoTextEntry> entries;
  ParseOrDie("[pkg.ext.field]: 7 a: 1", &entries);
  ASSERT_EQ(entries.size(), 2U);
  EXPECT_EQ(entries[0].name, "[pkg.ext.field]");
  EXPECT_EQ(entries[0].text, "7");
  EXPECT_EQ(entries[1].name, "a");
}

TEST(ProtoTextParseTest, MessageBodiesAcceptBracesAndAngleBrackets) {
  std::vector<ProtoTextEntry> braced;
  ParseOrDie("m { a: 1 b \"x\" } c: 2", &braced);
  ASSERT_EQ(braced.size(), 2U);
  EXPECT_TRUE(braced[0].is_message);
  EXPECT_EQ(braced[0].text, " a: 1 b \"x\" ");
  std::vector<ProtoTextEntry> angled;
  ParseOrDie("m < a: 1 > c: 2", &angled);
  ASSERT_EQ(angled.size(), 2U);
  EXPECT_TRUE(angled[0].is_message);
  EXPECT_EQ(angled[0].text, " a: 1 ");
}

TEST(ProtoTextParseTest, QuotedStringsMayContainBracesAndQuotes) {
  std::vector<ProtoTextEntry> entries;
  ParseOrDie(R"(s: "a } b" t: 'it\'s' u: "{ x: 1 }")", &entries);
  ASSERT_EQ(entries.size(), 3U);
  EXPECT_EQ(entries[0].text, "\"a } b\"");
  EXPECT_EQ(entries[2].text, "\"{ x: 1 }\"");
}

TEST(ProtoTextParseTest, NumericFieldNamesAreReserved) {
  std::vector<ProtoTextEntry> entries;
  ParseOrDie("2: 7 a: 1", &entries);
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
  for (std::string_view input : {"", "  \n\t "}) {
    SCOPED_TRACE(input);
    std::vector<ProtoTextEntry> entries;
    ASSERT_TRUE(ParseProtoTextEntries(input, &entries));
    EXPECT_TRUE(entries.empty());
  }
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
  assert(normalized.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(*normalized, "b: 2 a: 0 [x.y]: 1");

  // Normalization preserves field order inside message bodies (only
  // extension entries move after plain fields); it re-nests recursively.
  const auto nested = NormalizeProtoText("m { b: 2 a: 1 }");
  ASSERT_TRUE(nested.has_value());
  assert(nested.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(*nested, "m { b: 2 a: 1 }");

  const auto empty = NormalizeProtoText("   ");
  ASSERT_TRUE(empty.has_value());
  EXPECT_EQ(empty.value(), "");  // NOLINT(bugprone-unchecked-optional-access)

  EXPECT_FALSE(NormalizeProtoText("a:").has_value());
}

TEST(ProtoTextFormatTest, TimestampOffsetsAreAppliedBeforeEncodingFields) {
  constexpr int64_t k125Millis = 125'000'000;
  for (const char* text :
       {"1970-01-01T05:30:00.125+0530", "1970-01-01 05:30:00.125+05:30",
        "1969-12-31T21:00:00.125-03", "1970-01-01T00:00:00.125Z"}) {
    const auto nanos = ParseTimestampTextNanos(text);
    ASSERT_TRUE(nanos.has_value()) << text;
    EXPECT_TRUE(nanos.value() == k125Millis) << text;
  }

  const auto updated = TryProtoTextSetField(
      "seconds_default: 5", {"seconds_default"},
      Value("1970-01-01 05:30:00+0530"), "test.TimestampPB");
  ASSERT_TRUE(updated.HasValue()) << updated.GetStatus();
  EXPECT_EQ(updated.Value(), std::optional<std::string>("seconds_default: 0"));

  const auto constructed = TryConstructProtoText(
      "test.TimestampPB",
      {{"millis_default", Value("1970-01-01T02:00:00.125+02")}});
  ASSERT_TRUE(constructed.HasValue()) << constructed.GetStatus();
  EXPECT_NE(constructed.Value().find("millis_default: 125"), std::string::npos);

  for (const char* bad :
       {"1970-01-01T00:00:00+05:30:00", "1970-01-01T00:00:00+0560",
        "1970-01-01T00:00:00Zextra"}) {
    EXPECT_FALSE(ParseTimestampTextNanos(bad).has_value()) << bad;
    EXPECT_EQ(TryProtoTextSetField("seconds_default: 5", {"seconds_default"},
                                   Value(bad), "test.TimestampPB")
                  .GetStatus(),
              Status::kInvalidArgument);
    EXPECT_EQ(TryConstructProtoText("test.TimestampPB",
                                    {{"millis_default", Value(bad)}})
                  .GetStatus(),
              Status::kInvalidArgument);
  }
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
  // handled reads (true), and an unannotated absent scalar reads as the
  // INT64 zero default (GoogleSQL proto semantics: message-typed reads NULL,
  // scalar reads its type default). Pin the value too so a regression that
  // flips it to NULL cannot pass silently.
  auto absent = TryProtoTextExtractField("a: 1", "no_such_field", &out);
  ASSERT_TRUE(absent.HasValue());
  EXPECT_TRUE(absent.MoveValue());
  EXPECT_EQ(out, (Value(int64_t{0})));
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
  assert(updated.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(*updated, "a: 1 b: 9");

  // Append a new field at the end.
  auto appended =
      TryProtoTextSetField("a: 1", {"c"}, Value(int64_t{3}), "").MoveValue();
  ASSERT_TRUE(appended.has_value());
  assert(appended.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(*appended, "a: 1 c: 3");

  // A NULL clears the field.
  auto cleared =
      TryProtoTextSetField("a: 1 b: 2", {"a"}, Value(), "").MoveValue();
  ASSERT_TRUE(cleared.has_value());
  assert(cleared.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(*cleared, "b: 2");

  // Dotted paths reach into nested messages.
  auto nested = TryProtoTextSetField("m { inner: 1 }", {"m", "inner"},
                                     Value(int64_t{5}), "")
                    .MoveValue();
  ASSERT_TRUE(nested.has_value());
  assert(nested.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_NE(nested->find("inner: 5"), std::string::npos);
  Value out;
  assert(nested.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  ASSERT_TRUE(TryProtoTextExtractField(*nested, "m", &out).MoveValue());
  EXPECT_NE(out.AsString().find("inner: 5"), std::string::npos);

  // Empty path is a no-op.
  auto noop =
      TryProtoTextSetField("a: 1", {}, Value(int64_t{2}), "").MoveValue();
  EXPECT_FALSE(noop.has_value());
}

TEST(ProtoTextSetFieldTest, AuditMalformedPayloadNeverBecomesReplacement) {
  for (const std::string& text :
       {std::string("garbage$$$"), std::string("a: 1 garbage$$$"),
        std::string("m { garbage$$$ }")}) {
    for (const Value& replacement : {Value(9), Value()}) {
      auto result = TryProtoTextSetField(
          text,
          text.starts_with("m") ? std::vector<std::string>{"m", "a"}
                                : std::vector<std::string>{"a"},
          replacement, "");
      EXPECT_FALSE(result.HasValue()) << text;
      EXPECT_EQ(result.GetStatus(), Status::kInvalidArgument);
    }
  }
  auto empty = TryProtoTextSetField("", {"a"}, Value(9), "");
  ASSERT_TRUE(empty.HasValue());
  EXPECT_EQ(empty.Value(), std::optional<std::string>("a: 9"));
}

TEST(ProtoTextSetFieldTest, OuterBracesAreOptional) {
  auto updated = TryProtoTextSetField("{ a: 1 }", {"a"}, Value(int64_t{4}), "")
                     .MoveValue();
  ASSERT_TRUE(updated.has_value());
  assert(updated.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(*updated, "a: 4");
}

TEST(ProtoTextWriteTest, VarCharValuesQuotedEnumMembersStayBare) {
  // The proto TEXT writer must not decide bare-vs-quoted from token spelling
  // alone: a VARCHAR source is a string value.  Bare "true" read back as
  // bool 1, "007" as the number 7, and unquoted non-token text corrupted
  // the payload so the next write failed with malformed proto
  // (tracker #761).
  const std::string base = "int64_key_1: 1 int64_key_2: 2";
  const std::string type = "googlesql_test.KitchenSinkPB";
  for (const std::string& text :
       {std::string("true"), std::string("false"), std::string("007"),
        std::string("5 apples"), std::string("GREEN"), std::string("1.5"),
        std::string("hello world")}) {
    auto updated = TryProtoTextSetField(base, {"string_val"},
                                        Value(std::string(text)), type)
                       .MoveValue();
    ASSERT_TRUE(updated.has_value());
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    EXPECT_NE(updated->find("string_val: \""), std::string::npos) << *updated;
    Value out;
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    ASSERT_TRUE(
        TryProtoTextExtractField(*updated, "string_val", &out).MoveValue())
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer
        // assumes success
        << *updated;  // NOLINT(bugprone-unchecked-optional-access)
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer
    // assumes success
    EXPECT_EQ(out, Value(std::string(text)))
        << *updated;  // NOLINT(bugprone-unchecked-optional-access)
    // The rewritten payload stays writable.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer
    // assumes success
    auto again = TryProtoTextSetField(
                     *updated, {"int64_key_1"}, Value(int64_t{7}),
                     type)  // NOLINT(bugprone-unchecked-optional-access)
                     .MoveValue();
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer
    // assumes success
    ASSERT_TRUE(again.has_value())
        << *updated;  // NOLINT(bugprone-unchecked-optional-access)
  }

  // Known enum fields carry enum member names, which proto TEXT renders
  // bare.
  auto enum_set = TryProtoTextSetField(base, {"test_enum"},
                                       Value(std::string("TESTENUM1")), type)
                      .MoveValue();
  ASSERT_TRUE(enum_set.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_NE(enum_set->find("test_enum: TESTENUM1"), std::string::npos);
  Value enum_out;
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  ASSERT_TRUE(
      TryProtoTextExtractField(*enum_set, "test_enum", &enum_out).MoveValue());
  EXPECT_EQ(enum_out, (Value(std::string("TESTENUM1"))));

  // The NEW-constructor path follows the same rule: scalar strings and the
  // repeated str_value channel render quoted even when the spelling is
  // bool-, number- or identifier-shaped.
  auto constructed = TryConstructProtoText(
      "googlesql_test.TestExtraPB",
      {{"str_value",
        Value::Array({Value(std::string("true")), Value(std::string("GREEN")),
                      Value(std::string("007"))},
                     "STRING")}});
  ASSERT_TRUE(constructed.HasValue());
  EXPECT_EQ(constructed.Value(),
            "str_value: \"true\" str_value: \"GREEN\" str_value: \"007\"");
  auto scalar_constructed =
      TryConstructProtoText("googlesql_test.TestExtraPB",
                            {{"int32_val1", Value(std::string("true"))}});
  ASSERT_TRUE(scalar_constructed.HasValue());
  EXPECT_NE(scalar_constructed.Value().find("int32_val1: \"true\""),
            std::string::npos);
}

TEST(ProtoTextExtractFieldTest, DateDecimalConversionReturnsStatusNotThrow) {
  // Decimal-encoded (YYYYMMDD) dates with out-of-range month/day must
  // surface as an InvalidArgument Status across the Status boundary,
  // never as an exception escaping through the throwing ParseDateDays
  // wrapper (bugs2 P731 / tracker #762).
  for (const std::string& bad : {std::string("date_decimal: 20231301"),
                                 std::string("date_decimal: 20230230"),
                                 std::string("date_decimal: 20230099")}) {
    Value out;
    auto result = TryProtoTextExtractField(bad, "date_decimal", &out);
    EXPECT_FALSE(result.HasValue()) << bad;
    EXPECT_EQ(result.GetStatus(), Status::kInvalidArgument);
  }
  // Valid decimal dates still convert to DATE.
  Value out;
  ASSERT_TRUE(
      TryProtoTextExtractField("date_decimal: 20230228", "date_decimal", &out)
          .MoveValue());
  ASSERT_EQ(out.type, ValueType::kDate);
  EXPECT_EQ(out.DateDays(), TryParseDateDays("2023-02-28").MoveValue());
  // Decimal-encoded 0 keeps reading as NULL.
  Value null_out;
  ASSERT_TRUE(
      TryProtoTextExtractField("date_decimal: 0", "date_decimal", &null_out)
          .MoveValue());
  EXPECT_TRUE(null_out.IsNull());
}

// T3665: nested reads must follow the WHOLE path the way the SetField side
// descends; passing only the leaf name looked for a top-level field and
// missed anything inside a message (nested DML could write msg.a.b but the
// matching read never found it).
TEST(ProtoTextParseTest, GetFieldPathDescendsNestedMessages) {
  Value out;
  ASSERT_TRUE(
      TryProtoTextGetFieldPath("outer { val: 7 }", {"outer", "val"}, &out)
          .MoveValue());
  EXPECT_FALSE(out.IsNull());
  EXPECT_EQ(out, Value(int64_t{7}));
  // Repeated leaves merge across every matching parent (a write replaces
  // the leaf in EACH parent, so the read has to see all of them).
  Value merged;
  ASSERT_TRUE(TryProtoTextGetFieldPath(
                  "outer { repeated_num: 1 } outer { repeated_num: 2 }",
                  {"outer", "repeated_num"}, &merged)
                  .MoveValue());
  ASSERT_TRUE(merged.IsArray());
  EXPECT_EQ(merged.ArrayElements().size(), 2U);
  // An absent parent reads like an unset field (empty repeated array).
  Value absent;
  ASSERT_TRUE(
      TryProtoTextGetFieldPath("other: 1", {"outer", "repeated_num"}, &absent)
          .MoveValue());
  ASSERT_TRUE(absent.IsArray());
  EXPECT_TRUE(absent.ArrayElements().empty());
  // Single-segment paths keep the flat reader semantics.
  Value flat;
  ASSERT_TRUE(
      TryProtoTextGetFieldPath("repeated_num: 3", {"repeated_num"}, &flat)
          .MoveValue());
  EXPECT_TRUE(flat.IsArray());
}

}  // namespace tinylamb
