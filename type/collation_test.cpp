/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "type/collation.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <string>
#include <vector>

#include "common/status_or.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

Value Ci(std::string text) {
  return Value(std::move(text))
      .WithCollation(static_cast<uint8_t>(Collation::kCaseInsensitive));
}

Value Bin(std::string text) {
  return Value(std::move(text))
      .WithCollation(static_cast<uint8_t>(Collation::kBinary));
}

}  // namespace

// ----------------------------------------------------------------- the tag ---

TEST(CollationTest, TagRoundTripsThroughValue) {
  EXPECT_EQ(CollationOf(Value("plain")), Collation::kNone);
  EXPECT_EQ(CollationOf(Bin("b")), Collation::kBinary);
  EXPECT_EQ(CollationOf(Ci("c")), Collation::kCaseInsensitive);
  // Tag 3 is the unsigned marker, not a collation, and an unknown tag
  // degrades to the engine default rather than to a bogus collator.
  EXPECT_TRUE(Value(static_cast<int64_t>(1)).WithUnsigned().IsUnsigned());
  EXPECT_EQ(CollationOf(Value(static_cast<int64_t>(1)).WithUnsigned()),
            Collation::kNone);
  EXPECT_EQ(CollationOf(static_cast<uint8_t>(9)), Collation::kNone);
}

TEST(CollationTest, NamesRoundTripAndSpecResolution) {
  EXPECT_EQ(CollationName(Collation::kNone), "");
  EXPECT_EQ(CollationName(Collation::kBinary), "binary");
  EXPECT_EQ(CollationName(Collation::kCaseInsensitive), "und:ci");
  EXPECT_EQ(CollationFromName(""), Collation::kNone);
  EXPECT_EQ(CollationFromName("binary"), Collation::kBinary);
  EXPECT_EQ(CollationFromName("BINARY"), Collation::kBinary);
  // Every non-binary spec folds: the engine has no locale collation, so an
  // unrecognised language name behaves as case-insensitive rather than
  // silently reordering text under a rule it does not implement.
  EXPECT_EQ(CollationFromName("und:ci"), Collation::kCaseInsensitive);
  EXPECT_EQ(CollationFromName("en_US"), Collation::kCaseInsensitive);
  EXPECT_EQ(CollationFromName("czech"), Collation::kCaseInsensitive);
}

TEST(CollationTest, ResolveCollationFoldsWhenEitherSideAsks) {
  EXPECT_EQ(ResolveCollation(Collation::kNone, Collation::kNone),
            Collation::kNone);
  EXPECT_EQ(ResolveCollation(Collation::kBinary, Collation::kNone),
            Collation::kBinary);
  // A case-insensitive side wins over an explicit binary side, matching the
  // `=` predicate (expression/binary_expression.cpp) so filtering and
  // ordering never disagree about peers.
  EXPECT_EQ(ResolveCollation(Collation::kCaseInsensitive, Collation::kBinary),
            Collation::kCaseInsensitive);
  EXPECT_EQ(ResolveCollation(Bin("x"), Ci("x")), Collation::kCaseInsensitive);
  EXPECT_EQ(ResolveCollation(Value("x"), Ci("x")), Collation::kCaseInsensitive);
  EXPECT_EQ(ResolveCollation(Value("x"), Value("y")), Collation::kNone);
}

// ------------------------------------------------------------------- fold ----

TEST(CollationTest, FoldIsAsciiOnlySoUtf8StaysByteOrdered) {
  EXPECT_EQ(FoldCaseForCollation("AbC-1"), "abc-1");
  EXPECT_EQ(FoldCaseForCollation(""), "");
  // Non-ASCII bytes are untouched: a multi-byte code point never contains a
  // byte in 'A'..'Z' in a well-formed UTF-8 sequence, so folding cannot
  // corrupt it and the UTF-8 byte order survives.
  const std::string greek = "\xD0\x93\xD1\x80";  // "Гр"
  const std::string latin1 = "\xC3\x89";         // "É"
  EXPECT_EQ(FoldCaseForCollation(greek), greek);
  EXPECT_EQ(FoldCaseForCollation(latin1), latin1);
  // Folding cannot reach those bytes, so the text is unchanged; only the
  // collator tag is added.
  const Value folded_greek =
      ApplyCollation(Value(std::string(greek)), Collation::kCaseInsensitive);
  EXPECT_EQ(folded_greek.value.varchar_value, greek);
  EXPECT_EQ(CollationOf(folded_greek), Collation::kCaseInsensitive);
  // A non-string or a binary collation is returned unchanged.
  EXPECT_EQ(ApplyCollation(Value(7), Collation::kCaseInsensitive), Value(7));
  EXPECT_EQ(ApplyCollation(Value("Keep"), Collation::kBinary), Bin("Keep"));
}

TEST(CollationTest, ApplyCollationKeepsTheTagForDownstreamChecks) {
  const Value folded = ApplyCollation(Ci("ABC"), Collation::kCaseInsensitive);
  EXPECT_EQ(folded.type, ValueType::kVarChar);
  EXPECT_EQ(folded.value.varchar_value, "abc");
  EXPECT_EQ(CollationOf(folded), Collation::kCaseInsensitive);
  // A value with no tag of its own gains one, so the folded text still
  // reports a collator to LIKE validation and grouping.
  const Value tagged =
      ApplyCollation(Value("ABC"), Collation::kCaseInsensitive);
  EXPECT_EQ(CollationOf(tagged), Collation::kCaseInsensitive);
}

// ------------------------------------------------------------- comparison ----

TEST(CollationTest, BinaryOrderIsUtf8ByteOrderAndUnchanged) {
  // 'B' (0x42) before 'a' (0x61) under the engine default.
  EXPECT_LT(CompareWithCollation(Value("B"), Value("a")), 0);
  EXPECT_LT(CompareForOrderBy(Value("B"), Value("a")), 0);
  EXPECT_GT(CompareWithCollation(Bin("a"), Bin("B")), 0);
  // An explicit binary collator on one side resolves for the whole
  // comparison, so the untagged side is compared byte-wise against it.
  EXPECT_LT(CompareWithCollation(Bin("B"), Value("a")), 0);
  EXPECT_LT(CompareWithCollation(Value("B"), Bin("a")), 0);
}

TEST(CollationTest, CaseInsensitiveOrderIsCaseAndAccentBlindWithinAscii) {
  EXPECT_EQ(CompareWithCollation(Ci("abc"), Ci("ABC")), 0);
  EXPECT_EQ(CompareWithCollation(Ci("AbC"), Ci("aBc")), 0);
  EXPECT_LT(CompareWithCollation(Ci("a"), Ci("B")), 0);
  EXPECT_GT(CompareWithCollation(Ci("B"), Ci("a")), 0);
  // A collator on either side governs the whole comparison.
  EXPECT_EQ(CompareWithCollation(Ci("abc"), Value("ABC")), 0);
  EXPECT_EQ(CompareWithCollation(Value("abc"), Ci("ABC")), 0);
  // Case-insensitive order is NOT binary order: this is exactly why an
  // ORDER BY under one collation may not be replaced by the other.
  // CompareForOrderBy is the binary comparison and ignores the tag.
  EXPECT_GT(CompareForOrderBy(Ci("a"), Ci("B")), 0);
  EXPECT_LT(CompareWithCollation(Ci("a"), Ci("B")), 0);
}

TEST(CollationTest, NonAsciiAndIntervalTextKeepTheirExistingOrder) {
  const Value greek = Value(std::string("\xD0\x93"));  // "Г"
  const Value latin = Value(std::string("\xC5"));      // first byte of "Å"
  const Value ascii_upper = Value("Z");                // 0x5A
  // The fold cannot reach these bytes, so every collator produces the same
  // (byte) order as CompareForOrderBy.
  for (const Collation collation :
       {Collation::kNone, Collation::kBinary, Collation::kCaseInsensitive}) {
    const int expected = CompareForOrderBy(ascii_upper, greek);
    EXPECT_EQ(CompareWithCollation(ApplyCollation(ascii_upper, collation),
                                   ApplyCollation(greek, collation)),
              expected);
    EXPECT_EQ(CompareWithCollation(ApplyCollation(greek, collation),
                                   ApplyCollation(latin, collation)),
              CompareForOrderBy(greek, latin));
  }
  // Interval-shaped text sorts byte-wise (as ORDER BY always has), and a
  // case-insensitive key only folds the case of that rendering.
  const Value year = Value("1 YEAR 2 MONTHS");
  const Value months = Value("12 MONTHS");
  EXPECT_LT(CompareWithCollation(year, months), 0);
  EXPECT_LT(CompareWithCollation(Ci("1 year 2 months"), Ci("12 months")), 0);
  // A collator on a non-VARCHAR value changes nothing.
  EXPECT_EQ(CompareWithCollation(Value(static_cast<int64_t>(1)).WithUnsigned(),
                                 Value(static_cast<int64_t>(-1))),
            CompareForOrderBy(Value(static_cast<int64_t>(1)).WithUnsigned(),
                              Value(static_cast<int64_t>(-1))));
}

TEST(CollationTest, CollatedEnumStringsFallBackToByteOrder) {
  // The compliance catalog ranks TestEnum members by a fixed table. That
  // ranking is byte-based, so it survives the binary collation and is
  // deliberately bypassed once the text is folded (folding would miss every
  // literal).
  EXPECT_LT(CompareWithCollation(Value("TESTENUMNEGATIVE"), Value("TESTENUM0")),
            0);
  // Once folded, the same member in another case is a peer: the fixed
  // ranking no longer applies and plain folded byte order takes over.
  EXPECT_EQ(CompareWithCollation(Ci("TESTENUM0"), Ci("testenum0")), 0);
  EXPECT_LT(CompareWithCollation(Ci("TESTENUM0"), Ci("testenum1")), 0);
}

TEST(CollationTest, CompareWithCollationIsATotalOrder) {
  std::vector<Value> values{Value(),    Value(1),   Value(-3),
                            Value(2.5), Value("a"), Value("B"),
                            Ci("b"),    Value(""),  Value::Date("2026-01-01")};
  for (const Value& a : values) {
    for (const Value& b : values) {
      const int ab = CompareWithCollation(a, b);
      EXPECT_EQ(ab, -CompareWithCollation(b, a));
      EXPECT_EQ(ab == 0, CompareWithCollation(b, a) == 0);
    }
  }
}

TEST(CollationTest, EqualUnderCollationKeepsValueEqualitySemantics) {
  EXPECT_TRUE(EqualUnderCollation(Ci("abc"), Ci("ABC")));
  EXPECT_TRUE(EqualUnderCollation(Ci("abc"), Value("ABC")));
  EXPECT_FALSE(EqualUnderCollation(Ci("abc"), Ci("abd")));
  EXPECT_FALSE(EqualUnderCollation(Value("abc"), Value("ABC")));
  // NULL is equal to NULL and to nothing else.
  EXPECT_TRUE(EqualUnderCollation(Value(), Value()));
  EXPECT_FALSE(EqualUnderCollation(Value(), Value("a")));
  // NaN, -0.0 and interval equivalence keep Value's semantics.
  const Value nan = Value(std::numeric_limits<double>::quiet_NaN());
  EXPECT_TRUE(EqualUnderCollation(nan, nan));
  EXPECT_TRUE(EqualUnderCollation(Value(0.0), Value(-0.0)));
  EXPECT_TRUE(
      EqualUnderCollation(Value("2014-1 0 0 0"), Value("2013-13 0 0 0")));
  // ... and a case-insensitive interval pair folds like any other text.
  EXPECT_TRUE(EqualUnderCollation(Ci("1 Year"), Ci("1 year")));
}

// ------------------------------------------------------- SortKeyOrder --------

TEST(CollationTest, SortKeyOrderPlacesNullsPerTheResolvedDefault) {
  const SortKeyOrder asc{true};
  EXPECT_TRUE(asc.NullsFirst());
  EXPECT_EQ(asc.Compare(Value(), Value("a")), -1);
  EXPECT_EQ(asc.Compare(Value(), Value()), 0);
  const SortKeyOrder desc{false};
  EXPECT_FALSE(desc.NullsFirst());
  EXPECT_EQ(desc.Compare(Value(), Value("a")), 1);
  // An explicit request overrides the ASC/DESC-derived default.
  const SortKeyOrder asc_nulls_last{true, false};
  EXPECT_EQ(asc_nulls_last.Compare(Value(), Value("a")), 1);
  const SortKeyOrder desc_nulls_first{false, true};
  EXPECT_EQ(desc_nulls_first.Compare(Value(), Value("a")), -1);
}

TEST(CollationTest, SortKeyOrderDescendsNonNullOperandsExactly) {
  const SortKeyOrder asc{true, true, Collation::kCaseInsensitive};
  const SortKeyOrder desc{false, true, Collation::kCaseInsensitive};
  const std::vector<Value> values{Value("a"), Value("B"), Ci("c"), Ci("A")};
  for (const Value& a : values) {
    for (const Value& b : values) {
      EXPECT_EQ(asc.Compare(a, b), -desc.Compare(a, b));
    }
  }
  // NULL placement is independent of the direction (NULLS FIRST stays first
  // under DESC unless the query asked otherwise), so the reversal above
  // deliberately does not extend to NULL.
  EXPECT_EQ(asc.Compare(Value(), Value("a")), -1);
  EXPECT_EQ(desc.Compare(Value(), Value("a")), -1);
  const SortKeyOrder desc_nulls_last{false, false, Collation::kNone};
  EXPECT_EQ(desc_nulls_last.Compare(Value(), Value("a")), 1);
}

TEST(CollationTest, SortKeyOrderResolvesKeyAndValueCollationsTogether) {
  // A bare key with the collator on the key...
  const SortKeyOrder from_key{true, std::nullopt, Collation::kCaseInsensitive};
  EXPECT_LT(from_key.Compare(Value("a"), Value("B")), 0);
  // ... and the same key with the collator only on the value agree.
  const SortKeyOrder from_value{true};
  EXPECT_EQ(from_value.Compare(Ci("a"), Ci("B")),
            from_key.Compare(Value("a"), Value("B")));
  // A binary key keeps byte order for two untagged operands, but an operand
  // that carries a case-insensitive tag still resolves for the whole
  // comparison, exactly as it does for the `=` predicate.
  const SortKeyOrder binary{true, std::nullopt, Collation::kBinary};
  EXPECT_LT(binary.Compare(Value("B"), Value("a")), 0);
  EXPECT_LT(binary.Compare(Ci("a"), Ci("B")), 0);
  EXPECT_EQ(binary.Compare(Ci("a"), Value("A")), 0);
  EXPECT_FALSE(binary.Precedes(Value("a"), Ci("A")));
}

TEST(CollationTest, MakeSortKeyOrderMatchesAnyKeyShape) {
  // Any sort key struct exposing (ascending, nulls_first, collation) builds
  // the same contract, so SortExecutor::Key, TopNExecutor::Key and the plan
  // key structs cannot diverge on the NULL default or the direction.
  struct SortKeyLike {
    bool ascending{true};
    std::optional<bool> nulls_first{std::nullopt};
    Collation collation{Collation::kNone};
  };
  const SortKeyLike key{.ascending = false,
                        .nulls_first = std::nullopt,
                        .collation = Collation::kCaseInsensitive};
  const SortKeyOrder order = MakeSortKeyOrder(key);
  EXPECT_FALSE(order.Ascending());
  EXPECT_FALSE(order.NullsFirst());
  EXPECT_EQ(order.KeyCollation(), Collation::kCaseInsensitive);
  // DESC: the fold still applies, only the direction is mirrored.
  EXPECT_GT(order.Compare(Ci("a"), Ci("B")), 0);
  EXPECT_LT(order.Compare(Ci("B"), Ci("a")), 0);
}

}  // namespace tinylamb
