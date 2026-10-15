/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// Extra Value coverage beyond value_test.cpp: NULL vs empty-string
// distinctions, signed/unsigned boundary ordering in the memcomparable
// encoding, and double ordering across the sign boundary.
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

void ExpectMemcomparableRoundTrip(const std::vector<Value>& values) {
  for (const Value& value : values) {
    const std::string encoded = value.EncodeMemcomparableFormat();
    Value decoded;
    const size_t consumed = decoded.DecodeMemcomparableFormat(encoded);
    ASSERT_EQ(consumed, encoded.size());
    EXPECT_EQ(decoded, value);
  }
}

void ExpectMemcomparableOrder(const std::vector<Value>& ascending) {
  for (size_t i = 0; i + 1 < ascending.size(); ++i) {
    const std::string low = ascending[i].EncodeMemcomparableFormat();
    const std::string high = ascending[i + 1].EncodeMemcomparableFormat();
    ASSERT_LT(low, high) << ascending[i] << " must order below "
                         << ascending[i + 1];
  }
}

}  // namespace

TEST(ValueExtraTest, NullAndEmptyVarcharStayDistinctEverywhere) {
  const Value null_value;
  const Value empty_string("");
  ASSERT_TRUE(null_value.IsNull());
  ASSERT_FALSE(empty_string.IsNull());
  EXPECT_NE(null_value, empty_string);
  std::hash<Value> hasher;
  EXPECT_NE(hasher(null_value), hasher(empty_string));

  // Both survive the storage encodings without collapsing into each other.
  const std::string null_encoded = null_value.EncodeMemcomparableFormat();
  const std::string empty_encoded = empty_string.EncodeMemcomparableFormat();
  EXPECT_NE(null_encoded, empty_encoded);
  ExpectMemcomparableRoundTrip({null_value, empty_string});
  Value decoded_null;
  decoded_null.DecodeMemcomparableFormat(null_encoded);
  EXPECT_TRUE(decoded_null.IsNull());
  Value decoded_empty;
  decoded_empty.DecodeMemcomparableFormat(empty_encoded);
  EXPECT_FALSE(decoded_empty.IsNull());
  EXPECT_EQ(decoded_empty, Value(""));
}

TEST(ValueExtraTest, Int64SignBoundariesOrderInMemcomparableEncoding) {
  const std::vector<Value> ascending = {
      Value(std::numeric_limits<int64_t>::min()), Value(-1), Value(0), Value(1),
      Value(std::numeric_limits<int64_t>::max())};
  ExpectMemcomparableOrder(ascending);
  ExpectMemcomparableRoundTrip(ascending);
}

TEST(ValueExtraTest, UnsignedBoundariesOrderAboveEverySignedValue) {
  const std::vector<Value> ascending = {
      Value(std::numeric_limits<int64_t>::min()),
      Value(int64_t{-1}),
      Value(int64_t{0}),
      Value(std::numeric_limits<int64_t>::max()),
      Value(int64_t{0}).WithUnsigned(),
      Value(static_cast<int64_t>(1ULL << 63)).WithUnsigned(),
      Value(static_cast<int64_t>(std::numeric_limits<uint64_t>::max()))
          .WithUnsigned()};
  ExpectMemcomparableOrder(ascending);
  ExpectMemcomparableRoundTrip(ascending);
}

TEST(ValueExtraTest, DoubleOrderingCrossesTheSignBoundary) {
  const std::vector<Value> ascending = {
      Value(-std::numeric_limits<double>::infinity()),
      Value(std::numeric_limits<double>::lowest()),
      Value(-1.5),
      Value(-0.5),
      Value(0.0),
      Value(0.5),
      Value(1.5),
      Value(std::numeric_limits<double>::max()),
      Value(std::numeric_limits<double>::infinity())};
  ExpectMemcomparableOrder(ascending);
  ExpectMemcomparableRoundTrip(ascending);
}

TEST(ValueExtraTest, NegativeZeroEqualsPositiveZeroAfterRoundTrip) {
  const Value negative_zero(-0.0);
  const Value positive_zero(0.0);
  EXPECT_EQ(negative_zero, positive_zero);
  Value decoded;
  decoded.DecodeMemcomparableFormat(negative_zero.EncodeMemcomparableFormat());
  EXPECT_EQ(decoded, positive_zero);
}

TEST(ValueExtraTest, VarcharLengthBoundariesRoundTrip) {
  std::vector<Value> sizes;
  sizes.emplace_back("");
  sizes.emplace_back(std::string(1, 'a'));
  sizes.emplace_back(std::string(255, 'b'));
  sizes.emplace_back(std::string(256, 'c'));
  sizes.emplace_back(std::string(65536, 'd'));
  ExpectMemcomparableRoundTrip(sizes);
  ExpectMemcomparableOrder(sizes);
}

}  // namespace tinylamb
