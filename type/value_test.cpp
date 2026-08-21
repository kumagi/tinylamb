/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "type/value.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "gtest/gtest.h"
#include "type/date.hpp"

namespace tinylamb {
TEST(ValueTest, DefaultConstruct) {
  // Arrange -- nothing more than default Value ctor
  // Act -- default-construct a Value
  Value v;
  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST(ValueTest, Calculate) {
  // Arrange -- three integer Values to exercise arithmetic
  // Act + Assert -- equality and arithmetic semantics
  ASSERT_EQ(Value(1), Value(1));
  ASSERT_EQ(Value(2) + Value(3), Value(5));
  ASSERT_EQ(Value(3) - Value(4), Value(-1));
}

void SerializeDeserializeTest(const Value& v) {
  std::string buff;
  const size_t estimated_footprint = v.Size();
  buff.resize(estimated_footprint);
  ASSERT_EQ(estimated_footprint, v.Serialize(buff.data()));
  Value another;
  ASSERT_NE(v, another);
  another.Deserialize(buff.data(), v.type);
  ASSERT_EQ(v, another);
}

TEST(ValueTest, SerializeDesrialize) {
  // Arrange -- four Values of different types for round-trip serdes
  // Act + Assert -- SerializeDeserializeTest asserts equality after round-trip
  SerializeDeserializeTest(Value(1));
  SerializeDeserializeTest(Value(301L));
  SerializeDeserializeTest(Value("hello"));
  SerializeDeserializeTest(Value(439.3));
}

TEST(ValueTest, SerializeRoundTripCoversDateNullAndBinaryVarchar) {
  // The core round-trip only exercises int/double/varchar; these remaining
  // shapes must also survive Serialize/Deserialize unchanged so rows, indexes,
  // and WAL records can carry them losslessly.  (A NULL value is serialized
  // but Deserialize(NULL) is deliberately rejected, so it is not round-tripped
  // here.)
  SerializeDeserializeTest(Value::Date("2020-01-02"));
  SerializeDeserializeTest(Value(""));
  const std::string binary("\x00\x01\xff embedded \x00 nulls", 20);
  SerializeDeserializeTest(Value(std::string(binary)));
  const std::string long_text(1000, 'x');
  SerializeDeserializeTest(Value(std::string(long_text)));
}

TEST(ValueTest, Compare) {
  // Arrange -- three Values of different types for inequality comparison
  // Act + Assert -- < semantics across int, double, varchar
  ASSERT_TRUE(Value(1) < Value(2));
  ASSERT_TRUE(Value(-123.0) < Value(23.0));
  ASSERT_TRUE(Value("abc") < Value("d"));
}

TEST(ValueTest, VarcharCopiesOwnTheReferencedBytes) {
  std::string backing = "statistics-boundary";
  Value borrowed;
  borrowed.type = ValueType::kVarChar;
  borrowed.value.varchar_value = backing;

  Value copied = borrowed;
  backing.assign("overwritten");

  EXPECT_EQ(copied.value.varchar_value, "statistics-boundary");
}

TEST(ValueTest, Dump) {
  // Arrange -- six Values of different types/levels for LOG streaming
  // Act -- stream each Value to LOG at various levels (no assertion;
  // output-only)
  LOG(TRACE) << Value(12);
  LOG(DEBUG) << Value(120214143342323);
  LOG(INFO) << Value("foo-bar");
  LOG(WARN) << Value(1.23e3);
  LOG(ERROR) << Value();
  LOG(FATAL) << Value("foo");
  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

void MemcomparableFormatEncodeTest(const std::vector<Value>& input) {
  std::vector<Value> values(input);
  std::sort(values.begin(), values.end());
  std::vector<std::string> encoded;
  encoded.reserve(values.size());
  for (const auto& v : values) {
    encoded.push_back(v.EncodeMemcomparableFormat());
  }
  for (size_t i = 0; i < encoded.size(); ++i) {
    for (size_t j = i + 1; j < encoded.size(); ++j) {
      EXPECT_LT(encoded[i], encoded[j]);
    }
  }
}

TEST(ValueTest, MemcomparableOrderInt) {
  // Arrange -- three int Value sets: ascending, descending, and extreme values
  // Act + Assert -- MemcomparableFormatEncodeTest macro asserts sorted encoding
  // order
  MemcomparableFormatEncodeTest({Value(1), Value(2), Value(3)});
  MemcomparableFormatEncodeTest({Value(-1), Value(-2), Value(-3)});
  MemcomparableFormatEncodeTest({Value(std::numeric_limits<int64_t>::max()),
                                 Value(std::numeric_limits<int64_t>::min()),
                                 Value(1), Value(0), Value(-1)});
}

TEST(ValueTest, MemcomparableVarchar) {
  // Arrange -- 13 varchar Values of increasing length and one binary value
  // Act + Assert -- EncodeMemcomparableFormat yields expected prefix-tagged
  // bytes
  EXPECT_EQ(Value("a").EncodeMemcomparableFormat(),
            std::string(
                {'\2', 'a', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\x01'}));
  EXPECT_EQ(Value("ab").EncodeMemcomparableFormat(),
            std::string(
                {'\2', 'a', 'b', '\0', '\0', '\0', '\0', '\0', '\0', '\x02'}));
  EXPECT_EQ(
      Value("abc").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', '\0', '\0', '\0', '\0', '\0', '\x03'}));
  EXPECT_EQ(
      Value("abc").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', '\0', '\0', '\0', '\0', '\0', '\x03'}));
  EXPECT_EQ(
      Value("abcd").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', '\0', '\0', '\0', '\0', '\x04'}));
  EXPECT_EQ(
      Value("abcde").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', 'e', '\0', '\0', '\0', '\x05'}));
  EXPECT_EQ(
      Value("abcdef").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', 'e', 'f', '\0', '\0', '\x06'}));
  EXPECT_EQ(
      Value("abcdefg").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', 'e', 'f', 'g', '\0', '\x07'}));
  EXPECT_EQ(
      Value("abcdefgh").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', '\x08'}));
  EXPECT_EQ(Value("abcdefghi").EncodeMemcomparableFormat(),
            std::string({'\2', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', '\x09',
                         'i', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\1'}));
  EXPECT_EQ(
      Value("abcdefghij").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', '\x09', 'i',
                   'j', '\0', '\0', '\0', '\0', '\0', '\0', '\x02'}));
  EXPECT_EQ(
      Value("\x60\x70\x10\x11\x12\x80\x90\x01").EncodeMemcomparableFormat(),
      std::string("\x02\x60\x70\x10\x11\x12\x80\x90\x01\x08"));
}

TEST(ValueTest, MemcomparableOrderVarchar) {
  // Arrange -- three varchar Value sets: prefixes, alphabet, long strings
  // Act + Assert -- MemcomparableFormatEncodeTest asserts sorted encoding order
  MemcomparableFormatEncodeTest({Value("a"), Value("aa"), Value("aaa")});
  MemcomparableFormatEncodeTest({Value("a"), Value("b"), Value("c")});
  MemcomparableFormatEncodeTest(
      {Value("blah,blah,blah"), Value("this is a pen"), Value("0123456789")});
}

TEST(ValueTest, MemcomparableDouble) {
  // Arrange -- three double Values: positive, zero, negative
  // Act + Assert -- EncodeMemcomparableFormat yields expected byte sequences
  EXPECT_EQ(
      Value(1.0).EncodeMemcomparableFormat(),
      std::string({'\3', '\xbf', '\xf0', '\0', '\0', '\0', '\0', '\0', '\0'}));
  EXPECT_EQ(Value(0.0).EncodeMemcomparableFormat(),
            std::string(
                {'\3', '\x80', '\x00', '\0', '\0', '\0', '\0', '\0', '\x00'}));
  EXPECT_EQ(Value(-1.0).EncodeMemcomparableFormat(),
            std::string({'\3', '\x40', '\x0F', '\xff', '\xff', '\xff', '\xff',
                         '\xff', '\xff'}));
}

TEST(ValueTest, MemcomparableOrderDouble) {
  // Arrange -- four double Value sets: ascending, descending, signed, extreme
  // Act + Assert -- MemcomparableFormatEncodeTest asserts sorted encoding order
  MemcomparableFormatEncodeTest({Value(1.0), Value(2.0), Value(3.0)});
  MemcomparableFormatEncodeTest({Value(-1.0), Value(-2.0), Value(-3.0)});
  MemcomparableFormatEncodeTest({Value(-1.0), Value(0.0), Value(1.0)});
  MemcomparableFormatEncodeTest({Value(std::numeric_limits<double>::max()),
                                 Value(std::numeric_limits<double>::min()),
                                 Value(-1.0), Value(0.0), Value(1.0)});
}

void EncodeDecodeTest(const Value& v) {
  std::string encoded = v.EncodeMemcomparableFormat();
  Value another;
  const char* src = encoded.c_str();

  another.DecodeMemcomparableFormat(src);
  ASSERT_EQ(v, another);
}

TEST(ValueTest, EncodeDecodeInt) {
  // Arrange -- five int Values: max, 12, 0, -1, min
  // Act + Assert -- EncodeDecodeTest asserts round-trip equality
  EncodeDecodeTest(Value(std::numeric_limits<int64_t>::max()));
  EncodeDecodeTest(Value(12));
  EncodeDecodeTest(Value(0));
  EncodeDecodeTest(Value(-1));
  EncodeDecodeTest(Value(std::numeric_limits<int64_t>::min()));
}

TEST(ValueTest, EncodeDecodeVarchar) {
  // Arrange -- 9 varchar Values: short, empty, long, binary, hex
  // Act + Assert -- EncodeDecodeTest asserts round-trip equality
  EncodeDecodeTest(Value("a"));
  EncodeDecodeTest(Value(""));
  EncodeDecodeTest(Value("hello"));
  EncodeDecodeTest(Value("A bit long string"));
  EncodeDecodeTest(Value("12345678"));
  EncodeDecodeTest(Value("\x50\x60\x70\x10\x11\x12\x80\x02\x01"));
  EncodeDecodeTest(Value("\x60\x70\x10\x11\x12\x80\x90\x08"));
  EncodeDecodeTest(Value("\x60\x70\x10\x11\x12\x90\x80\x08"));
  EncodeDecodeTest(Value("49p2u3po32u423pori2pouropiu"));
}

TEST(ValueTest, EncodeDecodeDouble) {
  // Arrange -- five double Values: max, 12.0, 0.0, -1.0, min
  // Act + Assert -- EncodeDecodeTest asserts round-trip equality
  EncodeDecodeTest(Value(std::numeric_limits<double>::max()));
  EncodeDecodeTest(Value(12.0));
  EncodeDecodeTest(Value(0.0));
  EncodeDecodeTest(Value(-1.0));
  EncodeDecodeTest(Value(std::numeric_limits<double>::min()));
}

void MemcomparableFormatDecodeTest(const std::vector<std::string>& input) {
  std::vector<std::string> values(input);
  std::sort(values.begin(), values.end());
  std::vector<Value> decoded;
  decoded.reserve(values.size());
  for (const auto& value : values) {
    Value v;
    v.DecodeMemcomparableFormat(value.c_str());
    decoded.push_back(v);
  }
  for (size_t i = 0; i < decoded.size(); ++i) {
    for (size_t j = i + 1; j < decoded.size(); ++j) {
      ASSERT_LT(decoded[i], decoded[j]);
    }
  }
}

TEST(ValueTest, MemComparableFormatDecodeInt) {
  // Arrange -- 7-byte source string and its 5040 permutations as targets
  std::string src = "\x60\x70\x80\x90\x10\x11\x12";
  ASSERT_EQ(src.size(), 7);
  std::vector<std::string> targets;
  do {
    targets.emplace_back("\x01" + src + "\x01");
  } while (std::next_permutation(src.begin(), src.end()));

  // Act + Assert -- MemcomparableFormatDecodeTest asserts decoded Values are
  // strictly ascending
  MemcomparableFormatDecodeTest(targets);
}

TEST(ValueTest, MemComparableFormatDecodeVarchar) {
  // Arrange -- 7-byte source string and its 5040 permutations as
  // varchar-prefixed targets
  std::string src = "\x60\x70\x80\x90\x10\x11\x12";
  ASSERT_EQ(src.size(), 7);
  std::vector<std::string> targets;
  do {
    std::string v = "\x02" + src;
    v.push_back(0);
    v.push_back(static_cast<char>(v.size() - 1));
    ASSERT_EQ(v.size(), 10);
    targets.push_back(v);
  } while (std::next_permutation(src.begin(), src.end()));

  // Act + Assert -- MemcomparableFormatDecodeTest asserts decoded Values are
  // strictly ascending
  MemcomparableFormatDecodeTest(targets);
}

TEST(ValueTest, MemComparableFormatDecodeDouble) {
  // Arrange -- 7-byte source string and its 5040 permutations as
  // double-prefixed targets
  std::string src = "\x60\x70\x80\x90\x10\x11\x12";
  std::vector<std::string> targets;
  do {
    targets.emplace_back("\x03" + src + "\x01");
  } while (std::next_permutation(src.begin(), src.end()));

  // Act + Assert -- MemcomparableFormatDecodeTest asserts decoded Values are
  // strictly ascending
  MemcomparableFormatDecodeTest(targets);
}

TEST(ValueTest, UnaryAndAggregationToString) {
  // Arrange + Act -- stream every UnaryOperation and AggregationType value
  std::ostringstream unary;
  unary << UnaryOperation::kIsNull << "|" << UnaryOperation::kIsNotNull << "|"
        << UnaryOperation::kNot << "|" << UnaryOperation::kMinus << "|"
        << static_cast<UnaryOperation>(99);
  // Assert -- every enum member has a textual name, unknown falls back
  ASSERT_EQ(unary.str(), "IS NULL|IS NOT NULL|NOT|-|UNKNOWN");

  std::ostringstream agg;
  agg << AggregationType::kCount << "|" << AggregationType::kSum << "|"
      << AggregationType::kAvg << "|" << AggregationType::kMin << "|"
      << AggregationType::kMax << "|" << static_cast<AggregationType>(99);
  ASSERT_EQ(agg.str(), "COUNT|SUM|AVG|MIN|MAX|UNKNOWN");
}

TEST(ValueTest, NullSizeAndSerialize) {
  // Arrange -- a default (null) Value
  Value v;
  char buffer[4] = {0};
  // Act + Assert -- null has a 1-byte serialized footprint
  ASSERT_EQ(v.Size(), 1);
  ASSERT_EQ(v.Serialize(buffer), 1);
  // Act -- deserializing as kNull requires an explicit type and is rejected
  Value restored;
  ASSERT_THROW(restored.Deserialize(buffer, ValueType::kNull),
               std::runtime_error);
}

TEST(ValueTest, AsStringVariants) {
  // Arrange -- one value per type
  // Act + Assert -- AsString returns the documented formatting
  ASSERT_EQ(Value().AsString(), "(unknown type)");
  ASSERT_EQ(Value(42).AsString(), "42");
  ASSERT_EQ(Value("x").AsString(), "\"x\"");
  ASSERT_EQ(Value(1.5).AsString(), "1.500000");
  std::ostringstream oss;
  oss << Value(7);
  ASSERT_EQ(oss.str(), "7");
}

TEST(ValueTest, VarcharConcatenation) {
  // Arrange + Act -- concatenate two varchar values
  ASSERT_EQ(Value("foo") + Value("bar"), Value("foobar"));
  // Assert -- concatenating incompatible types throws
  ASSERT_THROW(Value(1) + Value("x"), std::runtime_error);
  ASSERT_THROW(Value(Value::DateFromDays(1)) + Value(Value::DateFromDays(2)),
               std::runtime_error);
}

TEST(ValueTest, DoubleArithmetic) {
  // Arrange + Act -- arithmetic on double values
  // Assert -- results match IEEE double expectations
  ASSERT_EQ(Value(3.0) + Value(1.5), Value(4.5));
  ASSERT_EQ(Value(3.0) - Value(1.0), Value(2.0));
  ASSERT_EQ(Value(2.0) * Value(3.0), Value(6.0));
  ASSERT_EQ(Value(6.0) / Value(2.0), Value(3.0));
  ASSERT_EQ(Value(7) / Value(2), Value(3));
}

TEST(ValueTest, ModuloAndBitwise) {
  // Arrange + Act -- % and bitwise ops on integers
  // Assert -- results match integer semantics
  ASSERT_EQ(Value(7) % Value(3), Value(1));
  ASSERT_EQ(Value(6) & Value(3), Value(2));
  ASSERT_EQ(Value(4) | Value(1), Value(5));
  ASSERT_EQ(Value(6) ^ Value(3), Value(5));
  // Assert -- applying them to doubles throws
  ASSERT_THROW(Value(1.0) % Value(2.0), std::runtime_error);
  ASSERT_THROW(Value(1.0) & Value(2.0), std::runtime_error);
  ASSERT_THROW(Value(1.0) | Value(2.0), std::runtime_error);
  ASSERT_THROW(Value(1.0) ^ Value(2.0), std::runtime_error);
}

TEST(ValueTest, DateRoundTrip) {
  // Arrange -- a date value parsed from an ISO string
  Value date = Value::Date("2020-01-02");
  ASSERT_EQ(date.type, ValueType::kDate);
  // Act -- encode, decode, and re-extract the day count
  std::string encoded = date.EncodeMemcomparableFormat();
  Value decoded;
  decoded.DecodeMemcomparableFormat(encoded.c_str());
  // Assert -- round trip preserves the date and its days
  ASSERT_EQ(decoded, date);
  ASSERT_EQ(decoded.DateDays(), date.DateDays());
  ASSERT_EQ(Value::DateFromDays(date.DateDays()), date);
}

TEST(ValueTest, HashValues) {
  // Arrange -- values of every concrete type
  std::hash<Value> hasher;
  // Act -- hash each value
  // Assert -- hashing succeeds and distinct values hash differently
  ASSERT_NE(hasher(Value(1)), hasher(Value(2)));
  ASSERT_NE(hasher(Value("a")), hasher(Value("b")));
  ASSERT_NE(hasher(Value(1.0)), hasher(Value(2.0)));
  ASSERT_EQ(hasher(Value("a")), hasher(Value("a")));
  ASSERT_EQ(hasher(Value(1)), hasher(Value(1)));
  ASSERT_EQ(hasher(Value(1.5)), hasher(Value(1.5)));
}

TEST(ValueTest, Truthy) {
  // Arrange + Act + Assert -- Truthy() distinguishes zero from non-zero ints
  ASSERT_FALSE(Value(0).Truthy());
  ASSERT_TRUE(Value(1).Truthy());
  ASSERT_TRUE(Value(-1).Truthy());
  // Assert -- non-int values are always truthy
  ASSERT_TRUE(Value("").Truthy());
  ASSERT_TRUE(Value(0.0).Truthy());
}

TEST(ValueTest, OrderedComparisonOperators) {
  // Arrange + Act + Assert -- <=, >=, != follow the same ordering as < and >
  ASSERT_TRUE(Value(1) <= Value(1));
  ASSERT_TRUE(Value(1) >= Value(1));
  ASSERT_TRUE(Value(1) != Value(2));
  ASSERT_FALSE(Value(1) > Value(1));
  ASSERT_FALSE(Value(1) < Value(1));
  ASSERT_TRUE(Value(2) > Value(1));
  ASSERT_TRUE(Value(1) <= Value(2));
}

TEST(DateTest, ParseRejectsMalformedDates) {
  // Act + Assert -- non-ISO formats and impossible calendar days are rejected.
  ASSERT_THROW(std::ignore = ParseDateDays("not-a-date"), std::runtime_error);
  ASSERT_THROW(std::ignore = ParseDateDays("2024-13-01"), std::runtime_error);
  ASSERT_THROW(std::ignore = ParseDateDays("2024-02-30"), std::runtime_error);
  ASSERT_THROW(std::ignore = ParseDateDays("2023-02-29"), std::runtime_error);
  // sscanf tolerates single-digit fields; it must still normalize to days.
  EXPECT_EQ(ParseDateDays("2024-1-1"), ParseDateDays("2024-01-01"));
}

TEST(DateTest, ParseFormatRoundTrip) {
  // Arrange -- representative dates, including a leap day.
  const std::vector<std::string> dates = {"2020-02-29", "2024-12-31",
                                          "2000-01-01", "1999-07-04"};
  for (const std::string& date : dates) {
    // Act -- parse to days and back.
    const int64_t days = ParseDateDays(date);
    // Assert -- formatting round-trips exactly.
    EXPECT_EQ(FormatDateDays(days), date);
  }
  // Assert -- day counts increase chronologically.
  EXPECT_LT(ParseDateDays("1999-01-01"), ParseDateDays("2000-01-01"));
  EXPECT_LT(ParseDateDays("2020-01-01"), ParseDateDays("2020-01-02"));
}

TEST(DateTest, AddDayMonthYearIntervals) {
  // Act + Assert -- day arithmetic is exact across month boundaries.
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(
                ParseDateDays("2024-01-31"), 1, "day")),
            "2024-02-01");
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(
                ParseDateDays("2024-03-01"), -1, "day")),
            "2024-02-29");

  // Act + Assert -- month arithmetic clamps overflowing month-ends.
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(
                ParseDateDays("2024-01-31"), 1, "month")),
            "2024-02-29");
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(
                ParseDateDays("2023-01-31"), 1, "month")),
            "2023-02-28");
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(
                ParseDateDays("2024-08-31"), 1, "month")),
            "2024-09-30");

  // Act + Assert -- year arithmetic clamps leap-day overflow.
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(
                ParseDateDays("2024-02-29"), 1, "year")),
            "2025-02-28");
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(
                ParseDateDays("2020-02-29"), 4, "year")),
            "2024-02-29");

  // Act + Assert -- the plural forms are accepted.
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(
                ParseDateDays("2024-01-15"), 2, "months")),
            "2024-03-15");
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(
                ParseDateDays("2024-01-15"), 2, "years")),
            "2026-01-15");
}

TEST(DateTest, AddIntervalRejectsUnknownUnits) {
  // Act + Assert -- unsupported interval units throw.
  ASSERT_THROW(std::ignore = AddDateIntervalDays(ParseDateDays("2024-01-01"), 1,
                                                 "week"),
               std::runtime_error);
  ASSERT_THROW(std::ignore = AddDateIntervalDays(ParseDateDays("2024-01-01"), 1,
                                                 "hour"),
               std::runtime_error);
}

TEST(DateTest, ValueDateFromDaysRoundTrip) {
  // Arrange -- a handful of day counts via the Value API.
  const std::vector<int64_t> day_counts = {
      ParseDateDays("1970-01-01"), ParseDateDays("2020-02-29"),
      ParseDateDays("2038-01-19")};
  for (int64_t days : day_counts) {
    // Act -- build a Value and extract its day count.
    const Value value = Value::DateFromDays(days);
    // Assert -- the day count survives the Value round-trip.
    EXPECT_EQ(value.DateDays(), days);
    EXPECT_EQ(value.type, ValueType::kDate);
  }
}

TEST(ValueTest, DateDaysRequiresDateType) {
  // Act + Assert -- DateDays() rejects every non-date Value with a throw.
  ASSERT_THROW(std::ignore = Value(1).DateDays(), std::runtime_error);
  ASSERT_THROW(std::ignore = Value("x").DateDays(), std::runtime_error);
  ASSERT_THROW(std::ignore = Value(1.5).DateDays(), std::runtime_error);
  ASSERT_THROW(std::ignore = Value().DateDays(), std::runtime_error);
}

TEST(ValueTest, MixedTypeComparisonThrows) {
  // Act + Assert -- ordering comparisons across different types throw.
  ASSERT_THROW(Value(1) < Value("a"), std::runtime_error);
  ASSERT_THROW(Value(1) > Value("a"), std::runtime_error);
  ASSERT_THROW(Value(1.5) < Value(1), std::runtime_error);
  ASSERT_THROW(Value("a") > Value(1.5), std::runtime_error);
}

TEST(ValueTest, NullComparisonThrows) {
  // Act + Assert -- comparing null Values by ordering throws.
  ASSERT_THROW(Value() < Value(), std::runtime_error);
  ASSERT_THROW(Value() > Value(), std::runtime_error);
  // Assert -- equality of two nulls still holds and is safe.
  ASSERT_TRUE(Value() == Value());
}

TEST(ValueTest, MixedTypeArithmeticThrows) {
  // Act + Assert -- every arithmetic/bitwise op rejects mixed operands.
  ASSERT_THROW(Value(1) + Value("a"), std::runtime_error);
  ASSERT_THROW(Value(1) - Value("a"), std::runtime_error);
  ASSERT_THROW(Value(1) * Value("a"), std::runtime_error);
  ASSERT_THROW(Value(1) / Value("a"), std::runtime_error);
  ASSERT_THROW(Value(1) % Value("a"), std::runtime_error);
  ASSERT_THROW(Value(1) & Value("a"), std::runtime_error);
  ASSERT_THROW(Value(1) | Value("a"), std::runtime_error);
  ASSERT_THROW(Value(1) ^ Value("a"), std::runtime_error);
  ASSERT_THROW(Value(1.5) - Value(1), std::runtime_error);
  ASSERT_THROW(Value("a") / Value(1.5), std::runtime_error);
}

TEST(ValueTest, NonNumericArithmeticThrows) {
  // Act + Assert -- varchar/date operands cannot participate in arithmetic.
  ASSERT_THROW(Value("a") - Value("b"), std::runtime_error);
  ASSERT_THROW(Value("a") * Value("b"), std::runtime_error);
  ASSERT_THROW(Value("a") / Value("b"), std::runtime_error);
  ASSERT_THROW(Value("a") % Value("b"), std::runtime_error);
  ASSERT_THROW(Value("a") & Value("b"), std::runtime_error);
  ASSERT_THROW(Value("a") | Value("b"), std::runtime_error);
  ASSERT_THROW(Value("a") ^ Value("b"), std::runtime_error);
  ASSERT_THROW(Value::DateFromDays(1) - Value::DateFromDays(2),
               std::runtime_error);
}

TEST(ValueTest, InvalidValueTypeThrows) {
  // Arrange -- a Value with a corrupted type discriminator.
  Value broken;
  broken.type = static_cast<ValueType>(99);
  char buffer[16] = {0};

  // Act + Assert -- every type-switching entry point rejects the bad type.
  ASSERT_THROW(std::ignore = broken.Size(), std::runtime_error);
  ASSERT_THROW(broken.Serialize(buffer), std::runtime_error);
  ASSERT_THROW(broken.Deserialize(buffer, static_cast<ValueType>(99)),
               std::runtime_error);
  ASSERT_THROW(std::ignore = broken.AsString(), std::runtime_error);
  ASSERT_THROW(std::ignore = broken.EncodeMemcomparableFormat(),
               std::runtime_error);
  ASSERT_THROW(broken < broken, std::runtime_error);
  ASSERT_THROW(broken > broken, std::runtime_error);
  ASSERT_THROW(broken == broken, std::runtime_error);
  ASSERT_THROW(std::hash<Value>{}(broken), std::runtime_error);
}

TEST(ValueTest, DecodeMemcomparableRejectsNullAndBrokenPrefixes) {
  // Arrange -- a destination Value and malformed encoded prefixes.
  Value v;
  // Act + Assert -- a kNull prefix byte and an out-of-range prefix both throw.
  ASSERT_THROW(v.DecodeMemcomparableFormat("\x00"), std::runtime_error);
  ASSERT_THROW(v.DecodeMemcomparableFormat("\x05"), std::runtime_error);
}

TEST(ValueTest, HashNullValueThrows) {
  // Act + Assert -- hashing a null Value falls through to the default throw.
  std::hash<Value> hasher;
  ASSERT_THROW(hasher(Value()), std::runtime_error);
}

}  // namespace tinylamb
