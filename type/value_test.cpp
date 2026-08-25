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
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/log_message.hpp"
#include "gtest/gtest.h"
#include "type/date.hpp"
#include "type/function.hpp"
#include "type/interval.hpp"
#include "type/type.hpp"
#include "type/value_type.hpp"

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

namespace {
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

TEST(ValueTest, ArrayRoundTrip) {
  const Value array =
      Value::Array({Value(int64_t{1}), Value(int64_t{2})}, "INT64");
  ASSERT_TRUE(array.IsArray());
  EXPECT_EQ(array.ArrayElementSqlType(), "INT64");
  ASSERT_EQ(array.ArrayElements().size(), 2);
  EXPECT_EQ(array.ArrayElements()[0], Value(int64_t{1}));
  EXPECT_EQ(array.ArrayElements()[1], Value(int64_t{2}));
  SerializeDeserializeTest(array);
  const std::string encoded = array.EncodeMemcomparableFormat();
  Value decoded;
  ASSERT_GT(decoded.DecodeMemcomparableFormat(encoded.data()), 0U);
  EXPECT_EQ(decoded, array);
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

TEST(ValueTest, VarcharDeserializeOwnsTheBytes) {
  // Deserialize must copy the payload: the returned value outlives the page
  // buffer it was decoded from.
  const Value original(std::string("page-payload"));
  std::string buffer(original.Size(), '\0');
  ASSERT_EQ(original.Serialize(buffer.data()), buffer.size());

  Value restored;
  restored.Deserialize(buffer.data(), ValueType::kVarChar);

  // Overwrite the source bytes; the decoded value must be unaffected.
  buffer.assign(buffer.size(), '#');
  EXPECT_EQ(restored.value.varchar_value, "page-payload");
  EXPECT_EQ(restored.owned_data, "page-payload");
}

TEST(ValueTest, Dump) {
  // Arrange -- six Values of different types/levels for LOG streaming
  // Act -- stream each Value to LOG at various levels (no assertion;
  // output-only). LOG(FATAL) aborts by contract, so the highest streamed
  // level here is ALERT.
  LOG(TRACE) << Value(12);
  LOG(DEBUG) << Value(120214143342323);
  LOG(INFO) << Value("foo-bar");
  LOG(WARN) << Value(1.23e3);
  LOG(ERROR) << Value();
  LOG(ALERT) << Value("foo");
  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

void MemcomparableFormatEncodeTest(const std::vector<Value>& input) {
  std::vector<Value> values(input);
  std::ranges::sort(values);
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

}  // namespace

namespace {

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
  std::ranges::sort(values);
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

}  // namespace

TEST(ValueTest, MemComparableFormatDecodeInt) {
  // Arrange -- 7-byte source string and its 5040 permutations as targets
  std::string src = "\x60\x70\x80\x90\x10\x11\x12";
  ASSERT_EQ(src.size(), 7);
  std::vector<std::string> targets;
  do {
    targets.emplace_back("\x01" + src + "\x01");
  } while (std::ranges::next_permutation(src).found);

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
  } while (std::ranges::next_permutation(src).found);

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
  } while (std::ranges::next_permutation(src).found);

  // Act + Assert -- MemcomparableFormatDecodeTest asserts decoded Values are
  // strictly ascending
  MemcomparableFormatDecodeTest(targets);
}

TEST(ValueTest, UnaryAndAggregationToString) {
  // Arrange + Act -- stream every UnaryOperation and AggregationType value
  std::ostringstream unary;
  unary << UnaryOperation::kIsNull << "|" << UnaryOperation::kIsNotNull << "|"
        << UnaryOperation::kNot << "|" << UnaryOperation::kMinus << "|"
        << static_cast<UnaryOperation>(  // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
            99);  // Deliberately invalid: ToString must fall back to UNKNOWN.
  // Assert -- every enum member has a textual name, unknown falls back
  ASSERT_EQ(unary.str(), "IS NULL|IS NOT NULL|NOT|-|UNKNOWN");

  std::ostringstream agg;
  agg << AggregationType::kCount << "|" << AggregationType::kSum << "|"
      << AggregationType::kAvg << "|" << AggregationType::kMin << "|"
      << AggregationType::kMax << "|"
      << static_cast<AggregationType>(  // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
          99);  // Deliberately invalid: ToString must fall back to UNKNOWN.
  ASSERT_EQ(agg.str(), "COUNT|SUM|AVG|MIN|MAX|UNKNOWN");
}

TEST(ValueTest, NullSizeAndSerialize) {
  // Arrange -- a default (null) Value
  Value v;
  std::array<char, 4> buffer{};
  // Act + Assert -- null has a 1-byte serialized footprint
  ASSERT_EQ(v.Size(), 1);
  ASSERT_EQ(v.Serialize(buffer.data()), 1);
  // Act -- deserializing as kNull requires an explicit type and is rejected
  Value restored;
  ASSERT_THROW(restored.Deserialize(buffer.data(), ValueType::kNull),
               std::runtime_error);
}

TEST(ValueTest, AsStringVariants) {
  // Arrange -- one value per type
  // Act + Assert -- AsString returns the documented formatting
  ASSERT_EQ(Value().AsString(), "(unknown type)");
  ASSERT_EQ(Value(42).AsString(), "42");
  ASSERT_EQ(Value("x").AsString(), "\"x\"");
  ASSERT_EQ(Value(1.5).AsString(), "1.5");
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

TEST(ValueTest, DoubleEqualityUsesEpsilon) {
  // Equality on doubles is tolerant: accumulated sums (e.g. SUM over doubles)
  // must compare equal to their literal, so values within 1e-9 are equal.
  const Value one(1.0);
  const Value neighbor(std::nextafter(1.0, 2.0));
  EXPECT_TRUE(one == neighbor);
  EXPECT_FALSE(one != neighbor);
  EXPECT_TRUE(one < Value(std::nextafter(1.0 + 2e-9, 2.0)));
  EXPECT_TRUE(one == Value(1.0 + 0.9e-9));
  EXPECT_FALSE(one == Value(1.1));
}

TEST(ValueTest, DoubleEqualityAgreesWithHash) {
  const double a = 1.0;
  const double b = std::nextafter(a, 2.0);
  std::hash<Value> hasher;
  EXPECT_NE(hasher(Value(a)), hasher(Value(b)));
  std::unordered_set<Value> seen;
  seen.insert(Value(a));
  EXPECT_EQ(seen.count(Value(b)), 0U);
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
  // Deliberately corrupted type discriminator: exercising the fallback paths.
  broken.type =
      static_cast<ValueType>(99);  // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
  std::array<char, 16> buffer{};

  // Act + Assert -- every type-switching entry point rejects the bad type.
  ASSERT_THROW(std::ignore = broken.Size(), std::runtime_error);
  ASSERT_THROW(broken.Serialize(buffer.data()), std::runtime_error);
  ASSERT_THROW(
      broken.Deserialize(
          buffer.data(),
          static_cast<
              ValueType>(99)),  // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
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
  ASSERT_THROW(v.DecodeMemcomparableFormat("\x06"), std::runtime_error);
}

TEST(ValueTest, HashNullValueIsStable) {
  // Act + Assert -- hashing a null Value neither throws nor varies.
  std::hash<Value> hasher;
  ASSERT_EQ(hasher(Value()), hasher(Value()));
  ASSERT_EQ(hasher(Value()), 0x9e3779b97f4a7c15ULL);
}

TEST(ValueTest, NullValuesSurviveUnorderedContainers) {
  // Regression: DISTINCT/GROUP BY keep values in unordered containers; a NULL
  // key used to throw from std::hash<Value> and crash the aggregation.
  std::unordered_set<Value> distinct;
  distinct.insert(Value());
  EXPECT_EQ(distinct.count(Value()), 1U);
  EXPECT_EQ(distinct.size(), 1U);
  distinct.insert(Value(1));
  EXPECT_EQ(distinct.size(), 2U);

  std::unordered_map<Value, int> groups;
  ++groups[Value()];
  ++groups[Value()];
  EXPECT_EQ(groups[Value()], 2);
}

// Pins derived from the value_fuzzer oracle (type/value_fuzzer.cpp): for any
// two strings, memcomparable encoding must preserve the source total order.
// The fuzzer explores arbitrary split points; these cases cover the boundary
// shapes that historically break order-preserving encoders.
TEST(ValueTest, MemcomparableVarcharFuzzerEdgeShapes) {
  // Empty string sorts before everything; prefix sorts before extension.
  MemcomparableFormatEncodeTest(
      {Value(""), Value("a"), Value("ab"), Value("abc")});

  // Embedded NUL bytes must not create false equalities.
  MemcomparableFormatEncodeTest(
      {Value(std::string("a\0b", 3)), Value(std::string("a", 1)),
       Value(std::string("a\0", 2)), Value(std::string("a!", 2))});

  // High-bit bytes keep their unsigned order after encoding.
  MemcomparableFormatEncodeTest({Value(std::string("\x01", 1)),
                                 Value(std::string("\x7f", 1)),
                                 Value(std::string("\x80", 1)),
                                 Value(std::string("\xff", 1))});

  // Long shared prefixes with divergent tails.
  MemcomparableFormatEncodeTest({
      Value(std::string(64, 'x') + "a"),
      Value(std::string(64, 'x') + "b"),
      Value(std::string(63, 'x') + "za"),
      Value(std::string(65, 'x')),
  });
}

TEST(ValueTest, MemcomparableVarcharEmptyIsLessThanOneChar) {
  // Explicit pairwise pin (not just sorted-vector): empty < "a".
  const std::string empty = Value("").EncodeMemcomparableFormat();
  const std::string one = Value("a").EncodeMemcomparableFormat();
  EXPECT_LT(empty, one);
  // And the encoded forms of distinct strings never collide.
  EXPECT_NE(empty, one);
}

TEST(ValueTest, ToStringAllUnaryOperations) {
  std::ostringstream oss;
  oss << UnaryOperation::kIsTrue << "|"
      << UnaryOperation::kIsNotTrue << "|"
      << UnaryOperation::kIsFalse << "|"
      << UnaryOperation::kIsNotFalse;
  ASSERT_EQ(oss.str(), "IS TRUE|IS NOT TRUE|IS FALSE|IS NOT FALSE");
}

TEST(ValueTest, DeserializeUndefinedTypeFallthrough) {
  Value v;
  char buf[16]{};
  ASSERT_THROW(v.Deserialize(buf, static_cast<ValueType>(99)), std::runtime_error);
}

TEST(ValueTest, SkipSerializedRejectsNullAndUndefined) {
  ASSERT_THROW(Value::SkipSerialized(nullptr, ValueType::kNull), std::runtime_error);
  char buf[16]{};
  ASSERT_THROW(Value::SkipSerialized(buf, static_cast<ValueType>(99)), std::runtime_error);
}

TEST(ValueTest, AsStringInfAndNegInfAndNan) {
  ASSERT_EQ(Value(std::numeric_limits<double>::infinity()).AsString(), "inf");
  ASSERT_EQ(Value(-std::numeric_limits<double>::infinity()).AsString(), "-inf");
  ASSERT_EQ(Value(std::numeric_limits<double>::quiet_NaN()).AsString(), "nan");
}

TEST(ValueTest, EncodeMemcomparableRejectsNull) {
  Value null_val;
  ASSERT_THROW(null_val.EncodeMemcomparableFormat(), std::runtime_error);
}

TEST(ValueTest, ArrayWithNullElementRoundTrip) {
  Value arr = Value::Array({Value(int64_t{1}), Value(), Value(int64_t{3})}, "INT64");
  std::string encoded = arr.EncodeMemcomparableFormat();
  Value decoded;
  decoded.DecodeMemcomparableFormat(encoded.data());
  EXPECT_EQ(decoded, arr);
}

TEST(ValueTest, ArrayComparisonGreaterThan) {
  Value a = Value::Array({Value(int64_t{1})}, "INT64");
  Value b = Value::Array({Value(int64_t{2})}, "INT64");
  EXPECT_TRUE(b > a);
  EXPECT_FALSE(a > b);
}

TEST(ValueTest, ArithmeticEdgeCases) {
  ASSERT_THROW(
      Value(std::numeric_limits<int64_t>::min()) - Value(int64_t{1}),
      std::runtime_error);
  ASSERT_THROW(Value(10) / Value(0), std::runtime_error);
  ASSERT_THROW(Value(std::numeric_limits<int64_t>::min()) / Value(int64_t{-1}),
               std::runtime_error);
}

TEST(ValueTest, ArrayEncoderDecoderRoundTrip) {
  Value arr = Value::Array({Value(int64_t{7}), Value(int64_t{8})}, "INT64");
  std::stringstream ss;
  Encoder enc(ss);
  enc << arr;
  Value restored;
  Decoder dec(ss);
  dec >> restored;
  EXPECT_EQ(restored, arr);
}

TEST(DateTest, ToSysDaysOutOfRange) {
  ASSERT_THROW(FormatDateDays(-20000000LL), std::runtime_error);
  ASSERT_THROW(FormatDateDays(20000000LL), std::runtime_error);
}

TEST(DateTest, ShiftYMDClampedYearOverflow) {
  ASSERT_THROW(
      (void)AddDateIntervalDays(ParseDateDays("9999-12-31"), 999999, "year"),
      std::runtime_error);
  ASSERT_THROW(
      (void)AddDateIntervalDays(ParseDateDays("0001-01-01"), -999999, "year"),
      std::runtime_error);
}

TEST(DateTest, ParseDateEdgeCases) {
  ASSERT_THROW((void)ParseDateDays("999999999-01-01"), std::runtime_error);
  ASSERT_THROW((void)ParseDateDays("2024-01-01xyz"), std::runtime_error);
  ASSERT_THROW((void)ParseDateDays("2024-01-01 "), std::runtime_error);
  ASSERT_THROW((void)ParseDateDays("99999999-01-01"), std::runtime_error);
  ASSERT_THROW((void)ParseDateDays("-2024-01-01"), std::runtime_error);
  ASSERT_THROW((void)ParseDateDays("2024/01/01"), std::runtime_error);
  ASSERT_THROW((void)ParseDateDays("2024-02-30"), std::runtime_error);
  ASSERT_THROW((void)ParseDateDays(""), std::runtime_error);
  ASSERT_THROW(
      (void)AddDateIntervalDays(ParseDateDays("2024-01-01"),
                                std::numeric_limits<int64_t>::max(), "day"),
      std::runtime_error);
  ASSERT_THROW(
      (void)AddDateIntervalDays(ParseDateDays("2024-01-01"),
                                std::numeric_limits<int64_t>::min(), "day"),
      std::runtime_error);
}

TEST(DateTest, SetGetDefaultTimeZone) {
  SetDefaultTimeZone("UTC");
  EXPECT_EQ(GetDefaultTimeZone(), "UTC");
  SetDefaultTimeZone("");
  EXPECT_EQ(GetDefaultTimeZone(), "America/Los_Angeles");
}

TEST(IntervalTest, CoverageGaps) {
  IntervalValue iv = IntervalValue::Parse("  P1Y");
  EXPECT_EQ(iv.months, 12);

  SetSessionConstant("foo_cov", "bar_cov");
  EXPECT_EQ(GetSessionConstant("foo_cov"), "bar_cov");
  EXPECT_TRUE(HasSessionConstant("foo_cov"));

  IntervalValue neg_days{0, -45, 0};
  IntervalValue justified_days = neg_days.JustifyDays();
  EXPECT_EQ(justified_days.months, -1);
  EXPECT_EQ(justified_days.days, -15);

  IntervalValue neg_nanos{0, 0, -(35LL * 24LL * 3600LL * 1000000000LL)};
  IntervalValue justified_nanos = neg_nanos.JustifyInterval();
  EXPECT_LT(justified_nanos.months, 0);

  ASSERT_THROW((void)IntervalValue::Parse("P1X"), std::runtime_error);
  ASSERT_THROW((void)IntervalValue::Parse("PT1X"), std::runtime_error);
  ASSERT_THROW((void)IntervalValue::Parse("P1"), std::runtime_error);
  ASSERT_THROW((void)IntervalValue::Parse("P-X"), std::runtime_error);

  IntervalValue iv_min = IntervalValue::Parse("5", "minute");
  EXPECT_EQ(iv_min.nanos, 5LL * 60LL * 1000000000LL);

  // Test single unit interval branches
  EXPECT_EQ(IntervalValue::Parse("2", "quarter").months, 6);
  EXPECT_EQ(IntervalValue::Parse("3", "quarters").months, 9);
  EXPECT_EQ(IntervalValue::Parse("2", "week").days, 14);
  EXPECT_EQ(IntervalValue::Parse("1", "weeks").days, 7);
  EXPECT_EQ(IntervalValue::Parse("5", "days").days, 5);
  EXPECT_EQ(IntervalValue::Parse("100", "milliseconds").nanos, 100000000);
  EXPECT_EQ(IntervalValue::Parse("500", "microseconds").nanos, 500000);
  EXPECT_EQ(IntervalValue::Parse("42", "nanoseconds").nanos, 42);
  EXPECT_EQ(IntervalValue::Parse("-2", "hours").nanos, -7200000000000LL);
  EXPECT_EQ(IntervalValue::Parse("+3", "minutes").nanos, 180000000000LL);

  // ISO 8601 interval parsing branches
  EXPECT_EQ(IntervalValue::Parse("P1Y2M3D").months, 14);
  EXPECT_EQ(IntervalValue::Parse("PT1H2M3S").nanos, (3600 + 120 + 3) * 1000000000LL);
  EXPECT_EQ(IntervalValue::Parse("P+1Y-2M+3DT-1H+2M-3.5S").months, 10);
  EXPECT_EQ(IntervalValue::Parse("P").months, 0);
  EXPECT_EQ(IntervalValue::Parse("PT").nanos, 0);
  ASSERT_THROW(IntervalValue::Parse("P1X"), std::runtime_error);
  ASSERT_THROW(IntervalValue::Parse("PT1X"), std::runtime_error);
  ASSERT_THROW(IntervalValue::Parse("P1"), std::runtime_error);

  // JustifyDays and JustifyInterval with empty/zero
  IntervalValue iv_zero{};
  IntervalValue j_zero = iv_zero.JustifyInterval();
  EXPECT_EQ(j_zero.months, 0);
  EXPECT_EQ(j_zero.days, 0);
  EXPECT_EQ(j_zero.nanos, 0);
  IntervalValue j_days = iv_zero.JustifyDays();
  EXPECT_EQ(j_days.months, 0);
  IntervalValue j_hours = iv_zero.JustifyHours();
  EXPECT_EQ(j_hours.days, 0);
}

TEST(ValueTypeTest, ValueTypeToStringAllCases) {
  EXPECT_EQ(ValueTypeToString(ValueType::kNull), "(null)");
  EXPECT_EQ(ValueTypeToString(ValueType::kInt64), "Integer");
  EXPECT_EQ(ValueTypeToString(ValueType::kVarChar), "Varchar");
  EXPECT_EQ(ValueTypeToString(ValueType::kDouble), "Double");
  EXPECT_EQ(ValueTypeToString(ValueType::kDate), "Date");
  EXPECT_EQ(ValueTypeToString(ValueType::kArray), "Array");
  EXPECT_EQ(ValueTypeToString(static_cast<ValueType>(99)), "unknown value type");
}

TEST(FunctionTest, CoverageGaps) {
  Function f_def;
  Function f_named("my_func", 3);
  Function original("add", {Type(TypeTag::kBigInt), Type(TypeTag::kBigInt)},
                    Type(TypeTag::kBigInt));
  std::stringstream ss;
  Encoder enc(ss);
  enc << original;

  Function restored;
  Decoder dec(ss);
  dec >> restored;
}

TEST(TypeTest, CoverageGaps) {
  Type t;
  EXPECT_EQ(t.GetType(), TypeTag::kInvalid);
  EXPECT_FALSE(t.IsValid());

  Type original(TypeTag::kVarChar);
  std::stringstream ss;
  Encoder enc(ss);
  enc << original;

  Type restored;
  Decoder dec(ss);
  dec >> restored;
  EXPECT_EQ(restored.GetType(), TypeTag::kVarChar);
  EXPECT_TRUE(restored.IsValid());
}

TEST(ValueTest, VarcharIntervalComparisonBranches) {
  Value iv1("1-2 3 4:5:6");
  Value iv2("2-0 0 0:0:0");
  EXPECT_TRUE(iv1 < iv2);
  EXPECT_FALSE(iv2 < iv1);
  EXPECT_TRUE(iv2 > iv1);
  EXPECT_FALSE(iv1 > iv2);

  Value s1("foo-bar");
  Value s2("hello");
  EXPECT_TRUE(s1 < s2 || s2 < s1);
  EXPECT_TRUE(s1 > s2 || s2 > s1);
}

}  // namespace tinylamb



