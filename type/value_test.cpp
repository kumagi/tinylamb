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

TEST(ValueTest, Constructor_Default_CreatesNullValue) {
  Value v;

  EXPECT_EQ(v.type, ValueType::kNull);
}

TEST(ValueTest, AddAndSubtract_WithIntegers_ComputesExpectedResults) {
  Value v1(1);
  Value v2(2);
  Value v3(3);
  Value v4(4);

  Value add_result = v2 + v3;
  Value sub_result = v3 - v4;

  EXPECT_EQ(v1, Value(1));
  EXPECT_EQ(add_result, Value(5));
  EXPECT_EQ(sub_result, Value(-1));
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

TEST(ValueTest, SerializeDeserialize_DiverseTypes_PreservesValues) {
  Value v_int(1);
  Value v_int64(301L);
  Value v_str("hello");
  Value v_double(439.3);

  SerializeDeserializeTest(v_int);
  SerializeDeserializeTest(v_int64);
  SerializeDeserializeTest(v_str);
  SerializeDeserializeTest(v_double);
}

TEST(ValueTest,
     SerializeDeserialize_DateAndBinaryAndLongVarchar_PreservesValues) {
  Value v_date = Value::Date("2020-01-02");
  Value v_empty("");
  const std::string binary("\x00\x01\xff embedded \x00 nulls", 20);
  Value v_binary{std::string(binary)};
  const std::string long_text(1000, 'x');
  Value v_long{std::string(long_text)};

  SerializeDeserializeTest(v_date);
  SerializeDeserializeTest(v_empty);
  SerializeDeserializeTest(v_binary);
  SerializeDeserializeTest(v_long);
}

TEST(ValueTest, Array_RoundTripSerializationAndMemcomparable_PreservesArray) {
  const Value array =
      Value::Array({Value(int64_t{1}), Value(int64_t{2})}, "INT64");

  const std::string encoded = array.EncodeMemcomparableFormat();
  Value decoded;
  size_t decoded_size = decoded.DecodeMemcomparableFormat(encoded.data());

  ASSERT_TRUE(array.IsArray());
  EXPECT_EQ(array.ArrayElementSqlType(), "INT64");
  ASSERT_EQ(array.ArrayElements().size(), 2);
  EXPECT_EQ(array.ArrayElements()[0], Value(int64_t{1}));
  EXPECT_EQ(array.ArrayElements()[1], Value(int64_t{2}));
  SerializeDeserializeTest(array);
  ASSERT_GT(decoded_size, 0U);
  EXPECT_EQ(decoded, array);
}

TEST(ValueTest, LessThan_DiverseTypes_ReturnsTrueWhenSmaller) {
  Value int_small(1);
  Value int_large(2);
  Value dbl_small(-123.0);
  Value dbl_large(23.0);
  Value str_small("abc");
  Value str_large("d");

  EXPECT_TRUE(int_small < int_large);
  EXPECT_TRUE(dbl_small < dbl_large);
  EXPECT_TRUE(str_small < str_large);
}

TEST(ValueTest, CopyConstructor_WithVarchar_OwnsIndependentCopy) {
  std::string backing = "statistics-boundary";
  Value borrowed;
  borrowed.type = ValueType::kVarChar;
  borrowed.value.varchar_value = backing;

  Value copied = borrowed;
  backing.assign("overwritten");

  EXPECT_EQ(copied.value.varchar_value, "statistics-boundary");
}

TEST(ValueTest, Deserialize_VarcharFromBuffer_OwnsDecodedPayload) {
  const Value original(std::string("page-payload"));
  std::string buffer(original.Size(), '\0');
  size_t written = original.Serialize(buffer.data());

  Value restored;
  restored.Deserialize(buffer.data(), ValueType::kVarChar);
  buffer.assign(buffer.size(), '#');

  ASSERT_EQ(written, buffer.size());
  EXPECT_EQ(restored.value.varchar_value, "page-payload");
  EXPECT_EQ(restored.owned_data, "page-payload");
}

TEST(ValueTest, OutputOperator_VariousTypes_OutputsToLogWithoutError) {
  Value v_trace(12);
  Value v_debug(120214143342323);
  Value v_info("foo-bar");
  Value v_warn(1.23e3);
  Value v_error;
  Value v_alert("foo");

  LOG(TRACE) << v_trace;
  LOG(DEBUG) << v_debug;
  LOG(INFO) << v_info;
  LOG(WARN) << v_warn;
  LOG(ERROR) << v_error;
  LOG(ALERT) << v_alert;

  EXPECT_EQ(v_trace.type, ValueType::kInt64);
  EXPECT_EQ(v_error.type, ValueType::kNull);
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

TEST(ValueTest, EncodeMemcomparableFormat_WithIntValues_PreservesTotalOrder) {
  std::vector<Value> ascending = {Value(1), Value(2), Value(3)};
  std::vector<Value> descending = {Value(-1), Value(-2), Value(-3)};
  std::vector<Value> extreme = {Value(std::numeric_limits<int64_t>::max()),
                                Value(std::numeric_limits<int64_t>::min()),
                                Value(1), Value(0), Value(-1)};

  MemcomparableFormatEncodeTest(ascending);
  MemcomparableFormatEncodeTest(descending);
  MemcomparableFormatEncodeTest(extreme);
}

TEST(ValueTest,
     EncodeMemcomparableFormat_WithDoubleValues_PreservesOrderAndSignFlag) {
  // Fixed: the sign flag used to be written via `be |= 0x80`, which only
  // landed on the first image byte on little-endian hosts.  The flag byte is
  // now written positionally and must match the decoder's expectation on any
  // endianness.
  const Value pos(1.0);
  const Value neg(-1.0);
  const std::string enc_pos = pos.EncodeMemcomparableFormat();
  const std::string enc_neg = neg.EncodeMemcomparableFormat();
  // First image byte (after the 1-byte type prefix) carries the flipped sign.
  EXPECT_NE(enc_pos[1] & 0x80, 0);
  EXPECT_EQ(enc_neg[1] & 0x80, 0);

  std::vector<Value> edge = {Value(-std::numeric_limits<double>::infinity()),
                             Value(-1.0),
                             Value(0.0),
                             Value(1e-300),
                             Value(1.0),
                             Value(std::numeric_limits<double>::max()),
                             Value(std::numeric_limits<double>::infinity())};
  MemcomparableFormatEncodeTest(edge);

  // -0.0 and +0.0 encode to the same image (SQL treats them equal).
  EXPECT_EQ(Value(-0.0).EncodeMemcomparableFormat(),
            Value(0.0).EncodeMemcomparableFormat());

  for (const Value& v : edge) {
    const std::string encoded = v.EncodeMemcomparableFormat();
    Value decoded;
    const size_t consumed = decoded.DecodeMemcomparableFormat(encoded.data());
    EXPECT_EQ(consumed, encoded.size());
    EXPECT_EQ(decoded.type, ValueType::kDouble);
    EXPECT_EQ(decoded.value.double_value, v.value.double_value);
  }

  // NaN round-trips to some NaN.
  const Value nan(std::numeric_limits<double>::quiet_NaN());
  Value decoded_nan;
  decoded_nan.DecodeMemcomparableFormat(nan.EncodeMemcomparableFormat().data());
  EXPECT_TRUE(std::isnan(decoded_nan.value.double_value));
}

TEST(ValueTest,
     EncodeMemcomparableFormat_WithVariousVarchars_MatchesExpectedEncoding) {
  Value v_a("a");
  Value v_ab("ab");
  Value v_abc("abc");
  Value v_abcd("abcd");
  Value v_abcde("abcde");
  Value v_abcdef("abcdef");
  Value v_abcdefg("abcdefg");
  Value v_abcdefgh("abcdefgh");
  Value v_abcdefghi("abcdefghi");
  Value v_abcdefghij("abcdefghij");
  Value v_bin("\x60\x70\x10\x11\x12\x80\x90\x01");

  EXPECT_EQ(v_a.EncodeMemcomparableFormat(),
            std::string(
                {'\2', 'a', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\x01'}));
  EXPECT_EQ(v_ab.EncodeMemcomparableFormat(),
            std::string(
                {'\2', 'a', 'b', '\0', '\0', '\0', '\0', '\0', '\0', '\x02'}));
  EXPECT_EQ(
      v_abc.EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', '\0', '\0', '\0', '\0', '\0', '\x03'}));
  EXPECT_EQ(
      v_abcd.EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', '\0', '\0', '\0', '\0', '\x04'}));
  EXPECT_EQ(
      v_abcde.EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', 'e', '\0', '\0', '\0', '\x05'}));
  EXPECT_EQ(
      v_abcdef.EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', 'e', 'f', '\0', '\0', '\x06'}));
  EXPECT_EQ(
      v_abcdefg.EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', 'e', 'f', 'g', '\0', '\x07'}));
  EXPECT_EQ(
      v_abcdefgh.EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', '\x08'}));
  EXPECT_EQ(v_abcdefghi.EncodeMemcomparableFormat(),
            std::string({'\2', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', '\x09',
                         'i', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\1'}));
  EXPECT_EQ(
      v_abcdefghij.EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', '\x09', 'i',
                   'j', '\0', '\0', '\0', '\0', '\0', '\0', '\x02'}));
  EXPECT_EQ(v_bin.EncodeMemcomparableFormat(),
            std::string("\x02\x60\x70\x10\x11\x12\x80\x90\x01\x08"));
}

TEST(ValueTest,
     EncodeMemcomparableFormat_WithVarcharValues_PreservesTotalOrder) {
  std::vector<Value> prefixes = {Value("a"), Value("aa"), Value("aaa")};
  std::vector<Value> alpha = {Value("a"), Value("b"), Value("c")};
  std::vector<Value> long_strs = {Value("blah,blah,blah"),
                                  Value("this is a pen"), Value("0123456789")};

  MemcomparableFormatEncodeTest(prefixes);
  MemcomparableFormatEncodeTest(alpha);
  MemcomparableFormatEncodeTest(long_strs);
}

TEST(ValueTest,
     EncodeMemcomparableFormat_WithDoubleValues_MatchesExpectedEncoding) {
  Value v_pos(1.0);
  Value v_zero(0.0);
  Value v_neg(-1.0);

  EXPECT_EQ(
      v_pos.EncodeMemcomparableFormat(),
      std::string({'\3', '\xbf', '\xf0', '\0', '\0', '\0', '\0', '\0', '\0'}));
  EXPECT_EQ(v_zero.EncodeMemcomparableFormat(),
            std::string(
                {'\3', '\x80', '\x00', '\0', '\0', '\0', '\0', '\0', '\x00'}));
  EXPECT_EQ(v_neg.EncodeMemcomparableFormat(),
            std::string({'\3', '\x40', '\x0F', '\xff', '\xff', '\xff', '\xff',
                         '\xff', '\xff'}));
}

TEST(ValueTest,
     EncodeMemcomparableFormat_WithDoubleValues_PreservesTotalOrder) {
  std::vector<Value> ascending = {Value(1.0), Value(2.0), Value(3.0)};
  std::vector<Value> descending = {Value(-1.0), Value(-2.0), Value(-3.0)};
  std::vector<Value> signed_vals = {Value(-1.0), Value(0.0), Value(1.0)};
  std::vector<Value> extreme = {Value(std::numeric_limits<double>::max()),
                                Value(std::numeric_limits<double>::min()),
                                Value(-1.0), Value(0.0), Value(1.0)};

  MemcomparableFormatEncodeTest(ascending);
  MemcomparableFormatEncodeTest(descending);
  MemcomparableFormatEncodeTest(signed_vals);
  MemcomparableFormatEncodeTest(extreme);
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

TEST(ValueTest, MemcomparableFormat_WithInt64Values_RoundTripsAccurately) {
  Value v_max(std::numeric_limits<int64_t>::max());
  Value v_12(12);
  Value v_0(0);
  Value v_neg1(-1);
  Value v_min(std::numeric_limits<int64_t>::min());

  EncodeDecodeTest(v_max);
  EncodeDecodeTest(v_12);
  EncodeDecodeTest(v_0);
  EncodeDecodeTest(v_neg1);
  EncodeDecodeTest(v_min);
}

TEST(ValueTest, MemcomparableFormat_WithVarcharValues_RoundTripsAccurately) {
  Value v_a("a");
  Value v_empty("");
  Value v_hello("hello");
  Value v_long("A bit long string");
  Value v_num("12345678");
  Value v_bin1("\x50\x60\x70\x10\x11\x12\x80\x02\x01");
  Value v_bin2("\x60\x70\x10\x11\x12\x80\x90\x08");
  Value v_bin3("\x60\x70\x10\x11\x12\x90\x80\x08");
  Value v_rand("49p2u3po32u423pori2pouropiu");

  EncodeDecodeTest(v_a);
  EncodeDecodeTest(v_empty);
  EncodeDecodeTest(v_hello);
  EncodeDecodeTest(v_long);
  EncodeDecodeTest(v_num);
  EncodeDecodeTest(v_bin1);
  EncodeDecodeTest(v_bin2);
  EncodeDecodeTest(v_bin3);
  EncodeDecodeTest(v_rand);
}

TEST(ValueTest, MemcomparableFormat_VarcharEmptyAndCorruptFlag) {
  // Empty string encodes as a single all-zero group (flag byte 0); the
  // hardened decoder must accept it as "" rather than reject flag==0.
  Value empty("");
  EncodeDecodeTest(empty);

  // A flag byte in [10,255] is not a valid group terminator: reject it
  // instead of reading a bogus length.
  std::string corrupt;
  corrupt.push_back(static_cast<char>(ValueType::kVarChar));
  corrupt.append(8, 'x');
  corrupt.push_back(static_cast<char>(12));  // invalid flag
  Value decoded;
  EXPECT_THROW(decoded.DecodeMemcomparableFormat(corrupt.c_str()),
               std::runtime_error);
}

TEST(ValueTest, MemcomparableFormat_WithDoubleValues_RoundTripsAccurately) {
  Value v_max(std::numeric_limits<double>::max());
  Value v_12(12.0);
  Value v_0(0.0);
  Value v_neg1(-1.0);
  Value v_min(std::numeric_limits<double>::min());

  EncodeDecodeTest(v_max);
  EncodeDecodeTest(v_12);
  EncodeDecodeTest(v_0);
  EncodeDecodeTest(v_neg1);
  EncodeDecodeTest(v_min);
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

TEST(ValueTest,
     DecodeMemcomparableFormat_WithPermutedIntBytes_DecodesInAscendingOrder) {
  std::string src = "\x60\x70\x80\x90\x10\x11\x12";
  ASSERT_EQ(src.size(), 7);
  std::vector<std::string> targets;
  do {
    targets.emplace_back("\x01" + src + "\x01");
  } while (std::ranges::next_permutation(src).found);

  MemcomparableFormatDecodeTest(targets);
}

TEST(
    ValueTest,
    DecodeMemcomparableFormat_WithPermutedVarcharBytes_DecodesInAscendingOrder) {
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

  MemcomparableFormatDecodeTest(targets);
}

TEST(
    ValueTest,
    DecodeMemcomparableFormat_WithPermutedDoubleBytes_DecodesInAscendingOrder) {
  std::string src = "\x60\x70\x80\x90\x10\x11\x12";
  std::vector<std::string> targets;
  do {
    targets.emplace_back("\x03" + src + "\x01");
  } while (std::ranges::next_permutation(src).found);

  MemcomparableFormatDecodeTest(targets);
}

TEST(ValueTest, ToString_UnaryAndAggregationEnums_FormatsExpectedStrings) {
  std::ostringstream unary;
  std::ostringstream agg;

  unary
      << UnaryOperation::kIsNull << "|" << UnaryOperation::kIsNotNull << "|"
      << UnaryOperation::kNot << "|" << UnaryOperation::kMinus << "|"
      << static_cast<
             UnaryOperation>(  // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
             99);
  agg << AggregationType::kCount << "|" << AggregationType::kSum << "|"
      << AggregationType::kAvg << "|" << AggregationType::kMin << "|"
      << AggregationType::kMax << "|"
      << static_cast<
             AggregationType>(  // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
             99);

  EXPECT_EQ(unary.str(), "IS NULL|IS NOT NULL|NOT|-|UNKNOWN");
  EXPECT_EQ(agg.str(), "COUNT|SUM|AVG|MIN|MAX|UNKNOWN");
}

TEST(ValueTest, Serialize_NullValue_ProducesOneByteAndRejectsDeserialize) {
  Value v;
  std::array<char, 4> buffer{};

  size_t size = v.Size();
  size_t written = v.Serialize(buffer.data());
  Value restored;

  EXPECT_EQ(size, 1);
  EXPECT_EQ(written, 1);
  EXPECT_THROW(restored.Deserialize(buffer.data(), ValueType::kNull),
               std::runtime_error);
}

TEST(ValueTest, AsString_VariousValueTypes_ReturnsFormattedString) {
  Value v_null;
  Value v_int(42);
  Value v_str("x");
  Value v_double(1.5);
  Value v_stream(7);
  std::ostringstream oss;

  std::string s_null = v_null.AsString();
  std::string s_int = v_int.AsString();
  std::string s_str = v_str.AsString();
  std::string s_double = v_double.AsString();
  oss << v_stream;

  EXPECT_EQ(s_null, "NULL");
  EXPECT_EQ(s_int, "42");
  EXPECT_EQ(s_str, "\"x\"");
  EXPECT_EQ(s_double, "1.5");
  EXPECT_EQ(oss.str(), "7");
}

TEST(ValueTest,
     PlusOperator_WithVarcharsAndIncompatibleTypes_ConcatenatesOrThrows) {
  Value s1("foo");
  Value s2("bar");
  Value num(1);
  Value str("x");
  Value d1(Value::DateFromDays(1));
  Value d2(Value::DateFromDays(2));

  Value result = s1 + s2;

  EXPECT_EQ(result, Value("foobar"));
  EXPECT_THROW(std::ignore = num + str, std::runtime_error);
  EXPECT_THROW(std::ignore = d1 + d2, std::runtime_error);
}

TEST(ValueTest, Arithmetic_WithDoublesAndIntegers_CalculatesExpectedValues) {
  Value a(3.0);
  Value b(1.5);
  Value c(1.0);
  Value d(2.0);
  Value e(6.0);
  Value i1(7);
  Value i2(2);

  Value add_res = a + b;
  Value sub_res = a - c;
  Value mul_res = d * a;
  Value div_res = e / d;
  Value idiv_res = i1 / i2;

  EXPECT_EQ(add_res, Value(4.5));
  EXPECT_EQ(sub_res, Value(2.0));
  EXPECT_EQ(mul_res, Value(6.0));
  EXPECT_EQ(div_res, Value(3.0));
  EXPECT_EQ(idiv_res, Value(3));
}

TEST(ValueTest, EqualityOperator_WithDoubleEpsilon_TreatsCloseValuesAsEqual) {
  // Fixed: equality is exact (plus NaN canonicalization).  The old 1e-9
  // epsilon made == non-transitive, contradicted operator<, and disagreed
  // with std::hash<Value>, breaking unordered containers on double keys.
  const Value one(1.0);
  const Value neighbor(std::nextafter(1.0, 2.0));
  const Value different(1.1);
  const Value nan(std::numeric_limits<double>::quiet_NaN());
  const Value neg_zero(-0.0);
  const Value pos_zero(0.0);

  EXPECT_FALSE(one == neighbor);
  EXPECT_TRUE(one != neighbor);
  EXPECT_TRUE(one < neighbor);
  EXPECT_FALSE(one == different);
  EXPECT_TRUE(nan == nan);
  EXPECT_TRUE(nan != one);
  EXPECT_TRUE(neg_zero == pos_zero);
}

TEST(ValueTest, Hash_WithCloseDoubleValues_DifferentiatesDistinctBitPatterns) {
  const double a = 1.0;
  const double b = std::nextafter(a, 2.0);
  std::hash<Value> hasher;
  std::unordered_set<Value> seen;

  seen.insert(Value(a));

  EXPECT_NE(hasher(Value(a)), hasher(Value(b)));
  EXPECT_EQ(seen.count(Value(b)), 0U);
}

TEST(ValueTest, Hash_IsConsistentWithEquality) {
  // (a == b) must imply hash(a) == hash(b) for unordered containers.
  std::hash<Value> hasher;
  const Value one(1.0);
  const Value one_again(1.0);
  const Value neg_zero(-0.0);
  const Value pos_zero(0.0);
  const Value nan(std::numeric_limits<double>::quiet_NaN());
  const Value nan_again(-std::numeric_limits<double>::quiet_NaN());

  EXPECT_TRUE(one == one_again);
  EXPECT_EQ(hasher(one), hasher(one_again));
  EXPECT_TRUE(neg_zero == pos_zero);
  EXPECT_EQ(hasher(neg_zero), hasher(pos_zero));
  EXPECT_TRUE(nan == nan_again);
  EXPECT_EQ(hasher(nan), hasher(nan_again));

  std::unordered_set<Value> seen;
  seen.insert(nan);
  seen.insert(neg_zero);
  EXPECT_EQ(seen.size(), 2U);
  EXPECT_EQ(seen.count(nan_again), 1U);
  EXPECT_EQ(seen.count(pos_zero), 1U);
}

TEST(ValueTest, OrderingOperators_AreConsistentForNaN) {
  // NaN must not satisfy <= / >= against numbers: the old `!>`/`!<` forms
  // made `NaN <= 1.0` true while `NaN < 1.0` and `NaN == 1.0` were false.
  const Value nan(std::numeric_limits<double>::quiet_NaN());
  const Value one(1.0);

  EXPECT_FALSE(nan <= one);
  EXPECT_FALSE(nan >= one);
  EXPECT_FALSE(nan < one);
  EXPECT_FALSE(nan > one);
  EXPECT_FALSE(one <= nan);
  EXPECT_FALSE(one >= nan);
  EXPECT_TRUE(one <= one);
  EXPECT_TRUE(one >= one);
}

TEST(ValueTest, BitwiseAndModulo_WithIntegersAndDoubles_ComputesOrThrows) {
  Value i7(7);
  Value i6(6);
  Value i4(4);
  Value i3(3);
  Value i1(1);
  Value d1(1.0);
  Value d2(2.0);

  Value mod_res = i7 % i3;
  Value and_res = i6 & i3;
  Value or_res = i4 | i1;
  Value xor_res = i6 ^ i3;

  EXPECT_EQ(mod_res, Value(1));
  EXPECT_EQ(and_res, Value(2));
  EXPECT_EQ(or_res, Value(5));
  EXPECT_EQ(xor_res, Value(5));
  EXPECT_THROW(std::ignore = d1 % d2, std::runtime_error);
  EXPECT_THROW(std::ignore = d1 & d2, std::runtime_error);
  EXPECT_THROW(std::ignore = d1 | d2, std::runtime_error);
  EXPECT_THROW(std::ignore = d1 ^ d2, std::runtime_error);
}

TEST(ValueTest, Date_MemcomparableEncoding_PreservesDateAndDays) {
  Value date = Value::Date("2020-01-02");

  std::string encoded = date.EncodeMemcomparableFormat();
  Value decoded;
  decoded.DecodeMemcomparableFormat(encoded.c_str());

  ASSERT_EQ(date.type, ValueType::kDate);
  EXPECT_EQ(decoded, date);
  EXPECT_EQ(decoded.DateDays(), date.DateDays());
  EXPECT_EQ(Value::DateFromDays(date.DateDays()), date);
}

TEST(ValueTest, Hash_DiverseValues_ProducesConsistentAndDistinctHashes) {
  std::hash<Value> hasher;
  Value v1(1);
  Value v2(2);
  Value va("a");
  Value vb("b");
  Value vd1(1.0);
  Value vd2(2.0);
  Value vd15(1.5);

  EXPECT_NE(hasher(v1), hasher(v2));
  EXPECT_NE(hasher(va), hasher(vb));
  EXPECT_NE(hasher(vd1), hasher(vd2));
  EXPECT_EQ(hasher(va), hasher(Value("a")));
  EXPECT_EQ(hasher(v1), hasher(Value(1)));
  EXPECT_EQ(hasher(vd15), hasher(Value(1.5)));
}

TEST(ValueTest, Truthy_VariousTypes_ReturnsExpectedBoolean) {
  Value int_zero(0);
  Value int_one(1);
  Value int_neg(-1);
  Value empty_str("");
  Value double_zero(0.0);

  EXPECT_FALSE(int_zero.Truthy());
  EXPECT_TRUE(int_one.Truthy());
  EXPECT_TRUE(int_neg.Truthy());
  EXPECT_TRUE(empty_str.Truthy());
  EXPECT_TRUE(double_zero.Truthy());
}

TEST(ValueTest,
     ComparisonOperators_WithIntegers_ReturnsConsistentBooleanResults) {
  Value v1(1);
  Value v2(2);

  EXPECT_TRUE(v1 <= Value(1));
  EXPECT_TRUE(v1 >= Value(1));
  EXPECT_TRUE(v1 != v2);
  EXPECT_FALSE(v1 > Value(1));
  EXPECT_FALSE(v1 < Value(1));
  EXPECT_TRUE(v2 > v1);
  EXPECT_TRUE(v1 <= v2);
}

TEST(DateTest, ParseDateDays_WithMalformedStrings_ThrowsRuntimeError) {
  EXPECT_THROW(std::ignore = ParseDateDays("not-a-date"), std::runtime_error);
  EXPECT_THROW(std::ignore = ParseDateDays("2024-13-01"), std::runtime_error);
  EXPECT_THROW(std::ignore = ParseDateDays("2024-02-30"), std::runtime_error);
  EXPECT_THROW(std::ignore = ParseDateDays("2023-02-29"), std::runtime_error);
  EXPECT_EQ(ParseDateDays("2024-1-1"), ParseDateDays("2024-01-01"));
}

TEST(DateTest, ParseAndFormat_ValidDates_RoundTripsPreservingChronology) {
  const std::vector<std::string> dates = {"2020-02-29", "2024-12-31",
                                          "2000-01-01", "1999-07-04"};

  for (const std::string& date : dates) {
    const int64_t days = ParseDateDays(date);
    EXPECT_EQ(FormatDateDays(days), date);
  }
  EXPECT_LT(ParseDateDays("1999-01-01"), ParseDateDays("2000-01-01"));
  EXPECT_LT(ParseDateDays("2020-01-01"), ParseDateDays("2020-01-02"));
}

TEST(DateTest,
     AddDateIntervalDays_DayMonthYearIntervals_CalculatesCorrectDates) {
  int64_t d_jan31_2024 = ParseDateDays("2024-01-31");
  int64_t d_mar01_2024 = ParseDateDays("2024-03-01");
  int64_t d_jan31_2023 = ParseDateDays("2023-01-31");
  int64_t d_aug31_2024 = ParseDateDays("2024-08-31");
  int64_t d_feb29_2024 = ParseDateDays("2024-02-29");
  int64_t d_feb29_2020 = ParseDateDays("2020-02-29");
  int64_t d_jan15_2024 = ParseDateDays("2024-01-15");

  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(d_jan31_2024, 1, "day")),
            "2024-02-01");
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(d_mar01_2024, -1, "day")),
            "2024-02-29");
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(d_jan31_2024, 1, "month")),
            "2024-02-29");
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(d_jan31_2023, 1, "month")),
            "2023-02-28");
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(d_aug31_2024, 1, "month")),
            "2024-09-30");
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(d_feb29_2024, 1, "year")),
            "2025-02-28");
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(d_feb29_2020, 4, "year")),
            "2024-02-29");
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(d_jan15_2024, 2, "months")),
            "2024-03-15");
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(d_jan15_2024, 2, "years")),
            "2026-01-15");
}

TEST(DateTest, AddDateIntervalDays_WithUnknownUnits_ThrowsRuntimeError) {
  int64_t base_date = ParseDateDays("2024-01-01");

  EXPECT_THROW(std::ignore = AddDateIntervalDays(base_date, 1, "week"),
               std::runtime_error);
  EXPECT_THROW(std::ignore = AddDateIntervalDays(base_date, 1, "hour"),
               std::runtime_error);
}

TEST(DateTest, ValueDateFromDays_ValidDays_ConstructsExpectedValue) {
  const std::vector<int64_t> day_counts = {ParseDateDays("1970-01-01"),
                                           ParseDateDays("2020-02-29"),
                                           ParseDateDays("2038-01-19")};

  for (int64_t days : day_counts) {
    const Value value = Value::DateFromDays(days);
    EXPECT_EQ(value.DateDays(), days);
    EXPECT_EQ(value.type, ValueType::kDate);
  }
}

TEST(ValueTest, DateDays_OnNonDateTypes_ThrowsRuntimeError) {
  Value v_int(1);
  Value v_str("x");
  Value v_double(1.5);
  Value v_null;

  EXPECT_THROW(std::ignore = v_int.DateDays(), std::runtime_error);
  EXPECT_THROW(std::ignore = v_str.DateDays(), std::runtime_error);
  EXPECT_THROW(std::ignore = v_double.DateDays(), std::runtime_error);
  EXPECT_THROW(std::ignore = v_null.DateDays(), std::runtime_error);
}

TEST(ValueTest, Comparison_BetweenDifferentTypes_ThrowsRuntimeError) {
  Value v_int(1);
  Value v_str("a");
  Value v_double(1.5);

  EXPECT_THROW(std::ignore = (v_int < v_str), std::runtime_error);
  EXPECT_THROW(std::ignore = (v_int > v_str), std::runtime_error);
  EXPECT_THROW(std::ignore = (v_double < v_int), std::runtime_error);
  EXPECT_THROW(std::ignore = (v_str > v_double), std::runtime_error);
}

TEST(ValueTest, Comparison_WithNullValues_ThrowsForOrderingAndAllowsEquality) {
  Value null1;
  Value null2;

  EXPECT_THROW(std::ignore = (null1 < null2), std::runtime_error);
  EXPECT_THROW(std::ignore = (null1 > null2), std::runtime_error);
  EXPECT_TRUE(null1 == null2);
}

TEST(ValueTest, Arithmetic_WithIncompatibleMixedTypes_ThrowsRuntimeError) {
  Value v_int(1);
  Value v_str("a");
  Value v_double(1.5);

  EXPECT_THROW(std::ignore = (v_int + v_str), std::runtime_error);
  EXPECT_THROW(std::ignore = (v_int - v_str), std::runtime_error);
  EXPECT_THROW(std::ignore = (v_int * v_str), std::runtime_error);
  EXPECT_THROW(std::ignore = (v_int / v_str), std::runtime_error);
  EXPECT_THROW(std::ignore = (v_int % v_str), std::runtime_error);
  EXPECT_THROW(std::ignore = (v_int & v_str), std::runtime_error);
  EXPECT_THROW(std::ignore = (v_int | v_str), std::runtime_error);
  EXPECT_THROW(std::ignore = (v_int ^ v_str), std::runtime_error);
  EXPECT_THROW(std::ignore = (v_double - v_int), std::runtime_error);
  EXPECT_THROW(std::ignore = (v_str / v_double), std::runtime_error);
}

TEST(ValueTest, Arithmetic_OnNonNumericTypes_ThrowsRuntimeError) {
  Value va("a");
  Value vb("b");
  Value d1 = Value::DateFromDays(1);
  Value d2 = Value::DateFromDays(2);

  EXPECT_THROW(std::ignore = (va - vb), std::runtime_error);
  EXPECT_THROW(std::ignore = (va * vb), std::runtime_error);
  EXPECT_THROW(std::ignore = (va / vb), std::runtime_error);
  EXPECT_THROW(std::ignore = (va % vb), std::runtime_error);
  EXPECT_THROW(std::ignore = (va & vb), std::runtime_error);
  EXPECT_THROW(std::ignore = (va | vb), std::runtime_error);
  EXPECT_THROW(std::ignore = (va ^ vb), std::runtime_error);
  EXPECT_THROW(std::ignore = (d1 - d2), std::runtime_error);
}

TEST(ValueTest, Operations_OnInvalidValueType_ThrowsRuntimeError) {
  Value broken;
  broken.type = static_cast<ValueType>(
      99);  // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
  std::array<char, 16> buffer{};

  EXPECT_THROW(std::ignore = broken.Size(), std::runtime_error);
  EXPECT_THROW(broken.Serialize(buffer.data()), std::runtime_error);
  EXPECT_THROW(
      broken.Deserialize(
          buffer.data(),
          static_cast<ValueType>(
              99)),  // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
      std::runtime_error);
  EXPECT_THROW(std::ignore = broken.AsString(), std::runtime_error);
  EXPECT_THROW(std::ignore = broken.EncodeMemcomparableFormat(),
               std::runtime_error);
  EXPECT_THROW(std::ignore = (broken < broken), std::runtime_error);
  EXPECT_THROW(std::ignore = (broken > broken), std::runtime_error);
  EXPECT_THROW(std::ignore = (broken == broken), std::runtime_error);
  EXPECT_THROW(std::hash<Value>{}(broken), std::runtime_error);
}

TEST(ValueTest,
     DecodeMemcomparableFormat_WithInvalidPrefix_ThrowsRuntimeError) {
  Value v;

  EXPECT_THROW(v.DecodeMemcomparableFormat("\x00"), std::runtime_error);
  EXPECT_THROW(v.DecodeMemcomparableFormat("\x06"), std::runtime_error);
}

TEST(ValueTest, Hash_WithNullValue_ReturnsStableConstant) {
  std::hash<Value> hasher;
  Value null1;
  Value null2;

  size_t h1 = hasher(null1);
  size_t h2 = hasher(null2);

  EXPECT_EQ(h1, h2);
  EXPECT_EQ(h1, 0x9e3779b97f4a7c15ULL);
}

TEST(ValueTest, UnorderedContainers_WithNullKeys_HandlesLookupAndCounting) {
  std::unordered_set<Value> distinct;
  std::unordered_map<Value, int> groups;

  distinct.insert(Value());
  size_t count_null = distinct.count(Value());
  size_t size_one = distinct.size();
  distinct.insert(Value(1));
  size_t size_two = distinct.size();

  ++groups[Value()];
  ++groups[Value()];
  int group_count = groups[Value()];

  EXPECT_EQ(count_null, 1U);
  EXPECT_EQ(size_one, 1U);
  EXPECT_EQ(size_two, 2U);
  EXPECT_EQ(group_count, 2);
}

TEST(ValueTest,
     EncodeMemcomparableFormat_VarcharBoundaryShapes_PreservesRelativeOrder) {
  std::vector<Value> prefixes = {Value(""), Value("a"), Value("ab"),
                                 Value("abc")};
  std::vector<Value> nul_bytes = {
      Value(std::string("a\0b", 3)), Value(std::string("a", 1)),
      Value(std::string("a\0", 2)), Value(std::string("a!", 2))};
  std::vector<Value> high_bit = {
      Value(std::string("\x01", 1)), Value(std::string("\x7f", 1)),
      Value(std::string("\x80", 1)), Value(std::string("\xff", 1))};
  std::vector<Value> long_prefixes = {
      Value(std::string(64, 'x') + "a"),
      Value(std::string(64, 'x') + "b"),
      Value(std::string(63, 'x') + "za"),
      Value(std::string(65, 'x')),
  };

  MemcomparableFormatEncodeTest(prefixes);
  MemcomparableFormatEncodeTest(nul_bytes);
  MemcomparableFormatEncodeTest(high_bit);
  MemcomparableFormatEncodeTest(long_prefixes);
}

TEST(ValueTest,
     EncodeMemcomparableFormat_EmptyAndNonEmptyVarchar_SortsEmptyFirst) {
  Value empty_val("");
  Value one_val("a");

  const std::string empty = empty_val.EncodeMemcomparableFormat();
  const std::string one = one_val.EncodeMemcomparableFormat();

  EXPECT_LT(empty, one);
  EXPECT_NE(empty, one);
}

TEST(ValueTest, ToString_AllUnaryOperations_FormatsCorrectStrings) {
  std::ostringstream oss;

  oss << UnaryOperation::kIsTrue << "|" << UnaryOperation::kIsNotTrue << "|"
      << UnaryOperation::kIsFalse << "|" << UnaryOperation::kIsNotFalse;

  EXPECT_EQ(oss.str(), "IS TRUE|IS NOT TRUE|IS FALSE|IS NOT FALSE");
}

TEST(ValueTest, Deserialize_WithUndefinedValueType_ThrowsRuntimeError) {
  Value v;
  char buf[16]{};

  EXPECT_THROW(v.Deserialize(buf, static_cast<ValueType>(99)),
               std::runtime_error);
}

TEST(ValueTest, SkipSerialized_WithNullOrUndefinedType_ThrowsRuntimeError) {
  char buf[16]{};

  EXPECT_THROW(std::ignore = Value::SkipSerialized(nullptr, ValueType::kNull),
               std::runtime_error);
  EXPECT_THROW(
      std::ignore = Value::SkipSerialized(buf, static_cast<ValueType>(99)),
      std::runtime_error);
}

TEST(ValueTest, AsString_SpecialDoubles_FormatsInfAndNan) {
  Value inf_val(std::numeric_limits<double>::infinity());
  Value neg_inf_val(-std::numeric_limits<double>::infinity());
  Value nan_val(std::numeric_limits<double>::quiet_NaN());

  EXPECT_EQ(inf_val.AsString(), "inf");
  EXPECT_EQ(neg_inf_val.AsString(), "-inf");
  EXPECT_EQ(nan_val.AsString(), "nan");
}

TEST(ValueTest, EncodeMemcomparableFormat_OnNullValue_ThrowsRuntimeError) {
  Value null_val;

  EXPECT_THROW(std::ignore = null_val.EncodeMemcomparableFormat(),
               std::runtime_error);
}

TEST(ValueTest,
     EncodeMemcomparableFormat_ArrayWithNullElement_RoundTripsDecodedArray) {
  Value arr =
      Value::Array({Value(int64_t{1}), Value(), Value(int64_t{3})}, "INT64");

  std::string encoded = arr.EncodeMemcomparableFormat();
  Value decoded;
  decoded.DecodeMemcomparableFormat(encoded.data());

  EXPECT_EQ(decoded, arr);
}

TEST(ValueTest, GreaterThan_WithArrays_ComparesElementsCorrectly) {
  Value a = Value::Array({Value(int64_t{1})}, "INT64");
  Value b = Value::Array({Value(int64_t{2})}, "INT64");

  EXPECT_TRUE(b > a);
  EXPECT_FALSE(a > b);
}

TEST(ValueTest, Arithmetic_EdgeCasesAndOverflow_ThrowsRuntimeError) {
  Value min_int(std::numeric_limits<int64_t>::min());
  Value one(int64_t{1});
  Value zero(0);
  Value ten(10);
  Value neg_one(int64_t{-1});

  EXPECT_THROW(std::ignore = (min_int - one), std::runtime_error);
  EXPECT_THROW(std::ignore = (ten / zero), std::runtime_error);
  EXPECT_THROW(std::ignore = (min_int / neg_one), std::runtime_error);
}

TEST(ValueTest, Serialize_ArrayWithEncoderDecoder_RoundTripsSuccessfully) {
  Value arr = Value::Array({Value(int64_t{7}), Value(int64_t{8})}, "INT64");
  std::stringstream ss;
  Encoder enc(ss);

  enc << arr;
  Value restored;
  Decoder dec(ss);
  dec >> restored;

  EXPECT_EQ(restored, arr);
}

TEST(DateTest, FormatDateDays_DaysOutOfRange_ThrowsRuntimeError) {
  int64_t underflow_days = -20000000LL;
  int64_t overflow_days = 20000000LL;

  EXPECT_THROW(std::ignore = FormatDateDays(underflow_days),
               std::runtime_error);
  EXPECT_THROW(std::ignore = FormatDateDays(overflow_days), std::runtime_error);
}

TEST(DateTest, AddDateIntervalDays_YearOverflow_ThrowsRuntimeError) {
  int64_t max_date = ParseDateDays("9999-12-31");
  int64_t min_date = ParseDateDays("0001-01-01");

  EXPECT_THROW(std::ignore = AddDateIntervalDays(max_date, 999999, "year"),
               std::runtime_error);
  EXPECT_THROW(std::ignore = AddDateIntervalDays(min_date, -999999, "year"),
               std::runtime_error);
}

TEST(DateTest, ParseDateDays_InvalidDateStrings_ThrowsRuntimeError) {
  int64_t valid_date = ParseDateDays("2024-01-01");

  EXPECT_THROW(std::ignore = ParseDateDays("999999999-01-01"),
               std::runtime_error);
  EXPECT_THROW(std::ignore = ParseDateDays("2024-01-01xyz"),
               std::runtime_error);
  EXPECT_THROW(std::ignore = ParseDateDays("2024-01-01 "), std::runtime_error);
  EXPECT_THROW(std::ignore = ParseDateDays("99999999-01-01"),
               std::runtime_error);
  EXPECT_THROW(std::ignore = ParseDateDays("-2024-01-01"), std::runtime_error);
  EXPECT_THROW(std::ignore = ParseDateDays("2024/01/01"), std::runtime_error);
  EXPECT_THROW(std::ignore = ParseDateDays("2024-02-30"), std::runtime_error);
  EXPECT_THROW(std::ignore = ParseDateDays(""), std::runtime_error);
  EXPECT_THROW(std::ignore = AddDateIntervalDays(
                   valid_date, std::numeric_limits<int64_t>::max(), "day"),
               std::runtime_error);
  EXPECT_THROW(std::ignore = AddDateIntervalDays(
                   valid_date, std::numeric_limits<int64_t>::min(), "day"),
               std::runtime_error);
}

TEST(DateTest, SetDefaultTimeZone_CustomAndEmpty_UpdatesOrResetsTimeZone) {
  SetDefaultTimeZone("UTC");
  std::string utc_tz = GetDefaultTimeZone();

  SetDefaultTimeZone("");
  std::string default_tz = GetDefaultTimeZone();

  EXPECT_EQ(utc_tz, "UTC");
  EXPECT_EQ(default_tz, "America/Los_Angeles");
}

TEST(IntervalTest,
     ParseAndJustify_VariousIntervalFormats_ComputesExpectedValues) {
  IntervalValue iv = IntervalValue::Parse("  P1Y");
  SetSessionConstant("foo_cov", "bar_cov");

  IntervalValue neg_days{0, -45, 0};
  IntervalValue justified_days = neg_days.JustifyDays();

  IntervalValue neg_nanos{0, 0, -(35LL * 24LL * 3600LL * 1000000000LL)};
  IntervalValue justified_nanos = neg_nanos.JustifyInterval();

  IntervalValue iv_min = IntervalValue::Parse("5", "minute");
  IntervalValue iv_quarter = IntervalValue::Parse("2", "quarter");
  IntervalValue iv_quarters = IntervalValue::Parse("3", "quarters");
  IntervalValue iv_week = IntervalValue::Parse("2", "week");
  IntervalValue iv_weeks = IntervalValue::Parse("1", "weeks");
  IntervalValue iv_days = IntervalValue::Parse("5", "days");
  IntervalValue iv_ms = IntervalValue::Parse("100", "milliseconds");
  IntervalValue iv_us = IntervalValue::Parse("500", "microseconds");
  IntervalValue iv_ns = IntervalValue::Parse("42", "nanoseconds");
  IntervalValue iv_hours = IntervalValue::Parse("-2", "hours");
  IntervalValue iv_minutes = IntervalValue::Parse("+3", "minutes");

  IntervalValue iso_ymd = IntervalValue::Parse("P1Y2M3D");
  IntervalValue iso_hms = IntervalValue::Parse("PT1H2M3S");
  IntervalValue iso_complex = IntervalValue::Parse("P+1Y-2M+3DT-1H+2M-3.5S");
  IntervalValue iso_empty_p = IntervalValue::Parse("P");
  IntervalValue iso_empty_pt = IntervalValue::Parse("PT");

  IntervalValue iv_zero{};
  IntervalValue j_zero = iv_zero.JustifyInterval();
  IntervalValue j_days = iv_zero.JustifyDays();
  IntervalValue j_hours = iv_zero.JustifyHours();

  EXPECT_EQ(iv.months, 12);
  EXPECT_EQ(GetSessionConstant("foo_cov"), "bar_cov");
  EXPECT_TRUE(HasSessionConstant("foo_cov"));
  EXPECT_EQ(justified_days.months, -1);
  EXPECT_EQ(justified_days.days, -15);
  EXPECT_LT(justified_nanos.months, 0);

  EXPECT_THROW(std::ignore = IntervalValue::Parse("P1X"), std::runtime_error);
  EXPECT_THROW(std::ignore = IntervalValue::Parse("PT1X"), std::runtime_error);
  EXPECT_THROW(std::ignore = IntervalValue::Parse("P1"), std::runtime_error);
  EXPECT_THROW(std::ignore = IntervalValue::Parse("P-X"), std::runtime_error);

  EXPECT_EQ(iv_min.nanos, 5LL * 60LL * 1000000000LL);
  EXPECT_EQ(iv_quarter.months, 6);
  EXPECT_EQ(iv_quarters.months, 9);
  EXPECT_EQ(iv_week.days, 14);
  EXPECT_EQ(iv_weeks.days, 7);
  EXPECT_EQ(iv_days.days, 5);
  EXPECT_EQ(iv_ms.nanos, 100000000);
  EXPECT_EQ(iv_us.nanos, 500000);
  EXPECT_EQ(iv_ns.nanos, 42);
  EXPECT_EQ(iv_hours.nanos, -7200000000000LL);
  EXPECT_EQ(iv_minutes.nanos, 180000000000LL);

  EXPECT_EQ(iso_ymd.months, 14);
  EXPECT_EQ(iso_hms.nanos, (3600 + 120 + 3) * 1000000000LL);
  EXPECT_EQ(iso_complex.months, 10);
  EXPECT_EQ(iso_empty_p.months, 0);
  EXPECT_EQ(iso_empty_pt.nanos, 0);

  EXPECT_EQ(j_zero.months, 0);
  EXPECT_EQ(j_zero.days, 0);
  EXPECT_EQ(j_zero.nanos, 0);
  EXPECT_EQ(j_days.months, 0);
  EXPECT_EQ(j_hours.days, 0);
}

TEST(IntervalTest, JustifyHours_WithHugeInterval_ThrowsInsteadOfWrapping) {
  // Fixed: days * kDayNanos overflowed silently for parseable intervals,
  // returning garbage (UB). It must now throw like the date layer does.
  const IntervalValue huge = IntervalValue::Parse("P4000000000D");
  EXPECT_THROW(std::ignore = huge.JustifyHours(), std::runtime_error);
  EXPECT_THROW(std::ignore = huge.JustifyInterval(), std::runtime_error);
}

TEST(IntervalTest, Comparison_WithHugeIntervals_KeepsOrderAndEquality) {
  // Fixed: operator== / <=> used to fold via mod-2^64 arithmetic, making
  // distinct huge intervals "equal".
  const IntervalValue big = IntervalValue::Parse("P296Y");
  const IntervalValue small = IntervalValue::Parse("P295Y");
  EXPECT_FALSE(big == small);
  EXPECT_TRUE(small < big);
  EXPECT_TRUE(big == big);

  // Beyond ~3558 years the total nanoseconds no longer fit in int64_t.
  EXPECT_THROW(std::ignore = IntervalValue::Parse("P99999999Y").TotalNanos(),
               std::runtime_error);
}

TEST(IntervalTest, Arithmetic_WithHugeOperands_ThrowsOnOverflow) {
  const IntervalValue huge = IntervalValue::Parse("P4000000000D");
  EXPECT_THROW(std::ignore = huge + huge, std::runtime_error);
  EXPECT_THROW(std::ignore = huge * 2, std::runtime_error);
  EXPECT_THROW(std::ignore = -huge, std::runtime_error);

  const IntervalValue normal = IntervalValue::Parse("P1D");
  const IntervalValue sum = normal + normal;
  EXPECT_EQ(sum.days, 2);
  EXPECT_EQ(sum, IntervalValue(0, 2, 0));
}

TEST(ValueTypeTest, ValueTypeToString_AllEnumValues_ReturnsExpectedString) {
  EXPECT_EQ(ValueTypeToString(ValueType::kNull), "(null)");
  EXPECT_EQ(ValueTypeToString(ValueType::kInt64), "Integer");
  EXPECT_EQ(ValueTypeToString(ValueType::kVarChar), "Varchar");
  EXPECT_EQ(ValueTypeToString(ValueType::kDouble), "Double");
  EXPECT_EQ(ValueTypeToString(ValueType::kDate), "Date");
  EXPECT_EQ(ValueTypeToString(ValueType::kArray), "Array");
  EXPECT_EQ(ValueTypeToString(static_cast<ValueType>(99)),
            "unknown value type");
}

TEST(FunctionTest, Serialize_CustomFunction_RoundTripsThroughDecoder) {
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

  std::stringstream ss2;
  Encoder enc2(ss2);
  enc2 << restored;

  EXPECT_EQ(ss.str(), ss2.str());
}

TEST(TypeTest, Serialize_TypeTag_RoundTripsThroughDecoder) {
  Type t;
  Type original(TypeTag::kVarChar);
  std::stringstream ss;
  Encoder enc(ss);

  enc << original;
  Type restored;
  Decoder dec(ss);
  dec >> restored;

  EXPECT_EQ(t.GetType(), TypeTag::kInvalid);
  EXPECT_FALSE(t.IsValid());
  EXPECT_EQ(restored.GetType(), TypeTag::kVarChar);
  EXPECT_TRUE(restored.IsValid());
}

TEST(ValueTest,
     Comparison_WithIntervalLikeAndGenericStrings_OrdersLexicographically) {
  Value iv1("1-2 3 4:5:6");
  Value iv2("2-0 0 0:0:0");
  Value s1("foo-bar");
  Value s2("hello");

  EXPECT_TRUE(iv1 < iv2);
  EXPECT_FALSE(iv2 < iv1);
  EXPECT_TRUE(iv2 > iv1);
  EXPECT_FALSE(iv1 > iv2);
  EXPECT_TRUE(s1 < s2 || s2 < s1);
  EXPECT_TRUE(s1 > s2 || s2 > s1);
}

}  // namespace tinylamb
