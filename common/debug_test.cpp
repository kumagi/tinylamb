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

#include "common/debug.hpp"

#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/status_or.hpp"
#include "gtest/gtest.h"

namespace tinylamb {

TEST(DebugTest, Hex_WithAsciiStrings_ConvertsToHexPairs) {
  const std::string s_empty = "";
  const std::string s_a = "a";
  const std::string s_ab = "ab";

  const std::string h_empty = Hex(s_empty);
  const std::string h_a = Hex(s_a);
  const std::string h_ab = Hex(s_ab);

  EXPECT_EQ(h_empty, "");
  EXPECT_EQ(h_a, "61");
  EXPECT_EQ(h_ab, "61 62");
}

TEST(DebugTest, Hex_WithBinaryData_ConvertsToHexPairs) {
  const std::string bin("\x00\xff\x10", 3);

  const std::string h_bin = Hex(bin);

  EXPECT_EQ(h_bin, "00 ff 10");
}

TEST(DebugTest, OmittedString_WhenShorterThanLimit_ReturnsOriginalString) {
  const std::string s = "hello";

  const std::string out5 = OmittedString(s, 5);
  const std::string out100 = OmittedString(s, 100);

  EXPECT_EQ(out5, "hello");
  EXPECT_EQ(out100, "hello");
}

TEST(DebugTest, OmittedString_WhenLongerThanLimit_TruncatesMiddleWithEllipsis) {
  const std::string long_str(100, 'a');

  const std::string out = OmittedString(long_str, 10);

  EXPECT_NE(out, long_str);
  EXPECT_EQ(out.substr(0, 8), long_str.substr(0, 8));
  EXPECT_EQ(out.substr(out.size() - 8), long_str.substr(long_str.size() - 8));
}

TEST(
    DebugTest,
    OmittedString_WhenShorterThanEightBytes_DoesNotThrow) {  // Fixed: the old
                                                             // tail slice
                                                             // `substr(len -
                                                             // 8)` underflowed
                                                             // size_t and threw
  // std::out_of_range whenever limit < size < 8 (e.g. a 6-byte index key in
  // Dump output).
  EXPECT_NO_THROW(std::ignore = OmittedString("hello", 4));
  EXPECT_NO_THROW(std::ignore = OmittedString("abc", 2));
  EXPECT_NO_THROW(std::ignore = OmittedString("abcdef", 5));
  EXPECT_NO_THROW(std::ignore = OmittedString("abcdefgh", 3));
  EXPECT_EQ(OmittedString("hello", 4), "hello");
  EXPECT_EQ(OmittedString("abc", 2), "abc");

  const std::string long_str(100, 'x');
  const std::string out = OmittedString(long_str, 10);
  EXPECT_NE(out, long_str);
}

TEST(DebugTest, HeadString_WhenShorterThanLimit_ReturnsOriginalString) {
  const std::string s = "hello";

  const std::string out5 = HeadString(s, 5);
  const std::string out100 = HeadString(s, 100);

  EXPECT_EQ(out5, "hello");
  EXPECT_EQ(out100, "hello");
}

TEST(DebugTest, HeadString_WhenLongerThanLimit_TruncatesTail) {
  const std::string long_str(100, 'b');

  const std::string out = HeadString(long_str, 10);

  EXPECT_NE(out, long_str);
  EXPECT_EQ(out.substr(0, 8), long_str.substr(0, 8));
  EXPECT_NE(out, OmittedString(long_str, 10));
}

TEST(DebugTest, ConstantsToString_ForAllEnums_RendersExpectedLabels) {
  EXPECT_EQ(ToString(BinaryOperation::kModulo), "%");
  EXPECT_EQ(ToString(BinaryOperation::kXor), "XOR");
  EXPECT_EQ(ToString(static_cast<BinaryOperation>(99)), "INVALID");

  EXPECT_EQ(ToString(Status::kUnknown), "Unknown");
  EXPECT_EQ(ToString(Status::kSuccess), "Success");
  EXPECT_EQ(ToString(Status::kNoSpace), "NoSpace");
  EXPECT_EQ(ToString(Status::kDuplicates), "Duplicates");
  EXPECT_EQ(ToString(Status::kConflicts), "Conflicts");
  EXPECT_EQ(ToString(Status::kUnknownType), "UnknownType");
  EXPECT_EQ(ToString(Status::kNotExists), "NotExists");
  EXPECT_EQ(ToString(Status::kNotImplemented), "NotImplemented");
  EXPECT_EQ(ToString(Status::kTooBigData), "TooBigData");
  EXPECT_EQ(ToString(Status::kAmbiguousQuery), "AmbiguousQuery");
  EXPECT_EQ(ToString(Status::kIsInfinity), "IsInfinity");
  EXPECT_EQ(ToString(Status::kDeleted), "Deleted");
  EXPECT_EQ(ToString(Status::kCorrupt), "Corrupt");
  EXPECT_EQ(ToString(static_cast<Status>(999)), "INVALID STATUS");

  std::ostringstream oss;
  oss << Status::kSuccess;
  EXPECT_EQ(oss.str(), "Success");
}

TEST(DebugTest, EncoderDecoder_WithDiverseTypes_RoundTripsAccurately) {
  std::stringstream ss;
  Encoder enc(ss);
  const uint8_t u8 = 42;
  const uint32_t u32 = 12345;
  const slot_t slot = 99;
  const int64_t i64 = -9876543210LL;
  const uint64_t u64 = 9876543210ULL;
  const double d = 3.1415926535;
  const ValueType vt = ValueType::kDouble;
  const bool b = true;
  const std::string str = "hello encoder";
  const std::vector<int64_t> vec = {1, 2, 3, 4, 5};
  const std::pair<std::string, int64_t> pair_val = {"key", 100};

  enc << u8 << u32 << slot << i64 << u64 << d << vt << b
      << std::string_view(str) << vec << pair_val;

  Decoder dec(ss);
  uint8_t r_u8 = 0;
  uint32_t r_u32 = 0;
  slot_t r_slot = 0;
  int64_t r_i64 = 0;
  uint64_t r_u64 = 0;
  double r_d = 0.0;
  ValueType r_vt = ValueType::kNull;
  bool r_b = false;
  std::string r_str;
  std::vector<int64_t> r_vec;
  std::pair<std::string, int64_t> r_pair;

  dec >> r_u8 >> r_u32 >> r_slot >> r_i64 >> r_u64 >> r_d >> r_vt >> r_b >>
      r_str >> r_vec >> r_pair;

  EXPECT_EQ(r_u8, u8);
  EXPECT_EQ(r_u32, u32);
  EXPECT_EQ(r_slot, slot);
  EXPECT_EQ(r_i64, i64);
  EXPECT_EQ(r_u64, u64);
  EXPECT_EQ(r_d, d);
  EXPECT_EQ(r_vt, vt);
  EXPECT_EQ(r_b, b);
  EXPECT_EQ(r_str, str);
  EXPECT_EQ(r_vec, vec);
  EXPECT_EQ(r_pair, pair_val);

  std::stringstream ss_trunc;
  ss_trunc.write(
      "\x05\x00"
      "ab",
      4);
  Decoder dec_trunc(ss_trunc);
  std::string r_trunc;
  dec_trunc >> r_trunc;
  EXPECT_TRUE(ss_trunc.fail());
}

TEST(DecoderTest, Bool_NonCanonicalByte_NormalizesToTrue) {
  // Fixed: the raw byte used to become the object representation of bool
  // (UB for anything but 0/1 on a corrupted stream).
  std::stringstream ss;
  ss.put('\xFF');
  Decoder dec(ss);
  bool v = false;
  dec >> v;
  EXPECT_TRUE(v);
  EXPECT_TRUE(!ss.fail());

  std::stringstream ss_zero;
  ss_zero.put('\x00');
  Decoder dec_zero(ss_zero);
  bool v_zero = true;
  dec_zero >> v_zero;
  EXPECT_FALSE(v_zero);
}

TEST(DecoderTest, ValueType_OutOfRangeByte_ThrowsInsteadOfPoisoning) {
  // Fixed: the raw byte became the ValueType enum without validation, so a
  // corrupted stream materialized an invalid discriminant.
  std::stringstream ss;
  ss.put(static_cast<char>(99));
  Decoder dec(ss);
  ValueType v = ValueType::kNull;
  EXPECT_THROW(dec >> v, std::runtime_error);
}

TEST(StatusOrTest, MoveValue_SecondCallThrows) {
  // Fixed: MoveValue() left the optional engaged, so a second call returned a
  // second (moved-from) copy instead of throwing as documented.
  StatusOr<std::string> so(std::string("x"));
  EXPECT_TRUE(so.HasValue());
  std::string first = std::move(so).MoveValue();
  EXPECT_EQ(first, "x");
  EXPECT_FALSE(so.HasValue());
  EXPECT_THROW(std::ignore = so.MoveValue(), std::runtime_error);
  EXPECT_THROW(std::ignore = so.Value(), std::runtime_error);
}

TEST(StatusOrTest, Value_OnFailedStatus_Throws) {
  StatusOr<int> failed(Status::kNotExists);
  EXPECT_THROW(std::ignore = failed.Value(), std::runtime_error);
  EXPECT_THROW(std::ignore = failed.MoveValue(), std::runtime_error);
}

}  // namespace tinylamb
