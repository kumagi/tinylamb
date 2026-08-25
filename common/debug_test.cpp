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

#include "common/constants.hpp"
#include "common/debug.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/status_or.hpp"

#include <sstream>
#include <string>
#include <vector>
#include <utility>

#include "gtest/gtest.h"

namespace tinylamb {

TEST(DebugTest, Hex) {
  EXPECT_EQ(Hex(""), "");
  EXPECT_EQ(Hex("a"), "61");
  EXPECT_EQ(Hex("ab"), "61 62");
}

TEST(DebugTest, HexBinary) {
  EXPECT_EQ(Hex(std::string("\x00\xff\x10", 3)), "00 ff 10");
}

TEST(DebugTest, OmittedStringShort) {
  EXPECT_EQ(OmittedString("hello", 5), "hello");
  EXPECT_EQ(OmittedString("hello", 100), "hello");
}

TEST(DebugTest, OmittedStringLong) {
  std::string long_str(100, 'a');
  std::string out = OmittedString(long_str, 10);
  EXPECT_NE(out, long_str);
  EXPECT_EQ(out.substr(0, 8), long_str.substr(0, 8));
  EXPECT_EQ(out.substr(out.size() - 8), long_str.substr(long_str.size() - 8));
}

TEST(DebugTest, HeadStringShort) {
  EXPECT_EQ(HeadString("hello", 5), "hello");
  EXPECT_EQ(HeadString("hello", 100), "hello");
}

TEST(DebugTest, HeadStringLong) {
  std::string long_str(100, 'b');
  std::string out = HeadString(long_str, 10);
  EXPECT_NE(out, long_str);
  EXPECT_EQ(out.substr(0, 8), long_str.substr(0, 8));
  EXPECT_NE(out, OmittedString(long_str, 10));
}

TEST(DebugTest, ConstantsToStringCoverage) {
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

TEST(DebugTest, EncoderDecoderRoundTripTypes) {
  std::stringstream ss;
  Encoder enc(ss);
  uint8_t u8 = 42;
  uint32_t u32 = 12345;
  slot_t slot = 99;
  int64_t i64 = -9876543210LL;
  uint64_t u64 = 9876543210ULL;
  double d = 3.1415926535;
  ValueType vt = ValueType::kDouble;
  bool b = true;
  std::string str = "hello encoder";
  std::vector<int64_t> vec = {1, 2, 3, 4, 5};
  std::pair<std::string, int64_t> pair_val = {"key", 100};

  enc << u8 << u32 << slot << i64 << u64 << d << vt << b << std::string_view(str) << vec << pair_val;

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

  dec >> r_u8 >> r_u32 >> r_slot >> r_i64 >> r_u64 >> r_d >> r_vt >> r_b >> r_str >> r_vec >> r_pair;

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

  // Error paths: truncated string and empty string decode
  std::stringstream ss_trunc;
  ss_trunc.write("\x05\x00" "ab", 4); // claims 5 bytes but only 2 given
  Decoder dec_trunc(ss_trunc);
  std::string r_trunc;
  dec_trunc >> r_trunc;
  EXPECT_TRUE(ss_trunc.fail());
}

}  // namespace tinylamb


