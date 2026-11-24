/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#include "common/serdes.hpp"

#include <array>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

#include "gtest/gtest.h"

namespace tinylamb {

namespace {
// A buffer wide enough for any payload this suite serializes, poisoned so a
// write past the length prefix shows up as a byte compare failure.
constexpr size_t kBufSize = 128;

struct RoundTripCase {
  std::string_view name;
  std::string_view payload;
};

// Short strings exercise the inline fast path. Separate cases cover
// truncation and the 64 KiB length boundary.
constexpr std::array<RoundTripCase, 5> kRoundTrip = {
    {RoundTripCase{.name = "empty", .payload = std::string_view()},
     RoundTripCase{.name = "one byte", .payload = std::string_view("x")},
     RoundTripCase{.name = "typical",
                   .payload = std::string_view("hello, serdes")},
     RoundTripCase{.name = "embedded nul",
                   .payload = std::string_view("a\0b", 3)},
     RoundTripCase{.name = "binary",
                   .payload = std::string_view("\xff\x00\x01\xfe", 4)}}};
}  // namespace

TEST(SerdesStringTest, RoundTripsThroughLengthPrefix) {
  for (const RoundTripCase& test : kRoundTrip) {
    SCOPED_TRACE(test.name);
    std::string buf(kBufSize, '\xAB');
    const size_t written = SerializeStringView(buf.data(), test.payload);
    EXPECT_EQ(written, sizeof(uint16_t) + test.payload.size());
    // The 2-byte length prefix lands first, then exactly the payload; the
    // poison bytes past it are untouched.
    std::string_view body(buf.data() + sizeof(uint16_t), test.payload.size());
    EXPECT_EQ(body, test.payload);
    EXPECT_EQ(buf[written], '\xAB');

    std::string_view decoded;
    const size_t read =
        DeserializeStringView(buf.data(), buf.data() + written, &decoded);
    EXPECT_EQ(read, written);
    EXPECT_EQ(decoded, test.payload);
  }
}

TEST(SerdesStringTest, LargestLengthRoundTripsAndOverflowIsRejected) {
  constexpr size_t max_length = std::numeric_limits<uint16_t>::max();
  std::string payload(max_length, 'x');
  std::string buffer(sizeof(uint16_t) + max_length + 1, '\0');
  const auto encoded = SerializeStringView(buffer.data(), payload);
  ASSERT_EQ(encoded, sizeof(uint16_t) + max_length);
  std::string_view decoded;
  EXPECT_EQ(
      DeserializeStringView(buffer.data(), buffer.data() + encoded, &decoded),
      encoded);
  EXPECT_EQ(decoded, payload);
  payload.push_back('x');
  EXPECT_DEATH(SerializeStringView(buffer.data(), payload),
               "string too long to serialize");
}

TEST(SerdesStringTest, DeserializeRejectsTruncatedPrefix) {
  const std::string payload = "abc";
  std::string buf(kBufSize, '\0');
  const size_t written = SerializeStringView(buf.data(), payload);
  ASSERT_GT(written, sizeof(uint16_t));

  std::string_view decoded = "untouched";
  EXPECT_EQ(DeserializeStringView(buf.data(), buf.data() + 1, &decoded), 0U);
  EXPECT_EQ(decoded, "untouched");
}

TEST(SerdesStringTest, DeserializeRejectsTruncatedPayload) {
  const std::string payload = "abcdef";
  std::string buf(kBufSize, '\0');
  const size_t written = SerializeStringView(buf.data(), payload);
  ASSERT_GT(written, sizeof(uint16_t) + payload.size() - 1);

  // The prefix claims six bytes but only five follow.
  std::string_view decoded = "untouched";
  const size_t read = DeserializeStringView(
      buf.data(), buf.data() + sizeof(uint16_t) + payload.size() - 1, &decoded);
  EXPECT_EQ(read, 0U);
  EXPECT_EQ(decoded, "untouched");
}

TEST(SerdesStringTest, DeserializeRejectsReversedRange) {
  constexpr std::string_view input = "abc";
  std::string_view out = "unchanged";
  EXPECT_EQ(DeserializeStringView(input.data() + 2, input.data() + 1, &out),
            0U);
  EXPECT_EQ(out, "unchanged");
}

TEST(SerdesStringTest, DeserializeRejectsNullEnd) {
  std::string buf(kBufSize, '\0');
  SerializeStringView(buf.data(), "abc");
  std::string_view decoded = "untouched";
  EXPECT_EQ(DeserializeStringView(buf.data(), nullptr, &decoded), 0U);
  EXPECT_EQ(decoded, "untouched");
}

TEST(SerdesStringTest, FixedWidthScalarsRoundTripBigEndian) {
  std::string buf(kBufSize, '\0');
  size_t off = 0;
  const uint16_t u16 = 0x1234;
  const uint32_t u32 = 0xdeadbeef;
  const uint64_t u64 = 0x0102030405060708;
  off += SerializeU16(buf.data() + off, u16);
  off += SerializeU32(buf.data() + off, u32);
  off += SerializeU64(buf.data() + off, u64);
  // Big-endian wire order: the most significant byte first.
  constexpr std::string_view wire(
      "\x12\x34\xde\xad\xbe\xef\x01\x02\x03\x04\x05\x06\x07\x08", 14);
  EXPECT_EQ(std::string_view(buf.data(), off), wire);

  uint16_t out16 = 0;
  uint32_t out32 = 0;
  uint64_t out64 = 0;
  size_t read = 0;
  read += DeserializeU16(buf.data() + read, &out16);
  read += DeserializeU32(buf.data() + read, &out32);
  read += DeserializeU64(buf.data() + read, &out64);
  EXPECT_EQ(read, off);
  EXPECT_EQ(out16, u16);
  EXPECT_EQ(out32, u32);
  EXPECT_EQ(out64, u64);
}

}  // namespace tinylamb
