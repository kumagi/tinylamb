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

#include "common/crc32c.hpp"

#include <array>
#include <cstdint>
#include <random>
#include <span>
#include <vector>

#include "gtest/gtest.h"

namespace tinylamb {

TEST(Crc32CTest, Crc32C_WhenDataEmpty_ReturnsZero) {
  const uint32_t crc = Crc32C(nullptr, 0);

  EXPECT_EQ(crc, 0U);
}

TEST(Crc32CTest, Crc32C_WithKnownAsciiVector_ComputesExpectedChecksum) {
  const char* data = "123456789";

  const uint32_t crc = Crc32C(data, 9);

  EXPECT_EQ(crc, 0xe3069283U);
}

// The multi-lane fast path must stay bit-identical to the software loop for
// every length, alignment and incoming state.
TEST(Crc32CTest, Crc32C_MatchesSoftwareLoop_AcrossSizesAndSeeds) {
  // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp)
  std::mt19937_64 rng(20260929);  // deterministic vectors
  std::vector<uint8_t> buf(8192 + 64);
  for (auto& byte : buf) {
    byte = static_cast<uint8_t>(rng());
  }

  // Boundary lengths around the 256-byte lane threshold and every tail shape.
  for (size_t length :
       {size_t{0},   size_t{1},   size_t{2},    size_t{3},    size_t{4},
        size_t{7},   size_t{8},   size_t{9},    size_t{15},   size_t{16},
        size_t{17},  size_t{63},  size_t{64},   size_t{65},   size_t{127},
        size_t{128}, size_t{255}, size_t{256},  size_t{257},  size_t{264},
        size_t{511}, size_t{512}, size_t{1024}, size_t{4096}, size_t{8192}}) {
    for (size_t offset = 0; offset < 16; ++offset) {
      ASSERT_LE(length + offset, buf.size());
      const auto raw = static_cast<uint32_t>(rng());
      EXPECT_EQ(detail::Crc32CRawHardware(
                    raw, std::span(buf).subspan(offset, length)),
                detail::Crc32CRawSoftware(
                    raw, std::span(buf).subspan(offset, length)))
          << "length=" << length << " offset=" << offset;
    }
  }

  // Randomized sweep.
  for (int trial = 0; trial < 256; ++trial) {
    const size_t length = rng() % 6000;
    const size_t offset = rng() % 16;
    ASSERT_LE(length + offset, buf.size());
    const auto raw = static_cast<uint32_t>(rng());
    EXPECT_EQ(
        detail::Crc32CRawHardware(raw, std::span(buf).subspan(offset, length)),
        detail::Crc32CRawSoftware(raw, std::span(buf).subspan(offset, length)))
        << "trial=" << trial;
  }
}

// The extend form must be slice-composable: computing in pieces equals one
// shot over the concatenation.
TEST(Crc32CTest, Crc32CExtend_ComposesAcrossSlices) {
  // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp)
  std::mt19937_64 rng(777);  // deterministic vectors
  std::vector<uint8_t> buf(1000);
  for (auto& byte : buf) {
    byte = static_cast<uint8_t>(rng());
  }
  constexpr std::array<size_t, 10> cuts = {0,   1,   7,   63,  255,
                                           256, 257, 511, 999, 1000};
  for (const size_t cut : cuts) {
    const uint32_t one_shot = detail::Crc32CRawHardware(0xffffffffU, buf);
    uint32_t piecewise = 0xffffffffU;
    if (cut > 0) {
      piecewise =
          detail::Crc32CRawHardware(piecewise, std::span(buf).first(cut));
    }
    piecewise =
        detail::Crc32CRawHardware(piecewise, std::span(buf).subspan(cut));
    EXPECT_EQ(piecewise, one_shot) << "cut=" << cut;
  }
}

}  // namespace tinylamb
