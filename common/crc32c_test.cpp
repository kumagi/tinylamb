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

#include "gtest/gtest.h"

namespace tinylamb {

TEST(Crc32CTest, EmptyIsZero) {
  EXPECT_EQ(Crc32C(nullptr, 0), 0U);
}

TEST(Crc32CTest, KnownVector123456789) {
  const char* data = "123456789";
  EXPECT_EQ(Crc32C(data, 9), 0xe3069283U);
}

}  // namespace tinylamb
