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

#include <string>

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

}  // namespace tinylamb
