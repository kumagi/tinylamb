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


#ifndef TINYLAMB_TEST_UTIL_HPP
#define TINYLAMB_TEST_UTIL_HPP

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "gtest/gtest.h"
#include "serdes.hpp"

#define ASSERT_SUCCESS(x) ASSERT_EQ(Status::kSuccess, x)
#define EXPECT_SUCCESS(x) EXPECT_EQ(Status::kSuccess, x)
#define ASSERT_FAIL(x) ASSERT_NE(Status::kSuccess, x)
#define EXPECT_FAIL(x) EXPECT_NE(Status::kSuccess, x)

template <typename T>
void SerializeDeserializeTest(const T& c) {
  std::stringstream ss;
  tinylamb::Encoder arc(ss);
  arc << c;

  tinylamb::Decoder ext(ss);
  T another;
  ext >> another;
  ASSERT_EQ(c, another);
}

#endif  // TINYLAMB_TEST_UTIL_HPP
