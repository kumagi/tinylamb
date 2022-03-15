//
// Created by kumagi on 2022/02/02.
//

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
