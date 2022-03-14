//
// Created by kumagi on 2022/02/02.
//

#ifndef TINYLAMB_TEST_UTIL_HPP
#define TINYLAMB_TEST_UTIL_HPP

#include <random>

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

thread_local std::random_device seed_gen;
thread_local std::mt19937 engine(seed_gen());

std::string RandomString() {
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  const static size_t len = 16;
  std::string ret;
  ret.reserve(len);
  for (int i = 0; i < 12; ++i) {
    ret.push_back(alphanum[engine() % (sizeof(alphanum) - 1)]);
  }
  return ret;
}

#endif  // TINYLAMB_TEST_UTIL_HPP
