//
// Created by kumagi on 2022/02/02.
//

#ifndef TINYLAMB_TEST_UTIL_HPP
#define TINYLAMB_TEST_UTIL_HPP

#include "gtest/gtest.h"

#define ASSERT_SUCCESS(x) ASSERT_EQ(Status::kSuccess, x)
#define EXPECT_SUCCESS(x) EXPECT_EQ(Status::kSuccess, x)
#define ASSERT_FAIL(x) ASSERT_NE(Status::kSuccess, x)
#define EXPECT_FAIL(x) EXPECT_NE(Status::kSuccess, x)

template <typename T>
void SerializeDeserializeTest(const T& c) {
  std::string buff;
  buff.resize(c.Size());
  c.Serialize(buff.data());
  T another;
  another.Deserialize(buff.data());
  ASSERT_EQ(c, another);
}

#endif  // TINYLAMB_TEST_UTIL_HPP
