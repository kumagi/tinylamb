//
// Created by kumagi on 2022/02/02.
//

#ifndef TINYLAMB_TEST_UTIL_HPP
#define TINYLAMB_TEST_UTIL_HPP

#define ASSERT_SUCCESS(x) ASSERT_EQ(Status::kSuccess, x)
#define EXPECT_SUCCESS(x) EXPECT_EQ(Status::kSuccess, x)
#define ASSERT_FAIL(x) ASSERT_NE(Status::kSuccess, x)
#define EXPECT_FAIL(x) EXPECT_NE(Status::kSuccess, x)

#endif  // TINYLAMB_TEST_UTIL_HPP
