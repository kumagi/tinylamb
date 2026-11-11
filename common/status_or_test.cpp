/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/status_or.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>

namespace tinylamb {
TEST(StatusOrTest, NullOptionalIsAValueRatherThanMissingStorage) {
  const StatusOr<std::optional<int>> empty = std::nullopt;
  ASSERT_TRUE(empty.HasValue());
  EXPECT_FALSE(empty.Value().has_value());
  const StatusOr<std::optional<int>> explicit_empty = std::optional<int>{};
  ASSERT_TRUE(explicit_empty.HasValue());
  EXPECT_FALSE(explicit_empty.Value().has_value());
  const StatusOr<std::optional<int>> present = 42;
  ASSERT_TRUE(present.HasValue());
  EXPECT_EQ(present.Value(), 42);
}
TEST(StatusOrTest, ConvertingValuesAndErrorsRemainDistinct) {
  const StatusOr<std::string> text = "value";
  ASSERT_TRUE(text.HasValue());
  EXPECT_EQ(text.Value(), "value");
  StatusOr<std::unique_ptr<int>> owned = std::make_unique<int>(7);
  ASSERT_TRUE(owned.HasValue());
  EXPECT_EQ(*owned.MoveValue(), 7);
  EXPECT_FALSE(owned.HasValue());
  const StatusOr<std::optional<int>> failure = Status::kCorrupt;
  EXPECT_FALSE(failure.HasValue());
  EXPECT_EQ(failure.GetStatus(), Status::kCorrupt);
}
}  // namespace tinylamb
