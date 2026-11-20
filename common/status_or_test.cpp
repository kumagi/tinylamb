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

  EXPECT_EQ(ToString(StatusCode::kMemoryLimitExceeded), "MemoryLimitExceeded");
  EXPECT_EQ(SqlStateForStatus(Status::kMemoryLimitExceeded), "53200");
  EXPECT_EQ(SqlStateForStatus(Status::kNoSpace), "53100");
  EXPECT_EQ(SqlStateForStatus(Status::kConflicts), "40001");
  EXPECT_EQ(SqlStateForStatus(Status::kDuplicates), "23505");
  EXPECT_EQ(ToString(StatusCode::kQueryCanceled), "QueryCanceled");
  EXPECT_EQ(SqlStateForStatus(Status::kQueryCanceled), "57014");
  EXPECT_EQ(SqlStateForStatus(Status::kQueryCanceled, "XX000"), "57014");
  // P2839: kNotImplemented is feature_not_supported, never a syntax error.
  EXPECT_EQ(SqlStateForStatus(Status::kNotImplemented, "42601"), "0A000");
  // P2838: statuses with definite meaning map identically no matter which
  // call-site fallback they arrive with.
  EXPECT_EQ(SqlStateForStatus(Status::kInvalidArgument, "42601"), "42601");
  EXPECT_EQ(SqlStateForStatus(Status::kInvalidArgument, "XX000"), "42601");
  EXPECT_EQ(SqlStateForStatus(Status::kNotExists, "42601"), "42P01");
  EXPECT_EQ(SqlStateForStatus(Status::kNotExists, "XX000"), "42P01");
  EXPECT_EQ(SqlStateForStatus(Status::kAmbiguousQuery, "XX000"), "42702");
  EXPECT_EQ(SqlStateForStatus(Status::kUnknownType, "42601"), "42704");
  // P2842: operator-budget overflow is program_limit_exceeded, not the
  // truncation code the old fallback chain produced.
  EXPECT_EQ(SqlStateForStatus(Status::kTooBigData, "XX000"), "54000");
  // P2843: the whole semantic table is pinned with call-site fallbacks that
  // a mapping regression would have to collide with -- none of these lines
  // is a fallback self-test any more.
  EXPECT_EQ(SqlStateForStatus(Status::kTooBigData, "42601"), "54000");
  EXPECT_EQ(SqlStateForStatus(Status::kIsInfinity, "42601"), "22003");
  EXPECT_EQ(SqlStateForStatus(Status::kRuntimeError, "42601"), "XX000");
  EXPECT_EQ(SqlStateForStatus(Status::kIOError, "42601"), "XX000");
  EXPECT_EQ(SqlStateForStatus(Status::kCorrupt, "42601"), "XX000");
  // Unmapped statuses still honor the site-chosen fallback.
  EXPECT_EQ(SqlStateForStatus(Status::kUnknown, "42601"), "42601");
}
}  // namespace tinylamb
