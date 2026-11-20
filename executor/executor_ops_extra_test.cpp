/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <gtest/gtest.h>

#include <cstddef>
#include <string>

#include "executor/detail/explain_format.hpp"

namespace tinylamb {
namespace {

// T996: this binary was a CoverageScaffold/EXPECT_EQ(1,1) placeholder.  It
// now pins the EXPLAIN byte formatter, whose unit cutoffs silently shift
// EXPLAIN ANALYZE / spill diagnostics text when touched.
TEST(ExecutorOpsExtraTest, FormatBytesUsesBinaryUnitsAtExactCutoffs) {
  EXPECT_EQ(relational_detail::FormatBytes(0), "0B");
  EXPECT_EQ(relational_detail::FormatBytes(1), "1B");
  EXPECT_EQ(relational_detail::FormatBytes(1023), "1023B");
  EXPECT_EQ(relational_detail::FormatBytes(1024), "1.0KiB");
  EXPECT_EQ(relational_detail::FormatBytes((1U << 20) - 1), "1024.0KiB");
  EXPECT_EQ(relational_detail::FormatBytes(1U << 20), "1.0MiB");
  EXPECT_EQ(relational_detail::FormatBytes((1U << 30) - 1), "1024.0MiB");
  EXPECT_EQ(relational_detail::FormatBytes(1U << 30), "1.0GiB");
  EXPECT_EQ(relational_detail::FormatBytes(size_t{3} * (size_t{1} << 30) / 2), "1.5GiB");
}

TEST(ExecutorOpsExtraTest, FormatBytesKeepsLargestUnitPastGibibyteScale) {
  const size_t four_gib = size_t{4} * (size_t{1} << 30);
  EXPECT_EQ(relational_detail::FormatBytes(four_gib), "4.0GiB");
  EXPECT_EQ(relational_detail::FormatBytes(four_gib * 1024), "4096.0GiB");
}

}  // namespace
}  // namespace tinylamb
