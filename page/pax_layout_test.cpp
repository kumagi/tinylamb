/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "page/pax_layout.hpp"

#include <type_traits>

#include "common/constants.hpp"
#include "gtest/gtest.h"

namespace tinylamb {

TEST(PaxLayoutTest, UsesStableFixedWidthDiskStructures) {
  EXPECT_TRUE(std::is_standard_layout_v<PaxPageHeader>);
  EXPECT_TRUE(std::is_standard_layout_v<PaxColumnDirectory>);
  EXPECT_EQ(sizeof(PaxPageHeader), 32U);
  EXPECT_EQ(sizeof(PaxColumnDirectory), 28U);
  EXPECT_LT(sizeof(PaxPageHeader) + PaxDirectoryBytes(64), kPageBodySize);
}

TEST(PaxLayoutTest, BitmapRoundsCapacityToWholeBytes) {
  EXPECT_EQ(PaxBitmapBytes(0), 0U);
  EXPECT_EQ(PaxBitmapBytes(1), 1U);
  EXPECT_EQ(PaxBitmapBytes(8), 1U);
  EXPECT_EQ(PaxBitmapBytes(9), 2U);
  EXPECT_EQ(PaxBitmapBytes(1024), 128U);
}

}  // namespace tinylamb
