/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// Extra Page-header coverage beyond the per-page-shape suites: the LSN
// fields' ratchet contracts (page_lsn stores verbatim, recovery_lsn is a
// monotone floor) and PageInit re-initialization.
#include <gtest/gtest.h>

#include <limits>

#include "common/constants.hpp"
#include "page/page.hpp"
#include "page/page_type.hpp"

namespace tinylamb {

TEST(PageExtraTest, ConstructorSetsIdentityAndType) {
  const Page page(1234, PageType::kRowPage);
  EXPECT_EQ(page.PageID(), 1234U);
  EXPECT_EQ(page.Type(), PageType::kRowPage);
}

TEST(PageExtraTest, PageLSNStoresVerbatim) {
  Page page(1, PageType::kRowPage);
  EXPECT_EQ(page.PageLSN(), 0U);
  page.SetPageLSN(0xdeadbeefcafef00dULL);
  EXPECT_EQ(page.PageLSN(), 0xdeadbeefcafef00dULL);
  // Unlike recLSN, the page LSN is a plain assignment: it may move backwards
  // when recovery restores an older image.
  page.SetPageLSN(7);
  EXPECT_EQ(page.PageLSN(), 7U);
}

TEST(PageExtraTest, RecLSNIsAMonotoneFloor) {
  Page page(1, PageType::kRowPage);
  EXPECT_EQ(page.RecoveryLSN(), std::numeric_limits<lsn_t>::max());
  page.SetRecLSN(500);
  EXPECT_EQ(page.RecoveryLSN(), 500U);
  // A higher hint must not raise the floor: recLSN only ever moves down so
  // it keeps covering the oldest redo a page may still need.
  page.SetRecLSN(900);
  EXPECT_EQ(page.RecoveryLSN(), 500U);
  page.SetRecLSN(42);
  EXPECT_EQ(page.RecoveryLSN(), 42U);
}

TEST(PageExtraTest, ResetRecLSNReleasesTheFloor) {
  Page page(1, PageType::kRowPage);
  page.SetRecLSN(77);
  ASSERT_EQ(page.RecoveryLSN(), 77U);
  page.ResetRecLSN();
  EXPECT_EQ(page.RecoveryLSN(), std::numeric_limits<lsn_t>::max());
  // After the reset the next hint sticks again.
  page.SetRecLSN(99);
  EXPECT_EQ(page.RecoveryLSN(), 99U);
}

TEST(PageExtraTest, PageInitReinitializesIdentity) {
  Page page(7, PageType::kLeafPage);
  page.SetPageLSN(1234);
  page.SetRecLSN(1000);
  page.PageInit(8, PageType::kBranchPage);
  EXPECT_EQ(page.PageID(), 8U);
  EXPECT_EQ(page.Type(), PageType::kBranchPage);
}

}  // namespace tinylamb
