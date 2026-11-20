/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "page/page_lsm_store.hpp"

#include <gtest/gtest.h>

#include <mutex>

namespace tinylamb {
namespace {

// T4094: Current() must not dereference a reset block_ (or index past the
// last decoded entry) when a consumer forgets to check Valid().
TEST(LsmRunCursorTest, CurrentOnInvalidCursorYieldsEmptyEntry) {
  std::mutex file_mu;
  LsmBlockCache cache;
  const int fd = -1;
  LsmRunCursor cursor(fd, file_mu, cache, /*run_id=*/1, /*blocks=*/{},
                      /*cache_limit_bytes=*/64 << 10,
                      /*populate_cache=*/false);
  ASSERT_FALSE(cursor.Valid());
  const LsmRunEntry& entry = cursor.Current();
  EXPECT_TRUE(entry.key.empty());
  EXPECT_TRUE(entry.value.empty());
  EXPECT_FALSE(entry.deleted);
}

}  // namespace
}  // namespace tinylamb
