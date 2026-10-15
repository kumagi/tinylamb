/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// FlatIntKeyMap (executor/detail/flat_int_key_map.hpp): the open-addressing
// int64 -> T table under the grouped-aggregate fast paths.  The contract under
// test: TryEmplace inserts once and finds forever after, Grow preserves every
// group (and invalidates outstanding value pointers, which callers must treat
// as per-call), and ForEach sees exactly the inserted set.
#include "executor/detail/flat_int_key_map.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <set>
#include <vector>

namespace tinylamb {

TEST(FlatIntKeyMapTest, EmplaceInsertsOnceAndFindsAfterwards) {
  relational_detail::FlatIntKeyMap<int64_t> map;
  bool inserted = false;
  int64_t* first = map.TryEmplace(42, &inserted);
  ASSERT_NE(first, nullptr);
  EXPECT_TRUE(inserted);
  *first = 7;
  bool reinserted = true;
  int64_t* again = map.TryEmplace(42, &reinserted);
  EXPECT_FALSE(reinserted);
  EXPECT_EQ(*again, 7);
}

TEST(FlatIntKeyMapTest, GrowPreservesEveryGroup) {
  relational_detail::FlatIntKeyMap<size_t> map;
  const int kKeys = 5000;  // forces several x4 growths from the 16-slot start
  std::set<int64_t> keys;
  for (int i = 0; i < kKeys; ++i) {
    const int64_t key = (static_cast<int64_t>(i) * 7919) - 1000000;
    keys.insert(key);
    bool inserted = false;
    *map.TryEmplace(key, &inserted) = static_cast<size_t>(i);
    EXPECT_TRUE(inserted) << "key " << key << " reported already present";
  }
  size_t visited = 0;
  std::set<int64_t> seen;
  map.ForEach([&](int64_t key, size_t) {
    ++visited;
    seen.insert(key);
  });
  EXPECT_EQ(visited, keys.size());
  EXPECT_EQ(seen, keys);
  // After growth every lookup still resolves to the stored value.
  for (int i = 0; i < kKeys; ++i) {
    const int64_t key = (static_cast<int64_t>(i) * 7919) - 1000000;
    bool inserted = true;
    const size_t* found = map.TryEmplace(key, &inserted);
    ASSERT_FALSE(inserted);
    EXPECT_EQ(*found, static_cast<size_t>(i));
  }
}

TEST(FlatIntKeyMapTest, ExtremeKeysAndCollisionsStayDistinct) {
  relational_detail::FlatIntKeyMap<int64_t> map;
  const std::vector<int64_t> extremes = {
      std::numeric_limits<int64_t>::min(), -1, 0, 1,
      std::numeric_limits<int64_t>::max(),
  };
  for (const int64_t key : extremes) {
    bool inserted = false;
    *map.TryEmplace(key, &inserted) = key;
    EXPECT_TRUE(inserted);
  }
  // Sequential keys from one field share high hash bits; the splitmix final
  // must still separate them.
  for (int64_t key = 1000; key < 1100; ++key) {
    bool inserted = false;
    *map.TryEmplace(key, &inserted) = key * 3;
    EXPECT_TRUE(inserted);
  }
  size_t total = 0;
  map.ForEach([&](int64_t key, int64_t value) {
    const bool extreme = key == std::numeric_limits<int64_t>::min() ||
                         key == std::numeric_limits<int64_t>::max() ||
                         key == -1 || key == 0 || key == 1;
    EXPECT_EQ(value, extreme ? key : key * 3);
    ++total;
  });
  EXPECT_EQ(total, extremes.size() + 100U);
}

TEST(FlatIntKeyMapTest, PrefetchOnEmptyAndFreshMapsIsSafe) {
  // Prefetch forms an address inside the slot vector; on a never-grown map
  // that vector is still empty, and the prefetch must degrade to a no-op
  // instead of forming an out-of-range element.
  relational_detail::FlatIntKeyMap<uint64_t> fresh;
  fresh.Prefetch(42);
  bool inserted = false;
  *fresh.TryEmplace(42, &inserted) = 1;
  EXPECT_TRUE(inserted);
  // Prefetching after growth exercises the populated path too.
  fresh.Prefetch(43);
  *fresh.TryEmplace(43, &inserted) = 2;
  EXPECT_TRUE(inserted);
  EXPECT_EQ(*fresh.TryEmplace(42, &inserted), 1U);
  EXPECT_FALSE(inserted);
}

TEST(FlatIntKeyMapTest, ForEachOnEmptyMapVisitsNothing) {
  relational_detail::FlatIntKeyMap<int64_t> map;
  size_t visited = 0;
  map.ForEach([&](int64_t, int64_t) { ++visited; });
  EXPECT_EQ(visited, 0U);
}

}  // namespace tinylamb
