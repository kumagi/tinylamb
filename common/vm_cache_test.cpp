
/**
 * Copyright 2024 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "common/vm_cache.hpp"

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

#include "common/log_message.hpp"
#include "gtest/gtest.h"
#include "random_string.hpp"

namespace tinylamb {
namespace {

class VMCacheTest : public ::testing::Test {
 protected:
  template <typename T>
  std::unique_ptr<VMCache<T>> MakeCache(size_t data_size, size_t offset = 0) {
    path_ = "vm_cache_test-" + RandomString();
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
    std::string random = RandomString(offset);
    ssize_t padding_wrote = ::write(fd_, random.data(), random.length());
    EXPECT_EQ(padding_wrote, offset);
    std::vector<T> value;
    value.resize(data_size);
    for (size_t i = 0; i < data_size; ++i) {
      value[i] = Expected<T>(i);
    }
    EXPECT_EQ(value.size(), data_size);
    size_t remaining = value.size() * sizeof(T);
    size_t written = 0;
    while (0 < remaining) {
      ssize_t wrote = ::write(
          fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
      EXPECT_LT(0, wrote);
      written += wrote;
      remaining -= wrote;
    }
    EXPECT_EQ(written, value.size() * sizeof(T));
    ::fsync(fd_);
    EXPECT_EQ(std::filesystem::file_size(path_),
              offset + data_size * sizeof(T));
    return std::make_unique<VMCache<T>>(fd_, data_size * 1024, offset);
  }

  void TearDown() override {
    ::close(fd_);
    std::ignore = std::remove(path_.c_str());
  }

  template <typename T>
  static T Expected(size_t key) {
    return T(kSeed + std::hash<size_t>()(key));
  }

  constexpr static int kSeed = 0;
  // 0xdeadbeef;
  int fd_;
  std::filesystem::path path_;
};

TEST_F(VMCacheTest, one_page) {
  // Arrange -- create a VMCache of 1024 int32_t values
  constexpr size_t kCount = 1024;
  auto cache = MakeCache<int32_t>(kCount);

  // Act -- read each element back via cache->Read
  for (int i = 0; i < 1024; ++i) {
    int32_t data;
    cache->Read(&data, i, 1);

    // Assert -- each read yields the expected value written by MakeCache
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, offset) {
  // Arrange -- for each offset 1..4095 (step 127), create a VMCache of 1024 int32_t values at that offset
  constexpr size_t kCount = 1024;
  for (int i = 1; i < 4096; i += 127) {
    auto cache = MakeCache<int32_t>(kCount, i);

    // Act -- read each element back via cache->Read
    for (int j = 0; j < 1024; ++j) {
      int32_t data;
      cache->Read(&data, j, 1);
      if (data != Expected<int32_t>(j)) {
        exit(1);
      }
      // Assert -- each read yields the expected value, or exit(1) on mismatch
      ASSERT_EQ(data, Expected<int32_t>(j));
    }
  }
}

struct Data {
  int a;
  int b;
  char c;
  Data() = default;
  Data(size_t from)
      : a(from & 0xffffffff), b(from & 0x0000ffff), c(from & 0xff) {}
  bool operator==(const Data& rhs) const {
    return a == rhs.a && b == rhs.b && c == rhs.c;
  }
  friend std::ostream& operator<<(std::ostream& o, const Data& d) {
    o << "{ a: " << d.a << " b: " << d.b << " c: " << (int)d.c << " }\n";
    return o;
  }
};

TEST_F(VMCacheTest, offset_struct) {
  // Arrange -- for each offset 1..4095 (step 127), create a VMCache of 1024 Data structs at that offset
  constexpr size_t kCount = 1024;
  for (int i = 1; i < 4096; i += 127) {
    auto cache = MakeCache<Data>(kCount, i);

    // Act -- read each struct back via cache->Read
    for (int j = 0; j < 1024; ++j) {
      Data data;
      cache->Read(&data, j, 1);

      // Assert -- each read yields the expected struct
      ASSERT_EQ(data, Expected<Data>(j));
    }
  }
}

TEST_F(VMCacheTest, Invalidate) {
  // Arrange -- a single-page cache of int32 values
  constexpr size_t kCount = 1024;
  auto cache = MakeCache<int32_t>(kCount);

  // Act -- read element 0, invalidate it, then read it again
  int32_t data;
  cache->Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));
  cache->Invalidate(0, 1);
  cache->Read(&data, 0, 1);

  // Assert -- the value survives invalidation (re-loaded from the file)
  ASSERT_EQ(data, Expected<int32_t>(0));
}

TEST_F(VMCacheTest, InvalidateRangeAcrossPages) {
  // Arrange -- a two-page cache; invalidate a range straddling the boundary
  constexpr size_t kCount = 2048;
  auto cache = MakeCache<int32_t>(kCount);

  // Act -- invalidate elements 1023..1024 (crosses the 4096-byte page border)
  cache->Invalidate(1023, 2);

  // Assert -- the straddled range can still be read back correctly
  for (int i = 1020; i < 1028; ++i) {
    int32_t data;
    cache->Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, CrossPageRead) {
  // Arrange -- a two-page cache
  constexpr size_t kCount = 2048;
  auto cache = MakeCache<int32_t>(kCount);

  // Act -- read a two-element range that spans the page boundary
  int32_t buf[2];
  cache->Read(buf, 1023, 2);

  // Assert -- both halves of the read land on the correct values
  EXPECT_EQ(buf[0], Expected<int32_t>(1023));
  EXPECT_EQ(buf[1], Expected<int32_t>(1024));
}

TEST_F(VMCacheTest, Dump) {
  // Arrange -- a cache with a few resident pages
  constexpr size_t kCount = 1024;
  auto cache = MakeCache<int32_t>(kCount);

  // Act -- read all elements so pages are resident, then Dump via both APIs
  for (int i = 0; i < 1024; ++i) {
    int32_t data;
    cache->Read(&data, i, 1);
  }
  const std::string dump = cache->Dump();

  // Assert -- Dump produces the "[...] {...} [...]" queue layout
  EXPECT_EQ(dump.front(), '[');
  EXPECT_NE(dump.find("}"), std::string::npos);
  std::ostringstream oss;
  oss << *cache;
  EXPECT_FALSE(oss.str().empty());
}

TEST_F(VMCacheTest, FindNearestSize) {
  // Arrange -- any cache instance exposing FindNearestSize
  auto cache = MakeCache<int32_t>(16);

  // Act + Assert -- default block rounding and explicit target/around pairs
  EXPECT_EQ(cache->FindNearestSize(), 4096);
  EXPECT_EQ(cache->FindNearestSize(4, 4096), 4096);
  EXPECT_EQ(cache->FindNearestSize(100, 4096), 4100);
  EXPECT_EQ(cache->FindNearestSize(3000, 4096), 6000);
  EXPECT_EQ(cache->FindNearestSize(5000, 4096), 5000);
}

TEST_F(VMCacheTest, FileSizeParameter) {
  // Arrange -- write a file and build a VMCache with an explicit file_size
  constexpr size_t kCount = 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  const size_t file_size = std::filesystem::file_size(path_);
  VMCache<int32_t> cache(fd_, kCount * 1024, 0, file_size);

  // Act -- read one element through the file_size-aware cache
  int32_t data;
  cache.Read(&data, 42, 1);

  // Assert -- the value matches what was written
  ASSERT_EQ(data, Expected<int32_t>(42));
}

TEST_F(VMCacheTest, SmallCacheEviction) {
  // Arrange -- 64 pages of data but only a 2-page memory budget; each page is
  // touched exactly once so pages stay kUnlocked and evict through the
  // small/ghost FIFO queues.
  constexpr size_t kPages = 64;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 2 * 4096);

  // Act -- read element 0 of every page, forcing 62 evictions/reloads
  for (size_t page = 0; page < kPages; ++page) {
    int32_t data;
    cache.Read(&data, page * 1024, 1);

    // Assert -- evicted pages are transparently reloaded with correct data
    ASSERT_EQ(data, Expected<int32_t>(page * 1024));
  }
}

TEST_F(VMCacheTest, PromoteAccessedPagesToMainQueue) {
  // Arrange -- a 2-page budget: small/main/ghost queues hold exactly 1 page.
  constexpr size_t kPages = 3;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 2 * 4096);

  // Act -- read page 0 twice so it is re-fixed while resident, then read page 1
  // (forces page 0 through the promotion path) and page 2 (forces another
  // eviction), keeping the tiny budget under pressure.
  int32_t data;
  cache.Read(&data, 0, 1);
  cache.Read(&data, 0, 1);
  cache.Read(&data, 1024, 1);
  cache.Read(&data, 2048, 1);

  // Assert -- the Dump stays well-formed and every page is still readable.
  const std::string dump = cache.Dump();
  EXPECT_EQ(dump.front(), '[');
  EXPECT_NE(dump.find('}'), std::string::npos);
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, ReloadsMarkedGhostPageIntoMainQueue) {
  // Arrange -- a 2-page budget where a page evicted into the ghost queue is
  // re-read (state kMarked -> kUnlocked) before the ghost FIFO overflows.
  constexpr size_t kPages = 3;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 2 * 4096);

  // Act -- page 0 -> ghost (marked); re-read page 0 so it revives inside the
  // ghost queue; then read page 2 to overflow the ghost FIFO.
  int32_t data;
  cache.Read(&data, 0, 1);
  cache.Read(&data, 1024, 1);
  cache.Read(&data, 0, 1);
  cache.Read(&data, 2048, 1);

  // Assert -- the revived page is served correctly and the queues stay sane.
  EXPECT_EQ(cache.Dump().front(), '[');
  cache.Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, EvictsAccessedPagesThroughFullMainQueue) {
  // Arrange -- a 5-page budget (small=1, main=4). Re-reading pages while they
  // sit in the main queue leaves them accessed, so the overflow path that
  // re-enqueues accessed pages must be exercised.
  constexpr size_t kPages = 6;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 5 * 4096);

  // Act -- fill the main queue with accessed pages, re-read page 0, then
  // overflow both the small and main queues with a fresh page.
  int32_t data;
  for (size_t page = 0; page < 5; ++page) {
    cache.Read(&data, page * 1024, 1);
    cache.Read(&data, page * 1024, 1);
  }
  cache.Read(&data, 0, 1);
  cache.Read(&data, 5 * 1024, 1);

  // Assert -- the Dump stays well-formed and all data remains intact.
  EXPECT_EQ(cache.Dump().front(), '[');
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, DumpWithMultiEntrySmallAndMainQueues) {
  // Arrange -- a 20-page budget gives small/main queues with capacity 2 so
  // each can hold several entries and the Dump comma separators are emitted.
  constexpr size_t kPages = 6;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 20 * 4096);

  // Act -- fill the small queue with two pages.
  int32_t data;
  cache.Read(&data, 0, 1);
  cache.Read(&data, 1024, 1);
  EXPECT_EQ(cache.Dump(), "[0, 1] {} []");

  // Re-read both pages, then push pages 2..5 so the accessed pages promote to
  // the main queue (four entries) while two pages stay in the small queue.
  cache.Read(&data, 0, 1);
  cache.Read(&data, 1024, 1);
  for (size_t page = 2; page <= 5; ++page) {
    cache.Read(&data, page * 1024, 1);
    cache.Read(&data, page * 1024, 1);
  }
  EXPECT_EQ(cache.Dump(), "[4, 5] {0, 1, 2, 3} []");
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, GhostQueueAccumulatesMultipleEntries) {
  // Arrange -- a 20-page budget (ghost capacity 2). Pages touched only once
  // stay kUnlocked, so evictions flow through the ghost FIFO.
  constexpr size_t kPages = 5;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 20 * 4096);

  // Act -- read four pages once each so evicted pages churn through the
  // ghost/small queues while two pages stay in the small queue.
  int32_t data;
  for (size_t page = 0; page < 5; ++page) {
    cache.Read(&data, page * 1024, 1);
  }

  // Assert -- the Dump stays well-formed after the queue churn.
  EXPECT_EQ(cache.Dump(), "[3, 4] {0, 1, 2} []");

  // Assert -- every element is still readable after all the evictions.
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, ReadLargeSpanningBuffer) {
  // Arrange -- a 6-page cache; a single Read may span several page boundaries.
  constexpr size_t kCount = 6 * 1024;
  auto cache = MakeCache<int32_t>(kCount);

  // Act -- read 3000 elements starting at element 1000 (crosses pages 1..4).
  std::vector<int32_t> buffer(3000);
  cache->Read(buffer.data(), 1000, 3000);

  // Assert -- every element in the span is correct.
  for (size_t i = 0; i < 3000; ++i) {
    ASSERT_EQ(buffer[i], Expected<int32_t>(1000 + i));
  }

  // Act -- read the very last element of the file.
  int32_t tail;
  cache->Read(&tail, kCount - 1, 1);
  ASSERT_EQ(tail, Expected<int32_t>(kCount - 1));
}

TEST_F(VMCacheTest, InvalidateBeyondFileEndIsClamped) {
  // Arrange -- a single-page cache; meta_ has exactly two entries (0 and 1).
  constexpr size_t kCount = 1024;
  auto cache = MakeCache<int32_t>(kCount);

  // Act -- warm page 0, then invalidate elements 1020..1100 (bytes 4080..4400)
  // so the loop visits page 0 (resident) and page 1 (past end-of-file but
  // still a valid meta_ entry).
  int32_t data;
  cache->Read(&data, 0, 1);
  cache->Invalidate(1020, 80);

  // Assert -- the reloaded page still serves the expected values.
  for (int i = 1000; i < 1020; ++i) {
    cache->Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }

  // Act -- a gigantic range whose first page index is already past max_size_;
  // the `target < max_size_` guard stops the loop without touching meta_.
  cache->Invalidate(1024LL * 1024 * 1024, 1);

  // Assert -- the cache is still fully functional afterwards.
  for (int i = 0; i < 64; ++i) {
    cache->Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

// Invalidate marked (ghost) and unlocked (small) pages, then reload them.
// Invalidate must drop the page from every FIFO so a later FixPage enqueue
// cannot trip SanityCheck's duplicate detection.
TEST_F(VMCacheTest, InvalidateMarkedAndUnlockedPagesThenReload) {
  // Arrange -- a 20-page budget (small queue capacity 2, ghost capacity 18).
  constexpr size_t kPages = 4;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 20 * 4096);

  // Act -- read pages 0..2 once each (offset by one element so the page is
  // touched exactly once and stays kUnlocked): page 0 is evicted into the ghost
  // queue (kMarked), pages 1 and 2 stay in the small queue.
  int32_t data;
  for (size_t page = 0; page < 3; ++page) {
    cache.Read(&data, page * 1024 + 1, 1);
    ASSERT_EQ(data, Expected<int32_t>(page * 1024 + 1));
  }
  EXPECT_EQ(cache.Dump(), "[1, 2] {} [0]");

  // Act -- invalidate the kMarked ghost page 0, then reload it (promotes page 1
  // to the ghost queue), then invalidate and reload the kUnlocked page 0 again.
  cache.Invalidate(0, 1);
  cache.Read(&data, 1, 1);
  ASSERT_EQ(data, Expected<int32_t>(1));
  cache.Invalidate(0, 1);
  cache.Read(&data, 1, 1);
  ASSERT_EQ(data, Expected<int32_t>(1));

  // Assert -- ghost hit on page 1 revives into the main queue; page 0 stays in
  // small with page 2 (small capacity is 2). Data must still round-trip.
  cache.Read(&data, 1025, 1);
  ASSERT_EQ(data, Expected<int32_t>(1025));
  EXPECT_EQ(cache.Dump(), "[2, 0] {1} []");
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, GhostFifoOverflowEvictsMarkedEntry) {
  // Arrange -- a 3-page budget (small=1, main=2, ghost=2).
  constexpr size_t kPages = 4;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 3 * 4096);

  // Act -- read pages 0..3 once each (non-aligned so pages stay kUnlocked); the
  // ghost queue overflows when page 2 is evicted, dropping the kMarked page 0
  // back to kEvicted.
  int32_t data;
  for (size_t page = 0; page < 4; ++page) {
    cache.Read(&data, page * 1024 + 1, 1);
    ASSERT_EQ(data, Expected<int32_t>(page * 1024 + 1));
  }

  // Assert -- the ghost FIFO kept [1, 2] and page 0 was fully evicted.
  EXPECT_EQ(cache.Dump(), "[3] {} [1, 2]");

  // Assert -- the evicted page 0 transparently reloads from the file.
  cache.Read(&data, 1, 1);
  ASSERT_EQ(data, Expected<int32_t>(1));
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, SmallQueueEvictsSingleTouchedPageToGhost) {
  // Arrange -- a 2-page budget (small=1, main=1, ghost=1).
  constexpr size_t kPages = 3;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 2 * 4096);

  // Act -- pages touched exactly once (offset by one element so the page stays
  // kUnlocked) churn through the small -> ghost FIFO chain as new pages arrive.
  int32_t data;
  cache.Read(&data, 1, 1);
  cache.Read(&data, 1025, 1);

  // Assert -- page 0 was demoted to the ghost queue, page 1 is resident.
  EXPECT_EQ(cache.Dump(), "[1] {} [0]");

  cache.Read(&data, 2 * 1024 + 1, 1);
  EXPECT_EQ(cache.Dump(), "[2] {} [1]");

  // Assert -- every element is still readable after the FIFO churn.
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, CrossPageStructReadWithTwelveByteElements) {
  // Arrange -- a 5000-element Data file; block size rounds to 4092 bytes
  // (341 structs per page), exercising the non-4096 boundary arithmetic.
  constexpr size_t kCount = 5000;
  auto cache = MakeCache<Data>(kCount);

  // Act -- read 2000 structs starting at element 2500 across several pages.
  std::vector<Data> buffer(2000);
  cache->Read(buffer.data(), 2500, 2000);

  // Assert -- every struct in the span is correct.
  for (size_t i = 0; i < 2000; ++i) {
    ASSERT_EQ(buffer[i], Expected<Data>(2500 + i));
  }

  // Act -- read the last element of the file.
  Data tail;
  cache->Read(&tail, kCount - 1, 1);
  ASSERT_EQ(tail, Expected<Data>(kCount - 1));

  // Act -- invalidate a two-element range straddling the 4092-byte boundary.
  cache->Invalidate(340, 2);
  Data first;
  Data second;
  cache->Read(&first, 340, 1);
  cache->Read(&second, 341, 1);
  ASSERT_EQ(first, Expected<Data>(340));
  ASSERT_EQ(second, Expected<Data>(341));
}


TEST_F(VMCacheTest, ConstructorWithExplicitFileSizeAndOffset) {
  // Arrange -- a file with 100 padding bytes followed by 1024 int32 values.
  constexpr size_t kCount = 1024;
  constexpr size_t kOffset = 100;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::string random = RandomString(kOffset);
  ssize_t padding_wrote = ::write(fd_, random.data(), random.length());
  ASSERT_EQ(padding_wrote, static_cast<ssize_t>(kOffset));
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  const size_t file_size = std::filesystem::file_size(path_);

  // Act -- construct with both an explicit offset and an explicit file size.
  VMCache<int32_t> cache(fd_, 1024 * 1024, kOffset, file_size);

  // Assert -- all elements are readable through the offset-adjusted cache.
  for (size_t i = 0; i < kCount; ++i) {
    int32_t data;
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, MultiEntryDumpAcrossAllThreeQueues) {
  // Arrange -- a 20-page budget (small=2, main=18, ghost=18). Non-aligned
  // single-touch reads keep pages kUnlocked so they evict into the ghost
  // queue, while re-read pages turn kUnlockedAccessed and get promoted to the
  // main queue when the small FIFO overflows.
  constexpr size_t kPages = 12;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 20 * 4096);

  // Act -- grow the main queue with re-read (accessed) pages while single-touch
  // pages churn through the small -> ghost FIFO chain.
  int32_t data;
  auto read1 = [&](size_t page) {
    cache.Read(&data, page * 1024 + 2, 1);
  };
  read1(0);
  read1(1);
  read1(0);
  read1(2);
  read1(1);
  read1(3);
  read1(2);
  read1(4);
  read1(3);
  read1(5);
  read1(6);
  read1(7);
  read1(8);

  // Assert -- all three queues hold multiple entries with comma separators.
  EXPECT_EQ(cache.Dump(), "[7, 8] {0, 1, 2, 3} [4, 5, 6]");

  // Assert -- every page still returns deterministic data after the churn.
  for (size_t i = 0; i < 9 * 1024; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, InvalidateCachedPageLogsEvictedEntryInSmallFifo) {
  // Arrange -- a 2-page budget (small=1, main=1, ghost=1).
  constexpr size_t kPages = 3;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 2 * 4096);

  // Act -- page 0 becomes resident (aligned reads promote it to accessed),
  // then Invalidate() flips it to kEvicted while it still occupies a slot in
  // the small FIFO.
  int32_t data;
  cache.Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));
  cache.Invalidate(0, 1);

  // Assert -- enqueueing a new page pops the stale kEvicted slot (this logs a
  // non-fatal "Evicted Page inside small fifo" ERROR and discards the entry);
  // re-reading page 0 then promotes page 1 to the main queue.
  cache.Read(&data, 1024, 1);
  ASSERT_EQ(data, Expected<int32_t>(1024));
  cache.Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));
  EXPECT_EQ(cache.Dump(), "[0] {1} []");
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, RepeatedReadsOfResidentPageStayConsistent) {
  // Arrange -- a single-page cache that keeps page 0 resident.
  constexpr size_t kCount = 1024;
  auto cache = MakeCache<int32_t>(kCount);

  // Act -- read element 0 many times; each read re-fixes the resident page
  // through the kUnlocked/kUnlockedAccessed promotion paths.
  for (int i = 0; i < 8; ++i) {
    int32_t data;
    cache->Read(&data, 0, 1);

    // Assert -- the cached value never changes across repeated fixes.
    ASSERT_EQ(data, Expected<int32_t>(0));
  }

  // Act -- interleave reads of the last element so the same resident page
  // serves both ends of the file.
  for (int i = 0; i < 4; ++i) {
    int32_t head;
    int32_t tail;
    cache->Read(&head, 0, 1);
    cache->Read(&tail, kCount - 1, 1);
    ASSERT_EQ(head, Expected<int32_t>(0));
    ASSERT_EQ(tail, Expected<int32_t>(kCount - 1));
  }

  // Assert -- the queues stay duplicate-free after all the churn.
  EXPECT_EQ(cache->Dump(), "[0] {} []");
}

TEST_F(VMCacheTest, ZeroCapacityConstructorLogsFatalButDoesNotAbort) {
  // Arrange -- a small backing file.
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  constexpr size_t kCount = 16;
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);

  // Act/Assert -- a zero-byte memory budget logs a (non-fatal) FATAL message;
  // the constructor still completes and the cache is destructible.  It must
  // never be used for actual reads.
  VMCache<int32_t> zero(fd_, 0);
  SUCCEED();
}

TEST(VMCacheInvalidFd, ConstructorLogsFileSizeFailure) {
  // Arrange/Act/Assert -- an invalid fd makes VMCacheImpl::FileSize() fail
  // (logging a non-fatal FATAL line) and return -1, which expands max_size_ to
  // SIZE_MAX.  The meta_ allocation then throws, proving the error path ran.
  // The exception propagates out of the constructor and is caught here.
  try {
    VMCache<int32_t> cache(-1, 4096);
    FAIL() << "meta_ allocation should throw after FileSize() failure";
  } catch (const std::exception&) {
    SUCCEED();
  }
}

TEST_F(VMCacheTest, InvalidateZeroLengthIsNoop) {
  // Arrange -- a single-page cache.
  constexpr size_t kCount = 1024;
  auto cache = MakeCache<int32_t>(kCount);

  // Act -- warm the page, then invalidate zero-length ranges.
  int32_t data;
  cache->Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));
  cache->Invalidate(0, 0);
  cache->Invalidate(512, 0);
  cache->Invalidate(1023, 0);

  // Assert -- nothing was invalidated; the cache still serves every element.
  for (size_t i = 0; i < 4; ++i) {
    cache->Read(&data, i * 256, 1);
    ASSERT_EQ(data, Expected<int32_t>(i * 256));
  }
}

TEST_F(VMCacheTest, InvalidateSpanningMultipleResidentPages) {
  // Arrange -- a three-page cache with all three pages resident.
  constexpr size_t kCount = 3 * 1024;
  auto cache = MakeCache<int32_t>(kCount);
  int32_t data;
  for (size_t page = 0; page < 3; ++page) {
    cache->Read(&data, page * 1024, 1);
    ASSERT_EQ(data, Expected<int32_t>(page * 1024));
  }

  // Act -- invalidate a range covering all three resident pages at once.
  cache->Invalidate(0, kCount);

  // Assert -- every element transparently reloads from the file afterwards.
  for (size_t i = 0; i < kCount; ++i) {
    cache->Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, SingleReadSpanningManyPagesWithTinyCache) {
  // Arrange -- 64 pages of data behind a 2-page memory budget.
  constexpr size_t kPages = 64;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 2 * 4096);

  // Act -- a single Read that spans all 64 pages, forcing ~62 evictions and
  // reloads from inside the Read loop itself.
  std::vector<int32_t> buffer(kCount);
  cache.Read(buffer.data(), 0, kCount);

  // Assert -- every element across the whole span is correct.
  for (size_t i = 0; i < kCount; ++i) {
    ASSERT_EQ(buffer[i], Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, OffsetConstructorWithEvictionPressure) {
  // Arrange -- 100 padding bytes followed by 16 pages of data, served through
  // a 2-page cache whose offset_ is 100 so every pread lands at offset+100.
  constexpr size_t kOffset = 100;
  constexpr size_t kPages = 16;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::string padding(kOffset, 'x');
  ASSERT_EQ(::write(fd_, padding.data(), padding.size()),
            static_cast<ssize_t>(kOffset));
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 2 * 4096, kOffset);

  // Act -- touch every page twice (unaligned then aligned) so pages churn
  // through both the kUnlocked and accessed eviction paths.
  int32_t data;
  for (size_t page = 0; page < kPages; ++page) {
    cache.Read(&data, page * 1024 + 7, 1);
    ASSERT_EQ(data, Expected<int32_t>(page * 1024 + 7));
    cache.Read(&data, page * 1024, 1);
    ASSERT_EQ(data, Expected<int32_t>(page * 1024));
  }
}

TEST_F(VMCacheTest, GhostHitPromotesToMainQueueWithExactDump) {
  // Arrange -- a 3-page budget (small=1, main=2, ghost=2).
  constexpr size_t kPages = 6;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 3 * 4096);

  int32_t data;
  auto read1 = [&](size_t offset) {
    cache.Read(&data, offset, 1);
    return data;
  };

  // Act -- single-touch non-aligned reads leave pages kUnlocked so they churn
  // through the small -> ghost FIFO chain with exact queue transitions.
  ASSERT_EQ(read1(1), Expected<int32_t>(1));
  EXPECT_EQ(cache.Dump(), "[0] {} []");
  ASSERT_EQ(read1(1025), Expected<int32_t>(1025));
  EXPECT_EQ(cache.Dump(), "[1] {} [0]");
  ASSERT_EQ(read1(2049), Expected<int32_t>(2049));
  EXPECT_EQ(cache.Dump(), "[2] {} [0, 1]");

  // Act -- re-reading the kMarked ghost page revives it directly into the main
  // queue (vm_cache_impl erases it from the ghost FIFO and enqueues to main).
  ASSERT_EQ(read1(1), Expected<int32_t>(1));
  EXPECT_EQ(cache.Dump(), "[2] {0} [1]");

  // Act -- loading page 3 evicts the small-queue front into the ghost FIFO.
  ASSERT_EQ(read1(3073), Expected<int32_t>(3073));
  EXPECT_EQ(cache.Dump(), "[3] {0} [1, 2]");

  // Assert -- every element still round-trips after the queue transitions.
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, AlignedAndUnalignedInterleavedChurn) {
  // Arrange -- a 3-page budget and 12 pages of data.
  constexpr size_t kPages = 12;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 3 * 4096);

  // Act -- alternate aligned reads (which fix each page twice and leave it
  // accessed) with unaligned reads (which fix each page once) under heavy
  // eviction pressure.
  int32_t data;
  for (size_t page = 0; page < kPages; ++page) {
    cache.Read(&data, page * 1024, 1);
    ASSERT_EQ(data, Expected<int32_t>(page * 1024));
    cache.Read(&data, page * 1024 + 5, 1);
    ASSERT_EQ(data, Expected<int32_t>(page * 1024 + 5));
  }

  // Assert -- the queues stay duplicate-free and all data remains intact.
  EXPECT_EQ(cache.Dump().front(), '[');
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, RepeatedInvalidateAndReloadCycles) {
  // Arrange -- a 3-page budget and 4 pages of data.
  constexpr size_t kPages = 4;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 3 * 4096);

  // Act -- repeatedly warm all four pages (kUnlocked/accessed), invalidate the
  // entire address space, then reload.  Invalidate must drop every page from
  // all three FIFOs each round so SanityCheck never sees a duplicate.
  int32_t data;
  for (int round = 0; round < 8; ++round) {
    for (size_t page = 0; page < kPages; ++page) {
      cache.Read(&data, page * 1024 + 3, 1);
      ASSERT_EQ(data, Expected<int32_t>(page * 1024 + 3));
    }
    cache.Invalidate(0, kCount);
    for (size_t i = 0; i < kCount; ++i) {
      cache.Read(&data, i, 1);
      ASSERT_EQ(data, Expected<int32_t>(i));
    }
    EXPECT_EQ(cache.Dump().front(), '[');
  }
}

// A page touched exactly once (non-aligned read) stays kUnlocked, so evictions
// churn through the small -> ghost FIFO chain.  Revived ghost pages are
// re-enqueued into the MAIN queue (kUnlocked).  When the main FIFO fills with
// such revived pages, the next accessed page promoted from the small FIFO
// evicts the oldest revived page (kUnlocked -> kEvicted in
// EnqueueToMainFifo).
TEST_F(VMCacheTest, GhostRevivedPagesInMainQueueAreEvictedUnderAccessPressure) {
  // Arrange -- a 5-page budget (small=1, main=4, ghost=4).
  constexpr size_t kPages = 7;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 5 * 4096);

  // Act -- touch pages 0..4 once (non-aligned, kUnlocked) so pages 0..3 land
  // in the ghost FIFO while page 4 stays in the small FIFO.
  int32_t data;
  auto read1 = [&](size_t offset) {
    cache.Read(&data, offset, 1);
    return data;
  };
  for (size_t page = 0; page < 5; ++page) {
    read1(page * 1024 + 3);
  }
  ASSERT_EQ(cache.Dump(), "[4] {} [0, 1, 2, 3]");

  // Act -- revive pages 0..3; each kMarked ghost page is moved to the main
  // queue (kUnlocked).
  for (size_t page = 0; page < 4; ++page) {
    read1(page * 1024 + 3);
  }
  ASSERT_EQ(cache.Dump(), "[4] {0, 1, 2, 3} []");

  // Act -- make page 5 accessed (kUnlockedAccessed), then read page 6; the
  // small FIFO promotes page 5 into the full main queue, evicting page 0.
  read1(5 * 1024 + 3);
  read1(5 * 1024 + 3);
  ASSERT_EQ(cache.Dump(), "[5] {0, 1, 2, 3} [4]");
  read1(6 * 1024 + 3);

  // Assert -- the oldest revived page was evicted from the main queue.
  ASSERT_EQ(cache.Dump(), "[6] {1, 2, 3, 5} [4]");

  // Assert -- all data survives the main-queue evictions.
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

// Invalidate() of an accessed page resident in the MAIN queue removes it from
// the queue and flips it to kEvicted; the next read reloads it as a fresh page
// (small FIFO) and evicts the small-FIFO front into the main queue.
TEST_F(VMCacheTest, InvalidateAccessedPageResidentInMainQueue) {
  // Arrange -- a 5-page budget (small=1, main=4, ghost=4).
  constexpr size_t kPages = 5;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 5 * 4096);

  // Act -- aligned reads fix each page twice (accessed), so pages 0..2 promote
  // to the main queue and pages 3..4 churn through the small FIFO.
  int32_t data;
  auto read1 = [&](size_t offset) {
    cache.Read(&data, offset, 1);
    return data;
  };
  for (size_t page = 0; page < 3; ++page) {
    read1(page * 1024);
    read1(page * 1024);
  }
  read1(3 * 1024);
  read1(4 * 1024);
  ASSERT_EQ(cache.Dump(), "[4] {0, 1, 2, 3} []");

  // Act -- invalidate the page-0 element range; page 0 leaves the main queue.
  cache.Invalidate(0, 1024);
  ASSERT_EQ(cache.Dump(), "[4] {1, 2, 3} []");

  // Act -- re-read element 0; the fresh page 0 enters the small FIFO and the
  // small-FIFO front (page 4) is promoted into the main queue.
  ASSERT_EQ(read1(0), Expected<int32_t>(0));
  ASSERT_EQ(cache.Dump(), "[0] {1, 2, 3, 4} []");

  // Assert -- all data remains correct after the invalidate/reload cycle.
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

// An ALIGNED read at a page start has a zero-length first chunk, so it fixes
// the page twice (kLocked -> kUnlocked -> kLockedAccessed -> kUnlockedAccessed)
// and the page is treated as accessed: on eviction it is promoted to the main
// queue instead of the ghost queue.
TEST_F(VMCacheTest, AlignedReadIsAccessedAndEvictsToMainQueue) {
  // Arrange -- a 3-page budget (small=1, main=2, ghost=2).
  constexpr size_t kCount = 3 * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 3 * 4096);

  // Act -- one aligned read of page 0 is enough to mark it accessed.
  int32_t data;
  auto read1 = [&](size_t offset) {
    cache.Read(&data, offset, 1);
    return data;
  };
  read1(0);
  ASSERT_EQ(cache.Dump(), "[0] {} []");

  // Act -- the unaligned read of page 1 overflows the small FIFO; accessed
  // page 0 is promoted to the main queue, not evicted to the ghost queue.
  read1(1025);
  ASSERT_EQ(cache.Dump(), "[1] {0} []");

  // Act -- page 2's enqueue demotes page 1 (kUnlocked, single fix) to the
  // ghost queue.
  read1(2049);
  ASSERT_EQ(cache.Dump(), "[2] {0} [1]");

  // Assert -- all data still round-trips.
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

// An UNALIGNED read fixes the page exactly once (kLocked -> kUnlocked), so the
// page stays "cold" and is evicted into the ghost queue on FIFO overflow.
TEST_F(VMCacheTest, UnalignedReadStaysUnlockedAndEvictsToGhostQueue) {
  // Arrange -- a 3-page budget (small=1, main=2, ghost=2).
  constexpr size_t kCount = 3 * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 3 * 4096);

  // Act -- three non-aligned single-touch reads churn through small -> ghost.
  int32_t data;
  auto read1 = [&](size_t offset) {
    cache.Read(&data, offset, 1);
    return data;
  };
  read1(3);
  ASSERT_EQ(cache.Dump(), "[0] {} []");
  read1(1027);
  ASSERT_EQ(cache.Dump(), "[1] {} [0]");
  read1(2051);
  ASSERT_EQ(cache.Dump(), "[2] {} [0, 1]");

  // Assert -- the ghost pages transparently reload with the right data.
  ASSERT_EQ(read1(3), Expected<int32_t>(3));
  ASSERT_EQ(read1(1027), Expected<int32_t>(1027));
}

// A zero-length Read never fixes a page: the buffer contents stay untouched and
// the queue layout is unchanged.
TEST_F(VMCacheTest, ReadZeroLengthDoesNotTouchPages) {
  // Arrange -- a single-page cache.
  constexpr size_t kCount = 1024;
  auto cache = MakeCache<int32_t>(kCount);

  // Act -- warm page 0, then issue zero-length reads and invalidations.
  int32_t data = -1;
  cache->Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));
  ASSERT_EQ(cache->Dump(), "[0] {} []");
  cache->Read(&data, 0, 0);
  cache->Read(&data, 512, 0);
  cache->Invalidate(0, 0);
  cache->Invalidate(512, 0);

  // Assert -- the resident page and buffer are untouched.
  ASSERT_EQ(cache->Dump(), "[0] {} []");
  cache->Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));
}

// Invalidate() of a range covering every resident page must drop those pages
// from all three FIFOs, leaving a completely empty cache.
TEST_F(VMCacheTest, InvalidateWholeFileClearsAllQueues) {
  // Arrange -- a 5-page budget (small=1, main=4, ghost=4).
  constexpr size_t kPages = 7;
  constexpr size_t kCount = kPages * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote =
        ::write(fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, 5 * 4096);

  // Act -- populate the small, main and ghost queues.
  int32_t data;
  auto read1 = [&](size_t offset) {
    cache.Read(&data, offset, 1);
    return data;
  };
  for (size_t page = 0; page < 5; ++page) {
    read1(page * 1024 + 3);
  }
  for (size_t page = 0; page < 4; ++page) {
    read1(page * 1024 + 3);
  }
  read1(5 * 1024 + 3);
  read1(5 * 1024 + 3);
  read1(6 * 1024 + 3);
  ASSERT_EQ(cache.Dump(), "[6] {1, 2, 3, 5} [4]");

  // Act -- invalidate the whole file.
  cache.Invalidate(0, kCount);

  // Assert -- every queue is empty.
  ASSERT_EQ(cache.Dump(), "[] {} []");

  // Assert -- the data still reloads correctly from the file.
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

}  // namespace
}  // namespace tinylamb
