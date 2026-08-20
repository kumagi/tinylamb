
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

  // Act -- read pages 0..2 once: page 0 is evicted into the ghost queue
  // (kMarked), pages 1 and 2 stay in the small queue (kUnlocked).
  int32_t data;
  for (size_t page = 0; page < 3; ++page) {
    cache.Read(&data, page * 1024, 1);
    ASSERT_EQ(data, Expected<int32_t>(page * 1024));
  }
  EXPECT_EQ(cache.Dump(), "[1, 2] {} [0]");

  // Act -- invalidate the kMarked ghost page 0, then reload it (promotes page 1
  // to the ghost queue), then invalidate and reload the kUnlocked page 0 again.
  cache.Invalidate(0, 1);
  cache.Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));
  cache.Invalidate(0, 1);
  cache.Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));

  // Assert -- the surviving ghost page 1 revives through the main queue and
  // every page still returns the deterministic data.
  cache.Read(&data, 1024, 1);
  ASSERT_EQ(data, Expected<int32_t>(1024));
  EXPECT_EQ(cache.Dump(), "[0] {1} [2]");
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

  // Act -- read pages 0..3 once; the ghost queue overflows when page 2 is
  // evicted, dropping the kMarked page 0 back to kEvicted.
  int32_t data;
  for (size_t page = 0; page < 4; ++page) {
    cache.Read(&data, page * 1024, 1);
    ASSERT_EQ(data, Expected<int32_t>(page * 1024));
  }

  // Assert -- the ghost FIFO kept [1, 2] and page 0 was fully evicted.
  EXPECT_EQ(cache.Dump(), "[3] {} [1, 2]");

  // Assert -- the evicted page 0 transparently reloads from the file.
  cache.Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));
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

  // Act -- pages touched exactly once stay kUnlocked and churn through the
  // small -> ghost FIFO chain as new pages arrive.
  int32_t data;
  cache.Read(&data, 0, 1);
  cache.Read(&data, 1024, 1);

  // Assert -- page 0 was demoted to the ghost queue, page 1 is resident.
  EXPECT_EQ(cache.Dump(), "[1] {} [0]");

  cache.Read(&data, 2 * 1024, 1);
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

}  // namespace
}  // namespace tinylamb