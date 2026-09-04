
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

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

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
              offset + (data_size * sizeof(T)));
    return std::make_unique<VMCache<T>>(fd_, data_size * 1024, offset);
  }

  void TearDown() override {
    if (0 <= fd_) {
      ::close(fd_);
    }
    if (!path_.empty()) {
      std::ignore = std::remove(path_.c_str());
    }
  }

  template <typename T>
  static T Expected(size_t key) {
    return T(kSeed + std::hash<size_t>()(key));
  }

  constexpr static int kSeed = 0;
  int fd_{-1};
  std::filesystem::path path_;
};

TEST_F(VMCacheTest, Read_OnePage_ReturnsExpectedData) {
  constexpr size_t kCount = 1024;
  auto cache = MakeCache<int32_t>(kCount);

  for (int i = 0; i < 1024; ++i) {
    int32_t data = 0;
    cache->Read(&data, i, 1);

    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, Read_WithOffsets_ReturnsExpectedData) {
  constexpr size_t kCount = 1024;
  for (int i = 1; i < 4096; i += 127) {
    auto cache = MakeCache<int32_t>(kCount, i);

    for (int j = 0; j < 1024; ++j) {
      int32_t data = 0;
      cache->Read(&data, j, 1);
      if (data != Expected<int32_t>(j)) {
        exit(1);
      }
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

TEST_F(VMCacheTest, Read_StructWithOffsets_ReturnsExpectedData) {
  constexpr size_t kCount = 1024;
  for (int i = 1; i < 4096; i += 127) {
    auto cache = MakeCache<Data>(kCount, i);

    for (int j = 0; j < 1024; ++j) {
      Data data;
      cache->Read(&data, j, 1);

      ASSERT_EQ(data, Expected<Data>(j));
    }
  }
}

TEST_F(VMCacheTest, Invalidate_SingleElement_ReloadsExpectedData) {
  constexpr size_t kCount = 1024;
  auto cache = MakeCache<int32_t>(kCount);

  int32_t data = 0;
  cache->Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));
  cache->Invalidate(0, 1);
  cache->Read(&data, 0, 1);

  ASSERT_EQ(data, Expected<int32_t>(0));
}

TEST_F(VMCacheTest, Invalidate_RangeAcrossPages_ReloadsExpectedData) {
  constexpr size_t kCount = 2048;
  auto cache = MakeCache<int32_t>(kCount);

  cache->Invalidate(1023, 2);

  for (int i = 1020; i < 1028; ++i) {
    int32_t data = 0;
    cache->Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, Read_CrossPageRange_ReturnsExpectedData) {
  constexpr size_t kCount = 2048;
  auto cache = MakeCache<int32_t>(kCount);

  std::array<int32_t, 2> buf{};
  cache->Read(buf.data(), 1023, 2);

  EXPECT_EQ(buf[0], Expected<int32_t>(1023));
  EXPECT_EQ(buf[1], Expected<int32_t>(1024));
}

TEST_F(VMCacheTest, Dump_ResidentPages_OutputsExpectedFormat) {
  constexpr size_t kCount = 1024;
  auto cache = MakeCache<int32_t>(kCount);

  for (int i = 0; i < 1024; ++i) {
    int32_t data = 0;
    cache->Read(&data, i, 1);
  }
  const std::string dump = cache->Dump();

  EXPECT_EQ(dump.front(), '[');
  EXPECT_NE(dump.find('}'), std::string::npos);
  std::ostringstream oss;
  oss << *cache;
  EXPECT_FALSE(oss.str().empty());
}

TEST_F(VMCacheTest, FindNearestSize_VariousSizes_ReturnsMultipleOfPageSize) {
  auto cache = MakeCache<int32_t>(16);

  EXPECT_EQ(cache->FindNearestSize(), 4096);
  EXPECT_EQ(cache->FindNearestSize(4, 4096), 4096);
  EXPECT_EQ(cache->FindNearestSize(100, 4096), 4096);
  EXPECT_EQ(cache->FindNearestSize(3000, 4096), 4096);
  EXPECT_EQ(cache->FindNearestSize(5000, 4096), 8192);
}

TEST_F(VMCacheTest, Constructor_ExplicitFileSize_ReadsExpectedData) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  const size_t file_size = std::filesystem::file_size(path_);
  VMCache<int32_t> cache(fd_, kCount * 1024, 0, file_size);

  int32_t data = 0;
  cache.Read(&data, 42, 1);

  ASSERT_EQ(data, Expected<int32_t>(42));
}

TEST_F(VMCacheTest, Read_UnderEvictionPressure_TransparentlyReloadsData) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(2) * 4096);

  for (size_t page = 0; page < kPages; ++page) {
    int32_t data = 0;
    cache.Read(&data, page * 1024, 1);

    ASSERT_EQ(data, Expected<int32_t>(page * 1024));
  }
}

TEST_F(VMCacheTest, Read_AccessedPages_PromotesToMainQueue) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(2) * 4096);

  int32_t data = 0;
  cache.Read(&data, 0, 1);
  cache.Read(&data, 0, 1);
  cache.Read(&data, 1024, 1);
  cache.Read(&data, 2048, 1);

  const std::string dump = cache.Dump();
  EXPECT_EQ(dump.front(), '[');
  EXPECT_NE(dump.find('}'), std::string::npos);
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, Read_MarkedGhostPage_PromotesToMainQueue) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(2) * 4096);

  int32_t data = 0;
  cache.Read(&data, 0, 1);
  cache.Read(&data, 1024, 1);
  cache.Read(&data, 0, 1);
  cache.Read(&data, 2048, 1);

  EXPECT_EQ(cache.Dump().front(), '[');
  cache.Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, Read_FullMainQueue_EvictsAccessedPages) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(5) * 4096);

  int32_t data = 0;
  for (size_t page = 0; page < 5; ++page) {
    cache.Read(&data, page * 1024, 1);
    cache.Read(&data, page * 1024, 1);
  }
  cache.Read(&data, 0, 1);
  cache.Read(&data, static_cast<size_t>(5) * 1024, 1);

  EXPECT_EQ(cache.Dump().front(), '[');
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, Dump_MultiEntrySmallAndMainQueues_OutputsExpectedFormat) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(20) * 4096);

  int32_t data = 0;
  cache.Read(&data, 0, 1);
  cache.Read(&data, 1024, 1);
  EXPECT_EQ(cache.Dump(), "[0, 1] {} []");

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

TEST_F(VMCacheTest, Read_SingleTouchPages_AccumulatesInGhostQueue) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(20) * 4096);

  int32_t data = 0;
  for (size_t page = 0; page < 5; ++page) {
    cache.Read(&data, page * 1024, 1);
  }

  EXPECT_EQ(cache.Dump(), "[3, 4] {0, 1, 2} []");

  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, Read_LargeSpanningBuffer_ReadsAllDataCorrectly) {
  constexpr size_t kCount = static_cast<size_t>(6) * 1024;
  auto cache = MakeCache<int32_t>(kCount);

  std::vector<int32_t> buffer(3000);
  cache->Read(buffer.data(), 1000, 3000);

  for (size_t i = 0; i < 3000; ++i) {
    ASSERT_EQ(buffer[i], Expected<int32_t>(1000 + i));
  }

  int32_t tail = 0;
  cache->Read(&tail, kCount - 1, 1);
  ASSERT_EQ(tail, Expected<int32_t>(kCount - 1));
}

TEST_F(VMCacheTest, Invalidate_BeyondFileEnd_IsClampedWithoutError) {
  constexpr size_t kCount = 1024;
  auto cache = MakeCache<int32_t>(kCount);

  int32_t data = 0;
  cache->Read(&data, 0, 1);
  cache->Invalidate(1020, 80);

  for (int i = 1000; i < 1020; ++i) {
    cache->Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }

  cache->Invalidate(1024LL * 1024 * 1024, 1);

  for (int i = 0; i < 64; ++i) {
    cache->Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

// Invalidate marked (ghost) and unlocked (small) pages, then reload them.
// Invalidate must drop the page from every FIFO so a later FixPage enqueue
// cannot trip SanityCheck's duplicate detection.
TEST_F(VMCacheTest, Invalidate_MarkedAndUnlockedPages_ReloadsCorrectly) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(20) * 4096);

  int32_t data = 0;
  for (size_t page = 0; page < 3; ++page) {
    cache.Read(&data, (page * 1024) + 1, 1);
    ASSERT_EQ(data, Expected<int32_t>((page * 1024) + 1));
  }
  EXPECT_EQ(cache.Dump(), "[1, 2] {} [0]");

  cache.Invalidate(0, 1);
  cache.Read(&data, 1, 1);
  ASSERT_EQ(data, Expected<int32_t>(1));
  cache.Invalidate(0, 1);
  cache.Read(&data, 1, 1);
  ASSERT_EQ(data, Expected<int32_t>(1));

  cache.Read(&data, 1025, 1);
  ASSERT_EQ(data, Expected<int32_t>(1025));
  EXPECT_EQ(cache.Dump(), "[2, 0] {1} []");
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, Read_GhostFifoOverflow_EvictsMarkedEntry) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(3) * 4096);

  int32_t data = 0;
  for (size_t page = 0; page < 4; ++page) {
    cache.Read(&data, (page * 1024) + 1, 1);
    ASSERT_EQ(data, Expected<int32_t>((page * 1024) + 1));
  }

  EXPECT_EQ(cache.Dump(), "[3] {} [1, 2]");

  cache.Read(&data, 1, 1);
  ASSERT_EQ(data, Expected<int32_t>(1));
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, Read_SmallQueueOverflow_EvictsSingleTouchedPageToGhost) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(2) * 4096);

  int32_t data = 0;
  cache.Read(&data, 1, 1);
  cache.Read(&data, 1025, 1);

  EXPECT_EQ(cache.Dump(), "[1] {} [0]");

  cache.Read(&data, (2 * 1024) + 1, 1);
  EXPECT_EQ(cache.Dump(), "[2] {} [1]");

  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, Read_CrossPageTwelveByteStruct_ReturnsExpectedData) {
  constexpr size_t kCount = 5000;
  auto cache = MakeCache<Data>(kCount);

  std::vector<Data> buffer(2000);
  cache->Read(buffer.data(), 2500, 2000);

  for (size_t i = 0; i < 2000; ++i) {
    ASSERT_EQ(buffer[i], Expected<Data>(2500 + i));
  }

  Data tail;
  cache->Read(&tail, kCount - 1, 1);
  ASSERT_EQ(tail, Expected<Data>(kCount - 1));

  cache->Invalidate(340, 2);
  Data first;
  Data second;
  cache->Read(&first, 340, 1);
  cache->Read(&second, 341, 1);
  ASSERT_EQ(first, Expected<Data>(340));
  ASSERT_EQ(second, Expected<Data>(341));
}

TEST_F(VMCacheTest, Constructor_ExplicitFileSizeAndOffset_ReadsExpectedData) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  const size_t file_size = std::filesystem::file_size(path_);

  VMCache<int32_t> cache(fd_, static_cast<size_t>(1024) * 1024, kOffset,
                         file_size);

  for (size_t i = 0; i < kCount; ++i) {
    int32_t data = 0;
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, Dump_AllThreeQueuesMultiEntry_OutputsExpectedFormat) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(20) * 4096);

  int32_t data = 0;
  auto read1 = [&](size_t page) { cache.Read(&data, (page * 1024) + 2, 1); };
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

  EXPECT_EQ(cache.Dump(), "[7, 8] {0, 1, 2, 3} [4, 5, 6]");

  for (size_t i = 0; i < static_cast<size_t>(9) * 1024; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, Invalidate_CachedPage_DropsFromSmallFifo) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(2) * 4096);

  int32_t data = 0;
  cache.Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));
  cache.Invalidate(0, 1);

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

TEST_F(VMCacheTest, Read_RepeatedResidentPage_StaysConsistent) {
  constexpr size_t kCount = 1024;
  auto cache = MakeCache<int32_t>(kCount);

  for (int i = 0; i < 8; ++i) {
    int32_t data = 0;
    cache->Read(&data, 0, 1);

    ASSERT_EQ(data, Expected<int32_t>(0));
  }

  for (int i = 0; i < 4; ++i) {
    int32_t head = 0;
    int32_t tail = 0;
    cache->Read(&head, 0, 1);
    cache->Read(&tail, kCount - 1, 1);
    ASSERT_EQ(head, Expected<int32_t>(0));
    ASSERT_EQ(tail, Expected<int32_t>(kCount - 1));
  }

  EXPECT_EQ(cache->Dump(), "[0] {} []");
}

TEST_F(VMCacheTest, Constructor_ZeroCapacity_ThrowsException) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);

  try {
    VMCache<int32_t> zero(fd_, 0);
    FAIL() << "zero-capacity constructor should throw";
  } catch (const std::exception&) {
    SUCCEED();
  }
}

TEST_F(VMCacheTest, Constructor_InvalidFd_LogsFileSizeFailure) {
  try {
    VMCache<int32_t> cache(-1, 4096);
    FAIL() << "meta_ allocation should throw after FileSize() failure";
  } catch (const std::exception&) {
    SUCCEED();
  }
}

TEST_F(VMCacheTest, Invalidate_ZeroLength_DoesNothing) {
  constexpr size_t kCount = 1024;
  auto cache = MakeCache<int32_t>(kCount);

  int32_t data = 0;
  cache->Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));
  cache->Invalidate(0, 0);
  cache->Invalidate(512, 0);
  cache->Invalidate(1023, 0);

  for (size_t i = 0; i < 4; ++i) {
    cache->Read(&data, i * 256, 1);
    ASSERT_EQ(data, Expected<int32_t>(i * 256));
  }
}

TEST_F(VMCacheTest, Invalidate_SpanningMultipleResidentPages_ReloadsCorrectly) {
  constexpr size_t kCount = static_cast<size_t>(3) * 1024;
  auto cache = MakeCache<int32_t>(kCount);
  int32_t data = 0;
  for (size_t page = 0; page < 3; ++page) {
    cache->Read(&data, page * 1024, 1);
    ASSERT_EQ(data, Expected<int32_t>(page * 1024));
  }

  cache->Invalidate(0, kCount);

  for (size_t i = 0; i < kCount; ++i) {
    cache->Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, Read_SpanningManyPagesWithTinyCache_ReadsAllDataCorrectly) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(2) * 4096);

  std::vector<int32_t> buffer(kCount);
  cache.Read(buffer.data(), 0, kCount);

  for (size_t i = 0; i < kCount; ++i) {
    ASSERT_EQ(buffer[i], Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, Read_OffsetCacheWithEvictionPressure_ReadsExpectedData) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(2) * 4096, kOffset);

  int32_t data = 0;
  for (size_t page = 0; page < kPages; ++page) {
    cache.Read(&data, (page * 1024) + 7, 1);
    ASSERT_EQ(data, Expected<int32_t>((page * 1024) + 7));
    cache.Read(&data, page * 1024, 1);
    ASSERT_EQ(data, Expected<int32_t>(page * 1024));
  }
}

TEST_F(VMCacheTest, Read_GhostHit_PromotesToMainQueue) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(3) * 4096);

  int32_t data = 0;
  auto read1 = [&](size_t offset) {
    cache.Read(&data, offset, 1);
    return data;
  };

  ASSERT_EQ(read1(1), Expected<int32_t>(1));
  EXPECT_EQ(cache.Dump(), "[0] {} []");
  ASSERT_EQ(read1(1025), Expected<int32_t>(1025));
  EXPECT_EQ(cache.Dump(), "[1] {} [0]");
  ASSERT_EQ(read1(2049), Expected<int32_t>(2049));
  EXPECT_EQ(cache.Dump(), "[2] {} [0, 1]");

  ASSERT_EQ(read1(1), Expected<int32_t>(1));
  EXPECT_EQ(cache.Dump(), "[2] {0} [1]");

  ASSERT_EQ(read1(3073), Expected<int32_t>(3073));
  EXPECT_EQ(cache.Dump(), "[3] {0} [1, 2]");

  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, Read_AlignedAndUnalignedInterleaved_MaintainsConsistency) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(3) * 4096);

  int32_t data = 0;
  for (size_t page = 0; page < kPages; ++page) {
    cache.Read(&data, page * 1024, 1);
    ASSERT_EQ(data, Expected<int32_t>(page * 1024));
    cache.Read(&data, (page * 1024) + 5, 1);
    ASSERT_EQ(data, Expected<int32_t>((page * 1024) + 5));
  }

  EXPECT_EQ(cache.Dump().front(), '[');
  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, Invalidate_RepeatedCycles_MaintainsConsistency) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(3) * 4096);

  int32_t data = 0;
  for (int round = 0; round < 8; ++round) {
    for (size_t page = 0; page < kPages; ++page) {
      cache.Read(&data, (page * 1024) + 3, 1);
      ASSERT_EQ(data, Expected<int32_t>((page * 1024) + 3));
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
TEST_F(VMCacheTest, Read_GhostRevivedPagesInMainQueue_EvictsUnderPressure) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(5) * 4096);

  int32_t data = 0;
  auto read1 = [&](size_t offset) {
    cache.Read(&data, offset, 1);
    return data;
  };
  for (size_t page = 0; page < 5; ++page) {
    read1((page * 1024) + 3);
  }
  ASSERT_EQ(cache.Dump(), "[4] {} [0, 1, 2, 3]");

  for (size_t page = 0; page < 4; ++page) {
    read1((page * 1024) + 3);
  }
  ASSERT_EQ(cache.Dump(), "[4] {0, 1, 2, 3} []");

  read1((5 * 1024) + 3);
  read1((5 * 1024) + 3);
  ASSERT_EQ(cache.Dump(), "[5] {0, 1, 2, 3} [4]");
  read1((6 * 1024) + 3);

  ASSERT_EQ(cache.Dump(), "[6] {1, 2, 3, 5} [4]");

  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

// Invalidate() of an accessed page resident in the MAIN queue removes it from
// the queue and flips it to kEvicted; the next read reloads it as a fresh page
// (small FIFO) and evicts the small-FIFO front into the main queue.
TEST_F(VMCacheTest,
       Invalidate_AccessedPageInMainQueue_DropsAndReloadsCorrectly) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(5) * 4096);

  int32_t data = 0;
  auto read1 = [&](size_t offset) {
    cache.Read(&data, offset, 1);
    return data;
  };
  for (size_t page = 0; page < 3; ++page) {
    read1(page * 1024);
    read1(page * 1024);
  }
  read1(static_cast<size_t>(3) * 1024);
  read1(static_cast<size_t>(4) * 1024);
  ASSERT_EQ(cache.Dump(), "[4] {0, 1, 2, 3} []");

  cache.Invalidate(0, 1024);
  ASSERT_EQ(cache.Dump(), "[4] {1, 2, 3} []");

  ASSERT_EQ(read1(0), Expected<int32_t>(0));
  ASSERT_EQ(cache.Dump(), "[0] {1, 2, 3, 4} []");

  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

// An ALIGNED read at a page start has a zero-length first chunk, so it fixes
// the page twice (kLocked -> kUnlocked -> kLockedAccessed -> kUnlockedAccessed)
// and the page is treated as accessed: on eviction it is promoted to the main
// queue instead of the ghost queue.
TEST_F(VMCacheTest, Read_AlignedRead_EvictsToMainQueue) {
  constexpr size_t kCount = static_cast<size_t>(3) * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(3) * 4096);

  int32_t data = 0;
  auto read1 = [&](size_t offset) {
    cache.Read(&data, offset, 1);
    return data;
  };
  read1(0);
  ASSERT_EQ(cache.Dump(), "[0] {} []");

  read1(1025);
  ASSERT_EQ(cache.Dump(), "[1] {0} []");

  read1(2049);
  ASSERT_EQ(cache.Dump(), "[2] {0} [1]");

  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

// An UNALIGNED read fixes the page exactly once (kLocked -> kUnlocked), so the
// page stays "cold" and is evicted into the ghost queue on FIFO overflow.
TEST_F(VMCacheTest, Read_UnalignedRead_EvictsToGhostQueue) {
  constexpr size_t kCount = static_cast<size_t>(3) * 1024;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  size_t remaining = value.size() * sizeof(int32_t);
  size_t written = 0;
  while (0 < remaining) {
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(3) * 4096);

  int32_t data = 0;
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

  ASSERT_EQ(read1(3), Expected<int32_t>(3));
  ASSERT_EQ(read1(1027), Expected<int32_t>(1027));
}

// A zero-length Read never fixes a page: the buffer contents stay untouched and
// the queue layout is unchanged.
TEST_F(VMCacheTest, Read_ZeroLength_DoesNotTouchPages) {
  constexpr size_t kCount = 1024;
  auto cache = MakeCache<int32_t>(kCount);

  int32_t data = -1;
  cache->Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));
  ASSERT_EQ(cache->Dump(), "[0] {} []");
  cache->Read(&data, 0, 0);
  cache->Read(&data, 512, 0);
  cache->Invalidate(0, 0);
  cache->Invalidate(512, 0);

  ASSERT_EQ(cache->Dump(), "[0] {} []");
  cache->Read(&data, 0, 1);
  ASSERT_EQ(data, Expected<int32_t>(0));
}

// Invalidate() of a range covering every resident page must drop those pages
// from all three FIFOs, leaving a completely empty cache.
TEST_F(VMCacheTest, Invalidate_WholeFile_ClearsAllQueues) {
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
    ssize_t wrote = ::write(
        fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
    ASSERT_LT(0, wrote);
    written += wrote;
    remaining -= wrote;
  }
  ::fsync(fd_);
  VMCache<int32_t> cache(fd_, static_cast<size_t>(5) * 4096);

  int32_t data = 0;
  auto read1 = [&](size_t offset) {
    cache.Read(&data, offset, 1);
    return data;
  };
  for (size_t page = 0; page < 5; ++page) {
    read1((page * 1024) + 3);
  }
  for (size_t page = 0; page < 4; ++page) {
    read1((page * 1024) + 3);
  }
  read1((5 * 1024) + 3);
  read1((5 * 1024) + 3);
  read1((6 * 1024) + 3);
  ASSERT_EQ(cache.Dump(), "[6] {1, 2, 3, 5} [4]");

  cache.Invalidate(0, kCount);

  ASSERT_EQ(cache.Dump(), "[] {} []");

  for (size_t i = 0; i < kCount; ++i) {
    cache.Read(&data, i, 1);
    ASSERT_EQ(data, Expected<int32_t>(i));
  }
}

TEST_F(VMCacheTest, ReadAt_VariousOffsetsAndLengths_ReturnsExpectedResults) {
  constexpr size_t kCount = 100;
  path_ = "vm_cache_test-" + RandomString();
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
  std::vector<int32_t> value(kCount);
  for (size_t i = 0; i < kCount; ++i) {
    value[i] = Expected<int32_t>(i);
  }
  ssize_t written = ::write(fd_, value.data(), value.size() * sizeof(int32_t));
  EXPECT_EQ(written, value.size() * sizeof(int32_t));

  VMCacheImpl impl(fd_, 4096, 16384, 0, 0, false);
  std::string s = impl.ReadAt(0, sizeof(int32_t) * 5);
  EXPECT_EQ(s.size(), sizeof(int32_t) * 5);

  std::string_view sv;
  auto locks_zero = impl.ReadAt(0, 0, sv);
  EXPECT_TRUE(locks_zero.empty());
  EXPECT_TRUE(sv.empty());

  auto locks_oob = impl.ReadAt(100000000, 10, sv);
  EXPECT_TRUE(locks_oob.empty());
  EXPECT_TRUE(sv.empty());
}

}  // namespace
}  // namespace tinylamb
