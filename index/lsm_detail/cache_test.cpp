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

#include "index/lsm_detail/cache.hpp"

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstddef>
#include <cstdio>
#include <cstring>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "common/random_string.hpp"
#include "gtest/gtest.h"
#include "index/lsm_detail/blob_file.hpp"

namespace tinylamb {

class CacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = "cache_test-" + RandomString();
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
    size_t kSize = 4L * 1024L * 1024L;
    std::vector<int> value;
    value.resize(kSize);
    for (size_t i = 0; i < kSize; ++i) {
      value[i] = Expected(i);
    }
    ASSERT_EQ(value.size(), kSize);
    size_t remaining = value.size() * sizeof(int);
    size_t written = 0;
    while (0 < remaining) {
      ssize_t wrote = ::write(
          fd_, reinterpret_cast<char*>(value.data()) + written, remaining);
      ASSERT_LT(0, wrote);
      written += wrote;
      remaining -= wrote;
    }
    ASSERT_EQ(written, value.size() * sizeof(int));
    ::fsync(fd_);
    ASSERT_EQ(std::filesystem::file_size(path_), kSize * sizeof(int));
    cache_ = std::make_unique<Cache>(fd_, 32 * 1024);
  }

  void TearDown() override {
    ::close(fd_);
    std::ignore = std::remove(path_.c_str());
  }

  static int Expected(size_t pos) { return kSeed + std::hash<size_t>()(pos); }

  constexpr static int kSeed = 0xdeadbeef;
  int fd_;
  std::filesystem::path path_;
  std::unique_ptr<Cache> cache_;
};

TEST_F(CacheTest, one_page) {
  // Arrange -- nothing more than default CacheTest SetUp(); 4 MiB file + 32 KiB cache
  // Act -- read 1024 sequential 4-byte ints at stride 4
  for (int i = 0; i < 1024; ++i) {
    std::string data = cache_->ReadAt(i * sizeof(int), sizeof(int));
    int data_as_int = *(reinterpret_cast<int*>(data.data()));

    // Assert -- each read yields the deterministic Expected(i) value
    ASSERT_EQ(data_as_int, Expected(i));
  }
}

TEST_F(CacheTest, mega_page) {
  // Arrange -- nothing more than default CacheTest SetUp(); 4 MiB file + 32 KiB cache
  // Act -- read 1024 sequential 4-byte ints at stride 4096 (1 MiB page boundaries)
  for (int i = 0; i < 1024; ++i) {
    std::string data = cache_->ReadAt(static_cast<size_t>(i) * 1024 * sizeof(int),
                                      sizeof(int));
    int data_as_int = *(reinterpret_cast<int*>(data.data()));

    // Assert -- each read yields the deterministic Expected(i*1024) value
    ASSERT_EQ(data_as_int, Expected(static_cast<size_t>(i) * 1024));
  }
}

TEST_F(CacheTest, mega_pages) {
  // Arrange -- nothing more than default CacheTest SetUp(); 4 MiB file + 32 KiB cache
  // Act -- read 4 sequential 4-byte ints at stride 1 MiB (full page boundaries)
  for (int i = 0; i < 4; ++i) {
    std::string data =
        cache_->ReadAt(static_cast<size_t>(i) * 1024 * 1024 * sizeof(int),
                       sizeof(int));
    int data_as_int = *(reinterpret_cast<int*>(data.data()));

    // Assert -- each read yields the deterministic Expected(i*1024*1024) value
    ASSERT_EQ(data_as_int, Expected(static_cast<size_t>(i) * 1024 * 1024));
  }
}

TEST_F(CacheTest, read_at_with_locks) {
  // Arrange -- default 4 MiB file, lock-returning ReadAt overload
  // Act -- read via the Locks + string_view overload
  for (int i = 0; i < 1024; i += 32) {
    std::string_view out;
    {
      Cache::Locks locks = cache_->ReadAt(i * sizeof(int), sizeof(int), out);
      int data_as_int = *(reinterpret_cast<const int*>(out.data()));
      // Assert -- the locked view matches the deterministic file content
      ASSERT_EQ(data_as_int, Expected(i));
    }
  }
}

TEST_F(CacheTest, copy_unaligned_across_page_boundary) {
  // Arrange -- default 4 MiB file
  // Act -- read 8 bytes straddling the 4096-byte page boundary at offset 4092
  int first = 0;
  cache_->Copy(&first, 4092, sizeof(int));
  int second = 0;
  cache_->Copy(&second, 4096, sizeof(int));
  // Assert -- both ints decode to the expected deterministic values
  ASSERT_EQ(first, Expected(1023));
  ASSERT_EQ(second, Expected(1024));

  // Act -- read an unaligned 4-byte window that overlaps the boundary
  std::string raw = cache_->ReadAt(4094, 4);
  // Assert -- the window is the tail of Expected(1023) then the head of
  // Expected(1024) under little-endian byte order
  const int lo = Expected(1023);
  const int hi = Expected(1024);
  ASSERT_EQ(raw.size(), 4U);
  ASSERT_EQ(static_cast<unsigned char>(raw[0]),
            static_cast<unsigned char>((lo >> 16) & 0xff));
  ASSERT_EQ(static_cast<unsigned char>(raw[1]),
            static_cast<unsigned char>((lo >> 24) & 0xff));
  ASSERT_EQ(static_cast<unsigned char>(raw[2]),
            static_cast<unsigned char>(hi & 0xff));
  ASSERT_EQ(static_cast<unsigned char>(raw[3]),
            static_cast<unsigned char>((hi >> 8) & 0xff));
}

TEST_F(CacheTest, read_zero_length_and_boundary_bytes) {
  // Arrange -- default 4 MiB file
  // Act -- read zero bytes and a single byte at page boundaries
  std::string empty = cache_->ReadAt(4096, 0);
  ASSERT_TRUE(empty.empty());
  std::string first_byte = cache_->ReadAt(0, 1);
  std::string last_page_first_byte = cache_->ReadAt(4096, 1);
  // Assert -- both boundary reads return exactly one byte of file content
  ASSERT_EQ(first_byte.size(), 1U);
  ASSERT_EQ(last_page_first_byte.size(), 1U);
}

TEST_F(CacheTest, invalidate_then_reread) {
  // Arrange -- a cache large enough that re-reading an invalidated page does
  // not trip the small-queue eviction spin.  (With the default 32 KiB cache the
  // small queue holds exactly one page; after Invalidate() the evicted page is
  // still queued, so a re-read re-locks it while it is the queue front and
  // Cache::EnqueueToSmallFifo() spins forever in the kLocked arm.)
  Cache wide(fd_, size_t{256} * 1024);
  ASSERT_EQ(wide.ReadAt(0, sizeof(int)).size(), sizeof(int));

  // Act -- invalidate the first 4 KiB and read the same int again
  wide.Invalidate(0, size_t{4} * 1024);
  std::string data = wide.ReadAt(0, sizeof(int));

  // Assert -- the page is transparently reloaded from the file
  int data_as_int = *(reinterpret_cast<int*>(data.data()));
  ASSERT_EQ(data_as_int, Expected(0));
}

TEST_F(CacheTest, invalidate_fresh_pages_is_noop) {
  // Arrange -- default 4 MiB file; the first two pages were never touched
  // Act -- invalidate a never-cached range, then read through it
  cache_->Invalidate(0, size_t{4} * 1024);
  std::string data = cache_->ReadAt(0, sizeof(int));
  int data_as_int = *(reinterpret_cast<int*>(data.data()));
  ASSERT_EQ(data_as_int, Expected(0));

  // Act -- invalidate the (now cached) first page; the page is discarded
  cache_->Invalidate(0, size_t{4} * 1024);
  // (No further read of page 0: re-reading it with the 1-page small queue
  // spins forever -- see invalidate_then_reread.)
}

TEST_F(CacheTest, dump_and_stream_operator) {
  // Arrange -- cache a handful of pages, then dump internal queue state
  for (int i = 0; i < 8; ++i) {
    std::ignore = cache_->ReadAt(static_cast<size_t>(i) * 4096, sizeof(int));
  }
  std::string dump = cache_->Dump();
  ASSERT_FALSE(dump.empty());
  std::stringstream ss;
  ss << *cache_;
  // Assert -- the streamed dump is identical to Dump() (SanityCheck ran)
  ASSERT_EQ(ss.str(), dump);
}

TEST_F(CacheTest, explicit_max_size_constructor) {
  // Arrange -- construct a cache whose max_size is given explicitly instead of
  // being derived from the file size
  Cache capped(fd_, size_t{32} * 1024, size_t{2} * 1024 * 1024);
  // Act -- read ints spread across the capped address space
  for (int i = 0; i < 128; ++i) {
    std::string data =
        capped.ReadAt(static_cast<size_t>(i) * 1024 * sizeof(int), sizeof(int));
    int data_as_int = *(reinterpret_cast<int*>(data.data()));
    // Assert -- each read yields the deterministic Expected(i*1024) value
    ASSERT_EQ(data_as_int, Expected(static_cast<size_t>(i) * 1024));
  }
}

TEST_F(CacheTest, eviction_across_tiny_cache) {
  // Arrange -- a 2-page cache forces the small/ghost queues to evict on every
  // other distinct page touch
  Cache tiny(fd_, size_t{2} * 4096);
  // Act -- touch 16 distinct pages, far more than fit in the 2-page cache
  for (int i = 0; i < 16; ++i) {
    std::string data = tiny.ReadAt(static_cast<size_t>(i) * 4096, sizeof(int));
    int data_as_int = *(reinterpret_cast<int*>(data.data()));
    // Assert -- eviction never corrupts the read value
    ASSERT_EQ(data_as_int, Expected(static_cast<size_t>(i) * 1024));
  }
}

TEST_F(CacheTest, locked_read_spans_page_boundary) {
  // Arrange -- default 4 MiB file
  // Act -- read 20 bytes straddling the 4096-byte page boundary via the
  // lock-returning overload (fixes two pages at once)
  std::string_view out;
  {
    Cache::Locks locks = cache_->ReadAt(4090, 20, out);
    ASSERT_EQ(out.size(), 20U);
  }
}

TEST_F(CacheTest, locked_read_within_single_page) {
  // Arrange -- default 4 MiB file
  // Act -- read one int via the lock-returning overload within a single page
  std::string_view out;
  {
    Cache::Locks locks = cache_->ReadAt(1000, sizeof(int), out);
    ASSERT_EQ(out.size(), sizeof(int));
    int value = 0;
    ::memcpy(&value, out.data(), sizeof(int));
    ASSERT_EQ(value, Expected(250));
  }
}

TEST_F(CacheTest, reaccess_promotes_to_accessed_state) {
  // Arrange -- default 4 MiB file
  // Act -- read the same page repeatedly; the second FixPage sees kUnlocked
  // and promotes to kLockedAccessed, so UnfixPage demotes to kUnlockedAccessed
  for (int i = 0; i < 4; ++i) {
    std::string data = cache_->ReadAt(0, sizeof(int));
    ASSERT_EQ(*(reinterpret_cast<int*>(data.data())), Expected(0));
  }
  // Act -- stream the cache; SanityCheck runs over the (multi-access) queue
  std::stringstream ss;
  ss << *cache_;
  EXPECT_FALSE(ss.str().empty());
}

TEST_F(CacheTest, eviction_promotes_through_ghost_queue) {
  // Arrange -- 3-page cache: small_queue=1, main_queue=2, ghost_queue=2
  Cache tiny(fd_, size_t{3} * 4096);
  auto read_int = [&](size_t page) {
    std::string data = tiny.ReadAt(page * 4096, sizeof(int));
    return *(reinterpret_cast<int*>(data.data()));
  };
  // Act -- a sequence that pushes pages through small -> ghost -> main queues:
  //   page i evicts page i-1 to the ghost queue, and re-reading a ghost page
  //   walks the kMarked -> kLocked and kUnlocked -> kLockedAccessed paths
  for (int i = 0; i < 7; ++i) {
    ASSERT_EQ(read_int(i), Expected(static_cast<size_t>(i) * 1024));
  }
  // Assert -- re-reading pages that were promoted/re-evicted still returns the
  // same deterministic values (nothing corrupted by the queue transitions)
  ASSERT_EQ(read_int(0), Expected(0));
  ASSERT_EQ(read_int(2), Expected(size_t{2} * 1024));
  ASSERT_EQ(read_int(5), Expected(size_t{5} * 1024));
  // Act -- dump the multi-queue state; SanityCheck runs over all three queues
  std::string dump = tiny.Dump();
  EXPECT_FALSE(dump.empty());
}

TEST_F(CacheTest, copy_spanning_multiple_pages) {
  // Arrange -- default 4 MiB file
  // Act -- copy 9000 bytes starting at offset 3000, crossing two boundaries
  std::string dst(9000, '\0');
  cache_->Copy(dst.data(), 3000, 9000);
  // Assert -- the int at file offset 4096 lands at dst index 1096
  int first_page_two = 0;
  ::memcpy(&first_page_two, dst.data() + 1096, sizeof(int));
  ASSERT_EQ(first_page_two, Expected(1024));
  // Assert -- the final int (file offset 11996) is Expected(2999)
  int last = 0;
  ::memcpy(&last, dst.data() + 8996, sizeof(int));
  ASSERT_EQ(last, Expected(2999));
}

TEST_F(CacheTest, invalidate_spanning_pages) {
  // Arrange -- a cache large enough to hold all four pages at once
  Cache wide(fd_, size_t{256} * 1024);
  for (int i = 0; i < 4; ++i) {
    std::ignore = wide.ReadAt(static_cast<size_t>(i) * 4096, sizeof(int));
  }
  // Act -- invalidate a 2-page range; multiple page entries are discarded
  wide.Invalidate(0, size_t{2} * 4096);
  // Assert -- every page transparently reloads from the file afterwards
  for (int i = 0; i < 4; ++i) {
    std::string data = wide.ReadAt(static_cast<size_t>(i) * 4096, sizeof(int));
    ASSERT_EQ(*(reinterpret_cast<int*>(data.data())),
              Expected(static_cast<size_t>(i) * 1024));
  }
}

TEST_F(CacheTest, capped_max_size_limits_address_space) {
  // Arrange -- a cache whose max_size (2 pages) is smaller than the 4 MiB file
  Cache capped(fd_, size_t{16} * 1024, size_t{2} * 4096);
  // Act -- read the first int of each of the two mapped pages
  for (int i = 0; i < 2; ++i) {
    std::string data =
        capped.ReadAt(static_cast<size_t>(i) * 4096, sizeof(int));
    ASSERT_EQ(*(reinterpret_cast<int*>(data.data())), Expected(static_cast<size_t>(i) * 1024));
  }
  // Assert -- a single-byte read at the very end of the address space works
  std::string boundary = capped.ReadAt((2 * 4096) - 1, 1);
  ASSERT_EQ(boundary.size(), 1U);
}

// VMCacheImpl bounds-checks page indices; these regression tests stay enabled.
TEST_F(CacheTest, invalidate_beyond_max_size_indexes_meta_out_of_bounds) {
  // Arrange -- a BlobFile whose max_filesize (16 KiB = 4 pages) is smaller than
  // the total data appended to it.  Cache derives meta_ from max_size_:
  //   meta_.size() == max_size_/kBlockSize + 1 == 5  (valid indices 0..4)
  // but Cache::Invalidate() (cache.cpp:116) guards its loop with
  // `target < max_size_`, i.e. it compares a *page index* against a *byte
  // count* (16384), so every page index in [5, 16383] passes the guard and
  // reads past the end of meta_.
  const std::string path = "cache_test_blob-" + RandomString();
  constexpr size_t kMaxFileSize = size_t{4} * 4096;  // 16 KiB cache address space
  {
    BlobFile blob(path, size_t{256} * 1024, kMaxFileSize);

    // Act -- append 16 KiB, filling the file to exactly max_size_, then append
    // a second page.  The second Append() runs Cache::Invalidate(16384, 4096),
    // whose loop visits target = 4 then target = 5; InvalidatePage(5) touches
    // meta_[5], one past the 5-entry meta_ vector (heap-buffer-overflow).
    blob.Append(std::string(size_t{4} * 4096, 'a'));
    blob.Append(std::string(4096, 'b'));
    blob.Flush();
  }
  std::filesystem::remove(path);
}

TEST_F(CacheTest, locked_read_past_max_size_indexes_meta_out_of_bounds) {
  // Arrange -- a cache capped at 16 KiB (meta_.size() == max_size_/kBlockSize+1
  // == 5 page entries, valid indices 0..4).  Memory capacity is large so the
  // small queue never fills while the first pages are being fixed (a full
  // 1-page small queue would otherwise spin in EnqueueToSmallFifo on a kLocked
  // front element).  The file backing the cache is the 4 MiB SetUp file.
  Cache capped(fd_, size_t{1024} * 1024, size_t{4} * 4096);

  // Act -- request 20 KiB through the lock-returning overload.  Cache::ReadAt()
  // (cache.cpp:86) computes last_page == (offset + length) / kBlockSize == 5 and
  // FixPage()es every page in [0, 5]; pages 5 (and beyond) are past the 5-entry
  // meta_ vector, so FixPage(5) reads meta_[5] out of bounds.  Unlike Invalidate,
  // no guard is attempted at all here.
  std::string_view out;
  // (Crash happens inside ReadAt; the Locks are never returned.)
  Cache::Locks locks = capped.ReadAt(0, size_t{5} * 4096, out);
  (void)locks;
  (void)out;
}

TEST_F(CacheTest, UnalignedReadsFillGhostQueue) {
  // Arrange -- a 3-page cache (small=1, main=2, ghost=2).
  Cache tiny(fd_, size_t{3} * 4096);
  auto read_int = [&](size_t byte_offset) {
    std::string data = tiny.ReadAt(byte_offset, sizeof(int));
    int value = 0;
    ::memcpy(&value, data.data(), sizeof(int));
    return value;
  };

  // Act -- touch pages 0..2 once each at non-page-aligned offsets.  Unlike the
  // aligned reads used elsewhere (whose first chunk is zero-length and fixes
  // the page twice, marking it accessed), these reads fix each page exactly
  // once so the pages stay kUnlocked and evict through small -> ghost.
  ASSERT_EQ(read_int(8), Expected(2));
  ASSERT_EQ(read_int(4096 + 8), Expected(1026));
  ASSERT_EQ(read_int((2 * 4096) + 8), Expected(2050));

  // Assert -- pages 0 and 1 were demoted to the ghost queue.
  EXPECT_EQ(tiny.Dump(), "[2] {} [0, 1]");

  // Assert -- evicted pages transparently reload with the right data.
  ASSERT_EQ(read_int(8), Expected(2));
  ASSERT_EQ(read_int(4096 + 8), Expected(1026));
  // Re-reading kMarked ghost pages promotes them into the main queue.
  EXPECT_EQ(tiny.Dump(), "[2] {0, 1} []");
}

TEST_F(CacheTest, GhostOverflowEvictsMarkedEntry) {
  // Arrange -- a 3-page cache (small=1, main=2, ghost=2).
  Cache tiny(fd_, size_t{3} * 4096);
  auto read_int = [&](size_t byte_offset) {
    std::string data = tiny.ReadAt(byte_offset, sizeof(int));
    int value = 0;
    ::memcpy(&value, data.data(), sizeof(int));
    return value;
  };

  // Act -- touch pages 0..3 once each at non-aligned offsets.  Page 3's
  // enqueue overflows the ghost FIFO, dropping the kMarked page 0 back to
  // kEvicted.
  for (size_t page = 0; page < 4; ++page) {
    ASSERT_EQ(read_int((page * 4096) + 8), Expected((page * 1024) + 2));
  }

  // Assert -- the ghost FIFO kept [1, 2] and page 0 was fully evicted.
  EXPECT_EQ(tiny.Dump(), "[3] {} [1, 2]");

  // Assert -- the evicted page 0 transparently reloads from the file.
  ASSERT_EQ(read_int(8), Expected(2));
  ASSERT_EQ(read_int((1 * 4096) + 8), Expected(1026));
}

TEST_F(CacheTest, RevivedGhostPageMovesToMainQueueOnOverflow) {
  // Arrange -- a 3-page cache (small=1, main=2, ghost=2).  VMCacheImpl FixPage
  // on a kMarked (ghost) page removes it from the ghost FIFO and enqueues it
  // to the main queue immediately.
  Cache tiny(fd_, size_t{3} * 4096);
  auto read_int = [&](size_t byte_offset) {
    std::string data = tiny.ReadAt(byte_offset, sizeof(int));
    int value = 0;
    ::memcpy(&value, data.data(), sizeof(int));
    return value;
  };

  // Act -- pages 0..2 churn into the ghost FIFO, then page 0 is revived while
  // still marked in the ghost queue (kMarked -> kLocked -> kUnlocked -> main).
  read_int(8);
  read_int(4096 + 8);
  read_int((2 * 4096) + 8);
  read_int(8);
  ASSERT_EQ(read_int(8), Expected(2));
  EXPECT_EQ(tiny.Dump(), "[2] {0} [1]");

  // Act -- page 3's enqueue overflows the ghost FIFO; the revived page 0 is
  // already in the main queue.
  read_int((3 * 4096) + 8);

  // Assert -- page 0 lives in the main queue; page 1 remains in ghost.
  EXPECT_EQ(tiny.Dump(), "[3] {0} [1, 2]");
  ASSERT_EQ(read_int(8), Expected(2));
  ASSERT_EQ(read_int((3 * 4096) + 8), Expected((3 * 1024) + 2));
}

TEST_F(CacheTest, AccessedGhostPageMovesToMainQueueOnOverflow) {
  // Arrange -- a 3-page cache.  Re-reading a revived ghost page a second time
  // leaves it kUnlockedAccessed inside the ghost FIFO.
  Cache tiny(fd_, size_t{3} * 4096);
  auto read_int = [&](size_t byte_offset) {
    std::string data = tiny.ReadAt(byte_offset, sizeof(int));
    int value = 0;
    ::memcpy(&value, data.data(), sizeof(int));
    return value;
  };

  // Act -- fill the ghost FIFO, revive page 0 twice (kUnlockedAccessed), then
  // overflow the FIFO so the accessed ghost page promotes into the main queue.
  read_int(8);
  read_int(4096 + 8);
  read_int((2 * 4096) + 8);
  read_int(8);
  read_int(8);
  ASSERT_EQ(read_int(8), Expected(2));
  read_int((3 * 4096) + 8);

  // Assert -- the accessed ghost page 0 moved to the main queue.
  EXPECT_EQ(tiny.Dump(), "[3] {0} [1, 2]");
  ASSERT_EQ(read_int(8), Expected(2));
}

TEST_F(CacheTest, MultiEntryDumpCoversAllThreeQueues) {
  // Arrange -- a 20-page cache (small=2, main=18, ghost=18).
  Cache wide(fd_, size_t{20} * 4096);
  auto read_int = [&](size_t byte_offset) {
    std::string data = wide.ReadAt(byte_offset, sizeof(int));
    int value = 0;
    ::memcpy(&value, data.data(), sizeof(int));
    return value;
  };

  // Act -- re-read pages promote to the main queue; single-touch non-aligned
  // pages churn through the small -> ghost FIFO chain.  This leaves every
  // queue holding multiple entries so the Dump comma separators are emitted.
  read_int(8);              // page 0
  read_int(4096 + 8);       // page 1
  read_int(8);              // page 0 (accessed)
  read_int((2 * 4096) + 8);   // page 2 -> page 0 to main
  read_int(4096 + 8);       // page 1 (accessed)
  read_int((3 * 4096) + 8);   // page 3 -> page 1 to main
  read_int((2 * 4096) + 8);   // page 2 (accessed)
  read_int((4 * 4096) + 8);   // page 4 -> page 2 to main
  read_int((3 * 4096) + 8);   // page 3 (accessed)
  read_int((5 * 4096) + 8);   // page 5 -> page 3 to main
  read_int((6 * 4096) + 8);   // page 6 -> page 4 to ghost
  read_int((7 * 4096) + 8);   // page 7 -> page 5 to ghost
  read_int((8 * 4096) + 8);   // page 8 -> page 6 to ghost

  // Assert -- small, main and ghost each hold multiple entries.
  EXPECT_EQ(wide.Dump(), "[7, 8] {0, 1, 2, 3} [4, 5, 6]");

  // Assert -- data stays intact after the churn.
  ASSERT_EQ(read_int(8), Expected(2));
  ASSERT_EQ(read_int((8 * 4096) + 8), Expected((8 * 1024) + 2));
}

TEST_F(CacheTest, LockedReadEarlyReturnsForOutOfRangeAndEmpty) {
  // Arrange -- the fixture's 16 MiB file; max_size_ is the whole file.
  const size_t kMaxSize = 4L * 1024L * 1024L * sizeof(int);

  // Act -- lock-returning read at and beyond max_size_, and zero length.
  std::string_view out;
  {
    Cache::Locks locks = cache_->ReadAt(kMaxSize, sizeof(int), out);
    ASSERT_TRUE(locks.empty());
    ASSERT_EQ(out.size(), 0U);
  }
  {
    Cache::Locks locks = cache_->ReadAt(0, 0, out);
    ASSERT_TRUE(locks.empty());
    ASSERT_EQ(out.size(), 0U);
  }

  // Act -- a lock-returning read clamped to the very end of the address space.
  {
    Cache::Locks locks = cache_->ReadAt(kMaxSize - 2, 4, out);
    ASSERT_EQ(out.size(), 2U);
    ASSERT_EQ(locks.size(), 1U);
  }
}

TEST_F(CacheTest, ZeroCapacityConstructorThrows) {
  // A zero-byte memory budget is rejected by the constructor: the cache would
  // be unusable, so it throws instead of completing.
  EXPECT_THROW(Cache zero(fd_, 0), std::exception);
}

TEST(CacheInvalidFd, ConstructorLogsFileSizeFailure) {
  // Invalid fd makes FileSize() throw before meta_/mmap, so the constructor
  // fails without expanding max_size_ to SIZE_MAX.
  try {
    Cache bad(-1, 4096);
    FAIL() << "Cache constructor should throw after FileSize() failure";
  } catch (const std::exception&) {
    SUCCEED();
  }
}

TEST_F(CacheTest, InvalidateZeroLengthIsNoop) {
  // Act -- invalidate zero-length ranges at the start of the file.
  cache_->Invalidate(0, 0);
  cache_->Invalidate(4096, 0);

  // Assert -- the (never-cached) first page is untouched and reads correctly.
  std::string data = cache_->ReadAt(0, sizeof(int));
  ASSERT_EQ(data.size(), sizeof(int));
  ASSERT_EQ(*(reinterpret_cast<int*>(data.data())), Expected(0));
}

TEST_F(CacheTest, LockedReadSpansPageWithCorrectContent) {
  // Arrange -- a cache large enough that fixing page 1 cannot evict page 0
  // within the same ReadAt call (the lock-returning overload only pins pages
  // weakly, so a small cache zeroes earlier pages -- see
  // UnalignedLockedReadAcrossPageBoundary).
  Cache wide(fd_, size_t{1024} * 1024);

  // Act -- read 16 bytes starting exactly at the page boundary.
  std::string_view out;
  {
    Cache::Locks locks = wide.ReadAt(4092, 16, out);

    // Assert -- two pages are fixed and the window decodes to four ints.
    ASSERT_EQ(locks.size(), 2U);
    ASSERT_EQ(out.size(), 16U);
    for (int i = 0; i < 4; ++i) {
      int value = 0;
      ::memcpy(&value, out.data() + (i * sizeof(int)), sizeof(int));
      ASSERT_EQ(value, Expected(1023 + i));
    }
  }
}

// This test intentionally FAILS against the current production code and
// documents a real bug in Cache::ReadAt(size_t, size_t, std::string_view&).
//
// The lock-returning overload fixes every page in [first_page, last_page] and
// then hands back a std::string_view into the mmap buffer.  The returned Locks
// were meant to pin those pages, but Cache::Lock objects are released the
// moment push_back() finishes (the temporary's destructor stores kUnlocked),
// so nothing actually pins them.  On a small cache the FixPage of the second
// page evicts the freshly-fixed first page (kUnlocked -> kMarked -> Release(),
// which madvise(MADV_DONTNEED)es its buffer), and the string_view over that
// now-discarded page reads as zeroes.
//
// The default 8-page fixture cache has a 1-page small queue, so reading the
// two-byte tail of page 0 together with the head of page 1 loses bytes 0..1.
TEST_F(CacheTest, UnalignedLockedReadAcrossPageBoundary) {
  // Act -- read 6 bytes starting 2 bytes into the last int of page 0, so the
  // window crosses the boundary without being int-aligned.
  std::string_view out;
  {
    Cache::Locks locks = cache_->ReadAt(4094, 6, out);
    ASSERT_EQ(locks.size(), 2U);
    ASSERT_EQ(out.size(), 6U);
    const int lo = Expected(1023);
    const int hi = Expected(1024);
    // Assert -- bytes 0..1 are the high half of Expected(1023), bytes 2..5 are
    // the whole of Expected(1024) under little-endian byte order.  The first
    // two bytes come from page 0, which the FixPage of page 1 just evicted.
    ASSERT_EQ(static_cast<unsigned char>(out[0]),
              static_cast<unsigned char>((lo >> 16) & 0xff));
    ASSERT_EQ(static_cast<unsigned char>(out[1]),
              static_cast<unsigned char>((lo >> 24) & 0xff));
    ASSERT_EQ(static_cast<unsigned char>(out[2]),
              static_cast<unsigned char>(hi & 0xff));
    ASSERT_EQ(static_cast<unsigned char>(out[3]),
              static_cast<unsigned char>((hi >> 8) & 0xff));
    ASSERT_EQ(static_cast<unsigned char>(out[4]),
              static_cast<unsigned char>((hi >> 16) & 0xff));
    ASSERT_EQ(static_cast<unsigned char>(out[5]),
              static_cast<unsigned char>((hi >> 24) & 0xff));
  }
}

TEST_F(CacheTest, AccessedMainQueueOverflowReenqueuesFront) {
  // Arrange -- a 3-page cache (small=1, main=2, ghost=2).  Revived ghost
  // pages move into the main queue on FixPage; overflow then promotes accessed
  // ghost pages and demotes the main-queue front.
  Cache tiny(fd_, size_t{3} * 4096);
  auto read_int = [&](size_t byte_offset) {
    std::string data = tiny.ReadAt(byte_offset, sizeof(int));
    int value = 0;
    ::memcpy(&value, data.data(), sizeof(int));
    return value;
  };

  // Act -- churn pages 0 and 1 into the small -> ghost FIFO chain, then make
  // page 0 kUnlockedAccessed by re-reading it inside the ghost FIFO.
  read_int(8);
  read_int(4096 + 8);
  read_int(8);
  read_int(8);
  EXPECT_EQ(tiny.Dump(), "[1] {0} []");

  // Page 2's enqueue pushes page 1 into the (now full) ghost FIFO.
  read_int((2 * 4096) + 8);
  EXPECT_EQ(tiny.Dump(), "[2] {0} [1]");

  // Page 3's enqueue overflows the ghost FIFO: the accessed page 0 is already
  // in the main queue, staying kUnlockedAccessed.
  read_int(4096 + 8);
  read_int(4096 + 8);
  read_int((3 * 4096) + 8);
  EXPECT_EQ(tiny.Dump(), "[3] {0, 1} [2]");

  // Page 4's enqueue overflows the ghost FIFO: the accessed page 1 joins page 0
  // in the now-full main queue.
  read_int((2 * 4096) + 8);
  read_int((2 * 4096) + 8);
  read_int((4 * 4096) + 8);
  EXPECT_EQ(tiny.Dump(), "[4] {1, 2} [3]");

  // Page 5's enqueue overflows the ghost FIFO, pushing the accessed page 2 into
  // the full main queue: the front (accessed page 1) is demoted and re-enqueued
  // while an eviction makes room.
  read_int((3 * 4096) + 8);
  read_int((3 * 4096) + 8);
  read_int((5 * 4096) + 8);
  EXPECT_EQ(tiny.Dump(), "[5] {2, 3} [4]");

  // Assert -- every touched page still returns deterministic data.
  ASSERT_EQ(read_int(8), Expected(2));
  ASSERT_EQ(read_int(4096 + 8), Expected(1026));
  ASSERT_EQ(read_int((4 * 4096) + 8), Expected((4 * 1024) + 2));
  ASSERT_EQ(read_int((5 * 4096) + 8), Expected((5 * 1024) + 2));
}

TEST_F(CacheTest, ReadingPastFileEndInMaxSizeLargerCacheReturnsZeroes) {
  // Arrange -- max_size (32 MiB) is larger than the 16 MiB fixture file, so
  // page 4096 lies entirely past the end of the real file.
  Cache beyond(fd_, size_t{1024} * 1024, 32L * 1024 * 1024);

  // Act -- read 4 bytes at the real file end (offset 16 MiB).
  std::string z = beyond.ReadAt(16L * 1024 * 1024, 4);

  // Assert -- Activate()'s EOF short-read path leaves the anonymous buffer
  // zeroed rather than crashing or returning garbage.
  ASSERT_EQ(z.size(), 4U);
  for (char c : z) {
    ASSERT_EQ(static_cast<unsigned char>(c), 0U);
  }

  // Act -- read a 4-byte window straddling the real EOF.
  std::string straddle = beyond.ReadAt((16L * 1024 * 1024) - 2, 4);

  // Assert -- the last two bytes (past the file end) read as zeroes.
  ASSERT_EQ(straddle.size(), 4U);
  ASSERT_EQ(static_cast<unsigned char>(straddle[2]), 0U);
  ASSERT_EQ(static_cast<unsigned char>(straddle[3]), 0U);
}

// A 24-page budget gives small_queue_ capacity 3.  Locking pages 0 and 1 puts
// two kLocked entries at the head of the small FIFO; appending unlocked pages 2
// and 3 then overflows the FIFO.  The two pinned fronts must be rotated to the
// back (Cache::EnqueueToSmallFifo's kLocked arm re-scans) and the kUnlocked
// page 2 is evicted to the ghost queue instead.
TEST_F(CacheTest, SmallFifoRotatesPinnedPagesThenEvictsUnlockedFront) {
  // Arrange -- a cache whose small FIFO holds exactly three entries.
  Cache wide(fd_, size_t{24} * 4096);
  auto read_int = [&](size_t byte_offset) {
    std::string data = wide.ReadAt(byte_offset, sizeof(int));
    return *(reinterpret_cast<const int*>(data.data()));
  };

  // Act -- pin pages 0 and 1 with the lock-returning overload, then touch
  // unlocked pages 2 and 3.
  std::string_view out;
  {
    Cache::Locks locks0 = wide.ReadAt(0, 8, out);
    Cache::Locks locks1 = wide.ReadAt(4096, 8, out);
    ASSERT_EQ(wide.Dump(), "[0, 1] {} []");
    read_int((2 * 4096) + 8);
    read_int((3 * 4096) + 8);
  }

  // Assert -- the pinned pages rotated to the back; page 2 was evicted.
  EXPECT_EQ(wide.Dump(), "[0, 1, 3] {} [2]");

  // Assert -- every page still returns the deterministic file content.
  for (size_t page = 0; page < 4; ++page) {
    ASSERT_EQ(read_int((page * 4096) + 8), Expected((page * 1024) + 2));
  }
}

// A re-fixed page inside the small FIFO is kLockedAccessed while a Lock holds
// it.  When the FIFO overflows, that pinned accessed front is demoted to
// kLocked and promoted into the main queue (the kLockedAccessed arm of
// EnqueueToSmallFifo) rather than being evicted.
TEST_F(CacheTest, SmallFifoPromotesPinnedAccessedPageToMainQueue) {
  // Arrange -- a 3-page budget (small=1, main=2, ghost=2).
  Cache tiny(fd_, size_t{3} * 4096);
  auto read_int = [&](size_t byte_offset) {
    std::string data = tiny.ReadAt(byte_offset, sizeof(int));
    return *(reinterpret_cast<const int*>(data.data()));
  };

  // Act -- page 0 is accessed twice, then pinned as kLockedAccessed; page 1's
  // enqueue overflows the small FIFO.
  read_int(0);
  read_int(0);
  ASSERT_EQ(tiny.Dump(), "[0] {} []");
  std::string_view out;
  {
    Cache::Locks locks = tiny.ReadAt(0, sizeof(int), out);
    read_int(4096 + 8);
  }

  // Assert -- the pinned accessed page moved to the main queue.
  EXPECT_EQ(tiny.Dump(), "[1] {0} []");
  ASSERT_EQ(read_int(8), Expected(2));
  ASSERT_EQ(read_int(4096 + 8), Expected(1026));
}

// A pinned page resident in the MAIN queue is kLockedAccessed.  When the main
// FIFO overflows, that pinned accessed front is demoted to kLocked and rotated
// to the back, while a kUnlocked sibling is evicted (the kLockedAccessed arm of
// EnqueueToMainFifo).
TEST_F(CacheTest, MainFifoDemotesPinnedAccessedFrontThenEvictsUnlocked) {
  // Arrange -- a 3-page budget.
  Cache tiny(fd_, size_t{3} * 4096);
  auto read_int = [&](size_t byte_offset) {
    std::string data = tiny.ReadAt(byte_offset, sizeof(int));
    return *(reinterpret_cast<const int*>(data.data()));
  };

  // Act -- build a main queue holding page 0 (kUnlocked), then pin it so it
  // becomes kLockedAccessed; overflow the main FIFO with an accessed page.
  read_int(0);
  read_int(0);
  read_int(4096 + 8);
  read_int((2 * 4096) + 8);
  read_int((2 * 4096) + 8);
  ASSERT_EQ(tiny.Dump(), "[2] {0} [1]");
  std::string_view out;
  {
    Cache::Locks locks = tiny.ReadAt(0, sizeof(int), out);
    read_int((3 * 4096) + 8);
    read_int((3 * 4096) + 8);
    read_int((4 * 4096) + 8);
  }

  // Assert -- the pinned page 0 rotated within main, page 2 was evicted.
  EXPECT_EQ(tiny.Dump(), "[4] {0, 3} [1]");
  ASSERT_EQ(read_int(0), Expected(0));
  ASSERT_EQ(read_int((4 * 4096) + 8), Expected((4 * 1024) + 2));
}

// Two ghost pages revived while pinned (kMarked -> kLocked, still queued in the
// ghost FIFO) are promoted into the main queue by a ghost overflow.  Releasing
// one Lock leaves page 1 kUnlocked while page 0 stays kLocked; the next main
// overflow rotates the pinned page 0 to the back and evicts the kUnlocked
// sibling (the kLocked arm of EnqueueToMainFifo).
TEST_F(CacheTest, MainFifoRotatesPinnedPageThenEvictsUnlockedSibling) {
  // Arrange -- a 3-page budget.
  Cache tiny(fd_, size_t{3} * 4096);
  auto read_int = [&](size_t byte_offset) {
    std::string data = tiny.ReadAt(byte_offset, sizeof(int));
    return *(reinterpret_cast<const int*>(data.data()));
  };

  // Act -- push pages 0 and 1 into the ghost FIFO, revive both while pinned,
  // then promote them to the main queue.
  read_int(8);
  read_int(4096 + 8);
  read_int((2 * 4096) + 8);
  ASSERT_EQ(tiny.Dump(), "[2] {} [0, 1]");
  std::string_view out;
  {
    Cache::Locks locks0 = tiny.ReadAt(0, sizeof(int), out);
    {
      std::string_view out2;
      Cache::Locks locks1 = tiny.ReadAt(4096, sizeof(int), out2);
      read_int((3 * 4096) + 8);
      read_int((4 * 4096) + 8);
      ASSERT_EQ(tiny.Dump(), "[4] {0, 1} [2, 3]");
    }
    // locks1 destroyed: page 1 is now kUnlocked, page 0 still kLocked.
    read_int((4 * 4096) + 8);
    read_int((5 * 4096) + 8);

    // Assert -- the pinned page 0 rotated; the unlocked page 1 was evicted.
    EXPECT_EQ(tiny.Dump(), "[5] {0, 4} [2, 3]");
  }
  // locks0 destroyed: page 0 is now kUnlocked and readable again.
  ASSERT_EQ(read_int(8), Expected(2));
  ASSERT_EQ(read_int((5 * 4096) + 8), Expected((5 * 1024) + 2));
}

// When every page in the full main FIFO is pinned (kLocked), the rotation scan
// in EnqueueToMainFifo exhausts the queue and gives up, allowing a temporary
// overflow rather than spinning forever.
TEST_F(CacheTest, MainFifoAllPagesPinnedAllowsTemporaryOverflow) {
  // Arrange -- a 3-page budget.
  Cache tiny(fd_, size_t{3} * 4096);
  auto read_int = [&](size_t byte_offset) {
    std::string data = tiny.ReadAt(byte_offset, sizeof(int));
    return *(reinterpret_cast<const int*>(data.data()));
  };

  // Act -- fill the ghost FIFO, revive pages 0 and 1 pinned (kLocked), promote
  // both into the main queue, then overflow main while both fronts are pinned.
  read_int(8);
  read_int(4096 + 8);
  read_int((2 * 4096) + 8);
  ASSERT_EQ(tiny.Dump(), "[2] {} [0, 1]");
  std::string_view out;
  {
    Cache::Locks locks0 = tiny.ReadAt(0, sizeof(int), out);
    std::string_view out2;
    Cache::Locks locks1 = tiny.ReadAt(4096, sizeof(int), out2);
    read_int((3 * 4096) + 8);
    read_int((4 * 4096) + 8);
    ASSERT_EQ(tiny.Dump(), "[4] {0, 1} [2, 3]");
    read_int((4 * 4096) + 8);
    read_int((5 * 4096) + 8);
  }

  // Assert -- the main FIFO was allowed to grow to three entries.
  EXPECT_EQ(tiny.Dump(), "[5] {0, 1, 4} [2, 3]");
  ASSERT_EQ(read_int(8), Expected(2));
  ASSERT_EQ(read_int((5 * 4096) + 8), Expected((5 * 1024) + 2));
}

// A single ReadAt spanning many pages on a two-page cache forces continuous
// small -> ghost evictions from inside the Copy loop without pinning any page,
// so every FixPage/UnfixPage pair stays balanced.
TEST_F(CacheTest, ReadLargeSpanningBufferOnTinyCache) {
  // Arrange -- a 2-page budget.
  Cache tiny(fd_, size_t{2} * 4096);

  // Act -- read 64 KiB in one call; the read crosses 16 page boundaries.
  std::string data = tiny.ReadAt(0, size_t{64} * 1024);

  // Assert -- every int across the span is intact.
  ASSERT_EQ(data.size(), 64U * 1024U);
  ASSERT_EQ(*(reinterpret_cast<const int*>(data.data())), Expected(0));
  ASSERT_EQ(*(reinterpret_cast<const int*>(data.data() + data.size() - 4)),
            Expected(16383));
  EXPECT_EQ(tiny.Dump().front(), '[');
}

}  // namespace tinylamb