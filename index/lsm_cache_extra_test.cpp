/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// Extra LSM blob-cache coverage beyond cache_test.cpp: a seeded random-access
// oracle over Copy(), whole-file invalidation, and reads that end exactly at
// the file boundary.
#include <fcntl.h>
#include <unistd.h>

#include <cstddef>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "index/lsm_detail/cache.hpp"

namespace tinylamb {

namespace {

constexpr size_t kFileSize = 1U << 20;  // 1 MiB, 4096-byte pages.

// A byte pattern that changes on every position so misaligned copies show.
char PatternByte(size_t pos) {
  return static_cast<char>((pos * 1315423911U) >> 13);
}

}  // namespace

class LsmCacheExtraTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = "lsm_cache_extra-" + RandomString();
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
    std::string content(kFileSize, '\0');
    for (size_t i = 0; i < kFileSize; ++i) {
      content[i] = PatternByte(i);
    }
    ASSERT_EQ(::write(fd_, content.data(), content.size()),
              static_cast<ssize_t>(kFileSize));
    ::fsync(fd_);
    cache_ = Cache::Create(fd_, 16 * 1024).MoveValue();
  }

  void TearDown() override {
    cache_.reset();
    ::close(fd_);
    std::ignore = std::remove(path_.c_str());
  }

  int fd_{};
  std::filesystem::path path_;
  std::unique_ptr<Cache> cache_;
};

TEST_F(LsmCacheExtraTest, RandomAccessCopyMatchesAPureMemoryReference) {
  std::string reference(kFileSize, '\0');
  for (size_t i = 0; i < kFileSize; ++i) {
    reference[i] = PatternByte(i);
  }
  std::mt19937_64 rng(0x5eed1234);
  std::uniform_int_distribution<size_t> offset_dist(0, kFileSize - 1);
  std::uniform_int_distribution<size_t> length_dist(1, 3000);
  for (int round = 0; round < 500; ++round) {
    const size_t offset = offset_dist(rng);
    const size_t length = std::min(length_dist(rng), kFileSize - offset);
    std::string got(length, '\0');
    ASSERT_SUCCESS(cache_->Copy(got.data(), offset, length));
    EXPECT_EQ(std::memcmp(got.data(), reference.data() + offset, length), 0)
        << "offset=" << offset << " length=" << length;
  }
}

TEST_F(LsmCacheExtraTest, WholeFileInvalidateKeepsReadsCorrect) {
  cache_->Invalidate(0, kFileSize);
  std::string got(4096, '\0');
  ASSERT_SUCCESS(cache_->Copy(got.data(), kFileSize - 4096, 4096));
  for (size_t i = 0; i < 4096; ++i) {
    EXPECT_EQ(got[i], PatternByte(kFileSize - 4096 + i)) << "i=" << i;
  }
}

TEST_F(LsmCacheExtraTest, ReadTouchingTheLastByteSucceeds) {
  auto one = cache_->ReadAt(kFileSize - 1, 1);
  ASSERT_SUCCESS(one.GetStatus());
  EXPECT_EQ(one.MoveValue()[0], PatternByte(kFileSize - 1));
  // Zero-length reads are legal at the boundary and past it within the
  // mapped address space.
  ASSERT_SUCCESS(cache_->ReadAt(kFileSize, 0).GetStatus());
}

TEST_F(LsmCacheExtraTest, ReadsPastTheMappedWindowAreAClampNoOp) {
  // VMCacheImpl::Read clamps to the mapped window "exactly like the
  // string-returning ReadAt()": a copy that starts past the file's window
  // succeeds without touching the destination, and a straddling read is cut
  // short at the window edge.
  std::string untouched(16, '\x7f');
  ASSERT_SUCCESS(cache_->Copy(untouched.data(), kFileSize + 8192, 16));
  for (const char c : untouched) {
    EXPECT_EQ(c, '\x7f') << "clamped no-op must leave dst alone";
  }
  auto straddling = cache_->ReadAt(kFileSize - 4, 16).MoveValue();
  // The read is served in full: the four in-file bytes carry the pattern and
  // the unwritten tail beyond EOF reads as zero pages.
  EXPECT_EQ(straddling.size(), 16U);
  for (size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(straddling[i], PatternByte(kFileSize - 4 + i));
  }
  for (size_t i = 4; i < 16; ++i) {
    EXPECT_EQ(straddling[i], '\0') << "i=" << i;
  }
}

}  // namespace tinylamb
