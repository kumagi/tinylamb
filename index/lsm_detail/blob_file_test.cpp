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

#include "index/lsm_detail/blob_file.hpp"

#include <algorithm>
#include <filesystem>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "index/lsm_detail/cache.hpp"
#include "recovery/logger.hpp"

namespace tinylamb {
class BlobFileTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = "tmp_blob_file_test-" + RandomString();
    blob_ = BlobFile::Create(path_).MoveValue();
  }

  void TearDown() override {
    // Drain the writer explicitly instead of sleeping and hoping.
    if (blob_) {
      (void)blob_->Flush();
    }
    blob_.reset();
    std::filesystem::remove_all(path_);
  }

  std::unique_ptr<BlobFile> blob_;
  std::string path_;
};

TEST_F(BlobFileTest, ReadAt) {
  std::map<std::string, std::string> tree;
  tree.emplace("foo", "barr");
  tree.emplace("value", "notice");
  for (const auto& it : tree) {
    ASSERT_TRUE(blob_->Append(it.first).HasValue());
    ASSERT_TRUE(blob_->Append(it.second).HasValue());
  }

  ASSERT_EQ(blob_->ReadAt(0, 3).Value(), "foo");
  ASSERT_EQ(blob_->ReadAt(3, 4).Value(), "barr");
  ASSERT_EQ(blob_->ReadAt(7, 5).Value(), "value");
  ASSERT_EQ(blob_->ReadAt(12, 6).Value(), "notice");
}

namespace {
std::string EncodeRecord(std::string_view payload) {
  std::string record(4, '\0');
  record[0] = static_cast<char>((payload.size() >> 24U) & 0xffU);
  record[1] = static_cast<char>((payload.size() >> 16U) & 0xffU);
  record[2] = static_cast<char>((payload.size() >> 8U) & 0xffU);
  record[3] = static_cast<char>(payload.size() & 0xffU);
  record += payload;
  return record;
}
}  // namespace

TEST_F(BlobFileTest, ReadAtWithLengthPrefix) {
  ASSERT_TRUE(blob_->Append(EncodeRecord("hello")).HasValue());
  ASSERT_TRUE(blob_->Append(EncodeRecord("world of blob")).HasValue());

  std::string_view out;
  {
    Cache::Locks locks = blob_->ReadAt(0, out).MoveValue();
    ASSERT_EQ(std::string(out), "hello");
  }
  {
    Cache::Locks locks = blob_->ReadAt(4 + 5, out).MoveValue();
    ASSERT_EQ(std::string(out), "world of blob");
  }
}

TEST_F(BlobFileTest, AppendWrittenAndFlush) {
  // Arrange -- a fresh BlobFile with no writes yet
  ASSERT_EQ(blob_->Written(), 0);

  // Act -- append three payloads
  const lsn_t first = blob_->Append("first").Value();
  const lsn_t second = blob_->Append("second").Value();
  const lsn_t third = blob_->Append("third").Value();

  // Assert -- offsets are sequential and Written() advances past them
  ASSERT_EQ(first, 0);
  ASSERT_EQ(second, static_cast<lsn_t>(first + 5));
  ASSERT_EQ(third, static_cast<lsn_t>(second + 6));

  // Act -- wait for the writer to flush, then read the appended bytes
  (void)blob_->Flush();
  ASSERT_GE(blob_->Written(), static_cast<lsn_t>(third + 5));

  // Assert -- each appended payload reads back verbatim
  ASSERT_EQ(blob_->ReadAt(first, 5).Value(), "first");
  ASSERT_EQ(blob_->ReadAt(second, 6).Value(), "second");
  ASSERT_EQ(blob_->ReadAt(third, 5).Value(), "third");
}

TEST_F(BlobFileTest, StreamOperator) {
  // Arrange -- a BlobFile with a couple of writes
  blob_->Append("stream");
  (void)blob_->Flush();

  // Act -- stream the file
  std::stringstream ss;
  ss << *blob_;

  // Assert -- the dump exposes the written byte count and cache state
  ASSERT_NE(ss.str().find("BlobFile(written="), std::string::npos);
  ASSERT_NE(ss.str().find("cache="), std::string::npos);
}

TEST_F(BlobFileTest, LargeValueSurvivesReopen) {
  const std::string value((size_t{16} << 20) + 123, 'v');
  const lsn_t offset = blob_->Append(value).Value();
  ASSERT_EQ(blob_->Written(), offset + value.size());
  ASSERT_SUCCESS(blob_->Sync());
  ASSERT_EQ(blob_->ReadAt(offset, value.size()).Value(), value);

  blob_.reset();
  auto reopened = BlobFile::Create(path_, size_t{32} << 20).MoveValue();
  ASSERT_EQ(reopened->Written(), offset + value.size());
  ASSERT_EQ(reopened->ReadAt(offset, value.size()).Value(), value);
}

TEST_F(BlobFileTest, MigratesLegacySegments) {
  blob_.reset();
  constexpr size_t kSegmentSize = 64;
  std::vector<std::pair<lsn_t, std::string>> expected;
  lsn_t expected_end = 0;
  {
    auto legacy = Logger::Create(path_, 64, 1, kSegmentSize).MoveValue();
    for (int i = 0; i < 20; ++i) {
      const std::string payload(static_cast<size_t>(3 + ((i * 7) % 23)),
                                static_cast<char>('a' + i));
      const lsn_t offset = legacy->AddLog(payload).Value();
      expected.emplace_back(offset, payload);
      expected_end = std::max(expected_end, offset + payload.size());
    }
    ASSERT_SUCCESS(legacy->Finish());
  }

  auto migrated = BlobFile::Create(path_).MoveValue();
  ASSERT_EQ(migrated->Written(), expected_end);
  for (const auto& [offset, payload] : expected) {
    ASSERT_EQ(migrated->ReadAt(offset, payload.size()).Value(), payload);
  }
  bool any = false;
  auto highest = Logger::FindHighestSegment(path_, &any);
  ASSERT_TRUE(highest.HasValue());
  EXPECT_TRUE(any);
  EXPECT_EQ(highest.Value(), 0U);
  EXPECT_FALSE(std::filesystem::exists(Logger::SegmentMetadataPath(path_)));
  EXPECT_FALSE(std::filesystem::exists(path_ + ".migrating"));
  EXPECT_FALSE(std::filesystem::exists(path_ + ".migrating.tmp"));
}

TEST_F(BlobFileTest, MissingLegacySegmentFailsClosed) {
  blob_.reset();
  constexpr size_t kSegmentSize = 64;
  {
    auto legacy = Logger::Create(path_, 64, 1, kSegmentSize).MoveValue();
    for (int i = 0; i < 8; ++i) {
      ASSERT_TRUE(legacy->AddLog(std::string(32, static_cast<char>('a' + i)))
                      .HasValue());
    }
    ASSERT_SUCCESS(legacy->Finish());
  }
  ASSERT_TRUE(std::filesystem::remove(Logger::SegmentPath(path_, 1)));
  EXPECT_FALSE(BlobFile::Create(path_).HasValue());
  for (uint64_t index = 1; index < 8; ++index) {
    std::filesystem::remove(Logger::SegmentPath(path_, index));
  }
  std::filesystem::remove(Logger::SegmentMetadataPath(path_));
}

// A segment that is shorter than its extent must fail the migration's
// ValidateSeries pass instead of silently copying a truncated image.
TEST_F(BlobFileTest, ShortLegacySegmentFailsClosed) {
  blob_.reset();
  constexpr size_t kSegmentSize = 64;
  {
    auto legacy = Logger::Create(path_, 64, 1, kSegmentSize).MoveValue();
    for (int i = 0; i < 8; ++i) {
      ASSERT_TRUE(legacy->AddLog(std::string(32, static_cast<char>('a' + i)))
                      .HasValue());
    }
    ASSERT_SUCCESS(legacy->Finish());
  }
  // Segment 1 holds exactly kSegmentSize bytes; shaving one off must trip
  // the extent check (a torn tail is not a valid legacy layout).
  const auto segment_1 = Logger::SegmentPath(path_, 1);
  ASSERT_TRUE(std::filesystem::exists(segment_1));
  std::filesystem::resize_file(segment_1,
                               std::filesystem::file_size(segment_1) - 1);
  EXPECT_FALSE(BlobFile::Create(path_).HasValue());
  for (uint64_t index = 1; index < 8; ++index) {
    std::filesystem::remove(Logger::SegmentPath(path_, index));
  }
  std::filesystem::remove(Logger::SegmentMetadataPath(path_));
}

// Truncated geometry metadata (lost magic/version tail) must fail the open
// as kCorrupt instead of guessing a segment size.
TEST_F(BlobFileTest, TruncatedSegmentMetadataFailsClosed) {
  blob_.reset();
  constexpr size_t kSegmentSize = 64;
  {
    auto legacy = Logger::Create(path_, 64, 1, kSegmentSize).MoveValue();
    for (int i = 0; i < 8; ++i) {
      ASSERT_TRUE(legacy->AddLog(std::string(32, static_cast<char>('a' + i)))
                      .HasValue());
    }
    ASSERT_SUCCESS(legacy->Finish());
  }
  const auto metadata = Logger::SegmentMetadataPath(path_);
  ASSERT_TRUE(std::filesystem::exists(metadata));
  std::filesystem::resize_file(metadata, 3U);
  const StatusOr<std::unique_ptr<BlobFile>> opened = BlobFile::Create(path_);
  EXPECT_FALSE(opened.HasValue());
  EXPECT_EQ(opened.GetStatus().GetCode(), StatusCode::kCorrupt);
  for (uint64_t index = 1; index < 8; ++index) {
    std::filesystem::remove(Logger::SegmentPath(path_, index));
  }
  std::filesystem::remove(metadata);
}

}  // namespace tinylamb
