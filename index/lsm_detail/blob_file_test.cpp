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

#include <filesystem>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "gtest/gtest.h"
#include "index/lsm_detail/cache.hpp"
#include "recovery/logger.hpp"

namespace tinylamb {
class BlobFileTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = "tmp_blob_file_test-" + RandomString();
    blob_ = std::make_unique<BlobFile>(path_);
  }

  void TearDown() override {
    // Drain the writer explicitly instead of sleeping and hoping.
    if (blob_) {
      blob_->Flush();
    }
    blob_.reset();
    std::filesystem::remove_all(path_);
  }

  std::unique_ptr<BlobFile> blob_;
  std::string path_;
};

TEST_F(BlobFileTest, ReadAt) {
  // Arrange -- build a tree of 2 key/value pairs and a Logger to write them
  std::map<std::string, std::string> tree;
  tree.emplace("foo", "barr");
  tree.emplace("value", "notice");
  auto lg = std::make_unique<Logger>(path_);

  // Act -- write each key and value through the Logger, then destroy the logger
  for (const auto& it : tree) {
    lg->AddLog(it.first);
    lg->AddLog(it.second);
  }
  lg.reset();

  // Assert -- read back each entry at the expected offset via BlobFile::ReadAt
  ASSERT_EQ(blob_->ReadAt(0, 3), "foo");
  ASSERT_EQ(blob_->ReadAt(3, 4), "barr");
  ASSERT_EQ(blob_->ReadAt(7, 5), "value");
  ASSERT_EQ(blob_->ReadAt(12, 6), "notice");
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
  // Arrange -- write two length-prefixed records through a Logger
  auto lg = std::make_unique<Logger>(path_);
  lg->AddLog(EncodeRecord("hello"));
  lg->AddLog(EncodeRecord("world of blob"));
  lg.reset();

  // Act -- read each record with the locked, length-prefix aware overload
  std::string_view out;
  {
    Cache::Locks locks = blob_->ReadAt(0, out);
    ASSERT_EQ(std::string(out), "hello");
  }
  {
    Cache::Locks locks = blob_->ReadAt(4 + 5, out);
    ASSERT_EQ(std::string(out), "world of blob");
  }
}

TEST_F(BlobFileTest, AppendWrittenAndFlush) {
  // Arrange -- a fresh BlobFile with no writes yet
  ASSERT_EQ(blob_->Written(), 0);

  // Act -- append three payloads
  const lsn_t first = blob_->Append("first");
  const lsn_t second = blob_->Append("second");
  const lsn_t third = blob_->Append("third");

  // Assert -- offsets are sequential and Written() advances past them
  ASSERT_EQ(first, 0);
  ASSERT_EQ(second, static_cast<lsn_t>(first + 5));
  ASSERT_EQ(third, static_cast<lsn_t>(second + 6));

  // Act -- wait for the writer to flush, then read the appended bytes
  blob_->Flush();
  ASSERT_GE(blob_->Written(), static_cast<lsn_t>(third + 5));

  // Assert -- each appended payload reads back verbatim
  ASSERT_EQ(blob_->ReadAt(first, 5), "first");
  ASSERT_EQ(blob_->ReadAt(second, 6), "second");
  ASSERT_EQ(blob_->ReadAt(third, 5), "third");
}

TEST_F(BlobFileTest, StreamOperator) {
  // Arrange -- a BlobFile with a couple of writes
  blob_->Append("stream");
  blob_->Flush();

  // Act -- stream the file
  std::stringstream ss;
  ss << *blob_;

  // Assert -- the dump exposes the written byte count and cache state
  ASSERT_NE(ss.str().find("BlobFile(written="), std::string::npos);
  ASSERT_NE(ss.str().find("cache="), std::string::npos);
}
}  // namespace tinylamb
