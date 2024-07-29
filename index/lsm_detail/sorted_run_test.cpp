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

#include "index/lsm_detail/sorted_run.hpp"

#include <fcntl.h>

#include <cstddef>
#include <ios>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "blob_file.hpp"
#include "cache.hpp"
#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "gtest/gtest.h"
#include "recovery/logger.hpp"

namespace tinylamb {

class SortedRunEntryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    filepath_ = "sorted_run_entry_test-" + RandomString() + ".db";
  }

  void TearDown() override { std::ignore = std::filesystem::remove(filepath_); }

  std::string filepath_;
};

TEST_F(SortedRunEntryTest, Generate) {
  auto l = std::make_unique<BlobFile>(filepath_);
  SortedRun::Entry short_entry =
      SortedRun::Entry("abc", LSMValue{false, "val"}, *l);
  SortedRun::Entry middle_entry =
      SortedRun::Entry("abcdefhijk", LSMValue{false, "foobar"}, *l);
  std::string long_key(2000, 'a');
  SortedRun::Entry long_entry =
      SortedRun::Entry(long_key, LSMValue{false, "long value"}, *l);
  while (l->Written() < 6) {
  }
  l.reset();

  BlobFile blob(filepath_);
  ASSERT_EQ(short_entry.BuildKey(blob), "abc");
  ASSERT_EQ(short_entry.BuildValue(blob), "val");
  ASSERT_EQ(middle_entry.BuildKey(blob), "abcdefhijk");
  ASSERT_EQ(middle_entry.BuildValue(blob), "foobar");
  ASSERT_EQ(long_entry.BuildKey(blob), long_key);
  ASSERT_EQ(long_entry.BuildValue(blob), "long value");
}

TEST_F(SortedRunEntryTest, Compare) {
  auto l = std::make_unique<BlobFile>(filepath_);
  SortedRun::Entry short_entry("abc", LSMValue{false, "val"}, *l);
  SortedRun::Entry middle_entry("abcdefhijk", LSMValue{false, "foobar"}, *l);
  std::string long_key("abcdefghijklmnopqrstuvwxyz");
  SortedRun::Entry long_entry(long_key, LSMValue{false, "long value"}, *l);
  while (l->Written() < 6) {
  }
  l.reset();

  BlobFile blob(filepath_);
  ASSERT_LT(short_entry.Compare("abb", blob), 0);
  ASSERT_EQ(short_entry.Compare("abc", blob), 0);
  ASSERT_GT(short_entry.Compare("abd", blob), 0);
  ASSERT_LT(middle_entry.Compare("abcdefhijj", blob), 0);
  ASSERT_EQ(middle_entry.Compare("abcdefhijk", blob), 0);
  ASSERT_GT(middle_entry.Compare("abcdefhijkl", blob), 0);

  std::string long_smaller_key(long_key);
  long_smaller_key[long_smaller_key.size() - 1]--;
  ASSERT_EQ(long_entry.Compare(long_smaller_key, blob), -1);
  ASSERT_EQ(long_entry.Compare(long_key, blob), 0);

  std::string long_bigger_key(long_key);
  long_bigger_key[long_smaller_key.size() - 1]++;
  ASSERT_EQ(long_entry.Compare(long_bigger_key, blob), 1);
}

TEST_F(SortedRunEntryTest, MoreCompare) {
  std::vector<std::string> keys = {
      std::string(1, 0), std::string(2, 0),  std::string(3, 0),
      std::string(1, 0), std::string(2, 0),  std::string(3, 0),
      std::string(4, 0), std::string(4, 0),  std::string(8, 0),
      std::string(8, 0), std::string(12, 0), std::string(12, 0)};
  keys[3][0] = keys[4][1] = keys[5][1] = keys[7][3] = keys[10][7] =
      keys[11][11] = 1;
  std::vector<std::string> candidates;
  std::vector<SortedRun::Entry> entries;
  {
    auto l = std::make_unique<BlobFile>(filepath_);
    for (const auto& key : keys) {
      candidates.push_back(key);
      entries.emplace_back(key, LSMValue{false, ""}, *l);
      for (const auto& ext : keys) {
        candidates.push_back(key + ext);
        entries.emplace_back(key + ext, LSMValue{false, ""}, *l);
      }
    }
    l.reset();
  }
  BlobFile blob(filepath_);

  for (size_t i = 0; i < candidates.size(); ++i) {
    for (size_t j = 0; j < candidates.size(); ++j) {
      if (candidates[i] < candidates[j]) {
        ASSERT_GT(entries[i].Compare(candidates[j], blob), 0);
        ASSERT_GT(entries[i].Compare(entries[j], blob), 0);
      } else if (candidates[i] > candidates[j]) {
        ASSERT_LT(entries[i].Compare(candidates[j], blob), 0);
        ASSERT_LT(entries[i].Compare(entries[j], blob), 0);
      } else {
        ASSERT_EQ(entries[i].Compare(candidates[j], blob), 0);
        ASSERT_EQ(entries[i].Compare(entries[j], blob), 0);
      }
    }
  }
}

class SortedRunTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::map<std::string, LSMValue> input;
    for (int i = 0; i < 1000; ++i) {
      input.emplace("common_prefix:" + std::to_string(i),
                    LSMValue{false, std::to_string(i * i)});
    }
    std::string file_name = "sorted_run_build-test-" + RandomString() + ".db";
    auto l = std::make_unique<BlobFile>(file_name);
    sr_ = std::make_unique<SortedRun>(input, *l, 1);
    l.reset();

    blob_ = std::make_unique<BlobFile>(file_name);
  }
  std::unique_ptr<SortedRun> sr_;
  std::unique_ptr<BlobFile> blob_;
};

TEST_F(SortedRunTest, Build) {
  auto result = sr_->Find("common_prefix:121", *blob_);
  ASSERT_SUCCESS_AND_EQ(result, "14641");
}

TEST_F(SortedRunTest, Find) {
  for (int i = 0; i < 1000; ++i) {
    auto result = sr_->Find("common_prefix:" + std::to_string(i), *blob_);
    ASSERT_SUCCESS_AND_EQ(result, std::to_string(i * i));
  }
  auto minus = sr_->Find("common_prefix:" + std::to_string(-1), *blob_);
  ASSERT_EQ(minus.GetStatus(), Status::kNotExists);
  auto over = sr_->Find("common_prefix:" + std::to_string(10000), *blob_);
  ASSERT_EQ(over.GetStatus(), Status::kNotExists);
}

TEST_F(SortedRunTest, Iterator) {
  auto iter = sr_->Begin(*blob_);
  while (iter.IsValid()) {
    ++iter;
  }
}

}  // namespace tinylamb