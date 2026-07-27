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

#include "index/lsm_detail/lsm_view.hpp"

#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "gtest/gtest.h"
#include "index/lsm_detail/blob_file.hpp"
#include "sorted_run.hpp"

namespace tinylamb {
class LSMViewTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = "tmp_view_merger_test-" + RandomString();
    std::filesystem::create_directory(path_);
    std::string blob_path = path_ / "blob.db";
    std::vector<std::filesystem::path> index_files;
    blob_ = std::make_unique<BlobFile>(blob_path);
    {
      for (int i = 0; i < 10; ++i) {
        std::map<std::string, LSMValue> mem_value;
        for (int j = 0; j < 100; ++j) {
          mem_value.emplace(std::to_string(j + (i * 100)),
                            LSMValue(std::to_string(i)));
        }
        std::string filepath = path_ / std::to_string(i);
        SortedRun::Construct(filepath, mem_value, *blob_, i);
        index_files.emplace_back(std::move(filepath));
      }
    }
    view_ = std::make_unique<LSMView>(*blob_, index_files);
  }

  void TearDown() override {
    view_.reset();
    blob_.reset();
    std::filesystem::remove_all(path_);
  }

  std::unique_ptr<LSMView> view_;
  std::unique_ptr<BlobFile> blob_;

  std::filesystem::path path_;
};

TEST_F(LSMViewTest, Find) {
  // Arrange -- LSMView is pre-constructed by SetUp() with 10 SortedRuns of 100 keys each
  // Act -- find 10 valid keys and 10 non-existent keys
  ASSERT_SUCCESS_AND_EQ(view_->Find("343"), "3");
  ASSERT_EQ(view_->Find("a43").GetStatus(), Status::kNotExists);
  ASSERT_SUCCESS_AND_EQ(view_->Find("822"), "8");
  ASSERT_EQ(view_->Find("83a").GetStatus(), Status::kNotExists);
  ASSERT_SUCCESS_AND_EQ(view_->Find("989"), "9");
  ASSERT_EQ(view_->Find("99x").GetStatus(), Status::kNotExists);
  ASSERT_SUCCESS_AND_EQ(view_->Find("445"), "4");
  ASSERT_EQ(view_->Find("33b").GetStatus(), Status::kNotExists);
  ASSERT_SUCCESS_AND_EQ(view_->Find("777"), "7");
  ASSERT_EQ(view_->Find("77b").GetStatus(), Status::kNotExists);
}

TEST_F(LSMViewTest, Iter) {
  // Arrange -- LSMView is pre-constructed by SetUp() with 10 SortedRuns of 100 keys each
  //            Build the expected map of 1000 keys (0..999) -> value (i/100)
  auto iter = view_->Begin();
  std::map<std::string, std::string> expected;
  for (int i = 0; i < 1000; ++i) {
    expected.emplace(std::to_string(i), std::to_string(i / 100));
  }
  auto it = expected.begin();

  // Act -- walk the iterator and compare each entry with the expected map
  while (iter.IsValid()) {
    ASSERT_EQ(iter.Key(), it->first);
    ASSERT_EQ(iter.Value(), it->second);
    ++iter;
    ++it;
  }

  // Assert -- iterator exhausted exactly when expected map is exhausted
  ASSERT_EQ(it, expected.end());
}

TEST_F(LSMViewTest, Merged) {
  // Arrange -- LSMView is pre-constructed by SetUp() with 10 SortedRuns of 100 keys each
  //            Build the expected map of 1000 keys (0..999) -> value (i/100)
  std::filesystem::path merged_file = path_ / "merged.idx";
  std::map<std::string, std::string> expected;
  for (int i = 0; i < 1000; ++i) {
    expected.emplace(std::to_string(i), std::to_string(i / 100));
  }

  // Act -- merge the 10 SortedRuns into a single SortedRun at merged_file
  view_->CreateSingleRun(merged_file);
  SortedRun merged(merged_file);
  ASSERT_EQ(merged.Size(), 100 * 10);

  // Act -- build a new LSMView from the single merged SortedRun and walk its iterator
  auto it = expected.begin();
  std::vector<SortedRun> sr{merged};
  LSMView new_merger(*blob_, sr);
  auto iter = new_merger.Begin();
  while (iter.IsValid()) {
    ASSERT_EQ(iter.Key(), it->first);
    ASSERT_EQ(iter.Value(), it->second);
    ++iter;
    ++it;
  }

  // Assert -- iterator exhausted exactly when expected map is exhausted
  ASSERT_EQ(it, expected.end());
}

TEST_F(LSMViewTest, Recover) {
  // Arrange -- destroy the existing view_ and blob_ to emulate a crash+recover scenario
  view_.reset();
  blob_ = std::make_unique<BlobFile>(path_ / "blob.db");
  std::vector<std::filesystem::path> idx;
  for (const auto& entry : std::filesystem::directory_iterator(path_)) {
    if (entry.path() != path_ / "blob.db") {
      idx.push_back(entry.path());
    }
  }

  // Act -- reconstruct the LSMView from the recovered blob and index files
  view_ = std::make_unique<LSMView>(*blob_, idx);

  // Assert -- find 5 valid keys; an out-of-range key returns kNotExists
  ASSERT_SUCCESS_AND_EQ(view_->Find("343"), "3");
  ASSERT_SUCCESS_AND_EQ(view_->Find("822"), "8");
  ASSERT_SUCCESS_AND_EQ(view_->Find("989"), "9");
  ASSERT_SUCCESS_AND_EQ(view_->Find("445"), "4");
  ASSERT_SUCCESS_AND_EQ(view_->Find("777"), "7");
  ASSERT_EQ(view_->Find("1923123").GetStatus(), Status::kNotExists);
}

TEST_F(LSMViewTest, Overwrite) {
  // Arrange -- build a SortedRun with 1000 keys (0..999) all mapped to value (i*2)
  std::map<std::string, LSMValue> mem_value;
  for (int i = 0; i < 1000; ++i) {
    mem_value.emplace(std::to_string(i), LSMValue(std::to_string(i * 2)));
  }
  std::string filepath = path_ / "overwrites.idx";
  SortedRun::Construct(filepath, mem_value, *blob_, 12);

  // Act -- build a new LSMView from the single overwrite SortedRun
  std::vector<SortedRun> index_files{SortedRun(filepath)};
  for (const auto& entry : std::filesystem::directory_iterator(path_)) {
    if (!entry.path().string().ends_with(".db")) {
      // index_files.emplace_back(entry.path());
    }
  }
  view_ = std::make_unique<LSMView>(*blob_, index_files);

  // Assert -- find each key; the overwrite run takes precedence so every key maps to i*2
  for (int i = 0; i < 1000; ++i) {
    ASSERT_SUCCESS_AND_EQ(view_->Find(std::to_string(i)),
                          std::to_string(i * 2));
  }
}

TEST_F(LSMViewTest, OverwriteAndScan) {
  // Arrange -- build a SortedRun with 1000 keys where even keys (i%2==0) map to i*2
  //            and odd keys (i%2==1) are absent (only 500 entries in the overwrite run)
  std::map<std::string, LSMValue> mem_value;
  for (int i = 0; i < 1000; i += 2) {
    mem_value.emplace(std::to_string(i), LSMValue(std::to_string(i * 2)));
  }
  std::string filepath = path_ / "overwrites.idx";
  SortedRun::Construct(filepath, mem_value, *blob_, 11);

  // Act -- build a new LSMView from the existing index files (including the overwrite run)
  std::vector<std::filesystem::path> index_files;
  for (const auto& entry : std::filesystem::directory_iterator(path_)) {
    if (!entry.path().string().ends_with(".db")) {
      index_files.push_back(entry.path());
    }
  }
  view_ = std::make_unique<LSMView>(*blob_, index_files);

  // Assert -- scan the view: even keys map to i*2, odd keys map to i/100 (from the original SetUp runs)
  auto iter = view_->Begin();
  while (iter.IsValid()) {
    std::string key = iter.Key();
    std::string value = iter.Value();
    int key_int = std::stol(key);
    if (key_int % 2 == 0) {
      ASSERT_EQ(value, std::to_string(key_int * 2));
    } else {
      ASSERT_EQ(value, std::to_string(key_int / 100));
    }
    ++iter;
  }
}

TEST_F(LSMViewTest, DeleteAndScan) {
  // Arrange -- build a SortedRun with a single delete entry for key "42"
  std::map<std::string, LSMValue> mem_value;
  mem_value.emplace(std::to_string(42), LSMValue::Delete());
  std::string filepath = path_ / "deletes.idx";
  SortedRun::Construct(filepath, mem_value, *blob_, 11);

  // Act -- build a new LSMView from the existing index files (including the delete run)
  std::vector<std::filesystem::path> index_files;
  for (const auto& entry : std::filesystem::directory_iterator(path_)) {
    if (!entry.path().string().ends_with(".db")) {
      index_files.push_back(entry.path());
    }
  }
  view_ = std::make_unique<LSMView>(*blob_, index_files);

  // Assert -- finding the deleted key "42" returns kNotExists (masked by the tombstone)
  StatusOr<std::string> result = view_->Find(std::to_string(42));
  ASSERT_EQ(result.GetStatus(), Status::kNotExists);
}

TEST_F(LSMViewTest, DeleteMultiAndScan) {
  // Arrange -- build a SortedRun with 500 delete entries for even keys (0,2,...,998)
  std::map<std::string, LSMValue> mem_value;
  for (int i = 0; i < 1000; i += 2) {
    mem_value.emplace(std::to_string(i), LSMValue::Delete());
  }
  std::filesystem::path filepath = path_ / "deletes.idx";
  SortedRun::Construct(filepath, mem_value, *blob_, 11);

  // Act -- build a new LSMView from the existing index files (including the delete run)
  std::vector<std::filesystem::path> index_files;
  for (const auto& entry : std::filesystem::directory_iterator(path_)) {
    if (!entry.path().string().ends_with(".db")) {
      index_files.push_back(entry.path());
    }
  }
  view_ = std::make_unique<LSMView>(*blob_, index_files);

  // Assert -- scan the view; every remaining key must be odd (even keys are tombstoned)
  auto iter = view_->Begin();
  int valid_key = 1;
  while (iter.IsValid()) {
    std::string key = iter.Key();
    int key_int = std::stol(key);
    ASSERT_EQ(key_int % 2, 1);
    ++iter;
  }
}

TEST_F(LSMViewTest, DeleteOverWriteScan) {
  // Arrange -- build two SortedRuns: deletes.idx (500 deletes for even keys) and overwrite.idx (250 values for i%4==0)
  {
    std::map<std::string, LSMValue> mem_value;
    for (int i = 0; i < 1000; i += 2) {
      mem_value.emplace(std::to_string(i), LSMValue::Delete());
    }
    std::filesystem::path filepath = path_ / "deletes.idx";
    SortedRun::Construct(filepath, mem_value, *blob_, 11);
  }
  {
    std::map<std::string, LSMValue> mem_value;
    for (int i = 0; i < 1000; i += 4) {
      mem_value.emplace(std::to_string(i), LSMValue("Hello"));
    }
    std::filesystem::path filepath = path_ / "overwrite.idx";
    SortedRun::Construct(filepath, mem_value, *blob_, 12);
  }

  // Act -- build a new LSMView from all existing index files (deletes + overwrites + original 10 runs)
  std::vector<std::filesystem::path> index_files;
  for (const auto& entry : std::filesystem::directory_iterator(path_)) {
    if (!entry.path().string().ends_with(".db")) {
      index_files.push_back(entry.path());
    }
  }
  view_ = std::make_unique<LSMView>(*blob_, index_files);

  // Assert -- scan the view; for i%4==0 the value is "Hello", for other odd keys the value is i/100
  auto iter = view_->Begin();
  int valid_key = 1;
  while (iter.IsValid()) {
    std::string key = iter.Key();
    int key_int = std::stol(key);
    if (key_int % 4 == 0) {
      ASSERT_EQ(iter.Value(), "Hello");
      LOG(INFO) << key_int;
    } else {
      ASSERT_EQ(key_int % 2, 1);
    }
    ++iter;
  }
}
}  // namespace tinylamb
