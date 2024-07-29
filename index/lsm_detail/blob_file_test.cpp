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

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "gtest/gtest.h"
#include "recovery/logger.hpp"

namespace tinylamb {

class BlobFileTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = "tmp_blob_file_test-" + RandomString();
    blob_ = std::make_unique<BlobFile>(path_);
  }
  void TearDown() override {
    blob_.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::filesystem::remove_all(path_);
  }

  std::unique_ptr<BlobFile> blob_;
  std::string path_;
};

TEST_F(BlobFileTest, ReadAt) {
  std::map<std::string, std::string> tree;
  tree.emplace("foo", "barr");
  tree.emplace("value", "notice");
  auto lg = std::make_unique<Logger>(path_);
  for (const auto& it : tree) {
    lg->AddLog(it.first);
    lg->AddLog(it.second);
  }
  lg.reset();
  ASSERT_EQ(blob_->ReadAt(0, 3), "foo");
  ASSERT_EQ(blob_->ReadAt(3, 4), "barr");
  ASSERT_EQ(blob_->ReadAt(7, 5), "value");
  ASSERT_EQ(blob_->ReadAt(12, 6), "notice");
}

}  // namespace tinylamb