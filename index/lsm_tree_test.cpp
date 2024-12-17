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

#include "index/lsm_tree.hpp"

#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <thread>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "gtest/gtest.h"
#include "lsm_detail/lsm_view.hpp"

namespace tinylamb {
class LSMTreeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_path_ = "lsm_tree_test-" + RandomString();
    t_ = std::make_unique<LSMTree>(dir_path_);
  }

  void TearDown() override {
    t_.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::filesystem::remove_all(dir_path_);
  }

  std::unique_ptr<LSMTree> t_;
  std::string dir_path_;
};

// TEST_F(LSMTreeTest, Construct) {}

TEST_F(LSMTreeTest, WriteOne) {
  t_->Write("foo", "bar");
  std::this_thread::sleep_for(std::chrono::milliseconds(1));
}

TEST_F(LSMTreeTest, WriteMany) {
  for (int i = 0; i < 1000; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
}

TEST_F(LSMTreeTest, ReadOne) {
  t_->Write("foo", "bar");
  auto result = t_->Read("foo");
  ASSERT_SUCCESS_AND_EQ(result, "bar");
}

TEST_F(LSMTreeTest, ReadManyMemory) {
  for (int i = 0; i < 100; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
  }
  for (int i = 0; i < 100; ++i) {
    ASSERT_SUCCESS_AND_EQ(t_->Read(std::to_string(i)), std::to_string(i * 2));
  }
}

TEST_F(LSMTreeTest, RangeScan) {
  std::map<std::string, std::string> expected;
  for (int i = 0; i < 1000; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
    expected.emplace(std::to_string(i), std::to_string(i * 2));
  }
  t_->Sync();
  LSMView v = t_->GetView();
  LSMView::Iterator it = v.Begin();
  auto expected_iter = expected.begin();
  while (it.IsValid()) {
    ASSERT_EQ(expected_iter->first, it.Key());
    ASSERT_EQ(expected_iter->second, it.Value());
    ++it;
    ++expected_iter;
  }
  ASSERT_EQ(expected_iter, expected.end());
}

TEST_F(LSMTreeTest, PointQuery) {
  for (int i = 0; i < 1000; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
  }
  t_->Sync();
  LSMView v = t_->GetView();
  for (int i = 0; i < 1000; ++i) {
    auto ret = v.Find(std::to_string(i));
    ASSERT_SUCCESS_AND_EQ(ret, std::to_string(i * 2));
  }
}

TEST_F(LSMTreeTest, OverwrittenRangeScan) {
  for (int i = 0; i < 1000; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
  }
  t_->Sync();
  LSMView v1 = t_->GetView();
  for (int i = 0; i < 1000; i += 2) {
    t_->Write(std::to_string(i), std::to_string(i * i));
  }
  t_->Sync();
  LSMView v2 = t_->GetView();
  for (int i = 0; i < 1000; ++i) {
    auto ret = v1.Find(std::to_string(i));
    ASSERT_SUCCESS_AND_EQ(ret, std::to_string(i * 2));
  }
  for (int i = 0; i < 1000; ++i) {
    if (i % 2 == 0) {
      auto ret = v2.Find(std::to_string(i));
      ASSERT_SUCCESS_AND_EQ(ret, std::to_string(i * i));
    } else {
      auto ret = v2.Find(std::to_string(i));
      ASSERT_SUCCESS_AND_EQ(ret, std::to_string(i * 2));
    }
  }
}

TEST_F(LSMTreeTest, LongKeyRangeScan) {
  std::map<std::string, std::string> expected;
  for (int i = 0; i < 300; ++i) {
    std::string key(i * i + 1, 'x');
    t_->Write(key, std::to_string(i * 2));
    expected.emplace(key, std::to_string(i * 2));
  }
  t_->Sync();
  LSMView v = t_->GetView();
  LSMView::Iterator it = v.Begin();
  auto expected_iter = expected.begin();
  while (it.IsValid()) {
    ASSERT_EQ(expected_iter->first, it.Key());
    ASSERT_EQ(expected_iter->second, it.Value());
    ++it;
    ++expected_iter;
  }
  ASSERT_EQ(expected_iter, expected.end());
}

TEST_F(LSMTreeTest, DeleteSingle) {
  for (int i = 0; i < 1000; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
  }
  t_->Sync();
  for (int i = 1; i < 1000; i += 2) {
    t_->Delete(std::to_string(i));
  }
  t_->Sync();
  for (int i = 0; i < 1000; ++i) {
    StatusOr<std::string> ret = t_->Read(std::to_string(i));
    if (i % 2 == 0) {
      ASSERT_SUCCESS_AND_EQ(ret, std::to_string(i * 2));
    } else {
      ASSERT_EQ(ret.GetStatus(), Status::kNotExists);
    }
  }
}

TEST_F(LSMTreeTest, DeleteRangeScan) {
  for (int i = 0; i < 1000; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
  }
  t_->Sync();
  for (int i = 1; i < 1000; i += 2) {
    t_->Delete(std::to_string(i));
  }
  t_->Sync();
  LSMView v = t_->GetView();
  LSMView::Iterator iter = v.Begin();
  while (iter.IsValid()) {
    int key = std::stoi(iter.Key());
    ASSERT_EQ(key % 2, 0);
    ASSERT_EQ(iter.Value(), std::to_string(key * 2));
    ++iter;
  }
}
}  // namespace tinylamb
