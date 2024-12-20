/**
 * Copyright 2023 KUMAZAKI Hiroki
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

#include <cstddef>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/row_page_test.hpp"
#include "page_ref.hpp"
#include "page_type.hpp"

namespace tinylamb {
class RowPageConcurrentTest : public RowPageTest {
  void SetUp() override {
    std::string current_test =
        ::testing::UnitTest::GetInstance()->current_test_info()->name();
    file_name_ = "row_page_concurrent_test-" + current_test + RandomString();
    Recover();
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage);
    page_id_ = page->PageID();
    EXPECT_SUCCESS(txn.PreCommit());
  }
};

constexpr int kThreads = 8;
TEST_F(RowPageConcurrentTest, InsertInsert) {
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([this]() {
      for (int j = 0; j < 100; ++j) {
        EXPECT_TRUE(InsertRow(RandomString()));
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(RowPageConcurrentTest, InsertUpdate) {
  constexpr int kRows = 100;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int j = 0; j < kRows; ++j) {
    EXPECT_TRUE(InsertRow(RandomString()));
  }
  for (int i = 0; i < kThreads / 2; ++i) {
    threads.emplace_back([this]() {
      for (int j = 0; j < kRows; ++j) {
        EXPECT_TRUE(InsertRow(RandomString()));
      }
    });
  }
  for (int i = 0; i < kThreads / 2; ++i) {
    threads.emplace_back([&, i]() {
      std::mt19937 rand(i);
      for (int j = 0; j < kRows; ++j) {
        UpdateRow(rand() % kRows, RandomString());
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(RowPageConcurrentTest, UpdateUpdate) {
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  thread_local std::mt19937 engine(seed_gen());
  while (InsertRow(RandomString(engine() % 64))) {
  }
  size_t rows = GetRowCount();
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < 100; ++j) {
        UpdateRow(engine() % rows, RandomString(engine() % 64));
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
}  // namespace tinylamb
