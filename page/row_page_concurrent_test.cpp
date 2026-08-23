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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <future>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
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
  // Arrange -- spawn 8 threads, each inserting 100 random-string rows
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([this]() {
      for (int j = 0; j < 100; ++j) {
        // Act -- insert a row with a random string key
        EXPECT_TRUE(InsertRow(RandomString()));
      }
    });
  }

  // Act -- join all threads (waits for all insertions to complete)
  for (auto& thread : threads) {
    thread.join();
  }

  // Assert -- implicit; all threads completed without deadlock; gtest green on
  // pass
}

TEST_F(RowPageConcurrentTest, InsertUpdate) {
  // Arrange -- pre-insert 100 rows, then spawn 4 inserters + 4 updaters
  constexpr int kRows = 100;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int j = 0; j < kRows; ++j) {
    EXPECT_TRUE(InsertRow(RandomString()));
  }
  for (int i = 0; i < kThreads / 2; ++i) {
    threads.emplace_back([this]() {
      for (int j = 0; j < kRows; ++j) {
        // Act (inserter) -- insert a row with a random string key
        EXPECT_TRUE(InsertRow(RandomString()));
      }
    });
  }
  for (int i = 0; i < kThreads / 2; ++i) {
    threads.emplace_back([&, i]() {
      std::mt19937 rand(i);
      for (int j = 0; j < kRows; ++j) {
        // Act (updater) -- update a random existing row with a new random
        // string
        UpdateRow(rand() % kRows, RandomString());
      }
    });
  }

  // Act -- join all threads (waits for all insertions and updates to complete)
  for (auto& thread : threads) {
    thread.join();
  }

  // Assert -- implicit; all threads completed without deadlock; gtest green on
  // pass
}

TEST_F(RowPageConcurrentTest, UpdateUpdate) {
  // Arrange -- fill the page with random-string rows until full, then spawn 8
  // updaters
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  thread_local std::mt19937 engine(seed_gen());
  while (InsertRow(RandomString(engine() % 64))) {
  }
  size_t rows = GetRowCount();

  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&]() {
      // Act -- each thread updates 100 random rows with new random strings
      for (int j = 0; j < 100; ++j) {
        UpdateRow(engine() % rows, RandomString(engine() % 64));
      }
    });
  }

  // Act -- join all threads (waits for all updates to complete)
  for (auto& thread : threads) {
    thread.join();
  }

  // Assert -- implicit; all threads completed without deadlock; gtest green on
  // pass
}

TEST_F(RowPageConcurrentTest, ReaderUsesSnapshotWhileWriterIsUncommitted) {
  ASSERT_TRUE(InsertRow("committed"));

  Transaction writer = tm_->Begin();
  PageRef writer_page = p_->GetPage(page_id_);
  ASSERT_SUCCESS(writer_page->Update(writer, 0, "uncommitted"));
  writer_page.PageUnlock();

  auto reader = std::async(std::launch::async, [this]() {
    Transaction txn = tm_->Begin();
    PageRef page = p_->GetPage(page_id_);
    StatusOr<std::string_view> value = page->Read(txn, 0);
    std::string result = value.HasValue() ? std::string(value.Value()) : "";
    page.PageUnlock();
    EXPECT_SUCCESS(txn.PreCommit());
    return std::pair(value.GetStatus(), result);
  });

  ASSERT_EQ(reader.wait_for(std::chrono::milliseconds(500)),
            std::future_status::ready);
  const auto [status, value] = reader.get();
  EXPECT_EQ(status, Status::kSuccess);
  EXPECT_EQ(value, "committed");

  EXPECT_SUCCESS(writer.PreCommit());
  EXPECT_EQ(ReadRow(0), "uncommitted");
}

TEST_F(RowPageConcurrentTest, SnapshotRemainsStableAfterWriterCommits) {
  ASSERT_TRUE(InsertRow("version-1"));
  Transaction reader = tm_->Begin();

  Transaction writer = tm_->Begin();
  PageRef page = p_->GetPage(page_id_);
  ASSERT_SUCCESS(page->Update(writer, 0, "version-2"));
  page.PageUnlock();
  ASSERT_SUCCESS(writer.PreCommit());

  PageRef reader_page = p_->GetPage(page_id_);
  ASSERT_SUCCESS_AND_EQ(reader_page->Read(reader, 0), "version-1");
  reader_page.PageUnlock();
  ASSERT_SUCCESS(reader.PreCommit());
  EXPECT_EQ(ReadRow(0), "version-2");
}

TEST_F(RowPageConcurrentTest, DeleteAndInsertRespectSnapshotVisibility) {
  ASSERT_TRUE(InsertRow("existing"));
  Transaction old_reader = tm_->Begin();

  Transaction deleter = tm_->Begin();
  PageRef page = p_->GetPage(page_id_);
  ASSERT_SUCCESS(page->Delete(deleter, 0));
  page.PageUnlock();
  ASSERT_SUCCESS(deleter.PreCommit());

  PageRef old_page = p_->GetPage(page_id_);
  ASSERT_SUCCESS_AND_EQ(old_page->Read(old_reader, 0), "existing");
  old_page.PageUnlock();

  Transaction inserter = tm_->Begin();
  PageRef insert_page = p_->GetPage(page_id_);
  ASSIGN_OR_ASSERT_FAIL(slot_t, reused,
                        insert_page->Insert(inserter, "replacement"));
  ASSERT_EQ(reused, 0);
  insert_page.PageUnlock();

  PageRef still_old_page = p_->GetPage(page_id_);
  ASSERT_SUCCESS_AND_EQ(still_old_page->Read(old_reader, 0), "existing");
  still_old_page.PageUnlock();
  ASSERT_SUCCESS(inserter.PreCommit());
  ASSERT_SUCCESS(old_reader.PreCommit());
  EXPECT_EQ(ReadRow(0), "replacement");
}

TEST_F(RowPageConcurrentTest, AbortedVersionNeverBecomesVisible) {
  ASSERT_TRUE(InsertRow("durable"));
  Transaction writer = tm_->Begin();
  PageRef page = p_->GetPage(page_id_);
  ASSERT_SUCCESS(page->Update(writer, 0, "discarded"));
  page.PageUnlock();

  Transaction reader = tm_->Begin();
  PageRef reader_page = p_->GetPage(page_id_);
  ASSERT_SUCCESS_AND_EQ(reader_page->Read(reader, 0), "durable");
  reader_page.PageUnlock();
  ASSERT_SUCCESS(reader.PreCommit());

  writer.Abort();
  EXPECT_EQ(ReadRow(0), "durable");
}

TEST_F(RowPageConcurrentTest, WritersWaitForExclusiveLock) {
  ASSERT_TRUE(InsertRow("base"));
  Transaction first = tm_->Begin();
  PageRef page = p_->GetPage(page_id_);
  ASSERT_SUCCESS(page->Update(first, 0, "first"));
  page.PageUnlock();

  std::atomic<bool> started{false};
  std::thread waiter([&] {
    Transaction second = tm_->Begin();
    started.store(true, std::memory_order_release);
    PageRef second_page = p_->GetPage(page_id_);
    ASSERT_SUCCESS(second_page->Update(second, 0, "second"));
    second_page.PageUnlock();
    ASSERT_SUCCESS(second.PreCommit());
  });
  while (!started.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  ASSERT_SUCCESS(first.PreCommit());
  waiter.join();
  EXPECT_EQ(ReadRow(0), "second");
}
}  // namespace tinylamb
