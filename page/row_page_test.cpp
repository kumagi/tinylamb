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

#include "page/row_page.hpp"

#include <cstddef>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/row_page_test.hpp"
#include "page_ref.hpp"
#include "page_type.hpp"
#include "row_pointer.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {
TEST(ConstructTest, constrct) {
  Page test_page(0, PageType::kRowPage);
  RowPage* row = &test_page.body.row_page;
  ASSERT_EQ(row->FreePtrForTest(), kPageBodySize);
  ASSERT_EQ(row->FreeSizeForTest(), kPageBodySize - sizeof(RowPage));
}

TEST_F(RowPageTest, Insert) { InsertRow("hello"); }

TEST_F(RowPageTest, InsertMany) {
  constexpr int kInserts = 100;
  size_t consumed = 0;
  PageRef ref = p_->GetPage(page_id_);
  RowPage& page = ref.GetRowPage();
  size_t before_size = page.FreeSizeForTest();
  ref.PageUnlock();
  for (int i = 0; i < kInserts; ++i) {
    std::string message = std::to_string(i) + " message";
    ASSERT_EQ(page.RowCount(), i);
    InsertRow(message);
    ASSERT_EQ(page.RowCount(), i + 1);
    consumed += message.size();
  }
  ASSERT_EQ(page.FreeSizeForTest(),
            before_size - (kInserts * sizeof(RowPointer) + consumed));
}

TEST_F(RowPageTest, ReadMany) {
  constexpr int kInserts = 180;
  for (int i = 0; i < kInserts; ++i) {
    InsertRow(std::to_string(i) + " message");
    ASSERT_EQ(ReadRow(i), std::to_string(i) + " message");
  }
  Flush();
  Recover();
  for (int i = 0; i < kInserts; ++i) {
    ASSERT_EQ(ReadRow(i), std::to_string(i) + " message");
  }
}

TEST_F(RowPageTest, UpdateMany) {
  constexpr static int kInserts = 20;
  constexpr static char kLongMessage[] = " long updated messages!!!!!";
  constexpr static char kShortMessage[] = "s";
  for (int i = 0; i < kInserts; ++i) {
    InsertRow(std::to_string(i) + " message");
  }
  Flush();
  Recover();  // RecoveryManager process will not do wrong thing.
  for (int i = 0; i < kInserts; i += 2) {
    // even numbers.
    UpdateRow(i, std::to_string(i) + kLongMessage);
    ASSERT_EQ(ReadRow(i), std::to_string(i) + kLongMessage);
  }
  Flush();
  Recover();  // RecoveryManager process will not do wrong thing.
  for (int i = 1; i < kInserts; i += 2) {
    // odd numbers.
    UpdateRow(i, std::to_string(i) + kShortMessage);
    ASSERT_EQ(ReadRow(i), std::to_string(i) + kShortMessage);
  }
  Flush();
  Recover();  // RecoveryManager process will not do wrong thing.
  for (int i = 0; i < kInserts; ++i) {
    if (i % 2 == 0) {
      ASSERT_EQ(ReadRow(i), std::to_string(i) + kLongMessage);
    } else {
      ASSERT_EQ(ReadRow(i), std::to_string(i) + kShortMessage);
    }
  }
}

TEST_F(RowPageTest, DeleteMany) {
  constexpr static char kMessage[] = "this is a pen";
  constexpr int kRows = 100;
  std::unordered_set<std::string> inserted;
  for (int i = 0; i < kRows; ++i) {
    std::string message = std::to_string(i) + kMessage;
    InsertRow(message);
    inserted.insert(message);
  }
  Flush();
  Recover();
  int deleted = 0;
  for (int i = 0; i < kRows; i += 2) {
    std::string victim = ReadRow(i);
    inserted.erase(victim);
    DeleteRow(i);
    ++deleted;
  }
  ASSERT_EQ(GetRowCount(), kRows - deleted);
  Flush();
  Recover();
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(page_id_);
  for (size_t i = 0; i < kRows; ++i) {
    if (i % 2 == 0) {
      ASSERT_EQ(Status::kNotExists, page->Read(txn, i).GetStatus());
    } else {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, got_row, page->Read(txn, i));
      ASSERT_NE(inserted.find(std::string(got_row)), inserted.end());
      inserted.erase(std::string(got_row));
    }
  }
  ASSERT_TRUE(inserted.empty());
}

TEST_F(RowPageTest, InsertZeroLenAbort) {
  auto txn = tm_->Begin();
  PageRef ref = p_->GetPage(page_id_);
  ASSIGN_OR_ASSERT_FAIL(slot_t, s, ref->Insert(txn, ""));
  ASSERT_EQ(s, 0);
  ref.PageUnlock();
  txn.Abort();
}

TEST_F(RowPageTest, DeFragmentInvoked) {
  size_t kBigRowSize = kPageBodySize / 3 - 16;

  ASSERT_TRUE(InsertRow(std::string(kBigRowSize, '0')));
  ASSERT_TRUE(InsertRow(std::string(kBigRowSize, '1')));
  ASSERT_TRUE(InsertRow(std::string(kBigRowSize, '2')));
  ASSERT_FALSE(InsertRow(std::string(kBigRowSize, '3')));

  DeleteRow(0);

  EXPECT_EQ(GetRowCount(), 2);
  ASSERT_EQ(std::set<std::string>(
                {std::string(kBigRowSize, '1'), std::string(kBigRowSize, '2')}),
            std::set<std::string>({
                ReadRow(1),
                ReadRow(2),
            }));

  ASSERT_TRUE(InsertRow(std::string(kBigRowSize, '3')));

  EXPECT_EQ(GetRowCount(), 3);
  ASSERT_EQ(std::set<std::string>({
                std::string(kBigRowSize, '1'),
                std::string(kBigRowSize, '2'),
                std::string(kBigRowSize, '3'),
            }),
            std::set<std::string>({
                ReadRow(0),
                ReadRow(1),
                ReadRow(2),
            }));
}

TEST_F(RowPageTest, InsertTwoThreads) {
  auto txn1 = tm_->Begin();
  auto txn2 = tm_->Begin();
  {
    // txn1
    PageRef ref = p_->GetPage(page_id_);
    std::string message = "message1";
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, ref->Insert(txn1, message));
    ASSERT_EQ(slot, 0);
  }
  {
    PageRef ref = p_->GetPage(page_id_);
    std::string message = "message2";
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, ref->Insert(txn2, message));
    ASSERT_EQ(slot, 1);
  }
  {
    PageRef ref = p_->GetPage(page_id_);
    std::string message = "message1-again";
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, ref->Insert(txn1, message));
    ASSERT_EQ(slot, 2);
  }
  ASSERT_SUCCESS(txn1.PreCommit());
  ASSERT_SUCCESS(txn2.PreCommit());
}

TEST_F(RowPageTest, UpdateHeavy) {
  constexpr int kCount = 50;
  Transaction txn = tm_->Begin();
  std::vector<std::string> rows(kCount);
  std::vector<slot_t> slots;
  slots.reserve(kCount);
  PageRef ref = p_->GetPage(page_id_);
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 120 + 100);
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, ref->Insert(txn, key));
    slots.push_back(slot);
    rows[i] = key;
  }
  Row read;
  for (int i = 0; i < kCount * 20; ++i) {
    slot_t slot = slots[(i * 63) % slots.size()];
    std::string key = RandomString((19937 * i) % 120 + 10);
    ASSERT_SUCCESS(ref->Update(txn, slot, key));
    rows[slot] = key;
  }
  for (int i = 0; i < kCount; ++i) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, row, ref->Read(txn, i));
    ASSERT_EQ(rows[i], row);
  }
}

TEST_F(RowPageTest, UpdateAndDeleteHeavy) {
  constexpr int kCount = 60;
  Transaction txn = tm_->Begin();
  std::vector<std::string> rows(kCount);
  std::vector<slot_t> slots;
  slots.reserve(kCount);
  PageRef ref = p_->GetPage(page_id_);
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 120 + 100);
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, ref->Insert(txn, key));
    slots.push_back(slot);
    rows[i] = key;
  }
  Row read;
  for (int i = 0; i < kCount * 40; ++i) {
    slot_t slot = slots[(i * 63) % slots.size()];
    std::string key = RandomString((19937 * i) % 120 + 10);
    if (i % 2 == 0) {
      ASSERT_SUCCESS(ref->Update(txn, slot, key));
    } else {
      ASSERT_SUCCESS(ref->Delete(txn, slot));
      ASSERT_SUCCESS(ref->Insert(txn, key).GetStatus());
    }
    rows[slot] = key;
  }
  for (int i = 0; i < kCount; ++i) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, row, ref->Read(txn, i));
    ASSERT_EQ(rows[i], row);
  }
}
}  // namespace tinylamb
