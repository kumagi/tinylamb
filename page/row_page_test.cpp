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
  // Arrange
  Page test_page(0, PageType::kRowPage);
  RowPage* row = &test_page.body.row_page;

  // Act -- nothing; default-constructed RowPage exposes free pointers
  // Assert -- free pointers match expected page body geometry
  ASSERT_EQ(row->FreePtrForTest(), kPageBodySize);
  ASSERT_EQ(row->FreeSizeForTest(), kPageBodySize - sizeof(RowPage));
}

TEST_F(RowPageTest, Insert) {
  // Act
  InsertRow("hello");
  // Assert -- implicit; gtest death on crash, gtest green on pass
}

TEST_F(RowPageTest, InsertMany) {
  // Arrange
  constexpr int kInserts = 100;
  size_t consumed = 0;
  PageRef ref = p_->GetPage(page_id_);
  RowPage& page = ref.GetRowPage();
  size_t before_size = page.FreeSizeForTest();
  ref.PageUnlock();

  // Act -- insert kInserts strings and verify row count after each insert
  for (int i = 0; i < kInserts; ++i) {
    std::string message = std::to_string(i) + " message";
    ASSERT_EQ(page.RowCount(), i);
    InsertRow(message);
    ASSERT_EQ(page.RowCount(), i + 1);
    consumed += message.size();
  }

  // Assert -- free size decreased by exactly kInserts*sizeof(RowPointer) +
  // consumed bytes
  ASSERT_EQ(page.FreeSizeForTest(),
            before_size - (kInserts * sizeof(RowPointer) + consumed));
}

TEST_F(RowPageTest, ReadMany) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert kInserts strings and read each back immediately
  constexpr int kInserts = 180;
  for (int i = 0; i < kInserts; ++i) {
    InsertRow(std::to_string(i) + " message");
    ASSERT_EQ(ReadRow(i), std::to_string(i) + " message");
  }

  // Act 2 -- flush and recover, then read all rows back to verify persistence
  Flush();
  Recover();
  for (int i = 0; i < kInserts; ++i) {
    ASSERT_EQ(ReadRow(i), std::to_string(i) + " message");
  }

  // Assert -- all rows survived flush/recover round-trip
  // (implicit in Act 2 assertions)
}

TEST_F(RowPageTest, UpdateMany) {
  // Arrange
  constexpr static int kInserts = 20;
  constexpr static char kLongMessage[] = " long updated messages!!!!!";
  constexpr static char kShortMessage[] = "s";

  // Act 1 -- insert kInserts short messages
  for (int i = 0; i < kInserts; ++i) {
    InsertRow(std::to_string(i) + " message");
  }
  Flush();
  Recover();  // RecoveryManager process will not do wrong thing.

  // Act 2 -- update even-indexed rows to long messages and verify each update
  for (int i = 0; i < kInserts; i += 2) {
    // even numbers.
    UpdateRow(i, std::to_string(i) + kLongMessage);
    ASSERT_EQ(ReadRow(i), std::to_string(i) + kLongMessage);
  }
  Flush();
  Recover();  // RecoveryManager process will not do wrong thing.

  // Act 3 -- update odd-indexed rows to short messages and verify each update
  for (int i = 1; i < kInserts; i += 2) {
    // odd numbers.
    UpdateRow(i, std::to_string(i) + kShortMessage);
    ASSERT_EQ(ReadRow(i), std::to_string(i) + kShortMessage);
  }
  Flush();
  Recover();  // RecoveryManager process will not do wrong thing.

  // Assert -- every row has the expected final message after all updates +
  // recoveries
  for (int i = 0; i < kInserts; ++i) {
    if (i % 2 == 0) {
      ASSERT_EQ(ReadRow(i), std::to_string(i) + kLongMessage);
    } else {
      ASSERT_EQ(ReadRow(i), std::to_string(i) + kShortMessage);
    }
  }
}

TEST_F(RowPageTest, DeleteMany) {
  // Arrange
  constexpr static char kMessage[] = "this is a pen";
  constexpr int kRows = 100;
  std::unordered_set<std::string> inserted;

  // Act 1 -- insert kRows messages and track each inserted message
  for (int i = 0; i < kRows; ++i) {
    std::string message = std::to_string(i) + kMessage;
    InsertRow(message);
    inserted.insert(message);
  }
  Flush();
  Recover();

  // Act 2 -- delete even-indexed rows and verify row count decremented
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

  // Act 3 -- verify remaining odd-indexed rows still readable, even-indexed
  // gone
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

  // Assert -- all surviving rows were read and erased from the tracking set
  ASSERT_TRUE(inserted.empty());
}

TEST_F(RowPageTest, InsertZeroLenAbort) {
  // Arrange
  auto txn = tm_->Begin();
  PageRef ref = p_->GetPage(page_id_);

  // Act -- insert a zero-length string into slot 0 then abort the txn
  ASSIGN_OR_ASSERT_FAIL(slot_t, s, ref->Insert(txn, ""));
  ASSERT_EQ(s, 0);
  ref.PageUnlock();
  txn.Abort();

  // Assert -- implicit; aborted txn leaves no durable trace; gtest green on
  // pass
}

TEST_F(RowPageTest, DeFragmentInvoked) {
  // Arrange
  size_t kBigRowSize = kPageBodySize / 3 - 16;

  // Act 1 -- insert three big rows, then a fourth that should fail (no space)
  ASSERT_TRUE(InsertRow(std::string(kBigRowSize, '0')));
  ASSERT_TRUE(InsertRow(std::string(kBigRowSize, '1')));
  ASSERT_TRUE(InsertRow(std::string(kBigRowSize, '2')));
  ASSERT_FALSE(InsertRow(std::string(kBigRowSize, '3')));

  // Act 2 -- delete row 0 to make space, then insert row 3 should succeed
  DeleteRow(0);
  EXPECT_EQ(GetRowCount(), 2);
  ASSERT_EQ(std::set<std::string>(
                {std::string(kBigRowSize, '1'), std::string(kBigRowSize, '2')}),
            std::set<std::string>({
                ReadRow(1),
                ReadRow(2),
            }));
  ASSERT_TRUE(InsertRow(std::string(kBigRowSize, '3')));

  // Assert -- after defrag, row 3 occupies the freed slot and all 3 big rows
  // coexist
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

TEST_F(RowPageTest, RepeatedGrowingUpdatesPreserveAllRows) {
  constexpr int kRows = 400;
  constexpr int kGrowingRows = 100;
  for (int i = 0; i < kRows; ++i) {
    ASSERT_TRUE(InsertRow(std::to_string(i) + std::string(60, 'a')));
  }
  for (int i = 0; i < kGrowingRows; ++i) {
    UpdateRow(i, std::to_string(i) + std::string(80, 'b'));
  }
  for (int i = 0; i < kRows; ++i) {
    const std::string expected =
        std::to_string(i) +
        std::string(i < kGrowingRows ? 80 : 60, i < kGrowingRows ? 'b' : 'a');
    EXPECT_EQ(ReadRow(i), expected) << "slot " << i;
  }
}

TEST_F(RowPageTest, ReusingManyHolesDoesNotOverwriteSlotArray) {
  constexpr int kRows = 300;
  constexpr int kDeletedRows = 250;
  for (int i = 0; i < kRows; ++i) {
    ASSERT_TRUE(InsertRow("original-" + std::to_string(i)));
  }
  for (int i = 0; i < kDeletedRows; ++i) {
    DeleteRow(i);
  }

  int replacements = 0;
  const std::string replacement(100, 'r');
  while (InsertRow(replacement + std::to_string(replacements))) {
    ++replacements;
  }
  ASSERT_GT(replacements, 0);

  for (int i = kDeletedRows; i < kRows; ++i) {
    EXPECT_EQ(ReadRow(i), "original-" + std::to_string(i)) << "slot " << i;
  }
  for (int i = 0; i < replacements; ++i) {
    const int slot = i < kDeletedRows ? i : kRows + i - kDeletedRows;
    EXPECT_EQ(ReadRow(slot), replacement + std::to_string(i))
        << "slot " << slot;
  }
}

TEST_F(RowPageTest, InsertTwoThreads) {
  // Arrange -- begin two concurrent transactions
  auto txn1 = tm_->Begin();
  auto txn2 = tm_->Begin();

  // Act -- interleave inserts from two transactions on the same page
  {
    // txn1
    PageRef ref = p_->GetPage(page_id_);
    std::string message = "message1";
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, ref->Insert(txn1, message));
    ASSERT_EQ(slot, 0);
  }
  {
    // txn2
    PageRef ref = p_->GetPage(page_id_);
    std::string message = "message2";
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, ref->Insert(txn2, message));
    ASSERT_EQ(slot, 1);
  }
  {
    // txn1 again
    PageRef ref = p_->GetPage(page_id_);
    std::string message = "message1-again";
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, ref->Insert(txn1, message));
    ASSERT_EQ(slot, 2);
  }

  // Assert -- both transactions commit successfully
  ASSERT_SUCCESS(txn1.PreCommit());
  ASSERT_SUCCESS(txn2.PreCommit());
}

TEST_F(RowPageTest, UpdateHeavy) {
  // Arrange
  constexpr int kCount = 50;
  Transaction txn = tm_->Begin();
  std::vector<std::string> rows(kCount);
  std::vector<slot_t> slots;
  slots.reserve(kCount);
  PageRef ref = p_->GetPage(page_id_);

  // Act 1 -- insert kCount random keys and record their slots
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 120 + 100);
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, ref->Insert(txn, key));
    slots.push_back(slot);
    rows[i] = key;
  }

  // Act 2 -- update each slot kCount*20 times via round-robin with new random
  // keys
  Row read;
  for (int i = 0; i < kCount * 20; ++i) {
    slot_t slot = slots[(i * 63) % slots.size()];
    std::string key = RandomString((19937 * i) % 120 + 10);
    ASSERT_SUCCESS(ref->Update(txn, slot, key));
    rows[slot] = key;
  }

  // Assert -- every slot's final value matches the last written key
  for (int i = 0; i < kCount; ++i) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, row, ref->Read(txn, i));
    ASSERT_EQ(rows[i], row);
  }
}

TEST_F(RowPageTest, UpdateAndDeleteHeavy) {
  // Arrange
  constexpr int kCount = 60;
  Transaction txn = tm_->Begin();
  std::vector<std::string> rows(kCount);
  std::vector<slot_t> slots;
  slots.reserve(kCount);
  PageRef ref = p_->GetPage(page_id_);

  // Act 1 -- insert kCount random keys and record their slots
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 120 + 100);
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, ref->Insert(txn, key));
    slots.push_back(slot);
    rows[i] = key;
  }

  // Act 2 -- for kCount*40 iterations, update or delete+re-insert each slot
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

  // Assert -- every slot's final value matches the last written key
  for (int i = 0; i < kCount; ++i) {
    ASSIGN_OR_ASSERT_FAIL(std::string_view, row, ref->Read(txn, i));
    ASSERT_EQ(rows[i], row);
  }
}
}  // namespace tinylamb
