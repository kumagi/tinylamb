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
#include <sstream>
#include <string>
#include <string_view>
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
  constexpr static std::string_view kLongMessage =
      " long updated messages!!!!!";
  constexpr static std::string_view kShortMessage = "s";

  // Act 1 -- insert kInserts short messages
  for (int i = 0; i < kInserts; ++i) {
    InsertRow(std::to_string(i) + " message");
  }
  Flush();
  Recover();  // RecoveryManager process will not do wrong thing.

  // Act 2 -- update even-indexed rows to long messages and verify each update
  for (int i = 0; i < kInserts; i += 2) {
    // even numbers.
    UpdateRow(i, std::to_string(i) + std::string(kLongMessage));
    ASSERT_EQ(ReadRow(i), std::to_string(i) + std::string(kLongMessage));
  }
  Flush();
  Recover();  // RecoveryManager process will not do wrong thing.

  // Act 3 -- update odd-indexed rows to short messages and verify each update
  for (int i = 1; i < kInserts; i += 2) {
    // odd numbers.
    UpdateRow(i, std::to_string(i) + std::string(kShortMessage));
    ASSERT_EQ(ReadRow(i), std::to_string(i) + std::string(kShortMessage));
  }
  Flush();
  Recover();  // RecoveryManager process will not do wrong thing.

  // Assert -- every row has the expected final message after all updates +
  // recoveries
  for (int i = 0; i < kInserts; ++i) {
    if (i % 2 == 0) {
      ASSERT_EQ(ReadRow(i), std::to_string(i) + std::string(kLongMessage));
    } else {
      ASSERT_EQ(ReadRow(i), std::to_string(i) + std::string(kShortMessage));
    }
  }
}

TEST_F(RowPageTest, DeleteMany) {
  // Arrange
  constexpr static std::string_view kMessage = "this is a pen";
  constexpr int kRows = 100;
  std::unordered_set<std::string> inserted;

  // Act 1 -- insert kRows messages and track each inserted message
  for (int i = 0; i < kRows; ++i) {
    std::string message = std::to_string(i) + std::string(kMessage);
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
  size_t kBigRowSize = (kPageBodySize / 3) - 16;

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
    std::string key = RandomString(((19937 * i) % 120) + 100);
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, ref->Insert(txn, key));
    slots.push_back(slot);
    rows[i] = key;
  }

  // Act 2 -- update each slot kCount*20 times via round-robin with new random
  // keys
  Row read;
  for (int i = 0; i < kCount * 20; ++i) {
    slot_t slot = slots[(static_cast<size_t>(i) * 63) % slots.size()];
    std::string key = RandomString(((19937 * i) % 120) + 10);
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
    std::string key = RandomString(((19937 * i) % 120) + 100);
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, ref->Insert(txn, key));
    slots.push_back(slot);
    rows[i] = key;
  }

  // Act 2 -- for kCount*40 iterations, update or delete+re-insert each slot
  Row read;
  for (int i = 0; i < kCount * 40; ++i) {
    slot_t slot = slots[(static_cast<size_t>(i) * 63) % slots.size()];
    std::string key = RandomString(((19937 * i) % 120) + 10);
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

// Regression test for a heap-buffer-overflow in RowPage::UpdateRow
// (page/row_page.cpp:153) reached through the crash-recovery undo path.
//
// Scenario:
//   1. The page is filled near capacity with committed 2000-byte rows.
//   2. Transaction T1 (never committed) shrinks slot 0 from 2000 to 100
//      bytes, freeing ~1900 bytes.
//   3. Transaction T2 (committed) inserts a ~1900-byte row into the freed
//      space.
//   4. The database "crashes" and is recovered via RecoverFrom().
//
// Recovery replays the log and then rolls back the uncommitted T1.  Rolling
// back the shrink calls Page::UpdateImpl -> RowPage::UpdateRow with the old
// 2000-byte value, but T2's committed row is still live so the page no longer
// has room for it.  UpdateRow has no space check on this path (unlike the
// guarded RowPage::Update): free_ptr_ is driven below the slot array and the
// memcpy at row_page.cpp:153 writes out of bounds, corrupting the slot array
// and, for large records, overflowing the page heap allocation.
TEST_F(RowPageTest, RecoveryUndoOfUncommittedShrinkDoesNotCorruptPage) {
  // Arrange -- fill the page near capacity with committed 2000-byte rows.
  for (int i = 0; i < 16; ++i) {
    ASSERT_TRUE(InsertRow(std::string(2000, 'a')));
  }

  // Act 1 -- an uncommitted transaction shrinks slot 0, freeing ~1900 bytes.
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(page_id_);
    ASSERT_SUCCESS(page->Update(txn, 0, std::string(100, 'b')));
    page.PageUnlock();
    // Deliberately never commit or abort: recovery must roll it back.
  }

  // Act 2 -- a second, committed transaction consumes the freed space.
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(page_id_);
    StatusOr<slot_t> slot = page->Insert(txn, std::string(1900, 'c'));
    ASSERT_SUCCESS(slot.GetStatus());
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Act 3 -- flush the page image, then crash and recover from the log.
  Flush();
  Recover();
  ASSERT_NO_FATAL_FAILURE(r_->RecoverFrom(0, tm_.get()));

  // Assert -- the page layout must stay intact: free_ptr_ never drops below
  // the slot array end, every live row must sit fully inside the page body,
  // and no live row may be lost.
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(page_id_);
  const RowPage& rp = page.GetRowPage();
  const char* const body = reinterpret_cast<const char*>(&rp);
  const char* const slot_array_end =
      body + sizeof(RowPage) + (rp.RowMax() * sizeof(RowPointer));
  EXPECT_GE(body + rp.FreePtrForTest(), slot_array_end);
  for (slot_t i = 0; i < rp.RowMax(); ++i) {
    if (rp.rows_[i].offset == 0) {
      continue;
    }
    const char* const row = body + rp.rows_[i].offset;
    EXPECT_GE(row, slot_array_end);
    EXPECT_LE(row + rp.rows_[i].size, body + kPageBodySize);
  }
  page.PageUnlock();

  // Restoring slot 0's 2000-byte image cannot fit anymore once T2's committed
  // insert consumed the freed space.  The undo must refuse cleanly instead of
  // dropping live rows as the old reclaim did, so only require that every
  // other committed row -- including T2's -- survives and the shrunken slot
  // stays readable without any structural damage.
  (void)ReadRow(0);
  for (int i = 1; i < 16; ++i) {
    EXPECT_EQ(ReadRow(i), std::string(2000, 'a')) << "slot " << i;
  }
  bool found_committed_insert = false;
  {
    auto probe = tm_->Begin();
    PageRef scan = p_->GetPage(page_id_);
    const RowPage& body_rp = scan.GetRowPage();
    for (slot_t i = 0; i < body_rp.RowMax(); ++i) {
      if (body_rp.rows_[i].offset != 0 &&
          body_rp.GetRow(i) == std::string(1900, 'c')) {
        found_committed_insert = true;
      }
    }
    scan.PageUnlock();
    EXPECT_SUCCESS(probe.PreCommit());
    probe.CommitWait();
  }
  EXPECT_TRUE(found_committed_insert);
}

// Regression test for the sibling heap-buffer-overflow in RowPage::InsertRow
// (page/row_page.cpp:103) reached through the crash-recovery undo path.
//
// Same root cause as RecoveryUndoOfUncommittedShrinkDoesNotCorruptPage, but
// through the delete path: an uncommitted DELETE frees slot 0, a committed
// transaction consumes the freed space, and recovery then rolls the delete
// back by re-inserting the old 2000-byte row via Page::InsertImpl.  InsertRow
// has no space check on this path (unlike the guarded RowPage::Insert), so
// free_ptr_ is driven below the slot array and the memcpy at row_page.cpp:103
// writes out of bounds.
TEST_F(RowPageTest, RecoveryUndoOfUncommittedDeleteDoesNotCorruptPage) {
  // Arrange -- fill the page near capacity with committed 2000-byte rows.
  for (int i = 0; i < 16; ++i) {
    ASSERT_TRUE(InsertRow(std::string(2000, 'a')));
  }

  // Act 1 -- an uncommitted transaction deletes slot 0, freeing 2000 bytes.
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(page_id_);
    ASSERT_SUCCESS(page->Delete(txn, 0));
    page.PageUnlock();
    // Deliberately never commit or abort: recovery must roll it back.
  }

  // Act 2 -- a second, committed transaction grows a different row (slot 15,
  // a position txn 1 does not lock) to consume the freed ~1900 bytes.
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(page_id_);
    ASSERT_SUCCESS(page->Update(txn, 15, std::string(3900, 'c')));
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Act 3 -- flush the page image, then crash and recover from the log.
  Flush();
  Recover();
  ASSERT_NO_FATAL_FAILURE(r_->RecoverFrom(0, tm_.get()));

  // Assert -- the page layout must stay intact: free_ptr_ never drops below
  // the slot array end, every live row must sit fully inside the page body.
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(page_id_);
  const RowPage& rp = page.GetRowPage();
  const char* const body = reinterpret_cast<const char*>(&rp);
  const char* const slot_array_end =
      body + sizeof(RowPage) + (rp.RowMax() * sizeof(RowPointer));
  EXPECT_GE(body + rp.FreePtrForTest(), slot_array_end);
  for (slot_t i = 0; i < rp.RowMax(); ++i) {
    if (rp.rows_[i].offset == 0) {
      continue;
    }
    const char* const row = body + rp.rows_[i].offset;
    EXPECT_GE(row, slot_array_end);
    EXPECT_LE(row + rp.rows_[i].size, body + kPageBodySize);
  }
  page.PageUnlock();

  // Re-inserting slot 0's 2000-byte image cannot fit after T2's committed
  // update consumed the freed space.  The undo must refuse cleanly instead of
  // dropping other live rows as the old reclaim did: the delete stays
  // applied, all untouched rows keep their committed images, and T2's grown
  // row survives.
  {
    auto probe = tm_->Begin();
    PageRef scan = p_->GetPage(page_id_);
    EXPECT_EQ(scan->Read(probe, 0).GetStatus(), Status::kNotExists);
    for (int i = 1; i < 15; ++i) {
      ASSIGN_OR_ASSERT_FAIL(std::string_view, value, scan->Read(probe, i));
      EXPECT_EQ(value, std::string(2000, 'a')) << "slot " << i;
    }
    ASSIGN_OR_ASSERT_FAIL(std::string_view, grown, scan->Read(probe, 15));
    EXPECT_EQ(grown, std::string(3900, 'c'));
    scan.PageUnlock();
    EXPECT_SUCCESS(probe.PreCommit());
    probe.CommitWait();
  }
}

TEST_F(RowPageTest, UpdateAndDeleteOutOfRangeSlot) {
  // Arrange -- a fresh, empty row page
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(page_id_);

  // Act/Assert -- updating a non-existent slot reports kNotExists
  ASSERT_EQ(page->Update(txn, 0, "x"), Status::kNotExists);

  // Act/Assert -- deleting a non-existent slot reports kNotExists
  ASSERT_EQ(page->Delete(txn, 0), Status::kNotExists);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(RowPageTest, ReadOnlyTransactionRejectedForWrites) {
  // Arrange -- commit one row first so Update/Delete address an existing
  // slot.  (The previous version of this test relied on the ghost-row bug:
  // the rejected Insert physically populated slot 0 without any WAL record,
  // which let the subsequent Update/Delete reach the lock check.)
  ASSERT_TRUE(InsertRow("committed"));

  // A read-only transaction cannot acquire write locks
  auto txn = tm_->Begin(true);
  PageRef page = p_->GetPage(page_id_);

  // Act/Assert -- insert conflicts on a read-only transaction
  ASSERT_EQ(page->Insert(txn, "hello").GetStatus(), Status::kConflicts);

  // Act/Assert -- update conflicts on a read-only transaction
  ASSERT_EQ(page->Update(txn, 0, "world"), Status::kConflicts);

  // Act/Assert -- delete conflicts on a read-only transaction
  ASSERT_EQ(page->Delete(txn, 0), Status::kConflicts);
}

// Regression test for the ghost-row bug (improvements2.md §2.1): a rejected
// lock acquisition used to leave the physical slot populated without any WAL
// record.  The WAL-less slot survived WriteBack and the MVCC fast path then
// showed the aborted row as a committed value to every snapshot.
TEST_F(RowPageTest, ConflictedInsertConsumesNoPhysicalSlot) {
  // Act -- an insert whose lock acquisition fails must not touch the page.
  {
    auto ro = tm_->Begin(true);
    PageRef page = p_->GetPage(page_id_);
    ASSERT_EQ(page->Insert(ro, "ghost").GetStatus(), Status::kConflicts);
    page.PageUnlock();
    ro.Abort();
  }

  // The residue must neither be visible now nor survive flush + recovery.
  EXPECT_EQ(GetRowCount(), 0U);
  Flush();
  Recover();
  ASSERT_NO_FATAL_FAILURE(r_->RecoverFrom(0, tm_.get()));
  EXPECT_EQ(GetRowCount(), 0U);

  // Assert -- the freed-up slot numbering is untouched: the next real insert
  // reuses slot 0 and reads back its own image.
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(page_id_);
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, page->Insert(txn, "real"));
    EXPECT_EQ(slot, 0U);
    ASSERT_SUCCESS(txn.PreCommit());
    txn.CommitWait();
  }
  EXPECT_EQ(ReadRow(0), "real");
}

TEST_F(RowPageTest, InsertSkipsHoleReservedByConcurrentDelete) {
  ASSERT_TRUE(InsertRow("row0"));
  ASSERT_TRUE(InsertRow("row1"));

  auto deleter = tm_->Begin();
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_SUCCESS(page->Delete(deleter, 0));
  }

  auto inserter = tm_->Begin();
  {
    PageRef page = p_->GetPage(page_id_);
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, page->Insert(inserter, "replacement"));
    EXPECT_EQ(slot, 2U);
  }
  ASSERT_SUCCESS(inserter.PreCommit());
  inserter.CommitWait();
  deleter.Abort();

  EXPECT_EQ(ReadRow(0), "row0");
  EXPECT_EQ(ReadRow(1), "row1");
  EXPECT_EQ(ReadRow(2), "replacement");
}

TEST_F(RowPageTest, DeFragmentAndDump) {
  // Arrange -- populate the page then leave a hole via delete
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(page_id_);
  ASSERT_SUCCESS(page->Insert(txn, "row0").GetStatus());
  ASSERT_SUCCESS(page->Insert(txn, "row1").GetStatus());
  ASSERT_SUCCESS(page->Delete(txn, 0));

  // Act -- compact the free space with the public DeFragment() entry point
  page->body.row_page.DeFragment();
  ASSERT_EQ(page->RowCount(), 1U);

  // Act -- stream the page through the Dump path
  std::ostringstream oss;
  oss << *page;

  // Assert -- the dump renders the page header and surviving row
  const std::string dumped = oss.str();
  EXPECT_NE(dumped.find("Rows: 1"), std::string::npos);
  EXPECT_NE(dumped.find("row1"), std::string::npos);
  ASSERT_SUCCESS(txn.PreCommit());
}
TEST_F(RowPageTest, OversizedRecordIsRejectedWithTooBigData) {
  // InsertRowAt/UpdateRow narrowed size_t into bin_size_t (u16) behind a
  // debug-only assert; NDEBUG builds silently truncated oversized records.
  Page test_page(0, PageType::kRowPage);
  RowPage* row = &test_page.body.row_page;
  const std::string huge(70000, 'x');
  StatusOr<slot_t> inserted = row->InsertRow(huge);
  EXPECT_FALSE(inserted.HasValue());
  EXPECT_EQ(inserted.GetStatus(), Status::kTooBigData);
  // Leaf/Branch parity: oversized payloads report kTooBigData, never kNoSpace.
  ASSERT_TRUE(InsertRow("row-a"));
  EXPECT_EQ(row->UpdateRow(0, huge), Status::kTooBigData);
  EXPECT_EQ(ReadRow(0), "row-a");
}
TEST_F(RowPageTest, DeleteRowReappliedIsIdempotent) {
  // D2 (docs/design.md): the loser-undo pattern rewinds page_lsn, so a
  // second recovery pass re-applies the same DeleteRow.  It must not
  // underflow row_count_ (the 65535 audit symptom) nor inflate free_size_.
  ASSERT_TRUE(InsertRow("row-a"));
  ASSERT_TRUE(InsertRow("row-b"));
  DeleteRow(0);  // committed delete of slot 0
  {
    PageRef page = p_->GetPage(page_id_);
    const bin_size_t free_before = page->body.row_page.FreeSizeForTest();
    page->body.row_page.DeleteRow(0);   // already deleted: no-op
    page->body.row_page.DeleteRow(99);  // out of range: no-op
    EXPECT_EQ(page->body.row_page.RowCount(), 1);
    EXPECT_EQ(page->body.row_page.FreeSizeForTest(), free_before);
  }
  EXPECT_EQ(GetRowCount(), 1);
  EXPECT_EQ(ReadRow(1), "row-b");
}

}  // namespace tinylamb
