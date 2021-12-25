#include "page/row_page.hpp"

#include <string>

#include "gtest/gtest.h"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

#include "page/row_page_test.hpp"

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
  for (int i = 0; i < kInserts; ++i) {
    std::string message = std::to_string(i) + " message";
    ASSERT_EQ(page.RowCount(), i);
    InsertRow(message);
    ASSERT_EQ(page.RowCount(), i + 1);
    consumed += message.size();
  }
  ASSERT_EQ(page.FreeSizeForTest(),
            before_size - (kInserts * sizeof(RowPage::RowPointer) + consumed));
  LOG(ERROR) << page.FreeSizeForTest();
}

TEST_F(RowPageTest, ReadMany) {
  constexpr int kInserts = 180;
  for (int i = 0; i < kInserts; ++i) {
    InsertRow(std::to_string(i) + " message");
    ASSERT_EQ(ReadRow(i), std::to_string(i) + " message");
  }
  Recover();
  for (int i = 0; i < kInserts; ++i) {
    ASSERT_EQ(ReadRow(i), std::to_string(i) + " message");
  }
}

TEST_F(RowPageTest, UpdateMany) {
  constexpr int kInserts = 20;
  constexpr char kLongMessage[] = " long updated messages!!!!!";
  constexpr char kShortMessage[] = "s";
  for (int i = 0; i < kInserts; ++i) {
    InsertRow(std::to_string(i) + " message");
  }
  Recover();  // Recovery process will not do wrong thing.
  for (int i = 0; i < kInserts; i += 2) {  // even numbers.
    UpdateRow(i, std::to_string(i) + kLongMessage);
    ASSERT_EQ(ReadRow(i), std::to_string(i) + kLongMessage);
  }
  Recover();  // Recovery process will not do wrong thing.
  for (int i = 1; i < kInserts; i += 2) {  // odd numbers.
    UpdateRow(i, std::to_string(i) + kShortMessage);
    ASSERT_EQ(ReadRow(i), std::to_string(i) + kShortMessage);
  }
  Recover();  // Recovery process will not do wrong thing.
  for (int i = 0; i < kInserts; ++i) {
    if (i % 2 == 0) {
      ASSERT_EQ(ReadRow(i), std::to_string(i) + kLongMessage);
    } else {
      ASSERT_EQ(ReadRow(i), std::to_string(i) + kShortMessage);
    }
  }
}

TEST_F(RowPageTest, DeleteMany) {
  constexpr char kMessage[] = "this is a pen";
  constexpr int kRows = 100;
  std::unordered_set<std::string> inserted;
  for (int i = 0; i < kRows; ++i) {
    std::string message = std::to_string(i) + kMessage;
    InsertRow(message);
    inserted.insert(message);
  }
  Recover();
  int deleted = 0;
  for (int i = 0; i < kRows / 2; ++i) {
    std::string victim = ReadRow(i);
    inserted.erase(victim);
    DeleteRow(i);
    ++deleted;
  }
  ASSERT_EQ(GetRowCount(), kRows - deleted);
  Recover();
  for (int i = 0; i < GetRowCount(); ++i) {
    std::string got_row = ReadRow(i);
    ASSERT_NE(inserted.find(got_row), inserted.end());
    inserted.erase(got_row);
  }
  ASSERT_TRUE(inserted.empty());
}

TEST_F(RowPageTest, InsertAbort) {
  auto txn = tm_->Begin();
  Row r;
  r.data = "blah~blah";
  PageRef page = p_->GetPage(page_id_);
  ASSERT_FALSE(page.IsNull());
  ASSERT_EQ(page->Type(), PageType::kRowPage);

  const uint16_t before_size = page->body.row_page.FreeSizeForTest();
  RowPosition pos;
  ASSERT_TRUE(page->Insert(txn, r, pos));
  ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
            before_size - r.data.size() - sizeof(RowPage::RowPointer));
  txn.Abort();
  ASSERT_EQ(GetRowCount(), 0);
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
                ReadRow(0),
                ReadRow(1),
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

TEST_F(RowPageTest, UpdateAbort) {
  std::string before = "before string hello world!",
              after = "replaced by this";
  ASSERT_TRUE(InsertRow(before));
  auto txn = tm_->Begin();
  Row r;
  r.data = after;
  PageRef page = p_->GetPage(page_id_);
  ASSERT_FALSE(page.IsNull());
  ASSERT_EQ(page->Type(), PageType::kRowPage);

  const uint16_t before_size = page->body.row_page.FreeSizeForTest();
  RowPosition pos(page->PageId(), 0);
  ASSERT_TRUE(page->Update(txn, pos, r));

  ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
            before_size - before.length() + after.length());
  txn.Abort();
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), before);
}

TEST_F(RowPageTest, DeleteAbort) {
  std::string before = "living row";
  ASSERT_TRUE(InsertRow(before));
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(page_id_);
  RowPosition target(page->PageId(), 0);
  page->Delete(txn, target);
  txn.Abort();
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), before);
}

}  // namespace tinylamb