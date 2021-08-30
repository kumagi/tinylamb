#include "page/row_page.hpp"

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/catalog.hpp"

namespace tinylamb {

TEST(RowPageTest, constrct) {
  std::unique_ptr<RowPage> row(
      reinterpret_cast<RowPage*>(new Page(0, PageType::kRowPage)));
  ASSERT_EQ(row->FreePtrForTest(), kPageSize);
  ASSERT_EQ(row->FreeSizeForTest(), kPageSize - sizeof(RowPage));
}

class SingleSchemaRowPageTest : public ::testing::Test {
 protected:
  static constexpr char kDBFileName[] = "row_page_test.db";
  static constexpr char kLogName[] = "row_page_test.log";
  static constexpr char kTableName[] = "test-table_for_row_page";

  void SetUp() override {
    Recover();
    auto txn = tm_->Begin();
    auto* page = reinterpret_cast<RowPage*>(
        p_->AllocateNewPage(txn, PageType::kRowPage));
    page_id_ = page->PageId();
  }

  void Recover() {
    tm_.reset();
    lm_.reset();
    l_.reset();
    c_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    c_ = std::make_unique<Catalog>(p_.get());
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get());
  }

  void TearDown() override {
    std::remove(kDBFileName);
    std::remove(kLogName);
  }

  void WaitForCommit(uint64_t target_lsn, size_t timeout_ms = 1000) {
    size_t counter = 0;
    while (l_->CommittedLSN() != target_lsn && counter < timeout_ms) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      ++counter;
    }
    EXPECT_LT(counter, timeout_ms);
  }

  bool InsertRow(std::string_view str) {
    auto txn = tm_->Begin();
    Row r;
    r.data = str;
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(page_id_));
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(p->Type(), PageType::kRowPage);

    const uint16_t before_size = p->FreeSizeForTest();
    RowPosition pos;
    bool success = p->Insert(txn, r, pos);
    if (success) {
      EXPECT_EQ(p->FreeSizeForTest(),
                before_size - r.data.size() - sizeof(RowPage::RowPointer));
    }
    p_->Unpin(p->PageId());
    EXPECT_TRUE(txn.PreCommit());
    txn.CommitWait();
    return success;
  }

  void UpdateRow(int slot, std::string_view str) {
    auto txn = tm_->Begin();
    Row r;
    r.data = str;
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(page_id_));
    ASSERT_NE(p, nullptr);
    ASSERT_EQ(p->Type(), PageType::kRowPage);

    RowPosition pos(page_id_, slot);
    ASSERT_TRUE(p->Update(txn, pos, r));
    p_->Unpin(p->PageId());
    ASSERT_TRUE(txn.PreCommit());
    txn.CommitWait();
  }

  void DeleteRow(int slot) {
    auto txn = tm_->Begin();
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(page_id_));
    ASSERT_NE(p, nullptr);
    ASSERT_EQ(p->Type(), PageType::kRowPage);

    RowPosition pos(page_id_, slot);
    ASSERT_TRUE(p->Delete(txn, pos));
    p_->Unpin(p->PageId());
    ASSERT_TRUE(txn.PreCommit());
    txn.CommitWait();
  }

  std::string ReadRow(int slot) {
    auto txn = tm_->Begin();
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(page_id_));
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(p->Type(), PageType::kRowPage);
    Row dst;
    RowPosition pos(page_id_, slot);
    EXPECT_TRUE(p->Read(txn, pos, dst));
    dst.MakeOwned();
    p_->Unpin(p->PageId());
    EXPECT_TRUE(txn.PreCommit());
    txn.CommitWait();
    return dst.owned_data;
  }

  size_t GetRowCount() {
    auto txn = tm_->Begin();
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(page_id_));
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(p->Type(), PageType::kRowPage);
    size_t row_count = p->RowCount();
    p_->Unpin(p->PageId());
    EXPECT_TRUE(txn.PreCommit());
    txn.CommitWait();
    return row_count;
  }

  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<Catalog> c_;
  int page_id_ = 0;
};

TEST_F(SingleSchemaRowPageTest, Insert) { InsertRow("hello"); }

TEST_F(SingleSchemaRowPageTest, InsertMany) {
  constexpr int kInserts = 100;
  size_t consumed = 0;
  auto* page = reinterpret_cast<RowPage*>(p_->GetPage(page_id_));
  size_t before_size = page->FreeSizeForTest();
  for (int i = 0; i < kInserts; ++i) {
    std::string message = std::to_string(i) + " message";
    ASSERT_EQ(page->RowCount(), i);
    InsertRow(message);
    ASSERT_EQ(page->RowCount(), i + 1);
    consumed += message.size();
  }
  ASSERT_EQ(page->FreeSizeForTest(),
            before_size - (kInserts * sizeof(RowPage::RowPointer) + consumed));
  p_->Unpin(page->PageId());
  LOG(ERROR) << page->FreeSizeForTest();
}

TEST_F(SingleSchemaRowPageTest, ReadMany) {
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

TEST_F(SingleSchemaRowPageTest, UpdateMany) {
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

TEST_F(SingleSchemaRowPageTest, DeleteMany) {
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

TEST_F(SingleSchemaRowPageTest, InsertAbort) {
  auto txn = tm_->Begin();
  Row r;
  r.data = "blah~blah";
  auto* p = reinterpret_cast<RowPage*>(p_->GetPage(page_id_));
  ASSERT_NE(p, nullptr);
  ASSERT_EQ(p->Type(), PageType::kRowPage);

  const uint16_t before_size = p->FreeSizeForTest();
  RowPosition pos;
  ASSERT_TRUE(p->Insert(txn, r, pos));
  ASSERT_EQ(p->FreeSizeForTest(),
            before_size - r.data.size() - sizeof(RowPage::RowPointer));
  p_->Unpin(p->PageId());
  txn.Abort();
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(SingleSchemaRowPageTest, DeFragmentHappen) {
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

}  // namespace tinylamb