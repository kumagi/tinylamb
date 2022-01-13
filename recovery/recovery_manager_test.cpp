#include "recovery/recovery_manager.hpp"

#include <memory>
#include <string>

#include "page/row_page_test.hpp"

namespace tinylamb {

class RecoveryManagerTest : public RowPageTest {
 protected:
  static constexpr char kDBFileName[] = "recovery_test.db";
  static constexpr char kLogName[] = "recovery_test.log";

  void SetUp() override { RowPageTest::SetUp(); }

  void RecoverBase(const std::function<void(void)>& f) {
    if (p_) {
      p_->GetPool()->LostAllPageForTest();
    }
    tm_.reset();
    lm_.reset();
    l_.reset();
    r_.reset();
    p_.reset();
    f();
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(kLogName, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), l_.get(), r_.get());
  }

  void Recover() override {
    RecoverBase([]() {});
  }

  void MediaFailure() {
    RecoverBase([]() { std::remove(kDBFileName); });
  }

  void SinglePageFailure(page_id_t failed_page) {
    RecoverBase([&]() {
      std::fstream db(kDBFileName, std::ios_base::out | std::ios_base::binary);
      db.seekp(failed_page * kPageSize, std::ios_base::beg);
      ASSERT_FALSE(db.fail());
      for (size_t i = 0; i < kPageSize; ++i) {
        db.write("\xff", 1);
      }
      ASSERT_FALSE(db.fail());
    });
  }

  void TearDown() override {
    std::remove(kDBFileName);
    std::remove(kLogName);
  }

  std::unique_ptr<RecoveryManager> r_;
};

TEST_F(RecoveryManagerTest, EmptyRecovery) { r_->RecoverFrom(0, tm_.get()); }

TEST_F(RecoveryManagerTest, InsertAbort) {
  auto txn = tm_->Begin();
  Row r;
  r.data = "blah~blah";
  PageRef page = p_->GetPage(page_id_);
  ASSERT_FALSE(page.IsNull());
  ASSERT_EQ(page->Type(), PageType::kRowPage);

  const uint16_t before_size = page->body.row_page.FreeSizeForTest();
  RowPosition pos;
  ASSERT_TRUE(page->Insert(txn, r, pos));
  page.PageUnlock();

  ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
            before_size - r.data.size() - sizeof(RowPage::RowPointer));
  txn.Abort();
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, UpdateAbort) {
  std::string before = "before string hello world!", after = "replaced by this";
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
  page.PageUnlock();

  ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
            before_size - before.length() + after.length());
  txn.Abort();
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), before);
}

TEST_F(RecoveryManagerTest, DeleteAbort) {
  std::string before = "living row";
  ASSERT_TRUE(InsertRow(before));
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(page_id_);
  RowPosition target(page->PageId(), 0);
  page->Delete(txn, target);
  page.PageUnlock();

  txn.Abort();
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), before);
}

TEST_F(RecoveryManagerTest, InsertRowRecovery) {
  InsertRow("hoge");
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(ReadRow(0), "hoge");
}

TEST_F(RecoveryManagerTest, UpdateRowRecovery) {
  ASSERT_TRUE(InsertRow("hoge"));
  UpdateRow(0, "bar");
  ASSERT_EQ(ReadRow(0), "bar");
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(ReadRow(0), "bar");
}

TEST_F(RecoveryManagerTest, DeleteRowRecovery) {
  ASSERT_TRUE(InsertRow("hoge"));
  DeleteRow(0);
  ASSERT_EQ(GetRowCount(), 0);
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, InsertRowAbortRecovery) {
  InsertRow("hoge", false);
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, UpdateRowAbortRecovery) {
  ASSERT_TRUE(InsertRow("hoge"));
  UpdateRow(0, "bar", false);
  ASSERT_EQ(ReadRow(0), "hoge");
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(ReadRow(0), "hoge");
}

TEST_F(RecoveryManagerTest, DeleteRowAbortRecovery) {
  ASSERT_TRUE(InsertRow("hoge"));
  DeleteRow(0, false);
  ASSERT_EQ(GetRowCount(), 1);
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "hoge");
}

TEST_F(RecoveryManagerTest, InsertCrash) {
  auto txn = tm_->Begin();
  Row r;
  r.data = "blah~blah";
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);

    const uint16_t before_size = page->body.row_page.FreeSizeForTest();
    RowPosition pos;
    ASSERT_TRUE(page->Insert(txn, r, pos));
    ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
              before_size - r.data.size() - sizeof(RowPage::RowPointer));
  }
  // Note that txn is not committed.
  Recover();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, UpdateCrash) {
  ASSERT_TRUE(InsertRow("original message"));
  auto txn = tm_->Begin();
  Row r;
  r.data = "blah~blah";
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);

    RowPosition pos(page_id_, 0);
    ASSERT_TRUE(page->Update(txn, pos, r));
  }
  // Note that txn is not committed.
  Recover();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryManagerTest, DeleteCrash) {
  ASSERT_TRUE(InsertRow("original message"));
  auto txn = tm_->Begin();
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);

    RowPosition pos(page_id_, 0);
    ASSERT_TRUE(page->Delete(txn, pos));
  }
  // Note that txn is not committed.
  Recover();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryManagerTest, InsertMediaCrash) {
  auto txn = tm_->Begin();
  Row r;
  r.data = "blah~blah";
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);

    const uint16_t before_size = page->body.row_page.FreeSizeForTest();
    RowPosition pos;
    ASSERT_TRUE(page->Insert(txn, r, pos));
    ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
              before_size - r.data.size() - sizeof(RowPage::RowPointer));
  }
  // Note that txn is not committed.
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, UpdateMediaCrash) {
  ASSERT_TRUE(InsertRow("original message"));
  auto txn = tm_->Begin();
  Row r;
  r.data = "blah~blah";
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);

    RowPosition pos(page_id_, 0);
    ASSERT_TRUE(page->Update(txn, pos, r));
    // Note that txn is not committed.
  }
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryManagerTest, DeleteMediaCrash) {
  ASSERT_TRUE(InsertRow("original message"));
  auto txn = tm_->Begin();
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);

    RowPosition pos(page_id_, 0);
    ASSERT_TRUE(page->Delete(txn, pos));
  }
  // Note that txn is not committed.
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryManagerTest, InsertSinglePageFailure) {
  auto txn = tm_->Begin();
  Row r;
  r.data = "blah~blah";
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);

    const uint16_t before_size = page->body.row_page.FreeSizeForTest();
    RowPosition pos;
    ASSERT_TRUE(page->Insert(txn, r, pos));
    ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
              before_size - r.data.size() - sizeof(RowPage::RowPointer));
  }
  // Note that txn is not committed.
  SinglePageFailure(page_id_);
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, UpdateSinglePageFailure) {
  ASSERT_TRUE(InsertRow("original message"));
  auto txn = tm_->Begin();
  Row r;
  r.data = "modified message";
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);

    const uint16_t before_size = page->body.row_page.FreeSizeForTest();
    RowPosition pos(page_id_, 0);
    ASSERT_TRUE(page->Update(txn, pos, r));
  }
  // Note that txn is not committed.
  SinglePageFailure(page_id_);
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryManagerTest, DeleteSinglePageFailure) {
  ASSERT_TRUE(InsertRow("original message"));
  auto txn = tm_->Begin();
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);

    RowPosition pos(page_id_, 0);
    ASSERT_TRUE(page->Delete(txn, pos));
  }
  // Note that txn is not committed.
  SinglePageFailure(page_id_);
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

}  // namespace tinylamb