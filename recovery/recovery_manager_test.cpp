#include "recovery/recovery_manager.hpp"

#include <memory>
#include <string>

#include "common/test_util.hpp"
#include "page/row_page_test.hpp"

namespace tinylamb {

class RecoveryManagerTest : public RowPageTest {
 protected:
  void SetUp() override {
    file_name_ = "recovery_manager_test-" + RandomString();
    RowPageTest::SetUp();
  }

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
    p_ = std::make_unique<PageManager>(file_name_ + ".db", 10);
    l_ = std::make_unique<Logger>(file_name_ + ".log");
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(file_name_ + ".log", p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), l_.get(), r_.get());
  }

  void Recover() override {
    RecoverBase([]() {});
  }

  void MediaFailure() {
    RecoverBase([&]() { std::remove((file_name_ + ".db").c_str()); });
  }

  void SinglePageFailure(page_id_t failed_page) {
    RecoverBase([&]() {
      std::fstream db(file_name_ + ".db",
                      std::ios_base::out | std::ios_base::binary);
      db.seekp(failed_page * kPageSize, std::ios_base::beg);
      ASSERT_FALSE(db.fail());
      for (size_t i = 0; i < kPageSize; ++i) {
        db.write("\xff", 1);
      }
      ASSERT_FALSE(db.fail());
    });
  }

  void TearDown() override {
    std::remove((file_name_ + ".db").c_str());
    std::remove((file_name_ + ".log").c_str());
  }

  std::unique_ptr<RecoveryManager> r_;
};

TEST_F(RecoveryManagerTest, EmptyRecovery) { r_->RecoverFrom(0, tm_.get()); }

TEST_F(RecoveryManagerTest, InsertAbort) {
  auto txn = tm_->Begin();
  std::string record = "blah~blah";
  PageRef page = p_->GetPage(page_id_);
  ASSERT_FALSE(page.IsNull());
  ASSERT_EQ(page->Type(), PageType::kRowPage);

  const bin_size_t before_size = page->body.row_page.FreeSizeForTest();
  slot_t slot;
  ASSERT_SUCCESS(page->Insert(txn, record, &slot));
  page.PageUnlock();

  ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
            before_size - record.size() - sizeof(RowPage::RowPointer));
  txn.Abort();
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, UpdateAbort) {
  std::string before = "before string hello world!", after = "replaced by this";
  ASSERT_TRUE(InsertRow(before));
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(page_id_);
  ASSERT_FALSE(page.IsNull());
  ASSERT_EQ(page->Type(), PageType::kRowPage);

  const bin_size_t before_size = page->body.row_page.FreeSizeForTest();
  ASSERT_SUCCESS(page->Update(txn, 0, after));
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
  page->Delete(txn, 0);
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
  std::string record = "blah~blah";
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);

    const bin_size_t before_size = page->body.row_page.FreeSizeForTest();
    slot_t slot;
    ASSERT_SUCCESS(page->Insert(txn, record, &slot));
    ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
              before_size - record.size() - sizeof(RowPage::RowPointer));
  }
  // Note that txn is not committed.
  Recover();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, UpdateCrash) {
  ASSERT_TRUE(InsertRow("original message"));
  auto txn = tm_->Begin();
  std::string record = "blah~blah";
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);

    ASSERT_SUCCESS(page->Update(txn, 0, record));
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
    ASSERT_SUCCESS(page->Delete(txn, 0));
  }
  // Note that txn is not committed.
  Recover();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryManagerTest, InsertMediaCrash) {
  auto txn = tm_->Begin();
  std::string record = "blah~blah";
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);

    const bin_size_t before_size = page->body.row_page.FreeSizeForTest();
    slot_t slot;
    ASSERT_SUCCESS(page->Insert(txn, record, &slot));
    ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
              before_size - record.size() - sizeof(RowPage::RowPointer));
  }
  // Note that txn is not committed.
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, UpdateMediaCrash) {
  ASSERT_TRUE(InsertRow("original message"));
  auto txn = tm_->Begin();
  std::string record = "blah~blah";
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);
    ASSERT_SUCCESS(page->Update(txn, 0, record));
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
    ASSERT_SUCCESS(page->Delete(txn, 0));
  }
  // Note that txn is not committed.
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryManagerTest, InsertSinglePageFailure) {
  auto txn = tm_->Begin();
  std::string record = "blah~blah";
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);

    const bin_size_t before_size = page->body.row_page.FreeSizeForTest();
    slot_t slot;
    ASSERT_SUCCESS(page->Insert(txn, record, &slot));
    ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
              before_size - record.size() - sizeof(RowPage::RowPointer));
  }
  // Note that txn is not committed.
  SinglePageFailure(page_id_);
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, UpdateSinglePageFailure) {
  ASSERT_TRUE(InsertRow("original message"));
  auto txn = tm_->Begin();
  std::string record = "modified message";
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);
    ASSERT_SUCCESS(page->Update(txn, 0, record));
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
    ASSERT_SUCCESS(page->Delete(txn, 0));
  }
  // Note that txn is not committed.
  SinglePageFailure(page_id_);
  r_->RecoverFrom(0, tm_.get());
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

}  // namespace tinylamb