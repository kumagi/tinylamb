#include "recovery/recovery.hpp"

#include <memory>
#include <string>

#include "page/row_page_test.hpp"

namespace tinylamb {

class RecoveryTest : public RowPageTest {
 protected:
  static constexpr char kDBFileName[] = "recovery_test.db";
  static constexpr char kLogName[] = "recovery_test.log";

  void SetUp() override { RowPageTest::SetUp(); }

  void RecoverBase(const std::function<void(void)>& f) {
    tm_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    r_.reset();
    f();
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get());
    r_ = std::make_unique<Recovery>(kLogName, kDBFileName, p_->GetPool(),
                                    tm_.get());
  }

  void Recover() override {
    RecoverBase([]() {});
  }

  void MediaFailure() {
    RecoverBase([]() { std::remove(kDBFileName); });
  }

  void SinglePageFailure(uint64_t failed_page) {
    RecoverBase([&]() {
      std::fstream db(kDBFileName, std::ios_base::out | std::ios_base::binary);
      db.seekp(failed_page * kPageSize, std::ios_base::beg);
      ASSERT_FALSE(db.fail());
      for (size_t i = 0; i < kPageSize; ++i) {
        db.write("\x01", 1);
      }
      ASSERT_FALSE(db.fail());
    });
  }

  void TearDown() override {
    std::remove(kDBFileName);
    std::remove(kLogName);
  }

  std::unique_ptr<Recovery> r_;
};

TEST_F(RecoveryTest, EmptyRecovery) { r_->StartFrom(0); }

TEST_F(RecoveryTest, InsertRowRecovery) {
  InsertRow("hoge");
  MediaFailure();
  r_->StartFrom(0);
  ASSERT_EQ(ReadRow(0), "hoge");
}

TEST_F(RecoveryTest, UpdateRowRecovery) {
  ASSERT_TRUE(InsertRow("hoge"));
  UpdateRow(0, "bar");
  ASSERT_EQ(ReadRow(0), "bar");
  MediaFailure();
  r_->StartFrom(0);
  ASSERT_EQ(ReadRow(0), "bar");
}

TEST_F(RecoveryTest, DeleteRowRecovery) {
  ASSERT_TRUE(InsertRow("hoge"));
  DeleteRow(0);
  ASSERT_EQ(GetRowCount(), 0);
  MediaFailure();
  r_->StartFrom(0);
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryTest, InsertCrash) {
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
  r_->StartFrom(0);
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryTest, UpdateCrash) {
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
  r_->StartFrom(0);
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryTest, DeleteCrash) {
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
  r_->StartFrom(0);
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryTest, InsertMediaCrash) {
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
  r_->StartFrom(0);
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryTest, UpdateMediaCrash) {
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
  r_->StartFrom(0);
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryTest, DeleteMediaCrash) {
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
  r_->StartFrom(0);
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryTest, InsertSinglePageFailure) {
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
  r_->StartFrom(0);
  ASSERT_EQ(GetRowCount(), 0);
}

}  // namespace tinylamb