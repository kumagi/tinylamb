#include "recovery/recovery.hpp"

#include <memory>
#include <string>

#include "page/row_page_test.hpp"

namespace tinylamb {

class RecoveryTest : public RowPageTest {
 protected:
  static constexpr char kDBFileName[] = "recovery_test.db";
  static constexpr char kLogName[] = "recovery_test.log";

  void SetUp() override {
    RowPageTest::SetUp();
  }

  void Recover() override {
    tm_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    r_.reset();
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get());
    r_ = std::make_unique<Recovery>(kLogName, kDBFileName, p_.get(), tm_.get());
  }

  void MediaFailure() {
    tm_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    r_.reset();
    std::remove(kDBFileName);
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get());
    r_ = std::make_unique<Recovery>(kLogName, kDBFileName, p_.get(), tm_.get());
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
  auto* p = reinterpret_cast<RowPage*>(p_->GetPage(page_id_));
  LOG(ERROR) << "allocated " << p->PageId() << " for " << page_id_;
  ASSERT_NE(p, nullptr);
  ASSERT_EQ(p->Type(), PageType::kRowPage);

  const uint16_t before_size = p->FreeSizeForTest();
  RowPosition pos;
  ASSERT_TRUE(p->Insert(txn, r, pos));
  ASSERT_EQ(p->FreeSizeForTest(),
            before_size - r.data.size() - sizeof(RowPage::RowPointer));
  p_->Unpin(p->PageId());
  // Note that txn is not committed.
  Recover();
  LOG(INFO) << "recovery start";
  r_->StartFrom(0);
  ASSERT_EQ(GetRowCount(), 0);
}

}  // namespace tinylamb