#include "recovery/recovery.hpp"

#include <memory>
#include <string>

#include "page/row_page_test.hpp"

namespace tinylamb {

class RecoveryTest : public RowPageTest {
 protected:
  static constexpr char kDBFileName[] = "recovery_test.db";
  static constexpr char kLogName[] = "recovery_test.log";
  static constexpr char kTableName[] = "test-table_for_recovery";

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

  void WaitForCommit(uint64_t target_lsn, size_t timeout_ms = 1000) {
    size_t counter = 0;
    while (l_->CommittedLSN() != target_lsn && counter < timeout_ms) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      ++counter;
    }
    EXPECT_LT(counter, timeout_ms);
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

}  // namespace tinylamb