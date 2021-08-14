#include "recovery/recovery.hpp"

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "page/row_page.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/catalog.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

class RecoveryTest : public ::testing::Test {
 protected:
  static constexpr char kDBFileName[] = "recovery_test.db";
  static constexpr char kLogName[] = "recovery_test.log";
  static constexpr char kTableName[] = "test-table_for_recovery";

  void SetUp() override {
    LogCleanup();
    Recover();
    std::vector<Column> columns = {Column("int_column", ValueType::kInt64, 8,
                                          Restriction::kNoRestriction, 0),
                                   Column("varchar_column", ValueType::kVarChar,
                                          16, Restriction::kNoRestriction, 8)};
    Schema sc(kTableName, columns, 2);
    auto txn = tm_->Begin();
    ASSERT_TRUE(c_->CreateTable(txn, sc));
    ASSERT_EQ(c_->Schemas(), 1);
    txn.PreCommit();
    txn.CommitWait();
    ASSERT_EQ(1, c_->Schemas());
  }

  void Recover() {
    r_.reset();
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
    r_ = std::make_unique<Recovery>(kLogName, kDBFileName, p_.get(), tm_.get());
  }
  void MediaFailure() {
    tm_.reset();
    lm_.reset();
    l_.reset();
    c_.reset();
    p_.reset();
    std::remove(kDBFileName);
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    c_ = std::make_unique<Catalog>(p_.get());
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get());
    r_ = std::make_unique<Recovery>(kLogName, kDBFileName, p_.get(), tm_.get());
  }

  void TearDown() override {
    LogCleanup();
  }

  void LogCleanup() {
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

  RowPosition InsertRow(int num, std::string_view str) {
    auto txn = tm_->Begin();
    Schema s = c_->GetSchema(txn, kTableName);
    Row r;
    r.SetValue(s, 0, Value(num));
    r.SetValue(s, 1, Value(str.data()));
    uint64_t data_pid = s.RowPage();
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(data_pid));
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(p->Type(), PageType::kFixedLengthRow);

    RowPosition pos;
    EXPECT_TRUE(p->Insert(txn, r, s, pos));
    p_->Unpin(p->PageId());
    EXPECT_TRUE(txn.PreCommit());
    txn.CommitWait();
    return pos;
  }
  void UpdateRow(RowPosition pos, int num, std::string_view str) {
    auto txn = tm_->Begin();
    Schema s = c_->GetSchema(txn, kTableName);
    Row r;
    r.SetValue(s, 0, Value(num));
    r.SetValue(s, 1, Value(str.data()));
    uint64_t data_pid = s.RowPage();
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(data_pid));
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(p->Type(), PageType::kFixedLengthRow);

    EXPECT_TRUE(p->Update(txn, pos, r, s));
    p_->Unpin(p->PageId());
    EXPECT_TRUE(txn.PreCommit());
    txn.CommitWait();
  }

  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<Catalog> c_;
  std::unique_ptr<Recovery> r_;
};

TEST_F(RecoveryTest, SchemaRecovery) { r_->StartFrom(0); }
TEST_F(RecoveryTest, InsertRowRecovery) {
  InsertRow(0, "hoge");
  r_->StartFrom(0);

}
TEST_F(RecoveryTest, UpdateRowRecovery) {
  RowPosition pos = InsertRow(0, "hoge");
  UpdateRow(pos, 1, "bar");
  r_->StartFrom(0);
}

}  // namespace tinylamb