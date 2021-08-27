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
#include "type/column.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class SingleSchemaRowPageTest : public ::testing::Test {
 protected:
  static constexpr char kDBFileName[] = "row_page_test.db";
  static constexpr char kLogName[] = "row_page_test.log";
  static constexpr char kTableName[] = "test-table_for_row_page";

  void SetUp() override {
    Recover();
    std::vector<Column> columns = {
        Column("int_column", ValueType::kInt64, 8, Restriction::kNoRestriction,
               0),
        Column("varchar_column", ValueType::kVarChar, 16,
               Restriction::kNoRestriction, 8),
    };
    Schema sc = Schema(kTableName, columns, 2);
    auto txn = tm_->Begin();
    ASSERT_TRUE(c_->CreateTable(txn, sc));
    ASSERT_EQ(c_->Schemas(), 1);
    txn.PreCommit();
    txn.CommitWait();
    ASSERT_EQ(1, c_->Schemas());
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

  void InsertRow(int num, std::string_view str) {
    auto txn = tm_->Begin();
    Schema s = c_->GetSchema(txn, kTableName);
    Row r;
    r.SetValue(s, 0, Value(num));
    r.SetValue(s, 1, Value(str.data()));
    uint64_t data_pid = s.RowPage();
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(data_pid));
    ASSERT_NE(p, nullptr);
    ASSERT_EQ(p->Type(), PageType::kFixedLengthRow);

    RowPosition pos;
    ASSERT_TRUE(p->Insert(txn, r, s, pos));
    p_->Unpin(p->PageId());
    ASSERT_TRUE(txn.PreCommit());
    txn.CommitWait();
  }

  size_t GetRowCount() {
    auto txn = tm_->Begin();
    Schema s = c_->GetSchema(txn, kTableName);
    uint64_t data_pid = s.RowPage();
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(data_pid));
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(p->Type(), PageType::kFixedLengthRow);
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
};

TEST_F(SingleSchemaRowPageTest, Insert) {
  auto txn = tm_->Begin();
  Schema s = c_->GetSchema(txn, kTableName);
  Row r;
  r.SetValue(s, 0, Value(23));
  r.SetValue(s, 1, Value("hello"));
  uint64_t data_pid = s.RowPage();
  auto* p = reinterpret_cast<RowPage*>(p_->GetPage(data_pid));
  ASSERT_NE(p, nullptr);
  ASSERT_EQ(p->Type(), PageType::kFixedLengthRow);

  RowPosition pos;
  p_->Unpin(p->PageId());
  ASSERT_TRUE(p->Insert(txn, r, s, pos));
  ASSERT_TRUE(txn.PreCommit());
  txn.CommitWait();
}

TEST_F(SingleSchemaRowPageTest, InsertMany) {
  constexpr int kInserts = 32;
  auto txn = tm_->Begin();
  Schema s = c_->GetSchema(txn, kTableName);
  Row r;
  r.SetValue(s, 0, Value(23));
  r.SetValue(s, 1, Value("hello"));
  uint64_t data_pid = s.RowPage();
  auto* p = reinterpret_cast<RowPage*>(p_->GetPage(data_pid));
  ASSERT_NE(p, nullptr);
  ASSERT_EQ(p->Type(), PageType::kFixedLengthRow);
  for (int i = 0; i < kInserts; ++i) {
    RowPosition pos;
    r.SetValue(s, 0, i);
    ASSERT_TRUE(p->Insert(txn, r, s, pos));
  }
  ASSERT_TRUE(txn.PreCommit());
  p_->Unpin(p->PageId());
  txn.CommitWait();
}

TEST_F(SingleSchemaRowPageTest, ReadMany) {
  constexpr char kMessage[] = "this is a pen";
  constexpr int kRows = 100;
  for (int i = 0; i < kRows; ++i) {
    InsertRow(i * i, kMessage);
  }
  Recover();
  auto txn = tm_->Begin();
  Schema s = c_->GetSchema(txn, kTableName);
  uint64_t data_pid = s.RowPage();
  auto* p = reinterpret_cast<RowPage*>(p_->GetPage(data_pid));
  for (int i = 0; i < kRows; ++i) {
    RowPosition pos(p->PageId(), i);
    Row ret;
    ASSERT_TRUE(p->Read(txn, pos, s, ret));
    ASSERT_FALSE(ret.IsOwned());
    Value col1, col2;
    ASSERT_TRUE(ret.GetValue(s, 0, col1));
    ASSERT_TRUE(ret.GetValue(s, 1, col2));
    ASSERT_EQ(col1.value.int_value, i * i);
    ASSERT_EQ(std::string(col2.value.varchar_value), std::string(kMessage));
  }
  p_->Unpin(p->PageId());
}

TEST_F(SingleSchemaRowPageTest, UpdateMany) {
  constexpr char kMessage[] = "this is a pen";
  constexpr int kRows = 100;
  for (int i = 0; i < kRows; ++i) {
    InsertRow(i * i, kMessage);
  }
  Recover();
  {
    // Update even rows.
    auto txn = tm_->Begin();
    Schema s = c_->GetSchema(txn, kTableName);
    uint64_t data_pid = s.RowPage();
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(data_pid));
    for (int i = 0; i < kRows; i += 2) {
      RowPosition pos(p->PageId(), i);
      Row ret;
      ASSERT_TRUE(p->Read(txn, pos, s, ret));
      ASSERT_FALSE(ret.IsOwned());
      Value col1, col2;
      ASSERT_TRUE(ret.GetValue(s, 0, col1));
      ASSERT_TRUE(ret.GetValue(s, 1, col2));
      col1.value.int_value += 100;
      ret.SetValue(s, 0, col1);
      p->Update(txn, pos, ret);
    }
    p_->Unpin(p->PageId());
    txn.PreCommit();
    txn.CommitWait();
  }
  Recover();
  {
    // Check rows.
    auto txn = tm_->Begin();
    Schema s = c_->GetSchema(txn, kTableName);
    uint64_t data_pid = s.RowPage();
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(data_pid));
    for (int i = 0; i < kRows; ++i) {
      RowPosition pos(p->PageId(), i);
      Row ret;
      ASSERT_TRUE(p->Read(txn, pos, s, ret));
      ASSERT_FALSE(ret.IsOwned());
      Value col1, col2;
      ASSERT_TRUE(ret.GetValue(s, 0, col1));
      ASSERT_TRUE(ret.GetValue(s, 1, col2));
      if (i % 2 == 0) {
        ASSERT_EQ(col1.value.int_value, 100 + i * i);
      } else {
        ASSERT_EQ(col1.value.int_value, i * i);
      }
    }
    p_->Unpin(p->PageId());
    txn.PreCommit();
    txn.CommitWait();
  }
}

TEST_F(SingleSchemaRowPageTest, DeleteMany) {
  constexpr char kMessage[] = "this is a pen";
  constexpr int kRows = 100;
  std::unordered_set<int64_t> inserted;
  for (int i = 0; i < kRows; ++i) {
    InsertRow(i, kMessage);
    inserted.insert(i);
  }
  Recover();
  {
    auto txn = tm_->Begin();
    Schema s = c_->GetSchema(txn, kTableName);
    uint64_t data_pid = s.RowPage();
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(data_pid));
    for (int i = 0; i < kRows / 3; i += 2) {
      RowPosition pos(p->PageId(), i);
      Row ret;
      ASSERT_TRUE(p->Read(txn, pos, s, ret));
      Value col1;
      ASSERT_TRUE(ret.GetValue(s, 0, col1));
      auto it = inserted.find(col1.value.int_value);
      ASSERT_NE(it, inserted.end());
      inserted.erase(it);
      ASSERT_TRUE(p->Delete(txn, pos, s));
    }
    p_->Unpin(p->PageId());
    txn.PreCommit();
    txn.CommitWait();
  }
  Recover();
  {
    auto txn = tm_->Begin();
    Schema s = c_->GetSchema(txn, kTableName);
    uint64_t data_pid = s.RowPage();
    auto* p = reinterpret_cast<RowPage*>(p_->GetPage(data_pid));
    std::unordered_set<int64_t> existing;
    for (size_t i = 0; i < p->RowCount(); ++i) {
      RowPosition pos(p->PageId(), i);
      Row ret;
      ASSERT_TRUE(p->Read(txn, pos, s, ret));
      Value col1;
      ASSERT_TRUE(ret.GetValue(s, 0, col1));
      existing.insert(col1.value.int_value);
    }
    ASSERT_EQ(inserted, existing);
    p_->Unpin(p->PageId());
    txn.PreCommit();
    txn.CommitWait();
  }
}

TEST_F(SingleSchemaRowPageTest, InsertAbort) {
  ASSERT_EQ(GetRowCount(), 0);
  auto txn = tm_->Begin();
  Schema s = c_->GetSchema(txn, kTableName);
  Row r;
  r.SetValue(s, 0, Value(23));
  r.SetValue(s, 1, Value("hello"));
  uint64_t data_pid = s.RowPage();
  auto* p = reinterpret_cast<RowPage*>(p_->GetPage(data_pid));
  ASSERT_EQ(p->RowCount(), 1);
  ASSERT_NE(p, nullptr);
  ASSERT_EQ(p->Type(), PageType::kFixedLengthRow);

  RowPosition pos;
  p_->Unpin(p->PageId());
  ASSERT_TRUE(p->Insert(txn, r, s, pos));
  txn.Abort();
  txn.CommitWait();
  ASSERT_EQ(GetRowCount(), 0);
}

}  // namespace tinylamb