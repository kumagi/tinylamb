//
// Created by kumagi on 2022/02/21.
//

#include "index_scan_iterator.hpp"

#include <memory>

#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "table/index.hpp"
#include "table/table.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class IndexScanIteratorTest : public ::testing::Test {
 protected:
  static constexpr char kDBFileName[] = "index_scan_iterator_test.db";
  static constexpr char kLogName[] = "index_scan_iterator_test.log";
  static constexpr char kMasterRecordName[] = "index_scan_iterator_master.log";

 public:
  void SetUp() override {
    schema_ = Schema("SampleTable", {Column("col1", ValueType::kInt64,
                                            Constraint(Constraint::kIndex)),
                                     Column("col2", ValueType::kVarChar),
                                     Column("col3", ValueType::kDouble)});
    Recover();
    Transaction txn = tm_->Begin();
    PageRef row_page = p_->AllocateNewPage(txn, PageType::kRowPage);
    PageRef leaf_page = p_->AllocateNewPage(txn, PageType::kLeafPage);
    std::vector<Index> ind;
    ind.push_back(Index("bt", {0LLU}, leaf_page->page_id));
    table_ =
        std::make_unique<Table>(p_.get(), schema_, row_page->PageID(), ind);
  }

  void Flush(page_id_t pid) const { p_->GetPool()->FlushPageForTest(pid); }

  void Recover() {
    if (p_) {
      p_->GetPool()->LostAllPageForTest();
    }
    table_.reset();
    tm_.reset();
    r_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(kLogName, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), l_.get(), r_.get());
    cm_ = std::make_unique<CheckpointManager>(kMasterRecordName, tm_.get(),
                                              p_->GetPool(), 1);
  }

  void TearDown() override {
    table_.reset();
    tm_.reset();
    r_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    std::remove(kDBFileName);
    std::remove(kLogName);
  }

 public:
  Schema schema_;
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<CheckpointManager> cm_;
  std::unique_ptr<Table> table_;
};

TEST_F(IndexScanIteratorTest, Construct) {}

TEST_F(IndexScanIteratorTest, ScanAscending) {
  Transaction txn = tm_->Begin();
  RowPosition rp;
  for (int i = 0; i < 230; ++i) {
    ASSERT_SUCCESS(table_->Insert(
        txn, Row({Value(i), Value("v" + std::to_string(i)), Value(0.1 + i)}),
        &rp));
  }
  Row iter_begin({Value(43)});
  Row iter_end({Value(180)});
  IndexScanIterator it =
      table_->BeginIndexScan(txn, "bt", iter_begin, iter_end);
  ASSERT_TRUE(it.IsValid());
  for (int i = 43; i <= 180; ++i) {
    Row cur = *it;
    ASSERT_EQ(cur[0], Value(i));
    ASSERT_EQ(cur[1], Value("v" + std::to_string(i)));
    ASSERT_EQ(cur[2], Value(0.1 + i));
    ++it;
  }
  ASSERT_FALSE(it.IsValid());
}
TEST_F(IndexScanIteratorTest, ScanDecending) {
  Transaction txn = tm_->Begin();
  RowPosition rp;
  for (int i = 0; i < 230; ++i) {
    ASSERT_SUCCESS(table_->Insert(
        txn, Row({Value(i), Value("v" + std::to_string(i)), Value(0.1 + i)}),
        &rp));
  }
  Row iter_begin({Value(104)});
  Row iter_end({Value(200)});
  IndexScanIterator it =
      table_->BeginIndexScan(txn, "bt", iter_begin, iter_end, false);
  ASSERT_TRUE(it.IsValid());
  for (int i = 200; i >= 104; --i) {
    Row cur = *it;
    ASSERT_EQ(cur[0], Value(i));
    ASSERT_EQ(cur[1], Value("v" + std::to_string(i)));
    ASSERT_EQ(cur[2], Value(0.1 + i));
    --it;
  }
  ASSERT_FALSE(it.IsValid());
}

}  // namespace tinylamb
