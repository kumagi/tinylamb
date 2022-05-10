//
// Created by kumagi on 2022/02/21.
//

#include "index_scan_iterator.hpp"

#include <memory>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "gtest/gtest.h"
#include "index.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/recovery_manager.hpp"
#include "table/table.hpp"
#include "transaction/lock_manager.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class IndexScanIteratorTest : public ::testing::Test {
 public:
  static constexpr char kTableName[] = "SampleTable";
  void SetUp() override {
    prefix_ = "index_scan_iterator_test-" + RandomString();
    Recover();
    Schema sc(kTableName, {Column("col1", ValueType::kInt64,
                                  Constraint(Constraint::kIndex)),
                           Column("col2", ValueType::kVarChar),
                           Column("col3", ValueType::kDouble)});
    Transaction txn = rs_->Begin();
    ASSERT_SUCCESS(rs_->CreateTable(txn, sc));
    IndexSchema is("idx", {0});
    ASSERT_SUCCESS(rs_->CreateIndex(txn, kTableName, is));
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->GetTable(txn, kTableName));
    ASSERT_EQ(table.IndexCount(), 1);
    ASSERT_SUCCESS(txn.PreCommit());
  }

  void Recover() {
    if (rs_) {
      rs_->GetPageStorage()->LostAllPageForTest();
    }
    rs_ = std::make_unique<RelationStorage>(prefix_);
  }

  void TearDown() override {
    std::remove(rs_->GetPageStorage()->DBName().c_str());
    std::remove(rs_->GetPageStorage()->LogName().c_str());
    std::remove(rs_->GetPageStorage()->MasterRecordName().c_str());
  }

  std::string prefix_;
  std::unique_ptr<RelationStorage> rs_;
};

TEST_F(IndexScanIteratorTest, Construct) {}

TEST_F(IndexScanIteratorTest, ScanAscending) {
  Transaction txn = rs_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->GetTable(txn, kTableName));
  for (int i = 0; i < 230; ++i) {
    ASSERT_SUCCESS(
        table
            .Insert(txn, Row({Value(i), Value("v" + std::to_string(i)),
                              Value(0.1 + i)}))
            .GetStatus());
  }
  Row iter_begin({Value(43)});
  Row iter_end({Value(180)});
  Iterator it = table.BeginIndexScan(txn, "idx", iter_begin, iter_end);
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
  Transaction txn = rs_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->GetTable(txn, kTableName));
  RowPosition rp;
  for (int i = 0; i < 230; ++i) {
    ASSERT_SUCCESS(
        table
            .Insert(txn, Row({Value(i), Value("v" + std::to_string(i)),
                              Value(0.1 + i)}))
            .GetStatus());
  }
  Row iter_begin({Value(104)});
  Row iter_end({Value(200)});
  Iterator it = table.BeginIndexScan(txn, "idx", iter_begin, iter_end, false);
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
