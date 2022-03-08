//
// Created by kumagi on 2022/03/06.
//

#include "table/table_statistics.hpp"

#include "common/test_util.hpp"
#include "database/catalog.hpp"
#include "database/transaction_context.hpp"
#include "expression/constant_value.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "table/table.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"

namespace tinylamb {

class TableStatisticsTest : public ::testing::Test {
 public:
  static constexpr char kDBFileName[] = "table_statistics_test.db";
  static constexpr char kLogName[] = "table_statistics_test.log";
  void SetUp() override {
    Recover();
    Transaction txn = tm_->Begin();
    catalog_->InitializeIfNeeded(txn);
    ctx_ = std::make_unique<TransactionContext>(tm_.get(), p_.get(),
                                                catalog_.get());
    {
      ASSERT_SUCCESS(catalog_->CreateTable(
          txn, Schema("Sc1", {Column("c1", ValueType::kInt64),
                              Column("c2", ValueType::kVarChar),
                              Column("c3", ValueType::kDouble)})));
      Table tbl(p_.get());
      ASSERT_SUCCESS(catalog_->GetTable(txn, "Sc1", &tbl));
      RowPosition rp;
      for (int i = 0; i < 100; ++i) {
        tbl.Insert(
            txn,
            Row({Value(i), Value("c2-" + std::to_string(i)), Value(i + 9.9)}),
            &rp);
      }
    }
    {
      ASSERT_SUCCESS(catalog_->CreateTable(
          txn, Schema("Sc2", {Column("d1", ValueType::kInt64),
                              Column("d2", ValueType::kDouble),
                              Column("d3", ValueType::kVarChar),
                              Column("d4", ValueType::kInt64)})));
      Table tbl(p_.get());
      ASSERT_SUCCESS(catalog_->GetTable(txn, "Sc2", &tbl));
      RowPosition rp;
      for (int i = 0; i < 20; ++i) {
        tbl.Insert(txn,
                   Row({Value(i), Value(i + 0.2),
                        Value("d3-" + std::to_string(i)), Value(16)}),
                   &rp);
      }
    }
    {
      ASSERT_SUCCESS(catalog_->CreateTable(
          txn, Schema("Sc3", {Column("e1", ValueType::kInt64),
                              Column("e2", ValueType::kDouble)})));
      Table tbl(p_.get());
      ASSERT_SUCCESS(catalog_->GetTable(txn, "Sc3", &tbl));
      RowPosition rp;
      for (int i = 10; 0 < i; --i) {
        tbl.Insert(txn, Row({Value(i), Value(i + 53.4)}), &rp);
      }
    }
    txn.PreCommit();
    {
      auto stat_tx = tm_->Begin();
      catalog_->RefreshStatistics(stat_tx, "Sc1");
      catalog_->RefreshStatistics(stat_tx, "Sc2");
      catalog_->RefreshStatistics(stat_tx, "Sc3");
      ASSERT_SUCCESS(stat_tx.PreCommit());
    }
  }
  void Recover() {
    if (p_) {
      p_->GetPool()->LostAllPageForTest();
    }
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
    catalog_ = std::make_unique<Catalog>(1, 2, p_.get());
  }

  void TearDown() override {
    catalog_.reset();
    tm_.reset();
    r_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    std::remove(kDBFileName);
    std::remove(kLogName);
  }

  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<TransactionContext> ctx_;
};

TEST_F(TableStatisticsTest, Construct) {}

TEST_F(TableStatisticsTest, Update) {
  auto txn = tm_->Begin();
  Table tbl(p_.get());
  ASSERT_SUCCESS(catalog_->GetTable(txn, "Sc1", &tbl));
  TableStatistics ts(tbl.GetSchema());
  catalog_->GetStatistics(txn, "Sc1", &ts);
  ts.Update(txn, tbl);
  LOG(TRACE) << ts;
}

TEST_F(TableStatisticsTest, Store) {
  auto txn = tm_->Begin();
  Table tbl(p_.get());
  ASSERT_SUCCESS(catalog_->GetTable(txn, "Sc2", &tbl));
  TableStatistics ts(tbl.GetSchema());
  catalog_->GetStatistics(txn, "Sc2", &ts);
  ts.Update(txn, tbl);
  LOG(TRACE) << ts;
  catalog_->UpdateStatistics(txn, "Sc2", ts);
}

}  // namespace tinylamb