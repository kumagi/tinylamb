//
// Created by kumagi on 2022/03/06.
//

#include "table/table_statistics.hpp"

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/relation_storage.hpp"
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
  void SetUp() override {
    prefix_ = "table_statistics_test-" + RandomString();
    Recover();
    TransactionContext ctx = db_->BeginContext();
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          db_->CreateTable(ctx,
                           Schema("Sc1", {Column("c1", ValueType::kInt64),
                                          Column("c2", ValueType::kVarChar),
                                          Column("c3", ValueType::kDouble)})));
      for (int i = 0; i < 100; ++i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_,
                       Row({Value(i), Value("c2-" + std::to_string(i)),
                            Value(i + 9.9)}))
                .GetStatus());
      }
    }
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          db_->CreateTable(ctx,
                           Schema("Sc2", {Column("d1", ValueType::kInt64),
                                          Column("d2", ValueType::kDouble),
                                          Column("d3", ValueType::kVarChar),
                                          Column("d4", ValueType::kInt64)})));
      for (int i = 0; i < 20; ++i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_,
                       Row({Value(i), Value(i + 0.2),
                            Value("d3-" + std::to_string(i)), Value(16)}))
                .GetStatus());
      }
    }
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          db_->CreateTable(ctx,
                           Schema("Sc3", {Column("e1", ValueType::kInt64),
                                          Column("e2", ValueType::kDouble)})));
      for (int i = 10; 0 < i; --i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_, Row({Value(i), Value(i + 53.4)})).GetStatus());
      }
    }
    ctx.txn_.PreCommit();
    {
      TransactionContext stat_tx = db_->BeginContext();
      db_->RefreshStatistics(stat_tx, "Sc1");
      db_->RefreshStatistics(stat_tx, "Sc2");
      db_->RefreshStatistics(stat_tx, "Sc3");
      ASSERT_SUCCESS(stat_tx.PreCommit());
    }
  }
  void Recover() {
    if (db_) {
      db_->Storage().LostAllPageForTest();
    }
    db_ = std::make_unique<Database>(prefix_);
  }

  void TearDown() override {
    std::remove(db_->Storage().DBName().c_str());
    std::remove(db_->Storage().LogName().c_str());
  }

  std::string prefix_;
  std::unique_ptr<Database> db_;
};

TEST_F(TableStatisticsTest, Construct) {}

TEST_F(TableStatisticsTest, Update) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, db_->GetTable(ctx, "Sc1"));
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, ts, db_->GetStatistics(ctx, "Sc1"));
  ts.Update(ctx.txn_, tbl);
  LOG(TRACE) << ts;
}

TEST_F(TableStatisticsTest, Store) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, db_->GetTable(ctx, "Sc1"));
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, ts, db_->GetStatistics(ctx, "Sc1"));
  ts.Update(ctx.txn_, tbl);
  LOG(TRACE) << ts;
  db_->UpdateStatistics(ctx, "Sc2", ts);
}

}  // namespace tinylamb