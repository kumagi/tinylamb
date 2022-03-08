//
// Created by kumagi on 2022/03/01.
//
#include "plan/plan.hpp"

#include "common/test_util.hpp"
#include "database/catalog.hpp"
#include "database/transaction_context.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "table/table.hpp"
#include "table/table_interface.hpp"
#include "table/table_statistics.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"

namespace tinylamb {

class PlanTest : public ::testing::Test {
 public:
  static constexpr char kDBFileName[] = "plan_test.db";
  static constexpr char kLogName[] = "plan_test.log";
  static constexpr char kMasterRecordName[] = "plan_master.log";
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
      Schema sc;
      Table tbl(p_.get());
      ASSERT_SUCCESS(catalog_->GetTable(txn, "Sc1", &tbl));
      RowPosition rp;
      tbl.Insert(txn, Row({Value(12), Value("hello"), Value(2.3)}), &rp);
      tbl.Insert(txn, Row({Value(10), Value("world"), Value(4.5)}), &rp);
      tbl.Insert(txn, Row({Value(52), Value("ought"), Value(5.3)}), &rp);
      tbl.Insert(txn, Row({Value(242), Value("arise"), Value(6.0)}), &rp);
      tbl.Insert(txn, Row({Value(431), Value("vivid"), Value(2.03)}), &rp);
      tbl.Insert(txn, Row({Value(100), Value("aster"), Value(1.2)}), &rp);
    }
    {
      ASSERT_SUCCESS(catalog_->CreateTable(
          txn, Schema("Sc2", {Column("d1", ValueType::kInt64),
                              Column("d2", ValueType::kDouble),
                              Column("d3", ValueType::kVarChar),
                              Column("d4", ValueType::kInt64)})));
      Schema sc;
      Table tbl(p_.get());
      ASSERT_SUCCESS(catalog_->GetTable(txn, "Sc2", &tbl));
      RowPosition rp;
      tbl.Insert(txn, Row({Value(52), Value(53.4), Value("ou"), Value(16)}),
                 &rp);
      tbl.Insert(txn, Row({Value(242), Value(6.1), Value("ai"), Value(32)}),
                 &rp);
      tbl.Insert(txn, Row({Value(12), Value(5.3), Value("heo"), Value(4)}),
                 &rp);
      tbl.Insert(txn, Row({Value(10), Value(6.5), Value("wld"), Value(8)}),
                 &rp);
      tbl.Insert(txn, Row({Value(33), Value(2.5), Value("vid"), Value(64)}),
                 &rp);
      tbl.Insert(txn, Row({Value(1), Value(7.2), Value("aer"), Value(128)}),
                 &rp);
    }
    {
      ASSERT_SUCCESS(catalog_->CreateTable(
          txn, Schema("Sc3", {Column("e1", ValueType::kInt64),
                              Column("e2", ValueType::kDouble)})));
      Schema sc;
      Table tbl(p_.get());
      ASSERT_SUCCESS(catalog_->GetTable(txn, "Sc2", &tbl));
      RowPosition rp;
      tbl.Insert(txn, Row({Value(52), Value(53.4), Value("ou"), Value(16)}),
                 &rp);
    }
    ASSERT_SUCCESS(txn.PreCommit());
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
    cm_ = std::make_unique<CheckpointManager>(kMasterRecordName, tm_.get(),
                                              p_->GetPool(), 1);
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
  std::unique_ptr<CheckpointManager> cm_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<TransactionContext> ctx_;
};

TEST_F(PlanTest, Construct) {}

void DumpAll(TransactionContext& ctx, Plan& plan) {
  plan.Dump(std::cout, 0);
  std::cout << "\n";
  std::unique_ptr<ExecutorBase> scan = plan.EmitExecutor(ctx);
  Row result;
  std::cout << plan.GetSchema(ctx) << "\n";
  while (scan->Next(&result)) {
    std::cout << result << "\n";
  }
}

TEST_F(PlanTest, ScanPlan) {
  TableStatistics ts((Schema()));
  Plan fs = Plan::FullScan("Sc1", ts);
  DumpAll(*ctx_, fs);
}

TEST_F(PlanTest, ProjectPlan) {
  TableStatistics ts((Schema()));
  Plan pp =
      Plan::Projection(Plan::FullScan("Sc1", ts), {NamedExpression("c1")});
  DumpAll(*ctx_, pp);
}

TEST_F(PlanTest, SelectionPlan) {
  TableStatistics ts((Schema()));
  Expression exp = Expression::BinaryExpression(
      Expression::ColumnValue("c1"), BinaryOperation::kGreaterThanEquals,
      Expression::ConstantValue(Value(100)));
  Plan sp = Plan::Selection(Plan::FullScan("Sc1", ts), std::move(exp), ts);
  DumpAll(*ctx_, sp);
}

TEST_F(PlanTest, ProductPlan) {
  TableStatistics ts((Schema()));
  Plan prop = Plan::Product(Plan::FullScan("Sc1", ts), {0},
                            Plan::FullScan("Sc2", ts), {0});
  DumpAll(*ctx_, prop);
}

TEST_F(PlanTest, ProductPlanCrossJoin) {
  TableStatistics ts((Schema()));
  Plan prop =
      Plan::Product(Plan::FullScan("Sc1", ts), Plan::FullScan("Sc2", ts));
  DumpAll(*ctx_, prop);
}

}  // namespace tinylamb