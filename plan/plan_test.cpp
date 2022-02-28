//
// Created by kumagi on 2022/03/01.
//
#include "common/test_util.hpp"
#include "database/catalog.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "gtest/gtest.h"
#include "plan/full_scan_plan.hpp"
#include "plan/plan_base.hpp"
#include "plan/projection_plan.hpp"
#include "plan/selection_plan.hpp"
#include "product_plan.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "table/table.hpp"
#include "table/table_interface.hpp"
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
    {
      ASSERT_SUCCESS(catalog_->CreateTable(
          txn, Schema("Sc1", {Column("c1", ValueType::kInt64),
                              Column("c2", ValueType::kVarChar),
                              Column("c3", ValueType::kDouble)})));
      Schema sc;
      Table tbl(p_.get());
      ASSERT_SUCCESS(catalog_->GetTable(txn, "Sc1", &sc, &tbl));
      RowPosition rp;
      tbl.Insert(txn, Row({Value(12), Value("hello"), Value(2.3)}), &rp);
      tbl.Insert(txn, Row({Value(10), Value("world"), Value(4.5)}), &rp);
      tbl.Insert(txn, Row({Value(52), Value("ought"), Value(5.3)}), &rp);
      tbl.Insert(txn, Row({Value(242), Value("arise"), Value(6.0)}), &rp);
      tbl.Insert(txn, Row({Value(431), Value("vivid"), Value(2.03)}), &rp);
      tbl.Insert(txn, Row({Value(100), Value("aster"), Value(1.2)}), &rp);
    }
    ASSERT_SUCCESS(catalog_->CreateTable(
        txn, Schema("Sc2", {Column("d1", ValueType::kInt64),
                            Column("d2", ValueType::kDouble),
                            Column("d3", ValueType::kVarChar),
                            Column("d4", ValueType::kInt64)})));
    Schema sc;
    Table tbl(p_.get());
    ASSERT_SUCCESS(catalog_->GetTable(txn, "Sc2", &sc, &tbl));
    RowPosition rp;
    tbl.Insert(txn, Row({Value(52), Value(53.4), Value("ou"), Value(16)}), &rp);
    tbl.Insert(txn, Row({Value(242), Value(6.1), Value("ai"), Value(32)}), &rp);
    tbl.Insert(txn, Row({Value(12), Value(5.3), Value("heo"), Value(4)}), &rp);
    tbl.Insert(txn, Row({Value(10), Value(6.5), Value("wld"), Value(8)}), &rp);
    tbl.Insert(txn, Row({Value(33), Value(2.5), Value("vid"), Value(64)}), &rp);
    tbl.Insert(txn, Row({Value(1), Value(7.2), Value("aer"), Value(128)}), &rp);
    ASSERT_SUCCESS(catalog_->CreateTable(
        txn, Schema("Sc3", {Column("e1", ValueType::kInt64),
                            Column("e2", ValueType::kDouble)})));
    txn.PreCommit();
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
    catalog_ = std::make_unique<Catalog>(1, p_.get());
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
};

TEST_F(PlanTest, Construct) {}

TEST_F(PlanTest, ScanPlan) {
  auto txn = tm_->Begin();
  FullScanPlan fs(txn, "Sc1", catalog_.get(), p_.get());
  std::unique_ptr<ExecutorBase> scan = fs.EmitExecutor(txn);
  Row result;
  while (scan->Next(&result)) {
    LOG(TRACE) << result;
  }
}

TEST_F(PlanTest, ProjectPlan) {
  auto txn = tm_->Begin();
  ProjectionPlan pp(
      std::make_unique<FullScanPlan>(txn, "Sc1", catalog_.get(), p_.get()),
      {1});
  std::unique_ptr<ExecutorBase> scan = pp.EmitExecutor(txn);
  Row result;
  while (scan->Next(&result)) {
    LOG(TRACE) << result;
  }
}

TEST_F(PlanTest, SelectionPlan) {
  auto txn = tm_->Begin();
  std::shared_ptr<ExpressionBase> exp(new BinaryExpression(
      std::make_unique<ColumnValue>("c1"),
      std::make_unique<ConstantValue>(Value(100)), OpType::kGreaterThanEquals));
  SelectionPlan sp(
      std::make_unique<FullScanPlan>(txn, "Sc1", catalog_.get(), p_.get()),
      exp);
  std::unique_ptr<ExecutorBase> scan = sp.EmitExecutor(txn);
  Row result;
  while (scan->Next(&result)) {
    LOG(TRACE) << result;
  }
}

TEST_F(PlanTest, ProductPlan) {
  auto txn = tm_->Begin();
  ProductPlan prop(
      std::make_unique<FullScanPlan>(txn, "Sc1", catalog_.get(), p_.get()), {0},
      std::make_unique<FullScanPlan>(txn, "Sc2", catalog_.get(), p_.get()),
      {0});
  std::unique_ptr<ExecutorBase> scan = prop.EmitExecutor(txn);
  Row result;
  while (scan->Next(&result)) {
    LOG(TRACE) << result;
  }
}

}  // namespace tinylamb