//
// Created by kumagi on 2022/03/01.
//
#include "plan/plan.hpp"

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/page_storage.hpp"
#include "database/relation_storage.hpp"
#include "database/transaction_context.hpp"
#include "expression/expression.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"

namespace tinylamb {

class PlanTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "plan_test-" + RandomString();
    ts_ = std::make_unique<PageStorage>(prefix_);
    Recover();
    Transaction txn = ts_->Begin();
    ctx_ = std::make_unique<TransactionContext>(ts_->Begin(), catalog_.get());
    {
      ASSERT_SUCCESS(catalog_->CreateTable(
          txn, Schema("Sc1", {Column("c1", ValueType::kInt64),
                              Column("c2", ValueType::kVarChar),
                              Column("c3", ValueType::kDouble)})));
      Schema sc;
      Table tbl;
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
      Table tbl;
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
      Table tbl;
      ASSERT_SUCCESS(catalog_->GetTable(txn, "Sc2", &tbl));
      RowPosition rp;
      tbl.Insert(txn, Row({Value(52), Value(53.4), Value("ou"), Value(16)}),
                 &rp);
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }

  void Recover() {
    ts_->LostAllPageForTest();
    ts_ = std::make_unique<PageStorage>(prefix_);
    catalog_ = std::make_unique<RelationStorage>(1, 2, ts_.get());
  }

  void TearDown() override {
    catalog_.reset();
    std::remove(ts_->DBName().c_str());
    std::remove(ts_->LogName().c_str());
    ts_.reset();
  }

  void DumpAll(const Plan& plan) {
    plan->Dump(std::cout, 0);
    std::cout << "\n";
    TransactionContext ctx{ts_->Begin(), catalog_.get()};
    Executor scan = plan->EmitExecutor(ctx);
    Row result;
    std::cout << plan->GetSchema(ctx) << "\n";
    while (scan->Next(&result, nullptr)) {
      std::cout << result << "\n";
    }
  }

  std::string prefix_;
  std::unique_ptr<PageStorage> ts_;
  std::unique_ptr<RelationStorage> catalog_;
  std::unique_ptr<TransactionContext> ctx_;
};

TEST_F(PlanTest, Construct) {}

TEST_F(PlanTest, ScanPlan) {
  TableStatistics ts((Schema()));
  Plan fs = NewFullScanPlan("Sc1", ts);
  DumpAll(fs);
}

TEST_F(PlanTest, ProjectPlan) {
  TableStatistics ts((Schema()));
  Plan pp =
      NewProjectionPlan(NewFullScanPlan("Sc1", ts), {NamedExpression("c1")});
  DumpAll(pp);
}

TEST_F(PlanTest, SelectionPlan) {
  TableStatistics ts((Schema()));
  Expression exp = BinaryExpressionExp(ColumnValueExp("c1"),
                                       BinaryOperation::kGreaterThanEquals,
                                       ConstantValueExp(Value(100)));
  Plan sp = NewSelectionPlan(NewFullScanPlan("Sc1", ts), exp, ts);
  DumpAll(sp);
}

TEST_F(PlanTest, ProductPlan) {
  TableStatistics ts((Schema()));
  Plan prop = NewProductPlan(NewFullScanPlan("Sc1", ts), {0},
                             NewFullScanPlan("Sc2", ts), {0});
  DumpAll(prop);
}

TEST_F(PlanTest, ProductPlanCrossJoin) {
  TableStatistics ts((Schema()));
  Plan prop =
      NewProductPlan(NewFullScanPlan("Sc1", ts), NewFullScanPlan("Sc2", ts));
  DumpAll(prop);
}

}  // namespace tinylamb
