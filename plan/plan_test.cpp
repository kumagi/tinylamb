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
    Recover();
    TransactionContext ctx = rs_->BeginContext();
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          rs_->CreateTable(ctx,
                           Schema("Sc1", {Column("c1", ValueType::kInt64),
                                          Column("c2", ValueType::kVarChar),
                                          Column("c3", ValueType::kDouble)})));
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(12), Value("hello"), Value(2.3)}))
              .GetStatus());
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(10), Value("world"), Value(4.5)}))
              .GetStatus());
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(52), Value("ought"), Value(5.3)}))
              .GetStatus());
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(242), Value("arise"), Value(6.0)}))
              .GetStatus());
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(431), Value("vivid"), Value(2.03)}))
              .GetStatus());
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(100), Value("aster"), Value(1.2)}))
              .GetStatus());
    }
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          rs_->CreateTable(ctx,
                           Schema("Sc2", {Column("d1", ValueType::kInt64),
                                          Column("d2", ValueType::kDouble),
                                          Column("d3", ValueType::kVarChar),
                                          Column("d4", ValueType::kInt64)})));
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(52), Value(53.4),
                                               Value("ou"), Value(16)}))
                         .GetStatus());
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(242), Value(6.1),
                                               Value("ai"), Value(32)}))
                         .GetStatus());
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(12), Value(5.3),
                                               Value("heo"), Value(4)}))
                         .GetStatus());
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(10), Value(6.5),
                                               Value("wld"), Value(8)}))
                         .GetStatus());
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(33), Value(2.5),
                                               Value("vid"), Value(64)}))
                         .GetStatus());
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(1), Value(7.2),
                                               Value("aer"), Value(128)}))
                         .GetStatus());
    }
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          rs_->CreateTable(ctx,
                           Schema("Sc3", {Column("e1", ValueType::kInt64),
                                          Column("e2", ValueType::kDouble)})));
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(52), Value(53.4),
                                               Value("ou"), Value(16)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
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
  }

  void DumpAll(const Plan& plan) const {
    plan->Dump(std::cout, 0);
    std::cout << "\n";
    TransactionContext ctx = rs_->BeginContext();
    Executor scan = plan->EmitExecutor(ctx);
    Row result;
    std::cout << plan->GetSchema() << "\n";
    while (scan->Next(&result, nullptr)) {
      std::cout << result << "\n";
    }
  }

  std::string prefix_;
  std::unique_ptr<RelationStorage> rs_;
};

TEST_F(PlanTest, Construct) {}

TEST_F(PlanTest, ScanPlan) {
  TableStatistics ts((Schema()));
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable("Sc1"));
  Plan fs = NewFullScanPlan(*tbl, ts);
  DumpAll(fs);
}

TEST_F(PlanTest, ProjectPlan) {
  TableStatistics ts((Schema()));
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable("Sc1"));
  Plan pp =
      NewProjectionPlan(NewFullScanPlan(*tbl, ts), {NamedExpression("c1")});
  DumpAll(pp);
}

TEST_F(PlanTest, SelectionPlan) {
  TableStatistics ts((Schema()));
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable("Sc1"));
  Expression exp = BinaryExpressionExp(ColumnValueExp("c1"),
                                       BinaryOperation::kGreaterThanEquals,
                                       ConstantValueExp(Value(100)));
  Plan sp = NewSelectionPlan(NewFullScanPlan(*tbl, ts), exp, ts);
  DumpAll(sp);
}

TEST_F(PlanTest, ProductPlan) {
  TableStatistics ts((Schema()));
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl1, ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl2, ctx.GetTable("Sc2"));
  Plan prop = NewProductPlan(NewFullScanPlan(*tbl1, ts), {0},
                             NewFullScanPlan(*tbl2, ts), {0});
  DumpAll(prop);
}

TEST_F(PlanTest, ProductPlanCrossJoin) {
  TableStatistics ts((Schema()));
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl1, ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl2, ctx.GetTable("Sc2"));
  Plan prop =
      NewProductPlan(NewFullScanPlan(*tbl1, ts), NewFullScanPlan(*tbl2, ts));
  DumpAll(prop);
}

}  // namespace tinylamb
