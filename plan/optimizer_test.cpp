//
// Created by kumagi on 2022/03/04.
//
#include "plan/optimizer.hpp"

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/catalog.hpp"
#include "database/query_data.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor.hpp"
#include "expression/expression.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "table/table.hpp"
#include "table/table_interface.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"

namespace tinylamb {

class OptimizerTest : public ::testing::Test {
 public:
  void SetUp() override {
    std::string prefix = "optimizer_test-" + RandomString();
    db_name_ = prefix + ".db";
    log_name_ = prefix + ".log";
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
    ASSERT_SUCCESS(txn.PreCommit());
    auto stat_tx = tm_->Begin();
    catalog_->RefreshStatistics(stat_tx, "Sc1");
    catalog_->RefreshStatistics(stat_tx, "Sc2");
    catalog_->RefreshStatistics(stat_tx, "Sc3");
    ASSERT_SUCCESS(stat_tx.PreCommit());
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
    p_ = std::make_unique<PageManager>(db_name_, 10);
    l_ = std::make_unique<Logger>(log_name_);
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(log_name_, p_->GetPool());
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
    std::remove(db_name_.c_str());
    std::remove(log_name_.c_str());
  }

  std::string db_name_;
  std::string log_name_;
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<TransactionContext> ctx_;
};

TEST_F(OptimizerTest, Construct) {}

void DumpAll(TransactionContext& ctx, const QueryData& qd) {
  std::cout << qd << "\n\n";
  Optimizer opt;
  Schema sc;
  Executor exec;
  opt.Optimize(qd, ctx, sc, exec);
  exec->Dump(std::cout, 0);
  std::cout << "\n\n" << sc << "\n";
  Row result;
  while (exec->Next(&result, nullptr)) {
    std::cout << result << "\n";
  }
}

TEST_F(OptimizerTest, Simple) {
  QueryData qd{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(2))),
      {NamedExpression("c1"), NamedExpression("Column2Varchar", "c2")}};
  DumpAll(*ctx_, qd);
}

TEST_F(OptimizerTest, Join) {
  QueryData qd{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ColumnValueExp("d1")),
      {NamedExpression("c2"), NamedExpression("d1"), NamedExpression("d3")}};
  DumpAll(*ctx_, qd);
}

TEST_F(OptimizerTest, ThreeJoin) {
  QueryData qd{
      {"Sc1", "Sc2", "Sc3"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                              ColumnValueExp("d1")),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("d1"), BinaryOperation::kEquals,
                              ColumnValueExp("e1"))),

      {NamedExpression("Sc1-c2", "c2"), NamedExpression("Sc2-e1", "e1"),
       NamedExpression("Sc3-d3", "d3"),
       NamedExpression(
           "e1+100",
           BinaryExpressionExp(ConstantValueExp(Value(100)),
                               BinaryOperation::kAdd, ColumnValueExp("e1")))}};
  DumpAll(*ctx_, qd);
}

TEST_F(OptimizerTest, JoinWhere) {
  QueryData qd{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                              ColumnValueExp("d1")),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(2)))),
      {NamedExpression("c1"), NamedExpression("c2"), NamedExpression("d1"),
       NamedExpression("d2"), NamedExpression("d3")}};
  DumpAll(*ctx_, qd);
}

}  // namespace tinylamb