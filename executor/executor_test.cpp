//
// Created by kumagi on 2022/02/23.
//
#include <memory>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/relation_storage.hpp"
#include "executor/full_scan.hpp"
#include "executor/hash_join.hpp"
#include "executor/projection.hpp"
#include "executor/selection.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "gtest/gtest.h"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

static const char* kTableName = "SampleTable";
class ExecutorTest : public ::testing::Test {
 public:
  static void BulkInsert(Transaction& txn, Table& tbl,
                         std::initializer_list<Row> rows) {
    for (const auto& row : rows) {
      ASSERT_SUCCESS(tbl.Insert(txn, row).GetStatus());
    }
  }

  void SetUp() override {
    prefix_ = "table_test-" + RandomString();
    Recover();
    Schema schema{
        kTableName,
        {Column("key", ValueType::kInt64), Column("name", ValueType::kVarChar),
         Column("score", ValueType::kDouble)}};
    Transaction txn = rs_->Begin();
    rs_->CreateTable(txn, schema);
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(txn, kTableName));
    BulkInsert(txn, tbl,
               {{Row({Value(0), Value("hello"), Value(1.2)})},
                {Row({Value(3), Value("piyo"), Value(12.2)})},
                {Row({Value(1), Value("world"), Value(4.9)})},
                {Row({Value(2), Value("arise"), Value(4.14)})}});
    ASSERT_SUCCESS(txn.PreCommit());
  }

  void Recover() {
    if (rs_) {
      rs_->GetPageStorage()->LostAllPageForTest();
    }
    rs_ = std::make_unique<RelationStorage>(prefix_);
  }

  std::string prefix_;
  std::unique_ptr<RelationStorage> rs_;
};

TEST_F(ExecutorTest, Construct) {}

TEST_F(ExecutorTest, FullScan) {
  Transaction txn = rs_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(txn, kTableName));
  FullScan fs(txn, &tbl);
  std::unordered_set rows({Row({Value(0), Value("hello"), Value(1.2)}),
                           Row({Value(3), Value("piyo"), Value(12.2)}),
                           Row({Value(1), Value("world"), Value(4.9)}),
                           Row({Value(2), Value("arise"), Value(4.14)})});
  fs.Dump(std::cout, 0);
  std::cout << "\n";
  Row got;
  ASSERT_TRUE(fs.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  rows.erase(got);
  ASSERT_TRUE(fs.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  rows.erase(got);
  ASSERT_TRUE(fs.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  rows.erase(got);
  ASSERT_TRUE(fs.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  rows.erase(got);
  ASSERT_TRUE(rows.empty());
  ASSERT_FALSE(fs.Next(&got, nullptr));
}

TEST_F(ExecutorTest, Projection) {
  Transaction txn = rs_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(txn, kTableName));
  auto fs = std::make_unique<FullScan>(txn, &tbl);
  Projection proj({NamedExpression("key"), NamedExpression("score")},
                  tbl.GetSchema(), std::move(fs));
  std::unordered_set rows(
      {Row({Value(0), Value(1.2)}), Row({Value(3), Value(12.2)}),
       Row({Value(1), Value(4.9)}), Row({Value(2), Value(4.14)})});
  proj.Dump(std::cout, 0);
  std::cout << "\n";
  Row got;
  ASSERT_TRUE(proj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));
  ASSERT_TRUE(proj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));
  ASSERT_TRUE(proj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));
  ASSERT_TRUE(proj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));
  ASSERT_FALSE(proj.Next(&got, nullptr));
  ASSERT_TRUE(rows.empty());
}

TEST_F(ExecutorTest, Selection) {
  Transaction txn = rs_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(txn, kTableName));
  Expression key_is_1 =
      BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(1)));
  Selection sel(key_is_1, tbl.GetSchema(),
                std::make_unique<FullScan>(txn, &tbl));
  std::unordered_set rows({Row({Value(1), Value("world"), Value(4.9)})});
  sel.Dump(std::cout, 0);
  std::cout << "\n";
  Row got;
  ASSERT_TRUE(sel.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_FALSE(sel.Next(&got, nullptr));
}

TEST_F(ExecutorTest, BasicJoin) {
  Transaction txn = rs_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(txn, kTableName));

  Schema right_schema{
      "RightTable",
      {Column("key2", ValueType::kInt64), Column("score2", ValueType::kDouble),
       Column("name2", ValueType::kVarChar)}};
  rs_->CreateTable(txn, right_schema);
  ASSIGN_OR_ASSERT_FAIL(Table, right_tbl, rs_->GetTable(txn, "RightTable"));
  BulkInsert(txn, right_tbl,
             {{Row({Value(9), Value(1.2), Value("troop")})},
              {Row({Value(7), Value(3.9), Value("arise")})},
              {Row({Value(1), Value(4.9), Value("probe")})},
              {Row({Value(3), Value(12.4), Value("ought")})},
              {Row({Value(3), Value(99.9), Value("extra")})},
              {Row({Value(232), Value(40.9), Value("out")})},
              {Row({Value(0), Value(9.2), Value("arise")})}});

  HashJoin hj(std::make_unique<FullScan>(txn, &tbl), {0},
              std::make_unique<FullScan>(txn, &right_tbl), {0});
  hj.Dump(std::cout, 0);
  std::cout << "\n";
  std::unordered_set rows({Row({Value(0), Value("hello"), Value(1.2), Value(0),
                                Value(9.2), Value("arise")}),
                           Row({Value(3), Value("piyo"), Value(12.2), Value(3),
                                Value(12.4), Value("ought")}),
                           Row({Value(3), Value("piyo"), Value(12.2), Value(3),
                                Value(99.9), Value("extra")}),
                           Row({Value(1), Value("world"), Value(4.9), Value(1),
                                Value(4.9), Value("probe")})});

  Row got;
  ASSERT_TRUE(hj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));
  ASSERT_TRUE(hj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));
  ASSERT_TRUE(hj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));
  ASSERT_TRUE(hj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));
  ASSERT_FALSE(hj.Next(&got, nullptr));
  ASSERT_TRUE(rows.empty());
}

}  // namespace tinylamb