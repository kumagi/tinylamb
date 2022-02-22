//
// Created by kumagi on 2022/02/23.
//
#include <memory>

#include "executor/full_scan.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "gtest/gtest.h"
#include "projection.hpp"
#include "selection.hpp"
#include "table/fake_table.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

class ExecutorTest : public ::testing::Test {
 public:
  void SetUp() override {}

  FakeTable table_{{Row({Value(0), Value("hello"), Value(1.2)})},
                   {Row({Value(3), Value("piyo"), Value(12.2)})},
                   {Row({Value(1), Value("world"), Value(4.9)})},
                   {Row({Value(2), Value("arise"), Value(4.14)})}};
  Transaction fake_txn_;
  Schema schema_{
      "test_table",
      {Column("key", ValueType::kInt64), Column("name", ValueType::kVarChar),
       Column("score", ValueType::kDouble)}};
};

TEST_F(ExecutorTest, Construct) {}

TEST_F(ExecutorTest, FullScan) {
  FullScan fs(fake_txn_, &table_);
  std::unordered_set rows({Row({Value(0), Value("hello"), Value(1.2)}),
                           Row({Value(3), Value("piyo"), Value(12.2)}),
                           Row({Value(1), Value("world"), Value(4.9)}),
                           Row({Value(2), Value("arise"), Value(4.14)})});
  Row got;
  ASSERT_TRUE(fs.Next(&got));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(fs.Next(&got));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(fs.Next(&got));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(fs.Next(&got));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_FALSE(fs.Next(&got));
}

TEST_F(ExecutorTest, Projection) {
  FullScan fs(fake_txn_, &table_);
  Projection proj({0, 2}, &fs);
  std::unordered_set rows(
      {Row({Value(0), Value(1.2)}), Row({Value(3), Value(12.2)}),
       Row({Value(1), Value(4.9)}), Row({Value(2), Value(4.14)})});
  Row got;
  ASSERT_TRUE(proj.Next(&got));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(proj.Next(&got));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(proj.Next(&got));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(proj.Next(&got));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_FALSE(proj.Next(&got));
}

TEST_F(ExecutorTest, Selection) {
  FullScan fs(fake_txn_, &table_);
  Selection sel(std::make_unique<BinaryExpression>(
                    std::make_unique<ColumnValue>("key"),
                    std::make_unique<ConstantValue>(Value(1)), OpType::kEquals),
                &schema_, &fs);
  std::unordered_set rows({Row({Value(1), Value("world"), Value(4.9)})});
  Row got;
  ASSERT_TRUE(sel.Next(&got));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_FALSE(sel.Next(&got));
}

}  // namespace tinylamb