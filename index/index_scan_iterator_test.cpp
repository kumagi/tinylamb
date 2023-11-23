/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "index_scan_iterator.hpp"

#include <memory>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "gtest/gtest.h"
#include "index.hpp"
#include "recovery/recovery_manager.hpp"
#include "table/table.hpp"
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
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateTable(ctx, sc).GetStatus());
    ASSERT_SUCCESS(rs_->CreateIndex(ctx, kTableName, IndexSchema("PK", {0})));
    ASSERT_SUCCESS(rs_->CreateIndex(
        ctx, kTableName,
        IndexSchema("NameIdx", {1}, {2}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(rs_->CreateIndex(ctx, kTableName,
                                    IndexSchema("KeyScore", {0, 2}, {1})));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable(kTableName));
    ASSERT_EQ(table->IndexCount(), 3);
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
    std::remove(rs_->GetPageStorage()->MasterRecordName().c_str());
  }

  std::string prefix_;
  std::unique_ptr<RelationStorage> rs_;
};

TEST_F(IndexScanIteratorTest, Construct) {}

TEST_F(IndexScanIteratorTest, ScanAscending) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        ctx.GetTable(kTableName));
  for (int i = 0; i < 230; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)}))
            .GetStatus());
  }
  Iterator it = table->BeginIndexScan(ctx.txn_, table->GetIndex(0), Value(43),
                                      Value(180));
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

TEST_F(IndexScanIteratorTest, NonUniqueAscending) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        ctx.GetTable(kTableName));
  for (int i = 0; i < 120; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_,
                     Row({Value(i), Value("v" + std::to_string(i % 10)),
                          Value(static_cast<double>(i * 2))}))
            .GetStatus());
  }
  {
    // Partial scan.
    Iterator it = table->BeginIndexScan(ctx.txn_, table->GetIndex(1),
                                        Value("v2"), Value("v7"));
    ASSERT_TRUE(it.IsValid());
    int counter = 0;
    while (it.IsValid()) {
      Row row = *it;
      ASSERT_DOUBLE_EQ(row[0].value.int_value * 2, row[2].value.double_value);
      ++it;
      ++counter;
    }
    ASSERT_EQ(counter, 12 * (7 - 2 + 1));
    ASSERT_FALSE(it.IsValid());
  }
  {
    // Full scan.
    Iterator it = table->BeginIndexScan(ctx.txn_, table->GetIndex(1));
    ASSERT_TRUE(it.IsValid());
    int counter = 0;
    while (it.IsValid()) {
      Row row = *it;
      ASSERT_DOUBLE_EQ(row[0].value.int_value * 2, row[2].value.double_value);
      ++it;
      ++counter;
    }
    ASSERT_EQ(counter, 120);
    ASSERT_FALSE(it.IsValid());
  }

  // Delete where PK % 5 == 0
  {
    Iterator it = table->BeginFullScan(ctx.txn_);
    ASSERT_TRUE(it.IsValid());
    while (it.IsValid()) {
      Row row = *it;
      if (row[0].value.int_value % 5 == 0) {
        table->Delete(ctx.txn_, it.Position());
      }
      ++it;
    }
    ASSERT_FALSE(it.IsValid());
  }
  {
    // Full scan again.
    Iterator it = table->BeginFullScan(ctx.txn_);
    ASSERT_TRUE(it.IsValid());
    int counter = 0;
    while (it.IsValid()) {
      Row row = *it;
      ASSERT_DOUBLE_EQ(row[0].value.int_value * 2, row[2].value.double_value);
      ++it;
      ++counter;
    }
    ASSERT_EQ(counter, 80);
    ASSERT_FALSE(it.IsValid());
  }
}

TEST_F(IndexScanIteratorTest, ScanDecending) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        ctx.GetTable(kTableName));
  for (int i = 0; i < 230; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)}))
            .GetStatus());
  }
  Iterator it = table->BeginIndexScan(ctx.txn_, table->GetIndex(0), Value(104),
                                      Value(200), false);
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

TEST_F(IndexScanIteratorTest, NonUniqueDescending) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        ctx.GetTable(kTableName));
  for (int i = 0; i < 120; ++i) {
    ASSERT_SUCCESS(
        table
            ->Insert(ctx.txn_,
                     Row({Value(i), Value("v" + std::to_string(i % 10)),
                          Value(static_cast<double>(i * 2))}))
            .GetStatus());
  }
  {
    Iterator it = table->BeginIndexScan(ctx.txn_, table->GetIndex(1),
                                        Value("v2"), Value("v7"), false);
    ASSERT_TRUE(it.IsValid());
    int counter = 0;
    while (it.IsValid()) {
      Row row = *it;
      ASSERT_DOUBLE_EQ(row[0].value.int_value * 2, row[2].value.double_value);
      --it;
      ++counter;
    }
    ASSERT_EQ(counter, 12 * (7 - 2 + 1));
    ASSERT_FALSE(it.IsValid());
  }
  {
    // Full scan.
    Iterator it = table->BeginIndexScan(ctx.txn_, table->GetIndex(1), Value(),
                                        Value(), false);
    ASSERT_TRUE(it.IsValid());
    int counter = 0;
    while (it.IsValid()) {
      Row row = *it;
      ASSERT_DOUBLE_EQ(row[0].value.int_value * 2, row[2].value.double_value);
      --it;
      ++counter;
    }
    ASSERT_EQ(counter, 120);
    ASSERT_FALSE(it.IsValid());
  }
}

}  // namespace tinylamb
