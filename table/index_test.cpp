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

#include <memory>
#include <utility>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "gtest/gtest.h"
#include "index/index_schema.hpp"
#include "page/page_manager.hpp"
#include "recovery/recovery_manager.hpp"
#include "table/table.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
static const char* kTableName = "SampleTable";

class IndexTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "index_test-" + RandomString();
    Recover();
    TransactionContext ctx = rs_->BeginContext();
    Schema schema(kTableName, {Column("col1", ValueType::kInt64,
                                      Constraint(Constraint::kIndex)),
                               Column("col2", ValueType::kVarChar),
                               Column("col3", ValueType::kInt64)});
    ASSERT_SUCCESS(rs_->CreateTable(ctx, schema).GetStatus());
    IndexSchema idx1("idx1", {0, 1}, {}, IndexMode::kNonUnique);
    ASSERT_SUCCESS(rs_->CreateIndex(ctx, schema.Name(), idx1));
    IndexSchema idx2("idx2", {2}, {}, IndexMode::kNonUnique);
    ASSERT_SUCCESS(rs_->CreateIndex(ctx, schema.Name(), idx2));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  void Recover() {
    if (rs_) {
      rs_->EmulateCrash();
    }
    rs_ = std::make_unique<Database>(prefix_);
  }

  void TearDown() override { rs_->DeleteAll(); }

  std::string prefix_;
  std::unique_ptr<Database> rs_;
};

TEST_F(IndexTest, Construct) {
  // Arrange -- nothing to set up; default IndexTest environment via SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest green on pass, death on crash
}

TEST_F(IndexTest, Insert) {
  // Arrange -- begin context, get table
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  Row r({Value(1), Value("fuga"), Value(3)});

  // Act -- insert the same row 3 times (non-unique index allows duplicates)
  ASSERT_SUCCESS(tbl->Insert(ctx.txn_, r).GetStatus());
  ASSERT_SUCCESS(tbl->Insert(ctx.txn_, r).GetStatus());
  ASSERT_SUCCESS(tbl->Insert(ctx.txn_, r).GetStatus());

  // Assert -- implicit; all 3 inserts succeeded; gtest green on pass
}

TEST_F(IndexTest, Read) {
  // Arrange -- begin context, get table, insert a row
  TransactionContext ctx = rs_->BeginContext();
  Row r({Value(1), Value("string"), Value(43)});
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Insert(ctx.txn_, r));

  // Act -- read the row back via the row position
  ASSIGN_OR_ASSERT_FAIL(Row, read, tbl->Read(ctx.txn_, rp));

  // Assert -- the read row matches the inserted row
  ASSERT_EQ(read, r);
}

TEST_F(IndexTest, Update) {
  // Arrange -- begin context, get table, insert a row
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  Row new_row({Value(1), Value("hogefuga"), Value(99)});
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp,
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(39)})));

  // Act -- update the row at rp with new_row, then read it back
  ASSERT_SUCCESS(tbl->Update(ctx.txn_, rp, new_row).GetStatus());
  ASSIGN_OR_ASSERT_FAIL(Row, read, tbl->Read(ctx.txn_, rp));

  // Assert -- the read row matches the updated row
  ASSERT_EQ(read, new_row);
}

TEST_F(IndexTest, UpdateMany) {
  // Arrange -- begin context, get table, reserve rps vector
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  std::vector<RowPosition> rps;

  // Act -- insert 30 rows then update each 260 times via round-robin
  for (int i = 0; i < 30; ++i) {
    Row new_row({Value(i), Value(RandomString(20)), Value(i * 9)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Insert(ctx.txn_, new_row));
    rps.push_back(rp);
  }
  for (int i = 0; i < 260; ++i) {
    Row new_row({Value(i), Value(RandomString(40)), Value(i * 9)});
    RowPosition pos = rps[i % rps.size()];
    ASSIGN_OR_ASSERT_FAIL(RowPosition, new_pos,
                          tbl->Update(ctx.txn_, pos, new_row));
    rps[i % rps.size()] = new_pos;
  }

  // Assert -- implicit; 30 inserts + 260 updates completed without crash; gtest green on pass
}

TEST_F(IndexTest, Delete) {
  // Arrange -- begin context, get table, insert a row
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp,
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3)})));

  // Act -- delete the row at rp
  ASSERT_SUCCESS(tbl->Delete(ctx.txn_, rp));

  // Assert -- reading the deleted row position should fail
  ASSERT_FAIL(tbl->Read(ctx.txn_, rp).GetStatus());
}

TEST_F(IndexTest, IndexRead) {
  // Arrange -- begin context, get table
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));

  // Act -- insert three rows into the indexed table
  ASSERT_SUCCESS(
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3.3)}))
          .GetStatus());
  ASSERT_SUCCESS(tbl->Insert(ctx.txn_, Row({Value(2), Value("hoge"), Value(8)}))
                     .GetStatus());
  ASSERT_SUCCESS(tbl->Insert(ctx.txn_, Row({Value(3), Value("foo"), Value(5)}))
                     .GetStatus());

  // Assert -- TODO(kumagi): do index scan to verify indexed reads
}

TEST_F(IndexTest, IndexUpdateRead) {
  // Arrange -- begin context, get table
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));

  // Act -- insert three rows then update one via index
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp0,
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3)})));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp1,
      tbl->Insert(ctx.txn_, Row({Value(2), Value("hoge"), Value(3)})));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp2,
      tbl->Insert(ctx.txn_, Row({Value(3), Value("foo"), Value(3)})));
  ASSERT_NE(rp0, rp2);
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp3,
      tbl->Update(ctx.txn_, rp1, Row({Value(2), Value("baz"), Value(8)})));

  // Assert -- update produced same row position; TODO(kumagi): do index scan
  ASSERT_EQ(rp1, rp3);
}

TEST_F(IndexTest, IndexUpdateDelete) {
  // Arrange -- begin context, get table
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));

  // Act -- insert three rows then delete one via index
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp1,
      tbl->Insert(ctx.txn_, Row({Value(1), Value("string"), Value(3)})));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp2,
      tbl->Insert(ctx.txn_, Row({Value(2), Value("hoge"), Value(4)})));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp3,
      tbl->Insert(ctx.txn_, Row({Value(3), Value("foo"), Value(5)})));
  ASSERT_SUCCESS(tbl->Delete(ctx.txn_, rp1));

  // Assert -- remaining row positions are distinct; TODO(kumagi): do index scan
  ASSERT_NE(rp2, rp3);
}

std::string KeyPayload(int num, int width) {
  std::stringstream ss;
  ss << std::setw(width) << std::setfill('0') << num;
  return ss.str();
}

TEST_F(IndexTest, InsertMany) {
  // Arrange -- begin context, get table, reserve sets
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  std::unordered_set<Row> rows;
  std::unordered_set<RowPosition> rps;

  // Act -- insert 1000 rows with sequential keys and read each back
  for (int i = 0; i < 1000; ++i) {
    std::string key = KeyPayload(i, 1000);
    Row new_row({Value(i), Value(std::move(key)), Value(i * 3)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Insert(ctx.txn_, new_row));
    rps.insert(rp);
    ASSIGN_OR_ASSERT_FAIL(Row, read, tbl->Read(ctx.txn_, rp));
    ASSERT_EQ(read, new_row);
    rows.insert(new_row);
  }

  // Assert -- every inserted row is readable via its row position
  for (const auto& row : rps) {
    ASSIGN_OR_ASSERT_FAIL(Row, read, tbl->Read(ctx.txn_, row));
    ASSERT_NE(rows.find(read), rows.end());
  }
}

TEST_F(IndexTest, UpdateHeavy) {
  // Arrange -- begin context, get table, reserve rps vector
  constexpr int kCount = 50;
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  std::unordered_set<Row> rows;
  std::vector<RowPosition> rps;
  rps.reserve(kCount);

  // Act -- insert kCount rows then update each kCount*4 times via round-robin
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 120 + 10, false);
    Row new_row({Value(i), Value(std::move(key)), Value(i % 10)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Insert(ctx.txn_, new_row));
    rps.push_back(rp);
  }
  Row read;
  for (int i = 0; i < kCount * 4; ++i) {
    RowPosition& pos = rps[(i * 63) % rps.size()];
    std::string key = RandomString((19937 * i) % 3200 + 500, false);
    Row new_row({Value(i), Value(std::move(key)), Value(i % 20)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl->Update(ctx.txn_, pos, new_row));
    rps[(i * 63) % rps.size()] = rp;
  }

  // Assert -- implicit; kCount inserts + kCount*4 updates completed without crash
}
}  // namespace tinylamb
