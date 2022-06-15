//
// Created by kumagi on 2022/02/06.
//

#include <memory>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/relation_storage.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "table/table.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

static const char* kTableName = "SampleTable";
class IndexTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "index_test-" + RandomString();
    Recover();
    Transaction txn = rs_->Begin();
    Schema schema(kTableName, {Column("col1", ValueType::kInt64,
                                      Constraint(Constraint::kIndex)),
                               Column("col2", ValueType::kVarChar),
                               Column("col3", ValueType::kInt64)});
    ASSERT_SUCCESS(rs_->CreateTable(txn, schema));
    IndexSchema idx1("idx1", {0, 1}, {}, IndexMode::kNonUnique);
    ASSERT_SUCCESS(rs_->CreateIndex(txn, schema.Name(), idx1));
    IndexSchema idx2("idx2", {2}, {}, IndexMode::kNonUnique);
    ASSERT_SUCCESS(rs_->CreateIndex(txn, schema.Name(), idx2));
    ASSERT_SUCCESS(txn.PreCommit());
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

TEST_F(IndexTest, Construct) {}

TEST_F(IndexTest, Insert) {
  Transaction txn = rs_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(txn, kTableName));
  Row r({Value(1), Value("fuga"), Value(3)});
  ASSERT_SUCCESS(tbl.Insert(txn, r).GetStatus());
  ASSERT_SUCCESS(tbl.Insert(txn, r).GetStatus());
  ASSERT_SUCCESS(tbl.Insert(txn, r).GetStatus());
}

TEST_F(IndexTest, Read) {
  Transaction txn = rs_->Begin();
  Row r({Value(1), Value("string"), Value(43)});
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(txn, kTableName));
  ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl.Insert(txn, r));
  ASSIGN_OR_ASSERT_FAIL(Row, read, tbl.Read(txn, rp));
  ASSERT_EQ(read, r);
}

TEST_F(IndexTest, Update) {
  Transaction txn = rs_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(txn, kTableName));
  Row new_row({Value(1), Value("hogefuga"), Value(99)});
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp,
      tbl.Insert(txn, Row({Value(1), Value("string"), Value(39)})));
  ASSERT_SUCCESS(tbl.Update(txn, rp, new_row).GetStatus());
  ASSIGN_OR_ASSERT_FAIL(Row, read, tbl.Read(txn, rp));
  ASSERT_EQ(read, new_row);
}

TEST_F(IndexTest, UpdateMany) {
  Transaction txn = rs_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(txn, kTableName));
  std::vector<RowPosition> rps;
  for (int i = 0; i < 30; ++i) {
    Row new_row({Value(i), Value(RandomString(20)), Value(i * 9)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl.Insert(txn, new_row));
    rps.push_back(rp);
  }
  for (int i = 0; i < 260; ++i) {
    Row new_row({Value(i), Value(RandomString(40)), Value(i * 9)});
    RowPosition pos = rps[i % rps.size()];
    ASSIGN_OR_ASSERT_FAIL(RowPosition, new_pos, tbl.Update(txn, pos, new_row));
    rps[i % rps.size()] = new_pos;
  }
}

TEST_F(IndexTest, Delete) {
  Transaction txn = rs_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(txn, kTableName));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp,
      tbl.Insert(txn, Row({Value(1), Value("string"), Value(3)})));
  ASSERT_SUCCESS(tbl.Delete(txn, rp));
  ASSERT_FAIL(tbl.Read(txn, rp).GetStatus());
}

TEST_F(IndexTest, IndexRead) {
  Transaction txn = rs_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(txn, kTableName));
  ASSERT_SUCCESS(tbl.Insert(txn, Row({Value(1), Value("string"), Value(3.3)}))
                     .GetStatus());
  ASSERT_SUCCESS(
      tbl.Insert(txn, Row({Value(2), Value("hoge"), Value(8)})).GetStatus());
  ASSERT_SUCCESS(
      tbl.Insert(txn, Row({Value(3), Value("foo"), Value(5)})).GetStatus());

  // TODO(kumagi): do index scan.
}

TEST_F(IndexTest, IndexUpdateRead) {
  Transaction txn = rs_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(txn, kTableName));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp0,
      tbl.Insert(txn, Row({Value(1), Value("string"), Value(3)})));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp1,
      tbl.Insert(txn, Row({Value(2), Value("hoge"), Value(3)})));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp2,
      tbl.Insert(txn, Row({Value(3), Value("foo"), Value(3)})));
  ASSERT_NE(rp0, rp2);
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp3,
      tbl.Update(txn, rp1, Row({Value(2), Value("baz"), Value(8)})));
  ASSERT_EQ(rp1, rp3);

  // TODO(kumagi): do index scan.
}

TEST_F(IndexTest, IndexUpdateDelete) {
  Transaction txn = rs_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(txn, kTableName));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp1,
      tbl.Insert(txn, Row({Value(1), Value("string"), Value(3)})));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp2,
      tbl.Insert(txn, Row({Value(2), Value("hoge"), Value(4)})));
  ASSIGN_OR_ASSERT_FAIL(
      RowPosition, rp3,
      tbl.Insert(txn, Row({Value(3), Value("foo"), Value(5)})));
  ASSERT_SUCCESS(tbl.Delete(txn, rp1));
  // TODO(kumagi): do index scan.
  ASSERT_NE(rp2, rp3);
}

std::string KeyPayload(int num, int width) {
  std::stringstream ss;
  ss << std::setw(width) << std::setfill('0') << num;
  return ss.str();
}

TEST_F(IndexTest, InsertMany) {
  Transaction txn = rs_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(txn, kTableName));
  std::unordered_set<Row> rows;
  std::unordered_set<RowPosition> rps;
  for (int i = 0; i < 1000; ++i) {
    std::string key = KeyPayload(i, 1000);
    Row new_row({Value(i), Value(std::move(key)), Value(i * 3)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl.Insert(txn, new_row));
    rps.insert(rp);
    ASSIGN_OR_ASSERT_FAIL(Row, read, tbl.Read(txn, rp));
    ASSERT_EQ(read, new_row);
    rows.insert(new_row);
  }
  for (const auto& row : rps) {
    ASSIGN_OR_ASSERT_FAIL(Row, read, tbl.Read(txn, row));
    ASSERT_NE(rows.find(read), rows.end());
  }
}

TEST_F(IndexTest, UpdateHeavy) {
  constexpr int kCount = 50;
  Transaction txn = rs_->Begin();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->GetTable(txn, kTableName));
  std::unordered_set<Row> rows;
  std::vector<RowPosition> rps;
  rps.reserve(kCount);
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 120 + 10, false);
    Row new_row({Value(i), Value(std::move(key)), Value(i % 10)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl.Insert(txn, new_row));
    rps.push_back(rp);
  }
  Row read;
  for (int i = 0; i < kCount * 4; ++i) {
    RowPosition& pos = rps[(i * 63) % rps.size()];
    std::string key = RandomString((19937 * i) % 3200 + 5000, false);
    Row new_row({Value(i), Value(std::move(key)), Value(i % 20)});
    ASSIGN_OR_ASSERT_FAIL(RowPosition, rp, tbl.Update(txn, pos, new_row));
    rps[(i * 63) % rps.size()] = rp;
  }
}

}  // namespace tinylamb