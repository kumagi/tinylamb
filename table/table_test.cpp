//
// Created by kumagi on 2022/02/06.
//

#include "table/table.hpp"

#include <memory>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class TableTest : public ::testing::Test {
 public:
  void SetUp() override {
    std::string prefix = "table_test-" + RandomString();
    db_name_ = prefix + ".db";
    log_name_ = prefix + ".log";
    master_record_name_ = prefix + ".master.log";
    schema_ = Schema("SampleTable", {Column("col1", ValueType::kInt64,
                                            Constraint(Constraint::kIndex)),
                                     Column("col2", ValueType::kVarChar),
                                     Column("col3", ValueType::kDouble)});
    Recover();
    Transaction txn = tm_->Begin();
    PageRef row_page = p_->AllocateNewPage(txn, PageType::kRowPage);
    std::vector<Index> ind;
    PageRef bt_root = p_->AllocateNewPage(txn, PageType::kLeafPage);
    ind.push_back({"idx1", {0, 1}, bt_root->PageID()});
    table_ =
        std::make_unique<Table>(p_.get(), schema_, row_page->PageID(), ind);
  }

  void Flush(page_id_t pid) { p_->GetPool()->FlushPageForTest(pid); }

  void Recover() {
    if (p_) {
      p_->GetPool()->LostAllPageForTest();
    }
    table_.reset();
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
    cm_ = std::make_unique<CheckpointManager>(master_record_name_, tm_.get(),
                                              p_->GetPool(), 1);
  }

  void TearDown() override {
    table_.reset();
    tm_.reset();
    r_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    std::remove(db_name_.c_str());
    // std::remove(log_name_.c_str());
    std::remove(master_record_name_.c_str());
  }

 public:
  std::string db_name_;
  std::string log_name_;
  std::string master_record_name_;
  Schema schema_;
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<CheckpointManager> cm_;
  std::unique_ptr<Table> table_;
};

TEST_F(TableTest, Construct) {}

TEST_F(TableTest, Insert) {
  Transaction txn = tm_->Begin();
  Row r({Value(1), Value("fuga"), Value(3.3)});
  RowPosition rp;
  ASSERT_SUCCESS(table_->Insert(txn, r, &rp));
}

TEST_F(TableTest, Read) {
  Transaction txn = tm_->Begin();
  Row r({Value(1), Value("string"), Value(3.3)});
  RowPosition rp;
  ASSERT_SUCCESS(table_->Insert(txn, r, &rp));
  Row read;
  ASSERT_SUCCESS(table_->Read(txn, rp, &read));
  ASSERT_EQ(read, r);
  LOG(INFO) << read;
}

TEST_F(TableTest, Update) {
  Transaction txn = tm_->Begin();
  RowPosition rp;
  Row new_row({Value(1), Value("hogefuga"), Value(99e8)});
  ASSERT_SUCCESS(
      table_->Insert(txn, Row({Value(1), Value("string"), Value(3.3)}), &rp));
  ASSERT_SUCCESS(table_->Update(txn, rp, new_row));
  Row read;
  ASSERT_SUCCESS(table_->Read(txn, rp, &read));
  ASSERT_EQ(read, new_row);
  LOG(INFO) << read;
}

TEST_F(TableTest, UpdateMany) {
  Transaction txn = tm_->Begin();
  RowPosition rp;
  std::vector<RowPosition> rps;
  for (int i = 0; i < 30; ++i) {
    Row new_row({Value(i), Value(RandomString(20)), Value(i * 99e8)});
    ASSERT_SUCCESS(table_->Insert(txn, new_row, &rp));
    rps.push_back(rp);
  }
  for (int i = 0; i < 260; ++i) {
    Row new_row({Value(i), Value(RandomString(40)), Value(i * 99e8)});
    ASSERT_SUCCESS(table_->Update(txn, rps[i % rps.size()], new_row));
  }
}

TEST_F(TableTest, Delete) {
  Transaction txn = tm_->Begin();
  RowPosition rp;
  ASSERT_SUCCESS(
      table_->Insert(txn, Row({Value(1), Value("string"), Value(3.3)}), &rp));
  ASSERT_SUCCESS(table_->Delete(txn, rp));
  Row read;
  ASSERT_FAIL(table_->Read(txn, rp, &read));
}

TEST_F(TableTest, IndexRead) {
  Transaction txn = tm_->Begin();
  RowPosition rp;
  ASSERT_SUCCESS(
      table_->Insert(txn, Row({Value(1), Value("string"), Value(3.3)}), &rp));
  ASSERT_SUCCESS(
      table_->Insert(txn, Row({Value(2), Value("hoge"), Value(4.8)}), &rp));
  ASSERT_SUCCESS(
      table_->Insert(txn, Row({Value(3), Value("foo"), Value(1.5)}), &rp));

  Row key({Value(2), Value("hoge")});
  Row result;
  ASSERT_SUCCESS(table_->ReadByKey(txn, "idx1", key, &result));
  ASSERT_EQ(result[0], Value(2));
  ASSERT_EQ(result[1], Value("hoge"));
  ASSERT_EQ(result[2], Value(4.8));
}

TEST_F(TableTest, IndexUpdateRead) {
  Transaction txn = tm_->Begin();
  RowPosition _, target;
  ASSERT_SUCCESS(
      table_->Insert(txn, Row({Value(1), Value("string"), Value(3.3)}), &_));
  ASSERT_SUCCESS(
      table_->Insert(txn, Row({Value(2), Value("hoge"), Value(4.8)}), &target));
  ASSERT_SUCCESS(
      table_->Insert(txn, Row({Value(3), Value("foo"), Value(1.5)}), &_));
  ASSERT_SUCCESS(
      table_->Update(txn, target, Row({Value(2), Value("baz"), Value(5.8)})));

  Row key({Value(2), Value("baz")});
  Row result;
  ASSERT_SUCCESS(table_->ReadByKey(txn, "idx1", key, &result));
  ASSERT_EQ(result[0], Value(2));
  ASSERT_EQ(result[1], Value("baz"));
  ASSERT_EQ(result[2], Value(5.8));
}

TEST_F(TableTest, IndexUpdateDelete) {
  Transaction txn = tm_->Begin();
  RowPosition _, target;
  ASSERT_SUCCESS(
      table_->Insert(txn, Row({Value(1), Value("string"), Value(3.3)}), &_));
  ASSERT_SUCCESS(
      table_->Insert(txn, Row({Value(2), Value("hoge"), Value(4.8)}), &target));
  ASSERT_SUCCESS(
      table_->Insert(txn, Row({Value(3), Value("foo"), Value(1.5)}), &_));
  ASSERT_SUCCESS(table_->Delete(txn, target));

  Row key({Value(2), Value("hoge")});
  Row result;
  ASSERT_EQ(table_->ReadByKey(txn, "idx1", key, &result), Status::kNotExists);
}

std::string KeyPayload(int num, int width) {
  std::stringstream ss;
  ss << std::setw(width) << std::setfill('0') << num;
  return ss.str();
}

TEST_F(TableTest, InsertMany) {
  Transaction txn = tm_->Begin();
  RowPosition rp;
  std::unordered_set<Row> rows;
  std::unordered_set<RowPosition> rps;
  for (int i = 0; i < 1000; ++i) {
    std::string key = KeyPayload(i, 1000);
    Row new_row({Value(i), Value(std::move(key)), Value(i * 3.3)});
    ASSERT_SUCCESS(table_->Insert(txn, new_row, &rp));
    rps.insert(rp);
    Row read;
    ASSERT_SUCCESS(table_->Read(txn, rp, &read));
    ASSERT_EQ(read, new_row);
    rows.insert(new_row);
  }
  Row read;
  for (const auto& row : rps) {
    ASSERT_SUCCESS(table_->Read(txn, row, &read));
    ASSERT_NE(rows.find(read), rows.end());
  }
}

TEST_F(TableTest, UpdateHeavy) {
  Transaction txn = tm_->Begin();
  RowPosition rp;
  std::unordered_set<Row> rows;
  std::vector<RowPosition> rps;
  rps.reserve(4000);
  for (int i = 0; i < 4000; ++i) {
    std::string key = RandomString((31 * i) % 800 + 1);
    Row new_row({Value(i), Value(std::move(key)), Value(i * 3.3)});
    ASSERT_SUCCESS(table_->Insert(txn, new_row, &rp));
    rps.push_back(rp);
  }
  Row read;
  for (int i = 0; i < 8000; ++i) {
    RowPosition pos = rps[(i * 63) % rps.size()];
    std::string key = RandomString((63 * i) % 120 + 1);
    LOG(TRACE) << "Update: " << key;
    Row new_row({Value(i), Value(std::move(key)), Value(i * 3.3)});
    ASSERT_SUCCESS(table_->Update(txn, pos, new_row));
  }
}

}  // namespace tinylamb