#include <memory>
#include <string>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/page_storage.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "relation_storage.hpp"
#include "table/table.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class CatalogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "catalog_test-" + RandomString();
    ts_ = std::make_unique<PageStorage>(prefix_);
    Recover();
    auto txn = ts_->Begin();
  }
  void Recover() {
    ts_->LostAllPageForTest();
    ts_ = std::make_unique<PageStorage>(prefix_);
    catalog_ = std::make_unique<RelationStorage>(1, 2, ts_.get());
  }

  void TearDown() override {
    std::remove(ts_->DBName().c_str());
    std::remove(ts_->LogName().c_str());
    std::remove(ts_->MasterRecordName().c_str());
    ts_.reset();
  }

  std::string prefix_;
  std::unique_ptr<PageStorage> ts_;
  std::unique_ptr<RelationStorage> catalog_;
};

TEST_F(CatalogTest, Construction) {}

TEST_F(CatalogTest, CreateTable) {
  auto txn = ts_->Begin();
  Schema new_schema("test_schema", {Column("col1", ValueType::kInt64),
                                    Column("key", ValueType::kInt64),
                                    Column("col3", ValueType::kVarChar)});
  catalog_->CreateTable(txn, new_schema);
  txn.PreCommit();
}

TEST_F(CatalogTest, GetTable) {
  Schema new_schema("test_schema", {Column("col1", ValueType::kInt64),
                                    Column("key", ValueType::kInt64),
                                    Column("col3", ValueType::kVarChar)});
  {
    auto txn = ts_->Begin();
    catalog_->CreateTable(txn, new_schema);
    txn.PreCommit();
  }
  {
    auto txn = ts_->Begin();
    Table tbl;
    ASSERT_SUCCESS(catalog_->GetTable(txn, "test_schema", &tbl));
    txn.PreCommit();
    ASSERT_EQ(new_schema, tbl.GetSchema());
  }
}

TEST_F(CatalogTest, Recover) {
  Schema new_schema("test_schema", {Column("col1", ValueType::kInt64),
                                    Column("key", ValueType::kInt64),
                                    Column("col3", ValueType::kVarChar)});
  {
    auto txn = ts_->Begin();
    catalog_->CreateTable(txn, new_schema);
    catalog_->DebugDump(txn, std::cout);
    txn.PreCommit();
  }
  Recover();
  {
    auto txn = ts_->Begin();
    Table tbl;
    ASSERT_SUCCESS(catalog_->GetTable(txn, "test_schema", &tbl));
    txn.PreCommit();
    ASSERT_EQ(new_schema, tbl.GetSchema());
  }
}

}  // namespace tinylamb