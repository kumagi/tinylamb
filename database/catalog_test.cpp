#include "catalog.hpp"

#include <memory>
#include <string>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "table/table.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class CatalogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string prefix = "catalog_test-" + RandomString();
    db_name_ = prefix + ".db";
    log_name_ = prefix + ".log";
    master_record_name_ = prefix + ".master.log";
    Recover();
    auto txn = tm_->Begin();
    catalog_->InitializeIfNeeded(txn);
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
    cm_ = std::make_unique<CheckpointManager>(master_record_name_, tm_.get(),
                                              p_->GetPool(), 1);
    catalog_ = std::make_unique<Catalog>(1, 2, p_.get());
  }

  void TearDown() override {
    std::remove(db_name_.c_str());
    std::remove(log_name_.c_str());
    std::remove(master_record_name_.c_str());
  }

  std::string db_name_;
  std::string log_name_;
  std::string master_record_name_;
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<CheckpointManager> cm_;
  std::unique_ptr<Catalog> catalog_;
};

TEST_F(CatalogTest, Construction) {}

TEST_F(CatalogTest, CreateTable) {
  auto txn = tm_->Begin();
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
    auto txn = tm_->Begin();
    catalog_->CreateTable(txn, new_schema);
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    Table tbl(nullptr);
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
    auto txn = tm_->Begin();
    catalog_->CreateTable(txn, new_schema);
    catalog_->DebugDump(txn, std::cout);
    txn.PreCommit();
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());
  {
    auto txn = tm_->Begin();
    Table tbl(nullptr);
    ASSERT_SUCCESS(catalog_->GetTable(txn, "test_schema", &tbl));
    txn.PreCommit();
    ASSERT_EQ(new_schema, tbl.GetSchema());
  }
}

}  // namespace tinylamb