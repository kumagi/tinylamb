#include "catalog.hpp"

#include <memory>
#include <string>

#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "table/fake_table.hpp"
#include "table/table.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class CatalogTest : public ::testing::Test {
 protected:
  static constexpr char kDBFileName[] = "catalog_test.db";
  static constexpr char kLogName[] = "catalog_test.log";
  static constexpr char kMasterRecordName[] = "catalog_test.log";
  void SetUp() override {
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
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(kLogName, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), l_.get(), r_.get());
    cm_ = std::make_unique<CheckpointManager>(kMasterRecordName, tm_.get(),
                                              p_->GetPool(), 1);
    catalog_ = std::make_unique<Catalog>(1, p_.get());
  }

  void TearDown() override {
    std::remove(kDBFileName);
    std::remove(kLogName);
    std::remove(kMasterRecordName);
  }

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
    Schema got;
    Table tbl(nullptr);
    ASSERT_SUCCESS(catalog_->GetTable(txn, "test_schema", &got, &tbl));
    txn.PreCommit();
    ASSERT_EQ(new_schema, got);
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
    Schema got;
    Table tbl(nullptr);
    ASSERT_SUCCESS(catalog_->GetTable(txn, "test_schema", &got, &tbl));
    txn.PreCommit();
    ASSERT_EQ(new_schema, got);
  }
}

}  // namespace tinylamb