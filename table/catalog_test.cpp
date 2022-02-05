#include "catalog.hpp"

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class CatalogTest : public ::testing::Test {
 protected:
  static constexpr char kFileName[] = "catalog_test.db";
  static constexpr char kLogName[] = "catalog_test.log";
  void SetUp() override {
    Recover();
    c_->Initialize();
  }
  void Recover() {
    tm_.reset();
    lm_.reset();
    l_.reset();
    c_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(kFileName, 10);
    c_ = std::make_unique<Catalog>(p_.get());
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get());
  }

  void TearDown() override { std::remove(kFileName); }

  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<Catalog> c_;
};

TEST_F(CatalogTest, Construction) {}

TEST_F(CatalogTest, CreateTable) {
  Column c1("int_column", ValueType::kInt64, 8, Restriction::kNoRestriction, 0);
  Column c2("varchar_column", ValueType::kVarChar, 16,
            Restriction::kNoRestriction, 8);
  std::vector<Column> columns = {c1, c2};
  Schema sc("test_table_for_create", columns, 2);
  std::cerr << "inserting schema: " << sc << "\n";
  auto txn = tm_->Begin();
  ASSERT_TRUE(c_->CreateTable(txn, sc));
  ASSERT_EQ(c_->Schemas(), 1u);
  c_->DebugDump(std::cout);
  txn.PreCommit();
  txn.CommitWait();
  ASSERT_EQ(1, c_->Schemas());
}

TEST_F(CatalogTest, GetTable) {
  static const char kTableName[] = "test_table_for_get";
  Column c1("int_column", ValueType::kInt64, 8, Restriction::kNoRestriction, 0);
  Column c2("varchar_column", ValueType::kVarChar, 16,
            Restriction::kNoRestriction, 8);
  std::vector<Column> columns = {c1, c2};
  Schema sc(kTableName, columns, 2);
  LOG(DEBUG) << sc;
  auto txn = tm_->Begin();
  ASSERT_TRUE(c_->CreateTable(txn, sc));
  ASSERT_EQ(c_->GetSchema(txn, kTableName), sc);
  txn.PreCommit();
  txn.CommitWait();
}

TEST_F(CatalogTest, Recover) {
  static const char kTableName[] = "test_table_for_recover";
  Column c1("int_column", ValueType::kInt64, 8, Restriction::kNoRestriction, 0);
  Column c2("varchar_column", ValueType::kVarChar, 16,
            Restriction::kNoRestriction, 8);
  std::vector<Column> columns = {c1, c2};
  Schema sc(kTableName, columns, 2);
  {
    auto txn = tm_->Begin();
    ASSERT_TRUE(c_->CreateTable(txn, sc));
    txn.PreCommit();
    txn.CommitWait();
  }
  Recover();
  {
    auto txn = tm_->Begin();
    ASSERT_EQ(c_->GetSchema(txn, "test_table_for_recover").Name(),
              "test_table_for_recover");
    Schema recovered_schema(c_->GetSchema(txn, kTableName));
    ASSERT_EQ(sc, recovered_schema);
    txn.PreCommit();
    txn.CommitWait();
  }
}

}  // namespace tinylamb