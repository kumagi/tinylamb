#include <memory>
#include <string>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/page_storage.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "relation_storage.hpp"
#include "table/table.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class CatalogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "catalog_test-" + RandomString();
    Recover();
  }
  void Recover() {
    if (rs_) {
      rs_->GetPageStorage()->LostAllPageForTest();
    }
    rs_ = std::make_unique<RelationStorage>(prefix_);
  }

  void TearDown() override {
    std::ignore = std::remove(rs_->GetPageStorage()->DBName().c_str());
    std::ignore = std::remove(rs_->GetPageStorage()->LogName().c_str());
    std::ignore =
        std::remove(rs_->GetPageStorage()->MasterRecordName().c_str());
  }

  std::string prefix_;
  std::unique_ptr<RelationStorage> rs_;
};

TEST_F(CatalogTest, Construction) {}

TEST_F(CatalogTest, CreateTable) {
  TransactionContext ctx = rs_->BeginContext();
  Schema new_schema("test_schema", {Column("col1", ValueType::kInt64),
                                    Column("key", ValueType::kInt64),
                                    Column("col3", ValueType::kVarChar)});
  rs_->CreateTable(ctx, new_schema);
  ctx.txn_.PreCommit();
}

TEST_F(CatalogTest, GetTable) {
  Schema new_schema("test_schema", {Column("col1", ValueType::kInt64),
                                    Column("key", ValueType::kInt64),
                                    Column("col3", ValueType::kVarChar)});
  {
    TransactionContext ctx = rs_->BeginContext();
    rs_->CreateTable(ctx, new_schema);
    ctx.txn_.PreCommit();
  }
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl,
                          ctx.GetTable("test_schema"));
    ctx.txn_.PreCommit();
    ASSERT_EQ(new_schema, tbl->GetSchema());
  }
}

TEST_F(CatalogTest, Recover) {
  Schema new_schema("test_schema", {Column("col1", ValueType::kInt64),
                                    Column("key", ValueType::kInt64),
                                    Column("col3", ValueType::kVarChar)});
  {
    TransactionContext ctx = rs_->BeginContext();
    rs_->CreateTable(ctx, new_schema);
    rs_->DebugDump(ctx.txn_, std::cout);
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  Recover();
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl,
                          ctx.GetTable("test_schema"));
    ctx.txn_.PreCommit();
    ASSERT_EQ(new_schema, tbl->GetSchema());
  }
}

}  // namespace tinylamb