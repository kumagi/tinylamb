#include "page/catalog_page.hpp"

#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class CatalogPageTest : public ::testing::Test {
 protected:
  static constexpr char kDBFileName[] = "catalog_page_test.db";
  static constexpr char kLogName[] = "catalog_page_test.log";
  static constexpr char kSchamaName[] = "test-schema";
  static constexpr uint64_t kPageId = 1;

  void SetUp() override { Recover(); }

  void Recover() {
    tm_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get());
  }

  void TearDown() override {
    std::remove(kDBFileName);
    std::remove(kLogName);
  }

  void PageInit() {
    auto* catalog_page = reinterpret_cast<CatalogPage*>(p_->GetPage(kPageId));
    catalog_page->PageInit(kPageId, PageType::kCatalogPage);
    p_->Unpin(catalog_page->page_id);
  }

  bool InsertSchema(const Schema& sc) {
    Transaction txn = tm_->Begin();
    auto* catalog_page = reinterpret_cast<CatalogPage*>(p_->GetPage(kPageId));
    bool result = catalog_page->AddSchema(txn, sc).IsValid();
    txn.PreCommit();
    p_->Unpin(catalog_page->page_id);
    return result;
  }

  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<Recovery> r_;
};

TEST_F(CatalogPageTest, Construct) {}

TEST_F(CatalogPageTest, AddSchema) {
  PageInit();

  Column c1("int_column", ValueType::kInt64, 8, Restriction::kNoRestriction, 0);
  Column c2("varchar_column", ValueType::kVarChar, 16,
            Restriction::kNoRestriction, 8);
  Schema schema(kSchamaName, {c1, c2}, 2);

  ASSERT_TRUE(InsertSchema(schema));
}

TEST_F(CatalogPageTest, GetSchema) {
  Column c1("int_column", ValueType::kInt64, 8, Restriction::kNoRestriction, 0);
  Column c2("varchar_column", ValueType::kVarChar, 16,
            Restriction::kNoRestriction, 8);
  Schema schema(kSchamaName, {c1, c2}, 2);
  PageInit();
  ASSERT_TRUE(InsertSchema(schema));
  Transaction txn = tm_->Begin();
  auto* catalog_page = reinterpret_cast<CatalogPage*>(p_->GetPage(kPageId));
  ASSERT_EQ(catalog_page->SlotCount(), 1);
  RowPosition pos(kPageId, 0);
  Schema read_schema = catalog_page->Read(txn, pos);
  ASSERT_EQ(read_schema, schema);
  ASSERT_TRUE(txn.PreCommit());
}

}  // namespace tinylamb