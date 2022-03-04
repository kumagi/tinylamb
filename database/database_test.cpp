#include "database.hpp"

#include "gtest/gtest.h"

namespace tinylamb {

class DatabaseTest : public ::testing::Test {
  void SetUp() override {
    db_ = std::make_unique<Database>("transaction_test.db");
  }

 protected:
  std::unique_ptr<Database> db_;
};

TEST_F(DatabaseTest, DoNothing) {}

TEST_F(DatabaseTest, SimpleTxn) { TransactionContext txn = db_->Begin(); }

}  // namespace tinylamb