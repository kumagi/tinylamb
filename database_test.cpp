#include "gtest/gtest.h"
#include "database.hpp"

namespace tinylamb {

class DatabaseTest : public ::testing::Test {
  void SetUp() override {
    db_ = std::make_unique<Database>("transaction_test.db");
  }
 protected:
  std::unique_ptr<Database> db_;
};

TEST_F(DatabaseTest, DoNothing) {}

}  // namespace tinylamb