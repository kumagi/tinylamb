#include "transaction/transaction.hpp"

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class TransactionTest : public ::testing::Test {
  static constexpr char kLogName[] = "row_page_test.log";

 public:
  void SetUp() override { Reset(); }

  virtual void Reset() {
    tm_.reset();
    lm_.reset();
    l_.reset();
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get());
  }

 protected:
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<TransactionManager> tm_;
};

TEST_F(TransactionTest, construct) {}

}  // namespace tinylamb