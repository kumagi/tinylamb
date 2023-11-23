/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

TEST_F(DatabaseTest, SimpleTxn) {
  TransactionContext txn = db_->BeginContext();
}

}  // namespace tinylamb