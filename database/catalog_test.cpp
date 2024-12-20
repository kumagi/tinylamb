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

#include <iostream>
#include <memory>
#include <string>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "table/table.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction_context.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class CatalogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "catalog_test-" + RandomString();
    Recover();
  }
  void Recover() {
    if (rs_) {
      rs_->EmulateCrash();
    }
    rs_ = std::make_unique<Database>(prefix_);
  }

  void TearDown() override { rs_->DeleteAll(); }

  std::string prefix_;
  std::unique_ptr<Database> rs_;
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