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
#include <sstream>
#include <string>

#include "common/encoder.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction_context.hpp"
#include "type/function.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
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

TEST_F(CatalogTest, Construction) {
  // Arrange -- nothing to set up; default database created by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest green on pass, death on crash
}

TEST_F(CatalogTest, CreateTable) {
  // Arrange -- begin context, define schema with 3 columns
  TransactionContext ctx = rs_->BeginContext();
  Schema new_schema("test_schema", {Column("col1", ValueType::kInt64),
                                    Column("key", ValueType::kInt64),
                                    Column("col3", ValueType::kVarChar)});

  // Act -- create table and pre-commit
  rs_->CreateTable(ctx, new_schema);
  ctx.txn_.PreCommit();

  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST_F(CatalogTest, GetTable) {
  // Arrange -- define schema; create table in first context, pre-commit
  Schema new_schema("test_schema", {Column("col1", ValueType::kInt64),
                                    Column("key", ValueType::kInt64),
                                    Column("col3", ValueType::kVarChar)});
  {
    TransactionContext ctx = rs_->BeginContext();
    rs_->CreateTable(ctx, new_schema);
    ctx.txn_.PreCommit();
  }

  // Act -- open second context, get table, pre-commit
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl,
                          ctx.GetTable("test_schema"));
    ctx.txn_.PreCommit();

    // Assert -- retrieved table's schema matches the created schema
    ASSERT_EQ(new_schema, tbl->GetSchema());
  }
}

TEST_F(CatalogTest, Recover) {
  // Arrange -- define schema; create table, debug-dump, pre-commit
  Schema new_schema("test_schema", {Column("col1", ValueType::kInt64),
                                    Column("key", ValueType::kInt64),
                                    Column("col3", ValueType::kVarChar)});
  {
    TransactionContext ctx = rs_->BeginContext();
    rs_->CreateTable(ctx, new_schema);
    {
      std::ostringstream oss;
      rs_->DebugDump(ctx.txn_, oss);
      LOG(INFO) << oss.str();
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Act -- emulate crash, recover database, open new context, get table, pre-commit
  Recover();
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl,
                          ctx.GetTable("test_schema"));
    ctx.txn_.PreCommit();

    // Assert -- recovered table's schema matches the originally created schema
    ASSERT_EQ(new_schema, tbl->GetSchema());
  }
}

TEST_F(CatalogTest, CreateDuplicateTable) {
  // Arrange -- create a table named "dup"
  Schema schema("dup", {Column("c", ValueType::kInt64)});
  {
    TransactionContext ctx = rs_->BeginContext();
    rs_->CreateTable(ctx, schema);
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Act -- attempt to create the same table again
  TransactionContext ctx = rs_->BeginContext();
  const Status status = rs_->CreateTable(ctx, schema).GetStatus();

  // Assert -- duplicate creation is rejected with kConflicts
  ASSERT_EQ(status, Status::kConflicts);
  ctx.txn_.Abort();
}

TEST_F(CatalogTest, GetOrAddFunction) {
  // Arrange -- a function name that does not exist yet
  TransactionContext ctx = rs_->BeginContext();

  // Act -- add it, then request it again
  auto first = rs_->GetOrAddFunction(ctx, "my_func", 2);
  auto second = rs_->GetOrAddFunction(ctx, "my_func", 2);
  const auto serialize = [](const Function& f) {
    std::stringstream ss;
    Encoder e(ss);
    e << f;
    return ss.str();
  };

  // Assert -- the second call returns the identical stored function
  ASSERT_TRUE(first.HasValue());
  ASSERT_TRUE(second.HasValue());
  EXPECT_EQ(serialize(first.Value()), serialize(second.Value()));
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(CatalogTest, StatisticsUpdateAndRefresh) {
  // Arrange -- create a table and insert two rows
  Schema schema("stats_tbl", {Column("c", ValueType::kInt64)});
  {
    TransactionContext ctx = rs_->BeginContext();
    rs_->CreateTable(ctx, schema);
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl,
                          ctx.GetTable("stats_tbl"));
    ASSERT_SUCCESS(
        tbl->Insert(ctx.txn_, Row({Value(1)})).GetStatus());
    ASSERT_SUCCESS(
        tbl->Insert(ctx.txn_, Row({Value(2)})).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                          ctx.GetStats("stats_tbl"));

    // Assert -- statistics are not updated implicitly by inserts
    EXPECT_EQ(stats->Rows(), 0);

    // Act -- refresh the stored statistics
    ASSERT_SUCCESS(rs_->RefreshStatistics(ctx, "stats_tbl"));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, refreshed,
                          ctx.GetStats("stats_tbl"));
    EXPECT_EQ(refreshed->Rows(), 2);

    // Act -- scale the statistics threefold and write them back
    TableStatistics scaled = *refreshed * 3;
    ASSERT_SUCCESS(rs_->UpdateStatistics(ctx, "stats_tbl", scaled));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, updated,
                          ctx.GetStats("stats_tbl"));
    EXPECT_EQ(updated->Rows(), 6);
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
}

TEST_F(CatalogTest, BeginReadOnlyContext) {
  // Arrange -- create a table and commit it
  Schema schema("ro_tbl", {Column("c", ValueType::kInt64)});
  {
    TransactionContext ctx = rs_->BeginContext();
    rs_->CreateTable(ctx, schema);
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Act -- open a read-only context and fetch the table
  TransactionContext ctx = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl,
                        ctx.GetTable("ro_tbl"));

  // Assert -- the table is visible to the read-only transaction
  ASSERT_EQ(schema, tbl->GetSchema());
  ctx.txn_.Abort();
}

TEST_F(CatalogTest, StreamDatabaseAndContext) {
  // Arrange -- create a table, then populate a context with table + stats
  Schema schema("stream_tbl", {Column("c", ValueType::kInt64)});
  {
    TransactionContext ctx = rs_->BeginContext();
    rs_->CreateTable(ctx, schema);
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl,
                        ctx.GetTable("stream_tbl"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        ctx.GetStats("stream_tbl"));

  // Act -- stream the context and the database
  std::ostringstream oss;
  oss << ctx;
  // Assert -- the context dump names its cached table and stats
  EXPECT_NE(oss.str().find("stream_tbl"), std::string::npos);
  EXPECT_NE(oss.str().find("TransactionContext"), std::string::npos);

  oss.str("");
  oss << *rs_;
  // Assert -- the database dump includes its PageStorage member
  EXPECT_NE(oss.str().find("PageStorage"), std::string::npos);
  ctx.txn_.PreCommit();
}

TEST_F(CatalogTest, TransactionContextCache) {
  // Arrange -- create a table and commit it
  Schema schema("cache_tbl", {Column("c", ValueType::kInt64)});
  {
    TransactionContext ctx = rs_->BeginContext();
    rs_->CreateTable(ctx, schema);
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  TransactionContext ctx = rs_->BeginContext();

  // Act -- request the table and stats twice each
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl1,
                        ctx.GetTable("cache_tbl"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl2,
                        ctx.GetTable("cache_tbl"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, s1,
                        ctx.GetStats("cache_tbl"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, s2,
                        ctx.GetStats("cache_tbl"));

  // Assert -- subsequent requests hit the context-local cache
  EXPECT_EQ(tbl1.get(), tbl2.get());
  EXPECT_EQ(s1.get(), s2.get());
  ctx.txn_.PreCommit();
}

TEST_F(CatalogTest, DebugDumpMultipleTables) {
  // Arrange -- create two tables
  Schema s1("tbl_one", {Column("a", ValueType::kInt64)});
  Schema s2("tbl_two", {Column("b", ValueType::kDouble)});
  {
    TransactionContext ctx = rs_->BeginContext();
    rs_->CreateTable(ctx, s1);
    rs_->CreateTable(ctx, s2);
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Act -- dump the catalog
  TransactionContext ctx = rs_->BeginContext();
  std::ostringstream oss;
  rs_->DebugDump(ctx.txn_, oss);

  // Assert -- both tables are present in the dump
  EXPECT_NE(oss.str().find("tbl_one"), std::string::npos);
  EXPECT_NE(oss.str().find("tbl_two"), std::string::npos);
  ctx.txn_.PreCommit();
}

TEST_F(CatalogTest, GetTableNotExists) {
  // Arrange + Act -- ask for a table that was never created
  TransactionContext ctx = rs_->BeginContext();
  const Status status = ctx.GetTable("missing_table").GetStatus();

  // Assert -- the lookup fails with kNotExists
  ASSERT_EQ(status, Status::kNotExists);
  ctx.txn_.Abort();
}

}  // namespace tinylamb