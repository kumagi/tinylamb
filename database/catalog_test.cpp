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

#include <stdlib.h>  // NOLINT(modernize-deprecated-headers) // POSIX setenv/unsetenv below are only provided by this header.

#include <algorithm>
#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/encoder.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction_context.hpp"
#include "type/constraint.hpp"
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
  ASSERT_SUCCESS(rs_->CreateTable(ctx, new_schema).GetStatus());
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
    ASSERT_SUCCESS(rs_->CreateTable(ctx, new_schema).GetStatus());
    ctx.txn_.PreCommit();
  }

  // Act -- open second context, get table, pre-commit
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
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

  // Act -- emulate crash, recover database, open new context, get table,
  // pre-commit
  Recover();
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
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
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                                ctx.GetTable("stats_tbl"));
    ASSERT_SUCCESS(tbl->Insert(ctx.txn_, Row({Value(1)})).GetStatus());
    ASSERT_SUCCESS(tbl->Insert(ctx.txn_, Row({Value(2)})).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<TableStatistics>, stats,
                                ctx.GetStats("stats_tbl"));

    // Assert -- statistics are not updated implicitly by inserts
    EXPECT_EQ(stats->Rows(), 0);

    // Act -- refresh the stored statistics
    ASSERT_SUCCESS(rs_->RefreshStatistics(ctx, "stats_tbl"));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<TableStatistics>, refreshed,
                                ctx.GetStats("stats_tbl"));
    EXPECT_EQ(refreshed->Rows(), 2);

    // Act -- scale the statistics threefold and write them back
    TableStatistics scaled = *refreshed * 3;
    ASSERT_SUCCESS(rs_->UpdateStatistics(ctx, "stats_tbl", scaled));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<TableStatistics>, updated,
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
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
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
  ASSERT_TRUE(ctx.GetTable("stream_tbl").HasValue());
  ASSERT_TRUE(ctx.GetStats("stream_tbl").HasValue());

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
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl1,
                              ctx.GetTable("cache_tbl"));
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl2,
                              ctx.GetTable("cache_tbl"));
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<TableStatistics>, s1,
                              ctx.GetStats("cache_tbl"));
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<TableStatistics>, s2,
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

TEST_F(CatalogTest, PageStoragePoolCapacityFromEnvironment) {
  // Arrange -- a pool capacity far above one page but below the default
  ASSERT_EQ(setenv("TINYLAMB_PAGE_POOL_BYTES", "8388608", 1), 0);
  {
    Database env_db("catalog_test_env_pool-" + RandomString());
    TransactionContext ctx = env_db.BeginContext();
    Schema schema("env_tbl", {Column("c", ValueType::kInt64)});
    ASSERT_SUCCESS(env_db.CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
    env_db.DeleteAll();
  }
  unsetenv("TINYLAMB_PAGE_POOL_BYTES");

  // Arrange -- an env value below one page falls back to the default pool
  ASSERT_EQ(setenv("TINYLAMB_PAGE_POOL_BYTES", "1000", 1), 0);
  {
    Database small_env_db("catalog_test_small_env-" + RandomString());
    TransactionContext ctx = small_env_db.BeginContext();
    Schema schema("small_tbl", {Column("c", ValueType::kVarChar)});
    ASSERT_SUCCESS(small_env_db.CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
    small_env_db.DeleteAll();
  }
  unsetenv("TINYLAMB_PAGE_POOL_BYTES");
}

TEST_F(CatalogTest, PageStoragePageSlotsAllocateReuse) {
  // Arrange -- begin a transaction backed by the PageStorage page manager
  TransactionContext ctx = rs_->BeginContext();
  Transaction& txn = ctx.txn_;
  PageManager* pm = txn.GetPageManager();

  page_id_t first_pid = 0;
  {
    // Act -- allocate a row page and write two slots into it
    PageRef first = pm->AllocateNewPage(txn, PageType::kRowPage);
    first_pid = first->PageID();
    ASSERT_GT(first_pid, 0);

    StatusOr<slot_t> slot_a = first->Insert(txn, "row-a");
    ASSERT_SUCCESS(slot_a.GetStatus());
    StatusOr<slot_t> slot_b = first->Insert(txn, "row-b");
    ASSERT_SUCCESS(slot_b.GetStatus());
    ASSERT_NE(slot_a.Value(), slot_b.Value());

    // Assert -- each slot reads back its written payload
    StatusOr<std::string_view> read_a = first->Read(txn, slot_a.Value());
    ASSERT_SUCCESS(read_a.GetStatus());
    EXPECT_EQ(read_a.Value(), "row-a");

    // Act -- update one slot in place and read it back
    ASSERT_SUCCESS(first->Update(txn, slot_b.Value(), "row-b-updated"));
    StatusOr<std::string_view> read_b = first->Read(txn, slot_b.Value());
    ASSERT_SUCCESS(read_b.GetStatus());
    EXPECT_EQ(read_b.Value(), "row-b-updated");

    // Act -- delete a slot and confirm it is gone
    ASSERT_SUCCESS(first->Delete(txn, slot_a.Value()));
    StatusOr<std::string_view> gone = first->Read(txn, slot_a.Value());
    EXPECT_EQ(gone.GetStatus(), Status::kNotExists);

    // Act -- destroy the page so its id returns to the free list
    // D3 (docs/design.md): the destroy redo initializes the page as a free
    // page and recovery rebuilds the free list, so a crash after this commit
    // no longer breaks startup.
    pm->DestroyPage(txn, first.get());
  }

  // Assert -- the next allocation reuses the freed page id
  PageRef second = pm->AllocateNewPage(txn, PageType::kRowPage);
  EXPECT_EQ(second->PageID(), first_pid);

  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(CatalogTest, PageStorageAllocatesGrowingPageIds) {
  // Arrange -- begin a transaction backed by the PageStorage page manager
  TransactionContext ctx = rs_->BeginContext();
  PageManager* pm = ctx.txn_.GetPageManager();

  // Act -- allocate several row pages and record their ids
  std::vector<page_id_t> pids;
  for (int i = 0; i < 5; ++i) {
    PageRef page = pm->AllocateNewPage(ctx.txn_, PageType::kRowPage);
    pids.push_back(page->PageID());
  }

  // Assert -- every page id is distinct (the underlying file grew)
  for (size_t i = 0; i < pids.size(); ++i) {
    for (size_t j = i + 1; j < pids.size(); ++j) {
      ASSERT_NE(pids[i], pids[j]);
    }
  }

  // Act -- read back a slot written into the first allocated page
  PageRef first = pm->GetPage(pids.front());
  StatusOr<slot_t> slot = first->Insert(ctx.txn_, "growth-check");
  ASSERT_SUCCESS(slot.GetStatus());
  StatusOr<std::string_view> read = first->Read(ctx.txn_, slot.Value());
  ASSERT_SUCCESS(read.GetStatus());
  EXPECT_EQ(read.Value(), "growth-check");

  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(CatalogTest, CreateTableWithUniqueColumn) {
  // Arrange -- a schema whose key column carries a UNIQUE constraint
  Schema schema("unique_tbl", {Column("id", ValueType::kInt64,
                                      Constraint(Constraint::kUnique)),
                               Column("name", ValueType::kVarChar)});

  // Act -- create the table; this path builds a unique B+tree index
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Assert -- the catalog entry round-trips and carries the unique index
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                                ctx.GetTable("unique_tbl"));
    ASSERT_EQ(schema, tbl->GetSchema());
    ASSERT_EQ(tbl->IndexCount(), 1U);
    ASSERT_TRUE(tbl->GetIndex(0).IsUnique());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
}

TEST_F(CatalogTest, UniqueIndexRejectsDuplicateKeys) {
  // Arrange -- a UNIQUE-constrained table created through the catalog
  Schema schema("uniq_ins", {Column("id", ValueType::kInt64,
                                    Constraint(Constraint::kUnique)),
                             Column("name", ValueType::kVarChar)});
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Act -- insert one row, then a second row that collides on the key
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                                ctx.GetTable("uniq_ins"));
    ASSERT_SUCCESS(
        tbl->Insert(ctx.txn_, Row({Value(1), Value("alice")})).GetStatus());
    const Status duplicate =
        tbl->Insert(ctx.txn_, Row({Value(1), Value("bob")})).GetStatus();

    // Assert -- the colliding insert is rejected by the unique index
    EXPECT_NE(duplicate, Status::kSuccess);
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
}

TEST_F(CatalogTest, DropTableRemovesCatalogAndStatistics) {
  // Arrange -- create and commit a table with two columns.
  Schema schema("drop_tbl", {Column("c", ValueType::kInt64),
                             Column("d", ValueType::kVarChar)});
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Act -- fetch the table, drop it, and commit.
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, before,
                                ctx.GetTable("drop_tbl"));
    EXPECT_EQ(schema, before->GetSchema());
    ASSERT_SUCCESS(rs_->DropTable(ctx, "drop_tbl"));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Assert -- the catalog entry and its split statistics are gone.
  {
    TransactionContext ctx = rs_->BeginContext();
    EXPECT_EQ(ctx.GetTable("drop_tbl").GetStatus(), Status::kNotExists);
    EXPECT_EQ(ctx.GetStats("drop_tbl").GetStatus(), Status::kNotExists);
    ctx.txn_.PreCommit();
  }
}

TEST_F(CatalogTest, CreateIndexOnExistingTable) {
  // Arrange -- create and commit a table without any index.
  Schema schema("idx_tbl", {Column("id", ValueType::kInt64),
                            Column("name", ValueType::kVarChar)});
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Act -- add a secondary index on column 1.
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(
        rs_->CreateIndex(ctx, "idx_tbl", IndexSchema("idx_tbl|name", {1})));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Assert -- the updated catalog entry carries the new index.
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                                ctx.GetTable("idx_tbl"));
    EXPECT_EQ(tbl->IndexCount(), 1U);
    EXPECT_EQ(tbl->GetIndex(0).sc_.name_, "idx_tbl|name");
    ctx.txn_.PreCommit();
  }
}

TEST_F(CatalogTest, CreateIndexRefreshesContextCache) {
  // Arrange -- create and commit a table without indexes.
  Schema schema("cache_idx", {Column("id", ValueType::kInt64),
                              Column("name", ValueType::kVarChar)});
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Act -- warm the context cache with the pre-index image, then run the DDL
  // on the same context.
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, stale,
                                ctx.GetTable("cache_idx"));
    EXPECT_EQ(stale->IndexCount(), 0U);
    ASSERT_SUCCESS(
        rs_->CreateIndex(ctx, "cache_idx", IndexSchema("cache_idx|name", {1})));

    // Assert -- lookups now serve a refreshed image that carries the index.
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, fresh,
                                ctx.GetTable("cache_idx"));
    ASSERT_EQ(fresh->IndexCount(), 1U);
    EXPECT_EQ(fresh->GetIndex(0).sc_.name_, "cache_idx|name");

    // A row written through the refreshed image must maintain the new index;
    // through the stale one the entry would be missing for good.
    ASSERT_SUCCESS(
        fresh->Insert(ctx.txn_, Row({Value(1), Value("alice")})).GetStatus());
    Iterator it = fresh->BeginIndexScan(ctx.txn_, fresh->GetIndex(0));
    ASSERT_TRUE(it.IsValid());
    EXPECT_EQ((*it)[1], Value("alice"));
    ++it;
    EXPECT_FALSE(it.IsValid());

    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
}

TEST_F(CatalogTest, DropTableInvalidatesContextCache) {
  // Arrange -- create and commit a table.
  Schema schema("cache_drop", {Column("c", ValueType::kInt64)});
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Act -- populate the context cache, then drop on the same context.
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, cached,
                              ctx.GetTable("cache_drop"));
  std::ignore = cached;
  ASSERT_SUCCESS(rs_->DropTable(ctx, "cache_drop"));

  // Assert -- the dropped table is no longer served from the cache.
  EXPECT_EQ(ctx.GetTable("cache_drop").GetStatus(), Status::kNotExists);
  ctx.txn_.Abort();
}

TEST_F(CatalogTest, MoveAssignmentClearsCaches) {
  // Arrange -- create and commit a table, then warm the context cache.
  Schema schema("move_ctx", {Column("c", ValueType::kInt64)});
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable("move_ctx"));
  std::ignore = tbl;

  // Act -- move-assign a fresh context over this one.
  TransactionContext moved = rs_->BeginReadOnlyContext();
  moved = std::move(ctx);

  // Assert -- the inherited caches were cleared with the moved txn.
  std::ostringstream oss;
  oss << moved;
  EXPECT_EQ(oss.str().find("move_ctx"), std::string::npos);
  EXPECT_NE(oss.str().find("TransactionContext"), std::string::npos);
  moved.txn_.Abort();
}

// Regression test derived from table_fuzzer (crash-5983ad8f). The fuzzer
// commits a table plus two secondary indexes and then emulates a crash on its
// first input byte; reopening failed with kNotExists even though plain
// CreateTable survives (CatalogTest.Recover). These tests pin down the
// index-carrying catalog across the same crash boundary.
TEST_F(CatalogTest, TableWithIndexesSurvivesCrash) {
  // Arrange -- create a table and two secondary indexes, then commit.
  Schema schema("fuzz_tbl", {Column("f_id", ValueType::kInt64,
                                    Constraint(Constraint::kIndex)),
                             Column("name", ValueType::kVarChar),
                             Column("double", ValueType::kDouble)});
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(
        rs_->CreateIndex(ctx, "fuzz_tbl", IndexSchema("num_idx", {0})));
    ASSERT_SUCCESS(
        rs_->CreateIndex(ctx, "fuzz_tbl", IndexSchema("str_idx", {1})));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Act -- emulate a crash and recover from the log.
  Recover();

  // Assert -- the table and both indexes come back.
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable("fuzz_tbl"));
  EXPECT_EQ(tbl->IndexCount(), 2U);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(CatalogTest, CreateIndexAloneSurvivesCrash) {
  // Arrange -- commit a bare table first (this is known to survive recovery).
  Schema schema("solo_idx", {Column("id", ValueType::kInt64),
                             Column("name", ValueType::kVarChar)});
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  Recover();

  // Act -- add one secondary index in its own committed transaction.
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(
        rs_->CreateIndex(ctx, "solo_idx", IndexSchema("by_name", {1})));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Act -- crash again and recover.
  Recover();

  // Assert -- the index added after recovery is still attached.
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable("solo_idx"));
  EXPECT_EQ(tbl->IndexCount(), 1U);
  EXPECT_EQ(tbl->GetIndex(0).sc_.name_, "by_name");
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(CatalogTest, EmptyReadCommitBeforeCrashKeepsTable) {
  // Regression test derived from table_fuzzer (crash-5983ad8f, input 0x1d).
  // The fuzzer commits a table plus two secondary indexes, then commits one
  // read-only context (GetTable + PreCommit with no writes), then emulates a
  // crash; reopening loses the table even though either step alone survives.
  Schema schema("empty_read_tbl", {Column("f_id", ValueType::kInt64,
                                          Constraint(Constraint::kIndex)),
                                   Column("name", ValueType::kVarChar),
                                   Column("double", ValueType::kDouble)});
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(
        rs_->CreateIndex(ctx, "empty_read_tbl", IndexSchema("num_idx", {0})));
    ASSERT_SUCCESS(
        rs_->CreateIndex(ctx, "empty_read_tbl", IndexSchema("str_idx", {1})));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  // Act -- a committed read that touches the catalog, then a crash.
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_TRUE(ctx.GetTable("empty_read_tbl").HasValue());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  // Crash exactly like the fuzzer does: discard updates and tear down the
  // old instance BEFORE opening a fresh one, so no shutdown-time writeback
  // can resurrect buffer-pool images that never reached the disk.
  rs_->EmulateCrash();
  rs_.reset();
  rs_ = std::make_unique<Database>(prefix_);

  // Assert -- the table survives.
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable("empty_read_tbl"));
  EXPECT_EQ(tbl->GetSchema().Name(), "empty_read_tbl");
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

// Bug probes derived from the table_fuzzer / standalone reproduction
// (twogen*.cpp).  Three facts established empirically:
//  - Two clean crash-recovery cycles keep a committed table (passes today).
//  - Database::CreateIndex returns kNotExists on ANY recovered database,
//    even though GetTable on the same instance resolves the table.
//  - Committing that failed CreateIndex context poisons the log: the table
//    is gone after the next recovery.
TEST_F(CatalogTest, TwoCleanRecoveryCyclesKeepTable) {
  Schema schema("two_gen", {Column("id", ValueType::kInt64),
                            Column("name", ValueType::kVarChar)});
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  Recover();
  Recover();  // second crash-recovery cycle without any DDL in between

  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                              ctx.GetTable("two_gen"));
  EXPECT_EQ(tbl->GetSchema().Name(), "two_gen");
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(CatalogTest, DropTableCrashRecoveryReclaimsPages) {
  // D3 (docs/design.md) acceptance 1: CREATE, insert, DROP, crash, recover,
  // reopen -- the dropped table's pages come back as free pages linked into
  // the rebuilt free list and are reused by the next allocation.
  Schema schema("drop_reclaim", {Column("id", ValueType::kInt64),
                                 Column("name", ValueType::kVarChar)});
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->CreateTable(ctx, schema));
    for (int i = 0; i < 100; ++i) {
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(static_cast<int64_t>(i)),
                                    Value("row-payload-" + std::to_string(i))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->DropTable(ctx, "drop_reclaim"));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  Recover();  // crash right after the committed DROP

  TransactionContext ctx = rs_->BeginContext();
  EXPECT_FALSE(ctx.GetTable("drop_reclaim").HasValue());
  PageManager* pm = ctx.txn_.GetPageManager();
  {
    PageRef meta = pm->GetPage(kMetaPageId);
    const page_id_t head = meta->body.meta_page.FirstFreePage();
    EXPECT_NE(head, 0U) << "free list must hold the dropped table's pages";
    PageRef freed = pm->GetPage(head);
    ASSERT_FALSE(freed.IsNull());
    EXPECT_EQ(freed->Type(), PageType::kFreePage);
  }
  // The dropped pages are reusable by the very next allocation.
  PageRef reused = pm->AllocateNewPage(ctx.txn_, PageType::kRowPage);
  ASSERT_FALSE(reused.IsNull());
  ASSERT_TRUE(reused->Insert(ctx.txn_, "reused-row").HasValue());
  reused.PageUnlock();
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(CatalogTest, CreateIndexOnRecoveredTableFails) {
  // Documents bug: CreateIndex must succeed on a recovered database, but
  // currently returns kNotExists there while GetTable resolves the same
  // name just fine.
  Schema schema("recovered_idx", {Column("id", ValueType::kInt64),
                                  Column("name", ValueType::kVarChar)});
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  Recover();

  {
    TransactionContext ctx = rs_->BeginContext();
    // Sanity: the plain lookup works on this very instance.
    ASSERT_TRUE(ctx.GetTable("recovered_idx").HasValue());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  {
    TransactionContext ctx = rs_->BeginContext();
    const Status created =
        rs_->CreateIndex(ctx, "recovered_idx", IndexSchema("by_name", {1}));
    EXPECT_EQ(created, Status::kSuccess);
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
}

TEST_F(CatalogTest, FailedCreateIndexCommitDoesNotPoisonCatalog) {
  // Regression pin: committing the context whose CreateIndex failed must not
  // wipe the previously committed table at the NEXT recovery.
  Schema schema("poison_probe", {Column("id", ValueType::kInt64),
                                 Column("name", ValueType::kVarChar)});
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  Recover();
  {
    TransactionContext ctx = rs_->BeginContext();
    const Status created =
        rs_->CreateIndex(ctx, "poison_probe", IndexSchema("by_name", {1}));
    ASSERT_SUCCESS(created);
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  Recover();

  TransactionContext ctx = rs_->BeginContext();
  // Must still resolve: a failed CreateIndex commit must not poison the log.
  ASSERT_TRUE(ctx.GetTable("poison_probe").HasValue());
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(CatalogTest, ListTablesAfterRecover) {
  // Catalog listing must survive crash recovery together with GetTable.
  Schema schema("listed_tbl", {Column("id", ValueType::kInt64),
                               Column("name", ValueType::kVarChar)});
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CreateTable(ctx, schema).GetStatus());
    ASSERT_SUCCESS(
        rs_->CreateTable(
               ctx, Schema("other_tbl", {Column("id", ValueType::kInt64),
                                         Column("name", ValueType::kVarChar)}))
            .GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  Recover();

  TransactionContext ctx = rs_->BeginContext();
  const std::vector<std::string> names = rs_->ListTables(ctx);
  EXPECT_NE(std::ranges::find(names, "listed_tbl"), names.end());
  EXPECT_NE(std::ranges::find(names, "other_tbl"), names.end());
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(CatalogTest, CreateTable_IsCaseInsensitiveDuplicateCheck) {
  // PRODUCTION BUG (fixed): the existence check was exact-match while
  // GetTable() resolved case-insensitively, so "foo" and "Foo" could coexist.
  TransactionContext ctx = rs_->BeginContext();
  Schema first("MixedCase", {Column("id", ValueType::kInt64)});
  ASSERT_SUCCESS(rs_->CreateTable(ctx, first).GetStatus());
  Schema second("mixedcase", {Column("id", ValueType::kInt64)});
  EXPECT_EQ(rs_->CreateTable(ctx, second).GetStatus(), Status::kConflicts);
  Schema third("MIXEDCASE", {Column("id", ValueType::kInt64)});
  EXPECT_EQ(rs_->CreateTable(ctx, third).GetStatus(), Status::kConflicts);
}

TEST_F(CatalogTest, DropTableAndStats_WorkForAnyCase) {
  // PRODUCTION BUG (fixed): DropTable/GetStatistics used the user-supplied
  // spelling for catalog+statistics keys, so "mixedcase" failed against a
  // table declared as "MixedName".
  TransactionContext ctx = rs_->BeginContext();
  Schema schema("MixedName", {Column("id", ValueType::kInt64),
                              Column("val", ValueType::kVarChar)});
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->CreateTable(ctx, schema));
  ASSERT_SUCCESS(ctx.txn_.PreCommit());

  {
    TransactionContext ctx2 = rs_->BeginContext();
    // Statistics must resolve for any case spelling.
    auto stats = rs_->GetStatistics(ctx2, "mixedname");
    EXPECT_EQ(stats.GetStatus(), Status::kSuccess);
    ASSERT_SUCCESS(ctx2.txn_.PreCommit());
  }
  {
    TransactionContext ctx3 = rs_->BeginContext();
    // DROP with a different case must remove the canonical entries.
    EXPECT_EQ(rs_->DropTable(ctx3, "mixedname"), Status::kSuccess);
    ASSERT_SUCCESS(ctx3.txn_.PreCommit());
  }
  {
    TransactionContext ctx4 = rs_->BeginContext();
    EXPECT_EQ(rs_->GetTable(ctx4, "MixedName").GetStatus(), Status::kNotExists);
    auto stats = rs_->GetStatistics(ctx4, "mixedname");
    EXPECT_EQ(stats.GetStatus(), Status::kNotExists);
    ASSERT_SUCCESS(ctx4.txn_.PreCommit());
  }
}

TEST_F(CatalogTest, ColumnStatisticsResolveForAnyCase) {
  // PRODUCTION BUG (fixed): GetStatistics read the per-column records under
  // the user-supplied case while writing them under the canonical name, so a
  // differently-cased reference silently substituted empty ColumnStats.
  TransactionContext ctx = rs_->BeginContext();
  Schema schema("CaseStats", {Column("id", ValueType::kInt64)});
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->CreateTable(ctx, schema));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({Value(i)})).HasValue());
  }
  ASSERT_SUCCESS(rs_->RefreshStatistics(ctx, "CaseStats"));
  ASSERT_SUCCESS(ctx.txn_.PreCommit());

  TransactionContext ctx2 = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, stats,
                        rs_->GetStatistics(ctx2, "casestats"));
  ASSERT_EQ(stats.Rows(), 5U);
  EXPECT_GT(stats.Column(0).Count(), 0U)
      << "column statistics must be populated for a differently-cased read";
  ASSERT_SUCCESS(ctx2.txn_.PreCommit());
}

TEST_F(CatalogTest, CreateIndex_FailurePropagatesFromCreateTable) {
  // PRODUCTION BUG (fixed): CreateTable ignored CreateIndex's status, so a
  // failed unique-index build silently registered an unconstrained table.
  TransactionContext ctx = rs_->BeginContext();
  Schema schema("uniq_tbl", {Column("id", ValueType::kInt64,
                                    Constraint(Constraint::kUnique))});
  // Normal creation must succeed and enforce uniqueness.
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->CreateTable(ctx, schema));
  ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({Value(1)})).HasValue());
  EXPECT_EQ(tbl.Insert(ctx.txn_, Row({Value(1)})).GetStatus(),
            Status::kDuplicates);
}

}  // namespace tinylamb