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

#include "full_scan_iterator.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <future>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/platform/file_io.hpp"
#include "common/platform/process.hpp"
#include "common/random_string.hpp"
#include "common/runtime_filter.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "gtest/gtest.h"
#include "iterator.hpp"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

void ForceScanWalFailure(Logger* logger) {
  const int full_fd = platform::OpenWriteFailingDescriptor();
  ASSERT_GE(full_fd, 0);
  ASSERT_EQ(platform::Dup2(full_fd, logger->Fd()), logger->Fd());
  ASSERT_EQ(platform::CloseFile(full_fd), 0);
  std::ignore = logger->AddLog("dead");
  ASSERT_NE(logger->WaitForDurable(logger->BufferedLSN()), Status::kSuccess);
  ASSERT_TRUE(logger->Failed());
}

}  // namespace

class FullScanIteratorTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "full_scan_iterator_test-" + RandomString();
    Recover();
    Schema sc("SampleTable", {Column("col1", ValueType::kInt64,
                                     Constraint(Constraint::kIndex)),
                              Column("col2", ValueType::kVarChar),
                              Column("col3", ValueType::kDouble)});
    TransactionContext ctx = db_->BeginContext();
    ASSERT_SUCCESS(db_->CreateTable(ctx, sc).GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  void Recover() {
    if (db_) {
      db_->EmulateCrash();
      db_.reset();
    }
    db_ = Database::Create(prefix_).MoveValue();
  }

  void TearDown() override { db_->DeleteAll(); }

  std::string prefix_;
  std::unique_ptr<Database> db_;
};

TEST_F(FullScanIteratorTest, Construct_Default_Succeeds) {}

// A non-empty columnar store built before a committed row mutation is stale,
// not a reason to keep declining rebuilds: columnar acceleration must come
// back on the next read transaction.
TEST_F(FullScanIteratorTest, StaleNonEmptyLSTableStoreIsRebuilt) {
  {
    auto ctx = db_->BeginContext();
    auto tbl = db_->GetTable(ctx, "SampleTable").MoveValue();
    for (int64_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(i), Value("x"),
                                               Value(static_cast<double>(i))}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  std::shared_ptr<const LSTableStore> first;
  {
    auto reader = db_->BeginReadOnlyContext();
    auto tbl = db_->GetTable(reader, "SampleTable").MoveValue();
    first = GetOrBuildLSTableStore(reader.txn_, tbl, true);
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first->total_rows, 10U);
    ASSERT_SUCCESS(reader.txn_.PreCommit());
  }
  {
    auto writer = db_->BeginContext();
    auto tbl = db_->GetTable(writer, "SampleTable").MoveValue();
    ASSERT_SUCCESS(
        tbl.Insert(writer.txn_, Row({Value(10), Value("y"), Value(2.0)}))
            .GetStatus());
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  auto fresh = db_->BeginReadOnlyContext();
  auto fresh_table = db_->GetTable(fresh, "SampleTable").MoveValue();
  // Install the stale non-empty image to exercise the exact gate that used to
  // permanently disable lazy rebuilds.
  fresh_table.SetLSTableStore(first);
  const auto rebuilt = GetOrBuildLSTableStore(fresh.txn_, fresh_table, true);
  ASSERT_NE(rebuilt, nullptr);
  EXPECT_EQ(rebuilt->total_rows, 11U);
  EXPECT_NE(rebuilt, first);
  ASSERT_SUCCESS(fresh.txn_.PreCommit());
}

TEST(LSTableIntegerRunsTest, ExactGapsConcurrentReuseAndBudgetedLifetime) {
  auto& budget = ProcessMemoryBudget::Global();
  const auto before = budget.ColumnarUsed();
  std::shared_ptr<const LSTableIntegerRuns> retained;
  {
    LSTableStore store;
    store.total_rows = 8;
    store.integer_columns = {{-3, -3, 0, 0, 0, 4, 4, 4}};
    store.column_has_null = {0};
    store.column_is_monotonic = {1};
    std::vector<std::future<std::shared_ptr<const LSTableIntegerRuns>>> readers;
    readers.reserve(4);
for (int i = 0; i < 4; ++i) {
      readers.push_back(
          std::async(std::launch::async, [&] { return store.IntegerRuns(0); }));
    }
    retained = readers[0].get();
    ASSERT_NE(retained, nullptr);
    EXPECT_EQ(retained->max_run_rows, 3);
    for (size_t i = 1; i < readers.size(); ++i) {
      EXPECT_EQ(readers[i].get(), retained);
}
    for (int64_t key = -5; key <= 6; ++key) {
      const auto [begin, end] = retained->Find(key);
      const auto& values = store.integer_columns[0];
      const auto first = std::lower_bound(values.begin(), values.end(), key);
      const auto last = std::upper_bound(values.begin(), values.end(), key);
      EXPECT_EQ(end - begin, static_cast<size_t>(last - first));
      if (begin != end) {
        EXPECT_EQ(begin, static_cast<size_t>(first - values.begin()));
}
    }
    EXPECT_GT(budget.ColumnarUsed(), before);
  }
  EXPECT_GT(budget.ColumnarUsed(), before);
  retained.reset();
  EXPECT_EQ(budget.ColumnarUsed(), before);
}

TEST(LSTableIntegerRunsTest, MaximumRunBoundsIncludeSkewAndEndpoints) {
  for (const auto& keys : std::vector<std::vector<int64_t>>{
           {1},
           {1, 1, 1, 1},
           {1, 1, 1, 1, 2},
           {1, 2, 2, 2, 2},
           {std::numeric_limits<int64_t>::min(),
            std::numeric_limits<int64_t>::min(),
            std::numeric_limits<int64_t>::min() + 2},
           {std::numeric_limits<int64_t>::max() - 2,
            std::numeric_limits<int64_t>::max(),
            std::numeric_limits<int64_t>::max()}}) {
    LSTableStore store;
    store.total_rows = keys.size();
    store.integer_columns = {keys};
    store.column_has_null = {0};
    store.column_is_monotonic = {1};
    const auto index = store.IntegerRuns(0);
    ASSERT_NE(index, nullptr);
    uint32_t expected = 0;
    for (int64_t key : keys) {
      expected = std::max(expected, static_cast<uint32_t>(std::count(
                                        keys.begin(), keys.end(), key)));
}
    EXPECT_EQ(index->max_run_rows, expected);
    uint32_t runs = 1;
    for (size_t i = 1; i < keys.size(); ++i) { runs += static_cast<uint32_t>(keys[i] != keys[i - 1]);
}
    EXPECT_EQ(index->run_count, runs);
  }
}

TEST(LSTableIntegerRunsTest, RefusesUnprovenSparseAndUnbudgetedInputs) {
  LSTableStore store;
  store.total_rows = 3;
  store.integer_columns = {{0, 0, 1}};
  store.column_has_null = {1};
  store.column_is_monotonic = {1};
  EXPECT_EQ(store.IntegerRuns(0), nullptr);
  store.column_has_null[0] = 0;
  store.column_is_monotonic[0] = 0;
  EXPECT_EQ(store.IntegerRuns(0), nullptr);
  store.column_is_monotonic[0] = 1;
  store.integer_columns[0] = {0, 2, 1};
  EXPECT_EQ(store.IntegerRuns(0), nullptr);
  store.integer_columns[0] = {INT64_MIN, 0, INT64_MAX};
  EXPECT_EQ(store.IntegerRuns(0), nullptr);
  store.integer_columns[0] = {0, 0, 1};
  auto& budget = ProcessMemoryBudget::Global();
  struct RestoreLimit {
    size_t old = ProcessMemoryBudget::Global().ColumnarPoolLimit();
    ~RestoreLimit() {
      ProcessMemoryBudget::Global().SetColumnarPoolLimitForTest(old);
    }
  } restore;
  budget.SetColumnarPoolLimitForTest(1);
  EXPECT_EQ(store.IntegerRuns(0), nullptr);
  budget.SetColumnarPoolLimitForTest(restore.old);
  EXPECT_NE(store.IntegerRuns(0), nullptr);
}

TEST(LSTableIntegerRunsTest, RunBoundsKeepExactExtremaAndBudgetedLifetime) {
  auto& budget = ProcessMemoryBudget::Global();
  for (int64_t base : {INT64_MIN, int64_t{-32768}, INT64_MAX - 65535}) {
    const auto before = budget.ColumnarUsed();
    std::shared_ptr<const LSTableIntegerRunBounds> retained;
    {
      LSTableStore store;
      store.total_rows = 8;
      store.integer_columns = {{-3, -3, 0, 0, 0, 4, 4, 4},
                               {base, base + 10, base + 65535, base + 7,
                                base + 20, base + 1, base + 11, base + 100}};
      store.column_min_int = {-3, base};
      store.column_max_int = {4, base + 65535};
      store.column_has_null = {0, 0};
      store.column_is_monotonic = {1, 0};
      std::vector<std::future<std::shared_ptr<const LSTableIntegerRunBounds>>>
          readers;
      readers.reserve(4);
for (int i = 0; i < 4; ++i) {
        readers.push_back(std::async(
            std::launch::async, [&] { return store.IntegerRunBounds(0, 1); }));
}
      retained = readers[0].get();
      ASSERT_NE(retained, nullptr);
      for (size_t i = 1; i < readers.size(); ++i) {
        EXPECT_EQ(readers[i].get(), retained);
}
      for (int64_t key : {-3, 0, 4}) {
        int64_t lo = INT64_MAX, hi = INT64_MIN;
        for (size_t r = 0; r < store.total_rows; ++r) {
          if (store.integer_columns[0][r] != key) { continue;
}
          lo = std::min(lo, store.integer_columns[1][r]);
          hi = std::max(hi, store.integer_columns[1][r]);
        }
        const uint32_t packed = retained->packed[key + 3];
        EXPECT_EQ(retained->value_minimum + (packed & 65535), lo);
        EXPECT_EQ(retained->value_minimum + (packed >> 16), hi);
        EXPECT_TRUE(retained->MayOverlap(key, lo, hi));
        EXPECT_TRUE(retained->MayOverlap(key, lo, lo));
        EXPECT_TRUE(retained->MayOverlap(key, hi, hi));
        EXPECT_TRUE(retained->MayOverlap(key, INT64_MIN, INT64_MAX));
        EXPECT_FALSE(retained->MayOverlap(key, hi, lo - (lo != INT64_MIN)));
        if (lo != INT64_MIN) {
          EXPECT_FALSE(retained->MayOverlap(key, INT64_MIN, lo - 1));
}
        if (hi != INT64_MAX) {
          EXPECT_FALSE(retained->MayOverlap(key, hi + 1, INT64_MAX));
}
        EXPECT_FALSE(retained->MayExceed(key, hi));
        EXPECT_TRUE(retained->MayExceed(key, hi - 1));
      }
      EXPECT_FALSE(retained->MayExceed(-4, INT64_MIN));
      EXPECT_FALSE(retained->MayExceed(5, INT64_MIN));
    }
    EXPECT_GT(budget.ColumnarUsed(), before);
    retained.reset();
    EXPECT_EQ(budget.ColumnarUsed(), before);
  }
}

TEST(LSTableIntegerRunsTest, SparseRunBoundsKeepHolesRanksAndEndpoints) {
  for (int64_t base : {INT64_MIN, int64_t{-4096}, INT64_MAX - 5000}) {
    LSTableStore store;
    store.integer_columns.resize(2);
    for (int64_t i = 0; i <= 1100; ++i) {
      const int64_t key = base + (i * 4);
      for (int64_t v : {i % 17, (i % 17) + 3}) {
        store.integer_columns[0].push_back(key);
        store.integer_columns[1].push_back(v);
      }
    }
    store.total_rows = store.integer_columns[0].size();
    store.column_has_null = {0, 0};
    store.column_is_monotonic = {1, 0};
    const auto bounds = store.IntegerRunBounds(0, 1);
    ASSERT_NE(bounds, nullptr);
    ASSERT_FALSE(bounds->key_bits.empty());
    EXPECT_EQ(bounds->packed.size(), 1101);
    for (int64_t offset = 0; offset <= 4400; ++offset) {
      const int64_t key = base + offset;
      if (offset % 4 != 0) {
        EXPECT_FALSE(bounds->MayOverlap(key, INT64_MIN, INT64_MAX));
        continue;
      }
      const int64_t lo = (offset / 4) % 17, hi = lo + 3;
      EXPECT_TRUE(bounds->MayOverlap(key, lo, lo));
      EXPECT_TRUE(bounds->MayOverlap(key, hi, hi));
      EXPECT_FALSE(bounds->MayOverlap(key, hi + 1, INT64_MAX));
      EXPECT_TRUE(bounds->MayExceed(key, hi - 1));
      EXPECT_FALSE(bounds->MayExceed(key, hi));
    }
    EXPECT_FALSE(bounds->MayOverlap(base + 4401, INT64_MIN, INT64_MAX));
    if (base != INT64_MIN) {
      EXPECT_FALSE(bounds->MayOverlap(base - 1, INT64_MIN, INT64_MAX));
}
  }
}

TEST(LSTableIntegerRunsTest, CoarseRunBoundsNeverRejectMatchingRows) {
  for (int64_t base : {INT64_MIN, int64_t{-32768}, INT64_MAX - 65535}) {
    LSTableStore store;
    store.integer_columns.resize(2);
    for (int64_t i = 0; i <= 1100; ++i) {
      for (int64_t delta : {(i % 256) * 257, (i % 256) * 257 / 2}) {
        store.integer_columns[0].push_back(i * 4);
        store.integer_columns[1].push_back(base + delta);
      }
    }
    store.total_rows = store.integer_columns[0].size();
    store.column_has_null = {0, 0};
    store.column_is_monotonic = {1, 0};
    ASSERT_NE(store.IntegerRuns(0), nullptr);
    auto& budget = ProcessMemoryBudget::Global();
    struct RestoreLimit {
      size_t saved = ProcessMemoryBudget::Global().ColumnarPoolLimit();
      ~RestoreLimit() {
        ProcessMemoryBudget::Global().SetColumnarPoolLimitForTest(saved);
      }
    } restore;
    const size_t before = budget.ColumnarUsed();
    budget.SetColumnarPoolLimitForTest(before +
                                       sizeof(LSTableIntegerRunBounds) + 3500);
    const auto bounds = store.IntegerRunBounds(0, 1);
    ASSERT_NE(bounds, nullptr);
    ASSERT_FALSE(bounds->coarse.empty());
    EXPECT_TRUE(bounds->packed.empty());
    EXPECT_EQ(bounds->BoundsCount(), 1101);
    EXPECT_LE(budget.ColumnarUsed() - before,
              sizeof(LSTableIntegerRunBounds) + 3500);
    for (size_t row = 0; row < store.total_rows; ++row) {
      const int64_t key = store.integer_columns[0][row];
      const int64_t value = store.integer_columns[1][row];
      EXPECT_TRUE(bounds->MayOverlap(key, value, value));
      if (value != INT64_MIN) { EXPECT_TRUE(bounds->MayExceed(key, value - 1));
}
      const auto [lo, hi] = bounds->ValueBounds(bounds->BoundsOffset(key));
      EXPECT_LE(lo, value);
      EXPECT_GE(hi, value);
      EXPECT_GE(lo, base);
      EXPECT_LE(hi, base + 65535);
      if (lo != INT64_MIN) {
        EXPECT_FALSE(bounds->MayOverlap(key, INT64_MIN, lo - 1));
}
      if (hi != INT64_MAX) {
        EXPECT_FALSE(bounds->MayOverlap(key, hi + 1, INT64_MAX));
}
      EXPECT_FALSE(bounds->MayOverlap(key, 1, 0));
      EXPECT_FALSE(bounds->MayOverlap(key + 1, INT64_MIN, INT64_MAX));
    }
  }
}

TEST(LSTableIntegerRunsTest, RunBoundsRefuseNullWideAndUnbudgetedColumns) {
  for (int mode : {0, 1, 2}) {
    LSTableStore store;
    store.total_rows = 3;
    store.integer_columns = {
        {0, 0, 1},
        {INT64_MIN, mode == 1 ? INT64_MAX : INT64_MIN + 3, INT64_MIN + 1}};
    store.column_has_null = {0, static_cast<uint8_t>(mode == 0)};
    store.column_is_monotonic = {1, 0};
    ASSERT_NE(store.IntegerRuns(0), nullptr);
    if (mode < 2) {
      EXPECT_EQ(store.IntegerRunBounds(0, 1), nullptr);
      continue;
    }
    struct RestoreLimit {
      size_t saved = ProcessMemoryBudget::Global().ColumnarPoolLimit();
      ~RestoreLimit() {
        ProcessMemoryBudget::Global().SetColumnarPoolLimitForTest(saved);
      }
    } restore;
    ProcessMemoryBudget::Global().SetColumnarPoolLimitForTest(1);
    size_t reclaims = 0;
    auto& budget = ProcessMemoryBudget::Global();
    const uint64_t hook =
        budget.RegisterReclaimHook(ReclaimStage::kColumnar, [&](size_t) {
          ++reclaims;
          return size_t{0};
        });
    EXPECT_EQ(store.IntegerRunBounds(0, 1), nullptr);
    budget.UnregisterReclaimHook(hook);
    EXPECT_EQ(reclaims, 0);
    ProcessMemoryBudget::Global().SetColumnarPoolLimitForTest(restore.saved);
    EXPECT_NE(store.IntegerRunBounds(0, 1), nullptr);
  }
}

TEST_F(FullScanIteratorTest,
       ProjectionReplacementReleasesIdleImageBeforeBuild) {
  auto load = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      db_->CreateTable(
          load, Schema("ReplaceProjection", {Column("a", ValueType::kInt64),
                                             Column("b", ValueType::kInt64)})));
  for (int64_t i = 0; i < 9000; ++i) {
    ASSERT_SUCCESS(
        table.Insert(load.txn_, Row({Value(i), Value(-i)})).GetStatus());
}
  ASSERT_SUCCESS(load.PreCommit());
  auto reader = db_->BeginReadOnlyContext();
  table.RejectLSTableCapacity(table.LayoutGeneration());
  const std::vector<slot_t> first{0}, second{1};
  auto original = GetOrBuildLSTableStore(reader.txn_, table, true, &first);
  ASSERT_NE(original, nullptr);
  auto& budget = ProcessMemoryBudget::Global();
  constexpr size_t cap = 2 * 1024 * 1024;
  ASSERT_LT(budget.ColumnarUsed(), cap - 65536);
  // Model a large old image without putting MiB of row payload in a unit test.
  ASSERT_TRUE(
      original->memory_charge->TryAdd(cap - 65536 - budget.ColumnarUsed()));
  std::weak_ptr<const LSTableStore> old_image = original;
  original.reset();
  struct RestoreLimit {
    size_t saved = ProcessMemoryBudget::Global().ColumnarPoolLimit();
    ~RestoreLimit() {
      ProcessMemoryBudget::Global().SetColumnarPoolLimitForTest(saved);
    }
  } restore;
  budget.SetColumnarPoolLimitForTest(cap);
  const auto before = budget.TelemetrySnapshot().reclaim_calls;
  const auto replacement =
      GetOrBuildLSTableStore(reader.txn_, table, true, &second);
  ASSERT_NE(replacement, nullptr);
  EXPECT_TRUE(old_image.expired());
  EXPECT_EQ(budget.TelemetrySnapshot().reclaim_calls, before);
  EXPECT_TRUE(replacement->HasColumn(1));
  EXPECT_FALSE(replacement->HasColumn(0));
  EXPECT_EQ(replacement->integer_columns[1].front(), 0);
  EXPECT_EQ(replacement->integer_columns[1].back(), -8999);
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(FullScanIteratorTest, IntegerRunIndexStaysWithItsLayoutImage) {
  auto ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      db_->CreateTable(ctx, Schema("Runs", {Column("k", ValueType::kInt64)})));
  for (int64_t k : {-3, -3, 0, 4, 4}) {
    ASSERT_SUCCESS(table.Insert(ctx.txn_, Row({Value(k)})).GetStatus());
}
  const auto original = BuildLSTableStore(ctx.txn_, table);
  ASSERT_NE(original, nullptr);
  const auto old_index = original->IntegerRuns(0);
  ASSERT_NE(old_index, nullptr);
  const auto old_bounds = original->IntegerRunBounds(0, 0);
  ASSERT_NE(old_bounds, nullptr);
  ASSERT_SUCCESS(table.Insert(ctx.txn_, Row({Value(7)})).GetStatus());
  // The ordinary cache lookup refuses the stale image while a write is
  // pending. An explicit transaction-local rebuild has a separate index.
  EXPECT_EQ(GetOrBuildLSTableStore(ctx.txn_, table, true), nullptr);
  const auto changed = BuildLSTableStore(ctx.txn_, table);
  ASSERT_NE(changed, nullptr);
  EXPECT_NE(changed, original);
  const auto new_index = changed->IntegerRuns(0);
  ASSERT_NE(new_index, nullptr);
  const auto new_bounds = changed->IntegerRunBounds(0, 0);
  ASSERT_NE(new_bounds, nullptr);
  EXPECT_FALSE(old_bounds->MayExceed(7, 6));
  EXPECT_TRUE(new_bounds->MayExceed(7, 6));
  EXPECT_EQ(old_index->Find(7), std::make_pair(0U, 0U));
  EXPECT_EQ(new_index->Find(7), std::make_pair(5U, 6U));
  ASSERT_SUCCESS(table.Insert(ctx.txn_, Row({Value()})).GetStatus());
  EXPECT_EQ(GetOrBuildLSTableStore(ctx.txn_, table, true), nullptr);
  const auto nullable = BuildLSTableStore(ctx.txn_, table);
  ASSERT_NE(nullable, nullptr);
  EXPECT_EQ(nullable->IntegerRuns(0), nullptr);
  EXPECT_EQ(nullable->IntegerRunBounds(0, 0), nullptr);
  EXPECT_TRUE(new_bounds->MayExceed(7, 6));
  EXPECT_EQ(new_index->Find(7), std::make_pair(5U, 6U));
  ctx.Abort();
}

TEST_F(FullScanIteratorTest,
       ExactUniquenessSurvivesRejectedDictionaryEncoding) {
  auto context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      db_->CreateTable(context,
                       Schema("UniqueProof",
                              {Column("k", ValueType::kInt64),
                               Column("duplicate", ValueType::kInt64),
                               Column("nullable", ValueType::kInt64),
                               Column("unique_later", ValueType::kInt64),
                               Column("late_duplicate", ValueType::kInt64)})));
  for (int64_t i = 0; i < 8192; ++i) {
    // Exceed the separate first-column dense-domain uniqueness bitmap.
    const int64_t k = ((i * 4093) % 8192) * 100003;
    ASSERT_SUCCESS(
        table
            .Insert(context.txn_,
                    Row({Value(k), Value(i % 4096), i < 2 ? Value() : Value(k),
                         Value(k), Value(i == 8191 ? 0 : k)}))
            .GetStatus());
  }
  const auto store = BuildLSTableStore(context.txn_, table);
  ASSERT_NE(store, nullptr);
  EXPECT_EQ(store->column_is_monotonic[0], 0);
  EXPECT_EQ(store->dictionaries[0].wide_code_bytes, 0);
  EXPECT_EQ(store->column_is_unique_nonnull[0], 1);
  EXPECT_EQ(store->column_is_unique_nonnull[1], 0);
  EXPECT_EQ(store->column_is_unique_nonnull[2], 0);
  EXPECT_EQ(store->column_is_unique_nonnull[3], 1);
  EXPECT_EQ(store->column_is_unique_nonnull[4], 0);
  ASSERT_SUCCESS(table
                     .Insert(context.txn_, Row({Value(0), Value(0), Value(0),
                                                Value(0), Value(0)}))
                     .GetStatus());
  const auto changed = BuildLSTableStore(context.txn_, table);
  ASSERT_NE(changed, nullptr);
  EXPECT_EQ(changed->column_is_unique_nonnull[3], 0);
  EXPECT_EQ(store->column_is_unique_nonnull[3], 1);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(FullScanIteratorTest,
       AuditRejectedRuntimeFilterPreservesLegacyKeyFilter) {
  auto context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(context, "SampleTable"));
  for (int64_t key : {1, 2, 3}) {
    ASSERT_SUCCESS(
        table.Insert(context.txn_, Row({Value(key), Value("s"), Value(1.0)}))
            .GetStatus());
  }
  std::unordered_set<int64_t> keys{1, 3};
  TableScanOptions options;
  options.key_filter = &keys;
  options.key_column = 0;
  RuntimeFilterBuilder builder;
  builder.Observe(3);
  const auto group = builder.Build();
  for (bool absent : {false, true}) {
    auto iterator = table.BeginFullScan(context.txn_, options);
    iterator.SetRuntimeFilter(absent ? nullptr : group.get(), 1,
                              ValueType::kInt64);
    std::vector<int64_t> actual;
    while (iterator.IsValid()) {
      actual.push_back((*iterator)[0].value.int_value);
      ++iterator;
    }
    EXPECT_EQ(actual, (std::vector<int64_t>{1, 3}));
    EXPECT_EQ(iterator.GetStatus(), Status::kSuccess);
  }
}

TEST_F(FullScanIteratorTest, AuditRuntimeFilterRebuildsPeekForNewColumn) {
  auto context = db_->BeginContext();
  const Schema schema("two_ints", {Column("a", ValueType::kInt64),
                                   Column("b", ValueType::kInt64)});
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->CreateTable(context, schema));
  for (int64_t key : {1, 2, 3}) {
    ASSERT_SUCCESS(table.Insert(context.txn_, Row({Value(key), Value(key + 1)}))
                       .GetStatus());
  }
  const std::unordered_set<int64_t> keys{1, 2, 3};
  TableScanOptions options;
  options.key_filter = &keys;
  options.key_column = 0;
  auto iterator = table.BeginFullScan(context.txn_, options);
  RuntimeFilterBuilder builder;
  builder.Observe(3);
  const auto group = builder.Build();
  iterator.SetRuntimeFilter(group.get(), 1, ValueType::kInt64);
  ASSERT_TRUE(iterator.IsValid());
  EXPECT_EQ((*iterator)[0], Value(2));
  ++iterator;
  EXPECT_FALSE(iterator.IsValid());
  EXPECT_EQ(iterator.GetStatus(), Status::kSuccess);
}

TEST_F(FullScanIteratorTest, NumericStreamsOwnPageSlicesAndPreserveBits) {
  constexpr int64_t kRows = 4097;
  const auto make_row = [](int64_t row) {
    constexpr std::array<uint64_t, 6> kDoubleBits{0,
                                                  0x8000000000000000ULL,
                                                  0x7ff0000000000000ULL,
                                                  0xfff0000000000000ULL,
                                                  0x7ff8000000000042ULL,
                                                  1};
    const int64_t integer = row == 1   ? std::numeric_limits<int64_t>::min()
                            : row == 2 ? std::numeric_limits<int64_t>::max()
                                       : -row;
    return Row({row % 11 == 0 ? Value() : Value(integer),
                row % 13 == 0 ? Value() : Value::DateFromDays(row - 2000),
                row % 17 == 0 ? Value()
                              : Value(std::bit_cast<double>(
                                    kDoubleBits[static_cast<size_t>(row) % 6])),
                Value(), Value(std::string(80, 'p'))});
  };
  {
    auto context = db_->BeginContext();
    const Schema schema(
        "NumericStreams",
        {Column("n", ValueType::kInt64), Column("d", ValueType::kDate),
         Column("f", ValueType::kDouble), Column("z", ValueType::kInt64),
         Column("s", ValueType::kVarChar)});
    ASSERT_SUCCESS(db_->CreateTable(context, schema).GetStatus());
    ASSIGN_OR_ASSERT_FAIL(Table, table,
                          db_->GetTable(context, "NumericStreams"));
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_SUCCESS(table.Insert(context.txn_, make_row(i)).GetStatus());
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  auto reader = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(reader, "NumericStreams"));
  const auto store = BuildLSTableStore(reader.txn_, table);
  ASSERT_NE(store, nullptr);
  ASSERT_GT(store->blocks.size(), 1U);
  ASSERT_EQ(store->integer_columns.size(), 5U);
  EXPECT_TRUE(store->integer_columns[4].empty());
  size_t base = 0;
  for (const auto& block : store->blocks) {
    EXPECT_TRUE(block.columns[4].int_cells.empty());
    for (size_t c = 0; c < 4; ++c) {
      const auto& cells = store->integer_columns[c];
      const auto& column = block.columns[c];
      ASSERT_EQ(cells.size(), kRows);
      EXPECT_EQ(column.int_cells.data(), cells.data() + base);
      ASSERT_EQ(column.int_cells.size(), block.row_count);
      for (size_t r = 0; r < block.row_count; ++r) {
        const auto expected = make_row(static_cast<int64_t>(base + r));
        EXPECT_EQ(column.IsNull(r), expected[c].IsNull());
        const int64_t bits =
            expected[c].IsNull() ? 0
            : c == 2 ? std::bit_cast<int64_t>(expected[c].value.double_value)
                     : expected[c].value.int_value;
        EXPECT_EQ(column.int_cells[r], bits);
        EXPECT_EQ(cells[base + r], bits);
      }
    }
    base += block.row_count;
  }
  EXPECT_EQ(base, kRows);
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(FullScanIteratorTest, NarrowDictionaryStreamsOwnAllPageSlices) {
  constexpr int64_t kRows = 1200;
  const auto text = [](int64_t row) {
    if (row % 17 == 0) {
      return Value();
    }
    if (row % 19 == 0) {
      return Value("");
    }
    std::string value = "東京" + std::string(80, 'p');
    value.push_back('\0');
    value += std::to_string(row % 37);
    return Value(std::move(value));
  };
  {
    auto context = db_->BeginContext();
    const Schema schema(
        "NarrowStreams",
        {Column("n", ValueType::kInt64), Column("d", ValueType::kDouble),
         Column("b", ValueType::kVarChar), Column("s", ValueType::kVarChar)});
    ASSERT_SUCCESS(db_->CreateTable(context, schema).GetStatus());
    ASSIGN_OR_ASSERT_FAIL(Table, table,
                          db_->GetTable(context, "NarrowStreams"));
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_SUCCESS(
          table
              .Insert(
                  context.txn_,
                  Row({Value(i % 7), Value(static_cast<double>(i % 11) * 0.25),
                       Value(std::string(1, static_cast<char>('a' + (i % 7)))),
                       text(i)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  auto reader = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(reader, "NarrowStreams"));
  const auto store = BuildLSTableStore(reader.txn_, table);
  ASSERT_NE(store, nullptr);
  ASSERT_GT(store->blocks.size(), 1U);
  for (size_t c = 0; c < 4; ++c) {
    const auto& dictionary = store->dictionaries[c];
    ASSERT_TRUE(dictionary.active);
    ASSERT_EQ(dictionary.narrow_codes.size(), kRows);
    EXPECT_TRUE(dictionary.wide_codes.empty());
    size_t base = 0;
    for (const auto& block : store->blocks) {
      const auto& column = block.columns[c];
      EXPECT_EQ(column.dict_codes.data(),
                dictionary.narrow_codes.data() + base);
      EXPECT_EQ(column.dict_codes.size(), block.row_count);
      EXPECT_EQ(column.DictionaryCodeAt(block.row_count), UINT32_MAX);
      for (size_t r = 0; r < block.row_count; ++r) {
        const size_t row = base + r;
        const uint32_t code = column.DictionaryCodeAt(r);
        EXPECT_EQ(code, dictionary.DictionaryCodeAt(row));
        if (c == 0) {
          ASSERT_LT(code, dictionary.int_entries.size());
          EXPECT_EQ(dictionary.int_entries[code], row % 7);
        } else if (c == 1) {
          ASSERT_LT(code, dictionary.double_entries.size());
          EXPECT_EQ(dictionary.double_entries[code],
                    static_cast<double>(row % 11) * 0.25);
        } else if (c == 2) {
          EXPECT_EQ(column.VarcharAt(r),
                    std::string(1, static_cast<char>('a' + (row % 7))));
        } else {
          const auto expected = text(static_cast<int64_t>(row));
          EXPECT_EQ(column.IsNull(r), expected.IsNull());
          if (expected.IsNull()) {
            EXPECT_EQ(code, LSTableColumnDictionary::kNullCode);
          } else {
            EXPECT_EQ(column.VarcharAt(r), expected.value.varchar_value);
          }
        }
      }
      base += block.row_count;
    }
    EXPECT_EQ(base, kRows);
    EXPECT_EQ(dictionary.DictionaryCodeAt(base), UINT32_MAX);
  }
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(FullScanIteratorTest, PackedIntegerDictionaryKeepsNullsAndBoundaries) {
  for (const int64_t cardinality : {int64_t{1}, int64_t{300}, int64_t{65536}}) {
    const std::string name = "PackedIntegers" + std::to_string(cardinality);
    const int64_t rows = std::max(int64_t{8193}, (cardinality * 3) + 1);
    const auto value_at = [cardinality](int64_t i) {
      if (i % 17 == 0) {
        return Value();
      }
      const int64_t code = i % cardinality;
      return Value(cardinality == 1 ? 42
                   : code == 0      ? INT64_MIN
                   : code == 1      ? INT64_MAX
                                    : code - 150);
    };
    {
      auto setup = db_->BeginContext();
      const Schema schema(
          name, {Column("n", ValueType::kInt64), Column("k", ValueType::kInt64),
                 Column("d", ValueType::kDate)});
      ASSERT_SUCCESS(db_->CreateTable(setup, schema).GetStatus());
      ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(setup, name));
      for (int64_t i = 0; i < rows; ++i) {
        ASSERT_SUCCESS(
            table
                .Insert(
                    setup.txn_,
                    Row({Value(i), value_at(i),
                         i % 23 == 0 ? Value() : Value::DateFromDays(i % 31)}))
                .GetStatus());
      }
      ASSERT_SUCCESS(setup.PreCommit());
    }
    auto reader = db_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(reader, name));
    const auto store = BuildLSTableStore(reader.txn_, table);
    ASSERT_NE(store, nullptr);
    for (const size_t c : {size_t{1}, size_t{2}}) {
      const auto& dictionary = store->dictionaries[c];
      ASSERT_FALSE(dictionary.active);
      ASSERT_EQ(dictionary.packed_code_bits,
                std::bit_width(dictionary.int_entries.size()));
      ASSERT_GE(dictionary.packed_code_bits, 1U);
      EXPECT_EQ(dictionary.DictionaryCodeCount(), static_cast<size_t>(rows));
      EXPECT_EQ(
          dictionary.wide_codes.size(),
          (((static_cast<size_t>(rows) * dictionary.packed_code_bits) + 7) /
           8) +
              7);
      EXPECT_LT(dictionary.wide_codes.size(),
                static_cast<size_t>(rows) * dictionary.wide_code_bytes);
      const size_t domain = dictionary.int_entries.size();
      if (domain <= 300) {
        ASSERT_EQ(dictionary.code_counts.size(), domain + 1);
      }
      std::vector<int64_t> expected_counts(domain + 1);
      std::vector<uint64_t> expected_first(domain + 1, UINT64_MAX);
      std::vector<uint32_t> expected_encounter;
      size_t base = 0;
      for (const auto& block : store->blocks) {
        const auto& column = block.columns[c];
        EXPECT_EQ(column.DictionaryCodeAt(block.row_count), UINT32_MAX);
        for (size_t row = 0; row < block.row_count; ++row) {
          const int64_t n = block.columns[0].int_cells[row];
          const auto expected = c == 1        ? value_at(n)
                                : n % 23 == 0 ? Value()
                                              : Value::DateFromDays(n % 31);
          const uint32_t code = dictionary.DictionaryCodeAt(base + row);
          EXPECT_EQ(code, column.DictionaryCodeAt(row));
          ASSERT_LE(code, domain);
          if (expected_counts[code]++ == 0) {
            expected_first[code] = base + row;
            expected_encounter.push_back(code);
          }
          if (expected.IsNull()) {
            EXPECT_EQ(code, dictionary.int_entries.size());
          } else {
            ASSERT_LT(code, dictionary.int_entries.size());
            EXPECT_EQ(dictionary.int_entries[code], expected.value.int_value);
          }
        }
        base += block.row_count;
      }
      EXPECT_EQ(base, dictionary.DictionaryCodeCount());
      EXPECT_EQ(dictionary.DictionaryCodeAt(base), UINT32_MAX);
      EXPECT_EQ(dictionary.DictionaryCodeAt(SIZE_MAX), UINT32_MAX);
      if (!dictionary.code_counts.empty()) {
        EXPECT_EQ(dictionary.code_counts, expected_counts);
        EXPECT_EQ(dictionary.code_first_row, expected_first);
        EXPECT_EQ(dictionary.encounter_codes, expected_encounter);
      }
    }
    ASSERT_SUCCESS(reader.PreCommit());
  }
}

TEST_F(FullScanIteratorTest, WideStringDictionaryKeepsBytesAndSnapshot) {
  for (const int64_t cardinality : {int64_t{300}, int64_t{65536}}) {
    const std::string name = "WideStrings" + std::to_string(cardinality);
    const int64_t rows = cardinality == 300 ? 3000 : cardinality * 3;
    const auto text = [cardinality](int64_t i) {
      // Some complete page blocks contain only one-byte values; their old
      // raw-byte fast path must be disabled after installing wider codes.
      if (cardinality == 300 && i < 1024) {
        return Value(std::string(1, static_cast<char>('a' + (i % 26))));
      }
      if (i % 17 == 0) {
        return Value();
      }
      if (i % 19 == 0) {
        return Value("");
      }
      std::string value = "é東京" + std::string(80, 'p');
      value.push_back('\0');
      value += std::to_string(i % cardinality);
      return Value(std::move(value));
    };
    {
      auto setup = db_->BeginContext();
      const Schema schema(name, {Column("k", ValueType::kInt64),
                                 Column("s", ValueType::kVarChar),
                                 Column("v", ValueType::kDouble)});
      ASSERT_SUCCESS(db_->CreateTable(setup, schema).GetStatus());
      ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(setup, name));
      for (int64_t i = 0; i < rows; ++i) {
        ASSERT_SUCCESS(
            table.Insert(setup.txn_, Row({Value(i), text(i), Value(1.0)}))
                .GetStatus());
      }
      ASSERT_SUCCESS(setup.PreCommit());
    }
    auto reader = db_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(reader, name));
    const auto store = BuildLSTableStore(reader.txn_, table);
    ASSERT_NE(store, nullptr);
    const size_t width = cardinality < 65535 ? 2 : 4;
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, width);
    ASSERT_FALSE(store->dictionaries[1].active);
    size_t row_offset = 0;
    const auto& dictionary = store->dictionaries[1];
    ASSERT_NE(dictionary.packed_code_bits, 0);
    EXPECT_EQ(
        dictionary.wide_codes.size(),
        ((static_cast<size_t>(rows) * dictionary.packed_code_bits + 7) / 8) +
            7);
    EXPECT_LT(dictionary.wide_codes.size(), static_cast<size_t>(rows) * width);
    for (const auto& block : store->blocks) {
      const auto& strings = block.columns[1];
      EXPECT_TRUE(strings.varchar_ends.empty());
      EXPECT_TRUE(strings.varchar_bytes.empty());
      EXPECT_EQ(strings.wide_code_begin, row_offset);
      EXPECT_EQ(strings.wide_code_count, block.row_count);
      EXPECT_EQ(strings.DictionaryCodeAt(block.row_count), UINT32_MAX);
      for (uint32_t r = 0; r < block.row_count; ++r) {
        EXPECT_TRUE(store->MayContainInteger(0, block.columns[0].int_cells[r]));
        const auto expected = text(block.columns[0].int_cells[r]);
        EXPECT_EQ(strings.DictionaryCodeAt(r),
                  dictionary.DictionaryCodeAt(row_offset + r));
        EXPECT_EQ(strings.IsNull(r), expected.IsNull());
        if (!expected.IsNull()) {
          EXPECT_EQ(strings.VarcharAt(r), expected.value.varchar_value);
        }
      }
      row_offset += block.row_count;
    }
    EXPECT_EQ(dictionary.DictionaryCodeAt(static_cast<size_t>(rows)),
              UINT32_MAX);
    const auto check_scan = [&] {
      auto scan = table.BeginFullScan(reader.txn_, {0, 1});
      TypedCellBatch batch;
      size_t count = 0;
      while (const size_t n = scan.DecodeNextTypedBatch(&batch, 137)) {
        for (size_t r = 0; r < n; ++r) {
          const auto expected = text(batch.cells[r * batch.stride]);
          EXPECT_EQ(batch.nulls[(r * batch.stride) + 1] != 0,
                    expected.IsNull());
          if (!expected.IsNull()) {
            EXPECT_EQ(batch.Varchar(r, 0), expected.value.varchar_value);
          }
        }
        count += n;
      }
      EXPECT_EQ(count, static_cast<size_t>(rows));
      EXPECT_EQ(scan.GetStatus(), Status::kSuccess);
    };
    check_scan();
    {
      auto writer = db_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(Table, current, db_->GetTable(writer, name));
      auto scan = current.BeginFullScan(writer.txn_);
      const auto position = scan.Position();
      scan.DropPageLatch();
      ASSERT_SUCCESS(current
                         .Update(writer.txn_, position,
                                 Row({Value(0), Value("updated"), Value(1.0)}))
                         .GetStatus());
      ASSERT_SUCCESS(writer.PreCommit());
    }
    check_scan();
    ASSERT_SUCCESS(reader.PreCommit());
  }
}

TEST_F(FullScanIteratorTest, BeginFullScan_AllRows_ScansSuccessfully) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "SampleTable"));

  for (int i = 0; i < 130; ++i) {
    ASSERT_SUCCESS(
        table
            .Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                   Value(0.1 + i)}))
            .GetStatus());
  }
  Iterator it = table.BeginFullScan(ctx.txn_);
  while (it.IsValid()) {
    LOG(TRACE) << *it;
    ++it;
  }
}

TEST_F(FullScanIteratorTest,
       BeginFullScan_WithProjection_ReturnsRequestedColumnsInOrder) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "SampleTable"));
  ASSERT_SUCCESS(
      table.Insert(ctx.txn_, Row({Value(42), Value("not copied"), Value(3.5)}))
          .GetStatus());

  Iterator projected = table.BeginFullScan(ctx.txn_, {0, 2});
  ASSERT_TRUE(projected.IsValid());
  ASSERT_EQ((*projected).values_.size(), 2);
  EXPECT_EQ((*projected)[0], Value(42));
  EXPECT_EQ((*projected)[1], Value(3.5));

  Iterator no_columns = table.BeginFullScan(ctx.txn_, std::vector<slot_t>{});
  ASSERT_TRUE(no_columns.IsValid());
  EXPECT_TRUE((*no_columns).values_.empty());
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanIteratorTest,
       BeginFullScan_AfterDeletingFirstSlot_ScansRemainingRows) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "SampleTable"));
  RowPosition first;
  for (int i = 0; i < 6; ++i) {
    ASSIGN_OR_ASSERT_FAIL(
        RowPosition, position,
        table.Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                    Value(0.1 + i)})));
    if (i == 0) {
      first = position;
    }
  }
  ASSERT_SUCCESS(table.Delete(ctx.txn_, first));

  int count = 0;
  int64_t maximum = 0;
  for (Iterator it = table.BeginFullScan(ctx.txn_); it.IsValid(); ++it) {
    ++count;
    maximum = std::max(maximum, (*it)[0].value.int_value);
  }

  EXPECT_EQ(count, 5);
  EXPECT_EQ(maximum, 5);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanIteratorTest,
       BeginFullScan_OldSnapshotAfterDelete_ScansPhysicallyDeletedTailRow) {
  TransactionContext seed = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(seed, "SampleTable"));
  RowPosition tail;
  for (int i = 0; i < 2; ++i) {
    ASSIGN_OR_ASSERT_FAIL(
        RowPosition, inserted,
        table.Insert(seed.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                     Value(0.1 + i)})));
    tail = inserted;
  }
  ASSERT_SUCCESS(seed.PreCommit());

  TransactionContext old_reader = db_->BeginContext();
  TransactionContext writer = db_->BeginContext();
  ASSERT_SUCCESS(table.Delete(writer.txn_, tail));
  ASSERT_SUCCESS(writer.PreCommit());

  size_t old_count = 0;
  for (Iterator it = table.BeginFullScan(old_reader.txn_); it.IsValid(); ++it) {
    ++old_count;
  }
  EXPECT_EQ(old_count, 2);
  ASSERT_SUCCESS(old_reader.PreCommit());

  TransactionContext fresh_reader = db_->BeginContext();
  size_t fresh_count = 0;
  for (Iterator it = table.BeginFullScan(fresh_reader.txn_); it.IsValid();
       ++it) {
    ++fresh_count;
  }
  EXPECT_EQ(fresh_count, 1);
  ASSERT_SUCCESS(fresh_reader.PreCommit());
}

TEST_F(FullScanIteratorTest,
       BeginMorselScan_WithMorsels_IteratesOnlyRequestedPages) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "SampleTable"));
  for (int i = 0; i < 130; ++i) {
    ASSERT_SUCCESS(
        table
            .Insert(ctx.txn_, Row({Value(i), Value("v" + std::to_string(i)),
                                   Value(0.1 + i)}))
            .GetStatus());
  }
  const std::vector<Table::ScanMorsel> morsels =
      table.BuildScanMorsels(ctx.txn_, 2);
  ASSERT_FALSE(morsels.empty());

  int64_t seen = 0;
  for (const Table::ScanMorsel& morsel : morsels) {
    Iterator it = table.BeginMorselScan(ctx.txn_, morsel);
    while (it.IsValid()) {
      ASSERT_EQ((*it).values_.size(), 3U);
      ++seen;
      ++it;
    }
  }
  EXPECT_EQ(seen, 130);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanIteratorTest, BeginFullScan_EmptyTable_YieldsNoRows) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL_CONST(Table, table, db_->GetTable(ctx, "SampleTable"));
  Iterator it = table.BeginFullScan(ctx.txn_);

  EXPECT_FALSE(it.IsValid());
  EXPECT_FALSE(it.Position().IsValid());
  ++it;
  EXPECT_FALSE(it.IsValid());
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanIteratorTest, OutputOperator_WhenCalled_PrintsScanName) {
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "SampleTable"));
  ASSERT_SUCCESS(
      table.Insert(ctx.txn_, Row({Value(7), Value("seven"), Value(0.7)}))
          .GetStatus());
  Iterator it = table.BeginFullScan(ctx.txn_);
  ASSERT_TRUE(it.IsValid());

  std::ostringstream output;
  output << it;

  EXPECT_NE(output.str().find("FullScan"), std::string::npos);
  EXPECT_NE(output.str().find("SampleTable"), std::string::npos);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(FullScanIteratorTest, ReadVersionFailureTerminatesScanSticky) {
  const std::string db_name = "full_scan_failure-" + RandomString() + ".db";
  const std::string log_name = "full_scan_failure-" + RandomString() + ".log";
  {
    auto page_manager = PageManager::Create(db_name, 10).MoveValue();
    auto logger = Logger::Create(log_name).MoveValue();
    RecoveryManager recovery(log_name, page_manager->GetPool());
    TransactionManager transactions(page_manager.get(), logger.get(),
                                    &recovery);

    Transaction pin = transactions.Begin(true);
    Transaction seed = transactions.Begin();
    PageRef allocated =
        page_manager->AllocateNewPage(seed, PageType::kRowPage).MoveValue();
    const page_id_t page_id = allocated->PageID();
    allocated.PageUnlock();
    Table table(Schema("failure_table", {Column("value", ValueType::kInt64)}),
                page_id);
    ASSERT_SUCCESS(table.Insert(seed, Row({Value(7)})).GetStatus());
    ASSERT_SUCCESS(seed.PreCommit());

    Transaction reader = transactions.Begin(true);
    Transaction broken = transactions.Begin();
    ASSERT_TRUE(broken.AddWriteSet(RowPosition(page_id + 1000, 0)));
    ForceScanWalFailure(logger.get());
    EXPECT_NE(broken.PreCommit(), Status::kSuccess);
    ASSERT_TRUE(transactions.FailStopped());

    Iterator scan = table.BeginFullScan(reader);
    EXPECT_FALSE(scan.IsValid());
    EXPECT_EQ(scan.GetStatus(), Status::kIOError);
  }
  std::ignore = std::remove(db_name.c_str());
  tinylamb::RemoveLogAndSegments(log_name);
}

TEST_F(FullScanIteratorTest,
       ProjectedStoreBoundsPayloadAndKeepsSnapshotIdentity) {
  auto load = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      db_->CreateTable(
          load, Schema("Projected", {Column("k", ValueType::kInt64),
                                     Column("payload", ValueType::kVarChar),
                                     Column("d", ValueType::kDouble)})));
  for (int64_t i = 0; i < 9000; ++i) {
    ASSERT_SUCCESS(
        table
            .Insert(load.txn_, Row({i % 101 == 0 ? Value() : Value(i),
                                    i % 17 == 0 ? Value()
                                                : Value(std::to_string(i) +
                                                        std::string(512, 'x')),
                                    Value(static_cast<double>(i) / 2)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(load.PreCommit());
  auto reader = db_->BeginReadOnlyContext();
  constexpr uint64_t budget = uint64_t{256} * 1024;
  EXPECT_EQ(BuildLSTableStore(reader.txn_, table, nullptr, budget), nullptr);
  EXPECT_FALSE(table.LSTableCapacityRejected(table.LayoutGeneration()));
  const std::vector<slot_t> numeric{2, 0};
  const auto projected =
      BuildLSTableStore(reader.txn_, table, &numeric, budget);
  ASSERT_NE(projected, nullptr);
  EXPECT_EQ(projected->total_rows, 9000U);
  EXPECT_TRUE(projected->HasColumn(0));
  EXPECT_FALSE(projected->HasColumn(1));
  EXPECT_TRUE(projected->HasColumn(2));
  EXPECT_EQ(table.GetLSTableStore(), nullptr);
  EXPECT_EQ(projected->pax_chunk, nullptr);
  int64_t i = 0;
  for (const auto& block : projected->blocks) {
    EXPECT_EQ(block.columns.Stored().size(), 2U);
    EXPECT_EQ(block.columns.size(), 3U);
    for (uint32_t r = 0; r < block.row_count; ++r, ++i) {
      EXPECT_EQ(block.columns[0].IsNull(r), i % 101 == 0);
      if (i % 101 != 0) {
        EXPECT_EQ(block.columns[0].int_cells[r], i);
      }
      EXPECT_EQ(std::bit_cast<double>(block.columns[2].int_cells[r]),
                static_cast<double>(i) / 2);
    }
  }
  EXPECT_EQ(i, 9000);
  // Model the production capacity verdict without allocating four GiB in a
  // unit test. It is only a scheduling hint, never a visibility/row-count
  // proof.
  table.RejectLSTableCapacity(table.LayoutGeneration());
  EXPECT_EQ(GetOrBuildLSTableStore(reader.txn_, table, true), nullptr);
  EXPECT_EQ(GetOrBuildLSTableStore(reader.txn_, table, true, &numeric),
            projected);
  const std::vector<slot_t> text{1};
  const auto strings = GetOrBuildLSTableStore(reader.txn_, table, true, &text);
  ASSERT_NE(strings, nullptr);
  EXPECT_NE(strings, projected);
  EXPECT_EQ(table.GetProjectedLSTableStore(), strings);
  EXPECT_FALSE(strings->HasColumn(0));
  i = 0;
  for (const auto& block : strings->blocks) {
    for (uint32_t r = 0; r < block.row_count; ++r, ++i) {
      EXPECT_EQ(block.columns[1].IsNull(r), i % 17 == 0);
      if (i % 17 != 0) {
        EXPECT_EQ(block.columns[1].VarcharAt(r),
                  std::to_string(i) + std::string(512, 'x'));
      }
    }
  }
  const std::vector<slot_t> no_columns;
  const auto count = BuildLSTableStore(reader.txn_, table, &no_columns, 1);
  ASSERT_NE(count, nullptr);
  EXPECT_EQ(count->total_rows, 9000U);
  EXPECT_FALSE(count->HasColumn(0));
  EXPECT_FALSE(count->HasColumn(1));
  auto writer = db_->BeginContext();
  ASSERT_SUCCESS(
      table.Insert(writer.txn_, Row({Value(9000), Value("new"), Value(1.0)}))
          .GetStatus());
  ASSERT_SUCCESS(writer.PreCommit());
  EXPECT_FALSE(table.LSTableCapacityRejected(table.LayoutGeneration()));
  EXPECT_EQ(GetOrBuildLSTableStore(reader.txn_, table, true, &text), nullptr);
  size_t old_rows = 0;
  auto scan = table.BeginFullScan(reader.txn_);
  while (scan.IsValid()) {
    ++old_rows;
    ++scan;
  }
  EXPECT_EQ(old_rows, 9000U);
  ASSERT_SUCCESS(reader.PreCommit());
  auto fresh = db_->BeginReadOnlyContext();
  const auto full = GetOrBuildLSTableStore(fresh.txn_, table, true);
  ASSERT_NE(full, nullptr);
  EXPECT_EQ(full->total_rows, 9001U);
  EXPECT_TRUE(full->loaded_columns.empty());
  ASSERT_SUCCESS(fresh.PreCommit());
}

TEST_F(FullScanIteratorTest, StringDictionaryKeepsModestEstimatedSavings) {
  constexpr int64_t cardinality = 1000;
  constexpr int64_t rows = cardinality * 3;
  const auto text = [](int64_t i) {
    return std::string(12, 'x') + std::to_string(1000 + (i % cardinality));
  };
  {
    auto setup = db_->BeginContext();
    ASSERT_SUCCESS(
        db_->CreateTable(setup, Schema("ModestStrings",
                                       {Column("s", ValueType::kVarChar)}))
            .GetStatus());
    ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(setup, "ModestStrings"));
    for (int64_t i = 0; i < rows; ++i) {
      ASSERT_SUCCESS(
          table.Insert(setup.txn_, Row({Value(text(i))})).GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  auto reader = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(reader, "ModestStrings"));
  const auto store = BuildLSTableStore(reader.txn_, table);
  ASSERT_NE(store, nullptr);
  const auto& dictionary = store->dictionaries[0];
  ASSERT_EQ(dictionary.StringEntryCount(), cardinality);
  const size_t encoded_bytes =
      dictionary.wide_codes.size() +
      (static_cast<size_t>(cardinality) * (sizeof(std::string) + 16));
  constexpr size_t source_bytes =
      static_cast<size_t>(rows) * (16 + sizeof(uint32_t));
  EXPECT_LT(encoded_bytes, source_bytes);
  EXPECT_GT(encoded_bytes, source_bytes - (source_bytes / 5));
  for (int64_t i = 0; i < rows; ++i) {
    const auto code = dictionary.DictionaryCodeAt(static_cast<size_t>(i));
    ASSERT_LT(code, dictionary.StringEntryCount());
    EXPECT_EQ(dictionary.StringEntryAt(code), text(i));
  }
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(FullScanIteratorTest, IntegerDictionaryCrossesFormerCardinalityLimit) {
  constexpr int64_t cardinality = (int64_t{1} << 20) + 17;
  constexpr int64_t rows = (cardinality * 9) / 4;
  constexpr int64_t kCommitBatch = 262144;
  {
    auto setup = db_->BeginContext();
    const Schema schema("LargeDictionary", {Column("k", ValueType::kInt64)});
    ASSERT_SUCCESS(db_->CreateTable(setup, schema).GetStatus());
    ASSERT_SUCCESS(setup.PreCommit());
  }
  Row insert_row({Value(int64_t{0})});
  for (int64_t base = 0; base < rows; base += kCommitBatch) {
    auto batch = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table,
                          db_->GetTable(batch, "LargeDictionary"));
    const int64_t end = std::min(rows, base + kCommitBatch);
    for (int64_t i = base; i < end; ++i) {
      insert_row[0].value.int_value = i % cardinality;
      ASSERT_SUCCESS(table.Insert(batch.txn_, insert_row).GetStatus());
    }
    if (end == rows) {
      ASSERT_SUCCESS(table.Insert(batch.txn_, Row({Value()})).GetStatus());
    }
    ASSERT_SUCCESS(batch.PreCommit());
  }
  auto reader = db_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(reader, "LargeDictionary"));
  const auto store = BuildLSTableStore(reader.txn_, table);
  ASSERT_NE(store, nullptr);
  const auto& dictionary = store->dictionaries[0];
  ASSERT_EQ(dictionary.IntEntryCount(), static_cast<size_t>(cardinality));
  EXPECT_EQ(dictionary.packed_code_bits, 21);
  EXPECT_LT(dictionary.wide_codes.size() +
                (dictionary.int_entries.size() * sizeof(int64_t)),
            static_cast<size_t>(rows) * sizeof(int64_t) * 4 / 5);
  for (size_t row = 0; row < static_cast<size_t>(rows); row += 997) {
    const auto code = dictionary.DictionaryCodeAt(row);
    ASSERT_LT(code, dictionary.IntEntryCount());
    EXPECT_EQ(dictionary.IntEntryAt(code),
              static_cast<int64_t>(row) % cardinality);
  }
  EXPECT_EQ(dictionary.DictionaryCodeAt(static_cast<size_t>(rows)),
            cardinality);
  EXPECT_EQ(dictionary.DictionaryCodeAt(static_cast<size_t>(rows + 1)),
            UINT32_MAX);
  ASSERT_SUCCESS(reader.PreCommit());
}

}  // namespace tinylamb
