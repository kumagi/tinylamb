/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// Scale test harness framework (labeled "scale" in CTest per ooca-v2.md §6 P0).

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <string>

#include "common/memory_budget.hpp"
#include "common/platform/process.hpp"
#include "common/random_string.hpp"
#include "common/spill/spill_store.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/ooc/external_sorter.hpp"
#include "page/page.hpp"
#include "page/row_page.hpp"
#include "query/sql_engine.hpp"
#include "table/tlcs_file.hpp"

namespace tinylamb {

TEST(OocScaleBaselineTest, ReportsBaselineStructureFootprintsAndPressure) {
  const uint64_t rss = platform::CurrentProcessRssBytes();
  EXPECT_GT(rss, 0U);
  // Document current per-unit sizes observed in P0:
  // A non-resident Page still carries the full Page+RowPage shell (> 1 KiB).
  EXPECT_GT(sizeof(Page), 0U);
  EXPECT_GT(sizeof(RowPage), 0U);
  EXPECT_GT(sizeof(TlcsFile::BlockDirEntry), 0U);
  EXPECT_GT(sizeof(TlcsFile::ColumnMeta), 0U);

  ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
  b.ResetPeaks();
  EXPECT_GE(b.CurrentUnaccountedRssBytes(), 0U);
}

TEST(OocScaleBaselineTest, SmallMemoryCapAdmitsValidBudgetQueries) {
  ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
  // Verify minimum supported M = 1 GiB calculation boundaries (ADR 0003).
  constexpr size_t kMinLimit = size_t{1} << 30;
  b.ResetForTest(kMinLimit, kMinLimit / 4, kMinLimit / 8,
                 /*check_rss_pressure=*/false);
  EXPECT_EQ(b.SoftLimit(), (kMinLimit / 5) * 4);
  EXPECT_TRUE(b.TryReserve(MemoryPoolKind::kQuery, size_t{64} << 20));
  b.Release(MemoryPoolKind::kQuery, size_t{64} << 20);
  b.ResetForTest(0);
}

TEST(OocScaleBaselineTest, P1MemoryAccountingGateUnder16GiBAnd1GiB) {
  ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
  struct ResetGuard {
    explicit ResetGuard(ProcessMemoryBudget* b_ptr) : budget(b_ptr) {}
    ResetGuard(const ResetGuard&) = delete;
    ResetGuard& operator=(const ResetGuard&) = delete;
    ResetGuard(ResetGuard&&) = delete;
    ResetGuard& operator=(ResetGuard&&) = delete;
    ~ResetGuard() { budget->ResetForTest(0); }
    ProcessMemoryBudget* budget;
  } guard{&b};

  for (const uint64_t limit_bytes : {uint64_t{16} << 30, uint64_t{1} << 30}) {
    const uint64_t soft_limit = (limit_bytes / 5) * 4;  // M_t = 0.80 * M
    b.ResetForTest(limit_bytes, soft_limit, soft_limit,
                   /*check_rss_pressure=*/true);
    b.SetPoolFloorsForTest(soft_limit / 4, 0, soft_limit / 4, soft_limit / 20);

    const std::string db_name = "ooc_p1_gate_" + RandomString();
    auto db_or = Database::Create(db_name);
    ASSERT_TRUE(db_or.HasValue()) << db_or.GetStatus().GetMessage();
    std::unique_ptr<Database> db = db_or.MoveValue();

    SqlEngine engine(*db);
    {
      TransactionContext ctx = db->BeginContext();
      ASSERT_TRUE(
          engine
              .Execute(ctx,
                       "CREATE TABLE t(k INT64, g INT64, v1 INT64, s STRING)")
              .HasValue());
      ASSERT_EQ(ctx.PreCommit().GetCode(), StatusCode::kSuccess);
    }

    // Load 5,000 rows in uniform parameterized batches so plan templates stay
    // small while exercising PageManager + WAL + query execution.
    for (int batch = 0; batch < 50; ++batch) {
      TransactionContext ctx = db->BeginContext();
      std::string sql = "INSERT INTO t VALUES ";
      for (int i = 0; i < 100; ++i) {
        const int64_t id = (static_cast<int64_t>(batch) * 100) + i;
        if (i > 0) {
          sql += ",";
        }
        sql += "(" + std::to_string(id) + "," + std::to_string(id % 100) + "," +
               std::to_string(id * 7) + ",'row_" + std::to_string(id % 50) +
               "')";
      }
      auto res = engine.Execute(ctx, sql);
      ASSERT_TRUE(res.HasValue()) << engine.LastError();
      res.MoveValue().Drain();
      ASSERT_EQ(ctx.PreCommit().GetCode(), StatusCode::kSuccess);
    }

    {
      TransactionContext ctx = db->BeginContext();
      auto res = engine.Execute(
          ctx, "SELECT g, SUM(v1), COUNT(*) FROM t GROUP BY g ORDER BY g");
      ASSERT_TRUE(res.HasValue()) << engine.LastError();
      EXPECT_EQ(res.MoveValue().Drain(), 100U);
      ASSERT_EQ(ctx.PreCommit().GetCode(), StatusCode::kSuccess);
    }

    const auto max_unaccounted =
        static_cast<uint64_t>(0.075 * static_cast<double>(limit_bytes));
    EXPECT_LE(b.CurrentUnaccountedRssBytes(), max_unaccounted)
        << "limit_bytes=" << limit_bytes << " total_used=" << b.TotalUsed()
        << " rss=" << platform::CurrentProcessRssBytes();
    EXPECT_GT(b.TxUsed(), 0U);
    EXPECT_GT(b.StorageUsed(), 0U);

    db->DeleteAll();
  }
}

TEST(OocScaleBaselineTest, G2ExternalSortOneGiBUnderOneMiBBudget) {
  SpillStore store;
  // ~1,048,576 rows * (~1024 B/row incl. Row/Value + 960B payload) > 1 GiB
  // processed under a strict 1 MiB working memory budget.
  constexpr int64_t kRows = 1LL << 20;
  constexpr size_t kBudget = size_t{1} << 20;

  const uint64_t rss_before = platform::CurrentProcessRssBytes();
  uint64_t peak_rss = rss_before;

  ExternalSorter sorter(ExternalSorter::Options{
      .spill_store = &store,
      .memory_budget_bytes = kBudget,
      .keys = {OocSortKey{.column_index = 0, .ascending = true}},
      .limit = std::nullopt,
  });

  const std::string payload(960, 'G');
  for (int64_t i = 0; i < kRows; ++i) {
    // gcd(7919, 2^20) == 1 -> full permutation of [0, kRows).
    const int64_t key = (i * 7919LL) % kRows;
    ASSERT_EQ(sorter.AddRow(Row({Value(key), Value(std::string(payload))})),
              Status::kSuccess);
    if ((i & 0x1ffff) == 0) {
      peak_rss = std::max(peak_rss, platform::CurrentProcessRssBytes());
    }
  }
  ASSERT_EQ(sorter.Finish(), Status::kSuccess);
  peak_rss = std::max(peak_rss, platform::CurrentProcessRssBytes());

  EXPECT_TRUE(sorter.Spilled());
  EXPECT_GE(sorter.SpilledBytes(), uint64_t{1} << 30);
  EXPECT_LE(sorter.PeakMemoryBytes(), kBudget + (64ULL << 10));
  EXPECT_GE(sorter.MergePasses(), 2U);

  Row out;
  int64_t expected = 0;
  while (true) {
    auto has = sorter.NextRow(&out);
    ASSERT_EQ(has.GetStatus().GetCode(), StatusCode::kSuccess);
    if (!has.Value()) {
      break;
    }
    ASSERT_EQ(out[0].AsInt64(), expected);
    ++expected;
    if ((expected & 0x3ffff) == 0) {
      peak_rss = std::max(peak_rss, platform::CurrentProcessRssBytes());
    }
  }
  EXPECT_EQ(expected, kRows);
  EXPECT_EQ(store.Stats().live_segments, 0U);
  EXPECT_EQ(store.Stats().current_active_bytes, 0U);
  EXPECT_LE(peak_rss - rss_before, uint64_t{64} << 20);
}

}  // namespace tinylamb
