/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// P0 empirical footprint & baseline measurement tool (ooca-v2.md §6 P0).

#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "common/memory_budget.hpp"
#include "common/platform/process.hpp"
#include "common/random_string.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "page/row_page.hpp"
#include "query/plan_cache.hpp"
#include "query/sql_engine.hpp"
#include "table/tlcs_file.hpp"
#include "transaction/transaction.hpp"

namespace {

using namespace tinylamb;

double ToMiB(uint64_t bytes) {
  return static_cast<double>(bytes) / (1024.0 * 1024.0);
}

struct DatabaseCleanup {
  Database* db;
  ~DatabaseCleanup() {
    if (db != nullptr) {
      db->DeleteAll();
    }
  }
};

void PrintSizeRow(const char* name, size_t bytes_per_unit, uint64_t units_1tb,
                  const char* note) {
  const double total_1tb_bytes =
      static_cast<double>(bytes_per_unit) * static_cast<double>(units_1tb);
  std::cout << std::left << std::setw(36) << name << " | " << std::right
            << std::setw(6) << bytes_per_unit << " B | " << std::setw(12)
            << units_1tb << " | " << std::setw(8) << std::fixed
            << std::setprecision(2)
            << (total_1tb_bytes / (1024.0 * 1024.0 * 1024.0)) << " GiB | "
            << note << "\n";
}

}  // namespace

int main() {
  std::cout << "=== Static sizeof() Audit (P0 / ooca-v2.md) ===\n";
  std::cout << std::left << std::setw(36) << "Structure" << " | " << std::right
            << std::setw(8) << "sizeof" << " | " << std::setw(12) << "1TB units"
            << " | " << std::setw(12) << "1TB ExTr"
            << " | Note\n";
  std::cout << std::string(96, '-') << "\n";

  // 1 TB / 32 KiB page = 33,554,432 pages
  constexpr uint64_t kPagesPer1TB = (uint64_t{1} << 40) / kPageSize;
  // Assume ~500 rows/page -> ~1.6e10 rows, or ~300K-3M TLCS blocks for column
  // data
  constexpr uint64_t kBlocksPer1TB = 500'000;
  constexpr uint64_t kBulkTxRows = 1'000'000;

  PrintSizeRow("Page (incl. RowPage shell)", sizeof(Page), kPagesPer1TB,
               "Permanent in PageManager::blocks_ even after EvictBatch");
  PrintSizeRow("RowPage alone", sizeof(RowPage), kPagesPer1TB,
               "Two PackedSlots, vectors, atomics, mutex in Page");
  PrintSizeRow("TlcsFile::BlockDirEntry", sizeof(TlcsFile::BlockDirEntry),
               kBlocksPer1TB, "Header only (+ columns.size() * ColumnMeta)");
  PrintSizeRow("TlcsFile::ColumnMeta (x16 cols)",
               sizeof(TlcsFile::ColumnMeta) * 16, kBlocksPer1TB,
               "16-col table (e.g. TPC-H lineitem)");
  PrintSizeRow("LsmTreeState", sizeof(LsmTreeState), 1000,
               "Per index/catalog tree");
  PrintSizeRow("LsmTreeState::MemEntry", sizeof(LsmTreeState::MemEntry),
               1'000'000, "Per uncommitted/memtable entry");
  PrintSizeRow("LsmSortedRun::Entry", sizeof(LsmSortedRun::Entry), 10'000'000,
               "Per in-memory sorted run entry");
  PrintSizeRow("LogRecord", sizeof(LogRecord), kBulkTxRows,
               "In Transaction::staged_records_");
  PrintSizeRow("CompiledPlan", sizeof(CompiledPlan), 10000,
               "Per plan cache entry (plus retained_tables)");

  std::cout << "\n=== Live Empty / Baseline Execution & RSS vs Tracked ===\n";
  const uint64_t rss_start = platform::CurrentProcessRssBytes();
  std::cout << "Startup RSS: " << ToMiB(rss_start) << " MiB\n";

  const std::string db_name = "ooc_p0_measure_" + RandomString();
  ProcessMemoryBudget& mem = ProcessMemoryBudget::Global();
  mem.ResetPeaks();

  {
    auto db_or = Database::Create(db_name);
    if (!db_or.HasValue()) {
      std::cerr << "Database::Create failed: " << db_or.GetStatus().GetMessage()
                << "\n";
      return 1;
    }
    std::unique_ptr<Database> db = db_or.MoveValue();
    DatabaseCleanup cleanup{db.get()};
    std::cout << "After Database::Create: RSS="
              << ToMiB(platform::CurrentProcessRssBytes())
              << " MiB, tracked=" << ToMiB(mem.TotalUsed())
              << " MiB (tx=" << ToMiB(mem.TxUsed()) << " MiB), unaccounted="
              << ToMiB(mem.CurrentUnaccountedRssBytes()) << " MiB\n";
    TransactionContext ctx = db->BeginContext();
    SqlEngine engine(*db);

    auto commit = [&](std::string_view operation) {
      const Status status = ctx.PreCommit();
      if (!status.ok()) {
        std::cerr << operation << " commit failed: " << status << '\n';
      }
      return status.ok();
    };
    auto run_sql = [&](std::string_view sql) -> uint64_t {
      auto res = engine.Execute(ctx, sql);
      if (!res.HasValue()) {
        std::cerr << "SQL failed (" << sql << "): " << engine.LastError()
                  << "\n";
        std::exit(1);
      }
      QueryResult qr = res.MoveValue();
      const size_t n = qr.Drain();
      if (const Status status = qr.GetStatus(); !status.ok()) {
        std::cerr << "SQL failed (" << sql << "): " << status << '\n';
        std::exit(1);
      }
      return n;
    };

    run_sql(
        "CREATE TABLE t(k INT64, g INT64, v1 INT64, v2 FLOAT64, s "
        "STRING(MAX))");
    if (!commit("CREATE TABLE")) {
      return 1;
    }
    ctx = db->BeginContext();
    std::cout << "After CREATE TABLE: RSS="
              << ToMiB(platform::CurrentProcessRssBytes())
              << " MiB, tracked=" << ToMiB(mem.TotalUsed())
              << " MiB, unaccounted=" << ToMiB(mem.CurrentUnaccountedRssBytes())
              << " MiB\n";

    // Load 50,000 rows in batches to measure storage + query + RSS delta.
    for (int b = 0; b < 50; ++b) {
      std::string ins = "INSERT INTO t VALUES ";
      ins.reserve(1000 * 80);
      for (int i = 0; i < 1000; ++i) {
        const int64_t id = (static_cast<int64_t>(b) * 1000) + i;
        if (i > 0) {
          ins += ",";
        }
        ins += "(" + std::to_string(id) + "," + std::to_string(id % 1000) +
               "," + std::to_string(id * 3) + "," +
               std::to_string(static_cast<double>(id) * 0.5) + ",'payload_" +
               std::to_string(id % 500) + "_padding_text')";
      }
      run_sql(ins);
    }
    if (!commit("data load")) {
      return 1;
    }
    ctx = db->BeginContext();

    const uint64_t rss_after_load = platform::CurrentProcessRssBytes();
    std::cout << "After 50k load: RSS=" << ToMiB(rss_after_load)
              << " MiB, tracked_total=" << ToMiB(mem.TotalUsed())
              << " MiB (storage=" << ToMiB(mem.StorageUsed())
              << " MiB, columnar=" << ToMiB(mem.ColumnarUsed())
              << " MiB, query=" << ToMiB(mem.QueryUsed())
              << " MiB, tx=" << ToMiB(mem.TxUsed()) << " MiB), unaccounted="
              << ToMiB(mem.CurrentUnaccountedRssBytes()) << " MiB\n";

    // Run EXPLAIN ANALYZE on a Sort + Join + GroupBy query.
    mem.ResetPeaks();
    auto ea = engine.Execute(ctx,
                             "EXPLAIN ANALYZE SELECT a.g, SUM(a.v1), COUNT(*) "
                             "FROM t a JOIN t b ON a.k = b.k "
                             "GROUP BY a.g ORDER BY a.g");
    if (ea.HasValue()) {
      QueryResult qr = ea.MoveValue();
      Row row;
      while (qr.Next(&row)) {
        if (row.Count() == 0) {
          std::cerr << "WARNING: EXPLAIN ANALYZE returned a zero-column row\n";
          continue;
        }
        std::cout << row[0].AsString() << "\n";
      }
    } else {
      std::cerr << "WARNING: EXPLAIN ANALYZE failed, peak reflects load only: "
                << ea.GetStatus().GetMessage() << "\n";
    }
    const uint64_t rss_after_query = platform::CurrentProcessRssBytes();
    std::cout << "Peak after query: RSS=" << ToMiB(rss_after_query)
              << " MiB, total_peak=" << ToMiB(mem.TotalPeak())
              << " MiB, unaccounted=" << ToMiB(mem.CurrentUnaccountedRssBytes())
              << " MiB\n";

    // Check EvictColdPagesForTest behavior on PageManager.
    PageManager* pm = ctx.txn_.GetPageManager();
    if (pm != nullptr) {
      const size_t before_res = pm->ResidentBytes();
      const Status evict_st = pm->EvictColdPagesForTest(0);
      const size_t after_res = pm->ResidentBytes();
      const uint64_t rss_after_evict = platform::CurrentProcessRssBytes();
      std::cout << "PageManager EvictColdPagesForTest(0): status="
                << static_cast<int>(evict_st.GetCode()) << ", ResidentBytes "
                << ToMiB(before_res) << " -> " << ToMiB(after_res)
                << " MiB, EvictedPageCount=" << pm->EvictedPageCount()
                << ", RSS=" << ToMiB(rss_after_evict) << " MiB\n";
    }

    if (!commit("measurement")) {
      return 1;
    }
    if (pm != nullptr) {
      const size_t before_res = pm->ResidentBytes();
      const Status evict_st = pm->EvictColdPagesForTest(0);
      const size_t after_res = pm->ResidentBytes();
      const uint64_t rss_after_evict = platform::CurrentProcessRssBytes();
      std::cout << "Post-commit EvictColdPagesForTest(0): status="
                << static_cast<int>(evict_st.GetCode()) << ", ResidentBytes "
                << ToMiB(before_res) << " -> " << ToMiB(after_res)
                << " MiB, EvictedPageCount=" << pm->EvictedPageCount()
                << ", RSS=" << ToMiB(rss_after_evict) << " MiB\n";
    }
  }
  return 0;
}
