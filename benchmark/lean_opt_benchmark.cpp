/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// Comparative OFF-vs-ON benchmark spanning all 17 integrated Lean optimization
// proofs under lean/TinylambOpt/.
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "common/json_string.hpp"
#include "common/literal_like_pattern.hpp"
#include "common/platform/process.hpp"
#include "common/regex_guard.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "query/plan_cache.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"

namespace {
using namespace tinylamb;

struct ScopedDbCleanup {
  std::string prefix;
  explicit ScopedDbCleanup(std::string p) : prefix(std::move(p)) {}
  ~ScopedDbCleanup() {
    std::error_code ec;
    const std::filesystem::path dir =
        std::filesystem::path(prefix).parent_path();
    const std::string base = std::filesystem::path(prefix).filename().string();
    const auto is_db_artifact = [&](std::string_view name) {
      const auto exact_or_sidecar = [&](std::string_view suffix) {
        const std::string stem = base + std::string(suffix);
        return name == stem ||
               (name.starts_with(stem) && name.size() > stem.size() &&
                name[stem.size()] == '.');
      };
      return exact_or_sidecar(".db") || exact_or_sidecar(".log") ||
             exact_or_sidecar(".last_checkpoint");
    };
    for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
      if (is_db_artifact(entry.path().filename().string())) {
        std::filesystem::remove_all(entry.path(), ec);
      }
    }
  }
};

void ExecOrDie(SqlEngine& engine, TransactionContext& ctx,
               const std::string& sql) {
  auto res = engine.Execute(ctx, sql);
  if (!res.HasValue()) {
    std::cerr << "SQL failed: " << sql << "\nErr: " << engine.LastError()
              << "\nStatus: " << res.GetStatus() << "\n";
    std::abort();
  }
  res.Value().Drain();
  if (res.Value().GetStatus() != Status::kSuccess) {
    std::cerr << "Cursor failed: " << sql
              << "\nStatus: " << res.Value().GetStatus() << "\n";
    std::abort();
  }
}

std::vector<std::string> CollectRowsUnsorted(SqlEngine& engine,
                                             TransactionContext& ctx,
                                             const std::string& sql) {
  auto res = engine.Execute(ctx, sql);
  if (!res.HasValue()) {
    std::cerr << "Query failed: " << sql << "\nErr: " << engine.LastError()
              << "\nStatus: " << res.GetStatus() << "\n";
    std::abort();
  }
  std::vector<std::string> out;
  Row row;
  while (res.Value().Next(&row)) {
    out.push_back(row.ToString());
    row = Row();
  }
  if (res.Value().GetStatus() != Status::kSuccess) {
    std::cerr << "Query cursor failed: " << sql
              << "\nStatus: " << res.Value().GetStatus() << "\n";
    std::abort();
  }
  return out;
}

std::vector<std::string> CollectSortedRows(SqlEngine& engine,
                                           TransactionContext& ctx,
                                           const std::string& sql) {
  std::vector<std::string> out = CollectRowsUnsorted(engine, ctx, sql);
  std::ranges::sort(out);
  return out;
}

size_t CountRows(SqlEngine& engine, TransactionContext& ctx,
                 const std::string& sql) {
  auto res = engine.Execute(ctx, sql);
  if (!res.HasValue()) {
    std::cerr << "Query failed: " << sql << "\nErr: " << engine.LastError()
              << "\nStatus: " << res.GetStatus() << "\n";
    std::abort();
  }
  size_t count = 0;
  Row row;
  while (res.Value().Next(&row)) {
    ++count;
    row = Row();
  }
  if (res.Value().GetStatus() != Status::kSuccess) {
    std::cerr << "Query cursor failed: " << sql
              << "\nStatus: " << res.Value().GetStatus() << "\n";
    std::abort();
  }
  return count;
}

template <typename Fn>
void BulkInsert(SqlEngine& engine, TransactionContext& ctx,
                const std::string& table, size_t total_rows, size_t batch_size,
                Fn&& append_tuple) {
  for (size_t start = 0; start < total_rows; start += batch_size) {
    const size_t end = std::min(total_rows, start + batch_size);
    std::string sql;
    sql.reserve(((end - start) * 48) + table.size() + 20);
    sql.append("INSERT INTO ").append(table).append(" VALUES ");
    for (size_t i = start; i < end; ++i) {
      if (i > start) {
        sql += ',';
      }
      append_tuple(sql, i);
    }
    sql += ';';
    ExecOrDie(engine, ctx, sql);
  }
}

void CompactAndAnalyze(Database& db, SqlEngine& engine,
                       const std::string& table) {
  {
    TransactionContext ctx = db.BeginContext();
    const Status st = db.CompactTableToTlcs(ctx, table);
    if (st != Status::kSuccess) {
      std::cerr << "CompactTableToTlcs(" << table << ") failed: " << st << "\n";
      std::abort();
    }
    if (ctx.txn_.PreCommit() != Status::kSuccess) {
      std::cerr << "PreCommit after compact failed for " << table << "\n";
      std::abort();
    }
  }
  {
    TransactionContext ctx = db.BeginContext();
    ExecOrDie(engine, ctx, "ANALYZE " + table + ";");
    if (ctx.txn_.PreCommit() != Status::kSuccess) {
      std::cerr << "PreCommit after ANALYZE failed for " << table << "\n";
      std::abort();
    }
  }
}

struct WorkloadSpec {
  std::string name;
  std::string lean_proof;
  std::string disable_env;
  std::string sql;
  bool print_explain{false};
};

struct WorkloadResult {
  WorkloadSpec spec;
  double off_ms{0.0};
  double on_ms{0.0};
  double speedup{1.0};
  size_t rows{0};
  bool parity_ok{false};
};

double Median(std::vector<double> samples) {
  std::ranges::sort(samples);
  const size_t n = samples.size();
  if (n == 0) {
    return 0.0;
  }
  if (n % 2 == 1) {
    return samples[n / 2];
  }
  return 0.5 * (samples[(n / 2) - 1] + samples[n / 2]);
}

void ResetOptFlags(const std::vector<WorkloadSpec>& specs) {
  for (const auto& s : specs) {
    static_cast<void>(platform::UnsetEnv(s.disable_env.c_str()));
  }
  ReloadFastLikeDisabledFromEnv();
  ReloadFastRegexDisabledFromEnv();
  PreparedPlanCache::Instance().Clear();
  CurrentCompiledPlans().clear();
}

std::string JsonEscape(std::string_view s) {
  std::string out;
  tinylamb::json_text::AppendJsonString(out, s);
  return out;
}

}  // namespace

int main() {
  const std::string db_path =
      (std::filesystem::temp_directory_path() /
       ("lean_opt_bench_" +
        std::to_string(static_cast<long>(platform::GetPid()))))
          .string();
  ScopedDbCleanup cleanup(db_path);

  auto db_or = Database::Create(db_path);
  if (!db_or.HasValue()) {
    std::cerr << "Database::Create failed: " << db_or.GetStatus() << "\n";
    return 1;
  }
  std::shared_ptr<Database> db = db_or.MoveValue();

  auto with_write_tx = [&](auto&& fn) -> bool {
    TransactionContext ctx = db->BeginContext();
    SqlEngine engine(*db);
    fn(engine, ctx);
    if (ctx.txn_.PreCommit() != Status::kSuccess) {
      std::cerr << "PreCommit after bulk insert failed\n";
      return false;
    }
    return true;
  };

  std::cout << "Loading Lean optimization benchmark tables...\n";

  // 1. text_tbl (100k rows) for REGEX / FilterEvalOrder.
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(
            engine, ctx,
            "CREATE TABLE text_tbl (id INT64, code INT64, payload STRING);");
        BulkInsert(engine, ctx, "text_tbl", 100000, 500,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i);
                     sql += ',';
                     sql += std::to_string(i % 2000);
                     sql += ",'";
                     if (i % 50 == 3) {
                       sql += "pre_42-MANDATORY_TOKEN-xyz_";
                       sql += std::to_string(i);
                     } else if (i % 25 == 4) {
                       sql += "hdr_aa9876x_";
                       sql += std::to_string(i % 1000);
                       sql += "_z12qabc";
                     } else {
                       sql += "distractor_alpha_beta_node_p9999mod_a_d_e_item_";
                       sql += std::to_string(i);
                       sql += "_end";
                     }
                     sql += "')";
                   });
      })) {
    return 1;
  }

  // 1b. like_tbl (10k rows, ~1 KB ASCII each) for LikeSegmentChain /
  //     LikeUnderscore / LikeShiftAnd directly via ScanFilter (no CONCAT).
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx,
                  "CREATE TABLE like_tbl (id INT64, body STRING);");
        const std::string a400(400, 'a');
        const std::string a300(300, 'a');
        const std::string a200(200, 'a');
        BulkInsert(engine, ctx, "like_tbl", 10000, 200,
                   [&](std::string& sql, size_t i) {
                     const size_t d4 = 1000 + (i % 9000);
                     sql += '(';
                     sql += std::to_string(i);
                     sql += ",'hdr";
                     sql += std::to_string(d4);
                     if (i % 20 == 1) {
                       sql += "mid_";
                       sql += a400;
                       sql += "_mid_tag_";
                       sql += a300;
                       sql += "_end";
                       sql += std::to_string(d4);
                     } else if (i % 20 == 0) {
                       sql += "skip_";
                       sql += a200;
                       sql += "_alpha_seg_";
                       sql += a200;
                       sql += "_beta_seg_";
                       sql += a200;
                       sql += "_gamma_seg_";
                       sql += a200;
                       sql += "_delta_seg";
                     } else if (i % 20 == 2) {
                       sql += "skip_";
                       sql += a400;
                       sql += a400;
                       sql += "_aaaaaaaaaaaaaaaXz_tail";
                     } else {
                       sql += "mid_";
                       sql += a400;
                       sql += "_alpha_seg_";
                       sql += a300;
                       sql += "_beta_seg_";
                       sql += a200;
                       sql += "_exd";
                       sql += std::to_string(d4);
                     }
                     sql += "')";
                   });
      })) {
    return 1;
  }

  // 2. num_tbl (200k rows) for SimdInRange & ReciprocalDivMod.
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx, "CREATE TABLE num_tbl (id INT64, a INT64);");
        BulkInsert(engine, ctx, "num_tbl", 200000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i);
                     sql += ',';
                     sql += std::to_string((i * 17) % 1000);
                     sql += ')';
                   });
      })) {
    return 1;
  }

  // 3. bk_tbl (200k rows) sorted monotonic `a` for BlockKleene TLCS pruning.
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx,
                  "CREATE TABLE bk_tbl (a INT64, b INT64, payload INT64);");
        BulkInsert(engine, ctx, "bk_tbl", 200000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i);
                     sql += ',';
                     sql += std::to_string(i % 100);
                     sql += ',';
                     sql += std::to_string((i * 7) % 1000);
                     sql += ')';
                   });
      })) {
    return 1;
  }

  // 4. oob_l / oob_r (3k x 3k rows) for OuterOrBranches.
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx,
                  "CREATE TABLE oob_l (id INT64, a INT64, b INT64);");
        ExecOrDie(engine, ctx,
                  "CREATE TABLE oob_r (id INT64, a INT64, b INT64);");
        BulkInsert(engine, ctx, "oob_l", 3000, 500,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i);
                     sql += ',';
                     sql += std::to_string((i * 3) + 1);
                     sql += ',';
                     sql += std::to_string((i * 5) + 2);
                     sql += ')';
                   });
        BulkInsert(
            engine, ctx, "oob_r", 3000, 500, [](std::string& sql, size_t i) {
              sql += '(';
              sql += std::to_string(i);
              sql += ',';
              sql += std::to_string((i % 20 == 0) ? ((i * 3) + 1) : ((i * 3) + 2));
              sql += ',';
              sql += std::to_string((i % 25 == 0) ? ((i * 5) + 2) : ((i * 5) + 4));
              sql += ')';
            });
      })) {
    return 1;
  }

  // 5. wdi_tbl (30k rows) for WindowDistinctInverse (201-row sliding frame).
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx, "CREATE TABLE wdi_tbl (id INT64, v INT64);");
        BulkInsert(engine, ctx, "wdi_tbl", 30000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i);
                     sql += ',';
                     sql += std::to_string((i * 37) % 120);
                     sql += ')';
                   });
      })) {
    return 1;
  }

  // 6. halo_tbl (100k rows shuffled) for WindowHalo.
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx, "CREATE TABLE halo_tbl (id INT64, val INT64);");
        BulkInsert(engine, ctx, "halo_tbl", 100000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string((i * 48271ULL) % 100000ULL);
                     sql += ',';
                     sql += std::to_string((i * 13) % 1000);
                     sql += ')';
                   });
      })) {
    return 1;
  }

  // 7. don_tbl (100k rows across 1000 groups) for DistinctOnTop1.
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx,
                  "CREATE TABLE don_tbl (grp INT64, ts INT64, payload INT64);");
        BulkInsert(engine, ctx, "don_tbl", 100000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i % 1000);
                     sql += ',';
                     sql += std::to_string((i * 97) % 100000);
                     sql += ',';
                     sql += std::to_string(i);
                     sql += ')';
                   });
      })) {
    return 1;
  }

  // 8. oft_l / oft_r (100k rows each, r indexed) for OuterFetchTies.
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx,
                  "CREATE TABLE oft_l (id INT64 PRIMARY KEY, score INT64, pad "
                  "STRING);");
        ExecOrDie(engine, ctx, "CREATE TABLE oft_r (id INT64, info STRING);");
        BulkInsert(engine, ctx, "oft_l", 100000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i);
                     sql += ',';
                     sql += std::to_string((i * 29) % 5000);
                     sql += ",'pad_";
                     sql += std::to_string(i % 50);
                     sql += "')";
                   });
        BulkInsert(engine, ctx, "oft_r", 100000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i);
                     sql += ",'info_";
                     sql += std::to_string(i % 100);
                     sql += "')";
                   });
        ExecOrDie(engine, ctx, "CREATE INDEX idx_oft_r_id ON oft_r (id);");
      })) {
    return 1;
  }

  // 9. q_bench (200k rows) for QualifyPushdown (grouped QUALIFY).
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx,
                  "CREATE TABLE q_bench (dept INT64, item INT64, val INT64);");
        BulkInsert(engine, ctx, "q_bench", 200000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i % 100);
                     sql += ',';
                     sql += std::to_string((i / 100) % 200);
                     sql += ',';
                     sql += std::to_string((i * 13) % 1000);
                     sql += ')';
                   });
      })) {
    return 1;
  }

  // 10. ojc_l / ojc_r (10k x 10k rows across 40 keys + NULLs) for
  // OuterJoinCounts.
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx, "CREATE TABLE ojc_l (k INT64, pad INT64);");
        ExecOrDie(engine, ctx, "CREATE TABLE ojc_r (k INT64, pad INT64);");
        BulkInsert(
            engine, ctx, "ojc_l", 10000, 1000, [](std::string& sql, size_t i) {
              sql += '(';
              sql +=
                  (i % 100 == 0) ? std::string("NULL") : std::to_string(i % 40);
              sql += ',';
              sql += std::to_string(i);
              sql += ')';
            });
        BulkInsert(
            engine, ctx, "ojc_r", 10000, 1000, [](std::string& sql, size_t i) {
              sql += '(';
              sql +=
                  (i % 120 == 0) ? std::string("NULL") : std::to_string(i % 40);
              sql += ',';
              sql += std::to_string(i);
              sql += ')';
            });
      })) {
    return 1;
  }

  // 11. rowin_tbl (200k rows) for RowInMask.
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx,
                  "CREATE TABLE rowin_tbl (id INT64, a INT64, b INT64);");
        BulkInsert(engine, ctx, "rowin_tbl", 200000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i);
                     sql += ',';
                     sql += std::to_string(i % 200);
                     sql += ',';
                     sql += std::to_string((i % 200) * 11);
                     sql += ')';
                   });
      })) {
    return 1;
  }

  // 12. wnth_tbl (30k rows, sparse non-NULL) for NthNonNullFrame.
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx,
                  "CREATE TABLE wnth_tbl (p INT64, id INT64, v INT64);");
        BulkInsert(engine, ctx, "wnth_tbl", 30000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i % 3);
                     sql += ',';
                     sql += std::to_string(i / 3);
                     sql += ',';
                     sql += (i % 100 == 0) ? std::to_string(i) : "NULL";
                     sql += ')';
                   });
      })) {
    return 1;
  }

  // 13. wrd_tbl (20k rows) for RangeDistinctIndex (non-trailing-CURRENT ROW).
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx, "CREATE TABLE wrd_tbl (id INT64, v INT64);");
        BulkInsert(engine, ctx, "wrd_tbl", 20000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i);
                     sql += ',';
                     sql += std::to_string((i * 17) % 1500);
                     sql += ')';
                   });
      })) {
    return 1;
  }

  // 14. qrs_outer (30k rows) / qrs_inner (20k rows) for QuantifiedRangeSummary.
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx,
                  "CREATE TABLE qrs_outer (id INT64, k INT64, x INT64);");
        ExecOrDie(engine, ctx, "CREATE TABLE qrs_inner (k INT64, y INT64);");
        BulkInsert(engine, ctx, "qrs_outer", 30000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i);
                     sql += ',';
                     sql += std::to_string(i % 100);
                     sql += ',';
                     sql += std::to_string((i * 7) % 500);
                     sql += ')';
                   });
        BulkInsert(engine, ctx, "qrs_inner", 20000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i % 100);
                     sql += ',';
                     sql += std::to_string((i * 13) % 450);
                     sql += ')';
                   });
      })) {
    return 1;
  }

  // 15. ds_bench_o (80k rows) / ds_bench_i (2k rows) for DemandScalar.
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx, "CREATE TABLE ds_bench_o (id INT64, k INT64);");
        ExecOrDie(engine, ctx, "CREATE TABLE ds_bench_i (k INT64, val INT64);");
        BulkInsert(engine, ctx, "ds_bench_o", 80000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i);
                     sql += ',';
                     sql += std::to_string(i % 2000);
                     sql += ')';
                   });
        BulkInsert(engine, ctx, "ds_bench_i", 2000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i);
                     sql += ',';
                     sql += std::to_string((i * 3) + 1);
                     sql += ')';
                   });
      })) {
    return 1;
  }

  // 16. sky_bench (4k rows) for SkylineAntiJoin.
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx,
                  "CREATE TABLE sky_bench (id INT64, x INT64, y INT64);");
        BulkInsert(engine, ctx, "sky_bench", 4000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i);
                     sql += ',';
                     sql += std::to_string((i * 29) % 1000);
                     sql += ',';
                     sql += std::to_string((i * 53) % 1000);
                     sql += ')';
                   });
      })) {
    return 1;
  }

  // 17. iv_bench_segs (2k intervals) / iv_bench_pts (4k pts) for
  // IntervalCountSweep.
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx,
                  "CREATE TABLE iv_bench_segs (lo INT64, hi INT64);");
        ExecOrDie(engine, ctx,
                  "CREATE TABLE iv_bench_pts (id INT64, t INT64);");
        BulkInsert(engine, ctx, "iv_bench_segs", 2000, 1000,
                   [](std::string& sql, size_t i) {
                     const size_t lo = (i * 37) % 5000;
                     const size_t hi = lo + 50 + (i % 400);
                     sql += '(';
                     sql += std::to_string(lo);
                     sql += ',';
                     sql += std::to_string(hi);
                     sql += ')';
                   });
        BulkInsert(engine, ctx, "iv_bench_pts", 4000, 1000,
                   [](std::string& sql, size_t i) {
                     sql += '(';
                     sql += std::to_string(i);
                     sql += ',';
                     sql += std::to_string((i * 19) % 5500);
                     sql += ')';
                   });
      })) {
    return 1;
  }

  // 18. sort2_bench (150k rows) for PackedOrderKey.
  if (!with_write_tx([&](SqlEngine& engine, TransactionContext& ctx) {
        ExecOrDie(engine, ctx,
                  "CREATE TABLE sort2_bench (a INT64, b INT64, c INT64);");
        BulkInsert(engine, ctx, "sort2_bench", 150000, 1000,
                   [](std::string& sql, size_t i) {
                     const int64_t a =
                         static_cast<int64_t>((i * 7919) % 40000) - 20000;
                     const int64_t b =
                         static_cast<int64_t>((i * 104729) % 40000) - 20000;
                     sql += '(';
                     sql += std::to_string(a);
                     sql += ',';
                     sql += std::to_string(b);
                     sql += ',';
                     sql += std::to_string(i);
                     sql += ')';
                   });
      })) {
    return 1;
  }

  {
    SqlEngine engine(*db);
    for (const std::string& t :
         {"text_tbl",   "like_tbl",  "num_tbl",       "bk_tbl",
          "oob_l",      "oob_r",     "wdi_tbl",       "halo_tbl",
          "don_tbl",    "oft_l",     "oft_r",         "q_bench",
          "ojc_l",      "ojc_r",     "rowin_tbl",     "wnth_tbl",
          "wrd_tbl",    "qrs_outer", "qrs_inner",     "ds_bench_o",
          "ds_bench_i", "sky_bench", "iv_bench_segs", "iv_bench_pts",
          "sort2_bench"}) {
      CompactAndAnalyze(*db, engine, t);
    }
  }

  std::string rowin_list;
  rowin_list.reserve(1024);
  for (int i = 1; i <= 80; ++i) {
    if (i > 1) { rowin_list += ',';
}
    rowin_list += '(' + std::to_string(i) + ',' + std::to_string(i * 11) + ')';
  }

  const std::vector<WorkloadSpec> workloads = {
      {"LikeSegmentChain", "LikeSegmentChain.lean",
       "TINYLAMB_DISABLE_FAST_LIKE",
       "SELECT COUNT(*) FROM like_tbl WHERE body LIKE "
       "'%alpha_seg%beta_seg%gamma_seg%delta_seg';",
       false},
      {"LikeUnderscore", "LikeUnderscore.lean", "TINYLAMB_DISABLE_FAST_LIKE",
       "SELECT COUNT(*) FROM like_tbl WHERE body LIKE "
       "'hdr____mid_%_end____';",
       false},
      {"LikeShiftAnd", "LikeShiftAnd.lean", "TINYLAMB_DISABLE_FAST_LIKE",
       "SELECT COUNT(*) FROM like_tbl WHERE body LIKE "
       "'%aaaaaaaaaaaaaaa_z%';",
       true},
      {"RegexMandatoryLiteral", "RegexMandatoryLiteral.lean",
       "TINYLAMB_DISABLE_FAST_REGEX",
       "SELECT COUNT(*) FROM text_tbl WHERE REGEXP_CONTAINS(payload, "
       "'([0-9]+)-MANDATORY_TOKEN-([a-z]+)');",
       false},
      {"RegexBitsetNfa+ThompsonSearch",
       "RegexBitsetNfa.lean + ThompsonSearch.lean",
       "TINYLAMB_DISABLE_FAST_REGEX",
       "SELECT COUNT(*) FROM text_tbl WHERE REGEXP_CONTAINS(payload, "
       "'(aa|bb)[0-9]+[xy]?');",
       false},
      {"SimdInRange", "SimdInRange.lean", "TINYLAMB_DISABLE_SIMD_IN_RANGE",
       "SELECT COUNT(*) FROM num_tbl WHERE ((a >= 100 AND a <= 163) OR a = "
       "-1);",
       true},
      {"ReciprocalDivMod", "ResearchProofs/ReciprocalDivMod.lean",
       "TINYLAMB_DISABLE_RECIPROCAL_DIVMOD",
       "SELECT SUM(DIV(a, 7) + MOD(a, 13)) FROM num_tbl;", false},
      {"FilterEvalOrder", "FilterEvalOrder.lean",
       "TINYLAMB_DISABLE_FILTER_ORDER",
       "SELECT COUNT(*) FROM like_tbl WHERE REGEXP_CONTAINS(body, "
       "'(aaaaaaaaaa[X-Z]z|bbbbbbbbbb[0-9]q)') AND (id < 50 OR id > 9950 OR "
       "id IS NULL);",
       true},
      {"BlockKleene", "BlockKleene.lean", "TINYLAMB_DISABLE_BLOCK_KLEENE",
       "SELECT a FROM bk_tbl WHERE (a >= 199500 AND b < 50) OR (a < 100 AND "
       "b >= 0) OR NOT (a <= 199000);",
       true},
      {"OuterOrBranches", "OuterOrBranches.lean",
       "TINYLAMB_DISABLE_OUTER_OR_BRANCHES",
       "SELECT l.id, r.id FROM oob_l l LEFT JOIN oob_r r ON l.a = r.a OR "
       "l.b = r.b;",
       true},
      {"WindowDistinctInverse", "Research/WindowDistinctInverse.lean",
       "TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE",
       "SELECT SUM(c) FROM (SELECT COUNT(DISTINCT v) OVER (ORDER BY id ROWS "
       "BETWEEN 200 PRECEDING AND CURRENT ROW) AS c FROM wdi_tbl) sub;",
       false},
      {"WindowHalo", "WindowHalo.lean", "TINYLAMB_DISABLE_WINDOW_HALO",
       "SELECT id, AVG(val) OVER (ORDER BY id ROWS BETWEEN 100 PRECEDING AND "
       "100 FOLLOWING), SUM(val) OVER (ORDER BY id ROWS BETWEEN 100 PRECEDING "
       "AND 100 FOLLOWING), MAX(val) OVER (ORDER BY id ROWS BETWEEN 100 "
       "PRECEDING AND 100 FOLLOWING) FROM halo_tbl ORDER BY id LIMIT 20 OFFSET "
       "200;",
       true},
      {"DistinctOnTop1", "ProposedProofs/DistinctOnTop1.lean",
       "TINYLAMB_DISABLE_DISTINCT_ON_TOP1",
       "SELECT DISTINCT ON (grp) grp, ts, payload FROM don_tbl ORDER BY grp, "
       "ts DESC;",
       false},
      {"OuterFetchTies", "OuterFetchTies.lean",
       "TINYLAMB_DISABLE_OUTER_FETCH_TIES",
       "SELECT l.id, l.score, r.info FROM oft_l l LEFT JOIN oft_r r ON l.id = "
       "r.id ORDER BY l.score DESC, l.id ASC LIMIT 20;",
       true},
      {"QualifyPushdown", "ProposedProofs/QualifyPushdown.lean",
       "TINYLAMB_DISABLE_QUALIFY_PUSHDOWN",
       "SELECT dept, item, SUM(val) AS s, ROW_NUMBER() OVER (PARTITION BY dept "
       "ORDER BY SUM(val) DESC, item) AS rn FROM q_bench GROUP BY dept, item "
       "QUALIFY dept = 7 AND rn <= 5 ORDER BY item;",
       true},
      {"OuterJoinCounts", "ResearchProofs/OuterJoinCounts.lean",
       "TINYLAMB_DISABLE_OUTER_JOIN_COUNTS",
       "SELECT COALESCE(l.k, r.k) AS k, COUNT(*) AS cnt FROM ojc_l l FULL "
       "OUTER JOIN ojc_r r ON l.k = r.k GROUP BY COALESCE(l.k, r.k) ORDER BY k "
       "NULLS LAST;",
       true},
      {"RowInMask", "ProposedProofs/RowInMask.lean",
       "TINYLAMB_DISABLE_ROW_IN_MASK",
       "SELECT COUNT(*) FROM rowin_tbl WHERE (a, b) IN (" + rowin_list + ");",
       false},
      {"NthNonNullFrame", "ProposedProofs/NthNonNullFrame.lean",
       "TINYLAMB_DISABLE_NTH_NON_NULL_FRAME",
       "SELECT SUM(fv) FROM (SELECT FIRST_VALUE(v IGNORE NULLS) OVER "
       "(PARTITION BY p ORDER BY id ROWS BETWEEN 2000 PRECEDING AND 2000 "
       "FOLLOWING) AS fv FROM wnth_tbl) sub;",
       false},
      {"RangeDistinctIndex", "ProposedProofs/RangeDistinctIndex.lean",
       "TINYLAMB_DISABLE_RANGE_DISTINCT_INDEX",
       "SELECT SUM(c) FROM (SELECT COUNT(DISTINCT v) OVER (ORDER BY id RANGE "
       "BETWEEN 500 PRECEDING AND 500 FOLLOWING) AS c FROM wrd_tbl) sub;",
       false},
      {"QuantifiedRangeSummary", "ProposedProofs/QuantifiedRangeSummary.lean",
       "TINYLAMB_DISABLE_QUANTIFIED_RANGE_SUMMARY",
       "SELECT COUNT(*) FROM qrs_outer o WHERE o.x < ANY (SELECT i.y FROM "
       "qrs_inner i WHERE i.k = o.k);",
       false},
      {"DemandScalar", "ProposedProofs/DemandScalar.lean",
       "TINYLAMB_DISABLE_DEMAND_SCALAR",
       "SELECT SUM((SELECT i.val FROM ds_bench_i i WHERE i.k = o.k)) FROM "
       "ds_bench_o o;",
       false},
      {"SkylineAntiJoin", "ProposedProofs/SkylineAntiJoin.lean",
       "TINYLAMB_DISABLE_SKYLINE_ANTIJOIN",
       "SELECT b.id FROM sky_bench b WHERE NOT EXISTS (SELECT 1 FROM "
       "sky_bench a WHERE a.x < b.x AND a.y < b.y);",
       false},
      {"IntervalCountSweep", "ProposedProofs/IntervalCountSweep.lean",
       "TINYLAMB_DISABLE_INTERVAL_COUNT_SWEEP",
       "SELECT SUM((SELECT COUNT(*) FROM iv_bench_segs s WHERE s.lo <= p.t AND "
       "p.t < s.hi)) FROM iv_bench_pts p;",
       false},
      {"PackedOrderKey", "ProposedProofs/PackedOrderKey.lean",
       "TINYLAMB_DISABLE_PACKED_ORDER_KEY",
       "SELECT a, b, c FROM sort2_bench ORDER BY a ASC, b DESC LIMIT 10 OFFSET "
       "149990;",
       false},
  };

  std::vector<WorkloadResult> results;
  results.reserve(workloads.size());

  TransactionContext read_ctx = db->BeginReadOnlyContext();
  SqlEngine engine(*db);

  for (const WorkloadSpec& spec : workloads) {
    auto measure_mode = [&](bool disable, std::vector<std::string>* out_rows) {
      ResetOptFlags(workloads);
      if (disable) {
        static_cast<void>(platform::SetEnv(spec.disable_env.c_str(), "1", 1));
        ReloadFastLikeDisabledFromEnv();
        ReloadFastRegexDisabledFromEnv();
        PreparedPlanCache::Instance().Clear();
        CurrentCompiledPlans().clear();
      }
      if (spec.print_explain && !disable) {
        const auto exp =
            CollectRowsUnsorted(engine, read_ctx, "EXPLAIN " + spec.sql);
        std::cout << "\n[EXPLAIN " << spec.name << " ON]:\n";
        for (const auto& line : exp) {
          std::cout << "  " << line << "\n";
        }
      }
      // 1 warmup iteration (outside timing)
      *out_rows = CollectSortedRows(engine, read_ctx, spec.sql);
      std::vector<double> samples;
      samples.reserve(5);
      for (int it = 0; it < 5; ++it) {
        const auto t0 = std::chrono::steady_clock::now();
        const size_t cur_rows = CountRows(engine, read_ctx, spec.sql);
        const auto t1 = std::chrono::steady_clock::now();
        if (cur_rows != out_rows->size()) {
          std::cerr << "Self-inconsistency within mode for " << spec.name
                    << "\n";
          std::abort();
        }
        const double ms =
            std::chrono::duration<double, std::milli>(t1 - t0).count();
        samples.push_back(ms);
      }
      return Median(std::move(samples));
    };

    std::vector<std::string> off_rows;
    std::vector<std::string> on_rows;
    const double off_ms = measure_mode(true, &off_rows);
    const double on_ms = measure_mode(false, &on_rows);
    ResetOptFlags(workloads);

    const bool parity = (off_rows == on_rows);
    if (!parity) {
      std::cerr << "PARITY MISMATCH in " << spec.name
                << " off_rows=" << off_rows.size()
                << " on_rows=" << on_rows.size() << "\n";
      return 2;
    }

    WorkloadResult res;
    res.spec = spec;
    res.off_ms = off_ms;
    res.on_ms = on_ms;
    res.speedup = off_ms / std::max(1e-6, on_ms);
    res.rows = on_rows.size();
    res.parity_ok = parity;
    results.push_back(std::move(res));
  }
  if (read_ctx.txn_.PreCommit() != Status::kSuccess) {
    std::cerr << "PreCommit after read-only workload failed\n";
    return 1;
  }

  double log_sum = 0.0;
  double total_off = 0.0;
  double total_on = 0.0;
  std::cout << "\n============================================================"
               "============================\n";
  std::cout << std::left << std::setw(32) << "Workload" << std::right
            << std::setw(12) << "OFF (ms)" << std::setw(12) << "ON (ms)"
            << std::setw(12) << "Speedup" << std::setw(10) << "Rows"
            << std::setw(10) << "Parity\n";
  std::cout << "------------------------------------------------------------"
               "----------------------------\n";
  for (const auto& r : results) {
    log_sum += std::log(r.speedup);
    total_off += r.off_ms;
    total_on += r.on_ms;
    std::cout << std::left << std::setw(32) << r.spec.name << std::right
              << std::fixed << std::setprecision(2) << std::setw(12) << r.off_ms
              << std::setw(12) << r.on_ms << std::setw(11) << r.speedup << "x"
              << std::setw(10) << r.rows << std::setw(10)
              << (r.parity_ok ? "PASS" : "FAIL") << "\n";
  }
  const double geomean =
      std::exp(log_sum / static_cast<double>(results.size()));
  const double total_speedup = total_off / std::max(1e-6, total_on);
  std::cout << "------------------------------------------------------------"
               "----------------------------\n";
  std::cout << std::left << std::setw(32) << "GEOMEAN / TOTAL" << std::right
            << std::fixed << std::setprecision(2) << std::setw(12) << total_off
            << std::setw(12) << total_on << std::setw(11) << total_speedup
            << "x (geomean " << geomean << "x)\n";

  const std::string json_path =
      (std::filesystem::temp_directory_path() /
       ("lean_opt_benchmark_results_" +
        std::to_string(static_cast<long>(platform::GetPid())) + ".json"))
          .string();
  std::ofstream jf(json_path);
  jf << "{\n  \"workloads\": [\n";
  for (size_t i = 0; i < results.size(); ++i) {
    const auto& r = results[i];
    jf << "    {\"name\": " << JsonEscape(r.spec.name)
       << ", \"lean_proof\": " << JsonEscape(r.spec.lean_proof)
       << ", \"disable_env\": " << JsonEscape(r.spec.disable_env)
       << ", \"off_ms\": " << std::fixed << std::setprecision(3) << r.off_ms
       << ", \"on_ms\": " << r.on_ms << ", \"speedup\": " << r.speedup
       << ", \"rows\": " << r.rows
       << ", \"parity_ok\": " << (r.parity_ok ? "true" : "false") << "}";
    jf << (i + 1 < results.size() ? ",\n" : "\n");
  }
  jf << "  ],\n  \"total_off_ms\": " << total_off
     << ",\n  \"total_on_ms\": " << total_on
     << ",\n  \"total_speedup\": " << total_speedup
     << ",\n  \"geomean_speedup\": " << geomean << "\n}\n";
  jf.close();
  std::cout << "Results written to " << json_path << "\n";
  return 0;
}
