/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <ratio>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#if __has_include(<valgrind/callgrind.h>)
#include <valgrind/callgrind.h>
#define TINYLAMB_HAS_CALLGRIND 1
#endif

#include "benchmark/tpcc_workload.hpp"
#include "common/constants.hpp"
#include "database/database.hpp"
#include "executor/query_scheduler.hpp"
#include "query/plan_cache.hpp"
#include "query/sql_engine.hpp"

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
  std::string database_path;
  int scale_factor{1};
  int clients{0};
  int warmup_seconds{2};
  int measure_seconds{10};
  uint64_t seed{20260819};
  bool verify_only{false};
  bool reuse_existing{false};
  bool synchronous_commit{true};
  bool profile_waits{false};
  size_t wal_sync_ms{1};
  std::string transaction_only;
};

struct TypeStats {
  uint64_t attempted{0};
  uint64_t committed{0};
  uint64_t user_rollback{0};
  uint64_t statements{0};
  uint64_t sql_path_bypasses{0};
  std::chrono::nanoseconds latency{0};
  std::string first_error;
};

std::string TruncateDiagnostic(std::string_view text, size_t maximum = 320) {
  if (text.size() <= maximum) { return std::string(text); }
  return std::string(text.substr(0, maximum)) + "...<truncated>";
}

struct WorkerStats {
  std::array<TypeStats, tinylamb::kTpccTransactionTypeCount> types;
  uint64_t warmup_engine_aborts{0};
  std::string warmup_first_error;
  std::string first_error;
  tinylamb::SqlRuntimeStats sql_runtime;
};

void Usage(std::ostream& out, std::string_view program) {
  out << "usage: " << program << " <new-database-path> [options]\n"
      << "  --scale-factor N    TPC-C warehouses W (default: 1)\n"
      << "  --clients N         terminals (default: 10 per warehouse)\n"
      << "  --warmup N          warmup seconds (default: 2)\n"
      << "  --seconds N         measurement seconds (default: 10)\n"
      << "  --seed N            random seed\n"
      << "  --verify-only       run each transaction once and stop\n"
      << "  --reuse-existing    reuse an already initialized fixture\n"
      << "  --no-sync-commit    diagnostic only: do not wait for WAL durability\n"
      << "  --profile-waits     collect WAL/MVCC/scheduler wait counters\n"
      << "  --transaction N     diagnostic: run only one transaction type\n"
      << "  --wal-sync-ms N     group-commit interval (default: 1)\n"
      << "  --deadlock-policy P legacy|wait_die|wound_wait|deadlock_detect\n"
      << "\n"
      << "Population follows TPC-C Clause 4.3 for scale factor W:\n"
      << "  10 districts/warehouse, 3000 customers/district, 100000 items,\n"
      << "  3000 orders/district with the newest 900 in NEW-ORDER.\n"
      << "Think and keying time are omitted, so the result is not audited "
         "tpmC.\n";
}

template <typename Integer>
bool ParseInteger(std::string_view value, Integer* destination) {
  const char* begin = value.data();
  const char* end = begin + value.size();
  auto [parsed_end, error] = std::from_chars(begin, end, *destination);
  return error == std::errc() && parsed_end == end;
}

bool ParseOptions(int argc, char** argv, Options* options) {
  for (int i = 1; i < argc; ++i) {
    if (std::string_view(argv[i]) == "--help") {
      return false;
    }
  }
  if (argc < 2) {
    return false;
  }
  if (std::string_view(argv[1]).starts_with("--")) {
    return false;
  }
  options->database_path = argv[1];
  for (int i = 2; i < argc; ++i) {
    const std::string_view argument(argv[i]);
    if (argument == "--verify-only") {
      options->verify_only = true;
      continue;
    }
    if (argument == "--reuse-existing") {
      options->reuse_existing = true;
      continue;
    }
    if (argument == "--no-sync-commit") {
      options->synchronous_commit = false;
      continue;
    }
    if (argument == "--profile-waits") {
      options->profile_waits = true;
      continue;
    }
    if (argument == "--transaction") {
      if (++i == argc) {
        return false;
      }
      options->transaction_only = argv[i];
      continue;
    }
    if (argument == "--help") {
      return false;
    }
    if (i + 1 >= argc) {
      return false;
    }
    const std::string_view value(argv[++i]);
    bool parsed = false;
    if (argument == "--scale-factor" || argument == "--sf") {
      parsed = ParseInteger(value, &options->scale_factor);
    } else if (argument == "--clients") {
      parsed = ParseInteger(value, &options->clients);
    } else if (argument == "--warmup") {
      parsed = ParseInteger(value, &options->warmup_seconds);
    } else if (argument == "--seconds") {
      parsed = ParseInteger(value, &options->measure_seconds);
    } else if (argument == "--seed") {
      parsed = ParseInteger(value, &options->seed);
    } else if (argument == "--wal-sync-ms") {
      parsed = ParseInteger(value, &options->wal_sync_ms);
    }
    if (!parsed) {
      return false;
    }
  }
  return options->scale_factor >= 1 && options->clients >= 0 &&
         options->warmup_seconds >= 0 && options->measure_seconds > 0 &&
         options->wal_sync_ms > 0;
}

bool VerifyTransactions(tinylamb::Database& database,
                        const tinylamb::TpccScale& scale,
                        const tinylamb::TpccNurand& nurand, uint64_t seed) {
  tinylamb::TpccWorkload workload(database, scale, seed, 0, nurand);
  constexpr std::array<tinylamb::TpccTransactionType, 5> transactions = {
      tinylamb::TpccTransactionType::kNewOrder,
      tinylamb::TpccTransactionType::kPayment,
      tinylamb::TpccTransactionType::kOrderStatus,
      tinylamb::TpccTransactionType::kDelivery,
      tinylamb::TpccTransactionType::kStockLevel};
  bool ok = true;
  for (tinylamb::TpccTransactionType type : transactions) {
    try {
      const tinylamb::TpccTransactionResult result = workload.Execute(type);
      const bool transaction_pass = result.committed || result.user_rollback;
      const bool sql_path_pass = result.sql_statements > 0;
      const bool pass = transaction_pass && sql_path_pass;
      std::cout << "verification." << tinylamb::ToString(type) << '='
                << (pass ? "PASS" : "FAIL")
                << " statements=" << result.sql_statements
                << " sql_path=" << (sql_path_pass ? "PASS" : "FAIL");
      if (!sql_path_pass) {
        std::cout << " gate_error=\"transaction bypassed SqlEngine\"";
      }
      if (!result.error.empty()) {
        std::cout << " error=\"" << result.error << '"';
      }
      std::cout << '\n';
      ok = ok && pass;
    } catch (const std::exception& ex) {
      std::cout << "verification." << tinylamb::ToString(type)
                << "=FAIL exception=\"" << ex.what() << "\"\n";
      ok = false;
    }
  }
  return ok;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!ParseOptions(argc, argv, &options)) {
    Usage(std::cerr, argc == 0 ? "tinylamb_tpcc_benchmark" : argv[0]);
    return 2;
  }
  if (!options.transaction_only.empty()) {
    bool known = false;
    for (size_t i = 0; i < tinylamb::kTpccTransactionTypeCount; ++i) {
      known = known || tinylamb::ToString(
                           static_cast<tinylamb::TpccTransactionType>(i)) ==
                           options.transaction_only;
    }
    if (!known) {
      std::cerr << "unknown transaction type: " << options.transaction_only
                << '\n';
      return 2;
    }
  }
  const tinylamb::TpccScale scale =
      tinylamb::TpccScale::Official(options.scale_factor);
  const tinylamb::TpccNurand nurand =
      tinylamb::TpccNurand::FromSeed(options.seed);
  if (options.clients == 0) {
    options.clients = scale.Terminals();
  }

  std::cout << "benchmark=tinylamb_tpcc\n"
            << "tpmc_compliant=false\n"
            << "think_time=omitted\n"
            << "keying_time=omitted\n"
            << "database=" << options.database_path << '\n';
  // TINYLAMB_DEADLOCK_POLICY selects the write-intent victim-selection
  // policy. The --deadlock-policy CLI argument overrides the env var.
  auto parse_policy =
      [](std::string_view s) -> tinylamb::Database::DeadlockPolicy {
    if (s == "wait_die") return tinylamb::Database::DeadlockPolicy::kWaitDie;
    if (s == "wound_wait") {
      return tinylamb::Database::DeadlockPolicy::kWoundWait;
    }
    if (s == "deadlock_detect" || s == "detect") {
      return tinylamb::Database::DeadlockPolicy::kDeadlockDetect;
    }
    return tinylamb::Database::DeadlockPolicy::kLegacy;
  };
  const char* deadlock_env = std::getenv("TINYLAMB_DEADLOCK_POLICY");
  std::string deadlock_arg;
  for (int i = 2; i < argc; ++i) {
    if (std::string_view(argv[i]) == "--deadlock-policy") {
      if (i + 1 < argc) { deadlock_arg = argv[++i]; }
    }
  }
  const std::string policy_source = !deadlock_arg.empty()
                                        ? deadlock_arg
                                        : (deadlock_env != nullptr
                                               ? std::string(deadlock_env)
                                               : std::string("legacy"));
  const tinylamb::Database::DeadlockPolicy deadlock_policy =
      parse_policy(policy_source);
  std::cout << "deadlock_policy=" << policy_source
            << " (0=legacy 1=wait_die 2=wound_wait 3=deadlock_detect)\n";

  tinylamb::Database database(options.database_path, options.wal_sync_ms);
  database.SetDeadlockPolicy(deadlock_policy);
  std::string error;
  if (!options.reuse_existing) {
    const tinylamb::Status initialized = tinylamb::TpccWorkload::Initialize(
        database, scale, &error, options.seed);
    if (initialized != tinylamb::Status::kSuccess) {
      std::cerr << "initialization failed: " << error << " (" << initialized
                << ")\n";
      return 1;
    }
  }
  try {
    if (!VerifyTransactions(database, scale, nurand, options.seed)) {
      return 1;
    }
  } catch (const std::exception& ex) {
    std::cerr << "verification aborted: " << ex.what() << '\n';
    return 1;
  }
  if (options.verify_only) {
    return 0;
  }

  database.SetSynchronousCommit(options.synchronous_commit);
  database.SetTransactionMetricsEnabled(options.profile_waits);
  tinylamb::QueryScheduler::Global().SetMetricsEnabled(options.profile_waits);
  const tinylamb::TransactionRuntimeStats txn_stats_before =
      database.TransactionStats();
  const tinylamb::QuerySchedulerStats scheduler_stats_before =
      tinylamb::QueryScheduler::Global().Stats();
  const uint64_t plan_hits_before =
      tinylamb::PlanCacheStats().hits.load(std::memory_order_relaxed);
  const uint64_t plan_misses_before =
      tinylamb::PlanCacheStats().misses.load(std::memory_order_relaxed);

  std::cout << "clients=" << options.clients << '\n'
            << "synchronous_commit="
            << (options.synchronous_commit ? "true" : "false") << '\n'
            << "wal_sync_ms=" << options.wal_sync_ms << '\n'
            << "profile_waits=" << (options.profile_waits ? "true" : "false")
            << '\n'
            << "transaction_only="
            << (options.transaction_only.empty() ? "mixed"
                                                 : options.transaction_only)
            << '\n'
            << "warmup_seconds=" << options.warmup_seconds << '\n'
            << "measurement_seconds=" << options.measure_seconds << '\n'
            << "mix=new_order:45,payment:43,order_status:4,delivery:4,"
               "stock_level:4\n";

  std::vector<WorkerStats> worker_stats(static_cast<size_t>(options.clients));
  const Clock::time_point start = Clock::now() + std::chrono::milliseconds(100);
  const Clock::time_point measured_start =
      start + std::chrono::seconds(options.warmup_seconds);
  const Clock::time_point measured_end =
      measured_start + std::chrono::seconds(options.measure_seconds);
  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(options.clients));
  for (int worker = 0; worker < options.clients; ++worker) {
    workers.emplace_back([&, worker] {
      tinylamb::SqlEngine::SetThreadRuntimeProfiling(options.profile_waits);
      tinylamb::TpccWorkload workload(
          database, scale, options.seed + static_cast<uint64_t>(worker) + 1,
          (worker * scale.Terminals()) / options.clients, nurand);
      std::this_thread::sleep_until(start);
      const auto next_type = [&] {
        if (options.transaction_only.empty()) {
          return workload.NextTransactionType();
        }
        for (size_t i = 0; i < tinylamb::kTpccTransactionTypeCount; ++i) {
          const auto candidate = static_cast<tinylamb::TpccTransactionType>(i);
          if (tinylamb::ToString(candidate) == options.transaction_only) {
            return candidate;
          }
        }
        return tinylamb::TpccTransactionType::kCount;
      };
      while (Clock::now() < measured_start) {
        try {
          const tinylamb::TpccTransactionResult result =
              workload.Execute(next_type());
          if (!result.committed && !result.user_rollback) {
            ++worker_stats[static_cast<size_t>(worker)].warmup_engine_aborts;
            if (worker_stats[static_cast<size_t>(worker)]
                    .warmup_first_error.empty()) {
              worker_stats[static_cast<size_t>(worker)].warmup_first_error =
                  result.error;
            }
          }
        } catch (const std::exception& ex) {
          ++worker_stats[static_cast<size_t>(worker)].warmup_engine_aborts;
          if (worker_stats[static_cast<size_t>(worker)]
                  .warmup_first_error.empty()) {
            worker_stats[static_cast<size_t>(worker)].warmup_first_error =
                ex.what();
          }
        }
      }
      // Warmup exercises the same caches but is not part of the reported
      // measurement interval.
      tinylamb::SqlEngine::SetThreadRuntimeProfiling(options.profile_waits);
      while (Clock::now() < measured_end) {
        const tinylamb::TpccTransactionType type = next_type();
        const Clock::time_point before = Clock::now();
        const uint64_t sql_before =
            tinylamb::SqlEngine::ThreadExecutionCount();
        tinylamb::TpccTransactionResult result;
        try {
          result = workload.Execute(type);
        } catch (const std::exception& ex) {
          result.type = type;
          result.error = ex.what();
        }
        const uint64_t sql_invocations =
            tinylamb::SqlEngine::ThreadExecutionCount() - sql_before;
        result.sql_statements = std::max(result.sql_statements,
                                         sql_invocations);
        const auto elapsed = Clock::now() - before;
        TypeStats& stats = worker_stats[static_cast<size_t>(worker)]
                               .types[static_cast<size_t>(type)];
        ++stats.attempted;
        stats.committed += result.committed ? 1 : 0;
        stats.user_rollback += result.user_rollback ? 1 : 0;
        stats.statements += result.sql_statements;
        stats.sql_path_bypasses += result.sql_statements == 0 ? 1 : 0;
        stats.latency +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed);
        if (!result.committed && !result.user_rollback &&
            worker_stats[static_cast<size_t>(worker)].first_error.empty()) {
          worker_stats[static_cast<size_t>(worker)].first_error = result.error;
        }
        if (!result.committed && !result.user_rollback &&
            stats.first_error.empty()) {
          stats.first_error = result.error;
        }
      }
      worker_stats[static_cast<size_t>(worker)].sql_runtime =
          tinylamb::SqlEngine::ThreadRuntimeStats();
    });
  }
#if defined(TINYLAMB_HAS_CALLGRIND)
  // Opt-in instruction profiling excludes database recovery, verification,
  // population and warmup.  Those phases otherwise dwarf a short OLTP run
  // under Callgrind and produce a confidently wrong optimization target.
  if (std::getenv("TINYLAMB_CALLGRIND_MEASUREMENT") != nullptr) {
    std::this_thread::sleep_until(measured_start);
    CALLGRIND_START_INSTRUMENTATION;
    std::this_thread::sleep_until(measured_end);
    CALLGRIND_STOP_INSTRUMENTATION;
  }
#endif
  for (std::thread& worker : workers) {
    worker.join();
  }

  std::array<TypeStats, tinylamb::kTpccTransactionTypeCount> totals;
  uint64_t warmup_engine_aborts = 0;
  std::string warmup_first_error;
  std::string first_error;
  for (const WorkerStats& worker : worker_stats) {
    warmup_engine_aborts += worker.warmup_engine_aborts;
    if (warmup_first_error.empty()) {
      warmup_first_error = worker.warmup_first_error;
    }
    if (first_error.empty()) {
      first_error = worker.first_error;
    }
    for (size_t i = 0; i < totals.size(); ++i) {
      totals[i].attempted += worker.types[i].attempted;
      totals[i].committed += worker.types[i].committed;
      totals[i].user_rollback += worker.types[i].user_rollback;
      totals[i].statements += worker.types[i].statements;
      totals[i].sql_path_bypasses += worker.types[i].sql_path_bypasses;
      totals[i].latency += worker.types[i].latency;
      if (totals[i].first_error.empty()) {
        totals[i].first_error = worker.types[i].first_error;
      }
    }
  }
  uint64_t attempted = 0;
  uint64_t committed = 0;
  uint64_t user_rollback = 0;
  uint64_t statements = 0;
  uint64_t sql_prepare_ns = 0;
  uint64_t sql_collect_ns = 0;
  bool sql_path_gate_ok = true;
  for (size_t i = 0; i < totals.size(); ++i) {
    attempted += totals[i].attempted;
    committed += totals[i].committed;
    user_rollback += totals[i].user_rollback;
    statements += totals[i].statements;
    if (totals[i].attempted > 0 &&
        (totals[i].statements == 0 || totals[i].sql_path_bypasses != 0)) {
      sql_path_gate_ok = false;
    }
    const auto type = static_cast<tinylamb::TpccTransactionType>(i);
    const double average_ms =
        totals[i].attempted == 0
            ? 0
            : std::chrono::duration<double, std::milli>(totals[i].latency)
                      .count() /
                  static_cast<double>(totals[i].attempted);
    std::cout << "transaction." << tinylamb::ToString(type)
              << ".attempted=" << totals[i].attempted << '\n'
              << "transaction." << tinylamb::ToString(type)
              << ".committed=" << totals[i].committed << '\n'
              << "transaction." << tinylamb::ToString(type)
              << ".user_rollback=" << totals[i].user_rollback << '\n'
              << "transaction." << tinylamb::ToString(type)
              << ".sql_path_bypasses=" << totals[i].sql_path_bypasses << '\n'
              << "transaction." << tinylamb::ToString(type)
              << ".average_latency_ms=" << std::fixed << std::setprecision(3)
              << average_ms << '\n';
    if (!totals[i].first_error.empty()) {
      std::cout << "transaction." << tinylamb::ToString(type)
                << ".first_error=\""
                << TruncateDiagnostic(totals[i].first_error) << "\"\n";
    }
  }
  for (const WorkerStats& worker : worker_stats) {
    sql_prepare_ns += worker.sql_runtime.prepare_ns;
    sql_collect_ns += worker.sql_runtime.collect_ns;
  }
  const auto seconds = static_cast<double>(options.measure_seconds);
  const uint64_t new_order_committed =
      totals[static_cast<size_t>(tinylamb::TpccTransactionType::kNewOrder)]
          .committed;
  const uint64_t engine_aborts = attempted - committed - user_rollback;
  sql_path_gate_ok = sql_path_gate_ok && attempted > 0 && statements > 0;
  std::cout << "warmup_engine_aborted_transactions=" << warmup_engine_aborts
            << '\n';
  if (!warmup_first_error.empty()) {
    std::cout << "warmup_first_error=\""
              << TruncateDiagnostic(warmup_first_error) << "\"\n";
  }
  std::cout << "attempted_transactions=" << attempted << '\n'
            << "committed_transactions=" << committed << '\n'
            << "user_rollback_transactions=" << user_rollback << '\n'
            << "engine_aborted_transactions=" << engine_aborts << '\n'
            << "executed_sql_statements=" << statements << '\n'
            << "sql_path_gate=" << (sql_path_gate_ok ? "PASS" : "FAIL") << '\n'
            << "tps=" << std::fixed << std::setprecision(3)
            << static_cast<double>(committed) / seconds << '\n'
            << "sql_qps=" << static_cast<double>(statements) / seconds << '\n'
            << "sql_prepare_total_ms="
            << static_cast<double>(sql_prepare_ns) / 1'000'000.0 << '\n'
            << "sql_collect_total_ms="
            << static_cast<double>(sql_collect_ns) / 1'000'000.0 << '\n'
            << "new_order_tpm="
            << static_cast<double>(new_order_committed) * 60.0 / seconds
            << '\n';
  const uint64_t mix_total = attempted == 0 ? 1 : attempted;
  const auto percent = [&](size_t type) {
    return 100.0 * static_cast<double>(totals[type].attempted) /
           static_cast<double>(mix_total);
  };
  const double payment_pct =
      percent(static_cast<size_t>(tinylamb::TpccTransactionType::kPayment));
  const double order_status_pct =
      percent(static_cast<size_t>(tinylamb::TpccTransactionType::kOrderStatus));
  const double delivery_pct =
      percent(static_cast<size_t>(tinylamb::TpccTransactionType::kDelivery));
  const double stock_level_pct =
      percent(static_cast<size_t>(tinylamb::TpccTransactionType::kStockLevel));
  const bool mix_ok = payment_pct >= 43.0 && order_status_pct >= 4.0 &&
                      delivery_pct >= 4.0 && stock_level_pct >= 4.0;
  std::cout << "mix.new_order.percent=" << std::fixed << std::setprecision(3)
            << percent(static_cast<size_t>(
                   tinylamb::TpccTransactionType::kNewOrder))
            << '\n'
            << "mix.payment.percent=" << payment_pct << '\n'
            << "mix.order_status.percent=" << order_status_pct << '\n'
            << "mix.delivery.percent=" << delivery_pct << '\n'
            << "mix.stock_level.percent=" << stock_level_pct << '\n'
            << "mix_clause_542=" << (mix_ok ? "ok" : "short_interval") << '\n';
  if (!first_error.empty()) {
    std::cout << "first_error=\"" << TruncateDiagnostic(first_error)
              << "\"\n";
  }
  const uint64_t plan_hits =
      tinylamb::PlanCacheStats().hits.load(std::memory_order_relaxed) -
      plan_hits_before;
  const uint64_t plan_misses =
      tinylamb::PlanCacheStats().misses.load(std::memory_order_relaxed) -
      plan_misses_before;
  const uint64_t plan_replays =
      tinylamb::PlanCacheStats().replays.load(std::memory_order_relaxed);
  const uint64_t plan_parameter_mismatches =
      tinylamb::PlanCacheStats().parameter_mismatches.load(
          std::memory_order_relaxed);
  std::cout << "plan_cache_hits=" << plan_hits << '\n'
            << "plan_cache_misses=" << plan_misses << '\n'
            << "plan_cache_replays=" << plan_replays << '\n'
            << "plan_cache_parameter_mismatches="
            << plan_parameter_mismatches << '\n'
            << "plan_cache_hit_percent=" << std::fixed << std::setprecision(3)
            << (plan_hits + plan_misses == 0
                    ? 0.0
                    : 100.0 * static_cast<double>(plan_hits) /
                          static_cast<double>(plan_hits + plan_misses))
            << '\n';
  if (options.profile_waits) {
    const tinylamb::TransactionRuntimeStats txn_after =
        database.TransactionStats();
    const tinylamb::QuerySchedulerStats scheduler_after =
        tinylamb::QueryScheduler::Global().Stats();
    const uint64_t wal_waits =
        txn_after.wal_wait_count - txn_stats_before.wal_wait_count;
    const uint64_t wal_wait_ns =
        txn_after.wal_wait_ns - txn_stats_before.wal_wait_ns;
    const uint64_t intent_attempts = txn_after.write_intent_attempts -
                                     txn_stats_before.write_intent_attempts;
    const uint64_t intent_conflicts = txn_after.write_intent_conflicts -
                                      txn_stats_before.write_intent_conflicts;
    const uint64_t intent_wait_ns = txn_after.write_intent_mutex_wait_ns -
                                    txn_stats_before.write_intent_mutex_wait_ns;
    const uint64_t commit_wait_ns = txn_after.commit_shard_mutex_wait_ns -
                                    txn_stats_before.commit_shard_mutex_wait_ns;
    const uint64_t scheduler_acquires =
        scheduler_after.acquire_count - scheduler_stats_before.acquire_count;
    const uint64_t scheduler_wait_ns = scheduler_after.acquire_wait_ns -
                                       scheduler_stats_before.acquire_wait_ns;
    std::cout << "wait.wal.count=" << wal_waits << '\n'
              << "wait.wal.total_ms="
              << static_cast<double>(wal_wait_ns) / 1'000'000.0 << '\n'
              << "wait.wal.average_us="
              << (wal_waits == 0 ? 0.0
                                 : static_cast<double>(wal_wait_ns) / 1000.0 /
                                       static_cast<double>(wal_waits))
              << '\n'
              << "mvcc.write_intent.attempts=" << intent_attempts << '\n'
              << "mvcc.write_intent.conflicts=" << intent_conflicts << '\n'
              << "mvcc.write_intent.conflict_percent="
              << (intent_attempts == 0
                      ? 0.0
                      : 100.0 * static_cast<double>(intent_conflicts) /
                            static_cast<double>(intent_attempts))
              << '\n'
              << "wait.mvcc_intent_mutex.total_ms="
              << static_cast<double>(intent_wait_ns) / 1'000'000.0 << '\n'
              << "wait.mvcc_commit_shards.total_ms="
              << static_cast<double>(commit_wait_ns) / 1'000'000.0 << '\n'
              << "wait.scheduler.acquires=" << scheduler_acquires << '\n'
              << "wait.scheduler.total_ms="
              << static_cast<double>(scheduler_wait_ns) / 1'000'000.0 << '\n'
              << "wait.scheduler.contended="
              << scheduler_after.contended_acquires -
                     scheduler_stats_before.contended_acquires
              << '\n';
  }
  if (!sql_path_gate_ok) {
    std::cerr << "TPC-C SQL path gate failed: measured transactions did not "
                 "execute through SqlEngine\n";
    return 4;
  }
  return engine_aborts == 0 ? 0 : 3;
}
