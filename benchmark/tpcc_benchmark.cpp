/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "benchmark/tpcc_workload.hpp"
#include "database/database.hpp"

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
};

struct TypeStats {
  uint64_t attempted{0};
  uint64_t committed{0};
  uint64_t user_rollback{0};
  uint64_t statements{0};
  std::chrono::nanoseconds latency{0};
};

struct WorkerStats {
  std::array<TypeStats, tinylamb::kTpccTransactionTypeCount> types;
  std::string first_error;
};

void Usage(std::ostream& out, std::string_view program) {
  out << "usage: " << program << " <new-database-path> [options]\n"
      << "  --scale-factor N  TPC-C warehouses W (default: 1)\n"
      << "  --clients N       terminals (default: 10 per warehouse)\n"
      << "  --warmup N        warmup seconds (default: 2)\n"
      << "  --seconds N       measurement seconds (default: 10)\n"
      << "  --seed N          random seed\n"
      << "  --verify-only     run each transaction once and stop\n"
      << "\n"
      << "Population follows TPC-C Clause 4.3 for scale factor W:\n"
      << "  10 districts/warehouse, 3000 customers/district, 100000 items,\n"
      << "  3000 orders/district with the newest 900 in NEW-ORDER.\n"
      << "Think and keying time are omitted, so the result is not audited tpmC.\n";
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
    if (std::string_view(argv[i]) == "--help") return false;
  }
  if (argc < 2) return false;
  if (std::string_view(argv[1]).starts_with("--")) return false;
  options->database_path = argv[1];
  for (int i = 2; i < argc; ++i) {
    const std::string_view argument(argv[i]);
    if (argument == "--verify-only") {
      options->verify_only = true;
      continue;
    }
    if (argument == "--help") return false;
    if (i + 1 >= argc) return false;
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
    }
    if (!parsed) return false;
  }
  return options->scale_factor >= 1 && options->clients >= 0 &&
         options->warmup_seconds >= 0 && options->measure_seconds > 0;
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
      const bool pass = result.committed || result.user_rollback;
      std::cout << "verification." << tinylamb::ToString(type) << '='
                << (pass ? "PASS" : "FAIL")
                << " statements=" << result.sql_statements;
      if (!result.error.empty()) std::cout << " error=\"" << result.error << '"';
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
  const tinylamb::TpccScale scale =
      tinylamb::TpccScale::Official(options.scale_factor);
  const tinylamb::TpccNurand nurand =
      tinylamb::TpccNurand::FromSeed(options.seed);
  if (options.clients == 0) options.clients = scale.Terminals();

  std::cout << "benchmark=tinylamb_tpcc\n"
            << "tpmc_compliant=false\n"
            << "think_time=omitted\n"
            << "keying_time=omitted\n"
            << "database=" << options.database_path << '\n'
            << "scale_factor=" << scale.ScaleFactor() << '\n'
            << "warehouses=" << scale.warehouses << '\n'
            << "districts_per_warehouse=" << scale.districts_per_warehouse
            << '\n'
            << "customers_per_district=" << scale.customers_per_district << '\n'
            << "items=" << scale.items << '\n'
            << "initial_orders_per_district="
            << scale.initial_orders_per_district << '\n'
            << "new_orders_per_district=" << scale.new_orders_per_district
            << '\n'
            << "order_lines=" << scale.min_order_lines << '-'
            << scale.max_order_lines << '\n'
            << "terminals=" << scale.Terminals() << '\n'
            << "nurand.c_last_load=" << nurand.c_last_load << '\n'
            << "nurand.c_last_run=" << nurand.c_last_run << '\n'
            << "nurand.c_id=" << nurand.c_id << '\n'
            << "nurand.c_ol_i_id=" << nurand.c_ol_i_id << '\n';

  tinylamb::Database database(options.database_path);
  std::string error;
  const tinylamb::Status initialized = tinylamb::TpccWorkload::Initialize(
      database, scale, &error, options.seed);
  if (initialized != tinylamb::Status::kSuccess) {
    std::cerr << "initialization failed: " << error << " (" << initialized
              << ")\n";
    return 1;
  }
  try {
    if (!VerifyTransactions(database, scale, nurand, options.seed)) return 1;
  } catch (const std::exception& ex) {
    std::cerr << "verification aborted: " << ex.what() << '\n';
    return 1;
  }
  if (options.verify_only) return 0;

  std::cout << "clients=" << options.clients << '\n'
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
      tinylamb::TpccWorkload workload(
          database, scale, options.seed + static_cast<uint64_t>(worker) + 1,
          worker, nurand);
      std::this_thread::sleep_until(start);
      while (Clock::now() < measured_start) {
        workload.Execute(workload.NextTransactionType());
      }
      while (Clock::now() < measured_end) {
        const tinylamb::TpccTransactionType type =
            workload.NextTransactionType();
        const Clock::time_point before = Clock::now();
        tinylamb::TpccTransactionResult result;
        try {
          result = workload.Execute(type);
        } catch (const std::exception& ex) {
          result.type = type;
          result.error = ex.what();
        }
        const auto elapsed = Clock::now() - before;
        TypeStats& stats = worker_stats[static_cast<size_t>(worker)]
                               .types[static_cast<size_t>(type)];
        ++stats.attempted;
        stats.committed += result.committed ? 1 : 0;
        stats.user_rollback += result.user_rollback ? 1 : 0;
        stats.statements += result.sql_statements;
        stats.latency +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed);
        if (!result.committed && !result.user_rollback &&
            worker_stats[static_cast<size_t>(worker)].first_error.empty()) {
          worker_stats[static_cast<size_t>(worker)].first_error = result.error;
        }
      }
    });
  }
  for (std::thread& worker : workers) worker.join();

  std::array<TypeStats, tinylamb::kTpccTransactionTypeCount> totals;
  std::string first_error;
  for (const WorkerStats& worker : worker_stats) {
    if (first_error.empty()) first_error = worker.first_error;
    for (size_t i = 0; i < totals.size(); ++i) {
      totals[i].attempted += worker.types[i].attempted;
      totals[i].committed += worker.types[i].committed;
      totals[i].user_rollback += worker.types[i].user_rollback;
      totals[i].statements += worker.types[i].statements;
      totals[i].latency += worker.types[i].latency;
    }
  }
  uint64_t attempted = 0;
  uint64_t committed = 0;
  uint64_t user_rollback = 0;
  uint64_t statements = 0;
  for (size_t i = 0; i < totals.size(); ++i) {
    attempted += totals[i].attempted;
    committed += totals[i].committed;
    user_rollback += totals[i].user_rollback;
    statements += totals[i].statements;
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
              << ".average_latency_ms=" << std::fixed << std::setprecision(3)
              << average_ms << '\n';
  }
  const double seconds = static_cast<double>(options.measure_seconds);
  const uint64_t new_order_committed =
      totals[static_cast<size_t>(tinylamb::TpccTransactionType::kNewOrder)]
          .committed;
  const uint64_t engine_aborts = attempted - committed - user_rollback;
  std::cout << "attempted_transactions=" << attempted << '\n'
            << "committed_transactions=" << committed << '\n'
            << "user_rollback_transactions=" << user_rollback << '\n'
            << "engine_aborted_transactions=" << engine_aborts << '\n'
            << "executed_sql_statements=" << statements << '\n'
            << "tps=" << std::fixed << std::setprecision(3)
            << static_cast<double>(committed) / seconds << '\n'
            << "sql_qps=" << static_cast<double>(statements) / seconds << '\n'
            << "new_order_tpm=" << static_cast<double>(new_order_committed) *
                                       60.0 / seconds
            << '\n';
  const uint64_t mix_total = attempted == 0 ? 1 : attempted;
  const auto percent = [&](size_t type) {
    return 100.0 * static_cast<double>(totals[type].attempted) /
           static_cast<double>(mix_total);
  };
  const double payment_pct =
      percent(static_cast<size_t>(tinylamb::TpccTransactionType::kPayment));
  const double order_status_pct = percent(
      static_cast<size_t>(tinylamb::TpccTransactionType::kOrderStatus));
  const double delivery_pct =
      percent(static_cast<size_t>(tinylamb::TpccTransactionType::kDelivery));
  const double stock_level_pct = percent(
      static_cast<size_t>(tinylamb::TpccTransactionType::kStockLevel));
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
  if (!first_error.empty())
    std::cout << "first_error=\"" << first_error << "\"\n";
  return engine_aborts == 0 ? 0 : 3;
}
