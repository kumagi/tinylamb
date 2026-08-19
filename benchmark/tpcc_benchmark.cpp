/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
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
  tinylamb::TpccScale scale;
  int clients{1};
  int warmup_seconds{2};
  int measure_seconds{10};
  uint64_t seed{20260819};
  bool verify_only{false};
};

struct TypeStats {
  uint64_t attempted{0};
  uint64_t committed{0};
  uint64_t statements{0};
  std::chrono::nanoseconds latency{0};
};

struct WorkerStats {
  std::array<TypeStats, tinylamb::kTpccTransactionTypeCount> types;
  std::string first_error;
};

void Usage(std::ostream& out, std::string_view program) {
  out << "usage: " << program << " <new-database-path> [options]\n"
      << "  --clients N       clients (currently must be 1)\n"
      << "  --warmup N        warmup seconds (default: 2)\n"
      << "  --seconds N       measurement seconds (default: 10)\n"
      << "  --warehouses N   (default: 1)\n"
      << "  --districts N    districts/warehouse (default: 1)\n"
      << "  --customers N    customers/district (default: 100)\n"
      << "  --items N        item count (default: 100)\n"
      << "  --initial-orders N  initial orders/district (default: 100)\n"
      << "  --order-lines N  lines/new-order (default: 5)\n"
      << "  --seed N         random seed\n"
      << "  --verify-only    run each transaction once and stop\n";
}

template <typename Integer>
bool ParseInteger(std::string_view value, Integer* destination) {
  const char* begin = value.data();
  const char* end = begin + value.size();
  auto [parsed_end, error] = std::from_chars(begin, end, *destination);
  return error == std::errc() && parsed_end == end;
}

bool ParseOptions(int argc, char** argv, Options* options) {
  if (argc < 2) return false;
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
    if (argument == "--clients") {
      parsed = ParseInteger(value, &options->clients);
    } else if (argument == "--warmup") {
      parsed = ParseInteger(value, &options->warmup_seconds);
    } else if (argument == "--seconds") {
      parsed = ParseInteger(value, &options->measure_seconds);
    } else if (argument == "--warehouses") {
      parsed = ParseInteger(value, &options->scale.warehouses);
    } else if (argument == "--districts") {
      parsed = ParseInteger(value, &options->scale.districts_per_warehouse);
    } else if (argument == "--customers") {
      parsed = ParseInteger(value, &options->scale.customers_per_district);
    } else if (argument == "--items") {
      parsed = ParseInteger(value, &options->scale.items);
    } else if (argument == "--initial-orders") {
      parsed = ParseInteger(value, &options->scale.initial_orders_per_district);
    } else if (argument == "--order-lines") {
      parsed = ParseInteger(value, &options->scale.order_lines_per_order);
    } else if (argument == "--seed") {
      parsed = ParseInteger(value, &options->seed);
    }
    if (!parsed) return false;
  }
  return options->clients > 0 && options->warmup_seconds >= 0 &&
         options->measure_seconds > 0;
}

bool VerifyTransactions(tinylamb::Database& database,
                        const tinylamb::TpccScale& scale, uint64_t seed) {
  tinylamb::TpccWorkload workload(database, scale, seed);
  constexpr std::array<tinylamb::TpccTransactionType, 5> transactions = {
      tinylamb::TpccTransactionType::kNewOrder,
      tinylamb::TpccTransactionType::kPayment,
      tinylamb::TpccTransactionType::kOrderStatus,
      tinylamb::TpccTransactionType::kDelivery,
      tinylamb::TpccTransactionType::kStockLevel};
  bool ok = true;
  for (tinylamb::TpccTransactionType type : transactions) {
    const tinylamb::TpccTransactionResult result = workload.Execute(type);
    std::cout << "verification." << tinylamb::ToString(type) << '='
              << (result.committed ? "PASS" : "FAIL")
              << " statements=" << result.sql_statements;
    if (!result.error.empty()) std::cout << " error=\"" << result.error << '"';
    std::cout << '\n';
    ok = ok && result.committed;
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
  if (options.clients != 1) {
    std::cerr << "concurrent TPC-C clients are not supported yet: tinylamb "
                 "currently terminates an index scan on row-lock conflict\n";
    return 2;
  }

  std::cout << "benchmark=tinylamb_scaled_tpcc\n"
            << "tpmc_compliant=false\n"
            << "database=" << options.database_path << '\n'
            << "warehouses=" << options.scale.warehouses << '\n'
            << "districts_per_warehouse="
            << options.scale.districts_per_warehouse << '\n'
            << "customers_per_district=" << options.scale.customers_per_district
            << '\n'
            << "items=" << options.scale.items << '\n'
            << "initial_orders_per_district="
            << options.scale.initial_orders_per_district << '\n'
            << "order_lines_per_order=" << options.scale.order_lines_per_order
            << '\n';

  tinylamb::Database database(options.database_path);
  std::string error;
  const tinylamb::Status initialized =
      tinylamb::TpccWorkload::Initialize(database, options.scale, &error);
  if (initialized != tinylamb::Status::kSuccess) {
    std::cerr << "initialization failed: " << error << " (" << initialized
              << ")\n";
    return 1;
  }
  if (!VerifyTransactions(database, options.scale, options.seed)) return 1;
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
          database, options.scale,
          options.seed + static_cast<uint64_t>(worker) + 1);
      std::this_thread::sleep_until(start);
      while (Clock::now() < measured_start) {
        workload.Execute(workload.NextTransactionType());
      }
      while (Clock::now() < measured_end) {
        const tinylamb::TpccTransactionType type =
            workload.NextTransactionType();
        const Clock::time_point before = Clock::now();
        const tinylamb::TpccTransactionResult result = workload.Execute(type);
        const auto elapsed = Clock::now() - before;
        TypeStats& stats = worker_stats[static_cast<size_t>(worker)]
                               .types[static_cast<size_t>(type)];
        ++stats.attempted;
        stats.committed += result.committed ? 1 : 0;
        stats.statements += result.sql_statements;
        stats.latency +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed);
        if (!result.committed &&
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
      totals[i].statements += worker.types[i].statements;
      totals[i].latency += worker.types[i].latency;
    }
  }
  uint64_t attempted = 0;
  uint64_t committed = 0;
  uint64_t statements = 0;
  for (size_t i = 0; i < totals.size(); ++i) {
    attempted += totals[i].attempted;
    committed += totals[i].committed;
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
              << ".average_latency_ms=" << std::fixed << std::setprecision(3)
              << average_ms << '\n';
  }
  const double seconds = static_cast<double>(options.measure_seconds);
  std::cout << "attempted_transactions=" << attempted << '\n'
            << "committed_transactions=" << committed << '\n'
            << "aborted_transactions=" << attempted - committed << '\n'
            << "executed_sql_statements=" << statements << '\n'
            << "tps=" << std::fixed << std::setprecision(3)
            << static_cast<double>(committed) / seconds << '\n'
            << "sql_qps=" << static_cast<double>(statements) / seconds << '\n';
  if (!first_error.empty())
    std::cout << "first_error=\"" << first_error << "\"\n";
  return attempted == committed ? 0 : 3;
}
