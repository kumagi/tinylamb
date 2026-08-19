/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_BENCHMARK_TPCC_WORKLOAD_HPP
#define TINYLAMB_BENCHMARK_TPCC_WORKLOAD_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "common/constants.hpp"

namespace tinylamb {

class Database;
class TransactionContext;
struct Row;

enum class TpccTransactionType : size_t {
  kNewOrder = 0,
  kPayment,
  kOrderStatus,
  kDelivery,
  kStockLevel,
  kCount,
};

constexpr size_t kTpccTransactionTypeCount =
    static_cast<size_t>(TpccTransactionType::kCount);

std::string_view ToString(TpccTransactionType type);

// TPC-C Clause 1.3 / 4.3 population sized by scale factor W (warehouse count).
struct TpccScale {
  int warehouses{1};
  int districts_per_warehouse{10};
  int customers_per_district{3000};
  int items{100000};
  int initial_orders_per_district{3000};
  int new_orders_per_district{900};
  int min_order_lines{5};
  int max_order_lines{15};

  static TpccScale Official(int scale_factor);
  static TpccScale ForTest();

  [[nodiscard]] int ScaleFactor() const { return warehouses; }
  [[nodiscard]] int Terminals() const {
    return warehouses * districts_per_warehouse;
  }
};

// Clause 2.1.6: one C per field for every terminal. C-Load vs C-Run for
// C_LAST must satisfy |delta| in [65, 119] excluding 96 and 112.
struct TpccNurand {
  int c_last_load{0};
  int c_last_run{0};
  int c_id{0};
  int c_ol_i_id{0};
  bool valid{false};

  static TpccNurand FromSeed(uint64_t seed);
};

struct TpccTransactionResult {
  TpccTransactionType type{TpccTransactionType::kNewOrder};
  bool committed{false};
  bool user_rollback{false};
  size_t sql_statements{0};
  int warehouse_id{0};
  int district_id{0};
  int customer_id{0};
  int order_id{0};
  int delivered_orders{0};
  int64_t low_stock{0};
  double amount{0};
  std::string error;
};

std::string TpccLastName(int n);

// TPC-C transaction mix and data access (Clause 2 and 5.2). Not an audited
// tpmC run: think/keying time is omitted unless the benchmark driver adds it.
class TpccWorkload {
 public:
  // terminal_id < 0 picks a random home warehouse/district (tests).
  // Otherwise the terminal is bound like TPC-C: warehouse-major, 10 districts.
  TpccWorkload(Database& database, TpccScale scale, uint64_t seed,
               int terminal_id = -1, TpccNurand nurand = {});

  static Status Initialize(Database& database, const TpccScale& scale,
                           std::string* error, uint64_t seed = 1);

  TpccTransactionType NextTransactionType();
  TpccTransactionResult Execute(TpccTransactionType type);

 private:
  Status RunSql(TransactionContext& context, std::string_view sql,
                std::vector<Row>* rows, TpccTransactionResult* result);
  bool RequireRows(const std::vector<Row>& rows, std::string_view operation,
                   TpccTransactionResult* result) const;
  bool RequireAffected(const std::vector<Row>& rows, int64_t minimum,
                       std::string_view operation,
                       TpccTransactionResult* result) const;

  TpccTransactionResult NewOrder();
  TpccTransactionResult Payment();
  TpccTransactionResult OrderStatus();
  TpccTransactionResult Delivery();
  TpccTransactionResult StockLevel();

  int Random(int lower, int upper);
  int NURand(int a, int x, int y, int c);
  int PickCustomerId();
  int PickItemId();
  int PickLastNameNumber();
  std::string PickCustomerLastName();

  Database* database_;
  TpccScale scale_;
  std::mt19937_64 random_;
  int home_warehouse_{1};
  int home_district_{1};
  TpccNurand nurand_{};
};

}  // namespace tinylamb

#endif  // TINYLAMB_BENCHMARK_TPCC_WORKLOAD_HPP
