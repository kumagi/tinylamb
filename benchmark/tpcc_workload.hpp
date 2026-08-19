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

struct TpccScale {
  int warehouses{1};
  int districts_per_warehouse{1};
  int customers_per_district{100};
  int items{100};
  int initial_orders_per_district{100};
  int order_lines_per_order{5};
};

struct TpccTransactionResult {
  TpccTransactionType type{TpccTransactionType::kNewOrder};
  bool committed{false};
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

// A scaled, no-think-time TPC-C workload. Every transaction is executed through
// SqlEngine, so parsing, AST conversion, optimization, execution, logging, and
// commit are included in the measured path. It is intentionally not advertised
// as an audited TPC-C/tpmC implementation.
class TpccWorkload {
 public:
  TpccWorkload(Database& database, TpccScale scale, uint64_t seed);

  static Status Initialize(Database& database, const TpccScale& scale,
                           std::string* error);

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
  int PickWarehouse();
  int PickDistrict();
  int PickCustomer();
  std::string CustomerLastName(int customer_id) const;

  Database* database_;
  TpccScale scale_;
  std::mt19937_64 random_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_BENCHMARK_TPCC_WORKLOAD_HPP
