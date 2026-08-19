/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "benchmark/tpcc_workload.hpp"

#include <algorithm>
#include <array>
#include <exception>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "index/index_schema.hpp"
#include "query/sql_engine.hpp"
#include "table/table.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

constexpr std::array<std::string_view, 9> kTpccTables = {
    "warehouse", "district", "customer",  "history",   "item",
    "stock",     "orders",   "new_order", "order_line"};

std::string Number(double value) {
  std::ostringstream out;
  out << std::fixed << std::setprecision(2) << value;
  return out.str();
}

int64_t IntValue(const Value& value) {
  return value.type == ValueType::kInt64 ? value.value.int_value : 0;
}

double DoubleValue(const Value& value) {
  if (value.type == ValueType::kDouble) return value.value.double_value;
  if (value.type == ValueType::kInt64) {
    return static_cast<double>(value.value.int_value);
  }
  return 0;
}

Status ExecuteSql(Database& database, TransactionContext& context,
                  std::string_view sql, std::string* error) {
  SqlEngine engine(database);
  StatusOr<Executor> prepared = engine.Prepare(context, sql);
  if (!prepared.HasValue()) {
    if (error != nullptr) {
      *error = engine.LastError().empty()
                   ? std::string(ToString(prepared.GetStatus()))
                   : engine.LastError();
    }
    return prepared.GetStatus();
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
  }
  return Status::kSuccess;
}

Status CreateIndex(Database& database, TransactionContext& context,
                   std::string_view table, std::string_view name,
                   std::vector<slot_t> keys,
                   IndexMode mode = IndexMode::kUnique) {
  return database.CreateIndex(context, table,
                              IndexSchema(name, std::move(keys), {}, mode));
}

constexpr std::array<std::string_view, 10> kLastNameSyllables = {
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION",
    "EING"};

int ComputeNURand(int a, int x, int y, int c, int rand_a, int rand_xy) {
  (void)a;
  return (((rand_a | rand_xy) + c) % (y - x + 1)) + x;
}

std::string SqlLiteral(std::string_view text) {
  std::string out;
  out.reserve(text.size() + 2);
  out.push_back('\'');
  for (char ch : text) {
    if (ch == '\'') out.push_back('\'');
    out.push_back(ch);
  }
  out.push_back('\'');
  return out;
}

}  // namespace

TpccNurand TpccNurand::FromSeed(uint64_t seed) {
  std::mt19937_64 rng(seed);
  auto pick = [&](int lower, int upper) {
    return std::uniform_int_distribution<int>(lower, upper)(rng);
  };
  TpccNurand nurand;
  nurand.c_id = pick(0, 1023);
  nurand.c_ol_i_id = pick(0, 8191);
  nurand.c_last_load = pick(0, 255);
  std::vector<int> deltas;
  deltas.reserve(53);
  for (int delta = 65; delta <= 119; ++delta) {
    if (delta == 96 || delta == 112) continue;
    deltas.push_back(delta);
  }
  for (;;) {
    const int delta = deltas[static_cast<size_t>(
        pick(0, static_cast<int>(deltas.size()) - 1))];
    const int sign = pick(0, 1) == 0 ? -1 : 1;
    const int run = nurand.c_last_load + sign * delta;
    if (run >= 0 && run <= 255) {
      nurand.c_last_run = run;
      nurand.valid = true;
      return nurand;
    }
  }
}

TpccScale TpccScale::Official(int scale_factor) {
  TpccScale scale;
  scale.warehouses = std::max(1, scale_factor);
  scale.districts_per_warehouse = 10;
  scale.customers_per_district = 3000;
  scale.items = 100000;
  scale.initial_orders_per_district = 3000;
  scale.new_orders_per_district = 900;
  scale.min_order_lines = 5;
  scale.max_order_lines = 15;
  return scale;
}

TpccScale TpccScale::ForTest() {
  TpccScale scale;
  scale.warehouses = 1;
  scale.districts_per_warehouse = 1;
  scale.customers_per_district = 10;
  scale.items = 20;
  scale.initial_orders_per_district = 10;
  scale.new_orders_per_district = 3;
  scale.min_order_lines = 3;
  scale.max_order_lines = 3;
  return scale;
}

std::string TpccLastName(int n) {
  n = std::clamp(n, 0, 999);
  std::string name;
  name += kLastNameSyllables[static_cast<size_t>(n / 100)];
  name += kLastNameSyllables[static_cast<size_t>((n / 10) % 10)];
  name += kLastNameSyllables[static_cast<size_t>(n % 10)];
  return name;
}

std::string_view ToString(TpccTransactionType type) {
  switch (type) {
    case TpccTransactionType::kNewOrder:
      return "new_order";
    case TpccTransactionType::kPayment:
      return "payment";
    case TpccTransactionType::kOrderStatus:
      return "order_status";
    case TpccTransactionType::kDelivery:
      return "delivery";
    case TpccTransactionType::kStockLevel:
      return "stock_level";
    case TpccTransactionType::kCount:
      break;
  }
  return "unknown";
}

TpccWorkload::TpccWorkload(Database& database, TpccScale scale, uint64_t seed,
                           int terminal_id, TpccNurand nurand)
    : database_(&database),
      scale_(scale),
      random_(seed),
      nurand_(nurand.valid ? nurand : TpccNurand::FromSeed(seed)) {
  if (terminal_id >= 0) {
    const int terminals = std::max(1, scale_.Terminals());
    const int id = terminal_id % terminals;
    home_warehouse_ = id / scale_.districts_per_warehouse + 1;
    home_district_ = id % scale_.districts_per_warehouse + 1;
  } else {
    home_warehouse_ = Random(1, scale_.warehouses);
    home_district_ = Random(1, scale_.districts_per_warehouse);
  }
}

Status TpccWorkload::Initialize(Database& database, const TpccScale& scale,
                                std::string* error, uint64_t seed) {
  if (scale.warehouses < 1 || scale.districts_per_warehouse < 1 ||
      scale.customers_per_district < 1 || scale.items < 1 ||
      scale.initial_orders_per_district < 1 ||
      scale.new_orders_per_district < 1 || scale.min_order_lines < 1 ||
      scale.max_order_lines < scale.min_order_lines) {
    if (error != nullptr) *error = "invalid TPC-C scale values";
    return Status::kUnknown;
  }
  if (scale.initial_orders_per_district < scale.customers_per_district) {
    if (error != nullptr) {
      *error = "initial orders must cover every customer for Order-Status";
    }
    return Status::kUnknown;
  }
  if (scale.new_orders_per_district > scale.initial_orders_per_district) {
    if (error != nullptr) {
      *error = "new-order rows cannot exceed initial orders per district";
    }
    return Status::kUnknown;
  }

  TransactionContext schema_context = database.BeginContext();
  constexpr std::array<std::string_view, 9> schema = {
      "CREATE TABLE warehouse (w_id INT64, w_ytd NUMERIC, w_tax NUMERIC, "
      "w_name STRING);",
      "CREATE TABLE district (d_w_id INT64, d_id INT64, d_ytd NUMERIC, "
      "d_tax NUMERIC, d_next_o_id INT64, d_name STRING);",
      "CREATE TABLE customer (c_w_id INT64, c_d_id INT64, c_id INT64, "
      "c_first STRING, c_middle STRING, c_last STRING, c_credit STRING, "
      "c_discount NUMERIC, c_balance NUMERIC, c_ytd_payment NUMERIC, "
      "c_payment_cnt INT64, c_delivery_cnt INT64, c_data STRING);",
      "CREATE TABLE history (h_c_w_id INT64, h_c_d_id INT64, h_c_id INT64, "
      "h_w_id INT64, h_d_id INT64, h_date STRING, h_amount NUMERIC, "
      "h_data STRING);",
      "CREATE TABLE item (i_id INT64, i_name STRING, i_price NUMERIC, "
      "i_data STRING);",
      "CREATE TABLE stock (s_w_id INT64, s_i_id INT64, s_quantity INT64, "
      "s_ytd NUMERIC, s_order_cnt INT64, s_remote_cnt INT64, s_data STRING);",
      "CREATE TABLE orders (o_w_id INT64, o_d_id INT64, o_id INT64, "
      "o_c_id INT64, o_entry_d STRING, o_carrier_id INT64, o_ol_cnt INT64, "
      "o_all_local INT64);",
      "CREATE TABLE new_order (no_w_id INT64, no_d_id INT64, no_o_id INT64);",
      "CREATE TABLE order_line (ol_w_id INT64, ol_d_id INT64, ol_o_id INT64, "
      "ol_number INT64, ol_i_id INT64, ol_supply_w_id INT64, "
      "ol_delivery_d STRING, ol_quantity INT64, ol_amount NUMERIC);"};
  for (std::string_view statement : schema) {
    Status status = ExecuteSql(database, schema_context, statement, error);
    if (status != Status::kSuccess) {
      schema_context.Abort();
      return status;
    }
  }

  const std::array<Status, 11> index_statuses = {
      CreateIndex(database, schema_context, "warehouse", "warehouse_pk", {0}),
      CreateIndex(database, schema_context, "district", "district_pk", {0, 1}),
      CreateIndex(database, schema_context, "customer", "customer_pk",
                  {0, 1, 2}),
      CreateIndex(database, schema_context, "customer", "customer_name_idx",
                  {0, 1, 5, 3}, IndexMode::kNonUnique),
      CreateIndex(database, schema_context, "item", "item_pk", {0}),
      CreateIndex(database, schema_context, "stock", "stock_pk", {0, 1}),
      CreateIndex(database, schema_context, "orders", "orders_pk", {0, 1, 2}),
      CreateIndex(database, schema_context, "orders", "orders_customer_idx",
                  {0, 1, 3, 2}, IndexMode::kNonUnique),
      CreateIndex(database, schema_context, "new_order", "new_order_pk",
                  {0, 1, 2}),
      CreateIndex(database, schema_context, "order_line", "order_line_pk",
                  {0, 1, 2, 3}),
      CreateIndex(database, schema_context, "order_line", "order_line_item_idx",
                  {0, 1, 4, 2}, IndexMode::kNonUnique)};
  for (Status status : index_statuses) {
    if (status != Status::kSuccess) {
      if (error != nullptr) *error = "failed to create TPC-C index";
      schema_context.Abort();
      return status;
    }
  }
  Status status = schema_context.PreCommit();
  if (status != Status::kSuccess) {
    if (error != nullptr) *error = "failed to commit TPC-C schema";
    return status;
  }

  TransactionContext load_context = database.BeginContext();
  std::array<std::shared_ptr<Table>, kTpccTables.size()> tables;
  for (size_t i = 0; i < kTpccTables.size(); ++i) {
    StatusOr<std::shared_ptr<Table>> table =
        load_context.GetTable(kTpccTables[i]);
    if (!table.HasValue()) {
      if (error != nullptr) *error = "failed to open TPC-C table";
      load_context.Abort();
      return table.GetStatus();
    }
    tables[i] = table.Value();
  }
  auto insert = [&](size_t table, Row row) {
    return tables[table]->Insert(load_context.txn_, row).GetStatus();
  };
  std::mt19937_64 rng(seed);
  auto load_random = [&](int lower, int upper) {
    return std::uniform_int_distribution<int>(lower, upper)(rng);
  };
  const TpccNurand nurand = TpccNurand::FromSeed(seed);

  for (int item_id = 1; item_id <= scale.items; ++item_id) {
    const bool original = load_random(1, 10) == 1;
    status =
        insert(4, Row({Value(item_id), Value("Item#" + std::to_string(item_id)),
                       Value(static_cast<double>(load_random(100, 10000)) /
                             100.0),
                       Value(original ? "ORIGINAL" : "DATA")}));
    if (status != Status::kSuccess) break;
  }
  for (int warehouse_id = 1;
       status == Status::kSuccess && warehouse_id <= scale.warehouses;
       ++warehouse_id) {
    status =
        insert(0, Row({Value(warehouse_id), Value(300000.0),
                       Value(static_cast<double>(load_random(0, 2000)) / 10000.0),
                       Value("Warehouse#" + std::to_string(warehouse_id))}));
    for (int item_id = 1; status == Status::kSuccess && item_id <= scale.items;
         ++item_id) {
      const bool original = load_random(1, 10) == 1;
      status = insert(
          5, Row({Value(warehouse_id), Value(item_id),
                  Value(load_random(10, 100)), Value(0.0), Value(0), Value(0),
                  Value(original ? "ORIGINAL" : "DATA")}));
    }
    for (int district_id = 1; status == Status::kSuccess &&
                              district_id <= scale.districts_per_warehouse;
         ++district_id) {
      status = insert(
          1, Row({Value(warehouse_id), Value(district_id), Value(30000.0),
                  Value(static_cast<double>(load_random(0, 2000)) / 10000.0),
                  Value(scale.initial_orders_per_district + 1),
                  Value("District#" + std::to_string(district_id))}));
      for (int customer_id = 1; status == Status::kSuccess &&
                                customer_id <= scale.customers_per_district;
           ++customer_id) {
        const int last_name_n =
            customer_id <= 1000
                ? customer_id - 1
                : ComputeNURand(255, 0, 999, nurand.c_last_load,
                                load_random(0, 255), load_random(0, 999));
        status = insert(
            2, Row({Value(warehouse_id), Value(district_id), Value(customer_id),
                    Value("First#" + std::to_string(customer_id)), Value("OE"),
                    Value(TpccLastName(last_name_n)),
                    Value(load_random(1, 10) == 1 ? "BC" : "GC"),
                    Value(static_cast<double>(load_random(0, 5000)) / 10000.0),
                    Value(-10.0), Value(10.0), Value(1), Value(0),
                    Value("customer data")}));
        if (status == Status::kSuccess) {
          status = insert(
              3, Row({Value(warehouse_id), Value(district_id),
                      Value(customer_id), Value(warehouse_id),
                      Value(district_id), Value("2026-01-01 00:00:00"),
                      Value(10.0), Value("history")}));
        }
      }
      std::vector<int> customer_permutation(
          static_cast<size_t>(scale.customers_per_district));
      std::iota(customer_permutation.begin(), customer_permutation.end(), 1);
      std::shuffle(customer_permutation.begin(), customer_permutation.end(),
                   rng);
      const int delivered_orders =
          scale.initial_orders_per_district - scale.new_orders_per_district;
      for (int order_id = 1; status == Status::kSuccess &&
                             order_id <= scale.initial_orders_per_district;
           ++order_id) {
        const int customer_id =
            customer_permutation[static_cast<size_t>(
                (order_id - 1) % scale.customers_per_district)];
        const int line_count =
            load_random(scale.min_order_lines, scale.max_order_lines);
        const bool queued = order_id > delivered_orders;
        status = insert(
            6, Row({Value(warehouse_id), Value(district_id), Value(order_id),
                    Value(customer_id), Value("2026-01-01 00:00:00"),
                    queued ? Value() : Value(load_random(1, 10)),
                    Value(line_count), Value(1)}));
        if (status == Status::kSuccess && queued) {
          status = insert(7, Row({Value(warehouse_id), Value(district_id),
                                  Value(order_id)}));
        }
        for (int line = 1; status == Status::kSuccess && line <= line_count;
             ++line) {
          const int item_id = load_random(1, scale.items);
          status = insert(8, Row({Value(warehouse_id), Value(district_id),
                                  Value(order_id), Value(line), Value(item_id),
                                  Value(warehouse_id),
                                  queued ? Value()
                                         : Value("2026-01-01 00:00:00"),
                                  Value(5), Value(5.0)}));
        }
      }
    }
  }
  if (status != Status::kSuccess) {
    if (error != nullptr) *error = "failed to load TPC-C fixture";
    load_context.Abort();
    return status;
  }
  status = load_context.PreCommit();
  if (status != Status::kSuccess) {
    if (error != nullptr) *error = "failed to commit TPC-C fixture";
    return status;
  }

  const bool skip_stats =
      scale.customers_per_district >= 1000 || scale.items >= 10000;
  if (skip_stats) {
    std::cerr << "warning: skipping TPC-C statistics refresh at SF="
              << scale.ScaleFactor()
              << " (serialized histograms exceed a 32KiB leaf)\n";
    return Status::kSuccess;
  }

  TransactionContext stats_context = database.BeginContext();
  for (std::string_view table : kTpccTables) {
    status = database.RefreshStatistics(stats_context, table);
    if (status != Status::kSuccess) {
      if (error != nullptr) {
        *error = "failed to refresh TPC-C statistics for " + std::string(table);
      }
      stats_context.Abort();
      return status;
    }
  }
  return stats_context.PreCommit();
}

int TpccWorkload::Random(int lower, int upper) {
  return std::uniform_int_distribution<int>(lower, upper)(random_);
}

int TpccWorkload::NURand(int a, int x, int y, int c) {
  return ComputeNURand(a, x, y, c, Random(0, a), Random(x, y));
}

int TpccWorkload::PickCustomerId() {
  return NURand(1023, 1, scale_.customers_per_district, nurand_.c_id);
}

int TpccWorkload::PickItemId() {
  return NURand(8191, 1, scale_.items, nurand_.c_ol_i_id);
}

int TpccWorkload::PickLastNameNumber() {
  const int max_name = std::min(999, scale_.customers_per_district - 1);
  return NURand(255, 0, max_name, nurand_.c_last_run);
}

std::string TpccWorkload::PickCustomerLastName() {
  return TpccLastName(PickLastNameNumber());
}

TpccTransactionType TpccWorkload::NextTransactionType() {
  const int choice = Random(1, 100);
  if (choice <= 45) return TpccTransactionType::kNewOrder;
  if (choice <= 88) return TpccTransactionType::kPayment;
  if (choice <= 92) return TpccTransactionType::kOrderStatus;
  if (choice <= 96) return TpccTransactionType::kDelivery;
  return TpccTransactionType::kStockLevel;
}

Status TpccWorkload::RunSql(TransactionContext& context, std::string_view sql,
                            std::vector<Row>* rows,
                            TpccTransactionResult* result) {
  SqlEngine engine(*database_);
  StatusOr<Executor> prepared = engine.Prepare(context, sql);
  if (!prepared.HasValue()) {
    result->error = std::string(ToString(result->type)) + ": " +
                    (engine.LastError().empty()
                         ? std::string(ToString(prepared.GetStatus()))
                         : engine.LastError()) +
                    " [" + std::string(sql) + "]";
    return prepared.GetStatus();
  }
  ++result->sql_statements;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) rows->push_back(row);
  return Status::kSuccess;
}

bool TpccWorkload::RequireRows(const std::vector<Row>& rows,
                               std::string_view operation,
                               TpccTransactionResult* result) const {
  if (!rows.empty() && rows.front().IsValid()) return true;
  result->error = std::string(operation) + " returned no rows";
  return false;
}

bool TpccWorkload::RequireAffected(const std::vector<Row>& rows,
                                   int64_t minimum, std::string_view operation,
                                   TpccTransactionResult* result) const {
  if (RequireRows(rows, operation, result) &&
      rows.front().values_.size() >= 2 &&
      IntValue(rows.front()[1]) >= minimum) {
    return true;
  }
  result->error = std::string(operation) + " affected too few rows";
  return false;
}

TpccTransactionResult TpccWorkload::Execute(TpccTransactionType type) {
  switch (type) {
    case TpccTransactionType::kNewOrder:
      return NewOrder();
    case TpccTransactionType::kPayment:
      return Payment();
    case TpccTransactionType::kOrderStatus:
      return OrderStatus();
    case TpccTransactionType::kDelivery:
      return Delivery();
    case TpccTransactionType::kStockLevel:
      return StockLevel();
    case TpccTransactionType::kCount:
      break;
  }
  TpccTransactionResult result;
  result.type = type;
  result.error = "invalid TPC-C transaction type";
  return result;
}

TpccTransactionResult TpccWorkload::NewOrder() {
  TpccTransactionResult result;
  result.type = TpccTransactionType::kNewOrder;
  result.warehouse_id = home_warehouse_;
  result.district_id = home_district_;
  result.customer_id = PickCustomerId();
  const int line_count =
      Random(scale_.min_order_lines, scale_.max_order_lines);
  const bool rollback = Random(1, 100) == 1;
  std::vector<int> item_ids(static_cast<size_t>(line_count));
  std::vector<int> supply_warehouses(static_cast<size_t>(line_count),
                                     result.warehouse_id);
  std::vector<int> quantities(static_cast<size_t>(line_count));
  int all_local = 1;
  for (int line = 0; line < line_count; ++line) {
    item_ids[static_cast<size_t>(line)] = PickItemId();
    quantities[static_cast<size_t>(line)] = Random(1, 10);
    if (scale_.warehouses > 1 && Random(1, 100) == 1) {
      int remote = result.warehouse_id;
      while (remote == result.warehouse_id) remote = Random(1, scale_.warehouses);
      supply_warehouses[static_cast<size_t>(line)] = remote;
      all_local = 0;
    }
  }
  if (rollback) item_ids.back() = scale_.items + 1;

  TransactionContext context = database_->BeginContext();
  auto fail = [&]() {
    context.Abort();
    return result;
  };
  std::vector<Row> rows;
  std::ostringstream sql;
  sql << "SELECT w_tax FROM warehouse WHERE w_id = " << result.warehouse_id
      << ';';
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireRows(rows, "new-order warehouse read", &result)) {
    return fail();
  }
  rows.clear();
  sql.str("");
  sql << "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = "
      << result.warehouse_id << " AND d_id = " << result.district_id << ';';
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireRows(rows, "new-order district read", &result)) {
    return fail();
  }
  result.order_id = static_cast<int>(IntValue(rows.front()[1]));
  rows.clear();
  sql.str("");
  sql << "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = "
      << result.warehouse_id << " AND d_id = " << result.district_id << ';';
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireAffected(rows, 1, "new-order district update", &result)) {
    return fail();
  }
  rows.clear();
  sql.str("");
  sql << "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = "
      << result.warehouse_id << " AND c_d_id = " << result.district_id
      << " AND c_id = " << result.customer_id << ';';
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireRows(rows, "new-order customer read", &result)) {
    return fail();
  }

  rows.clear();
  sql.str("");
  sql << "INSERT INTO orders VALUES (" << result.warehouse_id << ','
      << result.district_id << ',' << result.order_id << ','
      << result.customer_id << ",'2026-08-19 00:00:00',NULL," << line_count
      << ',' << all_local << ");";
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireAffected(rows, 1, "new-order order insert", &result)) {
    return fail();
  }
  rows.clear();
  sql.str("");
  sql << "INSERT INTO new_order VALUES (" << result.warehouse_id << ','
      << result.district_id << ',' << result.order_id << ");";
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireAffected(rows, 1, "new-order queue insert", &result)) {
    return fail();
  }

  for (int line = 0; line < line_count; ++line) {
    const int item_id = item_ids[static_cast<size_t>(line)];
    const int quantity = quantities[static_cast<size_t>(line)];
    const int supply_warehouse = supply_warehouses[static_cast<size_t>(line)];
    rows.clear();
    sql.str("");
    sql << "SELECT i_price, i_name, i_data FROM item WHERE i_id = " << item_id
        << ';';
    if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
        !RequireRows(rows, "new-order item read", &result)) {
      if (rollback && line == line_count - 1) {
        result.user_rollback = true;
        result.error = "new-order unused item rollback";
      }
      return fail();
    }
    const double price = DoubleValue(rows.front()[0]);
    rows.clear();
    sql.str("");
    sql << "SELECT s_quantity, s_data FROM stock WHERE s_w_id = "
        << supply_warehouse << " AND s_i_id = " << item_id << ';';
    if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
        !RequireRows(rows, "new-order stock read", &result)) {
      return fail();
    }
    rows.clear();
    sql.str("");
    sql << "UPDATE stock SET s_quantity = CASE WHEN s_quantity >= "
        << quantity + 10 << " THEN s_quantity - " << quantity
        << " ELSE s_quantity + 91 - " << quantity << " END, s_ytd = s_ytd + "
        << Number(static_cast<double>(quantity))
        << ", s_order_cnt = s_order_cnt + 1, s_remote_cnt = "
        << "s_remote_cnt + "
        << (supply_warehouse == result.warehouse_id ? 0 : 1)
        << " WHERE s_w_id = " << supply_warehouse << " AND s_i_id = " << item_id
        << ';';
    if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
        !RequireAffected(rows, 1, "new-order stock update", &result)) {
      return fail();
    }
    const double line_amount = price * quantity;
    result.amount += line_amount;
    rows.clear();
    sql.str("");
    sql << "INSERT INTO order_line VALUES (" << result.warehouse_id << ','
        << result.district_id << ',' << result.order_id << ',' << (line + 1)
        << ',' << item_id << ',' << supply_warehouse << ",NULL," << quantity
        << ',' << Number(line_amount) << ");";
    if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
        !RequireAffected(rows, 1, "new-order line insert", &result)) {
      return fail();
    }
  }
  const Status commit = context.PreCommit();
  result.committed = commit == Status::kSuccess;
  if (!result.committed) result.error = "new-order commit failed";
  return result;
}

TpccTransactionResult TpccWorkload::Payment() {
  TpccTransactionResult result;
  result.type = TpccTransactionType::kPayment;
  result.warehouse_id = home_warehouse_;
  result.district_id = home_district_;
  int customer_warehouse = home_warehouse_;
  int customer_district = home_district_;
  if (scale_.warehouses > 1 && Random(1, 100) <= 15) {
    do {
      customer_warehouse = Random(1, scale_.warehouses);
    } while (customer_warehouse == home_warehouse_);
    customer_district = Random(1, scale_.districts_per_warehouse);
  }
  result.customer_id = PickCustomerId();
  result.amount = static_cast<double>(Random(100, 500000)) / 100.0;
  TransactionContext context = database_->BeginContext();
  auto fail = [&]() {
    context.Abort();
    return result;
  };
  std::vector<Row> rows;
  std::ostringstream sql;
  sql << "UPDATE warehouse SET w_ytd = w_ytd + " << Number(result.amount)
      << " WHERE w_id = " << result.warehouse_id << ';';
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireAffected(rows, 1, "payment warehouse update", &result)) {
    return fail();
  }
  rows.clear();
  sql.str("");
  sql << "SELECT w_name, w_ytd FROM warehouse WHERE w_id = "
      << result.warehouse_id << ';';
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireRows(rows, "payment warehouse read", &result)) {
    return fail();
  }
  rows.clear();
  sql.str("");
  sql << "UPDATE district SET d_ytd = d_ytd + " << Number(result.amount)
      << " WHERE d_w_id = " << result.warehouse_id
      << " AND d_id = " << result.district_id << ';';
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireAffected(rows, 1, "payment district update", &result)) {
    return fail();
  }
  rows.clear();
  sql.str("");
  sql << "SELECT d_name, d_ytd FROM district WHERE d_w_id = "
      << result.warehouse_id << " AND d_id = " << result.district_id << ';';
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireRows(rows, "payment district read", &result)) {
    return fail();
  }
  rows.clear();
  sql.str("");
  if (Random(1, 100) <= 60) {
    const std::string last_name = PickCustomerLastName();
    sql << "SELECT c_id, c_first, c_credit, c_balance, c_data FROM customer "
        << "WHERE c_w_id = " << customer_warehouse
        << " AND c_d_id = " << customer_district << " AND c_last = '"
        << last_name << "' ORDER BY c_first;";
    if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
        !RequireRows(rows, "payment customer-name read", &result)) {
      return fail();
    }
    const Row& chosen = rows[(rows.size() - 1) / 2];
    result.customer_id = static_cast<int>(IntValue(chosen[0]));
    const std::string credit = chosen[2].AsString();
    std::string customer_data = chosen[4].AsString();
    rows.clear();
    sql.str("");
    sql << "UPDATE customer SET c_balance = c_balance - "
        << Number(result.amount) << ", c_ytd_payment = c_ytd_payment + "
        << Number(result.amount) << ", c_payment_cnt = c_payment_cnt + 1";
    if (credit == "BC") {
      std::ostringstream data;
      data << result.customer_id << ',' << customer_district << ','
           << customer_warehouse << ',' << result.district_id << ','
           << result.warehouse_id << ',' << Number(result.amount) << ','
           << customer_data;
      std::string next = data.str();
      if (next.size() > 500) next.resize(500);
      sql << ", c_data = " << SqlLiteral(next);
    }
    sql << " WHERE c_w_id = " << customer_warehouse
        << " AND c_d_id = " << customer_district
        << " AND c_id = " << result.customer_id << ';';
  } else {
    sql << "SELECT c_id, c_first, c_credit, c_balance, c_data FROM customer "
        << "WHERE c_w_id = " << customer_warehouse
        << " AND c_d_id = " << customer_district
        << " AND c_id = " << result.customer_id << ';';
    if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
        !RequireRows(rows, "payment customer read", &result)) {
      return fail();
    }
    const std::string credit = rows.front()[2].AsString();
    std::string customer_data = rows.front()[4].AsString();
    rows.clear();
    sql.str("");
    sql << "UPDATE customer SET c_balance = c_balance - "
        << Number(result.amount) << ", c_ytd_payment = c_ytd_payment + "
        << Number(result.amount) << ", c_payment_cnt = c_payment_cnt + 1";
    if (credit == "BC") {
      std::ostringstream data;
      data << result.customer_id << ',' << customer_district << ','
           << customer_warehouse << ',' << result.district_id << ','
           << result.warehouse_id << ',' << Number(result.amount) << ','
           << customer_data;
      std::string next = data.str();
      if (next.size() > 500) next.resize(500);
      sql << ", c_data = " << SqlLiteral(next);
    }
    sql << " WHERE c_w_id = " << customer_warehouse
        << " AND c_d_id = " << customer_district
        << " AND c_id = " << result.customer_id << ';';
  }
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireAffected(rows, 1, "payment customer update", &result)) {
    return fail();
  }
  rows.clear();
  sql.str("");
  sql << "INSERT INTO history VALUES (" << customer_warehouse << ','
      << customer_district << ',' << result.customer_id << ','
      << result.warehouse_id << ',' << result.district_id
      << ",'2026-08-19 00:00:00'," << Number(result.amount) << ",'payment');";
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireAffected(rows, 1, "payment history insert", &result)) {
    return fail();
  }
  const Status commit = context.PreCommit();
  result.committed = commit == Status::kSuccess;
  if (!result.committed) result.error = "payment commit failed";
  return result;
}

TpccTransactionResult TpccWorkload::OrderStatus() {
  TpccTransactionResult result;
  result.type = TpccTransactionType::kOrderStatus;
  result.warehouse_id = home_warehouse_;
  result.district_id = home_district_;
  result.customer_id = PickCustomerId();
  TransactionContext context = database_->BeginContext();
  auto fail = [&]() {
    context.Abort();
    return result;
  };
  std::vector<Row> rows;
  std::ostringstream sql;
  if (Random(1, 100) <= 60) {
    sql << "SELECT c_id, c_first, c_balance FROM customer WHERE c_w_id = "
        << result.warehouse_id << " AND c_d_id = " << result.district_id
        << " AND c_last = '" << PickCustomerLastName()
        << "' ORDER BY c_first;";
    if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
        !RequireRows(rows, "order-status customer-name read", &result)) {
      return fail();
    }
    result.customer_id =
        static_cast<int>(IntValue(rows[(rows.size() - 1) / 2][0]));
  } else {
    sql << "SELECT c_id, c_first, c_balance FROM customer WHERE c_w_id = "
        << result.warehouse_id << " AND c_d_id = " << result.district_id
        << " AND c_id = " << result.customer_id << ';';
    if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
        !RequireRows(rows, "order-status customer read", &result)) {
      return fail();
    }
  }
  rows.clear();
  sql.str("");
  sql << "SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = "
      << result.warehouse_id << " AND o_d_id = " << result.district_id
      << " AND o_c_id = " << result.customer_id
      << " ORDER BY o_id DESC LIMIT 1;";
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireRows(rows, "order-status order read", &result)) {
    return fail();
  }
  result.order_id = static_cast<int>(IntValue(rows.front()[0]));
  rows.clear();
  sql.str("");
  sql << "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, "
      << "ol_delivery_d FROM order_line WHERE ol_w_id = " << result.warehouse_id
      << " AND ol_d_id = " << result.district_id
      << " AND ol_o_id = " << result.order_id << " ORDER BY ol_number;";
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireRows(rows, "order-status line read", &result)) {
    return fail();
  }
  const Status commit = context.PreCommit();
  result.committed = commit == Status::kSuccess;
  if (!result.committed) result.error = "order-status commit failed";
  return result;
}

TpccTransactionResult TpccWorkload::Delivery() {
  TpccTransactionResult result;
  result.type = TpccTransactionType::kDelivery;
  result.warehouse_id = home_warehouse_;
  const int carrier_id = Random(1, 10);
  TransactionContext context = database_->BeginContext();
  auto fail = [&]() {
    context.Abort();
    return result;
  };
  std::vector<Row> rows;
  std::ostringstream sql;
  for (int district_id = 1; district_id <= scale_.districts_per_warehouse;
       ++district_id) {
    rows.clear();
    sql.str("");
    sql << "SELECT no_o_id FROM new_order WHERE no_w_id = "
        << result.warehouse_id << " AND no_d_id = " << district_id
        << " ORDER BY no_o_id LIMIT 1;";
    if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess) {
      return fail();
    }
    if (rows.empty()) continue;
    const int order_id = static_cast<int>(IntValue(rows.front()[0]));
    rows.clear();
    sql.str("");
    sql << "DELETE FROM new_order WHERE no_w_id = " << result.warehouse_id
        << " AND no_d_id = " << district_id << " AND no_o_id = " << order_id
        << ';';
    if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
        !RequireAffected(rows, 1, "delivery queue delete", &result)) {
      return fail();
    }
    if (IntValue(rows.front()[1]) != 1) {
      result.error = "delivery queue delete affected " +
                     std::to_string(IntValue(rows.front()[1])) + " rows";
      return fail();
    }
    rows.clear();
    sql.str("");
    sql << "SELECT o_c_id FROM orders WHERE o_w_id = " << result.warehouse_id
        << " AND o_d_id = " << district_id << " AND o_id = " << order_id << ';';
    if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
        !RequireRows(rows, "delivery order read", &result)) {
      return fail();
    }
    const int customer_id = static_cast<int>(IntValue(rows.front()[0]));
    rows.clear();
    sql.str("");
    sql << "UPDATE orders SET o_carrier_id = " << carrier_id
        << " WHERE o_w_id = " << result.warehouse_id
        << " AND o_d_id = " << district_id << " AND o_id = " << order_id << ';';
    if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
        !RequireAffected(rows, 1, "delivery order update", &result)) {
      return fail();
    }
    rows.clear();
    sql.str("");
    sql << "UPDATE order_line SET ol_delivery_d = CURRENT_TIMESTAMP() "
        << "WHERE ol_w_id = " << result.warehouse_id
        << " AND ol_d_id = " << district_id << " AND ol_o_id = " << order_id
        << ';';
    if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
        !RequireAffected(rows, 1, "delivery line update", &result)) {
      return fail();
    }
    rows.clear();
    sql.str("");
    sql << "SELECT SUM(ol_amount) FROM order_line WHERE ol_w_id = "
        << result.warehouse_id << " AND ol_d_id = " << district_id
        << " AND ol_o_id = " << order_id << ';';
    if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
        !RequireRows(rows, "delivery amount read", &result)) {
      return fail();
    }
    const double amount = DoubleValue(rows.front()[0]);
    rows.clear();
    sql.str("");
    sql << "UPDATE customer SET c_balance = c_balance + " << Number(amount)
        << ", c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = "
        << result.warehouse_id << " AND c_d_id = " << district_id
        << " AND c_id = " << customer_id << ';';
    if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
        !RequireAffected(rows, 1, "delivery customer update", &result)) {
      return fail();
    }
    result.amount += amount;
    ++result.delivered_orders;
  }
  const Status commit = context.PreCommit();
  result.committed = commit == Status::kSuccess;
  if (!result.committed) result.error = "delivery commit failed";
  return result;
}

TpccTransactionResult TpccWorkload::StockLevel() {
  TpccTransactionResult result;
  result.type = TpccTransactionType::kStockLevel;
  result.warehouse_id = home_warehouse_;
  result.district_id = home_district_;
  const int threshold = Random(10, 20);
  TransactionContext context = database_->BeginContext();
  auto fail = [&]() {
    context.Abort();
    return result;
  };
  std::vector<Row> rows;
  std::ostringstream sql;
  sql << "SELECT d_next_o_id FROM district WHERE d_w_id = "
      << result.warehouse_id << " AND d_id = " << result.district_id << ';';
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireRows(rows, "stock-level district read", &result)) {
    return fail();
  }
  const int next_order = static_cast<int>(IntValue(rows.front()[0]));
  rows.clear();
  sql.str("");
  sql << "SELECT COUNT(DISTINCT s_i_id) FROM stock WHERE s_w_id = "
      << result.warehouse_id << " AND s_quantity < " << threshold
      << " AND s_i_id IN (SELECT ol_i_id FROM order_line WHERE ol_w_id = "
      << result.warehouse_id << " AND ol_d_id = " << result.district_id
      << " AND ol_o_id >= " << std::max(1, next_order - 20) << " AND ol_o_id < "
      << next_order << ");";
  if (RunSql(context, sql.str(), &rows, &result) != Status::kSuccess ||
      !RequireRows(rows, "stock-level count", &result)) {
    return fail();
  }
  result.low_stock = IntValue(rows.front()[0]);
  const Status commit = context.PreCommit();
  result.committed = commit == Status::kSuccess;
  if (!result.committed) result.error = "stock-level commit failed";
  return result;
}

}  // namespace tinylamb
