/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "executor/executor_base.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

class SqlEngineTpccTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = "sql_engine_tpcc_test-" + RandomString();
    database_ = std::make_unique<Database>(path_);
  }

  void TearDown() override { database_->DeleteAll(); }

  std::vector<Row> Run(TransactionContext& context, const std::string& sql) {
    SqlEngine engine(*database_);
    StatusOr<Executor> result = engine.Prepare(context, sql);
    EXPECT_EQ(result.GetStatus(), Status::kSuccess) << sql << "\n"
                                                    << engine.LastError();
    if (!result.HasValue()) { return {};
}
    std::vector<Row> rows;
    Row row;
    while (result.Value()->Next(&row, nullptr)) { rows.push_back(row);
}
    return rows;
  }

  void CreateSchema(TransactionContext& context) {
    Run(context,
        "CREATE TABLE warehouse (w_id INT64, w_tax NUMERIC, "
        "w_ytd NUMERIC, w_name STRING(16));");
    Run(context,
        "CREATE TABLE district (d_id INT64, d_w_id INT64, "
        "d_tax NUMERIC, d_ytd NUMERIC, d_next_o_id INT64);");
    Run(context,
        "CREATE TABLE customer (c_id INT64, c_d_id INT64, "
        "c_w_id INT64, c_first STRING(16), c_last STRING(16), "
        "c_credit STRING(2), c_discount NUMERIC, c_balance NUMERIC, "
        "c_ytd_payment NUMERIC, c_payment_cnt INT64, "
        "c_delivery_cnt INT64);");
    Run(context,
        "CREATE TABLE item (i_id INT64, i_name STRING(24), "
        "i_price NUMERIC, i_data STRING(50));");
    Run(context,
        "CREATE TABLE stock (s_i_id INT64, s_w_id INT64, "
        "s_quantity INT64, s_ytd NUMERIC, s_order_cnt INT64, "
        "s_remote_cnt INT64, s_data STRING(50));");
    Run(context,
        "CREATE TABLE orders (o_id INT64, o_d_id INT64, "
        "o_w_id INT64, o_c_id INT64, o_carrier_id INT64, "
        "o_ol_cnt INT64);");
    Run(context,
        "CREATE TABLE new_order (no_o_id INT64, no_d_id INT64, "
        "no_w_id INT64);");
    Run(context,
        "CREATE TABLE order_line (ol_o_id INT64, ol_d_id INT64, "
        "ol_w_id INT64, ol_number INT64, ol_i_id INT64, "
        "ol_quantity INT64, ol_amount NUMERIC, "
        "ol_delivery_d STRING(24));");
    Run(context,
        "CREATE TABLE history (h_c_id INT64, h_c_d_id INT64, "
        "h_c_w_id INT64, h_d_id INT64, h_w_id INT64, "
        "h_amount NUMERIC, h_data STRING(24));");
  }

  std::string path_;
  std::unique_ptr<Database> database_;
};

TEST_F(SqlEngineTpccTest, ExecutesAllTransactionQueryShapes) {
  TransactionContext context = database_->BeginContext();
  CreateSchema(context);
  Run(context,
      "INSERT INTO warehouse (w_name, w_ytd, w_tax, w_id) "
      "VALUES ('main', 100, 0.1, 1);");
  Run(context, "INSERT INTO district VALUES (1, 1, 0.2, 50.0, 101);");
  Run(context,
      "INSERT INTO customer VALUES "
      "(1, 1, 1, 'Alice', 'Smith', 'GC', 0.05, 10.0, 0.0, 0, 0), "
      "(2, 1, 1, 'Bob', 'Smith', 'BC', 0.10, 20.0, 0.0, 0, 0);");
  Run(context,
      "INSERT INTO item VALUES "
      "(1, 'widget', 3.5, 'original'), (2, 'gadget', 5.0, 'plain');");
  Run(context,
      "INSERT INTO stock VALUES "
      "(1, 1, 15, 0.0, 0, 0, 'original'), "
      "(2, 1, 8, 0.0, 0, 0, 'plain');");
  Run(context, "INSERT INTO orders VALUES (100, 1, 1, 1, NULL, 2);");
  Run(context, "INSERT INTO new_order VALUES (100, 1, 1);");
  Run(context,
      "INSERT INTO order_line VALUES "
      "(100, 1, 1, 1, 1, 5, 17.5, NULL), "
      "(100, 1, 1, 2, 2, 5, 25.0, NULL);");

  // New-Order: reads, district sequence update, stock CASE update and inserts.
  EXPECT_EQ(Run(context, "SELECT w_tax FROM warehouse WHERE w_id = 1;")[0][0],
            Value(0.1));
  Run(context,
      "UPDATE district SET d_next_o_id = d_next_o_id + 1 "
      "WHERE d_w_id = 1 AND d_id = 1;");
  Run(context,
      "UPDATE stock SET s_quantity = CASE WHEN s_quantity >= 10 "
      "THEN s_quantity - 5 ELSE s_quantity + 86 END, "
      "s_ytd = s_ytd + 5.0, s_order_cnt = s_order_cnt + 1 "
      "WHERE s_w_id = 1 AND s_i_id = 1;");
  Run(context, "INSERT INTO orders VALUES (101, 1, 1, 1, NULL, 1);");
  Run(context, "INSERT INTO new_order VALUES (101, 1, 1);");
  Run(context,
      "INSERT INTO order_line VALUES "
      "(101, 1, 1, 1, 1, 5, 17.5, NULL);");

  // Payment: updates all three entities and records history.
  Run(context, "UPDATE warehouse SET w_ytd = w_ytd + 10.0 WHERE w_id = 1;");
  Run(context,
      "UPDATE district SET d_ytd = d_ytd + 10.0 "
      "WHERE d_w_id = 1 AND d_id = 1;");
  Run(context,
      "UPDATE customer SET c_balance = c_balance - 10.0, "
      "c_ytd_payment = c_ytd_payment + 10.0, "
      "c_payment_cnt = c_payment_cnt + 1 "
      "WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;");
  Run(context,
      "INSERT INTO history VALUES "
      "(1, 1, 1, 1, 1, 10.0, 'main district');");

  // Order-Status: customer-name ordering, latest order and ordered lines.
  auto customers = Run(context,
                       "SELECT c_id, c_first FROM customer "
                       "WHERE c_w_id = 1 AND c_d_id = 1 "
                       "AND c_last = 'Smith' ORDER BY c_first;");
  ASSERT_EQ(customers.size(), 2);
  EXPECT_EQ(customers[0][1], Value("Alice"));
  auto latest = Run(context,
                    "SELECT MAX(o_id) AS latest FROM orders "
                    "WHERE o_w_id = 1 AND o_d_id = 1 AND o_c_id = 1;");
  EXPECT_EQ(latest[0][0], Value(101));
  auto joined = Run(context,
                    "SELECT c.c_id, o.o_id FROM customer AS c "
                    "JOIN orders AS o ON c.c_id = o.o_c_id "
                    "WHERE c.c_w_id = 1 AND o.o_w_id = 1 "
                    "ORDER BY o.o_id;");
  ASSERT_EQ(joined.size(), 2);
  EXPECT_EQ(joined[1][1], Value(101));
  auto lines = Run(context,
                   "SELECT ol_i_id, ol_amount FROM order_line "
                   "WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 100 "
                   "ORDER BY ol_number;");
  ASSERT_EQ(lines.size(), 2);
  EXPECT_EQ(lines[0][0], Value(1));

  // Delivery: oldest new order, delete, carrier/date updates and total amount.
  auto oldest = Run(context,
                    "SELECT no_o_id FROM new_order WHERE no_w_id = 1 "
                    "AND no_d_id = 1 ORDER BY no_o_id LIMIT 1;");
  EXPECT_EQ(oldest[0][0], Value(100));
  Run(context,
      "DELETE FROM new_order WHERE no_w_id = 1 AND no_d_id = 1 "
      "AND no_o_id = 100;");
  Run(context,
      "UPDATE orders SET o_carrier_id = 7 WHERE o_w_id = 1 "
      "AND o_d_id = 1 AND o_id = 100;");
  Run(context,
      "UPDATE order_line SET ol_delivery_d = CURRENT_TIMESTAMP() "
      "WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 100;");
  auto amount = Run(context,
                    "SELECT SUM(ol_amount) AS total FROM order_line "
                    "WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 100;");
  EXPECT_EQ(amount[0][0], Value(42.5));
  Run(context,
      "UPDATE customer SET c_balance = c_balance + 42.5, "
      "c_delivery_cnt = c_delivery_cnt + 1 "
      "WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;");

  // Stock-Level: correlated benchmark shape rewritten to an optimized join.
  auto low_stock = Run(context,
                       "SELECT COUNT(DISTINCT s_i_id) AS low_stock FROM stock "
                       "WHERE s_w_id = 1 AND s_quantity < 20 AND s_i_id IN "
                       "(SELECT ol_i_id FROM order_line WHERE ol_w_id = 1 "
                       "AND ol_d_id = 1 AND ol_o_id >= 82 AND ol_o_id < 102);");
  ASSERT_EQ(low_stock.size(), 1);
  EXPECT_EQ(low_stock[0][0], Value(2));
  ASSERT_EQ(context.PreCommit(), Status::kSuccess);
}

}  // namespace tinylamb
