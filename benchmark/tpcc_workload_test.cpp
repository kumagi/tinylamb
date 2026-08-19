/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "benchmark/tpcc_workload.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/random_string.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

class TpccWorkloadTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = "tpcc_workload_test-" + RandomString();
    database_ = std::make_unique<Database>(path_);
  }

  void TearDown() override { database_->DeleteAll(); }

  std::vector<Row> Run(std::string_view sql) {
    TransactionContext context = database_->BeginContext();
    SqlEngine engine(*database_);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    EXPECT_TRUE(prepared.HasValue()) << sql << "\n" << engine.LastError();
    std::vector<Row> rows;
    if (prepared.HasValue()) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) rows.push_back(row);
    }
    EXPECT_EQ(context.PreCommit(), Status::kSuccess);
    return rows;
  }

  std::string path_;
  std::unique_ptr<Database> database_;
};

TEST_F(TpccWorkloadTest, CommitsFiveTransactionsAndPreservesInvariants) {
  TpccScale scale;
  scale.customers_per_district = 5;
  scale.items = 20;
  scale.initial_orders_per_district = 5;
  scale.order_lines_per_order = 3;
  std::string error;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kSuccess)
      << error;
  const std::vector<Row> initial_customer_count =
      Run("SELECT COUNT(*) FROM customer;");
  ASSERT_EQ(initial_customer_count.size(), 1);
  EXPECT_EQ(initial_customer_count[0][0], Value(5));
  const std::vector<Row> initial_customers =
      Run("SELECT c_id FROM customer WHERE c_w_id = 1 AND c_d_id = 1 "
          "AND c_id = 4;");
  ASSERT_EQ(initial_customers.size(), 1);
  const std::vector<Row> initial_queues =
      Run("SELECT COUNT(*) FROM new_order;");
  ASSERT_EQ(initial_queues.size(), 1);
  EXPECT_EQ(initial_queues[0][0], Value(5));
  TpccWorkload workload(*database_, scale, 42);

  const TpccTransactionResult new_order =
      workload.Execute(TpccTransactionType::kNewOrder);
  ASSERT_TRUE(new_order.committed) << new_order.error;
  EXPECT_EQ(new_order.order_id, 6);
  EXPECT_EQ(new_order.sql_statements, 18);
  const std::vector<Row> queues_after_new_order =
      Run("SELECT COUNT(*) FROM new_order;");
  ASSERT_EQ(queues_after_new_order.size(), 1);
  EXPECT_EQ(queues_after_new_order[0][0], Value(6));

  const TpccTransactionResult payment =
      workload.Execute(TpccTransactionType::kPayment);
  ASSERT_TRUE(payment.committed) << payment.error;
  EXPECT_EQ(payment.sql_statements, 7);

  const TpccTransactionResult order_status =
      workload.Execute(TpccTransactionType::kOrderStatus);
  ASSERT_TRUE(order_status.committed) << order_status.error;
  EXPECT_EQ(order_status.sql_statements, 3);

  const TpccTransactionResult delivery =
      workload.Execute(TpccTransactionType::kDelivery);
  ASSERT_TRUE(delivery.committed) << delivery.error;
  EXPECT_EQ(delivery.delivered_orders, 1);
  EXPECT_EQ(delivery.sql_statements, 7);
  const std::vector<Row> queues_after_delivery =
      Run("SELECT COUNT(*) FROM new_order;");
  ASSERT_EQ(queues_after_delivery.size(), 1);
  EXPECT_EQ(queues_after_delivery[0][0], Value(5));

  const TpccTransactionResult stock_level =
      workload.Execute(TpccTransactionType::kStockLevel);
  ASSERT_TRUE(stock_level.committed) << stock_level.error;
  EXPECT_EQ(stock_level.sql_statements, 2);
  EXPECT_GE(stock_level.low_stock, 0);

  const std::vector<Row> district =
      Run("SELECT d_next_o_id FROM district WHERE d_w_id = 1 AND d_id = 1;");
  ASSERT_EQ(district.size(), 1);
  EXPECT_EQ(district[0][0], Value(7));

  const std::vector<Row> queues = Run("SELECT COUNT(*) FROM new_order;");
  ASSERT_EQ(queues.size(), 1);
  EXPECT_EQ(queues[0][0], Value(5));

  const std::vector<Row> history = Run("SELECT COUNT(*) FROM history;");
  ASSERT_EQ(history.size(), 1);
  EXPECT_EQ(history[0][0], Value(1));

  const std::vector<Row> stock = Run("SELECT SUM(s_order_cnt) FROM stock;");
  ASSERT_EQ(stock.size(), 1);
  EXPECT_EQ(stock[0][0], Value(3));

  const std::vector<Row> delivered =
      Run("SELECT COUNT(*) FROM orders WHERE o_carrier_id IS NOT NULL;");
  ASSERT_EQ(delivered.size(), 1);
  EXPECT_EQ(delivered[0][0], Value(1));

  const std::vector<Row> delivery_count =
      Run("SELECT SUM(c_delivery_cnt) FROM customer;");
  ASSERT_EQ(delivery_count.size(), 1);
  EXPECT_EQ(delivery_count[0][0], Value(1));
}

}  // namespace
}  // namespace tinylamb
