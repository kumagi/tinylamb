/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "benchmark/tpcc_workload.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
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

TEST(TpccScaleTest, OfficialMatchesClause43) {
  const TpccScale scale = TpccScale::Official(1);
  EXPECT_EQ(scale.ScaleFactor(), 1);
  EXPECT_EQ(scale.warehouses, 1);
  EXPECT_EQ(scale.districts_per_warehouse, 10);
  EXPECT_EQ(scale.customers_per_district, 3000);
  EXPECT_EQ(scale.items, 100000);
  EXPECT_EQ(scale.initial_orders_per_district, 3000);
  EXPECT_EQ(scale.new_orders_per_district, 900);
  EXPECT_EQ(scale.min_order_lines, 5);
  EXPECT_EQ(scale.max_order_lines, 15);
  EXPECT_EQ(scale.Terminals(), 10);
  EXPECT_EQ(TpccScale::Official(4).warehouses, 4);
  EXPECT_EQ(TpccScale::Official(4).Terminals(), 40);
}

TEST(TpccScaleTest, LastNameSyllables) {
  EXPECT_EQ(TpccLastName(0), "BARBARBAR");
  EXPECT_EQ(TpccLastName(3), "BARBARPRI");
  EXPECT_EQ(TpccLastName(999), "EINGEINGEING");
}

TEST(TpccScaleTest, NurandCLastDelta) {
  for (uint64_t seed = 1; seed < 200; ++seed) {
    const TpccNurand nurand = TpccNurand::FromSeed(seed);
    ASSERT_TRUE(nurand.valid);
    const int delta = std::abs(nurand.c_last_load - nurand.c_last_run);
    EXPECT_GE(delta, 65) << seed;
    EXPECT_LE(delta, 119) << seed;
    EXPECT_NE(delta, 96) << seed;
    EXPECT_NE(delta, 112) << seed;
    EXPECT_GE(nurand.c_last_load, 0);
    EXPECT_LE(nurand.c_last_load, 255);
    EXPECT_GE(nurand.c_last_run, 0);
    EXPECT_LE(nurand.c_last_run, 255);
  }
}

TEST_F(TpccWorkloadTest, CommitsFiveTransactionsAndPreservesInvariants) {
  const TpccScale scale = TpccScale::ForTest();
  std::string error;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kSuccess)
      << error;
  const std::vector<Row> initial_customer_count =
      Run("SELECT COUNT(*) FROM customer;");
  ASSERT_EQ(initial_customer_count.size(), 1);
  EXPECT_EQ(initial_customer_count[0][0], Value(10));
  const std::vector<Row> initial_customers =
      Run("SELECT c_id FROM customer WHERE c_w_id = 1 AND c_d_id = 1 "
          "AND c_id = 4;");
  ASSERT_EQ(initial_customers.size(), 1);
  const std::vector<Row> initial_queues =
      Run("SELECT COUNT(*) FROM new_order;");
  ASSERT_EQ(initial_queues.size(), 1);
  EXPECT_EQ(initial_queues[0][0], Value(3));
  const std::vector<Row> initial_history =
      Run("SELECT COUNT(*) FROM history;");
  ASSERT_EQ(initial_history.size(), 1);
  EXPECT_EQ(initial_history[0][0], Value(10));
  ASSERT_EQ(Run("SELECT s_quantity FROM stock WHERE s_w_id = 1 AND s_i_id = 1;")
                .size(),
            1);
  ASSERT_EQ(Run("SELECT s_quantity, s_data FROM stock WHERE s_w_id = 1 AND "
                "s_i_id = 2;")
                .size(),
            1);
  ASSERT_EQ(Run("SELECT s_quantity, s_data FROM stock WHERE s_w_id = 1 AND "
                "s_i_id = 7;")
                .size(),
            1);

  auto ExplainText = [this](std::string_view sql) {
    std::string text;
    for (const Row& row : Run(std::string("EXPLAIN ") + std::string(sql))) {
      text += row[0].AsString();
      text += '\n';
    }
    return text;
  };
  const std::string customer_plan = ExplainText(
      "SELECT c_id FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 4;");
  EXPECT_NE(customer_plan.find("Index"), std::string::npos) << customer_plan;
  EXPECT_EQ(customer_plan.find("ParallelSort"), std::string::npos)
      << customer_plan;
  const std::string name_plan = ExplainText(
      "SELECT c_id FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND "
      "c_last = 'BARBARPRI' ORDER BY c_first;");
  EXPECT_NE(name_plan.find("Index"), std::string::npos) << name_plan;
  EXPECT_EQ(name_plan.find("ParallelSort"), std::string::npos) << name_plan;
  const std::string queue_plan = ExplainText(
      "SELECT no_o_id FROM new_order WHERE no_w_id = 1 AND no_d_id = 1 "
      "ORDER BY no_o_id LIMIT 1;");
  EXPECT_NE(queue_plan.find("Index"), std::string::npos) << queue_plan;
  EXPECT_EQ(queue_plan.find("ParallelSort"), std::string::npos) << queue_plan;
  TpccWorkload workload(*database_, scale, 42, 0);

  const TpccTransactionResult new_order =
      workload.Execute(TpccTransactionType::kNewOrder);
  ASSERT_TRUE(new_order.committed || new_order.user_rollback) << new_order.error;
  if (new_order.committed) {
    EXPECT_EQ(new_order.order_id, 11);
    EXPECT_GE(new_order.sql_statements,
              6 + 4 * static_cast<size_t>(scale.min_order_lines));
  }
  const std::vector<Row> queues_after_new_order =
      Run("SELECT COUNT(*) FROM new_order;");
  ASSERT_EQ(queues_after_new_order.size(), 1);
  EXPECT_EQ(queues_after_new_order[0][0],
            Value(new_order.committed ? 4 : 3));

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
  const std::vector<Row> queues_after_delivery =
      Run("SELECT COUNT(*) FROM new_order;");
  ASSERT_EQ(queues_after_delivery.size(), 1);
  EXPECT_EQ(queues_after_delivery[0][0],
            Value(new_order.committed ? 3 : 2));

  const TpccTransactionResult stock_level =
      workload.Execute(TpccTransactionType::kStockLevel);
  ASSERT_TRUE(stock_level.committed) << stock_level.error;
  EXPECT_EQ(stock_level.sql_statements, 2);
  EXPECT_GE(stock_level.low_stock, 0);

  const std::vector<Row> district =
      Run("SELECT d_next_o_id FROM district WHERE d_w_id = 1 AND d_id = 1;");
  ASSERT_EQ(district.size(), 1);
  EXPECT_EQ(district[0][0], Value(new_order.committed ? 12 : 11));

  const std::vector<Row> history = Run("SELECT COUNT(*) FROM history;");
  ASSERT_EQ(history.size(), 1);
  EXPECT_EQ(history[0][0], Value(11));

  const std::vector<Row> delivery_count =
      Run("SELECT SUM(c_delivery_cnt) FROM customer;");
  ASSERT_EQ(delivery_count.size(), 1);
  EXPECT_EQ(delivery_count[0][0], Value(1));
}

}  // namespace
}  // namespace tinylamb
