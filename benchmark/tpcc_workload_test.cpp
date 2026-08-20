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
  EXPECT_GE(stock_level.sql_statements, 3);
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

TEST_F(TpccWorkloadTest, InitializeRejectsInvalidScales) {
  std::string error;

  TpccScale no_warehouse = TpccScale::ForTest();
  no_warehouse.warehouses = 0;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, no_warehouse, &error),
            Status::kUnknown);
  EXPECT_FALSE(error.empty());

  TpccScale no_customers = TpccScale::ForTest();
  no_customers.customers_per_district = 0;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, no_customers, &error),
            Status::kUnknown);
  EXPECT_FALSE(error.empty());

  TpccScale no_lines = TpccScale::ForTest();
  no_lines.min_order_lines = 0;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, no_lines, &error),
            Status::kUnknown);
  EXPECT_FALSE(error.empty());

  TpccScale orders_below_customers = TpccScale::ForTest();
  orders_below_customers.initial_orders_per_district = 1;
  ASSERT_EQ(
      TpccWorkload::Initialize(*database_, orders_below_customers, &error),
      Status::kUnknown);
  EXPECT_FALSE(error.empty());

  TpccScale too_many_new_orders = TpccScale::ForTest();
  too_many_new_orders.new_orders_per_district =
      too_many_new_orders.initial_orders_per_district + 1;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, too_many_new_orders, &error),
            Status::kUnknown);
  EXPECT_FALSE(error.empty());
}

TEST_F(TpccWorkloadTest, InitializeBeyondThousandCustomersUsesNurandLastName) {
  // Arrange -- more than 1000 customers per district forces the C-Load
  // NURand last-name path during population.
  TpccScale scale = TpccScale::ForTest();
  scale.customers_per_district = 1005;
  scale.initial_orders_per_district = 1005;
  scale.new_orders_per_district = 1;

  // Act -- initialize the fixture with the enlarged customer population.
  std::string error;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kSuccess)
      << error;

  // Assert -- every customer row landed with a well-formed last name.
  const std::vector<Row> count = Run("SELECT COUNT(*) FROM customer;");
  ASSERT_EQ(count.size(), 1);
  EXPECT_EQ(count[0][0], Value(1005));
  const std::vector<Row> first =
      Run("SELECT c_last FROM customer WHERE c_id = 1;");
  ASSERT_EQ(first.size(), 1);
  EXPECT_EQ(first[0][0], Value(TpccLastName(0)));
  const std::vector<Row> nurand =
      Run("SELECT c_last FROM customer WHERE c_id = 1005;");
  ASSERT_EQ(nurand.size(), 1);
  EXPECT_FALSE(nurand[0][0].IsNull());
  EXPECT_EQ(nurand[0][0].type, ValueType::kVarChar);
}

TEST_F(TpccWorkloadTest, NextTransactionTypeSamplesClause52Mix) {
  // Arrange -- a workload bound to a random (negative) terminal id so the
  // home warehouse/district selection uses the random path.
  TpccWorkload workload(*database_, TpccScale::ForTest(), 7, -1);

  // Act -- sample the transaction type mix many times.
  std::array<int, 5> counts = {0, 0, 0, 0, 0};
  for (int i = 0; i < 500; ++i) {
    const TpccTransactionType type = workload.NextTransactionType();
    ++counts[static_cast<size_t>(type)];
  }

  // Assert -- the Clause 5.2 mix is roughly 45/43/4/4/4 percent.
  EXPECT_GE(counts[0], 150);  // kNewOrder
  EXPECT_GE(counts[1], 150);  // kPayment
  EXPECT_GT(counts[2], 0);    // kOrderStatus
  EXPECT_GT(counts[3], 0);    // kDelivery
  EXPECT_GT(counts[4], 0);    // kStockLevel
  const int new_order_plus_payment = counts[0] + counts[1];
  EXPECT_GE(new_order_plus_payment, 400);
  EXPECT_LE(new_order_plus_payment, 480);
}

TEST(TpccScaleTest, ToStringCoversEveryTransactionType) {
  EXPECT_EQ(ToString(TpccTransactionType::kNewOrder), "new_order");
  EXPECT_EQ(ToString(TpccTransactionType::kPayment), "payment");
  EXPECT_EQ(ToString(TpccTransactionType::kOrderStatus), "order_status");
  EXPECT_EQ(ToString(TpccTransactionType::kDelivery), "delivery");
  EXPECT_EQ(ToString(TpccTransactionType::kStockLevel), "stock_level");
  EXPECT_EQ(ToString(TpccTransactionType::kCount), "unknown");
  EXPECT_EQ(ToString(static_cast<TpccTransactionType>(99)), "unknown");
}

TEST_F(TpccWorkloadTest, ExecuteOnUninitializedDatabaseReportsError) {
  // Arrange -- the database has no TPC-C tables at all.
  TpccWorkload workload(*database_, TpccScale::ForTest(), 42, 0);

  // Act -- run NewOrder against the empty schema.
  const TpccTransactionResult new_order =
      workload.Execute(TpccTransactionType::kNewOrder);

  // Assert -- the failure is surfaced through the result, not a crash.
  EXPECT_FALSE(new_order.committed);
  EXPECT_FALSE(new_order.user_rollback);
  EXPECT_NE(new_order.error.find("new_order"), std::string::npos)
      << new_order.error;

  // Act -- execute an invalid transaction type.
  const TpccTransactionResult invalid =
      workload.Execute(static_cast<TpccTransactionType>(99));

  // Assert -- the invalid type is reported precisely.
  EXPECT_FALSE(invalid.committed);
  EXPECT_EQ(invalid.error, "invalid TPC-C transaction type");
}

TEST_F(TpccWorkloadTest, PaymentNameLookupAndBcCreditUpdate) {
  // Arrange -- a full test-scale fixture.
  const TpccScale scale = TpccScale::ForTest();
  std::string error;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kSuccess)
      << error;

  // Act -- run payments with a variety of seeds. The 60% name-based lookup
  // path and the BC-credit c_data rewrite are chosen by the workload RNG, so
  // several seeds are tried until a BC-credit update is observed.
  const std::vector<Row> bc_customers =
      Run("SELECT COUNT(*) FROM customer WHERE c_credit = 'BC';");
  ASSERT_EQ(bc_customers.size(), 1);
  for (uint64_t seed = 1; seed <= 60; ++seed) {
    TpccWorkload workload(*database_, scale, seed, 0);
    const TpccTransactionResult payment =
        workload.Execute(TpccTransactionType::kPayment);
    ASSERT_TRUE(payment.committed) << payment.error;
    EXPECT_GT(payment.amount, 0.0);
  }
  if (bc_customers[0][0] != Value(int64_t{0})) {
    const std::vector<Row> rewritten =
        Run("SELECT COUNT(*) FROM customer WHERE c_data <> 'customer data';");
    ASSERT_EQ(rewritten.size(), 1);
    EXPECT_NE(rewritten[0][0], Value(int64_t{0}));
  }
  const std::vector<Row> history = Run("SELECT COUNT(*) FROM history;");
  ASSERT_EQ(history.size(), 1);
  EXPECT_EQ(history[0][0], Value(70));
}

}  // namespace
}  // namespace tinylamb

namespace tinylamb {
namespace {

TEST(TpccScaleTest, OfficialClampsScaleFactorBelowOne) {
  EXPECT_EQ(TpccScale::Official(0).warehouses, 1);
  EXPECT_EQ(TpccScale::Official(-3).warehouses, 1);
  EXPECT_EQ(TpccScale::Official(0).Terminals(), 10);
}

TEST(TpccScaleTest, LastNameClampsOutOfRangeInputs) {
  EXPECT_EQ(TpccLastName(-1), "BARBARBAR");
  EXPECT_EQ(TpccLastName(1000), "EINGEINGEING");
  EXPECT_EQ(TpccLastName(1500), "EINGEINGEING");
}

TEST(TpccScaleTest, NurandFromSeedIsDeterministic) {
  const TpccNurand first = TpccNurand::FromSeed(77);
  const TpccNurand second = TpccNurand::FromSeed(77);
  ASSERT_TRUE(first.valid);
  ASSERT_TRUE(second.valid);
  EXPECT_EQ(first.c_id, second.c_id);
  EXPECT_EQ(first.c_ol_i_id, second.c_ol_i_id);
  EXPECT_EQ(first.c_last_load, second.c_last_load);
  EXPECT_EQ(first.c_last_run, second.c_last_run);
  EXPECT_GE(first.c_id, 0);
  EXPECT_LE(first.c_id, 1023);
  EXPECT_GE(first.c_ol_i_id, 0);
  EXPECT_LE(first.c_ol_i_id, 8191);
}

TEST_F(TpccWorkloadTest, InitializeRejectsMaxLinesBelowMin) {
  TpccScale scale = TpccScale::ForTest();
  scale.max_order_lines = scale.min_order_lines - 1;
  std::string error;
  EXPECT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kUnknown);
  EXPECT_FALSE(error.empty());
}

TEST_F(TpccWorkloadTest, PaymentRewritesBcCreditData) {
  const TpccScale scale = TpccScale::ForTest();
  std::string error;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kSuccess)
      << error;

  // Seed 2 deterministically pays a BC-credit customer, whose c_data field is
  // rewritten with the payment audit trail.
  TpccWorkload workload(*database_, scale, 2, 0);
  const TpccTransactionResult payment =
      workload.Execute(TpccTransactionType::kPayment);
  ASSERT_TRUE(payment.committed) << payment.error;

  const std::vector<Row> rewritten =
      Run("SELECT COUNT(*) FROM customer WHERE c_data <> 'customer data';");
  ASSERT_EQ(rewritten.size(), 1);
  EXPECT_EQ(rewritten[0][0], Value(1));
  const std::vector<Row> history = Run("SELECT COUNT(*) FROM history;");
  ASSERT_EQ(history.size(), 1);
  EXPECT_EQ(history[0][0], Value(11));
}

TEST_F(TpccWorkloadTest, NewOrderRemoteSupplyUpdatesRemoteStock) {
  TpccScale scale = TpccScale::ForTest();
  scale.warehouses = 2;
  std::string error;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kSuccess)
      << error;

  // Seed 46 deterministically picks a remote supply warehouse for one line, so
  // s_remote_cnt on warehouse 2 must increase.
  TpccWorkload workload(*database_, scale, 46, 0);
  const TpccTransactionResult new_order =
      workload.Execute(TpccTransactionType::kNewOrder);
  ASSERT_TRUE(new_order.committed) << new_order.error;

  const std::vector<Row> remote =
      Run("SELECT SUM(s_remote_cnt) FROM stock WHERE s_w_id = 2;");
  ASSERT_EQ(remote.size(), 1);
  EXPECT_GE(remote[0][0].value.int_value, 1);
}

TEST_F(TpccWorkloadTest, NewOrderRollsBackOnInvalidItem) {
  TpccScale scale = TpccScale::ForTest();
  scale.warehouses = 2;
  std::string error;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kSuccess)
      << error;

  // Seed 107 deterministically draws the 1%-probability rollback branch: the
  // last order line references a non-existent item and the transaction must be
  // rolled back instead of committed.
  TpccWorkload workload(*database_, scale, 107, 0);
  const TpccTransactionResult new_order =
      workload.Execute(TpccTransactionType::kNewOrder);
  EXPECT_TRUE(new_order.user_rollback);
  EXPECT_FALSE(new_order.committed);

  const std::vector<Row> queues =
      Run("SELECT COUNT(*) FROM new_order;");
  ASSERT_EQ(queues.size(), 1);
  EXPECT_EQ(queues[0][0], Value(6));
}

TEST_F(TpccWorkloadTest, ExecuteCountTypeReportsError) {
  // kCount is a sentinel, not a runnable transaction; it falls through the
  // dispatch switch to the invalid-type error result.
  TpccWorkload workload(*database_, TpccScale::ForTest(), 1, 0);
  const TpccTransactionResult result =
      workload.Execute(TpccTransactionType::kCount);
  EXPECT_FALSE(result.committed);
  EXPECT_EQ(result.error, "invalid TPC-C transaction type");
}

TEST_F(TpccWorkloadTest, PaymentChoosesRemoteCustomerWarehouse) {
  TpccScale scale = TpccScale::ForTest();
  scale.warehouses = 2;
  std::string error;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kSuccess)
      << error;

  // Seed 1 deterministically draws the 15% remote-customer branch: the payment
  // targets a customer in warehouse 2, recorded in the history row. The seed
  // history already contains one h_c_w_id=2 row per warehouse-2 customer (10),
  // so the payment raises the count to 11.
  TpccWorkload workload(*database_, scale, 1, 0);
  const TpccTransactionResult payment =
      workload.Execute(TpccTransactionType::kPayment);
  ASSERT_TRUE(payment.committed) << payment.error;

  const std::vector<Row> remote_history =
      Run("SELECT COUNT(*) FROM history WHERE h_c_w_id = 2;");
  ASSERT_EQ(remote_history.size(), 1);
  EXPECT_EQ(remote_history[0][0], Value(11));
}

}  // namespace
}  // namespace tinylamb
