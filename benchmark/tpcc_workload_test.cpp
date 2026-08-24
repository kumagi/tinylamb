/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "benchmark/tpcc_workload.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

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
      while (prepared.Value()->Next(&row, nullptr)) {
        rows.push_back(row);
      }
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
  const std::vector<Row> initial_history = Run("SELECT COUNT(*) FROM history;");
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
      "SELECT c_id FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = "
      "4;");
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
  ASSERT_TRUE(new_order.committed || new_order.user_rollback)
      << new_order.error;
  EXPECT_GT(new_order.sql_statements, 0);
  if (new_order.committed) {
    EXPECT_EQ(new_order.order_id, 11);
    // Six fixed statements plus batched item read, stock read/update and the
    // multi-row order-line insert at this all-local test scale.
    EXPECT_EQ(new_order.sql_statements, 10);
  }
  const std::vector<Row> queues_after_new_order =
      Run("SELECT COUNT(*) FROM new_order;");
  ASSERT_EQ(queues_after_new_order.size(), 1);
  EXPECT_EQ(queues_after_new_order[0][0], Value(new_order.committed ? 4 : 3));

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
  EXPECT_GT(delivery.sql_statements, 0);
  EXPECT_EQ(delivery.delivered_orders, 1);
  const std::vector<Row> queues_after_delivery =
      Run("SELECT COUNT(*) FROM new_order;");
  ASSERT_EQ(queues_after_delivery.size(), 1);
  EXPECT_EQ(queues_after_delivery[0][0], Value(new_order.committed ? 3 : 2));

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
  // Deliberately out-of-range value to probe the fallback arm.
  EXPECT_EQ(ToString(static_cast<TpccTransactionType>(
                99)),  // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
            "unknown");
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

  // Act -- execute an invalid transaction type (deliberately out of range).
  const TpccTransactionResult invalid =
      workload.Execute(static_cast<TpccTransactionType>(
          99));  // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)

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

  // D_ID now consumes its mandated per-transaction random draw, so branch
  // tests must not bake in the old PRNG call position. Search a bounded,
  // deterministic seed range until a BC customer is selected.
  std::vector<Row> rewritten;
  for (uint64_t seed = 1; seed <= 128; ++seed) {
    TpccWorkload workload(*database_, scale, seed, 0);
    const TpccTransactionResult payment =
        workload.Execute(TpccTransactionType::kPayment);
    ASSERT_TRUE(payment.committed) << payment.error;
    rewritten =
        Run("SELECT COUNT(*) FROM customer WHERE c_data <> 'customer data';");
    if (rewritten[0][0].value.int_value != 0) { break; }
  }
  ASSERT_EQ(rewritten.size(), 1);
  EXPECT_EQ(rewritten[0][0], Value(1));
  const std::vector<Row> history = Run("SELECT COUNT(*) FROM history;");
  ASSERT_EQ(history.size(), 1);
  EXPECT_GT(history[0][0].value.int_value, 10);
}

TEST_F(TpccWorkloadTest, NewOrderRemoteSupplyUpdatesRemoteStock) {
  TpccScale scale = TpccScale::ForTest();
  scale.warehouses = 2;
  std::string error;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kSuccess)
      << error;

  std::vector<Row> remote;
  for (uint64_t seed = 1; seed <= 128; ++seed) {
    TpccWorkload workload(*database_, scale, seed, 0);
    const TpccTransactionResult new_order =
        workload.Execute(TpccTransactionType::kNewOrder);
    ASSERT_TRUE(new_order.committed || new_order.user_rollback)
        << new_order.error;
    remote = Run("SELECT SUM(s_remote_cnt) FROM stock WHERE s_w_id = 2;");
    if (remote[0][0].value.int_value != 0) { break; }
  }
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

  bool observed_rollback = false;
  int64_t committed = 0;
  for (uint64_t seed = 1; seed <= 512; ++seed) {
    TpccWorkload workload(*database_, scale, seed, 0);
    const TpccTransactionResult new_order =
        workload.Execute(TpccTransactionType::kNewOrder);
    if (new_order.user_rollback) {
      observed_rollback = true;
      EXPECT_FALSE(new_order.committed);
      break;
    }
    ASSERT_TRUE(new_order.committed) << new_order.error;
    ++committed;
  }
  EXPECT_TRUE(observed_rollback);

  const std::vector<Row> queues = Run("SELECT COUNT(*) FROM new_order;");
  ASSERT_EQ(queues.size(), 1);
  EXPECT_EQ(queues[0][0], Value(6 + committed));
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

TEST_F(TpccWorkloadTest, InitializeTwiceRejectsExistingSchema) {
  const TpccScale scale = TpccScale::ForTest();
  std::string error;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kSuccess)
      << error;

  // Act -- a second population must fail at the first CREATE TABLE because
  // the schema already exists, and the partial schema transaction aborts.
  std::string second_error;
  EXPECT_EQ(TpccWorkload::Initialize(*database_, scale, &second_error),
            Status::kConflicts);
  EXPECT_FALSE(second_error.empty());
}

TEST(TpccWorkloadFailPathTest, NewOrderAbortsWhenSupportingRowsMissing) {
  auto MakeDb = []() -> std::unique_ptr<Database> {
    const std::string path = "tpcc_workload_failpath_test-" + RandomString();
    auto database = std::make_unique<Database>(path);
    std::string error;
    if (TpccWorkload::Initialize(*database, TpccScale::ForTest(), &error) !=
        Status::kSuccess) {
      ADD_FAILURE() << error;
      return nullptr;
    }
    return database;
  };
  auto RunSql = [](Database& database, std::string_view sql) {
    TransactionContext context = database.BeginContext();
    SqlEngine engine(database);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    if (!prepared.HasValue()) {
      ADD_FAILURE() << sql << "\n" << engine.LastError();
      context.Abort();
      return;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    if (context.PreCommit() != Status::kSuccess) {
      ADD_FAILURE() << "commit failed: " << sql;
    }
  };
  auto RunNewOrder = [](Database& database, uint64_t seed) {
    TpccWorkload workload(database, TpccScale::ForTest(), seed, 0);
    return workload.Execute(TpccTransactionType::kNewOrder);
  };

  {
    // A missing district makes the d_tax/d_next_o_id read return no rows.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DELETE FROM district WHERE d_w_id = 1 AND d_id = 1;");
    const TpccTransactionResult result = RunNewOrder(*database, 1);
    EXPECT_FALSE(result.committed);
    EXPECT_FALSE(result.user_rollback);
    EXPECT_NE(result.error.find("new-order district update affected too few rows"),
              std::string::npos)
        << result.error;
    database->DeleteAll();
  }
  {
    // A missing customer row makes the c_discount/c_last read return no rows.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DELETE FROM customer;");
    const TpccTransactionResult result = RunNewOrder(*database, 1);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("new-order customer read returned no rows"),
              std::string::npos)
        << result.error;
    database->DeleteAll();
  }
  {
    // Missing stock makes the first line's s_quantity read return no rows.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DELETE FROM stock;");
    const TpccTransactionResult result = RunNewOrder(*database, 1);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("new-order stock batch returned too few rows"),
              std::string::npos)
        << result.error;
    database->DeleteAll();
  }
  {
    // Dropping the orders table makes the orders INSERT fail to prepare.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DROP TABLE orders;");
    const TpccTransactionResult result = RunNewOrder(*database, 1);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("INSERT INTO orders"), std::string::npos)
        << result.error;
    database->DeleteAll();
  }
  {
    // Dropping the new_order table makes the queue INSERT fail to prepare.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DROP TABLE new_order;");
    const TpccTransactionResult result = RunNewOrder(*database, 1);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("INSERT INTO new_order"), std::string::npos)
        << result.error;
    database->DeleteAll();
  }
  {
    // Dropping the order_line table makes the line INSERT fail to prepare.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DROP TABLE order_line;");
    const TpccTransactionResult result = RunNewOrder(*database, 1);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("INSERT INTO order_line"), std::string::npos)
        << result.error;
    database->DeleteAll();
  }
}

TEST(TpccWorkloadFailPathTest, PaymentAbortsWhenSupportingRowsMissing) {
  auto MakeDb = []() -> std::unique_ptr<Database> {
    const std::string path = "tpcc_workload_payment_test-" + RandomString();
    auto database = std::make_unique<Database>(path);
    std::string error;
    if (TpccWorkload::Initialize(*database, TpccScale::ForTest(), &error) !=
        Status::kSuccess) {
      ADD_FAILURE() << error;
      return nullptr;
    }
    return database;
  };
  auto RunSql = [](Database& database, std::string_view sql) {
    TransactionContext context = database.BeginContext();
    SqlEngine engine(database);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    if (!prepared.HasValue()) {
      ADD_FAILURE() << sql << "\n" << engine.LastError();
      context.Abort();
      return;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    if (context.PreCommit() != Status::kSuccess) {
      ADD_FAILURE() << "commit failed: " << sql;
    }
  };

  {
    // A missing warehouse makes the first w_ytd UPDATE affect zero rows.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DELETE FROM warehouse;");
    TpccWorkload workload(*database, TpccScale::ForTest(), 1, 0);
    const TpccTransactionResult result =
        workload.Execute(TpccTransactionType::kPayment);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("payment warehouse read returned no rows"),
              std::string::npos)
        << result.error;
    database->DeleteAll();
  }
  {
    // A missing district makes the d_ytd UPDATE affect zero rows.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DELETE FROM district WHERE d_w_id = 1 AND d_id = 1;");
    TpccWorkload workload(*database, TpccScale::ForTest(), 1, 0);
    const TpccTransactionResult result =
        workload.Execute(TpccTransactionType::kPayment);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("payment district read returned no rows"),
              std::string::npos)
        << result.error;
    database->DeleteAll();
  }
  {
    // With every customer gone, the customer lookup (either by name or by
    // id depending on the seed) returns no rows.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DELETE FROM customer;");
    bool saw_name_lookup = false;
    bool saw_id_lookup = false;
    for (uint64_t seed = 1; seed <= 25; ++seed) {
      TpccWorkload workload(*database, TpccScale::ForTest(), seed, 0);
      const TpccTransactionResult result =
          workload.Execute(TpccTransactionType::kPayment);
      EXPECT_FALSE(result.committed) << result.error;
      if (result.error.find("payment customer-name read returned no rows") !=
          std::string::npos) {
        saw_name_lookup = true;
      } else if (result.error.find("payment customer read returned no rows") !=
                 std::string::npos) {
        saw_id_lookup = true;
      } else {
        ADD_FAILURE() << "unexpected payment failure: " << result.error;
      }
    }
    EXPECT_TRUE(saw_name_lookup);
    EXPECT_TRUE(saw_id_lookup);
    database->DeleteAll();
  }
  {
    // Dropping the history table makes the history INSERT fail to prepare.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DROP TABLE history;");
    TpccWorkload workload(*database, TpccScale::ForTest(), 1, 0);
    const TpccTransactionResult result =
        workload.Execute(TpccTransactionType::kPayment);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("INSERT INTO history"), std::string::npos)
        << result.error;
    database->DeleteAll();
  }
}

TEST(TpccWorkloadFailPathTest, OrderStatusAbortsWhenSupportingRowsMissing) {
  auto MakeDb = []() -> std::unique_ptr<Database> {
    const std::string path = "tpcc_workload_orderstatus_test-" + RandomString();
    auto database = std::make_unique<Database>(path);
    std::string error;
    if (TpccWorkload::Initialize(*database, TpccScale::ForTest(), &error) !=
        Status::kSuccess) {
      ADD_FAILURE() << error;
      return nullptr;
    }
    return database;
  };
  auto RunSql = [](Database& database, std::string_view sql) {
    TransactionContext context = database.BeginContext();
    SqlEngine engine(database);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    if (!prepared.HasValue()) {
      ADD_FAILURE() << sql << "\n" << engine.LastError();
      context.Abort();
      return;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    if (context.PreCommit() != Status::kSuccess) {
      ADD_FAILURE() << "commit failed: " << sql;
    }
  };

  {
    // On a healthy fixture both the name-based and the id-based customer
    // lookup paths must commit (seeds take each branch deterministically).
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    for (uint64_t seed = 1; seed <= 12; ++seed) {
      TpccWorkload workload(*database, TpccScale::ForTest(), seed, 0);
      const TpccTransactionResult result =
          workload.Execute(TpccTransactionType::kOrderStatus);
      EXPECT_TRUE(result.committed) << result.error;
    }
    database->DeleteAll();
  }
  {
    // With every customer gone, either lookup path returns no rows.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DELETE FROM customer;");
    bool saw_name_lookup = false;
    bool saw_id_lookup = false;
    for (uint64_t seed = 1; seed <= 25; ++seed) {
      TpccWorkload workload(*database, TpccScale::ForTest(), seed, 0);
      const TpccTransactionResult result =
          workload.Execute(TpccTransactionType::kOrderStatus);
      EXPECT_FALSE(result.committed) << result.error;
      if (result.error.find("order-status customer-name read returned no "
                            "rows") != std::string::npos) {
        saw_name_lookup = true;
      } else if (result.error.find(
                     "order-status customer read returned no rows") !=
                 std::string::npos) {
        saw_id_lookup = true;
      } else {
        ADD_FAILURE() << "unexpected order-status failure: " << result.error;
      }
    }
    EXPECT_TRUE(saw_name_lookup);
    EXPECT_TRUE(saw_id_lookup);
    database->DeleteAll();
  }
  {
    // With every order gone, the latest-order read returns no rows.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DELETE FROM orders;");
    TpccWorkload workload(*database, TpccScale::ForTest(), 1, 0);
    const TpccTransactionResult result =
        workload.Execute(TpccTransactionType::kOrderStatus);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("order-status order read returned no rows"),
              std::string::npos)
        << result.error;
    database->DeleteAll();
  }
  {
    // With every order_line gone, the line read returns no rows.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DELETE FROM order_line;");
    TpccWorkload workload(*database, TpccScale::ForTest(), 1, 0);
    const TpccTransactionResult result =
        workload.Execute(TpccTransactionType::kOrderStatus);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("order-status line read returned no rows"),
              std::string::npos)
        << result.error;
    database->DeleteAll();
  }
}

TEST(TpccWorkloadFailPathTest, DeliveryAndStockLevelAbortWhenRowsMissing) {
  auto MakeDb = []() -> std::unique_ptr<Database> {
    const std::string path = "tpcc_workload_delivery_test-" + RandomString();
    auto database = std::make_unique<Database>(path);
    std::string error;
    if (TpccWorkload::Initialize(*database, TpccScale::ForTest(), &error) !=
        Status::kSuccess) {
      ADD_FAILURE() << error;
      return nullptr;
    }
    return database;
  };
  auto RunSql = [](Database& database, std::string_view sql) {
    TransactionContext context = database.BeginContext();
    SqlEngine engine(database);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    if (!prepared.HasValue()) {
      ADD_FAILURE() << sql << "\n" << engine.LastError();
      context.Abort();
      return;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    if (context.PreCommit() != Status::kSuccess) {
      ADD_FAILURE() << "commit failed: " << sql;
    }
  };

  {
    // The queued orders are gone, so the delivery order read returns no rows.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DELETE FROM orders WHERE o_id >= 8;");
    TpccWorkload workload(*database, TpccScale::ForTest(), 1, 0);
    const TpccTransactionResult result =
        workload.Execute(TpccTransactionType::kDelivery);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("delivery order read returned no rows"),
              std::string::npos)
        << result.error;
    database->DeleteAll();
  }
  {
    // The order_line rows are gone, so the delivery line UPDATE affects zero
    // rows even though the queue row and the order row are still present.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DELETE FROM order_line;");
    TpccWorkload workload(*database, TpccScale::ForTest(), 1, 0);
    const TpccTransactionResult result =
        workload.Execute(TpccTransactionType::kDelivery);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("delivery line update affected too few rows"),
              std::string::npos)
        << result.error;
    database->DeleteAll();
  }
  {
    // The customer rows are gone, so the delivery customer UPDATE affects
    // zero rows after the SUM read succeeds.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DELETE FROM customer;");
    TpccWorkload workload(*database, TpccScale::ForTest(), 1, 0);
    const TpccTransactionResult result =
        workload.Execute(TpccTransactionType::kDelivery);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("delivery customer update affected too few "
                                "rows"),
              std::string::npos)
        << result.error;
    database->DeleteAll();
  }
  {
    // Dropping new_order makes the delivery queue SELECT fail to prepare.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DROP TABLE new_order;");
    TpccWorkload workload(*database, TpccScale::ForTest(), 1, 0);
    const TpccTransactionResult result =
        workload.Execute(TpccTransactionType::kDelivery);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("SELECT no_o_id FROM new_order"),
              std::string::npos)
        << result.error;
    database->DeleteAll();
  }
  {
    // A missing district makes the stock-level d_next_o_id read return no
    // rows.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DELETE FROM district WHERE d_w_id = 1 AND d_id = 1;");
    TpccWorkload workload(*database, TpccScale::ForTest(), 1, 0);
    const TpccTransactionResult result =
        workload.Execute(TpccTransactionType::kStockLevel);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("stock-level district read returned no rows"),
              std::string::npos)
        << result.error;
    database->DeleteAll();
  }
  {
    // Dropping order_line makes the stock-level line SELECT fail to prepare.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DROP TABLE order_line;");
    TpccWorkload workload(*database, TpccScale::ForTest(), 1, 0);
    const TpccTransactionResult result =
        workload.Execute(TpccTransactionType::kStockLevel);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("SELECT ol_i_id FROM order_line"),
              std::string::npos)
        << result.error;
    database->DeleteAll();
  }
  {
    // Dropping stock makes the batched low-stock SELECT fail to prepare.
    std::unique_ptr<Database> database = MakeDb();
    ASSERT_NE(database, nullptr);
    RunSql(*database, "DROP TABLE stock;");
    TpccWorkload workload(*database, TpccScale::ForTest(), 1, 0);
    const TpccTransactionResult result =
        workload.Execute(TpccTransactionType::kStockLevel);
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.error.find("SELECT s_i_id FROM stock"),
              std::string::npos)
        << result.error;
    database->DeleteAll();
  }
}

TEST_F(TpccWorkloadTest, NewOrderCreatesOrderLineRows) {
  const TpccScale scale = TpccScale::ForTest();
  std::string error;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kSuccess)
      << error;

  // NewOrder has a 1% rollback draw; retry with deterministic seeds until one
  // commits so the data assertions below are stable.
  TpccTransactionResult new_order;
  for (uint64_t seed = 1; seed <= 50 && !new_order.committed; ++seed) {
    TpccWorkload workload(*database_, scale, seed, 0);
    new_order = workload.Execute(TpccTransactionType::kNewOrder);
  }
  ASSERT_TRUE(new_order.committed) << new_order.error;

  const std::vector<Row> order =
      Run("SELECT o_ol_cnt, o_all_local FROM orders WHERE o_w_id = 1 AND "
          "o_d_id = 1 AND o_id = " +
          std::to_string(new_order.order_id) + ";");
  ASSERT_EQ(order.size(), 1);
  const int line_count = static_cast<int>(order[0][0].value.int_value);
  EXPECT_GE(line_count, scale.min_order_lines);
  EXPECT_LE(line_count, scale.max_order_lines);

  const std::vector<Row> line_rows =
      Run("SELECT COUNT(*) FROM order_line WHERE ol_w_id = 1 AND ol_d_id = 1 "
          "AND ol_o_id = " +
          std::to_string(new_order.order_id) + ";");
  ASSERT_EQ(line_rows.size(), 1);
  EXPECT_EQ(line_rows[0][0], Value(line_count));

  EXPECT_EQ(order[0][1], Value(1));

  const std::vector<Row> total =
      Run("SELECT SUM(ol_amount) FROM order_line WHERE ol_w_id = 1 AND "
          "ol_d_id = 1 AND ol_o_id = " +
          std::to_string(new_order.order_id) + ";");
  ASSERT_EQ(total.size(), 1);
  EXPECT_NEAR(total[0][0].value.double_value, new_order.amount,
              (0.01 * line_count) + 0.001);
}

TEST_F(TpccWorkloadTest, DeliveryWithEmptyQueueCommitsIdle) {
  const TpccScale scale = TpccScale::ForTest();
  std::string error;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kSuccess)
      << error;
  TpccWorkload workload(*database_, scale, 42, 0);

  // ForTest queues exactly new_orders_per_district = 3 orders per district;
  // one district means three deliveries drain it and later ones are idle.
  int total_delivered = 0;
  for (int i = 0; i < 5; ++i) {
    const TpccTransactionResult delivery =
        workload.Execute(TpccTransactionType::kDelivery);
    ASSERT_TRUE(delivery.committed) << delivery.error;
    total_delivered += delivery.delivered_orders;
  }
  EXPECT_EQ(total_delivered, 3);

  const std::vector<Row> queues = Run("SELECT COUNT(*) FROM new_order;");
  ASSERT_EQ(queues.size(), 1);
  EXPECT_EQ(queues[0][0], Value(0));
  // The fixture stamps carrier ids on already-delivered orders (o_id 1..7), so
  // the freshly delivered queued orders are precisely the last three.
  const std::vector<Row> delivered =
      Run("SELECT COUNT(*) FROM orders WHERE o_carrier_id IS NOT NULL AND "
          "o_id >= 8;");
  ASSERT_EQ(delivered.size(), 1);
  EXPECT_EQ(delivered[0][0], Value(3));
}

TEST_F(TpccWorkloadTest, PaymentAdjustsCustomerBalances) {
  const TpccScale scale = TpccScale::ForTest();
  std::string error;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kSuccess)
      << error;

  const std::vector<Row> before = Run("SELECT SUM(c_balance) FROM customer;");
  ASSERT_EQ(before.size(), 1);
  TpccWorkload workload(*database_, scale, 42, 0);
  const TpccTransactionResult payment =
      workload.Execute(TpccTransactionType::kPayment);
  ASSERT_TRUE(payment.committed) << payment.error;
  EXPECT_GT(payment.amount, 0.0);

  // Every customer starts at -10.00; a payment shifts exactly the recorded
  // (2-decimal rounded) amount out of the customer balance pool.
  const std::vector<Row> after = Run("SELECT SUM(c_balance) FROM customer;");
  ASSERT_EQ(after.size(), 1);
  EXPECT_NEAR(after[0][0].value.double_value,
              before[0][0].value.double_value - payment.amount, 0.01);
}

TEST_F(TpccWorkloadTest, TerminalBindsWarehouseAndOnlyStockLevelDistrict) {
  TpccScale scale = TpccScale::ForTest();
  scale.warehouses = 2;
  scale.districts_per_warehouse = 2;
  std::string error;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kSuccess)
      << error;

  // Every terminal keeps its home warehouse.  TPC-C randomizes Payment's
  // district per invocation (§2.5.1.2); only Stock-Level owns a unique fixed
  // (warehouse,district) pair (§2.8.1.1).
  TpccWorkload t0(*database_, scale, 1, 0);
  const auto t0_payment = t0.Execute(TpccTransactionType::kPayment);
  EXPECT_EQ(t0_payment.warehouse_id, 1);
  EXPECT_GE(t0_payment.district_id, 1);
  EXPECT_LE(t0_payment.district_id, 2);
  EXPECT_EQ(t0.Execute(TpccTransactionType::kStockLevel).district_id, 1);

  TpccWorkload t1(*database_, scale, 1, 1);
  EXPECT_EQ(t1.Execute(TpccTransactionType::kPayment).warehouse_id, 1);
  EXPECT_EQ(t1.Execute(TpccTransactionType::kStockLevel).district_id, 2);

  TpccWorkload t2(*database_, scale, 1, 2);
  EXPECT_EQ(t2.Execute(TpccTransactionType::kPayment).warehouse_id, 2);
  EXPECT_EQ(t2.Execute(TpccTransactionType::kStockLevel).district_id, 1);

  TpccWorkload t3(*database_, scale, 1, 3);
  EXPECT_EQ(t3.Execute(TpccTransactionType::kPayment).warehouse_id, 2);
  EXPECT_EQ(t3.Execute(TpccTransactionType::kStockLevel).district_id, 2);
}

TEST_F(TpccWorkloadTest, StockLevelRespectsDistinctItemWindow) {
  const TpccScale scale = TpccScale::ForTest();
  std::string error;
  ASSERT_EQ(TpccWorkload::Initialize(*database_, scale, &error),
            Status::kSuccess)
      << error;

  // StockLevel counts distinct order-line items in
  // [max(1, d_next_o_id - 20), d_next_o_id) whose stock is below the random
  // threshold, so the result can never exceed the window's item count.
  const std::vector<Row> next =
      Run("SELECT d_next_o_id FROM district WHERE d_w_id = 1 AND d_id = 1;");
  ASSERT_EQ(next.size(), 1);
  const int64_t next_order = next[0][0].value.int_value;
  const std::vector<Row> items =
      Run("SELECT COUNT(DISTINCT ol_i_id) FROM order_line WHERE ol_w_id = 1 "
          "AND ol_d_id = 1 AND ol_o_id >= 1 AND ol_o_id < " +
          std::to_string(next_order) + ";");
  ASSERT_EQ(items.size(), 1);
  const int64_t distinct_items = items[0][0].value.int_value;

  TpccWorkload workload(*database_, scale, 42, 0);
  const TpccTransactionResult stock =
      workload.Execute(TpccTransactionType::kStockLevel);
  ASSERT_TRUE(stock.committed) << stock.error;
  EXPECT_GE(stock.low_stock, 0);
  EXPECT_LE(stock.low_stock, distinct_items);
}

}  // namespace
}  // namespace tinylamb
