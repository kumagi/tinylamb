/**
 * Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0.
 */

#pragma once

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "benchmark/tpcds_fixture.hpp"
#include "benchmark/tpcds_queries.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "gtest/gtest.h"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

struct Q68Key {
  int64_t ticket{0};
  int64_t customer{0};
  int64_t address{0};
  std::string bought_city;

  bool operator<(const Q68Key& other) const {
    return std::tie(ticket, customer, address, bought_city) <
           std::tie(other.ticket, other.customer, other.address,
                    other.bought_city);
  }
};

struct Q68Expected {
  std::string last_name;
  std::string first_name;
  std::string current_city;
  std::string bought_city;
  int64_t ticket{0};
  double extended_price{0.0};
  double extended_tax{0.0};
  double list_price{0.0};
};

inline std::vector<Q68Expected> ComputeQ68Expected(
    const tpcds::GeneratedData& fixture) {
  std::unordered_map<int64_t, const Row*> dates;
  std::unordered_map<int64_t, const Row*> stores;
  std::unordered_map<int64_t, const Row*> households;
  std::unordered_map<int64_t, const Row*> addresses;
  std::unordered_map<int64_t, const Row*> customers;
  for (const Row& row : fixture.date_dim) {
    dates.emplace(row[0].value.int_value, &row);
  }
  for (const Row& row : fixture.store) {
    stores.emplace(row[0].value.int_value, &row);
  }
  for (const Row& row : fixture.household_demographics) {
    households.emplace(row[0].value.int_value, &row);
  }
  for (const Row& row : fixture.customer_address) {
    addresses.emplace(row[0].value.int_value, &row);
  }
  for (const Row& row : fixture.customer) {
    customers.emplace(row[0].value.int_value, &row);
  }

  std::map<Q68Key, Q68Expected> groups;
  for (const Row& sale : fixture.store_sales) {
    const auto date_it = dates.find(sale[0].value.int_value);
    const auto store_it = stores.find(sale[6].value.int_value);
    const auto household_it = households.find(sale[4].value.int_value);
    const auto address_it = addresses.find(sale[5].value.int_value);
    if (date_it == dates.end() || store_it == stores.end() ||
        household_it == households.end() || address_it == addresses.end()) {
      continue;
    }
    const Row& date = *date_it->second;
    const Row& store = *store_it->second;
    const Row& household = *household_it->second;
    const Row& address = *address_it->second;
    const int64_t year = date[4].value.int_value;
    const int64_t dom = date[7].value.int_value;
    const std::string city(store[7].value.varchar_value);
    if (dom < 1 || dom > 2 || year < 1999 || year > 2001 ||
        (city != "Midway" && city != "Fairview") ||
        (household[3].value.int_value != 4 &&
         household[4].value.int_value != 3)) {
      continue;
    }
    const Q68Key key{
        .ticket = sale[8].value.int_value,
        .customer = sale[2].value.int_value,
        .address = sale[5].value.int_value,
        .bought_city = std::string(address[2].value.varchar_value)};
    auto& group = groups[key];
    group.bought_city = key.bought_city;
    group.ticket = key.ticket;
    group.extended_price += sale[14].value.double_value;
    group.extended_tax += sale[17].value.double_value;
    group.list_price += sale[16].value.double_value;
  }

  std::vector<Q68Expected> expected;
  expected.reserve(groups.size());
  for (const auto& [key, group] : groups) {
    const auto customer_it = customers.find(key.customer);
    if (customer_it == customers.end()) {
      continue;
    }
    const Row& customer = *customer_it->second;
    const auto address_it = addresses.find(customer[4].value.int_value);
    if (address_it == addresses.end()) {
      continue;
    }
    const std::string current_city(
        (*address_it->second)[2].value.varchar_value);
    if (current_city == group.bought_city) {
      continue;
    }
    expected.push_back(
        Q68Expected{.last_name = std::string(customer[11].value.varchar_value),
                    .first_name = std::string(customer[10].value.varchar_value),
                    .current_city = current_city,
                    .bought_city = group.bought_city,
                    .ticket = group.ticket,
                    .extended_price = group.extended_price,
                    .extended_tax = group.extended_tax,
                    .list_price = group.list_price});
  }
  std::sort(expected.begin(), expected.end(),
            [](const Q68Expected& left, const Q68Expected& right) {
              return std::tie(left.last_name, left.ticket, left.first_name,
                              left.current_city, left.bought_city) <
                     std::tie(right.last_name, right.ticket, right.first_name,
                              right.current_city, right.bought_city);
            });
  if (expected.size() > 100) {
    expected.resize(100);
  }
  return expected;
}

class TpcdsQueryTest : public ::testing::Test {
 protected:
  void SetUp() override { SetUpWithFixture(tpcds::Generate(1)); }

  void SetUpWithFixture(tpcds::GeneratedData fixture) {
    const std::filesystem::path workspace = "tpcds_" + RandomString();
    std::error_code error;
    ASSERT_TRUE(std::filesystem::create_directory(workspace, error))
        << workspace << ": " << error.message();
    workspace_ = workspace;
    database_ = Database::Create((workspace_ / "fixture").string()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    for (const std::string_view ddl : tpcds::Ddl()) {
      RunDdl(ddl);
    }
    fixture_ = std::move(fixture);
    for (const std::string_view ddl : tpcds::Ddl()) {
      static constexpr std::string_view kCreate = "CREATE TABLE ";
      const size_t begin = ddl.find(kCreate) + kCreate.size();
      const size_t end = ddl.find(' ', begin);
      const std::string_view table = ddl.substr(begin, end - begin);
      InsertRows(table, tpcds::RowsOf(fixture_, table));
    }
    // Publish the loaded rows before ANALYZE: statistics gathered in a
    // separate transaction cannot see uncommitted inserts, and stale/empty
    // stats steer the multi-fact joins into degenerate plans.
    EXPECT_EQ(context_->PreCommit(), Status::kSuccess);
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    // ANALYZE mirrors the benchmark's load flow; without statistics the
    // multi-fact joins (q17/q18) fall back to prohibitively bad join orders.
    {
      SqlEngine analyzer(*database_);
      TransactionContext context = database_->BeginContext();
      StatusOr<QueryResult> result = analyzer.Execute(context, "ANALYZE;");
      EXPECT_TRUE(result.HasValue()) << analyzer.LastError();
      if (result.HasValue()) {
        result.Value().Drain();
      }
      EXPECT_EQ(context.PreCommit(), Status::kSuccess);
    }
  }

  void TearDown() override {
    context_.reset();
    database_.reset();
    if (!workspace_.empty()) {
      std::error_code error;
      std::filesystem::remove_all(workspace_, error);
      EXPECT_FALSE(error) << workspace_ << ": " << error.message();
    }
  }

  void RunDdl(std::string_view sql) {
    SqlEngine engine(*database_);
    TransactionContext context = database_->BeginContext();
    StatusOr<QueryResult> result = engine.Execute(context, sql);
    if (!result.HasValue()) {
      ADD_FAILURE() << sql << " -> " << engine.LastError();
      context.Abort();
      return;
    }
    result.Value().Drain();
    if (context.PreCommit() != Status::kSuccess) {
      ADD_FAILURE() << "DDL commit failed";
    }
  }

  // Batched commits (10k rows) mirror the benchmark's load flow so both
  // see the same page layout and the optimizer makes the same choices.
  void InsertRows(std::string_view table, const std::vector<Row>& rows) {
    constexpr size_t kBatch = 10000;
    for (size_t begin = 0; begin < rows.size(); begin += kBatch) {
      const size_t end = std::min(begin + kBatch, rows.size());
      for (size_t i = begin; i < end; ++i) {
        StatusOr<RowPosition> inserted = database_->GetTable(*context_, table)
                                             .MoveValue()
                                             .Insert(context_->txn_, rows[i]);
        ASSERT_TRUE(inserted.HasValue()) << table;
      }
      EXPECT_EQ(context_->PreCommit(), Status::kSuccess);
      context_ =
          std::make_unique<TransactionContext>(database_->BeginContext());
    }
  }

  // Fetches the query result rows; fails the test on any execution error.
  // Each statement runs in its own transaction (the same shape the
  // benchmark uses); reusing one long-lived transaction across the whole
  // suite stalls the streaming path on some multi-fact joins.
  std::vector<Row> RunSql(std::string_view sql) {
    TransactionContext context = database_->BeginContext();
    SqlEngine engine(*database_);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    if (!prepared.HasValue()) {
      ADD_FAILURE() << sql << " -> " << engine.LastError();
      context.Abort();
      return {};
    }
    std::vector<Row> rows;
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      rows.push_back(row);
      row = Row();
    }
    EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
    EXPECT_EQ(context.PreCommit(), Status::kSuccess) << sql;
    return rows;
  }

  std::string DumpSql(std::string_view sql) {
    TransactionContext context = database_->BeginReadOnlyContext();
    SqlEngine engine(*database_);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    if (!prepared.HasValue()) {
      ADD_FAILURE() << sql << " -> " << engine.LastError();
      return {};
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
    std::ostringstream dump;
    prepared.Value()->Dump(dump, 0);
    EXPECT_EQ(context.PreCommit(), Status::kSuccess) << sql;
    return dump.str();
  }

  std::string ExplainSql(std::string_view sql) {
    TransactionContext context = database_->BeginReadOnlyContext();
    SqlEngine engine(*database_);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    if (!prepared.HasValue()) {
      ADD_FAILURE() << sql << " -> " << engine.LastError();
      return {};
    }
    std::string plan;
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      if (row.values_.size() != 1 || row[0].IsNull() ||
          row[0].type != ValueType::kVarChar) {
        ADD_FAILURE() << "EXPLAIN returned a non-text row for " << sql;
        break;
      }
      plan.append(row[0].AsStringView());
      plan.push_back('\n');
    }
    EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
    EXPECT_EQ(context.PreCommit(), Status::kSuccess) << sql;
    return plan;
  }

  std::vector<Row> RunSqlReadOnly(std::string_view sql) {
    TransactionContext context = database_->BeginReadOnlyContext();
    SqlEngine engine(*database_);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    if (!prepared.HasValue()) {
      ADD_FAILURE() << sql << " -> " << engine.LastError();
      return {};
    }
    std::vector<Row> rows;
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      rows.push_back(row);
      row = Row();
    }
    EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
    EXPECT_EQ(context.PreCommit(), Status::kSuccess) << sql;
    return rows;
  }

  std::filesystem::path workspace_;
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  tpcds::GeneratedData fixture_;
};

}  // namespace tinylamb
