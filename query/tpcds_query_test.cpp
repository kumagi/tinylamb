/**
 * Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0.
 */

// TPC-DS subset correctness: builds the small deterministic fixture from
// benchmark/tpcds_fixture.hpp in-memory, runs every ported query from
// benchmark/tpcds_queries.hpp, and cross-checks the aggregate semantics of a
// few queries against a plain C++ reference computed over the generated rows.

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdio>
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
namespace {

TEST(TpcdsManifestTest, RegisteredSubsetIsUniqueAndStable) {
  std::vector<std::string> expected{
      "q1",  "q2",  "q3",  "q6",  "q7",  "q9",  "q10", "q11", "q12",
      "q13", "q15", "q16", "q17", "q18", "q19", "q21", "q26", "q40",
      "q42", "q46", "q47", "q52", "q55", "q61", "q68", "q73", "q89"};
  std::vector<std::string> actual;
  actual.reserve(tpcds::kQueries.size());
  for (const tpcds::TpcdsQuery& query : tpcds::kQueries) {
    actual.emplace_back(query.name);
  }
  std::sort(actual.begin(), actual.end());
  std::sort(expected.begin(), expected.end());
  ASSERT_EQ(actual, expected);
}

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

std::vector<Q68Expected> ComputeQ68Expected(
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
  void SetUp() override {
    db_name_ = "tpcds_" + RandomString();
    database_ = Database::Create(db_name_).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    for (const std::string_view ddl : tpcds::Ddl()) {
      EXPECT_TRUE(RunDdl(ddl));
    }
    fixture_ = tpcds::Generate(1);
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
    engine_.reset();
    context_.reset();
    database_.reset();
    std::filesystem::remove(db_name_ + ".db");
    tinylamb::RemoveLogAndSegments(db_name_ + ".log");
    std::filesystem::remove(db_name_ + ".last_checkpoint");
  }

  bool RunDdl(std::string_view sql) {
    SqlEngine engine(*database_);
    TransactionContext context = database_->BeginContext();
    StatusOr<QueryResult> result = engine.Execute(context, sql);
    if (!result.HasValue()) {
      ADD_FAILURE() << sql << " -> " << engine.LastError();
      context.Abort();
      return false;
    }
    result.Value().Drain();
    if (context.PreCommit() != Status::kSuccess) {
      ADD_FAILURE() << "DDL commit failed";
      return false;
    }
    return true;
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
    context.PreCommit();
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
    context.PreCommit();
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
      plan += row[0].AsString();
      plan += '\n';
    }
    EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
    context.PreCommit();
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
    context.PreCommit();
    return rows;
  }

  std::string db_name_;
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
  tpcds::GeneratedData fixture_;
};

// Every ported TPC-DS query must execute over the fixture and stay
// deterministic across two runs. Explicit ORDER BY is compared positionally.
TEST_F(TpcdsQueryTest, AllQueriesExecuteDeterministically) {
  auto rows_equal = [](const Row& left, const Row& right) {
    if (left.values_.size() != right.values_.size()) {
      return false;
    }
    for (size_t c = 0; c < left.values_.size(); ++c) {
      const Value& x = left.values_[c];
      const Value& y = right.values_[c];
      if (x.type == ValueType::kDouble && y.type == ValueType::kDouble) {
        if (std::abs(x.value.double_value - y.value.double_value) >
            1e-6 * std::max(1.0, std::abs(x.value.double_value))) {
          return false;
        }
      } else if (x != y) {
        return false;
      }
    }
    return true;
  };
  for (const tpcds::TpcdsQuery& query : tpcds::kQueries) {
    const std::vector<Row> first = RunSql(query.sql);
    ASSERT_FALSE(first.empty()) << query.name << " returned no rows";
    const std::vector<Row> second = RunSql(query.sql);
    ASSERT_EQ(first.size(), second.size())
        << query.name << " returned different row counts";
    if (query.sql.find("ORDER BY") != std::string_view::npos &&
        query.sql.find("LIMIT") != std::string_view::npos) {
      for (size_t r = 0; r < first.size(); ++r) {
        EXPECT_TRUE(rows_equal(first[r], second[r]))
            << query.name << " ordered row mismatch at " << r;
      }
      continue;
    }
    auto row_key = [](const Row& row) {
      std::ostringstream text;
      for (size_t c = 0; c < row.values_.size(); ++c) {
        if (c != 0) {
          text << '|';
        }
        const Value& v = row.values_[c];
        if (v.type == ValueType::kDouble) {
          std::array<char, 32> buffer{};
          (void)std::snprintf(buffer.data(), buffer.size(), "%.9g",
                              v.value.double_value);
          text << buffer.data();
        } else {
          text << v.AsString();
        }
      }
      return text.str();
    };
    std::vector<std::pair<std::string, const Row*>> a;
    std::vector<std::pair<std::string, const Row*>> b;
    a.reserve(first.size());
    b.reserve(second.size());
    for (const Row& row : first) {
      a.emplace_back(row_key(row), &row);
    }
    for (const Row& row : second) {
      b.emplace_back(row_key(row), &row);
    }
    std::sort(a.begin(), a.end());
    std::sort(b.begin(), b.end());
    ASSERT_EQ(a.size(), b.size()) << query.name;
    for (size_t r = 0; r < a.size(); ++r) {
      EXPECT_TRUE(rows_equal(*a[r].second, *b[r].second))
          << query.name << " row mismatch: " << a[r].first << " vs "
          << b[r].first;
    }
  }
}

TEST_F(TpcdsQueryTest, AllQueriesAvoidCrossProducts) {
  for (const tpcds::TpcdsQuery& query : tpcds::kQueries) {
    const std::string plan = DumpSql(query.sql);
    SCOPED_TRACE(query.name);
    EXPECT_EQ(plan.find("Cross Join"), std::string::npos) << plan;
    EXPECT_EQ(plan.find("CrossJoin"), std::string::npos) << plan;
  }
}

TEST_F(TpcdsQueryTest, CascadesCoreQueriesAvoidCrossProducts) {
  constexpr std::array<std::string_view, 8> names{"q3",  "q7",  "q13", "q17",
                                                  "q26", "q42", "q52", "q55"};
  for (const std::string_view name : names) {
    const auto found = std::ranges::find_if(
        tpcds::kQueries,
        [name](const tpcds::TpcdsQuery& query) { return query.name == name; });
    ASSERT_NE(found, tpcds::kQueries.end()) << name;
    const std::string plan = DumpSql(found->sql);
    SCOPED_TRACE(name);
    EXPECT_EQ(plan.find("Cross Join"), std::string::npos) << plan;
    EXPECT_EQ(plan.find("RelationalExecutor"), std::string::npos) << plan;
    EXPECT_NE(plan.find("HashJoin"), std::string::npos) << plan;
  }
}

TEST_F(TpcdsQueryTest, LimitedQueriesUseBoundedFinishPath) {
  for (const tpcds::TpcdsQuery& query : tpcds::kQueries) {
    if (query.sql.find("LIMIT") == std::string_view::npos) {
      continue;
    }
    const std::string plan = DumpSql(query.sql);
    SCOPED_TRACE(query.name);
    EXPECT_TRUE(plan.find("TopNFinish") != std::string::npos ||
                plan.find("TopN (") != std::string::npos ||
                plan.find("topn_finish_uses=1") != std::string::npos)
        << plan;
  }
}

TEST_F(TpcdsQueryTest, Q13OrBranchesInferAllEquijoins) {
  const auto found = std::ranges::find_if(
      tpcds::kQueries,
      [](const tpcds::TpcdsQuery& query) { return query.name == "q13"; });
  ASSERT_NE(found, tpcds::kQueries.end());
  const std::string plan = ExplainSql("EXPLAIN " + std::string(found->sql));
  EXPECT_EQ(plan.find("Product: Cross Join"), std::string::npos) << plan;
  size_t hash_joins = 0;
  size_t position = 0;
  while ((position = plan.find("Product: Hash Join", position)) !=
         std::string::npos) {
    ++hash_joins;
    position += std::string_view("Product: Hash Join").size();
  }
  EXPECT_GE(hash_joins, 5U) << plan;
  for (const std::string_view table :
       {"store_sales", "store", "customer_demographics",
        "household_demographics", "customer_address", "date_dim"}) {
    EXPECT_NE(plan.find(table), std::string::npos) << table << "\n" << plan;
  }
}

TEST_F(TpcdsQueryTest, ExplainSeparatesHeuristicAndSelectedCore) {
  const auto found = std::ranges::find_if(
      tpcds::kQueries,
      [](const tpcds::TpcdsQuery& query) { return query.name == "q3"; });
  ASSERT_NE(found, tpcds::kQueries.end());
  const std::string plan = ExplainSql("EXPLAIN " + std::string(found->sql));
  constexpr std::string_view kHeuristic =
      "Relational Physical Plan (heuristic estimate; not selected)";
  constexpr std::string_view kSelected =
      "Selected Cascades Core (FROM + WHERE)";
  const size_t heuristic = plan.find(kHeuristic);
  const size_t selected = plan.find(kSelected);
  ASSERT_NE(heuristic, std::string::npos) << plan;
  ASSERT_NE(selected, std::string::npos) << plan;
  EXPECT_LT(heuristic, selected);
  EXPECT_NE(plan.find("TopNFinish"), std::string::npos) << plan;
  EXPECT_EQ(plan.find("estimated cost:"), std::string::npos) << plan;
  EXPECT_EQ(plan.find("CardinalityFeedback"), std::string::npos) << plan;
  EXPECT_EQ(plan.find("feedback_recorded=true"), std::string::npos) << plan;
  EXPECT_EQ(plan.find("Reoptimization"), std::string::npos) << plan;
}

// q18 executes and agrees with itself. The deterministic sweep above also
// runs it twice; this case keeps a direct hook on the ROLLUP query.
TEST_F(TpcdsQueryTest, RollupQueryExecutes) {
  const tpcds::TpcdsQuery* query = nullptr;
  for (const tpcds::TpcdsQuery& candidate : tpcds::kQueries) {
    if (candidate.name == "q18") {
      query = &candidate;
      break;
    }
  }
  ASSERT_NE(query, nullptr);
  const std::vector<Row> rows = RunSql(query->sql);
  EXPECT_FALSE(rows.empty());
  const std::string plan = DumpSql(query->sql);
  EXPECT_NE(plan.find("TopN"), std::string::npos) << plan;
}

TEST_F(TpcdsQueryTest, Q68MatchesCppReference) {
  const tpcds::TpcdsQuery* query = nullptr;
  for (const tpcds::TpcdsQuery& candidate : tpcds::kQueries) {
    if (candidate.name == "q68") {
      query = &candidate;
      break;
    }
  }
  ASSERT_NE(query, nullptr);
  const std::vector<Row> rows = RunSqlReadOnly(query->sql);
  const std::vector<Q68Expected> expected = ComputeQ68Expected(fixture_);
  // Sanity floor only: the exact group count tracks the fixture, whose
  // price values can shift in the last ulp under FP contraction differences
  // between compilers, moving a few rows across the q68 filter predicates.
  // The row-for-row match against the reference below is the real assertion.
  ASSERT_GE(expected.size(), 60U);
  ASSERT_EQ(rows.size(), expected.size());
  std::vector<bool> used(expected.size(), false);
  for (const Row& row : rows) {
    ASSERT_EQ(row.values_.size(), 8U);
    const auto found = std::find_if(
        expected.begin(), expected.end(), [&](const Q68Expected& item) {
          return !used[static_cast<size_t>(&item - expected.data())] &&
                 item.last_name == row[0].value.varchar_value &&
                 item.first_name == row[1].value.varchar_value &&
                 item.current_city == row[2].value.varchar_value &&
                 item.bought_city == row[3].value.varchar_value &&
                 item.ticket == row[4].value.int_value;
        });
    ASSERT_NE(found, expected.end());
    const size_t index = static_cast<size_t>(found - expected.begin());
    used[index] = true;
    const double scale =
        std::max({1.0, std::abs(found->extended_price),
                  std::abs(found->extended_tax), std::abs(found->list_price)});
    EXPECT_NEAR(row[5].value.double_value, found->extended_price, 1e-9 * scale);
    EXPECT_NEAR(row[6].value.double_value, found->extended_tax, 1e-9 * scale);
    EXPECT_NEAR(row[7].value.double_value, found->list_price, 1e-9 * scale);
  }
}

TEST_F(TpcdsQueryTest, Q42MatchesCppReference) {
  const auto query = std::ranges::find_if(
      tpcds::kQueries, [](const tpcds::TpcdsQuery& candidate) {
        return candidate.name == "q42";
      });
  ASSERT_NE(query, tpcds::kQueries.end());
  const std::vector<Row> rows = RunSql(query->sql);
  struct Key {
    int64_t year;
    int64_t category_id;
    bool operator==(const Key& other) const {
      return year == other.year && category_id == other.category_id;
    }
  };
  struct Entry {
    Key key{};
    double total{0};
    std::string category;
  };
  std::vector<Entry> expected;
  for (const Row& sale : fixture_.store_sales) {
    const auto date =
        std::find_if(fixture_.date_dim.begin(), fixture_.date_dim.end(),
                     [&](const Row& row) {
                       return row[0].value.int_value == sale[0].value.int_value;
                     });
    if (date == fixture_.date_dim.end() || (*date)[4].value.int_value != 2000 ||
        (*date)[6].value.int_value != 11) {
      continue;
    }
    const auto item = std::find_if(
        fixture_.item.begin(), fixture_.item.end(), [&](const Row& row) {
          return row[0].value.int_value == sale[1].value.int_value;
        });
    if (item == fixture_.item.end() || (*item)[13].value.int_value != 1) {
      continue;
    }
    const Key key{.year = (*date)[4].value.int_value,
                  .category_id = (*item)[9].value.int_value};
    const auto slot =
        std::find_if(expected.begin(), expected.end(),
                     [&](const Entry& entry) { return entry.key == key; });
    if (slot == expected.end()) {
      expected.push_back(
          Entry{.key = key,
                .total = sale[14].value.double_value,
                .category = std::string((*item)[10].value.varchar_value)});
    } else {
      slot->total += sale[14].value.double_value;
    }
  }
  std::sort(expected.begin(), expected.end(),
            [](const Entry& left, const Entry& right) {
              if (left.total != right.total) {
                return left.total > right.total;
              }
              if (left.key.year != right.key.year) {
                return left.key.year < right.key.year;
              }
              if (left.key.category_id != right.key.category_id) {
                return left.key.category_id < right.key.category_id;
              }
              return left.category < right.category;
            });
  if (expected.size() > 100) {
    expected.resize(100);
  }
  ASSERT_EQ(rows.size(), expected.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    ASSERT_EQ(rows[i].values_.size(), 4U);
    EXPECT_EQ(rows[i][0].value.int_value, expected[i].key.year);
    EXPECT_EQ(rows[i][1].value.int_value, expected[i].key.category_id);
    EXPECT_EQ(rows[i][2].value.varchar_value, expected[i].category);
    const double scale = std::max(1.0, std::abs(expected[i].total));
    EXPECT_NEAR(rows[i][3].value.double_value, expected[i].total, 1e-9 * scale);
  }
}

TEST_F(TpcdsQueryTest, Q89WindowAverageMatchesCppReference) {
  const auto query = std::ranges::find_if(
      tpcds::kQueries, [](const tpcds::TpcdsQuery& candidate) {
        return candidate.name == "q89";
      });
  ASSERT_NE(query, tpcds::kQueries.end());
  const std::vector<Row> rows = RunSqlReadOnly(query->sql);
  struct GroupKey {
    std::string category;
    std::string klass;
    std::string brand;
    std::string store;
    std::string company;
    int64_t moy{0};
    bool operator<(const GroupKey& other) const {
      return std::tie(category, klass, brand, store, company, moy) <
             std::tie(other.category, other.klass, other.brand, other.store,
                      other.company, other.moy);
    }
  };
  struct Partition {
    double total{0.0};
    size_t groups{0};
  };
  struct Expected {
    GroupKey key;
    double sum{0.0};
    double average{0.0};
  };
  std::map<GroupKey, double> group_totals;
  for (const Row& sale : fixture_.store_sales) {
    const auto date =
        std::find_if(fixture_.date_dim.begin(), fixture_.date_dim.end(),
                     [&](const Row& row) {
                       return row[0].value.int_value == sale[0].value.int_value;
                     });
    if (date == fixture_.date_dim.end() || (*date)[4].value.int_value != 1999) {
      continue;
    }
    const auto item = std::find_if(
        fixture_.item.begin(), fixture_.item.end(), [&](const Row& row) {
          return row[0].value.int_value == sale[1].value.int_value;
        });
    const auto store = std::find_if(
        fixture_.store.begin(), fixture_.store.end(), [&](const Row& row) {
          return row[0].value.int_value == sale[6].value.int_value;
        });
    if (item == fixture_.item.end() || store == fixture_.store.end()) {
      continue;
    }
    const std::string category((*item)[10].value.varchar_value);
    const std::string klass((*item)[8].value.varchar_value);
    const bool electronics_family =
        (category == "Books" || category == "Electronics" ||
         category == "Sports") &&
        (klass == "computers" || klass == "stereo" || klass == "football");
    const bool apparel_family =
        (category == "Men" || category == "Jewelry" || category == "Women") &&
        (klass == "shirts" || klass == "birdal" || klass == "dresses");
    if (!electronics_family && !apparel_family) {
      continue;
    }
    const GroupKey key{.category = category,
                       .klass = klass,
                       .brand = std::string((*item)[6].value.varchar_value),
                       .store = std::string((*store)[2].value.varchar_value),
                       .company = std::string((*store)[8].value.varchar_value),
                       .moy = (*date)[6].value.int_value};
    group_totals[key] += sale[12].value.double_value;
  }
  std::map<std::tuple<std::string, std::string, std::string, std::string>,
           Partition>
      partitions;
  for (const auto& [key, total] : group_totals) {
    auto& partition = partitions[std::make_tuple(key.category, key.brand,
                                                 key.store, key.company)];
    partition.total += total;
    ++partition.groups;
  }
  std::vector<Expected> expected;
  for (const auto& [key, sum] : group_totals) {
    const auto& partition = partitions.at(
        std::make_tuple(key.category, key.brand, key.store, key.company));
    const double average =
        partition.total / static_cast<double>(partition.groups);
    if (average != 0.0 && std::abs(sum - average) / average > 0.1) {
      expected.push_back(Expected{.key = key, .sum = sum, .average = average});
    }
  }
  std::sort(expected.begin(), expected.end(),
            [](const Expected& left, const Expected& right) {
              const double left_delta = left.sum - left.average;
              const double right_delta = right.sum - right.average;
              if (left_delta != right_delta) {
                return left_delta < right_delta;
              }
              return left.key.store < right.key.store;
            });
  if (expected.size() > 100) {
    expected.resize(100);
  }
  ASSERT_FALSE(expected.empty());
  ASSERT_EQ(rows.size(), expected.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    ASSERT_EQ(rows[i].values_.size(), 8U);
    const Expected& item = expected[i];
    EXPECT_EQ(rows[i][0].value.varchar_value, item.key.category);
    EXPECT_EQ(rows[i][1].value.varchar_value, item.key.klass);
    EXPECT_EQ(rows[i][2].value.varchar_value, item.key.brand);
    EXPECT_EQ(rows[i][3].value.varchar_value, item.key.store);
    EXPECT_EQ(rows[i][4].value.varchar_value, item.key.company);
    EXPECT_EQ(rows[i][5].value.int_value, item.key.moy);
    const double scale =
        std::max({1.0, std::abs(item.sum), std::abs(item.average)});
    EXPECT_NEAR(rows[i][6].value.double_value, item.sum, 1e-9 * scale);
    EXPECT_NEAR(rows[i][7].value.double_value, item.average, 1e-9 * scale);
  }
}

}  // namespace
}  // namespace tinylamb
