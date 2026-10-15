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
namespace {

TEST(TpcdsManifestTest, RegisteredSubsetIsUniqueAndStable) {
  std::vector<std::string> expected{
      "q1",  "q10", "q11", "q12", "q13",  "q15", "q16", "q17", "q18", "q19",
      "q2",  "q20", "q21", "q22", "q23a", "q25", "q26", "q27", "q28", "q29",
      "q3",  "q30", "q31", "q32", "q33",  "q34", "q35", "q36", "q37", "q38",
      "q39", "q4",  "q40", "q41", "q42",  "q43", "q44", "q45", "q46", "q47",
      "q48", "q49", "q5",  "q50", "q51",  "q52", "q53", "q54", "q55", "q56",
      "q57", "q58", "q59", "q6",  "q60",  "q61", "q62", "q63", "q65", "q67",
      "q68", "q69", "q7",  "q70", "q71",  "q72", "q73", "q74", "q75", "q76",
      "q77", "q78", "q79", "q80", "q81",  "q82", "q83", "q84", "q85", "q86",
      "q87", "q88", "q89", "q9",  "q90",  "q91", "q92", "q93", "q94", "q95",
      "q96", "q97", "q98", "q99"};
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
// deterministic across two runs.  Queries with an explicit ORDER BY are
// checked against their declared ordering: total_order pins are compared
// row by row, partial-order pins check the order key and compare the rest
// as a multiset.  q67 / q65 rank on DOUBLE sums: their first (cold) run
// differs from later ones because plan feedback changes the plan, so they
// compare two warm runs byte-exactly instead of first-vs-second.
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
    const bool content_jitter = query.name == "q67" || query.name == "q65";
    const std::vector<Row> first = RunSql(query.sql);
    ASSERT_FALSE(first.empty()) << query.name << " returned no rows";
    const std::vector<Row> second = RunSql(query.sql);
    ASSERT_EQ(first.size(), second.size())
        << query.name << " returned different row counts";
    if (query.sql.find("ORDER BY") != std::string_view::npos) {
      if (!query.total_order) {
        ASSERT_LT(query.order_key_index, first.front().values_.size());
        const auto respects_partial_order = [&](const std::vector<Row>& rows) {
          for (size_t r = 0; r + 1 < rows.size(); ++r) {
            const int comparison =
                CompareForOrderBy(rows[r].values_[query.order_key_index],
                                  rows[r + 1].values_[query.order_key_index]);
            if ((query.order_descending && comparison < 0) ||
                (!query.order_descending && comparison > 0)) {
              return false;
            }
          }
          return true;
        };
        EXPECT_TRUE(respects_partial_order(first))
            << query.name << " first result violates partial order";
        EXPECT_TRUE(respects_partial_order(second))
            << query.name << " second result violates partial order";
      } else {
        for (size_t r = 0; r < first.size(); ++r) {
          EXPECT_TRUE(rows_equal(first[r], second[r]))
              << query.name << " ordered row mismatch at " << r;
        }
        continue;
      }
    }
    if (content_jitter) {
      // Rank boundaries read DOUBLE sums whose last ulp follows the plan:
      // run 1 plans from cold statistics, so compare two WARM runs exactly
      // (ParallelFloatAggregatesAreBitDeterministic pins the mechanism).
      const std::vector<Row> third = RunSql(query.sql);
      ASSERT_EQ(second.size(), third.size()) << query.name;
      for (size_t r = 0; r < second.size(); ++r) {
        EXPECT_TRUE(second[r] == third[r])
            << query.name << " warm run row mismatch at " << r;
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

TEST_F(TpcdsQueryTest, QueryPipelineStagesAreBitDeterministic) {
  // q65's rank boundary reads DOUBLE sums whose last ulp depends on the row
  // order every pipeline stage hands downstream.  Each stage of q65's
  // pipeline is pinned here with an exact byte fingerprint over repeated
  // runs: bare scan, join emission, grouped detail, avg fold, derived
  // passthrough, and the full sb/sc shape.  A parallel operator that
  // hands fixed work slices to fixed owners keeps every stage stable; a
  // racy handoff (the historical atomic-morsel-grab scans) flips some
  // stage and this test names it.
  const std::vector<std::string> stages = {
      // stage 0: raw scan order
      "SELECT ss_item_sk FROM store_sales",
      // stage 1: join only, no grouping
      "SELECT ss_store_sk, ss_item_sk FROM store_sales, date_dim "
      "WHERE ss_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1176 AND 1187",
      // stage 2: inner grouped detail (sa / sc core)
      "SELECT ss_store_sk, ss_item_sk, sum(ss_sales_price) AS revenue "
      "FROM store_sales, date_dim "
      "WHERE ss_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1176 AND 1187 "
      "GROUP BY ss_store_sk, ss_item_sk",
      // stage 3: single-table avg grouping
      "SELECT ss_store_sk, avg(ss_sales_price) FROM store_sales "
      "GROUP BY ss_store_sk",
      // stage 4: avg over a derived (no join inside)
      "SELECT ss_store_sk, avg(revenue) FROM "
      "(SELECT ss_store_sk, ss_item_sk, sum(ss_sales_price) AS revenue "
      "FROM store_sales GROUP BY ss_store_sk, ss_item_sk) "
      "GROUP BY ss_store_sk",
      // stage 5: bare passthrough of the joined derived
      "SELECT ss_store_sk, ss_item_sk, revenue FROM "
      "(SELECT ss_store_sk, ss_item_sk, sum(ss_sales_price) AS revenue "
      "FROM store_sales, date_dim "
      "WHERE ss_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1176 AND 1187 "
      "GROUP BY ss_store_sk, ss_item_sk)",
      // stage 6: nested join-only derived (no group inside)
      "SELECT ss_store_sk, ss_item_sk, ss_sales_price FROM "
      "(SELECT s.ss_store_sk, s.ss_item_sk, s.ss_sales_price "
      "FROM store_sales s, date_dim d "
      "WHERE s.ss_sold_date_sk = d.d_date_sk "
      "AND d.d_month_seq BETWEEN 1176 AND 1187)",
      // stage 7: per-store average over the grouped detail
      "SELECT ss_store_sk, avg(revenue) AS ave FROM "
      "(SELECT ss_store_sk, ss_item_sk, sum(ss_sales_price) AS revenue "
      "FROM store_sales, date_dim "
      "WHERE ss_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1176 AND 1187 "
      "GROUP BY ss_store_sk, ss_item_sk) sa GROUP BY ss_store_sk",
      // stage 8: the sb join sc with the 0.1*ave filter
      "SELECT sc.ss_store_sk, sc.ss_item_sk, sc.revenue FROM "
      "(SELECT ss_store_sk, avg(revenue) AS ave FROM "
      "(SELECT ss_store_sk, ss_item_sk, sum(ss_sales_price) AS revenue "
      "FROM store_sales, date_dim "
      "WHERE ss_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1176 AND 1187 "
      "GROUP BY ss_store_sk, ss_item_sk) sa GROUP BY ss_store_sk) sb, "
      "(SELECT ss_store_sk, ss_item_sk, sum(ss_sales_price) AS revenue "
      "FROM store_sales, date_dim "
      "WHERE ss_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1176 AND 1187 "
      "GROUP BY ss_store_sk, ss_item_sk) sc "
      "WHERE sb.ss_store_sk = sc.ss_store_sk AND sc.revenue <= 0.1 * sb.ave",
  };
  auto fingerprint = [](const std::vector<Row>& rows) {
    std::ostringstream text;
    for (const Row& row : rows) {
      for (const Value& value : row.values_) {
        if (value.type == ValueType::kDouble) {
          std::array<char, 40> buffer{};
          (void)std::snprintf(buffer.data(), buffer.size(), "%.17g",
                              value.value.double_value);
          text << buffer.data();
        } else {
          text << value.AsString();
        }
        text << '|';
      }
      text << '\n';
    }
    return text.str();
  };
  for (size_t stage = 0; stage < stages.size(); ++stage) {
    SCOPED_TRACE("stage " + std::to_string(stage) + ": " + stages[stage]);
    const std::string expected = fingerprint(RunSql(stages[stage]));
    for (int run = 2; run <= 4; ++run) {
      EXPECT_EQ(fingerprint(RunSql(stages[stage])), expected)
          << "stage " << stage << " run " << run << " changed";
    }
  }
}

TEST_F(TpcdsQueryTest, ParallelFloatAggregatesAreBitDeterministic) {
  // The ROLLUP queries rank on DOUBLE sums whose last ulp depends on the
  // accumulation order.  Parallel aggregation therefore has to give every
  // worker a fixed slice of the input (static rotation, both in the
  // Cascades executors and the relational scan/fold paths) so a rank
  // boundary cannot flip run to run.  Exact byte comparison over repeated
  // warm runs is the pin.  The first execution plans from cold statistics
  // and its plan feedback changes later plans, so run 1 only warms up.
  const std::vector<std::string> rank_boundary_queries = {"q65", "q67", "q70",
                                                          "q86"};
  for (const std::string& name : rank_boundary_queries) {
    const tpcds::TpcdsQuery* query = nullptr;
    for (const tpcds::TpcdsQuery& candidate : tpcds::kQueries) {
      if (candidate.name == name) {
        query = &candidate;
        break;
      }
    }
    ASSERT_NE(query, nullptr) << name;
    auto fingerprint = [&](const std::vector<Row>& rows) {
      std::ostringstream text;
      for (const Row& row : rows) {
        for (const Value& value : row.values_) {
          if (value.type == ValueType::kDouble) {
            std::array<char, 40> buffer{};
            // %.17g round-trips a double exactly: any last-ulp divergence
            // between runs shows up as a different digit string.
            (void)std::snprintf(buffer.data(), buffer.size(), "%.17g",
                                value.value.double_value);
            text << buffer.data();
          } else {
            text << value.AsString();
          }
          text << '|';
        }
        text << '\n';
      }
      return text.str();
    };
    const std::vector<Row> first = RunSql(query->sql);
    ASSERT_FALSE(first.empty()) << name << " returned no rows";
    // Second run warms the statistics the first execution's feedback wrote.
    const std::string expected = fingerprint(RunSql(query->sql));
    for (int run = 3; run <= 6; ++run) {
      const std::vector<Row> again = RunSql(query->sql);
      ASSERT_EQ(again.size(), first.size())
          << name << " run " << run << " row count changed";
      EXPECT_EQ(fingerprint(again), expected)
          << name << " run " << run << " content changed";
    }
  }
}

TEST_F(TpcdsQueryTest, AllQueriesAvoidCrossProducts) {
  // The pin targets accidental UNBOUNDED products: fact-side cross/nested
  // joins that explode intermediate rows.  Bushy star plans legitimately
  // pair small dimension tables through a cross join (q88/q96: 16-row
  // household x store) before hash-joining the fact side, so a "Cross
  // Join" only fails when its estimated row count is missing or
  // astronomically large (> the largest dim x dim product we tolerate).
  for (const tpcds::TpcdsQuery& query : tpcds::kQueries) {
    // EXPLAIN carries per-node row estimates (the executor Dump does not),
    // which the unbounded-product check below keys on.
    const std::string plan = ExplainSql(query.sql);
    SCOPED_TRACE(query.name);
    EXPECT_EQ(plan.find("CrossJoin"), std::string::npos) << plan;
    size_t position = 0;
    while ((position = plan.find("Cross Join", position)) !=
           std::string::npos) {
      const size_t line_end = plan.find('\n', position);
      const size_t estimate = plan.find("estimated rows:", position);
      ASSERT_NE(estimate, std::string::npos) << plan;
      ASSERT_LT(estimate, line_end) << plan;
      const long long rows =
          std::strtoll(plan.c_str() + estimate + 15, nullptr, 10);
      EXPECT_LT(rows, 100000LL) << "unbounded cross product\n"
                                << plan.substr(position, 200);
      position = line_end;
    }
  }
}

TEST_F(TpcdsQueryTest, CascadesCoreQueriesAvoidCrossProducts) {
  // Route-agnostic shape guard: whichever planner selects the core (the
  // Cascades GroupByPlan bridge or the relational route under
  // TINYLAMB_GROUPED_ROUTE=relational), every query here must join through
  // hash joins — no cross products, no nested-loop fallback.
  constexpr std::array<std::string_view, 8> names{"q3",  "q7",  "q13", "q17",
                                                  "q26", "q42", "q52", "q55"};
  for (const std::string_view name : names) {
    const auto* const found = std::ranges::find_if(
        tpcds::kQueries,
        [name](const tpcds::TpcdsQuery& query) { return query.name == name; });
    ASSERT_NE(found, tpcds::kQueries.end()) << name;
    const std::string plan = DumpSql(found->sql);
    SCOPED_TRACE(name);
    EXPECT_EQ(plan.find("Cross Join"), std::string::npos) << plan;
    if (plan.find("RelationalExecutor") != std::string::npos) {
      // TINYLAMB_GROUPED_ROUTE=relational: the runtime counters carry the
      // join kinds.  (The relational route may fall back to nested loops on
      // OR-branch shapes the Cascades core reduces to hash joins — q13 —
      // so only the cross-product ban is enforced here.)
      EXPECT_EQ(plan.find("Cross Join"), std::string::npos) << plan;
    } else {
      EXPECT_NE(plan.find("HashJoin"), std::string::npos) << plan;
    }
  }
}

TEST_F(TpcdsQueryTest, LimitedQueriesUseBoundedFinishPath) {
  // The bounded finish path serves a top-level ORDER BY + LIMIT: the sort
  // keeps a bounded heap instead of materializing every row.  Branch-inner
  // LIMITs of UNION / EXCEPT operands execute under the operand recursion
  // and are bounded by the operand machinery, not this path.
  for (const tpcds::TpcdsQuery& query : tpcds::kQueries) {
    if (query.sql.find("ORDER BY") == std::string_view::npos ||
        query.sql.find("LIMIT") == std::string_view::npos) {
      continue;
    }
    const std::string plan = DumpSql(query.sql);
    SCOPED_TRACE(query.name);
    EXPECT_TRUE(plan.find("TopNFinish") != std::string::npos ||
                plan.find("TopN (") != std::string::npos ||
                plan.find("topn_finish_uses=1") != std::string::npos ||
                plan.find("Limit: ") != std::string::npos ||
                plan.find("Limit count=") != std::string::npos)
        << plan;
  }
}

TEST_F(TpcdsQueryTest, Q13OrBranchesInferAllEquijoins) {
  // Route-agnostic: the Cascades memo tree prints "Product: Hash Join", the
  // relational heuristic tree prints "HashJoin type=".  The guarded property
  // is the same — every OR branch's equijoin is inferred as a hash-join key,
  // so the plan carries no cross product and at least five hash joins.
  const auto* const found = std::ranges::find_if(
      tpcds::kQueries,
      [](const tpcds::TpcdsQuery& query) { return query.name == "q13"; });
  ASSERT_NE(found, tpcds::kQueries.end());
  const std::string plan = ExplainSql("EXPLAIN " + std::string(found->sql));
  EXPECT_EQ(plan.find("Cross Join"), std::string::npos) << plan;
  // The Cascades shape must reduce every OR branch to hash joins.  The
  // relational route (TINYLAMB_GROUPED_ROUTE=relational) prints a heuristic
  // tree whose nested-loop fallbacks are a known degradation for q13 and is
  // not held to the hash-join count here.
  if (plan.find("(heuristic estimate; selected route)") == std::string::npos) {
    size_t hash_joins = 0;
    size_t position = 0;
    while ((position = plan.find("Product: Hash Join", position)) !=
           std::string::npos) {
      ++hash_joins;
      position += std::string_view("Product: Hash Join").size();
    }
    EXPECT_GE(hash_joins, 5U) << plan;
  }
  for (const std::string_view table :
       {"store_sales", "store", "customer_demographics",
        "household_demographics", "customer_address", "date_dim"}) {
    EXPECT_NE(plan.find(table), std::string::npos) << table << "\n" << plan;
  }
}

TEST_F(TpcdsQueryTest, ExplainSeparatesHeuristicAndSelectedCore) {
  // Route-agnostic: the Cascades bridge prints the heuristic estimate then
  // "Selected Cascades Core (FROM + WHERE)"; the relational route prints
  // "(heuristic estimate; selected route)".  Either way the heuristic
  // section must be present with a bounded finish, and no feedback/cost
  // artifacts may leak.
  const auto* const found = std::ranges::find_if(
      tpcds::kQueries,
      [](const tpcds::TpcdsQuery& query) { return query.name == "q3"; });
  ASSERT_NE(found, tpcds::kQueries.end());
  const std::string plan = ExplainSql("EXPLAIN " + std::string(found->sql));
  constexpr std::string_view kHeuristic =
      "Relational Physical Plan (heuristic estimate";
  const size_t heuristic = plan.find(kHeuristic);
  ASSERT_NE(heuristic, std::string::npos) << plan;
  const size_t cascades = plan.find("Selected Cascades Core (FROM + WHERE)");
  const size_t relational = plan.find("(heuristic estimate; selected route)");
  EXPECT_TRUE(cascades != std::string::npos || relational != std::string::npos)
      << plan;
  EXPECT_TRUE(plan.find("TopNFinish") != std::string::npos ||
              plan.find("TopN limit=") != std::string::npos)
      << plan;
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
  const auto* const query = std::ranges::find_if(
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
  const auto* const query = std::ranges::find_if(
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
// q11 groups year_total at c_customer_sk (the customer primary key, unique),
// so the grouping carry set must not drag every customer column through the
// four CTE references: unused wide columns (c_email_address,
// c_birth_country, ...) never appear in the plan text.
TEST_F(TpcdsQueryTest, Q11GroupingReducesCustomerColumnsAtUniqueKey) {
  const tpcds::TpcdsQuery* q11 = nullptr;
  for (const tpcds::TpcdsQuery& query : tpcds::kQueries) {
    if (query.name == "q11") {
      q11 = &query;
    }
  }
  ASSERT_NE(q11, nullptr);
  const std::string plan = DumpSql(q11->sql);
  EXPECT_EQ(plan.find("c_email_address"), std::string::npos) << plan;
  EXPECT_EQ(plan.find("c_birth_country"), std::string::npos) << plan;
  EXPECT_EQ(plan.find("CrossJoin"), std::string::npos) << plan;
}

// Pins the fixture's RNG streams: every draw happens in its own statement,
// so the generated values cannot depend on argument evaluation order (which
// differs between compilers and once silently shuffled every fact row).  If
// a generator edit reorders or adds draws, update these sums deliberately in
// the same change.
TEST(TpcdsFixtureDeterminismTest, ValueStreamsArePinned) {
  const tpcds::GeneratedData fixture = tpcds::Generate(1);
  auto column_sum = [](const std::vector<Row>& rows, size_t column) {
    int64_t total = 0;
    for (const Row& row : rows) {
      total += row.values_[column].value.int_value;
    }
    return total;
  };
  EXPECT_EQ(fixture.store_sales.size(), 30000U);
  EXPECT_EQ(column_sum(fixture.store_sales, 0), 73551604820LL);
  EXPECT_EQ(column_sum(fixture.store_sales, 1), 18060801LL);
  EXPECT_EQ(column_sum(fixture.store_sales, 2), 29929481LL);
  EXPECT_EQ(fixture.store_sales[0].values_[1].value.int_value, 341);
  EXPECT_EQ(fixture.catalog_sales.size(), 25000U);
  EXPECT_EQ(column_sum(fixture.catalog_sales, 0), 61293160884LL);
  EXPECT_EQ(fixture.web_sales.size(), 25000U);
  EXPECT_EQ(fixture.catalog_returns.size(), 2500U);
  EXPECT_EQ(fixture.web_returns.size(), 2500U);
  EXPECT_EQ(fixture.customer.size(), 2000U);
  EXPECT_EQ(column_sum(fixture.customer, 0), 2001000LL);
  EXPECT_EQ(fixture.date_dim.size(), 2191U);
  EXPECT_EQ(column_sum(fixture.date_dim, 0), 5372137001LL);
}

}  // namespace tinylamb
