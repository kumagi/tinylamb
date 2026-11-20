/**
 * Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0.
 */

#include "query/tpcds_query_test.hpp"

namespace tinylamb {

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
  expected.reserve(fixture_.store_sales.size());
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
  expected.reserve(group_totals.size());
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

TEST_F(TpcdsQueryTest, LeanProvenTpcdsRewritesMatchRelationalFallback) {
  constexpr std::array<std::string_view, 16> kTargetNames = {
      "q1",  "q4",  "q5",  "q11", "q30", "q38", "q44", "q65",
      "q71", "q72", "q74", "q76", "q78", "q81", "q87", "q97"};
  auto exec_mode = [&](std::string_view sql, bool fallback) {
    TransactionContext context = database_->BeginReadOnlyContext();
    SqlEngine engine(*database_);
    engine.SetForceRelationalFallback(fallback);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    if (!prepared.HasValue()) {
      ADD_FAILURE() << sql << " -> " << engine.LastError();
      return std::vector<Row>{};
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
  };

  for (std::string_view target : kTargetNames) {
    const tpcds::TpcdsQuery* found = nullptr;
    for (const tpcds::TpcdsQuery& q : tpcds::kQueries) {
      if (q.name == target) {
        found = &q;
        break;
      }
    }
    ASSERT_NE(found, nullptr) << target;
    const std::vector<Row> opt_rows = exec_mode(found->sql, false);
    const std::vector<Row> ref_rows = exec_mode(found->sql, true);
    ASSERT_EQ(opt_rows.size(), ref_rows.size())
        << "Row count mismatch on " << target;
    if (found->total_order) {
      for (size_t r = 0; r < opt_rows.size(); ++r) {
        ASSERT_EQ(opt_rows[r].values_.size(), ref_rows[r].values_.size())
            << target << " row " << r;
        for (size_t c = 0; c < opt_rows[r].values_.size(); ++c) {
          const Value& a = opt_rows[r][c];
          const Value& b = ref_rows[r][c];
          ASSERT_EQ(a.IsNull(), b.IsNull())
              << target << " (" << r << "," << c << ")";
          if (!a.IsNull() && a.type == ValueType::kDouble &&
              b.type == ValueType::kDouble) {
            const double scale = std::max({1.0, std::abs(a.value.double_value),
                                           std::abs(b.value.double_value)});
            EXPECT_NEAR(a.value.double_value, b.value.double_value,
                        1e-6 * scale)
                << target << " (" << r << "," << c << ")";
          } else {
            EXPECT_EQ(a, b) << target << " (" << r << "," << c << ")";
          }
        }
      }
    }
  }
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
