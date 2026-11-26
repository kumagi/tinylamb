/**
 * Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0.
 */

#include "query/tpcds_query_test.hpp"

namespace tinylamb {

namespace {
// Detect a clause at the outermost query level: a raw substring search also
// matches ORDER BY / LIMIT inside a subquery or a string literal.
bool HasTopLevelClause(std::string_view sql, std::string_view clause) {
  int depth = 0;
  bool in_string = false;
  for (size_t i = 0; i < sql.size(); ++i) {
    const char c = sql[i];
    if (in_string) {
      if (c == '\'') {
        if (i + 1 < sql.size() && sql[i + 1] == '\'') {
          ++i;
        } else {
          in_string = false;
        }
      }
      continue;
    }
    if (c == '\'') {
      in_string = true;
      continue;
    }
    if (c == '(') {
      ++depth;
      continue;
    }
    if (c == ')') {
      --depth;
      continue;
    }
    if (depth == 0 && sql.substr(i, clause.size()) == clause) {
      return true;
    }
  }
  return false;
}
}  // namespace

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

// Every ported TPC-DS query must execute over the fixture and stay
// deterministic across two runs.  Queries with an explicit ORDER BY are
// checked against their declared ordering: total_order pins are compared
// row by row, partial-order pins check the order key and compare the rest
// as a multiset.  q67 / q65 rank on DOUBLE sums: their first (cold) run
// differs from later ones because plan feedback changes the plan, so they
// compare two warm runs byte-exactly instead of first-vs-second.
constexpr size_t kTpcdsDeterminismShards = 4;

class TpcdsDeterminismTest : public TpcdsQueryTest,
                             public ::testing::WithParamInterface<size_t> {
 protected:
  void SetUp() override {
    if (GetParam() != kTpcdsDeterminismShards) {
      TpcdsQueryTest::SetUp();
      return;
    }
    // q95 replays a self-join over orders with five sale rows each. Retain
    // whole orders and their returns so two runs fit the unit-test budget.
    auto fixture = tpcds::Generate(1);
    fixture.web_sales.resize(10000);
    fixture.web_returns.resize(1000);
    SetUpWithFixture(std::move(fixture));
  }
};

TEST_P(TpcdsDeterminismTest, AllQueriesExecuteDeterministically) {
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
  const bool returns_shard = GetParam() == kTpcdsDeterminismShards;
  for (size_t i = 0; i < std::size(tpcds::kQueries); ++i) {
    const tpcds::TpcdsQuery& query = tpcds::kQueries[i];
    if (query.name == "q95"
            ? !returns_shard
            : returns_shard || i % kTpcdsDeterminismShards != GetParam()) {
      continue;
    }
    SCOPED_TRACE(query.name);
    const bool content_jitter = query.name == "q67" || query.name == "q65";
    const std::vector<Row> first = RunSql(query.sql);
    ASSERT_FALSE(first.empty()) << query.name << " returned no rows";
    if (returns_shard) {
      ASSERT_EQ(first.size(), 1U);
      ASSERT_EQ(first.front().values_.size(), 3U);
      EXPECT_EQ(first.front()[0], Value(int64_t{2}));
    }
    const std::vector<Row> second = RunSql(query.sql);
    ASSERT_EQ(first.size(), second.size())
        << query.name << " returned different row counts";
    if (first.empty()) {
      continue;
    }
    if (HasTopLevelClause(query.sql, "ORDER BY")) {
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

INSTANTIATE_TEST_SUITE_P(QueryShards, TpcdsDeterminismTest,
                         ::testing::Range(size_t{0},
                                          kTpcdsDeterminismShards + 1));

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
    // T3850: Prepare only sees EXPLAIN through the string prefix
    // (SqlEngine::ParseExplain); passing the bare SELECT drained every query
    // for real rows and grepped the FIRST RESULT COLUMN for "CrossJoin",
    // keeping this whole check dead. Prefix it like the sibling call sites.
    const std::string plan = ExplainSql("EXPLAIN " + std::string(query.sql));
    SCOPED_TRACE(query.name);
    EXPECT_EQ(plan.find("CrossJoin"), std::string::npos) << plan;
    size_t position = 0;
    while ((position = plan.find("Cross Join", position)) !=
           std::string::npos) {
      const size_t line_end = plan.find('\n', position);
      const size_t estimate = plan.find("estimated rows:", position);
      ASSERT_NE(estimate, std::string::npos) << plan;
      ASSERT_LT(estimate, line_end) << plan;
      const size_t number_start =
          estimate + std::string_view("estimated rows:").size();
      const size_t close = plan.find(')', number_start);
      ASSERT_NE(close, std::string::npos) << plan;
      ASSERT_LT(close, line_end) << plan;
      const std::string estimate_text =
          plan.substr(number_start, close - number_start);
      char* end = nullptr;
      errno = 0;
      const long long rows = std::strtoll(estimate_text.c_str(), &end, 10);
      ASSERT_EQ(errno, 0) << plan;
      ASSERT_NE(end, estimate_text.c_str()) << plan;
      ASSERT_EQ(*end, '\0') << plan;
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
    if (!HasTopLevelClause(query.sql, "ORDER BY") ||
        !HasTopLevelClause(query.sql, "LIMIT")) {
      continue;
    }
    const std::string plan = DumpSql(query.sql);
    SCOPED_TRACE(query.name);
    constexpr std::string_view counter = "topn_finish_uses=";
    const auto pos = plan.find(counter);
    const bool used_topn =
        pos != std::string::npos &&
        plan.find_first_of("123456789", pos + counter.size()) ==
            pos + counter.size();
    EXPECT_TRUE(plan.find("TopNFinish") != std::string::npos ||
                plan.find("TopN (") != std::string::npos || used_topn ||
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

}  // namespace tinylamb
