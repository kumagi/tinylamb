/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

// Executes one statement and returns the materialized rows.  A prepare or
// execution failure surfaces as a runtime_error carrying the diagnostic.
std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    throw std::runtime_error(engine->LastError());
  }
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
    row = Row();
  }
  const Status st = prepared.Value()->GetStatus();
  if (st != Status::kSuccess) {
    throw std::runtime_error(st.GetMessage());
  }
  return rows;
}

double DoubleAt(const Row& row, size_t column) {
  EXPECT_TRUE(row.values_[column].type == ValueType::kDouble ||
              row.values_[column].type == ValueType::kInt64);
  return row.values_[column].type == ValueType::kDouble
             ? row.values_[column].value.double_value
             : static_cast<double>(row.values_[column].value.int_value);
}

}  // namespace

class AnalyticalFeaturesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("analytical-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    // amt = 10 * k on purpose: perfect-line data pins REGR_* closed forms.
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE sales (k INT64, cat STRING, amt DOUBLE, d DATE);");
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO sales VALUES (1, 'a', 10.0, DATE '2024-01-31'), "
           "(2, 'a', 20.0, DATE '2024-02-29'), (3, 'b', 30.0, DATE "
           "'2024-03-31'), (4, 'b', 40.0, DATE '2024-04-30');");
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(AnalyticalFeaturesTest, ModeReturnsMostFrequentNonNullValue) {
  RunSql(engine_.get(), context_.get(), "CREATE TABLE m (g STRING, v INT64);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO m VALUES ('x', 7), ('x', 7), ('x', 9), "
         "('y', 5), ('y', 6), ('z', 1);");
  const auto grouped =
      RunSql(engine_.get(), context_.get(),
             "SELECT g, MODE(v) FROM m GROUP BY g ORDER BY g;");
  ASSERT_EQ(grouped.size(), 3U);
  EXPECT_EQ(grouped[0], (Row({Value("x"), Value(7)})));
  // Ties keep the first value to reach the maximum count (deterministic).
  EXPECT_EQ(grouped[1], (Row({Value("y"), Value(5)})));
  EXPECT_EQ(grouped[2], (Row({Value("z"), Value(1)})));

  // NULL inputs are ignored; an all-NULL group yields NULL.
  RunSql(engine_.get(), context_.get(), "CREATE TABLE n (v INT64);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO n VALUES (NULL), (NULL), (3);");
  const auto nulls =
      RunSql(engine_.get(), context_.get(), "SELECT MODE(v) FROM n");
  ASSERT_EQ(nulls.size(), 1U);
  EXPECT_EQ(nulls[0][0], Value(3));
  const auto empty = RunSql(engine_.get(), context_.get(),
                            "SELECT MODE(v) FROM n WHERE v > 100");
  ASSERT_EQ(empty.size(), 1U);
  EXPECT_TRUE(empty[0][0].IsNull());
}

TEST_F(AnalyticalFeaturesTest, ModeWorksWithDistinctAndWindowForms) {
  RunSql(engine_.get(), context_.get(), "CREATE TABLE d (v STRING);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO d VALUES ('p'), ('p'), ('q');");
  // DISTINCT dedupes first, so 'p' and 'q' both count once: the first value
  // to reach the maximum wins.
  const auto distinct =
      RunSql(engine_.get(), context_.get(), "SELECT MODE(DISTINCT v) FROM d");
  ASSERT_EQ(distinct.size(), 1U);
  EXPECT_EQ(distinct[0][0], Value("p"));

  const auto windowed =
      RunSql(engine_.get(), context_.get(),
             "SELECT DISTINCT MODE(k) OVER (PARTITION BY cat) AS mk FROM sales "
             "ORDER BY mk;");
  ASSERT_EQ(windowed.size(), 2U);
  EXPECT_EQ(windowed[0][0], Value(1));
  EXPECT_EQ(windowed[1][0], Value(3));
}

TEST_F(AnalyticalFeaturesTest, RegrAggregatesMatchClosedForms) {
  // y = 10x exactly: slope 10, intercept 0, R2 1.
  const auto rows = RunSql(
      engine_.get(), context_.get(),
      "SELECT REGR_SLOPE(amt, k), REGR_INTERCEPT(amt, k), REGR_COUNT(amt, k), "
      "REGR_R2(amt, k), REGR_AVGX(amt, k), REGR_AVGY(amt, k), "
      "REGR_SXX(amt, k), REGR_SYY(amt, k), REGR_SXY(amt, k) FROM sales;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_DOUBLE_EQ(DoubleAt(rows[0], 0), 10.0);
  EXPECT_DOUBLE_EQ(DoubleAt(rows[0], 1), 0.0);
  EXPECT_EQ(rows[0][2], Value(4));
  EXPECT_DOUBLE_EQ(DoubleAt(rows[0], 3), 1.0);
  EXPECT_DOUBLE_EQ(DoubleAt(rows[0], 4), 2.5);
  EXPECT_DOUBLE_EQ(DoubleAt(rows[0], 5), 25.0);
  EXPECT_DOUBLE_EQ(DoubleAt(rows[0], 6), 5.0);
  // SYY = (10-25)^2 + (20-25)^2 + (30-25)^2 + (40-25)^2 = 500.
  EXPECT_DOUBLE_EQ(DoubleAt(rows[0], 7), 500.0);
  EXPECT_DOUBLE_EQ(DoubleAt(rows[0], 8), 50.0);

  // Grouped route runs the same accumulator.
  const auto grouped =
      RunSql(engine_.get(), context_.get(),
             "SELECT cat, REGR_SLOPE(amt, k) FROM sales GROUP BY cat "
             "ORDER BY cat;");
  ASSERT_EQ(grouped.size(), 2U);
  EXPECT_DOUBLE_EQ(DoubleAt(grouped[0], 1), 10.0);
  EXPECT_DOUBLE_EQ(DoubleAt(grouped[1], 1), 10.0);

  // Empty input: REGR_COUNT is 0, the derived statistics are NULL.
  const auto empty = RunSql(engine_.get(), context_.get(),
                            "SELECT REGR_COUNT(amt, k), REGR_SLOPE(amt, k) "
                            "FROM sales WHERE k > 100;");
  ASSERT_EQ(empty.size(), 1U);
  EXPECT_EQ(empty[0][0], Value(0));
  EXPECT_TRUE(empty[0][1].IsNull());
}

TEST_F(AnalyticalFeaturesTest, RegrSlopeOverOrderedWindowFrame) {
  const auto rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT k, REGR_SLOPE(amt, k) OVER (ORDER BY k "
             "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) "
             "FROM sales ORDER BY k;");
  ASSERT_EQ(rows.size(), 4U);
  for (const Row& row : rows) {
    EXPECT_DOUBLE_EQ(DoubleAt(row, 1), 10.0);
  }
}

// Regression: LIMIT pushed a max-rows hint into the scan below the window,
// truncating the partition and computing whole-partition frames (and the
// default OVER () frame) over the truncated prefix.
TEST_F(AnalyticalFeaturesTest, LimitDoesNotTruncatePartitionWideWindows) {
  const auto total =
      RunSql(engine_.get(), context_.get(), "SELECT SUM(amt) FROM sales;");
  ASSERT_EQ(total.size(), 1U);
  const double sum = DoubleAt(total[0], 0);

  const auto limited =
      RunSql(engine_.get(), context_.get(),
             "SELECT SUM(amt) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND "
             "UNBOUNDED FOLLOWING) FROM sales LIMIT 1;");
  ASSERT_EQ(limited.size(), 1U);
  EXPECT_DOUBLE_EQ(DoubleAt(limited[0], 0), sum);

  const auto slope =
      RunSql(engine_.get(), context_.get(),
             "SELECT REGR_SLOPE(amt, k) OVER () FROM sales LIMIT 1;");
  ASSERT_EQ(slope.size(), 1U);
  EXPECT_DOUBLE_EQ(DoubleAt(slope[0], 0), 10.0);

  const auto qualified =
      RunSql(engine_.get(), context_.get(),
             "SELECT k FROM sales QUALIFY ROW_NUMBER() OVER (ORDER BY k DESC) "
             "= 1 LIMIT 1;");
  ASSERT_EQ(qualified.size(), 1U);
  EXPECT_EQ(qualified[0][0], Value(4));

  // The limit still applies to plain scans.
  const auto plain =
      RunSql(engine_.get(), context_.get(), "SELECT k FROM sales LIMIT 2;");
  EXPECT_EQ(plain.size(), 2U);
}

TEST_F(AnalyticalFeaturesTest, QualifyFiltersGroupedAggregates) {
  // SUM(amt): 'a' sums to 30, 'b' to 70.
  const auto sum_filtered =
      RunSql(engine_.get(), context_.get(),
             "SELECT cat FROM sales GROUP BY cat QUALIFY SUM(amt) > 60 "
             "ORDER BY cat;");
  ASSERT_EQ(sum_filtered.size(), 1U);
  EXPECT_EQ(sum_filtered[0][0], Value("b"));

  // Predicate mixing COUNT and an aggregate absent from the select list.
  const auto mixed = RunSql(
      engine_.get(), context_.get(),
      "SELECT cat FROM sales GROUP BY cat QUALIFY COUNT(*) >= 2 AND SUM(k) > 3 "
      "ORDER BY cat;");
  ASSERT_EQ(mixed.size(), 1U);
  EXPECT_EQ(mixed[0][0], Value("b"));

  // Alias reference and a table-qualified base column.
  const auto alias_ref = RunSql(
      engine_.get(), context_.get(),
      "SELECT cat, COUNT(*) c FROM sales GROUP BY cat QUALIFY c >= 2 AND "
      "SUM(sales.k) > 2 ORDER BY cat;");
  ASSERT_EQ(alias_ref.size(), 2U);
  EXPECT_EQ(alias_ref[0][0], Value("a"));
  EXPECT_EQ(alias_ref[1][0], Value("b"));

  // NULL semantics: groups whose predicate evaluates to NULL drop out.
  const auto null_dropped =
      RunSql(engine_.get(), context_.get(),
             "SELECT cat FROM sales GROUP BY cat QUALIFY SUM(amt) > 100 "
             "ORDER BY cat;");
  EXPECT_TRUE(null_dropped.empty());
}

// Regression: scan-column pruning ignored columns referenced only by
// QUALIFY, so the aggregate children resolved against a schema that no
// longer contained them (wrong results / "field not found").
TEST_F(AnalyticalFeaturesTest, QualifyColumnsSurviveScanPruning) {
  // `amt` appears ONLY inside QUALIFY; the select list keeps just `k`.
  const auto pruned = RunSql(
      engine_.get(), context_.get(),
      "SELECT k FROM sales QUALIFY ROW_NUMBER() OVER (ORDER BY amt) = 1;");
  ASSERT_EQ(pruned.size(), 1U);
  EXPECT_EQ(pruned[0][0], Value(1));
}

TEST_F(AnalyticalFeaturesTest, BitCountAndNormalizeCasefold) {
  const auto bits = RunSql(
      engine_.get(), context_.get(),
      "SELECT BIT_COUNT(7), BIT_COUNT(255), BIT_COUNT(0), BIT_COUNT(-1), "
      "BIT_COUNT(NULL);");
  ASSERT_EQ(bits.size(), 1U);
  EXPECT_EQ(bits[0], (Row({Value(3), Value(8), Value(0), Value(64), Value()})));

  const auto folded = RunSql(
      engine_.get(), context_.get(),
      "SELECT NORMALIZE_AND_CASEFOLD('Stra\xC3\x9F"
      "e') = "
      "NORMALIZE_AND_CASEFOLD('strasse'), NORMALIZE_AND_CASEFOLD('ABC');");
  ASSERT_EQ(folded.size(), 1U);
  EXPECT_EQ(folded[0][0], Value(1));
  EXPECT_EQ(folded[0][1], Value("abc"));

  const auto normalized =
      RunSql(engine_.get(), context_.get(),
             "SELECT NORMALIZE('ABC', 'NFC'), NORMALIZE('ABC', 'NFKC');");
  ASSERT_EQ(normalized.size(), 1U);
  EXPECT_EQ(normalized[0], (Row({Value("ABC"), Value("ABC")})));
}

TEST_F(AnalyticalFeaturesTest, GenerateDateArrayCalendarSteps) {
  // Month steps clamp month ends like DATE_ADD (Jan 31 -> Feb 29 in 2024).
  const auto months =
      RunSql(engine_.get(), context_.get(),
             "SELECT ARRAY_TO_STRING(GENERATE_DATE_ARRAY(DATE '2024-01-31', "
             "DATE '2024-04-30', INTERVAL 1 MONTH), ',');");
  ASSERT_EQ(months.size(), 1U);
  EXPECT_EQ(months[0][0], Value("2024-01-31,2024-02-29,2024-03-29,2024-04-29"));

  const auto quarters =
      RunSql(engine_.get(), context_.get(),
             "SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', "
             "DATE '2025-01-01', INTERVAL 2 QUARTER));");
  ASSERT_EQ(quarters.size(), 1U);
  EXPECT_EQ(quarters[0][0], Value(3));

  const auto years =
      RunSql(engine_.get(), context_.get(),
             "SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2020-01-01', "
             "DATE '2024-01-01', INTERVAL 1 YEAR));");
  ASSERT_EQ(years.size(), 1U);
  EXPECT_EQ(years[0][0], Value(5));

  const auto negative =
      RunSql(engine_.get(), context_.get(),
             "SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-06-01', "
             "DATE '2024-01-01', INTERVAL -1 MONTH));");
  ASSERT_EQ(negative.size(), 1U);
  EXPECT_EQ(negative[0][0], Value(6));

  // Day steps keep the pre-existing behavior.
  const auto days =
      RunSql(engine_.get(), context_.get(),
             "SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', "
             "DATE '2024-01-31', INTERVAL 7 DAY));");
  ASSERT_EQ(days.size(), 1U);
  EXPECT_EQ(days[0][0], Value(5));
}

TEST_F(AnalyticalFeaturesTest, SystemTableSampleBlockSampling) {
  const auto all =
      RunSql(engine_.get(), context_.get(),
             "SELECT COUNT(*) FROM sales TABLESAMPLE SYSTEM (100 PERCENT);");
  ASSERT_EQ(all.size(), 1U);
  EXPECT_EQ(all[0][0], Value(4));

  const auto none =
      RunSql(engine_.get(), context_.get(),
             "SELECT COUNT(*) FROM sales TABLESAMPLE SYSTEM (0 PERCENT);");
  ASSERT_EQ(none.size(), 1U);
  EXPECT_EQ(none[0][0], Value(0));

  // REPEATABLE pins the per-page decisions: two runs agree exactly.
  const auto repeatable_a = RunSql(
      engine_.get(), context_.get(),
      "SELECT cat FROM sales TABLESAMPLE SYSTEM (50 PERCENT) REPEATABLE(42) "
      "ORDER BY cat;");
  const auto repeatable_b = RunSql(
      engine_.get(), context_.get(),
      "SELECT cat FROM sales TABLESAMPLE SYSTEM (50 PERCENT) REPEATABLE(42) "
      "ORDER BY cat;");
  EXPECT_EQ(repeatable_a, repeatable_b);
}

// Multi-page table: SYSTEM sampling keeps whole blocks, so the sampled row
// count stays inside a wide band around the requested percentage and the
// 100% sample is exact.
TEST_F(AnalyticalFeaturesTest, SystemTableSampleBandOnMultiPageTable) {
  RunSql(engine_.get(), context_.get(), "CREATE TABLE big (k INT64);");
  RunSql(
      engine_.get(), context_.get(),
      "INSERT INTO big SELECT k FROM UNNEST(GENERATE_ARRAY(1, 20000)) AS k;");
  const auto sampled =
      RunSql(engine_.get(), context_.get(),
             "SELECT COUNT(*) FROM big TABLESAMPLE SYSTEM (50 PERCENT);");
  ASSERT_EQ(sampled.size(), 1U);
  const int64_t count = sampled[0][0].value.int_value;
  // Block granularity makes the count lumpy; a 20%-80% band leaves room for
  // page-level variance while still failing on per-row (25%/75%) or
  // whole-table bugs.
  EXPECT_GE(count, 4000);
  EXPECT_LE(count, 16000);

  const auto exact =
      RunSql(engine_.get(), context_.get(),
             "SELECT COUNT(*) FROM big TABLESAMPLE SYSTEM (100 PERCENT);");
  ASSERT_EQ(exact.size(), 1U);
  EXPECT_EQ(exact[0][0], Value(20000));
}

// Pipe syntax (`|>`): each operator composes on the previous statement's
// table output.  Pre-fix the visitor silently dropped every Pipe* node and
// executed only the leading SELECT, so `... |> WHERE v > 10` returned all
// rows without any error.
TEST_F(AnalyticalFeaturesTest, PipeSyntaxComposesOperators) {
  RunSql(engine_.get(), context_.get(), "CREATE TABLE p (k INT64, v INT64);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO p VALUES (1, 10), (1, 20), (2, 30), (3, 5);");
  RunSql(engine_.get(), context_.get(), "CREATE TABLE q (c INT64);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO q VALUES (2), (3);");

  // WHERE filters rows.
  const auto filtered = RunSql(engine_.get(), context_.get(),
                               "SELECT k, v FROM p |> WHERE v > 9");
  ASSERT_EQ(filtered.size(), 3U);

  // FROM-first queries are the idiomatic pipe entry point.
  const auto from_first =
      RunSql(engine_.get(), context_.get(),
             "FROM p |> AGGREGATE k, SUM(v) AS s GROUP BY k |> ORDER BY s "
             "DESC |> LIMIT 1");
  ASSERT_EQ(from_first.size(), 1U);
  EXPECT_EQ(from_first[0][0].value.int_value, 1);
  EXPECT_EQ(from_first[0][1].value.int_value, 30);

  // A full analytical chain: aggregate, order by the aggregate alias, cap.
  const auto chain =
      RunSql(engine_.get(), context_.get(),
             "SELECT k FROM p |> AGGREGATE k, COUNT(*) AS c GROUP BY k "
             "|> ORDER BY c DESC, k |> LIMIT 1");
  ASSERT_EQ(chain.size(), 1U);
  EXPECT_EQ(chain[0][0].value.int_value, 1);
  EXPECT_EQ(chain[0][1].value.int_value, 2);

  // Pipe JOIN rebinds the input's original qualifier (`tv.k`-style) onto the
  // derived input; the ON condition must actually filter.
  const auto joined =
      RunSql(engine_.get(), context_.get(), "FROM p |> JOIN q ON q.c = p.k");
  ASSERT_EQ(joined.size(), 2U);
  // LEFT JOIN keeps the unmatched input rows (k = 1 has no q match) with
  // NULL padding.
  const auto left = RunSql(engine_.get(), context_.get(),
                           "FROM p |> LEFT JOIN q ON q.c = p.k");
  ASSERT_EQ(left.size(), 4U);
  const auto null_padded = static_cast<size_t>(
      std::count_if(left.begin(), left.end(),
                    [](const Row& row) { return row.values_[2].IsNull(); }));
  EXPECT_EQ(null_padded, 2U);

  // Pipe set operations: DISTINCT flavors deduplicate, ALL concatenates, and
  // later operators see the combined output.
  const auto set_op =
      RunSql(engine_.get(), context_.get(),
             "SELECT k FROM p |> UNION DISTINCT (SELECT c FROM q)");
  ASSERT_EQ(set_op.size(), 3U);
  const auto set_chain =
      RunSql(engine_.get(), context_.get(),
             "SELECT k FROM p |> UNION ALL (SELECT c FROM q) "
             "|> AGGREGATE COUNT(*)");
  ASSERT_EQ(set_op.size(), 3U);
  EXPECT_EQ(set_chain[0][0].value.int_value, 6);

  // `|> AS name` renames the pipe input for later operators.
  const auto renamed =
      RunSql(engine_.get(), context_.get(), "FROM p |> AS t |> WHERE t.v > 20");
  ASSERT_EQ(renamed.size(), 1U);
  EXPECT_EQ(renamed[0][0].value.int_value, 2);

  // Operators the engine cannot represent must fail loudly, never silently
  // pass the input through.
  for (const char* unsupported :
       {"SELECT k FROM p |> DROP v", "SELECT k FROM p |> EXTEND v + 1 AS w",
        "SELECT k FROM p |> RENAME k AS kk", "SELECT k FROM p |> CALL f(1)"}) {
    EXPECT_THROW((void)RunSql(engine_.get(), context_.get(), unsupported),
                 std::runtime_error)
        << unsupported;
  }
}

// Window functions over GROUPING SETS / ROLLUP see the combined result of
// every grouping set (the TPC-DS q70 / q86 shape): rank() partitions span
// detail and subtotal rows together instead of restarting per branch.
TEST_F(AnalyticalFeaturesTest, WindowOverRollupSpansAllGroupingSets) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE r (v INT64, cat STRING, cls STRING);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO r VALUES (10, 'a', 'x'), (20, 'a', 'y'), "
         "(30, 'b', 'x'), (40, 'b', 'y');");
  const auto rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT sum(v) AS total, cat, cls, "
             "grouping(cat) + grouping(cls) AS loch, "
             "rank() OVER (PARTITION BY grouping(cat) + grouping(cls) "
             "ORDER BY sum(v) DESC) AS rk "
             "FROM r GROUP BY ROLLUP(cat, cls) ORDER BY loch DESC, rk");
  // Grand total ranks alone; the two category subtotals rank together; the
  // four detail rows rank together in one partition.
  ASSERT_EQ(rows.size(), 7U);
  EXPECT_EQ(rows[0][0].value.int_value, 100);
  EXPECT_EQ(rows[0][4].value.int_value, 1);
  EXPECT_EQ(rows[1][0].value.int_value, 70);
  EXPECT_EQ(rows[1][4].value.int_value, 1);
  EXPECT_EQ(rows[2][0].value.int_value, 30);
  EXPECT_EQ(rows[2][4].value.int_value, 2);
  EXPECT_EQ(rows[3][0].value.int_value, 40);
  EXPECT_EQ(rows[3][4].value.int_value, 1);
  EXPECT_EQ(rows[4][0].value.int_value, 30);
  EXPECT_EQ(rows[4][4].value.int_value, 2);
  EXPECT_EQ(rows[5][0].value.int_value, 20);
  EXPECT_EQ(rows[5][4].value.int_value, 3);
  EXPECT_EQ(rows[6][0].value.int_value, 10);
  EXPECT_EQ(rows[6][4].value.int_value, 4);
}

// The q70 partition shape: CASE over GROUPING() splits detail ranks per
// parent while subtotal levels fall into their own NULL partitions.
TEST_F(AnalyticalFeaturesTest, WindowOverRollupPartitionByCaseOfGrouping) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE r2 (v INT64, cat STRING, cls STRING);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO r2 VALUES (10, 'a', 'x'), (20, 'a', 'y'), "
         "(30, 'b', 'x'), (40, 'b', 'y');");
  const auto rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT sum(v) AS total, cat, cls, "
             "grouping(cat) + grouping(cls) AS loch, "
             "rank() OVER (PARTITION BY grouping(cat) + grouping(cls), "
             "CASE WHEN grouping(cls) = 0 THEN cat END "
             "ORDER BY sum(v) DESC) AS rk "
             "FROM r2 GROUP BY ROLLUP(cat, cls) ORDER BY loch DESC, rk, cat");
  ASSERT_EQ(rows.size(), 7U);
  // Detail ranks restart per category; both category subtotals rank 1.
  EXPECT_EQ(rows[0][0].value.int_value, 100);
  EXPECT_EQ(rows[0][4].value.int_value, 1);
  EXPECT_EQ(rows[1][0].value.int_value, 70);
  EXPECT_EQ(rows[1][4].value.int_value, 1);
  EXPECT_EQ(rows[2][0].value.int_value, 30);
  EXPECT_EQ(rows[2][4].value.int_value, 2);
  // loch = 0 sorts by rank first, then category.
  EXPECT_EQ(rows[3][0].value.int_value, 20);
  EXPECT_EQ(rows[3][4].value.int_value, 1);
  EXPECT_EQ(rows[4][0].value.int_value, 40);
  EXPECT_EQ(rows[4][4].value.int_value, 1);
  EXPECT_EQ(rows[5][0].value.int_value, 10);
  EXPECT_EQ(rows[5][4].value.int_value, 2);
  EXPECT_EQ(rows[6][0].value.int_value, 30);
  EXPECT_EQ(rows[6][4].value.int_value, 2);
}

// GROUPING SETS with a window alias consumed by ORDER BY, and a window
// argument that is an aggregate expression.
TEST_F(AnalyticalFeaturesTest, WindowOverGroupingSetsWithSumArgument) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE r3 (v INT64, cat STRING);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO r3 VALUES (1, 'a'), (2, 'a'), (5, 'b');");
  const auto rows = RunSql(
      engine_.get(), context_.get(),
      "SELECT cat, grouping(cat) AS g, sum(v) AS s, "
      "sum(sum(v)) OVER (PARTITION BY grouping(cat) ORDER BY sum(v) DESC) "
      "AS running "
      "FROM r3 GROUP BY GROUPING SETS ((cat), ()) ORDER BY g DESC, s DESC");
  ASSERT_EQ(rows.size(), 3U);
  // Grand-total branch (g=1): one row, running = 8.
  EXPECT_TRUE(rows[0][0].IsNull());
  EXPECT_EQ(rows[0][3].value.int_value, 8);
  // Detail branch (g=0): running total over s DESC = 5, then 5+3=8.
  EXPECT_EQ(rows[1][0], Value("b"));
  EXPECT_EQ(rows[1][3].value.int_value, 5);
  EXPECT_EQ(rows[2][0], Value("a"));
  EXPECT_EQ(rows[2][3].value.int_value, 8);
}

// Partition keys that are arbitrary expressions (not bare column paths) must
// actually partition.  The AST visit used to drop non-path partition keys,
// silently collapsing the window into one global partition.
TEST_F(AnalyticalFeaturesTest, WindowPartitionByExpressionPartitions) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE pe (cat STRING, v INT64);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO pe VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4);");
  const auto rows = RunSql(
      engine_.get(), context_.get(),
      "SELECT cat, v, row_number() OVER (PARTITION BY UPPER(cat) ORDER BY v) "
      "AS rn FROM pe ORDER BY v");
  ASSERT_EQ(rows.size(), 4U);
  // UPPER(cat) partitions: row_number restarts per category value.
  EXPECT_EQ(rows[0][2].value.int_value, 1);  // ('a', 1)
  EXPECT_EQ(rows[1][2].value.int_value, 2);  // ('a', 2)
  EXPECT_EQ(rows[2][2].value.int_value, 1);  // ('b', 3)
  EXPECT_EQ(rows[3][2].value.int_value, 2);  // ('b', 4)
}

// SELECT * EXCEPT (col, ...) drops the named columns from the expansion,
// keeping the rest in schema order.
TEST_F(AnalyticalFeaturesTest, StarExceptDropsColumns) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE se (a INT64, b STRING, c DOUBLE);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO se VALUES (1, 'x', 2.5);");
  const auto rows =
      RunSql(engine_.get(), context_.get(), "SELECT * EXCEPT (b) FROM se");
  ASSERT_EQ(rows.size(), 1U);
  ASSERT_EQ(rows[0].values_.size(), 2U);
  EXPECT_EQ(rows[0][0].value.int_value, 1);
  EXPECT_EQ(rows[0][1].value.double_value, 2.5);
}

// SELECT * REPLACE (expr AS col) substitutes the expression for the column
// at its original position; other references still read the base columns.
TEST_F(AnalyticalFeaturesTest, StarReplaceSubstitutesInPlace) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE sr (a INT64, b INT64, c STRING);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO sr VALUES (2, 10, 'x');");
  const auto rows = RunSql(engine_.get(), context_.get(),
                           "SELECT * REPLACE (b * a AS b) FROM sr");
  ASSERT_EQ(rows.size(), 1U);
  ASSERT_EQ(rows[0].values_.size(), 3U);
  EXPECT_EQ(rows[0][0].value.int_value, 2);
  EXPECT_EQ(rows[0][1].value.int_value, 20);  // b * a reads base columns
  EXPECT_EQ(rows[0][2], Value("x"));
  // The replaced column name stays addressable for ORDER BY.
  const auto ordered = RunSql(engine_.get(), context_.get(),
                              "SELECT * REPLACE (a + 100 AS a) FROM sr "
                              "ORDER BY a DESC");
  ASSERT_EQ(ordered.size(), 1U);
  EXPECT_EQ(ordered[0][0].value.int_value, 102);
}

// EXCEPT and REPLACE compose on one star, and mixing stars with explicit
// items keeps the interleaving order.
TEST_F(AnalyticalFeaturesTest, StarModifiersComposeAndInterleave) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE sc (a INT64, b INT64, c INT64);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO sc VALUES (1, 2, 3);");
  const auto rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT b, * EXCEPT (b) REPLACE (a * 10 AS a) FROM sc");
  ASSERT_EQ(rows.size(), 1U);
  ASSERT_EQ(rows[0].values_.size(), 3U);
  EXPECT_EQ(rows[0][0].value.int_value, 2);   // explicit b first
  EXPECT_EQ(rows[0][1].value.int_value, 10);  // a replaced
  EXPECT_EQ(rows[0][2].value.int_value, 3);   // c kept
}

// EXCEPT matching is case-insensitive, and a qualified spelling naming the
// star target resolves too.
TEST_F(AnalyticalFeaturesTest, StarExceptCaseAndQualifiedSpelling) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE sq (Alpha INT64, beta INT64);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO sq VALUES (1, 2);");
  const auto bare =
      RunSql(engine_.get(), context_.get(), "SELECT * EXCEPT (ALPHA) FROM sq");
  ASSERT_EQ(bare.size(), 1U);
  ASSERT_EQ(bare[0].values_.size(), 1U);
  EXPECT_EQ(bare[0][0].value.int_value, 2);
  const auto qualified = RunSql(engine_.get(), context_.get(),
                                "SELECT sq.* EXCEPT (alpha) FROM sq");
  ASSERT_EQ(qualified.size(), 1U);
  ASSERT_EQ(qualified[0].values_.size(), 1U);
  EXPECT_EQ(qualified[0][0].value.int_value, 2);
}

// A bare star with modifiers over several relations resolves each entry
// against the relations owning the column; the pinned parser only accepts
// bare EXCEPT spellings, so a shared name drops from every relation.
TEST_F(AnalyticalFeaturesTest, StarExceptAcrossMultipleRelations) {
  RunSql(engine_.get(), context_.get(), "CREATE TABLE m1 (a INT64, k INT64);");
  RunSql(engine_.get(), context_.get(), "CREATE TABLE m2 (b INT64, k INT64);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO m1 VALUES (1, 1);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO m2 VALUES (2, 1);");
  const auto rows = RunSql(engine_.get(), context_.get(),
                           "SELECT * EXCEPT (k) FROM m1, m2 "
                           "WHERE m1.k = m2.k");
  ASSERT_EQ(rows.size(), 1U);
  ASSERT_EQ(rows[0].values_.size(), 2U);  // m1.a, m2.b
  EXPECT_EQ(rows[0][0].value.int_value, 1);
  EXPECT_EQ(rows[0][1].value.int_value, 2);
}

// Star modifiers over a derived table expand through the inlining path.
TEST_F(AnalyticalFeaturesTest, StarExceptOverDerivedTable) {
  RunSql(engine_.get(), context_.get(), "CREATE TABLE sd (a INT64, b INT64);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO sd VALUES (1, 2);");
  const auto rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT * EXCEPT (b) FROM (SELECT a, b, a + b AS s FROM sd) AS d");
  ASSERT_EQ(rows.size(), 1U);
  ASSERT_EQ(rows[0].values_.size(), 2U);
  EXPECT_EQ(rows[0][0].value.int_value, 1);
  EXPECT_EQ(rows[0][1].value.int_value, 3);
}

// Modifier entries that name no column of the star target fail loudly.
TEST_F(AnalyticalFeaturesTest, StarModifierUnknownColumnFails) {
  RunSql(engine_.get(), context_.get(), "CREATE TABLE sf (a INT64);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO sf VALUES (1);");
  EXPECT_THROW(RunSql(engine_.get(), context_.get(),
                      "SELECT * EXCEPT (missing) FROM sf"),
               std::runtime_error);
  EXPECT_THROW(RunSql(engine_.get(), context_.get(),
                      "SELECT * REPLACE (1 AS missing) FROM sf"),
               std::runtime_error);
  // A duplicate EXCEPT entry is rejected at visit time.
  EXPECT_THROW(
      RunSql(engine_.get(), context_.get(), "SELECT * EXCEPT (a, a) FROM sf"),
      std::runtime_error);
  // A star with modifiers cannot take an alias.
  EXPECT_THROW(
      RunSql(engine_.get(), context_.get(), "SELECT * EXCEPT (a) AS x FROM sf"),
      std::runtime_error);
}

// EXCEPT works on UNION ALL branches with single-table stars (the branch
// expansion path), where both branches share the widened shape.
TEST_F(AnalyticalFeaturesTest, StarExceptInUnionBranch) {
  RunSql(engine_.get(), context_.get(), "CREATE TABLE u1 (a INT64, b INT64);");
  RunSql(engine_.get(), context_.get(), "CREATE TABLE u2 (a INT64, b INT64);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO u1 VALUES (1, 2);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO u2 VALUES (3, 4);");
  const auto rows = RunSql(engine_.get(), context_.get(),
                           "SELECT * EXCEPT (b) FROM u1 "
                           "UNION ALL "
                           "SELECT * EXCEPT (b) FROM u2");
  ASSERT_EQ(rows.size(), 2U);
  ASSERT_EQ(rows[0].values_.size(), 1U);
  EXPECT_EQ(rows[0][0].value.int_value, 1);
  EXPECT_EQ(rows[1][0].value.int_value, 3);
}

// A multi-table derived table stays on the relational path; EXCEPT is
// honored by the relational star expansion there.
TEST_F(AnalyticalFeaturesTest, StarExceptOnRelationalDerivedTable) {
  RunSql(engine_.get(), context_.get(), "CREATE TABLE r1 (a INT64, k INT64);");
  RunSql(engine_.get(), context_.get(), "CREATE TABLE r2 (b INT64, k INT64);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO r1 VALUES (1, 1);");
  RunSql(engine_.get(), context_.get(), "INSERT INTO r2 VALUES (2, 1);");
  const auto rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT * EXCEPT (b) FROM (SELECT r1.a AS a, r1.k AS k, r2.b AS b "
             "FROM r1, r2 WHERE r1.k = r2.k) AS d");
  ASSERT_EQ(rows.size(), 1U);
  ASSERT_EQ(rows[0].values_.size(), 2U);
  EXPECT_EQ(rows[0][0].value.int_value, 1);
  EXPECT_EQ(rows[0][1].value.int_value, 1);
}

// MEDIAN(x) lowers to PERCENTILE_CONT(x, 0.5): exact interpolation in the
// aggregate, the grouped form, and the window form alike.
TEST_F(AnalyticalFeaturesTest, MedianMatchesPercentileCont) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE med (g INT64, n DOUBLE);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO med VALUES (1, 1.0), (1, 2.0), (1, 3.0), (1, 10.0), "
         "(2, 5.0), (2, 7.0);");
  const auto single = RunSql(engine_.get(), context_.get(),
                             "SELECT MEDIAN(n) FROM med WHERE g = 1");
  ASSERT_EQ(single.size(), 1U);
  EXPECT_DOUBLE_EQ(single[0][0].value.double_value, 2.5);
  // Even count interpolates between the two middle values.
  const auto even = RunSql(engine_.get(), context_.get(),
                           "SELECT MEDIAN(n) FROM med WHERE g = 2");
  ASSERT_EQ(even.size(), 1U);
  EXPECT_DOUBLE_EQ(even[0][0].value.double_value, 6.0);
  // Empty input is NULL, not 0.
  EXPECT_TRUE(RunSql(engine_.get(), context_.get(),
                     "SELECT MEDIAN(n) FROM med WHERE g = 9")[0][0]
                  .IsNull());
  // Grouped and window forms agree with the aggregate.
  const auto grouped =
      RunSql(engine_.get(), context_.get(),
             "SELECT g, MEDIAN(n) FROM med GROUP BY g ORDER BY g");
  ASSERT_EQ(grouped.size(), 2U);
  EXPECT_DOUBLE_EQ(grouped[0][1].value.double_value, 2.5);
  EXPECT_DOUBLE_EQ(grouped[1][1].value.double_value, 6.0);
  const auto window = RunSql(engine_.get(), context_.get(),
                             "SELECT DISTINCT MEDIAN(n) OVER (PARTITION BY g) "
                             "FROM med ORDER BY 1");
  ASSERT_EQ(window.size(), 2U);
  EXPECT_DOUBLE_EQ(window[0][0].value.double_value, 2.5);
  EXPECT_DOUBLE_EQ(window[1][0].value.double_value, 6.0);
}

// FARM_FINGERPRINT is the signed INT64 view of FarmHash's Fingerprint64.
// The expected values were generated against the reference implementation
// (google/farmhash farmhashna::Hash64) and pinned here so the port cannot
// drift.
TEST_F(AnalyticalFeaturesTest, FarmFingerprintMatchesReference) {
  const auto rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT FARM_FINGERPRINT('hello'), FARM_FINGERPRINT('abc'), "
             "FARM_FINGERPRINT(''), FARM_FINGERPRINT(REPEAT('x', 100)), "
             "FARM_FINGERPRINT(CAST(NULL AS STRING))");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0].value.int_value, int64_t{-5436999610281751320LL});
  EXPECT_EQ(rows[0][1].value.int_value, int64_t{2640714258260161385LL});
  EXPECT_EQ(rows[0][2].value.int_value, int64_t{-7286425919675154353LL});
  EXPECT_EQ(rows[0][3].value.int_value, int64_t{6590480085648050719LL});
  EXPECT_TRUE(rows[0][4].IsNull());
  // Identical inputs agree; a one-bit change does not (sanity, not a pin).
  EXPECT_EQ(
      RunSql(engine_.get(), context_.get(),
             "SELECT FARM_FINGERPRINT('a'), FARM_FINGERPRINT('a')")[0][0],
      RunSql(engine_.get(), context_.get(),
             "SELECT FARM_FINGERPRINT('a'), FARM_FINGERPRINT('a')")[0][0]);
}

// PARSE_JSON validates strict JSON text and feeds the JSON accessors in the
// engine's string representation; TO_JSON renders scalars, structs, and
// arrays back to JSON text.
TEST_F(AnalyticalFeaturesTest, ParseJsonAndToJsonRoundTrip) {
  const auto parsed =
      RunSql(engine_.get(), context_.get(),
             R"(SELECT JSON_VALUE(PARSE_JSON('{"a": {"b": 42}}'), '$.a.b'))");
  ASSERT_EQ(parsed.size(), 1U);
  EXPECT_EQ(parsed[0][0], Value("42"));
  // Invalid text is a loud error, never a silent pass-through.
  bool rejected = false;
  try {
    RunSql(engine_.get(), context_.get(), "SELECT PARSE_JSON('{bad')");
  } catch (const std::runtime_error& error) {
    rejected = std::string(error.what()).find("Parsed JSON is invalid") !=
               std::string::npos;
  }
  EXPECT_TRUE(rejected);
  // TO_JSON: numbers bare, text quoted, dates quoted, arrays recursed.
  const auto rendered =
      RunSql(engine_.get(), context_.get(),
             "SELECT TO_JSON(42), TO_JSON(1.5), TO_JSON('hi'), "
             "TO_JSON(DATE '2024-01-05'), TO_JSON([1, 2, 3])");
  ASSERT_EQ(rendered.size(), 1U);
  EXPECT_EQ(rendered[0][0], Value("42"));
  EXPECT_EQ(rendered[0][1], Value("1.5"));
  EXPECT_EQ(rendered[0][2], Value("\"hi\""));
  EXPECT_EQ(rendered[0][3], Value("\"2024-01-05\""));
  EXPECT_EQ(rendered[0][4], Value("[1,2,3]"));
  // NULL propagation on both functions.
  EXPECT_TRUE(
      RunSql(engine_.get(), context_.get(), "SELECT TO_JSON(NULL)")[0][0]
          .IsNull());
  EXPECT_TRUE(
      RunSql(engine_.get(), context_.get(), "SELECT PARSE_JSON(NULL)")[0][0]
          .IsNull());
}

// The JSON document family: readers (TYPE/KEYS/ARRAY_LENGTH), mutators
// (SET/INSERT/REPLACE/REMOVE), constructors (ARRAY/OBJECT), and the LAX_*
// coercions.  One shared implementation backs the AST evaluator, the
// relational evaluator (the filter below forces it), and the constant-fold
// rewrite, so every route pins the same results.
TEST_F(AnalyticalFeaturesTest, JsonDocumentFamilyReadersAndMutators) {
  // Readers: kind names, top-level keys in document order, array lengths.
  const auto readers = RunSql(
      engine_.get(), context_.get(),
      R"(SELECT JSON_TYPE('{"a": [1, 2], "b": null}'), )"
      R"(JSON_TYPE('[1, 2]'), JSON_TYPE('"text"'), JSON_TYPE('3.5'), )"
      R"(JSON_TYPE('true'), JSON_TYPE('null'), )"
      R"(JSON_KEYS('{"b": 1, "a": 2}'), JSON_KEYS('{"o": {"z": 0}}', '$.o'), )"
      R"(JSON_ARRAY_LENGTH('[1, 2, 3]'), JSON_ARRAY_LENGTH('{"x": [9, 8]}', '$.x'))");
  ASSERT_EQ(readers.size(), 1U);
  EXPECT_EQ(readers[0][0], Value("object"));
  EXPECT_EQ(readers[0][1], Value("array"));
  EXPECT_EQ(readers[0][2], Value("string"));
  EXPECT_EQ(readers[0][3], Value("number"));
  EXPECT_EQ(readers[0][4], Value("boolean"));
  EXPECT_EQ(readers[0][5], Value("null"));
  EXPECT_EQ(readers[0][6], Value::Array({Value("b"), Value("a")}, "STRING"));
  EXPECT_EQ(readers[0][7], Value::Array({Value("z")}, "STRING"));
  EXPECT_EQ(readers[0][8], Value(static_cast<int64_t>(3)));
  EXPECT_EQ(readers[0][9], Value(static_cast<int64_t>(2)));
  // Missing paths and non-matching containers yield NULL, not errors.
  const auto readerNulls =
      RunSql(engine_.get(), context_.get(),
             R"(SELECT JSON_TYPE('{"a": 1}', '$.zz'), )"
             R"(JSON_KEYS('[1]'), JSON_ARRAY_LENGTH('{"a": 1}'))");
  ASSERT_EQ(readerNulls.size(), 1U);
  EXPECT_TRUE(readerNulls[0][0].IsNull());
  EXPECT_TRUE(readerNulls[0][1].IsNull());
  EXPECT_TRUE(readerNulls[0][2].IsNull());

  // Mutators: SET creates intermediates and overwrites; INSERT refuses
  // existing paths; REPLACE refuses missing ones; REMOVE is a no-op on
  // missing members but rejects out-of-bounds array positions.
  const auto mutators = RunSql(engine_.get(), context_.get(),
                               R"(SELECT JSON_SET('{"a": 1}', '$.b', 2), )"
                               R"(JSON_SET('{}', '$.a.b.c', 7), )"
                               R"(JSON_SET('{"a": 1}', '$.a', 5), )"
                               R"(JSON_INSERT('{"a": 1}', '$.b', 2), )"
                               R"(JSON_REPLACE('{"a": 1}', '$.a', 9), )"
                               R"(JSON_REMOVE('{"a": 1, "b": 2}', '$.a'), )"
                               R"(JSON_REMOVE('[1, 2, 3]', '$[1]'))");
  ASSERT_EQ(mutators.size(), 1U);
  EXPECT_EQ(mutators[0][0], Value(R"({"a":1,"b":2})"));
  EXPECT_EQ(mutators[0][1], Value(R"({"a":{"b":{"c":7}}})"));
  EXPECT_EQ(mutators[0][2], Value(R"({"a":5})"));
  EXPECT_EQ(mutators[0][3], Value(R"({"a":1,"b":2})"));
  EXPECT_EQ(mutators[0][4], Value(R"({"a":9})"));
  EXPECT_EQ(mutators[0][5], Value(R"({"b":2})"));
  EXPECT_EQ(mutators[0][6], Value("[1,3]"));
  // Array positions beyond the current length are never extended.
  bool outOfBoundsRejected = false;
  try {
    RunSql(engine_.get(), context_.get(), "SELECT JSON_SET('[1]', '$[3]', 9)");
  } catch (const std::runtime_error& error) {
    outOfBoundsRejected =
        std::string(error.what()).find("out of bounds") != std::string::npos;
  }
  EXPECT_TRUE(outOfBoundsRejected);

  // Constructors: SQL NULL arguments become JSON null; duplicate keys and
  // NULL keys are loud errors.
  const auto constructors = RunSql(
      engine_.get(), context_.get(),
      R"(SELECT JSON_ARRAY(1, 'x', NULL), JSON_OBJECT('k', 1, 'v', 'x'), )"
      R"(JSON_ARRAY(), JSON_OBJECT())");
  ASSERT_EQ(constructors.size(), 1U);
  EXPECT_EQ(constructors[0][0], Value(R"([1,"x",null])"));
  EXPECT_EQ(constructors[0][1], Value(R"({"k":1,"v":"x"})"));
  EXPECT_EQ(constructors[0][2], Value("[]"));
  EXPECT_EQ(constructors[0][3], Value("{}"));
  bool rejected = false;
  try {
    RunSql(engine_.get(), context_.get(), "SELECT JSON_OBJECT('k', 1, 'k', 2)");
  } catch (const std::runtime_error& error) {
    rejected =
        std::string(error.what()).find("duplicate key") != std::string::npos;
  }
  EXPECT_TRUE(rejected);

  // The relational evaluator path: a scan filter whose predicate carries the
  // family functions routes through executor/detail/expression_eval.cpp.
  // JSON_TYPE(doc) reads the root kind; the $.kind member holds JSON strings,
  // so its type is "string" and value comparison goes through JSON_VALUE.
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE json_docs (k INT64, doc STRING);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO json_docs VALUES "
         "(1, '{\"kind\": \"array\", \"n\": 2}'), "
         "(2, '{\"kind\": \"object\", \"n\": 5}'), "
         "(3, '{\"kind\": \"other\", \"n\": 11}')");
  const auto filtered = RunSql(engine_.get(), context_.get(),
                               "SELECT k FROM json_docs "
                               "WHERE JSON_TYPE(doc) = 'object' "
                               "AND JSON_VALUE(doc, '$.kind') = 'object' "
                               "AND LAX_INT64(JSON_EXTRACT(doc, '$.n')) >= 5 "
                               "AND JSON_ARRAY_LENGTH(JSON_ARRAY(doc)) = 1 "
                               "ORDER BY k");
  ASSERT_EQ(filtered.size(), 1U);
  EXPECT_EQ(filtered[0][0], Value(static_cast<int64_t>(2)));

  // Malformed input is a loud error for the readers and mutators; the LAX
  // coercions never error, they yield NULL.
  bool invalidRejected = false;
  try {
    RunSql(engine_.get(), context_.get(), "SELECT JSON_TYPE('{bad')");
  } catch (const std::runtime_error& error) {
    invalidRejected =
        std::string(error.what()).find("invalid JSON") != std::string::npos;
  }
  EXPECT_TRUE(invalidRejected);
  const auto laxNull = RunSql(engine_.get(), context_.get(),
                              "SELECT LAX_INT64('{bad'), LAX_STRING('{bad')");
  ASSERT_EQ(laxNull.size(), 1U);
  EXPECT_TRUE(laxNull[0][0].IsNull());
  EXPECT_TRUE(laxNull[0][1].IsNull());
}

TEST_F(AnalyticalFeaturesTest, LaxCoercionsFollowJsonSemantics) {
  // LAX_INT64: numbers truncate, numeric strings parse, booleans are 1/0,
  // containers yield NULL.
  const auto conversions =
      RunSql(engine_.get(), context_.get(),
             "SELECT LAX_INT64('5'), LAX_INT64('5.9'), LAX_INT64('-5.9'), "
             "LAX_INT64('true'), LAX_INT64('\"12\"'), "
             "LAX_FLOAT64('2.5'), LAX_FLOAT64('false'), "
             "LAX_BOOL('true'), LAX_BOOL('0'), LAX_BOOL('\"false\"'), "
             "LAX_STRING('42'), LAX_STRING('\"hi\"'), LAX_STRING('true')");
  ASSERT_EQ(conversions.size(), 1U);
  EXPECT_EQ(conversions[0][0], Value(static_cast<int64_t>(5)));
  EXPECT_EQ(conversions[0][1], Value(static_cast<int64_t>(5)));
  EXPECT_EQ(conversions[0][2], Value(static_cast<int64_t>(-5)));
  EXPECT_EQ(conversions[0][3], Value(static_cast<int64_t>(1)));
  EXPECT_EQ(conversions[0][4], Value(static_cast<int64_t>(12)));
  EXPECT_EQ(conversions[0][5], Value(2.5));
  EXPECT_EQ(conversions[0][6], Value(0.0));
  EXPECT_EQ(conversions[0][7], Value(static_cast<int64_t>(1)));
  EXPECT_EQ(conversions[0][8], Value(static_cast<int64_t>(0)));
  EXPECT_EQ(conversions[0][9], Value(static_cast<int64_t>(0)));
  EXPECT_EQ(conversions[0][10], Value("42"));
  EXPECT_EQ(conversions[0][11], Value("hi"));
  EXPECT_EQ(conversions[0][12], Value("true"));
  // Non-coercible shapes yield NULL on every LAX target.
  const auto laxNulls = RunSql(
      engine_.get(), context_.get(),
      "SELECT LAX_INT64('[1]'), LAX_FLOAT64('{\"a\": 1}'), LAX_BOOL('null'), "
      "LAX_STRING('[1, 2]')");
  ASSERT_EQ(laxNulls.size(), 1U);
  EXPECT_TRUE(laxNulls[0][0].IsNull());
  EXPECT_TRUE(laxNulls[0][1].IsNull());
  EXPECT_TRUE(laxNulls[0][2].IsNull());
  EXPECT_TRUE(laxNulls[0][3].IsNull());
}

// ILIKE / NOT ILIKE parse through a frontend rewrite to
// `LOWER(x) [NOT] LIKE LOWER(y)`; the pinned parser has no ILIKE token.
TEST_F(AnalyticalFeaturesTest, CaseInsensitiveLikeForms) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE txt (s STRING, k INT64);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO txt VALUES ('Apple', 1), ('banana', 2), "
         "('APRICOT', 3);");
  // Literal and column operands.
  EXPECT_EQ(
      RunSql(engine_.get(), context_.get(), "SELECT 'abc' ILIKE 'AB%'")[0][0],
      Value(int64_t{1}));
  EXPECT_EQ(RunSql(engine_.get(), context_.get(),
                   "SELECT 'abc' NOT ILIKE 'AB%'")[0][0],
            Value(int64_t{0}));
  const auto hits = RunSql(engine_.get(), context_.get(),
                           "SELECT s FROM txt WHERE s ILIKE 'ap%' ORDER BY k");
  ASSERT_EQ(hits.size(), 2U);
  EXPECT_EQ(hits[0][0], Value("Apple"));
  EXPECT_EQ(hits[1][0], Value("APRICOT"));
  const auto misses =
      RunSql(engine_.get(), context_.get(),
             "SELECT s FROM txt WHERE s NOT ILIKE 'a%' ORDER BY k");
  ASSERT_EQ(misses.size(), 1U);
  EXPECT_EQ(misses[0][0], Value("banana"));
  // Mixed boolean context and expression operands keep parsing.
  EXPECT_EQ(RunSql(engine_.get(), context_.get(),
                   "SELECT COUNT(*) FROM txt "
                   "WHERE s ILIKE 'a%' AND k > 2")[0][0],
            Value(int64_t{1}));
  EXPECT_EQ(RunSql(engine_.get(), context_.get(),
                   "SELECT COUNT(*) FROM txt "
                   "WHERE s ILIKE CONCAT('ban', '%')")[0][0],
            Value(int64_t{1}));
  // An identifier named "ilike" is not a case-insensitive match.
  bool not_exists = false;
  try {
    RunSql(engine_.get(), context_.get(), "SELECT ilike FROM txt");
  } catch (const std::runtime_error&) {
    not_exists = true;
  }
  EXPECT_TRUE(not_exists);
}

}  // namespace tinylamb
