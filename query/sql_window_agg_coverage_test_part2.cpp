/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/memory_budget.hpp"
#include "common/platform/process.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/relational_sort_limit.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/executor_base.hpp"
#include "executor/values.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/plan_cache.hpp"
#include "query/sql_engine.hpp"
#include "query/sql_window_agg_coverage_test.hpp"
#include "query/statement.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
using namespace window_coverage_test;

// ---------------------------------------------------------------------------
// Scalar aggregation (AggregationExecutor): typed batch fast paths, constant
// and all-NULL inputs, generic mixed lists, DISTINCT and FILTER clauses.
// ---------------------------------------------------------------------------

class AggSmallCov : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("agg_small-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    EXPECT_TRUE(
        TryRunSql(engine_.get(), context_.get(),
                  "CREATE TABLE med (a INT64, cnt_uint64 INT64, f FLOAT64, "
                  "nul INT64, logi BOOL, s VARCHAR(20));")
            .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO med SELECT n, n * 2, n * 0.5, NULL, "
                          "MOD(n, 2) = 0, CAST(MOD(n, 7) AS STRING) "
                          "FROM UNNEST(GENERATE_ARRAY(1, 64)) AS n;")
                    .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO med VALUES (0, 0, CAST('NaN' AS "
                          "FLOAT64), NULL, FALSE, 'x');")
                    .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO med VALUES (0, 0, NULL, NULL, TRUE, "
                          "'y');")
                    .HasValue());
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

TEST_F(AggSmallCov, TypedColumnAggregateSweep) {
  const std::string sql =
      "SELECT COUNT(*), COUNT(a), COUNT(nul), SUM(a), AVG(a), MIN(a), "
      "MAX(a), SUM(f), AVG(f), MIN(f), MAX(f), SUM(cnt_uint64), "
      "BIT_AND(a), BIT_OR(a), BIT_XOR(a), LOGICAL_AND(a), LOGICAL_OR(a) "
      "FROM med";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 1U);
  const Row& r = rows.Value()[0];
  EXPECT_EQ(r[0], Value(int64_t{66}));
  EXPECT_EQ(r[1], Value(int64_t{66}));
  EXPECT_EQ(r[2], Value(int64_t{0}));
  EXPECT_EQ(r[3], Value(int64_t{2080}));  // 1..64 plus two extra zeros.
  EXPECT_DOUBLE_EQ(r[4].value.double_value, 2080.0 / 66.0);
  EXPECT_EQ(r[5], Value(int64_t{0}));
  EXPECT_EQ(r[6], Value(int64_t{64}));
  // The NaN row poisons every float aggregate.
  EXPECT_TRUE(std::isnan(r[7].value.double_value));
  EXPECT_TRUE(std::isnan(r[8].value.double_value));
  EXPECT_TRUE(std::isnan(r[9].value.double_value));
  EXPECT_TRUE(std::isnan(r[10].value.double_value));
  EXPECT_EQ(r[11].value.int_value, int64_t{4160});  // uint64-named column.
  EXPECT_EQ(r[12], Value(int64_t{0}));
  EXPECT_EQ(r[13], Value(int64_t{127}));
  EXPECT_EQ(r[14], Value(int64_t{64}));
  EXPECT_FALSE(r[15].Truthy());
  EXPECT_TRUE(r[16].Truthy());
  // 66 rows fed all 17 typed column aggregates (66 * 17 = 1122 updates), so
  // the sweep touched every row/aggregate pair rather than short-circuiting.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("aggregate_input_rows=66"), std::string::npos) << *dump;
  EXPECT_NE(dump->find("aggregate_updates=1122"), std::string::npos) << *dump;
}

TEST_F(AggSmallCov, ConstantAggregatesSkipColumnDecoding) {
  const std::string sql =
      "SELECT SUM(5), SUM(2.5), AVG(4), MIN(3), MAX(7), COUNT(2), "
      "BIT_AND(9), BIT_OR(8), BIT_XOR(7), LOGICAL_AND(TRUE), "
      "LOGICAL_OR(FALSE) FROM med";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row& r = rows.Value()[0];
  EXPECT_EQ(r[0], Value(int64_t{330}));
  EXPECT_DOUBLE_EQ(r[1].value.double_value, 165.0);
  EXPECT_DOUBLE_EQ(r[2].value.double_value, 4.0);
  EXPECT_EQ(r[3], Value(int64_t{3}));
  EXPECT_EQ(r[4], Value(int64_t{7}));
  EXPECT_EQ(r[5], Value(int64_t{66}));
  EXPECT_EQ(r[6], Value(int64_t{9}));
  EXPECT_EQ(r[7], Value(int64_t{8}));
  // 66 rows: 7 XORed an even number of times cancels out.
  EXPECT_EQ(r[8], Value(int64_t{0}));
  EXPECT_TRUE(r[9].Truthy());
  EXPECT_FALSE(r[10].Truthy());
  // Every aggregate argument is a literal, so the scan decodes no column
  // values (scan_values_decoded=0) even though it still counts 66 rows.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("scan_values_decoded=0"), std::string::npos) << *dump;
}

TEST_F(AggSmallCov, AllNullColumnAggregatesEvaluateToNull) {
  const std::string sql =
      "SELECT SUM(nul), AVG(nul), MIN(nul), MAX(nul), COUNT(nul), "
      "BIT_AND(nul), BIT_OR(nul), BIT_XOR(nul), LOGICAL_AND(nul), "
      "LOGICAL_OR(nul) FROM med";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row& r = rows.Value()[0];
  EXPECT_TRUE(r[0].IsNull());
  EXPECT_TRUE(r[1].IsNull());
  EXPECT_TRUE(r[2].IsNull());
  EXPECT_TRUE(r[3].IsNull());
  EXPECT_EQ(r[4], Value(int64_t{0}));
  EXPECT_TRUE(r[5].IsNull());
  EXPECT_TRUE(r[6].IsNull());
  EXPECT_TRUE(r[7].IsNull());
  EXPECT_TRUE(r[8].IsNull());
  EXPECT_TRUE(r[9].IsNull());
  // There is no all-NULL short circuit: all 66 rows still feed all 10
  // aggregates (66 * 10 = 660 updates), each folding NULLs to the shown
  // NULL/0 results.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("aggregate_input_rows=66"), std::string::npos) << *dump;
  EXPECT_NE(dump->find("aggregate_updates=660"), std::string::npos) << *dump;
}

TEST_F(AggSmallCov, GenericMixedListKeepsTypedAggregatesCorrect) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(a), BIT_AND(a), BIT_OR(a), BIT_XOR(a), "
      "LOGICAL_AND(logi), LOGICAL_OR(logi), STRING_AGG(s, ',') FROM med");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row& r = rows.Value()[0];
  EXPECT_EQ(r[0], Value(int64_t{2080}));
  EXPECT_EQ(r[1], Value(int64_t{0}));
  EXPECT_EQ(r[2], Value(int64_t{127}));
  EXPECT_EQ(r[3], Value(int64_t{64}));
  EXPECT_FALSE(r[4].Truthy());
  EXPECT_TRUE(r[5].Truthy());
  EXPECT_FALSE(r[6].IsNull());
  // med.s is CAST(MOD(n,7) AS STRING) for n = 1..64 followed by 'x','y', so
  // the join is nine "1,2,3,4,5,6,0" cycles, a trailing '1' (n = 64), then
  // 'x','y' — 66 elements in insertion order.
  std::string expected_join;
  for (int i = 0; i < 9; ++i) {
    if (i > 0) {
      expected_join += ',';
    }
    expected_join += "1,2,3,4,5,6,0";
  }
  expected_join += ",1,x,y";
  EXPECT_EQ(std::string(r[6].AsStringView()), expected_join);
}

TEST_F(AggSmallCov, DistinctAggregatesRouteThroughAccumulators) {
  const std::string sql =
      "SELECT COUNT(DISTINCT s), SUM(DISTINCT a), MIN(DISTINCT s), "
      "MAX(DISTINCT s), COUNT(DISTINCT a) FROM med";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row& r = rows.Value()[0];
  EXPECT_EQ(r[0], Value(int64_t{9}));  // '0'..'6', 'x', 'y'.
  EXPECT_EQ(r[1], Value(int64_t{2080}));
  EXPECT_EQ(r[2], Value("0"));
  EXPECT_EQ(r[3], Value("y"));
  EXPECT_EQ(r[4], Value(int64_t{65}));
  // DISTINCT members are folded through the hash-table accumulator path, not
  // the typed batch sweep: the plan shows a HashAggregate consuming all 66
  // input rows into one group.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("HashAggregate"), std::string::npos) << *dump;
  EXPECT_NE(dump->find("input_rows=66"), std::string::npos) << *dump;
}

TEST_F(AggSmallCov, FilteredAggregatesSkipNonMatchingRows) {
  const std::string sql =
      "SELECT COUNT(* WHERE a > 32), SUM(a WHERE a > 32), "
      "MIN(a WHERE a > 32), SUM(a WHERE a > 1000) "
      "FROM med";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row& r = rows.Value()[0];
  EXPECT_EQ(r[0], Value(int64_t{32}));
  EXPECT_EQ(r[1], Value(int64_t{1552}));
  EXPECT_EQ(r[2], Value(int64_t{33}));
  EXPECT_TRUE(r[3].IsNull());
  // The FILTER (WHERE ...) clauses survive into the aggregate plan rather
  // than being silently dropped — each member keeps its own predicate text.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("a > 32"), std::string::npos) << *dump;
  EXPECT_NE(dump->find("a > 1000"), std::string::npos) << *dump;
}

TEST_F(AggSmallCov, SumOverVarcharRaises) {
  const auto result =
      TryRunSql(engine_.get(), context_.get(), "SELECT SUM(s) FROM med");
  ASSERT_FALSE(result.HasValue());
  EXPECT_NE(result.GetStatus().GetMessage().find("numeric value required"),
            std::string::npos)
      << result.GetStatus().GetMessage();
}

TEST_F(AggSmallCov, IntegerAndDoubleSumOverflowRaise) {
  EXPECT_FALSE(TryRunSql(engine_.get(), context_.get(),
                         "SELECT SUM(v), COUNT(v) FROM "
                         "UNNEST([9223372036854775807, 1]) AS v")
                   .HasValue());
  EXPECT_FALSE(TryRunSql(engine_.get(), context_.get(),
                         "SELECT SUM(f), COUNT(f) FROM "
                         "UNNEST([1e308, 1e308]) AS f")
                   .HasValue());
}

TEST_F(AggSmallCov, EmptyInputScalarAggregates) {
  const std::string sql =
      "SELECT SUM(a), AVG(a), COUNT(*), MIN(a), STRING_AGG(s, ',') FROM med "
      "WHERE 1 = 0";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row& r = rows.Value()[0];
  EXPECT_TRUE(r[0].IsNull());
  EXPECT_TRUE(r[1].IsNull());
  EXPECT_EQ(r[2], Value(int64_t{0}));
  EXPECT_TRUE(r[3].IsNull());
  EXPECT_TRUE(r[4].IsNull());
  // The empty input is observable: the scalar aggregate consumed zero rows
  // (input_rows=0) yet still emitted the single NULL/0 group row.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("input_rows=0"), std::string::npos) << *dump;
}

TEST_F(AggSmallCov, PercentileAndQuantilesReturnNullOnEmptyInput) {
  const auto rows =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT PERCENTILE_CONT(a, 0.5), PERCENTILE_DISC(a, 0.5), "
                "APPROX_QUANTILES(a, 2) FROM med WHERE FALSE");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus();
  ASSERT_EQ(rows.Value().size(), 1U);
  for (const auto& value : rows.Value()[0].values_) {
    EXPECT_TRUE(value.IsNull());
  }
}

TEST_F(AggSmallCov, SingleSumUsesCheckedTypedFoldPath) {
  const std::string sql = "SELECT SUM(a) FROM med";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  EXPECT_EQ(rows.Value()[0][0], Value(int64_t{2080}));
  // A lone SUM over a scanned column folds through the checked typed path
  // (fused-typed-fold), which is what makes the overflow below safe.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("fused-typed-fold"), std::string::npos) << *dump;
  const auto overflow =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT SUM(v) FROM UNNEST([9223372036854775807, 1]) AS v");
  ASSERT_FALSE(overflow.HasValue());
  EXPECT_NE(overflow.GetStatus().GetMessage().find("integer overflow in SUM"),
            std::string::npos)
      << overflow.GetStatus().GetMessage();
}

// ---------------------------------------------------------------------------
// Grouped aggregation coverage over 10k rows. This is below the current
// parallel threshold; aggregate_shape_test verifies parallel operator
// selection with inputs sized from kParallelAggregationMinRows.
// ---------------------------------------------------------------------------

class AggBigCov : public ::testing::Test {
 protected:
  static constexpr int64_t kRows = 10000;

  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("agg_big-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "CREATE TABLE big (g INT64, a INT64, f FLOAT64, "
                          "nul INT64, u_uint64 INT64, huge INT64, "
                          "s VARCHAR(20));")
                    .HasValue());
    EXPECT_TRUE(
        TryRunSql(engine_.get(), context_.get(),
                  "INSERT INTO big SELECT MOD(n, 3), n, n * 1.5, NULL, n * 3, "
                  "CASE WHEN n <= 2 THEN 9223372036854775807 ELSE 0 END, "
                  "CAST(MOD(n, 7) AS STRING) "
                  "FROM UNNEST(GENERATE_ARRAY(1, 10000)) AS n;")
            .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO big VALUES (0, 0, CAST('NaN' AS "
                          "FLOAT64), NULL, 0, 0, 'zz');")
                    .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO big VALUES (0, 0, NULL, NULL, 0, 0, "
                          "'zz');")
                    .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO big VALUES (NULL, 1, 2.5, NULL, 3, 0, "
                          "'zz');")
                    .HasValue());
    // A small dimension table for the join-driven runtime key filter test.
    EXPECT_TRUE(
        TryRunSql(engine_.get(), context_.get(), "CREATE TABLE med (a INT64);")
            .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO med SELECT n FROM "
                          "UNNEST(GENERATE_ARRAY(1, 64)) AS n;")
                    .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO med VALUES (0), (0);")
                    .HasValue());
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  // Reference sums for group g: a = {g, g+3, ...} <= 10000.
  static int64_t SumFor(int64_t g) {
    int64_t total = 0;
    for (int64_t n = g; n <= kRows; n += 3) {
      total += n;
    }
    return total;
  }
  static int64_t CountFor(int64_t g) {
    int64_t count = 0;
    for (int64_t n = g; n <= kRows; n += 3) {
      ++count;
    }
    return count;
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(AggBigCov, GroupedWideAggregateSweep) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT g, COUNT(*), COUNT(a), COUNT(f), COUNT(nul), SUM(a), AVG(a), "
      "MIN(a), MAX(a), SUM(f), SUM(u_uint64), LOGICAL_AND(a), LOGICAL_OR(a) "
      "FROM big GROUP BY g");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 4U);  // g = NULL, 0, 1, 2.
  const Row* null_group = FindRow(rows.Value(), 0, Value());
  ASSERT_NE(null_group, nullptr);
  EXPECT_EQ((*null_group)[1], Value(int64_t{1}));
  EXPECT_EQ((*null_group)[4], Value(int64_t{0}));
  EXPECT_EQ((*null_group)[5], Value(int64_t{1}));

  const Row* g0 = FindRow(rows.Value(), 0, Value(int64_t{0}));
  ASSERT_NE(g0, nullptr);
  // g=0: 3333 generated rows + the NaN and NULL-f rows = 3335.
  EXPECT_EQ((*g0)[1], Value(int64_t{3335}));
  EXPECT_EQ((*g0)[2], Value(int64_t{3335}));
  EXPECT_EQ((*g0)[3], Value(int64_t{3334}));  // one NULL f.
  EXPECT_EQ((*g0)[4], Value(int64_t{0}));
  EXPECT_EQ((*g0)[5], Value(int64_t{SumFor(0)}));
  EXPECT_TRUE(std::isnan((*g0)[9].value.double_value));  // NaN poisons.
  EXPECT_EQ((*g0)[10].value.int_value, 3 * SumFor(0));
  // a=0 rows make LOGICAL_AND false; a has non-zero values for OR.
  EXPECT_EQ((*g0)[11], Value(int64_t{0}));
  EXPECT_EQ((*g0)[12], Value(int64_t{1}));

  const Row* g1 = FindRow(rows.Value(), 0, Value(int64_t{1}));
  ASSERT_NE(g1, nullptr);
  EXPECT_EQ((*g1)[1], Value(int64_t{CountFor(1)}));
  EXPECT_EQ((*g1)[5], Value(int64_t{SumFor(1)}));
  EXPECT_DOUBLE_EQ(
      (*g1)[6].value.double_value,
      static_cast<double>(SumFor(1)) / static_cast<double>(CountFor(1)));
  EXPECT_EQ((*g1)[7], Value(int64_t{1}));
  EXPECT_EQ((*g1)[8], Value(int64_t{10000}));
  EXPECT_DOUBLE_EQ((*g1)[9].value.double_value,
                   1.5 * static_cast<double>(SumFor(1)));
  EXPECT_EQ((*g1)[10].value.int_value, 3 * SumFor(1));
}

TEST_F(AggBigCov, GroupedStatisticsMatchClosedForms) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT g, VAR_POP(a), VAR_SAMP(a), STDDEV_POP(a), STDDEV_SAMP(a), "
      "COVAR_POP(a, f), COVAR_SAMP(a, f), CORR(a, f) FROM big GROUP BY g");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row* g1 = FindRow(rows.Value(), 0, Value(int64_t{1}));
  ASSERT_NE(g1, nullptr);
  // Reference moments for a = 1, 4, ..., 10000 and f = 1.5 * a.
  const auto n = static_cast<double>(CountFor(1));
  double sx = 0, sxx = 0, sy = 0, sxy = 0;
  for (int64_t v = 1; v <= kRows; v += 3) {
    const auto dv = static_cast<double>(v);
    sx += dv;
    sxx += (dv * dv);
    sy += (1.5 * dv);
    sxy += ((1.5 * dv) * dv);
  }
  const double mean = sx / n;
  const double var_pop = (sxx / n) - (mean * mean);
  EXPECT_NEAR((*g1)[1].value.double_value, var_pop, (var_pop * 1e-9) + 1e-9);
  EXPECT_NEAR((*g1)[2].value.double_value, var_pop * n / (n - 1.0),
              (var_pop * 1e-9) + 1e-9);
  EXPECT_NEAR((*g1)[3].value.double_value, std::sqrt(var_pop),
              (std::sqrt(var_pop) * 1e-9) + 1e-9);
  const double covar_pop = (sxy / n) - (mean * (sy / n));
  EXPECT_NEAR((*g1)[5].value.double_value, covar_pop,
              (std::abs(covar_pop) * 1e-9) + 1e-9);
  EXPECT_NEAR((*g1)[6].value.double_value, covar_pop * n / (n - 1.0),
              (std::abs(covar_pop) * 1e-9) + 1e-9);
  EXPECT_NEAR((*g1)[7].value.double_value, 1.0, 1e-9);

  // VAR_SAMP with a single sample row is NULL (the NULL-key group).
  const Row* null_group = FindRow(rows.Value(), 0, Value());
  ASSERT_NE(null_group, nullptr);
  EXPECT_TRUE((*null_group)[2].IsNull());
  EXPECT_TRUE((*null_group)[6].IsNull());
  EXPECT_TRUE((*null_group)[7].IsNull());
}

TEST_F(AggBigCov, GroupedDistinctAggregatesReplayThroughMerge) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT g, COUNT(DISTINCT s), SUM(DISTINCT a), MIN(DISTINCT a), "
      "MAX(DISTINCT a), AVG(DISTINCT a) FROM big GROUP BY g");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 4U);
  const Row* g1 = FindRow(rows.Value(), 0, Value(int64_t{1}));
  ASSERT_NE(g1, nullptr);
  EXPECT_EQ((*g1)[1], Value(int64_t{7}));  // n % 7 covers '0'..'6'.
  EXPECT_EQ((*g1)[2], Value(int64_t{SumFor(1)}));
  EXPECT_EQ((*g1)[3], Value(int64_t{1}));
  EXPECT_EQ((*g1)[4], Value(int64_t{10000}));
  EXPECT_DOUBLE_EQ(
      (*g1)[5].value.double_value,
      static_cast<double>(SumFor(1)) / static_cast<double>(CountFor(1)));
}

// DISTINCT statistical aggregates over a 10k-row GROUP BY must still merge
// correctly per group.  No runtime counter exposes the worker count and the
// table is below kParallelAggregationMinRows anyway, so this pins the merged
// per-group values rather than claiming a single-worker route.
TEST_F(AggBigCov, GroupedDistinctStatisticsEvaluatePerGroup) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT g, VAR_POP(DISTINCT a), CORR(DISTINCT a, f) FROM big GROUP BY "
      "g");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row* g1 = FindRow(rows.Value(), 0, Value(int64_t{1}));
  ASSERT_NE(g1, nullptr);
  const auto n = static_cast<double>(CountFor(1));
  double sx = 0, sxx = 0;
  for (int64_t v = 1; v <= kRows; v += 3) {
    const auto dv = static_cast<double>(v);
    sx += dv;
    sxx += (dv * dv);
  }
  const double mean = sx / n;
  const double var_pop = (sxx / n) - (mean * mean);
  EXPECT_NEAR((*g1)[1].value.double_value, var_pop, (var_pop * 1e-9) + 1e-9);
  // a and f are perfectly correlated within the group.
  EXPECT_NEAR((*g1)[2].value.double_value, 1.0, 1e-9);
}

TEST_F(AggBigCov, GroupedFilteredAggregatesApplyRowFilters) {
  int64_t big_count = 0;
  int64_t big_sum = 0;
  for (int64_t v = 1; v <= kRows; v += 3) {
    if (v > 5000) {
      ++big_count;
      big_sum += v;
    }
  }
  const std::string sql =
      "SELECT g, COUNT(* WHERE a > 5000), SUM(a WHERE a > "
      "5000), AVG(a WHERE a > 0), VAR_POP(a WHERE a > 0) "
      "FROM big GROUP BY g";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row* g1 = FindRow(rows.Value(), 0, Value(int64_t{1}));
  ASSERT_NE(g1, nullptr);
  EXPECT_EQ((*g1)[1], Value(big_count));
  EXPECT_EQ((*g1)[2], Value(big_sum));
  EXPECT_DOUBLE_EQ(
      (*g1)[3].value.double_value,
      static_cast<double>(SumFor(1)) / static_cast<double>(CountFor(1)));
  // FILTER (WHERE a > 0) keeps every row of the group.
  double sx = 0, sxx = 0;
  for (int64_t v = 1; v <= kRows; v += 3) {
    const auto dv = static_cast<double>(v);
    sx += dv;
    sxx += (dv * dv);
  }
  const auto n = static_cast<double>(CountFor(1));
  const double mean = sx / n;
  const double var_pop = (sxx / n) - (mean * mean);
  EXPECT_NEAR((*g1)[4].value.double_value, var_pop, (var_pop * 1e-9) + 1e-9);
  // The grouped aggregation consumed all 10003 input rows into 4 groups and
  // applied each member's row filter during accumulation: 30002 updates is
  // exactly the per-(row,aggregate) pairs whose predicate held.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("aggregate_input_rows=10003"), std::string::npos)
      << *dump;
  EXPECT_NE(dump->find("aggregate_groups=4"), std::string::npos) << *dump;
  EXPECT_NE(dump->find("aggregate_updates=30002"), std::string::npos) << *dump;
}

// Statistical aggregates over expression children ((a - g), (f - g)) keep the
// per-row generic accumulation path; adding a per-group constant never
// changes variance or correlation.  The counters show every one of the 10003
// input rows fed both aggregates (20006 updates).
TEST_F(AggBigCov, StatisticalAggregatesOnExpressionChildren) {
  const std::string sql =
      "SELECT g, VAR_POP(a - g), CORR(a - g, f - g) FROM big GROUP BY g";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row* g2 = FindRow(rows.Value(), 0, Value(int64_t{2}));
  ASSERT_NE(g2, nullptr);
  const auto n = static_cast<double>(CountFor(2));
  double sx = 0, sxx = 0;
  for (int64_t v = 2; v <= kRows; v += 3) {
    const auto dv = static_cast<double>(v);
    sx += dv;
    sxx += (dv * dv);
  }
  const double mean = sx / n;
  const double var_pop = (sxx / n) - (mean * mean);
  EXPECT_NEAR((*g2)[1].value.double_value, var_pop, (var_pop * 1e-9) + 1e-9);
  EXPECT_NEAR((*g2)[2].value.double_value, 1.0, 1e-9);
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("aggregate_input_rows=10003"), std::string::npos)
      << *dump;
  EXPECT_NE(dump->find("aggregate_updates=20006"), std::string::npos) << *dump;
}

TEST_F(AggBigCov, ParallelSumOverflowRaises) {
  // Multi-aggregate: the parallel typed int64 path must raise instead of
  // wrapping.
  const auto multi = TryRunSql(engine_.get(), context_.get(),
                               "SELECT SUM(huge), COUNT(*) FROM big");
  ASSERT_FALSE(multi.HasValue());
  EXPECT_NE(multi.GetStatus().GetMessage().find("integer overflow in SUM"),
            std::string::npos)
      << multi.GetStatus().GetMessage();
  // Single SUM: the checked typed fold raises too.
  const auto single =
      TryRunSql(engine_.get(), context_.get(), "SELECT SUM(huge) FROM big");
  ASSERT_FALSE(single.HasValue());
  EXPECT_NE(single.GetStatus().GetMessage().find("integer overflow in SUM"),
            std::string::npos)
      << single.GetStatus().GetMessage();
}

TEST_F(AggBigCov, ParallelScanAppliesSimpleAndResidualFilters) {
  // Statistics let the planner pick the parallel scan; the mixed predicate
  // (g >= 0 simple, a / 2 >= 0 residual, s != 'zz' simple) must be applied by
  // the worker-filter on the scan side.
  ASSERT_TRUE(
      TryRunSql(engine_.get(), context_.get(), "ANALYZE big;").HasValue());
  const std::string sql =
      "SELECT COUNT(*) FROM big WHERE g >= 0 AND a / 2 >= 0 AND s != 'zz'";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  EXPECT_EQ(rows.Value()[0][0], Value(int64_t{10000}));
  // 10002 rows have a non-NULL g (10000 generated + two g=0 extras);
  // two of them carry s = 'zz'.  The dump proves the parallel worker-filter
  // scan ran rather than a serial full scan.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("ParallelScan: big ("), std::string::npos) << *dump;
  EXPECT_NE(dump->find("worker-filter"), std::string::npos) << *dump;
}

TEST_F(AggBigCov, JoinInstallsIntegerKeyFilterOnProbeSide) {
  // Statistics drive the join to build a runtime exact-set filter over big.a
  // and probe med with it; the filter must not change the answer (69
  // matching pairs).
  ASSERT_TRUE(
      TryRunSql(engine_.get(), context_.get(), "ANALYZE big;").HasValue());
  const std::string sql =
      "SELECT COUNT(*) FROM med JOIN big ON med.a = big.a WHERE big.a >= 0";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  EXPECT_EQ(rows.Value()[0][0], Value(int64_t{69}));
  // The dump proves a runtime join filter was installed and consulted at
  // least one scan.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("RuntimeFilter type="), std::string::npos) << *dump;
  EXPECT_NE(dump->find("scans="), std::string::npos) << *dump;
}

// ---------------------------------------------------------------------------
// Direct CompiledScanFilter coverage: compile/evaluate branches that typed
// SQL cannot spell (constant-left comparisons, DATE-vs-string literals,
// mixed numeric operands, disjunctive branch errors).
// ---------------------------------------------------------------------------

class ScanFilterDirectTest : public ::testing::Test {
 protected:
  using CompiledScanFilter = relational_detail::CompiledScanFilter;

  void SetUp() override {
    database_ =
        Database::Create("scan_filter_cov-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  }
  void TearDown() override {
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  bool Match(const CompiledScanFilter& filter, const Row& row,
             const Schema& schema, Status* error = nullptr) {
    Status sink{Status::kSuccess};
    Status* err = error != nullptr ? error : &sink;
    return relational_detail::MatchScanFilter(row, schema, filter, nullptr,
                                              *context_, {}, err);
  }

  static CompiledScanFilter Compile(const std::vector<Expression>& preds,
                                    const Schema& schema) {
    return relational_detail::CompileScanFilter(preds, schema);
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
};

TEST_F(ScanFilterDirectTest, SimpleCompareEveryOpBothOperandOrders) {
  const Schema schema("t", {Column("v", ValueType::kInt64)});
  const auto cmp = [](BinaryOperation op, int64_t l, int64_t r) {
    switch (op) {
      case BinaryOperation::kEquals:
        return l == r;
      case BinaryOperation::kNotEquals:
        return l != r;
      case BinaryOperation::kLessThan:
        return l < r;
      case BinaryOperation::kLessThanEquals:
        return l <= r;
      case BinaryOperation::kGreaterThan:
        return l > r;
      case BinaryOperation::kGreaterThanEquals:
        return l >= r;
      default:
        return false;
    }
  };
  const std::vector<BinaryOperation> ops = {
      BinaryOperation::kEquals,      BinaryOperation::kNotEquals,
      BinaryOperation::kLessThan,    BinaryOperation::kLessThanEquals,
      BinaryOperation::kGreaterThan, BinaryOperation::kGreaterThanEquals};
  for (const BinaryOperation op : ops) {
    // Column on the left.
    CompiledScanFilter f =
        Compile({BinaryExpressionExp(ColumnValueExp("v"), op,
                                     ConstantValueExp(Value(int64_t{3})))},
                schema);
    EXPECT_EQ(Match(f, Row({Value(int64_t{3})}), schema), cmp(op, 3, 3))
        << static_cast<int>(op);
    EXPECT_EQ(Match(f, Row({Value(int64_t{1})}), schema), cmp(op, 1, 3))
        << static_cast<int>(op);
    // Constant on the left exercises FlipCompare.
    CompiledScanFilter flipped =
        Compile({BinaryExpressionExp(ConstantValueExp(Value(int64_t{3})), op,
                                     ColumnValueExp("v"))},
                schema);
    EXPECT_EQ(flipped.simple.size(), 1U) << static_cast<int>(op);
    EXPECT_EQ(Match(flipped, Row({Value(int64_t{3})}), schema), cmp(op, 3, 3))
        << static_cast<int>(op);
    EXPECT_EQ(Match(flipped, Row({Value(int64_t{2})}), schema), cmp(op, 3, 2))
        << static_cast<int>(op);
    EXPECT_EQ(Match(flipped, Row({Value(int64_t{4})}), schema), cmp(op, 3, 4))
        << static_cast<int>(op);
  }
}

TEST_F(ScanFilterDirectTest, DateColumnVersusStringConstants) {
  const Schema schema("t", {Column("d", ValueType::kDate)});
  const Value date = Value::Date("2020-01-15");
  struct OpCase {
    std::string literal;
    BinaryOperation op;
    bool expected;
  };
  const std::array<OpCase, 6> cases = {{
      {.literal = "2020-01-15",
       .op = BinaryOperation::kEquals,
       .expected = true},
      {.literal = "2020-01-15",
       .op = BinaryOperation::kNotEquals,
       .expected = false},
      {.literal = "2020-02-01",
       .op = BinaryOperation::kLessThan,
       .expected = true},
      {.literal = "2020-01-15",
       .op = BinaryOperation::kLessThanEquals,
       .expected = true},
      {.literal = "2020-01-01",
       .op = BinaryOperation::kGreaterThan,
       .expected = true},
      {.literal = "2020-01-16",
       .op = BinaryOperation::kGreaterThanEquals,
       .expected = false},
  }};
  for (const OpCase& c : cases) {
    CompiledScanFilter f = Compile(
        {BinaryExpressionExp(ColumnValueExp("d"), c.op,
                             ConstantValueExp(Value(c.literal.c_str())))},
        schema);
    EXPECT_EQ(Match(f, Row({date}), schema), c.expected)
        << c.literal << " op " << static_cast<int>(c.op);
  }
  // An unparsable date literal rejects the row without raising.
  CompiledScanFilter bad = Compile(
      {BinaryExpressionExp(ColumnValueExp("d"), BinaryOperation::kEquals,
                           ConstantValueExp(Value("not-a-date")))},
      schema);
  EXPECT_FALSE(Match(bad, Row({date}), schema));
  // NULL on either side never matches.
  CompiledScanFilter null_row = Compile(
      {BinaryExpressionExp(ColumnValueExp("d"), BinaryOperation::kEquals,
                           ConstantValueExp(Value("2020-01-15")))},
      schema);
  EXPECT_FALSE(Match(null_row, Row({Value()}), schema));
}

TEST_F(ScanFilterDirectTest, MixedNumericOperandTypes) {
  const Schema schema(
      "t", {Column("i", ValueType::kInt64), Column("f", ValueType::kDouble)});
  struct Case {
    Expression pred;
    Row row;
    bool expected;
  };
  const std::array<Case, 14> cases = {{
      // INT64 value vs DOUBLE constant: falls through to double coercion.
      {.pred = BinaryExpressionExp(ColumnValueExp("i"),
                                   BinaryOperation::kGreaterThan,
                                   ConstantValueExp(Value(1.5))),
       .row = Row({Value(int64_t{2}), Value(0.0)}),
       .expected = true},
      {.pred = BinaryExpressionExp(ColumnValueExp("i"),
                                   BinaryOperation::kGreaterThan,
                                   ConstantValueExp(Value(1.5))),
       .row = Row({Value(int64_t{1}), Value(0.0)}),
       .expected = false},
      // DOUBLE value vs INT64 constant.
      {.pred =
           BinaryExpressionExp(ColumnValueExp("f"), BinaryOperation::kEquals,
                               ConstantValueExp(Value(int64_t{2}))),
       .row = Row({Value(int64_t{0}), Value(2.0)}),
       .expected = true},
      {.pred =
           BinaryExpressionExp(ColumnValueExp("f"), BinaryOperation::kNotEquals,
                               ConstantValueExp(Value(int64_t{2}))),
       .row = Row({Value(int64_t{0}), Value(2.5)}),
       .expected = true},
      {.pred =
           BinaryExpressionExp(ColumnValueExp("f"), BinaryOperation::kLessThan,
                               ConstantValueExp(Value(int64_t{3}))),
       .row = Row({Value(int64_t{0}), Value(2.5)}),
       .expected = true},
      {.pred = BinaryExpressionExp(ColumnValueExp("f"),
                                   BinaryOperation::kLessThanEquals,
                                   ConstantValueExp(Value(int64_t{2}))),
       .row = Row({Value(int64_t{0}), Value(2.0)}),
       .expected = true},
      {.pred = BinaryExpressionExp(ColumnValueExp("f"),
                                   BinaryOperation::kGreaterThan,
                                   ConstantValueExp(Value(int64_t{2}))),
       .row = Row({Value(int64_t{0}), Value(2.5)}),
       .expected = true},
      {.pred = BinaryExpressionExp(ColumnValueExp("f"),
                                   BinaryOperation::kGreaterThanEquals,
                                   ConstantValueExp(Value(int64_t{3}))),
       .row = Row({Value(int64_t{0}), Value(2.5)}),
       .expected = false},
      // INT64 value vs VARCHAR constant: no numeric coercion, never matches.
      {.pred =
           BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kEquals,
                               ConstantValueExp(Value("3"))),
       .row = Row({Value(int64_t{3}), Value(0.0)}),
       .expected = false},
      {.pred =
           BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kNotEquals,
                               ConstantValueExp(Value("3"))),
       .row = Row({Value(int64_t{3}), Value(0.0)}),
       .expected = false},
      {.pred =
           BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kLessThan,
                               ConstantValueExp(Value("9"))),
       .row = Row({Value(int64_t{3}), Value(0.0)}),
       .expected = false},
      {.pred = BinaryExpressionExp(ColumnValueExp("i"),
                                   BinaryOperation::kLessThanEquals,
                                   ConstantValueExp(Value("9"))),
       .row = Row({Value(int64_t{3}), Value(0.0)}),
       .expected = false},
      {.pred = BinaryExpressionExp(ColumnValueExp("i"),
                                   BinaryOperation::kGreaterThan,
                                   ConstantValueExp(Value("1"))),
       .row = Row({Value(int64_t{3}), Value(0.0)}),
       .expected = false},
      {.pred = BinaryExpressionExp(ColumnValueExp("i"),
                                   BinaryOperation::kGreaterThanEquals,
                                   ConstantValueExp(Value("1"))),
       .row = Row({Value(int64_t{3}), Value(0.0)}),
       .expected = false},
  }};
  for (const Case& c : cases) {
    CompiledScanFilter f = Compile({c.pred}, schema);
    EXPECT_EQ(Match(f, c.row, schema), c.expected);
  }
}

TEST_F(ScanFilterDirectTest, DisjunctiveBranchesOrOfAnds) {
  const Schema schema(
      "t", {Column("v", ValueType::kInt64), Column("w", ValueType::kInt64)});
  Expression pred = BinaryExpressionExp(
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("v"),
                              BinaryOperation::kGreaterThan,
                              ConstantValueExp(Value(int64_t{1}))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(
              BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kAdd,
                                  ColumnValueExp("w")),
              BinaryOperation::kGreaterThan,
              ConstantValueExp(Value(int64_t{3})))),
      BinaryOperation::kOr,
      BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kLessThan,
                          ConstantValueExp(Value(int64_t{0}))));
  CompiledScanFilter f = Compile({pred}, schema);
  ASSERT_EQ(f.disjunctive_branches.size(), 2U);
  EXPECT_FALSE(f.disjunctive_branches[0].residual.empty());
  EXPECT_TRUE(Match(f, Row({Value(int64_t{2}), Value(int64_t{3})}), schema));
  EXPECT_TRUE(Match(f, Row({Value(int64_t{-1}), Value(int64_t{0})}), schema));
  EXPECT_FALSE(Match(f, Row({Value(int64_t{1}), Value(int64_t{9})}), schema));
}

TEST_F(ScanFilterDirectTest, DisjunctiveCrossProductAndTooWideGuard) {
  const Schema schema(
      "t", {Column("v", ValueType::kInt64), Column("w", ValueType::kInt64)});
  auto eq = [](const std::string& col, int64_t value) {
    return BinaryExpressionExp(ColumnValueExp(col), BinaryOperation::kEquals,
                               ConstantValueExp(Value(value)));
  };
  Expression six_way_v = eq("v", 1);
  Expression six_way_w = eq("w", 1);
  for (int i = 2; i <= 6; ++i) {
    six_way_v = BinaryExpressionExp(six_way_v, BinaryOperation::kOr,
                                    eq("v", static_cast<int64_t>(i)));
    six_way_w = BinaryExpressionExp(six_way_w, BinaryOperation::kOr,
                                    eq("w", static_cast<int64_t>(i)));
  }
  // Two 6-way OR predicates would cross into 36 branches, exceeding
  // kMaxDisjunctiveBranches: the second predicate falls back to residual.
  CompiledScanFilter wide = Compile({six_way_v, six_way_w}, schema);
  ASSERT_EQ(wide.disjunctive_branches.size(), 6U);
  EXPECT_EQ(wide.residual.size(), 1U);
  EXPECT_TRUE(Match(wide, Row({Value(int64_t{6}), Value(int64_t{1})}), schema));
  EXPECT_FALSE(
      Match(wide, Row({Value(int64_t{7}), Value(int64_t{1})}), schema));

  // 3x2 = 6 branches stay within the guard and build a cross product.
  Expression three_way = BinaryExpressionExp(
      eq("v", 1), BinaryOperation::kOr,
      BinaryExpressionExp(eq("v", 2), BinaryOperation::kOr, eq("v", 3)));
  Expression two_way =
      BinaryExpressionExp(eq("w", 4), BinaryOperation::kOr, eq("w", 5));
  CompiledScanFilter crossed = Compile({three_way, two_way}, schema);
  ASSERT_EQ(crossed.disjunctive_branches.size(), 6U);
  EXPECT_TRUE(
      Match(crossed, Row({Value(int64_t{2}), Value(int64_t{5})}), schema));
  EXPECT_FALSE(
      Match(crossed, Row({Value(int64_t{2}), Value(int64_t{6})}), schema));
}

TEST_F(ScanFilterDirectTest, BranchResidualErrorIsReportedToCaller) {
  const Schema schema("t", {Column("v", ValueType::kInt64)});
  // (v = 1 AND 10/(v - v) > 0) OR v = 2: evaluating row v=1 raises inside the
  // first branch's residual; no branch passes and the error surfaces.
  Expression division = BinaryExpressionExp(
      ConstantValueExp(Value(int64_t{10})), BinaryOperation::kDivide,
      BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kSubtract,
                          ColumnValueExp("v")));
  Expression pred = BinaryExpressionExp(
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(int64_t{1}))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(division, BinaryOperation::kGreaterThan,
                              ConstantValueExp(Value(int64_t{0})))),
      BinaryOperation::kOr,
      BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{2}))));
  CompiledScanFilter f = Compile({pred}, schema);
  ASSERT_EQ(f.disjunctive_branches.size(), 2U);
  Status error{Status::kSuccess};
  EXPECT_FALSE(Match(f, Row({Value(int64_t{1})}), schema, &error));
  EXPECT_FALSE(error.ok());

  // The second branch passes cleanly, so no error is reported.
  Status ok_error{Status::kSuccess};
  EXPECT_TRUE(Match(f, Row({Value(int64_t{2})}), schema, &ok_error));
  EXPECT_TRUE(ok_error.ok());
}

TEST_F(ScanFilterDirectTest, ResidualBytecodeErrorFallsBackPerConjunct) {
  const Schema schema("t", {Column("v", ValueType::kInt64)});
  // 10/(v-v) > 0 raises on every row: bytecode eval fails, the per-conjunct
  // fallback records the error, and the row is rejected.
  Expression division = BinaryExpressionExp(
      ConstantValueExp(Value(int64_t{10})), BinaryOperation::kDivide,
      BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kSubtract,
                          ColumnValueExp("v")));
  CompiledScanFilter f =
      Compile({BinaryExpressionExp(division, BinaryOperation::kGreaterThan,
                                   ConstantValueExp(Value(int64_t{0})))},
              schema);
  EXPECT_TRUE(f.residual_bytecode.has_value() || !f.residual.empty());
  Status error{Status::kSuccess};
  EXPECT_FALSE(Match(f, Row({Value(int64_t{1})}), schema, &error));
  EXPECT_FALSE(error.ok());
}

TEST_F(ScanFilterDirectTest, ResidualOnlyFiltersUseBytecode) {
  const Schema schema("t", {Column("v", ValueType::kInt64)});
  CompiledScanFilter f = Compile(
      {BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kModulo,
                              ConstantValueExp(Value(int64_t{2}))),
          BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{0})))},
      schema);
  // v % 2 is not a simple compare, so the whole predicate is residual and is
  // carried as compiled bytecode rather than a per-row AST residual.
  EXPECT_TRUE(f.simple.empty());
  EXPECT_TRUE(f.residual_bytecode.has_value());
  EXPECT_TRUE(Match(f, Row({Value(int64_t{4})}), schema));
  EXPECT_FALSE(Match(f, Row({Value(int64_t{5})}), schema));
  EXPECT_FALSE(Match(f, Row({Value()}), schema));
}

TEST_F(ScanFilterDirectTest, UnsignedColumnsAreRetaggedBeforeMatching) {
  Column u("u", ValueType::kInt64);
  u.SetUnsigned(true);
  Schema schema("t", {u});
  CompiledScanFilter f = Compile(
      {BinaryExpressionExp(ColumnValueExp("u"), BinaryOperation::kLessThan,
                           ConstantValueExp(Value(int64_t{1})))},
      schema);
  EXPECT_TRUE(f.needs_unsigned_tagging);
  // Stored as INT64_MIN bit pattern; as UINT64 that is huge, so NOT < 1.
  EXPECT_FALSE(
      Match(f, Row({Value(std::numeric_limits<int64_t>::min())}), schema));
  EXPECT_TRUE(Match(f, Row({Value(int64_t{0})}), schema));
}

TEST_F(ScanFilterDirectTest, BuildIntegerPeeksSkipsUnusablePredicates) {
  Column unsigned_col("u", ValueType::kInt64);
  unsigned_col.SetUnsigned(true);
  const Schema schema(
      "t", {Column("i", ValueType::kInt64), Column("s", ValueType::kVarChar)});
  const Schema full("t", {Column("i", ValueType::kInt64),
                          Column("s", ValueType::kVarChar), unsigned_col});

  std::vector<Expression> preds = {
      BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(int64_t{3}))),
      BinaryExpressionExp(ColumnValueExp("s"), BinaryOperation::kEquals,
                          ConstantValueExp(Value("x"))),
      BinaryExpressionExp(ColumnValueExp("u"), BinaryOperation::kLessThan,
                          ConstantValueExp(Value(int64_t{9}))),
      BinaryExpressionExp(ColumnValueExp("nope"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{1})))};
  CompiledScanFilter filter = Compile(preds, schema);

  // No projection: column indexes pass through; the string and unsigned
  // predicates are skipped.
  std::vector<IntegerPeekCompare> peeks =
      relational_detail::BuildIntegerPeeks(filter, nullptr, full);
  ASSERT_EQ(peeks.size(), 1U);
  EXPECT_EQ(peeks[0].column, 0U);
  EXPECT_EQ(peeks[0].constant, int64_t{3});

  // Projection remaps slot 0 -> full-schema column 1 (VARCHAR) which is not
  // peekable, and drops out-of-range slots.
  const std::vector<slot_t> projection = {1};
  peeks = relational_detail::BuildIntegerPeeks(filter, &projection, full);
  EXPECT_TRUE(peeks.empty());
}

TEST_F(ScanFilterDirectTest, SplitAndCombineDisjuncts) {
  Expression or3 = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{1}))),
      BinaryOperation::kOr,
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(int64_t{2}))),
          BinaryOperation::kOr,
          BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(int64_t{3})))));
  std::vector<Expression> parts = relational_detail::SplitDisjuncts(or3);
  ASSERT_EQ(parts.size(), 3U);
  Expression combined = relational_detail::CombineDisjuncts(parts);
  EXPECT_EQ(relational_detail::SplitDisjuncts(combined).size(), 3U);
  EXPECT_FALSE(relational_detail::CombineDisjuncts({}));
}

TEST(LeanPhase3ParityTest,
     WindowDistinctInverseMatchesAcrossNullsDuplicatesAndExclusions) {
  // The body toggles this knob repeatedly and unsets it at the end; the guard
  // restores any externally supplied value even on an early ASSERT exit.
  EnvGuard wdi_env("TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE");
  auto db_or = Database::Create("lean_wdi_" + RandomString());
  ASSERT_EQ(db_or.GetStatus(), Status::kSuccess);
  auto db = db_or.MoveValue();
  {
    TransactionContext ctx = db->BeginContext();
    SqlEngine engine(*db);
    ASSERT_TRUE(
        engine.Execute(ctx, "CREATE TABLE t (id INT64, v INT64);").HasValue());
    ASSERT_TRUE(engine
                    .Execute(ctx,
                             "INSERT INTO t VALUES "
                             "(1, 10), (2, 10), (3, NULL), (4, -5), (5, 20), "
                             "(6, 10), (7, 20), (8, NULL), (9, 30), (10, -5);")
                    .HasValue());
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  const std::vector<std::string> queries = {
      "SELECT id, "
      "COUNT(DISTINCT v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 "
      "FOLLOWING), "
      "SUM(DISTINCT v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 "
      "FOLLOWING) "
      "FROM t ORDER BY id;",
      "SELECT id, "
      "COUNT(DISTINCT v) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 "
      "PRECEDING), "
      "SUM(DISTINCT v) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 3 "
      "FOLLOWING) "
      "FROM t ORDER BY id;",
      "SELECT id, "
      "COUNT(DISTINCT s) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND "
      "CURRENT ROW) "
      "FROM (SELECT id, CASE WHEN v IS NULL THEN NULL WHEN v < 0 THEN 'neg' "
      "WHEN v = 10 THEN 'ten' ELSE 'pos' END AS s FROM t) sub "
      "ORDER BY id;"};

  for (const std::string& q : queries) {
    auto eval = [&](bool disable) -> std::vector<Row> {
      if (disable) {
        ::tinylamb::platform::SetEnv("TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE",
                                     "1", 1);
      } else {
        ::tinylamb::platform::UnsetEnv(
            "TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE");
      }
      PreparedPlanCache::Instance().Clear();
      CurrentCompiledPlans().clear();
      TransactionContext ctx = db->BeginReadOnlyContext();
      SqlEngine engine(*db);
      auto rows = RunSql(&engine, &ctx, q);
      EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.PreCommit());
      return rows;
    };
    const auto on_rows = eval(false);
    const auto off_rows = eval(true);
    EXPECT_EQ(on_rows, off_rows) << q;
  }

  // Test EXCLUDE CURRENT ROW via AST + ApplyWindows (parser rejects EXCLUDE).
  {
    auto cnt_w = MakeWindow("COUNT", {ColumnValueExp("v")});
    cnt_w->distinct = true;
    cnt_w->order_by = {WindowOrderTerm{.expression = ColumnValueExp("id"),
                                       .ascending = true,
                                       .nulls_first = std::nullopt}};
    cnt_w->has_frame = true;
    cnt_w->frame_unit = WindowFrameUnit::kRows;
    cnt_w->frame_start = OffsetBound(WindowFrameBoundType::kOffsetPreceding, 2);
    cnt_w->frame_end = OffsetBound(WindowFrameBoundType::kOffsetFollowing, 2);
    cnt_w->exclusion = WindowFrameExclusion::kCurrentRow;

    auto sum_w = MakeWindow("SUM", {ColumnValueExp("v")});
    sum_w->distinct = true;
    sum_w->order_by = cnt_w->order_by;
    sum_w->has_frame = true;
    sum_w->frame_unit = WindowFrameUnit::kRows;
    sum_w->frame_start = cnt_w->frame_start;
    sum_w->frame_end = cnt_w->frame_end;
    sum_w->exclusion = WindowFrameExclusion::kCurrentRow;

    SelectStatement stmt({NamedExpression("id", ColumnValueExp("id")),
                          NamedExpression("c", Expression(cnt_w)),
                          NamedExpression("s", Expression(sum_w))},
                         {}, nullptr);
    const Schema schema(
        "t", {Column("id", ValueType::kInt64), Column("v", ValueType::kInt64)});
    const std::vector<Row> base_rows{
        Row({Value(1), Value(10)}), Row({Value(2), Value(10)}),
        Row({Value(3), Value()}),   Row({Value(4), Value(-5)}),
        Row({Value(5), Value(20)}), Row({Value(6), Value(10)}),
        Row({Value(7), Value(20)}), Row({Value(8), Value()})};
    auto run_excl = [&](bool disable) -> std::vector<Row> {
      if (disable) {
        ::tinylamb::platform::SetEnv("TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE",
                                     "1", 1);
      } else {
        ::tinylamb::platform::UnsetEnv(
            "TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE");
      }
      TransactionContext ctx = db->BeginReadOnlyContext();
      relational_detail::Relation rel;
      rel.schema = schema;
      rel.rows = base_rows;
      auto res_or = relational_detail::ApplyWindows(ctx, stmt, std::move(rel),
                                                    nullptr, {});
      if (!res_or.HasValue()) {
        ADD_FAILURE() << res_or.GetStatus();
        return {};
      }
      std::vector<Row> out;
      EXPECT_EQ(tinylamb::Status::kSuccess,
                res_or.Value().input.ForEachRow(
                    [&](const Row& r) { out.push_back(r); }));
      EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.PreCommit());
      return out;
    };
    const auto on_excl = run_excl(false);
    const auto off_excl = run_excl(true);
    EXPECT_EQ(on_excl.size(), base_rows.size());
    EXPECT_EQ(on_excl, off_excl);
  }
  ::tinylamb::platform::UnsetEnv("TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE");
  db->DeleteAll();
}

TEST(LeanPhase3ParityTest, WindowHaloAndDistinctOnAndOuterOrBranchesParity) {
  // The bodies below toggle these knobs repeatedly; the guards restore any
  // externally supplied values even on an early ASSERT exit.
  EnvGuard halo_env("TINYLAMB_DISABLE_WINDOW_HALO");
  EnvGuard distinct_on_env("TINYLAMB_DISABLE_DISTINCT_ON_TOP1");
  EnvGuard or_branches_env("TINYLAMB_DISABLE_OUTER_OR_BRANCHES");
  auto db_or = Database::Create("lean_p3sql_" + RandomString());
  ASSERT_EQ(db_or.GetStatus(), Status::kSuccess);
  auto db = db_or.MoveValue();
  {
    TransactionContext ctx = db->BeginContext();
    SqlEngine engine(*db);
    ASSERT_TRUE(
        engine.Execute(ctx, "CREATE TABLE w (id INT64, grp INT64, val INT64);")
            .HasValue());
    ASSERT_TRUE(
        engine.Execute(ctx, "CREATE TABLE lo (a INT64, b INT64, c INT64);")
            .HasValue());
    ASSERT_TRUE(
        engine.Execute(ctx, "CREATE TABLE ro (a INT64, b INT64, d INT64);")
            .HasValue());
    std::string inserts = "INSERT INTO w VALUES ";
    for (int i = 1; i <= 60; ++i) {
      if (i > 1) {
        inserts += ", ";
      }
      const std::string val_str =
          (i % 9 == 0) ? "NULL" : std::to_string(((i * 17) % 50) - 15);
      inserts += "(" + std::to_string(i) + ", " + std::to_string(i % 7) + ", " +
                 val_str + ")";
    }
    inserts += ";";
    ASSERT_TRUE(engine.Execute(ctx, inserts).HasValue());
    ASSERT_TRUE(
        engine
            .Execute(ctx,
                     "INSERT INTO lo VALUES "
                     "(1, 10, 1), (NULL, 20, 1), (99, 99, 1), (2, 30, -5);")
            .HasValue());
    ASSERT_TRUE(
        engine
            .Execute(ctx,
                     "INSERT INTO ro VALUES "
                     "(1, 10, 1), (50, 20, 1), (50, 20, 2), (2, 30, 3);")
            .HasValue());
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  auto eval_sorted = [&](const std::string& q, const char* disable_var,
                         bool disable) -> std::vector<Row> {
    if (disable) {
      ::tinylamb::platform::SetEnv(disable_var, "1", 1);
    } else {
      ::tinylamb::platform::UnsetEnv(disable_var);
    }
    PreparedPlanCache::Instance().Clear();
    CurrentCompiledPlans().clear();
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine engine(*db);
    auto rows = RunSql(&engine, &ctx, q);
    EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.PreCommit());
    return rows;
  };

  // WindowHalo on direct relational ApplyWindows path (both Row-backed and
  // Columnar-backed inputs) and SQL path.
  {
    auto win = MakeWindow("AVG", {ColumnValueExp("val")});
    win->order_by = {WindowOrderTerm{.expression = ColumnValueExp("id"),
                                     .ascending = true,
                                     .nulls_first = std::nullopt}};
    win->has_frame = true;
    win->frame_unit = WindowFrameUnit::kRows;
    win->frame_start = OffsetBound(WindowFrameBoundType::kOffsetPreceding, 2);
    win->frame_end = OffsetBound(WindowFrameBoundType::kOffsetFollowing, 2);

    SelectStatement stmt({NamedExpression("id", ColumnValueExp("id")),
                          NamedExpression("a", Expression(win))},
                         {}, nullptr);
    stmt.SetOrderBy({{ColumnValueExp("id"), true, std::nullopt}});
    stmt.SetLimit(7);
    stmt.SetOffset(12);

    const Schema schema("w", {Column("id", ValueType::kInt64),
                              Column("val", ValueType::kInt64)});
    std::vector<Row> base_rows;
    base_rows.reserve(60);
    for (int i = 60; i >= 1; --i) {  // intentionally unsorted input
      base_rows.push_back(
          Row({Value(int64_t{i}),
               (i % 7 == 0) ? Value() : Value(static_cast<int64_t>(i * 3))}));
    }

    auto run_apply = [&](bool disable) -> std::vector<Row> {
      if (disable) {
        ::tinylamb::platform::SetEnv("TINYLAMB_DISABLE_WINDOW_HALO", "1", 1);
      } else {
        ::tinylamb::platform::UnsetEnv("TINYLAMB_DISABLE_WINDOW_HALO");
      }
      TransactionContext ctx = db->BeginReadOnlyContext();
      relational_detail::Relation rel;
      rel.schema = schema;
      rel.rows = base_rows;
      auto win_or = relational_detail::ApplyWindows(
          ctx, stmt, std::move(rel), nullptr, {}, /*allow_window_halo=*/true);
      EXPECT_TRUE(win_or.HasValue()) << win_or.GetStatus();
      auto win_res = win_or.MoveValue();
      EXPECT_EQ(relational_detail::ApplyOrderBy(ctx, *win_res.statement,
                                                &win_res.input, nullptr, {}),
                Status::kSuccess);
      auto fin_or = relational_detail::LimitedRows(
          *win_res.statement, std::move(win_res.input), &ctx, nullptr, {});
      EXPECT_TRUE(fin_or.HasValue()) << fin_or.GetStatus();
      std::vector<Row> out;
      EXPECT_EQ(
          tinylamb::Status::kSuccess,
          fin_or.Value().ForEachRow([&](const Row& r) { out.push_back(r); }));
      EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.PreCommit());
      return out;
    };

    const auto on_halo = run_apply(false);
    const auto off_halo = run_apply(true);
    EXPECT_EQ(on_halo.size(), 7U);
    EXPECT_EQ(on_halo, off_halo);
  }
  for (const std::string& wq :
       {"SELECT id, AVG(val) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 "
        "FOLLOWING) AS a "
        "FROM w ORDER BY id LIMIT 10 OFFSET 15;",
        "SELECT id, SUM(val) OVER (ORDER BY id DESC ROWS BETWEEN 2 PRECEDING "
        "AND 4 FOLLOWING) AS s, "
        "COUNT(val) OVER (ORDER BY id DESC ROWS BETWEEN 1 PRECEDING AND "
        "CURRENT ROW) AS c "
        "FROM w ORDER BY id DESC LIMIT 12 OFFSET 5;"}) {
    const auto on_rows = eval_sorted(wq, "TINYLAMB_DISABLE_WINDOW_HALO", false);
    const auto off_rows = eval_sorted(wq, "TINYLAMB_DISABLE_WINDOW_HALO", true);
    EXPECT_EQ(on_rows, off_rows) << wq;
  }
  ::tinylamb::platform::UnsetEnv("TINYLAMB_DISABLE_WINDOW_HALO");

  // DistinctOnTop1 parity (ties, DESC + NULLS FIRST/LAST, ordinals, expression
  // keys).
  for (const std::string& dq :
       {"SELECT DISTINCT ON (grp) grp, id, val FROM w ORDER BY grp, val DESC "
        "NULLS LAST, id ASC;",
        "SELECT DISTINCT ON (grp) grp, id, val FROM w ORDER BY grp DESC, val "
        "ASC NULLS FIRST, id DESC;",
        "SELECT DISTINCT ON (MOD(grp, 3)) grp, id, COALESCE(val, -1) FROM w "
        "ORDER BY 1, id DESC;"}) {
    const auto on_rows =
        eval_sorted(dq, "TINYLAMB_DISABLE_DISTINCT_ON_TOP1", false);
    const auto off_rows =
        eval_sorted(dq, "TINYLAMB_DISABLE_DISTINCT_ON_TOP1", true);
    EXPECT_EQ(on_rows, off_rows) << dq;
  }
  ::tinylamb::platform::UnsetEnv("TINYLAMB_DISABLE_DISTINCT_ON_TOP1");

  // OuterOrBranches end-to-end through SqlEngine (PipelineLowering
  // OocCrossJoinProbeSource).
  for (const std::string& jq :
       {"SELECT lo.a, lo.b, ro.a, ro.b, ro.d FROM lo LEFT JOIN ro ON lo.a = "
        "ro.a OR lo.b = ro.b;",
        "SELECT lo.a, lo.b, ro.d FROM lo INNER JOIN ro ON (lo.a = ro.a AND "
        "lo.c > 0) OR (lo.b = ro.b AND ro.d < 5);"}) {
    auto on_rows = eval_sorted(jq, "TINYLAMB_DISABLE_OUTER_OR_BRANCHES", false);
    auto off_rows = eval_sorted(jq, "TINYLAMB_DISABLE_OUTER_OR_BRANCHES", true);
    auto cmp = [](const Row& x, const Row& y) {
      return x.ToString() < y.ToString();
    };
    std::ranges::sort(on_rows, cmp);
    std::ranges::sort(off_rows, cmp);
    EXPECT_EQ(on_rows, off_rows) << jq;
  }
  ::tinylamb::platform::UnsetEnv("TINYLAMB_DISABLE_OUTER_OR_BRANCHES");

  db->DeleteAll();
}

}  // namespace tinylamb
