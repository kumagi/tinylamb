/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/expr_oracle_fuzzer.hpp"

#include <cstdint>
#include <random>
#include <string>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "executor/executor_base.hpp"
#include "gtest/gtest.h"
#include "query/sql_engine.hpp"
#include "type/row.hpp"

namespace tinylamb {
namespace {

std::string RunSingleCellSql(const std::string& sql) {
  Database db("expr_oracle_pin-" + RandomString(8));
  TransactionContext ctx = db.BeginContext();
  SqlEngine engine(db);
  // Probe: does table presence change scalar-select row widths?
  {
    StatusOr<QueryResult> setup =
        engine.Execute(ctx, "CREATE TABLE probe_t (a INT64);");
    if (setup.HasValue()) {
      setup.Value().Drain();
    }
  }
  StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
  EXPECT_TRUE(prepared.HasValue()) << engine.LastError() << "\n" << sql;
  Row row;
  EXPECT_TRUE(prepared.Value()->Next(&row, nullptr));
  // Scalar SELECTs yield one row; only the first cell carries the projected
  // value (repo convention, cf. sql_oracle_fuzzer RunScalar and query_test
  // RunScalar, which both read [0][0]).
  EXPECT_GE(row.Size(), 1U);
  return row[0].IsNull() ? "NULL" : row[0].AsString();
}

// Oracle-found pins through the full SQL frontend + optimizer + executor:
// lazy COALESCE skips a throwing branch, and LIKE treats '%' as a wildcard
// even against literal '%' in the value.
TEST(ExprOracleFuzzer, EngineAgreesOnShortCircuitAndLike) {
  EXPECT_EQ(RunSingleCellSql("SELECT COALESCE(7, 1/0);"), "7");
  EXPECT_EQ(RunSingleCellSql("SELECT COALESCE(CAST(NULL AS INT64), 9);"), "9");
  EXPECT_EQ(RunSingleCellSql("SELECT NULLIF(0, 0) IS NULL;"), "1");
  EXPECT_EQ(RunSingleCellSql("SELECT '%%' LIKE '%';"), "1");
  EXPECT_EQ(RunSingleCellSql("SELECT ABS(-3);"), "3");
  EXPECT_EQ(RunSingleCellSql("SELECT -1 * 5;"), "-5");
  EXPECT_EQ(RunSingleCellSql("SELECT 1 + 2;"), "3");
  EXPECT_EQ(RunSingleCellSql("SELECT 1 + 2 AS x;"), "3");
  EXPECT_EQ(RunSingleCellSql("SELECT -9223372036854775807 - 1 AS lo;"),
            "-9223372036854775808");
}

}  // namespace

// Seeded end-to-end runs: rewrite equivalence plus engine-vs-AST-reference
// for every generated expression. A non-empty report is a logic bug in the
// rewrite rules, the optimizer, or the executor.
TEST(ExprOracleFuzzer, SeededIterationsHoldOracles) {
  int ran = 0;
  int engine_ran = 0;
  constexpr int kIterations = 64;
  for (uint32_t seed = 0; seed < kIterations; ++seed) {
    const auto seed32 = seed;
    const uint64_t packed = (static_cast<uint64_t>(seed32) << 32) | seed32;
    std::mt19937 rng(seed32);
    ExprOracleTrace trace;
    trace.seed = packed;
    std::string report = RunExprOracleIteration(rng, false, &trace);
    ASSERT_EQ(report, "") << "failing seed=" << seed << "\n" << report;
    EXPECT_EQ(trace.seed, packed);
    EXPECT_TRUE(trace.sql.rfind("SELECT ", 0) == 0)
        << "seed=" << seed << " sql=" << trace.sql;
    EXPECT_TRUE(trace.sexpr.front() == '(') << "seed=" << seed;
    EXPECT_TRUE(trace.reference.empty() == false) << "seed=" << seed;
    if (trace.engine_ran) {
      ++engine_ran;
    }
    ++ran;
  }
  EXPECT_EQ(ran, kIterations);
  // The engine oracle must actually execute (not skip every iteration),
  // or the sweep is vacuous.
  EXPECT_GT(engine_ran, kIterations / 2)
      << "engine oracle almost never ran; harness is broken";
}

// Failure->file->replay pipeline: serialize/parse round-trips, replay of a
// healthy trace holds, and a tampered trace is reported, not replayed clean.
TEST(ExprOracleFuzzer, TestFileRoundTripAndReplay) {
  const uint32_t seed32 = 7;
  std::mt19937 rng(seed32);  // NOLINT(cert-msc32-c,cert-msc51-cpp)
                             // deterministic seed for reproducibility
  ExprOracleTrace trace;
  trace.seed = (static_cast<uint64_t>(seed32) << 32) | seed32;
  ASSERT_EQ(RunExprOracleIteration(rng, false, &trace), "");

  const std::string text = SerializeExprOracleTest(trace);
  ExprOracleTrace parsed;
  ASSERT_TRUE(ParseExprOracleTest(text, &parsed));
  EXPECT_EQ(parsed, trace);
  EXPECT_EQ(ReplayExprOracleTrace(parsed, false), "")
      << "healthy trace replayed as a mismatch";

  // Tampered SQL must not replay clean.
  ExprOracleTrace tampered = parsed;
  tampered.sql = "SELECT 1 + 1;";
  EXPECT_NE(ReplayExprOracleTrace(tampered, false), "")
      << "tampered trace replayed clean; pipeline is broken";

  // Malformed input is refused, never half-replayed.
  ExprOracleTrace junk;
  EXPECT_FALSE(ParseExprOracleTest("SELECT 1;", &junk));
  EXPECT_FALSE(ParseExprOracleTest("-- tinylamb-expr-oracle-test v1\n", &junk));
}

}  // namespace tinylamb
