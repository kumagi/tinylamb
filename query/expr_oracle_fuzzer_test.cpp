/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/expr_oracle_fuzzer.hpp"

#include <cstdint>
#include <random>
#include <stdexcept>
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

// Oracle-found regression pin: uniform_case_result used to collapse
// `CASE WHEN CAST(NaN AS INT64) = 0 THEN NULL ELSE NULL END` to NULL,
// erasing the throw the AST reference raises.  The recorded trace (from
// expr_oracle_fuzzer_libfuzzer, seed 0x831a3df2) must now replay clean.
TEST(ExprOracleFuzzer, ReplayPinnedUniformCaseThrowRegression) {
  static const char* kTrace =
      "-- tinylamb-expr-oracle-test v1\n"
      "-- seed: 9446931280153951730\n"
      "-- sql: SELECT (CAST(((CASE WHEN ((CASE WHEN 0 THEN "
      "(-9223372036854775807 - 1) WHEN 0 THEN -469341141271522 ELSE "
      "(-9223372036854775807 - 1) END) IN ((CAST((CAST('NaN' AS FLOAT64) * "
      "-1.0) AS INT64)))) THEN (CAST((-(CAST((--5) AS FLOAT64))) AS INT64)) "
      "END) + (CAST(((CASE WHEN 1 THEN (-9223372036854775807 - 1) ELSE "
      "ABS(3954908965629019) END) + (CAST(LEAST((CAST('NaN' AS FLOAT64) / "
      "CAST(NULL AS FLOAT64)), (-1.9 * 0.0)) AS INT64))) AS FLOAT64))) AS "
      "INT64));\n"
      "-- sexpr: (cast-int (add (case ((in (case ((b false) "
      "(i -9223372036854775808)) ((b false) (i -469341141271522)) "
      "(i -9223372036854775808)) (cast-int (mul (f nan) (f -1)))) "
      "(cast-int (neg (cast-float (neg (i -5)))))) (n int)) (cast-float "
      "(add (case ((b true) (i -9223372036854775808)) (abs "
      "(i 3954908965629019))) (cast-int (least (div (f nan) (n int)) "
      "(mul (f -1.9) (f 0))))))))\n"
      "-- reference: THROW(cannot cast NaN/Inf float to int)\n"
      "-- actual: REWRITE-MISMATCH shrunk=CAST((CASE WHEN CASE WHEN 0 THEN "
      "-9223372036854775808 WHEN 0 THEN -469341141271522 ELSE "
      "-9223372036854775808 END IN (CAST((nan * -1) AS INT64)) THEN "
      "CAST((-CAST((--5) AS FLOAT64)) AS INT64) END + CAST((CASE WHEN 1 THEN "
      "-9223372036854775808 ELSE abs(3954908965629019) END + CAST(least((nan "
      "/ NULL), (-1.9 * 0)) AS INT64)) AS FLOAT64)) AS INT64) | engine "
      "skipped\n"
      "-- engine_ran: false\n"
      "-- failure: rewrite equivalence failed\n";
  ExprOracleTrace trace;
  ASSERT_TRUE(ParseExprOracleTest(kTrace, &trace));
  EXPECT_EQ(ReplayExprOracleTrace(trace, false), "");
}

// Oracle-found regression pins through the full SQL frontend + optimizer +
// executor:
//   - lazy COALESCE skips a throwing branch, and LIKE treats '%' as a
//     wildcard even against literal '%' in the value.
//   - the executor's MOD fast path must raise on INT64_MIN % -1 exactly like
//     the AST reference (Value::operator%), not return the mathematical 0.
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
  EXPECT_THROW(RunSingleCellSql("SELECT MOD(-9223372036854775807 - 1, -1);"),
               std::runtime_error);
  EXPECT_THROW(RunSingleCellSql("SELECT MOD(1, 0.0);"), std::runtime_error);
}

// Oracle-found regression pin (seed 0x1c7f3d8c): the executor's MOD fast
// path folded INT64_MIN % -1 to 0, so a CASE predicated on it silently
// returned NULL instead of raising the AST's overflow error.
TEST(ExprOracleFuzzer, ReplayPinnedModuloOverflowRegression) {
  static const char* kTrace =
      "-- tinylamb-expr-oracle-test v1\n"
      "-- seed: 2051287015386227372\n"
      "-- sql: SELECT (CASE WHEN (ABS(MOD((-9223372036854775807 - 1), -1)) "
      "= (CASE WHEN 1 THEN (-1) ELSE -3 END)) THEN ((CAST(ABS((CASE WHEN 0 "
      "THEN 2635108465532378 WHEN 0 THEN 9223372036854775807 ELSE "
      "-3081505891392267 END)) AS FLOAT64)) - CAST(NULL AS FLOAT64)) END);\n"
      "-- sexpr: (case ((eq (abs (mod (i -9223372036854775808) (i -1))) "
      "(case ((b true) (neg (i 1))) (i -3))) (sub (cast-float (abs (case ((b "
      "false) (i 2635108465532378)) ((b false) (i 9223372036854775807)) (i "
      "-3081505891392267)))) (n int))) (n int))\n"
      "-- reference: THROW(integer overflow on '%')\n"
      "-- actual: NULL\n"
      "-- engine_ran: true\n"
      "-- failure: engine mismatch vs AST reference\n";
  ExprOracleTrace trace;
  ASSERT_TRUE(ParseExprOracleTest(kTrace, &trace));
  EXPECT_EQ(ReplayExprOracleTrace(trace, false), "");
}

// Oracle-found regression pin (seed 0x16b1e7e6): boolean_identity folded
// `x OR TRUE` to TRUE even when the AST evaluates x first (left side) and x
// raises (`CAST(NaN/Inf AS INT64)`), erasing the throw.
TEST(ExprOracleFuzzer, ReplayPinnedOrTrueThrowRegression) {
  static const char* kTrace =
      "-- tinylamb-expr-oracle-test v1\n"
      "-- seed: 1635862084609884134\n"
      "-- sql: SELECT (CASE WHEN (((CAST(NULLIF(-CAST('Infinity' AS "
      "FLOAT64), -1.9) AS INT64)) != -CAST('Infinity' AS FLOAT64)) OR ((-4.7 "
      "* 2.5) < 3708171684520782)) THEN -1.0 END);\n"
      "-- sexpr: (case ((or (ne (cast-int (nullif (f -inf) (f -1.9))) (f "
      "-inf)) (lt (mul (f -4.7) (f 2.5)) (i 3708171684520782))) (f -1)) (n "
      "int))\n"
      "-- reference: THROW(cannot cast NaN/Inf float to int)\n"
      "-- actual: REWRITE-MISMATCH shrunk=CASE WHEN ((CAST(nullif(-inf, -1.9)"
      " AS INT64) != -inf) OR ((-4.7 * 2.5) < 3708171684520782)) THEN -1 END "
      "| engine=THROW(cannot cast NaN/Inf float to int)\n"
      "-- engine_ran: true\n"
      "-- failure: rewrite equivalence failed\n";
  ExprOracleTrace trace;
  ASSERT_TRUE(ParseExprOracleTest(kTrace, &trace));
  EXPECT_EQ(ReplayExprOracleTrace(trace, false), "");
}

// Oracle-found regression pin (seed 0x16b1e8b4): `x IN (NULL)` folded to
// NULL even when evaluating x raises (`INT64_MIN / 0`), dropping the throw.
TEST(ExprOracleFuzzer, ReplayPinnedInNullDivisionThrowRegression) {
  static const char* kTrace =
      "-- tinylamb-expr-oracle-test v1\n"
      "-- seed: 1638306152930111172\n"
      "-- sql: SELECT (NOT (((-9223372036854775807 - 1) / 0.0) IN (CAST("
      "NULL AS FLOAT64))));\n"
      "-- sexpr: (not (in (div (i -9223372036854775808) (f 0)) (n float)))\n"
      "-- reference: THROW(division by zero)\n"
      "-- actual: REWRITE-MISMATCH shrunk=(NOT (-9223372036854775808 / 0) IN "
      "(NULL)) | engine=THROW(division by zero)\n"
      "-- engine_ran: true\n"
      "-- failure: rewrite equivalence failed\n";
  ExprOracleTrace trace;
  ASSERT_TRUE(ParseExprOracleTest(kTrace, &trace));
  EXPECT_EQ(ReplayExprOracleTrace(trace, false), "");
}

// Oracle-found regression pin (seed 0x83e4e3d5): contradiction_from_null_eq
// folded `CAST(Inf AS INT64) >= ... = 0` chains that end in `x = NULL` to the
// UNKNOWN constant, dropping the CAST(Inf) that the AST evaluates (and
// raises) first.
TEST(ExprOracleFuzzer, ReplayPinnedNullEqCastThrowRegression) {
  static const char* kTrace =
      "-- tinylamb-expr-oracle-test v1\n"
      "-- seed: 9506000356231616757\n"
      "-- sql: SELECT ((NOT CAST(NULL AS BOOL)) IN (NULLIF(((CAST(NULLIF("
      "CAST('Infinity' AS FLOAT64), 0.0) AS INT64)) >= (0.9 + CAST(NULL AS "
      "FLOAT64))), 0)));\n"
      "-- sexpr: (in (not (n bool)) (nullif (ge (cast-int (nullif (f inf) (f "
      "0))) (add (f 0.9) (n int))) (b false)))\n"
      "-- reference: THROW(cannot cast NaN/Inf float to int)\n"
      "-- actual: REWRITE-MISMATCH shrunk=(NOT NULL) IN (nullif((CAST(nullif("
      "inf, 0) AS INT64) >= (0.9 + NULL)), 0)) | engine=THROW(cannot cast "
      "NaN/Inf float to int)\n"
      "-- engine_ran: true\n"
      "-- failure: rewrite equivalence failed\n";
  ExprOracleTrace trace;
  ASSERT_TRUE(ParseExprOracleTest(kTrace, &trace));
  EXPECT_EQ(ReplayExprOracleTrace(trace, false), "");
}

// Oracle-found regression pin (seed 0x835596c1): the executor's MOD raised on
// a zero divisor only for int/int; the double path silently returned fmod's
// NaN, diverging from the AST reference (THROW division by zero).
TEST(ExprOracleFuzzer, ReplayPinnedModuloDoubleZeroRegression) {
  static const char* kTrace =
      "-- tinylamb-expr-oracle-test v1\n"
      "-- seed: 9476934689035179491\n"
      "-- sql: SELECT (CAST(MOD((CAST(COALESCE(1.0, (0 / 1896039082833363)) "
      "AS INT64)), (CASE WHEN CAST(NULL AS BOOL) THEN ABS((CAST(((-922337203"
      "6854775807 - 1) * (-9223372036854775807 - 1)) AS FLOAT64))) WHEN 0 "
      "THEN NULLIF((-CAST('NaN' AS FLOAT64)), (CAST(MOD(-2962346319136238, "
      "(-9223372036854775807 - 1)) AS FLOAT64))) ELSE 0.0 END)) AS FLOAT64));\n"
      "-- sexpr: (cast-float (mod (cast-int (coalesce (f 1) (div (i 0) (i "
      "1896039082833363)))) (case ((n bool) (abs (cast-float (mul (i -92233"
      "72036854775808) (i -9223372036854775808))))) ((b false) (nullif (neg "
      "(f nan)) (cast-float (mod (i -2962346319136238) (i -9223372036854775808)"
      ")))) (f 0))))\n"
      "-- reference: THROW(division by zero)\n"
      "-- actual: nan\n"
      "-- engine_ran: true\n"
      "-- failure: engine mismatch vs AST reference\n";
  ExprOracleTrace trace;
  ASSERT_TRUE(ParseExprOracleTest(kTrace, &trace));
  EXPECT_EQ(ReplayExprOracleTrace(trace, false), "");
}

// Oracle-found regression pin (seed 0x16b15447): the executor's MOD computed
// fmod for two FLOAT64 arguments, but GoogleSQL MOD is integer-only and the
// AST reference raises "unsupported binary operation".
TEST(ExprOracleFuzzer, ReplayPinnedModuloDoubleDoubleRegression) {
  static const char* kTrace =
      "-- tinylamb-expr-oracle-test v1\n"
      "-- seed: 1635809587224612903\n"
      "-- sql: SELECT (CAST(((CAST((CAST(MOD((CASE WHEN CAST(NULL AS BOOL) "
      "THEN 2.5 ELSE -1.5 END), (CASE WHEN 0 THEN 0.5 WHEN 1 THEN -1.5 END)) "
      "AS FLOAT64)) AS INT64)) + (-(CAST(((CAST(COALESCE(-4.3, -4.8) AS "
      "INT64)) * (1.0 - -1305957821310940)) AS INT64)))) AS FLOAT64));\n"
      "-- sexpr: (cast-float (add (cast-int (cast-float (mod (case ((n bool) "
      "(f 2.5)) (f -1.5)) (case ((b false) (f 0.5)) ((b true) (f -1.5)) (n "
      "int))))) (neg (cast-int (mul (cast-int (coalesce (f -4.3) (f -4.8))) "
      "(sub (f 1) (i -1305957821310940)))))))\n"
      "-- reference: THROW(unsupported binary operation)\n"
      "-- actual: 5223831285243764\n"
      "-- engine_ran: true\n"
      "-- failure: engine mismatch vs AST reference\n";
  ExprOracleTrace trace;
  ASSERT_TRUE(ParseExprOracleTest(kTrace, &trace));
  EXPECT_EQ(ReplayExprOracleTrace(trace, false), "");
}

// Oracle-found regression pin (seed 0x5888d3c0): reassociate_add/subtract
// folded `(x + a) + b -> x + (a + b)` even with opposite-sign constants,
// eliding the intermediate `(-1 + INT64_MIN)` overflow the AST raises.
TEST(ExprOracleFuzzer, ReplayPinnedReassociateAddOverflowRegression) {
  static const char* kTrace =
      "-- tinylamb-expr-oracle-test v1\n"
      "-- seed: 6383078004781039520\n"
      "-- sql: SELECT (CASE WHEN 0 THEN (CAST(MOD((-9223372036854775807 - "
      "1), (CAST(ABS(1.0) AS INT64))) AS FLOAT64)) ELSE (CAST(LEAST((CAST("
      "((CAST(((-9223372036854775807 - 1) / -CAST('Infinity' AS FLOAT64)) AS "
      "INT64)) * (CASE WHEN 0 THEN -1.5 WHEN 1 THEN CAST(NULL AS FLOAT64) "
      "ELSE CAST('NaN' AS FLOAT64) END)) AS INT64)), ((-1 + "
      "(-9223372036854775807 - 1)) - (CAST((-3160301361151923 + -3.1) AS "
      "INT64)))) AS FLOAT64)) END);\n"
      "-- sexpr: (case ((b false) (cast-float (mod (i -9223372036854775808) "
      "(cast-int (abs (f 1)))))) (cast-float (least (cast-int (mul (cast-int "
      "(div (i -9223372036854775808) (f -inf))) (case ((b false) (f -1.5)) "
      "((b true) (n float)) (f nan)))) (sub (add (i -1) (i "
      "-9223372036854775808)) (cast-int (add (i -3160301361151923) (f "
      "-3.1)))))))\n"
      "-- reference: THROW(integer overflow on '+')\n"
      "-- actual: REWRITE-MISMATCH shrunk=CASE WHEN 0 THEN CAST((-92233720368"
      "54775808 % CAST(abs(1) AS INT64)) AS FLOAT64) ELSE CAST(least(CAST("
      "(CAST((-9223372036854775808 / -inf) AS INT64) * CASE WHEN 0 THEN -1.5 "
      "WHEN 1 THEN NULL ELSE nan END) AS INT64), ((-1 + -9223372036854775808) "
      "- CAST((-3160301361151923 + -3.1) AS INT64))) AS FLOAT64) END | "
      "engine=THROW(integer overflow on '+')\n"
      "-- engine_ran: true\n"
      "-- failure: rewrite equivalence failed\n";
  ExprOracleTrace trace;
  ASSERT_TRUE(ParseExprOracleTest(kTrace, &trace));
  EXPECT_EQ(ReplayExprOracleTrace(trace, false), "");
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
