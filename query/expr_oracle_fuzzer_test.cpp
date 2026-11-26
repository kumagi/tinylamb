/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/expr_oracle_fuzzer.hpp"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/platform/process.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "executor/executor_base.hpp"
#include "gtest/gtest.h"
#include "query/fuzz_scoped_db.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"

namespace tinylamb {
namespace {

std::string RunSingleCellSql(const std::string& sql) {
  ScopedDb sdb("expr_oracle_pin");
  if (!sdb) {
    ADD_FAILURE() << "failed to create scratch database";
    return "";
  }
  Database& db = *sdb;
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
  // These checks must return early: Value() on a failed StatusOr aborts the
  // whole test binary, and row[0] on an empty Row is out of bounds. A bare
  // EXPECT_* would turn a diagnosable regression into an abort that skips the
  // rest of the suite. (ASSERT_* cannot be used in this std::string helper.)
  if (!prepared.HasValue()) {
    ADD_FAILURE() << engine.LastError() << "\n" << sql;
    return "";
  }
  Row row;
  const bool got_row = prepared.Value()->Next(&row, nullptr);
  const Status st = prepared.Value()->GetStatus();
  if (st != Status::kSuccess) {
    throw std::runtime_error(st.GetMessage());
  }
  if (!got_row) {
    ADD_FAILURE() << "no row returned: " << sql;
    return "";
  }
  // Scalar SELECTs yield one row; only the first cell carries the projected
  // value (repo convention, cf. sql_oracle_fuzzer RunScalar and query_test
  // RunScalar, which both read [0][0]).
  if (row.Count() < 1) {
    ADD_FAILURE() << "empty row returned: " << sql;
    return "";
  }
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
      "-- sql: SELECT (CAST(((CASE WHEN ((CASE WHEN FALSE THEN "
      "(-9223372036854775807 - 1) WHEN FALSE THEN -469341141271522 ELSE "
      "(-9223372036854775807 - 1) END) IN ((CAST((CAST('NaN' AS FLOAT64) * "
      "-1.0) AS INT64)))) THEN (CAST((-(CAST((--5) AS FLOAT64))) AS INT64)) "
      "END) + (CAST(((CASE WHEN TRUE THEN (-9223372036854775807 - 1) ELSE "
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
      "= (CASE WHEN TRUE THEN (-1) ELSE -3 END)) THEN ((CAST(ABS((CASE WHEN "
      "FALSE "
      "THEN 2635108465532378 WHEN FALSE THEN 9223372036854775807 ELSE "
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

// Oracle-found regression pin: a COALESCE/NULLIF tower whose eager NULLIF
// arguments raise `cannot cast NaN/Inf float to int` must keep raising after
// the rewriter folds CASE/IN constants; folding may not drop the throw.
TEST(ExprOracleFuzzer, ReplayPinnedNullifCoalesceThrowRegression) {
  static const char* kTrace =
      "-- tinylamb-expr-oracle-test v1\n"
      "-- seed: 1638323354274135657\n"
      "-- sql: SELECT ((CAST(((CAST(((0 * 9223372036854775807) + "
      "1803856200320681) AS FLOAT64)) * (-2959661964383290 - (CASE WHEN FALSE "
      "THEN CAST(NULL AS INT64) WHEN FALSE THEN CAST(NULL AS INT64) END))) AS "
      "INT64)) > (CAST(COALESCE(NULLIF((CAST((-1939353828762478 + "
      "-CAST('Infinity' AS FLOAT64)) AS INT64)), (CAST((-CAST('Infinity' AS "
      "FLOAT64) - -3425790469862174) AS INT64))), (CASE WHEN CAST(NULL AS "
      "BOOL) THEN (CAST(-CAST('Infinity' AS FLOAT64) AS INT64)) END), (CASE "
      "WHEN (CAST(NULL AS INT64) IN (905452120508842, 427095843553405, "
      "3023283314291129)) THEN MOD(3847358689358039, 1) END)) AS FLOAT64)));\n"
      "-- sexpr: (gt (cast-int (mul (cast-float (add (mul (i 0) (i "
      "9223372036854775807)) (i 1803856200320681))) (sub (i -2959661964383290)"
      " (case ((b false) (n int)) ((b false) (n int)) (n int))))) (cast-float "
      "(coalesce (nullif (cast-int (add (i -1939353828762478) (f -inf))) "
      "(cast-int (sub (f -inf) (i -3425790469862174)))) (case ((n bool) "
      "(cast-int (f -inf))) (n int)) (case ((in (n int) (i 905452120508842) "
      "(i 427095843553405) (i 3023283314291129)) (mod (i 3847358689358039) "
      "(i 1))) (n int)))))\n"
      "-- reference: THROW(cannot cast NaN/Inf float to int)\n"
      "-- actual: REWRITE-MISMATCH shrunk=(CAST((CAST(((0 * "
      "9223372036854775807) + 1803856200320681) AS FLOAT64) * "
      "(-2959661964383290 - CASE WHEN 0 THEN NULL WHEN 0 THEN NULL END)) AS "
      "INT64) > CAST(coalesce(nullif(CAST((-1939353828762478 + -inf) AS "
      "INT64), CAST((-inf - -3425790469862174) AS INT64)), CASE WHEN NULL "
      "THEN CAST(-inf AS INT64) END, CASE WHEN NULL IN (905452120508842, "
      "427095843553405, 3023283314291129) THEN (3847358689358039 % 1) END) AS "
      "FLOAT64)) | engine=THROW(cannot cast NaN/Inf float to int)\n"
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
      "FLOAT64))), FALSE)));\n"
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
      "6854775807 - 1) * (-9223372036854775807 - 1)) AS FLOAT64))) WHEN FALSE "
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
TEST(ExprOracleFuzzer, ReplayPinnedModuloTwoFloat64ArgsRegression) {
  static const char* kTrace =
      "-- tinylamb-expr-oracle-test v1\n"
      "-- seed: 1635809587224612903\n"
      "-- sql: SELECT (CAST(((CAST((CAST(MOD((CASE WHEN CAST(NULL AS BOOL) "
      "THEN 2.5 ELSE -1.5 END), (CASE WHEN FALSE THEN 0.5 WHEN TRUE THEN -1.5 "
      "END)) "
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
      "-- sql: SELECT (CASE WHEN FALSE THEN (CAST(MOD((-9223372036854775807 - "
      "1), (CAST(ABS(1.0) AS INT64))) AS FLOAT64)) ELSE (CAST(LEAST((CAST("
      "((CAST(((-9223372036854775807 - 1) / -CAST('Infinity' AS FLOAT64)) AS "
      "INT64)) * (CASE WHEN FALSE THEN -1.5 WHEN TRUE THEN CAST(NULL AS "
      "FLOAT64) "
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

// Oracle-found regression pin (seed 0x16b1e7e6-2 / 1638323354274135657):
// reassociation dropped the `CASE WHEN 0 THEN CAST(NULL AS INT64) ...` dead
// branch whose NULL rendering used to change float->int cast raising order
// relative to the AST reference. The recorded trace (from
// expr_oracle_fuzzer_libfuzzer) must replay clean.
TEST(ExprOracleFuzzer, ReplayPinnedDeadBranchCastOrderRegression) {
  static const char* kTrace =
      "-- tinylamb-expr-oracle-test v1\n"
      "-- seed: 1638323354274135657\n"
      "-- sql: SELECT ((CAST(((CAST(((0 * 9223372036854775807) + "
      "1803856200320681) AS FLOAT64)) * (-2959661964383290 - (CASE WHEN FALSE "
      "THEN CAST(NULL AS INT64) WHEN FALSE THEN CAST(NULL AS INT64) END))) AS "
      "INT64)) > (CAST(COALESCE(NULLIF((CAST((-1939353828762478 + -CAST("
      "'Infinity' AS FLOAT64)) AS INT64)), (CAST((-CAST('Infinity' AS "
      "FLOAT64) - -3425790469862174) AS INT64))), (CASE WHEN CAST(NULL AS "
      "BOOL) THEN (CAST(-CAST('Infinity' AS FLOAT64) AS INT64)) END), (CASE "
      "WHEN (CAST(NULL AS INT64) IN (905452120508842, 427095843553405, "
      "3023283314291129)) THEN MOD(3847358689358039, 1) END)) AS FLOAT64)));\n"
      "-- sexpr: (gt (cast-int (mul (cast-float (add (mul (i 0) (i "
      "9223372036854775807)) (i 1803856200320681))) (sub (i -2959661964383290) "
      "(case ((b false) (n int)) ((b false) (n int)) (n int))))) (cast-float "
      "(coalesce (nullif (cast-int (add (i -1939353828762478) (f -inf))) "
      "(cast-int (sub (f -inf) (i -3425790469862174)))) (case ((n bool) "
      "(cast-int (f -inf))) (n int)) (case ((in (n int) (i 905452120508842) (i "
      "427095843553405) (i 3023283314291129)) (mod (i 3847358689358039) (i "
      "1))) (n int)))))\n"
      "-- reference: THROW(cannot cast NaN/Inf float to int)\n"
      "-- actual: REWRITE-MISMATCH shrunk=(CAST((CAST(((0 * "
      "9223372036854775807) + 1803856200320681) AS FLOAT64) * "
      "(-2959661964383290 - CASE WHEN 0 THEN NULL WHEN 0 THEN NULL END)) AS "
      "INT64) > CAST(coalesce(nullif(CAST((-1939353828762478 + -inf) AS "
      "INT64), CAST((-inf - -3425790469862174) AS INT64)), CASE WHEN NULL "
      "THEN CAST(-inf AS INT64) END, CASE WHEN NULL IN (905452120508842, "
      "427095843553405, 3023283314291129) THEN (3847358689358039 % 1) END) AS "
      "FLOAT64)) | engine=THROW(cannot cast NaN/Inf float to int)\n"
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
  const char* scale = ::tinylamb::platform::GetEnv("TINYLAMB_FUZZ_ITERS");
  int kIterations = 64;
  if (scale != nullptr) {
    char* end = nullptr;
    errno = 0;
    const long parsed = std::strtol(scale, &end, 10);
    if (errno == 0 && end != scale && *end == '\0' && parsed > 0 &&
        parsed <= std::numeric_limits<int>::max()) {
      kIterations = static_cast<int>(parsed);
    }
  }
  for (uint32_t seed = 0; std::cmp_less(seed, kIterations); ++seed) {
    const auto seed32 = seed;
    const uint64_t packed = (static_cast<uint64_t>(seed32) << 32) | seed32;
    std::mt19937 rng(seed32);
    ExprOracleTrace trace;
    trace.seed = packed;
    std::string report = RunExprOracleIteration(rng, false, &trace);
    ASSERT_EQ(report, "") << "failing seed=" << seed << "\n" << report;
    EXPECT_EQ(trace.seed, packed);
    EXPECT_TRUE(trace.sql.rfind("SELECT ", 0) == 0)
        << "failing seed=" << seed << "\n"
        << trace.sql;
    ASSERT_FALSE(trace.sexpr.empty());
    EXPECT_EQ(trace.sexpr.front(), '(');
    EXPECT_TRUE(trace.reference.empty() == false);
    if (trace.engine_ran) {
      ++engine_ran;
    }
    ++ran;
  }
  EXPECT_EQ(ran, kIterations);
  // The engine oracle must actually execute (not skip every iteration),
  // or the sweep is vacuous.
  EXPECT_GT(engine_ran, kIterations / 2);
}

class ExprOracleExtendedTest : public ::testing::TestWithParam<uint32_t> {};
constexpr uint32_t kExtendedOpsShards = 4;

TEST_P(ExprOracleExtendedTest, SeededExtendedOpsHoldOracles) {
  int ran = 0;
  int engine_ran = 0;
  const char* scale = ::tinylamb::platform::GetEnv("TINYLAMB_FUZZ_ITERS");
  int kIterations = 200;
  if (scale != nullptr) {
    char* end = nullptr;
    errno = 0;
    const long parsed = std::strtol(scale, &end, 10);
    if (errno == 0 && end != scale && *end == '\0' && parsed > 0 &&
        parsed <= std::numeric_limits<int>::max()) {
      kIterations = static_cast<int>(parsed);
    }
  }
  ExprGenConfig config;
  config.extended_ops = true;
  const uint32_t shard = GetParam();
  for (uint32_t seed = shard; std::cmp_less(seed, kIterations);
       seed += kExtendedOpsShards) {
    const auto seed32 = seed + 3000;
    const uint64_t packed = (static_cast<uint64_t>(seed32) << 32) | seed32;
    std::mt19937 rng(seed32);
    ExprOracleTrace trace;
    trace.seed = packed;
    std::string report = RunExprOracleIteration(rng, false, &trace, config);
    ASSERT_EQ(report, "") << "Seed " << seed32 << " failed:\n" << report;
    EXPECT_EQ(trace.seed, packed);
    EXPECT_TRUE(trace.sql.rfind("SELECT ", 0) == 0);
    ASSERT_FALSE(trace.sexpr.empty());
    EXPECT_EQ(trace.sexpr.front(), '(');
    EXPECT_TRUE(trace.reference.empty() == false);
    if (trace.engine_ran) {
      ++engine_ran;
    }
    ++ran;
  }
  const auto iterations = static_cast<uint32_t>(kIterations);
  EXPECT_EQ(ran,
            (iterations + kExtendedOpsShards - 1 - shard) / kExtendedOpsShards);
  if (ran > 0) {
    // With one seed per shard, ran / 2 == 0, so the ratio check alone was
    // vacuous: the engine arm could skip every seed and still pass.
    EXPECT_GT(engine_ran, 0)
        << "extended-op sweep must execute the engine oracle at least once";
    EXPECT_GT(engine_ran, ran / 2)
        << "extended-op sweep must execute the engine oracle";
  }
}

INSTANTIATE_TEST_SUITE_P(SeedShards, ExprOracleExtendedTest,
                         ::testing::Range(uint32_t{0}, kExtendedOpsShards));

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
  EXPECT_EQ(ReplayExprOracleTrace(parsed, false), "");

  // Tampered SQL must not replay clean.
  ExprOracleTrace tampered = parsed;
  tampered.sql = "SELECT 1 + 1;";
  EXPECT_NE(ReplayExprOracleTrace(tampered, false), "");

  // Malformed input is refused, never half-replayed.
  ExprOracleTrace junk;
  EXPECT_FALSE(ParseExprOracleTest("SELECT 1;", &junk));
  EXPECT_FALSE(ParseExprOracleTest("-- tinylamb-expr-oracle-test v1\n", &junk));
}

TEST(ExprOracleFuzzer, RowAwareNullRejectAndDifferentialEquivalence) {
  const char* scale = ::tinylamb::platform::GetEnv("TINYLAMB_FUZZ_ITERS");
  int kIterations = 64;
  if (scale != nullptr) {
    char* end = nullptr;
    errno = 0;
    const long parsed = std::strtol(scale, &end, 10);
    if (errno == 0 && end != scale && *end == '\0' && parsed > 0 &&
        parsed <= std::numeric_limits<int>::max()) {
      kIterations = static_cast<int>(parsed);
    }
  }
  int null_reject_verified_count = 0;
  int engine_ran_count = 0;

  for (uint32_t seed = 0; std::cmp_less(seed, kIterations); ++seed) {
    const auto seed32 = seed;
    const uint64_t packed = (static_cast<uint64_t>(seed32) << 32) | seed32;
    std::mt19937 rng(seed32);
    RowExprOracleTrace trace;
    trace.seed = packed;
    std::string report = RunRowExprOracleIteration(rng, false, &trace);
    ASSERT_EQ(report, "") << "Seed " << seed << " failed:\n" << report;
    EXPECT_EQ(trace.seed, packed);
    EXPECT_FALSE(trace.predicate_sql.empty());
    EXPECT_GT(trace.total_rows, 0U);
    if (trace.null_reject_verified) {
      ++null_reject_verified_count;
    }
    if (trace.engine_ran) {
      ++engine_ran_count;
    }
  }

  EXPECT_EQ(engine_ran_count, kIterations);
  EXPECT_GT(null_reject_verified_count, 0)
      << "Expected null rejection verification to trigger on generated "
         "predicates";
}

TEST(ExprOracleFuzzer, RowTraceReplayEquivalence) {
  const uint32_t seed32 = 42;
  std::mt19937 rng(seed32);  // NOLINT(cert-msc32-c,cert-msc51-cpp)
                             // deterministic seed for reproducibility
  RowExprOracleTrace trace;
  trace.seed = (static_cast<uint64_t>(seed32) << 32) | seed32;
  ASSERT_EQ(RunRowExprOracleIteration(rng, false, &trace), "");

  EXPECT_EQ(ReplayRowExprOracleTrace(trace, false), "");

  // Tampering with trace IDs should cause replay to detect discrepancy
  RowExprOracleTrace tampered = trace;
  tampered.matched_ids.push_back(999999);
  EXPECT_NE(ReplayRowExprOracleTrace(tampered, false), "");
}

TEST(ExprOracleFuzzer, RowAwareExtendedOpsEquivalence) {
  ExprGenConfig config;
  config.extended_ops = true;
  constexpr int kIterations = 100;
  int null_reject_verified_count = 0;
  int engine_ran_count = 0;

  for (uint32_t seed = 0; seed < static_cast<uint32_t>(kIterations); ++seed) {
    std::mt19937 rng(seed + 1000);
    RowExprOracleTrace trace;
    trace.seed = (static_cast<uint64_t>(seed + 1000) << 32) | (seed + 1000);
    std::string report = RunRowExprOracleIteration(rng, false, &trace, config);
    ASSERT_EQ(report, "") << "Seed " << seed << " failed:\n" << report;
    if (trace.null_reject_verified) {
      ++null_reject_verified_count;
    }
    if (trace.engine_ran) {
      ++engine_ran_count;
    }
  }

  EXPECT_EQ(engine_ran_count, kIterations);
  EXPECT_GT(null_reject_verified_count, 0)
      << "Expected null rejection verification to trigger on generated "
         "extended-op predicates";
}

TEST(ExprOracleFuzzer, ReplayPinnedFuzzFindingsRegressions) {
  // Row-aware ABS(INT64_MIN) error-suppression regressions:
  // - seed 1236065886992294752: CompileScanFilter disjunctive branch reordering
  // - seed 6144139855833357989: FactorAndEnvelopeOneSelect absorption
  for (uint64_t seed :
       {uint64_t{1236065886992294752ULL}, uint64_t{6144139855833357989ULL}}) {
    std::mt19937 rng(static_cast<uint32_t>(seed));
    ExprGenConfig config;
    config.extended_ops = true;
    RowExprOracleTrace fresh;
    fresh.seed = seed;
    fresh.extended_ops = true;
    EXPECT_EQ(RunRowExprOracleIteration(rng, false, &fresh, config), "")
        << "row_expr_oracle seed=" << seed;
  }

  // Scalar expression regressions:
  // - 11118779134808807094: __bit_xor unsignedness parity (AST vs
  // function_eval)
  // - 11580306021212848888: UINT64 + INT64 overflow in binary_expression
  // - 14649602716663142413: CASE-lift over binary comparison throw order
  // - 4199220810818691348:  StaticallyNonBoolean for CASE under x AND x -> x
  // - 10137269684562481425: InferType missing CHR() in COALESCE NULL hint
  // - 6016210131723607869:  CaseExpression::ResultType skipping kInvalid branch
  // - 8560459556290616476:  inner_join_not_null_inference in scalar rewrite
  for (const auto& [seed, extended] :
       std::vector<std::pair<uint64_t, bool>>{{11118779134808807094ULL, true},
                                              {11580306021212848888ULL, true},
                                              {14649602716663142413ULL, true},
                                              {4199220810818691348ULL, true},
                                              {10137269684562481425ULL, true},
                                              {6016210131723607869ULL, true},
                                              {8560459556290616476ULL, true}}) {
    std::mt19937 rng(static_cast<uint32_t>(seed));
    ExprGenConfig config;
    config.extended_ops = extended;
    ExprOracleTrace fresh;
    fresh.seed = seed;
    fresh.extended_ops = extended;
    EXPECT_EQ(RunExprOracleIteration(rng, false, &fresh, config), "")
        << "expr_oracle seed=" << seed;
  }
}

TEST(ExprOracleFuzzer, ReplayRegressionDir) {
  const char* dir =
      ::tinylamb::platform::GetEnv("TINYLAMB_EXPR_REGRESSION_DIR");
  if (dir == nullptr || *dir == '\0') {
    GTEST_SKIP() << "set TINYLAMB_EXPR_REGRESSION_DIR to replay .test files";
  }
  std::vector<std::filesystem::path> files;
  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    if (entry.is_regular_file() && entry.path().extension() == ".test") {
      files.push_back(entry.path());
    }
  }
  std::sort(files.begin(), files.end());
  for (const auto& path : files) {
    std::ifstream in(path);
    ASSERT_TRUE(in.good()) << path;
    std::string text((std::istreambuf_iterator<char>(in)),
                     std::istreambuf_iterator<char>());
    if (text.starts_with("-- tinylamb-expr-oracle-test v1")) {
      ExprOracleTrace trace;
      if (ParseExprOracleTest(text, &trace)) {
        EXPECT_EQ(ReplayExprOracleTrace(trace, false), "") << path;
      } else {
        // Traces whose SQL contains an unescaped newline fail line-based
        // parsing; replay directly from the recorded seed + extended_ops.
        uint64_t seed = 0;
        bool extended = false;
        std::istringstream iss(text);
        std::string line;
        while (std::getline(iss, line)) {
          if (line.starts_with("-- seed: ")) {
            seed = std::stoull(line.substr(9));
          } else if (line.starts_with("-- extended_ops: ")) {
            extended = (line.substr(17) == "true");
          }
        }
        std::mt19937 rng(static_cast<uint32_t>(seed));
        ExprGenConfig config;
        config.extended_ops = extended;
        ExprOracleTrace fresh;
        fresh.seed = seed;
        fresh.extended_ops = extended;
        EXPECT_EQ(RunExprOracleIteration(rng, false, &fresh, config), "")
            << path;
      }
    } else if (text.starts_with("-- tinylamb-row-expr-oracle-test v1")) {
      uint64_t seed = 0;
      bool extended = false;
      std::istringstream iss(text);
      std::string line;
      while (std::getline(iss, line)) {
        if (line.starts_with("-- seed: ")) {
          seed = std::stoull(line.substr(9));
        } else if (line.starts_with("-- extended_ops: ")) {
          extended = (line.substr(17) == "true");
        }
      }
      const auto seed32 = static_cast<uint32_t>(seed);
      std::mt19937 rng(seed32);
      ExprGenConfig config;
      config.extended_ops = extended;
      RowExprOracleTrace fresh;
      fresh.seed = seed;
      fresh.extended_ops = extended;
      EXPECT_EQ(RunRowExprOracleIteration(rng, false, &fresh, config), "")
          << path;
    }
  }
}

}  // namespace tinylamb
