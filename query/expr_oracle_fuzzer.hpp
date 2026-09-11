/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXPR_ORACLE_FUZZER_HPP
#define TINYLAMB_EXPR_ORACLE_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>

#include "expression/expr_simplify_oracle.hpp"

namespace tinylamb {

// Expression-oracle fuzzing of scalar simplification and evaluation.
//
// Each iteration derives a deterministic seed, generates one complex scalar
// expression (expression/expr_simplify_oracle.hpp), and checks three
// identities:
//
//   (a) rewrite equivalence: the Expression tree evaluates identically
//       before and after ExpressionRuleSet::Default() + RewriteTypedArithmetic
//       (ground truth is Expression::Evaluate, the AST interpreter);
//   (b) engine equivalence: `SELECT <sql>` through SqlEngine returns the same
//       value the AST reference computed;
//   (c) Python cross-check: the emitted S-expression is the input format of
//       scripts/expr_oracle.py, an independent reimplementation of the same
//       GoogleSQL scalar semantics, so auditors can confirm the reference
//       outside C++.
//
// A violation of (a) or (b) is a logic bug. The iteration serializes the
// failing case to a self-contained `.test` file (seed + SQL + S-expression +
// reference/actual values) that replays via ParseExprOracleTest +
// ReplayExprOracleTrace: replay regenerates the expression from the seed, so
// no AST persistence format is needed.

struct ExprOracleTrace {
  uint64_t seed = 0;
  std::string sql;        // full `SELECT ...;` statement, engine-runnable
  std::string sexpr;      // S-expression for scripts/expr_oracle.py
  std::string reference;  // AST ground-truth value (FormatSimplifyValue)
  std::string actual;     // engine (or rewritten-tree) value, or THROW/E(...)s
  std::string failure;    // one-line summary of the mismatch
  // True when the engine executed the SQL (vs rejecting it as unsupported).
  // Guards against vacuous sweeps where every iteration skips.
  bool engine_ran = false;

  bool operator==(const ExprOracleTrace&) const = default;
};

// Runs one iteration from `rng` (which must have been seeded with the trace
// seed so replay can regenerate it). Returns "" when every oracle held (or
// the iteration was skipped because the engine rejected the SQL), otherwise
// a diagnostic. `trace` always receives the executed case.
std::string RunExprOracleIteration(std::mt19937& rng, bool verbose,
                                   ExprOracleTrace* trace = nullptr,
                                   const ExprGenConfig& config = {});

// Regenerates the expression from `trace.seed` and re-runs both oracles.
// Returns "" when the recorded failure no longer reproduces (fixed) or when
// the regenerated case agrees with the recorded reference/actual pair;
// otherwise returns the fresh mismatch diagnostic. A trace whose SQL does
// not match its seed is reported as tampered.
std::string ReplayExprOracleTrace(const ExprOracleTrace& trace,
                                  bool verbose = false);

// Self-contained text format ("-- key: value" lines).
std::string SerializeExprOracleTest(const ExprOracleTrace& trace);
// Returns false on malformed input.
bool ParseExprOracleTest(std::string_view text, ExprOracleTrace* trace);

// libFuzzer harness: interprets `data` as a seed for the generator. On a
// mismatch, writes `expr_oracle_fuzz-repro-<seed>.test` into the working
// directory (after verifying the serialized form replays the mismatch) and
// aborts. Compatible with sudden-death fuzzing: no state escapes the call.
void ExprOracleFuzzTry(const uint8_t* data, size_t size, bool verbose);

// ---------------------------------------------------------------------------
// Row-Aware NULL-Rejection & Differential Execution Fuzzer
// ---------------------------------------------------------------------------

struct RowExprOracleTrace {
  uint64_t seed = 0;
  std::string predicate_sql;
  std::string predicate_ast;
  size_t total_rows = 0;
  size_t matched_rows = 0;
  std::vector<int64_t> matched_ids;
  bool null_reject_claimed = false;
  std::string null_reject_col;
  bool null_reject_verified = false;
  bool engine_ran = false;
  std::string failure;

  bool operator==(const RowExprOracleTrace&) const = default;
};

// Generates a table `t_fuzz(id INT64, i INT64, f FLOAT64, b BOOL, s VARCHAR(32))`
// populated with edge-case rows (all-NULL, alternating NULLs, extremes, 0, +/-1,
// NaN, Inf, empty/wildcard strings), synthesizes a column-aware predicate,
// and checks three oracles:
//
//   (1) Null-Rejection Soundness Oracle:
//       If cascades::ExpressionRejectsNullsOnColumn(expr, C) claims true, then on
//       EVERY test row where row[C] is NULL, expr->TryEvaluate(row).Truthy() MUST be false.
//   (2) Rewrite 3-Valued Logic Equivalence Oracle:
//       For every row, expr and its rewritten form (ExpressionRuleSet::Default() +
//       RewriteTypedArithmetic) must agree on truthiness (filter context: TRUE vs FALSE/NULL).
//   (3) Differential Execution Engine Oracle:
//       On a real database table populated with the test rows:
//       - Ground-truth AST reference filter
//       - Rewritten tree in-memory filter
//       - Engine SQL execution: `SELECT id FROM t_fuzz WHERE <predicate_sql> ORDER BY id;`
//       Must all return the identical set of row IDs.
std::string RunRowExprOracleIteration(std::mt19937& rng, bool verbose,
                                      RowExprOracleTrace* trace = nullptr,
                                      const ExprGenConfig& config = {});

std::string ReplayRowExprOracleTrace(const RowExprOracleTrace& trace,
                                     bool verbose = false);

}  // namespace tinylamb

#endif  // TINYLAMB_EXPR_ORACLE_FUZZER_HPP
