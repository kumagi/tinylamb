/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXPR_SIMPLIFY_ORACLE_HPP
#define TINYLAMB_EXPR_SIMPLIFY_ORACLE_HPP

#include <cstdint>
#include <random>
#include <string>

#include "expression/expression.hpp"

namespace tinylamb {

// Seeded random scalar-expression generator + rewrite-equivalence oracle.
//
// Pipeline: GenerateSimplifyExpr(rng) builds a typed Expression tree together
// with its GoogleSQL text and a Lisp-style S-expression. The S-expression is
// the input format of scripts/expr_oracle.py, an independent Python
// reimplementation of the same GoogleSQL scalar semantics (INT64 overflow is
// an error, `/` on integers is a floating division, MOD takes the dividend's
// sign, NULL follows three-valued logic, NaN propagates). Any disagreement
// between the engine and the Python oracle -- or between the original tree
// and its rewritten form -- is a logic bug and can be serialized to a
// self-contained `.test` file by the query-layer driver
// (query/expr_oracle_fuzzer.hpp).
//
// This header lives in the expression layer, so it only depends on
// common/type/expression. SQL-engine execution belongs to the query layer.

struct ExprGenConfig {
  int max_depth = 4;
  // Probability (0-100) that a leaf is a typed NULL.
  int null_percent = 15;
};

struct GeneratedExpr {
  Expression expr;
  // `SELECT <sql>` reproduces the tree through the SQL frontend.
  std::string sql;
  // `(add (i 1) (i 2))` style input for scripts/expr_oracle.py.
  std::string sexpr;
};

// Deterministic in the RNG stream: the same seed always yields the same
// triple. Only uses the RNG (no map iteration), so libFuzzer bytes hashed
// into a seed reproduce byte-identical output.
GeneratedExpr GenerateSimplifyExpr(std::mt19937& rng,
                                   const ExprGenConfig& config = {});

// GoogleSQL text / S-expression serializers for an arbitrary tree built by
// the generator (constants, binary/unary ops, CASE, IN, ABS/GREATEST/LEAST/
// COALESCE/NULLIF, CAST).
std::string ToSimplifySql(const Expression& expr);
std::string ToSimplifySExpr(const Expression& expr);

// Evaluates `expr` before and after the default rewrite set
// (ExpressionRuleSet::Default() plus the typed arithmetic pass) on an empty
// row and returns "" when both sides agree (same value under
// SimplifyValuesEqual, or both throw). Otherwise returns a diagnostic.
std::string CheckSimplifyEquivalence(const Expression& expr);

// Value equality for oracle purposes: NULL == NULL, NaN == NaN (payload and
// sign-insensitive), +inf == +inf, -inf == -inf; otherwise Value::operator==.
// Doubles compare exactly: both sides run the same deterministic IEEE
// arithmetic, so no epsilon is needed (and an epsilon would hide real bugs).
[[nodiscard]] bool SimplifyValuesEqual(const Value& a, const Value& b);

// Renders a reference value for trace files: NULL/nan/inf/-inf/integers/
// shortest-round-trip doubles / quoted strings.
std::string FormatSimplifyValue(const Value& value);

// Bounded delta-shrinker: replaces subtrees with typed NULL/0/1/TRUE while
// the mismatch (as reported by CheckSimplifyEquivalence) is preserved.
// Always terminates; returns the smallest mismatch-preserving tree found
// (or the input when nothing shrinks).
Expression ShrinkSimplifyCounterexample(const Expression& expr);

}  // namespace tinylamb

#endif  // TINYLAMB_EXPR_SIMPLIFY_ORACLE_HPP
