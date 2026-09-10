/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/expr_oracle_fuzzer.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expr_simplify_oracle.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "plan/cascades.hpp"
#include "query/sql_engine.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

// Runs `sql` (a full SELECT statement) and returns the single scalar cell.
// nullopt means the engine rejected the query: skip the oracle, it is not a
// logic bug for the generator to emit SQL outside the supported fragment.
struct EngineOutcome {
  bool ran = false;
  bool threw = false;
  std::string error;
  Value value;
};

EngineOutcome RunScalar(Database& db, TransactionContext& ctx,
                        const std::string& sql) {
  EngineOutcome outcome;
  SqlEngine engine(db);
  StatusOr<QueryResult> result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    return outcome;  // rejected: ran stays false
  }
  try {
    std::vector<Row> rows;
    Row row;
    while (result.Value().Next(&row)) {
      rows.push_back(row);
    }
    if (rows.size() != 1 || rows[0].Size() < 1) {
      return outcome;  // unexpected shape: skip rather than misreport
    }
    outcome.ran = true;
    outcome.value = rows[0][0];
  } catch (const std::exception& error) {
    outcome.ran = true;
    outcome.threw = true;
    outcome.error = error.what();
  } catch (...) {
    outcome.ran = true;
    outcome.threw = true;
    outcome.error = "unknown exception";
  }
  return outcome;
}

struct ReferenceOutcome {
  bool threw = false;
  std::string error;
  Value value;
};

ReferenceOutcome RunReference(const Expression& expr) {
  ReferenceOutcome outcome;
  try {
    outcome.value = expr->Evaluate(Row(), Schema());
  } catch (const std::exception& error) {
    outcome.threw = true;
    outcome.error = error.what();
  } catch (...) {
    outcome.threw = true;
    outcome.error = "unknown exception";
  }
  return outcome;
}

std::string DescribeReference(const ReferenceOutcome& outcome) {
  if (outcome.threw) {
    return "THROW(" + outcome.error + ")";
  }
  return FormatSimplifyValue(outcome.value);
}

std::string DescribeEngine(const EngineOutcome& outcome) {
  if (outcome.threw) {
    return "THROW(" + outcome.error + ")";
  }
  return FormatSimplifyValue(outcome.value);
}

constexpr const char* kTestHeader = "-- tinylamb-expr-oracle-test v1";

}  // namespace

std::string RunExprOracleIteration(std::mt19937& rng, bool verbose,
                                   ExprOracleTrace* trace) {
  ExprOracleTrace local;
  ExprOracleTrace& t = (trace != nullptr) ? *trace : local;

  GeneratedExpr generated = GenerateSimplifyExpr(rng);
  if (!generated.expr) {
    return "";
  }
  t.sql = "SELECT " + generated.sql + ";";
  t.sexpr = generated.sexpr;

  // Oracle (a): rewrite equivalence on the tree itself.
  const std::string rewrite_report = CheckSimplifyEquivalence(generated.expr);
  const ReferenceOutcome reference = RunReference(generated.expr);
  t.reference = DescribeReference(reference);

  std::string report;
  if (!rewrite_report.empty()) {
    Expression shrunk = ShrinkSimplifyCounterexample(generated.expr);
    t.actual = "REWRITE-MISMATCH shrunk=" + shrunk->ToString();
    t.failure = "rewrite equivalence failed";
    report += "[EXPR-REWRITE MISMATCH]\n" + rewrite_report +
              "\nshrunk: " + shrunk->ToString() + "\n";
  }

  // Oracle (b): full engine execution of the same expression text.
  auto db_holder =
      Database::Create("expr_oracle_fuzz-" + RandomString(8)).MoveValue();
  CHECK(db_holder != nullptr);
  Database& db = *db_holder;
  TransactionContext ctx = db.BeginContext();
  const EngineOutcome engine = RunScalar(db, ctx, t.sql);
  t.engine_ran = engine.ran;
  if (!engine.ran) {
    if (verbose) {
      std::cerr << "[expr_oracle_fuzz][skip] engine rejected: " << t.sql
                << "\n";
    }
    if (report.empty()) {
      t.actual.clear();
      return "";
    }
    t.actual += " | engine skipped";
    return report;
  }
  t.actual =
      (t.actual.empty() ? DescribeEngine(engine)
                        : t.actual + " | engine=" + DescribeEngine(engine));

  const bool engine_agrees =
      (reference.threw && engine.threw) ||
      (!reference.threw && !engine.threw &&
       SimplifyValuesEqual(reference.value, engine.value));
  if (!engine_agrees) {
    t.failure = t.failure.empty() ? "engine mismatch vs AST reference"
                                  : t.failure + " + engine mismatch";
    report += "[EXPR-ENGINE MISMATCH]\n  sql: " + t.sql +
              "\n  sexpr: " + t.sexpr +
              "\n  reference(AST): " + DescribeReference(reference) +
              "\n  actual(engine): " + DescribeEngine(engine) + "\n";
  }
  if (verbose && report.empty()) {
    std::cerr << "[expr_oracle_fuzz][ok] " << t.sql << " => " << t.reference
              << "\n";
  }
  return report;
}

std::string ReplayExprOracleTrace(const ExprOracleTrace& trace, bool verbose) {
  // Regenerate from the seed: the generator is deterministic, so the
  // recorded SQL/S-expression must match byte-identically. The canonical
  // seed packs the uint32 generator seed into both halves.
  const auto seed32 = static_cast<uint32_t>(trace.seed);
  std::mt19937 rng(seed32);
  GeneratedExpr regenerated = GenerateSimplifyExpr(rng);
  const std::string expected_sql = "SELECT " + regenerated.sql + ";";
  if (regenerated.sexpr != trace.sexpr || expected_sql != trace.sql) {
    return "tampered trace: sql/sexpr do not match seed " +
           std::to_string(trace.seed);
  }
  ExprOracleTrace fresh;
  fresh.seed = trace.seed;
  // Fresh RNG at the same seed reproduces the same expression; run the live
  // oracles and compare against the recorded outcome.
  std::mt19937 rng2(seed32);
  std::string report = RunExprOracleIteration(rng2, verbose, &fresh);
  if (report.empty()) {
    return "";
  }
  if (fresh.reference != trace.reference || fresh.actual != trace.actual) {
    // Still mismatching, but differently: the bug moved. Report the fresh
    // evidence while keeping the recorded trace for context.
    return "reproduces differently than recorded:\nrecorded reference=" +
           trace.reference + " actual=" + trace.actual +
           "\ncurrent reference=" + fresh.reference +
           " actual=" + fresh.actual + "\n" + report;
  }
  return report;
}

std::string SerializeExprOracleTest(const ExprOracleTrace& trace) {
  std::string out = std::string(kTestHeader) + "\n";
  out += "-- seed: " + std::to_string(trace.seed) + "\n";
  out += "-- sql: " + trace.sql + "\n";
  out += "-- sexpr: " + trace.sexpr + "\n";
  out += "-- reference: " + trace.reference + "\n";
  out += "-- actual: " + trace.actual + "\n";
  out += std::string("-- engine_ran: ") +
         (trace.engine_ran ? "true" : "false") + "\n";
  if (!trace.failure.empty()) {
    out += "-- failure: " + trace.failure + "\n";
  }
  out += "-- cross-check: printf '%s' '" + trace.sexpr +
         "' | python3 scripts/expr_oracle.py\n";
  return out;
}

bool ParseExprOracleTest(std::string_view text, ExprOracleTrace* trace) {
  *trace = ExprOracleTrace{};
  bool saw_header = false;
  size_t pos = 0;
  while (pos <= text.size()) {
    const size_t eol = text.find('\n', pos);
    std::string line(text.substr(pos, eol == std::string_view::npos
                                          ? std::string_view::npos
                                          : eol - pos));
    pos = (eol == std::string_view::npos) ? text.size() + 1 : eol + 1;
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (line.empty()) {
      continue;
    }
    if (line.starts_with("-- tinylamb-expr-oracle-test")) {
      saw_header = true;
      continue;
    }
    auto consume = [&](const std::string& tag, std::string* value) {
      if (!line.starts_with(tag)) {
        return false;
      }
      *value = line.substr(tag.size());
      return true;
    };
    std::string value;
    if (consume("-- seed: ", &value)) {
      trace->seed = std::stoull(value);
    } else if (consume("-- sql: ", &value)) {
      trace->sql = value;
    } else if (consume("-- sexpr: ", &value)) {
      trace->sexpr = value;
    } else if (consume("-- reference: ", &value)) {
      trace->reference = value;
    } else if (consume("-- actual: ", &value)) {
      trace->actual = value;
    } else if (consume("-- engine_ran: ", &value)) {
      if (value != "true" && value != "false") {
        return false;
      }
      trace->engine_ran = (value == "true");
    } else if (consume("-- failure: ", &value)) {
      trace->failure = value;
    } else if (consume("-- cross-check: ", &value)) {
      continue;  // documentation line, not part of the trace
    } else {
      return false;  // unknown line: refuse a half-understood file
    }
  }
  return saw_header && !trace->sql.empty() && !trace->sexpr.empty();
}

void ExprOracleFuzzTry(const uint8_t* data, size_t size, bool verbose) {
  uint64_t seed = 0x9e3779b97f4a7c15ULL;
  for (size_t i = 0; i < size; ++i) {
    seed = (seed * 257) + data[i] + 1;
  }
  const auto seed32 = static_cast<uint32_t>(seed ^ (seed >> 32));
  std::mt19937 rng(seed32);
  ExprOracleTrace trace;
  trace.seed = (static_cast<uint64_t>(seed32) << 32) | seed32;
  // RunExprOracleIteration consumes the RNG; replay derives its own RNG from
  // the stored seed, so re-seed an identical stream for the checked run.
  std::mt19937 run_rng(seed32);
  std::string report = RunExprOracleIteration(run_rng, verbose, &trace);
  if (report.empty()) {
    return;
  }

  // Prove the emitted file reproduces before saving it.
  const std::string text = SerializeExprOracleTest(trace);
  ExprOracleTrace parsed;
  std::string verify;
  if (!ParseExprOracleTest(text, &parsed)) {
    verify = "internal error: serialized test does not parse";
  } else if (!(parsed == trace)) {
    verify = "internal error: serialized test does not round-trip";
  } else {
    verify = ReplayExprOracleTrace(parsed, false);
    if (verify.empty()) {
      verify = "internal error: serialized test replay holds (flaky mismatch)";
    }
  }

  const std::string path =
      "expr_oracle_fuzz-repro-" + std::to_string(trace.seed) + ".test";
  std::ofstream file(path);
  file << text;
  file.close();

  std::cerr << "[expr_oracle_fuzz] seed=" << trace.seed << "\n"
            << report << "regression test written to " << path
            << (verify.empty() ? "" : "\n" + verify) << "\n";
  abort();
}

namespace {

struct ScopedDb {
  std::string name;
  std::unique_ptr<Database> db;
  ScopedDb(std::string n, std::unique_ptr<Database> d)
      : name(std::move(n)), db(std::move(d)) {}
  ScopedDb(const ScopedDb&) = delete;
  ScopedDb& operator=(const ScopedDb&) = delete;
  ScopedDb(ScopedDb&&) = delete;
  ScopedDb& operator=(ScopedDb&&) = delete;
  ~ScopedDb() {
    db.reset();
    std::error_code ec;
    std::filesystem::remove(name + ".log", ec);
    std::filesystem::remove(name + ".db", ec);
    std::filesystem::remove(name + ".last_checkpoint", ec);
  }
};

struct RowGenPred {
  Expression expr;
  std::string sql;
};

std::pair<Schema, std::vector<Row>> BuildRowFuzzDataset(std::mt19937& rng) {
  Schema schema("t_fuzz", {
                              Column("id", ValueType::kInt64),
                              Column("i", ValueType::kInt64),
                              Column("f", ValueType::kDouble),
                              Column("b", ValueType::kInt64),
                              Column("s", ValueType::kVarChar),
                          });

  std::vector<Row> rows;
  int64_t id = 0;

  auto add_row = [&](std::optional<int64_t> i_val, std::optional<double> f_val,
                     std::optional<int64_t> b_val,
                     std::optional<std::string> s_val) {
    std::vector<Value> vals;
    vals.reserve(5);
    vals.emplace_back(id++);
    if (i_val.has_value()) {
      vals.emplace_back(*i_val);
    } else {
      vals.emplace_back();
    }
    if (f_val.has_value()) {
      vals.emplace_back(*f_val);
    } else {
      vals.emplace_back();
    }
    if (b_val.has_value()) {
      vals.emplace_back(*b_val);
    } else {
      vals.emplace_back();
    }
    if (s_val.has_value()) {
      vals.emplace_back(std::string(*s_val));
    } else {
      vals.emplace_back();
    }
    rows.emplace_back(std::move(vals));
  };

  // Deterministic edge cases:
  add_row(std::nullopt, std::nullopt, std::nullopt, std::nullopt);
  add_row(0, 0.0, 0, "");
  add_row(1, 1.5, 1, "foo");
  add_row(-1, -2.5, 0, "bar");
  add_row(std::nullopt, 10.0, 1, "hello");
  add_row(42, std::nullopt, 0, "world");
  add_row(-99, 3.14, std::nullopt, "test");
  add_row(100, -0.5, 1, std::nullopt);
  add_row(std::numeric_limits<int64_t>::min(),
          -std::numeric_limits<double>::infinity(), 0, "min");
  add_row(std::numeric_limits<int64_t>::max(),
          std::numeric_limits<double>::infinity(), 1, "max");
  add_row(0, std::numeric_limits<double>::quiet_NaN(), std::nullopt, "nan");
  add_row(5, 5.0, 1, "%_");
  add_row(10, 0.0, 0, "abc");
  add_row(10, 1.0, 1, "a%b");
  add_row(std::nullopt, std::nullopt, 1, "two_nulls_1");
  add_row(std::nullopt, 2.0, std::nullopt, "two_nulls_2");
  add_row(3, std::nullopt, std::nullopt, "two_nulls_3");
  add_row(std::nullopt, 0.0, 0, std::nullopt);
  add_row(7, std::nullopt, 1, std::nullopt);

  // Random rows per seed
  for (int r = 0; r < 8; ++r) {
    const int pick_i = std::uniform_int_distribution<int>(0, 100)(rng);
    const int pick_f = std::uniform_int_distribution<int>(0, 100)(rng);
    const int pick_b = std::uniform_int_distribution<int>(0, 100)(rng);
    const int pick_s = std::uniform_int_distribution<int>(0, 100)(rng);

    std::optional<int64_t> i_v =
        (pick_i < 20)
            ? std::nullopt
            : std::optional<int64_t>(
                  std::uniform_int_distribution<int64_t>(-50, 50)(rng));
    std::optional<double> f_v =
        (pick_f < 20)
            ? std::nullopt
            : std::optional<double>(
                  std::uniform_int_distribution<int>(-50, 50)(rng) / 10.0);
    std::optional<int64_t> b_v =
        (pick_b < 20) ? std::nullopt
                      : std::optional<int64_t>(
                            std::uniform_int_distribution<int>(0, 1)(rng));
    std::optional<std::string> s_v =
        (pick_s < 20)
            ? std::nullopt
            : std::optional<std::string>(pick_s % 2 == 0 ? "abc" : "xyz");
    add_row(i_v, f_v, b_v, s_v);
  }

  return {schema, rows};
}

RowGenPred GenRowPred(std::mt19937& rng, int depth = 0) {
  auto pick = [&](int lo, int hi) {
    return std::uniform_int_distribution<int>(lo, hi)(rng);
  };

  if (depth >= 2 || pick(0, 10) < 4) {
    switch (pick(0, 13)) {
      case 0: {
        int64_t c = pick(-3, 3);
        return {.expr = BinaryExpressionExp(ColumnValueExp("i"),
                                            BinaryOperation::kEquals,
                                            ConstantValueExp(Value(c))),
                .sql = "(i = " + std::to_string(c) + ")"};
      }
      case 1: {
        int64_t c = pick(-3, 3);
        return {.expr = BinaryExpressionExp(ColumnValueExp("i"),
                                            BinaryOperation::kGreaterThan,
                                            ConstantValueExp(Value(c))),
                .sql = "(i > " + std::to_string(c) + ")"};
      }
      case 2: {
        int64_t c = pick(-3, 3);
        return {.expr = BinaryExpressionExp(ColumnValueExp("i"),
                                            BinaryOperation::kLessThanEquals,
                                            ConstantValueExp(Value(c))),
                .sql = "(i <= " + std::to_string(c) + ")"};
      }
      case 3: {
        double f = pick(-20, 20) / 10.0;
        return {.expr = BinaryExpressionExp(ColumnValueExp("f"),
                                            BinaryOperation::kGreaterThan,
                                            ConstantValueExp(Value(f))),
                .sql = "(f > " + std::to_string(f) + ")"};
      }
      case 4:
        return {.expr = BinaryExpressionExp(ColumnValueExp("s"),
                                            BinaryOperation::kEquals,
                                            ConstantValueExp(Value("foo"))),
                .sql = "(s = 'foo')"};
      case 5:
        return {.expr = BinaryExpressionExp(ColumnValueExp("s"),
                                            BinaryOperation::kLike,
                                            ConstantValueExp(Value("a%"))),
                .sql = "(s LIKE 'a%')"};
      case 6:
        return {.expr = UnaryExpressionExp(ColumnValueExp("i"),
                                           UnaryOperation::kIsNull),
                .sql = "(i IS NULL)"};
      case 7:
        return {.expr = UnaryExpressionExp(ColumnValueExp("i"),
                                           UnaryOperation::kIsNotNull),
                .sql = "(i IS NOT NULL)"};
      case 8:
        return {.expr = UnaryExpressionExp(ColumnValueExp("f"),
                                           UnaryOperation::kIsNull),
                .sql = "(f IS NULL)"};
      case 9:
        return {.expr = UnaryExpressionExp(ColumnValueExp("s"),
                                           UnaryOperation::kIsNotNull),
                .sql = "(s IS NOT NULL)"};
      case 10:
        return {
            .expr = BinaryExpressionExp(
                FunctionCallExp(
                    "coalesce",
                    {ColumnValueExp("i"), ConstantValueExp(Value(int64_t{0}))}),
                BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{0}))),
            .sql = "(COALESCE(i, 0) = 0)"};
      case 11:
        return {
            .expr = BinaryExpressionExp(
                FunctionCallExp("coalesce", {ColumnValueExp("s"),
                                             ConstantValueExp(Value("bar"))}),
                BinaryOperation::kEquals, ConstantValueExp(Value("bar"))),
            .sql = "(COALESCE(s, 'bar') = 'bar')"};
      case 12: {
        int64_t v1 = pick(-1, 1);
        int64_t v2 = pick(2, 5);
        return {.expr = InExpressionExp(
                    ColumnValueExp("i"),
                    {ConstantValueExp(Value(v1)), ConstantValueExp(Value(v2))}),
                .sql = "(i IN (" + std::to_string(v1) + ", " +
                       std::to_string(v2) + "))"};
      }
      default:
        return {.expr = BinaryExpressionExp(
                    ColumnValueExp("b"), BinaryOperation::kEquals,
                    ConstantValueExp(Value(int64_t{1}))),
                .sql = "(b = TRUE)"};
    }
  }

  switch (pick(0, 2)) {
    case 0: {
      auto l = GenRowPred(rng, depth + 1);
      auto r = GenRowPred(rng, depth + 1);
      return {
          .expr = BinaryExpressionExp(l.expr, BinaryOperation::kAnd, r.expr),
          .sql = "(" + l.sql + " AND " + r.sql + ")"};
    }
    case 1: {
      auto l = GenRowPred(rng, depth + 1);
      auto r = GenRowPred(rng, depth + 1);
      return {.expr = BinaryExpressionExp(l.expr, BinaryOperation::kOr, r.expr),
              .sql = "(" + l.sql + " OR " + r.sql + ")"};
    }
    default: {
      auto inner = GenRowPred(rng, depth + 1);
      return {.expr = UnaryExpressionExp(inner.expr, UnaryOperation::kNot),
              .sql = "(NOT " + inner.sql + ")"};
    }
  }
}

}  // namespace

std::string RunRowExprOracleIteration(std::mt19937& rng, bool verbose,
                                      RowExprOracleTrace* trace) {
  RowExprOracleTrace local;
  RowExprOracleTrace& t = (trace != nullptr) ? *trace : local;

  const auto [schema, rows] = BuildRowFuzzDataset(rng);
  const RowGenPred pred = GenRowPred(rng);
  t.predicate_sql = pred.sql;
  t.predicate_ast = pred.expr->ToString();
  t.total_rows = rows.size();

  std::string report;

  // Oracle 1: Null-Rejection Soundness Oracle
  static const std::vector<std::pair<std::string, slot_t>> kCheckCols = {
      {"i", 1}, {"f", 2}, {"b", 3}, {"s", 4}};
  for (const auto& [col_name, col_idx] : kCheckCols) {
    if (cascades::ExpressionRejectsNullsOnColumn(pred.expr, col_name)) {
      t.null_reject_claimed = true;
      t.null_reject_col = col_name;
      for (const auto& r_idx : rows) {
        if (r_idx[col_idx].IsNull()) {
          StatusOr<Value> v = pred.expr->TryEvaluate(r_idx, schema);
          if (v.HasValue() && !v.Value().IsNull() && v.Value().Truthy()) {
            report += "[NULL-REJECT SOUNDNESS VIOLATION]\n";
            report += "  Predicate claims to reject NULL on column '";
            report += col_name;
            report += "':\n  Expression SQL: ";
            report += pred.sql;
            report += "\n  Expression AST: ";
            report += pred.expr->ToString();
            report += "\n  Evaluated to TRUE on row where ";
            report += col_name;
            report += " is NULL:\n  Row:            ";
            report += r_idx.ToString();
            report += "\n";
            t.failure = "null-reject soundness violation on column " + col_name;
            return report;
          }
        }
      }
      t.null_reject_verified = true;
    }
  }

  // Oracle 2: Rewrite 3-Valued Logic Equivalence Oracle
  Expression rewritten =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(pred.expr);
  rewritten = RewriteTypedArithmetic(rewritten, schema);
  for (const auto& r_idx : rows) {
    StatusOr<Value> orig_v = pred.expr->TryEvaluate(r_idx, schema);
    StatusOr<Value> rew_v = rewritten->TryEvaluate(r_idx, schema);
    if (!orig_v.HasValue() || !rew_v.HasValue()) {
      if (orig_v.HasValue() != rew_v.HasValue()) {
        report +=
            "[REWRITE THROW MISMATCH]\n"
            "  Expression SQL: " +
            pred.sql +
            "\n"
            "  Original AST:   " +
            pred.expr->ToString() + " => " +
            (orig_v.HasValue() ? orig_v.Value().AsString() : "THROW") +
            "\n"
            "  Rewritten AST:  " +
            rewritten->ToString() + " => " +
            (rew_v.HasValue() ? rew_v.Value().AsString() : "THROW") +
            "\n"
            "  On Row:         " +
            r_idx.ToString() + "\n";
        t.failure = "rewrite throw mismatch";
        return report;
      }
      continue;
    }
    bool orig_pass = !orig_v.Value().IsNull() && orig_v.Value().Truthy();
    bool rew_pass = !rew_v.Value().IsNull() && rew_v.Value().Truthy();
    if (orig_pass != rew_pass) {
      report +=
          "[REWRITE TRUTHINESS MISMATCH]\n"
          "  Expression SQL: " +
          pred.sql +
          "\n"
          "  Original AST:   " +
          pred.expr->ToString() + " => " + (orig_pass ? "PASS" : "REJECT") +
          "\n"
          "  Rewritten AST:  " +
          rewritten->ToString() + " => " + (rew_pass ? "PASS" : "REJECT") +
          "\n"
          "  On Row:         " +
          r_idx.ToString() + "\n";
      t.failure = "rewrite truthiness mismatch";
      return report;
    }
  }

  // Oracle 3: Differential Execution Engine Oracle
  bool ast_threw = false;
  std::string ast_error;
  std::vector<int64_t> expected_ids;
  for (const Row& r : rows) {
    StatusOr<Value> v = pred.expr->TryEvaluate(r, schema);
    if (!v.HasValue()) {
      ast_threw = true;
      ast_error = v.GetStatus().GetMessage();
      break;
    }
    if (!v.Value().IsNull() && v.Value().Truthy()) {
      expected_ids.push_back(r[0].value.int_value);
    }
  }
  std::sort(expected_ids.begin(), expected_ids.end());
  t.matched_rows = expected_ids.size();
  t.matched_ids = expected_ids;

  const std::string db_name = "row_expr_fuzz-" + RandomString(8);
  auto db_holder = Database::Create(db_name).MoveValue();
  CHECK(db_holder != nullptr);
  ScopedDb sdb(db_name, std::move(db_holder));
  Database& db = *sdb.db;

  const std::string tab = "t_fuzz_" + RandomString(6);
  {
    TransactionContext ctx = db.BeginContext();
    SqlEngine engine(db);
    std::string ddl = "CREATE TABLE " + tab +
                      " (id INT64, i INT64, f FLOAT64, b BOOL, s VARCHAR(32));";
    StatusOr<QueryResult> ddl_res = engine.Execute(ctx, ddl);
    if (!ddl_res.HasValue()) {
      if (verbose) {
        std::cerr << "[row_expr_fuzz][skip] DDL failed: " << ddl << "\n";
      }
      return "";
    }
    ddl_res.Value().Drain();

    StatusOr<std::shared_ptr<Table>> tbl_or = ctx.GetTable(tab);
    if (!tbl_or.HasValue()) {
      if (verbose) {
        std::cerr << "[row_expr_fuzz][skip] GetTable failed: " << tab << "\n";
      }
      return "";
    }
    std::shared_ptr<Table> tbl = tbl_or.MoveValue();
    for (const Row& r : rows) {
      (void)tbl->Insert(ctx.txn_, r);
    }
    (void)ctx.PreCommit();
  }

  TransactionContext read_ctx = db.BeginReadOnlyContext();
  SqlEngine read_engine(db);
  std::string sql =
      "SELECT id FROM " + tab + " WHERE " + pred.sql + " ORDER BY id;";
  StatusOr<QueryResult> qr = read_engine.Execute(read_ctx, sql);
  if (!qr.HasValue()) {
    if (ast_threw) {
      return "";
    }
    if (verbose) {
      std::cerr << "[row_expr_fuzz][skip] engine rejected: " << sql << "\n";
    }
    return "";
  }
  t.engine_ran = true;
  std::vector<int64_t> engine_ids;
  Row row;
  while (qr.Value().Next(&row)) {
    if (row.Size() >= 1 && !row[0].IsNull()) {
      engine_ids.push_back(row[0].value.int_value);
    }
  }
  Status engine_status = qr.Value().GetStatus();
  if (ast_threw) {
    if (engine_status == Status::kSuccess) {
      report +=
          "[ENGINE DIFFERENTIAL MISMATCH: ENGINE DID NOT THROW]\n"
          "  Query SQL:     " +
          sql +
          "\n"
          "  AST threw:     " +
          ast_error +
          "\n"
          "  Engine status: Success\n";
      t.failure = "engine did not throw on error row";
      return report;
    }
    return "";
  }

  if (engine_status != Status::kSuccess) {
    report +=
        "[ENGINE EXECUTION ERROR]\n"
        "  Query SQL:     " +
        sql +
        "\n"
        "  Engine error:  " +
        engine_status.GetMessage() +
        "\n"
        "  AST evaluated: Success\n";
    t.failure = "engine failed unexpectedly";
    return report;
  }

  std::sort(engine_ids.begin(), engine_ids.end());

  if (engine_ids != expected_ids) {
    auto fmt = [](const std::vector<int64_t>& ids) {
      std::string out = "[";
      for (size_t i = 0; i < ids.size(); ++i) {
        if (i > 0) {
          out += ", ";
        }
        out += std::to_string(ids[i]);
      }
      out += "]";
      return out;
    };
    report +=
        "[ENGINE DIFFERENTIAL MISMATCH]\n"
        "  Query SQL:   " +
        sql +
        "\n"
        "  Expression:  " +
        pred.expr->ToString() +
        "\n"
        "  Expected IDs (AST ground truth): " +
        fmt(expected_ids) +
        "\n"
        "  Actual IDs   (SQL Engine):       " +
        fmt(engine_ids) + "\n";
    t.failure = "engine differential mismatch";
  }

  return report;
}

std::string ReplayRowExprOracleTrace(const RowExprOracleTrace& trace,
                                     bool verbose) {
  const auto seed32 = static_cast<uint32_t>(trace.seed);
  std::mt19937 rng(seed32);
  const auto [schema, rows] = BuildRowFuzzDataset(rng);
  const RowGenPred pred = GenRowPred(rng);
  if (pred.sql != trace.predicate_sql || rows.size() != trace.total_rows) {
    return "tampered trace: predicate_sql does not match seed " +
           std::to_string(trace.seed);
  }
  RowExprOracleTrace fresh;
  fresh.seed = trace.seed;
  std::mt19937 rng2(seed32);
  std::string report = RunRowExprOracleIteration(rng2, verbose, &fresh);
  if (fresh.matched_ids != trace.matched_ids) {
    return "tampered trace: matched_ids do not match regenerated run";
  }
  if (report.empty()) {
    return "";
  }
  if (fresh.failure != trace.failure) {
    return "reproduces differently than recorded:\n" + report;
  }
  return report;
}

}  // namespace tinylamb
