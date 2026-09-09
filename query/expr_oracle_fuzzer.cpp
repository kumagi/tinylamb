/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/expr_oracle_fuzzer.hpp"

#include <cstdint>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "expression/expr_simplify_oracle.hpp"
#include "expression/expression.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
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

}  // namespace tinylamb
