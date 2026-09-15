/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_join_fuzzer.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "query/fuzz_scoped_db.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"

namespace tinylamb {
namespace {

constexpr const char* kTestHeader = "-- tinylamb-join-test v1";
constexpr int64_t kNullRepr = INT64_MIN;

class Gen {
 public:
  explicit Gen(std::mt19937& rng) : rng_(rng) {}
  int Pick(int lo, int hi) {
    return std::uniform_int_distribution<int>(lo, hi)(rng_);
  }
  bool Chance(int percent) { return Pick(1, 100) <= percent; }

 private:
  std::mt19937& rng_;
};

// A mirror row of fact(u, k, a) or dim(k, tag).
struct FactRow {
  int64_t u{0};
  int64_t k{kNullRepr};
  int64_t a{kNullRepr};
};
struct DimRow {
  int64_t k{kNullRepr};
  int64_t tag{kNullRepr};  // 0 = 'x', 1 = 'yy'
};

std::string Fmt(int64_t v) {
  return v == kNullRepr ? "NULL" : std::string("|") + std::to_string(v) + "|";
}
std::string SqlInt(int64_t v) {
  return v == kNullRepr ? "NULL" : std::to_string(v);
}
std::string FmtStr(int64_t v) {
  if (v == kNullRepr) {
    return "NULL";
  }
  // Value::AsString keeps the quotes for VARCHAR cells.
  return v == 0 ? "|\"x\"|" : "|\"yy\"|";
}

struct EngineRow {
  std::vector<Value> cells;
  [[nodiscard]] const Value& At(size_t i) const { return cells[i]; }
};

std::string FormatEngineValue(const Value& v) {
  if (v.IsNull()) {
    return "NULL";
  }
  // INT64 columns render as |n|; VARCHAR as |text|.  The delimiters make
  // string/number confusions visible in the diff.
  const std::string text = v.AsString();
  return "|" + text + "|";
}

std::optional<std::vector<EngineRow>> RunQuery(Database& db,
                                               TransactionContext& ctx,
                                               const std::string& sql,
                                               std::string* error) {
  SqlEngine engine(db);
  StatusOr<QueryResult> result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    *error = engine.LastError();
    return std::nullopt;
  }
  std::vector<EngineRow> rows;
  Row row;
  while (result.Value().Next(&row)) {
    EngineRow er;
    for (size_t i = 0; i < row.values_.size(); ++i) {
      er.cells.push_back(row[i]);
    }
    rows.push_back(std::move(er));
  }
  return rows;
}

// Columns of the joined row in output order.
struct ProjCol {
  enum Kind { kFactU, kFactA, kFactK, kDimK, kDimTag } kind{kFactU};
};

std::string FormatJoined(const ProjCol& col, const FactRow* f,
                         const DimRow* d) {
  // On the null-extended side the pointer is null and every column of that
  // side renders as NULL - including the join key columns.
  switch (col.kind) {
    case ProjCol::kFactU:
      return f == nullptr ? std::string("NULL") : Fmt(f->u);
    case ProjCol::kFactA:
      return f == nullptr ? std::string("NULL") : Fmt(f->a);
    case ProjCol::kFactK:
      return f == nullptr ? std::string("NULL") : Fmt(f->k);
    case ProjCol::kDimK:
      return d == nullptr ? std::string("NULL") : Fmt(d->k);
    case ProjCol::kDimTag:
      return d == nullptr ? std::string("NULL") : FmtStr(d->tag);
  }
  return "NULL";
}

constexpr const char* kFactDdl =
    "CREATE TABLE fact (u INT64, k INT64, a INT64);";
constexpr const char* kDimDdl = "CREATE TABLE dim (k INT64, tag VARCHAR(8));";

}  // namespace

std::string RunJoinIteration(std::mt19937& rng, bool verbose, JoinStats* stats,
                             JoinTrace* trace) {
  Gen g(rng);
  JoinTrace local;
  JoinTrace& t = (trace != nullptr) ? *trace : local;

  t.setup.emplace_back(kFactDdl);
  t.setup.emplace_back(kDimDdl);
  std::vector<FactRow> facts;
  std::vector<DimRow> dims;
  const int fact_count = g.Pick(3, 8);
  for (int i = 0; i < fact_count; ++i) {
    FactRow r;
    r.u = i;
    r.k = g.Chance(20) ? kNullRepr : g.Pick(0, 3);
    r.a = g.Chance(25) ? kNullRepr : g.Pick(-3, 3);
    facts.push_back(r);
    t.setup.push_back("INSERT INTO fact VALUES (" + std::to_string(r.u) + ", " +
                      SqlInt(r.k) + ", " + SqlInt(r.a) + ");");
  }
  const int dim_count = g.Pick(2, 5);
  for (int i = 0; i < dim_count; ++i) {
    DimRow r;
    r.k = g.Chance(15) ? kNullRepr : g.Pick(0, 4);
    r.tag = g.Pick(0, 1);
    dims.push_back(r);
    t.setup.push_back("INSERT INTO dim VALUES (" + SqlInt(r.k) + ", " +
                      (r.tag == 0 ? "'x'" : "'yy'") + ");");
  }

  // ScopedDb deletes the throwaway .db/.log pair when the iteration ends.
  ScopedDb db_owner("sql_join_fuzz");
  Database& db = *db_owner;
  TransactionContext ctx = db.BeginContext();
  SqlEngine engine(db);
  for (const std::string& sql : t.setup) {
    StatusOr<QueryResult> result = engine.Execute(ctx, sql);
    if (!result.HasValue()) {
      if (verbose) {
        std::cerr << "[join_fuzz][skip-setup] " << sql << "\n";
      }
      return "";
    }
    // QueryResults are lazy: drain or the INSERT never happens.
    result.Value().Drain();
  }

  const int query_count = g.Pick(2, 5);
  std::string report;
  for (int q = 0; q < query_count; ++q) {
    const bool fact_left = g.Chance(60);
    // 0 = INNER, 1 = LEFT, 2 = RIGHT, 3 = FULL, 4 = CROSS.
    const int join_kind = g.Pick(0, 4);
    const std::string join_kw = join_kind == 0   ? "JOIN"
                                : join_kind == 1 ? "LEFT JOIN"
                                : join_kind == 2 ? "RIGHT JOIN"
                                : join_kind == 3 ? "FULL JOIN"
                                                 : "CROSS JOIN";
    // Projection: u and/or a from fact, tag and/or k from dim.
    std::vector<ProjCol> proj;
    std::string proj_sql;
    proj.push_back({ProjCol::kFactU});
    proj_sql = fact_left ? "f.u" : "u";
    if (g.Chance(70)) {
      proj.push_back({ProjCol::kFactA});
      proj_sql += ", " + std::string(fact_left ? "f.a" : "a");
    }
    if (g.Chance(60)) {
      proj.push_back({ProjCol::kDimTag});
      proj_sql += ", " + std::string(fact_left ? "d.tag" : "tag");
    }
    if (g.Chance(30)) {
      proj.push_back({fact_left ? ProjCol::kDimK : ProjCol::kFactK});
      proj_sql += ", " + std::string(fact_left ? "d.k" : "f.k");
    }
    const std::string fl = fact_left ? "fact f" : "dim d";
    const std::string fr = fact_left ? "dim d" : "fact f";
    // ON condition: equi-join on the keys, a non-equi comparison between
    // f.a and d.k, or f.k <cmp> d.k; optionally AND a residual predicate.
    std::string on_sql;
    int on_kind = 0;  // 0 = f.k = d.k, 1 = f.k <cmp> d.k,
                      // 2 = f.a <cmp> d.k
    std::string on_op = "=";
    int residual = 0;  // 0 = none, 1 = f.a <cmp> const, 2 = d.tag = 'x'
    std::string res_op;
    int64_t res_const = 0;
    if (join_kind != 4) {
      on_kind = g.Pick(0, 9) < 6 ? 0 : g.Pick(1, 2);
      if (on_kind == 0) {
        on_sql = "f.k = d.k";
      } else {
        static constexpr std::array<std::string_view, 6> kJoinOps = {
            "=", "<>", "<", "<=", ">", ">="};
        on_op = std::string(kJoinOps[g.Pick(0, 5)]);
        on_sql =
            on_kind == 1 ? "f.k " + on_op + " d.k" : "f.a " + on_op + " d.k";
      }
      if (g.Chance(30)) {
        residual = g.Pick(1, 2);
        if (residual == 1) {
          static constexpr std::array<std::string_view, 6> kJoinOps2 = {
              "=", "<>", "<", "<=", ">", ">="};
          res_op = std::string(kJoinOps2[g.Pick(0, 5)]);
          res_const = g.Pick(-3, 3);
          on_sql += " AND f.a " + res_op + " " + std::to_string(res_const);
        } else {
          on_sql += " AND d.tag = 'x'";
        }
      }
    }
    std::string sql;
    sql.reserve(proj_sql.size() + fl.size() + join_kw.size() + fr.size() + 64);
    sql += "SELECT ";
    sql += proj_sql;
    sql += " FROM ";
    sql += fl;
    sql += " ";
    sql += join_kw;
    sql += " ";
    sql += fr;
    if (join_kind != 4) {
      sql += " ON " + on_sql;
    }
    // Optional post-join filter over fact or dim columns.
    int filter = g.Pick(0, 3);
    if (filter == 1) {
      sql += " WHERE f.a IS NOT NULL";
    } else if (filter == 2) {
      sql += " WHERE f.k = 1";
    } else if (filter == 3) {
      sql += " WHERE d.tag = 'x'";
    }
    sql += ";";

    // Mirror: nested-loop join with SQL NULL semantics.  Preserved sides
    // are the left operand for LEFT, right for RIGHT, both for FULL.
    std::vector<std::string> expected;
    auto num_cmp = [](int64_t l, const std::string& o, int64_t r) {
      if (l == kNullRepr || r == kNullRepr) {
        return false;  // UNKNOWN never produces a join match
      }
      if (o == "=") {
        return l == r;
      }
      if (o == "<>") {
        return l != r;
      }
      if (o == "<") {
        return l < r;
      }
      if (o == "<=") {
        return l <= r;
      }
      if (o == ">") {
        return l > r;
      }
      return l >= r;
    };
    auto match = [&](const FactRow& f, const DimRow& d) {
      if (join_kind == 4) {
        return true;  // CROSS JOIN
      }
      bool ok = false;
      if (on_kind == 0) {
        ok = num_cmp(f.k, "=", d.k);
      } else if (on_kind == 1) {
        ok = num_cmp(f.k, on_op, d.k);
      } else {
        ok = num_cmp(f.a, on_op, d.k);
      }
      if (!ok) {
        return false;
      }
      if (residual == 1) {
        return num_cmp(f.a, res_op, res_const);
      }
      if (residual == 2) {
        return d.tag == 0;
      }
      return true;
    };
    auto passes_filter = [&](const FactRow* f, const DimRow* d) {
      if (filter == 1) {
        return f != nullptr && f->a != kNullRepr;
      }
      if (filter == 2) {
        return f != nullptr && f->k == 1;
      }
      if (filter == 3) {
        return d != nullptr && d->tag == 0;
      }
      return true;
    };
    auto emit = [&](const FactRow* f, const DimRow* d) {
      if (!passes_filter(f, d)) {
        return;
      }
      std::string row;
      for (const ProjCol& c : proj) {
        row += FormatJoined(c, f, d) + ",";
      }
      expected.push_back(row);
    };
    const bool preserve_left = (join_kind == 1 || join_kind == 3);
    const bool preserve_right = (join_kind == 2 || join_kind == 3);
    std::vector<bool> fact_matched(facts.size(), false);
    std::vector<bool> dim_matched(dims.size(), false);
    for (size_t i = 0; i < facts.size(); ++i) {
      for (size_t j = 0; j < dims.size(); ++j) {
        if (!match(facts[i], dims[j])) {
          continue;
        }
        fact_matched[i] = true;
        dim_matched[j] = true;
        emit(&facts[i], &dims[j]);
      }
    }
    for (size_t i = 0; i < facts.size(); ++i) {
      if (!fact_matched[i] && (fact_left ? preserve_left : preserve_right)) {
        emit(&facts[i], nullptr);
      }
    }
    for (size_t j = 0; j < dims.size(); ++j) {
      if (!dim_matched[j] && (fact_left ? preserve_right : preserve_left)) {
        emit(nullptr, &dims[j]);
      }
    }
    std::sort(expected.begin(), expected.end());

    std::string error;
    auto got = RunQuery(db, ctx, sql, &error);
    if (!got.has_value()) {
      if (verbose) {
        std::cerr << "[join_fuzz][skip-query] " << sql << " :: " << error
                  << "\n";
      }
      continue;
    }
    if (stats != nullptr) {
      ++stats->queries;
      stats->left_joins += (join_kind == 1 || join_kind == 3) ? 1 : 0;
      for (const FactRow& f : facts) {
        stats->null_key_rows += f.k == kNullRepr ? 1 : 0;
      }
    }
    std::vector<std::string> actual;
    for (const EngineRow& r : *got) {
      std::string row;
      for (size_t i = 0; i < r.cells.size() && i < proj.size(); ++i) {
        row += FormatEngineValue(r.At(i)) + ",";
      }
      actual.push_back(row);
    }
    std::sort(actual.begin(), actual.end());
    t.queries.push_back({sql, expected});
    if (actual == expected) {
      continue;
    }
    report += "[JOIN MISMATCH] " + sql + "\n";
    report += "  expected (" + std::to_string(expected.size()) + "):\n";
    for (const std::string& r : expected) {
      report += "    " + r + "\n";
    }
    report += "  actual (" + std::to_string(actual.size()) + "):\n";
    for (const std::string& r : actual) {
      report += "    " + r + "\n";
    }
    return report;  // first mismatch is enough; keep the session short
  }
  QueryExpect last;
  (void)last;
  return report;
}

std::string ReplayJoinTrace(const JoinTrace& trace, bool verbose) {
  if (trace.setup.empty()) {
    return "malformed trace: no setup";
  }
  ScopedDb db_owner("sql_join_replay");
  Database& db = *db_owner;
  TransactionContext ctx = db.BeginContext();
  SqlEngine engine(db);
  for (const std::string& sql : trace.setup) {
    StatusOr<QueryResult> result = engine.Execute(ctx, sql);
    // QueryResults are lazy: drain or the INSERT never happens.
    if (!result.HasValue()) {
      return "setup failed: " + sql;
    }
    result.Value().Drain();
  }
  for (const QueryExpect& q : trace.queries) {
    std::string error;
    auto got = RunQuery(db, ctx, q.sql, &error);
    if (!got.has_value()) {
      return "query failed: " + q.sql + " :: " + error;
    }
    std::vector<std::string> actual;
    for (const EngineRow& r : *got) {
      std::string row;
      for (const Value& v : r.cells) {
        row += FormatEngineValue(v) + ",";
      }
      actual.push_back(row);
    }
    std::sort(actual.begin(), actual.end());
    std::vector<std::string> expected = q.expected;
    std::sort(expected.begin(), expected.end());
    if (actual != expected) {
      std::string report = "[JOIN REPLAY MISMATCH] " + q.sql + "\n";
      report += "  expected:\n";
      for (const std::string& r : expected) {
        report += "    " + r + "\n";
      }
      report += "  actual:\n";
      for (const std::string& r : actual) {
        report += "    " + r + "\n";
      }
      return report;
    }
    if (verbose) {
      std::cerr << "[join_fuzz][replay-ok] " << q.sql << "\n";
    }
  }
  return "";
}

std::string SerializeJoinTest(uint64_t seed, const JoinTrace& trace,
                              const std::string& failure_summary) {
  std::string out = std::string(kTestHeader) + "\n";
  out += "-- seed: " + std::to_string(seed) + "\n";
  for (const std::string& sql : trace.setup) {
    out += "-- setup: " + sql + "\n";
  }
  for (const QueryExpect& q : trace.queries) {
    out += "-- query: " + q.sql + "\n";
    for (const std::string& row : q.expected) {
      out += "-- expect: " + row + "\n";
    }
  }
  if (!failure_summary.empty()) {
    out += "-- failure: " + failure_summary + "\n";
  }
  return out;
}

bool ParseJoinTest(std::string_view text, uint64_t* seed, JoinTrace* trace,
                   std::string* failure_summary) {
  *seed = 0;
  *trace = JoinTrace{};
  failure_summary->clear();
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
    if (line.starts_with("-- tinylamb-join-test")) {
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
      *seed = std::stoull(value);
    } else if (consume("-- setup: ", &value)) {
      trace->setup.push_back(value);
    } else if (consume("-- query: ", &value)) {
      trace->queries.push_back({value, {}});
    } else if (consume("-- expect: ", &value)) {
      if (trace->queries.empty()) {
        return false;
      }
      trace->queries.back().expected.push_back(value);
    } else if (consume("-- failure: ", &value)) {
      *failure_summary = value;
    } else {
      return false;
    }
  }
  return saw_header && !trace->setup.empty();
}

void JoinFuzzTry(const uint8_t* data, size_t size, bool verbose) {
  uint64_t seed = 0x2b3e6af1c97d4b8bULL;
  for (size_t i = 0; i < size; ++i) {
    seed = (seed * 257) + data[i] + 1;
  }
  std::mt19937 rng(static_cast<uint32_t>(seed ^ (seed >> 32)));
  JoinTrace trace;
  std::string report = RunJoinIteration(rng, verbose, nullptr, &trace);
  if (report.empty()) {
    return;
  }
  // Verify the serialized form reproduces, then persist and abort.
  const std::string text =
      SerializeJoinTest(seed, trace, "auto-generated by sql_join_fuzzer");
  uint64_t parsed_seed = 0;
  JoinTrace parsed;
  std::string summary;
  std::string verify;
  if (!ParseJoinTest(text, &parsed_seed, &parsed, &summary)) {
    verify = "internal error: serialized join test does not parse";
  } else {
    verify = ReplayJoinTrace(parsed, false);
    if (verify.empty()) {
      verify = "internal error: serialized join replay holds (flaky)";
    }
  }
  const std::string path =
      "sql_join_fuzz-repro-" + std::to_string(seed) + ".test";
  std::ofstream file(path);
  file << text;
  file.close();
  std::cerr << "[sql_join_fuzz] seed=" << seed << "\n"
            << report << "regression test written to " << path
            << (verify.empty() ? "" : "\n" + verify) << "\n";
  abort();
}

}  // namespace tinylamb
