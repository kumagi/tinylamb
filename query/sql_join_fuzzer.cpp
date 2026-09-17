/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_join_fuzzer.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <tuple>
#include <utility>
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
  // QueryResult is lazy: a mid-stream error truncates the result, which is
  // an engine bug for these always-valid generated queries — mark it so the
  // caller reports rather than skipping.
  if (Status st = result.Value().GetStatus(); st != Status::kSuccess) {
    *error = "stream-error: " + (st.GetMessage().empty()
                                     ? std::string(ToString(st.GetCode()))
                                     : std::string(st.GetMessage()));
    return std::nullopt;
  }
  return rows;
}

// Columns of the joined row in output order.
struct ProjCol {
  enum Kind {
    kFactU,
    kFactA,
    kFactK,
    kDimK,
    kDimTag,
    kDim2K,
    kDim2Tag
  } kind{kFactU};
};

std::string FormatJoined(const ProjCol& col, const FactRow* f, const DimRow* d,
                         const DimRow* d2 = nullptr) {
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
    case ProjCol::kDim2K:
      return d2 == nullptr ? std::string("NULL") : Fmt(d2->k);
    case ProjCol::kDim2Tag:
      return d2 == nullptr ? std::string("NULL") : FmtStr(d2->tag);
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
  // ~10% of iterations run a large round: 9000 fact rows push the planner
  // past the parallel-join threshold (~8192) that tiny tables never reach.
  const bool large = g.Chance(10);
  if (large) {
    const int64_t fn = 9000;
    const int64_t dn = 100;
    t.setup.push_back(
        "INSERT INTO fact SELECT n, CASE WHEN MOD(n, 11) = 0 THEN NULL ELSE "
        "MOD(n, 7) END, MOD(n, 5) - 2 FROM UNNEST(GENERATE_SERIES(0, " +
        std::to_string(fn - 1) + ")) AS n;");
    t.setup.push_back(
        "INSERT INTO dim SELECT MOD(n, 6), CASE WHEN MOD(n, 2) = 0 THEN 'x' "
        "ELSE 'yy' END FROM UNNEST(GENERATE_SERIES(0, " +
        std::to_string(dn - 1) + ")) AS n;");
    facts.reserve(static_cast<size_t>(fn));
    for (int64_t i = 0; i < fn; ++i) {
      facts.push_back({i, i % 11 == 0 ? kNullRepr : i % 7, (i % 5) - 2});
    }
    dims.reserve(static_cast<size_t>(dn));
    for (int64_t i = 0; i < dn; ++i) {
      dims.push_back({i % 6, i % 2});
    }
  } else {
    const int fact_count = g.Pick(3, 8);
    for (int i = 0; i < fact_count; ++i) {
      FactRow r;
      r.u = i;
      r.k = g.Chance(20) ? kNullRepr : g.Pick(0, 3);
      r.a = g.Chance(25) ? kNullRepr : g.Pick(-3, 3);
      facts.push_back(r);
      t.setup.push_back("INSERT INTO fact VALUES (" + std::to_string(r.u) +
                        ", " + SqlInt(r.k) + ", " + SqlInt(r.a) + ");");
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
  }
  // Hash map over dim for O(n+m) mirror evaluation on large rounds.
  std::multimap<int64_t, const DimRow*> dims_by_k;
  for (const DimRow& d : dims) {
    dims_by_k.emplace(d.k, &d);
  }

  // ScopedDb deletes the throwaway .db/.log pair when the iteration ends.
  ScopedDb db_owner("sql_join_fuzz");
  if (!db_owner) {
    return "";  // resource pressure: iteration skipped
  }
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
    // Three-table chain: (fact <j1> dim d) <j2> dim d2.  Exercises join
    // ordering plus NULL-extended rows flowing into a second ON clause.
    if (!large && g.Chance(35)) {
      auto kw = [](int kind) {
        return kind == 0   ? "JOIN"
               : kind == 1 ? "LEFT JOIN"
               : kind == 2 ? "RIGHT JOIN"
                           : "FULL JOIN";
      };
      const int j1 = g.Pick(0, 3);
      const int j2 = g.Pick(0, 3);
      // on1 over (f,d); on2 can reference f, d, or d2.
      const int on1_kind = g.Chance(60) ? 0 : g.Pick(1, 2);
      static constexpr std::array<std::string_view, 6> kOps = {
          "=", "<>", "<", "<=", ">", ">="};
      const std::string on1_op = std::string(kOps[g.Pick(0, 5)]);
      std::string on1 = on1_kind == 0   ? "f.k = d.k"
                        : on1_kind == 1 ? "f.k " + on1_op + " d.k"
                                        : "f.a " + on1_op + " d.k";
      const int on2_kind = g.Pick(0, 3);  // 0 f.k=d2.k, 1 d.k=d2.k,
                                          // 2 f.a<cmp>d2.k, 3 d.k<cmp>d2.k
      const std::string on2_op = std::string(kOps[g.Pick(0, 5)]);
      std::string on2 = on2_kind == 0   ? "f.k = d2.k"
                        : on2_kind == 1 ? "d.k = d2.k"
                        : on2_kind == 2 ? "f.a " + on2_op + " d2.k"
                                        : "d.k " + on2_op + " d2.k";
      const int res2 =
          g.Chance(30) ? g.Pick(1, 2) : 0;  // d2.tag='x' / f.a<cmp>
      std::string res2_op;
      int64_t res2_const = 0;
      if (res2 == 1) {
        on2 += " AND d2.tag = 'x'";
      } else if (res2 == 2) {
        res2_op = std::string(kOps[g.Pick(0, 5)]);
        res2_const = g.Pick(-3, 3);
        on2 += " AND f.a " + res2_op + " " + std::to_string(res2_const);
      }
      std::vector<ProjCol> proj{{ProjCol::kFactU}};
      std::string proj_sql = "f.u";
      if (g.Chance(70)) {
        proj.push_back({ProjCol::kFactA});
        proj_sql += ", f.a";
      }
      if (g.Chance(60)) {
        proj.push_back({ProjCol::kDimTag});
        proj_sql += ", d.tag";
      }
      if (g.Chance(60)) {
        proj.push_back({ProjCol::kDim2Tag});
        proj_sql += ", d2.tag";
      }
      if (g.Chance(30)) {
        proj.push_back({ProjCol::kDim2K});
        proj_sql += ", d2.k";
      }
      std::string sql = "SELECT " + proj_sql + " FROM fact f " + kw(j1) +
                        " dim d ON " + on1 + " " + kw(j2) + " dim d2 ON " +
                        on2 + ";";

      auto num_cmp = [](int64_t l, const std::string& o, int64_t r) {
        if (l == kNullRepr || r == kNullRepr) {
          return false;
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
      auto match1 = [&](const FactRow& f, const DimRow& d) {
        if (on1_kind == 0) {
          return num_cmp(f.k, "=", d.k);
        }
        return num_cmp(on1_kind == 1 ? f.k : f.a, on1_op, d.k);
      };
      struct StageRow {
        const FactRow* f;
        const DimRow* d;
      };
      auto match2 = [&](const StageRow& r, const DimRow& d2) {
        bool ok;
        switch (on2_kind) {
          case 0:
            ok = r.f != nullptr && num_cmp(r.f->k, "=", d2.k);
            break;
          case 1:
            ok = r.d != nullptr && num_cmp(r.d->k, "=", d2.k);
            break;
          case 2:
            ok = r.f != nullptr && num_cmp(r.f->a, on2_op, d2.k);
            break;
          default:
            ok = r.d != nullptr && num_cmp(r.d->k, on2_op, d2.k);
            break;
        }
        if (!ok) {
          return false;
        }
        if (res2 == 1) {
          return d2.tag == 0;
        }
        if (res2 == 2) {
          return r.f != nullptr && num_cmp(r.f->a, res2_op, res2_const);
        }
        return true;
      };
      const bool pl1 = j1 == 1 || j1 == 3;
      const bool pr1 = j1 == 2 || j1 == 3;
      const bool pl2 = j2 == 1 || j2 == 3;
      const bool pr2 = j2 == 2 || j2 == 3;
      std::vector<StageRow> s1;
      std::vector<bool> f_hit(facts.size(), false);
      std::vector<bool> d_hit(dims.size(), false);
      for (size_t i = 0; i < facts.size(); ++i) {
        for (size_t j = 0; j < dims.size(); ++j) {
          if (!match1(facts[i], dims[j])) {
            continue;
          }
          f_hit[i] = true;
          d_hit[j] = true;
          s1.push_back({&facts[i], &dims[j]});
        }
      }
      for (size_t i = 0; i < facts.size(); ++i) {
        if (!f_hit[i] && pl1) {
          s1.push_back({&facts[i], nullptr});
        }
      }
      for (size_t j = 0; j < dims.size(); ++j) {
        if (!d_hit[j] && pr1) {
          s1.push_back({nullptr, &dims[j]});
        }
      }
      std::vector<std::string> expected;
      std::vector<bool> d2_hit(dims.size(), false);
      auto emit = [&](const StageRow& r, const DimRow* d2) {
        std::string row;
        for (const ProjCol& c : proj) {
          row += FormatJoined(c, r.f, r.d, d2) + ",";
        }
        expected.push_back(row);
      };
      std::vector<bool> s1_hit(s1.size(), false);
      for (size_t i = 0; i < s1.size(); ++i) {
        for (size_t j = 0; j < dims.size(); ++j) {
          if (!match2(s1[i], dims[j])) {
            continue;
          }
          s1_hit[i] = true;
          d2_hit[j] = true;
          emit(s1[i], &dims[j]);
        }
      }
      for (size_t i = 0; i < s1.size(); ++i) {
        if (!s1_hit[i] && pl2) {
          emit(s1[i], nullptr);
        }
      }
      for (size_t j = 0; j < dims.size(); ++j) {
        if (!d2_hit[j] && pr2) {
          emit({nullptr, nullptr}, &dims[j]);
        }
      }
      std::sort(expected.begin(), expected.end());
      std::string error;
      auto got = RunQuery(db, ctx, sql, &error);
      if (!got.has_value()) {
        if (verbose) {
          std::cerr << "[join_fuzz][skip-chain] " << sql << " :: " << error
                    << "\n";
        }
        continue;
      }
      if (stats != nullptr) {
        ++stats->queries;
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
      if (actual != expected) {
        report += "[JOIN MISMATCH] " + sql + "\n";
        report += "  expected (" + std::to_string(expected.size()) + "):\n";
        for (const std::string& r : expected) {
          report += "    " + r + "\n";
        }
        report += "  actual (" + std::to_string(actual.size()) + "):\n";
        for (const std::string& r : actual) {
          report += "    " + r + "\n";
        }
      }
      continue;
    }
    // ---- join shape ----------------------------------------------------
    // kind: 0 INNER, 1 LEFT, 2 RIGHT, 3 FULL.  ON: equi on k, optionally
    // with an extra conjunct on either side, or a non-equi range predicate.
    const int join_kind = g.Pick(0, 4);
    static const std::array<const char*, 5> kJoinKw = {
        "JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN"};
    const char* join_kw = kJoinKw[static_cast<size_t>(join_kind)];
    int on_kind = join_kind == 4 ? 0 : g.Pick(0, 11);
    if (join_kind != 0 && on_kind >= 9) {
      on_kind = 0;  // keep OUTER-join ONs equi-based; non-equi outer ONs are
                    // rarer in the supported surface
    }
    // Extra conjunct parameters.
    static const std::array<const char*, 6> kOps = {"<",  "<=", ">",
                                                    ">=", "=",  "<>"};
    const char* fact_op = kOps[static_cast<size_t>(g.Pick(0, 5))];
    const int64_t fact_const = g.Pick(-3, 3);
    const int64_t dim_tag = g.Pick(0, 1);

    std::string on_sql;
    switch (on_kind) {
      case 6:
        on_sql = "f.k = d.k AND f.a " + std::string(fact_op) + " " +
                 std::to_string(fact_const);
        break;
      case 7:
        on_sql = std::string("f.k = d.k AND d.tag = '") +
                 (dim_tag == 0 ? "x" : "yy") + "'";
        break;
      case 8:
        on_sql = "f.k = d.k AND f.a " + std::string(fact_op) + " " +
                 std::to_string(fact_const) + " AND d.tag = '" +
                 (dim_tag == 0 ? "x" : "yy") + "'";
        break;
      case 9:
        on_sql = "f.a <= d.k";
        break;
      case 10:
        on_sql = "f.k " + std::string(fact_op) + " d.k";
        break;
      case 11:
        on_sql = "f.a " + std::string(fact_op) + " d.k";
        break;
      default:
        on_sql = "f.k = d.k";
        break;
    }

    auto cmp = [](int64_t l, const char* op, int64_t r) {
      if (op[0] == '<' && op[1] == '>') {
        return l != r;
      }
      switch (op[0]) {
        case '<':
          return op[1] == '=' ? l <= r : l < r;
        case '>':
          return op[1] == '=' ? l >= r : l > r;
        case '=':
          return l == r;
        default:
          return l != r;
      }
    };
    auto on_match = [&](const FactRow& f, const DimRow& d) {
      if (join_kind == 4) {
        return true;  // CROSS JOIN
      }
      if (on_kind == 9) {
        return f.a != kNullRepr && d.k != kNullRepr && f.a <= d.k;
      }
      if (on_kind >= 10) {
        const int64_t lhs = on_kind == 10 ? f.k : f.a;
        return lhs != kNullRepr && d.k != kNullRepr && cmp(lhs, fact_op, d.k);
      }
      if (f.k == kNullRepr || d.k == kNullRepr || f.k != d.k) {
        return false;
      }
      if ((on_kind == 6 || on_kind == 8) &&
          (f.a == kNullRepr || !cmp(f.a, fact_op, fact_const))) {
        return false;
      }
      if ((on_kind == 7 || on_kind == 8) && d.tag != dim_tag) {
        return false;
      }
      return true;
    };

    // Materialize the join result as (fact*, dim*) pairs, including the
    // null-extended rows of preserved sides.
    std::vector<std::pair<const FactRow*, const DimRow*>> joined;
    auto fact_matched = [&](const FactRow& f, std::vector<const DimRow*>* out) {
      out->clear();
      if (join_kind == 4 || on_kind >= 9) {
        for (const DimRow& d : dims) {
          if (on_match(f, d)) {
            out->push_back(&d);
          }
        }
      } else if (large) {
        auto range = dims_by_k.equal_range(f.k);
        for (auto it = range.first; it != range.second; ++it) {
          if (on_match(f, *it->second)) {
            out->push_back(it->second);
          }
        }
      } else {
        for (const DimRow& d : dims) {
          if (on_match(f, d)) {
            out->push_back(&d);
          }
        }
      }
      return !out->empty();
    };
    {
      std::vector<const DimRow*> hits;
      std::vector<bool> dim_hit(dims.size(), false);
      for (const FactRow& f : facts) {
        if (fact_matched(f, &hits)) {
          for (const DimRow* d : hits) {
            joined.emplace_back(&f, d);
            if (join_kind == 2 || join_kind == 3) {
              dim_hit[static_cast<size_t>(d - dims.data())] = true;
            }
          }
        } else if (join_kind == 1 || join_kind == 3) {
          joined.emplace_back(&f, nullptr);
        }
      }
      if (join_kind == 2 || join_kind == 3) {
        for (size_t i = 0; i < dims.size(); ++i) {
          if (!dim_hit[i]) {
            joined.emplace_back(nullptr, &dims[i]);
          }
        }
      }
    }
    const std::string join_sql =
        join_kind == 4
            ? "fact f CROSS JOIN dim d"
            : "fact f " + std::string(join_kw) + " dim d ON " + on_sql;

    // ---- query shape ----------------------------------------------------
    // Large rounds stay on count-shaped probes to keep .test repros small.
    const int shape = large ? g.Pick(0, 6) : g.Pick(0, 9);
    std::string sql;
    std::vector<std::string> expected;
    switch (shape) {
      case 0: {  // plain projection + optional post-join WHERE
        std::vector<ProjCol> proj{{ProjCol::kFactU}};
        std::string proj_sql = "f.u";
        if (g.Chance(70)) {
          proj.push_back({ProjCol::kFactA});
          proj_sql += ", f.a";
        }
        if (g.Chance(60)) {
          proj.push_back({ProjCol::kDimTag});
          proj_sql += ", d.tag";
        }
        if (g.Chance(30)) {
          proj.push_back({ProjCol::kDimK});
          proj_sql += ", d.k";
        }
        const int filter = g.Pick(0, 2);
        sql = "SELECT ";
        sql += proj_sql;
        sql += " FROM ";
        sql += join_sql;
        if (filter == 1) {
          sql += " WHERE f.a IS NOT NULL";
        } else if (filter == 2) {
          sql += " WHERE f.k = 1";
        }
        sql += ";";
        for (const auto& [f, d] : joined) {
          if (filter == 1 && (f == nullptr || f->a == kNullRepr)) {
            continue;
          }
          if (filter == 2 && (f == nullptr || f->k != 1)) {
            continue;
          }
          std::string row;
          for (const ProjCol& c : proj) {
            row += FormatJoined(c, f, d) + ",";
          }
          expected.push_back(row);
        }
        break;
      }
      case 1: {  // scalar aggregates over the joined multiset
        sql =
            "SELECT COUNT(*), COUNT(d.tag), COUNT(DISTINCT f.k), SUM(f.a),"
            " MIN(f.a), MAX(d.k) FROM " +
            join_sql + ";";
        int64_t cnt = 0, cnt_tag = 0;
        std::set<int64_t> distinct_k;
        int64_t sum = 0, mn = 0, mx = 0;
        bool any_a = false, any_dk = false;
        for (const auto& [f, d] : joined) {
          ++cnt;
          if (d != nullptr) {
            ++cnt_tag;
            if (d->k != kNullRepr) {
              mx = !any_dk || d->k > mx ? d->k : mx;
              any_dk = true;
            }
          }
          if (f != nullptr && f->a != kNullRepr) {
            sum += f->a;
            mn = !any_a || f->a < mn ? f->a : mn;
            any_a = true;
          }
          if (f != nullptr && f->k != kNullRepr) {
            distinct_k.insert(f->k);
          }
        }
        expected.push_back(Fmt(cnt) + "," + Fmt(cnt_tag) + "," +
                           Fmt(static_cast<int64_t>(distinct_k.size())) + "," +
                           (any_a ? Fmt(sum) : "NULL") + "," +
                           (any_a ? Fmt(mn) : "NULL") + "," +
                           (any_dk ? Fmt(mx) : "NULL") + ",");
        break;
      }
      case 2: {  // GROUP BY over a join column (+ HAVING)
        const bool by_tag = g.Chance(50);
        const int64_t having = g.Pick(1, 3);
        if (by_tag) {
          sql = "SELECT d.tag, COUNT(*) FROM " + join_sql +
                " GROUP BY d.tag HAVING COUNT(*) >= " + std::to_string(having) +
                ";";
          std::map<int64_t, int64_t> groups;
          for (const auto& [f, d] : joined) {
            ++groups[d == nullptr ? kNullRepr : d->tag];
          }
          for (const auto& [tag, c] : groups) {
            if (c >= having) {
              expected.push_back(FmtStr(tag) + "," + Fmt(c) + ",");
            }
          }
        } else {
          sql = "SELECT f.k, COUNT(*) FROM " + join_sql +
                " GROUP BY f.k HAVING COUNT(*) >= " + std::to_string(having) +
                ";";
          std::map<int64_t, int64_t> groups;
          for (const auto& [f, d] : joined) {
            ++groups[f == nullptr ? kNullRepr : f->k];
          }
          for (const auto& [k, c] : groups) {
            if (c >= having) {
              expected.push_back(Fmt(k) + "," + Fmt(c) + ",");
            }
          }
        }
        break;
      }
      case 3: {  // semi join via EXISTS, optionally with a dim-side conjunct
        const bool tag_conj = g.Chance(40);
        sql =
            "SELECT f.u FROM fact f WHERE EXISTS (SELECT 1 FROM dim d WHERE "
            "d.k = f.k" +
            (tag_conj ? std::string(" AND d.tag = '") +
                            (dim_tag == 0 ? "x" : "yy") + "'"
                      : "") +
            ") ORDER BY f.u;";
        for (const FactRow& f : facts) {
          bool found = false;
          auto range = dims_by_k.equal_range(f.k);
          for (auto it = range.first; it != range.second; ++it) {
            if (f.k != kNullRepr && (!tag_conj || it->second->tag == dim_tag)) {
              found = true;
              break;
            }
          }
          if (found) {
            expected.push_back(Fmt(f.u) + ",");
          }
        }
        break;
      }
      case 4: {  // anti join via NOT EXISTS
        const bool tag_conj = g.Chance(40);
        sql =
            "SELECT f.u FROM fact f WHERE NOT EXISTS (SELECT 1 FROM dim d "
            "WHERE d.k = f.k" +
            (tag_conj ? std::string(" AND d.tag = '") +
                            (dim_tag == 0 ? "x" : "yy") + "'"
                      : "") +
            ") ORDER BY f.u;";
        for (const FactRow& f : facts) {
          bool found = false;
          auto range = dims_by_k.equal_range(f.k);
          for (auto it = range.first; it != range.second; ++it) {
            if (f.k != kNullRepr && (!tag_conj || it->second->tag == dim_tag)) {
              found = true;
              break;
            }
          }
          if (!found) {
            expected.push_back(Fmt(f.u) + ",");
          }
        }
        break;
      }
      case 5: {  // IN subquery: NULL keys never emit (result is never TRUE)
        sql =
            "SELECT f.u FROM fact f WHERE f.k IN (SELECT k FROM dim) "
            "ORDER BY f.u;";
        for (const FactRow& f : facts) {
          if (f.k != kNullRepr && dims_by_k.contains(f.k)) {
            expected.push_back(Fmt(f.u) + ",");
          }
        }
        break;
      }
      case 6: {  // NOT IN: empty when the subquery can yield NULL
        sql =
            "SELECT f.u FROM fact f WHERE f.k NOT IN (SELECT k FROM dim) "
            "ORDER BY f.u;";
        bool dim_has_null = false;
        for (const DimRow& d : dims) {
          dim_has_null |= d.k == kNullRepr;
        }
        if (!dim_has_null) {
          for (const FactRow& f : facts) {
            if (f.k != kNullRepr && !dims_by_k.contains(f.k)) {
              expected.push_back(Fmt(f.u) + ",");
            }
          }
        }
        break;
      }
      case 7: {  // ORDER BY over all projected cols + LIMIT. The (u, k,
                 // tag) ordering is total on inner-equi output, so the
                 // boundary is deterministic.
        const int64_t lim = g.Pick(1, 6);
        const bool simple = join_kind == 0 && on_kind < 9;
        sql = "SELECT f.u, d.k, d.tag FROM " +
              (simple ? join_sql
                      : std::string("fact f JOIN dim d ON f.k = d.k")) +
              " ORDER BY f.u, d.k, d.tag LIMIT " + std::to_string(lim) + ";";
        std::vector<std::tuple<int64_t, int64_t, int64_t>> keyed;
        auto add_keyed = [&](const FactRow* f, const DimRow* d) {
          if (f != nullptr && d != nullptr) {
            keyed.emplace_back(f->u, d->k, d->tag);
          }
        };
        if (simple) {
          for (const auto& [f, d] : joined) {
            add_keyed(f, d);
          }
        } else {
          for (const FactRow& f : facts) {
            auto range = dims_by_k.equal_range(f.k);
            for (auto it = range.first; it != range.second; ++it) {
              if (f.k != kNullRepr) {
                add_keyed(&f, it->second);
              }
            }
          }
        }
        std::sort(keyed.begin(), keyed.end());
        for (size_t i = 0; i < keyed.size() && std::cmp_less(i, lim); ++i) {
          const auto& [u, k, tag] = keyed[i];
          expected.push_back(Fmt(u) + "," + Fmt(k) + "," + FmtStr(tag) + ",");
        }
        break;
      }
      case 8: {  // set operation over both base tables (NULL == NULL)
        static const std::array<const char*, 4> kSetOps = {
            "UNION ALL", "UNION", "INTERSECT", "EXCEPT"};
        const char* op = kSetOps[static_cast<size_t>(g.Pick(0, 3))];
        sql = "SELECT a FROM fact " + std::string(op) + " SELECT k FROM dim;";
        std::multiset<int64_t> left, right, out;
        for (const FactRow& f : facts) {
          left.insert(f.a);
        }
        for (const DimRow& d : dims) {
          right.insert(d.k);
        }
        if (op[0] == 'U' && op[6] == 'A') {  // UNION ALL
          out = left;
          out.insert(right.begin(), right.end());
        } else {
          std::set<int64_t> ls(left.begin(), left.end());
          std::set<int64_t> rs(right.begin(), right.end());
          std::set<int64_t> res;
          if (op[0] == 'U') {
            std::set_union(ls.begin(), ls.end(), rs.begin(), rs.end(),
                           std::inserter(res, res.begin()));
          } else if (op[0] == 'I') {
            std::set_intersection(ls.begin(), ls.end(), rs.begin(), rs.end(),
                                  std::inserter(res, res.begin()));
          } else {
            std::set_difference(ls.begin(), ls.end(), rs.begin(), rs.end(),
                                std::inserter(res, res.begin()));
          }
          out.insert(res.begin(), res.end());
        }
        for (int64_t v : out) {
          expected.push_back(Fmt(v) + ",");
        }
        break;
      }
      default: {  // CTE over a filtered dim, then a semi probe
        sql = "WITH xd AS (SELECT k FROM dim WHERE tag = '" +
              std::string(dim_tag == 0 ? "x" : "yy") +
              "') SELECT COUNT(*) FROM fact f WHERE f.k IN (SELECT k FROM "
              "xd);";
        int64_t cnt = 0;
        for (const FactRow& f : facts) {
          if (f.k == kNullRepr) {
            continue;
          }
          auto range = dims_by_k.equal_range(f.k);
          for (auto it = range.first; it != range.second; ++it) {
            if (it->second->tag == dim_tag) {
              ++cnt;
              break;
            }
          }
        }
        expected.push_back(Fmt(cnt) + ",");
        break;
      }
    }
    std::sort(expected.begin(), expected.end());

    std::string error;
    auto got = RunQuery(db, ctx, sql, &error);
    if (!got.has_value()) {
      if (error.starts_with("stream-error")) {
        report += "[JOIN MISMATCH] query aborted mid-stream: ";
        report += sql;
        report += "\n  ";
        report += error;
        report += "\n";
        return report;
      }
      if (verbose) {
        std::cerr << "[join_fuzz][skip-query] " << sql << " :: " << error
                  << "\n";
      }
      continue;
    }
    if (stats != nullptr) {
      ++stats->queries;
      stats->left_joins +=
          (join_kind == 1 || join_kind == 2 || join_kind == 3) ? 1 : 0;
      for (const FactRow& f : facts) {
        stats->null_key_rows += f.k == kNullRepr ? 1 : 0;
      }
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
  if (!db_owner) {
    return "";  // resource pressure: replay skipped, not a mismatch
  }
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
