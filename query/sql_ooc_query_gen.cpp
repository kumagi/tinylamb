/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_ooc_query_gen.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace tinylamb {
namespace {

std::string I(int64_t x) { return std::to_string(x); }

const std::vector<std::string>& TagLits() {
  static const std::vector<std::string> kLits = {"'a'", "'bb'", "'ccc'",
                                                 "'dddd'"};
  return kLits;
}

// One select-list item of an inner query.
struct OutCol {
  std::string expr;
  bool is_string{false};
  bool wide{false};
};

// An inner query whose columns are aliased c1..cn.
struct Inner {
  std::string sql;
  std::vector<OutCol> cols;
  int64_t est_rows{0};
  bool heavy{false};
};

std::string C(const std::string& alias, const char* col) {
  return alias.empty() ? std::string(col) : alias + "." + col;
}

std::string SelectList(const std::vector<OutCol>& cols) {
  std::string out;
  for (size_t i = 0; i < cols.size(); ++i) {
    if (i > 0) {
      out += ", ";
    }
    out += cols[i].expr + " AS c" + std::to_string(i + 1);
  }
  return out;
}

class Builder {
 public:
  Builder(OocRng& g, const std::vector<OocGenTable>& tables, int64_t cap)
      : g_(g), tables_(tables), cap_(std::max<int64_t>(cap, 2000)) {}

  OocGenQuery Build() {
    const int64_t roll = g_.Pick(0, 999);
    if (roll < 110) {
      return Sort();
    }
    if (roll < 150) {
      return FullSort();
    }
    if (roll < 300) {
      return GroupAgg();
    }
    if (roll < 350) {
      return Distinct();
    }
    if (roll < 500) {
      return Join();
    }
    if (roll < 540) {
      return Join3();
    }
    if (roll < 620) {
      return SemiAnti();
    }
    if (roll < 740) {
      return Window();
    }
    if (roll < 800) {
      return SetOp();
    }
    if (roll < 835) {
      return Recursive();
    }
    if (roll < 870) {
      return Apply();
    }
    if (roll < 900) {
      return Cross();
    }
    if (roll < 940) {
      return Band();
    }
    if (roll < 980) {
      return IndexPath();
    }
    return Nested();
  }

 private:
  const OocGenTable& T() {
    return tables_[static_cast<size_t>(
        g_.Pick(0, static_cast<int64_t>(tables_.size()) - 1))];
  }
  int64_t Key(const OocGenTable& t) { return g_.Pick(t.key_lo, t.key_hi); }
  const std::string& Tag() { return g_.OneOf(TagLits()); }

  // ---- predicates ---------------------------------------------------------

  std::string Atom(const std::string& a, const OocGenTable& t) {
    switch (g_.Pick(0, 11)) {
      case 0: {
        const int64_t lo = Key(t);
        const int64_t span = std::max<int64_t>(1, t.key_hi - t.key_lo);
        return C(a, "k") + " BETWEEN " + I(lo) + " AND " +
               I(lo + g_.Pick(0, g_.Chance(50) ? span : span / 8));
      }
      case 1: {
        const int64_t m = g_.Pick(2, 9);
        return "MOD(" + C(a, "k") + ", " + I(m) + ") = " + I(g_.Pick(0, m - 1));
      }
      case 2:
        return C(a, "g") + " = " + I(g_.Pick(0, t.groups - 1));
      case 3:
        return C(a, "g") + " IN (" + I(g_.Pick(0, t.groups - 1)) + ", " +
               I(g_.Pick(0, t.groups)) + ", " + I(g_.Pick(0, t.groups + 2)) +
               ")";
      case 4:
        return C(a, "v") + " > " + I(g_.Pick(0, t.v_mod));
      case 5:
        return C(a, "v") + " IS NULL";
      case 6:
        return C(a, "v") + " IS NOT NULL";
      case 7:
        return C(a, "t") + " = " + Tag();
      case 8:
        return C(a, "t") + " IS NULL";
      case 9:
        return "LENGTH(" + C(a, "pad") + ") > " +
               I(g_.Pick(0, static_cast<int64_t>(t.pad_max)));
      case 10:
        return C(a, "pad") + " LIKE '" + I(g_.Pick(1, 9)) + "%'";
      default: {
        const int64_t lo = g_.Pick(0, t.v_mod);
        return C(a, "v") + " BETWEEN " + I(lo) + " AND " +
               I(lo + g_.Pick(0, t.v_mod / 2 + 1));
      }
    }
  }

  std::string Pred(const std::string& a, const OocGenTable& t) {
    const int64_t roll = g_.Pick(0, 99);
    if (roll < 55) {
      return Atom(a, t);
    }
    if (roll < 75) {
      return Atom(a, t) + " AND " + Atom(a, t);
    }
    if (roll < 92) {
      return "(" + Atom(a, t) + " OR " + Atom(a, t) + ")";
    }
    return "NOT (" + Atom(a, t) + ")";
  }

  std::string MaybeWhere(const std::string& a, const OocGenTable& t, int pct) {
    return g_.Chance(pct) ? " WHERE " + Pred(a, t) : "";
  }

  // "k BETWEEN lo AND hi" keeping roughly `max_rows` rows of `t` ("" when the
  // whole table already fits).
  std::string RangeFor(const std::string& a, const OocGenTable& t,
                       int64_t max_rows, int64_t* est) {
    max_rows = std::max<int64_t>(1, max_rows);
    if (t.rows <= max_rows) {
      *est = t.rows;
      return "";
    }
    const int64_t span = std::max<int64_t>(1, t.key_hi - t.key_lo + 1);
    const double density =
        static_cast<double>(t.rows) / static_cast<double>(span);
    const auto width = std::max<int64_t>(
        1, static_cast<int64_t>(static_cast<double>(max_rows) /
                                std::max(density, 0.001)));
    const int64_t lo = g_.Pick(t.key_lo, std::max(t.key_lo, t.key_hi - width));
    *est = max_rows;
    return C(a, "k") + " BETWEEN " + I(lo) + " AND " + I(lo + width - 1);
  }

  // FROM item for `t` under alias `a`, limited to about `max_rows` rows.
  std::string Src(const OocGenTable& t, const std::string& a, int64_t max_rows,
                  int64_t* est) {
    const std::string range = RangeFor("", t, max_rows, est);
    if (range.empty()) {
      if (g_.Chance(12)) {
        *est = std::max<int64_t>(1, t.rows / 2);
        return "(SELECT * FROM " + t.name + " WHERE " + Pred("", t) + ") " + a;
      }
      return t.name + " " + a;
    }
    return "(SELECT * FROM " + t.name + " WHERE " + range + ") " + a;
  }

  // ---- result assembly ----------------------------------------------------

  static std::string Digest(const Inner& in) {
    std::string cat = "CONCAT(";
    for (size_t i = 0; i < in.cols.size(); ++i) {
      const std::string c = "c" + std::to_string(i + 1);
      if (i > 0) {
        cat += ", '|', ";
      }
      cat += in.cols[i].is_string ? "IFNULL(" + c + ", '~')"
                                  : "IFNULL(CAST(" + c + " AS STRING), '~')";
    }
    cat += ", '|')";
    return "SELECT COUNT(*), SUM(MOD(FARM_FINGERPRINT(s), 1000003)), "
           "BIT_XOR(FARM_FINGERPRINT(s)) FROM (SELECT " +
           cat + " AS s FROM (" + in.sql + "))";
  }

  OocGenQuery Finish(const std::string& shape, const Inner& in) {
    OocGenQuery q;
    q.shape = shape;
    q.heavy = in.heavy;
    const bool wide =
        std::ranges::any_of(in.cols, [](const OutCol& c) { return c.wide; });
    q.heavy = q.heavy || wide;
    // Wrapping the query in a derived table sends it down the legacy
    // executor (EXPLAIN ANALYZE shows one RelationalPlan stage), so the
    // digest form is the exception: raw rows keep the statement on the
    // pipeline sinks this fuzzer is after.
    if (g_.Chance(10)) {
      q.sql = Digest(in);
      q.shape += "_digest";
    } else {
      q.sql = in.sql;
    }
    return q;
  }

  // ---- column pools -------------------------------------------------------

  // True when `rows` copies of the widest pad of `t` are cheap to return.
  static bool WideAffordable(const OocGenTable& t, int64_t rows) {
    return rows * static_cast<int64_t>(t.pad_max) <= (int64_t{4} << 20);
  }

  OutCol PlainCol(const std::string& a, const OocGenTable& t, bool allow_wide) {
    switch (g_.Pick(0, allow_wide ? 8 : 7)) {
      case 0:
        return {C(a, "v")};
      case 1:
        return {C(a, "t"), true};
      case 2:
        return {C(a, "g")};
      case 3:
        return {"LENGTH(" + C(a, "pad") + ")"};
      case 4:
        return {"SUBSTR(" + C(a, "pad") + ", 1, " + I(g_.Pick(1, 16)) + ")",
                true};
      case 5:
        return {"IFNULL(" + C(a, "v") + ", -1) + " + C(a, "g")};
      case 6:
        return {"MOD(" + C(a, "k") + ", " + I(g_.Pick(2, 50)) + ")"};
      case 7:
        return {"CONCAT(IFNULL(" + C(a, "t") + ", '-'), CAST(" + C(a, "g") +
                    " AS STRING))",
                true};
      default:
        return {C(a, "pad"), true, t.pad_max > 256};
    }
  }

  std::string SortKey(const std::string& a) {
    std::string e;
    bool nullable = false;
    switch (g_.Pick(0, 8)) {
      case 0:
        e = C(a, "v");
        nullable = true;
        break;
      case 1:
        e = C(a, "t");
        nullable = true;
        break;
      case 2:
        e = C(a, "g");
        break;
      case 3:
        e = "LENGTH(" + C(a, "pad") + ")";
        break;
      case 4:
        e = C(a, "pad");
        break;
      case 5:
        e = "SUBSTR(" + C(a, "pad") + ", 1, " + I(g_.Pick(1, 6)) + ")";
        break;
      case 6:
        e = "MOD(" + C(a, "k") + ", " + I(g_.Pick(2, 13)) + ")";
        break;
      case 7:
        e = "IFNULL(" + C(a, "v") + ", 0) + " + C(a, "g");
        break;
      default:
        e = C(a, "v") + " * -1";
        nullable = true;
        break;
    }
    if (g_.Chance(45)) {
      e += " DESC";
    }
    if (nullable && g_.Chance(40)) {
      e += g_.Chance(50) ? " NULLS FIRST" : " NULLS LAST";
    }
    return e;
  }

  // 0..n random keys followed by k (unique), so the order is total.
  std::string TotalOrder(const std::string& a, int64_t extra_keys) {
    std::string out;
    for (int64_t i = 0; i < extra_keys; ++i) {
      out += SortKey(a) + ", ";
    }
    out += C(a, "k");
    if (g_.Chance(30)) {
      out += " DESC";
    }
    return out;
  }

  // ---- sort / top-N -------------------------------------------------------

  OocGenQuery Sort() {
    const OocGenTable& t = T();
    const bool wide_table = t.pad_max > 2000;
    std::vector<OutCol> cols = {{"k"}};
    const int64_t extra = g_.Pick(1, 3);
    const bool bare = g_.Chance(50);
    for (int64_t i = 0; i < extra; ++i) {
      if (bare) {
        static const std::vector<OutCol> kBare = {{"v"}, {"t", true}, {"g"}};
        cols.push_back(g_.OneOf(kBare));
      } else {
        cols.push_back(PlainCol("", t, /*allow_wide=*/!wide_table));
      }
    }
    if (bare && !wide_table && g_.Chance(30)) {
      cols.push_back({"pad", true, t.pad_max > 256});
    }
    OocGenQuery q;
    q.shape = "sort";
    q.ordered = true;
    int64_t limit = g_.Pick(1, wide_table ? 20 : 300);
    int64_t offset = 0;
    const int64_t roll = g_.Pick(0, 99);
    if (roll < 25) {
      offset = g_.Pick(0, 60);
    } else if (roll < 45) {
      // Deep OFFSET: LIMIT + OFFSET far beyond a tiny operator budget.
      offset = g_.Pick(0, std::max<int64_t>(1, t.rows));
    } else if (roll < 55 && !wide_table) {
      limit = std::min<int64_t>(std::max<int64_t>(1, t.rows), 20000);
      q.shape = "sort_all";
    }
    q.heavy = std::ranges::any_of(cols, [](const OutCol& c) { return c.wide; });
    q.sql = "SELECT " + SelectList(cols) + " FROM " + t.name +
            MaybeWhere("", t, 35) + " ORDER BY " +
            TotalOrder("", g_.Pick(0, 2)) + " LIMIT " + I(limit);
    if (offset > 0) {
      q.sql += " OFFSET " + I(offset);
    }
    return q;
  }

  // Whole-table sort without LIMIT: either the sorted rows themselves or a
  // single-partition window (the G4 skew case) numbering them.
  OocGenQuery FullSort() {
    const OocGenTable& t = T();
    OocGenQuery q;
    if (g_.Chance(50)) {
      q.shape = "full_sort";
      q.ordered = true;
      q.sql = "SELECT k, v, t FROM " + t.name + MaybeWhere("", t, 25) +
              " ORDER BY " + TotalOrder("", g_.Pick(0, 2));
      return q;
    }
    q.shape = "full_sort_window";
    q.sql = "SELECT k, ROW_NUMBER() OVER (ORDER BY " +
            TotalOrder("", g_.Pick(1, 2)) + ") AS rn, RANK() OVER (ORDER BY " +
            SortKey("") + ") AS rk FROM " + t.name + MaybeWhere("", t, 25);
    return q;
  }

  // ---- aggregation --------------------------------------------------------

  struct KeySet {
    std::vector<OutCol> cols;
    int64_t est{1};
  };

  KeySet GroupKeys(const std::string& a, const OocGenTable& t) {
    switch (g_.Pick(0, 11)) {
      case 0:
        return {{{C(a, "g")}}, t.groups};
      case 1:
        return {{{C(a, "t"), true}}, 5};
      case 2:
        return {{{C(a, "g")}, {C(a, "t"), true}}, t.groups * 5};
      case 3: {
        const int64_t m = g_.Pick(2, 400);
        return {{{"MOD(" + C(a, "k") + ", " + I(m) + ")"}}, m};
      }
      case 4: {
        const int64_t n = g_.Pick(1, 4);
        return {{{"SUBSTR(" + C(a, "pad") + ", 1, " + I(n) + ")", true}},
                std::min<int64_t>(t.rows, 300 * n)};
      }
      case 5:
        return {{{C(a, "k")}}, t.rows};
      case 6:
        return {{{C(a, "pad"), true, t.pad_max > 256}}, t.rows};
      case 7:
        return {{{"LENGTH(" + C(a, "pad") + ")"}},
                std::min<int64_t>(t.rows, 3000)};
      case 8:
        return {{{C(a, "v")}}, std::min<int64_t>(t.rows, t.v_mod + 1)};
      case 9:
        return {{{C(a, "g")}, {"MOD(" + C(a, "v") + ", 3)"}}, t.groups * 4};
      case 10:
        return {{{C(a, "v")}, {C(a, "t"), true}},
                std::min<int64_t>(t.rows, (t.v_mod + 1) * 5)};
      default:
        return {{}, 1};
    }
  }

  OutCol Agg(const std::string& a, const OocGenTable& t, bool* heavy) {
    const std::string k = C(a, "k");
    const std::string v = C(a, "v");
    const std::string tg = C(a, "t");
    const std::string pad = C(a, "pad");
    switch (g_.Pick(0, 22)) {
      case 0:
        return {"COUNT(*)"};
      case 1:
        return {"COUNT(" + v + ")"};
      case 2:
        return {"SUM(" + v + ")"};
      case 3:
        return {"MIN(" + k + ")"};
      case 4:
        return {"MAX(" + k + ")"};
      case 5:
        return {"MIN(" + tg + ")", true};
      case 6:
        return {"MAX(" + tg + ")", true};
      case 7:
        return {"AVG(" + v + ")"};
      case 8:
        return {"MIN(" + pad + ")", true, t.pad_max > 256};
      case 9:
        return {"MAX(" + pad + ")", true, t.pad_max > 256};
      case 10:
        return {"COUNT(DISTINCT " + v + ")"};
      case 11:
        return {"COUNT(DISTINCT " + tg + ")"};
      case 12:
        *heavy = true;
        return {"COUNT(DISTINCT " + pad + ")"};
      case 13:
        return {"SUM(DISTINCT " + v + ")"};
      case 14:
        *heavy = true;
        return {"LENGTH(STRING_AGG(" + tg + ", ',' ORDER BY " + k + "))"};
      case 15:
        *heavy = true;
        return {"SUBSTR(STRING_AGG(SUBSTR(" + pad + ", 1, 6), '' ORDER BY " +
                    k + " DESC), 1, 40)",
                true};
      case 16:
        *heavy = true;
        return {"LENGTH(ARRAY_TO_STRING(ARRAY_AGG(CAST(" + k +
                " AS STRING) ORDER BY " + k + "), ','))"};
      case 17:
        return {"COUNTIF(" + v + " > " + I(g_.Pick(0, t.v_mod)) + ")"};
      case 18:
        return {"BIT_XOR(" + k + ")"};
      case 19:
        return {"MAX(LENGTH(" + pad + "))"};
      case 20:
        return {"SUM(LENGTH(" + pad + "))"};
      case 21:
        return {"LOGICAL_OR(" + v + " > " + I(g_.Pick(0, t.v_mod)) + ")"};
      default:
        return {"SUM(" + k + " * " + C(a, "g") + ")"};
    }
  }

  OocGenQuery GroupAgg() {
    const OocGenTable& t = T();
    Inner in;
    std::string from;
    std::string a;
    if (g_.Chance(50)) {
      // Aggregate straight over a join pipeline (the single-table form is
      // served by the legacy grouped executor, not the OOC GroupAgg sink).
      const OocGenTable& u = T();
      a = "a";
      from = t.name + " a JOIN " + u.name + " b ON a.k = b.k";
      if (g_.Chance(40)) {
        from += " WHERE " + Pred("a", t);
      }
    } else {
      from = t.name + MaybeWhere("", t, 35);
    }
    KeySet keys = GroupKeys(a, t);
    in.cols = keys.cols;
    const int64_t aggs = g_.Pick(1, 4);
    for (int64_t i = 0; i < aggs; ++i) {
      in.cols.push_back(Agg(a, t, &in.heavy));
    }
    if (!a.empty() && g_.Chance(60)) {
      in.cols.push_back(g_.Chance(50) ? OutCol{"SUM(b.v)"}
                                      : OutCol{"MAX(LENGTH(b.pad))"});
    }
    in.sql = "SELECT " + SelectList(in.cols) + " FROM " + from;
    if (!keys.cols.empty()) {
      in.sql += " GROUP BY ";
      for (size_t i = 0; i < keys.cols.size(); ++i) {
        in.sql += (i > 0 ? ", " : "") + std::to_string(i + 1);
      }
      if (g_.Chance(15)) {
        in.sql += " HAVING COUNT(*) > " + I(g_.Pick(0, 3));
      }
    }
    in.est_rows = keys.est;
    const bool wide =
        std::ranges::any_of(in.cols, [](const OutCol& c) { return c.wide; });
    if (!keys.cols.empty() && keys.est <= 600 && !wide && g_.Chance(60)) {
      // The group keys identify a row: ordering by all of them is total.
      OocGenQuery q;
      q.shape = "group_agg_ordered";
      q.ordered = true;
      q.heavy = in.heavy;
      q.sql = in.sql + " ORDER BY ";
      for (size_t i = 0; i < keys.cols.size(); ++i) {
        q.sql += (i > 0 ? ", " : "") + std::to_string(i + 1);
      }
      return q;
    }
    return Finish(keys.cols.empty() ? "global_agg" : "group_agg", in);
  }

  OocGenQuery Distinct() {
    const OocGenTable& t = T();
    Inner in;
    const int64_t n = g_.Pick(1, 2);
    for (int64_t i = 0; i < n; ++i) {
      in.cols.push_back(
          PlainCol("", t, /*allow_wide=*/WideAffordable(t, t.rows)));
    }
    in.sql = "SELECT DISTINCT " + SelectList(in.cols) + " FROM " + t.name +
             MaybeWhere("", t, 30);
    in.est_rows = t.rows;
    return Finish("distinct", in);
  }

  // ---- joins --------------------------------------------------------------

  std::vector<OutCol> JoinCols(const OocGenTable& ta, const OocGenTable& tb) {
    std::vector<OutCol> cols = {{"a.k"}, {"b.k"}};
    const int64_t extra = g_.Pick(1, 3);
    for (int64_t i = 0; i < extra; ++i) {
      const bool left = g_.Chance(50);
      // Carrying the wide pad through the join is the interesting case for
      // spill, but keep it rare so digests stay cheap.
      const OocGenTable& side = left ? ta : tb;
      cols.push_back(PlainCol(left ? "a" : "b", side,
                              g_.Chance(25) && WideAffordable(side, cap_)));
    }
    return cols;
  }

  // Rows per side of a join the engine may run as a nested loop (no plain
  // column equi-key): the pair count, not the output, is what costs time, and
  // an unbounded N x N under a 16 KiB pool looks like a hang without being
  // one.  ~1.5M pairs per scale unit keeps such a query within seconds.
  [[nodiscard]] int64_t NestedLoopSide() const {
    return std::max<int64_t>(
        64, static_cast<int64_t>(std::sqrt(static_cast<double>(cap_) * 25.0)));
  }

  static int64_t SideCap(int64_t cap, int64_t distinct) {
    // ra * rb / distinct <= cap with ra == rb.
    const double v =
        std::sqrt(static_cast<double>(cap) *
                  static_cast<double>(std::max<int64_t>(1, distinct)));
    return std::max<int64_t>(8, static_cast<int64_t>(v));
  }

  OocGenQuery Join() {
    const OocGenTable& ta = T();
    const OocGenTable& tb = T();
    int64_t cap_a = ta.rows + 1;
    int64_t cap_b = tb.rows + 1;
    std::string on;
    std::string shape = "join_";
    switch (g_.Pick(0, 8)) {
      case 0:
      case 1:
        on = "a.k = b.k";
        shape += "kk";
        break;
      case 2:
        on = "a.g = b.g";
        cap_a = cap_b = SideCap(cap_, std::min(ta.groups, tb.groups));
        shape += "gg";
        break;
      case 3:
        on = "a.v = b.v";
        cap_a = cap_b = SideCap(cap_, std::min(ta.v_mod, tb.v_mod));
        shape += "vv";
        break;
      case 4:
        on = "a.t = b.t";
        cap_a = cap_b = SideCap(cap_, 4);
        shape += "tt";
        break;
      case 5:
        on = "a.pad = b.pad";
        shape += "pad";
        break;
      case 6:
        on = "a.k = b.k + " + I(g_.Pick(-50, 50));
        cap_a = cap_b = NestedLoopSide();
        shape += "kshift";
        break;
      case 7:
        on = "a.k = b.k AND a.g = b.g";
        shape += "kg";
        break;
      default:
        on = "a.k = b.v";
        shape += "kv";
        break;
    }
    if (g_.Chance(25)) {
      static const std::vector<std::string> kResidual = {
          " AND a.v < b.v", " AND b.t = 'a'", " AND a.g <> b.g",
          " AND LENGTH(a.pad) >= LENGTH(b.pad)", " AND a.v IS NOT NULL"};
      on += g_.OneOf(kResidual);
      shape += "_res";
    }
    static const std::vector<std::string> kTypes = {
        "JOIN",      "JOIN",       "JOIN",      "LEFT JOIN",
        "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "FULL JOIN"};
    const std::string jt = g_.OneOf(kTypes);
    int64_t ea = 0;
    int64_t eb = 0;
    Inner in;
    in.cols = JoinCols(ta, tb);
    in.sql = "SELECT " + SelectList(in.cols) + " FROM " +
             Src(ta, "a", cap_a, &ea) + " " + jt + " " +
             Src(tb, "b", cap_b, &eb) + " ON " + on;
    if (jt == "LEFT JOIN" && g_.Chance(20)) {
      in.sql += " WHERE b.k IS NULL";  // anti-join spelled as outer join
    } else if (g_.Chance(25)) {
      in.sql += " WHERE " + Pred("a", ta);
    }
    in.est_rows = cap_;
    if (jt != "JOIN") {
      shape += jt == "LEFT JOIN" ? "_left"
                                 : (jt == "RIGHT JOIN" ? "_right" : "_full");
    }
    return Finish(shape, in);
  }

  OocGenQuery Join3() {
    const OocGenTable& ta = T();
    const OocGenTable& tb = T();
    const OocGenTable& tc = T();
    const int64_t mid = std::max<int64_t>(1, std::min(ta.rows, tb.rows));
    const int64_t cap_c =
        std::max<int64_t>(4, cap_ * std::max<int64_t>(1, tc.groups) / mid);
    int64_t ec = 0;
    int64_t ea = 0;
    Inner in;
    in.cols = {{"a.k"}, {"b.v"}, {"c.k"}, {"c.t", true}};
    in.sql = "SELECT " + SelectList(in.cols) + " FROM " +
             Src(ta, "a", ta.rows + 1, &ea) + (g_.Chance(30) ? " LEFT" : "") +
             " JOIN " + tb.name + " b ON a.k = b.k JOIN " +
             Src(tc, "c", cap_c, &ec) + " ON b.g = c.g";
    if (g_.Chance(30)) {
      in.sql += " WHERE " + Pred("a", ta);
    }
    in.est_rows = cap_;
    return Finish("join3", in);
  }

  OocGenQuery SemiAnti() {
    const OocGenTable& ta = T();
    const OocGenTable& tb = T();
    std::string cond;
    std::string shape;
    const std::string bw = g_.Chance(40) ? " WHERE " + Pred("b", tb) : "";
    switch (g_.Pick(0, 9)) {
      case 0:
        cond = "a.k IN (SELECT b.k FROM " + tb.name + " b" + bw + ")";
        shape = "semi_in";
        break;
      case 1:
        cond = "a.k NOT IN (SELECT b.k FROM " + tb.name + " b" + bw + ")";
        shape = "anti_not_in";
        break;
      case 2:
        cond = "a.v IN (SELECT b.v FROM " + tb.name + " b" + bw + ")";
        shape = "semi_in_nullable";
        break;
      case 3:
        // NULL-aware anti join: one NULL on the build side empties the result.
        cond = "a.v NOT IN (SELECT b.v FROM " + tb.name + " b" + bw + ")";
        shape = "anti_not_in_nullable";
        break;
      case 4: {
        int64_t unused = 0;
        const std::string range =
            RangeFor("b", tb, SideCap(cap_, tb.groups), &unused);
        cond = "EXISTS (SELECT 1 FROM " + tb.name +
               " b WHERE b.g = a.g AND b.k > a.k" +
               (range.empty() ? "" : " AND " + range) + ")";
        shape = "semi_exists_corr";
        break;
      }
      case 5:
        cond = "NOT EXISTS (SELECT 1 FROM " + tb.name + " b WHERE b.k = a.k" +
               (g_.Chance(50) ? " AND b.v > " + I(g_.Pick(0, tb.v_mod)) : "") +
               ")";
        shape = "anti_not_exists";
        break;
      case 6:
        cond = "EXISTS (SELECT 1 FROM " + tb.name + " b WHERE b.v = a.v)";
        shape = "semi_exists_v";
        break;
      case 7:
        cond = "NOT EXISTS (SELECT 1 FROM " + tb.name +
               " b WHERE b.g = a.g AND b.t = a.t)";
        shape = "anti_not_exists_gt";
        break;
      case 8:
        cond =
            "(a.g, a.t) IN (SELECT b.g, b.t FROM " + tb.name + " b" + bw + ")";
        shape = "semi_tuple_in";
        break;
      default:
        cond = "a.pad " + std::string(g_.Chance(50) ? "IN" : "NOT IN") +
               " (SELECT b.pad FROM " + tb.name + " b" + bw + ")";
        shape = "semi_pad";
        break;
    }
    Inner in;
    in.cols = {{"a.k"}, {"a.v"}, {"a.t", true}};
    in.sql = "SELECT " + SelectList(in.cols) + " FROM " + ta.name +
             " a WHERE " + cond;
    if (g_.Chance(30)) {
      in.sql += " AND " + Pred("a", ta);
    }
    in.est_rows = ta.rows;
    in.heavy = shape == "semi_pad";
    return Finish(shape, in);
  }

  // ---- windows ------------------------------------------------------------

  std::string Partition() {
    switch (g_.Pick(0, 5)) {
      case 0:
        return "";  // one partition: the skew case of ooca-v2.md G4
      case 1:
      case 2:
        return "PARTITION BY g ";
      case 3:
        return "PARTITION BY t ";
      case 4:
        return "PARTITION BY MOD(k, " + I(g_.Pick(2, 200)) + ") ";
      default:
        return "PARTITION BY g, t ";
    }
  }

  std::string RowsFrame() {
    auto bound = [&](bool preceding) {
      const int64_t roll = g_.Pick(0, 9);
      const std::string dir = preceding ? " PRECEDING" : " FOLLOWING";
      if (roll < 5) {
        return I(g_.Pick(0, 5)) + dir;
      }
      if (roll < 7) {
        return I(g_.Pick(20, 300)) + dir;
      }
      if (roll < 8) {
        return I(g_.Pick(3000, 100000)) + dir;  // wider than any partition
      }
      return "UNBOUNDED" + dir;
    };
    const int64_t roll = g_.Pick(0, 9);
    if (roll < 6) {
      return " ROWS BETWEEN " + bound(true) + " AND " + bound(false);
    }
    if (roll < 8) {
      return " ROWS BETWEEN " + bound(true) + " AND CURRENT ROW";
    }
    return " ROWS BETWEEN CURRENT ROW AND " + bound(false);
  }

  OutCol WindowFn(const OocGenTable& t, bool* heavy) {
    const std::string part = Partition();
    const std::string total = "ORDER BY " + TotalOrder("", g_.Pick(0, 1));
    static const std::vector<std::string> kAggs = {
        "SUM(v)", "COUNT(*)", "COUNT(v)", "MIN(v)",
        "MAX(k)", "AVG(v)",   "MAX(t)",   "SUM(LENGTH(pad))"};
    switch (g_.Pick(0, 17)) {
      case 0:
        return {"ROW_NUMBER() OVER (" + part + total + ")"};
      case 1:
        return {"RANK() OVER (" + part + "ORDER BY " + SortKey("") + ")"};
      case 2:
        return {"DENSE_RANK() OVER (" + part + "ORDER BY " + SortKey("") + ")"};
      case 3:
      case 4:
      case 5: {
        const std::string agg = g_.OneOf(kAggs);
        return {agg + " OVER (" + part + total + RowsFrame() + ")",
                agg == "MAX(t)"};
      }
      case 6: {
        // RANGE frames are peer-based: a non-unique numeric key is fine.
        const std::string key = g_.Chance(50) ? "k" : "v";
        return {g_.OneOf(kAggs) + " OVER (" + part + "ORDER BY " + key +
                    " RANGE BETWEEN " + I(g_.Pick(0, 40)) + " PRECEDING AND " +
                    I(g_.Pick(0, 40)) + " FOLLOWING)",
                false};
      }
      case 7:
        // Default frame (RANGE UNBOUNDED PRECEDING .. CURRENT ROW peers).
        return {"SUM(v) OVER (" + part + "ORDER BY " + SortKey("") + ")"};
      case 8:
        return {"COUNT(*) OVER (" + part + ")"};
      case 9:
        return {"LAG(v, " + I(g_.Pick(1, 40)) + ") OVER (" + part + total +
                ")"};
      case 10:
        return {"LEAD(k, " + I(g_.Pick(1, 400)) + ") OVER (" + part + total +
                ")"};
      case 11:
        return {"FIRST_VALUE(v) OVER (" + part + total + ")"};
      case 12:
        return {"LAST_VALUE(k) OVER (" + part + total + RowsFrame() + ")"};
      case 13:
        return {"NTH_VALUE(k, " + I(g_.Pick(1, 30)) + ") OVER (" + part +
                total + RowsFrame() + ")"};
      case 14:
        return {"NTILE(" + I(g_.Pick(1, 50)) + ") OVER (" + part + total + ")"};
      case 15:
        return {(g_.Chance(50) ? "PERCENT_RANK()" : "CUME_DIST()") +
                std::string(" OVER (") + part + "ORDER BY " + SortKey("") +
                ")"};
      case 16:
        *heavy = *heavy || t.pad_max > 256;
        return {"LENGTH(LAG(pad, " + I(g_.Pick(1, 9)) + ") OVER (" + part +
                total + "))"};
      default:
        *heavy = *heavy || t.pad_max > 256;
        return {"LENGTH(MAX(pad) OVER (" + part + total + RowsFrame() + "))"};
    }
  }

  OocGenQuery Window() {
    const OocGenTable& t = T();
    Inner in;
    if (g_.Chance(15)) {
      // Top-N per group through QUALIFY.
      in.cols = {{"k"}, {"v"}, {"g"}};
      in.sql = "SELECT " + SelectList(in.cols) + " FROM " + t.name +
               " WHERE k >= " + I(t.key_lo) + " QUALIFY ROW_NUMBER() OVER (" +
               Partition() + "ORDER BY " + TotalOrder("", 1) +
               ") <= " + I(g_.Pick(1, 5));
      in.est_rows = t.rows;
      return Finish("window_qualify", in);
    }
    in.cols = {{"k"}};
    const int64_t fns = g_.Pick(1, 3);
    for (int64_t i = 0; i < fns; ++i) {
      in.cols.push_back(WindowFn(t, &in.heavy));
    }
    in.sql = "SELECT " + SelectList(in.cols) + " FROM " + t.name +
             MaybeWhere("", t, 25);
    in.est_rows = t.rows;
    return Finish("window", in);
  }

  // ---- set operations -----------------------------------------------------

  OocGenQuery SetOp() {
    struct Proj {
      std::vector<OutCol> cols;
      bool wide;
    };
    const std::vector<Proj> projs = {
        {{{"g"}, {"t", true}}, false},
        {{{"k"}}, false},
        {{{"v"}}, false},
        {{{"pad", true, true}}, true},
        {{{"SUBSTR(pad, 1, 3)", true}}, false},
        {{{"g"}}, false},
        {{{"t", true}, {"MOD(v, 5)"}}, false},
        {{{"k"}, {"v"}}, false},
        {{{"k"}, {"pad", true, true}}, true},
    };
    static const std::vector<std::string> kOps = {
        "UNION ALL",          "UNION DISTINCT", "INTERSECT ALL",
        "INTERSECT DISTINCT", "EXCEPT ALL",     "EXCEPT DISTINCT"};
    const Proj& proj = g_.OneOf(projs);
    const std::string op = g_.OneOf(kOps);
    const int64_t inputs = g_.Pick(2, 3);
    Inner in;
    in.cols = proj.cols;
    in.est_rows = 0;
    for (int64_t i = 0; i < inputs; ++i) {
      const OocGenTable& t = T();
      if (i > 0) {
        in.sql += " " + op + " ";
      }
      in.sql += "SELECT " + SelectList(proj.cols) + " FROM " + t.name +
                MaybeWhere("", t, 35);
      in.est_rows += t.rows;
    }
    std::string shape = "setop_" + op;
    std::ranges::replace(shape, ' ', '_');
    return Finish(shape, in);
  }

  // ---- recursive CTE ------------------------------------------------------

  OocGenQuery Recursive() {
    const OocGenTable& t = T();
    OocGenQuery q;
    const int64_t roll = g_.Pick(0, 9);
    if (roll < 3) {
      const int64_t n = g_.Pick(5, 400);
      q.shape = "recursive_numbers";
      // Joining r against a base table in the outer query is rejected by
      // the binder today ("column a.k not found"); see the findings report.
      q.sql = "WITH RECURSIVE r AS (SELECT " + I(t.key_lo) +
              " AS n UNION ALL SELECT n + 1 FROM r WHERE n < " +
              I(t.key_lo + n) + ") SELECT COUNT(*), SUM(n), MAX(n) FROM r";
      return q;
    }
    const int64_t step = g_.Pick(1, 7);
    const int64_t seed_lo = Key(t);
    const int64_t seed_hi = seed_lo + g_.Pick(0, step * 3);
    const int64_t iterations = g_.Pick(3, 120);
    const int64_t limit = seed_hi + step * iterations;
    if (roll < 7) {
      const bool carry_pad = g_.Chance(40);
      q.shape = carry_pad ? "recursive_distinct_wide" : "recursive_distinct";
      q.heavy = carry_pad && t.pad_max > 256;
      const std::string cols = carry_pad ? "k, pad" : "k, g";
      const std::string acols = carry_pad ? "a.k, a.pad" : "a.k, a.g";
      q.sql = "WITH RECURSIVE r AS (SELECT " + cols + " FROM " + t.name +
              " WHERE k BETWEEN " + I(seed_lo) + " AND " + I(seed_hi) +
              " UNION DISTINCT SELECT " + acols + " FROM " + t.name +
              " a JOIN r ON a.k = r.k + " + I(step) +
              " WHERE a.k <= " + I(limit) + ") SELECT COUNT(*), SUM(k), " +
              (carry_pad ? "SUM(LENGTH(pad))" : "SUM(g)") + " FROM r";
      return q;
    }
    q.shape = "recursive_all_depth";
    q.sql = "WITH RECURSIVE r AS (SELECT k, 0 AS d FROM " + t.name +
            " WHERE k BETWEEN " + I(seed_lo) + " AND " + I(seed_hi) +
            " UNION ALL SELECT a.k, r.d + 1 FROM " + t.name +
            " a JOIN r ON a.k = r.k + " + I(step) + " WHERE r.d < " +
            I(iterations) + ") SELECT COUNT(*), SUM(k), SUM(d), MAX(d) FROM r";
    return q;
  }

  // ---- correlated subqueries (Apply) --------------------------------------

  OocGenQuery Apply() {
    const OocGenTable& ta = T();
    const OocGenTable& tb = T();
    int64_t unused = 0;
    const std::string brange = RangeFor("b", tb, 2500, &unused);
    const std::string band = brange.empty() ? "" : " AND " + brange;
    const int64_t lo = Key(ta);
    const std::string arange =
        "a.k BETWEEN " + I(lo) + " AND " + I(lo + g_.Pick(0, 150));
    Inner in;
    std::string shape = "apply_";
    switch (g_.Pick(0, 5)) {
      case 0:
        in.cols = {{"a.k"},
                   {"(SELECT COUNT(*) FROM " + tb.name +
                    " b WHERE b.g = a.g AND b.k < a.k + " + I(g_.Pick(0, 200)) +
                    band + ")"}};
        shape += "count";
        break;
      case 1:
        in.cols = {
            {"a.k"},
            {"(SELECT MAX(b.k) FROM " + tb.name + " b WHERE b.k < a.k + " +
             I(g_.Pick(-50, 200)) + band + ")"}};
        shape += "max";
        break;
      case 2:
        in.cols = {{"a.k"},
                   {"(SELECT SUM(b.v) FROM " + tb.name + " b WHERE b.g = a.g" +
                    band + ")"}};
        shape += "sum";
        break;
      case 3:
        in.cols = {{"a.k"},
                   {"(SELECT COUNT(*) FROM " + tb.name + " b WHERE b.v = a.v" +
                    band + ")"}};
        shape += "count_v";
        break;
      case 4:
        in.cols = {{"a.k"},
                   {"(SELECT MIN(LENGTH(b.pad)) FROM " + tb.name +
                    " b WHERE b.k > a.k" + band + ")"}};
        shape += "min_len";
        break;
      default: {
        in.cols = {{"a.k"}, {"a.v"}};
        in.sql = "SELECT " + SelectList(in.cols) + " FROM " + ta.name +
                 " a WHERE " + arange + " AND a.v > (SELECT AVG(b.v) FROM " +
                 tb.name + " b WHERE b.g = a.g" + band + ")";
        in.est_rows = 151;
        return Finish("apply_filter", in);
      }
    }
    in.sql = "SELECT " + SelectList(in.cols) + " FROM " + ta.name +
             " a WHERE " + arange;
    in.est_rows = 151;
    return Finish(shape, in);
  }

  // ---- cross / band joins -------------------------------------------------

  OocGenQuery Cross() {
    const OocGenTable& ta = T();
    const OocGenTable& tb = T();
    const int64_t product = std::min<int64_t>(cap_, 120000);
    const int64_t ra = g_.Pick(1, 300);
    int64_t ea = 0;
    int64_t eb = 0;
    const std::string sa = Src(ta, "a", ra, &ea);
    const std::string sb = Src(
        tb, "b", std::max<int64_t>(1, product / std::max<int64_t>(1, ea)), &eb);
    static const std::vector<std::string> kFilters = {
        "",
        "",
        " WHERE a.v < b.v",
        " WHERE a.g <> b.g",
        " WHERE MOD(a.k + b.k, 7) = 0",
        " WHERE a.t = b.t OR a.v IS NULL"};
    OocGenQuery q;
    if (g_.Chance(50)) {
      q.shape = "cross_agg";
      q.sql =
          "SELECT COUNT(*), SUM(a.k * b.k), SUM(LENGTH(a.pad) + "
          "LENGTH(b.pad)), COUNT(a.v + b.v) FROM " +
          sa + " CROSS JOIN " + sb + g_.OneOf(kFilters);
      return q;
    }
    Inner in;
    in.cols = {{"a.k"}, {"b.k"}, {"LENGTH(b.pad)"}, {"a.t", true}};
    in.sql = "SELECT " + SelectList(in.cols) + " FROM " + sa + " CROSS JOIN " +
             sb + g_.OneOf(kFilters);
    in.est_rows = product;
    return Finish("cross_rows", in);
  }

  OocGenQuery Band() {
    const OocGenTable& ta = T();
    const OocGenTable& tb = T();
    int64_t cap_a = ta.rows + 1;
    int64_t cap_b = tb.rows + 1;
    std::string on;
    std::string shape;
    switch (g_.Pick(0, 3)) {
      case 0:
        on = "b.k BETWEEN a.k - " + I(g_.Pick(0, 5)) + " AND a.k + " +
             I(g_.Pick(0, 5));
        cap_a = cap_b = NestedLoopSide();
        shape = "band_k";
        break;
      case 1: {
        const int64_t w = g_.Pick(0, 4);
        on = "a.v BETWEEN b.v - " + I(w) + " AND b.v + " + I(w);
        cap_a = cap_b = std::min(
            NestedLoopSide(),
            SideCap(cap_, std::max<int64_t>(
                              1, std::min(ta.v_mod, tb.v_mod) / (2 * w + 1))));
        shape = "band_v";
        break;
      }
      case 2:
        on = "a.k < b.k AND a.g = b.g";
        cap_a = cap_b = SideCap(cap_, std::min(ta.groups, tb.groups));
        shape = "band_lt_eq";
        break;
      default:
        on = "a.k <= b.k AND b.k < a.k + " + I(g_.Pick(1, 8));
        cap_a = cap_b = NestedLoopSide();
        shape = "band_half_open";
        break;
    }
    int64_t ea = 0;
    int64_t eb = 0;
    Inner in;
    in.cols = {{"a.k"}, {"b.k"}, {"a.v"}, {"b.t", true}};
    in.sql = "SELECT " + SelectList(in.cols) + " FROM " +
             Src(ta, "a", cap_a, &ea) + (g_.Chance(25) ? " LEFT" : "") +
             " JOIN " + Src(tb, "b", cap_b, &eb) + " ON " + on;
    in.est_rows = cap_;
    return Finish(shape, in);
  }

  // ---- index-shaped access paths ------------------------------------------

  OocGenQuery IndexPath() {
    const OocGenTable& t = T();
    OocGenQuery q;
    Inner in;
    switch (g_.Pick(0, 9)) {
      case 0:
        in.cols = {{"k"}, {"v"}};
        in.sql = "SELECT " + SelectList(in.cols) + " FROM " + t.name +
                 " WHERE g = " + I(g_.Pick(0, t.groups - 1)) +
                 " AND t = " + Tag();
        in.est_rows = t.rows;
        return Finish("index_and", in);
      case 1: {
        const int64_t lo = Key(t);
        in.cols = {{"k"}, {"g"}, {"t", true}};
        in.sql = "SELECT " + SelectList(in.cols) + " FROM " + t.name +
                 " WHERE g = " + I(g_.Pick(0, t.groups - 1)) +
                 " OR k BETWEEN " + I(lo) + " AND " + I(lo + g_.Pick(0, 400)) +
                 " OR t = " + Tag();
        in.est_rows = t.rows;
        return Finish("index_or", in);
      }
      case 2:
        in.cols = {{"g"}};
        in.sql = "SELECT DISTINCT g AS c1 FROM " + t.name;
        in.est_rows = t.groups;
        return Finish("index_skip_distinct", in);
      case 3:
        q.shape = "index_minmax";
        q.sql =
            "SELECT MIN(k), MAX(k), MIN(g), MAX(g), MIN(t), MAX(t) FROM " +
            t.name +
            (g_.Chance(40) ? " WHERE g = " + I(g_.Pick(0, t.groups - 1)) : "");
        return q;
      case 4:
        q.shape = "index_order_gv";
        q.ordered = true;
        q.sql = "SELECT g, v, k FROM " + t.name +
                " WHERE g = " + I(g_.Pick(0, t.groups - 1)) +
                " ORDER BY v, k LIMIT " + I(g_.Pick(1, 200));
        return q;
      case 5:
        // Sorted prefix from an index on g + remaining key: incremental sort.
        q.shape = "incremental_sort";
        q.ordered = true;
        q.sql = "SELECT g, k, v FROM " + t.name + " ORDER BY g, k" +
                (g_.Chance(50) ? " DESC" : "") + " LIMIT " +
                I(g_.Pick(1, 300)) + " OFFSET " +
                I(g_.Pick(0, std::max<int64_t>(1, t.rows)));
        return q;
      case 6:
        in.cols = {{"k"}, {"LENGTH(pad)"}, {"v"}};
        in.sql = "SELECT " + SelectList(in.cols) + " FROM " + t.name +
                 " WHERE k = " + I(Key(t));
        in.est_rows = 1;
        return Finish("index_point", in);
      case 7: {
        q.shape = "index_in_list";
        q.ordered = true;
        std::string list;
        for (int i = 0; i < 8; ++i) {
          list += (i > 0 ? ", " : "") + I(Key(t));
        }
        q.sql = "SELECT k, v, t FROM " + t.name + " WHERE k IN (" + list +
                ") ORDER BY k";
        return q;
      }
      case 8:
        q.shape = "index_tag_range";
        q.ordered = true;
        q.sql = "SELECT t, k, v FROM " + t.name + " WHERE t = " + Tag() +
                " AND k > " + I(Key(t)) + " ORDER BY k LIMIT " +
                I(g_.Pick(1, 200));
        return q;
      default:
        q.shape = "index_pad_range";
        q.sql = "SELECT COUNT(*), SUM(k), MIN(pad) < MAX(pad) FROM " + t.name +
                " WHERE pad >= '" + I(g_.Pick(1, 9)) + "' AND pad < '" +
                I(g_.Pick(2, 9)) + "z'";
        return q;
    }
  }

  // ---- multi-stage pipelines ----------------------------------------------

  OocGenQuery Nested() {
    const OocGenTable& ta = T();
    const OocGenTable& tb = T();
    OocGenQuery q;
    Inner in;
    switch (g_.Pick(0, 8)) {
      case 0:
        q.shape = "nested_agg_join_agg";
        q.ordered = true;
        q.sql =
            "SELECT x.g, x.c, y.s, y.m FROM (SELECT g, COUNT(*) AS c FROM " +
            ta.name + " GROUP BY g) x " +
            (g_.Chance(40) ? "LEFT JOIN" : "JOIN") +
            " (SELECT g, SUM(v) AS s, MAX(k) AS m FROM " + tb.name +
            " GROUP BY g) y ON x.g = y.g ORDER BY x.g";
        return q;
      case 1:
        q.shape = "nested_window_over_agg";
        q.ordered = true;
        q.sql = "SELECT c1, c2, SUM(c2) OVER (ORDER BY c1 ROWS BETWEEN " +
                I(g_.Pick(0, 9)) +
                " PRECEDING AND CURRENT ROW), RANK() OVER (ORDER BY c2 DESC) "
                "FROM (SELECT MOD(k, " +
                I(g_.Pick(2, 300)) + ") AS c1, COUNT(*) AS c2 FROM " + ta.name +
                " GROUP BY 1) ORDER BY c1";
        return q;
      case 2: {
        int64_t ea = 0;
        int64_t eb = 0;
        const int64_t side = SideCap(cap_, std::min(ta.groups, tb.groups));
        q.shape = "nested_agg_over_join";
        q.ordered = true;
        q.sql =
            "SELECT a.t, COUNT(*), SUM(b.v), COUNT(DISTINCT b.k), "
            "MAX(LENGTH(b.pad)) FROM " +
            Src(ta, "a", side, &ea) + " JOIN " + Src(tb, "b", side, &eb) +
            " ON a.g = b.g GROUP BY a.t ORDER BY a.t";
        return q;
      }
      case 3: {
        int64_t ea = 0;
        int64_t eb = 0;
        const int64_t side = SideCap(cap_, std::min(ta.groups, tb.groups));
        q.shape = "nested_topn_over_join";
        q.ordered = true;
        q.sql = "SELECT a.k, b.k, a.v FROM " + Src(ta, "a", side, &ea) +
                " JOIN " + Src(tb, "b", side, &eb) +
                " ON a.g = b.g ORDER BY a.v DESC NULLS LAST, a.k, b.k DESC "
                "LIMIT " +
                I(g_.Pick(1, 200)) + " OFFSET " + I(g_.Pick(0, 5000));
        return q;
      }
      case 4:
        in.cols = {{"a.g"}, {"b.t", true}, {"MOD(a.v, 4)"}};
        in.sql = "SELECT DISTINCT " + SelectList(in.cols) + " FROM " + ta.name +
                 " a JOIN " + tb.name + " b ON a.k = b.k";
        in.est_rows = ta.rows;
        return Finish("nested_distinct_over_join", in);
      case 5:
        q.shape = "nested_agg_over_window";
        q.ordered = true;
        q.sql = "SELECT c2, COUNT(*), SUM(k), MIN(k) FROM (SELECT k, NTILE(" +
                I(g_.Pick(2, 40)) + ") OVER (ORDER BY " + TotalOrder("", 1) +
                ") AS c2 FROM " + ta.name + ") GROUP BY c2 ORDER BY c2";
        return q;
      case 6:
        q.shape = "nested_union_of_aggs";
        q.sql = "SELECT g, COUNT(*) AS c, SUM(v) AS s FROM " + ta.name +
                " GROUP BY g UNION ALL SELECT g, COUNT(*), SUM(v) FROM " +
                tb.name +
                " GROUP BY g UNION ALL SELECT -1, COUNT(*), SUM(k) "
                "FROM " +
                ta.name;
        return q;
      case 7:
        q.shape = "nested_semi_agg_sort";
        q.ordered = true;
        q.sql = "SELECT a.g, COUNT(*), SUM(a.v) FROM " + ta.name +
                " a WHERE a.k IN (SELECT b.k FROM " + tb.name + " b WHERE " +
                Pred("b", tb) + ") GROUP BY a.g ORDER BY 2 DESC, a.g LIMIT " +
                I(g_.Pick(1, 50));
        return q;
      default:
        // Wide sort key: every comparison walks the pad bytes.
        q.shape = "nested_wide_key_sort";
        q.heavy = ta.pad_max > 256;
        q.sql =
            "SELECT COUNT(*), SUM(MOD(rn * LENGTH(pad), 1000003)) FROM "
            "(SELECT pad, ROW_NUMBER() OVER (ORDER BY pad" +
            std::string(g_.Chance(50) ? " DESC" : "") + ", k) AS rn FROM " +
            ta.name + MaybeWhere("", ta, 25) + ")";
        return q;
    }
  }

  OocRng& g_;
  const std::vector<OocGenTable>& tables_;
  int64_t cap_;
};

}  // namespace

OocGenQuery GenerateOocQuery(OocRng& rng,
                             const std::vector<OocGenTable>& tables,
                             int64_t max_join_rows) {
  Builder b(rng, tables, max_join_rows);
  return b.Build();
}

std::vector<std::string> OocQueryShapes() {
  return {"sort",       "sort_all",  "full_sort", "group_agg", "global_agg",
          "distinct",   "join",      "join3",     "semi_anti", "window",
          "setop",      "recursive", "apply",     "cross",     "band",
          "index_path", "nested"};
}

}  // namespace tinylamb
