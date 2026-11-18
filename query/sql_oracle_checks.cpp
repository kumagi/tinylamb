/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_oracle_checks.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/platform/process.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/query_memory.hpp"
#include "query/fuzz_scoped_db.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

// Rebuilds the mirror from the trace's single-row INSERT statements so a
// replayed .test file checks the same model the generator pinned.
std::vector<MirrorRow> MirrorFromSetup(const std::vector<std::string>& setup,
                                       const std::string& table) {
  std::vector<MirrorRow> mirror;
  // Only the base table feeds the mirror: sibling tables (e.g. the `_s`
  // copy used by the set-operation oracle) share the schema but not the
  // probe's FROM clause.
  const std::string prefix = "INSERT INTO " + table + " VALUES";
  for (const std::string& sql : setup) {
    if (!sql.starts_with(prefix)) {
      continue;
    }
    const size_t open = sql.find('(');
    if (open == std::string::npos) {
      continue;
    }
    std::vector<std::string> fields;
    std::string cur;
    bool in_quote = false;
    for (size_t i = open + 1; i < sql.size(); ++i) {
      const char c = sql[i];
      if (c == '\'') {
        in_quote = !in_quote;
        cur += c;
      } else if ((c == ',' || c == ')') && !in_quote) {
        fields.push_back(cur);
        cur.clear();
        if (c == ')') {
          break;
        }
      } else {
        cur += c;
      }
    }
    if (fields.size() < 5) {
      continue;
    }
    auto to_int = [](const std::string& v) -> int64_t {
      size_t i = 0;
      while (i < v.size() && (v[i] == ' ' || v[i] == '\t')) {
        ++i;
      }
      if (v.substr(i).starts_with("NULL")) {
        return kNull;
      }
      if (v.substr(i).starts_with("TRUE")) {
        return 1;
      }
      if (v.substr(i).starts_with("FALSE")) {
        return 0;
      }
      return std::stoll(v.substr(i));
    };
    MirrorRow m;
    m.u = to_int(fields[0]);
    m.a = to_int(fields[1]);
    m.b = to_int(fields[2]);
    m.flag = to_int(fields[3]);
    m.s = kNull;
    {
      const std::string& raw = fields[4];
      const size_t q1 = raw.find('\'');
      const size_t q2 = raw.rfind('\'');
      if (q1 != std::string::npos && q2 > q1) {
        const std::string content = raw.substr(q1 + 1, q2 - q1 - 1);
        for (size_t i = 0; i < kStrPool.size(); ++i) {
          if (content == kStrPool[i]) {
            m.s = static_cast<int64_t>(i);
            break;
          }
        }
      }
    }
    mirror.push_back(m);
  }
  return mirror;
}

int64_t ArgValue(const MirrorRow& m, int arg) {
  switch (arg) {
    case 0:
      return m.b;
    case 1:
      return m.a;
    default:
      return m.u;
  }
}

// Parses "KIND|P1|P2|ARG|PART|FLAGS|DEF|SQL".
bool ParseProbeSpec(const std::string& spec, int* kind, int64_t* p1,
                    int64_t* p2, int* arg, int* part, int64_t* flags,
                    int64_t* def, std::string* sql) {
  size_t pos = 0;
  std::array<long, 7> vals = {0, 0, 0, 0, -1, 0, 0};
  for (long& val : vals) {
    const size_t bar = spec.find('|', pos);
    if (bar == std::string::npos) {
      return false;
    }
    val = std::stoll(spec.substr(pos, bar - pos));
    pos = bar + 1;
  }
  *kind = static_cast<int>(vals[0]);
  *p1 = vals[1];
  *p2 = vals[2];
  *arg = static_cast<int>(vals[3]);
  *part = static_cast<int>(vals[4]);
  *flags = vals[5];
  *def = vals[6];
  *sql = spec.substr(pos);
  return !sql->empty();
}

// Expected (int64 or NULL) value of a model probe per u; kAnyValue entries
// are shape-only.  Ordering inside every partition is by u (unique key).
std::map<int64_t, int64_t> WindowModel(const std::vector<MirrorRow>& mirror,
                                       int kind, int64_t p1, int64_t p2,
                                       int arg, int part, int64_t flags,
                                       int64_t def) {
  std::map<int64_t, int64_t> expected;
  // Partition key: -1 → single partition, 0 → group by `a` (NULLs together).
  auto part_key = [part](const MirrorRow& m) {
    return part == 0 ? m.a : int64_t{0};
  };
  std::map<int64_t, std::vector<const MirrorRow*>> groups;
  for (const MirrorRow& m : mirror) {
    groups[part_key(m)].push_back(&m);
  }
  for (auto& [key, rows] : groups) {
    std::sort(
        rows.begin(), rows.end(),
        [](const MirrorRow* l, const MirrorRow* r) { return l->u < r->u; });
    const size_t n = rows.size();
    for (size_t i = 0; i < n; ++i) {
      int64_t out = kAnyValue;
      switch (kind) {
        case 0:  // ROW_NUMBER
          out = static_cast<int64_t>(i) + 1;
          break;
        case 1:  // RANK over `arg` column (ASC NULLS FIRST, no partition)
        case 2: {
          // RANK/DENSE_RANK computed globally in generation; handled below.
          break;
        }
        case 3: {  // LAG/LEAD over ORDER BY u
          const bool lead = (flags & 2) != 0;
          const int64_t target = lead ? static_cast<int64_t>(i) + p1
                                      : static_cast<int64_t>(i) - p1;
          if (target < 0 || std::cmp_greater_equal(target, n)) {
            out = def == kNoDefault ? kNull : def;
          } else {
            out = ArgValue(*rows[static_cast<size_t>(target)], arg);
          }
          break;
        }
        case 10:    // SUM over ROWS p1 PRECEDING .. p2 FOLLOWING
        case 11:    // COUNT(col)
        case 12:    // MIN
        case 13: {  // MAX
          bool any = false;
          int64_t acc = 0;
          const size_t lo =
              std::cmp_greater_equal(i, p1) ? i - static_cast<size_t>(p1) : 0;
          for (size_t j = lo; j <= i + static_cast<size_t>(p2) && j < n; ++j) {
            if (j == i && (flags & 1) != 0) {  // EXCLUDE CURRENT ROW
              continue;
            }
            const int64_t v = ArgValue(*rows[j], arg);
            if (kind == 11) {
              if (v != kNull) {
                ++acc;
              }
              continue;
            }
            if (v == kNull) {
              continue;
            }
            acc = !any ? v
                       : (kind == 10              ? acc + v
                          : kind == 12 && v < acc ? v
                          : kind == 13 && v > acc ? v
                                                  : acc);
            any = true;
          }
          // COUNT(col) yields 0 on an empty/all-NULL frame, never NULL.
          out = kind == 11 ? acc : (any ? acc : kNull);
          break;
        }
        case 20:  // whole-partition SUM/COUNT/MIN/MAX (UNBOUNDED frame)
        case 21:
        case 22:
        case 23: {
          bool any = false;
          int64_t acc = 0;
          for (const MirrorRow* r : rows) {
            const int64_t v = ArgValue(*r, arg);
            if (v == kNull) {
              continue;
            }
            if (kind == 21) {
              ++acc;
              continue;
            }
            acc = !any ? v
                       : (kind == 20              ? acc + v
                          : kind == 22 && v < acc ? v
                          : kind == 23 && v > acc ? v
                                                  : acc);
            any = true;
          }
          out = kind == 21 ? acc : (any ? acc : kNull);
          break;
        }
        case 50: {  // NTILE(k) over ORDER BY u: first n%k buckets get +1 row
          const int64_t k = p1;
          const int64_t base = static_cast<int64_t>(n) / k;
          const int64_t extra = static_cast<int64_t>(n) % k;
          const auto idx = static_cast<int64_t>(i);
          out = idx < extra * (base + 1)
                    ? (idx / (base + 1)) + 1
                    : extra + ((idx - extra * (base + 1)) / base) + 1;
          break;
        }
        case 51:  // FIRST_VALUE (default frame: unbounded .. current row)
          out = ArgValue(*rows[0], arg);
          break;
        case 52:  // LAST_VALUE (default frame ends at current row's peers;
                  // ORDER BY u is unique, so that is the current row)
          out = ArgValue(*rows[i], arg);
          break;
        case 60: {  // COUNTIF(b > p2) over the whole partition
          int64_t cnt = 0;
          for (const MirrorRow* r : rows) {
            if (r->b != kNull && r->b > p2) {
              ++cnt;
            }
          }
          out = cnt;
          break;
        }
        case 61: {  // COUNT(DISTINCT arg) over the whole partition
          std::set<int64_t> seen;
          for (const MirrorRow* r : rows) {
            const int64_t v = ArgValue(*r, arg);
            if (v != kNull) {
              seen.insert(v);
            }
          }
          out = static_cast<int64_t>(seen.size());
          break;
        }
        default:
          break;  // shape-only kinds (30-49): permutation/index checks only
      }
      if (out != kAnyValue) {
        expected[rows[i]->u] = out;
      }
    }
  }
  // RANK/DENSE_RANK are global (generated without PARTITION BY).
  if (kind == 1 || kind == 2) {
    const bool desc = (flags & 4) != 0;
    std::vector<const MirrorRow*> all;
    all.reserve(mirror.size());
    for (const MirrorRow& m : mirror) {
      all.push_back(&m);
    }
    std::sort(all.begin(), all.end(),
              [arg, desc](const MirrorRow* l, const MirrorRow* r) {
                const int64_t lv = ArgValue(*l, arg);
                const int64_t rv = ArgValue(*r, arg);
                if (lv == kNull || rv == kNull) {
                  // ASC: NULLS FIRST; DESC: NULLS LAST.
                  const bool l_null = lv == kNull;
                  const bool r_null = rv == kNull;
                  if (l_null && r_null) {
                    return false;
                  }
                  return desc ? r_null : l_null;
                }
                return desc ? lv > rv : lv < rv;
              });
    int64_t prev = kAnyValue;
    bool first = true;
    int64_t rank = 0;
    int64_t dense = 0;
    for (size_t i = 0; i < all.size(); ++i) {
      const int64_t v = ArgValue(*all[i], arg);
      if (first || v != prev) {
        rank = static_cast<int64_t>(i) + 1;
        ++dense;
        prev = v;
        first = false;
      }
      expected[all[i]->u] = kind == 1 ? rank : dense;
    }
  }
  return expected;
}

}  // namespace

namespace {

// ---------------------------------------------------------------------------
// Engine helpers.
// ---------------------------------------------------------------------------

struct ScopedEnv {
  ScopedEnv(const char* name, const char* value) : name_(name) {
    if (const char* prev = std::getenv(name)) {
      had_prev_ = true;
      prev_ = prev;
    }
    if (value != nullptr) {
      platform::SetEnv(name, value);
    } else {
      platform::UnsetEnv(name);
    }
  }
  ~ScopedEnv() {
    if (had_prev_) {
      platform::SetEnv(name_, prev_.c_str());
    } else {
      platform::UnsetEnv(name_);
    }
  }
  ScopedEnv(const ScopedEnv&) = delete;
  ScopedEnv& operator=(const ScopedEnv&) = delete;
  ScopedEnv(ScopedEnv&&) = delete;
  ScopedEnv& operator=(ScopedEnv&&) = delete;

  const char* name_;
  bool had_prev_{false};
  std::string prev_;
};

struct ScopedQueryMemory {
  explicit ScopedQueryMemory(uint64_t budget_bytes)
      : previous_limit_(QueryMemoryBudget::Global().Limit()) {
    QueryMemoryBudget::Global().ResetForTest(budget_bytes);
  }
  ~ScopedQueryMemory() {
    QueryMemoryBudget::Global().ResetForTest(previous_limit_);
  }
  ScopedQueryMemory(const ScopedQueryMemory&) = delete;
  ScopedQueryMemory& operator=(const ScopedQueryMemory&) = delete;
  ScopedQueryMemory(ScopedQueryMemory&&) = delete;
  ScopedQueryMemory& operator=(ScopedQueryMemory&&) = delete;
  size_t previous_limit_{0};
};

// Executes a SELECT and returns its rows.  nullopt means the engine rejected
// the query (skip the oracle).
}  // namespace

std::optional<std::vector<Row>> RunRowsMode(Database& db,
                                            TransactionContext& ctx,
                                            const std::string& sql,
                                            bool force_relational,
                                            std::string* error) {
  SqlEngine engine(db);
  engine.SetForceRelationalFallback(force_relational);
  StatusOr<QueryResult> result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    *error = engine.LastError();
    return std::nullopt;
  }
  std::vector<Row> rows;
  Row row;
  while (result.Value().Next(&row)) {
    rows.push_back(row);
  }
  if (Status st = result.Value().GetStatus(); st != Status::kSuccess) {
    *error = st.GetMessage().empty() ? ToString(st.GetCode()) : st.GetMessage();
    return std::nullopt;
  }
  return rows;
}

std::optional<std::vector<Row>> RunRows(Database& db, TransactionContext& ctx,
                                        const std::string& sql,
                                        std::string* error) {
  return RunRowsMode(db, ctx, sql, /*force_relational=*/false, error);
}

std::vector<std::string> SerializeSorted(const std::vector<Row>& rows) {
  std::vector<std::string> serialized;
  serialized.reserve(rows.size());
  for (const Row& r : rows) {
    serialized.push_back(r.ToString());
  }
  std::sort(serialized.begin(), serialized.end());
  return serialized;
}

// Runs `sql` expected to yield a single scalar row; NULL counts as 0.
std::optional<std::string> RunScalar(Database& db, TransactionContext& ctx,
                                     const std::string& sql,
                                     std::string* error) {
  auto rows = RunRows(db, ctx, sql, error);
  if (!rows.has_value() || rows->size() != 1) {
    return std::nullopt;
  }
  const Value& value = (*rows)[0][0];
  if (value.IsNull()) {
    return std::string("0");
  }
  return value.AsString();
}

// Runs any statement to completion; returns false on error.
bool RunUpdate(Database& db, TransactionContext& ctx, const std::string& sql,
               std::string* error) {
  SqlEngine engine(db);
  StatusOr<QueryResult> result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    if (error != nullptr) {
      *error = engine.LastError();
    }
    return false;
  }
  // QueryResults are lazy: drain or the mutation never happens.
  result.Value().Drain();
  if (result.Value().GetStatus() != Status::kSuccess) {
    if (error != nullptr) {
      *error = result.Value().GetStatus().GetMessage();
    }
    return false;
  }
  return true;
}

std::string DumpRows(const std::vector<std::string>& rows) {
  std::string out;
  for (const std::string& r : rows) {
    out += "    " + r + "\n";
  }
  return out;
}

bool CheckExpected(Database& db, TransactionContext& ctx,
                   const std::vector<std::string>& queries,
                   const std::vector<std::string>& expected, bool ordered,
                   const char* tag, std::string* report, bool verbose) {
  if (queries.size() != 1) {
    return true;
  }
  std::string error;
  auto rows = RunRows(db, ctx, queries[0], &error);
  if (!rows.has_value()) {
    if (verbose) {
      std::cerr << "[sql_oracle][" << tag << "-error] " << error << "\n";
    }
    return false;
  }
  std::vector<std::string> actual;
  actual.reserve(rows->size());
  for (const Row& r : *rows) {
    actual.push_back(r.ToString());
  }
  std::vector<std::string> expect = expected;
  if (!ordered) {
    std::sort(actual.begin(), actual.end());
    std::sort(expect.begin(), expect.end());
  }
  if (actual == expect) {
    return true;
  }
  *report += std::string("[") + tag + " MISMATCH]\n";
  *report += "  query: " + queries[0] + "\n";
  *report += "  expected (" + std::to_string(expect.size()) + "):\n" +
             DumpRows(expect);
  *report +=
      "  actual (" + std::to_string(actual.size()) + "):\n" + DumpRows(actual);
  return true;
}

// Two-query equivalence: both forms must return the same row multiset.
bool CheckPair(Database& db, TransactionContext& ctx,
               const std::vector<std::string>& cte, const char* tag,
               std::string* report, bool verbose) {
  if (cte.size() != 2) {
    return true;
  }
  std::string error;
  auto with = RunRows(db, ctx, cte[0], &error);
  auto flat = RunRows(db, ctx, cte[1], &error);
  if (!(with.has_value() && flat.has_value())) {
    if (verbose) {
      std::cerr << "[sql_oracle][" << tag << "-error] " << error << "\n";
    }
    return false;
  }
  std::vector<std::string> s0 = SerializeSorted(*with);
  std::vector<std::string> s1 = SerializeSorted(*flat);
  if (s0 == s1) {
    return true;
  }
  *report += std::string("[") + tag + " MISMATCH]\n";
  *report += "  lhs: " + cte[0] + " (" + std::to_string(s0.size()) +
             " rows)\n" + DumpRows(s0);
  *report += "  rhs: " + cte[1] + " (" + std::to_string(s1.size()) +
             " rows)\n" + DumpRows(s1);
  return true;
}

// WITH RECURSIVE counters: the single aggregate row must match arithmetic.
bool CheckRecursive(Database& db, TransactionContext& ctx, const OracleTrace& t,
                    std::string* report, bool verbose) {
  if (t.recursive.empty()) {
    return true;
  }
  std::string error;
  auto rows = RunRows(db, ctx, t.recursive, &error);
  if (!rows.has_value()) {
    if (verbose) {
      std::cerr << "[sql_oracle][recursive-error] " << error << "\n";
    }
    return false;
  }
  if (rows->size() == 1 && (*rows)[0].ToString() == t.recursive_expect) {
    return true;
  }
  *report += "[RECURSIVE MISMATCH]\n";
  *report += "  " + t.recursive + "\n";
  *report += "  expected: " + t.recursive_expect + "\n  actual:";
  for (const Row& r : *rows) {
    *report += " " + r.ToString();
  }
  *report += "\n";
  return true;
}

// ---------------------------------------------------------------------------
// Oracle checks shared by generation and replay.  Each takes the trace
// fragment, executes it, appends a mismatch diagnostic to `report`, and
// returns false when a statement could not be executed (oracle skipped).
// ---------------------------------------------------------------------------

bool CheckTlp(Database& db, TransactionContext& ctx,
              const std::vector<std::string>& tlp, std::string* report,
              bool verbose) {
  if (tlp.size() != 4) {
    return true;
  }
  std::string error;
  auto r0 = RunRows(db, ctx, tlp[0], &error);
  auto r1 = RunRows(db, ctx, tlp[1], &error);
  auto r2 = RunRows(db, ctx, tlp[2], &error);
  auto r3 = RunRows(db, ctx, tlp[3], &error);
  if (!(r0.has_value() && r1.has_value() && r2.has_value() && r3.has_value())) {
    if (verbose) {
      std::cerr << "[sql_oracle][tlp-error] " << error << "\n";
    }
    return false;
  }
  const std::vector<std::string> s0 = SerializeSorted(*r0);
  std::vector<std::string> s1 = SerializeSorted(*r1);
  std::vector<std::string> s2 = SerializeSorted(*r2);
  std::vector<std::string> s3 = SerializeSorted(*r3);
  std::vector<std::string> merged = s1;
  merged.insert(merged.end(), s2.begin(), s2.end());
  merged.insert(merged.end(), s3.begin(), s3.end());
  std::sort(merged.begin(), merged.end());
  if (s0 == merged) {
    return true;
  }
  *report += "[TLP MISMATCH]\n";
  *report += "  original: " + tlp[0] + " (" + std::to_string(s0.size()) +
             " rows)\n" + DumpRows(s0);
  *report += "  part1:    " + tlp[1] + " (" + std::to_string(s1.size()) +
             " rows)\n" + DumpRows(s1);
  *report += "  part2:    " + tlp[2] + " (" + std::to_string(s2.size()) +
             " rows)\n" + DumpRows(s2);
  *report += "  part3:    " + tlp[3] + " (" + std::to_string(s3.size()) +
             " rows)\n" + DumpRows(s3);
  *report += "  merged partition rows: " + std::to_string(merged.size()) + "\n";
  return true;
}

// Aggregate TLP: grouped COUNT(*)/SUM over the whole predicate must equal
// the three partitions merged group-wise (count: sum, sum: add skipping
// NULL-only slices, presence: union).  Stresses grouped execution, NULL
// group keys, and the optimizer's partition pushdown under joins.
bool CheckTlpAgg(Database& db, TransactionContext& ctx,
                 const std::vector<std::string>& tlp_agg, std::string* report,
                 bool verbose) {
  if (tlp_agg.size() != 4) {
    return true;
  }
  std::string error;
  // Materialize each arm up front (a failed arm aborts the whole check), so
  // the later loops read plain vectors instead of re-probing optionals.
  std::vector<std::vector<Row>> parts;
  parts.reserve(4);
  for (int i = 0; i < 4; ++i) {
    std::optional<std::vector<Row>> rows =
        RunRows(db, ctx, tlp_agg[static_cast<size_t>(i)], &error);
    if (!rows.has_value()) {
      if (verbose) {
        std::cerr << "[sql_oracle][tlpagg-error] " << error << "\n";
      }
      return false;
    }
    parts.push_back(std::move(*rows));
    for (const Row& r : parts.back()) {
      if (r.Size() < 3 || r[1].IsNull() || r[1].type != ValueType::kInt64 ||
          (!r[2].IsNull() && r[2].type != ValueType::kInt64)) {
        return false;  // unexpected aggregate output shape: skip
      }
    }
  }
  using Group = std::pair<int64_t, std::optional<int64_t>>;
  auto key_of = [](const Row& r) { return r[0].AsString(); };
  std::map<std::string, Group> expected;
  for (int i = 1; i < 4; ++i) {
    for (const Row& r : parts[static_cast<size_t>(i)]) {
      Group& g = expected[key_of(r)];
      g.first += r[1].value.int_value;
      if (!r[2].IsNull()) {
        g.second = g.second.value_or(0) + r[2].value.int_value;
      }
    }
  }
  std::map<std::string, Group> actual;
  for (const Row& r : parts[0]) {
    actual[key_of(r)] = {r[1].value.int_value,
                         r[2].IsNull()
                             ? std::optional<int64_t>()
                             : std::optional<int64_t>(r[2].value.int_value)};
  }
  if (expected == actual) {
    return true;
  }
  *report += "[AGG-TLP MISMATCH]\n";
  *report += "  original: " + tlp_agg[0] + "\n";
  auto dump = [](const std::map<std::string, Group>& m) {
    std::string out;
    for (const auto& [key, grp] : m) {
      out += "    " + key + " -> count " + std::to_string(grp.first) +
             ", sum " +
             (grp.second.has_value() ? std::to_string(*grp.second)
                                     : std::string("NULL")) +
             "\n";
    }
    return out;
  };
  *report += "  merged partitions:\n" + dump(expected);
  *report += "  original result:\n" + dump(actual);
  return true;
}

// True when the SELECT clause itself is a DISTINCT projection.  A bare
// substring search for "DISTINCT" also matches "IS [NOT] DISTINCT FROM"
// inside the WHERE predicate, which must not degrade the bag comparison
// into a set comparison.
namespace {

bool HasDistinctProjection(const std::string& sql) {
  const size_t select = sql.find("SELECT");
  if (select == std::string::npos) {
    return false;
  }
  size_t pos = select + 6;
  while (pos < sql.size() && (sql[pos] == ' ' || sql[pos] == '\t' ||
                              sql[pos] == '\n' || sql[pos] == '\r')) {
    ++pos;
  }
  if (sql.compare(pos, 8, "DISTINCT") != 0) {
    return false;
  }
  const size_t end = pos + 8;
  return end == sql.size() ||
         (sql[end] != '_' && (sql[end] < '0' || sql[end] > '9') &&
          (sql[end] < 'A' || sql[end] > 'Z') &&
          (sql[end] < 'a' || sql[end] > 'z'));
}

// UNION ALL partitioning: the engine itself must recombine the partitions.
// Q[p] as a BAG equals part1 UNION ALL part2 UNION ALL part3 (the WHERE
// splits are disjoint, so plain rows never duplicate across parts).  A
// DISTINCT projection breaks the bag invariant: one value projected in two
// parts is deduplicated per branch yet appears twice in the concatenation,
// so DISTINCT queries compare as SETS instead (same rule as CheckTlp).
}  // namespace

bool CheckUnionAll(Database& db, TransactionContext& ctx,
                   const std::vector<std::string>& unionall,
                   std::string* report, bool verbose) {
  if (unionall.size() != 2) {
    return true;
  }
  std::string error;
  auto original = RunRows(db, ctx, unionall[0], &error);
  auto merged = RunRows(db, ctx, unionall[1], &error);
  if (!(original.has_value() && merged.has_value())) {
    if (verbose) {
      std::cerr << "[sql_oracle][unionall-error] " << error << "\n";
    }
    return false;
  }
  std::vector<std::string> s0 = SerializeSorted(*original);
  std::vector<std::string> s1 = SerializeSorted(*merged);
  if (HasDistinctProjection(unionall[0])) {
    s0.erase(std::unique(s0.begin(), s0.end()), s0.end());
    s1.erase(std::unique(s1.begin(), s1.end()), s1.end());
  }
  if (s0 == s1) {
    return true;
  }
  *report += "[UNION-ALL MISMATCH]\n";
  *report += "  original: " + unionall[0] + " (" + std::to_string(s0.size()) +
             " rows)\n" + DumpRows(s0);
  *report += "  union:    " + unionall[1] + " (" + std::to_string(s1.size()) +
             " rows)\n" + DumpRows(s1);
  return true;
}

// Subquery differential: the three COUNT spellings must be identical.
bool CheckSubq(Database& db, TransactionContext& ctx,
               const std::vector<std::string>& subq, std::string* report,
               bool verbose) {
  if (subq.size() != 3) {
    return true;
  }
  std::string error;
  std::vector<std::string> counts;
  counts.reserve(3);
  for (int i = 0; i < 3; ++i) {
    std::optional<std::string> count =
        RunScalar(db, ctx, subq[static_cast<size_t>(i)], &error);
    if (!count.has_value()) {
      if (verbose) {
        std::cerr << "[sql_oracle][subq-error] " << error << "\n";
      }
      return false;
    }
    counts.push_back(std::move(*count));
  }
  if (counts[0] == counts[1] && counts[1] == counts[2]) {
    return true;
  }
  *report += "[SUBQ MISMATCH]\n";
  for (int i = 0; i < 3; ++i) {
    const char* tag = i == 0 ? "IN     " : (i == 1 ? "EXISTS " : "JOIN   ");
    *report += std::string("  ") + tag + subq[static_cast<size_t>(i)] + " => " +
               counts[static_cast<size_t>(i)] + "\n";
  }
  return true;
}

// Reference projections end with a 0/1 indicator.  Rows whose indicator is 1
// are the ones the optimized WHERE must have returned.  Comparing the
// payload (every column but the indicator) catches a wrong row whose
// cardinality still matches.
namespace {

std::vector<std::string> NoRecPayloads(const std::vector<Row>& rows,
                                       bool drop_indicator) {
  std::vector<std::string> out;
  out.reserve(rows.size());
  for (const Row& row : rows) {
    if (row.Count() == 0) {
      continue;
    }
    // Count() is the cell count.  Size() is the serialized byte length.
    size_t keep = row.Count();
    if (drop_indicator) {
      if (keep < 2) {
        continue;
      }
      const Value& indicator = row[keep - 1];
      if (indicator.type != ValueType::kInt64 ||
          indicator.value.int_value != 1) {
        continue;
      }
      --keep;
    }
    std::vector<Value> cells;
    cells.reserve(keep);
    for (size_t i = 0; i < keep; ++i) {
      cells.push_back(row[i]);
    }
    out.push_back(Row(std::move(cells)).ToString());
  }
  std::sort(out.begin(), out.end());
  return out;
}

// One NoREC phase.  A pair the engine rejects is skipped (the paper's
// short-circuit / unsupported-syntax rule): a one-sided error is not a
// mismatch.  Scalar pairs compare counts; content pairs compare payloads.
}  // namespace

bool CheckNoRec(Database& db, TransactionContext& ctx, const OracleTrace& t,
                const char* phase, std::string* report, bool verbose) {
  auto note_skip = [&](const char* kind, const std::string& error,
                       const std::string& sql) {
    if (verbose) {
      std::cerr << "[sql_oracle][norec-skip] " << phase << " " << kind << " "
                << error << " :: " << sql << "\n";
    }
  };
  // An arm with no recorded pairs does not exist for this trace and so
  // cannot count as skipped; a recorded pair that cannot execute does.
  const bool norec_recorded = t.norec.size() >= 2 && t.norec.size() % 2 == 0;
  const bool content_recorded =
      t.norec_content.size() >= 2 && t.norec_content.size() % 2 == 0;
  bool ran = !norec_recorded && !content_recorded;
  if (norec_recorded) {
    for (size_t i = 0; i + 1 < t.norec.size(); i += 2) {
      std::string error;
      auto optimized = RunScalar(db, ctx, t.norec[i], &error);
      if (!optimized.has_value()) {
        note_skip("opt", error, t.norec[i]);
        continue;
      }
      auto reference = RunScalar(db, ctx, t.norec[i + 1], &error);
      if (!reference.has_value()) {
        note_skip("ref", error, t.norec[i + 1]);
        continue;
      }
      ran = true;
      if (*optimized == *reference) {
        continue;
      }
      *report += "[NoREC MISMATCH]\n";
      *report += std::string("  phase: ") + phase + "\n";
      *report += "  pair: " + std::to_string(i / 2) + "\n";
      *report += "  optimized: " + t.norec[i] + " => " + *optimized + "\n";
      *report += "  reference: " + t.norec[i + 1] + " => " + *reference + "\n";
    }
  }
  if (content_recorded) {
    for (size_t i = 0; i + 1 < t.norec_content.size(); i += 2) {
      std::string error;
      auto optimized = RunRows(db, ctx, t.norec_content[i], &error);
      if (!optimized.has_value()) {
        note_skip("content-opt", error, t.norec_content[i]);
        continue;
      }
      auto reference = RunRows(db, ctx, t.norec_content[i + 1], &error);
      if (!reference.has_value()) {
        note_skip("content-ref", error, t.norec_content[i + 1]);
        continue;
      }
      ran = true;
      const std::vector<std::string> got = NoRecPayloads(*optimized, false);
      const std::vector<std::string> expect = NoRecPayloads(*reference, true);
      if (got == expect) {
        continue;
      }
      *report += "[NoREC MISMATCH]\n";
      *report += std::string("  phase: ") + phase + "\n";
      *report += "  kind: rows\n";
      *report += "  pair: " + std::to_string(i / 2) + "\n";
      *report += "  optimized: " + t.norec_content[i] + " (" +
                 std::to_string(got.size()) + " rows)\n" + DumpRows(got);
      *report += "  reference: " + t.norec_content[i + 1] + " (" +
                 std::to_string(expect.size()) + " rows)\n" + DumpRows(expect);
    }
  }
  return ran;
}

// ANALYZE every table the trace created so the indexed NoREC phase costs
// access paths with statistics, not only with the indexes present.
void AnalyzeCreatedTables(Database& db, TransactionContext& ctx,
                          const std::vector<std::string>& setup) {
  static constexpr std::string_view kCreate = "CREATE TABLE ";
  for (const std::string& sql : setup) {
    if (!sql.starts_with(kCreate)) {
      continue;
    }
    const size_t begin = kCreate.size();
    const size_t end = sql.find_first_of(" (", begin);
    if (end == std::string::npos || end == begin) {
      continue;
    }
    RunUpdate(db, ctx, "ANALYZE " + sql.substr(begin, end - begin) + ";");
  }
}

// PQS: the harness-mirror count must match, and the pivot row must appear.
bool CheckPqs(Database& db, TransactionContext& ctx, const OracleTrace& t,
              std::string* report, bool verbose) {
  if (t.pqs_count.empty() || t.pqs_u.empty()) {
    return true;
  }
  std::string error;
  auto count = RunScalar(db, ctx, t.pqs_count, &error);
  auto us = RunRows(db, ctx, t.pqs_u, &error);
  if (!(count.has_value() && us.has_value())) {
    if (verbose) {
      std::cerr << "[sql_oracle][pqs-error] " << error << "\n";
    }
    return false;
  }
  bool pivot_found = false;
  for (const Row& r : *us) {
    if (r[0] == Value(t.pqs_pivot_u)) {
      pivot_found = true;
      break;
    }
  }
  const bool count_ok = *count == std::to_string(t.pqs_expected);
  if (count_ok && pivot_found) {
    return true;
  }
  *report += "[PQS MISMATCH]\n";
  if (!count_ok) {
    *report += "  " + t.pqs_count + "\n    expected " +
               std::to_string(t.pqs_expected) + " rows, got " + *count + "\n";
  }
  if (!pivot_found) {
    *report += "  pivot u=" + std::to_string(t.pqs_pivot_u) +
               " missing from: " + t.pqs_u + "\n";
  }
  return true;
}

// Constraint rewriting: the probe must survive CREATE INDEX unchanged.
bool CheckIdx(Database& db, TransactionContext& ctx, const OracleTrace& t,
              std::string* report, bool verbose) {
  if (t.index_ddl.empty() || t.index_probe.empty()) {
    return true;
  }
  std::string error;
  auto before = RunScalar(db, ctx, t.index_probe, &error);
  if (!before.has_value()) {
    if (verbose) {
      std::cerr << "[sql_oracle][idx-error] " << error << "\n";
    }
    return false;
  }
  for (const std::string& ddl : t.index_ddl) {
    if (!ApplyIndexSpec(db, ctx, ddl)) {
      if (verbose) {
        std::cerr << "[sql_oracle][idx-ddl-error] :: " << ddl << "\n";
      }
      return false;
    }
  }
  if (!t.table.empty()) {
    RunUpdate(db, ctx, "ANALYZE " + t.table + ";");
  }
  auto after = RunScalar(db, ctx, t.index_probe, &error);
  if (!after.has_value()) {
    if (verbose) {
      std::cerr << "[sql_oracle][idx-error] " << error << "\n";
    }
    return false;
  }
  if (*before != *after) {
    *report += "[INDEX MISMATCH]\n";
    *report += "  before: " + t.index_probe + " => " + *before + "\n";
    for (const std::string& ddl : t.index_ddl) {
      *report += "  ddl:    " + ddl + "\n";
    }
    *report += "  after:  " + t.index_probe + " => " + *after + "\n";
    return true;
  }
  // Also verify that composite xmode_probes return identical results on the
  // newly indexed + analyzed table vs relational AST fallback.
  for (const std::string& sql : t.xmode_probes) {
    std::string ref_err;
    std::optional<std::vector<Row>> ref_rows;
    {
      ScopedEnv bc("TINYLAMB_DISABLE_BYTECODE", "1");
      ScopedEnv fsa("TINYLAMB_FUSE_SIBLING_AGG", "0");
      ref_rows = RunRowsMode(db, ctx, sql, /*force_relational=*/true, &ref_err);
    }
    if (!ref_rows.has_value()) {
      continue;
    }
    std::string idx_err;
    auto idx_rows =
        RunRowsMode(db, ctx, sql, /*force_relational=*/false, &idx_err);
    if (!idx_rows.has_value()) {
      continue;
    }
    const std::vector<std::string> s_ref = SerializeSorted(*ref_rows);
    const std::vector<std::string> s_idx = SerializeSorted(*idx_rows);
    if (s_ref != s_idx) {
      *report += "[XMODE-INDEXED MISMATCH]\n";
      *report += "  query: " + sql + "\n";
      for (const std::string& ddl : t.index_ddl) {
        *report += "  ddl:   " + ddl + "\n";
      }
      *report += "  expected (" + std::to_string(s_ref.size()) + "):\n" +
                 DumpRows(s_ref);
      *report += "  actual (" + std::to_string(s_idx.size()) + "):\n" +
                 DumpRows(s_idx);
      return true;
    }
  }
  return true;
}

// Cross-mode / cross-engine differential oracle: compares composite queries
// across (1) unindexed relational AST fallback (TINYLAMB_DISABLE_BYTECODE=1,
// TINYLAMB_FUSE_SIBLING_AGG=0, SetForceRelationalFallback(true)), (2) default
// Cascades/Relational + Bytecode/JIT, (3) Cascades GROUP BY route
// (TINYLAMB_GROUPED_ROUTE=cascades), and (4) USING lstable PAX columnar tables
// after ANALYZE.
bool CheckXMode(Database& db, TransactionContext& ctx, const OracleTrace& t,
                std::string* report, bool verbose) {
  if (t.xmode_probes.empty() || t.table.empty()) {
    return true;
  }
  const std::string tab = t.table;
  const std::string tab_lst = tab + "l";
  auto substitute_lst = [&tab, &tab_lst](std::string sql) {
    size_t pos = 0;
    while ((pos = sql.find(tab, pos)) != std::string::npos) {
      sql.replace(pos, tab.size(), tab_lst);
      pos += tab_lst.size();
    }
    return sql;
  };

  // Build PAX columnar (USING lstable) mirror tables and ANALYZE them using a
  // single INSERT ... SELECT per table instead of re-parsing every row INSERT.
  bool lst_ready = true;
  for (const std::string& sql : t.setup) {
    if (!sql.starts_with("CREATE TABLE ") || !sql.ends_with(";")) {
      continue;
    }
    const size_t name_start = std::string("CREATE TABLE ").size();
    const size_t name_end = sql.find(' ', name_start);
    if (name_end == std::string::npos) {
      continue;
    }
    const std::string orig_tab = sql.substr(name_start, name_end - name_start);
    std::string lsql = substitute_lst(sql);
    lsql.pop_back();
    lsql += " USING lstable;";
    const std::string ltab = substitute_lst(orig_tab);
    std::string ins_sql = "INSERT INTO ";
    ins_sql += ltab;
    ins_sql += " SELECT * FROM ";
    ins_sql += orig_tab;
    ins_sql += ";";
    if (!RunUpdate(db, ctx, lsql) || !RunUpdate(db, ctx, ins_sql) ||
        !RunUpdate(db, ctx, "ANALYZE " + ltab + ";")) {
      lst_ready = false;
      break;
    }
  }
  if (lst_ready) {
    (void)ctx.txn_.PreCommit();
    ctx = db.BeginContext();
  }

  for (const std::string& sql : t.xmode_probes) {
    std::string error;
    auto note_skip = [&](const char* kind, const std::string& why) {
      if (verbose) {
        std::cerr << "[sql_oracle][xmode-variant-skip] " << kind << " " << why
                  << " :: " << sql << "\n";
      }
    };
    std::optional<std::vector<Row>> ref_rows;
    {
      ScopedEnv bc("TINYLAMB_DISABLE_BYTECODE", "1");
      ScopedEnv fsa("TINYLAMB_FUSE_SIBLING_AGG", "0");
      ref_rows = RunRowsMode(db, ctx, sql, /*force_relational=*/true, &error);
    }
    if (!ref_rows.has_value()) {
      if (verbose) {
        std::cerr << "[sql_oracle][xmode-ref-skip] " << error << " :: " << sql
                  << "\n";
      }
      continue;
    }
    const std::vector<std::string> s_ref = SerializeSorted(*ref_rows);

    // Mode A: default Cascades/Relational + Bytecode/JIT + Sibling Agg Fusion.
    if (auto opt_rows =
            RunRowsMode(db, ctx, sql, /*force_relational=*/false, &error);
        opt_rows.has_value()) {
      const std::vector<std::string> s_opt = SerializeSorted(*opt_rows);
      if (s_opt != s_ref) {
        *report += "[XMODE-OPT MISMATCH]\n";
        *report += "  query: " + sql + "\n";
        *report += "  expected (" + std::to_string(s_ref.size()) + "):\n" +
                   DumpRows(s_ref);
        *report += "  actual (" + std::to_string(s_opt.size()) + "):\n" +
                   DumpRows(s_opt);
        return true;
      }
    } else {
      note_skip("opt", error);
    }

    // Mode B: Cascades GROUP BY route (TINYLAMB_GROUPED_ROUTE=cascades).
    {
      ScopedEnv gr("TINYLAMB_GROUPED_ROUTE", "cascades");
      if (auto cas_rows =
              RunRowsMode(db, ctx, sql, /*force_relational=*/false, &error);
          cas_rows.has_value()) {
        const std::vector<std::string> s_cas = SerializeSorted(*cas_rows);
        if (s_cas != s_ref) {
          *report += "[XMODE-CASCADES MISMATCH]\n";
          *report += "  query: " + sql + "\n";
          *report += "  expected (" + std::to_string(s_ref.size()) + "):\n" +
                     DumpRows(s_ref);
          *report += "  actual (" + std::to_string(s_cas.size()) + "):\n" +
                     DumpRows(s_cas);
          return true;
        }
      } else {
        note_skip("cascades", error);
      }
    }

    // Mode C: USING lstable PAX columnar store + ANALYZE.
    if (lst_ready) {
      const std::string lsql = substitute_lst(sql);
      if (auto lst_rows =
              RunRowsMode(db, ctx, lsql, /*force_relational=*/false, &error);
          lst_rows.has_value()) {
        const std::vector<std::string> s_lst = SerializeSorted(*lst_rows);
        if (s_lst != s_ref) {
          *report += "[XMODE-LSTABLE MISMATCH]\n";
          *report += "  query: " + sql + "\n";
          *report += "  expected (" + std::to_string(s_ref.size()) + "):\n" +
                     DumpRows(s_ref);
          *report += "  actual (" + std::to_string(s_lst.size()) + "):\n" +
                     DumpRows(s_lst);
          return true;
        }
      } else {
        note_skip("lstable", error);
      }
      // Mode D: USING lstable + Cascades GROUP BY route.
      {
        ScopedEnv gr("TINYLAMB_GROUPED_ROUTE", "cascades");
        if (auto lst_cas_rows =
                RunRowsMode(db, ctx, lsql, /*force_relational=*/false, &error);
            lst_cas_rows.has_value()) {
          const std::vector<std::string> s_lst_cas =
              SerializeSorted(*lst_cas_rows);
          if (s_lst_cas != s_ref) {
            *report += "[XMODE-LSTABLE-CASCADES MISMATCH]\n";
            *report += "  query: " + sql + "\n";
            *report += "  expected (" + std::to_string(s_ref.size()) + "):\n" +
                       DumpRows(s_ref);
            *report += "  actual (" + std::to_string(s_lst_cas.size()) +
                       "):\n" + DumpRows(s_lst_cas);
            return true;
          }
        } else {
          note_skip("lstable-cascades", error);
        }
      }
    }

    // Mode E: External disk spill under tight QueryMemoryBudget (512B).
    {
      ScopedQueryMemory low_mem(512);
      if (auto spill_rows =
              RunRowsMode(db, ctx, sql, /*force_relational=*/false, &error);
          spill_rows.has_value()) {
        const std::vector<std::string> s_spill = SerializeSorted(*spill_rows);
        if (s_spill != s_ref) {
          *report += "[XMODE-SPILL MISMATCH]\n";
          *report += "  query: " + sql + "\n";
          *report += "  expected (" + std::to_string(s_ref.size()) + "):\n" +
                     DumpRows(s_ref);
          *report += "  actual (" + std::to_string(s_spill.size()) + "):\n" +
                     DumpRows(s_spill);
          return true;
        }
      } else {
        note_skip("spill", error);
      }
      ScopedEnv gr("TINYLAMB_GROUPED_ROUTE", "cascades");
      if (auto cas_spill_rows =
              RunRowsMode(db, ctx, sql, /*force_relational=*/false, &error);
          cas_spill_rows.has_value()) {
        const std::vector<std::string> s_cas_spill =
            SerializeSorted(*cas_spill_rows);
        if (s_cas_spill != s_ref) {
          *report += "[XMODE-CASCADES-SPILL MISMATCH]\n";
          *report += "  query: " + sql + "\n";
          *report += "  expected (" + std::to_string(s_ref.size()) + "):\n" +
                     DumpRows(s_ref);
          *report += "  actual (" + std::to_string(s_cas_spill.size()) +
                     "):\n" + DumpRows(s_cas_spill);
          return true;
        }
      } else {
        note_skip("cascades-spill", error);
      }
    }
  }

  // Template-cache multi-CTE parameter rebinding oracle (2026-11-10 audit
  // High #22): warm up a single SqlEngine with non-alphabetical CTEs
  // (`z_cte` before `a_cte`), then execute a second query with shifted
  // literals on the SAME SqlEngine instance and compare against a fresh
  // uncached relational reference engine.
  {
    SqlEngine cached_engine(db);
    const std::string warm_sql =
        "WITH z_cte AS (SELECT a AS v FROM " + tab +
        " WHERE a IS NOT NULL UNION ALL SELECT 7), "
        "a_cte AS (SELECT b AS v FROM " +
        tab +
        " WHERE b IS NOT NULL UNION ALL SELECT 8) "
        "SELECT z_cte.v, a_cte.v FROM z_cte, a_cte WHERE z_cte.v >= -10;";
    const std::string rebound_sql =
        "WITH z_cte AS (SELECT a AS v FROM " + tab +
        " WHERE a IS NOT NULL UNION ALL SELECT 17), "
        "a_cte AS (SELECT b AS v FROM " +
        tab +
        " WHERE b IS NOT NULL UNION ALL SELECT 18) "
        "SELECT z_cte.v, a_cte.v FROM z_cte, a_cte WHERE z_cte.v >= -5;";
    if (auto w = cached_engine.Execute(ctx, warm_sql); w.HasValue()) {
      w.Value().Drain();
      if (auto r_cached = cached_engine.Execute(ctx, rebound_sql);
          r_cached.HasValue()) {
        std::vector<Row> cached_rows = r_cached.Value().Collect();
        if (r_cached.Value().GetStatus().ok()) {
          std::string ref_err;
          ScopedEnv bc("TINYLAMB_DISABLE_BYTECODE", "1");
          if (auto ref_rows = RunRowsMode(db, ctx, rebound_sql,
                                          /*force_relational=*/true, &ref_err);
              ref_rows.has_value()) {
            const std::vector<std::string> s_cached =
                SerializeSorted(cached_rows);
            const std::vector<std::string> s_ref = SerializeSorted(*ref_rows);
            if (s_cached != s_ref) {
              *report += "[TEMPLATE-CACHE CTE REBIND MISMATCH]\n";
              *report += "  warm:    " + warm_sql + "\n";
              *report += "  rebound: " + rebound_sql + "\n";
              *report += "  expected:\n" + DumpRows(s_ref);
              *report += "  actual:\n" + DumpRows(s_cached);
              return true;
            }
          }
        }
      }
    }
  }

  // `SELECT AS STRUCT s.*` wildcard expansion oracle (2026-11-10 audit
  // High #18): must match explicit column listing `SELECT AS STRUCT s.u, ...`.
  {
    std::string err1, err2;
    const std::string q_star = "SELECT AS STRUCT s.* FROM " + tab + " s;";
    const std::string q_expl =
        "SELECT AS STRUCT s.u, s.a, s.b, s.flag, s.s FROM " + tab + " s;";
    auto r_star =
        RunRowsMode(db, ctx, q_star, /*force_relational=*/false, &err1);
    auto r_expl =
        RunRowsMode(db, ctx, q_expl, /*force_relational=*/false, &err2);
    if (r_star.has_value() && r_expl.has_value()) {
      const std::vector<std::string> s_star = SerializeSorted(*r_star);
      const std::vector<std::string> s_expl = SerializeSorted(*r_expl);
      if (s_star != s_expl) {
        *report += "[SELECT-AS-STRUCT-STAR MISMATCH]\n";
        *report += "  star: " + q_star + "\n";
        *report += "  expl: " + q_expl + "\n";
        return true;
      }
    }
  }

  // Suffixed table name (`<tab>` vs `<tab>_1`) outer-join null-rejection and
  // anti-join oracle (2026-11-10 audit High #16, Medium #75): `IsSameTable`
  // must not conflate a real table `<tab>_1` with `<tab>`.
  {
    const std::string tab_suf = tab + "_1";
    if (RunUpdate(db, ctx, "CREATE TABLE " + tab_suf + " (x INT64);") &&
        RunUpdate(db, ctx, "INSERT INTO " + tab_suf + " VALUES (0), (1);")) {
      (void)ctx.txn_.PreCommit();
      ctx = db.BeginContext();
      const std::array<std::string, 2> suf_probes = {
          "SELECT " + tab + ".u, " + tab + ".a FROM " + tab + " LEFT JOIN " +
              tab_suf + " ON " + tab + ".a = " + tab_suf + ".x WHERE " + tab +
              ".b > 0;",
          "SELECT " + tab + ".u, " + tab + ".a FROM " + tab + " LEFT JOIN " +
              tab_suf + " ON " + tab + ".a = " + tab_suf + ".x WHERE " + tab +
              ".b IS NULL;",
      };
      for (const std::string& q_suf : suf_probes) {
        std::string err_opt, err_ref;
        auto r_opt =
            RunRowsMode(db, ctx, q_suf, /*force_relational=*/false, &err_opt);
        ScopedEnv bc("TINYLAMB_DISABLE_BYTECODE", "1");
        auto r_ref =
            RunRowsMode(db, ctx, q_suf, /*force_relational=*/true, &err_ref);
        if (r_opt.has_value() && r_ref.has_value()) {
          const std::vector<std::string> s_opt = SerializeSorted(*r_opt);
          const std::vector<std::string> s_ref = SerializeSorted(*r_ref);
          if (s_opt != s_ref) {
            *report += "[SUFFIXED-TABLE OUTER-JOIN MISMATCH]\n";
            *report += "  query: " + q_suf + "\n";
            *report += "  expected:\n" + DumpRows(s_ref);
            *report += "  actual:\n" + DumpRows(s_opt);
            return true;
          }
        }
      }
    }
  }

  return true;
}

// Value of a result cell as int64/kNull; kAnyValue for other types (arrays,
// strings) which the model does not pin.
namespace {

int64_t CellInt(const Value& v) {
  if (v.IsNull()) {
    return kNull;
  }
  if (v.type == ValueType::kInt64) {
    return v.value.int_value;
  }
  return kAnyValue;
}

// Window oracle: model-checked values on the base table, then identical
// results after (a) reinserting the rows in reversed physical order and
// (b) creating the trace's indexes: every probe orders by the unique key,
// so the answer must not depend on how the rows physically arrive.
}  // namespace

bool CheckWindow(Database& db, TransactionContext& ctx, const OracleTrace& t,
                 std::string* report, bool verbose) {
  if (t.window_probes.empty()) {
    return true;
  }
  const std::vector<MirrorRow> mirror = MirrorFromSetup(t.setup, t.table);
  const std::string tab2 = t.table + "w";
  auto substitute = [&t, &tab2](std::string sql) {
    size_t pos = 0;
    while ((pos = sql.find(t.table, pos)) != std::string::npos) {
      sql.replace(pos, t.table.size(), tab2);
      pos += tab2.size();
    }
    return sql;
  };
  std::vector<std::pair<std::string, std::vector<std::string>>> baseline;
  std::string error;
  for (const std::string& spec : t.window_probes) {
    int kind = 0;
    int64_t p1 = 0;
    int64_t p2 = 0;
    int arg = 0;
    int part = -1;
    int64_t flags = 0;
    int64_t def = 0;
    std::string sql;
    if (!ParseProbeSpec(spec, &kind, &p1, &p2, &arg, &part, &flags, &def,
                        &sql)) {
      continue;
    }
    auto rows = RunRows(db, ctx, sql, &error);
    if (!rows.has_value()) {
      // Unsupported syntax for this probe: skip it (other kinds keep
      // running).  The whole oracle reports a skip only if nothing ran.
      if (verbose) {
        std::cerr << "[sql_oracle][win-skip] " << error << " :: " << sql
                  << "\n";
      }
      continue;
    }
    std::vector<std::string> rendered;
    const std::map<int64_t, int64_t> expected =
        WindowModel(mirror, kind, p1, p2, arg, part, flags, def);
    for (const Row& r : *rows) {
      const int64_t u = CellInt(r[0]);
      const int64_t got = CellInt(r[1]);
      rendered.push_back(std::to_string(u) + "=" + std::to_string(got));
      auto it = expected.find(u);
      if (it != expected.end() && got != kAnyValue && it->second != got) {
        *report += "[WINDOW MODEL MISMATCH]\n";
        *report += "  sql: " + sql + "\n";
        *report += "  u=" + std::to_string(u) + " expected " +
                   (it->second == kNull ? "NULL" : std::to_string(it->second)) +
                   " got " + (got == kNull ? "NULL" : std::to_string(got)) +
                   "\n";
      }
    }
    baseline.emplace_back(sql, std::move(rendered));
  }
  if (baseline.empty()) {
    return false;  // every probe was rejected: oracle did not run
  }

  // Reversed physical order.
  TransactionContext ctx2 = db.BeginContext();
  std::vector<std::string> inserts;
  for (const std::string& sql : t.setup) {
    if (sql.starts_with("INSERT ")) {
      inserts.push_back(substitute(sql));
    } else if (sql.starts_with("CREATE TABLE")) {
      if (!RunUpdate(db, ctx2, substitute(sql))) {
        return true;
      }
    }
  }
  for (auto& insert : std::ranges::reverse_view(inserts)) {
    if (!RunUpdate(db, ctx2, insert)) {
      return true;
    }
  }
  std::vector<std::string> idx_ddl;
  idx_ddl.reserve(t.index_ddl.size());
  for (const std::string& ddl : t.index_ddl) {
    idx_ddl.push_back(substitute(ddl));
  }
  // Pass 1: permuted, no indexes; pass 2: permuted + indexed.
  for (int pass = 0; pass < 2; ++pass) {
    if (pass == 1) {
      for (const std::string& ddl : idx_ddl) {
        if (!ApplyIndexSpec(db, ctx2, ddl)) {
          break;
        }
      }
    }
    for (const auto& [sql, rendered] : baseline) {
      auto rows = RunRows(db, ctx2, substitute(sql), &error);
      if (!rows.has_value()) {
        if (verbose) {
          std::cerr << "[sql_oracle][win-perm-error] " << error << " :: " << sql
                    << "\n";
        }
        continue;
      }
      std::vector<std::string> got;
      for (const Row& r : *rows) {
        got.push_back(std::to_string(CellInt(r[0])) + "=" +
                      std::to_string(CellInt(r[1])));
      }
      if (got != rendered) {
        *report += std::string("[WINDOW ") +
                   (pass == 0 ? "PERMUTATION" : "INDEX") + " MISMATCH]\n";
        *report += "  sql: " + sql + "\n";
        *report += "  base: [";
        for (const std::string& s : rendered) {
          *report += s + " ";
        }
        *report += "]\n  alt:  [";
        for (const std::string& s : got) {
          *report += s + " ";
        }
        *report += "]\n";
      }
    }
  }
  return true;
}

// Skip-scan distinct: COUNT(DISTINCT c) / DISTINCT c with and without an
// index on c must agree, with and without a WHERE predicate.
bool CheckSkip(Database& db, TransactionContext& ctx, const OracleTrace& t,
               std::string* report, bool /*verbose*/) {
  if (t.skip_probes.empty() || t.setup.empty()) {
    return true;
  }
  const std::string tab2 = t.table + "k";
  auto substitute = [&t, &tab2](std::string sql) {
    size_t pos = 0;
    while ((pos = sql.find(t.table, pos)) != std::string::npos) {
      sql.replace(pos, t.table.size(), tab2);
      pos += tab2.size();
    }
    return sql;
  };
  auto render = [&](TransactionContext& c,
                    const std::string& sql) -> std::optional<std::string> {
    std::string error;
    auto rows = RunRows(db, c, sql, &error);
    if (!rows.has_value()) {
      return std::nullopt;
    }
    std::string out;
    for (const Row& r : *rows) {
      out += r.ToString() + ";";
    }
    return out;
  };
  if (!RunUpdate(db, ctx, substitute(t.setup[0])) ||
      !RunUpdate(db, ctx,
                 "INSERT INTO " + tab2 + " SELECT * FROM " + t.table + ";")) {
    return true;
  }
  std::vector<std::pair<std::string, std::string>> before;
  for (const std::string& probe : t.skip_probes) {
    auto out = render(ctx, substitute(probe));
    if (!out.has_value()) {
      return true;
    }
    before.emplace_back(probe, *out);
  }
  for (const char* col : {"a", "b"}) {
    if (!ApplyIndexSpec(db, ctx,
                        "CREATE INDEX idx_skip_" + std::string(col) + " ON " +
                            tab2 + "(" + col + ");")) {
      return true;
    }
  }
  for (const auto& [probe, expected] : before) {
    auto out = render(ctx, substitute(probe));
    if (!out.has_value()) {
      return true;
    }
    if (*out != expected) {
      *report += "[SKIP-SCAN MISMATCH]\n";
      *report += "  probe:    " + probe + "\n";
      *report += "  no-index: " + expected + "\n";
      *report += "  index:    " + *out + "\n";
    }
  }
  return true;
}

// TOP-n WITH TIES: rows past the n-th position whose ORDER BY key ties the
// boundary must all be present (set of u values, null-safe boundary).
bool CheckTopTies(Database& db, TransactionContext& ctx, const OracleTrace& t,
                  std::string* report, bool verbose) {
  if (t.topties.empty() || t.topties_limit <= 0) {
    return true;
  }
  const std::vector<MirrorRow> mirror = MirrorFromSetup(t.setup, t.table);
  std::vector<const MirrorRow*> sorted;
  sorted.reserve(mirror.size());
  for (const MirrorRow& m : mirror) {
    sorted.push_back(&m);
  }
  std::sort(
      sorted.begin(), sorted.end(), [](const MirrorRow* l, const MirrorRow* r) {
        if (l->a == kNull || r->a == kNull) {
          return l->a == kNull ? r->a != kNull : false;  // ASC NULLS FIRST
        }
        return l->a < r->a;
      });
  std::set<int64_t> expected;
  if (sorted.size() <= static_cast<size_t>(t.topties_limit)) {
    for (const MirrorRow* m : sorted) {
      expected.insert(m->u);
    }
  } else {
    const int64_t boundary =
        sorted[static_cast<size_t>(t.topties_limit) - 1]->a;
    for (const MirrorRow* m : sorted) {
      const bool keep = boundary == kNull ? m->a == kNull
                                          : (m->a == kNull || m->a <= boundary);
      if (keep) {
        expected.insert(m->u);
      }
    }
  }
  std::string error;
  auto rows = RunRows(db, ctx, t.topties, &error);
  if (!rows.has_value()) {
    if (verbose) {
      std::cerr << "[sql_oracle][topties-error] " << error << "\n";
    }
    return true;
  }
  std::set<int64_t> got;
  for (const Row& r : *rows) {
    got.insert(CellInt(r[0]));
  }
  if (got != expected) {
    *report += "[WITH TIES MISMATCH]\n";
    *report += "  sql: " + t.topties + "\n";
    *report += "  limit: " + std::to_string(t.topties_limit) + "\n";
    *report += "  expected u: ";
    for (const int64_t u : expected) {
      *report += std::to_string(u) + " ";
    }
    *report += "\n  got u:      ";
    for (const int64_t u : got) {
      *report += std::to_string(u) + " ";
    }
    *report += "\n";
  }
  return true;
}

// Transaction splitting: atomic execution must match autocommit execution.
bool CheckTroc(Database& db, const OracleTrace& t, std::string* report,
               bool verbose) {
  if (t.troc.empty() || t.troc_probe.empty()) {
    return true;
  }
  const std::string tab = t.table.empty() ? "t0" : t.table;
  auto substitute_to = [&tab](std::string sql, const std::string& suffix) {
    const std::string& from = tab;
    const std::string to = tab + suffix;
    size_t pos = 0;
    while ((pos = sql.find(from, pos)) != std::string::npos) {
      sql.replace(pos, from.size(), to);
      pos += to.size();
    }
    return sql;
  };
  auto substitute_a = [&](std::string sql) {
    return substitute_to(std::move(sql), "a");
  };
  auto substitute = [&](std::string sql) {
    return substitute_to(std::move(sql), "b");
  };

  auto is_primary_tab_sql = [&tab](const std::string& sql) {
    return sql.find(tab + "_j") == std::string::npos &&
           sql.find(tab + "_s") == std::string::npos;
  };

  std::string error;
  // Branch A: everything inside one transaction on shadow table <tab>a.
  TransactionContext ctx_a = db.BeginContext();
  for (const std::string& sql : t.setup) {
    if (!is_primary_tab_sql(sql)) {
      continue;
    }
    if (!RunUpdate(db, ctx_a, substitute_a(sql), &error)) {
      if (verbose) {
        std::cerr << "[sql_oracle][troc-setup-error] " << error << "\n";
      }
      return false;
    }
  }
  for (const std::string& sql : t.troc) {
    if (!RunUpdate(db, ctx_a, substitute_a(sql), &error)) {
      if (verbose) {
        std::cerr << "[sql_oracle][troc-stmt-error] " << sql << "\n";
      }
      return false;
    }
  }
  auto inside = RunRows(db, ctx_a, substitute_a(t.troc_probe), &error);
  bool committed = ctx_a.txn_.PreCommit() == Status::kSuccess;
  TransactionContext ctx_a2 = db.BeginContext();
  auto after_a = RunRows(db, ctx_a2, substitute_a(t.troc_probe), &error);
  if (!(inside.has_value() && after_a.has_value()) || !committed) {
    if (verbose) {
      std::cerr << "[sql_oracle][troc-error-a] " << error << "\n";
    }
    return false;
  }

  // Branch B: each statement in its own transaction (autocommit), on a
  // shadow table t0b so both branches start from the same pristine state.
  TransactionContext ctx_b = db.BeginContext();
  for (const std::string& sql : t.setup) {
    if (!is_primary_tab_sql(sql)) {
      continue;
    }
    if (!RunUpdate(db, ctx_b, substitute(sql))) {
      if (verbose) {
        std::cerr << "[sql_oracle][troc-setup-error-b]\n";
      }
      return false;
    }
  }
  ctx_b.txn_.PreCommit();
  for (const std::string& sql : t.troc) {
    TransactionContext ctx = db.BeginContext();
    if (!RunUpdate(db, ctx, substitute(sql))) {
      if (verbose) {
        std::cerr << "[sql_oracle][troc-stmt-error-b] " << sql << "\n";
      }
      return false;
    }
    ctx.txn_.PreCommit();
  }
  TransactionContext ctx_b2 = db.BeginContext();
  auto after_b = RunRows(db, ctx_b2, substitute(t.troc_probe), &error);
  if (!after_b.has_value()) {
    if (verbose) {
      std::cerr << "[sql_oracle][troc-error-b] " << error << "\n";
    }
    return false;
  }

  const std::string state_inside =
      inside->empty() ? "" : (*inside)[0].ToString();
  const std::string state_a = after_a->empty() ? "" : (*after_a)[0].ToString();
  const std::string state_b = after_b->empty() ? "" : (*after_b)[0].ToString();
  if (state_inside == state_a && state_a == state_b) {
    return true;
  }
  *report += "[TRANSACTION MISMATCH]\n";
  *report += "  probe:      " + t.troc_probe + "\n";
  *report += "  inside txn: " + state_inside + "\n";
  *report += "  committed:  " + state_a + "\n";
  *report += "  autocommit: " + state_b + "\n";
  return true;
}

// Statement-type transformation: DELETE must affect exactly COUNT(WHERE p).
bool CheckDqe(Database& db, TransactionContext& ctx, const OracleTrace& t,
              std::string* report, bool verbose) {
  if (t.dqe.size() != 3) {
    return true;
  }
  std::string error;
  auto count_p = RunScalar(db, ctx, t.dqe[0], &error);
  auto count_all = RunScalar(db, ctx, t.dqe[1], &error);
  if (!(count_p.has_value() && count_all.has_value())) {
    if (verbose) {
      std::cerr << "[sql_oracle][dqe-error] " << error << "\n";
    }
    return false;
  }
  if (!RunUpdate(db, ctx, t.dqe[2])) {
    if (verbose) {
      std::cerr << "[sql_oracle][dqe-delete-error] " << t.dqe[2] << "\n";
    }
    return false;
  }
  auto count_after = RunScalar(db, ctx, t.dqe[1], &error);
  if (!count_after.has_value()) {
    if (verbose) {
      std::cerr << "[sql_oracle][dqe-error] " << error << "\n";
    }
    return false;
  }
  const int64_t cp = std::stoll(*count_p);
  const int64_t ca = std::stoll(*count_all);
  const int64_t cr = std::stoll(*count_after);
  if (cr == ca - cp) {
    return true;
  }
  *report += "[DQE MISMATCH]\n";
  *report += "  " + t.dqe[0] + " => " + *count_p + "\n";
  *report += "  " + t.dqe[1] + " => " + *count_all + "\n";
  *report += "  " + t.dqe[2] + "\n";
  *report += "  remaining => " + *count_after + " (expected " +
             std::to_string(ca - cp) + ")\n";
  return true;
}

}  // namespace tinylamb
