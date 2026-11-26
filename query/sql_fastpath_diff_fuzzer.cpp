/**
 * Copyright 2026 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Differential SQL fuzzer targeting the newest fast execution paths:
//   - Columnar TLCS fast-path scan & aggregation (compacted .tlcs files)
//   - Fused grouped join & analytics factorized join
//   - Typed stream aggregate (dict / compact / direct / partitioned)
//   - Pipeline lowering + bytecode/JIT vs legacy + relational fallback + AST
//
// Builds identical fact/dim tables in two DBs (one compacted to TLCS, one
// uncompacted row-page DB), runs rich analytical SQL queries on both with
// opposite execution-path settings, and asserts exact result equivalence.

#include <fuzzer/FuzzedDataProvider.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <system_error>
#include <vector>

#include "common/platform/process.hpp"
#include "common/random_string.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

struct ScopedEnv {
  ScopedEnv(const char* name, const char* value) : name_(name) {
    if (const char* prev = platform::GetEnv(name)) {
      had_ = true;
      prev_ = prev;
    }
    if (value != nullptr) {
      platform::SetEnv(name, value);
    } else {
      platform::UnsetEnv(name);
    }
  }
  ~ScopedEnv() {
    if (had_) {
      platform::SetEnv(name_, prev_.c_str());
    } else {
      platform::UnsetEnv(name_);
    }
  }
  const char* name_;
  bool had_{false};
  std::string prev_;
};

std::string CanonicalCell(const Value& v) {
  if (v.IsNull()) {
    return "NULL";
  }
  if (v.type == ValueType::kDouble) {
    if (std::isnan(v.value.double_value)) {
      return "NaN";
    }
    if (std::isinf(v.value.double_value)) {
      return v.value.double_value > 0 ? "+Inf" : "-Inf";
    }
    double d = v.value.double_value == 0.0 ? 0.0 : v.value.double_value;
    // Round to 9 significant digits to avoid minor FP summation-order drift
    // between parallel/columnar and scalar row-order aggregation.
    std::ostringstream oss;
    oss << std::setprecision(9) << d;
    return oss.str();
  }
  return v.AsString();
}

std::string CanonicalRow(const Row& r) {
  std::string out;
  for (size_t i = 0; i < r.values_.size(); ++i) {
    if (i > 0) {
      out += " | ";
    }
    out += CanonicalCell(r[i]);
  }
  return out;
}

struct ExecOut {
  bool ok{false};
  std::vector<std::string> rows;
  std::string err;
};

ExecOut RunQuery(Database& db, const std::string& sql, bool legacy_fallback,
                 bool sort_rows) {
  ExecOut out;
  ScopedEnv e1("TINYLAMB_EXEC_ENGINE", legacy_fallback ? "legacy" : nullptr);
  ScopedEnv e2("TINYLAMB_DISABLE_BYTECODE", legacy_fallback ? "1" : nullptr);
  TransactionContext ctx = db.BeginReadOnlyContext();
  SqlEngine engine(db);
  engine.SetForceRelationalFallback(legacy_fallback);
  auto res = engine.Execute(ctx, sql);
  if (!res.HasValue()) {
    out.err = engine.LastError();
    std::ignore = ctx.Abort();
    return out;
  }
  try {
    Row row;
    while (res.Value().Next(&row)) {
      out.rows.push_back(CanonicalRow(row));
    }
    if (res.Value().GetStatus() != Status::kSuccess) {
      out.err = res.Value().GetStatus().GetMessage();
      std::ignore = ctx.Abort();
      return out;
    }
  } catch (const std::exception& e) {
    out.err = e.what();
    std::ignore = ctx.Abort();
    return out;
  }
  std::ignore = ctx.PreCommit();
  if (sort_rows) {
    std::sort(out.rows.begin(), out.rows.end());
  }
  out.ok = true;
  return out;
}

bool ExecDdlOrDml(Database& db, const std::string& sql) {
  TransactionContext ctx = db.BeginContext();
  SqlEngine engine(db);
  auto res = engine.Execute(ctx, sql);
  if (!res.HasValue()) {
    std::ignore = ctx.Abort();
    return false;
  }
  Row row;
  while (res.Value().Next(&row)) {
  }
  if (res.Value().GetStatus() != Status::kSuccess) {
    std::ignore = ctx.Abort();
    return false;
  }
  return ctx.PreCommit() == Status::kSuccess;
}

std::string RandPred(FuzzedDataProvider& fdp, const std::string& alias) {
  const int kind = fdp.ConsumeIntegralInRange<int>(0, 7);
  const std::string p = alias.empty() ? "" : alias + ".";
  if (kind == 0) {
    return p + "k >= " + std::to_string(fdp.ConsumeIntegralInRange<int>(-2, 6));
  }
  if (kind == 1) {
    return p + "k != " + std::to_string(fdp.ConsumeIntegralInRange<int>(0, 6));
  }
  if (kind == 2) {
    return p + "v IS NOT NULL";
  }
  if (kind == 3) {
    return p + "v IS NULL OR " + p + "v > " +
           std::to_string(fdp.ConsumeIntegralInRange<int>(-10, 10));
  }
  if (kind == 4) {
    return p + "tag IN ('a', 'bb', 'ccc', '')";
  }
  if (kind == 5) {
    return p + "tag LIKE 'a%'";
  }
  if (kind == 6) {
    return p + "k BETWEEN " +
           std::to_string(fdp.ConsumeIntegralInRange<int>(0, 3)) + " AND " +
           std::to_string(fdp.ConsumeIntegralInRange<int>(3, 8));
  }
  return "TRUE";
}

std::string BuildAnalyticalQuery(FuzzedDataProvider& fdp, bool* ordered_out) {
  *ordered_out = false;
  const int shape = fdp.ConsumeIntegralInRange<int>(0, 5);
  const std::string pred_f = RandPred(fdp, "f");
  const std::string pred_d = RandPred(fdp, "d");

  if (shape == 0) {
    // Single-table TLCS columnar fast-path GROUP BY + aggregates + HAVING
    const bool with_having = fdp.ConsumeBool();
    const bool with_distinct = fdp.ConsumeBool();
    std::string sql =
        "SELECT f.k, f.tag, COUNT(*), COUNT(f.v), SUM(f.v), MIN(f.v), MAX(f.v)";
    if (with_distinct) {
      sql += ", COUNT(DISTINCT f.v)";
    }
    sql += " FROM fact f WHERE " + pred_f + " GROUP BY f.k, f.tag";
    if (with_having) {
      sql += " HAVING COUNT(*) >= " +
             std::to_string(fdp.ConsumeIntegralInRange<int>(1, 3));
    }
    return sql;
  }
  if (shape == 1) {
    // Fused grouped join: fact JOIN dim1 (+ optional dim2) + GROUP BY + ORDER BY LIMIT
    const char* join_kw = fdp.ConsumeBool() ? "INNER JOIN" : "LEFT JOIN";
    std::string sql =
        "SELECT d.grp, COUNT(*), SUM(f.v), MIN(f.v), MAX(f.v) "
        "FROM fact f " +
        std::string(join_kw) + " dim1 d ON f.k = d.k WHERE " + pred_f +
        " AND (" + pred_d + " OR d.k IS NULL) GROUP BY d.grp";
    if (fdp.ConsumeBool()) {
      sql += " ORDER BY d.grp ASC LIMIT " +
             std::to_string(fdp.ConsumeIntegralInRange<int>(1, 10));
      *ordered_out = true;
    }
    return sql;
  }
  if (shape == 2) {
    // 3-way star join + factorized/fused grouped aggregate
    return "SELECT d1.grp, d2.cat, COUNT(*), SUM(f.v + d1.w), COUNT(DISTINCT f.tag) "
           "FROM fact f "
           "JOIN dim1 d1 ON f.k = d1.k "
           "LEFT JOIN dim2 d2 ON f.k2 = d2.k2 "
           "WHERE " +
           pred_f + " GROUP BY d1.grp, d2.cat";
  }
  if (shape == 3) {
    // Filtered aggregates + CASE + scalar aggregate over TLCS
    return "SELECT COUNT(*), "
           "SUM(CASE WHEN f.v > 0 THEN f.v ELSE 0 END), "
           "COUNT(DISTINCT f.k), "
           "MIN(f.tag), MAX(f.tag) "
           "FROM fact f WHERE " +
           pred_f;
  }
  if (shape == 4) {
    // Window + QUALIFY over join
    return "SELECT f.id, f.k, f.v, "
           "ROW_NUMBER() OVER (PARTITION BY f.k ORDER BY f.v DESC, f.id ASC) AS rn, "
           "SUM(f.v) OVER (PARTITION BY f.k) AS s "
           "FROM fact f WHERE " +
           pred_f +
           " QUALIFY ROW_NUMBER() OVER (PARTITION BY f.k ORDER BY f.v DESC, f.id ASC) <= " +
           std::to_string(fdp.ConsumeIntegralInRange<int>(1, 3));
  }
  // Set operation + EXISTS / NOT EXISTS
  const char* setop = fdp.ConsumeBool() ? "UNION ALL" : "EXCEPT DISTINCT";
  return "(SELECT f.k, f.tag FROM fact f WHERE EXISTS (SELECT 1 FROM dim1 d WHERE d.k = f.k AND " +
         pred_d + ")) " + setop +
         " (SELECT d.k, d.tag FROM dim1 d WHERE " + pred_d + ")";
}

}  // namespace
}  // namespace tinylamb

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  using namespace tinylamb;
  if (size < 8) {
    return 0;
  }
  FuzzedDataProvider fdp(data, size);

  const std::filesystem::path dir =
      std::filesystem::temp_directory_path() /
      ("fastpath_diff_" + RandomString(10));
  std::error_code ec;
  std::filesystem::create_directories(dir, ec);

  auto db_fast_res = Database::Create((dir / "fast").string(), 1);
  auto db_ref_res = Database::Create((dir / "ref").string(), 1);
  if (!db_fast_res.HasValue() || !db_ref_res.HasValue()) {
    std::filesystem::remove_all(dir, ec);
    return 0;
  }
  auto db_fast = db_fast_res.MoveValue();
  auto db_ref = db_ref_res.MoveValue();
  db_fast->SetSynchronousCommit(true);
  db_ref->SetSynchronousCommit(false);

  const std::vector<std::string> ddls = {
      "CREATE TABLE fact (id INT64, k INT64, k2 INT64, v INT64, tag VARCHAR)",
      "CREATE TABLE dim1 (k INT64, grp INT64, w INT64, tag VARCHAR)",
      "CREATE TABLE dim2 (k2 INT64, cat VARCHAR)",
  };
  for (const auto& ddl : ddls) {
    if (!ExecDdlOrDml(*db_fast, ddl) || !ExecDdlOrDml(*db_ref, ddl)) {
      db_fast.reset();
      db_ref.reset();
      std::filesystem::remove_all(dir, ec);
      return 0;
    }
  }

  const char* kTags[] = {"'a'", "'bb'", "'ccc'", "''", "'A'", "NULL"};
  auto val_or_null = [&](int lo, int hi) -> std::string {
    if (fdp.ConsumeIntegralInRange<int>(0, 6) == 0) {
      return "NULL";
    }
    return std::to_string(fdp.ConsumeIntegralInRange<int>(lo, hi));
  };

  auto insert_table = [&](const std::string& table_sql_prefix, int n,
                          auto make_tuple) {
    std::string sql = table_sql_prefix + " VALUES ";
    for (int i = 0; i < n; ++i) {
      if (i > 0) {
        sql += ", ";
      }
      sql += make_tuple(i);
    }
    assert(ExecDdlOrDml(*db_fast, sql));
    assert(ExecDdlOrDml(*db_ref, sql));
  };

  const int nfact = fdp.ConsumeIntegralInRange<int>(1, 40);
  const int ndim1 = fdp.ConsumeIntegralInRange<int>(1, 12);
  const int ndim2 = fdp.ConsumeIntegralInRange<int>(1, 8);

  insert_table("INSERT INTO fact", nfact, [&](int i) {
    return "(" + std::to_string(i) + ", " + val_or_null(0, 8) + ", " +
           val_or_null(0, 6) + ", " + val_or_null(-20, 20) + ", " +
           kTags[fdp.ConsumeIntegralInRange<size_t>(0, 5)] + ")";
  });
  insert_table("INSERT INTO dim1", ndim1, [&](int i) {
    return "(" + (fdp.ConsumeBool() ? std::to_string(i % 8) : val_or_null(0, 8)) +
           ", " + val_or_null(0, 4) + ", " + val_or_null(-5, 5) + ", " +
           kTags[fdp.ConsumeIntegralInRange<size_t>(0, 5)] + ")";
  });
  insert_table("INSERT INTO dim2", ndim2, [&](int i) {
    return "(" + (fdp.ConsumeBool() ? std::to_string(i % 6) : val_or_null(0, 6)) +
           ", " + kTags[fdp.ConsumeIntegralInRange<size_t>(0, 5)] + ")";
  });

  // Compact db_fast to TLCS so columnar_tlcs_fast_path + fused_grouped_join
  // engage!
  std::ignore = db_fast->CompactAllTablesToTlcs();

  const int nqueries = fdp.ConsumeIntegralInRange<int>(1, 4);
  for (int q = 0; q < nqueries && fdp.remaining_bytes() > 0; ++q) {
    bool ordered = false;
    const std::string sql = BuildAnalyticalQuery(fdp, &ordered);
    ExecOut fast_out =
        RunQuery(*db_fast, sql, /*legacy_fallback=*/false, /*sort_rows=*/!ordered);
    ExecOut ref_out =
        RunQuery(*db_ref, sql, /*legacy_fallback=*/true, /*sort_rows=*/!ordered);
    if (fast_out.ok != ref_out.ok || (fast_out.ok && fast_out.rows != ref_out.rows)) {
      std::fprintf(stderr,
                   "[SQL-FASTPATH-DIFF MISMATCH]\nSQL: %s\n"
                   "fast.ok=%d (rows=%zu err=%s)\nref.ok=%d (rows=%zu err=%s)\n",
                   sql.c_str(), fast_out.ok ? 1 : 0, fast_out.rows.size(),
                   fast_out.err.c_str(), ref_out.ok ? 1 : 0, ref_out.rows.size(),
                   ref_out.err.c_str());
      assert(false);
    }
  }

  db_fast.reset();
  db_ref.reset();
  std::filesystem::remove_all(dir, ec);
  return 0;
}
