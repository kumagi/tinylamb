/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "expression/expr_simplify_oracle.hpp"
#include "index/index_schema.hpp"
#include "plan/cascades.hpp"
#include "plan/plan_memo_oracle.hpp"
#include "query/expr_oracle_fuzzer.hpp"
#include "query/griffin_fuzzer.hpp"
#include "query/sql_engine.hpp"
#include "query/sql_oracle_fuzzer.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

struct Stats {
  std::atomic<uint64_t> total_iterations{0};
  std::atomic<uint64_t> norec_pushdown_runs{0};
  std::atomic<uint64_t> complex_memo_runs{0};
  std::atomic<uint64_t> complex_subset_runs{0};
  std::atomic<uint64_t> expr_simplify_runs{0};
  std::atomic<uint64_t> full_expr_oracle_runs{0};
  std::atomic<uint64_t> row_expr_null_reject_runs{0};
  std::atomic<uint64_t> sql_oracle_runs{0};
  std::atomic<uint64_t> amoeba_runs{0};
  std::atomic<uint64_t> griffin_runs{0};
  std::atomic<uint64_t> failures{0};
};

class PushdownDb {
 public:
  static std::unique_ptr<PushdownDb> Create(int thread_id) {
    std::string dbname =
        (std::filesystem::temp_directory_path() /
         ("rule_fuzz_db_t" + std::to_string(thread_id) + "_" +
          RandomString(8)))
            .string();
    auto db_res = Database::Create(dbname);
    if (!db_res.HasValue()) {
      return nullptr;
    }
    auto holder = std::make_unique<PushdownDb>(dbname, db_res.MoveValue());
    if (!holder->Setup()) {
      return nullptr;
    }
    return holder;
  }

  PushdownDb(std::string name, std::unique_ptr<Database> db)
      : name_(std::move(name)), db_(std::move(db)) {}

  ~PushdownDb() {
    db_.reset();
    std::error_code ec;
    std::filesystem::remove(name_ + ".log", ec);
    std::filesystem::remove(name_ + ".db", ec);
    std::filesystem::remove(name_ + ".last_checkpoint", ec);
  }

  Database& GetDb() { return *db_; }

 private:
  bool Setup() {
    TransactionContext ctx = db_->BeginContext();
    SqlEngine engine(*db_);

    const std::vector<std::string> setup_sqls = {
        "CREATE TABLE t1 (id INT64, val INT64, flag INT64, note VARCHAR(16));",
        "CREATE TABLE t2 (id INT64, t1_id INT64, val INT64, flag INT64, note "
        "VARCHAR(16));",
        "CREATE TABLE t3 (id INT64, t2_id INT64, val INT64, flag INT64, note "
        "VARCHAR(16));",
        // Rows for t1
        "INSERT INTO t1 VALUES (1, 10, 1, 'a1');",
        "INSERT INTO t1 VALUES (2, 20, 0, 'a2');",
        "INSERT INTO t1 VALUES (3, NULL, NULL, 'a3');",
        "INSERT INTO t1 VALUES (4, 40, 1, 'a4');",
        "INSERT INTO t1 VALUES (5, 0, 0, 'a5');",
        "INSERT INTO t1 VALUES (6, -10, 1, 'a6');",
        "INSERT INTO t1 VALUES (7, NULL, 0, 'a7');",
        "INSERT INTO t1 VALUES (8, 20, 1, 'a8');",
        // Rows for t2
        "INSERT INTO t2 VALUES (101, 1, 10, 1, 'b1');",
        "INSERT INTO t2 VALUES (102, 1, NULL, 0, 'b1_dup');",
        "INSERT INTO t2 VALUES (103, 2, 25, NULL, 'b2');",
        "INSERT INTO t2 VALUES (104, 3, 30, 1, 'b3');",
        "INSERT INTO t2 VALUES (105, 999, 50, 0, 'b_orphan');",
        "INSERT INTO t2 VALUES (106, NULL, 60, 1, 'b_nullfk');",
        "INSERT INTO t2 VALUES (107, 4, 0, 0, 'b4');",
        "INSERT INTO t2 VALUES (108, 4, -5, 1, 'b4_dup');",
        // Rows for t3
        "INSERT INTO t3 VALUES (201, 101, 100, 1, 'c1');",
        "INSERT INTO t3 VALUES (202, 101, 200, 0, 'c1_dup');",
        "INSERT INTO t3 VALUES (203, 103, NULL, NULL, 'c3');",
        "INSERT INTO t3 VALUES (204, 9999, 300, 1, 'c_orphan');",
        "INSERT INTO t3 VALUES (205, 104, 0, 1, 'c4');",
    };

    for (const auto& sql : setup_sqls) {
      StatusOr<QueryResult> res = engine.Execute(ctx, sql);
      if (!res.HasValue()) {
        std::cerr << "Setup failed on: " << sql
                  << " err: " << engine.LastError() << "\n";
        return false;
      }
      res.Value().Drain();
    }

    // Create indexes on tables for index scan / index join rules.
    auto add_idx = [&](std::string_view tbl, std::string_view idx_name,
                       std::vector<slot_t> cols,
                       IndexMode mode = IndexMode::kUnique) {
      Status s =
          db_->CreateIndex(ctx, tbl, IndexSchema(idx_name, cols, {}, mode));
      if (s != Status::kSuccess) {
        std::cerr << "Setup index failed on " << tbl << " (" << idx_name << ")\n";
      }
    };
    add_idx("t1", "t1_pk", {0}, IndexMode::kUnique);
    add_idx("t2", "t2_pk", {0}, IndexMode::kUnique);
    add_idx("t3", "t3_pk", {0}, IndexMode::kUnique);
    return true;
  }

  std::string name_;
  std::unique_ptr<Database> db_;
};

std::optional<int64_t> RunScalarCount(Database& db, TransactionContext& ctx,
                                      const std::string& sql,
                                      std::string* error) {
  SqlEngine engine(db);
  StatusOr<QueryResult> result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    if (error) *error = engine.LastError();
    return std::nullopt;
  }
  std::vector<Row> rows;
  Row row;
  while (result.Value().Next(&row)) {
    rows.push_back(row);
  }
  if (rows.size() != 1 || rows[0].Size() == 0) {
    if (error) *error = "expected 1 row, got " + std::to_string(rows.size());
    return std::nullopt;
  }
  const Value& val = rows[0][0];
  if (val.IsNull()) {
    return int64_t{0};
  }
  if (val.type == ValueType::kInt64) {
    return val.value.int_value;
  }
  if (val.type == ValueType::kDouble) {
    return static_cast<int64_t>(val.value.double_value);
  }
  return std::nullopt;
}

bool RunNoRecPushdownCheck(Database& db, std::mt19937_64& rng,
                           std::string* mismatch_report) {
  const int flavor = static_cast<int>(rng() % 25);
  std::string from_clause;
  std::string predicate;

  switch (flavor) {
    case 0: {
      // Flavour 0: Outer Join + Null-Allowing / Non-Null-Rejecting Predicate
      from_clause = "t1 LEFT JOIN t2 ON t1.id = t2.t1_id";
      static const std::array<const char*, 10> kPreds = {
          "t2.val IS NULL",
          "t2.t1_id IS NULL",
          "COALESCE(t2.val, 0) = 0",
          "(t2.val = 10 OR t2.val IS NULL)",
          "(t2.val IS NULL OR t2.val > 20)",
          "((t1.val > 10) AND (t2.val IS NULL))",
          "((t1.val = 20) OR (t2.val IS NULL))",
          "COALESCE(t2.flag, 1) = 1",
          "NOT (t2.val = 10)",
          "((t2.val IS NULL) OR (t2.flag = 1))",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 1: {
      // Flavour 1: Cross-Relation Disjunction (OR across relations)
      from_clause = (rng() % 2 == 0) ? "t1 JOIN t2 ON t1.id = t2.t1_id"
                                     : "t1 LEFT JOIN t2 ON t1.id = t2.t1_id";
      static const std::array<const char*, 7> kPreds = {
          "t1.val = 10 OR t2.val = 25",
          "t1.val > 20 OR t2.val < 10",
          "(t1.val = 10 AND t2.flag = 1) OR (t1.val = 20 AND t2.flag = 0)",
          "t1.flag = 1 OR t2.flag = 1",
          "t1.id = 1 OR t2.id = 103",
          "(t1.val IS NULL AND t2.val = 30) OR (t1.val = 20 AND t2.val IS NULL)",
          "t1.val = 0 OR t2.val = 0",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 2: {
      // Flavour 2: 3-Table Outer Join Chain Pushdown
      from_clause =
          "t1 LEFT JOIN t2 ON t1.id = t2.t1_id LEFT JOIN t3 ON t2.id = "
          "t3.t2_id";
      static const std::array<const char*, 7> kPreds = {
          "t3.val IS NULL",
          "t2.val IS NOT NULL AND t3.val IS NULL",
          "t1.val > 0 AND (t2.val IS NULL OR t3.val IS NULL)",
          "(t1.val = 10 AND t2.val = 10) OR t3.val = 100",
          "COALESCE(t3.val, 0) = 0",
          "t3.id IS NULL AND t2.id IS NOT NULL",
          "t1.flag = 1 AND (t2.flag IS NULL OR t3.flag = 1)",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 3: {
      // Flavour 3: Aggregation / GROUP BY / HAVING Pushdown
      from_clause =
          "(SELECT t1.id, t1.val, COUNT(t2.id) AS cnt FROM t1 LEFT JOIN t2 ON "
          "t1.id = t2.t1_id GROUP BY t1.id, t1.val) AS s";
      static const std::array<const char*, 4> kPreds = {
          "val > 10",
          "cnt > 1",
          "cnt = 0 OR cnt > 1",
          "val IS NULL AND cnt > 0",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 4: {
      // Flavour 4: Join + Limit Pushdown
      TransactionContext ctx = db.BeginReadOnlyContext();
      std::string opt_sql =
          "SELECT COUNT(*) FROM (SELECT t1.id, t2.id AS t2_id FROM t1 LEFT "
          "JOIN "
          "t2 ON t1.id = t2.t1_id LIMIT 3) AS s;";
      std::string ref_sql =
          "SELECT t1.id, t2.id AS t2_id FROM t1 LEFT JOIN t2 ON t1.id = "
          "t2.t1_id;";
      std::string err;
      auto opt_cnt = RunScalarCount(db, ctx, opt_sql, &err);
      if (!opt_cnt) return true;
      SqlEngine engine(db);
      StatusOr<QueryResult> ref_res = engine.Execute(ctx, ref_sql);
      if (!ref_res.HasValue()) return true;
      int64_t actual_rows = 0;
      Row r;
      while (ref_res.Value().Next(&r) && actual_rows < 3) {
        ++actual_rows;
      }
      if (*opt_cnt != actual_rows) {
        *mismatch_report =
            "[LIMIT PUSHDOWN MISMATCH]\n"
            "  Query: " +
            opt_sql + " => " + std::to_string(*opt_cnt) +
            "\n"
            "  Expected: " +
            std::to_string(actual_rows) + "\n";
        return false;
      }
      return true;
    }
    case 5: {
      // Flavour 5: Anti-Join / Subquery Pushdowns
      from_clause = "t1";
      static const std::array<const char*, 3> kPreds = {
          "t1.id NOT IN (SELECT t2.t1_id FROM t2 WHERE t2.val > 10)",
          "t1.id IN (SELECT t2.t1_id FROM t2 WHERE t2.flag = 1)",
          "t1.id NOT IN (SELECT t2.t1_id FROM t2 WHERE t2.t1_id IS NOT NULL)",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 6: {
      // Flavour 6: UNION ALL Pushdown
      from_clause =
          "(SELECT id, val, flag FROM t1 UNION ALL SELECT id, val, flag FROM "
          "t2) AS u";
      static const std::array<const char*, 4> kPreds = {
          "val = 10",
          "val IS NULL",
          "flag = 1 OR val > 20",
          "COALESCE(val, 0) = 0",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 7: {
      // Flavour 7: Randomized Composite AST Expression Attack
      from_clause = "t1 LEFT JOIN t2 ON t1.id = t2.t1_id";
      static const std::array<const char*, 4> kCols = {"t1.val", "t1.flag",
                                                       "t2.val", "t2.flag"};
      static const std::array<const char*, 4> kOps = {"=", "!=", ">", "<"};
      static const std::array<const char*, 4> kConsts = {"0", "1", "10", "20"};
      const int col_idx1 = static_cast<int>(rng() % kCols.size());
      const int col_idx2 = static_cast<int>(rng() % kCols.size());
      std::string part1 = std::string(kCols[col_idx1]) + " " +
                          kOps[rng() % kOps.size()] + " " +
                          kConsts[rng() % kConsts.size()];
      std::string part2 =
          (rng() % 2 == 0)
              ? (std::string(kCols[col_idx2]) + " IS NULL")
              : (std::string(kCols[col_idx2]) + " " +
                 kOps[rng() % kOps.size()] + " " +
                 kConsts[rng() % kConsts.size()]);
      const char* log_op = (rng() % 2 == 0) ? " AND " : " OR ";
      predicate = "(" + part1 + log_op + part2 + ")";
      break;
    }
    case 8: {
      // Flavour 8: Outer Join + IS NULL (Anti-Join trigger candidate)
      from_clause = "t1 LEFT JOIN t2 ON t1.id = t2.t1_id";
      static const std::array<const char*, 5> kPreds = {
          "t2.id IS NULL",
          "t2.t1_id IS NULL",
          "t2.id IS NULL AND t1.val > 10",
          "t2.id IS NULL OR t1.val = 20",
          "t2.id IS NOT NULL AND t2.val IS NULL",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 9: {
      // Flavour 9: Self-Join / Self-Join Elimination candidate
      from_clause = "t1 AS a JOIN t1 AS b ON a.id = b.id";
      static const std::array<const char*, 4> kPreds = {
          "a.val = b.val",
          "a.flag = b.flag AND a.val > 0",
          "a.val IS NULL OR b.val IS NULL",
          "a.flag = 1",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 10: {
      // Flavour 10: IN-list with NULLs and multi-element sets
      from_clause = "t1";
      static const std::array<const char*, 6> kPreds = {
          "t1.val IN (10, 20, 40)",
          "t1.val IN (10, NULL, 20)",
          "t1.val NOT IN (10, 20)",
          "t1.val NOT IN (10, NULL, 20)",
          "t1.flag IN (0, 1)",
          "t1.flag NOT IN (1)",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 11: {
      // Flavour 11: DISTINCT subquery with filters
      from_clause =
          "(SELECT DISTINCT t1.val, t1.flag FROM t1 LEFT JOIN t2 ON t1.id = "
          "t2.t1_id) AS d";
      static const std::array<const char*, 4> kPreds = {
          "val > 10",
          "val IS NULL",
          "flag = 1 OR val = 20",
          "COALESCE(val, 0) = 0",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 12: {
      // Flavour 12: Window functions (ROW_NUMBER, RANK) under filter
      from_clause =
          "(SELECT id, val, flag, ROW_NUMBER() OVER (PARTITION BY flag ORDER BY "
          "val) AS rn, RANK() OVER (PARTITION BY flag ORDER BY val) AS rk FROM "
          "t1) AS w";
      static const std::array<const char*, 5> kPreds = {
          "rn = 1",
          "rn <= 2",
          "rk = 1 AND flag = 1",
          "flag = 0 AND rn > 1",
          "rn = 1 OR rk = 2",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 13: {
      // Flavour 13: CASE WHEN and NULLIF conditional expressions
      from_clause = "t1 LEFT JOIN t2 ON t1.id = t2.t1_id";
      static const std::array<const char*, 5> kPreds = {
          "CASE WHEN t2.val IS NULL THEN t1.val ELSE t2.val END > 15",
          "CASE WHEN t1.flag = 1 THEN t2.flag ELSE 0 END = 1",
          "NULLIF(t1.val, t2.val) IS NULL",
          "NULLIF(t1.flag, 1) = 0",
          "COALESCE(t2.flag, t1.flag, 99) = 99",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 14: {
      // Flavour 14: 3-way join with mixed equalities and cross filters
      from_clause =
          "t1 JOIN t2 ON t1.id = t2.t1_id JOIN t3 ON t2.id = t3.t2_id";
      static const std::array<const char*, 4> kPreds = {
          "t1.val = t2.val",
          "t1.flag = 1 AND t3.val > 100",
          "t1.val + t2.val = 20",
          "t1.flag = t2.flag AND t2.flag = t3.flag",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 15: {
      // Flavour 15: Full Outer Join
      from_clause = "t1 FULL JOIN t2 ON t1.id = t2.t1_id";
      static const std::array<const char*, 4> kPreds = {
          "t1.id IS NULL OR t2.id IS NULL",
          "t1.val = t2.val",
          "COALESCE(t1.val, t2.val, 0) > 10",
          "t1.flag IS NOT NULL AND t2.flag IS NOT NULL",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 16: {
      // Flavour 16: IS DISTINCT FROM / IS NOT DISTINCT FROM
      from_clause = "t1 LEFT JOIN t2 ON t1.id = t2.t1_id";
      static const std::array<const char*, 4> kPreds = {
          "t1.val IS DISTINCT FROM t2.val",
          "t1.val IS NOT DISTINCT FROM 20",
          "t2.val IS DISTINCT FROM NULL",
          "t1.flag IS NOT DISTINCT FROM t2.flag",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 17: {
      // Flavour 17: Correlated Subquery EXISTS and NOT EXISTS
      from_clause = "t1";
      static const std::array<const char*, 4> kPreds = {
          "EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id AND t2.val > 10)",
          "NOT EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id)",
          "NOT EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id AND t2.val IS NULL)",
          "EXISTS (SELECT 1 FROM t2 JOIN t3 ON t2.id = t3.t2_id WHERE t2.t1_id = t1.id)",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 18: {
      // Flavour 18: BETWEEN and Arithmetic Range Checks
      from_clause = "t1 JOIN t2 ON t1.id = t2.t1_id";
      static const std::array<const char*, 4> kPreds = {
          "t1.val BETWEEN 10 AND 30",
          "t2.val BETWEEN t1.val - 5 AND t1.val + 5",
          "t1.val NOT BETWEEN 0 AND 20",
          "t1.val * 2 BETWEEN 20 AND 50",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 19: {
      // Flavour 19: Aggregation over Join with HAVING in Subquery
      from_clause =
          "(SELECT t1.id, SUM(t2.val) AS total_val, COUNT(t2.id) AS cnt FROM t1 "
          "LEFT JOIN t2 ON t1.id = t2.t1_id GROUP BY t1.id HAVING COUNT(t2.id) >= 1) AS agg";
      static const std::array<const char*, 4> kPreds = {
          "total_val > 20",
          "total_val IS NULL",
          "cnt = 1 OR total_val > 10",
          "total_val <= 30",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 20: {
      // Flavour 20: Window Function with Frames
      from_clause =
          "(SELECT id, val, flag, SUM(val) OVER (PARTITION BY flag ORDER BY id "
          "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS win_sum FROM t1) AS w";
      static const std::array<const char*, 4> kPreds = {
          "win_sum > 30",
          "win_sum IS NULL",
          "win_sum = val",
          "flag = 1 AND win_sum >= 20",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 21: {
      // Flavour 21: String Predicates with LIKE and SUBSTR
      from_clause = "t1 JOIN t2 ON t1.id = t2.t1_id";
      static const std::array<const char*, 4> kPreds = {
          "t1.note LIKE 'a%'",
          "t2.note LIKE '%dup'",
          "SUBSTR(t1.note, 1, 1) = 'a'",
          "t1.note = 'a1' OR t2.note = 'b2'",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 22: {
      // Flavour 22: Arithmetic Division / MOD with Protected Zero/Null
      from_clause = "t1";
      static const std::array<const char*, 3> kPreds = {
          "(CASE WHEN t1.val = 0 OR t1.val IS NULL THEN 0 ELSE 100 / t1.val END) > 2",
          "(CASE WHEN t1.val IS NULL THEN 0 ELSE MOD(t1.val, 3) END) = 1",
          "t1.val + 10 > 25",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    case 23: {
      // Flavour 23: Mixed Set Operation Derived Table
      from_clause =
          "(SELECT id, val FROM t1 WHERE val > 0 UNION ALL SELECT t1_id AS id, val FROM t2 WHERE val IS NOT NULL) AS s";
      static const std::array<const char*, 3> kPreds = {
          "val > 15",
          "id = 1",
          "val = 10 OR val = 20",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
    default: {
      // Flavour 24: Self-join with triangle conditions
      from_clause = "t1 AS a JOIN t1 AS b ON a.flag = b.flag";
      static const std::array<const char*, 3> kPreds = {
          "a.id < b.id AND a.val + b.val > 20",
          "a.id != b.id AND a.val = b.val",
          "a.val IS NOT NULL AND b.val IS NOT NULL AND a.val > b.val",
      };
      predicate = kPreds[rng() % kPreds.size()];
      break;
    }
  }

  TransactionContext ctx = db.BeginReadOnlyContext();
  const std::string opt_query =
      "SELECT COUNT(*) FROM " + from_clause + " WHERE " + predicate + ";";
  const std::string ref_query =
      "SELECT COALESCE(SUM(CASE WHEN " + predicate +
      " THEN 1 ELSE 0 END), 0) FROM " + from_clause + ";";

  std::string opt_err;
  std::string ref_err;
  auto opt_cnt = RunScalarCount(db, ctx, opt_query, &opt_err);
  auto ref_cnt = RunScalarCount(db, ctx, ref_query, &ref_err);

  if (!opt_cnt.has_value() || !ref_cnt.has_value()) {
    return true;
  }

  if (*opt_cnt != *ref_cnt) {
    *mismatch_report =
        "[NoREC PUSHDOWN MISMATCH]\n"
        "  Optimized: " +
        opt_query + " => " + std::to_string(*opt_cnt) +
        "\n"
        "  Reference: " +
        ref_query + " => " + std::to_string(*ref_cnt) +
        "\n"
        "  Flavor: " +
        std::to_string(flavor) + "\n";
    return false;
  }
  return true;
}

void Worker(int thread_id, uint64_t base_seed,
            std::atomic<bool>& stop_requested,
            std::chrono::steady_clock::time_point deadline, Stats& stats) {
  std::mt19937_64 rng(base_seed +
                      static_cast<uint64_t>(thread_id) * 1000003ULL);

  auto db_holder = PushdownDb::Create(thread_id);
  if (!db_holder) {
    std::cerr << "\n[FAILURE: Thread " << thread_id
              << " failed to initialize in-memory database]\n";
    stop_requested.store(true);
    stats.failures.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  Database& db = db_holder->GetDb();

  while (!stop_requested.load(std::memory_order_relaxed) &&
         std::chrono::steady_clock::now() < deadline) {
    const uint32_t pass = rng() % 9;
    if (pass == 0) {
      // Pass 0: Semantic Pushdown & NoREC Differential Execution Oracle
      std::string mismatch;
      if (!RunNoRecPushdownCheck(db, rng, &mismatch)) {
        std::cerr << "\n[FAILURE in Worker " << thread_id
                  << " (Semantic NoREC Pushdown Oracle)]: " << mismatch << "\n";
        stop_requested.store(true);
        stats.failures.fetch_add(1, std::memory_order_relaxed);
        break;
      }
      stats.norec_pushdown_runs.fetch_add(1, std::memory_order_relaxed);
    } else if (pass == 1) {
      // Pass 1: Complex Multi-Operator Memo Oracle (All 105 Rules)
      ComplexMemoGenConfig config;
      config.min_relations = 2 + static_cast<int>(rng() % 2);
      config.max_relations = config.min_relations + static_cast<int>(rng() % 2);
      config.max_conjuncts = static_cast<int>(rng() % 5);
      config.max_operator_depth = 2 + static_cast<int>(rng() % 3);

      std::mt19937 memo_rng(static_cast<uint32_t>(rng()));
      GeneratedComplexMemo gen = GenerateComplexMemo(memo_rng, config);
      const std::string problem = CheckComplexMemoEquivalence(gen);
      if (!problem.empty()) {
        std::cerr << "\n[FAILURE in Worker " << thread_id
                  << " (Complex Memo Oracle)]: " << problem << "\n";
        stop_requested.store(true);
        stats.failures.fetch_add(1, std::memory_order_relaxed);
        break;
      }
      stats.complex_memo_runs.fetch_add(1, std::memory_order_relaxed);
    } else if (pass == 2) {
      // Pass 2: Random Rule-Subset on Complex Memos
      ComplexMemoGenConfig config;
      config.min_relations = 2 + static_cast<int>(rng() % 2);
      config.max_relations = config.min_relations + static_cast<int>(rng() % 2);
      config.max_conjuncts = static_cast<int>(rng() % 5);
      config.max_operator_depth = 2 + static_cast<int>(rng() % 3);

      std::mt19937 memo_rng(static_cast<uint32_t>(rng()));
      GeneratedComplexMemo gen = GenerateComplexMemo(memo_rng, config);

      cascades::RuleSet rules = cascades::RuleSet::Default();
      std::vector<std::string> all_rules = rules.Names();
      if (all_rules.size() > 5) {
        const size_t num_to_drop =
            1 + (rng() % std::min<size_t>(10, all_rules.size() - 5));
        std::shuffle(all_rules.begin(), all_rules.end(), memo_rng);
        for (size_t i = 0; i < num_to_drop; ++i) {
          rules.Remove(all_rules[i]);
        }
      }

      cascades::Memo memo(128);
      cascades::GroupId root = 0;
      try {
        root = gen.builder(memo);
      } catch (const std::exception& e) {
        std::cerr << "\n[FAILURE in Worker " << thread_id
                  << " Complex Memo Build]: " << e.what() << "\n";
        stop_requested.store(true);
        stats.failures.fetch_add(1, std::memory_order_relaxed);
        break;
      }

      cascades::SearchEngine search(std::move(memo), rules);
      search.Explore(root);

      const std::string problem = CheckMemoInvariants(search.GetMemo(), false);
      if (!problem.empty()) {
        std::cerr << "\n[FAILURE in Worker " << thread_id
                  << " (Complex Subset Oracle)]: " << problem << "\n";
        stop_requested.store(true);
        stats.failures.fetch_add(1, std::memory_order_relaxed);
        break;
      }
      if (search.GetMemo().ExpressionCount(root) == 0) {
        std::cerr
            << "\n[FAILURE: explore left complex root empty with subset]\n";
        stop_requested.store(true);
        stats.failures.fetch_add(1, std::memory_order_relaxed);
        break;
      }
      stats.complex_subset_runs.fetch_add(1, std::memory_order_relaxed);
    } else if (pass == 3) {
      // Pass 3: Scalar Expression AST Simplification Oracle
      ExprGenConfig config;
      config.max_depth = 2 + static_cast<int>(rng() % 4);  // 2..5
      config.null_percent = static_cast<int>(rng() % 25);
      config.extended_ops = (rng() % 2 == 0);

      std::mt19937 expr_rng(static_cast<uint32_t>(rng()));
      GeneratedExpr gen = GenerateSimplifyExpr(expr_rng, config);
      const std::string problem = CheckSimplifyEquivalence(gen.expr);
      if (!problem.empty()) {
        std::cerr << "\n[FAILURE in Worker " << thread_id
                  << " (Expr Simplify Oracle)]: " << problem << "\n";
        stop_requested.store(true);
        stats.failures.fetch_add(1, std::memory_order_relaxed);
        break;
      }
      stats.expr_simplify_runs.fetch_add(1, std::memory_order_relaxed);
    } else if (pass == 4) {
      // Pass 4: Full Expression Engine vs AST Reference Oracle
      ExprGenConfig config;
      config.max_depth = 2 + static_cast<int>(rng() % 4);  // 2..5
      config.null_percent = static_cast<int>(rng() % 25);
      config.extended_ops = (rng() % 2 == 0);

      std::mt19937 expr_rng(static_cast<uint32_t>(rng()));
      ExprOracleTrace trace;
      const std::string problem =
          RunExprOracleIteration(expr_rng, false, &trace, config);
      if (!problem.empty()) {
        std::cerr << "\n[FAILURE in Worker " << thread_id
                  << " (Full Expr Engine vs AST Oracle)]: " << problem << "\n";
        stop_requested.store(true);
        stats.failures.fetch_add(1, std::memory_order_relaxed);
        break;
      }
      stats.full_expr_oracle_runs.fetch_add(1, std::memory_order_relaxed);
    } else if (pass == 5) {
      // Pass 5: Row-Aware NULL-Rejection & Differential Execution Oracle
      ExprGenConfig config;
      config.extended_ops = (rng() % 2 == 0);
      std::mt19937 row_rng(static_cast<uint32_t>(rng()));
      RowExprOracleTrace trace;
      const std::string problem =
          RunRowExprOracleIteration(row_rng, false, &trace, config);
      if (!problem.empty()) {
        std::cerr << "\n[FAILURE in Worker " << thread_id
                  << " (Row Expr Null-Reject / Differential Oracle)]: "
                  << problem << "\n";
        stop_requested.store(true);
        stats.failures.fetch_add(1, std::memory_order_relaxed);
        break;
      }
      stats.row_expr_null_reject_runs.fetch_add(1, std::memory_order_relaxed);
    } else if (pass == 6) {
      // Pass 6: SQL Oracle Metamorphic / Constraint Checks (TLP, PQS, IDX, DQE, TROC)
      std::mt19937 sql_rng(static_cast<uint32_t>(rng()));
      OracleIterationStats o_stats;
      OracleTrace trace;
      const std::string problem = RunOracleIteration(sql_rng, false, &o_stats, &trace);
      if (!problem.empty()) {
        std::cerr << "\n[FAILURE in Worker " << thread_id
                  << " (SQL Metamorphic Oracle)]: " << problem << "\n";
        stop_requested.store(true);
        stats.failures.fetch_add(1, std::memory_order_relaxed);
        break;
      }
      stats.sql_oracle_runs.fetch_add(1, std::memory_order_relaxed);
    } else if (pass == 7) {
      // Pass 7: AMOEBA Performance / Metamorphic Equivalence Oracle
      std::mt19937 amoeba_rng(static_cast<uint32_t>(rng()));
      const std::string problem = RunAmoebaIteration(amoeba_rng, false);
      if (!problem.empty()) {
        std::cerr << "\n[FAILURE in Worker " << thread_id
                  << " (AMOEBA Oracle)]: " << problem << "\n";
        stop_requested.store(true);
        stats.failures.fetch_add(1, std::memory_order_relaxed);
        break;
      }
      stats.amoeba_runs.fetch_add(1, std::memory_order_relaxed);
    } else {
      // Pass 8: Griffin Grammar-Free Metadata-Guided DBMS Oracle
      std::mt19937 griffin_rng(static_cast<uint32_t>(rng()));
      GriffinTrace trace;
      const std::string problem = RunGriffinIteration(griffin_rng, false, &trace);
      if (!problem.empty()) {
        std::cerr << "\n[FAILURE in Worker " << thread_id
                  << " (Griffin Oracle)]: " << problem << "\n";
        stop_requested.store(true);
        stats.failures.fetch_add(1, std::memory_order_relaxed);
        break;
      }
      stats.griffin_runs.fetch_add(1, std::memory_order_relaxed);
    }
    stats.total_iterations.fetch_add(1, std::memory_order_relaxed);
  }
}

}  // namespace
}  // namespace tinylamb

int main(int argc, char** argv) {
  int duration_sec = 1800;  // Default 30 minutes
  int num_threads = static_cast<int>(std::thread::hardware_concurrency());
  if (num_threads <= 0) num_threads = 4;

  int positional_idx = 0;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];
    if (arg.starts_with("--duration_sec=")) {
      duration_sec = std::atoi(arg.substr(15).data());
    } else if (arg.starts_with("--duration=")) {
      duration_sec = std::atoi(arg.substr(11).data());
    } else if (arg.starts_with("--threads=")) {
      num_threads = std::atoi(arg.substr(10).data());
    } else if (!arg.starts_with("-")) {
      if (positional_idx == 0) {
        duration_sec = std::atoi(arg.data());
        positional_idx++;
      } else if (positional_idx == 1) {
        num_threads = std::atoi(arg.data());
        positional_idx++;
      }
    }
  }

  if (const char* env_dur = std::getenv("FUZZ_DURATION_SEC")) {
    duration_sec = std::atoi(env_dur);
  }
  if (const char* env_threads = std::getenv("FUZZ_THREADS")) {
    num_threads = std::atoi(env_threads);
  }

  std::cout << "============================================================\n"
            << "Starting Ferocious TinyLamb Rule & Pushdown Fuzzer\n"
            << "Concurrency: " << num_threads << " threads\n"
            << "Duration:    " << duration_sec << " seconds ("
            << (duration_sec / 60) << "m " << (duration_sec % 60) << "s)\n"
            << "Oracles:     1. Semantic Execution Pushdown Oracle (NoREC)\n"
            << "             2. Complex Multi-Operator Cascades Memo (105 Rules)\n"
            << "             3. Complex Memo Random Rule-Subset Exploration\n"
            << "             4. Scalar Expression AST Simplification Oracle\n"
            << "             5. Full Expression Engine vs AST Reference Oracle\n"
            << "             6. Row-Aware NULL-Rejection & Differential Oracle\n"
            << "             7. SQL Metamorphic & Constraint Oracles (TLP/PQS/IDX/DQE/TROC)\n"
            << "             8. AMOEBA Performance / Metamorphic Equivalence Oracle\n"
            << "             9. Griffin Grammar-Free Metadata-Guided DBMS Oracle\n"
            << "============================================================\n"
            << std::flush;

  const auto start_time = std::chrono::steady_clock::now();
  const auto deadline = start_time + std::chrono::seconds(duration_sec);

  std::random_device rd;
  const uint64_t base_seed = (static_cast<uint64_t>(rd()) << 32) | rd();

  tinylamb::Stats stats;
  std::atomic<bool> stop_requested{false};

  std::vector<std::thread> workers;
  workers.reserve(num_threads);
  for (int t = 0; t < num_threads; ++t) {
    workers.emplace_back(tinylamb::Worker, t, base_seed,
                         std::ref(stop_requested), deadline, std::ref(stats));
  }

  uint64_t prev_total = 0;
  auto prev_time = start_time;

  while (!stop_requested.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) break;

    const auto elapsed_sec =
        std::chrono::duration_cast<std::chrono::seconds>(now - start_time)
            .count();
    const auto step_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(now -
                                                                  prev_time)
            .count();
    const uint64_t cur_total =
        stats.total_iterations.load(std::memory_order_relaxed);
    const double cur_rate = (cur_total - prev_total) / step_sec;
    prev_total = cur_total;
    prev_time = now;

    const auto norec_cnt =
        stats.norec_pushdown_runs.load(std::memory_order_relaxed);
    const auto complex_cnt =
        stats.complex_memo_runs.load(std::memory_order_relaxed);
    const auto subset_cnt =
        stats.complex_subset_runs.load(std::memory_order_relaxed);
    const auto expr_cnt =
        stats.expr_simplify_runs.load(std::memory_order_relaxed);
    const auto full_expr_cnt =
        stats.full_expr_oracle_runs.load(std::memory_order_relaxed);
    const auto row_cnt =
        stats.row_expr_null_reject_runs.load(std::memory_order_relaxed);
    const auto sql_cnt =
        stats.sql_oracle_runs.load(std::memory_order_relaxed);
    const auto amoeba_cnt =
        stats.amoeba_runs.load(std::memory_order_relaxed);
    const auto griffin_cnt =
        stats.griffin_runs.load(std::memory_order_relaxed);
    const auto fails = stats.failures.load(std::memory_order_relaxed);

    const int hh = static_cast<int>(elapsed_sec / 3600);
    const int mm = static_cast<int>((elapsed_sec % 3600) / 60);
    const int ss = static_cast<int>(elapsed_sec % 60);
    const int thh = duration_sec / 3600;
    const int tmm = (duration_sec % 3600) / 60;
    const int tss = duration_sec % 60;

    std::cout << "[" << std::setfill('0') << std::setw(2) << hh << ":"
              << std::setw(2) << mm << ":" << std::setw(2) << ss << " / "
              << std::setw(2) << thh << ":" << std::setw(2) << tmm << ":"
              << std::setw(2) << tss << "] "
              << "Total: " << cur_total << " (" << std::fixed
              << std::setprecision(1) << cur_rate << " it/s) | NoREC: "
              << norec_cnt << " | ComplexMemo: " << complex_cnt
              << " | ComplexSubset: " << subset_cnt << " | ExprRule: "
              << expr_cnt << " | FullExpr: " << full_expr_cnt
              << " | RowNull: " << row_cnt
              << " | SqlOracle: " << sql_cnt
              << " | Amoeba: " << amoeba_cnt
              << " | Griffin: " << griffin_cnt
              << " | Failures: " << fails << "\n"
              << std::flush;
  }

  stop_requested.store(true);
  for (auto& w : workers) {
    if (w.joinable()) {
      w.join();
    }
  }

  const auto end_time = std::chrono::steady_clock::now();
  const auto total_elapsed =
      std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time)
          .count();

  const uint64_t final_total = stats.total_iterations.load();
  const uint64_t final_norec = stats.norec_pushdown_runs.load();
  const uint64_t final_complex = stats.complex_memo_runs.load();
  const uint64_t final_subset = stats.complex_subset_runs.load();
  const uint64_t final_expr = stats.expr_simplify_runs.load();
  const uint64_t final_full_expr = stats.full_expr_oracle_runs.load();
  const uint64_t final_row = stats.row_expr_null_reject_runs.load();
  const uint64_t final_sql = stats.sql_oracle_runs.load();
  const uint64_t final_amoeba = stats.amoeba_runs.load();
  const uint64_t final_griffin = stats.griffin_runs.load();
  const uint64_t final_fails = stats.failures.load();

  const int f_hh = static_cast<int>(total_elapsed / 3600);
  const int f_mm = static_cast<int>((total_elapsed % 3600) / 60);
  const int f_ss = static_cast<int>(total_elapsed % 60);

  std::cout << "\n============================================================\n"
            << "Ferocious Rule Fuzzing Finished!\n"
            << "Elapsed:          " << f_hh << "h " << f_mm << "m "
            << f_ss << "s (" << total_elapsed << "s)\n"
            << "Total Iterations: " << final_total << "\n"
            << "  - Semantic NoREC Pushdown: " << final_norec << "\n"
            << "  - Complex Multi-Op Memo:   " << final_complex << "\n"
            << "  - Complex Rule Subsets:    " << final_subset << "\n"
            << "  - Expression Rule Simpl:   " << final_expr << "\n"
            << "  - Full Expression Engine:  " << final_full_expr << "\n"
            << "  - Row Null-Reject / Diff:  " << final_row << "\n"
            << "  - SQL Metamorphic Oracles: " << final_sql << "\n"
            << "  - AMOEBA Perf / Equiv:     " << final_amoeba << "\n"
            << "  - Griffin DBMS Oracle:     " << final_griffin << "\n"
            << "Total Failures:   " << final_fails << "\n"
            << "============================================================\n"
            << std::flush;

  return final_fails == 0 ? 0 : 1;
}
