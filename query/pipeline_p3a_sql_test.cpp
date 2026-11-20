/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <future>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/memory_budget.hpp"
#include "common/query_grant.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/pipeline/pipeline_ir.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

class EnvVarGuard {
 public:
  EnvVarGuard(const char* name, const char* value) : name_(name) {
    const char* prev = std::getenv(name);
    if (prev != nullptr) {
      had_prev_ = true;
      prev_ = prev;
    }
    if (value != nullptr) {
      setenv(name_, value, 1);
    } else {
      unsetenv(name_);
    }
  }
  EnvVarGuard(const EnvVarGuard&) = delete;
  EnvVarGuard& operator=(const EnvVarGuard&) = delete;
  EnvVarGuard(EnvVarGuard&&) = delete;
  EnvVarGuard& operator=(EnvVarGuard&&) = delete;
  ~EnvVarGuard() {
    if (had_prev_) {
      setenv(name_, prev_.c_str(), 1);
    } else {
      unsetenv(name_);
    }
  }

 private:
  const char* name_;
  bool had_prev_{false};
  std::string prev_;
};

struct OocKnobsResetGuard {
  OocKnobsResetGuard() = default;
  OocKnobsResetGuard(const OocKnobsResetGuard&) = delete;
  OocKnobsResetGuard& operator=(const OocKnobsResetGuard&) = delete;
  OocKnobsResetGuard(OocKnobsResetGuard&&) = delete;
  OocKnobsResetGuard& operator=(OocKnobsResetGuard&&) = delete;
  ~OocKnobsResetGuard() {
    ProcessMemoryBudget::Global().ResetTestKnobs();
    ProcessMemoryBudget::Global().ResetForTest(0);
  }
};

std::string SerializeRow(const Row& row) {
  std::ostringstream oss;
  for (size_t i = 0; i < row.Count(); ++i) {
    if (i > 0) {
      oss << '|';
    }
    const Value& v = row[i];
    if (v.IsNull()) {
      oss << "NULL";
    } else if (v.type == ValueType::kVarChar) {
      oss << std::string_view(v.value.varchar_value.data(),
                              v.value.varchar_value.size());
    } else {
      oss << v.AsString();
    }
  }
  return oss.str();
}

std::vector<std::string> SortedLines(const std::vector<Row>& rows) {
  std::vector<std::string> lines;
  lines.reserve(rows.size());
  for (const Row& r : rows) {
    lines.push_back(SerializeRow(r));
  }
  std::sort(lines.begin(), lines.end());
  return lines;
}

class PipelineP3aSqlTest : public ::testing::Test {
 public:
  void ExecDdlOrInsert(std::string_view sql) { (void)MustRun(sql); }

 protected:
  void SetUp() override {
    auto created = Database::Create("pipe_p3a_sql-" + RandomString());
    ASSERT_TRUE(created.HasValue()) << created.GetStatus();
    db_ = created.MoveValue();
    engine_ = std::make_unique<SqlEngine>(*db_);
  }

  StatusOr<std::vector<Row>> Run(std::string_view sql) {
    auto ctx = db_->BeginContext();
    ASSIGN_OR_RETURN(QueryResult, res, engine_->Execute(ctx, sql));
    std::vector<Row> rows = res.Collect();
    RETURN_IF_FAIL(res.GetStatus());
    RETURN_IF_FAIL(ctx.PreCommit());
    return rows;
  }

  std::vector<Row> MustRun(std::string_view sql) {
    auto res = Run(sql);
    EXPECT_TRUE(res.HasValue()) << sql << " failed: " << res.GetStatus() << " ("
                                << engine_->LastError() << ")";
    return res.HasValue() ? res.MoveValue() : std::vector<Row>{};
  }

  std::string ExplainText(std::string_view sql) {
    std::vector<Row> rows = MustRun(sql);
    std::string joined;
    for (size_t i = 0; i < rows.size(); ++i) {
      if (i > 0) {
        joined.push_back('\n');
      }
      joined += SerializeRow(rows[i]);
    }
    return joined;
  }

  std::shared_ptr<Database> db_;
  std::unique_ptr<SqlEngine> engine_;
};

void PopulateJoinAggDataset(PipelineP3aSqlTest* t, size_t fact_rows,
                            size_t dim_rows) {
  t->ExecDdlOrInsert(
      "CREATE TABLE dim (d_id INT64 PRIMARY KEY, region INT64, d_pad "
      "VARCHAR);");
  t->ExecDdlOrInsert(
      "CREATE TABLE fact (f_id INT64 PRIMARY KEY, f_key INT64, amount "
      "INT64, f_pad VARCHAR);");

  const std::string pad(120, 'P');
  {
    std::string sql = "INSERT INTO dim VALUES ";
    for (size_t i = 0; i < dim_rows; ++i) {
      if (i > 0) {
        sql += ',';
      }
      sql += "(" + std::to_string(i + 1) + "," + std::to_string((i % 7) + 1) +
             ",'" + pad + "')";
    }
    t->ExecDdlOrInsert(sql);
  }
  constexpr size_t kChunk = 500;
  for (size_t start = 0; start < fact_rows; start += kChunk) {
    const size_t end = std::min(fact_rows, start + kChunk);
    std::string sql = "INSERT INTO fact VALUES ";
    for (size_t i = start; i < end; ++i) {
      if (i > start) {
        sql += ',';
      }
      // Heavy skew: 50% join key = 1, rest distributed across dim_rows and a
      // few unmatched keys (`dim_rows + 5`).
      const size_t fk = (i % 2 == 0) ? 1 : ((i % (dim_rows + 3)) + 1);
      const size_t amt = (i % 31) + 1;
      sql += "(" + std::to_string(i + 1) + "," + std::to_string(fk) + "," +
             std::to_string(amt) + ",'" + pad + "')";
    }
    t->ExecDdlOrInsert(sql);
  }
}

TEST_F(PipelineP3aSqlTest, GroupedMinMaxPoisonsNanLikeCanonical) {
  // GoogleSQL: any NaN in the group makes MIN/MAX NaN. The typed partitioned
  // aggregate used to keep only the stored extreme, so MAX silently returned
  // a finite value for a group containing NaN.
  ExecDdlOrInsert("CREATE TABLE g_nan (g INT64, f FLOAT64);");
  ExecDdlOrInsert(
      "INSERT INTO g_nan VALUES "
      "(1, 1.5), (1, CAST('NaN' AS FLOAT64)), (1, -2.5), "
      "(2, 3.0), (2, 4.0)");
  const auto rows =
      MustRun("SELECT g, MIN(f), MAX(f) FROM g_nan GROUP BY g ORDER BY g");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
  ASSERT_TRUE(std::isnan(rows[0][1].value.double_value)) << rows[0][1];
  ASSERT_TRUE(std::isnan(rows[0][2].value.double_value)) << rows[0][2];
  EXPECT_EQ(rows[1][0], Value(int64_t{2}));
  EXPECT_EQ(rows[1][1], Value(3.0));
  EXPECT_EQ(rows[1][2], Value(4.0));
}

TEST_F(PipelineP3aSqlTest, GroupedDistinctOrderByMatchesRelationalLane) {
  // DISTINCT + ORDER BY on the grouped OOC lane picked a different survivor
  // representative for groups sharing the visible prefix than the relational
  // lane's first-occurrence fold (the hidden order payload of the survivor
  // decides the final sort position), inverting the row order between lanes.
  // The combination now delegates to the relational path.
  ExecDdlOrInsert("CREATE TABLE gd (k INT64, v INT64);");
  ExecDdlOrInsert(
      "INSERT INTO gd VALUES (2, 30), (2, 10), (1, 5), (1, 20), (3, 7)");
  const auto rows =
      MustRun("SELECT DISTINCT k FROM gd GROUP BY k, v ORDER BY SUM(v) DESC");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(int64_t{2}));
  EXPECT_EQ(rows[1][0], Value(int64_t{1}));
  EXPECT_EQ(rows[2][0], Value(int64_t{3}));
}

TEST_F(PipelineP3aSqlTest, G3AndG4ParityUnderSpillAndNormalVsLegacy) {
  OocKnobsResetGuard reset_knobs;
  PopulateJoinAggDataset(this, /*fact_rows=*/2500, /*dim_rows=*/400);

  const std::vector<std::string> order_sensitive_queries = {
      // Join + GroupBy + Having + OrderBy + Limit (Top-N)
      "SELECT d.region, COUNT(*) AS cnt, SUM(f.amount) AS total_amt "
      "FROM fact f JOIN dim d ON f.f_key = d.d_id "
      "GROUP BY d.region HAVING COUNT(*) >= 10 "
      "ORDER BY total_amt DESC, d.region ASC LIMIT 5;",
      // Single-table ORDER BY + OFFSET + LIMIT
      "SELECT f_id, f_key, amount FROM fact WHERE amount >= 5 "
      "ORDER BY f_key ASC, amount DESC, f_id ASC LIMIT 40 OFFSET 25;",
      // Single-table GROUP BY + HAVING + ORDER BY
      "SELECT f_key, COUNT(*) AS c, MIN(amount) AS mn, MAX(amount) AS mx, "
      "SUM(amount) AS sm FROM fact GROUP BY f_key HAVING SUM(amount) > 20 "
      "ORDER BY sm DESC, f_key ASC LIMIT 20;",
      // Scalar aggregate
      "SELECT COUNT(*), SUM(amount), MIN(f_key), MAX(f_id) FROM fact;",
      // COUNT(DISTINCT) with heavy skew + ORDER BY
      "SELECT f_key, COUNT(DISTINCT amount) AS ucnt FROM fact "
      "GROUP BY f_key ORDER BY f_key ASC LIMIT 15;",
  };

  const std::vector<std::string> unordered_queries = {
      // Inner join with skewed key
      "SELECT f.f_id, d.d_id, f.amount FROM fact f JOIN dim d "
      "ON f.f_key = d.d_id WHERE f.f_id <= 300;",
      // Left outer join preserving unmatched fact keys
      "SELECT f.f_id, d.d_id FROM fact f LEFT JOIN dim d ON f.f_key = d.d_id "
      "WHERE f.f_id > 2100;",
      // Right outer join preserving unmatched dim keys
      "SELECT f.f_id, d.d_id FROM fact f RIGHT JOIN dim d ON f.f_key = d.d_id "
      "WHERE d.d_id > 350 OR d.d_id = 2;",
      // Full outer join
      "SELECT f.f_id, d.d_id FROM fact f FULL OUTER JOIN dim d "
      "ON f.f_key = d.d_id WHERE f.f_key IS NULL OR d.d_id >= 390;",
      // DISTINCT over high-duplicate column
      "SELECT DISTINCT f_key, amount FROM fact;",
  };

  auto capture_all = [&](const char* mode_env, size_t op_budget,
                         bool always_spill) {
    EnvVarGuard mode_guard("TINYLAMB_EXEC_ENGINE", mode_env);
    ProcessMemoryBudget::Global().SetTestOpMemoryBytes(op_budget);
    ProcessMemoryBudget::Global().SetTestAlwaysSpill(always_spill);

    std::pair<std::vector<std::vector<std::string>>,
              std::vector<std::vector<std::string>>>
        out;
    for (const std::string& q : order_sensitive_queries) {
      std::vector<std::string> lines;
      for (const Row& r : MustRun(q)) {
        lines.push_back(SerializeRow(r));
      }
      out.first.push_back(std::move(lines));
    }
    for (const std::string& q : unordered_queries) {
      out.second.push_back(SortedLines(MustRun(q)));
    }
    return out;
  };

  // Baseline: legacy execution engine.
  const auto legacy_res = capture_all("legacy", 0, false);
  // Pipeline default (normal memory budget).
  const auto pipe_normal = capture_all("pipeline", 0, false);
  // G4: Pipeline under strict AlwaysSpill.
  const auto pipe_always_spill = capture_all("pipeline", 64ULL << 10, true);
  // G3: results must match legacy under both a 64 KiB always-spill budget and
  // both match.
  const auto pipe_1mib = capture_all("pipeline", 1ULL << 20, false);

  for (size_t i = 0; i < order_sensitive_queries.size(); ++i) {
    EXPECT_EQ(pipe_normal.first[i], legacy_res.first[i])
        << "ordered normal mismatch on: " << order_sensitive_queries[i];
    EXPECT_EQ(pipe_always_spill.first[i], legacy_res.first[i])
        << "ordered AlwaysSpill mismatch on: " << order_sensitive_queries[i];
    EXPECT_EQ(pipe_1mib.first[i], legacy_res.first[i])
        << "ordered 1MiB mismatch on: " << order_sensitive_queries[i];
  }
  for (size_t i = 0; i < unordered_queries.size(); ++i) {
    EXPECT_EQ(pipe_normal.second[i], legacy_res.second[i])
        << "unordered normal mismatch on: " << unordered_queries[i];
    EXPECT_EQ(pipe_always_spill.second[i], legacy_res.second[i])
        << "unordered AlwaysSpill mismatch on: " << unordered_queries[i];
    EXPECT_EQ(pipe_1mib.second[i], legacy_res.second[i])
        << "unordered 1MiB mismatch on: " << unordered_queries[i];
  }
}

TEST_F(PipelineP3aSqlTest, ExplainAnalyzeShowsPipelineMetricsAndZeroLegacy) {
  OocKnobsResetGuard reset_knobs;
  PopulateJoinAggDataset(this, /*fact_rows=*/1500, /*dim_rows=*/200);
  EnvVarGuard mode_guard("TINYLAMB_EXEC_ENGINE", "pipeline");
  ProcessMemoryBudget::Global().SetTestOpMemoryBytes(32ULL << 10);

  const std::string explain_pipe = ExplainText(
      "EXPLAIN PIPELINE "
      "SELECT d.region, COUNT(*), SUM(f.amount) AS s "
      "FROM fact f JOIN dim d ON f.f_key = d.d_id "
      "GROUP BY d.region ORDER BY s DESC;");
  EXPECT_NE(explain_pipe.find("Stage 0"), std::string::npos) << explain_pipe;
  EXPECT_NE(explain_pipe.find("Pipeline:"), std::string::npos) << explain_pipe;

  const std::string analyze = ExplainText(
      "EXPLAIN ANALYZE "
      "SELECT d.region, COUNT(*), SUM(f.amount) AS s "
      "FROM fact f JOIN dim d ON f.f_key = d.d_id "
      "GROUP BY d.region ORDER BY s DESC;");
  EXPECT_NE(analyze.find("Pipeline:"), std::string::npos) << analyze;
  EXPECT_NE(analyze.find("legacy_source_ms=0"), std::string::npos) << analyze;
  EXPECT_NE(analyze.find("Memory:"), std::string::npos) << analyze;
  EXPECT_NE(analyze.find("OOC:"), std::string::npos) << analyze;
  // The aggregate is now pushed below the join ("Aggregate below Join"), so
  // the join builds a handful of pre-aggregated rows and fits the 32 KiB op
  // budget without spilling. Only the reporting contract is pinned here:
  // the pipeline line carries its spill counter.
  EXPECT_NE(analyze.find("Aggregate below Join"), std::string::npos)
      << analyze;
  EXPECT_NE(analyze.find("spill_bytes="), std::string::npos) << analyze;
}

TEST_F(PipelineP3aSqlTest, G5TempLimitFaultAndMemoryCoordinatorSharing) {
  OocKnobsResetGuard reset_knobs;
  PopulateJoinAggDataset(this, /*fact_rows=*/1200, /*dim_rows=*/100);
  EnvVarGuard mode_guard("TINYLAMB_EXEC_ENGINE", "pipeline");
  ProcessMemoryBudget::Global().SetTestOpMemoryBytes(16ULL << 10);
  ProcessMemoryBudget::Global().SetTestTempLimitBytes(64ULL << 10);

  // Forced spill over 64 KiB temp limit must cleanly fail with kNoSpace!
  const auto res =
      Run("SELECT f_id, f_pad FROM fact ORDER BY f_key DESC, f_id ASC;");
  ASSERT_FALSE(res.HasValue());
  EXPECT_EQ(res.GetStatus().GetCode(), StatusCode::kNoSpace);

  // Clearing the temp limit lets a subsequent query succeed; spill/grant
  // leak-freedom of the failed query is not asserted by this check.
  ProcessMemoryBudget::Global().SetTestTempLimitBytes(0);
  const auto ok =
      Run("SELECT COUNT(*) FROM fact f JOIN dim d ON f.f_key = d.d_id;");
  ASSERT_TRUE(ok.HasValue()) << ok.GetStatus();
  EXPECT_EQ(ok.Value().size(), 1U);
  EXPECT_GT(ok.Value()[0][0].AsInt64(), 1000);

  // Verify G5 QueryMemoryCoordinator elastic admission & kMemoryLimitExceeded
  // on exhaustion cleanly propagates through SQL execution.
  ProcessMemoryBudget& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(/*limit_bytes=*/2ULL << 20, /*storage_limit_bytes=*/0,
                      /*columnar_limit_bytes=*/0, /*check_rss_pressure=*/false);
  const size_t qmax = budget.EffectivePoolMaxBytes(MemoryPoolKind::kQuery);
  ASSERT_GT(qmax, 64ULL << 10);
  // Fill the query pool so less than the 64 KiB blocking minimum remains.
  std::vector<QueryMemoryGrant> held_grants;
  while (true) {
    const size_t avail = budget.Available();
    const size_t chunk = std::min<size_t>(avail, qmax / 2);
    if (chunk < (16ULL << 10)) {
      break;
    }
    auto g = QueryMemoryCoordinator::Global().AcquireGrant(
        chunk, 16ULL << 10, /*wait_timeout_ms=*/0);
    if (!g.HasValue()) {
      break;
    }
    held_grants.push_back(std::move(g.Value()));
  }
  ASSERT_FALSE(held_grants.empty());
  // Exhausted query pool must fail end-to-end SQL with kMemoryLimitExceeded
  // (mapped to PG SQLSTATE 53200).
  EnvVarGuard wait_guard("TINYLAMB_GRANT_WAIT_TIMEOUT_MS", "10");
  const auto oom_sql =
      Run("SELECT f_id FROM fact ORDER BY f_key DESC, f_id ASC;");
  ASSERT_FALSE(oom_sql.HasValue());
  EXPECT_EQ(oom_sql.GetStatus().GetCode(), StatusCode::kMemoryLimitExceeded);
  // Verify DOP scaling responds to tight operator budget.
  EXPECT_EQ(QueryMemoryGrant::ComputeEffectiveDop(8, 1ULL << 20, 512ULL << 10),
            2U);
  held_grants.clear();
  const auto ok_after =
      Run("SELECT f_id FROM fact ORDER BY f_key DESC, f_id ASC LIMIT 3;");
  ASSERT_TRUE(ok_after.HasValue()) << ok_after.GetStatus();
  EXPECT_EQ(ok_after.Value().size(), 3U);
}

TEST_F(PipelineP3aSqlTest, G7ConcurrentSpillingSqlQueriesStayIsolated) {
  OocKnobsResetGuard reset_knobs;
  PopulateJoinAggDataset(this, /*fact_rows=*/1500, /*dim_rows=*/150);
  EnvVarGuard mode_guard("TINYLAMB_EXEC_ENGINE", "pipeline");
  ProcessMemoryBudget::Global().SetTestOpMemoryBytes(32ULL << 10);

  const std::string sql =
      "SELECT d.region, COUNT(*) AS cnt, SUM(f.amount) AS total_amt "
      "FROM fact f JOIN dim d ON f.f_key = d.d_id "
      "GROUP BY d.region ORDER BY total_amt DESC, d.region ASC;";
  const std::vector<Row> expected = MustRun(sql);
  ASSERT_FALSE(expected.empty());

  constexpr int kThreads = 4;
  std::vector<std::future<StatusOr<std::vector<Row>>>> futures;
  futures.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    futures.push_back(
        std::async(std::launch::async, [&]() -> StatusOr<std::vector<Row>> {
          SqlEngine local_engine(*db_);
          auto ctx = db_->BeginContext();
          ASSIGN_OR_RETURN(QueryResult, res, local_engine.Execute(ctx, sql));
          std::vector<Row> rows = res.Collect();
          RETURN_IF_FAIL(res.GetStatus());
          RETURN_IF_FAIL(ctx.PreCommit());
          return rows;
        }));
  }

  for (auto& fut : futures) {
    auto r = fut.get();
    ASSERT_TRUE(r.HasValue()) << r.GetStatus();
    EXPECT_EQ(SortedLines(r.Value()), SortedLines(expected));
  }
}

// sql_oracle_fuzzer under TINYLAMB_ALWAYS_SPILL: the optimizer serves
// `SELECT DISTINCT a ... ORDER BY a` with a HashDistinct over an ordered
// IndexOnlyScan and no Sort, trusting the distinct to keep its input order.
// The spilled distinct partitioned by hash and returned [-2];[NULL];[0]...
TEST_F(PipelineP3aSqlTest, SpilledDistinctKeepsTheOrderTheOptimizerRelies) {
  ExecDdlOrInsert(
      "CREATE TABLE t (u INT64, a INT64, flag BOOL, s VARCHAR(8));");
  ExecDdlOrInsert(
      "INSERT INTO t VALUES (0, 3, NULL, ''), (1, NULL, FALSE, ''), "
      "(2, 2, FALSE, ''), (3, 3, TRUE, 'a'), (4, 1, NULL, ''), "
      "(5, 0, NULL, ''), (6, -2, FALSE, 'a');");
  ExecDdlOrInsert("CREATE INDEX t_af ON t (a, flag);");
  const std::string sql = "SELECT DISTINCT a FROM t ORDER BY a;";
  const auto run = [&]() {
    std::vector<std::string> lines;
    for (const Row& r : MustRun(sql)) {
      lines.push_back(SerializeRow(r));
    }
    return lines;
  };
  const std::vector<std::string> normal = run();
  ASSERT_EQ(normal.size(), 6U);
  ProcessMemoryBudget::Global().SetTestAlwaysSpill(true);
  const std::vector<std::string> spilled = run();
  ProcessMemoryBudget::Global().SetTestAlwaysSpill(false);
  EXPECT_EQ(spilled, normal);
}

}  // namespace
}  // namespace tinylamb

// Blocking stages drive every sink with a single worker, so the reported DOP
// must be 1 even when the grant would permit more.
TEST(PipelineContextDopTest, ReportsTheWorkerCountActuallyUsed) {
  tinylamb::PipelineContext ctx(nullptr, tinylamb::QueryMemoryGrant(), 4);
  const tinylamb::PipelineExecutionMetrics m = ctx.Snapshot();
  EXPECT_EQ(m.effective_dop, 1U);
  EXPECT_EQ(m.permitted_dop, 4U);
}
