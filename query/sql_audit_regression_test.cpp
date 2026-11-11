/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <gtest/gtest.h>

#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/explain_format.hpp"
#include "executor/unnest.hpp"
#include "executor/values.hpp"
#include "expression/expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/sql_engine.hpp"
#include "type/date.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {
class SqlAuditRegressionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto created = Database::Create("sql_audit-" + RandomString());
    ASSERT_TRUE(created.HasValue()) << created.GetStatus();
    database_ = created.MoveValue();
    engine_ = std::make_unique<SqlEngine>(*database_);
  }
  StatusOr<std::vector<Row>> Run(std::string_view sql) {
    auto context = database_->BeginContext();
    ASSIGN_OR_RETURN(QueryResult, result, engine_->Execute(context, sql));
    auto rows = result.Collect();
    RETURN_IF_FAIL(result.GetStatus());
    RETURN_IF_FAIL(context.PreCommit());
    return rows;
  }
  std::vector<Row> Rows(std::string_view sql) {
    auto result = Run(sql);
    EXPECT_TRUE(result.HasValue())
        << sql << ": " << result.GetStatus() << ": " << engine_->LastError();
    return result.HasValue() ? result.MoveValue() : std::vector<Row>{};
  }
  std::unique_ptr<Database> database_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(SqlAuditRegressionTest, EmptyJoinRollupPreservesGrandTotalWithHaving) {
  Rows("CREATE TABLE audit_rollup (a INT64, b INT64, s STRING)");
  Rows("INSERT INTO audit_rollup VALUES (0,-2,NULL),(2,-1,NULL),(-3,-1,'a')");
  Rows("CREATE TABLE audit_rollup_j (x INT64, y STRING)");
  Rows("INSERT INTO audit_rollup_j VALUES (3,NULL),(-2,'a')");
  for (bool force_relational : {false, true}) {
    SCOPED_TRACE(force_relational);
    engine_->SetForceRelationalFallback(force_relational);
    EXPECT_EQ(Rows(R"sql(
      SELECT a,COALESCE(y,'none'),COUNT(*),
             SUM(CASE WHEN NOT (s IS NOT NULL) THEN b ELSE 0 END),
             MIN(a),MAX(NULLIF(b,-1))
      FROM audit_rollup JOIN audit_rollup_j ON a=x
      GROUP BY ROLLUP(1,2) HAVING COUNT(*) >= 0
    )sql"),
              (std::vector<Row>{Row(
                  {Value(), Value(), Value(0), Value(), Value(), Value()})}));
    EXPECT_EQ(Rows("SELECT COUNT(*),SUM(b) FROM audit_rollup "
                   "JOIN audit_rollup_j ON a=x GROUP BY () HAVING COUNT(*)=0"),
              (std::vector<Row>{Row({Value(0), Value()})}));
    EXPECT_TRUE(Rows("SELECT COUNT(*) FROM audit_rollup "
                     "JOIN audit_rollup_j ON a=x GROUP BY () HAVING COUNT(*)>0")
                    .empty());
    EXPECT_TRUE(Rows("SELECT COUNT(*) FROM audit_rollup "
                     "JOIN audit_rollup_j ON a=x GROUP BY 'constant'")
                    .empty());
  }
}

TEST_F(SqlAuditRegressionTest, CtasPreservesDeclaredIntegerAndIntervalDomains) {
  Rows(
      "CREATE TABLE audit_ctas_domain AS SELECT CAST(1 AS INT32) AS i, "
      "CAST(1 AS UINT32) AS u, CAST('0-0 1 0:0:0' AS INTERVAL) AS iv");
  Rows("CREATE TABLE audit_ctas_copy AS SELECT * FROM audit_ctas_domain");
  for (const std::string name : {"audit_ctas_domain", "audit_ctas_copy"}) {
    EXPECT_FALSE(
        Run("INSERT INTO " + name + " (i) VALUES (2147483648)").HasValue());
    EXPECT_FALSE(
        Run("INSERT INTO " + name + " (u) VALUES (4294967296)").HasValue());
    const auto rows = Rows("SELECT iv FROM " + name);
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_TRUE(rows[0][0].IsInterval());
  }
}

TEST_F(SqlAuditRegressionTest,
       IntervalStringsAndTypedIntervalsHaveDistinctDomains) {
  EXPECT_EQ(Rows("SELECT '2-0 0 0:0:0' < '10-0 0 0:0:0', "
                 "'1-0 0 0:0:0' = '01-00 0 00:00:00', "
                 "INTERVAL 2 YEAR < INTERVAL 10 YEAR"),
            (std::vector<Row>{Row({Value(false), Value(false), Value(true)})}));
  ASSERT_TRUE(
      Run("CREATE TABLE interval_audit (k INTERVAL, id INT64)").HasValue());
  ASSERT_TRUE(Run("INSERT INTO interval_audit VALUES (INTERVAL 10 YEAR, 10), "
                  "(INTERVAL 2 YEAR, 2)")
                  .HasValue());
  EXPECT_EQ(Rows("SELECT id FROM interval_audit ORDER BY k"),
            (std::vector<Row>{Row({Value(2)}), Row({Value(10)})}));
  ASSERT_TRUE(
      Run("CREATE INDEX interval_audit_idx ON interval_audit(k)").HasValue());
  EXPECT_EQ(Rows("SELECT id FROM interval_audit WHERE k < INTERVAL 10 YEAR"),
            std::vector<Row>{Row({Value(2)})});
  ASSERT_TRUE(Run("CREATE TABLE interval_text_audit (k STRING)").HasValue());
  ASSERT_TRUE(Run("INSERT INTO interval_text_audit VALUES ('2-0 0 0:0:0'), "
                  "('10-0 0 0:0:0'), ('1-0 0 0:0:0'), ('01-00 0 00:00:00')")
                  .HasValue());
  for (int indexed = 0; indexed < 2; ++indexed) {
    EXPECT_EQ(
        Rows(
            "SELECT COUNT(*) FROM interval_text_audit WHERE k < '2-0 0 0:0:0'"),
        std::vector<Row>{Row({Value(3)})});
    EXPECT_EQ(Rows("SELECT COUNT(DISTINCT k) FROM interval_text_audit"),
              std::vector<Row>{Row({Value(4)})});
    if (indexed == 0) {
      ASSERT_TRUE(
          Run("CREATE INDEX interval_text_idx ON interval_text_audit(k)")
              .HasValue());
    }
  }
}

TEST_F(SqlAuditRegressionTest,
       PivotRejectsUnknownKeysAndRespectsIdentifierCase) {
  ASSERT_TRUE(
      Run("CREATE TABLE pivot_audit (g INT64, q STRING, n INT64)").HasValue());
  ASSERT_TRUE(Run("INSERT INTO pivot_audit VALUES (1, 'a', 10), (2, 'a', 20)")
                  .HasValue());
  EXPECT_FALSE(
      Run("SELECT * FROM pivot_audit PIVOT(SUM(n) FOR q IN ('a'))").HasValue());
  EXPECT_NE(engine_->LastError().find("explicit named columns"),
            std::string::npos);
  EXPECT_EQ(
      Rows("SELECT * FROM (SELECT g AS g, q AS Q, n AS N FROM pivot_audit) "
           "PIVOT(SUM(n) FOR q IN ('a')) ORDER BY g"),
      (std::vector<Row>{Row({Value(1), Value(10)}),
                        Row({Value(2), Value(20)})}));
}

TEST_F(SqlAuditRegressionTest, StructInsertExpansionHasRealStoredRows) {
  ASSERT_TRUE(
      Run("CREATE TABLE struct_row_audit (id INT64, label STRING)").HasValue());
  ASSERT_TRUE(Run("INSERT INTO struct_row_audit VALUES "
                  "(STRUCT(1 AS id, 'one' AS label)), (NULL)")
                  .HasValue());
  EXPECT_EQ(
      Rows("SELECT id, label FROM struct_row_audit ORDER BY id NULLS LAST"),
      (std::vector<Row>{Row({Value(1), Value("one")}),
                        Row({Value(), Value()})}));
  ASSERT_TRUE(
      Run("CREATE TABLE struct_cell_audit (id INT64, s STRING)").HasValue());
  ASSERT_TRUE(Run("INSERT INTO struct_cell_audit VALUES "
                  "(1, STRUCT(10 AS x, 'hello' AS y)), (2, NULL)")
                  .HasValue());
  ASSERT_TRUE(
      Run("UPDATE struct_cell_audit SET s.x = 99 WHERE id = 1").HasValue());
  EXPECT_EQ(
      Rows("SELECT id, s.x FROM struct_cell_audit ORDER BY id"),
      (std::vector<Row>{Row({Value(1), Value(99)}), Row({Value(2), Value()})}));
}

TEST_F(SqlAuditRegressionTest, InCollationRulesAgreeForListsAndSubqueries) {
  for (const std::string sql :
       {"SELECT COLLATE('a', 'binary') IN (COLLATE('A', 'und:ci'))",
        "SELECT COLLATE('a', 'binary') IN (SELECT COLLATE('A', 'und:ci'))"}) {
    const auto result = Run(sql);
    EXPECT_FALSE(result.HasValue()) << sql;
    EXPECT_NE((result.GetStatus().GetMessage() + engine_->LastError())
                  .find("Collation conflict"),
              std::string::npos);
  }
  EXPECT_EQ(Rows("SELECT COLLATE('a', 'und:ci') IN "
                 "(SELECT COLLATE('A', 'und:ci'))"),
            std::vector<Row>{Row({Value(true)})});
  EXPECT_EQ(Rows("SELECT COLLATE('a', 'binary') IN "
                 "(COLLATE('a', 'binary'), COLLATE('A', 'und:ci'))"),
            std::vector<Row>{Row({Value(true)})});
  EXPECT_EQ(Rows("SELECT CAST(1 AS UINT64) IN (SELECT 1)"),
            std::vector<Row>{Row({Value(true)})});
}

TEST_F(SqlAuditRegressionTest, FailedViewRegistrationRetriesSameEpoch) {
  {
    auto ctx = database_->BeginContext();
    ASSERT_EQ(
        database_->PutViewDefinition(ctx, "audit_retry_view", "SELECT FROM"),
        Status::kSuccess);
    ASSERT_EQ(ctx.PreCommit(), Status::kSuccess);
  }
  const auto epoch = database_->CatalogEpoch();
  EXPECT_EQ(Rows("SELECT 1"), std::vector<Row>{Row({Value(1)})});
  {
    auto ctx = database_->BeginContext();
    ASSERT_EQ(
        database_->PutViewDefinition(ctx, "audit_retry_view", "SELECT 42 AS x"),
        Status::kSuccess);
    ASSERT_EQ(ctx.PreCommit(), Status::kSuccess);
  }
  ASSERT_EQ(database_->CatalogEpoch(), epoch);
  EXPECT_EQ(Rows("SELECT x FROM audit_retry_view"),
            std::vector<Row>{Row({Value(42)})});
}

TEST_F(SqlAuditRegressionTest, DefaultTimeZoneAndTimestampTruncUseUtc) {
  EXPECT_EQ(GetDefaultTimeZone(), "UTC");
  EXPECT_EQ(
      Rows("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2026-05-04 03:04:05+00', DAY), "
           "TIMESTAMP_TRUNC(TIMESTAMP '2026-05-04 03:04:05+00', DAY, "
           "'America/Los_Angeles')"),
      (std::vector<Row>{Row({Value("2026-05-04 00:00:00+00"),
                             Value("2026-05-03 07:00:00+00")})}));
  SetDefaultTimeZone("Asia/Tokyo");
  SetDefaultTimeZone("");
  EXPECT_EQ(GetDefaultTimeZone(), "UTC");
}

TEST_F(SqlAuditRegressionTest, IntegerAssignmentBoundsUseDeclaredTypeNotName) {
  Rows(
      "CREATE TABLE audit_names (point32 INT64, joint8count INT64, "
      "uint8_counter INT64)");
  Rows("INSERT INTO audit_names VALUES (10000000000, 1000000, -1)");
  EXPECT_EQ(Rows("SELECT * FROM audit_names"),
            (std::vector<Row>{Row(
                {Value(int64_t{10000000000}), Value(1000000), Value(-1)})}));
  Rows(
      "CREATE TABLE audit_widths (tiny INT8 NOT NULL, positive UINT8, "
      "normal INT16 DEFAULT 32767)");
  Rows("INSERT INTO audit_widths (tiny, positive) VALUES (127, 255)");
  EXPECT_FALSE(Run("INSERT INTO audit_widths VALUES (128, 1, 1)").HasValue());
  EXPECT_FALSE(Run("INSERT INTO audit_widths VALUES (1, 256, 1)").HasValue());
  EXPECT_FALSE(Run("INSERT INTO audit_widths VALUES (1, 1, 32768)").HasValue());
  EXPECT_FALSE(Run("UPDATE audit_widths SET tiny = -129").HasValue());
  EXPECT_FALSE(Run("UPDATE audit_widths SET positive = 256").HasValue());
  const auto rows = Rows("SELECT tiny, positive, normal FROM audit_widths");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(127));
  EXPECT_EQ(rows[0][1], Value(255).WithUnsigned());
  EXPECT_EQ(rows[0][2], Value(32767));
}

TEST_F(SqlAuditRegressionTest, UserFunctionsReportSameIdentity) {
  auto rows = Rows("SELECT USER(), SESSION_USER(), CURRENT_USER()");
  ASSERT_EQ(rows.size(), 1U);
  ASSERT_EQ(rows[0].Count(), 3U);
  EXPECT_EQ(rows[0][0], rows[0][1]);
  EXPECT_EQ(rows[0][1], rows[0][2]);
}

TEST_F(SqlAuditRegressionTest, DmlDoesNotInvalidateSchemaEpoch) {
  Rows("CREATE TABLE audit_epoch (k INT64, v INT64)");
  Rows("INSERT INTO audit_epoch VALUES (1, 10)");
  Rows("CREATE TABLE audit_src (k INT64)");
  Rows("INSERT INTO audit_src VALUES (1)");
  Rows("CREATE TABLE audit_struct (s STRING)");
  Rows("CREATE TABLE audit_array (a ARRAY<INT64>)");
  const auto epoch = database_->CatalogEpoch();
  for (const char* sql :
       {"MERGE INTO audit_epoch USING audit_src ON audit_epoch.k=audit_src.k "
        "WHEN MATCHED THEN UPDATE SET v=20",
        "UPDATE audit_struct SET s.x=7 WHERE TRUE",
        "UPDATE audit_array SET (DELETE a WHERE TRUE) WHERE TRUE"}) {
    auto result = Run(sql);
    ASSERT_TRUE(result.HasValue()) << sql << ": " << result.GetStatus();
    EXPECT_EQ(database_->CatalogEpoch(), epoch) << sql;
  }
  EXPECT_EQ(Rows("SELECT v FROM audit_epoch"),
            std::vector<Row>{Row({Value(20)})});
}

TEST_F(SqlAuditRegressionTest, WithTiesCteNameSurvivesFetchPrepass) {
  EXPECT_EQ(Rows("WITH ties AS (SELECT 42 AS x) SELECT x FROM ties"),
            std::vector<Row>{Row({Value(42)})});
  EXPECT_EQ(Rows("WITH ties AS (SELECT 42 AS x) SELECT x FROM ties LIMIT 1"),
            std::vector<Row>{Row({Value(42)})});
}

TEST_F(SqlAuditRegressionTest,
       WithTiesPrepassRespectsNestedQueriesAndIdentifiers) {
  EXPECT_EQ(Rows("WITH limit_prefix AS (WITH ties AS (SELECT 42 AS x) SELECT x "
                 "FROM ties) SELECT x FROM limit_prefix"),
            std::vector<Row>{Row({Value(42)})});
  EXPECT_EQ(
      Rows("WITH limited AS (SELECT 1 AS x LIMIT 1), nested AS (WITH ties AS "
           "(SELECT 42 AS x) SELECT x FROM ties) SELECT x FROM nested"),
      std::vector<Row>{Row({Value(42)})});
  EXPECT_EQ(Rows("WITH ties AS (SELECT 42 AS x UNION ALL SELECT 42 AS x) "
                 "SELECT x FROM ties ORDER BY x FETCH FIRST 1 ROWS WITH TIES"),
            (std::vector<Row>{Row({Value(42)}), Row({Value(42)})}));
}

TEST_F(SqlAuditRegressionTest, InvalidTimeZonesAreRejected) {
  for (const char* expression :
       {"CURRENT_DATE('Audit/No_Such_Zone')",
        "CURRENT_DATETIME('Audit/No_Such_Zone')",
        "STRING(TIMESTAMP '2020-01-01', 'Audit/No_Such_Zone')",
        "TIMESTAMP('2020-01-01', 'Audit/No_Such_Zone')",
        "DATETIME(TIMESTAMP '2020-01-01', 'Audit/No_Such_Zone')",
        "TIME(TIMESTAMP '2020-01-01', 'Audit/No_Such_Zone')",
        "FORMAT_TIMESTAMP('%Y', TIMESTAMP '2020-01-01', 'Audit/No_Such_Zone')",
        "PARSE_TIMESTAMP('%Y-%m-%d', '2020-01-01', 'Audit/No_Such_Zone')",
        "TIMESTAMP_TRUNC(TIMESTAMP '2020-01-01', DAY, 'Audit/No_Such_Zone')",
        "CURRENT_DATE('+01:00junk')", "CURRENT_DATETIME('UTC+999999999999')"}) {
    EXPECT_FALSE(Run(std::string("SELECT ") + expression).HasValue())
        << expression;
  }
}

TEST_F(SqlAuditRegressionTest, ExplainDoesNotInventUniqueEventId) {
  Rows("CREATE TABLE audit_events (event_id INT64)");
  Rows("INSERT INTO audit_events VALUES (1),(1)");
  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = "ROW_NUMBER";
  window->partition_by = {ColumnValueExp("event_id")};
  SelectStatement statement({NamedExpression("rn", window)}, {"audit_events"},
                            nullptr);
  auto context = database_->BeginContext();
  std::ostringstream estimate;
  relational_detail::WriteEstimatedPhysicalPlan(context, statement, estimate,
                                                0);
  EXPECT_EQ(estimate.str().find("Project constant 1"), std::string::npos);
  EXPECT_EQ(estimate.str().find("constant_partition=true"), std::string::npos);
  const auto plan = Rows(
      "EXPLAIN SELECT ROW_NUMBER() OVER (PARTITION BY event_id) FROM "
      "audit_events");
  std::string text;
  for (const Row& row : plan) {
    for (const Value& value : row.values_) {
      text += value.AsString();
    }
  }
  EXPECT_EQ(text.find("constant_partition=true"), std::string::npos);
  EXPECT_EQ(text.find("Project constant 1"), std::string::npos);
  EXPECT_EQ(Rows("SELECT ROW_NUMBER() OVER (PARTITION BY event_id) FROM "
                 "audit_events ORDER BY 1"),
            std::vector<Row>({Row({Value(1)}), Row({Value(2)})}));
}

TEST_F(SqlAuditRegressionTest, TypedNullArithmeticAndNotPropagateNull) {
  for (const char* sql :
       {"SELECT 1 + CAST(NULL AS INT64)", "SELECT CAST(NULL AS INT64) - 1",
        "SELECT 1.5 * CAST(NULL AS FLOAT64)", "SELECT 1 / CAST(NULL AS INT64)",
        "SELECT NOT CAST(NULL AS BOOL)"}) {
    EXPECT_EQ(Rows(sql), std::vector<Row>({Row({Value()})})) << sql;
  }
  // The pinned GoogleSQL corpus rejects *literal* untyped NULL in these
  // operators; this differs from a typed NULL's runtime propagation.
  EXPECT_FALSE(Run("SELECT 1 + NULL").HasValue());
  EXPECT_FALSE(Run("SELECT NOT NULL").HasValue());
}

TEST_F(SqlAuditRegressionTest, JsonEscapesSurviveSqlStringDecoding) {
  EXPECT_EQ(
      Rows(R"sql(SELECT JSON_EXTRACT(r'{"a": "quote\" inside"}', '$.a'))sql"),
      std::vector<Row>({Row({Value(R"json("quote\" inside")json")})}));
  EXPECT_EQ(Rows(R"sql(SELECT JSON_EXTRACT('{"a b": 1}', "$['a b']"))sql"),
            std::vector<Row>({Row({Value("1")})}));
}

TEST_F(SqlAuditRegressionTest, ChrRejectsNonIntegerArguments) {
  for (const char* sql : {"SELECT CHR(65.5)", "SELECT CHR('65')",
                          "SELECT CHR(DATE '2020-01-01')"}) {
    EXPECT_FALSE(Run(sql).HasValue()) << sql;
  }
  EXPECT_EQ(Rows("SELECT CHR(65)"), std::vector<Row>({Row({Value("A")})}));
}

TEST_F(SqlAuditRegressionTest, HugeLimitDoesNotWrapScanHint) {
  Rows("CREATE TABLE audit_limit (x INT64)");
  Rows("INSERT INTO audit_limit VALUES (0),(1),(2),(3),(4),(5),(6)");
  EXPECT_EQ(
      Rows("SELECT x FROM audit_limit LIMIT 18446744073709551615 OFFSET 5"),
      std::vector<Row>({Row({Value(5)}), Row({Value(6)})}));
}

TEST_F(SqlAuditRegressionTest, MalformedProtoUpdatePreservesStoredText) {
  Rows("CREATE TABLE audit_proto (p STRING)");
  Rows("INSERT INTO audit_proto VALUES ('garbage$$$')");
  auto result =
      Run("UPDATE audit_proto SET p.str_value = 'replacement' WHERE TRUE");
  EXPECT_FALSE(result.HasValue());
  EXPECT_EQ(Rows("SELECT p FROM audit_proto"),
            std::vector<Row>({Row({Value("garbage$$$")})}));
}

TEST_F(SqlAuditRegressionTest, SuffixedTableNamesKeepOuterRows) {
  Rows("CREATE TABLE audit_base (k INT64, amount INT64)");
  Rows("CREATE TABLE audit_base_1 (k INT64)");
  Rows("INSERT INTO audit_base VALUES (1, 10), (2, NULL)");
  EXPECT_EQ(Rows("SELECT audit_base.k FROM audit_base LEFT JOIN audit_base_1 "
                 "ON audit_base.k=audit_base_1.k WHERE audit_base.amount > 0"),
            std::vector<Row>({Row({Value(1)})}));
  EXPECT_EQ(
      Rows("SELECT audit_base.k FROM audit_base LEFT JOIN audit_base_1 ON "
           "audit_base.k=audit_base_1.k WHERE audit_base.amount IS NULL"),
      std::vector<Row>({Row({Value(2)})}));
}

TEST_F(SqlAuditRegressionTest, NamedWindowExtensionsKeepPartition) {
  Rows("CREATE TABLE audit_window (k INT64, y INT64, x INT64)");
  Rows(
      "INSERT INTO audit_window VALUES (1, 1, 10), (1, 2, 20), (2, 1, 100), "
      "(2, 2, 200)");
  EXPECT_EQ(Rows("SELECT k,y,SUM(x) OVER (w ORDER BY y ROWS BETWEEN UNBOUNDED "
                 "PRECEDING AND CURRENT ROW) FROM audit_window WINDOW w AS "
                 "(PARTITION BY k) ORDER BY k,y"),
            std::vector<Row>({Row({Value(1), Value(1), Value(10)}),
                              Row({Value(1), Value(2), Value(30)}),
                              Row({Value(2), Value(1), Value(100)}),
                              Row({Value(2), Value(2), Value(300)})}));
}

TEST_F(SqlAuditRegressionTest, NestedQueryPreservesEnclosingNamedWindow) {
  Rows("CREATE TABLE audit_nested_window (k INT64, x INT64)");
  Rows("INSERT INTO audit_nested_window VALUES (1, 10), (2, 20)");
  EXPECT_EQ(Rows("SELECT (SELECT 1), SUM(x) OVER w FROM audit_nested_window "
                 "WINDOW w AS (PARTITION BY k) ORDER BY k"),
            std::vector<Row>(
                {Row({Value(1), Value(10)}), Row({Value(1), Value(20)})}));
}

TEST_F(SqlAuditRegressionTest, StructStarUsesActualColumns) {
  Rows("CREATE TABLE audit_struct (a INT64, b STRING, c INT64)");
  Rows("INSERT INTO audit_struct VALUES (1, 'two', 3)");
  const auto explicit_fields =
      Rows("SELECT AS STRUCT s.a,s.b,s.c FROM audit_struct s");
  ASSERT_EQ(explicit_fields.size(), 1U);
  EXPECT_EQ(Rows("SELECT AS STRUCT s.* FROM audit_struct s"), explicit_fields);
}

TEST_F(SqlAuditRegressionTest, FunctionStructStarUsesDeclaredFields) {
  Rows(
      "CREATE TEMP FUNCTION audit_record(x INT64) AS (STRUCT(x AS first, x + 1 "
      "AS second))");
  EXPECT_EQ(Rows("SELECT audit_record(7).*"),
            std::vector<Row>({Row({Value(7), Value(8)})}));
  Rows(
      "CREATE TEMP FUNCTION audit_literal_record() AS (STRUCT(42 AS answer, "
      "'ok' AS label))");
  EXPECT_EQ(Rows("SELECT audit_literal_record().*"),
            std::vector<Row>({Row({Value(42), Value("ok")})}));
}

TEST_F(SqlAuditRegressionTest, DerivedFilterStaysBelowLaterOuterJoin) {
  Rows("CREATE TABLE audit_t0 (k INT64)");
  Rows("CREATE TABLE audit_t1 (k INT64, b INT64)");
  Rows("CREATE TABLE audit_t2 (k INT64)");
  Rows("INSERT INTO audit_t0 VALUES (0)");
  Rows("INSERT INTO audit_t1 VALUES (1, 0)");
  Rows("INSERT INTO audit_t2 VALUES (1)");
  for (const char* kind : {"FULL OUTER", "RIGHT OUTER"}) {
    const std::string sql =
        std::string(
            "SELECT audit_t2.k,d.a FROM audit_t0 CROSS JOIN (SELECT k AS a,b "
            "FROM audit_t1 WHERE b>5) d ") +
        kind + " JOIN audit_t2 ON audit_t2.k=d.a";
    EXPECT_EQ(Rows(sql), std::vector<Row>({Row({Value(1), Value()})}));
  }
}

TEST_F(SqlAuditRegressionTest, DmlExistsCannotSwallowEvaluationFailure) {
  Rows("CREATE TABLE audit_dml (x INT64)");
  Rows("INSERT INTO audit_dml VALUES (1), (2)");
  const auto result =
      Run("DELETE FROM audit_dml WHERE NOT EXISTS (SELECT x FROM audit_dml "
          "WHERE 1 / (x - x) > 0)");
  EXPECT_FALSE(result.HasValue());
  EXPECT_EQ(Rows("SELECT COUNT(*) FROM audit_dml"),
            std::vector<Row>({Row({Value(2)})}));
}

TEST_F(SqlAuditRegressionTest, CteUnionLiteralsKeepTextOrderOnTemplateReplay) {
  Rows("CREATE TABLE audit_cte (v INT64)");
  Rows("INSERT INTO audit_cte VALUES (1)");
  for (int64_t offset : {int64_t{0}, int64_t{0}, int64_t{10}}) {
    const std::string sql =
        "WITH z AS (SELECT v FROM audit_cte UNION ALL SELECT " +
        std::to_string(7 + offset) +
        "), a AS (SELECT v FROM audit_cte UNION ALL SELECT " +
        std::to_string(8 + offset) +
        ") SELECT z.v,a.v FROM z,a ORDER BY z.v,a.v";
    EXPECT_EQ(Rows(sql),
              std::vector<Row>({Row({Value(1), Value(1)}),
                                Row({Value(1), Value(8 + offset)}),
                                Row({Value(7 + offset), Value(1)}),
                                Row({Value(7 + offset), Value(8 + offset)})}));
  }
}

TEST_F(SqlAuditRegressionTest, IntegerHavingKeepsValuesBeyondDoublePrecision) {
  Rows("CREATE TABLE audit_fact (k INT64, v INT64)");
  Rows("CREATE TABLE audit_dim (k INT64)");
  Rows("INSERT INTO audit_fact VALUES (1, 9007199254740993)");
  Rows("INSERT INTO audit_dim VALUES (1)");
  EXPECT_EQ(
      Rows("SELECT f.k,SUM(f.v) FROM audit_fact f JOIN audit_dim d ON f.k=d.k "
           "GROUP BY f.k HAVING SUM(f.v)>9007199254740992"),
      std::vector<Row>({Row({Value(1), Value(int64_t{9007199254740993})})}));
}
}  // namespace

TEST_F(SqlAuditRegressionTest, HavingAliasesIgnoreIdentifierCase) {
  Rows("CREATE TABLE audit_alias (k INT64, v INT64)");
  Rows("INSERT INTO audit_alias VALUES (1,3),(1,4),(2,1)");
  EXPECT_EQ(Rows("SELECT k,SUM(v) AS total FROM audit_alias GROUP BY k HAVING "
                 "Total>5 ORDER BY k"),
            std::vector<Row>({Row({Value(1), Value(7)})}));
}

TEST_F(SqlAuditRegressionTest, OrderByOrdinalKeepsDuplicateColumnPosition) {
  Rows("CREATE TABLE audit_ordinal (a INT64,b INT64)");
  Rows("INSERT INTO audit_ordinal VALUES (1,9),(2,3),(3,6)");
  EXPECT_EQ(
      Rows("SELECT a AS x,b AS x FROM audit_ordinal ORDER BY 2"),
      std::vector<Row>({Row({Value(2), Value(3)}), Row({Value(3), Value(6)}),
                        Row({Value(1), Value(9)})}));
}

TEST_F(SqlAuditRegressionTest, MalformedDateUnnestReturnsStatus) {
  auto context = database_->BeginContext();
  for (const std::string text : {"2026-13-01", " 2026-01-02", "not a date"}) {
    SCOPED_TRACE(text);
    UnnestExecutor unnest(
        context, Schema("s", {Column("v", ValueType::kInt64)}),
        std::make_shared<ValuesExecutor>(std::vector<Row>{Row({Value(1)})}),
        ConstantValueExp(Value::Array({Value(std::string(text))}, "DATE")), "d",
        "");
    DataChunk output;
    EXPECT_NO_THROW(EXPECT_EQ(unnest.NextBatch(&output), 0U));
    EXPECT_NE(unnest.GetStatus(), Status::kSuccess);
  }
}

TEST_F(SqlAuditRegressionTest, StreamingAggregateErrorsStayInStatus) {
  Rows("CREATE TABLE audit_stream (k INT64,s STRING)");
  Rows("INSERT INTO audit_stream VALUES (1,'not a number')");
  for (const std::string query :
       {"SELECT k,SUM(CAST(s AS INT64)) FROM audit_stream GROUP BY k",
        "SELECT k,COUNT(*) FROM audit_stream GROUP BY k HAVING "
        "ERROR('having')"}) {
    auto context = database_->BeginContext();
    auto prepared = engine_->Prepare(context, query);
    if (!prepared.HasValue()) {
      continue;
    }
    Row row;
    EXPECT_NO_THROW({
      while (prepared.Value()->Next(&row, nullptr)) {
      }
    });
    EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess);
  }
}

TEST_F(SqlAuditRegressionTest, DistinctOnKeepsFirstRowWithCollation) {
  Rows("CREATE TABLE audit_distinct (k STRING,v INT64)");
  Rows(
      "INSERT INTO audit_distinct VALUES "
      "('A',1),('a',2),('B',3),(NULL,4),(NULL,5)");
  EXPECT_EQ(Rows("SELECT DISTINCT ON (COLLATE(k,'und:ci')) k,v FROM "
                 "audit_distinct ORDER BY v"),
            std::vector<Row>({Row({Value("A"), Value(1)}),
                              Row({Value("B"), Value(3)}),
                              Row({Value(), Value(4)})}));
}

}  // namespace tinylamb
