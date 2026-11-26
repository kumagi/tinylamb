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

#include <cstdlib>

#include "common/platform/process.hpp"
#include "common/test_util.hpp"
#include "executor/relational_regression_test.hpp"
#include "table/tlcs_file.hpp"

namespace tinylamb {
namespace {
TEST_F(RelationalRegressionTest, FusedJoinKeepsMixedNumericCaseBranches) {
  RelationalRun(*rs_, "CREATE TABLE CaseDim (k INT64, label STRING)");
  RelationalRun(*rs_, "CREATE TABLE CaseFact (k INT64, v FLOAT64)");
  RelationalRun(
      *rs_, "INSERT INTO CaseDim VALUES (1,'PROMO X'),(2,'OTHER'),(3,NULL)");
  RelationalRun(*rs_,
                "INSERT INTO CaseFact VALUES (1,2.5),(1,3.5),(2,7.5),(3,10.5)");
  const std::string sql =
      "SELECT SUM(CASE WHEN d.label LIKE 'PROMO%' THEN f.v ELSE 0 END), "
      "SUM(f.v) "
      "FROM CaseFact f JOIN CaseDim d ON f.k=d.k";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(6.0), Value(24.0)})}));
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("grouped_join_fusions=1"),
            std::string::npos);
}

TEST_F(RelationalRegressionTest, WideFusedAggregatesKeepNullsAndDuplicateKeys) {
  RelationalRun(*rs_, "CREATE TABLE WideDim (k INT64, g STRING)");
  RelationalRun(*rs_, "CREATE TABLE WideFact (k INT64, v FLOAT64)");
  RelationalRun(*rs_, "INSERT INTO WideDim VALUES (1,'a'),(1,'a'),(2,'b')");
  RelationalRun(*rs_,
                "INSERT INTO WideFact VALUES (1,2.5),(1,3.5),(1,NULL),"
                "(2,NULL),(3,99)");
  const std::string sql =
      "SELECT d.g, COUNT(*), COUNT(f.v), SUM(f.v), AVG(f.v), "
      "SUM(CASE WHEN f.v > 3 THEN 1 ELSE 0 END), "
      "SUM(CASE WHEN f.v <= 3 THEN 1 ELSE 0 END), "
      "COUNT(DISTINCT f.k), SUM(f.v * 2) "
      "FROM WideFact f JOIN WideDim d ON f.k=d.k GROUP BY d.g ORDER BY d.g";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>(
                {Row({Value("a"), Value(6), Value(4), Value(12.0), Value(3.0),
                      Value(2), Value(2), Value(1), Value(24.0)}),
                 Row({Value("b"), Value(1), Value(0), Value(), Value(),
                      Value(0), Value(0), Value(1), Value()})}));
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("grouped_join_fusions=1"),
            std::string::npos);
}

TEST_F(RelationalRegressionTest, FusedDirectEmissionKeepsRawCellsAndErrors) {
  RelationalRun(*rs_, "CREATE TABLE EmitDim(k INT64)");
  RelationalRun(*rs_, "CREATE TABLE EmitFact(k INT64, i INT64, d DOUBLE)");
  RelationalRun(*rs_, "INSERT INTO EmitDim VALUES (1),(2),(3)");
  RelationalRun(*rs_,
                "INSERT INTO EmitFact VALUES "
                "(1,2,1.5),(1,4,2.5),(1,NULL,NULL),"
                "(2,NULL,NULL)");
  const std::string sql =
      "SELECT d.k, SUM(f.i), SUM(f.d), AVG(f.d), COUNT(f.d), COUNT(*), "
      "SUM(f.d), 17 FROM EmitDim d LEFT JOIN EmitFact f ON d.k=f.k "
      "GROUP BY d.k ORDER BY d.k";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(1), Value(6), Value(4.0), Value(2.0),
                                   Value(2), Value(3), Value(4.0), Value(17)}),
                              Row({Value(2), Value(), Value(), Value(),
                                   Value(0), Value(1), Value(), Value(17)}),
                              Row({Value(3), Value(), Value(), Value(),
                                   Value(0), Value(1), Value(), Value(17)})}));
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("grouped_join_fusions=1"),
            std::string::npos);
  RelationalRun(*rs_, "INSERT INTO EmitFact VALUES (1,9223372036854775807,1)");
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto result = engine.Execute(context, sql);
  if (result.HasValue()) {
    result.Value().Drain();
    EXPECT_EQ(result.Value().GetStatus(), Status::kIsInfinity);
  } else {
    EXPECT_EQ(result.GetStatus(), Status::kIsInfinity);
  }
}

TEST_F(RelationalRegressionTest, FusedIntegerCaseRangesKeepNullAndOverflow) {
  RelationalRun(*rs_, "CREATE TABLE RangeDim (k INT64, g STRING)");
  RelationalRun(*rs_, "CREATE TABLE RangeFact (k INT64, a INT64, b INT64)");
  RelationalRun(*rs_, "INSERT INTO RangeDim VALUES (1,'a'),(1,'a'),(2,'b')");
  RelationalRun(*rs_,
                "INSERT INTO RangeFact VALUES (1,30,0),(1,60,0),(1,90,0),"
                "(1,NULL,0),(1,-1,0),(2,0,NULL)");
  const std::string sql =
      "SELECT d.g, SUM(CASE WHEN f.a-f.b<=30 THEN 1 ELSE 0 END), "
      "SUM(CASE WHEN f.a-f.b>30 AND f.a-f.b<=60 THEN 1 ELSE 0 END), "
      "SUM(CASE WHEN f.a-f.b>60 AND f.a-f.b<=90 THEN 1 ELSE 0 END), "
      "SUM(CASE WHEN f.a-f.b>90 THEN 1 ELSE 0 END), "
      "SUM(CASE WHEN f.a-f.b=30 THEN 1 ELSE 0 END), COUNT(*) "
      "FROM RangeFact f JOIN RangeDim d ON f.k=d.k GROUP BY d.g ORDER BY d.g";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value("a"), Value(4), Value(2), Value(2),
                                   Value(0), Value(2), Value(10)}),
                              Row({Value("b"), Value(0), Value(0), Value(0),
                                   Value(0), Value(0), Value(1)})}));
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("grouped_join_fusions=1"),
            std::string::npos);
  RelationalRun(*rs_,
                "INSERT INTO RangeFact VALUES (2,9223372036854775807,-1)");
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto executed = engine.Execute(context, sql);
  ASSERT_TRUE(executed.HasValue());
  executed.Value().Collect();
  EXPECT_NE(executed.Value().GetStatus(), Status::kSuccess);
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, InnerJoinCompiledResidualPreservesNulls) {
  RelationalRun(*rs_, "CREATE TABLE ResidualL (k INT64, v FLOAT64)");
  RelationalRun(*rs_, "CREATE TABLE ResidualR (k INT64, zip STRING)");
  RelationalRun(*rs_,
                "INSERT INTO ResidualL VALUES (1,2),(1,8),(2,NULL),(3,8)");
  RelationalRun(*rs_,
                "INSERT INTO ResidualR VALUES (1,'AB123'),(1,'CD123'),"
                "(2,'AB456'),(3,NULL)");
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT l.k,l.v,r.zip FROM ResidualL l, ResidualR r "
                          "WHERE l.k=r.k AND (SUBSTR(r.zip,1,2) IN ('AB','XY') "
                          "OR l.v>5) ORDER BY l.k,l.v,r.zip"),
            std::vector<Row>({Row({Value(1), Value(2.0), Value("AB123")}),
                              Row({Value(1), Value(8.0), Value("AB123")}),
                              Row({Value(1), Value(8.0), Value("CD123")}),
                              Row({Value(2), Value(), Value("AB456")}),
                              Row({Value(3), Value(8.0), Value()})}));
}

TEST_F(RelationalRegressionTest, AbsorbedStringsKeepOwnershipAndSnapshots) {
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, dim,
        rs_->CreateTable(
            setup,
            Schema("OwnedDim", {Column("k", ValueType::kInt64,
                                       Constraint(Constraint::kPrimaryKey)),
                                Column("label", ValueType::kVarChar)})));
    for (const auto& row : {Row({Value(1), Value("shared string payload")}),
                            Row({Value(2), Value()}),
                            Row({Value(3), Value("shared string payload")}),
                            Row({Value(4), Value("")})}) {
      ASSERT_SUCCESS(dim.Insert(setup.txn_, row).GetStatus());
    }
    ASSIGN_OR_ASSERT_FAIL(
        Table, fact,
        rs_->CreateTable(
            setup, Schema("OwnedFact", {Column("k", ValueType::kInt64),
                                        Column("d", ValueType::kInt64)})));
    for (int64_t i = 0; i < 2560; ++i) {
      ASSERT_SUCCESS(
          fact.Insert(setup.txn_, Row({Value((i % 5) + 1), Value(i % 2)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "CREATE TABLE OwnedExtra(d INT64, v INT64)");
  RelationalRun(*rs_, "INSERT INTO OwnedExtra VALUES (0,10),(1,20)");
  for (const auto* table : {"OwnedDim", "OwnedFact", "OwnedExtra"}) {
    RelationalRun(*rs_, std::string("ANALYZE ") + table);
  }
  const std::string sql =
      "SELECT s.label,COUNT(*),SUM(e.v) FROM OwnedFact f "
      "JOIN OwnedDim s ON f.k=s.k JOIN OwnedExtra e ON f.d=e.d "
      "GROUP BY s.label ORDER BY s.label NULLS FIRST";
  const std::vector<Row> expected{
      Row({Value(), Value(512), Value(7680)}),
      Row({Value(""), Value(512), Value(7680)}),
      Row({Value("shared string payload"), Value(1024), Value(15360)})};
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    EXPECT_EQ(RelationalRun(*rs_, sql), expected);
    const auto kept_key =
        RelationalRun(*rs_,
                      "SELECT f.k,s.label,COUNT(*),SUM(e.v) FROM OwnedFact f "
                      "JOIN OwnedDim s ON f.k=s.k JOIN OwnedExtra e ON f.d=e.d "
                      "GROUP BY f.k,s.label ORDER BY f.k");
    ASSERT_EQ(kept_key.size(), 4U);
    for (size_t i = 0; i < kept_key.size(); ++i) {
      EXPECT_EQ(kept_key[i][0], Value(static_cast<int64_t>(i + 1)));
      EXPECT_EQ(kept_key[i][2], Value(512));
      EXPECT_EQ(kept_key[i][3], Value(7680));
    }
  }
  auto old = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  const auto read_old = [&] {
    auto result = engine.Execute(old, sql);
    EXPECT_TRUE(result.HasValue());
    if (!result.HasValue()) {
      return std::vector<Row>{};
    }
    auto rows = result.Value().Collect();
    EXPECT_TRUE(result.Value().GetStatus().ok());
    return rows;
  };
  EXPECT_EQ(read_old(), expected);
  RelationalRun(*rs_, "UPDATE OwnedDim SET label='changed' WHERE k=1");
  EXPECT_EQ(read_old(), expected);
  EXPECT_EQ(RelationalRun(*rs_, sql),
            (std::vector<Row>{expected[0], expected[1],
                              Row({Value("changed"), Value(512), Value(7680)}),
                              Row({Value("shared string payload"), Value(512),
                                   Value(7680)})}));
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest, DeferredFilteredPkKeepsNullsAndSnapshots) {
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, days,
        rs_->CreateTable(
            setup,
            Schema("DeferredDays", {Column("day", ValueType::kInt64,
                                           Constraint(Constraint::kPrimaryKey)),
                                    Column("year", ValueType::kInt64)})));
    for (const auto& row :
         {Row({Value(1), Value(2001)}), Row({Value(2), Value()}),
          Row({Value(3), Value(std::numeric_limits<int64_t>::max())}),
          Row({Value(4), Value(std::numeric_limits<int32_t>::min())})}) {
      ASSERT_SUCCESS(days.Insert(setup.txn_, row).GetStatus());
    }
    ASSIGN_OR_ASSERT_FAIL(
        Table, fact,
        rs_->CreateTable(
            setup, Schema("DeferredFact", {Column("k", ValueType::kInt64),
                                           Column("day", ValueType::kInt64),
                                           Column("v", ValueType::kInt64),
                                           Column("p", ValueType::kDate)})));
    for (int64_t i = 0; i < 40000; ++i) {
      ASSERT_SUCCESS(
          fact.Insert(setup.txn_,
                      Row({Value((i % 2) + 1), Value((i % 4) + 1), Value(1),
                           i % 3 == 0 ? Value()
                                      : Value::DateFromDays((i % 3) - 2)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "CREATE TABLE DeferredDim (k INT64,g STRING)");
  RelationalRun(*rs_, "INSERT INTO DeferredDim VALUES (1,'a'),(2,'b'),(2,'b')");
  for (const auto* table : {"DeferredDays", "DeferredFact", "DeferredDim"}) {
    RelationalRun(*rs_, std::string("ANALYZE ") + table);
  }
  const std::string sql =
      "SELECT d.g,y.year,SUM(f.v),COUNT(*) FROM DeferredFact f "
      "JOIN DeferredDays y ON f.day=y.day JOIN DeferredDim d ON f.k=d.k "
      "WHERE y.day<=2 GROUP BY d.g,y.year ORDER BY d.g";
  const std::vector<Row> expected{
      Row({Value("a"), Value(2001), Value(10000), Value(10000)}),
      Row({Value("b"), Value(), Value(20000), Value(20000)})};
  EXPECT_EQ(RelationalRun(*rs_, sql), expected);
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("grouped_join_fusions=1"),
            std::string::npos);
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    const auto all = RelationalRun(
        *rs_,
        "SELECT d.g,y.year,SUM(f.v),COUNT(*) FROM DeferredFact f "
        "JOIN DeferredDays y ON f.day=y.day JOIN DeferredDim d ON f.k=d.k "
        "WHERE y.day<=4 GROUP BY d.g,y.year "
        "HAVING COUNT(*)>1 ORDER BY d.g,y.year");
    EXPECT_EQ(all,
              (std::vector<Row>{
                  expected[0],
                  Row({Value("a"), Value(std::numeric_limits<int64_t>::max()),
                       Value(10000), Value(10000)}),
                  expected[1],
                  Row({Value("b"), Value(std::numeric_limits<int32_t>::min()),
                       Value(20000), Value(20000)})}));
    EXPECT_EQ(
        RelationalRun(*rs_,
                      "SELECT d.g,y.year+1,COUNT(*) FROM DeferredFact f "
                      "JOIN DeferredDays y ON f.day=y.day "
                      "JOIN DeferredDim d ON f.k=d.k WHERE y.day<=2 "
                      "GROUP BY d.g,y.year HAVING COUNT(*)>1 ORDER BY d.g"),
        (std::vector<Row>{Row({Value("a"), Value(2002), Value(10000)}),
                          Row({Value("b"), Value(), Value(20000)})}));
  }
  std::array<std::array<int64_t, 3>, 2> counts{};
  for (int64_t i = 0; i < 40000; ++i) {
    if (i % 4 < 2) {
      counts[static_cast<size_t>(i % 4)][static_cast<size_t>(i % 3)] +=
          i % 4 == 0 ? 1 : 2;
    }
  }
  std::vector<Row> date_expected;
  date_expected.reserve(counts.size() * counts[0].size());
  for (size_t g = 0; g < counts.size(); ++g) {
    for (size_t p = 0; p < counts[g].size(); ++p) {
      date_expected.push_back(Row(
          {Value(g == 0 ? "a" : "b"), g == 0 ? Value(2001) : Value(),
           p == 0 ? Value() : Value::DateFromDays(static_cast<int64_t>(p) - 2),
           Value(counts[g][p]), Value(counts[g][p])}));
    }
  }
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    EXPECT_EQ(RelationalRun(
                  *rs_,
                  "SELECT d.g,y.year,f.p,SUM(f.v),COUNT(*) FROM DeferredFact f "
                  "JOIN DeferredDays y ON f.day=y.day "
                  "JOIN DeferredDim d ON f.k=d.k WHERE y.day<=2 "
                  "GROUP BY d.g,y.year,f.p HAVING COUNT(*)>1 "
                  "ORDER BY d.g,y.year,f.p"),
              date_expected);
  }
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const auto& [predicate, expect_null] :
         {std::pair{"f.p IS NULL", true}, std::pair{"NOT (f.p IS NULL)", false},
          std::pair{"NOT (NOT (f.p IS NULL))", true},
          std::pair{"f.p IS NOT NULL AND f.v=1", false}}) {
      std::vector<Row> filtered;
      filtered.reserve(date_expected.size());
      for (const Row& row : date_expected) {
        if (row[2].IsNull() == expect_null) {
          filtered.push_back(row);
        }
      }
      const std::string null_sql =
          "SELECT d.g,y.year,f.p,SUM(f.v),COUNT(*) FROM DeferredFact f "
          "JOIN DeferredDays y ON f.day=y.day "
          "JOIN DeferredDim d ON f.k=d.k WHERE y.day<=2 AND " +
          std::string(predicate) +
          " GROUP BY d.g,y.year,f.p HAVING COUNT(*)>1 "
          "ORDER BY d.g,y.year,f.p";
      EXPECT_EQ(RelationalRun(*rs_, null_sql), filtered) << predicate;
    }
  }
  auto old = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  const auto read_old = [&] {
    std::vector<Row> rows;
    auto prepared = engine.Prepare(old, sql);
    EXPECT_TRUE(prepared.HasValue());
    if (prepared.HasValue()) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) {
        rows.push_back(row);
      }
      EXPECT_TRUE(prepared.Value()->GetStatus().ok());
    }
    return rows;
  };
  EXPECT_EQ(read_old(), expected);
  RelationalRun(*rs_, "UPDATE DeferredDays SET year=2003 WHERE day=1");
  EXPECT_EQ(read_old(), expected);
  auto fresh = expected;
  fresh[0][1] = Value(2003);
  EXPECT_EQ(RelationalRun(*rs_, sql), fresh);
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest,
       StreamGroupedJoinKeepsDuplicatesNullsAndSnapshot) {
  RelationalRun(*rs_, "CREATE TABLE StreamDim (k INT64, g INT64, v INT64)");
  RelationalRun(*rs_, "CREATE TABLE StreamFact (k INT64, v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO StreamDim VALUES "
                "(1,0,10),(1,NULL,20),(2,0,NULL),(3,3,NULL),(99,9,100)");
  RelationalRun(*rs_,
                "INSERT INTO StreamFact VALUES "
                "(1,5),(1,NULL),(2,7),(3,NULL),(7,99),(NULL,100)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("StreamFact"));
    for (int i = 0; i < 2000; ++i) {
      ASSERT_SUCCESS(
          fact->Insert(setup.txn_, Row({Value(100), Value(i)})).GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE StreamDim");
  RelationalRun(*rs_, "ANALYZE StreamFact");
  const std::string sql =
      "SELECT d.g, SUM(f.v), SUM(d.v), COUNT(*), COUNT(f.v) FROM StreamDim d "
      "JOIN StreamFact f ON f.k=d.k GROUP BY d.g ORDER BY d.g";
  const std::vector<Row> expected{
      Row({Value(), Value(5), Value(40), Value(2), Value(1)}),
      Row({Value(0), Value(12), Value(20), Value(3), Value(2)}),
      Row({Value(3), Value(), Value(), Value(1), Value(0)})};
  EXPECT_EQ(RelationalRun(*rs_, sql), expected);
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("streamed_grouped_joins=1"), std::string::npos)
      << profile;
  EXPECT_NE(profile.find("relation_spills=0"), std::string::npos) << profile;
  const std::string no_groups =
      "SELECT d.g AS g, COUNT(*) AS n FROM StreamDim d "
      "JOIN StreamFact f ON f.k=d.k WHERE d.g<0 GROUP BY d.g";
  const auto empty_profile = ExplainAnalyze(*rs_, no_groups);
  EXPECT_NE(empty_profile.find("streamed_grouped_joins=1"), std::string::npos)
      << empty_profile;
  EXPECT_TRUE(RelationalRun(*rs_, no_groups).empty());
  RelationalRun(*rs_, "CREATE TABLE StreamEmpty AS " + no_groups);
  {
    auto catalog = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, empty,
                          catalog.GetTable("StreamEmpty"));
    const auto& schema = empty->GetSchema();
    ASSERT_EQ(schema.ColumnCount(), 2U);
    EXPECT_EQ(schema.GetColumn(0).Type(), ValueType::kInt64);
    EXPECT_EQ(schema.GetColumn(1).Type(), ValueType::kInt64);
    ASSERT_SUCCESS(catalog.PreCommit());
  }
  auto old = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  const auto read_old = [&] {
    std::vector<Row> rows;
    auto prepared = engine.Prepare(old, sql);
    EXPECT_TRUE(prepared.HasValue());
    if (prepared.HasValue()) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) {
        rows.push_back(row);
      }
      EXPECT_TRUE(prepared.Value()->GetStatus().ok());
    }
    return rows;
  };
  EXPECT_EQ(read_old(), expected);
  RelationalRun(*rs_, "INSERT INTO StreamFact VALUES (2,11)");
  EXPECT_EQ(read_old(), expected);
  auto fresh = expected;
  fresh[1] = Row({Value(0), Value(23), Value(20), Value(4), Value(3)});
  EXPECT_EQ(RelationalRun(*rs_, sql), fresh);
  ASSERT_SUCCESS(old.PreCommit());
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(128);
  EXPECT_EQ(RelationalRun(*rs_, sql), fresh);
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("streamed_grouped_joins=0"),
            std::string::npos);
  EXPECT_EQ(QueryMemoryBudget::Global().Used(), 0U);
}

TEST_F(RelationalRegressionTest,
       StreamGroupedJoinPreservesPhysicalSumPrefixes) {
  RelationalRun(*rs_, "CREATE TABLE PrefixDim (k INT64, g INT64)");
  RelationalRun(*rs_, "INSERT INTO PrefixDim VALUES (1,0)");
  RelationalRun(
      *rs_, "CREATE TABLE PrefixFact (k INT64, v INT64, padding STRING(MAX))");
  const auto maximum = std::numeric_limits<int64_t>::max();
  const auto run_case = [&](const std::vector<int64_t>& values, bool overflow,
                            int64_t total) {
    RelationalRun(*rs_, "DELETE FROM PrefixFact");
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("PrefixFact"));
    for (int64_t value : values) {
      ASSERT_SUCCESS(
          fact->Insert(setup.txn_, Row({Value(1), Value(value),
                                        Value(std::string(12000, 'x'))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
    RelationalRun(*rs_, "ANALYZE PrefixDim");
    RelationalRun(*rs_, "ANALYZE PrefixFact");
    auto read = rs_->BeginReadOnlyContext();
    SqlEngine engine(*rs_);
    auto prepared = engine.Prepare(read,
                                   "SELECT d.g, SUM(f.v) FROM PrefixFact f "
                                   "JOIN PrefixDim d ON f.k=d.k GROUP BY d.g");
    ASSERT_TRUE(prepared.HasValue()) << engine.LastError();
    Row row;
    if (overflow) {
      EXPECT_FALSE(prepared.Value()->Next(&row, nullptr));
      EXPECT_EQ(prepared.Value()->GetStatus(), Status::kIsInfinity);
    } else {
      ASSERT_TRUE(prepared.Value()->Next(&row, nullptr));
      EXPECT_EQ(row, Row({Value(0), Value(total)}));
      EXPECT_FALSE(prepared.Value()->Next(&row, nullptr));
      EXPECT_TRUE(prepared.Value()->GetStatus().ok());
    }
  };
  run_case({maximum, 0, 1, -1}, true, 0);
  run_case({-maximum, 0, maximum, maximum}, false, maximum);
}

TEST_F(RelationalRegressionTest,
       PartitionOwnedNumericGroupsKeepNullAndOverflowSemantics) {
  RelationalRun(*rs_, "CREATE TABLE NumericEdges(k INT64,j INT64,v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO NumericEdges VALUES "
                "(NULL,2,3),(NULL,2,4),(1,1,5),(1,1,6)");
  const auto* const sql =
      "SELECT k,j,COUNT(*) c,SUM(v) s FROM NumericEdges "
      "GROUP BY k,j ORDER BY k NULLS FIRST LIMIT 2";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            (std::vector<Row>{Row({Value(), Value(2), Value(2), Value(7)}),
                              Row({Value(1), Value(1), Value(2), Value(11)})}));
  // Even a small final LIMIT cannot hide overflow in a discarded group.
  RelationalRun(*rs_,
                "INSERT INTO NumericEdges VALUES "
                "(3,3,9223372036854775807),(3,3,1)");
  auto context = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  auto result =
      engine.Execute(context,
                     "SELECT k,j,SUM(v) FROM NumericEdges GROUP BY k,j "
                     "ORDER BY k NULLS FIRST LIMIT 1");
  if (result.HasValue()) {
    result.Value().Drain();
    EXPECT_EQ(result.Value().GetStatus(), Status::kIsInfinity);
  } else {
    EXPECT_EQ(result.GetStatus(), Status::kIsInfinity);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest,
       DictionaryFoldOwnsResultsAcrossPreparationWaves) {
  RelationalRun(*rs_, "CREATE TABLE WaveText(s STRING)");
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("WaveText"));
    for (int64_t i = 0; i < 36000; ++i) {
      std::string value(90, 'x');
      value += static_cast<char>('a' + ((i % 18000) % 11));
      value += '/' + std::to_string(i % 18000);
      ASSERT_SUCCESS(
          table
              ->Insert(context.txn_,
                       Row({i % 127 == 0 ? Value() : Value(std::move(value))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  {
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("WaveText"));
    auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_GT(store->dictionaries[0].entries.size(), 16384U);
    ASSERT_SUCCESS(context.PreCommit());
  }
  const auto query = [](std::string_view argument) {
    return "SELECT REGEXP_REPLACE(" + std::string(argument) +
           ",'[0-9]+$','') k,COUNT(*),COUNT(s),MIN(s),MAX(s),"
           "AVG(CAST(BYTE_LENGTH(s) AS FLOAT64)) FROM WaveText GROUP BY k "
           "HAVING COUNT(*)>10 ORDER BY k NULLS LAST";
  };
  const auto expected = RelationalRun(*rs_, query("s||''"));
  ASSERT_FALSE(expected.empty());
  const auto capture_query = [](std::string_view argument, int64_t threshold) {
    return "SELECT REGEXP_REPLACE(" + std::string(argument) +
           ",'^(.*)/[0-9]+$','\\\\1') k,COUNT(*),COUNT(s),MIN(s),MAX(s),"
           "AVG(CAST(BYTE_LENGTH(s) AS FLOAT64)) FROM WaveText GROUP BY k "
           "HAVING COUNT(*)>" +
           std::to_string(threshold) + " ORDER BY k NULLS LAST";
  };
  const auto captured = RelationalRun(*rs_, capture_query("s||''", 10));
  ASSERT_FALSE(captured.empty());
  ASSERT_TRUE(RelationalRun(*rs_, capture_query("s||''", 4000)).empty());
  for (const size_t workers : {size_t{1}, size_t{3}, size_t{8}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    EXPECT_EQ(RelationalRun(*rs_, query("s")), expected);
    EXPECT_EQ(RelationalRun(*rs_, capture_query("s", 10)), captured);
    EXPECT_TRUE(RelationalRun(*rs_, capture_query("s", 4000)).empty());
  }
}

TEST_F(RelationalRegressionTest, PartitionOwnedNumericGroupsKeepGlobalTopN) {
  struct Expected {
    int64_t key;
    int64_t second;
    int64_t count;
    int64_t sum;
    int64_t width_sum;
    size_t first;
  };
  std::map<std::pair<int64_t, int64_t>, Expected> expected_map;
  std::map<std::pair<int64_t, int64_t>, Expected> filtered_map;
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, table,
        rs_->CreateTable(setup, Schema("NumericGroups",
                                       {Column("k", ValueType::kInt64),
                                        Column("j", ValueType::kInt64),
                                        Column("v", ValueType::kInt64),
                                        Column("w", ValueType::kInt64),
                                        Column("tag", ValueType::kVarChar)})));
    for (int64_t i = 0; i < 60000; ++i) {
      const int64_t k = ((i % 50000) * 11259375) - 300000000000;
      const int64_t j = ((i * 1009) % 50000) * 2731;
      const int64_t v = (i % 17) - 8;
      const int64_t w = (i % 29) + 1024;
      ASSERT_SUCCESS(
          table
              .Insert(
                  setup.txn_,
                  Row({Value(k), Value(j), Value(v), Value(w),
                       i % 13 == 0 ? Value() : Value(i % 5 ? "present" : "")}))
              .GetStatus());
      auto [it, inserted] = expected_map.try_emplace(
          std::pair{k, j}, Expected{.key = k,
                                    .second = j,
                                    .count = 0,
                                    .sum = 0,
                                    .width_sum = 0,
                                    .first = static_cast<size_t>(i)});
      ++it->second.count;
      it->second.sum += v;
      it->second.width_sum += w;
      if (i % 13 != 0 && i % 5 != 0) {
        auto [f, fresh] = filtered_map.try_emplace(
            std::pair{k, j}, Expected{.key = k,
                                      .second = j,
                                      .count = 0,
                                      .sum = 0,
                                      .width_sum = 0,
                                      .first = static_cast<size_t>(i)});
        ++f->second.count;
        f->second.sum += v;
        f->second.width_sum += w;
      }
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  {
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("NumericGroups"));
    ASSERT_NE(GetOrBuildLSTableStore(context.txn_, *table, true), nullptr);
    ASSERT_SUCCESS(context.PreCommit());
  }
  std::vector<Expected> groups;
  groups.reserve(expected_map.size());
  for (const auto& [key, value] : expected_map) {
    groups.push_back(value);
  }
  const auto rows = [](const std::vector<Expected>& expected_groups,
                       size_t offset, size_t limit) {
    std::vector<Row> result;
    for (size_t i = offset;
         i < std::min(expected_groups.size(), offset + limit); ++i) {
      const auto& g = expected_groups[i];
      result.emplace_back(std::vector<Value>{
          Value(g.key), Value(g.second), Value(g.count), Value(g.sum),
          Value(static_cast<double>(g.width_sum) /
                static_cast<double>(g.count))});
    }
    return result;
  };
  const std::string base =
      "SELECT k,j,COUNT(*) c,SUM(v) s,AVG(CAST(w AS FLOAT64)) a "
      "FROM NumericGroups GROUP BY k,j ";
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{4}, size_t{8}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    std::ranges::sort(groups, [](const Expected& a, const Expected& b) {
      return std::tie(a.count, a.sum, a.key, a.second) >
             std::tie(b.count, b.sum, b.key, b.second);
    });
    const auto ordered =
        base + "ORDER BY c DESC,s DESC,k DESC,j DESC LIMIT 13 OFFSET 7";
    EXPECT_EQ(RelationalRun(*rs_, ordered), rows(groups, 7, 13));
    std::vector<Expected> filtered;
    filtered.reserve(filtered_map.size());
    for (const auto& [key, value] : filtered_map) {
      filtered.push_back(value);
    }
    std::ranges::sort(filtered, [](const Expected& a, const Expected& b) {
      return std::tie(a.count, a.sum, a.key, a.second) >
             std::tie(b.count, b.sum, b.key, b.second);
    });
    const auto* const filtered_sql =
        "SELECT k,j,COUNT(*) c,SUM(v) s,AVG(CAST(w AS FLOAT64)) a "
        "FROM NumericGroups WHERE tag<>'' GROUP BY k,j "
        "ORDER BY c DESC,s DESC,k DESC,j DESC LIMIT 13 OFFSET 7";
    EXPECT_EQ(RelationalRun(*rs_, filtered_sql), rows(filtered, 7, 13));
    EXPECT_NE(ExplainAnalyze(*rs_, filtered_sql)
                  .find("partitioned_numeric_aggregations=1"),
              std::string::npos);
    EXPECT_NE(ExplainAnalyze(*rs_, ordered)
                  .find("partitioned_numeric_aggregations=1"),
              std::string::npos);
    std::ranges::sort(groups, [](const Expected& a, const Expected& b) {
      return a.first < b.first;
    });
    EXPECT_EQ(RelationalRun(*rs_, base + "LIMIT 19 OFFSET 11"),
              rows(groups, 11, 19));
    std::stable_sort(
        groups.begin(), groups.end(),
        [](const Expected& a, const Expected& b) { return a.count > b.count; });
    EXPECT_EQ(RelationalRun(*rs_, base + "ORDER BY c DESC LIMIT 13 OFFSET 7"),
              rows(groups, 7, 13));
    EXPECT_TRUE(RelationalRun(*rs_, base + "ORDER BY c DESC LIMIT 0").empty());
  }
}

TEST_F(RelationalRegressionTest,
       SourceIntegerPeeksPreserveTypesNullsAndSnapshot) {
  for (const int64_t count : {257, 20000}) {
    const std::string name = "PeekSource" + std::to_string(count);
    RelationalRun(*rs_,
                  "CREATE TABLE " + name +
                      " (z STRING,i INT64,d DATE,f FLOAT64,u UINT64,p STRING)");
    std::vector<int64_t> expected;
    {
      auto setup = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                            setup.GetTable(name));
      for (int64_t i = 0; i < count; ++i) {
        Row row(
            {Value(std::string(64, 'z')), i % 13 == 0 ? Value() : Value(i),
             i % 11 == 0 ? Value() : Value::DateFromDays(i % 100),
             i % 7 == 0 ? Value() : Value(i % 2 == 0 ? 10.0 : 30.0),
             i % 3 == 0 ? Value(-1).WithUnsigned() : Value(0).WithUnsigned(),
             Value(std::string(128, 'p'))});
        ASSERT_TRUE(table->Insert(setup.txn_, row).HasValue());
        if (i % 13 != 0 && i % 11 != 0 && i % 7 != 0 && i >= count / 2 &&
            i % 100 >= 42 && i % 2 == 0 && i % 3 == 0) {
          expected.push_back(i);
        }
      }
      ASSERT_SUCCESS(setup.PreCommit());
    }
    RelationalRun(*rs_, "ANALYZE " + name);
    SelectSource source;
    source.table = name;
    source.alias = "r";
    const std::vector<slot_t> projection{1, 2, 3, 4};
    const auto compare = [](const char* column, BinaryOperation operation,
                            const Value& value) {
      return BinaryExpressionExp(ColumnValueExp(ColumnName("r", column)),
                                 operation, ConstantValueExp(value));
    };
    const std::vector<Expression> predicates{
        compare("i", BinaryOperation::kGreaterThanEquals, Value(count / 2)),
        compare("i", BinaryOperation::kLessThan, Value(-1).WithUnsigned()),
        compare("d", BinaryOperation::kGreaterThanEquals,
                Value::DateFromDays(42)),
        compare("f", BinaryOperation::kLessThan, Value(25)),
        compare("u", BinaryOperation::kGreaterThan, Value(0))};
    auto read = [&](TransactionContext& context) {
      auto result = relational_detail::LoadSource(context, source, nullptr, {},
                                                  &projection, &predicates);
      EXPECT_TRUE(result.HasValue()) << result.GetStatus();
      std::vector<int64_t> ids;
      if (result.HasValue()) {
        EXPECT_SUCCESS(result.Value().ForEachRow(
            [&](const Row& row) { ids.push_back(row[0].value.int_value); }));
      }
      std::sort(ids.begin(), ids.end());
      return ids;
    };
    auto old = rs_->BeginReadOnlyContext();
    ASSERT_FALSE(expected.empty());
    EXPECT_EQ(read(old), expected);
    RelationalRun(*rs_, "UPDATE " + name + " SET d=DATE '1970-01-01' WHERE i=" +
                            std::to_string(expected.front()));
    EXPECT_EQ(read(old), expected);
    auto fresh = rs_->BeginReadOnlyContext();
    expected.erase(expected.begin());
    EXPECT_EQ(read(fresh), expected);
    EXPECT_EQ(tinylamb::Status::kSuccess, old.Abort());
    EXPECT_EQ(tinylamb::Status::kSuccess, fresh.Abort());
  }
}

TEST_F(RelationalRegressionTest,
       SelectiveLoadFrontierPrunesAcrossDimensionChain) {
  RelationalRun(*rs_, "CREATE TABLE FrontierRegion (r INT64, flag INT64)");
  RelationalRun(*rs_,
                "INSERT INTO FrontierRegion VALUES (0,0),(1,1),(1,1),(NULL,1)");
  RelationalRun(*rs_, "CREATE TABLE FrontierDim (k INT64, r INT64)");
  RelationalRun(*rs_, "CREATE TABLE FrontierFact (k INT64, v INT64, p STRING)");
  int64_t expected_count = 0, expected_sum = 0;
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                          setup.GetTable("FrontierDim"));
    for (int64_t i = 0; i < 100; ++i) {
      ASSERT_TRUE(
          dim->Insert(setup.txn_, Row({Value(i), Value(i % 10)})).HasValue());
    }
    ASSERT_TRUE(dim->Insert(setup.txn_, Row({Value(1), Value(1)})).HasValue());
    ASSERT_TRUE(dim->Insert(setup.txn_, Row({Value(), Value(1)})).HasValue());
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("FrontierFact"));
    for (int64_t i = 0; i < 20000; ++i) {
      ASSERT_TRUE(
          fact->Insert(setup.txn_, Row({i % 17 == 0 ? Value() : Value(i % 100),
                                        i % 13 == 0 ? Value() : Value(i % 7),
                                        Value(std::string(128, 'p'))}))
              .HasValue());
      if (i % 17 != 0 && i % 13 != 0 && i % 10 == 1) {
        const int64_t multiplicity = i % 100 == 1 ? 4 : 2;
        expected_count += multiplicity;
        expected_sum += (i % 7) * multiplicity;
      }
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE");
  const std::string query =
      "SELECT d.r,COUNT(*),SUM(f.v) FROM FrontierFact f "
      "JOIN FrontierDim d ON f.k=d.k JOIN FrontierRegion r ON d.r=r.r "
      "WHERE f.v>=0 AND r.flag=1 GROUP BY d.r";
  EXPECT_EQ(RelationalRun(*rs_, query),
            std::vector<Row>(
                {Row({Value(1), Value(expected_count), Value(expected_sum)})}));
  const auto profile = ExplainAnalyze(*rs_, query);
  const std::string field = "scan_output_rows=";
  const size_t position = profile.find(field);
  ASSERT_NE(position, std::string::npos) << profile;
  const auto rows = std::stoull(profile.substr(position + field.size()));
  EXPECT_LT(rows, 2500U) << profile;

  // The LEFT join keeps the ordered loading path. Its null-supplying side
  // must not seed a filter; the preserved region -> dimension -> fact chain
  // can still prune rows, without losing duplicate join multiplicities.
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    const std::string ordered_query =
        "SELECT d.r,COUNT(*),SUM(f.v),COUNT(t.k) FROM FrontierFact f "
        "JOIN FrontierDim d ON f.k=d.k JOIN FrontierRegion r ON d.r=r.r "
        "LEFT JOIN KeyTable t ON r.r=t.k AND t.k<0 "
        "WHERE f.v>=0 AND r.flag=1 AND t.k IS NULL GROUP BY d.r";
    EXPECT_EQ(RelationalRun(*rs_, ordered_query),
              std::vector<Row>({Row({Value(1), Value(expected_count),
                                     Value(expected_sum), Value(0)})}));
    const auto ordered_profile = ExplainAnalyze(*rs_, ordered_query);
    const size_t ordered_position = ordered_profile.find(field);
    ASSERT_NE(ordered_position, std::string::npos) << ordered_profile;
    const auto ordered_rows =
        std::stoull(ordered_profile.substr(ordered_position + field.size()));
    EXPECT_LT(ordered_rows, 2500U) << ordered_profile;
  }
}

TEST_F(RelationalRegressionTest,
       DenseBuildFrequencyFoldKeepsNullsAndNegativeFallback) {
  RelationalRun(*rs_, "CREATE TABLE DenseDim (k INT64, g INT64, v INT64)");
  RelationalRun(*rs_, "CREATE TABLE DenseFact (k INT64, padding STRING)");
  std::map<int64_t, std::array<int64_t, 3>> expected;
  int64_t ones = 0;
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                          setup.GetTable("DenseDim"));
    for (int64_t k = -128; k < 128; ++k) {
      ASSERT_TRUE(
          dim->Insert(setup.txn_,
                      Row({Value(k), k % 7 == 0 ? Value() : Value(k % 4),
                           k % 5 == 0 ? Value() : Value(2)}))
              .HasValue());
    }
    ASSERT_TRUE(dim->Insert(setup.txn_, Row({Value(129), Value(99), Value()}))
                    .HasValue());
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("DenseFact"));
    for (int64_t i = 0; i < 20000; ++i) {
      const int64_t k = (i % 300) - 150;
      ASSERT_TRUE(
          fact->Insert(setup.txn_, Row({i % 17 == 0 ? Value() : Value(k),
                                        Value(std::string(256, 'x'))}))
              .HasValue());
      if (i % 17 == 0) {
        continue;
      }
      if (k == 129) {
        ++expected[99][0];
        continue;
      }
      if (k < -128 || k >= 128) {
        continue;
      }
      auto& group = expected[k % 7 == 0 ? 1000 : k % 4];
      ++group[0];
      if (k % 5 != 0) {
        ++group[1];
        group[2] += 2;
      }
      if (k == 1) {
        ++ones;
      }
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE");
  const std::string sql =
      "SELECT d.g,COUNT(*),COUNT(d.v),SUM(d.v) FROM DenseFact f "
      "JOIN DenseDim d ON f.k=d.k GROUP BY d.g";
  const auto check = [&](const std::vector<Row>& rows, bool negative) {
    EXPECT_EQ(rows.size(), expected.size());
    for (const auto& row : rows) {
      const int64_t key = row[0].IsNull() ? 1000 : row[0].value.int_value;
      const auto found = expected.find(key);
      ASSERT_NE(found, expected.end());
      EXPECT_EQ(row[1], Value(found->second[0]));
      EXPECT_EQ(row[2], Value(found->second[1]));
      EXPECT_EQ(row[3], found->second[1] == 0
                            ? Value()
                            : Value(found->second[2] -
                                    (negative && key == 1 ? 3 * ones : 0)));
    }
  };
  {
    struct
        RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
      size_t limit = QueryMemoryBudget::Global().Limit();
      ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
    } restore;
    // The fact spans many morsels. Its unselected payload must not make
    // nonnegative frequency folding allocate a group table for each morsel.
    QueryMemoryBudget::Global().ResetForTest(size_t{512} * 1024);
    check(RelationalRun(*rs_, sql), false);
    const auto profile = ExplainAnalyze(*rs_, sql);
    EXPECT_NE(profile.find("streamed_grouped_joins=1"), std::string::npos)
        << profile;
  }
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("streamed_grouped_joins=1"),
            std::string::npos);
  auto old = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  auto prepared = engine.Prepare(old, sql);
  ASSERT_TRUE(prepared.HasValue());
  RelationalRun(*rs_, "UPDATE DenseDim SET v=-1 WHERE k=1");
  std::vector<Row> old_rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    old_rows.push_back(row);
  }
  ASSERT_SUCCESS(prepared.Value()->GetStatus());
  check(old_rows, false);
  check(RelationalRun(*rs_, sql), true);
  EXPECT_EQ(tinylamb::Status::kSuccess, old.Abort());
  RelationalRun(*rs_, "CREATE TABLE DenseOverflow (k INT64,g INT64,v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO DenseOverflow VALUES (1,0,9223372036854775807)");
  RelationalRun(*rs_, "ANALYZE DenseOverflow");
  auto overflow_context = rs_->BeginReadOnlyContext();
  SqlEngine overflow_engine(*rs_);
  auto overflow =
      overflow_engine.Prepare(overflow_context,
                              "SELECT d.g,SUM(d.v) FROM DenseFact f JOIN "
                              "DenseOverflow d ON f.k=d.k GROUP BY d.g");
  ASSERT_TRUE(overflow.HasValue());
  EXPECT_FALSE(overflow.Value()->Next(&row, nullptr));
  EXPECT_EQ(overflow.Value()->GetStatus(), Status::kIsInfinity);
  EXPECT_EQ(tinylamb::Status::kSuccess, overflow_context.Abort());
}

TEST_F(RelationalRegressionTest,
       IntegerKeyCountReuseStillReadsFreshDimensionAndOldFactSnapshots) {
  RelationalRun(*rs_, "CREATE TABLE CachedDim (k INT64,g INT64,v INT64)");
  RelationalRun(*rs_, "INSERT INTO CachedDim VALUES (1,0,3),(2,0,5)");
  RelationalRun(*rs_, "CREATE TABLE CachedFact (k INT64,p STRING)");
  RowPosition first;
  int64_t ones = 0, twos = 0;
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("CachedFact"));
    for (int64_t i = 0; i < 2000; ++i) {
      const Value key = i % 7 == 0 ? Value() : Value((i % 2) + 1);
      ASSIGN_OR_ASSERT_FAIL(
          RowPosition, pos,
          fact->Insert(setup.txn_, Row({key, Value(std::string(64, 'p'))})));
      if (i == 1) {
        first = pos;  // key 2, changed below while old is open.
      }
      if (!key.IsNull()) {
        (i % 2 == 0 ? ones : twos)++;
      }
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE");
  const std::string sql =
      "SELECT d.g,COUNT(*),SUM(d.v) FROM CachedFact f JOIN CachedDim d ON "
      "f.k=d.k GROUP BY d.g";
  SqlEngine engine(*rs_);
  const auto read = [&](TransactionContext& context) {
    std::vector<Row> rows;
    auto plan = engine.Prepare(context, sql);
    EXPECT_TRUE(plan.HasValue());
    if (!plan.HasValue()) {
      return rows;
    }
    Row row;
    while (plan.Value()->Next(&row, nullptr)) {
      rows.push_back(row);
    }
    EXPECT_SUCCESS(plan.Value()->GetStatus());
    return rows;
  };
  auto old = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                        old.GetTable("CachedFact"));
  const auto morsels = fact->BuildScanMorsels(old.txn_);
  std::shared_ptr<const Table::IntegerKeyCounts> counts;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(3);
  do {
    counts = fact->TryReadIntegerKeyCounts(old.txn_, morsels, 0, 1, 2);
    if (counts) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  } while (std::chrono::steady_clock::now() < deadline);
  ASSERT_NE(counts, nullptr);
  const std::vector<Row> initial{
      Row({Value(0), Value(ones + twos), Value((3 * ones) + (5 * twos))})};
  EXPECT_EQ(read(old), initial);
  // Reusing fact counts must never cache the dimension's aggregate payload.
  RelationalRun(*rs_, "UPDATE CachedDim SET v=7 WHERE k=1");
  {
    auto fresh = rs_->BeginReadOnlyContext();
    EXPECT_EQ(read(fresh),
              std::vector<Row>({Row({Value(0), Value(ones + twos),
                                     Value((7 * ones) + (5 * twos))})}));
    EXPECT_EQ(tinylamb::Status::kSuccess, fresh.Abort());
  }
  auto writer = rs_->BeginContext();
  ASSERT_TRUE(fact->Update(writer.txn_, first,
                           Row({Value(1), Value(std::string(64, 'p'))}))
                  .HasValue());
  ASSERT_SUCCESS(writer.PreCommit());
  EXPECT_EQ(read(old), initial);
  {
    auto fresh = rs_->BeginReadOnlyContext();
    EXPECT_EQ(
        read(fresh),
        std::vector<Row>({Row({Value(0), Value(ones + twos),
                               Value((7 * (ones + 1)) + (5 * (twos - 1)))})}));
    EXPECT_EQ(tinylamb::Status::kSuccess, fresh.Abort());
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, old.Abort());
  // Warm the exact histogram after the fact update, then add a positive
  // duplicate dimension key. A cache hit counts it once per dimension row.
  {
    auto warm = rs_->BeginReadOnlyContext();
    std::shared_ptr<const Table::IntegerKeyCounts> warmed;
    const auto until =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    do {
      warmed = fact->TryReadIntegerKeyCounts(warm.txn_, morsels, 0, 1, 2);
      if (warmed) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (std::chrono::steady_clock::now() < until);
    ASSERT_NE(warmed, nullptr);
    RelationalRun(*rs_, "INSERT INTO CachedDim VALUES (2,0,4)");
    auto fresh = rs_->BeginReadOnlyContext();
    EXPECT_EQ(
        read(fresh),
        std::vector<Row>({Row({Value(0), Value(ones + (2 * twos) - 1),
                               Value((7 * (ones + 1)) + (9 * (twos - 1)))})}));
    EXPECT_EQ(tinylamb::Status::kSuccess, fresh.Abort());
    EXPECT_EQ(tinylamb::Status::kSuccess, warm.Abort());
    RelationalRun(*rs_, "DELETE FROM CachedDim WHERE v=4");
  }
  // Columnar input must retain the canonical signed and duplicate-key folds.
  RelationalRun(*rs_, "UPDATE CachedDim SET v=-1 WHERE k=1");
  RelationalRun(*rs_, "INSERT INTO CachedDim VALUES (2,0,4)");
  {
    auto fresh = rs_->BeginReadOnlyContext();
    EXPECT_EQ(read(fresh),
              std::vector<Row>({Row({Value(0), Value(ones + (2 * twos) - 1),
                                     Value(-(ones + 1) + (9 * (twos - 1)))})}));
    EXPECT_EQ(tinylamb::Status::kSuccess, fresh.Abort());
  }
  RelationalRun(*rs_, "DELETE FROM CachedDim WHERE v=4");
  RelationalRun(*rs_, "UPDATE CachedDim SET k=100000 WHERE k=2");
  {
    auto fresh = rs_->BeginReadOnlyContext();
    EXPECT_EQ(read(fresh), std::vector<Row>({Row({Value(0), Value(ones + 1),
                                                  Value(-(ones + 1))})}));
    EXPECT_EQ(tinylamb::Status::kSuccess, fresh.Abort());
  }
  RelationalRun(*rs_, "UPDATE CachedDim SET k=2 WHERE k=100000");
  RelationalRun(*rs_, "UPDATE CachedDim SET v=9223372036854775807 WHERE k=1");
  auto overflow_context = rs_->BeginReadOnlyContext();
  auto overflow = engine.Prepare(overflow_context, sql);
  ASSERT_TRUE(overflow.HasValue());
  Row row;
  EXPECT_FALSE(overflow.Value()->Next(&row, nullptr));
  EXPECT_EQ(overflow.Value()->GetStatus(), Status::kIsInfinity);
  EXPECT_EQ(tinylamb::Status::kSuccess, overflow_context.Abort());
  RelationalRun(*rs_, "DROP TABLE CachedFact");
  RelationalRun(*rs_, "CREATE TABLE CachedFact (k INT64,p STRING)");
  RelationalRun(*rs_, "INSERT INTO CachedFact VALUES (2,'new')");
  RelationalRun(*rs_, "ANALYZE CachedFact");
  auto recreated = rs_->BeginReadOnlyContext();
  EXPECT_EQ(read(recreated),
            std::vector<Row>({Row({Value(0), Value(1), Value(5)})}));
  EXPECT_EQ(tinylamb::Status::kSuccess, recreated.Abort());
}

TEST_F(RelationalRegressionTest,
       ColumnarGroupedJoinKeepsDateNullGroupsAndSmallBudgetFallback) {
  RelationalRun(*rs_, "CREATE TABLE ColumnDim (k INT64,g DATE,v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO ColumnDim VALUES (0,DATE "
                "'2000-01-01',NULL),(1,NULL,2),(2,DATE '2000-01-01',3)");
  RelationalRun(*rs_, "CREATE TABLE ColumnFact (k INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("ColumnFact"));
    for (int64_t i = 0; i < 6000; ++i) {
      ASSERT_TRUE(fact->Insert(setup.txn_, Row({Value(i % 3)})).HasValue());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE");
  const std::string sql =
      "SELECT d.g,COUNT(*),COUNT(d.v),SUM(d.v) FROM ColumnFact f JOIN "
      "ColumnDim d ON f.k=d.k GROUP BY d.g ORDER BY d.g";
  const std::vector<Row> expected{
      Row({Value(), Value(2000), Value(2000), Value(4000)}),
      Row({Value::Date("2000-01-01"), Value(4000), Value(2000), Value(6000)})};
  const auto read = [&] {
    auto context = rs_->BeginReadOnlyContext();
    SqlEngine engine(*rs_);
    auto plan = engine.Prepare(context, sql);
    EXPECT_TRUE(plan.HasValue());
    std::vector<Row> rows;
    if (plan.HasValue()) {
      Row row;
      while (plan.Value()->Next(&row, nullptr)) {
        rows.push_back(row);
      }
      EXPECT_SUCCESS(plan.Value()->GetStatus());
    }
    EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
    return rows;
  };
  EXPECT_EQ(read(), expected);
  EXPECT_EQ(read(), expected);
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(size_t{512} * 1024);
  EXPECT_EQ(read(), expected);
}

TEST_F(RelationalRegressionTest,
       ColumnarGroupedJoinFallsBackWhenDimensionExceedsBound) {
  RelationalRun(*rs_,
                "CREATE TABLE WideColumnDim (k INT64,g INT64,a INT64,b INT64,c "
                "INT64,d INT64,e INT64,f INT64)");
  RelationalRun(*rs_, "CREATE TABLE WideColumnFact (k INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                          setup.GetTable("WideColumnDim"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("WideColumnFact"));
    for (int64_t i = 0; i < 20000; ++i) {
      ASSERT_TRUE(
          dim->Insert(setup.txn_, Row({Value(i), Value(0), Value(1), Value(2),
                                       Value(3), Value(4), Value(5), Value(6)}))
              .HasValue());
    }
    for (int64_t i = 0; i < 40000; ++i) {
      ASSERT_TRUE(fact->Insert(setup.txn_, Row({Value(i % 20000)})).HasValue());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE");
  auto context = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  auto plan = engine.Prepare(
      context,
      "SELECT d.g,SUM(d.a),SUM(d.b),SUM(d.c),SUM(d.d),SUM(d.e),SUM(d.f) FROM "
      "WideColumnFact f JOIN WideColumnDim d ON f.k=d.k GROUP BY d.g");
  ASSERT_TRUE(plan.HasValue());
  Row row;
  ASSERT_TRUE(plan.Value()->Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(0), Value(40000), Value(80000), Value(120000),
                      Value(160000), Value(200000), Value(240000)}));
  EXPECT_FALSE(plan.Value()->Next(&row, nullptr));
  EXPECT_SUCCESS(plan.Value()->GetStatus());
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest,
       SelectivityAdaptiveLSTableKernelsMatchAcrossAllRegimes) {
  RelationalRun(*rs_,
                "CREATE TABLE SelAdaptiveLineitem ("
                "shipday INT64, k_int INT64, rf VARCHAR(4), ls VARCHAR(4), "
                "qty DOUBLE, price DOUBLE, disc DOUBLE, tax DOUBLE)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl,
                          setup.GetTable("SelAdaptiveLineitem"));
    for (int64_t i = 0; i < 2000; ++i) {
      const int64_t shipday = i % 1000;
      const int64_t k_int = i % 10;
      const char* rf = (i % 2 == 0) ? "A" : "B";
      const char* ls = (i % 4 < 2) ? "F" : "O";
      const auto qty = static_cast<double>((i % 50) + 1);
      const double price = 100.0 + static_cast<double>(i % 20);
      const double disc = 0.05 * static_cast<double>(i % 3);
      const double tax = 0.02 * static_cast<double>(i % 4);
      ASSERT_TRUE(
          tbl->Insert(setup.txn_,
                      Row({Value(shipday), Value(k_int), Value(rf), Value(ls),
                           Value(qty), Value(price), Value(disc), Value(tax)}))
              .HasValue());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }

  // Exercise Pattern C (s=100% kDenseAllPass), Pattern B (s=50%
  // kSelectionPrefetch), and Pattern A (s=1% kSparseGather) on the Q1 5-term
  // shape, bounded-int group key, general dict group key, and table scan.
  for (const int64_t threshold : {int64_t{999}, int64_t{499}, int64_t{9}}) {
    const int64_t expected_rows = (threshold + 1) * 2;
    // 1. Q1 5-term shape
    const auto q1_rows = RelationalRun(
        *rs_,
        "SELECT rf, ls, SUM(qty), SUM(price), SUM(price * (1.0 - disc)), "
        "SUM(price * (1.0 - disc) * (1.0 + tax)), AVG(qty), AVG(price), "
        "AVG(disc), COUNT(*) FROM SelAdaptiveLineitem "
        "WHERE shipday <= " +
            std::to_string(threshold) + " GROUP BY rf, ls ORDER BY rf, ls");
    ASSERT_EQ(q1_rows.size(), 4U);
    int64_t total_cnt = 0;
    for (const Row& r : q1_rows) {
      total_cnt += r[9].value.int_value;
    }
    EXPECT_EQ(total_cnt, expected_rows);

    // 2. Bounded-int group key
    const auto bint_rows = RelationalRun(
        *rs_,
        "SELECT k_int, SUM(price * (1.0 - disc)), COUNT(*) "
        "FROM SelAdaptiveLineitem WHERE shipday <= " +
            std::to_string(threshold) + " GROUP BY k_int ORDER BY k_int");
    ASSERT_EQ(bint_rows.size(), 10U);
    int64_t bint_cnt = 0;
    for (const Row& r : bint_rows) {
      bint_cnt += r[2].value.int_value;
    }
    EXPECT_EQ(bint_cnt, expected_rows);

    // 3. General dict group key
    const auto dict_rows = RelationalRun(
        *rs_,
        "SELECT rf, SUM(price * disc), COUNT(*) "
        "FROM SelAdaptiveLineitem WHERE shipday <= " +
            std::to_string(threshold) + " GROUP BY rf ORDER BY rf");
    ASSERT_EQ(dict_rows.size(), 2U);
    EXPECT_EQ(dict_rows[0][2].value.int_value + dict_rows[1][2].value.int_value,
              expected_rows);
  }
}

TEST_F(RelationalRegressionTest,
       WindowApproxQuantilesAndTypedAggregatesRejectInvalidTypes) {
  auto expect_query_error = [&](std::string_view sql) {
    TransactionContext ctx = rs_->BeginContext();
    SqlEngine engine(*rs_);
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    if (!prepared.HasValue()) {
      EXPECT_EQ(tinylamb::Status::kSuccess, ctx.Abort());
      return;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
    EXPECT_EQ(tinylamb::Status::kSuccess, ctx.Abort());
  };

  // Bug 4: DOUBLE count in window APPROX_QUANTILES must not reinterpret IEEE754
  // bits as int64_t and loop 4.6e18 times: valid small DOUBLE converts to int64
  // count, while out-of-range / non-numeric counts fail cleanly.
  const auto quant_rows = RelationalRun(
      *rs_, "SELECT APPROX_QUANTILES(k, 2.0) OVER () FROM SmallKeys");
  ASSERT_EQ(quant_rows.size(), 4U);
  ASSERT_TRUE(quant_rows[0][0].IsArray());
  EXPECT_EQ(quant_rows[0][0].ArrayElements().size(), 3U);
  expect_query_error("SELECT APPROX_QUANTILES(k, 1e20) OVER () FROM SmallKeys");
  expect_query_error(
      "SELECT APPROX_QUANTILES(k, 1000000) OVER () FROM SmallKeys");

  // Bug 6: VARCHAR input to window SUM/AVG/BIT_OR and aggregate BIT_OR must
  // return an error rather than reading union int_value UB.
  RelationalRun(*rs_, "CREATE TABLE WinStr (s STRING)");
  RelationalRun(*rs_, "INSERT INTO WinStr VALUES ('abc'), ('def')");
  expect_query_error("SELECT SUM(s) OVER () FROM WinStr");
  expect_query_error("SELECT AVG(s) OVER () FROM WinStr");
  expect_query_error("SELECT BIT_OR(s) OVER () FROM WinStr");
  expect_query_error("SELECT BIT_OR(s) FROM WinStr");
}

TEST_F(RelationalRegressionTest,
       RelationStickySpillErrorPropagatesThroughFinishSpillAndMerge) {
  relational_detail::Relation rel;
  rel.schema = Schema("s", {Column("k", ValueType::kInt64)});
  rel.spill_tail_ = std::make_shared<SpillFile>();
  rel.AddRow(Row({Value(int64_t{0})}));
  ASSERT_SUCCESS(rel.spill_tail_->FinishWriting());
  // A failed append must preserve the successful tail and its row count.
  rel.AddRow(Row({Value(int64_t{1})}));
  EXPECT_EQ(rel.spilled_rows_, 1U);
  ASSERT_NE(rel.spill_tail_, nullptr);
  EXPECT_EQ(rel.spill_tail_->Count(), 1U);
  EXPECT_FALSE(rel.GetStatus().ok());
  EXPECT_FALSE(rel.FinishSpill().ok());
  EXPECT_FALSE(
      rel.ForEachRow([](const Row&) { return Status::kSuccess; }).ok());

  relational_detail::Relation dst;
  dst.schema = Schema("s", {Column("k", ValueType::kInt64)});
  dst.MergeInMemoryShard(std::move(rel));
  EXPECT_FALSE(dst.GetStatus().ok());
  EXPECT_FALSE(dst.FinishSpill().ok());
}

TEST_F(RelationalRegressionTest,
       FusedGroupedJoinPreservesHavingWhenOutputSpills) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, fact,
      rs_->CreateTable(
          setup, Schema("SpillHavingFact", {Column("fk", ValueType::kInt64),
                                            Column("v", ValueType::kInt64)})));
  for (int64_t i = 0; i < 2000; ++i) {
    const int64_t fk = i % 10;
    const int64_t v = (fk < 5) ? 1 : 10;
    ASSERT_SUCCESS(
        fact.Insert(setup.txn_, Row({Value(fk), Value(v)})).GetStatus());
  }
  ASSERT_SUCCESS(setup.PreCommit());

  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  // Constrain memory budget so the fused grouped-join output spills to disk,
  // and use MIN(f.v) so post_having must run on the post-grouped relation.
  QueryMemoryBudget::Global().ResetForTest(128);
  const std::string sql =
      "SELECT a.k, MIN(f.v), SUM(f.v) FROM SpillHavingFact f "
      "JOIN KeyTable a ON f.fk = a.k "
      "GROUP BY a.k HAVING SUM(f.v) > 500 ORDER BY a.k";
  const auto rows = RelationalRun(*rs_, sql);
  ASSERT_EQ(rows.size(), 5U);
  for (int64_t i = 0; i < 5; ++i) {
    EXPECT_EQ(rows[static_cast<size_t>(i)],
              Row({Value(i + 5), Value(10), Value(2000)}));
  }
}

TEST_F(RelationalRegressionTest, AuditSubqueryInPropagatesComparisonErrors) {
  RelationalRun(*rs_, "CREATE TABLE AuditInText (s STRING)");
  RelationalRun(*rs_, "INSERT INTO AuditInText VALUES ('abc')");
  for (const char* op : {" IN ", " NOT IN "}) {
    auto context = rs_->BeginContext();
    SqlEngine engine(*rs_);
    const std::string sql =
        std::string("SELECT 1") + op + "(SELECT s FROM AuditInText)";
    auto prepared = engine.Prepare(context, sql);
    if (prepared.HasValue()) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) {
      }
      EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
    }
    EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
  }
}

TEST_F(RelationalRegressionTest, AuditDateIntegerFiltersPreserveTypeErrors) {
  RelationalRun(*rs_, "CREATE TABLE AuditDates (d DATE, n INT64)");
  RelationalRun(*rs_, "INSERT INTO AuditDates VALUES (DATE '1970-01-06', 5)");
  for (const char* predicate : {"d = 5", "d <> 6", "d = n", "n = d"}) {
    auto context = rs_->BeginContext();
    SqlEngine engine(*rs_);
    const std::string sql =
        std::string("SELECT d FROM AuditDates WHERE ") + predicate;
    auto prepared = engine.Prepare(context, sql);
    if (prepared.HasValue()) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) {
      }
      EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
    }
    EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
  }
}

TEST_F(RelationalRegressionTest, AuditDoubleZoneMapsKeepNaNNotEqualsRows) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      rs_->CreateTable(setup,
                       Schema("AuditNaNs", {Column("k", ValueType::kInt64),
                                            Column("d", ValueType::kDouble)})));
  for (int64_t i = 0; i < 33000; ++i) {
    const Value value = i < 256 || i % 3 == 0
                            ? Value(std::numeric_limits<double>::quiet_NaN())
                            : (i % 3 == 1 ? Value(5.0) : Value());
    ASSERT_SUCCESS(
        table.Insert(setup.txn_, Row({Value(1), value})).GetStatus());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  RelationalRun(*rs_, "ANALYZE AuditNaNs");
  const int64_t expected = 256 + (32999 / 3 - 255 / 3);
  EXPECT_EQ(RelationalRun(*rs_, "SELECT COUNT(*) FROM AuditNaNs WHERE d <> 5"),
            std::vector<Row>({Row({Value(expected)})}));
  EXPECT_EQ(
      RelationalRun(
          *rs_, "SELECT k, COUNT(*) FROM AuditNaNs WHERE d <> 5 GROUP BY k"),
      std::vector<Row>({Row({Value(1), Value(expected)})}));
  EXPECT_EQ(
      RelationalRun(
          *rs_,
          "SELECT COUNT(*) FROM (SELECT d FROM AuditNaNs WHERE d <> 5) t"),
      std::vector<Row>({Row({Value(expected)})}));
}

TEST_F(RelationalRegressionTest, AuditFilteredMembershipDoesNotRetainNulls) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      rs_->CreateTable(setup, Schema("AuditNullMembership",
                                     {Column("k", ValueType::kInt64)})));
  for (int64_t i = 0; i < 33000; ++i) {
    ASSERT_SUCCESS(table.Insert(setup.txn_, Row({i == 0 ? Value() : Value(1)}))
                       .GetStatus());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  RelationalRun(*rs_, "ANALYZE AuditNullMembership");
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT k FROM SmallKeys WHERE k NOT IN (SELECT k FROM "
                    "AuditNullMembership WHERE k > 0) ORDER BY k"),
      std::vector<Row>({Row({Value(2)}), Row({Value(3)}), Row({Value(4)})}));
}

TEST_F(RelationalRegressionTest, AuditFusedCountsKeepIndependentNullDomains) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "k"), ValueType::kInt64,
                                    Constraint(Constraint::kNotNull))});
  right.schema = Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64),
                              Column(ColumnName("d", "v"), ValueType::kInt64)});
  left.rows = {Row({Value(1)}), Row({Value(2)})};
  right.rows = {Row({Value(1), Value()}), Row({Value(1), Value(7)})};
  const auto key = ColumnValueExp(ColumnName("f", "k"));
  const auto left_count =
      std::make_shared<AggregateExpression>(AggregationType::kCount, key);
  const auto right_count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp(ColumnName("d", "v")));
  for (const auto kind : {JoinType::kInner, JoinType::kLeft}) {
    relational_detail::GroupedJoinFusionSpec fusion;
    fusion.group_keys = {key};
    fusion.select_items = {key, left_count, right_count};
    fusion.select_names = {"k", "l", "r"};
    fusion.aggregates = {left_count.get(), right_count.get()};
    fusion.count_star = {false, false};
    fusion.eligible = true;
    const std::vector<Expression> predicates{BinaryExpressionExp(
        key, BinaryOperation::kEquals, ColumnValueExp(ColumnName("d", "k")))};
    auto result = relational_detail::TryFusedGroupedJoin(
        context, left, right, predicates, kind, nullptr, {}, &fusion);
    ASSERT_TRUE(result.HasValue()) << result.GetStatus();
    auto output = result.MoveValue();
    ASSERT_TRUE(output.has_value());
    if (!output) {
      return;
    }
    std::sort(output->rows.begin(), output->rows.end(),
              [](const Row& a, const Row& b) {
                return a[0].value.int_value < b[0].value.int_value;
              });
    std::vector<Row> expected{Row({Value(1), Value(2), Value(1)})};
    if (kind == JoinType::kLeft) {
      expected.emplace_back(std::vector<Value>{Value(2), Value(1), Value(0)});
    }
    EXPECT_EQ(output->rows, expected);
  }
}

TEST_F(RelationalRegressionTest, AuditWindowQuantilesOrderNaNsConsistently) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      rs_->CreateTable(
          setup, Schema("AuditQuantiles", {Column("v", ValueType::kDouble)})));
  for (int64_t i = 0; i < 1024; ++i) {
    const double value = i % 3 == 0 ? std::numeric_limits<double>::quiet_NaN()
                                    : static_cast<double>(i);
    ASSERT_SUCCESS(table.Insert(setup.txn_, Row({Value(value)})).GetStatus());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  const auto rows = RelationalRun(
      *rs_,
      "SELECT APPROX_QUANTILES(v, 4) OVER () FROM AuditQuantiles LIMIT 1");
  ASSERT_EQ(rows.size(), 1U);
  ASSERT_TRUE(rows[0][0].IsArray());
  const auto& values = rows[0][0].ArrayElements();
  ASSERT_EQ(values.size(), 5U);
  EXPECT_TRUE(std::isnan(values.front().value.double_value));
  EXPECT_EQ(values.back(), Value(1022.0));
  for (size_t i = 1; i < values.size(); ++i) {
    EXPECT_LE(CompareForOrderBy(values[i - 1], values[i]), 0);
  }
}

TEST_F(RelationalRegressionTest, AuditDirectWindowQuantilesUseTotalOrder) {
  auto context = rs_->BeginContext();
  std::vector<Row> rows;
  rows.reserve(1024);
  for (int64_t i = 0; i < 1024; ++i) {
    rows.emplace_back(
        Row({Value(i % 3 == 0 ? std::numeric_limits<double>::quiet_NaN()
                              : static_cast<double>(i))}));
  }
  const Schema schema("q", {Column("v", ValueType::kDouble)});
  WindowFunctionCallExpression window;
  window.function = "APPROX_QUANTILES";
  window.args = {ColumnValueExp("v"), ConstantValueExp(Value(4))};
  std::vector<Value> output;
  ASSERT_EQ(relational_detail::ComputeOneWindow(context, window, rows, schema,
                                                nullptr, {}, &output),
            Status::kSuccess);
  ASSERT_EQ(output.size(), rows.size());
  const Value expected =
      Value::Array({Value(std::numeric_limits<double>::quiet_NaN()),
                    Value(std::numeric_limits<double>::quiet_NaN()),
                    Value(254.0), Value(638.0), Value(1022.0)},
                   "FLOAT64");
  for (const Value& quantiles : output) {
    EXPECT_EQ(quantiles, expected);
  }
}

TEST_F(RelationalRegressionTest, AuditSubqueryStructEscapesFieldContents) {
  const Row injected({Value(R"(x","f2":"y)")});
  const Row two_fields({Value("x"), Value("y")});
  const Value single = relational_detail::ProjectSubqueryRow(injected, true);
  const Value pair = relational_detail::ProjectSubqueryRow(two_fields, true);
  EXPECT_NE(single, pair);
  EXPECT_EQ(single, Value("{\"f1\":\"x\\\",\\\"f2\\\":\\\"y\"}"));
}

TEST_F(RelationalRegressionTest, AuditWindowStatisticsRejectNonNumericValues) {
  auto context = rs_->BeginContext();
  const Schema schema("s", {Column("v", ValueType::kVarChar)});
  std::vector<Row> rows{Row({Value("12")}), Row({Value("34")})};
  for (const std::string function :
       {"APPROX_TOP_SUM", "VAR_POP", "STDDEV_POP", "CORR"}) {
    SCOPED_TRACE(function);
    WindowFunctionCallExpression window;
    window.function = function;
    window.args = {ColumnValueExp("v")};
    if (function == "APPROX_TOP_SUM" || function == "CORR") {
      window.args.push_back(ColumnValueExp("v"));
    }
    if (function == "APPROX_TOP_SUM") {
      window.args.push_back(ConstantValueExp(Value(2)));
    }
    std::vector<Value> output;
    EXPECT_NE(relational_detail::ComputeOneWindow(context, window, rows, schema,
                                                  nullptr, {}, &output),
              Status::kSuccess);
  }
}

TEST_F(RelationalRegressionTest, AuditFilteredInfinityCannotMaskSumOverflow) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      rs_->CreateTable(
          setup, Schema("AuditInfGroups", {Column("g", ValueType::kVarChar),
                                           Column("keep", ValueType::kInt64),
                                           Column("v", ValueType::kDouble)})));
  for (int64_t i = 0; i < 33000; ++i) {
    const double value =
        i == 0 ? std::numeric_limits<double>::infinity() : 1e308;
    ASSERT_TRUE(
        table
            .Insert(setup.txn_,
                    Row({Value("group"), Value(i == 0 ? 0 : 1), Value(value)}))
            .HasValue());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  RelationalRun(*rs_, "ANALYZE AuditInfGroups");
  SqlEngine engine(*rs_);
  auto context = rs_->BeginReadOnlyContext();
  auto prepared = engine.Prepare(
      context,
      "SELECT g,SUM(v),SUM(v) FROM AuditInfGroups WHERE keep=1 GROUP BY g");
  ASSERT_TRUE(prepared.HasValue()) << prepared.GetStatus();
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
  }
  EXPECT_EQ(prepared.Value()->GetStatus(), Status::kIsInfinity);
}

TEST_F(RelationalRegressionTest, AuditRowFillWithVarcharRemainsReusable) {
  RelationalRun(*rs_, "CREATE TABLE AuditRowArena (g INT64,s STRING,v DOUBLE)");
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        setup.GetTable("AuditRowArena"));
  for (int64_t i = 0; i < 33000; ++i) {
    ASSERT_TRUE(
        table
            ->Insert(setup.txn_, Row({Value(i % 2), Value(i % 2 ? "yes" : "no"),
                                      Value(1.0)}))
            .HasValue());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  const std::string branch =
      "SELECT g,SUM(v) AS total FROM AuditRowArena WHERE s='yes' GROUP BY g";
  const auto rows = RelationalRun(*rs_, branch + " UNION ALL " + branch);
  EXPECT_EQ(rows, std::vector<Row>({Row({Value(1), Value(16500.0)}),
                                    Row({Value(1), Value(16500.0)})}));
}

}  // namespace
}  // namespace tinylamb
