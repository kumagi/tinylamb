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

#include "common/test_util.hpp"
#include "executor/detail/build_input_internal.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/relational_regression_test.hpp"

namespace tinylamb {
namespace {
TEST_F(RelationalRegressionTest, CorrelatedLocalFilterAvoidsBuildSpill) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, fact,
      rs_->CreateTable(
          setup, Schema("LocalFact", {Column("fk", ValueType::kInt64),
                                      Column("flag", ValueType::kInt64)})));
  for (int64_t i = 0; i < 3000; ++i) {
    ASSERT_SUCCESS(
        fact.Insert(setup.txn_, Row({i % 101 == 0 ? Value() : Value(i % 10),
                                     Value(i % 100)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(65536);
  const std::string sql =
      "SELECT COUNT(*) FROM KeyTable a WHERE EXISTS "
      "(SELECT 1 FROM LocalFact f WHERE f.fk = a.k AND f.flag = 1)";
  EXPECT_EQ(RelationalRun(*rs_, sql), std::vector<Row>({Row({Value(1)})}));
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("relation_spills=0"), std::string::npos) << profile;
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k FROM KeyTable a WHERE NOT EXISTS "
                          "(SELECT 1 FROM LocalFact f WHERE f.fk = a.k AND "
                          "f.flag = NULL) ORDER BY k")
                .size(),
            10U);
}

TEST_F(RelationalRegressionTest, SelectiveExistsBuildsOneTwoValuedKeySet) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, parent,
      rs_->CreateTable(
          setup, Schema("SelectiveParent", {Column("k", ValueType::kInt64)})));
  ASSIGN_OR_ASSERT_FAIL(
      Table, child,
      rs_->CreateTable(setup, Schema("SelectiveChild",
                                     {Column("k", ValueType::kInt64),
                                      Column("flag", ValueType::kInt64)})));
  int64_t expected = 0;
  for (int i = 0; i < 1000; ++i) {
    const Value key = i % 101 == 0 ? Value() : Value(i % 100);
    expected += static_cast<int64_t>(!key.IsNull() && key == Value(7));
    ASSERT_SUCCESS(parent.Insert(setup.txn_, Row({key})).GetStatus());
  }
  for (int i = 0; i < 2000; ++i) {
    ASSERT_SUCCESS(
        child.Insert(setup.txn_, Row({Value(i % 50), Value(i % 100)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(
      child.Insert(setup.txn_, Row({Value(), Value(7)})).GetStatus());
  ASSERT_SUCCESS(rs_->RefreshStatistics(setup, "SelectiveParent"));
  ASSERT_SUCCESS(rs_->RefreshStatistics(setup, "SelectiveChild"));
  ASSERT_SUCCESS(setup.PreCommit());
  const std::string body =
      "(SELECT 1 FROM SelectiveChild c WHERE c.k = p.k AND c.flag = 7)";
  const std::string sql =
      "SELECT COUNT(*) FROM SelectiveParent p WHERE EXISTS " + body;
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(expected)})}));
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("correlated_index_probes=0"), std::string::npos)
      << profile;
  // Inner NULLs cannot poison a NOT EXISTS miss; outer NULLs survive it.
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT COUNT(*) FROM SelectiveParent p "
                          "WHERE NOT EXISTS " +
                              body),
            std::vector<Row>({Row({Value(1000 - expected)})}));
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT COUNT(*) FROM SelectiveParent p "
                          "WHERE NOT EXISTS (SELECT 1 FROM SelectiveChild c "
                          "WHERE c.k = p.k AND c.flag = NULL)"),
            std::vector<Row>({Row({Value(1000)})}));
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT COUNT(*) FROM SelectiveParent p "
                          "WHERE EXISTS (SELECT 1 FROM SelectiveChild c "
                          "WHERE c.k = p.k AND c.flag = 7 LIMIT 0)"),
            std::vector<Row>({Row({Value(0)})}));
}

void BuildIndexedExistsTables(Database& database) {
  auto setup = database.BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, parent,
      database.CreateTable(
          setup, Schema("IndexedParent", {Column("k", ValueType::kInt64)})));
  ASSIGN_OR_ASSERT_FAIL(
      Table, child,
      database.CreateTable(
          setup,
          Schema("IndexedChild", {Column("k", ValueType::kInt64),
                                  Column("flag", ValueType::kInt64),
                                  Column("payload", ValueType::kInt64)})));
  for (int i = 0; i < 1000; ++i) {
    ASSERT_SUCCESS(
        parent.Insert(setup.txn_, Row({i == 999 ? Value() : Value(i)}))
            .GetStatus());
  }
  for (int i = 0; i < 6000; ++i) {
    ASSERT_SUCCESS(child
                       .Insert(setup.txn_, Row({Value(i % 500), Value(i % 100),
                                                Value(i % 13 == 0 ? -1 : 1)}))
                       .GetStatus());
  }
  ASSERT_SUCCESS(
      child.Insert(setup.txn_, Row({Value(), Value(7), Value(1)})).GetStatus());
  ASSERT_SUCCESS(
      child.Insert(setup.txn_, Row({Value(998), Value(7), Value(-1)}))
          .GetStatus());
  // A composite prefix whose payload does not cover the correlation key.
  ASSERT_SUCCESS(database.CreateIndex(
      setup, "IndexedChild",
      IndexSchema("flag_payload", {1, 2}, {}, IndexMode::kNonUnique)));
  ASSERT_SUCCESS(database.RefreshStatistics(setup, "IndexedParent"));
  ASSERT_SUCCESS(database.RefreshStatistics(setup, "IndexedChild"));
  ASSERT_SUCCESS(setup.PreCommit());
}

constexpr const char* kIndexedExistsSql =
    "SELECT COUNT(*) FROM IndexedParent p WHERE EXISTS "
    "(SELECT 1 FROM IndexedChild c WHERE c.k = p.k AND c.flag = 7 "
    "AND c.payload > 0)";

TEST_F(RelationalRegressionTest,
       IndexedExistsPrefixRechecksRowsAndKeepsNullSemantics) {
  BuildIndexedExistsTables(*rs_);
  const size_t used = QueryMemoryBudget::Global().Used();
  EXPECT_EQ(RelationalRun(*rs_, kIndexedExistsSql),
            std::vector<Row>({Row({Value(5)})}));
  const auto profile = ExplainAnalyze(*rs_, kIndexedExistsSql);
  EXPECT_NE(profile.find("prebuilt_exists_index_builds=1,"), std::string::npos)
      << profile;
  EXPECT_NE(profile.find("prebuilt_exists_index_rows=62,"), std::string::npos)
      << profile;
  EXPECT_NE(profile.find("prebuilt_exists_keys=5,"), std::string::npos)
      << profile;
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT COUNT(*) FROM IndexedParent p WHERE NOT EXISTS "
                    "(SELECT NULL FROM IndexedChild c WHERE p.k = c.k "
                    "AND c.flag = 7 AND c.payload > 0)"),
      std::vector<Row>({Row({Value(995)})}));
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT COUNT(*) FROM IndexedParent p WHERE NOT EXISTS "
                    "(SELECT 1 FROM IndexedChild c WHERE c.k = p.k "
                    "AND c.flag = NULL)"),
      std::vector<Row>({Row({Value(1000)})}));
  EXPECT_EQ(QueryMemoryBudget::Global().Used(), used);
}

TEST_F(RelationalRegressionTest,
       IndexedExistsKeepsSmallBuildDespiteUnderestimatedStatistics) {
  BuildIndexedExistsTables(*rs_);
  auto write = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, child,
                        write.GetTable("IndexedChild"));
  for (int i = 600; i < 695; ++i) {
    ASSERT_SUCCESS(
        child->Insert(write.txn_, Row({Value(i), Value(7), Value(1)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(write.PreCommit());
  // Old stats predict fewer member rows than the 100 distinct keys now
  // present. Capacity slack should absorb this small error without a rescan.
  EXPECT_EQ(RelationalRun(*rs_, kIndexedExistsSql),
            std::vector<Row>({Row({Value(100)})}));
  const auto profile = ExplainAnalyze(*rs_, kIndexedExistsSql);
  EXPECT_NE(profile.find("prebuilt_exists_index_builds=1,"), std::string::npos)
      << profile;
  EXPECT_NE(profile.find("prebuilt_exists_keys=100,"), std::string::npos)
      << profile;
}

TEST_F(RelationalRegressionTest,
       IndexedExistsFallsBackForStaleStatisticsAndMemoryPressure) {
  BuildIndexedExistsTables(*rs_);
  // Add more distinct matching keys than the old ANALYZE estimate can hold.
  auto write = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, child,
                        write.GetTable("IndexedChild"));
  for (int i = 600; i < 800; ++i) {
    ASSERT_SUCCESS(
        child->Insert(write.txn_, Row({Value(i), Value(7), Value(1)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(write.PreCommit());
  EXPECT_EQ(RelationalRun(*rs_, kIndexedExistsSql),
            std::vector<Row>({Row({Value(205)})}));
  const auto stale = ExplainAnalyze(*rs_, kIndexedExistsSql);
  EXPECT_NE(stale.find("prebuilt_exists_index_builds=0,"), std::string::npos)
      << stale;
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  // Even the smallest charged set cannot fit. The ordinary route must still
  // return the complete answer while its outer relation spills.
  QueryMemoryBudget::Global().ResetForTest(128);
  EXPECT_EQ(RelationalRun(*rs_, kIndexedExistsSql),
            std::vector<Row>({Row({Value(205)})}));
  const auto small = ExplainAnalyze(*rs_, kIndexedExistsSql);
  EXPECT_NE(small.find("prebuilt_exists_index_builds=0,"), std::string::npos)
      << small;
  EXPECT_EQ(QueryMemoryBudget::Global().Used(), 0U);
}

TEST_F(RelationalRegressionTest,
       IndexedExistsUsesSnapshotAfterInnerIndexKeyUpdate) {
  BuildIndexedExistsTables(*rs_);
  auto old = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto read_old = [&]() {
    std::vector<Row> rows;
    auto prepared = engine.Prepare(old, kIndexedExistsSql);
    EXPECT_TRUE(prepared.HasValue()) << engine.LastError();
    if (prepared.HasValue()) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) {
        rows.push_back(row);
      }
      EXPECT_TRUE(prepared.Value()->GetStatus().ok());
    }
    return rows;
  };
  EXPECT_EQ(read_old(), std::vector<Row>({Row({Value(5)})}));
  RelationalRun(*rs_, "UPDATE IndexedChild SET flag = 8 WHERE k = 7");
  // The index has removed old flag=7 entries, but this snapshot still sees
  // their old heap versions. Serving the new index would silently lose a key.
  EXPECT_EQ(read_old(), std::vector<Row>({Row({Value(5)})}));
  EXPECT_EQ(RelationalRun(*rs_, kIndexedExistsSql),
            std::vector<Row>({Row({Value(4)})}));
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest, SpilledProbeFeedsGroupedJoinDirectly) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, dimension,
      rs_->CreateTable(setup,
                       Schema("SpillDim", {Column("k", ValueType::kInt64),
                                           Column("g", ValueType::kInt64)})));
  for (int64_t i = 0; i < 10; ++i) {
    ASSERT_SUCCESS(dimension.Insert(setup.txn_, Row({Value(i), Value(i + 100)}))
                       .GetStatus());
  }
  ASSIGN_OR_ASSERT_FAIL(
      Table, fact,
      rs_->CreateTable(setup,
                       Schema("SpillFact", {Column("fk", ValueType::kInt64),
                                            Column("v", ValueType::kInt64)})));
  for (int64_t i = 0; i < 3000; ++i) {
    ASSERT_SUCCESS(
        fact.Insert(setup.txn_,
                    Row({i % 10 == 9 ? Value() : Value(i % 10), Value(2)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(65536);
  // Group by a payload from the dimension so a key-only join reduction cannot
  // replace the join with a predicate over the fact table.
  const std::string sql =
      "SELECT a.g, SUM(f.v), COUNT(*) FROM SpillFact f "
      "JOIN SpillDim a ON f.fk = a.k GROUP BY a.g ORDER BY a.g";
  auto rows = RelationalRun(*rs_, sql);
  ASSERT_EQ(rows.size(), 9U);
  for (int64_t i = 0; i < 9; ++i) {
    EXPECT_EQ(rows[static_cast<size_t>(i)],
              Row({Value(i + 100), Value(600), Value(300)}));
  }
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("grouped_join_fusions=1"), std::string::npos)
      << profile;
}

TEST_F(RelationalRegressionTest, LeftNullKeyFilterKeepsOnlyUnmatchedRows) {
  RelationalRun(*rs_, "CREATE TABLE AntiFact (a INT64,b INT64,v INT64)");
  RelationalRun(*rs_, "CREATE TABLE AntiDim (a INT64,b INT64,p INT64)");
  RelationalRun(*rs_, R"sql(
    INSERT INTO AntiFact VALUES (1,10,1),(1,10,2),(2,10,3),(NULL,10,4),
                               (3,20,5),(1,NULL,6),(4,40,7),(2,10,8)
  )sql");
  RelationalRun(*rs_, R"sql(
    INSERT INTO AntiDim VALUES (1,10,NULL),(1,10,2),(2,NULL,9),
                              (NULL,10,9),(3,20,NULL)
  )sql");
  const std::string join =
      " FROM AntiFact f LEFT JOIN AntiDim d "
      "ON f.a=d.a AND f.b=d.b";
  std::vector<Row> expected;
  expected.reserve(5);
  for (const int v : {3, 4, 6, 7, 8}) {
    expected.emplace_back(Row({Value(v), Value()}));
  }
  EXPECT_EQ(RelationalRun(*rs_, "SELECT f.v,d.p" + join +
                                    " WHERE d.a IS NULL ORDER BY f.v"),
            expected);
  EXPECT_EQ(
      RelationalRun(
          *rs_,
          "SELECT f.v,z.k" + join +
              " FULL JOIN (SELECT 1 AS k UNION ALL SELECT 2) z ON f.v=z.k "
              "WHERE d.a IS NULL ORDER BY f.v NULLS LAST,z.k"),
      expected);
  // NULL payloads can occur in matching rows; they do not prove absence.
  EXPECT_EQ(RelationalRun(*rs_, "SELECT f.v,d.p" + join +
                                    " WHERE d.p IS NULL ORDER BY f.v")
                .size(),
            8U);
  // A failed ON residual makes even an equal-key row unmatched.
  EXPECT_EQ(RelationalRun(*rs_, "SELECT f.v,d.p" + join +
                                    " AND d.p>2 WHERE d.a IS NULL ORDER BY f.v")
                .size(),
            8U);
  // NULL-safe equality may match a NULL key, whose payload must survive.
  const auto null_safe = RelationalRun(*rs_, R"sql(
    SELECT f.v,d.p FROM AntiFact f LEFT JOIN AntiDim d
    ON f.a IS NOT DISTINCT FROM d.a AND f.b=d.b
    WHERE d.a IS NULL ORDER BY f.v
  )sql");
  expected[1] = Row({Value(4), Value(9)});
  EXPECT_EQ(null_safe, expected);
}

TEST_F(RelationalRegressionTest, UnmatchedPackedKeysKeepSignedBoundaries) {
  RelationalRun(*rs_, "CREATE TABLE PackedProbe(a INT64,b INT64)");
  RelationalRun(*rs_, "CREATE TABLE PackedBuild(a INT64,b INT64)");
  RelationalRun(*rs_,
                "INSERT INTO PackedProbe VALUES "
                "(-2147483648,2147483647),(-1,0),(0,-1),(NULL,0),(1,NULL)");
  RelationalRun(*rs_,
                "INSERT INTO PackedBuild VALUES "
                "(-2147483648,2147483647),(-2147483648,2147483647),"
                "(-1,0),(NULL,0),(1,NULL)");
  const std::string sql =
      "SELECT f.a,f.b FROM PackedProbe f LEFT JOIN PackedBuild d "
      "ON f.a=d.a AND f.b=d.b WHERE d.a IS NULL ORDER BY f.a NULLS FIRST";
  std::vector<Row> expected{Row({Value(), Value(0)}),
                            Row({Value(0), Value(-1)}),
                            Row({Value(1), Value()})};
  EXPECT_EQ(RelationalRun(*rs_, sql), expected);
  // A build-only wide value must disable packing before it can alias (0,-1).
  RelationalRun(*rs_, "INSERT INTO PackedBuild VALUES (4294967296,-1)");
  EXPECT_EQ(RelationalRun(*rs_, sql), expected);
  RelationalRun(*rs_, "INSERT INTO PackedProbe VALUES (4294967295,0)");
  expected.emplace_back(Row({Value(int64_t{4294967295}), Value(0)}));
  EXPECT_EQ(RelationalRun(*rs_, sql), expected);
}

TEST_F(RelationalRegressionTest, ParallelUnmatchedLeftJoinPreservesRows) {
  RelationalRun(*rs_, "CREATE TABLE AntiParallel (a INT64,b INT64,v INT64)");
  RelationalRun(*rs_, "CREATE TABLE AntiBuild (a INT64,b INT64,p INT64)");
  RelationalRun(*rs_,
                "INSERT INTO AntiBuild VALUES "
                "(1,10,2),(1,10,NULL),(2,20,3),(NULL,10,4),(3,NULL,5)");
  std::vector<Row> expected;
  expected.reserve(65536);
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("AntiParallel"));
    for (int64_t i = 0; i < 65536; ++i) {
      const Value a =
          i % 4 == 0
              ? Value()
              : Value(i % 11 == 0 ? (int64_t{1} << 32) + (i % 4) : i % 4);
      const Value b = i % 7 == 0 ? Value() : Value(10);
      ASSERT_SUCCESS(
          table->Insert(context.txn_, Row({a, b, Value(i)})).GetStatus());
      if (a.IsNull() || b.IsNull() || a.value.int_value != 1) {
        expected.emplace_back(Row({Value(i), Value()}));
      }
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    const auto rows = RelationalRun(
        *rs_,
        "SELECT f.v,d.p FROM AntiParallel f LEFT JOIN AntiBuild d "
        "ON f.a=d.a AND f.b=d.b WHERE d.a IS NULL ORDER BY f.v");
    EXPECT_EQ(rows, expected);
  }
}

TEST_F(RelationalRegressionTest, LeftOnLocalFilterKeepsNullExtensionAndWhere) {
  RelationalRun(*rs_, "CREATE TABLE OnRight (fk INT64, flag INT64)");
  RelationalRun(*rs_,
                "INSERT INTO OnRight VALUES (1, 1), (1, 1), (2, 0), (NULL, 1)");
  const auto rows = RelationalRun(
      *rs_,
      "SELECT a.k, COUNT(r.fk), COUNT(*) FROM SmallKeys a LEFT JOIN OnRight r "
      "ON a.k = r.fk AND r.flag = 1 GROUP BY a.k ORDER BY a.k");
  EXPECT_EQ(rows, std::vector<Row>({Row({Value(1), Value(2), Value(2)}),
                                    Row({Value(2), Value(0), Value(1)}),
                                    Row({Value(3), Value(0), Value(1)}),
                                    Row({Value(4), Value(0), Value(1)})}));
  EXPECT_EQ(
      RelationalRun(
          *rs_,
          "SELECT a.k FROM SmallKeys a LEFT JOIN OnRight r "
          "ON a.k = r.fk AND r.flag = 1 WHERE r.fk IS NULL ORDER BY a.k"),
      std::vector<Row>({Row({Value(2)}), Row({Value(3)}), Row({Value(4)})}));
}

TEST_F(RelationalRegressionTest, CorrelatedBuildKeepsItsSelectiveDomain) {
  RelationalRun(*rs_,
                "CREATE TABLE CorrFact (part INT64, supplier INT64, v INT64)");
  RelationalRun(*rs_, "CREATE TABLE CorrSupplier (id INT64, flag INT64)");
  RelationalRun(*rs_, "CREATE TABLE CorrOuter (id INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("CorrFact"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, supplier,
                          setup.GetTable("CorrSupplier"));
    for (int64_t p = 0; p < 1000; ++p) {
      for (int64_t s = 0; s < 16; ++s) {
        ASSERT_SUCCESS(fact->Insert(setup.txn_, Row({Value(p), Value(s),
                                                     Value((p * 10) + s)}))
                           .GetStatus());
      }
    }
    for (int64_t s = 0; s < 16; ++s) {
      ASSERT_SUCCESS(supplier->Insert(setup.txn_, Row({Value(s), Value(s % 2)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "INSERT INTO CorrOuter VALUES (2),(503),(999),(NULL)");
  RelationalRun(*rs_, "CREATE INDEX corr_part ON CorrFact(part)");
  for (const auto* table : {"CorrFact", "CorrSupplier", "CorrOuter"}) {
    RelationalRun(*rs_, std::string("ANALYZE ") + table);
  }
  const std::string sql =
      "SELECT d.id FROM CorrOuter d WHERE d.id*10+1 = "
      "(SELECT MIN(f.v) FROM CorrFact f JOIN CorrSupplier s ON f.supplier=s.id "
      "WHERE f.part=d.id AND s.flag=1) ORDER BY d.id";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>(
                {Row({Value(2)}), Row({Value(503)}), Row({Value(999)})}));
  std::string profile;
  for (const auto& row : RelationalRun(*rs_, "EXPLAIN ANALYZE " + sql)) {
    profile += row[0].AsString();
  }
  EXPECT_NE(profile.find("selective_index_scans=1"), std::string::npos)
      << profile;
}

TEST_F(RelationalRegressionTest, ParallelSourceRetainsAllCompiledFilterKinds) {
  RelationalRun(*rs_,
                "CREATE TABLE FilterFact (k INT64, label STRING, v INT64, "
                "padding STRING)");
  RelationalRun(*rs_, "CREATE TABLE FilterDim (k INT64, bucket INT64)");
  RelationalRun(
      *rs_,
      "CREATE TABLE FilterBlocked (k INT64, threshold INT64, reason STRING)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("FilterFact"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                          setup.GetTable("FilterDim"));
    for (int64_t i = 0; i < 32768; ++i) {
      const int64_t key = i % 16;
      ASSERT_SUCCESS(
          fact->Insert(
                  setup.txn_,
                  Row({Value(key),
                       key == 15 ? Value()
                                 : Value(key % 2 == 0 ? "ABvalue" : "ZZvalue"),
                       Value(i % 4), Value(std::string(100, 'x'))}))
              .GetStatus());
    }
    for (int64_t k = 0; k < 16; ++k) {
      ASSERT_SUCCESS(
          dim->Insert(setup.txn_, Row({Value(k), Value(k % 2)})).GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_,
                "INSERT INTO FilterBlocked VALUES (1,5,'Customer abc "
                "Complaints'),(3,NULL,'Customer xyz Complaints'),(4,2,'good')");
  RelationalRun(*rs_, "ANALYZE FilterFact");
  RelationalRun(*rs_, "ANALYZE FilterDim");
  RelationalRun(*rs_, "ANALYZE FilterBlocked");
  const std::string from =
      " FROM FilterFact f JOIN FilterDim d ON f.k=d.k WHERE ";
  const std::string members =
      "(SELECT k FROM FilterBlocked WHERE reason LIKE '%Customer%Complaints%')";
  EXPECT_EQ(RelationalRun(*rs_, "SELECT COUNT(*)" + from + "f.k IN " + members),
            std::vector<Row>({Row({Value(4096)})}));
  EXPECT_EQ(
      RelationalRun(*rs_, "SELECT COUNT(*)" + from + "f.k NOT IN " + members),
      std::vector<Row>({Row({Value(28672)})}));
  EXPECT_EQ(
      RelationalRun(*rs_, "SELECT d.bucket, COUNT(DISTINCT f.k)" + from +
                              "f.k NOT IN " + members +
                              " GROUP BY d.bucket ORDER BY d.bucket"),
      std::vector<Row>({Row({Value(0), Value(8)}), Row({Value(1), Value(6)})}));
  EXPECT_EQ(RelationalRun(*rs_, "SELECT d.bucket, COUNT(*)" + from +
                                    "SUBSTR(f.label,1,2) IN ('AB','CD') GROUP "
                                    "BY d.bucket ORDER BY d.bucket"),
            std::vector<Row>({Row({Value(0), Value(16384)})}));
  EXPECT_EQ(RelationalRun(*rs_, "SELECT COUNT(*)" + from +
                                    "f.v < (SELECT MIN(b.threshold) FROM "
                                    "FilterBlocked b WHERE b.k=f.k)"),
            std::vector<Row>({Row({Value(4096)})}));
  RelationalRun(
      *rs_,
      "INSERT INTO FilterBlocked VALUES (NULL,5,'Customer other Complaints')");
  EXPECT_EQ(
      RelationalRun(*rs_, "SELECT COUNT(*)" + from + "f.k NOT IN " + members),
      std::vector<Row>({Row({Value(0)})}));
}

TEST_F(RelationalRegressionTest,
       SelectiveIndexSourceKeepsHeapOrderAndSnapshot) {
  // The reader and private writer deliberately share a test thread; do not
  // park waiting for an undo reservation only this thread can release.
  rs_->SetDeadlockPolicy(Database::DeadlockPolicy::kNoWait);
  RelationalRun(*rs_, "CREATE TABLE RangeSource (id INT64, k INT64, v STRING)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("RangeSource"));
    for (int64_t i = 0; i < 4000; ++i) {
      ASSERT_TRUE(table
                      ->Insert(setup.txn_, Row({Value(i), Value(3999 - i),
                                                Value("payload")}))
                      .HasValue());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "CREATE INDEX range_k ON RangeSource(k)");
  RelationalRun(*rs_, "ANALYZE RangeSource");
  auto old = rs_->BeginReadOnlyContext();
  SelectSource source;
  source.table = "RangeSource";
  source.alias = "r";
  const std::vector<slot_t> projection{0, 1, 2};
  const std::vector<Expression> predicates{
      BinaryExpressionExp(ColumnValueExp(ColumnName("r", "k")),
                          BinaryOperation::kGreaterThanEquals,
                          ConstantValueExp(Value(3000))),
      BinaryExpressionExp(ColumnValueExp(ColumnName("r", "k")),
                          BinaryOperation::kLessThan,
                          ConstantValueExp(Value(3020)))};
  auto read = [&](TransactionContext& context, size_t* visited) {
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    auto loaded = relational_detail::LoadSource(context, source, nullptr, {},
                                                &projection, &predicates);
    context.set_execution_runtime(nullptr);
    EXPECT_TRUE(loaded.HasValue()) << loaded.GetStatus();
    *visited = runtime.scan_rows;
    std::vector<Row> rows;
    if (loaded.HasValue()) {
      EXPECT_EQ(loaded.Value().ForEachRow(
                    [&](const Row& row) { rows.push_back(row); }),
                Status::kSuccess);
    }
    return rows;
  };
  size_t visited = 0;
  const auto expected = read(old, &visited);
  ASSERT_EQ(expected.size(), 20U);
  EXPECT_LT(visited, 100U);
  for (int64_t i = 0; i < 20; ++i) {
    EXPECT_EQ(expected[static_cast<size_t>(i)],
              Row({Value(980 + i), Value(3019 - i), Value("payload")}));
  }
  // A selective key enclosure remains useful with a local LIKE residual.
  // The canonical residual still decides the rows after heap order is restored.
  for (const auto op : {BinaryOperation::kLike, BinaryOperation::kNotLike}) {
    auto with_like = predicates;
    with_like.push_back(BinaryExpressionExp(
        ColumnValueExp(ColumnName("r", "v")), op,
        ConstantValueExp(
            Value(op == BinaryOperation::kLike ? "%load" : "%other"))));
    relational_detail::ExecutionRuntime runtime;
    old.set_execution_runtime(&runtime);
    auto loaded = relational_detail::LoadSource(old, source, nullptr, {},
                                                &projection, &with_like);
    old.set_execution_runtime(nullptr);
    ASSERT_TRUE(loaded.HasValue()) << loaded.GetStatus();
    std::vector<Row> rows;
    ASSERT_SUCCESS(loaded.Value().ForEachRow(
        [&](const Row& row) { rows.push_back(row); }));
    EXPECT_EQ(rows, expected);
    EXPECT_EQ(runtime.selective_index_scans, 1U);
    EXPECT_LT(runtime.scan_rows, 100U);
  }
  {
    relational_detail::ExecutionRuntime runtime;
    old.set_execution_runtime(&runtime);
    RuntimeFilterBuilder builder;
    builder.Observe(3000);
    builder.Observe(3005);
    const ScanRuntimeFilter filter{
        .group = builder.Build(), .column = 1, .type = ValueType::kInt64};
    auto loaded = relational_detail::LoadSource(old, source, nullptr, {},
                                                &projection, nullptr, &filter);
    ASSERT_TRUE(loaded.HasValue()) << loaded.GetStatus();
    std::vector<Row> rows;
    ASSERT_SUCCESS(loaded.Value().ForEachRow(
        [&](const Row& row) { rows.push_back(row); }));
    ASSERT_EQ(rows.size(), 2U);
    EXPECT_EQ(rows[0][1], Value(3005));
    EXPECT_EQ(rows[1][1], Value(3000));
    EXPECT_EQ(runtime.scan_rows, 2U);
    old.set_execution_runtime(nullptr);
  }
  {
    relational_detail::ExecutionRuntime runtime;
    old.set_execution_runtime(&runtime);
    RuntimeFilterBuilder builder;
    for (int64_t k = 3000; k < 3065; ++k) {
      builder.Observe(k);
    }
    const ScanRuntimeFilter filter{
        .group = builder.Build(), .column = 1, .type = ValueType::kInt64};
    auto loaded = relational_detail::LoadSource(old, source, nullptr, {},
                                                &projection, nullptr, &filter);
    old.set_execution_runtime(nullptr);
    ASSERT_TRUE(loaded.HasValue()) << loaded.GetStatus();
    std::vector<Row> rows;
    ASSERT_SUCCESS(loaded.Value().ForEachRow(
        [&](const Row& row) { rows.push_back(row); }));
    ASSERT_EQ(rows.size(), 65U);
    EXPECT_EQ(runtime.selective_index_scans, 1U);
    EXPECT_EQ(runtime.scan_rows, 65U);
    for (size_t i = 1; i < rows.size(); ++i) {
      EXPECT_GT(rows[i - 1][1].value.int_value, rows[i][1].value.int_value);
    }
  }
  {
    auto only_one = predicates;
    only_one.push_back(InExpressionExp(
        ColumnValueExp(ColumnName("r", "id")),
        {ConstantValueExp(Value(980)), ConstantValueExp(Value())}));
    auto loaded = relational_detail::LoadSource(old, source, nullptr, {},
                                                &projection, &only_one);
    ASSERT_TRUE(loaded.HasValue()) << loaded.GetStatus();
    std::vector<Row> rows;
    ASSERT_SUCCESS(loaded.Value().ForEachRow(
        [&](const Row& row) { rows.push_back(row); }));
    EXPECT_EQ(rows, std::vector<Row>({expected.front()}));
  }
  // A private append can grow and then unlink the heap tail. Page-order
  // metadata must not leak its rows into another snapshot or misorder a
  // later append which reuses the freed page ids.
  for (int round = 0; round < 2; ++round) {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          writer.GetTable("RangeSource"));
    for (int64_t i = 0; i < 20; ++i) {
      ASSERT_TRUE(table
                      ->Insert(writer.txn_,
                               Row({Value(4000 + (round * 20) + i), Value(3001),
                                    Value(std::string(4096, 'a'))}))
                      .HasValue());
    }
    const auto own = read(writer, &visited);
    ASSERT_EQ(own.size(), expected.size() + 20);
    for (size_t i = 0; i < expected.size(); ++i) {
      EXPECT_EQ(own[i], expected[i]);
    }
    for (size_t i = expected.size(); i < own.size(); ++i) {
      EXPECT_EQ(own[i][0], Value(4000 + (round * 20) +
                                 static_cast<int64_t>(i - expected.size())));
    }
    // An index path may answer from the committed image. If it declines,
    // the heap scan must report its unpublished-tail wait failure rather
    // than quietly accepting the prefix as a complete scan (audit medium-44).
    relational_detail::ExecutionRuntime pending_runtime;
    old.set_execution_runtime(&pending_runtime);
    auto pending = relational_detail::LoadSource(old, source, nullptr, {},
                                                 &projection, &predicates);
    old.set_execution_runtime(nullptr);
    if (pending.HasValue()) {
      std::vector<Row> rows;
      ASSERT_SUCCESS(pending.Value().ForEachRow(
          [&](const Row& row) { rows.push_back(row); }));
      EXPECT_EQ(rows, expected);
    } else {
      EXPECT_EQ(pending.GetStatus(), Status::kConflicts);
    }
    EXPECT_EQ(tinylamb::Status::kSuccess, writer.Abort());
    auto fresh = rs_->BeginReadOnlyContext();
    EXPECT_EQ(read(fresh, &visited), expected);
    EXPECT_EQ(tinylamb::Status::kSuccess, fresh.Abort());
  }
  RelationalRun(*rs_, "UPDATE RangeSource SET k=0 WHERE id=980");
  EXPECT_EQ(read(old, &visited), expected);
  auto fresh = rs_->BeginReadOnlyContext();
  EXPECT_EQ(read(fresh, &visited).size(), 19U);
  EXPECT_EQ(tinylamb::Status::kSuccess, fresh.Abort());
  EXPECT_EQ(tinylamb::Status::kSuccess, old.Abort());
}

TEST_F(RelationalRegressionTest,
       MaterializedStarJoinAccumulatesSingleChunkDimGather) {
  // P232: when several fact chunks merge into one columnar output chunk, a
  // single-chunk dimension's gather index must append at `old_sz`, not 0.
  using relational_detail::ColumnarChunk;
  using relational_detail::ColumnarTable;
  using relational_detail::Relation;
  auto context = rs_->BeginContext();
  Relation fact;
  fact.schema = Schema("f", {Column(ColumnName("f", "a"), ValueType::kInt64),
                             Column(ColumnName("f", "b"), ValueType::kInt64),
                             Column(ColumnName("f", "v"), ValueType::kInt64)});
  auto fact_table = std::make_shared<ColumnarTable>();
  const std::vector<Row> batch0{Row({Value(1), Value(10), Value(5)}),
                                Row({Value(2), Value(20), Value(7)})};
  const std::vector<Row> batch1{Row({Value(3), Value(10), Value(9)}),
                                Row({Value(1), Value(20), Value(11)})};
  for (const auto* batch : {&batch0, &batch1}) {
    ColumnarChunk chunk;
    chunk.InitFromSchema(fact.schema, batch->size());
    for (const Row& row : *batch) {
      chunk.AppendRow(row);
    }
    fact_table->AppendChunk(std::move(chunk));
  }
  fact.columnar = std::move(fact_table);
  Relation dim_a;
  dim_a.schema =
      Schema("a", {Column(ColumnName("a", "k"), ValueType::kInt64),
                   Column(ColumnName("a", "payload"), ValueType::kVarChar)});
  dim_a.rows = {Row({Value(1), Value("one")}), Row({Value(2), Value("two")}),
                Row({Value(3), Value("three")})};
  Relation dim_b;
  dim_b.schema =
      Schema("b", {Column(ColumnName("b", "k"), ValueType::kInt64),
                   Column(ColumnName("b", "payload"), ValueType::kVarChar)});
  dim_b.rows = {Row({Value(10), Value("ten")}),
                Row({Value(20), Value("twenty")})};
  const auto col = [](const char* table, const char* name) {
    return ColumnValueExp(ColumnName(table, name));
  };
  std::vector<Relation> relations;
  relations.push_back(std::move(fact));
  relations.push_back(std::move(dim_a));
  relations.push_back(std::move(dim_b));
  std::vector<relational_detail::PredicateInfo> predicates;
  for (const auto& [fact_col, dim_idx] :
       {std::pair{"a", size_t{1}}, std::pair{"b", size_t{2}}}) {
    relational_detail::PredicateInfo edge;
    edge.expression =
        BinaryExpressionExp(col("f", fact_col), BinaryOperation::kEquals,
                            col(dim_idx == 1 ? "a" : "b", "k"));
    edge.sources = {size_t{0}, dim_idx};
    edge.resolved = true;
    predicates.push_back(std::move(edge));
  }
  SelectStatement statement({NamedExpression("v", col("f", "v")),
                             NamedExpression("pa", col("a", "payload")),
                             NamedExpression("pb", col("b", "payload"))},
                            {}, nullptr);
  auto joined = relational_detail::TryMaterializedStarJoin(
      relations, std::vector<uint8_t>{0, 0, 0}, predicates, statement,
      /*columnar_output=*/true);
  ASSERT_TRUE(joined.HasValue()) << joined.GetStatus();
  ASSERT_TRUE(joined.Value().has_value());
  std::vector<Row> materialized;
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes success
  ASSERT_SUCCESS(joined.Value()->ForEachRow(
      [&](const Row& row) { materialized.push_back(row); }));
  const std::vector<Row> expected{
      Row({Value(5), Value("one"), Value("ten")}),
      Row({Value(7), Value("two"), Value("twenty")}),
      Row({Value(9), Value("three"), Value("ten")}),
      Row({Value(11), Value("one"), Value("twenty")})};
  EXPECT_EQ(materialized, expected);
}

TEST_F(RelationalRegressionTest, PromoReturnThresholdCountsMatchesCanonical) {
  // Verifies PromoReturnCounts_sound + ThresholdFanout_sound
  // (DuckDBGaps/Retail.lean) against the canonical plan across:
  //   - NULL cs_promo_sk (no_promo)
  //   - duplicate p_promo_sk rows (multiplies promo and total_cnt)
  //   - duplicate (cr_item_sk, cr_order_number) rows (ret_mult = max(1, cnt))
  //   - NULL inv_quantity_on_hand and boundary inv_qoh == cs_qty vs < cs_qty
  //   - duplicate i_item_desc across distinct i_item_sk
  RelationalRun(*rs_,
                "CREATE TABLE q72_cs (cs_sold_date_sk INT64, "
                "cs_ship_date_sk INT64, cs_item_sk INT64, "
                "cs_bill_cdemo_sk INT64, cs_bill_hdemo_sk INT64, "
                "cs_promo_sk INT64, cs_order_number INT64, "
                "cs_quantity INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE q72_inv (inv_date_sk INT64, inv_item_sk INT64, "
                "inv_warehouse_sk INT64, inv_quantity_on_hand INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE q72_wh (w_warehouse_sk INT64, "
                "w_warehouse_name STRING)");
  RelationalRun(*rs_,
                "CREATE TABLE q72_item (i_item_sk INT64, i_item_desc STRING)");
  RelationalRun(*rs_,
                "CREATE TABLE q72_cd (cd_demo_sk INT64, "
                "cd_marital_status STRING)");
  RelationalRun(*rs_,
                "CREATE TABLE q72_hd (hd_demo_sk INT64, "
                "hd_buy_potential STRING)");
  RelationalRun(*rs_,
                "CREATE TABLE q72_dt (d_date_sk INT64, d_date DATE, "
                "d_week_seq INT64, d_year INT64)");
  RelationalRun(*rs_, "CREATE TABLE q72_promo (p_promo_sk INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE q72_cr (cr_item_sk INT64, "
                "cr_order_number INT64)");

  RelationalRun(*rs_,
                "INSERT INTO q72_dt VALUES "
                "(1, DATE '2001-01-01', 100, 2001), "
                "(2, DATE '2001-01-02', 100, 2001), "
                "(10, DATE '2001-01-06', 100, 2001), "
                "(11, DATE '2001-01-07', 101, 2001), "
                "(20, DATE '2002-01-01', 200, 2002)");
  RelationalRun(*rs_, "INSERT INTO q72_cd VALUES (1, 'D'), (2, 'M'), (3, 'D')");
  RelationalRun(*rs_, "INSERT INTO q72_hd VALUES (1, '>10000'), (2, '0-500')");
  RelationalRun(*rs_, "INSERT INTO q72_wh VALUES (1, 'W1'), (2, 'W2')");
  RelationalRun(*rs_,
                "INSERT INTO q72_item VALUES "
                "(1, 'SharedDesc'), (2, 'SharedDesc'), (3, 'Other')");
  // Duplicate p_promo_sk = 10 to test promo multiplicity = 2.
  RelationalRun(*rs_, "INSERT INTO q72_promo VALUES (10), (10), (20)");
  // Duplicate (cr_item_sk, cr_order_number) = (1, 100) to test ret_mult = 2.
  RelationalRun(*rs_, "INSERT INTO q72_cr VALUES (1, 100), (1, 100), (2, 200)");
  RelationalRun(*rs_,
                "INSERT INTO q72_cs VALUES "
                // d1=1 (2001-01-01), d3=11 (2001-01-07 > +5): passes!
                // promo=10 (dup x2), cr=(1,100) (dup x2) -> promo+=4, total+=4
                "(1, 11, 1, 1, 1, 10, 100, 10), "
                // promo=NULL, no return -> no_promo+=1, total+=1
                "(1, 11, 1, 3, 1, NULL, 101, 5), "
                // d3=10 (2001-01-06 == +5, NOT > +5): filtered out
                "(1, 10, 1, 1, 1, 20, 102, 50), "
                // item=2 shares i_item_desc='SharedDesc', promo=20 (x1),
                // cr=(2,200) (x1) -> promo+=1, total+=1
                "(1, 11, 2, 1, 1, 20, 200, 8), "
                // cd=2 ('M'): filtered out
                "(1, 11, 3, 2, 1, 20, 300, 100)");
  RelationalRun(*rs_,
                "INSERT INTO q72_inv VALUES "
                // inv_qoh=4 < 5 and < 10: matches BOTH item=1 sales
                "(2, 1, 1, 4), "
                // inv_qoh=5: boundary! NOT < 5, YES < 10 -> matches only qty=10
                "(2, 1, 1, 5), "
                // inv_qoh=10: NOT < 10 -> matches neither
                "(2, 1, 1, 10), "
                // inv_qoh=NULL: filtered out
                "(2, 1, 1, NULL), "
                // item=2, inv_qoh=7 < 8: merges into ('SharedDesc', 'W1', 100)
                "(1, 2, 1, 7), "
                // wrong week (d2=20 -> week 200): filtered out
                "(20, 1, 2, 1)");

  const std::string sql =
      "SELECT i_item_desc, w_warehouse_name, d1.d_week_seq, "
      "SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) no_promo, "
      "SUM(CASE WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) promo, "
      "COUNT(*) total_cnt "
      "FROM q72_cs "
      "JOIN q72_inv ON (cs_item_sk = inv_item_sk) "
      "JOIN q72_wh ON (w_warehouse_sk = inv_warehouse_sk) "
      "JOIN q72_item ON (i_item_sk = cs_item_sk) "
      "JOIN q72_cd ON (cs_bill_cdemo_sk = cd_demo_sk) "
      "JOIN q72_hd ON (cs_bill_hdemo_sk = hd_demo_sk) "
      "JOIN q72_dt d1 ON (cs_sold_date_sk = d1.d_date_sk) "
      "JOIN q72_dt d2 ON (inv_date_sk = d2.d_date_sk) "
      "JOIN q72_dt d3 ON (cs_ship_date_sk = d3.d_date_sk) "
      "LEFT OUTER JOIN q72_promo ON (cs_promo_sk = p_promo_sk) "
      "LEFT OUTER JOIN q72_cr ON (cr_item_sk = cs_item_sk AND "
      "cr_order_number = cs_order_number) "
      "WHERE d1.d_week_seq = d2.d_week_seq "
      "AND inv_quantity_on_hand < cs_quantity "
      "AND d3.d_date > d1.d_date + 5 "
      "AND hd_buy_potential = '>10000' "
      "AND d1.d_year = 2001 "
      "AND cd_marital_status = 'D' "
      "GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq "
      "ORDER BY total_cnt DESC, i_item_desc, w_warehouse_name, d1.d_week_seq";

  const auto fast_rows = RelationalRun(*rs_, sql);
  std::vector<Row> canonical_rows;
  {
    ScopedEnvVar disable("TINYLAMB_DISABLE_PROMO_RETURN_THRESHOLD", "1");
    canonical_rows = RelationalRun(*rs_, sql);
  }
  EXPECT_EQ(fast_rows, canonical_rows);
  ASSERT_EQ(fast_rows.size(), 1U);
  // inv_qoh=4: no_promo=1, promo=4, total=5
  // inv_qoh=5: no_promo=0, promo=4, total=4
  // item=2 inv_qoh=7: no_promo=0, promo=1, total=1
  // merged ('SharedDesc', 'W1', 100): no_promo=1, promo=9, total=10
  EXPECT_EQ(fast_rows[0], Row({Value("SharedDesc"), Value("W1"), Value(100),
                               Value(1), Value(9), Value(10)}));
}

TEST_F(RelationalRegressionTest, MarketShareYearRoutesMatchesCanonical) {
  // Verifies MarketShareWeights_sound + YearRoutes_sound
  // (DuckDBGaps/Orders.lean) against the canonical 8-way join across:
  //   - duplicate part, supplier, customer, and orders keys (including an
  //     o_orderkey appearing in two distinct years)
  //   - NULL volume (CASE ELSE 0 still contributes non-NULL 0 to numerator)
  RelationalRun(*rs_, "CREATE TABLE q8_part (p_partkey INT64, p_type STRING)");
  RelationalRun(*rs_,
                "CREATE TABLE q8_supplier (s_suppkey INT64, "
                "s_nationkey INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE q8_lineitem (l_orderkey INT64, l_partkey INT64, "
                "l_suppkey INT64, l_extendedprice FLOAT64, "
                "l_discount FLOAT64)");
  RelationalRun(*rs_,
                "CREATE TABLE q8_orders (o_orderkey INT64, o_custkey INT64, "
                "o_orderdate DATE)");
  RelationalRun(*rs_,
                "CREATE TABLE q8_customer (c_custkey INT64, "
                "c_nationkey INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE q8_nation (n_nationkey INT64, n_name STRING, "
                "n_regionkey INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE q8_region (r_regionkey INT64, r_name STRING)");

  RelationalRun(*rs_,
                "INSERT INTO q8_region VALUES (1, 'AMERICA'), (2, 'ASIA')");
  RelationalRun(*rs_,
                "INSERT INTO q8_nation VALUES "
                "(10, 'PERU', 1), (20, 'BRAZIL', 1), (30, 'JAPAN', 2)");
  // Duplicate c_custkey=1 (multiplicity 2).
  RelationalRun(*rs_,
                "INSERT INTO q8_customer VALUES (1, 10), (1, 20), (2, 30)");
  // Duplicate p_partkey=100 (multiplicity 2).
  RelationalRun(*rs_,
                "INSERT INTO q8_part VALUES "
                "(100, 'STANDARD POLISHED TIN'), "
                "(100, 'STANDARD POLISHED TIN'), "
                "(200, 'OTHER')");
  // Duplicate s_suppkey=500 across PERU and BRAZIL (all=2, chosen=1).
  RelationalRun(*rs_,
                "INSERT INTO q8_supplier VALUES "
                "(500, 10), (500, 20), (600, 30)");
  // o_orderkey=1000 appears in both 1995 and 1996 (tests YearRoutes_sound).
  RelationalRun(*rs_,
                "INSERT INTO q8_orders VALUES "
                "(1000, 1, DATE '1995-06-15'), "
                "(1000, 1, DATE '1996-03-10'), "
                "(1001, 1, DATE '1996-08-20'), "
                "(1002, 2, DATE '1995-07-01'), "
                "(1003, 1, DATE '1998-01-01')");
  RelationalRun(*rs_,
                "INSERT INTO q8_lineitem VALUES "
                "(1000, 100, 500, 100.0, 0.1), "
                "(1000, 100, 600, 200.0, 0.2), "
                "(1001, 100, 500, NULL, 0.1), "
                "(1001, 100, 500, 50.0, 0.0), "
                "(1000, 200, 500, 999.0, 0.0)");

  const std::string sql =
      "SELECT o_year, "
      "SUM(CASE WHEN nation = 'PERU' THEN volume ELSE 0 END) / SUM(volume) "
      "AS mkt_share "
      "FROM (SELECT EXTRACT(year FROM o_orderdate) AS o_year, "
      "             l_extendedprice * (1 - l_discount) AS volume, "
      "             n2.n_name AS nation "
      "      FROM q8_part, q8_supplier, q8_lineitem, q8_orders, q8_customer, "
      "           q8_nation n1, q8_nation n2, q8_region "
      "      WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey "
      "        AND l_orderkey = o_orderkey AND o_custkey = c_custkey "
      "        AND c_nationkey = n1.n_nationkey "
      "        AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA' "
      "        AND s_nationkey = n2.n_nationkey "
      "        AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' "
      "        AND p_type = 'STANDARD POLISHED TIN') all_nations "
      "GROUP BY o_year ORDER BY o_year";

  const auto fast_rows = RelationalRun(*rs_, sql);
  std::vector<Row> canonical_rows;
  {
    ScopedEnvVar disable("TINYLAMB_DISABLE_MARKET_SHARE_ROUTES", "1");
    canonical_rows = RelationalRun(*rs_, sql);
  }
  ASSERT_EQ(fast_rows.size(), canonical_rows.size());
  ASSERT_EQ(fast_rows.size(), 2U);
  for (size_t i = 0; i < fast_rows.size(); ++i) {
    EXPECT_EQ(fast_rows[i][0], canonical_rows[i][0]);
    EXPECT_NEAR(fast_rows[i][1].value.double_value,
                canonical_rows[i][1].value.double_value, 1e-12);
  }
}

TEST_F(RelationalRegressionTest, OrderedFetchTopNMatchesCanonical) {
  // Verifies OrderedFetch_sound (DuckDBGaps/Hits.lean) against canonical
  // execution across:
  //   - SELECT * wide-row late materialization (kInt64, kDate, kDouble,
  //     kVarChar)
  //   - LIKE '%google%' substring filter + ORDER BY string timestamp
  //     ASC NULLS LAST / DESC NULLS FIRST + LIMIT + OFFSET
  //   - Integer ORDER BY with NULLs and range filter
  RelationalRun(*rs_,
                "CREATE TABLE of_hits (id INT64, event_time STRING, "
                "url STRING, d DATE, score FLOAT64, payload STRING)");
  RelationalRun(*rs_,
                "INSERT INTO of_hits VALUES "
                "(1, '2013-07-15 10:00:05', 'https://google.com/a', "
                "DATE '2013-07-15', 1.5, 'wide-1'), "
                "(2, '2013-07-15 10:00:01', 'https://example.org/', "
                "DATE '2013-07-15', 2.5, 'wide-2'), "
                "(3, '2013-07-15 10:00:02', 'https://mail.google.com/b', "
                "DATE '2013-07-15', NULL, 'wide-3'), "
                "(4, NULL, 'https://google.com/null-ts', "
                "DATE '2013-07-15', 4.5, 'wide-4'), "
                "(5, '2013-07-15 10:00:04', 'https://maps.google.com/c', "
                "DATE '2013-07-15', 5.5, 'wide-5'), "
                "(6, '2013-07-15 10:00:03', NULL, "
                "DATE '2013-07-15', 6.5, 'wide-6'), "
                "(7, '2013-07-15 10:00:00', 'https://google.com/earliest', "
                "DATE '2013-07-14', 7.5, 'wide-7')");

  const std::vector<std::string> queries{
      "SELECT * FROM of_hits WHERE url LIKE '%google%' "
      "ORDER BY event_time ASC NULLS LAST LIMIT 3",
      "SELECT * FROM of_hits WHERE url LIKE '%google%' "
      "ORDER BY event_time DESC NULLS FIRST LIMIT 3 OFFSET 1",
      "SELECT id, payload FROM of_hits WHERE url <> '' "
      "ORDER BY id ASC LIMIT 4",
      "SELECT id, d, score FROM of_hits WHERE id >= 3 "
      "ORDER BY id DESC LIMIT 2 OFFSET 1",
  };
  for (const std::string& sql : queries) {
    const auto fast_rows = RelationalRun(*rs_, sql);
    std::vector<Row> canonical_rows;
    {
      ScopedEnvVar disable("TINYLAMB_DISABLE_ORDERED_FETCH", "1");
      canonical_rows = RelationalRun(*rs_, sql);
    }
    EXPECT_EQ(fast_rows, canonical_rows) << sql;
  }
}

TEST_F(RelationalRegressionTest,
       SurrogateIntersectAndExceptCountMatchesCanonical) {
  // Verifies SurrogateIntersectSetCount_sound (Wave9TpcdsRollupAndSetOps.lean)
  // for both INTERSECT DISTINCT (TPC-DS Q38) and EXCEPT DISTINCT (TPC-DS Q87),
  // including:
  //   - two distinct customer_sk rows sharing the exact same (last, first) name
  //   - NULL last/first names (NULL == NULL under DISTINCT/INTERSECT/EXCEPT)
  //   - duplicate fact rows per channel
  RelationalRun(*rs_,
                "CREATE TABLE si_dt (d_date_sk INT64, d_date DATE, "
                "d_month_seq INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE si_cust (c_customer_sk INT64, "
                "c_last_name STRING, c_first_name STRING)");
  RelationalRun(*rs_,
                "CREATE TABLE si_ss (ss_sold_date_sk INT64, "
                "ss_customer_sk INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE si_cs (cs_sold_date_sk INT64, "
                "cs_bill_customer_sk INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE si_ws (ws_sold_date_sk INT64, "
                "ws_bill_customer_sk INT64)");

  RelationalRun(*rs_,
                "INSERT INTO si_dt VALUES "
                "(1, DATE '2001-01-01', 1188), "
                "(2, DATE '2001-01-02', 1189), "
                "(3, DATE '2005-01-01', 1250)");
  // c_customer_sk=1 and 2 share ('Smith', 'Alice'); 3 has (NULL, 'Bob').
  RelationalRun(*rs_,
                "INSERT INTO si_cust VALUES "
                "(1, 'Smith', 'Alice'), "
                "(2, 'Smith', 'Alice'), "
                "(3, NULL, 'Bob'), "
                "(4, 'Jones', 'Carol')");
  RelationalRun(*rs_,
                "INSERT INTO si_ss VALUES "
                "(1, 1), (1, 1), (1, 3), (2, 4), (3, 1)");
  // cs uses c_customer_sk=2 (same name as 1!) and c_customer_sk=3 (NULL name).
  RelationalRun(*rs_, "INSERT INTO si_cs VALUES (1, 2), (1, 3), (2, 1)");
  RelationalRun(*rs_, "INSERT INTO si_ws VALUES (1, 1), (1, 3)");

  const std::string q38 =
      "SELECT COUNT(*) FROM ("
      "  SELECT DISTINCT c_last_name, c_first_name, d_date "
      "  FROM si_ss, si_dt, si_cust "
      "  WHERE ss_sold_date_sk = d_date_sk AND ss_customer_sk = c_customer_sk "
      "    AND d_month_seq BETWEEN 1188 AND 1199 "
      "  INTERSECT DISTINCT "
      "  SELECT DISTINCT c_last_name, c_first_name, d_date "
      "  FROM si_cs, si_dt, si_cust "
      "  WHERE cs_sold_date_sk = d_date_sk "
      "    AND cs_bill_customer_sk = c_customer_sk "
      "    AND d_month_seq BETWEEN 1188 AND 1199 "
      "  INTERSECT DISTINCT "
      "  SELECT DISTINCT c_last_name, c_first_name, d_date "
      "  FROM si_ws, si_dt, si_cust "
      "  WHERE ws_sold_date_sk = d_date_sk "
      "    AND ws_bill_customer_sk = c_customer_sk "
      "    AND d_month_seq BETWEEN 1188 AND 1199"
      ") hot_cust LIMIT 100";

  const std::string q87 =
      "SELECT COUNT(*) FROM ("
      "  SELECT DISTINCT c_last_name, c_first_name, d_date "
      "  FROM si_ss, si_dt, si_cust "
      "  WHERE ss_sold_date_sk = d_date_sk AND ss_customer_sk = c_customer_sk "
      "    AND d_month_seq BETWEEN 1188 AND 1199 "
      "  EXCEPT DISTINCT "
      "  SELECT DISTINCT c_last_name, c_first_name, d_date "
      "  FROM si_cs, si_dt, si_cust "
      "  WHERE cs_sold_date_sk = d_date_sk "
      "    AND cs_bill_customer_sk = c_customer_sk "
      "    AND d_month_seq BETWEEN 1188 AND 1199 "
      "  EXCEPT DISTINCT "
      "  SELECT DISTINCT c_last_name, c_first_name, d_date "
      "  FROM si_ws, si_dt, si_cust "
      "  WHERE ws_sold_date_sk = d_date_sk "
      "    AND ws_bill_customer_sk = c_customer_sk "
      "    AND d_month_seq BETWEEN 1188 AND 1199"
      ") cool_cust";

  for (const auto& [sql, expected_cnt] :
       {std::pair{q38, int64_t{2}}, std::pair{q87, int64_t{1}}}) {
    const auto fast_rows = RelationalRun(*rs_, sql);
    std::vector<Row> canonical_rows;
    {
      ScopedEnvVar disable("TINYLAMB_DISABLE_SURROGATE_INTERSECT", "1");
      canonical_rows = RelationalRun(*rs_, sql);
    }
    EXPECT_EQ(fast_rows, canonical_rows) << sql;
    ASSERT_EQ(fast_rows.size(), 1U);
    EXPECT_EQ(fast_rows[0], Row({Value(expected_cnt)}));
  }
}

TEST_F(RelationalRegressionTest, TwoPassGroupAvgThresholdMatchesCanonical) {
  // Verifies TwoPassGroupAvgThreshold_sound (IndividualQueryGaps.lean) on the
  // TPC-DS Q65 self-join shape, including NULL sales prices, NULL store keys,
  // and exact boundary revenue == 0.1 * avg(revenue).
  RelationalRun(*rs_,
                "CREATE TABLE tp_store (s_store_sk INT64, "
                "s_store_name STRING)");
  RelationalRun(*rs_,
                "CREATE TABLE tp_item (i_item_sk INT64, i_item_desc STRING)");
  RelationalRun(*rs_,
                "CREATE TABLE tp_sales (ss_store_sk INT64, ss_item_sk INT64, "
                "ss_sales_price FLOAT64)");

  RelationalRun(*rs_,
                "INSERT INTO tp_store VALUES (1, 'StoreA'), (2, 'StoreB')");
  RelationalRun(*rs_,
                "INSERT INTO tp_item VALUES "
                "(10, 'Cheap'), (20, 'Mid'), (30, 'Expensive'), "
                "(40, 'NullItem')");
  // Store 1: item 10 = 10.0, item 20 = 90.0, item 30 = 200.0 -> avg = 100.0,
  // 0.1 * avg = 10.0 -> item 10 (10.0 <= 10.0) qualifies!
  // item 40 has NULL price -> skipped by AVG and by <= comparison.
  // Store 2: item 10 = 5.0, item 20 = 95.0 -> avg = 50.0, 0.1 * avg = 5.0 ->
  // item 10 qualifies!
  RelationalRun(*rs_,
                "INSERT INTO tp_sales VALUES "
                "(1, 10, 4.0), (1, 10, 6.0), "
                "(1, 20, 90.0), (1, 30, 200.0), (1, 40, NULL), "
                "(2, 10, 5.0), (2, 20, 95.0), "
                "(NULL, 10, 1.0)");

  const std::string sql =
      "SELECT s_store_name, i_item_desc, sc.revenue "
      "FROM tp_store, tp_item, ("
      "  SELECT ss_store_sk, AVG(revenue) AS ave "
      "  FROM ("
      "    SELECT ss_store_sk, ss_item_sk, SUM(ss_sales_price) AS revenue "
      "    FROM tp_sales GROUP BY ss_store_sk, ss_item_sk"
      "  ) AS sa GROUP BY ss_store_sk"
      ") AS sb, ("
      "  SELECT ss_store_sk, ss_item_sk, SUM(ss_sales_price) AS revenue "
      "  FROM tp_sales GROUP BY ss_store_sk, ss_item_sk"
      ") AS sc "
      "WHERE sb.ss_store_sk = sc.ss_store_sk "
      "  AND sc.revenue <= 0.1 * sb.ave "
      "  AND s_store_sk = sc.ss_store_sk "
      "  AND i_item_sk = sc.ss_item_sk "
      "ORDER BY s_store_name, i_item_desc";

  const auto fast_rows = RelationalRun(*rs_, sql);
  std::vector<Row> canonical_rows;
  {
    ScopedEnvVar disable("TINYLAMB_DISABLE_TWO_PASS_GROUP_AVG", "1");
    canonical_rows = RelationalRun(*rs_, sql);
  }
  EXPECT_EQ(fast_rows, canonical_rows);
  ASSERT_EQ(fast_rows.size(), 2U);
  EXPECT_EQ(fast_rows[0], Row({Value("StoreA"), Value("Cheap"), Value(10.0)}));
  EXPECT_EQ(fast_rows[1], Row({Value("StoreB"), Value("Cheap"), Value(5.0)}));
}

TEST_F(RelationalRegressionTest,
       FrequencyUpperBoundHavingPruneMatchesCanonical) {
  // Verifies FrequencyUpperBoundHavingPrune_sound (IndividualQueryGaps.lean)
  // on GROUP BY key HAVING COUNT(*) > T / >= T with secondary AVG/MIN/MAX.
  RelationalRun(*rs_, "CREATE TABLE hfp_t (k INT64, v FLOAT64, tag INT64)");
  std::string ins = "INSERT INTO hfp_t VALUES ";
  bool first = true;
  for (int k = 1; k <= 120; ++k) {
    const int reps = (k <= 3) ? 15 : 2;
    for (int r = 0; r < reps; ++r) {
      if (!first) {
        ins += ", ";
      }
      first = false;
      ins += "(" + std::to_string(k) + ", " +
             (r == 0 && k == 2 ? std::string("NULL")
                               : std::to_string((k * 10) + r)) +
             ", " + std::to_string(r) + ")";
    }
  }
  RelationalRun(*rs_, ins);
  for (const std::string& sql : {
           "SELECT k, COUNT(*) AS c, AVG(v) AS av, MIN(tag) AS mn "
           "FROM hfp_t GROUP BY k HAVING COUNT(*) > 10 ORDER BY k",
           "SELECT k, COUNT(*) AS c, AVG(v) AS av, MAX(tag) AS mx "
           "FROM hfp_t GROUP BY k HAVING COUNT(*) >= 15 ORDER BY k",
       }) {
    const auto fast_rows = RelationalRun(*rs_, sql);
    std::vector<Row> canonical_rows;
    {
      ScopedEnvVar disable("TINYLAMB_DISABLE_HAVING_FREQ_PRUNE", "1");
      canonical_rows = RelationalRun(*rs_, sql);
    }
    EXPECT_EQ(fast_rows, canonical_rows) << sql;
    ASSERT_EQ(fast_rows.size(), 3U);
  }
}

TEST_F(RelationalRegressionTest,
       CompositeKeySemiJoinChainReductionMatchesCanonical) {
  // Verifies CompositeKeySemiJoinChainReduction_sound
  // (IndividualQueryGaps.lean) on a 3-table composite-key join chain
  // A ⋈_{(a1,a2)} B ⋈_{(b1,b2)} C with duplicates and NULLs.
  RelationalRun(*rs_, "CREATE TABLE ck_a (a1 INT64, a2 INT64, va INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE ck_b (b1 INT64, b2 INT64, j1 INT64, j2 INT64)");
  RelationalRun(*rs_, "CREATE TABLE ck_c (c1 INT64, c2 INT64, vc INT64)");
  RelationalRun(*rs_,
                "INSERT INTO ck_a VALUES (1, 10, 100), (2, 20, 200), "
                "(1, NULL, 999)");
  std::string ins_b =
      "INSERT INTO ck_b VALUES (1, 10, 7, 70), (1, 10, 7, 70), "
      "(2, 20, 8, 80), (1, NULL, 7, 70)";
  for (int i = 10; i < 120; ++i) {
    ins_b += ", (" + std::to_string(i) + ", " + std::to_string(i * 10) + ", " +
             std::to_string(i) + ", " + std::to_string(i * 100) + ")";
  }
  RelationalRun(*rs_, ins_b);
  std::string ins_c =
      "INSERT INTO ck_c VALUES (7, 70, 5), (8, 80, 6), (7, NULL, 9)";
  for (int i = 10; i < 200; ++i) {
    ins_c += ", (" + std::to_string(i) + ", " + std::to_string(i * 100) + ", " +
             std::to_string(i) + ")";
  }
  RelationalRun(*rs_, ins_c);

  const std::string sql =
      "SELECT a.a1, a.va, c.vc "
      "FROM ck_a a "
      "JOIN ck_b b ON a.a1 = b.b1 AND a.a2 = b.b2 "
      "JOIN ck_c c ON b.j1 = c.c1 AND b.j2 = c.c2 "
      "ORDER BY a.a1, a.va, c.vc";
  const auto fast_rows = RelationalRun(*rs_, sql);
  std::vector<Row> canonical_rows;
  {
    ScopedEnvVar disable("TINYLAMB_DISABLE_COMPOSITE_SEMIJOIN", "1");
    canonical_rows = RelationalRun(*rs_, sql);
  }
  EXPECT_EQ(fast_rows, canonical_rows);
  ASSERT_EQ(fast_rows.size(), 3U);
}

TEST_F(RelationalRegressionTest, DirectionBucketsMatchesCanonical) {
  // Verifies DirectionBuckets_sound (DuckDBGaps/Orders.lean) on TPC-H Q7
  // including duplicate keys, NULL volume, and opposite nation routes.
  RelationalRun(*rs_, "CREATE TABLE q7_nat (n_nationkey INT64, n_name STRING)");
  RelationalRun(*rs_,
                "CREATE TABLE q7_sup (s_suppkey INT64, s_nationkey INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE q7_cust (c_custkey INT64, c_nationkey INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE q7_ord (o_orderkey INT64, o_custkey INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE q7_line (l_orderkey INT64, l_suppkey INT64, "
                "l_shipdate DATE, l_extendedprice FLOAT64, "
                "l_discount FLOAT64)");
  RelationalRun(*rs_,
                "INSERT INTO q7_nat VALUES (1, 'FRANCE'), (2, 'GERMANY'), "
                "(3, 'JAPAN'), (4, NULL)");
  RelationalRun(*rs_, "INSERT INTO q7_sup VALUES (10, 1), (20, 2), (30, 3)");
  RelationalRun(*rs_,
                "INSERT INTO q7_cust VALUES (100, 2), (200, 1), (300, 4)");
  RelationalRun(*rs_,
                "INSERT INTO q7_ord VALUES (1000, 100), (2000, 200), "
                "(3000, 300)");
  RelationalRun(*rs_,
                "INSERT INTO q7_line VALUES "
                "(1000, 10, DATE '1995-05-01', 100.0, 0.1), "
                "(1000, 10, DATE '1995-06-01', NULL, 0.1), "
                "(2000, 20, DATE '1996-02-01', 200.0, 0.2), "
                "(1000, 20, DATE '1995-05-01', 999.0, 0.0), "
                "(3000, 10, DATE '1995-05-01', 500.0, 0.0), "
                "(1000, 10, DATE '1997-01-01', 700.0, 0.0)");

  const std::string sql =
      "SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue "
      "FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, "
      "             EXTRACT(year FROM l_shipdate) AS l_year, "
      "             l_extendedprice * (1 - l_discount) AS volume "
      "      FROM q7_sup, q7_line, q7_ord, q7_cust, q7_nat n1, q7_nat n2 "
      "      WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey "
      "        AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey "
      "        AND c_nationkey = n2.n_nationkey "
      "        AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') "
      "          OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')) "
      "        AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') "
      "     shipping "
      "GROUP BY supp_nation, cust_nation, l_year "
      "ORDER BY supp_nation, cust_nation, l_year";
  const auto fast_rows = RelationalRun(*rs_, sql);
  std::vector<Row> canonical_rows;
  {
    ScopedEnvVar disable("TINYLAMB_DISABLE_DIRECTION_BUCKETS", "1");
    canonical_rows = RelationalRun(*rs_, sql);
  }
  ASSERT_EQ(fast_rows.size(), canonical_rows.size());
  ASSERT_EQ(fast_rows.size(), 2U);
  for (size_t i = 0; i < fast_rows.size(); ++i) {
    EXPECT_EQ(fast_rows[i][0], canonical_rows[i][0]);
    EXPECT_EQ(fast_rows[i][1], canonical_rows[i][1]);
    EXPECT_EQ(fast_rows[i][2], canonical_rows[i][2]);
    EXPECT_NEAR(fast_rows[i][3].value.double_value,
                canonical_rows[i][3].value.double_value, 1e-12);
  }
}

TEST_F(RelationalRegressionTest, RetireWitnessExistsMatchesCanonical) {
  // Verifies LateSlack_sound + RetireWitness_sound (DuckDBGaps/Orders.lean)
  // on TPC-H Q4 including duplicate outer orders and paired-date checks.
  RelationalRun(*rs_,
                "CREATE TABLE q4_ord (o_orderkey INT64, o_orderdate DATE, "
                "o_orderpriority STRING)");
  RelationalRun(*rs_,
                "CREATE TABLE q4_line (l_orderkey INT64, l_commitdate DATE, "
                "l_receiptdate DATE)");
  // Duplicate o_orderkey=1 in '1-URGENT' must both be counted once retired.
  RelationalRun(*rs_,
                "INSERT INTO q4_ord VALUES "
                "(1, DATE '1993-07-10', '1-URGENT'), "
                "(1, DATE '1993-07-11', '1-URGENT'), "
                "(2, DATE '1993-08-01', '2-HIGH'), "
                "(3, DATE '1993-08-15', '2-HIGH'), "
                "(4, DATE '1994-01-01', '1-URGENT')");
  // Order 1 has two late lines (must only retire once, retaining multiplicity
  // 2); Order 2 has commit == receipt (NOT late); Order 3 has NULL commitdate
  // on one line and late on another.
  RelationalRun(*rs_,
                "INSERT INTO q4_line VALUES "
                "(1, DATE '1993-07-12', DATE '1993-07-15'), "
                "(1, DATE '1993-07-13', DATE '1993-07-20'), "
                "(2, DATE '1993-08-05', DATE '1993-08-05'), "
                "(2, DATE '1993-08-10', DATE '1993-08-09'), "
                "(3, NULL, DATE '1993-09-01'), "
                "(3, DATE '1993-08-20', DATE '1993-08-25')");

  const std::string sql =
      "SELECT o_orderpriority, COUNT(*) AS order_count FROM q4_ord "
      "WHERE o_orderdate >= DATE '1993-07-01' "
      "  AND o_orderdate < DATE_ADD(DATE '1993-07-01', INTERVAL 3 MONTH) "
      "  AND EXISTS (SELECT * FROM q4_line "
      "              WHERE l_orderkey = o_orderkey "
      "                AND l_commitdate < l_receiptdate) "
      "GROUP BY o_orderpriority ORDER BY o_orderpriority";
  const auto fast_rows = RelationalRun(*rs_, sql);
  std::vector<Row> canonical_rows;
  {
    ScopedEnvVar disable("TINYLAMB_DISABLE_RETIRE_WITNESS", "1");
    canonical_rows = RelationalRun(*rs_, sql);
  }
  EXPECT_EQ(fast_rows, canonical_rows);
  ASSERT_EQ(fast_rows.size(), 2U);
  EXPECT_EQ(fast_rows[0], Row({Value("1-URGENT"), Value(2)}));
  EXPECT_EQ(fast_rows[1], Row({Value("2-HIGH"), Value(1)}));
}

TEST_F(RelationalRegressionTest, RebucketLeftJoinHistMatchesCanonical) {
  // Verifies RebucketOrders_sound (DuckDBGaps/Orders.lean) on TPC-H Q13,
  // including 0-order customers, excluded comments, and NULL comments.
  RelationalRun(*rs_, "CREATE TABLE q13_cust (c_custkey INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE q13_ord (o_orderkey INT64, o_custkey INT64, "
                "o_comment STRING)");
  RelationalRun(*rs_, "INSERT INTO q13_cust VALUES (1), (2), (3), (4), (5)");
  RelationalRun(*rs_,
                "INSERT INTO q13_ord VALUES "
                "(10, 1, 'normal order'), "
                "(11, 1, 'very unusual big packages here'), "
                "(12, 2, 'ok one'), "
                "(13, 2, 'ok two'), "
                "(14, 3, NULL), "
                "(15, 4, 'unusual packages')");

  const std::string sql =
      "SELECT c_count, COUNT(*) AS custdist "
      "FROM (SELECT c_custkey, COUNT(o_orderkey) AS c_count "
      "      FROM q13_cust LEFT OUTER JOIN q13_ord "
      "        ON c_custkey = o_custkey "
      "       AND o_comment NOT LIKE '%unusual%packages%' "
      "      GROUP BY c_custkey) c_orders "
      "GROUP BY c_count ORDER BY custdist DESC, c_count DESC";
  const auto fast_rows = RelationalRun(*rs_, sql);
  std::vector<Row> canonical_rows;
  {
    ScopedEnvVar disable("TINYLAMB_DISABLE_REBUCKET_ORDERS", "1");
    canonical_rows = RelationalRun(*rs_, sql);
  }
  EXPECT_EQ(fast_rows, canonical_rows);
  // c_custkey 3, 4, 5 have 0 qualifying orders; 1 has 1; 2 has 2.
  ASSERT_EQ(fast_rows.size(), 3U);
  EXPECT_EQ(fast_rows[0], Row({Value(0), Value(3)}));
  EXPECT_EQ(fast_rows[1], Row({Value(2), Value(1)}));
  EXPECT_EQ(fast_rows[2], Row({Value(1), Value(1)}));
}

TEST_F(RelationalRegressionTest, ContiguousRevenueRunsMatchesCanonical) {
  // Verifies RevenueRuns_sound (DuckDBGaps/Orders.lean) on TPC-H Q3,
  // including contiguous runs, NULL prices, and Top-K tie-breaking by date.
  RelationalRun(*rs_,
                "CREATE TABLE q3_cust (c_custkey INT64, c_mktsegment STRING)");
  RelationalRun(*rs_,
                "CREATE TABLE q3_ord (o_orderkey INT64, o_custkey INT64, "
                "o_orderdate DATE, o_shippriority INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE q3_line (l_orderkey INT64, l_shipdate DATE, "
                "l_extendedprice FLOAT64, l_discount FLOAT64)");
  RelationalRun(*rs_,
                "INSERT INTO q3_cust VALUES (1, 'BUILDING'), (2, 'AUTO'), "
                "(3, 'BUILDING')");
  RelationalRun(*rs_,
                "INSERT INTO q3_ord VALUES "
                "(10, 1, DATE '1995-03-01', 0), "
                "(20, 1, DATE '1995-03-10', 1), "
                "(30, 2, DATE '1995-03-05', 0), "
                "(40, 3, DATE '1995-03-02', 2)");
  RelationalRun(*rs_,
                "INSERT INTO q3_line VALUES "
                "(10, DATE '1995-03-20', 100.0, 0.1), "
                "(10, DATE '1995-03-25', 50.0, 0.0), "
                "(20, DATE '1995-03-20', 200.0, 0.25), "
                "(30, DATE '1995-03-20', 999.0, 0.0), "
                "(40, DATE '1995-03-20', 140.0, 0.0), "
                "(40, DATE '1995-03-10', 500.0, 0.0)");

  const std::string sql =
      "SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, "
      "       o_orderdate, o_shippriority "
      "FROM q3_cust, q3_ord, q3_line "
      "WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey "
      "  AND l_orderkey = o_orderkey "
      "  AND o_orderdate < DATE '1995-03-15' "
      "  AND l_shipdate > DATE '1995-03-15' "
      "GROUP BY l_orderkey, o_orderdate, o_shippriority "
      "ORDER BY revenue DESC, o_orderdate LIMIT 2";
  const auto fast_rows = RelationalRun(*rs_, sql);
  std::vector<Row> canonical_rows;
  {
    ScopedEnvVar disable("TINYLAMB_DISABLE_REVENUE_RUNS", "1");
    canonical_rows = RelationalRun(*rs_, sql);
  }
  ASSERT_EQ(fast_rows.size(), canonical_rows.size());
  ASSERT_EQ(fast_rows.size(), 2U);
  for (size_t i = 0; i < fast_rows.size(); ++i) {
    EXPECT_EQ(fast_rows[i][0], canonical_rows[i][0]);
    EXPECT_NEAR(fast_rows[i][1].value.double_value,
                canonical_rows[i][1].value.double_value, 1e-12);
    EXPECT_EQ(fast_rows[i][2], canonical_rows[i][2]);
    EXPECT_EQ(fast_rows[i][3], canonical_rows[i][3]);
  }
}

TEST_F(RelationalRegressionTest,
       HitsDatePostingsAndStridedPointMatchCanonical) {
  // Verifies DatePostings_sound (whole-block interior predicate skip) and
  // StridedPoint_sound (FOR code-domain point equality) from
  // DuckDBGaps/Hits.lean.
  RelationalRun(*rs_,
                "CREATE TABLE hits_dp (counter_id INT64, event_date DATE, "
                "user_id INT64, url STRING)");
  std::string ins = "INSERT INTO hits_dp VALUES ";
  bool first = true;
  for (int i = 0; i < 600; ++i) {
    if (!first) {
      ins += ", ";
    }
    first = false;
    const int cid = (i % 3 == 0) ? 62 : 10;
    const std::string dt =
        (i < 200) ? "DATE '2013-07-01'"
                  : (i < 400 ? "DATE '2013-07-10'" : "DATE '2013-07-20'");
    const int64_t uid = 1000000LL + ((i % 50) * 4LL);
    const std::string url = (i % 4 == 0) ? "''" : "'https://example.com'";
    ins += "(";
    ins += std::to_string(cid);
    ins += ", ";
    ins += dt;
    ins += ", ";
    ins += std::to_string(uid);
    ins += ", ";
    ins += url;
    ins += ")";
  }
  RelationalRun(*rs_, ins);

  // DatePostings: interior block (2013-07-10) satisfies both date bounds while
  // boundary/outside blocks are filtered per row or rejected.
  const auto dp =
      RelationalRun(*rs_,
                    "SELECT COUNT(*) FROM hits_dp "
                    "WHERE counter_id = 62 AND event_date >= DATE '2013-07-05' "
                    "  AND event_date <= DATE '2013-07-15' AND url <> ''");
  ASSERT_EQ(dp.size(), 1U);
  int64_t expected_dp = 0;
  for (int i = 200; i < 400; ++i) {
    if ((i % 3 == 0) && (i % 4 != 0)) {
      ++expected_dp;
    }
  }
  EXPECT_EQ(dp[0], Row({Value(expected_dp)}));

  // StridedPoint: congruent hit vs non-congruent miss.
  const auto sp_hit = RelationalRun(
      *rs_, "SELECT COUNT(*) FROM hits_dp WHERE user_id = 1000040");
  ASSERT_EQ(sp_hit.size(), 1U);
  EXPECT_EQ(sp_hit[0], Row({Value(12)}));

  const auto sp_miss = RelationalRun(
      *rs_, "SELECT COUNT(*) FROM hits_dp WHERE user_id = 1000041");
  ASSERT_EQ(sp_miss.size(), 1U);
  EXPECT_EQ(sp_miss[0], Row({Value(0)}));
}

}  // namespace
}  // namespace tinylamb
