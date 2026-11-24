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

#include <cstddef>

#include "common/test_util.hpp"
#include "executor/detail/relational_sort_limit.hpp"
#include "executor/relational_regression_test.hpp"

namespace tinylamb {
namespace {
TEST_F(RelationalRegressionTest, EmptyJoinDoesNotBuildUnusedTableCache) {
  auto context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context.GetTable("SmallKeys"));
  ASSERT_EQ(table->GetLSTableStore(), nullptr);
  auto empty = std::make_shared<relational_detail::Relation>();
  empty->schema = Schema("empty", {Column("k", ValueType::kInt64)});
  const relational_detail::CteMap ctes{{"empty", empty}};
  SelectSource first;
  first.table = "empty";
  SelectSource second;
  second.table = "SmallKeys";
  second.join_type = JoinType::kInner;
  second.join_condition = BinaryExpressionExp(
      ColumnValueExp(ColumnName("empty", "k")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("SmallKeys", "k")));
  SelectStatement statement(
      {NamedExpression("k", ColumnValueExp(ColumnName("SmallKeys", "k")))}, {},
      nullptr);
  statement.SetSources({first, second});
  bool applied = false;
  ASSIGN_OR_ASSERT_FAIL(relational_detail::Relation, result,
                        relational_detail::BuildInput(context, statement,
                                                      nullptr, ctes, &applied));
  EXPECT_EQ(result.TotalRows(), 0);
  EXPECT_EQ(table->GetLSTableStore(), nullptr);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(RelationalRegressionTest,
       RuntimeFilterOwnerIncludesStatementGeneration) {
  SelectStatement statement;
  relational_detail::TableKeyFilter filter;
  filter.SetOwner(statement);
  ASSERT_TRUE(filter.OwnedBy(statement));
  statement.SetWhereClause(nullptr);
  EXPECT_FALSE(filter.OwnedBy(statement));
  filter.SetOwner(statement);
  SelectStatement replacement(statement);
  statement = replacement;
  EXPECT_FALSE(filter.OwnedBy(statement));
}

TEST_F(RelationalRegressionTest,
       FoldedHavingUnionDoesNotReuseSiblingKeyFilters) {
  const auto rows = RelationalRun(*rs_, R"sql(
    WITH cutoff AS (SELECT 0 AS n)
    SELECT k, SUM(n) AS total FROM (
      SELECT a.k, COUNT(*) AS n
      FROM AggTable a, KeyTable d, SmallKeys i
      WHERE a.k = d.k AND a.k = i.k AND d.k < 3
      GROUP BY a.k HAVING COUNT(*) > (SELECT n FROM cutoff)
      UNION ALL
      SELECT a.k, COUNT(*) AS n
      FROM AggTable a, KeyTable d, SmallKeys i
      WHERE a.k = d.k AND a.k = i.k AND d.k >= 3
      GROUP BY a.k HAVING COUNT(*) > (SELECT n FROM cutoff)
      UNION ALL
      SELECT a.k, COUNT(*) AS n
      FROM AggTable a, KeyTable d, SmallKeys i
      WHERE a.k = d.k AND a.k = i.k
      GROUP BY a.k HAVING COUNT(*) > (SELECT n FROM cutoff)
    ) u GROUP BY ROLLUP(k) ORDER BY k NULLS LAST
  )sql");
  ASSERT_EQ(rows.size(), 5);
  for (int64_t k = 1; k <= 4; ++k) {
    EXPECT_EQ(rows[static_cast<size_t>(k - 1)], Row({Value(k), Value(40)}));
  }
  EXPECT_EQ(rows[4], Row({Value(), Value(160)}));
}

TEST_F(RelationalRegressionTest, CorrelatedDisjunctKeysKeepResidualAndNulls) {
  RelationalRun(*rs_, "CREATE TABLE OrKeys (k INT64, v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO OrKeys VALUES (1,1),(1,2),(1,9),(2,2),(NULL,1)");
  const std::string sql =
      "SELECT d.k, (SELECT COUNT(*) FROM OrKeys a WHERE "
      "(a.k=d.k AND a.v=1) OR (d.k=a.k AND a.v=2)) AS n "
      "FROM (SELECT 1 AS k UNION ALL SELECT 2 UNION ALL SELECT NULL "
      "UNION ALL SELECT 3) d ORDER BY d.k NULLS LAST";
  EXPECT_EQ(
      RelationalRun(*rs_, sql),
      std::vector<Row>({Row({Value(1), Value(2)}), Row({Value(2), Value(1)}),
                        Row({Value(3), Value(0)}), Row({Value(), Value(0)})}));
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("correlated_index_builds=1"),
            std::string::npos);
  EXPECT_EQ(RelationalRun(
                *rs_,
                "SELECT (SELECT COUNT(*) FROM OrKeys a WHERE "
                "(a.k=d.k AND a.v=1) OR a.v=2) FROM SmallKeys d ORDER BY d.k"),
            std::vector<Row>({Row({Value(3)}), Row({Value(2)}), Row({Value(2)}),
                              Row({Value(2)})}));
}

TEST_F(RelationalRegressionTest, CorrelatedStringDisjunctUsesIndexedAggregate) {
  RelationalRun(*rs_, "CREATE TABLE OrStrings (k STRING, v INT64)");
  RelationalRun(
      *rs_,
      "INSERT INTO OrStrings VALUES ('a',1),('a',2),('a',9),('A',2),(NULL,1)");
  const std::string sql =
      "SELECT d.k,(SELECT COUNT(*) FROM OrStrings a WHERE "
      "(a.k=d.k AND a.v=1) OR (a.k=d.k AND a.v=2)) FROM "
      "(SELECT 'a' AS k UNION ALL SELECT 'A' UNION ALL SELECT 'missing' "
      "UNION ALL SELECT NULL) d ORDER BY d.k NULLS LAST";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>(
                {Row({Value("A"), Value(1)}), Row({Value("a"), Value(2)}),
                 Row({Value("missing"), Value(0)}), Row({Value(), Value(0)})}));
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("correlated_index_builds=1"), std::string::npos);
  EXPECT_NE(profile.find("correlated_result_cache_hits=2"), std::string::npos)
      << profile;
}

TEST_F(RelationalRegressionTest,
       CorrelatedAliasRetainsResidualColumnsBeyondSharedProjection) {
  RelationalRun(*rs_, "CREATE TABLE AliasExists (k INT64, v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO AliasExists VALUES "
                "(1,1),(1,2),(1,NULL),(2,4),(2,4),(3,NULL),(NULL,9)");
  // Both outer a (referenced by a.v <> b.v) and inner b need {k, v}, so the
  // cache key includes both columns (14 values decoded for 7 rows) and reuses
  // that full projection rather than discarding v.
  const std::string sql =
      "SELECT k FROM AliasExists a WHERE EXISTS "
      "(SELECT * FROM AliasExists b WHERE a.k=b.k AND a.v<>b.v) ORDER BY k";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(1)}), Row({Value(1)})}));
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("correlated_index_builds=1,"), std::string::npos)
      << profile;
  EXPECT_NE(profile.find("base_scan_cache_hits=1,"), std::string::npos)
      << profile;
  EXPECT_NE(profile.find("scan_values_decoded=14,"), std::string::npos)
      << profile;

  // When outer a only needs k while inner b needs v, Cascades decorrelates to
  // SemiHashJoin and projects only a.k on the outer branch while filtering b.v
  // on the inner branch.
  const std::string asymmetric_sql =
      "SELECT k FROM AliasExists a WHERE EXISTS "
      "(SELECT * FROM AliasExists b WHERE a.k=b.k AND b.v<>1) ORDER BY k";
  EXPECT_EQ(RelationalRun(*rs_, asymmetric_sql),
            std::vector<Row>({Row({Value(1)}), Row({Value(1)}), Row({Value(1)}),
                              Row({Value(2)}), Row({Value(2)})}));
  const auto asymmetric_profile = ExplainAnalyze(*rs_, asymmetric_sql);
  EXPECT_NE(asymmetric_profile.find("SemiHashJoin"), std::string::npos)
      << asymmetric_profile;
}

TEST_F(RelationalRegressionTest, DerivedStarKeepsBaseScanProjectionNarrow) {
  RelationalRun(*rs_,
                "CREATE TABLE NarrowScan (k INT64, v INT64, spare STRING)");
  RelationalRun(*rs_, "INSERT INTO NarrowScan VALUES (1,3,'a'),(2,5,'b')");
  const std::string sql =
      "SELECT * FROM (SELECT COUNT(v) AS n FROM NarrowScan WHERE k = 1) a, "
      "(SELECT SUM(v) AS total FROM NarrowScan WHERE k = 2) b";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(1), Value(5)})}));
  const std::string profile = ExplainAnalyze(*rs_, sql);
  constexpr std::string_view kDecoded = "scan_values_decoded=";
  constexpr std::string_view kAvailable = "scan_values_available=";
  const auto decoded = profile.find(kDecoded);
  const auto available = profile.find(kAvailable);
  ASSERT_NE(decoded, std::string::npos) << profile;
  ASSERT_NE(available, std::string::npos) << profile;
  // Scalar aggregate fusion may share one scan between the subqueries.
  // In either plan, the scan decodes k and v and leaves spare unread.
  EXPECT_LT(std::stoul(profile.substr(decoded + kDecoded.size())),
            std::stoul(profile.substr(available + kAvailable.size())))
      << profile;
}

TEST_F(RelationalRegressionTest, SubqueryCacheTracksOnlyReferencedCteBindings) {
  using namespace relational_detail;
  ExecutionRuntime runtime;
  auto context = rs_->BeginReadOnlyContext();
  context.set_execution_runtime(&runtime);
  SelectStatement subquery(
      {NamedExpression("k", ColumnValueExp(ColumnName("k")))}, {}, nullptr);
  SelectSource base;
  base.table = "SmallKeys";
  subquery.SetSources({base});
  auto cte = std::make_shared<Relation>();
  cte->schema = Schema("", {Column("k", ValueType::kInt64)});
  const CteMap unrelated{{"other", cte}};
  const Relation* cached =
      ExecuteCachedUncorrelated(context, subquery, unrelated, false);
  ASSERT_NE(cached, nullptr);
  EXPECT_GT(cached->TotalRows(), 0U);
  EXPECT_EQ(ExecuteCachedUncorrelated(context, subquery, unrelated, false),
            cached);

  const CteMap shadowing{{"SmallKeys", cte}};
  const Relation* shadowed =
      ExecuteCachedUncorrelated(context, subquery, shadowing, false);
  ASSERT_NE(shadowed, nullptr);
  EXPECT_EQ(shadowed->TotalRows(), 0U);
  auto rebound_cte = std::make_shared<Relation>();
  rebound_cte->schema = cte->schema;
  rebound_cte->AddRow(Row({Value(42)}));
  const CteMap rebound{{"SmallKeys", rebound_cte}, {"other", cte}};
  const Relation* rebound_result =
      ExecuteCachedUncorrelated(context, subquery, rebound, false);
  ASSERT_NE(rebound_result, nullptr);
  EXPECT_EQ(rebound_result->TotalRows(), 1U);
  EXPECT_EQ(ExecuteCachedUncorrelated(context, subquery, unrelated, false),
            cached);

  // The same statement object only exercises the pointer-keyed fast path. Use
  // a separately allocated, structurally identical AST to reach the CTE
  // structural key, and make sure a different CTE binding never reuses it.
  SelectStatement equivalent(
      {NamedExpression("k", ColumnValueExp(ColumnName("k")))}, {}, nullptr);
  SelectSource equivalent_base;
  equivalent_base.table = "SmallKeys";
  equivalent.SetSources({equivalent_base});
  const size_t hits_before = runtime.uncorrelated_cache_hits;
  const Relation* equivalent_shadowed =
      ExecuteCachedUncorrelated(context, equivalent, shadowing, false);
  ASSERT_NE(equivalent_shadowed, nullptr);
  EXPECT_EQ(equivalent_shadowed, shadowed);
  EXPECT_GT(runtime.uncorrelated_cache_hits, hits_before);
  const Relation* equivalent_rebound =
      ExecuteCachedUncorrelated(context, equivalent, rebound, false);
  ASSERT_NE(equivalent_rebound, nullptr);
  EXPECT_EQ(equivalent_rebound->TotalRows(), 1U);
  EXPECT_EQ(ExecuteCachedUncorrelated(context, equivalent, shadowing, false),
            shadowed);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(RelationalRegressionTest, CorrelatedExistsStarPrunesUnusedJoinPayload) {
  RelationalRun(*rs_,
                "CREATE TABLE WideExists(k INT64, flag INT64, payload STRING)");
  RelationalRun(*rs_,
                "INSERT INTO WideExists VALUES "
                "(1,2,'a'),(1,NULL,'b'),(2,3,'c'),(3,1,'d')");
  const std::string sql = R"sql(
    SELECT d.k FROM SmallKeys d WHERE EXISTS (
      SELECT * FROM WideExists f,KeyTable b
      WHERE f.k=d.k AND f.flag=b.k AND b.k>=2) ORDER BY d.k
  )sql";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(1)}), Row({Value(2)})}));
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("correlated_index_builds=1,"), std::string::npos)
      << profile;
  // A row-valued dependency in the residual must still keep its payload.
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT d.k FROM SmallKeys d WHERE EXISTS (
      SELECT * FROM WideExists f,KeyTable b WHERE f.k=d.k AND f.flag=b.k
      AND f.payload='c') ORDER BY d.k
  )sql"),
            std::vector<Row>({Row({Value(2)})}));
}

TEST_F(RelationalRegressionTest, SingleUseCteFiltersCrossLeftJoinTowardBuild) {
  const std::string sql = R"sql(
    WITH a AS (SELECT k AS ak, SUM(v) AS av FROM AggTable GROUP BY k),
         b AS (SELECT k AS bk, SUM(v) AS bv FROM AggTable GROUP BY k),
         c AS (SELECT k AS ck, SUM(v) AS cv FROM AggTable WHERE k <> 2 GROUP BY k)
    SELECT ak, av, bv, cv FROM a LEFT JOIN b ON ak=bk LEFT JOIN c ON bk=ck
    WHERE ak=2
  )sql";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(2), Value(20), Value(20), Value()})}));
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("aggregate_input_rows=40,"), std::string::npos)
      << profile;
  // A nullable-side ON condition must not restrict the preserved input.
  EXPECT_EQ(
      RelationalRun(*rs_, R"sql(
    WITH a AS (SELECT k AS ak, SUM(v) AS av FROM AggTable GROUP BY k),
         b AS (SELECT k AS bk FROM AggTable WHERE k=1 GROUP BY k)
    SELECT ak, bk FROM a LEFT JOIN b ON ak=bk WHERE ak>=1 AND ak<=2 ORDER BY ak
  )sql"),
      std::vector<Row>({Row({Value(1), Value(1)}), Row({Value(2), Value()})}));
}

TEST_F(RelationalRegressionTest, ParallelFilterMovesOwnedRowBuffers) {
  auto context = rs_->BeginContext();
  relational_detail::Relation relation;
  relation.schema = Schema("f", {Column("k", ValueType::kInt64),
                                 Column("text", ValueType::kVarChar)});
  std::vector<const Value*> buffers;
  buffers.reserve(40000);
  for (int64_t k = 0; k < 40000; ++k) {
    relation.AddRow(
        Row({Value(k), k % 3 == 0 ? Value() : Value(std::string(80, 'x'))}));
    buffers.push_back(relation.rows.back().values_.data());
  }
  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget budget(&active, 2);
  ASSERT_SUCCESS(relational_detail::FilterRelation(
      context, &relation,
      {BinaryExpressionExp(ColumnValueExp(ColumnName("k")),
                           BinaryOperation::kLessThan,
                           ConstantValueExp(Value(30000)))},
      nullptr, {}));
  ASSERT_EQ(relation.rows.size(), 30000U);
  std::vector<bool> seen(30000);
  for (const auto& row : relation.rows) {
    const auto key = static_cast<size_t>(row[0].value.int_value);
    ASSERT_LT(key, seen.size());
    EXPECT_FALSE(seen[key]);
    seen[key] = true;
    EXPECT_EQ(row.values_.data(), buffers[key]);
    EXPECT_EQ(row[1].IsNull(), key % 3 == 0);
  }
}

TEST_F(RelationalRegressionTest, ColumnarResidualUsesValueTruthiness) {
  auto context = rs_->BeginContext();
  relational_detail::Relation source;
  source.schema = Schema(
      "f", {Column("id", ValueType::kInt64), Column("d", ValueType::kDate),
            Column("s", ValueType::kVarChar)});
  const std::vector<Row> rows{
      Row({Value(1), Value::Date("1969-12-31"), Value("")}),
      Row({Value(2), Value::Date("1970-01-01"), Value("x")}),
      Row({Value(3), Value(), Value()})};
  auto table =
      relational_detail::BuildColumnarTableFromRows(source.schema, rows);
  ASSERT_TRUE(table.HasValue());
  source.SetColumnarTable(table.MoveValue(), false);
  ASSERT_TRUE(source.HasColumnarOnly());

  const Expression d = ColumnValueExp(ColumnName("d"));
  for (const Expression& predicate :
       {ColumnValueExp(ColumnName("s")), d,
        BinaryExpressionExp(d, BinaryOperation::kAdd,
                            ConstantValueExp(Value(1)))}) {
    // Use the real compiler so this pins *where* the predicate lands. Moving
    // a VARCHAR/DATE truthiness test into the simple/payload path would still
    // select the same rows here but regress in another query shape.
    relational_detail::CompiledScanFilter filter =
        relational_detail::CompileScanFilter({predicate}, source.schema);
    ASSERT_EQ(filter.residual.size(), 1U)
        << "truthiness predicate left the residual path";
    EXPECT_TRUE(filter.simple.empty());
    EXPECT_TRUE(filter.prefix_in.empty());
    EXPECT_TRUE(filter.prebuilt_membership.empty());
    EXPECT_TRUE(filter.prebuilt_scalar_compare.empty());
    relational_detail::Relation result;
    result.schema = source.schema;
    Status error{Status::kSuccess};
    ASSERT_TRUE(relational_detail::TryParallelFilterRows(
        context, source, filter, source.schema, nullptr, {}, nullptr, &result,
        &error, nullptr));
    ASSERT_SUCCESS(error);
    // A columnar filter yields a columnar result; a silent fallback to
    // row-at-a-time evaluation would leave a row-backed result.
    EXPECT_TRUE(result.HasColumnarOnly());
    std::vector<Row> matched;
    ASSERT_SUCCESS(
        result.ForEachRow([&](const Row& row) { matched.push_back(row); }));
    EXPECT_EQ(matched, (std::vector<Row>{rows[0], rows[1]}));
  }
}

TEST_F(RelationalRegressionTest, StringColumnFiltersCompareTimestampInstants) {
  auto context = rs_->BeginContext();
  using namespace relational_detail;
  Relation source;
  source.schema = Schema("f", {Column("a", ValueType::kVarChar),
                               Column("b", ValueType::kVarChar)});
  const std::vector<Row> rows{
      Row({Value("2021-01-01 00:00:00.0+00:00"),
           Value("2021-01-01T00:00:00Z")}),
      Row({Value("2021-01-01T01:00:00+02:00"), Value("2021-01-01T00:00:00Z")}),
      Row({Value("plain"), Value("plain")}), Row({Value("plain"), Value()})};
  auto table = BuildColumnarTableFromRows(source.schema, rows);
  ASSERT_TRUE(table.HasValue());
  source.SetColumnarTable(table.MoveValue(), false);

  BaseRelationArena arena;
  arena.stride = 2;
  arena.slot_types = {ValueType::kVarChar, ValueType::kVarChar};
  arena.null_bits.resize(2);
  arena.varchars.resize(2);
  for (const Row& row : rows) {
    const std::array<int64_t, 2> cells{};
    const std::array<uint8_t, 2> nulls{static_cast<uint8_t>(row[0].IsNull()),
                                       static_cast<uint8_t>(row[1].IsNull())};
    arena.AppendRun(cells.data(), nulls.data(), 1);
    for (size_t column = 0; column < 2; ++column) {
      auto& run = arena.varchars[column];
      if (!row[column].IsNull()) {
        const std::string_view value = row[column].value.varchar_value;
        run.bytes.insert(run.bytes.end(), value.begin(), value.end());
      }
      run.ends.push_back(run.bytes.size());
    }
  }

  for (const BinaryOperation op :
       {BinaryOperation::kEquals, BinaryOperation::kLessThan}) {
    const auto filter = CompileScanFilter(
        {BinaryExpressionExp(ColumnValueExp(ColumnName("a")), op,
                             ColumnValueExp(ColumnName("b")))},
        source.schema);
    ASSERT_EQ(filter.simple.size(), 1U);
    ASSERT_TRUE(filter.simple[0].column_payload);
    const std::vector<Row> expected = op == BinaryOperation::kEquals
                                          ? std::vector<Row>{rows[0], rows[2]}
                                          : std::vector<Row>{rows[1]};
    std::vector<Row> matched;
    for (const Row& row : rows) {
      if (MatchScanFilter(row, source.schema, filter, nullptr, context, {})) {
        matched.push_back(row);
      }
    }
    EXPECT_EQ(matched, expected);

    Relation columnar_result;
    columnar_result.schema = source.schema;
    Relation arena_result;
    arena_result.schema = source.schema;
    Status error{Status::kSuccess};
    ASSERT_TRUE(TryParallelFilterRows(context, source, filter, source.schema,
                                      nullptr, {}, nullptr, &columnar_result,
                                      &error, nullptr));
    ASSERT_SUCCESS(error);
    ASSERT_TRUE(TryFilterArenaCache(context, arena, source.schema, &filter,
                                    &source.schema, nullptr, {}, nullptr,
                                    &arena_result, &error));
    ASSERT_SUCCESS(error);
    for (const Relation* result : {&columnar_result, &arena_result}) {
      matched.clear();
      ASSERT_SUCCESS(
          result->ForEachRow([&](const Row& row) { matched.push_back(row); }));
      EXPECT_EQ(matched, expected);
    }
  }
}

TEST_F(RelationalRegressionTest, ParallelFiltersReportCollatedLikeErrors) {
  auto context = rs_->BeginContext();
  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget budget(&active, 2);
  using namespace relational_detail;
  Relation source;
  source.schema = Schema(
      "f", {Column("id", ValueType::kInt64), Column("s", ValueType::kVarChar)});
  constexpr int64_t kRows = 16384;
  for (int64_t id = 0; id < kRows; ++id) {
    source.rows.emplace_back(Row({Value(id), Value("axb").WithCollation(2)}));
  }
  Relation columnar;
  columnar.schema = source.schema;
  auto table = BuildColumnarTableFromRows(columnar.schema, source.rows);
  ASSERT_TRUE(table.HasValue());
  columnar.SetColumnarTable(table.MoveValue(), false);
  const Expression like = BinaryExpressionExp(ColumnValueExp(ColumnName("s")),
                                              BinaryOperation::kLike,
                                              ConstantValueExp(Value("a_b")));
  const Expression reject = BinaryExpressionExp(
      ColumnValueExp(ColumnName("id")), BinaryOperation::kLessThan,
      ConstantValueExp(Value(0)));
  for (const Relation* input : {&source, &columnar}) {
    for (bool masked : {false, true}) {
      const auto filter =
          CompileScanFilter(masked ? std::vector<Expression>{like, reject}
                                   : std::vector<Expression>{like},
                            input->schema);
      Relation result;
      result.schema = input->schema;
      Status error{Status::kSuccess};
      ASSERT_TRUE(TryParallelFilterRows(context, *input, filter, input->schema,
                                        nullptr, {}, nullptr, &result, &error,
                                        nullptr));
      if (masked) {
        EXPECT_SUCCESS(error);
        EXPECT_EQ(result.TotalRows(), 0U);
      } else {
        EXPECT_FALSE(error.ok());
        EXPECT_NE(error.GetMessage().find("LIKE pattern has '_'"),
                  std::string::npos);
      }
    }
  }
}

TEST_F(RelationalRegressionTest, WithTiesStreamsSpilledPeerGroup) {
  auto& budget = QueryMemoryBudget::Global();
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions):
                          // scope-owned cleanup.
    size_t limit;
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore{budget.Limit()};
  budget.ResetForTest(static_cast<size_t>(64 * 1024));

  relational_detail::Relation input;
  input.schema = Schema("sorted", {Column("k", ValueType::kInt64),
                                   Column("id", ValueType::kInt64),
                                   Column("payload", ValueType::kVarChar)});
  input.EnsureSpill();
  for (int64_t id = 0; id < 2048; ++id) {
    const int64_t key = id == 0 ? 0 : id < 514 ? 1 : 2;
    input.AddRow(Row({Value(key), Value(id), Value(std::string(256, 'x'))}));
  }
  ASSERT_SUCCESS(input.FinishSpill());
  ASSERT_TRUE(input.HasSpill());
  size_t visited = 0;
  ASSERT_SUCCESS(input.ForEachRowBounded(
      input.TotalRows(), [&](const Row&) { return ++visited < 3; }));
  EXPECT_EQ(visited, 3U);

  SelectStatement stmt;
  stmt.SetOrderBy({{ConstantValueExp(Value(1)), true, std::nullopt}});
  stmt.SetOffset(2);
  stmt.SetLimit(3);
  stmt.SetWithTies(true);
  auto result = relational_detail::LimitedRows(stmt, std::move(input));
  ASSERT_TRUE(result.HasValue());
  EXPECT_TRUE(result.Value().HasSpill());
  int64_t expected_id = 2;
  ASSERT_SUCCESS(result.Value().ForEachRow([&](const Row& row) {
    EXPECT_EQ(row[0], Value(1));
    EXPECT_EQ(row[1], Value(expected_id++));
  }));
  EXPECT_EQ(expected_id, 514);

  // The direct LimitedRows call above pre-spills the input Relation, so it
  // never runs the ExternalKeyedSorter that the SQL route constructs with
  // top1_with_ties=true. Cover that path too: a low budget makes the sorter
  // spill while it adds rows, and every peer of the top row must survive.
  RelationalRun(*rs_,
                "CREATE TABLE TieSpill (k INT64, id INT64, payload STRING)");
  {
    auto ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("TieSpill"));
    for (int64_t id = 0; id < 1024; ++id) {
      ASSERT_SUCCESS(table
                         ->Insert(ctx.txn_, Row({Value(1), Value(id),
                                                 Value(std::string(256, 'x'))}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  const auto tied = RelationalRun(
      *rs_, "SELECT k, id FROM TieSpill ORDER BY k LIMIT 1 WITH TIES",
      /*force_relational=*/true);
  ASSERT_EQ(tied.size(), 1024U);
}

TEST_F(RelationalRegressionTest, BoundedTraversalEarlyExitLeavesTailWritable) {
  auto& budget = QueryMemoryBudget::Global();
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions):
                          // scope-owned cleanup.
    size_t limit;
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore{budget.Limit()};
  budget.ResetForTest(static_cast<size_t>(64 * 1024));

  relational_detail::Relation input;
  input.schema = Schema("s", {Column("k", ValueType::kInt64),
                              Column("payload", ValueType::kVarChar)});
  for (int64_t i = 0; i < 64; ++i) {
    input.AddRow(Row({Value(i), Value(std::string(256, 'x'))}));
  }
  // Move the resident rows into a finished spill and leave a fresh open tail.
  input.EnsureSpill();
  ASSERT_TRUE(input.HasSpill());
  ASSERT_SUCCESS(input.GetStatus());
  const size_t before = input.TotalRows();

  // The first spill alone fills max_rows, so the tail is never read. A bounded
  // traversal must not finalize it on the way out.
  size_t visited = 0;
  ASSERT_SUCCESS(input.ForEachRowBounded(3, [&](const Row&) { ++visited; }));
  EXPECT_EQ(visited, 3U);

  // A later append still goes into the open tail instead of latching
  // "Append after FinishWriting".
  input.AddRow(Row({Value(999), Value(std::string(256, 'y'))}));
  EXPECT_SUCCESS(input.GetStatus());
  EXPECT_EQ(input.TotalRows(), before + 1);
  // A correct row count alone cannot tell an appended tail row from a row
  // written into a fresh generation (or from a reordered/overwritten one).
  std::vector<int64_t> keys;
  keys.reserve(before + 1);
  ASSERT_SUCCESS(input.ForEachRow(
      [&](const Row& row) { keys.push_back(row[0].value.int_value); }));
  ASSERT_EQ(keys.size(), before + 1);
  for (size_t i = 0; i < before; ++i) {
    EXPECT_EQ(keys[i], static_cast<int64_t>(i));
  }
  EXPECT_EQ(keys.back(), 999);
}

TEST_F(RelationalRegressionTest, AuditParallelFilterPreservesDeferredRowOrder) {
  auto context = rs_->BeginContext();
  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget budget(&active, 2);
  struct RestoreBudget {
    RestoreBudget() = default;
    RestoreBudget(const RestoreBudget&) = delete;
    RestoreBudget& operator=(const RestoreBudget&) = delete;
    RestoreBudget(RestoreBudget&&) = delete;
    RestoreBudget& operator=(RestoreBudget&&) = delete;
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  for (bool spill_rows : {false, true}) {
    QueryMemoryBudget::Global().ResetForTest(spill_rows ? 65536
                                                        : restore.limit);
    for (bool move_rows : {false, true}) {
      relational_detail::Relation source;
      source.schema = Schema("f", {Column("k", ValueType::kInt64),
                                   Column("d", ValueType::kInt64)});
      for (int64_t i = 0; i < 40000; ++i) {
        source.rows.emplace_back(Row({Value(i), Value(i % 7 == 3 ? 0 : 1)}));
      }
      relational_detail::CompiledScanFilter filter;
      filter.disjunctive_branches.resize(2);
      filter.disjunctive_branches[0].residual.push_back(BinaryExpressionExp(
          ConstantValueExp(Value(1)), BinaryOperation::kDivide,
          ColumnValueExp(ColumnName("d"))));
      // A division error in the first OR branch is masked by this true branch.
      // Workers defer those rows; serial evaluation accepts them in-place.
      relational_detail::Relation result;
      result.schema = source.schema;
      Status error{Status::kSuccess};
      ASSERT_TRUE(relational_detail::TryParallelFilterRows(
          context, source, filter, source.schema, nullptr, {}, nullptr, &result,
          &error, move_rows ? &source : nullptr));
      ASSERT_SUCCESS(error);
      ASSERT_SUCCESS(result.FinishSpill());
      if (spill_rows) {
        EXPECT_TRUE(result.HasSpill());
      }
      size_t count = 0;
      ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
        EXPECT_EQ(row[0].value.int_value, static_cast<int64_t>(count++));
      }));
      EXPECT_EQ(count, 40000U);
    }
  }
}

TEST_F(RelationalRegressionTest, AuditArenaFilterPreservesRowOrder) {
  auto context = rs_->BeginContext();
  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget budget(&active, 2);
  struct RestoreBudget {
    RestoreBudget() = default;
    RestoreBudget(const RestoreBudget&) = delete;
    RestoreBudget& operator=(const RestoreBudget&) = delete;
    RestoreBudget(RestoreBudget&&) = delete;
    RestoreBudget& operator=(RestoreBudget&&) = delete;
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  for (bool spill_rows : {false, true}) {
    QueryMemoryBudget::Global().ResetForTest(spill_rows ? 65536
                                                        : restore.limit);
    relational_detail::BaseRelationArena arena;
    arena.stride = 2;
    arena.slot_types = {ValueType::kInt64, ValueType::kInt64};
    arena.null_bits.resize(2);
    constexpr size_t kRows = 40000;
    std::vector<int64_t> cells(kRows * 2);
    std::vector<uint8_t> nulls(kRows * 2, 0);
    for (size_t i = 0; i < kRows; ++i) {
      cells[i * 2] = static_cast<int64_t>(i);
      cells[(i * 2) + 1] = (i % 7 == 3) ? 0 : 1;
    }
    arena.AppendRun(cells.data(), nulls.data(), kRows);
    const Schema schema(
        "f", {Column("k", ValueType::kInt64), Column("d", ValueType::kInt64)});
    relational_detail::CompiledScanFilter filter;
    filter.disjunctive_branches.resize(2);
    filter.disjunctive_branches[0].residual.push_back(BinaryExpressionExp(
        ConstantValueExp(Value(1)), BinaryOperation::kDivide,
        ColumnValueExp(ColumnName("d"))));
    // Same deferred-row shape as AuditParallelFilterPreservesDeferredRowOrder:
    // `1/d` errors on d == 0 rows, which the empty second OR branch masks;
    // workers defer those rows and the merge must reinsert them in place.
    relational_detail::Relation result;
    result.schema = schema;
    Status error{Status::kSuccess};
    ASSERT_TRUE(relational_detail::TryFilterArenaCache(
        context, arena, schema, &filter, &schema, nullptr, {}, nullptr, &result,
        &error));
    ASSERT_SUCCESS(error);
    ASSERT_SUCCESS(result.FinishSpill());
    if (spill_rows) {
      EXPECT_TRUE(result.HasSpill());
    }
    size_t count = 0;
    ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
      EXPECT_EQ(row[0].value.int_value, static_cast<int64_t>(count++));
    }));
    EXPECT_EQ(count, kRows);
  }
}

TEST_F(RelationalRegressionTest,
       AuditRelationalDumpDoesNotInventAdaptiveSwitch) {
  auto context = rs_->BeginContext();
  auto statement = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{
          NamedExpression("k", ColumnValueExp(ColumnName("a", "k")))},
      std::vector<std::string>{},
      BinaryExpressionExp(ColumnValueExp(ColumnName("a", "k")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("b", "k"))));
  SelectSource left;
  left.table = "AggTable";
  left.alias = "a";
  SelectSource right = left;
  right.alias = "b";
  statement->SetSources({left, right});
  RelationalExecutor executor(context, statement);
  Row row;
  size_t count = 0;
  while (executor.Next(&row, nullptr)) {
    ++count;
  }
  ASSERT_SUCCESS(executor.GetStatus());
  EXPECT_EQ(count, 4000U);
  std::ostringstream dump;
  executor.Dump(dump, 0);
  ASSERT_NE(dump.str().find(", hash_joins=1,"), std::string::npos)
      << dump.str();
  EXPECT_EQ(dump.str().find("AdaptiveJoin"), std::string::npos) << dump.str();
}

TEST_F(RelationalRegressionTest, SingleUseCteResolvesQualifiedGroupingKeys) {
  const std::string sql = R"sql(
    WITH a AS (SELECT x.k AS ak,SUM(x.v) AS av FROM AggTable x GROUP BY k),
         b AS (SELECT y.k AS bk,SUM(y.v) AS bv FROM AggTable y GROUP BY k)
    SELECT ak,av,bv FROM a LEFT JOIN b ON ak=bk WHERE ak=2
  )sql";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(2), Value(20), Value(20)})}));
  const auto profile = ExplainAnalyze(*rs_, sql);
  EXPECT_NE(profile.find("aggregate_input_rows=40,"), std::string::npos)
      << profile;
}

TEST_F(RelationalRegressionTest, CtePushdownPreservesSharedUsesAndBoundaries) {
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    WITH a AS (SELECT k AS ak, SUM(v) AS av FROM AggTable GROUP BY k)
    SELECT x.ak,y.ak FROM a x, a y WHERE x.ak=2 AND y.ak=3
  )sql"),
            std::vector<Row>({Row({Value(2), Value(3)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    WITH a AS (SELECT k AS ak FROM AggTable ORDER BY k LIMIT 1)
    SELECT ak FROM a WHERE ak=2
  )sql"),
            std::vector<Row>());
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    WITH a AS (SELECT k AS ak, SUM(v) AS av FROM AggTable GROUP BY k),
         b AS (SELECT SUM(av) AS total FROM a)
    SELECT ak,total FROM a,b WHERE ak=2
  )sql"),
            std::vector<Row>({Row({Value(2), Value(200)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    WITH a AS (SELECT k AS ak, ROW_NUMBER() OVER (ORDER BY k) AS n FROM SmallKeys)
    SELECT n FROM a WHERE ak=3
  )sql"),
            std::vector<Row>({Row({Value(3)})}));
}

TEST_F(RelationalRegressionTest, UnionJoinBoundsPreserveDuplicatesAndNulls) {
  RelationalRun(*rs_, "CREATE TABLE UnionDim (dk INT64, flag INT64)");
  RelationalRun(*rs_, "INSERT INTO UnionDim VALUES (2,1),(2,1),(NULL,1),(3,0)");
  RelationalRun(*rs_, "ANALYZE UnionDim");
  const std::string prefix = R"sql(
    SELECT SUM(u.v) FROM (
      SELECT k AS key, v FROM AggTable
      UNION ALL SELECT k AS other, 10 AS value FROM KeyTable
    ) u, UnionDim d WHERE u.key=d.dk AND d.flag=
  )sql";
  EXPECT_EQ(RelationalRun(*rs_, prefix + "1"),
            std::vector<Row>({Row({Value(60)})}));
  EXPECT_EQ(RelationalRun(*rs_, prefix + "99"),
            std::vector<Row>({Row({Value()})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT COUNT(*) FROM (
      SELECT k AS key, v FROM AggTable
      UNION ALL SELECT k AS other, 10 AS value FROM KeyTable
    ) u LEFT JOIN UnionDim d ON u.key=d.dk AND d.flag=1 WHERE u.key=9
  )sql"),
            std::vector<Row>({Row({Value(21)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT SUM(u.v) FROM (
      (SELECT k AS key,v FROM AggTable ORDER BY k LIMIT 1)
      UNION ALL SELECT k AS other,10 AS value FROM KeyTable
    ) u, UnionDim d WHERE u.key=d.dk AND d.flag=1
  )sql"),
            std::vector<Row>({Row({Value(20)})}));
  const std::string union_body =
      "(SELECT k AS key,v FROM AggTable UNION ALL "
      "SELECT k AS other,10 AS value FROM KeyTable) named";
  for (const auto& [modifier, expected] :
       std::vector<std::pair<std::string, Value>>{
           {"", Value(60)},
           {" WHERE key>5", Value()},
           {" ORDER BY key LIMIT 1", Value()}}) {
    std::string query = "SELECT SUM(u.v) FROM (SELECT key,v FROM ";
    query += union_body;
    query += modifier;
    query += ") u,UnionDim d WHERE u.key=d.dk AND d.flag=1";
    EXPECT_EQ(RelationalRun(*rs_, query), std::vector<Row>({Row({expected})}));
  }
  EXPECT_EQ(
      RelationalRun(*rs_, "SELECT SUM(u.v) FROM (SELECT DISTINCT key,v FROM " +
                              union_body +
                              ") u,UnionDim d WHERE u.key=d.dk AND d.flag=1"),
      std::vector<Row>({Row({Value(22)})}));
}

TEST_F(RelationalRegressionTest, OrderedJoinDropsOnlyConsumedColumns) {
  using relational_detail::Relation;
  auto context = rs_->BeginContext();
  auto fact = std::make_shared<Relation>();
  fact->schema = Schema(
      "", {Column("id", ValueType::kInt64), Column("a", ValueType::kInt64),
           Column("b", ValueType::kInt64), Column("late", ValueType::kInt64)});
  for (int64_t i = 0; i < 32768; ++i) {
    fact->rows.emplace_back(
        std::vector<Value>{Value(i), Value(i % 128), Value(i % 31), Value(1)});
  }
  auto dim = std::make_shared<Relation>();
  dim->schema = Schema("", {Column("k", ValueType::kInt64)});
  for (int64_t i = 1; i < 128; i += 2) {
    dim->rows.emplace_back(std::vector<Value>{Value(i)});
    dim->rows.emplace_back(std::vector<Value>{Value(i)});
  }
  auto nullable = std::make_shared<Relation>();
  nullable->schema = Schema("", {Column("k", ValueType::kInt64),
                                 Column("payload", ValueType::kVarChar)});
  for (int64_t i = 0; i < 16; ++i) {
    nullable->rows.emplace_back(
        std::vector<Value>{Value(i), Value("value-" + std::to_string(i))});
  }
  const relational_detail::CteMap ctes{
      {"f", fact}, {"d", dim}, {"r", nullable}};
  const auto col = [](const char* owner, const char* name) {
    return ColumnValueExp(ColumnName(owner, name));
  };
  SelectSource f;
  f.table = "f";
  SelectSource d;
  d.table = "d";
  d.join_type = JoinType::kInner;
  d.join_condition = BinaryExpressionExp(
      col("f", "a"), BinaryOperation::kEquals, col("d", "k"));
  SelectSource r;
  r.table = "r";
  r.join_type = JoinType::kLeft;
  r.join_condition = BinaryExpressionExp(
      col("f", "b"), BinaryOperation::kEquals, col("r", "k"));
  for (const bool late_filter : {false, true}) {
    SelectStatement statement({NamedExpression("id", col("f", "id")),
                               NamedExpression("payload", col("r", "payload"))},
                              {}, nullptr);
    statement.SetSources({f, d, r});
    if (late_filter) {
      statement.SetWhereClause(
          BinaryExpressionExp(col("f", "late"), BinaryOperation::kGreaterThan,
                              FunctionCallExp("rand", {})));
    }
    for (const size_t workers : {size_t{1}, size_t{4}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      bool applied = false;
      ASSIGN_OR_ASSERT_FAIL(Relation, result,
                            relational_detail::BuildInput(
                                context, statement, nullptr, ctes, &applied));
      EXPECT_EQ(applied, !late_filter);
      EXPECT_EQ(result.TotalRows(), 32768U);
      EXPECT_EQ(result.schema.ColumnCount(), late_filter ? 5U : 4U);
      const auto id = relational_detail::LocalColumnOffset(
          result.schema, ColumnName("f", "id"));
      const auto value = relational_detail::LocalColumnOffset(
          result.schema, ColumnName("r", "payload"));
      if (!id || !value) {
        FAIL() << "missing live output column";
      }
      std::vector<size_t> seen(32768);
      ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
        const int64_t key = row[*id].value.int_value;
        ASSERT_GE(key, 0);
        ASSERT_LT(key, 32768);
        EXPECT_EQ(key % 2, 1);
        ++seen[static_cast<size_t>(key)];
        EXPECT_EQ(row[*value], key % 31 < 16
                                   ? Value("value-" + std::to_string(key % 31))
                                   : Value());
      }));
      for (size_t i = 0; i < seen.size(); ++i) {
        EXPECT_EQ(seen[i], i % 2 == 1 ? 2U : 0U);
      }
    }
  }
  EXPECT_EQ(fact->schema.ColumnCount(), 4U);
  EXPECT_EQ(fact->rows.front().Count(), 4U);
}

TEST_F(RelationalRegressionTest, SharedCteFiltersBeforeCopyingConsumerColumns) {
  using relational_detail::Relation;
  auto context = rs_->BeginContext();
  auto shared = std::make_shared<Relation>();
  shared->schema = Schema("shared", {Column("k", ValueType::kInt64),
                                     Column("v", ValueType::kVarChar),
                                     Column("unused", ValueType::kVarChar)});
  shared->rows = {Row({Value(1), Value("one"), Value("unused")}),
                  Row({Value(1), Value("one"), Value("unused")}),
                  Row({Value(2), Value(), Value("unused")}),
                  Row({Value(), Value("null"), Value("unused")})};
  const auto original = shared->rows;
  relational_detail::CteMap ctes{{"shared", shared}};
  SelectSource left;
  left.table = "shared";
  left.alias = "a";
  SelectSource right = left;
  right.alias = "b";
  const auto comparison = [](const char* alias, int64_t value) {
    return BinaryExpressionExp(ColumnValueExp(ColumnName(alias, "k")),
                               BinaryOperation::kEquals,
                               ConstantValueExp(Value(value)));
  };
  SelectStatement statement(
      {NamedExpression("v", ColumnValueExp(ColumnName("a", "v")))}, {},
      BinaryExpressionExp(comparison("a", 1), BinaryOperation::kAnd,
                          comparison("b", 2)));
  statement.SetSources({left, right});
  bool applied = false;
  auto result = relational_detail::BuildInput(context, statement, nullptr, ctes,
                                              &applied);
  ASSERT_TRUE(result.HasValue()) << result.GetStatus();
  EXPECT_TRUE(applied);
  EXPECT_EQ(result.Value().TotalRows(), 2U);
  // a keeps k and v; b only needs k. Neither copies the unused payload.
  EXPECT_EQ(result.Value().schema.ColumnCount(), 3U);
  EXPECT_EQ(shared->rows, original);
  EXPECT_EQ(shared->schema.ColumnCount(), 3U);

  statement.SetWhereClause(BinaryExpressionExp(
      comparison("a", 2), BinaryOperation::kAnd, comparison("b", 1)));
  result = relational_detail::BuildInput(context, statement, nullptr, ctes,
                                         &applied);
  ASSERT_TRUE(result.HasValue()) << result.GetStatus();
  EXPECT_EQ(result.Value().TotalRows(), 2U);
  const auto output = relational_detail::LocalColumnOffset(
      result.Value().schema, ColumnName("a", "v"));
  if (!output) {
    FAIL() << "missing projected CTE payload";
  }
  for (const Row& row : result.Value().rows) {
    EXPECT_TRUE(row[*output].IsNull());
  }
  EXPECT_EQ(shared->rows, original);
  for (int64_t k = 100; k < 65636; ++k) {
    shared->rows.emplace_back(
        std::vector<Value>{Value(k), Value("extra"), Value("unused")});
  }
  const auto large_original = shared->rows;
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto parallel = relational_detail::BuildInput(context, statement, nullptr,
                                                  ctes, &applied);
    ASSERT_TRUE(parallel.HasValue()) << parallel.GetStatus();
    EXPECT_EQ(parallel.Value().rows, result.Value().rows);
    EXPECT_EQ(shared->rows, large_original);
  }
  // Force a failed append without consuming the resident input: a directory
  // cannot be opened as a spill file. The sticky error must survive removal
  // of the failed tail, including the parallel direct-rows traversal.
  shared->spill_tail_ = std::make_shared<SpillFile>();
  ASSERT_TRUE(std::filesystem::create_directory(shared->spill_tail_->Path()));
  shared->AddRow(Row({Value(1), Value("failed"), Value("unused")}));
  const Status spill_error = shared->GetStatus();
  ASSERT_NE(spill_error, Status::kSuccess);
  shared->spill_tail_.reset();
  ASSERT_FALSE(shared->HasSpill());
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto failed = relational_detail::BuildInput(context, statement, nullptr,
                                                ctes, &applied);
    EXPECT_FALSE(failed.HasValue());
    EXPECT_EQ(failed.GetStatus(), spill_error);
  }
}

TEST_F(RelationalRegressionTest,
       MaterializedStarKeepsNullsDuplicatesAndResiduals) {
  using relational_detail::Relation;
  auto context = rs_->BeginContext();
  relational_detail::ExecutionRuntime runtime;
  context.set_execution_runtime(&runtime);
  relational_detail::CteMap ctes;
  const auto bind = [&](const std::string& name, std::vector<Column> columns,
                        std::vector<Row> rows) {
    auto relation = std::make_shared<Relation>();
    relation->schema = Schema(name, std::move(columns));
    relation->rows = std::move(rows);
    ctes[name] = std::move(relation);
  };
  bind(
      "f",
      {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64),
       Column("v", ValueType::kInt64)},
      {Row({Value(1), Value(10), Value(5)}),
       Row({Value(1), Value(10), Value(5)}),
       Row({Value(2), Value(20), Value(7)}),
       Row({Value(), Value(10), Value(9)}), Row({Value(1), Value(), Value(11)}),
       Row({Value(3), Value(10), Value(13)}),
       Row({Value(1), Value(30), Value(17)})});
  bind("a",
       {Column("k", ValueType::kInt64), Column("payload", ValueType::kVarChar)},
       {Row({Value(1), Value("one")}), Row({Value(2), Value()}),
        Row({Value(), Value("null")})});
  bind("b",
       {Column("k", ValueType::kInt64), Column("payload", ValueType::kVarChar)},
       {Row({Value(10), Value("ten")}), Row({Value(20), Value("twenty")}),
        Row({Value(), Value("null")})});
  const auto column = [](const char* table, const char* name) {
    return ColumnValueExp(ColumnName(table, name));
  };
  const auto edge_a = BinaryExpressionExp(
      column("f", "a"), BinaryOperation::kEquals, column("a", "k"));
  const auto edge_b = BinaryExpressionExp(
      column("f", "b"), BinaryOperation::kEquals, column("b", "k"));
  SelectStatement statement(
      {NamedExpression("v", column("f", "v")),
       NamedExpression("pa", column("a", "payload")),
       NamedExpression("pb", column("b", "payload"))},
      {}, BinaryExpressionExp(edge_a, BinaryOperation::kAnd, edge_b));
  std::vector<SelectSource> sources;
  sources.reserve(3);
  for (const char* name : {"f", "a", "b"}) {
    SelectSource source;
    source.table = name;
    sources.push_back(std::move(source));
  }
  statement.SetSources(std::move(sources));
  const auto run = [&](size_t count, size_t copies = 1) {
    std::vector<Row> serial;
    for (size_t workers : {size_t{1}, size_t{2}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      bool applied = false;
      auto result = relational_detail::BuildInput(context, statement, nullptr,
                                                  ctes, &applied);
      ASSERT_TRUE(result.HasValue());
      EXPECT_TRUE(applied);
      EXPECT_EQ(result.Value().TotalRows(), count);
      if (copies > 1) {
        EXPECT_EQ(result.Value().schema.ColumnCount(), 3U);
      }
      size_t fives = 0;
      size_t sevens = 0;
      for (const Row& row : result.Value().rows) {
        const auto v = relational_detail::LocalColumnOffset(
            result.Value().schema, ColumnName("f", "v"));
        ASSERT_TRUE(v.has_value());
        if (row[*v] == Value(5)) {
          ++fives;
        } else {
          EXPECT_EQ(row[*v], Value(7));
          ++sevens;
        }
      }
      EXPECT_EQ(sevens, copies);
      EXPECT_EQ(fives, count - copies);
      if (workers == 1) {
        serial = result.Value().rows;
      } else {
        EXPECT_EQ(result.Value().rows, serial);
      }
    }
  };
  run(3);
  const auto original = ctes["f"]->rows;
  ctes["f"]->rows.reserve(original.size() * 8192);
  for (size_t repeat = 1; repeat < 8192; ++repeat) {
    ctes["f"]->rows.insert(ctes["f"]->rows.end(), original.begin(),
                           original.end());
  }
  run(size_t{3} * 8192, 8192);
  ctes["f"]->rows = original;

  // A duplicated build key must retain both matches via the general join.
  ctes["a"]->rows.push_back(Row({Value(1), Value("another")}));
  run(5);
  ctes["a"]->rows.pop_back();
  // A non-key cross-source predicate must still be evaluated.
  statement.SetWhereClause(BinaryExpressionExp(
      statement.WhereClause(), BinaryOperation::kAnd,
      BinaryExpressionExp(
          column("f", "v"), BinaryOperation::kGreaterThan,
          BinaryExpressionExp(column("a", "k"), BinaryOperation::kAdd,
                              ConstantValueExp(Value(4))))));
  run(1);
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
}

TEST_F(RelationalRegressionTest, RollupUnionNormalizesAllComputedNumericTerms) {
  RelationalRun(*rs_, "CREATE TABLE RollupNumeric (a INT64,b INT64,v DOUBLE)");
  RelationalRun(
      *rs_, "INSERT INTO RollupNumeric VALUES (1,1,NULL),(2,1,1.5),(2,2,2.5)");
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT a,b,SUM(COALESCE(v,0)) AS n FROM RollupNumeric
    GROUP BY ROLLUP(a,b) ORDER BY a NULLS LAST,b NULLS LAST
  )sql"),
            std::vector<Row>({Row({Value(1), Value(1), Value(0.0)}),
                              Row({Value(1), Value(), Value(0.0)}),
                              Row({Value(2), Value(1), Value(1.5)}),
                              Row({Value(2), Value(2), Value(2.5)}),
                              Row({Value(2), Value(), Value(4.0)}),
                              Row({Value(), Value(), Value(4.0)})}));
}

TEST_F(RelationalRegressionTest, SharedGroupedCteKeepsEveryConsumerDomain) {
  const std::string sql = R"sql(
    WITH c AS (
      SELECT k AS key, SUM(v) AS n FROM AggTable GROUP BY k
      UNION ALL SELECT k AS other, SUM(v) AS n FROM AggTable GROUP BY k
    )
    SELECT SUM(a.n+b.n) FROM c a, c b WHERE a.key=1 AND b.key=1+1
  )sql";
  EXPECT_EQ(RelationalRun(*rs_, sql), std::vector<Row>({Row({Value(160)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    WITH c AS (SELECT k AS key, SUM(v) AS n FROM AggTable GROUP BY k)
    SELECT SUM(a.n+b.n) FROM c a, c b WHERE a.key=1
  )sql"),
            std::vector<Row>({Row({Value(400)})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    WITH c AS (SELECT k AS key, SUM(v) AS n FROM AggTable GROUP BY k LIMIT 2)
    SELECT COUNT(*) FROM c a, c b WHERE a.key=9 AND b.key=8
  )sql"),
            std::vector<Row>({Row({Value(0)})}));
}

TEST_F(RelationalRegressionTest, SharedGroupedJoinDomainsKeepCompleteGroups) {
  RelationalRun(*rs_,
                "CREATE TABLE DomainDay (day INT64, week INT64, year INT64)");
  RelationalRun(*rs_, "CREATE TABLE DomainFact (day INT64, v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO DomainDay VALUES (1,10,2000),(2,10,2001),"
                "(3,20,2002),(4,30,2003),(5,NULL,2001)");
  RelationalRun(*rs_,
                "INSERT INTO DomainFact VALUES (1,10),(2,20),(3,40),(4,80)");
  RelationalRun(*rs_, "ANALYZE DomainDay");
  const std::string prefix = R"sql(
    WITH c AS (
      SELECT d.week, SUM(f.v) AS total FROM DomainFact f, DomainDay d
      WHERE f.day=d.day GROUP BY d.week
    )
  )sql";
  // Week 10 spans two years. A consumer's year is not a legal filter on
  // the aggregate input: all of the requested week's rows must survive.
  EXPECT_EQ(RelationalRun(*rs_, prefix + R"sql(
    SELECT SUM(a.total+b.total)
    FROM (SELECT c.total FROM c, DomainDay d
          WHERE c.week=d.week AND d.year=2001) a,
         (SELECT c.total FROM c, DomainDay d
          WHERE c.week=d.week AND d.year=2002) b
  )sql"),
            std::vector<Row>({Row({Value(70)})}));
  // An unrestricted site needs every group, including week 30.
  EXPECT_EQ(RelationalRun(*rs_, prefix + R"sql(
    SELECT SUM(a.total+b.total)
    FROM (SELECT c.total FROM c, DomainDay d
          WHERE c.week=d.week AND d.year=2001) a, c b
  )sql"),
            std::vector<Row>({Row({Value(240)})}));
  EXPECT_EQ(RelationalRun(*rs_, prefix + R"sql(
    SELECT COUNT(*)
    FROM (SELECT c.total FROM c, DomainDay d
          WHERE c.week=d.week AND d.year=1990) a,
         (SELECT c.total FROM c, DomainDay d
          WHERE c.week=d.week AND d.year=1991) b
  )sql"),
            std::vector<Row>({Row({Value(0)})}));
  EXPECT_EQ(RelationalRun(*rs_, prefix + R"sql(
    SELECT COUNT(*) FROM c a LEFT JOIN c b ON a.week=b.week
    LEFT JOIN DomainDay d ON a.week=d.week AND d.year=2001
  )sql"),
            std::vector<Row>({Row({Value(3)})}));
}

TEST_F(RelationalRegressionTest, FusedDoubleArithmeticKeepsNullsAndErrors) {
  RelationalRun(*rs_,
                "CREATE TABLE DoubleFact "
                "(k INT64, a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE)");
  RelationalRun(*rs_,
                "INSERT INTO DoubleFact VALUES "
                "(1, 10.0, 2.0, 3.0, 4.0), "
                "(1, NULL, 2.0, 3.0, 4.0), (2, 20.0, 4.0, 5.0, 6.0)");
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT t.k, SUM((f.a - f.b - f.c + f.d) / 2) "
                    "FROM KeyTable t LEFT JOIN DoubleFact f ON t.k = f.k "
                    "GROUP BY t.k ORDER BY t.k");
  ASSERT_EQ(rows.size(), 10U);
  for (size_t i = 0; i < rows.size(); ++i) {
    if (i == 1 || i == 2) {
      EXPECT_EQ(rows[i][1], Value(i == 1 ? 4.5 : 8.5));
    } else {
      EXPECT_TRUE(rows[i][1].IsNull());
    }
  }
  // A NULL on the left cannot suppress an error in a separately evaluated
  // right subtree. The grouped kernel must retain the AST's evaluation order.
  RelationalRun(*rs_, "INSERT INTO DoubleFact VALUES (3, NULL, 1.0, 0.0, 1.0)");
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto prepared = engine.Prepare(
      context,
      "SELECT t.k, SUM(f.a + (f.b / f.c)) "
      "FROM KeyTable t JOIN DoubleFact f ON t.k = f.k GROUP BY t.k");
  ASSERT_TRUE(prepared.HasValue());
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
  }
  EXPECT_EQ(prepared.Value()->GetStatus(), Status::kIsInfinity);
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest,
       FusedSingletonCrossJoinKeepsAggregateSemantics) {
  const std::string sql = R"sql(
    SELECT a.k,SUM(a.v),MAX(c.m) FROM AggTable a,
        (SELECT MAX(k) AS m FROM SmallKeys) c
    GROUP BY a.k HAVING SUM(a.v)>MAX(c.m) ORDER BY a.k
  )sql";
  const auto rows = RelationalRun(*rs_, sql, /*force_relational=*/true);
  ASSERT_EQ(rows.size(), 10U);
  for (size_t i = 0; i < rows.size(); ++i) {
    EXPECT_EQ(rows[i], Row({Value(i), Value(20), Value(4)}));
  }
  EXPECT_NE(ExplainAnalyze(*rs_, sql, /*force_relational=*/true)
                .find("grouped_join_fusions=1"),
            std::string::npos);
  EXPECT_EQ(
      RelationalRun(*rs_, R"sql(
    SELECT c.m,COUNT(*),SUM(a.v),MAX(c.m) FROM AggTable a,
        (SELECT MAX(k) AS m FROM SmallKeys WHERE k=99) c GROUP BY c.m
  )sql"),
      std::vector<Row>({Row({Value(), Value(200), Value(200), Value()})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT COUNT(*),SUM(a.v),MAX(c.m) FROM AggTable a,
        (SELECT MAX(k) AS m FROM SmallKeys) c WHERE a.k=99
  )sql"),
            std::vector<Row>({Row({Value(0), Value(), Value()})}));
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT COUNT(*),SUM(a.v) FROM AggTable a,
        (SELECT k AS m FROM SmallKeys WHERE k=99) c
  )sql"),
            std::vector<Row>({Row({Value(0), Value()})}));
  // Two build rows multiply every input row and must not enter the singleton
  // path, even if their values happen to be equal.
  EXPECT_EQ(RelationalRun(*rs_, R"sql(
    SELECT a.k,COUNT(*),SUM(a.v) FROM AggTable a,
        (SELECT 1 AS m UNION ALL SELECT 1) c GROUP BY a.k ORDER BY a.k LIMIT 1
  )sql"),
            std::vector<Row>({Row({Value(0), Value(40), Value(40)})}));
}

TEST_F(RelationalRegressionTest, InnerOnEdgesReorderBeforePreservedOuterJoins) {
  RelationalRun(*rs_, "CREATE TABLE ReorderFact (k INT64, day INT64)");
  RelationalRun(*rs_,
                "CREATE TABLE ReorderStock (k INT64, day INT64, v INT64)");
  RelationalRun(*rs_, "CREATE TABLE ReorderDay (day INT64, week INT64)");
  RelationalRun(*rs_, "CREATE TABLE ReorderExtra (k INT64)");
  RelationalRun(*rs_,
                "INSERT INTO ReorderFact VALUES (1, 10), (2, 10), (3, 20)");
  RelationalRun(*rs_,
                "INSERT INTO ReorderStock VALUES "
                "(1, 11, 5), (1, 11, 6), (1, 20, 99), (2, 11, 7), (3, 20, 8)");
  RelationalRun(*rs_,
                "INSERT INTO ReorderDay VALUES (10, 1), (11, 1), (20, 2)");
  RelationalRun(*rs_, "INSERT INTO ReorderExtra VALUES (1), (1), (9)");
  const std::string prefix =
      "FROM ReorderFact f JOIN ReorderStock s ON f.k = s.k "
      "JOIN ReorderDay a ON f.day = a.day "
      "JOIN ReorderDay b ON s.day = b.day ";
  const auto rows = RelationalRun(
      *rs_, "SELECT f.k, SUM(s.v), COUNT(e.k) " + prefix +
                "LEFT JOIN ReorderExtra e ON f.k = e.k "
                "WHERE a.week = b.week GROUP BY f.k ORDER BY f.k");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(22), Value(4)}));
  EXPECT_EQ(rows[1], Row({Value(2), Value(7), Value(0)}));
  EXPECT_EQ(rows[2], Row({Value(3), Value(8), Value(0)}));
  const auto right = RelationalRun(
      *rs_,
      "SELECT e.k, COUNT(f.k) " + prefix +
          "RIGHT JOIN ReorderExtra e ON f.k = e.k "
          "WHERE a.week = b.week OR f.k IS NULL GROUP BY e.k ORDER BY e.k");
  ASSERT_EQ(right.size(), 2U);
  EXPECT_EQ(right[0], Row({Value(1), Value(4)}));
  EXPECT_EQ(right[1], Row({Value(9), Value(0)}));
}

TEST_F(RelationalRegressionTest, GroupedWindowFusesJoinBeforeWindowEvaluation) {
  RelationalRun(*rs_, "CREATE TABLE WindowKeys (k INT64)");
  RelationalRun(*rs_, "INSERT INTO WindowKeys VALUES (1), (2), (40)");
  const std::string sql =
      "SELECT a.k, SUM(SUM(b.v)) OVER (ORDER BY a.k "
      "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running, "
      "MAX(COUNT(b.v)) OVER () AS largest "
      "FROM WindowKeys a LEFT JOIN AggTable b ON a.k = b.k "
      "GROUP BY a.k QUALIFY running >= 40 ORDER BY a.k";
  const auto rows = RelationalRun(*rs_, sql);
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(2), Value(40), Value(20)}));
  EXPECT_EQ(rows[1], Row({Value(40), Value(40), Value(20)}));
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("grouped_join_fusions=1"),
            std::string::npos);
  const auto reused = RelationalRun(
      *rs_,
      "SELECT a.k, SUM(b.v) AS subtotal, SUM(SUM(b.v)) OVER "
      "(ORDER BY a.k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
      "AS running FROM WindowKeys a LEFT JOIN AggTable b ON a.k = b.k "
      "GROUP BY a.k ORDER BY a.k");
  ASSERT_EQ(reused.size(), 3U);
  EXPECT_EQ(reused[0], Row({Value(1), Value(20), Value(20)}));
  EXPECT_EQ(reused[1], Row({Value(2), Value(20), Value(40)}));
  EXPECT_EQ(reused[2], Row({Value(40), Value(), Value(40)}));
}

TEST_F(RelationalRegressionTest, GroupedWindowsReuseTemporaryInputColumns) {
  RelationalRun(*rs_, "CREATE TABLE WindowReuse (k INT64, p STRING, v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO WindowReuse VALUES "
                "(1,'a',3),(1,'a',4),(2,'a',NULL),(3,'b',5)");
  const std::string grouped =
      "SELECT SUM(SUM(v)) OVER (PARTITION BY p ORDER BY k) AS running, "
      "k, MAX(COUNT(v)) OVER (PARTITION BY p) AS largest "
      "FROM WindowReuse GROUP BY k,p";
  const std::vector<Row> expected = {Row({Value(7), Value(1), Value(2)}),
                                     Row({Value(7), Value(2), Value(2)}),
                                     Row({Value(5), Value(3), Value(1)})};
  EXPECT_EQ(RelationalRun(*rs_, "SELECT * FROM (" + grouped + ") q ORDER BY k"),
            expected);
  EXPECT_EQ(RelationalRun(*rs_, grouped + " ORDER BY k"), expected);
  // Every projected item can be a window, and multiple windows can consume
  // the same hidden aggregate before its cell is reused.
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT * FROM (SELECT SUM(SUM(v)) OVER () AS s, "
                          "MAX(SUM(v)) OVER () AS m FROM WindowReuse "
                          "GROUP BY k,p) q"),
            std::vector<Row>(3, Row({Value(12), Value(7)})));
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT * FROM (SELECT k, SUM(v) AS subtotal, "
                          "SUM(SUM(v)) OVER () AS total FROM WindowReuse "
                          "GROUP BY k) q ORDER BY k"),
            std::vector<Row>({Row({Value(1), Value(7), Value(12)}),
                              Row({Value(2), Value(), Value(12)}),
                              Row({Value(3), Value(5), Value(12)})}));
}

TEST_F(RelationalRegressionTest, FusedConstantOutputsKeepNullsAndTopN) {
  RelationalRun(*rs_, "CREATE TABLE ConstantKeys (k INT64)");
  RelationalRun(*rs_, "INSERT INTO ConstantKeys VALUES (1),(40)");
  const std::string from =
      " FROM ConstantKeys k LEFT JOIN AggTable a ON k.k=a.k ";
  const std::string sql =
      "SELECT k.k,'channel' AS tag,NULL AS empty,42 AS fixed,"
      "SUM(a.v),COUNT(a.v)" +
      from + "GROUP BY k.k ORDER BY tag,k.k LIMIT 2";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(1), Value("channel"), Value(),
                                   Value(42), Value(20), Value(20)}),
                              Row({Value(40), Value("channel"), Value(),
                                   Value(42), Value(), Value(0)})}));
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("grouped_join_fusions=1"),
            std::string::npos);
  EXPECT_EQ(RelationalRun(*rs_, "SELECT 'count' AS tag,k.k,COUNT(a.v)" + from +
                                    "GROUP BY k.k ORDER BY tag,k.k LIMIT 1"),
            std::vector<Row>({Row({Value("count"), Value(1), Value(20)})}));
  EXPECT_EQ(
      RelationalRun(
          *rs_,
          "SELECT 'empty',NULL,SUM(a.v),COUNT(*) "
          "FROM ConstantKeys k JOIN AggTable a ON k.k=a.k WHERE k.k=40"),
      std::vector<Row>({Row({Value("empty"), Value(), Value(), Value(0)})}));
}

TEST_F(RelationalRegressionTest, FullJoinNullAggregatesKeepMultiplicity) {
  RelationalRun(*rs_, "CREATE TABLE NullLeft (k INT64,v INT64)");
  RelationalRun(*rs_, "CREATE TABLE NullRight (k INT64,v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO NullLeft VALUES (1,10),(1,NULL),(2,20),(NULL,30)");
  RelationalRun(
      *rs_, "INSERT INTO NullRight VALUES (1,100),(1,NULL),(3,200),(NULL,300)");
  const std::string select =
      "SELECT SUM(CASE WHEN l.k IS NOT NULL AND r.k IS NULL THEN 1 ELSE 0 END),"
      "SUM(CASE WHEN l.k IS NULL AND r.k IS NOT NULL THEN 1 ELSE 0 END),"
      "SUM(CASE WHEN l.k IS NOT NULL AND r.k IS NOT NULL THEN 1 ELSE 0 END),"
      "COUNT(*),SUM(CASE WHEN l.v IS NULL THEN 1 ELSE 0 END),"
      "SUM(CASE WHEN l.k IS NULL THEN 10 END),"
      "COUNT(CASE WHEN l.k IS NOT NULL THEN NULL ELSE 1 END) ";
  const std::string from = "FROM NullLeft l FULL JOIN NullRight r ON l.k=r.k";
  EXPECT_EQ(RelationalRun(*rs_, select + from),
            std::vector<Row>({Row({Value(1), Value(1), Value(4), Value(8),
                                   Value(4), Value(30), Value(3)})}));
  EXPECT_NE(ExplainAnalyze(*rs_, select + from).find("grouped_join_fusions=1"),
            std::string::npos);
  // A rejected ON pair leaves BOTH input rows eligible for null extension.
  EXPECT_EQ(RelationalRun(*rs_, select + from + " AND l.v>r.v"),
            std::vector<Row>({Row({Value(3), Value(3), Value(0), Value(8),
                                   Value(5), Value(50), Value(5)})}));
  EXPECT_EQ(
      RelationalRun(
          *rs_, select + "FROM (SELECT * FROM NullLeft WHERE k<0) l FULL JOIN "
                         "(SELECT * FROM NullRight WHERE k<0) r ON l.k=r.k"),
      std::vector<Row>({Row(
          {Value(), Value(), Value(), Value(0), Value(), Value(), Value(0)})}));
  EXPECT_EQ(
      RelationalRun(*rs_, select + "FROM (SELECT * FROM NullLeft WHERE k<0) l "
                                   "FULL JOIN NullRight r ON l.k=r.k"),
      std::vector<Row>({Row({Value(0), Value(3), Value(0), Value(4), Value(4),
                             Value(40), Value(4)})}));
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto overflow = engine.Execute(
      context,
      "SELECT SUM(CASE WHEN l.k IS NULL THEN 9223372036854775807 ELSE 0 END) " +
          from);
  if (overflow.HasValue()) {
    overflow.Value().Drain();
    EXPECT_EQ(overflow.Value().GetStatus(), Status::kIsInfinity);
  } else {
    EXPECT_EQ(overflow.GetStatus(), Status::kIsInfinity);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, DirectWindowOrderingKeepsPeersAndNulls) {
  RelationalRun(*rs_, "CREATE TABLE WindowOrderKeys (p INT64,k INT64,v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO WindowOrderKeys VALUES "
                "(1,2,20),(1,NULL,30),(1,1,10),(1,2,21),"
                "(2,2,40),(2,1,50)");
  const auto rows = RelationalRun(*rs_, R"sql(
    SELECT p,k,v,
      SUM(v) OVER (PARTITION BY p ORDER BY k NULLS LAST) AS total,
      ROW_NUMBER() OVER (PARTITION BY p ORDER BY k DESC NULLS FIRST,v) AS rn
    FROM WindowOrderKeys ORDER BY p,v
  )sql");
  ASSERT_EQ(rows.size(), 6U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(1), Value(10), Value(10), Value(4)}));
  EXPECT_EQ(rows[1], Row({Value(1), Value(2), Value(20), Value(51), Value(2)}));
  EXPECT_EQ(rows[2], Row({Value(1), Value(2), Value(21), Value(51), Value(3)}));
  EXPECT_EQ(rows[3], Row({Value(1), Value(), Value(30), Value(81), Value(1)}));
  EXPECT_EQ(rows[4], Row({Value(2), Value(2), Value(40), Value(90), Value(1)}));
  EXPECT_EQ(rows[5], Row({Value(2), Value(1), Value(50), Value(50), Value(2)}));
}

TEST_F(RelationalRegressionTest, MonotonicDictionarySumKeepsInfinity) {
  RelationalRun(*rs_, "CREATE TABLE MonoInf(k INT64, v FLOAT64)");
  const double infinity = std::numeric_limits<double>::infinity();
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("MonoInf"));
    for (int64_t i = 0; i < 65536; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(context.txn_,
                       Row({Value(i / 32768), Value(i == 0 ? infinity : 1.0)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  {
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("MonoInf"));
    const auto store = table->RebuildLSTableStore(context.txn_);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->total_rows, 65536U);
    ASSERT_NE(store->column_is_monotonic[0], 0U);
    ASSERT_TRUE(store->dictionaries[1].active);
    ASSERT_SUCCESS(context.PreCommit());
  }
  const auto result =
      RelationalRun(*rs_, "SELECT k, SUM(v) FROM MonoInf GROUP BY k", true);
  ASSERT_EQ(result.size(), 2U);
  for (const auto& row : result) {
    EXPECT_EQ(row[1], Value(row[0] == Value(0) ? infinity : 32768.0));
  }
  // The monotonic path reads dictionary codes per row. Check its row counter
  // so the same result from the generic fallback does not pass this check.
  const std::string sql = "SELECT k, SUM(v) FROM MonoInf GROUP BY k";
  const std::string profile =
      ExplainAnalyze(*rs_, sql, /*force_relational=*/true);
  constexpr std::string_view kCounter = "dictionary_aggregate_rows=";
  const auto pos = profile.find(kCounter);
  ASSERT_NE(pos, std::string::npos) << profile;
  EXPECT_GT(std::stoul(profile.substr(pos + kCounter.size())), 0U) << profile;
}

TEST_F(RelationalRegressionTest, DirectDoubleWindowOrderKeepsExceptionalPeers) {
  RelationalRun(*rs_, "CREATE TABLE DoubleWindowKeys(k FLOAT64)");
  RelationalRun(*rs_,
                "INSERT INTO DoubleWindowKeys VALUES (2.5),(NULL),(-0.0),(0.0),"
                "(2.5),(CAST('NaN' AS FLOAT64)),(CAST('NaN' AS FLOAT64)),"
                "(CAST('inf' AS FLOAT64)),(CAST('-inf' AS FLOAT64)),(-4.0)");
  for (const std::string_view order : {"ASC NULLS FIRST", "ASC NULLS LAST",
                                       "DESC NULLS FIRST", "DESC NULLS LAST"}) {
    const auto query = [&](std::string_view key) {
      const std::string spec = std::string(key) + " " + std::string(order);
      std::string sql = "SELECT RANK() OVER (ORDER BY ";
      sql += spec;
      sql += "), DENSE_RANK() OVER (ORDER BY ";
      sql += spec;
      sql += "), ROW_NUMBER() OVER (ORDER BY ";
      sql += spec;
      sql += ") FROM DoubleWindowKeys";
      return sql;
    };
    SCOPED_TRACE(order);
    EXPECT_EQ(RelationalRun(*rs_, query("k")),
              RelationalRun(*rs_, query("k+0.0")));
  }
}

TEST_F(RelationalRegressionTest, BorrowedWindowKeysKeepFramesAndSharedLayouts) {
  RelationalRun(*rs_,
                "CREATE TABLE WindowBorrow (id INT64,p INT64,k INT64,"
                "v FLOAT64,d DATE)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("WindowBorrow"));
    for (int i = 0; i < 1600; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({Value(i), Value(i / 8),
                            i % 5 == 0 ? Value() : Value(i % 4),
                            i % 3 == 0 ? Value() : Value(i * 0.5),
                            i % 7 == 0 ? Value() : Value::DateFromDays(i % 4)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto query = [](bool computed) {
    const std::string key = computed ? "(k+0)" : "k";
    const std::string date = computed ? "(d+0)" : "d";
    const std::string ascending =
        "PARTITION BY p ORDER BY " + key + " ASC NULLS FIRST";
    const std::string descending =
        "PARTITION BY p ORDER BY " + key + " DESC NULLS LAST";
    return "SELECT id,k,v,d,RANK() OVER (" + ascending + "),SUM(v) OVER (" +
           ascending +
           " ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),COUNT(v) OVER (" +
           ascending +
           " RANGE BETWEEN 1 PRECEDING AND CURRENT ROW),"
           "LAG(v) OVER (" +
           ascending + "),LAST_VALUE(v) OVER (" + descending +
           " ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING),DENSE_RANK() OVER ("
           "PARTITION BY p ORDER BY " +
           date + "),SUM(v) OVER (PARTITION BY p ORDER BY " + date +
           " RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) "
           "FROM WindowBorrow ORDER BY id";
  };
  const auto direct = RelationalRun(*rs_, query(false));
  const auto computed = RelationalRun(*rs_, query(true));
  ASSERT_EQ(direct.size(), 1600U);
  EXPECT_EQ(direct, computed);
  for (size_t i = 0; i < direct.size(); ++i) {
    EXPECT_EQ(direct[i][0], Value(i));
    EXPECT_EQ(direct[i][1], i % 5 == 0 ? Value() : Value(i % 4));
    EXPECT_EQ(direct[i][2],
              i % 3 == 0 ? Value() : Value(static_cast<double>(i) * 0.5));
    EXPECT_EQ(direct[i][3], i % 7 == 0 ? Value() : Value::DateFromDays(i % 4));
  }
}

TEST_F(RelationalRegressionTest,
       NumericWindowKeysKeepExceptionalPeersAndOrder) {
  auto context = rs_->BeginContext();
  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget budget(&active, 2);
  const std::vector<std::vector<Value>> domains{
      {Value(), Value(std::numeric_limits<int64_t>::min()), Value(-1), Value(0),
       Value(std::numeric_limits<int64_t>::max()), Value(0)},
      {Value(), Value(-std::numeric_limits<double>::infinity()), Value(-1.0),
       Value(-0.0), Value(0.0), Value(1.0),
       Value(std::numeric_limits<double>::infinity()),
       Value(std::numeric_limits<double>::quiet_NaN()),
       Value(-std::numeric_limits<double>::quiet_NaN())}};
  for (const auto& domain : domains) {
    Schema schema(
        "", {Column("id", ValueType::kInt64), Column("p", ValueType::kInt64),
             Column("k", domain[1].type)});
    std::vector<Row> rows;
    rows.reserve(32768);
    for (size_t i = 0; i < 32768; ++i) {
      rows.emplace_back(std::vector<Value>{Value(i), Value(i % 3),
                                           domain[i % domain.size()]});
    }
    for (const bool ascending : {false, true}) {
      for (const bool nulls_first : {false, true}) {
        for (const std::string function :
             {"ROW_NUMBER", "RANK", "DENSE_RANK", "LAG"}) {
          WindowFunctionCallExpression window;
          window.function = function;
          if (function == "LAG") {
            window.args = {ColumnValueExp("id")};
          }
          window.partition_by = {ColumnValueExp("p")};
          window.order_by = {{ColumnValueExp("k"), ascending, nulls_first}};
          std::vector<Value> encoded, reference;
          ASSERT_SUCCESS(relational_detail::ComputeOneWindow(
              context, window, rows, schema, nullptr, {}, &encoded));
          // The contextual expression route retains the general comparator.
          window.order_by[0].expression = FunctionCallExp(
              "coalesce", {ColumnValueExp("k"), ColumnValueExp("k")});
          ASSERT_SUCCESS(relational_detail::ComputeOneWindow(
              context, window, rows, schema, nullptr, {}, &reference));
          EXPECT_EQ(encoded, reference)
              << function << ' ' << ascending << ' ' << nulls_first;
        }
      }
    }
  }
}

TEST_F(RelationalRegressionTest, RankBoundKeepsWholeBoundaryPeers) {
  auto context = rs_->BeginContext();
  const Schema schema(
      "", {Column("id", ValueType::kInt64), Column("p", ValueType::kInt64),
           Column("k", ValueType::kDouble)});
  std::vector<Row> source;
  source.reserve(32768);
  for (size_t i = 0; i < 32768; ++i) {
    Value value(static_cast<double>(i % 1024));
    if (i % 257 == 0) {
      value = Value();
    } else if (i % 509 == 0) {
      value = Value(std::numeric_limits<double>::quiet_NaN());
    } else if (i % 251 == 0) {
      value = Value(std::numeric_limits<double>::infinity());
    } else if (i % 127 == 0) {
      value = Value(-0.0);
    }
    source.emplace_back(std::vector<Value>{Value(i), Value(i % 3), value});
  }
  for (const bool ascending : {false, true}) {
    for (const bool nulls_first : {false, true}) {
      auto window = std::make_shared<WindowFunctionCallExpression>();
      window->function = "RANK";
      window->partition_by = {ColumnValueExp("p")};
      window->order_by = {{ColumnValueExp("k"), ascending, nulls_first}};
      WindowFunctionCallExpression generic;
      generic.function = "RANK";
      generic.partition_by = window->partition_by;
      generic.order_by = window->order_by;
      generic.order_by[0].expression = FunctionCallExp(
          "coalesce", {ColumnValueExp("k"), ColumnValueExp("k")});
      std::vector<Value> reference;
      ASSERT_SUCCESS(relational_detail::ComputeOneWindow(
          context, generic, source, schema, nullptr, {}, &reference));
      for (const int64_t limit : {int64_t{1}, int64_t{100}, int64_t{32768}}) {
        for (const bool alias : {false, true}) {
          SelectStatement statement(
              {NamedExpression("id", ColumnValueExp("id")),
               NamedExpression("r", window)},
              {}, nullptr);
          statement.SetQualify(BinaryExpressionExp(
              alias ? ColumnValueExp("r") : window,
              alias ? BinaryOperation::kLessThan
                    : BinaryOperation::kLessThanEquals,
              ConstantValueExp(Value(limit + (alias ? 1 : 0)))));
          relational_detail::Relation input;
          input.schema = schema;
          input.rows = source;
          ASSIGN_OR_ASSERT_FAIL(
              relational_detail::WindowedInput, actual,
              relational_detail::ApplyWindows(context, statement,
                                              std::move(input), nullptr, {}));
          size_t kept = 0;
          for (size_t r = 0; r < source.size(); ++r) {
            if (reference[r].value.int_value > limit) {
              continue;
            }
            ASSERT_LT(kept, actual.input.rows.size());
            EXPECT_EQ(actual.input.rows[kept][0], Value(r));
            EXPECT_EQ(actual.input.rows[kept][3], reference[r]);
            ++kept;
          }
          EXPECT_EQ(actual.input.rows.size(), kept);
        }
      }
    }
  }
}

TEST_F(RelationalRegressionTest, DerivedWindowFiltersEnterQualifyAfterRanking) {
  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = "ROW_NUMBER";
  window->partition_by = {ColumnValueExp("k")};
  window->order_by = {{ColumnValueExp("v"), true, false}};
  auto derived = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression("k", ColumnValueExp("k")),
                                   NamedExpression("r", window)},
      std::vector<std::string>{"AggTable"}, nullptr);
  SelectStatement outer(
      {NamedExpression("k", ColumnValueExp("k")),
       NamedExpression("r", ColumnValueExp("r"))},
      {},
      BinaryExpressionExp(ColumnValueExp("r"), BinaryOperation::kLessThanEquals,
                          ConstantValueExp(Value(2))));
  SelectSource source;
  source.query = derived;
  source.alias = "w";
  outer.SetSources({source});
  const auto rewritten =
      relational_detail::OptimizeDerivedBoundaries(outer, {});
  ASSERT_NE(rewritten, nullptr);
  EXPECT_EQ(rewritten->WhereClause(), nullptr);
  EXPECT_NE(rewritten->Sources()[0].query->Qualify(), nullptr);
  derived->SetOrderBy({{ColumnValueExp("k"), true, false}});
  EXPECT_EQ(relational_detail::OptimizeDerivedBoundaries(outer, {}), nullptr);
  derived->SetOrderBy({});
  derived->SetLimit(1);
  EXPECT_EQ(relational_detail::OptimizeDerivedBoundaries(outer, {}), nullptr);

  const std::string inner =
      "SELECT k,v,ROW_NUMBER() OVER(PARTITION BY k ORDER BY v) r FROM AggTable";
  const auto nested = RelationalRun(
      *rs_, "SELECT * FROM (" + inner + ") w WHERE r<=2 ORDER BY k,v,r");
  const auto qualified =
      RelationalRun(*rs_, inner + " QUALIFY r<=2 ORDER BY k,v,r");
  EXPECT_EQ(nested, qualified);
  EXPECT_EQ(nested.size(), 20U);
  EXPECT_TRUE(RelationalRun(*rs_, "SELECT * FROM (" + inner +
                                      " ORDER BY r LIMIT 1) w WHERE r>1")
                  .empty());
  const std::string ties =
      "SELECT k,RANK() OVER(ORDER BY k) r FROM "
      "(SELECT 1 k UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t";
  EXPECT_EQ(RelationalRun(*rs_, "SELECT * FROM (" + ties + ") w WHERE r<=2"),
            RelationalRun(*rs_, ties + " QUALIFY r<=2"));
}

TEST_F(RelationalRegressionTest,
       ColumnByteLengthAggregatesKeepNullsAndSnapshot) {
  const Schema schema("input", {Column("s", ValueType::kVarChar)});
  const auto average = std::make_shared<AggregateExpression>(
      AggregationType::kAvg,
      CastExpressionExp(FunctionCallExp("byte_length", {ColumnValueExp("s")}),
                        "FLOAT64"));
  const SelectStatement statement({NamedExpression("n", average)}, {}, nullptr);
  const auto plan = relational_detail::PlanTypedAggregation(
      statement, nullptr, schema, {average.get()}, {});
  ASSERT_TRUE(plan.eligible);
  ASSERT_TRUE(plan.slim);
  const auto& program = plan.aggregates[0].child;
  ASSERT_TRUE(program);
  if (!program) {
    return;
  }
  ASSERT_EQ(program->Instructions().back().opcode, BytecodeOp::kByteLength);
  for (const bool nullable : {false, true}) {
    const std::string name =
        nullable ? "ByteLengthNullable" : "ByteLengthPlain";
    RelationalRun(*rs_, "CREATE TABLE " + name + "(k INT64,s STRING)");
    {
      auto setup = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                            setup.GetTable(name));
      for (int64_t i = 0; i < 40000; ++i) {
        Value text =
            nullable && i % 13 == 0
                ? Value()
                : Value(i % 7 == 0 ? std::string()
                                   : (i % 3 == 0 ? std::string("é東京")
                                                 : std::string("payload-") +
                                                       std::to_string(i)));
        ASSERT_SUCCESS(
            table->Insert(setup.txn_, Row({Value(i % 8), std::move(text)}))
                .GetStatus());
      }
      ASSERT_SUCCESS(setup.PreCommit());
    }
    RelationalRun(*rs_, "ANALYZE " + name);
    const auto sql = [&](bool reference, bool scalar,
                         const std::string& predicate) {
      const std::string input = reference ? "s || ''" : "s";
      std::string result = scalar ? "SELECT " : "SELECT k,";
      result += "SUM(BYTE_LENGTH(" + input;
      result += ")),AVG(CAST(BYTE_LENGTH(" + input;
      result += ") AS FLOAT64)),COUNT(BYTE_LENGTH(" + input;
      result += ")),COUNT(*) FROM " + name;
      result += " WHERE " + predicate;
      result += scalar ? "" : " GROUP BY k ORDER BY k";
      return result;
    };
    for (const size_t workers : {size_t{1}, size_t{2}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      for (const bool scalar : {false, true}) {
        for (const std::string predicate : {"TRUE", "s<>''", "s=''", "k<0"}) {
          EXPECT_EQ(RelationalRun(*rs_, sql(false, scalar, predicate)),
                    RelationalRun(*rs_, sql(true, scalar, predicate)));
        }
      }
    }
    auto old = rs_->BeginReadOnlyContext();
    SqlEngine engine(*rs_);
    const auto read_old = [&] {
      std::vector<Row> rows;
      auto result = engine.Prepare(old, sql(false, false, "TRUE"));
      EXPECT_TRUE(result.HasValue()) << engine.LastError();
      if (result.HasValue()) {
        Row row;
        while (result.Value()->Next(&row, nullptr)) {
          rows.push_back(row);
        }
        EXPECT_TRUE(result.Value()->GetStatus().ok());
      }
      return rows;
    };
    const auto expected = read_old();
    RelationalRun(*rs_, "UPDATE " + name + " SET s='changed' WHERE k=0");
    EXPECT_EQ(read_old(), expected);
    EXPECT_EQ(RelationalRun(*rs_, sql(false, false, "TRUE")),
              RelationalRun(*rs_, sql(true, false, "TRUE")));
    ASSERT_SUCCESS(old.PreCommit());
  }
}

TEST_F(RelationalRegressionTest, WideStringCodesKeepGroupingAndFilters) {
  RelationalRun(*rs_,
                "CREATE TABLE WideCodeInput(k INT64,s STRING,v INT64,d DATE)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("WideCodeInput"));
    for (int64_t i = 0; i < 40000; ++i) {
      std::string text(80, i % 2 == 0 ? 'a' : 'b');
      text += i % 2 == 0 ? "é_" : "_";
      text += std::to_string(i % 303);
      Value value = i % 17 == 0 ? Value() : Value(i % 19 == 0 ? "" : text);
      ASSERT_SUCCESS(
          table
              ->Insert(
                  setup.txn_,
                  Row({Value(i), std::move(value), Value(i % 7),
                       i % 13 == 0 ? Value() : Value::DateFromDays(i % 31)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE WideCodeInput");
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const std::string predicate :
         {" = ''", " <> ''", " LIKE '%é%'", " NOT LIKE '%é%'",
          " LIKE 'a%3' AND v=2", " LIKE '%é%' OR v=2"}) {
      const std::string actual =
          "SELECT k FROM WideCodeInput WHERE s" + predicate;
      const std::string reference =
          "SELECT k FROM WideCodeInput WHERE (s||'')" + predicate;
      EXPECT_EQ(RelationalRun(*rs_, actual + " ORDER BY k"),
                RelationalRun(*rs_, reference + " ORDER BY k"));
    }
    const std::string base =
        "SELECT s,COUNT(*),SUM(v),AVG(CAST(v AS FLOAT64)) "
        "FROM WideCodeInput GROUP BY s";
    EXPECT_EQ(RelationalRun(*rs_, base + " ORDER BY s"),
              RelationalRun(*rs_, base + ",BYTE_LENGTH(s) ORDER BY s"));
    for (const std::string predicate :
         {"", " WHERE s<>''", " WHERE v<>3", " WHERE v>=0", " WHERE v<0",
          " WHERE d>='1970-1-16'", " WHERE v=2 AND s<>''"}) {
      for (const std::string order :
           {" ORDER BY c DESC,s NULLS FIRST LIMIT 17 OFFSET 3",
            " ORDER BY c,s DESC NULLS LAST LIMIT 700",
            " ORDER BY s DESC NULLS FIRST LIMIT 11 OFFSET 600",
            " ORDER BY s NULLS LAST LIMIT 0"}) {
        std::string actual =
            "SELECT s,COUNT(*) c,COUNT(*) again,7 tag "
            "FROM WideCodeInput";
        actual += predicate;
        actual += " GROUP BY s";
        actual += order;
        std::string reference =
            "SELECT s||'' AS s,COUNT(*) c,COUNT(*) again,7 tag "
            "FROM WideCodeInput";
        reference += predicate;
        reference += " GROUP BY s||''";
        reference += order;
        EXPECT_EQ(RelationalRun(*rs_, actual), RelationalRun(*rs_, reference))
            << actual;
      }
    }
    const std::string filtered =
        "SELECT s,COUNT(*) c FROM WideCodeInput WHERE s<>'' GROUP BY s";
    EXPECT_EQ(
        RelationalRun(*rs_, filtered + " ORDER BY c DESC,s LIMIT 17"),
        RelationalRun(*rs_,
                      filtered + ",BYTE_LENGTH(s) ORDER BY c DESC,s LIMIT 17"));
    for (const std::string op : {"=", "<>", "<", "<=", ">", ">="}) {
      for (const bool reversed : {false, true}) {
        const auto query = [&](bool reference) {
          std::string literal = reference ? "DATE '1970-01-16'" : "'1970-1-16'";
          std::string condition = reversed ? literal : "d";
          condition += op;
          condition += reversed ? "d" : literal;
          std::string result = "SELECT s,COUNT(*) c FROM WideCodeInput WHERE ";
          result += condition;
          result += " GROUP BY s ORDER BY c DESC,s LIMIT 17";
          return result;
        };
        EXPECT_EQ(RelationalRun(*rs_, query(false)),
                  RelationalRun(*rs_, query(true)));
      }
    }
  }
  auto context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context.GetTable("WideCodeInput"));
  const auto store = table->GetLSTableStore();
  ASSERT_NE(store, nullptr);
  EXPECT_EQ(store->dictionaries[1].wide_code_bytes, 2);
  EXPECT_TRUE(store->dictionaries[1].bytewise_equality);
  ASSERT_SUCCESS(context.PreCommit());
  EXPECT_EQ(
      RelationalRun(*rs_, "SELECT COUNT(DISTINCT s) FROM WideCodeInput"),
      RelationalRun(*rs_, "SELECT COUNT(DISTINCT (s||'')) FROM WideCodeInput"));
  // Distinct byte strings can be one SQL group. Rebuilding the dictionary
  // must revoke the proof and keep canonical grouping for these peers.
  RelationalRun(*rs_,
                "INSERT INTO WideCodeInput VALUES "
                "(40000,'0-12 0 0:0:0',1,DATE '1970-01-01'),"
                "(40001,'1-0 0 0:0:0',1,DATE '1970-01-01')");
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT s,COUNT(*) c FROM WideCodeInput "
                          "GROUP BY s ORDER BY s LIMIT 700"),
            RelationalRun(*rs_,
                          "SELECT s||'' AS s,COUNT(*) c FROM WideCodeInput "
                          "GROUP BY s||'' ORDER BY s LIMIT 700"));
}

// Derived aggregate producers have no LIMIT: every group must survive into
// the next stage even when the outer query has a small LIMIT or HAVING.
TEST_F(RelationalRegressionTest,
       UnboundedDictionaryAggregatesFeedNestedStages) {
  RelationalRun(*rs_, "CREATE TABLE InnerCodes(k INT64,s STRING,v INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("InnerCodes"));
    for (int64_t i = 0; i < 24000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({i % 17 == 0 ? Value() : Value((i % 307) - 153),
                            i % 19 == 0 ? Value()
                                        : Value(std::string(80, 'x') +
                                                std::to_string(i % 311)),
                            Value((i % 13) - 6)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE InnerCodes");
  const std::string inner =
      "SELECT k,COUNT(*) c,COUNT(k) n,SUM(k) s,AVG(k) a,MIN(k) lo,MAX(k) hi "
      "FROM InnerCodes GROUP BY k";
  const std::string reference = inner + ",CAST(k AS STRING)";
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    for (const std::string suffix :
         {"", " ORDER BY s NULLS LAST",
          " ORDER BY a DESC NULLS FIRST LIMIT 7 OFFSET 3"}) {
      EXPECT_EQ(RelationalRun(*rs_, inner + suffix),
                RelationalRun(*rs_, reference + suffix));
    }
    const std::string nested =
        "SELECT c,COUNT(*) group_count,SUM(n) nonnull,SUM(s) "
        "total,MIN(lo),MAX(hi) "
        "FROM (" +
        inner + ") GROUP BY c HAVING SUM(n)>0 ORDER BY c LIMIT 5";
    const std::string nested_reference =
        "SELECT c,COUNT(*) group_count,SUM(n) nonnull,SUM(s) "
        "total,MIN(lo),MAX(hi) "
        "FROM (" +
        reference + ") GROUP BY c HAVING SUM(n)>0 ORDER BY c LIMIT 5";
    EXPECT_EQ(RelationalRun(*rs_, nested),
              RelationalRun(*rs_, nested_reference));
    const auto explain = ExplainAnalyze(*rs_, nested);
    EXPECT_NE(explain.find("dictionary_aggregations=1"), std::string::npos)
        << explain;
    EXPECT_NE(explain.find("dictionary_summary_aggregations=1"),
              std::string::npos)
        << explain;
    EXPECT_NE(explain.find("dictionary_aggregate_rows=0"), std::string::npos)
        << explain;
    // Independent dictionaries must be packed jointly; multiplying their
    // marginal frequencies would invent combinations not present in the data.
    for (const std::string where : {"", " WHERE v<0", " WHERE v>100"}) {
      const std::string actual =
          "SELECT c,COUNT(*) FROM (SELECT k,s,COUNT(*) c FROM InnerCodes" +
          where + " GROUP BY k,s) GROUP BY c ORDER BY c";
      const std::string expected =
          "SELECT c,COUNT(*) FROM (SELECT MIN(k),s||'',COUNT(*) c FROM "
          "InnerCodes" +
          where + " GROUP BY CAST(k AS STRING),s||'') GROUP BY c ORDER BY c";
      EXPECT_EQ(RelationalRun(*rs_, actual), RelationalRun(*rs_, expected));
      EXPECT_NE(ExplainAnalyze(*rs_, actual).find("dictionary_aggregations=1"),
                std::string::npos);
    }
    const std::string scalar =
        "SELECT n,COUNT(*) FROM (SELECT LENGTH(s) len,COUNT(*) n FROM "
        "InnerCodes "
        "GROUP BY LENGTH(s)) GROUP BY n ORDER BY n";
    EXPECT_EQ(
        RelationalRun(*rs_, scalar),
        RelationalRun(
            *rs_,
            "SELECT n,COUNT(*) FROM (SELECT LENGTH(s||'') len,COUNT(*) n "
            "FROM InnerCodes GROUP BY LENGTH(s||'')) GROUP BY n ORDER BY n"));
    EXPECT_NE(
        ExplainAnalyze(*rs_, scalar).find("dictionary_summary_aggregations=1"),
        std::string::npos);
  }
  // A new snapshot may no longer have a safe SUM bound. It must not reuse
  // stale summaries, or let the outer LIMIT hide an overflowing inner group.
  RelationalRun(*rs_,
                "INSERT INTO InnerCodes VALUES "
                "(9223372036854775807,'overflow',0),"
                "(9223372036854775807,'overflow',0)");
  auto context = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  auto result =
      engine.Execute(context, "SELECT SUM(s) FROM (" + inner + ") LIMIT 1");
  if (result.HasValue()) {
    result.Value().Collect();
    EXPECT_NE(result.Value().GetStatus(), Status::kSuccess);
  } else {
    EXPECT_NE(result.GetStatus(), Status::kSuccess);
  }
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(RelationalRegressionTest,
       ReusableDictionaryStagesKeepIndependentFilters) {
  RelationalRun(*rs_, "CREATE TABLE SharedCodes(k INT64,v INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("SharedCodes"));
    for (int64_t i = 0; i < 24000; ++i) {
      ASSERT_SUCCESS(table
                         ->Insert(setup.txn_,
                                  Row({Value((i % 401) * 10000), Value(i % 7)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE SharedCodes");
  const std::string actual =
      "SELECT SUM(c) FROM (SELECT k,COUNT(*) c FROM SharedCodes WHERE "
      "k<1000000 "
      "GROUP BY k) UNION ALL SELECT SUM(c) FROM (SELECT k,COUNT(*) c FROM "
      "SharedCodes WHERE k>=1000000 GROUP BY k)";
  const std::string expected =
      "SELECT COUNT(*) FROM SharedCodes WHERE k<1000000 UNION ALL "
      "SELECT COUNT(*) FROM SharedCodes WHERE k>=1000000";
  EXPECT_EQ(RelationalRun(*rs_, actual), RelationalRun(*rs_, expected));
  const auto explain = ExplainAnalyze(*rs_, actual);
  EXPECT_NE(explain.find("dictionary_summary_aggregations=2"),
            std::string::npos)
      << explain;
  EXPECT_NE(explain.find("dictionary_aggregate_rows=0"), std::string::npos)
      << explain;
  auto old = rs_->BeginReadOnlyContext();
  const auto before = RelationalRun(*rs_, actual);
  RelationalRun(*rs_, "INSERT INTO SharedCodes VALUES (0,0),(4000000,0)");
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(QueryResult, old_result, engine.Execute(old, actual));
  EXPECT_EQ(old_result.Collect(), before);
  ASSERT_SUCCESS(old_result.GetStatus());
  EXPECT_EQ(RelationalRun(*rs_, actual), RelationalRun(*rs_, expected));
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest, DictionaryCountTopNWith32BitCodes) {
  RelationalRun(*rs_, "CREATE TABLE Code32Input(k INT64,s STRING,g INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("Code32Input"));
    for (int64_t i = 0; i < int64_t{65536} * 3; ++i) {
      std::string text(80, 'x');
      text += std::to_string(i % 65536);
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({Value(i), Value(std::move(text)), Value(i % 101)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(
        table->Insert(setup.txn_, Row({Value(-1), Value(), Value(0)}))
            .GetStatus());
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE Code32Input");
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{8}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const std::string predicate :
         {"", " WHERE k>=65536", " WHERE g<3", " WHERE g<0",
          " WHERE g IN (0,1,NULL)"}) {
      std::string actual = "SELECT s,COUNT(*) c FROM Code32Input";
      actual += predicate;
      actual += " GROUP BY s ORDER BY s NULLS FIRST LIMIT 17";
      std::string reference = "SELECT s||'' AS s,COUNT(*) c FROM Code32Input";
      reference += predicate;
      reference += " GROUP BY s||'' ORDER BY s NULLS FIRST LIMIT 17";
      EXPECT_EQ(RelationalRun(*rs_, actual), RelationalRun(*rs_, reference));
    }
  }
  const std::string selective =
      "SELECT s,COUNT(*) c FROM Code32Input WHERE g=0 GROUP BY s "
      "ORDER BY c DESC LIMIT 13 OFFSET 7";
  const auto expected = RelationalRun(
      *rs_,
      "SELECT s||'' s,COUNT(*) c FROM Code32Input WHERE g=0 GROUP BY s||'' "
      "ORDER BY c DESC LIMIT 13 OFFSET 7");
  EXPECT_EQ(RelationalRun(*rs_, selective), expected);
  EXPECT_NE(ExplainAnalyze(*rs_, selective)
                .find("partitioned_dictionary_aggregations=1"),
            std::string::npos);
  {
    struct RestoreBudget {
      size_t limit = QueryMemoryBudget::Global().Limit();
      RestoreBudget() = default;
      RestoreBudget(const RestoreBudget&) = delete;
      RestoreBudget& operator=(const RestoreBudget&) = delete;
      RestoreBudget(RestoreBudget&&) = delete;
      RestoreBudget& operator=(RestoreBudget&&) = delete;
      ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
    } restore;
    QueryMemoryBudget::Global().ResetForTest(size_t{8} << 20);
    EXPECT_EQ(RelationalRun(*rs_, selective), expected);
  }
  auto context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context.GetTable("Code32Input"));
  const auto store = table->GetLSTableStore();
  ASSERT_NE(store, nullptr);
  EXPECT_EQ(store->dictionaries[1].wide_code_bytes, 4);
  EXPECT_TRUE(store->dictionaries[1].bytewise_equality);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(RelationalRegressionTest,
       ProvenSingletonGroupsPushLimitWithoutHidingErrors) {
  RelationalRun(
      *rs_,
      "CREATE TABLE UniqueGroups(k INT64, tag STRING, v INT64, d DOUBLE)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("UniqueGroups"));
    for (int64_t i = 0; i < 8192; ++i) {
      const int64_t k = (i * 4093) % 8192;
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_, Row({Value(k), Value(i % 2 ? "odd" : "even"),
                                        i == 0 ? Value() : Value(i),
                                        Value(static_cast<double>(i) / 2)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const std::string sql =
      "SELECT k,tag,COUNT(*) c,SUM(v),AVG(CAST(v AS DOUBLE)),"
      "MIN(d),MAX(d),COUNT(DISTINCT v) FROM UniqueGroups "
      "GROUP BY k,tag ORDER BY c DESC LIMIT 6 OFFSET 3";
  const auto rows = RelationalRun(*rs_, sql);
  ASSERT_EQ(rows.size(), 6U);
  for (int64_t i = 3; i < 9; ++i) {
    EXPECT_EQ(rows[static_cast<size_t>(i - 3)],
              Row({Value((i * 4093) % 8192), Value(i % 2 ? "odd" : "even"),
                   Value(1), Value(i), Value(static_cast<double>(i)),
                   Value(static_cast<double>(i) / 2),
                   Value(static_cast<double>(i) / 2), Value(1)}));
  }
  EXPECT_NE(ExplainAnalyze(*rs_, sql).find("scan_rows=9,"), std::string::npos);
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(*) c FROM UniqueGroups GROUP BY k "
                          "ORDER BY c DESC LIMIT 2 WITH TIES")
                .size(),
            8192U);
  const auto null_first =
      RelationalRun(*rs_,
                    "SELECT k,COUNT(*) c,SUM(v),AVG(v),COUNT(v) "
                    "FROM UniqueGroups GROUP BY k ORDER BY c DESC LIMIT 2");
  ASSERT_EQ(null_first.size(), 2U);
  EXPECT_EQ(null_first[0],
            Row({Value(0), Value(1), Value(), Value(), Value(0)}));
  EXPECT_EQ(null_first[1],
            Row({Value(4093), Value(1), Value(1), Value(1.0), Value(1)}));
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT k,COUNT(*) c FROM UniqueGroups WHERE tag='odd' "
                    "GROUP BY k ORDER BY c DESC LIMIT 2"),
      (std::vector<Row>{Row({Value(4093), Value(1)}),
                        Row({Value(4087), Value(1)})}));
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(*) c FROM UniqueGroups GROUP BY k "
                          "ORDER BY c DESC,k DESC LIMIT 1"),
            (std::vector<Row>{Row({Value(8191), Value(1)})}));
  EXPECT_TRUE(RelationalRun(*rs_,
                            "SELECT k,COUNT(*) c FROM UniqueGroups GROUP BY k "
                            "HAVING c>1 ORDER BY c DESC LIMIT 1")
                  .empty());
  for (const std::string bad :
       {"SELECT k,k+9223372036854775807,COUNT(*) c FROM UniqueGroups "
        "GROUP BY k,k+9223372036854775807 ORDER BY c DESC LIMIT 1",
        "SELECT k,COUNT(*) c,SUM(v+9223372036854775807) FROM UniqueGroups "
        "GROUP BY k ORDER BY c DESC LIMIT 1",
        "SELECT k,COUNT(*) c FROM UniqueGroups WHERE 1/(8191-k)>-1 "
        "GROUP BY k ORDER BY c DESC LIMIT 1"}) {
    auto context = rs_->BeginReadOnlyContext();
    SqlEngine engine(*rs_);
    auto result = engine.Execute(context, bad);
    if (result.HasValue()) {
      const auto discarded = result.Value().Collect();
      EXPECT_NE(result.Value().GetStatus(), Status::kSuccess) << bad;
    }
  }
  auto old = rs_->BeginReadOnlyContext();
  RelationalRun(*rs_, "INSERT INTO UniqueGroups VALUES (0,'even',NULL,0.0)");
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(QueryResult, old_result,
                        engine.Execute(old,
                                       "SELECT k,COUNT(*) c FROM UniqueGroups "
                                       "GROUP BY k ORDER BY c DESC LIMIT 1"));
  const auto old_rows = old_result.Collect();
  ASSERT_SUCCESS(old_result.GetStatus());
  ASSERT_EQ(old_rows.size(), 1U);
  EXPECT_EQ(old_rows[0][1], Value(1));
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(*) c FROM UniqueGroups GROUP BY k "
                          "ORDER BY c DESC LIMIT 1"),
            (std::vector<Row>{Row({Value(0), Value(2)})}));
  ASSERT_SUCCESS(old.PreCommit());
  auto before_update = rs_->BeginReadOnlyContext();
  RelationalRun(*rs_, "UPDATE UniqueGroups SET k=0 WHERE k=8191");
  ASSIGN_OR_ASSERT_FAIL(
      QueryResult, before_update_result,
      engine.Execute(before_update,
                     "SELECT k,COUNT(*) c FROM UniqueGroups GROUP BY k "
                     "ORDER BY c DESC LIMIT 1"));
  EXPECT_EQ(before_update_result.Collect(),
            (std::vector<Row>{Row({Value(0), Value(2)})}));
  ASSERT_SUCCESS(before_update_result.GetStatus());
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(*) c FROM UniqueGroups GROUP BY k "
                          "ORDER BY c DESC LIMIT 1"),
            (std::vector<Row>{Row({Value(0), Value(3)})}));
  ASSERT_SUCCESS(before_update.PreCommit());
}

TEST_F(RelationalRegressionTest,
       LiteralLikeFiltersKeepDictionaryAndRowAnswers) {
  RelationalRun(
      *rs_,
      "CREATE TABLE LikeInput(k INT64,s STRING,t STRING,label STRING,n INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("LikeInput"));
    for (int64_t i = 0; i < 24000; ++i) {
      std::string s = i % 3 == 0 ? "alpha" : "beta";
      s += std::to_string(i % 1024);
      s += i % 5 == 0 ? "尾" : "end";
      std::string t = std::to_string(i);
      t += i % 7 == 0 ? "needle" : "other";
      t.append(80, 'x');
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({Value(i % 113),
                            i % 17 == 0 ? Value() : Value(std::move(s)),
                            i % 19 == 0 ? Value() : Value(std::move(t)),
                            i % 23 == 0 ? Value()
                                        : Value(i % 2 == 0 ? "alpha" : "beta"),
                            Value(i % 307)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE LikeInput");
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const std::string column : {"s", "t", "label"}) {
      for (const std::string pattern :
           {"'%alpha%'", "'alpha%'", "'%尾'", "'%needle%'", "''", "'%'",
            "'a_ph%'", "'%a%l%'"}) {
        for (const std::string op : {" LIKE ", " NOT LIKE "}) {
          const auto query = [&](bool reference) {
            std::string sql =
                "SELECT k,MIN(s),MIN(t),COUNT(*) c,COUNT(DISTINCT n) FROM "
                "LikeInput WHERE ";
            sql += reference ? "(" + column + "||'')" : column;
            sql += op + pattern;
            sql += " GROUP BY k ORDER BY c DESC,k LIMIT 13 OFFSET 2";
            return sql;
          };
          EXPECT_EQ(RelationalRun(*rs_, query(false)),
                    RelationalRun(*rs_, query(true)))
              << column << op << pattern;
        }
      }
    }
    for (const std::string column : {"s", "label"}) {
      const auto query = [&](bool reference) {
        std::string sql =
            "SELECT " + column + ",COUNT(*) c FROM LikeInput WHERE ";
        sql += reference ? "(" + column + "||'')" : column;
        sql += " NOT LIKE '%alpha%' AND t LIKE '%needle%' GROUP BY " + column;
        sql += " ORDER BY c DESC," + column + " LIMIT 19";
        return sql;
      };
      EXPECT_EQ(RelationalRun(*rs_, query(false)),
                RelationalRun(*rs_, query(true)));
    }
    EXPECT_EQ(
        RelationalRun(*rs_,
                      "SELECT k,SUM(CASE WHEN s LIKE '%alpha%' THEN n ELSE 0 "
                      "END) FROM LikeInput GROUP BY k ORDER BY k"),
        RelationalRun(*rs_,
                      "SELECT k,SUM(CASE WHEN s||'' LIKE '%alpha%' THEN n ELSE "
                      "0 END) FROM LikeInput GROUP BY k ORDER BY k"));
    EXPECT_EQ(
        RelationalRun(*rs_,
                      "SELECT k,COUNT(*) FROM LikeInput WHERE s LIKE 'alpha%' "
                      "OR label NOT LIKE '%beta%' GROUP BY k ORDER BY k"),
        RelationalRun(
            *rs_,
            "SELECT k,COUNT(*) FROM LikeInput WHERE s||'' LIKE 'alpha%' OR "
            "label||'' NOT LIKE '%beta%' GROUP BY k ORDER BY k"));
  }
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT k,MIN(s),COUNT(*) FROM LikeInput WHERE s LIKE "
                    "'%123尾' AND s NOT LIKE '%alpha%' GROUP BY k ORDER BY k"),
      RelationalRun(
          *rs_,
          "SELECT k,MIN(s),COUNT(*) FROM LikeInput WHERE s||'' LIKE '%123尾' "
          "AND s||'' NOT LIKE '%alpha%' GROUP BY k ORDER BY k"));
  auto context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context.GetTable("LikeInput"));
  const auto store = table->GetLSTableStore();
  ASSERT_NE(store, nullptr);
  EXPECT_EQ(store->dictionaries[1].wide_code_bytes, 2);
  EXPECT_FALSE(store->dictionaries[2].active);
  EXPECT_EQ(store->dictionaries[2].wide_code_bytes, 0);
  EXPECT_TRUE(store->dictionaries[3].active);
  SqlEngine engine(*rs_);
  const std::string sql =
      "SELECT k,COUNT(*) FROM LikeInput WHERE label LIKE 'alpha%' GROUP BY k "
      "ORDER BY k";
  const auto before = RelationalRun(*rs_, sql);
  RelationalRun(*rs_, "UPDATE LikeInput SET label='beta' WHERE k=0");
  ASSIGN_OR_ASSERT_FAIL(QueryResult, old_result, engine.Execute(context, sql));
  EXPECT_EQ(old_result.Collect(), before);
  ASSERT_SUCCESS(old_result.GetStatus());
  EXPECT_EQ(RelationalRun(*rs_, sql),
            RelationalRun(*rs_,
                          "SELECT k,COUNT(*) FROM LikeInput WHERE label||'' "
                          "LIKE 'alpha%' GROUP BY k ORDER BY k"));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(RelationalRegressionTest, ExtractDayGroupsTimestampTextByCalendarDay) {
  RelationalRun(*rs_, "CREATE TABLE ExtractTimes(s TIMESTAMP)");
  RelationalRun(
      *rs_,
      "INSERT INTO ExtractTimes VALUES ('2024-02-29 12:34:56+00'),('2024-03-01 "
      "00:12:34+00'),('2024-02-29T12:34:56Z'),(NULL)");
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT EXTRACT(DAY FROM s) d,COUNT(*) FROM ExtractTimes "
                    "GROUP BY d ORDER BY d NULLS FIRST"),
      (std::vector<Row>{Row({Value(), Value(1)}), Row({Value(1), Value(1)}),
                        Row({Value(29), Value(2)})}));
}

TEST_F(RelationalRegressionTest, DictionaryScalarKeysCountAfterFiltering) {
  RelationalRun(*rs_, "CREATE TABLE TimeCodeInput(n INT64,s TIMESTAMP)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("TimeCodeInput"));
    for (int64_t i = 0; i < 24000; ++i) {
      std::string text = "2020-01-01 00:";
      const int64_t minute = (i % 600) / 60;
      text.push_back(static_cast<char>('0' + (minute / 10)));
      text.push_back(static_cast<char>('0' + (minute % 10)));
      text.push_back(':');
      text.push_back(static_cast<char>('0' + ((i % 60) / 10)));
      text.push_back(static_cast<char>('0' + (i % 10)));
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({Value(i),
                            i % 17 == 0 ? Value() : Value(std::move(text))}))
              .GetStatus());
    }
    // This value must not be evaluated when the row predicate rejects it.
    ASSERT_SUCCESS(table
                       ->Insert(setup.txn_,
                                Row({Value(24000), Value("invalid-timestamp")}))
                       .GetStatus());
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE TimeCodeInput");
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const auto& [key, reference] :
         std::vector<std::pair<std::string, std::string>>{
             {"TIMESTAMP_TRUNC(s,MINUTE)", "TIMESTAMP_TRUNC(s||'',MINUTE)"},
             {"EXTRACT(MINUTE FROM s)", "EXTRACT(MINUTE FROM s||'')"},
             {"BYTE_LENGTH(s)", "BYTE_LENGTH(s||'')"}}) {
      for (const std::string predicate :
           {"n<24000", "n>=20000 AND n<24000", "n<0"}) {
        const auto query = [&](const std::string& expression) {
          std::string sql = "SELECT ";
          sql += expression;
          sql += " AS m,COUNT(*) c FROM TimeCodeInput WHERE ";
          sql += predicate;
          sql += " GROUP BY m ORDER BY m NULLS LAST LIMIT 20";
          return sql;
        };
        EXPECT_EQ(RelationalRun(*rs_, query(key)),
                  RelationalRun(*rs_, query(reference)))
            << key;
      }
    }
  }
  // Exercise the encoded path directly, including regrouping many physical
  // timestamp values into one minute and retaining the NULL group.
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  {
    auto reader = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          reader.GetTable("TimeCodeInput"));
    const auto key = FunctionCallExp(
        "timestamp_trunc", {ColumnValueExp("s"), ColumnValueExp("MINUTE")});
    const auto count = std::make_shared<AggregateExpression>(
        AggregationType::kCount, ColumnValueExp("*"));
    SelectStatement statement(
        {NamedExpression("m", key), NamedExpression("c", count)}, {}, nullptr);
    statement.SetGroupBy({key});
    statement.SetOrderBy({{key, true, false}});
    statement.SetLimit(20);
    relational_detail::ExecutionRuntime runtime;
    reader.set_execution_runtime(&runtime);
    ASSIGN_OR_ASSERT_FAIL(
        std::optional<relational_detail::Relation>, grouped,
        relational_detail::TryDictionaryExpressionAggregate(
            reader, statement, *table, {}, table->GetSchema(), {count.get()},
            {BinaryExpressionExp(ColumnValueExp("n"),
                                 BinaryOperation::kLessThan,
                                 ConstantValueExp(Value(24000)))},
            {}));
    if (!grouped) {
      FAIL() << "dictionary scalar grouping declined";
    }
    EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
    EXPECT_EQ(grouped->rows,
              RelationalRun(*rs_,
                            "SELECT TIMESTAMP_TRUNC(s||'',MINUTE) m,COUNT(*) "
                            "FROM TimeCodeInput WHERE n<24000 GROUP BY m "
                            "ORDER BY m NULLS LAST LIMIT 20"));
    ASSERT_SUCCESS(reader.PreCommit());
  }
  // LIMIT must not hide the invalid key, even if it would not be retained.
  auto context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  auto result = engine.Execute(
      context,
      "SELECT TIMESTAMP_TRUNC(s,MINUTE) m,COUNT(*) FROM TimeCodeInput "
      "GROUP BY m ORDER BY m LIMIT 1");
  if (result.HasValue()) {
    result.Value().Drain();
    EXPECT_NE(result.Value().GetStatus(), Status::kSuccess);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, PackedTimestampGroupsKeepFallbacksAndOrder) {
  RelationalRun(*rs_, "CREATE TABLE PackedTimeCodes(n INT64,s STRING)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("PackedTimeCodes"));
    const std::array<std::string_view, 4> dates{"1969-12-31", "2000-02-29",
                                                "2024-03-31", "9999-12-31"};
    for (int64_t i = 0; i < 12000; ++i) {
      std::string text(dates[static_cast<size_t>((i / 600) % 4)]);
      text += i % 3 == 0 ? "T12:" : " 12:";
      const int64_t minute = (i % 600) / 60;
      text.push_back('0');
      text.push_back(static_cast<char>('0' + minute));
      text.push_back(':');
      text.push_back(static_cast<char>('0' + ((i % 60) / 10)));
      text.push_back(static_cast<char>('0' + (i % 10)));
      if (i % 3 != 1) {
        text += "+00";
      }
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({Value(i % 7),
                            i % 31 == 0 ? Value() : Value(std::move(text))}))
              .GetStatus());
    }
    for (const char* text :
         {"2024-03-31 12:00:01.1+00", "2024-03-31 13:00:01+01",
          "1969-12-31 23:59:60+00"}) {
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({Value(-2), Value(std::string(text))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(
        table->Insert(setup.txn_, Row({Value(-3), Value("bad-time")}))
            .GetStatus());
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
                          context.GetTable("PackedTimeCodes"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
    ASSERT_SUCCESS(context.PreCommit());
  }
  const auto query = [](std::string_view input, std::string_view unit,
                        std::string_view filter, std::string_view tail,
                        std::string_view zone = "UTC") {
    std::string sql = "SELECT TIMESTAMP_TRUNC(";
    sql += input;
    sql += ",";
    sql += unit;
    sql += ",'";
    sql += zone;
    sql += "') m,COUNT(*) c FROM PackedTimeCodes WHERE ";
    sql += filter;
    sql += " GROUP BY m";
    sql += tail;
    return sql;
  };
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    for (const std::string_view unit : {"SECOND", "MINUTE", "DAY"}) {
      for (const std::string_view tail :
           {" ORDER BY m NULLS FIRST LIMIT 11 OFFSET 3",
            " ORDER BY m DESC NULLS LAST LIMIT 13", " ORDER BY c DESC LIMIT 7",
            " LIMIT 0"}) {
        EXPECT_EQ(RelationalRun(*rs_, query("s", unit, "n>=0", tail)),
                  RelationalRun(*rs_, query("s||''", unit, "n>=0", tail)));
      }
    }
    // Noncanonical valid values must restart ordinary evaluation for the
    // whole grouping operation, so peers never mix strings and packed keys.
    for (const std::string_view zone : {"UTC", "+09:00"}) {
      EXPECT_EQ(RelationalRun(*rs_, query("s", "MINUTE", "n>=-2",
                                          " ORDER BY m LIMIT 17", zone)),
                RelationalRun(*rs_, query("s||''", "MINUTE", "n>=-2",
                                          " ORDER BY m LIMIT 17", zone)));
    }
  }
  SqlEngine engine(*rs_);
  auto invalid = rs_->BeginReadOnlyContext();
  auto bad = engine.Execute(
      invalid, query("s", "MINUTE", "n>=-3", " ORDER BY c DESC LIMIT 1"));
  if (bad.HasValue()) {
    static_cast<void>(bad.Value().Collect());
    EXPECT_EQ(bad.Value().GetStatus(), Status::kInvalidArgument);
  } else {
    EXPECT_EQ(bad.GetStatus(), Status::kInvalidArgument);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, invalid.Abort());
  const std::string snapshot_query =
      query("s", "MINUTE", "n>=0", " ORDER BY c DESC,m NULLS FIRST LIMIT 13");
  const auto expected = RelationalRun(*rs_, snapshot_query);
  auto old = rs_->BeginReadOnlyContext();
  RelationalRun(
      *rs_, "INSERT INTO PackedTimeCodes VALUES (0,'2024-03-31 12:00:00+00')");
  ASSIGN_OR_ASSERT_FAIL(QueryResult, result,
                        engine.Execute(old, snapshot_query));
  EXPECT_EQ(result.Collect(), expected);
  ASSERT_SUCCESS(result.GetStatus());
  EXPECT_EQ(
      RelationalRun(*rs_, snapshot_query),
      RelationalRun(*rs_, query("s||''", "MINUTE", "n>=0",
                                " ORDER BY c DESC,m NULLS FIRST LIMIT 13")));
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest,
       DictionaryMultiplicityKeepsFirstEncounterOrder) {
  for (const int64_t domain : {300, 70000}) {
    SCOPED_TRACE(domain);
    const std::string name = "FoldOrder" + std::to_string(domain);
    const Schema schema(name, {Column("s", ValueType::kVarChar)});
    const int64_t rows = domain == 300 ? 30000 : domain * 2;
    {
      auto setup = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(setup, schema));
      for (int64_t i = 0; i < rows; ++i) {
        // Permute values and put NULL first. Repeated values cross pages,
        // and some values first occur only after their initial NULL slot.
        const int64_t id = (i * 73) % domain;
        const Value value =
            i % 997 == 0
                ? Value()
                : Value(std::string(80, 'x') +
                        static_cast<char>('a' + (id % 7)) + std::to_string(id));
        ASSERT_SUCCESS(table.Insert(setup.txn_, Row({value})).GetStatus());
      }
      ASSERT_SUCCESS(setup.PreCommit());
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (rs_->TransactionStats().version_chains != 0) {
      ASSERT_LT(std::chrono::steady_clock::now(), deadline);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable(name));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[0].wide_code_bytes, domain == 300 ? 2 : 4);
    const auto column = ColumnValueExp("s");
    const auto count =
        AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
    SelectStatement statement(
        {NamedExpression("s", column), NamedExpression("c", count)}, {},
        nullptr);
    statement.SetGroupBy({column});
    std::vector<Row> folded;
    ASSIGN_OR_ASSERT_FAIL(
        bool, used,
        relational_detail::TryFoldDictionaryAggregateRows(
            context, statement, *table, {}, schema,
            {&count->AsAggregateExpression()}, {},
            [&](const Row& row, int64_t multiplicity) {
              folded.emplace_back(Row({row[0], Value(multiplicity)}));
              return Status::kSuccess;
            }));
    ASSERT_TRUE(used);
    EXPECT_EQ(folded, RelationalRun(*rs_, "SELECT s||'',COUNT(*) FROM " + name +
                                              " GROUP BY s||''"));
    ASSERT_FALSE(folded.empty());
    EXPECT_TRUE(folded.front()[0].IsNull());
    size_t visits = 0;
    const auto stopped = relational_detail::TryFoldDictionaryAggregateRows(
        context, statement, *table, {}, schema,
        {&count->AsAggregateExpression()}, {}, [&](const Row&, int64_t) {
          ++visits;
          return Status::kConflicts;
        });
    EXPECT_EQ(stopped.GetStatus(), Status::kConflicts);
    EXPECT_EQ(visits, 1U);
    for (const size_t workers : {size_t{3}, size_t{7}}) {
      std::atomic<size_t> active{1};
      ScopedQueryParallelismBudget parallelism(&active, workers);
      std::vector<Row> prepared_rows;
      size_t prepared_visits = 0;
      ASSIGN_OR_ASSERT_FAIL(
          bool, prepared_used,
          relational_detail::TryFoldDictionaryAggregateRows(
              context, statement, *table, {}, schema,
              {&count->AsAggregateExpression()}, {},
              [&](const Row& row, int64_t multiplicity) {
                prepared_rows.emplace_back(Row({row[0], Value(multiplicity)}));
                return Status::kSuccess;
              },
              [&](const Row& row, int64_t multiplicity, const Value& key,
                  const std::vector<relational_detail::AggregateAccumulator>&
                      arguments) {
                EXPECT_EQ(key, row[0]);
                EXPECT_EQ(arguments.size(), 1U);
                EXPECT_EQ(arguments[0].count, multiplicity);
                prepared_rows.emplace_back(Row({row[0], Value(multiplicity)}));
                ++prepared_visits;
                return Status::kSuccess;
              }));
      EXPECT_TRUE(prepared_used);
      EXPECT_GT(prepared_visits, 0U);
      EXPECT_EQ(prepared_rows, folded);
    }
    ASSERT_SUCCESS(context.PreCommit());
    // A scalar key can merge several source codes into one group. Preserve
    // its first-occurrence tie order after filtering and applying OFFSET.
    EXPECT_EQ(
        RelationalRun(*rs_,
                      "SELECT REGEXP_REPLACE(s,'[0-9]+$','') k,COUNT(*) c,"
                      "MIN(s) FROM " +
                          name + " WHERE s<>'' GROUP BY k LIMIT 2 OFFSET 1"),
        RelationalRun(*rs_,
                      "SELECT REGEXP_REPLACE(s||'','[0-9]+$','') k,"
                      "COUNT(*) c,MIN(s) FROM " +
                          name + " WHERE s<>'' GROUP BY k LIMIT 2 OFFSET 1"));
  }
}

TEST_F(RelationalRegressionTest,
       ParallelDictionaryFoldKeepsTemporalUnitSemantics) {
  RelationalRun(*rs_, "CREATE TABLE TemporalFold(s STRING)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("TemporalFold"));
    for (int64_t i = 0; i < 6000; ++i) {
      std::string text = "2024-03-31 12:0";
      text += std::to_string((i % 300) / 60);
      text += i % 60 < 10 ? ":0" : ":";
      text += std::to_string(i % 60);
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({Value(std::move(text))})).GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  std::atomic<size_t> active{1};
  ScopedQueryParallelismBudget parallelism(&active, 3);
  for (const std::string_view function :
       {"TIMESTAMP_TRUNC", "DATETIME_TRUNC"}) {
    const auto query = [&](bool ordinary) {
      std::string sql = "SELECT ";
      sql += function;
      sql += ordinary ? "(s||'',MINUTE) k" : "(s,MINUTE) k";
      sql +=
          ",COUNT(*) c,MIN(s),AVG(BYTE_LENGTH(s)) FROM TemporalFold "
          "GROUP BY k HAVING c>0 ORDER BY k";
      return sql;
    };
    const auto expected = RelationalRun(*rs_, query(true));
    ASSERT_EQ(expected.size(), 5U);
    EXPECT_EQ(RelationalRun(*rs_, query(false)), expected);
  }
}

TEST_F(RelationalRegressionTest, ParallelScalarFoldWithoutGlobalDictionary) {
  RelationalRun(*rs_, "CREATE TABLE RawScalar(s STRING)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("RawScalar"));
    for (int64_t i = 0; i < 12000; ++i) {
      std::string text(static_cast<size_t>(i % 19 == 0 ? 2000 : 1), 'x');
      text += std::to_string(i);
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({i % 97 == 0    ? Value()
                            : i % 101 == 0 ? Value("")
                                           : Value(std::move(text))}))
              .GetStatus());
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
                          context.GetTable("RawScalar"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[0].wide_code_bytes, 0);
    ASSERT_SUCCESS(context.PreCommit());
  }
  const auto query = [](bool ordinary) {
    std::string sql = "SELECT REGEXP_REPLACE(";
    sql += ordinary ? "s||''" : "s";
    sql +=
        ",'[0-9]+$','') k,COUNT(*) c,COUNT(s),MIN(s),MAX(s),"
        "AVG(CAST(BYTE_LENGTH(s) AS FLOAT64)) FROM RawScalar "
        "WHERE s<>'' GROUP BY k HAVING c>3 ORDER BY k LIMIT 2";
    return sql;
  };
  const auto expected = RelationalRun(*rs_, query(true));
  for (const size_t workers : {size_t{1}, size_t{3}, size_t{7}}) {
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    EXPECT_EQ(RelationalRun(*rs_, query(false)), expected);
    struct RestoreBudget {
      RestoreBudget() = default;
      RestoreBudget(const RestoreBudget&) = delete;
      RestoreBudget& operator=(const RestoreBudget&) = delete;
      RestoreBudget(RestoreBudget&&) = delete;
      RestoreBudget& operator=(RestoreBudget&&) = delete;
      size_t limit = QueryMemoryBudget::Global().Limit();
      ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
    } restore;
    QueryMemoryBudget::Global().ResetForTest(size_t{4} << 20);
    EXPECT_EQ(RelationalRun(*rs_, query(false)), expected);
  }
}

TEST_F(RelationalRegressionTest,
       DictionaryMultiplicityKeepsAggregatesAndHaving) {
  RelationalRun(*rs_, "CREATE TABLE RepeatedText(s STRING)");
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("RepeatedText"));
    for (int64_t i = 0; i < 12000; ++i) {
      std::string text = "圧縮された文字列-";
      text.push_back(static_cast<char>('a' + (i % 7)));
      text += std::string(static_cast<size_t>(i % 5), 'x');
      text += std::to_string(i % 600);
      ASSERT_SUCCESS(table
                         ->Insert(context.txn_,
                                  Row({i % 19 == 0   ? Value()
                                       : i % 17 == 0 ? Value("")
                                                     : Value(std::move(text))}))
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
  auto context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context.GetTable("RepeatedText"));
  const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
  ASSERT_NE(store, nullptr);
  ASSERT_EQ(store->dictionaries[0].wide_code_bytes, 2);
  for (const size_t workers : {size_t{1}, size_t{3}, size_t{7}}) {
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    for (const bool filtered : {false, true}) {
      const Expression column = ColumnValueExp("s");
      const Expression key = FunctionCallExp(
          "regexp_replace", {column, ConstantValueExp(Value("[0-9]+$")),
                             ConstantValueExp(Value(""))});
      const auto count =
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
      const auto nonnull =
          AggregateExpressionExp(AggregationType::kCount, column);
      const auto minimum =
          AggregateExpressionExp(AggregationType::kMin, column);
      const auto maximum =
          AggregateExpressionExp(AggregationType::kMax, column);
      const auto average = AggregateExpressionExp(
          AggregationType::kAvg, FunctionCallExp("byte_length", {column}));
      SelectStatement statement(
          {NamedExpression("k", key), NamedExpression("c", count),
           NamedExpression("n", nonnull), NamedExpression("lo", minimum),
           NamedExpression("hi", maximum), NamedExpression("av", average)},
          {},
          filtered ? BinaryExpressionExp(column, BinaryOperation::kNotEquals,
                                         ConstantValueExp(Value("")))
                   : Expression{});
      SelectSource source;
      source.table = "RepeatedText";
      statement.SetSources({source});
      statement.SetGroupBy({key});
      statement.SetHaving(BinaryExpressionExp(
          count, BinaryOperation::kGreaterThan, ConstantValueExp(Value(100))));
      statement.SetOrderBy({{key, true, true}});
      statement.SetLimit(13);
      statement.SetOffset(1);
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, result,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_EQ(runtime.scan_rows, 12000U);
      EXPECT_LT(runtime.scan_values_decoded, 5000U);
      EXPECT_GT(runtime.scan_output_rows, runtime.scan_values_decoded);
      std::string reference =
          "SELECT REGEXP_REPLACE(s||'','[0-9]+$','') k,COUNT(*) c,COUNT(s),"
          "MIN(s),MAX(s),AVG(CAST(BYTE_LENGTH(s) AS FLOAT64)) FROM "
          "RepeatedText";
      if (filtered) {
        reference += " WHERE s<>''";
      }
      reference +=
          " GROUP BY k HAVING c>100 ORDER BY k NULLS FIRST LIMIT 13 OFFSET 1";
      EXPECT_EQ(result.rows, RelationalRun(*rs_, reference));
      context.set_execution_runtime(nullptr);
    }
  }
  // Decline order-sensitive/overflow-sensitive payloads rather than changing
  // their accumulation order. The callback must never run on rejection.
  const auto key = FunctionCallExp("lower", {ColumnValueExp("s")});
  const auto sum = AggregateExpressionExp(
      AggregationType::kSum,
      FunctionCallExp("byte_length", {ColumnValueExp("s")}));
  SelectStatement unsupported(
      {NamedExpression("k", key), NamedExpression("v", sum)}, {}, nullptr);
  unsupported.SetGroupBy({key});
  bool visited = false;
  ASSIGN_OR_ASSERT_FAIL(
      bool, folded,
      relational_detail::TryFoldDictionaryAggregateRows(
          context, unsupported, *table, {}, table->GetSchema(),
          {&sum->AsAggregateExpression()}, {}, [&](const Row&, int64_t) {
            visited = true;
            return Status::kSuccess;
          }));
  EXPECT_FALSE(folded);
  EXPECT_FALSE(visited);
  ASSERT_SUCCESS(context.PreCommit());
  // Invalid scalar expressions are not evaluated for rejected dictionary
  // values, but LIMIT must not hide an error on an observed input.
  EXPECT_TRUE(
      RelationalRun(
          *rs_,
          "SELECT REGEXP_REPLACE(s,'[','') k,COUNT(*),MIN(s) FROM RepeatedText "
          "WHERE s='absent' GROUP BY k LIMIT 1")
          .empty());
  auto error_context = rs_->BeginReadOnlyContext();
  SqlEngine engine(*rs_);
  auto result = engine.Execute(
      error_context,
      "SELECT REGEXP_REPLACE(s,'[','') k,COUNT(*),MIN(s) FROM RepeatedText "
      "GROUP BY k LIMIT 1");
  if (result.HasValue()) {
    result.Value().Drain();
    EXPECT_NE(result.Value().GetStatus(), Status::kSuccess);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, error_context.Abort());
}

TEST_F(RelationalRegressionTest,
       CompositeDictionaryScalarKeysKeepRowsAndErrors) {
  RelationalRun(*rs_,
                "CREATE TABLE CompositeTime(k INT64,s TIMESTAMP,g INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("CompositeTime"));
    for (int64_t i = 0; i < 12000; ++i) {
      std::string text = "2020-01-01 00:";
      const int64_t minute = (i % 600) / 60;
      text.push_back(static_cast<char>('0' + (minute / 10)));
      text.push_back(static_cast<char>('0' + (minute % 10)));
      text.push_back(':');
      text.push_back(static_cast<char>('0' + ((i % 60) / 10)));
      text.push_back(static_cast<char>('0' + (i % 10)));
      ASSERT_SUCCESS(
          table
              ->Insert(
                  setup.txn_,
                  Row({i % 17 == 0 ? Value() : Value((i % 300) * 1000000001),
                       i % 19 == 0 ? Value() : Value(std::move(text)),
                       i % 23 == 0 ? Value() : Value(i % 301)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const bool trunc : {false, true}) {
      const Expression key =
          trunc ? FunctionCallExp("timestamp_trunc", {ColumnValueExp("s"),
                                                      ColumnValueExp("MINUTE")})
                : FunctionCallExp("extract_minute", {ColumnValueExp("s")});
      const auto count =
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
      SelectStatement statement(
          {NamedExpression("k", ColumnValueExp("k")), NamedExpression("m", key),
           NamedExpression("g", ColumnValueExp("g")),
           NamedExpression("c", count)},
          {}, nullptr);
      SelectSource source;
      source.table = "CompositeTime";
      statement.SetSources({source});
      statement.SetGroupBy({ColumnValueExp("k"), key, ColumnValueExp("g")});
      statement.SetOrderBy({{AggregateExpressionExp(AggregationType::kCount,
                                                    ColumnValueExp("*")),
                             false, std::nullopt},
                            {ColumnValueExp("k"), true, true},
                            {key, true, false},
                            {ColumnValueExp("g"), true, true}});
      statement.SetLimit(13);
      statement.SetOffset(5);
      auto context = rs_->BeginReadOnlyContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                            context.GetTable("CompositeTime"));
      const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
      ASSERT_NE(store, nullptr);
      for (const auto& dictionary : store->dictionaries) {
        ASSERT_EQ(dictionary.wide_code_bytes, 2);
      }
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, result,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
      EXPECT_EQ(runtime.scan_values_decoded, 0U);
      std::string expected = "SELECT k,";
      expected += trunc ? "TIMESTAMP_TRUNC(s||'',MINUTE)"
                        : "EXTRACT(MINUTE FROM s||'')";
      expected +=
          " m,g,COUNT(*) c FROM CompositeTime GROUP BY k,m,g ORDER BY c DESC,"
          "k NULLS FIRST,m NULLS LAST,g NULLS FIRST LIMIT 13 OFFSET 5";
      EXPECT_EQ(result.rows, RelationalRun(*rs_, expected));
      ASSERT_SUCCESS(context.PreCommit());
    }
  }
  // A filtered invalid timestamp must stay on the ordinary evaluator, while
  // an observed invalid value must raise even when LIMIT would discard it.
  RelationalRun(*rs_,
                "INSERT INTO CompositeTime VALUES "
                "(9223372036854775807,'invalid-timestamp',0)");
  EXPECT_FALSE(RelationalRun(*rs_,
                             "SELECT k,TIMESTAMP_TRUNC(s,MINUTE) m,g,COUNT(*) "
                             "FROM CompositeTime WHERE k<9223372036854775807 "
                             "GROUP BY k,m,g LIMIT 1")
                   .empty());
  const auto invalid_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), invalid_deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  auto context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context.GetTable("CompositeTime"));
  const auto store = BuildLSTableStore(context.txn_, *table);
  ASSERT_NE(store, nullptr);
  SqlEngine engine(*rs_);
  auto result =
      engine.Execute(context,
                     "SELECT k,TIMESTAMP_TRUNC(s,MINUTE) m,g,COUNT(*) "
                     "FROM CompositeTime GROUP BY k,m,g ORDER BY k LIMIT 1");
  if (result.HasValue()) {
    result.Value().Drain();
    EXPECT_NE(result.Value().GetStatus(), Status::kSuccess);
  } else {
    EXPECT_NE(result.GetStatus(), Status::kSuccess);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, DictionaryCaseKeysAndNonKeyFiltersKeepRows) {
  RelationalRun(*rs_,
                "CREATE TABLE CaseCodes(a INT64,b INT64,c INT64,s STRING,t "
                "STRING,f INT64,d DATE)");
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("CaseCodes"));
    for (int64_t i = 0; i < 15000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(
                  context.txn_,
                  Row({i % 31 == 0 ? Value() : Value(i % 3), Value(i % 5),
                       Value(i % 7),
                       i % 19 == 0   ? Value()
                       : i % 17 == 0 ? Value("")
                                     : Value(std::string(80, 's') +
                                             std::to_string(i % 307)),
                       i % 23 == 0 ? Value()
                                   : Value(std::string(70, 't') +
                                           std::to_string(i % 311)),
                       i % 11 == 0 ? Value() : Value(i % 5),
                       i % 29 == 0 ? Value() : Value::DateFromDays(18262)}))
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
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const auto& literal :
         std::vector<Value>{Value(""), Value("fallback"), Value()}) {
      auto context = rs_->BeginReadOnlyContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                            context.GetTable("CaseCodes"));
      const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
      ASSERT_NE(store, nullptr);
      ASSERT_TRUE(store->dictionaries[1].active);
      ASSERT_EQ(store->dictionaries[3].wide_code_bytes, 2);
      ASSERT_EQ(store->dictionaries[5].wide_code_bytes, 2);
      const auto a = ColumnValueExp("a");
      const auto b = ColumnValueExp("b");
      const auto c = ColumnValueExp("c");
      const auto text = ColumnValueExp("t");
      const auto condition =
          BinaryExpressionExp(BinaryExpressionExp(a, BinaryOperation::kEquals,
                                                  ConstantValueExp(Value(0))),
                              BinaryOperation::kAnd,
                              BinaryExpressionExp(b, BinaryOperation::kEquals,
                                                  ConstantValueExp(Value(0))));
      const auto key = CaseExpressionExp({{condition, ColumnValueExp("s")}},
                                         ConstantValueExp(literal));
      const auto count =
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
      const auto predicate = BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("f"),
                              BinaryOperation::kGreaterThanEquals,
                              ConstantValueExp(Value(2))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("d"),
                              BinaryOperation::kGreaterThanEquals,
                              ConstantValueExp(Value("2020-01-01"))));
      SelectStatement statement(
          {NamedExpression("a", a), NamedExpression("b", b),
           NamedExpression("c", c), NamedExpression("src", key),
           NamedExpression("dst", text), NamedExpression("n", count)},
          {}, predicate);
      SelectSource source;
      source.table = "CaseCodes";
      statement.SetSources({source});
      statement.SetGroupBy({a, b, c, key, text});
      statement.SetOrderBy({{count, false, std::nullopt},
                            {a, true, true},
                            {b, true, false},
                            {c, true, false},
                            {key, true, true},
                            {text, true, false}});
      statement.SetLimit(13);
      statement.SetOffset(5);
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, result,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
      EXPECT_EQ(runtime.scan_values_decoded, 0U);
      EXPECT_GT(runtime.scan_output_rows, 0U);
      EXPECT_LT(runtime.scan_output_rows, runtime.scan_rows);
      const std::string sql_literal =
          literal.IsNull() ? "NULL"
                           : "'" + std::string(literal.AsStringView()) + "'";
      const std::string reference =
          "SELECT a,b,c,CASE WHEN a=0 AND b=0 THEN s||'' ELSE " + sql_literal +
          " END src,t dst,COUNT(*) n FROM CaseCodes WHERE f>=2 AND "
          "d>='2020-01-01' GROUP BY a,b,c,src,dst ORDER BY n DESC,a NULLS "
          "FIRST,b,c,"
          "src NULLS FIRST,dst NULLS LAST LIMIT 13 OFFSET 5";
      EXPECT_EQ(result.rows, RelationalRun(*rs_, reference));
      statement.SetWhereClause(BinaryExpressionExp(
          ColumnValueExp("f"), BinaryOperation::kGreaterThan,
          ConstantValueExp(Value(100))));
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, empty,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_TRUE(empty.rows.empty());
      ASSERT_SUCCESS(context.PreCommit());
    }
  }
  // An interval-shaped ELSE string is still ordinary text. The dictionary
  // path and the materialized string path must preserve the same groups.
  EXPECT_EQ(
      RelationalRun(
          *rs_,
          "SELECT a,b,c,CASE WHEN a=0 THEN s ELSE '0-0 0 24:0:0' END "
          "src,t,COUNT(*) "
          "FROM CaseCodes GROUP BY a,b,c,src,t ORDER BY a,b,c,src,t LIMIT 11"),
      RelationalRun(
          *rs_,
          "SELECT a,b,c,CASE WHEN a=0 THEN s||'' ELSE '0-0 0 24:0:0' END "
          "src,t,COUNT(*) "
          "FROM CaseCodes GROUP BY a,b,c,src,t ORDER BY a,b,c,src,t LIMIT 11"));
}

TEST_F(RelationalRegressionTest,
       DictionaryMembershipFiltersKeepNullsAndOffsets) {
  RelationalRun(*rs_,
                "CREATE TABLE ListCodes(k INT64,s STRING,f INT64,t STRING)");
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("ListCodes"));
    for (int64_t i = 0; i < 12000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(context.txn_,
                       Row({Value(i % 7),
                            i % 19 == 0 ? Value()
                                        : Value(std::string(80, 'A') +
                                                std::to_string(i % 301)),
                            i % 11 == 0 ? Value() : Value((i % 5) - 2),
                            i % 13 == 0 ? Value()
                                        : Value(i % 3 == 0   ? ""
                                                : i % 3 == 1 ? "alpha"
                                                             : "beta")}))
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
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const bool scalar : {false, true}) {
      auto context = rs_->BeginReadOnlyContext();
      const auto k = ColumnValueExp("k");
      const auto s = ColumnValueExp("s");
      const auto key = scalar ? FunctionCallExp("lower", {s}) : s;
      const auto count =
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
      std::vector<NamedExpression> select;
      std::vector<Expression> keys;
      if (!scalar) {
        select.emplace_back("k", k);
        keys.push_back(k);
      }
      select.emplace_back("s", key);
      select.emplace_back("c", count);
      keys.push_back(key);
      SelectStatement statement(std::move(select), {}, nullptr);
      SelectSource source;
      source.table = "ListCodes";
      statement.SetSources({source});
      statement.SetGroupBy(keys);
      std::vector<SelectStatement::OrderByTerm> order{
          {count, false, std::nullopt}};
      for (const auto& group : keys) {
        order.push_back({group, true, false});
      }
      statement.SetOrderBy(std::move(order));
      statement.SetLimit(13);
      statement.SetOffset(3);
      statement.SetWhereClause(BinaryExpressionExp(
          InExpressionExp(ColumnValueExp("f"),
                          {UnaryExpressionExp(ConstantValueExp(Value(1)),
                                              UnaryOperation::kMinus),
                           BinaryExpressionExp(ConstantValueExp(Value(1)),
                                               BinaryOperation::kAdd,
                                               ConstantValueExp(Value(1))),
                           ConstantValueExp(Value(-1))}),
          BinaryOperation::kAnd,
          InExpressionExp(ColumnValueExp("t"),
                          {ConstantValueExp(Value("")),
                           ConstantValueExp(Value("alpha"))})));
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, result,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
      EXPECT_EQ(runtime.scan_values_decoded, 0U);
      ASSERT_EQ(result.rows.size(), 13U);
      const std::string reference =
          scalar
              ? "SELECT LOWER(s||'') x,COUNT(*) c FROM ListCodes WHERE f IN "
                "(-1,2,-1) AND t IN ('','alpha') GROUP BY x ORDER BY c DESC,x "
                "NULLS LAST LIMIT 13 OFFSET 3"
              : "SELECT k,s||'' x,COUNT(*) c FROM ListCodes WHERE f IN "
                "(-1,2,-1) AND t IN ('','alpha') GROUP BY k,x ORDER BY c "
                "DESC,k NULLS LAST,x NULLS LAST LIMIT 13 OFFSET 3";
      EXPECT_EQ(result.rows, RelationalRun(*rs_, reference));
      statement.SetWhereClause(
          InExpressionExp(ColumnValueExp("f"), {ConstantValueExp(Value(100))}));
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, empty,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_TRUE(empty.rows.empty());
      for (const int64_t target : {3, 100}) {
        statement.SetWhereClause(BinaryExpressionExp(
            BinaryExpressionExp(k, BinaryOperation::kEquals,
                                ConstantValueExp(Value(target))),
            BinaryOperation::kAnd,
            InExpressionExp(ColumnValueExp("f"),
                            {ConstantValueExp(Value(-1))})));
        ASSIGN_OR_ASSERT_FAIL(
            relational_detail::Relation, selective,
            relational_detail::ExecuteQuery(context, statement, nullptr, {}));
        const std::string predicate =
            " WHERE f IN (-1) AND k=" + std::to_string(target);
        const std::string oracle =
            scalar
                ? "SELECT LOWER(s||'') x,COUNT(*) c FROM ListCodes" +
                      predicate +
                      " GROUP BY x ORDER BY c DESC,x NULLS LAST LIMIT 13 "
                      "OFFSET 3"
                : "SELECT k,s||'' x,COUNT(*) c FROM ListCodes" + predicate +
                      " GROUP BY k,x ORDER BY c DESC,k NULLS LAST,x NULLS LAST "
                      "LIMIT 13 OFFSET 3";
        EXPECT_EQ(selective.rows, RelationalRun(*rs_, oracle));
        EXPECT_EQ(selective.rows.size(), target == 3 ? 13U : 0U);
        EXPECT_EQ(runtime.scan_values_decoded, 0U);
      }
      ASSERT_SUCCESS(context.PreCommit());
    }
  }
  // NULL list elements and NOT IN retain SQL's three-valued fallback.
  for (const std::string predicate : {"f IN (2,NULL)", "f NOT IN (2,NULL)"}) {
    EXPECT_EQ(RelationalRun(*rs_, "SELECT k,s,COUNT(*) FROM ListCodes WHERE " +
                                      predicate +
                                      " GROUP BY k,s ORDER BY k,s LIMIT 11"),
              RelationalRun(
                  *rs_, "SELECT k,s||'' x,COUNT(*) FROM ListCodes WHERE " +
                            predicate + " GROUP BY k,x ORDER BY k,x LIMIT 11"));
  }
}

TEST_F(RelationalRegressionTest, CompositeDictionaryUnsignedKeysKeepTags) {
  RelationalRun(*rs_, "CREATE TABLE UnsignedCodes(k UINT64,s STRING)");
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("UnsignedCodes"));
    for (int64_t i = 0; i < 12000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(context.txn_, Row({Value(-1 - (i % 7)).WithUnsigned(),
                                          Value(std::string(80, 'x') +
                                                std::to_string(i % 300))}))
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
  auto context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        context.GetTable("UnsignedCodes"));
  const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
  ASSERT_NE(store, nullptr);
  ASSERT_TRUE(store->dictionaries[0].active);
  ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
  const auto k = ColumnValueExp("k");
  const auto s = ColumnValueExp("s");
  const auto count =
      AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
  SelectStatement statement({NamedExpression("k", k), NamedExpression("s", s),
                             NamedExpression("c", count)},
                            {}, nullptr);
  SelectSource source;
  source.table = "UnsignedCodes";
  statement.SetSources({source});
  statement.SetGroupBy({k, s});
  statement.SetOrderBy({{k, true, std::nullopt}, {s, true, std::nullopt}});
  statement.SetLimit(11);
  relational_detail::ExecutionRuntime runtime;
  context.set_execution_runtime(&runtime);
  ASSIGN_OR_ASSERT_FAIL(
      relational_detail::Relation, result,
      relational_detail::ExecuteQuery(context, statement, nullptr, {}));
  ASSERT_EQ(result.rows.size(), 11U);
  EXPECT_TRUE(result.schema.GetColumn(0).IsUnsigned());
  for (const auto& row : result.rows) {
    EXPECT_TRUE(row[0].IsUnsigned());
  }
  EXPECT_EQ(
      result.rows,
      RelationalRun(
          *rs_,
          "SELECT k,s||'' text,COUNT(*) FROM UnsignedCodes GROUP BY k,text "
          "ORDER BY k,text LIMIT 11"));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(RelationalRegressionTest, CompositeDictionaryMixedWidthsFilterKeys) {
  RelationalRun(*rs_, "CREATE TABLE MixedCodes(k INT64,s STRING)");
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("MixedCodes"));
    for (int64_t i = 0; i < 15000; ++i) {
      std::string text = std::string(80, 'x') + std::to_string(i % 601);
      ASSERT_SUCCESS(table
                         ->Insert(context.txn_,
                                  Row({Value(i % 7),
                                       i % 19 == 0   ? Value()
                                       : i % 17 == 0 ? Value("")
                                                     : Value(std::move(text))}))
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
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    for (const int64_t low : {int64_t{-1}, int64_t{2}, int64_t{100}}) {
      auto context = rs_->BeginReadOnlyContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                            context.GetTable("MixedCodes"));
      const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
      ASSERT_NE(store, nullptr);
      ASSERT_TRUE(store->dictionaries[0].active);
      const auto& codes = store->dictionaries[0].narrow_codes;
      ASSERT_EQ(codes.size(), 15000U);
      size_t code_offset = 0;
      for (const auto& block : store->blocks) {
        EXPECT_EQ(block.columns[0].dict_codes.data(),
                  codes.data() + code_offset);
        for (size_t r = 0; r < block.row_count; ++r) {
          EXPECT_EQ(block.columns[0].DictionaryCodeAt(r),
                    codes[code_offset + r]);
        }
        code_offset += block.row_count;
      }
      EXPECT_EQ(code_offset, codes.size());
      ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
      const auto k = ColumnValueExp("k");
      const auto text = ColumnValueExp("s");
      const auto count =
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
      const auto predicate = BinaryExpressionExp(
          BinaryExpressionExp(k, BinaryOperation::kGreaterThan,
                              ConstantValueExp(Value(low))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(text, BinaryOperation::kNotEquals,
                              ConstantValueExp(Value(""))));
      SelectStatement statement(
          {NamedExpression("k", k), NamedExpression("s", text),
           NamedExpression("c", count)},
          {}, predicate);
      SelectSource source;
      source.table = "MixedCodes";
      statement.SetSources({source});
      statement.SetGroupBy({k, text});
      statement.SetOrderBy({{count, false, std::nullopt},
                            {k, false, true},
                            {text, true, false}});
      statement.SetLimit(13);
      statement.SetOffset(5);
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, result,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
      EXPECT_EQ(runtime.scan_values_decoded, 0U);
      EXPECT_EQ(
          result.rows,
          RelationalRun(
              *rs_,
              "SELECT k,s||'' AS text,COUNT(*) c FROM MixedCodes WHERE k>" +
                  std::to_string(low) +
                  " AND s<>'' GROUP BY k,text ORDER BY c DESC,k DESC NULLS "
                  "FIRST,"
                  "text NULLS LAST LIMIT 13 OFFSET 5"));
      ASSERT_SUCCESS(context.PreCommit());
    }
  }
}

TEST_F(RelationalRegressionTest, CompositeDictionaryGroupsKeepNullsAndOrder) {
  const Schema schema("CompositeCodes", {Column("k", ValueType::kInt64),
                                         Column("s", ValueType::kVarChar)});
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(setup, schema));
    for (int64_t i = 0; i < 15000; ++i) {
      std::string text(80, 'x');
      text += std::to_string((i / 7) % 301);
      const Value key = i % 17 == 0 ? Value() : Value((i % 300) * 1000000001);
      const Value string = i % 19 == 0 ? Value() : Value(std::move(text));
      ASSERT_SUCCESS(table.Insert(setup.txn_, Row({key, string})).GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{8}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    const auto count =
        AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
    const auto key = ColumnValueExp("k");
    const auto string = ColumnValueExp("s");
    SelectStatement statement(
        {NamedExpression("k", key), NamedExpression("s", string),
         NamedExpression("c", count)},
        {}, nullptr);
    SelectSource source;
    source.table = "CompositeCodes";
    statement.SetSources({source});
    statement.SetGroupBy({key, string});
    statement.SetOrderBy({{count, false, std::nullopt},
                          {key, true, true},
                          {string, false, false}});
    statement.SetLimit(13);
    statement.SetOffset(7);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("CompositeCodes"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[0].wide_code_bytes, 2);
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    ASSIGN_OR_ASSERT_FAIL(
        relational_detail::Relation, result,
        relational_detail::ExecuteQuery(context, statement, nullptr, {}));
    EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
    EXPECT_EQ(runtime.scan_values_decoded, 0U);
    EXPECT_EQ(
        result.rows,
        RelationalRun(
            *rs_,
            "SELECT MIN(k) k,s||'' s,COUNT(*) c FROM CompositeCodes "
            "GROUP BY CAST(k AS STRING),s||'' ORDER BY c DESC,k NULLS FIRST,"
            "s DESC NULLS LAST LIMIT 13 OFFSET 7"));
    ASSERT_SUCCESS(context.PreCommit());
    statement.SetOrderBy({});
    statement.SetLimit(11);
    statement.SetOffset(5);
    auto prefix_context = rs_->BeginReadOnlyContext();
    relational_detail::ExecutionRuntime prefix_runtime;
    prefix_context.set_execution_runtime(&prefix_runtime);
    ASSIGN_OR_ASSERT_FAIL(relational_detail::Relation, prefix_result,
                          relational_detail::ExecuteQuery(
                              prefix_context, statement, nullptr, {}));
    EXPECT_LE(prefix_runtime.aggregate_groups, 16U);
    EXPECT_EQ(
        prefix_result.rows,
        RelationalRun(*rs_,
                      "SELECT MIN(k),s||'',COUNT(*) FROM CompositeCodes "
                      "GROUP BY CAST(k AS STRING),s||'' LIMIT 11 OFFSET 5"));
    ASSERT_SUCCESS(prefix_context.PreCommit());
    // No ORDER BY retains first-occurrence order, independently of worker
    // completion order and the hash table's iteration order.
    EXPECT_EQ(
        RelationalRun(*rs_,
                      "SELECT k,s,COUNT(*) FROM CompositeCodes "
                      "GROUP BY k,s LIMIT 11 OFFSET 5"),
        RelationalRun(*rs_,
                      "SELECT MIN(k),s||'',COUNT(*) FROM CompositeCodes "
                      "GROUP BY CAST(k AS STRING),s||'' LIMIT 11 OFFSET 5"));
    EXPECT_TRUE(RelationalRun(*rs_,
                              "SELECT k,s,COUNT(*) FROM CompositeCodes "
                              "GROUP BY k,s LIMIT 0")
                    .empty());
  }
  // Byte-distinct INTERVAL spellings are one SQL key. Rebuilding the
  // dictionary must revoke code-equality eligibility for the entire column.
  RelationalRun(*rs_,
                "INSERT INTO CompositeCodes VALUES "
                "(9223372036854775807,'0-12 0 0:0:0'),"
                "(9223372036854775807,'1-0 0 0:0:0')");
  const auto peer_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), peer_deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  {
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("CompositeCodes"));
    // Lazy lookup deliberately declines a stale nonempty cache; explicitly
    // build the new committed snapshot for this eligibility regression.
    const auto store = BuildLSTableStore(context.txn_, *table);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
    EXPECT_TRUE(store->dictionaries[1].bytewise_equality);
    ASSERT_SUCCESS(context.PreCommit());
  }
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(*) FROM CompositeCodes "
                          "GROUP BY k,s ORDER BY k DESC NULLS LAST LIMIT 1"),
            std::vector<Row>{
                Row({Value(std::numeric_limits<int64_t>::max()), Value(1)})});
}

TEST_F(RelationalRegressionTest, CompositeDictionaryCountsMergeAcrossWorkers) {
  const Schema schema("MergedCodes", {Column("k", ValueType::kInt64),
                                      Column("s", ValueType::kVarChar)});
  const std::string common(80, 'x');
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(setup, schema));
    for (int64_t i = 0; i < 90000; ++i) {
      // A count above 16 bits and duplicate keys on every worker. The tail
      // also includes NULL and groups tied at the LIMIT boundary.
      const int64_t id = i % 5 == 0 ? (i % 307) + 1 : 0;
      const Value key = id == 1 ? Value() : Value(id * 1000000001);
      const Value text = id == 2 ? Value() : Value(common + std::to_string(id));
      ASSERT_SUCCESS(table.Insert(setup.txn_, Row({key, text})).GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  for (const size_t workers :
       {size_t{1}, size_t{2}, size_t{3}, size_t{4}, size_t{8}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    const auto key = ColumnValueExp("k");
    const auto text = ColumnValueExp("s");
    const auto count =
        AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
    SelectStatement statement(
        {NamedExpression("k", key), NamedExpression("s", text),
         NamedExpression("c", count)},
        {}, nullptr);
    SelectSource source;
    source.table = "MergedCodes";
    statement.SetSources({source});
    statement.SetGroupBy({key, text});
    statement.SetOrderBy({{count, false, std::nullopt}});
    statement.SetLimit(11);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("MergedCodes"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[0].wide_code_bytes, 2);
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    ASSIGN_OR_ASSERT_FAIL(
        relational_detail::Relation, result,
        relational_detail::ExecuteQuery(context, statement, nullptr, {}));
    EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
    EXPECT_EQ(runtime.scan_values_decoded, 0U);
    ASSERT_EQ(result.rows.size(), 11U);
    EXPECT_EQ(result.rows.front(),
              Row({Value(0), Value(common + "0"), Value(72000)}));
    EXPECT_EQ(result.rows,
              RelationalRun(*rs_,
                            "SELECT MIN(k),s||'',COUNT(*) c FROM MergedCodes "
                            "GROUP BY CAST(k AS STRING),s||'' ORDER BY c DESC "
                            "LIMIT 11"));
    ASSERT_SUCCESS(context.PreCommit());
    for (const std::string_view suffix :
         {" LIMIT 17 OFFSET 5", " ORDER BY c DESC LIMIT 13 OFFSET 3",
          " ORDER BY c ASC LIMIT 13 OFFSET 3"}) {
      EXPECT_EQ(RelationalRun(*rs_,
                              "SELECT k,s,COUNT(*) c FROM MergedCodes "
                              "WHERE k<>0 GROUP BY k,s" +
                                  std::string(suffix)),
                RelationalRun(*rs_,
                              "SELECT MIN(k),s||'',COUNT(*) c FROM MergedCodes "
                              "WHERE k<>0 GROUP BY CAST(k AS STRING),s||''" +
                                  std::string(suffix)));
    }
    EXPECT_TRUE(RelationalRun(*rs_,
                              "SELECT k,s,COUNT(*) FROM MergedCodes WHERE k<0 "
                              "GROUP BY k,s ORDER BY COUNT(*) DESC LIMIT 10")
                    .empty());
  }
}

TEST_F(RelationalRegressionTest,
       DictionaryDistinctCountsMergeAndKeepNullGroups) {
  RelationalRun(*rs_, "CREATE TABLE DistinctCodes(v INT64,k INT64,n INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("DistinctCodes"));
    for (int64_t i = 0; i < 90000; ++i) {
      const int64_t t = i % 30000;
      const int64_t k = (t * 37) % 307;
      ASSERT_SUCCESS(
          table
              ->Insert(
                  setup.txn_,
                  Row({k == 0 || t % 17 == 0
                           ? Value()
                           : Value(((t * 19) % 1021) * 1000000001),
                       k == 1 ? Value() : Value(k * 10000001), Value(t % 7)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const auto key = ColumnValueExp("k");
  const auto count =
      AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
  for (const std::string_view argument : {"v", "n"}) {
    const auto distinct = AggregateExpressionExp(
        AggregationType::kCount, ColumnValueExp(std::string(argument)), true);
    SelectStatement statement(
        {NamedExpression("k", key), NamedExpression("d", distinct),
         NamedExpression("c", count)},
        {}, nullptr);
    SelectSource source;
    source.table = "DistinctCodes";
    statement.SetSources({source});
    statement.SetGroupBy({key});
    statement.SetOrderBy(
        {{distinct, false, std::nullopt}, {count, false, std::nullopt}});
    statement.SetLimit(13);
    for (const size_t workers : {size_t{1}, size_t{2}, size_t{4}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      auto context = rs_->BeginReadOnlyContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                            context.GetTable("DistinctCodes"));
      const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
      ASSERT_NE(store, nullptr);
      ASSERT_EQ(store->dictionaries[0].wide_code_bytes, 2);
      ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
      ASSERT_TRUE(store->dictionaries[2].active);
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, result,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
      EXPECT_EQ(runtime.scan_values_decoded, 0U);
      const std::string prefix = "SELECT k,COUNT(DISTINCT " +
                                 std::string(argument) +
                                 ") d,COUNT(*) c FROM DistinctCodes";
      EXPECT_EQ(
          result.rows,
          RelationalRun(*rs_, prefix + " GROUP BY k,CAST(k AS STRING) ORDER BY "
                                       "d DESC,c DESC LIMIT 13"));
      ASSERT_SUCCESS(context.PreCommit());
      for (const std::string_view filter : {"", " WHERE k>0 AND n<3"}) {
        for (const std::string_view tail :
             {" LIMIT 11 OFFSET 3", " ORDER BY d DESC LIMIT 7 OFFSET 2",
              " ORDER BY k DESC NULLS FIRST LIMIT 19",
              " ORDER BY d DESC LIMIT 3 WITH TIES", " LIMIT 0"}) {
          EXPECT_EQ(RelationalRun(*rs_, prefix + std::string(filter) +
                                            " GROUP BY k" + std::string(tail)),
                    RelationalRun(*rs_, prefix + std::string(filter) +
                                            " GROUP BY k,CAST(k AS STRING)" +
                                            std::string(tail)));
        }
      }
    }
  }
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(DISTINCT v) FROM DistinctCodes "
                          "WHERE k=0 GROUP BY k LIMIT 3"),
            (std::vector<Row>{Row({Value(0), Value(0)})}));
  EXPECT_TRUE(RelationalRun(*rs_,
                            "SELECT k,COUNT(DISTINCT v) FROM DistinctCodes "
                            "WHERE k<0 GROUP BY k LIMIT 3")
                  .empty());
  const std::string snapshot_query =
      "SELECT k,COUNT(DISTINCT v) d FROM DistinctCodes "
      "GROUP BY k ORDER BY d DESC,k NULLS FIRST LIMIT 7";
  const auto expected = RelationalRun(*rs_, snapshot_query);
  auto old = rs_->BeginReadOnlyContext();
  RelationalRun(*rs_, "INSERT INTO DistinctCodes VALUES (999999999999999,0,0)");
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(QueryResult, old_result,
                        engine.Execute(old, snapshot_query));
  EXPECT_EQ(old_result.Collect(), expected);
  ASSERT_SUCCESS(old_result.GetStatus());
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(DISTINCT v) FROM DistinctCodes "
                          "WHERE k=0 GROUP BY k LIMIT 3"),
            (std::vector<Row>{Row({Value(0), Value(1)})}));
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest,
       DictionaryDistinctWideCodesAndWorkspaceFallback) {
  RelationalRun(*rs_, "CREATE TABLE WideDistinctCodes(k INT64,v INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("WideDistinctCodes"));
    for (int64_t i = 0; i < 180000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_, Row({Value((i % 4099) * 10000001),
                                        Value((i % 70001) * 1000000001)}))
              .GetStatus());
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
                          context.GetTable("WideDistinctCodes"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    EXPECT_EQ(store->dictionaries[0].wide_code_bytes, 2);
    EXPECT_EQ(store->dictionaries[1].wide_code_bytes, 4);
    const size_t words = (store->dictionaries[1].int_entries.size() + 63) / 64;
    const size_t bytes = (store->dictionaries[0].int_entries.size() + 1) *
                         words * sizeof(uint64_t);
    EXPECT_LT(bytes, size_t{64} * 1024 * 1024);
    EXPECT_GT(bytes * 2, size_t{64} * 1024 * 1024);
    ASSERT_SUCCESS(context.PreCommit());
  }
  const std::string prefix =
      "SELECT k,COUNT(DISTINCT v) d FROM WideDistinctCodes GROUP BY k";
  const std::string tail = " ORDER BY d DESC,k LIMIT 23 OFFSET 5";
  const auto expected =
      RelationalRun(*rs_, prefix + ",CAST(k AS STRING)" + tail);
  // A four-byte argument code works with one worker. With two workers the
  // exact bitmap product exceeds the cap and sparse code pairs are used.
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    EXPECT_EQ(RelationalRun(*rs_, prefix + tail), expected);
  }
  const size_t old_limit = QueryMemoryBudget::Global().Limit();
  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget parallelism(&active, 1);
  QueryMemoryBudget::Global().ResetForTest(size_t{1024} * 1024);
  const auto constrained = RelationalRun(*rs_, prefix + tail);
  QueryMemoryBudget::Global().ResetForTest(old_limit);
  EXPECT_EQ(constrained, expected);
}

TEST_F(RelationalRegressionTest,
       SparseDictionaryDistinctPairsDeduplicateAcrossWorkers) {
  RelationalRun(*rs_, "CREATE TABLE SparseDistinct(k STRING,v INT64,f INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("SparseDistinct"));
    for (int64_t i = 0; i < 180000; ++i) {
      // The second half repeats every pair across worker boundaries. The
      // Cartesian dictionary domain exceeds 64 MiB even with one worker.
      const int64_t t = i % 90000;
      const int64_t key = t % 10007;
      ASSERT_SUCCESS(
          table
              ->Insert(
                  setup.txn_,
                  Row({key == 1 ? Value() : Value("key_" + std::to_string(key)),
                       key == 0 || t % 17 == 0
                           ? Value()
                           : Value((t % 70001) * 1000000001),
                       Value(t % 7)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const std::string prefix =
      "SELECT k,COUNT(DISTINCT v) d,COUNT(DISTINCT v) again,COUNT(*) c "
      "FROM SparseDistinct";
  const std::string tail =
      " ORDER BY d DESC,c DESC,k NULLS FIRST LIMIT 17 OFFSET 3";
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    for (const std::string_view filter : {"", " WHERE k<>'key_7' AND f<3"}) {
      std::string reference_query = prefix;
      reference_query.append(filter);
      reference_query += " GROUP BY k,CAST(k AS STRING)";
      reference_query += tail;
      const auto expected = RelationalRun(*rs_, reference_query);
      auto context = rs_->BeginReadOnlyContext();
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                            context.GetTable("SparseDistinct"));
      const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
      ASSERT_NE(store, nullptr);
      const size_t domain = store->dictionaries[0].entries.size() + 1;
      const size_t words =
          (store->dictionaries[1].int_entries.size() + 63) / 64;
      ASSERT_GT(domain * words * sizeof(uint64_t), size_t{64} * 1024 * 1024);
      const auto key = ColumnValueExp("k");
      const auto distinct = AggregateExpressionExp(AggregationType::kCount,
                                                   ColumnValueExp("v"), true);
      const auto count =
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
      Expression predicate;
      if (!filter.empty()) {
        predicate = BinaryExpressionExp(
            BinaryExpressionExp(key, BinaryOperation::kNotEquals,
                                ConstantValueExp(Value("key_7"))),
            BinaryOperation::kAnd,
            BinaryExpressionExp(ColumnValueExp("f"), BinaryOperation::kLessThan,
                                ConstantValueExp(Value(3))));
      }
      SelectStatement statement(
          {NamedExpression("k", key), NamedExpression("d", distinct),
           NamedExpression("again", distinct), NamedExpression("c", count)},
          {}, predicate);
      SelectSource source;
      source.table = "SparseDistinct";
      statement.SetSources({source});
      statement.SetGroupBy({key});
      statement.SetOrderBy({{distinct, false, std::nullopt},
                            {count, false, std::nullopt},
                            {key, true, true}});
      statement.SetLimit(17);
      statement.SetOffset(3);
      relational_detail::ExecutionRuntime runtime;
      context.set_execution_runtime(&runtime);
      ASSIGN_OR_ASSERT_FAIL(
          relational_detail::Relation, result,
          relational_detail::ExecuteQuery(context, statement, nullptr, {}));
      EXPECT_EQ(result.rows, expected);
      EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
      EXPECT_EQ(runtime.scan_values_decoded, 0U);
      if (workers == 4 && filter.empty()) {
        // The same query must keep the encoded path when its observed pairs
        // exceed the available staging buffer; duplicates cross disk runs
        // and worker boundaries. Restore the global budget on assertion exit.
        struct RestoreBudget {
          RestoreBudget() = default;
          RestoreBudget(const RestoreBudget&) = delete;
          RestoreBudget& operator=(const RestoreBudget&) = delete;
          RestoreBudget(RestoreBudget&&) = delete;
          RestoreBudget& operator=(RestoreBudget&&) = delete;
          size_t limit = QueryMemoryBudget::Global().Limit();
          ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
        } restore;
        QueryMemoryBudget::Global().ResetForTest(size_t{4} << 20);
        runtime = {};
        ASSIGN_OR_ASSERT_FAIL(
            relational_detail::Relation, spilled,
            relational_detail::ExecuteQuery(context, statement, nullptr, {}));
        EXPECT_EQ(spilled.rows, expected);
        EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
        EXPECT_EQ(runtime.scan_values_decoded, 0U);
      }
      ASSERT_SUCCESS(context.PreCommit());
    }
  }
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT k,COUNT(DISTINCT v) FROM SparseDistinct "
                          "WHERE k='key_0' GROUP BY k LIMIT 1"),
            (std::vector<Row>{Row({Value("key_0"), Value(0)})}));
  EXPECT_TRUE(
      RelationalRun(*rs_, prefix + " WHERE f<0 GROUP BY k LIMIT 3").empty());
  const auto expected =
      RelationalRun(*rs_, prefix + " GROUP BY k,CAST(k AS STRING)" + tail);
  const size_t old_limit = QueryMemoryBudget::Global().Limit();
  QueryMemoryBudget::Global().ResetForTest(size_t{1024} * 1024);
  const auto constrained = RelationalRun(*rs_, prefix + " GROUP BY k" + tail);
  QueryMemoryBudget::Global().ResetForTest(old_limit);
  EXPECT_EQ(constrained, expected);
  for (const std::string_view suffix :
       {" ORDER BY k NULLS FIRST LIMIT 1", " LIMIT 0"}) {
    EXPECT_EQ(RelationalRun(*rs_, prefix + " GROUP BY k" + std::string(suffix)),
              RelationalRun(*rs_, prefix + " GROUP BY k,CAST(k AS STRING)" +
                                      std::string(suffix)));
  }
  const std::string snapshot_query =
      prefix + " GROUP BY k ORDER BY k NULLS FIRST LIMIT 1";
  const auto before_insert = RelationalRun(*rs_, snapshot_query);
  auto old = rs_->BeginReadOnlyContext();
  RelationalRun(*rs_,
                "INSERT INTO SparseDistinct VALUES (NULL,999999999999999,0)");
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(QueryResult, old_result,
                        engine.Execute(old, snapshot_query));
  EXPECT_EQ(old_result.Collect(), before_insert);
  ASSERT_SUCCESS(old_result.GetStatus());
  EXPECT_NE(RelationalRun(*rs_, snapshot_query), before_insert);
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest,
       CompositeDictionaryNumericAggregatesKeepMergeAndFallbacks) {
  RelationalRun(*rs_,
                "CREATE TABLE CompositeNumbers(v INT64,k INT64,s INT64,"
                "w INT64,f STRING)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("CompositeNumbers"));
    for (int64_t i = 0; i < 24000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_,
                       Row({Value((i % 1009) - 504),
                            i % 19 == 0 ? Value() : Value((i % 307) * 10000001),
                            Value(i % 5), Value((i % 503) - 251),
                            Value(i % 7 == 0 ? "" : "keep")}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const auto key = ColumnValueExp("k");
  const auto text = ColumnValueExp("s");
  const auto sum =
      AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("v"));
  const auto avg =
      AggregateExpressionExp(AggregationType::kAvg, ColumnValueExp("w"));
  const auto count =
      AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
  SelectStatement statement(
      {NamedExpression("k", key), NamedExpression("s", text),
       NamedExpression("total", sum), NamedExpression("mean", avg),
       NamedExpression("c", count)},
      {}, nullptr);
  SelectSource source;
  source.table = "CompositeNumbers";
  statement.SetSources({source});
  statement.SetGroupBy({key, text});
  statement.SetOrderBy({{count, false, std::nullopt}});
  statement.SetLimit(13);
  statement.SetOffset(3);
  const std::string prefix =
      "SELECT k,s,SUM(v) total,AVG(w) mean,COUNT(*) c FROM CompositeNumbers";
  const auto query = [&](bool reference, std::string_view filter,
                         std::string_view tail) {
    return prefix + std::string(filter) +
           (reference ? " GROUP BY k,s,CAST(k AS STRING)" : " GROUP BY k,s") +
           std::string(tail);
  };
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("CompositeNumbers"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
    ASSERT_TRUE(store->dictionaries[2].active);
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    ASSIGN_OR_ASSERT_FAIL(
        relational_detail::Relation, result,
        relational_detail::ExecuteQuery(context, statement, nullptr, {}));
    EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
    EXPECT_EQ(runtime.scan_values_decoded, 0U);
    EXPECT_EQ(result.rows,
              RelationalRun(*rs_, query(true, "",
                                        " ORDER BY c DESC LIMIT 13 "
                                        "OFFSET 3")));
    ASSERT_SUCCESS(context.PreCommit());
    for (const std::string_view filter :
         {"", " WHERE f<>'' AND k>=0", " WHERE f='absent'"}) {
      for (const std::string_view tail :
           {" ORDER BY total DESC,k,s LIMIT 17 OFFSET 5",
            " ORDER BY mean,c DESC,k NULLS FIRST,s NULLS LAST LIMIT 11",
            " ORDER BY c DESC LIMIT 13 OFFSET 3", " LIMIT 0"}) {
        EXPECT_EQ(RelationalRun(*rs_, query(false, filter, tail)),
                  RelationalRun(*rs_, query(true, filter, tail)));
      }
    }
    EXPECT_EQ(RelationalRun(*rs_,
                            "SELECT k,s,AVG(CAST(w AS DOUBLE)),SUM(v),AVG(v) "
                            "FROM CompositeNumbers WHERE f<>'' GROUP BY k,s "
                            "ORDER BY SUM(w) DESC,k,s LIMIT 17"),
              RelationalRun(*rs_,
                            "SELECT k,s,AVG(CAST(w AS DOUBLE)),SUM(v),AVG(v) "
                            "FROM CompositeNumbers WHERE f<>'' GROUP BY "
                            "k,s,CAST(k AS STRING) "
                            "ORDER BY SUM(w) DESC,k,s LIMIT 17"));
  }
  const std::string tail = " ORDER BY c DESC,k,s LIMIT 11";
  const auto expected = RelationalRun(*rs_, query(true, "", tail));
  const size_t old_limit = QueryMemoryBudget::Global().Limit();
  QueryMemoryBudget::Global().ResetForTest(size_t{1024} * 1024);
  const auto constrained = RelationalRun(*rs_, query(false, "", tail));
  QueryMemoryBudget::Global().ResetForTest(old_limit);
  EXPECT_EQ(constrained, expected);
  auto old = rs_->BeginReadOnlyContext();
  RelationalRun(*rs_,
                "INSERT INTO CompositeNumbers VALUES (NULL,0,0,NULL,'keep')");
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(QueryResult, old_result,
                        engine.Execute(old, query(false, "", tail)));
  EXPECT_EQ(old_result.Collect(), expected);
  ASSERT_SUCCESS(old_result.GetStatus());
  EXPECT_EQ(RelationalRun(*rs_, query(false, "", tail)),
            RelationalRun(*rs_, query(true, "", tail)));
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(RelationalRegressionTest,
       DictionaryNumericAggregatesKeepBoundsAndRounding) {
  RelationalRun(*rs_, "CREATE TABLE NumericCodes(v INT64,k INT64,n INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("NumericCodes"));
    for (int64_t i = 0; i < 30000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(
                  setup.txn_,
                  Row({Value((i % 1009) - 504),
                       i % 19 == 0 ? Value() : Value((i % 307) * 10000001),
                       i % 13 == 0 ? Value() : Value((i % 1021) * 1000000001)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const auto key = ColumnValueExp("k");
  const auto sum =
      AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("v"));
  const auto avg =
      AggregateExpressionExp(AggregationType::kAvg, ColumnValueExp("v"));
  const auto count =
      AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
  const auto distinct = AggregateExpressionExp(AggregationType::kCount,
                                               ColumnValueExp("n"), true);
  SelectStatement statement(
      {NamedExpression("k", key), NamedExpression("s", sum),
       NamedExpression("a", avg), NamedExpression("c", count),
       NamedExpression("d", distinct)},
      {}, nullptr);
  SelectSource source;
  source.table = "NumericCodes";
  statement.SetSources({source});
  statement.SetGroupBy({key});
  statement.SetOrderBy({{avg, false, std::nullopt}, {sum, true, std::nullopt}});
  statement.SetLimit(11);
  const std::string prefix =
      "SELECT k,SUM(v) s,AVG(v) a,COUNT(*) c,COUNT(DISTINCT n) d FROM "
      "NumericCodes";
  for (const size_t workers : {size_t{1}, size_t{2}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget parallelism(&active, workers);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("NumericCodes"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    ASSIGN_OR_ASSERT_FAIL(
        relational_detail::Relation, result,
        relational_detail::ExecuteQuery(context, statement, nullptr, {}));
    EXPECT_EQ(runtime.typed_parallel_aggregations, 1U);
    EXPECT_EQ(runtime.scan_values_decoded, 0U);
    EXPECT_EQ(
        result.rows,
        RelationalRun(
            *rs_,
            prefix +
                " GROUP BY k,CAST(k AS STRING) ORDER BY a DESC,s LIMIT 11"));
    ASSERT_SUCCESS(context.PreCommit());
    for (const std::string_view filter : {"", " WHERE v<0 AND n<3000000003"}) {
      for (const std::string_view order :
           {" ORDER BY s DESC", " ORDER BY a,c DESC", ""}) {
        const std::string tail = std::string(order) + " LIMIT 13 OFFSET 3";
        std::string direct = prefix;
        direct += filter;
        direct += " GROUP BY k";
        std::string reference = direct;
        reference += ",CAST(k AS STRING)";
        direct += tail;
        reference += tail;
        EXPECT_EQ(RelationalRun(*rs_, direct), RelationalRun(*rs_, reference));
      }
    }
    EXPECT_EQ(RelationalRun(*rs_,
                            "SELECT k,AVG(CAST(v AS FLOAT64)),SUM(v),AVG(v) "
                            "FROM NumericCodes GROUP BY k ORDER BY "
                            "COUNT(DISTINCT n) DESC LIMIT 17"),
              RelationalRun(*rs_,
                            "SELECT k,AVG(CAST(v AS FLOAT64)),SUM(v),AVG(v) "
                            "FROM NumericCodes GROUP BY k,CAST(k AS STRING) "
                            "ORDER BY COUNT(DISTINCT n) DESC LIMIT 17"));
  }
  const std::string query =
      prefix + " GROUP BY k ORDER BY c DESC,k NULLS FIRST LIMIT 7";
  const auto expected = RelationalRun(*rs_, query);
  auto old = rs_->BeginReadOnlyContext();
  RelationalRun(*rs_, "INSERT INTO NumericCodes VALUES (NULL,0,0)");
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(QueryResult, old_result, engine.Execute(old, query));
  EXPECT_EQ(old_result.Collect(), expected);
  ASSERT_SUCCESS(old_result.GetStatus());
  EXPECT_EQ(RelationalRun(*rs_, query),
            RelationalRun(*rs_, prefix + " GROUP BY k,CAST(k AS STRING) ORDER "
                                         "BY c DESC,k NULLS FIRST LIMIT 7"));
  ASSERT_SUCCESS(old.PreCommit());
  // A fresh append-only table keeps the bounds checks independent of the
  // fallback for NULLs and in-place row rewrites exercised above.
  RelationalRun(*rs_, "CREATE TABLE NumericBounds(v INT64,k INT64,n INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("NumericBounds"));
    for (int64_t i = 0; i < 30000; ++i) {
      ASSERT_SUCCESS(
          table
              ->Insert(setup.txn_, Row({Value((i % 1009) - 504),
                                        Value((i % 307) * 10000001),
                                        Value((i % 1021) * 1000000001)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto require_clean_numeric_store = [&] {
    const auto gc_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (rs_->TransactionStats().version_chains != 0) {
      ASSERT_LT(std::chrono::steady_clock::now(), gc_deadline);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("NumericBounds"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(store->dictionaries[1].wide_code_bytes, 2);
    ASSERT_EQ(store->column_has_null[0], 0);
    ASSERT_SUCCESS(context.PreCommit());
  };
  // Promoting each argument to DOUBLE rounds away +1 before the sum. The
  // exact-integer shortcut must refuse these values rather than return 0.5.
  RelationalRun(
      *rs_,
      "INSERT INTO NumericBounds VALUES "
      "(9007199254740993,999999999999,0),(-9007199254740992,999999999999,0),"
      "(9223372036854775807,888888888888,0),(1,888888888888,0)");
  require_clean_numeric_store();
  EXPECT_EQ(RelationalRun(
                *rs_,
                "SELECT AVG(v),AVG(CAST(v AS FLOAT64)) "
                "FROM NumericBounds WHERE k=999999999999 GROUP BY k LIMIT 1"),
            (std::vector<Row>{Row({Value(0.0), Value(0.0)})}));
  EXPECT_EQ(RelationalRun(
                *rs_,
                "SELECT AVG(v),AVG(CAST(v AS FLOAT64)) "
                "FROM NumericBounds WHERE k=999999999999 GROUP BY k,n LIMIT 1"),
            (std::vector<Row>{Row({Value(0.0), Value(0.0)})}));
  // The overflowing group loses COUNT-based TopN, but must still fail.
  auto current = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(
      QueryResult, overflow,
      engine.Execute(current,
                     "SELECT k,SUM(v),COUNT(*) c,COUNT(DISTINCT n) "
                     "FROM NumericBounds GROUP BY k ORDER BY c DESC LIMIT 1"));
  static_cast<void>(overflow.Collect());
  EXPECT_EQ(overflow.GetStatus(), Status::kIsInfinity);
  ASSERT_SUCCESS(current.PreCommit());
  auto composite_context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(
      QueryResult, composite_overflow,
      engine.Execute(composite_context,
                     "SELECT k,n,SUM(v),COUNT(*) c FROM NumericBounds "
                     "GROUP BY k,n ORDER BY c DESC LIMIT 1"));
  static_cast<void>(composite_overflow.Collect());
  EXPECT_EQ(composite_overflow.GetStatus(), Status::kIsInfinity);
  ASSERT_SUCCESS(composite_context.PreCommit());
}

TEST_F(RelationalRegressionTest, MoreThanEightSlimAggregatesFoldCorrectly) {
  // The cell-streaming folds index a fixed std::array<Value, 8> scratch by
  // aggregate slot.  A slim plan with 9+ aggregates must take the generic
  // path instead of writing past the array (both the LSTable-direct fold
  // and the typed-fill fold_cell_row path).
  const Schema schema("ManySlimAggs", {Column("k", ValueType::kInt64),
                                       Column("a", ValueType::kInt64),
                                       Column("b", ValueType::kInt64)});
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(setup, schema));
    // >16384 rows makes the LSTable snapshot skip the relation cache and
    // select the direct column-store fold.
    for (int64_t i = 0; i < 20000; ++i) {
      ASSERT_SUCCESS(
          table.Insert(setup.txn_, Row({Value(i % 4), Value(i), Value(i * 2)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (rs_->TransactionStats().version_chains != 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const std::string wide =
      "SELECT k, COUNT(*), SUM(a), MIN(a), MAX(a), SUM(b), MIN(b), MAX(b),"
      " AVG(a), COUNT(DISTINCT a) FROM ManySlimAggs GROUP BY k ORDER BY k";
  const std::vector<Row> expected = {
      Row({Value(0), Value(5000), Value(49990000), Value(0), Value(19996),
           Value(99980000), Value(0), Value(39992), Value(9998.0),
           Value(5000)}),
      Row({Value(1), Value(5000), Value(49995000), Value(1), Value(19997),
           Value(99990000), Value(2), Value(39994), Value(9999.0),
           Value(5000)}),
      Row({Value(2), Value(5000), Value(50000000), Value(2), Value(19998),
           Value(100000000), Value(4), Value(39996), Value(10000.0),
           Value(5000)}),
      Row({Value(3), Value(5000), Value(50005000), Value(3), Value(19999),
           Value(100010000), Value(6), Value(39998), Value(10001.0),
           Value(5000)})};
  // Generic morsel fold before the column store exists.
  EXPECT_EQ(RelationalRun(*rs_, wide), expected);
  {
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("ManySlimAggs"));
    ASSERT_NE(GetOrBuildLSTableStore(context.txn_, *table, true), nullptr);
    ASSERT_SUCCESS(context.PreCommit());
  }
  // LSTable-direct fold over the column store.
  EXPECT_EQ(RelationalRun(*rs_, wide), expected);
}

// The flat columnar window path computes a dense slot span as
// uint64(max) - uint64(min) + 1. The full [INT64_MIN, INT64_MAX] range wraps
// that span to 0, which used to enter the dense path with a 0/1-slot array
// and write far out of bounds (heap corruption). It must fall back instead.
TEST_F(RelationalRegressionTest, ColumnarWindowFullInt64PartitionRange) {
  auto context = rs_->BeginContext();
  const Schema schema(
      "w", {Column("p", ValueType::kInt64), Column("k", ValueType::kInt64)});
  std::vector<Row> rows;
  rows.reserve(4096);
  for (int64_t i = 0; i < 4096; ++i) {
    const int64_t key = (i % 2 == 0) ? std::numeric_limits<int64_t>::min()
                                     : std::numeric_limits<int64_t>::max();
    rows.emplace_back(std::vector<Value>{Value(key), Value(i)});
  }
  auto table = relational_detail::BuildColumnarTableFromRows(schema, rows);
  ASSERT_TRUE(table.HasValue());
  relational_detail::Relation input;
  input.schema = schema;
  input.SetColumnarTable(table.MoveValue(), false);
  ASSERT_TRUE(input.HasColumnarOnly());

  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = "SUM";
  window->args = {ColumnValueExp(ColumnName("k"))};
  window->partition_by = {ColumnValueExp(ColumnName("p"))};
  const std::vector<const WindowFunctionCallExpression*> windows{window.get()};
  const std::vector<std::string> names{"$win0"};

  auto result = relational_detail::ExtendColumnarRelationWithWindows(
      context, std::move(input), windows, names, nullptr, {});
  ASSERT_TRUE(result.HasValue()) << result.GetStatus().GetMessage();
  EXPECT_EQ(result.Value().TotalRows(), rows.size());
  ASSERT_SUCCESS(context.PreCommit());
}

}  // namespace
}  // namespace tinylamb
