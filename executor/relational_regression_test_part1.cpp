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

}  // namespace
}  // namespace tinylamb
