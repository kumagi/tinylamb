/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "index/index_schema.hpp"
#include "plan/cascades.hpp"
#include "plan/implementation_rules.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
// Executes one statement and returns the materialized rows.  A prepare or
// execution failure surfaces as a runtime_error carrying the diagnostic.
std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    throw std::runtime_error(engine->LastError());
  }
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
    row = Row();
  }
  const Status st = prepared.Value()->GetStatus();
  if (st != Status::kSuccess) {
    throw std::runtime_error(st.GetMessage());
  }
  return rows;
}

// Prepares (and drains) an EXPLAIN statement, returning the plan text.  Used
// to confirm that a query shape reached the intended physical operator.
std::string ExplainSql(SqlEngine* engine, TransactionContext* ctx,
                       const std::string& sql) {
  StatusOr<Executor> prepared = engine->Prepare(*ctx, "EXPLAIN " + sql);
  if (!prepared.HasValue()) {
    throw std::runtime_error(engine->LastError());
  }
  std::string plan;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    if (!plan.empty()) {
      plan += '\n';
    }
    plan += row[0].AsString();
  }
  const Status st = prepared.Value()->GetStatus();
  if (st != Status::kSuccess) {
    throw std::runtime_error(st.GetMessage());
  }
  return plan;
}
}  // namespace

// Rich optimizer fixture: several tables with varied shapes (primary keys,
// single- and multi-column indexes, an unindexed table, a UNIQUE dimension)
// so SQL queries exercise implementation rules, memo exploration and costing
// branches.  Mirrors plan/optimizer_test.cpp conventions.
class CascadesRulesCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "GoogleSql frontend unavailable";
    }
    prefix_ = "cascades_rules_coverage-" + RandomString();
    database_ = Database::Create(prefix_).MoveValue();
    engine_ = std::make_unique<SqlEngine>(*database_);
    BuildSchema();
    // Begin the query transaction only after the fixture commits, so its
    // MVCC snapshot sees the loaded rows.
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  }

  void BuildSchema() {
    TransactionContext ctx = database_->BeginContext();
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          database_->CreateTable(
              ctx, Schema("Sc1", {Column("c1", ValueType::kInt64,
                                         Constraint(Constraint::kPrimaryKey)),
                                  Column("c2", ValueType::kVarChar),
                                  Column("c3", ValueType::kDouble)})));
      for (int i = 0; i < 100; ++i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_, Row({Value(i),
                                      Value("c2-" + std::to_string(i % 7)),
                                      Value(i + 9.9)}))
                .GetStatus());
      }
      ASSERT_SUCCESS(database_->CreateIndex(ctx, "Sc1", IndexSchema("Sc1PK", {0})));
      ASSERT_SUCCESS(
          database_->CreateIndex(ctx, "Sc1", IndexSchema("KeyIdx", {1, 2})));
    }
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          database_->CreateTable(
              ctx, Schema("Sc2", {Column("d1", ValueType::kInt64,
                                         Constraint(Constraint::kPrimaryKey)),
                                  Column("d2", ValueType::kDouble),
                                  Column("d3", ValueType::kVarChar),
                                  Column("d4", ValueType::kInt64)})));
      for (int i = 0; i < 200; ++i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_, Row({Value(i), Value(i + 0.2),
                                      Value("d3-" + std::to_string(i % 10)),
                                      Value(i % 4)}))
                .GetStatus());
      }
      ASSERT_SUCCESS(database_->CreateIndex(ctx, "Sc2", IndexSchema("Sc2PK", {0})));
      ASSERT_SUCCESS(database_->CreateIndex(
          ctx, "Sc2",
          IndexSchema("NameIdx", {2, 3}, {0, 1}, IndexMode::kNonUnique)));
    }
    {
      // No primary key / no index: full-scan-only relation.
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          database_->CreateTable(
              ctx, Schema("Sc3", {Column("e1", ValueType::kInt64),
                                  Column("e2", ValueType::kDouble)})));
      for (int i = 20; i > 0; --i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_, Row({Value(i), Value(i + 53.4)}))
                .GetStatus());
      }
    }
    {
      // Single-column non-unique index for skip-scan / bitmap shapes.
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          database_->CreateTable(
              ctx, Schema("Sc4", {Column("f1", ValueType::kInt64),
                                  Column("f2", ValueType::kVarChar)})));
      for (int i = 0; i < 100; ++i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_, Row({Value(i), Value(std::to_string(i % 4))}))
                .GetStatus());
      }
      ASSERT_SUCCESS(database_->CreateIndex(
          ctx, "Sc4", IndexSchema("Sc4_IDX", {1}, {}, IndexMode::kNonUnique)));
    }
    {
      // UNIQUE (non-PK) dimension for join-elimination / semi-join rewrites.
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          database_->CreateTable(
              ctx, Schema("Dim", {Column("k", ValueType::kInt64,
                                         Constraint(Constraint::kUnique)),
                                  Column("v", ValueType::kVarChar)})));
      for (int i = 0; i < 10; ++i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_, Row({Value(i), Value("dim" + std::to_string(i))}))
                .GetStatus());
      }
      ASSERT_SUCCESS(database_->CreateIndex(ctx, "Dim", IndexSchema("DimK", {0})));
    }
    {
      // Fact table referencing Dim keys, including values outside Dim and a
      // NULL (unmatched rows for outer/anti join shapes).
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          database_->CreateTable(
              ctx, Schema("Fact", {Column("fk", ValueType::kInt64),
                                   Column("payload", ValueType::kInt64)})));
      for (int i = 0; i < 12; ++i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_, Row({Value(i % 13), Value(i)}))
                .GetStatus());
      }
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());

    auto stat_tx = database_->BeginContext();
    for (const char* name : {"Sc1", "Sc2", "Sc3", "Sc4", "Dim", "Fact"}) {
      database_->RefreshStatistics(stat_tx, name);
    }
    ASSERT_SUCCESS(stat_tx.PreCommit());
  }

  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  std::vector<Row> Run(const std::string& sql) {
    return RunSql(engine_.get(), context_.get(), sql);
  }
  std::string Explain(const std::string& sql) {
    return ExplainSql(engine_.get(), context_.get(), sql);
  }

  std::string prefix_;
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

// ---------------------------------------------------------------------------
// Join enumeration, associativity, commutativity
// ---------------------------------------------------------------------------

TEST_F(CascadesRulesCoverageTest, TwoTableEquiJoinPlansAndExecutes) {
  const auto rows = Run("SELECT sc1.c1, sc2.d4 FROM sc1 JOIN sc2 "
                        "ON sc1.c1 = sc2.d1 WHERE sc1.c1 < 10");
  EXPECT_EQ(rows.size(), 10U);
}

TEST_F(CascadesRulesCoverageTest, ThreeTableChainJoin) {
  const auto rows = Run(
      "SELECT sc1.c1 FROM sc1 JOIN sc2 ON sc1.c1 = sc2.d1 "
      "JOIN dim ON sc2.d4 = dim.k WHERE sc1.c1 < 8");
  EXPECT_EQ(rows.size(), 8U);
}

TEST_F(CascadesRulesCoverageTest, FourTableJoinEnumeratesOrders) {
  const auto rows = Run(
      "SELECT sc1.c1 FROM sc1, sc2, dim, fact "
      "WHERE sc1.c1 = sc2.d1 AND sc2.d4 = dim.k AND dim.k = fact.fk "
      "AND sc1.c1 < 4");
  EXPECT_EQ(rows.size(), 4U);
}

TEST_F(CascadesRulesCoverageTest, FiveTableJoinPlans) {
  const auto rows = Run(
      "SELECT sc1.c1 FROM sc1 JOIN sc2 ON sc1.c1 = sc2.d1 "
      "JOIN dim ON sc2.d4 = dim.k JOIN fact ON dim.k = fact.fk "
      "JOIN sc3 ON fact.payload = sc3.e1 LIMIT 3");
  EXPECT_LE(rows.size(), 3U);
}

TEST_F(CascadesRulesCoverageTest, CommaJoinWithEqualityBecomesInnerJoin) {
  const auto rows = Run("SELECT sc1.c1 FROM sc1, sc2 "
                        "WHERE sc1.c1 = sc2.d1 AND sc1.c1 < 5");
  EXPECT_EQ(rows.size(), 5U);
}

TEST_F(CascadesRulesCoverageTest, CommaJoinWithInequalityStaysCartesian) {
  // Non-equi comma join: batch nested loop / product paths, no hash join.
  const auto rows = Run("SELECT sc1.c1, sc3.e1 FROM sc1, sc3 "
                        "WHERE sc1.c1 < sc3.e1 AND sc1.c1 < 3");
  EXPECT_FALSE(rows.empty());
}

TEST_F(CascadesRulesCoverageTest, AliasedSelfJoin) {
  const auto rows = Run(
      "SELECT a.c1 FROM sc1 a JOIN sc1 b ON a.c1 = b.c1 "
      "WHERE a.c1 < 5 AND b.c3 > 10.0");
  // c1=0 carries c3=9.9, which fails b.c3 > 10.
  EXPECT_EQ(rows.size(), 4U);
}

TEST_F(CascadesRulesCoverageTest, SelfJoinEliminationOnUniqueKey) {
  // dim.k is UNIQUE: the self join is the identity and may be eliminated.
  const auto rows = Run("SELECT a.k FROM dim a JOIN dim b ON a.k = b.k");
  EXPECT_EQ(rows.size(), 10U);
}

TEST_F(CascadesRulesCoverageTest, RightJoinNormalizesToLeft) {
  const auto rows = Run("SELECT sc1.c1, sc3.e1 FROM sc1 RIGHT JOIN sc3 "
                        "ON sc1.c1 = sc3.e1 WHERE sc3.e1 <= 5");
  EXPECT_EQ(rows.size(), 5U);
}

TEST_F(CascadesRulesCoverageTest, FullOuterJoinPlans) {
  const auto rows = Run("SELECT sc1.c1, fact.fk FROM sc1 FULL JOIN fact "
                        "ON sc1.c1 = fact.fk WHERE sc1.c1 < 3 OR fact.fk > 11");
  EXPECT_GE(rows.size(), 2U);
}

TEST_F(CascadesRulesCoverageTest, LeftJoinNonEquiUsesNestedLoop) {
  const auto rows = Run("SELECT sc1.c1, sc3.e1 FROM sc1 LEFT JOIN sc3 "
                        "ON sc1.c1 < sc3.e1 WHERE sc1.c1 < 2");
  EXPECT_GE(rows.size(), 2U);
}

TEST_F(CascadesRulesCoverageTest, LeftJoinTruePredicatePadsAllRows) {
  const auto rows = Run("SELECT sc1.c1 FROM sc1 LEFT JOIN sc3 ON 1 = 1 "
                        "WHERE sc1.c1 < 2");
  EXPECT_GT(rows.size(), 2U);
}

TEST_F(CascadesRulesCoverageTest, LeftJoinFalsePredicateKeepsLeftRows) {
  const auto rows = Run("SELECT sc1.c1, sc3.e1 FROM sc1 LEFT JOIN sc3 ON 1 = 2 "
                        "WHERE sc1.c1 < 3");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][1], Value());
}

TEST_F(CascadesRulesCoverageTest, LeftJoinFilterOnLeftSidePushesBelow) {
  // WHERE on the preserved side of a LEFT join pushes into the scan.
  const auto rows = Run("SELECT sc1.c1, fact.fk FROM sc1 LEFT JOIN fact "
                        "ON sc1.c1 = fact.fk WHERE sc1.c1 > 95");
  EXPECT_EQ(rows.size(), 4U);
}

TEST_F(CascadesRulesCoverageTest, LeftJoinFilterOnRightSideStaysAbove) {
  // WHERE on the null-supplying side must NOT push below the join.
  const auto rows = Run("SELECT sc1.c1 FROM sc1 LEFT JOIN fact "
                        "ON sc1.c1 = fact.fk WHERE fact.payload > 5");
  EXPECT_EQ(rows.size(), 6U);
}

TEST_F(CascadesRulesCoverageTest, OuterJoinSliceEliminatesUnusedUniqueRight) {
  // Only Dim's unique key is referenced; Fact is dropped from the plan.
  const auto rows = Run(
      "SELECT fact.fk, dim.k FROM fact LEFT JOIN dim ON fact.fk = dim.k "
      "WHERE fact.fk < 3");
  EXPECT_EQ(rows.size(), 3U);
}

TEST_F(CascadesRulesCoverageTest, SemiJoinViaInSubquery) {
  const auto rows = Run("SELECT sc1.c1 FROM sc1 WHERE sc1.c1 IN "
                        "(SELECT fact.fk FROM fact) AND sc1.c1 < 12");
  EXPECT_EQ(rows.size(), 12U);
}

TEST_F(CascadesRulesCoverageTest, AntiJoinViaNotInSubquery) {
  const auto rows = Run("SELECT sc1.c1 FROM sc1 WHERE sc1.c1 NOT IN "
                        "(SELECT fact.fk FROM fact) AND sc1.c1 < 20");
  EXPECT_EQ(rows.size(), 8U);
}

TEST_F(CascadesRulesCoverageTest, SemiJoinViaExists) {
  const auto rows = Run("SELECT sc1.c1 FROM sc1 WHERE EXISTS "
                        "(SELECT 1 FROM fact WHERE fact.fk = sc1.c1) "
                        "AND sc1.c1 < 12");
  EXPECT_EQ(rows.size(), 12U);
}

TEST_F(CascadesRulesCoverageTest, AntiJoinViaNotExists) {
  const auto rows = Run("SELECT sc1.c1 FROM sc1 WHERE NOT EXISTS "
                        "(SELECT 1 FROM fact WHERE fact.fk = sc1.c1) "
                        "AND sc1.c1 < 20");
  EXPECT_EQ(rows.size(), 8U);
}

TEST_F(CascadesRulesCoverageTest, SemiJoinPushesThroughInnerJoin) {
  // The semi join's leftover conjuncts move across the inner join edge.
  const auto rows = Run(
      "SELECT sc1.c1 FROM sc1 JOIN sc2 ON sc1.c1 = sc2.d1 WHERE sc1.c1 IN "
      "(SELECT fact.fk FROM fact) AND sc1.c1 < 6");
  EXPECT_EQ(rows.size(), 6U);
}

TEST_F(CascadesRulesCoverageTest, InLiteralListUsesPointLookup) {
  const auto rows = Run("SELECT sc1.c1 FROM sc1 WHERE sc1.c1 IN (1, 3, 5)");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
}

TEST_F(CascadesRulesCoverageTest, ExplainSemiJoinMentionsJoinOperator) {
  const std::string plan =
      Explain("SELECT sc1.c1 FROM sc1 WHERE EXISTS "
              "(SELECT 1 FROM fact WHERE fact.fk = sc1.c1)");
  EXPECT_FALSE(plan.empty());
}

// ---------------------------------------------------------------------------
// Aggregation family
// ---------------------------------------------------------------------------

TEST_F(CascadesRulesCoverageTest, CountStarUsesConstantTableRewrite) {
  const auto rows = Run("SELECT COUNT(*) FROM sc1");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{100}));
}

TEST_F(CascadesRulesCoverageTest, CountNonNullColumnRewritesToCountStar) {
  const auto rows = Run("SELECT COUNT(c1) FROM sc1");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{100}));
}

TEST_F(CascadesRulesCoverageTest, GroupByWithHavingOnGroupingKey) {
  const auto rows = Run("SELECT d4, COUNT(*) FROM sc2 GROUP BY d4 "
                        "HAVING d4 >= 2 ORDER BY d4");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(int64_t{2}));
}

TEST_F(CascadesRulesCoverageTest, GroupByWithResidualHaving) {
  const auto rows = Run("SELECT d4, COUNT(*) AS n FROM sc2 GROUP BY d4 "
                        "HAVING COUNT(*) > 10 ORDER BY d4");
  EXPECT_EQ(rows.size(), 4U);
}

TEST_F(CascadesRulesCoverageTest, ScalarAggregatesWrapInMax1Row) {
  const auto rows = Run(
      "SELECT SUM(c3), MIN(c1), MAX(c1), COUNT(*) FROM sc1 WHERE c1 < 10");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][3], Value(int64_t{10}));
}

TEST_F(CascadesRulesCoverageTest, AggregateOverJoinWithUniqueSide) {
  // sc1.c1 is unique: aggregation may be pushed below the join.
  const auto rows = Run(
      "SELECT sc1.c2, SUM(sc2.d2) FROM sc1 JOIN sc2 ON sc1.c1 = sc2.d1 "
      "GROUP BY sc1.c2 ORDER BY sc1.c2 LIMIT 4");
  EXPECT_EQ(rows.size(), 4U);
}

TEST_F(CascadesRulesCoverageTest, DistinctOverJoinWithUniqueKey) {
  const auto rows = Run("SELECT DISTINCT sc1.c1 FROM sc1 JOIN fact "
                        "ON sc1.c1 = fact.fk WHERE sc1.c1 < 5");
  EXPECT_EQ(rows.size(), 5U);
}

TEST_F(CascadesRulesCoverageTest, DistinctSingleIndexedColumnSkipScan) {
  const auto rows = Run("SELECT DISTINCT f2 FROM sc4 ORDER BY f2");
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0][0], Value("0"));
}

TEST_F(CascadesRulesCoverageTest, FilterBelowDistinctRewrite) {
  const auto rows = Run("SELECT DISTINCT f2 FROM sc4 WHERE f1 < 10");
  EXPECT_EQ(rows.size(), 4U);
}

TEST_F(CascadesRulesCoverageTest, AggregateJoinTransposeShape) {
  // 1:N join from unique side aggregated after the join.
  const auto rows = Run(
      "SELECT dim.k, COUNT(sc1.c1) FROM dim JOIN sc1 ON dim.k = sc1.c1 "
      "GROUP BY dim.k ORDER BY dim.k LIMIT 3");
  EXPECT_EQ(rows.size(), 3U);
}

// ---------------------------------------------------------------------------
// LIMIT / ORDER BY / TopN
// ---------------------------------------------------------------------------

TEST_F(CascadesRulesCoverageTest, UnorderedLimitCapsScan) {
  const auto rows = Run("SELECT c1 FROM sc1 LIMIT 5");
  EXPECT_EQ(rows.size(), 5U);
}

TEST_F(CascadesRulesCoverageTest, OrderedLimitUsesTopN) {
  const auto rows = Run("SELECT e1 FROM sc3 ORDER BY e1 LIMIT 4");
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
}

TEST_F(CascadesRulesCoverageTest, OrderedLimitDescending) {
  const auto rows = Run("SELECT e1 FROM sc3 ORDER BY e1 DESC LIMIT 3");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(int64_t{20}));
}

TEST_F(CascadesRulesCoverageTest, LimitWithOffsetOverIndexOrder) {
  const auto rows = Run("SELECT c1 FROM sc1 ORDER BY c1 LIMIT 5 OFFSET 90");
  ASSERT_EQ(rows.size(), 5U);
  EXPECT_EQ(rows[0][0], Value(int64_t{90}));
}

TEST_F(CascadesRulesCoverageTest, LimitOverUnionAllCapsBranches) {
  const auto rows = Run(
      "SELECT c1 FROM sc1 WHERE c1 < 50 UNION ALL "
      "SELECT d1 FROM sc2 WHERE d1 < 50 ORDER BY 1 LIMIT 7");
  ASSERT_EQ(rows.size(), 7U);
  EXPECT_EQ(rows[0][0], Value(int64_t{0}));
}

TEST_F(CascadesRulesCoverageTest, NestedSortsCollapse) {
  const auto rows = Run(
      "SELECT c1 FROM (SELECT c1, c2 FROM sc1 ORDER BY c1) t "
      "ORDER BY c1 LIMIT 4");
  ASSERT_EQ(rows.size(), 4U);
}

TEST_F(CascadesRulesCoverageTest, OrderByRedundantDuplicateKey) {
  const auto rows =
      Run("SELECT c1 FROM sc1 ORDER BY c1, c1 LIMIT 3");
  ASSERT_EQ(rows.size(), 3U);
}

// ---------------------------------------------------------------------------
// Set operations
// ---------------------------------------------------------------------------

TEST_F(CascadesRulesCoverageTest, UnionDistinctRemovesDuplicates) {
  const auto rows = Run("SELECT c1 FROM sc1 WHERE c1 < 5 "
                        "UNION DISTINCT SELECT d1 FROM sc2 WHERE d1 < 5");
  EXPECT_EQ(rows.size(), 5U);
}

TEST_F(CascadesRulesCoverageTest, UnionAllKeepsDuplicates) {
  const auto rows = Run("SELECT c1 FROM sc1 WHERE c1 < 3 "
                        "UNION ALL SELECT d1 FROM sc2 WHERE d1 < 3");
  EXPECT_EQ(rows.size(), 6U);
}

TEST_F(CascadesRulesCoverageTest, IntersectPlans) {
  const auto rows = Run("SELECT c1 FROM sc1 WHERE c1 < 6 "
                        "INTERSECT DISTINCT SELECT d1 FROM sc2 WHERE d1 > 2");
  EXPECT_EQ(rows.size(), 3U);
}

TEST_F(CascadesRulesCoverageTest, ExceptPlans) {
  const auto rows = Run("SELECT c1 FROM sc1 WHERE c1 < 6 "
                        "EXCEPT DISTINCT SELECT d1 FROM sc2 WHERE d1 > 2");
  EXPECT_EQ(rows.size(), 3U);
}

TEST_F(CascadesRulesCoverageTest, IntersectAllPlans) {
  const auto rows = Run(
      "SELECT c1 FROM sc1 WHERE c1 < 4 INTERSECT ALL "
      "SELECT c1 FROM sc1 WHERE c1 > 1");
  EXPECT_EQ(rows.size(), 2U);
}

TEST_F(CascadesRulesCoverageTest, ExceptAllPlans) {
  const auto rows = Run("SELECT c1 FROM sc1 WHERE c1 < 4 "
                        "EXCEPT ALL SELECT c1 FROM sc1 WHERE c1 > 1");
  EXPECT_EQ(rows.size(), 2U);
}

TEST_F(CascadesRulesCoverageTest, FilterPushedIntoEveryUnionBranch) {
  const auto rows = Run(
      "SELECT u.c1 FROM (SELECT c1 FROM sc1 UNION DISTINCT SELECT d1 "
      "FROM sc2) u WHERE u.c1 < 2 ORDER BY 1");
  EXPECT_EQ(rows.size(), 2U);
}

TEST_F(CascadesRulesCoverageTest, ProjectionPushedThroughUnion) {
  const auto rows = Run("SELECT c1 + 100 AS x FROM (SELECT c1 FROM sc1 "
                        "WHERE c1 < 2 UNION DISTINCT SELECT d1 FROM sc2 WHERE d1 < 2) t "
                        "ORDER BY x");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(int64_t{100}));
}

TEST_F(CascadesRulesCoverageTest, ExceptWithNullSafeComparison) {
  // EXCEPT must treat NULL rows as equal (IS NOT DISTINCT FROM lowering).
  const auto rows = Run(
      "SELECT fk FROM fact WHERE payload >= 12 EXCEPT DISTINCT "
      "SELECT fk FROM fact WHERE payload >= 12");
  EXPECT_EQ(rows.size(), 0U);
}

// ---------------------------------------------------------------------------
// Window functions
// ---------------------------------------------------------------------------

TEST_F(CascadesRulesCoverageTest, WindowRankPlans) {
  const auto rows = Run("SELECT c1, RANK() OVER (ORDER BY c3) FROM sc1 "
                        "WHERE c1 < 5");
  EXPECT_EQ(rows.size(), 5U);
}

TEST_F(CascadesRulesCoverageTest, WindowPartitionSharesSort) {
  const auto rows = Run(
      "SELECT c1, SUM(c3) OVER (PARTITION BY c2 ORDER BY c1 "
      "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s "
      "FROM sc1 WHERE c1 < 8");
  EXPECT_EQ(rows.size(), 8U);
}

TEST_F(CascadesRulesCoverageTest, RowNumberFilterBecomesTopN) {
  const auto rows = Run(
      "SELECT c1 FROM (SELECT c1, ROW_NUMBER() OVER (ORDER BY c1) AS rn "
      "FROM sc1) t WHERE rn <= 5");
  ASSERT_EQ(rows.size(), 5U);
  EXPECT_EQ(rows[4][0], Value(int64_t{4}));
}

TEST_F(CascadesRulesCoverageTest, TwoWindowsShareOneSort) {
  const auto rows = Run(
      "SELECT c1, RANK() OVER (ORDER BY c3) AS r, "
      "ROW_NUMBER() OVER (ORDER BY c3) AS n FROM sc1 WHERE c1 < 6");
  EXPECT_EQ(rows.size(), 6U);
}

// ---------------------------------------------------------------------------
// Correlated subqueries / apply family
// ---------------------------------------------------------------------------

TEST_F(CascadesRulesCoverageTest, CorrelatedAggregateSubquery) {
  const auto rows = Run(
      "SELECT c1 FROM sc1 WHERE c1 < 5 AND c3 > "
      "(SELECT AVG(e2) FROM sc3 WHERE sc3.e1 = sc1.c1) - 1000.0");
  // sc3 has no e1=0, so c1=0's correlated AVG is NULL and drops out.
  EXPECT_EQ(rows.size(), 4U);
}

TEST_F(CascadesRulesCoverageTest, CorrelatedCountSubquery) {
  const auto rows = Run(
      "SELECT c1, (SELECT COUNT(*) FROM fact WHERE fact.fk = sc1.c1) AS n "
      "FROM sc1 WHERE c1 < 6 ORDER BY c1");
  ASSERT_EQ(rows.size(), 6U);
  EXPECT_EQ(rows[0][1], Value(int64_t{1}));
}

TEST_F(CascadesRulesCoverageTest, ComplexCorrelationStaysApply) {
  // The correlation uses an expression, not a plain column equality.
  const auto rows = Run(
      "SELECT c1 FROM sc1 WHERE c1 < 3 AND c3 > "
      "(SELECT MAX(e2) FROM sc3 WHERE sc3.e1 * 2 = sc1.c1 + 100) - 100000.0");
  // e1 <= 20 can never satisfy e1*2 = c1+100, so every row sees NULL.
  EXPECT_EQ(rows.size(), 0U);
}

TEST_F(CascadesRulesCoverageTest, UncorrelatedScalarSubqueryMax1Row) {
  const auto rows = Run(
      "SELECT c1 FROM sc1 WHERE c1 < 3 AND c3 > (SELECT AVG(e2) FROM sc3) "
      "- 10000.0");
  EXPECT_EQ(rows.size(), 3U);
}

TEST_F(CascadesRulesCoverageTest, ExistsWithResidualInnerPredicate) {
  const auto rows = Run(
      "SELECT c1 FROM sc1 WHERE EXISTS (SELECT 1 FROM sc2 WHERE sc2.d1 = "
      "sc1.c1 AND sc2.d4 = 3) AND sc1.c1 < 20");
  EXPECT_EQ(rows.size(), 5U);
}

// ---------------------------------------------------------------------------
// CTEs, derived tables, values
// ---------------------------------------------------------------------------

TEST_F(CascadesRulesCoverageTest, NonRecursiveCteInlines) {
  const auto rows = Run(
      "WITH t AS (SELECT c1 FROM sc1 WHERE c1 < 10) "
      "SELECT c1 FROM t WHERE c1 > 7");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[1][0], Value(int64_t{9}));
}

TEST_F(CascadesRulesCoverageTest, RecursiveCtePlans) {
  const auto rows = Run(
      "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r "
      "WHERE n < 6) SELECT SUM(n) FROM r");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{21}));
}

TEST_F(CascadesRulesCoverageTest, DerivedTableWithFilterAndProjection) {
  const auto rows = Run(
      "SELECT t.c1 FROM (SELECT c1, c3 FROM sc1 WHERE c3 > 50.0) t "
      "WHERE t.c1 < 3");
  EXPECT_EQ(rows.size(), 0U);
}

TEST_F(CascadesRulesCoverageTest, NestedDerivedProjectionsMerge) {
  const auto rows = Run(
      "SELECT b.c1 FROM (SELECT a.c1 FROM (SELECT c1, c2 FROM sc1) a) b "
      "WHERE b.c1 < 4");
  EXPECT_EQ(rows.size(), 4U);
}

TEST_F(CascadesRulesCoverageTest, ValuesLeafPlans) {
  const auto rows = Run(
      "SELECT v.x FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL "
      "SELECT 3) v WHERE v.x < 3");
  EXPECT_EQ(rows.size(), 2U);
}

// ---------------------------------------------------------------------------
// Index access paths / scan filters
// ---------------------------------------------------------------------------

TEST_F(CascadesRulesCoverageTest, EqualityUsesPrimaryKeyIndex) {
  const auto rows = Run("SELECT c2 FROM sc1 WHERE c1 = 42");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value("c2-0"));
}

TEST_F(CascadesRulesCoverageTest, RangePredicateUsesIndexBounds) {
  const auto rows = Run("SELECT c1 FROM sc1 WHERE c1 > 95 AND c1 < 98");
  EXPECT_EQ(rows.size(), 2U);
}

TEST_F(CascadesRulesCoverageTest, LikePrefixUsesIndexRange) {
  const auto rows = Run("SELECT d1 FROM sc2 WHERE d3 LIKE 'd3-1%' AND d1 < 30");
  EXPECT_EQ(rows.size(), 3U);
}

TEST_F(CascadesRulesCoverageTest, CompositeIndexEqualityPrefix) {
  const auto rows =
      Run("SELECT c1 FROM sc1 WHERE c2 = 'c2-3' AND c3 > 60.0 AND c3 < 62.0");
  EXPECT_EQ(rows.size(), 1U);
}

TEST_F(CascadesRulesCoverageTest, DisjunctionOverIndexedColumnUsesBitmap) {
  const auto rows =
      Run("SELECT f1 FROM sc4 WHERE f2 = '1' OR f2 = '2' ORDER BY f1 LIMIT 6");
  EXPECT_EQ(rows.size(), 6U);
}

TEST_F(CascadesRulesCoverageTest, NullPredicateOnUnindexedColumn) {
  const auto rows = Run("SELECT e1 FROM sc3 WHERE e1 IS NULL");
  EXPECT_EQ(rows.size(), 0U);
}

TEST_F(CascadesRulesCoverageTest, IsNotNotNullPredicateCombinedWithRange) {
  const auto rows = Run(
      "SELECT e1 FROM sc3 WHERE e1 IS NOT NULL AND e1 > 15 ORDER BY e1");
  EXPECT_EQ(rows.size(), 5U);
}

TEST_F(CascadesRulesCoverageTest, ContradictoryConjunctsProduceEmptyPlan) {
  const auto rows = Run("SELECT c1 FROM sc1 WHERE c1 < 3 AND c1 > 7");
  EXPECT_EQ(rows.size(), 0U);
}

TEST_F(CascadesRulesCoverageTest, TransitiveEqualityInferredAcrossJoin) {
  // sc1.c1 = sc2.d1 AND sc1.c1 < 5 infers sc2.d1 < 5 for the right scan.
  const auto rows = Run(
      "SELECT sc2.d3 FROM sc1 JOIN sc2 ON sc1.c1 = sc2.d1 WHERE sc1.c1 < 2");
  EXPECT_EQ(rows.size(), 2U);
}

TEST_F(CascadesRulesCoverageTest, OrDisjunctionStaysResidual) {
  const auto rows =
      Run("SELECT c1 FROM sc1 WHERE c1 = 1 OR c1 = 97 OR c2 = 'c2-5'");
  EXPECT_GE(rows.size(), 2U);
}

TEST_F(CascadesRulesCoverageTest, CaseExpressionInProjection) {
  const auto rows = Run(
      "SELECT CASE WHEN c1 < 5 THEN 'low' ELSE 'high' END FROM sc1 "
      "WHERE c1 < 2");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value("low"));
}

TEST_F(CascadesRulesCoverageTest, CastInPredicateStaysResidual) {
  const auto rows = Run("SELECT c1 FROM sc1 WHERE CAST(c1 AS DOUBLE) < 2.5");
  EXPECT_EQ(rows.size(), 3U);
}

// ---------------------------------------------------------------------------
// Direct memo API: rule pattern-match failure branches
// ---------------------------------------------------------------------------

namespace {

using cascades::GroupId;
using cascades::LogicalExpression;
using cascades::LogicalOperator;
using cascades::Memo;
using cascades::Rule;
using cascades::RuleSet;
using cascades::SearchEngine;

Expression EqExp(std::string_view column, const Value& value) {
  return BinaryExpressionExp(ColumnValueExp(column), BinaryOperation::kEquals,
                             ConstantValueExp(value));
}

Expression UnaryTest(UnaryOperation op, const ColumnName& column) {
  return UnaryExpressionExp(ColumnValueExp(column), op);
}

// Builds a memo whose root group holds Selection(MarkJoin(scan a, scan b))
// with the given marker test, and explores it with ONLY the built-in
// `mark_join_to_filter` rule.  Returns the logical operators discovered in
// the root group (empty when the rule declined or nothing was added).
std::vector<LogicalOperator> ExploreMarkJoinToFilter(
    const Expression& marker_test) {
  Memo memo;
  const GroupId root = memo.Build({"a", "b"});
  const cascades::Group& base = memo.Get(root);
  EXPECT_EQ(base.expressions.size(), 1U);
  if (base.expressions.size() != 1) {
    return {};
  }
  const GroupId left = base.expressions.at(0).children.at(0);
  const GroupId right = base.expressions.at(0).children.at(1);
  const GroupId mark = memo.EnsureDerivedGroup({"a", "b"}, "mark");
  LogicalExpression mark_join{.operation = LogicalOperator::kMarkJoin,
                              .children = {left, right},
                              .marker_column = "mark"};
  EXPECT_TRUE(memo.AddExpression(mark, mark_join));
  const GroupId selection = memo.EnsureDerivedGroup({"a", "b"}, "selection");
  LogicalExpression selection_expression{.operation =
                                             LogicalOperator::kSelection,
                                         .children = {mark}};
  selection_expression.predicate = marker_test;
  EXPECT_TRUE(memo.AddExpression(selection, selection_expression));

  // Keep only mark_join_to_filter from the default set.
  RuleSet single;
  for (const Rule& rule : RuleSet::Default().Rules()) {
    if (rule.Name() == "mark_join_to_filter") {
      single.Add(rule);
    }
  }
  EXPECT_EQ(single.Rules().size(), 1U);
  SearchEngine search(std::move(memo), single);
  search.Explore(selection);
  const Memo& result = search.GetMemo();
  std::vector<LogicalOperator> operations;
  for (const LogicalExpression& expression :
       result.Get(selection).expressions) {
    operations.push_back(expression.operation);
  }
  return operations;
}

bool HasOperator(const std::vector<LogicalOperator>& operations,
                 LogicalOperator operation) {
  return std::find(operations.begin(), operations.end(), operation) !=
         operations.end();
}

}  // namespace

TEST(MarkJoinToFilterBranchTest, IsTrueMarkerBecomesSemiJoin) {
  const auto operations = ExploreMarkJoinToFilter(
      UnaryTest(UnaryOperation::kIsTrue, ColumnName("mark")));
  EXPECT_TRUE(HasOperator(operations, LogicalOperator::kSemiJoin));
}

TEST(MarkJoinToFilterBranchTest, IsNotFalseMarkerBecomesSemiJoin) {
  const auto operations = ExploreMarkJoinToFilter(
      UnaryTest(UnaryOperation::kIsNotFalse, ColumnName("mark")));
  EXPECT_TRUE(HasOperator(operations, LogicalOperator::kSemiJoin));
}

TEST(MarkJoinToFilterBranchTest, IsFalseMarkerBecomesAntiJoin) {
  const auto operations = ExploreMarkJoinToFilter(
      UnaryTest(UnaryOperation::kIsFalse, ColumnName("mark")));
  EXPECT_TRUE(HasOperator(operations, LogicalOperator::kAntiJoin));
}

TEST(MarkJoinToFilterBranchTest, IsNotTrueMarkerBecomesAntiJoin) {
  const auto operations = ExploreMarkJoinToFilter(
      UnaryTest(UnaryOperation::kIsNotTrue, ColumnName("mark")));
  EXPECT_TRUE(HasOperator(operations, LogicalOperator::kAntiJoin));
}

TEST(MarkJoinToFilterBranchTest, NotMarkerBecomesAntiJoin) {
  const auto operations = ExploreMarkJoinToFilter(
      UnaryTest(UnaryOperation::kNot, ColumnName("mark")));
  EXPECT_TRUE(HasOperator(operations, LogicalOperator::kAntiJoin));
}

TEST(MarkJoinToFilterBranchTest, IsNullMarkerDoesNotRewrite) {
  // The unary `default` branch: an unsupported marker test declines.
  const auto operations = ExploreMarkJoinToFilter(
      UnaryTest(UnaryOperation::kIsNull, ColumnName("mark")));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kSemiJoin));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kAntiJoin));
}

TEST(MarkJoinToFilterBranchTest, IsTrueOverNonColumnDoesNotRewrite) {
  // kIsTrue whose child is not a plain column leaves `marker` empty.
  const auto test = BinaryExpressionExp(ColumnValueExp(ColumnName("a", "x")),
                                        BinaryOperation::kAdd,
                                        ConstantValueExp(Value(1)));
  const auto operations =
      ExploreMarkJoinToFilter(UnaryExpressionExp(test, UnaryOperation::kIsTrue));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kSemiJoin));
}

TEST(MarkJoinToFilterBranchTest, MarkerEqualsTrueBecomesSemiJoin) {
  const auto operations = ExploreMarkJoinToFilter(BinaryExpressionExp(
      ColumnValueExp(ColumnName("mark")), BinaryOperation::kEquals,
      ConstantValueExp(Value(true))));
  EXPECT_TRUE(HasOperator(operations, LogicalOperator::kSemiJoin));
}

TEST(MarkJoinToFilterBranchTest, MarkerEqualsFalseBecomesAntiJoin) {
  const auto operations = ExploreMarkJoinToFilter(BinaryExpressionExp(
      ColumnValueExp(ColumnName("mark")), BinaryOperation::kEquals,
      ConstantValueExp(Value(false))));
  EXPECT_TRUE(HasOperator(operations, LogicalOperator::kAntiJoin));
}

TEST(MarkJoinToFilterBranchTest, MarkerEqualsNullDeclines) {
  // A NULL constant is neither TRUE nor FALSE: the rule declines.
  const auto operations = ExploreMarkJoinToFilter(BinaryExpressionExp(
      ColumnValueExp(ColumnName("mark")), BinaryOperation::kEquals,
      ConstantValueExp(Value())));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kSemiJoin));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kAntiJoin));
}

TEST(MarkJoinToFilterBranchTest, MarkerNonEqualityComparisonDeclines) {
  const auto operations = ExploreMarkJoinToFilter(BinaryExpressionExp(
      ColumnValueExp(ColumnName("mark")), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(true))));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kSemiJoin));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kAntiJoin));
}

TEST(MarkJoinToFilterBranchTest, ConstantEqualsMarkerBecomesSemiJoin) {
  // Flipped operand order: TRUE = marker.
  const auto operations = ExploreMarkJoinToFilter(BinaryExpressionExp(
      ConstantValueExp(Value(true)), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("mark"))));
  EXPECT_TRUE(HasOperator(operations, LogicalOperator::kSemiJoin));
}

TEST(MarkJoinToFilterBranchTest, FalseConstantEqualsMarkerBecomesAntiJoin) {
  const auto operations = ExploreMarkJoinToFilter(BinaryExpressionExp(
      ConstantValueExp(Value(false)), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("mark"))));
  EXPECT_TRUE(HasOperator(operations, LogicalOperator::kAntiJoin));
}

TEST(MarkJoinToFilterBranchTest, NonConstantAgainstMarkerDeclines) {
  // The flipped side is a non-constant expression: decline.
  const auto operations = ExploreMarkJoinToFilter(BinaryExpressionExp(
      ColumnValueExp(ColumnName("a", "x")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("mark"))));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kSemiJoin));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kAntiJoin));
}

TEST(MarkJoinToFilterBranchTest, ColumnEqualsNonConstantDeclines) {
  // marker = <column> has no TRUE/FALSE constant: decline.
  const auto operations = ExploreMarkJoinToFilter(BinaryExpressionExp(
      ColumnValueExp(ColumnName("mark")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("a", "x"))));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kSemiJoin));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kAntiJoin));
}

TEST(MarkJoinToFilterBranchTest, NeitherSideIsMarkerDeclines) {
  const auto operations = ExploreMarkJoinToFilter(
      EqExp("a.x", Value(1)));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kSemiJoin));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kAntiJoin));
}

TEST(MarkJoinToFilterBranchTest, ConstantTestDeclines) {
  // Neither unary nor binary: a bare constant test declines.
  const auto operations =
      ExploreMarkJoinToFilter(ConstantValueExp(Value(true)));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kSemiJoin));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kAntiJoin));
}

TEST(MarkJoinToFilterBranchTest, ConjunctiveMarkerTestDeclines) {
  // Multiple conjuncts: the rule only handles a single marker test.
  const auto test = BinaryExpressionExp(
      UnaryTest(UnaryOperation::kIsTrue, ColumnName("mark")),
      BinaryOperation::kAnd,
      UnaryTest(UnaryOperation::kIsFalse, ColumnName("mark")));
  const auto operations = ExploreMarkJoinToFilter(test);
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kSemiJoin));
  EXPECT_FALSE(HasOperator(operations, LogicalOperator::kAntiJoin));
}

TEST(MarkJoinToFilterBranchTest, MissingPredicateDeclines) {
  // A predicate-less Selection violates the memo invariant before the rule
  // ever runs.
  EXPECT_DEATH(ExploreMarkJoinToFilter(Expression{nullptr}),
               "selection must carry a predicate");
}

TEST(MarkJoinToFilterBranchTest, MarkerNameMismatchDeclines) {
  Memo memo;
  const GroupId root = memo.Build({"a", "b"});
  const cascades::Group& base = memo.Get(root);
  const GroupId left = base.expressions.at(0).children.at(0);
  const GroupId right = base.expressions.at(0).children.at(1);
  const GroupId mark = memo.EnsureDerivedGroup({"a", "b"}, "mark");
  LogicalExpression mark_join{.operation = LogicalOperator::kMarkJoin,
                              .children = {left, right},
                              .marker_column = "other_marker"};
  EXPECT_TRUE(memo.AddExpression(mark, mark_join));
  const GroupId selection = memo.EnsureDerivedGroup({"a", "b"}, "selection");
  LogicalExpression selection_expression{
      .operation = LogicalOperator::kSelection, .children = {mark}};
  selection_expression.predicate =
      UnaryTest(UnaryOperation::kIsTrue, ColumnName("mark"));
  EXPECT_TRUE(memo.AddExpression(selection, selection_expression));

  RuleSet single;
  for (const Rule& rule : RuleSet::Default().Rules()) {
    if (rule.Name() == "mark_join_to_filter") {
      single.Add(rule);
    }
  }
  SearchEngine search(std::move(memo), single);
  search.Explore(selection);
  bool found_semi = false;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(selection).expressions) {
    found_semi |= expression.operation == LogicalOperator::kSemiJoin;
  }
  EXPECT_FALSE(found_semi);
}

TEST(MarkJoinToFilterBranchTest, GroupWithoutMarkJoinDeclines) {
  Memo memo;
  const GroupId root = memo.Build({"a", "b"});
  const cascades::Group& base = memo.Get(root);
  const GroupId left = base.expressions.at(0).children.at(0);
  const GroupId right = base.expressions.at(0).children.at(1);
  // The "mark" group holds an inner join instead of a MarkJoin.
  const GroupId inner = memo.EnsureDerivedGroup({"a", "b"}, "plain-join");
  EXPECT_TRUE(memo.AddExpression(
      inner, LogicalExpression{.operation = LogicalOperator::kJoin,
                               .children = {left, right}}));
  const GroupId selection = memo.EnsureDerivedGroup({"a", "b"}, "selection");
  LogicalExpression selection_expression{
      .operation = LogicalOperator::kSelection, .children = {inner}};
  selection_expression.predicate =
      UnaryTest(UnaryOperation::kIsTrue, ColumnName("mark"));
  EXPECT_TRUE(memo.AddExpression(selection, selection_expression));

  RuleSet single;
  for (const Rule& rule : RuleSet::Default().Rules()) {
    if (rule.Name() == "mark_join_to_filter") {
      single.Add(rule);
    }
  }
  SearchEngine search(std::move(memo), single);
  search.Explore(selection);
  bool found_semi = false;
  for (const LogicalExpression& expression :
       search.GetMemo().Get(selection).expressions) {
    found_semi |= expression.operation == LogicalOperator::kSemiJoin;
  }
  EXPECT_FALSE(found_semi);
}

}  // namespace tinylamb
