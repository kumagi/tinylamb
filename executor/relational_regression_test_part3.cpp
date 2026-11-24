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
TEST_F(RelationalRegressionTest, WherePredicateError_PropagatesFailure) {
  TransactionContext context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(
      Table, t,
      rs_->CreateTable(context,
                       Schema("ErrTable", {Column("id", ValueType::kInt64),
                                           Column("val", ValueType::kInt64)})));
  ASSERT_SUCCESS(
      t.Insert(context.txn_,
               Row({Value(1), Value(std::numeric_limits<int64_t>::min())}))
          .GetStatus());
  ASSERT_SUCCESS(context.txn_.PreCommit());

  TransactionContext run_ctx = rs_->BeginContext();
  StatusOr<Executor> prepared =
      engine.Prepare(run_ctx, "SELECT id FROM ErrTable WHERE ABS(val) <= 0;");
  ASSERT_TRUE(prepared.HasValue());
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
  }
  EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess);
  EXPECT_EQ(tinylamb::Status::kSuccess, run_ctx.Abort());
}

TEST_F(RelationalRegressionTest,
       ConjunctEvaluationToleratesErrorOnRejectedRow) {
  TransactionContext context = rs_->BeginContext();
  SqlEngine engine(*rs_);
  ASSIGN_OR_ASSERT_FAIL(
      Table, t,
      rs_->CreateTable(context,
                       Schema("ErrTable2", {Column("id", ValueType::kInt64),
                                            Column("i", ValueType::kInt64),
                                            Column("f", ValueType::kDouble)})));
  ASSERT_SUCCESS(
      t.Insert(context.txn_,
               Row({Value(1), Value(std::numeric_limits<int64_t>::min()),
                    Value(-100.0)}))
          .GetStatus());
  ASSERT_SUCCESS(t.Insert(context.txn_, Row({Value(2), Value(0), Value(10.0)}))
                     .GetStatus());
  ASSERT_SUCCESS(context.txn_.PreCommit());

  // Simple compare rejects row 1 (f > 0 is false for row 1):
  // Both orders of conjuncts must succeed and return id = 2.
  {
    std::vector<Row> r1 = RelationalRun(
        *rs_, "SELECT id FROM ErrTable2 WHERE f > 0 AND ABS(i) <= 3;");
    ASSERT_EQ(r1.size(), 1U);
    EXPECT_EQ(r1[0][0], Value(2));

    std::vector<Row> r2 = RelationalRun(
        *rs_, "SELECT id FROM ErrTable2 WHERE ABS(i) <= 3 AND f > 0;");
    ASSERT_EQ(r2.size(), 1U);
    EXPECT_EQ(r2[0][0], Value(2));
  }

  // Residual compare rejects row 1 (SAFE_ADD(i, -1) = 0 is false/null for row
  // 1): Both orders of conjuncts must succeed and return empty.
  {
    std::vector<Row> r1 =
        RelationalRun(*rs_,
                      "SELECT id FROM ErrTable2 WHERE (SAFE_ADD(i, -1) = 0) "
                      "AND ABS(i) <= 3;");
    EXPECT_TRUE(r1.empty());

    std::vector<Row> r2 = RelationalRun(*rs_,
                                        "SELECT id FROM ErrTable2 WHERE ABS(i) "
                                        "<= 3 AND (SAFE_ADD(i, -1) = 0);");
    EXPECT_TRUE(r2.empty());
  }

  // When no conjunct rejects row 1 (f < 0 is true for row 1), ABS(i) must
  // throw.
  {
    TransactionContext run_ctx = rs_->BeginContext();
    StatusOr<Executor> prepared = engine.Prepare(
        run_ctx, "SELECT id FROM ErrTable2 WHERE f < 0 AND ABS(i) <= 3;");
    ASSERT_TRUE(prepared.HasValue());
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess);
    EXPECT_EQ(tinylamb::Status::kSuccess, run_ctx.Abort());
  }
}

TEST_F(RelationalRegressionTest, WindowFunctions_ArgumentAndFrameValidation) {
  auto check_fails = [&](std::string_view sql) {
    TransactionContext run_ctx = rs_->BeginContext();
    SqlEngine engine(*rs_);
    StatusOr<Executor> prepared = engine.Prepare(run_ctx, sql);
    if (!prepared.HasValue()) {
      EXPECT_EQ(tinylamb::Status::kSuccess, run_ctx.Abort());
      return;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
    EXPECT_EQ(tinylamb::Status::kSuccess, run_ctx.Abort());
  };

  // NTILE validations
  check_fails("SELECT NTILE(NULL) OVER () FROM KeyTable;");
  check_fails("SELECT NTILE(-1) OVER () FROM KeyTable;");
  check_fails("SELECT NTILE(0) OVER () FROM KeyTable;");

  // LAG / LEAD validations
  check_fails("SELECT LAG(k, NULL) OVER (ORDER BY k) FROM KeyTable;");
  check_fails("SELECT LAG(k, -1) OVER (ORDER BY k) FROM KeyTable;");
  check_fails("SELECT LEAD(k, NULL) OVER (ORDER BY k) FROM KeyTable;");
  check_fails("SELECT LEAD(k, -1) OVER (ORDER BY k) FROM KeyTable;");

  // Row-dependent offset evaluation in LAG
  {
    std::vector<Row> rows = RelationalRun(
        *rs_, "SELECT LAG(k, k) OVER (ORDER BY k) FROM KeyTable;");
    ASSERT_EQ(rows.size(), 10U);
    for (size_t i = 0; i < 10; ++i) {
      // Offset is k, so target is i - i = 0 for each row; always yields row 0
      // (Value(0)).
      EXPECT_EQ(rows[i][0], Value(0));
    }
  }

  // NTH_VALUE validations
  check_fails("SELECT NTH_VALUE(k, NULL) OVER (ORDER BY k) FROM KeyTable;");
  check_fails("SELECT NTH_VALUE(k, 0) OVER (ORDER BY k) FROM KeyTable;");
  check_fails("SELECT NTH_VALUE(k, -1) OVER (ORDER BY k) FROM KeyTable;");

  // Window frame bound validations
  check_fails(
      "SELECT SUM(k) OVER (ORDER BY k ROWS BETWEEN -1 PRECEDING AND CURRENT "
      "ROW) FROM KeyTable;");
  check_fails(
      "SELECT SUM(k) OVER (ORDER BY k RANGE BETWEEN NULL PRECEDING AND CURRENT "
      "ROW) FROM KeyTable;");
  check_fails(
      "SELECT SUM(k) OVER (ORDER BY k RANGE BETWEEN -1 PRECEDING AND CURRENT "
      "ROW) FROM KeyTable;");
  check_fails(
      "SELECT SUM(k) OVER (ORDER BY k RANGE BETWEEN NULL PRECEDING AND CURRENT "
      "ROW) FROM KeyTable;");
}

TEST_F(RelationalRegressionTest, DerivedSingleColumnEquiJoinUsesHashJoin) {
  // TPC-H Q20 at SF1: joining a base table against a single-column derived
  // table on `AggTable.k = s.k` must resolve the key into a hash join.
  // Previously the derived output schema stayed bare while outer predicates
  // reference it through the source alias, so the equality went unresolved
  // and execution fell back to cross product + filter (800K x derived rows
  // at SF1). LoadSource now alias-qualifies derived schemas, mirroring the
  // estimate path.
  const std::string profile = ExplainAnalyze(
      *rs_,
      "SELECT COUNT(*) FROM AggTable, (SELECT k FROM SmallKeys) s "
      "WHERE AggTable.k = s.k;");
  EXPECT_NE(profile.find("nested_loop_joins=0"), std::string::npos) << profile;

  // Results are unchanged by the join shape (20 matches per key 1..4).
  const std::vector<Row> rows = RelationalRun(
      *rs_,
      "SELECT COUNT(*) FROM AggTable, (SELECT k FROM SmallKeys) s "
      "WHERE AggTable.k = s.k;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{80}));
}

TEST_F(RelationalRegressionTest, DerivedSingleColumnBareEqualityUsesHashJoin) {
  // Same join with bare, disjoint column names (`v = kk`, the TPC-H Q20
  // `ps_partkey = p_partkey` shape). Previously IsVirtualValueTableField
  // claimed every bare predicate column against the single-column derived
  // schema, marking the equality ambiguous and forcing the same cross
  // product fallback.
  const std::string profile = ExplainAnalyze(
      *rs_,
      "SELECT COUNT(*) FROM AggTable, (SELECT k AS kk FROM SmallKeys) s "
      "WHERE v = kk;");
  EXPECT_NE(profile.find("nested_loop_joins=0"), std::string::npos) << profile;

  // v is constantly 1 while derived kk ranges over {1,2,3,4}: every one
  // of the 200 AggTable rows matches exactly the kk = 1 row.
  const std::vector<Row> rows = RelationalRun(
      *rs_,
      "SELECT COUNT(*) FROM AggTable, (SELECT k AS kk FROM SmallKeys) s "
      "WHERE v = kk;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{200}));
}

// Grouped-join fusion: the final join of a grouped statement feeds the
// aggregation directly and the joined relation never materializes.  These
// pins guard the fused emit against the materializing path's semantics:
// LEFT-join NULL padding feeds COUNT a NULL (not a skip), every outer row
// joins multiplicatively, and ORDER BY/LIMIT still resolve over the
// projected output.
TEST_F(RelationalRegressionTest,
       UniformExtremaKeepNullExtensionAndVaryingInputs) {
  RelationalRun(*rs_, "CREATE TABLE ConstantDimension(k INT64, v DOUBLE)");
  RelationalRun(*rs_,
                "INSERT INTO ConstantDimension VALUES (1,7.5),(2,7.5),(2,7.5)");
  EXPECT_EQ(
      RelationalRun(*rs_, R"sql(
    SELECT a.k,SUM(a.v),MIN(d.v),MAX(d.v) FROM AggTable a
    LEFT JOIN ConstantDimension d ON a.k=d.k
    GROUP BY a.k ORDER BY a.k LIMIT 4
  )sql"),
      std::vector<Row>({Row({Value(0), Value(20), Value(), Value()}),
                        Row({Value(1), Value(20), Value(7.5), Value(7.5)}),
                        Row({Value(2), Value(40), Value(7.5), Value(7.5)}),
                        Row({Value(3), Value(20), Value(), Value()})}));
  RelationalRun(*rs_, "INSERT INTO ConstantDimension VALUES (1,9.0),(1,NULL)");
  EXPECT_EQ(
      RelationalRun(*rs_, R"sql(
    SELECT a.k,SUM(a.v),MIN(d.v),MAX(d.v) FROM AggTable a,ConstantDimension d
    WHERE a.k=d.k GROUP BY a.k ORDER BY a.k
  )sql"),
      std::vector<Row>({Row({Value(1), Value(60), Value(7.5), Value(9.0)}),
                        Row({Value(2), Value(40), Value(7.5), Value(7.5)})}));
}

TEST_F(RelationalRegressionTest, FusedConditionalAggregateKeepsNullElse) {
  RelationalRun(*rs_, "CREATE TABLE CaseFacts(k INT64, v DOUBLE)");
  RelationalRun(
      *rs_, "INSERT INTO CaseFacts VALUES (1,2.5),(1,NULL),(2,4.0),(3,NULL)");
  RelationalRun(*rs_, "ANALYZE CaseFacts");
  RelationalRun(*rs_, "ANALYZE SmallKeys");
  const std::string sql = R"sql(
    SELECT d.k, SUM(CASE WHEN d.k=1 THEN f.v ELSE NULL END),
      AVG(CASE WHEN d.k=1 THEN f.v END),
      COUNT(CASE WHEN d.k=1 THEN f.v END),
      SUM(CASE WHEN d.k=2 THEN f.v*(1.0-f.v) END),
      COUNT(CASE WHEN d.k=1 THEN 1 END),
      COUNT(CASE WHEN d.k=1 THEN 1 ELSE 0 END),
      COUNT(CASE WHEN d.k=1 THEN f.v+f.v END)
    FROM CaseFacts f, SmallKeys d WHERE f.k=d.k GROUP BY d.k ORDER BY d.k
  )sql";
  EXPECT_EQ(RelationalRun(*rs_, sql),
            std::vector<Row>({Row({Value(1), Value(2.5), Value(2.5), Value(1),
                                   Value(), Value(2), Value(2), Value(1)}),
                              Row({Value(2), Value(), Value(), Value(0),
                                   Value(-12.0), Value(0), Value(1), Value(0)}),
                              Row({Value(3), Value(), Value(), Value(0),
                                   Value(), Value(0), Value(1), Value(0)})}));
}

TEST_F(RelationalRegressionTest, GroupedJoinFusionKeepsGroupedJoinSemantics) {
  TransactionContext setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, customer,
      rs_->CreateTable(
          setup, Schema("FusedCustomer", {Column("ck", ValueType::kInt64),
                                          Column("cn", ValueType::kVarChar)})));
  ASSIGN_OR_ASSERT_FAIL(
      Table, orders,
      rs_->CreateTable(
          setup, Schema("FusedOrders", {Column("ok", ValueType::kInt64),
                                        Column("oc", ValueType::kInt64),
                                        Column("amt", ValueType::kInt64)})));
  // Customers 1 (2 orders), 2 (1 order), 3 (no orders), 4 (2 orders, one
  // filtered away only by a predicate that stays above the join).
  struct OrderRow {
    int64_t ok;
    int64_t oc;
    int64_t amt;
  };
  for (const OrderRow& order : {OrderRow{.ok = 10, .oc = 1, .amt = 100},
                                OrderRow{.ok = 11, .oc = 1, .amt = 50},
                                OrderRow{.ok = 12, .oc = 2, .amt = 70},
                                OrderRow{.ok = 13, .oc = 4, .amt = 20},
                                OrderRow{.ok = 14, .oc = 4, .amt = 80}}) {
    ASSERT_SUCCESS(
        orders
            .Insert(setup.txn_,
                    Row({Value(order.ok), Value(order.oc), Value(order.amt)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(
      customer.Insert(setup.txn_, Row({Value(int64_t{1}), Value("a")}))
          .GetStatus());
  ASSERT_SUCCESS(
      customer.Insert(setup.txn_, Row({Value(int64_t{2}), Value("b")}))
          .GetStatus());
  ASSERT_SUCCESS(
      customer.Insert(setup.txn_, Row({Value(int64_t{3}), Value("c")}))
          .GetStatus());
  ASSERT_SUCCESS(
      customer.Insert(setup.txn_, Row({Value(int64_t{4}), Value("d")}))
          .GetStatus());
  ASSERT_SUCCESS(setup.txn_.PreCommit());

  // Q13 shape: LEFT JOIN grouped on the left key; the empty customer 3 must
  // survive with COUNT = 0 (COUNT(NULL) does not skip the group).
  const auto counts =
      RelationalRun(*rs_,
                    "SELECT ck, COUNT(ok) AS cnt FROM FusedCustomer "
                    "LEFT OUTER JOIN FusedOrders ON ck = oc GROUP BY ck "
                    "ORDER BY cnt DESC, ck ASC;");
  ASSERT_EQ(counts.size(), 4U);
  EXPECT_EQ(counts[0], (Row({Value(int64_t{1}), Value(int64_t{2})})));
  EXPECT_EQ(counts[1], (Row({Value(int64_t{4}), Value(int64_t{2})})));
  EXPECT_EQ(counts[2], (Row({Value(int64_t{2}), Value(int64_t{1})})));
  EXPECT_EQ(counts[3], (Row({Value(int64_t{3}), Value(int64_t{0})})));

  // Inner join + SUM + HAVING-free ORDER BY over an alias, matching the Q21
  // tail shape.
  const auto sums =
      RelationalRun(*rs_,
                    "SELECT cn, SUM(amt) AS total FROM FusedCustomer "
                    "JOIN FusedOrders ON ck = oc GROUP BY cn "
                    "ORDER BY total DESC, cn ASC;");
  ASSERT_EQ(sums.size(), 3U);
  EXPECT_EQ(sums[0], (Row({Value("a"), Value(int64_t{150})})));
  EXPECT_EQ(sums[1], (Row({Value("d"), Value(int64_t{100})})));
  EXPECT_EQ(sums[2], (Row({Value("b"), Value(int64_t{70})})));

  // LIMIT over the fused grouped output (TopN tail).
  const auto top_one =
      RelationalRun(*rs_,
                    "SELECT cn, SUM(amt) AS total FROM FusedCustomer "
                    "JOIN FusedOrders ON ck = oc GROUP BY cn "
                    "ORDER BY total DESC, cn ASC LIMIT 1;");
  ASSERT_EQ(top_one.size(), 1U);
  EXPECT_EQ(top_one[0], (Row({Value("a"), Value(int64_t{150})})));
}

TEST_F(RelationalRegressionTest,
       SmallGroupedJoinResetsExpressionScratchAfterAnError) {
  const std::string valid =
      "SELECT SUM(CASE WHEN a.k < 3 THEN a.v / 2 ELSE a.v END) "
      "FROM AggTable a JOIN SmallKeys s ON a.k = s.k";
  EXPECT_NE(ExplainAnalyze(*rs_, valid, /*force_relational=*/true)
                .find("grouped_join_fusions=1"),
            std::string::npos);
  // The small join has one probe team. Its CASE and floating arithmetic
  // reuse expression scratch after a failed evaluation on the same caller.
  for (int repetition = 0; repetition < 3; ++repetition) {
    const auto rows = RelationalRun(*rs_, valid, /*force_relational=*/true);
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows[0], Row({Value(60.0)}));
    auto context = rs_->BeginReadOnlyContext();
    SqlEngine engine(*rs_);
    engine.SetForceRelationalFallback(true);
    auto prepared = engine.Prepare(
        context,
        "SELECT SUM(CASE WHEN a.k < 3 THEN a.v / (a.k - 1) ELSE a.v END) "
        "FROM AggTable a JOIN SmallKeys s ON a.k = s.k");
    if (prepared.HasValue()) {
      Row row;
      size_t emitted = 0;
      while (prepared.Value()->Next(&row, nullptr)) {
        ++emitted;
      }
      EXPECT_EQ(emitted, 0U);
      EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess);
      prepared.Value().reset();
    } else {
      EXPECT_NE(prepared.GetStatus(), Status::kSuccess);
    }
    EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
  }
  EXPECT_EQ(RelationalRun(*rs_, valid, /*force_relational=*/true),
            std::vector<Row>({Row({Value(60.0)})}));
}

TEST_F(RelationalRegressionTest, FilteredAggregateOverJoinUsesPredicate) {
  TransactionContext setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, left,
      rs_->CreateTable(
          setup, Schema("FilteredLeft", {Column("k", ValueType::kInt64),
                                         Column("v", ValueType::kInt64)})));
  ASSIGN_OR_ASSERT_FAIL(
      Table, right,
      rs_->CreateTable(
          setup, Schema("FilteredRight", {Column("k", ValueType::kInt64)})));
  for (const Row& row : {Row({Value(int64_t{1}), Value(int64_t{10})}),
                         Row({Value(int64_t{1}), Value(int64_t{30})}),
                         Row({Value(int64_t{2}), Value(int64_t{40})})}) {
    ASSERT_SUCCESS(left.Insert(setup.txn_, row).GetStatus());
  }
  for (const int64_t key : {int64_t{1}, int64_t{2}}) {
    ASSERT_SUCCESS(right.Insert(setup.txn_, Row({Value(key)})).GetStatus());
  }
  ASSERT_SUCCESS(setup.txn_.PreCommit());

  const auto countif =
      RelationalRun(*rs_,
                    "SELECT COUNTIF(t.v > 20) FROM FilteredLeft t "
                    "JOIN FilteredRight u ON t.k = u.k;");
  ASSERT_EQ(countif.size(), 1U);
  EXPECT_EQ(countif[0][0], Value(int64_t{2}));

  const auto countif_one =
      RelationalRun(*rs_,
                    "SELECT COUNTIF(t.v > 30) FROM FilteredLeft t "
                    "JOIN FilteredRight u ON t.k = u.k;");
  ASSERT_EQ(countif_one.size(), 1U);
  EXPECT_EQ(countif_one[0][0], Value(int64_t{1}));

  const auto filtered_count =
      RelationalRun(*rs_,
                    "SELECT COUNT(* WHERE t.v > 30) FROM FilteredLeft t "
                    "JOIN FilteredRight u ON t.k = u.k;");
  ASSERT_EQ(filtered_count.size(), 1U);
  EXPECT_EQ(filtered_count[0][0], Value(int64_t{1}));
}

// A correlated-subquery build whose scan was pruned by the outer
// correlation-key domain (ScopedCorrelatedOuter registration) must answer
// every outer key of THAT pipeline, and a later statement with a different
// outer domain must rebuild rather than reuse the pruned build.  NULL
// correlation keys on either side never match, so pruning them is
// observationally identical.
TEST_F(RelationalRegressionTest,
       CorrelatedDomainFilteredBuildsStaySoundAcrossOuterDomains) {
  TransactionContext setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, fact,
      rs_->CreateTable(setup,
                       Schema("DomFact", {Column("fkey", ValueType::kInt64),
                                          Column("val", ValueType::kInt64)})));
  ASSIGN_OR_ASSERT_FAIL(
      Table, dim,
      rs_->CreateTable(setup,
                       Schema("DomDim", {Column("dkey", ValueType::kInt64),
                                         Column("flag", ValueType::kInt64)})));
  // Fact rows for keys 1..6 plus one NULL key; NULL never equals a probe.
  for (int64_t key = 1; key <= 6; ++key) {
    for (int64_t j = 0; j < 8; ++j) {
      ASSERT_SUCCESS(
          fact.Insert(setup.txn_, Row({Value(key), Value((key * 10) + j)}))
              .GetStatus());
    }
  }
  ASSERT_SUCCESS(
      fact.Insert(setup.txn_, Row({Value(), Value(int64_t{99})})).GetStatus());
  for (int64_t key = 1; key <= 6; ++key) {
    ASSERT_SUCCESS(
        dim.Insert(setup.txn_, Row({Value(key), Value(key % 2)})).GetStatus());
  }
  ASSERT_SUCCESS(setup.txn_.PreCommit());

  const auto avg_active =
      RelationalRun(*rs_,
                    "SELECT d.dkey, (SELECT AVG(f.val) FROM DomFact f "
                    "WHERE f.fkey = d.dkey) FROM DomDim d "
                    "WHERE d.flag = 1 ORDER BY 1;");
  ASSERT_EQ(avg_active.size(), 3U);
  EXPECT_EQ(avg_active[0][0], Value(int64_t{1}));
  EXPECT_EQ(avg_active[0][1], Value(13.5));
  EXPECT_EQ(avg_active[1][0], Value(int64_t{3}));
  EXPECT_EQ(avg_active[1][1], Value(33.5));
  EXPECT_EQ(avg_active[2][0], Value(int64_t{5}));
  EXPECT_EQ(avg_active[2][1], Value(53.5));

  // Same subquery shape, disjoint outer domain: keys 2/4/6.  A reused build
  // pruned to the {1, 3, 5} domain above would answer NULL/NULL here.
  const auto avg_inactive =
      RelationalRun(*rs_,
                    "SELECT d.dkey, (SELECT AVG(f.val) FROM DomFact f "
                    "WHERE f.fkey = d.dkey) FROM DomDim d "
                    "WHERE d.flag = 0 ORDER BY 1;");
  ASSERT_EQ(avg_inactive.size(), 3U);
  EXPECT_EQ(avg_inactive[0][0], Value(int64_t{2}));
  EXPECT_EQ(avg_inactive[0][1], Value(23.5));
  EXPECT_EQ(avg_inactive[1][0], Value(int64_t{4}));
  EXPECT_EQ(avg_inactive[1][1], Value(43.5));
  EXPECT_EQ(avg_inactive[2][0], Value(int64_t{6}));
  EXPECT_EQ(avg_inactive[2][1], Value(63.5));

  // Both domains in one pass over the same dim scan: the build (keyed once,
  // under whichever outer context triggered it) must still answer every key.
  const auto avg_all =
      RelationalRun(*rs_,
                    "SELECT d.dkey, (SELECT AVG(f.val) FROM DomFact f "
                    "WHERE f.fkey = d.dkey) FROM DomDim d ORDER BY 1;");
  ASSERT_EQ(avg_all.size(), 6U);
  for (int64_t key = 1; key <= 6; ++key) {
    EXPECT_EQ(avg_all[static_cast<size_t>(key) - 1][0], Value(key));
    EXPECT_EQ(avg_all[static_cast<size_t>(key) - 1][1],
              Value(static_cast<double>(key * 10) + 3.5));
  }
}

TEST(RelationalPlanningTest, NullRejectingPredicateClassification) {
  const Expression strict = BinaryExpressionExp(
      ColumnValueExp(ColumnName("r", "v")), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(int64_t{0})));
  const Expression not_null = UnaryExpressionExp(
      ColumnValueExp(ColumnName("r", "v")), UnaryOperation::kIsNotNull);
  const Expression null_test = UnaryExpressionExp(
      ColumnValueExp(ColumnName("r", "v")), UnaryOperation::kIsNull);
  const Expression coalesced = BinaryExpressionExp(
      FunctionCallExp("coalesce", {ColumnValueExp(ColumnName("r", "v")),
                                   ConstantValueExp(Value(int64_t{0}))}),
      BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{0})));
  EXPECT_TRUE(relational_detail::IsNullRejectingPredicate(strict));
  EXPECT_TRUE(relational_detail::IsNullRejectingPredicate(not_null));
  EXPECT_TRUE(relational_detail::IsNullRejectingPredicate(
      BinaryExpressionExp(strict, BinaryOperation::kAnd, null_test)));
  EXPECT_FALSE(relational_detail::IsNullRejectingPredicate(null_test));
  EXPECT_FALSE(relational_detail::IsNullRejectingPredicate(coalesced));
}

TEST_F(RelationalRegressionTest,
       FusedDimensionTupleKeepsNullsAndWideProbeKeys) {
  RelationalRun(*rs_,
                "CREATE TABLE TupleDim(id INT64,label STRING,region STRING)");
  RelationalRun(*rs_,
                "INSERT INTO TupleDim VALUES "
                "(1,'same','r'),(2,'same','r'),(3,NULL,'r'),(3,NULL,'r')");
  RelationalRun(*rs_, "CREATE TABLE TupleFact(id INT64,p INT64,v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO TupleFact VALUES "
                "(1,0,10),(2,0,20),(3,0,4),(4,0,7),"
                "(1,NULL,2),(2,NULL,3),(1,9223372036854775807,6)");
  const auto rows = RelationalRun(*rs_, R"sql(
    SELECT f.p,d.label,d.region,SUM(f.v) FROM TupleFact f
    LEFT JOIN TupleDim d ON f.id=d.id GROUP BY f.p,d.label,d.region
    ORDER BY f.p NULLS FIRST,d.label NULLS FIRST,d.region NULLS FIRST
  )sql");
  EXPECT_EQ(rows, std::vector<Row>(
                      {Row({Value(), Value("same"), Value("r"), Value(5)}),
                       Row({Value(0), Value(), Value(), Value(7)}),
                       Row({Value(0), Value(), Value("r"), Value(8)}),
                       Row({Value(0), Value("same"), Value("r"), Value(30)}),
                       Row({Value(std::numeric_limits<int64_t>::max()),
                            Value("same"), Value("r"), Value(6)})}));
}

TEST_F(RelationalRegressionTest,
       CompactJoinedCountsKeepWorkerMergesAndFallback) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "k"), ValueType::kInt64),
                             Column(ColumnName("f", "p"), ValueType::kInt64)});
  right.schema =
      Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64),
                   Column(ColumnName("d", "label"), ValueType::kVarChar)});
  right.rows = {Row({Value(0), Value("a")}), Row({Value(0), Value("a")}),
                Row({Value(1), Value("b")}), Row({Value(2), Value()})};
  left.rows.reserve(32768);
  for (int64_t i = 0; i < 32768; ++i) {
    // A prime period keeps the capacity estimator's distributed sample
    // diverse while worker-local and cross-worker duplicate groups remain.
    left.rows.emplace_back(
        std::vector<Value>{Value(i % 3), Value(((i * 7919) % 8009) - 4000)});
  }
  const auto col = [](const char* owner, const char* name) {
    return ColumnValueExp(ColumnName(owner, name));
  };
  const auto count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("*"));
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {col("f", "p"), col("d", "k"), col("d", "label")};
  fusion.select_items = fusion.group_keys;
  fusion.select_items.push_back(count);
  fusion.select_names = {"p", "k", "label", "n"};
  fusion.aggregates = {count.get()};
  fusion.count_star = {true};
  fusion.eligible = true;
  fusion.post_having_agg_slot = 0;
  fusion.post_having_op = BinaryOperation::kGreaterThan;
  fusion.post_having_constant_or_factor = 4;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      col("f", "k"), BinaryOperation::kEquals, col("d", "k"))};
  for (const bool nullable : {false, true}) {
    if (nullable) {
      left.rows.front().values_[1] = Value();
      left.rows.back().values_[1] = Value(std::numeric_limits<int64_t>::max());
    }
    for (const size_t workers : {size_t{1}, size_t{2}, size_t{4}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      std::vector<Row> reference;
      for (const int mode : {0, 1, 2, 3}) {
        const bool early = mode != 0;
        fusion.post_having_active = mode == 1;
        fusion.having =
            mode >= 2
                ? BinaryExpressionExp(
                      count, BinaryOperation::kGreaterThan,
                      ConstantValueExp(Value(
                          mode == 2 ? int64_t{4}
                                    : std::numeric_limits<int64_t>::max())))
                : Expression{};
        fusion.post_having_applied = false;
        auto result = relational_detail::TryFusedGroupedJoin(
            context, left, right, predicates, JoinType::kInner, nullptr, {},
            &fusion);
        ASSERT_TRUE(result.HasValue()) << result.GetStatus();
        auto output = result.MoveValue();
        if (!output) {
          FAIL() << "counted grouped join declined";
        }
        if (!early) {
          for (const auto& row : output->rows) {
            if (row[3].value.int_value > 4) {
              reference.push_back(row);
            }
          }
        } else {
          EXPECT_TRUE(fusion.post_having_applied);
          if (mode == 3) {
            EXPECT_TRUE(output->rows.empty());
          } else {
            EXPECT_EQ(output->rows, reference);
          }
        }
      }
    }
  }
}

TEST_F(RelationalRegressionTest, HighCardinalityCountLogKeepsLateDuplicates) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "k"), ValueType::kInt64),
                             Column(ColumnName("f", "p"), ValueType::kInt64)});
  right.schema =
      Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64),
                   Column(ColumnName("d", "label"), ValueType::kVarChar)});
  right.rows = {Row({Value(0), Value("a")}), Row({Value(1), Value("b")})};
  constexpr int64_t kGroups = 32771;
  left.rows.reserve(kGroups * 3);
  // Each worker first sees almost entirely distinct groups. Later chunks
  // revisit them, including NULL and an INT64 key outside the packed range.
  for (int repeat = 0; repeat < 3; ++repeat) {
    for (int64_t key = 0; key < kGroups; ++key) {
      left.rows.emplace_back(std::vector<Value>{
          Value(key % 2), key == 0 ? Value()
                          : key == 1
                              ? Value(std::numeric_limits<int64_t>::max())
                              : Value(key)});
    }
  }
  const auto col = [](const char* owner, const char* name) {
    return ColumnValueExp(ColumnName(owner, name));
  };
  const auto count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("*"));
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {col("f", "p"), col("d", "label")};
  fusion.select_items = {col("f", "p"), col("d", "label"), count};
  fusion.select_names = {"p", "label", "n"};
  fusion.aggregates = {count.get()};
  fusion.count_star = {true};
  fusion.eligible = true;
  fusion.having = BinaryExpressionExp(count, BinaryOperation::kGreaterThan,
                                      ConstantValueExp(Value(2)));
  const std::vector<Expression> predicates{BinaryExpressionExp(
      col("f", "k"), BinaryOperation::kEquals, col("d", "k"))};
  for (const bool duplicate_build : {false, true}) {
    if (duplicate_build) {
      right.rows.emplace_back(std::vector<Value>{Value(0), Value("a")});
    }
    for (const size_t workers : {size_t{1}, size_t{4}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      auto result = relational_detail::TryFusedGroupedJoin(
          context, left, right, predicates, JoinType::kInner, nullptr, {},
          &fusion);
      ASSERT_TRUE(result.HasValue()) << result.GetStatus();
      auto output = result.MoveValue();
      ASSERT_TRUE(output.has_value());
      ASSERT_EQ(output->rows.size(), kGroups);
      std::set<int64_t> seen;
      for (const auto& row : output->rows) {
        const int64_t key =
            row[0].IsNull() ? 0
            : row[0].value.int_value == std::numeric_limits<int64_t>::max()
                ? 1
                : row[0].value.int_value;
        EXPECT_TRUE(seen.insert(key).second);
        EXPECT_GE(key, 0);
        EXPECT_LT(key, kGroups);
        EXPECT_EQ(row[1], Value(key % 2 == 0 ? "a" : "b"));
        EXPECT_EQ(row[2], Value(duplicate_build && key % 2 == 0 ? 6 : 3));
      }
    }
  }
}

TEST_F(RelationalRegressionTest, CountPrefilterKeepsExactBoundaryGroups) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "k"), ValueType::kInt64),
                             Column(ColumnName("f", "p"), ValueType::kInt64)});
  right.schema =
      Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64),
                   Column(ColumnName("d", "label"), ValueType::kVarChar)});
  right.rows = {Row({Value(0), Value("a")}), Row({Value(1), Value("b")})};
  constexpr int64_t kGroups = 65536;
  std::vector<int64_t> counts(kGroups);
  left.rows.reserve(kGroups * 4);
  const auto append = [&](int64_t key) {
    left.rows.emplace_back(
        std::vector<Value>{Value(key % 2), key == 0   ? Value()
                                           : key == 1 ? Value(INT64_MAX)
                                                      : Value(key)});
    ++counts[static_cast<size_t>(key)];
  };
  // A weighted local partial precedes the append-log phase. Most other
  // groups remain below the threshold, with late duplicates at its boundary.
  for (int i = 0; i < 6; ++i) {
    append(0);
  }
  for (int repeat = 0; repeat < 6; ++repeat) {
    for (int64_t key = 0; key < kGroups; ++key) {
      const int64_t frequency = key % 128 == 0   ? 6
                                : key % 128 == 1 ? 5
                                                 : (key % 4) + 1;
      if (repeat < frequency) {
        append(key);
      }
    }
  }
  const auto col = [](const char* owner, const char* name) {
    return ColumnValueExp(ColumnName(owner, name));
  };
  const auto count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("*"));
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {col("f", "p"), col("d", "label")};
  fusion.select_items = {col("f", "p"), col("d", "label"), count};
  fusion.select_names = {"p", "label", "n"};
  fusion.aggregates = {count.get()};
  fusion.count_star = {true};
  fusion.eligible = true;
  fusion.post_having_agg_slot = 0;
  fusion.post_having_op = BinaryOperation::kGreaterThan;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      col("f", "k"), BinaryOperation::kEquals, col("d", "k"))};
  for (const bool duplicates : {false, true}) {
    if (duplicates) {
      right.rows.emplace_back(std::vector<Value>{Value(0), Value("a")});
    }
    for (const auto& threshold : {Value(4), Value(4.5), Value(254)}) {
      const bool integer = threshold.type == ValueType::kInt64;
      const double limit = integer
                               ? static_cast<double>(threshold.value.int_value)
                               : threshold.value.double_value;
      fusion.having =
          integer ? BinaryExpressionExp(count, BinaryOperation::kGreaterThan,
                                        ConstantValueExp(threshold))
                  : Expression{};
      fusion.post_having_active = !integer;
      fusion.post_having_constant_or_factor = limit;
      for (const size_t workers : {size_t{1}, size_t{4}}) {
        std::atomic<size_t> active{0};
        ScopedQueryParallelismBudget budget(&active, workers);
        auto result = relational_detail::TryFusedGroupedJoin(
            context, left, right, predicates, JoinType::kInner, nullptr, {},
            &fusion);
        ASSERT_TRUE(result.HasValue()) << result.GetStatus();
        auto output = result.MoveValue();
        ASSERT_TRUE(output.has_value());
        size_t expected_groups = 0;
        for (int64_t key = 0; key < kGroups; ++key) {
          const int64_t expected = counts[static_cast<size_t>(key)] *
                                   (duplicates && key % 2 == 0 ? 2 : 1);
          expected_groups += static_cast<double>(expected) > limit ? 1U : 0U;
        }
        ASSERT_EQ(output->rows.size(), expected_groups);
        std::set<int64_t> seen;
        for (const auto& row : output->rows) {
          const int64_t key = row[0].IsNull() ? 0
                              : row[0].value.int_value == INT64_MAX
                                  ? 1
                                  : row[0].value.int_value;
          ASSERT_GE(key, 0);
          ASSERT_LT(key, kGroups);
          EXPECT_TRUE(seen.insert(key).second);
          const int64_t expected = counts[static_cast<size_t>(key)] *
                                   (duplicates && key % 2 == 0 ? 2 : 1);
          EXPECT_GT(static_cast<double>(expected), limit);
          EXPECT_EQ(row[1], Value(key % 2 == 0 ? "a" : "b"));
          EXPECT_EQ(row[2], Value(expected));
        }
      }
    }
  }
}

TEST_F(RelationalRegressionTest, PresenceGroupingDeduplicatesWholeBuildRows) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "k"), ValueType::kInt64),
                             Column(ColumnName("f", "v"), ValueType::kInt64)});
  right.schema = Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64),
                              Column(ColumnName("d", "v"), ValueType::kInt64)});
  left.rows = {Row({Value(1), Value(10)}), Row({Value(2), Value(20)}),
               Row({Value(3), Value()}), Row({Value(4), Value(1)})};
  for (size_t i = 0; i < 1024; ++i) {
    for (const auto& row :
         {Row({Value(1), Value(10)}), Row({Value(1), Value(11)}),
          Row({Value(2), Value(20)}), Row({Value(3), Value(30)}),
          Row({Value(4), Value()})}) {
      right.rows.push_back(row);
    }
  }
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {col("f", "k"), col("d", "v")};
  fusion.select_items = fusion.group_keys;
  fusion.select_names = {"k", "v"};
  fusion.eligible = true;
  const std::vector<Expression> predicates{
      BinaryExpressionExp(col("f", "k"), BinaryOperation::kEquals,
                          col("d", "k")),
      BinaryExpressionExp(col("f", "v"), BinaryOperation::kNotEquals,
                          col("d", "v"))};
  for (const auto join : {JoinType::kInner, JoinType::kLeft}) {
    auto output = relational_detail::TryFusedGroupedJoin(
        context, left, right, predicates, join, nullptr, {}, &fusion);
    ASSERT_TRUE(output.HasValue()) << output.GetStatus();
    auto relation = output.MoveValue();
    if (!relation) {
      FAIL() << "presence-only grouped join declined";
    }
    EXPECT_EQ(relation->rows.size(), join == JoinType::kInner ? 1U : 4U);
    EXPECT_EQ(std::ranges::count(relation->rows, Row({Value(1), Value(11)})),
              1);
    EXPECT_LT(relation->join_comparisons, 100U);
    if (join == JoinType::kLeft) {
      for (const int key : {2, 3, 4}) {
        EXPECT_EQ(
            std::ranges::count(relation->rows, Row({Value(key), Value()})), 1);
      }
    }
  }
  const auto count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("*"));
  fusion.aggregates = {count.get()};
  fusion.count_star = {true};
  fusion.select_items.emplace_back(count);
  fusion.select_names.emplace_back("n");
  auto counted = relational_detail::TryFusedGroupedJoin(
      context, left, right, predicates, JoinType::kInner, nullptr, {}, &fusion);
  ASSERT_TRUE(counted.HasValue()) << counted.GetStatus();
  auto counted_rows = counted.MoveValue();
  if (!counted_rows) {
    FAIL() << "counted grouped join declined";
  }
  EXPECT_EQ(counted_rows->rows,
            std::vector<Row>({Row({Value(1), Value(11), Value(1024)})}));
}

TEST_F(RelationalRegressionTest, FusedBuildCaseCacheKeepsNullAndCollation) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "k"), ValueType::kInt64),
                             Column(ColumnName("f", "v"), ValueType::kDouble)});
  right.schema =
      Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64),
                   Column(ColumnName("d", "tag"), ValueType::kVarChar)});
  right.rows = {Row({Value(1), Value("yes")}), Row({Value(2), Value("no")}),
                Row({Value(3), Value()}),
                Row({Value(4), Value("YES").WithCollation(2)})};
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  const auto child = CaseExpressionExp(
      {{BinaryExpressionExp(col("d", "tag"), BinaryOperation::kEquals,
                            ConstantValueExp(Value("yes"))),
        col("f", "v")}},
      ConstantValueExp(Value()));
  const auto sum =
      std::make_shared<AggregateExpression>(AggregationType::kSum, child);
  const auto avg =
      std::make_shared<AggregateExpression>(AggregationType::kAvg, child);
  const auto count =
      std::make_shared<AggregateExpression>(AggregationType::kCount, child);
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.aggregates = {sum.get(), avg.get(), count.get()};
  fusion.count_star = {false, false, false};
  fusion.select_names = {"k", "sum", "avg", "count"};
  fusion.eligible = true;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      col("f", "k"), BinaryOperation::kEquals, col("d", "k"))};
  for (const int64_t repeats : {1, 1024}) {
    left.rows.clear();
    for (int64_t i = 0; i < repeats; ++i) {
      for (int key = 1; key <= 5; ++key) {
        left.rows.emplace_back(std::vector<Value>{Value(key), Value(2.5)});
        left.rows.emplace_back(std::vector<Value>{Value(key), Value()});
      }
    }
    for (const auto* alias : {"f", "d"}) {
      fusion.group_keys = {col(alias, "k")};
      fusion.select_items = {fusion.group_keys[0], sum, avg, count};
      auto result = relational_detail::TryFusedGroupedJoin(
          context, left, right, predicates, JoinType::kLeft, nullptr, {},
          &fusion);
      ASSERT_TRUE(result.HasValue()) << result.GetStatus();
      auto output = result.MoveValue();
      if (!output) {
        FAIL() << "conditional grouped join declined";
      }
      ASSERT_EQ(output->rows.size(), 5U);
      for (int key = 1; key <= 5; ++key) {
        const bool accepted = key == 1 || key == 4;
        const Row expected(
            {key == 5 && std::string_view(alias) == "d" ? Value() : Value(key),
             accepted ? Value(2.5 * static_cast<double>(repeats)) : Value(),
             accepted ? Value(2.5) : Value(),
             Value(accepted ? repeats : int64_t{0})});
        EXPECT_EQ(std::ranges::count(output->rows, expected), 1);
      }
    }
  }
}

TEST_F(RelationalRegressionTest, BorrowedJoinKeysKeepBytesBiasAndDuplicates) {
  auto context = rs_->BeginContext();
  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget budget(&active, 4);
  const std::string binary("a\0b", 3);
  const std::string long_text(96, 'x');
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  const std::vector<Expression> predicates{
      BinaryExpressionExp(col("a", "s"), BinaryOperation::kEquals,
                          col("b", "s")),
      BinaryExpressionExp(
          col("a", "k"), BinaryOperation::kEquals,
          BinaryExpressionExp(col("b", "k"), BinaryOperation::kAdd,
                              ConstantValueExp(Value(1))))};
  for (const auto& [repeats, with_residual] :
       {std::pair{1, false}, std::pair{8192, false}, std::pair{8192, true}}) {
    relational_detail::Relation left, right;
    left.schema =
        Schema("a", {Column(ColumnName("a", "id"), ValueType::kInt64),
                     Column(ColumnName("a", "s"), ValueType::kVarChar),
                     Column(ColumnName("a", "k"), ValueType::kInt64)});
    right.schema =
        Schema("b", {Column(ColumnName("b", "id"), ValueType::kInt64),
                     Column(ColumnName("b", "s"), ValueType::kVarChar),
                     Column(ColumnName("b", "k"), ValueType::kInt64)});
    const std::vector<Row> pattern{
        Row({Value(1), Value(std::string(long_text)), Value(2)}),
        Row({Value(2), Value(std::string(long_text)), Value(2)}),
        Row({Value(3), Value(""), Value(0)}),
        Row({Value(4), Value(std::string(binary)), Value(-3)}),
        Row({Value(5), Value(), Value(2)}),
        Row({Value(6), Value("other"), Value()})};
    for (int i = 0; i < repeats; ++i) {
      left.rows.insert(left.rows.end(), pattern.begin(), pattern.end());
    }
    right.rows = {Row({Value(10), Value(std::string(long_text)), Value(1)}),
                  Row({Value(11), Value(std::string(long_text)), Value(1)}),
                  Row({Value(12), Value(""), Value(-1)}),
                  Row({Value(13), Value(std::string(binary)), Value(-4)}),
                  Row({Value(14), Value(), Value(1)}),
                  Row({Value(15), Value(""), Value()}),
                  Row({Value(16), Value("a"), Value(-4)})};
    auto active_predicates = predicates;
    if (with_residual) {
      active_predicates.push_back(
          BinaryExpressionExp(col("a", "id"), BinaryOperation::kGreaterThan,
                              ConstantValueExp(Value(1))));
    }
    auto result =
        relational_detail::InnerJoin(context, std::move(left), std::move(right),
                                     active_predicates, nullptr, {});
    ASSERT_TRUE(result.HasValue()) << result.GetStatus();
    std::map<std::pair<int64_t, int64_t>, int> counts;
    for (const auto& row : result.Value().rows) {
      ++counts[{row[0].value.int_value, row[3].value.int_value}];
    }
    std::map<std::pair<int64_t, int64_t>, int> expected{
        {{1, 10}, repeats}, {{1, 11}, repeats}, {{2, 10}, repeats},
        {{2, 11}, repeats}, {{3, 12}, repeats}, {{4, 13}, repeats}};
    if (with_residual) {
      expected.erase({1, 10});
      expected.erase({1, 11});
    }
    EXPECT_EQ(counts, expected);
  }
}

TEST_F(RelationalRegressionTest, PackedOuterKeysKeepRangesNullsAndDuplicates) {
  constexpr int kGroups = 1024;
  constexpr int kRows = 20480;
  const auto maximum = std::numeric_limits<int64_t>::max();
  const auto minimum = std::numeric_limits<int64_t>::min();
  auto context = rs_->BeginContext();
  for (bool wide : {false, true}) {
    for (size_t workers : {1U, 2U}) {
      for (JoinType kind : {JoinType::kLeft, JoinType::kFull}) {
        relational_detail::Relation left, right;
        left.schema = Schema("l", {Column("ld", ValueType::kDate),
                                   Column("la", ValueType::kInt64),
                                   Column("lc", ValueType::kInt64),
                                   Column("lid", ValueType::kInt64)});
        right.schema = Schema("r", {Column("ra", ValueType::kInt64),
                                    Column("rd", ValueType::kDate),
                                    Column("rc", ValueType::kInt64),
                                    Column("rid", ValueType::kInt64)});
        for (int g = 0; g < kGroups; ++g) {
          for (int duplicate = 0; duplicate < 2; ++duplicate) {
            right.AddRow(
                Row({Value(g - 500), Value::DateFromDays(g % 3),
                     Value(maximum - (g % 5)), Value((g * 2) + duplicate)}));
          }
        }
        // One unmatched row expands a single field to 64 bits in the wide
        // run, exercising the ordinary composite-key fallback.
        const Row extra({Value(50000), Value::DateFromDays(0),
                         Value(wide ? minimum : maximum), Value(-1)});
        const Row null_right(
            {Value(), Value::DateFromDays(0), Value(maximum), Value(-2)});
        right.AddRow(extra);
        right.AddRow(null_right);
        std::map<std::string, size_t> expected;
        for (int i = 0; i < kRows; ++i) {
          const int g = i % 1200;
          const bool null_key = i % 17 == 0;
          const bool outside = i % 13 == 0;
          const Row row(
              {Value::DateFromDays(g % 3), null_key ? Value() : Value(g - 501),
               Value(outside ? minimum : maximum - (g % 5)), Value(i)});
          left.AddRow(row);
          if (!null_key && !outside && g < kGroups) {
            for (int duplicate = 0; duplicate < 2; ++duplicate) {
              ++expected[(row + right.rows[(static_cast<size_t>(g) * 2) +
                                           static_cast<size_t>(duplicate)])
                             .ToString()];
            }
          } else {
            auto unmatched = row;
            unmatched.values_.resize(8);
            ++expected[unmatched.ToString()];
          }
        }
        if (kind == JoinType::kFull) {
          ++expected[(Row(std::vector<Value>(4)) + extra).ToString()];
          ++expected[(Row(std::vector<Value>(4)) + null_right).ToString()];
        }
        const auto equality = [](Expression l, const char* r) {
          return BinaryExpressionExp(std::move(l), BinaryOperation::kEquals,
                                     ColumnValueExp(r));
        };
        SelectSource source;
        source.join_type = kind;
        source.join_condition = BinaryExpressionExp(
            equality(
                BinaryExpressionExp(ColumnValueExp("la"), BinaryOperation::kAdd,
                                    ConstantValueExp(Value(1))),
                "ra"),
            BinaryOperation::kAnd,
            BinaryExpressionExp(equality(ColumnValueExp("ld"), "rd"),
                                BinaryOperation::kAnd,
                                equality(ColumnValueExp("lc"), "rc")));
        std::atomic<size_t> active{0};
        ScopedQueryParallelismBudget budget(&active, workers);
        ASSIGN_OR_ASSERT_FAIL(
            relational_detail::Relation, result,
            relational_detail::Join(context, std::move(left), std::move(right),
                                    source, nullptr, {}));
        std::map<std::string, size_t> actual;
        ASSERT_SUCCESS(result.ForEachRow(
            [&](const Row& row) { ++actual[row.ToString()]; }));
        EXPECT_EQ(actual, expected);
      }
    }
  }
}

TEST_F(RelationalRegressionTest, ParallelJoinConsumesOnlyLastMatchBuffer) {
  auto context = rs_->BeginContext();
  std::atomic<size_t> active{0};
  ScopedQueryParallelismBudget budget(&active, 4);
  const std::string text(96, 'p');
  const Value array = Value::Array({Value(3), Value(), Value("nested")}, "ANY");
  for (const auto kind : {JoinType::kInner, JoinType::kLeft, JoinType::kFull}) {
    for (const bool flip : {false, true}) {
      SCOPED_TRACE(static_cast<int>(kind));
      SCOPED_TRACE(flip);
      relational_detail::Relation left, right;
      left.schema = Schema("a", {Column("ak", ValueType::kInt64),
                                 Column("at", ValueType::kVarChar),
                                 Column("av", ValueType::kArray)});
      right.schema = Schema("b", {Column("bk", ValueType::kInt64),
                                  Column("bid", ValueType::kInt64)});
      constexpr int kRepeats = 4096;
      for (int i = 0; i < kRepeats; ++i) {
        for (int k = 0; k < 5; ++k) {
          Row row(
              {k == 0 ? Value() : Value(k), Value(std::string(text)), array});
          // Exercise both reuse with sufficient capacity and reallocation.
          if (i % 2 == 0) {
            row.values_.reserve(8);
          }
          left.AddRow(std::move(row));
        }
      }
      right.rows = {Row({Value(1), Value(10)}), Row({Value(1), Value(11)}),
                    Row({Value(2), Value(12)}), Row({Value(6), Value(16)}),
                    Row({Value(), Value(19)})};
      if (flip && kind == JoinType::kInner) {
        std::swap(left, right);
      }
      SelectSource source;
      source.join_type = kind;
      source.join_condition = BinaryExpressionExp(
          ColumnValueExp("ak"), BinaryOperation::kEquals, ColumnValueExp("bk"));
      auto result =
          kind == JoinType::kInner
              ? relational_detail::InnerJoin(
                    context, std::move(left), std::move(right),
                    {source.join_condition}, nullptr, {})
              : relational_detail::Join(context, std::move(left),
                                        std::move(right), source, nullptr, {});
      ASSERT_TRUE(result.HasValue()) << result.GetStatus();
      const size_t a = flip && kind == JoinType::kInner ? 2 : 0;
      const size_t b = flip && kind == JoinType::kInner ? 0 : 3;
      std::map<std::pair<int64_t, int64_t>, int> counts;
      for (const auto& row : result.Value().rows) {
        const int64_t key = row[a].IsNull() ? -1 : row[a].value.int_value;
        const int64_t id =
            row[b + 1].IsNull() ? -1 : row[b + 1].value.int_value;
        ++counts[{key, id}];
        if (id != 16 && id != 19) {
          EXPECT_EQ(row[a + 1], Value(std::string(text)));
          EXPECT_EQ(row[a + 2], array);
        } else {
          EXPECT_TRUE(row[a + 1].IsNull());
          EXPECT_TRUE(row[a + 2].IsNull());
        }
      }
      std::map<std::pair<int64_t, int64_t>, int> expected{
          {{1, 10}, kRepeats}, {{1, 11}, kRepeats}, {{2, 12}, kRepeats}};
      if (kind != JoinType::kInner) {
        expected[{3, -1}] = kRepeats;
        expected[{4, -1}] = kRepeats;
        expected[{-1, -1}] = kRepeats;
      }
      if (kind == JoinType::kFull) {
        expected[{-1, 16}] = 1;
        expected[{-1, 19}] = 1;
      }
      EXPECT_EQ(counts, expected);
    }
  }
}

TEST_F(RelationalRegressionTest, BorrowedJoinKeysDoNotHideBiasedOverflow) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("a", {Column(ColumnName("a", "s"), ValueType::kVarChar),
                             Column(ColumnName("a", "k"), ValueType::kInt64)});
  right.schema = Schema("b", {Column(ColumnName("b", "s"), ValueType::kVarChar),
                              Column(ColumnName("b", "k"), ValueType::kInt64)});
  left.rows = {
      Row({Value("same"), Value(std::numeric_limits<int64_t>::min())})};
  right.rows = {
      Row({Value("same"), Value(std::numeric_limits<int64_t>::max())})};
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  const std::vector<Expression> predicates{
      BinaryExpressionExp(col("a", "s"), BinaryOperation::kEquals,
                          col("b", "s")),
      BinaryExpressionExp(
          col("a", "k"), BinaryOperation::kEquals,
          BinaryExpressionExp(col("b", "k"), BinaryOperation::kAdd,
                              ConstantValueExp(Value(1))))};
  auto result = relational_detail::InnerJoin(
      context, std::move(left), std::move(right), predicates, nullptr, {});
  EXPECT_FALSE(result.HasValue());
}

TEST_F(RelationalRegressionTest, UnobservedLeftBuildNeedsActualUniqueness) {
  RelationalRun(*rs_,
                "CREATE TABLE ElideFact (a INT64,b INT64,g INT64,v INT64)");
  RelationalRun(*rs_, "CREATE TABLE ElideDim (a INT64,b INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          setup.GetTable("ElideFact"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                          setup.GetTable("ElideDim"));
    for (int i = 0; i < 2048; ++i) {
      ASSERT_SUCCESS(dim->Insert(setup.txn_, Row({Value(i / 4), Value(i % 4)}))
                         .GetStatus());
    }
    for (int i = 0; i < 8192; ++i) {
      const int key = i % 2500;
      ASSERT_SUCCESS(
          fact->Insert(setup.txn_,
                       Row({i % 17 == 0 ? Value() : Value(key / 4),
                            Value(key % 4), Value(i % 31),
                            i % 5 == 0 ? Value() : Value((i % 7) - 3)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  for (bool duplicates : {false, true}) {
    if (duplicates) {
      RelationalRun(*rs_,
                    "INSERT INTO ElideDim VALUES (0,3),(NULL,0),(NULL,0)");
    }
    std::vector<int64_t> sums(31), counts(31), matches(31);
    for (int i = 0; i < 8192; ++i) {
      const int key = i % 2500;
      const auto group = static_cast<size_t>(i % 31);
      const bool matched = key < 2048 && i % 17 != 0;
      const int64_t copies = matched && duplicates && key == 3 ? 2 : 1;
      counts[group] += copies;
      matches[group] += matched ? copies : 0;
      if (i % 5 != 0) {
        sums[group] += copies * ((i % 7) - 3);
      }
    }
    for (size_t workers : {1U, 2U}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      for (bool observed : {false, true}) {
        const std::string sql =
            "SELECT f.g,SUM(f.v),COUNT(" + std::string(observed ? "d.a" : "*") +
            ") FROM ElideFact f LEFT JOIN ElideDim d ON f.a=d.a AND f.b=d.b "
            "GROUP BY f.g ORDER BY f.g";
        const auto rows = RelationalRun(*rs_, sql);
        ASSERT_EQ(rows.size(), 31U);
        for (size_t g = 0; g < rows.size(); ++g) {
          EXPECT_EQ(rows[g], Row({Value(g), Value(sums[g]),
                                  Value(observed ? matches[g] : counts[g])}));
        }
      }
    }
  }
}

TEST_F(RelationalRegressionTest, ExactIntegerRollupReusesWeightedFineGroups) {
  RelationalRun(*rs_, "CREATE TABLE RollupDim (k INT64, label STRING)");
  for (const bool wide : {false, true}) {
    const std::string name = wide ? "RollupWide" : "RollupBounded";
    RelationalRun(*rs_,
                  "CREATE TABLE " + name + " (k INT64, g INT64, v INT64)");
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact, setup.GetTable(name));
    for (int64_t i = 0; i < 32768; ++i) {
      const Value value = i % 5 == 0 || i % 128 == 0
                              ? Value()
                              : Value(wide ? int64_t{1} << 54 : (i % 31) - 15);
      ASSERT_SUCCESS(
          fact->Insert(setup.txn_,
                       Row({Value(i % 128), Value((i / 128) % 7), value}))
              .GetStatus());
    }
    if (!wide) {
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                            setup.GetTable("RollupDim"));
      for (int64_t key = 0; key < 127; ++key) {
        ASSERT_SUCCESS(
            dim->Insert(
                   setup.txn_,
                   Row({Value(key),
                        key == 0 ? Value() : Value(std::to_string(key % 8))}))
                .GetStatus());
      }
      ASSERT_SUCCESS(
          dim->Insert(setup.txn_, Row({Value(7), Value("7")})).GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
    for (const size_t workers : {size_t{1}, size_t{4}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      // Different predicate literals also exercise template rebinding; empty
      // input must still emit the NULL grand total, and duplicate sets survive.
      for (const int minimum : {0, 2, 99}) {
        const std::string source =
            " FROM " + name + " f JOIN RollupDim d ON f.k=d.k WHERE f.g >= " +
            std::to_string(minimum);
        const std::string query =
            "SELECT d.label, f.g, AVG(f.v) AS a" + source +
            " GROUP BY GROUPING SETS ((d.label,f.g),(d.label),(d.label),())";
        const auto got = RelationalRun(*rs_, query);
        std::vector<Row> expected =
            RelationalRun(*rs_, "SELECT d.label, f.g, AVG(f.v) AS a" + source +
                                    " GROUP BY d.label,f.g");
        const auto subtotal =
            RelationalRun(*rs_, "SELECT d.label, NULL, AVG(f.v) AS a" + source +
                                    " GROUP BY d.label");
        expected.insert(expected.end(), subtotal.begin(), subtotal.end());
        expected.insert(expected.end(), subtotal.begin(), subtotal.end());
        const auto total =
            RelationalRun(*rs_, "SELECT NULL, NULL, AVG(f.v) AS a" + source);
        expected.insert(expected.end(), total.begin(), total.end());
        ASSERT_EQ(got.size(), expected.size());
        std::vector<bool> matched(expected.size(), false);
        for (const Row& row : got) {
          size_t index = 0;
          while (index < expected.size() &&
                 (matched[index] || row != expected[index])) {
            ++index;
          }
          ASSERT_LT(index, expected.size()) << row;
          matched[index] = true;
        }
        if (!wide && minimum == 0) {
          EXPECT_NE(ExplainAnalyze(*rs_, query).find("grouped_join_fusions=1"),
                    std::string::npos);
        }
      }
    }
  }
}

TEST_F(RelationalRegressionTest, ExactPartialTotalsCheckActualValues) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "k"), ValueType::kInt64),
                             Column(ColumnName("f", "v"), ValueType::kInt64)});
  right.schema = Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64)});
  right.rows.emplace_back(std::vector<Value>{Value(1)});
  right.rows.emplace_back(std::vector<Value>{Value(1)});
  const auto key = ColumnValueExp(ColumnName("f", "k"));
  const auto value = ColumnValueExp(ColumnName("f", "v"));
  const auto sum =
      std::make_shared<AggregateExpression>(AggregationType::kSum, value);
  const auto count =
      std::make_shared<AggregateExpression>(AggregationType::kCount, value);
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {key};
  fusion.aggregates = {sum.get(), count.get()};
  fusion.count_star = {false, false};
  fusion.select_items = {key, sum, count};
  fusion.select_names = {"k", "s", "n"};
  fusion.eligible = true;
  fusion.require_exact_integer_totals = true;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      key, BinaryOperation::kEquals, ColumnValueExp(ColumnName("d", "k")))};
  for (const Value& input :
       {Value(int64_t{1} << 52), Value((int64_t{1} << 52) + 1),
        Value(-(int64_t{1} << 52)), Value(std::numeric_limits<int64_t>::min()),
        Value(1.0), Value(int64_t{1}).WithUnsigned(), Value()}) {
    left.rows = {Row({Value(1), input})};
    ASSIGN_OR_ASSERT_FAIL(std::optional<relational_detail::Relation>, output,
                          relational_detail::TryFusedGroupedJoin(
                              context, left, right, predicates,
                              JoinType::kInner, nullptr, {}, &fusion));
    const bool exact =
        input.IsNull() ||
        (input.type == ValueType::kInt64 && !input.IsUnsigned() &&
         (input.value.int_value == (int64_t{1} << 52) ||
          input.value.int_value == -(int64_t{1} << 52)));
    ASSERT_EQ(output.has_value(), exact) << input;
    if (output) {
      ASSERT_EQ(output->rows.size(), 1U);
      EXPECT_EQ(output->rows[0][1],
                input.IsNull() ? Value() : Value(input.value.int_value * 2));
      EXPECT_EQ(output->rows[0][2], Value(input.IsNull() ? 0 : 2));
    }
    EXPECT_EQ(left.rows[0][1], input);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, LargeBuildGroupsMergeParallelAggregateStates) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "k"), ValueType::kInt64),
                             Column(ColumnName("f", "v"), ValueType::kDouble)});
  right.schema = Schema("d", {Column(ColumnName("d", "k"), ValueType::kInt64)});
  constexpr int64_t kGroups = 8192;
  for (int64_t k = 0; k < kGroups; ++k) {
    right.rows.emplace_back(std::vector<Value>{Value(k)});
  }
  for (int repeat = 0; repeat < 8; ++repeat) {
    for (int64_t k = 0; k < kGroups; ++k) {
      left.rows.emplace_back(std::vector<Value>{
          Value(k), repeat % 2 == 0 || k == 0 ? Value() : Value(2.5)});
    }
  }
  const auto key = ColumnValueExp(ColumnName("d", "k"));
  const auto value = ColumnValueExp(ColumnName("f", "v"));
  const auto sum =
      std::make_shared<AggregateExpression>(AggregationType::kSum, value);
  const auto avg =
      std::make_shared<AggregateExpression>(AggregationType::kAvg, value);
  const auto count =
      std::make_shared<AggregateExpression>(AggregationType::kCount, value);
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {key};
  fusion.aggregates = {sum.get(), avg.get(), count.get()};
  fusion.count_star = {false, false, false};
  fusion.select_items = {key, sum, avg, count};
  fusion.select_names = {"k", "s", "a", "n"};
  fusion.eligible = true;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      ColumnValueExp(ColumnName("f", "k")), BinaryOperation::kEquals, key)};
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto result = relational_detail::TryFusedGroupedJoin(
        context, left, right, predicates, JoinType::kInner, nullptr, {},
        &fusion);
    ASSERT_TRUE(result.HasValue()) << result.GetStatus();
    auto output = result.MoveValue();
    if (!output) {
      FAIL() << "grouped join declined";
    }
    ASSERT_EQ(output->rows.size(), static_cast<size_t>(kGroups));
    for (const auto& row : output->rows) {
      const bool all_null = row[0] == Value(0);
      EXPECT_EQ(row[1], all_null ? Value() : Value(10.0));
      EXPECT_EQ(row[2], all_null ? Value() : Value(2.5));
      EXPECT_EQ(row[3], Value(all_null ? 0 : 4));
    }
  }
  for (const bool multiply : {false, true}) {
    const auto argument =
        multiply ? BinaryExpressionExp(value, BinaryOperation::kMultiply, value)
                 : value;
    const auto single_sum =
        std::make_shared<AggregateExpression>(AggregationType::kSum, argument);
    fusion.aggregates = {single_sum.get()};
    fusion.count_star = {false};
    fusion.select_items = {key, single_sum};
    fusion.select_names = {"k", "s"};
    for (const size_t workers : {size_t{1}, size_t{2}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      auto result = relational_detail::TryFusedGroupedJoin(
          context, left, right, predicates, JoinType::kInner, nullptr, {},
          &fusion);
      ASSERT_TRUE(result.HasValue()) << result.GetStatus();
      auto output = result.MoveValue();
      if (!output) {
        FAIL() << "single SUM grouped join declined";
      }
      ASSERT_EQ(output->rows.size(), static_cast<size_t>(kGroups));
      for (const auto& row : output->rows) {
        EXPECT_EQ(row[1],
                  row[0] == Value(0) ? Value() : Value(multiply ? 25.0 : 10.0));
      }
    }
  }
  const std::array<double, 8> ordered_values{1e16, 1.0, -1e16, 1.0,
                                             0.0,  0.0, 0.0,   0.0};
  for (size_t repeat = 0; repeat < ordered_values.size(); ++repeat) {
    left.rows[(repeat * static_cast<size_t>(kGroups)) + 1].values_[1] =
        Value(ordered_values[repeat]);
  }
  fusion.aggregates = {sum.get()};
  fusion.count_star = {false};
  fusion.select_items = {key, sum};
  fusion.select_names = {"k", "s"};
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto result = relational_detail::TryFusedGroupedJoin(
        context, left, right, predicates, JoinType::kInner, nullptr, {},
        &fusion);
    ASSERT_TRUE(result.HasValue()) << result.GetStatus();
    auto output = result.MoveValue();
    if (!output) {
      FAIL() << "ordered SUM grouped join declined";
    }
    for (const auto& row : output->rows) {
      if (row[0] == Value(1)) {
        EXPECT_EQ(row[1], Value(1.0));
      }
    }
  }
}

TEST_F(RelationalRegressionTest,
       LargeOrderedFilterPreservesDuplicatesAndNulls) {
  RelationalRun(*rs_, "CREATE TABLE OrderedFilterDim(k INT64,keep INT64)");
  RelationalRun(*rs_, "CREATE TABLE OrderedFilterFact(k INT64,p INT64)");
  RelationalRun(*rs_, "CREATE TABLE OrderedFilterPad(k INT64)");
  constexpr int64_t kDimension = 40000;
  constexpr int64_t kFact = 160000;
  int64_t expected_count = 0, expected_matched = 0;
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                          context.GetTable("OrderedFilterDim"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          context.GetTable("OrderedFilterFact"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, pad,
                          context.GetTable("OrderedFilterPad"));
    for (int64_t k = 0; k < kDimension; ++k) {
      ASSERT_SUCCESS(
          dim->Insert(context.txn_, Row({Value(k), Value(k % 8 == 7 ? 1 : 0)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(
        dim->Insert(context.txn_, Row({Value(7), Value(1)})).GetStatus());
    ASSERT_SUCCESS(
        dim->Insert(context.txn_, Row({Value(), Value(1)})).GetStatus());
    for (int64_t k = 0; k < 8; ++k) {
      ASSERT_SUCCESS(pad->Insert(context.txn_, Row({Value(k)})).GetStatus());
    }
    for (int64_t i = 0; i < kFact; ++i) {
      const bool null_key = i % 101 == 0;
      const bool missing = i % 127 == 0;
      const bool null_pad = i % 107 == 0;
      const int64_t key = missing ? kDimension + 1000 : i % kDimension;
      ASSERT_SUCCESS(
          fact->Insert(context.txn_, Row({null_key ? Value() : Value(key),
                                          null_pad ? Value() : Value(i % 17)}))
              .GetStatus());
      if (!null_key && !missing && key % 8 == 7) {
        const int64_t copies = key == 7 ? 2 : 1;
        expected_count += copies;
        if (!null_pad && i % 17 < 8) {
          expected_matched += copies;
        }
      }
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE");
  const std::string sql =
      "SELECT COUNT(*),COUNT(p.k) FROM OrderedFilterFact f "
      "JOIN OrderedFilterDim d ON f.k=d.k LEFT JOIN OrderedFilterPad p ON "
      "f.p=p.k "
      "WHERE d.keep=1";
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    EXPECT_EQ(RelationalRun(*rs_, sql),
              std::vector<Row>(
                  {Row({Value(expected_count), Value(expected_matched)})}));
    const auto profile = ExplainAnalyze(*rs_, sql);
    const std::string field = "scan_output_rows=";
    const auto position = profile.find(field);
    ASSERT_NE(position, std::string::npos) << profile;
    EXPECT_LT(std::stoull(profile.substr(position + field.size())), 100000U)
        << profile;
  }
}

TEST_F(RelationalRegressionTest, RelativeDimensionPreloadKeepsDeferredFact) {
  RelationalRun(*rs_,
                "CREATE TABLE PreloadDim (k INT64,g INT64,label STRING(MAX))");
  RelationalRun(*rs_, "CREATE TABLE PreloadDate (k INT64,y INT64,z INT64)");
  RelationalRun(*rs_, "CREATE TABLE PreloadFact (dk INT64,dt INT64,v INT64)");
  constexpr int64_t kDim = 40000;
  constexpr int64_t kFact = kDim * 4;
  constexpr std::array<std::string_view, 3> kLabels{"0", "1", "2"};
  std::map<std::pair<int64_t, std::string_view>, int64_t> counts;
  std::map<std::tuple<int64_t, std::string_view, int64_t>, int64_t> wide_counts;
  {
    auto context = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dim,
                          context.GetTable("PreloadDim"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, date,
                          context.GetTable("PreloadDate"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          context.GetTable("PreloadFact"));
    for (int64_t i = 0; i < kDim; ++i) {
      ASSERT_TRUE(dim->Insert(context.txn_, Row({Value(i), Value(i % 7),
                                                 Value(std::to_string(i % 3))}))
                      .HasValue());
    }
    for (int64_t i = 0; i < 100; ++i) {
      ASSERT_TRUE(
          date->Insert(context.txn_, Row({Value(i), Value(i / 10),
                                          i % 3 == 0 ? Value() : Value(i % 3)}))
              .HasValue());
    }
    for (int64_t i = 0; i < kFact; ++i) {
      const bool null_key = i % 101 == 0;
      const int64_t key = i % kDim;
      ASSERT_TRUE(
          fact->Insert(context.txn_, Row({null_key ? Value() : Value(key),
                                          Value(i % 100), Value(1)}))
              .HasValue());
      if (!null_key && (i % 100) / 10 == 1) {
        const auto label = kLabels[static_cast<size_t>(key % 3)];
        ++counts[{key % 7, label}];
        ++wide_counts[{key % 7, label,
                       ((i % 100) % 3 == 0 ? -1 : (i % 100) % 3)}];
      }
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE");
  const std::string sql =
      "SELECT d.g,d.label,t.y,SUM(f.v) FROM PreloadDim d,PreloadFact "
      "f,PreloadDate t "
      "WHERE d.k=f.dk AND t.k=f.dt AND t.y=1 GROUP BY d.g,d.label,t.y "
      "ORDER BY d.g,d.label,t.y";
  std::vector<Row> expected;
  expected.reserve(counts.size());
  for (const auto& [key, count] : counts) {
    expected.emplace_back(Row({Value(key.first), Value(std::string(key.second)),
                               Value(1), Value(count)}));
  }
  const std::string wide_sql =
      "SELECT d.g,d.label,t.y,t.z,SUM(f.v) FROM PreloadDim d,PreloadFact "
      "f,PreloadDate t "
      "WHERE d.k=f.dk AND t.k=f.dt AND t.y=1 GROUP BY d.g,d.label,t.y,t.z "
      "ORDER BY d.g,d.label,t.y,t.z";
  const auto wide_expected = [&] {
    std::vector<Row> rows;
    rows.reserve(wide_counts.size());
    for (const auto& [key, count] : wide_counts) {
      rows.emplace_back(Row(
          {Value(std::get<0>(key)), Value(std::string(std::get<1>(key))),
           Value(1), std::get<2>(key) == -1 ? Value() : Value(std::get<2>(key)),
           Value(count)}));
    }
    return rows;
  };
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    std::atomic<size_t> active{1};
    ScopedQueryParallelismBudget budget(&active, workers);
    EXPECT_EQ(RelationalRun(*rs_, sql), expected);
    EXPECT_EQ(RelationalRun(*rs_, wide_sql), wide_expected());
    const auto profile = ExplainAnalyze(*rs_, wide_sql);
    const std::string field = "scan_rows=";
    const auto position = profile.find(field);
    ASSERT_NE(position, std::string::npos) << profile;
    EXPECT_LT(std::stoull(profile.substr(position + field.size())),
              static_cast<uint64_t>(kDim + 1000))
        << profile;
  }
  // A duplicate dimension key must retain join multiplicity via fallback.
  RelationalRun(*rs_, "INSERT INTO PreloadDate VALUES (10,1,1)");
  for (int64_t i = 10; i < kFact; i += 100) {
    if (i % 101 != 0) {
      const int64_t key = i % kDim;
      ++wide_counts[{key % 7, std::to_string(key % 3), 1}];
    }
  }
  EXPECT_EQ(RelationalRun(*rs_, wide_sql), wide_expected());
}

TEST_F(RelationalRegressionTest, RawPartitionMergeKeepsCompositeSums) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "id"), ValueType::kInt64),
                             Column(ColumnName("f", "g"), ValueType::kInt64),
                             Column(ColumnName("f", "v"), ValueType::kDouble)});
  right.schema =
      Schema("d", {Column(ColumnName("d", "id"), ValueType::kInt64),
                   Column(ColumnName("d", "s"), ValueType::kVarChar)});
  right.rows = {Row({Value(1), Value("x")}), Row({Value(1), Value("x")}),
                Row({Value(2), Value()})};
  constexpr int64_t kGroups = 32768;
  for (int64_t repeat = 0; repeat < 4; ++repeat) {
    for (int64_t i = 0; i < kGroups; ++i) {
      left.rows.emplace_back(Row(
          {Value((i % 2) + 1), Value(i), i % 5 == 0 ? Value() : Value(0.25)}));
    }
  }
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  const auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                         col("f", "v"));
  const auto count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, col("f", "v"));
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {col("f", "g"), col("d", "s")};
  fusion.aggregates = {sum.get(), count.get()};
  fusion.count_star = {false, false};
  fusion.select_items = {col("f", "g"), col("d", "s"), sum, count};
  fusion.select_names = {"g", "s", "sum", "count"};
  fusion.eligible = true;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      col("f", "id"), BinaryOperation::kEquals, col("d", "id"))};
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    auto result = relational_detail::TryFusedGroupedJoin(
        context, left, right, predicates, JoinType::kInner, nullptr, {},
        &fusion);
    ASSERT_TRUE(result.HasValue()) << result.GetStatus();
    auto output = result.MoveValue();
    if (!output) {
      FAIL() << "expected specialized aggregation";
    }
    ASSERT_EQ(output->rows.size(), static_cast<size_t>(kGroups));
    EXPECT_EQ(runtime.partitioned_aggregate_merges, workers == 1 ? 0U : 1U);
    std::vector<bool> seen(kGroups, false);
    for (const auto& row : output->rows) {
      const int64_t group = row[0].value.int_value;
      ASSERT_GE(group, 0);
      ASSERT_LT(group, kGroups);
      EXPECT_FALSE(seen[static_cast<size_t>(group)]);
      seen[static_cast<size_t>(group)] = true;
      const bool null_value = group % 5 == 0;
      const int64_t multiplicity = group % 2 == 0 ? 2 : 1;
      EXPECT_EQ(row[1], group % 2 == 0 ? Value("x") : Value());
      EXPECT_EQ(row[2], null_value ? Value()
                                   : Value(static_cast<double>(multiplicity)));
      EXPECT_EQ(row[3], Value(null_value ? 0 : multiplicity * 4));
    }
  }
}

TEST_F(RelationalRegressionTest, FusedSplitKeysMergeAcrossWorkers) {
  using relational_detail::Relation;
  auto context = rs_->BeginContext();
  Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "id"), ValueType::kInt64),
                             Column(ColumnName("f", "p"), ValueType::kInt64),
                             Column(ColumnName("f", "v"), ValueType::kInt64)});
  right.schema =
      Schema("d", {Column(ColumnName("d", "id"), ValueType::kInt64),
                   Column(ColumnName("d", "s"), ValueType::kVarChar),
                   Column(ColumnName("d", "t"), ValueType::kVarChar)});
  right.rows = {Row({Value(1), Value("same"), Value("r")}),
                Row({Value(2), Value("same"), Value("r")}),
                Row({Value(3), Value(), Value("r")}),
                Row({Value(3), Value(), Value("r")})};
  const auto wide = std::numeric_limits<int64_t>::max();
  const std::vector<Row> pattern{Row({Value(1), Value(0), Value(10)}),
                                 Row({Value(2), Value(0), Value(20)}),
                                 Row({Value(3), Value(0), Value(4)}),
                                 Row({Value(4), Value(0), Value(7)}),
                                 Row({Value(1), Value(), Value(2)}),
                                 Row({Value(2), Value(), Value(3)}),
                                 Row({Value(1), Value(wide), Value(6)})};
  constexpr int64_t kRepeats = 8192;
  for (int64_t i = 0; i < kRepeats; ++i) {
    left.rows.insert(left.rows.end(), pattern.begin(), pattern.end());
  }
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  const auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                         col("f", "v"));
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {col("f", "p"), col("d", "s"), col("d", "t")};
  fusion.aggregates = {sum.get()};
  fusion.count_star = {false};
  fusion.select_items = fusion.group_keys;
  fusion.select_items.push_back(sum);
  fusion.select_names = {"p", "s", "t", "n"};
  fusion.eligible = true;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      col("f", "id"), BinaryOperation::kEquals, col("d", "id"))};
  std::vector<Row> expected{
      Row({Value(), Value("same"), Value("r"), Value(5 * kRepeats)}),
      Row({Value(0), Value(), Value(), Value(7 * kRepeats)}),
      Row({Value(0), Value(), Value("r"), Value(8 * kRepeats)}),
      Row({Value(0), Value("same"), Value("r"), Value(30 * kRepeats)}),
      Row({Value(wide), Value("same"), Value("r"), Value(6 * kRepeats)})};
  const auto verify = [&] {
    for (const size_t workers : {size_t{1}, size_t{2}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      auto result = relational_detail::TryFusedGroupedJoin(
          context, left, right, predicates, JoinType::kLeft, nullptr, {},
          &fusion);
      ASSERT_TRUE(result.HasValue()) << result.GetStatus();
      auto output = result.MoveValue();
      if (!output) {
        FAIL() << "fused split-key aggregation declined";
      }
      ASSIGN_OR_ASSERT_FAIL(Relation, materialized,
                            relational_detail::MaterializeRelation(*output));
      *output = std::move(materialized);
      ASSERT_EQ(output->rows.size(), expected.size());
      for (const auto& row : expected) {
        EXPECT_TRUE(std::ranges::any_of(output->rows, [&](const Row& actual) {
          if (actual.Count() != row.Count()) {
            return false;
          }
          for (size_t column = 0; column < row.Count(); ++column) {
            if (!relational_detail::DistinctValueEqual{}(
                    relational_detail::CanonicalDistinctValue(actual[column]),
                    relational_detail::CanonicalDistinctValue(row[column]))) {
              return false;
            }
          }
          return true;
        }));
      }
    }
  };
  verify();
  // NULL and out-of-range probe keys use the fallback map even when the
  // dimension has no row representation. Keep its distinct dimension keys.
  ASSIGN_OR_ASSERT_FAIL(
      std::shared_ptr<relational_detail::ColumnarTable>, columnar_right,
      relational_detail::BuildColumnarTableFromRows(right.schema, right.rows));
  auto saved_right = std::move(right.rows);
  right.columnar = std::move(columnar_right);
  verify();
  ASSIGN_OR_ASSERT_FAIL(
      std::shared_ptr<relational_detail::ColumnarTable>, columnar_left,
      relational_detail::BuildColumnarTableFromRows(left.schema, left.rows));
  auto saved_left = std::move(left.rows);
  left.columnar = std::move(columnar_left);
  verify();
  left.columnar.reset();
  left.rows = std::move(saved_left);
  right.columnar.reset();
  right.rows = std::move(saved_right);
  const double nan = std::numeric_limits<double>::quiet_NaN();
  right.schema =
      Schema("d", {Column(ColumnName("d", "id"), ValueType::kInt64),
                   Column(ColumnName("d", "s"), ValueType::kDouble),
                   Column(ColumnName("d", "t"), ValueType::kVarChar)});
  right.rows[0].values_[1] = Value(nan);
  right.rows[1].values_[1] = Value(-nan);
  right.rows[0].values_[2] = Value("UPPER").WithCollation(2);
  right.rows[1].values_[2] = Value("upper").WithCollation(2);
  for (const size_t index : {size_t{0}, size_t{3}, size_t{4}}) {
    expected[index].values_[1] = Value(nan);
    expected[index].values_[2] = Value("upper").WithCollation(2);
  }
  verify();
}

TEST_F(RelationalRegressionTest, ColumnarSplitKeysKeepFallbackDimension) {
  using relational_detail::Relation;
  auto context = rs_->BeginContext();
  Relation left, right;
  left.schema = Schema("f", {Column(ColumnName("f", "id"), ValueType::kInt64),
                             Column(ColumnName("f", "p"), ValueType::kInt64),
                             Column(ColumnName("f", "v"), ValueType::kDouble)});
  right.schema =
      Schema("d", {Column(ColumnName("d", "id"), ValueType::kInt64),
                   Column(ColumnName("d", "s"), ValueType::kVarChar)});
  right.rows = {Row({Value(1), Value("a")}), Row({Value(2), Value("b")})};
  const auto wide = std::numeric_limits<int64_t>::max();
  constexpr int64_t kRepeats = 4096;
  for (int64_t i = 0; i < kRepeats; ++i) {
    for (const auto& key : {Value(), Value(0), Value(wide)}) {
      left.rows.emplace_back(std::vector<Value>{Value(1), key, Value(2.0)});
      left.rows.emplace_back(std::vector<Value>{Value(2), key, Value(3.0)});
    }
  }
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  const auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                         col("f", "v"));
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {col("f", "p"), col("d", "s")};
  fusion.aggregates = {sum.get()};
  fusion.count_star = {false};
  fusion.select_items = fusion.group_keys;
  fusion.select_items.push_back(sum);
  fusion.select_names = {"p", "s", "n"};
  fusion.eligible = true;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      col("f", "id"), BinaryOperation::kEquals, col("d", "id"))};
  ASSIGN_OR_ASSERT_FAIL(
      std::shared_ptr<relational_detail::ColumnarTable>, build,
      relational_detail::BuildColumnarTableFromRows(right.schema, right.rows));
  right.columnar = std::move(build);
  right.rows.clear();
  for (const bool columnar_probe : {false, true}) {
    if (columnar_probe) {
      ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<relational_detail::ColumnarTable>,
                            probe,
                            relational_detail::BuildColumnarTableFromRows(
                                left.schema, left.rows));
      left.columnar = std::move(probe);
      left.rows.clear();
    }
    for (const size_t workers : {size_t{1}, size_t{2}}) {
      std::atomic<size_t> active{0};
      ScopedQueryParallelismBudget budget(&active, workers);
      auto result = relational_detail::TryFusedGroupedJoin(
          context, left, right, predicates, JoinType::kInner, nullptr, {},
          &fusion);
      ASSERT_TRUE(result.HasValue()) << result.GetStatus();
      auto output = result.MoveValue();
      ASSERT_TRUE(output.has_value());
      ASSIGN_OR_ASSERT_FAIL(Relation, rows,
                            relational_detail::MaterializeRelation(*output));
      SCOPED_TRACE(::testing::Message()
                   << "columnar=" << columnar_probe << " workers=" << workers);
      EXPECT_EQ(rows.rows.size(), 6U) << ::testing::PrintToString(rows.rows);
      for (const auto& key : {Value(), Value(0), Value(wide)}) {
        for (const auto& label : {"a", "b"}) {
          const double total =
              (std::string_view(label) == "a" ? 2.0 : 3.0) * kRepeats;
          EXPECT_TRUE(std::ranges::any_of(rows.rows, [&](const Row& row) {
            return relational_detail::DistinctValueEqual{}(row[0], key) &&
                   row[1] == Value(label) && row[2] == Value(total);
          }));
        }
      }
    }
  }
}

TEST_F(RelationalRegressionTest, FusedStringGroupHashesKeepCanonicalPeers) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left, right;
  left.schema =
      Schema("f", {Column(ColumnName("f", "id"), ValueType::kInt64),
                   Column(ColumnName("f", "label"), ValueType::kVarChar),
                   Column(ColumnName("f", "v"), ValueType::kInt64)});
  right.schema =
      Schema("d", {Column(ColumnName("d", "id"), ValueType::kInt64),
                   Column(ColumnName("d", "label"), ValueType::kVarChar)});
  right.rows = {Row({Value(1), Value("dimension with shared long text")}),
                Row({Value(2), Value("dimension with shared long text")})};
  const std::vector<Value> labels{Value("1-0 0 0:0:0"),
                                  Value("01-00 0 00:00:00"),
                                  Value("shared long string grouping key"),
                                  Value(),
                                  Value("UPPER").WithCollation(2),
                                  Value("upper").WithCollation(2)};
  left.rows.reserve(49152);
  for (size_t i = 0; i < 49152; ++i) {
    left.rows.emplace_back(
        std::vector<Value>{Value(static_cast<int64_t>(i % 2) + 1),
                           labels[i % labels.size()], Value(1)});
  }
  const auto col = [](const char* alias, const char* name) {
    return ColumnValueExp(ColumnName(alias, name));
  };
  const auto sum = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                         col("f", "v"));
  relational_detail::GroupedJoinFusionSpec fusion;
  fusion.group_keys = {col("f", "label"), col("d", "label")};
  fusion.aggregates = {sum.get()};
  fusion.count_star = {false};
  fusion.select_items = {fusion.group_keys[0], fusion.group_keys[1], sum};
  fusion.select_names = {"a", "b", "s"};
  fusion.eligible = true;
  const std::vector<Expression> predicates{BinaryExpressionExp(
      col("f", "id"), BinaryOperation::kEquals, col("d", "id"))};
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto result = relational_detail::TryFusedGroupedJoin(
        context, left, right, predicates, JoinType::kInner, nullptr, {},
        &fusion);
    ASSERT_TRUE(result.HasValue()) << result.GetStatus();
    auto output = result.MoveValue();
    if (!output) {
      FAIL() << "string-key grouped join declined";
    }
    ASSERT_EQ(output->rows.size(), 5U);
    for (const auto& row : output->rows) {
      const bool single = !row[0].IsCaseInsensitive();
      EXPECT_EQ(row[2], Value(single ? 8192 : 16384));
      EXPECT_EQ(row[1], right.rows[0][1]);
    }
  }
}

TEST_F(RelationalRegressionTest, FusedCountHavingKeepsAllComparisonsAndNulls) {
  for (const std::string op : {"=", "!=", "<", "<=", ">", ">="}) {
    const std::string join =
        " FROM SmallKeys d LEFT JOIN AggTable a ON d.k=a.k AND a.k<3 ";
    std::string reference = "SELECT * FROM (SELECT d.k,COUNT(a.k) AS n";
    reference.append(join)
        .append("GROUP BY d.k) t WHERE n ")
        .append(op)
        .append(" 10 ORDER BY k");
    std::string fused = "SELECT d.k,COUNT(a.k) AS n";
    fused.append(join)
        .append("GROUP BY d.k HAVING COUNT(a.k) ")
        .append(op)
        .append(" 10 ORDER BY d.k");
    const auto expected = RelationalRun(*rs_, reference);
    EXPECT_EQ(RelationalRun(*rs_, fused), expected);
    EXPECT_EQ(
        RelationalRun(*rs_,
                      "SELECT d.k,COUNT(*) AS n FROM SmallKeys d,AggTable a "
                      "WHERE d.k=a.k GROUP BY d.k HAVING COUNT(*) " +
                          op + " 20 ORDER BY d.k"),
        RelationalRun(*rs_,
                      "SELECT * FROM (SELECT d.k,COUNT(*) AS n FROM SmallKeys "
                      "d,AggTable a "
                      "WHERE d.k=a.k GROUP BY d.k) t WHERE n " +
                          op + " 20 ORDER BY k"));
  }
}

TEST_F(RelationalRegressionTest, CompactGroupingWithoutAggregatesKeepsPeers) {
  EXPECT_EQ(
      RelationalRun(*rs_, R"sql(
    SELECT k,v FROM (
      SELECT k,v FROM (SELECT 1 AS k,'a' AS v UNION ALL SELECT 1,'a'
                       UNION ALL SELECT 2,NULL UNION ALL SELECT 2,NULL
                       UNION ALL SELECT NULL,'b') t GROUP BY k,v
    ) g ORDER BY k NULLS FIRST
  )sql"),
      std::vector<Row>({Row({Value(), Value("b")}), Row({Value(1), Value("a")}),
                        Row({Value(2), Value()})}));
  EXPECT_TRUE(
      RelationalRun(
          *rs_, "SELECT k FROM (SELECT k FROM AggTable WHERE k<0 GROUP BY k) g")
          .empty());
}

TEST(RelationalPlanningTest, OuterJoinReductionResolvesUnqualifiedColumns) {
  std::vector<relational_detail::Relation> schemas(2);
  schemas[0].schema = Schema("l", {Column("lk", ValueType::kInt64)});
  schemas[1].schema = Schema("r", {Column("rk", ValueType::kInt64)});
  SelectStatement statement;
  SelectSource lhs;
  lhs.table = "l";
  SelectSource rhs;
  rhs.table = "r";
  rhs.join_type = JoinType::kLeft;
  statement.SetSources({lhs, rhs});
  statement.SetWhereClause(BinaryExpressionExp(ColumnValueExp("rk"),
                                               BinaryOperation::kGreaterThan,
                                               ConstantValueExp(Value(0))));
  bool reduced = false;
  auto sources =
      relational_detail::ReduceOuterJoinsToInner(statement, schemas, &reduced);
  EXPECT_TRUE(reduced);
  EXPECT_EQ(sources[1].join_type, JoinType::kInner);
  statement.SetWhereClause(
      UnaryExpressionExp(ColumnValueExp("rk"), UnaryOperation::kIsNull));
  sources =
      relational_detail::ReduceOuterJoinsToInner(statement, schemas, &reduced);
  EXPECT_FALSE(reduced);
  EXPECT_EQ(sources[1].join_type, JoinType::kLeft);
  // An ambiguous bare reference must never justify a rewrite.
  schemas[0].schema = Schema("l", {Column("rk", ValueType::kInt64)});
  statement.SetWhereClause(BinaryExpressionExp(ColumnValueExp("rk"),
                                               BinaryOperation::kGreaterThan,
                                               ConstantValueExp(Value(0))));
  sources =
      relational_detail::ReduceOuterJoinsToInner(statement, schemas, &reduced);
  EXPECT_FALSE(reduced);
  EXPECT_EQ(sources[1].join_type, JoinType::kLeft);
}

TEST(RelationalPlanningTest, JoinKeyEncodingMatchesValueEncoding) {
  const std::vector<Value> values = {
      Value(),
      Value(std::numeric_limits<int64_t>::min()),
      Value(std::numeric_limits<int64_t>::max()),
      Value(std::numeric_limits<int64_t>::max()).WithUnsigned(),
      Value::DateFromDays(42),
      Value(""),
      Value("a"),
      Value("abcdefgh"),
      Value("abcdefghi"),
      Value("abcdefghijklmnop"),
      Value(1.25),
      Value(-1.25),
      Value(0.0),
      Value(std::numeric_limits<double>::quiet_NaN()),
      Value::Array({Value(int64_t{1}), Value()}, "INT64")};
  for (const Value& value : values) {
    const Row input({value, Value("tail")});
    const Row expected({value, Value("tail")});
    EXPECT_EQ(relational_detail::EncodeJoinKey(input, {0, 1}),
              expected.EncodeMemcomparableFormat());
  }
  const Row negative_zero({Value(-0.0), Value("tail")});
  const Row positive_zero({Value(0.0), Value("tail")});
  EXPECT_EQ(relational_detail::EncodeJoinKey(negative_zero, {0, 1}),
            positive_zero.EncodeMemcomparableFormat());
}

TEST(RelationalPlanningTest, EstimateJoinRowsHandlesScalarKeyDomains) {
  const std::vector<Expression> int_keys = {BinaryExpressionExp(
      ColumnValueExp("key"), BinaryOperation::kEquals, ColumnValueExp("key"))};
  relational_detail::Relation int_left;
  int_left.schema = Schema("left", {Column("key", ValueType::kInt64)});
  int_left.AddRow(Row({Value(int64_t{1})}));
  int_left.AddRow(Row({Value(int64_t{2})}));
  int_left.AddRow(Row({Value(int64_t{2})}));
  int_left.AddRow(Row({Value()}));
  relational_detail::Relation int_right;
  int_right.schema = Schema("right", {Column("key", ValueType::kInt64)});
  int_right.AddRow(Row({Value(int64_t{2})}));
  int_right.AddRow(Row({Value(int64_t{2})}));
  int_right.AddRow(Row({Value(int64_t{3})}));
  int_right.AddRow(Row({Value()}));
  EXPECT_EQ(relational_detail::EstimateJoinRows(int_left, int_right, int_keys),
            4U);

  const std::vector<Expression> string_keys = {BinaryExpressionExp(
      ColumnValueExp("key"), BinaryOperation::kEquals, ColumnValueExp("key"))};
  relational_detail::Relation string_left;
  string_left.schema = Schema("left", {Column("key", ValueType::kVarChar)});
  string_left.AddRow(Row({Value("a")}));
  string_left.AddRow(Row({Value("b")}));
  string_left.AddRow(Row({Value("b")}));
  string_left.AddRow(Row({Value()}));
  relational_detail::Relation string_right;
  string_right.schema = Schema("right", {Column("key", ValueType::kVarChar)});
  string_right.AddRow(Row({Value("b")}));
  string_right.AddRow(Row({Value("b")}));
  string_right.AddRow(Row({Value("c")}));
  string_right.AddRow(Row({Value()}));
  EXPECT_EQ(relational_detail::EstimateJoinRows(string_left, string_right,
                                                string_keys),
            4U);
}

TEST_F(RelationalRegressionTest,
       FlatIntegerJoinHandlesCollisionsAndDuplicateOrder) {
  TransactionContext context = rs_->BeginContext();
  relational_detail::Relation left;
  left.schema = Schema("left", {Column("left_key", ValueType::kInt64)});
  left.AddRow(Row({Value(int64_t{1})}));
  relational_detail::Relation right;
  right.schema = Schema("right", {Column("right_key", ValueType::kInt64),
                                  Column("tag", ValueType::kVarChar)});
  right.AddRow(Row({Value(int64_t{1}), Value("first")}));
  right.AddRow(Row({Value(int64_t{1}), Value("second")}));
  right.AddRow(Row({Value(int64_t{9}), Value("collision")}));
  ASSERT_EQ(left.rows.size(), 1U);
  ASSERT_EQ(right.rows.size(), 3U);
  ASSERT_FALSE(right.HasSpill());
  const std::vector<Expression> predicates = {
      BinaryExpressionExp(ColumnValueExp("left_key"), BinaryOperation::kEquals,
                          ColumnValueExp("right_key"))};
  const std::vector<relational_detail::EqualityKey> equality_keys =
      relational_detail::EqualityKeys(left.schema, right.schema, predicates);
  ASSERT_EQ(equality_keys.size(), 1U);
  EXPECT_EQ(equality_keys[0].left, 0U);
  EXPECT_EQ(equality_keys[0].right, 0U);
  StatusOr<relational_detail::Relation> joined = relational_detail::InnerJoin(
      context, std::move(left), std::move(right), predicates, nullptr, {});
  ASSERT_TRUE(joined.HasValue()) << joined.GetStatus();
  EXPECT_EQ(joined.Value().join_comparisons, 2U);
  ASSERT_EQ(joined.Value().rows.size(), 2U);
  EXPECT_EQ(joined.Value().rows[0][2], Value("second"));
  EXPECT_EQ(joined.Value().rows[1][2], Value("first"));
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest,
       ParallelOuterProbeKeepsEveryUnmatchedBuildRow) {
  auto context = rs_->BeginContext();
  constexpr size_t kRepeats = 8192;
  for (const int key_shape : {0, 1, 2}) {
    const bool text = key_shape == 2;
    const ValueType key_type = text ? ValueType::kVarChar : ValueType::kInt64;
    const auto key = [text](int64_t value) {
      return text ? Value(std::to_string(value)) : Value(value);
    };
    for (const JoinType kind : {JoinType::kFull, JoinType::kRight}) {
      for (const size_t workers : {size_t{1}, size_t{2}}) {
        relational_detail::Relation left, right;
        left.schema =
            Schema("l", {Column("lk", key_type), Column("ld", ValueType::kDate),
                         Column("lv", ValueType::kInt64)});
        right.schema =
            Schema("r", {Column("rk", key_type), Column("rd", ValueType::kDate),
                         Column("rv", ValueType::kInt64)});
        const Value day = Value::DateFromDays(-10);
        for (size_t i = 0; i < kRepeats; ++i) {
          left.rows.emplace_back(Row({key(1), day, Value(3)}));
          left.rows.emplace_back(Row({key(1), day, Value(7)}));
          left.rows.emplace_back(Row({Value(), Value(), Value(0)}));
          left.rows.emplace_back(Row({key(3), day, Value(0)}));
        }
        right.rows = {
            Row({key(1), day, Value(5)}), Row({key(1), day, Value(9)}),
            Row({Value(), Value(), Value(0)}), Row({key(4), day, Value(0)})};
        SelectSource source;
        source.join_type = kind;
        source.join_condition =
            BinaryExpressionExp(ColumnValueExp("lk"), BinaryOperation::kEquals,
                                ColumnValueExp("rk"));
        if (key_shape != 0) {
          source.join_condition =
              BinaryExpressionExp(source.join_condition, BinaryOperation::kAnd,
                                  BinaryExpressionExp(ColumnValueExp("ld"),
                                                      BinaryOperation::kEquals,
                                                      ColumnValueExp("rd")));
        }
        std::atomic<size_t> active{0};
        ScopedQueryParallelismBudget budget(&active, workers);
        auto result = relational_detail::Join(
            context, std::move(left), std::move(right), source, nullptr, {});
        ASSERT_TRUE(result.HasValue()) << result.GetStatus();
        const size_t unmatched_left =
            kind == JoinType::kFull ? 2 * kRepeats : 0;
        EXPECT_EQ(result.Value().TotalRows(),
                  (4 * kRepeats) + unmatched_left + 2);
        EXPECT_EQ(result.Value().join_comparisons, 4 * kRepeats);
        size_t left_nulls = 0;
        size_t right_nulls = 0;
        size_t fives = 0;
        size_t nines = 0;
        for (const Row& row : result.Value().rows) {
          left_nulls += static_cast<size_t>(row[2].IsNull());
          right_nulls += static_cast<size_t>(row[5].IsNull());
          fives += static_cast<size_t>(row[5] == Value(5));
          nines += static_cast<size_t>(row[5] == Value(9));
        }
        EXPECT_EQ(left_nulls, 2U);
        EXPECT_EQ(right_nulls, unmatched_left);
        EXPECT_EQ(fives, 2 * kRepeats);
        EXPECT_EQ(nines, 2 * kRepeats);
      }
    }
  }
}

TEST_F(RelationalRegressionTest, CompositeDateFullJoinKeepsNullsAndResiduals) {
  auto context = rs_->BeginContext();
  relational_detail::Relation left;
  left.schema = Schema(
      "l", {Column("lk", ValueType::kInt64), Column("ld", ValueType::kDate),
            Column("lv", ValueType::kInt64)});
  relational_detail::Relation right;
  right.schema = Schema(
      "r", {Column("rk", ValueType::kInt64), Column("rd", ValueType::kDate),
            Column("rv", ValueType::kInt64)});
  const Value day = Value::DateFromDays(-10);
  const Value other = Value::DateFromDays(20);
  left.AddRow(Row({Value(1), day, Value(3)}));
  left.AddRow(Row({Value(1), day, Value(7)}));
  left.AddRow(Row({Value(2), Value(), Value(0)}));
  left.AddRow(Row({Value(3), other, Value(0)}));
  right.AddRow(Row({Value(1), day, Value(5)}));
  right.AddRow(Row({Value(1), day, Value(9)}));
  right.AddRow(Row({Value(2), Value(), Value(0)}));
  right.AddRow(Row({Value(4), other, Value(0)}));
  ASSERT_TRUE(relational_detail::CompositeIntJoinKey(left.schema, {0, 1},
                                                     right.schema, {0, 1}));
  const Schema mixed("mixed", {Column("rk", ValueType::kInt64),
                               Column("rd", ValueType::kInt64)});
  EXPECT_FALSE(relational_detail::CompositeIntJoinKey(left.schema, {0, 1},
                                                      mixed, {0, 1}));
  SelectSource source;
  source.join_type = JoinType::kFull;
  source.join_condition = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("lk"), BinaryOperation::kEquals,
                          ColumnValueExp("rk")),
      BinaryOperation::kAnd,
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("ld"), BinaryOperation::kEquals,
                              ColumnValueExp("rd")),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("lv"), BinaryOperation::kLessThan,
                              ColumnValueExp("rv"))));
  auto joined = relational_detail::Join(context, std::move(left),
                                        std::move(right), source, nullptr, {});
  ASSERT_TRUE(joined.HasValue()) << joined.GetStatus();
  const std::vector<Row> expected{
      Row({Value(1), day, Value(3), Value(1), day, Value(5)}),
      Row({Value(1), day, Value(3), Value(1), day, Value(9)}),
      Row({Value(1), day, Value(7), Value(1), day, Value(9)}),
      Row({Value(2), Value(), Value(0), Value(), Value(), Value()}),
      Row({Value(3), other, Value(0), Value(), Value(), Value()}),
      Row({Value(), Value(), Value(), Value(2), Value(), Value(0)}),
      Row({Value(), Value(), Value(), Value(4), other, Value(0)})};
  EXPECT_TRUE(std::ranges::is_permutation(joined.Value().rows, expected));
  EXPECT_EQ(joined.Value().join_comparisons, 4U);
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
}

TEST_F(RelationalRegressionTest, SmallInnerBuildStreamsEverySpilledProbeRow) {
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(65536);
  for (int variant = 0; variant < 6; ++variant) {
    SCOPED_TRACE(variant);
    const int shape = variant % 3;  // integer / composite integer / byte key
    auto context = rs_->BeginContext();
    relational_detail::ExecutionRuntime runtime;
    context.set_execution_runtime(&runtime);
    const ValueType key_type =
        shape == 2 ? ValueType::kVarChar : ValueType::kInt64;
    relational_detail::Relation left(&runtime);
    left.schema =
        Schema("l", {Column("lk", key_type), Column("lk2", ValueType::kInt64)});
    relational_detail::Relation right(&runtime);
    right.schema =
        Schema("r", {Column("rk", key_type), Column("rk2", ValueType::kInt64)});
    const auto key = [&](int n) {
      return shape == 2 ? Value(std::to_string(n)) : Value(n);
    };
    left.AddRow(Row({key(0), Value(9)}));
    left.AddRow(Row({key(1), Value(9)}));
    left.AddRow(Row({Value(), Value(9)}));
    size_t expected = 0;
    for (int i = 0; i < 8192; ++i) {
      const bool is_null = i % 17 == 0;
      right.AddRow(Row({is_null ? Value() : key(i % 5), Value(9)}));
      expected += !is_null && i % 5 < 2 ? 1 : 0;
    }
    right.EnsureSpill();
    ASSERT_SUCCESS(right.FinishSpill());
    ASSERT_TRUE(right.HasSpill());
    std::vector<Expression> predicates{BinaryExpressionExp(
        ColumnValueExp("lk"), BinaryOperation::kEquals, ColumnValueExp("rk"))};
    if (shape == 1) {
      predicates.push_back(BinaryExpressionExp(ColumnValueExp("lk2"),
                                               BinaryOperation::kEquals,
                                               ColumnValueExp("rk2")));
    }
    SelectSource source;
    source.join_type = JoinType::kInner;
    source.join_condition = CombineConjuncts(predicates);
    auto joined =
        variant < 3
            ? relational_detail::InnerJoin(context, std::move(left),
                                           std::move(right), predicates,
                                           nullptr, {})
            : relational_detail::Join(context, std::move(left),
                                      std::move(right), source, nullptr, {});
    ASSERT_TRUE(joined.HasValue()) << joined.GetStatus();
    EXPECT_EQ(joined.Value().hybrid_hash_joins, 0U);
    EXPECT_EQ(joined.Value().in_memory_hash_joins, 1U);
    size_t count = 0;
    ASSERT_SUCCESS(joined.Value().ForEachRow([&](const Row& row) {
      ASSERT_EQ(row.values_.size(), 4U);
      EXPECT_FALSE(row[0].IsNull());
      EXPECT_EQ(row[0], row[2]);
      EXPECT_EQ(row[1], Value(9));
      EXPECT_EQ(row[3], Value(9));
      ++count;
    }));
    EXPECT_EQ(count, expected);
    EXPECT_EQ(runtime.inner_hash_build_rows, 3U);
    EXPECT_EQ(runtime.inner_hash_left_builds, 1U);
    EXPECT_EQ(runtime.inner_hash_spilled_probe_rows, 8192U);
  }
}

TEST_F(RelationalRegressionTest,
       SpilledInnerNestedLoopRescansOuterPerInnerChunk) {
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(65536);
  const std::vector<int64_t> left_keys = {3, 1, 7, 0, 9, 5};
  std::vector<int64_t> right_keys;
  right_keys.reserve(9000);
  for (int i = 0; i < 9000; ++i) {
    right_keys.push_back(i % 11);
  }
  auto build_sides = [&] {
    relational_detail::Relation left, right;
    left.schema = Schema("l", {Column("lk", ValueType::kInt64)});
    right.schema = Schema("r", {Column("rk", ValueType::kInt64)});
    for (const int64_t key : left_keys) {
      left.AddRow(Row({Value(key)}));
    }
    for (const int64_t key : right_keys) {
      right.AddRow(Row({Value(key)}));
    }
    right.EnsureSpill();
    EXPECT_EQ(right.FinishSpill(), Status::kSuccess);
    EXPECT_TRUE(right.HasSpill());
    return std::pair{std::move(left), std::move(right)};
  };
  auto expected_for = [&](JoinType kind) {
    std::map<std::string, size_t> expected;
    std::vector<char> right_matched(right_keys.size(), 0);
    for (const int64_t lk : left_keys) {
      bool matched = false;
      for (size_t j = 0; j < right_keys.size(); ++j) {
        if (lk < right_keys[j]) {
          matched = true;
          right_matched[j] = 1;
          ++expected[(Row({Value(lk)}) + Row({Value(right_keys[j])}))
                         .ToString()];
        }
      }
      if (!matched && (kind == JoinType::kLeft || kind == JoinType::kFull)) {
        ++expected[(Row({Value(lk)}) + Row(std::vector<Value>(1))).ToString()];
      }
    }
    if (kind == JoinType::kRight || kind == JoinType::kFull) {
      for (size_t j = 0; j < right_keys.size(); ++j) {
        if (right_matched[j] == 0) {
          ++expected[(Row(std::vector<Value>(1)) + Row({Value(right_keys[j])}))
                         .ToString()];
        }
      }
    }
    return expected;
  };
  const std::vector<Expression> predicates{BinaryExpressionExp(
      ColumnValueExp("lk"), BinaryOperation::kLessThan, ColumnValueExp("rk"))};
  for (const JoinType kind :
       {JoinType::kInner, JoinType::kLeft, JoinType::kRight, JoinType::kFull}) {
    SCOPED_TRACE(static_cast<int>(kind));
    auto context = rs_->BeginContext();
    auto [left, right] = build_sides();
    SelectSource source;
    source.join_type = kind;
    source.join_condition = CombineConjuncts(predicates);
    auto joined = relational_detail::Join(
        context, std::move(left), std::move(right), source, nullptr, {});
    ASSERT_TRUE(joined.HasValue()) << joined.GetStatus();
    EXPECT_GE(joined.Value().nested_loop_joins, 1U);
    std::map<std::string, size_t> actual;
    ASSERT_SUCCESS(joined.Value().ForEachRow(
        [&](const Row& row) { ++actual[row.ToString()]; }));
    EXPECT_EQ(actual, expected_for(kind));
  }
  {
    auto context = rs_->BeginContext();
    auto [left, right] = build_sides();
    auto joined = relational_detail::InnerJoin(
        context, std::move(left), std::move(right), predicates, nullptr, {});
    ASSERT_TRUE(joined.HasValue()) << joined.GetStatus();
    EXPECT_GE(joined.Value().nested_loop_joins, 1U);
    std::map<std::string, size_t> actual;
    ASSERT_SUCCESS(joined.Value().ForEachRow(
        [&](const Row& row) { ++actual[row.ToString()]; }));
    EXPECT_EQ(actual, expected_for(JoinType::kInner));
  }
}

TEST_F(RelationalRegressionTest,
       DenseRuntimeScanKeepsNullableKeysAndProjection) {
  RelationalRun(*rs_, "CREATE TABLE NullableDenseScan (k INT64,id INT64)");
  std::map<int64_t, Row> expected;
  RuntimeFilterBuilder builder;
  for (const int64_t key : {-2, 0, 4}) {
    builder.Observe(key);
  }
  const auto filter = builder.Build();
  ASSERT_TRUE(filter->has_dense_bits());
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("NullableDenseScan"));
    for (int64_t i = 0; i < 18000; ++i) {
      const int64_t key = (i % 13) - 6;
      const Value cell = i % 7 == 0 ? Value() : Value(key);
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({cell, Value(i)})).GetStatus());
      if (!cell.IsNull() && (key == -2 || key == 0 || key == 4)) {
        expected.emplace(i, Row({Value(i), cell}));
      }
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const Schema schema("NullableDenseScan", {Column("id", ValueType::kInt64),
                                            Column("k", ValueType::kInt64)});
  const std::vector<slot_t> projection{1, 0};
  for (const size_t workers : {size_t{1}, size_t{2}}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("NullableDenseScan"));
    for (const int mode : {0, 1, 2, 3}) {
      const bool columnar = (mode & 1) != 0;
      const bool local_predicate = mode >= 2;
      const auto predicates = relational_detail::CompileScanFilter(
          {BinaryExpressionExp(ColumnValueExp("k"),
                               BinaryOperation::kGreaterThanEquals,
                               ConstantValueExp(Value(0)))},
          schema);
      relational_detail::Relation result;
      result.schema = schema;
      Status error = Status::kSuccess;
      ASSERT_TRUE(relational_detail::TryParallelTableScan(
          context, *table, &projection, filter.get(), ValueType::kInt64,
          slot_t{0}, true, local_predicate ? &predicates : nullptr, schema,
          nullptr, {}, &result, &error, nullptr, columnar));
      ASSERT_SUCCESS(error);
      std::map<int64_t, Row> actual;
      ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
        EXPECT_TRUE(actual.emplace(row[0].value.int_value, row).second);
      }));
      auto filtered_expected = expected;
      if (local_predicate) {
        std::erase_if(filtered_expected, [](const auto& item) {
          return item.second[1].value.int_value < 0;
        });
      }
      EXPECT_EQ(actual, filtered_expected);
    }
  }
}

TEST_F(RelationalRegressionTest, DateStringPeeksKeepHolesAndTypeBoundaries) {
  RelationalRun(*rs_, "CREATE TABLE DatePeek (d DATE,id INT64)");
  std::vector<Row> projected;
  projected.reserve(9000);
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("DatePeek"));
    for (int i = 0; i < 9000; ++i) {
      const Value date =
          i % 7 == 0 ? Value() : Value::DateFromDays((i % 100) - 20);
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({date, Value(i)})).GetStatus());
      projected.emplace_back(Row({Value(i), date}));
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const Schema schema("DatePeek", {Column("id", ValueType::kInt64),
                                   Column("d", ValueType::kDate)});
  const std::vector<slot_t> projection{1, 0};
  const auto compare = [](BinaryOperation op, const char* date) {
    return BinaryExpressionExp(ColumnValueExp("d"), op,
                               ConstantValueExp(Value(date)));
  };
  const Expression first = compare(BinaryOperation::kEquals, "1970-01-02");
  const Expression last = compare(BinaryOperation::kEquals, "1970-01-16");
  const auto alternatives =
      BinaryExpressionExp(first, BinaryOperation::kOr, last);
  const std::vector<std::vector<Expression>> predicates{
      {compare(BinaryOperation::kGreaterThan, "1969-12-31"),
       compare(BinaryOperation::kLessThanEquals, "1970-01-16")},
      {alternatives},
      {BinaryExpressionExp(
          alternatives, BinaryOperation::kOr,
          BinaryExpressionExp(ColumnValueExp("id"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(3))))}};
  for (size_t workers : {1U, 2U}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("DatePeek"));
    for (size_t c = 0; c < predicates.size(); ++c) {
      const auto filter =
          relational_detail::CompileScanFilter(predicates[c], schema);
      const auto peeks = relational_detail::BuildIntegerPeeks(
          filter, &projection, table->GetSchema());
      EXPECT_EQ(peeks.empty(), c == 2);
      relational_detail::Relation result;
      result.schema = schema;
      Status error = Status::kSuccess;
      ASSERT_TRUE(relational_detail::TryParallelTableScan(
          context, *table, &projection, nullptr, ValueType::kNull, std::nullopt,
          true, &filter, schema, nullptr, {}, &result, &error));
      ASSERT_SUCCESS(error);
      std::map<int64_t, Row> expected, actual;
      for (const Row& row : projected) {
        bool matches = true;
        for (const Expression& predicate : predicates[c]) {
          ASSIGN_OR_ASSERT_FAIL(Value, verdict,
                                predicate->TryEvaluate(row, schema));
          matches &= !verdict.IsNull() && verdict.value.int_value != 0;
        }
        if (matches) {
          expected.emplace(row[0].value.int_value, row);
        }
      }
      ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
        EXPECT_TRUE(actual.emplace(row[0].value.int_value, row).second);
      }));
      EXPECT_EQ(actual, expected);
    }
    const auto invalid = relational_detail::CompileScanFilter(
        {compare(BinaryOperation::kEquals, "not-a-date")}, schema);
    EXPECT_TRUE(relational_detail::BuildIntegerPeeks(invalid, &projection,
                                                     table->GetSchema())
                    .empty());
    const auto dates = relational_detail::CompileScanFilter({first}, schema);
    const Schema integer_schema("DatePeek", {Column("d", ValueType::kInt64),
                                             Column("id", ValueType::kInt64)});
    EXPECT_TRUE(
        relational_detail::BuildIntegerPeeks(dates, &projection, integer_schema)
            .empty());
    EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
  }
}

TEST_F(RelationalRegressionTest, PackedDateComparisonsKeepHolesAndProjection) {
  RelationalRun(*rs_, "CREATE TABLE PackedDatePeek (d DATE,id INT64)");
  std::vector<Row> projected;
  projected.reserve(9000);
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("PackedDatePeek"));
    for (int i = 0; i < 9000; ++i) {
      const Value date =
          i % 7 == 0 ? Value() : Value::DateFromDays((i % 1024) - 20);
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({date, Value(i)})).GetStatus());
      projected.emplace_back(Row({Value(i), date}));
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const Schema schema("PackedDatePeek", {Column("id", ValueType::kInt64),
                                         Column("d", ValueType::kDate)});
  const std::vector<slot_t> projection{1, 0};
  const auto compare = [](BinaryOperation op, const char* date) {
    return BinaryExpressionExp(ColumnValueExp("d"), op,
                               ConstantValueExp(Value(date)));
  };
  const Expression first = compare(BinaryOperation::kEquals, "1970-01-02");
  const Expression last = compare(BinaryOperation::kEquals, "1970-01-16");
  const auto alternatives =
      BinaryExpressionExp(first, BinaryOperation::kOr, last);
  const std::vector<std::vector<Expression>> predicates{
      {compare(BinaryOperation::kGreaterThan, "1969-12-31"),
       compare(BinaryOperation::kLessThanEquals, "1970-01-16")},
      {alternatives},
      {BinaryExpressionExp(
          alternatives, BinaryOperation::kOr,
          BinaryExpressionExp(ColumnValueExp("id"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(3))))}};
  for (size_t workers : {1U, 2U}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("PackedDatePeek"));
    const auto store = GetOrBuildLSTableStore(context.txn_, *table, true);
    ASSERT_NE(store, nullptr);
    ASSERT_NE(store->dictionaries[0].wide_code_bytes, 0);
    for (size_t c = 0; c < predicates.size(); ++c) {
      const auto filter =
          relational_detail::CompileScanFilter(predicates[c], schema);
      const auto peeks = relational_detail::BuildIntegerPeeks(
          filter, &projection, table->GetSchema());
      EXPECT_EQ(peeks.empty(), c == 2);
      relational_detail::Relation result;
      result.schema = schema;
      Status error = Status::kSuccess;
      ASSERT_TRUE(relational_detail::TryParallelTableScan(
          context, *table, &projection, nullptr, ValueType::kNull, std::nullopt,
          true, &filter, schema, nullptr, {}, &result, &error));
      ASSERT_SUCCESS(error);
      std::map<int64_t, Row> expected, actual;
      for (const Row& row : projected) {
        bool matches = true;
        for (const Expression& predicate : predicates[c]) {
          ASSIGN_OR_ASSERT_FAIL(Value, verdict,
                                predicate->TryEvaluate(row, schema));
          matches &= !verdict.IsNull() && verdict.value.int_value != 0;
        }
        if (matches) {
          expected.emplace(row[0].value.int_value, row);
        }
      }
      ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
        EXPECT_TRUE(actual.emplace(row[0].value.int_value, row).second);
      }));
      EXPECT_EQ(actual, expected);
    }
    const auto invalid = relational_detail::CompileScanFilter(
        {compare(BinaryOperation::kEquals, "not-a-date")}, schema);
    EXPECT_TRUE(relational_detail::BuildIntegerPeeks(invalid, &projection,
                                                     table->GetSchema())
                    .empty());
    const auto dates = relational_detail::CompileScanFilter({first}, schema);
    const Schema integer_schema(
        "PackedDatePeek",
        {Column("d", ValueType::kInt64), Column("id", ValueType::kInt64)});
    EXPECT_TRUE(
        relational_detail::BuildIntegerPeeks(dates, &projection, integer_schema)
            .empty());
    EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
  }
}

TEST_F(RelationalRegressionTest, ColumnarNullTestsKeepProjectionAndResiduals) {
  RelationalRun(*rs_,
                "CREATE TABLE NullScan (id INT64, k INT64, s STRING, "
                "v DOUBLE, absent INT64)");
  std::vector<Row> projected;
  projected.reserve(10000);
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          setup.GetTable("NullScan"));
    for (int i = 0; i < 10000; ++i) {
      const Value key = i >= 1000 && i % 3 == 0 ? Value() : Value(i % 7);
      const Value text =
          i >= 1000 && i % 5 == 0
              ? Value()
              : Value(std::string(96, static_cast<char>('a' + (i % 20))));
      const Value number = i % 2 == 0 ? Value() : Value(i * 0.5);
      ASSERT_SUCCESS(
          table->Insert(setup.txn_, Row({Value(i), key, text, number, Value()}))
              .GetStatus());
      projected.emplace_back(Row({number, Value(i), text, key, Value()}));
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  const std::vector<slot_t> projection{3, 0, 2, 1, 4};
  const Schema schema(
      "NullScan",
      {Column("v", ValueType::kDouble), Column("id", ValueType::kInt64),
       Column("s", ValueType::kVarChar), Column("k", ValueType::kInt64),
       Column("absent", ValueType::kInt64)});
  const auto is_null = [](const char* column) {
    return UnaryExpressionExp(ColumnValueExp(column), UnaryOperation::kIsNull);
  };
  const auto is_not_null = [](const char* column) {
    return UnaryExpressionExp(ColumnValueExp(column),
                              UnaryOperation::kIsNotNull);
  };
  const Expression extra = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kAdd,
                          ConstantValueExp(Value(1))),
      BinaryOperation::kGreaterThan, ConstantValueExp(Value(3)));
  const std::vector<std::vector<Expression>> predicates{
      {is_null("k")},
      {is_not_null("s")},
      {UnaryExpressionExp(is_null("k"), UnaryOperation::kNot)},
      {is_null("s"), is_not_null("v")},
      {is_null("id")},
      {is_null("absent")},
      {is_not_null("absent")},
      {is_null("s"), extra},
      {BinaryExpressionExp(is_null("k"), BinaryOperation::kOr, is_null("s"))},
      {UnaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kAdd,
                              ConstantValueExp(Value(1))),
          UnaryOperation::kIsNull)}};
  for (size_t workers : {1U, 2U}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, workers);
    auto context = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable("NullScan"));
    for (const auto& conjunction : predicates) {
      const auto filter =
          relational_detail::CompileScanFilter(conjunction, schema);
      relational_detail::Relation result;
      result.schema = schema;
      Status error = Status::kSuccess;
      ASSERT_TRUE(relational_detail::TryParallelTableScan(
          context, *table, &projection, nullptr, ValueType::kNull, std::nullopt,
          true, &filter, schema, nullptr, {}, &result, &error));
      ASSERT_SUCCESS(error);
      ASSERT_SUCCESS(result.GetStatus());
      std::map<int64_t, Row> expected;
      for (const Row& row : projected) {
        bool matches = true;
        for (const Expression& predicate : conjunction) {
          ASSIGN_OR_ASSERT_FAIL(Value, verdict,
                                predicate->TryEvaluate(row, schema));
          matches &= !verdict.IsNull() && verdict.value.int_value != 0;
        }
        if (matches) {
          expected.emplace(row[1].value.int_value, row);
        }
      }
      std::map<int64_t, Row> actual;
      ASSERT_SUCCESS(result.ForEachRow([&](const Row& row) {
        EXPECT_TRUE(actual.emplace(row[1].value.int_value, row).second);
      }));
      EXPECT_EQ(actual, expected);
    }
    EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
  }
}

TEST_F(RelationalRegressionTest, ScalarIntervalFoldKeepsNullsAndBoundaries) {
  auto setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, table,
      rs_->CreateTable(
          setup, Schema("IntervalFold", {Column("k", ValueType::kInt64),
                                         Column("v", ValueType::kDouble)})));
  const std::vector<Row> pattern{
      Row({Value(), Value(100.0)}),
      Row({Value(-2), Value(1.5)}),
      Row({Value(0), Value()}),
      Row({Value(1), Value(-2.0)}),
      Row({Value(2), Value(4.5)}),
      Row({Value(std::numeric_limits<int64_t>::min()), Value(8.0)}),
      Row({Value(std::numeric_limits<int64_t>::max()), Value(16.0)})};
  constexpr int kRepeats = 512;
  for (int i = 0; i < kRepeats; ++i) {
    for (const Row& row : pattern) {
      ASSERT_SUCCESS(table.Insert(setup.txn_, row).GetStatus());
    }
  }
  ASSERT_SUCCESS(setup.PreCommit());
  const auto run = [&](const std::string& predicate) {
    std::string sql = "SELECT ";
    for (const auto* aggregate : {"COUNT(*)", "SUM(v)", "AVG(v)"}) {
      if (sql != "SELECT ") {
        sql += ", ";
      }
      sql += "(SELECT ";
      sql += aggregate;
      sql += " FROM IntervalFold WHERE ";
      sql += predicate;
      sql += ")";
    }
    sql += " FROM KeyTable WHERE k = 0";
    return RelationalRun(*rs_, sql);
  };
  for (size_t worker_count : {1U, 2U}) {
    std::atomic<size_t> active{0};
    ScopedQueryParallelismBudget budget(&active, worker_count);
    EXPECT_EQ(run("k >= -2 AND k <= 2"),
              std::vector<Row>({Row({Value(kRepeats * 4), Value(kRepeats * 4.0),
                                     Value(4.0 / 3.0)})}));
    EXPECT_EQ(run("k > -2 AND k < 2"),
              std::vector<Row>({Row({Value(kRepeats * 2),
                                     Value(kRepeats * -2.0), Value(-2.0)})}));
    EXPECT_EQ(run("k = 0"),
              std::vector<Row>({Row({Value(kRepeats), Value(), Value()})}));
    for (const auto* predicate :
         {"k > 9223372036854775807", "k < -9223372036854775808",
          "k >= 2 AND k < 1"}) {
      EXPECT_EQ(run(predicate),
                std::vector<Row>({Row({Value(0), Value(), Value()})}));
    }
    EXPECT_EQ(run("k >= 1 AND k <= 2 AND v > 0"),
              std::vector<Row>(
                  {Row({Value(kRepeats), Value(kRepeats * 4.5), Value(4.5)})}));
  }
  RelationalRun(*rs_,
                "INSERT INTO IntervalFold VALUES (1, CAST('inf' AS FLOAT64))");
  const auto infinite = run("k = 1");
  ASSERT_EQ(infinite.size(), 1U);
  EXPECT_TRUE(std::isinf(infinite[0][1].value.double_value));
  EXPECT_TRUE(std::isinf(infinite[0][2].value.double_value));
  RelationalRun(*rs_,
                "INSERT INTO IntervalFold VALUES (1, CAST('nan' AS FLOAT64))");
  const auto nan = run("k = 1");
  ASSERT_EQ(nan.size(), 1U);
  EXPECT_TRUE(std::isnan(nan[0][1].value.double_value));
  EXPECT_TRUE(std::isnan(nan[0][2].value.double_value));
}

TEST_F(RelationalRegressionTest, NarrowScalarAggregatesKeepNullAndEmptyInput) {
  RelationalRun(*rs_,
                "CREATE TABLE narrow_scalar (padding STRING, a INT64, "
                "b FLOAT64, unused STRING)");
  RelationalRun(*rs_,
                "INSERT INTO narrow_scalar VALUES "
                "('x', NULL, NULL, 'y'), ('x', 3, 2.5, 'y'), "
                "('x', 5, NULL, 'y')");
  EXPECT_EQ(
      RelationalRun(*rs_,
                    "SELECT COUNT(*), COUNT(a), SUM(a), SUM(b) "
                    "FROM narrow_scalar"),
      std::vector<Row>({Row({Value(3), Value(2), Value(8), Value(2.5)})}));
  EXPECT_EQ(RelationalRun(*rs_, "SELECT COUNT(*) FROM narrow_scalar"),
            std::vector<Row>({Row({Value(3)})}));
  RelationalRun(*rs_, "DELETE FROM narrow_scalar");
  EXPECT_EQ(RelationalRun(*rs_,
                          "SELECT COUNT(*), SUM(a), SUM(b) "
                          "FROM narrow_scalar"),
            std::vector<Row>({Row({Value(0), Value(), Value()})}));
}

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

}  // namespace
}  // namespace tinylamb
