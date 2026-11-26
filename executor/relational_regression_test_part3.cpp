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

TEST_F(RelationalRegressionTest,
       SurrogateKeyPreAggRollupMatchesCanonicalAndHandlesDuplicates) {
  // Lean proof: lean/TinylambOpt/ResearchProofs/Wave9TpcdsRollupAndSetOps.lean
  //   (SurrogateKeyPreAggRollup_sound)
  RelationalRun(*rs_,
                "CREATE TABLE SurrInv (inv_date_sk INT64, inv_item_sk INT64, "
                "inv_qty INT64)");
  RelationalRun(*rs_, "CREATE TABLE SurrDate (d_date_sk INT64, d_month INT64)");
  RelationalRun(
      *rs_,
      "CREATE TABLE SurrItem (i_item_sk INT64, i_prod STRING, i_brand STRING)");
  RelationalRun(
      *rs_,
      "CREATE TABLE SurrItemDup (i_item_sk INT64, i_prod STRING, i_brand "
      "STRING)");

  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, inv,
                          setup.GetTable("SurrInv"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, dt,
                          setup.GetTable("SurrDate"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, it,
                          setup.GetTable("SurrItem"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, it_dup,
                          setup.GetTable("SurrItemDup"));

    for (int64_t d = 1; d <= 10; ++d) {
      ASSERT_SUCCESS(
          dt->Insert(setup.txn_, Row({Value(d), Value(d <= 6 ? 1 : 2)}))
              .GetStatus());
    }
    // Distinct i_item_sk 1 and 2 share identical (i_prod, i_brand) to verify
    // finest_keys_unique=false merges colliding attribute tuples.
    // i_item_sk 3 has all-NULL inv_qty. NULL PK row must be skipped cleanly.
    const std::vector<Row> item_rows = {
        Row({Value(int64_t{1}), Value(std::string("pA")),
             Value(std::string("b1"))}),
        Row({Value(int64_t{2}), Value(std::string("pA")),
             Value(std::string("b1"))}),
        Row({Value(int64_t{3}), Value(std::string("pB")),
             Value(std::string("b2"))}),
        Row({Value(int64_t{4}), Value(std::string("pA")),
             Value(std::string("b2"))}),
        Row({Value(), Value(std::string("pNull")),
             Value(std::string("bNull"))}),
    };
    for (const Row& r : item_rows) {
      ASSERT_SUCCESS(it->Insert(setup.txn_, r).GetStatus());
      ASSERT_SUCCESS(it_dup->Insert(setup.txn_, r).GetStatus());
    }
    // Add duplicate non-NULL PK to SurrItemDup to verify safe fallback.
    ASSERT_SUCCESS(it_dup
                       ->Insert(setup.txn_, Row({Value(int64_t{1}),
                                                 Value(std::string("pA_dup")),
                                                 Value(std::string("b1_dup"))}))
                       .GetStatus());

    for (int64_t i = 0; i < 400; ++i) {
      const int64_t d = (i % 10) + 1;
      const int64_t item_sk =
          (i % 6);  // includes 0 (unmatched) and 5 (unmatched)
      Value fk = (i % 17 == 0) ? Value() : Value(item_sk);
      Value qty = (item_sk == 3 || i % 7 == 0) ? Value() : Value((i % 25) + 1);
      ASSERT_SUCCESS(
          inv->Insert(setup.txn_, Row({Value(d), fk, qty})).GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }

  for (const std::string& dim_table : {"SurrItem", "SurrItemDup"}) {
    for (const int month_filter : {1, 999}) {
      const std::string from_where =
          " FROM SurrInv, SurrDate, " + dim_table +
          " WHERE inv_date_sk = d_date_sk AND inv_item_sk = i_item_sk"
          " AND d_month = " +
          std::to_string(month_filter);
      const std::string rollup_sql =
          "SELECT i_prod, i_brand, AVG(inv_qty) AS qoh" + from_where +
          " GROUP BY ROLLUP (i_prod, i_brand)";
      const auto got = RelationalRun(*rs_, rollup_sql);

      std::vector<Row> expected =
          RelationalRun(*rs_, "SELECT i_prod, i_brand, AVG(inv_qty) AS qoh" +
                                  from_where + " GROUP BY i_prod, i_brand");
      const auto sub =
          RelationalRun(*rs_, "SELECT i_prod, NULL, AVG(inv_qty) AS qoh" +
                                  from_where + " GROUP BY i_prod");
      const auto grand = RelationalRun(
          *rs_, "SELECT NULL, NULL, AVG(inv_qty) AS qoh" + from_where);
      expected.insert(expected.end(), sub.begin(), sub.end());
      expected.insert(expected.end(), grand.begin(), grand.end());

      ASSERT_EQ(got.size(), expected.size())
          << "dim=" << dim_table << " month=" << month_filter;
      std::vector<bool> matched(expected.size(), false);
      for (const Row& row : got) {
        size_t idx = 0;
        while (idx < expected.size() &&
               (matched[idx] || row != expected[idx])) {
          ++idx;
        }
        ASSERT_LT(idx, expected.size())
            << "dim=" << dim_table << " month=" << month_filter
            << " unmatched row: " << row;
        matched[idx] = true;
      }
    }
  }
}

TEST_F(RelationalRegressionTest, MultiBucketScalarCrossJoinMatchesCanonical) {
  // Lean proof:
  //   lean/TinylambOpt/ResearchProofs/Wave9TpcdsMultiChannelAndTpch.lean
  //   (SingleScanConditionalCountDistinct_sound)
  RelationalRun(*rs_,
                "CREATE TABLE BucketSales (qty INT64, lp FLOAT64, "
                "cp FLOAT64, wc FLOAT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl,
                          setup.GetTable("BucketSales"));
    for (int64_t i = 0; i < 600; ++i) {
      Value qty = (i % 29 == 0) ? Value() : Value((i % 20) + 1);
      Value lp = (i % 19 == 0)
                     ? Value()
                     : Value(i % 41 == 0 ? -0.0 : static_cast<double>(i % 30));
      Value cp = (i % 23 == 0) ? Value() : Value(static_cast<double>(i % 100));
      Value wc = (i % 31 == 0) ? Value() : Value(static_cast<double>(i % 50));
      ASSERT_SUCCESS(
          tbl->Insert(setup.txn_, Row({qty, lp, cp, wc})).GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }

  const std::string query =
      "SELECT * FROM "
      "(SELECT AVG(lp) AS b1_lp, COUNT(lp) AS b1_cnt, "
      "  COUNT(DISTINCT lp) AS b1_cntd FROM BucketSales "
      " WHERE qty BETWEEN 1 AND 5 AND (lp BETWEEN 0 AND 10 OR cp BETWEEN 10 "
      " AND 10 + 20 OR wc BETWEEN 5 AND 5 + 10)) AS B1, "
      "(SELECT AVG(lp) AS b2_lp, COUNT(lp) AS b2_cnt, "
      "  COUNT(DISTINCT lp) AS b2_cntd FROM BucketSales "
      " WHERE qty BETWEEN 6 AND 10 AND (lp BETWEEN 15 AND 25 OR cp BETWEEN 50 "
      " AND 70 OR wc BETWEEN 20 AND 30)) AS B2, "
      "(SELECT AVG(lp) AS b3_lp, COUNT(lp) AS b3_cnt, "
      "  COUNT(DISTINCT lp) AS b3_cntd FROM BucketSales "
      " WHERE qty BETWEEN 90 AND 95 AND (lp BETWEEN 0 AND 10 OR cp BETWEEN 0 "
      " AND 10)) AS B3 "
      "LIMIT 100";

  const auto got = RelationalRun(*rs_, query);
  ASSERT_EQ(got.size(), 1U);
  const auto b1 = RelationalRun(
      *rs_,
      "SELECT AVG(lp), COUNT(lp), COUNT(DISTINCT lp) FROM BucketSales "
      "WHERE qty BETWEEN 1 AND 5 AND (lp BETWEEN 0 AND 10 OR cp BETWEEN 10 "
      "AND 10 + 20 OR wc BETWEEN 5 AND 5 + 10)");
  const auto b2 = RelationalRun(
      *rs_,
      "SELECT AVG(lp), COUNT(lp), COUNT(DISTINCT lp) FROM BucketSales "
      "WHERE qty BETWEEN 6 AND 10 AND (lp BETWEEN 15 AND 25 OR cp BETWEEN 50 "
      "AND 70 OR wc BETWEEN 20 AND 30)");
  const auto b3 = RelationalRun(
      *rs_,
      "SELECT AVG(lp), COUNT(lp), COUNT(DISTINCT lp) FROM BucketSales "
      "WHERE qty BETWEEN 90 AND 95 AND (lp BETWEEN 0 AND 10 OR cp BETWEEN 0 "
      "AND 10)");
  ASSERT_EQ(b1.size(), 1U);
  ASSERT_EQ(b2.size(), 1U);
  ASSERT_EQ(b3.size(), 1U);
  EXPECT_EQ(got[0][0], b1[0][0]);
  EXPECT_EQ(got[0][1], b1[0][1]);
  EXPECT_EQ(got[0][2], b1[0][2]);
  EXPECT_EQ(got[0][3], b2[0][0]);
  EXPECT_EQ(got[0][4], b2[0][1]);
  EXPECT_EQ(got[0][5], b2[0][2]);
  EXPECT_TRUE(got[0][6].IsNull());
  EXPECT_EQ(got[0][7], Value(int64_t{0}));
  EXPECT_EQ(got[0][8], Value(int64_t{0}));
}

TEST_F(RelationalRegressionTest,
       LexicographicDimPrefixTopKMatchesCanonicalWithTies) {
  // Lean proof: lean/TinylambOpt/ResearchProofs/IndividualQueryGaps.lean
  //   (LexicographicDimPrefixTopK_sound)
  RelationalRun(*rs_, "CREATE TABLE LexStore (s_sk INT64, s_name STRING)");
  RelationalRun(*rs_, "CREATE TABLE LexItem (i_sk INT64, i_desc STRING)");
  RelationalRun(*rs_,
                "CREATE TABLE LexSales (ss_s INT64, ss_i INT64, ss_p FLOAT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, st,
                          setup.GetTable("LexStore"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, it,
                          setup.GetTable("LexItem"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, sl,
                          setup.GetTable("LexSales"));
    // 30 stores (>= 24 cost-gate threshold) with ties on s_name.
    for (int64_t s = 1; s <= 30; ++s) {
      const std::string nm =
          std::string("grp_") + static_cast<char>('a' + ((s - 1) % 5));
      ASSERT_SUCCESS(
          st->Insert(setup.txn_, Row({Value(s), Value(std::string(nm))}))
              .GetStatus());
    }
    for (int64_t i = 1; i <= 20; ++i) {
      ASSERT_SUCCESS(
          it->Insert(setup.txn_,
                     Row({Value(i), Value("item_" + std::to_string(21 - i))}))
              .GetStatus());
    }
    for (int64_t s = 1; s <= 30; ++s) {
      for (int64_t i = 1; i <= 20; ++i) {
        ASSERT_SUCCESS(
            sl->Insert(
                  setup.txn_,
                  Row({Value(s), Value(i),
                       Value(static_cast<double>((s * 7 + i * 3) % 50) + 1.0)}))
                .GetStatus());
      }
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }

  const std::string query =
      "SELECT s_name, i_desc, sc.rev, s_sk, i_sk "
      "FROM LexStore, LexItem, "
      "(SELECT ss_s, ss_i, rev, AVG(rev) OVER (PARTITION BY ss_s) AS ave "
      " FROM (SELECT ss_s, ss_i, SUM(ss_p) AS rev FROM LexSales "
      "       GROUP BY ss_s, ss_i) AS inner_agg) AS sc "
      "WHERE sc.rev <= 0.8 * sc.ave AND s_sk = sc.ss_s AND i_sk = sc.ss_i "
      "ORDER BY s_name, i_desc, s_sk, i_sk "
      "LIMIT 15";

  const auto got = RelationalRun(*rs_, query);
  ASSERT_EQ(got.size(), 15U);

  // Compare against full-table execution (OFFSET 0 and OFFSET 5) without
  // prefix cutoff by ordering on an expression (s_name || '') which bypasses
  // the bare-column prefix gate while preserving identical sort order.
  const std::string canonical =
      "SELECT s_name, i_desc, sc.rev, s_sk, i_sk "
      "FROM LexStore, LexItem, "
      "(SELECT ss_s, ss_i, rev, AVG(rev) OVER (PARTITION BY ss_s) AS ave "
      " FROM (SELECT ss_s, ss_i, SUM(ss_p) AS rev FROM LexSales "
      "       GROUP BY ss_s, ss_i) AS inner_agg) AS sc "
      "WHERE sc.rev <= 0.8 * sc.ave AND s_sk = sc.ss_s AND i_sk = sc.ss_i "
      "ORDER BY CONCAT(s_name, ''), i_desc, s_sk, i_sk "
      "LIMIT 15";
  const auto expected = RelationalRun(*rs_, canonical);
  ASSERT_EQ(got.size(), expected.size());
  for (size_t i = 0; i < got.size(); ++i) {
    EXPECT_EQ(got[i], expected[i]) << "row " << i;
  }
}

TEST_F(RelationalRegressionTest,
       MultiChannelExistsBitsetPushdownOrdersConjuncts) {
  // Lean proof: lean/TinylambOpt/ResearchProofs/IndividualQueryGaps.lean
  //   (MultiChannelExistsBitsetPushdown_sound,
  //    MultiChannelAntiExistsBitsetPushdown_sound)
  RelationalRun(*rs_,
                "CREATE TABLE McCust (c_sk INT64, c_addr INT64, c_tag STRING)");
  RelationalRun(*rs_, "CREATE TABLE McAddr (a_sk INT64, a_state STRING)");
  RelationalRun(*rs_, "CREATE TABLE McStore (s_csk INT64, s_yr INT64)");
  RelationalRun(*rs_, "CREATE TABLE McWeb (w_csk INT64, w_yr INT64)");
  RelationalRun(*rs_, "CREATE TABLE McCat (k_csk INT64, k_yr INT64)");
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, cu, setup.GetTable("McCust"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, ad, setup.GetTable("McAddr"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, st,
                          setup.GetTable("McStore"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, wb, setup.GetTable("McWeb"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, ct, setup.GetTable("McCat"));
    for (int64_t a = 1; a <= 5; ++a) {
      ASSERT_SUCCESS(
          ad->Insert(setup.txn_,
                     Row({Value(a), Value("ST" + std::to_string(a))}))
              .GetStatus());
    }
    for (int64_t c = 1; c <= 100; ++c) {
      ASSERT_SUCCESS(
          cu->Insert(setup.txn_, Row({Value(c), Value((c % 5) + 1),
                                      Value(c % 2 == 0 ? std::string("E")
                                                       : std::string("O"))}))
              .GetStatus());
      if (c % 3 == 0) {
        ASSERT_SUCCESS(
            st->Insert(setup.txn_, Row({Value(c), Value(int64_t{2002})}))
                .GetStatus());
      }
      if (c % 5 == 0) {
        ASSERT_SUCCESS(
            wb->Insert(setup.txn_, Row({Value(c), Value(int64_t{2002})}))
                .GetStatus());
      }
      if (c % 7 == 0) {
        ASSERT_SUCCESS(
            ct->Insert(setup.txn_, Row({Value(c), Value(int64_t{2002})}))
                .GetStatus());
      }
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }

  // Reversed conjunct order: `(EXISTS(W) OR EXISTS(C)) AND EXISTS(S)` must
  // match `EXISTS(S) AND (EXISTS(W) OR EXISTS(C))` identically.
  const std::string q_fwd =
      "SELECT a_state, c_tag, COUNT(*) "
      "FROM McCust, McAddr "
      "WHERE c_addr = a_sk "
      "  AND EXISTS (SELECT 1 FROM McStore WHERE s_csk = c_sk AND s_yr = 2002) "
      "  AND (EXISTS (SELECT 1 FROM McWeb WHERE w_csk = c_sk AND w_yr = 2002) "
      "       OR EXISTS (SELECT 1 FROM McCat WHERE k_csk = c_sk AND k_yr = "
      "2002)) "
      "GROUP BY a_state, c_tag ORDER BY a_state, c_tag";
  const std::string q_rev =
      "SELECT a_state, c_tag, COUNT(*) "
      "FROM McCust, McAddr "
      "WHERE c_addr = a_sk "
      "  AND (EXISTS (SELECT 1 FROM McWeb WHERE w_csk = c_sk AND w_yr = 2002) "
      "       OR EXISTS (SELECT 1 FROM McCat WHERE k_csk = c_sk AND k_yr = "
      "2002)) "
      "  AND EXISTS (SELECT 1 FROM McStore WHERE s_csk = c_sk AND s_yr = 2002) "
      "GROUP BY a_state, c_tag ORDER BY a_state, c_tag";
  const auto fwd = RelationalRun(*rs_, q_fwd);
  const auto rev = RelationalRun(*rs_, q_rev);
  ASSERT_FALSE(fwd.empty());
  EXPECT_EQ(fwd, rev);
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

}  // namespace
}  // namespace tinylamb
