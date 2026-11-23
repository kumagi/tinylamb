/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "executor/executor_feature_test.hpp"

namespace tinylamb {
// ===========================================================================
// SQL-level coverage: set operations and join shapes through SqlEngine.
// ===========================================================================

class ExecutorFeatureSqlTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "SQL frontend unavailable";
    }
    database_ =
        Database::Create("executor_feature_test-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  std::vector<Row> Exec(const std::string& sql) {
    return RunSql(engine_.get(), context_.get(), sql);
  }
  void Run(const std::string& sql) { (void)Exec(sql); }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(ExecutorFeatureSqlTest, Smoke) {
  const auto rows = Exec("SELECT 1");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
}

TEST_F(ExecutorFeatureSqlTest, SetOperationsDistinctAndAll) {
  Run("CREATE TABLE s1(x INT64)");
  Run("CREATE TABLE s2(x INT64)");
  Run("INSERT INTO s1 VALUES (1), (2), (2), (3)");
  Run("INSERT INTO s2 VALUES (2), (3), (5)");

  const auto union_rows =
      Exec("SELECT x FROM s1 UNION DISTINCT SELECT x FROM s2 ORDER BY x");
  ASSERT_EQ(union_rows.size(), 4U);
  EXPECT_EQ(union_rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(union_rows[1][0], Value(int64_t{2}));
  EXPECT_EQ(union_rows[2][0], Value(int64_t{3}));
  EXPECT_EQ(union_rows[3][0], Value(int64_t{5}));

  const auto union_all =
      Exec("SELECT x FROM s1 UNION ALL SELECT x FROM s2 ORDER BY x");
  ASSERT_EQ(union_all.size(), 7U);

  const auto intersect =
      Exec("SELECT x FROM s1 INTERSECT DISTINCT SELECT x FROM s2 ORDER BY x");
  ASSERT_EQ(intersect.size(), 2U);
  EXPECT_EQ(intersect[0][0], Value(int64_t{2}));
  EXPECT_EQ(intersect[1][0], Value(int64_t{3}));

  // INTERSECT ALL keeps min(left, right) multiplicity: 2 appears twice on
  // the left but once on the right.
  const auto intersect_all =
      Exec("SELECT x FROM s1 INTERSECT ALL SELECT x FROM s2 ORDER BY x");
  ASSERT_EQ(intersect_all.size(), 2U);
  EXPECT_EQ(intersect_all[0][0], Value(int64_t{2}));
  EXPECT_EQ(intersect_all[1][0], Value(int64_t{3}));

  const auto except =
      Exec("SELECT x FROM s1 EXCEPT DISTINCT SELECT x FROM s2 ORDER BY x");
  ASSERT_EQ(except.size(), 1U);
  EXPECT_EQ(except[0][0], Value(int64_t{1}));

  // EXCEPT ALL consumes one right-hand copy per left occurrence: the
  // duplicate 2 on the left survives once.
  const auto except_all =
      Exec("SELECT x FROM s1 EXCEPT ALL SELECT x FROM s2 ORDER BY x");
  ASSERT_EQ(except_all.size(), 2U);
  EXPECT_EQ(except_all[0][0], Value(int64_t{1}));
  EXPECT_EQ(except_all[1][0], Value(int64_t{2}));
}

TEST_F(ExecutorFeatureSqlTest, SetOperationsWithNulls) {
  Run("CREATE TABLE n1(x INT64)");
  Run("CREATE TABLE n2(x INT64)");
  Run("INSERT INTO n1 VALUES (1), (NULL), (NULL)");
  Run("INSERT INTO n2 VALUES (1), (NULL)");

  // UNION deduplicates NULLs to a single row.
  const auto union_rows =
      Exec("SELECT x FROM n1 UNION DISTINCT SELECT x FROM n2 ORDER BY x");
  ASSERT_EQ(union_rows.size(), 2U);
  EXPECT_TRUE(union_rows[0][0].IsNull());
  EXPECT_EQ(union_rows[1][0], Value(int64_t{1}));

  // INTERSECT: NULL matches NULL in set operations.
  const auto intersect =
      Exec("SELECT x FROM n1 INTERSECT DISTINCT SELECT x FROM n2");
  ASSERT_EQ(intersect.size(), 2U);

  // EXCEPT removes the matching NULL too.
  const auto except = Exec("SELECT x FROM n1 EXCEPT DISTINCT SELECT x FROM n2");
  EXPECT_TRUE(except.empty());

  // EXCEPT ALL: left has two NULLs, right one -> one NULL survives.
  const auto except_all = Exec("SELECT x FROM n1 EXCEPT ALL SELECT x FROM n2");
  ASSERT_EQ(except_all.size(), 1U);
  EXPECT_TRUE(except_all[0][0].IsNull());
}

TEST_F(ExecutorFeatureSqlTest, SetOperationTypeCoercion) {
  Run("CREATE TABLE ci(x INT64)");
  Run("CREATE TABLE cd(x DOUBLE)");
  Run("INSERT INTO ci VALUES (1), (2)");
  Run("INSERT INTO cd VALUES (2.5), (0.5)");
  const auto mixed =
      Exec("SELECT x FROM ci UNION DISTINCT SELECT x FROM cd ORDER BY x");
  ASSERT_EQ(mixed.size(), 4U);
  EXPECT_EQ(mixed[0][0].type, ValueType::kDouble);
  EXPECT_EQ(mixed[0][0], Value(0.5));
  EXPECT_EQ(mixed[1][0], Value(1.0));
  EXPECT_EQ(mixed[2][0], Value(2.0));
  EXPECT_EQ(mixed[3][0], Value(2.5));

  // INT64 2 and DOUBLE 2.0 dedup to one row under UNION DISTINCT.
  Run("INSERT INTO cd VALUES (2.0)");
  const auto dedup = Exec("SELECT x FROM ci UNION DISTINCT SELECT x FROM cd");
  EXPECT_EQ(dedup.size(), 4U);
}

TEST_F(ExecutorFeatureSqlTest, MultiSourceUnionAllChains) {
  const auto rows =
      Exec("SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY v");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(rows[1][0], Value(int64_t{2}));
  EXPECT_EQ(rows[2][0], Value(int64_t{3}));
}

TEST_F(ExecutorFeatureSqlTest, OuterJoinShapesWithNullAndDuplicateKeys) {
  Run("CREATE TABLE l(a INT64, lv VARCHAR)");
  Run("CREATE TABLE r(b INT64, rv VARCHAR)");
  Run("INSERT INTO l VALUES (1, 'l1'), (1, 'l1b'), (2, 'l2'), (NULL, 'ln')");
  Run("INSERT INTO r VALUES (1, 'r1'), (1, 'r1b'), (3, 'r3'), (NULL, 'rn')");

  const auto inner =
      Exec("SELECT l.a, lv, b, rv FROM l JOIN r ON l.a = r.b ORDER BY lv, rv");
  ASSERT_EQ(inner.size(), 4U);  // 2 x 2 matches on key 1

  const auto left =
      Exec("SELECT l.a, lv, b, rv FROM l LEFT JOIN r ON l.a = r.b ORDER BY lv");
  ASSERT_EQ(left.size(), 6U);  // 4 matches + l2 + the NULL-keyed row
  // The NULL-keyed left row survives with a NULL-padded right side.
  EXPECT_EQ(left[5][1], Value("ln"));
  EXPECT_TRUE(left[5][2].IsNull());

  const auto right = Exec(
      "SELECT l.a, lv, b, rv FROM l RIGHT JOIN r ON l.a = r.b ORDER BY rv");
  ASSERT_EQ(right.size(), 6U);
  EXPECT_EQ(right[5][3], Value("rn"));
  EXPECT_TRUE(right[5][0].IsNull());

  const auto full = Exec(
      "SELECT l.a, lv, b, rv FROM l FULL JOIN r ON l.a = r.b ORDER BY lv, rv");
  ASSERT_EQ(full.size(), 8U);  // 4 matches + 2 unmatched per side
}

TEST_F(ExecutorFeatureSqlTest, JoinKeysOfEveryType) {
  Run("CREATE TABLE sl(s VARCHAR, v INT64)");
  Run("CREATE TABLE sr(s VARCHAR, w INT64)");
  Run("INSERT INTO sl VALUES ('a', 1), ('b', 2), ('', 3)");
  Run("INSERT INTO sr VALUES ('a', 10), ('', 30), ('c', 40)");
  const auto string_join =
      Exec("SELECT sl.v, sr.w FROM sl JOIN sr ON sl.s = sr.s ORDER BY sl.v");
  ASSERT_EQ(string_join.size(), 2U);
  EXPECT_EQ(string_join[0][0], Value(int64_t{1}));
  EXPECT_EQ(string_join[0][1], Value(int64_t{10}));
  EXPECT_EQ(string_join[1][0], Value(int64_t{3}));
  EXPECT_EQ(string_join[1][1], Value(int64_t{30}));

  Run("CREATE TABLE dl(d DOUBLE, v INT64)");
  Run("CREATE TABLE dr(d DOUBLE, w INT64)");
  Run("INSERT INTO dl VALUES (-0.0, 1), (1.5, 2)");
  Run("INSERT INTO dr VALUES (0.0, 10), (1.5, 20)");
  // -0.0 and 0.0 are the same key in SQL equality.
  const auto double_join =
      Exec("SELECT dl.v, dr.w FROM dl JOIN dr ON dl.d = dr.d ORDER BY dl.v");
  ASSERT_EQ(double_join.size(), 2U);
  EXPECT_EQ(double_join[0][1], Value(int64_t{10}));
  EXPECT_EQ(double_join[1][1], Value(int64_t{20}));

  Run("CREATE TABLE tl(d DATE, v INT64)");
  Run("CREATE TABLE tr(d DATE, w INT64)");
  Run("INSERT INTO tl VALUES (DATE '2020-01-01', 1), (DATE '2021-06-15', 2)");
  Run("INSERT INTO tr VALUES (DATE '2020-01-01', 10), (DATE '1999-12-31', 20)");
  const auto date_join =
      Exec("SELECT tl.v, tr.w FROM tl JOIN tr ON tl.d = tr.d ORDER BY tl.v");
  ASSERT_EQ(date_join.size(), 1U);
  EXPECT_EQ(date_join[0][1], Value(int64_t{10}));
}

TEST_F(ExecutorFeatureSqlTest, MixedNumericJoinUsesCoercingFallback) {
  Run("CREATE TABLE mixed_i(i INT64)");
  Run("CREATE TABLE mixed_d(d DOUBLE)");
  Run("INSERT INTO mixed_i VALUES (1), (2)");
  Run("INSERT INTO mixed_d VALUES (1.0), (2.5)");
  const auto rows = Exec(
      "SELECT mixed_i.i, mixed_d.d FROM mixed_i JOIN mixed_d "
      "ON mixed_i.i = mixed_d.d ORDER BY mixed_i.i");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[0][1], Value(1.0));
}

TEST_F(ExecutorFeatureSqlTest, MultiKeyJoinAndNullSafeEquiJoin) {
  Run("CREATE TABLE mk1(a INT64, b INT64, v VARCHAR)");
  Run("CREATE TABLE mk2(a INT64, b INT64, w VARCHAR)");
  Run("INSERT INTO mk1 VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z')");
  Run("INSERT INTO mk2 VALUES (1, 1, 'p'), (1, 2, 'q'), (2, 2, 'r')");
  const auto multi = Exec(
      "SELECT mk1.v, mk2.w FROM mk1 JOIN mk2 ON mk1.a = mk2.a AND mk1.b = "
      "mk2.b ORDER BY mk1.v");
  ASSERT_EQ(multi.size(), 2U);
  EXPECT_EQ(multi[0][0], Value("x"));
  EXPECT_EQ(multi[0][1], Value("p"));
  EXPECT_EQ(multi[1][0], Value("y"));
  EXPECT_EQ(multi[1][1], Value("q"));

  Run("CREATE TABLE ns1(a INT64)");
  Run("CREATE TABLE ns2(b INT64)");
  Run("INSERT INTO ns1 VALUES (1), (NULL), (2)");
  Run("INSERT INTO ns2 VALUES (NULL), (2), (3)");
  const auto null_safe = Exec(
      "SELECT ns1.a, ns2.b FROM ns1 JOIN ns2 ON ns1.a IS NOT DISTINCT FROM "
      "ns2.b ORDER BY ns1.a");
  // NULL matches NULL; 2 matches 2; unmatched (1) and (3) drop.
  ASSERT_EQ(null_safe.size(), 2U);
  EXPECT_TRUE(null_safe[0][0].IsNull());
  EXPECT_TRUE(null_safe[0][1].IsNull());
  EXPECT_EQ(null_safe[1][0], Value(int64_t{2}));
}

TEST_F(ExecutorFeatureSqlTest, NullSafeOuterKindsKeepNullAndOuterSemantics) {
  Run("CREATE TABLE nso_l(id INT64, k INT64)");
  Run("CREATE TABLE nso_r(id INT64, k INT64)");
  Run("INSERT INTO nso_l VALUES (1, NULL), (2, NULL), (3, 1), (4, NULL), "
      "(5, 2)");
  Run("INSERT INTO nso_r VALUES (10, NULL), (11, NULL), (12, 1), (13, 2), "
      "(14, 3)");

  EXPECT_EQ(Exec("SELECT l.id, r.id FROM nso_l l LEFT JOIN nso_r r "
                 "ON l.k IS NOT DISTINCT FROM r.k ORDER BY l.id, r.id"),
            (std::vector<Row>{
                Row({Value(1), Value(10)}), Row({Value(1), Value(11)}),
                Row({Value(2), Value(10)}), Row({Value(2), Value(11)}),
                Row({Value(3), Value(12)}), Row({Value(4), Value(10)}),
                Row({Value(4), Value(11)}), Row({Value(5), Value(13)})}));
  EXPECT_EQ(
      Exec("SELECT l.id, r.id FROM nso_l l RIGHT JOIN nso_r r "
           "ON l.k IS NOT DISTINCT FROM r.k ORDER BY r.id, l.id"),
      (std::vector<Row>{Row({Value(1), Value(10)}), Row({Value(2), Value(10)}),
                        Row({Value(4), Value(10)}), Row({Value(1), Value(11)}),
                        Row({Value(2), Value(11)}), Row({Value(4), Value(11)}),
                        Row({Value(3), Value(12)}), Row({Value(5), Value(13)}),
                        Row({Value(), Value(14)})}));
  EXPECT_EQ(
      Exec("SELECT l.id, r.id FROM nso_l l FULL JOIN nso_r r "
           "ON l.k IS NOT DISTINCT FROM r.k ORDER BY r.id, l.id"),
      (std::vector<Row>{Row({Value(1), Value(10)}), Row({Value(2), Value(10)}),
                        Row({Value(4), Value(10)}), Row({Value(1), Value(11)}),
                        Row({Value(2), Value(11)}), Row({Value(4), Value(11)}),
                        Row({Value(3), Value(12)}), Row({Value(5), Value(13)}),
                        Row({Value(), Value(14)})}));

  EXPECT_EQ(
      Exec("SELECT l.id, r.id FROM nso_l l LEFT JOIN nso_r r "
           "ON l.k = r.k ORDER BY l.id, r.id"),
      (std::vector<Row>{Row({Value(1), Value()}), Row({Value(2), Value()}),
                        Row({Value(3), Value(12)}), Row({Value(4), Value()}),
                        Row({Value(5), Value(13)})}));
  const auto distinct_count = Exec(
      "SELECT COUNT(*) FROM nso_l l JOIN nso_r r "
      "ON l.k IS DISTINCT FROM r.k");
  ASSERT_EQ(distinct_count.size(), 1U);
  EXPECT_EQ(distinct_count[0][0], Value(int64_t{17}));
  EXPECT_EQ(Exec("SELECT l.id, r.id FROM nso_l l LEFT JOIN nso_r r "
                 "ON l.k IS DISTINCT FROM r.k WHERE l.id IN (1, 4) "
                 "ORDER BY l.id, r.id"),
            (std::vector<Row>{
                Row({Value(1), Value(12)}), Row({Value(1), Value(13)}),
                Row({Value(1), Value(14)}), Row({Value(4), Value(12)}),
                Row({Value(4), Value(13)}), Row({Value(4), Value(14)})}));

  std::string plan;
  for (const Row& row :
       Exec("EXPLAIN SELECT l.id, r.id FROM nso_l l LEFT JOIN nso_r r "
            "ON l.k IS NOT DISTINCT FROM r.k")) {
    plan += row[0].AsString();
    plan.push_back('\n');
  }
  EXPECT_NE(plan.find("LeftHashJoin null_safe=true"), std::string::npos)
      << plan;
  EXPECT_EQ(plan.find("SemiHashJoin"), std::string::npos) << plan;
  EXPECT_EQ(plan.find("AntiHashJoin"), std::string::npos) << plan;
  EXPECT_EQ(plan.find("IndexJoin"), std::string::npos) << plan;
  EXPECT_EQ(plan.find("MergeJoin"), std::string::npos) << plan;

  Run("CREATE TABLE nso_empty(id INT64, k INT64)");
  const auto empty_left = Exec(
      "SELECT l.id, e.id FROM nso_l l LEFT JOIN nso_empty e "
      "ON l.k IS NOT DISTINCT FROM e.k ORDER BY l.id");
  ASSERT_EQ(empty_left.size(), 5U);
  for (const Row& row : empty_left) {
    EXPECT_TRUE(row[1].IsNull());
  }
  const auto empty_full = Exec(
      "SELECT l.id, e.id FROM nso_l l FULL JOIN nso_empty e "
      "ON l.k IS NOT DISTINCT FROM e.k ORDER BY l.id");
  ASSERT_EQ(empty_full.size(), 5U);
  for (const Row& row : empty_full) {
    EXPECT_TRUE(row[1].IsNull());
  }

  EXPECT_THROW((void)Exec("SELECT l.id, r.id FROM nso_l l LEFT JOIN nso_r r "
                          "ON l.k IS NOT DISTINCT FROM r.missing"),
               std::runtime_error);
}

TEST_F(ExecutorFeatureSqlTest, ScalarSubquerySingleJoin) {
  Run("CREATE TABLE s1(a INT64)");
  Run("CREATE TABLE s2(b INT64)");
  Run("INSERT INTO s1 VALUES (1), (2), (3)");
  Run("INSERT INTO s2 VALUES (2)");
  const auto rows = Exec(
      "SELECT s1.a, (SELECT s2.b FROM s2 WHERE s2.b = s1.a) FROM s1 ORDER BY "
      "s1.a");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_TRUE(rows[0][1].IsNull());
  EXPECT_EQ(rows[1][1], Value(int64_t{2}));
  EXPECT_TRUE(rows[2][1].IsNull());

  // A scalar subquery matching more than one row is a runtime error.
  Run("INSERT INTO s2 VALUES (2)");
  EXPECT_THROW(
      (void)Exec("SELECT s1.a, (SELECT s2.b FROM s2 WHERE s2.b = s1.a) FROM "
                 "s1"),
      std::runtime_error);
}

TEST_F(ExecutorFeatureSqlTest, SemiAntiAndNullAwareAntiSubqueries) {
  Run("CREATE TABLE e1(a INT64)");
  Run("CREATE TABLE e2(b INT64)");
  Run("INSERT INTO e1 VALUES (1), (2), (3), (NULL)");
  Run("INSERT INTO e2 VALUES (2), (NULL)");

  const auto semi = Exec("SELECT a FROM e1 WHERE a IN (SELECT b FROM e2)");
  ASSERT_EQ(semi.size(), 1U);
  EXPECT_EQ(semi[0][0], Value(int64_t{2}));

  // NOT EXISTS: NULL probe keys match nothing, so they survive.  NULLs sort
  // first under the default ascending ordering.
  const auto anti = Exec(
      "SELECT a FROM e1 AS e1x WHERE NOT EXISTS (SELECT 1 FROM e2 WHERE e2.b "
      "= e1x.a) ORDER BY a");
  ASSERT_EQ(anti.size(), 3U);
  EXPECT_TRUE(anti[0][0].IsNull());
  EXPECT_EQ(anti[1][0], Value(int64_t{1}));
  EXPECT_EQ(anti[2][0], Value(int64_t{3}));

  // NOT IN with a NULL in the set is UNKNOWN for non-matching rows: nothing
  // survives.
  const auto not_in_null =
      Exec("SELECT a FROM e1 WHERE a NOT IN (SELECT b FROM e2)");
  EXPECT_TRUE(not_in_null.empty());

  Run("CREATE TABLE e3(b INT64)");
  Run("INSERT INTO e3 VALUES (2)");
  const auto not_in =
      Exec("SELECT a FROM e1 WHERE a NOT IN (SELECT b FROM e3) ORDER BY a");
  ASSERT_EQ(not_in.size(), 2U);
  EXPECT_EQ(not_in[0][0], Value(int64_t{1}));
  EXPECT_EQ(not_in[1][0], Value(int64_t{3}));
}

TEST_F(ExecutorFeatureSqlTest, EmptySideJoins) {
  Run("CREATE TABLE empty_t(a INT64)");
  Run("CREATE TABLE one(a INT64)");
  Run("INSERT INTO one VALUES (1)");
  EXPECT_TRUE(
      Exec("SELECT * FROM one JOIN empty_t ON one.a = empty_t.a").empty());
  const auto left = Exec(
      "SELECT one.a, empty_t.a FROM one LEFT JOIN empty_t ON one.a = "
      "empty_t.a");
  ASSERT_EQ(left.size(), 1U);
  EXPECT_EQ(left[0][0], Value(int64_t{1}));
  EXPECT_TRUE(left[0][1].IsNull());
  const auto full = Exec(
      "SELECT one.a, empty_t.a FROM one FULL JOIN empty_t ON one.a = "
      "empty_t.a");
  ASSERT_EQ(full.size(), 1U);
  EXPECT_TRUE(
      Exec("SELECT * FROM empty_t JOIN one ON empty_t.a = one.a").empty());
}

TEST_F(ExecutorFeatureSqlTest, CrossJoin) {
  Run("CREATE TABLE cj(a INT64)");
  Run("INSERT INTO cj VALUES (1), (2)");
  const auto cross = Exec("SELECT x.a, y.a FROM cj x, cj y ORDER BY x.a, y.a");
  ASSERT_EQ(cross.size(), 4U);
  EXPECT_EQ(cross[0][0], Value(int64_t{1}));
  EXPECT_EQ(cross[0][1], Value(int64_t{1}));
  EXPECT_EQ(cross[3][0], Value(int64_t{2}));
  EXPECT_EQ(cross[3][1], Value(int64_t{2}));
}

TEST_F(ExecutorFeatureSqlTest, SetOperationSpillsUnderTinyBudget) {
  Run("CREATE TABLE big1(x INT64)");
  Run("CREATE TABLE big2(x INT64)");
  for (int64_t base = 0; base < 4; ++base) {
    std::string values;
    values.reserve(40 * 6);
    for (int64_t i = 0; i < 40; ++i) {
      if (i != 0) {
        values += ", ";
      }
      values += "(" + std::to_string((base * 10) + (i % 10)) + ")";
    }
    Run("INSERT INTO big1 VALUES " + values);
    Run("INSERT INTO big2 VALUES " + values);
  }
  BudgetGuard guard(2048);
  // base*10 + i%10 yields 40 distinct values repeated 4x in each table.
  const auto union_rows =
      Exec("SELECT x FROM big1 UNION DISTINCT SELECT x FROM big2 ORDER BY x");
  EXPECT_EQ(union_rows.size(), 40U);
  const auto intersect = Exec(
      "SELECT x FROM big1 INTERSECT DISTINCT SELECT x FROM big2 ORDER BY x");
  EXPECT_EQ(intersect.size(), 40U);
  // UNION ALL used to be exempt from the budget check although its input is
  // materialized identically. Under the same tiny budget it must still return
  // every row from both sources.
  const auto union_all =
      Exec("SELECT x FROM big1 UNION ALL SELECT x FROM big2 ORDER BY x");
  EXPECT_EQ(union_all.size(), 320U);
}

// ===========================================================================
// JoinHashIndex: open-addressing index, growth for both key modes.
// ===========================================================================

TEST(JoinHashIndexTest, InsertFindAndChainInt64Mode) {
  JoinHashIndex index;
  index.Init(JoinHashIndex::KeyMode::kInt64, 4);
  for (int64_t k = 0; k < 30; ++k) {
    index.Insert(JoinHashIndex::HashInt64(k), k, {}, static_cast<size_t>(k));
  }
  // Duplicate key chains in LIFO order.
  index.Insert(JoinHashIndex::HashInt64(7), 7, {}, 1000);
  const size_t head = index.Find(JoinHashIndex::HashInt64(7), 7, {});
  ASSERT_NE(head, JoinHashIndex::kNil);
  EXPECT_EQ(index.RowIndex(head), 1000U);
  const size_t second = index.ChainNext(head);
  EXPECT_EQ(index.RowIndex(second), 7U);
  EXPECT_EQ(index.ChainNext(second), JoinHashIndex::kNil);

  EXPECT_EQ(index.Find(JoinHashIndex::HashInt64(12345), 12345, {}),
            JoinHashIndex::kNil);
  // Every key remains findable after the rehash inside Grow().  Key 7 now
  // chains to the duplicate inserted above, so only its original entry is
  // checked here.
  for (int64_t k = 0; k < 30; ++k) {
    if (k == 7) {
      continue;
    }
    const size_t e = index.Find(JoinHashIndex::HashInt64(k), k, {});
    ASSERT_NE(e, JoinHashIndex::kNil);
    EXPECT_EQ(index.RowIndex(e), static_cast<size_t>(k));
  }
}

TEST(JoinHashIndexTest, InsertFindAndChainBytesMode) {
  JoinHashIndex index;
  index.Init(JoinHashIndex::KeyMode::kBytes, 4);
  const std::string k1 = "alpha";
  const std::string k2 = "beta";
  index.Insert(JoinHashIndex::HashBytes(k1), 0, k1, 0);
  index.Insert(JoinHashIndex::HashBytes(k2), 0, k2, 1);
  index.Insert(JoinHashIndex::HashBytes(k1), 0, k1, 2);  // duplicate chain

  const size_t head = index.Find(JoinHashIndex::HashBytes(k1), 0, k1);
  ASSERT_NE(head, JoinHashIndex::kNil);
  EXPECT_EQ(index.RowIndex(head), 2U);
  const size_t next = index.ChainNext(head);
  EXPECT_EQ(index.RowIndex(next), 0U);
  EXPECT_EQ(index.ChainNext(next), JoinHashIndex::kNil);

  const size_t beta = index.Find(JoinHashIndex::HashBytes(k2), 0, k2);
  ASSERT_NE(beta, JoinHashIndex::kNil);
  EXPECT_EQ(index.RowIndex(beta), 1U);
  EXPECT_EQ(index.Find(JoinHashIndex::HashBytes("missing"), 0, "missing"),
            JoinHashIndex::kNil);
}

// ===========================================================================
// HashJoin kinds that the SQL planner rarely emits: kSingle and kMark.
// ===========================================================================

TEST(HashJoinKindTest, SingleJoinMatchesOnceAndPadsMisses) {
  auto left = Src({Row({Value(1), Value("a")}), Row({Value(2), Value("b")}),
                   Row({Value(3), Value("c")})});
  auto right = Src({Row({Value(2), Value("x")})});
  HashJoin join(left, {0}, right, {0}, HashJoinMode::kInMemory,
                JoinKind::kSingle, /*worker_count=*/2, /*right_width=*/2,
                /*left_width=*/2);
  const std::vector<Row> out = Drain(&join);
  ASSERT_EQ(out.size(), 3U);
  // Left rows 1 and 3 miss -> NULL-padded right side.
  EXPECT_EQ(out[0], (Row({Value(1), Value("a"), Value(), Value()})));
  EXPECT_EQ(out[1], (Row({Value(2), Value("b"), Value(2), Value("x")})));
  EXPECT_EQ(out[2], (Row({Value(3), Value("c"), Value(), Value()})));
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
}

TEST(HashJoinKindTest, SingleJoinRejectsDuplicateMatch) {
  auto left = Src({IRow(1)});
  auto right = Src({Row({Value(1), Value("x")}), Row({Value(1), Value("y")})});
  HashJoin join(left, {0}, right, {0}, HashJoinMode::kInMemory,
                JoinKind::kSingle, 2, 2, 1);
  const std::vector<Row> out = Drain(&join);
  EXPECT_TRUE(out.empty());
  EXPECT_NE(join.GetStatus(), Status::kSuccess);
}

TEST(HashJoinKindTest, SingleJoinReloadsSpilledSides) {
  std::vector<Row> left_rows;
  left_rows.reserve(120);
  std::vector<Row> right_rows;
  right_rows.reserve(60);
  for (int64_t i = 0; i < 120; ++i) {
    left_rows.push_back(IRow(i));
    if (i % 2 == 0) {
      right_rows.push_back(IRow(i));
    }
  }
  BudgetGuard guard(2048);  // forces reactive spill + LoadAll on both sides
  HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                HashJoinMode::kInMemory, JoinKind::kSingle, 2, 1, 1);
  const std::vector<Row> out = Drain(&join);
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  ASSERT_EQ(out.size(), 120U);
  int64_t matched = 0;
  for (const Row& row : out) {
    if (!row[1].IsNull()) {
      ++matched;
      EXPECT_EQ(row[1], row[0]);
    }
  }
  EXPECT_EQ(matched, 60);
}

TEST(HashJoinKindTest, MarkJoinMarkerValues) {
  // Build side without NULLs: match -> TRUE, miss -> FALSE.
  HashJoin join(Src({IRow(1), IRow(2)}), {0}, Src({IRow(2)}), {0},
                HashJoinMode::kInMemory, JoinKind::kMark, 2, 1, 1);
  const std::vector<Row> out = Drain(&join);
  ASSERT_EQ(out.size(), 2U);
  EXPECT_EQ(out[0], (Row({Value(1), Value(false)})));
  EXPECT_EQ(out[1], (Row({Value(2), Value(true)})));

  // A NULL build key turns a miss into UNKNOWN (NULL marker).
  HashJoin join2(Src({IRow(1), IRow(2)}), {0}, Src({IRow(2), Row({Value()})}),
                 {0}, HashJoinMode::kInMemory, JoinKind::kMark, 2, 1, 1);
  const std::vector<Row> out2 = Drain(&join2);
  ASSERT_EQ(out2.size(), 2U);
  EXPECT_EQ(out2[0], (Row({Value(1), Value()})));
  EXPECT_EQ(out2[1], (Row({Value(2), Value(true)})));

  // Empty build side: every marker is FALSE.
  HashJoin join3(Src({IRow(1)}), {0}, Src(std::vector<Row>{}), {0},
                 HashJoinMode::kInMemory, JoinKind::kMark, 2, 1, 1);
  const std::vector<Row> out3 = Drain(&join3);
  ASSERT_EQ(out3.size(), 1U);
  EXPECT_EQ(out3[0], (Row({Value(1), Value(false)})));

  // NULL probe key over a non-empty build side is UNKNOWN.
  HashJoin join4(Src({Row({Value()})}), {0}, Src({IRow(2)}), {0},
                 HashJoinMode::kInMemory, JoinKind::kMark, 2, 1, 1);
  const std::vector<Row> out4 = Drain(&join4);
  ASSERT_EQ(out4.size(), 1U);
  EXPECT_TRUE(out4[0][1].IsNull());
}

TEST(HashJoinKindTest, MarkJoinSpilledBuildSideAccumulatesMatches) {
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  left_rows.reserve(200);
  right_rows.reserve(10);
  for (int64_t i = 0; i < 200; ++i) {
    left_rows.push_back(IRow(i % 20));
  }
  for (int64_t i = 0; i < 10; ++i) {
    right_rows.push_back(IRow(i * 2));  // even keys 0..18
  }
  BudgetGuard guard(2048);  // spills the build side -> partitioned lookup
  HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                HashJoinMode::kInMemory, JoinKind::kMark, 2, 1, 1);
  const std::vector<Row> out = Drain(&join);
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  ASSERT_EQ(out.size(), 200U);
  size_t true_markers = 0;
  size_t false_markers = 0;
  for (const Row& row : out) {
    if (row[1] == Value(true)) {
      ++true_markers;
    } else if (row[1] == Value(false)) {
      ++false_markers;
    }
  }
  // 200 probe rows over 20 distinct keys, 10 of them matching.
  EXPECT_EQ(true_markers, 100U);
  EXPECT_EQ(false_markers, 100U);
}

// ===========================================================================
// HashJoin outer / semi / anti variants.
// ===========================================================================

TEST(HashJoinOuterTest, LeftRightFullOuterWithNullKeys) {
  const auto left_rows = std::vector<Row>{
      Row({Value(1), Value("l1")}),
      Row({Value(), Value("lnull")}),
      Row({Value(3), Value("l3")}),
  };
  const auto right_rows = std::vector<Row>{
      Row({Value(3), Value("r3")}),
      Row({Value(4), Value("r4")}),
      Row({Value(), Value("rnull")}),
  };
  {
    HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                  HashJoinMode::kInMemory, JoinKind::kLeftOuter, 2, 2, 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 3U);
    bool saw_null_pad = false;
    for (const Row& row : out) {
      EXPECT_EQ(row.values_.size(), 4U);
      if (row[0].IsNull()) {
        saw_null_pad = true;
        EXPECT_TRUE(row[2].IsNull());
      }
      if (row[0] == Value(3)) {
        EXPECT_EQ(row[3], Value("r3"));
      }
    }
    EXPECT_TRUE(saw_null_pad);
  }
  {
    HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                  HashJoinMode::kInMemory, JoinKind::kRightOuter, 2, 2, 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 3U);
    bool saw_right_pad = false;
    for (const Row& row : out) {
      if (row[3] == Value("r4")) {
        saw_right_pad = true;
        EXPECT_TRUE(row[0].IsNull());
      }
    }
    EXPECT_TRUE(saw_right_pad);
  }
  {
    HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                  HashJoinMode::kInMemory, JoinKind::kFullOuter, 2, 2, 2);
    const std::vector<Row> out = Drain(&join);
    // 1 match + 2 unmatched left + 2 unmatched right.
    ASSERT_EQ(out.size(), 5U);
  }
}

TEST(HashJoinOuterTest, NonRejectingResidualPadsEveryOuterKind) {
  const Schema left_schema(
      "l", {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64)});
  const Schema right_schema(
      "r", {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64)});
  const Schema combined = left_schema + right_schema;
  const std::vector<Row> left_rows{Row({Value(int64_t{1}), Value(int64_t{10})}),
                                   Row({Value(int64_t{1}), Value(int64_t{11})}),
                                   Row({Value(int64_t{2}), Value(int64_t{20})}),
                                   Row({Value(int64_t{3}), Value(int64_t{30})}),
                                   Row({Value(), Value(int64_t{40})})};
  const std::vector<Row> right_rows{
      Row({Value(int64_t{1}), Value(int64_t{100})}),
      Row({Value(int64_t{1}), Value(int64_t{101})}),
      Row({Value(int64_t{2}), Value(int64_t{0})}),
      Row({Value(int64_t{3}), Value()}),
      Row({Value(int64_t{4}), Value(int64_t{400})})};
  const Expression residual = BinaryExpressionExp(
      FunctionCallExp("coalesce", {ColumnValueExp(ColumnName("r", "v")),
                                   ConstantValueExp(Value(int64_t{0}))}),
      BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{0})));
  const auto run = [&](JoinKind kind) {
    HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                  HashJoinMode::kInMemory, kind, 2, 2, 2);
    join.SetJoinResidual(residual, combined);
    return Drain(&join);
  };

  ExpectSameMultiset(
      run(JoinKind::kLeftOuter),
      {Row({Value(int64_t{1}), Value(int64_t{10}), Value(), Value()}),
       Row({Value(int64_t{1}), Value(int64_t{11}), Value(), Value()}),
       Row({Value(int64_t{2}), Value(int64_t{20}), Value(int64_t{2}),
            Value(int64_t{0})}),
       Row({Value(int64_t{3}), Value(int64_t{30}), Value(int64_t{3}), Value()}),
       Row({Value(), Value(int64_t{40}), Value(), Value()})});
  ExpectSameMultiset(
      run(JoinKind::kRightOuter),
      {Row({Value(), Value(), Value(int64_t{1}), Value(int64_t{100})}),
       Row({Value(), Value(), Value(int64_t{1}), Value(int64_t{101})}),
       Row({Value(int64_t{2}), Value(int64_t{20}), Value(int64_t{2}),
            Value(int64_t{0})}),
       Row({Value(int64_t{3}), Value(int64_t{30}), Value(int64_t{3}), Value()}),
       Row({Value(), Value(), Value(int64_t{4}), Value(int64_t{400})})});
  ExpectSameMultiset(
      run(JoinKind::kFullOuter),
      {Row({Value(int64_t{1}), Value(int64_t{10}), Value(), Value()}),
       Row({Value(int64_t{1}), Value(int64_t{11}), Value(), Value()}),
       Row({Value(int64_t{2}), Value(int64_t{20}), Value(int64_t{2}),
            Value(int64_t{0})}),
       Row({Value(int64_t{3}), Value(int64_t{30}), Value(int64_t{3}), Value()}),
       Row({Value(), Value(int64_t{40}), Value(), Value()}),
       Row({Value(), Value(), Value(int64_t{1}), Value(int64_t{100})}),
       Row({Value(), Value(), Value(int64_t{1}), Value(int64_t{101})}),
       Row({Value(), Value(), Value(int64_t{4}), Value(int64_t{400})})});

  HashJoin empty(Src(std::vector<Row>{}), {0}, Src(std::vector<Row>{}), {0},
                 HashJoinMode::kInMemory, JoinKind::kFullOuter, 2, 2, 2);
  empty.SetJoinResidual(residual, combined);
  EXPECT_TRUE(Drain(&empty).empty());

  const Expression bad_residual =
      CastExpressionExp(ConstantValueExp(Value("not-a-date")), "DATE");
  HashJoin error(
      Src(std::vector<Row>{Row({Value(int64_t{1}), Value(int64_t{10})})}), {0},
      Src(std::vector<Row>{Row({Value(int64_t{1}), Value(int64_t{0})})}), {0},
      HashJoinMode::kInMemory, JoinKind::kLeftOuter, 2, 2, 2);
  error.SetJoinResidual(bad_residual, combined);
  Row error_row;
  EXPECT_FALSE(error.Next(&error_row, nullptr));
  EXPECT_NE(error.GetStatus(), Status::kSuccess);
}

TEST(HashJoinOuterTest, NullSafeOuterKindsPreserveNullAndUnmatchedRows) {
  const std::vector<Row> left_rows{
      Row({Value(), Value("l-null-1")}), Row({Value(), Value("l-null-2")}),
      Row({Value(1), Value("l-one")}), Row({Value(3), Value("l-three")})};
  const std::vector<Row> right_rows{
      Row({Value(), Value("r-null-1")}), Row({Value(), Value("r-null-2")}),
      Row({Value(1), Value("r-one")}), Row({Value(2), Value("r-two")})};
  const auto run = [&](JoinKind kind, bool null_safe) {
    HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                  HashJoinMode::kInMemory, kind, 2, 2, 2);
    if (null_safe) {
      join.SetJoinAnnotations({true}, Expression());
    }
    const std::vector<Row> output = Drain(&join);
    EXPECT_EQ(join.GetStatus(), Status::kSuccess);
    return output;
  };

  ExpectSameMultiset(
      run(JoinKind::kLeftOuter, true),
      {Row({Value(), Value("l-null-1"), Value(), Value("r-null-1")}),
       Row({Value(), Value("l-null-1"), Value(), Value("r-null-2")}),
       Row({Value(), Value("l-null-2"), Value(), Value("r-null-1")}),
       Row({Value(), Value("l-null-2"), Value(), Value("r-null-2")}),
       Row({Value(1), Value("l-one"), Value(1), Value("r-one")}),
       Row({Value(3), Value("l-three"), Value(), Value()})});
  ExpectSameMultiset(
      run(JoinKind::kRightOuter, true),
      {Row({Value(), Value("l-null-1"), Value(), Value("r-null-1")}),
       Row({Value(), Value("l-null-1"), Value(), Value("r-null-2")}),
       Row({Value(), Value("l-null-2"), Value(), Value("r-null-1")}),
       Row({Value(), Value("l-null-2"), Value(), Value("r-null-2")}),
       Row({Value(1), Value("l-one"), Value(1), Value("r-one")}),
       Row({Value(), Value(), Value(2), Value("r-two")})});
  ExpectSameMultiset(
      run(JoinKind::kFullOuter, true),
      {Row({Value(), Value("l-null-1"), Value(), Value("r-null-1")}),
       Row({Value(), Value("l-null-1"), Value(), Value("r-null-2")}),
       Row({Value(), Value("l-null-2"), Value(), Value("r-null-1")}),
       Row({Value(), Value("l-null-2"), Value(), Value("r-null-2")}),
       Row({Value(1), Value("l-one"), Value(1), Value("r-one")}),
       Row({Value(3), Value("l-three"), Value(), Value()}),
       Row({Value(), Value(), Value(2), Value("r-two")})});

  ExpectSameMultiset(run(JoinKind::kLeftOuter, false),
                     {Row({Value(1), Value("l-one"), Value(1), Value("r-one")}),
                      Row({Value(3), Value("l-three"), Value(), Value()}),
                      Row({Value(), Value("l-null-1"), Value(), Value()}),
                      Row({Value(), Value("l-null-2"), Value(), Value()})});
  ExpectSameMultiset(run(JoinKind::kRightOuter, false),
                     {Row({Value(1), Value("l-one"), Value(1), Value("r-one")}),
                      Row({Value(), Value(), Value(2), Value("r-two")}),
                      Row({Value(), Value(), Value(), Value("r-null-1")}),
                      Row({Value(), Value(), Value(), Value("r-null-2")})});
  ExpectSameMultiset(run(JoinKind::kFullOuter, false),
                     {Row({Value(1), Value("l-one"), Value(1), Value("r-one")}),
                      Row({Value(3), Value("l-three"), Value(), Value()}),
                      Row({Value(), Value("l-null-1"), Value(), Value()}),
                      Row({Value(), Value("l-null-2"), Value(), Value()}),
                      Row({Value(), Value(), Value(2), Value("r-two")}),
                      Row({Value(), Value(), Value(), Value("r-null-1")}),
                      Row({Value(), Value(), Value(), Value("r-null-2")})});

  {
    HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                  HashJoinMode::kInMemory, JoinKind::kSemi, 2, 2, 2);
    join.SetJoinAnnotations({true}, Expression());
    ExpectSameMultiset(Drain(&join), {Row({Value(), Value("l-null-1")}),
                                      Row({Value(), Value("l-null-2")}),
                                      Row({Value(1), Value("l-one")})});
  }
  {
    HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                  HashJoinMode::kInMemory, JoinKind::kAnti, 2, 2, 2);
    join.SetJoinAnnotations({true}, Expression());
    ExpectSameMultiset(Drain(&join), {Row({Value(3), Value("l-three")})});
  }

  const std::vector<Row> composite_left{Row({Value(), Value(1), Value("l1")}),
                                        Row({Value(), Value(), Value("ln")}),
                                        Row({Value(1), Value(1), Value("l2")}),
                                        Row({Value(2), Value(2), Value("l3")})};
  const std::vector<Row> composite_right{Row({Value(), Value(1), Value("r1")}),
                                         Row({Value(), Value(), Value("rn")}),
                                         Row({Value(1), Value(1), Value("r2")}),
                                         Row({Value(2), Value(), Value("r3")})};
  {
    HashJoin join(Src(composite_left), {0, 1}, Src(composite_right), {0, 1},
                  HashJoinMode::kInMemory, JoinKind::kLeftOuter, 2, 3, 3);
    join.SetJoinAnnotations({true, false}, Expression());
    ExpectSameMultiset(
        Drain(&join),
        {Row({Value(), Value(1), Value("l1"), Value(), Value(1), Value("r1")}),
         Row({Value(), Value(), Value("ln"), Value(), Value(), Value()}),
         Row({Value(1), Value(1), Value("l2"), Value(1), Value(1),
              Value("r2")}),
         Row({Value(2), Value(2), Value("l3"), Value(), Value(), Value()})});
  }
  {
    HashJoin join(Src(composite_left), {0, 1}, Src(composite_right), {0, 1},
                  HashJoinMode::kInMemory, JoinKind::kFullOuter, 2, 3, 3);
    join.SetJoinAnnotations({true, false}, Expression());
    ExpectSameMultiset(
        Drain(&join),
        {Row({Value(), Value(1), Value("l1"), Value(), Value(1), Value("r1")}),
         Row({Value(), Value(), Value("ln"), Value(), Value(), Value()}),
         Row({Value(1), Value(1), Value("l2"), Value(1), Value(1),
              Value("r2")}),
         Row({Value(2), Value(2), Value("l3"), Value(), Value(), Value()}),
         Row({Value(), Value(), Value(), Value(), Value(), Value("rn")}),
         Row({Value(), Value(), Value(), Value(2), Value(), Value("r3")})});
  }

  const std::vector<Row> empty_left;
  const std::vector<Row> empty_right;
  const std::vector<Row> one_left{Row({Value(1), Value("left")})};
  const std::vector<Row> one_right{Row({Value(1), Value("right")})};
  {
    HashJoin join(Src(empty_left), {0}, Src(one_right), {0},
                  HashJoinMode::kInMemory, JoinKind::kLeftOuter, 1, 2, 2);
    join.SetJoinAnnotations({true}, Expression());
    EXPECT_TRUE(Drain(&join).empty());
  }
  {
    HashJoin join(Src(empty_left), {0}, Src(one_right), {0},
                  HashJoinMode::kInMemory, JoinKind::kFullOuter, 1, 2, 2);
    join.SetJoinAnnotations({true}, Expression());
    ExpectSameMultiset(Drain(&join),
                       {Row({Value(), Value(), Value(1), Value("right")})});
  }
  {
    HashJoin join(Src(one_left), {0}, Src(empty_right), {0},
                  HashJoinMode::kInMemory, JoinKind::kFullOuter, 1, 2, 2);
    join.SetJoinAnnotations({true}, Expression());
    ExpectSameMultiset(Drain(&join),
                       {Row({Value(1), Value("left"), Value(), Value()})});
  }
  {
    HashJoin join(std::make_shared<FailingSource>(), {0}, Src(one_right), {0},
                  HashJoinMode::kInMemory, JoinKind::kLeftOuter, 1, 2, 2);
    join.SetJoinAnnotations({true}, Expression());
    Row row;
    EXPECT_FALSE(join.Next(&row, nullptr));
    EXPECT_NE(join.GetStatus(), Status::kSuccess);
  }

  std::vector<Row> spill_left;
  spill_left.reserve(80);
  std::vector<Row> spill_right;
  spill_right.reserve(80);
  for (int64_t i = 0; i < 80; ++i) {
    const std::string payload(96, 'x');
    spill_left.push_back(Row({Value(i % 7), Value(std::string(payload))}));
    spill_right.push_back(Row({Value(i % 7), Value(std::string(payload))}));
  }
  {
    BudgetGuard guard(512);
    HashJoin join(Src(std::move(spill_left)), {0}, Src(std::move(spill_right)),
                  {0}, HashJoinMode::kInMemory, JoinKind::kFullOuter, 2, 2, 2);
    join.SetJoinAnnotations({true}, Expression());
    const std::vector<Row> output = Drain(&join);
    EXPECT_EQ(join.GetStatus(), Status::kSuccess);
    ASSERT_EQ(output.size(), 916U);
    for (const Row& row : output) {
      EXPECT_EQ(row[0], row[2]);
      EXPECT_EQ(row[1], row[3]);
    }
  }
}

TEST(HashJoinOuterTest, OuterJoinsSurviveSpillAndReload) {
  std::vector<Row> left_rows;
  left_rows.reserve(150);
  std::vector<Row> right_rows;
  right_rows.reserve(150);
  for (int64_t i = 0; i < 150; ++i) {
    left_rows.push_back(IRow(i));
    right_rows.push_back(IRow(i % 50));
  }
  BudgetGuard guard(2048);
  HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                HashJoinMode::kInMemory, JoinKind::kFullOuter, 2, 1, 1);
  const std::vector<Row> out = Drain(&join);
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  // Right keys 0..49 appear 3x each: 150 matches; left keys 50..149 add 100
  // left-only padded rows.
  ASSERT_EQ(out.size(), 250U);
  size_t padded = 0;
  for (const Row& row : out) {
    EXPECT_EQ(row.values_.size(), 2U);
    if (row[1].IsNull()) {
      ++padded;
    }
  }
  EXPECT_EQ(padded, 100U);
}

TEST(HashJoinSemiAntiTest, VariantsResidentAndSpilled) {
  const auto probe_rows = std::vector<Row>{Row({Value(1)}), Row({Value(2)}),
                                           Row({Value()}), Row({Value(4)})};
  const auto build_rows =
      std::vector<Row>{Row({Value(2)}), Row({Value(4)}), Row({Value()})};
  {
    HashJoin semi(Src(probe_rows), {0}, Src(build_rows), {0},
                  HashJoinMode::kInMemory, JoinKind::kSemi, 2, 1, 1);
    const std::vector<Row> out = Drain(&semi);
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(out[0][0], Value(2));
    EXPECT_EQ(out[1][0], Value(4));
  }
  {
    // Anti: NULL probe keys match nothing, so they survive (NOT EXISTS).
    // Survivors stream in probe order: key 1 first, then the NULL key.
    HashJoin anti(Src(probe_rows), {0}, Src(build_rows), {0},
                  HashJoinMode::kInMemory, JoinKind::kAnti, 2, 1, 1);
    const std::vector<Row> out = Drain(&anti);
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(out[0][0], Value(1));
    EXPECT_TRUE(out[1][0].IsNull());
  }
  {
    // NullAwareAnti (NOT IN): the build side carries a NULL, so every
    // non-matching probe is UNKNOWN and filtered; only strict non-NULL
    // matches would be excluded, and here nothing survives at all.
    HashJoin nanti(Src(probe_rows), {0}, Src(build_rows), {0},
                   HashJoinMode::kInMemory, JoinKind::kNullAwareAnti, 2, 1, 1);
    EXPECT_TRUE(Drain(&nanti).empty());
  }
  {
    // NullAwareAnti with a NULL build key: everything is UNKNOWN.
    const auto build_with_null =
        std::vector<Row>{Row({Value(2)}), Row({Value()}), Row({Value(9)})};
    HashJoin nanti(Src(probe_rows), {0}, Src(build_with_null), {0},
                   HashJoinMode::kInMemory, JoinKind::kNullAwareAnti, 2, 1, 1);
    EXPECT_TRUE(Drain(&nanti).empty());
  }
}

TEST(HashJoinSemiAntiTest, AntiJoinWithEmptySpilledBuildEmitsAll) {
  std::vector<Row> probe_rows;
  probe_rows.reserve(200);
  for (int64_t i = 0; i < 200; ++i) {
    probe_rows.push_back(IRow(i));
  }
  BudgetGuard guard(2048);  // probe side spills; build side stays empty
  HashJoin anti(Src(probe_rows), {0}, Src(std::vector<Row>{}), {0},
                HashJoinMode::kInMemory, JoinKind::kAnti, 2, 1, 1);
  const std::vector<Row> out = Drain(&anti);
  EXPECT_EQ(anti.GetStatus(), Status::kSuccess);
  EXPECT_EQ(out.size(), 200U);
}

TEST(HashJoinSemiAntiTest, SemiJoinTinyBudgetMatchesSpilledBuild) {
  std::vector<Row> probe_rows;
  probe_rows.reserve(7);
  probe_rows.push_back(Row({Value()}));
  for (int64_t i = 1; i <= 6; ++i) {
    probe_rows.push_back(IRow(i));
  }
  std::vector<Row> build_rows;
  build_rows.reserve(300);
  for (int64_t i = 0; i < 300; ++i) {
    build_rows.push_back(IRow(i % 4));
  }
  BudgetGuard guard(2048);
  HashJoin semi(Src(probe_rows), {0}, Src(build_rows), {0},
                HashJoinMode::kInMemory, JoinKind::kSemi, 2, 1, 1);
  const std::vector<Row> out = Drain(&semi);
  EXPECT_EQ(semi.GetStatus(), Status::kSuccess);
  ExpectSameMultiset(out, {IRow(1), IRow(2), IRow(3)});
}

// ===========================================================================
// HashJoin hybrid mode and in-memory pipelined paths.
// ===========================================================================

TEST(HashJoinHybridTest, HybridModeMatchesInnerJoinSemantics) {
  // Variant A: unique keys on both sides -> 1:1 join.
  {
    std::vector<Row> left_rows;
    left_rows.reserve(300);
    std::vector<Row> right_rows;
    right_rows.reserve(300);
    for (int64_t i = 0; i < 300; ++i) {
      left_rows.push_back(Row({Value(i), Value(i * 2)}));
      right_rows.push_back(Row({Value(i), Value("r")}));
    }
    HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                  HashJoinMode::kHybrid, JoinKind::kInner, /*worker_count=*/2);
    const std::vector<Row> out = Drain(&join);
    EXPECT_EQ(join.GetStatus(), Status::kSuccess);
    ASSERT_EQ(out.size(), 300U);
    for (const Row& row : out) {
      ASSERT_EQ(row.values_.size(), 4U);
      EXPECT_EQ(row[1], Value(row[0].value.int_value * 2));
      EXPECT_EQ(row[3], Value("r"));
    }
  }
  // Variant B: duplicate keys on the right side.
  {
    std::vector<Row> left_rows;
    left_rows.reserve(40);
    std::vector<Row> right_rows;
    right_rows.reserve(40);
    for (int64_t i = 0; i < 40; ++i) {
      left_rows.push_back(Row({Value(i), Value(i * 2)}));
      right_rows.push_back(Row({Value(i % 10), Value("r")}));
    }
    HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                  HashJoinMode::kHybrid, JoinKind::kInner, /*worker_count=*/2);
    const std::vector<Row> out = Drain(&join);
    EXPECT_EQ(join.GetStatus(), Status::kSuccess);
    // Left keys 0..9 match 4 right rows each; keys 10..39 match nothing.
    EXPECT_EQ(out.size(), 40U);
    for (const Row& row : out) {
      EXPECT_EQ(row[1], Value(row[0].value.int_value * 2));
      EXPECT_EQ(row[3], Value("r"));
    }
  }
}

TEST(HashJoinHybridTest, HybridModeDisjointKeysYieldNothing) {
  auto left = Src(SequenceRows(0, 50));
  auto right = Src(SequenceRows(1000, 1100));
  HashJoin join(left, {0}, right, {0}, HashJoinMode::kHybrid, JoinKind::kInner,
                2);
  EXPECT_TRUE(Drain(&join).empty());
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("HybridHashJoin"), std::string::npos);
}

TEST(HashJoinAdaptiveTest, StaysInNestedLoopBelowProbeThreshold) {
  // Equal-size sides: the right side becomes the build index and the left
  // (3 rows, below kNestedLoopProbeLimit) stays on the nested-loop phase.
  auto left = Src({IRow(1), IRow(2), IRow(777)});
  auto right = Src({IRow(1), IRow(2), IRow(999)});
  HashJoin join(left, {0}, right, {0}, HashJoinMode::kInMemory,
                JoinKind::kInner, 2);
  const std::vector<Row> out = Drain(&join);
  ASSERT_EQ(out.size(), 2U);
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("AdaptiveJoin initial=NestedLoop final=NestedLoop"),
            std::string::npos);
  // Probe key 777 matched nothing: the exact-set filter counted the reject.
  EXPECT_NE(ss.str().find("probe_rows_rejected=1"), std::string::npos);
}

TEST(HashJoinAdaptiveTest, SwitchesToHashBuildWhenProbeGrows) {
  std::vector<Row> build_rows;
  build_rows.reserve(10);
  for (int64_t i = 0; i < 10; ++i) {
    build_rows.push_back(IRow(i));
  }
  // 6 probe rows cross kNestedLoopProbeLimit (4) mid-stream.
  const std::vector<Row> probe_rows = {IRow(0), IRow(1), IRow(2),
                                       IRow(3), IRow(4), IRow(999)};
  HashJoin join(Src(build_rows), {0}, Src(probe_rows), {0},
                HashJoinMode::kInMemory, JoinKind::kInner, 2);
  const std::vector<Row> out = Drain(&join);
  ASSERT_EQ(out.size(), 5U);
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("AdaptiveJoin initial=NestedLoop final=HashJoin"),
            std::string::npos);
}

TEST(HashJoinParallelProbeTest, StripedProbeOverLargeProbeSide) {
  std::vector<Row> build_rows;
  build_rows.reserve(100);
  for (int64_t i = 0; i < 100; ++i) {
    build_rows.push_back(IRow(i));
  }
  std::vector<Row> probe_rows;
  probe_rows.reserve(34000);
  for (int64_t i = 0; i < 34000; ++i) {
    probe_rows.push_back(IRow(i % 1000));
  }
  HashJoin join(Src(build_rows), {0}, Src(probe_rows), {0},
                HashJoinMode::kInMemory, JoinKind::kInner,
                /*worker_count=*/8);
  const std::vector<Row> out = Drain(&join);
  // 34 full cycles of keys 0..99 matched within each 1000-block.
  EXPECT_EQ(out.size(), 3400U);
  EXPECT_GT(join.MaterializedBytes(), 0U);

  // The batched consumer drains the striped queues too.
  HashJoin join2(Src(build_rows), {0}, Src(probe_rows), {0},
                 HashJoinMode::kInMemory, JoinKind::kInner, 8);
  DataChunk chunk;
  size_t total = 0;
  for (;;) {
    const size_t got = join2.NextBatch(&chunk, 512);
    if (got == 0) {
      break;
    }
    total += got;
    EXPECT_LE(chunk.Size(), 512U);
  }
  EXPECT_EQ(total, 3400U);
  EXPECT_EQ(join2.GetStatus(), Status::kSuccess);
}

TEST(HashJoinSpillTest, BothSidesSpilledInnerJoinPartitionPairwise) {
  std::vector<Row> left_rows;
  left_rows.reserve(400);
  std::vector<Row> right_rows;
  right_rows.reserve(400);
  for (int64_t i = 0; i < 400; ++i) {
    left_rows.push_back(Row({Value(i % 13)}));
    right_rows.push_back(Row({Value(i % 7)}));
  }
  BudgetGuard guard(2048);
  HashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                HashJoinMode::kInMemory, JoinKind::kInner, 2);
  const std::vector<Row> out = Drain(&join);
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  // Shared keys 0..6: total = sum over k of left_count(k) * right_count(k).
  size_t expected = 0;
  for (int64_t k = 0; k < 7; ++k) {
    size_t lc = 0;
    for (int64_t i = k; i < 400; i += 13) {
      ++lc;
    }
    size_t rc = 0;
    for (int64_t i = k; i < 400; i += 7) {
      ++rc;
    }
    expected += lc * rc;
  }
  EXPECT_EQ(out.size(), expected);
  EXPECT_GT(expected, 10000U);  // sanity: the join really produced bulk rows
}

TEST(HashJoinSpillTest, OneSideSpilledKeepsResultExact) {
  // Tiny left side (resident), large right side (spilled).
  auto left = Src({IRow(5), IRow(600)});
  std::vector<Row> right_rows;
  right_rows.reserve(500);
  for (int64_t i = 0; i < 500; ++i) {
    right_rows.push_back(IRow(i));
  }
  BudgetGuard guard(4096);
  HashJoin join(left, {0}, Src(right_rows), {0}, HashJoinMode::kInMemory,
                JoinKind::kInner, 2);
  const std::vector<Row> out = Drain(&join);
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  ASSERT_EQ(out.size(), 1U);
  EXPECT_EQ(out[0], (Row({Value(5), Value(5)})));
}

TEST(HashJoinSpillTest, MultiKeySpilledJoinWithStringComponent) {
  std::vector<Row> left_rows;
  left_rows.reserve(200);
  std::vector<Row> right_rows;
  right_rows.reserve(200);
  for (int64_t i = 0; i < 200; ++i) {
    left_rows.push_back(Row({Value(i % 10), Value("k")}));
    right_rows.push_back(Row({Value(i % 6), Value("k")}));
  }
  BudgetGuard guard(2048);
  HashJoin join(Src(left_rows), {0, 1}, Src(right_rows), {0, 1},
                HashJoinMode::kInMemory, JoinKind::kInner, 2);
  const std::vector<Row> out = Drain(&join);
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  size_t expected = 0;
  for (int64_t k = 0; k < 6; ++k) {
    size_t lc = 0;
    for (int64_t i = k; i < 200; i += 10) {
      ++lc;
    }
    size_t rc = 0;
    for (int64_t i = k; i < 200; i += 6) {
      ++rc;
    }
    expected += lc * rc;
  }
  EXPECT_EQ(out.size(), expected);
}

// ===========================================================================
// MergeJoin (serial).
// ===========================================================================

TEST(MergeJoinTest, InnerJoinDuplicateRunsCrossProduct) {
  MergeJoin join(Src({IRow(1), IRow(2), IRow(2), IRow(4)}), {0},
                 Src({IRow(2), IRow(2), IRow(3)}), {0}, JoinKind::kInner, 1, 1);
  const std::vector<Row> out = Drain(&join);
  ASSERT_EQ(out.size(), 4U);
  for (const Row& row : out) {
    EXPECT_EQ(row[0], Value(2));
    EXPECT_EQ(row[1], Value(2));
  }
}

TEST(MergeJoinTest, OuterVariantsEmitNullPaddedRows) {
  {
    MergeJoin join(Src({IRow(1), IRow(3)}), {0}, Src({IRow(2), IRow(3)}), {0},
                   JoinKind::kLeftOuter, 1, 1);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(out[0], (Row({Value(1), Value()})));
    EXPECT_EQ(out[1], (Row({Value(3), Value(3)})));
  }
  {
    MergeJoin join(Src({IRow(1), IRow(3)}), {0}, Src({IRow(2), IRow(3)}), {0},
                   JoinKind::kRightOuter, 1, 1);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(out[0], (Row({Value(), Value(2)})));
    EXPECT_EQ(out[1], (Row({Value(3), Value(3)})));
  }
  {
    MergeJoin join(Src({IRow(1), IRow(3)}), {0}, Src({IRow(2), IRow(3)}), {0},
                   JoinKind::kFullOuter, 1, 1);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 3U);
    // Merge order: left 1 < right 2 pads left; right 2 is then passed over
    // and padded; the (3,3) match follows.
    EXPECT_EQ(out[0], (Row({Value(1), Value()})));
    EXPECT_EQ(out[1], (Row({Value(), Value(2)})));
    EXPECT_EQ(out[2], (Row({Value(3), Value(3)})));
  }
}

TEST(MergeJoinTest, SemiAndAntiPreserveRowPositions) {
  {
    // SEMI must surface the LEFT (probe) row's own position: the matching
    // left row is IRow(2) at RowPosition(12, 1).
    auto left = std::make_shared<PositionedSource>(
        std::vector<std::pair<Row, RowPosition>>{
            {IRow(1), RowPosition(11, 0)},
            {IRow(2), RowPosition(12, 1)},
            {IRow(5), RowPosition(13, 2)}});
    MergeJoin semi(left, {0}, Src({IRow(2), IRow(3)}), {0}, JoinKind::kSemi, 1,
                   1);
    Row row;
    RowPosition rp;
    size_t count = 0;
    while (semi.Next(&row, &rp)) {
      EXPECT_EQ(row[0], Value(2));
      EXPECT_EQ(rp, RowPosition(12, 1));
      ++count;
    }
    EXPECT_EQ(count, 1U);
  }
  {
    // ANTI keeps the non-matching left rows together with their positions.
    auto left = std::make_shared<PositionedSource>(
        std::vector<std::pair<Row, RowPosition>>{
            {IRow(1), RowPosition(11, 0)},
            {IRow(2), RowPosition(12, 1)},
            {IRow(5), RowPosition(13, 2)}});
    MergeJoin anti(left, {0}, Src({IRow(2), IRow(3)}), {0}, JoinKind::kAnti, 1,
                   1);
    Row row;
    RowPosition rp;
    std::vector<std::pair<int64_t, RowPosition>> emitted;
    while (anti.Next(&row, &rp)) {
      emitted.emplace_back(row[0].value.int_value, rp);
    }
    ASSERT_EQ(emitted.size(), 2U);
    EXPECT_EQ(emitted[0].first, 1);
    EXPECT_EQ(emitted[0].second, RowPosition(11, 0));
    EXPECT_EQ(emitted[1].first, 5);
    EXPECT_EQ(emitted[1].second, RowPosition(13, 2));
  }
}

TEST(MergeJoinTest, NullKeysNeverMatchAndOuterKeepsThem) {
  {
    MergeJoin join(Src({Row({Value()}), IRow(1)}), {0},
                   Src({Row({Value()}), IRow(1)}), {0}, JoinKind::kInner, 1, 1);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0], (Row({Value(1), Value(1)})));
  }
  {
    MergeJoin join(Src({Row({Value()}), IRow(1)}), {0},
                   Src({Row({Value()}), IRow(1)}), {0}, JoinKind::kFullOuter, 1,
                   1);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 3U);  // 1 match + 2 NULL-keyed rows preserved
  }
}

}  // namespace tinylamb
