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
TEST_F(RelationalRegressionTest,
       ExactIntegerTotalParallelFlatNullableAndOverflowFallback) {
  RelationalRun(*rs_, "CREATE TABLE ExSeq(x INT64)");
  {
    std::string sql = "INSERT INTO ExSeq VALUES ";
    for (int64_t x = 0; x < 300; ++x) {
      if (x > 0) {
        sql += ",";
      }
      sql += "(" + std::to_string(x) + ")";
    }
    RelationalRun(*rs_, sql);
  }
  RelationalRun(*rs_, "CREATE TABLE ExDim(k INT64)");
  RelationalRun(*rs_, "INSERT INTO ExDim VALUES (1),(2)");
  RelationalRun(*rs_, "CREATE TABLE ExFact(k INT64, v INT64)");
  RelationalRun(*rs_,
                "INSERT INTO ExFact "
                "SELECT MOD(a.x, 2) + 1, "
                "CASE WHEN MOD(a.x + b.x, 7) = 0 THEN NULL "
                "ELSE (a.x - 150) * 10 + MOD(b.x, 10) END "
                "FROM ExSeq a, ExSeq b");

  std::array<int64_t, 3> exp_sum{};
  for (int64_t ax = 0; ax < 300; ++ax) {
    for (int64_t bx = 0; bx < 300; ++bx) {
      if ((ax + bx) % 7 == 0) {
        continue;
      }
      exp_sum[(ax % 2) + 1] += ((ax - 150) * 10) + (bx % 10);
    }
  }
  for (bool force_rel : {false, true}) {
    const auto rows = RelationalRun(
        *rs_,
        "SELECT d.k, SUM(f.v) FROM ExFact f JOIN ExDim d ON f.k = d.k "
        "GROUP BY d.k ORDER BY d.k",
        force_rel);
    ASSERT_EQ(rows.size(), 2U);
    EXPECT_EQ(rows[0], Row({Value(int64_t{1}), Value(exp_sum[1])}));
    EXPECT_EQ(rows[1], Row({Value(int64_t{2}), Value(exp_sum[2])}));
  }

  // Trigger magnitude > kExactLimit / pairs fallback with INT64_MIN boundary +
  // large cancelling values while keeping true SUM exact in int64_t.
  RelationalRun(
      *rs_,
      "INSERT INTO ExFact VALUES "
      "(1, 100000000000000),(1, -100000000000000),"
      "(2, -9223372036854775807 - 1),(2, 9223372036854775807),(2, 1)");
  for (bool force_rel : {false, true}) {
    const auto rows = RelationalRun(
        *rs_,
        "SELECT d.k, SUM(f.v) FROM ExFact f JOIN ExDim d ON f.k = d.k "
        "GROUP BY d.k ORDER BY d.k",
        force_rel);
    ASSERT_EQ(rows.size(), 2U);
    EXPECT_EQ(rows[0], Row({Value(int64_t{1}), Value(exp_sum[1])}));
    EXPECT_EQ(rows[1], Row({Value(int64_t{2}), Value(exp_sum[2])}));
  }
}

TEST_F(RelationalRegressionTest,
       LeftPackGatheredDictAndScaledStddevDirectEmit) {
  RelationalRun(*rs_, "CREATE TABLE LpWh(w_sk INT64, w_name STRING)");
  RelationalRun(*rs_,
                "INSERT INTO LpWh VALUES "
                "(1,'alpha'),(2,'beta'),(3,NULL),(4,'gamma'),(99,'unused_wh')");
  RelationalRun(*rs_, "CREATE TABLE LpDd(d_sk INT64, d_moy INT64)");
  RelationalRun(*rs_, "INSERT INTO LpDd VALUES (1,10),(2,20),(3,30)");
  RelationalRun(*rs_, "CREATE TABLE LpTopDim(d_moy INT64)");
  RelationalRun(*rs_, "INSERT INTO LpTopDim VALUES (10),(20),(30)");
  RelationalRun(*rs_, "CREATE TABLE LpS100(x INT64)");
  {
    std::string sql = "INSERT INTO LpS100 VALUES ";
    for (int64_t x = 0; x < 100; ++x) {
      if (x > 0) {
        sql += ",";
      }
      sql += "(" + std::to_string(x) + ")";
    }
    RelationalRun(*rs_, sql);
  }
  RelationalRun(*rs_, "CREATE TABLE LpS90(y INT64)");
  {
    std::string sql = "INSERT INTO LpS90 VALUES ";
    for (int64_t y = 0; y < 90; ++y) {
      if (y > 0) {
        sql += ",";
      }
      sql += "(" + std::to_string(y) + ")";
    }
    RelationalRun(*rs_, sql);
  }
  RelationalRun(*rs_,
                "CREATE TABLE LpFact(w_sk INT64, d_sk INT64, qty DOUBLE)");
  // w_sk = 5 triggers LEFT JOIN NULL padding; w_sk = 99 stays unused in LpWh.
  RelationalRun(*rs_,
                "INSERT INTO LpFact "
                "SELECT MOD(x + y, 5) + 1, MOD(y, 3) + 1, "
                "(CAST(MOD(x * 3 + y * 5, 17) AS DOUBLE) + 0.5) * "
                "(1.0 + 0.2 * CAST(MOD(x + y, 5) + 1 AS DOUBLE) + "
                "0.05 * CAST(MOD(y, 3) + 1 AS DOUBLE)) "
                "FROM LpS100, LpS90");

  struct Stat {
    int64_t n{0};
    double sx{0.0};
    double sxx{0.0};
  };
  std::map<std::pair<std::optional<std::string>, int64_t>, Stat> stats;
  for (int64_t x = 0; x < 100; ++x) {
    for (int64_t y = 0; y < 90; ++y) {
      const int64_t wsk = ((x + y) % 5) + 1;
      const int64_t dsk = (y % 3) + 1;
      std::optional<std::string> wn;
      if (wsk == 1) {
        wn = "alpha";
      } else if (wsk == 2) {
        wn = "beta";
      } else if (wsk == 4) {
        wn = "gamma";
      }
      const int64_t dmoy = dsk * 10;
      const double q = (static_cast<double>((x * 3 + y * 5) % 17) + 0.5) *
                       (1.0 + 0.2 * static_cast<double>(wsk) +
                        0.05 * static_cast<double>(dsk));
      Stat& st = stats[{wn, dmoy}];
      ++st.n;
      st.sx += q;
      st.sxx += q * q;
    }
  }
  struct ExpRow {
    std::optional<std::string> wn;
    int64_t dmoy{};
    double sd{};
    double nv{};
  };
  std::vector<ExpRow> expected;
  for (const auto& [key, st] : stats) {
    const double mean = st.sx / static_cast<double>(st.n);
    const double var =
        std::max(0.0, (st.sxx - static_cast<double>(st.n) * mean * mean) /
                          static_cast<double>(st.n - 1));
    expected.push_back(
        {key.first, key.second, std::sqrt(var) * 1.000, var * -2.0});
  }
  std::ranges::sort(expected, [](const ExpRow& a, const ExpRow& b) {
    if (a.nv != b.nv) {
      return a.nv < b.nv;
    }
    if (a.wn.has_value() != b.wn.has_value()) {
      return a.wn.has_value();
    }
    if (a.wn.has_value() && *a.wn != *b.wn) {
      return *a.wn < *b.wn;
    }
    return a.dmoy < b.dmoy;
  });
  const std::vector<ExpRow> all_expected = expected;
  expected.resize(6);

  const std::string sql =
      "SELECT j.w_name, t.d_moy, STDDEV_SAMP(j.qty) * 1.000 AS sd, "
      "VAR_SAMP(j.qty) * -2.0 AS nv "
      "FROM ("
      "  SELECT w.w_name, d.d_moy, f.qty "
      "  FROM LpFact f LEFT JOIN LpWh w ON f.w_sk = w.w_sk "
      "  JOIN LpDd d ON f.d_sk = d.d_sk"
      ") j "
      "JOIN LpTopDim t ON j.d_moy = t.d_moy "
      "GROUP BY j.w_name, t.d_moy "
      "ORDER BY nv, j.w_name NULLS LAST, t.d_moy "
      "LIMIT 6";
  for (bool force_rel : {false, true}) {
    const auto rows = RelationalRun(*rs_, sql, force_rel);
    ASSERT_EQ(rows.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
      if (expected[i].wn.has_value()) {
        ASSERT_FALSE(rows[i][0].IsNull());
        EXPECT_EQ(rows[i][0], Value(std::string(*expected[i].wn)));
      } else {
        EXPECT_TRUE(rows[i][0].IsNull());
      }
      EXPECT_EQ(rows[i][1], Value(expected[i].dmoy));
      EXPECT_NEAR(rows[i][2].value.double_value, expected[i].sd, 1e-9);
      EXPECT_NEAR(rows[i][3].value.double_value, expected[i].nv, 1e-9);
    }
  }

  // Exercise left_pack_unpack_only + fast_col_emit (dict + packed int +
  // kStddevSamp + kVarSamp) without LIMIT over all 12 groups.
  const std::string unpack_sql =
      "SELECT j.w_name, j.d_moy, STDDEV_SAMP(j.qty) * 1.000 AS sd, "
      "VAR_SAMP(j.qty) * -2.0 AS nv "
      "FROM ("
      "  SELECT w.w_name, d.d_moy, f.qty "
      "  FROM LpFact f LEFT JOIN LpWh w ON f.w_sk = w.w_sk "
      "  JOIN LpDd d ON f.d_sk = d.d_sk"
      ") j "
      "JOIN LpTopDim t ON j.d_moy = t.d_moy "
      "GROUP BY j.w_name, j.d_moy "
      "ORDER BY nv, j.w_name NULLS LAST, j.d_moy";
  for (bool force_rel : {false, true}) {
    const auto rows = RelationalRun(*rs_, unpack_sql, force_rel);
    ASSERT_EQ(rows.size(), all_expected.size());
    for (size_t i = 0; i < all_expected.size(); ++i) {
      if (all_expected[i].wn.has_value()) {
        ASSERT_FALSE(rows[i][0].IsNull());
        EXPECT_EQ(rows[i][0], Value(std::string(*all_expected[i].wn)));
      } else {
        EXPECT_TRUE(rows[i][0].IsNull());
      }
      EXPECT_EQ(rows[i][1], Value(all_expected[i].dmoy));
      EXPECT_NEAR(rows[i][2].value.double_value, all_expected[i].sd, 1e-9);
      EXPECT_NEAR(rows[i][3].value.double_value, all_expected[i].nv, 1e-9);
    }
  }

  // Pin BytecodeProgram ORDER BY + LIMIT Top-N correctness (non-trivial combo
  // expression must not be mis-sorted by key column 0 inside fast_topn).
  std::vector<ExpRow> combo_expected = all_expected;
  std::ranges::sort(combo_expected, [](const ExpRow& a, const ExpRow& b) {
    const double ca = a.sd + a.nv;
    const double cb = b.sd + b.nv;
    if (ca != cb) {
      return ca < cb;
    }
    if (a.wn.has_value() != b.wn.has_value()) {
      return a.wn.has_value();
    }
    if (a.wn.has_value() && *a.wn != *b.wn) {
      return *a.wn < *b.wn;
    }
    return a.dmoy < b.dmoy;
  });
  combo_expected.resize(6);
  const std::string combo_sql =
      "SELECT j.w_name, t.d_moy, "
      "STDDEV_SAMP(j.qty) + VAR_SAMP(j.qty) * -2.0 AS combo "
      "FROM ("
      "  SELECT w.w_name, d.d_moy, f.qty "
      "  FROM LpFact f LEFT JOIN LpWh w ON f.w_sk = w.w_sk "
      "  JOIN LpDd d ON f.d_sk = d.d_sk"
      ") j "
      "JOIN LpTopDim t ON j.d_moy = t.d_moy "
      "GROUP BY j.w_name, t.d_moy "
      "ORDER BY combo, j.w_name NULLS LAST, t.d_moy "
      "LIMIT 6";
  for (bool force_rel : {false, true}) {
    const auto rows = RelationalRun(*rs_, combo_sql, force_rel);
    ASSERT_EQ(rows.size(), combo_expected.size());
    for (size_t i = 0; i < combo_expected.size(); ++i) {
      if (combo_expected[i].wn.has_value()) {
        ASSERT_FALSE(rows[i][0].IsNull());
        EXPECT_EQ(rows[i][0], Value(std::string(*combo_expected[i].wn)));
      } else {
        EXPECT_TRUE(rows[i][0].IsNull());
      }
      EXPECT_EQ(rows[i][1], Value(combo_expected[i].dmoy));
      EXPECT_NEAR(rows[i][2].value.double_value,
                  combo_expected[i].sd + combo_expected[i].nv, 1e-9);
    }
  }
}

TEST_F(RelationalRegressionTest,
       Packed2x32RadixBlockFastPathNullAndLargeKeyFallback) {
  RelationalRun(*rs_, "CREATE TABLE RxDim(k INT64)");
  {
    std::string sql = "INSERT INTO RxDim VALUES ";
    for (int64_t k = 1; k <= 40; ++k) {
      if (k > 1) {
        sql += ",";
      }
      sql += "(" + std::to_string(k) + ")";
    }
    RelationalRun(*rs_, sql);
  }
  RelationalRun(*rs_, "CREATE TABLE Rx100(x INT64)");
  {
    std::string sql = "INSERT INTO Rx100 VALUES ";
    for (int64_t x = 0; x < 100; ++x) {
      if (x > 0) {
        sql += ",";
      }
      sql += "(" + std::to_string(x) + ")";
    }
    RelationalRun(*rs_, sql);
  }
  RelationalRun(
      *rs_, "CREATE TABLE RxFact(fk INT64, k0 INT64, k1 INT64, amt DOUBLE)");
  RelationalRun(*rs_,
                "INSERT INTO RxFact "
                "SELECT MOD(a.x, 40) + 1, "
                "CASE WHEN MOD(a.x * 100 + b.x, 500) = 0 THEN NULL "
                "     WHEN MOD(a.x * 100 + b.x, 500) = 1 THEN 5000000000 "
                "     ELSE MOD(a.x * 23 + b.x, 2500) END, "
                "CASE WHEN MOD(a.x * 100 + b.x, 500) = 2 THEN NULL "
                "     ELSE MOD(b.x * 7 + a.x, 200) END, "
                "CASE WHEN MOD(a.x * 23 + b.x, 2500) = 2499 "
                "       OR MOD(a.x * 100 + b.x, 500) = 3 THEN NULL "
                "     ELSE CAST(MOD(a.x + b.x, 19) AS DOUBLE) + 0.25 END "
                "FROM Rx100 a, Rx100 b");

  using Key = std::pair<std::optional<int64_t>, std::optional<int64_t>>;
  struct Acc {
    bool has_val{false};
    double sum{0.0};
  };
  std::map<Key, Acc> expected;
  for (int64_t ax = 0; ax < 100; ++ax) {
    for (int64_t bx = 0; bx < 100; ++bx) {
      const int64_t idx = (ax * 100) + bx;
      std::optional<int64_t> k0;
      if (idx % 500 == 1) {
        k0 = int64_t{5000000000LL};
      } else if (idx % 500 != 0) {
        k0 = (ax * 23 + bx) % 2500;
      }
      std::optional<int64_t> k1;
      if (idx % 500 != 2) {
        k1 = (bx * 7 + ax) % 200;
      }
      Acc& a = expected[{k0, k1}];
      if ((ax * 23 + bx) % 2500 != 2499 && idx % 500 != 3) {
        a.has_val = true;
        a.sum += static_cast<double>((ax + bx) % 19) + 0.25;
      }
    }
  }

  const std::string sql =
      "SELECT f.k0, f.k1, SUM(f.amt) "
      "FROM RxFact f JOIN RxDim d ON f.fk = d.k "
      "GROUP BY f.k0, f.k1 "
      "ORDER BY f.k0 NULLS FIRST, f.k1 NULLS FIRST";
  for (bool force_rel : {false, true}) {
    const auto rows = RelationalRun(*rs_, sql, force_rel);
    ASSERT_EQ(rows.size(), expected.size());
    size_t ri = 0;
    for (const auto& [k, acc] : expected) {
      if (k.first.has_value()) {
        EXPECT_EQ(rows[ri][0], Value(*k.first));
      } else {
        EXPECT_TRUE(rows[ri][0].IsNull());
      }
      if (k.second.has_value()) {
        EXPECT_EQ(rows[ri][1], Value(*k.second));
      } else {
        EXPECT_TRUE(rows[ri][1].IsNull());
      }
      if (acc.has_val) {
        ASSERT_FALSE(rows[ri][2].IsNull());
        EXPECT_DOUBLE_EQ(rows[ri][2].value.double_value, acc.sum);
      } else {
        EXPECT_TRUE(rows[ri][2].IsNull());
      }
      ++ri;
    }
  }

  // Exercise monotonic sorted join-key TLCS table with contiguous BETWEEN range
  // filter (`mono_contiguous_rf` + boundary lower_bound/upper_bound).
  RelationalRun(
      *rs_,
      "CREATE TABLE RxMonoFact(fk INT64, k0 INT64, k1 INT64, amt DOUBLE)");
  RelationalRun(*rs_,
                "INSERT INTO RxMonoFact "
                "SELECT DIV(a.x, 3) + 1, "
                "CASE WHEN MOD(a.x * 100 + b.x, 500) = 0 THEN NULL "
                "     WHEN MOD(a.x * 100 + b.x, 500) = 1 THEN 5000000000 "
                "     ELSE MOD(a.x * 23 + b.x, 2500) END, "
                "CASE WHEN MOD(a.x * 100 + b.x, 500) = 2 THEN NULL "
                "     ELSE MOD(b.x * 7 + a.x, 200) END, "
                "CASE WHEN MOD(a.x * 23 + b.x, 2500) = 2499 "
                "       OR MOD(a.x * 100 + b.x, 500) = 3 THEN NULL "
                "     ELSE CAST(MOD(a.x + b.x, 19) AS DOUBLE) + 0.25 END "
                "FROM Rx100 a, Rx100 b");
  {
    auto ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CompactTableToTlcs(ctx, "RxMonoFact"));
    ASSERT_SUCCESS(ctx.PreCommit());
    RelationalRun(*rs_, "ANALYZE");
  }
  std::map<Key, Acc> mono_expected;
  for (int64_t ax = 0; ax < 100; ++ax) {
    const int64_t fk = (ax / 3) + 1;
    if (fk < 10 || fk > 25) {
      continue;
    }
    for (int64_t bx = 0; bx < 100; ++bx) {
      const int64_t idx = (ax * 100) + bx;
      std::optional<int64_t> k0;
      if (idx % 500 == 1) {
        k0 = int64_t{5000000000LL};
      } else if (idx % 500 != 0) {
        k0 = (ax * 23 + bx) % 2500;
      }
      std::optional<int64_t> k1;
      if (idx % 500 != 2) {
        k1 = (bx * 7 + ax) % 200;
      }
      Acc& a = mono_expected[{k0, k1}];
      if ((ax * 23 + bx) % 2500 != 2499 && idx % 500 != 3) {
        a.has_val = true;
        a.sum += static_cast<double>((ax + bx) % 19) + 0.25;
      }
    }
  }
  const std::string mono_sql =
      "SELECT f.k0, f.k1, SUM(f.amt) "
      "FROM RxMonoFact f JOIN RxDim d ON f.fk = d.k "
      "WHERE d.k BETWEEN 10 AND 25 "
      "GROUP BY f.k0, f.k1 "
      "ORDER BY f.k0 NULLS FIRST, f.k1 NULLS FIRST";
  for (bool force_rel : {false, true}) {
    const auto rows = RelationalRun(*rs_, mono_sql, force_rel);
    ASSERT_EQ(rows.size(), mono_expected.size());
    size_t ri = 0;
    for (const auto& [k, acc] : mono_expected) {
      if (k.first.has_value()) {
        EXPECT_EQ(rows[ri][0], Value(*k.first));
      } else {
        EXPECT_TRUE(rows[ri][0].IsNull());
      }
      if (k.second.has_value()) {
        EXPECT_EQ(rows[ri][1], Value(*k.second));
      } else {
        EXPECT_TRUE(rows[ri][1].IsNull());
      }
      if (acc.has_val) {
        ASSERT_FALSE(rows[ri][2].IsNull());
        EXPECT_DOUBLE_EQ(rows[ri][2].value.double_value, acc.sum);
      } else {
        EXPECT_TRUE(rows[ri][2].IsNull());
      }
      ++ri;
    }
  }
}

TEST_F(RelationalRegressionTest,
       DeferredMaterializeColumnarPkLookupPreservesMatchesNullsAndTypes) {
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, dim,
        rs_->CreateTable(
            setup,
            Schema("DefColDim", {Column("pk", ValueType::kInt64,
                                        Constraint(Constraint::kPrimaryKey)),
                                 Column("amt", ValueType::kDouble),
                                 Column("tag", ValueType::kVarChar),
                                 Column("dt", ValueType::kDate)})));
    for (int64_t k = 1; k <= 50; ++k) {
      const Value tag_v =
          (k % 5 == 0) ? Value() : Value("tag_" + std::to_string(k));
      const Value amt_v =
          (k % 7 == 0) ? Value() : Value(static_cast<double>(k) * 0.5);
      ASSERT_SUCCESS(dim.Insert(setup.txn_, Row({Value(k), amt_v, tag_v,
                                                 Value::DateFromDays(100 + k)}))
                         .GetStatus());
    }
    ASSIGN_OR_ASSERT_FAIL(
        Table, fact,
        rs_->CreateTable(
            setup, Schema("DefColFact", {Column("fk", ValueType::kInt64),
                                         Column("m_id", ValueType::kInt64),
                                         Column("qty", ValueType::kInt64)})));
    for (int64_t i = 0; i < 40000; ++i) {
      // Include unmatched fk=999 alongside matched fk in [1, 50].
      const int64_t fk = (i % 13 == 0) ? 999 : ((i % 50) + 1);
      const int64_t mid = (i % 200) + 1;
      ASSERT_SUCCESS(fact.Insert(setup.txn_, Row({Value(fk), Value(mid),
                                                  Value((i % 7) + 1)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "CREATE TABLE DefColMid (m_id INT64, g_id INT64)");
  for (int64_t m = 1; m <= 200; ++m) {
    RelationalRun(*rs_, "INSERT INTO DefColMid VALUES (" + std::to_string(m) +
                            ", " + std::to_string((m % 3) + 1) + ")");
  }
  RelationalRun(*rs_, "CREATE TABLE DefColGrp (g_id INT64, g_name STRING)");
  RelationalRun(*rs_, "INSERT INTO DefColGrp VALUES (1, 'g1'), (2, 'g2')");
  for (const auto* tbl :
       {"DefColDim", "DefColFact", "DefColMid", "DefColGrp"}) {
    auto ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CompactTableToTlcs(ctx, tbl));
    ASSERT_SUCCESS(ctx.PreCommit());
    RelationalRun(*rs_, std::string("ANALYZE ") + tbl);
  }
  const std::string sql =
      "SELECT g.g_name, d.tag, EXTRACT(YEAR FROM d.dt), "
      "SUM(f.qty * d.amt), COUNT(*) "
      "FROM DefColFact f "
      "JOIN DefColDim d ON f.fk = d.pk "
      "JOIN DefColMid m ON f.m_id = m.m_id "
      "JOIN DefColGrp g ON m.g_id = g.g_id "
      "WHERE d.pk <= 30 "
      "GROUP BY g.g_name, d.tag, EXTRACT(YEAR FROM d.dt) "
      "ORDER BY g.g_name, d.tag NULLS FIRST";
  const auto expected = RelationalRun(*rs_, sql, /*force_relational=*/false);
  const auto actual = RelationalRun(*rs_, sql, /*force_relational=*/true);
  EXPECT_FALSE(expected.empty());
  EXPECT_EQ(actual, expected);
}

TEST_F(RelationalRegressionTest,
       TlcsSynopsesPreservePkPromotionAfterStoreEviction) {
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, dim,
        rs_->CreateTable(
            setup, Schema("SynDim", {Column("id", ValueType::kInt64),
                                     Column("grp", ValueType::kInt64),
                                     Column("val", ValueType::kDouble)})));
    for (int64_t k = 1; k <= 2000; ++k) {
      ASSERT_SUCCESS(
          dim.Insert(setup.txn_, Row({Value(k), Value((k % 5) + 1),
                                      Value(static_cast<double>(k) * 0.5)}))
              .GetStatus());
    }
    ASSIGN_OR_ASSERT_FAIL(
        Table, fact,
        rs_->CreateTable(
            setup, Schema("SynFact", {Column("fk", ValueType::kInt64),
                                      Column("amt", ValueType::kInt64)})));
    for (int64_t i = 0; i < 35000; ++i) {
      ASSERT_SUCCESS(fact.Insert(setup.txn_, Row({Value((i % 2000) + 1),
                                                  Value((i % 7) + 1)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  for (const auto* tbl : {"SynDim", "SynFact"}) {
    auto ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CompactTableToTlcs(ctx, tbl));
    ASSERT_SUCCESS(ctx.PreCommit());
    RelationalRun(*rs_, std::string("ANALYZE ") + tbl);
  }
  {
    auto ctx = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, dim, rs_->GetTable(ctx, "SynDim"));
    ASSIGN_OR_ASSERT_FAIL(Table, fact, rs_->GetTable(ctx, "SynFact"));
    ASSERT_NE(dim.GetBackingTlcsStore(), nullptr);
    ASSERT_NE(fact.GetBackingTlcsStore(), nullptr);
    EXPECT_TRUE(dim.GetBackingTlcsStore()->HasColumnSynopses());
    EXPECT_TRUE(
        dim.GetBackingTlcsStore()->ColumnSynopses()[0].is_unique_nonnull);

    dim.SetLSTableStore(nullptr);
    fact.SetLSTableStore(nullptr);
    (void)dim.GetBackingTlcsStore()->EvictDecodedCacheForTest(size_t{1} << 40);
    (void)fact.GetBackingTlcsStore()->EvictDecodedCacheForTest(size_t{1} << 40);
    (void)dim.EvictIdleColumnarHeadroom(size_t{1} << 40);
    (void)fact.EvictIdleColumnarHeadroom(size_t{1} << 40);
    EXPECT_EQ(dim.GetLSTableStore(), nullptr);
    EXPECT_EQ(fact.GetLSTableStore(), nullptr);
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  std::array<int64_t, 5> expected_sums{};
  for (int64_t i = 0; i < 35000; ++i) {
    const int64_t k = (i % 2000) + 1;
    const int64_t grp = (k % 5) + 1;
    expected_sums[static_cast<size_t>(grp - 1)] += (i % 7) + 1;
  }
  const std::string sql =
      "SELECT d.grp, SUM(f.amt) FROM SynFact f JOIN SynDim d ON f.fk = d.id "
      "GROUP BY d.grp ORDER BY d.grp";
  for (bool force_rel : {false, true}) {
    const auto rows = RelationalRun(*rs_, sql, force_rel);
    ASSERT_EQ(rows.size(), 5U);
    for (int64_t g = 1; g <= 5; ++g) {
      EXPECT_EQ(rows[static_cast<size_t>(g - 1)][0], Value(g));
      EXPECT_EQ(rows[static_cast<size_t>(g - 1)][1],
                Value(expected_sums[static_cast<size_t>(g - 1)]));
    }
  }
}

TEST_F(RelationalRegressionTest,
       BlockLazyDeferredProbeAndParallelScanMatchDenseResultsAndPruneBlocks) {
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, dim,
        rs_->CreateTable(
            setup, Schema("BlDim", {Column("k", ValueType::kInt64),
                                    Column("grp", ValueType::kInt64),
                                    Column("d_day", ValueType::kInt64)})));
    for (int64_t k = 1; k <= 200; ++k) {
      ASSERT_SUCCESS(
          dim.Insert(setup.txn_, Row({Value(k), Value(k % 10), Value(k * 3)}))
              .GetStatus());
    }
    ASSIGN_OR_ASSERT_FAIL(
        Table, fact,
        rs_->CreateTable(
            setup, Schema("BlFact", {Column("k", ValueType::kInt64),
                                     Column("dt", ValueType::kInt64),
                                     Column("tag", ValueType::kVarChar),
                                     Column("note", ValueType::kVarChar),
                                     Column("val", ValueType::kDouble)})));
    constexpr std::array<const char*, 4> kTags{"alpha", "beta", "gamma",
                                               "delta"};
    constexpr int64_t kFactRows = 72000;
    for (int64_t i = 0; i < kFactRows; ++i) {
      Value tag = (i % 19 == 0)
                      ? Value()
                      : Value(kTags[static_cast<size_t>(i % kTags.size())]);
      Value note = (i % 23 == 0) ? Value() : Value("n_" + std::to_string(i));
      const double dv = static_cast<double>((i % 50) + 1) * 0.5;
      ASSERT_SUCCESS(
          fact.Insert(setup.txn_,
                      Row({Value((i % 200) + 1), Value((i ^ 1) + 1),
                           std::move(tag), std::move(note), Value(dv)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  for (const auto* tbl : {"BlDim", "BlFact"}) {
    auto ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->CompactTableToTlcs(ctx, tbl));
    ASSERT_SUCCESS(ctx.PreCommit());
    RelationalRun(*rs_, std::string("ANALYZE ") + tbl);
  }

  auto clear_caches_and_read_gen = [&](uint64_t* gen_out) {
    auto ctx = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, fact, rs_->GetTable(ctx, "BlFact"));
    *gen_out = fact.LayoutGeneration();
    fact.SetLSTableStore(nullptr);
    if (fact.GetBackingTlcsStore() != nullptr) {
      (void)fact.GetBackingTlcsStore()->EvictDecodedCacheForTest(size_t{1}
                                                                 << 40);
    }
    (void)fact.EvictIdleColumnarHeadroom(size_t{1} << 40);
    ASSERT_SUCCESS(ctx.PreCommit());
  };
  auto check_lazy_pruned = [&](uint64_t gen, slot_t col) {
    auto ctx = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, fact, rs_->GetTable(ctx, "BlFact"));
    auto entry = fact.GetCachedLSTableColumn(col, gen);
    ASSERT_NE(entry, nullptr);
    EXPECT_TRUE(entry->is_block_lazy);
    EXPECT_LT(entry->LazyDecodedBlockCount(), entry->blocks.size());
    EXPECT_GT(entry->LazyDecodedBlockCount(), 0U);
    ASSERT_SUCCESS(ctx.PreCommit());
  };

  uint64_t gen = 0;
  clear_caches_and_read_gen(&gen);

  // 1. Selective filter scan hitting only block 2 (dt >= 68000).
  const std::string scan_sql =
      "SELECT k, dt, tag, val FROM BlFact WHERE dt >= 68000 ORDER BY dt LIMIT "
      "25";
  const auto scan_expected =
      RelationalRun(*rs_, scan_sql, /*force_relational=*/false);
  clear_caches_and_read_gen(&gen);
  const auto scan_actual =
      RelationalRun(*rs_, scan_sql, /*force_relational=*/true);
  EXPECT_FALSE(scan_expected.empty());
  EXPECT_EQ(scan_actual, scan_expected);
  check_lazy_pruned(gen, 0);

  // 2. Narrow-dictionary filter scan on lazy blocks.
  const std::string dict_sql =
      "SELECT dt, tag FROM BlFact WHERE tag = 'beta' AND dt >= 68000 ORDER BY "
      "dt";
  const auto dict_expected =
      RelationalRun(*rs_, dict_sql, /*force_relational=*/false);
  clear_caches_and_read_gen(&gen);
  const auto dict_actual =
      RelationalRun(*rs_, dict_sql, /*force_relational=*/true);
  EXPECT_FALSE(dict_expected.empty());
  EXPECT_EQ(dict_actual, dict_expected);
  check_lazy_pruned(gen, 2);

  // 3. Deferred-probe GROUP BY join on lazy blocks (pruned to last block).
  const std::string grp_sql =
      "SELECT d.grp, SUM(f.val), COUNT(*) FROM BlFact f "
      "JOIN BlDim d ON f.k = d.k WHERE f.dt >= 69000 "
      "GROUP BY d.grp ORDER BY d.grp";
  const auto grp_expected =
      RelationalRun(*rs_, grp_sql, /*force_relational=*/false);
  clear_caches_and_read_gen(&gen);
  const auto grp_actual =
      RelationalRun(*rs_, grp_sql, /*force_relational=*/true);
  EXPECT_EQ(grp_expected.size(), 10U);
  EXPECT_EQ(grp_actual, grp_expected);
  {
    auto ctx = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, fact, rs_->GetTable(ctx, "BlFact"));
    auto col0_entry = fact.GetCachedLSTableColumn(0, gen);
    ASSERT_NE(col0_entry, nullptr);
    EXPECT_TRUE(col0_entry->is_block_lazy);
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  check_lazy_pruned(gen, 1);
  check_lazy_pruned(gen, 4);

  // 4. SELECT DISTINCT join via fused_grouped_join_stream.
  const std::string dist_sql =
      "SELECT DISTINCT d.grp, f.k FROM BlFact f JOIN BlDim d ON f.k = d.k "
      "WHERE f.dt >= 71800 ORDER BY d.grp, f.k";
  const auto dist_expected =
      RelationalRun(*rs_, dist_sql, /*force_relational=*/false);
  clear_caches_and_read_gen(&gen);
  const auto dist_actual =
      RelationalRun(*rs_, dist_sql, /*force_relational=*/true);
  EXPECT_FALSE(dist_expected.empty());
  EXPECT_EQ(dist_actual, dist_expected);

  // 5. Streamed probe join with VARCHAR payload columns.
  const std::string join_sql =
      "SELECT f.dt, f.tag, f.note, d.grp FROM BlFact f JOIN BlDim d ON f.k = "
      "d.k "
      "WHERE f.dt >= 71900 AND d.grp < 3 ORDER BY f.dt, d.grp";
  const auto join_expected =
      RelationalRun(*rs_, join_sql, /*force_relational=*/false);
  clear_caches_and_read_gen(&gen);
  const auto join_actual =
      RelationalRun(*rs_, join_sql, /*force_relational=*/true);
  EXPECT_FALSE(join_expected.empty());
  EXPECT_EQ(join_actual, join_expected);
}

TEST_F(RelationalRegressionTest,
       FusedGroupedJoinDoesNotAliasDistinctSideTablePrograms) {
  {
    auto setup = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, fact,
        rs_->CreateTable(
            setup, Schema("FusedFact", {Column("k", ValueType::kInt64),
                                        Column("val", ValueType::kInt64)})));
    ASSIGN_OR_ASSERT_FAIL(
        Table, dim,
        rs_->CreateTable(
            setup, Schema("FusedDim", {Column("k", ValueType::kInt64),
                                       Column("hr", ValueType::kInt64),
                                       Column("tag", ValueType::kVarChar)})));
    for (int64_t i = 0; i < 60; ++i) {
      ASSERT_SUCCESS(
          fact.Insert(setup.txn_, Row({Value(i), Value(10 + i)})).GetStatus());
      const int64_t hr =
          (i < 20) ? (8 + (i % 2)) : ((i < 32) ? (19 + (i % 2)) : 12);
      const char* tag = (i < 15)   ? "a"
                        : (i < 25) ? "b"
                        : (i < 35) ? "c"
                        : (i < 42) ? "d"
                                   : "e";
      ASSERT_SUCCESS(
          dim.Insert(setup.txn_, Row({Value(i), Value(hr), Value(tag)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  RelationalRun(*rs_, "ANALYZE FusedFact");
  RelationalRun(*rs_, "ANALYZE FusedDim");

  // 1. Distinct Int64RangeSpec (`kInt64InRange`) programs in one join +
  // TPC-DS Q90-shaped merged scalar subquery cross join.
  const std::string range_sql =
      "SELECT SUM(CASE WHEN d.hr BETWEEN 8 AND 9 THEN 1 ELSE 0 END) AS amc, "
      "SUM(CASE WHEN d.hr BETWEEN 19 AND 20 THEN 1 ELSE 0 END) AS pmc "
      "FROM FusedFact f JOIN FusedDim d ON f.k = d.k";
  const auto range_rel =
      RelationalRun(*rs_, range_sql, /*force_relational=*/true);
  const auto range_opt =
      RelationalRun(*rs_, range_sql, /*force_relational=*/false);
  ASSERT_EQ(range_rel.size(), 1U);
  EXPECT_EQ(range_rel[0][0], Value(int64_t{20}));
  EXPECT_EQ(range_rel[0][1], Value(int64_t{12}));
  EXPECT_EQ(range_rel, range_opt);

  const std::string q90_sql =
      "SELECT CASE WHEN pmc = 0 THEN NULL "
      "ELSE CAST(amc AS FLOAT64) / CAST(pmc AS FLOAT64) END AS ratio "
      "FROM (SELECT COUNT(*) AS amc FROM FusedFact f, FusedDim d "
      "      WHERE f.k = d.k AND d.hr BETWEEN 8 AND 8 + 1) AS at_, "
      "     (SELECT COUNT(*) AS pmc FROM FusedFact f, FusedDim d "
      "      WHERE f.k = d.k AND d.hr BETWEEN 19 AND 19 + 1) AS pt";
  const auto q90_rel = RelationalRun(*rs_, q90_sql, /*force_relational=*/true);
  const auto q90_opt = RelationalRun(*rs_, q90_sql, /*force_relational=*/false);
  ASSERT_EQ(q90_rel.size(), 1U);
  ASSERT_EQ(q90_opt.size(), 1U);
  EXPECT_NEAR(q90_rel[0][0].value.double_value, 20.0 / 12.0, 1e-12);
  EXPECT_NEAR(q90_opt[0][0].value.double_value, 20.0 / 12.0, 1e-12);

  // 2. Distinct ReciprocalDivModSpec (`kDivInt64Const` / `kModInt64Const`)
  // programs sharing identical opcodes and constants.
  int64_t exp_div7 = 0;
  int64_t exp_div13 = 0;
  int64_t exp_mod7 = 0;
  int64_t exp_mod13 = 0;
  for (int64_t i = 0; i < 60; ++i) {
    const int64_t v = 10 + i;
    exp_div7 += v / 7;
    exp_div13 += v / 13;
    exp_mod7 += v % 7;
    exp_mod13 += v % 13;
  }
  const std::string divmod_sql =
      "SELECT SUM(DIV(f.val, 7)), SUM(DIV(f.val, 13)), "
      "SUM(MOD(f.val, 7)), SUM(MOD(f.val, 13)) "
      "FROM FusedFact f JOIN FusedDim d ON f.k = d.k";
  const auto divmod_rel =
      RelationalRun(*rs_, divmod_sql, /*force_relational=*/true);
  const auto divmod_opt =
      RelationalRun(*rs_, divmod_sql, /*force_relational=*/false);
  ASSERT_EQ(divmod_rel.size(), 1U);
  EXPECT_EQ(divmod_rel[0][0], Value(exp_div7));
  EXPECT_EQ(divmod_rel[0][1], Value(exp_div13));
  EXPECT_EQ(divmod_rel[0][2], Value(exp_mod7));
  EXPECT_EQ(divmod_rel[0][3], Value(exp_mod13));
  EXPECT_EQ(divmod_rel, divmod_opt);

  // 3. Distinct StringSetSpec (`kInStringSet`) programs in one join.
  const std::string sset_sql =
      "SELECT SUM(CASE WHEN d.tag IN ('a', 'b') THEN 1 ELSE 0 END), "
      "SUM(CASE WHEN d.tag IN ('c', 'd') THEN 1 ELSE 0 END) "
      "FROM FusedFact f JOIN FusedDim d ON f.k = d.k";
  const auto sset_rel =
      RelationalRun(*rs_, sset_sql, /*force_relational=*/true);
  const auto sset_opt =
      RelationalRun(*rs_, sset_sql, /*force_relational=*/false);
  ASSERT_EQ(sset_rel.size(), 1U);
  EXPECT_EQ(sset_rel[0][0], Value(int64_t{25}));
  EXPECT_EQ(sset_rel[0][1], Value(int64_t{17}));
  EXPECT_EQ(sset_rel, sset_opt);
}

// Lean proof: lean/TinylambOpt/ResearchProofs/NthNonNullFrame.lean (sound)
TEST_F(RelationalRegressionTest,
       NthNonNullFrameMatchesFallbackAcrossIgnoreNullsAndExclusions) {
  struct ScopedEnv {  // NOLINT(cppcoreguidelines-special-member-functions):
                      // scoped environment restore.
    const char* name;
    std::string prev;
    bool had_prev{false};
    ScopedEnv(const char* n, const char* val) : name(n) {
      if (const char* cur = ::tinylamb::platform::GetEnv(name)) {
        prev = cur;
        had_prev = true;
      }
      if (val != nullptr) {
        ::tinylamb::platform::SetEnv(name, val, 1);
      } else {
        ::tinylamb::platform::UnsetEnv(name);
      }
    }
    ~ScopedEnv() {
      if (had_prev) {
        ::tinylamb::platform::SetEnv(name, prev.c_str(), 1);
      } else {
        ::tinylamb::platform::UnsetEnv(name);
      }
    }
  };

  RelationalRun(*rs_, "CREATE TABLE w_nth (p INT64, id INT64, v INT64);");
  RelationalRun(*rs_,
                "INSERT INTO w_nth VALUES "
                "(1, 1, NULL), (1, 2, 10), (1, 3, NULL), (1, 4, 20), "
                "(1, 5, NULL), (1, 6, 30), (1, 7, 40), (1, 8, NULL), "
                "(2, 1, NULL), (2, 2, NULL), (2, 3, NULL), "
                "(3, 1, 100), (3, 2, 200);");

  const std::string sql =
      "SELECT p, id, "
      "FIRST_VALUE(v IGNORE NULLS) OVER (PARTITION BY p ORDER BY id ROWS "
      "BETWEEN 2 PRECEDING AND 2 FOLLOWING), "
      "LAST_VALUE(v IGNORE NULLS) OVER (PARTITION BY p ORDER BY id ROWS "
      "BETWEEN "
      "2 PRECEDING AND 2 FOLLOWING), "
      "NTH_VALUE(v, 2 IGNORE NULLS) OVER (PARTITION BY p ORDER BY id ROWS "
      "BETWEEN 2 PRECEDING AND 2 FOLLOWING), "
      "FIRST_VALUE(v IGNORE NULLS) OVER (PARTITION BY p ORDER BY id ROWS "
      "BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), "
      "NTH_VALUE(v, 2 IGNORE NULLS) OVER (PARTITION BY p ORDER BY id ROWS "
      "BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) "
      "FROM w_nth ORDER BY p, id;";

  std::vector<Row> pipe_on;
  {
    ScopedEnv guard("TINYLAMB_DISABLE_NTH_NON_NULL_FRAME", nullptr);
    pipe_on = RelationalRun(*rs_, sql, /*force_relational=*/false);
  }
  std::vector<Row> pipe_off;
  {
    ScopedEnv guard("TINYLAMB_DISABLE_NTH_NON_NULL_FRAME", "1");
    pipe_off = RelationalRun(*rs_, sql, /*force_relational=*/false);
  }
  std::vector<Row> rel_on;
  {
    ScopedEnv guard("TINYLAMB_DISABLE_NTH_NON_NULL_FRAME", nullptr);
    rel_on = RelationalRun(*rs_, sql, /*force_relational=*/true);
  }
  EXPECT_EQ(pipe_on.size(), 13U);
  EXPECT_EQ(pipe_on, pipe_off);
  EXPECT_EQ(pipe_on, rel_on);

  // Also test EXCLUDE CURRENT ROW directly through ComputeOneWindow.
  const Schema schema(
      "w", {Column("p", ValueType::kInt64), Column("id", ValueType::kInt64),
            Column("v", ValueType::kInt64)});
  std::vector<Row> rows{
      Row({Value(1), Value(1), Value()}), Row({Value(1), Value(2), Value(10)}),
      Row({Value(1), Value(3), Value()}), Row({Value(1), Value(4), Value(20)}),
      Row({Value(1), Value(5), Value()}), Row({Value(1), Value(6), Value(30)}),
      Row({Value(2), Value(1), Value()}), Row({Value(2), Value(2), Value()}),
  };
  for (const std::string fn : {"FIRST_VALUE", "LAST_VALUE", "NTH_VALUE"}) {
    WindowFunctionCallExpression win;
    win.function = fn;
    win.args = {ColumnValueExp("v")};
    if (fn == "NTH_VALUE") {
      win.args.push_back(ConstantValueExp(Value(int64_t{2})));
    }
    win.partition_by = {ColumnValueExp("p")};
    win.order_by = {{.expression = ColumnValueExp("id"), .ascending = true}};
    win.has_frame = true;
    win.frame_unit = WindowFrameUnit::kRows;
    win.frame_start = {.type = WindowFrameBoundType::kUnboundedPreceding,
                       .offset = nullptr};
    win.frame_end = {.type = WindowFrameBoundType::kUnboundedFollowing,
                     .offset = nullptr};
    win.exclusion = WindowFrameExclusion::kCurrentRow;
    win.ignore_nulls = true;

    auto run_direct = [&](const char* env_val) {
      ScopedEnv guard("TINYLAMB_DISABLE_NTH_NON_NULL_FRAME", env_val);
      TransactionContext ctx = rs_->BeginReadOnlyContext();
      std::vector<Row> copy = rows;
      std::vector<Value> out;
      const relational_detail::CteMap empty_ctes;
      EXPECT_EQ(relational_detail::ComputeOneWindow(ctx, win, copy, schema,
                                                    nullptr, empty_ctes, &out),
                Status::kSuccess);
      return out;
    };
    EXPECT_EQ(run_direct(nullptr), run_direct("1")) << fn;
  }
}

// Lean proof: lean/TinylambOpt/ResearchProofs/RangeDistinctIndex.lean (sound)
TEST_F(RelationalRegressionTest,
       RangeDistinctIndexMatchesFallbackAcrossArbitraryFrames) {
  struct ScopedEnv {  // NOLINT(cppcoreguidelines-special-member-functions):
                      // scoped environment restore.
    const char* name;
    std::string prev;
    bool had_prev{false};
    ScopedEnv(const char* n, const char* val) : name(n) {
      if (const char* cur = ::tinylamb::platform::GetEnv(name)) {
        prev = cur;
        had_prev = true;
      }
      if (val != nullptr) {
        ::tinylamb::platform::SetEnv(name, val, 1);
      } else {
        ::tinylamb::platform::UnsetEnv(name);
      }
    }
    ~ScopedEnv() {
      if (had_prev) {
        ::tinylamb::platform::SetEnv(name, prev.c_str(), 1);
      } else {
        ::tinylamb::platform::UnsetEnv(name);
      }
    }
  };

  RelationalRun(*rs_, "CREATE TABLE w_rdist (p INT64, ord INT64, v INT64);");
  std::string ins = "INSERT INTO w_rdist VALUES ";
  for (int i = 0; i < 40; ++i) {
    if (i > 0) {
      ins += ", ";
    }
    const int part = i / 20;
    const std::string val = (i % 7 == 0) ? "NULL" : std::to_string((i * 3) % 6);
    ins += "(" + std::to_string(part) + ", " + std::to_string(i % 20) + ", " +
           val + ")";
  }
  ins += ";";
  RelationalRun(*rs_, ins);

  const std::string sql =
      "SELECT p, ord, "
      "COUNT(DISTINCT v) OVER (PARTITION BY p ORDER BY ord ROWS BETWEEN 1 "
      "FOLLOWING AND 5 FOLLOWING), "
      "COUNT(DISTINCT v) OVER (PARTITION BY p ORDER BY ord ROWS BETWEEN 3 "
      "PRECEDING AND 3 FOLLOWING), "
      "COUNT(DISTINCT v) OVER (PARTITION BY p ORDER BY ord ROWS BETWEEN 2 "
      "PRECEDING AND CURRENT ROW), "
      "COUNT(DISTINCT v) OVER (PARTITION BY p ORDER BY ord RANGE BETWEEN 3 "
      "PRECEDING AND 3 FOLLOWING) "
      "FROM w_rdist ORDER BY p, ord;";

  std::vector<Row> on_rows;
  {
    ScopedEnv guard("TINYLAMB_DISABLE_RANGE_DISTINCT_INDEX", nullptr);
    on_rows = RelationalRun(*rs_, sql, /*force_relational=*/false);
  }
  std::vector<Row> off_rows;
  {
    ScopedEnv guard("TINYLAMB_DISABLE_RANGE_DISTINCT_INDEX", "1");
    off_rows = RelationalRun(*rs_, sql, /*force_relational=*/false);
  }
  EXPECT_EQ(on_rows.size(), 40U);
  EXPECT_EQ(on_rows, off_rows);

  // Also test EXCLUDE CURRENT ROW directly through ComputeOneWindow.
  const Schema schema(
      "r", {Column("p", ValueType::kInt64), Column("ord", ValueType::kInt64),
            Column("v", ValueType::kInt64)});
  std::vector<Row> rows;
  rows.reserve(20);
  for (int i = 0; i < 20; ++i) {
    rows.emplace_back(std::vector<Value>{
        Value(i / 10), Value(i % 10),
        (i % 5 == 0) ? Value() : Value(static_cast<int64_t>(i % 4))});
  }
  WindowFunctionCallExpression win;
  win.function = "COUNT";
  win.distinct = true;
  win.args = {ColumnValueExp("v")};
  win.partition_by = {ColumnValueExp("p")};
  win.order_by = {{.expression = ColumnValueExp("ord"), .ascending = true}};
  win.has_frame = true;
  win.frame_unit = WindowFrameUnit::kRows;
  win.frame_start = {.type = WindowFrameBoundType::kOffsetPreceding,
                     .offset = ConstantValueExp(Value(int64_t{3}))};
  win.frame_end = {.type = WindowFrameBoundType::kOffsetFollowing,
                   .offset = ConstantValueExp(Value(int64_t{3}))};
  win.exclusion = WindowFrameExclusion::kCurrentRow;

  auto run_direct = [&](const char* env_val) {
    ScopedEnv guard("TINYLAMB_DISABLE_RANGE_DISTINCT_INDEX", env_val);
    TransactionContext ctx = rs_->BeginReadOnlyContext();
    std::vector<Row> copy = rows;
    std::vector<Value> out;
    const relational_detail::CteMap empty_ctes;
    EXPECT_EQ(relational_detail::ComputeOneWindow(ctx, win, copy, schema,
                                                  nullptr, empty_ctes, &out),
              Status::kSuccess);
    return out;
  };
  EXPECT_EQ(run_direct(nullptr), run_direct("1"));
}

// Lean proof: lean/TinylambOpt/ResearchProofs/QuantifiedRangeSummary.lean
// (sound)
TEST_F(RelationalRegressionTest,
       QuantifiedRangeSummaryMatchesFallbackAllOpsAndNulls) {
  struct ScopedEnv {  // NOLINT(cppcoreguidelines-special-member-functions):
                      // scoped environment restore.
    const char* name;
    std::string prev;
    bool had_prev{false};
    ScopedEnv(const char* n, const char* val) : name(n) {
      if (const char* cur = ::tinylamb::platform::GetEnv(name)) {
        prev = cur;
        had_prev = true;
      }
      if (val != nullptr) {
        ::tinylamb::platform::SetEnv(name, val, 1);
      } else {
        ::tinylamb::platform::UnsetEnv(name);
      }
    }
    ~ScopedEnv() {
      if (had_prev) {
        ::tinylamb::platform::SetEnv(name, prev.c_str(), 1);
      } else {
        ::tinylamb::platform::UnsetEnv(name);
      }
    }
  };

  RelationalRun(*rs_, "CREATE TABLE qo (id INT64, k INT64, x INT64);");
  RelationalRun(*rs_, "CREATE TABLE qi (k INT64, y INT64);");
  RelationalRun(*rs_,
                "INSERT INTO qi VALUES "
                "(1, 10), (1, 20), (1, 30), "
                "(2, 10), (2, NULL), (2, 30), "
                "(3, NULL), (3, NULL);");
  RelationalRun(*rs_,
                "INSERT INTO qo VALUES "
                "(1, 1, 5), (2, 1, 10), (3, 1, 20), (4, 1, 30), (5, 1, 35), "
                "(6, 1, NULL), "
                "(7, 2, 5), (8, 2, 15), (9, 2, 35), (10, 2, NULL), "
                "(11, 3, 15), (12, 3, NULL), "
                "(13, 99, 15), (14, 99, NULL), "
                "(15, NULL, 15), (16, NULL, NULL);");

  const std::string sql =
      "SELECT o.id, "
      "o.x < ANY (SELECT i.y FROM qi i WHERE i.k = o.k), "
      "o.x <= ANY (SELECT i.y FROM qi i WHERE i.k = o.k), "
      "o.x > ANY (SELECT i.y FROM qi i WHERE i.k = o.k), "
      "o.x >= ANY (SELECT i.y FROM qi i WHERE i.k = o.k), "
      "o.x < ALL (SELECT i.y FROM qi i WHERE i.k = o.k), "
      "o.x <= ALL (SELECT i.y FROM qi i WHERE i.k = o.k), "
      "o.x > ALL (SELECT i.y FROM qi i WHERE i.k = o.k), "
      "o.x >= ALL (SELECT i.y FROM qi i WHERE i.k = o.k) "
      "FROM qo o ORDER BY o.id;";

  std::vector<Row> on_rows;
  {
    ScopedEnv guard("TINYLAMB_DISABLE_QUANTIFIED_RANGE_SUMMARY", nullptr);
    on_rows = RelationalRun(*rs_, sql, /*force_relational=*/false);
  }
  std::vector<Row> off_rows;
  {
    ScopedEnv guard("TINYLAMB_DISABLE_QUANTIFIED_RANGE_SUMMARY", "1");
    off_rows = RelationalRun(*rs_, sql, /*force_relational=*/false);
  }
  ASSERT_EQ(on_rows.size(), 16U);
  EXPECT_EQ(on_rows, off_rows);
  // Empty group (id=13 k=99 non-NULL x, id=14 k=99 NULL x, id=15/16 NULL k):
  // ANY is ALWAYS false (0), ALL is ALWAYS true (1).
  for (size_t idx : {12U, 13U, 14U, 15U}) {
    EXPECT_EQ(on_rows[idx][1], Value(int64_t{0})) << "idx=" << idx;
    EXPECT_EQ(on_rows[idx][5], Value(int64_t{1})) << "idx=" << idx;
  }
}

// Lean proof: lean/TinylambOpt/ResearchProofs/DemandScalar.lean (sound)
TEST_F(RelationalRegressionTest,
       DemandScalarDefersUnusedMultiRowErrorAndErrorsOnActive) {
  struct ScopedEnv {  // NOLINT(cppcoreguidelines-special-member-functions):
                      // scoped environment restore.
    const char* name;
    std::string prev;
    bool had_prev{false};
    ScopedEnv(const char* n, const char* val) : name(n) {
      if (const char* cur = ::tinylamb::platform::GetEnv(name)) {
        prev = cur;
        had_prev = true;
      }
      if (val != nullptr) {
        ::tinylamb::platform::SetEnv(name, val, 1);
      } else {
        ::tinylamb::platform::UnsetEnv(name);
      }
    }
    ~ScopedEnv() {
      if (had_prev) {
        ::tinylamb::platform::SetEnv(name, prev.c_str(), 1);
      } else {
        ::tinylamb::platform::UnsetEnv(name);
      }
    }
  };

  RelationalRun(*rs_, "CREATE TABLE ds_outer (id INT64, k INT64);");
  RelationalRun(*rs_, "CREATE TABLE ds_inner (k INT64, val INT64);");
  RelationalRun(*rs_,
                "INSERT INTO ds_inner VALUES "
                "(1, 100), (3, NULL), (99, 10), (99, 20);");
  RelationalRun(*rs_,
                "INSERT INTO ds_outer VALUES (1, 1), (2, 2), (3, 3), "
                "(4, NULL);");

  const std::string sql =
      "SELECT o.id, (SELECT i.val FROM ds_inner i WHERE i.k = o.k) "
      "FROM ds_outer o ORDER BY o.id;";

  std::vector<Row> on_rows;
  {
    ScopedEnv guard("TINYLAMB_DISABLE_DEMAND_SCALAR", nullptr);
    on_rows = RelationalRun(*rs_, sql, /*force_relational=*/false);
  }
  std::vector<Row> off_rows;
  {
    ScopedEnv guard("TINYLAMB_DISABLE_DEMAND_SCALAR", "1");
    off_rows = RelationalRun(*rs_, sql, /*force_relational=*/false);
  }
  ASSERT_EQ(on_rows.size(), 4U);
  EXPECT_EQ(on_rows, off_rows);
  EXPECT_EQ(on_rows[0][1], Value(int64_t{100}));
  EXPECT_TRUE(on_rows[1][1].IsNull());
  EXPECT_TRUE(on_rows[2][1].IsNull());
  EXPECT_TRUE(on_rows[3][1].IsNull());

  // Now activate key 99 (which has two matching rows in ds_inner): must fail.
  RelationalRun(*rs_, "INSERT INTO ds_outer VALUES (5, 99);");
  for (const char* env_val : {static_cast<const char*>(nullptr), "1"}) {
    ScopedEnv guard("TINYLAMB_DISABLE_DEMAND_SCALAR", env_val);
    TransactionContext ctx = rs_->BeginContext();
    SqlEngine engine(*rs_);
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    ASSERT_TRUE(prepared.HasValue());
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    EXPECT_EQ(prepared.Value()->GetStatus().GetCode(),
              StatusCode::kInvalidArgument);
    EXPECT_NE(prepared.Value()->GetStatus().GetMessage().find("more than one"),
              std::string::npos);
    EXPECT_EQ(tinylamb::Status::kSuccess, ctx.Abort());
  }
}

// Lean proof: lean/TinylambOpt/ResearchProofs/SkylineAntiJoin.lean (sound)
TEST_F(RelationalRegressionTest,
       SkylineAntiJoinMatchesFallbackTiesNullsAndExtremes) {
  struct ScopedEnv {  // NOLINT(cppcoreguidelines-special-member-functions):
                      // scoped environment restore.
    const char* name;
    std::string prev;
    bool had_prev{false};
    ScopedEnv(const char* n, const char* val) : name(n) {
      if (const char* cur = ::tinylamb::platform::GetEnv(name)) {
        prev = cur;
        had_prev = true;
      }
      if (val != nullptr) {
        ::tinylamb::platform::SetEnv(name, val, 1);
      } else {
        ::tinylamb::platform::UnsetEnv(name);
      }
    }
    ~ScopedEnv() {
      if (had_prev) {
        ::tinylamb::platform::SetEnv(name, prev.c_str(), 1);
      } else {
        ::tinylamb::platform::UnsetEnv(name);
      }
    }
  };

  RelationalRun(*rs_, "CREATE TABLE sky_pts (id INT64, x INT64, y INT64);");
  RelationalRun(*rs_,
                "INSERT INTO sky_pts VALUES "
                "(1, 1, 10), (2, 1, 20), (3, 1, 10), "
                "(4, 2, 5), (5, 2, 15), (6, 3, 5), "
                "(7, 0, NULL), (8, NULL, 0), (9, NULL, NULL), "
                "(10, -9223372036854775807, 100), "
                "(11, 9223372036854775807, -9223372036854775807);");

  const std::string min_sky =
      "SELECT b.id, b.x, b.y FROM sky_pts b "
      "WHERE NOT EXISTS (SELECT 1 FROM sky_pts a "
      "                  WHERE a.x < b.x AND a.y < b.y) "
      "ORDER BY b.id;";
  const std::string max_sky =
      "SELECT b.id, b.x, b.y FROM sky_pts b "
      "WHERE NOT EXISTS (SELECT 1 FROM sky_pts a "
      "                  WHERE a.x > b.x AND a.y > b.y) "
      "ORDER BY b.id;";

  for (const std::string& sql : {min_sky, max_sky}) {
    std::vector<Row> on_rows;
    {
      ScopedEnv guard("TINYLAMB_DISABLE_SKYLINE_ANTIJOIN", nullptr);
      on_rows = RelationalRun(*rs_, sql, /*force_relational=*/false);
    }
    std::vector<Row> off_rows;
    {
      ScopedEnv guard("TINYLAMB_DISABLE_SKYLINE_ANTIJOIN", "1");
      off_rows = RelationalRun(*rs_, sql, /*force_relational=*/false);
    }
    EXPECT_EQ(on_rows, off_rows) << sql;
  }
}

// Lean proof: lean/TinylambOpt/ResearchProofs/IntervalCountSweep.lean (sound)
TEST_F(RelationalRegressionTest,
       IntervalCountSweepMatchesFallbackBoundariesNullsAndClosedOpen) {
  struct ScopedEnv {  // NOLINT(cppcoreguidelines-special-member-functions):
                      // scoped environment restore.
    const char* name;
    std::string prev;
    bool had_prev{false};
    ScopedEnv(const char* n, const char* val) : name(n) {
      if (const char* cur = ::tinylamb::platform::GetEnv(name)) {
        prev = cur;
        had_prev = true;
      }
      if (val != nullptr) {
        ::tinylamb::platform::SetEnv(name, val, 1);
      } else {
        ::tinylamb::platform::UnsetEnv(name);
      }
    }
    ~ScopedEnv() {
      if (had_prev) {
        ::tinylamb::platform::SetEnv(name, prev.c_str(), 1);
      } else {
        ::tinylamb::platform::UnsetEnv(name);
      }
    }
  };

  RelationalRun(*rs_, "CREATE TABLE iv_segs (lo INT64, hi INT64, tag INT64);");
  RelationalRun(*rs_, "CREATE TABLE iv_pts (id INT64, t INT64);");
  RelationalRun(*rs_,
                "INSERT INTO iv_segs VALUES "
                "(10, 20, 1), (10, 20, 2), (15, 25, 1), (20, 20, 1), "
                "(30, 10, 1), (NULL, 50, 1), (10, NULL, 1), "
                "(-9223372036854775807, 0, 1), "
                "(9223372036854775806, 9223372036854775807, 1);");
  RelationalRun(*rs_,
                "INSERT INTO iv_pts VALUES "
                "(1, -9223372036854775807), (2, 0), (3, 9), (4, 10), "
                "(5, 15), (6, 19), (7, 20), (8, 25), "
                "(9, 9223372036854775807), (10, NULL);");

  const std::vector<std::string> queries{
      "SELECT p.id, (SELECT COUNT(*) FROM iv_segs s "
      "              WHERE s.lo <= p.t AND p.t < s.hi) "
      "FROM iv_pts p ORDER BY p.id;",
      "SELECT p.id, (SELECT COUNT(*) FROM iv_segs s "
      "              WHERE s.lo <= p.t AND p.t <= s.hi) "
      "FROM iv_pts p ORDER BY p.id;",
      "SELECT p.id, (SELECT COUNT(*) FROM iv_segs s "
      "              WHERE s.lo < p.t AND p.t < s.hi) "
      "FROM iv_pts p ORDER BY p.id;",
      "SELECT p.id, (SELECT COUNT(*) FROM iv_segs s "
      "              WHERE s.lo <= p.t AND p.t < s.hi AND s.tag = 1) "
      "FROM iv_pts p ORDER BY p.id;",
  };

  for (const std::string& sql : queries) {
    std::vector<Row> on_rows;
    {
      ScopedEnv guard("TINYLAMB_DISABLE_INTERVAL_COUNT_SWEEP", nullptr);
      on_rows = RelationalRun(*rs_, sql, /*force_relational=*/false);
    }
    std::vector<Row> off_rows;
    {
      ScopedEnv guard("TINYLAMB_DISABLE_INTERVAL_COUNT_SWEEP", "1");
      off_rows = RelationalRun(*rs_, sql, /*force_relational=*/false);
    }
    ASSERT_EQ(on_rows.size(), 10U) << sql;
    EXPECT_EQ(on_rows, off_rows) << sql;
    // NULL point timestamp (id=10) must yield COUNT(*) = 0, never NULL.
    EXPECT_EQ(on_rows.back()[1], Value(int64_t{0})) << sql;
  }
}

// Lean proof: lean/TinylambOpt/ResearchProofs/PackedOrderKey.lean (sound)
// Lean proof: lean/TinylambOpt/ResearchProofs/StableRadix.lean (sound)
TEST_F(RelationalRegressionTest,
       ExternalSorterPackedOrderKeyMatchesFallbackViaSql) {
  struct ScopedEnv {  // NOLINT(cppcoreguidelines-special-member-functions):
                      // scoped environment restore.
    const char* name;
    std::string prev;
    bool had_prev{false};
    ScopedEnv(const char* n, const char* val) : name(n) {
      if (const char* cur = ::tinylamb::platform::GetEnv(name)) {
        prev = cur;
        had_prev = true;
      }
      if (val != nullptr) {
        ::tinylamb::platform::SetEnv(name, val, 1);
      } else {
        ::tinylamb::platform::UnsetEnv(name);
      }
    }
    ~ScopedEnv() {
      if (had_prev) {
        ::tinylamb::platform::SetEnv(name, prev.c_str(), 1);
      } else {
        ::tinylamb::platform::UnsetEnv(name);
      }
    }
  };

  RelationalRun(*rs_, "CREATE TABLE es_sort (a INT64, b INT64, tag INT64);");
  std::string ins = "INSERT INTO es_sort VALUES ";
  for (int i = 0; i < 120; ++i) {
    if (i > 0) {
      ins += ", ";
    }
    std::string sa;
    std::string sb;
    if (i % 15 == 0) {
      sa = "NULL";
    } else if (i == 7) {
      sa = "9223372036854775807";
    } else if (i == 13) {
      sa = "-9223372036854775807";
    } else {
      sa = std::to_string(((i * 7) % 13) - 6);
    }
    if (i % 11 == 0) {
      sb = "NULL";
    } else if (i == 19) {
      sb = "2147483648";
    } else {
      sb = std::to_string(((i * 5) % 9) - 4);
    }
    ins += '(';
    ins += sa;
    ins += ", ";
    ins += sb;
    ins += ", ";
    ins += std::to_string(i);
    ins += ')';
  }
  ins += ";";
  RelationalRun(*rs_, ins);

  const std::vector<std::string> queries{
      "SELECT a, b, tag FROM es_sort ORDER BY a ASC NULLS FIRST, b DESC NULLS "
      "LAST, tag ASC;",
      "SELECT a, b, tag FROM es_sort ORDER BY a DESC NULLS LAST, b ASC NULLS "
      "FIRST;",
      "SELECT a, b, tag FROM es_sort ORDER BY a ASC, b DESC LIMIT 15 OFFSET "
      "80;",
      "SELECT a, b, tag FROM es_sort WHERE a IS NULL OR a BETWEEN -5 AND 5 "
      "ORDER BY a ASC, b DESC;",
  };
  for (const std::string& sql : queries) {
    std::vector<Row> on_rows;
    {
      ScopedEnv guard("TINYLAMB_DISABLE_PACKED_ORDER_KEY", nullptr);
      on_rows = RelationalRun(*rs_, sql, /*force_relational=*/false);
    }
    std::vector<Row> off_rows;
    {
      ScopedEnv guard("TINYLAMB_DISABLE_PACKED_ORDER_KEY", "1");
      off_rows = RelationalRun(*rs_, sql, /*force_relational=*/false);
    }
    EXPECT_EQ(on_rows, off_rows) << sql;
  }
}

}  // namespace
}  // namespace tinylamb
