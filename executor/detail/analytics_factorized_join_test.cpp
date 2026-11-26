/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/analytics_factorized_join.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/platform/process.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/sql_engine.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

using relational_detail::EncodeSignedInt64ForRadix;
using relational_detail::PackShortString7;
using relational_detail::TryStableRadixSortNullableInt64;

class ScopedEnvVar {  // NOLINT(cppcoreguidelines-special-member-functions):
                      // scope-owned cleanup.

 public:
  ScopedEnvVar(const char* name, const char* value) : name_(name) {
    if (const char* old = ::tinylamb::platform::GetEnv(name_); old != nullptr) {
      had_old_ = true;
      old_value_ = old;
    }
    if (value != nullptr) {
      ::tinylamb::platform::SetEnv(name_, value, 1);
    } else {
      ::tinylamb::platform::UnsetEnv(name_);
    }
  }
  ScopedEnvVar(const ScopedEnvVar&) = delete;
  ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;
  ~ScopedEnvVar() {
    if (had_old_) {
      ::tinylamb::platform::SetEnv(name_, old_value_.c_str(), 1);
    } else {
      ::tinylamb::platform::UnsetEnv(name_);
    }
  }

 private:
  const char* name_;
  bool had_old_{false};
  std::string old_value_;
};

std::vector<Row> RunQuery(SqlEngine* engine, Database* db,
                          const std::string& sql) {
  TransactionContext ctx = db->BeginReadOnlyContext();
  StatusOr<QueryResult> res = engine->Execute(ctx, sql);
  if (!res.HasValue()) {
    ADD_FAILURE() << "Execute failed: " << engine->LastError()
                  << "\nSQL: " << sql
                  << "\nStatus: " << ToString(res.GetStatus());
    return {};
  }
  std::vector<Row> rows = res.Value().Collect();
  EXPECT_EQ(res.Value().GetStatus(), Status::kSuccess) << sql;
  return rows;
}

std::string CanonicalizeRows(const std::vector<Row>& rows, bool sort_rows) {
  std::vector<std::string> formatted;
  formatted.reserve(rows.size());
  for (const Row& r : rows) {
    formatted.push_back(r.ToString());
  }
  if (sort_rows) {
    std::sort(formatted.begin(), formatted.end());
  }
  std::string out;
  for (const auto& s : formatted) {
    out += s;
    out.push_back('\n');
  }
  return out;
}

void ExecDml(SqlEngine* engine, Database* db, const std::string& sql) {
  TransactionContext ctx = db->BeginContext();
  StatusOr<QueryResult> res = engine->Execute(ctx, sql);
  ASSERT_TRUE(res.HasValue()) << sql << " -> " << engine->LastError();
  res.Value().Drain();
  ASSERT_EQ(res.Value().GetStatus(), Status::kSuccess) << sql;
  ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess) << sql;
}

TEST(AnalyticsFactorizedJoinTest, PackShortString7AndRadixHelpers) {
  EXPECT_EQ(PackShortString7(""), std::optional<uint64_t>(0ULL));
  EXPECT_TRUE(PackShortString7("abcdefg").has_value());
  EXPECT_FALSE(PackShortString7("abcdefgh").has_value());
  const auto p_nul1 = PackShortString7(std::string_view("ab\0cd", 5));
  const auto p_nul2 = PackShortString7(std::string_view("ab\0cd\0", 6));
  ASSERT_TRUE(p_nul1 && p_nul2);
  EXPECT_NE(p_nul1.value_or(0), p_nul2.value_or(0));

  const auto p_a = PackShortString7("a");
  const auto p_ab = PackShortString7("ab");
  const auto p_a1 = PackShortString7("a\x01");
  ASSERT_TRUE(p_a && p_ab && p_a1);
  EXPECT_NE(p_a.value_or(0), p_ab.value_or(0));
  EXPECT_NE(p_a.value_or(0), p_a1.value_or(0));
  EXPECT_NE(p_ab.value_or(0), p_a1.value_or(0));
  EXPECT_NE(p_ab.value_or(0), p_nul1.value_or(0));

  EXPECT_LT(
      EncodeSignedInt64ForRadix(std::numeric_limits<int64_t>::min(), true),
      EncodeSignedInt64ForRadix(-1, true));
  EXPECT_LT(EncodeSignedInt64ForRadix(-1, true),
            EncodeSignedInt64ForRadix(0, true));
  EXPECT_LT(EncodeSignedInt64ForRadix(0, true),
            EncodeSignedInt64ForRadix(1, true));
  EXPECT_LT(
      EncodeSignedInt64ForRadix(1, true),
      EncodeSignedInt64ForRadix(std::numeric_limits<int64_t>::max(), true));
  EXPECT_GT(
      EncodeSignedInt64ForRadix(std::numeric_limits<int64_t>::min(), false),
      EncodeSignedInt64ForRadix(std::numeric_limits<int64_t>::max(), false));

  struct Item {
    bool is_null{false};
    int64_t val{0};
    int seq{0};
  };
  // Stability and extremes without NULLs.
  std::vector<Item> items = {
      {.val = std::numeric_limits<int64_t>::max(), .seq = 0},
      {.val = 5, .seq = 1},
      {.val = std::numeric_limits<int64_t>::min(), .seq = 2},
      {.val = 5, .seq = 3},
      {.val = -7, .seq = 4},
  };
  EXPECT_TRUE(TryStableRadixSortNullableInt64(
      items, /*ascending=*/true, /*nulls_first=*/true,
      [](const Item& it) noexcept { return it.is_null; },
      [](const Item& it) noexcept { return it.val; }));
  EXPECT_EQ(items[0].val, std::numeric_limits<int64_t>::min());
  EXPECT_EQ(items[1].val, -7);
  EXPECT_EQ(items[2].seq, 1);
  EXPECT_EQ(items[3].seq, 3);
  EXPECT_EQ(items[4].val, std::numeric_limits<int64_t>::max());

  // With NULLs: safe non-extreme values succeed, and NULL + INT64_MIN bails.
  std::vector<Item> null_items = {
      {.val = 10, .seq = 0},
      {.is_null = true, .seq = 1},
      {.val = -20, .seq = 2},
      {.is_null = true, .seq = 3},
  };
  EXPECT_TRUE(TryStableRadixSortNullableInt64(
      null_items, /*ascending=*/false, /*nulls_first=*/false,
      [](const Item& it) noexcept { return it.is_null; },
      [](const Item& it) noexcept { return it.val; }));
  EXPECT_FALSE(null_items[0].is_null);
  EXPECT_EQ(null_items[0].val, 10);
  EXPECT_EQ(null_items[1].val, -20);
  EXPECT_TRUE(null_items[2].is_null);
  EXPECT_EQ(null_items[2].seq, 1);
  EXPECT_TRUE(null_items[3].is_null);
  EXPECT_EQ(null_items[3].seq, 3);

  std::vector<Item> extreme_with_null = {
      {.is_null = true, .seq = 0},
      {.val = std::numeric_limits<int64_t>::min(), .seq = 1},
  };
  EXPECT_FALSE(TryStableRadixSortNullableInt64(
      extreme_with_null, true, true,
      [](const Item& it) noexcept { return it.is_null; },
      [](const Item& it) noexcept { return it.val; }));
}

TEST(AnalyticsFactorizedJoinTest, CollapsedGroupedJoinInnerLeftAndEdgeCases) {
  auto db = Database::Create("afj_cjoin_" + RandomString()).MoveValue();
  SqlEngine engine(*db);
  ExecDml(&engine, db.get(),
          "CREATE TABLE x (id INT64, j INT64, g INT64, v INT64)");
  ExecDml(&engine, db.get(),
          "CREATE TABLE y (id INT64, j INT64, w INT64, lbl VARCHAR)");
  ExecDml(&engine, db.get(), "CREATE TABLE empty_y (j INT64, w INT64)");

  {
    TransactionContext ctx = db->BeginContext();
    Table tx = db->GetTable(ctx, "x").MoveValue();
    Table ty = db->GetTable(ctx, "y").MoveValue();
    for (int64_t i = 0; i < 60; ++i) {
      Row rx({Value(i), (i % 10 == 0) ? Value() : Value((i % 7) + 1),
              Value(i % 4), Value((i + 1) * 3)});
      ASSERT_TRUE(tx.Insert(ctx.txn_, rx).HasValue());
    }
    for (int64_t j = 0; j < 28; ++j) {
      Row ry({Value(j), (j % 9 == 0) ? Value() : Value((j % 5) + 1),
              Value((j * 10) + 1),
              Value(std::string("t") + std::to_string(j % 3))});
      ASSERT_TRUE(ty.Insert(ctx.txn_, ry).HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  ExecDml(&engine, db.get(), "ANALYZE TABLE x");
  ExecDml(&engine, db.get(), "ANALYZE TABLE y");
  ExecDml(&engine, db.get(), "ANALYZE TABLE empty_y");

  const std::vector<std::string> queries = {
      // M:N INNER grouped join with aggs on both sides (H5 shape)
      "SELECT x.g, SUM(x.v) AS sx, SUM(y.w) AS sy, COUNT(*) AS n "
      "FROM x JOIN y ON x.j = y.j GROUP BY x.g ORDER BY sx DESC, x.g ASC",
      // LEFT JOIN with NULL + unreferenced build side / build SUM (C4 shape)
      "SELECT x.g, COUNT(*) AS n, COALESCE(SUM(x.v), 0) AS sx, "
      "COALESCE(SUM(y.w), 0) AS sy "
      "FROM x LEFT JOIN y ON x.j = y.j GROUP BY x.g ORDER BY x.g",
      // Empty build table
      "SELECT COUNT(*) AS n, COALESCE(SUM(x.v), 0) AS sx "
      "FROM x LEFT JOIN empty_y ON x.j = empty_y.j",
  };
  for (const auto& sql : queries) {
    std::string baseline;
    {
      ScopedEnvVar off("TINYLAMB_DISABLE_COLLAPSED_GROUPED_JOIN", "1");
      baseline = CanonicalizeRows(RunQuery(&engine, db.get(), sql), false);
    }
    const std::string opt =
        CanonicalizeRows(RunQuery(&engine, db.get(), sql), false);
    EXPECT_EQ(opt, baseline) << sql;
  }

  // Signed INT64 SUM overflow must propagate cleanly (fallback/error parity).
  ExecDml(&engine, db.get(), "CREATE TABLE ov_l (j INT64, g INT64, v INT64)");
  ExecDml(&engine, db.get(), "CREATE TABLE ov_r (j INT64, w INT64)");
  ExecDml(&engine, db.get(),
          "INSERT INTO ov_l VALUES (1, 1, 9223372036854775800), (1, 1, 100)");
  ExecDml(&engine, db.get(), "INSERT INTO ov_r VALUES (1, 1), (1, 2)");
  {
    TransactionContext ctx = db->BeginReadOnlyContext();
    auto r = engine.Execute(
        ctx,
        "SELECT ov_l.g, SUM(ov_l.v) FROM ov_l JOIN ov_r ON ov_l.j = "
        "ov_r.j GROUP BY ov_l.g");
    if (r.HasValue()) {
      r.Value().Collect();
      EXPECT_NE(r.Value().GetStatus(), Status::kSuccess);
    }
  }
}

TEST(AnalyticsFactorizedJoinTest, BinaryVarcharKeyElisionAndCollationSafety) {
  auto db = Database::Create("afj_vchar_" + RandomString()).MoveValue();
  SqlEngine engine(*db);
  ExecDml(&engine, db.get(), "CREATE TABLE a (id INT64, k VARCHAR, v INT64)");
  ExecDml(&engine, db.get(), "CREATE TABLE b (k VARCHAR, w INT64)");
  ExecDml(&engine, db.get(),
          "INSERT INTO a VALUES (1, 'Alpha', 10), (2, 'alpha', 20), "
          "(3, 'beta', 30), (4, NULL, 40)");
  ExecDml(&engine, db.get(),
          "INSERT INTO b VALUES ('Alpha', 5), ('Alpha', 6), ('BETA', 7)");
  ExecDml(&engine, db.get(), "ANALYZE TABLE a");
  ExecDml(&engine, db.get(), "ANALYZE TABLE b");

  const std::string bin_sql =
      "SELECT a.k, COUNT(*) AS n, SUM(a.v) AS sv, SUM(b.w) AS sw "
      "FROM a JOIN b ON a.k = b.k GROUP BY a.k ORDER BY a.k";
  std::string no_elide;
  {
    ScopedEnvVar off("TINYLAMB_DISABLE_VARCHAR_KEY_ELISION", "1");
    no_elide = CanonicalizeRows(RunQuery(&engine, db.get(), bin_sql), false);
  }
  EXPECT_EQ(CanonicalizeRows(RunQuery(&engine, db.get(), bin_sql), false),
            no_elide);

  // Collated VARCHAR join key must match case-insensitively (`beta` == `BETA`,
  // `Alpha` == `alpha`).
  const std::string ci_sql =
      "SELECT a.id, b.w FROM a JOIN b ON COLLATE(a.k, 'und:ci') = b.k "
      "ORDER BY a.id, b.w";
  const auto ci_rows = RunQuery(&engine, db.get(), ci_sql);
  // Matches: (1,5), (1,6), (2,5), (2,6), (3,7) -> 5 rows
  EXPECT_EQ(ci_rows.size(), 5U);
}

TEST(AnalyticsFactorizedJoinTest, DictBitmapNullOrLikePrefilter) {
  auto db = Database::Create("afj_bitmap_" + RandomString()).MoveValue();
  {
    TransactionContext ctx = db->BeginContext();
    Schema schema("posts", {Column("id", ValueType::kInt64),
                            Column("tags", ValueType::kVarChar),
                            Column("v", ValueType::kInt64)});
    ASSERT_TRUE(db->CreateTable(ctx, schema).HasValue());
    Table tbl = db->GetTable(ctx, "posts").MoveValue();
    for (int64_t i = 0; i < 6000; ++i) {
      Value tag = (i % 5 == 0) ? Value()
                  : (i % 7 == 0)
                      ? Value("postgres-sql-tag")
                      : Value(std::string("other-") + std::to_string(i % 20));
      ASSERT_TRUE(
          tbl.Insert(ctx.txn_, Row({Value(i), tag, Value(i % 13)})).HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "posts"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  SqlEngine engine(*db);
  const std::string sql =
      "SELECT COUNT(*) AS n, COALESCE(SUM(v), 0) AS s "
      "FROM posts WHERE (tags IS NULL OR tags LIKE '%sql%') AND id >= 100";
  const auto fast_rows = RunQuery(&engine, db.get(), sql);
  ASSERT_EQ(fast_rows.size(), 1U);
  int64_t expected_n = 0;
  int64_t expected_s = 0;
  for (int64_t i = 100; i < 6000; ++i) {
    if (i % 5 == 0 || i % 7 == 0) {
      ++expected_n;
      expected_s += (i % 13);
    }
  }
  EXPECT_EQ(fast_rows[0][0].value.int_value, expected_n);
  EXPECT_EQ(fast_rows[0][1].value.int_value, expected_s);
}

TEST(AnalyticsFactorizedJoinTest, FactorizedJoinTreeStarAndSelfJoinParity) {
  auto db = Database::Create("afj_tree_" + RandomString()).MoveValue();
  SqlEngine engine(*db);
  ExecDml(&engine, db.get(), "CREATE TABLE p (id INT64, f INT64, c VARCHAR)");
  ExecDml(&engine, db.get(), "CREATE TABLE k (p1 INT64, p2 INT64)");
  ExecDml(&engine, db.get(), "CREATE TABLE d1 (id INT64, cat VARCHAR)");
  ExecDml(&engine, db.get(), "CREATE TABLE d2 (id INT64, yr INT64)");
  ExecDml(&engine, db.get(),
          "CREATE TABLE fact (id INT64, d1_id INT64, d2_id INT64, val INT64)");
  {
    TransactionContext ctx = db->BeginContext();
    Table tp = db->GetTable(ctx, "p").MoveValue();
    Table tk = db->GetTable(ctx, "k").MoveValue();
    Table td1 = db->GetTable(ctx, "d1").MoveValue();
    Table td2 = db->GetTable(ctx, "d2").MoveValue();
    Table tf = db->GetTable(ctx, "fact").MoveValue();
    for (int64_t i = 1; i <= 16; ++i) {
      ASSERT_TRUE(tp.Insert(ctx.txn_, Row({Value(i), Value(i % 6),
                                           Value(std::string("c") +
                                                 std::to_string(i % 3))}))
                      .HasValue());
      ASSERT_TRUE(
          tk.Insert(ctx.txn_, Row({Value(i), Value((i % 16) + 1)})).HasValue());
      ASSERT_TRUE(
          td1.Insert(ctx.txn_, Row({Value(i), Value(std::string("cat") +
                                                    std::to_string(i % 4))}))
              .HasValue());
      ASSERT_TRUE(td2.Insert(ctx.txn_, Row({Value(i), Value(2010 + (i % 5))}))
                      .HasValue());
    }
    for (int64_t i = 1; i <= 120; ++i) {
      ASSERT_TRUE(
          tf.Insert(ctx.txn_, Row({Value(i), Value((i % 16) + 1),
                                   Value(((i * 3) % 16) + 1), Value(i % 11)}))
              .HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  for (const char* tbl : {"p", "k", "d1", "d2", "fact"}) {
    ExecDml(&engine, db.get(), std::string("ANALYZE TABLE ") + tbl);
  }

  // Acyclic chain + transitive eq + self-join + cyclic triangle fallback + star
  const std::vector<std::string> queries = {
      // Multi-hop tree (#9/#11/#12)
      "SELECT p1.c, COUNT(*) AS n FROM p p1 JOIN k ON p1.id = k.p1 "
      "JOIN p p2 ON k.p2 = p2.id WHERE p1.f >= 2 AND p2.f < 4 GROUP BY p1.c",
      // Self-join dual filter split (#13)
      "SELECT p1.c, COUNT(*) AS n FROM p p1 JOIN p p2 ON p1.id = p2.id "
      "WHERE p1.f >= 1 AND p2.f <= 4 GROUP BY p1.c",
      // Star join small dim direct agg (#14)
      "SELECT d1.cat, fact.d2_id, SUM(fact.val) AS s, COUNT(*) AS n "
      "FROM fact JOIN d1 ON fact.d1_id = d1.id JOIN d2 ON fact.d2_id = d2.id "
      "WHERE d2.yr >= 2012 GROUP BY d1.cat, fact.d2_id",
      // Cyclic 3-triangle (must fall back safely and produce exact answer)
      "SELECT a.c, COUNT(*) AS n FROM p a JOIN p b ON a.id = b.id "
      "JOIN p c ON b.id = c.id AND c.id = a.id GROUP BY a.c",
  };
  for (const auto& sql : queries) {
    std::string baseline;
    {
      ScopedEnvVar off_tree("TINYLAMB_DISABLE_FACTORIZED_JOIN_TREE", "1");
      ScopedEnvVar off_star("TINYLAMB_DISABLE_SMALL_DIM_DENSE_LOOKUP", "1");
      ScopedEnvVar off_self("TINYLAMB_DISABLE_SELF_JOIN_SPLIT", "1");
      baseline = CanonicalizeRows(RunQuery(&engine, db.get(), sql), true);
    }
    EXPECT_EQ(CanonicalizeRows(RunQuery(&engine, db.get(), sql), true),
              baseline)
        << sql;
  }

  // Non-unique dimension join key fallback (#14 safety)
  ExecDml(&engine, db.get(), "CREATE TABLE dup_d (id INT64, cat VARCHAR)");
  ExecDml(&engine, db.get(),
          "INSERT INTO dup_d VALUES (1, 'a'), (1, 'b'), (2, 'c')");
  std::string star_no_opt;
  {
    ScopedEnvVar off_star("TINYLAMB_DISABLE_SMALL_DIM_DENSE_LOOKUP", "1");
    star_no_opt = CanonicalizeRows(
        RunQuery(&engine, db.get(),
                 "SELECT dup_d.cat, COUNT(*) FROM fact JOIN dup_d ON "
                 "fact.d1_id = dup_d.id GROUP BY dup_d.cat"),
        true);
  }
  EXPECT_EQ(CanonicalizeRows(
                RunQuery(&engine, db.get(),
                         "SELECT dup_d.cat, COUNT(*) FROM fact JOIN dup_d ON "
                         "fact.d1_id = dup_d.id GROUP BY dup_d.cat"),
                true),
            star_no_opt);
}

TEST(AnalyticsFactorizedJoinTest, TwoKeyBoundedLocalDictAndRadixSortParity) {
  auto db = Database::Create("afj_tlcs_" + RandomString()).MoveValue();
  {
    TransactionContext ctx = db->BeginContext();
    Schema schema(
        "big_t",
        {Column("id1", ValueType::kInt64), Column("id2", ValueType::kInt64),
         Column("id3", ValueType::kInt64), Column("url", ValueType::kVarChar),
         Column("v", ValueType::kInt64)});
    ASSERT_TRUE(db->CreateTable(ctx, schema).HasValue());
    Table tbl = db->GetTable(ctx, "big_t").MoveValue();
    for (int64_t i = 0; i < 3000; ++i) {
      Value id1 = (i % 111 == 0) ? Value() : Value(i % 25);
      Value id2 = (i % 137 == 0) ? Value() : Value((i * 3) % 60);
      std::string url = (i % 3 == 0)
                            ? ("s" + std::to_string(i % 40))
                            : ("long-url-prefix-" + std::to_string(i % 1400));
      Value uval = (i % 251 == 0) ? Value() : Value(std::move(url));
      ASSERT_TRUE(tbl.Insert(ctx.txn_, Row({id1, id2, Value(i % 10), uval,
                                            Value((i % 50) - 5)}))
                      .HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "big_t"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  SqlEngine engine(*db);
  ExecDml(&engine, db.get(), "ANALYZE TABLE big_t");

  const std::vector<std::string> queries = {
      // #15 + #17: 2-key bounded int array + ORDER BY COUNT(*) DESC
      "SELECT id1, id2, COUNT(*) AS n FROM big_t GROUP BY id1, id2 "
      "ORDER BY n DESC, id1 ASC, id2 ASC",
      // #15: filtered AVG
      "SELECT id1, id2, AVG(v) AS a FROM big_t WHERE id3 < 4 "
      "GROUP BY id1, id2 ORDER BY id1, id2",
      // #16 + #8 + #2 + #17: single VarChar local-dict 2-stage + ORDER BY SUM
      // DESC (>= 1300 groups -> exercises radix sort!)
      "SELECT url, SUM(v) AS total, COUNT(*) AS c FROM big_t "
      "GROUP BY url ORDER BY total DESC, url ASC",
  };
  for (const auto& sql : queries) {
    std::string baseline;
    {
      ScopedEnvVar off2("TINYLAMB_DISABLE_TWO_KEY_DIRECT_AGG", "1");
      ScopedEnvVar off_ld("TINYLAMB_DISABLE_LOCAL_DICT_TWO_STAGE_AGG", "1");
      ScopedEnvVar off_rx("TINYLAMB_DISABLE_RADIX_SORT", "1");
      baseline = CanonicalizeRows(RunQuery(&engine, db.get(), sql), false);
    }
    EXPECT_EQ(CanonicalizeRows(RunQuery(&engine, db.get(), sql), false),
              baseline)
        << sql;
  }

  // Fallback when 2-key product exceeds 262144 slots.
  ExecDml(&engine, db.get(),
          "CREATE TABLE wide2 (k0 INT64, k1 INT64, v INT64)");
  ExecDml(&engine, db.get(),
          "INSERT INTO wide2 VALUES (0, 0, 1), (1000, 1000, 2)");
  ExecDml(&engine, db.get(), "ANALYZE TABLE wide2");
  const auto wrows = RunQuery(
      &engine, db.get(),
      "SELECT k0, k1, SUM(v) FROM wide2 GROUP BY k0, k1 ORDER BY k0, k1");
  ASSERT_EQ(wrows.size(), 2U);
}

TEST(AnalyticsFactorizedJoinTest, Wave8UrlGroupingAndSparseOuterCorrection) {
  // lean/TinylambOpt/ResearchProofs/AnalyticsWorkloads/DictionaryJoinsAndLateLabels.lean.UrlGrouping_sound:
  // canonical "page-<N>" bijection.
  uint32_t id = 0;
  EXPECT_TRUE(relational_detail::TryParseCanonicalPageUrl("page-0", &id));
  EXPECT_EQ(id, 0U);
  EXPECT_TRUE(relational_detail::TryParseCanonicalPageUrl("page-42", &id));
  EXPECT_EQ(id, 42U);
  EXPECT_TRUE(relational_detail::TryParseCanonicalPageUrl("page-262144", &id));
  EXPECT_EQ(id, 262144U);
  EXPECT_FALSE(relational_detail::TryParseCanonicalPageUrl("page-01", &id));
  EXPECT_FALSE(relational_detail::TryParseCanonicalPageUrl("page-262145", &id));
  EXPECT_FALSE(relational_detail::TryParseCanonicalPageUrl("page-", &id));
  EXPECT_FALSE(relational_detail::TryParseCanonicalPageUrl("page-1a", &id));
  EXPECT_FALSE(relational_detail::TryParseCanonicalPageUrl("http://x", &id));

  // End-to-end parity for B2-like VarChar equi-join (UrlGrouping) and C4-like
  // LEFT JOIN with unique and duplicate right keys (SparseOuterCorrection).
  auto db = Database::Create("afj_wave8_" + RandomString()).MoveValue();
  SqlEngine engine(*db);
  ExecDml(&engine, db.get(),
          "CREATE TABLE visits (url VARCHAR, cc VARCHAR, rev INT64)");
  ExecDml(&engine, db.get(), "CREATE TABLE ranks (url VARCHAR, pr INT64)");
  ExecDml(&engine, db.get(), "CREATE TABLE posts (tag VARCHAR, views INT64)");
  ExecDml(&engine, db.get(), "CREATE TABLE tags_uniq (name VARCHAR)");
  ExecDml(&engine, db.get(), "CREATE TABLE tags_dup (name VARCHAR)");
  {
    TransactionContext ctx = db->BeginContext();
    Table tv = db->GetTable(ctx, "visits").MoveValue();
    Table tr = db->GetTable(ctx, "ranks").MoveValue();
    Table tp = db->GetTable(ctx, "posts").MoveValue();
    Table tu = db->GetTable(ctx, "tags_uniq").MoveValue();
    Table td = db->GetTable(ctx, "tags_dup").MoveValue();
    for (int64_t i = 0; i < 40; ++i) {
      std::string url = (i % 7 == 0) ? ("custom-" + std::to_string(i))
                                     : ("page-" + std::to_string(i % 20));
      ASSERT_TRUE(tv.Insert(ctx.txn_, Row({Value(std::move(url)),
                                           Value(std::string("C") +
                                                 std::to_string(i % 3)),
                                           Value(i * 10)}))
                      .HasValue());
    }
    for (int64_t i = 0; i < 20; ++i) {
      ASSERT_TRUE(tr.Insert(ctx.txn_, Row({Value("page-" + std::to_string(i)),
                                           Value(i * 5)}))
                      .HasValue());
    }
    ASSERT_TRUE(
        tr.Insert(ctx.txn_, Row({Value("custom-7"), Value(int64_t{99})}))
            .HasValue());
    ASSERT_TRUE(
        tp.Insert(ctx.txn_, Row({Value(), Value(int64_t{10})})).HasValue());
    ASSERT_TRUE(tp.Insert(ctx.txn_, Row({Value("sql"), Value(int64_t{20})}))
                    .HasValue());
    ASSERT_TRUE(tp.Insert(ctx.txn_, Row({Value("miss"), Value(int64_t{30})}))
                    .HasValue());
    ASSERT_TRUE(tu.Insert(ctx.txn_, Row({Value("sql")})).HasValue());
    ASSERT_TRUE(tu.Insert(ctx.txn_, Row({Value("other")})).HasValue());
    ASSERT_TRUE(td.Insert(ctx.txn_, Row({Value("sql")})).HasValue());
    ASSERT_TRUE(td.Insert(ctx.txn_, Row({Value("sql")})).HasValue());
    ASSERT_TRUE(td.Insert(ctx.txn_, Row({Value()})).HasValue());
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  const std::vector<std::string> queries = {
      "SELECT v.cc, COUNT(*) AS n, SUM(v.rev) AS r "
      "FROM visits v JOIN ranks k ON v.url = k.url "
      "WHERE k.pr > 40 GROUP BY v.cc ORDER BY r DESC, v.cc ASC",
      "SELECT COUNT(*) AS n, COALESCE(SUM(p.views), 0) AS s "
      "FROM posts p LEFT JOIN tags_uniq t ON p.tag = t.name",
      "SELECT COUNT(*) AS n, COALESCE(SUM(p.views), 0) AS s "
      "FROM posts p LEFT JOIN tags_dup t ON p.tag = t.name",
  };
  for (const auto& sql : queries) {
    std::string baseline;
    {
      ScopedEnvVar off_col("TINYLAMB_DISABLE_COLLAPSED_RIGHT_JOIN", "1");
      baseline = CanonicalizeRows(RunQuery(&engine, db.get(), sql), false);
    }
    EXPECT_EQ(CanonicalizeRows(RunQuery(&engine, db.get(), sql), false),
              baseline)
        << sql;
  }
}

TEST(AnalyticsFactorizedJoinTest,
     PastWaveAndWave8HelpersAndTwoKeyEpochReuseParity) {
  // OffsetHeavyReverseTopNAndQuotientBucket.lean
  std::vector<int> vals = {9, 1, 7, 3, 5, 2, 8, 4, 6, 0, 5, 3};
  auto less_fn = [&](size_t a, size_t b) {
    return vals[a] != vals[b] ? vals[a] < vals[b] : a < b;
  };
  std::vector<size_t> full(vals.size());
  for (size_t i = 0; i < vals.size(); ++i) {
    full[i] = i;
  }
  std::sort(full.begin(), full.end(), less_fn);
  for (size_t offset :
       {size_t{0}, size_t{2}, size_t{7}, size_t{10}, size_t{12}}) {
    for (size_t limit : {size_t{0}, size_t{1}, size_t{3}, size_t{5}}) {
      auto got = relational_detail::SelectOffsetHeavyReverseTopNIndices(
          vals.size(), offset, limit, less_fn);
      std::vector<int> got_v, exp_v;
      for (size_t idx : got) {
        got_v.push_back(vals[idx]);
      }
      for (size_t i = offset; i < std::min(vals.size(), offset + limit); ++i) {
        exp_v.push_back(vals[full[i]]);
      }
      EXPECT_EQ(got_v, exp_v) << "offset=" << offset << " limit=" << limit;
    }
  }

  // LazyDictionary.lean
  relational_detail::LazyDictionaryPredicateCache cache(100);
  auto pred = [](size_t code) { return (code % 3) == 0; };
  EXPECT_TRUE(cache.Test(6, pred));
  EXPECT_TRUE(cache.Test(6, pred));
  EXPECT_FALSE(cache.Test(7, pred));
  EXPECT_FALSE(cache.Test(7, pred));
  EXPECT_EQ(cache.EvalCount(), 2U);

  // lean/TinylambOpt/ResearchProofs/AnalyticsWorkloads/HotKeyAndEpochAggregates.lean.EpochAverage_sound
  // + TwoLevelPartitionedAggMerge + EmptyStringKeyPartitionSplit +
  // lean/TinylambOpt/ResearchProofs/AnalyticsWorkloads/MovieJoinFiltersAndFanout.lean.FixedGender_sound
  auto db = Database::Create("afj_ab_" + RandomString()).MoveValue();
  SqlEngine engine(*db);
  ExecDml(
      &engine, db.get(),
      "CREATE TABLE grid (k0 INT64, k1 INT64, k2 INT64, v INT64, s VARCHAR)");
  ExecDml(&engine, db.get(), "CREATE TABLE dim_g (id INT64, g VARCHAR)");
  ExecDml(&engine, db.get(), "CREATE TABLE fact_g (did INT64, yr INT64)");
  {
    TransactionContext ctx = db->BeginContext();
    Table tg = db->GetTable(ctx, "grid").MoveValue();
    Table td = db->GetTable(ctx, "dim_g").MoveValue();
    Table tf = db->GetTable(ctx, "fact_g").MoveValue();
    for (int64_t i = 0; i < 6000; ++i) {
      Value v = (i % 9 == 0) ? Value() : Value((i * 7) % 100);
      Value s =
          (i % 3 == 0)
              ? Value(std::string(""))
              : (i % 11 == 0 ? Value() : Value("k-" + std::to_string(i % 25)));
      ASSERT_TRUE(tg.Insert(ctx.txn_, Row({Value(i % 40), Value((i / 40) % 30),
                                           Value(i % 8), v, s}))
                      .HasValue());
    }
    for (int64_t i = 1; i <= 50; ++i) {
      ASSERT_TRUE(
          td.Insert(ctx.txn_, Row({Value(i), Value(i % 2 == 0 ? "F" : "M")}))
              .HasValue());
    }
    for (int64_t i = 0; i < 5000; ++i) {
      ASSERT_TRUE(tf.Insert(ctx.txn_,
                            Row({Value((i % 50) + 1), Value(1980 + (i % 30))}))
                      .HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(ctx, "grid"), Status::kSuccess);
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  const std::vector<std::string> queries = {
      "SELECT k0, k1, COUNT(*) AS n FROM grid GROUP BY k0, k1 ORDER BY k0, k1",
      "SELECT k0, k1, AVG(v) AS a FROM grid WHERE k2 < 4 GROUP BY k0, k1 ORDER "
      "BY k0, k1",
      "SELECT k0, k1, AVG(v) AS a FROM grid WHERE k2 < 2 GROUP BY k0, k1 ORDER "
      "BY k0, k1",
      "SELECT s, COUNT(*) AS n, SUM(v) AS sv FROM grid GROUP BY s ORDER BY s",
      "SELECT d.g, COUNT(*) AS n, AVG(f.yr) AS ay "
      "FROM fact_g f JOIN dim_g d ON f.did = d.id WHERE d.g = 'F' GROUP BY d.g",
  };
  for (const auto& sql : queries) {
    std::string baseline;
    {
      ScopedEnvVar off_ep("TINYLAMB_DISABLE_EPOCH_TWO_KEY_AGG", "1");
      ScopedEnvVar off_sd("TINYLAMB_DISABLE_SMALL_DIM_DENSE_LOOKUP", "1");
      baseline = CanonicalizeRows(RunQuery(&engine, db.get(), sql), false);
    }
    // Run twice to exercise epoch reuse across consecutive queries!
    EXPECT_EQ(CanonicalizeRows(RunQuery(&engine, db.get(), sql), false),
              baseline)
        << "run1: " << sql;
    EXPECT_EQ(CanonicalizeRows(RunQuery(&engine, db.get(), sql), false),
              baseline)
        << "run2: " << sql;
  }
}

TEST(AnalyticsFactorizedJoinTest, Wave9ProvedKernelsAndSelfJoinBridgeParity) {
  // lean/TinylambOpt/ResearchProofs/QueryRewrites/SurrogateKeyRollupsAndSetCounts.lean
  // (DisjointRangeBucketHistogram_sound)
  EXPECT_EQ(relational_detail::ClassifyDisjointRangeBucket5(-5), 0U);
  EXPECT_EQ(relational_detail::ClassifyDisjointRangeBucket5(30), 0U);
  EXPECT_EQ(relational_detail::ClassifyDisjointRangeBucket5(31), 1U);
  EXPECT_EQ(relational_detail::ClassifyDisjointRangeBucket5(60), 1U);
  EXPECT_EQ(relational_detail::ClassifyDisjointRangeBucket5(61), 2U);
  EXPECT_EQ(relational_detail::ClassifyDisjointRangeBucket5(90), 2U);
  EXPECT_EQ(relational_detail::ClassifyDisjointRangeBucket5(91), 3U);
  EXPECT_EQ(relational_detail::ClassifyDisjointRangeBucket5(120), 3U);
  EXPECT_EQ(relational_detail::ClassifyDisjointRangeBucket5(121), 4U);

  // lean/TinylambOpt/ResearchProofs/QueryRewrites/SurrogateKeyRollupsAndSetCounts.lean
  // (LeftJoinNullFlagSplitCount_sound)
  uint64_t no_m = 0, m = 0, tot = 0;
  relational_detail::FoldLeftJoinNullFlagCounts(0, &no_m, &m, &tot);
  relational_detail::FoldLeftJoinNullFlagCounts(3, &no_m, &m, &tot);
  relational_detail::FoldLeftJoinNullFlagCounts(1, &no_m, &m, &tot);
  EXPECT_EQ(no_m, 1U);
  EXPECT_EQ(m, 4U);
  EXPECT_EQ(tot, 5U);

  // lean/TinylambOpt/ResearchProofs/QueryRewrites/BlockPruningAndBoundedGrouping.lean
  // (TopKCountPruneByRemainingRows_sound)
  EXPECT_EQ(relational_detail::ComputeTopKRemainingPruneThreshold(
                {10, 50, 20, 40, 30}, 3),
            30U);
  EXPECT_EQ(relational_detail::ComputeTopKRemainingPruneThreshold({10, 20}, 3),
            0U);

  // lean/TinylambOpt/ResearchProofs/QueryRewrites/JoinMultiplicityAndZoneFilters.lean
  // (SelfJoinBridgeBipartiteWeights_sound — C3)
  auto db = Database::Create("afj_wave9_" + RandomString()).MoveValue();
  SqlEngine engine(*db);
  ExecDml(&engine, db.get(),
          "CREATE TABLE posts (id INT64, post_type INT64, score INT64)");
  ExecDml(&engine, db.get(),
          "CREATE TABLE post_links (post_id INT64, related_post_id INT64, "
          "link_type INT64)");
  {
    TransactionContext ctx = db->BeginContext();
    Table tp = db->GetTable(ctx, "posts").MoveValue();
    Table tl = db->GetTable(ctx, "post_links").MoveValue();
    for (int64_t i = 1; i <= 30; ++i) {
      Value sc = (i % 6 == 0) ? Value() : Value(i * 3);
      ASSERT_TRUE(
          tp.Insert(ctx.txn_, Row({Value(i), Value(i % 2), sc})).HasValue());
    }
    for (int64_t i = 1; i <= 60; ++i) {
      Value lt = (i % 15 == 0) ? Value() : Value(i % 4);
      ASSERT_TRUE(tl.Insert(ctx.txn_, Row({Value((i % 30) + 1),
                                           Value(((i * 7) % 30) + 1), lt}))
                      .HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }
  for (const std::string& c3_sql : {
           std::string("SELECT pl.link_type, COUNT(*) AS n, "
                       "COALESCE(SUM(p1.score), 0) AS s "
                       "FROM posts p1 JOIN post_links pl ON p1.id = pl.post_id "
                       "JOIN posts p2 ON pl.related_post_id = p2.id "
                       "WHERE p2.post_type = 1 GROUP BY pl.link_type "
                       "ORDER BY pl.link_type"),
           std::string(
               "SELECT pl.link_type, COUNT(*) AS n, AVG(p1.score) AS avg_s "
               "FROM posts p1 JOIN post_links pl ON p1.id = pl.post_id "
               "JOIN posts p2 ON pl.related_post_id = p2.id "
               "WHERE p2.post_type = 1 GROUP BY pl.link_type "
               "ORDER BY pl.link_type"),
       }) {
    std::string baseline;
    {
      ScopedEnvVar off_ft("TINYLAMB_DISABLE_FACTORIZED_JOIN_TREE", "1");
      baseline = CanonicalizeRows(RunQuery(&engine, db.get(), c3_sql), false);
    }
    EXPECT_EQ(CanonicalizeRows(RunQuery(&engine, db.get(), c3_sql), false),
              baseline)
        << c3_sql;
  }

  // lean/TinylambOpt/ResearchProofs/QueryRewrites/PivotedAggregatesAndSemijoinFilters.lean
  // (YearPairPivotSelfJoinElim_sound — Q75 derived UNION DISTINCT CTE year-pair
  // self-join pivot).
  {
    auto q75_db = Database::Create("afj_w9_q75_" + RandomString()).MoveValue();
    SqlEngine q75_eng(*q75_db);
    ExecDml(&q75_eng, q75_db.get(),
            "CREATE TABLE s1 (yr INT64, k INT64, cnt INT64, amt DOUBLE)");
    ExecDml(&q75_eng, q75_db.get(),
            "CREATE TABLE s2 (yr INT64, k INT64, cnt INT64, amt DOUBLE)");
    {
      TransactionContext ctx = q75_db->BeginContext();
      Table tbl1 = q75_db->GetTable(ctx, "s1").MoveValue();
      Table tbl2 = q75_db->GetTable(ctx, "s2").MoveValue();
      for (int64_t yr : {2000, 2001, 2002}) {
        for (int64_t k = 1; k <= 8; ++k) {
          ASSERT_TRUE(
              tbl1.Insert(ctx.txn_, Row({Value(yr), Value(k), Value(k * 2),
                                         Value(static_cast<double>(k) * 10.0)}))
                  .HasValue());
          ASSERT_TRUE(
              tbl2.Insert(ctx.txn_, Row({Value(yr), Value(k), Value(k + 1),
                                         Value(static_cast<double>(k) * 5.0)}))
                  .HasValue());
        }
      }
      ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
    }
    const std::string q75_sql =
        "WITH all_sales AS ("
        "  SELECT yr, k, SUM(cnt) AS sales_cnt, SUM(amt) AS sales_amt"
        "  FROM ("
        "    SELECT yr, k, cnt, amt FROM s1"
        "    UNION DISTINCT"
        "    SELECT yr, k, cnt, amt FROM s2"
        "  ) AS sales_detail"
        "  GROUP BY yr, k"
        ") "
        "SELECT prev_yr.yr AS prev_year, curr_yr.yr AS year_, curr_yr.k, "
        "  prev_yr.sales_cnt AS prev_cnt, curr_yr.sales_cnt AS curr_cnt "
        "FROM all_sales AS curr_yr, all_sales AS prev_yr "
        "WHERE curr_yr.k = prev_yr.k AND curr_yr.yr = 2002 "
        "  AND prev_yr.yr = 2002 - 1 "
        "ORDER BY curr_yr.k";
    const auto rows = RunQuery(&q75_eng, q75_db.get(), q75_sql);
    ASSERT_EQ(rows.size(), 8U);
    for (size_t i = 0; i < 8; ++i) {
      const auto k = static_cast<int64_t>(i + 1);
      // For k=1, s1=(yr,1,2,10.0) and s2=(yr,1,2,5.0) are distinct -> sum=4.
      const int64_t expected_cnt = (k * 2) + (k + 1);
      EXPECT_EQ(rows[i][0].value.int_value, 2001);
      EXPECT_EQ(rows[i][1].value.int_value, 2002);
      EXPECT_EQ(rows[i][2].value.int_value, k);
      EXPECT_EQ(rows[i][3].value.int_value, expected_cnt);
      EXPECT_EQ(rows[i][4].value.int_value, expected_cnt);
    }
  }

  // lean/TinylambOpt/ResearchProofs/QueryRewrites/SurrogateKeyRollupsAndSetCounts.lean
  // (LeftJoinNullFlagSplitCount_sound — Q72 complementary IS NULL / NOT IS NULL
  // split counts).
  {
    auto q72_db = Database::Create("afj_w9_q72_" + RandomString()).MoveValue();
    SqlEngine q72_eng(*q72_db);
    ExecDml(&q72_eng, q72_db.get(),
            "CREATE TABLE fact (g INT64, promo_sk INT64)");
    ExecDml(&q72_eng, q72_db.get(), "CREATE TABLE promo (p_sk INT64)");
    {
      TransactionContext ctx = q72_db->BeginContext();
      Table tf = q72_db->GetTable(ctx, "fact").MoveValue();
      Table tp = q72_db->GetTable(ctx, "promo").MoveValue();
      for (int64_t p = 1; p <= 5; ++p) {
        ASSERT_TRUE(tp.Insert(ctx.txn_, Row({Value(p)})).HasValue());
      }
      for (int64_t i = 1; i <= 40; ++i) {
        const Value psk = (i % 3 == 0) ? Value((i % 5) + 1) : Value(100 + i);
        ASSERT_TRUE(tf.Insert(ctx.txn_, Row({Value(i % 4), psk})).HasValue());
      }
      ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
    }
    const std::string q72_sql =
        "SELECT f.g, "
        "  SUM(CASE WHEN p.p_sk IS NULL THEN 1 ELSE 0 END) AS no_promo, "
        "  SUM(CASE WHEN NOT p.p_sk IS NULL THEN 1 ELSE 0 END) AS promo, "
        "  COUNT(*) AS total_cnt "
        "FROM fact f LEFT OUTER JOIN promo p ON f.promo_sk = p.p_sk "
        "GROUP BY f.g ORDER BY f.g";
    const auto rows = RunQuery(&q72_eng, q72_db.get(), q72_sql);
    ASSERT_EQ(rows.size(), 4U);
    int64_t grand_total = 0;
    for (const Row& row : rows) {
      const int64_t no_p = row[1].value.int_value;
      const int64_t p = row[2].value.int_value;
      const int64_t row_tot = row[3].value.int_value;
      EXPECT_EQ(no_p + p, row_tot);
      grand_total += row_tot;
    }
    EXPECT_EQ(grand_total, 40);
  }
}

}  // namespace
}  // namespace tinylamb
