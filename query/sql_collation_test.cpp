/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// End-to-end ORDER BY contract for collation-aware comparison: the same
// `ORDER BY x COLLATE 'und:ci'` must produce the same rows whichever route
// answers it (base-table scan, UNNEST, grouped, windowed, LIMIT / OFFSET /
// WITH TIES), and must agree with what the `=` predicate calls a peer. The
// comparison rules themselves are pinned in type/collation_test.cpp, the
// per-operator wiring in executor/sort_collation_test.cpp and the optimizer
// property in plan/collation_property_test.cpp; this file covers the wiring
// from SQL text down to the chosen executor, including the hidden `$orderN`
// projection column a non-column ORDER BY key goes through.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

std::vector<Row> RunSql(TransactionContext& context, Database& database,
                        std::string_view sql) {
  SqlEngine engine(database);
  StatusOr<Executor> prepared = engine.Prepare(context, sql);
  EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << sql << "\n"
                                                    << engine.LastError();
  std::vector<Row> rows;
  if (!prepared.HasValue()) {
    return rows;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
    row = Row();
  }
  return rows;
}

std::vector<std::string> TextColumn(const std::vector<Row>& rows,
                                    size_t index) {
  std::vector<std::string> values;
  values.reserve(rows.size());
  for (const Row& row : rows) {
    if (row[index].IsNull()) {
      values.emplace_back("<null>");
    } else {
      values.emplace_back(row[index].value.varchar_value);
    }
  }
  return values;
}

std::vector<int64_t> IntColumn(const std::vector<Row>& rows, size_t index) {
  std::vector<int64_t> values;
  values.reserve(rows.size());
  for (const Row& row : rows) {
    values.push_back(row[index].value.int_value);
  }
  return values;
}

class SqlCollationTest : public ::testing::Test {
 public:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ =
        Database::Create("sql_collation_test-" + RandomString()).MoveValue();
    TransactionContext setup = database_->BeginContext();
    RunSql(setup, *database_,
           "CREATE TABLE words (id INT64, s STRING(MAX), g INT64);");
    RunSql(setup, *database_,
           "INSERT INTO words (id, s, g) VALUES "
           "(1, 'Banana', 1), (2, 'apple', 1), (3, 'Cherry', 1), "
           "(4, 'APPLE', 1), (5, 'date', 1), (6, 'Egg', 1), (7, NULL, 1);");
    ASSERT_SUCCESS(setup.PreCommit());
  }

  void TearDown() override {
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
  }

  std::vector<std::string> Order(std::string_view sql) {
    TransactionContext ctx = Begin();
    const std::vector<Row> rows =
        RunSql(ctx, database(), std::string(sql) + ";");
    return TextColumn(rows, 0);
  }

  Database& database() { return *database_; }
  TransactionContext Begin() { return database_->BeginContext(); }

 private:
  std::unique_ptr<Database> database_;
};

}  // namespace

TEST_F(SqlCollationTest, CollatedOrderFoldsAndBinaryOrderDoesNot) {
  // 'APPLE' and 'apple' are peers under und:ci, so the fold puts 'apple'
  // first (its input row) while the binary order keeps 'APPLE' first.
  EXPECT_EQ(Order("SELECT s FROM words ORDER BY COLLATE(s, 'und:ci')"),
            (std::vector<std::string>{"<null>", "apple", "APPLE", "Banana",
                                      "Cherry", "date", "Egg"}));
  EXPECT_EQ(Order("SELECT s FROM words ORDER BY s"),
            Order("SELECT s FROM words ORDER BY COLLATE(s, 'binary')"));
  EXPECT_EQ(Order("SELECT s FROM words ORDER BY COLLATE(s, 'binary')"),
            (std::vector<std::string>{"<null>", "APPLE", "Banana", "Cherry",
                                      "Egg", "apple", "date"}));
}

TEST_F(SqlCollationTest, CollatedOrderDescendingAndNullPlacement) {
  EXPECT_EQ(Order("SELECT s FROM words ORDER BY COLLATE(s, 'und:ci') DESC"),
            (std::vector<std::string>{"Egg", "date", "Cherry", "Banana",
                                      "apple", "APPLE", "<null>"}));
  // NULLS FIRST / LAST are honoured on the collated key.
  EXPECT_EQ(Order("SELECT s FROM words ORDER BY COLLATE(s, 'und:ci') NULLS "
                  "LAST"),
            (std::vector<std::string>{"apple", "APPLE", "Banana", "Cherry",
                                      "date", "Egg", "<null>"}));
  EXPECT_EQ(Order("SELECT s FROM words ORDER BY COLLATE(s, 'und:ci') DESC "
                  "NULLS FIRST"),
            (std::vector<std::string>{"<null>", "Egg", "date", "Cherry",
                                      "Banana", "apple", "APPLE"}));
}

TEST_F(SqlCollationTest, CollatedOrderAgreesAcrossEveryRoute) {
  // The same ORDER BY over a base table, an UNNEST and a subquery must agree
  // row for row: the route may change, the collation contract may not.
  const std::vector<std::string> expected =
      Order("SELECT s FROM words ORDER BY COLLATE(s, 'und:ci')");
  EXPECT_EQ(Order("SELECT s FROM (SELECT s FROM words) ORDER BY COLLATE(s, "
                  "'und:ci')"),
            expected);
  EXPECT_EQ(Order("SELECT s FROM UNNEST(['Banana', 'apple', 'Cherry', "
                  "'APPLE', 'date', 'Egg', NULL]) AS s ORDER BY COLLATE(s, "
                  "'und:ci')"),
            expected);
  // An aliased / ordinal key is the same key: the projection path must not
  // drop the collator on the way to the sort.
  EXPECT_EQ(Order("SELECT s AS w FROM words ORDER BY COLLATE(w, 'und:ci')"),
            expected);
  EXPECT_EQ(Order("SELECT s FROM words ORDER BY 1 COLLATE 'und:ci'"), expected);
}

TEST_F(SqlCollationTest, CollatedLimitOffsetAndWithTies) {
  // LIMIT keeps the collated top rows...
  EXPECT_EQ(Order("SELECT s FROM words ORDER BY COLLATE(s, 'und:ci') LIMIT 2"),
            (std::vector<std::string>{"<null>", "apple"}));
  // ... OFFSET skips them...
  EXPECT_EQ(Order("SELECT s FROM words ORDER BY COLLATE(s, 'und:ci') "
                  "LIMIT 2 OFFSET 1"),
            (std::vector<std::string>{"apple", "APPLE"}));
  // ... and WITH TIES keeps the whole folded peer group at the boundary.
  EXPECT_EQ(Order("SELECT s FROM words ORDER BY COLLATE(s, 'und:ci') "
                  "LIMIT 2 WITH TIES"),
            (std::vector<std::string>{"<null>", "apple", "APPLE"}));
  // A binary key does not group them.
  EXPECT_EQ(Order("SELECT s FROM words ORDER BY COLLATE(s, 'binary') "
                  "LIMIT 2 WITH TIES"),
            (std::vector<std::string>{"<null>", "APPLE"}));
}

TEST_F(SqlCollationTest, CollatedGroupByAndWindowAgreeWithTheOrderBy) {
  // GROUP BY the collated key collapses 'APPLE' into 'apple' and emits in the
  // same order the bare ORDER BY would.
  TransactionContext ctx = Begin();
  const std::vector<Row> grouped = RunSql(
      ctx, database(),
      "SELECT s, COUNT(*) AS n FROM words GROUP BY g, COLLATE(s, 'und:ci') "
      "ORDER BY COLLATE(s, 'und:ci')");
  EXPECT_EQ(TextColumn(grouped, 0),
            (std::vector<std::string>{"<null>", "apple", "Banana", "Cherry",
                                      "date", "Egg"}));
  // The peer group really is one group of two rows.
  EXPECT_EQ(IntColumn(grouped, 1), (std::vector<int64_t>{1, 2, 1, 1, 1, 1}));
  // RANK over the collated window order agrees: the two case variants share
  // rank 2 and the next rank jumps to 4.
  const std::vector<Row> ranked = RunSql(
      ctx, database(),
      "SELECT id, RANK() OVER (ORDER BY COLLATE(s, 'und:ci')) AS r FROM words "
      "ORDER BY id");
  EXPECT_EQ(IntColumn(ranked, 1), (std::vector<int64_t>{4, 2, 5, 2, 6, 7, 1}));
}

TEST_F(SqlCollationTest, CollatedOrderAgreesWithTheEqualityPredicate) {
  // The fold is one rule: whatever `=` calls a peer, ORDER BY groups.
  TransactionContext ctx = Begin();
  const std::vector<Row> equal =
      RunSql(ctx, database(),
             "SELECT s FROM words WHERE COLLATE(s, 'und:ci') = 'APPLE'");
  // The predicate folds too, so both spellings match.
  EXPECT_EQ(TextColumn(equal, 0), (std::vector<std::string>{"apple", "APPLE"}));
  // DISTINCT over the collated value folds the same way.
  const std::vector<Row> distinct = RunSql(
      ctx, database(),
      "SELECT DISTINCT s FROM (SELECT COLLATE(s, 'und:ci') AS s FROM words) "
      "ORDER BY s");
  EXPECT_EQ(TextColumn(distinct, 0),
            (std::vector<std::string>{"<null>", "apple", "banana", "cherry",
                                      "date", "egg"}));
}

TEST_F(SqlCollationTest, NonAsciiAndIntervalTextKeepTheirByteOrder) {
  TransactionContext ctx = Begin();
  RunSql(ctx, database(), "CREATE TABLE mixed (id INT64, s STRING(MAX));");
  RunSql(ctx, database(),
         "INSERT INTO mixed (id, s) VALUES (1, 'Z'), (2, 'a'), (3, 'z'), "
         "(4, '\u0413'), (5, '\u00C4'), (6, '1 year'), (7, '12 MONTHS');");
  ASSERT_SUCCESS(ctx.PreCommit());
  ctx = Begin();
  // The fold is byte order plus ASCII case: '1 year' < '12 MONTHS' < 'a' <
  // 'Z' == 'z' (peers, input order) < the multi-byte rows, which the fold
  // cannot reach and which therefore keep their UTF-8 byte order.
  EXPECT_EQ(Order("SELECT s FROM mixed ORDER BY COLLATE(s, 'und:ci')"),
            (std::vector<std::string>{"1 year", "12 MONTHS", "a", "Z", "z",
                                      "\u00C4", "\u0413"}));
  // Interval-shaped text is byte-ordered under both collations: no interval
  // collation exists, so COLLATE cannot quietly reinterpret it.
  EXPECT_EQ(Order("SELECT s FROM mixed WHERE id >= 6 ORDER BY "
                  "COLLATE(s, 'und:ci')"),
            Order("SELECT s FROM mixed WHERE id >= 6 ORDER BY "
                  "COLLATE(s, 'binary')"));
}

TEST_F(SqlCollationTest, CollatedKeyOverANonTextColumnIsUnchanged) {
  TransactionContext ctx = Begin();
  const std::vector<Row> rows =
      RunSql(ctx, database(),
             "SELECT id FROM words ORDER BY COLLATE(id, 'und:ci') LIMIT 3");
  EXPECT_EQ(IntColumn(rows, 0), (std::vector<int64_t>{1, 2, 3}));
  const std::vector<Row> descending = RunSql(
      ctx, database(),
      "SELECT id FROM words ORDER BY COLLATE(id, 'und:ci') DESC LIMIT 3");
  EXPECT_EQ(IntColumn(descending, 0), (std::vector<int64_t>{7, 6, 5}));
}

TEST_F(SqlCollationTest, CollatedOrderOverEmptyAndDuplicatedInput) {
  TransactionContext ctx = Begin();
  RunSql(ctx, database(), "CREATE TABLE few (id INT64, s STRING(MAX));");
  RunSql(ctx, database(),
         "INSERT INTO few (id, s) VALUES (1, 'b'), (2, 'B'), "
         "(3, 'a'), (4, 'A');");
  ASSERT_SUCCESS(ctx.PreCommit());
  // Peers keep their input order (the sort is stable), so a LIMIT is
  // deterministic.
  EXPECT_EQ(Order("SELECT s FROM few ORDER BY COLLATE(s, 'und:ci')"),
            (std::vector<std::string>{"a", "A", "b", "B"}));
  EXPECT_EQ(Order("SELECT s FROM few WHERE id = 1 ORDER BY COLLATE(s, "
                  "'und:ci')"),
            (std::vector<std::string>{"b"}));
  EXPECT_EQ(Order("SELECT s FROM few WHERE id = 99 ORDER BY COLLATE(s, "
                  "'und:ci')"),
            std::vector<std::string>{});
}

TEST_F(SqlCollationTest, CollatedOrderReportsTheCollationInExplain) {
  TransactionContext ctx = Begin();
  std::string plan;
  for (const Row& row : RunSql(ctx, database(),
                               "EXPLAIN SELECT s FROM words ORDER BY "
                               "COLLATE(s, 'und:ci');")) {
    ASSERT_EQ(row.values_.size(), 1U);
    plan += row[0].AsString();
    plan += "\n";
  }
  // The plan still sorts (a case-insensitive order is not a storage order),
  // and the ordering property it reports names the collation it was asked for.
  EXPECT_NE(plan.find("Sort keys=1"), std::string::npos);
}

}  // namespace tinylamb
