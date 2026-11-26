/**
 * Copyright 2023 KUMAZAKI Hiroki
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
#include "executor/executor_test.hpp"

namespace tinylamb {
TEST_F(ExecutorTest, OptimizerAndRelationalPathsAgree) {
  RelationalRun(*rs_, "CREATE TABLE D5A (id INT64, k INT64);");
  RelationalRun(*rs_, "CREATE TABLE D5B (id INT64, v INT64);");
  RelationalRun(*rs_, "CREATE TABLE D5C (id INT64, w INT64);");
  RelationalRun(*rs_,
                "INSERT INTO D5A VALUES (1, 10), (2, 20), (3, NULL), "
                "(4, 40), (5, NULL);");
  RelationalRun(*rs_, "INSERT INTO D5B VALUES (1, 100), (2, 200), (6, 600);");
  RelationalRun(*rs_, "INSERT INTO D5C VALUES (1, 7), (2, 8), (3, 9);");

  const std::vector<std::pair<std::string, std::string>> pairs{
      {"SELECT COUNT(*) FROM D5A;",
       "SELECT COUNT(*) FROM D5A WHERE EXISTS (SELECT 1 FROM D5A);"},
      {"SELECT a.id FROM D5A AS a LEFT JOIN D5B AS b ON a.id = b.id "
       "WHERE b.id IS NULL;",
       "SELECT a.id FROM D5A AS a LEFT JOIN D5B AS b ON a.id = b.id "
       "WHERE b.id IS NULL AND EXISTS (SELECT 1 FROM D5C);"},
      {"SELECT a.id, b.v, c.w FROM D5A AS a JOIN D5B AS b ON a.id = b.id "
       "JOIN D5C AS c ON a.id = c.id WHERE a.k > 5;",
       "SELECT a.id, b.v, c.w FROM D5A AS a JOIN D5B AS b ON a.id = b.id "
       "JOIN D5C AS c ON a.id = c.id WHERE a.k > 5 AND "
       "EXISTS (SELECT 1 FROM D5C AS e WHERE e.id = a.id);"},
  };
  for (const auto& [optimizer_sql, relational_sql] : pairs) {
    const auto normalize = [](const std::vector<Row>& rows) {
      std::vector<std::string> lines;
      lines.reserve(rows.size());
      for (const Row& row : rows) {
        std::ostringstream out;
        out << row;
        lines.push_back(out.str());
      }
      std::ranges::sort(lines);
      return lines;
    };
    const auto optimized = normalize(RelationalRun(*rs_, optimizer_sql));
    const auto relational = normalize(RelationalRun(*rs_, relational_sql));
    EXPECT_EQ(optimized, relational) << "divergence for: " << optimizer_sql;
    EXPECT_FALSE(optimized.empty());
  }
}

// Row-independent subqueries in a scan filter fold to constants at filter
// compile time (FoldRowIndependentSubqueries).  avg(score) over the fixture =
// (1.2 + 12.2 + 4.9 + 4.14) / 4 = 5.61, so exactly key 3 qualifies.
TEST_F(ExecutorTest, RelationalUncorrelatedScalarSubqueryFoldKeepsResults) {
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT key FROM SampleTable WHERE score > "
                    "(SELECT avg(score) FROM SampleTable) ORDER BY key;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(3));
}

// Uncorrelated membership and EXISTS probes share the same fold contract;
// their folded constants must reproduce the three-valued per-row verdicts.
TEST_F(ExecutorTest, RelationalUncorrelatedMembershipAndExistsFold) {
  const auto not_in = RelationalRun(
      *rs_,
      "SELECT key FROM SampleTable WHERE key NOT IN "
      "(SELECT key FROM SampleTable WHERE key <= 1) ORDER BY key;");
  ASSERT_EQ(not_in.size(), 2U);
  EXPECT_EQ(not_in[0][0], Value(2));
  EXPECT_EQ(not_in[1][0], Value(3));

  const auto exists =
      RelationalRun(*rs_,
                    "SELECT key FROM SampleTable WHERE key = 0 AND EXISTS "
                    "(SELECT 1 FROM SampleTable WHERE key = 3);");
  ASSERT_EQ(exists.size(), 1U);
  EXPECT_EQ(exists[0][0], Value(0));
}

// Correlated subqueries must never fold: the per-row verdict decides the row.
// min(score of the OTHER rows) is 1.2 for every outer row, so keys 1, 2, 3
// (score >= 1.2) qualify while key 0 (1.2 > 1.2 is false) does not.  A wrong
// compile-time fold would use one global constant instead and keep key 0.
TEST_F(ExecutorTest, RelationalCorrelatedSubqueryStaysPerRow) {
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT a.key FROM SampleTable a WHERE a.score >= "
                    "(SELECT min(b.score) FROM SampleTable b "
                    "WHERE b.key <> a.key) ORDER BY a.key;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[1][0], Value(2));
  EXPECT_EQ(rows[2][0], Value(3));
}

// `substr(name, 1, 2) IN (...)` compiles to the byte-prefix membership
// predicate: ASCII prefixes keep the byte compare equivalent to the AST's
// code-point substr, including NULL, short, and multi-byte-leading values.
TEST_F(ExecutorTest, RelationalPrefixInPeelMatchesSubstrVerdicts) {
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{"PrefixTable",
                  {Column("key", ValueType::kInt64),
                   Column("name", ValueType::kVarChar)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    BulkInsert(writer.txn_, table,
               {{Row({Value(0), Value("hello")})},  // prefix "he" -> match
                {Row({Value(1), Value("he")})},     // exact prefix -> match
                {Row({Value(2), Value("h")})},      // shorter than prefix
                {Row({Value(3), Value("hex")})},    // prefix "he" -> match
                {Row({Value(4), Value("héllo")})},  // multi-byte 2nd cp
                {Row({Value(5), Value("πe")})},     // multi-byte 1st cp
                {Row({Value(6), Value()})}});       // NULL -> UNKNOWN
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT key FROM PrefixTable WHERE "
                    "substr(name, 1, 2) IN ('he', 'ho') ORDER BY key;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(0));
  EXPECT_EQ(rows[1][0], Value(1));
  EXPECT_EQ(rows[2][0], Value(3));

  // A start offset other than 1 stays on the AST residual path and must keep
  // its exact semantics: substr(name, 2, 2) matches only "hello" -> "el".
  const auto offset_rows =
      RelationalRun(*rs_,
                    "SELECT key FROM SampleTable WHERE "
                    "substr(name, 2, 2) IN ('el') ORDER BY key;");
  ASSERT_EQ(offset_rows.size(), 1U);
  EXPECT_EQ(offset_rows[0][0], Value(0));
}

// `col [NOT] IN (uncorrelated subquery)` peels to a compile-time value set
// (PrebuiltMembershipPredicate).  These pin the exact three-valued semantics
// the peel must reproduce: NULL members, NULL probes, and the decisive
// empty set.
TEST_F(ExecutorTest, RelationalPrebuiltMembershipMatchesThreeValuedIn) {
  {
    TransactionContext writer = rs_->BeginContext();
    Schema outer_schema{
        "MemOuter",
        {Column("key", ValueType::kInt64), Column("d", ValueType::kDate)}};
    ASSIGN_OR_ASSERT_FAIL(Table, outer, rs_->CreateTable(writer, outer_schema));
    BulkInsert(writer.txn_, outer,
               {{Row({Value(1), Value::DateFromDays(100)})},
                {Row({Value(2), Value::DateFromDays(200)})},
                {Row({Value(3), Value::DateFromDays(300)})},
                {Row({Value(), Value::DateFromDays(400)})}});
    Schema inner_schema{
        "MemInner",
        {Column("k", ValueType::kInt64), Column("dd", ValueType::kDate)}};
    ASSIGN_OR_ASSERT_FAIL(Table, inner, rs_->CreateTable(writer, inner_schema));
    // Members 1, 2 and one NULL: IN keeps {1}; NOT IN rejects everything
    // (the NULL member turns every miss UNKNOWN).
    BulkInsert(writer.txn_, inner,
               {{Row({Value(1), Value::DateFromDays(100)})},
                {Row({Value(2), Value::DateFromDays(200)})},
                {Row({Value(), Value()})}});
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  const auto in_rows = RelationalRun(*rs_,
                                     "SELECT key FROM MemOuter WHERE key IN "
                                     "(SELECT k FROM MemInner) ORDER BY key;");
  // Members are {1, 2, NULL}: keys 1 and 2 match.
  ASSERT_EQ(in_rows.size(), 2U);
  EXPECT_EQ(in_rows[0][0], Value(1));
  EXPECT_EQ(in_rows[1][0], Value(2));

  const auto not_in_rows =
      RelationalRun(*rs_,
                    "SELECT key FROM MemOuter WHERE key NOT IN "
                    "(SELECT k FROM MemInner);");
  EXPECT_TRUE(not_in_rows.empty());

  // NULL probe: `NULL IN (...)` is UNKNOWN for any non-empty set.
  const auto null_probe =
      RelationalRun(*rs_,
                    "SELECT d FROM MemOuter WHERE key IS NULL AND key IN "
                    "(SELECT k FROM MemInner);");
  EXPECT_TRUE(null_probe.empty());

  // DATE probes over a DATE member set take the same peel; members are
  // days {100, 200, NULL}.
  const auto date_rows =
      RelationalRun(*rs_,
                    "SELECT key FROM MemOuter WHERE d IN "
                    "(SELECT dd FROM MemInner) ORDER BY key;");
  ASSERT_EQ(date_rows.size(), 2U);
  EXPECT_EQ(date_rows[0][0], Value(1));
  EXPECT_EQ(date_rows[1][0], Value(2));
}

// The empty member set is decisive without consulting the probe: `x IN ()`
// is FALSE and `x NOT IN ()` is TRUE even for a NULL x.
TEST_F(ExecutorTest, RelationalPrebuiltMembershipEmptySetIsDecisive) {
  {
    TransactionContext writer = rs_->BeginContext();
    Schema outer_schema{"EmptyOuter", {Column("key", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, outer, rs_->CreateTable(writer, outer_schema));
    BulkInsert(writer.txn_, outer, {{Row({Value(1)})}, {Row({Value()})}});
    Schema inner_schema{"EmptyInner", {Column("k", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, inner, rs_->CreateTable(writer, inner_schema));
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  const auto all = RelationalRun(*rs_,
                                 "SELECT key FROM EmptyOuter WHERE key NOT IN "
                                 "(SELECT k FROM EmptyInner) ORDER BY key;");
  ASSERT_EQ(all.size(), 2U);
  // ASC orders NULL first (the engine default).
  EXPECT_TRUE(all[0][0].IsNull());
  EXPECT_EQ(all[1][0], Value(1));
  const auto none = RelationalRun(*rs_,
                                  "SELECT key FROM EmptyOuter WHERE key IN "
                                  "(SELECT k FROM EmptyInner);");
  EXPECT_TRUE(none.empty());
}

// `col op (correlated min/max scalar subquery)` peels to a pre-aggregated
// key->value map (PrebuiltScalarComparePredicate): one GROUP BY execution
// replaces the per-key correlated applies.  These pin the per-key values,
// the empty-group scalar NULL, and the NULL-probe rejection.
TEST_F(ExecutorTest, RelationalPrebuiltScalarCompareMatchesCorrelatedMin) {
  {
    TransactionContext writer = rs_->BeginContext();
    Schema part_schema{
        "ScalarPart",
        {Column("pid", ValueType::kInt64), Column("price", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, part, rs_->CreateTable(writer, part_schema));
    BulkInsert(writer.txn_, part,
               {{Row({Value(1), Value(10)})},
                {Row({Value(2), Value(20)})},
                {Row({Value(3), Value(30)})},
                {Row({Value(), Value(40)})}});
    Schema supply_schema{
        "ScalarSupply",
        {Column("pid", ValueType::kInt64), Column("cost", ValueType::kInt64),
         Column("flag", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, supply,
                          rs_->CreateTable(writer, supply_schema));
    // Per-pid MIN(cost) over flag=1 rows: pid1 -> 5, pid2 -> 20 (its only
    // flag=1 row), pid3 -> no flag=1 rows (scalar NULL).
    BulkInsert(writer.txn_, supply,
               {{Row({Value(1), Value(9), Value(0)})},
                {Row({Value(1), Value(5), Value(1)})},
                {Row({Value(1), Value(7), Value(1)})},
                {Row({Value(2), Value(20), Value(1)})},
                {Row({Value(2), Value(1), Value(0)})},
                {Row({Value(3), Value(1), Value(0)})},
                {Row({Value(), Value(1), Value(1)})}});
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  const auto min_rows =
      RelationalRun(*rs_,
                    "SELECT pid FROM ScalarPart WHERE price = "
                    "(SELECT MIN(cost) FROM ScalarSupply "
                    "WHERE ScalarSupply.pid = ScalarPart.pid AND flag = 1) "
                    "ORDER BY pid;");
  // pid1: 10 != MIN(5,7)=5.  pid2: 20 = 20.  pid3: scalar NULL -> UNKNOWN.
  // NULL probe -> UNKNOWN.
  ASSERT_EQ(min_rows.size(), 1U);
  EXPECT_EQ(min_rows[0][0], Value(2));

  const auto gt_rows =
      RelationalRun(*rs_,
                    "SELECT pid FROM ScalarPart WHERE price > "
                    "(SELECT MIN(cost) FROM ScalarSupply "
                    "WHERE ScalarSupply.pid = ScalarPart.pid AND flag = 1) "
                    "ORDER BY pid;");
  ASSERT_EQ(gt_rows.size(), 1U);
  EXPECT_EQ(gt_rows[0][0], Value(1));  // 10 > 5.

  // MAX over the same correlation: pid1 -> 7, pid2 -> 20, pid3 -> NULL.
  const auto max_rows =
      RelationalRun(*rs_,
                    "SELECT pid FROM ScalarPart WHERE price = "
                    "(SELECT MAX(cost) FROM ScalarSupply "
                    "WHERE ScalarSupply.pid = ScalarPart.pid AND flag = 1) "
                    "ORDER BY pid;");
  // pid1: 10 != MAX(5,7)=7.  pid2: 20 = 20.  pid3: scalar NULL.
  ASSERT_EQ(max_rows.size(), 1U);
  EXPECT_EQ(max_rows[0][0], Value(2));
}

TEST_F(ExecutorTest, TypedMorselTopNKeepsNullOrderStableTiesAndPositions) {
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, table,
        rs_->CreateTable(
            writer,
            Schema("TypedTop", {Column("k", ValueType::kInt64),
                                Column("id", ValueType::kInt64),
                                Column("padding", ValueType::kVarChar)})));
    for (int64_t i = 0; i < 100; ++i) {
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_,
                      Row({i % 9 == 0 ? Value() : Value(i % 5), Value(i),
                           Value(std::string(12000, 'x'))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("TypedTop"));
  const std::vector<slot_t> projection{0, 1};
  const Schema schema("TypedTop", {Column("k", ValueType::kInt64),
                                   Column("id", ValueType::kInt64)});
  for (const bool ascending : {true, false}) {
    for (const bool nulls_first : {true, false}) {
      const std::vector<TopNExecutor::Key> keys{
          {ColumnValueExp(ColumnName("k")), ascending, nulls_first}};
      auto parallel =
          std::make_shared<ParallelScan>(reader.txn_, *table, 4, 1, projection);
      auto serial = std::make_shared<FullScan>(
          reader.txn_, *table, std::numeric_limits<size_t>::max(), false, true,
          std::nullopt, projection);
      TopNExecutor actual(parallel, schema, keys, 11, 3);
      TopNExecutor expected(serial, schema, keys, 11, 3);
      Row row, reference;
      RowPosition position, reference_position;
      while (expected.Next(&reference, &reference_position)) {
        ASSERT_TRUE(actual.Next(&row, &position));
        EXPECT_EQ(row, reference);
        EXPECT_EQ(position, reference_position);
      }
      EXPECT_FALSE(actual.Next(&row, &position));
      EXPECT_TRUE(actual.GetStatus().ok());
      EXPECT_TRUE(parallel->WasTypedFolded());
    }
  }
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(128);
  auto parallel =
      std::make_shared<ParallelScan>(reader.txn_, *table, 4, 1, projection);
  TopNExecutor fallback(
      parallel, schema,
      {{ColumnValueExp(ColumnName("id")), false, std::nullopt}}, 1, 0);
  Row row;
  ASSERT_TRUE(fallback.Next(&row, nullptr));
  EXPECT_EQ(row[1], Value(99));
  EXPECT_FALSE(parallel->WasTypedFolded());
  EXPECT_EQ(QueryMemoryBudget::Global().Used(), 0U);
}

TEST_F(ExecutorTest, TypedSerialFoldHonorsLimitsAndDeclinesAfterConsumption) {
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable(kTableName));
  FullScan scan(reader.txn_, *table, 2, false, true, std::nullopt,
                std::vector<slot_t>{0});
  size_t count = 0;
  auto folded =
      scan.TryFoldTypedBatches([&](size_t, const TypedCellBatch& batch) {
        count += batch.rows;
        return Status::kSuccess;
      });
  ASSERT_TRUE(folded.HasValue());
  EXPECT_TRUE(folded.Value());
  EXPECT_EQ(count, 2U);
  Row row;
  EXPECT_FALSE(scan.Next(&row, nullptr));
  FullScan consumed(reader.txn_, *table, 3, false, true, std::nullopt,
                    std::vector<slot_t>{0});
  ASSERT_TRUE(consumed.Next(&row, nullptr));
  auto declined = consumed.TryFoldTypedBatches(
      [](size_t, const TypedCellBatch&) { return Status::kSuccess; });
  ASSERT_TRUE(declined.HasValue());
  EXPECT_FALSE(declined.Value());
  EXPECT_TRUE(consumed.Next(&row, nullptr));
  FullScan unsupported(reader.txn_, *table);
  auto no = unsupported.TryFoldTypedBatches(
      [](size_t, const TypedCellBatch&) { return Status::kSuccess; });
  ASSERT_TRUE(no.HasValue());
  EXPECT_FALSE(no.Value());
  EXPECT_TRUE(unsupported.Next(&row, nullptr));
}

TEST_F(ExecutorTest, FullScanExecutesLSTableFragmentOverPaxChunk) {
  TransactionContext reader = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable(kTableName));
  FullScan scan(reader.txn_, *table);

  Fragment fragment;
  fragment.access = AccessMode::kFilterScan;
  Conjunct ge_one;
  ge_one.kind = ConjunctKind::kCompare;
  ge_one.col = 1;  // column 1 in lstable_schema is "key" (0 is "__rowid")
  ge_one.op = CompareOp::kGe;
  ge_one.literal_kind = LiteralKind::kInt64;
  ge_one.int64_literal = 1;
  fragment.conjuncts.push_back(ge_one);
  Aggregate sum_key;
  sum_key.kind = AggregateKind::kSum;
  sum_key.col = 1;
  fragment.aggregate = sum_key;

  StatusOr<ExecResult> exec_or =
      scan.ExecuteLSTableFragment(fragment, Runtime::kNative);
  ASSERT_TRUE(exec_or.HasValue());
  const ExecResult& res = exec_or.Value();
  EXPECT_FALSE(res.rejected);
  EXPECT_EQ(res.status, Status::kSuccess);
  EXPECT_EQ(res.scalar_count, 3U);
  EXPECT_EQ(res.scalar_value, 6);
}

TEST_F(ExecutorTest, AffineSumFoldsShareColumnsAndPreserveNullsAndOverflow) {
  const int64_t hi = std::numeric_limits<int64_t>::max();
  const std::vector<std::vector<Value>> cases{
      {Value(1), Value(), Value(4), Value(7)},
      {Value(-10), Value(-5), Value()},
      {Value(), Value()},
      {},
      {Value(hi - 1), Value(1)},
      {Value(hi)},
      {Value(hi), Value(1), Value(-1)}};
  for (size_t test = 0; test < cases.size(); ++test) {
    SCOPED_TRACE(test);
    const std::string name = "AffineSum" + std::to_string(test);
    const Schema schema(name, {Column("value", ValueType::kInt64)});
    std::vector<Row> reference;
    reference.reserve(cases[test].size());
    {
      auto writer = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
      for (const auto& value : cases[test]) {
        reference.emplace_back(std::vector<Value>{value});
        ASSERT_SUCCESS(table.Insert(writer.txn_, reference.back()).GetStatus());
      }
      ASSERT_SUCCESS(writer.PreCommit());
    }
    const std::vector<NamedExpression> aggregates{
        NamedExpression("base",
                        AggregateExpressionExp(AggregationType::kSum,
                                               ColumnValueExp("value"))),
        NamedExpression("plus",
                        AggregateExpressionExp(
                            AggregationType::kSum,
                            BinaryExpressionExp(ColumnValueExp("value"),
                                                BinaryOperation::kAdd,
                                                ConstantValueExp(Value(1))))),
        NamedExpression("minus",
                        AggregateExpressionExp(
                            AggregationType::kSum,
                            BinaryExpressionExp(ColumnValueExp("value"),
                                                BinaryOperation::kSubtract,
                                                ConstantValueExp(Value(-1)))))};
    WaitForChainFreePages();
    auto reader = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, reader.GetTable(name));
    auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
    AggregationExecutor actual(scan, schema, aggregates);
    AggregationExecutor expected(std::make_shared<ConstantExecutor>(reference),
                                 schema, aggregates);
    Row row, wanted;
    EXPECT_EQ(actual.Next(&row, nullptr), expected.Next(&wanted, nullptr));
    EXPECT_EQ(actual.GetStatus(), expected.GetStatus());
    if (actual.GetStatus().ok()) {
      EXPECT_EQ(row, wanted);
    }
    if (test < 3) {
      EXPECT_TRUE(actual.AffineFolded());
    }
    if (test >= 5) {
      EXPECT_FALSE(actual.AffineFolded());
    }
    EXPECT_FALSE(actual.Next(&row, nullptr));
    ASSERT_SUCCESS(reader.PreCommit());
  }
}

TEST_F(ExecutorTest, AffineSumRetainsSnapshotVisibility) {
  const Schema schema("AffineSnapshot", {Column("value", ValueType::kInt64)});
  RowPosition position;
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    ASSIGN_OR_ASSERT_FAIL(RowPosition, inserted,
                          table.Insert(writer.txn_, Row({Value(10)})));
    position = inserted;
    ASSERT_SUCCESS(writer.PreCommit());
  }
  auto old = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        old.GetTable("AffineSnapshot"));
  auto scan = std::make_shared<ParallelScan>(old.txn_, *table, 2, 1);
  {
    auto writer = rs_->BeginContext();
    ASSERT_SUCCESS(
        table->Update(writer.txn_, position, Row({Value(90)})).GetStatus());
    ASSERT_SUCCESS(writer.PreCommit());
  }
  AggregationExecutor aggregate(
      scan, schema,
      {NamedExpression("s",
                       AggregateExpressionExp(
                           AggregationType::kSum,
                           BinaryExpressionExp(ColumnValueExp("value"),
                                               BinaryOperation::kAdd,
                                               ConstantValueExp(Value(5)))))});
  Row row;
  ASSERT_TRUE(aggregate.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(15)}));
  EXPECT_FALSE(aggregate.AffineFolded());
  ASSERT_SUCCESS(aggregate.GetStatus());
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(ExecutorTest, BlockTopNFiltersStringsAndKeepsStablePositions) {
  const Schema schema("StringTop", {Column("text", ValueType::kVarChar),
                                    Column("id", ValueType::kInt64),
                                    Column("payload", ValueType::kVarChar)});
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int i = 0; i < 200; ++i) {
      ASSERT_SUCCESS(
          table
              .Insert(
                  writer.txn_,
                  Row({i % 17 == 0 ? Value() : Value(i % 2 ? "alpha" : "Alpha"),
                       Value(i), Value(std::string(600, 'x'))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("StringTop"));
  for (const auto op : {BinaryOperation::kNotEquals, BinaryOperation::kLike,
                        BinaryOperation::kNotLike}) {
    const std::vector<Expression> predicates{BinaryExpressionExp(
        ColumnValueExp("text"), op,
        ConstantValueExp(
            Value(op == BinaryOperation::kNotEquals ? "" : "%lph%")))};
    const auto filter =
        relational_detail::CompileScanFilter(predicates, schema);
    std::vector<Row> reference;
    std::vector<RowPosition> positions;
    FullScan original(reader.txn_, *table);
    Row row;
    RowPosition position;
    while (original.Next(&row, &position)) {
      if (relational_detail::MatchSimpleCompare(row, filter.simple.at(0))) {
        reference.push_back(row);
        positions.push_back(position);
      }
    }
    for (const bool ascending : {true, false}) {
      auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
      scan->SetRowFilter(
          predicates, filter,
          relational_detail::CompileWorkerFilter(filter, schema));
      const std::vector<TopNExecutor::Key> keys{{ColumnValueExp("text"),
                                                 ascending, false,
                                                 Collation::kCaseInsensitive}};
      TopNExecutor actual(scan, schema, keys, 9, 4);
      TopNExecutor expected(std::make_shared<ConstantExecutor>(reference),
                            schema, keys, 9, 4);
      Row wanted;
      while (expected.Next(&wanted, nullptr)) {
        ASSERT_TRUE(actual.Next(&row, &position));
        EXPECT_EQ(row, wanted);
        const auto found = std::ranges::find(reference, row);
        ASSERT_NE(found, reference.end());
        EXPECT_EQ(position,
                  positions[static_cast<size_t>(found - reference.begin())]);
      }
      EXPECT_FALSE(actual.Next(&row, nullptr));
      ASSERT_SUCCESS(actual.GetStatus());
      EXPECT_TRUE(scan->WasTypedFolded());
    }
  }
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest,
       ProjectedTopNLateMaterializesPayloadAndKeepsStablePositions) {
  const Schema schema("StringTop", {Column("text", ValueType::kVarChar),
                                    Column("id", ValueType::kInt64),
                                    Column("payload", ValueType::kVarChar)});
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int i = 0; i < 200; ++i) {
      ASSERT_SUCCESS(
          table
              .Insert(
                  writer.txn_,
                  Row({i % 17 == 0 ? Value() : Value(i % 2 ? "alpha" : "Alpha"),
                       Value(i), Value(std::string(600, 'x'))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("StringTop"));
  for (const auto op : {BinaryOperation::kNotEquals, BinaryOperation::kLike,
                        BinaryOperation::kNotLike}) {
    const std::vector<Expression> predicates{BinaryExpressionExp(
        ColumnValueExp("text"), op,
        ConstantValueExp(
            Value(op == BinaryOperation::kNotEquals ? "" : "%lph%")))};
    const auto filter =
        relational_detail::CompileScanFilter(predicates, schema);
    std::vector<Row> reference;
    std::vector<RowPosition> positions;
    FullScan original(reader.txn_, *table);
    Row row;
    RowPosition position;
    while (original.Next(&row, &position)) {
      if (relational_detail::MatchSimpleCompare(row, filter.simple.at(0))) {
        reference.push_back(row);
        positions.push_back(position);
      }
    }
    for (const bool ascending : {true, false}) {
      table->SetLSTableStore(nullptr);
      table->SetProjectedLSTableStore(nullptr);
      table->RejectLSTableCapacity(table->LayoutGeneration());
      auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
      scan->SetRowFilter(
          predicates, filter,
          relational_detail::CompileWorkerFilter(filter, schema));
      const std::vector<TopNExecutor::Key> keys{{ColumnValueExp("text"),
                                                 ascending, false,
                                                 Collation::kCaseInsensitive}};
      TopNExecutor actual(scan, schema, keys, 9, 4);
      TopNExecutor expected(std::make_shared<ConstantExecutor>(reference),
                            schema, keys, 9, 4);
      Row wanted;
      while (expected.Next(&wanted, nullptr)) {
        ASSERT_TRUE(actual.Next(&row, &position));
        EXPECT_EQ(row, wanted);
        const auto found = std::ranges::find(reference, row);
        ASSERT_NE(found, reference.end());
        EXPECT_EQ(position,
                  positions[static_cast<size_t>(found - reference.begin())]);
      }
      EXPECT_FALSE(actual.Next(&row, nullptr));
      ASSERT_SUCCESS(actual.GetStatus());
      EXPECT_TRUE(scan->WasTypedFolded());
      ASSERT_NE(scan->GetLSTableStore(), nullptr);
      EXPECT_TRUE(scan->GetLSTableStore()->HasColumn(0));
      EXPECT_FALSE(scan->GetLSTableStore()->HasColumn(2));
    }
  }
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, DictionaryTopNUsesProvenOrderAndPreservesSecondaryTies) {
  for (bool enums : {false, true}) {
    const std::string name = enums ? "EnumOrderedTop" : "DictionaryOrderedTop";
    const Schema schema(name, {Column("s", ValueType::kVarChar),
                               Column("id", ValueType::kInt64),
                               Column("padding", ValueType::kVarChar),
                               Column("g", ValueType::kInt64)});
    std::vector<Row> reference;
    reference.reserve(8192);
    {
      auto writer = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
      for (int64_t i = 0; i < 8192; ++i) {
        std::string text = "key" + std::to_string(i / 16);
        if (enums && i < 32) {
          text = i < 16 ? "TESTENUMNEGATIVE" : "TESTENUM0";
        }
        Row row({i == 8191 ? Value() : Value(std::move(text)), Value(i),
                 Value(std::string(512, 'x')),
                 i == 8191 ? Value() : Value(i / 16)});
        ASSERT_SUCCESS(table.Insert(writer.txn_, row).GetStatus());
        reference.push_back(std::move(row));
      }
      ASSERT_SUCCESS(writer.PreCommit());
    }
    WaitForChainFreePages();
    auto reader = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, reader.GetTable(name));
    for (bool ascending : {false, true}) {
      for (bool collated : {false, true}) {
        auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 4, 1);
        const std::vector<TopNExecutor::Key> keys{
            {ColumnValueExp("s"), ascending, true,
             collated ? Collation::kCaseInsensitive : Collation::kNone},
            {ColumnValueExp("id"), false, false}};
        TopNExecutor actual(scan, schema, keys, 9, 4);
        TopNExecutor expected(std::make_shared<ConstantExecutor>(reference),
                              schema, keys, 9, 4);
        Row wanted, row;
        while (expected.Next(&wanted, nullptr)) {
          ASSERT_TRUE(actual.Next(&row, nullptr));
          EXPECT_EQ(row, wanted);
        }
        EXPECT_FALSE(actual.Next(&row, nullptr));
        ASSERT_SUCCESS(actual.GetStatus());
        EXPECT_TRUE(scan->WasTypedFolded());
        ASSERT_NE(scan->GetLSTableStore(), nullptr);
        const auto& dictionary = scan->GetLSTableStore()->dictionaries[0];
        EXPECT_NE(dictionary.wide_code_bytes, 0);
        EXPECT_EQ(dictionary.codes_follow_string_order, !enums);
        ASSERT_EQ(dictionary.code_counts.size(), dictionary.entries.size() + 1);
        EXPECT_EQ(std::accumulate(dictionary.code_counts.begin(),
                                  dictionary.code_counts.end(), int64_t{0}),
                  8192);
        EXPECT_EQ(dictionary.code_counts.back(), 1);
        EXPECT_EQ(dictionary.code_first_row.back(), 8191U);
        ASSERT_FALSE(dictionary.encounter_codes.empty());
        EXPECT_EQ(dictionary.code_first_row[dictionary.encounter_codes.front()],
                  0U);
      }
    }
    for (const auto operation :
         {BinaryOperation::kLike, BinaryOperation::kNotLike,
          BinaryOperation::kEquals, BinaryOperation::kNotEquals}) {
      for (const bool reject : {false, true}) {
        std::vector<Expression> predicates{BinaryExpressionExp(
            ColumnValueExp("s"), operation, ConstantValueExp(Value("%key1%")))};
        if (reject) {
          predicates.push_back(ConstantValueExp(Value(0)));
        }
        const auto filter =
            relational_detail::CompileScanFilter(predicates, schema);
        auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 4, 1);
        scan->SetRowFilter(
            predicates, filter,
            relational_detail::CompileWorkerFilter(filter, schema));
        AggregationExecutor actual(
            scan, schema,
            {NamedExpression("n",
                             AggregateExpressionExp(AggregationType::kCount,
                                                    ColumnValueExp("*")))});
        int64_t count = 0;
        if (!reject) {
          for (const auto& row : reference) {
            count += static_cast<int64_t>(relational_detail::MatchSimpleCompare(
                row, filter.simple.front()));
          }
        }
        Row row;
        ASSERT_TRUE(actual.Next(&row, nullptr));
        EXPECT_EQ(row, Row({Value(count)}));
        EXPECT_FALSE(actual.Next(&row, nullptr));
        ASSERT_SUCCESS(actual.GetStatus());
      }
    }
    const auto old_store = table->GetLSTableStore();
    ASSERT_NE(old_store, nullptr);
    const auto count_distinct = [&](TransactionContext& context,
                                    int64_t integer_count) {
      for (const std::string column : {"s", "g"}) {
        for (bool filtered : {false, true}) {
          auto scan =
              std::make_shared<ParallelScan>(context.txn_, *table, 4, 1);
          if (filtered) {
            std::vector<Expression> predicates{BinaryExpressionExp(
                ColumnValueExp("id"), BinaryOperation::kLessThan,
                ConstantValueExp(Value(64)))};
            const auto filter =
                relational_detail::CompileScanFilter(predicates, schema);
            scan->SetRowFilter(
                predicates, filter,
                relational_detail::CompileWorkerFilter(filter, schema));
          }
          AggregationExecutor count(
              scan, schema,
              {NamedExpression(
                  "n", AggregateExpressionExp(AggregationType::kCount,
                                              ColumnValueExp(column), true))});
          Row row;
          ASSERT_TRUE(count.Next(&row, nullptr));
          EXPECT_EQ(row, Row({Value(filtered        ? 4
                                    : column == "s" ? 512
                                                    : integer_count)}));
          EXPECT_FALSE(count.Next(&row, nullptr));
          ASSERT_SUCCESS(count.GetStatus());
        }
      }
    };
    const auto count_key = [&](TransactionContext& context, int64_t expected,
                               const LSTableStore* expected_store = nullptr) {
      std::vector<Expression> predicates{
          BinaryExpressionExp(ColumnValueExp("s"), BinaryOperation::kEquals,
                              ConstantValueExp(Value("key100")))};
      const auto filter =
          relational_detail::CompileScanFilter(predicates, schema);
      auto scan = std::make_shared<ParallelScan>(context.txn_, *table, 4, 1);
      scan->SetRowFilter(
          predicates, filter,
          relational_detail::CompileWorkerFilter(filter, schema));
      AggregationExecutor count(
          scan, schema,
          {NamedExpression("n", AggregateExpressionExp(AggregationType::kCount,
                                                       ColumnValueExp("*")))});
      Row row;
      ASSERT_TRUE(count.Next(&row, nullptr));
      EXPECT_EQ(row, Row({Value(expected)}));
      EXPECT_FALSE(count.Next(&row, nullptr));
      ASSERT_SUCCESS(count.GetStatus());
      if (expected_store != nullptr) {
        EXPECT_EQ(scan->GetLSTableStore(), expected_store);
      }
    };
    count_key(reader, 16);
    count_distinct(reader, 512);
    {
      auto writer = rs_->BeginContext();
      ASSERT_SUCCESS(
          table
              ->Insert(writer.txn_,
                       Row({Value("key100"), Value(8192),
                            Value(std::string(512, 'x')), Value(513)}))
              .GetStatus());
      ASSERT_SUCCESS(writer.PreCommit());
    }
    // A prepared frequency summary cannot expose a later committed append
    // through the old snapshot. A fresh snapshot must observe that append.
    count_key(reader, 16);
    count_distinct(reader, 512);
    ASSERT_SUCCESS(reader.PreCommit());
    WaitForChainFreePages();
    auto fresh = rs_->BeginReadOnlyContext();
    count_key(fresh, 17);  // A stale full store falls back until rebuilt.
    count_distinct(fresh, 513);
    const auto new_store = BuildLSTableStore(fresh.txn_, *table);
    ASSERT_NE(new_store, nullptr);
    count_key(fresh, 17, new_store.get());
    count_distinct(fresh, 513);
    EXPECT_NE(new_store, old_store);
    EXPECT_EQ(std::accumulate(old_store->dictionaries[0].code_counts.begin(),
                              old_store->dictionaries[0].code_counts.end(),
                              int64_t{0}),
              8192);
    EXPECT_EQ(std::accumulate(new_store->dictionaries[0].code_counts.begin(),
                              new_store->dictionaries[0].code_counts.end(),
                              int64_t{0}),
              8193);
    ASSERT_SUCCESS(fresh.PreCommit());
  }
}

TEST_F(ExecutorTest, BlockDistinctUsesCanonicalStringEqualityAndNulls) {
  const Schema schema("BlockDistinct", {Column("s", ValueType::kVarChar),
                                        Column("n", ValueType::kInt64)});
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    const std::vector<Value> strings{Value(),
                                     Value(""),
                                     Value("A"),
                                     Value("a"),
                                     Value("4-th avenue"),
                                     Value("4-rd avenue"),
                                     Value("0-12 0 0:0:0"),
                                     Value("1-0 0 0:0:0"),
                                     Value("4-th avenue")};
    for (const auto& text : strings) {
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_,
                      Row({text, Value(std::numeric_limits<int64_t>::min())}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("BlockDistinct"));
  for (const std::string column : {"s", "n"}) {
    auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
    AggregationExecutor aggregate(
        scan, schema,
        {NamedExpression(
            "d", AggregateExpressionExp(AggregationType::kCount,
                                        ColumnValueExp(column), true))});
    Row row;
    ASSERT_TRUE(aggregate.Next(&row, nullptr));
    EXPECT_EQ(row, Row({Value(column == "s" ? 7 : 1)}));
    EXPECT_TRUE(scan->WasTypedFolded());
    ASSERT_SUCCESS(aggregate.GetStatus());
  }
  auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
  Expression collated = FunctionCallExp(
      "collate", {ColumnValueExp("s"), ConstantValueExp(Value("und:ci"))});
  AggregationExecutor aggregate(
      scan, schema,
      {NamedExpression("d",
                       AggregateExpressionExp(AggregationType::kCount,
                                              std::move(collated), true))});
  Row row;
  ASSERT_TRUE(aggregate.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(6)}));
  EXPECT_FALSE(scan->WasTypedFolded());
  ASSERT_SUCCESS(aggregate.GetStatus());
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, BlockDateExtremaKeepDateTypeAndNullableBoundaries) {
  const Schema schema("DateExtrema", {Column("d", ValueType::kDate)});
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (const Value& date :
         {Value(), Value::Date("1969-12-31"), Value::Date("2024-02-29")}) {
      ASSERT_SUCCESS(table.Insert(writer.txn_, Row({date})).GetStatus());
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("DateExtrema"));
  auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
  AggregationExecutor aggregate(
      scan, schema,
      {NamedExpression("lo", AggregateExpressionExp(AggregationType::kMin,
                                                    ColumnValueExp("d"))),
       NamedExpression("hi", AggregateExpressionExp(AggregationType::kMax,
                                                    ColumnValueExp("d")))});
  Row row;
  ASSERT_TRUE(aggregate.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value::Date("1969-12-31"), Value::Date("2024-02-29")}));
  EXPECT_EQ(row[0].type, ValueType::kDate);
  EXPECT_EQ(row[1].type, ValueType::kDate);
  EXPECT_TRUE(scan->WasTypedFolded());
  ASSERT_SUCCESS(aggregate.GetStatus());
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, WholeColumnStoreFoldKeepsScanGuardsAndErrors) {
  const Schema schema("WholeStore", {Column("n", ValueType::kInt64)});
  RowPosition first;
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t n = 0; n < 400; ++n) {
      ASSIGN_OR_ASSERT_FAIL(RowPosition, position,
                            table.Insert(writer.txn_, Row({Value(n)})));
      if (n == 0) {
        first = position;
      }
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("WholeStore"));
  size_t calls = 0;
  const auto consume = [&](const LSTableStore& store) {
    ++calls;
    EXPECT_EQ(store.total_rows, 400U);
    return Status::kSuccess;
  };
  {
    ParallelScan scan(reader.txn_, *table, 2, 1, std::vector<slot_t>{0});
    ASSIGN_OR_ASSERT_FAIL(bool, folded, scan.TryFoldLSTableStore(consume));
    EXPECT_TRUE(folded);
    EXPECT_TRUE(scan.WasTypedFolded());
    EXPECT_EQ(calls, 1U);
    ASSIGN_OR_ASSERT_FAIL(bool, repeated, scan.TryFoldLSTableStore(consume));
    EXPECT_FALSE(repeated);
    Row row;
    EXPECT_FALSE(scan.Next(&row, nullptr));
    EXPECT_FALSE(scan.Next(&row, nullptr));
    ASSERT_SUCCESS(scan.GetStatus());
  }
  for (const bool throws : {false, true}) {
    ParallelScan scan(reader.txn_, *table, 2, 1);
    auto result = scan.TryFoldLSTableStore([&](const LSTableStore&) -> Status {
      if (throws) {
        throw std::runtime_error("fold callback error");
      }
      return Status::kCorrupt;
    });
    EXPECT_FALSE(result.HasValue());
    EXPECT_NE(scan.GetStatus(), Status::kSuccess);
    Row row;
    EXPECT_FALSE(scan.Next(&row, nullptr));
    EXPECT_FALSE(scan.Next(&row, nullptr));
  }
  for (const bool attached : {false, true}) {
    ParallelScan scan(reader.txn_, *table, 2, 1, std::nullopt,
                      attached ? std::vector<IntegerPeekCompare>{}
                               : std::vector<IntegerPeekCompare>{
                                     {.column = 0,
                                      .op = BinaryOperation::kEquals,
                                      .constant = 1}});
    if (attached) {
      const std::vector<Expression> predicates{
          BinaryExpressionExp(ColumnValueExp("n"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(1)))};
      const auto filter =
          relational_detail::CompileScanFilter(predicates, schema);
      scan.SetRowFilter(predicates, filter,
                        relational_detail::CompileWorkerFilter(filter, schema));
    }
    ASSIGN_OR_ASSERT_FAIL(bool, folded, scan.TryFoldLSTableStore(consume));
    EXPECT_FALSE(folded);
    Row row;
    ASSERT_TRUE(scan.Next(&row, nullptr));
    EXPECT_EQ(row, Row({Value(1)}));
    EXPECT_FALSE(scan.Next(&row, nullptr));
    ASSERT_SUCCESS(scan.GetStatus());
  }
  {
    ParallelScan scan(reader.txn_, *table, 2, 1);
    Row row;
    ASSERT_TRUE(scan.Next(&row, nullptr));
    ASSIGN_OR_ASSERT_FAIL(bool, folded, scan.TryFoldLSTableStore(consume));
    EXPECT_FALSE(folded);
  }
  {
    ParallelScan scan(reader.txn_, *table, 2, 1, std::vector<slot_t>{1});
    ASSIGN_OR_ASSERT_FAIL(bool, folded, scan.TryFoldLSTableStore(consume));
    EXPECT_FALSE(folded);
  }
  // A store obtained before a concurrent update must not bypass the old
  // reader's version-chain traversal, even though it is still owned here.
  ParallelScan old_scan(reader.txn_, *table, 2, 1);
  ASSERT_NE(old_scan.GetLSTableStore(), nullptr);
  {
    auto writer = rs_->BeginContext();
    ASSERT_SUCCESS(
        table->Update(writer.txn_, first, Row({Value(999)})).GetStatus());
    ASSERT_SUCCESS(writer.PreCommit());
  }
  ASSIGN_OR_ASSERT_FAIL(bool, old_folded,
                        old_scan.TryFoldLSTableStore(consume));
  EXPECT_FALSE(old_folded);
  int64_t sum = 0;
  Row row;
  while (old_scan.Next(&row, nullptr)) {
    sum += row[0].value.int_value;
  }
  EXPECT_EQ(sum, 399 * 400 / 2);
  ASSERT_SUCCESS(old_scan.GetStatus());
  EXPECT_EQ(calls, 1U);
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, WideDictionaryDistinctAndMembershipUseVisibleRows) {
  const Schema schema("EncodedDistinct", {Column("s", ValueType::kVarChar),
                                          Column("n", ValueType::kInt64)});
  std::vector<Value> texts{Value(), Value(""), Value("0-12 0 0:0:0"),
                           Value("1-0 0 0:0:0")};
  texts.reserve(texts.size() + 300);
  for (size_t i = 0; i < 300; ++i) {
    std::string text(80, 'x');
    text += std::to_string(i);
    texts.emplace_back(std::move(text));
  }
  constexpr int64_t kRows = 12280;
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_,
                      Row({texts[static_cast<size_t>(i) % texts.size()],
                           Value(i * 3)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("EncodedDistinct"));
  for (const int64_t bound : {int64_t{0}, int64_t{30}, kRows * 3}) {
    std::unordered_set<Value> expected;
    expected.reserve(static_cast<size_t>(bound / 3) + 1);
    for (int64_t i = 0; i * 3 < bound; ++i) {
      const auto& text = texts[static_cast<size_t>(i) % texts.size()];
      if (!text.IsNull()) {
        expected.insert(text);
      }
    }
    auto scan = std::make_shared<ParallelScan>(
        reader.txn_, *table, 2, 1, std::nullopt,
        std::vector<IntegerPeekCompare>{{.column = 1,
                                         .op = BinaryOperation::kLessThan,
                                         .constant = bound}});
    ASSERT_NE(scan->GetLSTableStore(), nullptr);
    EXPECT_EQ(scan->GetLSTableStore()->dictionaries[0].wide_code_bytes, 2);
    EXPECT_TRUE(scan->GetLSTableStore()->dictionaries[0].bytewise_equality);
    AggregationExecutor aggregate(
        scan, schema,
        {NamedExpression("d",
                         AggregateExpressionExp(AggregationType::kCount,
                                                ColumnValueExp("s"), true))});
    Row row;
    ASSERT_TRUE(aggregate.Next(&row, nullptr));
    EXPECT_EQ(row, Row({Value(static_cast<int64_t>(expected.size()))}));
    EXPECT_TRUE(scan->WasTypedFolded());
    ASSERT_SUCCESS(aggregate.GetStatus());
  }
  const auto store = table->GetLSTableStore();
  ASSERT_NE(store, nullptr);
  int64_t absent = 1;
  while (store->MayContainInteger(1, absent) && absent < kRows * 3) {
    absent += 3;
  }
  ASSERT_LT(absent, kRows * 3);
  for (const bool attached : {false, true}) {
    for (const int64_t value : {absent, int64_t{0}, (kRows - 1) * 3}) {
      const auto make_scan = [&] {
        if (attached) {
          auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
          const std::vector<Expression> predicates{
              BinaryExpressionExp(ColumnValueExp("n"), BinaryOperation::kEquals,
                                  ConstantValueExp(Value(value)))};
          const auto filter =
              relational_detail::CompileScanFilter(predicates, schema);
          scan->SetRowFilter(
              predicates, filter,
              relational_detail::CompileWorkerFilter(filter, schema));
          return scan;
        }
        return std::make_shared<ParallelScan>(
            reader.txn_, *table, 2, 1, std::nullopt,
            std::vector<IntegerPeekCompare>{{.column = 1,
                                             .op = BinaryOperation::kEquals,
                                             .constant = value}});
      };
      auto scan = make_scan();
      Row row;
      if (value != absent) {
        ASSERT_TRUE(scan->Next(&row, nullptr));
        EXPECT_EQ(row[1], Value(value));
      }
      EXPECT_FALSE(scan->Next(&row, nullptr));
      EXPECT_FALSE(scan->Next(&row, nullptr));
      ASSERT_SUCCESS(scan->GetStatus());
      std::atomic<size_t> rows{0};
      auto folded = make_scan()->TryFoldLSTableBlocks(
          2,
          [&](size_t, const LSTablePageBlock&, const uint32_t*, size_t count) {
            rows += count;
            return Status::kSuccess;
          });
      ASSERT_TRUE(folded.HasValue());
      EXPECT_TRUE(folded.Value());
      EXPECT_EQ(rows.load(), value == absent ? 0 : 1);
    }
  }
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, TypedGroupingHandlesConstantsEmptyInputAndAverageCasts) {
  RelationalRun(*rs_, "CREATE TABLE ConstantGroups (k STRING, n INT64)");
  RelationalRun(*rs_,
                "INSERT INTO ConstantGroups VALUES ('a',1), ('a',3), "
                "('b',NULL), ('b',7)");
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT 1, k, AVG(CAST(n AS FLOAT64)), COUNT(*) "
                    "FROM ConstantGroups GROUP BY 1,k ORDER BY k");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(1), Value("a"), Value(2.0), Value(2)}));
  EXPECT_EQ(rows[1], Row({Value(1), Value("b"), Value(7.0), Value(2)}));
  EXPECT_TRUE(
      RelationalRun(*rs_,
                    "SELECT 'constant', COUNT(*) FROM ConstantGroups "
                    "WHERE n > 100 GROUP BY 'constant' ORDER BY COUNT(*)")
          .empty());
  EXPECT_EQ(
      RelationalRun(*rs_, "SELECT AVG(CAST(n AS FLOAT64)) FROM ConstantGroups"),
      std::vector<Row>{Row({Value(11.0 / 3.0)})});
}

TEST_F(ExecutorTest, ProjectedParallelFoldConvertsExceptionsToStickyStatus) {
  RelationalRun(*rs_, "CREATE TABLE FoldFailure(unused STRING, k INT64)");
  RelationalRun(*rs_,
                "INSERT INTO FoldFailure VALUES ('unused',1),('unused',2)");
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("FoldFailure"));
  table->SetLSTableStore(nullptr);
  table->SetProjectedLSTableStore(nullptr);
  table->RejectLSTableCapacity(table->LayoutGeneration());
  const std::vector<slot_t> needed{1};
  for (const bool allocation_failure : {true, false}) {
    auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
    EXPECT_EQ(scan->GetLSTableStore(), nullptr);
    StatusOr<bool> folded(false);
    EXPECT_NO_THROW(
        folded = scan->TryFoldLSTableBlocks(
            2,
            [&](size_t /*worker*/, const LSTablePageBlock& /*block*/,
                const uint32_t* /*rows*/, size_t /*count*/) -> Status {
              if (allocation_failure) {
                throw std::bad_alloc();
              }
              throw std::runtime_error("injected fold failure");
            },
            &needed));
    const Status expected =
        allocation_failure ? Status::kNoSpace : Status::kRuntimeError;
    ASSERT_FALSE(folded.HasValue());
    EXPECT_EQ(folded.GetStatus(), expected);
    EXPECT_EQ(scan->GetStatus(), expected);
    ASSERT_NE(scan->GetLSTableStore(), nullptr);
    EXPECT_TRUE(scan->GetLSTableStore()->HasColumn(1));
    EXPECT_FALSE(scan->GetLSTableStore()->HasColumn(0));
    DataChunk output;
    EXPECT_EQ(scan->NextBatch(&output), 0U);
    EXPECT_EQ(scan->GetStatus(), expected);
  }
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, ProjectedTopNRestoresReorderedOutputColumns) {
  RelationalRun(
      *rs_,
      "CREATE TABLE TopProjection(k INT64, unused STRING, payload STRING)");
  RelationalRun(*rs_,
                "INSERT INTO TopProjection VALUES "
                "(2,'x','two'),(1,'y','one'),(NULL,'z','nil')");
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("TopProjection"));
  table->SetLSTableStore(nullptr);
  table->SetProjectedLSTableStore(nullptr);
  table->RejectLSTableCapacity(table->LayoutGeneration());
  // Prepare an unrelated column, then replace it with the physical sort key.
  // The output projection needs the winning payloads from the table.
  auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
  ASSIGN_OR_ASSERT_FAIL(bool, prepared, scan->PrepareLSTableColumns({1}));
  ASSERT_TRUE(prepared);
  ASSIGN_OR_ASSERT_FAIL(bool, replaced, scan->PrepareLSTableColumns({0}));
  ASSERT_TRUE(replaced);
  const Schema& scan_schema = table->GetSchema();
  auto top = std::make_shared<TopNExecutor>(
      scan, scan_schema,
      std::vector<TopNExecutor::Key>{{ColumnValueExp("k"), true, false}}, 2, 0);
  Projection output({NamedExpression("payload", ColumnValueExp("payload")),
                     NamedExpression("k", ColumnValueExp("k"))},
                    scan_schema, top);
  Row row;
  ASSERT_TRUE(output.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value("one"), Value(1)}));
  ASSERT_TRUE(output.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value("two"), Value(2)}));
  EXPECT_FALSE(output.Next(&row, nullptr));
  ASSERT_SUCCESS(output.GetStatus());
  EXPECT_TRUE(scan->WasTypedFolded());
  ASSERT_NE(scan->GetLSTableStore(), nullptr);
  EXPECT_TRUE(scan->GetLSTableStore()->HasColumn(0));
  EXPECT_FALSE(scan->GetLSTableStore()->HasColumn(2));
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, PreparedNarrowFoldCanFallBackToFullRows) {
  RelationalRun(*rs_, "CREATE TABLE NarrowFallback(k INT64, payload STRING)");
  RelationalRun(*rs_, "INSERT INTO NarrowFallback VALUES (2,'two'),(1,'one')");
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("NarrowFallback"));
  table->SetLSTableStore(nullptr);
  table->SetProjectedLSTableStore(nullptr);
  table->RejectLSTableCapacity(table->LayoutGeneration());
  auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
  ASSIGN_OR_ASSERT_FAIL(bool, prepared, scan->PrepareLSTableColumns({0}));
  ASSERT_TRUE(prepared);
  ASSERT_NE(scan->GetLSTableStore(), nullptr);
  EXPECT_FALSE(scan->GetLSTableStore()->HasColumn(1));
  Row row;
  ASSERT_TRUE(scan->Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(2), Value("two")}));
  ASSERT_TRUE(scan->Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(1), Value("one")}));
  EXPECT_FALSE(scan->Next(&row, nullptr));
  ASSERT_SUCCESS(scan->GetStatus());
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, NarrowEqualitySynopsisKeepsMatchingPayloadAndFreshWrites) {
  const Schema schema(
      "EqualityPayload",
      {Column("k", ValueType::kInt64), Column("payload", ValueType::kVarChar)});
  RelationalRun(*rs_, "CREATE TABLE EqualityPayload(k INT64,payload STRING)");
  RelationalRun(*rs_, "INSERT INTO EqualityPayload VALUES (1,'one'),(2,'two')");
  WaitForChainFreePages();
  for (const int64_t wanted : {int64_t{1}, int64_t{333}}) {
    auto reader = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          reader.GetTable("EqualityPayload"));
    table->SetLSTableStore(nullptr);
    table->SetProjectedLSTableStore(nullptr);
    table->RejectLSTableCapacity(table->LayoutGeneration());
    ParallelScan scan(reader.txn_, *table, 2, 1);
    const std::vector<Expression> predicates{
        BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kEquals,
                            ConstantValueExp(Value(wanted)))};
    auto filter = relational_detail::CompileScanFilter(predicates, schema);
    scan.SetRowFilter(predicates, filter,
                      relational_detail::CompileWorkerFilter(filter, schema));
    ASSERT_NE(scan.GetLSTableStore(), nullptr);
    EXPECT_FALSE(scan.GetLSTableStore()->HasColumn(1));
    Row row;
    EXPECT_EQ(scan.Next(&row, nullptr), wanted == 1);
    if (wanted == 1) {
      EXPECT_EQ(row, Row({Value(1), Value("one")}));
      EXPECT_FALSE(scan.Next(&row, nullptr));
    }
    ASSERT_SUCCESS(scan.GetStatus());
    ASSERT_SUCCESS(reader.PreCommit());
  }
  // A membership rejection from the previous generation cannot hide a new
  // matching row, whether its MVCC chain is still live or already retired.
  RelationalRun(*rs_, "INSERT INTO EqualityPayload VALUES (333,'new')");
  EXPECT_EQ(
      RelationalRun(*rs_, "SELECT payload FROM EqualityPayload WHERE k=333"),
      std::vector<Row>{Row({Value("new")})});
}

TEST_F(ExecutorTest,
       RelationalHybridHashJoinRepartitionsAndHandlesSingleKeySkewOuter) {
  // Part 1: multi-level salted repartitioning when distinct keys exceed
  // partition_budget_bytes() under a 16 KiB limit.
  CreateWideTable(*rs_, "RepartL", 400);
  CreateWideTable(*rs_, "RepartR", 400);
  const std::string inner_repart_sql =
      "SELECT a.key, b.key FROM RepartL AS a JOIN RepartR AS b "
      "ON a.key = b.key WHERE EXISTS (SELECT 1 FROM RepartL);";
  std::vector<std::pair<int64_t, int64_t>> repart_ref;
  {
    ScopedQueryMemory unlimited(0);
    repart_ref = KeyPairCounts(RelationalRun(*rs_, inner_repart_sql));
  }
  ASSERT_EQ(repart_ref.size(), 400U);
  {
    ScopedQueryMemory memory(16384);
    EXPECT_EQ(KeyPairCounts(RelationalRun(*rs_, inner_repart_sql)), repart_ref);
  }

  // Part 2: all rows share key=7 on both sides plus a few unmatched rows so
  // salted repartitioning hits kMaxHybridDepth and falls back to chunked skew
  // probing across INNER, LEFT, RIGHT, and FULL OUTER joins.
  auto seed_skew_table = [&](std::string_view name, int64_t unmatched_base) {
    TransactionContext ctx = rs_->BeginContext();
    Schema schema{std::string(name),
                  {Column("key", ValueType::kInt64),
                   Column("payload", ValueType::kVarChar)}};
    StatusOr<Table> created = rs_->CreateTable(ctx, schema);
    ASSERT_SUCCESS(created.GetStatus());
    std::string wide_payload(192, 's');
    for (int i = 0; i < 60; ++i) {
      ASSERT_SUCCESS(
          created.Value()
              .Insert(ctx.txn_,
                      Row(std::vector{Value(int64_t{7}),
                                      Value(wide_payload + std::to_string(i))}))
              .GetStatus());
    }
    for (int i = 0; i < 4; ++i) {
      ASSERT_SUCCESS(
          created.Value()
              .Insert(ctx.txn_,
                      Row(std::vector{Value(unmatched_base + i),
                                      Value(std::string(wide_payload))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  };
  seed_skew_table("SkewSingleL", 900);
  seed_skew_table("SkewSingleR", 800);

  const std::vector<std::string> skew_sqls{
      "SELECT a.key, b.key FROM SkewSingleL AS a JOIN SkewSingleR AS b "
      "ON a.key = b.key WHERE EXISTS (SELECT 1 FROM SkewSingleL);",
      "SELECT a.key, b.key FROM SkewSingleL AS a LEFT JOIN SkewSingleR AS b "
      "ON a.key = b.key WHERE EXISTS (SELECT 1 FROM SkewSingleL);",
      "SELECT a.key, b.key FROM SkewSingleL AS a RIGHT JOIN SkewSingleR AS b "
      "ON a.key = b.key WHERE EXISTS (SELECT 1 FROM SkewSingleL);",
      "SELECT a.key, b.key FROM SkewSingleL AS a FULL JOIN SkewSingleR AS b "
      "ON a.key = b.key WHERE EXISTS (SELECT 1 FROM SkewSingleL);"};
  for (const std::string& sql : skew_sqls) {
    std::vector<std::pair<int64_t, int64_t>> ref;
    {
      ScopedQueryMemory unlimited(0);
      ref = KeyPairCounts(RelationalRun(*rs_, sql));
    }
    ASSERT_GE(ref.size(), 3600U) << sql;
    {
      ScopedQueryMemory memory(16384);
      EXPECT_EQ(KeyPairCounts(RelationalRun(*rs_, sql)), ref) << sql;
    }
  }
}

TEST_F(ExecutorTest, RelationalPartitionAggRecursiveSplitUnderTightBudget) {
  CreateWideTable(*rs_, "WideRecAgg", 400);
  const std::string sql =
      "SELECT key, COUNT(*), SUM(LENGTH(name)) FROM WideRecAgg "
      "GROUP BY key HAVING COUNT(*) > 0 ORDER BY key;";
  std::vector<Row> reference;
  {
    ScopedQueryMemory unlimited(0);
    reference = RelationalRun(*rs_, sql);
  }
  ASSERT_EQ(reference.size(), 400U);
  {
    ScopedQueryMemory memory(32768);
    const std::vector<Row> spilled = RelationalRun(*rs_, sql);
    EXPECT_EQ(spilled, reference);
  }
}

TEST_F(ExecutorTest, RelationalOrderByExternalMergeAndApplyOrderByUnderBudget) {
  CreateWideTable(*rs_, "WideOrderMerge", 500);
  ScopedQueryMemory memory(32768);

  // Full external merge sort via Project's ExternalKeyedSorter.
  const auto full_desc = RelationalRun(
      *rs_,
      "SELECT key, name FROM WideOrderMerge "
      "WHERE EXISTS (SELECT 1 FROM WideOrderMerge) ORDER BY key DESC;");
  ASSERT_EQ(full_desc.size(), 500U);
  EXPECT_EQ(full_desc.front()[0], Value(int64_t{499}));
  EXPECT_EQ(full_desc.back()[0], Value(int64_t{0}));
  for (size_t i = 0; i < full_desc.size(); ++i) {
    EXPECT_EQ(full_desc[i][0], Value(static_cast<int64_t>(499 - i)));
  }

  // Bounded Top-K heap path with OFFSET.
  const auto topk = RelationalRun(*rs_,
                                  "SELECT key FROM WideOrderMerge "
                                  "WHERE EXISTS (SELECT 1 FROM WideOrderMerge) "
                                  "ORDER BY key DESC LIMIT 5 OFFSET 2;");
  ASSERT_EQ(topk.size(), 5U);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(topk[i][0], Value(static_cast<int64_t>(497 - i)));
  }

  // Set-operation + ORDER BY routes directly through ApplyOrderBy. Setting a
  // large LIMIT that fails CanReserve(5000 * 160) under 32 KiB exercises the
  // non-heap external merge sort path inside ApplyOrderBy.
  const auto apply_order =
      RelationalRun(*rs_,
                    "SELECT key, name FROM WideOrderMerge "
                    "UNION ALL "
                    "SELECT key + 1000, name FROM WideOrderMerge "
                    "ORDER BY key DESC LIMIT 5000;");
  ASSERT_EQ(apply_order.size(), 1000U);
  EXPECT_EQ(apply_order.front()[0], Value(int64_t{1499}));
  EXPECT_EQ(apply_order[499][0], Value(int64_t{1000}));
  EXPECT_EQ(apply_order[500][0], Value(int64_t{499}));
  EXPECT_EQ(apply_order.back()[0], Value(int64_t{0}));
}

TEST_F(ExecutorTest, RelationalAndGroupedFinishNextBatchStreamSpilledRelation) {
  CreateWideTable(*rs_, "WideBatchStream", 300);
  auto run_batched = [&](std::string_view sql, size_t batch_size,
                         size_t min_batches) -> std::vector<Row> {
    TransactionContext context = rs_->BeginContext();
    SqlEngine engine(*rs_);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    std::vector<Row> rows;
    if (!prepared.HasValue()) {
      ADD_FAILURE() << sql << "\n" << engine.LastError();
      EXPECT_SUCCESS(context.Abort());
      return rows;
    }
    DataChunk chunk;
    size_t batches = 0;
    while (prepared.Value()->NextBatch(&chunk, batch_size) > 0) {
      ++batches;
      EXPECT_LE(chunk.Size(), batch_size);
      for (size_t i = 0; i < chunk.Size(); ++i) {
        rows.push_back(chunk.RowAt(i));
      }
    }
    EXPECT_GE(batches, min_batches) << sql;
    EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
    EXPECT_EQ(context.PreCommit(), Status::kSuccess);
    return rows;
  };

  ScopedQueryMemory memory(32768);
  const std::string rel_sql =
      "SELECT key, name FROM WideBatchStream "
      "WHERE EXISTS (SELECT 1 FROM WideBatchStream) ORDER BY key;";
  const auto rel_rows = run_batched(rel_sql, 13, 20);
  ASSERT_EQ(rel_rows.size(), 300U);
  for (size_t i = 0; i < rel_rows.size(); ++i) {
    EXPECT_EQ(rel_rows[i][0], Value(static_cast<int64_t>(i)));
  }
  EXPECT_GT(StatsValue(RelationalExplain(*rs_, rel_sql, /*analyze=*/true),
                       "relation_spills="),
            0);

  // Directly test GroupedFinishExecutor::NextBatch over a spilled core output.
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("WideBatchStream"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                          ctx.GetStats("WideBatchStream"));
    auto core_plan = std::make_shared<FullScanPlan>(*table, *stats);
    auto stmt = std::make_shared<SelectStatement>(
        std::vector<NamedExpression>{
            NamedExpression("key", ColumnValueExp("key")),
            NamedExpression("name", ColumnValueExp("name"))},
        std::vector<std::string>{"WideBatchStream"}, nullptr);
    stmt->SetGroupBy({ColumnValueExp("key"), ColumnValueExp("name")});
    stmt->SetOrderBy(
        {SelectStatement::OrderByTerm{.expression = ColumnValueExp("key"),
                                      .ascending = true,
                                      .nulls_first = false}});
    Executor grouped_finish =
        EmitGroupedFinishExecutor(ctx, std::move(core_plan), std::move(stmt));
    std::vector<Row> grp_rows;
    DataChunk chunk;
    size_t batches = 0;
    while (grouped_finish->NextBatch(&chunk, 17) > 0) {
      ++batches;
      EXPECT_LE(chunk.Size(), 17U);
      for (size_t i = 0; i < chunk.Size(); ++i) {
        grp_rows.push_back(chunk.RowAt(i));
      }
    }
    EXPECT_GE(batches, 15U);
    ASSERT_SUCCESS(grouped_finish->GetStatus());
    std::ostringstream dump;
    grouped_finish->Dump(dump, 0);
    EXPECT_NE(dump.str().find("GroupByFinish"), std::string::npos)
        << dump.str();
    ASSERT_EQ(grp_rows.size(), 300U);
    for (size_t i = 0; i < grp_rows.size(); ++i) {
      EXPECT_EQ(grp_rows[i][0], Value(static_cast<int64_t>(i)));
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
}

}  // namespace tinylamb
