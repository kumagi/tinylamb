/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <cstddef>
#include <limits>

#include "common/serdes.hpp"
#include "executor/executor_feature_test.hpp"

namespace tinylamb {
TEST(MergeJoinTest, ResidualFiltersPairsAndPadsUnmatchedOuterRows) {
  const Schema lschema(
      "l", {Column("a", ValueType::kInt64), Column("lv", ValueType::kInt64)});
  const Schema rschema(
      "r", {Column("b", ValueType::kInt64), Column("rv", ValueType::kInt64)});
  const Expression residual = BinaryExpressionExp(
      ColumnValueExp(ColumnName("l", "lv")), BinaryOperation::kLessThan,
      ColumnValueExp(ColumnName("r", "rv")));
  const Schema combined = lschema + rschema;
  {
    MergeJoin inner(
        Src({Row({Value(1), Value(10)}), Row({Value(2), Value(1)})}), {0},
        Src({Row({Value(1), Value(20)}), Row({Value(2), Value(0)})}), {0},
        JoinKind::kInner, 2, 2, residual, combined);
    const std::vector<Row> out = Drain(&inner);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0], (Row({Value(1), Value(10), Value(1), Value(20)})));
  }
  {
    // Left outer: the left row whose only pair fails the residual is
    // NULL-padded.
    MergeJoin outer(
        Src({Row({Value(1), Value(10)}), Row({Value(2), Value(1)})}), {0},
        Src({Row({Value(1), Value(20)}), Row({Value(2), Value(0)})}), {0},
        JoinKind::kLeftOuter, 2, 2, residual, combined);
    const std::vector<Row> out = Drain(&outer);
    ASSERT_EQ(out.size(), 2U);
    bool saw_pad = false;
    for (const Row& row : out) {
      if (row[0] == Value(2)) {
        saw_pad = true;
        EXPECT_TRUE(row[2].IsNull());
      }
    }
    EXPECT_TRUE(saw_pad);
  }
}

TEST(MergeJoinTest, UnmatchedKeysDoNotEvaluateResidual) {
  // Unequal keys never reach the residual, so the unmatched left row is
  // NULL-padded without evaluating a division by zero.
  const Schema lschema("l", {Column("a", ValueType::kInt64)});
  const Schema rschema("r", {Column("b", ValueType::kInt64)});
  const Expression bad = BinaryExpressionExp(
      ColumnValueExp(ColumnName("l", "a")), BinaryOperation::kDivide,
      ColumnValueExp(ColumnName("r", "b")));
  MergeJoin join(Src({IRow(1)}), {0}, Src({IRow(0)}), {0}, JoinKind::kLeftOuter,
                 1, 1, bad, lschema + rschema);
  const std::vector<Row> out = Drain(&join);
  ASSERT_EQ(out.size(), 1U);
  EXPECT_EQ(out[0], (Row({Value(1), Value()})));
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
}

TEST(MergeJoinTest, MatchingKeysPropagateResidualDivisionError) {
  const Schema lschema("l", {Column("a", ValueType::kInt64)});
  const Schema rschema("r", {Column("b", ValueType::kInt64)});
  const Expression bad = BinaryExpressionExp(
      ColumnValueExp(ColumnName("l", "a")), BinaryOperation::kDivide,
      ColumnValueExp(ColumnName("r", "b")));
  MergeJoin join(Src({IRow(0)}), {0}, Src({IRow(0)}), {0}, JoinKind::kLeftOuter,
                 1, 1, bad, lschema + rschema);
  EXPECT_TRUE(Drain(&join).empty());
  EXPECT_NE(join.GetStatus(), Status::kSuccess);
  EXPECT_NE(join.GetStatus().GetMessage().find("division by zero"),
            std::string::npos);
}

TEST(MergeJoinTest, MultiColumnKeysAndEmptyInputs) {
  MergeJoin join(
      Src({Row({Value(1), Value("a")}), Row({Value(1), Value("b")})}), {0, 1},
      Src({Row({Value(1), Value("a")})}), {0, 1}, JoinKind::kInner, 2, 2);
  const std::vector<Row> out = Drain(&join);
  ASSERT_EQ(out.size(), 1U);
  EXPECT_EQ(out[0], (Row({Value(1), Value("a"), Value(1), Value("a")})));

  MergeJoin empty(Src(std::vector<Row>{}), {0}, Src({IRow(1)}), {0},
                  JoinKind::kInner, 1, 1);
  EXPECT_TRUE(Drain(&empty).empty());
  std::stringstream ss;
  empty.Dump(ss, 0);
  EXPECT_NE(ss.str().find("MergeJoin"), std::string::npos);
}

// ===========================================================================
// ParallelMergeJoin.
// ===========================================================================

TEST(ParallelMergeJoinTest, InnerJoinMultiPartitionMatchesSerialResult) {
  ParallelMergeJoin join(
      Src(SortedKeyRows(0, 40, 2)), {0}, Src(SortedKeyRows(10, 50, 1)), {0},
      /*worker_count=*/4, JoinKind::kInner, Expression(), Schema(), 2);
  std::vector<Row> out = Drain(&join);
  SortRowsByValue(&out);
  ASSERT_EQ(out.size(), 60U);  // keys 10..39, 2 x 1 pairs each
  EXPECT_EQ(out[0], (Row({Value(10), Value(100), Value(10), Value(100)})));
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("ParallelMergeJoin"), std::string::npos);
}

TEST(ParallelMergeJoinTest, OuterVariantsPadUnmatchedSides) {
  // Keys 0..9 left, 5..14 right: 0..4 left-only, 5..9 matched, 10..14
  // right-only.  Small inputs keep a single steering partition.
  {
    ParallelMergeJoin join(Src(SortedKeyRows(0, 10, 1)), {0},
                           Src(SortedKeyRows(5, 15, 1)), {0}, 2,
                           JoinKind::kLeftOuter, Expression(), Schema(), 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 10U);  // 5 matched + 5 left-padded
    size_t padded = 0;
    for (const Row& row : out) {
      EXPECT_EQ(row.values_.size(), 4U);
      if (row[2].IsNull()) {
        EXPECT_TRUE(row[3].IsNull());
        ++padded;
      }
    }
    EXPECT_EQ(padded, 5U);
  }
  {
    ParallelMergeJoin join(Src(SortedKeyRows(0, 10, 1)), {0},
                           Src(SortedKeyRows(5, 15, 1)), {0}, 2,
                           JoinKind::kRightOuter, Expression(), Schema(), 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 10U);  // 5 matched + 5 right-padded
    size_t padded = 0;
    for (const Row& row : out) {
      EXPECT_EQ(row.values_.size(), 4U);
      if (row[0].IsNull()) {
        EXPECT_TRUE(row[1].IsNull());
        EXPECT_GE(row[2].value.int_value, 10);  // keys 10..14 are right-only
        ++padded;
      }
    }
    EXPECT_EQ(padded, 5U);
  }
  {
    ParallelMergeJoin join(Src(SortedKeyRows(0, 10, 1)), {0},
                           Src(SortedKeyRows(5, 15, 1)), {0}, 2,
                           JoinKind::kFullOuter, Expression(), Schema(), 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 15U);  // 5 matched + 5 + 5 padded
  }
}

TEST(ParallelMergeJoinTest, SemiAntiAndNullKeys) {
  {
    ParallelMergeJoin semi(Src({Row({Value()}), IRow(1), IRow(2), IRow(3)}),
                           {0}, Src({Row({Value()}), IRow(2)}), {0}, 2,
                           JoinKind::kSemi, Expression(), Schema(), 1);
    const std::vector<Row> out = Drain(&semi);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0][0], Value(2));
  }
  {
    // NULL probe keys match nothing and survive anti.
    ParallelMergeJoin anti(Src({Row({Value()}), IRow(1), IRow(2), IRow(3)}),
                           {0}, Src({Row({Value()}), IRow(2)}), {0}, 2,
                           JoinKind::kAnti, Expression(), Schema(), 1);
    const std::vector<Row> out = Drain(&anti);
    ASSERT_EQ(out.size(), 3U);
    EXPECT_TRUE(out[0][0].IsNull());
    EXPECT_EQ(out[1][0], Value(1));
    EXPECT_EQ(out[2][0], Value(3));
  }
}

TEST(ParallelMergeJoinTest, ResidualDecidesMatchesWithOuterFallback) {
  const Schema lschema(
      "l", {Column("a", ValueType::kInt64), Column("lv", ValueType::kInt64)});
  const Schema rschema(
      "r", {Column("b", ValueType::kInt64), Column("rv", ValueType::kInt64)});
  // lv < rv: pairs against rv=20 pass, pairs against rv=5 fail.
  const Expression residual = BinaryExpressionExp(
      ColumnValueExp(ColumnName("l", "lv")), BinaryOperation::kLessThan,
      ColumnValueExp(ColumnName("r", "rv")));
  {
    ParallelMergeJoin join(
        Src({Row({Value(1), Value(10)}), Row({Value(1), Value(11)})}), {0},
        Src({Row({Value(1), Value(20)}), Row({Value(1), Value(5)})}), {0}, 2,
        JoinKind::kLeftOuter, residual, lschema + rschema, 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 2U);
    for (const Row& row : out) {
      EXPECT_EQ(row[3], Value(20));  // only the rv=20 pairs pass
    }
  }
  {
    // An impossible residual rejects every pair: the left rows fall back to
    // NULL-padded output.
    const Expression impossible = BinaryExpressionExp(
        ColumnValueExp(ColumnName("l", "lv")), BinaryOperation::kGreaterThan,
        ConstantValueExp(Value(int64_t{1000})));
    ParallelMergeJoin join(
        Src({Row({Value(1), Value(10)}), Row({Value(1), Value(11)})}), {0},
        Src({Row({Value(1), Value(20)}), Row({Value(1), Value(5)})}), {0}, 2,
        JoinKind::kLeftOuter, impossible, lschema + rschema, 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 2U);
    for (const Row& row : out) {
      EXPECT_TRUE(row[2].IsNull());
      EXPECT_TRUE(row[3].IsNull());
    }
  }
}

TEST(ParallelMergeJoinTest, AllEqualKeysFormOneGiantCluster) {
  // Every row shares one key: the steering partitioner walks past the whole
  // cluster and the trailing partition holds the full 50 x 50 cross product.
  std::vector<Row> left_rows;
  left_rows.reserve(50);
  std::vector<Row> right_rows;
  right_rows.reserve(50);
  for (int64_t i = 0; i < 50; ++i) {
    left_rows.push_back(Row({Value(int64_t{1}), Value(i)}));
    right_rows.push_back(Row({Value(int64_t{1}), Value(i)}));
  }
  ParallelMergeJoin join(Src(left_rows), {0}, Src(right_rows), {0}, 4,
                         JoinKind::kInner, Expression(), Schema(), 2);
  const std::vector<Row> out = Drain(&join);
  EXPECT_EQ(out.size(), 2500U);
}

TEST(ParallelMergeJoinTest, EmptySidesAndBatchPath) {
  {
    ParallelMergeJoin join(Src(std::vector<Row>{}), {0}, Src({IRow(1)}), {0}, 2,
                           JoinKind::kInner, Expression(), Schema(), 1);
    EXPECT_TRUE(Drain(&join).empty());
    EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  }
  {
    ParallelMergeJoin join(Src(SortedKeyRows(0, 40, 2)), {0},
                           Src(SortedKeyRows(10, 50, 1)), {0}, 4,
                           JoinKind::kInner, Expression(), Schema(), 2);
    DataChunk chunk;
    size_t total = 0;
    for (;;) {
      const size_t got = join.NextBatch(&chunk, 32);
      if (got == 0) {
        break;
      }
      total += got;
    }
    EXPECT_EQ(total, 60U);
  }
}

namespace {
// A residual that throws out of TryEvaluate, modeling the bad_alloc / logic
// exception that used to escape the synchronous single-partition worker path.
class ThrowingResidual final : public ExpressionBase {
 public:
  [[nodiscard]] TypeTag NodeKind() const override { return TypeTag::kInvalid; }
  [[nodiscard]] Value Evaluate(const Row& /*row*/, const Schema& /*schema*/) const override {
    throw std::runtime_error("residual exploded");
  }
  [[nodiscard]] StatusOr<Value> TryEvaluate(
      const Row& row, const Schema& schema) const override {
    return Evaluate(row, schema);
  }
  [[nodiscard]] std::string ToString() const override { return "THROWING()"; }
  void Dump(std::ostream& o) const override { o << ToString(); }
};
}  // namespace

TEST(ParallelMergeJoinTest, SinglePartitionWorkerExceptionBecomesStatus) {
  ParallelMergeJoin join(Src({IRow(1)}), {0}, Src({IRow(1)}), {0},
                         /*worker_count=*/1, JoinKind::kInner,
                         Expression(std::make_shared<ThrowingResidual>()),
                         Schema(), /*right_width=*/1);
  EXPECT_TRUE(Drain(&join).empty());
  EXPECT_EQ(join.GetStatus().GetCode(), StatusCode::kRuntimeError);
  EXPECT_NE(join.GetStatus().GetMessage().find("residual exploded"),
            std::string::npos);
}

TEST(ParallelMergeJoinTest, FailedChildLatchesStatusWithoutPartialResult) {
  ParallelMergeJoin join(std::make_shared<FailingSource>(), {0}, Src({IRow(1)}),
                         {0}, 2, JoinKind::kInner, Expression(), Schema(), 1);
  EXPECT_TRUE(Drain(&join).empty());
  EXPECT_NE(join.GetStatus(), Status::kSuccess);
}

// ===========================================================================
// SharedBuildParallelHashJoin.
// ===========================================================================

TEST(SharedBuildParallelHashJoinTest, KindsMatchSerialSemantics) {
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  left_rows.reserve(70);
  for (int64_t i = 0; i < 70; ++i) {
    left_rows.push_back(Row({Value(i % 10), Value("L")}));
  }
  right_rows.reserve(70);
  for (int64_t i = 0; i < 70; ++i) {
    right_rows.push_back(Row({Value(i % 5), Value("R")}));
  }
  {
    SharedBuildParallelHashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                                     4, JoinKind::kInner, 2);
    const std::vector<Row> out = Drain(&join);
    // Left keys 0..4 (7 rows each) match 14 right rows each: 5 * 7 * 14.
    EXPECT_EQ(out.size(), 70U * 7U);
  }
  {
    // Semi: left rows with keys 0..4 match (35 rows); keys 5..9 have no
    // right counterpart.
    SharedBuildParallelHashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                                     4, JoinKind::kSemi, 2);
    EXPECT_EQ(Drain(&join).size(), 35U);
  }
  {
    // Anti: only the unmatched left keys 5..9 survive.
    SharedBuildParallelHashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                                     4, JoinKind::kAnti, 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 35U);
    for (const Row& row : out) {
      EXPECT_GE(row[0].value.int_value, 5);
    }
  }
  {
    SharedBuildParallelHashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                                     4, JoinKind::kLeftOuter, 2);
    const std::vector<Row> out = Drain(&join);
    // 490 matched pairs + 35 unmatched left rows padded to width 4.
    EXPECT_EQ(out.size(), 525U);
    for (const Row& row : out) {
      EXPECT_EQ(row.values_.size(), 4U);
    }
  }
}

TEST(SharedBuildParallelHashJoinTest, NullKeysNeverMatchAndPadsOuter) {
  const auto left_rows = std::vector<Row>{Row({Value(), Value("ln")}),
                                          Row({Value(1), Value("l1")}),
                                          Row({Value(2), Value("l2")})};
  const auto right_rows = std::vector<Row>{Row({Value(), Value("rn")}),
                                           Row({Value(2), Value("r1")}),
                                           Row({Value(2), Value("r2")})};
  {
    SharedBuildParallelHashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                                     2, JoinKind::kInner, 2);
    const std::vector<Row> out = Drain(&join);
    ASSERT_EQ(out.size(), 2U);
    for (const Row& row : out) {
      EXPECT_EQ(row[0], Value(2));
      EXPECT_EQ(row.values_.size(), 4U);
    }
  }
  {
    SharedBuildParallelHashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                                     2, JoinKind::kLeftOuter, 2);
    const std::vector<Row> out = Drain(&join);
    // 2 matched pairs (left key 2) + both unmatched left rows padded.
    ASSERT_EQ(out.size(), 4U);
    size_t padded = 0;
    for (const Row& row : out) {
      EXPECT_EQ(row.values_.size(), 4U);
      if (row[2].IsNull() && row[3].IsNull()) {
        // Padded rows keep their left payload and NULL right side.
        EXPECT_TRUE(row[0].IsNull() || row[0] == Value(1));
        ++padded;
      }
    }
    EXPECT_EQ(padded, 2U);
  }
}

TEST(SharedBuildParallelHashJoinTest, NullAwareAntiDropsAllOnNullBuildKey) {
  std::vector<Row> left_rows;
  left_rows.reserve(40);
  for (int64_t i = 0; i < 40; ++i) {
    left_rows.push_back(IRow(i));
  }
  SharedBuildParallelHashJoin join(Src(left_rows), {0},
                                   Src({IRow(1000), Row({Value()})}), {0}, 2,
                                   JoinKind::kNullAwareAnti, 1);
  EXPECT_TRUE(Drain(&join).empty());

  // Without a NULL build key the NOT IN set behaves like plain anti.
  SharedBuildParallelHashJoin join2(Src(left_rows), {0}, Src({IRow(1000)}), {0},
                                    2, JoinKind::kNullAwareAnti, 1);
  EXPECT_EQ(Drain(&join2).size(), 40U);
}

// A join key wider than the fixed-width int-key path must not be truncated:
// clamping the width to four parts made (1,1,1,1,1) and (1,1,1,1,2) hash and
// compare equal, so a five-column equi-join returned a false match.
TEST(SharedBuildParallelHashJoinTest, WideKeyNeverTruncatesBelowFifthColumn) {
  const Row left({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{1}),
                  Value(int64_t{1}), Value(int64_t{1})});
  const Row right({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{1}),
                   Value(int64_t{1}), Value(int64_t{2})});
  const std::vector<Row> left_rows{left};
  const std::vector<Row> right_rows{right};
  SharedBuildParallelHashJoin join(Src(left_rows), {0, 1, 2, 3, 4},
                                   Src(right_rows), {0, 1, 2, 3, 4}, 2,
                                   JoinKind::kInner, 1);
  EXPECT_TRUE(Drain(&join).empty());

  // The exact five-column match still has to be found on the byte-key path.
  SharedBuildParallelHashJoin matching(Src(left_rows), {0, 1, 2, 3, 4},
                                       Src(std::vector<Row>{left}),
                                       {0, 1, 2, 3, 4}, 2, JoinKind::kInner, 1);
  EXPECT_EQ(Drain(&matching).size(), 1U);
}

TEST(SharedBuildParallelHashJoinTest, BatchPathAndFailurePropagation) {
  {
    std::vector<Row> left_rows;
    left_rows.reserve(40);
    std::vector<Row> right_rows;
    right_rows.reserve(40);
    for (int64_t i = 0; i < 40; ++i) {
      left_rows.push_back(IRow(i));
      right_rows.push_back(IRow(i));
    }
    SharedBuildParallelHashJoin join(Src(left_rows), {0}, Src(right_rows), {0},
                                     2, JoinKind::kInner, 1);
    DataChunk chunk;
    size_t total = 0;
    for (;;) {
      const size_t got = join.NextBatch(&chunk, 7);
      // The ExecutorBase contract is "fill the destination from scratch".
      // A caller that reuses one DataChunk (Selection does) trusts
      // chunk.Size(); appending to the previous batch would re-emit rows and
      // blow past max_rows.
      EXPECT_EQ(chunk.Size(), got);
      if (got == 0) {
        break;
      }
      EXPECT_LE(got, 7U);
      total += got;
    }
    EXPECT_EQ(total, 40U);
    std::stringstream ss;
    join.Dump(ss, 0);
    EXPECT_NE(ss.str().find("SharedBuildParallelHashJoin"), std::string::npos);
    EXPECT_GT(join.MaterializedBytes(), 0U);
  }
  {
    SharedBuildParallelHashJoin join(std::make_shared<FailingSource>(), {0},
                                     Src({IRow(1)}), {0}, 2, JoinKind::kInner,
                                     1);
    Row row;
    EXPECT_FALSE(join.Next(&row, nullptr));
    EXPECT_NE(join.GetStatus(), Status::kSuccess);
    // Retried pulls keep reporting the latched failure.
    EXPECT_FALSE(join.Next(&row, nullptr));
    EXPECT_NE(join.GetStatus(), Status::kSuccess);
  }
  {
    SharedBuildParallelHashJoin join(Src({IRow(1)}), {0},
                                     std::make_shared<FailingSource>(), {0}, 2,
                                     JoinKind::kInner, 1);
    EXPECT_TRUE(Drain(&join).empty());
    EXPECT_NE(join.GetStatus(), Status::kSuccess);
  }
}

TEST(SetOperationExecutorTest, IncompatibleColumnTypesFailSticky) {
  SetOperationExecutor bad({Src({Row({Value::Date("2020-01-01")})}),
                            Src({Row({Value(int64_t{1})})})},
                           SetOperationKind::kUnion);
  Row row;
  EXPECT_FALSE(bad.Next(&row, nullptr));
  EXPECT_NE(bad.GetStatus(), Status::kSuccess);

  SetOperationExecutor width_mismatch(
      {Src({IRow(1)}), Src({Row({Value(1), Value(2)})})},
      SetOperationKind::kUnion);
  EXPECT_FALSE(width_mismatch.Next(&row, nullptr));
  EXPECT_NE(width_mismatch.GetStatus(), Status::kSuccess);

  SetOperationExecutor no_sources({}, SetOperationKind::kUnion);
  EXPECT_FALSE(no_sources.Next(&row, nullptr));
  EXPECT_NE(no_sources.GetStatus(), Status::kSuccess);
}

TEST(SetOperationExecutorTest, DateAndTextUnifyToDateDomain) {
  // A DATE column unioned with a string keeps the DATE domain, and the
  // string side is parsed back into a date.
  SetOperationExecutor op({Src({Row({Value::Date("2020-01-01")})}),
                           Src({Row({Value("2020-01-01")})})},
                          SetOperationKind::kUnion);
  const std::vector<Row> out = Drain(&op);
  ASSERT_EQ(out.size(), 1U);  // same date after coercion
  EXPECT_EQ(out[0][0].type, ValueType::kDate);
  EXPECT_EQ(out[0][0], Value::Date("2020-01-01"));
}

TEST(SetOperationExecutorTest, TextAndNumericUnifyToVarchar) {
  SetOperationExecutor op(
      {Src({Row({Value(int64_t{7})})}), Src({Row({Value("7")})})},
      SetOperationKind::kUnion);
  const std::vector<Row> out = Drain(&op);
  ASSERT_EQ(out.size(), 1U);
  EXPECT_EQ(out[0][0].type, ValueType::kVarChar);
  EXPECT_EQ(out[0][0], Value("7"));
}

TEST(SetOperationExecutorTest, SpillPathNormalizesIntDoubleAndDedups) {
  std::vector<Row> left_rows;
  left_rows.reserve(300);
  std::vector<Row> right_rows;
  right_rows.reserve(300);
  for (int64_t i = 0; i < 300; ++i) {
    left_rows.push_back(Row({Value(i % 30)}));
    right_rows.push_back(Row({Value(static_cast<double>(i % 30))}));
  }
  BudgetGuard guard(2048);  // non-UNION-ALL operations spill under pressure
  SetOperationExecutor op({Src(left_rows), Src(right_rows)},
                          SetOperationKind::kUnion);
  const std::vector<Row> out = Drain(&op);
  EXPECT_EQ(op.GetStatus(), Status::kSuccess);
  EXPECT_EQ(out.size(), 30U);  // INT64 k and DOUBLE k dedup after coercion
  for (const Row& row : out) {
    EXPECT_EQ(row[0].type, ValueType::kDouble);
  }

  SetOperationExecutor intersect({Src(left_rows), Src(right_rows)},
                                 SetOperationKind::kIntersectAll);
  const std::vector<Row> out2 = Drain(&intersect);
  // Every key appears 10 times per side; INTERSECT ALL keeps min(10, 10).
  EXPECT_EQ(out2.size(), 300U);
}

TEST(SetOperationExecutorTest, SpillPathRejectsWidthMismatch) {
  std::vector<Row> wide_rows;
  wide_rows.reserve(100);
  std::vector<Row> narrow_rows;
  narrow_rows.reserve(100);
  for (int64_t i = 0; i < 100; ++i) {
    wide_rows.push_back(Row({Value(i), Value(i)}));
    narrow_rows.push_back(Row({Value(i)}));
  }
  BudgetGuard guard(2048);
  SetOperationExecutor op({Src(wide_rows), Src(narrow_rows)},
                          SetOperationKind::kExcept);
  Row row;
  EXPECT_FALSE(op.Next(&row, nullptr));
  EXPECT_NE(op.GetStatus(), Status::kSuccess);
}

TEST(SetOperationExecutorTest, ExceptDistinctKeepsRemovingLaterDuplicates) {
  // 1 appears twice on the left and once on the right: EXCEPT DISTINCT must
  // remove BOTH left copies (the removal stays latched for the whole input).
  SetOperationExecutor op({Src({IRow(1), IRow(1), IRow(2)}), Src({IRow(1)})},
                          SetOperationKind::kExcept);
  const std::vector<Row> out = Drain(&op);
  ASSERT_EQ(out.size(), 1U);
  EXPECT_EQ(out[0][0], Value(2));
}

TEST(SetOperationExecutorTest, DumpListsAllSources) {
  SetOperationExecutor op({Src({IRow(1)}), Src({IRow(2)})},
                          SetOperationKind::kUnionAll);
  std::stringstream ss;
  op.Dump(ss, 0);
  EXPECT_NE(ss.str().find("SetOperation"), std::string::npos);
}

// ===========================================================================
// DataChunk / ColumnVector.
// ===========================================================================

TEST(DataChunkFeatureTest, ColumnVectorInfersTypeAfterLeadingNulls) {
  ColumnVector col;
  col.Append(Value());
  col.Append(Value());
  EXPECT_TRUE(col.IsNull(0));
  col.Append(Value(int64_t{7}));
  col.Append(Value(int64_t{9}));
  ASSERT_EQ(col.Size(), 4U);
  EXPECT_EQ(col.Type(), ValueType::kInt64);
  EXPECT_EQ(col.ValueAt(2), Value(int64_t{7}));
  EXPECT_TRUE(col.IsNull(1));

  col.Reset();
  EXPECT_EQ(col.Size(), 0U);
  col.Append(Value(int64_t{1}));
  EXPECT_EQ(col.ValueAt(0), Value(int64_t{1}));
}

TEST(DataChunkFeatureTest, ColumnVectorSupportsDateDoubleStringArray) {
  ColumnVector dates(ValueType::kDate, 2);
  dates.Append(Value::Date("2021-05-06"));
  dates.Append(Value::Date("1999-12-31"));
  EXPECT_EQ(dates.ValueAt(0), Value::Date("2021-05-06"));

  ColumnVector doubles(ValueType::kDouble, 2);
  doubles.Append(Value(1.5));
  doubles.Append(Value());
  EXPECT_EQ(doubles.ValueAt(0), Value(1.5));
  EXPECT_TRUE(doubles.IsNull(1));

  ColumnVector strings(ValueType::kVarChar, 2);
  strings.Append(Value("hello"));
  strings.Append(Value());
  EXPECT_EQ(strings.ValueAt(0), Value("hello"));

  ColumnVector arrays(ValueType::kArray, 2);
  const Value arr =
      Value::Array({Value(int64_t{1}), Value(int64_t{2})}, "INT64");
  arrays.Append(arr);
  arrays.Append(Value());
  EXPECT_EQ(arrays.ValueAt(0), arr);
  EXPECT_TRUE(arrays.IsNull(1));

  // AppendFrom copies storage without re-boxing through Value.
  ColumnVector copy(ValueType::kNull, 0);
  copy.AppendFrom(dates, 1);
  copy.AppendFrom(dates, 0);
  EXPECT_EQ(copy.ValueAt(0), Value::Date("1999-12-31"));
  EXPECT_EQ(copy.ValueAt(1), Value::Date("2021-05-06"));
}

TEST(DataChunkFeatureTest, UnsignedIntColumnsRoundTrip) {
  Column unsigned_col("x", ValueType::kInt64);
  unsigned_col.SetUnsigned(true);
  const Schema schema("t", {unsigned_col});
  DataChunk chunk(schema, 4);
  chunk.Append(Row({Value(uint64_t{18446744073709551615ULL}).WithUnsigned()}));
  chunk.Append(Row({Value(int64_t{5}).WithUnsigned()}));
  EXPECT_TRUE(chunk.ColumnAt(0).IsUnsigned());
  EXPECT_TRUE(chunk.ColumnAt(0).ValueAt(0).IsUnsigned());
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(0),
            Value(uint64_t{18446744073709551615ULL}).WithUnsigned());

  // Bitwise aggregates report unsigned results too.
  EXPECT_TRUE(chunk.AggregateBitAnd(0).IsUnsigned());
  EXPECT_EQ(chunk.AggregateBitAnd(0), Value(int64_t{5}).WithUnsigned());
  EXPECT_EQ(chunk.AggregateBitOr(0),
            Value(uint64_t{18446744073709551615ULL}).WithUnsigned());
  EXPECT_EQ(chunk.AggregateBitXor(0),
            Value(uint64_t{18446744073709551610ULL}).WithUnsigned());
}

TEST(DataChunkFeatureTest, AppendChunkCopiesZoneMapsAcrossTypes) {
  const Schema src_schema(
      "src", {Column("i", ValueType::kInt64), Column("d", ValueType::kDouble),
              Column("s", ValueType::kVarChar), Column("dt", ValueType::kDate),
              Column("arr", ValueType::kArray)});
  DataChunk src(src_schema, 4);
  src.Append(
      Row({Value(int64_t{1}), Value(0.5), Value("a"), Value::Date("2020-01-01"),
           Value::Array({Value(int64_t{9})}, "INT64")}));
  src.Append(Row({Value(), Value(), Value(), Value(), Value()}));

  DataChunk dst;
  dst.AppendFrom(src, 0);
  dst.AppendFrom(src, 1);
  ASSERT_EQ(dst.Size(), 2U);
  EXPECT_EQ(dst.ColumnAt(0).ValueAt(0), Value(int64_t{1}));
  EXPECT_TRUE(dst.ColumnAt(1).IsNull(1));
  EXPECT_TRUE(dst.ColumnAt(4).IsNull(1));
  EXPECT_EQ(dst.PositionAt(0).page_id, src.PositionAt(0).page_id);
  EXPECT_TRUE(dst.HasLayout(src_schema));
}

TEST(DataChunkFeatureTest, AppendRowFromColumnsJoinsAcrossChunks) {
  const Schema schema(
      "t", {Column("a", ValueType::kInt64), Column("b", ValueType::kVarChar)});
  DataChunk c1(schema, 4);
  c1.Append(Row({Value(int64_t{1}), Value("x")}));
  DataChunk c2(schema, 4);
  c2.Append(Row({Value(int64_t{2}), Value("y")}));

  DataChunk out;
  const std::vector<const ColumnVector*> sources = {&c1.ColumnAt(0),
                                                    &c2.ColumnAt(1)};
  out.AppendRowFromColumns(sources, 0, RowPosition());
  ASSERT_EQ(out.Size(), 1U);
  EXPECT_EQ(out.RowAt(0), (Row({Value(int64_t{1}), Value("y")})));
}

TEST(DataChunkFeatureTest, AppendGatherSelectsRowsAndPositions) {
  const Schema schema("t", {Column("a", ValueType::kInt64)});
  DataChunk src(schema, 8);
  src.Append(Row({Value(int64_t{10})}), RowPosition(7, 1));
  src.Append(Row({Value(int64_t{20})}), RowPosition(9, 2));
  src.Append(Row({Value(int64_t{30})}));

  DataChunk dst;
  const std::vector<uint32_t> selection = {2, 0};
  dst.AppendGather(src, selection.data(), selection.size());
  ASSERT_EQ(dst.Size(), 2U);
  EXPECT_EQ(dst.RowAt(0), (Row({Value(int64_t{30})})));
  EXPECT_EQ(dst.RowAt(1), (Row({Value(int64_t{10})})));
  EXPECT_EQ(dst.PositionAt(1).page_id, 7);
  EXPECT_EQ(dst.PositionAt(1).slot, 1);
}

TEST(DataChunkFeatureTest, ResetWithSchemaReinitializesLayout) {
  const Schema schema("t", {Column("a", ValueType::kInt64)});
  DataChunk chunk(schema, 4);
  chunk.Append(IRow(1));
  chunk.Reset(schema, 8);
  EXPECT_EQ(chunk.Size(), 0U);
  EXPECT_EQ(chunk.ColumnCount(), 1U);
  chunk.Append(IRow(2));
  EXPECT_EQ(chunk.RowAt(0), (Row({Value(int64_t{2})})));
  // HasLayout tolerates an inferred kNull-typed column.
  DataChunk inferred;
  inferred.Append(Row({Value(int64_t{3})}));
  EXPECT_TRUE(inferred.HasLayout(schema));
  EXPECT_FALSE(
      inferred.HasLayout(Schema("u", {Column("a", ValueType::kVarChar),
                                      Column("b", ValueType::kInt64)})));
}

TEST(DataChunkFeatureTest, BitAggregatesHandleNullWordsAndSelections) {
  const Schema schema("t", {Column("v", ValueType::kInt64)});
  DataChunk chunk(schema, 70);
  // First word (rows 0..63): all NULL.  Second word: 0b1111, NULL, 0b0011.
  for (int64_t i = 0; i < 64; ++i) {
    chunk.Append(Row({Value()}));
  }
  chunk.Append(Row({Value(int64_t{0b1111})}));
  chunk.Append(Row({Value()}));
  chunk.Append(Row({Value(int64_t{0b0011})}));
  ASSERT_EQ(chunk.Size(), 67U);

  // The all-NULL word is skipped; the mixed word contributes non-NULLs only.
  EXPECT_EQ(chunk.AggregateBitAnd(0), Value(int64_t{0b0011}));
  EXPECT_EQ(chunk.AggregateBitOr(0), Value(int64_t{0b1111}));
  EXPECT_EQ(chunk.AggregateBitXor(0), Value(int64_t{0b1100}));
  // Every value NULL -> NULL aggregate.
  DataChunk all_null(schema, 3);
  all_null.Append(Row({Value()}));
  all_null.Append(Row({Value()}));
  EXPECT_TRUE(all_null.AggregateBitAnd(0).IsNull());
  EXPECT_TRUE(all_null.AggregateBitOr(0).IsNull());
  EXPECT_TRUE(all_null.AggregateBitXor(0).IsNull());
  EXPECT_TRUE(all_null.AggregateLogicalAnd(0).IsNull());
  EXPECT_TRUE(all_null.AggregateLogicalOr(0).IsNull());

  // Selection vector: out-of-range entries are ignored.
  SelectionVector sel(std::vector<uint32_t>{64U, 66U, 1000U});
  EXPECT_EQ(chunk.AggregateBitAnd(0, &sel), Value(int64_t{0b0011}));
  EXPECT_EQ(chunk.AggregateBitOr(0, &sel), Value(int64_t{0b1111}));
  EXPECT_EQ(chunk.AggregateBitXor(0, &sel), Value(int64_t{0b1100}));

  // Logical aggregates: both non-NULL rows (0b1111, 0b0011) are truthy, so
  // AND is TRUE and OR is TRUE.
  EXPECT_EQ(chunk.AggregateLogicalAnd(0), Value(int64_t{1}));
  EXPECT_EQ(chunk.AggregateLogicalOr(0), Value(int64_t{1}));
}

TEST(DataChunkFeatureTest, LogicalAggregatesOnNonIntColumnUseTruthyPath) {
  const Schema schema("t", {Column("d", ValueType::kDouble)});
  DataChunk chunk(schema, 3);
  chunk.Append(Row({Value(0.5)}));  // truthy
  chunk.Append(Row({Value(0.0)}));  // falsy
  chunk.Append(Row({Value()}));     // NULL
  EXPECT_EQ(chunk.AggregateLogicalAnd(0), Value(int64_t{0}));
  EXPECT_EQ(chunk.AggregateLogicalOr(0), Value(int64_t{1}));

  DataChunk all_truthy(schema, 2);
  all_truthy.Append(Row({Value(1.5)}));
  all_truthy.Append(Row({Value(2.5)}));
  EXPECT_EQ(all_truthy.AggregateLogicalAnd(0), Value(int64_t{1}));
  EXPECT_EQ(all_truthy.AggregateLogicalOr(0), Value(int64_t{1}));

  // With a selection vector over the falsy row.
  SelectionVector sel(std::vector<uint32_t>{1U});
  EXPECT_EQ(chunk.AggregateLogicalOr(0, &sel), Value(int64_t{0}));
}

// ===========================================================================
// SimdComparisonKernel: scalar/vector comparisons across all operators.
// ===========================================================================

TEST(SimdComparisonKernelTest, Int64ScalarCoversAllOperatorsAcrossWords) {
  std::vector<int64_t> data;
  data.reserve(130);
  for (size_t i = 0; i < 130; ++i) {
    data.push_back(static_cast<int64_t>(i));  // crosses the 64-row word
  }
  const int64_t target = 64;
  struct Case {
    BinaryOperation op;
    size_t expected;
  };
  const std::vector<Case> cases = {
      {BinaryOperation::kEquals, 1},
      {BinaryOperation::kNotEquals, 129},
      {BinaryOperation::kLessThan, 64},
      {BinaryOperation::kLessThanEquals, 65},
      {BinaryOperation::kGreaterThan, 65},
      {BinaryOperation::kGreaterThanEquals, 66},
  };
  for (const Case& c : cases) {
    ValidityBitmap mask;
    SimdComparisonKernel::CompareInt64(data.data(), data.size(), c.op, target,
                                       &mask);
    EXPECT_EQ(mask.CountTrue(), c.expected) << static_cast<int>(c.op);
    EXPECT_EQ(mask.Size(), data.size());
  }
  // Spot-check positions.
  ValidityBitmap mask;
  SimdComparisonKernel::CompareInt64(data.data(), data.size(),
                                     BinaryOperation::kEquals, 70, &mask);
  EXPECT_FALSE(mask.Get(69));
  EXPECT_TRUE(mask.Get(70));
  EXPECT_FALSE(mask.Get(71));

  // An unsupported op clears the mask.
  ValidityBitmap cleared;
  SimdComparisonKernel::CompareInt64(data.data(), data.size(),
                                     BinaryOperation::kAdd, 0, &cleared);
  EXPECT_EQ(cleared.CountTrue(), 0U);

  // count = 0 yields an empty mask.
  ValidityBitmap empty_mask;
  SimdComparisonKernel::CompareInt64(data.data(), 0, BinaryOperation::kEquals,
                                     0, &empty_mask);
  EXPECT_EQ(empty_mask.Size(), 0U);
}

TEST(SimdComparisonKernelTest, DoubleScalarCoversAllOperators) {
  const std::vector<double> data = {1.0, 2.5, -3.0, 2.5, 0.0, 7.0};
  struct Case {
    BinaryOperation op;
    double target;
    size_t expected;
  };
  const std::vector<Case> cases = {
      {BinaryOperation::kEquals, 2.5, 2},
      {BinaryOperation::kNotEquals, 2.5, 4},
      {BinaryOperation::kLessThan, 2.5, 3},
      {BinaryOperation::kLessThanEquals, 2.5, 5},
      {BinaryOperation::kGreaterThan, 2.5, 1},
      {BinaryOperation::kGreaterThanEquals, 2.5, 3},
  };
  for (const Case& c : cases) {
    ValidityBitmap mask;
    SimdComparisonKernel::CompareDouble(data.data(), data.size(), c.op,
                                        c.target, &mask);
    EXPECT_EQ(mask.CountTrue(), c.expected) << static_cast<int>(c.op);
  }
  ValidityBitmap cleared;
  SimdComparisonKernel::CompareDouble(
      data.data(), data.size(), BinaryOperation::kMultiply, 0.0, &cleared);
  EXPECT_EQ(cleared.CountTrue(), 0U);
}

// TryEvaluateBinary returns TRUE for `NaN != anything` (including NaN != NaN).
// The AVX2 vector body used to use the ordered predicate, so its answer
// depended on whether a NaN row landed in the vector body or the scalar tail.
TEST(SimdComparisonKernelTest, DoubleNotEqualsMatchesEngineForNaN) {
  const double nan = std::numeric_limits<double>::quiet_NaN();
  // 9 rows: the 4-wide NEON body and the scalar tail both see NaNs.
  const std::vector<double> data = {nan, 1.0, nan, 2.0, nan,
                                    3.0, nan, 1.0, nan};
  ValidityBitmap mask;
  SimdComparisonKernel::CompareDouble(data.data(), data.size(),
                                      BinaryOperation::kNotEquals, 1.0, &mask);
  EXPECT_EQ(mask.CountTrue(), 7U);  // five NaNs plus 2.0 and 3.0
  SimdComparisonKernel::CompareDouble(data.data(), data.size(),
                                      BinaryOperation::kNotEquals, nan, &mask);
  EXPECT_EQ(mask.CountTrue(), 9U);  // every row is "not equal" to NaN
  SimdComparisonKernel::CompareDouble(data.data(), data.size(),
                                      BinaryOperation::kEquals, nan, &mask);
  EXPECT_EQ(mask.CountTrue(), 0U);
}

TEST(SimdComparisonKernelTest, StringPrefixCoversAllOperators) {
  const std::vector<std::string_view> data = {
      "apple",        "banana", "cherry",
      "banana",       "date",   "bananabananabanana",
      "bananabananab"};  // NOLINT(clang-diagnostic-error)
  struct Case {
    BinaryOperation op;
    size_t expected;
  };
  const std::vector<Case> cases = {
      // The two long "banana…" strings (past the 8-byte prefix) both sort
      // after "banana", so they count as not-equal and greater.
      {BinaryOperation::kEquals, 2},
      {BinaryOperation::kNotEquals, 5},
      {BinaryOperation::kLessThan, 1},
      {BinaryOperation::kLessThanEquals, 3},
      {BinaryOperation::kGreaterThan, 4},
      {BinaryOperation::kGreaterThanEquals, 6},
  };
  for (const Case& c : cases) {
    ValidityBitmap mask;
    SimdComparisonKernel::CompareString(data.data(), data.size(), c.op,
                                        "banana", &mask);
    EXPECT_EQ(mask.CountTrue(), c.expected) << static_cast<int>(c.op);
  }
  ValidityBitmap cleared;
  SimdComparisonKernel::CompareString(
      data.data(), data.size(), BinaryOperation::kAdd, "banana", &cleared);
  EXPECT_EQ(cleared.CountTrue(), 0U);
}

TEST(SimdComparisonKernelTest, VectorComparisonsMatchPairwiseSemantics) {
  const std::vector<int64_t> lhs = {1, 5, 3, 9, 2};
  const std::vector<int64_t> rhs = {2, 5, 4, 1, 3};
  struct Case {
    BinaryOperation op;
    size_t expected;
  };
  const std::vector<Case> cases = {
      {BinaryOperation::kEquals, 1},
      {BinaryOperation::kNotEquals, 4},
      {BinaryOperation::kLessThan, 3},
      {BinaryOperation::kLessThanEquals, 4},
      {BinaryOperation::kGreaterThan, 1},
      {BinaryOperation::kGreaterThanEquals, 2},
  };
  for (const Case& c : cases) {
    ValidityBitmap mask;
    SimdComparisonKernel::CompareInt64Vectors(lhs.data(), rhs.data(),
                                              lhs.size(), c.op, &mask);
    EXPECT_EQ(mask.CountTrue(), c.expected) << static_cast<int>(c.op);
  }
  ValidityBitmap cleared;
  SimdComparisonKernel::CompareInt64Vectors(lhs.data(), rhs.data(), lhs.size(),
                                            BinaryOperation::kModulo, &cleared);
  EXPECT_EQ(cleared.CountTrue(), 0U);

  const std::vector<double> dl = {1.5, -2.0, 0.0};
  const std::vector<double> dr = {1.5, 2.0, -0.0};
  ValidityBitmap dmask;
  SimdComparisonKernel::CompareDoubleVectors(dl.data(), dr.data(), dl.size(),
                                             BinaryOperation::kEquals, &dmask);
  EXPECT_EQ(dmask.CountTrue(), 2U);  // 1.5 == 1.5 and 0.0 == -0.0
  EXPECT_TRUE(dmask.Get(0));
  EXPECT_TRUE(dmask.Get(2));

  ValidityBitmap dcleared;
  SimdComparisonKernel::CompareDoubleVectors(
      dl.data(), dr.data(), dl.size(), BinaryOperation::kDivide, &dcleared);
  EXPECT_EQ(dcleared.CountTrue(), 0U);
}

// ===========================================================================
// SkipScanDistinct over a real table + index.
// ===========================================================================

class SkipScanDistinctTest : public ::testing::Test {
 protected:
  void SetUp() override {
    database_ =
        Database::Create("executor_feature_test-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  }
  void TearDown() override {
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  // Creates the table, inserts rows, backfills an index over them, commits,
  // and opens a fresh transaction for the scan.
  void SetupTable(const Schema& schema, const IndexSchema& index_schema,
                  const std::vector<Row>& rows) {
    Table tbl = database_->CreateTable(*context_, schema).MoveValue();
    for (const Row& row : rows) {
      EXPECT_TRUE(tbl.Insert(context_->txn_, row).HasValue());
    }
    EXPECT_EQ(database_->CreateIndex(*context_, schema.Name(), index_schema),
              Status::kSuccess);
    EXPECT_EQ(context_->PreCommit(), Status::kSuccess);
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
};

TEST_F(SkipScanDistinctTest, DistinctOverIndexedColumnSkipsDuplicates) {
  const Schema schema(
      "t", {Column("a", ValueType::kInt64), Column("b", ValueType::kVarChar)});
  std::vector<Row> rows;
  rows.reserve(static_cast<size_type>(3 * 20));
  for (int64_t round = 0; round < 3; ++round) {
    for (int64_t k = 1; k <= 20; ++k) {
      rows.push_back(Row({Value(k), Value("v" + std::to_string(k))}));
    }
  }
  SetupTable(schema, IndexSchema("t_a_idx", {0}, {}, IndexMode::kNonUnique),
             rows);
  Table table = database_->GetTable(*context_, "t").MoveValue();
  ASSERT_GE(table.IndexCount(), 1U);
  const Index& index = table.GetIndex(0);

  SkipScanDistinct scan(context_->txn_, table, index, std::vector<Value>{},
                        std::vector<Value>{}, /*ascending=*/true,
                        ConstantValueExp(Value(true)), table.GetSchema(),
                        /*prefix_cols=*/0);
  const std::vector<Row> scan_rows = Drain(&scan);
  ASSERT_EQ(scan_rows.size(), 20U);
  for (size_t i = 0; i < scan_rows.size(); ++i) {
    EXPECT_EQ(scan_rows[i][0], Value(static_cast<int64_t>(i + 1)));
  }
  EXPECT_EQ(scan.GetStatus(), Status::kSuccess);
  std::stringstream ss;
  scan.Dump(ss, 0);
  EXPECT_NE(ss.str().find("SkipScanDistinct"), std::string::npos);

  // Descending walk yields the reversed distinct set.
  SkipScanDistinct descending(
      context_->txn_, table, index, std::vector<Value>{}, std::vector<Value>{},
      false, ConstantValueExp(Value(true)), table.GetSchema(), 0);
  const std::vector<Row> down = Drain(&descending);
  ASSERT_EQ(down.size(), 20U);
  EXPECT_EQ(down[0][0], Value(int64_t{20}));
  EXPECT_EQ(down[19][0], Value(int64_t{1}));

  // A WHERE filter drops some distinct keys.
  const Expression where = BinaryExpressionExp(
      ColumnValueExp(ColumnName("t", "a")), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(int64_t{15})));
  SkipScanDistinct filtered(context_->txn_, table, index, std::vector<Value>{},
                            std::vector<Value>{}, true, where,
                            table.GetSchema(), 0);
  const std::vector<Row> kept = Drain(&filtered);
  ASSERT_EQ(kept.size(), 5U);
  EXPECT_EQ(kept[0][0], Value(int64_t{16}));

  // Batched consumer agreement.
  SkipScanDistinct batched(context_->txn_, table, index, std::vector<Value>{},
                           std::vector<Value>{}, true,
                           ConstantValueExp(Value(true)), table.GetSchema(), 0);
  DataChunk chunk(table.GetSchema(), 8);
  size_t total = 0;
  for (;;) {
    const size_t got = batched.NextBatch(&chunk, 6);
    if (got == 0) {
      break;
    }
    total += got;
  }
  EXPECT_EQ(total, 20U);
}

TEST_F(SkipScanDistinctTest, PrefixDistinctJumpsCompoundIndexGroups) {
  const Schema schema(
      "pt", {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64)});
  std::vector<Row> rows;
  rows.reserve(static_cast<size_type>(5 * 4));
  for (int64_t a = 1; a <= 5; ++a) {
    for (int64_t b = 1; b <= 4; ++b) {
      rows.push_back(Row({Value(a), Value(b)}));
    }
  }
  SetupTable(schema,
             IndexSchema("pt_ab_idx", {0, 1}, {}, IndexMode::kNonUnique), rows);
  Table table = database_->GetTable(*context_, "pt").MoveValue();
  const Index& index = table.GetIndex(0);

  SkipScanDistinct ascending(context_->txn_, table, index, std::vector<Value>{},
                             std::vector<Value>{}, true,
                             ConstantValueExp(Value(true)), table.GetSchema(),
                             /*prefix_cols=*/1);
  const std::vector<Row> up = Drain(&ascending);
  ASSERT_EQ(up.size(), 5U);
  for (size_t i = 0; i < up.size(); ++i) {
    EXPECT_EQ(up[i][0], Value(static_cast<int64_t>(i + 1)));
  }

  SkipScanDistinct descending(
      context_->txn_, table, index, std::vector<Value>{}, std::vector<Value>{},
      false, ConstantValueExp(Value(true)), table.GetSchema(), 1);
  const std::vector<Row> down = Drain(&descending);
  ASSERT_EQ(down.size(), 5U);
  EXPECT_EQ(down[0][0], Value(int64_t{5}));
  EXPECT_EQ(down[4][0], Value(int64_t{1}));
}

TEST_F(SkipScanDistinctTest, UniqueIndexNullKeysDecodeAsMultiValue) {
  const Schema schema("ut", {Column("a", ValueType::kInt64)});
  std::vector<Row> rows = {IRow(1), IRow(2), Row({Value()}), Row({Value()})};
  SetupTable(schema, IndexSchema("ut_a_idx", {0}, {}, IndexMode::kUnique),
             rows);
  Table table = database_->GetTable(*context_, "ut").MoveValue();
  const Index& index = table.GetIndex(0);

  SkipScanDistinct scan(context_->txn_, table, index, std::vector<Value>{},
                        std::vector<Value>{}, true,
                        ConstantValueExp(Value(true)), table.GetSchema(), 0);
  const std::vector<Row> out = Drain(&scan);
  EXPECT_EQ(scan.GetStatus(), Status::kSuccess);
  // DISTINCT sees keys {NULL, 1, 2}: whatever the multi-value list layout,
  // the two NULL entries collapse into one row (DISTINCT treats NULLs as
  // one value, like distinct_test's CollapsesNulls), and every non-NULL key
  // appears exactly once.
  size_t null_rows = 0;
  size_t ones = 0;
  size_t twos = 0;
  for (const Row& row : out) {
    if (row[0].IsNull()) {
      ++null_rows;
    } else if (row[0] == Value(1)) {
      ++ones;
    } else if (row[0] == Value(2)) {
      ++twos;
    }
  }
  EXPECT_EQ(ones, 1U);
  EXPECT_EQ(twos, 1U);
  EXPECT_EQ(null_rows, 1U);
}

TEST_F(SkipScanDistinctTest, InvalidRowImageClearsTheWholeBatch) {
  const Schema schema("distinct_images", {Column("k", ValueType::kInt64)});
  const auto created = database_->CreateTable(*context_, schema);
  ASSERT_TRUE(created.HasValue());
  Table table = created.Value();
  ASSERT_TRUE(table.Insert(context_->txn_, IRow(1)).HasValue());
  const auto second = table.Insert(context_->txn_, IRow(2));
  ASSERT_TRUE(second.HasValue());
  ASSERT_EQ(database_->CreateIndex(*context_, schema.Name(),
                                   IndexSchema("distinct_images_k", {0})),
            Status::kSuccess);
  ASSERT_EQ(context_->PreCommit(), Status::kSuccess);
  context_ = std::make_unique<TransactionContext>(database_->BeginContext());

  std::string header_only(sizeof(slot_t), '\0');
  SerializeSlot(header_only.data(), schema.ColumnCount());
  const Row valid = IRow(2);
  std::string original(valid.Size(), '\0');
  ASSERT_EQ(valid.Serialize(original.data()), original.size());
  ASSERT_TRUE(context_->txn_.AddWriteSet(second.Value()));
  context_->txn_.RegisterVersionWrite(second.Value(), original, header_only);

  table = database_->GetTable(*context_, schema.Name()).MoveValue();
  SkipScanDistinct scan(context_->txn_, table, table.GetIndex(0), Value(),
                        Value(), true, Expression{}, table.GetSchema(), 0);
  DataChunk batch;
  EXPECT_EQ(scan.NextBatch(&batch, 10), 0U);
  EXPECT_TRUE(batch.Empty());
  EXPECT_EQ(scan.GetStatus(), Status::kCorrupt);
  EXPECT_EQ(scan.NextBatch(&batch, 10), 0U);
  EXPECT_EQ(scan.GetStatus(), Status::kCorrupt);
}

TEST_F(SkipScanDistinctTest,
       DeletedFirstEntryInPrefixGroupStillFindsSurvivingKeys) {
  const Schema schema("pt_del", {Column("a", ValueType::kInt64),
                                 Column("b", ValueType::kInt64)});
  Table tbl = database_->CreateTable(*context_, schema).MoveValue();
  ASSERT_EQ(database_->CreateIndex(*context_, schema.Name(),
                                   IndexSchema("pt_del_ab_idx", {0, 1}, {},
                                               IndexMode::kNonUnique)),
            Status::kSuccess);
  ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, mutable_tbl,
                              context_->GetTable("pt_del"));
  const RowPosition pos_1_1 =
      mutable_tbl->Insert(context_->txn_, Row({Value(1), Value(1)}))
          .MoveValue();
  ASSERT_TRUE(mutable_tbl->Insert(context_->txn_, Row({Value(1), Value(2)}))
                  .HasValue());
  ASSERT_TRUE(mutable_tbl->Insert(context_->txn_, Row({Value(2), Value(1)}))
                  .HasValue());
  // Delete the first key (1, 1) so the LSM index holds a tombstone / invisible
  // entry at the start of prefix a=1 while (1, 2) remains visible.
  ASSERT_EQ(mutable_tbl->Delete(context_->txn_, pos_1_1), Status::kSuccess);
  ASSERT_EQ(context_->PreCommit(), Status::kSuccess);

  context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  Table table = database_->GetTable(*context_, "pt_del").MoveValue();
  const Index& index = table.GetIndex(0);

  SkipScanDistinct scan(context_->txn_, table, index, std::vector<Value>{},
                        std::vector<Value>{}, true,
                        ConstantValueExp(Value(true)), table.GetSchema(),
                        /*prefix_cols=*/1);
  const std::vector<Row> out = Drain(&scan);
  EXPECT_EQ(scan.GetStatus(), Status::kSuccess);
  ASSERT_EQ(out.size(), 2U);
  EXPECT_EQ(out[0][0], Value(int64_t{1}));
  EXPECT_EQ(out[1][0], Value(int64_t{2}));
}

TEST(SetOperationExecutorTest, UnionDistinctFoldsCaseInsensitiveText) {
  // SQL equality folds a case-insensitive VARCHAR pair (`=` and DISTINCT do),
  // so UNION DISTINCT must collapse 'A' and 'a' just like a scalar `=`.
  auto ci = [](const char* text) {
    Value v{std::string(text)};
    v.SetCollation(2);  // Collation::kCaseInsensitive / 'und:ci'
    return v;
  };
  SetOperationExecutor op({Src({Row({ci("A")})}), Src({Row({ci("a")})})},
                          SetOperationKind::kUnion);
  EXPECT_EQ(Drain(&op).size(), 1U);
}

TEST(MergeJoinTest, FailedChildLatchesStatusWithoutPartialResult) {
  {
    MergeJoin left_fail(std::make_shared<FailingSource>(), {0}, Src({IRow(1)}),
                        {0}, JoinKind::kInner, 1, 1);
    EXPECT_TRUE(Drain(&left_fail).empty());
    EXPECT_NE(left_fail.GetStatus(), Status::kSuccess);
  }
  {
    MergeJoin right_fail(Src({IRow(1)}), {0}, std::make_shared<FailingSource>(),
                         {0}, JoinKind::kLeftOuter, 1, 1);
    EXPECT_TRUE(Drain(&right_fail).empty());
    EXPECT_NE(right_fail.GetStatus(), Status::kSuccess);
  }
}

}  // namespace tinylamb
