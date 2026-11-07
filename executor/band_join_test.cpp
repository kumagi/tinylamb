/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/band_join.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/constant_executor.hpp"
#include "expression/binary_expression.hpp"
#include "expression/expression.hpp"
#include "plan/band_join_plan.hpp"
#include "plan/cascades.hpp"
#include "plan/implementation_rules.hpp"
#include "plan/sort_plan.hpp"
#include "plan/values_plan.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

Executor Source(std::vector<Row> rows) {
  return std::make_shared<ConstantExecutor>(std::move(rows));
}

std::vector<Row> Drain(ExecutorBase* executor) {
  std::vector<Row> result;
  Row row;
  RowPosition position;
  while (executor->Next(&row, &position)) {
    result.push_back(row);
    row = Row();
  }
  return result;
}

Schema IntSchema(const std::string& name, const std::string& column) {
  return Schema(name, {Column(column, ValueType::kInt64)});
}

Schema DateSchema(const std::string& name, const std::string& column) {
  return Schema(name, {Column(column, ValueType::kDate)});
}

Expression IntComparison(BinaryOperation operation) {
  return BinaryExpressionExp(ColumnValueExp(ColumnName("l", "x")), operation,
                             ColumnValueExp(ColumnName("r", "x")));
}

Expression DateComparison(BinaryOperation operation) {
  return BinaryExpressionExp(ColumnValueExp(ColumnName("l", "d")), operation,
                             ColumnValueExp(ColumnName("r", "d")));
}

std::vector<std::pair<int64_t, int64_t>> IntPairs(
    const std::vector<Row>& rows) {
  std::vector<std::pair<int64_t, int64_t>> result;
  result.reserve(rows.size());
  for (const Row& row : rows) {
    result.emplace_back(row[0].value.int_value, row[1].value.int_value);
  }
  return result;
}

class FailingSource final : public ExecutorBase {
 public:
  size_t NextBatch(DataChunk* dst, size_t /*max_rows*/) override {
    dst->Reset();
    return 0;
  }
  void Dump(std::ostream& o, int /*indent*/) const override {
    o << "FailingSource";
  }
  [[nodiscard]] Status GetStatus() const override {
    return StatusError(StatusCode::kRuntimeError, "failing source");
  }
};

class ErrorExpression final : public ExpressionBase {
 public:
  [[nodiscard]] TypeTag Type() const override { return TypeTag::kInvalid; }
  [[nodiscard]] Value Evaluate(const Row& /*row*/,
                               const Schema& /*schema*/) const override {
    return {};
  }
  [[nodiscard]] StatusOr<Value> TryEvaluate(
      const Row& /*row*/, const Schema& /*schema*/) const override {
    return Status{StatusCode::kInvalidArgument, "band predicate failure"};
  }
  [[nodiscard]] std::string ToString() const override { return "ERROR()"; }
  void Dump(std::ostream& /*output*/) const override {}
};

}  // namespace

TEST(BandJoinTest, Int64LessThanKeepsLeftMajorOrderDuplicatesAndNulls) {
  const Schema left_schema = IntSchema("l", "x");
  const Schema right_schema = IntSchema("r", "x");
  const Schema combined = left_schema + right_schema;
  const std::vector<Row> left_rows{Row({Value()}), Row({Value(1)}),
                                   Row({Value(1)}), Row({Value(3)}),
                                   Row({Value(5)})};
  const std::vector<Row> right_rows{Row({Value()}),  Row({Value(1)}),
                                    Row({Value(2)}), Row({Value(3)}),
                                    Row({Value(3)}), Row({Value(6)})};
  BandJoin join(Source(left_rows), {0}, Source(right_rows), {0},
                BinaryOperation::kLessThan,
                IntComparison(BinaryOperation::kLessThan), combined,
                JoinKind::kInner, 1, 1, combined);
  const std::vector<Row> output = Drain(&join);

  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  ASSERT_EQ(output.size(), 10U);
  const std::vector<std::pair<int64_t, int64_t>> expected{
      {1, 2}, {1, 3}, {1, 3}, {1, 6}, {1, 2},
      {1, 3}, {1, 3}, {1, 6}, {3, 6}, {5, 6}};
  EXPECT_EQ(IntPairs(output), expected);
  std::stringstream dump;
  join.Dump(dump, 0);
  EXPECT_NE(dump.str().find("BandJoin"), std::string::npos);
}

TEST(BandJoinTest, AllStrictAndInclusiveIntegerBoundaries) {
  const Schema left_schema = IntSchema("l", "x");
  const Schema right_schema = IntSchema("r", "x");
  const Schema combined = left_schema + right_schema;
  const std::vector<Row> left_rows{Row({Value(1)}), Row({Value(2)}),
                                   Row({Value(3)})};
  const std::vector<Row> right_rows{Row({Value(1)}), Row({Value(2)}),
                                    Row({Value(3)})};
  const auto run = [&](BinaryOperation operation) {
    BandJoin join(Source(left_rows), {0}, Source(right_rows), {0}, operation,
                  IntComparison(operation), combined, JoinKind::kInner, 1, 1,
                  combined);
    return IntPairs(Drain(&join));
  };
  EXPECT_EQ(run(BinaryOperation::kLessThan),
            (std::vector<std::pair<int64_t, int64_t>>{{1, 2}, {1, 3}, {2, 3}}));
  EXPECT_EQ(run(BinaryOperation::kLessThanEquals),
            (std::vector<std::pair<int64_t, int64_t>>{
                {1, 1}, {1, 2}, {1, 3}, {2, 2}, {2, 3}, {3, 3}}));
  EXPECT_EQ(run(BinaryOperation::kGreaterThan),
            (std::vector<std::pair<int64_t, int64_t>>{{2, 1}, {3, 1}, {3, 2}}));
  EXPECT_EQ(run(BinaryOperation::kGreaterThanEquals),
            (std::vector<std::pair<int64_t, int64_t>>{
                {1, 1}, {2, 1}, {2, 2}, {3, 1}, {3, 2}, {3, 3}}));
}

TEST(BandJoinTest, DateKeysUseTheSameBandSemantics) {
  const Schema left_schema = DateSchema("l", "d");
  const Schema right_schema = DateSchema("r", "d");
  const Schema combined = left_schema + right_schema;
  const std::vector<Row> left_rows{
      Row({Value()}), Row({Value::DateFromDays(10)}),
      Row({Value::DateFromDays(10)}), Row({Value::DateFromDays(20)})};
  const std::vector<Row> right_rows{
      Row({Value()}), Row({Value::DateFromDays(10)}),
      Row({Value::DateFromDays(20)}), Row({Value::DateFromDays(20)}),
      Row({Value::DateFromDays(30)})};
  BandJoin join(Source(left_rows), {0}, Source(right_rows), {0},
                BinaryOperation::kLessThan,
                DateComparison(BinaryOperation::kLessThan), combined,
                JoinKind::kInner, 1, 1, combined);
  const std::vector<Row> output = Drain(&join);

  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  ASSERT_EQ(output.size(), 7U);
  for (size_t i = 0; i < 6; ++i) {
    EXPECT_EQ(output[i][0], Value::DateFromDays(10));
  }
  EXPECT_EQ(output[6][0], Value::DateFromDays(20));
  EXPECT_EQ(output[0][1], Value::DateFromDays(20));
  EXPECT_EQ(output[1][1], Value::DateFromDays(20));
  EXPECT_EQ(output[2][1], Value::DateFromDays(30));
  EXPECT_EQ(output[3][1], Value::DateFromDays(20));
  EXPECT_EQ(output[4][1], Value::DateFromDays(20));
  EXPECT_EQ(output[5][1], Value::DateFromDays(30));
  EXPECT_EQ(output[6][1], Value::DateFromDays(30));
}

TEST(BandJoinTest, EmptyInputsAndOuterPaddingKeepWidthAndStatus) {
  const Schema left_schema = IntSchema("l", "x");
  const Schema right_schema = IntSchema("r", "x");
  const Schema combined = left_schema + right_schema;
  const Expression predicate = IntComparison(BinaryOperation::kLessThan);
  BandJoin empty(Source({}), {0}, Source({Row({Value(1)})}), {0},
                 BinaryOperation::kLessThan, predicate, combined,
                 JoinKind::kInner, 1, 1, combined);
  EXPECT_TRUE(Drain(&empty).empty());
  EXPECT_EQ(empty.GetStatus(), Status::kSuccess);

  BandJoin left_outer(Source({Row({Value(1)})}), {0}, Source({}), {0},
                      BinaryOperation::kLessThan, predicate, combined,
                      JoinKind::kLeftOuter, 1, 1, combined);
  const std::vector<Row> padded = Drain(&left_outer);
  ASSERT_EQ(padded.size(), 1U);
  EXPECT_EQ(padded[0], (Row({Value(1), Value()})));
  EXPECT_EQ(left_outer.GetStatus(), Status::kSuccess);
}

TEST(BandJoinTest, FullOuterPaddingUsesBothUnmatchedSides) {
  const Schema left_schema = IntSchema("l", "x");
  const Schema right_schema = IntSchema("r", "x");
  const Schema combined = left_schema + right_schema;
  BandJoin join(Source({Row({Value(1)}), Row({Value(3)})}), {0},
                Source({Row({Value(2)}), Row({Value(4)})}), {0},
                BinaryOperation::kGreaterThan,
                IntComparison(BinaryOperation::kGreaterThan), combined,
                JoinKind::kFullOuter, 1, 1, combined);
  const std::vector<Row> output = Drain(&join);
  ASSERT_EQ(output.size(), 3U);
  EXPECT_EQ(output[0], (Row({Value(1), Value()})));
  EXPECT_EQ(output[1], (Row({Value(3), Value(2)})));
  EXPECT_EQ(output[2], (Row({Value(), Value(4)})));
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
}

TEST(BandJoinTest, BatchPathMatchesRowPathAndPreservesPositions) {
  const Schema left_schema = IntSchema("l", "x");
  const Schema right_schema = IntSchema("r", "x");
  const Schema combined = left_schema + right_schema;
  const Expression predicate = IntComparison(BinaryOperation::kLessThan);
  const std::vector<Row> left_rows{Row({Value(1)}), Row({Value(2)}),
                                   Row({Value(3)})};
  const std::vector<Row> right_rows{Row({Value(2)}), Row({Value(3)}),
                                    Row({Value(4)})};
  const auto make_join = [&]() {
    return std::make_shared<BandJoin>(
        Source(left_rows), std::vector<slot_t>{0}, Source(right_rows),
        std::vector<slot_t>{0}, BinaryOperation::kLessThan, predicate, combined,
        JoinKind::kInner, 1, 1, combined);
  };
  auto row_join = make_join();
  const std::vector<Row> row_output = Drain(row_join.get());
  auto batch_join = make_join();
  DataChunk chunk(combined);
  std::vector<Row> batch_output;
  while (true) {
    const size_t count = batch_join->NextBatch(&chunk, 2);
    for (size_t i = 0; i < count; ++i) {
      batch_output.push_back(chunk.RowAt(i));
    }
    if (count < 2) {
      break;
    }
  }
  EXPECT_EQ(batch_join->GetStatus(), Status::kSuccess);
  EXPECT_EQ(batch_output, row_output);
  EXPECT_EQ(batch_output.size(), 6U);
}

TEST(BandJoinTest, ChildAndPredicateErrorsAreSticky) {
  const Schema left_schema = IntSchema("l", "x");
  const Schema right_schema = IntSchema("r", "x");
  const Schema combined = left_schema + right_schema;
  const Expression predicate = IntComparison(BinaryOperation::kLessThan);
  BandJoin child_error(std::make_shared<FailingSource>(), {0},
                       Source({Row({Value(2)})}), {0},
                       BinaryOperation::kLessThan, predicate, combined,
                       JoinKind::kInner, 1, 1, combined);
  Row row;
  RowPosition position;
  EXPECT_FALSE(child_error.Next(&row, &position));
  EXPECT_EQ(child_error.GetStatus(), Status::kRuntimeError);

  const Expression error_predicate = BinaryExpressionExp(
      predicate, BinaryOperation::kAnd, std::make_shared<ErrorExpression>());
  BandJoin predicate_error(Source({Row({Value(1)})}), {0},
                           Source({Row({Value(2)})}), {0},
                           BinaryOperation::kLessThan, error_predicate,
                           combined, JoinKind::kInner, 1, 1, combined);
  EXPECT_FALSE(predicate_error.Next(&row, &position));
  EXPECT_EQ(predicate_error.GetStatus(), Status::kInvalidArgument);
}

TEST(BandJoinTest, UnsortedInputReturnsError) {
  const Schema left_schema = IntSchema("l", "x");
  const Schema right_schema = IntSchema("r", "x");
  const Schema combined = left_schema + right_schema;
  const Expression predicate = IntComparison(BinaryOperation::kLessThan);
  BandJoin join(Source({Row({Value(2)}), Row({Value(1)})}), {0},
                Source({Row({Value(3)})}), {0}, BinaryOperation::kLessThan,
                predicate, combined, JoinKind::kInner, 1, 1, combined);
  Row row;
  RowPosition position;
  EXPECT_FALSE(join.Next(&row, &position));
  EXPECT_EQ(join.GetStatus(), Status::kInvalidArgument);
}

TEST(BandJoinTest, PlanRuleAddsSortsAndFixedOutputContract) {
  const Schema left_schema = IntSchema("l", "x");
  const Schema right_schema = IntSchema("r", "x");
  const std::vector<Row> left_rows{Row({Value(1)}), Row({Value(2)}),
                                   Row({Value(3)})};
  const std::vector<Row> right_rows{Row({Value(2)}), Row({Value(3)}),
                                    Row({Value(4)})};
  auto left = std::make_shared<ValuesPlan>(left_schema, left_rows);
  auto right = std::make_shared<ValuesPlan>(right_schema, right_rows);
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"l", "r"});
  cascades::LogicalExpression logical = memo.Get(root).expressions.front();
  logical.predicate = IntComparison(BinaryOperation::kLessThan);
  const std::vector<cascades::BestPlan> children{
      cascades::BestPlan{.plan = left, .estimated_rows = 3.0},
      cascades::BestPlan{.plan = right, .estimated_rows = 3.0}};
  const cascades::ImplementationRule* rule = nullptr;
  for (const auto& candidate : DefaultImplementationRules().Rules()) {
    if (candidate.Name() == "band_join") {
      rule = &candidate;
      break;
    }
  }
  ASSERT_NE(rule, nullptr);
  const std::vector<cascades::PlanAlternative> alternatives =
      rule->Apply(memo, root, logical, children, cascades::PhysicalProperties{},
                  cascades::RuleContext::Empty());
  ASSERT_EQ(alternatives.size(), 1U);
  const auto band =
      std::dynamic_pointer_cast<BandJoinPlan>(alternatives.front().plan);
  ASSERT_NE(band, nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<SortPlan>(band->Left()), nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<SortPlan>(band->Right()), nullptr);
  EXPECT_TRUE(
      band->IsOrderedBy({ColumnValueExp(ColumnName("l", "x"))}, {true}));
  EXPECT_EQ(band->GetSchema().ColumnCount(), 2U);
  EXPECT_EQ(band->Comparison(), BinaryOperation::kLessThan);
  EXPECT_EQ(alternatives.front().estimated_rows, 5.0);

  Plan left_plan = left;
  Plan right_plan = right;
  Plan fixed = std::make_shared<BandJoinPlan>(
      left_plan, std::vector<ColumnName>{ColumnName("l", "x")}, right_plan,
      std::vector<ColumnName>{ColumnName("r", "x")}, JoinKind::kInner,
      BinaryOperation::kLessThan, IntComparison(BinaryOperation::kLessThan),
      3U);
  EXPECT_EQ(fixed->EmitRowCount(), 3U);
}

TEST(BandJoinTest, RuleRejectsUnsupportedBandShapes) {
  const Schema int_left = IntSchema("l", "x");
  const Schema int_right = IntSchema("r", "x");
  const Schema double_left("l", {Column("x", ValueType::kDouble)});
  const Schema double_right("r", {Column("x", ValueType::kDouble)});
  const cascades::ImplementationRule* rule = nullptr;
  for (const auto& candidate : DefaultImplementationRules().Rules()) {
    if (candidate.Name() == "band_join") {
      rule = &candidate;
      break;
    }
  }
  ASSERT_NE(rule, nullptr);
  const auto apply = [&](const Schema& left_schema, const Schema& right_schema,
                         const Expression& predicate) {
    Plan left = std::make_shared<ValuesPlan>(left_schema,
                                             std::vector<Row>{Row({Value(1)})});
    Plan right = std::make_shared<ValuesPlan>(
        right_schema, std::vector<Row>{Row({Value(2)})});
    cascades::Memo memo;
    const cascades::GroupId root = memo.Build({"l", "r"});
    cascades::LogicalExpression logical = memo.Get(root).expressions.front();
    logical.predicate = predicate;
    const std::vector<cascades::BestPlan> children{
        cascades::BestPlan{.plan = left, .estimated_rows = 1.0},
        cascades::BestPlan{.plan = right, .estimated_rows = 1.0}};
    return rule->Apply(memo, root, logical, children,
                       cascades::PhysicalProperties{},
                       cascades::RuleContext::Empty());
  };
  EXPECT_TRUE(apply(double_left, double_right,
                    IntComparison(BinaryOperation::kLessThan))
                  .empty());
  const Expression conjunction = BinaryExpressionExp(
      IntComparison(BinaryOperation::kLessThan), BinaryOperation::kAnd,
      IntComparison(BinaryOperation::kGreaterThan));
  EXPECT_TRUE(apply(int_left, int_right, conjunction).empty());
  EXPECT_TRUE(
      apply(int_left, int_right, IntComparison(BinaryOperation::kEquals))
          .empty());
}

// join_commutativity registers both child orders in the same group, so the
// band_join rule can be handed children whose order is the reverse of the one
// the predicate is written in. The spec's comparison has to be re-expressed in
// the CHILD order, because the executor derives its candidate range from it
// (the residual predicate is re-evaluated by column name and is therefore
// orientation-independent). Reading the comparison as written made the range
// and the re-check disagree, so a `l.x <= r.x` join planned with (r, l)
// children silently lost rows.
TEST(BandJoinTest, RuleMirrorsComparisonForSwappedChildOrder) {
  const Schema int_left = IntSchema("l", "x");
  const Schema int_right = IntSchema("r", "x");
  const cascades::ImplementationRule* rule = nullptr;
  for (const auto& candidate : DefaultImplementationRules().Rules()) {
    if (candidate.Name() == "band_join") {
      rule = &candidate;
      break;
    }
  }
  ASSERT_NE(rule, nullptr);
  // left = (r.x), right = (l.x): the reverse of the predicate's operands.
  Plan left = std::make_shared<ValuesPlan>(
      int_right, std::vector<Row>{Row({Value(1)}), Row({Value(3)})});
  Plan right = std::make_shared<ValuesPlan>(
      int_left, std::vector<Row>{Row({Value(2)}), Row({Value(4)})});
  const auto apply = [&](BinaryOperation written) {
    cascades::Memo memo;
    const cascades::GroupId root = memo.Build({"r", "l"});
    cascades::LogicalExpression logical = memo.Get(root).expressions.front();
    logical.predicate = IntComparison(written);
    const std::vector<cascades::BestPlan> children{
        cascades::BestPlan{.plan = left, .estimated_rows = 2.0},
        cascades::BestPlan{.plan = right, .estimated_rows = 2.0}};
    return rule->Apply(memo, memo.Get(root).expressions.front().children[1],
                       logical, children, cascades::PhysicalProperties{},
                       cascades::RuleContext::Empty());
  };

  // `l.x <= r.x` over children (r, l) means `r.x >= l.x`.
  {
    const auto alternatives = apply(BinaryOperation::kLessThanEquals);
    ASSERT_EQ(alternatives.size(), 1U);
    const auto band =
        std::dynamic_pointer_cast<BandJoinPlan>(alternatives.front().plan);
    ASSERT_NE(band, nullptr);
    EXPECT_EQ(band->LeftKeys().front(), ColumnName("r", "x"));
    EXPECT_EQ(band->RightKeys().front(), ColumnName("l", "x"));
    EXPECT_EQ(band->Comparison(), BinaryOperation::kGreaterThanEquals);
    // The plan must be ordered by each child's own key, not by the predicate's
    // left-to-right order.
    EXPECT_NE(std::dynamic_pointer_cast<SortPlan>(band->Left()), nullptr);
    EXPECT_NE(std::dynamic_pointer_cast<SortPlan>(band->Right()), nullptr);
  }
  // The remaining operators mirror the same way.
  for (const auto& [written, expected] :
       {std::pair{BinaryOperation::kLessThan, BinaryOperation::kGreaterThan},
        std::pair{BinaryOperation::kGreaterThan, BinaryOperation::kLessThan},
        std::pair{BinaryOperation::kGreaterThanEquals,
                  BinaryOperation::kLessThanEquals}}) {
    const auto band =
        std::dynamic_pointer_cast<BandJoinPlan>(apply(written).front().plan);
    ASSERT_NE(band, nullptr) << static_cast<int>(written);
    EXPECT_EQ(band->Comparison(), expected) << static_cast<int>(written);
  }
}

// End-to-end shape of the regression: the same predicate planned with the
// children in the opposite order must return the same pairs as the
// non-swapped orientation.
TEST(BandJoinTest, SwappedChildOrderReturnsTheSamePairs) {
  const Schema l_schema = IntSchema("l", "x");
  const Schema r_schema = IntSchema("r", "x");
  // The seed-17 oracle shape: l = (2, 4) on the left of the predicate, r = (1,
  // 3) on its right.
  const std::vector<Row> l_rows{Row({Value(2)}), Row({Value(4)})};
  const std::vector<Row> r_rows{Row({Value(1)}), Row({Value(3)})};
  const Expression predicate = IntComparison(BinaryOperation::kLessThanEquals);
  // Written order: (l, r) with `l.x <= r.x` -> 2<=3, 4<=3 is false, 2<=1 is
  // false, 4<=1 is false -> one pair.
  {
    const Schema combined = l_schema + r_schema;
    BandJoin join(Source(l_rows), {0}, Source(r_rows), {0},
                  BinaryOperation::kLessThanEquals, predicate, combined,
                  JoinKind::kInner, 1, 1, combined);
    const std::vector<Row> output = Drain(&join);
    ASSERT_EQ(output.size(), 1U);
    EXPECT_EQ(output[0], (Row({Value(2), Value(3)})));
    EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  }
  // Swapped order: left = r, right = l, and the operator mirrored to `>`.
  {
    const Schema combined = r_schema + l_schema;
    BandJoin join(Source(r_rows), {0}, Source(l_rows), {0},
                  BinaryOperation::kGreaterThanEquals, predicate, combined,
                  JoinKind::kInner, 1, 1, combined);
    const std::vector<Row> output = Drain(&join);
    ASSERT_EQ(output.size(), 1U);
    // The row is emitted as (left, right) = (r.x, l.x).
    EXPECT_EQ(output[0], (Row({Value(3), Value(2)})));
    EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  }
}

TEST(BandJoinTest, FactoryEmitsBandJoinFromPlan) {
  const Schema left_schema = IntSchema("l", "x");
  const Schema right_schema = IntSchema("r", "x");
  auto left = std::make_shared<ValuesPlan>(
      left_schema, std::vector<Row>{Row({Value(1)}), Row({Value(2)})});
  auto right = std::make_shared<ValuesPlan>(
      right_schema, std::vector<Row>{Row({Value(2)}), Row({Value(3)})});
  Plan left_plan = left;
  Plan right_plan = right;
  Plan plan = std::make_shared<BandJoinPlan>(
      left_plan, std::vector<ColumnName>{ColumnName("l", "x")}, right_plan,
      std::vector<ColumnName>{ColumnName("r", "x")}, JoinKind::kInner,
      BinaryOperation::kLessThan, IntComparison(BinaryOperation::kLessThan),
      2U);
  ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Database>, database,
                        Database::Create("band_join_test-" + RandomString()));
  TransactionContext context = database->BeginContext();
  Executor executor = plan->EmitExecutor(context);
  EXPECT_NE(std::dynamic_pointer_cast<BandJoin>(executor), nullptr);
  const std::vector<Row> output = Drain(executor.get());
  EXPECT_EQ(IntPairs(output),
            (std::vector<std::pair<int64_t, int64_t>>{{1, 2}, {1, 3}, {2, 3}}));
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
  EXPECT_TRUE(context.PreCommit().ok());
  database->DeleteAll();
}

}  // namespace tinylamb
