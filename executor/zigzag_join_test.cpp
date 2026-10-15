#include "executor/zigzag_join.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/constant_executor.hpp"
#include "executor/data_chunk.hpp"
#include "expression/binary_expression.hpp"
#include "expression/expression.hpp"
#include "expression/unary_expression.hpp"
#include "index/index_schema.hpp"
#include "plan/cascades.hpp"
#include "plan/implementation_rules.hpp"
#include "plan/index_only_scan_plan.hpp"
#include "plan/index_scan_plan.hpp"
#include "plan/values_plan.hpp"
#include "plan/zigzag_join_plan.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

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

Schema IntSchema(const std::string& name, const std::string& first,
                 const std::string& second = {}) {
  std::vector<Column> columns;
  columns.emplace_back(first, ValueType::kInt64);
  if (!second.empty()) {
    columns.emplace_back(second, ValueType::kInt64);
  }
  return {name, std::move(columns)};
}

Expression Comparison(BinaryOperation operation) {
  return BinaryExpressionExp(ColumnValueExp(ColumnName("l", "k")), operation,
                             ColumnValueExp(ColumnName("r", "k")));
}

void InsertRows(TransactionContext& context, Table& table,
                std::initializer_list<Row> rows) {
  for (const Row& row : rows) {
    ASSERT_SUCCESS(table.Insert(context.txn_, row).GetStatus());
  }
}

class PositionedSource final : public ExecutorBase {
 public:
  PositionedSource(std::vector<Row> rows, std::vector<RowPosition> positions)
      : rows_(std::move(rows)), positions_(std::move(positions)) {}

  bool Next(Row* row, RowPosition* position) override {
    if (offset_ >= rows_.size()) {
      return false;
    }
    *row = std::move(rows_[offset_]);
    if (position != nullptr) {
      *position = positions_[offset_];
    }
    ++offset_;
    return true;
  }

  void Dump(std::ostream& output, int /*indent*/) const override {
    output << "PositionedSource";
  }

 private:
  std::vector<Row> rows_;
  std::vector<RowPosition> positions_;
  size_t offset_{0};
};

class FailingSource final : public ExecutorBase {
 public:
  bool Next(Row* /*row*/, RowPosition* /*position*/) override { return false; }
  void Dump(std::ostream& output, int /*indent*/) const override {
    output << "FailingSource";
  }
  [[nodiscard]] Status GetStatus() const override {
    return {StatusCode::kRuntimeError, "failing source"};
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
    return StatusError(StatusCode::kInvalidArgument,
                       "zigzag predicate failure");
  }
  [[nodiscard]] std::string ToString() const override { return "ERROR()"; }
  void Dump(std::ostream& /*output*/) const override {}
};

TEST(ZigZagJoinTest, EqualityIsLeftMajorAndPreservesDuplicateMultiplicity) {
  const Schema left_schema = IntSchema("l", "k", "id");
  const Schema right_schema = IntSchema("r", "k", "id");
  const Schema output_schema = left_schema + right_schema;
  const std::vector<Row> left_rows{
      Row({Value(), Value(0)}), Row({Value(1), Value(10)}),
      Row({Value(1), Value(11)}), Row({Value(3), Value(30)})};
  const std::vector<Row> right_rows{
      Row({Value(), Value(0)}), Row({Value(1), Value(20)}),
      Row({Value(1), Value(21)}), Row({Value(2), Value(22)}),
      Row({Value(3), Value(40)})};
  const std::vector<RowPosition> left_positions{
      RowPosition(10, 0), RowPosition(11, 0), RowPosition(11, 1),
      RowPosition(12, 0)};
  const std::vector<RowPosition> right_positions{
      RowPosition(20, 0), RowPosition(21, 0), RowPosition(21, 1),
      RowPosition(21, 2), RowPosition(22, 0)};
  ZigZagJoin join(
      std::make_shared<PositionedSource>(left_rows, left_positions), 0,
      std::make_shared<PositionedSource>(right_rows, right_positions), 0,
      BinaryOperation::kEquals, Comparison(BinaryOperation::kEquals),
      output_schema);

  std::vector<Row> output;
  std::vector<RowPosition> positions;
  Row row;
  RowPosition position;
  while (join.Next(&row, &position)) {
    output.push_back(row);
    positions.push_back(position);
  }

  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  ASSERT_EQ(output.size(), 5U);
  EXPECT_EQ(output[0], (Row({Value(1), Value(10), Value(1), Value(20)})));
  EXPECT_EQ(output[1], (Row({Value(1), Value(10), Value(1), Value(21)})));
  EXPECT_EQ(output[2], (Row({Value(1), Value(11), Value(1), Value(20)})));
  EXPECT_EQ(output[3], (Row({Value(1), Value(11), Value(1), Value(21)})));
  EXPECT_EQ(output[4], (Row({Value(3), Value(30), Value(3), Value(40)})));
  EXPECT_EQ(positions,
            (std::vector<RowPosition>{RowPosition(11, 0), RowPosition(11, 0),
                                      RowPosition(11, 1), RowPosition(11, 1),
                                      RowPosition(12, 0)}));
  std::stringstream dump;
  join.Dump(dump, 0);
  EXPECT_NE(dump.str().find("ZigZagJoin"), std::string::npos);
}

TEST(ZigZagJoinTest, EveryIntegerRangeMatchesNestedLoopSemantics) {
  const Schema left_schema = IntSchema("l", "k");
  const Schema right_schema = IntSchema("r", "k");
  const Schema output_schema = left_schema + right_schema;
  const std::vector<Row> left_rows{Row({Value()}), Row({Value(1)}),
                                   Row({Value(1)}), Row({Value(3)}),
                                   Row({Value(5)})};
  const std::vector<Row> right_rows{Row({Value()}),  Row({Value(1)}),
                                    Row({Value(2)}), Row({Value(3)}),
                                    Row({Value(3)}), Row({Value(6)})};
  for (const BinaryOperation operation :
       {BinaryOperation::kLessThan, BinaryOperation::kLessThanEquals,
        BinaryOperation::kGreaterThan, BinaryOperation::kGreaterThanEquals}) {
    std::vector<Row> expected_rows;
    for (const Row& left_row : left_rows) {
      if (left_row[0].IsNull()) {
        continue;
      }
      for (const Row& right_row : right_rows) {
        if (right_row[0].IsNull()) {
          continue;
        }
        const int64_t left = left_row[0].value.int_value;
        const int64_t right = right_row[0].value.int_value;
        const bool keep =
            (operation == BinaryOperation::kLessThan && left < right) ||
            (operation == BinaryOperation::kLessThanEquals && left <= right) ||
            (operation == BinaryOperation::kGreaterThan && left > right) ||
            (operation == BinaryOperation::kGreaterThanEquals && left >= right);
        if (keep) {
          expected_rows.emplace_back(Row({Value(left), Value(right)}));
        }
      }
    }
    ZigZagJoin join(Source(left_rows), 0, Source(right_rows), 0, operation,
                    Comparison(operation), output_schema);
    EXPECT_EQ(Drain(&join), expected_rows) << static_cast<int>(operation);
    EXPECT_EQ(join.GetStatus(), Status::kSuccess)
        << static_cast<int>(operation);
  }
}

TEST(ZigZagJoinTest, EmptyAndBatchPathsAreStable) {
  const Schema left_schema = IntSchema("l", "k");
  const Schema right_schema = IntSchema("r", "k");
  const Schema output_schema = left_schema + right_schema;
  const std::vector<Row> left_rows{Row({Value(1)}), Row({Value(2)}),
                                   Row({Value(3)})};
  const std::vector<Row> right_rows{Row({Value(2)}), Row({Value(3)}),
                                    Row({Value(4)})};
  const auto make_join = [&]() {
    return std::make_shared<ZigZagJoin>(
        Source(left_rows), 0, Source(right_rows), 0, BinaryOperation::kLessThan,
        Comparison(BinaryOperation::kLessThan), output_schema);
  };
  auto row_join = make_join();
  const std::vector<Row> row_output = Drain(row_join.get());
  auto batch_join = make_join();
  DataChunk chunk(output_schema);
  std::vector<Row> batch_output;
  while (true) {
    const size_t count = batch_join->NextBatch(&chunk, 2);
    for (size_t i = 0; i < count; ++i) {
      batch_output.push_back(chunk.RowAt(i));
    }
    if (count == 0) {
      break;
    }
  }
  EXPECT_EQ(batch_join->GetStatus(), Status::kSuccess);
  EXPECT_EQ(batch_output, row_output);
  EXPECT_EQ(batch_output.size(), 6U);

  ZigZagJoin empty_left(Source({}), 0, Source(right_rows), 0,
                        BinaryOperation::kLessThan,
                        Comparison(BinaryOperation::kLessThan), output_schema);
  EXPECT_TRUE(Drain(&empty_left).empty());
  EXPECT_EQ(empty_left.GetStatus(), Status::kSuccess);
  ZigZagJoin empty_right(Source(left_rows), 0, Source({}), 0,
                         BinaryOperation::kLessThan,
                         Comparison(BinaryOperation::kLessThan), output_schema);
  EXPECT_TRUE(Drain(&empty_right).empty());
  EXPECT_EQ(empty_right.GetStatus(), Status::kSuccess);
}

TEST(ZigZagJoinTest, UnsupportedTypeUnsortedInputAndPredicateErrorFail) {
  const Schema left_schema = IntSchema("l", "k");
  const Schema right_schema = IntSchema("r", "k");
  const Schema output_schema = left_schema + right_schema;
  Row row;
  RowPosition position;

  ZigZagJoin unsorted(Source({Row({Value(2)}), Row({Value(1)})}), 0,
                      Source({Row({Value(2)})}), 0, BinaryOperation::kEquals,
                      Comparison(BinaryOperation::kEquals), output_schema);
  while (unsorted.Next(&row, &position)) {
  }
  EXPECT_EQ(unsorted.GetStatus(), Status::kInvalidArgument);

  const Schema double_left("l", {Column("k", ValueType::kDouble)});
  const Schema double_right("r", {Column("k", ValueType::kDouble)});
  const Expression double_predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("l", "k")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("r", "k")));
  ZigZagJoin unsupported(
      Source({Row({Value(1.0)})}), 0, Source({Row({Value(1.0)})}), 0,
      BinaryOperation::kEquals, double_predicate, double_left + double_right);
  EXPECT_FALSE(unsupported.Next(&row, &position));
  EXPECT_EQ(unsupported.GetStatus(), Status::kInvalidArgument);

  const Expression error_predicate = BinaryExpressionExp(
      Comparison(BinaryOperation::kEquals), BinaryOperation::kAnd,
      std::make_shared<ErrorExpression>());
  ZigZagJoin predicate_error(
      Source({Row({Value(1)})}), 0, Source({Row({Value(1)})}), 0,
      BinaryOperation::kEquals, error_predicate, output_schema);
  EXPECT_FALSE(predicate_error.Next(&row, &position));
  EXPECT_EQ(predicate_error.GetStatus(), Status::kInvalidArgument);

  ZigZagJoin child_error(std::make_shared<FailingSource>(), 0,
                         Source({Row({Value(1)})}), 0, BinaryOperation::kEquals,
                         Comparison(BinaryOperation::kEquals), output_schema);
  EXPECT_FALSE(child_error.Next(&row, &position));
  EXPECT_EQ(child_error.GetStatus(), Status::kRuntimeError);
}

TEST(ZigZagJoinTest, RuleRequiresTwoOrderedIndexesAndRejectsResiduals) {
  ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Database>, database,
                        Database::Create("zigzag_join_test-" + RandomString()));
  TransactionContext setup = database->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, left_table,
      database->CreateTable(setup, IntSchema("ZigLeft", "k")));
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_table,
      database->CreateTable(setup, IntSchema("ZigRight", "k")));
  ASSERT_SUCCESS(setup.txn_.PreCommit());
  TransactionContext stats_context = database->BeginContext();
  database->RefreshStatistics(stats_context, "ZigLeft");
  database->RefreshStatistics(stats_context, "ZigRight");
  ASSERT_SUCCESS(stats_context.PreCommit());
  TransactionContext context = database->BeginContext();
  ASSERT_SUCCESS(database->CreateIndex(
      context, "ZigLeft",
      IndexSchema("zig_left_k", {0}, {}, IndexMode::kNonUnique)));
  ASSERT_SUCCESS(database->CreateIndex(
      context, "ZigRight",
      IndexSchema("zig_right_k", {0}, {}, IndexMode::kNonUnique)));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, indexed_left,
                        context.GetTable("ZigLeft"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, indexed_right,
                        context.GetTable("ZigRight"));
  InsertRows(context, *indexed_left,
             {{Value()}, {Value(1)}, {Value(1)}, {Value(2)}});
  InsertRows(context, *indexed_right,
             {{Value()}, {Value(1)}, {Value(1)}, {Value(3)}});
  ASSERT_SUCCESS(context.txn_.PreCommit());

  TransactionContext read_context = database->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, left_shared,
                        read_context.GetTable("ZigLeft"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, right_shared,
                        read_context.GetTable("ZigRight"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, left_stats,
                        read_context.GetStats("ZigLeft"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, right_stats,
                        read_context.GetStats("ZigRight"));
  const ColumnName left_key("ZigLeft", "k");
  const ColumnName right_key("ZigRight", "k");
  const auto left_index_plan = std::make_shared<IndexOnlyScanPlan>(
      *left_shared, left_shared->GetIndex(0), *left_stats, std::vector<Value>{},
      std::vector<Value>{}, true, ConstantValueExp(Value(true)),
      std::vector<ColumnName>{left_key});
  const auto right_index_plan = std::make_shared<IndexOnlyScanPlan>(
      *right_shared, right_shared->GetIndex(0), *right_stats,
      std::vector<Value>{}, std::vector<Value>{}, true,
      ConstantValueExp(Value(true)), std::vector<ColumnName>{right_key});

  const cascades::ImplementationRule* rule = nullptr;
  for (const auto& candidate : DefaultImplementationRules().Rules()) {
    if (candidate.Name() == "zigzag_join") {
      rule = &candidate;
      break;
    }
  }
  ASSERT_NE(rule, nullptr);
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"ZigLeft", "ZigRight"});
  cascades::LogicalExpression logical = memo.Get(root).expressions.front();
  const Expression equality =
      BinaryExpressionExp(ColumnValueExp(left_key), BinaryOperation::kEquals,
                          ColumnValueExp(right_key));
  logical.predicate = BinaryExpressionExp(
      equality, BinaryOperation::kAnd,
      UnaryExpressionExp(ColumnValueExp(left_key), UnaryOperation::kIsNotNull));
  logical.predicate =
      BinaryExpressionExp(*logical.predicate, BinaryOperation::kAnd,
                          UnaryExpressionExp(ColumnValueExp(right_key),
                                             UnaryOperation::kIsNotNull));
  const std::vector<cascades::BestPlan> children{
      cascades::BestPlan{.plan = left_index_plan, .estimated_rows = 4.0},
      cascades::BestPlan{.plan = right_index_plan, .estimated_rows = 4.0}};
  const auto alternatives =
      rule->Apply(memo, root, logical, children, cascades::PhysicalProperties{},
                  cascades::RuleContext::Empty());
  ASSERT_EQ(alternatives.size(), 1U);
  const auto plan =
      std::dynamic_pointer_cast<ZigZagJoinPlan>(alternatives.front().plan);
  ASSERT_NE(plan, nullptr);
  EXPECT_EQ(plan->Operation(), BinaryOperation::kEquals);
  EXPECT_TRUE(plan->IsOrderedBy({ColumnValueExp(left_key)}, {true}));

  Executor executor = plan->EmitExecutor(read_context);
  EXPECT_NE(std::dynamic_pointer_cast<ZigZagJoin>(executor), nullptr);
  const std::vector<Row> output = Drain(executor.get());
  ASSERT_EQ(output.size(), 4U);
  EXPECT_EQ(output[0], (Row({Value(1), Value(1)})));
  EXPECT_EQ(output[3], (Row({Value(1), Value(1)})));

  const auto left_scan_plan = std::make_shared<IndexScanPlan>(
      *left_shared, left_shared->GetIndex(0), *left_stats, std::vector<Value>{},
      std::vector<Value>{}, true, ConstantValueExp(Value(true)),
      std::vector<ColumnName>{left_key}, false, true);
  const std::vector<cascades::BestPlan> scan_children{
      cascades::BestPlan{.plan = left_scan_plan, .estimated_rows = 4.0},
      cascades::BestPlan{.plan = right_index_plan, .estimated_rows = 4.0}};
  const auto scan_alternatives = rule->Apply(memo, root, logical, scan_children,
                                             cascades::PhysicalProperties{},
                                             cascades::RuleContext::Empty());
  ASSERT_EQ(scan_alternatives.size(), 1U);
  Executor scan_executor =
      scan_alternatives.front().plan->EmitExecutor(read_context);
  EXPECT_EQ(Drain(scan_executor.get()).size(), 4U);
  EXPECT_EQ(scan_executor->GetStatus(), Status::kSuccess);

  cascades::Memo swapped_memo;
  const cascades::GroupId swapped_root =
      swapped_memo.Build({"ZigRight", "ZigLeft"});
  cascades::LogicalExpression swapped =
      swapped_memo.Get(swapped_root).expressions.front();
  swapped.predicate = BinaryExpressionExp(ColumnValueExp(left_key),
                                          BinaryOperation::kLessThanEquals,
                                          ColumnValueExp(right_key));
  const std::vector<cascades::BestPlan> swapped_children{
      cascades::BestPlan{.plan = right_index_plan, .estimated_rows = 4.0},
      cascades::BestPlan{.plan = left_index_plan, .estimated_rows = 4.0}};
  const auto swapped_alternatives = rule->Apply(
      swapped_memo, swapped_root, swapped, swapped_children,
      cascades::PhysicalProperties{}, cascades::RuleContext::Empty());
  ASSERT_EQ(swapped_alternatives.size(), 1U);
  const auto swapped_plan = std::dynamic_pointer_cast<ZigZagJoinPlan>(
      swapped_alternatives.front().plan);
  ASSERT_NE(swapped_plan, nullptr);
  EXPECT_EQ(swapped_plan->LeftKey(), right_key);
  EXPECT_EQ(swapped_plan->RightKey(), left_key);
  EXPECT_EQ(swapped_plan->Operation(), BinaryOperation::kGreaterThanEquals);
  Executor swapped_executor = swapped_plan->EmitExecutor(read_context);
  const std::vector<Row> swapped_output = Drain(swapped_executor.get());
  ASSERT_EQ(swapped_output.size(), 7U);
  EXPECT_EQ(swapped_output[0], (Row({Value(1), Value(1)})));
  EXPECT_EQ(swapped_output[3], (Row({Value(1), Value(1)})));
  EXPECT_EQ(swapped_output[4], (Row({Value(3), Value(1)})));
  EXPECT_EQ(swapped_output[6], (Row({Value(3), Value(2)})));
  EXPECT_EQ(swapped_executor->GetStatus(), Status::kSuccess);

  const auto residual =
      rule->Apply(memo, root, logical, children, cascades::PhysicalProperties{},
                  cascades::RuleContext::Empty());
  ASSERT_EQ(residual.size(), 1U);
  cascades::LogicalExpression compound = logical;
  compound.predicate = BinaryExpressionExp(
      equality, BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("ZigLeft", "k")),
                          BinaryOperation::kNotEquals,
                          ConstantValueExp(Value(1))));
  EXPECT_TRUE(rule->Apply(memo, root, compound, children,
                          cascades::PhysicalProperties{},
                          cascades::RuleContext::Empty())
                  .empty());

  ASSERT_SUCCESS(read_context.PreCommit());
  database->DeleteAll();
}

}  // namespace
}  // namespace tinylamb
