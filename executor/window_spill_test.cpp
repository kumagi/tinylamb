/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
// The operator half of the window memory shapes (executor/window.hpp): the
// buffered shape keeps the whole input and emits it in input order, the
// partitioned spill shape routes the drained input through PARTITION BY hash
// spill files and keeps one file resident at a time.
//
// The contract under test:
//   * spilling never changes an answer: the same rows and the same per-row
//     window values come out, in any order (the plan gives up the ordering
//     claim for this shape, see plan/window_shape_test.cpp);
//   * the buffered shape keeps the child's row order, which is what its
//     ordering claim rests on;
//   * the resident set is bounded by one spill file, not by the input;
//   * the batched path crosses spill-file boundaries without losing a row;
//   * calls that do not share one PARTITION BY list stay buffered, because a
//     file chosen from one call's key would cut another call's partition.
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/data_chunk.hpp"
#include "executor/values.hpp"
#include "executor/window.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "page/row_position.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

constexpr int64_t kRows = 120;
constexpr int64_t kGroups = 3;
constexpr int64_t kPerGroup = kRows / kGroups;

std::shared_ptr<WindowFunctionCallExpression> RowNumberByGroup() {
  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = "ROW_NUMBER";
  window->partition_by = {ColumnValueExp(ColumnName("g"))};
  WindowOrderTerm term;
  term.expression = ColumnValueExp(ColumnName("k"));
  window->order_by = {term};
  return window;
}

std::shared_ptr<WindowFunctionCallExpression> SumByGroup() {
  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = "SUM";
  window->args = {ColumnValueExp(ColumnName("k"))};
  window->partition_by = {ColumnValueExp(ColumnName("g"))};
  return window;
}

std::shared_ptr<WindowFunctionCallExpression> SumByKey() {
  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = "SUM";
  window->args = {ColumnValueExp(ColumnName("k"))};
  window->partition_by = {ColumnValueExp(ColumnName("k"))};
  return window;
}

// Input laid out so a group's rows are never contiguous: g = k % 3.
std::vector<Row> InputRows() {
  std::vector<Row> rows;
  rows.reserve(static_cast<size_t>(kRows));
  for (int64_t k = 0; k < kRows; ++k) {
    rows.emplace_back(Row({Value(k % kGroups), Value(k)}));
  }
  return rows;
}

Schema WindowSchema(size_t output_count) {
  std::vector<Column> columns = {Column("g", ValueType::kInt64),
                                 Column("k", ValueType::kInt64)};
  for (size_t i = 0; i < output_count; ++i) {
    columns.emplace_back(Column("$win" + std::to_string(i), ValueType::kNull));
  }
  return Schema("w", std::move(columns));
}

// SUM(k) over each g: k runs g, g+3, ... so the sum is per_group*g plus
// kGroups*(0+..+per_group-1).
int64_t ExpectedGroupSum(int64_t g) {
  return kPerGroup * g + kGroups * ((kPerGroup - 1) * kPerGroup / 2);
}

}  // namespace

class WindowSpillTest : public ::testing::Test {
 protected:
  void SetUp() override {
    database_ = Database::Create("window_spill-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  }
  void TearDown() override {
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  static std::vector<Row> Drain(ExecutorBase* exec) {
    std::vector<Row> rows;
    Row row;
    RowPosition rp;
    while (exec->Next(&row, &rp)) {
      rows.push_back(row);
    }
    EXPECT_EQ(exec->GetStatus(), Status::kSuccess);
    return rows;
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
};

TEST_F(WindowSpillTest, PartitionedSpillMatchesTheBufferedShape) {
  std::vector<NamedExpression> outputs{
      NamedExpression("$win0", Expression(RowNumberByGroup())),
      NamedExpression("$win1", Expression(SumByGroup()))};
  const Schema schema = WindowSchema(outputs.size());

  auto buffered = std::make_shared<WindowExecutor>(
      *context_, std::make_shared<ValuesExecutor>(InputRows()), schema,
      outputs);
  EXPECT_FALSE(buffered->SpillEnabled());
  const std::vector<Row> buffered_rows = Drain(buffered.get());
  ASSERT_EQ(buffered_rows.size(), static_cast<size_t>(kRows));

  auto spilled = std::make_shared<WindowExecutor>(
      *context_, std::make_shared<ValuesExecutor>(InputRows()), schema, outputs,
      WindowExecutor::Options{.memory_budget_bytes = 1,
                              .spill_partition_count = 4});
  EXPECT_TRUE(spilled->SpillEnabled());
  const std::vector<Row> spilled_rows = Drain(spilled.get());
  ASSERT_EQ(spilled_rows.size(), static_cast<size_t>(kRows));

  // The buffered shape still walks the child's rows in order.
  for (size_t i = 0; i < buffered_rows.size(); ++i) {
    EXPECT_EQ(buffered_rows[i][0], Value(static_cast<int64_t>(i) % kGroups));
    EXPECT_EQ(buffered_rows[i][1], Value(static_cast<int64_t>(i)));
  }

  // The spilled shape reorders rows by file, so compare row by row after
  // keying on (g, k): same rows, same computed values, no duplicates.
  std::map<std::pair<int64_t, int64_t>, std::pair<int64_t, int64_t>> got;
  for (const Row& row : spilled_rows) {
    const int64_t g = row[0].value.int_value;
    const int64_t k = row[1].value.int_value;
    const bool inserted = got.emplace(std::make_pair(g, k),
                                      std::make_pair(row[2].value.int_value,
                                                     row[3].value.int_value))
                              .second;
    EXPECT_TRUE(inserted) << "duplicate row for g=" << g << " k=" << k;
  }
  EXPECT_EQ(got.size(), static_cast<size_t>(kRows));
  for (int64_t k = 0; k < kRows; ++k) {
    const int64_t g = k % kGroups;
    const auto found = got.find(std::make_pair(g, k));
    ASSERT_NE(found, got.end()) << "missing row for g=" << g << " k=" << k;
    // Within a partition the order key is k, so the rank is the row's index
    // in its group.
    EXPECT_EQ(found->second.first, (k - g) / kGroups + 1) << "k=" << k;
    EXPECT_EQ(found->second.second, ExpectedGroupSum(g)) << "k=" << k;
  }
  EXPECT_EQ(buffered_rows.size(), spilled_rows.size());

  // The memory bound is one file, not the whole input.
  EXPECT_GE(spilled->SpillFileCount(), 2U);
  EXPECT_LT(spilled->PeakResidentRows(), static_cast<size_t>(kRows));

  std::ostringstream dump;
  spilled->Dump(dump, 0);
  EXPECT_NE(dump.str().find("spill=partitioned"), std::string::npos)
      << dump.str();
  EXPECT_NE(dump.str().find("resident-rows="), std::string::npos) << dump.str();
}

TEST_F(WindowSpillTest, BatchedEmissionCrossesSpillFilesWithoutLosingRows) {
  std::vector<NamedExpression> outputs{
      NamedExpression("$win0", Expression(RowNumberByGroup())),
      NamedExpression("$win1", Expression(SumByGroup()))};
  const Schema schema = WindowSchema(outputs.size());
  auto spilled = std::make_shared<WindowExecutor>(
      *context_, std::make_shared<ValuesExecutor>(InputRows()), schema, outputs,
      WindowExecutor::Options{.memory_budget_bytes = 1,
                              .spill_partition_count = 4});
  ASSERT_TRUE(spilled->SpillEnabled());

  DataChunk chunk(schema);
  std::map<std::pair<int64_t, int64_t>, std::pair<int64_t, int64_t>> got;
  size_t batches = 0;
  while (true) {
    const size_t count = spilled->NextBatch(&chunk, 7);
    if (count == 0) {
      break;
    }
    ++batches;
    for (size_t i = 0; i < count; ++i) {
      const Row row = chunk.RowAt(i);
      const bool inserted =
          got.emplace(
                 std::make_pair(row[0].value.int_value, row[1].value.int_value),
                 std::make_pair(row[2].value.int_value, row[3].value.int_value))
              .second;
      EXPECT_TRUE(inserted) << "duplicate row at batch " << batches;
    }
  }
  EXPECT_EQ(spilled->GetStatus(), Status::kSuccess);
  EXPECT_GT(batches, 1U);
  EXPECT_EQ(got.size(), static_cast<size_t>(kRows));
  for (int64_t k = 0; k < kRows; ++k) {
    const int64_t g = k % kGroups;
    const auto found = got.find(std::make_pair(g, k));
    ASSERT_NE(found, got.end()) << "missing row for g=" << g << " k=" << k;
    EXPECT_EQ(found->second.first, (k - g) / kGroups + 1);
    EXPECT_EQ(found->second.second, ExpectedGroupSum(g));
  }
}

TEST_F(WindowSpillTest, EndOfStreamIsStickyForBothShapes) {
  std::vector<NamedExpression> outputs{
      NamedExpression("$win0", Expression(RowNumberByGroup())),
      NamedExpression("$win1", Expression(SumByGroup()))};
  const Schema schema = WindowSchema(outputs.size());
  struct Shape {
    WindowExecutor::Options options;
    const char* name;
  };
  const std::vector<Shape> shapes = {
      {WindowExecutor::Options{}, "buffered"},
      {WindowExecutor::Options{.memory_budget_bytes = 1,
                               .spill_partition_count = 4},
       "spill"}};
  for (const Shape& shape : shapes) {
    SCOPED_TRACE(shape.name);
    auto exec = std::make_shared<WindowExecutor>(
        *context_, std::make_shared<ValuesExecutor>(InputRows()), schema,
        outputs, shape.options);
    size_t emitted = 0;
    Row row;
    RowPosition rp;
    while (exec->Next(&row, &rp)) {
      ++emitted;
    }
    EXPECT_EQ(emitted, static_cast<size_t>(kRows));
    // A materializing parent keeps pulling after the end (it takes an empty
    // batch as "retry"). Rewinding the buffered chunk instead of staying
    // exhausted would replay every row, so both entry points have to keep
    // answering "nothing".
    for (int attempt = 0; attempt < 3; ++attempt) {
      EXPECT_FALSE(exec->Next(&row, &rp)) << "attempt " << attempt;
      DataChunk chunk(schema);
      EXPECT_EQ(exec->NextBatch(&chunk, 8), 0U) << "attempt " << attempt;
    }
  }
}

TEST_F(WindowSpillTest, CallsWithoutOnePartitionKeyStayBuffered) {
  std::vector<NamedExpression> outputs{
      NamedExpression("$win0", Expression(RowNumberByGroup())),
      NamedExpression("$win1", Expression(SumByKey()))};
  const Schema schema = WindowSchema(outputs.size());
  auto exec = std::make_shared<WindowExecutor>(
      *context_, std::make_shared<ValuesExecutor>(InputRows()), schema, outputs,
      WindowExecutor::Options{.memory_budget_bytes = 1,
                              .spill_partition_count = 4});
  // The budget is finite but the calls disagree about the partition, so there
  // is no safe place to split the input: the buffered shape stays.
  EXPECT_FALSE(exec->SpillEnabled());
  const std::vector<Row> rows = Drain(exec.get());
  ASSERT_EQ(rows.size(), static_cast<size_t>(kRows));
  for (size_t i = 0; i < rows.size(); ++i) {
    EXPECT_EQ(rows[i][1], Value(static_cast<int64_t>(i))) << "row " << i;
    // SUM(k) PARTITION BY k over one row per key is just k.
    EXPECT_EQ(rows[i][3], Value(static_cast<int64_t>(i))) << "row " << i;
  }
}

}  // namespace tinylamb
