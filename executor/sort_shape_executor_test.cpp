/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0 license. */
// The operator half of the buffered / external-merge sort split
// (executor/sort.hpp Options, wired from plan/sort_plan.hpp): what each shape
// does with the memory the rules handed it, and what both shapes must return
// when that memory runs out.
//
// The contract under test:
//   * kBuffered never *plans* a spill -- it holds the input up to its budget
//     and only spills because the budget (or the process budget) ran out.
//   * kExternalMerge writes sorted runs once its row threshold is reached, and
//     merges them back through merge windows, so the working set stays bounded
//     however large the input is.
//   * Both keep the historical kAuto selection, and a failure anywhere (key
//     evaluation, the child cursor, a truncated run) surfaces as the sort's own
//     error instead of a short result.
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/exc_shim.hpp"
#include "common/status_or.hpp"
#include "executor/constant_executor.hpp"
#include "executor/executor_base.hpp"
#include "executor/query_memory.hpp"
#include "executor/sort.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

const Schema& SyntheticSchema() {
  static const Schema schema(
      "synthetic",
      {Column("key", ValueType::kInt64), Column("name", ValueType::kVarChar)});
  return schema;
}

const Expression& Key() {
  static const Expression key = ColumnValueExp("key");
  return key;
}

// Rows with ties (the key repeats every `peers` rows) and NULLs (every eighth
// row) mixed in, so a shape that reordered the wrong way -- or dropped the NULL
// peers, or lost the stability the in-memory sort has -- cannot hide. The
// payload column carries the input index.
std::vector<Row> MakeRows(size_t count, size_t peers = 5) {
  std::vector<Row> rows;
  rows.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    const Value key =
        i % 8 == 0 ? Value() : Value(static_cast<int64_t>(i % peers));
    rows.emplace_back(
        std::vector<Value>{key, Value("row" + std::to_string(i))});
  }
  return rows;
}

// A source that stamps a distinct position per row, so a spilled run's
// position handling can be checked alongside the ordering.
class PositionedRows final : public ExecutorBase {
 public:
  explicit PositionedRows(std::vector<Row> rows) : rows_(std::move(rows)) {}
  bool Next(Row* dst, RowPosition* rp) override {
    if (offset_ >= rows_.size()) {
      return false;
    }
    *dst = rows_[offset_];
    if (rp != nullptr) {
      *rp = RowPosition(11, static_cast<slot_t>(offset_));
    }
    ++offset_;
    return true;
  }
  void Dump(std::ostream& out, int /*indent*/) const override {
    out << "PositionedRows";
  }

 private:
  std::vector<Row> rows_{};
  size_t offset_{0};
};

// A source that fails after `fail_after` rows, standing in for a child cursor
// that hits an error mid-scan.
class FailingRows final : public ExecutorBase {
 public:
  FailingRows(std::vector<Row> rows, size_t fail_after)
      : rows_(std::move(rows)), fail_after_(fail_after) {}
  bool Next(Row* dst, RowPosition* rp) override {
    if (offset_ >= fail_after_) {
      return FailWith(StatusError(StatusCode::kIOError, "source failed"));
    }
    *dst = rows_[offset_];
    if (rp != nullptr) {
      *rp = RowPosition(0, static_cast<slot_t>(offset_));
    }
    ++offset_;
    return true;
  }
  void Dump(std::ostream& out, int /*indent*/) const override {
    out << "FailingRows";
  }

 private:
  std::vector<Row> rows_{};
  size_t fail_after_{0};
  size_t offset_{0};
};

// A key expression that fails for one specific value, so the sort's error
// plumbing can be exercised on both shapes.
class ExplodingKey final : public ExpressionBase {
 public:
  [[nodiscard]] TypeTag Type() const override {
    return TypeTag::kFunctionCallExp;
  }
  [[nodiscard]] StatusOr<Value> TryEvaluate(
      const Row& row, const Schema& schema) const override {
    const int offset = schema.Offset(ColumnName("name"));
    if (offset < 0) {
      return StatusError(StatusCode::kInvalidArgument, "no name column");
    }
    const std::string_view text =
        row.values_[static_cast<size_t>(offset)].value.varchar_value;
    if (text == "boom") {
      return StatusError(StatusCode::kInvalidArgument, "key evaluation failed");
    }
    return Value(std::string(text));
  }
  [[nodiscard]] Value Evaluate(const Row& row,
                               const Schema& schema) const override {
    return ExcShimUnwrap(TryEvaluate(row, schema), "ExplodingKey");
  }
  [[nodiscard]] std::string ToString() const override {
    return "exploding_key";
  }
  void Dump(std::ostream& out) const override { out << "exploding_key"; }
};

const Expression& ExplodingName() {
  static const Expression key = std::make_shared<ExplodingKey>();
  return key;
}

// RAII: temporarily replaces the process-wide query working-memory budget so a
// test can force (or forbid) the executor's internal spill selection. Restored
// even on failure, so a spilled test cannot corrupt the later ones.
class ScopedQueryMemory {
 public:
  explicit ScopedQueryMemory(size_t limit)
      : original_(QueryMemoryBudget::Global().Limit()) {
    QueryMemoryBudget::Global().ResetForTest(limit);
  }
  ~ScopedQueryMemory() { QueryMemoryBudget::Global().ResetForTest(original_); }
  ScopedQueryMemory(const ScopedQueryMemory&) = delete;
  ScopedQueryMemory(ScopedQueryMemory&&) = delete;
  ScopedQueryMemory& operator=(const ScopedQueryMemory&) = delete;
  ScopedQueryMemory& operator=(ScopedQueryMemory&&) = delete;

 private:
  size_t original_;
};

SortExecutor::Options Buffered(size_t budget_bytes) {
  return SortExecutor::Options{.spill_mode = SortExecutor::SpillMode::kBuffered,
                               .memory_budget_bytes = budget_bytes,
                               .merge_window_rows = 8,
                               .spill_threshold_rows = 0};
}

SortExecutor::Options ExternalMerge(size_t budget_bytes, size_t run_rows) {
  return SortExecutor::Options{
      .spill_mode = SortExecutor::SpillMode::kExternalMerge,
      .memory_budget_bytes = budget_bytes,
      .merge_window_rows = 8,
      .spill_threshold_rows = run_rows};
}

struct Collected {
  std::vector<Row> rows;
  std::vector<RowPosition> positions;
  Status status{Status::kSuccess};
};

Collected Collect(const std::shared_ptr<SortExecutor>& sort) {
  Collected out;
  Row row;
  RowPosition position;
  while (sort->Next(&row, &position)) {
    out.rows.push_back(row);
    out.positions.push_back(position);
    row = Row();
  }
  out.status = sort->GetStatus();
  return out;
}

bool InOrder(const std::vector<Row>& rows) {
  for (size_t i = 1; i < rows.size(); ++i) {
    const Value& previous = rows[i - 1][0];
    const Value& current = rows[i][0];
    if (previous.IsNull() != current.IsNull()) {
      // NULLS FIRST is the engine default for an ascending key.
      if (!previous.IsNull()) {
        return false;
      }
      continue;
    }
    if (previous.IsNull()) {
      continue;
    }
    if (previous.value.int_value > current.value.int_value) {
      return false;
    }
  }
  return true;
}

}  // namespace

// --------------------------------------------------------------- the modes ---

TEST(SortShapeExecutorTest, TheDefaultConstructorKeepsTheAdaptiveMode) {
  auto sort = std::make_shared<SortExecutor>(
      std::make_shared<PositionedRows>(MakeRows(8)), SyntheticSchema(),
      std::vector<SortExecutor::Key>{{Key(), true, std::nullopt}});
  EXPECT_EQ(sort->ActiveSpillMode(), SortExecutor::SpillMode::kAuto);
  EXPECT_EQ(sort->SortOptions().memory_budget_bytes, 0U);
  EXPECT_EQ(sort->SortOptions().spill_threshold_rows, 0U);
  EXPECT_EQ(sort->MergeWindowRows(), SortExecutor::kDefaultMergeWindowRows);
  EXPECT_EQ(
      std::string(SortExecutor::SpillModeLabel(SortExecutor::SpillMode::kAuto)),
      "adaptive");
  // The historical Dump still names the parallel sort.
  std::ostringstream dump;
  sort->Dump(dump, 0);
  EXPECT_NE(dump.str().find("ParallelSort"), std::string::npos) << dump.str();
  EXPECT_NE(dump.str().find("spill=adaptive"), std::string::npos) << dump.str();
}

TEST(SortShapeExecutorTest, TheBufferedShapeKeepsASmallInputInMemory) {
  // 40 rows with a 1 MiB budget: the buffered shape never plans a spill, and
  // the process budget has room besides, so nothing is written.
  ScopedQueryMemory memory(1UL << 20);
  auto sort = std::make_shared<SortExecutor>(
      std::make_shared<PositionedRows>(MakeRows(40)), SyntheticSchema(),
      std::vector<SortExecutor::Key>{{Key(), true, std::nullopt}}, 1,
      Buffered(1UL << 20));
  const Collected rows = Collect(sort);
  EXPECT_EQ(rows.status, Status::kSuccess);
  EXPECT_EQ(rows.rows.size(), 40U);
  EXPECT_TRUE(InOrder(rows.rows));
}

TEST(SortShapeExecutorTest, TheBufferedShapeStaysCorrectWhenItsBudgetRunsOut) {
  // The shape was planned for a budget the input does not fit: the sort spills
  // to the executor's own selection rather than failing or dropping rows.
  ScopedQueryMemory memory(1UL << 20);
  auto sort = std::make_shared<SortExecutor>(
      std::make_shared<PositionedRows>(MakeRows(2000)), SyntheticSchema(),
      std::vector<SortExecutor::Key>{{Key(), true, std::nullopt}}, 1,
      Buffered(static_cast<size_t>(64) * 1024));
  const Collected rows = Collect(sort);
  EXPECT_EQ(rows.status, Status::kSuccess);
  ASSERT_EQ(rows.rows.size(), 2000U);
  EXPECT_TRUE(InOrder(rows.rows));
  // Positions survive the round trip through the runs.
  for (const RowPosition& position : rows.positions) {
    EXPECT_EQ(position.page_id, static_cast<page_id_t>(11));
  }
}

TEST(SortShapeExecutorTest, TheExternalMergeShapeWritesRunsAboveItsThreshold) {
  // A huge process budget and a two-row run threshold: 300 rows become ~150
  // runs, so the merge path (not the in-memory one) produced the answer.
  ScopedQueryMemory memory(1UL << 30);
  auto sort = std::make_shared<SortExecutor>(
      std::make_shared<PositionedRows>(MakeRows(300)), SyntheticSchema(),
      std::vector<SortExecutor::Key>{{Key(), true, std::nullopt}}, 1,
      ExternalMerge(1UL << 20, 2));
  EXPECT_EQ(sort->ActiveSpillMode(), SortExecutor::SpillMode::kExternalMerge);
  EXPECT_EQ(sort->SortOptions().spill_threshold_rows, 2U);
  const Collected rows = Collect(sort);
  EXPECT_EQ(rows.status, Status::kSuccess);
  ASSERT_EQ(rows.rows.size(), 300U);
  EXPECT_TRUE(InOrder(rows.rows));
  // Stable across the runs: peers keep their input order, so the input index
  // carried in the payload column rises inside every tie group (the NULL
  // keys form one such group of their own).
  size_t index = 0;
  while (index < rows.rows.size()) {
    size_t end = index;
    while (end < rows.rows.size() &&
           (rows.rows[end][0].IsNull() == rows.rows[index][0].IsNull()) &&
           (rows.rows[index][0].IsNull() ||
            rows.rows[end][0].value.int_value ==
                rows.rows[index][0].value.int_value)) {
      ++end;
    }
    int64_t previous = -1;
    for (size_t i = index; i < end; ++i) {
      const std::string payload(rows.rows[i][1].value.varchar_value);
      const int64_t source = std::stoll(payload.substr(3));
      EXPECT_GT(source, previous)
          << "peer group at " << index << " lost its input order";
      previous = source;
    }
    EXPECT_GT(end, index);
    index = end;
  }
}

TEST(SortShapeExecutorTest, TheExternalMergeShapeBoundsTheWorkingSet) {
  // The rows threshold alone decides when a run is written, so the materialized
  // output is charged to the budget but the buffered run never grew past the
  // threshold. Observed through the merge window the shape was given: a
  // two-row window over two-row runs still returns every row exactly once.
  ScopedQueryMemory memory(1UL << 30);
  SortExecutor::Options options = ExternalMerge(1UL << 20, 2);
  options.merge_window_rows = 1;
  auto sort = std::make_shared<SortExecutor>(
      std::make_shared<PositionedRows>(MakeRows(50)), SyntheticSchema(),
      std::vector<SortExecutor::Key>{{Key(), true, std::nullopt}}, 1, options);
  EXPECT_EQ(sort->MergeWindowRows(), 1U);
  const Collected rows = Collect(sort);
  EXPECT_EQ(rows.status, Status::kSuccess);
  ASSERT_EQ(rows.rows.size(), 50U);
  EXPECT_TRUE(InOrder(rows.rows));
  // A window of 0 rows would spin forever; the option is clamped instead.
  options.merge_window_rows = 0;
  auto clamped = std::make_shared<SortExecutor>(
      std::make_shared<PositionedRows>(MakeRows(10)), SyntheticSchema(),
      std::vector<SortExecutor::Key>{{Key(), true, std::nullopt}}, 1, options);
  EXPECT_EQ(clamped->MergeWindowRows(), 1U);
  EXPECT_EQ(Collect(clamped).rows.size(), 10U);
}

TEST(SortShapeExecutorTest, BothShapesAgreeOnOrderNullsAndTies) {
  ScopedQueryMemory memory(1UL << 20);
  const std::vector<Row> rows = MakeRows(500);
  auto buffered = std::make_shared<SortExecutor>(
      std::make_shared<PositionedRows>(rows), SyntheticSchema(),
      std::vector<SortExecutor::Key>{{Key(), true, std::nullopt}}, 1,
      Buffered(1UL << 20));
  auto merged = std::make_shared<SortExecutor>(
      std::make_shared<PositionedRows>(rows), SyntheticSchema(),
      std::vector<SortExecutor::Key>{{Key(), true, std::nullopt}}, 1,
      ExternalMerge(1UL << 20, 7));
  const Collected from_buffered = Collect(buffered);
  const Collected from_merge = Collect(merged);
  EXPECT_EQ(from_buffered.status, Status::kSuccess);
  EXPECT_EQ(from_merge.status, Status::kSuccess);
  ASSERT_EQ(from_buffered.rows.size(), 500U);
  EXPECT_EQ(from_buffered.rows, from_merge.rows);
  EXPECT_TRUE(InOrder(from_buffered.rows));
  // The NULL keys land first, the rest ascend, and every row survives.
  size_t nulls = 0;
  for (const Row& row : from_buffered.rows) {
    nulls += row[0].IsNull() ? 1U : 0U;
  }
  // MakeRows NULLs every eighth row, so the count is ceil(rows / 8).
  EXPECT_EQ(nulls, (from_buffered.rows.size() + 7) / 8);
}

TEST(SortShapeExecutorTest, BothShapesHonourDescendingAndNullsLast) {
  ScopedQueryMemory memory(1UL << 20);
  const std::vector<Row> rows = MakeRows(200);
  std::vector<SortExecutor::Key> descending{
      SortExecutor::Key{.expression = Key(),
                        .ascending = false,
                        .nulls_first = std::optional<bool>(false),
                        .collation = Collation::kNone}};
  auto buffered = std::make_shared<SortExecutor>(
      std::make_shared<PositionedRows>(rows), SyntheticSchema(), descending, 1,
      Buffered(1UL << 20));
  auto merged = std::make_shared<SortExecutor>(
      std::make_shared<PositionedRows>(rows), SyntheticSchema(), descending, 1,
      ExternalMerge(1UL << 20, 5));
  const Collected from_buffered = Collect(buffered);
  const Collected from_merge = Collect(merged);
  ASSERT_EQ(from_buffered.rows.size(), 200U);
  EXPECT_EQ(from_buffered.rows, from_merge.rows);
  // NULLS LAST: the null keys are at the very end, everything above them
  // descends.
  for (size_t i = 1; i < from_buffered.rows.size(); ++i) {
    if (from_buffered.rows[i][0].IsNull()) {
      continue;
    }
    ASSERT_FALSE(from_buffered.rows[i - 1][0].IsNull())
        << "a non-NULL key must precede the NULLS LAST block";
    EXPECT_GE(from_buffered.rows[i - 1][0].value.int_value,
              from_buffered.rows[i][0].value.int_value);
  }
  EXPECT_TRUE(from_buffered.rows.back()[0].IsNull());
}

// ----------------------------------------------------------- error paths -----

TEST(SortShapeExecutorTest, AKeyFailureSurfacesInBothShapes) {
  ScopedQueryMemory memory(1UL << 20);
  std::vector<Row> rows = MakeRows(64);
  // Put the failing value past the external merge run threshold, so the merge
  // re-evaluates the key on rows it read back from disk.
  for (Row& row : rows) {
    row[1] = Value("row");
  }
  rows[40][1] = Value("boom");
  const std::vector<SortExecutor::Key> keys{
      SortExecutor::Key{.expression = ExplodingName(),
                        .ascending = true,
                        .nulls_first = std::nullopt,
                        .collation = Collation::kNone}};
  for (const SortExecutor::Options& options :
       {Buffered(1UL << 20), ExternalMerge(1UL << 20, 4)}) {
    auto sort =
        std::make_shared<SortExecutor>(std::make_shared<PositionedRows>(rows),
                                       SyntheticSchema(), keys, 1, options);
    const Collected out = Collect(sort);
    EXPECT_NE(out.status, Status::kSuccess)
        << SortExecutor::SpillModeLabel(options.spill_mode);
    // A failure must not hand back the rows that were already ordered.
    EXPECT_TRUE(out.rows.empty())
        << SortExecutor::SpillModeLabel(options.spill_mode);
  }
}

TEST(SortShapeExecutorTest, AChildFailureSurfacesInBothShapes) {
  ScopedQueryMemory memory(1UL << 20);
  const std::vector<Row> rows = MakeRows(200);
  const std::vector<SortExecutor::Key> keys{
      SortExecutor::Key{.expression = Key(),
                        .ascending = true,
                        .nulls_first = std::nullopt,
                        .collation = Collation::kNone}};
  for (const SortExecutor::Options& options :
       {Buffered(1UL << 20), ExternalMerge(1UL << 20, 4)}) {
    // The child dies half way through, in the middle of the runs for the spill
    // shape: the sort must report the error, never a short "sorted" prefix.
    auto sort =
        std::make_shared<SortExecutor>(std::make_shared<FailingRows>(rows, 50),
                                       SyntheticSchema(), keys, 1, options);
    const Collected out = Collect(sort);
    EXPECT_EQ(out.status, StatusCode::kIOError)
        << SortExecutor::SpillModeLabel(options.spill_mode);
  }
}

TEST(SortShapeExecutorTest, AnEmptyInputSpillsToNothing) {
  ScopedQueryMemory memory(1UL << 20);
  auto sort = std::make_shared<SortExecutor>(
      std::make_shared<PositionedRows>(std::vector<Row>{}), SyntheticSchema(),
      std::vector<SortExecutor::Key>{{Key(), true, std::nullopt}}, 1,
      ExternalMerge(1UL << 20, 4));
  const Collected rows = Collect(sort);
  EXPECT_EQ(rows.status, Status::kSuccess);
  EXPECT_TRUE(rows.rows.empty());
}

}  // namespace tinylamb
