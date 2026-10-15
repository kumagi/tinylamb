/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// The operator half of the grouped aggregation shapes
// (executor/grouped_aggregate.hpp, wired from plan/aggregation_plan.hpp): three
// algorithms over one GROUP BY payload -- a hash table, sorted runs, and a
// single streaming pass -- plus the memory contract each shape was planned
// with.
//
// The contract under test:
//   * all three shapes return the same groups for the same input: the group key
//     canonicalization (NULL is one group, NaN one peer, -0 folds), the
//     duplicate-group folding and the aggregate payload are shape independent.
//   * a payload without keys is one group (the historical scalar contract), so
//     the stream shape is valid over any input order there.
//   * the hash shape partitions its input and re-groups it per partition when
//     the group table outgrows the budget; the sort shape writes sorted runs
//     and folds equal runs. Neither may change a single row, and a spill file
//     that cannot be written is an error, not a short answer.
//   * the stream shape refuses an input that does not arrive in group-key order
//     instead of emitting an order it never had.
//   * a failing child, a failing key and a failing HAVING surface as the
//     shape's own error with no partial result.
//
// The plan half (payload, EXPLAIN, memory policy, rule selection, factory
// wiring) lives in plan/aggregate_shape_test.cpp; the SQL differential against
// the engine's own GROUP BY path is at the bottom of this file.
#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

// setenv/unsetenv below are POSIX APIs that <cstdlib> does not declare.
// NOLINTNEXTLINE(modernize-deprecated-headers)
#include <stdlib.h>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/constant_executor.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/grouped_aggregate.hpp"
#include "executor/query_memory.hpp"
#include "executor/sort.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/named_expression.hpp"
#include "page/row_position.hpp"
#include "query/sql_engine.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

// (k, name, v): an INT64 grouping key with NULLs and duplicates, a VARCHAR
// payload for the distinct/ordered-set aggregates, and a DOUBLE measure with
// NULLs for the NULL-skipping aggregates.
const Schema& InputSchema() {
  static const Schema schema("agg", {Column("k", ValueType::kInt64),
                                     Column("name", ValueType::kVarChar),
                                     Column("v", ValueType::kDouble)});
  return schema;
}

const Expression& K() {
  static const Expression key = ColumnValueExp("k");
  return key;
}

const Expression& Name() {
  static const Expression key = ColumnValueExp("name");
  return key;
}

const Expression& V() {
  static const Expression key = ColumnValueExp("v");
  return key;
}

NamedExpression Key(const std::string& name) { return {name, K()}; }

NamedExpression CountStar() {
  return {"n", AggregateExpressionExp(AggregationType::kCount, nullptr)};
}

NamedExpression CountOf(const Expression& argument) {
  return {"n", AggregateExpressionExp(AggregationType::kCount, argument)};
}

NamedExpression Sum(const Expression& argument,
                    const std::string& name = "total") {
  return {name, AggregateExpressionExp(AggregationType::kSum, argument)};
}

NamedExpression Min(const Expression& argument) {
  return {"lo", AggregateExpressionExp(AggregationType::kMin, argument)};
}

NamedExpression Max(const Expression& argument) {
  return {"hi", AggregateExpressionExp(AggregationType::kMax, argument)};
}

NamedExpression Avg(const Expression& argument) {
  return {"avg", AggregateExpressionExp(AggregationType::kAvg, argument)};
}

NamedExpression CountDistinct(const Expression& argument) {
  return {"distinct", AggregateExpressionExp(AggregationType::kCount, argument,
                                             /*distinct=*/true)};
}

// SUM(v WHERE v > 1.5): the row-level FILTER, which must be applied per
// aggregate inside the group.
NamedExpression FilteredSum() {
  auto aggregate =
      std::make_shared<AggregateExpression>(AggregationType::kSum, V());
  aggregate->SetWhereFilter(BinaryExpressionExp(
      V(), BinaryOperation::kGreaterThan, ConstantValueExp(Value(1.5))));
  return {"filtered", aggregate};
}

std::vector<Row> MakeRows() {
  return {
      // k = 1 appears three times (with a NULL v on one of them), k = 2 twice,
      // and a NULL key twice: three groups, one of them the NULL group.
      Row({Value(int64_t{1}), Value("a"), Value(1.0)}),
      Row({Value(int64_t{1}), Value("b"), Value(2.0)}),
      Row({Value(int64_t{1}), Value("b"), Value()}),
      Row({Value(int64_t{2}), Value("a"), Value(4.0)}),
      Row({Value(int64_t{2}), Value("c"), Value()}),
      Row({Value(), Value("a"), Value(8.0)}),
      Row({Value(), Value("d"), Value()}),
  };
}

// The same rows in group-key order (ascending, NULLs first) so the streaming
// shape's input-order contract holds.
std::vector<Row> MakeRowsOrdered() {
  return {
      Row({Value(), Value("a"), Value(8.0)}),
      Row({Value(), Value("d"), Value()}),
      Row({Value(int64_t{1}), Value("a"), Value(1.0)}),
      Row({Value(int64_t{1}), Value("b"), Value(2.0)}),
      Row({Value(int64_t{1}), Value("b"), Value()}),
      Row({Value(int64_t{2}), Value("a"), Value(4.0)}),
      Row({Value(int64_t{2}), Value("c"), Value()}),
  };
}

std::vector<Row> MakeManyRows(size_t groups, size_t rows_per_group) {
  std::vector<Row> rows;
  rows.reserve(groups * rows_per_group);
  for (size_t group = 0; group < groups; ++group) {
    for (size_t row = 0; row < rows_per_group; ++row) {
      rows.emplace_back(Row({Value(static_cast<int64_t>(group)),
                             Value("g" + std::to_string(group)),
                             Value(static_cast<double>(row))}));
    }
  }
  return rows;
}

// The same rows with the equal keys spread apart, so no two rows of one group
// are ever adjacent. The hash table folds them by key; the sort runs first
// bring them together. Any shape that folded by position would answer
// differently, which is what makes this the sharpest separation fixture.
std::vector<Row> MakeInterleavedRows(size_t groups, size_t rows_per_group) {
  std::vector<Row> rows;
  rows.reserve(groups * rows_per_group);
  for (size_t row = 0; row < rows_per_group; ++row) {
    for (size_t group = 0; group < groups; ++group) {
      rows.emplace_back(Row({Value(static_cast<int64_t>(group)),
                             Value("g" + std::to_string(group)),
                             Value(static_cast<double>(row))}));
    }
  }
  return rows;
}

struct Collected {
  std::vector<Row> rows;
  Status status{Status::kSuccess};
};

Collected Collect(
    Executor executor) {  // NOLINT(performance-unnecessary-value-param)
  Collected out;
  Row row;
  RowPosition position;
  while (executor->Next(&row, &position)) {
    out.rows.push_back(std::move(row));
    row = Row();
  }
  out.status = executor->GetStatus();
  return out;
}

// One text cell per row: "[k|n|total|lo|hi|...]" with NULL rendered as "NULL",
// so a shape difference shows up in a readable diff.
std::vector<std::string> Text(const std::vector<Row>& rows) {
  std::vector<std::string> out;
  out.reserve(rows.size());
  for (const Row& row : rows) {
    std::string text;
    for (size_t i = 0; i < row.values_.size(); ++i) {
      if (i > 0) {
        text += "|";
      }
      const Value& value = row.values_[i];
      if (value.IsNull()) {
        text += "NULL";
        continue;
      }
      if (value.type == ValueType::kVarChar) {
        // Bare text: the cell is already delimited by '|'.
        text += value.value.varchar_value;
        continue;
      }
      std::ostringstream cell;
      cell << value;
      text += cell.str();
    }
    out.push_back(text);
  }
  return out;
}

// The shape comparison sorts both sides, so an expectation may be written in
// whatever order reads best.
std::vector<std::string> Sorted(std::vector<std::string> values) {
  std::sort(values.begin(), values.end());
  return values;
}

// gtest cannot stream a vector of strings into a failure message, so a rendered
// group set is joined into one line when a test wants to show what it got.
std::string Join(const std::vector<std::string>& values) {
  std::string out;
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) {
      out += ", ";
    }
    out += values[i];
  }
  return out;
}

Executor Rows(const std::vector<Row>& rows) {
  return std::make_shared<ConstantExecutor>(rows);
}

// One builder for the three shapes: the streaming shape is the only one whose
// contract is an option rather than an algorithm, so it is set here instead of
// at every call site.
template <typename Shape>
std::shared_ptr<Shape> MakeShape(
    const Schema& schema, const std::vector<Row>& rows,
    std::vector<NamedExpression> keys, std::vector<NamedExpression> aggregates,
    Expression having = nullptr,
    GroupedAggregateOptions options = GroupedAggregateOptions{}) {
  if constexpr (std::is_same_v<Shape, StreamAggregateExecutor>) {
    options.require_ordered_input = true;
  }
  return std::make_shared<Shape>(Rows(rows), schema, std::move(keys),
                                 std::move(aggregates), std::move(having),
                                 options);
}

std::shared_ptr<HashAggregateExecutor> HashShape(
    const std::vector<Row>& rows, std::vector<NamedExpression> keys,
    std::vector<NamedExpression> aggregates, Expression having = nullptr,
    GroupedAggregateOptions options = GroupedAggregateOptions{}) {
  return MakeShape<HashAggregateExecutor>(InputSchema(), rows, std::move(keys),
                                          std::move(aggregates),
                                          std::move(having), options);
}

std::shared_ptr<SortAggregateExecutor> SortShape(
    const std::vector<Row>& rows, std::vector<NamedExpression> keys,
    std::vector<NamedExpression> aggregates, Expression having = nullptr,
    GroupedAggregateOptions options = GroupedAggregateOptions{}) {
  return MakeShape<SortAggregateExecutor>(InputSchema(), rows, std::move(keys),
                                          std::move(aggregates),
                                          std::move(having), options);
}

std::shared_ptr<StreamAggregateExecutor> StreamShape(
    const std::vector<Row>& rows, std::vector<NamedExpression> keys,
    std::vector<NamedExpression> aggregates, Expression having = nullptr,
    GroupedAggregateOptions options = GroupedAggregateOptions{}) {
  return MakeShape<StreamAggregateExecutor>(
      InputSchema(), rows, std::move(keys), std::move(aggregates),
      std::move(having), options);
}

std::vector<NamedExpression> StandardPayload() {
  return {CountStar(), Sum(V()), Min(V()), Max(V())};
}

// The three shapes, sorted by the rendered group so the comparison does not
// depend on which order a shape happens to emit groups in.
std::vector<std::string> GroupsOf(Executor executor) {
  Collected collected = Collect(std::move(executor));
  EXPECT_EQ(collected.status, Status::kSuccess);
  std::vector<std::string> text = Text(collected.rows);
  std::sort(text.begin(), text.end());
  return text;
}

// A child that fails after `fail_after` rows, standing in for a scan cursor
// that hits an error mid-input.
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
      *rp = RowPosition();
    }
    ++offset_;
    return true;
  }
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override {
    destination->Reset();
    if (offset_ >= fail_after_) {
      FailWith(StatusError(StatusCode::kIOError, "source failed"));
      return 0;
    }
    const size_t take = std::min(max_rows, fail_after_ - offset_);
    for (size_t i = 0; i < take; ++i) {
      destination->Append(std::move(rows_[offset_++]));
    }
    return destination->Size();
  }
  void Dump(std::ostream& out, int /*indent*/) const override {
    out << "FailingRows";
  }

 private:
  std::vector<Row> rows_{};
  size_t fail_after_{0};
  size_t offset_{0};
};

// A group key that fails for one specific value, so a key-evaluation error can
// be exercised on every shape. It reports kInvalid because it is a test double
// and not one of the engine's node kinds: the shapes ask a key for its
// collation and its column offset, and both answers must be "none" for a
// non-column key rather than a cast to a node type it is not.
class ExplodingKey final : public ExpressionBase {
 public:
  [[nodiscard]] TypeTag Type() const override { return TypeTag::kInvalid; }
  [[nodiscard]] StatusOr<Value> TryEvaluate(
      const Row& row, const Schema& schema) const override {
    const int offset = schema.Offset(ColumnName("v"));
    if (offset < 0) {
      return StatusError(StatusCode::kInvalidArgument, "no v column");
    }
    const Value& argument = row.values_[static_cast<size_t>(offset)];
    if (argument.IsNull()) {
      return StatusError(StatusCode::kInvalidArgument, "key evaluation failed");
    }
    return Value(static_cast<int64_t>(argument.value.double_value));
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

// RAII: points TINYLAMB_TEMP at a directory the test controls, so a spill file
// lands (or cannot land) where the test can see it.
class ScopedTempDir {
 public:
  explicit ScopedTempDir(const std::filesystem::path& path) {
    const char* previous = std::getenv("TINYLAMB_TEMP");
    if (previous != nullptr) {
      previous_ = previous;
      had_previous_ = true;
    }
    path_ = path;
    setenv("TINYLAMB_TEMP", path_.c_str(), 1);
  }
  ~ScopedTempDir() {
    if (had_previous_) {
      setenv("TINYLAMB_TEMP", previous_.c_str(), 1);
    } else {
      unsetenv("TINYLAMB_TEMP");
    }
  }
  ScopedTempDir(const ScopedTempDir&) = delete;
  ScopedTempDir(ScopedTempDir&&) = delete;
  ScopedTempDir& operator=(const ScopedTempDir&) = delete;
  ScopedTempDir& operator=(ScopedTempDir&&) = delete;

 private:
  std::filesystem::path path_;
  std::string previous_{};
  bool had_previous_{false};
};

}  // namespace

// ------------------------------------------------- one GROUP BY, three shapes
// --

TEST(GroupedAggregateTest, AllThreeShapesReturnTheSameGroups) {
  const std::vector<Row> rows = MakeRows();
  const std::vector<Row> ordered = MakeRowsOrdered();
  const std::vector<std::string> expected = Sorted({
      // NULL key: 2 rows, SUM 8.0 (one NULL v), MIN 8, MAX 8
      "NULL|2|8|8|8",
      // k = 1: 3 rows, SUM 3.0 over two non-NULL v, MIN 1, MAX 2
      "1|3|3|1|2",
      // k = 2: 2 rows, SUM 4.0, MIN 4, MAX 4
      "2|2|4|4|4",
  });
  EXPECT_EQ(GroupsOf(HashShape(rows, {Key("k")}, StandardPayload())), expected);
  EXPECT_EQ(GroupsOf(SortShape(rows, {Key("k")}, StandardPayload())), expected);
  EXPECT_EQ(GroupsOf(StreamShape(ordered, {Key("k")}, StandardPayload())),
            expected);
  // The sort shape emits in group-key order (ascending, NULLs first), so its
  // output is ordered where the hash shape's is not.
  const Collected sorted =
      Collect(SortShape(rows, {Key("k")}, StandardPayload()));
  ASSERT_EQ(sorted.status, Status::kSuccess);
  const std::vector<std::string> sorted_text = Text(sorted.rows);
  EXPECT_EQ(sorted_text.front(), "NULL|2|8|8|8") << sorted_text.front();
  EXPECT_EQ(sorted_text.back(), "2|2|4|4|4") << sorted_text.back();
}

TEST(GroupedAggregateTest, CompositeKeysAndDuplicateKeysGroupOnTheWholeKey) {
  const std::vector<Row> rows = {
      Row({Value(int64_t{1}), Value("a"), Value(1.0)}),
      Row({Value(int64_t{1}), Value("a"), Value(3.0)}),
      Row({Value(int64_t{1}), Value("b"), Value(5.0)}),
      Row({Value(int64_t{2}), Value("a"), Value(7.0)}),
  };
  const std::vector<NamedExpression> keys = {Key("k"),
                                             NamedExpression("name", Name())};
  const std::vector<std::string> expected = Sorted({
      "1|a|2|4|1|3",  // duplicate (1,'a') rows fold
      "1|b|1|5|5|5",  // same k, different name: its own group
      "2|a|1|7|7|7",
  });
  EXPECT_EQ(GroupsOf(HashShape(rows, keys, StandardPayload())), expected);
  EXPECT_EQ(GroupsOf(SortShape(rows, keys, StandardPayload())), expected);
  EXPECT_EQ(GroupsOf(StreamShape(rows, keys, StandardPayload())), expected);
}

TEST(GroupedAggregateTest, EmptyInputHasNoGroupsButOneScalarRow) {
  const std::vector<Row> empty;
  // A grouping key: no input, no group.
  EXPECT_TRUE(HashShape(empty, {Key("k")}, StandardPayload())->GroupCount() ==
              0);
  const Collected hash =
      Collect(HashShape(empty, {Key("k")}, StandardPayload()));
  EXPECT_EQ(hash.status, Status::kSuccess);
  EXPECT_TRUE(hash.rows.empty());
  EXPECT_TRUE(
      Collect(SortShape(empty, {Key("k")}, StandardPayload())).rows.empty());
  EXPECT_TRUE(
      Collect(StreamShape(empty, {Key("k")}, StandardPayload())).rows.empty());
  // No key: exactly one group, even over an empty input. COUNT(*) is 0 and
  // SUM/MIN/MAX are NULL, which is the historical scalar contract.
  for (const std::vector<Row>* source : {&empty}) {
    const Collected scalar = Collect(HashShape(*source, {}, StandardPayload()));
    ASSERT_EQ(scalar.status, Status::kSuccess);
    ASSERT_EQ(scalar.rows.size(), 1U);
    EXPECT_EQ(Text(scalar.rows), std::vector<std::string>{"0|NULL|NULL|NULL"});
  }
  const Collected scalar_sort =
      Collect(SortShape(empty, {}, StandardPayload()));
  ASSERT_EQ(scalar_sort.status, Status::kSuccess);
  EXPECT_EQ(scalar_sort.rows.size(), 1U);
  const Collected scalar_stream =
      Collect(StreamShape(empty, {}, StandardPayload()));
  ASSERT_EQ(scalar_stream.status, Status::kSuccess);
  EXPECT_EQ(scalar_stream.rows.size(), 1U);
}

TEST(GroupedAggregateTest, NullKeysAreOneGroupAndNullInputsAreSkipped) {
  const std::vector<Row> rows = {
      Row({Value(), Value("a"), Value()}),
      Row({Value(), Value("b"), Value(3.0)}),
      Row({Value(int64_t{7}), Value("c"), Value()}),
  };
  const std::vector<NamedExpression> aggregates = {
      CountStar(), CountOf(V()), Sum(V()), Min(V()), Max(V())};
  const std::vector<std::string> expected = Sorted({
      // NULL key: 2 rows, one non-NULL v, MIN 3 / MAX 3
      "NULL|2|1|3|3|3",
      // k = 7: the only v is NULL, so COUNT(v) is 0 and the rest are NULL
      "7|1|0|NULL|NULL|NULL",
  });
  EXPECT_EQ(GroupsOf(HashShape(rows, {Key("k")}, aggregates)), expected);
  EXPECT_EQ(GroupsOf(SortShape(rows, {Key("k")}, aggregates)), expected);
  EXPECT_EQ(GroupsOf(StreamShape(rows, {Key("k")}, aggregates)), expected);
}

TEST(GroupedAggregateTest, NanAndNegativeZeroKeysFoldIntoOneGroup) {
  // A DOUBLE key column: NaN, -0.0, 0.0 and 1.0 over one declared type, so
  // every shape can order it and the folding is the only variable.
  static const Schema double_key_schema(
      "aggd",
      {Column("k", ValueType::kDouble), Column("v", ValueType::kDouble)});
  const std::vector<Row> rows = {
      Row({Value(std::numeric_limits<double>::quiet_NaN()), Value(1.0)}),
      Row({Value(std::numeric_limits<double>::quiet_NaN()), Value(2.0)}),
      Row({Value(-0.0), Value(4.0)}),
      Row({Value(0.0), Value(8.0)}),
      Row({Value(1.0), Value(16.0)}),
  };
  const std::vector<NamedExpression> keys = {
      NamedExpression("k", ColumnValueExp("k"))};
  const std::vector<NamedExpression> aggregates = {
      CountStar(),
      NamedExpression("total", AggregateExpressionExp(AggregationType::kSum,
                                                      ColumnValueExp("v")))};
  // 5 rows fold into 3 groups: one NaN peer set, one -0/+0 peer set, and 1.0.
  // Every shape must agree on the folding, not just on the group count.
  const std::vector<std::string> expected =
      Sorted({"nan|2|3", "0|2|12", "1|1|16"});
  const Collected collected = Collect(MakeShape<HashAggregateExecutor>(
      double_key_schema, rows, keys, aggregates));
  ASSERT_EQ(collected.status, Status::kSuccess);
  EXPECT_EQ(collected.rows.size(), 3U) << Join(Text(collected.rows));
  EXPECT_EQ(GroupsOf(MakeShape<HashAggregateExecutor>(double_key_schema, rows,
                                                      keys, aggregates)),
            expected);
  EXPECT_EQ(GroupsOf(MakeShape<SortAggregateExecutor>(double_key_schema, rows,
                                                      keys, aggregates)),
            expected);
}

TEST(GroupedAggregateTest, FilterDistinctAndAverageRunInsideEveryGroup) {
  const std::vector<Row> rows = MakeRows();
  const std::vector<NamedExpression> aggregates = {
      FilteredSum(), CountDistinct(Name()), Avg(V())};
  const std::vector<std::string> expected = Sorted({
      "NULL|8|2|8",  // one non-NULL v (8) passes the filter, 2 distinct names
      "1|2|2|1.5",   // 2 and 1: filter keeps 2, AVG over both non-NULL v
      "2|4|2|4",
  });
  EXPECT_EQ(GroupsOf(HashShape(rows, {Key("k")}, aggregates)), expected);
  EXPECT_EQ(GroupsOf(SortShape(rows, {Key("k")}, aggregates)), expected);
  EXPECT_EQ(GroupsOf(StreamShape(MakeRowsOrdered(), {Key("k")}, aggregates)),
            expected);
}

// ------------------------------------------------------------------- HAVING --

TEST(GroupedAggregateTest, HavingKeepsOnlyGroupsWhoseOutputRowIsTrue) {
  const std::vector<Row> rows = MakeRows();
  // HAVING over the output row (keys then aggregates), the same scope a
  // Selection above the aggregation sees.
  const Expression having = BinaryExpressionExp(ColumnValueExp("total"),
                                                BinaryOperation::kGreaterThan,
                                                ConstantValueExp(Value(3.0)));
  for (const Collected& collected :
       {Collect(HashShape(rows, {Key("k")}, StandardPayload(), having)),
        Collect(SortShape(rows, {Key("k")}, StandardPayload(), having))}) {
    ASSERT_EQ(collected.status, Status::kSuccess);
    EXPECT_EQ(GroupsOf(HashShape(rows, {Key("k")}, StandardPayload(), having)),
              Sorted({"NULL|2|8|8|8", "2|2|4|4|4"}));
  }
  // The streaming shape filters the same way.
  EXPECT_EQ(GroupsOf(StreamShape(MakeRowsOrdered(), {Key("k")},
                                 StandardPayload(), having)),
            Sorted({"NULL|2|8|8|8", "2|2|4|4|4"}));
}

TEST(GroupedAggregateTest,
     ARejectedScalarGroupIsNotResynthesizedAsAnEmptyState) {
  // `SELECT COUNT(*) FROM t HAVING COUNT(*) = 0` over a NON-empty t: the one
  // real group carries n = 3, HAVING rejects it, and the empty-input scalar
  // synthesis must not fire afterwards -- re-aggregating an empty state has
  // n = 0, which would pass HAVING and emit a spurious row.
  const std::vector<Row> rows = MakeRows();
  const Expression having =
      BinaryExpressionExp(ColumnValueExp("n"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{0})));
  for (const Collected& collected :
       {Collect(HashShape(rows, {}, StandardPayload(), having)),
        Collect(SortShape(rows, {}, StandardPayload(), having)),
        Collect(StreamShape(rows, {}, StandardPayload(), having))}) {
    ASSERT_EQ(collected.status, Status::kSuccess);
    EXPECT_TRUE(collected.rows.empty());
  }
  // The empty-input contract still holds: one scalar row per shape, kept or
  // dropped by the same HAVING.
  const std::vector<Row> empty;
  for (const Collected& collected :
       {Collect(HashShape(empty, {}, StandardPayload(), having)),
        Collect(SortShape(empty, {}, StandardPayload(), having)),
        Collect(StreamShape(empty, {}, StandardPayload(), having))}) {
    ASSERT_EQ(collected.status, Status::kSuccess);
    EXPECT_EQ(Text(collected.rows),
              std::vector<std::string>{"0|NULL|NULL|NULL"});
  }
}

TEST(GroupedAggregateTest, HavingOnAGroupKeyAndANullPredicateDropsTheGroup) {
  const std::vector<Row> rows = MakeRows();
  // HAVING k = 1: a group-key predicate over the output row.
  const Expression on_key =
      BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{1})));
  const Collected keyed =
      Collect(HashShape(rows, {Key("k")}, StandardPayload(), on_key));
  ASSERT_EQ(keyed.status, Status::kSuccess);
  EXPECT_EQ(Text(keyed.rows), std::vector<std::string>{"1|3|3|1|2"});
  // HAVING NULL: three-valued, so it keeps nothing -- and never errors.
  const Expression unknown = ConstantValueExp(Value());
  const Collected dropped =
      Collect(HashShape(rows, {Key("k")}, StandardPayload(), unknown));
  ASSERT_EQ(dropped.status, Status::kSuccess);
  EXPECT_TRUE(dropped.rows.empty());
  // HAVING that cannot even be resolved over the output row is an error, not a
  // silently kept or silently dropped group.
  const Expression missing =
      BinaryExpressionExp(ColumnValueExp("nope"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(1.0)));
  const Collected failed =
      Collect(HashShape(rows, {Key("k")}, StandardPayload(), missing));
  EXPECT_NE(failed.status, Status::kSuccess);
  EXPECT_TRUE(failed.rows.empty());
}

// ------------------------------------------------------------------- errors --

TEST(GroupedAggregateTest, AChildFailureNeverBecomesAPartialGroupSet) {
  const std::vector<Row> rows = MakeRows();
  for (const std::shared_ptr<ExecutorBase>& child :
       {std::static_pointer_cast<ExecutorBase>(
            std::make_shared<FailingRows>(rows, 3)),
        std::static_pointer_cast<ExecutorBase>(
            std::make_shared<FailingRows>(rows, 0))}) {
    const Collected hash = Collect(std::make_shared<HashAggregateExecutor>(
        child, InputSchema(), std::vector<NamedExpression>{Key("k")},
        StandardPayload()));
    EXPECT_NE(hash.status, Status::kSuccess);
    EXPECT_TRUE(hash.rows.empty());
  }
  // A failing key is the shape's own error on every algorithm.
  const Expression exploding = std::make_shared<ExplodingKey>();
  const Collected hash = Collect(std::make_shared<HashAggregateExecutor>(
      Rows(rows), InputSchema(),
      std::vector<NamedExpression>{NamedExpression("boom", exploding)},
      StandardPayload()));
  EXPECT_NE(hash.status, Status::kSuccess);
  EXPECT_TRUE(hash.rows.empty());
  const Collected sort = Collect(std::make_shared<SortAggregateExecutor>(
      Rows(rows), InputSchema(),
      std::vector<NamedExpression>{NamedExpression("boom", exploding)},
      StandardPayload()));
  EXPECT_NE(sort.status, Status::kSuccess);
  EXPECT_TRUE(sort.rows.empty());
  const Collected stream = Collect(std::make_shared<StreamAggregateExecutor>(
      Rows(rows), InputSchema(),
      std::vector<NamedExpression>{NamedExpression("boom", exploding)},
      StandardPayload()));
  EXPECT_NE(stream.status, Status::kSuccess);
  EXPECT_TRUE(stream.rows.empty());
}

// ------------------------------------------------------------- memory / spill
// --

TEST(GroupedAggregateTest, TheHashShapePartitionsAndStillReturnsEveryGroup) {
  const std::vector<Row> rows = MakeManyRows(64, 4);
  const std::vector<NamedExpression> keys = {Key("k")};
  const std::vector<NamedExpression> aggregates = {CountStar(), Sum(V())};
  const std::vector<std::string> expected =
      GroupsOf(HashShape(rows, keys, aggregates));

  GroupedAggregateOptions resident;
  const auto in_memory = HashShape(rows, keys, aggregates, nullptr, resident);
  EXPECT_FALSE(in_memory->Spilled());
  EXPECT_EQ(in_memory->GroupCapacity(),
            kGroupedAggregateDefaultMemoryBudgetBytes /
                kGroupedAggregateEstimatedRowBytes);
  EXPECT_EQ(GroupsOf(in_memory), expected);

  // One group of capacity: the table outgrows its budget on the second group,
  // so the input is partitioned and re-grouped one partition at a time.
  GroupedAggregateOptions spilled;
  spilled.memory_budget_bytes = kGroupedAggregateEstimatedRowBytes;
  spilled.spill_partition_count = 8;
  const auto partitioned = HashShape(rows, keys, aggregates, nullptr, spilled);
  EXPECT_EQ(partitioned->GroupCapacity(), 1U);
  const Collected collected = Collect(partitioned);
  EXPECT_EQ(collected.status, Status::kSuccess);
  EXPECT_TRUE(partitioned->Spilled());
  EXPECT_EQ(partitioned->SpillPartitionCount(), 8U);
  EXPECT_EQ(GroupsOf(HashShape(rows, keys, aggregates, nullptr, spilled)),
            expected);
}

TEST(GroupedAggregateTest, ASpillFileThatCannotBeWrittenIsAnError) {
  if (geteuid() == 0) {
    GTEST_SKIP() << "root ignores the directory permission the test relies on";
  }
  const std::filesystem::path dir = std::filesystem::temp_directory_path() /
                                    ("tinylamb_grouped_agg_" + RandomString());
  std::filesystem::create_directories(dir);
  // A directory nobody may write: the spill file cannot be created.
  std::error_code ec;
  std::filesystem::permissions(
      dir,
      std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec,
      ec);
  ASSERT_FALSE(ec) << ec.message();
  {
    const ScopedTempDir scoped(dir);
    GroupedAggregateOptions spilled;
    spilled.memory_budget_bytes = kGroupedAggregateEstimatedRowBytes;
    const Collected collected =
        Collect(HashShape(MakeManyRows(8, 2), {Key("k")},
                          {CountStar(), Sum(V())}, nullptr, spilled));
    EXPECT_NE(collected.status, Status::kSuccess);
    // No partial answer: a failed spill must not look like a short GROUP BY.
    EXPECT_TRUE(collected.rows.empty());
  }
  std::filesystem::permissions(dir, std::filesystem::perms::owner_all, ec);
  std::filesystem::remove_all(dir, ec);
}

TEST(GroupedAggregateTest, TheSortShapeWritesRunsAndStillReturnsEveryGroup) {
  const std::vector<Row> rows = MakeManyRows(32, 4);
  const std::vector<NamedExpression> keys = {Key("k")};
  const std::vector<NamedExpression> aggregates = {CountStar(), Sum(V())};
  const std::vector<std::string> expected =
      GroupsOf(SortShape(rows, keys, aggregates));
  ASSERT_FALSE(expected.empty());

  // A two-row run threshold forces the external merge path even though the
  // memory budget could have held the whole input.
  GroupedAggregateOptions runs;
  runs.spill_threshold_rows = 2;
  runs.merge_window_rows = 4;
  const auto spilling = SortShape(rows, keys, aggregates, nullptr, runs);
  EXPECT_EQ(spilling->SpillThresholdRows(), 2U);
  const Collected collected = Collect(spilling);
  EXPECT_EQ(collected.status, Status::kSuccess);
  EXPECT_TRUE(spilling->Sorted());
  EXPECT_EQ(GroupsOf(SortShape(rows, keys, aggregates, nullptr, runs)),
            expected);
  // A budget-derived threshold is the fallback when the rule named none.
  GroupedAggregateOptions budgeted;
  budgeted.memory_budget_bytes = 2 * kGroupedAggregateEstimatedRowBytes;
  EXPECT_EQ(SortShape(rows, keys, aggregates, nullptr, budgeted)
                ->SpillThresholdRows(),
            2U);
}

// --------------------------------------- the two grouped algorithms are two
// algorithms: different state, different order, different spill
// --

TEST(GroupedAggregateTest,
     TheHashShapeKeepsAGroupTableTheSortShapeKeepsOneGroup) {
  // The two shapes deliver the same groups over the same input, but the state
  // they need to do it is what separates them: the hash shape's state IS the
  // group table (one group per distinct key, so the peak grows with the
  // distinct keys), while the sort runs need no group table at all -- only the
  // run being read is folded, so the peak is one group however many groups the
  // input has. This is the difference the memory contract is built on, so it is
  // pinned here rather than left to the comment.
  const std::vector<Row> rows = MakeManyRows(48, 3);
  const std::vector<NamedExpression> keys = {Key("k")};
  const std::vector<NamedExpression> aggregates = {CountStar(), Sum(V())};

  const std::shared_ptr<HashAggregateExecutor> hash =
      HashShape(rows, keys, aggregates);
  const Collected hashed = Collect(hash);
  ASSERT_EQ(hashed.status, Status::kSuccess);
  EXPECT_EQ(hash->GroupCount(), 48U);
  EXPECT_EQ(hash->PeakResidentGroups(), 48U)
      << "the hash shape's state is one group per distinct key";

  const std::shared_ptr<SortAggregateExecutor> sort =
      SortShape(rows, keys, aggregates);
  const Collected sorted = Collect(sort);
  ASSERT_EQ(sorted.status, Status::kSuccess);
  EXPECT_EQ(sort->GroupCount(), 48U);
  EXPECT_EQ(sort->PeakResidentGroups(), 1U)
      << "the sort runs fold one run at a time, so they keep no group table";
  EXPECT_TRUE(sort->Sorted());
  // Different state, same answer: which state a shape kept must never be
  // visible in the rows.
  EXPECT_EQ(Sorted(Text(hashed.rows)), Sorted(Text(sorted.rows)));
}

TEST(GroupedAggregateTest, TheHashShapeDoesNotReorderItsInputTheSortShapeDoes) {
  // Descending keys, with a duplicate and a NULL key: the hash shape emits the
  // groups in first-appearance order because it never sorts anything, while the
  // sort runs deliver the group-key order (ascending, NULLs first) as a side
  // effect of the sort they have to do anyway. A shape that quietly started
  // sorting -- or stopped -- would move these rows.
  const std::vector<Row> descending = {
      Row({Value(int64_t{3}), Value("c"), Value(3.0)}),
      Row({Value(int64_t{2}), Value("b"), Value(2.0)}),
      Row({Value(int64_t{3}), Value("d"), Value(4.0)}),
      Row({Value(), Value("n"), Value(1.0)}),
      Row({Value(int64_t{1}), Value("a"), Value(0.5)}),
      Row({Value(), Value("m"), Value(1.5)}),
  };
  const std::vector<NamedExpression> keys = {Key("k")};
  const std::vector<NamedExpression> aggregates = {CountStar(), Sum(V())};
  const std::vector<std::string> hash_order = {"3|2|7", "2|1|2", "NULL|2|2.5",
                                               "1|1|0.5"};
  const std::vector<std::string> sort_order = {"NULL|2|2.5", "1|1|0.5", "2|1|2",
                                               "3|2|7"};

  const Collected hashed = Collect(HashShape(descending, keys, aggregates));
  ASSERT_EQ(hashed.status, Status::kSuccess);
  // first appearance: 3, 2, NULL, 1 -- the duplicate folds into the first 3.
  EXPECT_EQ(Text(hashed.rows), hash_order) << Join(Text(hashed.rows));

  const Collected sorted = Collect(SortShape(descending, keys, aggregates));
  ASSERT_EQ(sorted.status, Status::kSuccess);
  EXPECT_EQ(Text(sorted.rows), sort_order) << Join(Text(sorted.rows));

  // The same pair with each shape on its own spill path: the order belongs to
  // the algorithm, not to the run or partition it happened to use.
  GroupedAggregateOptions partitioned;
  partitioned.memory_budget_bytes = kGroupedAggregateEstimatedRowBytes;
  partitioned.spill_partition_count = 4;
  const std::shared_ptr<HashAggregateExecutor> spilled_hash =
      HashShape(descending, keys, aggregates, nullptr, partitioned);
  const Collected spilled = Collect(spilled_hash);
  ASSERT_EQ(spilled.status, Status::kSuccess);
  EXPECT_TRUE(spilled_hash->Spilled());
  EXPECT_EQ(Text(spilled.rows), hash_order)
      << "partitioning must not reorder the group table's first-appearance "
         "order";

  GroupedAggregateOptions runs;
  runs.spill_threshold_rows = 2;
  runs.merge_window_rows = 3;
  const std::shared_ptr<SortAggregateExecutor> run_sort =
      SortShape(descending, keys, aggregates, nullptr, runs);
  const Collected merged = Collect(run_sort);
  ASSERT_EQ(merged.status, Status::kSuccess);
  EXPECT_EQ(Text(merged.rows), sort_order)
      << "the merged runs must deliver the order the in-memory sort delivers";
}

TEST(GroupedAggregateTest, InterleavedKeysFoldIntoTheSameGroupsAsAdjacentOnes) {
  // No two rows of one group are adjacent, so a shape that folded by position
  // instead of by key would answer differently. The hash table looks the key
  // up; the sort runs first make the equal keys adjacent. Both must return the
  // one group per key that the adjacency-based input produces, and each must
  // keep doing so on its own spill path.
  const std::vector<Row> adjacent = MakeManyRows(6, 4);
  const std::vector<Row> interleaved = MakeInterleavedRows(6, 4);
  const std::vector<NamedExpression> keys = {Key("k")};
  const std::vector<NamedExpression> aggregates = {CountStar(), Sum(V()),
                                                   Min(V()), Max(V())};
  const std::vector<std::string> expected =
      GroupsOf(HashShape(adjacent, keys, aggregates));

  const std::shared_ptr<HashAggregateExecutor> hash =
      HashShape(interleaved, keys, aggregates);
  EXPECT_EQ(GroupsOf(hash), expected);
  // The group table reached every key even though no key was ever repeated
  // adjantly, and the whole group set was resident while it did.
  EXPECT_EQ(hash->PeakResidentGroups(), 6U);

  const std::shared_ptr<SortAggregateExecutor> sort =
      SortShape(interleaved, keys, aggregates);
  EXPECT_EQ(GroupsOf(sort), expected);
  EXPECT_EQ(sort->PeakResidentGroups(), 1U);

  // Hash partitioning splits the input by key hash, so a group whose rows
  // landed in different partitions would be counted twice; the partition
  // assignment is by key, which is what makes re-grouping one partition at a
  // time exact.
  GroupedAggregateOptions partitioned;
  partitioned.memory_budget_bytes = kGroupedAggregateEstimatedRowBytes;
  partitioned.spill_partition_count = 4;
  const std::shared_ptr<HashAggregateExecutor> partitioned_hash =
      HashShape(interleaved, keys, aggregates, nullptr, partitioned);
  EXPECT_EQ(GroupsOf(partitioned_hash), expected);
  EXPECT_TRUE(partitioned_hash->Spilled());
  EXPECT_EQ(partitioned_hash->GroupCount(), 6U)
      << "a partitioned build must not emit a group twice";

  // Sorted runs cut the input at row boundaries, so every group is split across
  // runs and only the merge can fold it back into one row.
  GroupedAggregateOptions runs;
  runs.spill_threshold_rows = 3;
  runs.merge_window_rows = 4;
  const std::shared_ptr<SortAggregateExecutor> run_sort =
      SortShape(interleaved, keys, aggregates, nullptr, runs);
  EXPECT_EQ(run_sort->SpillThresholdRows(), 3U);
  EXPECT_EQ(GroupsOf(run_sort), expected);
  EXPECT_EQ(run_sort->GroupCount(), 6U)
      << "a run-spilled build must not emit a group twice";
  // Even when the runs are on disk, the folding state is still one run.
  EXPECT_EQ(run_sort->PeakResidentGroups(), 1U);
}

TEST(GroupedAggregateTest, EachGroupedShapeSpillsOnItsOwnContractOnly) {
  const std::vector<Row> rows = MakeManyRows(32, 4);
  const std::vector<NamedExpression> keys = {Key("k")};
  const std::vector<NamedExpression> aggregates = {CountStar(), Sum(V())};
  const std::vector<std::string> expected =
      GroupsOf(HashShape(rows, keys, aggregates));

  // The hash shape's spill is a key-hash partition set, bounded by the group
  // capacity; the sort shape's spill is a set of sorted runs bounded by a row
  // threshold. The two knobs belong to different shapes -- each shape's own
  // EXPLAIN text names its own spill and never the other's.
  GroupedAggregateOptions hash_spill;
  hash_spill.memory_budget_bytes = 2 * kGroupedAggregateEstimatedRowBytes;
  hash_spill.spill_partition_count = 8;
  const std::shared_ptr<HashAggregateExecutor> partitioned =
      HashShape(rows, keys, aggregates, nullptr, hash_spill);
  EXPECT_EQ(GroupsOf(partitioned), expected);
  EXPECT_TRUE(partitioned->Spilled());
  EXPECT_EQ(partitioned->SpillPartitionCount(), 8U);
  EXPECT_EQ(partitioned->GroupCount(), 32U);
  // The peak is the largest group table this shape ever held, which after a
  // spill is one partition's re-group table rather than the resident one; see
  // APartitionedHashBuildHoldsOnePartitionsGroups.
  EXPECT_GT(partitioned->PeakResidentGroups(), 0U);
  std::ostringstream hash_dump;
  partitioned->Dump(hash_dump, 0);
  EXPECT_NE(hash_dump.str().find("spill=partitioned"), std::string::npos)
      << hash_dump.str();
  EXPECT_NE(hash_dump.str().find("partitions=8"), std::string::npos)
      << hash_dump.str();
  EXPECT_EQ(hash_dump.str().find("run_rows"), std::string::npos)
      << "the hash shape must not report a sort run: " << hash_dump.str();

  GroupedAggregateOptions sort_spill;
  sort_spill.memory_budget_bytes = 2 * kGroupedAggregateEstimatedRowBytes;
  sort_spill.merge_window_rows = 16;
  const std::shared_ptr<SortAggregateExecutor> run_sort =
      SortShape(rows, keys, aggregates, nullptr, sort_spill);
  EXPECT_EQ(GroupsOf(run_sort), expected);
  EXPECT_EQ(run_sort->SpillThresholdRows(), 2U)
      << "the sort shape's run threshold comes from the budget, not from a "
         "partition count";
  EXPECT_EQ(run_sort->Options().spill_partition_count,
            kGroupedAggregateDefaultSpillPartitions)
      << "a partition count is the hash shape's knob; the sort shape keeps the "
         "default it never reads";
  EXPECT_EQ(run_sort->PeakResidentGroups(), 1U)
      << "spilling the runs to disk must not grow the folding state";
  std::ostringstream sort_dump;
  run_sort->Dump(sort_dump, 0);
  EXPECT_NE(sort_dump.str().find("run_rows=2"), std::string::npos)
      << sort_dump.str();
  EXPECT_NE(sort_dump.str().find("window=16"), std::string::npos)
      << sort_dump.str();
  EXPECT_EQ(sort_dump.str().find("partitions"), std::string::npos)
      << "the sort shape must not report a partition: " << sort_dump.str();
  EXPECT_EQ(sort_dump.str().find("spill="), std::string::npos)
      << "only the hash shape has a partition spill: " << sort_dump.str();

  // Neither shape reaches its spill path on an input its memory contract can
  // hold. The hash shape says so; the sort shape never claimed a partition and
  // never needed one, so the witness is the one-group state, not a flag.
  const std::shared_ptr<HashAggregateExecutor> resident =
      HashShape(rows, keys, aggregates);
  EXPECT_EQ(GroupsOf(resident), expected);
  EXPECT_FALSE(resident->Spilled());
  EXPECT_EQ(resident->SpillPartitionCount(), 0U);
  std::ostringstream resident_dump;
  resident->Dump(resident_dump, 0);
  EXPECT_NE(resident_dump.str().find("spill=in-memory"), std::string::npos)
      << resident_dump.str();
}

TEST(GroupedAggregateTest, APartitionedHashBuildHoldsOnePartitionsGroups) {
  // The hash shape's spill re-groups one partition at a time, so its state
  // during the spill is that partition's group table -- not the capacity it
  // spilled at, and not the whole group set either. The partition count is what
  // keeps that one table small, which is why it is part of the shape's memory
  // contract: without a partition count the degenerate shape buffers everything
  // and groups at the end. Either way the answer is the same group set, once
  // per key.
  const std::vector<Row> rows = MakeManyRows(64, 2);
  const std::vector<NamedExpression> keys = {Key("k")};
  const std::vector<NamedExpression> aggregates = {CountStar(), Sum(V())};
  const std::vector<std::string> expected =
      GroupsOf(HashShape(rows, keys, aggregates));

  GroupedAggregateOptions partitioned;
  partitioned.memory_budget_bytes = kGroupedAggregateEstimatedRowBytes;
  partitioned.spill_partition_count = 16;
  const std::shared_ptr<HashAggregateExecutor> wide =
      HashShape(rows, keys, aggregates, nullptr, partitioned);
  EXPECT_EQ(GroupsOf(wide), expected);
  EXPECT_TRUE(wide->Spilled());
  EXPECT_EQ(wide->GroupCount(), 64U);
  // A 16-way split of 64 keys cannot put all of them in one partition, so the
  // re-group tables are strictly smaller than the whole group set; the one that
  // spills is the largest of them.
  EXPECT_GT(wide->PeakResidentGroups(), 0U);
  EXPECT_LT(wide->PeakResidentGroups(), 64U)
      << "one partition must not hold every key of a 16-way split";
  const size_t one_partition = wide->PeakResidentGroups();

  // One partition is the degenerate form of the same algorithm: buffer the
  // rows, then group them. The answer is unchanged, which is what makes the
  // partition count a memory knob rather than a semantic one.
  partitioned.spill_partition_count = 1;
  const std::shared_ptr<HashAggregateExecutor> single =
      HashShape(rows, keys, aggregates, nullptr, partitioned);
  EXPECT_EQ(GroupsOf(single), expected);
  EXPECT_TRUE(single->Spilled());
  EXPECT_EQ(single->SpillPartitionCount(), 1U);
  EXPECT_EQ(single->PeakResidentGroups(), 64U);
  EXPECT_GE(single->PeakResidentGroups(), one_partition);
}

TEST(GroupedAggregateTest, TheEmptyInputReachesNeitherGroupedSpillPath) {
  // No rows, so neither algorithm has anything to hand to its spill: the hash
  // shape never partitions and the sort shape never writes a run. The peak
  // state is the honest witness -- a zero peak means the shape folded nothing.
  const std::vector<Row> empty;
  const std::vector<NamedExpression> keys = {Key("k")};
  const std::vector<NamedExpression> aggregates = {CountStar(), Sum(V())};

  GroupedAggregateOptions partitioned;
  partitioned.memory_budget_bytes = kGroupedAggregateEstimatedRowBytes;
  partitioned.spill_partition_count = 4;
  const std::shared_ptr<HashAggregateExecutor> hash =
      HashShape(empty, keys, aggregates, nullptr, partitioned);
  const Collected hashed = Collect(hash);
  EXPECT_EQ(hashed.status, Status::kSuccess);
  EXPECT_TRUE(hashed.rows.empty());
  EXPECT_FALSE(hash->Spilled());
  EXPECT_EQ(hash->PeakResidentGroups(), 0U);

  GroupedAggregateOptions runs;
  runs.spill_threshold_rows = 1;
  const std::shared_ptr<SortAggregateExecutor> sort =
      SortShape(empty, keys, aggregates, nullptr, runs);
  const Collected sorted = Collect(sort);
  EXPECT_EQ(sorted.status, Status::kSuccess);
  EXPECT_TRUE(sorted.rows.empty());
  EXPECT_EQ(sort->PeakResidentGroups(), 0U);

  // A keyless payload is one group over an empty input on both algorithms, with
  // the same historical scalar values, and each keeps that one group as its
  // whole state.
  const Collected scalar_hash =
      Collect(HashShape(empty, {}, StandardPayload(), nullptr, partitioned));
  ASSERT_EQ(scalar_hash.status, Status::kSuccess);
  ASSERT_EQ(scalar_hash.rows.size(), 1U);
  EXPECT_EQ(hash->PeakResidentGroups(), 0U);
  const std::shared_ptr<SortAggregateExecutor> scalar_sort =
      SortShape(empty, {}, StandardPayload(), nullptr, runs);
  const Collected sorted_scalar = Collect(scalar_sort);
  ASSERT_EQ(sorted_scalar.status, Status::kSuccess);
  ASSERT_EQ(sorted_scalar.rows.size(), 1U);
  EXPECT_EQ(scalar_sort->PeakResidentGroups(), 1U);
  EXPECT_EQ(Text(sorted_scalar.rows), Text(scalar_hash.rows));
  EXPECT_EQ(Text(sorted_scalar.rows),
            std::vector<std::string>{"0|NULL|NULL|NULL"});
}

TEST(GroupedAggregateTest, TheSpillPathsKeepTheNullAndDuplicateContract) {
  // NULL keys (one group), duplicate keys (folded) and NULL measures (skipped)
  // have to survive each algorithm's spill path, because the spill re-groups
  // rows read from a file instead of from the input: a NULL key needs a stable
  // partition, and a duplicate needs a group that re-forms inside one partition
  // or one merged run.
  const std::vector<Row> rows = MakeRows();
  const std::vector<NamedExpression> keys = {Key("k")};
  const std::vector<NamedExpression> aggregates = {
      CountStar(), CountOf(V()), Sum(V()), Min(V()), Max(V())};
  const std::vector<std::string> expected =
      Sorted({"NULL|2|1|8|8|8", "1|3|2|3|1|2", "2|2|1|4|4|4"});

  // The in-memory answer, per algorithm.
  EXPECT_EQ(GroupsOf(HashShape(rows, keys, aggregates)), expected);
  EXPECT_EQ(GroupsOf(SortShape(rows, keys, aggregates)), expected);

  // Partitioned hash, one group at a time.
  GroupedAggregateOptions partitioned;
  partitioned.memory_budget_bytes = kGroupedAggregateEstimatedRowBytes;
  partitioned.spill_partition_count = 2;
  const std::shared_ptr<HashAggregateExecutor> hash =
      HashShape(rows, keys, aggregates, nullptr, partitioned);
  EXPECT_EQ(GroupsOf(hash), expected);
  EXPECT_TRUE(hash->Spilled());
  EXPECT_EQ(hash->GroupCount(), 3U);

  // Sorted runs, two rows per run, so the NULL group and a duplicate group are
  // both cut across a run boundary.
  GroupedAggregateOptions runs;
  runs.spill_threshold_rows = 2;
  runs.merge_window_rows = 3;
  const std::shared_ptr<SortAggregateExecutor> run_sort =
      SortShape(rows, keys, aggregates, nullptr, runs);
  EXPECT_EQ(GroupsOf(run_sort), expected);
  EXPECT_EQ(run_sort->GroupCount(), 3U);
}

TEST(GroupedAggregateTest, TheSortShapeFoldsNanAndSignedZeroKeysAcrossItsRuns) {
  // A DOUBLE key column: NaN, -0.0, 0.0 and 1.0 fold into three peer sets. The
  // hash table canonicalizes the key; the sort shape has to reach the same peer
  // sets through its memcomparable encoding, which is what the spilled runs and
  // the merge window share with the in-memory permutation. A run boundary that
  // cut a peer set in half would emit it twice.
  static const Schema double_key_schema(
      "aggd",
      {Column("k", ValueType::kDouble), Column("v", ValueType::kDouble)});
  const std::vector<Row> rows = {
      Row({Value(std::numeric_limits<double>::quiet_NaN()), Value(1.0)}),
      Row({Value(-0.0), Value(4.0)}),
      Row({Value(std::numeric_limits<double>::quiet_NaN()), Value(2.0)}),
      Row({Value(1.0), Value(16.0)}),
      Row({Value(0.0), Value(8.0)}),
  };
  const std::vector<NamedExpression> keys = {
      NamedExpression("k", ColumnValueExp("k"))};
  const std::vector<NamedExpression> aggregates = {
      CountStar(),
      NamedExpression("total", AggregateExpressionExp(AggregationType::kSum,
                                                      ColumnValueExp("v")))};
  const std::vector<std::string> expected =
      Sorted({"nan|2|3", "0|2|12", "1|1|16"});

  EXPECT_EQ(GroupsOf(MakeShape<HashAggregateExecutor>(double_key_schema, rows,
                                                      keys, aggregates)),
            expected);
  EXPECT_EQ(GroupsOf(MakeShape<SortAggregateExecutor>(double_key_schema, rows,
                                                      keys, aggregates)),
            expected);
  // One row per run: every peer set is cut in half, so each one has to be
  // re-formed by the merge.
  GroupedAggregateOptions runs;
  runs.spill_threshold_rows = 1;
  runs.merge_window_rows = 2;
  const std::shared_ptr<SortAggregateExecutor> run_sort =
      MakeShape<SortAggregateExecutor>(double_key_schema, rows, keys,
                                       aggregates, nullptr, runs);
  EXPECT_EQ(GroupsOf(run_sort), expected);
  EXPECT_EQ(run_sort->GroupCount(), 3U);
  GroupedAggregateOptions partitioned;
  partitioned.memory_budget_bytes = kGroupedAggregateEstimatedRowBytes;
  partitioned.spill_partition_count = 3;
  EXPECT_EQ(
      GroupsOf(MakeShape<HashAggregateExecutor>(
          double_key_schema, rows, keys, aggregates, nullptr, partitioned)),
      expected);
}

TEST(GroupedAggregateTest, AFailingChildIsTheSameErrorOnBothGroupedAlgorithms) {
  // The hash shape reads the child directly and the sort shape reads it through
  // its sort, so the failure travels a different path in each -- but neither
  // may answer with the groups it had already folded.
  const std::vector<Row> rows = MakeManyRows(8, 4);
  const std::vector<NamedExpression> keys = {Key("k")};
  const std::vector<NamedExpression> aggregates = {CountStar(), Sum(V())};
  for (const size_t fail_after : {size_t{0}, size_t{3}, size_t{17}}) {
    const Collected hash = Collect(std::make_shared<HashAggregateExecutor>(
        std::make_shared<FailingRows>(rows, fail_after), InputSchema(), keys,
        aggregates));
    EXPECT_NE(hash.status, Status::kSuccess) << fail_after;
    EXPECT_TRUE(hash.rows.empty()) << fail_after;

    const Collected sort = Collect(std::make_shared<SortAggregateExecutor>(
        std::make_shared<FailingRows>(rows, fail_after), InputSchema(), keys,
        aggregates));
    EXPECT_NE(sort.status, Status::kSuccess) << fail_after;
    EXPECT_TRUE(sort.rows.empty()) << fail_after;

    const Collected stream = Collect(std::make_shared<StreamAggregateExecutor>(
        std::make_shared<FailingRows>(rows, fail_after), InputSchema(), keys,
        aggregates));
    EXPECT_NE(stream.status, Status::kSuccess) << fail_after;
    EXPECT_TRUE(stream.rows.empty()) << fail_after;
  }
  // A child that fails after the sort has already written its runs is still the
  // shape's own error, with no partial group set.
  GroupedAggregateOptions runs;
  runs.spill_threshold_rows = 2;
  runs.merge_window_rows = 4;
  const Collected run_sort = Collect(std::make_shared<SortAggregateExecutor>(
      std::make_shared<FailingRows>(rows, 20), InputSchema(), keys, aggregates,
      nullptr, runs));
  EXPECT_NE(run_sort.status, Status::kSuccess);
  EXPECT_TRUE(run_sort.rows.empty());

  // A hash shape that failed while partitioning has no partial answer either.
  GroupedAggregateOptions partitioned;
  partitioned.memory_budget_bytes = kGroupedAggregateEstimatedRowBytes;
  partitioned.spill_partition_count = 4;
  const Collected spilled = Collect(std::make_shared<HashAggregateExecutor>(
      std::make_shared<FailingRows>(rows, 20), InputSchema(), keys, aggregates,
      nullptr, partitioned));
  EXPECT_NE(spilled.status, Status::kSuccess);
  EXPECT_TRUE(spilled.rows.empty());

  // A failing HAVING is the shape's own error on both algorithms, even though
  // the groups themselves folded fine.
  const Expression missing =
      BinaryExpressionExp(ColumnValueExp("nope"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(1.0)));
  const Collected hash_having =
      Collect(HashShape(MakeRows(), {Key("k")}, StandardPayload(), missing));
  EXPECT_NE(hash_having.status, Status::kSuccess);
  EXPECT_TRUE(hash_having.rows.empty());
  const Collected sort_having =
      Collect(SortShape(MakeRows(), {Key("k")}, StandardPayload(), missing));
  EXPECT_NE(sort_having.status, Status::kSuccess);
  EXPECT_TRUE(sort_having.rows.empty());
}

// --------------------------------------------------- the stream order contract
// --

TEST(GroupedAggregateTest, TheStreamShapeRefusesAnUnorderedInput) {
  const std::vector<Row> unordered = MakeRows();
  const Collected collected =
      Collect(StreamShape(unordered, {Key("k")}, StandardPayload()));
  EXPECT_NE(collected.status, Status::kSuccess);
  EXPECT_TRUE(collected.rows.empty());
  // The shape says so in its EXPLAIN text, once it has actually checked.
  std::ostringstream dump;
  std::shared_ptr<StreamAggregateExecutor> shape =
      StreamShape(unordered, {Key("k")}, StandardPayload());
  EXPECT_NE(Collect(shape).status, Status::kSuccess);
  shape->Dump(dump, 0);
  EXPECT_NE(dump.str().find("StreamAggregate"), std::string::npos)
      << dump.str();
  EXPECT_NE(dump.str().find("order=checked"), std::string::npos) << dump.str();
}

TEST(GroupedAggregateTest, TheStreamShapeAcceptsThePromisedInputOrder) {
  const std::shared_ptr<StreamAggregateExecutor> stream =
      StreamShape(MakeRowsOrdered(), {Key("k")}, StandardPayload());
  const Collected collected = Collect(stream);
  EXPECT_EQ(collected.status, Status::kSuccess);
  EXPECT_TRUE(stream->InputOrderChecked());
  // The output keeps the input's group order, so a consumer above may rely on
  // it exactly like the sort shape's own order.
  EXPECT_EQ(
      Text(collected.rows),
      (std::vector<std::string>{"NULL|2|8|8|8", "1|3|3|1|2", "2|2|4|4|4"}));
}

TEST(GroupedAggregateTest, TheStreamShapeRefusesNullsThatAreNotFirst) {
  // Same groups, but the NULL key arrives last: the promised NULLS FIRST order
  // is broken even though the ascending order of the non-NULL keys holds.
  const std::vector<Row> nulls_last = {
      Row({Value(int64_t{1}), Value("a"), Value(1.0)}),
      Row({Value(int64_t{2}), Value("a"), Value(2.0)}),
      Row({Value(), Value("a"), Value(3.0)}),
  };
  const Collected collected =
      Collect(StreamShape(nulls_last, {Key("k")}, StandardPayload()));
  EXPECT_NE(collected.status, Status::kSuccess);
  EXPECT_TRUE(collected.rows.empty());
}

TEST(GroupedAggregateTest, AKeylessPayloadNeedsNoInputOrder) {
  // With no key there is exactly one group, so the stream shape is valid over
  // any input order -- and says so instead of failing.
  const Collected collected =
      Collect(StreamShape(MakeRows(), {}, StandardPayload()));
  EXPECT_EQ(collected.status, Status::kSuccess);
  ASSERT_EQ(collected.rows.size(), 1U);
  EXPECT_EQ(collected.rows[0][0], Value(int64_t{7}));
}

// ------------------------------------------------------------------ parallel
// --

TEST(GroupedAggregateTest, TheParallelHashBuildMatchesTheSerialOne) {
  const std::vector<Row> rows = MakeManyRows(96, 5);
  const std::vector<NamedExpression> keys = {Key("k")};
  const std::vector<NamedExpression> aggregates = {CountStar(), Sum(V()),
                                                   Min(V()), Max(V())};
  const std::vector<std::string> expected =
      GroupsOf(HashShape(rows, keys, aggregates));

  GroupedAggregateOptions parallel;
  parallel.worker_count = 4;
  const auto workers = HashShape(rows, keys, aggregates, nullptr, parallel);
  EXPECT_TRUE(workers->ParallelCapable());
  EXPECT_EQ(workers->WorkerCount(), 4U);
  const Collected collected = Collect(workers);
  EXPECT_EQ(collected.status, Status::kSuccess);
  EXPECT_EQ(GroupsOf(HashShape(rows, keys, aggregates, nullptr, parallel)),
            expected);

  // A payload no worker may read positionally (an expression argument) keeps
  // the serial build rather than being guessed at.
  const Expression computed =
      BinaryExpressionExp(V(), BinaryOperation::kAdd, V());
  GroupedAggregateOptions not_positional;
  not_positional.worker_count = 4;
  const auto serial = HashShape(
      rows, keys,
      {NamedExpression(
          "doubled", AggregateExpressionExp(AggregationType::kSum, computed))},
      nullptr, not_positional);
  EXPECT_FALSE(serial->ParallelCapable());
  EXPECT_EQ(serial->WorkerCount(), 1U);
  EXPECT_EQ(
      GroupsOf(serial),
      GroupsOf(HashShape(
          rows, keys,
          {NamedExpression("doubled", AggregateExpressionExp(
                                          AggregationType::kSum, computed))})));
}

TEST(GroupedAggregateTest, OnlyTheHashShapeFoldsItsInputInParallel) {
  // A worker count is the hash table's knob: the workers each fold a private
  // group table and the merge phase puts them together, so a mis-estimated
  // count can change the group order but not the groups. The sort runs have no
  // such phase -- they fold the equal runs in one pass -- so the same knob is
  // not theirs, and asking for workers must neither fan the shape out nor
  // change a single row.
  const std::vector<Row> rows = MakeManyRows(40, 4);
  const std::vector<NamedExpression> keys = {Key("k")};
  const std::vector<NamedExpression> aggregates = {CountStar(), Sum(V()),
                                                   Min(V()), Max(V())};
  const std::vector<std::string> expected =
      GroupsOf(HashShape(rows, keys, aggregates));

  GroupedAggregateOptions parallel;
  parallel.worker_count = 4;
  const std::shared_ptr<HashAggregateExecutor> workers =
      HashShape(rows, keys, aggregates, nullptr, parallel);
  EXPECT_TRUE(workers->ParallelCapable());
  EXPECT_EQ(workers->WorkerCount(), 4U);
  EXPECT_EQ(GroupsOf(workers), expected);
  // Each worker folds its own table and the merge phase unions them, so the
  // shape is still state-hungry: this is the table, not a single pass.
  EXPECT_EQ(workers->PeakResidentGroups(), 40U);
  std::ostringstream dump;
  workers->Dump(dump, 0);
  EXPECT_NE(dump.str().find("workers=4"), std::string::npos) << dump.str();

  const std::shared_ptr<SortAggregateExecutor> serial =
      SortShape(rows, keys, aggregates);
  const std::vector<std::string> serial_groups = GroupsOf(serial);
  const std::shared_ptr<SortAggregateExecutor> asked =
      SortShape(rows, keys, aggregates, nullptr, parallel);
  EXPECT_EQ(GroupsOf(asked), serial_groups);
  EXPECT_EQ(asked->PeakResidentGroups(), 1U);
  EXPECT_EQ(serial_groups, expected);
  std::ostringstream sort_dump;
  asked->Dump(sort_dump, 0);
  EXPECT_EQ(sort_dump.str().find("workers"), std::string::npos)
      << "the sort shape has no worker phase: " << sort_dump.str();

  // A worker build and a spilling build are two different answers to "the group
  // table does not fit": the workers union private tables (so the shape is
  // bounded by the distinct key count, not by the capacity), while a serial
  // build partitions its input and re-groups one partition at a time (so the
  // resident table stops at the capacity). A worker count therefore replaces
  // the partition spill rather than adding to it, and the groups are the same
  // either way.
  GroupedAggregateOptions spilled;
  spilled.memory_budget_bytes = 4 * kGroupedAggregateEstimatedRowBytes;
  spilled.spill_partition_count = 4;
  const std::shared_ptr<HashAggregateExecutor> partitioned =
      HashShape(rows, keys, aggregates, nullptr, spilled);
  EXPECT_EQ(GroupsOf(partitioned), expected);
  EXPECT_TRUE(partitioned->Spilled());
  EXPECT_EQ(partitioned->GroupCapacity(), 4U);
  spilled.worker_count = 4;
  const std::shared_ptr<HashAggregateExecutor> workers_over_budget =
      HashShape(rows, keys, aggregates, nullptr, spilled);
  EXPECT_EQ(GroupsOf(workers_over_budget), expected);
  EXPECT_FALSE(workers_over_budget->Spilled())
      << "the worker build folds private tables instead of partitioning";
  EXPECT_EQ(workers_over_budget->PeakResidentGroups(), 40U)
      << "the worker build merges private tables, so the group capacity does "
         "not bound it";
}

// --------------------------------------------------------------- row output --

TEST(GroupedAggregateTest, NextAndNextBatchAgreeAndExposeTheOutputSchema) {
  const std::vector<Row> rows = MakeRows();
  const std::shared_ptr<HashAggregateExecutor> hash =
      HashShape(rows, {Key("k")}, StandardPayload());
  // keys then aggregates, in GROUP BY / target-list order
  const Schema& schema = hash->OutputSchema();
  ASSERT_EQ(schema.ColumnCount(), 5U);
  EXPECT_EQ(schema.GetColumn(0).Name().name, "k");
  EXPECT_EQ(schema.GetColumn(0).Type(), ValueType::kInt64);
  EXPECT_EQ(schema.GetColumn(1).Name().name, "n");
  EXPECT_EQ(schema.GetColumn(2).Name().name, "total");
  EXPECT_EQ(schema.GetColumn(3).Name().name, "lo");
  EXPECT_EQ(schema.GetColumn(4).Name().name, "hi");

  std::vector<Row> via_batch;
  DataChunk chunk;
  while (hash->NextBatch(&chunk) > 0) {
    for (size_t row = 0; row < chunk.Size(); ++row) {
      via_batch.push_back(chunk.RowAt(row));
    }
    if (via_batch.size() > 1) {
      break;  // one batch is enough: the first batch holds every group
    }
  }
  const Collected via_next =
      Collect(HashShape(rows, {Key("k")}, StandardPayload()));
  ASSERT_EQ(via_next.status, Status::kSuccess);
  EXPECT_EQ(Text(via_batch), Text(via_next.rows));
  EXPECT_EQ(hash->GroupCount(), 3U);
}

TEST(GroupedAggregateTest, AGroupOutputOrderIsIndependentOfTheShape) {
  // Whatever order a shape emits groups in, an ORDER BY above it restores the
  // query's own order: the three shapes must not change an ORDER BY result.
  const std::vector<Row> rows = MakeRows();
  const Expression by_key = ColumnValueExp("k");
  const auto ordered = [&by_key](Executor executor) {
    auto sort = std::make_shared<SortExecutor>(
        std::move(executor),
        Schema("grouped_agg",
               {Column("k", ValueType::kInt64), Column("n", ValueType::kInt64),
                Column("total", ValueType::kDouble),
                Column("lo", ValueType::kDouble),
                Column("hi", ValueType::kDouble)}),
        std::vector<SortExecutor::Key>{{.expression = by_key,
                                        .ascending = true,
                                        .nulls_first = std::nullopt}});
    return GroupsOf(sort);
  };
  const std::vector<std::string> expected =
      Sorted({"NULL|2|8|8|8", "1|3|3|1|2", "2|2|4|4|4"});
  EXPECT_EQ(ordered(HashShape(rows, {Key("k")}, StandardPayload())), expected);
  EXPECT_EQ(ordered(SortShape(rows, {Key("k")}, StandardPayload())), expected);
  EXPECT_EQ(
      ordered(StreamShape(MakeRowsOrdered(), {Key("k")}, StandardPayload())),
      expected);
}

// ------------------------------------------------------------ SQL differential
// --

class GroupedAggregateSqlTest : public ::testing::Test {
 protected:
  void SetUp() override {
    database_ =
        Database::Create("grouped_aggregate-" + RandomString()).MoveValue();
    TransactionContext setup = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, table,
        database_->CreateTable(
            setup, Schema("sales", {Column("dept_id", ValueType::kInt64),
                                    Column("rep", ValueType::kVarChar),
                                    Column("amount", ValueType::kDouble)})));
    // Duplicate keys, a NULL key, NULL amounts and a name that repeats inside
    // one group (so COUNT(DISTINCT rep) has something to fold).
    for (const auto& [dept, rep, amount] :
         {std::tuple{Value(int64_t{10}), Value("a"), Value(100.0)},
          std::tuple{Value(int64_t{10}), Value("a"), Value(50.0)},
          std::tuple{Value(int64_t{10}), Value("b"), Value()},
          std::tuple{Value(int64_t{20}), Value("c"), Value(200.0)},
          std::tuple{Value(int64_t{20}), Value("c"), Value(100.0)},
          std::tuple{Value(), Value("d"), Value(75.0)}}) {
      ASSERT_SUCCESS(
          table.Insert(setup.txn_, Row({dept, rep, amount})).GetStatus());
    }
    ASSERT_SUCCESS(setup.txn_.PreCommit());
  }
  void TearDown() override { database_->DeleteAll(); }

  Status Run(std::string_view sql, std::vector<Row>* rows) {
    TransactionContext context = database_->BeginContext();
    SqlEngine engine(*database_);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    if (!prepared.HasValue()) {
      last_error_ = engine.LastError();
      context.Abort();
      return prepared.GetStatus();
    }
    *rows = Collect(prepared.Value()).rows;
    Status status = prepared.Value()->GetStatus();
    if (status != Status::kSuccess) {
      last_error_ = engine.LastError();
      context.Abort();
      return status;
    }
    const Status committed = context.PreCommit();
    if (committed != Status::kSuccess) {
      last_error_ = engine.LastError();
    }
    return committed;
  }

  // The rows of `sales` as the engine's own GROUP BY computes them, in a
  // canonical text form the shapes can be compared against.
  std::vector<std::string> SqlGroups(std::string_view sql) {
    std::vector<Row> rows;
    const Status status = Run(sql, &rows);
    EXPECT_EQ(status, Status::kSuccess) << sql << ": " << last_error_;
    return Text(rows);
  }

  static std::vector<Row> SalesRows() {
    return {Row({Value(int64_t{10}), Value("a"), Value(100.0)}),
            Row({Value(int64_t{10}), Value("a"), Value(50.0)}),
            Row({Value(int64_t{10}), Value("b"), Value()}),
            Row({Value(int64_t{20}), Value("c"), Value(200.0)}),
            Row({Value(int64_t{20}), Value("c"), Value(100.0)}),
            Row({Value(), Value("d"), Value(75.0)})};
  }

  static const Schema& SalesSchema() {
    static const Schema schema("sales", {Column("dept_id", ValueType::kInt64),
                                         Column("rep", ValueType::kVarChar),
                                         Column("amount", ValueType::kDouble)});
    return schema;
  }

  std::string last_error_{};
  std::unique_ptr<Database> database_;
};

TEST_F(GroupedAggregateSqlTest, EveryShapeAgreesWithTheEnginesGroupBy) {
  const std::vector<Row> rows = SalesRows();
  const std::vector<NamedExpression> keys = {
      NamedExpression("dept_id", ColumnValueExp("dept_id"))};
  std::vector<NamedExpression> aggregates = {
      NamedExpression("n",
                      AggregateExpressionExp(AggregationType::kCount, nullptr)),
      NamedExpression("total",
                      AggregateExpressionExp(AggregationType::kSum,
                                             ColumnValueExp("amount"))),
      NamedExpression("reps", AggregateExpressionExp(AggregationType::kCount,
                                                     ColumnValueExp("rep"))),
      NamedExpression(
          "reps_distinct",
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("rep"),
                                 /*distinct=*/true))};

  // The engine's own GROUP BY, ordered by the key (NULLs first).
  const std::vector<std::string> from_sql = SqlGroups(
      "SELECT dept_id, COUNT(*) AS n, SUM(amount) AS total, "
      "COUNT(rep) AS reps, COUNT(DISTINCT rep) AS reps_distinct "
      "FROM sales "
      "GROUP BY dept_id ORDER BY dept_id");
  EXPECT_EQ(from_sql, (std::vector<std::string>{"NULL|1|75|1|1", "10|3|150|3|2",
                                                "20|2|300|2|1"}));

  const std::vector<NamedExpression> sql_order_keys = {keys.front()};
  const auto sort_above = [&](Executor executor) {
    auto sort = std::make_shared<SortExecutor>(
        std::move(executor),
        Schema("grouped_agg", {Column("dept_id", ValueType::kInt64),
                               Column("n", ValueType::kInt64),
                               Column("total", ValueType::kDouble),
                               Column("reps", ValueType::kInt64),
                               Column("reps_distinct", ValueType::kInt64)}),
        std::vector<SortExecutor::Key>{{.expression = ColumnValueExp("dept_id"),
                                        .ascending = true,
                                        .nulls_first = std::nullopt}});
    Collected collected = Collect(sort);
    EXPECT_EQ(collected.status, Status::kSuccess);
    return Text(collected.rows);
  };
  const auto build = [&](bool hash, bool sort) {
    std::shared_ptr<ExecutorBase> child = Rows(rows);
    if (sort) {
      child = std::make_shared<SortExecutor>(
          child, SalesSchema(),
          std::vector<SortExecutor::Key>{
              {.expression = ColumnValueExp("dept_id"),
               .ascending = true,
               .nulls_first = std::nullopt}});
    }
    if (hash) {
      return std::static_pointer_cast<ExecutorBase>(
          std::make_shared<HashAggregateExecutor>(child, SalesSchema(),
                                                  sql_order_keys, aggregates));
    }
    return std::static_pointer_cast<ExecutorBase>(
        std::make_shared<StreamAggregateExecutor>(child, SalesSchema(),
                                                  sql_order_keys, aggregates));
  };
  EXPECT_EQ(sort_above(build(/*hash=*/true, /*sort=*/false)), from_sql);
  EXPECT_EQ(sort_above(build(/*hash=*/false, /*sort=*/false)), from_sql);
  // Over an already ordered input the streaming shape needs no sort above it.
  EXPECT_EQ(Text(Collect(build(/*hash=*/false, /*sort=*/true)).rows), from_sql);
}

TEST_F(GroupedAggregateSqlTest, TheEnginesHavingAndGroupingSetsStillWork) {
  // The shapes are a physical detail of the aggregation node: the engine's own
  // GROUP BY / HAVING / ROLLUP results must be untouched by their existence.
  EXPECT_EQ(SqlGroups("SELECT dept_id, COUNT(*) AS n FROM sales "
                      "GROUP BY dept_id HAVING COUNT(*) >= 2 ORDER BY dept_id"),
            (std::vector<std::string>{"10|3", "20|2"}));
  EXPECT_EQ(SqlGroups("SELECT COUNT(*) AS n FROM sales"),
            std::vector<std::string>{"6"});
  EXPECT_EQ(SqlGroups("SELECT COUNT(*) AS n FROM sales WHERE dept_id > 100"),
            std::vector<std::string>{"0"});
}

}  // namespace tinylamb
