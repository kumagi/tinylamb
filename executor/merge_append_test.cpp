/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache License 2.0. */
#include "executor/merge_append.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/sort.hpp"
#include "expression/expression.hpp"
#include "gtest/gtest.h"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

class FakeSortedSource : public ExecutorBase {
 public:
  explicit FakeSortedSource(std::vector<int64_t> keys)
      : keys_(std::move(keys)) {}
  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    destination->Reset();
    while (idx_ < keys_.size() && destination->Size() < max_rows) {
      destination->Append(Row(std::vector<Value>{Value(keys_[idx_++])}));
    }
    return destination->Size();
  }
  void Dump(std::ostream& o, int /*indent*/) const override { o << "fake"; }

 private:
  std::vector<int64_t> keys_{};
  size_t idx_{0};
};

std::vector<Executor> Sources(std::vector<std::vector<int64_t>> all) {
  std::vector<Executor> out;
  out.reserve(all.size());
  for (auto& keys : all) {
    out.emplace_back(std::make_shared<FakeSortedSource>(std::move(keys)));
  }
  return out;
}

std::vector<Schema> Schemas(size_t n, const Schema& s) {
  return std::vector<Schema>(n, s);
}

std::vector<int64_t> RunMerge(std::vector<std::vector<int64_t>> inputs) {
  Schema schema("", {Column("k", ValueType::kInt64)});
  std::vector<SortExecutor::Key> keys{
      SortExecutor::Key{.expression = ColumnValueExp("k"), .ascending = true}};
  const size_t input_count = inputs.size();
  MergeAppendExecutor ex(Sources(std::move(inputs)),
                         Schemas(input_count, schema), schema, std::move(keys));
  std::vector<int64_t> out;
  Row row;
  while (ex.Next(&row, nullptr)) {
    EXPECT_EQ(row.values_.size(), 1U);
    out.push_back(row.values_[0].value.int_value);
  }
  return out;
}

TEST(MergeAppendExecutorTest, MergesTwoSortedStreams) {
  auto out = RunMerge({{1, 3, 5}, {2, 4, 6}});
  EXPECT_EQ(out, std::vector<int64_t>({1, 2, 3, 4, 5, 6}));
}

TEST(MergeAppendExecutorTest, PreservesUnionAllMultiplicity) {
  auto out = RunMerge({{1, 1, 2}, {1, 3}});
  // 5 rows total, non-decreasing (the static merge is stable per source).
  ASSERT_EQ(out.size(), 5U);
  for (size_t i = 1; i < out.size(); ++i) {
    EXPECT_LE(out[i - 1], out[i]);
  }
}

TEST(MergeAppendExecutorTest, SingleSourcePassesThrough) {
  auto out = RunMerge({{5, 4, 3, 2, 1}});
  EXPECT_EQ(out, std::vector<int64_t>({5, 4, 3, 2, 1}));
}

TEST(MergeAppendExecutorTest, EmptySourcesProduceNoRows) {
  auto out = RunMerge({{}, {}});
  EXPECT_TRUE(out.empty());
}

// A sorted source that yields `fail_after` keys and then fails, standing in
// for a partition scan that dies mid-input.
class FailingSortedSource : public ExecutorBase {
 public:
  FailingSortedSource(std::vector<int64_t> keys, size_t fail_after)
      : keys_(std::move(keys)), fail_after_(fail_after) {}
  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    destination->Reset();
    if (idx_ >= fail_after_) {
      FailWith(StatusError(StatusCode::kIOError, "source failed"));
      return 0;
    }
    if (max_rows == 0 || idx_ >= keys_.size()) {
      return 0;
    }
    destination->Append(Row(std::vector<Value>{Value(keys_[idx_++])}));
    return destination->Size();
  }
  void Dump(std::ostream& o, int /*indent*/) const override { o << "failing"; }

 private:
  std::vector<int64_t> keys_{};
  size_t fail_after_{0};
  size_t idx_{0};
};

TEST(MergeAppendExecutorTest, FailingSourceDuringInitializeSurfacesError) {
  Schema schema("", {Column("k", ValueType::kInt64)});
  std::vector<SortExecutor::Key> keys{
      SortExecutor::Key{.expression = ColumnValueExp("k"), .ascending = true}};
  MergeAppendExecutor ex(
      {std::make_shared<FailingSortedSource>(std::vector<int64_t>{1, 2}, 0),
       std::make_shared<FakeSortedSource>(std::vector<int64_t>{3, 4})},
      Schemas(2, schema), schema, keys);
  Row row;
  EXPECT_FALSE(ex.Next(&row, nullptr));
  EXPECT_NE(ex.GetStatus(), Status::kSuccess);
}

TEST(MergeAppendExecutorTest, FailingSourceMidMergeSuppressesPartialOutput) {
  Schema schema("", {Column("k", ValueType::kInt64)});
  std::vector<SortExecutor::Key> keys{
      SortExecutor::Key{.expression = ColumnValueExp("k"), .ascending = true}};
  // Source 0 fails on its refill: the merge must stop with the sticky error
  // instead of serving the surviving stream as a complete result.
  MergeAppendExecutor ex(
      {std::make_shared<FailingSortedSource>(std::vector<int64_t>{1, 2}, 1),
       std::make_shared<FakeSortedSource>(std::vector<int64_t>{3, 4})},
      Schemas(2, schema), schema, keys);
  Row row;
  std::vector<int64_t> out;
  while (ex.Next(&row, nullptr)) {
    out.push_back(row.values_[0].value.int_value);
  }
  EXPECT_NE(ex.GetStatus(), Status::kSuccess);
}

TEST(MergeAppendExecutorTest, ExhaustedThenFailingSourceReportsError) {
  Schema schema("", {Column("k", ValueType::kInt64)});
  std::vector<SortExecutor::Key> keys{
      SortExecutor::Key{.expression = ColumnValueExp("k"), .ascending = true}};
  // Source 1 fails after its only key: rows from the healthy source come out
  // first, then the merge ends with the sticky error (never clean EOF).
  MergeAppendExecutor ex(
      {std::make_shared<FakeSortedSource>(std::vector<int64_t>{1}),
       std::make_shared<FailingSortedSource>(std::vector<int64_t>{2}, 1)},
      Schemas(2, schema), schema, keys);
  Row row;
  std::vector<int64_t> out;
  while (ex.Next(&row, nullptr)) {
    out.push_back(row.values_[0].value.int_value);
  }
  EXPECT_EQ(out, std::vector<int64_t>({1}));
  EXPECT_NE(ex.GetStatus(), Status::kSuccess);
}

namespace {
// A source that yields pre-built rows verbatim (NULL / mixed-typed keys).
class FakeRowSource : public ExecutorBase {
 public:
  explicit FakeRowSource(std::vector<Row> rows) : rows_(std::move(rows)) {}
  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    destination->Reset();
    while (idx_ < rows_.size() && destination->Size() < max_rows) {
      destination->Append(rows_[idx_++]);
    }
    return destination->Size();
  }
  void Dump(std::ostream& o, int /*indent*/) const override {
    o << "fake rows";
  }

 private:
  std::vector<Row> rows_{};
  size_t idx_{0};
};
}  // namespace

TEST(MergeAppendExecutorTest, NullKeysOrderAfterValuesWhenAscending) {
  Schema schema("", {Column("k", ValueType::kInt64)});
  std::vector<SortExecutor::Key> keys{
      SortExecutor::Key{.expression = ColumnValueExp("k"), .ascending = true}};
  std::vector<Executor> sources;
  sources.emplace_back(std::make_shared<FakeRowSource>(
      std::vector<Row>{Row(std::vector<Value>{Value(int64_t{2})}),
                       Row(std::vector<Value>{Value()}),
                       Row(std::vector<Value>{Value(int64_t{1})})}));
  MergeAppendExecutor ex(std::move(sources), {schema}, schema, std::move(keys));
  std::vector<int64_t> non_null;
  size_t nulls = 0;
  Row row;
  while (ex.Next(&row, nullptr)) {
    if (row.values_[0].IsNull()) {
      ++nulls;
    } else {
      non_null.push_back(row.values_[0].value.int_value);
    }
  }
  // Ascending default puts NULLs last; the real rows keep their order.
  EXPECT_EQ(nulls, 1U);
  EXPECT_EQ(non_null, std::vector<int64_t>({2, 1}));
}

TEST(MergeAppendExecutorTest, Int64KeysPromoteIntoDoubleSchema) {
  Schema schema("", {Column("k", ValueType::kDouble)});
  std::vector<SortExecutor::Key> keys{
      SortExecutor::Key{.expression = ColumnValueExp("k"), .ascending = true}};
  // Children are sorted by the caller; the int64 values coerce into the
  // double-typed output key at comparison time.
  std::vector<Executor> sources;
  sources.emplace_back(std::make_shared<FakeRowSource>(
      std::vector<Row>{Row(std::vector<Value>{Value(int64_t{1})}),
                       Row(std::vector<Value>{Value(2.5)})}));
  sources.emplace_back(std::make_shared<FakeRowSource>(
      std::vector<Row>{Row(std::vector<Value>{Value(int64_t{3})})}));
  MergeAppendExecutor ex(std::move(sources), std::vector<Schema>(2, schema),
                         schema, std::move(keys));
  std::vector<double> out;
  Row row;
  while (ex.Next(&row, nullptr)) {
    EXPECT_EQ(row.values_[0].type, ValueType::kDouble);
    out.push_back(row.values_[0].value.double_value);
  }
  EXPECT_EQ(out, std::vector<double>({1.0, 2.5, 3.0}));
}

TEST(MergeAppendExecutorTest, IncompatibleKeyTypesFailTheExecutor) {
  Schema schema("", {Column("k", ValueType::kInt64)});
  std::vector<SortExecutor::Key> keys{
      SortExecutor::Key{.expression = ColumnValueExp("k"), .ascending = true}};
  std::vector<Executor> sources;
  sources.emplace_back(std::make_shared<FakeRowSource>(
      std::vector<Row>{Row(std::vector<Value>{Value(int64_t{1})}),
                       Row(std::vector<Value>{Value("not a number")})}));
  MergeAppendExecutor ex(std::move(sources), {schema}, schema, std::move(keys));
  // The first row passes through; the incompatible key surfaces when the
  // offending row becomes a merge head.
  Row row;
  ASSERT_TRUE(ex.Next(&row, nullptr));
  EXPECT_FALSE(ex.Next(&row, nullptr));
  EXPECT_NE(ex.GetStatus(), Status::kSuccess);
}

TEST(MergeAppendExecutorTest, UnnamedChildFallsBackToOutputPosition) {
  Schema schema("", {Column("k", ValueType::kInt64)});
  // A child schema without the key's name reads the ordering value
  // positionally from the output layout.
  Schema child_schema("", {Column("unrelated", ValueType::kInt64)});
  std::vector<SortExecutor::Key> keys{
      SortExecutor::Key{.expression = ColumnValueExp("k"), .ascending = true}};
  std::vector<Executor> sources;
  sources.emplace_back(std::make_shared<FakeRowSource>(
      std::vector<Row>{Row(std::vector<Value>{Value(int64_t{1})})}));
  sources.emplace_back(std::make_shared<FakeRowSource>(
      std::vector<Row>{Row(std::vector<Value>{Value(int64_t{2})})}));
  MergeAppendExecutor ex(std::move(sources),
                         std::vector<Schema>(2, child_schema), schema,
                         std::move(keys));
  std::vector<int64_t> out;
  Row row;
  while (ex.Next(&row, nullptr)) {
    out.push_back(row.values_[0].value.int_value);
  }
  EXPECT_EQ(out, std::vector<int64_t>({1, 2}));
}

}  // namespace
}  // namespace tinylamb
