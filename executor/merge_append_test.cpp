/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache License 2.0. */
#include <cstdint>
#include <memory>
#include <vector>

#include "executor/merge_append.hpp"
#include "gtest/gtest.h"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

class FakeSortedSource : public ExecutorBase {
 public:
  explicit FakeSortedSource(std::vector<int64_t> keys) : keys_(std::move(keys)) {}
  bool Next(Row* row, RowPosition* rp) override {
    if (idx_ >= keys_.size()) { return false; }
    *row = Row(std::vector<Value>{Value(keys_[idx_++])});
    if (rp) { *rp = RowPosition{}; }
    return true;
  }
  size_t NextBatch(DataChunk*, size_t) override { return 0; }
  void Dump(std::ostream& o, int) const override { o << "fake"; }

 private:
  std::vector<int64_t> keys_;
  size_t idx_{0};
};

std::vector<Executor> Sources(std::vector<std::vector<int64_t>> all) {
  std::vector<Executor> out;
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
  std::vector<SortExecutor::Key> keys{SortExecutor::Key{ColumnValueExp("k"), true}};
  MergeAppendExecutor ex(Sources(std::move(inputs)), Schemas(inputs.size(), schema),
                          schema, std::move(keys));
  std::vector<int64_t> out;
  Row row;
  while (ex.Next(&row, nullptr)) {
    EXPECT_EQ(row.values_.size(), 1u);
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
  ASSERT_EQ(out.size(), 5u);
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

}  // namespace
}  // namespace tinylamb
