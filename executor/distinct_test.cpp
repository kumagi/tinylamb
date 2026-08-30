/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache License 2.0. */
#include <cmath>
#include <cstdint>
#include <memory>
#include <vector>

#include "executor/distinct.hpp"
#include "gtest/gtest.h"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

class FakeSource : public ExecutorBase {
 public:
  explicit FakeSource(std::vector<Row> rows) : rows_(std::move(rows)) {}
  bool Next(Row* row, RowPosition* rp) override {
    if (idx_ >= rows_.size()) { return false; }
    *row = rows_[idx_++];
    if (rp) { *rp = RowPosition{}; }
    return true;
  }
  size_t NextBatch(DataChunk*, size_t) override { return 0; }
  void Dump(std::ostream& o, int) const override { o << "fake"; }

 private:
  std::vector<Row> rows_;
  size_t idx_{0};
};

std::vector<Row> Rows(std::vector<Value> values) {
  std::vector<Row> out;
  out.reserve(values.size());
  for (auto& v : values) { out.emplace_back(std::vector<Value>{std::move(v)}); }
  return out;
}

int CountDistinct(std::vector<Row> rows) {
  DistinctExecutor ex(std::make_shared<FakeSource>(std::move(rows)));
  Row r;
  int n = 0;
  while (ex.Next(&r, nullptr)) { ++n; }
  return n;
}

int CountSortDistinct(std::vector<Row> rows) {
  // SortDistinctExecutor only collapses adjacent equal rows, so feed the
  // input pre-sorted (the sort plan that precedes it guarantees this).
  SortDistinctExecutor ex(std::make_shared<FakeSource>(std::move(rows)));
  Row r;
  int n = 0;
  while (ex.Next(&r, nullptr)) { ++n; }
  return n;
}

TEST(DistinctExecutorTest, CollapsesAllNan) {
  const double nan = std::nan("");
  std::vector<Row> rows;
  rows.insert(rows.end(), 9, Row(std::vector<Value>{Value(nan)}));
  EXPECT_EQ(CountDistinct(std::move(rows)), 1);
}

TEST(DistinctExecutorTest, CollapsesSignedZeros) {
  std::vector<Value> vals{Value(-0.0), Value(0.0), Value(+0.0)};
  EXPECT_EQ(CountDistinct(Rows(std::move(vals))), 1);
}

TEST(DistinctExecutorTest, CollapsesNulls) {
  std::vector<Value> vals{Value(), Value()};
  EXPECT_EQ(CountDistinct(Rows(std::move(vals))), 1);
}

TEST(DistinctExecutorTest, CollapsesInfinities) {
  std::vector<Value> vals{Value(INFINITY), Value(INFINITY), Value(-INFINITY),
                           Value(-INFINITY)};
  EXPECT_EQ(CountDistinct(Rows(std::move(vals))), 2);
}

TEST(DistinctExecutorTest, DistinctDoublesScenarioMatchesGoogleSQL) {
  const double nan = std::nan("");
  std::vector<Value> vals;
  auto d = [&](double x) { vals.emplace_back(Value(x)); };
  for (double x : {-0.0, +0.0, 0.0, -0.0, 0.0, +0.0, -0.0, 0.0}) { d(x); }
  for (int k = 0; k < 9; ++k) { d(nan); }
  d(INFINITY);
  d(-INFINITY);
  d(1.0);
  d(-1.0);
  d(1.0);
  d(-1.0);
  vals.emplace_back(Value());
  // GoogleSQL DISTINCT collapses to {NULL, nan, -inf, -1, 0, 1, inf} = 7.
  EXPECT_EQ(CountDistinct(Rows(std::move(vals))), 7);
}

TEST(DistinctExecutorTest, TypeSensitiveIntVsDouble) {
  std::vector<Value> vals{Value(int64_t{1}), Value(1.0)};
  EXPECT_EQ(CountDistinct(Rows(std::move(vals))), 2);
}

TEST(SortDistinctExecutorTest, CollapsesAdjacentNans) {
  const double nan = std::nan("");
  std::vector<Row> rows;
  rows.insert(rows.end(), 5, Row(std::vector<Value>{Value(nan)}));
  EXPECT_EQ(CountSortDistinct(std::move(rows)), 1);
}

TEST(SortDistinctExecutorTest, CollapsesAdjacentSignedZeros) {
  std::vector<Row> rows{Row(std::vector<Value>{Value(-0.0)}),
                         Row(std::vector<Value>{Value(0.0)}),
                         Row(std::vector<Value>{Value(+0.0)})};
  EXPECT_EQ(CountSortDistinct(std::move(rows)), 1);
}

}  // namespace
}  // namespace tinylamb
