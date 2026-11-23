/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache License 2.0. */
#include "executor/distinct.hpp"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <sstream>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/aggregation.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/grouped_aggregate.hpp"
#include "executor/merge_append.hpp"
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

class FakeSource : public ExecutorBase {
 public:
  explicit FakeSource(std::vector<Row> rows) : rows_(std::move(rows)) {}
  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    destination->Reset();
    while (idx_ < rows_.size() && destination->Size() < max_rows) {
      destination->Append(rows_[idx_++]);
    }
    return destination->Size();
  }
  void Dump(std::ostream& o, int /*indent*/) const override { o << "fake"; }

 private:
  std::vector<Row> rows_{};
  size_t idx_{0};
};

std::vector<Row> Rows(std::vector<Value> values) {
  std::vector<Row> out;
  out.reserve(values.size());
  for (auto& v : values) {
    out.emplace_back(std::vector<Value>{std::move(v)});
  }
  return out;
}

int CountDistinct(std::vector<Row> rows) {
  DistinctExecutor ex(std::make_shared<FakeSource>(std::move(rows)));
  Row r;
  int n = 0;
  while (ex.Next(&r, nullptr)) {
    ++n;
  }
  return n;
}

int CountSortDistinct(std::vector<Row> rows) {
  // SortDistinctExecutor only collapses adjacent equal rows, so feed the
  // input pre-sorted (the sort plan that precedes it guarantees this).
  SortDistinctExecutor ex(std::make_shared<FakeSource>(std::move(rows)));
  Row r;
  int n = 0;
  while (ex.Next(&r, nullptr)) {
    ++n;
  }
  return n;
}

TEST(DistinctExecutorTest, CollapsesAllNan) {
  const double nan = std::nan("");
  std::vector<Row> rows;
  rows.insert(rows.end(), 9, Row(std::vector<Value>{Value(nan)}));
  EXPECT_EQ(CountDistinct(std::move(rows)), 1);
}

TEST(DistinctExecutorTest, DistinctOnFailureClearsPartialBatchAndStaysFailed) {
  const Schema schema("input", {Column("v", ValueType::kInt64)});
  auto key = BinaryExpressionExp(ConstantValueExp(Value(1)),
                                 BinaryOperation::kDivide,
                                 ColumnValueExp("v"));
  auto source = std::make_shared<FakeSource>(Rows({Value(1), Value(0), Value(2)}));
  DistinctExecutor distinct(source, schema, {key});
  DataChunk result;
  EXPECT_EQ(distinct.NextBatch(&result), 0U);
  EXPECT_EQ(result.Size(), 0U);
  const Status failure = distinct.GetStatus();
  ASSERT_FALSE(failure.ok());
  EXPECT_NE(failure.GetMessage().find("division by zero"), std::string::npos);
  EXPECT_EQ(distinct.NextBatch(&result), 0U);
  EXPECT_EQ(result.Size(), 0U);
  EXPECT_EQ(distinct.GetStatus(), failure);
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

TEST(DistinctExecutorTest, DistinctDoublesScenarioMatchesGoogleSql) {
  const double nan = std::nan("");
  std::vector<Value> vals;
  auto d = [&](double x) { vals.emplace_back(x); };
  for (double x : {-0.0, +0.0, 0.0, -0.0, 0.0, +0.0, -0.0, 0.0}) {
    d(x);
  }
  for (int k = 0; k < 9; ++k) {
    d(nan);
  }
  d(INFINITY);
  d(-INFINITY);
  d(1.0);
  d(-1.0);
  d(1.0);
  d(-1.0);
  vals.emplace_back();
  // GoogleSQL DISTINCT collapses to {NULL, nan, -inf, -1, 0, 1, inf} = 7.
  EXPECT_EQ(CountDistinct(Rows(std::move(vals))), 7);
}

TEST(DistinctExecutorTest, TypeSensitiveIntVsDouble) {
  std::vector<Value> vals{Value(int64_t{1}), Value(1.0)};
  EXPECT_EQ(CountDistinct(Rows(std::move(vals))), 2);
}

// A positional key past the input width is a plan error. It used to become a
// NULL key and silently collapse unrelated rows into one DISTINCT result.
TEST(DistinctExecutorTest, OutOfRangePositionalKeyFails) {
  DistinctExecutor ex(std::make_shared<FakeSource>(Rows({Value(1), Value(2)})),
                      {}, {}, /*key_cols=*/{5});
  DataChunk chunk;
  EXPECT_EQ(ex.NextBatch(&chunk), 0U);
  EXPECT_EQ(ex.GetStatus().GetCode(), StatusCode::kInvalidArgument);
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

// T1791/T1793: Dump rendered measured counters (groups/input_rows/rows)
// unconditionally, so a plain EXPLAIN -- which calls Dump before anything
// is consumed -- printed dressed-up zeros as a profile.  Counters now join
// only after a drain, per the Selection::DumpHeader rows_seen_ contract.
TEST(ExecutorDumpCountersTest, DistinctCountersRenderOnlyAfterDrain) {
  DistinctExecutor hash_distinct(
      std::make_shared<FakeSource>(Rows({Value(1), Value(1), Value(2)})));
  std::ostringstream plain;
  hash_distinct.Dump(plain, 0);
  EXPECT_EQ(plain.str().find("HashDistinct\n"), 0U) << plain.str();

  Row r;
  while (hash_distinct.Next(&r, nullptr)) {
  }
  std::ostringstream analyzed;
  hash_distinct.Dump(analyzed, 0);
  EXPECT_NE(analyzed.str().find("HashDistinct (input_rows=3, output_rows=2)"),
            std::string::npos)
      << analyzed.str();

  SortDistinctExecutor sort_distinct(
      std::make_shared<FakeSource>(Rows({Value(1), Value(1), Value(2)})));
  std::ostringstream plain_sd;
  sort_distinct.Dump(plain_sd, 0);
  EXPECT_EQ(plain_sd.str().find("SortDistinct\n"), 0U) << plain_sd.str();
  while (sort_distinct.Next(&r, nullptr)) {
  }
  std::ostringstream analyzed_sd;
  sort_distinct.Dump(analyzed_sd, 0);
  EXPECT_NE(
      analyzed_sd.str().find("SortDistinct (input_rows=3, output_rows=2)"),
      std::string::npos)
      << analyzed_sd.str();
}

TEST(ExecutorDumpCountersTest, AggregateDumpsHideUnexecutedCounters) {
  const Schema schema("t", {Column("k", ValueType::kInt64)});
  const Expression k = ColumnValueExp("k");
  const Expression count =
      AggregateExpressionExp(AggregationType::kCount, nullptr);

  HashAggregateExecutor hash(
      std::make_shared<FakeSource>(Rows({Value(1), Value(1), Value(2)})),
      schema, {NamedExpression{"k", k}}, {NamedExpression{"n", count}});
  std::ostringstream plain;
  hash.Dump(plain, 0);
  EXPECT_EQ(plain.str().find("groups="), std::string::npos) << plain.str();
  EXPECT_EQ(plain.str().find("spill="), std::string::npos) << plain.str();
  EXPECT_NE(plain.str().find("HashAggregate (budget="), std::string::npos)
      << plain.str();
  Row r;
  while (hash.Next(&r, nullptr)) {
  }
  std::ostringstream analyzed;
  hash.Dump(analyzed, 0);
  EXPECT_NE(analyzed.str().find("HashAggregate (groups=2, input_rows=3"),
            std::string::npos)
      << analyzed.str();

  AggregationExecutor scalar(
      std::make_shared<FakeSource>(Rows({Value(1), Value(1), Value(2)})),
      schema, {NamedExpression{"n", count}});
  std::ostringstream s_plain;
  scalar.Dump(s_plain, 0);
  EXPECT_EQ(s_plain.str().find("input_rows"), std::string::npos)
      << s_plain.str();
  while (scalar.Next(&r, nullptr)) {
  }
  std::ostringstream s_analyzed;
  scalar.Dump(s_analyzed, 0);
  EXPECT_NE(s_analyzed.str().find("(input_rows=3)"), std::string::npos)
      << s_analyzed.str();

  MergeAppendExecutor append(
      {std::make_shared<FakeSource>(Rows({Value(1), Value(2)}))}, {schema},
      schema, {SortExecutor::Key{.expression = k}});
  std::ostringstream m_plain;
  append.Dump(m_plain, 0);
  EXPECT_EQ(m_plain.str().find(" rows="), std::string::npos) << m_plain.str();
  while (append.Next(&r, nullptr)) {
  }
  std::ostringstream m_analyzed;
  append.Dump(m_analyzed, 0);
  EXPECT_NE(m_analyzed.str().find(" rows=2"), std::string::npos)
      << m_analyzed.str();
}

}  // namespace
}  // namespace tinylamb
