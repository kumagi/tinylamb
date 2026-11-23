/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "executor/constant_executor.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/parallel_aggregation.hpp"
#include "executor/query_memory.hpp"
#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

// This binary covers ParallelAggregationExecutor directly: one independent
// per-worker state plus a deterministic merge.  The merge contract that matters is that
// DISTINCT sets are merged by VALUE, so duplicates spanning morsels stay
// correct for every team size -- exactly the path a per-shard "count of
// partial counts" shortcut would break.
class NumberSource final : public ExecutorBase {
 public:
  explicit NumberSource(size_t rows) : rows_(rows) {}

  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    destination->Reset();
    while (idx_ < rows_ && destination->Size() < max_rows) {
      destination->Append(
          Row(std::vector<Value>{Value(static_cast<int64_t>(idx_ % 7)),
                                 Value(static_cast<double>(idx_ % 64))}));
      ++idx_;
    }
    return destination->Size();
  }
  void Dump(std::ostream& o, int /*indent*/) const override { o << "numbers"; }

 private:
  size_t rows_;
  size_t idx_{0};
};

std::vector<NamedExpression> Aggregates() {
  const Expression k = ColumnValueExp("k");
  const Expression v = ColumnValueExp("v");
  return {
      {"total", AggregateExpressionExp(AggregationType::kCount, nullptr)},
      {"sum_k", AggregateExpressionExp(AggregationType::kSum, k)},
      {"ndv_k", AggregateExpressionExp(AggregationType::kCount, k,
                                       /*distinct=*/true)},
      {"sum_distinct_v", AggregateExpressionExp(AggregationType::kSum, v,
                                                /*distinct=*/true)},
  };
}

// 2048 rows over k in {0..6} and v in {0.0..63.0}: every DISTINCT set
// spans all 2048 rows, so any team size must still deduplicate globally.
TEST(ParallelExecCoverageTest, DistinctAndSumMergesAreTeamSizeInvariant) {
  Schema schema("nums", {Column("k", ValueType::kInt64),
                         Column("v", ValueType::kDouble)});
  constexpr size_t kRows = 2048;
  // sum over i%7 for 2048 rows: 2048 = 292*7 + 4 remaining rows
  int64_t expected_sum_k = 0;
  for (size_t i = 0; i < kRows; ++i) {
    expected_sum_k += static_cast<int64_t>(i % 7);
  }
  double expected_sum_distinct_v = 0.0;
  for (size_t d = 0; d < 64; ++d) {
    expected_sum_distinct_v += static_cast<double>(d);
  }

  QueryMemoryCharge unrelated_reservation(static_cast<size_t>(1024 * 1024));
  const size_t expected_charge = QueryMemoryBudget::Global().Used();
  for (const size_t workers : {size_t{1}, size_t{4}, size_t{16}}) {
    ParallelAggregationExecutor executor(std::make_shared<NumberSource>(kRows),
                                         schema, Aggregates(), workers);
    Row out;
    ASSERT_TRUE(executor.Next(&out, nullptr)) << "workers=" << workers;
    ASSERT_EQ(out.values_.size(), 4U);
    EXPECT_EQ(out.values_[0], Value(static_cast<int64_t>(kRows)))
        << "workers=" << workers;
    EXPECT_EQ(out.values_[1], Value(expected_sum_k)) << "workers=" << workers;
    EXPECT_EQ(out.values_[2], Value(int64_t{7})) << "workers=" << workers;
    EXPECT_DOUBLE_EQ(out.values_[3].value.double_value, expected_sum_distinct_v)
        << "workers=" << workers;
    EXPECT_FALSE(executor.Next(&out, nullptr));
    EXPECT_EQ(executor.GetStatus(), Status::kSuccess);
    EXPECT_EQ(QueryMemoryBudget::Global().Used(), expected_charge)
        << "workers=" << workers;
  }
}

TEST(ParallelExecCoverageTest, DoubleSumDetectsOverflowAndKeepsRealInfinity) {
  const Schema schema("numbers", {Column("v", ValueType::kDouble)});
  const auto aggregates = [] {
    return std::vector<NamedExpression>{
        {"sum",
         AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("v"))}};
  };
  for (size_t workers : {1, 4}) {
    const double maximum = std::numeric_limits<double>::max();
    ParallelAggregationExecutor overflowing(
        std::make_shared<ConstantExecutor>(
            std::vector<Row>{Row({Value(maximum)}), Row({Value(maximum)})}),
        schema, aggregates(), workers);
    Row out;
    EXPECT_FALSE(overflowing.Next(&out, nullptr));
    EXPECT_EQ(overflowing.GetStatus().GetCode(), StatusCode::kIsInfinity);

    const double infinity = std::numeric_limits<double>::infinity();
    ParallelAggregationExecutor already_infinite(
        std::make_shared<ConstantExecutor>(
            std::vector<Row>{Row({Value(infinity)}), Row({Value(1.0)})}),
        schema, aggregates(), workers);
    ASSERT_TRUE(already_infinite.Next(&out, nullptr));
    ASSERT_EQ(out.Count(), 1U);
    EXPECT_EQ(out[0], Value(infinity));
  }
}

TEST(ParallelExecCoverageTest, DoubleSumContinuesAfterIntegerBatch) {
  class MixedBatchSource final : public ExecutorBase {
   public:
    size_t NextBatch(DataChunk* output, size_t max_rows) override {
      output->Reset();
      if (max_rows == 0 || offset_ == values_.size()) {
        return 0;
      }
      const Value& value = values_[offset_++];
      output->Append(Row({value}));
      return 1;
    }
    void Dump(std::ostream& /*o*/, int /*indent*/) const override {}

   private:
    const std::vector<Value> values_{Value(5), Value(0.5), Value(2)};
    size_t offset_{0};
  };

  const Schema schema("numbers", {Column("v", ValueType::kDouble)});
  const Expression v = ColumnValueExp("v");
  const Expression doubled = BinaryExpressionExp(v, BinaryOperation::kAdd, v);
  ParallelAggregationExecutor executor(
      std::make_shared<MixedBatchSource>(), schema,
      {{"sum", AggregateExpressionExp(AggregationType::kSum, v)},
       {"sum_twice", AggregateExpressionExp(AggregationType::kSum, doubled)}},
      1);
  Row out;
  ASSERT_TRUE(executor.Next(&out, nullptr));
  EXPECT_EQ(out, Row({Value(7.5), Value(15.0)}));
  EXPECT_EQ(executor.GetStatus(), Status::kSuccess);
}

TEST(ParallelExecCoverageTest, EmptyInputStillEmitsOneRow) {
  Schema schema("nums", {Column("k", ValueType::kInt64),
                         Column("v", ValueType::kDouble)});
  ParallelAggregationExecutor executor(std::make_shared<NumberSource>(0),
                                       schema, Aggregates(), 4);
  Row out;
  ASSERT_TRUE(executor.Next(&out, nullptr));
  ASSERT_EQ(out.values_.size(), 4U);
  EXPECT_EQ(out.values_[0], Value(int64_t{0}));
  EXPECT_FALSE(executor.Next(&out, nullptr));
}

// A child that throws a value which is not a std::exception: without a
// catch-all inside the worker lambda this escapes the jthread and calls
// std::terminate, aborting the whole process instead of surfacing a Status.
class NonStdThrowingSource final : public ExecutorBase {
 public:
  size_t NextBatch(DataChunk* /*destination*/, size_t /*max_rows*/) override {
    throw 12345;  // deliberately not derived from std::exception
  }
  void Dump(std::ostream& o, int /*indent*/) const override { o << "throwing"; }
};

TEST(ParallelExecCoverageTest, NonStdChildExceptionBecomesStatus) {
  const Schema schema("nums", {Column("v", ValueType::kInt64)});
  const auto aggregates = [] {
    return std::vector<NamedExpression>{
        {"total", AggregateExpressionExp(AggregationType::kCount, nullptr)}};
  };
  ParallelAggregationExecutor executor(std::make_shared<NonStdThrowingSource>(),
                                       schema, aggregates(), 4);
  Row out;
  EXPECT_FALSE(executor.Next(&out, nullptr));
  EXPECT_EQ(executor.GetStatus().GetCode(), StatusCode::kRuntimeError);
}

}  // namespace
}  // namespace tinylamb
