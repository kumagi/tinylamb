/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PARTIAL_AGGREGATE_HPP
#define TINYLAMB_PARTIAL_AGGREGATE_HPP

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "expression/named_expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

// PartialAggregate: computes partial intermediate aggregation states
// (e.g. count, sum for AVG; count for COUNT; sum for SUM; min/max).
class PartialAggregate : public ExecutorBase {
 public:
  PartialAggregate(Executor child, Schema input_schema,
                   std::vector<NamedExpression> aggregates);

  PartialAggregate(Executor child, Schema input_schema,
                   std::vector<NamedExpression> group_by_keys,
                   std::vector<NamedExpression> aggregates);

  PartialAggregate(Executor child, Schema input_schema,
                   std::vector<Expression> group_by_keys,
                   std::vector<NamedExpression> aggregates);

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;

  [[nodiscard]] const Schema& OutputSchema() const { return output_schema_; }

 private:
  void Materialize();

  Executor child_;
  Schema input_schema_;
  std::vector<NamedExpression> group_by_keys_;
  std::vector<NamedExpression> aggregates_;
  Schema output_schema_;

  bool materialized_{false};
  std::vector<Row> output_rows_;
  size_t cursor_{0};
};

// FinalizeAggregate: combines partial intermediate aggregation states into final
// results (e.g. avg = sum / count).
class FinalizeAggregate : public ExecutorBase {
 public:
  FinalizeAggregate(Executor child, Schema input_schema,
                    std::vector<NamedExpression> aggregates);

  FinalizeAggregate(Executor child, Schema input_schema,
                    std::vector<NamedExpression> group_by_keys,
                    std::vector<NamedExpression> aggregates);

  FinalizeAggregate(Executor child, Schema input_schema,
                    std::vector<Expression> group_by_keys,
                    std::vector<NamedExpression> aggregates);

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;

  [[nodiscard]] const Schema& OutputSchema() const { return output_schema_; }

 private:
  void Materialize();

  Executor child_;
  Schema input_schema_;
  std::vector<NamedExpression> group_by_keys_;
  std::vector<NamedExpression> aggregates_;
  Schema output_schema_;

  bool materialized_{false};
  std::vector<Row> output_rows_;
  size_t cursor_{0};
};

using PartialAggregateExecutor = PartialAggregate;
using FinalizeAggregateExecutor = FinalizeAggregate;

}  // namespace tinylamb

#endif  // TINYLAMB_PARTIAL_AGGREGATE_HPP
