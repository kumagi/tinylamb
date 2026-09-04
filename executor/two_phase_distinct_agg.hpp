/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TWO_PHASE_DISTINCT_AGG_HPP
#define TINYLAMB_TWO_PHASE_DISTINCT_AGG_HPP

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

// TwoPhaseDistinctAgg: Two-phase distinct aggregation executor.
// Phase 1 (partial distinct): groups by group key + distinct column to
// eliminate duplicates. Phase 2 (finalize agg): combines deduplicated distinct
// values and non-distinct partials per group.
class TwoPhaseDistinctAgg : public ExecutorBase {
 public:
  TwoPhaseDistinctAgg(Executor child, Schema input_schema,
                      std::vector<NamedExpression> aggregates);

  TwoPhaseDistinctAgg(Executor child, Schema input_schema,
                      std::vector<NamedExpression> group_by_keys,
                      std::vector<NamedExpression> aggregates);

  TwoPhaseDistinctAgg(Executor child, Schema input_schema,
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

using TwoPhaseDistinctAggExecutor = TwoPhaseDistinctAgg;

}  // namespace tinylamb

#endif  // TINYLAMB_TWO_PHASE_DISTINCT_AGG_HPP
