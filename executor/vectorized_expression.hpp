/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_VECTORIZED_EXPRESSION_HPP
#define TINYLAMB_EXECUTOR_VECTORIZED_EXPRESSION_HPP

#include <cstddef>
#include <memory>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/selection_vector.hpp"
#include "expression/expression.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// Evaluates expressions in columnar batches directly over DataChunk and
// ColumnVector, avoiding per-row virtual calls and allocations.
class VectorizedExpression {
 public:
  // Evaluates `expr` over `chunk` (optionally restricted to `sel` active rows),
  // returning the resulting ColumnVector.
  static ColumnVector Evaluate(const Expression& expr, const Schema& schema,
                               const DataChunk& chunk,
                               const SelectionVector* sel = nullptr);

  // Evaluates a boolean predicate expression over `chunk`, returning a
  // ValidityBitmap where true bits indicate matching rows.
  static ValidityBitmap EvaluateFilter(const Expression& expr,
                                       const Schema& schema,
                                       const DataChunk& chunk,
                                       const SelectionVector* sel = nullptr);

  // Evaluates a boolean filter over `chunk` and fills `output_sel` with
  // the matching row indices. If `input_sel` is provided, only inspects
  // active indices in `input_sel`.
  static void FilterDataChunk(const Expression& expr, const Schema& schema,
                              const DataChunk& chunk,
                              SelectionVector* output_sel,
                              const SelectionVector* input_sel = nullptr);

  // Vectorized boolean and bitwise aggregations
  static Value Aggregate(AggregationType type, const ColumnVector& col,
                         const SelectionVector* sel = nullptr);
  static Value AggregateLogicalAnd(const ColumnVector& col,
                                  const SelectionVector* sel = nullptr);
  static Value AggregateLogicalOr(const ColumnVector& col,
                                 const SelectionVector* sel = nullptr);
  static Value AggregateBitAnd(const ColumnVector& col,
                               const SelectionVector* sel = nullptr);
  static Value AggregateBitOr(const ColumnVector& col,
                              const SelectionVector* sel = nullptr);
  static Value AggregateBitXor(const ColumnVector& col,
                               const SelectionVector* sel = nullptr);
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_VECTORIZED_EXPRESSION_HPP
