/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EVALUATION_CONTEXT_HPP
#define TINYLAMB_EVALUATION_CONTEXT_HPP

#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status_or.hpp"
#include "type/value.hpp"

namespace tinylamb {

class AggregateExpression;
class Row;
class SelectStatement;

// Prepared aggregation results keyed by the aggregate expression node that
// produced them.  relational_detail aliases the same specialization, so both
// spellings are interchangeable.
using AggregateResultMap =
    std::unordered_map<const AggregateExpression*, Value>;

// Abstract boundary between expression evaluation and query execution
// (improvement3.md A1 / S3).  Expression nodes reach everything that needs
// executor or database machinery through this interface only:
//
//   - SelectStatement and Row are forward declared; the expression layer does
//     not include the IR nor any database header any more.
//   - The production implementation lives above this layer in
//     query/evaluation_context_impl.hpp (wraps TransactionContext, Scope,
//     CteMap), and unit tests provide fake contexts so expressions can be
//     exercised without a Database.
class EvaluationContext {
 public:
  virtual ~EvaluationContext() = default;

  // Executes `statement` scoped to `outer_row` (nullptr when the subquery is
  // uncorrelated) and projects the first output column in row order.  An
  // empty vector means the subquery selected no row; EXISTS / IN / scalar
  // consumers derive their three-valued result from that distinction.
  [[nodiscard]] virtual StatusOr<std::vector<Value>> RunSubquery(
      const SelectStatement& statement, const Row* outer_row) = 0;

  // Aggregation results prepared for the group under evaluation, or nullptr
  // outside grouping contexts.
  [[nodiscard]] virtual const AggregateResultMap* CurrentAggregates() const = 0;

  // Registers (or finds) a function signature during validation; mirrors
  // Database::GetOrAddFunction for FunctionCallExpression::Validate.
  [[nodiscard]] virtual Status GetOrAddFunction(std::string_view function_name,
                                                int argument_count) = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EVALUATION_CONTEXT_HPP
