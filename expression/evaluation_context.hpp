/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EVALUATION_CONTEXT_HPP
#define TINYLAMB_EVALUATION_CONTEXT_HPP

#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status_or.hpp"
#include "type/value.hpp"

namespace tinylamb {

class AggregateExpression;
struct Row;
class SelectStatement;

// Prepared aggregation results keyed by the aggregate expression node that
// produced them. `tinylamb::relational_detail` declares the corresponding alias
// in executor/detail/expression_eval.hpp; both names resolve to the same map
// type, so keep their declarations in sync.
using AggregateResultMap =
    std::unordered_map<const AggregateExpression*, Value>;

// Abstract boundary between expression evaluation and query execution
// (the design contract).  Expression nodes reach everything that needs
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
  EvaluationContext() = default;
  // Non-copyable by design: instances are owned by smart pointers and
  // referenced by raw pointers throughout the executor/graph object web.
  EvaluationContext(const EvaluationContext&) = delete;
  EvaluationContext& operator=(const EvaluationContext&) = delete;
  EvaluationContext(EvaluationContext&&) = delete;
  EvaluationContext& operator=(EvaluationContext&&) = delete;
  virtual ~EvaluationContext() = default;

  // Executes `statement` scoped to `outer_row` (nullptr when the subquery is
  // uncorrelated) and projects the first output column in row order.  An
  // empty vector means the subquery selected no row; EXISTS / IN / scalar
  // consumers derive their three-valued result from that distinction.
  [[nodiscard]] virtual StatusOr<std::vector<Value>> RunSubquery(
      const SelectStatement& statement, const Row* outer_row) = 0;

  // RunSubquery plus the subquery's declared element type, when the
  // execution context can supply one.  ARRAY(SELECT ...) uses the hint only
  // after every produced element is NULL and no static type was declared —
  // the schema column keeps e.g. ARRAY<STRING> from degrading to the
  // ARRAY<INT64> last resort that the values alone would force.
  struct SubqueryResult {
    std::vector<Value> values;
    std::string element_type_hint;
  };
  // Declared element type of `statement`'s first output column, when the
  // context can supply one.  RunSubquery alone returns only values, so the
  // generic context has no schema to consult; contexts with schema access
  // (fake in tests, production in query/evaluation_context_impl.hpp) override
  // this hook to exercise ARRAY(SELECT ...) typing, notably the all-NULL case
  // where the declared type cannot be recovered from the values.  Every
  // context goes through the default RunSubqueryDetailed below, so the hook
  // is the single source of the hint.
  [[nodiscard]] virtual std::string SubqueryElementTypeHint(
      const SelectStatement& /*statement*/) {
    return {};
  }

  [[nodiscard]] virtual StatusOr<SubqueryResult> RunSubqueryDetailed(
      const SelectStatement& statement, const Row* outer_row) {
    StatusOr<std::vector<Value>> values = RunSubquery(statement, outer_row);
    if (!values.HasValue()) {
      return values.GetStatus();
    }
    return SubqueryResult{
        .values = values.MoveValue(),
        .element_type_hint = SubqueryElementTypeHint(statement)};
  }

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
