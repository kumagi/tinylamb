/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_EXPRESSION_EVAL_HPP
#define TINYLAMB_EXECUTOR_DETAIL_EXPRESSION_EVAL_HPP

#include <cmath>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "executor/detail/subquery_runtime.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression.hpp"
#include "query/statement.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
class TransactionContext;
}

namespace tinylamb::relational_detail {

bool Truthy(const Value& value);
Value Lookup(const ColumnName& name, const Scope& scope);
bool Like(std::string_view value, std::string_view pattern);
Value Binary(BinaryOperation operation, const Value& left, const Value& right);

bool ContainsAggregate(const Expression& expression);

void CollectAggregates(const Expression& expression,
                       std::vector<const AggregateExpression*>* aggregates,
                       std::unordered_set<const AggregateExpression*>* seen);

// One row fed into an aggregate.  `order_keys` carries the evaluated inner
// ORDER BY terms and `condition` the HAVING MAX/MIN condition value; both are
// only populated when the aggregate declares them.
struct AggregateInput {
  Value value;
  std::vector<Value> order_keys;
  Value condition;
  Value auxiliary;  // STRING_AGG delimiter
};

std::string ElementSqlTypeName(ValueType type);

struct AggregateAccumulator {
  explicit AggregateAccumulator(const AggregateExpression* aggregate);

  // Row-at-a-time accumulation for streaming aggregates.
  void Add(const Value& value);
  // Full-input accumulation: buffers rows for aggregates that need whole-group
  // context (HAVING modifier, inner ORDER BY/LIMIT, ARRAY_AGG, STRING_AGG).
  void Add(const AggregateInput& input);
  Value Finish() const;

  const AggregateExpression* expression;
  int64_t count = 0;
  double total = 0.0;
  // Integer sums accumulate exactly here (int64 with overflow checks);
  // `total` only carries double inputs.
  int64_t int_total = 0;
  bool total_is_double = false;
  Value extreme;
  // GoogleSQL MIN/MAX: a NaN input poisons the result.
  mutable bool saw_nan_{false};
  std::unique_ptr<std::unordered_set<Value>> distinct;
  std::unique_ptr<std::unordered_set<int64_t>> distinct_ints;

 private:
  struct BufferedRow {
    Value value;
    std::vector<Value> order_keys;
    Value condition;
    Value auxiliary;
  };
  mutable std::unique_ptr<std::vector<BufferedRow>> buffer_;
  mutable std::vector<Value> array_values_;
  mutable std::optional<std::string> delimiter_;
  // BIT_AND/OR/XOR fold state.
  mutable int64_t bit_acc_{0};
  mutable bool bit_saw_value_{false};
  // ARRAY_CONCAT_AGG declared element type (captured even for empty arrays).
  mutable std::string concat_elem_type_;
  // ELEMENTWISE_SUM/AVG positional state.
  mutable std::vector<int64_t> ew_int_sum_;
  mutable std::vector<double> ew_double_sum_;
  mutable std::vector<int64_t> ew_count_;
  mutable std::vector<bool> ew_saw_double_;
  mutable size_t ew_len_{0};
  mutable bool ew_any_input_{false};
  mutable std::string ew_input_elem_type_;

  void ElementwiseApply(const Value& array);
  Value FinishApproxTop(const std::vector<BufferedRow>& rows) const;
  void ApplyCore(const Value& value);
};

using AggregateResultMap =
    std::unordered_map<const AggregateExpression*, Value>;

Value Evaluate(const Expression& expression, const Scope& scope,
               const AggregateResultMap* aggregates,
               TransactionContext& context,
               const CteMap& ctes);

Schema QualifySchema(const Schema& schema, std::string_view qualifier);

void CollectStatementColumns(const SelectStatement& statement,
                             std::unordered_set<ColumnName>* columns);

std::vector<slot_t> RequiredColumns(const SelectStatement& statement,
                                    const Schema& schema,
                                    bool ignore_star = false);

Schema ProjectSchema(const Schema& schema,
                     const std::vector<slot_t>& projection);

std::string BaseRelationCacheKey(
    std::string_view table, const std::vector<slot_t>* projection);

bool ReusesBaseRelation(TransactionContext& context,
                        const SelectSource& source);

std::string ProjectionName(const NamedExpression& projection, size_t index);

ValueType ValueTypeOf(const Value& value);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_EXPRESSION_EVAL_HPP
