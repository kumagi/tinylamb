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
#include "parser/ast.hpp"
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

struct AggregateAccumulator {
  explicit AggregateAccumulator(const AggregateExpression* aggregate);

  void Add(const Value& value);
  Value Finish() const;

  const AggregateExpression* expression;
  int64_t count = 0;
  double total = 0.0;
  // Integer sums accumulate exactly here (uint64 to keep overflow defined);
  // `total` only carries double inputs.
  uint64_t int_total = 0;
  bool total_is_double = false;
  Value extreme;
  std::unique_ptr<std::unordered_set<Value>> distinct;
  std::unique_ptr<std::unordered_set<int64_t>> distinct_ints;
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

bool ReusesBaseRelation(const SelectSource& source);

std::string ProjectionName(const NamedExpression& projection, size_t index);

ValueType ValueTypeOf(const Value& value);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_EXPRESSION_EVAL_HPP
