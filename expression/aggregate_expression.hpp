/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TINYLAMB_AGGREGATE_EXPRESSION_HPP
#define TINYLAMB_AGGREGATE_EXPRESSION_HPP

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "expression/expression.hpp"
#include "expression/window_function_expression.hpp"

namespace tinylamb {

// GoogleSQL aggregate filtering: `AGG(x HAVING MAX cond)` only aggregates the
// rows whose `cond` value reaches the group-wide MAX (or MIN) of `cond`.
enum class AggregateHavingModifier { kNone, kMax, kMin };

class AggregateExpression : public ExpressionBase {
 public:
  AggregateExpression(AggregationType type, Expression child,
                      bool distinct = false)
      : type_(type), child_(std::move(child)), distinct_(distinct) {}
  [[nodiscard]] TypeTag Type() const override { return TypeTag::kAggregateExp; }
  [[nodiscard]] Value Evaluate(const Row& row,
                               const Schema& schema) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema& schema) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema& left,
                                          const Schema& right) const override;
  [[nodiscard]] AggregationType GetType() const { return type_; }
  [[nodiscard]] const Expression& Child() const { return child_; }
  [[nodiscard]] bool Distinct() const { return distinct_; }
  [[nodiscard]] AggregateHavingModifier Having() const { return having_; }
  [[nodiscard]] const Expression& HavingCondition() const {
    return having_cond_;
  }
  void SetHaving(AggregateHavingModifier modifier, Expression condition) {
    having_ = modifier;
    having_cond_ = std::move(condition);
  }
  [[nodiscard]] const std::vector<WindowOrderTerm>& InnerOrderBy() const {
    return inner_order_by_;
  }
  void SetInnerOrderBy(std::vector<WindowOrderTerm> order_by) {
    inner_order_by_ = std::move(order_by);
  }
  [[nodiscard]] const std::optional<size_t>& InnerLimit() const {
    return inner_limit_;
  }
  void SetInnerLimit(std::optional<size_t> limit) {
    inner_limit_ = std::move(limit);
  }
  // STRING_AGG delimiter (second argument), evaluated per row by call sites.
  [[nodiscard]] const Expression& SecondaryArg() const {
    return secondary_arg_;
  }
  void SetSecondaryArg(Expression arg) { secondary_arg_ = std::move(arg); }
  // Trailing arguments of multi-argument aggregates (COVAR(y, x),
  // APPROX_QUANTILES(x, n), APPROX_TOP_SUM(x, w, k), HLL_COUNT.INIT(x, p));
  // evaluated per row and handed to the accumulator.
  [[nodiscard]] const std::vector<Expression>& TrailingArgs() const {
    return trailing_args_;
  }
  void SetTrailingArgs(std::vector<Expression> args) {
    trailing_args_ = std::move(args);
  }
  // Alias kept for call sites that model per-row extras as named arguments
  // (APPROX_TOP_SUM weight / top-N count); same storage as TrailingArgs.
  [[nodiscard]] const std::vector<Expression>& ExtraArgs() const {
    return trailing_args_;
  }
  void SetExtraArgs(std::vector<Expression> args) {
    trailing_args_ = std::move(args);
  }
  // Row-level pre-filter: `AGG(x WHERE cond)` skips rows where cond is not
  // true; streaming-safe, unlike the HAVING MAX/MIN modifier.
  [[nodiscard]] const Expression& WhereFilter() const { return where_filter_; }
  void SetWhereFilter(Expression filter) { where_filter_ = std::move(filter); }
  // Static SQL type of ARRAY_AGG's element (BOOL, INT32, ...), inferred from
  // the argument AST.  Empty means infer from the aggregated values.
  [[nodiscard]] const std::string& ArrayElementSqlType() const {
    return array_element_sql_type_;
  }
  void SetArrayElementSqlType(std::string type) {
    array_element_sql_type_ = std::move(type);
  }
  // Aggregates that cannot stream row-at-a-time: they need whole-group
  // context for their HAVING modifier, inner ORDER BY, LIMIT, or per-row
  // trailing arguments (two-input statistics, sketch parameters).
  [[nodiscard]] bool NeedsGroupContext() const {
    return having_ != AggregateHavingModifier::kNone ||
           !inner_order_by_.empty() || inner_limit_.has_value() ||
           !trailing_args_.empty() ||
           type_ == AggregationType::kApproxTopCount ||
           type_ == AggregationType::kApproxTopSum;
  }
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& o) const override;
  [[nodiscard]] std::unordered_set<ColumnName> TouchedColumns() const override;

 private:
  AggregationType type_;
  Expression child_;
  bool distinct_{false};
  AggregateHavingModifier having_{AggregateHavingModifier::kNone};
  Expression having_cond_;
  std::vector<WindowOrderTerm> inner_order_by_;
  std::optional<size_t> inner_limit_;
  Expression secondary_arg_;
  std::vector<Expression> trailing_args_;
  Expression where_filter_;
  std::string array_element_sql_type_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_AGGREGATE_EXPRESSION_HPP
