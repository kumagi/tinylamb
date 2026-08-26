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
  // Row-level pre-filter: `AGG(x WHERE cond)` skips rows where cond is not
  // true; streaming-safe, unlike the HAVING MAX/MIN modifier.
  [[nodiscard]] const Expression& WhereFilter() const { return where_filter_; }
  void SetWhereFilter(Expression filter) { where_filter_ = std::move(filter); }
  // Aggregates that cannot stream row-at-a-time: they need whole-group
  // context for their HAVING modifier, inner ORDER BY, or LIMIT.
  [[nodiscard]] bool NeedsGroupContext() const {
    return having_ != AggregateHavingModifier::kNone ||
           !inner_order_by_.empty() || inner_limit_.has_value();
  }
  // Multi-level aggregation: SUM(AVG(x) GROUP BY y).  The inner GROUP BY
  // expressions partition input rows before the child aggregate runs; the
  // outer aggregate then combines the per-group results.
  [[nodiscard]] const std::vector<Expression>& InnerGroupBy() const {
    return inner_group_by_;
  }
  [[nodiscard]] bool HasInnerGroupBy() const {
    return !inner_group_by_.empty();
  }
  void SetInnerGroupBy(std::vector<Expression> group_by) {
    inner_group_by_ = std::move(group_by);
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
  Expression where_filter_;
  std::vector<Expression> inner_group_by_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_AGGREGATE_EXPRESSION_HPP
