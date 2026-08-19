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

#include "plan/optimizer.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "aggregation_plan.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/named_expression.hpp"
#include "expression/rewrite.hpp"
#include "full_scan_plan.hpp"
#include "index_only_scan_plan.hpp"
#include "index_scan_plan.hpp"
#include "plan/cascades.hpp"
#include "plan/plan.hpp"
#include "product_plan.hpp"
#include "projection_plan.hpp"
#include "query/query_data.hpp"
#include "selection_plan.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

struct Range {
  std::optional<Value> min;
  std::optional<Value> max;
  bool min_inclusive{false};
  bool max_inclusive{false};

  enum class Direction : bool { kRight, kLeft };

  void Update(BinaryOperation operation, const Value& value,
              Direction direction) {
    switch (operation) {
      case BinaryOperation::kEquals:
        min = max = value;
        min_inclusive = max_inclusive = true;
        return;
      case BinaryOperation::kNotEquals:
        return;
      case BinaryOperation::kLessThan:
      case BinaryOperation::kGreaterThan: {
        const bool upper = (direction == Direction::kRight &&
                            operation == BinaryOperation::kLessThan) ||
                           (direction == Direction::kLeft &&
                            operation == BinaryOperation::kGreaterThan);
        if (upper) {
          if (!max || value < *max) max = value;
          max_inclusive = false;
        } else {
          if (!min || *min < value) min = value;
          min_inclusive = false;
        }
        return;
      }
      case BinaryOperation::kLessThanEquals:
      case BinaryOperation::kGreaterThanEquals: {
        const bool upper = (direction == Direction::kRight &&
                            operation == BinaryOperation::kLessThanEquals) ||
                           (direction == Direction::kLeft &&
                            operation == BinaryOperation::kGreaterThanEquals);
        if (upper) {
          if (!max || value <= *max) max = value;
          max_inclusive = true;
        } else {
          if (!min || *min <= value) min = value;
          min_inclusive = true;
        }
        return;
      }
      default:
        return;
    }
  }
};

template <typename T>
bool Covered(const std::unordered_set<T>& available,
             const std::unordered_set<T>& required) {
  return std::ranges::all_of(
      required, [&](const T& value) { return available.contains(value); });
}

Plan BuildIndexScan(const Table& table, const Index& index,
                    const TableStatistics& statistics,
                    std::vector<Value> begin_key, std::vector<Value> end_key,
                    bool ascending, const Expression& predicate,
                    const std::vector<NamedExpression>& select,
                    bool require_row_position,
                    std::vector<ColumnName> provided_order) {
  std::unordered_set<ColumnName> touched = predicate->TouchedColumns();
  for (const NamedExpression& item : select) {
    touched.merge(item.expression->TouchedColumns());
  }
  std::unordered_set<slot_t> touched_offsets;
  for (const ColumnName& column : touched) {
    const int offset = table.GetSchema().Offset(column);
    if (offset >= 0) touched_offsets.insert(static_cast<slot_t>(offset));
  }
  if (!require_row_position &&
      Covered(index.CoveredColumns(), touched_offsets)) {
    return std::make_shared<IndexOnlyScanPlan>(
        table, index, statistics, std::move(begin_key), std::move(end_key),
        ascending, predicate, std::move(provided_order));
  }
  return std::make_shared<IndexScanPlan>(
      table, index, statistics, std::move(begin_key), std::move(end_key),
      ascending, predicate, std::move(provided_order));
}

std::vector<Plan> ScanCandidates(const std::vector<NamedExpression>& select,
                                 const Table& table,
                                 const Expression& predicate,
                                 const TableStatistics& statistics,
                                 bool require_row_position,
                                 bool include_indexes, bool include_full_scan,
                                 [[maybe_unused]] const std::vector<Expression>&
                                     order_expressions,
                                 [[maybe_unused]] const std::vector<bool>&
                                     order_ascending) {
  const Schema& schema = table.GetSchema();
  std::unordered_map<slot_t, Range> ranges;
  std::vector<Expression> range_predicates;
  for (const Expression& conjunct : SplitConjuncts(predicate)) {
    if (conjunct->Type() != TypeTag::kBinaryExp) continue;
    const auto& binary = conjunct->AsBinaryExpression();
    if (!IsComparison(binary.Op())) continue;

    const ColumnValue* column = nullptr;
    const ConstantValue* constant = nullptr;
    Range::Direction direction = Range::Direction::kRight;
    if (binary.Left()->Type() == TypeTag::kColumnValue &&
        binary.Right()->Type() == TypeTag::kConstantValue) {
      column = &binary.Left()->AsColumnValue();
      constant = &binary.Right()->AsConstantValue();
    } else if (binary.Left()->Type() == TypeTag::kConstantValue &&
               binary.Right()->Type() == TypeTag::kColumnValue) {
      column = &binary.Right()->AsColumnValue();
      constant = &binary.Left()->AsConstantValue();
      direction = Range::Direction::kLeft;
    }
    if (!column || !constant) continue;
    const int offset = schema.Offset(column->GetColumnName());
    if (offset < 0) continue;
    ranges[static_cast<slot_t>(offset)].Update(binary.Op(),
                                               constant->GetValue(), direction);
    range_predicates.push_back(conjunct);
  }

  const Expression pushed_predicate = CombineConjuncts(range_predicates);
  std::vector<Plan> candidates;
  if (include_indexes) {
    for (size_t index_offset = 0; index_offset < table.IndexCount();
         ++index_offset) {
      const Index& index = table.GetIndex(index_offset);
      std::vector<Value> begin_key;
      std::vector<Value> end_key;
      std::unordered_set<slot_t> consumed;
      size_t equality_prefix = 0;
      for (slot_t slot : index.sc_.key_) {
        const auto range = ranges.find(slot);
        if (range == ranges.end()) break;
        const bool equality = range->second.min && range->second.max &&
                              *range->second.min == *range->second.max;
        if (equality) {
          begin_key.push_back(*range->second.min);
          end_key.push_back(*range->second.max);
          consumed.insert(slot);
          ++equality_prefix;
          continue;
        }
        if (range->second.min) {
          begin_key.push_back(*range->second.min);
        } else if (begin_key.empty()) {
          break;
        }
        if (range->second.max) {
          end_key.push_back(*range->second.max);
        } else if (!begin_key.empty()) {
          end_key = begin_key;
          if (!range->second.min) end_key.clear();
          else end_key.pop_back();
        }
        consumed.insert(slot);
        break;
      }
      if (begin_key.empty()) continue;

      std::vector<ColumnName> provided_order;
      for (size_t key = equality_prefix; key < index.sc_.key_.size(); ++key) {
        provided_order.push_back(
            schema.GetColumn(index.sc_.key_[key]).Name());
      }
      // Prefix encoding is correct for forward scans. DESC still needs a
      // sort until the B+tree iterator can land on the last key of a prefix.

      Plan candidate = BuildIndexScan(
          table, index, statistics, begin_key, end_key, true,
          pushed_predicate, select, require_row_position,
          std::move(provided_order));
      const bool covered = std::ranges::all_of(
          pushed_predicate->TouchedColumns(), [&](const ColumnName& column) {
            const int offset = schema.Offset(column);
            return offset >= 0 && consumed.contains(static_cast<slot_t>(offset));
          });
      if (!covered) {
        candidate = std::make_shared<SelectionPlan>(candidate, pushed_predicate,
                                                    statistics);
      }
      if (select.size() != candidate->GetSchema().ColumnCount()) {
        candidate = std::make_shared<ProjectionPlan>(candidate, select);
      }
      candidates.push_back(std::move(candidate));
    }
  }

  if (include_full_scan) {
    Plan full_scan = std::make_shared<FullScanPlan>(table, statistics);
    if (!range_predicates.empty()) {
      full_scan = std::make_shared<SelectionPlan>(full_scan, pushed_predicate,
                                                  statistics);
    }
    if (select.size() != full_scan->GetSchema().ColumnCount()) {
      full_scan = std::make_shared<ProjectionPlan>(full_scan, select);
    }
    candidates.push_back(std::move(full_scan));
  }
  return candidates;
}

std::vector<Plan> JoinCandidates(TransactionContext& context,
                                 const Expression& predicate, const Plan& left,
                                 const Plan& right, bool include_hash,
                                 bool include_index, bool include_cross) {
  std::vector<std::pair<ColumnName, ColumnName>> equalities;
  for (const Expression& conjunct : SplitConjuncts(predicate)) {
    if (conjunct->Type() != TypeTag::kBinaryExp) continue;
    const auto& binary = conjunct->AsBinaryExpression();
    if (binary.Op() != BinaryOperation::kEquals ||
        binary.Left()->Type() != TypeTag::kColumnValue ||
        binary.Right()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
    if (left->GetSchema().Offset(lhs) >= 0 &&
        right->GetSchema().Offset(rhs) >= 0) {
      equalities.emplace_back(lhs, rhs);
    } else if (left->GetSchema().Offset(rhs) >= 0 &&
               right->GetSchema().Offset(lhs) >= 0) {
      equalities.emplace_back(rhs, lhs);
    }
  }

  std::vector<Plan> candidates;
  if (include_cross) {
    candidates.push_back(std::make_shared<ProductPlan>(left, right));
  }
  if (equalities.empty()) return candidates;

  std::vector<ColumnName> left_columns;
  std::vector<ColumnName> right_columns;
  for (const auto& [left_column, right_column] : equalities) {
    left_columns.push_back(left_column);
    right_columns.push_back(right_column);
  }

  if (include_hash) {
    candidates.push_back(std::make_shared<ProductPlan>(left, left_columns,
                                                       right, right_columns));
  }

  if (include_index) {
    const Table* right_table = right->ScanSource();
    if (!right_table) return candidates;
    ASSIGN_OR_CRASH(std::shared_ptr<TableStatistics>, statistics,
                    context.GetStats(right_table->GetSchema().Name()));
    for (size_t i = 0; i < right_table->IndexCount(); ++i) {
      const Index& index = right_table->GetIndex(i);
      for (const ColumnName& column : right_columns) {
        if (column ==
            right_table->GetSchema().GetColumn(index.sc_.key_.front()).Name()) {
          candidates.push_back(
              std::make_shared<ProductPlan>(left, left_columns, *right_table,
                                            index, right_columns, *statistics));
        }
      }
    }
  }
  return candidates;
}

std::vector<NamedExpression> ExpandSelect(const QueryData& query,
                                          TransactionContext& context) {
  const bool has_star =
      std::ranges::any_of(query.select_, [](const NamedExpression& selected) {
        return selected.expression->Type() == TypeTag::kColumnValue &&
               selected.expression->AsColumnValue().GetColumnName().name == "*";
      });
  if (!has_star) return query.select_;
  if (query.from_.size() != 1) {
    throw std::runtime_error("SELECT * with multiple tables not supported");
  }
  ASSIGN_OR_CRASH(std::shared_ptr<Table>, table,
                  context.GetTable(query.from_.front()));
  std::vector<NamedExpression> expanded;
  for (size_t i = 0; i < table->GetSchema().ColumnCount(); ++i) {
    expanded.emplace_back(table->GetSchema().GetColumn(i).Name());
  }
  return expanded;
}

bool IsAggregate(const NamedExpression& expression) {
  return expression.expression->Type() == TypeTag::kAggregateExp;
}

}  // namespace

StatusOr<Plan> Optimizer::Optimize(const QueryData& query,
                                   TransactionContext& context) {
  return Optimize(query, context, OptimizerOptions::Default());
}

StatusOr<Plan> Optimizer::Optimize(const QueryData& query,
                                   TransactionContext& context,
                                   const OptimizerOptions& options) {
  if (query.from_.empty()) throw std::runtime_error("No table specified");

  const std::vector<NamedExpression> expanded_select =
      ExpandSelect(query, context);
  const Expression source_predicate =
      query.where_ ? query.where_ : ConstantValueExp(Value(true));
  const Expression predicate =
      ExpressionRewriter(options.expression_rules).Rewrite(source_predicate);

  std::unordered_set<ColumnName> touched = predicate->TouchedColumns();
  for (const NamedExpression& selected : expanded_select) {
    touched.merge(selected.expression->TouchedColumns());
  }

  std::unordered_map<std::string, std::shared_ptr<Table>> tables;
  std::unordered_map<std::string, std::shared_ptr<TableStatistics>> statistics;
  for (const std::string& relation : query.from_) {
    ASSIGN_OR_RETURN(std::shared_ptr<Table>, table, context.GetTable(relation));
    ASSIGN_OR_RETURN(std::shared_ptr<TableStatistics>, table_statistics,
                     context.GetStats(relation));
    tables.emplace(relation, std::move(table));
    statistics.emplace(relation, std::move(table_statistics));
  }

  cascades::Memo memo;
  const cascades::GroupId root = memo.Build(query.from_);
  cascades::SearchEngine search(std::move(memo), options.relational_rules);
  const cascades::PhysicalProperties properties{
      .require_row_position = query.require_row_position_, .ordering = {}};

  const auto to_alternatives = [&](std::vector<Plan> plans) {
    std::vector<cascades::PlanAlternative> result;
    result.reserve(plans.size());
    for (Plan& plan : plans) {
      double cost = static_cast<double>(plan->AccessRowCount());
      if (!query.order_expressions_.empty() &&
          plan->IsOrderedBy(query.order_expressions_,
                            query.order_ascending_)) {
        cost = std::min(cost, 1.0);
      }
      result.push_back(
          cascades::PlanAlternative{std::move(plan), cost});
    }
    return result;
  };
  const auto scan_alternatives =
      [&](const cascades::LogicalExpression& logical,
          const cascades::PhysicalProperties& required, bool indexes,
          bool full_scan) {
        const Table& table = *tables.at(logical.table);
        std::vector<NamedExpression> projection;
        for (size_t i = 0; i < table.GetSchema().ColumnCount(); ++i) {
          const Column& table_column = table.GetSchema().GetColumn(i);
          if (std::ranges::any_of(touched, [&](const ColumnName& column) {
                return table_column.Name().name == column.name &&
                       (column.schema.empty() ||
                        column.schema == table.GetSchema().Name());
              })) {
            projection.emplace_back(table_column.Name());
          }
        }
        return to_alternatives(ScanCandidates(
            projection, table, predicate, *statistics.at(logical.table),
            required.require_row_position, indexes, full_scan,
            query.order_expressions_, query.order_ascending_));
      };
  const auto join_alternatives =
      [&](const std::vector<cascades::BestPlan>& children, bool hash,
          bool index, bool cross) {
        if (children.size() != 2) {
          return std::vector<cascades::PlanAlternative>{};
        }
        return to_alternatives(
            JoinCandidates(context, predicate, children[0].plan,
                           children[1].plan, hash, index, cross));
      };

  using namespace cascades::dsl;
  cascades::ImplementationRuleSet implementation_rules;
  implementation_rules.Add(cascades::ImplementationRule(
      "index_scan", Scan(),
      [&](const cascades::Bindings&, const cascades::LogicalExpression& logical,
          const std::vector<cascades::BestPlan>&,
          const cascades::PhysicalProperties& required) {
        return scan_alternatives(logical, required, true, false);
      }));
  implementation_rules.Add(cascades::ImplementationRule(
      "full_scan", Scan(),
      [&](const cascades::Bindings&, const cascades::LogicalExpression& logical,
          const std::vector<cascades::BestPlan>&,
          const cascades::PhysicalProperties& required) {
        return scan_alternatives(logical, required, false, true);
      }));
  implementation_rules.Add(cascades::ImplementationRule(
      "hash_join", Join(),
      [&](const cascades::Bindings&, const cascades::LogicalExpression&,
          const std::vector<cascades::BestPlan>& children,
          const cascades::PhysicalProperties&) {
        return join_alternatives(children, true, false, false);
      }));
  implementation_rules.Add(cascades::ImplementationRule(
      "index_join", Join(),
      [&](const cascades::Bindings&, const cascades::LogicalExpression&,
          const std::vector<cascades::BestPlan>& children,
          const cascades::PhysicalProperties&) {
        return join_alternatives(children, false, true, false);
      }));
  implementation_rules.Add(cascades::ImplementationRule(
      "nested_loop_join", Join(),
      [&](const cascades::Bindings&, const cascades::LogicalExpression&,
          const std::vector<cascades::BestPlan>& children,
          const cascades::PhysicalProperties&) {
        return join_alternatives(children, false, false, true);
      }));
  for (const std::string& disabled : options.disabled_implementation_rules) {
    implementation_rules.Remove(disabled);
  }
  for (const cascades::ImplementationRule& extra :
       options.extra_implementation_rules) {
    implementation_rules.Add(extra);
  }

  std::optional<cascades::BestPlan> best =
      search.Optimize(root, properties, implementation_rules);
  if (!best) return Status::kNotImplemented;

  // Range and join implementation rules may consume part of the predicate.
  // Keeping the normalized complete predicate at the root guarantees that a
  // newly added implementation rule cannot silently drop residual clauses.
  Plan solution = std::make_shared<SelectionPlan>(best->plan, predicate,
                                                  best->plan->GetStats());

  const bool has_aggregate = std::ranges::any_of(expanded_select, IsAggregate);
  if (has_aggregate) {
    if (!std::ranges::all_of(expanded_select, IsAggregate)) {
      return Status::kNotImplemented;
    }
    solution = std::make_shared<AggregationPlan>(solution, expanded_select);
  } else {
    solution = std::make_shared<ProjectionPlan>(solution, expanded_select);
  }
  return solution;
}

}  // namespace tinylamb
