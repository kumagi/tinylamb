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


#include "optimizer.hpp"

#include <algorithm>
#include <cassert>
#include <stack>
#include <unordered_map>
#include <unordered_set>

#include "common/converter.hpp"
#include "database/relation_storage.hpp"
#include "database/transaction_context.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "full_scan_plan.hpp"
#include "index_only_scan_plan.hpp"
#include "index_scan_plan.hpp"
#include "plan/plan.hpp"
#include "product_plan.hpp"
#include "projection_plan.hpp"
#include "selection_plan.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"

namespace std {
template <>
class hash<std::unordered_set<std::string>> {
 public:
  uint64_t operator()(const std::unordered_set<std::string>& s) const {
    uint64_t out = 0;
    for (const auto& e : s) {
      out += std::hash<std::string>()(e);
    }
    return out;
  }
};
}  // namespace std

namespace tinylamb {
namespace {

struct CostAndPlan {
  size_t cost;
  Plan plan;
  [[maybe_unused]] friend std::ostream& operator<<(std::ostream& o,
                                                   const CostAndPlan& c) {
    o << c.cost << ": " << *c.plan;
    return o;
  }
};

template <typename T>
std::unordered_set<T> Union(const std::unordered_set<T>& a,
                            const std::unordered_set<T>& b) {
  std::unordered_set<T> ret = a;
  std::copy(b.begin(), b.end(), std::inserter(ret, ret.end()));
  return ret;
}

struct Range {
  std::optional<Value> min;
  std::optional<Value> max;
  bool min_inclusive = false;
  bool max_inclusive = false;
  [[nodiscard]] bool Empty() const {
    return !min.has_value() && !max.has_value();
  }
  enum class Dir : bool { kRight, kLeft };
  void Update(BinaryOperation op, const Value& rhs, Dir dir) {
    switch (op) {
      case BinaryOperation::kEquals:
        // e.g. x == 10
        max = min = rhs;
        min_inclusive = max_inclusive = true;
        break;
      case BinaryOperation::kNotEquals:
        // e.g. x != 10
        // We have nothing to do here.
        break;
      case BinaryOperation::kLessThan:
      case BinaryOperation::kGreaterThan:
        if ((dir == Dir::kRight && op == BinaryOperation::kLessThan) ||
            (dir == Dir::kLeft && op == BinaryOperation::kGreaterThan)) {
          // e.g. x < 10
          // e.g. 10 > x
          if (!max.has_value() || (max.has_value() && rhs < *max)) {
            max = rhs;
            max_inclusive = false;
          }
        } else {
          // e.g. 10 < x
          // e.g. x > 10
          if (!min.has_value() || (min.has_value() && *min < rhs)) {
            min = rhs;
            min_inclusive = false;
          }
        }
        break;
      case BinaryOperation::kLessThanEquals:
      case BinaryOperation::kGreaterThanEquals:
        if ((dir == Dir::kRight && op == BinaryOperation::kLessThanEquals) ||
            (dir == Dir::kLeft && op == BinaryOperation::kGreaterThanEquals)) {
          // e.g. x <= 10
          // e.g. 10 >= x
          if (!max.has_value() || (max.has_value() && rhs <= *max)) {
            max = rhs;
            max_inclusive = true;
          }
        } else {
          // e.g. 10 <= x
          // e.g. x >= 10
          min = rhs;
          min_inclusive = true;
        }
        break;
      default:
        assert(!"invalid operator to update");
    }
  }
};

bool TouchOnly(const Expression& where, const ColumnName& col_name) {
  if (where->Type() == TypeTag::kColumnValue) {
    const ColumnValue& cv = where->AsColumnValue();
    return cv.GetColumnName() == col_name;
  }
  if (where->Type() == TypeTag::kBinaryExp) {
    const BinaryExpression& be = where->AsBinaryExpression();
    return TouchOnly(be.Left(), col_name) && TouchOnly(be.Right(), col_name);
  }
  assert(where->Type() == TypeTag::kConstantValue);
  return true;
}

template <typename T>
bool Covered(const std::unordered_set<T> big,
             const std::unordered_set<T> small) {
  return std::ranges::all_of(small.begin(), small.end(),
                             [&](const T& t) { return big.contains(t); });
}

Plan IndexScanSelect(const Table& from, const Index& target_idx,
                     const TableStatistics& stat, const Value& begin,
                     const Value& end, const Expression& where,
                     const std::vector<NamedExpression>& select) {
  std::unordered_set<ColumnName> touched = where->TouchedColumns();
  for (const auto& s : select) {
    touched.merge(s.expression->TouchedColumns());
  }
  std::unordered_set<slot_t> touched_cols;
  touched_cols.reserve(touched.size());
  for (const auto& t : touched) {
    touched_cols.insert(from.GetSchema().Offset(t));
  }
  if (Covered(target_idx.CoveredColumns(), touched_cols)) {
    return std::make_shared<IndexOnlyScanPlan>(from, target_idx, stat, begin,
                                               end, true, where);
  }
  return std::make_shared<IndexScanPlan>(from, target_idx, stat, begin, end,
                                         true, where);
}

Plan BestScan(const std::vector<NamedExpression>& select, const Table& from,
              const Expression& where, const TableStatistics& stat) {
  const Schema& sc = from.GetSchema();
  size_t minimum_cost = std::numeric_limits<size_t>::max();
  Plan best_scan;
  // Get { first-column offset => index offset } map.
  std::unordered_map<slot_t, size_t> candidates = from.AvailableKeyIndex();
  // Prepare all range of candidates.
  std::unordered_map<slot_t, Range> ranges;
  ranges.reserve(candidates.size());
  for (const auto& it : candidates) {
    ranges.emplace(it.first, Range());
  }
  std::vector<Expression> stack;
  stack.push_back(where);
  std::vector<Expression> related_ops;
  while (!stack.empty()) {
    Expression exp = stack.back();
    stack.pop_back();
    if (exp->Type() == TypeTag::kBinaryExp) {
      const BinaryExpression& be = exp->AsBinaryExpression();
      if (be.Op() == BinaryOperation::kAnd) {
        stack.push_back(be.Left());
        stack.push_back(be.Right());
        continue;
      }
      if (IsComparison(be.Op())) {
        if (be.Left()->Type() == TypeTag::kColumnValue &&
            be.Right()->Type() == TypeTag::kConstantValue) {
          const ColumnValue& cv = be.Left()->AsColumnValue();
          const int offset = sc.Offset(cv.GetColumnName());
          if (0 <= offset) {
            related_ops.push_back(exp);
            auto iter = ranges.find(offset);
            if (iter != ranges.end()) {
              iter->second.Update(be.Op(),
                                  be.Right()->AsConstantValue().GetValue(),
                                  Range::Dir::kRight);
            }
          }
        } else if (be.Left()->Type() == TypeTag::kColumnValue &&
                   be.Right()->Type() == TypeTag::kConstantValue) {
          const ColumnValue& cv = be.Right()->AsColumnValue();
          const int offset = sc.Offset(cv.GetColumnName());
          if (0 <= offset) {
            related_ops.push_back(exp);
            auto iter = ranges.find(offset);
            if (iter != ranges.end()) {
              iter->second.Update(be.Op(),
                                  be.Left()->AsConstantValue().GetValue(),
                                  Range::Dir::kLeft);
            }
          }
        }
      }
      if (be.Op() == BinaryOperation::kOr) {
        assert(!"Not supported!");
      }
    }
  }
  Expression scan_exp;
  if (!related_ops.empty()) {
    scan_exp = related_ops[0];
    for (size_t i = 1; i < related_ops.size(); ++i) {
      scan_exp =
          BinaryExpressionExp(scan_exp, BinaryOperation::kAnd, related_ops[i]);
    }
  }

  // Build all IndexScan.
  for (const auto& range : ranges) {
    slot_t key = range.first;
    const Range& span = range.second;
    if (span.Empty()) {
      continue;
    }
    const Index& target_idx = from.GetIndex(candidates[key]);
    Plan new_plan = IndexScanSelect(from, target_idx, stat, *span.min,
                                    *span.max, scan_exp, select);
    if (!TouchOnly(scan_exp, from.GetSchema().GetColumn(key).Name())) {
      new_plan = std::make_shared<SelectionPlan>(new_plan, scan_exp, stat);
    }
    if (select.size() != new_plan->GetSchema().ColumnCount()) {
      new_plan = std::make_shared<ProjectionPlan>(new_plan, select);
    }
    if (new_plan->AccessRowCount() < minimum_cost) {
      best_scan = new_plan;
      minimum_cost = new_plan->AccessRowCount();
    }
  }

  Plan full_scan_plan(new FullScanPlan(from, stat));
  if (scan_exp) {
    full_scan_plan =
        std::make_shared<SelectionPlan>(full_scan_plan, scan_exp, stat);
  }
  if (select.size() != full_scan_plan->GetSchema().ColumnCount()) {
    full_scan_plan = std::make_shared<ProjectionPlan>(full_scan_plan, select);
  }
  if (full_scan_plan->AccessRowCount() < minimum_cost) {
    best_scan = full_scan_plan;
    minimum_cost = full_scan_plan->AccessRowCount();
  }
  return best_scan;
}

bool ContainsAny(const std::unordered_set<std::string>& left,
                 const std::unordered_set<std::string>& right) {
  return std::any_of(left.begin(), left.end(), [&](const std::string& s) {
    return right.find(s) != right.end();
  });
}

Plan BestJoin(TransactionContext& ctx, const Expression& where,
              const Plan& left, const Plan& right) {
  std::vector<std::pair<ColumnName, ColumnName>> equals;
  std::stack<Expression> exp;
  exp.push(where);
  std::vector<Expression> related_exp;
  while (!exp.empty()) {
    const Expression here = exp.top();
    exp.pop();
    switch (here->Type()) {
      case TypeTag::kBinaryExp: {
        const auto& be = here->AsBinaryExpression();
        if (be.Op() == BinaryOperation::kEquals &&
            be.Right()->Type() == TypeTag::kColumnValue &&
            be.Left()->Type() == TypeTag::kColumnValue) {
          const auto& cv_l = be.Left()->AsColumnValue();
          const auto& cv_r = be.Right()->AsColumnValue();
          if (0 <= left->GetSchema().Offset(cv_l.GetColumnName()) &&
              0 <= right->GetSchema().Offset(cv_r.GetColumnName())) {
            equals.emplace_back(cv_l.GetColumnName(), cv_r.GetColumnName());
            related_exp.push_back(here);
          } else if (0 <= right->GetSchema().Offset(cv_l.GetColumnName()) &&
                     0 <= left->GetSchema().Offset(cv_r.GetColumnName())) {
            equals.emplace_back(cv_r.GetColumnName(), cv_l.GetColumnName());
            related_exp.push_back(here);
          }
        }
        if (be.Op() == BinaryOperation::kAnd) {
          exp.push(be.Left());
          exp.push(be.Right());
        }
        break;
      }
      case TypeTag::kColumnValue:
      case TypeTag::kConstantValue:
        // Ignore.
        break;
    }
  }

  std::vector<Plan> candidates;
  if (!equals.empty()) {
    std::vector<ColumnName> left_cols;
    std::vector<ColumnName> right_cols;
    left_cols.reserve(equals.size());
    right_cols.reserve(equals.size());
    for (auto&& cn : equals) {
      left_cols.emplace_back(std::move(cn.first));
      right_cols.emplace_back(std::move(cn.second));
    }
    // HashJoin.
    candidates.push_back(
        std::make_shared<ProductPlan>(left, left_cols, right, right_cols));
    candidates.push_back(
        std::make_shared<ProductPlan>(right, right_cols, left, left_cols));

    // IndexJoin.
    if (const Table* right_tbl = right->ScanSource()) {
      for (size_t i = 0; i < right_tbl->IndexCount(); ++i) {
        const Index& right_idx = right_tbl->GetIndex(i);
        const Schema& right_schema = right_tbl->GetSchema();
        ASSIGN_OR_CRASH(std::shared_ptr<TableStatistics>, stat,
                        ctx.GetStats(right_tbl->GetSchema().Name()));
        for (const auto& rcol : right_cols) {
          if (rcol == right_schema.GetColumn(right_idx.sc_.key_[0]).Name()) {
            candidates.push_back(std::make_shared<ProductPlan>(
                left, left_cols, *right_tbl, right_idx, right_cols, *stat));
          }
        }
      }
    }
  }
  if (candidates.empty()) {
    if (0 < related_exp.size()) {
      Expression final_selection = related_exp.back();
      related_exp.pop_back();
      for (const auto& e : related_exp) {
        final_selection =
            BinaryExpressionExp(e, BinaryOperation::kAnd, final_selection);
      }
      Plan ans = std::make_shared<ProductPlan>(left, right);
      candidates.push_back(std::make_shared<SelectionPlan>(ans, final_selection,
                                                           ans->GetStats()));
    } else {
      candidates.push_back(std::make_shared<ProductPlan>(left, right));
    }
  }
  std::sort(candidates.begin(), candidates.end(),
            [](const Plan& a, const Plan& b) {
              return a->AccessRowCount() < b->AccessRowCount();
            });
  return candidates[0];
}

}  // anonymous namespace

StatusOr<Plan> Optimizer::Optimize(const QueryData& query,
                                   TransactionContext& ctx) {
  if (query.from_.empty()) {
    throw std::runtime_error("No table specified");
  }
  std::unordered_map<std::unordered_set<std::string>, CostAndPlan>
      optimal_plans;

  // 1. Initialize every single tables to start.
  std::unordered_set<ColumnName> touched_columns =
      query.where_->TouchedColumns();
  for (const auto& sel : query.select_) {
    touched_columns.merge(sel.expression->TouchedColumns());
  }
  for (const auto& from : query.from_) {
    ASSIGN_OR_RETURN(std::shared_ptr<Table>, tbl, ctx.GetTable(from));
    ASSIGN_OR_RETURN(std::shared_ptr<TableStatistics>, stats,
                     ctx.GetStats(from));

    // Push down all selection & projection.
    std::vector<NamedExpression> project_target;
    for (size_t i = 0; i < tbl->GetSchema().ColumnCount(); ++i) {
      for (const auto& touched_col : touched_columns) {
        const Column& table_col = tbl->GetSchema().GetColumn(i);
        if (table_col.Name().name == touched_col.name &&
            (touched_col.schema.empty() ||
             touched_col.schema == tbl->GetSchema().Name())) {
          project_target.emplace_back(table_col.Name());
        }
      }
    }
    Plan scan = BestScan(project_target, *tbl, query.where_, *stats);
    optimal_plans.emplace(std::unordered_set({from}),
                          CostAndPlan{scan->AccessRowCount(), scan});
  }
  assert(optimal_plans.size() == query.from_.size());

  // 2. Combine all tables to find the best combination;
  for (size_t i = 0; i < query.from_.size(); ++i) {
    for (const auto& base_table : optimal_plans) {
      for (const auto& join_table : optimal_plans) {
        if (ContainsAny(base_table.first, join_table.first)) {
          continue;
        }
        Plan best_join = BestJoin(ctx, query.where_, base_table.second.plan,
                                  join_table.second.plan);
        std::unordered_set<std::string> joined_tables =
            Union(base_table.first, join_table.first);
        assert(1 < joined_tables.size());
        size_t cost = best_join->AccessRowCount();
        auto iter = optimal_plans.find(joined_tables);
        if (iter == optimal_plans.end()) {
          optimal_plans.emplace(joined_tables, CostAndPlan{cost, best_join});
        } else if (cost < iter->second.cost) {
          iter->second = CostAndPlan{cost, best_join};
        }
      }
    }
  }
  assert(optimal_plans.find(MakeSet(query.from_)) != optimal_plans.end());

  // 3. Attach final projection and emit the result.
  auto solution = optimal_plans.find(MakeSet(query.from_))->second.plan;
  solution = std::make_shared<ProjectionPlan>(solution, query.select_);
  return solution;
}

}  // namespace tinylamb