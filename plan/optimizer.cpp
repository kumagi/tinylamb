//
// Created by kumagi on 2022/03/02.
//

#include "optimizer.hpp"

#include <algorithm>
#include <cassert>
#include <unordered_map>
#include <unordered_set>

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
};

void ExtractTouchedColumns(const Expression& exp,
                           std::unordered_set<std::string>& cols) {
  if (exp->Type() == TypeTag::kColumnValue) {
    const auto* cv = reinterpret_cast<const ColumnValue*>(exp.get());
    cols.emplace(cv->ColumnName());
  } else if (exp->Type() == TypeTag::kBinaryExp) {
    const auto* be = reinterpret_cast<const BinaryExpression*>(exp.get());
    ExtractTouchedColumns(be->Left(), cols);
    ExtractTouchedColumns(be->Right(), cols);
  }
}

std::unordered_set<std::string> ColumnNames(const Schema& sc) {
  std::unordered_set<std::string> ret;
  ret.reserve(sc.ColumnCount());
  for (size_t i = 0; i < sc.ColumnCount(); ++i) {
    ret.emplace(sc.GetColumn(i).Name());
  }
  return ret;
}

std::unordered_set<std::string> Intersect(
    const std::unordered_set<std::string>& a,
    const std::unordered_set<std::string>& b) {
  std::unordered_set<std::string> ret;
  for (const auto& l : a) {
    if (b.contains(l)) {
      ret.emplace(l);
    }
  }
  return ret;
}

std::unordered_set<std::string> Union(
    const std::unordered_set<std::string>& a,
    const std::unordered_set<std::string>& b) {
  std::unordered_set<std::string> ret = a;
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

bool TouchOnly(const Expression& where, std::string_view col_name) {
  if (where->Type() == TypeTag::kColumnValue) {
    const ColumnValue& cv = where->AsColumnValue();
    return cv.ColumnName() == col_name;
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
  std::unordered_set<std::string> touched = where->TouchedColumns();
  for (const auto& s : select) {
    touched.emplace(s.name);
  }
  std::unordered_set<slot_t> touched_cols;
  touched_cols.reserve(touched.size());
  for (const auto& t : touched) {
    touched_cols.insert(from.GetSchema().Offset(t));
  }
  if (Covered(target_idx.CoeveredColumns(), touched_cols)) {
    return std::make_shared<IndexOnlyScanPlan>(from, target_idx, stat, begin,
                                               end, true, where, select);
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
          const int offset = sc.Offset(cv.ColumnName());
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
          const int offset = sc.Offset(cv.ColumnName());
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

void ExtractColumns(const Schema& sc, const Expression& exp,
                    std::vector<slot_t>& dst) {
  if (exp->Type() == TypeTag::kColumnValue) {
    const ColumnValue& cv = exp->AsColumnValue();
    const std::string& name = cv.ColumnName();
    for (size_t i = 0; i < sc.ColumnCount(); ++i) {
      if (sc.GetColumn(i).Name() == name) {
        dst.push_back(i);
      }
    }
  } else if (exp->Type() == TypeTag::kBinaryExp) {
    const BinaryExpression& be = exp->AsBinaryExpression();
    if (be.Op() == BinaryOperation::kAnd) {
      ExtractColumns(sc, be.Left(), dst);
      ExtractColumns(sc, be.Right(), dst);
    }
  }
}

std::vector<slot_t> TargetColumns(const Schema& schema, const Expression& exp) {
  std::vector<slot_t> out;
  ExtractColumns(schema, exp, out);
  return out;
}

Plan BetterPlan(Plan a, Plan b) {
  if (a->AccessRowCount() < b->AccessRowCount()) {
    return a;
  }
  return b;
}

bool ReferenceWithin(const Expression& where,
                     const std::unordered_set<std::string>& columns) {
  if (where->Type() == TypeTag::kBinaryExp) {
    const BinaryExpression& be = where->AsBinaryExpression();
    return ReferenceWithin(be.Left(), columns) ||
           ReferenceWithin(be.Right(), columns);
  }
  if (where->Type() == TypeTag::kColumnValue) {
    const ColumnValue& cv = where->AsColumnValue();
    return columns.find(cv.ColumnName()) != columns.end();
  }
  return false;
}

Plan BestJoin(
    const Expression& where,
    const std::pair<std::unordered_set<std::string>, CostAndPlan>& left,
    const std::pair<std::unordered_set<std::string>, CostAndPlan>& right) {
  if (where->Type() == TypeTag::kBinaryExp) {
    const BinaryExpression& be = where->AsBinaryExpression();
    if (be.Op() == BinaryOperation::kEquals) {
      Schema left_schema = left.second.plan->GetSchema();
      std::unordered_set<std::string> left_column_names =
          ColumnNames(left_schema);
      Schema right_schema = right.second.plan->GetSchema();
      std::unordered_set<std::string> right_column_names =
          ColumnNames(right.second.plan->GetSchema());
      if (ReferenceWithin(be.Left(), left_column_names) &&
          ReferenceWithin(be.Right(), right_column_names)) {
        // We can use HashJoin!
        std::vector<slot_t> left_cols = TargetColumns(left_schema, be.Left());
        std::vector<slot_t> right_cols =
            TargetColumns(right_schema, be.Right());
        return BetterPlan(
            std::make_shared<ProductPlan>(left.second.plan, left_cols,
                                          right.second.plan, right_cols),
            std::make_shared<ProductPlan>(right.second.plan, right_cols,
                                          left.second.plan, left_cols));
      }
    } else if (be.Op() == BinaryOperation::kAnd) {
      return BetterPlan(BestJoin(be.Left(), left, right),
                        BestJoin(be.Right(), left, right));
    }
  }
  return std::make_shared<ProductPlan>(left.second.plan, right.second.plan);
}

std::unordered_set<std::string> Set(const std::vector<std::string>& from) {
  std::unordered_set<std::string> joined;
  for (const auto& f : from) {
    joined.insert(f);
  }
  return joined;
}

}  // anonymous namespace

Status Optimizer::Optimize(const QueryData& query, TransactionContext& ctx,
                           Schema& schema, Executor& exec) {
  if (query.from_.empty()) {
    throw std::runtime_error("No table specified");
  }
  std::unordered_map<std::unordered_set<std::string>, CostAndPlan>
      optimal_plans;

  // 1. Initialize every single tables to start.
  std::unordered_set<std::string> touched_columns;
  ExtractTouchedColumns(query.where_, touched_columns);
  for (const auto& sel : query.select_) {
    ExtractTouchedColumns(sel.expression, touched_columns);
  }
  for (const auto& from : query.from_) {
    ASSIGN_OR_RETURN(std::shared_ptr<Table>, tbl, ctx.GetTable(from));
    ASSIGN_OR_RETURN(std::shared_ptr<TableStatistics>, stats,
                     ctx.GetStats(from));

    // Push down all selection & projection.
    std::vector<NamedExpression> project_target;
    for (const auto& col :
         Intersect(touched_columns, ColumnNames(tbl->GetSchema()))) {
      project_target.emplace_back(col);
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
        Plan best_join = BestJoin(query.where_, base_table, join_table);
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
  assert(optimal_plans.find(Set(query.from_)) != optimal_plans.end());

  // 3. Attach final projection and emit the result.
  auto solution = optimal_plans.find(Set(query.from_))->second.plan;
  solution = std::make_shared<ProjectionPlan>(solution, query.select_);
  schema = solution->GetSchema();
  exec = solution->EmitExecutor(ctx);
  return Status::kSuccess;
}

}  // namespace tinylamb