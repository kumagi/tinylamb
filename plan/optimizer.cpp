//
// Created by kumagi on 2022/03/02.
//

#include "optimizer.hpp"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

#include "database/catalog.hpp"
#include "database/transaction_context.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "plan.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"

namespace std {
template <>
class hash<std::unordered_set<std::string>> {
 public:
  uint64_t operator()(const std::unordered_set<std::string>& s) const {
    uint64_t out = 0;
    for (const auto& e : s) out += std::hash<std::string>()(e);
    return out;
  }
};
}  // namespace std

namespace tinylamb {
namespace {

struct CostAndPlan {
  int cost;
  Plan plan;
};

void ExtractTouchedColumns(const ExpressionBase* ptr,
                           std::unordered_set<std::string>& cols) {
  if (ptr->Type() == TypeTag::kColumnValue) {
    auto* cv = reinterpret_cast<const ColumnValue*>(ptr);
    cols.emplace(cv->ColumnName());
  } else if (ptr->Type() == TypeTag::kBinaryExp) {
    auto* be = reinterpret_cast<const BinaryExpression*>(ptr);
    ExtractTouchedColumns(be->Left().get(), cols);
    ExtractTouchedColumns(be->Right().get(), cols);
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

std::unordered_set<std::string> And(const std::unordered_set<std::string>& a,
                                    const std::unordered_set<std::string>& b) {
  std::unordered_set<std::string> ret;
  for (const auto& l : a) {
    if (b.find(l) != b.end()) ret.emplace(l);
  }
  return ret;
}

bool ReferenceWithin(const ExpressionBase* ptr,
                     const std::unordered_set<std::string>& columns) {
  if (ptr->Type() == TypeTag::kBinaryExp) {
    const auto* be = reinterpret_cast<const BinaryExpression*>(ptr);
    return ReferenceWithin(be->Left().get(), columns) ||
           ReferenceWithin(be->Right().get(), columns);
  }
  if (ptr->Type() == TypeTag::kColumnValue) {
    const auto* be = reinterpret_cast<const ColumnValue*>(ptr);
    return columns.find(be->ColumnName()) != columns.end();
  }
  return false;
}

bool DoesTouchWithin(const ExpressionBase* ptr,
                     const std::unordered_set<std::string>& columns) {
  if (ptr->Type() == TypeTag::kBinaryExp) {
    const auto* be = reinterpret_cast<const BinaryExpression*>(ptr);
    return DoesTouchWithin(be->Left().get(), columns) &&
           DoesTouchWithin(be->Right().get(), columns);
  }
  if (ptr->Type() == TypeTag::kColumnValue) {
    const auto* be = reinterpret_cast<const ColumnValue*>(ptr);
    return columns.find(be->ColumnName()) != columns.end();
  }
  return true;
}

Plan CalcTableSelection(const Schema& schema, const Expression& expression,
                        const TableStatistics& stat) {
  // Generate expression for selection.
  std::unordered_set<std::string> columns = ColumnNames(schema);
  if (expression.Type() == TypeTag::kBinaryExp) {
    auto* be = reinterpret_cast<BinaryExpression*>(expression.exp_.get());
    if (DoesTouchWithin(be->Left().get(), columns) &&
        DoesTouchWithin(be->Right().get(), columns)) {
      return Plan::Selection(Plan::FullScan(schema.Name(), stat), expression,
                             stat);
    }
    if (be->Op() == BinaryOperation::kAnd) {
      if (DoesTouchWithin(be->Left().get(), columns)) {
        return CalcTableSelection(schema, Expression(be->Left()), stat);
      }
      if (DoesTouchWithin(be->Right().get(), columns)) {
        return CalcTableSelection(schema, Expression(be->Right()), stat);
      }
    }
  }
  return Plan::FullScan(schema.Name(), stat);
}

bool ContainsAny(const std::unordered_set<std::string>& left,
                 const std::unordered_set<std::string>& right) {
  return std::any_of(left.begin(), left.end(), [&](const std::string& s) {
    return right.find(s) != right.end();
  });
}

std::unordered_set<std::string> JoinName(
    const std::unordered_set<std::string>& left,
    const std::unordered_set<std::string>& right) {
  std::unordered_set<std::string> ret = left;
  std::copy(right.begin(), right.end(), std::inserter(ret, ret.end()));
  return ret;
}

void ExtractColumns(const Schema& sc, ExpressionBase* exp,
                    std::vector<size_t>& dst) {
  if (exp->Type() == TypeTag::kColumnValue) {
    auto* cv = reinterpret_cast<ColumnValue*>(exp);
    const std::string& name = cv->ColumnName();
    for (size_t i = 0; i < sc.ColumnCount(); ++i) {
      if (sc.GetColumn(i).Name() == name) dst.push_back(i);
    }
  } else if (exp->Type() == TypeTag::kBinaryExp) {
    auto* be = reinterpret_cast<BinaryExpression*>(exp);
    if (be->Op() == BinaryOperation::kAnd) {
      ExtractColumns(sc, be->Left().get(), dst);
      ExtractColumns(sc, be->Right().get(), dst);
    }
  }
}

std::vector<size_t> TargetColumns(const Schema& sc, ExpressionBase* exp) {
  std::vector<size_t> out;
  ExtractColumns(sc, exp, out);
  return out;
}

Plan BetterPlan(TransactionContext& ctx, Plan a, Plan b) {
  if (a.AccessRowCount(ctx) < b.AccessRowCount(ctx)) {
    return a;
  } else {
    return b;
  }
}

Plan BestJoin(TransactionContext& ctx, ExpressionBase* where,
              const std::pair<std::unordered_set<std::string>,
                              CostAndPlan>& left,
              const std::pair<std::unordered_set<std::string>,
                              CostAndPlan>& right) {
  if (where->Type() == TypeTag::kBinaryExp) {
    auto* be = reinterpret_cast<BinaryExpression*>(where);
    if (be->Op() == BinaryOperation::kEquals) {
      Schema left_schema = left.second.plan.GetSchema(ctx);
      std::unordered_set<std::string> left_column_names =
          ColumnNames(left_schema);
      Schema right_schema = right.second.plan.GetSchema(ctx);
      std::unordered_set<std::string> right_column_names =
          ColumnNames(right.second.plan.GetSchema(ctx));
      if (ReferenceWithin(be->Left().get(), left_column_names) &&
          ReferenceWithin(be->Right().get(), right_column_names)) {
        // We can use HashJoin!
        std::vector<size_t> left_cols =
            TargetColumns(left_schema, be->Left().get());
        std::vector<size_t> right_cols =
            TargetColumns(right_schema, be->Right().get());
        return BetterPlan(ctx,
                          Plan::Product(left.second.plan, left_cols,
                                        right.second.plan, right_cols),
                          Plan::Product(right.second.plan, right_cols,
                                        left.second.plan, left_cols));
      }
    } else if (be->Op() == BinaryOperation::kAnd) {
      return BetterPlan(ctx, BestJoin(ctx, be->Left().get(), left, right),
                        BestJoin(ctx, be->Right().get(), left, right));
    }
  }
  return Plan::Product(left.second.plan, right.second.plan);
}

std::unordered_set<std::string> Set(const std::vector<std::string>& from) {
  std::unordered_set<std::string> joined;
  for (const auto& f : from) joined.insert(f);
  return joined;
}

}  // anonymous namespace

Status Optimizer::Optimize(const QueryData& query, TransactionContext& ctx,
                           Schema& schema,
                           std::unique_ptr<ExecutorBase>& exec) {
  if (query.from_.empty()) throw std::runtime_error("No table specified");
  std::unordered_map<std::unordered_set<std::string>, CostAndPlan>
      optimal_plans;
  std::unordered_map<std::string, TableStatistics> stats;

  // 1. Initialize every single tables to start.
  std::unordered_set<std::string> touched_columns;
  ExtractTouchedColumns(query.where_.exp_.get(), touched_columns);
  for (const auto& sel : query.select_) {
    ExtractTouchedColumns(sel.expression.exp_.get(), touched_columns);
  }
  for (const auto& from : query.from_) {
    Table tbl(ctx.pm_);
    ctx.c_->GetTable(ctx.txn_, from, &tbl);

    // Get statistics.
    TableStatistics ts(tbl.GetSchema());
    ctx.c_->GetStatistics(ctx.txn_, from, &ts);
    stats.emplace(from, ts);

    // Push down all selection & projection.
    std::vector<NamedExpression> project_target;
    for (const auto& col : And(touched_columns, ColumnNames(tbl.GetSchema()))) {
      project_target.emplace_back(col);
    }
    optimal_plans.emplace(
        std::unordered_set({from}),
        CostAndPlan{100, Plan::Projection(
            CalcTableSelection(tbl.GetSchema(), query.where_, ts),
                     project_target)});
  }

  // 2. Combine all tables to find the best combination;
  for (size_t i = 0; i < query.from_.size(); ++i) {
    for (const auto& base_table : optimal_plans) {
      for (const auto& join_table : optimal_plans) {
        if (ContainsAny(base_table.first, join_table.first)) continue;
        Plan bj =
            BestJoin(ctx, query.where_.exp_.get(), base_table, join_table);
        std::unordered_set<std::string> joined_tables =
            JoinName(base_table.first, join_table.first);
        int cost = bj.AccessRowCount(ctx);
        auto iter = optimal_plans.find(joined_tables);
        if (iter == optimal_plans.end()) {
          optimal_plans.emplace(joined_tables, CostAndPlan{cost, bj});
        } else if (cost < iter->second.cost) {
          iter->second = CostAndPlan{cost, bj};
        }
      }
    }
  }

  // 3. Attach final projection and emit the result.
  auto solution = optimal_plans.find(Set(query.from_))->second.plan;
  solution = Plan::Projection(solution, query.select_);
  schema = solution.GetSchema(ctx);
  exec = solution.EmitExecutor(ctx);
  return Status::kSuccess;
}

}  // namespace tinylamb