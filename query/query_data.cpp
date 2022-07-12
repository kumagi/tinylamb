//
// Created by kumagi on 22/07/10.
//

#include "query_data.hpp"

#include <stack>

#include "database/transaction_context.hpp"
#include "expression/binary_expression.hpp"
#include "table/table.hpp"

namespace tinylamb {

namespace {

Status ResolveExpression(
    Expression& exp,
    const std::unordered_map<std::string, std::string>& col_table_map,
    const std::unordered_set<std::string>& ambiguous_colum_name) {
  if (exp->Type() == TypeTag::kColumnValue) {
    auto& cv = exp->AsColumnValue();
    const ColumnName& col_name = cv.GetColumnName();
    if (!col_name.schema.empty()) {
      return Status::kSuccess;
    }
    if (ambiguous_colum_name.contains(col_name.name)) {
      return Status::kAmbiguousQuery;
    }
    const auto it = col_table_map.find(col_name.name);
    if (it == col_table_map.end()) {
      return Status::kNotExists;
    }
    cv.SetSchemaName(it->second);
  }
  return Status::kSuccess;
}

}  // namespace

Status QueryData::Rewrite(TransactionContext& ctx) {
  std::unordered_map<std::string, std::string> col_table_map;
  std::unordered_set<std::string> ambiguous_colum_name;
  for (const auto& table : from_) {
    ASSIGN_OR_RETURN(std::shared_ptr<Table>, from_table, ctx.GetTable(table));
    const Schema& sc = from_table->GetSchema();
    for (size_t i = 0; i < sc.ColumnCount(); ++i) {
      const ColumnName& col_name = sc.GetColumn(i).Name();
      if (col_table_map.find(col_name.name) == col_table_map.end()) {
        col_table_map.emplace(col_name.name, table);
      } else {
        ambiguous_colum_name.emplace(col_name.name);
      }
    }
  }

  // Rewrite SELECT clause.
  for (auto& cell : select_) {
    RETURN_IF_FAIL(ResolveExpression(cell.expression, col_table_map,
                                     ambiguous_colum_name));
  }

  // Rewrite WHERE clause.
  std::stack<Expression> stack;
  stack.push(where_);
  while (!stack.empty()) {
    Expression exp = stack.top();
    stack.pop();
    if (exp->Type() == TypeTag::kBinaryExp) {
      const auto& be = exp->AsBinaryExpression();
      stack.push(be.Left());
      stack.push(be.Right());
    } else if (exp->Type() == TypeTag::kColumnValue) {
      RETURN_IF_FAIL(
          ResolveExpression(exp, col_table_map, ambiguous_colum_name));
    }
  }
  return Status::kSuccess;
}

}  // namespace tinylamb