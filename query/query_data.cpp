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

Status ResolveSelect(
    std::vector<NamedExpression>& select,
    const std::unordered_map<std::string, std::string>& col_table_map,
    const std::unordered_set<std::string>& ambiguous_colum_name,
    const std::vector<ColumnName>& all_cols) {
  for (auto it = select.begin(); it != select.end();) {
    if (it->expression->Type() == TypeTag::kColumnValue) {
      auto& cv = it->expression->AsColumnValue();
      const ColumnName& col_name = cv.GetColumnName();
      if (col_name.name == "*") {
        it = select.erase(it);
        for (const auto& cols : all_cols) {
          if (ambiguous_colum_name.contains(cols.name)) {
            it = select.insert(it, NamedExpression(cols));
          } else {
            it = select.insert(it, NamedExpression(cols.name, cols));
          }
          ++it;
        }
        continue;
      }
      if (!col_name.schema.empty()) {
        ++it;
        continue;
      }
      const auto col_it = col_table_map.find(col_name.name);
      if (col_it == col_table_map.end()) {
        return Status::kNotExists;
      }
      cv.SetSchemaName(col_it->second);
      ++it;
    } else {
      ++it;
    }
  }
  return Status::kSuccess;
}

}  // namespace

Status QueryData::Rewrite(TransactionContext& ctx) {
  std::unordered_map<std::string, std::string> col_table_map;
  std::unordered_set<std::string> ambiguous_colum_name;
  std::vector<ColumnName> all_cols;

  for (const auto& table : from_) {
    ASSIGN_OR_RETURN(std::shared_ptr<Table>, from_table, ctx.GetTable(table));
    const Schema& sc = from_table->GetSchema();
    for (size_t i = 0; i < sc.ColumnCount(); ++i) {
      const ColumnName& col_name = sc.GetColumn(i).Name();
      all_cols.emplace_back(sc.Name(), col_name.name);
      if (col_table_map.find(col_name.name) == col_table_map.end()) {
        col_table_map.emplace(col_name.name, table);
      } else {
        ambiguous_colum_name.emplace(col_name.name);
      }
    }
  }

  // Rewrite SELECT clause.
  ResolveSelect(select_, col_table_map, ambiguous_colum_name, all_cols);

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