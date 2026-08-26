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

//
// Created by kumagi on 22/07/10.
//

#include "query_data.hpp"

#include <cctype>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/constants.hpp"

namespace {
std::string ToLowerCopy(std::string value) {
  for (char& c : value) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return value;
}
}  // namespace
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "table/table.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"

namespace tinylamb {

namespace {

// Dotted references whose qualifier names a COLUMN (not a FROM relation)
// address encoded fields of proto / struct payloads: "value.int32_val",
// "t.value.nested_value.nested_int64".  Rewrite them into chained
// __get_field_safe reads so resolution succeeds against the base column.
Expression BindColumnQualifiedFieldReads(
    const Expression& expr,
    const std::unordered_map<std::string, std::string>& col_table_map,
    const std::unordered_set<std::string>& relations) {
  if (!expr) {
    return expr;
  }
  if (expr->Type() == TypeTag::kColumnValue) {
    const ColumnName& name = expr->AsColumnValue().GetColumnName();
    if (name.schema.empty() || name.name.empty() || name.name == "*" ||
        relations.contains(name.schema)) {
      return expr;
    }
    // The qualifier may itself be dotted ("t.value"); the first segment must
    // name a column of the driving table for this to be a field read.
    const size_t first_dot = name.schema.find('.');
    const std::string head = first_dot == std::string::npos
                                 ? name.schema
                                 : name.schema.substr(0, first_dot);
    const auto it = col_table_map.find(ToLowerCopy(head));
    if (it == col_table_map.end()) {
      return expr;
    }
    std::vector<std::string> segments;
    {
      std::string remainder = name.schema.substr(
          first_dot == std::string::npos ? name.schema.size() : first_dot + 1);
      if (!remainder.empty()) {
        segments.push_back(std::move(remainder));
      }
      segments.push_back(name.name);
    }
    Expression current = ColumnValueExp(ColumnName(it->second, head));
    for (const std::string& segment : segments) {
      current = FunctionCallExp(
          "__get_field_safe",
          {std::move(current), ConstantValueExp(Value(std::string(segment)))});
    }
    return current;
  }
  std::vector<Expression> children = ExpressionChildren(expr);
  bool changed = false;
  for (Expression& child : children) {
    Expression mapped = BindColumnQualifiedFieldReads(child, col_table_map,
                                                      relations);
    changed |= child->ToString() != mapped->ToString();
    child = std::move(mapped);
  }
  return changed ? WithExpressionChildren(expr, std::move(children)) : expr;
}

Status
ResolveExpression(  // NOLINT(misc-no-recursion) // Recursive expression-tree
                    // resolution by design; trees are parser-bounded in depth.
    Expression& exp,
    const std::unordered_map<std::string, std::string>& col_table_map,
    const std::unordered_set<std::string>& ambiguous_colum_name,
    const std::unordered_set<std::string>& relations) {
  if (!exp) {
    return Status::kSuccess;
  }
  if (exp->Type() == TypeTag::kColumnValue) {
    auto& cv = exp->AsColumnValue();
    const ColumnName& col_name = cv.GetColumnName();
    // Qualified references must name a FROM relation: its alias when one is
    // given, else the table name. The qualifier is kept verbatim so scan
    // implementations can rename their output schemas to match it
    // (Phase 8 aliases/self-joins).
    if (!col_name.schema.empty()) {
      if (!relations.contains(col_name.schema)) {
        return Status::kNotExists;
      }
      return Status::kSuccess;
    }
    if (ambiguous_colum_name.contains(ToLowerCopy(col_name.name))) {
      return Status::kAmbiguousQuery;
    }
    const auto it = col_table_map.find(ToLowerCopy(col_name.name));
    if (it == col_table_map.end()) {
      return Status::kNotExists;
    }
    cv.SetSchemaName(it->second);
    return Status::kSuccess;
  }
  if (exp->Type() == TypeTag::kBinaryExp) {
    Expression left = exp->AsBinaryExpression().Left();
    Expression right = exp->AsBinaryExpression().Right();
    RETURN_IF_FAIL(ResolveExpression(left, col_table_map, ambiguous_colum_name,
                                     relations));
    RETURN_IF_FAIL(ResolveExpression(right, col_table_map, ambiguous_colum_name,
                                     relations));
  } else if (exp->Type() == TypeTag::kUnaryExp) {
    Expression child = exp->AsUnaryExpression().Child();
    RETURN_IF_FAIL(ResolveExpression(child, col_table_map, ambiguous_colum_name,
                                     relations));
  } else if (exp->Type() == TypeTag::kAggregateExp) {
    Expression child = exp->AsAggregateExpression().Child();
    if (child->Type() == TypeTag::kColumnValue &&
        child->AsColumnValue().GetColumnName().name == "*") {
      return Status::kSuccess;
    }
    RETURN_IF_FAIL(ResolveExpression(child, col_table_map, ambiguous_colum_name,
                                     relations));
  } else if (exp->Type() == TypeTag::kCaseExp) {
    const auto& case_expression = exp->AsCaseExpression();
    for (const auto& clause : case_expression.when_clauses_) {
      Expression condition = clause.first;
      Expression value = clause.second;
      RETURN_IF_FAIL(ResolveExpression(condition, col_table_map,
                                       ambiguous_colum_name, relations));
      RETURN_IF_FAIL(ResolveExpression(value, col_table_map,
                                       ambiguous_colum_name, relations));
    }
    Expression otherwise = case_expression.else_clause_;
    RETURN_IF_FAIL(ResolveExpression(otherwise, col_table_map,
                                     ambiguous_colum_name, relations));
  } else if (exp->Type() == TypeTag::kInExp) {
    const auto& in = exp->AsInExpression();
    Expression child = in.child_;
    RETURN_IF_FAIL(ResolveExpression(child, col_table_map, ambiguous_colum_name,
                                     relations));
    for (Expression item : in.list_) {
      RETURN_IF_FAIL(ResolveExpression(item, col_table_map,
                                       ambiguous_colum_name, relations));
    }
  } else if (exp->Type() == TypeTag::kFunctionCallExp) {
    for (Expression argument : exp->AsFunctionCallExpression().Args()) {
      RETURN_IF_FAIL(ResolveExpression(argument, col_table_map,
                                       ambiguous_colum_name, relations));
    }
  }
  return Status::kSuccess;
}

Status ResolveSelect(
    std::vector<NamedExpression>& select,
    const std::unordered_map<std::string, std::string>& col_table_map,
    const std::unordered_set<std::string>& ambiguous_colum_name,
    const std::vector<ColumnName>& all_cols,
    const std::unordered_set<std::string>& relations) {
  for (auto it = select.begin(); it != select.end();) {
    if (it->expression->Type() == TypeTag::kColumnValue) {
      auto& cv = it->expression->AsColumnValue();
      const ColumnName& col_name = cv.GetColumnName();
      if (col_name.name == "*") {
        it = select.erase(it);
        for (const auto& cols : all_cols) {
          if (ambiguous_colum_name.contains(ToLowerCopy(cols.name))) {
            it = select.insert(it, NamedExpression(cols));
          } else {
            it = select.insert(it, NamedExpression(cols.name, cols));
          }
          ++it;
        }
        continue;
      }
      if (!col_name.schema.empty()) {
        Expression expression = it->expression;
        RETURN_IF_FAIL(ResolveExpression(expression, col_table_map,
                                         ambiguous_colum_name, relations));
        ++it;
        continue;
      }
      const auto col_it = col_table_map.find(ToLowerCopy(col_name.name));
      if (col_it == col_table_map.end()) {
        return Status::kNotExists;
      }
      cv.SetSchemaName(col_it->second);
      ++it;
    } else {
      Expression expression = it->expression;
      RETURN_IF_FAIL(ResolveExpression(expression, col_table_map,
                                       ambiguous_colum_name, relations));
      ++it;
    }
  }
  return Status::kSuccess;
}

}  // namespace

Status QueryData::Rewrite(TransactionContext& ctx) {
  // from_ entries are relation identities: the alias when one is given, else
  // the table name (Phase 8). Column qualifiers are validated against these
  // relations and kept verbatim; unqualified columns are attributed to their
  // owning relation. Scan implementations rename their output schemas to the
  // relation identity so every downstream lookup resolves uniformly.
  const std::unordered_set<std::string> relations(from_.begin(), from_.end());
  std::unordered_map<std::string, std::string> col_table_map;
  std::unordered_set<std::string> ambiguous_colum_name;
  std::vector<ColumnName> all_cols;

  for (const auto& relation : from_) {
    const auto aliased = aliases_.find(relation);
    const std::string& physical =
        aliased == aliases_.end() ? relation : aliased->second;
    ASSIGN_OR_RETURN(std::shared_ptr<Table>, from_table,
                     ctx.GetTable(physical));
    const Schema& sc = from_table->GetSchema();
    for (size_t i = 0; i < sc.ColumnCount(); ++i) {
      const ColumnName& col_name = sc.GetColumn(i).Name();
      all_cols.emplace_back(relation, col_name.name);
      const std::string lower_name = ToLowerCopy(col_name.name);
      if (!col_table_map.contains(lower_name)) {
        col_table_map.emplace(lower_name, relation);
      } else {
        ambiguous_colum_name.emplace(lower_name);
      }
    }
  }

  // Rewrite SELECT clause.
  for (auto& named : select_) {
    if (named.expression) {
      named.expression = BindColumnQualifiedFieldReads(
          named.expression, col_table_map, relations);
    }
  }
  where_ = where_ ? BindColumnQualifiedFieldReads(where_, col_table_map,
                                                  relations)
                  : where_;
  RETURN_IF_FAIL(ResolveSelect(select_, col_table_map, ambiguous_colum_name,
                               all_cols, relations));

  // Rewrite WHERE clause.
  return ResolveExpression(where_, col_table_map, ambiguous_colum_name,
                           relations);
}

}  // namespace tinylamb
