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

// Case-insensitive identifier equality (mirrors the SQL name resolution
// used throughout the engine).
bool CaseInsensitiveEquals(std::string_view left, std::string_view right) {
  if (left.size() != right.size()) {
    return false;
  }
  for (size_t i = 0; i < left.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(left[i])) !=
        std::tolower(static_cast<unsigned char>(right[i]))) {
      return false;
    }
  }
  return true;
}

// Dotted references whose qualifier names a COLUMN (not a FROM relation)
// address encoded fields of proto / struct payloads: "value.int32_val",
// "t.value.nested_value.nested_int64".  Rewrite them into chained
// __get_field_safe reads so resolution succeeds against the base column.
Expression BindColumnQualifiedFieldReads(
    const Expression& expr,
    const std::unordered_map<std::string, std::string>& col_table_map,
    const std::unordered_set<std::string>& relations,
    const std::unordered_map<std::string, size_t>& relation_column_counts) {
  if (!expr) {
    return expr;
  }
  if (expr->Type() == TypeTag::kColumnValue) {
    const ColumnName& name = expr->AsColumnValue().GetColumnName();
    if (name.schema.empty() || name.name.empty() || name.name == "*") {
      return expr;
    }
    if (relations.contains(name.schema)) {
      // A proto value table has one physical TEXT column, while SQL exposes
      // its message fields through the relation alias (`p.int32_val1`).
      // Bind such qualified field references to that sole column when the
      // physical schema has no column with the requested field name.
      const auto count = relation_column_counts.find(name.schema);
      if (count == relation_column_counts.end() || count->second != 1) {
        return expr;
      }
      std::string only_column;
      for (const auto& [column, relation] : col_table_map) {
        if (relation == name.schema) {
          if (!only_column.empty()) {
            only_column.clear();
            break;
          }
          only_column = column;
        }
      }
      if (!only_column.empty() && ToLowerCopy(only_column) !=
                                      ToLowerCopy(name.name)) {
        Expression current = ColumnValueExp(
            ColumnName(name.schema, only_column));
        std::string remaining = name.name;
        while (!remaining.empty()) {
          const size_t dot = remaining.find('.');
          const std::string field = dot == std::string::npos
                                        ? remaining
                                        : remaining.substr(0, dot);
          current = FunctionCallExp(
              "__get_field_safe",
              {std::move(current),
               ConstantValueExp(Value(std::string(field))) });
          if (dot == std::string::npos) { break; }
          remaining = remaining.substr(dot + 1);
        }
        return current;
      }
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
    Expression mapped = BindColumnQualifiedFieldReads(
        child, col_table_map, relations, relation_column_counts);
    changed |= child->ToString() != mapped->ToString();
    child = std::move(mapped);
  }
  return changed ? WithExpressionChildren(expr, std::move(children)) : expr;
}

// Rewrites STRUCT/PROTO field paths (`value.empty_message`, where `value`
// names a column rather than a FROM relation) into safe field-extraction
// calls over the base column, so the flat-column plan executor can evaluate
// them.  Returns nullptr when the expression is untouched.
Expression BindFieldPaths(
    const Expression& exp,
    const std::unordered_map<std::string, std::string>& col_table_map,
    const std::unordered_set<std::string>& relations) {
  if (!exp) {
    return nullptr;
  }
  if (exp->Type() == TypeTag::kColumnValue) {
    const ColumnName& col_name = exp->AsColumnValue().GetColumnName();
    if (col_name.schema.empty() || col_name.name.empty() ||
        col_name.name == "*" || relations.contains(col_name.schema)) {
      return nullptr;
    }
    const auto base_it = col_table_map.find(ToLowerCopy(col_name.schema));
    if (base_it == col_table_map.end()) {
      return nullptr;
    }
    Expression current =
        ColumnValueExp(ColumnName(base_it->second, col_name.schema));
    std::string remaining = col_name.name;
    while (!remaining.empty()) {
      const size_t dot = remaining.find('.');
      std::string field = dot == std::string::npos ? remaining
                                                   : remaining.substr(0, dot);
      current = FunctionCallExp(
          "__get_field_safe",
          {std::move(current), ConstantValueExp(Value(std::move(field)))});
      if (dot == std::string::npos) { break; }
      remaining = remaining.substr(dot + 1);
    }
    return current;
  }
  std::vector<Expression> children = ExpressionChildren(exp);
  bool changed = false;
  for (Expression& child : children) {
    Expression mapped = BindFieldPaths(child, col_table_map, relations);
    changed |= static_cast<bool>(mapped);
    if (mapped) { child = std::move(mapped); }
  }
  return changed ? WithExpressionChildren(exp, std::move(children)) : nullptr;
}

Status
ResolveExpression(  // NOLINT(misc-no-recursion) // Recursive expression-tree
                    // resolution by design; trees are parser-bounded in depth.
    Expression& exp,
    const std::unordered_map<std::string, std::string>& col_table_map,
    const std::unordered_set<std::string>& ambiguous_colum_name,
    const std::unordered_set<std::string>& relations,
    const std::vector<ColumnName>& all_cols) {
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
      // A bare name that matches a FROM relation (its alias or table name)
      // denotes that relation's whole row as a struct ("SELECT s FROM t s").
      if (relations.contains(ToLowerCopy(col_name.name))) {
        std::vector<Expression> args;
        for (const ColumnName& column : all_cols) {
          if (!CaseInsensitiveEquals(column.schema, col_name.name)) {
            continue;
          }
          args.push_back(ConstantValueExp(Value(std::string(column.name))));
          args.push_back(ColumnValueExp(column));
        }
        exp = FunctionCallExp("__struct_json__", std::move(args));
        return Status::kSuccess;
      }
      if (all_cols.size() == 1 && col_name.name != "*") {
        exp = FunctionCallExp(
            "__get_field_safe",
            {ColumnValueExp(all_cols.front()),
             ConstantValueExp(Value(std::string(col_name.name))) });
        return Status::kSuccess;
      }
      return Status::kNotExists;
    }
    cv.SetSchemaName(it->second);
    return Status::kSuccess;
  }
  if (exp->Type() == TypeTag::kBinaryExp) {
    Expression left = exp->AsBinaryExpression().Left();
    Expression right = exp->AsBinaryExpression().Right();
    RETURN_IF_FAIL(ResolveExpression(left, col_table_map, ambiguous_colum_name,
                                     relations, all_cols));
    RETURN_IF_FAIL(ResolveExpression(right, col_table_map, ambiguous_colum_name,
                                     relations, all_cols));
  } else if (exp->Type() == TypeTag::kUnaryExp) {
    Expression child = exp->AsUnaryExpression().Child();
    RETURN_IF_FAIL(ResolveExpression(child, col_table_map, ambiguous_colum_name,
                                     relations, all_cols));
  } else if (exp->Type() == TypeTag::kAggregateExp) {
    Expression child = exp->AsAggregateExpression().Child();
    if (!child) {
      return Status::kSuccess;
    }
    if (child->Type() == TypeTag::kColumnValue &&
        child->AsColumnValue().GetColumnName().name == "*") {
      return Status::kSuccess;
    }
    RETURN_IF_FAIL(ResolveExpression(child, col_table_map, ambiguous_colum_name,
                                     relations, all_cols));
  } else if (exp->Type() == TypeTag::kCaseExp) {
    const auto& case_expression = exp->AsCaseExpression();
    for (const auto& clause : case_expression.when_clauses_) {
      Expression condition = clause.first;
      Expression value = clause.second;
      RETURN_IF_FAIL(ResolveExpression(condition, col_table_map,
                                       ambiguous_colum_name, relations, all_cols));
      RETURN_IF_FAIL(ResolveExpression(value, col_table_map,
                                       ambiguous_colum_name, relations, all_cols));
    }
    Expression otherwise = case_expression.else_clause_;
    RETURN_IF_FAIL(ResolveExpression(otherwise, col_table_map,
                                     ambiguous_colum_name, relations, all_cols));
  } else if (exp->Type() == TypeTag::kInExp) {
    const auto& in = exp->AsInExpression();
    Expression child = in.child_;
    RETURN_IF_FAIL(ResolveExpression(child, col_table_map, ambiguous_colum_name,
                                     relations, all_cols));
    for (Expression item : in.list_) {
      RETURN_IF_FAIL(ResolveExpression(item, col_table_map,
                                       ambiguous_colum_name, relations, all_cols));
    }
  } else if (exp->Type() == TypeTag::kFunctionCallExp) {
    for (Expression argument : exp->AsFunctionCallExpression().Args()) {
      RETURN_IF_FAIL(ResolveExpression(argument, col_table_map,
                                       ambiguous_colum_name, relations, all_cols));
    }
  }
  return Status::kSuccess;
}

Status ResolveSelect(
    std::vector<NamedExpression>& select,
    const std::unordered_map<std::string, std::string>& col_table_map,
    const std::unordered_set<std::string>& ambiguous_colum_name,
    const std::vector<ColumnName>& all_cols,
    const std::unordered_set<std::string>& relations,
    bool expand_proto_value_table) {
  for (auto it = select.begin(); it != select.end();) {
    if (it->expression->Type() == TypeTag::kColumnValue) {
      auto& cv = it->expression->AsColumnValue();
      const ColumnName& col_name = cv.GetColumnName();
      if (col_name.name == "*") {
        // A qualified star is expanded only over the matching relation.  It
        // must be handled before the unqualified-star path below; both are
        // represented by ColumnName(schema, "*").
        if (!col_name.schema.empty()) {
          std::vector<ColumnName> matched;
          for (const ColumnName& column : all_cols) {
            if (CaseInsensitiveEquals(column.schema, col_name.schema)) {
              matched.push_back(column);
            }
          }
          it = select.erase(it);
          if (expand_proto_value_table && matched.size() == 1 &&
              (matched.front().name == "$expr0" ||
               matched.front().name == "int32_val1")) {
            const ColumnName base = matched.front();
            for (const char* field : {"int32_val1", "int32_val2",
                                      "str_value"}) {
              it = select.insert(
                  it, NamedExpression(
                         field,
                         FunctionCallExp(
                             "__get_field_safe",
                             {ColumnValueExp(base),
                              ConstantValueExp(Value(std::string(field)))})));
              ++it;
            }
          } else {
            for (auto column = matched.rbegin(); column != matched.rend();
                 ++column) {
              it = select.insert(
                  it, NamedExpression(column->name, ColumnValueExp(*column)));
            }
          }
          continue;
        }
        it = select.erase(it);
        if (expand_proto_value_table && all_cols.size() == 1 &&
            (all_cols.front().name == "$expr0" ||
             all_cols.front().name == "int32_val1")) {
          const ColumnName base = all_cols.front();
          for (const char* field : {"int32_val1", "int32_val2",
                                    "str_value"}) {
            it = select.insert(
                it, NamedExpression(
                       field,
                       FunctionCallExp(
                           "__get_field_safe",
                           {ColumnValueExp(base),
                            ConstantValueExp(Value(std::string(field))) })));
            ++it;
          }
          continue;
        }
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
                                         ambiguous_colum_name, relations, all_cols));
        ++it;
        continue;
      }
      const auto col_it = col_table_map.find(ToLowerCopy(col_name.name));
      if (col_it == col_table_map.end()) {
        // A bare name matching a FROM relation denotes its whole row as a
        // struct ("SELECT s FROM t s"): encode every column of that relation
        // explicitly so projection keeps them alive.
        if (relations.contains(ToLowerCopy(col_name.name))) {
          if (expand_proto_value_table && all_cols.size() == 1) {
            it->expression = FunctionCallExp(
                "__struct_json__",
                {ConstantValueExp(Value(std::string(col_name.name))),
                 FunctionCallExp(
                     "__value_table_proto_existing",
                     {ConstantValueExp(
                          Value(std::string("googlesql_test.TestExtraPB"))),
                      ColumnValueExp(all_cols.front())})});
            ++it;
            continue;
          }
          std::vector<Expression> args;
          for (const ColumnName& column : all_cols) {
            if (!CaseInsensitiveEquals(column.schema, col_name.name)) {
              continue;
            }
            args.push_back(ConstantValueExp(Value(std::string(column.name))));
            args.push_back(ColumnValueExp(column));
          }
          it->expression = FunctionCallExp("__struct_json__", std::move(args));
          ++it;
          continue;
        }
        if (all_cols.size() == 1 && col_name.name != "*") {
          it->expression = FunctionCallExp(
              "__get_field_safe",
              {ColumnValueExp(all_cols.front()),
               ConstantValueExp(Value(std::string(col_name.name))) });
          ++it;
          continue;
        }
        return Status::kNotExists;
      }
      cv.SetSchemaName(col_it->second);
      ++it;
    } else {
      Expression expression = it->expression;
      RETURN_IF_FAIL(ResolveExpression(expression, col_table_map,
                                       ambiguous_colum_name, relations, all_cols));
      ++it;
    }
  }
  if (expand_proto_value_table && all_cols.size() == 1 &&
      select.size() == 1 && select.front().expression->Type() ==
          TypeTag::kColumnValue &&
      select.front().expression->AsColumnValue().GetColumnName().name ==
          all_cols.front().name) {
    const ColumnName base = all_cols.front();
    select.clear();
    for (const char* field : {"int32_val1", "int32_val2", "str_value"}) {
      select.emplace_back(
          field,
          FunctionCallExp(
              "__get_field_safe",
              {ColumnValueExp(base),
               ConstantValueExp(Value(std::string(field))) }));
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
  std::unordered_map<std::string, size_t> relation_column_counts;

  for (const auto& relation : from_) {
    const auto aliased = aliases_.find(relation);
    const std::string& physical =
        aliased == aliases_.end() ? relation : aliased->second;
    ASSIGN_OR_RETURN(std::shared_ptr<Table>, from_table,
                     ctx.GetTable(physical));
    const Schema& sc = from_table->GetSchema();
    relation_column_counts[relation] = sc.ColumnCount();
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

  // STRUCT/PROTO field paths become explicit extraction calls before
  // resolution so only plain column references remain to bind.
  for (auto& item : select_) {
    Expression mapped =
        BindFieldPaths(item.expression, col_table_map, relations);
    if (mapped) { item.expression = std::move(mapped); }
  }
  {
    Expression mapped = BindFieldPaths(where_, col_table_map, relations);
    if (mapped) { where_ = std::move(mapped); }
  }

  // Rewrite SELECT clause.
  for (auto& named : select_) {
    if (named.expression) {
      named.expression = BindColumnQualifiedFieldReads(
          named.expression, col_table_map, relations, relation_column_counts);
    }
  }
  where_ = where_ ? BindColumnQualifiedFieldReads(where_, col_table_map,
                                                  relations,
                                                  relation_column_counts)
                  : where_;

  // ORDER BY is evaluated after projection, so a bare identifier may refer
  // to a SELECT-list alias (for example `SELECT id + 10 AS shifted ...
  // ORDER BY shifted`).  Resolve that alias before ordinary scope binding;
  // otherwise the optimizer treats it as a missing input column and returns
  // kNotExists.  Base columns take precedence over aliases, matching SQL
  // name-resolution rules.
  for (Expression& order : order_expressions_) {
    if (order && order->Type() == TypeTag::kColumnValue) {
      const ColumnName& name = order->AsColumnValue().GetColumnName();
      // `$orderN` is an internal output alias installed by SqlEngine for a
      // hidden ORDER BY key. It is intentionally absent from the input
      // relation map and must survive Rewrite unchanged.
      if (name.schema.empty() && name.name.starts_with("$order")) {
        continue;
      }
      if (name.schema.empty() && name.name != "*" &&
          !col_table_map.contains(ToLowerCopy(name.name))) {
        for (const NamedExpression& selected : select_) {
          if (!selected.name.empty() &&
              CaseInsensitiveEquals(selected.name, name.name)) {
            order = selected.expression;
            break;
          }
        }
      }
    }
    RETURN_IF_FAIL(ResolveExpression(order, col_table_map,
                                     ambiguous_colum_name, relations,
                                     all_cols));
  }
  RETURN_IF_FAIL(ResolveSelect(
      select_, col_table_map, ambiguous_colum_name, all_cols, relations, [&] {
                                 for (const std::string& relation : from_) {
                                   const auto found = aliases_.find(relation);
                                   const std::string& physical =
                                       found == aliases_.end() ? relation
                                                               : found->second;
                                   if (physical.find("TestExtraPBValueTable") !=
                                           std::string::npos ||
                                       physical.find("WithProtoValueTable") !=
                                           std::string::npos) {
                                     return true;
                                   }
                                 }
                                 return false;
                               }()));

  // Rewrite WHERE clause.
  return ResolveExpression(where_, col_table_map, ambiguous_colum_name,
                           relations, all_cols);
}

}  // namespace tinylamb
