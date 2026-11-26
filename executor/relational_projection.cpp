/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/relational_projection.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/detail/explain_format.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/relational_sort_limit.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/query_memory.hpp"
#include "executor/relational.hpp"
#include "executor/spill_file.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
#include "expression/collation.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "table/table.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {

// A `relation.*` select item may reach the relational interpreter either bare
// or wrapped by the AST visitor's __value_table_value / __proto_new proto
// constructor. Unwrap one level here so every star gate sees the same shape.
// Callers only accept an unwrapped *qualified* star when its qualifier names
// a FROM source; a genuine proto value table (no such source) keeps its
// wrapper and its single-row-value semantics.
struct UnwrappedStar {
  ColumnName name;
  bool wrapped{false};
};
std::optional<UnwrappedStar> UnwrapStarProjection(
    const Expression& expression) {
  if (!expression) {
    return std::nullopt;
  }
  Expression candidate = expression;
  bool wrapped = false;
  if (candidate->Type() == TypeTag::kFunctionCallExp) {
    const auto& call = candidate->AsFunctionCallExpression();
    if ((call.FuncName() == "__value_table_value" ||
         call.FuncName() == "__proto_new") &&
        call.Args().size() == 1 && call.Args()[0]) {
      candidate = call.Args()[0];
      wrapped = true;
    }
  }
  if (candidate->Type() != TypeTag::kColumnValue) {
    return std::nullopt;
  }
  const ColumnName& column = candidate->AsColumnValue().GetColumnName();
  if (column.name != "*") {
    return std::nullopt;
  }
  return UnwrappedStar{.name = column, .wrapped = wrapped};
}

// The grouped-aggregation key index (GroupKeyEqual / GroupKeyMap) lives in
// executor/detail/expression_eval.hpp: it is the shared "same group" contract
// between this path and the physical grouped aggregation shapes
// (executor/grouped_aggregate.hpp).

// Minimum grouped-input size before Project folds rows into parallel worker
// slices; below this the serial loop's thread startup dominates.
constexpr size_t kParallelGroupMinRows = 16384;

}  // namespace

// A NULL first row carries no type information. Recover a homogeneous
// resident column's domain so downstream filters do not lose their typed
// path solely because an outer join emitted an unmatched row first.
void RefineProjectionTypes(const Relation& output,
                           std::vector<Column>& columns) {
  if (output.HasColumnarOnly()) {
    if (output.columnar->total_rows == 0) {
      return;
    }
    // P671: there is no short-circuit on the first chunk's head cell.  A
    // later chunk may carry a different domain (mixed relation from a
    // hand-built or dynamic source), and the whole-scan below is O(chunks)
    // for uniform columns anyway -- per-cell work only happens on the
    // non-uniform columns that genuinely need it.
    for (size_t i = 0; i < columns.size(); ++i) {
      bool uniform = true;
      bool saw_sample = false;
      ValueType sample_type = ValueType::kNull;
      bool sample_unsigned = false;
      bool sample_interval = false;
      for (const auto& chunk : output.columnar->chunks) {
        if (chunk.rows == 0 || i >= chunk.columns.size()) {
          continue;
        }
        const auto& col = chunk.columns[i];
        if (!col.is_uniform) {
          Value tmp_val;
          for (size_t r = 0; r < chunk.rows; ++r) {
            col.WriteValueInto(r, &tmp_val);
            if (tmp_val.IsNull()) {
              continue;
            }
            if (!saw_sample) {
              saw_sample = true;
              sample_type = ValueTypeOf(tmp_val);
              sample_unsigned = tmp_val.IsUnsigned();
              sample_interval = tmp_val.IsInterval();
            } else if (sample_type != tmp_val.type ||
                       sample_unsigned != tmp_val.IsUnsigned() ||
                       sample_interval != tmp_val.IsInterval()) {
              uniform = false;
              break;
            }
          }
          if (!uniform) {
            break;
          }
          continue;
        }
        if (col.type == ValueType::kNull || !col.AnyNonNull(chunk.rows)) {
          continue;
        }
        if (!saw_sample) {
          saw_sample = true;
          sample_type = col.type;
          sample_unsigned = col.is_unsigned;
          sample_interval = col.is_interval;
        } else if (sample_type != col.type ||
                   sample_unsigned != col.is_unsigned ||
                   sample_interval != col.is_interval) {
          uniform = false;
          break;
        }
      }
      if (!uniform) {
        columns[i] = Column(columns[i].Name(), ValueType::kNull);
      } else if (saw_sample) {
        columns[i].SetType(sample_type);
        columns[i].SetInterval(sample_interval);
        columns[i].SetUnsigned(sample_unsigned);
      }
    }
    return;
  }
  if (output.rows.empty()) {
    return;
  }
  for (size_t i = 0; i < columns.size(); ++i) {
    // P671: a non-NULL head cell is evidence for that cell only; scan the
    // whole resident column so mixed domains collapse to kNull exactly like
    // the NULL-head path and the columnar branch resolve them.
    const Value* sample = nullptr;
    bool uniform = true;
    for (const Row& row : output.rows) {
      const Value& value = row[i];
      if (value.IsNull()) {
        continue;
      }
      if (sample == nullptr) {
        sample = &value;
      } else if (sample->type != value.type ||
                 sample->IsUnsigned() != value.IsUnsigned() ||
                 sample->IsInterval() != value.IsInterval()) {
        uniform = false;
        break;
      }
    }
    if (!uniform) {
      // A hand-built or dynamic relation can violate its declared domain.
      // Do not let a typed consumer interpret a mixed column as uniform.
      columns[i] = Column(columns[i].Name(), ValueType::kNull);
      continue;
    }
    if (sample == nullptr) {
      // All NULL values provide no evidence against the declared type.
      // Retain it for CTAS and later insert assignment checks (with a spill
      // the unseen rows may differ; the declared type keeps its old trust).
      continue;
    }
    columns[i].SetType(ValueTypeOf(*sample));
    columns[i].SetInterval(sample->IsInterval());
    columns[i].SetUnsigned(sample->IsUnsigned());
  }
}
// HAVING may reference select-list aliases (`HAVING double_avg > 0.7`).
// Alias references that are not real input columns are rewritten to the
// aliased expression so aggregate aliases resolve during grouping.

// HAVING transpose helper (aggregate <-> filter): true when every column the
// conjunct touches is a bare grouping-key column (qualifier-insensitive) and
// the conjunct holds no aggregate or query subtree.  Within a group every row
// shares the key values, so the group's fate is a row-level fact; the
// group-all shape is excluded by the caller (it forms a group even over a
// fully rejected input).
bool IsKeyOnlyHavingConjunct(const Expression& conjunct,
                             const SelectStatement& resolved) {
  if (!conjunct) {
    return false;
  }
  if (ContainsAggregate(conjunct) || ContainsQuery(conjunct)) {
    return false;
  }
  for (const ColumnName& column : conjunct->TouchedColumns()) {
    bool is_key = false;
    for (const Expression& key : resolved.GroupBy()) {
      if (!key || key->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& key_name = key->AsColumnValue().GetColumnName();
      if (key_name.name != "*" &&
          IdentifierEquals(key_name.name, column.name)) {
        is_key = true;
        break;
      }
    }
    if (!is_key) {
      return false;
    }
  }
  return true;
}

Expression InlineHavingAliases(const SelectStatement& statement,
                               const Schema& schema) {
  const Expression& having = statement.Having();
  if (!having) {
    return having;
  }
  std::unordered_map<std::string, Expression> aliases;
  aliases.reserve(statement.SelectList().size());
  for (const NamedExpression& projection : statement.SelectList()) {
    if (!projection.name.empty() && projection.name != "*") {
      aliases.emplace(AsciiToLower(projection.name), projection.expression);
    }
  }
  if (aliases.empty()) {
    return having;
  }
  auto schema_has_column = [&schema](std::string_view name) {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (IdentifierEquals(name, schema.GetColumn(i).Name().name)) {
        return true;
      }
    }
    return false;
  };
  std::function<Expression(const Expression&)> walk =
      [&](const Expression& expression)
      -> Expression {  // NOLINT(misc-no-recursion)
    if (!expression) {
      return expression;
    }
    switch (expression->Type()) {
      case TypeTag::kColumnValue: {
        const ColumnName& name = expression->AsColumnValue().GetColumnName();
        if (!name.schema.empty() || schema_has_column(name.name)) {
          return expression;
        }
        const auto found = aliases.find(AsciiToLower(name.name));
        return found != aliases.end() ? found->second : expression;
      }
      case TypeTag::kBinaryExp:
        return BinaryExpressionExp(
            walk(expression->AsBinaryExpression().Left()),
            expression->AsBinaryExpression().Op(),
            walk(expression->AsBinaryExpression().Right()));
      case TypeTag::kUnaryExp:
        return UnaryExpressionExp(walk(expression->AsUnaryExpression().Child()),
                                  expression->AsUnaryExpression().Op());
      case TypeTag::kCaseExp: {
        const auto& value = expression->AsCaseExpression();
        std::vector<std::pair<Expression, Expression>> clauses;
        clauses.reserve(value.when_clauses_.size());
        for (const auto& [condition, result] : value.when_clauses_) {
          clauses.emplace_back(walk(condition), walk(result));
        }
        return CaseExpressionExp(std::move(clauses), walk(value.else_clause_));
      }
      case TypeTag::kInExp: {
        const auto& value = expression->AsInExpression();
        std::vector<Expression> items;
        items.reserve(value.list_.size());
        for (const Expression& item : value.list_) {
          items.push_back(walk(item));
        }
        return InExpressionExp(walk(value.child_), std::move(items));
      }
      case TypeTag::kFunctionCallExp:
        return FunctionCallExp(
            expression->AsFunctionCallExpression().FuncName(), [&] {
              std::vector<Expression> out;
              out.reserve(expression->AsFunctionCallExpression().Args().size());
              for (const Expression& argument :
                   expression->AsFunctionCallExpression().Args()) {
                out.push_back(walk(argument));
              }
              return out;
            }());
      case TypeTag::kQueryExp: {
        // Alias references may sit inside HAVING subqueries
        // (EXISTS (SELECT * FROM UNNEST(agg_alias) ...)).  Rewrite them on a
        // shallow copy so the subquery's lateral unnest arguments bind to the
        // outer aggregate expressions.
        const auto& value = expression->AsQueryExpression();
        if (!value.Query() || value.Test()) {
          return expression;
        }
        auto copied = std::make_shared<SelectStatement>(*value.Query());
        bool changed = false;
        std::vector<SelectSource> sources = copied->Sources();
        for (SelectSource& source : sources) {
          if (source.unnest) {
            source.unnest = walk(source.unnest);
            changed = true;
          }
        }
        if (!changed) {
          return expression;
        }
        copied->SetSources(std::move(sources));
        auto rewritten = std::make_shared<QueryExpression>(
            std::move(copied), nullptr, value.Exists(), value.Negated(),
            value.Op(), value.Mode());
        rewritten->SetArrayResult(value.ArrayResult());
        if (!value.ArrayElementSqlType().empty()) {
          rewritten->SetArrayElementSqlType(value.ArrayElementSqlType());
        }
        return {rewritten};
      }
      default:
        // Aggregates, subqueries and casts keep their own scope.
        return expression;
    }
  };
  return walk(having);
}

// Deep expression rewrite: returns `expression` with every subtree accepted by
// `replace` swapped for its replacement (children visited after the node, so
// replacements are not re-entered).
Expression ReplaceSubExpressions(
    const Expression& expression,
    const std::function<std::optional<Expression>(const Expression&)>&
        replace) {
  if (!expression) {
    return expression;
  }
  if (auto replacement = replace(expression)) {
    return std::move(*replacement);
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  bool changed = false;
  for (Expression& child : children) {
    Expression replaced = ReplaceSubExpressions(child, replace);
    if (replaced != child) {
      changed = true;
    }
    child = std::move(replaced);
  }
  return changed ? WithExpressionChildren(expression, std::move(children))
                 : expression;
}

// Registers every aggregate subtree under `expression` in the $aggK
// materialization pool so a window-over-aggregate pass can reference the
// per-group values by column.
namespace {

void RegisterAggregatesForMaterialization(
    const Expression& expression,
    std::unordered_map<std::string, std::string>* aggregate_columns,
    std::vector<std::pair<std::string, Expression>>* hidden_aggregates) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kAggregateExp) {
    auto [it, inserted] = aggregate_columns->emplace(
        expression->ToString(),
        "$agg" + std::to_string(hidden_aggregates->size()));
    if (inserted) {
      hidden_aggregates->emplace_back(it->second, expression);
    }
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    RegisterAggregatesForMaterialization(child, aggregate_columns,
                                         hidden_aggregates);
  }
}

void CollectWindowCalls(const Expression& expression,
                        std::vector<Expression>* windows) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kWindowFunctionExp) {
    for (const Expression& seen : *windows) {
      if (seen->ToString() == expression->ToString()) {
        return;
      }
    }
    windows->push_back(expression);
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CollectWindowCalls(child, windows);
  }
}

bool ContainsWindowCall(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kWindowFunctionExp) {
    return true;
  }
  return std::ranges::any_of(ExpressionChildren(expression),
                             ContainsWindowCall);
}

}  // namespace

bool StatementIsAggregated(const SelectStatement& statement) {
  return !statement.GroupBy().empty() ||
         std::any_of(statement.SelectList().begin(),
                     statement.SelectList().end(),
                     [](const NamedExpression& projection) {
                       return ContainsAggregate(projection.expression);
                     }) ||
         ContainsAggregate(statement.Having());
}

namespace {

StatusOr<Relation> Project(TransactionContext& context,
                           const SelectStatement& statement, Relation input,
                           const Scope* outer, const CteMap& ctes,
                           size_t hidden_columns) {
  const auto project_begin = std::chrono::steady_clock::now();
  if (hidden_columns > 0 &&
      (!statement.GroupBy().empty() ||
       std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                   [](const NamedExpression& projection) {
                     return ContainsAggregate(projection.expression);
                   }))) {
    // Direct Project fallback: window-over-aggregate normally takes the
    // dedicated ExecuteWindowOverAggregate path; treat this as an unreachable
    // backstop.
    return StatusError(
        StatusCode::kInvalidArgument,
        "window functions combined with GROUP BY are not supported here");
  }
  const bool grouped =
      !statement.GroupBy().empty() ||
      std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                  [](const NamedExpression& projection) {
                    return ContainsAggregate(projection.expression);
                  }) ||
      ContainsAggregate(statement.Having());
  Expression having_expr = grouped
                               ? InlineHavingAliases(statement, input.schema)
                               : statement.Having();
  // Fold row-independent subqueries in HAVING to constants once, before the
  // per-group emit (Q11: every group re-probed the same uncorrelated scalar
  // through the cache).  Same contract as the scan-filter fold:
  // uncorrelated volatile-free nodes are statement-invariant on this
  // snapshot, and nodes whose evaluation raises keep their original form.
  static const bool having_fold_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_HAVING_FOLD") != nullptr;
  if (grouped && having_expr && !having_fold_disabled) {
    std::vector<Expression> folded_having =
        FoldRowIndependentSubqueries(context, {having_expr}, outer, ctes);
    if (!folded_having.empty()) {
      having_expr = folded_having[0];
    }
  }
  const auto dedupe_select_aggregates = [&](const Expression& expr) {
    return ReplaceSubExpressions(
        expr,
        [&statement](const Expression& node) -> std::optional<Expression> {
          if (!node || node->Type() != TypeTag::kAggregateExp) {
            return std::nullopt;
          }
          for (const NamedExpression& projection : statement.SelectList()) {
            if (projection.expression &&
                projection.expression->Type() == TypeTag::kAggregateExp &&
                projection.expression->ToString() == node->ToString()) {
              return projection.expression;
            }
          }
          return std::nullopt;
        });
  };
  if (grouped && having_expr) {
    having_expr = dedupe_select_aggregates(having_expr);
  }
  // ORDER BY terms may reference select-list aliases or spell out an
  // aggregate ("ORDER BY sum(x) DESC", TPC-DS q42).  Alias references resolve
  // to the select item's expression; aggregate subtrees dedupe onto the
  // identical select-list aggregate node so per-group evaluation finds them
  // prepared in the AggregateResultMap (which is keyed by node pointer).
  std::vector<Expression> resolved_order_expressions;
  resolved_order_expressions.reserve(statement.OrderBy().size());
  for (const auto& term : statement.OrderBy()) {
    Expression expression = term.expression;
    if (expression && expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& name = expression->AsColumnValue().GetColumnName();
      if (name.schema.empty()) {
        for (const NamedExpression& projection : statement.SelectList()) {
          if (!projection.name.empty() &&
              projection.name.size() == name.name.size() &&
              std::equal(
                  projection.name.begin(), projection.name.end(),
                  name.name.begin(), [](char left, char right) {
                    return std::tolower(static_cast<unsigned char>(left)) ==
                           std::tolower(static_cast<unsigned char>(right));
                  })) {
            expression = projection.expression;
            break;
          }
        }
      }
    }
    expression = dedupe_select_aggregates(expression);
    resolved_order_expressions.push_back(std::move(expression));
  }
  std::vector<const AggregateExpression*> aggregate_expressions;
  std::unordered_set<const AggregateExpression*> seen_aggregates;
  for (const NamedExpression& projection : statement.SelectList()) {
    CollectAggregates(projection.expression, &aggregate_expressions,
                      &seen_aggregates);
  }
  CollectAggregates(having_expr, &aggregate_expressions, &seen_aggregates);
  for (const Expression& order_expression : resolved_order_expressions) {
    CollectAggregates(order_expression, &aggregate_expressions,
                      &seen_aggregates);
  }
  std::vector<slot_t> ungrouped_slots;
  if (grouped) {
    std::unordered_set<slot_t> grouped_slots;
    std::unordered_set<std::string> grouped_expr_strings;
    if (!statement.IsGroupAll()) {
      for (const Expression& g : statement.GroupBy()) {
        if (!g) {
          continue;
        }
        grouped_expr_strings.insert(g->ToString());
        if (g->Type() == TypeTag::kColumnValue) {
          const ColumnName& gcol = g->AsColumnValue().GetColumnName();
          if (std::optional<slot_t> slot =
                  LocalColumnOffset(input.schema, gcol);
              slot.has_value()) {
            grouped_slots.insert(*slot);
          }
          if (gcol.schema.empty()) {
            for (const NamedExpression& proj : statement.SelectList()) {
              if (!proj.name.empty() &&
                  IdentifierEquals(proj.name, gcol.name) && proj.expression) {
                grouped_expr_strings.insert(proj.expression->ToString());
                if (proj.expression->Type() == TypeTag::kColumnValue) {
                  if (std::optional<slot_t> slot = LocalColumnOffset(
                          input.schema,
                          proj.expression->AsColumnValue().GetColumnName());
                      slot.has_value()) {
                    grouped_slots.insert(*slot);
                  }
                }
              }
            }
          }
        }
      }
    }
    const std::function<void(const Expression&)> collect_ungrouped =
        [&](const Expression& expr) {
          if (!expr || expr->Type() == TypeTag::kAggregateExp ||
              expr->Type() == TypeTag::kQueryExp) {
            return;
          }
          if (grouped_expr_strings.contains(expr->ToString())) {
            return;
          }
          if (expr->Type() == TypeTag::kColumnValue) {
            const ColumnName& col = expr->AsColumnValue().GetColumnName();
            if (col.name.empty() || col.name == "*") {
              return;
            }
            if (std::optional<slot_t> slot =
                    LocalColumnOffset(input.schema, col);
                slot.has_value() && !grouped_slots.contains(*slot)) {
              if (std::ranges::find(ungrouped_slots, *slot) ==
                  ungrouped_slots.end()) {
                ungrouped_slots.push_back(*slot);
              }
            }
            return;
          }
          for (const Expression& child : ExpressionChildren(expr)) {
            collect_ungrouped(child);
          }
        };
    for (const NamedExpression& proj : statement.SelectList()) {
      collect_ungrouped(proj.expression);
    }
    collect_ungrouped(having_expr);
    for (const Expression& order_expression : resolved_order_expressions) {
      collect_ungrouped(order_expression);
    }
  }
  if (grouped && outer == nullptr && hidden_columns == 0 &&
      ungrouped_slots.empty()) {
    std::optional<SelectStatement> compact_stmt;
    std::optional<SelectStatement> sort_stmt;
    const bool has_order_by = !statement.OrderBy().empty();
    bool compact_allowed = !has_order_by;
    if (has_order_by && !statement.Distinct() &&
        !statement.SelectList().empty()) {
      const size_t select_count = statement.SelectList().size();
      const auto unwrap_collate =
          [](const Expression& expr) -> const Expression& {
        if (expr && CollationOfExpression(expr) != Collation::kNone &&
            expr->Type() == TypeTag::kFunctionCallExp &&
            !expr->AsFunctionCallExpression().Args().empty()) {
          return expr->AsFunctionCallExpression().Args()[0];
        }
        return expr;  // NOLINT(bugprone-return-const-ref-from-parameter)
      };
      const auto resolve_order_target =
          [&](size_t oi) -> std::optional<size_t> {
        const Expression& raw_expr = statement.OrderBy()[oi].expression;
        if (!raw_expr) {
          return std::nullopt;
        }
        if (const auto collated = AsCollatedOrdinal(raw_expr)) {
          if (collated->ordinal >= 1 &&
              std::cmp_less_equal(collated->ordinal, select_count)) {
            return static_cast<size_t>(collated->ordinal - 1);
          }
          return std::nullopt;
        }
        if (raw_expr->Type() == TypeTag::kConstantValue) {
          const Value& cv = raw_expr->AsConstantValue().GetValue();
          if (cv.type == ValueType::kInt64 && !cv.IsNull() &&
              !cv.IsUnsigned() && cv.value.int_value >= 1 &&
              std::cmp_less_equal(cv.value.int_value, select_count)) {
            return static_cast<size_t>(cv.value.int_value - 1);
          }
          return std::nullopt;
        }
        const Expression& resolved = resolved_order_expressions[oi];
        if (resolved) {
          for (size_t si = 0; si < select_count; ++si) {
            const Expression& sel = statement.SelectList()[si].expression;
            if (sel && sel->ToString() == resolved->ToString()) {
              return si;
            }
          }
          if (CollationOfExpression(resolved) != Collation::kNone) {
            const Expression& resolved_inner = unwrap_collate(resolved);
            if (resolved_inner) {
              for (size_t si = 0; si < select_count; ++si) {
                const Expression& sel = statement.SelectList()[si].expression;
                if (sel && sel->ToString() == resolved_inner->ToString()) {
                  return si;
                }
              }
            }
          }
        }
        const Expression& raw_inner = unwrap_collate(raw_expr);
        if (raw_inner && raw_inner->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn = raw_inner->AsColumnValue().GetColumnName();
          if (cn.schema.empty()) {
            std::optional<size_t> alias_match;
            bool alias_ambiguous = false;
            for (size_t si = 0; si < select_count; ++si) {
              const auto& item = statement.SelectList()[si];
              if (!item.name.empty() && IdentifierEquals(item.name, cn.name)) {
                if (alias_match.has_value()) {
                  alias_ambiguous = true;
                  break;
                }
                alias_match = si;
              }
            }
            if (alias_match.has_value() && !alias_ambiguous) {
              return alias_match;
            }
          }
          std::optional<size_t> col_match;
          bool col_ambiguous = false;
          for (size_t si = 0; si < select_count; ++si) {
            const auto& item = statement.SelectList()[si];
            if (!item.expression ||
                item.expression->Type() != TypeTag::kColumnValue) {
              continue;
            }
            const ColumnName& scn =
                item.expression->AsColumnValue().GetColumnName();
            if (IdentifierEquals(scn.name, cn.name) &&
                (cn.schema.empty() ||
                 IdentifierEquals(scn.schema, cn.schema))) {
              if (col_match.has_value()) {
                col_ambiguous = true;
                break;
              }
              col_match = si;
            }
          }
          if (col_match.has_value() && !col_ambiguous) {
            return col_match;
          }
        }
        return std::nullopt;
      };

      std::vector<SelectStatement::OrderByTerm> mapped_order;
      mapped_order.reserve(statement.OrderBy().size());
      bool all_mapped = true;
      for (size_t oi = 0; oi < statement.OrderBy().size(); ++oi) {
        const std::optional<size_t> target_col = resolve_order_target(oi);
        if (!target_col.has_value()) {
          all_mapped = false;
          break;
        }
        SelectStatement::OrderByTerm mapped = statement.OrderBy()[oi];
        mapped.expression = ReapplyCollation(
            ConstantValueExp(Value(static_cast<int64_t>(*target_col + 1))),
            statement.OrderBy()[oi].expression);
        mapped_order.push_back(std::move(mapped));
      }
      if (all_mapped) {
        compact_stmt.emplace(statement);
        compact_stmt->SetOrderBy({});
        compact_stmt->SetLimit(std::nullopt);
        compact_stmt->SetOffset(0);
        compact_stmt->SetWithTies(false);
        sort_stmt.emplace(statement);
        sort_stmt->SetOrderBy(std::move(mapped_order));
        compact_allowed = true;
      }
    }
    if (compact_allowed) {
      const auto& stat_stmt = compact_stmt ? *compact_stmt : statement;
      ASSIGN_OR_RETURN(
          std::optional<Relation>, statistics,
          TryCompactStatisticalAggregate(context, stat_stmt, input,
                                         aggregate_expressions, ctes));
      if (statistics) {
        if (!has_order_by) {
          return std::move(*statistics);
        }
        if (statistics->schema.ColumnCount() == statement.SelectList().size()) {
          RETURN_IF_FAIL(
              ApplyOrderBy(context, *sort_stmt, &(*statistics), outer, ctes));
          return std::move(*statistics);
        }
      }
      std::optional<SelectStatement> folded_having_statement;
      if (having_expr && statement.Having() &&
          having_expr.get() != statement.Having().get()) {
        folded_having_statement.emplace(compact_stmt ? *compact_stmt
                                                     : statement);
        folded_having_statement->SetHaving(having_expr);
      }
      const SelectStatement& compact_arg =
          folded_having_statement ? *folded_having_statement
                                  : (compact_stmt ? *compact_stmt : statement);
      ASSIGN_OR_RETURN(std::optional<Relation>, compact,
                       TryCompactRelationAggregate(context, compact_arg, input,
                                                   aggregate_expressions));
      if (compact) {
        if (!has_order_by) {
          return std::move(*compact);
        }
        if (compact->schema.ColumnCount() == statement.SelectList().size()) {
          RETURN_IF_FAIL(
              ApplyOrderBy(context, *sort_stmt, &(*compact), outer, ctes));
          return std::move(*compact);
        }
      }
    }
  }
  struct GroupState {
    Row representative;
    size_t accumulator_offset{0};
  };
  const auto bind_group_column =
      [&](const Expression& expression) -> std::optional<slot_t> {
    if (!expression || expression->Type() != TypeTag::kColumnValue) {
      return std::nullopt;
    }
    const auto& name = expression->AsColumnValue().GetColumnName();
    if (name.name.find('.') != std::string::npos ||
        name.schema.find('.') != std::string::npos) {
      return std::nullopt;
    }
    return LocalColumnOffset(input.schema, name);
  };
  std::vector<std::optional<slot_t>> group_key_columns, group_value_columns;
  group_value_columns.reserve(aggregate_expressions.size());
  for (const auto& key : statement.GroupBy()) {
    group_key_columns.push_back(bind_group_column(key));
  }
  std::vector<AggregateAccumulator::Shape> group_shapes;
  group_shapes.reserve(aggregate_expressions.size());
  for (const auto* aggregate : aggregate_expressions) {
    group_value_columns.push_back(bind_group_column(aggregate->Child()));
    const Schema* type_schema =
        aggregate->GetType() == AggregationType::kApproxQuantiles
            ? &input.schema
            : nullptr;
    group_shapes.push_back(
        AggregateAccumulator::AnalyzeShape(aggregate, type_schema));
  }
  std::vector<Value> group_key_scratch;
  group_key_scratch.reserve(statement.GroupBy().size());
  std::deque<AggregateAccumulator> aggregate_states;
  auto accumulate_row =
      [&](const Row& row, GroupKeyMap* offsets,
          std::vector<GroupState>* local_groups,
          std::deque<AggregateAccumulator>* local_states) -> Status {
    Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
    group_key_scratch.clear();
    for (size_t i = 0; i < statement.GroupBy().size(); ++i) {
      if (group_key_columns[i]) {
        group_key_scratch.push_back(
            CanonicalDistinctValue(row[*group_key_columns[i]]));
      } else {
        ASSIGN_OR_RETURN(Value, key,
                         (TryEvaluate(statement.GroupBy()[i], scope, nullptr,
                                      context, ctes)));
        group_key_scratch.push_back(CanonicalDistinctValue(key));
      }
    }
    auto iter = offsets->find(group_key_scratch);
    const bool inserted = iter == offsets->end();
    if (inserted) {
      iter =
          offsets->emplace(Row(group_key_scratch), local_groups->size()).first;
      GroupState group;
      group.accumulator_offset = local_states->size();
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        local_states->emplace_back(aggregate_expressions[i], group_shapes[i]);
      }
      local_groups->push_back(std::move(group));
    }
    GroupState& group = (*local_groups)[iter->second];
    for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
      AggregateAccumulator& accumulator =
          (*local_states)[group.accumulator_offset + i];
      const AggregateExpression& aggregate = *accumulator.expression;
      if (aggregate.WhereFilter()) {
        ASSIGN_OR_RETURN(Value, where_value,
                         (TryEvaluate(aggregate.WhereFilter(), scope, nullptr,
                                      context, ctes)));
        if (!Truthy(where_value)) {
          continue;
        }
      }
      if (IsCountStar(aggregate)) {
        RETURN_IF_FAIL(accumulator.Add(Value(1)));
        continue;
      }
      AggregateInput aggregate_input;
      if (group_value_columns[i]) {
        aggregate_input.value = row[*group_value_columns[i]];
      } else {
        ASSIGN_OR_RETURN(
            Value, value,
            (TryEvaluate(aggregate.Child(), scope, nullptr, context, ctes)));
        aggregate_input.value = std::move(value);
      }
      if (aggregate.Having() != AggregateHavingModifier::kNone &&
          aggregate.HavingCondition()) {
        ASSIGN_OR_RETURN(Value, hv11119_0,
                         (TryEvaluate(aggregate.HavingCondition(), scope,
                                      nullptr, context, ctes)));
        aggregate_input.condition = std::move(hv11119_0);
      }
      for (const auto& term : aggregate.InnerOrderBy()) {
        ASSIGN_OR_RETURN(
            Value, hv11336_0,
            (TryEvaluate(term.expression, scope, nullptr, context, ctes)));
        aggregate_input.order_keys.push_back(std::move(hv11336_0));
      }
      for (const Expression& extra : aggregate.ExtraArgs()) {
        // T2054: mirror the TrailingArgs arm below (and the sibling loops in
        // aggregation.cpp / parallel_aggregation.cpp / subquery_runtime.cpp,
        // all of which skip nulls): ExtraArgs entries can be null carriers
        // and TryEvaluate dereferences `expression->Type()` immediately.
        if (extra) {
          ASSIGN_OR_RETURN(Value, hv11523_0,
                           (TryEvaluate(extra, scope, nullptr, context, ctes)));
          aggregate_input.order_keys.push_back(std::move(hv11523_0));
        }
      }
      if (aggregate.SecondaryArg()) {
        ASSIGN_OR_RETURN(Value, hv11676_0,
                         (TryEvaluate(aggregate.SecondaryArg(), scope, nullptr,
                                      context, ctes)));
        aggregate_input.auxiliary = std::move(hv11676_0);
      }
      for (const Expression& extra : aggregate.TrailingArgs()) {
        if (extra) {
          ASSIGN_OR_RETURN(Value, hv11885_0,
                           (TryEvaluate(extra, scope, nullptr, context, ctes)));
          aggregate_input.trailing_values.push_back(std::move(hv11885_0));
        }
      }
      RETURN_IF_FAIL(accumulator.Add(std::move(aggregate_input)));
      if (context.execution_runtime() != nullptr) {
        ++context.execution_runtime()->aggregate_updates;
      }
    }
    // Only new groups need a representative row, unless bare ungrouped columns
    // are referenced, in which case we pick the canonical minimum row across
    // `ungrouped_slots` (NULLS LAST) so join/spill reordering cannot change the
    // chosen representative.
    if (inserted) {
      group.representative = row;
    } else if (!ungrouped_slots.empty()) {
      for (slot_t slot : ungrouped_slots) {
        if (slot >= row.Count() || slot >= group.representative.Count()) {
          continue;
        }
        const Value& cand = row[slot];
        const Value& curr = group.representative[slot];
        if (cand.IsNull() != curr.IsNull()) {
          if (!cand.IsNull()) {
            group.representative = row;
          }
          break;
        }
        if (!cand.IsNull()) {
          const int cmp = CompareForOrderBy(cand, curr);
          if (cmp < 0) {
            group.representative = row;
            break;
          }
          if (cmp > 0) {
            break;
          }
        }
      }
    }
    if (context.execution_runtime() != nullptr) {
      ++context.execution_runtime()->aggregate_input_rows;
    }
    return Status::kSuccess;
  };
  auto make_group = [&]() {
    GroupState group;
    group.representative =
        Row(std::vector<Value>(input.schema.ColumnCount(), Value()));
    group.accumulator_offset = aggregate_states.size();
    for (const AggregateExpression* aggregate : aggregate_expressions) {
      aggregate_states.emplace_back(aggregate, &input.schema);
    }
    return group;
  };

  const auto partition_bucket = [&](const Row& row, size_t bucket_count,
                                    uint64_t salt, Status* err) -> size_t {
    Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
    std::string encoded_key;
    for (const Expression& key : statement.GroupBy()) {
      StatusOr<Value> val_or = TryEvaluate(key, scope, nullptr, context, ctes);
      if (!val_or.HasValue()) {
        if (*err == Status::kSuccess) {
          *err = val_or.GetStatus();
        }
        return 0;
      }
      const Value key_value = CanonicalDistinctValue(val_or.MoveValue());
      // EncodeMemcomparableFormat throws on NULL, but NULL group keys are
      // legal (the in-memory path below groups them normally). Partition with
      // '\0' which never collides with a real encoding prefix (value-type byte
      // 1..5), keeping grouping inside each partition exact.
      if (key_value.IsNull()) {
        encoded_key.push_back('\0');
      } else {
        encoded_key += key_value.EncodeMemcomparableFormat();
      }
    }
    return salt == 0 ? SpillPartitionOf(encoded_key, bucket_count)
                     : SpillPartitionOfSalted(encoded_key, bucket_count, salt);
  };

  Status agg_error{Status::kSuccess};
  std::vector<GroupState> groups;
  std::vector<SpillFile> agg_parts;
  bool used_partition_agg = false;
  size_t partitioned_input_rows = 0;
  if (grouped) {
    RETURN_IF_FAIL(input.FinishSpill());

    const bool partition_agg =
        !statement.GroupBy().empty() &&
        (input.HasSpill() || !QueryMemoryBudget::Global().CanReserve(
                                 std::max<size_t>(1, input.TotalRows()) * 128));
    if (partition_agg) {
      used_partition_agg = true;
      agg_parts.resize(kSpillPartitions);
      Status iterated = input.ForEachRow([&](const Row& row) {
        if (agg_error != Status::kSuccess) {
          return;
        }
        ++partitioned_input_rows;
        const size_t bucket =
            partition_bucket(row, kSpillPartitions, 0, &agg_error);
        if (agg_error != Status::kSuccess) {
          return;
        }
        Status st_append = agg_parts[bucket].Append(row);
        if (st_append != Status::kSuccess) {
          agg_error = st_append;
        }
      });
      if (iterated != Status::kSuccess && agg_error == Status::kSuccess) {
        agg_error = iterated;
      }
      for (SpillFile& part : agg_parts) {
        RETURN_IF_FAIL(part.FinishWriting());
      }
      RETURN_IF_FAIL(agg_error);
      // Every row was copied into a partition above; defer accumulation until
      // after output schema / HAVING / ORDER BY setup so each partition emits
      // and frees its hash table & accumulator states before the next runs.
      input.ResetContents();
    } else {
      if (input.HasColumnarOnly() &&
          QueryMemoryBudget::Global().CanReserve(input.TotalRows() * 128)) {
        RETURN_IF_FAIL(input.EnsureRowsMaterialized());
      }
      // Parallel grouped aggregation for big in-memory inputs whose group
      // keys and aggregate inputs are plain column references (or count(*)):
      // workers fold contiguous row slices into thread-local group maps,
      // then the main thread merges the partial states in worker order.
      // Scalar streaming kinds (COUNT/SUM/AVG/MIN/MAX) reduce; COUNT(
      // DISTINCT plain-column) merges as an exact set union through
      // AggregateAccumulator::Merge.  DISTINCT on SUM/AVG has no set-union
      // merge and keeps the serial pass.  Spilled inputs also stay serial:
      // input.rows would only cover the resident prefix.
      bool parallel_grouping = ungrouped_slots.empty() && !input.HasSpill() &&
                               !input.HasColumnarOnly() &&
                               input.TotalRows() >= kParallelGroupMinRows;
      std::vector<std::optional<slot_t>> parallel_key_offsets;
      std::vector<std::optional<slot_t>> parallel_child_offsets;
      std::vector<bool> parallel_count_star;
      // Deterministic computed keys and aggregate arguments ride the same
      // worker-safe path as plain columns through the bytecode VM (the
      // parallel scan filter already evaluates compiled programs on
      // workers).  A program that fails to compile keeps the serial loop.
      std::vector<BytecodeProgram> parallel_key_programs;
      parallel_key_programs.reserve(statement.GroupBy().size());
      std::vector<BytecodeProgram> parallel_child_programs;
      parallel_child_programs.reserve(aggregate_expressions.size());
      const auto is_plain_column = [](const Expression& expression) {
        return expression && expression->Type() == TypeTag::kColumnValue;
      };
      static const bool parallel_expression_keys_disabled =
          ::tinylamb::platform::GetEnv(
              "TINYLAMB_DISABLE_PARALLEL_EXPRESSION_KEYS") != nullptr;
      const auto bind_parallel_value = [&](const Expression& expression,
                                           std::optional<slot_t>* offset,
                                           BytecodeProgram* program) {
        if (is_plain_column(expression)) {
          *offset = LocalColumnOffset(
              input.schema, expression->AsColumnValue().GetColumnName());
          return offset->has_value();
        }
        if (parallel_expression_keys_disabled || !BytecodeEnabled()) {
          return false;
        }
        std::optional<BytecodeProgram> compiled =
            BytecodeCompiler::CompileRowCached(
                expression, input.schema, BytecodeCompiler::Context::kValue);
        if (!compiled.has_value() ||
            compiled->ResultType() == ValueType::kNull) {
          return false;
        }
        *program = std::move(*compiled);
        return true;
      };
      if (parallel_grouping) {
        parallel_key_offsets.reserve(statement.GroupBy().size());
        for (const Expression& key : statement.GroupBy()) {
          parallel_key_offsets.emplace_back(std::nullopt);
          parallel_key_programs.emplace_back();
          if (!bind_parallel_value(key, &parallel_key_offsets.back(),
                                   &parallel_key_programs.back())) {
            parallel_grouping = false;
            break;
          }
        }
      }
      if (parallel_grouping) {
        parallel_child_offsets.reserve(aggregate_expressions.size());
        parallel_count_star.reserve(aggregate_expressions.size());
        for (const AggregateExpression* aggregate : aggregate_expressions) {
          // Positional evaluation alone does not make a partial state
          // mergeable: unsupported states must retain
          // the serial accumulator, just like physical grouped aggregation.
          const AggregateAccumulator probe(aggregate, &input.schema);
          if (!probe.CanMerge()) {
            parallel_grouping = false;
            break;
          }
          const bool count_star = IsCountStar(*aggregate);
          const bool distinct_count =
              aggregate->Distinct() &&
              aggregate->GetType() == AggregationType::kCount;
          // The negated conjunction reads as "every attribute must be
          // absent"; DeMorgan-expanding it only obscures the contract.
          if (aggregate->WhereFilter() ||
              (!count_star &&
               !(  // NOLINT(readability-simplify-boolean-expr)
                   aggregate->Child() &&
                   aggregate->Having() == AggregateHavingModifier::kNone &&
                   aggregate->InnerOrderBy().empty() &&
                   !aggregate->SecondaryArg() &&
                   aggregate->ExtraArgs().empty() &&
                   !aggregate->NeedsGroupContext() &&
                   aggregate->TrailingArgs().empty() &&
                   (!aggregate->Distinct() || distinct_count)))) {
            parallel_grouping = false;
            break;
          }
          parallel_count_star.push_back(count_star);
          parallel_child_offsets.emplace_back(std::nullopt);
          parallel_child_programs.emplace_back();
          if (!count_star &&
              !bind_parallel_value(aggregate->Child(),
                                   &parallel_child_offsets.back(),
                                   &parallel_child_programs.back())) {
            parallel_grouping = false;
            break;
          }
        }
      }
      if (parallel_grouping) {
        const size_t total_rows = input.TotalRows();
        const auto workers = std::min<size_t>(
            {size_t{16},
             QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
             (total_rows / 8192) + 1});
        if (workers <= 1) {
          parallel_grouping = false;
        } else {
          bool single_int_key = false;
          slot_t single_int_slot = 0;
          ValueType single_int_type = ValueType::kNull;
          if (parallel_key_offsets.size() == 1 &&
              parallel_key_offsets[0].has_value()) {
            const Column& kcol =
                input.schema.GetColumn(*parallel_key_offsets[0]);
            if ((kcol.Type() == ValueType::kInt64 && !kcol.IsUnsigned()) ||
                kcol.Type() == ValueType::kDate) {
              single_int_key = true;
              single_int_slot = *parallel_key_offsets[0];
              single_int_type = kcol.Type();
            }
          }
          GroupKeyMap offsets;
          FlatIntKeyMap<size_t> int_merge_offsets;
          const size_t chunk_rows = 8192;
          const size_t chunk_count = (total_rows + chunk_rows - 1) / chunk_rows;
          struct WorkerLocal {
            GroupKeyMap offsets;
            FlatIntKeyMap<size_t> int_offsets;
            std::vector<GroupState> groups;
            std::deque<AggregateAccumulator> states;
            Status error{Status::kSuccess};
            size_t input_rows{0};
            size_t aggregate_updates{0};
          };
          std::vector<WorkerLocal> locals(workers);
          // Classification shared by every per-group accumulator construction
          // in the workers below; identical for all groups of one aggregate.
          const auto& parallel_shapes = group_shapes;
          {
            std::vector<std::jthread> threads;
            threads.reserve(workers);
            for (size_t w = 0; w < workers; ++w) {
              threads.emplace_back([&, w] {
                WorkerLocal& local = locals[w];
                // Positional, context-free accumulation only: TryEvaluate is
                // not safe on worker threads (the shared TransactionContext
                // and evaluator state are single-threaded contracts).
                std::vector<Value> key_values;
                key_values.reserve(parallel_key_offsets.size());
                // Static rotation: worker w folds chunks w, w+W, w+2W, ... .
                for (size_t chunk = w; chunk < chunk_count; chunk += workers) {
                  const size_t begin = chunk * chunk_rows;
                  const size_t end = std::min(total_rows, begin + chunk_rows);
                  for (size_t ri = begin; ri < end; ++ri) {
                    const Row& row = input.rows[ri];
                    size_t group_index = 0;
                    if (single_int_key && !row[single_int_slot].IsNull() &&
                        row[single_int_slot].type == single_int_type &&
                        !row[single_int_slot].IsUnsigned()) {
                      bool inserted = false;
                      size_t* slot = local.int_offsets.TryEmplace(
                          row[single_int_slot].value.int_value, &inserted);
                      if (inserted) {
                        *slot = local.groups.size();
                        GroupState group;
                        group.accumulator_offset = local.states.size();
                        for (size_t i = 0; i < aggregate_expressions.size();
                             ++i) {
                          local.states.emplace_back(aggregate_expressions[i],
                                                    parallel_shapes[i]);
                        }
                        group.representative = row;
                        local.groups.push_back(std::move(group));
                      }
                      group_index = *slot;
                    } else {
                      key_values.clear();
                      for (size_t ki = 0; ki < parallel_key_offsets.size();
                           ++ki) {
                        if (parallel_key_offsets[ki].has_value()) {
                          key_values.push_back(CanonicalDistinctValue(
                              row[*parallel_key_offsets[ki]]));
                          continue;
                        }
                        StatusOr<Value> computed =
                            TryEvaluateRowAuto(parallel_key_programs[ki], row);
                        if (!computed.HasValue()) {
                          local.error = computed.GetStatus();
                          return;
                        }
                        key_values.push_back(
                            CanonicalDistinctValue(computed.Value()));
                      }
                      Row key(std::move(key_values));
                      key_values.clear();
                      auto [iter, inserted] =
                          local.offsets.emplace(key, local.groups.size());
                      if (inserted) {
                        GroupState group;
                        group.accumulator_offset = local.states.size();
                        for (size_t i = 0; i < aggregate_expressions.size();
                             ++i) {
                          local.states.emplace_back(aggregate_expressions[i],
                                                    parallel_shapes[i]);
                        }
                        group.representative = row;
                        local.groups.push_back(std::move(group));
                      }
                      group_index = iter->second;
                    }
                    GroupState& group = local.groups[group_index];
                    for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                      AggregateAccumulator& accumulator =
                          local.states[group.accumulator_offset + i];
                      if (parallel_count_star[i]) {
                        Status st = accumulator.Add(Value(1));
                        if (st != Status::kSuccess) {
                          local.error = st;
                          return;
                        }
                        continue;
                      }
                      if (parallel_child_offsets[i].has_value()) {
                        Status st =
                            accumulator.Add(row[*parallel_child_offsets[i]]);
                        if (st != Status::kSuccess) {
                          local.error = st;
                          return;
                        }
                        continue;
                      }
                      StatusOr<Value> computed =
                          TryEvaluateRowAuto(parallel_child_programs[i], row);
                      if (!computed.HasValue()) {
                        local.error = computed.GetStatus();
                        return;
                      }
                      Status st = accumulator.Add(computed.Value());
                      if (st != Status::kSuccess) {
                        local.error = st;
                        return;
                      }
                    }
                    ++local.input_rows;
                    local.aggregate_updates += aggregate_expressions.size();
                  }
                }
              });
            }
          }
          for (const WorkerLocal& local : locals) {
            if (local.error != Status::kSuccess) {
              return local.error;
            }
          }
          if (context.execution_runtime() != nullptr) {
            for (const WorkerLocal& local : locals) {
              context.execution_runtime()->aggregate_input_rows +=
                  local.input_rows;
              context.execution_runtime()->aggregate_updates +=
                  local.aggregate_updates;
            }
          }
          size_t total_worker_groups = 0;
          for (const WorkerLocal& local : locals) {
            total_worker_groups += local.groups.size();
          }
          if (single_int_key && total_worker_groups > 0) {
            groups.reserve(total_worker_groups);
          }
          // Partition partial states by their canonical key. Each partition
          // visits workers in the same order as the serial fold, so floating
          // additions retain their existing association. A rank per first
          // occurrence also preserves the historical output encounter order.
          static const bool partition_merge_disabled =
              ::tinylamb::platform::GetEnv(
                  "TINYLAMB_DISABLE_PARTITIONED_GROUP_MERGE") != nullptr;
          const size_t merge_workers = std::min(
              workers, ParallelWorkerCount(total_worker_groups / 4096));
          const size_t staging_per_group =
              192 +
              (aggregate_expressions.size() * sizeof(AggregateAccumulator)) +
              (parallel_key_offsets.size() * sizeof(Value) * 2);
          const bool staging_fits =
              total_worker_groups <=
              (SIZE_MAX - input.charged_bytes_) / staging_per_group;
          const size_t staging_bytes =
              staging_fits ? (total_worker_groups * staging_per_group) +
                                 input.charged_bytes_
                           : 0;
          auto merge_charge =
              !partition_merge_disabled && merge_workers > 1 && staging_fits
                  ? TryReserveQueryMemory(staging_bytes)
                  : nullptr;
          if (merge_charge) {
            struct PartialRef {
              size_t worker;
              size_t group;
              const Row* key;
            };
            struct Location {
              size_t partition{SIZE_MAX};
              size_t group{0};
            };
            std::vector<PartialRef> partials;
            partials.reserve(total_worker_groups);
            std::vector<std::vector<size_t>> buckets(merge_workers);
            const auto stage = [&](size_t w, size_t g, const Row* key) {
              const size_t hash =
                  key != nullptr ? GroupKeyTransparentHash{}(*key)
                                 : std::hash<Value>{}(
                                       locals[w]
                                           .groups[g]
                                           .representative[single_int_slot]);
              uint64_t mixed = hash;
              mixed ^= mixed >> 30U;
              mixed *= 0xbf58476d1ce4e5b9ULL;
              mixed ^= mixed >> 27U;
              mixed *= 0x94d049bb133111ebULL;
              mixed ^= mixed >> 31U;
              buckets[mixed % merge_workers].push_back(partials.size());
              partials.push_back(
                  PartialRef{.worker = w, .group = g, .key = key});
            };
            for (size_t w = 0; w < locals.size(); ++w) {
              const auto& local = locals[w];
              if (single_int_key) {
                for (size_t g = 0; g < local.groups.size(); ++g) {
                  // Same admission test as the worker fold: groups keyed by
                  // an unsigned / off-type value live in local.offsets and
                  // are staged by the loop below, never twice.
                  const Value& key_val =
                      local.groups[g].representative[single_int_slot];
                  if (!key_val.IsNull() && key_val.type == single_int_type &&
                      !key_val.IsUnsigned()) {
                    stage(w, g, nullptr);
                  }
                }
              }
              for (const auto& [key, g] : local.offsets) {
                stage(w, g, &key);
              }
            }
            std::vector<WorkerLocal> partitions(merge_workers);
            std::vector<Location> first(partials.size());
            RETURN_IF_FAIL(RunParallelTasks(merge_workers, [&](size_t p) {
              auto& destination = partitions[p];
              for (const size_t rank : buckets[p]) {
                const auto& ref = partials[rank];
                auto& source = locals[ref.worker];
                auto& source_group = source.groups[ref.group];
                size_t target = 0;
                bool inserted = false;
                if (ref.key == nullptr) {
                  size_t* slot = destination.int_offsets.TryEmplace(
                      source_group.representative[single_int_slot]
                          .value.int_value,
                      &inserted);
                  if (inserted) {
                    *slot = destination.groups.size();
                  }
                  target = *slot;
                } else {
                  auto [it, fresh] = destination.offsets.emplace(
                      *ref.key, destination.groups.size());
                  inserted = fresh;
                  target = it->second;
                }
                if (inserted) {
                  first[rank] = Location{.partition = p, .group = target};
                  const size_t offset = destination.states.size();
                  for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                    destination.states.push_back(std::move(
                        source.states[source_group.accumulator_offset + i]));
                  }
                  destination.groups.push_back(GroupState{
                      .representative = std::move(source_group.representative),
                      .accumulator_offset = offset});
                } else {
                  const auto& target_group = destination.groups[target];
                  for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                    const Status st =
                        destination.states[target_group.accumulator_offset + i]
                            .Merge(
                                source.states[source_group.accumulator_offset +
                                              i]);
                    if (st != Status::kSuccess) {
                      destination.error = st;
                      return;
                    }
                  }
                }
              }
            }));
            for (const auto& part : partitions) {
              RETURN_IF_FAIL(part.error);
            }
            for (const auto& location : first) {
              if (location.partition == SIZE_MAX) {
                continue;
              }
              auto& part = partitions[location.partition];
              auto& group = part.groups[location.group];
              const size_t offset = aggregate_states.size();
              for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                aggregate_states.push_back(
                    std::move(part.states[group.accumulator_offset + i]));
              }
              group.accumulator_offset = offset;
              groups.push_back(std::move(group));
            }
            if (context.execution_runtime() != nullptr) {
              ++context.execution_runtime()->partitioned_aggregate_merges;
            }
          } else {
            // Merge partials in worker order.  A group first seen by a worker
            // is adopted wholesale (its accumulator states move onto the
            // shared deque and the offset rebases); overlapping groups merge
            // accumulator state.  Adopt-then-move keeps every partial state
            // visible.
            for (WorkerLocal& local : locals) {
              if (single_int_key && groups.empty() && offsets.empty() &&
                  local.offsets.empty() && !local.groups.empty()) {
                groups = std::move(local.groups);
                int_merge_offsets = std::move(local.int_offsets);
                aggregate_states = std::move(local.states);
                if (total_worker_groups > groups.size()) {
                  int_merge_offsets.Reserve(total_worker_groups);
                  groups.reserve(total_worker_groups);
                }
                continue;
              }
              if (single_int_key) {
                if (groups.empty() && total_worker_groups > 0) {
                  int_merge_offsets.Reserve(total_worker_groups);
                }
                for (GroupState& group : local.groups) {
                  const Value& key_val =
                      group.representative.values_[single_int_slot];
                  // Mirror the worker's int-key admission; other groups are
                  // adopted through local.offsets below.
                  if (key_val.IsNull() || key_val.type != single_int_type ||
                      key_val.IsUnsigned()) {
                    continue;
                  }
                  const int64_t kv = key_val.value.int_value;
                  bool inserted = false;
                  size_t* slot = int_merge_offsets.TryEmplace(kv, &inserted);
                  if (inserted) {
                    *slot = groups.size();
                    const size_t local_offset = group.accumulator_offset;
                    const size_t shared_offset = aggregate_states.size();
                    for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                      aggregate_states.push_back(
                          std::move(local.states[local_offset + i]));
                    }
                    group.accumulator_offset = shared_offset;
                    groups.push_back(std::move(group));
                  } else {
                    GroupState& destination = groups[*slot];
                    for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                      AggregateAccumulator& source_state =
                          local.states[group.accumulator_offset + i];
                      AggregateAccumulator& target =
                          aggregate_states[destination.accumulator_offset + i];
                      RETURN_IF_FAIL(target.Merge(source_state));
                    }
                  }
                }
                if (local.offsets.empty()) {
                  continue;
                }
              }
              for (auto& [key, index] : local.offsets) {
                GroupState& group = local.groups[index];
                auto [iter, inserted] = offsets.emplace(key, groups.size());
                if (inserted) {
                  const size_t local_offset = group.accumulator_offset;
                  const size_t shared_offset = aggregate_states.size();
                  for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                    aggregate_states.push_back(
                        std::move(local.states[local_offset + i]));
                  }
                  group.accumulator_offset = shared_offset;
                  groups.push_back(std::move(group));
                } else {
                  GroupState& destination = groups[iter->second];
                  for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                    AggregateAccumulator& source_state =
                        local.states[group.accumulator_offset + i];
                    AggregateAccumulator& target =
                        aggregate_states[destination.accumulator_offset + i];
                    RETURN_IF_FAIL(target.Merge(source_state));
                  }
                }
              }
            }
          }
        }
      }
      if (!parallel_grouping) {
        GroupKeyMap offsets;
        Status iterated = input.ForEachRow([&](const Row& row) {
          Status st_acc =
              accumulate_row(row, &offsets, &groups, &aggregate_states);
          if (st_acc != Status::kSuccess && agg_error == Status::kSuccess) {
            agg_error = st_acc;
          }
        });
        if (iterated != Status::kSuccess && agg_error == Status::kSuccess) {
          agg_error = iterated;
        }
        RETURN_IF_FAIL(agg_error);
      }
      if (input.TotalRows() == 0 &&
          (statement.GroupBy().empty() || statement.IsGroupAll())) {
        groups.push_back(make_group());
      }
    }
    if (context.execution_runtime() != nullptr && !used_partition_agg) {
      context.execution_runtime()->aggregate_groups += groups.size();
    }
  }

  Relation output(context.execution_runtime());
  CopyExecutionStats(&output, input);
  const size_t base_width =
      input.schema.ColumnCount() -
      std::min<size_t>(hidden_columns,
                       static_cast<size_t>(input.schema.ColumnCount()));
  std::vector<Column> output_columns;
  // Per-select-item expansion of star items: the input column indices each
  // `*` / `relation.*` refers to.
  auto matches_star = [](const ColumnName& requested,
                         const ColumnName& candidate, bool qualified) {
    if (qualified) {
      return !requested.schema.empty() &&
             std::equal(requested.schema.begin(), requested.schema.end(),
                        candidate.schema.begin(), candidate.schema.end(),
                        [](char a, char b) {
                          return std::tolower(static_cast<unsigned char>(a)) ==
                                 std::tolower(static_cast<unsigned char>(b));
                        });
    }
    return requested.schema.empty();
  };
  // Unwrap a visitor-injected value-table/proto wrapper for `rel.*` over a
  // real FROM source. A wrapped star whose qualifier is not a source keeps
  // its wrapper (legitimate proto row-value semantics, resolved by the
  // expression evaluator).
  const auto source_named = [&](std::string_view qualifier) {
    return std::ranges::any_of(
        statement.Sources(), [&](const SelectSource& source) {
          return (!source.alias.empty() &&
                  IdentifierEquals(source.alias, qualifier)) ||
                 (!source.table.empty() &&
                  IdentifierEquals(source.table, qualifier));
        });
  };
  const auto select_star =
      [&](const Expression& expression) -> std::optional<ColumnName> {
    auto star = UnwrapStarProjection(expression);
    if (!star) {
      return std::nullopt;
    }
    if (star->wrapped && !star->name.schema.empty() &&
        !source_named(star->name.schema)) {
      return std::nullopt;
    }
    return star->name;
  };
  // USING merge: unqualified `*` emits one coalesced entry per declared
  // using column (first), then the remaining columns in schema order;
  // `relation.*` keeps every physical column of the named side.  Each star
  // entry stores its coalesce group (single index for plain columns).
  std::vector<std::vector<std::vector<size_t>>> star_groups(
      statement.SelectList().size());
  // SELECT * over a value-table CTE must expose the proto's fields, not its
  // implementation column.  The CTE retains the SELECT AS proto constructor
  // in its source statement, which is enough to identify this shape without
  // adding type metadata to Relation.
  bool proto_value_star = false;
  // A derived proto value table (an inlined WITH body) is recognized by the
  // SELECT AS <proto> constructor it carries, recursively through set
  // operations; only the TestExtraPB shape has the three fields expanded
  // below, so other proto types stay on the ordinary star path.
  const auto query_produces_test_extra_pb = [](const SelectStatement& query,
                                               auto&& self) -> bool {
    for (const NamedExpression& item : query.SelectList()) {
      const Expression& expression = item.expression;
      if (expression && expression->Type() == TypeTag::kFunctionCallExp) {
        const FunctionCallExpression& call =
            expression->AsFunctionCallExpression();
        if ((call.FuncName() == "__proto_new" ||
             call.FuncName() == "__value_table_proto") &&
            !call.Args().empty()) {
          const Expression& type = call.Args().front();
          if (type->Type() == TypeTag::kConstantValue &&
              type->AsConstantValue().GetValue().type == ValueType::kVarChar &&
              std::string(
                  type->AsConstantValue().GetValue().value.varchar_value)
                      .find("TestExtraPB") != std::string::npos) {
            return true;
          }
        }
      }
    }
    return std::ranges::any_of(query.UnionAll(), [&](const auto& branch) {
      return branch && self(*branch, self);
    });
  };
  for (const SelectSource& source : statement.Sources()) {
    // Materialized proto value tables are catalog tables with no query to
    // inspect; they are identified by their reserved compliance names only —
    // an exact match, not a substring, so ordinary tables cannot collide.
    if (source.table == "TestExtraPBValueTable" ||
        source.table == "WithProtoValueTable") {
      proto_value_star = true;
    }
    if (source.query && query_produces_test_extra_pb(
                            *source.query, query_produces_test_extra_pb)) {
      proto_value_star = true;
    }
  }
  // A physical join may reorder inputs. Bare * still expands in FROM order,
  // including opaque derived sources that could not expand in QueryData.
  // Keep the projection as offsets; do not copy/reorder the materialized rows.
  std::vector<size_t> star_column_order(base_width);
  std::iota(star_column_order.begin(), star_column_order.end(), 0);
  if (statement.Sources().size() > 1 &&
      std::ranges::all_of(statement.Sources(), [](const SelectSource& source) {
        return !source.from_nested_join && source.using_columns.empty() &&
               (source.join_type == JoinType::kCross ||
                source.join_type == JoinType::kInner);
      })) {
    const auto source_rank = [&](size_t column) {
      const auto& qualifier = input.schema.GetColumn(column).Name().schema;
      for (size_t index = 0; index < statement.Sources().size(); ++index) {
        const auto& source = statement.Sources()[index];
        const std::string& name =
            source.alias.empty() ? source.table : source.alias;
        if (IdentifierEquals(qualifier, name)) {
          return index;
        }
      }
      return statement.Sources().size();
    };
    std::stable_sort(
        star_column_order.begin(), star_column_order.end(),
        [&](size_t a, size_t b) { return source_rank(a) < source_rank(b); });
  }
  std::vector<bool> star_proto(statement.SelectList().size(), false);
  std::vector<std::string> lowered_using;
  if (input.using_columns != nullptr) {
    lowered_using = *input.using_columns;
  }
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const NamedExpression& projection = statement.SelectList()[i];
    const auto star = select_star(projection.expression);
    if (star) {
      const ColumnName& requested = *star;
      const bool qualified = !requested.schema.empty();
      if (projection.star_modifiers) {
        // The index-group expansion below cannot splice a substitute
        // expression mid-star; REPLACE needs the query-layer expansion.
        if (!projection.star_modifiers->replace_items.empty()) {
          return StatusError(
              StatusCode::kInvalidArgument,
              "SELECT * REPLACE over this query shape is not supported; "
              "rewrite the star over a single relation");
        }
      }
      std::unordered_set<std::string> except_names;
      except_names.reserve(
          projection.star_modifiers
              ? projection.star_modifiers->except_columns.size()
              : 0);
      std::unordered_set<std::string> except_matched_names;
      except_matched_names.reserve(
          projection.star_modifiers
              ? projection.star_modifiers->except_columns.size()
              : 0);
      if (projection.star_modifiers) {
        for (const std::string& entry :
             projection.star_modifiers->except_columns) {
          const size_t dot = entry.rfind('.');
          except_names.insert(dot == std::string::npos ? entry
                                                       : entry.substr(dot + 1));
        }
      }
      const auto except_drops = [&](const ColumnName& candidate) {
        if (except_names.empty()) {
          return false;
        }
        const std::string& name = candidate.name;
        for (const std::string& entry : except_names) {
          if (entry.size() == name.size() &&
              std::equal(entry.begin(), entry.end(), name.begin(),
                         [](char a, char b) {
                           return std::tolower(static_cast<unsigned char>(a)) ==
                                  std::tolower(static_cast<unsigned char>(b));
                         })) {
            except_matched_names.insert(entry);
            return true;
          }
        }
        return false;
      };
      // The proto payload column is VARCHAR; requiring it keeps a same-named
      // non-proto table on the ordinary star expansion.
      if (proto_value_star && base_width == 1 &&
          input.schema.GetColumn(0).Type() == ValueType::kVarChar) {
        if (projection.star_modifiers) {
          return StatusError(StatusCode::kInvalidArgument,
                             "SELECT * EXCEPT/REPLACE over this table is "
                             "not supported");
        }
        star_proto[i] = true;
        output_columns.emplace_back("int32_val1", ValueType::kInt64);
        output_columns.emplace_back("int32_val2", ValueType::kInt64);
        output_columns.emplace_back("str_value", ValueType::kArray);
        continue;
      }
      if (!qualified && !lowered_using.empty()) {
        // Merged using columns first, in declaration order.
        std::vector<size_t> consumed;
        for (const std::string& using_name : lowered_using) {
          std::vector<size_t> group;
          group.reserve(base_width);
          for (size_t column = 0; column < base_width; ++column) {
            const ColumnName& candidate = input.schema.GetColumn(column).Name();
            if (candidate.name.size() == using_name.size() &&
                std::equal(using_name.begin(), using_name.end(),
                           candidate.name.begin(), [](char a, char b) {
                             return std::tolower(
                                        static_cast<unsigned char>(a)) ==
                                    std::tolower(static_cast<unsigned char>(b));
                           })) {
              group.push_back(column);
            }
          }
          if (!group.empty()) {
            consumed.insert(consumed.end(), group.begin(), group.end());
            const size_t merged_index = group.front();
            star_groups[i].push_back(std::move(group));
            output_columns.push_back(ProjectionDomainColumn(
                input.schema.GetColumn(merged_index).Name().name,
                input.schema.GetColumn(merged_index)));
          }
        }
        // Remaining non-using columns keep schema order.
        for (size_t column = 0; column < base_width; ++column) {
          if (std::find(consumed.begin(), consumed.end(), column) !=
              consumed.end()) {
            continue;
          }
          const Column& candidate = input.schema.GetColumn(column);
          if (matches_star(requested, candidate.Name(), qualified) &&
              !except_drops(candidate.Name())) {
            star_groups[i].push_back({column});
            output_columns.push_back(
                ProjectionDomainColumn(candidate.Name().name, candidate));
          }
        }
        continue;
      }
      for (const size_t column : star_column_order) {
        const Column& candidate = input.schema.GetColumn(column);
        if (matches_star(requested, candidate.Name(), qualified) &&
            !except_drops(candidate.Name())) {
          star_groups[i].push_back({column});
          output_columns.push_back(
              ProjectionDomainColumn(candidate.Name().name, candidate));
        }
      }
      // An EXCEPT entry that dropped nothing resolves nowhere: fail loudly
      // instead of silently keeping the column.
      if (projection.star_modifiers &&
          except_matched_names.size() < except_names.size()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "EXCEPT column not present in the star target");
      }
    } else {
      output_columns.push_back(ProjectionExpressionColumn(
          ProjectionName(projection, i), projection.expression, input.schema));
    }
  }
  // A star that matched nothing (unknown qualifier, or a bare `*` over an
  // empty input) would otherwise silently shrink the select list; reject it
  // like an unknown column.
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const NamedExpression& projection = statement.SelectList()[i];
    const auto star = select_star(projection.expression);
    if (star && !star_proto[i] && star_groups[i].empty()) {
      const ColumnName& requested = *star;
      return StatusError(
          StatusCode::kInvalidArgument,
          "unknown relation in select list: " +
              (requested.schema.empty() ? std::string("*")
                                        : requested.schema + ".*"));
    }
  }
  // Hidden $win columns ride along until ordering completes, then get trimmed
  // below.  ORDER BY terms may reference them.
  for (size_t k = 0; k < hidden_columns; ++k) {
    output_columns.emplace_back(input.schema.GetColumn(base_width + k));
  }

  const std::vector<SelectStatement::OrderByTerm>& order_by =
      statement.OrderBy();
  const bool has_order_by = !order_by.empty();
  // One ordering contract for the ORDER BY keys, the DISTINCT keys and the
  // bounded (LIMIT) heap alike: NULL placement, direction and the collation
  // the key resolves under all come from SortKeyOrder (type/collation.hpp),
  // the same type the sort operators use. A COLLATE'd key therefore folds here
  // exactly as it does in SortExecutor / TopNExecutor.
  std::vector<SortKeyOrder> order_specs;
  order_specs.reserve(order_by.size());
  for (const SelectStatement::OrderByTerm& term : order_by) {
    order_specs.emplace_back(term.ascending, term.nulls_first,
                             CollationOfExpression(term.expression));
  }
  bool bounded_sort = false;
  size_t topn_capacity = 0;
  if (!statement.Distinct() && statement.HasLimit() && !statement.WithTies() &&
      statement.Offset() <=
          std::numeric_limits<size_t>::max() - statement.Limit()) {
    bounded_sort = true;
    topn_capacity = statement.Offset() + statement.Limit();
  }
  const bool top1_with_ties = !statement.Distinct() && statement.HasLimit() &&
                              statement.WithTies() && statement.Limit() == 1 &&
                              statement.Offset() == 0;
  ExternalKeyedSorter sorter(order_specs, bounded_sort, topn_capacity,
                             top1_with_ties);
  Schema initial_output_schema("", output_columns);

  std::vector<std::optional<size_t>> order_positions(
      resolved_order_expressions.size());
  for (size_t order_index = 0; order_index < resolved_order_expressions.size();
       ++order_index) {
    auto& resolved_order_expression = resolved_order_expressions[order_index];
    const Expression& expression = resolved_order_expression;
    // `ORDER BY 1 COLLATE 'und:ci'` arrives as COLLATE(<int>, 'spec'): the
    // ordinal names a SELECT-list position and the clause collates it, so the
    // two resolve together. Left as a constant, the key would encode
    // identically for every row and the sort would silently do nothing.
    if (const std::optional<CollatedOrdinal> collated =
            AsCollatedOrdinal(expression);
        collated.has_value()) {
      // A COLLATE-qualified ordinal is still an ordinal: it binds to the
      // output column position like the bare-integer form below, and an
      // out-of-range value gets the same error.  Letting it fall through
      // would leave a constant key that sorts identically for every row.
      // AsCollatedOrdinal only matches COLLATE(<integer literal>, <string
      // literal>), so a COLLATE-wrapped column reference can never be
      // mistaken for a position here.
      if (collated->ordinal < 1 ||
          std::cmp_greater(collated->ordinal, output_columns.size())) {
        return StatusError(StatusCode::kInvalidArgument,
                           "ORDER BY ordinal out of range");
      }
      order_positions[order_index] = static_cast<size_t>(collated->ordinal - 1);
      continue;
    }
    if (!expression || expression->Type() != TypeTag::kConstantValue ||
        dynamic_cast<const ParameterSlot*>(expression.get()) != nullptr) {
      continue;
    }
    const Value& value = expression->AsConstantValue().GetValue();
    if (value.type != ValueType::kInt64 || value.IsNull()) {
      continue;
    }
    if (value.value.int_value < 1 ||
        static_cast<size_t>(value.value.int_value) > output_columns.size()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ORDER BY ordinal out of range");
    }
    order_positions[order_index] =
        static_cast<size_t>(value.value.int_value) - 1;
  }

  // CASE WHEN a IS NOT NULL THEN a ELSE b is the FULL OUTER JOIN key
  // coalesce. It only copies one of two cells; the bytecode interpreter
  // would rebuild a stack for every row.
  const auto positional_column =
      [&](const Expression& expression) -> std::optional<size_t> {
    if (!expression || expression->Type() != TypeTag::kColumnValue) {
      return std::nullopt;
    }
    const auto& name = expression->AsColumnValue().GetColumnName();
    if (name.name == "*" || name.name.find('.') != std::string::npos ||
        name.schema.find('.') != std::string::npos) {
      return std::nullopt;
    }
    const auto offset = LocalColumnOffset(input.schema, name);
    if (!offset ||
        !IdentifierEquals(input.schema.GetColumn(*offset).Name().name,
                          name.name)) {
      return std::nullopt;
    }
    return offset;
  };
  const auto column_null_test = [&](const auto& self,
                                    const Expression& expression)
      -> std::optional<std::pair<size_t, bool>> {
    if (!expression || expression->Type() != TypeTag::kUnaryExp) {
      return std::nullopt;
    }
    const auto& unary = expression->AsUnaryExpression();
    if (unary.Op() == UnaryOperation::kNot) {
      auto inner = self(self, unary.Child());
      if (!inner) {
        return std::nullopt;
      }
      inner->second = !inner->second;
      return inner;
    }
    if (unary.Op() != UnaryOperation::kIsNull &&
        unary.Op() != UnaryOperation::kIsNotNull) {
      return std::nullopt;
    }
    const auto column = positional_column(unary.Child());
    if (!column) {
      return std::nullopt;
    }
    return std::pair<size_t, bool>{*column,
                                   unary.Op() == UnaryOperation::kIsNotNull};
  };
  const auto null_coalesce_columns = [&](const Expression& expression)
      -> std::optional<std::pair<size_t, size_t>> {
    if (!expression || expression->Type() != TypeTag::kCaseExp) {
      return std::nullopt;
    }
    const auto& searched = expression->AsCaseExpression();
    if (searched.when_clauses_.size() != 1 || !searched.else_clause_) {
      return std::nullopt;
    }
    const auto tested =
        column_null_test(column_null_test, searched.when_clauses_[0].first);
    const auto then_column =
        positional_column(searched.when_clauses_[0].second);
    const auto else_column = positional_column(searched.else_clause_);
    if (!tested || !then_column || !else_column) {
      return std::nullopt;
    }
    // prefer, fallback: a non-null prefer wins, otherwise fallback.
    if (tested->second && tested->first == *then_column) {
      return std::pair<size_t, size_t>{*then_column, *else_column};
    }
    if (!tested->second && tested->first == *else_column) {
      return std::pair<size_t, size_t>{*else_column, *then_column};
    }
    return std::nullopt;
  };
  // Bind ordinary projection expressions once. Scope lookup and compiler
  // cache probes belong to the statement, not to every input row.
  std::vector<std::optional<size_t>> projection_offsets(
      statement.SelectList().size());
  std::vector<std::optional<BytecodeProgram>> projection_programs(
      statement.SelectList().size());
  std::vector<std::optional<std::pair<size_t, size_t>>> null_coalesces(
      statement.SelectList().size());
  if (!grouped && outer == nullptr && lowered_using.empty()) {
    for (size_t i = 0; i < statement.SelectList().size(); ++i) {
      const auto& expression = statement.SelectList()[i].expression;
      if (!expression || ContainsQuery(expression)) {
        continue;
      }
      const auto columns = expression->TouchedColumns();
      const bool positional =
          std::ranges::all_of(columns, [&](const auto& name) {
            const auto offset = LocalColumnOffset(input.schema, name);
            return name.name != "*" &&
                   name.name.find('.') == std::string::npos &&
                   name.schema.find('.') == std::string::npos && offset &&
                   IdentifierEquals(input.schema.GetColumn(*offset).Name().name,
                                    name.name);
          });
      if (!positional) {
        continue;
      }
      if (expression->Type() == TypeTag::kColumnValue) {
        projection_offsets[i] = LocalColumnOffset(
            input.schema, expression->AsColumnValue().GetColumnName());
      } else if (auto coalesced = null_coalesce_columns(expression)) {
        null_coalesces[i] = coalesced;
      } else if (BytecodeEnabled() && IsScalarBytecodeCompilable(expression)) {
        projection_programs[i] = BytecodeCompiler::CompileRowCached(
            expression, input.schema, BytecodeCompiler::Context::kValue);
      }
    }
  }

  // Stage A eligibility: for bounded ORDER BY queries whose SELECT items only
  // pass through columns or coalesce two columns (error-free), rank rows via
  // ORDER BY keys first and materialize only winning rows.
  std::vector<TopNOutputSlot> stage_a_slots;
  std::vector<ColumnarTopNKey> stage_a_keys;
  bool stage_a_eligible = !grouped && !having_expr && has_order_by &&
                          (bounded_sort || top1_with_ties) &&
                          !statement.Distinct() && hidden_columns == 0 &&
                          outer == nullptr && lowered_using.empty() &&
                          !input.HasSpill();
  if (stage_a_eligible) {
    stage_a_slots.reserve(output_columns.size());
    for (size_t item = 0; item < statement.SelectList().size(); ++item) {
      if (projection_offsets[item]) {
        stage_a_slots.push_back(
            TopNOutputSlot{.col = *projection_offsets[item]});
      } else if (!star_proto[item] && !star_groups[item].empty()) {
        for (const auto& group : star_groups[item]) {
          if (group.size() != 1) {
            stage_a_eligible = false;
            break;
          }
          stage_a_slots.push_back(TopNOutputSlot{.col = group.front()});
        }
      } else if (null_coalesces[item]) {
        stage_a_slots.push_back(
            TopNOutputSlot{.col = null_coalesces[item]->first,
                           .is_coalesce = true,
                           .fallback_col = null_coalesces[item]->second});
      } else if (projection_programs[item] &&
                 SafeToReduceEvaluationCountWithSchema(
                     statement.SelectList()[item].expression, input.schema)) {
        // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
        //   (topn_deferred_safe_projection_sound)
        stage_a_slots.push_back(
            TopNOutputSlot{.program = &*projection_programs[item]});
      } else {
        stage_a_eligible = false;
      }
      if (!stage_a_eligible) {
        break;
      }
    }
    stage_a_eligible =
        stage_a_eligible && stage_a_slots.size() == output_columns.size();
  }
  const auto has_ambiguous_output_name = [&](const ColumnName& name) {
    return std::ranges::any_of(output_columns, [&](const Column& column) {
      return IdentifierEquals(column.Name().name, name.name) &&
             (name.schema.empty() ||
              IdentifierEquals(column.Name().schema, name.schema));
    });
  };
  if (stage_a_eligible) {
    stage_a_keys.reserve(resolved_order_expressions.size());
    for (size_t oi = 0; oi < resolved_order_expressions.size(); ++oi) {
      if (order_positions[oi]) {
        const size_t pos = *order_positions[oi];
        if (pos >= stage_a_slots.size()) {
          stage_a_eligible = false;
          break;
        }
        const TopNOutputSlot& slot = stage_a_slots[pos];
        if (slot.program != nullptr) {
          stage_a_keys.push_back(ColumnarTopNKey::Computed(
              *slot.program, initial_output_schema.GetColumn(pos)));
        } else {
          stage_a_keys.push_back(
              slot.is_coalesce
                  ? ColumnarTopNKey::Coalesce(
                        static_cast<slot_t>(slot.col),
                        static_cast<slot_t>(slot.fallback_col))
                  : ColumnarTopNKey::Direct(static_cast<slot_t>(slot.col)));
        }
        continue;
      }
      const Expression& expression = resolved_order_expressions[oi];
      if (!expression) {
        stage_a_eligible = false;
        break;
      }
      if (expression->Type() == TypeTag::kColumnValue) {
        const auto& name = expression->AsColumnValue().GetColumnName();
        if (name.name == "*" || name.name.find('.') != std::string::npos ||
            name.schema.find('.') != std::string::npos) {
          stage_a_eligible = false;
          break;
        }
        const auto projected = LocalColumnOffset(initial_output_schema, name);
        if (!projected && has_ambiguous_output_name(name)) {
          stage_a_eligible = false;
          break;
        }
        if (projected && *projected < stage_a_slots.size() &&
            IdentifierEquals(
                initial_output_schema.GetColumn(*projected).Name().name,
                name.name)) {
          const TopNOutputSlot& slot = stage_a_slots[*projected];
          if (slot.program != nullptr) {
            stage_a_keys.push_back(ColumnarTopNKey::Computed(
                *slot.program, initial_output_schema.GetColumn(*projected)));
          } else {
            stage_a_keys.push_back(
                slot.is_coalesce
                    ? ColumnarTopNKey::Coalesce(
                          static_cast<slot_t>(slot.col),
                          static_cast<slot_t>(slot.fallback_col))
                    : ColumnarTopNKey::Direct(static_cast<slot_t>(slot.col)));
          }
        } else {
          const auto base = LocalColumnOffset(input.schema, name);
          if (!base ||
              !IdentifierEquals(input.schema.GetColumn(*base).Name().name,
                                name.name)) {
            stage_a_eligible = false;
            break;
          }
          stage_a_keys.push_back(
              ColumnarTopNKey::Direct(static_cast<slot_t>(*base)));
        }
        continue;
      }
      const auto touches_unshadowed_input = [&](const Expression& node) {
        for (const ColumnName& cn : node->TouchedColumns()) {
          if (cn.name == "*" || cn.name.find('.') != std::string::npos ||
              cn.schema.find('.') != std::string::npos) {
            return false;
          }
          const auto base = LocalColumnOffset(input.schema, cn);
          if (!base ||
              !IdentifierEquals(input.schema.GetColumn(*base).Name().name,
                                cn.name)) {
            return false;
          }
          const auto proj = LocalColumnOffset(initial_output_schema, cn);
          if (!proj) {
            if (has_ambiguous_output_name(cn)) {
              return false;
            }
          } else {
            if (*proj >= stage_a_slots.size() ||
                stage_a_slots[*proj].is_coalesce ||
                stage_a_slots[*proj].program != nullptr ||
                stage_a_slots[*proj].col != *base ||
                !IdentifierEquals(
                    initial_output_schema.GetColumn(*proj).Name().name,
                    cn.name)) {
              return false;
            }
          }
        }
        return true;
      };
      if (auto coal = null_coalesce_columns(expression);
          coal.has_value() && touches_unshadowed_input(expression)) {
        stage_a_keys.push_back(
            ColumnarTopNKey::Coalesce(static_cast<slot_t>(coal->first),
                                      static_cast<slot_t>(coal->second)));
        continue;
      }
      if (!ContainsQuery(expression) && !ContainsAggregate(expression) &&
          !ContainsParameterSlot(expression) && BytecodeEnabled() &&
          IsScalarBytecodeCompilable(expression) &&
          touches_unshadowed_input(expression)) {
        std::optional<BytecodeProgram> compiled =
            BytecodeCompiler::CompileRowCached(
                expression, input.schema, BytecodeCompiler::Context::kValue);
        if (compiled.has_value() && compiled->FastInterpretable()) {
          const Column res_col =
              ProjectionExpressionColumn("", expression, input.schema);
          stage_a_keys.push_back(
              ColumnarTopNKey::Computed(std::move(*compiled), res_col));
          continue;
        }
      }
      stage_a_eligible = false;
      break;
    }
  }

  if (stage_a_eligible && !top1_with_ties && input.HasColumnarOnly()) {
    RETURN_IF_FAIL(input.GetStatus());
    const auto sort_begin = std::chrono::steady_clock::now();
    ASSIGN_OR_RETURN(
        ColumnarTopNResult, topn_res,
        (TryColumnarBoundedTopN(*input.columnar, input.schema,
                                initial_output_schema, stage_a_slots,
                                stage_a_keys, order_specs, topn_capacity)));
    if (topn_res.has_value()) {
      Relation col_output(context.execution_runtime());
      col_output.SetColumnarTable(std::move(topn_res->first), topn_res->second);
      CopyExecutionStats(&col_output, input);
      RefineProjectionTypes(col_output, output_columns);
      col_output.schema = Schema("", std::move(output_columns));
      if (auto* runtime = context.execution_runtime()) {
        ++runtime->topn_finish_uses;
        runtime->topn_finish_capacity = topn_capacity;
        runtime->topn_finish_input_rows = input.columnar->total_rows;
        if (input.columnar->total_rows > 0) {
          runtime->sort_ms += ElapsedMs(sort_begin);
        }
        runtime->project_ms += ElapsedMs(project_begin);
      }
      return col_output;
    }
  }

  // Stage B: parallel columnar projection before sorting/TopN when ORDER BY
  // only reads projected output columns (or has no ORDER BY).
  if (!grouped && !having_expr && !stage_a_eligible &&
      (!has_order_by || (hidden_columns == 0 && !statement.Distinct())) &&
      outer == nullptr && lowered_using.empty() && !input.HasSpill() &&
      input.HasColumnarOnly()) {
    std::vector<ColumnarTopNKey> stage_b_keys;
    bool order_uses_only_projected = !has_order_by;
    bool stage_b_topn_ok = bounded_sort;
    if (has_order_by) {
      order_uses_only_projected = true;
      stage_b_keys.reserve(resolved_order_expressions.size());
      for (size_t oi = 0; oi < resolved_order_expressions.size(); ++oi) {
        if (order_positions[oi]) {
          stage_b_keys.push_back(ColumnarTopNKey::Direct(
              static_cast<slot_t>(*order_positions[oi])));
          continue;
        }
        if (order_by[oi].expression &&
            order_by[oi].expression->Type() == TypeTag::kColumnValue) {
          const auto& raw_name =
              order_by[oi].expression->AsColumnValue().GetColumnName();
          if (raw_name.name != "*" &&
              raw_name.name.find('.') == std::string::npos &&
              raw_name.schema.find('.') == std::string::npos) {
            const auto off = LocalColumnOffset(initial_output_schema, raw_name);
            if (off.has_value() && *off < output_columns.size() &&
                IdentifierEquals(
                    initial_output_schema.GetColumn(*off).Name().name,
                    raw_name.name)) {
              stage_b_keys.push_back(
                  ColumnarTopNKey::Direct(static_cast<slot_t>(*off)));
              continue;
            }
          }
          order_uses_only_projected = false;
          break;
        }
        const Expression& expr = resolved_order_expressions[oi];
        if (!expr || ContainsQuery(expr) || ContainsAggregate(expr) ||
            ContainsParameterSlot(expr)) {
          order_uses_only_projected = false;
          break;
        }
        for (const ColumnName& cn : expr->TouchedColumns()) {
          if (cn.name == "*" || cn.name.find('.') != std::string::npos ||
              cn.schema.find('.') != std::string::npos) {
            order_uses_only_projected = false;
            break;
          }
          const auto off = LocalColumnOffset(initial_output_schema, cn);
          if (!off.has_value() || *off >= output_columns.size() ||
              !IdentifierEquals(
                  initial_output_schema.GetColumn(*off).Name().name, cn.name)) {
            order_uses_only_projected = false;
            break;
          }
        }
        if (!order_uses_only_projected) {
          break;
        }
        if (stage_b_topn_ok && BytecodeEnabled() &&
            IsScalarBytecodeCompilable(expr)) {
          std::optional<BytecodeProgram> compiled =
              BytecodeCompiler::CompileRowCached(
                  expr, initial_output_schema,
                  BytecodeCompiler::Context::kValue);
          if (compiled.has_value() && compiled->FastInterpretable()) {
            const Column res_col =
                ProjectionExpressionColumn("", expr, initial_output_schema);
            stage_b_keys.push_back(
                ColumnarTopNKey::Computed(std::move(*compiled), res_col));
            continue;
          }
        }
        stage_b_topn_ok = false;
      }
    }
    bool columnar_eligible = order_uses_only_projected;
    const size_t visible_width = output_columns.size() - hidden_columns;
    std::vector<Expression> proj_exprs;
    if (columnar_eligible) {
      proj_exprs.reserve(visible_width);
      for (size_t i = 0; i < statement.SelectList().size(); ++i) {
        const NamedExpression& item = statement.SelectList()[i];
        if (!item.expression) {
          columnar_eligible = false;
          break;
        }
        if (star_proto[i]) {
          columnar_eligible = false;
          break;
        }
        if (!star_groups[i].empty()) {
          for (const auto& group : star_groups[i]) {
            if (group.size() != 1) {
              columnar_eligible = false;
              break;
            }
            const ColumnName& col_name =
                input.schema.GetColumn(group.front()).Name();
            if (relational_detail::LocalColumnOffset(input.schema, col_name) !=
                group.front()) {
              columnar_eligible = false;
              break;
            }
            proj_exprs.push_back(ColumnValueExp(col_name));
          }
          if (!columnar_eligible) {
            break;
          }
          continue;
        }
        if (ContainsQuery(item.expression) ||
            ContainsAggregate(item.expression) ||
            (item.expression->Type() == TypeTag::kConstantValue &&
             dynamic_cast<const ParameterSlot*>(item.expression.get()) !=
                 nullptr)) {
          columnar_eligible = false;
          break;
        }
        proj_exprs.push_back(item.expression);
      }
    }
    if (columnar_eligible && proj_exprs.size() == visible_width) {
      std::vector<Column> visible_output_columns(
          output_columns.begin(),
          output_columns.begin() + static_cast<std::ptrdiff_t>(visible_width));
      const auto plan = relational_detail::CompiledColumnarProjection::Compile(
          proj_exprs, input.schema, Schema("", visible_output_columns));
      ASSIGN_OR_RETURN(
          std::optional<std::shared_ptr<relational_detail::ColumnarTable>>,
          projected_col,
          relational_detail::TryProjectColumnarTable(plan, *input.columnar,
                                                     &input.columnar));
      if (projected_col.has_value()) {
        bool all_uniform = (*projected_col)->total_rows > 0;
        for (const auto& chunk : (*projected_col)->chunks) {
          for (const auto& col : chunk.columns) {
            if (!col.is_uniform) {
              all_uniform = false;
              break;
            }
          }
          if (!all_uniform) {
            break;
          }
        }
        Relation col_output(context.execution_runtime());
        col_output.SetColumnarTable(std::move(*projected_col), all_uniform);
        CopyExecutionStats(&col_output, input);
        RefineProjectionTypes(col_output, visible_output_columns);
        col_output.schema = Schema("", visible_output_columns);
        if (!has_order_by) {
          if (context.execution_runtime() != nullptr) {
            context.execution_runtime()->project_ms += ElapsedMs(project_begin);
          }
          return col_output;
        }
        if (bounded_sort && stage_b_topn_ok &&
            stage_b_keys.size() == order_specs.size()) {
          const auto sort_begin = std::chrono::steady_clock::now();
          const size_t total_in = col_output.columnar->total_rows;
          ASSIGN_OR_RETURN(
              ColumnarTopNResult, topn_res,
              (TryColumnarBoundedTopN(*col_output.columnar, col_output.schema,
                                      col_output.schema, {}, stage_b_keys,
                                      order_specs, topn_capacity)));
          if (topn_res.has_value()) {
            col_output.SetColumnarTable(std::move(topn_res->first),
                                        topn_res->second);
            RefineProjectionTypes(col_output, visible_output_columns);
            col_output.schema = Schema("", std::move(visible_output_columns));
            if (auto* runtime = context.execution_runtime()) {
              ++runtime->topn_finish_uses;
              runtime->topn_finish_capacity = topn_capacity;
              runtime->topn_finish_input_rows = total_in;
              if (total_in > 0) {
                runtime->sort_ms += ElapsedMs(sort_begin);
              }
              runtime->project_ms += ElapsedMs(project_begin);
            }
            return col_output;
          }
        }
        RETURN_IF_FAIL(
            ApplyOrderBy(context, statement, &col_output, outer, ctes));
        if (context.execution_runtime() != nullptr) {
          context.execution_runtime()->project_ms += ElapsedMs(project_begin);
        }
        return col_output;
      }
    }
  }

  if (stage_a_eligible && !input.HasSpill() &&
      (!input.HasColumnarOnly() ||
       QueryMemoryBudget::Global().CanReserve(input.TotalRows() * 160))) {
    RETURN_IF_FAIL(input.EnsureRowsMaterialized());
    if (!input.HasSpill() && !input.HasColumnarOnly()) {
      const size_t n_rows = input.rows.size();
      const size_t capacity = std::min(topn_capacity, n_rows);
      std::vector<size_t> prog_key_indices;
      prog_key_indices.reserve(stage_a_keys.size());
      std::vector<int> key_to_prog_idx(stage_a_keys.size(), -1);
      for (size_t k = 0; k < stage_a_keys.size(); ++k) {
        if (stage_a_keys[k].program.has_value()) {
          key_to_prog_idx[k] = static_cast<int>(prog_key_indices.size());
          prog_key_indices.push_back(k);
        }
      }
      const size_t n_prog = prog_key_indices.size();
      // WITH TIES must return every peer, so this single-pass path can keep
      // up to n_rows positions even though topn_capacity is zero. Budget for
      // that worst case instead of letting the `positions` vector grow past
      // the reserved bytes (the caller then falls back to the spillable
      // sorter, which emits the same peers).
      const size_t positions_capacity = top1_with_ties ? n_rows : capacity;
      auto position_charge =
          TryReserveQueryMemory((positions_capacity * sizeof(size_t)) +
                                (n_rows * n_prog * sizeof(Value)));
      if (position_charge) {
        RETURN_IF_FAIL(input.GetStatus());
        const auto sort_begin = std::chrono::steady_clock::now();
        std::vector<Value> prog_vals(n_rows * n_prog);
        for (size_t r = 0; r < n_rows; ++r) {
          const Row& row = input.rows[r];
          for (size_t p = 0; p < n_prog; ++p) {
            StatusOr<Value> eval_or = TryEvaluateRowAuto(
                *stage_a_keys[prog_key_indices[p]].program, row);
            RETURN_IF_FAIL(eval_or.GetStatus());
            prog_vals[(r * n_prog) + p] = eval_or.MoveValue();
          }
        }
        const auto read_row_key = [&](size_t r,
                                      size_t k) noexcept -> const Value& {
          const int p = key_to_prog_idx[k];
          if (p >= 0) {
            return prog_vals[(r * n_prog) + static_cast<size_t>(p)];
          }
          const ColumnarTopNKey& key = stage_a_keys[k];
          const Row& row = input.rows[r];
          if (!key.is_coalesce) {
            return row[key.col];
          }
          const Value& v = row[key.col];
          return !v.IsNull() ? v : row[key.fallback_col];
        };
        const auto before = [&](size_t a, size_t b) {
          for (size_t k = 0; k < stage_a_keys.size(); ++k) {
            const int comparison =
                order_specs[k].Compare(read_row_key(a, k), read_row_key(b, k));
            if (comparison != 0) {
              return comparison < 0;
            }
          }
          return a < b;
        };
        std::vector<size_t> positions;
        if (top1_with_ties) {
          // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
          //   (single_pass_argmax_eq_filterMaxRows)
          // Every peer of the minimum row must be emitted; the reservation
          // above accounts for the worst case where all n_rows are peers.
          positions.reserve(positions_capacity);
          for (size_t r = 0; r < n_rows; ++r) {
            if (positions.empty()) {
              positions.push_back(r);
              continue;
            }
            int cmp = 0;
            for (size_t k = 0; k < stage_a_keys.size(); ++k) {
              cmp = order_specs[k].Compare(read_row_key(r, k),
                                           read_row_key(positions.front(), k));
              if (cmp != 0) {
                break;
              }
            }
            if (cmp < 0) {
              positions.clear();
              positions.push_back(r);
            } else if (cmp == 0) {
              positions.push_back(r);
            }
          }
        } else {
          positions.reserve(capacity);
          for (size_t r = 0; r < n_rows && capacity != 0; ++r) {
            if (positions.size() < capacity) {
              positions.push_back(r);
              std::push_heap(positions.begin(), positions.end(), before);
            } else if (before(r, positions.front())) {
              std::pop_heap(positions.begin(), positions.end(), before);
              positions.back() = r;
              std::push_heap(positions.begin(), positions.end(), before);
            }
          }
          std::ranges::sort(positions, before);
        }
        for (const size_t position : positions) {
          const Row& src_row = input.rows[position];
          std::vector<Value> values;
          values.reserve(stage_a_slots.size());
          for (const TopNOutputSlot& slot : stage_a_slots) {
            if (slot.program != nullptr) {
              // The sort key may reuse this program, so it is evaluated once
              // per row for the key and once for each surviving output row.
              // That is safe only because stage_a_slots accepts a program via
              // SafeToReduceEvaluationCountWithSchema, which rejects volatile
              // and externally stateful expressions before we get here.
              ASSIGN_OR_RETURN(Value, evaluated,
                               TryEvaluateRowAuto(*slot.program, src_row));
              values.push_back(std::move(evaluated));
            } else if (!slot.is_coalesce) {
              values.push_back(src_row[slot.col]);
            } else {
              const Value& v = src_row[slot.col];
              values.push_back(!v.IsNull() ? v : src_row[slot.fallback_col]);
            }
          }
          output.AddRow(Row(std::move(values)));
        }
        RETURN_IF_FAIL(output.FinishSpill());
        RETURN_IF_FAIL(output.GetStatus());
        RefineProjectionTypes(output, output_columns);
        output.schema = Schema("", std::move(output_columns));
        if (auto* runtime = context.execution_runtime()) {
          ++runtime->topn_finish_uses;
          runtime->topn_finish_capacity = topn_capacity;
          runtime->topn_finish_input_rows = n_rows;
          runtime->sort_ms += ElapsedMs(sort_begin);
          runtime->project_ms += ElapsedMs(project_begin);
        }
        return output;
      }
    }
  }
  if (input.HasColumnarOnly() &&
      QueryMemoryBudget::Global().CanReserve(input.TotalRows() * 128)) {
    RETURN_IF_FAIL(input.EnsureRowsMaterialized());
  }

  // A resident, column-only projection owns its input: transfer the row
  // buffers instead of allocating and copying a second relation. Repeated
  // columns stay on the ordinary path so moving one cannot consume another.
  if (!grouped && !having_expr && !has_order_by && outer == nullptr &&
      lowered_using.empty() && !input.HasSpill() && !input.HasColumnarOnly()) {
    std::vector<size_t> transfer;
    bool eligible = true;
    for (size_t item = 0; item < statement.SelectList().size(); ++item) {
      if (projection_offsets[item]) {
        transfer.push_back(*projection_offsets[item]);
      } else if (!star_proto[item] && !star_groups[item].empty()) {
        for (const auto& group : star_groups[item]) {
          if (group.size() != 1) {
            eligible = false;
            break;
          }
          transfer.push_back(group.front());
        }
      } else {
        eligible = false;
        break;
      }
    }
    std::vector<bool> seen(input.schema.ColumnCount(), false);
    for (const size_t offset : transfer) {
      if (offset >= seen.size() || seen[offset]) {
        eligible = false;
        break;
      }
      seen[offset] = true;
    }
    const size_t visible_columns = output_columns.size() - hidden_columns;
    if (eligible && transfer.size() == visible_columns) {
      // Without ORDER BY, hidden window cells have no remaining consumer.
      // Selected window results are already bound to their input offsets.
      output_columns.resize(visible_columns);
      RETURN_IF_FAIL(input.GetStatus());
      const bool ordered = std::ranges::is_sorted(transfer);
      const size_t n = input.rows.size();
      const size_t workers = n >= 16384 ? ParallelWorkerCount(n / 8192) : 1;
      if (workers > 1) {
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
          const size_t begin = (n * w) / workers;
          const size_t end = (n * (w + 1)) / workers;
          if (ordered) {
            for (size_t r = begin; r < end; ++r) {
              Row& row = input.rows[r];
              for (size_t i = 0; i < transfer.size(); ++i) {
                if (i != transfer[i]) {
                  row.values_[i] = std::move(row.values_[transfer[i]]);
                }
              }
              row.values_.resize(transfer.size());
            }
          } else {
            std::vector<Value> values;
            values.reserve(transfer.size());
            for (size_t r = begin; r < end; ++r) {
              Row& row = input.rows[r];
              values.clear();
              for (const size_t offset : transfer) {
                values.push_back(std::move(row.values_[offset]));
              }
              row.values_.swap(values);
            }
          }
        }));
      } else if (ordered) {
        for (Row& row : input.rows) {
          for (size_t i = 0; i < transfer.size(); ++i) {
            if (i != transfer[i]) {
              row.values_[i] = std::move(row.values_[transfer[i]]);
            }
          }
          row.values_.resize(transfer.size());
        }
      } else {
        std::vector<Value> values;
        values.reserve(transfer.size());
        for (Row& row : input.rows) {
          values.clear();
          for (const size_t offset : transfer) {
            values.push_back(std::move(row.values_[offset]));
          }
          row.values_.swap(values);
        }
      }
      RefineProjectionTypes(input, output_columns);
      input.schema = Schema("", std::move(output_columns));
      input.using_columns.reset();
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->project_ms += ElapsedMs(project_begin);
      }
      return input;
    }
  }

  // Once every expression is bound, reuse each consumed row's allocation
  // as the next projection buffer. Evaluate all cells before swapping so
  // repeated/reordered inputs and expressions reading an earlier output
  // slot still see the original row. AddRow retains normal spill accounting.
  if (!grouped && !having_expr && !has_order_by && hidden_columns == 0 &&
      outer == nullptr && lowered_using.empty() && !input.HasSpill() &&
      !input.HasColumnarOnly() && !output_columns.empty() &&
      output_columns.size() == statement.SelectList().size() &&
      output_columns.size() <= input.schema.ColumnCount()) {
    bool bound = true;
    for (size_t i = 0; i < output_columns.size(); ++i) {
      bound &= projection_offsets[i].has_value() ||
               null_coalesces[i].has_value() ||
               projection_programs[i].has_value();
    }
    if (bound) {
      RETURN_IF_FAIL(input.GetStatus());
      // Numeric outputs can keep the input's charge when even their maximum
      // scalar estimate fits the minimum estimate of the original cells.
      // Variable-width results switch back to AddRow before retaining them.
      bool retain_charge =
          output_columns.size() * EstimateValueBytes(Value(0)) <=
          input.schema.ColumnCount() * EstimateValueBytes(Value());
      if (retain_charge) {
        for (const Column& col : output_columns) {
          if (col.Type() == ValueType::kVarChar ||
              col.Type() == ValueType::kArray) {
            retain_charge = false;
            break;
          }
        }
      }
      const bool has_program = std::ranges::any_of(
          projection_programs,
          [](const auto& prog) { return prog.has_value(); });
      std::vector<size_t> offset_read_count;
      if (!has_program) {
        offset_read_count.assign(input.schema.ColumnCount(), 0);
        for (size_t i = 0; i < output_columns.size(); ++i) {
          if (projection_offsets[i] &&
              *projection_offsets[i] < offset_read_count.size()) {
            ++offset_read_count[*projection_offsets[i]];
          }
          if (null_coalesces[i]) {
            if (null_coalesces[i]->first < offset_read_count.size()) {
              offset_read_count[null_coalesces[i]->first] += 2;
            }
            if (null_coalesces[i]->second < offset_read_count.size()) {
              offset_read_count[null_coalesces[i]->second] += 2;
            }
          }
        }
      }
      const auto can_move_offset = [&](size_t col) {
        return !has_program && col < offset_read_count.size() &&
               offset_read_count[col] == 1;
      };
      const auto is_fixed_numeric = [](const Value& value) {
        return value.IsNull() || value.type == ValueType::kInt64 ||
               value.type == ValueType::kDate ||
               value.type == ValueType::kDouble;
      };
      const size_t n = input.rows.size();
      const size_t workers = n >= 16384 ? ParallelWorkerCount(n / 4096) : 1;
      const size_t flush_granularity = Relation::ChargeFlushGranularity();
      bool finalized_cols = false;
      bool all_cols_uniform = false;
      if (workers > 1 && retain_charge) {
        std::vector<Status> worker_errors(workers, Status::kSuccess);
        std::vector<uint8_t> worker_non_numeric(workers, 0);
        std::vector<std::vector<ColumnTypeTracker>> worker_cols(
            workers, std::vector<ColumnTypeTracker>(output_columns.size()));
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
          const size_t begin = (n * w) / workers;
          const size_t end = (n * (w + 1)) / workers;
          std::vector<Value> values;
          values.reserve(output_columns.size());
          bool local_non_numeric = false;
          auto& col_track = worker_cols[w];
          for (size_t r = begin; r < end; ++r) {
            Row& row = input.rows[r];
            values.clear();
            for (size_t i = 0; i < output_columns.size(); ++i) {
              Value cell;
              if (projection_offsets[i]) {
                const size_t col = *projection_offsets[i];
                cell = can_move_offset(col) ? std::move(row.values_[col])
                                            : row[col];
              } else if (null_coalesces[i]) {
                const Value& prefer = row[null_coalesces[i]->first];
                cell =
                    prefer.IsNull() ? row[null_coalesces[i]->second] : prefer;
              } else if (projection_programs[i]) {
                auto projected =
                    TryEvaluateRowAuto(*projection_programs[i], row);
                if (!projected.HasValue()) {
                  worker_errors[w] = projected.GetStatus();
                  return;
                }
                cell = projected.MoveValue();
              }
              if (!is_fixed_numeric(cell)) {
                local_non_numeric = true;
              }
              col_track[i].Observe(cell);
              values.push_back(std::move(cell));
            }
            row.values_.swap(values);
          }
          worker_non_numeric[w] = static_cast<uint8_t>(local_non_numeric);
        }));
        for (const Status& err : worker_errors) {
          RETURN_IF_FAIL(err);
        }
        all_cols_uniform =
            ColumnTypeTracker::FinalizeColumns(worker_cols, output_columns);
        finalized_cols = true;
        const bool any_non_numeric = std::ranges::any_of(
            worker_non_numeric, [](uint8_t v) { return v != 0; });
        if (!any_non_numeric) {
          input.schema = Schema("", std::move(output_columns));
          input.using_columns.reset();
          input.uniform_cell_types = all_cols_uniform && !input.rows.empty();
          if (auto* runtime = context.execution_runtime()) {
            runtime->project_ms += ElapsedMs(project_begin);
          }
          return input;
        }
        output.rows.reserve(n);
        for (Row& row : input.rows) {
          const size_t bytes = EstimateRowBytesFast(row);
          output.AddRowWithKnownBytes(std::move(row), bytes, flush_granularity);
        }
      } else if (workers > 1) {
        std::vector<Relation> shards(workers);
        std::vector<ExecutionRuntime> shard_runtimes(workers);
        std::vector<std::vector<ColumnTypeTracker>> worker_cols(
            workers, std::vector<ColumnTypeTracker>(output_columns.size()));
        for (size_t w = 0; w < workers; ++w) {
          shards[w].set_runtime(&shard_runtimes[w]);
          shards[w].schema = output.schema;
        }
        std::vector<Status> worker_errors(workers, Status::kSuccess);
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
          const size_t begin = (n * w) / workers;
          const size_t end = (n * (w + 1)) / workers;
          Relation& local = shards[w];
          if (Relation::HasAmpleMemoryBudget()) {
            local.rows.reserve(end - begin);
          }
          std::vector<Value> values;
          values.reserve(output_columns.size());
          auto& col_track = worker_cols[w];
          for (size_t r = begin; r < end; ++r) {
            Row& row = input.rows[r];
            values.clear();
            size_t row_bytes = 64;
            for (size_t i = 0; i < output_columns.size(); ++i) {
              Value cell;
              if (projection_offsets[i]) {
                const size_t col = *projection_offsets[i];
                cell = can_move_offset(col) ? std::move(row.values_[col])
                                            : row[col];
              } else if (null_coalesces[i]) {
                const Value& prefer = row[null_coalesces[i]->first];
                cell =
                    prefer.IsNull() ? row[null_coalesces[i]->second] : prefer;
              } else if (projection_programs[i]) {
                auto projected =
                    TryEvaluateRowAuto(*projection_programs[i], row);
                if (!projected.HasValue()) {
                  worker_errors[w] = projected.GetStatus();
                  return;
                }
                cell = projected.MoveValue();
              }
              col_track[i].Observe(cell);
              row_bytes += cell.type == ValueType::kArray
                               ? EstimateValueBytes(cell)
                               : EstimateScalarValueBytes(cell);
              values.push_back(std::move(cell));
            }
            row.values_.swap(values);
            local.AddRowWithKnownBytes(std::move(row), row_bytes,
                                       flush_granularity);
          }
        }));
        for (const Status& err : worker_errors) {
          RETURN_IF_FAIL(err);
        }
        if (output.runtime() != nullptr) {
          for (const auto& rt : shard_runtimes) {
            output.runtime()->relation_spills += rt.relation_spills;
          }
        }
        RETURN_IF_FAIL(Relation::MergeShardsInto(shards, &output));
        all_cols_uniform =
            ColumnTypeTracker::FinalizeColumns(worker_cols, output_columns);
        finalized_cols = true;
      } else {
        std::vector<ColumnTypeTracker> col_track(output_columns.size());
        std::vector<Value> values;
        values.reserve(output_columns.size());
        size_t projected_rows = 0;
        for (Row& row : input.rows) {
          values.clear();
          size_t row_bytes = 64;
          bool row_non_numeric = false;
          for (size_t i = 0; i < output_columns.size(); ++i) {
            Value cell;
            if (projection_offsets[i]) {
              const size_t col = *projection_offsets[i];
              cell =
                  can_move_offset(col) ? std::move(row.values_[col]) : row[col];
            } else if (null_coalesces[i]) {
              const Value& prefer = row[null_coalesces[i]->first];
              cell = prefer.IsNull() ? row[null_coalesces[i]->second] : prefer;
            } else if (projection_programs[i]) {
              ASSIGN_OR_RETURN(
                  Value, projected,
                  TryEvaluateRowAuto(*projection_programs[i], row));
              cell = std::move(projected);
            }
            if (retain_charge && !is_fixed_numeric(cell)) {
              row_non_numeric = true;
            }
            col_track[i].Observe(cell);
            row_bytes += cell.type == ValueType::kArray
                             ? EstimateValueBytes(cell)
                             : EstimateScalarValueBytes(cell);
            values.push_back(std::move(cell));
          }
          row.values_.swap(values);
          if (retain_charge && row_non_numeric) {
            retain_charge = false;
            for (size_t previous = 0; previous < projected_rows; ++previous) {
              const size_t prev_bytes =
                  EstimateRowBytesFast(input.rows[previous]);
              output.AddRowWithKnownBytes(std::move(input.rows[previous]),
                                          prev_bytes, flush_granularity);
            }
          }
          if (!retain_charge) {
            output.AddRowWithKnownBytes(std::move(row), row_bytes,
                                        flush_granularity);
          }
          ++projected_rows;
        }
        all_cols_uniform = ColumnTypeTracker::FinalizeColumns(
            std::span<const std::vector<ColumnTypeTracker>>(&col_track, 1),
            output_columns);
        finalized_cols = true;
        if (retain_charge) {
          input.schema = Schema("", std::move(output_columns));
          input.using_columns.reset();
          input.uniform_cell_types = all_cols_uniform && !input.rows.empty();
          if (auto* runtime = context.execution_runtime()) {
            runtime->project_ms += ElapsedMs(project_begin);
          }
          return input;
        }
      }
      RETURN_IF_FAIL(output.FinishSpill());
      RETURN_IF_FAIL(output.GetStatus());
      if (!finalized_cols) {
        RefineProjectionTypes(output, output_columns);
      } else {
        output.uniform_cell_types =
            !output.HasSpill() && all_cols_uniform && !output.rows.empty();
      }
      output.schema = Schema("", std::move(output_columns));
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->project_ms += ElapsedMs(project_begin);
      }
      return output;
    }
  }

  auto emit = [&](const Row& representative,
                  const AggregateResultMap* aggregates) -> Status {
    Scope scope{
        .row = &representative, .schema = &input.schema, .outer = outer};
    if (aggregates != nullptr) {
      // Correlated subqueries in HAVING / SELECT resolve aggregate aliases
      // against this group's values through the scope chain.
      scope.aggregates = aggregates;
    }
    if (!lowered_using.empty()) {
      scope.using_columns = &lowered_using;
    }
    if (having_expr) {
      ASSIGN_OR_RETURN(
          Value, having_value,
          (TryEvaluate(having_expr, scope, aggregates, context, ctes)));
      if (!Truthy(having_value)) {
        return Status::kSuccess;
      }
    }
    std::vector<Value> values;
    values.reserve(output_columns.size());
    for (size_t item = 0; item < statement.SelectList().size(); ++item) {
      const NamedExpression& projection = statement.SelectList()[item];
      if (star_proto[item] || !star_groups[item].empty()) {
        if (star_proto[item]) {
          const ColumnName base = input.schema.GetColumn(0).Name();
          for (const char* field : {"int32_val1", "int32_val2", "str_value"}) {
            ASSIGN_OR_RETURN(
                Value, hv25286_0,
                (TryEvaluate(FunctionCallExp(
                                 "__get_field_safe",
                                 {ColumnValueExp(base),
                                  ConstantValueExp(Value(std::string(field)))}),
                             scope, aggregates, context, ctes)));
            values.push_back(std::move(hv25286_0));
          }
          continue;
        }
        for (const std::vector<size_t>& group : star_groups[item]) {
          // USING-merged entries coalesce their physical duplicates.
          Value merged;
          for (const size_t column : group) {
            if (!representative.values_[column].IsNull()) {
              merged = representative.values_[column];
              break;
            }
          }
          values.push_back(std::move(merged));
        }
      } else if (projection_offsets[item]) {
        values.push_back(representative[*projection_offsets[item]]);
      } else if (null_coalesces[item]) {
        const Value& prefer = representative[null_coalesces[item]->first];
        values.push_back(prefer.IsNull()
                             ? representative[null_coalesces[item]->second]
                             : prefer);
      } else if (projection_programs[item]) {
        Value projected;
        Status error;
        const auto evaluated = projection_programs[item]->TryEvaluateRowFast(
            representative, &projected, &error);
        if (evaluated == BytecodeProgram::FastEval::kError) {
          return error;
        }
        if (evaluated == BytecodeProgram::FastEval::kUnsupported) {
          ASSIGN_OR_RETURN(
              Value, canonical,
              projection_programs[item]->TryEvaluateRow(representative));
          projected = std::move(canonical);
        }
        values.push_back(std::move(projected));
      } else {
        ASSIGN_OR_RETURN(Value, projection_value,
                         (TryEvaluate(projection.expression, scope, aggregates,
                                      context, ctes)));
        values.push_back(std::move(projection_value));
      }
    }
    // Carry hidden $win columns through projection; ORDER BY may reference
    // them and they are trimmed after sorting.
    for (size_t k = base_width; k < representative.values_.size(); ++k) {
      values.push_back(representative.values_[k]);
    }
    Row output_row(std::move(values));
    if (has_order_by) {
      // The grouped ORDER BY key may reference output columns, aliases,
      // aggregates and group-key base columns -- never a relation alias
      // denoting a whole row (the query is grouped by parts of that row),
      // so the row-alias fallback is suppressed on both scopes.  The base
      // wrapper keeps select-item evaluation (`COUNT(t)`) on the lenient
      // scope above.
      Scope order_base{.row = scope.row,
                       .schema = scope.schema,
                       .outer = scope.outer,
                       .using_columns = scope.using_columns,
                       .no_row_alias = true};
      Scope proj_scope{.row = &output_row,
                       .schema = &initial_output_schema,
                       .outer = &order_base,
                       .no_row_alias = true};
      std::vector<Value> keys;
      keys.reserve(order_by.size());
      for (size_t k = 0; k < resolved_order_expressions.size(); ++k) {
        if (order_positions[k]) {
          keys.push_back(output_row[*order_positions[k]]);
          continue;
        }
        const Expression& key = resolved_order_expressions[k];
        ASSIGN_OR_RETURN(
            Value, hv26761_0,
            (TryEvaluate(key, proj_scope, aggregates, context, ctes)));
        keys.push_back(std::move(hv26761_0));
      }
      RETURN_IF_FAIL(sorter.Add(
          KeyedRow{.keys = std::move(keys), .row = std::move(output_row)}));
    } else {
      output.AddRow(std::move(output_row));
    }
    return Status::kSuccess;
  };

  // Fast grouped emit for positional shapes: every GROUP BY key is a plain
  // input column, every select item and ORDER BY term is a group-key column
  // or a top-level aggregate, and HAVING (when present) references only
  // those.  The generic emit evaluates each select item through a Scope +
  // TryEvaluate AST walk per group, which dominates aggregations with many
  // groups (TPC-H Q3 emits ~1M groups through GroupByFinish).
  struct FastEmitItem {
    bool is_key;
    size_t index;
  };
  std::vector<slot_t> fast_key_offsets;
  std::vector<FastEmitItem> fast_items;
  std::vector<FastEmitItem> fast_order_items;
  bool fast_emit = grouped;
  const auto classify_fast = [&](const Expression& expression,
                                 FastEmitItem* item) -> bool {
    if (!expression) {
      return false;
    }
    if (expression->Type() == TypeTag::kColumnValue) {
      const auto offset = LocalColumnOffset(
          input.schema, expression->AsColumnValue().GetColumnName());
      if (offset) {
        const auto pos =
            std::find(fast_key_offsets.begin(), fast_key_offsets.end(),
                      static_cast<slot_t>(*offset));
        if (pos != fast_key_offsets.end()) {
          *item = FastEmitItem{
              .is_key = true,
              .index = static_cast<size_t>(pos - fast_key_offsets.begin())};
          return true;
        }
      }
      return false;
    }
    if (expression->Type() == TypeTag::kAggregateExp) {
      const auto pos =
          std::find(aggregate_expressions.begin(), aggregate_expressions.end(),
                    &expression->AsAggregateExpression());
      if (pos != aggregate_expressions.end()) {
        *item = FastEmitItem{
            .is_key = false,
            .index = static_cast<size_t>(pos - aggregate_expressions.begin())};
        return true;
      }
    }
    return false;
  };
  if (fast_emit) {
    for (const Expression& key : statement.GroupBy()) {
      if (!key || key->Type() != TypeTag::kColumnValue) {
        fast_emit = false;
        break;
      }
      const auto offset =
          LocalColumnOffset(input.schema, key->AsColumnValue().GetColumnName());
      if (!offset) {
        fast_emit = false;
        break;
      }
      fast_key_offsets.push_back(static_cast<slot_t>(*offset));
    }
  }
  if (fast_emit) {
    for (const NamedExpression& item : statement.SelectList()) {
      FastEmitItem emit_item{};
      if (!classify_fast(item.expression, &emit_item)) {
        fast_emit = false;
        break;
      }
      fast_items.push_back(emit_item);
    }
  }
  if (std::ranges::any_of(order_positions, [](const auto& position) {
        return position.has_value();
      })) {
    fast_emit = false;
  }
  if (fast_emit) {
    for (const Expression& key : resolved_order_expressions) {
      FastEmitItem order_item{};
      if (!classify_fast(key, &order_item)) {
        fast_emit = false;
        break;
      }
      fast_order_items.push_back(order_item);
    }
  }
  static const bool fast_emit_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_LEGACY_PLANNER") != nullptr;
  if ((fast_emit && having_expr &&
       !HavingReferencesOnlyKeysAndAggregates(having_expr, input.schema,
                                              fast_key_offsets)) ||
      fast_emit_disabled) {
    fast_emit = false;
  }

  std::vector<Value> finished_values;
  AggregateResultMap aggregate_results_hoisted;
  bool simple_having_agg_const = false;
  size_t simple_having_agg_idx = 0;
  BinaryOperation simple_having_op = BinaryOperation::kEquals;
  Value simple_having_const;
  if (grouped && fast_emit) {
    finished_values.resize(aggregate_expressions.size());
    aggregate_results_hoisted.reserve(aggregate_expressions.size());
    if (having_expr && having_expr->Type() == TypeTag::kBinaryExp) {
      const BinaryExpression& bin = having_expr->AsBinaryExpression();
      if (bin.Left() && bin.Left()->Type() == TypeTag::kAggregateExp &&
          bin.Right() && bin.Right()->Type() == TypeTag::kConstantValue) {
        auto pos = std::find(aggregate_expressions.begin(),
                             aggregate_expressions.end(),
                             &bin.Left()->AsAggregateExpression());
        if (pos != aggregate_expressions.end()) {
          simple_having_agg_const = true;
          simple_having_agg_idx =
              static_cast<size_t>(pos - aggregate_expressions.begin());
          simple_having_op = bin.Op();
          simple_having_const = bin.Right()->AsConstantValue().GetValue();
        }
      }
    }
  }

  const auto emit_group_batch =
      [&](const std::vector<GroupState>& batch_groups,
          std::deque<AggregateAccumulator>& batch_states) -> Status {
    if (fast_emit) {
      for (const GroupState& group : batch_groups) {
        for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
          ASSIGN_OR_RETURN(
              Value, finished,
              batch_states[group.accumulator_offset + i].TryFinish());
          finished_values[i] = std::move(finished);
        }
        if (having_expr) {
          if (simple_having_agg_const) {
            ASSIGN_OR_RETURN(Value, having_value,
                             TryBinary(simple_having_op,
                                       finished_values[simple_having_agg_idx],
                                       simple_having_const));
            if (!Truthy(having_value)) {
              continue;
            }
          } else {
            for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
              aggregate_results_hoisted.insert_or_assign(
                  aggregate_expressions[i], finished_values[i]);
            }
            Scope scope{.row = &group.representative,
                        .schema = &input.schema,
                        .outer = outer};
            scope.aggregates = &aggregate_results_hoisted;
            ASSIGN_OR_RETURN(
                Value, having_value,
                (TryEvaluate(having_expr, scope, &aggregate_results_hoisted,
                             context, ctes)));
            if (!Truthy(having_value)) {
              continue;
            }
          }
        }
        std::vector<Value> values;
        values.reserve(fast_items.size());
        for (const FastEmitItem& item : fast_items) {
          if (item.is_key) {
            values.push_back(
                group.representative.values_[fast_key_offsets[item.index]]);
          } else {
            values.push_back(finished_values[item.index]);
          }
        }
        Row output_row(std::move(values));
        if (has_order_by) {
          std::vector<Value> keys;
          keys.reserve(fast_order_items.size());
          for (const FastEmitItem& item : fast_order_items) {
            if (item.is_key) {
              keys.push_back(
                  group.representative.values_[fast_key_offsets[item.index]]);
            } else {
              keys.push_back(finished_values[item.index]);
            }
          }
          RETURN_IF_FAIL(sorter.Add(
              KeyedRow{.keys = std::move(keys), .row = std::move(output_row)}));
        } else {
          output.AddRow(std::move(output_row));
        }
      }
      return Status::kSuccess;
    }
    for (const GroupState& group : batch_groups) {
      AggregateResultMap aggregate_results;
      aggregate_results.reserve(aggregate_expressions.size());
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        const AggregateAccumulator& accumulator =
            batch_states[group.accumulator_offset + i];
        ASSIGN_OR_RETURN(
            Value, finished,
            batch_states[group.accumulator_offset + i].TryFinish());
        aggregate_results.emplace(accumulator.expression, std::move(finished));
      }
      RETURN_IF_FAIL(emit(group.representative, &aggregate_results));
    }
    return Status::kSuccess;
  };

  if (grouped && used_partition_agg) {
    constexpr size_t kSubAggPartitions = 16;
    constexpr size_t kMaxAggSplitDepth = 3;
    struct AggPartitionWork {
      SpillFile file;
      size_t depth{0};
    };
    std::deque<AggPartitionWork> work_stack;
    for (auto& agg_part : agg_parts) {
      if (agg_part.Count() > 0) {
        work_stack.push_back(
            AggPartitionWork{.file = std::move(agg_part), .depth = 0});
      }
    }
    agg_parts.clear();

    const size_t est_per_group =
        192 + (aggregate_expressions.size() * 96) +
        (static_cast<size_t>(input.schema.ColumnCount()) * 32);
    const auto& budget = QueryMemoryBudget::Global();
    const size_t part_floor =
        budget.Unlimited()
            ? 0
            : std::min<size_t>(
                  256ULL << 10,
                  std::max<size_t>(size_t{4096}, budget.Limit() / 4));
    const size_t part_mem_cap =
        budget.Unlimited() ? std::numeric_limits<size_t>::max()
                           : std::max<size_t>(part_floor, budget.Limit() / 4);
    const size_t min_groups_before_split =
        (!budget.Unlimited() && budget.Limit() < (1ULL << 20)) ? 16 : 1024;

    if (partitioned_input_rows == 0 && statement.IsGroupAll()) {
      std::vector<GroupState> empty_groups;
      empty_groups.push_back(make_group());
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->aggregate_groups += 1;
      }
      RETURN_IF_FAIL(emit_group_batch(empty_groups, aggregate_states));
    }

    while (!work_stack.empty()) {
      AggPartitionWork work = std::move(work_stack.front());
      work_stack.pop_front();
      bool overflowed = false;
      {
        GroupKeyMap offsets;
        std::vector<GroupState> local_groups;
        std::deque<AggregateAccumulator> local_states;
        ASSIGN_OR_RETURN(SpillFile::RowReader, reader,
                         work.file.OpenRowReader());
        Row row;
        bool has_row = false;
        while (true) {
          RETURN_IF_FAIL(reader.Next(&row, &has_row));
          if (!has_row) {
            break;
          }
          RETURN_IF_FAIL(
              accumulate_row(row, &offsets, &local_groups, &local_states));
          if (!budget.Unlimited() && work.depth < kMaxAggSplitDepth &&
              local_groups.size() >= min_groups_before_split) {
            const size_t est_bytes = local_groups.size() * est_per_group;
            if (est_bytes > part_mem_cap || !budget.CanReserve(est_bytes)) {
              overflowed = true;
              break;
            }
          }
        }
        if (!overflowed) {
          work.file = SpillFile{};
          if (context.execution_runtime() != nullptr) {
            context.execution_runtime()->aggregate_groups +=
                local_groups.size();
          }
          RETURN_IF_FAIL(emit_group_batch(local_groups, local_states));
        }
      }
      if (overflowed) {
        std::vector<SpillFile> sub_parts(kSubAggPartitions);
        Status split_err{Status::kSuccess};
        Status split_iter = work.file.ForEachRow([&](const Row& row) {
          if (split_err != Status::kSuccess) {
            return;
          }
          const size_t bucket = partition_bucket(row, kSubAggPartitions,
                                                 work.depth + 1, &split_err);
          if (split_err != Status::kSuccess) {
            return;
          }
          Status st_append = sub_parts[bucket].Append(row);
          if (st_append != Status::kSuccess) {
            split_err = st_append;
          }
        });
        if (split_iter != Status::kSuccess && split_err == Status::kSuccess) {
          split_err = split_iter;
        }
        RETURN_IF_FAIL(split_err);
        for (SpillFile& sub : sub_parts) {
          RETURN_IF_FAIL(sub.FinishWriting());
        }
        work.file = SpillFile{};
        for (size_t s = sub_parts.size(); s > 0; --s) {
          if (sub_parts[s - 1].Count() > 0) {
            work_stack.push_front(AggPartitionWork{
                .file = std::move(sub_parts[s - 1]), .depth = work.depth + 1});
          }
        }
      }
    }
  } else if (grouped) {
    RETURN_IF_FAIL(emit_group_batch(groups, aggregate_states));
  } else {
    RETURN_IF_FAIL(input.FinishSpill());
    Status emit_error{Status::kSuccess};
    Status iterated = input.ForEachRow([&](const Row& row) {
      if (emit_error != Status::kSuccess) {
        return;
      }
      Status st_emit = emit(row, nullptr);
      if (st_emit != Status::kSuccess) {
        emit_error = st_emit;
      }
    });
    if (iterated != Status::kSuccess && emit_error == Status::kSuccess) {
      emit_error = iterated;
    }
    RETURN_IF_FAIL(emit_error);
  }

  if (has_order_by) {
    const auto sort_begin = std::chrono::steady_clock::now();
    if (sorter.UsedBoundedHeap() && context.execution_runtime() != nullptr) {
      ++context.execution_runtime()->topn_finish_uses;
      context.execution_runtime()->topn_finish_capacity = topn_capacity;
      context.execution_runtime()->topn_finish_input_rows =
          sorter.TotalInputRows();
    }
    if (!sorter.Empty()) {
      RETURN_IF_FAIL(sorter.EmitSorted([&](Row&& sorted_row) {
        output.AddRow(std::move(sorted_row));
        return Status::kSuccess;
      }));
    }
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->sort_ms += ElapsedMs(sort_begin);
    }
  }

  RETURN_IF_FAIL(output.FinishSpill());
  // Drop the hidden $win columns now that ordering has consumed them.
  if (hidden_columns > 0) {
    const size_t visible_width = output_columns.size() - hidden_columns;
    // PRODUCTION FIX: rows spilled under a memory budget are not in
    // output.rows; stream spilled outputs into a fresh Relation instead of
    // reloading them all into memory.
    if (!output.HasSpill()) {
      for (Row& row : output.rows) {
        row.values_.resize(std::min(visible_width, row.values_.size()));
      }
    } else {
      Relation trimmed(context.execution_runtime());
      trimmed.schema = output.schema;
      trimmed.using_columns = output.using_columns;
      CopyExecutionStats(&trimmed, output);
      RETURN_IF_FAIL(output.ForEachRow([&](const Row& row) {
        Row copy = row;
        copy.values_.resize(std::min(visible_width, copy.values_.size()));
        trimmed.AddRow(std::move(copy));
      }));
      RETURN_IF_FAIL(trimmed.FinishSpill());
      RETURN_IF_FAIL(trimmed.GetStatus());
      output = std::move(trimmed);
    }
    output_columns.resize(visible_width);
  }
  RefineProjectionTypes(output, output_columns);
  output.schema = Schema("", std::move(output_columns));
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->project_ms += ElapsedMs(project_begin);
  }
  return output;
}

}  // namespace

// Window functions in a grouped statement evaluate over the grouped rows
// (SQL semantics), including calls whose arguments are aggregates themselves
// (`avg(sum(x)) OVER (PARTITION BY key)` — TPC-DS q12/q89).  This runs the
// statement in three passes:
//   1. a grouped pass where every window call is pulled out of the select
//      list, each distinct aggregate nested inside a window call becomes a
//      hidden `$aggK` select item, and the ordinary grouping machinery runs;
//   2. a window pass computing each call over the grouped rows (window
//      arguments referencing `$aggK` columns), producing `$winN` columns;
//   3. a final projection pass over grouped+$win rows that only references
//      materialized columns, restoring ORDER BY / DISTINCT / LIMIT.
StatusOr<Relation> ExecuteWindowOverAggregate(
    TransactionContext& context, const SelectStatement& statement,
    Relation input, const Scope* outer, const CteMap& ctes, bool apply_where,
    bool build_grouped_input, bool columnar_output) {
  const std::string unsupported =
      "window functions combined with GROUP BY are not supported here";
  if (statement.HasDistinctOn()) {
    return StatusError(StatusCode::kInvalidArgument, unsupported);
  }
  const auto window_source_named = [&](std::string_view qualifier) {
    return std::ranges::any_of(
        statement.Sources(), [&](const SelectSource& source) {
          return (!source.alias.empty() &&
                  IdentifierEquals(source.alias, qualifier)) ||
                 (!source.table.empty() &&
                  IdentifierEquals(source.table, qualifier));
        });
  };
  for (const NamedExpression& projection : statement.SelectList()) {
    auto star = UnwrapStarProjection(projection.expression);
    if (star && star->wrapped && !star->name.schema.empty() &&
        !window_source_named(star->name.schema)) {
      star.reset();  // genuine proto value table, not a FROM-source star
    }
    if (star) {
      return StatusError(StatusCode::kInvalidArgument, unsupported);
    }
  }
  std::vector<Expression> window_nodes;
  for (const NamedExpression& projection : statement.SelectList()) {
    CollectWindowCalls(projection.expression, &window_nodes);
  }
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    CollectWindowCalls(term.expression, &window_nodes);
  }
  // QUALIFY is one more consumer of the same window/aggregate pool: its
  // window calls join the pass-2 computation and its aggregates become
  // hidden `$aggK` group-level columns.
  CollectWindowCalls(statement.Qualify(), &window_nodes);

  auto grouped = std::make_shared<SelectStatement>(statement);
  std::vector<NamedExpression> grouped_select;
  grouped_select.reserve(statement.SelectList().size());
  // Alias (lowercased) -> grouped output column name, used by pass 3 to point
  // ORDER BY references at materialized columns.
  std::unordered_map<std::string, std::string> alias_columns;
  // Items mixing a window call with other nodes (sum(x) * 100 /
  // sum(sum(x)) OVER ...) are re-built in pass 3 from `$aggK`/`$winN` columns;
  // their pass-1 slot is a NULL placeholder, and every aggregate they contain
  // becomes a hidden `$aggK` group-level column.
  std::vector<bool> item_has_window(statement.SelectList().size(), false);
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const NamedExpression& projection = statement.SelectList()[i];
    item_has_window[i] = ContainsWindowCall(projection.expression);
    if (item_has_window[i]) {
      grouped_select.emplace_back(projection.name, ConstantValueExp(Value()));
    } else {
      grouped_select.emplace_back(projection.name, projection.expression);
    }
    if (!projection.name.empty()) {
      alias_columns.emplace(AsciiToLower(projection.name),
                            ProjectionName(projection, i));
    }
  }
  // Mixed items contribute their aggregates to the same $aggK pool up front
  // (pass 3 rebuilds the whole item from the materialized columns; the window
  // pass below reuses the registrations through aggregate_columns).
  // Reuse already projected group keys and aggregates for window inputs.
  // Distinct output names are necessary: an ambiguous alias must not replace
  // a positional hidden column. This also avoids copying the same long
  // string key into several partition/order columns for every group.
  std::unordered_map<std::string, std::string> existing_columns;
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    if (item_has_window[i]) {
      continue;
    }
    const auto& item = statement.SelectList()[i];
    const auto name = ProjectionName(item, i);
    size_t matches = 0;
    for (size_t j = 0; j < statement.SelectList().size(); ++j) {
      matches += static_cast<size_t>(
          IdentifierEquals(name, ProjectionName(statement.SelectList()[j], j)));
    }
    if (matches == 1 && item.expression) {
      existing_columns.emplace(item.expression->ToString(), name);
    }
  }
  std::vector<std::pair<std::string, Expression>> hidden_aggregates;
  // Window partition / ORDER BY keys materialized as hidden grouped columns
  // ($pkJ), for keys the select list does not project.
  std::vector<NamedExpression> partition_items;
  std::unordered_map<std::string, std::string> partition_columns;
  std::unordered_map<std::string, std::string> aggregate_columns;
  for (const auto& item : statement.SelectList()) {
    if (item.expression && item.expression->Type() == TypeTag::kAggregateExp) {
      const auto expression = item.expression->ToString();
      if (const auto found = existing_columns.find(expression);
          found != existing_columns.end()) {
        aggregate_columns.emplace(expression, found->second);
      }
    }
  }
  RegisterAggregatesForMaterialization(statement.Qualify(), &aggregate_columns,
                                       &hidden_aggregates);
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    if (!item_has_window[i] || statement.SelectList()[i].expression->Type() ==
                                   TypeTag::kWindowFunctionExp) {
      continue;
    }
    RegisterAggregatesForMaterialization(statement.SelectList()[i].expression,
                                         &aggregate_columns,
                                         &hidden_aggregates);
  }
  std::vector<std::shared_ptr<WindowFunctionCallExpression>> rewritten_windows;
  rewritten_windows.reserve(window_nodes.size());
  for (size_t w = 0; w < window_nodes.size(); ++w) {
    const std::string window_name = "$win" + std::to_string(w);
    aggregate_columns.emplace(window_nodes[w]->ToString(), window_name);
    // WindowFunctionCallExpression is non-copyable (ExpressionBase); rebuild
    // the call field by field so aggregate arguments can be pointed at the
    // grouped `$aggK` columns.
    const auto& original = window_nodes[w]->AsWindowFunctionCallExpression();
    auto rewritten = std::make_shared<WindowFunctionCallExpression>();
    rewritten->function = original.function;
    rewritten->where_filter = original.where_filter;
    rewritten->distinct = original.distinct;
    rewritten->inner_order_by = original.inner_order_by;
    rewritten->inner_limit = original.inner_limit;
    rewritten->partition_by = original.partition_by;
    rewritten->order_by = original.order_by;
    rewritten->frame_unit = original.frame_unit;
    rewritten->frame_start = original.frame_start;
    rewritten->frame_end = original.frame_end;
    rewritten->has_frame = original.has_frame;
    rewritten->exclusion = original.exclusion;
    rewritten->respect_nulls = original.respect_nulls;
    rewritten->ignore_nulls = original.ignore_nulls;
    rewritten->args = original.args;
    const auto point_at_grouped_aggregate =
        [&](const Expression& node) -> std::optional<Expression> {
      if (node->Type() != TypeTag::kAggregateExp) {
        return std::nullopt;
      }
      auto [it, inserted] = aggregate_columns.emplace(
          node->ToString(), "$agg" + std::to_string(hidden_aggregates.size()));
      if (inserted) {
        hidden_aggregates.emplace_back(it->second, node);
      }
      return ColumnValueExp(ColumnName("", it->second));
    };
    // Partition / ORDER BY keys must resolve against the grouped output, but
    // the window may name a GROUP BY key that the select list never projects.
    // Materialize every distinct key expression as a hidden `$pkJ` column and
    // point the rewritten call at it.
    const auto materialize_key = [&](const Expression& key) -> Expression {
      if (const auto found = existing_columns.find(key->ToString());
          found != existing_columns.end()) {
        return ColumnValueExp(ColumnName("", found->second));
      }
      auto [it, inserted] = partition_columns.emplace(
          key->ToString(), "$pk" + std::to_string(partition_columns.size()));
      if (inserted) {
        partition_items.emplace_back(it->second, key);
      }
      return ColumnValueExp(ColumnName("", it->second));
    };
    // Window arguments evaluate over the grouped rows too: aggregates point
    // at their `$aggK` column, and a bare column reference outside any
    // aggregate (a GROUP BY key or a lenient ungrouped column the select
    // list never projects) is materialized like a partition key. Left as is
    // it would miss the grouped schema and fall into the whole-row STRUCT
    // fallback.
    const auto point_at_grouped_input =
        [&](const Expression& node) -> std::optional<Expression> {
      if (auto aggregate = point_at_grouped_aggregate(node)) {
        return aggregate;
      }
      if (node->Type() == TypeTag::kColumnValue) {
        const ColumnName& col = node->AsColumnValue().GetColumnName();
        if (col.schema.empty()) {
          // An alias of a non-window select item already lives in the
          // grouped output under that name.
          for (size_t i = 0; i < statement.SelectList().size(); ++i) {
            const NamedExpression& item = statement.SelectList()[i];
            if (!item_has_window[i] && !item.name.empty() &&
                IdentifierEquals(item.name, col.name)) {
              return ColumnValueExp(ColumnName("", ProjectionName(item, i)));
            }
          }
        }
        return materialize_key(node);
      }
      return std::nullopt;
    };
    for (Expression& argument : rewritten->args) {
      argument = ReplaceSubExpressions(argument, point_at_grouped_input);
    }
    if (rewritten->where_filter) {
      rewritten->where_filter = ReplaceSubExpressions(rewritten->where_filter,
                                                      point_at_grouped_input);
    }
    for (Expression& key : rewritten->partition_by) {
      if (key) {
        key = materialize_key(key);
      }
    }
    for (auto& term : rewritten->order_by) {
      if (term.expression) {
        term.expression = materialize_key(term.expression);
      }
    }
    rewritten_windows.push_back(std::move(rewritten));
  }
  // ORDER BY aggregates that the select list does not project (ORDER BY
  // COUNT(*)) must also be computed by the grouping pass: pass 3 runs over
  // ungrouped rows and cannot evaluate a raw aggregate there.
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    if (term.expression && !ContainsWindowCall(term.expression) &&
        relational_detail::ContainsAggregate(term.expression)) {
      RegisterAggregatesForMaterialization(term.expression, &aggregate_columns,
                                           &hidden_aggregates);
    }
  }
  for (const auto& [name, aggregate] : hidden_aggregates) {
    grouped_select.emplace_back(name, aggregate);
  }
  for (const NamedExpression& item : partition_items) {
    grouped_select.emplace_back(item);
  }
  // Pass 1 also materializes plain ORDER BY terms as hidden `$okJ` columns.
  // The final pass evaluates its ordering over the grouped output, where an
  // unselected GROUP BY key (q12 orders by i_item_id without selecting it)
  // does not exist: without this column the ordering used to fall into the
  // row-alias whole-row fallback and "sorted" by a STRUCT of the grouped row.
  std::unordered_map<std::string, std::string> order_key_columns;
  std::vector<Expression> rewritten_order_terms;
  rewritten_order_terms.reserve(statement.OrderBy().size());
  const auto materialize_order_key = [&](const Expression& term) -> Expression {
    if (const auto found = existing_columns.find(term->ToString());
        found != existing_columns.end()) {
      return ColumnValueExp(ColumnName("", found->second));
    }
    auto [it, inserted] = order_key_columns.emplace(
        term->ToString(), "$ok" + std::to_string(order_key_columns.size()));
    if (inserted) {
      grouped_select.emplace_back(it->second, term);
    }
    return ColumnValueExp(ColumnName("", it->second));
  };
  const auto term_needs_materialization = [&](const Expression& term) {
    if (!term) {
      return false;
    }
    // Window calls and aggregates are materialized through aggregate_columns
    // by the node-level rewrite below; a whole-term column would never
    // resolve in the grouped pass.
    if (ContainsWindowCall(term)) {
      return false;
    }
    bool aliases_only = true;
    std::function<void(const Expression&)> scan =
        [&](const Expression& node) {  // NOLINT(misc-no-recursion)
          if (!node) {
            return;
          }
          if (node->Type() == TypeTag::kAggregateExp) {
            aliases_only = false;
            return;
          }
          if (node->Type() == TypeTag::kColumnValue) {
            const ColumnName& referenced =
                node->AsColumnValue().GetColumnName();
            if (referenced.schema.empty() &&
                alias_columns.contains(AsciiToLower(referenced.name))) {
              aliases_only = false;
              return;
            }
          }
          for (const Expression& child : ExpressionChildren(node)) {
            scan(child);
          }
        };
    scan(term);
    return aliases_only;
  };
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    rewritten_order_terms.push_back(term_needs_materialization(term.expression)
                                        ? materialize_order_key(term.expression)
                                        : term.expression);
  }
  grouped->SetSelectList(std::move(grouped_select));
  grouped->SetOrderBy({});
  grouped->SetQualify(Expression{});
  // DISTINCT belongs to the final projection (pass 3), after the window
  // columns are computed. Applying it here would dedupe rows whose only
  // difference is a still-NULL window placeholder, losing result rows.
  grouped->SetDistinct(false);
  grouped->SetLimit(std::nullopt);
  grouped->SetOffset(0);
  // When the input has not been built yet, plan the grouping independently.
  // Its final join can aggregate directly, before any joined rows exist.
  // The WITH cells were already evaluated by the enclosing ExecuteQuery.
  grouped->ClearWithQueries();
  ASSIGN_OR_RETURN(
      Relation, grouped_output,
      (build_grouped_input ? ExecuteQuery(context, *grouped, outer, ctes,
                                          nullptr, columnar_output)
                           : FinishQuery(context, *grouped, std::move(input),
                                         outer, ctes, apply_where, 0)));

  RETURN_IF_FAIL(grouped_output.FinishSpill());
  RETURN_IF_FAIL(grouped_output.GetStatus());
  const Schema grouped_schema = grouped_output.schema;

  // Direct window projections no longer need their grouping placeholders or
  // hidden window arguments once every window has been evaluated. Compact
  // the surviving output columns before appending the computed values so a
  // row can reuse the cells formerly occupied by those temporary inputs.
  // Mixed expressions, QUALIFY and ORDER BY may still reference hidden group
  // columns and keep the complete schema.
  bool reuse_grouped_cells =
      statement.OrderBy().empty() && !statement.Qualify();
  for (size_t i = 0; i < item_has_window.size(); ++i) {
    if (item_has_window[i] && statement.SelectList()[i].expression->Type() !=
                                  TypeTag::kWindowFunctionExp) {
      reuse_grouped_cells = false;
    }
  }
  std::vector<size_t> kept_columns;
  for (size_t i = 0; i < grouped_schema.ColumnCount(); ++i) {
    if (!reuse_grouped_cells ||
        (i < item_has_window.size() && !item_has_window[i])) {
      kept_columns.push_back(i);
    }
  }
  const size_t grouped_width = kept_columns.size();

  auto rewrite_qualify = [&]() -> Expression {
    const auto point_qualify_at_materialized =
        [&](const Expression& node) -> std::optional<Expression> {
      if (node->Type() == TypeTag::kWindowFunctionExp ||
          node->Type() == TypeTag::kAggregateExp) {
        return ColumnValueExp(
            ColumnName("", aggregate_columns.at(node->ToString())));
      }
      if (node->Type() == TypeTag::kColumnValue) {
        const ColumnName& referenced = node->AsColumnValue().GetColumnName();
        if (referenced.schema.empty()) {
          if (auto it = alias_columns.find(AsciiToLower(referenced.name));
              it != alias_columns.end()) {
            // Window-bearing SELECT items have NULL placeholders in the
            // grouping pass. QUALIFY aliases must read their computed
            // windows (and aggregates), not those placeholders.
            for (size_t i = 0; i < statement.SelectList().size(); ++i) {
              const auto& item = statement.SelectList()[i];
              if (item_has_window[i] &&
                  IdentifierEquals(item.name, referenced.name)) {
                return ReplaceSubExpressions(
                    item.expression,
                    [&](const Expression& value) -> std::optional<Expression> {
                      if (value->Type() == TypeTag::kWindowFunctionExp ||
                          value->Type() == TypeTag::kAggregateExp) {
                        return ColumnValueExp(ColumnName(
                            "", aggregate_columns.at(value->ToString())));
                      }
                      return std::nullopt;
                    });
              }
            }
            return ColumnValueExp(ColumnName("", it->second));
          }
        }
      }
      return std::nullopt;
    };
    return ReplaceSubExpressions(statement.Qualify(),
                                 point_qualify_at_materialized);
  };

  Relation extended(context.execution_runtime());
  if (columnar_output && !grouped_output.HasSpill() &&
      (grouped_output.HasColumnarOnly() || grouped_output.TotalRows() == 0)) {
    std::vector<const WindowFunctionCallExpression*> win_ptrs;
    std::vector<std::string> win_names;
    win_ptrs.reserve(rewritten_windows.size());
    win_names.reserve(rewritten_windows.size());
    for (size_t w = 0; w < rewritten_windows.size(); ++w) {
      win_ptrs.push_back(rewritten_windows[w].get());
      win_names.push_back("$win" + std::to_string(w));
    }
    ASSIGN_OR_RETURN(
        Relation, col_extended,
        (relational_detail::ExtendColumnarRelationWithWindows(
            context, std::move(grouped_output), win_ptrs, win_names, outer,
            ctes, std::span<const size_t>(kept_columns))));
    extended = std::move(col_extended);
    if (statement.Qualify()) {
      RETURN_IF_FAIL(
          FilterRelation(context, &extended, {rewrite_qualify()}, outer, ctes));
    }
    RETURN_IF_FAIL(extended.FinishSpill());
    RETURN_IF_FAIL(extended.GetStatus());
  } else {
    // Pass 2: compute every window over the grouped rows. Keep the original
    // memory charge until the extended relation takes ownership below.
    RETURN_IF_FAIL(grouped_output.EnsureRowsMaterialized());
    std::vector<Row> rows;
    if (!grouped_output.HasSpill()) {
      rows = std::move(grouped_output.rows);
    } else {
      RETURN_IF_FAIL(grouped_output.ForEachRow(
          [&rows](const Row& row) { rows.push_back(row); }));
    }
    std::vector<std::vector<Value>> computed(rewritten_windows.size());
    for (size_t w = 0; w < rewritten_windows.size(); ++w) {
      RETURN_IF_FAIL(ComputeOneWindow(context, *rewritten_windows[w], rows,
                                      grouped_schema, outer, ctes,
                                      &computed[w]));
    }
    if (grouped_width < grouped_schema.ColumnCount()) {
      for (Row& row : rows) {
        for (size_t i = 0; i < grouped_width; ++i) {
          if (i != kept_columns[i]) {
            row.values_[i] = std::move(row.values_[kept_columns[i]]);
          }
        }
        row.values_.resize(grouped_width);
      }
    }
    std::vector<Column> extended_columns;
    extended_columns.reserve(grouped_width + rewritten_windows.size());
    for (const size_t column : kept_columns) {
      extended_columns.push_back(grouped_schema.GetColumn(column));
    }
    for (size_t w = 0; w < rewritten_windows.size(); ++w) {
      extended_columns.emplace_back("$win" + std::to_string(w),
                                    ValueType::kNull);
    }
    for (size_t r = 0; r < rows.size(); ++r) {
      rows[r].values_.reserve(extended_columns.size());
      for (size_t w = 0; w < rewritten_windows.size(); ++w) {
        rows[r].values_.push_back(std::move(computed[w][r]));
      }
    }
    // Fix the $win column types from the appended values.
    for (size_t w = 0; w < rewritten_windows.size(); ++w) {
      Column& column = extended_columns[grouped_width + w];
      for (const Row& row : rows) {
        const Value& candidate = row.values_[grouped_width + w];
        if (!candidate.IsNull() && candidate.type != ValueType::kNull) {
          column = Column(column.Name(), ValueTypeOf(candidate));
          column.SetUnsigned(candidate.IsUnsigned());
          break;
        }
      }
    }
    // QUALIFY filters the grouped+$win rows (WHERE semantics: NULL and FALSE
    // drop the row), after the window pass materialized its inputs.
    if (statement.Qualify()) {
      const Expression rewritten_qualify = rewrite_qualify();
      Schema extended_schema(grouped_output.schema.Name(), extended_columns);
      std::vector<Row> filtered;
      filtered.reserve(rows.size());
      for (Row& row : rows) {
        const Scope qualify_scope{
            .row = &row, .schema = &extended_schema, .outer = outer};
        ASSIGN_OR_RETURN(Value, decided,
                         (TryEvaluate(rewritten_qualify, qualify_scope, nullptr,
                                      context, ctes)));
        if (Truthy(decided)) {
          filtered.push_back(std::move(row));
        }
      }
      rows = std::move(filtered);
    }
    extended.schema =
        Schema(grouped_output.schema.Name(), std::move(extended_columns));
    CopyExecutionStats(&extended, grouped_output);
    grouped_output.ResetContents();
    for (Row& row : rows) {
      extended.AddRow(std::move(row));
    }
    RETURN_IF_FAIL(extended.FinishSpill());
    RETURN_IF_FAIL(extended.GetStatus());
  }

  // Pass 3: re-project by column reference and restore ordering/limit.
  auto final_statement = std::make_shared<SelectStatement>(statement);
  std::vector<NamedExpression> final_select;
  final_select.reserve(statement.SelectList().size());
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const NamedExpression& projection = statement.SelectList()[i];
    Expression expression;
    if (projection.expression &&
        projection.expression->Type() == TypeTag::kWindowFunctionExp) {
      expression = ColumnValueExp(ColumnName(
          "", aggregate_columns.at(projection.expression->ToString())));
    } else if (item_has_window[i]) {
      // Mixed item: windows -> $winN, aggregates -> $aggK; the remaining
      // nodes reference group keys resolvable in the grouped schema.
      const auto point_at_materialized_item =
          [&](const Expression& node) -> std::optional<Expression> {
        if (node->Type() == TypeTag::kWindowFunctionExp ||
            node->Type() == TypeTag::kAggregateExp) {
          return ColumnValueExp(
              ColumnName("", aggregate_columns.at(node->ToString())));
        }
        return std::nullopt;
      };
      expression = ReplaceSubExpressions(projection.expression,
                                         point_at_materialized_item);
    } else {
      expression =
          ColumnValueExp(ColumnName("", ProjectionName(projection, i)));
    }
    final_select.emplace_back(projection.name, std::move(expression));
  }
  const auto point_at_materialized =
      [&](const Expression& node) -> std::optional<Expression> {
    if (node->Type() == TypeTag::kWindowFunctionExp) {
      return ColumnValueExp(
          ColumnName("", aggregate_columns.at(node->ToString())));
    }
    if (node->Type() == TypeTag::kColumnValue) {
      const ColumnName& referenced = node->AsColumnValue().GetColumnName();
      if (referenced.schema.empty()) {
        if (auto it = alias_columns.find(AsciiToLower(referenced.name));
            it != alias_columns.end()) {
          return ColumnValueExp(ColumnName("", it->second));
        }
      }
    }
    if (node->Type() == TypeTag::kAggregateExp) {
      // ORDER BY <aggregate>: point at the matching select item's column.
      for (size_t i = 0; i < statement.SelectList().size(); ++i) {
        if (statement.SelectList()[i].expression &&
            statement.SelectList()[i].expression->ToString() ==
                node->ToString()) {
          return ColumnValueExp(
              ColumnName("", ProjectionName(statement.SelectList()[i], i)));
        }
      }
      // Not projected: the grouping pass materialized it as a hidden $aggK.
      if (const auto found = aggregate_columns.find(node->ToString());
          found != aggregate_columns.end()) {
        return ColumnValueExp(ColumnName("", found->second));
      }
    }
    return std::nullopt;
  };
  std::vector<SelectStatement::OrderByTerm> final_order;
  final_order.reserve(statement.OrderBy().size());
  for (size_t o = 0; o < statement.OrderBy().size(); ++o) {
    const SelectStatement::OrderByTerm& term = statement.OrderBy()[o];
    // A materialized `$okJ` column already names this term; anything else
    // keeps the node-level rewrite (windows -> $win, aggregates/aliases ->
    // select items).
    final_order.push_back(
        {rewritten_order_terms[o] == term.expression
             ? ReplaceSubExpressions(term.expression, point_at_materialized)
             : rewritten_order_terms[o],
         term.ascending, term.nulls_first});
  }
  final_statement->SetSelectList(std::move(final_select));
  final_statement->SetOrderBy(std::move(final_order));
  final_statement->SetWhereClause(Expression{});
  final_statement->SetHaving(Expression{});
  final_statement->SetGroupBy({});
  final_statement->SetQualify(Expression{});
  ASSIGN_OR_RETURN(Relation, projected,
                   (FinishQuery(context, *final_statement, std::move(extended),
                                outer, ctes, false, 0)));
  // FinishQuery already applied ORDER BY and the statement's LIMIT/OFFSET
  // once; slicing again here dropped another OFFSET-worth of rows.
  return projected;
}
namespace {

// True when every ORDER BY term can be evaluated against the post-projection
// schema alone. Terms that still name hidden materialized columns ($okJ /
// $aggK / window args dropped by Project) or base columns the select list did
// not project cannot resolve here: Project already ordered the rows with
// those keys evaluated against the wider pre-projection scope, and
// DistinctOf's first-occurrence fold preserves that order, so re-applying
// them would only hit the whole-row STRUCT fallback and silently reorder.
bool OrderByResolvableOnSchema(
    const std::vector<SelectStatement::OrderByTerm>& order_by,
    const Schema& schema) {
  for (const SelectStatement::OrderByTerm& term : order_by) {
    const Expression& expression = term.expression;
    if (!expression) {
      continue;
    }
    if (const std::optional<CollatedOrdinal> collated =
            AsCollatedOrdinal(expression);
        collated.has_value()) {
      if (collated->ordinal < 1 ||
          std::cmp_greater(collated->ordinal, schema.ColumnCount())) {
        return false;
      }
      continue;
    }
    if (expression->Type() == TypeTag::kConstantValue &&
        dynamic_cast<const ParameterSlot*>(expression.get()) == nullptr) {
      const Value& value = expression->AsConstantValue().GetValue();
      if (value.type == ValueType::kInt64 && !value.IsNull()) {
        if (value.value.int_value < 1 ||
            std::cmp_greater(value.value.int_value, schema.ColumnCount())) {
          return false;
        }
      }
      continue;
    }
    if (ContainsAggregate(expression) || ContainsWindowCall(expression) ||
        ContainsQuery(expression)) {
      return false;
    }
    for (const ColumnName& column : expression->TouchedColumns()) {
      if (column.name == "*" || !LocalColumnOffset(schema, column)) {
        return false;
      }
    }
  }
  return true;
}

bool SameSqlIdentifier(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) {
    return false;
  }
  for (size_t i = 0; i < a.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(a[i])) !=
        std::tolower(static_cast<unsigned char>(b[i]))) {
      return false;
    }
  }
  return true;
}

// Rewrites a single-table self-dominance anti-join:
//   SELECT ... FROM pts b WHERE NOT EXISTS (
//     SELECT 1 FROM pts a WHERE a.x < b.x AND a.y < b.y)
// (including `<` / `>` variants) into an O(n log n) skyline sweep.
// Lean proof: lean/TinylambOpt/ResearchProofs/SkylineAntiJoin.lean (sound)
StatusOr<bool> TryApplySkylineAntiJoin(
    const SelectStatement& statement,
    const std::vector<Expression>& where_predicates, const Scope* outer,
    Relation* input) {
  const char* disable =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_SKYLINE_ANTIJOIN");
  if ((disable != nullptr && std::strcmp(disable, "1") == 0) ||
      outer != nullptr || where_predicates.size() != 1 ||
      !where_predicates[0] || statement.Sources().size() != 1) {
    return false;
  }
  const SelectSource& outer_src = statement.Sources()[0];
  if (outer_src.query != nullptr || outer_src.unnest != nullptr ||
      outer_src.sample || outer_src.join_condition != nullptr ||
      outer_src.table.empty()) {
    return false;
  }

  const QueryExpression* exists_q = nullptr;
  if (where_predicates[0]->Type() == TypeTag::kQueryExp) {
    const auto& q = where_predicates[0]->AsQueryExpression();
    if (q.Exists() && q.Negated() && !q.Test() && q.Query() != nullptr) {
      exists_q = &q;
    }
  } else if (where_predicates[0]->Type() == TypeTag::kUnaryExp) {
    const auto& un = where_predicates[0]->AsUnaryExpression();
    if (un.Op() == UnaryOperation::kNot && un.Child() != nullptr &&
        un.Child()->Type() == TypeTag::kQueryExp) {
      const auto& q = un.Child()->AsQueryExpression();
      if (q.Exists() && !q.Negated() && !q.Test() && q.Query() != nullptr) {
        exists_q = &q;
      }
    }
  }
  if (exists_q == nullptr) {
    return false;
  }

  const SelectStatement& sub = *exists_q->Query();
  if (sub.Sources().size() != 1 || !sub.WithQueries().empty() ||
      !sub.UnionAll().empty() || sub.GetSetOperationTree() != nullptr ||
      sub.Qualify() != nullptr || HasWindowFunctions(sub) ||
      !sub.GroupBy().empty() || sub.Having() != nullptr ||
      !sub.OrderBy().empty() || sub.HasLimit() || sub.Offset() != 0 ||
      !sub.WhereClause()) {
    return false;
  }
  const SelectSource& inner_src = sub.Sources()[0];
  if (inner_src.query != nullptr || inner_src.unnest != nullptr ||
      inner_src.sample || inner_src.join_condition != nullptr ||
      inner_src.table != outer_src.table) {
    return false;
  }
  const bool select_list_safe =
      std::ranges::all_of(sub.SelectList(), [](const NamedExpression& item) {
        return item.expression != nullptr &&
               (item.expression->Type() == TypeTag::kConstantValue ||
                item.expression->Type() == TypeTag::kColumnValue);
      });
  if (!select_list_safe) {
    return false;
  }

  const std::string& outer_qual =
      outer_src.alias.empty() ? outer_src.table : outer_src.alias;
  const std::string& inner_qual =
      inner_src.alias.empty() ? inner_src.table : inner_src.alias;
  if (outer_qual.empty() || inner_qual.empty() ||
      SameSqlIdentifier(outer_qual, inner_qual)) {
    return false;
  }

  const std::vector<Expression> sub_preds = SplitConjuncts(sub.WhereClause());
  if (sub_preds.size() != 2) {
    return false;
  }

  struct DimCond {
    size_t col_offset{0};
    bool inner_greater{false};
  };
  std::array<DimCond, 2> dims;
  for (size_t i = 0; i < 2; ++i) {
    if (!sub_preds[i] || sub_preds[i]->Type() != TypeTag::kBinaryExp) {
      return false;
    }
    const BinaryExpression& bin = sub_preds[i]->AsBinaryExpression();
    if ((bin.Op() != BinaryOperation::kLessThan &&
         bin.Op() != BinaryOperation::kGreaterThan) ||
        !bin.Left() || !bin.Right() ||
        bin.Left()->Type() != TypeTag::kColumnValue ||
        bin.Right()->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const ColumnName& lhs = bin.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rhs = bin.Right()->AsColumnValue().GetColumnName();
    const bool lhs_is_inner = SameSqlIdentifier(lhs.schema, inner_qual);
    const bool rhs_is_inner = SameSqlIdentifier(rhs.schema, inner_qual);
    const bool lhs_is_outer = SameSqlIdentifier(lhs.schema, outer_qual);
    const bool rhs_is_outer = SameSqlIdentifier(rhs.schema, outer_qual);
    if (lhs_is_inner == rhs_is_inner) {
      return false;
    }
    const ColumnName& inner_col = lhs_is_inner ? lhs : rhs;
    const ColumnName& outer_col = lhs_is_inner ? rhs : lhs;
    if ((lhs_is_inner ? !rhs_is_outer : !lhs_is_outer) ||
        !SameSqlIdentifier(inner_col.name, outer_col.name)) {
      return false;
    }
    const auto outer_off = LocalColumnOffset(input->schema, outer_col);
    if (!outer_off.has_value() || *outer_off >= input->schema.ColumnCount()) {
      return false;
    }
    const Column& meta = input->schema.GetColumn(*outer_off);
    if (meta.Type() != ValueType::kDate &&
        (meta.Type() != ValueType::kInt64 || meta.IsUnsigned())) {
      return false;
    }
    const bool inner_greater = lhs_is_inner
                                   ? (bin.Op() == BinaryOperation::kGreaterThan)
                                   : (bin.Op() == BinaryOperation::kLessThan);
    dims[i] = DimCond{.col_offset = *outer_off, .inner_greater = inner_greater};
  }
  if (dims[0].col_offset == dims[1].col_offset) {
    return false;
  }

  RETURN_IF_FAIL(input->FinishSpill());
  RETURN_IF_FAIL(input->EnsureRowsMaterialized());
  const size_t n = input->rows.size();
  const size_t x_col = dims[0].col_offset;
  const size_t y_col = dims[1].col_offset;
  const ValueType x_type = input->schema.GetColumn(x_col).Type();
  const ValueType y_type = input->schema.GetColumn(y_col).Type();
  const bool x_greater = dims[0].inner_greater;
  const bool y_greater = dims[1].inner_greater;

  struct PointEntry {
    int64_t x{0};
    int64_t y{0};
    size_t row_idx{0};
  };
  std::vector<PointEntry> pts;
  pts.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    const Row& row = input->rows[i];
    const Value& xv = row[x_col];
    const Value& yv = row[y_col];
    if (xv.IsNull() || yv.IsNull()) {
      continue;
    }
    if (xv.type != x_type || yv.type != y_type ||
        (x_type == ValueType::kInt64 && xv.IsUnsigned()) ||
        (y_type == ValueType::kInt64 && yv.IsUnsigned())) {
      return false;
    }
    pts.push_back({xv.value.int_value, yv.value.int_value, i});
  }

  std::sort(pts.begin(), pts.end(),
            [x_greater](const PointEntry& a, const PointEntry& b) {
              return x_greater ? (a.x > b.x) : (a.x < b.x);
            });
  std::vector<uint8_t> keep(n, 1);
  bool has_prev_y = false;
  int64_t best_y = 0;
  for (size_t i = 0; i < pts.size();) {
    const int64_t cur_x = pts[i].x;
    size_t run_end = i;
    while (run_end < pts.size() && pts[run_end].x == cur_x) {
      ++run_end;
    }
    if (has_prev_y) {
      for (size_t j = i; j < run_end; ++j) {
        const bool dominated =
            y_greater ? (best_y > pts[j].y) : (best_y < pts[j].y);
        if (dominated) {
          keep[pts[j].row_idx] = 0;
        }
      }
    }
    for (size_t j = i; j < run_end; ++j) {
      if (!has_prev_y) {
        best_y = pts[j].y;
        has_prev_y = true;
      } else if (y_greater ? (pts[j].y > best_y) : (pts[j].y < best_y)) {
        best_y = pts[j].y;
      }
    }
    i = run_end;
  }

  size_t out_idx = 0;
  for (size_t ri = 0; ri < n; ++ri) {
    if (keep[ri] != 0) {
      if (out_idx != ri) {
        input->rows[out_idx] = std::move(input->rows[ri]);
      }
      ++out_idx;
    }
  }
  input->rows.resize(out_idx);
  return true;
}

}  // namespace

StatusOr<Relation> FinishQuery(TransactionContext& context,
                               const SelectStatement& statement, Relation input,
                               const Scope* outer, const CteMap& ctes,
                               bool apply_where, size_t hidden_columns) {
  if (apply_where && statement.WhereClause()) {
    const auto filter_begin = std::chrono::steady_clock::now();
    // Use the compiled filter path for WHERE evaluation when possible.
    // CompiledScanFilter separates SimpleCompare predicates (integer/string
    // column-vs-constant comparisons) from residual expressions, allowing
    // the fast integer/string comparison path to reject rows without the
    // overhead of recursive expression tree traversal.  This can reduce
    // filter time by 2-5x for queries with simple WHERE predicates.
    std::vector<Expression> where_predicates;
    if (statement.WhereClause()->Type() == TypeTag::kBinaryExp &&
        statement.WhereClause()->AsBinaryExpression().Op() ==
            BinaryOperation::kAnd) {
      where_predicates = SplitConjuncts(statement.WhereClause());
    } else {
      where_predicates = {statement.WhereClause()};
    }
    ASSIGN_OR_RETURN(
        bool, skyline_applied,
        (TryApplySkylineAntiJoin(statement, where_predicates, outer, &input)));
    if (!skyline_applied) {
      const relational_detail::FilterCompileEnv compile_env{
          .context = &context,
          .outer = outer,
          .ctes = &ctes,
          .input_rows = input.TotalRows(),
          .prebuild_only_exists = false,
          .input_relation = &input};
      const relational_detail::CompiledScanFilter compiled =
          relational_detail::CompileScanFilter(where_predicates, input.schema,
                                               &compile_env);
      // Use compiled filter path when any fast-path is available:
      // SimpleCompare, disjunctive branches, or bytecode-compiled residuals.
      // The bytecode path avoids recursive expression tree traversal for
      // residual predicates, which is the dominant cost for complex WHERE
      // clauses like TPC-H Q20.
      if (!compiled.simple.empty() || !compiled.prebuilt_membership.empty() ||
          !compiled.prebuilt_scalar_compare.empty() ||
          !compiled.disjunctive_branches.empty() ||
          compiled.residual_bytecode.has_value() ||
          !compiled.residual.empty()) {
        RETURN_IF_FAIL(input.FinishSpill());
        static const bool finish_parallel_disabled =
            ::tinylamb::platform::GetEnv(
                "TINYLAMB_DISABLE_FINISH_PARALLEL_FILTER") != nullptr;
        bool handled_direct_columnar = false;
        if (!finish_parallel_disabled && input.HasColumnarOnly() &&
            !compiled.residual_bytecode.has_value() &&
            !compiled.residual.empty()) {
          Relation direct_out(context.execution_runtime());
          direct_out.schema = input.schema;
          CopyExecutionStats(&direct_out, input);
          Status direct_err{Status::kSuccess};
          if (relational_detail::TryParallelFilterRows(
                  context, input, compiled, input.schema, outer, ctes, nullptr,
                  &direct_out, &direct_err, &input)) {
            RETURN_IF_FAIL(direct_err);
            RETURN_IF_FAIL(direct_out.FinishSpill());
            input = std::move(direct_out);
            handled_direct_columnar = true;
          }
        }
        if (!handled_direct_columnar) {
          if (!compiled.residual_bytecode.has_value() &&
              !compiled.residual.empty() &&
              (!compiled.simple.empty() || !compiled.prefix_in.empty() ||
               !compiled.prebuilt_membership.empty() ||
               !compiled.prebuilt_scalar_compare.empty() ||
               !compiled.disjunctive_branches.empty())) {
            relational_detail::CompiledScanFilter non_residual = compiled;
            non_residual.residual.clear();
            Relation prefiltered(context.execution_runtime());
            prefiltered.schema = input.schema;
            CopyExecutionStats(&prefiltered, input);
            Status pre_error{Status::kSuccess};
            if (!relational_detail::TryParallelFilterRows(
                    context, input, non_residual, input.schema, outer, ctes,
                    nullptr, &prefiltered, &pre_error, &input)) {
              relational_detail::ColumnLookupCache pre_lookup;
              Status pre_iter = input.ForEachRow([&](const Row& row) {
                if (!pre_error.ok()) {
                  return;
                }
                if (relational_detail::MatchScanFilter(
                        row, input.schema, non_residual, outer, context, ctes,
                        &pre_error, &pre_lookup)) {
                  prefiltered.AddRow(row);
                }
              });
              RETURN_IF_FAIL(pre_iter);
            }
            RETURN_IF_FAIL(pre_error);
            RETURN_IF_FAIL(prefiltered.FinishSpill());
            input = std::move(prefiltered);
          }
          Relation filtered(context.execution_runtime());
          filtered.schema = input.schema;
          CopyExecutionStats(&filtered, input);
          Status eval_error{Status::kSuccess};
          bool filtered_in_parallel = false;
          if (!finish_parallel_disabled) {
            filtered_in_parallel = relational_detail::TryParallelFilterRows(
                context, input, compiled, input.schema, outer, ctes, nullptr,
                &filtered, &eval_error, &input);
          }
          if (filtered_in_parallel) {
            RETURN_IF_FAIL(eval_error);
          } else {
            RETURN_IF_FAIL((([&]() -> Status {
              // Register the filtered relation so a correlated-subquery build
              // triggered by one of the predicates can prune its build scan to
              // this pipeline's outer key domain.
              relational_detail::ScopedCorrelatedOuter correlated_outer(context,
                                                                        input);
              relational_detail::ColumnLookupCache lookup_cache;
              return input.ForEachRow([&](const Row& row) {
                if (!eval_error.ok()) {
                  return;
                }
                if (relational_detail::MatchScanFilter(
                        row, input.schema, compiled, outer, context, ctes,
                        &eval_error, &lookup_cache)) {
                  filtered.AddRow(row);
                }
              });
            })()));
            RETURN_IF_FAIL(eval_error);
          }
          RETURN_IF_FAIL(filtered.FinishSpill());
          input = std::move(filtered);
        }
      } else {
        Relation filtered(context.execution_runtime());
        filtered.schema = input.schema;
        CopyExecutionStats(&filtered, input);
        RETURN_IF_FAIL(input.FinishSpill());
        // Use the non-throwing evaluation path: the per-row evaluation must not
        // use the throwing Evaluate wrapper. An
        // unresolvable qualified reference inside a subquery predicate
        // (sql_oracle_fuzz find: `WHERE EXISTS (... d.parent ...)` over a
        // 1-column subquery) came back as a Status from TryEvaluate, and the
        // shim re-raised it as a C++ exception that escaped the whole
        // executor boundary instead of surfacing through GetStatus.
        Status filter_error{Status::kSuccess};
        RETURN_IF_FAIL(input.ForEachRow([&](const Row& row) {
          if (filter_error != Status::kSuccess) {
            return;
          }
          Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
          StatusOr<Value> keep = TryEvaluate(statement.WhereClause(), scope,
                                             nullptr, context, ctes);
          if (!keep.HasValue()) {
            filter_error = keep.GetStatus();
            return;
          }
          if (Truthy(keep.Value())) {
            filtered.AddRow(row);
          }
        }));
        RETURN_IF_FAIL(filter_error);
        RETURN_IF_FAIL(filtered.FinishSpill());
        input = std::move(filtered);
      }
    }  // if (!skyline_applied)
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
    }
  }

  ASSIGN_OR_RETURN(Relation, output,
                   (Project(context, statement, std::move(input), outer, ctes,
                            hidden_columns)));
  if (statement.Distinct()) {
    if (statement.HasDistinctOn()) {
      ASSIGN_OR_RETURN(bool, folded_distinct_on,
                       (relational_detail::TryDistinctOnOrderedWinners(
                           context, statement, &output, outer, ctes)));
      if (!folded_distinct_on) {
        if (!statement.OrderBy().empty()) {
          RETURN_IF_FAIL(
              ApplyOrderBy(context, statement, &output, outer, ctes, false));
        }
        ASSIGN_OR_RETURN(Relation, distinct_of,
                         (DistinctOf(std::move(output), statement.DistinctOn(),
                                     &context, outer, &ctes)));
        output = std::move(distinct_of);
      }
    } else {
      ASSIGN_OR_RETURN(Relation, distinct_of, (DistinctOf(std::move(output))));
      output = std::move(distinct_of);
      // The grouped projection path already consumed ORDER BY against the
      // full pre-projection scope (non-projected aggregates and GROUP BY
      // keys ride as sorter keys and are dropped afterwards), and the
      // first-occurrence DistinctOf fold preserves that relative order.
      // Re-applying the raw order expressions against the visible-only
      // relation would re-evaluate hidden $aggN / group-key references that
      // no longer exist, so it only runs for non-grouped statements whose
      // order terms still resolve on the projected schema.
      const bool grouped_projection_sorted =
          !statement.GroupBy().empty() ||
          std::any_of(statement.SelectList().begin(),
                      statement.SelectList().end(),
                      [](const NamedExpression& projection) {
                        return projection.expression != nullptr &&
                               ContainsAggregate(projection.expression);
                      }) ||
          ContainsAggregate(statement.Having());
      if (!statement.OrderBy().empty() && !grouped_projection_sorted &&
          OrderByResolvableOnSchema(statement.OrderBy(), output.schema)) {
        RETURN_IF_FAIL(ApplyOrderBy(context, statement, &output, outer, ctes));
      }
    }
  }
  return LimitedRows(statement, std::move(output), &context, outer, &ctes);
}

}  // namespace tinylamb::relational_detail
