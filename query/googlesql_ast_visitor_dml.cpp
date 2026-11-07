/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/googlesql_ast_visitor.hpp"

// NOLINTNEXTLINE(modernize-deprecated-headers) POSIX timegm/gmtime_r
#include <time.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <deque>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/set_operation.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/window_eval.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/collation.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/proto_schema.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/sql_udf.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/googlesql_ast.hpp"
#include "query/googlesql_ast_visitor_internal.hpp"
#include "query/statement.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/date.hpp"
#include "type/interval.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::ast_visitor_detail {

bool ContainsAggregate(const Expression& expression) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kAggregateExp:
      return true;
    case TypeTag::kBinaryExp:
      return ContainsAggregate(expression->AsBinaryExpression().Left()) ||
             ContainsAggregate(expression->AsBinaryExpression().Right());
    case TypeTag::kUnaryExp:
      return ContainsAggregate(expression->AsUnaryExpression().Child());
    case TypeTag::kCaseExp: {
      const auto& val = expression->AsCaseExpression();
      for (const auto& [c, r] : val.when_clauses_) {
        if (ContainsAggregate(c) || ContainsAggregate(r)) {
          return true;
        }
      }
      return ContainsAggregate(val.else_clause_);
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      return std::ranges::any_of(call.Args(), [](const Expression& arg) {
        return ContainsAggregate(arg);
      });
    }
    case TypeTag::kInExp: {
      const auto& in_exp = expression->AsInExpression();
      if (ContainsAggregate(in_exp.child_)) {
        return true;
      }
      return std::ranges::any_of(in_exp.list_, [](const Expression& item) {
        return ContainsAggregate(item);
      });
    }
    case TypeTag::kArrayExp: {
      const auto& arr = expression->AsArrayExpression();
      return std::ranges::any_of(arr.Elements(), [](const Expression& item) {
        return ContainsAggregate(item);
      });
    }
    default:
      return false;
  }
}

// ---------------------------------------------------------------------------
// GROUPING SETS / ROLLUP / CUBE surface.
//
// The pinned GoogleSQL parser emits Rollup / Cube / GroupingSetList nodes for
// `GROUP BY ROLLUP(..)`, `GROUP BY CUBE(..)` and `GROUP BY GROUPING SETS(..)`.
// tinylamb expands them at AST-visit time into a UNION ALL concatenation of
// plain GROUP BY branches (the `grouping_sets_to_union` strategy): every
// branch keeps the original FROM/WHERE, groups by one set's keys, NULL-pads
// select entries that reference keys outside the active set, and folds
// GROUPING() calls to per-branch constants.  Each branch therefore runs
// through the ordinary grouped-select pipeline (Cascades-optimized
// FROM+WHERE core + GroupByPlan), and the wrapper keeps the statement-level
// ORDER BY / LIMIT.
// ---------------------------------------------------------------------------

// Returns true when `expression` is a GROUPING(...) or GROUPING_ID(...) call,
// filling `args` and `is_grouping_id`.
bool IsGroupingFunctionCall(const Expression& expression,
                            std::vector<Expression>* args,
                            bool* is_grouping_id = nullptr) {
  if (!expression || expression->Type() != TypeTag::kFunctionCallExp) {
    return false;
  }
  const auto& call = expression->AsFunctionCallExpression();
  if (call.FuncName() != "grouping" && call.FuncName() != "grouping_id") {
    return false;
  }
  if (is_grouping_id != nullptr) {
    *is_grouping_id = call.FuncName() == "grouping_id";
  }
  *args = call.Args();
  return true;
}

bool ContainsGroupingCall(const Expression& expression) {
  if (!expression) {
    return false;
  }
  std::vector<Expression> grouping_args;
  if (IsGroupingFunctionCall(expression, &grouping_args)) {
    return true;
  }
  // Aggregates, window calls and subqueries own their scopes: GROUPING cannot
  // legally appear inside them, so the walk does not descend.
  switch (expression->Type()) {
    case TypeTag::kAggregateExp:
    case TypeTag::kWindowFunctionExp:
    case TypeTag::kQueryExp:
      return false;
    default:
      break;
  }
  return std::ranges::any_of(
      ExpressionChildren(expression),
      [](const Expression& child) { return ContainsGroupingCall(child); });
}

// One-branch rewrite of the GROUPING SETS expansion: folds GROUPING() calls
// to constants and replaces references to inactive grouping keys by NULL.
// Aggregates, window calls and subqueries keep their inner scope untouched
// (their inputs are pre-grouping row values, not grouped outputs).
StatusOr<Expression> RewriteGroupingSetBranch(
    const Expression& expression, const GroupingSetBranchContext& context) {
  if (!expression) {
    return expression;
  }
  std::vector<Expression> grouping_args;
  bool is_grouping_id = false;
  if (IsGroupingFunctionCall(expression, &grouping_args, &is_grouping_id)) {
    if (grouping_args.empty()) {
      return AstError<Expression>(
          is_grouping_id ? "GROUPING_ID requires at least one argument"
                         : "GROUPING requires at least one argument");
    }
    int64_t mask = 0;
    for (size_t i = 0; i < grouping_args.size(); ++i) {
      if (!grouping_args[i]) {
        return AstError<Expression>(is_grouping_id
                                        ? "GROUPING_ID argument is malformed"
                                        : "GROUPING argument is malformed");
      }
      const std::string arg_text = grouping_args[i]->ToString();
      if (std::ranges::find(context.all_key_texts, arg_text) ==
          context.all_key_texts.end()) {
        return AstError<Expression>(
            (is_grouping_id ? "GROUPING_ID" : "GROUPING") +
            std::string(" argument must appear in the GROUP BY list: ") +
            arg_text);
      }
      if (!context.active_key_texts.contains(arg_text)) {
        // GROUPING(x) is 1 when x is aggregated away. Multi-argument
        // GROUPING and GROUPING_ID pack the same flags into one integer
        // with the leftmost argument as the most significant bit
        // (GoogleSQL bit-vector order: GROUPING_ID(a, b) ==
        //  2*GROUPING(a) + GROUPING(b)).
        mask |= static_cast<int64_t>(1) << (grouping_args.size() - 1 - i);
      }
    }
    return ConstantValueExp(Value(mask));
  }
  switch (expression->Type()) {
    case TypeTag::kAggregateExp:
    case TypeTag::kWindowFunctionExp:
    case TypeTag::kQueryExp:
      return expression;
    default:
      break;
  }
  const std::string text = expression->ToString();
  if (context.active_key_texts.contains(text)) {
    // An active grouping key survives as a whole; descending further could
    // wrongly null an inactive key nested inside an active composite key.
    return expression;
  }
  if (std::ranges::find(context.all_key_texts, text) !=
      context.all_key_texts.end()) {
    // Inactive grouping key: the branch NULL-pads this output.
    return ConstantValueExp(Value());
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  bool changed = false;
  for (Expression& child : children) {
    ASSIGN_OR_RETURN(Expression, rewritten,
                     (RewriteGroupingSetBranch(child, context)));
    if (rewritten->ToString() != child->ToString()) {
      child = std::move(rewritten);
      changed = true;
    }
  }
  if (!changed) {
    return expression;
  }
  return WithExpressionChildren(expression, std::move(children));
}

// Collects window calls from a select-entry expression.  Window
// specifications own their scope: windows never nest inside aggregates,
// subqueries or other window calls, so the walk stops at those boundaries.
void CollectWindowCalls(const Expression& expression,
                        std::vector<const WindowFunctionCallExpression*>* out) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kWindowFunctionExp) {
    out->push_back(&expression->AsWindowFunctionCallExpression());
    return;
  }
  switch (expression->Type()) {
    case TypeTag::kAggregateExp:
    case TypeTag::kQueryExp:
      return;
    default:
      break;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CollectWindowCalls(child, out);
  }
}

// True when the expression can be evaluated directly against the combined
// grouping set output: only column references and constants downstream of
// scalar operators.  Aggregates would degrade into scalar aggregations and
// window calls cannot nest, so both fail the check.
bool OnlyReferencesWrapperColumns(const Expression& expression,
                                  const std::string& wrapper_alias) {
  if (!expression) {
    return true;
  }
  switch (expression->Type()) {
    case TypeTag::kAggregateExp:
    case TypeTag::kWindowFunctionExp:
    case TypeTag::kQueryExp:
      return false;
    case TypeTag::kColumnValue:
      return expression->AsColumnValue().GetColumnName().schema ==
             wrapper_alias;
    default:
      break;
  }
  return std::ranges::all_of(ExpressionChildren(expression),
                             [&wrapper_alias](const Expression& child) {
                               return OnlyReferencesWrapperColumns(
                                   child, wrapper_alias);
                             });
}

// Rewrites window calls into wrapper-level calls whose inputs reference the
// hidden per-branch columns.  Frame specifications carry constants and are
// copied verbatim; every other window input must have been materialized.
StatusOr<Expression> RewireWindowInputs(
    const Expression& expression, const std::string& wrapper_alias,
    const std::unordered_map<std::string, std::string>& hidden_columns) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kWindowFunctionExp) {
    const WindowFunctionCallExpression& window =
        expression->AsWindowFunctionCallExpression();
    auto rebuilt = std::make_shared<WindowFunctionCallExpression>();
    rebuilt->function = window.function;
    rebuilt->distinct = window.distinct;
    rebuilt->respect_nulls = window.respect_nulls;
    rebuilt->ignore_nulls = window.ignore_nulls;
    rebuilt->inner_limit = window.inner_limit;
    rebuilt->frame_unit = window.frame_unit;
    rebuilt->frame_start = window.frame_start;
    rebuilt->frame_end = window.frame_end;
    rebuilt->has_frame = window.has_frame;
    rebuilt->exclusion = window.exclusion;
    const auto map_input =
        [&](const Expression& input) -> StatusOr<Expression> {
      const auto found = hidden_columns.find(input->ToString());
      if (found == hidden_columns.end()) {
        return AstError<Expression>(
            "window function input over GROUPING SETS / ROLLUP / CUBE was "
            "not materialized: " +
            input->ToString());
      }
      return ColumnValueExp(ColumnName(wrapper_alias, found->second));
    };
    for (const Expression& arg : window.args) {
      ASSIGN_OR_RETURN(Expression, mapped, (map_input(arg)));
      rebuilt->args.push_back(std::move(mapped));
    }
    for (const Expression& partition : window.partition_by) {
      ASSIGN_OR_RETURN(Expression, mapped, (map_input(partition)));
      rebuilt->partition_by.push_back(std::move(mapped));
    }
    for (const WindowOrderTerm& term : window.order_by) {
      ASSIGN_OR_RETURN(Expression, mapped, (map_input(term.expression)));
      rebuilt->order_by.push_back(
          {std::move(mapped), term.ascending, term.nulls_first});
    }
    for (const WindowOrderTerm& term : window.inner_order_by) {
      ASSIGN_OR_RETURN(Expression, mapped, (map_input(term.expression)));
      rebuilt->inner_order_by.push_back(
          {std::move(mapped), term.ascending, term.nulls_first});
    }
    return rebuilt;
  }
  switch (expression->Type()) {
    case TypeTag::kAggregateExp:
    case TypeTag::kQueryExp:
      return expression;
    default:
      break;
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  bool changed = false;
  for (Expression& child : children) {
    ASSIGN_OR_RETURN(
        Expression, rewritten,
        (RewireWindowInputs(child, wrapper_alias, hidden_columns)));
    if (rewritten->ToString() != child->ToString()) {
      child = std::move(rewritten);
      changed = true;
    }
  }
  if (!changed) {
    return expression;
  }
  return WithExpressionChildren(expression, std::move(children));
}

// Expands GROUPING SETS / ROLLUP / CUBE for statements that also carry window
// functions.  A window function sees the *combined* result of every grouping
// set (rank() over a ROLLUP output must span detail and subtotal rows
// together), so computing it per branch would restart each rank per branch.
// The expansion therefore:
//  1. rewrites every grouping set into a branch of a UNION ALL that keeps the
//     aggregates, folds GROUPING() to constants, NULL-pads inactive keys,
//     renames each entry to a synthetic column and appends one hidden column
//     per expression the window calls (and unresolvable ORDER BY terms)
//     consume;
//  2. wraps the union as a derived table whose select list re-projects the
//     original entries and evaluates the window calls over the hidden
//     columns.  ORDER BY and LIMIT move to the wrapper: they belong to the
//     ordered, post-window result.
StatusOr<std::shared_ptr<SelectStatement>> ExpandGroupingSetsWithWindows(
    // clang-tidy performance-unnecessary-value-param: kept by value on
    // purpose (cheap shared_ptr incref; see the snapshot note below).
    std::shared_ptr<SelectStatement>
        statement,  // NOLINT(performance-unnecessary-value-param)
    const std::vector<std::vector<Expression>>& pending_grouping_sets) {
  const std::string wrapper_alias = "$grouping_sets_window";
  const auto branch_column = [](size_t index) {
    return "$gsc" + std::to_string(index);
  };
  const auto hidden_column = [](size_t index) {
    return "$gsw" + std::to_string(index);
  };

  std::vector<const WindowFunctionCallExpression*> window_calls;
  for (const NamedExpression& item : statement->SelectList()) {
    CollectWindowCalls(item.expression, &window_calls);
  }
  for (const auto& term : statement->OrderBy()) {
    std::vector<const WindowFunctionCallExpression*> term_windows;
    CollectWindowCalls(term.expression, &term_windows);
    if (!term_windows.empty()) {
      return AstError<std::shared_ptr<SelectStatement>>(
          "window functions in ORDER BY with GROUPING SETS / ROLLUP / CUBE "
          "are not supported; reference the select-list alias instead");
    }
  }

  // Every expression a window call consumes (arguments, PARTITION BY, ORDER
  // BY, inner ORDER BY) is post-aggregation and must be materialized per
  // branch.  AGG(x WHERE p) OVER (...) mixes a pre-aggregation row filter
  // into that post-aggregation surface and stays unsupported here.
  std::vector<Expression> window_inputs;
  std::unordered_map<std::string, std::string> hidden_columns;
  const auto add_window_input = [&](const Expression& input) -> Status {
    if (!input) {
      return Status::kSuccess;
    }
    std::vector<const WindowFunctionCallExpression*> nested;
    CollectWindowCalls(input, &nested);
    if (!nested.empty()) {
      return AstStatus(
          "nested window functions with GROUPING SETS / ROLLUP / "
          "CUBE are not supported");
    }
    const std::string text = input->ToString();
    if (hidden_columns.contains(text)) {
      return Status::kSuccess;
    }
    hidden_columns.emplace(text, hidden_column(window_inputs.size()));
    window_inputs.push_back(input);
    return Status::kSuccess;
  };
  for (const WindowFunctionCallExpression* call : window_calls) {
    if (call->where_filter) {
      return AstError<std::shared_ptr<SelectStatement>>(
          "AGG(x WHERE p) OVER (...) with GROUPING SETS / ROLLUP / CUBE is "
          "not supported");
    }
    for (const Expression& arg : call->args) {
      RETURN_IF_FAIL(add_window_input(arg));
    }
    for (const Expression& partition : call->partition_by) {
      RETURN_IF_FAIL(add_window_input(partition));
    }
    for (const WindowOrderTerm& term : call->order_by) {
      RETURN_IF_FAIL(add_window_input(term.expression));
    }
    for (const WindowOrderTerm& term : call->inner_order_by) {
      RETURN_IF_FAIL(add_window_input(term.expression));
    }
  }

  // ORDER BY belongs to the wrapper.  Bare names that identify a select entry
  // (by alias, or by the entry's own bare column) rebind to that branch
  // column; ordinal terms pass through; anything else is materialized per
  // branch like a window input so it survives the union.  Window entries have
  // no branch column (their branch cell is a placeholder), so terms naming a
  // window alias stay unresolved and bind to the wrapper's own output.
  std::unordered_map<std::string, size_t> entry_by_name;
  std::unordered_map<std::string, size_t> entry_by_text;
  std::unordered_set<std::string> window_entry_names;
  const std::vector<NamedExpression>& base_list = statement->SelectList();
  for (size_t j = 0; j < base_list.size(); ++j) {
    const NamedExpression& item = base_list[j];
    if (!item.name.empty()) {
      entry_by_name.emplace(Lower(item.name), j);
    } else if (item.expression &&
               item.expression->Type() == TypeTag::kColumnValue &&
               item.expression->AsColumnValue()
                   .GetColumnName()
                   .schema.empty()) {
      entry_by_name.emplace(
          Lower(item.expression->AsColumnValue().GetColumnName().name), j);
    }
    if (item.expression) {
      entry_by_text.emplace(item.expression->ToString(), j);
      std::vector<const WindowFunctionCallExpression*> windows;
      CollectWindowCalls(item.expression, &windows);
      if (!windows.empty() && !item.name.empty()) {
        window_entry_names.insert(Lower(item.name));
      }
    }
  }
  // Rebinds bare column references that name a select entry to that entry's
  // branch column.  Aggregate, window and subquery scopes are not descended.
  const std::function<Expression(const Expression&)> rebind_output_names =
      [&](const Expression& expression) -> Expression {
    if (!expression) {
      return expression;
    }
    if (expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& column = expression->AsColumnValue().GetColumnName();
      if (column.schema.empty()) {
        const auto found = entry_by_name.find(Lower(column.name));
        if (found != entry_by_name.end()) {
          return ColumnValueExp(
              ColumnName(wrapper_alias, branch_column(found->second)));
        }
      }
      return expression;
    }
    switch (expression->Type()) {
      case TypeTag::kAggregateExp:
      case TypeTag::kWindowFunctionExp:
      case TypeTag::kQueryExp:
        return expression;
      default:
        break;
    }
    std::vector<Expression> children = ExpressionChildren(expression);
    bool changed = false;
    for (Expression& child : children) {
      Expression rebound = rebind_output_names(child);
      if (rebound->ToString() != child->ToString()) {
        child = std::move(rebound);
        changed = true;
      }
    }
    if (!changed) {
      return expression;
    }
    return WithExpressionChildren(expression, std::move(children));
  };
  std::vector<SelectStatement::OrderByTerm> wrapper_terms;
  for (SelectStatement::OrderByTerm term : statement->OrderBy()) {
    if (term.expression && term.expression->Type() == TypeTag::kConstantValue) {
      // Ordinal terms resolve against the wrapper's select list, whose entry
      // order matches the original.
      wrapper_terms.push_back(std::move(term));
      continue;
    }
    if (term.expression && term.expression->Type() == TypeTag::kColumnValue &&
        term.expression->AsColumnValue().GetColumnName().schema.empty() &&
        window_entry_names.contains(
            Lower(term.expression->AsColumnValue().GetColumnName().name))) {
      // A bare window alias sorts the wrapper's own computed window column;
      // the engine's ORDER BY binding resolves it against the output.  The
      // branch-side column is a NULL placeholder and must not be used.
      wrapper_terms.push_back(std::move(term));
      continue;
    }
    Expression rebound = rebind_output_names(term.expression);
    const auto by_text = entry_by_text.find(rebound->ToString());
    if (by_text != entry_by_text.end()) {
      term.expression = ColumnValueExp(
          ColumnName(wrapper_alias, branch_column(by_text->second)));
    } else if (OnlyReferencesWrapperColumns(rebound, wrapper_alias)) {
      term.expression = std::move(rebound);
    } else {
      RETURN_IF_FAIL(add_window_input(rebound));
      term.expression = ColumnValueExp(
          ColumnName(wrapper_alias, hidden_columns.at(rebound->ToString())));
    }
    wrapper_terms.push_back(std::move(term));
  }

  // Fingerprint of every key across all sets (deduplicated, insertion
  // order); a branch NULL-pads references to keys outside its active set.
  std::vector<std::string> all_key_texts;
  for (const auto& set : pending_grouping_sets) {
    for (const Expression& key : set) {
      const std::string text = key->ToString();
      if (std::ranges::find(all_key_texts, text) == all_key_texts.end()) {
        all_key_texts.push_back(text);
      }
    }
  }
  const auto inactive_bare_key_aliases =
      [&](const GroupingSetBranchContext& context) {
        std::unordered_set<std::string> out;
        for (const auto& set : pending_grouping_sets) {
          for (const Expression& key : set) {
            if (!key || key->Type() != TypeTag::kColumnValue) {
              continue;
            }
            const std::string text = key->ToString();
            if (context.active_key_texts.contains(text)) {
              continue;
            }
            if (key->AsColumnValue().GetColumnName().schema.empty()) {
              out.insert(Lower(text));
            }
          }
        }
        return out;
      };

  // A snapshot, not a reference: the rewrite below mutates *statement while
  // still reading the original select list from `base`.
  const SelectStatement
      base =  // NOLINT(performance-unnecessary-copy-initialization)
      *statement;
  auto rewrite_window_branch =
      [&](SelectStatement& branch,
          const std::vector<Expression>& active) -> Status {
    GroupingSetBranchContext context;
    context.all_key_texts = all_key_texts;
    for (const Expression& key : active) {
      context.active_key_texts.insert(key->ToString());
    }
    const std::unordered_set<std::string> inactive_aliases =
        inactive_bare_key_aliases(context);

    std::vector<NamedExpression> rewritten_list;
    rewritten_list.reserve(base.SelectList().size() + window_inputs.size());
    std::vector<const WindowFunctionCallExpression*> windows_scratch;
    for (const NamedExpression& item : base.SelectList()) {
      const std::string entry_name = branch_column(rewritten_list.size());
      if (!item.expression) {
        rewritten_list.emplace_back(entry_name, item.expression);
        continue;
      }
      windows_scratch.clear();
      CollectWindowCalls(item.expression, &windows_scratch);
      if (!windows_scratch.empty()) {
        // The wrapper recomputes windows over the combined rows; the branch
        // keeps a placeholder so every branch shares the union width.
        rewritten_list.emplace_back(entry_name, ConstantValueExp(Value()));
        continue;
      }
      // `SELECT a AS k ... GROUP BY ROLLUP(k)`: an entry named after an
      // inactive bare-column key NULL-pads even though its expression (`a`)
      // differs from the key text (`k`).
      const std::string entry_text = item.expression->ToString();
      const bool entry_is_active =
          context.active_key_texts.contains(entry_text);
      const bool entry_aliases_inactive_key =
          !item.name.empty() && inactive_aliases.contains(Lower(item.name)) &&
          !entry_is_active;
      if (entry_aliases_inactive_key) {
        rewritten_list.emplace_back(entry_name, ConstantValueExp(Value()));
        continue;
      }
      ASSIGN_OR_RETURN(Expression, rewritten,
                       (RewriteGroupingSetBranch(item.expression, context)));
      rewritten_list.emplace_back(entry_name, std::move(rewritten));
    }
    for (const Expression& input : window_inputs) {
      ASSIGN_OR_RETURN(Expression, rewritten,
                       (RewriteGroupingSetBranch(input, context)));
      rewritten_list.emplace_back(
          hidden_column(rewritten_list.size() - base.SelectList().size()),
          std::move(rewritten));
    }
    branch.SetSelectList(std::move(rewritten_list));
    if (branch.Having()) {
      ASSIGN_OR_RETURN(Expression, having,
                       (RewriteGroupingSetBranch(branch.Having(), context)));
      branch.SetHaving(std::move(having));
    }
    // The empty grouping set () aggregates the whole input into one row;
    // the plain-item path keys it on a constant for that shape.
    std::vector<Expression> group_keys = active;
    if (group_keys.empty()) {
      group_keys.push_back(ConstantValueExp(Value(int64_t{0})));
      branch.SetGroupAll(true);
    }
    branch.SetGroupBy(std::move(group_keys));
    branch.SetOrderBy({});
    return Status::kSuccess;
  };

  for (size_t i = 0; i < pending_grouping_sets.size(); ++i) {
    if (i == 0) {
      RETURN_IF_FAIL(
          rewrite_window_branch(*statement, pending_grouping_sets.front()));
      continue;
    }
    auto branch = std::make_shared<SelectStatement>(base);
    RETURN_IF_FAIL(rewrite_window_branch(*branch, pending_grouping_sets[i]));
    // ORDER BY / LIMIT / WITH TIES belong to the wrapper only.
    branch->SetOrderBy({});
    branch->SetLimit(std::nullopt);
    branch->SetOffset(0);
    branch->SetWithTies(false);
    // Same as the plain expansion: the owning statement keeps the WITH
    // declarations so the CTE bodies execute once, not once per grouping set.
    branch->ClearWithQueries();
    statement->AddSetOperation(SetOperationKind::kUnionAll, std::move(branch));
  }

  // Wrapper select list: non-window entries re-project their branch column;
  // window entries are rebuilt with every input rewired to a hidden column.
  std::vector<NamedExpression> wrapper_list;
  wrapper_list.reserve(base.SelectList().size());
  for (size_t j = 0; j < base.SelectList().size(); ++j) {
    const NamedExpression& item = base.SelectList()[j];
    std::string name = item.name;
    if (name.empty() && item.expression &&
        item.expression->Type() == TypeTag::kColumnValue) {
      // Unnamed bare-column entries keep their column as the output name.
      name = item.expression->AsColumnValue().GetColumnName().name;
    }
    std::vector<const WindowFunctionCallExpression*> windows;
    CollectWindowCalls(item.expression, &windows);
    if (windows.empty()) {
      wrapper_list.emplace_back(
          std::move(name),
          ColumnValueExp(ColumnName(wrapper_alias, branch_column(j))));
      continue;
    }
    ASSIGN_OR_RETURN(
        Expression, rewired,
        (RewireWindowInputs(item.expression, wrapper_alias, hidden_columns)));
    wrapper_list.emplace_back(std::move(name), std::move(rewired));
  }

  auto wrapper = std::make_shared<SelectStatement>();
  SelectSource source;
  source.query = statement;
  source.alias = wrapper_alias;
  source.join_type = JoinType::kCross;
  wrapper->SetSources({std::move(source)});
  wrapper->SetSelectList(std::move(wrapper_list));
  wrapper->SetOrderBy(std::move(wrapper_terms));
  wrapper->SetLimit(statement->HasLimit()
                        ? std::optional<size_t>(statement->Limit())
                        : std::nullopt);
  wrapper->SetOffset(statement->Offset());
  wrapper->SetWithTies(statement->WithTies());
  statement->SetLimit(std::nullopt);
  statement->SetOffset(0);
  statement->SetWithTies(false);
  return wrapper;
}

StatusOr<SelectSource> ExpandPivotSource(SelectSource base,
                                         const GoogleSqlAstNode& pivot) {
  const GoogleSqlAstNode* expr_list = pivot.Child("PivotExpressionList");
  const GoogleSqlAstNode* for_col = pivot.Child("PathExpression");
  const GoogleSqlAstNode* value_list = pivot.Child("PivotValueList");
  if (expr_list == nullptr || for_col == nullptr || value_list == nullptr) {
    return AstError<SelectSource>("GoogleSQL AST: malformed PivotClause");
  }
  ASSIGN_OR_RETURN(std::string, hv155978_0, (Path(*for_col)));
  const std::string pivot_col_name = std::move(hv155978_0);

  struct PivotAggInfo {
    std::string agg_alias;
    std::string func_name;
    const GoogleSqlAstNode* arg_node{nullptr};
  };
  std::vector<PivotAggInfo> aggs;
  for (const GoogleSqlAstNode* pe : expr_list->Children("PivotExpression")) {
    PivotAggInfo info;
    ASSIGN_OR_RETURN(std::string, hv156297_0, (Alias(*pe)));
    info.agg_alias = std::move(hv156297_0);
    const GoogleSqlAstNode* fn_call = pe->Child("FunctionCall");
    if (fn_call == nullptr) {
      for (const auto& c : pe->children) {
        if (c->kind != "Alias" && c->kind != "Location") {
          fn_call = c.get();
          break;
        }
      }
    }
    if (fn_call != nullptr) {
      if (const GoogleSqlAstNode* fn_path = fn_call->Child("PathExpression")) {
        ASSIGN_OR_RETURN(std::string, hv156707_0, (Path(*fn_path)));
        info.func_name = std::move(hv156707_0);
      }
      for (const auto& c : fn_call->children) {
        if (c->kind != "PathExpression" && c->kind != "Alias" &&
            c->kind != "Location") {
          info.arg_node = c.get();
          break;
        }
      }
      if (info.arg_node == nullptr) {
        const auto paths = fn_call->Children("PathExpression");
        if (paths.size() >= 2) {
          info.arg_node = paths[1];
        }
      }
    }
    aggs.push_back(info);
  }

  std::vector<NamedExpression> projections;
  std::unordered_set<std::string> agg_column_names;
  for (const auto& agg : aggs) {
    if ((agg.arg_node != nullptr) && agg.arg_node->kind != "Star") {
      ASSIGN_OR_RETURN(std::string, hv157400_0, (Path(*agg.arg_node)));
      agg_column_names.insert(std::move(hv157400_0));
    }
  }

  std::vector<Expression> group_by_exprs;
  if (base.query) {
    for (const NamedExpression& named : base.query->SelectList()) {
      if (named.name != pivot_col_name &&
          !agg_column_names.contains(named.name)) {
        projections.emplace_back(named.name,
                                 ColumnValueExp(ColumnName(named.name)));
        group_by_exprs.push_back(ColumnValueExp(ColumnName(named.name)));
      }
    }
  }

  for (const GoogleSqlAstNode* pv : value_list->Children("PivotValue")) {
    const GoogleSqlAstNode* val_node = nullptr;
    for (const auto& c : pv->children) {
      if (c->kind != "Alias" && c->kind != "Location") {
        val_node = c.get();
        break;
      }
    }
    if (val_node == nullptr) {
      continue;
    }
    ASSIGN_OR_RETURN(Expression, hv158229_0, (VisitExpression(*val_node)));
    Expression val_expr = std::move(hv158229_0);
    ASSIGN_OR_RETURN(std::string, hv158283_0, (Alias(*pv)));
    std::string val_alias = std::move(hv158283_0);
    if (val_alias.empty()) {
      if (val_node->kind == "StringLiteral") {
        for (const auto& c : val_node->children) {
          if (c->kind == "StringLiteralComponent") {
            val_alias = c->detail;
            if (!val_alias.empty() && val_alias.front() == '\'' &&
                val_alias.back() == '\'') {
              val_alias = val_alias.substr(1, val_alias.size() - 2);
            }
            break;
          }
        }
      }
      if (val_alias.empty()) {
        val_alias = val_node->detail;
      }
    }

    for (const auto& agg : aggs) {
      std::string col_name;
      if (agg.agg_alias.empty()) {
        col_name = val_alias;
      } else if (val_alias.empty()) {
        col_name = agg.agg_alias;
      } else {
        col_name = agg.agg_alias + "_" + val_alias;
      }

      Expression condition =
          BinaryExpressionExp(ColumnValueExp(ColumnName(pivot_col_name)),
                              BinaryOperation::kEquals, val_expr);
      Expression inner_arg;
      if ((agg.arg_node != nullptr) && agg.arg_node->kind != "Star") {
        ASSIGN_OR_RETURN(Expression, hv159410_0,
                         (VisitExpression(*agg.arg_node)));
        inner_arg = std::move(hv159410_0);
      } else {
        inner_arg = ConstantValueExp(Value(1));
      }
      std::vector<std::pair<Expression, Expression>> when_clauses;
      when_clauses.emplace_back(std::move(condition), std::move(inner_arg));
      Expression case_expr =
          CaseExpressionExp(std::move(when_clauses), ConstantValueExp(Value()));

      const std::string lower_fn = Lower(agg.func_name);
      AggregationType agg_type = AggregationType::kSum;
      if (lower_fn == "count") {
        agg_type = AggregationType::kCount;
      } else if (lower_fn == "avg") {
        agg_type = AggregationType::kAvg;
      } else if (lower_fn == "min") {
        agg_type = AggregationType::kMin;
      } else if (lower_fn == "max") {
        agg_type = AggregationType::kMax;
      }

      Expression agg_expression =
          AggregateExpressionExp(agg_type, std::move(case_expr), false);
      projections.emplace_back(col_name, std::move(agg_expression));
    }
  }

  auto pivot_subquery = std::make_shared<SelectStatement>();
  pivot_subquery->SetSelectList(std::move(projections));
  std::string saved_alias = base.alias;
  pivot_subquery->SetSources({std::move(base)});
  if (!group_by_exprs.empty()) {
    pivot_subquery->SetGroupBy(std::move(group_by_exprs));
  } else {
    std::vector<Expression> inferred;
    for (const NamedExpression& named : pivot_subquery->SelectList()) {
      if (named.expression && !ContainsAggregate(named.expression)) {
        inferred.push_back(named.expression);
      }
    }
    if (!inferred.empty()) {
      pivot_subquery->SetGroupBy(std::move(inferred));
    }
  }
  pivot_subquery->MarkComplex();

  SelectSource result;
  result.query = std::move(pivot_subquery);
  result.alias = saved_alias.empty() ? "pivot_table" : saved_alias;
  return result;
}

StatusOr<SelectSource> ExpandUnpivotSource(const SelectSource& base,
                                           const GoogleSqlAstNode& unpivot) {
  const GoogleSqlAstNode* val_cols = unpivot.Child("ExpressionList");
  const GoogleSqlAstNode* name_col_node = unpivot.Child("PathExpression");
  const GoogleSqlAstNode* in_items = unpivot.Child("UnpivotInItemList");
  if (val_cols == nullptr || name_col_node == nullptr || in_items == nullptr) {
    return AstError<SelectSource>("GoogleSQL AST: malformed UnpivotClause");
  }
  std::string val_col_name = "val";
  if (!val_cols->children.empty()) {
    ASSIGN_OR_RETURN(std::string, hv161845_0, (Path(*val_cols->children[0])));
    val_col_name = std::move(hv161845_0);
  }
  ASSIGN_OR_RETURN(std::string, hv161898_0, (Path(*name_col_node)));
  std::string name_col_name = std::move(hv161898_0);
  const bool include_nulls =
      unpivot.detail.find("INCLUDE NULLS") != std::string::npos;

  std::shared_ptr<SelectStatement> root_branch;
  std::string saved_alias = base.alias;

  for (const GoogleSqlAstNode* item : in_items->Children("UnpivotInItem")) {
    const GoogleSqlAstNode* in_expr_list = item->Child("ExpressionList");
    if (in_expr_list == nullptr || in_expr_list->children.empty()) {
      continue;
    }
    ASSIGN_OR_RETURN(std::string, hv162376_0,
                     (Path(*in_expr_list->children[0])));
    std::string in_col_name = std::move(hv162376_0);
    std::string in_label = in_col_name;
    if (const GoogleSqlAstNode* label_node =
            item->Child("UnpivotInItemLabel")) {
      if (const GoogleSqlAstNode* str_node =
              label_node->Child("StringLiteral")) {
        for (const auto& c : str_node->children) {
          if (c->kind == "StringLiteralComponent") {
            in_label = c->detail;
            if (!in_label.empty() && in_label.front() == '\'' &&
                in_label.back() == '\'') {
              in_label = in_label.substr(1, in_label.size() - 2);
            }
            break;
          }
        }
      } else if (!label_node->children.empty()) {
        in_label = label_node->children[0]->detail;
      }
    }

    std::vector<NamedExpression> branch_select;
    branch_select.emplace_back("", ColumnValueExp(ColumnName("*")));
    branch_select.emplace_back(val_col_name,
                               ColumnValueExp(ColumnName(in_col_name)));
    branch_select.emplace_back(name_col_name,
                               ConstantValueExp(Value(std::string(in_label))));

    auto branch = std::make_shared<SelectStatement>();
    branch->SetSelectList(std::move(branch_select));
    branch->SetSources({base});
    if (!include_nulls) {
      branch->SetWhereClause(UnaryExpressionExp(
          ColumnValueExp(ColumnName(in_col_name)), UnaryOperation::kIsNotNull));
    }
    branch->MarkComplex();

    if (!root_branch) {
      root_branch = branch;
    } else {
      root_branch->AddUnionAll(branch);
    }
  }

  SelectSource result;
  result.query = std::move(root_branch);
  result.alias = saved_alias.empty() ? "unpivot_table" : saved_alias;
  return result;
}

struct ColumnTypeInfo {
  ValueType type{ValueType::kNull};
  bool is_unsigned{false};
};

StatusOr<ColumnTypeInfo> ColumnType(const GoogleSqlAstNode& definition) {
  const GoogleSqlAstNode* schema = definition.Child("SimpleColumnSchema");
  if (schema == nullptr) {
    if (definition.Child("ArrayColumnSchema") != nullptr) {
      return ColumnTypeInfo{.type = ValueType::kArray, .is_unsigned = false};
    }
  }
  const GoogleSqlAstNode* path =
      schema != nullptr ? schema->Child("PathExpression") : nullptr;
  if (path == nullptr) {
    return AstError<ColumnTypeInfo>("GoogleSQL AST: column type missing");
  }
  ASSIGN_OR_RETURN(std::string, hv212301_0, (Path(*path)));
  // Proto / user-defined type names arrive as backticked dotted paths
  // (`googlesql_test.Proto3KitchenSink`) or PROTO<...> wrappers; they store
  // through the VARCHAR channel carrying their TEXT-format payload.
  std::string raw_type = std::move(hv212301_0);
  std::string cleaned;
  for (const char c : raw_type) {
    if (c != '`') {
      cleaned.push_back(c);
    }
  }
  const std::string lower = Lower(cleaned);
  if (lower.starts_with("proto<") || lower.find('.') != std::string::npos) {
    return ColumnTypeInfo{.type = ValueType::kVarChar, .is_unsigned = false};
  }
  const std::string& type = lower;
  if (type == "int" || type == "int64" || type == "integer" ||
      type == "bigint" || type == "bool" || type == "boolean" ||
      type == "int32" || type == "int16" || type == "int8") {
    return ColumnTypeInfo{.type = ValueType::kInt64, .is_unsigned = false};
  }
  if (type == "uint" || type == "uint64" || type == "uint32" ||
      type == "uint16" || type == "uint8") {
    return ColumnTypeInfo{.type = ValueType::kInt64, .is_unsigned = true};
  }
  if (type == "numeric" || type == "decimal" || type == "double" ||
      type == "float" || type == "float64" || type == "float32" ||
      type == "bignumeric") {
    return ColumnTypeInfo{.type = ValueType::kDouble, .is_unsigned = false};
  }
  if (type == "date") {
    return ColumnTypeInfo{.type = ValueType::kDate, .is_unsigned = false};
  }
  if (type == "string" || type == "varchar" || type == "char" ||
      type == "timestamp" || type == "datetime" || type == "time" ||
      type == "interval" || type == "bytes" || type == "json") {
    return ColumnTypeInfo{.type = ValueType::kVarChar, .is_unsigned = false};
  }
  if (type == "array" || type.starts_with("array<")) {
    return ColumnTypeInfo{.type = ValueType::kArray, .is_unsigned = false};
  }
  return AstError<ColumnTypeInfo>("GoogleSQL AST: unsupported column type " +
                                  type);
}

// Parses a referential action ("NO ACTION" only) out of a ForeignKeyActions
// detail string such as "ON UPDATE NO ACTION ON DELETE NO ACTION".
// Anything stronger (CASCADE / SET NULL / SET DEFAULT) is rejected so
// unsupported semantics never silently degrade to NO ACTION.
StatusOr<ForeignKeyDef::Action> ParseForeignKeyAction(
    const GoogleSqlAstNode& reference, std::string_view clause) {
  const GoogleSqlAstNode* actions = reference.Child("ForeignKeyActions");
  if (actions == nullptr) {
    return ForeignKeyDef::Action::kNoAction;
  }
  std::string detail = actions->detail;
  for (char& c : detail) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  const std::string marker = std::string(clause) + " ";
  const size_t pos = detail.find(marker);
  if (pos == std::string::npos) {
    return ForeignKeyDef::Action::kNoAction;
  }
  const std::string rest = detail.substr(pos + marker.size());
  if (rest.starts_with("NO ACTION")) {
    return ForeignKeyDef::Action::kNoAction;
  }
  return AstError<ForeignKeyDef::Action>(
      "GoogleSQL AST: only NO ACTION referential actions are supported");
}

// Fills a ForeignKeyDef from a ForeignKeyReference node (parent table,
// parent columns, MATCH mode, actions).  `child_columns` come from the
// enclosing table-level ForeignKey node or the single column definition.
StatusOr<ForeignKeyDef> VisitForeignKeyReference(
    const GoogleSqlAstNode& reference, std::vector<std::string> child_columns) {
  if (child_columns.empty()) {
    return AstError<ForeignKeyDef>("GoogleSQL AST: bad FOREIGN KEY");
  }
  const std::string& match = reference.detail;
  if (!match.empty() && match.find("MATCH SIMPLE") == std::string::npos) {
    return AstError<ForeignKeyDef>(
        "GoogleSQL AST: only MATCH SIMPLE foreign keys are supported");
  }
  const GoogleSqlAstNode* parent = reference.Child("PathExpression");
  const GoogleSqlAstNode* columns = reference.Child("ColumnList");
  if (parent == nullptr || columns == nullptr) {
    return AstError<ForeignKeyDef>("GoogleSQL AST: bad FOREIGN KEY");
  }
  ASSIGN_OR_RETURN(std::string, h_parent, (Path(*parent)));
  std::vector<std::string> parent_columns;
  for (const GoogleSqlAstNode* id : columns->Children("Identifier")) {
    ASSIGN_OR_RETURN(std::string, h_col, (Identifier(*id)));
    parent_columns.push_back(std::move(h_col));
  }
  if (parent_columns.size() != child_columns.size()) {
    return AstError<ForeignKeyDef>(
        "GoogleSQL AST: FOREIGN KEY column count mismatch");
  }
  ASSIGN_OR_RETURN(ForeignKeyDef::Action, h_on_delete,
                   (ParseForeignKeyAction(reference, "ON DELETE")));
  ASSIGN_OR_RETURN(ForeignKeyDef::Action, h_on_update,
                   (ParseForeignKeyAction(reference, "ON UPDATE")));
  ForeignKeyDef def;
  def.child_columns = std::move(child_columns);
  def.parent_table = std::move(h_parent);
  def.parent_columns = std::move(parent_columns);
  def.on_delete = h_on_delete;
  def.on_update = h_on_update;
  return def;
}

Status ValidateCreateTableOptions(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* options = root.Child("OptionsList");
  if (options == nullptr) {
    return Status::kSuccess;
  }
  for (const GoogleSqlAstNode* entry : options->Children("OptionsEntry")) {
    const GoogleSqlAstNode* name_node = entry->Child("Identifier");
    if (name_node == nullptr) {
      return AstStatus("GoogleSQL AST: bad CREATE TABLE option");
    }
    ASSIGN_OR_RETURN(std::string, key, (Identifier(*name_node)));
    std::string lower_key = key;
    for (char& c : lower_key) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (lower_key != "engine" && lower_key != "storage_engine") {
      return AstStatus("GoogleSQL AST: unsupported CREATE TABLE option " + key);
    }
    std::string engine_str;
    for (const auto& child : entry->children) {
      if (child.get() == name_node) {
        continue;
      }
      if (child->kind == "PathExpression") {
        ASSIGN_OR_RETURN(std::string, parsed_path, (Path(*child)));
        engine_str = std::move(parsed_path);
        break;
      }
      if (child->kind == "Identifier") {
        ASSIGN_OR_RETURN(std::string, parsed_id, (Identifier(*child)));
        engine_str = std::move(parsed_id);
        break;
      }
      ASSIGN_OR_RETURN(Expression, expr, (VisitExpression(*child)));
      StatusOr<Value> val = expr->TryEvaluate(Row(), Schema());
      if (!val.HasValue() || val.Value().type != ValueType::kVarChar) {
        return AstStatus(
            "GoogleSQL AST: CREATE TABLE engine option must be a string");
      }
      engine_str = std::string(val.Value().value.varchar_value);
      break;
    }
    for (char& c : engine_str) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (engine_str != "lstable" && engine_str != "lst" && engine_str != "pax" &&
        engine_str != "columnar" && engine_str != "default") {
      return AstStatus("GoogleSQL AST: unknown storage engine " + engine_str);
    }
  }
  return Status::kSuccess;
}

StatusOr<std::unique_ptr<Statement>> VisitCreate(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  if (path == nullptr) {
    return AstError<std::unique_ptr<Statement>>("GoogleSQL AST: bad CREATE");
  }
  RETURN_IF_FAIL(ValidateCreateTableOptions(root));
  const GoogleSqlAstNode* elements = root.Child("TableElementList");
  if (elements != nullptr) {
    std::vector<Column> columns;
    std::vector<ForeignKeyDef> foreign_keys;
    for (const GoogleSqlAstNode* definition :
         elements->Children("ColumnDefinition")) {
      const GoogleSqlAstNode* name = definition->Child("Identifier");
      if (name == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: unnamed column");
      }
      ASSIGN_OR_RETURN(ColumnTypeInfo, col_info, (ColumnType(*definition)));
      ASSIGN_OR_RETURN(std::string, col_name, (Identifier(*name)));
      // Column DEFAULT clause: the pinned parser emits the default
      // expression as a direct SimpleColumnSchema child next to the type
      // path and the attribute list.  GoogleSQL requires constant literals
      // here, so the expression must evaluate without a row.
      std::optional<Value> column_default;
      const GoogleSqlAstNode* column_schema =
          definition->Child("SimpleColumnSchema");
      if (column_schema != nullptr) {
        for (const auto& child : column_schema->children) {
          const std::string& kind = child->kind;
          // The type path, its parameter list (VARCHAR(10)), and the
          // attribute list are structural children; anything else is the
          // DEFAULT expression.
          if (kind == "PathExpression" || kind == "TypeParameterList" ||
              kind == "ColumnAttributeList" || kind == "Collate") {
            continue;
          }
          if (column_default.has_value()) {
            return AstError<std::unique_ptr<Statement>>(
                "GoogleSQL AST: column has more than one DEFAULT clause");
          }
          ASSIGN_OR_RETURN(Expression, default_expr, (VisitExpression(*child)));
          // The clause freezes one value for every future INSERT, so
          // volatile calls (RAND()) and subqueries must not reach it.
          if (!SafeToReduceEvaluationCount(default_expr)) {
            return AstError<std::unique_ptr<Statement>>(
                "column DEFAULT must be a constant expression");
          }
          StatusOr<Value> evaluated =
              default_expr->TryEvaluate(Row(), Schema());
          if (!evaluated.HasValue()) {
            return AstError<std::unique_ptr<Statement>>(
                "column DEFAULT must be a constant expression");
          }
          if (evaluated.Value().IsNull()) {
            return AstError<std::unique_ptr<Statement>>(
                "column DEFAULT must not be NULL");
          }
          column_default = evaluated.MoveValue();
        }
      }
      Column col(std::move(col_name), col_info.type);
      if (col_info.is_unsigned) {
        col.SetUnsigned(true);
      }
      columns.push_back(std::move(col));
      const GoogleSqlAstNode* schema = definition->Child("SimpleColumnSchema");
      const GoogleSqlAstNode* attrs =
          schema != nullptr ? schema->Child("ColumnAttributeList") : nullptr;
      if (attrs != nullptr) {
        if (attrs->Child("PrimaryKeyColumnAttribute") != nullptr) {
          const bool was_unsigned = columns.back().IsUnsigned();
          columns.back() = Column(columns.back().Name().name, col_info.type,
                                  Constraint(Constraint::kPrimaryKey));
          columns.back().SetUnsigned(was_unsigned);
        }
        // NOT NULL rides the column's constraint slot: a key constraint or
        // an attribute with slot payload keeps its slot.  Kept semantics:
        //  - PRIMARY KEY: key constraints are enforced non-NULL by the table
        //    writer (CheckRowConstraints treats kPrimaryKey as kNotNull), so
        //    the slot stays kPrimaryKey and the declaration adds nothing.
        //  - FOREIGN KEY / DEFAULT: their slot payload has no NOT NULL side
        //    channel, so the conflicting dual constraint is not representable;
        //    the key/default keeps the slot (previous behavior for these
        //    columns is unchanged; the NOT NULL half stays unenforced).
        if (attrs->Child("NotNullColumnAttribute") != nullptr &&
            columns.back().GetConstraint().ctype == Constraint::kNothing) {
          const bool was_unsigned = columns.back().IsUnsigned();
          columns.back() =
              Column(columns.back().Name().name, columns.back().Type(),
                     Constraint(Constraint::kNotNull));
          columns.back().SetUnsigned(was_unsigned);
        }
        for (const GoogleSqlAstNode* attr :
             attrs->Children("ForeignKeyColumnAttribute")) {
          const GoogleSqlAstNode* reference =
              attr->Child("ForeignKeyReference");
          if (reference == nullptr) {
            return AstError<std::unique_ptr<Statement>>(
                "GoogleSQL AST: bad FOREIGN KEY");
          }
          ASSIGN_OR_RETURN(ForeignKeyDef, h_fk,
                           (VisitForeignKeyReference(
                               *reference, {columns.back().Name().name})));
          foreign_keys.push_back(std::move(h_fk));
        }
      }
      // The DEFAULT rides the column's reserved constraint slot
      // (Constraint::kDefault with the constant as the payload value), so
      // catalog persistence needs no new schema plumbing.  A column that
      // already carries PRIMARY KEY / FOREIGN KEY in the slot rejects the
      // combination explicitly instead of silently losing one side.
      if (column_default.has_value()) {
        if (columns.back().GetConstraint().ctype != Constraint::kNothing) {
          return AstError<std::unique_ptr<Statement>>(
              "column " + columns.back().Name().name +
              " cannot combine DEFAULT with its existing constraint");
        }
        const bool was_unsigned = columns.back().IsUnsigned();
        columns.back() =
            Column(columns.back().Name().name, columns.back().Type(),
                   Constraint(Constraint::kDefault, *column_default));
        columns.back().SetUnsigned(was_unsigned);
      }
    }
    for (const GoogleSqlAstNode* fk : elements->Children("ForeignKey")) {
      const GoogleSqlAstNode* child_list = fk->Child("ColumnList");
      const GoogleSqlAstNode* reference = fk->Child("ForeignKeyReference");
      if (child_list == nullptr || reference == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: bad FOREIGN KEY");
      }
      std::vector<std::string> child_columns;
      for (const GoogleSqlAstNode* id : child_list->Children("Identifier")) {
        ASSIGN_OR_RETURN(std::string, h_col, (Identifier(*id)));
        child_columns.push_back(std::move(h_col));
      }
      ASSIGN_OR_RETURN(
          ForeignKeyDef, h_fk,
          (VisitForeignKeyReference(*reference, std::move(child_columns))));
      foreign_keys.push_back(std::move(h_fk));
    }
    // Single-column foreign keys ride the column's constraint slot in the
    // canonical "parent(col)" form so catalog persistence and the existing
    // optimizer readers (ForeignKeyTarget and friends) see them without
    // new schema plumbing.  Multi-column keys have no slot and are
    // rejected; a column claimed by two keys is ambiguous, also rejected.
    for (const ForeignKeyDef& fk : foreign_keys) {
      if (fk.child_columns.size() != 1) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: multi-column FOREIGN KEY is not supported");
      }
      bool found = false;
      for (Column& col : columns) {
        if (col.Name().name != fk.child_columns[0]) {
          continue;
        }
        if (col.GetConstraint().ctype == Constraint::kForeign) {
          return AstError<std::unique_ptr<Statement>>(
              "GoogleSQL AST: column has two FOREIGN KEYs");
        }
        const bool was_unsigned = col.IsUnsigned();
        col = Column(col.Name().name, col.Type(),
                     Constraint(Constraint::kForeign,
                                Value(FormatForeignKeyReference(
                                    fk.parent_table, fk.parent_columns))));
        col.SetUnsigned(was_unsigned);
        found = true;
        break;
      }
      if (!found) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: FOREIGN KEY on unknown column " +
            fk.child_columns[0]);
      }
    }
    ASSIGN_OR_RETURN(std::string, h_tbl, (Path(*path)));
    return std::make_unique<CreateTableStatement>(
        std::move(h_tbl), std::move(columns), std::move(foreign_keys));
  }
  const GoogleSqlAstNode* query = root.Child("Query");
  if (query != nullptr) {
    ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv214435_0,
                     (VisitQuery(*query)));
    auto statement = std::move(hv214435_0);
    ASSIGN_OR_RETURN(std::string, h_tbl, (Path(*path)));
    return std::make_unique<CreateTableStatement>(std::move(h_tbl),
                                                  std::move(statement));
  }
  return AstError<std::unique_ptr<Statement>>("GoogleSQL AST: bad CREATE");
}

InsertMode InsertModeFromDetail(const std::string& detail) {
  if (detail.find("insert_mode=IGNORE") != std::string::npos) {
    return InsertMode::kIgnore;
  }
  if (detail.find("insert_mode=REPLACE") != std::string::npos) {
    return InsertMode::kReplace;
  }
  if (detail.find("insert_mode=UPDATE") != std::string::npos) {
    return InsertMode::kUpdate;
  }
  return InsertMode::kDefault;
}

StatusOr<int64_t> AssertRowsModifiedValue(const GoogleSqlAstNode& node) {
  const GoogleSqlAstNode* literal = node.Child("IntLiteral");
  if (literal == nullptr) {
    return AstError<int64_t>(
        "GoogleSQL AST: ASSERT_ROWS_MODIFIED without count");
  }
  return ParseIntLiteral(*literal);
}

StatusOr<std::unique_ptr<Statement>> VisitInsert(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  if (path == nullptr) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: INSERT table missing");
  }
  std::vector<std::string> columns;
  if (const GoogleSqlAstNode* list = root.Child("ColumnList")) {
    for (const GoogleSqlAstNode* name : list->Children("Identifier")) {
      ASSIGN_OR_RETURN(std::string, hv218946_0, (Identifier(*name)));
      columns.push_back(std::move(hv218946_0));
    }
  }
  std::vector<std::vector<Expression>> rows;
  const GoogleSqlAstNode* row_list = root.Child("InsertValuesRowList");
  if (row_list != nullptr) {
    for (const GoogleSqlAstNode* row : row_list->Children("InsertValuesRow")) {
      std::vector<Expression> values;
      for (const auto& value : row->children) {
        if (value->kind == "Location" || value->kind == "Hint") {
          continue;
        }
        ASSIGN_OR_RETURN(Expression, hv219408_0, (VisitExpression(*value)));
        values.push_back(std::move(hv219408_0));
      }
      rows.push_back(std::move(values));
    }
  }
  ASSIGN_OR_RETURN(std::string, hv219518_0, (Path(*path)));
  auto statement = std::make_unique<InsertStatement>(
      std::move(hv219518_0), std::move(rows), std::move(columns));
  statement->SetMode(InsertModeFromDetail(root.detail));
  if (const GoogleSqlAstNode* query = root.Child("Query")) {
    ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv219747_0,
                     (VisitQuery(*query)));
    statement->SetQuery(std::move(hv219747_0));
  }
  if (const GoogleSqlAstNode* assert = root.Child("AssertRowsModified")) {
    ASSIGN_OR_RETURN(int64_t, hv219871_0, (AssertRowsModifiedValue(*assert)));
    statement->SetAssertRowsModified(hv219871_0);
  }
  return statement;
}

StatusOr<std::unique_ptr<Statement>> VisitUpdate(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  const GoogleSqlAstNode* items = root.Child("UpdateItemList");
  if (path == nullptr || items == nullptr) {
    return AstError<std::unique_ptr<Statement>>("GoogleSQL AST: bad UPDATE");
  }
  std::vector<std::pair<ColumnName, Expression>> assignments;
  std::vector<NestedDmlItem> nested_items;
  // Extracts "WHERE <expr>" plus an optional ASSERT_ROWS_MODIFIED from a
  // nested DELETE/UPDATE statement node; remaining children are ignored
  // because nested targets are columns, not relations (no alias/RETURNING).
  auto parse_nested_tail = [](const GoogleSqlAstNode& node,
                              Expression* predicate,
                              int64_t* assert_rows) -> Status {
    std::vector<const GoogleSqlAstNode*> candidates;
    const GoogleSqlAstNode* nested_target = node.Child("PathExpression");
    for (const auto& child : node.children) {
      // Identity skip for the target: bare-column predicates (`WHERE flag`)
      // are PathExpressions too (see VisitDelete).
      if (child.get() == nested_target || child->kind == "UpdateItemList" ||
          child->kind == "Location" || child->kind == "Hint" ||
          child->kind == "AssertRowsModified") {
        continue;
      }
      if (child->kind == "Alias" || child->kind == "ReturningClause" ||
          child->kind == "FromClause") {
        continue;
      }
      candidates.push_back(child.get());
    }
    if (candidates.size() > 1) {
      return AstStatus(
          "GoogleSQL AST: multiple nested WHERE clause candidates");
    }
    if (!candidates.empty()) {
      ASSIGN_OR_RETURN(Expression, hv221454_0,
                       (VisitExpression(*candidates.front())));
      *predicate = std::move(hv221454_0);
    }
    if (const GoogleSqlAstNode* assert = node.Child("AssertRowsModified")) {
      ASSIGN_OR_RETURN(int64_t, hv221594_0, (AssertRowsModifiedValue(*assert)));
      *assert_rows = hv221594_0;
    }
    return Status::kSuccess;
  };
  for (const GoogleSqlAstNode* item : items->Children("UpdateItem")) {
    const GoogleSqlAstNode* set = item->Child("UpdateSetValue");
    if (set != nullptr && set->children.size() == 2) {
      ASSIGN_OR_RETURN(Expression, hv221851_0,
                       (VisitExpression(*set->children[1])));
      ASSIGN_OR_RETURN(std::string, hv221851_1, (Path(*set->children[0])));
      assignments.emplace_back(ColumnName(std::move(hv221851_1)),
                               std::move(hv221851_0));
      continue;
    }
    // Nested DML: SET (DELETE arr WHERE ...), SET (UPDATE arr SET ... WHERE
    // ...) and SET (INSERT arr VALUES ... / (SELECT ...)).
    if (const GoogleSqlAstNode* nested = item->Child("DeleteStatement")) {
      const GoogleSqlAstNode* target = nested->Child("PathExpression");
      if (target == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: nested DELETE without target");
      }
      NestedDmlItem parsed;
      parsed.kind = NestedDmlItem::Kind::kDelete;
      ASSIGN_OR_RETURN(std::string, hv222510_0, (Path(*target)));
      parsed.target_path = std::move(hv222510_0);
      if (Status st_tail = parse_nested_tail(*nested, &parsed.predicate,
                                             &parsed.assert_rows_modified);
          !st_tail.ok()) {
        return st_tail;
      }
      nested_items.push_back(std::move(parsed));
      continue;
    }
    if (const GoogleSqlAstNode* nested = item->Child("UpdateStatement")) {
      const GoogleSqlAstNode* target = nested->Child("PathExpression");
      const GoogleSqlAstNode* inner_items = nested->Child("UpdateItemList");
      if (target == nullptr || inner_items == nullptr ||
          inner_items->Children("UpdateItem").size() != 1) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: bad nested UPDATE assignment");
      }
      const GoogleSqlAstNode* inner_set =
          inner_items->Children("UpdateItem").front()->Child("UpdateSetValue");
      if (inner_set == nullptr || inner_set->children.size() != 2) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: bad nested UPDATE assignment");
      }
      NestedDmlItem parsed;
      parsed.kind = NestedDmlItem::Kind::kUpdate;
      ASSIGN_OR_RETURN(std::string, hv223557_0, (Path(*target)));
      parsed.target_path = std::move(hv223557_0);
      ASSIGN_OR_RETURN(Expression, hv223599_0,
                       (VisitExpression(*inner_set->children[1])));
      parsed.set_value = std::move(hv223599_0);
      if (Status st_tail = parse_nested_tail(*nested, &parsed.predicate,
                                             &parsed.assert_rows_modified);
          !st_tail.ok()) {
        return st_tail;
      }
      nested_items.push_back(std::move(parsed));
      continue;
    }
    if (const GoogleSqlAstNode* nested = item->Child("InsertStatement")) {
      const GoogleSqlAstNode* target = nested->Child("PathExpression");
      if (target == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: nested INSERT without target");
      }
      NestedDmlItem parsed;
      parsed.kind = NestedDmlItem::Kind::kInsert;
      ASSIGN_OR_RETURN(std::string, hv224208_0, (Path(*target)));
      parsed.target_path = std::move(hv224208_0);
      if (const GoogleSqlAstNode* row_list =
              nested->Child("InsertValuesRowList")) {
        for (const GoogleSqlAstNode* row :
             row_list->Children("InsertValuesRow")) {
          std::vector<Expression> values;
          for (const auto& value : row->children) {
            if (value->kind == "Location" || value->kind == "Hint") {
              continue;
            }
            ASSIGN_OR_RETURN(Expression, hv224648_0, (VisitExpression(*value)));
            values.push_back(std::move(hv224648_0));
          }
          parsed.insert_values.push_back(std::move(values));
        }
      }
      if (const GoogleSqlAstNode* query = nested->Child("Query")) {
        ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv224862_0,
                         (VisitQuery(*query)));
        parsed.insert_query = std::move(hv224862_0);
      }
      if (parsed.insert_values.empty() && parsed.insert_query == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: nested INSERT without values or query");
      }
      if (const GoogleSqlAstNode* assert =
              nested->Child("AssertRowsModified")) {
        ASSIGN_OR_RETURN(int64_t, hv225209_0,
                         (AssertRowsModifiedValue(*assert)));
        parsed.assert_rows_modified = hv225209_0;
      }
      nested_items.push_back(std::move(parsed));
      continue;
    }
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: bad UPDATE assignment");
  }
  // The WHERE clause is the single remaining child once the target path and
  // the assignment list are removed. Table aliases, ASSERT_ROWS_MODIFIED,
  // THEN RETURN and UPDATE...FROM siblings are recognized but not mapped
  // (returning/join-update are gated features); they must not masquerade as
  // WHERE candidates.
  std::vector<const GoogleSqlAstNode*> where_candidates;
  for (const auto& child : root.children) {
    // The target is skipped by IDENTITY: `WHERE flag` also arrives as a
    // PathExpression, and a kind-based skip dropped bare-column predicates
    // (the UPDATE then touched every row).
    if (child.get() == path || child->kind == "UpdateItemList" ||
        child->kind == "Location" || child->kind == "Hint" ||
        child->kind == "Alias" || child->kind == "AssertRowsModified" ||
        child->kind == "ReturningClause" || child->kind == "FromClause") {
      continue;
    }
    where_candidates.push_back(child.get());
  }
  if (where_candidates.size() > 1) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: multiple UPDATE WHERE clause candidates");
  }
  Expression where;
  if (!where_candidates.empty()) {
    ASSIGN_OR_RETURN(Expression, hv226442_0,
                     (VisitExpression(*where_candidates.front())));
    where = std::move(hv226442_0);
  }
  // UPDATE ... FROM desugars to MERGE: `USING <from> ON (<where>) WHEN
  // MATCHED THEN UPDATE SET ...`.  BigQuery UPDATE...FROM errors when one
  // target row matches several source rows; MERGE's one-target-one-action
  // contract enforces exactly that.  Reusing MERGE keeps FK enforcement,
  // plan-cache rebinding, and the server command tag on one path.
  if (const GoogleSqlAstNode* from_clause = root.Child("FromClause")) {
    if (root.Child("Alias") != nullptr) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: UPDATE ... FROM with a target alias is not "
          "supported; qualify target columns with the table name");
    }
    if (!nested_items.empty()) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: UPDATE ... FROM does not support nested DML");
    }
    if (root.Child("AssertRowsModified") != nullptr) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: UPDATE ... FROM does not support "
          "ASSERT_ROWS_MODIFIED");
    }
    ASSIGN_OR_RETURN(std::string, update_from_target, (Path(*path)));
    const GoogleSqlAstNode* source_table =
        from_clause->Child("TablePathExpression");
    const GoogleSqlAstNode* source_subquery =
        from_clause->Child("TableSubquery");
    if (source_table == nullptr && source_subquery == nullptr) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: bad UPDATE ... FROM source");
    }
    for (const auto& child : from_clause->children) {
      if (child.get() == source_table || child.get() == source_subquery ||
          child->kind == "Location" || child->kind == "Hint" ||
          (source_table != nullptr && child->kind == "Alias")) {
        continue;
      }
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: UPDATE ... FROM supports a single from item");
    }
    std::shared_ptr<SelectStatement> source;
    std::string source_alias;
    if (source_table != nullptr) {
      const GoogleSqlAstNode* source_path =
          source_table->Child("PathExpression");
      if (source_path == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: bad UPDATE ... FROM source");
      }
      ASSIGN_OR_RETURN(std::string, source_name, Path(*source_path));
      source_alias = source_name;
      source = std::make_shared<SelectStatement>(
          std::vector<NamedExpression>{
              NamedExpression("", ColumnValueExp("*"))},
          std::vector<std::string>{source_name}, Expression{});
      if (const GoogleSqlAstNode* alias = source_table->Child("Alias")) {
        if (const GoogleSqlAstNode* id = alias->Child("Identifier")) {
          ASSIGN_OR_RETURN(std::string, hv226910_0, (Identifier(*id)));
          source_alias = std::move(hv226910_0);
        }
      }
    } else {
      const GoogleSqlAstNode* query_node = source_subquery->Child("Query");
      if (query_node == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: bad UPDATE ... FROM source subquery");
      }
      ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, update_from_tmp,
                       (VisitQuery(*query_node)));
      source = std::move(update_from_tmp);
      ASSIGN_OR_RETURN(std::string, update_from_alias,
                       (Alias(*source_subquery)));
      source_alias = std::move(update_from_alias);
      if (source_alias.empty()) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: UPDATE ... FROM subquery requires an alias");
      }
    }
    Expression on_condition = where;
    if (!on_condition) {
      on_condition = ConstantValueExp(Value(true));
    }
    MergeWhenClause matched;
    matched.match_type = MergeWhenClause::MatchType::kMatched;
    matched.action.kind = MergeActionKind::kUpdate;
    matched.action.set_clause = std::move(assignments);
    auto merge = std::make_unique<MergeStatement>(
        update_from_target, std::move(source), std::move(source_alias),
        std::move(on_condition));
    merge->SetDesugaredFromUpdate(true);
    merge->SetClauses({std::move(matched)});
    return merge;
  }
  ASSIGN_OR_RETURN(std::string, hv226502_0, (Path(*path)));
  auto statement = std::make_unique<UpdateStatement>(
      std::move(hv226502_0), std::move(assignments), std::move(where));
  statement->SetNestedItems(std::move(nested_items));
  if (const GoogleSqlAstNode* alias = root.Child("Alias")) {
    if (const GoogleSqlAstNode* id = alias->Child("Identifier")) {
      ASSIGN_OR_RETURN(std::string, hv226800_0, (Identifier(*id)));
      statement->SetAlias(std::move(hv226800_0));
    }
  }
  if (const GoogleSqlAstNode* assert = root.Child("AssertRowsModified")) {
    ASSIGN_OR_RETURN(int64_t, hv226929_0, (AssertRowsModifiedValue(*assert)));
    statement->SetAssertRowsModified(hv226929_0);
  }
  return statement;
}

StatusOr<std::unique_ptr<Statement>> VisitDelete(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  if (path == nullptr) {
    return AstError<std::unique_ptr<Statement>>("GoogleSQL AST: bad DELETE");
  }
  std::vector<const GoogleSqlAstNode*> where_candidates;
  for (const auto& child : root.children) {
    // Skip the target by IDENTITY, not by kind: a bare-column predicate
    // (`WHERE flag`) is itself a PathExpression in the dump, and skipping
    // every PathExpression silently dropped it -- the DELETE then matched
    // every row (sql_oracle_fuzz find: DQE DELETE WHERE flag deleted the
    // whole table).
    if (child.get() == path || child->kind == "Location" ||
        child->kind == "Hint" || child->kind == "Alias" ||
        child->kind == "AssertRowsModified" ||
        child->kind == "ReturningClause") {
      continue;
    }
    if (child->kind == "FromClause") {
      // GoogleSQL has no DELETE ... USING.  Dropping the clause silently would
      // turn `DELETE FROM t USING u WHERE ...` into a delete whose WHERE can no
      // longer see the join, which deletes the wrong rows.  The parser rejects
      // the syntax, so reaching here means a future grammar or a hand-built
      // AST; either way the safe answer is an explicit error that names the
      // supported spelling.
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: DELETE ... USING is not part of GoogleSQL; write the "
          "join as a subquery (WHERE EXISTS (SELECT 1 FROM u WHERE u.k = "
          "t.k))");
    }
    where_candidates.push_back(child.get());
  }
  if (where_candidates.size() > 1) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: multiple DELETE WHERE clause candidates");
  }
  Expression where;
  if (!where_candidates.empty()) {
    ASSIGN_OR_RETURN(Expression, hv227907_0,
                     (VisitExpression(*where_candidates.front())));
    where = std::move(hv227907_0);
  }
  ASSIGN_OR_RETURN(std::string, hv227967_0, (Path(*path)));
  auto statement = std::make_unique<DeleteStatement>(std::move(hv227967_0),
                                                     std::move(where));
  if (const GoogleSqlAstNode* alias = root.Child("Alias")) {
    if (const GoogleSqlAstNode* id = alias->Child("Identifier")) {
      ASSIGN_OR_RETURN(std::string, hv228186_0, (Identifier(*id)));
      statement->SetAlias(std::move(hv228186_0));
    }
  }
  if (const GoogleSqlAstNode* assert = root.Child("AssertRowsModified")) {
    ASSIGN_OR_RETURN(int64_t, hv228315_0, (AssertRowsModifiedValue(*assert)));
    statement->SetAssertRowsModified(hv228315_0);
  }
  return statement;
}

// ALTER TABLE.  Catalog-level actions (RENAME TO, RENAME COLUMN) map to
// AlterTableStatement; row-layout actions (ADD/DROP COLUMN) stay explicit
// errors until the rebuild path lands — the earlier behavior silently
// rejected the whole statement as an unsupported node kind.
StatusOr<std::unique_ptr<Statement>> VisitAlterTable(
    const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  const GoogleSqlAstNode* actions = root.Child("AlterActionList");
  if (path == nullptr || actions == nullptr) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: bad ALTER TABLE");
  }
  ASSIGN_OR_RETURN(std::string, table_name, Path(*path));
  size_t action_count = 0;
  AlterTableAction parsed;
  for (const auto& action_holder : actions->children) {
    const GoogleSqlAstNode* action = action_holder.get();
    if (action->kind == "Location" || action->kind == "Hint") {
      continue;
    }
    if (++action_count > 1) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: exactly one ALTER action is supported per "
          "statement");
    }
    if (action->kind == "RenameToClause") {
      const GoogleSqlAstNode* new_path = action->Child("PathExpression");
      if (new_path == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: bad RENAME TO");
      }
      ASSIGN_OR_RETURN(std::string, new_name, Path(*new_path));
      parsed.kind = AlterTableAction::Kind::kRenameTo;
      parsed.new_name = std::move(new_name);
      continue;
    }
    if (action->kind == "RenameColumnAction") {
      const auto& identifiers = action->Children("Identifier");
      if (identifiers.size() != 2) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: bad RENAME COLUMN");
      }
      ASSIGN_OR_RETURN(std::string, old_name, Identifier(*identifiers[0]));
      ASSIGN_OR_RETURN(std::string, new_name, Identifier(*identifiers[1]));
      parsed.kind = AlterTableAction::Kind::kRenameColumn;
      parsed.old_column = std::move(old_name);
      parsed.new_name = std::move(new_name);
      continue;
    }
    if (action->kind == "AddColumnAction") {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: ALTER TABLE ADD COLUMN is not supported (a "
          "table rebuild path is required; create a new table and "
          "INSERT SELECT instead)");
    }
    if (action->kind == "DropColumnAction") {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: ALTER TABLE DROP COLUMN is not supported (a "
          "table rebuild path is required; create a new table and "
          "INSERT SELECT instead)");
    }
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: unsupported ALTER TABLE action " + action->kind);
  }
  if (action_count == 0) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: ALTER TABLE requires an action");
  }
  return std::make_unique<AlterTableStatement>(std::move(table_name),
                                               std::move(parsed));
}

StatusOr<std::unique_ptr<Statement>> VisitMerge(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* target_path = root.Child("PathExpression");
  if (target_path == nullptr) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: bad MERGE target");
  }
  ASSIGN_OR_RETURN(std::string, merge_target, Path(*target_path));
  const GoogleSqlAstNode* source_table = root.Child("TablePathExpression");
  const GoogleSqlAstNode* source_subquery = root.Child("TableSubquery");
  if (source_table == nullptr && source_subquery == nullptr) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: bad MERGE source");
  }
  std::shared_ptr<SelectStatement> source;
  std::string source_alias;
  if (source_table != nullptr) {
    const GoogleSqlAstNode* source_path = source_table->Child("PathExpression");
    if (source_path == nullptr) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: bad MERGE source");
    }
    ASSIGN_OR_RETURN(std::string, source_name, Path(*source_path));
    source_alias = source_name;
    // A bare table source reads as SELECT * FROM <table>; the star expands
    // during the prepare-time relation materialization in SqlEngine.
    source = std::make_shared<SelectStatement>(
        std::vector<NamedExpression>{NamedExpression("", ColumnValueExp("*"))},
        std::vector<std::string>{source_name}, Expression{});
  } else {
    const GoogleSqlAstNode* query_node = source_subquery->Child("Query");
    if (query_node == nullptr) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: bad MERGE source subquery");
    }
    ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, merge_source_tmp,
                     (VisitQuery(*query_node)));
    source = std::move(merge_source_tmp);
    ASSIGN_OR_RETURN(std::string, merge_alias_tmp, (Alias(*source_subquery)));
    source_alias = std::move(merge_alias_tmp);
    if (source_alias.empty()) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: MERGE source subquery requires an alias");
    }
  }
  const GoogleSqlAstNode* clause_list = root.Child("MergeWhenClauseList");
  if (clause_list == nullptr) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: bad MERGE clause list");
  }
  // ON condition: the free-form expression child between the source and the
  // clause list (its dump kind varies, so find it positionally).
  const GoogleSqlAstNode* on_node = nullptr;
  for (const auto& child : root.children) {
    if (child.get() == target_path || child.get() == source_table ||
        child.get() == source_subquery || child->kind == "Location" ||
        child->kind == "Hint" || child->kind == "MergeWhenClauseList") {
      continue;
    }
    on_node = child.get();
    break;
  }
  if (on_node == nullptr) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: bad MERGE ON condition");
  }
  Expression on_condition;
  ASSIGN_OR_RETURN(Expression, merge_on_tmp, (VisitExpression(*on_node)));
  on_condition = std::move(merge_on_tmp);

  std::vector<MergeWhenClause> clauses;
  for (const GoogleSqlAstNode* clause_node :
       clause_list->Children("MergeWhenClause")) {
    MergeWhenClause parsed;
    const std::string& match = clause_node->detail;
    if (match == "match_type=NOT_MATCHED_BY_TARGET") {
      parsed.match_type = MergeWhenClause::MatchType::kNotMatchedByTarget;
    } else if (match == "match_type=NOT_MATCHED_BY_SOURCE") {
      parsed.match_type = MergeWhenClause::MatchType::kNotMatchedBySource;
    } else if (match == "match_type=MATCHED") {
      parsed.match_type = MergeWhenClause::MatchType::kMatched;
    } else {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: unknown MERGE match type " + match);
    }
    const GoogleSqlAstNode* action_node = clause_node->Child("MergeAction");
    if (action_node == nullptr) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: bad MERGE action");
    }
    for (const auto& child : clause_node->children) {
      if (child.get() == action_node || child->kind == "Location" ||
          child->kind == "Hint") {
        break;
      }
      // Search conditions precede the action; at most one condition node.
      Expression condition;
      ASSIGN_OR_RETURN(Expression, merge_cond_tmp, (VisitExpression(*child)));
      condition = std::move(merge_cond_tmp);
      parsed.condition = std::move(condition);
      break;
    }
    const std::string& action_kind = action_node->detail;
    if (action_kind == "UPDATE") {
      if (parsed.match_type != MergeWhenClause::MatchType::kMatched &&
          parsed.match_type !=
              MergeWhenClause::MatchType::kNotMatchedBySource) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: MERGE WHEN [NOT] MATCHED [BY TARGET] cannot "
            "UPDATE");
      }
      parsed.action.kind = MergeActionKind::kUpdate;
      const GoogleSqlAstNode* items = action_node->Child("UpdateItemList");
      if (items == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: bad MERGE UPDATE SET");
      }
      for (const GoogleSqlAstNode* item : items->Children("UpdateItem")) {
        const GoogleSqlAstNode* assignment = item->Child("UpdateSetValue");
        if (assignment == nullptr || assignment->children.size() < 2) {
          return AstError<std::unique_ptr<Statement>>(
              "GoogleSQL AST: bad MERGE UPDATE SET item");
        }
        const GoogleSqlAstNode* target_column = assignment->children[0].get();
        if (target_column->kind != "PathExpression") {
          return AstError<std::unique_ptr<Statement>>(
              "GoogleSQL AST: MERGE UPDATE SET target must be a column");
        }
        ASSIGN_OR_RETURN(std::string, merge_col_tmp, Path(*target_column));
        std::string column_path = std::move(merge_col_tmp);
        if (column_path.find('.') != std::string::npos) {
          return AstError<std::unique_ptr<Statement>>(
              "GoogleSQL AST: MERGE UPDATE SET does not support dotted "
              "field targets");
        }
        Expression value;
        ASSIGN_OR_RETURN(Expression, merge_val_tmp,
                         (VisitExpression(*assignment->children[1].get())));
        value = std::move(merge_val_tmp);
        parsed.action.set_clause.emplace_back(ColumnName("", column_path),
                                              std::move(value));
      }
    } else if (action_kind == "DELETE") {
      if (parsed.match_type != MergeWhenClause::MatchType::kMatched &&
          parsed.match_type !=
              MergeWhenClause::MatchType::kNotMatchedBySource) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: MERGE WHEN [NOT] MATCHED [BY TARGET] cannot "
            "DELETE");
      }
      parsed.action.kind = MergeActionKind::kDelete;
    } else if (action_kind == "INSERT") {
      if (parsed.match_type !=
          MergeWhenClause::MatchType::kNotMatchedByTarget) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: MERGE WHEN MATCHED / BY SOURCE cannot INSERT");
      }
      parsed.action.kind = MergeActionKind::kInsert;
      if (const GoogleSqlAstNode* columns = action_node->Child("ColumnList")) {
        for (const GoogleSqlAstNode* identifier :
             columns->Children("Identifier")) {
          ASSIGN_OR_RETURN(std::string, merge_ident_tmp,
                           (Identifier(*identifier)));
          parsed.action.insert_columns.push_back(std::move(merge_ident_tmp));
        }
      }
      const GoogleSqlAstNode* row = action_node->Child("InsertValuesRow");
      if (row == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: MERGE INSERT ... SELECT is not supported");
      }
      for (const auto& value : row->children) {
        Expression expression;
        ASSIGN_OR_RETURN(Expression, merge_expr_tmp, (VisitExpression(*value)));
        expression = std::move(merge_expr_tmp);
        parsed.action.insert_values.push_back(std::move(expression));
      }
    } else {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: unknown MERGE action " + action_kind);
    }
    clauses.push_back(std::move(parsed));
  }
  auto statement = std::make_unique<MergeStatement>(
      std::move(merge_target), std::move(source), std::move(source_alias),
      std::move(on_condition));
  statement->SetClauses(std::move(clauses));
  return statement;
}

}  // namespace tinylamb::ast_visitor_detail
