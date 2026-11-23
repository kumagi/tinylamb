/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/grouped_pipeline_lowering.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/log_message.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/pipeline/ooc_sinks.hpp"
#include "executor/detail/pipeline/pipeline_ops.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/pipeline_lowering.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/collation.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "type/column.hpp"
#include "type/type.hpp"

namespace tinylamb {
namespace {

ValueType InferExprValueType(const Expression& expr, const Schema& schema) {
  if (!expr) {
    return ValueType::kVarChar;
  }
  if (expr->Type() == TypeTag::kColumnValue) {
    const int off = schema.Offset(expr->AsColumnValue().GetColumnName());
    if (off >= 0 && std::cmp_less(off, schema.ColumnCount())) {
      return schema.GetColumn(static_cast<size_t>(off)).Type();
    }
  }
  try {
    switch (expr->ResultType(schema).GetType()) {
      case TypeTag::kInteger:
      case TypeTag::kBigInt:
        return ValueType::kInt64;
      case TypeTag::kDouble:
        return ValueType::kDouble;
      case TypeTag::kDate:
        return ValueType::kDate;
      case TypeTag::kArray:
        return ValueType::kArray;
      // BOOL/TIMESTAMP/INTERVAL are not separate ValueTypes: they are stored
      // as kInt64/kVarChar, so their ResultType never needs another mapping.
      // The remaining tags are expression *node* kinds (or kInvalid), not
      // value types, and kVarChar is the conservative schema fallback.
      default:
        return ValueType::kVarChar;
    }
  } catch (const std::exception& e) {
    // Keep the conservative VARCHAR fallback, but never hide the reason: a
    // schema/type bug would otherwise surface only as a wrong sort order.
    LOG(WARN) << "InferExprValueType: ResultType threw for " << e.what();
    return ValueType::kVarChar;
  } catch (...) {
    LOG(WARN) << "InferExprValueType: ResultType threw an unknown exception";
    return ValueType::kVarChar;
  }
}

ValueType InferAggValueType(const AggregateExpression& agg,
                            const Schema& schema) {
  // P461/501: canonical result-kind table shared with
  // AggregateExpression::ResultType and the plan/executor schema sources.
  // (Differences vs. the old local table: PERCENTILE_DISC now follows the
  // input value type, statistical REGR_* helpers declare DOUBLE, sketch
  // inits/merges declare the serialized VARCHAR bytes, HLL_MERGE declares
  // its INT64 cardinality, and the approx array aggregates declare ARRAY.)
  if (const auto kind = CanonicalAggregateResultKind(agg.GetType());
      kind.has_value()) {
    return *kind;
  }
  if (!agg.Child()) {
    return ValueType::kInt64;
  }
  return InferExprValueType(agg.Child(), schema);
}

Expression MakeSynthCol(const std::string& name) {
  return ColumnValueExp(ColumnName("", name));
}

bool ValidateCoreExpr(const Expression& expr, const Schema& core_schema,
                      bool* all_ok) {
  if (!expr) {
    return true;
  }
  switch (expr->Type()) {
    case TypeTag::kConstantValue:
      return true;
    case TypeTag::kColumnValue: {
      const ColumnName& col = expr->AsColumnValue().GetColumnName();
      if (col.name == "*") {
        // A wildcard is not a resolvable group key column (SELECT-list '*'
        // is rejected elsewhere); keep the same guard for GROUP BY.
        *all_ok = false;
        return false;
      }
      if (core_schema.Offset(col) < 0) {
        *all_ok = false;
        return false;
      }
      return true;
    }
    case TypeTag::kBinaryExp:
      return ValidateCoreExpr(expr->AsBinaryExpression().Left(), core_schema,
                              all_ok) &&
             ValidateCoreExpr(expr->AsBinaryExpression().Right(), core_schema,
                              all_ok);
    case TypeTag::kUnaryExp:
      return ValidateCoreExpr(expr->AsUnaryExpression().Child(), core_schema,
                              all_ok);
    case TypeTag::kCastExp:
      return ValidateCoreExpr(expr->AsCastExpression().Child(), core_schema,
                              all_ok);
    case TypeTag::kCaseExp: {
      const auto& ce = expr->AsCaseExpression();
      for (const auto& [cond, res] : ce.when_clauses_) {
        if (!ValidateCoreExpr(cond, core_schema, all_ok) ||
            !ValidateCoreExpr(res, core_schema, all_ok)) {
          return false;
        }
      }
      return ValidateCoreExpr(ce.else_clause_, core_schema, all_ok);
    }
    case TypeTag::kInExp: {
      const auto& ie = expr->AsInExpression();
      if (!ValidateCoreExpr(ie.child_, core_schema, all_ok)) {
        return false;
      }
      return std::ranges::all_of(ie.list_, [&](const auto& item) {
        return ValidateCoreExpr(item, core_schema, all_ok);
      });
    }
    case TypeTag::kFunctionCallExp:
      for (const auto& arg : expr->AsFunctionCallExpression().Args()) {
        if (!ValidateCoreExpr(arg, core_schema, all_ok)) {
          return false;
        }
      }
      return true;
    case TypeTag::kArrayExp:
      for (const auto& el : expr->AsArrayExpression().Elements()) {
        if (!ValidateCoreExpr(el, core_schema, all_ok)) {
          return false;
        }
      }
      return true;
    default:
      *all_ok = false;
      return false;
  }
}

struct GroupedRewriter {
  const Schema& core_schema;
  std::vector<Expression> group_keys;
  std::unordered_map<std::string, size_t> group_by_str;
  std::unordered_map<slot_t, size_t> group_by_core_slot;
  std::vector<Expression> agg_exprs;
  std::unordered_map<std::string, size_t> agg_by_str;
  bool valid{true};

  void RegisterGroupKey(const Expression& expr) {
    const std::string key = expr->ToString();
    if (group_by_str.contains(key)) {
      return;
    }
    const size_t idx = group_keys.size();
    bool ok = true;
    if (!ValidateCoreExpr(expr, core_schema, &ok) || !ok) {
      valid = false;
      return;
    }
    group_keys.push_back(expr);
    group_by_str.emplace(key, idx);
    if (expr->Type() == TypeTag::kColumnValue) {
      const int off = core_schema.Offset(expr->AsColumnValue().GetColumnName());
      if (off >= 0) {
        group_by_core_slot.emplace(static_cast<slot_t>(off), idx);
      }
    }
  }

  Expression Rewrite(const Expression& expr) {
    if (!valid || !expr) {
      return expr;
    }
    const std::string s = expr->ToString();
    if (const auto it = group_by_str.find(s); it != group_by_str.end()) {
      return MakeSynthCol("__tinylamb_grp_" + std::to_string(it->second));
    }
    if (expr->Type() == TypeTag::kColumnValue) {
      const ColumnName& col = expr->AsColumnValue().GetColumnName();
      const int off = core_schema.Offset(col);
      if (off >= 0) {
        if (const auto sit = group_by_core_slot.find(static_cast<slot_t>(off));
            sit != group_by_core_slot.end()) {
          return MakeSynthCol("__tinylamb_grp_" + std::to_string(sit->second));
        }
      }
      // Bare non-grouped column -> representative-row semantics not lowered in
      // P3a.
      valid = false;
      return {};
    }
    if (expr->Type() == TypeTag::kAggregateExp) {
      const auto& ae = expr->AsAggregateExpression();
      if (ae.NeedsGroupContext() || ae.HasInnerGroupBy() ||
          ae.Having() != AggregateHavingModifier::kNone ||
          !ae.InnerOrderBy().empty() || ae.InnerLimit().has_value() ||
          ae.SecondaryArg() || !ae.TrailingArgs().empty()) {
        valid = false;
        return {};
      }
      if (!ValidateCoreExpr(ae.Child(), core_schema, &valid) ||
          !ValidateCoreExpr(ae.WhereFilter(), core_schema, &valid) || !valid) {
        valid = false;
        return {};
      }
      size_t idx = 0;
      if (const auto ait = agg_by_str.find(s); ait != agg_by_str.end()) {
        idx = ait->second;
      } else {
        idx = agg_exprs.size();
        agg_exprs.push_back(expr);
        agg_by_str.emplace(s, idx);
      }
      return MakeSynthCol("__tinylamb_agg_" + std::to_string(idx));
    }
    if (expr->Type() == TypeTag::kConstantValue) {
      return expr;
    }
    switch (expr->Type()) {
      case TypeTag::kBinaryExp: {
        const auto& b = expr->AsBinaryExpression();
        return BinaryExpressionExp(Rewrite(b.Left()), b.Op(),
                                   Rewrite(b.Right()));
      }
      case TypeTag::kUnaryExp: {
        const auto& u = expr->AsUnaryExpression();
        return UnaryExpressionExp(Rewrite(u.Child()), u.Op());
      }
      case TypeTag::kCastExp: {
        const auto& c = expr->AsCastExpression();
        return CastExpressionExp(Rewrite(c.Child()), c.TargetTypeName(),
                                 c.ReturnNullOnError());
      }
      case TypeTag::kCaseExp: {
        const auto& c = expr->AsCaseExpression();
        std::vector<std::pair<Expression, Expression>> clauses;
        clauses.reserve(c.when_clauses_.size());
        for (const auto& [cond, res] : c.when_clauses_) {
          clauses.emplace_back(Rewrite(cond), Rewrite(res));
        }
        return CaseExpressionExp(std::move(clauses), Rewrite(c.else_clause_));
      }
      case TypeTag::kInExp: {
        const auto& in = expr->AsInExpression();
        std::vector<Expression> items;
        items.reserve(in.list_.size());
        for (const auto& item : in.list_) {
          items.push_back(Rewrite(item));
        }
        return InExpressionExp(Rewrite(in.child_), std::move(items));
      }
      case TypeTag::kFunctionCallExp: {
        const auto& fn = expr->AsFunctionCallExpression();
        std::vector<Expression> args;
        args.reserve(fn.Args().size());
        for (const auto& arg : fn.Args()) {
          args.push_back(Rewrite(arg));
        }
        return FunctionCallExp(fn.FuncName(), std::move(args));
      }
      case TypeTag::kArrayExp: {
        const auto& arr = expr->AsArrayExpression();
        std::vector<Expression> elems;
        elems.reserve(arr.Elements().size());
        for (const auto& el : arr.Elements()) {
          elems.push_back(Rewrite(el));
        }
        return ArrayExpressionExp(std::move(elems), arr.ElementSqlType());
      }
      default:
        valid = false;
        return {};
    }
  }
};

}  // namespace

StatusOr<std::optional<PipelineGraph>> TryLowerGroupedStatementToPipeline(
    const SelectStatement& select, const Plan& core_plan,
    TransactionContext& ctx, std::function<std::string()> explain_text_producer,
    const std::function<Expression(const Expression&)>& resolve_alias) {
  if (!core_plan || !select.WithQueries().empty() || select.WithTies() ||
      select.Qualify() != nullptr || select.HasDistinctOn() ||
      !select.UnionAll().empty() || select.GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(select)) {
    return std::optional<PipelineGraph>(std::nullopt);
  }
  if (select.Distinct() && !select.OrderBy().empty()) {
    // DISTINCT before Sort on the OOC lane picks a different survivor
    // representative for groups sharing the visible prefix than the
    // relational lane's first-occurrence fold (the hidden __ord_ payload of
    // the survivor decides the final sort position), inverting the row
    // order between lanes. Delegate the combination to the relational path.
    return std::optional<PipelineGraph>(std::nullopt);
  }
  for (const NamedExpression& item : select.SelectList()) {
    if (!item.expression || item.star_modifiers != nullptr) {
      return std::optional<PipelineGraph>(std::nullopt);
    }
    if (item.expression->Type() == TypeTag::kColumnValue &&
        item.expression->AsColumnValue().GetColumnName().name == "*") {
      return std::optional<PipelineGraph>(std::nullopt);
    }
  }

  auto sub_alias = [&](const Expression& e) -> Expression {
    return resolve_alias ? resolve_alias(e) : e;
  };

  const Schema& core_schema = core_plan->GetSchema();
  GroupedRewriter rewriter{
      .core_schema = core_schema,
      .group_keys = {},
      .group_by_str = {},
      .group_by_core_slot = {},
      .agg_exprs = {},
      .agg_by_str = {},
      .valid = true,
  };
  rewriter.agg_exprs.reserve(select.SelectList().size());
  if (!select.IsGroupAll()) {
    rewriter.group_keys.reserve(select.GroupBy().size());
    for (const Expression& g : select.GroupBy()) {
      rewriter.RegisterGroupKey(sub_alias(g));
      if (!rewriter.valid) {
        return std::optional<PipelineGraph>(std::nullopt);
      }
    }
  }

  // Expand SELECT-list output aliases in HAVING / ORDER BY before rewriting,
  // unless an alias name shadows a grouped column already registered.
  auto lower_name = [](const std::string& name) {
    std::string lowered = name;
    for (char& c : lowered) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return lowered;
  };
  std::unordered_map<std::string, Expression> select_aliases;
  for (const NamedExpression& item : select.SelectList()) {
    if (!item.name.empty() && item.expression) {
      // Aliases are case-insensitive, just like identifier resolution in the
      // legacy lane. Duplicate aliases are invalid SQL; the general path
      // reports the error, so decline the pipeline rather than silently
      // picking one of the two expressions for HAVING/ORDER BY.
      if (!select_aliases.emplace(lower_name(item.name), item.expression)
               .second) {
        return std::optional<PipelineGraph>(std::nullopt);
      }
    }
  }
  // Expand aliases everywhere, not just at the root: HAVING s + 1 > 5 and
  // ORDER BY f(s) reference the alias inside a larger expression. The depth
  // cap counts alias hops only (expression nesting does not consume it), so
  // a cyclic or over-deep alias chain sets alias_expand_overflow and the
  // caller bails — a partially expanded expression would silently bind
  // leftover aliases as columns.
  bool alias_expand_overflow = false;
  auto expand_aliases = [&](const auto& self, const Expression& e,
                            int alias_depth) -> Expression {
    if (!e) {
      return e;
    }
    if (e->Type() == TypeTag::kColumnValue) {
      const ColumnName& col = e->AsColumnValue().GetColumnName();
      if (col.schema.empty() && !rewriter.group_by_str.contains(col.name) &&
          core_schema.Offset(col) < 0) {
        if (const auto it = select_aliases.find(lower_name(col.name));
            it != select_aliases.end() && it->second != e) {
          if (alias_depth >= 8) {
            alias_expand_overflow = true;
            return e;
          }
          return self(self, it->second, alias_depth + 1);
        }
      }
      return e;
    }
    std::vector<Expression> children = ExpressionChildren(e);
    if (children.empty()) {
      return e;
    }
    bool changed = false;
    for (Expression& child : children) {
      Expression next = self(self, child, alias_depth);
      if (next != child) {
        changed = true;
        child = std::move(next);
      }
    }
    return changed ? WithExpressionChildren(e, std::move(children)) : e;
  };

  std::vector<NamedExpression> proj_items;
  proj_items.reserve(select.SelectList().size() + select.OrderBy().size());
  for (size_t i = 0; i < select.SelectList().size(); ++i) {
    const NamedExpression& item = select.SelectList()[i];
    Expression rewritten = rewriter.Rewrite(item.expression);
    if (!rewriter.valid) {
      return std::optional<PipelineGraph>(std::nullopt);
    }
    const std::string out_name =
        item.name.empty() ? ("__tinylamb_sel_" + std::to_string(i)) : item.name;
    proj_items.emplace_back(out_name, std::move(rewritten));
  }

  Expression having_rewritten;
  if (select.Having()) {
    having_rewritten = rewriter.Rewrite(
        expand_aliases(expand_aliases, sub_alias(select.Having()), 0));
    if (alias_expand_overflow || !rewriter.valid) {
      return std::optional<PipelineGraph>(std::nullopt);
    }
  }

  // Reserve synthetic ORDER BY names against every user-visible projection
  // name and GROUP BY expression text. A collision would make the name-based
  // KeySpec resolve to a grouped value instead of the synthesized sort value,
  // silently sorting by the wrong key (the legacy slot-based route cannot
  // collide).
  std::unordered_set<std::string> reserved_names;
  for (const auto& item : proj_items) {
    reserved_names.insert(item.name);
  }
  for (const auto& name : select.GroupBy()) {
    reserved_names.insert(name->ToString());
  }
  std::vector<OocSortSink::KeySpec> order_keys;
  order_keys.reserve(select.OrderBy().size());
  for (size_t i = 0; i < select.OrderBy().size(); ++i) {
    const auto& term = select.OrderBy()[i];
    Expression ord_rewritten = rewriter.Rewrite(
        expand_aliases(expand_aliases, sub_alias(term.expression), 0));
    if (alias_expand_overflow || !rewriter.valid) {
      return std::optional<PipelineGraph>(std::nullopt);
    }
    // The ORDER BY term may be a bare alias whose COLLATE wrapper only becomes
    // visible after alias expansion, so read the collation from the rewritten
    // expression first (keeping the original as a fallback).
    Collation ord_collation = CollationOfExpression(ord_rewritten);
    if (ord_collation == Collation::kNone) {
      ord_collation = CollationOfExpression(term.expression);
    }
    std::string col_name = "__tinylamb_ord_" + std::to_string(i);
    while (reserved_names.contains(col_name)) {
      col_name.push_back('_');
    }
    reserved_names.insert(col_name);
    proj_items.emplace_back(col_name, std::move(ord_rewritten));
    order_keys.push_back(OocSortSink::KeySpec{
        .expression = MakeSynthCol(col_name),
        .ascending = term.ascending,
        .nulls_first = term.nulls_first,  // NOLINT(clang-diagnostic-error)
        .collation = ord_collation,
    });
  }

  if (rewriter.group_keys.empty() && rewriter.agg_exprs.empty()) {
    return std::optional<PipelineGraph>(std::nullopt);
  }

  // Construct synthetic aggregate schema: [__tinylamb_grp_0..G-1,
  // __tinylamb_agg_0..A-1].
  std::vector<Column> agg_cols;
  agg_cols.reserve(rewriter.group_keys.size() + rewriter.agg_exprs.size());
  for (size_t i = 0; i < rewriter.group_keys.size(); ++i) {
    agg_cols.emplace_back(
        "__tinylamb_grp_" + std::to_string(i),
        InferExprValueType(rewriter.group_keys[i], core_schema));
  }
  for (size_t i = 0; i < rewriter.agg_exprs.size(); ++i) {
    agg_cols.emplace_back(
        "__tinylamb_agg_" + std::to_string(i),
        InferAggValueType(rewriter.agg_exprs[i]->AsAggregateExpression(),
                          core_schema));
  }
  const Schema agg_schema("group_agg", std::move(agg_cols));

  // Build schema for initial ProjectOp ([select items..., __ord_0..]).
  std::vector<Column> proj_cols;
  proj_cols.reserve(proj_items.size());
  for (const auto& item : proj_items) {
    proj_cols.emplace_back(item.name,
                           InferExprValueType(item.expression, agg_schema));
  }
  const Schema proj_schema("group_proj", std::move(proj_cols));

  std::vector<Column> final_cols;
  final_cols.reserve(select.SelectList().size());
  std::vector<NamedExpression> strip_items;
  if (!order_keys.empty()) {
    strip_items.reserve(select.SelectList().size());
    for (size_t i = 0; i < select.SelectList().size(); ++i) {
      final_cols.push_back(proj_schema.GetColumn(i));
      // proj_schema already carries unique __tinylamb_sel_i names (or explicit
      // aliases). The has_dup_sel_names branch below keeps those internal names
      // through the final projection so colliding user aliases remain distinct.
      strip_items.emplace_back(proj_items[i].name,
                               MakeSynthCol(proj_items[i].name));
    }
  } else {
    for (size_t i = 0; i < proj_schema.ColumnCount(); ++i) {
      final_cols.push_back(proj_schema.GetColumn(i));
    }
  }

  // If any two user select names collide (e.g. `SELECT a, a`), give the
  // pre-sort/distinct ProjectOp unambiguous `__tinylamb_sel_i` names (unless we
  // keep them unique all the way through!). Notice Column names on output rows
  // are positional for clients; result_column_names_ already came from AST.
  bool has_dup_sel_names = false;
  if (!order_keys.empty() || select.Distinct()) {
    std::unordered_map<std::string, size_t> seen_names;
    for (size_t i = 0; i < select.SelectList().size(); ++i) {
      if (!seen_names.emplace(proj_items[i].name, i).second) {
        has_dup_sel_names = true;
        break;
      }
    }
  }
  Schema effective_proj_schema = proj_schema;
  if (has_dup_sel_names) {
    std::vector<Column> uniq_cols;
    uniq_cols.reserve(proj_schema.ColumnCount());
    for (size_t i = 0; i < proj_schema.ColumnCount(); ++i) {
      uniq_cols.push_back(proj_schema.GetColumn(i));
    }
    for (size_t i = 0; i < select.SelectList().size(); ++i) {
      const std::string uname = "__tinylamb_sel_" + std::to_string(i);
      proj_items[i].name = uname;
      uniq_cols[i] = Column(uname, uniq_cols[i].Type());
      if (!order_keys.empty()) {
        strip_items[i] =
            NamedExpression(final_cols[i].Name().name, MakeSynthCol(uname));
      }
    }
    effective_proj_schema = Schema("group_proj", std::move(uniq_cols));
  }

  PipelineGraph graph;
  graph.plan_explain_producer = std::move(explain_text_producer);
  LoweredStream stream;
  RETURN_IF_FAIL(
      PipelineLowering::LowerNode(core_plan, ctx, {}, &graph, &stream));

  auto agg_sink = std::make_shared<OocGroupAggSink>(
      core_schema, std::move(rewriter.group_keys),
      std::move(rewriter.agg_exprs), agg_schema, "GroupAgg");
  RETURN_IF_FAIL(PipelineLowering::BreakWithSink(
      &graph, &stream, std::move(agg_sink), "GroupAgg", agg_schema));

  if (having_rewritten) {
    stream.operators.push_back(
        std::make_shared<FilterOp>(having_rewritten, agg_schema));
  }

  // DISTINCT together with ORDER BY falls back to the relational lane at the
  // top of this function, so order_keys is empty whenever Distinct() is set.
  // There is no "visible-columns-only" OocDistinctSink shape here: projecting
  // every produced column is exactly the DISTINCT semantics we need.
  stream.operators.push_back(
      std::make_shared<ProjectOp>(std::move(proj_items), agg_schema));
  if (select.Distinct()) {
    auto all_columns_distinct_sink = std::make_shared<OocDistinctSink>(
        effective_proj_schema, std::vector<slot_t>{}, "Distinct");
    RETURN_IF_FAIL(PipelineLowering::BreakWithSink(
        &graph, &stream, std::move(all_columns_distinct_sink), "Distinct",
        effective_proj_schema));
  }
  stream.schema = effective_proj_schema;

  const std::optional<size_t> opt_limit =
      select.HasLimit() ? std::optional<size_t>(select.Limit()) : std::nullopt;
  if (!order_keys.empty()) {
    const std::string label = opt_limit.has_value() ? "TopN" : "Sort";
    auto sort_sink =
        std::make_shared<OocSortSink>(std::move(order_keys), stream.schema,
                                      opt_limit, select.Offset(), label);
    RETURN_IF_FAIL(PipelineLowering::BreakWithSink(
        &graph, &stream, std::move(sort_sink), label, stream.schema));
    stream.operators.push_back(std::make_shared<ProjectOp>(
        std::move(strip_items), effective_proj_schema));
    stream.schema = Schema("group_out", std::move(final_cols));
  } else if (opt_limit.has_value() || select.Offset() > 0) {
    stream.operators.push_back(std::make_shared<LimitOp>(
        select.Limit(), select.Offset(), select.HasLimit()));
  }

  const size_t root_id = graph.stages.size();
  graph.stages.push_back(PipelineStage{
      .id = root_id,
      .name = "Output",
      .source = std::move(stream.source),
      .operators = std::move(stream.operators),
      .sink = nullptr,
      .output_binding = nullptr,
  });
  graph.output_schema = std::move(stream.schema);

  // EmitRowCount()==0 also means "cardinality unknown" (no statistics,
  // subqueries), not necessarily an empty input. Keep a modest floor so the
  // request does not collapse to the 256 KiB clamp and spill prematurely.
  const size_t core_rows = std::max<size_t>(core_plan->EmitRowCount(), 1024);
  const size_t core_cols = std::max<size_t>(1, core_schema.ColumnCount());
  constexpr size_t kEstimatedBytesPerCell = 24;
  const size_t est_bytes =
      // ~24 bytes per Value slot is an optimistic fixed-size estimate;
      // VARCHAR/PAX cells can be much larger. This guards size_t overflow,
      // not a real memory bound.
      core_rows > std::numeric_limits<size_t>::max() /
                      kEstimatedBytesPerCell / core_cols
          ? std::numeric_limits<size_t>::max()
          : core_rows * core_cols * kEstimatedBytesPerCell;
  graph.estimated_grant_bytes = PipelineGrantRequestBytes(est_bytes);
  // A wide GROUP BY key needs a larger floor than the historical flat 64 KiB;
  // keep it below the request clamp so the coordinator can still shrink it.
  graph.min_grant_bytes = std::clamp<size_t>(core_cols, 64, 256) << 10;
  return std::optional<PipelineGraph>(std::move(graph));
}

}  // namespace tinylamb
