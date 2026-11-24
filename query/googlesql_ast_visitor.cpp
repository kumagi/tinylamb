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
#include "common/platform/time_format.hpp"
#include "common/platform/time_zone.hpp"
#include "common/set_operation.hpp"
#include "common/string_util.hpp"
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
#include "expression/json_doc.hpp"
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

namespace tinylamb {

using namespace ast_visitor_detail;

namespace ast_visitor_detail {

// User-facing parse/rewrite rejection: the AST visitor reports each
// unsupported construct with a diagnostic.
// The AST dump does not carry per-pair set-operator text (only byte ranges
// into the original SQL). Visit() stashes the source here so set operations
// can slice `SetOperationType` / `SetOperationAllOrDistinct` text out.
thread_local const std::string* t_visit_source = nullptr;

class VisitSourceScope {
 public:
  explicit VisitSourceScope(const std::string& source)
      : previous_(t_visit_source) {
    t_visit_source = &source;
  }
  ~VisitSourceScope() { t_visit_source = previous_; }

  VisitSourceScope(const VisitSourceScope&) = delete;
  VisitSourceScope& operator=(const VisitSourceScope&) = delete;
  VisitSourceScope(VisitSourceScope&&) = delete;
  VisitSourceScope& operator=(VisitSourceScope&&) = delete;

 private:
  const std::string* previous_;
};

std::string SliceSource(const GoogleSqlAstNode* node) {
  if (node == nullptr || t_visit_source == nullptr) {
    return {};
  }
  if (node->end < node->start || node->end > t_visit_source->size()) {
    return {};
  }
  return t_visit_source->substr(node->start, node->end - node->start);
}

namespace {

constexpr std::string_view kLateralKeyword = "LATERAL";

void SkipSqlLineComment(std::string_view text, size_t* position) {
  const size_t end = text.find_first_of("\r\n", *position + 1);
  *position = end == std::string_view::npos ? text.size() : end + 1;
}

}  // namespace

void SkipSqlTrivia(std::string_view text, size_t* position) {
  for (;;) {
    while (*position < text.size() &&
           std::isspace(static_cast<unsigned char>(text[*position])) != 0) {
      ++*position;
    }
    if (*position < text.size() &&
        (text[*position] == '#' ||
         (*position + 1 < text.size() && text[*position] == '-' &&
          text[*position + 1] == '-'))) {
      SkipSqlLineComment(text, position);
      continue;
    }
    if (*position + 1 < text.size() && text[*position] == '/' &&
        text[*position + 1] == '*') {
      const size_t end = text.find("*/", *position + 2);
      *position = end == std::string_view::npos ? text.size() : end + 2;
      continue;
    }
    return;
  }
}

namespace {

bool IsLateralAt(std::string_view text, size_t position) {
  if (position + kLateralKeyword.size() > text.size()) {
    return false;
  }
  for (size_t i = 0; i < kLateralKeyword.size(); ++i) {
    if (std::toupper(static_cast<unsigned char>(text[position + i])) !=
        static_cast<unsigned char>(kLateralKeyword[i])) {
      return false;
    }
  }
  const size_t end = position + kLateralKeyword.size();
  return end == text.size() || !IsLateralIdentifierChar(text[end]);
}

bool HasExplicitLateral(const GoogleSqlAstNode& table_subquery,
                        const GoogleSqlAstNode& query) {
  if (t_visit_source == nullptr || table_subquery.start > query.start ||
      table_subquery.end < table_subquery.start || query.end < query.start ||
      table_subquery.end > t_visit_source->size() ||
      query.end > t_visit_source->size()) {
    return false;
  }
  const std::string& source = *t_visit_source;
  const std::string prefix(source, table_subquery.start,
                           query.start - table_subquery.start);
  size_t position = 0;
  SkipSqlTrivia(prefix, &position);
  if (position < prefix.size() && prefix[position] == '(') {
    ++position;
    SkipSqlTrivia(prefix, &position);
  }
  if (!IsLateralAt(prefix, position)) {
    return false;
  }
  position += kLateralKeyword.size();
  SkipSqlTrivia(prefix, &position);
  return position < prefix.size() && prefix[position] == '(';
}

}  // namespace

namespace {

// GoogleSQL resolves an unqualified name in ORDER BY against the SELECT-list
// output names first; a name that more than one output column carries is an
// error ("Column name u is ambiguous"), even inside an expression
// (`SELECT u, u FROM t ORDER BY u + 1`).  The default lowering used to pick
// one of them silently while the relational route rejected the query
// (griffin_fuzz GRIFFIN-RELATIONAL-PATH MISMATCH).  Ordinals are resolved by
// the caller before this check, and `*` expansions carry names unknown here,
// so neither is counted (never a false reject).
std::optional<std::string> AmbiguousOrderByName(
    const std::vector<NamedExpression>& projections,
    const Expression& order_expression) {
  std::unordered_map<std::string, size_t> output_names;
  for (const NamedExpression& item : projections) {
    std::string name = item.name;
    if (name.empty() && item.expression != nullptr &&
        item.expression->Type() == TypeTag::kColumnValue) {
      name = item.expression->AsColumnValue().GetColumnName().name;
    }
    if (name.empty() || name == "*") {
      continue;
    }
    ++output_names[AsciiToLower(name)];
  }
  std::optional<std::string> ambiguous;
  std::function<void(const Expression&)> visit = [&](const Expression& e) {
    if (e == nullptr || ambiguous.has_value()) {
      return;
    }
    switch (e->Type()) {
      case TypeTag::kQueryExp:   // subqueries resolve names in their own scope
      case TypeTag::kLambdaExp:  // lambda parameters shadow output names
        return;
      case TypeTag::kColumnValue: {
        const ColumnName& col = e->AsColumnValue().GetColumnName();
        if (col.schema.empty()) {
          const auto it = output_names.find(AsciiToLower(col.name));
          if (it != output_names.end() && it->second > 1) {
            ambiguous = col.name;
          }
        }
        return;
      }
      default:
        break;
    }
    for (const Expression& child : ExpressionChildren(e)) {
      visit(child);
    }
  };
  visit(order_expression);
  return ambiguous;
}

// A lone star projection is sometimes wrapped by the value-table machinery
// (`SELECT t.*` and `SELECT AS VALUE <star>` fold it into
// `__value_table_value(<ColumnValue "*">)`); star-shape checks must see
// through that wrapper or the qualified star fails the tag-equality gates
// the unqualified star passes (P797).
bool IsUnexpandedStar(const Expression& expression) {
  if (!expression) {
    return false;
  }
  const Expression* star = &expression;
  if ((*star)->Type() == TypeTag::kFunctionCallExp) {
    const auto& call = (*star)->AsFunctionCallExpression();
    if (call.FuncName() != "__value_table_value" || call.Args().size() != 1 ||
        !call.Args().front()) {
      return false;
    }
    star = &call.Args().front();
  }
  return (*star)->Type() == TypeTag::kColumnValue &&
         (*star)->AsColumnValue().GetColumnName().name == "*";
}

// --- SQL UDF support (CREATE [TEMP] [AGGREGATE] FUNCTION) -------------------
//
// The visitor keeps a thread-local, session-scoped registry of user-defined
// SQL functions, mirroring the session-scoped TEMP semantics of the
// single-connection compliance harness (the same pattern as SetSessionConstant).
// Definitions
// store the raw body AST; scalar bodies are compiled lazily on first call and
// served from the expression-layer runtime registry, while aggregate bodies
// are spliced into every call site so the enclosing statement's aggregation
// machinery computes their inner aggregates.

struct UdfParameter {
  std::string name;
  // DEFAULT expression subtree; null when the parameter is required.
  std::unique_ptr<GoogleSqlAstNode> default_value;
};

// T2396: the 64-frame UDF/TVF expansion cap.  This guard used to be dead
// code; it is now constructed on the table-function expansion route
// (VisitTableSource below), which the expression visitor's
// ExpressionDepthGuard does not cover.  The scalar UDF/lambda route is
// bounded by the tighter 32-frame `t_udf_frames` recursion stack in
// ExpandUdfCall (sql_frontend_coverage_test's
// UdfExpansionChainHonorsFrameDepthCap pins it), so a scalar chain
// reports "SQL function recursion limit exceeded" before this guard can
// fire; nested TVF-in-TVF expansion is this guard's job.
constexpr int kMaxUdfExpansionDepth = 64;

thread_local int tls_udf_expansion_depth = 0;

class UdfExpansionDepthGuard {
 public:
  UdfExpansionDepthGuard() {
    if (tls_udf_expansion_depth >= kMaxUdfExpansionDepth) {
      failed_ = true;
      return;
    }
    ++tls_udf_expansion_depth;
  }
  // Symmetric with the constructor: a failed guard never incremented the
  // depth, so it must not decrement either (see ExpressionDepthGuard).
  ~UdfExpansionDepthGuard() {
    if (!failed_) {
      --tls_udf_expansion_depth;
    }
  }
  UdfExpansionDepthGuard(const UdfExpansionDepthGuard&) = delete;
  UdfExpansionDepthGuard& operator=(const UdfExpansionDepthGuard&) = delete;
  UdfExpansionDepthGuard(UdfExpansionDepthGuard&&) = delete;
  UdfExpansionDepthGuard& operator=(UdfExpansionDepthGuard&&) = delete;
  [[nodiscard]] bool failed() const { return failed_; }

 private:
  bool failed_{false};
};

// UDF/TVF expansion can hand these walkers a tree deeper than the parser
// could ever produce, so enforce an explicit recursion bound.
constexpr size_t kMaxSubstitutionDepth = 512;
thread_local size_t tls_substitution_depth = 0;
struct
    SubstitutionDepthGuard {  // NOLINT(cppcoreguidelines-special-member-functions)
  bool entered{false};
  SubstitutionDepthGuard() {
    if (tls_substitution_depth < kMaxSubstitutionDepth) {
      ++tls_substitution_depth;
      entered = true;
    }
  }
  ~SubstitutionDepthGuard() {
    if (entered) {
      --tls_substitution_depth;
    }
  }
};

Expression SubstituteParameters(
    const Expression& expression,
    const std::unordered_map<std::string, Expression>& bindings);

std::shared_ptr<SelectStatement> SubstituteInSelect(
    const SelectStatement& select,
    const std::unordered_map<std::string, Expression>& bindings);

WindowOrderTerm SubstituteInOrderTerm(
    const WindowOrderTerm& term,
    const std::unordered_map<std::string, Expression>& bindings) {
  WindowOrderTerm result;
  result.ascending = term.ascending;
  result.nulls_first = term.nulls_first;
  if (term.expression) {
    result.expression = SubstituteParameters(term.expression, bindings);
  }
  return result;
}

Expression SubstituteParameters(
    const Expression& expression,
    const std::unordered_map<std::string, Expression>&
        bindings) {  // NOLINT(misc-no-recursion) // AST-shaped tree walk; depth
                     // bounded by the parser's expression nesting.
  SubstitutionDepthGuard depth_guard;
  if (!expression || !depth_guard.entered) {
    return expression;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& column = expression->AsColumnValue().GetColumnName();
      if (!column.schema.empty() || column.name == "*") {
        return expression;
      }
      std::string lower_name = column.name;
      for (char& c : lower_name) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      const auto found = bindings.find(lower_name);
      if (found == bindings.end()) {
        return expression;
      }
      return found->second;
    }
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      return BinaryExpressionExp(
          SubstituteParameters(binary.Left(), bindings), binary.Op(),
          SubstituteParameters(binary.Right(), bindings));
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      return UnaryExpressionExp(SubstituteParameters(unary.Child(), bindings),
                                unary.Op());
    }
    case TypeTag::kAggregateExp: {
      const auto& aggregate = expression->AsAggregateExpression();
      auto rebuilt = std::make_shared<AggregateExpression>(
          aggregate.GetType(),
          SubstituteParameters(aggregate.Child(), bindings),
          aggregate.Distinct());
      rebuilt->SetIgnoreNulls(aggregate.IgnoreNulls());
      if (aggregate.Having() != AggregateHavingModifier::kNone) {
        rebuilt->SetHaving(
            aggregate.Having(),
            SubstituteParameters(aggregate.HavingCondition(), bindings));
      }
      if (aggregate.WhereFilter()) {
        rebuilt->SetWhereFilter(
            SubstituteParameters(aggregate.WhereFilter(), bindings));
      }
      if (aggregate.SecondaryArg()) {
        rebuilt->SetSecondaryArg(
            SubstituteParameters(aggregate.SecondaryArg(), bindings));
      }
      if (!aggregate.TrailingArgs().empty()) {
        std::vector<Expression> trailing;
        trailing.reserve(aggregate.TrailingArgs().size());
        for (const Expression& arg : aggregate.TrailingArgs()) {
          trailing.push_back(SubstituteParameters(arg, bindings));
        }
        rebuilt->SetTrailingArgs(std::move(trailing));
      }
      if (!aggregate.InnerOrderBy().empty()) {
        std::vector<WindowOrderTerm> order;
        order.reserve(aggregate.InnerOrderBy().size());
        for (const WindowOrderTerm& term : aggregate.InnerOrderBy()) {
          order.push_back(SubstituteInOrderTerm(term, bindings));
        }
        rebuilt->SetInnerOrderBy(std::move(order));
      }
      rebuilt->SetInnerLimit(aggregate.InnerLimit());
      if (!aggregate.ArrayElementSqlType().empty()) {
        rebuilt->SetArrayElementSqlType(aggregate.ArrayElementSqlType());
      }
      return rebuilt;
    }
    case TypeTag::kCaseExp: {
      const auto& searched = expression->AsCaseExpression();
      std::vector<std::pair<Expression, Expression>> clauses;
      clauses.reserve(searched.when_clauses_.size());
      for (const auto& clause : searched.when_clauses_) {
        clauses.emplace_back(SubstituteParameters(clause.first, bindings),
                             SubstituteParameters(clause.second, bindings));
      }
      return CaseExpressionExp(
          std::move(clauses),
          SubstituteParameters(searched.else_clause_, bindings));
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      std::vector<Expression> list;
      list.reserve(in.list_.size());
      for (const Expression& item : in.list_) {
        list.push_back(SubstituteParameters(item, bindings));
      }
      return InExpressionExp(SubstituteParameters(in.child_, bindings),
                             std::move(list));
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      std::vector<Expression> args;
      args.reserve(call.Args().size());
      for (const Expression& arg : call.Args()) {
        args.push_back(SubstituteParameters(arg, bindings));
      }
      return FunctionCallExp(call.FuncName(), std::move(args));
    }
    case TypeTag::kArrayExp: {
      const auto& array = expression->AsArrayExpression();
      std::vector<Expression> elements;
      elements.reserve(array.Elements().size());
      for (const Expression& element : array.Elements()) {
        elements.push_back(SubstituteParameters(element, bindings));
      }
      return ArrayExpressionExp(std::move(elements), array.ElementSqlType());
    }
    case TypeTag::kCastExp: {
      const auto& cast = expression->AsCastExpression();
      return CastExpressionExp(SubstituteParameters(cast.Child(), bindings),
                               cast.TargetTypeName(), cast.ReturnNullOnError());
    }
    case TypeTag::kQueryExp: {
      const auto& query = expression->AsQueryExpression();
      auto rebuilt = std::make_shared<QueryExpression>(
          SubstituteInSelect(*query.Query(), bindings),
          SubstituteParameters(query.Test(), bindings), query.Exists(),
          query.Negated(), query.Op(), query.Mode());
      rebuilt->SetArrayResult(query.ArrayResult());
      if (!query.ArrayElementSqlType().empty()) {
        rebuilt->SetArrayElementSqlType(query.ArrayElementSqlType());
      }
      return {rebuilt};
    }
    default:
      // Constants and intervals carry no parameter references.
      return expression;
  }
}

std::shared_ptr<SelectStatement> SubstituteInSelect(
    const SelectStatement& select,
    const std::unordered_map<std::string, Expression>&
        bindings) {  // NOLINT(misc-no-recursion) // Mirrors statement-tree
                     // binding in sql_template.cpp.
  SubstitutionDepthGuard depth_guard;
  if (!depth_guard.entered) {
    return nullptr;
  }
  std::vector<NamedExpression> items;
  items.reserve(select.SelectList().size());
  for (const NamedExpression& item : select.SelectList()) {
    items.emplace_back(item.name,
                       SubstituteParameters(item.expression, bindings));
    // `* EXCEPT (...)` / `* REPLACE (...)` live beside the star expression;
    // the 2-argument constructor drops them, which re-exposed excluded
    // columns in TVF expansions.
    if (item.star_modifiers) {
      auto modifiers = std::make_shared<StarModifiers>(*item.star_modifiers);
      for (StarReplaceItem& replace : modifiers->replace_items) {
        replace.expression = SubstituteParameters(replace.expression, bindings);
      }
      items.back().star_modifiers = std::move(modifiers);
    }
  }
  auto result = std::make_shared<SelectStatement>(
      std::move(items), select.FromClause(),
      SubstituteParameters(select.WhereClause(), bindings),
      std::vector<SelectStatement::OrderByTerm>{}, select.Limit(),
      select.Offset(), select.Distinct());
  result->SetLimit(select.HasLimit() ? std::optional<size_t>(select.Limit())
                                     : std::nullopt);
  std::vector<SelectSource> sources;
  sources.reserve(select.Sources().size());
  for (const SelectSource& source : select.Sources()) {
    SelectSource copied = source;
    if (source.query) {
      copied.query = SubstituteInSelect(*source.query, bindings);
    }
    if (source.sample) {
      TableSampleSpec sample = *source.sample;
      sample.size = SubstituteParameters(sample.size, bindings);
      if (sample.repeatable_seed) {
        sample.repeatable_seed =
            SubstituteParameters(*sample.repeatable_seed, bindings);
      }
      for (Expression& partition : sample.partition_by) {
        partition = SubstituteParameters(partition, bindings);
      }
      copied.sample = std::move(sample);
    }
    copied.join_condition =
        SubstituteParameters(source.join_condition, bindings);
    sources.push_back(std::move(copied));
  }
  result->SetSources(std::move(sources));
  for (const auto& [alias, table] : select.Aliases()) {
    result->AddAlias(alias, table);
  }
  if (!select.GroupBy().empty()) {
    std::vector<Expression> group;
    group.reserve(select.GroupBy().size());
    for (const Expression& item : select.GroupBy()) {
      group.push_back(SubstituteParameters(item, bindings));
    }
    result->SetGroupBy(std::move(group));
  }
  // `GROUP BY ()` is a placeholder constant key plus this marker; dropping
  // the marker on re-clone made group-all builds emit 0 rows for empty
  // input through template replay / TVF expansion paths.
  result->SetGroupAll(select.IsGroupAll());
  if (select.Having()) {
    result->SetHaving(SubstituteParameters(select.Having(), bindings));
  }
  if (select.Qualify()) {
    result->SetQualify(SubstituteParameters(select.Qualify(), bindings));
  }
  if (!select.OrderBy().empty()) {
    std::vector<SelectStatement::OrderByTerm> order;
    order.reserve(select.OrderBy().size());
    for (const SelectStatement::OrderByTerm& term : select.OrderBy()) {
      order.push_back({SubstituteParameters(term.expression, bindings),
                       term.ascending, term.nulls_first});
    }
    result->SetOrderBy(std::move(order));
  }
  if (select.HasDistinctOn()) {
    std::vector<Expression> d_on;
    d_on.reserve(select.DistinctOn().size());
    for (const Expression& item : select.DistinctOn()) {
      d_on.push_back(SubstituteParameters(item, bindings));
    }
    result->SetDistinctOn(std::move(d_on));
  }
  result->SetWithTies(select.WithTies());
  for (size_t branch_index = 0; branch_index < select.UnionAll().size();
       ++branch_index) {
    const SetOperationKind kind =
        branch_index < select.SetOperationKinds().size()
            ? select.SetOperationKinds()[branch_index]
            : SetOperationKind::kUnionAll;
    const SetOperationMatch match = branch_index < select.Matches().size()
                                        ? select.Matches()[branch_index]
                                        : SetOperationMatch{};
    result->AddSetOperation(
        kind, SubstituteInSelect(*select.UnionAll()[branch_index], bindings),
        match);
  }
  if (const auto& source_tree = select.GetSetOperationTree();
      source_tree != nullptr) {
    auto tree = std::make_shared<SetOperationTree>();
    tree->kinds = source_tree->kinds;
    tree->grouped = source_tree->grouped;
    bool shares_branches =
        source_tree->first != nullptr && !select.UnionAll().empty() &&
        source_tree->branches.size() == select.UnionAll().size();
    for (size_t i = 0; shares_branches && i < source_tree->branches.size();
         ++i) {
      shares_branches =
          source_tree->branches[i].get() == select.UnionAll()[i].get();
    }
    if (shares_branches) {
      tree->first = SubstituteInSelect(*source_tree->first, bindings);
      tree->branches = result->UnionAll();
    } else {
      if (source_tree->first != nullptr) {
        tree->first = SubstituteInSelect(*source_tree->first, bindings);
      }
      tree->branches.reserve(source_tree->branches.size());
      for (const auto& branch : source_tree->branches) {
        tree->branches.push_back(branch ? SubstituteInSelect(*branch, bindings)
                                        : nullptr);
      }
    }
    result->SetSetOperationTree(std::move(tree));
  }
  if (select.UnionDistinct()) {
    result->MarkUnionDistinct(select.UnionByName());
  }
  for (const auto& [name, query] : select.WithQueries()) {
    auto bound_query = SubstituteInSelect(*query, bindings);
    if (select.IsRecursiveWith(name)) {
      result->AddRecursiveWithQuery(name, std::move(bound_query));
      if (const RecursiveDepthSpec* depth = select.RecursiveDepthOf(name);
          depth != nullptr) {
        result->SetRecursiveDepth(name, *depth);
      }
    } else {
      result->AddWithQuery(name, std::move(bound_query));
    }
  }
  if (select.RequiresRelationalEvaluation()) {
    result->MarkComplex();
  }
  if (select.ForUpdate()) {
    result->SetForUpdate(true);
  }
  result->SetAsStruct(select.AsStruct());
  return result;
}
size_t& ViewExpansionDepth() {
  static thread_local size_t depth = 0;
  return depth;
}

std::unique_ptr<GoogleSqlAstNode> CloneAstNode(
    const GoogleSqlAstNode& node) {  // NOLINT(misc-no-recursion)
  auto clone = std::make_unique<GoogleSqlAstNode>();
  clone->kind = node.kind;
  clone->detail = node.detail;
  clone->start = node.start;
  clone->end = node.end;
  clone->children.reserve(node.children.size());
  for (const auto& child : node.children) {
    clone->children.push_back(CloneAstNode(*child));
  }
  return clone;
}
void AnalyzeUdfBody(const GoogleSqlAstNode& node,  // NOLINT(misc-no-recursion)
                    const std::vector<std::pair<std::string, bool>>& parameters,
                    std::vector<size_t>* counts, bool* simple) {
  static const std::unordered_set<std::string> kAggregateNames = {
      "count",   "sum",       "avg",        "min",         "max",
      "countif", "array_agg", "string_agg", "logical_and", "logical_or"};
  if (node.kind == "Query" || node.kind == "ExpressionSubquery" ||
      node.kind == "TableSubquery" || node.kind == "AnalyticFunctionCall") {
    *simple = false;
    return;
  }
  if (node.kind == "PathExpression" && node.children.size() == 1 &&
      node.children[0]->kind == "Identifier") {
    auto hv45660_0 = Identifier(*node.children[0]);
    if (!hv45660_0.HasValue()) {
      *simple = false;
      return;
    }
    const std::string lower = Lower(hv45660_0.MoveValue());
    for (size_t i = 0; i < parameters.size(); ++i) {
      if (parameters[i].first == lower) {
        ++(*counts)[i];
      }
    }
    return;
  }
  if (node.kind == "FunctionCall" && !node.children.empty() &&
      node.children.front()->kind == "PathExpression") {
    auto fn_or = Path(*node.children.front());
    if (fn_or.HasValue()) {
      const std::string fn = Lower(fn_or.MoveValue());
      if (kAggregateNames.contains(fn)) {
        *simple = false;
      }
    } else {
      // Unresolvable callee name: conservatively treat the body as complex.
      *simple = false;
    }
  }
  for (const auto& child : node.children) {
    AnalyzeUdfBody(*child, parameters, counts, simple);
  }
}
// Qualified hints (multi-part names) are ignored; unqualified hints must be
// recognized by the engine, and none are, so reject every unqualified hint.
Status RejectUnsupportedHints(  // NOLINT(misc-no-recursion)
    const GoogleSqlAstNode& node) {
  if (node.kind == "HintEntry") {
    size_t leading_identifiers = 0;
    for (const auto& child : node.children) {
      if (child->kind != "Identifier") {
        break;
      }
      ++leading_identifiers;
    }
    if (leading_identifiers <= 1) {
      std::string hint_name;
      if (!node.children.empty()) {
        auto hint_id = Identifier(*node.children.front());
        hint_name = hint_id.HasValue() ? hint_id.MoveValue()
                                       : hint_id.GetStatus().GetMessage();
      }
      return AstStatus(std::string("Unsupported hint: ") + hint_name);
    }
  }
  for (const auto& child : node.children) {
    Status st48171 = RejectUnsupportedHints(*child);
    if (st48171 != Status::kSuccess) {
      return st48171;
    }
  }
  return Status::kSuccess;
}

Status ValidateSelectAsProjections(
    const std::string& message_name,
    const std::vector<std::pair<std::string, const GoogleSqlAstNode*>>& named,
    std::vector<NamedExpression>* projections);
StatusOr<TableSampleSpec> VisitTableSample(
    const GoogleSqlAstNode& sample_clause) {
  const GoogleSqlAstNode* method_node = sample_clause.Child("Identifier");
  const GoogleSqlAstNode* size_node = sample_clause.Child("SampleSize");
  if (method_node == nullptr || size_node == nullptr) {
    return AstError<TableSampleSpec>(
        "GoogleSQL AST: incomplete TABLESAMPLE clause");
  }
  ASSIGN_OR_RETURN(std::string, method_name, (Identifier(*method_node)));
  method_name = UpperCopy(method_name);
  TableSampleSpec sample;
  if (method_name == "BERNOULLI") {
    sample.method = TableSampleMethod::kBernoulli;
  } else if (method_name == "RESERVOIR") {
    sample.method = TableSampleMethod::kReservoir;
  } else if (method_name == "SYSTEM") {
    sample.method = TableSampleMethod::kSystem;
  } else {
    return AstError<TableSampleSpec>("unsupported TABLESAMPLE method " +
                                     method_name);
  }

  std::string size_text = UpperCopy(SliceSource(size_node));
  // Comments inside the size expression must not decide the sample unit.
  // Skip over string literals so a quoted '--' or '/*' is not mistaken for a
  // comment opener.
  for (size_t i = 0; i + 1 < size_text.size();) {
    if (size_text[i] == '\'') {
      const size_t end = size_text.find('\'', i + 1);
      // Doubled quotes are consumed as two adjacent literals below.
      if (end == std::string::npos) {
        break;
      }
      i = end + 1;
    } else if (size_text[i] == '/' && size_text[i + 1] == '*') {
      const size_t end = size_text.find("*/", i + 2);
      if (end == std::string::npos) {
        // Unterminated comment: do not erase from here to the end of the
        // string, or PERCENT/ROWS (and any other unit keyword after `/*`)
        // would be swallowed and valid SQL would fail unit detection.
        break;
      }
      size_text.erase(i, end + 2 - i);
    } else if (size_text[i] == '-' && size_text[i + 1] == '-') {
      // GoogleSQL line comment: drop up to (but not including) the newline so
      // a PERCENT/ROWS keyword on the following line still decides the unit.
      const size_t end = size_text.find('\n', i + 2);
      size_text.erase(i, end == std::string::npos ? end : end - i);
    } else {
      ++i;
    }
  }
  if (size_text.find("PERCENT") != std::string::npos) {
    sample.unit = TableSampleUnit::kPercent;
  } else if (size_text.find("ROWS") != std::string::npos) {
    sample.unit = TableSampleUnit::kRows;
  } else {
    return AstError<TableSampleSpec>(
        "TABLESAMPLE unit requires the original SQL source");
  }
  if ((sample.method == TableSampleMethod::kReservoir) !=
      (sample.unit == TableSampleUnit::kRows)) {
    return AstError<TableSampleSpec>(
        "TABLESAMPLE method and unit are incompatible");
  }

  const GoogleSqlAstNode* size_expression = nullptr;
  for (const auto& child : size_node->children) {
    if (child->kind != "PartitionBy" && child->kind != "Location") {
      size_expression = child.get();
      break;
    }
  }
  if (size_expression == nullptr) {
    return AstError<TableSampleSpec>("TABLESAMPLE clause has no size");
  }
  ASSIGN_OR_RETURN(Expression, size, (VisitExpression(*size_expression)));
  sample.size = std::move(size);

  if (const GoogleSqlAstNode* partition = size_node->Child("PartitionBy")) {
    if (sample.method != TableSampleMethod::kReservoir) {
      return AstError<TableSampleSpec>(
          "PARTITION BY is only valid for RESERVOIR TABLESAMPLE");
    }
    size_t partition_keys = 0;
    for (const auto& child : partition->children) {
      partition_keys += child->kind == "ExpressionList" ? child->children.size()
                                                        : 1;
    }
    sample.partition_by.reserve(partition_keys);
    for (const auto& child : partition->children) {
      if (child->kind == "Location") {
        continue;
      }
      if (child->kind == "ExpressionList") {
        for (const auto& item : child->children) {
          if (item->kind == "Location") {
            continue;
          }
          ASSIGN_OR_RETURN(Expression, expression, (VisitExpression(*item)));
          sample.partition_by.push_back(std::move(expression));
        }
        continue;
      }
      ASSIGN_OR_RETURN(Expression, expression, (VisitExpression(*child)));
      sample.partition_by.push_back(std::move(expression));
    }
    if (sample.partition_by.empty()) {
      return AstError<TableSampleSpec>("TABLESAMPLE PARTITION BY is empty");
    }
  }

  const GoogleSqlAstNode* suffix = sample_clause.Child("SampleSuffix");
  if (const GoogleSqlAstNode* with_weight = sample_clause.Child("WithWeight")) {
    sample.with_weight = true;
    if (const GoogleSqlAstNode* alias = with_weight->Child("Alias")) {
      if (const GoogleSqlAstNode* identifier = alias->Child("Identifier")) {
        ASSIGN_OR_RETURN(std::string, weight_alias, (Identifier(*identifier)));
        sample.weight_alias = std::move(weight_alias);
      }
    }
  } else if (suffix != nullptr) {
    if (const GoogleSqlAstNode* with_weight_suffix =
            suffix->Child("WithWeight")) {
      sample.with_weight = true;
      if (const GoogleSqlAstNode* alias = with_weight_suffix->Child("Alias")) {
        if (const GoogleSqlAstNode* identifier = alias->Child("Identifier")) {
          ASSIGN_OR_RETURN(std::string, weight_alias,
                           (Identifier(*identifier)));
          sample.weight_alias = std::move(weight_alias);
        }
      }
    }
  }
  if (sample.weight_alias.empty()) {
    return AstError<TableSampleSpec>("TABLESAMPLE weight alias is empty");
  }

  const GoogleSqlAstNode* repeatable = sample_clause.Child("RepeatableClause");
  if (repeatable == nullptr && suffix != nullptr) {
    repeatable = suffix->Child("RepeatableClause");
  }
  if (repeatable != nullptr) {
    const GoogleSqlAstNode* seed = nullptr;
    for (const auto& child : repeatable->children) {
      if (child->kind != "Location") {
        seed = child.get();
        break;
      }
    }
    if (seed == nullptr) {
      return AstError<TableSampleSpec>("TABLESAMPLE REPEATABLE has no seed");
    }
    ASSIGN_OR_RETURN(Expression, seed_expression, (VisitExpression(*seed)));
    sample.repeatable_seed = std::move(seed_expression);
  }
  return sample;
}

StatusOr<SelectSource> VisitTableSource(
    const GoogleSqlAstNode& node,
    JoinType join_type,  // NOLINT(misc-no-recursion) // Recursive AST descent
                         // for nested joins/subqueries by design (see
                         // VisitQuery depth note).
    Expression join_condition) {
  // FROM sources are resolved outside the alias scope of their own query
  // block, so the innermost bound-name mask is suspended while descending
  // into them.
  struct SuspendInnermostMask {
    bool active = false;
    std::unordered_set<std::string> restored;
    SuspendInnermostMask() {
      active = !t_udf_frames.empty() && !t_udf_bound_masks.empty();
      if (active) {
        restored = std::move(t_udf_bound_masks.back());
        t_udf_bound_masks.pop_back();
      }
    }
    ~SuspendInnermostMask() {
      if (active) {
        t_udf_bound_masks.push_back(std::move(restored));
      }
    }
    SuspendInnermostMask(const SuspendInnermostMask&) = delete;
    SuspendInnermostMask& operator=(const SuspendInnermostMask&) = delete;
    SuspendInnermostMask(SuspendInnermostMask&&) = delete;
    SuspendInnermostMask& operator=(SuspendInnermostMask&&) = delete;
  } suspend_mask;
  SelectSource source;
  source.join_type = join_type;
  source.join_condition = std::move(join_condition);
  ASSIGN_OR_RETURN(std::string, hv165436_0, (Alias(node)));
  source.alias = std::move(hv165436_0);
  // WITH OFFSET applies to both explicit UNNEST operators and implicit
  // unnests written as qualified field paths (`t.arr elem WITH OFFSET off`).
  auto capture_offset_alias = [&]() -> Status {
    const GoogleSqlAstNode* with_offset = node.Child("WithOffset");
    if (with_offset == nullptr) {
      with_offset = node.Child("WithOffsetClause");
    }
    if (with_offset != nullptr) {
      if (const GoogleSqlAstNode* alias = with_offset->Child("Alias")) {
        if (alias->Child("Identifier") != nullptr) {
          ASSIGN_OR_RETURN(std::string, hv165974_0,
                           (Identifier(*alias->Child("Identifier"))));
          source.offset_alias = std::move(hv165974_0);
        } else {
          source.offset_alias = "offset";
        }
      } else {
        source.offset_alias = "offset";
      }
      return Status::kSuccess;
    }
    for (const auto& child : node.children) {
      if (child->kind.find("Offset") != std::string::npos) {
        if (const GoogleSqlAstNode* alias = child->Child("Alias")) {
          if (alias->Child("Identifier") != nullptr) {
            ASSIGN_OR_RETURN(std::string, hv166430_0,
                             (Identifier(*alias->Child("Identifier"))));
            source.offset_alias = std::move(hv166430_0);
          } else {
            source.offset_alias = "offset";
          }
        } else {
          source.offset_alias = "offset";
        }
        break;
      }
    }
    return Status::kSuccess;
  };
  if (node.kind == "TablePathExpression") {
    if (const GoogleSqlAstNode* unnest = node.Child("UnnestExpression")) {
      if (node.Child("SampleClause") != nullptr) {
        return AstError<SelectSource>(
            "TABLESAMPLE is not valid directly on UNNEST");
      }
      size_t unnest_expr_count = 0;
      for (const auto& child : unnest->children) {
        if (child->kind == "ExpressionWithOptAlias") {
          ++unnest_expr_count;
          if (unnest_expr_count > 1) {
            // The engine's UNNEST models a single array (plus optional
            // offset). Silently dropping later arrays changes row counts and
            // column shape, so reject the zip form explicitly.
            return AstError<SelectSource>(
                "UNNEST with multiple arrays is not supported");
          }
          for (const auto& expr_child : child->children) {
            if (expr_child->kind != "Location" && expr_child->kind != "Alias") {
              ASSIGN_OR_RETURN(Expression, hv167048_0,
                               (VisitExpression(*expr_child)));
              source.unnest = std::move(hv167048_0);
              break;
            }
          }
          if (source.alias.empty()) {
            ASSIGN_OR_RETURN(std::string, hv167193_0, (Alias(*child)));
            source.alias = std::move(hv167193_0);
          }
        }
      }
      Status st_off = capture_offset_alias();
      if (st_off != Status::kSuccess) {
        return st_off;
      }
      if (source.alias.empty()) {
        source.alias = "unnest";
      }
      return source;
    }
    const GoogleSqlAstNode* path = node.Child("PathExpression");
    if (path == nullptr) {
      return AstError<SelectSource>("GoogleSQL AST: table without path");
    }
    ASSIGN_OR_RETURN(std::string, hv167586_0, (Path(*path)));
    // GoogleSQL FROM items may be qualified field paths (`t4.array_val`,
    // `t.Info.str_value`): an implicit UNNEST of an array-typed column or
    // nested field reached through a scope alias.  Such paths never name a
    // base relation, so map them to an unnest source whose expression is
    // resolved against the enclosing scope chain at execution time.
    const std::string dotted = std::move(hv167586_0);
    if (dotted.find('.') != std::string::npos && dotted.back() != '.') {
      if (node.Child("SampleClause") != nullptr) {
        return AstError<SelectSource>(
            "TABLESAMPLE is not valid directly on an array path");
      }
      ASSIGN_OR_RETURN(Expression, hv168071_0, (VisitExpression(*path)));
      source.unnest = std::move(hv168071_0);
      if (source.alias.empty()) {
        source.alias = dotted.substr(dotted.rfind('.') + 1);
      }
      Status st_off = capture_offset_alias();
      if (st_off != Status::kSuccess) {
        return st_off;
      }
      return source;
    }
    source.table = dotted;
    if (source.alias.empty()) {
      source.alias = source.table;
    }
    // TEMP views expand as their stored defining query; the reference alias
    // stays the name used at the reference site.
    const auto& views = ViewRegistry();
    const auto found_view = views.find(Lower(dotted));
    if (found_view != views.end()) {
      if (found_view->second.query == nullptr) {
        std::string message = "view definition could not be loaded: " + dotted;
        if (!found_view->second.load_error.empty()) {
          message += " (" + found_view->second.load_error + ")";
        }
        return StatusError(StatusCode::kCorrupt, std::move(message));
      }
      if (ViewExpansionDepth() >= 16) {
        return AstError<SelectSource>("view expansion too deep: " + dotted);
      }
      ++ViewExpansionDepth();
      SelectSource view_source;
      Status st_view = [&]() -> Status {
        const VisitSourceScope defining_source(found_view->second.source);
        ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, h_tmp_4601,
                         VisitQuery(*found_view->second.query));
        view_source.query = std::move(h_tmp_4601);
        view_source.alias = source.alias.empty() ? Lower(dotted) : source.alias;
        return Status::kSuccess;
      }();
      --ViewExpansionDepth();
      if (st_view != Status::kSuccess) {
        return st_view;
      }
      // The join context lives on `source`; a bare `view_source` would drop
      // it and silently degrade `t JOIN v ON ...` (or LEFT/RIGHT/USING) to
      // an unconditional CROSS JOIN.
      view_source.join_type = source.join_type;
      view_source.join_condition = std::move(source.join_condition);
      view_source.offset_alias = std::move(source.offset_alias);
      view_source.using_columns = std::move(source.using_columns);
      view_source.is_lateral = source.is_lateral;
      view_source.from_nested_join = source.from_nested_join;
      if (const GoogleSqlAstNode* sample = node.Child("SampleClause")) {
        ASSIGN_OR_RETURN(TableSampleSpec, parsed, (VisitTableSample(*sample)));
        view_source.sample = std::move(parsed);
      }
      return view_source;
    }
  } else if (node.kind == "TVF") {
    // Table function invocation: expand the stored defining query with the
    // call-site arguments.  Arguments must evaluate as constants at this
    // statement boundary (correlated TVF references are not supported);
    // trailing parameters pad from their DEFAULT expressions exactly like
    // scalar UDF binding.
    const GoogleSqlAstNode* path = node.Child("PathExpression");
    if (path == nullptr) {
      return AstError<SelectSource>(
          "GoogleSQL AST: table function without name");
    }
    ASSIGN_OR_RETURN(std::string, hv244001_0, (Path(*path)));
    const std::string function_name = std::move(hv244001_0);
    // Builtin table-valued functions.  GENERATE_SERIES is a scalar function
    // that yields an ARRAY value, so a FROM-level call is an implicit UNNEST
    // of that array: the relational route row-ifies the value in LoadSource,
    // and the Cascades route lowers the source to kUnnest over the current
    // group.  Named arguments (START => 1, STOP => 3, STEP => 2) reorder into
    // the positional signature; an unaliased call names its value column
    // `value`, matching BigQuery's GENERATE_SERIES output.
    if (Lower(function_name) == "generate_series") {
      if (node.Child("SampleClause") != nullptr) {
        return AstError<SelectSource>(
            "TABLESAMPLE is not valid directly on a table function");
      }
      constexpr size_t kParameterCount = 3;
      static constexpr std::array<std::string_view, kParameterCount>
          kParameters = {"start", "stop", "step"};
      std::vector<Expression> arguments(kParameterCount);
      std::vector<bool> bound(kParameterCount, false);
      size_t next_positional = 0;
      for (const GoogleSqlAstNode* argument : node.Children("TVFArgument")) {
        const GoogleSqlAstNode* expression_node = nullptr;
        for (const auto& child : argument->children) {
          if (child->kind != "Location") {
            expression_node = child.get();
            break;
          }
        }
        if (expression_node == nullptr) {
          return AstError<SelectSource>("table function " + function_name +
                                        " has an empty argument");
        }
        std::string parameter;
        if (expression_node->kind == "NamedArgument") {
          const GoogleSqlAstNode* name_node =
              expression_node->Child("Identifier");
          if (name_node == nullptr) {
            return AstError<SelectSource>(
                "table function " + function_name +
                " has a named argument without a name");
          }
          ASSIGN_OR_RETURN(std::string, hv244006_0, (Identifier(*name_node)));
          parameter = Lower(std::move(hv244006_0));
          const GoogleSqlAstNode* value_node = nullptr;
          for (const auto& child : expression_node->children) {
            if (child->kind == "Location" || child->kind == "Identifier") {
              continue;
            }
            value_node = child.get();
            break;
          }
          if (value_node == nullptr) {
            std::string msg = "table function ";
            msg += function_name;
            msg += " argument ";
            msg += parameter;
            msg += " is empty";
            return AstError<SelectSource>(std::move(msg));
          }
          expression_node = value_node;
        }
        size_t parameter_index = 0;
        if (!parameter.empty()) {
          while (parameter_index < kParameterCount &&
                 kParameters[parameter_index] != parameter) {
            ++parameter_index;
          }
          if (parameter_index == kParameterCount) {
            std::string msg = "table function ";
            msg += function_name;
            msg += " has no argument named ";
            msg += parameter;
            return AstError<SelectSource>(std::move(msg));
          }
        } else {
          parameter_index = next_positional++;
          if (parameter_index >= kParameterCount) {
            return AstError<SelectSource>(
                "too many arguments for table function " + function_name);
          }
        }
        if (bound[parameter_index]) {
          return AstError<SelectSource>(
              "duplicate argument " +
              std::string(kParameters[parameter_index]) +
              " for table function " + function_name);
        }
        ASSIGN_OR_RETURN(Expression, hv244007_0,
                         (VisitExpression(*expression_node)));
        arguments[parameter_index] = std::move(hv244007_0);
        bound[parameter_index] = true;
      }
      if (!bound[0] || !bound[1]) {
        return AstError<SelectSource>("table function " + function_name +
                                      " requires START and STOP arguments");
      }
      std::vector<Expression> call_arguments;
      call_arguments.reserve(bound[2] ? 3 : 2);
      call_arguments.push_back(std::move(arguments[0]));
      call_arguments.push_back(std::move(arguments[1]));
      if (bound[2]) {
        call_arguments.push_back(std::move(arguments[2]));
      }
      source.unnest =
          FunctionCallExp("generate_series", std::move(call_arguments));
      if (source.alias.empty()) {
        source.alias = "value";
      }
      return source;
    }
    const SqlUdf* tvf_found = FindUdf(Lower(function_name));
    if (tvf_found == nullptr) {
      return AstError<SelectSource>("unknown table function " + function_name);
    }
    const SqlUdf& tvf = *tvf_found;
    if (tvf.body == nullptr) {
      return AstError<SelectSource>("table function " + function_name +
                                    " has no body");
    }
    std::unordered_map<std::string, Expression> bindings;
    size_t arg_index = 0;
    for (const GoogleSqlAstNode* argument : node.Children("TVFArgument")) {
      if (arg_index >= tvf.parameters.size()) {
        return AstError<SelectSource>("too many arguments for table function " +
                                      function_name);
      }
      const GoogleSqlAstNode* expression_node = nullptr;
      for (const auto& child : argument->children) {
        if (child->kind != "Location") {
          expression_node = child.get();
          break;
        }
      }
      if (expression_node == nullptr) {
        return AstError<SelectSource>("table function " + function_name +
                                      " argument " + std::to_string(arg_index) +
                                      " is empty");
      }
      ASSIGN_OR_RETURN(Expression, hv244002_0,
                       (VisitExpression(*expression_node)));
      // Volatile calls (RAND(), ...) would silently freeze one draw into
      // every expansion of the body; require genuinely constant arguments.
      const auto volatile_arg = [](const Expression& expression) {
        if (expression->Type() == TypeTag::kFunctionCallExp &&
            GetFunctionVolatility(
                expression->AsFunctionCallExpression().FuncName()) !=
                Volatility::kImmutable) {
          return true;
        }
        for (const Expression& child : ExpressionChildren(expression)) {
          std::vector<Expression> stack{child};
          while (!stack.empty()) {
            Expression current = stack.back();
            stack.pop_back();
            if (current->Type() == TypeTag::kFunctionCallExp &&
                GetFunctionVolatility(
                    current->AsFunctionCallExpression().FuncName()) !=
                    Volatility::kImmutable) {
              return true;
            }
            for (const Expression& next : ExpressionChildren(current)) {
              stack.push_back(next);
            }
          }
        }
        return false;
      };
      if (volatile_arg(hv244002_0)) {
        return AstError<SelectSource>(
            "table function arguments must be constant: " + function_name);
      }
      Row dummy_row;
      Schema dummy_schema;
      StatusOr<Value> constant =
          hv244002_0->TryEvaluate(dummy_row, dummy_schema);
      if (!constant.HasValue()) {
        return AstError<SelectSource>(
            "table function arguments must be constant: " + function_name);
      }
      bindings[tvf.parameters[arg_index].first] =
          ConstantValueExp(constant.MoveValue());
      ++arg_index;
    }
    for (; arg_index < tvf.parameters.size(); ++arg_index) {
      const std::shared_ptr<GoogleSqlAstNode>& default_expr =
          tvf.default_values[arg_index];
      if (default_expr == nullptr) {
        return AstError<SelectSource>("missing argument " +
                                      tvf.parameters[arg_index].first +
                                      " for table function " + function_name);
      }
      const VisitSourceScope defining_source(tvf.table_function_source);
      ASSIGN_OR_RETURN(Expression, hv244003_0,
                       (VisitExpression(*default_expr)));
      Row dummy_row;
      Schema dummy_schema;
      StatusOr<Value> constant =
          hv244003_0->TryEvaluate(dummy_row, dummy_schema);
      if (!constant.HasValue()) {
        return AstError<SelectSource>(
            "table function DEFAULT arguments must be constant: " +
            function_name);
      }
      bindings[tvf.parameters[arg_index].first] =
          ConstantValueExp(constant.MoveValue());
    }
    UdfExpansionDepthGuard tvf_guard;
    if (tvf_guard.failed()) {
      return AstError<SelectSource>("table function expansion too deep: " +
                                    function_name);
    }
    const auto visit_body = [&] {
      const VisitSourceScope defining_source(tvf.table_function_source);
      return VisitQuery(*tvf.body);
    };
    ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv244004_0,
                     visit_body());
    std::shared_ptr<SelectStatement> expanded =
        SubstituteInSelect(*hv244004_0, bindings);
    if (source.alias.empty()) {
      source.alias = Lower(function_name);
    }
    source.query = std::move(expanded);
    if (const GoogleSqlAstNode* sample = node.Child("SampleClause")) {
      ASSIGN_OR_RETURN(TableSampleSpec, hv244005_0,
                       (VisitTableSample(*sample)));
      source.sample = std::move(hv244005_0);
    }
    return source;
  } else if (node.kind == "TableSubquery") {
    const GoogleSqlAstNode* query = node.Child("Query");
    if (query == nullptr) {
      return AstError<SelectSource>(
          "GoogleSQL AST: table subquery missing query");
    }
    ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv169336_0,
                     (VisitQuery(*query)));
    source.query = std::move(hv169336_0);
    if (node.detail.find("lateral") != std::string::npos ||
        node.detail.find("LATERAL") != std::string::npos ||
        HasExplicitLateral(node, *query)) {
      source.is_lateral = true;
    } else if (source.query) {
      std::unordered_set<std::string> local_tables;
      local_tables.reserve(source.query->Sources().size());
      for (const auto& s : source.query->Sources()) {
        if (!s.alias.empty()) {
          local_tables.insert(s.alias);
        }
        if (!s.table.empty()) {
          local_tables.insert(s.table);
        }
      }
      if (source.query->WhereClause()) {
        for (const auto& col : source.query->WhereClause()->TouchedColumns()) {
          if (!col.schema.empty() && !local_tables.contains(col.schema)) {
            source.is_lateral = true;
            break;
          }
        }
      }
    }
  } else {
    return AstError<SelectSource>("GoogleSQL AST: unsupported table source " +
                                  node.kind);
  }
  if (const GoogleSqlAstNode* pivot = node.Child("PivotClause")) {
    ASSIGN_OR_RETURN(SelectSource, hv170954_0,
                     (ExpandPivotSource(std::move(source), *pivot)));
    source = std::move(hv170954_0);
  } else if (const GoogleSqlAstNode* unpivot = node.Child("UnpivotClause")) {
    ASSIGN_OR_RETURN(SelectSource, hv171091_0,
                     (ExpandUnpivotSource(source, *unpivot)));
    source = std::move(hv171091_0);
  }
  if (const GoogleSqlAstNode* sample = node.Child("SampleClause")) {
    ASSIGN_OR_RETURN(TableSampleSpec, parsed, (VisitTableSample(*sample)));
    source.sample = std::move(parsed);
  }
  return source;
}

Status AppendSources(
    const GoogleSqlAstNode& node,
    JoinType incoming,  // NOLINT(misc-no-recursion) // Recursive
                        // AST descent for nested joins by design
                        // (see VisitQuery depth note).
    Expression condition, std::vector<SelectSource>* sources) {
  // Parentheses around a nested JOIN are represented as a wrapper node by
  // the parser, but do not introduce a relation of their own.
  if (node.kind == "ParenthesizedJoin") {
    if (node.Child("SampleClause") != nullptr) {
      return AstStatus("TABLESAMPLE on a parenthesized JOIN is not supported");
    }
    const GoogleSqlAstNode* nested = node.Child("Join");
    if (nested == nullptr) {
      return AstStatus("GoogleSQL AST: empty parenthesized join");
    }
    const size_t nested_begin = sources->size();
    Status st171918 =
        AppendSources(*nested, incoming, std::move(condition), sources);
    if (st171918 != Status::kSuccess) {
      return st171918;
    }
    for (size_t i = nested_begin; i < sources->size(); ++i) {
      (*sources)[i].from_nested_join = true;
    }
    return Status::kSuccess;
  }
  if (node.kind != "Join") {
    ASSIGN_OR_RETURN(SelectSource, hv172145_0,
                     (VisitTableSource(node, incoming, std::move(condition))));
    sources->push_back(std::move(hv172145_0));
    return Status::kSuccess;
  }
  std::vector<const GoogleSqlAstNode*> operands;
  operands.reserve(node.children.size());
  const GoogleSqlAstNode* on = nullptr;
  const GoogleSqlAstNode* using_clause = nullptr;
  for (const auto& child : node.children) {
    if (child->kind == "TablePathExpression" ||
        child->kind == "TableSubquery" || child->kind == "Join" ||
        child->kind == "ParenthesizedJoin" || child->kind == "TVF") {
      operands.push_back(child.get());
    } else if (child->kind == "OnClause") {
      on = child.get();
    } else if (using_clause == nullptr && child->kind.starts_with("Using")) {
      using_clause = child.get();
    }
  }
  if (operands.size() != 2) {
    return AstStatus("GoogleSQL AST: join arity");
  }
  if (node.detail.starts_with("NATURAL")) {
    // A NATURAL join key is the shared column names, which are not known at
    // AST time. Treating it as a condition-less join silently returns the
    // cross product, so reject until the shared columns can be resolved.
    return AstStatus("GoogleSQL AST: unsupported NATURAL join");
  }
  RETURN_IF_FAIL(
      AppendSources(*operands[0], incoming, std::move(condition), sources));
  JoinType type = JoinType::kInner;
  if (node.detail == "COMMA") {
    type = JoinType::kCross;
  } else if (node.detail == "LEFT") {
    type = JoinType::kLeft;
  } else if (node.detail == "RIGHT") {
    type = JoinType::kRight;
  } else if (node.detail == "FULL") {
    type = JoinType::kFull;
  }
  Expression join_expression;
  std::vector<std::string> using_columns;
  if (on != nullptr && !on->children.empty()) {
    ASSIGN_OR_RETURN(Expression, hv173392_0,
                     (VisitExpression(*on->children[0])));
    join_expression = std::move(hv173392_0);
  } else if (using_clause != nullptr) {
    // USING(col, ...) carries no OnClause child; it is an equality join over
    // the shared columns. Dropping it silently would turn the statement into
    // a condition-less join.  The names also ride on the right-hand source so
    // execution can coalesce bare references and star expansion.
    using_columns.reserve(using_clause->Children("Identifier").size());
    for (const GoogleSqlAstNode* column :
         using_clause->Children("Identifier")) {
      ASSIGN_OR_RETURN(std::string, hv173881_0, (Identifier(*column)));
      using_columns.push_back(std::move(hv173881_0));
      ASSIGN_OR_RETURN(std::string, hv173933_0, (Identifier(*column)));
      ASSIGN_OR_RETURN(std::string, hv173933_1, (Identifier(*column)));
      Expression equality = BinaryExpressionExp(
          ColumnValueExp(std::move(hv173933_1)), BinaryOperation::kEquals,
          ColumnValueExp(std::move(hv173933_0)));
      join_expression =
          join_expression
              ? BinaryExpressionExp(std::move(join_expression),
                                    BinaryOperation::kAnd, std::move(equality))
              : std::move(equality);
    }
    if (!join_expression) {
      return AstStatus("GoogleSQL AST: unsupported join USING clause");
    }
  }
  const size_t right_source_index = sources->size();
  Status st174503 =
      AppendSources(*operands[1], type, std::move(join_expression), sources);
  if (st174503 != Status::kSuccess) {
    return st174503;
  }
  if (!using_columns.empty() && right_source_index < sources->size()) {
    (*sources)[right_source_index].using_columns = std::move(using_columns);
  }
  return Status::kSuccess;
}
// Each fold wraps the previous statement as the single derived-table source
// of a new one, so the engine's existing scoping, grouping, ordering and
// limit machinery evaluates the pipe with identical semantics; the wrapper
// shape also keeps the operator order (WHERE after AGGREGATE filters groups,
// not rows) without new rules.
std::shared_ptr<SelectStatement> WrapPipeInput(
    std::shared_ptr<SelectStatement> input, std::string alias) {
  auto wrapper = std::make_shared<SelectStatement>();
  SelectSource source;
  source.query = std::move(input);
  source.alias = std::move(alias);
  source.join_type = JoinType::kCross;
  wrapper->SetSources({std::move(source)});
  wrapper->SetSelectList({NamedExpression{"*", ColumnValueExp("*")}});
  return wrapper;
}

// Visits a pipe SELECT/EXTEND/AGGREGATE list: standard SelectColumn items
// with optional aliases.  Star items keep the "*" name the plain SELECT path
// assigns, so `SELECT *` inside a pipe behaves like the non-pipe form.
StatusOr<std::vector<NamedExpression>> VisitPipeSelectList(
    const GoogleSqlAstNode& select_list) {
  std::vector<NamedExpression> items;
  items.reserve(select_list.Children("SelectColumn").size());
  for (const GoogleSqlAstNode* column : select_list.Children("SelectColumn")) {
    const GoogleSqlAstNode* expression_node = nullptr;
    for (const auto& child : column->children) {
      if (child->kind != "Alias") {
        expression_node = child.get();
        break;
      }
    }
    if (expression_node == nullptr) {
      return AstError<std::vector<NamedExpression>>(
          "GoogleSQL AST: empty pipe select item");
    }
    ASSIGN_OR_RETURN(Expression, hvpipe_0, (VisitExpression(*expression_node)));
    Expression expression = std::move(hvpipe_0);
    if (!expression) {
      return AstError<std::vector<NamedExpression>>(
          "GoogleSQL AST: missing pipe SELECT expression");
    }
    ASSIGN_OR_RETURN(std::string, hvpipe_1, (Alias(*column)));
    std::string name = std::move(hvpipe_1);
    if (name.empty() && expression->Type() == TypeTag::kColumnValue) {
      name = expression->AsColumnValue().GetColumnName().name;
    }
    items.emplace_back(std::move(name), std::move(expression));
  }
  if (items.empty()) {
    return AstError<std::vector<NamedExpression>>(
        "GoogleSQL AST: empty pipe select list");
  }
  return items;
}

// The set-operation executor compares operand widths from the literal select
// lists, so a wrapper whose list is a single star cannot join one.  This
// rebuilds the pipe input's items as explicit references through the
// wrapper's derived alias, reproducing the input columns by name.
StatusOr<std::vector<NamedExpression>> ResolvePipeInputItems(
    const std::shared_ptr<SelectStatement>& input,
    const std::string& wrapper_alias, int depth) {
  if (depth > 64 || !input || input->Sources().size() != 1) {
    return AstError<std::vector<NamedExpression>>(
        "pipe set operation over this input shape is not supported");
  }
  std::vector<NamedExpression> items;
  for (const NamedExpression& item : input->SelectList()) {
    if (IsUnexpandedStar(item.expression)) {
      // A star inherits the wrapped statement's columns through its alias.
      const SelectSource& source = input->Sources().front();
      if (!source.query || source.alias.empty()) {
        return AstError<std::vector<NamedExpression>>(
            "pipe set operation over this input shape is not supported");
      }
      ASSIGN_OR_RETURN(
          std::vector<NamedExpression>, expanded,
          (ResolvePipeInputItems(source.query, source.alias, depth + 1)));
      for (NamedExpression& sub : expanded) {
        items.push_back(std::move(sub));
      }
      continue;
    }
    std::string name = item.name;
    if (name.empty() && item.expression &&
        item.expression->Type() == TypeTag::kColumnValue) {
      name = item.expression->AsColumnValue().GetColumnName().name;
    }
    if (name.empty()) {
      return AstError<std::vector<NamedExpression>>(
          "pipe set operation needs named input columns; alias the pipe "
          "AGGREGATE / SELECT items first");
    }
    items.emplace_back(std::move(name),
                       ColumnValueExp(ColumnName(wrapper_alias, name)));
  }
  if (items.empty()) {
    return AstError<std::vector<NamedExpression>>(
        "pipe set operation over an empty input");
  }
  return items;
}

// Folds the Pipe* children of `query` (siblings of the leading Select) into
// `statement`.  Operators the engine cannot represent yet must fail loudly:
// a silently dropped pipe changes results without any error.

// Deep-copies `node`, rewriting the leading identifier of every path
// expression whose qualifier equals `from` into `to`.  Used to rebind pipe
// JOIN conditions that qualify input columns by the input's earlier name
// (`FROM tv |> JOIN t2 ON tv.k = t2.c`) to the wrapper's derived alias.
std::unique_ptr<GoogleSqlAstNode> CloneRebindingPipeQualifier(
    const GoogleSqlAstNode& node, const std::string& from,
    const std::string& to) {
  auto clone = std::make_unique<GoogleSqlAstNode>();
  clone->kind = node.kind;
  clone->detail = node.detail;
  clone->start = node.start;
  clone->end = node.end;
  bool suppressed = false;
  if (node.kind == "PathExpression" && !node.children.empty() &&
      node.children.front()->kind == "Identifier" &&
      node.children.front()->detail == from) {
    // A path rooted at the old input name resolves under the wrapper alias.
    // Rebinding the root keeps the remaining path elements intact (deep
    // struct field chains only ever hang off a resolved column).
    clone->children.push_back(
        CloneRebindingPipeQualifier(*node.children.front(), from, to));
    clone->children.back()->detail = to;
    for (size_t i = 1; i < node.children.size(); ++i) {
      clone->children.push_back(
          CloneRebindingPipeQualifier(*node.children[i], from, to));
    }
    suppressed = true;
  }
  if (!suppressed) {
    for (const auto& child : node.children) {
      clone->children.push_back(CloneRebindingPipeQualifier(*child, from, to));
    }
  }
  return clone;
}

StatusOr<std::shared_ptr<SelectStatement>> FoldPipeOperators(
    const GoogleSqlAstNode& query, std::shared_ptr<SelectStatement> statement) {
  size_t pipe_index = 0;
  // The name the pipe input is currently visible under (its source alias or
  // base table).  Pipe JOIN conditions may qualify input columns with it;
  // every other fold renames the input to the wrapper's derived alias.
  std::string input_name;
  if (!statement->Sources().empty()) {
    const SelectSource& first = statement->Sources().front();
    input_name = first.alias.empty() ? first.table : first.alias;
  }
  // `|> AS name` renames the pipe input; it rides along until the next
  // operator names its derived source.  A trailing AS names the output
  // relation, which this engine never observes, so it is a no-op there.
  std::string pending_alias;
  auto wrapper_alias = [&]() {
    if (!pending_alias.empty()) {
      std::string alias = std::move(pending_alias);
      pending_alias.clear();
      return alias;
    }
    return "$pipe" + std::to_string(pipe_index);
  };
  auto adopt_wrapper = [&](std::shared_ptr<SelectStatement> wrapper) {
    input_name = wrapper->Sources().front().alias;
    statement = std::move(wrapper);
  };
  for (const auto& child : query.children) {
    if (!child->kind.starts_with("Pipe")) {
      continue;
    }
    const GoogleSqlAstNode& pipe = *child;
    if (pipe.kind == "PipeAs") {
      const GoogleSqlAstNode* identifier = nullptr;
      if (const GoogleSqlAstNode* alias = pipe.Child("Alias")) {
        identifier = alias->Child("Identifier");
      }
      if (identifier == nullptr) {
        identifier = pipe.Child("Identifier");
      }
      if (identifier == nullptr) {
        return AstError<std::shared_ptr<SelectStatement>>(
            "GoogleSQL AST: pipe AS without a name");
      }
      ASSIGN_OR_RETURN(std::string, hvpipe_as, (Identifier(*identifier)));
      pending_alias = std::move(hvpipe_as);
      ++pipe_index;
      continue;
    }
    auto wrapper = WrapPipeInput(statement, wrapper_alias());
    if (pipe.kind == "PipeWhere") {
      const GoogleSqlAstNode* where = pipe.Child("WhereClause");
      if (where == nullptr || where->children.empty()) {
        return AstError<std::shared_ptr<SelectStatement>>(
            "GoogleSQL AST: pipe WHERE without a predicate");
      }
      ASSIGN_OR_RETURN(Expression, hvpipe_where,
                       (VisitExpression(*where->children.front())));
      wrapper->SetWhereClause(std::move(hvpipe_where));
      adopt_wrapper(std::move(wrapper));
    } else if (pipe.kind == "PipeSelect" || pipe.kind == "PipeAggregate") {
      const GoogleSqlAstNode* select_node = pipe.Child("Select");
      if (select_node == nullptr) {
        select_node = &pipe;
      }
      const GoogleSqlAstNode* select_list = select_node->Child("SelectList");
      if (select_list == nullptr) {
        return AstError<std::shared_ptr<SelectStatement>>(
            "GoogleSQL AST: pipe select without a list");
      }
      ASSIGN_OR_RETURN(std::vector<NamedExpression>, hvpipe_items,
                       (VisitPipeSelectList(*select_list)));
      if (UpperCopy(select_node->detail).find("DISTINCT") !=
          std::string::npos) {
        wrapper->SetDistinct(true);
      }
      wrapper->SetSelectList(std::move(hvpipe_items));
      if (pipe.kind == "PipeAggregate") {
        const GoogleSqlAstNode* group = select_node->Child("GroupBy");
        if (group != nullptr) {
          std::vector<Expression> keys;
          keys.reserve(group->Children("GroupingItem").size());
          for (const GoogleSqlAstNode* item : group->Children("GroupingItem")) {
            if (item->Child("GroupingSetList") != nullptr ||
                item->Child("Rollup") != nullptr ||
                item->Child("Cube") != nullptr) {
              return AstError<std::shared_ptr<SelectStatement>>(
                  "GROUPING SETS / ROLLUP / CUBE inside pipe AGGREGATE is "
                  "not supported");
            }
            for (const auto& key_node : item->children) {
              if (key_node->kind == "Location") {
                continue;
              }
              ASSIGN_OR_RETURN(Expression, hvpipe_key,
                               (VisitExpression(*key_node)));
              keys.push_back(std::move(hvpipe_key));
              break;
            }
          }
          if (!keys.empty()) {
            wrapper->SetGroupBy(std::move(keys));
          }
        }
      }
      adopt_wrapper(std::move(wrapper));
    } else if (pipe.kind == "PipeOrderBy") {
      const GoogleSqlAstNode* order = pipe.Child("OrderBy");
      if (order == nullptr) {
        return AstError<std::shared_ptr<SelectStatement>>(
            "GoogleSQL AST: pipe ORDER BY without keys");
      }
      std::vector<SelectStatement::OrderByTerm> terms;
      terms.reserve(order->Children("OrderingExpression").size());
      for (const GoogleSqlAstNode* term :
           order->Children("OrderingExpression")) {
        ASSIGN_OR_RETURN(WindowOrderTerm, hvpipe_term,
                         (ParseOrderingTerm(term)));
        if (hvpipe_term.expression) {
          terms.push_back({std::move(hvpipe_term.expression),
                           hvpipe_term.ascending, hvpipe_term.nulls_first});
        }
      }
      wrapper->SetOrderBy(std::move(terms));
      adopt_wrapper(std::move(wrapper));
    } else if (pipe.kind == "PipeLimitOffset") {
      const GoogleSqlAstNode* limit_offset = pipe.Child("LimitOffset");
      if (limit_offset == nullptr) {
        return AstError<std::shared_ptr<SelectStatement>>(
            "GoogleSQL AST: pipe LIMIT without a clause");
      }
      std::optional<size_t> limit;
      size_t offset = 0;
      if (const GoogleSqlAstNode* limit_node = limit_offset->Child("Limit")) {
        for (const auto& sub : limit_node->children) {
          if (sub->kind == "Location" || sub->kind == "Hint") {
            continue;
          }
          if (sub->kind == "IntLiteral") {
            ASSIGN_OR_RETURN(uint64_t, hvpipe_limit,
                             (ParseUnsignedLiteral(*sub)));
            if (!std::in_range<size_t>(hvpipe_limit)) {
              return AstError<std::shared_ptr<SelectStatement>>(
                  "pipe LIMIT is outside the supported range");
            }
            limit = static_cast<size_t>(hvpipe_limit);
            break;
          }
          auto folded_or = VisitExpression(*sub);
          if (folded_or.HasValue()) {
            Expression folded = folded_or.MoveValue();
            if (folded && folded->Type() == TypeTag::kConstantValue) {
              const Value& constant = folded->AsConstantValue().GetValue();
              if (constant.type == ValueType::kInt64 && !constant.IsNull() &&
                  constant.value.int_value >= 0) {
                if (!std::in_range<size_t>(constant.value.int_value)) {
                  return AstError<std::shared_ptr<SelectStatement>>(
                      "pipe LIMIT is outside the supported range");
                }
                limit = static_cast<size_t>(constant.value.int_value);
                break;
              }
            }
          }
          return AstError<std::shared_ptr<SelectStatement>>(
              "pipe LIMIT requires an integer literal in this engine");
        }
      }
      // The parser emits the OFFSET as a bare IntLiteral sibling of the
      // Limit node ("LimitOffset -> Limit(IntLiteral), IntLiteral(2)").
      // Older dumps wrapped it in an "Offset" node; accept both shapes so
      // a literal OFFSET is never silently ignored. Invalid operands must
      // fail like the non-pipe LIMIT/OFFSET path instead of leaving the
      // offset at 0 and quietly changing the result window.
      const auto parse_pipe_offset = [](const GoogleSqlAstNode& operand)
          -> StatusOr<uint64_t> {
        if (operand.kind == "NullLiteral") {
          return StatusError(StatusCode::kInvalidArgument,
                             "OFFSET must not be NULL");
        }
        if (operand.kind == "UnaryExpression" && operand.detail == "-" &&
            !operand.children.empty() &&
            operand.children.front()->kind == "IntLiteral") {
          return StatusError(StatusCode::kInvalidArgument,
                             "OFFSET must be non-negative");
        }
        if (operand.kind == "IntLiteral") {
          return ParseUnsignedLiteral(operand);
        }
        const auto folded = VisitExpression(operand);
        if (folded.HasValue() && folded.Value() &&
            folded.Value()->Type() == TypeTag::kConstantValue) {
          const Value& constant = folded.Value()->AsConstantValue().GetValue();
          if (constant.IsNull()) {
            return StatusError(StatusCode::kInvalidArgument,
                               "OFFSET must not be NULL");
          }
          if (constant.type == ValueType::kInt64) {
            if (!constant.IsUnsigned() && constant.value.int_value < 0) {
              return StatusError(StatusCode::kInvalidArgument,
                                 "OFFSET must be non-negative");
            }
            return static_cast<uint64_t>(constant.value.int_value);
          }
        }
        return StatusError(StatusCode::kInvalidArgument,
                           "OFFSET requires an integer literal in this engine");
      };
      bool has_offset = false;
      for (const auto& sub : limit_offset->children) {
        if (sub->kind == "Limit" || sub->kind == "Location" ||
            sub->kind == "Hint" || sub->kind == "WithTies") {
          continue;
        }
        if (has_offset) {
          return AstError<std::shared_ptr<SelectStatement>>(
              "pipe OFFSET accepts one operand");
        }
        has_offset = true;
        const GoogleSqlAstNode* operand = sub.get();
        if (sub->kind == "Offset") {
          operand = nullptr;
          for (const auto& inner : sub->children) {
            if (inner->kind == "Location" || inner->kind == "Hint") {
              continue;
            }
            if (operand != nullptr) {
              return AstError<std::shared_ptr<SelectStatement>>(
                  "pipe OFFSET accepts one operand");
            }
            operand = inner.get();
          }
        }
        if (operand == nullptr) {
          return AstError<std::shared_ptr<SelectStatement>>(
              "pipe OFFSET requires an operand");
        }
        ASSIGN_OR_RETURN(uint64_t, parsed_offset, parse_pipe_offset(*operand));
        if (std::cmp_greater(parsed_offset, std::numeric_limits<size_t>::max())) {
          return StatusError(StatusCode::kInvalidArgument,
                             "OFFSET exceeds the supported range");
        }
        offset = static_cast<size_t>(parsed_offset);
      }
      if (limit.has_value()) {
        wrapper->SetLimit(limit);
      }
      wrapper->SetOffset(offset);
      adopt_wrapper(std::move(wrapper));
    } else if (pipe.kind == "PipeJoin") {
      const GoogleSqlAstNode* join = pipe.Child("Join");
      if (join == nullptr) {
        return AstError<std::shared_ptr<SelectStatement>>(
            "GoogleSQL AST: pipe JOIN without a join node");
      }
      if (join->detail.starts_with("NATURAL")) {
        return AstError<std::shared_ptr<SelectStatement>>(
            "GoogleSQL AST: unsupported NATURAL pipe join");
      }
      JoinType type = JoinType::kInner;
      if (join->detail == "COMMA") {
        type = JoinType::kCross;
      } else if (join->detail == "LEFT") {
        type = JoinType::kLeft;
      } else if (join->detail == "RIGHT") {
        type = JoinType::kRight;
      } else if (join->detail == "FULL") {
        type = JoinType::kFull;
      }
      const GoogleSqlAstNode* rhs = nullptr;
      const GoogleSqlAstNode* on = nullptr;
      const GoogleSqlAstNode* using_clause = nullptr;
      for (const auto& sub : join->children) {
        if (sub->kind == "Location" || sub->kind == "PipeJoinLhsPlaceholder") {
          continue;
        }
        if (sub->kind == "OnClause") {
          on = sub.get();
          continue;
        }
        if (using_clause == nullptr && sub->kind.starts_with("Using")) {
          using_clause = sub.get();
          continue;
        }
        if (rhs == nullptr) {
          rhs = sub.get();
        }
      }
      if (rhs == nullptr || rhs->kind != "TablePathExpression") {
        return AstError<std::shared_ptr<SelectStatement>>(
            "pipe JOIN supports plain table references only");
      }
      Expression condition;
      std::vector<std::string> using_columns;
      using_columns.reserve(
          (using_clause != nullptr) ? using_clause->Children("Identifier").size() : 0);
      if (on != nullptr && !on->children.empty()) {
        // Inside the wrapper the pipe input is only visible under its derived
        // alias: rebind ON qualifiers that name the input's earlier identity
        // (`ON t2.c = tv.k` after `FROM tv`) onto that alias before visiting.
        // Unqualified and already-renamed references need no rewrite.
        const std::string input_alias = wrapper->Sources().front().alias;
        auto rebound = CloneRebindingPipeQualifier(*on->children.front(),
                                                   input_name, input_alias);
        ASSIGN_OR_RETURN(Expression, hvpipe_on, (VisitExpression(*rebound)));
        condition = std::move(hvpipe_on);
      } else if (using_clause != nullptr) {
        // Mirror the canonical join path: USING(col, ...) is an equality join
        // over the shared columns, and dropping it silently would turn the
        // pipe join into a condition-less cross product. The names also ride
        // on the right-hand source so execution coalesces bare references and
        // star expansion. The pipe input participates as the wrapper's derived
        // source, so the bare per-side names need no qualifier rebinding.
        for (const GoogleSqlAstNode* column :
             using_clause->Children("Identifier")) {
          ASSIGN_OR_RETURN(std::string, hvpipe_using_name,
                           (Identifier(*column)));
          using_columns.push_back(std::move(hvpipe_using_name));
          ASSIGN_OR_RETURN(std::string, hvpipe_using_left,
                           (Identifier(*column)));
          ASSIGN_OR_RETURN(std::string, hvpipe_using_right,
                           (Identifier(*column)));
          Expression equality = BinaryExpressionExp(
              ColumnValueExp(std::move(hvpipe_using_left)),
              BinaryOperation::kEquals,
              ColumnValueExp(std::move(hvpipe_using_right)));
          condition = condition ? BinaryExpressionExp(std::move(condition),
                                                      BinaryOperation::kAnd,
                                                      std::move(equality))
                                : std::move(equality);
        }
        if (!condition) {
          return AstError<std::shared_ptr<SelectStatement>>(
              "GoogleSQL AST: unsupported pipe join USING clause");
        }
      }
      ASSIGN_OR_RETURN(SelectSource, hvpipe_rhs,
                       (VisitTableSource(*rhs, type, std::move(condition))));
      if (!using_columns.empty()) {
        hvpipe_rhs.using_columns = std::move(using_columns);
      }
      std::vector<SelectSource> wrapper_sources = wrapper->Sources();
      wrapper_sources.push_back(std::move(hvpipe_rhs));
      wrapper->SetSources(std::move(wrapper_sources));
      adopt_wrapper(std::move(wrapper));
    } else if (pipe.kind == "PipeSetOperation") {
      // The parser attaches SetOperationMetadata + the right operand directly
      // under PipeSetOperation (no SetOperation wrapper child).
      std::vector<const GoogleSqlAstNode*> operands;
      operands.reserve(pipe.children.size());
      for (const auto& sub : pipe.children) {
        if (sub->kind == "Query" || sub->kind == "Select" ||
            sub->kind == "SetOperation") {
          operands.push_back(sub.get());
        }
      }
      if (operands.size() != 1) {
        return AstError<std::shared_ptr<SelectStatement>>(
            "pipe set operations combine the pipe input with one operand");
      }
      SetOperationKind kind = SetOperationKind::kUnionAll;
      if (const GoogleSqlAstNode* metadata =
              pipe.Child("SetOperationMetadata")) {
        const std::string type_text =
            metadata->Child("SetOperationType") != nullptr
                ? UpperCopy(SliceSource(metadata->Child("SetOperationType")))
                : "";
        const GoogleSqlAstNode* mode_node =
            metadata->Child("SetOperationAllOrDistinct");
        const std::string mode_text =
            mode_node != nullptr ? UpperCopy(SliceSource(mode_node)) : "";
        if (type_text.find("INTERSECT") != std::string::npos) {
          kind = mode_text.find("ALL") != std::string::npos
                     ? SetOperationKind::kIntersectAll
                     : SetOperationKind::kIntersect;
        } else if (type_text.find("EXCEPT") != std::string::npos) {
          kind = mode_text.find("ALL") != std::string::npos
                     ? SetOperationKind::kExceptAll
                     : SetOperationKind::kExcept;
        } else if (mode_text.find("DISTINCT") != std::string::npos ||
                   mode_text.find("ALL") == std::string::npos) {
          // A bare UNION (no ALL) is UNION DISTINCT.
          kind = SetOperationKind::kUnion;
        }
      }
      ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hvpipe_branch,
                       (VisitQuery(*operands[0])));
      // The set-op width check reads literal select lists, so the wrapper
      // must project the input columns explicitly instead of a star.
      ASSIGN_OR_RETURN(std::vector<NamedExpression>, hvpipe_items,
                       (ResolvePipeInputItems(
                           statement, wrapper->Sources().front().alias, 0)));
      wrapper->SetSelectList(std::move(hvpipe_items));
      wrapper->AddSetOperation(kind, std::move(hvpipe_branch));
      statement = std::move(wrapper);
    } else if (pipe.kind == "PipeDrop" || pipe.kind == "PipeRename" ||
               pipe.kind == "PipeSet" || pipe.kind == "PipeExtend" ||
               pipe.kind == "PipeCall" || pipe.kind == "PipePivot" ||
               pipe.kind == "PipeTablesample") {
      // EXTEND/DROP/RENAME need the previous statement's output column names
      // (or a payload the pinned parser does not attach to the node), and
      // CALL/PIVOT/TABLESAMPLE need machinery the pipe input cannot carry.
      // Reject loudly: dropping the operator would change results silently.
      return AstError<std::shared_ptr<SelectStatement>>(
          "pipe operator " + pipe.kind.substr(4) +
          " is parsed but not supported yet");
    } else {
      return AstError<std::shared_ptr<SelectStatement>>(
          "GoogleSQL AST: unsupported pipe operator " + pipe.kind);
    }
    ++pipe_index;
  }
  return statement;
}

}  // namespace

std::unordered_map<std::string, Expression>& SessionConstantExpressions() {
  // Keyed by the same session scope as UDFs/views so a session clear cannot
  // destroy another connection's definitions.
  static thread_local std::unordered_map<
      uint64_t, std::unordered_map<std::string, Expression>>
      registries;
  return registries[UdfSessionScope()];
}

namespace {

// Per-thread UDF definitions, bucketed by lowered name and tagged with the
// connection that created them: two sessions may define the same name
// independently, and a lookup only sees the current scope's bucket entry.
struct UdfRegistryState {
  std::unordered_map<std::string, std::vector<SqlUdf>> definitions;
  uint64_t session_id{0};
};

UdfRegistryState& UdfRegistryStateForThread() {
  static thread_local UdfRegistryState state;
  return state;
}

}  // namespace

void SetUdfSessionScope(uint64_t session_id) {
  UdfRegistryStateForThread().session_id = session_id;
  // Keep the legacy string-constant ledger in lockstep with the typed,
  // scope-keyed registries: a name defined by one connection must not resolve
  // in another that shares the event-loop thread.
  SetSessionConstantScope(session_id);
}

uint64_t UdfSessionScope() { return UdfRegistryStateForThread().session_id; }

void RegisterUdf(SqlUdf udf) {
  UdfRegistryState& state = UdfRegistryStateForThread();
  udf.owner_session = state.session_id;
  std::vector<SqlUdf>& bucket = state.definitions[udf.name];
  for (SqlUdf& existing : bucket) {
    if (existing.owner_session == state.session_id) {
      existing = std::move(udf);
      return;
    }
  }
  bucket.push_back(std::move(udf));
}

SqlUdf* FindUdf(const std::string& name) {
  UdfRegistryState& state = UdfRegistryStateForThread();
  const auto found = state.definitions.find(name);
  if (found == state.definitions.end()) {
    return nullptr;
  }
  for (SqlUdf& udf : found->second) {
    if (udf.owner_session == state.session_id) {
      return &udf;
    }
  }
  return nullptr;
}

// Issue 2553: SQL UDFs/TVFs live only in the thread_local, session-bucketed
// registry above.  A persistent view definition that captured one of them
// re-expands for every session from the cataloged AST, so any other session
// -- or a read worker that offloaded the query -- resolves the callee to
// "unknown function" because the definition is cataloged but its dependency
// is not.  CREATE VIEW must refuse the capture: this walker returns the
// lowered name of the first scalar-function or TVF reference that resolves
// to a session-local UDF in the current scope.  Names that VisitFunction
// rewrites to builtins before consulting the registry (ucase/lcase/median,
// the lambda array rewrites, the builtin TVF generate_series) can never
// reach a UDF at read time, so shadowing those is not a capture.
std::optional<std::string> FirstSessionLocalUdfReference(
    const GoogleSqlAstNode& node) {  // NOLINT(misc-no-recursion)
  static const std::unordered_set<std::string> kRewrittenBeforeRegistry = {
      "ucase",        "lcase",          "median",         "array_transform",
      "array_filter", "array_includes", "generate_series"};
  if (node.kind == "FunctionCall" || node.kind == "TVF") {
    const GoogleSqlAstNode* path =
        node.kind == "TVF"
            ? node.Child("PathExpression")
            : (!node.children.empty() &&
                       node.children.front()->kind == "PathExpression"
                   ? node.children.front().get()
                   : nullptr);
    if (path != nullptr) {
      StatusOr<std::string> name = Path(*path);
      if (name.HasValue()) {
        std::string lowered = Lower(name.MoveValue());
        if (!kRewrittenBeforeRegistry.contains(lowered) &&
            FindUdf(lowered) != nullptr) {
          return lowered;
        }
      }
    }
  }
  for (const auto& child : node.children) {
    if (auto hit = FirstSessionLocalUdfReference(*child); hit.has_value()) {
      return hit;
    }
  }
  return std::nullopt;
}

void PurgeUdfSessionDefinitions(uint64_t session_id) {
  auto& definitions = UdfRegistryStateForThread().definitions;
  for (auto it = definitions.begin(); it != definitions.end();) {
    std::vector<SqlUdf>& bucket = it->second;
    std::erase_if(bucket, [session_id](const SqlUdf& udf) {
      return udf.owner_session == session_id;
    });
    if (bucket.empty()) {
      it = definitions.erase(it);
    } else {
      ++it;
    }
  }
}

// Reads the `WITH DEPTH ...` modifier carried by an AliasedQuery and attaches
// the resulting RecursiveDepthSpec to a recursive CTE. Shared by the plain
// WITH clause, the set-operation wrapper and the nested-WITH wrapper: all
// three build a recursive CTE but only the first used to carry the spec,
// silently dropping the depth bounds and DEPTH column on those root shapes.
// NOLINTNEXTLINE(misc-use-anonymous-namespace): shared only within this TU.
static Status ApplyRecursiveDepthFromAliased(SelectStatement* statement,
                                             const GoogleSqlAstNode& aliased,
                                             const std::string& cte_name) {
  const GoogleSqlAstNode* modifiers = aliased.Child("AliasedQueryModifiers");
  if (modifiers == nullptr) {
    return Status::kSuccess;
  }
  const GoogleSqlAstNode* depth_modifier =
      modifiers->Child("RecursionDepthModifier");
  if (depth_modifier == nullptr) {
    return Status::kSuccess;
  }
  RecursiveDepthSpec spec;
  if (const GoogleSqlAstNode* alias = depth_modifier->Child("Alias")) {
    if (const GoogleSqlAstNode* column = alias->Child("Identifier")) {
      ASSIGN_OR_RETURN(std::string, depth_column, (Identifier(*column)));
      spec.column = std::move(depth_column);
    }
  }
  const auto bounds = depth_modifier->Children("IntOrUnbounded");
  bool lower_is_literal = false;
  bool upper_is_literal = false;
  auto bound_value = [&](size_t index, int64_t fallback,
                         bool* is_literal) -> StatusOr<int64_t> {
    *is_literal = false;
    if (index >= bounds.size()) {
      return fallback;
    }
    for (const auto& child : bounds[index]->children) {
      if (child->kind == "IntLiteral") {
        ASSIGN_OR_RETURN(uint64_t, parsed_bound, ParseUnsignedLiteral(*child));
        if (!std::in_range<int64_t>(parsed_bound)) {
          return StatusError(StatusCode::kInvalidArgument,
                             "WITH DEPTH bound is outside the supported range");
        }
        *is_literal = true;
        return static_cast<int64_t>(parsed_bound);
      }
    }
    return fallback;
  };
  ASSIGN_OR_RETURN(int64_t, lower, bound_value(0, 0, &lower_is_literal));
  ASSIGN_OR_RETURN(
      int64_t, upper,
      bound_value(1, std::numeric_limits<int64_t>::max(), &upper_is_literal));
  spec.lower = lower;
  spec.upper = upper;
  spec.lower_is_literal = lower_is_literal;
  spec.upper_is_literal = upper_is_literal;
  statement->SetRecursiveDepth(cte_name, std::move(spec));
  return Status::kSuccess;
}

StatusOr<std::shared_ptr<SelectStatement>> VisitQuery(
    const GoogleSqlAstNode& query) {  // NOLINT(misc-no-recursion) // Recursive
                                      // subquery/CTE traversal by design; SQL
                                      // nesting is finite and parser-bounded.
  const GoogleSqlAstNode* select =
      (query.kind == "Select") ? &query : query.Child("Select");
  if (select == nullptr) {
    const GoogleSqlAstNode* set_op =
        (query.kind == "SetOperation") ? &query : query.Child("SetOperation");
    if (set_op == nullptr) {
      for (const auto& child : query.children) {
        if (child->kind == "SetOperation") {
          set_op = child.get();
          break;
        }
      }
    }
    if (set_op != nullptr) {
      std::vector<const GoogleSqlAstNode*> operands;
      operands.reserve(set_op->children.size());
      for (const auto& child : set_op->children) {
        if (child->kind == "Query" || child->kind == "Select" ||
            child->kind == "SetOperation") {
          operands.push_back(child.get());
        }
      }
      if (!operands.empty()) {
        ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv175800_0,
                         (VisitQuery(*operands[0])));
        auto first_stmt = std::move(hv175800_0);
        bool union_by_name = false;
        std::vector<SetOperationKind> per_pair;
        std::vector<SetOperationMatch> per_pair_match;
        per_pair.reserve(operands.size());
        per_pair_match.reserve(operands.size());
        SetOperationKind head_kind = SetOperationKind::kUnion;
        const std::string set_detail = UpperCopy(set_op->detail);
        if (set_detail.find("INTERSECT") != std::string::npos) {
          head_kind = set_detail.find("ALL") != std::string::npos
                          ? SetOperationKind::kIntersectAll
                          : SetOperationKind::kIntersect;
        } else if (set_detail.find("EXCEPT") != std::string::npos) {
          head_kind = set_detail.find("ALL") != std::string::npos
                          ? SetOperationKind::kExceptAll
                          : SetOperationKind::kExcept;
        } else if (set_detail.find("ALL") != std::string::npos) {
          head_kind = SetOperationKind::kUnionAll;
        }
        if (const GoogleSqlAstNode* metadata_list =
                set_op->Child("SetOperationMetadataList")) {
          for (const GoogleSqlAstNode* metadata :
               metadata_list->Children("SetOperationMetadata")) {
            const GoogleSqlAstNode* type_node =
                metadata->Child("SetOperationType");
            const GoogleSqlAstNode* mode_node =
                metadata->Child("SetOperationAllOrDistinct");
            const std::string type_text = UpperCopy(SliceSource(type_node));
            const std::string mode_text = UpperCopy(SliceSource(mode_node));
            if (type_text.empty() && mode_text.empty()) {
              continue;
            }
            SetOperationKind kind = SetOperationKind::kUnion;
            if (type_text.find("INTERSECT") != std::string::npos) {
              kind = mode_text.find("ALL") != std::string::npos
                         ? SetOperationKind::kIntersectAll
                         : SetOperationKind::kIntersect;
            } else if (type_text.find("EXCEPT") != std::string::npos) {
              kind = mode_text.find("ALL") != std::string::npos
                         ? SetOperationKind::kExceptAll
                         : SetOperationKind::kExcept;
            } else {
              kind = mode_text.find("ALL") != std::string::npos
                         ? SetOperationKind::kUnionAll
                         : SetOperationKind::kUnion;
            }
            per_pair.push_back(kind);
            SetOperationMatch match;
            if (const GoogleSqlAstNode* match_node =
                    metadata->Child("SetOperationColumnMatchMode")) {
              // `BY NAME` and `STRICT CORRESPONDING` require the same column
              // names on both sides; plain CORRESPONDING keeps the columns
              // both inputs produce (GoogleSQL's INNER default).  An ON/BY
              // list restricts either mode to the listed columns.
              const std::string match_text = UpperCopy(SliceSource(match_node));
              if (match_text.find("CORRESPONDING") != std::string::npos &&
                  match_text.find("STRICT") == std::string::npos &&
                  match_text.find("BY NAME") == std::string::npos) {
                match.corresponding = true;
              } else {
                match.by_name = true;
              }
              // `BY NAME ON (a, b)` / `CORRESPONDING BY (a, b)` hang the
              // column list off the metadata item, next to the mode node.
              const GoogleSqlAstNode* list_node =
                  metadata->Child("ExpressionList");
              if (list_node != nullptr) {
                for (const auto& child : list_node->children) {
                  if (child->kind != "Identifier") {
                    continue;
                  }
                  ASSIGN_OR_RETURN(std::string, hv243501_0,
                                   (Identifier(*child)));
                  match.columns.push_back(std::move(hv243501_0));
                }
              }
              union_by_name = union_by_name || match.by_name ||
                              match.corresponding || !match.columns.empty();
            }
            per_pair_match.push_back(std::move(match));
          }
        }
        while (per_pair.size() + 1 < operands.size()) {
          per_pair.push_back(head_kind);
        }
        if (set_detail.find("DISTINCT") != std::string::npos) {
          first_stmt->MarkUnionDistinct(union_by_name);
        }
        const auto first_operand =
            std::make_shared<SelectStatement>(*first_stmt);
        std::vector<std::shared_ptr<SelectStatement>> branches;
        branches.reserve(operands.size() - 1);
        for (size_t i = 1; i < operands.size(); ++i) {
          const SetOperationKind kind =
              i - 1 < per_pair.size() ? per_pair[i - 1] : head_kind;
          ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv178940_0,
                           (VisitQuery(*operands[i])));
          auto branch = std::move(hv178940_0);
          const SetOperationMatch match = i - 1 < per_pair_match.size()
                                              ? per_pair_match[i - 1]
                                              : SetOperationMatch{};
          branches.push_back(branch);
          first_stmt->AddSetOperation(kind, std::move(branch), match);
        }
        // Keep explicit parenthesized groups available to the relational
        // executor.  The legacy vectors above intentionally remain populated
        // because the optimizer and SQL-template binder still consume them,
        // but flattening a grouped operand changes the meaning of e.g.
        // `(A UNION ALL B) INTERSECT C`.
        const auto needs_scope = [](const SelectStatement& operand) {
          return operand.GetSetOperationTree() != nullptr ||
                 operand.HasLimit() || operand.Offset() != 0 ||
                 !operand.OrderBy().empty() || operand.WithTies();
        };
        bool has_grouped_operand = needs_scope(*first_operand);
        for (const auto& branch : branches) {
          if (branch != nullptr && needs_scope(*branch)) {
            has_grouped_operand = true;
          }
        }
        // The head's modifiers belong to the saved first operand. Only
        // modifiers attached to this set-operation wrapper constrain the
        // combined result.
        first_stmt->SetLimit(std::nullopt);
        first_stmt->SetOffset(0);
        first_stmt->SetOrderBy({});
        first_stmt->SetWithTies(false);
        auto tree = std::make_shared<SetOperationTree>();
        tree->first = first_operand;
        tree->branches = std::move(branches);
        tree->kinds = per_pair;
        tree->grouped = has_grouped_operand;
        first_stmt->SetSetOperationTree(std::move(tree));
        // A set operation is represented by the first operand in the
        // statement tree, but query-level LIMIT/OFFSET and WITH clauses hang
        // off the wrapper.  Preserve them here so execution sees the same
        // scope and applies bounds after concatenating the branches.
        if (const GoogleSqlAstNode* limit_offset = query.Child("LimitOffset")) {
          // `... WITH TIES` hangs off the same wrapper; the plain-SELECT
          // path reads it, so the set-op path must too or the modifier is
          // silently dropped (the frontend pre-rewrite already erased the
          // "with ties" text from the SQL).
          if (limit_offset->Child("WithTies") != nullptr ||
              limit_offset->detail.find("with_ties") != std::string::npos ||
              limit_offset->detail.find("WITH TIES") != std::string::npos) {
            first_stmt->SetWithTies(true);
          }
          if (const GoogleSqlAstNode* limit_node =
                  limit_offset->Child("Limit")) {
            for (const auto& child : limit_node->children) {
              if (child->kind == "Location" || child->kind == "Hint") {
                continue;
              }
              if (child->kind == "IntLiteral") {
                ASSIGN_OR_RETURN(uint64_t, hv180703_0,
                                 (ParseUnsignedLiteral(*child)));
                first_stmt->SetLimit(static_cast<size_t>(hv180703_0));
                break;
              }
              auto folded_or = VisitExpression(*child);
              if (folded_or.HasValue()) {
                Expression folded = folded_or.MoveValue();
                if (folded && folded->TouchedColumns().empty() &&
                    SafeToReduceEvaluationCount(folded)) {
                  ASSIGN_OR_RETURN(Value, constant,
                                   folded->TryEvaluate(Row(), Schema()));
                  if (constant.type == ValueType::kInt64 &&
                      !constant.IsNull() && constant.value.int_value >= 0) {
                    first_stmt->SetLimit(
                        static_cast<size_t>(constant.value.int_value));
                    break;
                  }
                }
              }
              return AstError<std::shared_ptr<SelectStatement>>(
                  "LIMIT requires an integer literal in this engine");
            }
          }
          for (const auto& child : limit_offset->children) {
            if (child->kind == "Limit" || child->kind == "Location" ||
                child->kind == "Hint" || child->kind == "WithTies" ||
                child->kind == "Offset") {
              if (child->kind == "Offset") {
                for (const auto& sub : child->children) {
                  if (sub->kind == "IntLiteral") {
                    ASSIGN_OR_RETURN(uint64_t, hv182030_0,
                                     (ParseUnsignedLiteral(*sub)));
                    first_stmt->SetOffset(hv182030_0);
                  } else if (!IsAstTrivia(sub->kind)) {
                    ASSIGN_OR_RETURN(Expression, folded, VisitExpression(*sub));
                    if (!folded || !folded->TouchedColumns().empty() ||
                        !SafeToReduceEvaluationCount(folded)) {
                      return AstError<std::shared_ptr<SelectStatement>>(
                          "OFFSET requires a constant integer");
                    }
                    ASSIGN_OR_RETURN(Value, constant,
                                     folded->TryEvaluate(Row(), Schema()));
                    if (constant.type != ValueType::kInt64 ||
                        constant.IsNull() || constant.value.int_value < 0) {
                      return AstError<std::shared_ptr<SelectStatement>>(
                          "OFFSET requires a non-negative integer");
                    }
                    first_stmt->SetOffset(
                        static_cast<size_t>(constant.value.int_value));
                  }
                }
              }
              continue;
            }
            if (child->kind == "IntLiteral") {
              ASSIGN_OR_RETURN(uint64_t, hv182240_0,
                               (ParseUnsignedLiteral(*child)));
              first_stmt->SetOffset(hv182240_0);
            } else {
              if (auto folded_or = VisitExpression(*child);
                  folded_or.HasValue()) {
                Expression folded = folded_or.MoveValue();
                if (folded && folded->TouchedColumns().empty() &&
                    SafeToReduceEvaluationCount(folded)) {
                  ASSIGN_OR_RETURN(Value, constant,
                                   folded->TryEvaluate(Row(), Schema()));
                  if (constant.type == ValueType::kInt64 &&
                      !constant.IsNull() && constant.value.int_value >= 0) {
                    first_stmt->SetOffset(
                        static_cast<size_t>(constant.value.int_value));
                    continue;
                  }
                }
              }
              return AstError<std::shared_ptr<SelectStatement>>(
                  "OFFSET requires an integer literal in this engine");
            }
          }
        }
        if (const GoogleSqlAstNode* order = query.Child("OrderBy")) {
          std::vector<SelectStatement::OrderByTerm> order_by;
          order_by.reserve(order->Children("OrderingExpression").size());
          for (const GoogleSqlAstNode* term :
               order->Children("OrderingExpression")) {
            ASSIGN_OR_RETURN(WindowOrderTerm, parsed,
                             (ParseOrderingTerm(term)));
            if (parsed.expression) {
              order_by.push_back({std::move(parsed.expression),
                                  parsed.ascending, parsed.nulls_first});
            }
          }
          first_stmt->SetOrderBy(std::move(order_by));
        }
        if (const GoogleSqlAstNode* with = query.Child("WithClause")) {
          const bool recursive =
              Lower(with->detail).find("recursive") != std::string::npos;
          for (const GoogleSqlAstNode* entry :
               with->Children("WithClauseEntry")) {
            const GoogleSqlAstNode* aliased = entry->Child("AliasedQuery");
            if (aliased == nullptr) {
              continue;
            }
            const GoogleSqlAstNode* name = aliased->Child("Identifier");
            const GoogleSqlAstNode* nested = aliased->Child("Query");
            if (name == nullptr || nested == nullptr) {
              continue;
            }
            ASSIGN_OR_RETURN(std::string, hv184351_0, (Identifier(*name)));
            const std::string cte_name = std::move(hv184351_0);
            if (recursive) {
              ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv184440_0,
                               (VisitQuery(*nested)));
              first_stmt->AddRecursiveWithQuery(cte_name,
                                                std::move(hv184440_0));
              RETURN_IF_FAIL(ApplyRecursiveDepthFromAliased(
                  first_stmt.get(), *aliased, cte_name));
            } else {
              ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, h_q,
                               (VisitQuery(*nested)));
              first_stmt->AddWithQuery(cte_name, std::move(h_q));
            }
          }
        }
        if (query.kind != "Select") {
          return FoldPipeOperators(query, std::move(first_stmt));
        }
        return first_stmt;
      }
    }
    // A parenthesized query can carry its own WITH clause while the outer
    // query wrapper carries another one.  The parser represents that shape
    // as Query -> WithClause, Query (without a Select child directly on the
    // outer node).  Visit the nested query and attach the outer CTE scope.
    for (const auto& child : query.children) {
      if (child->kind != "Query") {
        continue;
      }
      ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv185102_0,
                       (VisitQuery(*child)));
      auto nested = std::move(hv185102_0);
      if (const GoogleSqlAstNode* with = query.Child("WithClause")) {
        const bool recursive =
            Lower(with->detail).find("recursive") != std::string::npos;
        for (const GoogleSqlAstNode* entry :
             with->Children("WithClauseEntry")) {
          const GoogleSqlAstNode* aliased = entry->Child("AliasedQuery");
          if (aliased == nullptr) {
            continue;
          }
          const GoogleSqlAstNode* name = aliased->Child("Identifier");
          const GoogleSqlAstNode* body = aliased->Child("Query");
          if (name == nullptr || body == nullptr) {
            continue;
          }
          ASSIGN_OR_RETURN(std::string, hv185777_0, (Identifier(*name)));
          const std::string cte_name = std::move(hv185777_0);
          if (recursive) {
            ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv185862_0,
                             (VisitQuery(*body)));
            nested->AddRecursiveWithQuery(cte_name, std::move(hv185862_0));
            RETURN_IF_FAIL(ApplyRecursiveDepthFromAliased(nested.get(),
                                                          *aliased, cte_name));
          } else {
            ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, h_q,
                             (VisitQuery(*body)));
            nested->AddWithQuery(cte_name, std::move(h_q));
          }
        }
      }
      return nested;
    }
    // `FROM t |> ...`: a pipe query may start from a bare FROM clause.  The
    // base is the star projection over those sources; the pipe operators fold
    // on top of it below.
    if (const GoogleSqlAstNode* from_query = query.Child("FromQuery")) {
      const GoogleSqlAstNode* from_clause = from_query->Child("FromClause");
      if (from_clause != nullptr) {
        std::vector<SelectSource> sources;
        bool sources_ok = true;
        for (const auto& child : from_clause->children) {
          if (child->kind == "Location" || child->kind == "Hint") {
            continue;
          }
          Status st_pipe_from =
              AppendSources(*child, JoinType::kCross, Expression{}, &sources);
          if (st_pipe_from != Status::kSuccess) {
            sources_ok = false;
            break;
          }
        }
        if (sources_ok && !sources.empty()) {
          auto base = std::make_shared<SelectStatement>();
          base->SetSources(std::move(sources));
          base->SetSelectList({NamedExpression{"*", ColumnValueExp("*")}});
          return FoldPipeOperators(query, std::move(base));
        }
      }
    }
    return AstError<std::shared_ptr<SelectStatement>>(
        "GoogleSQL AST: query without SELECT");
  }
  const GoogleSqlAstNode* select_list = select->Child("SelectList");
  if (select_list == nullptr) {
    return AstError<std::shared_ptr<SelectStatement>>(
        "GoogleSQL AST: SELECT without list");
  }

  // Named windows must resolve before select-list expressions are built.
  // The WindowClause hangs off the Select node, next to the FromClause.
  struct NamedWindowsScope {
    std::unordered_map<std::string, NamedWindowParts> previous;
    NamedWindowsScope() { previous.swap(t_named_windows); }
    ~NamedWindowsScope() { t_named_windows.swap(previous); }
    NamedWindowsScope(const NamedWindowsScope&) = delete;
    NamedWindowsScope& operator=(const NamedWindowsScope&) = delete;
    NamedWindowsScope(NamedWindowsScope&&) = delete;
    NamedWindowsScope& operator=(NamedWindowsScope&&) = delete;
  } named_windows_scope;
  const GoogleSqlAstNode* window_clause =
      select->Child("WindowClause") != nullptr ? select->Child("WindowClause")
                                               : query.Child("WindowClause");
  if (window_clause != nullptr) {
    for (const GoogleSqlAstNode* definition :
         window_clause->Children("WindowDefinition")) {
      const GoogleSqlAstNode* name_node = definition->Child("Identifier");
      const GoogleSqlAstNode* spec = definition->Child("WindowSpecification");
      if (name_node == nullptr || spec == nullptr) {
        continue;
      }
      ASSIGN_OR_RETURN(NamedWindowParts, parts,
                       ParseWindowSpecification(*spec));
      ASSIGN_OR_RETURN(std::string, hv188409_0, (Identifier(*name_node)));
      // GoogleSQL identifiers (including named windows) are case-insensitive.
      if (!t_named_windows.emplace(Lower(hv188409_0), std::move(parts))
               .second) {
        return AstError<std::shared_ptr<SelectStatement>>(
            "duplicate named window: " + hv188409_0);
      }
    }
  }

  // UDF parameter substitution scoping: names explicitly bound by this
  // query block's FROM clause shadow parameters of enclosing expansions.
  std::unordered_set<std::string> bound_names;
  if (!t_udf_frames.empty()) {
    if (const GoogleSqlAstNode* from_clause = select->Child("FromClause")) {
      for (const auto& child : from_clause->children) {
        CollectFromBoundNames(*child, &bound_names);
      }
    }
  }
  t_udf_bound_masks.push_back(std::move(bound_names));
  struct BoundMaskScope {
    BoundMaskScope() = default;
    ~BoundMaskScope() {
      if (!t_udf_bound_masks.empty()) {
        t_udf_bound_masks.pop_back();
      }
    }
    BoundMaskScope(const BoundMaskScope&) = delete;
    BoundMaskScope& operator=(const BoundMaskScope&) = delete;
    BoundMaskScope(BoundMaskScope&&) = delete;
    BoundMaskScope& operator=(BoundMaskScope&&) = delete;
  } bound_mask_scope;

  std::vector<NamedExpression> projections;
  std::vector<const GoogleSqlAstNode*> projection_nodes;
  projections.reserve(select_list->Children("SelectColumn").size());
  projection_nodes.reserve(select_list->Children("SelectColumn").size());
  for (const GoogleSqlAstNode* column : select_list->Children("SelectColumn")) {
    const GoogleSqlAstNode* expression_node = nullptr;
    for (const auto& child : column->children) {
      if (child->kind != "Alias") {
        expression_node = child.get();
        break;
      }
    }
    if (expression_node == nullptr) {
      return AstError<std::shared_ptr<SelectStatement>>(
          "GoogleSQL AST: empty column");
    }
    if (expression_node->kind == "DotStar" &&
        expression_node->Child("PathExpression") == nullptr) {
      // `sql_udf(...).*` is a projection expansion, not a column named `*`.
      const GoogleSqlAstNode* call = expression_node->Child("FunctionCall");
      if (call == nullptr) {
        return AstError<std::shared_ptr<SelectStatement>>(
            "GoogleSQL AST: malformed DotStar");
      }
      ASSIGN_OR_RETURN(Expression, hv190303_0, (VisitExpression(*call)));
      Expression base = std::move(hv190303_0);
      const auto fields = StructProjectionFields(base);
      if (!fields) {
        return AstError<std::shared_ptr<SelectStatement>>(
            "GoogleSQL AST: cannot infer fields for function result .* "
            "expansion");
      }
      for (const std::string& field : *fields) {
        projections.emplace_back(
            field, FunctionCallExp(
                       "__get_field_safe",
                       {base, ConstantValueExp(Value(std::string(field)))}));
        projection_nodes.push_back(expression_node);
      }
      continue;
    }
    if (expression_node->kind == "StarWithModifiers" ||
        expression_node->kind == "DotStarWithModifiers") {
      // `* EXCEPT (...) / REPLACE (...)` and `rel.* EXCEPT (...)`: the star
      // keeps its plain expression shape so existing star detection holds;
      // the modifiers ride on the NamedExpression payload for the engine's
      // star expansion to honor.
      const GoogleSqlAstNode* star_path = nullptr;
      const GoogleSqlAstNode* star_modifiers = nullptr;
      if (expression_node->kind == "DotStarWithModifiers") {
        star_path = expression_node->Child("PathExpression");
        star_modifiers = expression_node->Child("StarModifiers");
        if (star_path == nullptr) {
          return AstError<std::shared_ptr<SelectStatement>>(
              "GoogleSQL AST: malformed DotStarWithModifiers");
        }
      } else {
        star_modifiers = expression_node->Child("StarModifiers");
      }
      if (star_modifiers == nullptr) {
        return AstError<std::shared_ptr<SelectStatement>>(
            "GoogleSQL AST: malformed " + expression_node->kind);
      }
      ASSIGN_OR_RETURN(std::string, hvstaralias_0, (Alias(*column)));
      if (!hvstaralias_0.empty()) {
        return AstError<std::shared_ptr<SelectStatement>>(
            "GoogleSQL AST: a star with EXCEPT/REPLACE cannot take an alias");
      }
      Expression star_expression;
      if (star_path != nullptr) {
        ASSIGN_OR_RETURN(std::string, hvstarp_0, (Path(*star_path)));
        star_expression = ColumnValueExp(ColumnName(std::move(hvstarp_0), "*"));
      } else {
        star_expression = ColumnValueExp("*");
      }
      auto modifiers = std::make_shared<StarModifiers>();
      std::unordered_set<std::string> seen_except;
      std::unordered_set<std::string> seen_replace;
      for (const GoogleSqlAstNode* except_list :
           star_modifiers->Children("StarExceptList")) {
        for (const GoogleSqlAstNode* list :
             except_list->Children("ExpressionList")) {
          for (const auto& entry_holder : list->children) {
            const GoogleSqlAstNode* entry = entry_holder.get();
            // The pinned parser only accepts bare identifiers here; accept a
            // path spelling defensively and store the raw reference text.
            std::string entry_name;
            if (entry->kind == "Identifier") {
              ASSIGN_OR_RETURN(std::string, hvstari_0, (Identifier(*entry)));
              entry_name = std::move(hvstari_0);
            } else if (entry->kind == "PathExpression") {
              ASSIGN_OR_RETURN(std::string, hvstare_0, (Path(*entry)));
              entry_name = std::move(hvstare_0);
            } else {
              return AstError<std::shared_ptr<SelectStatement>>(
                  "GoogleSQL AST: EXCEPT entries must be column names");
            }
            std::string lowered = entry_name;
            for (char& c : lowered) {
              c = static_cast<char>(
                  std::tolower(static_cast<unsigned char>(c)));
            }
            if (!seen_except.insert(lowered).second) {
              return AstError<std::shared_ptr<SelectStatement>>(
                  "GoogleSQL AST: duplicate column in EXCEPT list: " +
                  entry_name);
            }
            modifiers->except_columns.push_back(std::move(entry_name));
          }
        }
      }
      for (const GoogleSqlAstNode* replace :
           star_modifiers->Children("StarReplaceItem")) {
        if (replace->children.size() != 2) {
          return AstError<std::shared_ptr<SelectStatement>>(
              "GoogleSQL AST: malformed REPLACE item");
        }
        const GoogleSqlAstNode* value_node = replace->children[0].get();
        const GoogleSqlAstNode* name_node = replace->children[1].get();
        if (name_node->kind != "Identifier") {
          std::swap(value_node, name_node);
        }
        if (name_node->kind != "Identifier") {
          return AstError<std::shared_ptr<SelectStatement>>(
              "GoogleSQL AST: malformed REPLACE item");
        }
        ASSIGN_OR_RETURN(Expression, hvstarr_0, (VisitExpression(*value_node)));
        Expression replace_expression = std::move(hvstarr_0);
        ASSIGN_OR_RETURN(std::string, hvstarr_1, (Identifier(*name_node)));
        std::string lowered = hvstarr_1;
        for (char& c : lowered) {
          c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        if (!seen_replace.insert(lowered).second) {
          return AstError<std::shared_ptr<SelectStatement>>(
              "GoogleSQL AST: duplicate column in REPLACE list: " + hvstarr_1);
        }
        modifiers->replace_items.push_back(
            StarReplaceItem{.column = std::move(hvstarr_1),
                            .expression = std::move(replace_expression)});
      }
      NamedExpression star_item{"*", std::move(star_expression)};
      star_item.star_modifiers = std::move(modifiers);
      projections.push_back(std::move(star_item));
      projection_nodes.push_back(expression_node);
      continue;
    }
    ASSIGN_OR_RETURN(Expression, hv190694_0,
                     (VisitExpression(*expression_node)));
    Expression expression = std::move(hv190694_0);
    if (!expression) {
      return AstError<std::shared_ptr<SelectStatement>>(
          "GoogleSQL AST: missing SELECT expression");
    }
    ASSIGN_OR_RETURN(std::string, hv190757_0, (Alias(*column)));
    std::string name = std::move(hv190757_0);
    if (name.empty() && expression->Type() == TypeTag::kColumnValue) {
      name = expression->AsColumnValue().GetColumnName().name;
    }
    projections.emplace_back(name, std::move(expression));
    projection_nodes.push_back(expression_node);
  }

  const std::string upper_select_detail = UpperCopy(select->detail);
  const GoogleSqlAstNode* select_as = select->Child("SelectAs");
  const GoogleSqlAstNode* as_struct = select->Child("AsStruct");
  const bool is_as_struct =
      (select_as != nullptr &&
       UpperCopy(select_as->detail).find("STRUCT") != std::string::npos) ||
      as_struct != nullptr;
  // SELECT AS VALUE yields the bare value as the sole column; only named
  // proto/struct targets are folded into a field-list literal.
  const bool as_value =
      select_as != nullptr &&
      UpperCopy(select_as->detail).find("VALUE") != std::string::npos;
  if (as_value && projections.size() != 1) {
    return AstError<std::shared_ptr<SelectStatement>>(
        "SELECT AS VALUE requires exactly one expression");
  }
  if (as_value) {
    projections.front().expression = FunctionCallExp(
        "__value_table_value", {projections.front().expression});
  }
  // `SELECT AS Proto p.*` selects the value-table message itself.  It is not
  // a proto field literally named `*`, so preserve the value expression and
  // avoid wrapping it in a constructor that would serialize `{*: 1}`.
  const bool selects_value_table_star =
      projections.size() == 1 && projection_nodes.size() == 1 &&
      projection_nodes.front()->kind == "DotStar" &&
      projections.front().expression->Type() == TypeTag::kColumnValue &&
      projections.front().expression->AsColumnValue().GetColumnName().name ==
          "*";
  if (selects_value_table_star && !is_as_struct) {
    projections.front().expression = FunctionCallExp(
        "__value_table_value", {projections.front().expression});
  }
  // SELECT AS <proto>: validate projected fields against the registry so
  // required-field violations and invalid enum values fail at prepare time.
  if (!is_as_struct && !as_value && select_as != nullptr) {
    if (const GoogleSqlAstNode* as_path = select_as->Child("PathExpression")) {
      std::vector<std::pair<std::string, const GoogleSqlAstNode*>> named;
      const size_t count =
          std::min(projections.size(), projection_nodes.size());
      named.reserve(count);
      for (size_t i = 0; i < count; ++i) {
        named.emplace_back(projections[i].name, projection_nodes[i]);
      }
      ASSIGN_OR_RETURN(std::string, hv193609_0, (Path(*as_path)));
      Status st201702 =
          ValidateSelectAsProjections(hv193609_0, named, &projections);
      if (st201702 != Status::kSuccess) {
        return st201702;
      }
    }
  }
  if (!is_as_struct && !as_value &&
      (select_as != nullptr ||
       (upper_select_detail.find("AS_MODE=") != std::string::npos &&
        upper_select_detail.find("AS_MODE=VALUE") == std::string::npos))) {
    // SELECT AS <proto type>: fold the projections into a single proto
    // TEXT-format payload through the runtime constructor so non-constant
    // arguments (subqueries, column refs, temporal values) format per row.
    std::string type_name;
    if (select_as != nullptr) {
      for (const auto& child : select_as->children) {
        if (child->kind == "PathExpression" || child->kind == "SimpleType") {
          ASSIGN_OR_RETURN(std::string, hv194317_0, (Path(*child)));
          std::string candidate;
          if (child->kind == "PathExpression") {
            candidate = std::move(hv194317_0);
          } else {
            ASSIGN_OR_RETURN(std::string, h_cand_t, (SqlTypeFromAst(*child)));
            candidate = std::move(h_cand_t);
          }
          for (char& c : candidate) {
            if (c == '`') {
              c = ' ';
            }
          }
          std::string collapsed;
          for (const char c : candidate) {
            if (c != ' ' || (!collapsed.empty() && collapsed.back() != ' ')) {
              collapsed.push_back(c);
            }
          }
          if (!collapsed.empty()) {
            type_name = collapsed;
            break;
          }
        }
      }
    }
    if (type_name.empty()) {
      const size_t mode = upper_select_detail.find("AS_MODE=");
      if (mode != std::string::npos) {
        size_t end = mode + 8;
        while (end < select->detail.size() && select->detail[end] != ',' &&
               select->detail[end] != ' ') {
          ++end;
        }
        type_name = select->detail.substr(mode + 8, end - mode - 8);
      } else if (UpperCopy(select_as != nullptr ? select_as->detail
                                                : std::string())
                     .size() > 5) {
        // SelectAs detail carries the raw target text after "type=".
        const std::string& detail = select_as->detail;
        const size_t eq = detail.find("path=");
        if (eq != std::string::npos) {
          size_t end = eq + 5;
          while (end < detail.size() && detail[end] != ',' &&
                 detail[end] != ' ') {
            ++end;
          }
          type_name = detail.substr(eq + 5, end - eq - 5);
        }
      }
    }
    if (!type_name.empty()) {
      std::vector<Expression> args;
      args.reserve(1 +
                   (2 * std::min(projections.size(), projection_nodes.size())));
      args.emplace_back(ConstantValueExp(Value(std::string(type_name))));
      for (size_t i = 0; i < projections.size() && i < projection_nodes.size();
           ++i) {
        std::string fname = projections[i].name.empty()
                                ? ("f" + std::to_string(i + 1))
                                : projections[i].name;
        const GoogleSqlAstNode& value_node = *projection_nodes[i];
        if (value_node.kind == "BooleanLiteral") {
          const std::string upper_literal = UpperCopy(value_node.detail);
          args.emplace_back(ConstantValueExp(
              Value(upper_literal == "TRUE" ? std::string("true")
                                            : std::string("false"))));
        } else {
          args.push_back(projections[i].expression);
        }
        args.emplace_back(ConstantValueExp(Value(std::move(fname))));
      }
      if (selects_value_table_star) {
        projections = {NamedExpression(
            "",
            FunctionCallExp("__value_table_proto",
                            {ConstantValueExp(Value(std::string(type_name))),
                             projections.front().expression}))};
      } else {
        projections = {NamedExpression(
            "", FunctionCallExp("__proto_new", std::move(args)))};
      }
    }
  }

  std::vector<SelectSource> sources;

  std::vector<std::string> tables;
  tables.reserve(sources.size());
  if (const GoogleSqlAstNode* from = select->Child("FromClause")) {
    for (const auto& child : from->children) {
      Status st197530 =
          AppendSources(*child, JoinType::kCross, nullptr, &sources);
      if (st197530 != Status::kSuccess) {
        return st197530;
      }
    }
    for (const SelectSource& source : sources) {
      if (!source.table.empty()) {
        const std::string relation =
            source.alias.empty() ? source.table : source.alias;
        tables.push_back(relation);
      }
    }
  }

  Expression where;
  if (const GoogleSqlAstNode* clause = select->Child("WhereClause")) {
    if (!clause->children.empty()) {
      ASSIGN_OR_RETURN(Expression, hv197970_0,
                       (VisitExpression(*clause->children[0])));
      where = std::move(hv197970_0);
    }
  }

  std::vector<SelectStatement::OrderByTerm> order_by;
  if (const GoogleSqlAstNode* order = query.Child("OrderBy")) {
    for (const GoogleSqlAstNode* term : order->Children("OrderingExpression")) {
      ASSIGN_OR_RETURN(WindowOrderTerm, parsed, (ParseOrderingTerm(term)));
      if (!parsed.expression) {
        continue;
      }
      // GoogleSQL: an unsigned integer ORDER BY item sorts by the
      // SELECT-list ordinal, not by the constant itself.
      if (parsed.expression->Type() == TypeTag::kConstantValue) {
        const Value& ordinal_value =
            parsed.expression->AsConstantValue().GetValue();
        if (ordinal_value.type == ValueType::kInt64 &&
            ordinal_value.value.int_value >= 0) {
          const auto ordinal =
              static_cast<size_t>(ordinal_value.value.int_value);
          const bool has_unexpanded_star =
              std::ranges::any_of(projections, [](const NamedExpression& item) {
                return IsUnexpandedStar(item.expression);
              });
          if (has_unexpanded_star) {
            // The expanded select-list width is only known after binding,
            // so defer the ordinal; BindSelect resolves it against the
            // expanded list.
            order_by.push_back({std::move(parsed.expression), parsed.ascending,
                                parsed.nulls_first});
            continue;
          }
          if (ordinal >= 1 && ordinal <= projections.size() &&
              projections[ordinal - 1].expression) {
            order_by.push_back(
                {ReapplyCollation(projections[ordinal - 1].expression,
                                  parsed.expression),
                 parsed.ascending, parsed.nulls_first});
            continue;
          }
          return AstError<std::shared_ptr<SelectStatement>>(
              "GoogleSQL AST: ORDER BY ordinal out of range");
        }
      }
      if (const std::optional<std::string> name =
              AmbiguousOrderByName(projections, parsed.expression);
          name.has_value()) {
        return AstError<std::shared_ptr<SelectStatement>>(
            "Column name " + *name + " is ambiguous");
      }
      order_by.push_back(
          {std::move(parsed.expression), parsed.ascending, parsed.nulls_first});
    }
  }

  std::optional<size_t> limit;
  size_t offset = 0;
  // GoogleSQL raises when LIMIT/OFFSET is negative or NULL; the dump shapes
  // those operands as IntLiteral, UnaryExpression(-), or NullLiteral.
  auto validate_limit_operand = [](const GoogleSqlAstNode& operand,
                                   std::string_view clause)
      -> StatusOr<std::shared_ptr<SelectStatement>> {
    if (operand.kind == "NullLiteral") {
      return AstError<std::shared_ptr<SelectStatement>>(std::string(clause) +
                                                        " must not be NULL");
    }
    if (operand.kind == "UnaryExpression" && operand.detail == "-" &&
        !operand.children.empty() &&
        operand.children.front()->kind == "IntLiteral") {
      return AstError<std::shared_ptr<SelectStatement>>(
          std::string(clause) + " must be non-negative");
    }
    return std::shared_ptr<SelectStatement>{};
  };
  // Constant integer operands may arrive wrapped (e.g. LIMIT (cast(1 as
  // int32)) after parameter substitution, or an Offset wrapper node). Fold
  // them through the expression evaluator; only truly non-constant operands
  // (query parameters that survived substitution) fail loudly instead of
  // silently degrading to unlimited.
  std::function<std::optional<int64_t>(const GoogleSqlAstNode&)> fold_int =
      [&](const GoogleSqlAstNode& node) -> std::optional<int64_t> {
    if (node.kind == "IntLiteral") {
      auto h_int = ParseIntLiteral(node);
      if (!h_int.HasValue()) {
        return std::nullopt;
      }
      return h_int.Value();
    }
    if (node.kind == "Location" || node.kind == "Hint") {
      return std::nullopt;
    }
    // Wrapper nodes (Offset/Limit/ExpressionList/...): search children.
    if (node.kind == "Offset" || node.kind == "Limit" ||
        node.kind == "ExpressionList" || node.kind == "Expression") {
      for (const auto& sub : node.children) {
        if (auto found = fold_int(*sub)) {
          return found;
        }
      }
      return std::nullopt;
    }
    // An expression operand (LIMIT 1+2, LIMIT CAST(x AS INT64)) is folded as
    // a whole. Never fall back to the first integer literal found among its
    // children: that turned LIMIT 1+2 into LIMIT 1.
    auto folded_or = VisitExpression(node);
    if (!folded_or.HasValue()) {
      return std::nullopt;
    }
    Expression folded = folded_or.MoveValue();
    if (!folded) {
      return std::nullopt;
    }
    Value constant;
    if (folded->Type() == TypeTag::kConstantValue) {
      constant = folded->AsConstantValue().GetValue();
    } else if (folded->TouchedColumns().empty()) {
      StatusOr<Value> evaluated = folded->TryEvaluate(Row(), Schema());
      if (!evaluated.HasValue()) {
        return std::nullopt;
      }
      constant = evaluated.MoveValue();
    } else {
      return std::nullopt;
    }
    if (constant.type == ValueType::kInt64 && !constant.IsNull()) {
      return constant.value.int_value;
    }
    return std::nullopt;
  };
  if (const GoogleSqlAstNode* limit_offset = query.Child("LimitOffset")) {
    if (const GoogleSqlAstNode* limit_node = limit_offset->Child("Limit")) {
      for (const auto& child : limit_node->children) {
        if (child->kind == "Location" || child->kind == "Hint") {
          continue;
        }
        if (auto h_lim = validate_limit_operand(*child, "LIMIT");
            !h_lim.HasValue()) {
          return h_lim.GetStatus();
        }
        if (child->kind == "IntLiteral") {
          ASSIGN_OR_RETURN(uint64_t, hv202084_0,
                           (ParseUnsignedLiteral(*child)));
          limit = static_cast<size_t>(hv202084_0);
        } else if (auto folded = fold_int(*child)) {
          if (*folded < 0) {
            return AstError<std::shared_ptr<SelectStatement>>(
                "LIMIT must be non-negative");
          }
          limit = static_cast<size_t>(*folded);
        } else {
          // A parameterized LIMIT with no static row count. Silently
          // treating it as unlimited would return every row; fail loudly.
          return AstError<std::shared_ptr<SelectStatement>>(
              "LIMIT requires an integer literal in this engine");
        }
      }
    }
    for (const auto& child : limit_offset->children) {
      if (child->kind == "Limit" || child->kind == "Location" ||
          child->kind == "Hint" || child->kind == "WithTies") {
        continue;
      }
      if (auto h_off = validate_limit_operand(*child, "OFFSET");
          !h_off.HasValue()) {
        return h_off.GetStatus();
      }
      if (child->kind == "IntLiteral") {
        ASSIGN_OR_RETURN(uint64_t, hv202986_0, (ParseUnsignedLiteral(*child)));
        offset = hv202986_0;
      } else if (auto folded = fold_int(*child)) {
        if (*folded < 0) {
          return AstError<std::shared_ptr<SelectStatement>>(
              "OFFSET must be non-negative");
        }
        offset = static_cast<size_t>(*folded);
      } else if (child->kind == "Offset") {
        // Empty OFFSET wrapper (no numeric child): plain LIMIT without
        // OFFSET. Older code ignored it; keep ignoring instead of failing.
        continue;
      } else {
        return AstError<std::shared_ptr<SelectStatement>>(
            "OFFSET requires an integer literal in this engine");
      }
    }
  }

  auto statement = std::make_shared<SelectStatement>(
      std::move(projections), std::move(tables), std::move(where),
      std::move(order_by), limit.value_or(0), offset,
      select->detail.find("distinct=true") != std::string::npos);
  statement->SetLimit(limit);
  statement->SetSources(std::move(sources));
  for (const SelectSource& source : statement->Sources()) {
    if (!source.table.empty() && !source.alias.empty() &&
        source.alias != source.table) {
      statement->AddAlias(source.alias, source.table);
    }
  }

  std::vector<Expression> distinct_on_expressions;
  if (const GoogleSqlAstNode* distinct_on = select->Child("DistinctOn")) {
    size_t distinct_keys = 0;
    for (const auto& child : distinct_on->children) {
      distinct_keys += child->kind == "ExpressionList" ||
                               child->kind == "GroupingItem"
                           ? child->children.size()
                           : 1;
    }
    distinct_on_expressions.reserve(distinct_keys);
    for (const auto& child : distinct_on->children) {
      if (child->kind == "ExpressionList" || child->kind == "GroupingItem") {
        for (const auto& grandchild : child->children) {
          ASSIGN_OR_RETURN(Expression, hv204471_0,
                           (VisitExpression(*grandchild)));
          distinct_on_expressions.push_back(std::move(hv204471_0));
        }
      } else if (child->kind == "SelectColumn" && !child->children.empty()) {
        ASSIGN_OR_RETURN(Expression, hv204634_0,
                         (VisitExpression(*child->children[0])));
        distinct_on_expressions.push_back(std::move(hv204634_0));
      } else {
        ASSIGN_OR_RETURN(Expression, h_tmp_5432, (VisitExpression(*child)));
        distinct_on_expressions.push_back(std::move(h_tmp_5432));
      }
    }
  } else if (const GoogleSqlAstNode* distinct_on_clause =
                 select->Child("DistinctOnClause")) {
    distinct_on_expressions.reserve(distinct_on_clause->children.size());
    for (const auto& child : distinct_on_clause->children) {
      if (child->kind == "SelectColumn" && !child->children.empty()) {
        ASSIGN_OR_RETURN(Expression, hv205056_0,
                         (VisitExpression(*child->children[0])));
        distinct_on_expressions.push_back(std::move(hv205056_0));
      } else {
        ASSIGN_OR_RETURN(Expression, h_tmp_5441, (VisitExpression(*child)));
        distinct_on_expressions.push_back(std::move(h_tmp_5441));
      }
    }
  }
  if (!distinct_on_expressions.empty()) {
    statement->SetDistinctOn(std::move(distinct_on_expressions));
  }

  if (const GoogleSqlAstNode* limit_offset = query.Child("LimitOffset")) {
    if (limit_offset->Child("WithTies") != nullptr ||
        limit_offset->detail.find("with_ties") != std::string::npos ||
        limit_offset->detail.find("WITH TIES") != std::string::npos) {
      statement->SetWithTies(true);
    }
  }
  if (query.Child("WithTies") != nullptr) {
    statement->SetWithTies(true);
  }

  // Non-empty when GROUP BY carried ROLLUP / CUBE / GROUPING SETS: each entry
  // is one branch's active grouping keys. The UNION-ALL expansion runs after
  // HAVING / QUALIFY / WITH attached (see the end of this function).
  std::vector<std::vector<Expression>> pending_grouping_sets;

  if (const GoogleSqlAstNode* group = select->Child("GroupBy")) {
    const bool is_group_by_all = group->Child("GroupByAll") != nullptr;
    if (is_group_by_all) {
      std::vector<Expression> expressions;
      expressions.reserve(statement->SelectList().size());
      const bool has_unexpanded_star = std::ranges::any_of(
          statement->SelectList(), [](const NamedExpression& item) {
            return IsUnexpandedStar(item.expression);
          });
      for (const NamedExpression& named : statement->SelectList()) {
        if (!named.expression || IsUnexpandedStar(named.expression)) {
          continue;  // the star's expansion isn't known until binding
        }
        if (!ContainsAggregate(named.expression) &&
            named.expression->Type() != TypeTag::kWindowFunctionExp &&
            named.expression->Type() != TypeTag::kConstantValue) {
          expressions.push_back(named.expression);
        }
      }
      if (has_unexpanded_star) {
        // GROUP BY ALL with SELECT * groups by every expanded column. Leave
        // the grouping list empty here; the star-expanding route materializes
        // the full schema and derives the effective grouping from all columns.
        expressions.clear();
      }
      statement->SetGroupBy(std::move(expressions));
    } else {
      // Ordinal resolution shared by plain items and set-constructor
      // arguments (GoogleSQL: integer GROUP BY items are SELECT-list
      // ordinals).
      const auto resolve_grouping_term =
          [&](const GoogleSqlAstNode& node) -> StatusOr<Expression> {
        if (node.kind == "IntLiteral") {
          ASSIGN_OR_RETURN(uint64_t, ordinal_value,
                           (ParseUnsignedLiteral(node)));
          if (ordinal_value == 0 ||
              ordinal_value > statement->SelectList().size()) {
            return AstError<Expression>(
                "GoogleSQL AST: GROUP BY ordinal out of range");
          }
          const auto index = static_cast<size_t>(ordinal_value - 1);
          const Expression& expression = statement->SelectList()[index].expression;
          if (!expression) {
            return AstError<Expression>(
                "GoogleSQL AST: GROUP BY ordinal out of range");
          }
          return expression;
        }
        return VisitExpression(node);
      };
      std::vector<Expression> expressions;
      // GROUPING SETS / ROLLUP / CUBE state: plain items join every set,
      // constructors contribute lists of sets that combine as a cross
      // product (`GROUP BY a, ROLLUP(b)` == GROUPING SETS ((a,b), (a))).
      std::vector<Expression> plain_expressions;
      plain_expressions.reserve(group->Children("GroupingItem").size());
      std::vector<std::vector<std::vector<Expression>>> constructor_sets;
      constexpr size_t kMaxGroupingSetBranches = 64;
      // Expands one `Rollup`/`Cube` AST node into its grouping-key sets:
      // ROLLUP(a, b) => (a,b), (a), (); CUBE(a, b) => (a,b), (b), (a), ().
      // Shared by the top-level GROUP BY item and set-list elements, where
      // `GROUPING SETS ((k), ROLLUP(v))` unions the expansions into the list.
      const auto expand_rollup_or_cube = [&](const GoogleSqlAstNode& term)
          -> StatusOr<std::vector<std::vector<Expression>>> {
        std::vector<Expression> keys;
        keys.reserve(term.children.size());
        for (const auto& key_node : term.children) {
          ASSIGN_OR_RETURN(Expression, key, (resolve_grouping_term(*key_node)));
          keys.push_back(std::move(key));
        }
        std::vector<std::vector<Expression>> sets;
        sets.reserve(keys.size() + 1);
        if (term.kind == "Rollup") {
          // ROLLUP(a, b, c) => (a,b,c), (a,b), (a), ().
          for (size_t len = keys.size(); len > 0; --len) {
            sets.emplace_back(keys.begin(),
                              keys.begin() + static_cast<ptrdiff_t>(len));
          }
          sets.emplace_back();
        } else {
          // CUBE(a, b) => (a,b), (b), (a), () — popcount descending, mask
          // descending.
          std::vector<std::pair<size_t, size_t>> mask_popcount;
          const size_t total = size_t{1} << keys.size();
          mask_popcount.reserve(total);
          for (size_t mask = 0; mask < total; ++mask) {
            mask_popcount.emplace_back(
                mask, static_cast<size_t>(__builtin_popcountll(mask)));
          }
          std::stable_sort(mask_popcount.begin(), mask_popcount.end(),
                           [](const auto& l, const auto& r) {
                             if (l.second != r.second) {
                               return l.second > r.second;
                             }
                             return l.first > r.first;
                           });
          for (const auto& [mask, _] : mask_popcount) {
            std::vector<Expression> set;
            set.reserve(keys.size());
            for (size_t i = 0; i < keys.size(); ++i) {
              if ((mask & (size_t{1} << i)) != 0) {
                set.push_back(keys[i]);
              }
            }
            sets.push_back(std::move(set));
          }
        }
        return sets;
      };
      for (const GoogleSqlAstNode* item : group->Children("GroupingItem")) {
        if (item->children.empty()) {
          continue;
        }
        const GoogleSqlAstNode& term = *item->children[0];
        if (term.kind == "Rollup" || term.kind == "Cube") {
          if (term.kind == "Cube" &&
              term.children.size() >=
                  8) {  // 2^8 branches would swamp statement prep.
            return AstError<std::shared_ptr<SelectStatement>>(
                "GROUP BY CUBE supports at most 7 columns in this engine");
          }
          ASSIGN_OR_RETURN(std::vector<std::vector<Expression>>, sets,
                           (expand_rollup_or_cube(term)));
          constructor_sets.push_back(std::move(sets));
          continue;
        }
        if (term.kind == "GroupingSetList") {
          std::vector<std::vector<Expression>> sets;
          for (const GoogleSqlAstNode* grouping_set :
               term.Children("GroupingSet")) {
            // Elements may themselves be constructors: a ROLLUP/CUBE inside
            // the list unions every set it spans
            // (`GROUPING SETS ((k), ROLLUP(v))` == GROUPING SETS ((k), (v),
            // ())). A nested GROUPING SETS as an element is a parser-level
            // syntax error, so no other constructor shapes can appear here.
            if (grouping_set->children.size() == 1 &&
                (grouping_set->children[0]->kind == "Rollup" ||
                 grouping_set->children[0]->kind == "Cube")) {
              const GoogleSqlAstNode& nested = *grouping_set->children[0];
              if (nested.kind == "Cube" && nested.children.size() >= 8) {
                return AstError<std::shared_ptr<SelectStatement>>(
                    "GROUP BY CUBE supports at most 7 columns in this engine");
              }
              ASSIGN_OR_RETURN(std::vector<std::vector<Expression>>, expanded,
                               (expand_rollup_or_cube(nested)));
              sets.insert(sets.end(), expanded.begin(), expanded.end());
              continue;
            }
            std::vector<Expression> set;
            for (const auto& element : grouping_set->children) {
              if (element->kind == "StructConstructorWithParens") {
                // Composite set element: GROUPING SETS ((a, b), ...).
                for (const auto& inner : element->children) {
                  ASSIGN_OR_RETURN(Expression, key,
                                   (resolve_grouping_term(*inner)));
                  set.push_back(std::move(key));
                }
              } else if (element->kind != "Location") {
                ASSIGN_OR_RETURN(Expression, key,
                                 (resolve_grouping_term(*element)));
                set.push_back(std::move(key));
              }
            }
            sets.push_back(std::move(set));
          }
          if (sets.empty()) {
            return AstError<std::shared_ptr<SelectStatement>>(
                "GoogleSQL AST: GROUPING SETS requires at least one set");
          }
          constructor_sets.push_back(std::move(sets));
          continue;
        }
        ASSIGN_OR_RETURN(Expression, resolved, (resolve_grouping_term(term)));
        plain_expressions.push_back(std::move(resolved));
      }
      if (!constructor_sets.empty()) {
        size_t branch_count = 1;
        for (const auto& sets : constructor_sets) {
          branch_count *= sets.size();
          if (branch_count > kMaxGroupingSetBranches) {
            return AstError<std::shared_ptr<SelectStatement>>(
                "GROUPING SETS expansion exceeds " +
                std::to_string(kMaxGroupingSetBranches) + " branches");
          }
        }
        // Cross product of the constructors' sets, plain items joined into
        // every combination (kept in the order the SQL spelled them out).
        std::vector<std::vector<Expression>> combined = {plain_expressions};
        for (const auto& sets : constructor_sets) {
          std::vector<std::vector<Expression>> next;
          next.reserve(combined.size() * sets.size());
          for (const auto& prefix : combined) {
            for (const auto& set : sets) {
              std::vector<Expression> merged = prefix;
              merged.insert(merged.end(), set.begin(), set.end());
              next.push_back(std::move(merged));
            }
          }
          combined = std::move(next);
        }
        pending_grouping_sets = std::move(combined);
      } else {
        expressions = std::move(plain_expressions);
        if (expressions.empty()) {
          // `GROUP BY ()` groups every row into a single group. An empty key
          // list is indistinguishable from no GROUP BY downstream, so key on
          // a constant: one group, and non-aggregate references still error.
          expressions.push_back(ConstantValueExp(Value(int64_t{0})));
          statement->SetGroupAll(true);
        }
        statement->SetGroupBy(std::move(expressions));
      }
    }
  }
  if (const GoogleSqlAstNode* having = select->Child("Having")) {
    if (!having->children.empty()) {
      ASSIGN_OR_RETURN(Expression, hv207519_0,
                       (VisitExpression(*having->children[0])));
      statement->SetHaving(std::move(hv207519_0));
    }
  }
  const GoogleSqlAstNode* qualify = select->Child("Qualify") != nullptr
                                        ? select->Child("Qualify")
                                        : query.Child("Qualify");
  if (qualify != nullptr) {
    if (!qualify->children.empty()) {
      ASSIGN_OR_RETURN(Expression, hv207867_0,
                       (VisitExpression(*qualify->children[0])));
      statement->SetQualify(std::move(hv207867_0));
    }
  }
  if (const GoogleSqlAstNode* with = query.Child("WithClause")) {
    const bool recursive =
        Lower(with->detail).find("recursive") != std::string::npos;
    for (const GoogleSqlAstNode* entry : with->Children("WithClauseEntry")) {
      const GoogleSqlAstNode* aliased = entry->Child("AliasedQuery");
      if (aliased == nullptr) {
        continue;
      }
      const GoogleSqlAstNode* name = aliased->Child("Identifier");
      const GoogleSqlAstNode* nested = aliased->Child("Query");
      if (name != nullptr && nested != nullptr) {
        ASSIGN_OR_RETURN(std::string, hv208494_0, (Identifier(*name)));
        const std::string cte_name = std::move(hv208494_0);
        if (recursive) {
          ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv208575_0,
                           (VisitQuery(*nested)));
          statement->AddRecursiveWithQuery(cte_name, std::move(hv208575_0));
          RETURN_IF_FAIL(ApplyRecursiveDepthFromAliased(statement.get(),
                                                        *aliased, cte_name));
        } else {
          ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, q_q,
                           (VisitQuery(*nested)));
          statement->AddWithQuery(cte_name, std::move(q_q));
        }
      }
    }
  }
  // Phase 8 routing: plain multi-table FROM lists (cross/inner joins, with
  // or without table aliases) stay on the cost-based optimizer path; the
  // engine folds INNER ON conditions into the WHERE conjunction. Only
  // features the optimizer cannot represent yet force the relational
  // executor: FROM-subqueries and outer joins. M6 admits plain LEFT joins
  // (base-table sources, ON condition, no USING/nesting): the optimizer
  // carries their ON conditions as outer-join predicates, and any shape
  // beyond its slice returns kNotImplemented so the engine falls back to
  // the relational path. Single RIGHT/FULL edges are admitted by the
  // M6+1 post-rewrite gate in sql_engine.cpp; RIGHT/FULL chains, USING and
  // nested shapes stay relational.
  for (const SelectSource& source : statement->Sources()) {
    const bool plain_left_join =
        source.join_type == JoinType::kLeft && !source.query &&
        !source.from_nested_join && !source.unnest &&
        source.using_columns.empty() && source.join_condition &&
        !NeedsRelationalEvaluation(source.join_condition);
    if (plain_left_join) {
      continue;
    }
    if (source.query || source.join_type == JoinType::kLeft ||
        source.join_type == JoinType::kRight ||
        source.join_type == JoinType::kFull || source.from_nested_join ||
        source.unnest || !source.using_columns.empty() ||
        NeedsRelationalEvaluation(source.join_condition)) {
      statement->MarkComplex();
    }
  }
  for (const NamedExpression& projection : statement->SelectList()) {
    if (NeedsRelationalEvaluation(projection.expression)) {
      statement->MarkComplex();
    }
  }
  if (NeedsRelationalEvaluation(statement->WhereClause()) ||
      NeedsRelationalEvaluation(statement->Having())) {
    statement->MarkComplex();
  }
  if (as_value && !statement->Sources().empty()) {
    statement->MarkComplex();
  }
  // SELECT AS STRUCT / AS VALUE shape the projected value: STRUCT rows are
  // consumed by outer expressions as whole struct values (encoded like the
  // struct constructor output), VALUE strips the single column.
  statement->SetAsStruct(is_as_struct && !as_value);
  // Sort keys and grouping keys are evaluated by the plan executor with a
  // plain AST walk, so query expressions (EXISTS / scalar subqueries) there
  // must route to the relational interpreter, which resolves them against
  // the enclosing scope chain.
  for (const auto& term : statement->OrderBy()) {
    if (NeedsRelationalEvaluation(term.expression)) {
      statement->MarkComplex();
      break;
    }
  }

  // GROUPING SETS / ROLLUP / CUBE expansion. Runs last so branch copies
  // inherit the full statement (sources, HAVING, QUALIFY, WITH clauses and
  // the Phase-8 routing marks above).
  if (!pending_grouping_sets.empty()) {
    if (statement->Distinct() || statement->HasDistinctOn()) {
      return AstError<std::shared_ptr<SelectStatement>>(
          "SELECT DISTINCT with GROUPING SETS / ROLLUP / CUBE is not "
          "supported");
    }
    if (statement->Qualify()) {
      // QUALIFY windows are per-branch in the expansion but per-result-set in
      // the standard; refuse rather than silently change which rows survive.
      return AstError<std::shared_ptr<SelectStatement>>(
          "QUALIFY with GROUPING SETS / ROLLUP / CUBE is not supported");
    }
    const bool grouping_order = std::ranges::any_of(
        statement->OrderBy(), [](const SelectStatement::OrderByTerm& term) {
          return ContainsGroupingCall(term.expression);
        });
    if (relational_detail::HasWindowFunctions(*statement) || grouping_order) {
      // Hidden ORDER BY GROUPING expressions need the same post-union
      // projection as windows, even when absent from the visible select list.
      // Window calls observe the combined result of every grouping set
      // (rank() over a ROLLUP output spans detail and subtotal rows), so a
      // per-branch window would restart each rank per branch.  The window
      // expansion instead materializes every branch into a UNION ALL with
      // hidden columns for the window inputs, and a wrapper statement
      // evaluates the windows over the combined rows.
      ASSIGN_OR_RETURN(
          std::shared_ptr<SelectStatement>, expanded,
          (ExpandGroupingSetsWithWindows(statement, pending_grouping_sets)));
      if (query.kind != "Select") {
        return FoldPipeOperators(query, std::move(expanded));
      }
      return expanded;
    }

    // Reuse the grouping key fingerprints across all branches.
    const GroupingSetKeyTexts key_texts(pending_grouping_sets);

    // Alias-bound keys: `SELECT a AS k ... GROUP BY ROLLUP(k)` groups by the
    // alias, so a select entry named after an inactive bare-column key must
    // NULL-pad even though its expression (`a`) differs from the key text
    // (`k`). HAVING/ORDER BY references to the alias flow through the
    // ordinary fingerprint walk (the key text IS the alias there).
    const auto inactive_bare_key_aliases =
        [&](const GroupingSetBranchContext& context) {
          std::unordered_set<std::string> out;
          for (const auto& set : pending_grouping_sets) {
            for (const Expression& key : set) {
              if (!key || key->Type() != TypeTag::kColumnValue) {
                continue;
              }
              const std::string& text = key_texts.TextOf(key);
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

    const auto rewrite_branch =
        [&](SelectStatement& branch,
            const std::vector<Expression>& active) -> Status {
      const GroupingSetBranchContext context = key_texts.ForBranch(active);
      const std::unordered_set<std::string> inactive_aliases =
          inactive_bare_key_aliases(context);
      // Select list: NULL-pad entries that reference inactive keys (direct
      // fingerprint match or alias match), fold GROUPING() to constants.
      std::vector<NamedExpression> rewritten_list;
      rewritten_list.reserve(branch.SelectList().size());
      for (const NamedExpression& item : branch.SelectList()) {
        if (!item.expression) {
          rewritten_list.push_back(item);
          continue;
        }
        const std::string entry_text = item.expression->ToString();
        const bool entry_is_active =
            context.active_key_texts.contains(entry_text);
        const bool entry_is_inactive_key =
            !entry_is_active && context.all_key_texts.contains(entry_text);
        const bool entry_aliases_inactive_key =
            !item.name.empty() && inactive_aliases.contains(Lower(item.name)) &&
            !entry_is_active;
        if (entry_is_inactive_key || entry_aliases_inactive_key) {
          rewritten_list.emplace_back(item.name, ConstantValueExp(Value()));
          continue;
        }
        ASSIGN_OR_RETURN(Expression, rewritten,
                         (RewriteGroupingSetBranch(item.expression, context)));
        rewritten_list.emplace_back(item.name, std::move(rewritten));
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
      return Status::kSuccess;
    };

    if (pending_grouping_sets.size() == 1) {
      // A single set behaves as a plain GROUP BY; ORDER BY stays in-statement
      // and GROUPING() folds to constants inside it.
      RETURN_IF_FAIL(rewrite_branch(*statement, pending_grouping_sets.front()));
      if (query.kind != "Select") {
        return FoldPipeOperators(query, std::move(statement));
      }
      return statement;
    }

    // A snapshot, not a reference: the rewrite below mutates *statement while
    // still reading the original select list from `base`.
    const SelectStatement base =
        *statement;  // NOLINT(performance-unnecessary-copy-initialization)
    for (size_t i = 0; i < pending_grouping_sets.size(); ++i) {
      if (i == 0) {
        RETURN_IF_FAIL(
            rewrite_branch(*statement, pending_grouping_sets.front()));
        continue;
      }
      auto branch = std::make_shared<SelectStatement>(base);
      RETURN_IF_FAIL(rewrite_branch(*branch, pending_grouping_sets[i]));
      // ORDER BY / LIMIT / WITH TIES belong to the wrapper only.
      branch->SetOrderBy({});
      branch->SetLimit(std::nullopt);
      branch->SetOffset(0);
      branch->SetWithTies(false);
      // The WITH declarations stay on the owning statement; a branch carrying
      // its own copy would re-execute every CTE body once per grouping set
      // (branch execution resolves CTE references through the inherited map).
      branch->ClearWithQueries();
      statement->AddSetOperation(SetOperationKind::kUnionAll,
                                 std::move(branch));
    }
  }
  if (query.kind != "Select") {
    return FoldPipeOperators(query, statement);
  }
  return statement;
}

namespace {
// FUNCTION: register scalar and aggregate SQL functions for later call-site
// expansion. Table functions are registered as inert entries (their bodies
// produce relations, which expression call sites cannot consume yet); the
// DDL itself succeeds so downstream statements observe the objects.
const GoogleSqlAstNode* FindSqlFunctionBody(const GoogleSqlAstNode& root) {
  for (const auto& child : root.children) {
    if (child->kind == "SqlFunctionBody") {
      for (const auto& body_child : child->children) {
        if (body_child->kind != "Location") {
          return body_child.get();
        }
      }
    }
  }
  return nullptr;
}

StatusOr<std::unique_ptr<Statement>> VisitCreateFunction(
    const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* declaration = root.Child("FunctionDeclaration");
  if (declaration == nullptr) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: function declaration missing");
  }
  const GoogleSqlAstNode* path = declaration->Child("PathExpression");
  if (path == nullptr) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: function without name");
  }
  SqlUdf udf;
  ASSIGN_OR_RETURN(std::string, hv215914_0, (Path(*path)));
  udf.name = Lower(std::move(hv215914_0));
  udf.is_aggregate = root.detail.find("is_aggregate=true") != std::string::npos;
  if (const GoogleSqlAstNode* parameters =
          declaration->Child("FunctionParameters")) {
    for (const GoogleSqlAstNode* parameter :
         parameters->Children("FunctionParameter")) {
      const GoogleSqlAstNode* name_node = parameter->Child("Identifier");
      if (name_node == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: function parameter without name");
      }
      ASSIGN_OR_RETURN(std::string, hv216443_0, (Identifier(*name_node)));
      udf.parameters.emplace_back(
          Lower(std::move(hv216443_0)),
          parameter->detail.find("is_not_aggregate=true") != std::string::npos);
      std::shared_ptr<GoogleSqlAstNode> default_expr = nullptr;
      for (const auto& child : parameter->children) {
        if (child->kind != "Identifier" && child->kind != "SimpleType" &&
            child->kind != "TemplatedParameterType" &&
            child->kind != "Location") {
          default_expr = CloneAstNode(*child);
          break;
        }
      }
      udf.default_values.push_back(std::move(default_expr));
    }
  }
  bool has_pending_udf = false;
  SqlUdf pending_udf;
  if (root.kind != "CreateTableFunctionStatement") {
    const GoogleSqlAstNode* body = FindSqlFunctionBody(root);
    if (body != nullptr) {
      auto cloned = CloneAstNode(root);
      udf.root = std::move(cloned);
      udf.body = FindSqlFunctionBody(*udf.root);
      udf.parameter_counts.assign(udf.parameters.size(), 0);
      AnalyzeUdfBody(*udf.body, udf.parameters, &udf.parameter_counts,
                     &udf.simple_body);
      pending_udf = std::move(udf);
      has_pending_udf = true;
    }
  } else {
    // Table function: keep the defining Query AST so FROM references can
    // expand it with the call-site arguments (see the TVF table-source
    // branch).  Body references whose names match a parameter substitute
    // the argument value at expansion time.
    if (t_visit_source != nullptr) {
      udf.table_function_source = *t_visit_source;
    }
    const GoogleSqlAstNode* body = root.Child("Query");
    if (body != nullptr) {
      auto cloned = CloneAstNode(root);
      udf.root = std::move(cloned);
      udf.body = udf.root->Child("Query");
      udf.parameter_counts.assign(udf.parameters.size(), 0);
    }
    pending_udf = std::move(udf);
    has_pending_udf = true;
  }
  std::vector<NamedExpression> projection;
  projection.emplace_back("", ConstantValueExp(Value(int64_t{0})));
  if (!has_pending_udf) {
    return std::make_unique<SelectStatement>(
        std::move(projection), std::vector<std::string>{}, Expression{});
  }
  // T2552: the thread_local registry write is deferred into the statement
  // and runs only when the CREATE executes (PrepareStatement dispatch).
  // A Prepare that never dispatches -- EXPLAIN, or a re-Visit after a
  // failure -- must leave no function behind, mirroring CREATE VIEW.
  return std::make_unique<CreateObjectStatement>(
      StatementType::kCreateFunction,
      [udf = std::move(pending_udf)]() mutable {
        RegisterUdf(std::move(udf));
        return Status::kSuccess;
      },
      std::move(projection));
}
// Proto constructor (NEW) and SELECT AS <proto> validation against the
// embedded compliance-proto registry.

StatusOr<std::string> ConstructorTypeFullName(const GoogleSqlAstNode& node) {
  const GoogleSqlAstNode* path_node = nullptr;
  if (const GoogleSqlAstNode* simple = node.Child("SimpleType")) {
    path_node = simple->Child("PathExpression");
  }
  if (path_node == nullptr) {
    path_node = node.Child("PathExpression");
  }
  if (path_node == nullptr) {
    return std::string{};
  }
  return Path(*path_node);
}

bool FieldNameEquals(const std::string& left, const std::string& right) {
  if (left.size() != right.size()) {
    return false;
  }
  for (size_t i = 0; i < left.size(); ++i) {
    const char lc =
        static_cast<char>(std::tolower(static_cast<unsigned char>(left[i])));
    const char rc =
        static_cast<char>(std::tolower(static_cast<unsigned char>(right[i])));
    if (lc != rc) {
      return false;
    }
  }
  return true;
}

const ProtoFieldSchema* FindProtoField(
    const std::vector<ProtoFieldSchema>& fields, const std::string& name) {
  for (const ProtoFieldSchema& field : fields) {
    if (FieldNameEquals(field.name, name)) {
      return &field;
    }
  }
  return nullptr;
}

// Rejects literal enum assignments that the registry says are invalid.
Status ValidateEnumLiteralValue(const std::string& message_name,
                                const std::string& field_name,
                                const std::string& enum_type_name,
                                const GoogleSqlAstNode& expr) {
  const std::string enum_short_name(ShortTypeName(enum_type_name));
  if (!IsKnownEnum(enum_short_name)) {
    return Status::kSuccess;
  }
  auto reject = [&](const std::string& message) {
    return AstStatus("Could not store value into proto field " + message_name +
                     "." + field_name + ": " + message);
  };
  if (expr.kind == "StringLiteral") {
    ASSIGN_OR_RETURN(std::string, hv230390_0, (DecodeString(expr)));
    const std::string value = std::move(hv230390_0);
    int64_t ordinal = 0;
    if (!EnumValueForMember(enum_short_name, value, &ordinal)) {
      return reject("Out of range cast of string '" + value +
                    "' to enum type " + enum_short_name);
    }
    return Status::kSuccess;
  }
  if (expr.kind == "IntLiteral") {
    const std::string& digits = expr.detail;
    uint64_t magnitude = 0;
    const auto [ptr, ec] = std::from_chars(
        digits.data(), digits.data() + digits.size(), magnitude);
    // ec alone accepts a numeric prefix ("5junk" -> 5); the token must be
    // consumed in full.
    if (ec != std::errc() || ptr != digits.data() + digits.size() ||
        magnitude > static_cast<uint64_t>(2147483647LL)) {
      return reject("Out of range cast of integer " + digits +
                    " to enum type " + enum_short_name);
    }
    const auto ordinal = static_cast<int64_t>(magnitude);
    const std::optional<std::string> member =
        EnumMemberForValue(enum_short_name, ordinal);
    if (!member.has_value() && !EnumIsOpen(enum_short_name)) {
      return reject("Out of range cast of integer " + std::to_string(ordinal) +
                    " to enum type " + enum_short_name);
    }
  }
  return Status::kSuccess;
}

// Shared checks for one field assignment. Returns true when the assignment
// still needs a runtime guard because its value is not statically known.
StatusOr<bool> ValidateProtoFieldAssignment(const std::string& message_name,
                                            const ProtoFieldSchema& field,
                                            const GoogleSqlAstNode& expr) {
  if (expr.kind == "NullLiteral") {
    if (field.required) {
      return AstError<bool>(
          "Cannot encode a null value in required protocol message field " +
          message_name + "." + field.name);
    }
    return false;
  }
  if (field.is_enum && !field.repeated &&
      (expr.kind == "StringLiteral" || expr.kind == "IntLiteral")) {
    Status st232125 = ValidateEnumLiteralValue(message_name, field.name,
                                               field.type_name, expr);
    if (st232125 != Status::kSuccess) {
      return st232125;
    }
    return false;
  }
  if (field.repeated && expr.kind == "ArrayConstructor") {
    for (const auto& element : expr.children) {
      if (element->kind == "ArrayType" || IsAstTrivia(element->kind)) {
        continue;
      }
      if (element->kind == "NullLiteral") {
        return AstError<bool>(
            "Cannot encode a null value in repeated protocol message field " +
            message_name + "." + field.name);
      }
    }
    return false;
  }
  // Non-constant repeated arrays and non-constant enum values need runtime
  // validation.
  if (field.repeated &&
      (expr.kind == "QueryExpression" || expr.kind == "FunctionCall" ||
       expr.kind == "CallExpression" || expr.kind == "ScalarSubquery" ||
       expr.kind.starts_with("ExpressionSubquery"))) {
    return true;
  }
  if (field.is_enum && !field.repeated) {
    return true;
  }
  return false;
}

Status RequireProtoFieldsPresent(const std::string& message_name,
                                 const std::vector<ProtoFieldSchema>& fields,
                                 const std::set<std::string>& assigned) {
  for (const ProtoFieldSchema& field : fields) {
    if (field.required) {
      bool found = false;
      for (const std::string& name : assigned) {
        if (FieldNameEquals(name, field.name)) {
          found = true;
          break;
        }
      }
      if (!found) {
        return AstStatus("Required protocol message field " + message_name +
                         "." + field.name + " is not assigned");
      }
    }
  }
  return Status::kSuccess;
}

}  // namespace
StatusOr<Expression> BuildNewConstructor(const GoogleSqlAstNode& node) {
  // Registry validation runs at compile time; the actual payload is built
  // at runtime through __proto_new so per-row values (subqueries, column
  // references, TIMESTAMP/DATE conversions, NULLs) format correctly.
  ASSIGN_OR_RETURN(std::string, message_name, (ConstructorTypeFullName(node)));
  const std::vector<ProtoFieldSchema>* fields =
      message_name.empty() ? nullptr : FindProtoMessageFields(message_name);
  std::set<std::string> assigned;
  for (const auto& child : node.children) {
    if (child->kind != "NewConstructorArg" || child->children.size() < 2) {
      continue;
    }
    ASSIGN_OR_RETURN(std::string, hv234429_0,
                     (Identifier(*child->children[1])));
    const std::string field_name = std::move(hv234429_0);
    if (field_name.empty()) {
      continue;
    }
    assigned.insert(field_name);
    if (fields != nullptr) {
      const ProtoFieldSchema* field = FindProtoField(*fields, field_name);
      if (field != nullptr) {
        if (auto hv234716_0 = ValidateProtoFieldAssignment(message_name, *field,
                                                           *child->children[0]);
            !hv234716_0.HasValue()) {
          return hv234716_0.GetStatus();
        }
      }
    }
  }
  if (fields != nullptr) {
    RETURN_IF_FAIL(RequireProtoFieldsPresent(message_name, *fields, assigned));
  }
  std::string type_name;
  if (const GoogleSqlAstNode* path_node = node.Child("PathExpression")) {
    ASSIGN_OR_RETURN(std::string, hv235009_0, (Path(*path_node)));
    type_name = std::move(hv235009_0);
  } else if (const GoogleSqlAstNode* simple = node.Child("SimpleType")) {
    if (const GoogleSqlAstNode* inner = simple->Child("PathExpression")) {
      ASSIGN_OR_RETURN(std::string, hv235192_0, (Path(*inner)));
      type_name = std::move(hv235192_0);
    }
  }
  {
    for (char& c : type_name) {
      if (c == '`') {
        c = ' ';
      }
    }
    // Collapse whitespace left by backtick removal.
    std::string collapsed;
    for (const char c : type_name) {
      if (c != ' ' || (!collapsed.empty() && collapsed.back() != ' ')) {
        collapsed.push_back(c);
      }
    }
    type_name = std::move(collapsed);
  }
  std::vector<Expression> args;
  args.emplace_back(ConstantValueExp(Value(std::move(type_name))));
  for (const auto& child : node.children) {
    if (child->kind != "NewConstructorArg" || child->children.size() < 2) {
      continue;
    }
    const GoogleSqlAstNode& value_node = *child->children[0];
    const GoogleSqlAstNode& name_node = *child->children[1];
    ASSIGN_OR_RETURN(std::string, hv235966_0, (Identifier(name_node)));
    std::string field_name = std::move(hv235966_0);
    if (field_name.empty()) {
      ASSIGN_OR_RETURN(std::string, hv236048_0, (Path(name_node)));
      field_name = std::move(hv236048_0);
    }
    // Extension targets arrive as parenthesized paths: emit the bracketed
    // extension key used by TEXT format.
    if (!field_name.empty() && field_name.front() != '[' &&
        field_name.find('.') != std::string::npos) {
      field_name.insert(field_name.begin(), '[');
      field_name.push_back(']');
    }
    if (value_node.kind == "BooleanLiteral") {
      const std::string upper_literal = UpperCopy(value_node.detail);
      args.emplace_back(ConstantValueExp(Value(upper_literal == "TRUE"
                                                   ? std::string("true")
                                                   : std::string("false"))));
    } else {
      ASSIGN_OR_RETURN(Expression, h_tmp_6224, (VisitExpression(value_node)));
      args.push_back(std::move(h_tmp_6224));
    }
    args.emplace_back(ConstantValueExp(Value(std::move(field_name))));
  }
  return FunctionCallExp("__proto_new", std::move(args));
}

namespace {
Status ValidateSelectAsProjections(
    const std::string& message_name,
    const std::vector<std::pair<std::string, const GoogleSqlAstNode*>>& named,
    std::vector<NamedExpression>* projections) {
  const std::vector<ProtoFieldSchema>* fields =
      FindProtoMessageFields(message_name);
  if (fields == nullptr) {
    return Status::kSuccess;
  }
  std::set<std::string> assigned;
  for (const auto& [name, expression_node] : named) {
    assigned.insert(name);
    const ProtoFieldSchema* field = FindProtoField(*fields, name);
    if (field == nullptr) {
      continue;
    }
    if (auto v = ValidateProtoFieldAssignment(message_name, *field,
                                              *expression_node);
        !v.HasValue()) {
      return v.GetStatus();
    }
  }
  RETURN_IF_FAIL(RequireProtoFieldsPresent(message_name, *fields, assigned));
  for (size_t i = 0; i < named.size(); ++i) {
    const ProtoFieldSchema* field = FindProtoField(*fields, named[i].first);
    if (field == nullptr) {
      continue;
    }
    if (named[i].second->kind == "NullLiteral" ||
        named[i].second->kind == "StringLiteral" ||
        named[i].second->kind == "IntLiteral") {
      continue;
    }
    if (field->is_enum && !field->repeated) {
      ASSIGN_OR_RETURN(Expression, hv238054_0,
                       (VisitExpression(*named[i].second)));
      (*projections)[i] = NamedExpression(
          (*projections)[i].name,
          FunctionCallExp(
              "$proto_enum_guard",
              {std::move(hv238054_0),
               ConstantValueExp(Value(std::string(field->type_name)))}));
    }
  }
  return Status::kSuccess;
}

}  // namespace
}  // namespace ast_visitor_detail

using ViewRegistryMap = std::unordered_map<std::string, StoredViewDefinition>;

// NOLINTNEXTLINE(misc-use-anonymous-namespace): shared only within this TU.
static std::unordered_map<uint64_t, ViewRegistryMap>& AllViewRegistries() {
  static thread_local std::unordered_map<uint64_t, ViewRegistryMap> registries;
  return registries;
}

ViewRegistryMap& ViewRegistry() {
  // Key by session scope: sessions sharing an event-loop thread must not see
  // or wipe each other's TEMP views.
  return AllViewRegistries()[tinylamb::UdfSessionScope()];
}

void RegisterViewDefinition(std::string_view name,
                            const GoogleSqlAstNode& query,
                            std::string_view source) {
  ViewRegistry()[ast_visitor_detail::Lower(std::string(name))] = {
      .query = ast_visitor_detail::CloneAstNode(query),
      .source = std::string(source),
      .load_error = {}};
}
StatusOr<std::unique_ptr<Statement>> GoogleSqlAstVisitor::Visit(
    const GoogleSqlAstNode& root, std::string_view source) {
  // Callers can supply hand-built ASTs, bypassing the dump parser. Check
  // iteratively before hint validation, UDF analysis, cloning or binding.
  std::vector<std::pair<const GoogleSqlAstNode*, size_t>> pending{{&root, 0}};
  while (!pending.empty()) {
    const auto [node, depth] = pending.back();
    pending.pop_back();
    if (depth >= kMaxGoogleSqlAstDepth) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST depth limit exceeded");
    }
    for (const auto& child : node->children) {
      pending.emplace_back(child.get(), depth + 1);
    }
  }
  std::string source_storage(source);
  // Only the outermost Visit installs the source; recursive Visit calls
  // pass an empty view and keep the outer scope alive.
  std::optional<VisitSourceScope> scope;
  if (!source_storage.empty()) {
    scope.emplace(source_storage);
  }
  // Hints for other engines (qualified "engine.name") are ignored; an
  // unqualified hint is only meaningful when the engine knows it, and this
  // engine implements none: GoogleSQL rejects unknown default-engine hints
  // instead of silently executing the statement.
  if (Status st_hints = RejectUnsupportedHints(root);
      st_hints != Status::kSuccess) {
    return st_hints;
  }
  if (root.kind == "HintedStatement") {
    for (const auto& child : root.children) {
      if (child->kind == "Hint" || child->kind == "Location") {
        continue;
      }
      return Visit(*child);
    }
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: hinted statement without body");
  }
  if (root.kind == "QueryStatement") {
    const GoogleSqlAstNode* query = root.Child("Query");
    if (query == nullptr) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: missing query");
    }
    ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv239690_0,
                     (VisitQuery(*query)));
    auto statement = std::move(hv239690_0);
    if (const GoogleSqlAstNode* lock_mode = query->Child("LockMode")) {
      // The parser accepts exactly one lock mode (`FOR UPDATE`); FOR SHARE and
      // NOWAIT/SKIP LOCKED are syntax errors before this point.  Refuse an
      // unknown mode instead of silently running an unlocked read.
      if (UpperCopy(SliceSource(lock_mode)).find("UPDATE") ==
          std::string::npos) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: unsupported LockMode (only FOR UPDATE exists)");
      }
      statement->SetForUpdate(true);
    }
    return std::make_unique<SelectStatement>(*statement);
  }
  if (root.kind == "CreateConstantStatement") {
    const GoogleSqlAstNode* path = root.Child("PathExpression");
    if (path == nullptr) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: bad CREATE CONSTANT");
    }
    ASSIGN_OR_RETURN(std::string, hv240028_0, (Path(*path)));
    std::string const_name = std::move(hv240028_0);
    std::string const_val;
    // T2552: build the registry entry during the Visit, but write the
    // thread_local registries only when the statement executes (see
    // CreateObjectStatement / VisitCreateFunction).
    std::optional<Expression> pending_entry;
    for (const auto& child : root.children) {
      if (child->kind != "PathExpression" && child->kind != "Location") {
        ASSIGN_OR_RETURN(Expression, hv240217_0, (VisitExpression(*child)));
        Expression expr = std::move(hv240217_0);
        Row dummy_row;
        Schema dummy_schema;
        StatusOr<Value> v_or = expr->TryEvaluate(dummy_row, dummy_schema);
        if (v_or.HasValue()) {
          Value v = v_or.MoveValue();
          if (v.type == ValueType::kVarChar) {
            const_val = std::string(v.value.varchar_value);
          } else {
            const_val = v.AsString();
          }
          // Fully evaluable constant: pin the value itself so every
          // reference observes one deterministic result (e.g. Rand()).
          pending_entry = ConstantValueExp(Value(std::move(v)));
        } else {
          if (child->kind == "StringLiteral") {
            ASSIGN_OR_RETURN(std::string, hv240897_0, (DecodeString(*child)));
            const_val = std::move(hv240897_0);
            pending_entry = ConstantValueExp(Value(std::string(const_val)));
          } else {
            // Subqueries and function calls evaluate per reference through
            // the relational interpreter instead.
            pending_entry = std::move(expr);
          }
        }
        break;
      }
    }
    if (!pending_entry.has_value()) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: CREATE CONSTANT is missing its value expression");
    }
    std::vector<NamedExpression> proj;
    proj.emplace_back("constant",
                      ConstantValueExp(Value(std::string(const_name))));
    std::string const_key = Lower(const_name);
    return std::make_unique<CreateObjectStatement>(
        StatementType::kCreateConstant,
        [key = std::move(const_key), entry = std::move(pending_entry),
         name = std::move(const_name), val = std::move(const_val)]() mutable {
          if (entry) {
            SessionConstantExpressions()[key] = std::move(*entry);
          }
          SetSessionConstant(name, val);
          return Status::kSuccess;
        },
        std::move(proj));
  }
  if (root.kind == "CreateTableStatement") {
    return VisitCreate(root);
  }
  if (root.kind == "CreateIndexStatement") {
    const auto paths = root.Children("PathExpression");
    const auto* items = root.Child("IndexItemList");
    if (paths.size() != 2 || items == nullptr || items->children.empty() ||
        (!root.detail.empty() && root.detail != "UNIQUE")) {
      return AstError<std::unique_ptr<Statement>>(
          "CREATE INDEX requires an ordinary column index");
    }
    for (const auto& child : root.children) {
      if (child->kind != "PathExpression" && child->kind != "IndexItemList" &&
          child->kind != "IndexStoringExpressionList") {
        return AstError<std::unique_ptr<Statement>>(
            "unsupported CREATE INDEX clause " + child->kind);
      }
    }
    // The pinned parser omits IF NOT EXISTS / OR REPLACE from its dump.
    // Read only the keyword prefix before the name, skipping comments;
    // never silently treat OR REPLACE as plain CREATE.
    const std::string stmt_source = SliceSource(&root);
    if (stmt_source.empty() || paths[0]->start < root.start ||
        paths[0]->start - root.start > stmt_source.size()) {
      return AstError<std::unique_ptr<Statement>>(
          "CREATE INDEX requires its SQL source");
    }
    const std::string header =
        UpperCopy(stmt_source.substr(0, paths[0]->start - root.start));
    std::vector<std::string> words;
    for (size_t i = 0; i < header.size();) {
      if (std::isspace(static_cast<unsigned char>(header[i])) != 0) {
        ++i;
      } else if (header.compare(i, 2, "--") == 0) {
        const size_t end = header.find('\n', i + 2);
        i = end == std::string::npos ? header.size() : end;
      } else if (header.compare(i, 2, "/*") == 0) {
        const size_t end = header.find("*/", i + 2);
        i = end == std::string::npos ? header.size() : end + 2;
      } else {
        const size_t begin = i;
        while (i < header.size() &&
               (std::isalpha(static_cast<unsigned char>(header[i])) != 0)) {
          ++i;
        }
        if (i == begin) {
          return AstError<std::unique_ptr<Statement>>(
              "unsupported CREATE INDEX modifier");
        }
        words.push_back(header.substr(begin, i - begin));
      }
    }
    const bool unique = root.detail == "UNIQUE";
    std::vector<std::string> expected =
        unique ? std::vector<std::string>{"CREATE", "UNIQUE", "INDEX"}
               : std::vector<std::string>{"CREATE", "INDEX"};
    const bool if_not_exists = words.size() == expected.size() + 3;
    if (if_not_exists) {
      expected.insert(expected.end(), {"IF", "NOT", "EXISTS"});
    }
    if (words != expected) {
      return AstError<std::unique_ptr<Statement>>(
          "unsupported CREATE INDEX modifier (OR REPLACE is unavailable)");
    }
    std::vector<std::string> keys, storing;
    keys.reserve(items->children.size());
    const auto column_name =
        [](const GoogleSqlAstNode* path) -> StatusOr<std::string> {
      if (path == nullptr || path->kind != "PathExpression" ||
          path->children.size() != 1 ||
          path->children[0]->kind != "Identifier") {
        return AstError<std::string>(
            "CREATE INDEX supports plain column names only");
      }
      return Path(*path);
    };
    for (const auto& item : items->children) {
      if (item->kind != "OrderingExpression" ||
          item->detail.find("DESC") != std::string::npos ||
          item->children.size() != 1) {
        return AstError<std::unique_ptr<Statement>>(
            "CREATE INDEX supports ascending keys only");
      }
      ASSIGN_OR_RETURN(std::string, key,
                       column_name(item->Child("PathExpression")));
      keys.push_back(std::move(key));
    }
    if (const auto* includes = root.Child("IndexStoringExpressionList")) {
      storing.reserve(includes->children.size());
      for (const auto& item : includes->children) {
        ASSIGN_OR_RETURN(std::string, column, column_name(item.get()));
        storing.push_back(std::move(column));
      }
    }
    ASSIGN_OR_RETURN(std::string, name, Path(*paths[0]));
    ASSIGN_OR_RETURN(std::string, table, Path(*paths[1]));
    return std::make_unique<CreateIndexStatement>(
        std::move(name), std::move(table), std::move(keys), std::move(storing),
        unique, if_not_exists);
  }
  if (root.kind == "CreateFunctionStatement" ||
      root.kind == "CreateTableFunctionStatement") {
    return VisitCreateFunction(root);
  }
  if (root.kind == "CreateViewStatement") {
    // Views register the defining query for FROM-reference expansion right
    // away (same-transaction references resolve), and the IR statement
    // persists the definition text for later sessions.
    const GoogleSqlAstNode* path = root.Child("PathExpression");
    const GoogleSqlAstNode* query = root.Child("Query");
    if (path == nullptr || query == nullptr) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: bad CREATE VIEW");
    }
    ASSIGN_OR_RETURN(std::string, hv242328_0, (Path(*path)));
    const std::string view_name = std::move(hv242328_0);
    // Validate the definition by visiting it; an invalid body must fail the
    // CREATE VIEW statement, not the first later reference.  Registration
    // happens when the statement executes, so a failed or aborted CREATE
    // leaves no session-registry entry behind.
    ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv242329_1,
                     (VisitQuery(*query)));
    static_cast<void>(hv242329_1);
    const bool or_replace =
        root.detail.find("is_or_replace") != std::string::npos;
    const bool if_not_exists =
        root.detail.find("is_if_not_exists") != std::string::npos;
    const bool is_temp = root.detail.find("is_temp") != std::string::npos;
    return std::make_unique<CreateViewStatement>(
        view_name, or_replace, SliceSource(query), if_not_exists, is_temp);
  }
  if (root.kind == "CreateMaterializedViewStatement") {
    // Materialized views are catalog objects: materialize the query into a
    // table and persist the defining text for explicit refresh.  The IF NOT
    // EXISTS marker rides the node detail (the pinned parser emits
    // CreateMaterializedViewStatement(is_if_not_exists) for it).
    const GoogleSqlAstNode* path = root.Child("PathExpression");
    const GoogleSqlAstNode* query = root.Child("Query");
    if (path == nullptr || query == nullptr) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: bad CREATE MATERIALIZED VIEW");
    }
    ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv243001_0,
                     (VisitQuery(*query)));
    ASSIGN_OR_RETURN(std::string, hv243002_0, (Path(*path)));
    const bool if_not_exists =
        root.detail.find("is_if_not_exists") != std::string::npos;
    std::string query_text = SliceSource(query);
    return std::make_unique<CreateMaterializedViewStatement>(
        std::move(hv243002_0), if_not_exists, std::move(hv243001_0),
        std::move(query_text));
  }
  if (root.kind == "DropMaterializedViewStatement") {
    const GoogleSqlAstNode* path = root.Child("PathExpression");
    if (path == nullptr) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: bad DROP MATERIALIZED VIEW");
    }
    ASSIGN_OR_RETURN(std::string, hv243003_0, (Path(*path)));
    const bool drop_mv_if_exists =
        root.detail.find("is_if_exists") != std::string::npos;
    return std::make_unique<DropMaterializedViewStatement>(
        std::move(hv243003_0), drop_mv_if_exists);
  }
  if (root.kind == "InsertStatement") {
    return VisitInsert(root);
  }
  if (root.kind == "UpdateStatement") {
    return VisitUpdate(root);
  }
  if (root.kind == "DeleteStatement") {
    return VisitDelete(root);
  }
  if (root.kind == "MergeStatement") {
    return VisitMerge(root);
  }
  if (root.kind == "AlterTableStatement") {
    return VisitAlterTable(root);
  }
  if (root.kind == "TruncateStatement") {
    // TRUNCATE TABLE removes every row and keeps the table definition
    // (schema, constraints, indexes).  It desugars to an unconditional
    // DELETE so MVCC/WAL semantics stay uniform; a physical truncate
    // shortcut (page-chain reset with a dedicated WAL record) remains a
    // future transaction-layer optimization.
    const GoogleSqlAstNode* path = root.Child("PathExpression");
    if (path == nullptr) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: bad TRUNCATE");
    }
    ASSIGN_OR_RETURN(std::string, truncate_table, Path(*path));
    auto stmt = std::make_unique<DeleteStatement>(
        std::move(truncate_table), ConstantValueExp(Value(true)));
    return stmt;
  }
  if (root.kind == "DropStatement VIEW") {
    const GoogleSqlAstNode* path = root.Child("PathExpression");
    if (path == nullptr) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: bad DROP VIEW");
    }
    ASSIGN_OR_RETURN(std::string, hv245001_0, (Path(*path)));
    const bool drop_view_if_exists =
        root.detail.find("is_if_exists") != std::string::npos;
    return std::make_unique<DropViewStatement>(std::move(hv245001_0),
                                               drop_view_if_exists);
  }
  if (root.kind == "DropStatement" || root.kind == "DropStatement TABLE") {
    const GoogleSqlAstNode* path = root.Child("PathExpression");
    if (path == nullptr) {
      return AstError<std::unique_ptr<Statement>>("GoogleSQL AST: bad DROP");
    }
    ASSIGN_OR_RETURN(std::string, h_tbl, (Path(*path)));
    const bool drop_table_if_exists =
        root.detail.find("is_if_exists") != std::string::npos;
    return std::make_unique<DropTableStatement>(std::move(h_tbl),
                                                drop_table_if_exists);
  }
  if (root.kind.starts_with("DropStatement")) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: unsupported statement " + root.kind);
  }
  return AstError<std::unique_ptr<Statement>>(
      "GoogleSQL AST: unsupported statement " + root.kind);
}

void SetUdfSessionScope(uint64_t session_id) {
  ast_visitor_detail::SetUdfSessionScope(session_id);
}

uint64_t UdfSessionScope() { return ast_visitor_detail::UdfSessionScope(); }

void PurgeUdfSessionDefinitions(uint64_t session_id) {
  ast_visitor_detail::PurgeUdfSessionDefinitions(session_id);
}

void ClearSessionRegistries() {
  // Only the calling scope's definitions: another live session on this
  // thread must keep its own TEMP FUNCTION / VIEW / session constants.
  const uint64_t scope = ast_visitor_detail::UdfSessionScope();
  ast_visitor_detail::PurgeUdfSessionDefinitions(scope);
  AllViewRegistries().erase(scope);
  {
    // Same erase-by-scope for session constants (the public accessor returns
    // the current scope's map).
    auto& constants = SessionConstantExpressions();
    constants.clear();
  }
  t_named_windows.clear();
  t_udf_frames.clear();
  t_udf_bound_masks.clear();
  ClearSessionConstants();
}

}  // namespace tinylamb
