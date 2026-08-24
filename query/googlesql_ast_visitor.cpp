/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/googlesql_ast_visitor.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/unary_expression.hpp"
#include "query/googlesql_ast.hpp"
#include "type/type.hpp"
#include "query/statement.hpp"
#include "type/column.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

std::string Lower(std::string value) {
  std::ranges::transform(
      value, value.begin(),
      [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

// Deeply nested expressions (e.g. "1+1+1+..." chained 100k times) parse fine
// but would overflow the C++ stack during recursive visitation; a stack
// overflow is unrecoverable, so cap the visitation depth explicitly.
constexpr size_t kMaxExpressionDepth = 512;

size_t& ExpressionDepthCounter() {
  static thread_local size_t depth = 0;
  return depth;
}

class ExpressionDepthGuard {
 public:
  ExpressionDepthGuard() {
    size_t& depth = ExpressionDepthCounter();
    if (depth >= kMaxExpressionDepth) {
      throw std::runtime_error("GoogleSQL AST: expression nesting exceeds " +
                               std::to_string(kMaxExpressionDepth));
    }
    ++depth;
  }
  ~ExpressionDepthGuard() { --ExpressionDepthCounter(); }
  ExpressionDepthGuard(const ExpressionDepthGuard&) = delete;
  ExpressionDepthGuard& operator=(const ExpressionDepthGuard&) = delete;
  ExpressionDepthGuard(ExpressionDepthGuard&&) = delete;
  ExpressionDepthGuard& operator=(ExpressionDepthGuard&&) = delete;
};

// Dump-produced literals must be digits-only and in range: std::stoll would
// accept signs and std::stoull would wrap "-1" into a huge positive value.
int64_t ParseIntLiteral(const GoogleSqlAstNode& node) {
  const std::string& text = node.detail;
  const bool digits_only =
      !text.empty() && std::ranges::all_of(text, [](char c) {
        return '0' <= c && c <= '9';
      });
  if (!digits_only) {
    throw std::runtime_error("GoogleSQL AST: malformed integer literal " +
                             text);
  }
  int64_t value = 0;
  const auto* end = text.data() + text.size();
  const auto [ptr, ec] = std::from_chars(text.data(), end, value);
  if (ec != std::errc() || ptr != end) {
    throw std::runtime_error("GoogleSQL AST: integer literal out of range " +
                             text);
  }
  return value;
}

uint64_t ParseUnsignedLiteral(const GoogleSqlAstNode& node) {
  const std::string& text = node.detail;
  const bool digits_only =
      !text.empty() && std::ranges::all_of(text, [](char c) {
        return '0' <= c && c <= '9';
      });
  if (!digits_only) {
    throw std::runtime_error("GoogleSQL AST: malformed unsigned literal " +
                             text);
  }
  uint64_t value = 0;
  const auto* end = text.data() + text.size();
  const auto [ptr, ec] = std::from_chars(text.data(), end, value);
  if (ec != std::errc() || ptr != end) {
    throw std::runtime_error("GoogleSQL AST: unsigned literal out of range " +
                             text);
  }
  return value;
}

double ParseFloatLiteral(const GoogleSqlAstNode& node) {
  try {
    return std::stod(node.detail);
  } catch (const std::exception&) {
    throw std::runtime_error("GoogleSQL AST: float literal out of range " +
                             node.detail);
  }
}

std::string Identifier(const GoogleSqlAstNode& node) {
  if (node.kind != "Identifier") {
    throw std::runtime_error("GoogleSQL AST: expected Identifier");
  }
  std::string value = node.detail;
  if (value.size() >= 2 && value.front() == '`' && value.back() == '`') {
    value = value.substr(1, value.size() - 2);
  }
  return value;
}

std::vector<std::string> PathParts(const GoogleSqlAstNode& path) {
  std::vector<std::string> result;
  for (const auto& child : path.children) {
    if (child->kind == "Identifier") { result.push_back(Identifier(*child));
}
  }
  if (result.empty()) {
    throw std::runtime_error("GoogleSQL AST: empty path expression");
  }
  return result;
}

std::string Path(const GoogleSqlAstNode& path) {
  const std::vector<std::string> parts = PathParts(path);
  std::string result;
  for (const std::string& part : parts) {
    if (!result.empty()) { result += '.';
}
    result += part;
  }
  return result;
}

std::string Alias(const GoogleSqlAstNode& node) {
  const GoogleSqlAstNode* alias = node.Child("Alias");
  if (alias == nullptr || alias->Child("Identifier") == nullptr) {
    return {};
  }
  return Identifier(*alias->Child("Identifier"));
}

std::string DecodeString(const GoogleSqlAstNode& node) {
  const GoogleSqlAstNode* component = node.Child("StringLiteralComponent");
  if (component == nullptr) { component = &node;
}
  std::string value = component->detail;
  if (value.size() >= 2 && value.front() == '\'' && value.back() == '\'') {
    value = value.substr(1, value.size() - 2);
  }
  std::string decoded;
  decoded.reserve(value.size());
  for (size_t i = 0; i < value.size(); ++i) {
    if (value[i] == '\'' && i + 1 < value.size() && value[i + 1] == '\'') {
      decoded.push_back('\'');
      ++i;
    } else {
      decoded.push_back(value[i]);
    }
  }
  return decoded;
}

BinaryOperation BinaryOp(std::string_view detail) {
  if (detail == "+") { return BinaryOperation::kAdd;
}
  if (detail == "-") { return BinaryOperation::kSubtract;
}
  if (detail == "*") { return BinaryOperation::kMultiply;
}
  if (detail == "/") { return BinaryOperation::kDivide;
}
  if (detail == "%") { return BinaryOperation::kModulo;
}
  if (detail == "=") { return BinaryOperation::kEquals;
}
  if (detail == "!=" || detail == "<>") { return BinaryOperation::kNotEquals;
}
  if (detail == "<") { return BinaryOperation::kLessThan;
}
  if (detail == "<=") { return BinaryOperation::kLessThanEquals;
}
  if (detail == ">") { return BinaryOperation::kGreaterThan;
}
  if (detail == ">=") { return BinaryOperation::kGreaterThanEquals;
}
  if (detail == "LIKE") { return BinaryOperation::kLike;
}
  if (detail == "NOT LIKE") { return BinaryOperation::kNotLike;
}
  throw std::runtime_error("GoogleSQL AST: unsupported binary operator " +
                           std::string(detail));
}

std::shared_ptr<SelectStatement> VisitQuery(const GoogleSqlAstNode& query);
Expression VisitExpression(const GoogleSqlAstNode& node);

bool NeedsRelationalEvaluation(const Expression& expression,  // NOLINT(misc-no-recursion) // AST traversal recursion is intentional; expression depth bounded by ExpressionDepthGuard (kMaxExpressionDepth).
                               bool top_level = true) {
  if (!expression) { return false;
}
  switch (expression->Type()) {
    case TypeTag::kQueryExp:
    case TypeTag::kIntervalExp:
      return true;
    case TypeTag::kAggregateExp:
      return !top_level || NeedsRelationalEvaluation(
                               expression->AsAggregateExpression().Child());
    case TypeTag::kBinaryExp:
      // OR used to force the materializing relational executor because the
      // cost-based scan rules could not derive an access path for it.  They
      // now support both a full-scan residual and disjoint composite-prefix
      // index unions, so OR is no longer a complexity boundary.
      return NeedsRelationalEvaluation(expression->AsBinaryExpression().Left(),
                                       false) ||
             NeedsRelationalEvaluation(expression->AsBinaryExpression().Right(),
                                       false);
    case TypeTag::kUnaryExp:
      return NeedsRelationalEvaluation(expression->AsUnaryExpression().Child(),
                                       false);
    case TypeTag::kCaseExp: {
      const auto& value = expression->AsCaseExpression();
      for (const auto& [condition, result] : value.when_clauses_) {
        if (NeedsRelationalEvaluation(condition, false) ||
            NeedsRelationalEvaluation(result, false)) {
          return true;
        }
      }
      return NeedsRelationalEvaluation(value.else_clause_, false);
    }
    case TypeTag::kInExp: {
      const auto& value = expression->AsInExpression();
      if (NeedsRelationalEvaluation(value.child_, false)) { return true;
}
      return std::ranges::any_of(value.list_, [](const Expression& item) {  // NOLINT(misc-no-recursion) // Part of NeedsRelationalEvaluation recursion; depth-guarded by ExpressionDepthGuard.
        return NeedsRelationalEvaluation(item, false);
      });
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      if (call.FuncName() == "date_add" || call.FuncName() == "date_sub" ||
          call.FuncName() == "substr" ||
          call.FuncName().starts_with("extract_")) {
        return true;
      }
      return std::ranges::any_of(call.Args(), [](const Expression& argument) {  // NOLINT(misc-no-recursion) // Part of NeedsRelationalEvaluation recursion; depth-guarded by ExpressionDepthGuard.
        return NeedsRelationalEvaluation(argument, false);
      });
    }
    default:
      return false;
  }
}

Expression FoldBoolean(const GoogleSqlAstNode& node, BinaryOperation op) {  // NOLINT(misc-no-recursion) // AST traversal recursion is intentional; depth bounded by ExpressionDepthGuard in VisitExpression.
  Expression result;
  for (const auto& child : node.children) {
    Expression next = VisitExpression(*child);
    result = result
                 ? BinaryExpressionExp(std::move(result), op, std::move(next))
                 : std::move(next);
  }
  if (!result) { throw std::runtime_error("GoogleSQL AST: empty boolean node");
}
  return result;
}

Expression VisitFunction(const GoogleSqlAstNode& node) {  // NOLINT(misc-no-recursion) // AST traversal recursion is intentional; depth bounded by ExpressionDepthGuard in VisitExpression.
  if (node.children.empty() ||
      node.children.front()->kind != "PathExpression") {
    throw std::runtime_error("GoogleSQL AST: function without name");
  }
  std::string name = Lower(Path(*node.children.front()));
  std::vector<Expression> arguments;
  for (size_t i = 1; i < node.children.size(); ++i) {
    arguments.push_back(VisitExpression(*node.children[i]));
  }
  if (name == "count" || name == "sum" || name == "avg" || name == "min" ||
      name == "max") {
    if (arguments.size() != 1) {
      throw std::runtime_error("GoogleSQL AST: aggregate arity");
    }
    AggregationType type = AggregationType::kCount;
    if (name == "sum") { type = AggregationType::kSum;
}
    if (name == "avg") { type = AggregationType::kAvg;
}
    if (name == "min") { type = AggregationType::kMin;
}
    if (name == "max") { type = AggregationType::kMax;
}
    return AggregateExpressionExp(
        type, std::move(arguments[0]),
        node.detail.find("distinct=true") != std::string::npos);
  }
  return FunctionCallExp(name, std::move(arguments));
}

Expression VisitExpression(const GoogleSqlAstNode& node) {  // NOLINT(misc-no-recursion) // Recursive AST descent by design; stack overflow guarded via ExpressionDepthGuard.
  const ExpressionDepthGuard depth_guard;
  if (node.kind == "PathExpression") { return ColumnValueExp(Path(node));
}
  if (node.kind == "Star") { return ColumnValueExp("*");
}
  if (node.kind == "IntLiteral") {
    return ConstantValueExp(Value(ParseIntLiteral(node)));
  }
  if (node.kind == "FloatLiteral") {
    return ConstantValueExp(Value(ParseFloatLiteral(node)));
  }
  if (node.kind == "StringLiteral") {
    return ConstantValueExp(Value(DecodeString(node)));
  }
  if (node.kind == "DateOrTimeLiteral") {
    const GoogleSqlAstNode* literal = node.Child("StringLiteral");
    if (literal == nullptr) {
      throw std::runtime_error("GoogleSQL AST: invalid date");
    }
    return ConstantValueExp(Value::Date(DecodeString(*literal)));
  }
  if (node.kind == "NullLiteral") { return ConstantValueExp(Value());
}
  if (node.kind == "BooleanLiteral") {
    return ConstantValueExp(Value(node.detail == "TRUE"));
  }
  if (node.kind == "BinaryExpression") {
    if (node.children.size() != 2) {
      throw std::runtime_error("GoogleSQL AST: binary expression arity");
    }
    Expression left = VisitExpression(*node.children[0]);
    Expression right = VisitExpression(*node.children[1]);
    if ((node.detail == "IS" || node.detail == "IS NOT") &&
        node.children[1]->kind == "NullLiteral") {
      return UnaryExpressionExp(
          std::move(left), node.detail == "IS" ? UnaryOperation::kIsNull
                                               : UnaryOperation::kIsNotNull);
    }
    return BinaryExpressionExp(std::move(left), BinaryOp(node.detail),
                               std::move(right));
  }
  if (node.kind == "AndExpr") { return FoldBoolean(node, BinaryOperation::kAnd);
}
  if (node.kind == "OrExpr") { return FoldBoolean(node, BinaryOperation::kOr);
}
  if (node.kind == "UnaryExpression") {
    if (node.children.size() != 1) {
      throw std::runtime_error("GoogleSQL AST: unary expression arity");
    }
    return UnaryExpressionExp(
        VisitExpression(*node.children[0]),
        node.detail == "NOT" ? UnaryOperation::kNot : UnaryOperation::kMinus);
  }
  if (node.kind == "FunctionCall") { return VisitFunction(node);
}
  if (node.kind == "CaseNoValueExpression") {
    if (node.children.empty()) {
      throw std::runtime_error("GoogleSQL AST: empty CASE");
    }
    std::vector<std::pair<Expression, Expression>> clauses;
    size_t pair_end = node.children.size();
    Expression otherwise = ConstantValueExp(Value());
    if (pair_end % 2 == 1) {
      otherwise = VisitExpression(*node.children.back());
      --pair_end;
    }
    for (size_t i = 0; i < pair_end; i += 2) {
      clauses.emplace_back(VisitExpression(*node.children[i]),
                           VisitExpression(*node.children[i + 1]));
    }
    return CaseExpressionExp(std::move(clauses), std::move(otherwise));
  }
  if (node.kind == "BetweenExpression") {
    std::vector<const GoogleSqlAstNode*> operands;
    for (const auto& child : node.children) {
      if (child->kind != "Location") { operands.push_back(child.get());
}
    }
    if (operands.size() != 3) {
      throw std::runtime_error("GoogleSQL AST: BETWEEN arity");
    }
    Expression lower = BinaryExpressionExp(VisitExpression(*operands[0]),
                                           BinaryOperation::kGreaterThanEquals,
                                           VisitExpression(*operands[1]));
    Expression upper = BinaryExpressionExp(VisitExpression(*operands[0]),
                                           BinaryOperation::kLessThanEquals,
                                           VisitExpression(*operands[2]));
    Expression result = BinaryExpressionExp(
        std::move(lower), BinaryOperation::kAnd, std::move(upper));
    if (node.detail == "NOT BETWEEN") {
      result = UnaryExpressionExp(std::move(result), UnaryOperation::kNot);
    }
    return result;
  }
  if (node.kind == "InExpression") {
    if (node.children.empty()) {
      throw std::runtime_error("GoogleSQL AST: empty IN");
    }
    Expression test = VisitExpression(*node.children.front());
    const bool negated = node.detail == "NOT IN";
    if (const GoogleSqlAstNode* list = node.Child("InList")) {
      std::vector<Expression> values;
      values.reserve(list->children.size());
      for (const auto& child : list->children) {
        values.push_back(VisitExpression(*child));
      }
      Expression result = InExpressionExp(std::move(test), std::move(values));
      return negated
                 ? UnaryExpressionExp(std::move(result), UnaryOperation::kNot)
                 : result;
    }
    const GoogleSqlAstNode* query = node.Child("Query");
    if (query == nullptr) {
      throw std::runtime_error("GoogleSQL AST: IN without values");
    }
    return QueryExpressionExp(VisitQuery(*query), std::move(test), false,
                              negated);
  }
  if (node.kind == "ExpressionSubquery") {
    const GoogleSqlAstNode* query = node.Child("Query");
    if (query == nullptr) {
      throw std::runtime_error("GoogleSQL AST: subquery without query");
}
    return QueryExpressionExp(VisitQuery(*query), nullptr,
                              node.detail == "modifier=EXISTS", false);
  }
  if (node.kind == "ExtractExpression") {
    if (node.children.size() != 2) {
      throw std::runtime_error("GoogleSQL AST: EXTRACT arity");
    }
    const std::string part = Lower(Path(*node.children[0]));
    return FunctionCallExp("extract_" + part,
                           {VisitExpression(*node.children[1])});
  }
  if (node.kind == "IntervalExpr") {
    if (node.children.size() < 2 || node.children[0]->kind != "IntLiteral" ||
        node.children[1]->kind != "Identifier") {
      throw std::runtime_error("GoogleSQL AST: unsupported interval");
    }
    return IntervalExpressionExp(ParseIntLiteral(*node.children[0]),
                                 Lower(Identifier(*node.children[1])));
  }
  throw std::runtime_error("GoogleSQL AST: unsupported expression " +
                           node.kind);
}

SelectSource VisitTableSource(const GoogleSqlAstNode& node, JoinType join_type,  // NOLINT(misc-no-recursion) // Recursive AST descent for nested joins/subqueries by design (see VisitQuery depth note).
                              Expression join_condition) {
  SelectSource source;
  source.join_type = join_type;
  source.join_condition = std::move(join_condition);
  source.alias = Alias(node);
  if (node.kind == "TablePathExpression") {
    const GoogleSqlAstNode* path = node.Child("PathExpression");
    if (path == nullptr) {
      throw std::runtime_error("GoogleSQL AST: table without path");
    }
    source.table = Path(*path);
    if (source.alias.empty()) { source.alias = source.table;
}
  } else if (node.kind == "TableSubquery") {
    const GoogleSqlAstNode* query = node.Child("Query");
    if (query == nullptr) {
      throw std::runtime_error("GoogleSQL AST: table subquery missing query");
}
    source.query = VisitQuery(*query);
  } else {
    throw std::runtime_error("GoogleSQL AST: unsupported table source " +
                             node.kind);
  }
  return source;
}

void AppendSources(const GoogleSqlAstNode& node, JoinType incoming,  // NOLINT(misc-no-recursion) // Recursive AST descent for nested joins by design (see VisitQuery depth note).
                   Expression condition, std::vector<SelectSource>* sources) {
  if (node.kind != "Join") {
    sources->push_back(VisitTableSource(node, incoming, std::move(condition)));
    return;
  }
  std::vector<const GoogleSqlAstNode*> operands;
  const GoogleSqlAstNode* on = nullptr;
  const GoogleSqlAstNode* using_clause = nullptr;
  for (const auto& child : node.children) {
    if (child->kind == "TablePathExpression" ||
        child->kind == "TableSubquery" || child->kind == "Join") {
      operands.push_back(child.get());
    } else if (child->kind == "OnClause") {
      on = child.get();
    } else if (using_clause == nullptr &&
                child->kind.starts_with("Using")) {
      using_clause = child.get();
    }
  }
  if (operands.size() != 2) {
    throw std::runtime_error("GoogleSQL AST: join arity");
  }
  AppendSources(*operands[0], incoming, std::move(condition), sources);
  JoinType type = JoinType::kInner;
  if (node.detail == "COMMA") { type = JoinType::kCross;
}
  if (node.detail == "LEFT") { type = JoinType::kLeft;
}
  Expression join_expression;
  if (on != nullptr && !on->children.empty()) {
    join_expression = VisitExpression(*on->children[0]);
  } else if (using_clause != nullptr) {
    // USING(col, ...) carries no OnClause child; it is an equality join over
    // the shared columns. Dropping it silently would turn the statement into
    // a condition-less join.
    for (const GoogleSqlAstNode* column :
         using_clause->Children("Identifier")) {
      Expression equality =
          BinaryExpressionExp(ColumnValueExp(Identifier(*column)),
                              BinaryOperation::kEquals,
                              ColumnValueExp(Identifier(*column)));
      join_expression =
          join_expression
              ? BinaryExpressionExp(std::move(join_expression),
                                    BinaryOperation::kAnd,
                                    std::move(equality))
              : std::move(equality);
    }
    if (!join_expression) {
      throw std::runtime_error("GoogleSQL AST: unsupported join USING clause");
    }
  }
  AppendSources(*operands[1], type, std::move(join_expression), sources);
}

std::shared_ptr<SelectStatement> VisitQuery(const GoogleSqlAstNode& query) {  // NOLINT(misc-no-recursion) // Recursive subquery/CTE traversal by design; SQL nesting is finite and parser-bounded.
  const GoogleSqlAstNode* select = query.Child("Select");
  if (select == nullptr) {
    throw std::runtime_error("GoogleSQL AST: query without SELECT");
  }
  const GoogleSqlAstNode* select_list = select->Child("SelectList");
  if (select_list == nullptr) {
    throw std::runtime_error("GoogleSQL AST: SELECT without list");
  }
  std::vector<NamedExpression> projections;
  for (const GoogleSqlAstNode* column : select_list->Children("SelectColumn")) {
    const GoogleSqlAstNode* expression_node = nullptr;
    for (const auto& child : column->children) {
      if (child->kind != "Alias") {
        expression_node = child.get();
        break;
      }
    }
    if (expression_node == nullptr) {
      throw std::runtime_error("GoogleSQL AST: empty column");
}
    Expression expression = VisitExpression(*expression_node);
    std::string name = Alias(*column);
    if (name.empty() && expression->Type() == TypeTag::kColumnValue) {
      name = expression->AsColumnValue().GetColumnName().name;
    }
    projections.emplace_back(name, std::move(expression));
  }

  std::vector<SelectSource> sources;
  std::vector<std::string> tables;
  if (const GoogleSqlAstNode* from = select->Child("FromClause")) {
    for (const auto& child : from->children) {
      AppendSources(*child, JoinType::kCross, nullptr, &sources);
    }
    for (const SelectSource& source : sources) {
      if (!source.table.empty()) { tables.push_back(source.table);
}
    }
  }

  Expression where;
  if (const GoogleSqlAstNode* clause = select->Child("WhereClause")) {
    if (!clause->children.empty()) {
      where = VisitExpression(*clause->children[0]);
}
  }

  std::vector<SelectStatement::OrderByTerm> order_by;
  if (const GoogleSqlAstNode* order = query.Child("OrderBy")) {
    for (const GoogleSqlAstNode* term : order->Children("OrderingExpression")) {
      if (term->children.empty()) { continue;
}
      order_by.push_back(
          {VisitExpression(*term->children[0]), term->detail != "DESC"});
    }
  }

  std::optional<size_t> limit;
  size_t offset = 0;
  if (const GoogleSqlAstNode* limit_offset = query.Child("LimitOffset")) {
    if (const GoogleSqlAstNode* limit_node = limit_offset->Child("Limit")) {
      if (const GoogleSqlAstNode* value = limit_node->Child("IntLiteral")) {
        limit = static_cast<size_t>(ParseUnsignedLiteral(*value));
      }
    }
    for (const auto& child : limit_offset->children) {
      if (child->kind == "IntLiteral") { offset = ParseUnsignedLiteral(*child);
}
    }
  }

  auto statement = std::make_shared<SelectStatement>(
      std::move(projections), std::move(tables), std::move(where),
      std::move(order_by), limit.value_or(0), offset,
      select->detail.find("distinct=true") != std::string::npos);
  statement->SetLimit(limit);
  statement->SetSources(std::move(sources));

  if (const GoogleSqlAstNode* group = select->Child("GroupBy")) {
    std::vector<Expression> expressions;
    for (const GoogleSqlAstNode* item : group->Children("GroupingItem")) {
      if (!item->children.empty()) {
        expressions.push_back(VisitExpression(*item->children[0]));
      }
    }
    statement->SetGroupBy(std::move(expressions));
  }
  if (const GoogleSqlAstNode* having = select->Child("Having")) {
    if (!having->children.empty()) {
      statement->SetHaving(VisitExpression(*having->children[0]));
    }
  }
  if (const GoogleSqlAstNode* with = query.Child("WithClause")) {
    for (const GoogleSqlAstNode* entry : with->Children("WithClauseEntry")) {
      const GoogleSqlAstNode* aliased = entry->Child("AliasedQuery");
      if (aliased == nullptr) { continue;
}
      const GoogleSqlAstNode* name = aliased->Child("Identifier");
      const GoogleSqlAstNode* nested = aliased->Child("Query");
      if (name != nullptr && nested != nullptr) {
        statement->AddWithQuery(Identifier(*name), VisitQuery(*nested));
      }
    }
  }
  // Phase 8 routing: plain multi-table FROM lists (cross/inner joins, with
  // or without table aliases) stay on the cost-based optimizer path; the
  // engine folds INNER ON conditions into the WHERE conjunction. Only
  // features the optimizer cannot represent yet force the relational
  // executor: FROM-subqueries and outer joins.
  for (const SelectSource& source : statement->Sources()) {
    if (source.query || source.join_type == JoinType::kLeft) {
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
  return statement;
}

ValueType ColumnType(const GoogleSqlAstNode& definition) {
  const GoogleSqlAstNode* schema = definition.Child("SimpleColumnSchema");
  const GoogleSqlAstNode* path =
      schema != nullptr ? schema->Child("PathExpression") : nullptr;
  if (path == nullptr) {
    throw std::runtime_error("GoogleSQL AST: column type missing");
  }
  const std::string type = Lower(Path(*path));
  if (type == "int" || type == "int64" || type == "integer" ||
      type == "bigint" || type == "bool" || type == "boolean") {
    return ValueType::kInt64;
  }
  if (type == "numeric" || type == "decimal" || type == "double" ||
      type == "float" || type == "float64") {
    return ValueType::kDouble;
  }
  if (type == "date") { return ValueType::kDate;
}
  if (type == "string" || type == "varchar" || type == "char" ||
      type == "timestamp" || type == "datetime") {
    return ValueType::kVarChar;
  }
  throw std::runtime_error("GoogleSQL AST: unsupported column type " + type);
}

std::unique_ptr<Statement> VisitCreate(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  const GoogleSqlAstNode* elements = root.Child("TableElementList");
  if (path == nullptr || elements == nullptr) {
    throw std::runtime_error("GoogleSQL AST: bad CREATE");
  }
  std::vector<Column> columns;
  for (const GoogleSqlAstNode* definition :
       elements->Children("ColumnDefinition")) {
    const GoogleSqlAstNode* name = definition->Child("Identifier");
    if (name == nullptr) {
      throw std::runtime_error("GoogleSQL AST: unnamed column");
}
    columns.emplace_back(Identifier(*name), ColumnType(*definition));
  }
  return std::make_unique<CreateTableStatement>(Path(*path),
                                                std::move(columns));
}

std::unique_ptr<Statement> VisitInsert(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  if (path == nullptr) {
    throw std::runtime_error("GoogleSQL AST: INSERT table missing");
  }
  std::vector<std::string> columns;
  if (const GoogleSqlAstNode* list = root.Child("ColumnList")) {
    for (const GoogleSqlAstNode* name : list->Children("Identifier")) {
      columns.push_back(Identifier(*name));
    }
  }
  std::vector<std::vector<Expression>> rows;
  const GoogleSqlAstNode* row_list = root.Child("InsertValuesRowList");
  if (row_list == nullptr) {
    throw std::runtime_error("GoogleSQL AST: INSERT VALUES required");
  }
  for (const GoogleSqlAstNode* row : row_list->Children("InsertValuesRow")) {
    std::vector<Expression> values;
    for (const auto& value : row->children) {
      if (value->kind == "Location" || value->kind == "Hint") { continue;
}
      values.push_back(VisitExpression(*value));
    }
    rows.push_back(std::move(values));
  }
  return std::make_unique<InsertStatement>(Path(*path), std::move(rows),
                                           std::move(columns));
}

std::unique_ptr<Statement> VisitUpdate(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  const GoogleSqlAstNode* items = root.Child("UpdateItemList");
  if (path == nullptr || items == nullptr) {
    throw std::runtime_error("GoogleSQL AST: bad UPDATE");
  }
  std::vector<std::pair<ColumnName, Expression>> assignments;
  for (const GoogleSqlAstNode* item : items->Children("UpdateItem")) {
    const GoogleSqlAstNode* set = item->Child("UpdateSetValue");
    if (set == nullptr || set->children.size() != 2) {
      throw std::runtime_error("GoogleSQL AST: bad UPDATE assignment");
    }
    assignments.emplace_back(ColumnName(Path(*set->children[0])),
                             VisitExpression(*set->children[1]));
  }
  // The WHERE clause is the single remaining child once the target path and
  // the assignment list are removed. Collect candidates instead of taking the
  // last one so unmapped siblings (hints etc.) cannot silently overwrite an
  // earlier WHERE.
  std::vector<const GoogleSqlAstNode*> where_candidates;
  for (const auto& child : root.children) {
    if (child->kind == "PathExpression" || child->kind == "UpdateItemList" ||
        child->kind == "Location" || child->kind == "Hint") {
      continue;
    }
    where_candidates.push_back(child.get());
  }
  if (where_candidates.size() > 1) {
    throw std::runtime_error(
        "GoogleSQL AST: multiple UPDATE WHERE clause candidates");
  }
  Expression where;
  if (!where_candidates.empty()) {
    where = VisitExpression(*where_candidates.front());
  }
  return std::make_unique<UpdateStatement>(Path(*path), std::move(assignments),
                                           std::move(where));
}

std::unique_ptr<Statement> VisitDelete(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  if (path == nullptr) {
    throw std::runtime_error("GoogleSQL AST: bad DELETE");
}
  std::vector<const GoogleSqlAstNode*> where_candidates;
  for (const auto& child : root.children) {
    if (child->kind == "PathExpression" || child->kind == "Location" ||
        child->kind == "Hint") {
      continue;
    }
    where_candidates.push_back(child.get());
  }
  if (where_candidates.size() > 1) {
    throw std::runtime_error(
        "GoogleSQL AST: multiple DELETE WHERE clause candidates");
  }
  Expression where;
  if (!where_candidates.empty()) {
    where = VisitExpression(*where_candidates.front());
  }
  return std::make_unique<DeleteStatement>(Path(*path), std::move(where));
}

}  // namespace

std::unique_ptr<Statement> GoogleSqlAstVisitor::Visit(
    const GoogleSqlAstNode& root) {
  if (root.kind == "QueryStatement") {
    const GoogleSqlAstNode* query = root.Child("Query");
    if (query == nullptr) {
      throw std::runtime_error("GoogleSQL AST: missing query");
}
    auto statement = VisitQuery(*query);
    return std::make_unique<SelectStatement>(*statement);
  }
  if (root.kind == "CreateTableStatement") { return VisitCreate(root);
}
  if (root.kind == "InsertStatement") { return VisitInsert(root);
}
  if (root.kind == "UpdateStatement") { return VisitUpdate(root);
}
  if (root.kind == "DeleteStatement") { return VisitDelete(root);
}
  if (root.kind == "DropStatement" || root.kind == "DropStatement TABLE") {
    const GoogleSqlAstNode* path = root.Child("PathExpression");
    if (path == nullptr) {
      throw std::runtime_error("GoogleSQL AST: bad DROP");
}
    return std::make_unique<DropTableStatement>(Path(*path));
  }
  if (root.kind.starts_with("DropStatement")) {
    throw std::runtime_error("GoogleSQL AST: unsupported statement " +
                             root.kind);
  }
  throw std::runtime_error("GoogleSQL AST: unsupported statement " + root.kind);
}

}  // namespace tinylamb
