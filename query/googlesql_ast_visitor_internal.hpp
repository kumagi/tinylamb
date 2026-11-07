/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_GOOGLESQL_AST_VISITOR_INTERNAL_HPP
#define TINYLAMB_GOOGLESQL_AST_VISITOR_INTERNAL_HPP

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "expression/expression.hpp"
#include "query/googlesql_ast.hpp"
#include "query/statement.hpp"

namespace tinylamb::ast_visitor_detail {

template <typename T>
[[nodiscard]] inline StatusOr<T> AstError(std::string message) {
  return StatusError(StatusCode::kInvalidArgument, std::move(message));
}

inline Status AstStatus(std::string message) {
  return StatusError(StatusCode::kInvalidArgument, std::move(message));
}

inline std::string Lower(std::string value) {
  std::ranges::transform(value, value.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return value;
}

StatusOr<int64_t> ParseIntLiteral(const GoogleSqlAstNode& node);
StatusOr<std::string> Identifier(const GoogleSqlAstNode& node);
StatusOr<std::string> Path(const GoogleSqlAstNode& path);
StatusOr<std::string> Alias(const GoogleSqlAstNode& node);
StatusOr<Expression> VisitExpression(const GoogleSqlAstNode& node);
StatusOr<std::shared_ptr<SelectStatement>> VisitQuery(
    const GoogleSqlAstNode& query);

struct GroupingSetBranchContext {
  // Fingerprints (ToString) of every grouping key of the statement.
  std::vector<std::string> all_key_texts;
  // Fingerprints of the active keys for the branch being rewritten.
  std::unordered_set<std::string> active_key_texts;
};

bool ContainsAggregate(const Expression& expression);
bool ContainsGroupingCall(const Expression& expression);
StatusOr<Expression> RewriteGroupingSetBranch(
    const Expression& expression, const GroupingSetBranchContext& context);
StatusOr<std::shared_ptr<SelectStatement>> ExpandGroupingSetsWithWindows(
    std::shared_ptr<SelectStatement> statement,
    const std::vector<std::vector<Expression>>& pending_grouping_sets);
StatusOr<SelectSource> ExpandPivotSource(SelectSource base,
                                         const GoogleSqlAstNode& pivot);
StatusOr<SelectSource> ExpandUnpivotSource(const SelectSource& base,
                                           const GoogleSqlAstNode& unpivot);

StatusOr<std::unique_ptr<Statement>> VisitCreate(const GoogleSqlAstNode& root);
StatusOr<std::unique_ptr<Statement>> VisitInsert(const GoogleSqlAstNode& root);
StatusOr<std::unique_ptr<Statement>> VisitUpdate(const GoogleSqlAstNode& root);
StatusOr<std::unique_ptr<Statement>> VisitDelete(const GoogleSqlAstNode& root);
StatusOr<std::unique_ptr<Statement>> VisitAlterTable(
    const GoogleSqlAstNode& root);
StatusOr<std::unique_ptr<Statement>> VisitMerge(const GoogleSqlAstNode& root);

}  // namespace tinylamb::ast_visitor_detail

#endif  // TINYLAMB_GOOGLESQL_AST_VISITOR_INTERNAL_HPP
