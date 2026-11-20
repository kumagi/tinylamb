/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_GOOGLESQL_AST_VISITOR_INTERNAL_HPP
#define TINYLAMB_GOOGLESQL_AST_VISITOR_INTERNAL_HPP

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/string_util.hpp"
#include "common/status_or.hpp"
#include "expression/expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/googlesql_ast.hpp"
#include "query/statement.hpp"

namespace tinylamb::ast_visitor_detail {

// Canonical string-literal decoding (quote stripping, '' / \n / \uXXXX
// unescaping and multi-component concatenation).
StatusOr<std::string> DecodeString(const GoogleSqlAstNode& node);

template <typename T>
[[nodiscard]] inline StatusOr<T> AstError(std::string message) {
  return StatusError(StatusCode::kInvalidArgument, std::move(message));
}

inline Status AstStatus(std::string message) {
  return StatusError(StatusCode::kInvalidArgument, std::move(message));
}

inline std::string Lower(std::string value) {
  std::ranges::transform(value, value.begin(),
                         [](char c) { return AsciiToLower(c); });
  return value;
}

inline std::string UpperCopy(std::string text) {
  std::ranges::transform(text, text.begin(),
                         [](char c) { return AsciiToUpper(c); });
  return text;
}

// UDF types and registries
struct SqlUdf {
  std::string name;
  std::vector<std::pair<std::string, bool>>
      parameters;  // (lower name, NOT AGGREGATE)
  std::vector<std::shared_ptr<GoogleSqlAstNode>> default_values;
  bool is_aggregate{false};
  std::shared_ptr<GoogleSqlAstNode> root;  // owns the AST copy
  const GoogleSqlAstNode* body{nullptr};
  // Body analysis: how often each parameter occurs, and whether the body is
  // too complex (subqueries, aggregates) for argument-once binding.
  std::vector<size_t> parameter_counts;
  bool simple_body{true};
  // Connection that created the definition (see SetUdfSessionScope). 0 is the
  // unscoped CLI/test harness.
  uint64_t owner_session{0};
};

// CREATE [TEMP] FUNCTION definitions live in a thread-local registry. Each
// server connection scopes lookups to its own id so a function never resolves
// in another session that happens to share the event-loop thread; 0 is the
// unscoped default used by the CLI and the single-connection test harness.
void SetUdfSessionScope(uint64_t session_id);
uint64_t UdfSessionScope();
// Registers `udf` (lowered name) for the current scope, replacing only that
// scope's definition of the name.
void RegisterUdf(SqlUdf udf);
// Definition visible to the current scope, or nullptr.
SqlUdf* FindUdf(const std::string& name);
// Issue 2553: lowered name of the first scalar-function or TVF reference in
// `root` that resolves to a session-local UDF for the current scope, or
// nullopt.  CREATE VIEW rejects a persistent definition that would capture a
// thread_local UDF (cataloged view, un-cataloged dependency: every other
// session / read worker fails to expand it with "unknown function").
std::optional<std::string> FirstSessionLocalUdfReference(
    const GoogleSqlAstNode& node);
// Drops every definition owned by `session_id` (server disconnect).
void PurgeUdfSessionDefinitions(uint64_t session_id);

std::unordered_map<std::string, Expression>& SessionConstantExpressions();

struct UdfExpansionFrame {
  const SqlUdf* udf;
  const std::vector<Expression>* arguments;
  size_t mask_depth;
};

extern thread_local std::vector<UdfExpansionFrame> t_udf_frames;
extern thread_local std::vector<std::unordered_set<std::string>>
    t_udf_bound_masks;

// Named windows
struct NamedWindowParts {
  std::vector<Expression> partition_by;
  std::vector<WindowOrderTerm> order_by;
  WindowFrameUnit frame_unit{WindowFrameUnit::kDefault};
  WindowFrameBound frame_start;
  WindowFrameBound frame_end;
  bool has_frame{false};
};

extern thread_local std::unordered_map<std::string, NamedWindowParts>
    t_named_windows;
StatusOr<NamedWindowParts> ParseWindowSpecification(
    const GoogleSqlAstNode& spec);
void CollectFromBoundNames(const GoogleSqlAstNode& node,
                           std::unordered_set<std::string>* names);

extern thread_local const std::string* t_visit_source;
void SkipSqlTrivia(std::string_view text, size_t* position);
inline bool IsLateralIdentifierChar(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
         (c >= '0' && c <= '9') || c == '_';
}
inline bool IsAstTrivia(std::string_view kind) { return kind == "Location"; }

// Source slicing
std::string SliceSource(const GoogleSqlAstNode* node);

// Literal and type helpers
StatusOr<int64_t> ParseIntLiteral(const GoogleSqlAstNode& node);
StatusOr<uint64_t> ParseUnsignedLiteral(const GoogleSqlAstNode& node);
StatusOr<std::string> DecodeSingleComponent(std::string_view value_view);

StatusOr<std::string> SqlTypeFromAst(const GoogleSqlAstNode& node);
StatusOr<std::string> Identifier(const GoogleSqlAstNode& node);
StatusOr<std::string> Path(const GoogleSqlAstNode& path);
StatusOr<std::string> Alias(const GoogleSqlAstNode& node);
StatusOr<Expression> BuildNewConstructor(const GoogleSqlAstNode& node);

// Ordering terms
StatusOr<WindowOrderTerm> ParseOrderingTerm(const GoogleSqlAstNode* term);
StatusOr<std::vector<WindowOrderTerm>> ParseOrderingList(
    const GoogleSqlAstNode* list);

// Struct helpers
std::optional<std::vector<std::string>> StructProjectionFields(
    const Expression& expression, size_t depth = 0);

// Core visitors
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
