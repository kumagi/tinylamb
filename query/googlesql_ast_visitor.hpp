/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_GOOGLESQL_AST_VISITOR_HPP
#define TINYLAMB_GOOGLESQL_AST_VISITOR_HPP

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common/status_or.hpp"
#include "expression/expression.hpp"

namespace tinylamb {

struct GoogleSqlAstNode;
class Statement;

// Normalizes a TIMESTAMP string to UTC ("...+00"), interpreting an explicit
// offset / UTC marker when present and the session default time zone
// otherwise.  Shared by TIMESTAMP literals, typed array elements, and the
// SQL-template binder (which must reproduce the literal normalization when
// re-binding a cached template).  Returns nullopt for literals the visitor
// rejects (named zones, impossible dates, second >= 60) so the template
// scanner can decline to cache them.
std::optional<std::string> NormalizeTimestampText(const std::string& text);

// Decodes the backslash escape sequences inside an already quote-stripped,
// non-raw string literal body.  Shared by the AST visitor and the SQL
// template scanner: the template cache must extract byte-identical literals
// to the first parse, so both consumers decode through this one function
// (a `\'` swallowed only by the parser made cached replays bind the raw
// text and shift every following token).
StatusOr<std::string> DecodeStringEscapes(std::string_view value, bool is_bytes,
                                          bool is_triple, char quote);

// Session-scoped (thread-local) view registry: name -> defining query AST.
// CREATE VIEW registers here immediately; SqlEngine re-registers persisted
// definitions on demand so views survive a restart.  Declared here because
// the engine seeds it in Prepare before the visitor runs.
std::unordered_map<std::string, std::shared_ptr<GoogleSqlAstNode>>&
ViewRegistry();

// Clones `query` into the view registry under `name` (used by the engine to
// re-seed persisted definitions after a reopen; CREATE VIEW registers
// through the visitor directly).
void RegisterViewDefinition(std::string_view name,
                            const GoogleSqlAstNode& query);

// Session scope for CREATE [TEMP] FUNCTION definitions, which live in a
// thread-local registry. Each server connection scopes lookups to its own id,
// so a function never resolves in another session that shares the event-loop
// thread; 0 is the unscoped default of the CLI, the plan cache and the
// single-connection test harness.
void SetUdfSessionScope(uint64_t session_id);
uint64_t UdfSessionScope();
// Drops every definition created by `session_id` (server disconnect).
void PurgeUdfSessionDefinitions(uint64_t session_id);

// Drop the calling thread's frontend registries: the current scope's UDFs,
// plus session constants, views and named windows. The server calls this at
// session boundaries so a later session on the same event-loop thread cannot
// resolve identifiers through the previous session's temporary objects.
void ClearSessionRegistries();

class GoogleSqlAstVisitor {
 public:
  // `source` is the original SQL the dump was produced from.  The dump does
  // not carry per-pair set-operator text, so Visit slices it out of the
  // source via the recorded byte ranges; pass it whenever available.
  static StatusOr<std::unique_ptr<Statement>> Visit(
      const GoogleSqlAstNode& root, std::string_view source = {});
};

// True when an expression needs the relational interpreter's evaluation
// scope (subqueries, window functions, general function calls, nested-field
// paths, ...). Shared by the visitor's Phase 8 routing marks and the
// engine's post-rewrite routing re-check, which must apply the identical
// rule to the rewritten statement.
bool NeedsRelationalEvaluation(const Expression& expression,
                               bool top_level = true);

}  // namespace tinylamb

#endif  // TINYLAMB_GOOGLESQL_AST_VISITOR_HPP
