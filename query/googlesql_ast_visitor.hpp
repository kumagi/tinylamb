/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_GOOGLESQL_AST_VISITOR_HPP
#define TINYLAMB_GOOGLESQL_AST_VISITOR_HPP

#include <memory>

namespace tinylamb {

struct GoogleSqlAstNode;
class Statement;

// Normalizes a TIMESTAMP string to UTC ("...+00"), interpreting an explicit
// offset / UTC marker when present and the session default time zone
// otherwise.  Shared by TIMESTAMP literals, typed array elements, and the
// SQL-template binder (which must reproduce the literal normalization when
// re-binding a cached template).
std::string NormalizeTimestampText(const std::string& text);

class GoogleSqlAstVisitor {
 public:
  // `source` is the original SQL the dump was produced from.  The dump does
  // not carry per-pair set-operator text, so Visit slices it out of the
  // source via the recorded byte ranges; pass it whenever available.
  static std::unique_ptr<Statement> Visit(const GoogleSqlAstNode& root,
                                          std::string_view source = {});
};

}  // namespace tinylamb

#endif  // TINYLAMB_GOOGLESQL_AST_VISITOR_HPP
