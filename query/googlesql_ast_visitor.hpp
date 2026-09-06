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

// Decodes the backslash escape sequences inside an already quote-stripped,
// non-raw string literal body.  Shared by the AST visitor and the SQL
// template scanner: the template cache must extract byte-identical literals
// to the first parse, so both consumers decode through this one function
// (a `\'` swallowed only by the parser made cached replays bind the raw
// text and shift every following token).
std::string DecodeStringEscapes(std::string_view value, bool is_bytes,
                                bool is_triple, char quote);

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
