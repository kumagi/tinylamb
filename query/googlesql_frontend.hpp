/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef TINYLAMB_GOOGLESQL_FRONTEND_HPP
#define TINYLAMB_GOOGLESQL_FRONTEND_HPP

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

namespace tinylamb {

namespace frontend_detail {
// Returns false without modifying the AST if the parser dump cannot carry
// DISTINCT ON keys. Kept separate so malformed parser responses are testable.
// `select_offset`, when given, is the byte offset of the SELECT keyword that
// owns the DISTINCT ON clause; the keys are then attached to the Select node
// whose dump span starts there (a derived table, CTE body or set-operation
// branch), and the call fails if no such node exists.
bool InjectDistinctOnKeys(std::string& ast, const std::string& keys_ast,
                          std::optional<size_t> select_offset = std::nullopt);
}  // namespace frontend_detail

struct GoogleSqlParseResult {
  bool ok{false};
  std::string ast;
  std::string error;
};

// Runs the pinned GoogleSQL parser and returns its parser AST dump. Tinylamb
// consumes this tree directly and never parses the SQL text a second time.
class GoogleSqlFrontend {
 public:
  [[nodiscard]] static GoogleSqlParseResult Parse(std::string_view sql);
  [[nodiscard]] static bool Available();
};

}  // namespace tinylamb

#endif  // TINYLAMB_GOOGLESQL_FRONTEND_HPP
