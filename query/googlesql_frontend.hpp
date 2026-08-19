/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef TINYLAMB_GOOGLESQL_FRONTEND_HPP
#define TINYLAMB_GOOGLESQL_FRONTEND_HPP

#include <string>
#include <string_view>

namespace tinylamb {

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
