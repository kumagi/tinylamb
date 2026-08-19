/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_GOOGLESQL_AST_HPP
#define TINYLAMB_GOOGLESQL_AST_HPP

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status_or.hpp"

namespace tinylamb {

struct GoogleSqlAstNode {
  std::string kind;
  std::string detail;
  size_t start{0};
  size_t end{0};
  std::vector<std::unique_ptr<GoogleSqlAstNode>> children;

  [[nodiscard]] const GoogleSqlAstNode* Child(std::string_view child_kind,
                                              size_t occurrence = 0) const;
  [[nodiscard]] std::vector<const GoogleSqlAstNode*> Children(
      std::string_view child_kind) const;
};

class GoogleSqlAstParser {
 public:
  static StatusOr<std::unique_ptr<GoogleSqlAstNode>> Parse(
      std::string_view dump);
};

}  // namespace tinylamb

#endif  // TINYLAMB_GOOGLESQL_AST_HPP
