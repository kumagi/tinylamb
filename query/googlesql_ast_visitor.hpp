/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_GOOGLESQL_AST_VISITOR_HPP
#define TINYLAMB_GOOGLESQL_AST_VISITOR_HPP

#include <memory>

namespace tinylamb {

struct GoogleSqlAstNode;
class Statement;

class GoogleSqlAstVisitor {
 public:
  static std::unique_ptr<Statement> Visit(const GoogleSqlAstNode& root);
};

}  // namespace tinylamb

#endif  // TINYLAMB_GOOGLESQL_AST_VISITOR_HPP
