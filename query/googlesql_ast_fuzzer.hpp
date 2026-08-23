/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_GOOGLESQL_AST_FUZZER_HPP
#define TINYLAMB_GOOGLESQL_AST_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include "common/log_message.hpp"
#include "parser/ast.hpp"
#include "query/googlesql_ast.hpp"
#include "query/googlesql_ast_visitor.hpp"

namespace tinylamb {

// Text-driven fuzzer for the GoogleSQL AST dump consumer.  The dump normally
// comes from an external parser process, so tinylamb must treat it as
// untrusted: GoogleSqlAstParser::Parse rejects malformed indented text with a
// status, and GoogleSqlAstVisitor::Visit walks whatever tree it gets.  The
// visitor reports malformed trees by throwing (sql_engine.cpp catches), so
// exceptions here are error handling - crashes, sanitizer reports, and hangs
// are the findings.
inline void Try(const uint8_t* data, size_t size, bool verbose) {
  const std::string_view dump(reinterpret_cast<const char*>(data), size);
  StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
      GoogleSqlAstParser::Parse(dump);
  if (!ast.HasValue()) {
    return;
  }
  try {
    std::unique_ptr<Statement> statement =
        GoogleSqlAstVisitor::Visit(*ast.Value());
    if (verbose) {
      LOG(TRACE) << "visited root: "
                 << (statement != nullptr ? "ok" : "null");
    }
  } catch (const std::exception&) {
  }
}

}  // namespace tinylamb

#endif  // TINYLAMB_GOOGLESQL_AST_FUZZER_HPP
