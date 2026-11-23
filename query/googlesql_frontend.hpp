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
  bool retryable{false};
};

// Runs the pinned GoogleSQL parser and returns its parser AST dump. Tinylamb
// consumes this tree directly and never parses the SQL text a second time.
// Parses are memoised in a sharded cache whose total capacity is configured at
// runtime by TINYLAMB_PARSE_CACHE_SIZE; the implementation currently uses a
// fixed 16-shard cache.
class GoogleSqlFrontend {
 public:
  [[nodiscard]] static GoogleSqlParseResult Parse(std::string_view sql);
  [[nodiscard]] static bool Available();

  // Test hook: total memoised statements across all cache shards. Lets the
  // eviction test prove the cache stays bounded instead of only checking that
  // a fresh statement still parses.
  [[nodiscard]] static size_t CachedStatementCountForTest();
};

}  // namespace tinylamb

#endif  // TINYLAMB_GOOGLESQL_FRONTEND_HPP
