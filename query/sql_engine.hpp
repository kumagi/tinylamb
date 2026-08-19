/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef TINYLAMB_SQL_ENGINE_HPP
#define TINYLAMB_SQL_ENGINE_HPP

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/status_or.hpp"
#include "executor/executor_base.hpp"

namespace tinylamb {

class Database;
class Statement;
class TransactionContext;
enum class StatementType;

class SqlEngine {
 public:
  explicit SqlEngine(Database& database) : database_(&database) {}

  StatusOr<Executor> Prepare(TransactionContext& ctx, std::string_view sql);
  [[nodiscard]] const std::string& LastError() const { return last_error_; }
  [[nodiscard]] const std::optional<StatementType>& LastStatementType() const {
    return last_statement_type_;
  }
  [[nodiscard]] const std::vector<std::string>& ResultColumnNames() const {
    return result_column_names_;
  }

 private:
  StatusOr<Executor> PrepareStatement(TransactionContext& ctx,
                                      std::unique_ptr<Statement> statement);

  Database* database_;
  std::string last_error_;
  std::optional<StatementType> last_statement_type_;
  std::vector<std::string> result_column_names_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_SQL_ENGINE_HPP
