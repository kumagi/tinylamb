/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_SQL_TEMPLATE_HPP
#define TINYLAMB_SQL_TEMPLATE_HPP

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "type/value.hpp"

namespace tinylamb {

class Statement;

struct SqlTemplate {
  std::string fingerprint;
  std::vector<Value> parameters;
  bool templatable{false};
};

SqlTemplate ExtractSqlTemplate(std::string_view sql);

std::unique_ptr<Statement> BindStatementLiterals(
    const Statement& statement, const std::vector<Value>& parameters);

}  // namespace tinylamb

#endif  // TINYLAMB_SQL_TEMPLATE_HPP
