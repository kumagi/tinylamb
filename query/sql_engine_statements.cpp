/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"
#include "common/platform/process.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/database.hpp"
#include "database/page_storage.hpp"
#include "database/transaction_context.hpp"
#include "executor/constant_executor.hpp"
#include "executor/delete.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/pipeline/pipeline_ir.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/executor_base.hpp"
#include "executor/foreign_key.hpp"
#include "executor/insert.hpp"
#include "executor/relational.hpp"
#include "executor/update.hpp"
#include "expression/binary_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/proto_text.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "page/page_manager.hpp"
#include "plan/optimizer.hpp"
#include "plan/plan.hpp"
#include "query/googlesql_ast.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/googlesql_ast_visitor_internal.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/plan_cache.hpp"
#include "query/sql_engine.hpp"
#include "query/sql_engine_internal.hpp"
#include "query/statement.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

// Materialization arms (CREATE TABLE AS SELECT / INSERT .. SELECT / CREATE
// MATERIALIZED VIEW) report the source query's failure verbatim: the wire
// layer maps kNoSpace to 53100 and kMemoryLimitExceeded to 53200, so
// flattening the code to kUnknown turned a resource exhaustion into a
// syntax error.  The message stays in `last_error_` for the client.
Status MaterializeFailure(const Status& status, std::string* last_error) {
  if (last_error != nullptr) {
    const std::string& message = status.GetMessage();
    last_error->assign(message.empty() ? ToString(status) : message);
  }
  return status;
}

}  // namespace

StatusOr<std::vector<ForeignKeyCheck>> ResolveForeignKeyChecks(
    TransactionContext& ctx, const Table& target) {
  std::vector<ForeignKeyCheck> checks;
  const Schema& schema = target.GetSchema();
  checks.reserve(schema.ColumnCount());
  for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
    const Constraint& constraint = schema.GetColumn(i).GetConstraint();
    if (constraint.ctype != Constraint::kForeign) {
      continue;
    }
    std::string parent_table;
    std::vector<std::string> parent_columns;
    if (!ParseForeignKeyReference(constraint, &parent_table, &parent_columns) ||
        parent_columns.size() != 1) {
      continue;
    }
    auto* parent = const_cast<Table*>(&target);
    if (!IdentifierEquals(parent_table, schema.Name())) {
      ASSIGN_OR_RETURN(std::shared_ptr<Table>, h_parent,
                       (ctx.GetTable(parent_table)));
      // The txn context caches the handle for the statement's lifetime.
      parent = h_parent.get();
      // Keep the cache entry alive even if the caller drops its handle:
      // re-fetching by canonical name returns the same object.
      (void)ctx.GetTable(parent->GetSchema().Name());
    }
    const int parent_offset =
        parent->GetSchema().Offset(ColumnName("", parent_columns[0]));
    if (parent_offset < 0) {
      continue;
    }
    checks.push_back(
        ForeignKeyCheck{.child_offset = i,
                        .parent = parent,
                        .parent_offset = static_cast<size_t>(parent_offset)});
  }
  return checks;
}

StatusOr<std::vector<ReferencedByCheck>> ResolveReferencedByChecks(
    Database* database, TransactionContext& ctx, const Table& target) {
  std::vector<ReferencedByCheck> checks;
  const std::string target_name = std::string(target.GetSchema().Name());
  for (const std::string& name : database->ListTables(ctx)) {
    ASSIGN_OR_RETURN(std::shared_ptr<Table>, h_child, (ctx.GetTable(name)));
    Table* child = h_child.get();
    (void)ctx.GetTable(child->GetSchema().Name());
    const Schema& schema = child->GetSchema();
    for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
      const Constraint& constraint = schema.GetColumn(i).GetConstraint();
      if (constraint.ctype != Constraint::kForeign) {
        continue;
      }
      std::string parent_table;
      std::vector<std::string> parent_columns;
      if (!ParseForeignKeyReference(constraint, &parent_table,
                                    &parent_columns) ||
          parent_columns.size() != 1 ||
          !IdentifierEquals(parent_table, target_name)) {
        continue;
      }
      const int parent_offset =
          target.GetSchema().Offset(ColumnName("", parent_columns[0]));
      if (parent_offset < 0) {
        continue;
      }
      checks.push_back(ReferencedByCheck{
          .child = child,
          .child_offset = i,
          .parent_offset = static_cast<size_t>(parent_offset)});
    }
  }
  return checks;
}

Status ValidateNarrowInteger(const Column& column, const Value& value) {
  if (!column.IntegerValueFits(value)) {
    return {Status::kInvalidArgument,
            "assignment out of range for column " + column.Name().name};
  }
  return Status::kSuccess;
}

namespace {

// Decodes one JSON string token (surrounding quotes included) into a plain
// string.  Handles every escape the engine's canonical JSON validator accepts
// (expression/json_text.hpp): \" \\ \/ \b \f \n \r \t and \uXXXX, the latter
// joining a high/low surrogate pair into its code point and emitting UTF-8.
// Returns false on a malformed escape so the caller can decline the value
// instead of storing the escape's literal text (dropping only the backslash
// is what silently turned "caf\u00e9" into the stored "cafu00e9").
bool JsonUnescapeString(const std::string& token, std::string* out) {
  if (token.size() < 2 || token.front() != '"' || token.back() != '"') {
    return false;
  }
  out->clear();
  out->reserve(token.size());
  const auto hex_digit = [](char h) noexcept -> int {
    if (h >= '0' && h <= '9') {
      return h - '0';
    }
    if (h >= 'a' && h <= 'f') {
      return h - 'a' + 10;
    }
    if (h >= 'A' && h <= 'F') {
      return h - 'A' + 10;
    }
    return -1;
  };
  // Reads four hex digits at `at`; they must be content characters, so the
  // closing quote (the last byte of the token) can never be part of them.
  const auto read_hex4 = [&token, &hex_digit](size_t at,
                                              uint32_t* code) noexcept -> bool {
    if (at + 4 >= token.size()) {
      return false;
    }
    uint32_t value = 0;
    for (size_t k = 0; k < 4; ++k) {
      const int digit = hex_digit(token[at + k]);
      if (digit < 0) {
        return false;
      }
      value = (value << 4) | static_cast<uint32_t>(digit);
    }
    *code = value;
    return true;
  };
  const auto push_utf8 = [&out](uint32_t code_point) {
    if (code_point < 0x80) {
      out->push_back(static_cast<char>(code_point));
      return;
    }
    if (code_point < 0x800) {
      out->push_back(static_cast<char>(0xC0 | (code_point >> 6)));
      out->push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
      return;
    }
    if (code_point < 0x10000) {
      out->push_back(static_cast<char>(0xE0 | (code_point >> 12)));
      out->push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
      out->push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
      return;
    }
    out->push_back(static_cast<char>(0xF0 | (code_point >> 18)));
    out->push_back(static_cast<char>(0x80 | ((code_point >> 12) & 0x3F)));
    out->push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
    out->push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
  };
  for (size_t i = 1; i + 1 < token.size(); ++i) {
    const char c = token[i];
    if (c != '\\') {
      out->push_back(c);
      continue;
    }
    if (i + 1 >= token.size() - 1) {
      return false;
    }
    ++i;
    switch (token[i]) {
      case '"':
      case '\\':
      case '/':
        out->push_back(token[i]);
        break;
      case 'b':
        out->push_back('\b');
        break;
      case 'f':
        out->push_back('\f');
        break;
      case 'n':
        out->push_back('\n');
        break;
      case 'r':
        out->push_back('\r');
        break;
      case 't':
        out->push_back('\t');
        break;
      case 'u': {
        uint32_t code = 0;
        if (!read_hex4(i + 1, &code)) {
          return false;
        }
        i += 4;  // the loop's ++i steps onto the byte after the last digit
        if (code >= 0xD800 && code <= 0xDBFF) {
          // A high surrogate must be followed by its low half, spelled as a
          // second \uXXXX escape exactly like the canonical validator.
          if (i + 1 + 2 >= token.size() || token[i + 1] != '\\' ||
              token[i + 2] != 'u') {
            return false;
          }
          uint32_t low = 0;
          if (!read_hex4(i + 3, &low) || low < 0xDC00 || low > 0xDFFF) {
            return false;
          }
          i += 6;
          push_utf8(0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00));
          break;
        }
        if (code >= 0xDC00 && code <= 0xDFFF) {
          return false;  // lone low surrogate
        }
        push_utf8(code);
        break;
      }
      default:
        return false;
    }
  }
  return true;
}

bool ExpandStructInsertValue(const Value& single, size_t width,
                             std::vector<Value>* out) {
  if (width <= 1) {
    return false;
  }
  if (single.IsNull()) {
    *out = std::vector<Value>(width);
    return true;
  }
  const auto json_value_to_value = [](const std::string& token,
                                      Value* parsed) -> bool {
    const std::string trimmed = [&] {
      size_t b = token.find_first_not_of(" \t\r\n");
      if (b == std::string::npos) {
        return std::string();
      }
      size_t e = token.find_last_not_of(" \t\r\n");
      return token.substr(b, e - b + 1);
    }();
    if (trimmed == "null") {
      *parsed = Value();
      return true;
    }
    if (trimmed == "true") {
      *parsed = Value(int64_t{1});
      return true;
    }
    if (trimmed == "false") {
      *parsed = Value(int64_t{0});
      return true;
    }
    if (trimmed.size() >= 2 && trimmed.front() == '"' &&
        trimmed.back() == '"') {
      std::string unescaped;
      if (!JsonUnescapeString(trimmed, &unescaped)) {
        return false;  // malformed escape: decline rather than store the
                       // escape's literal text
      }
      *parsed = Value(std::move(unescaped));
      return true;
    }
    if (!trimmed.empty()) {
      try {
        size_t consumed = 0;
        const int64_t as_int = std::stoll(trimmed, &consumed);
        if (consumed == trimmed.size()) {
          *parsed = Value(as_int);
          return true;
        }
        const double as_double = std::stod(trimmed, &consumed);
        if (consumed == trimmed.size()) {
          *parsed = Value(as_double);
          return true;
        }
      } catch (const std::exception& error) {
        (void)error;
      }
    }
    // Nested structs / arrays stay as their raw JSON text.
    if (!trimmed.empty() &&
        ((trimmed.front() == '{' && trimmed.back() == '}') ||
         (trimmed.front() == '[' && trimmed.back() == ']'))) {
      *parsed = Value(std::string(trimmed));
      return true;
    }
    return false;
  };

  const auto split_top_level = [](const std::string& body) {
    std::vector<std::string> parts;
    parts.reserve(body.size());
    int depth = 0;
    bool in_str = false;
    char quote = '\0';
    std::string current;
    current.reserve(body.size());
    for (size_t i = 0; i < body.size(); ++i) {
      const char c = body[i];
      if (in_str) {
        current.push_back(c);
        if (c == '\\' && i + 1 < body.size()) {
          current.push_back(body[++i]);
        } else if (c == quote) {
          in_str = false;
        }
        continue;
      }
      if (c == '"' || c == '\'') {
        in_str = true;
        quote = c;
        current.push_back(c);
      } else if (c == '{' || c == '[' || c == '(') {
        ++depth;
        current.push_back(c);
      } else if (c == '}' || c == ']' || c == ')') {
        --depth;
        current.push_back(c);
      } else if (c == ',' && depth == 0) {
        parts.push_back(std::move(current));
        current.clear();
      } else {
        current.push_back(c);
      }
    }
    if (!current.empty()) {
      parts.push_back(std::move(current));
    }
    return parts;
  };

  if (single.type != ValueType::kVarChar) {
    return false;
  }
  const std::string text(single.value.varchar_value);
  if (text.size() < 2 || text.front() != '{' || text.back() != '}') {
    return false;
  }
  const std::vector<std::string> members =
      split_top_level(text.substr(1, text.size() - 2));
  if (members.size() != width) {
    return false;
  }
  std::vector<Value> expanded;
  expanded.reserve(width);
  for (const std::string& member : members) {
    // Each member is `"name":value`; the value starts after the first colon
    // outside quotes / nesting.
    size_t colon = std::string::npos;
    int depth = 0;
    bool in_str = false;
    char quote = '\0';
    for (size_t i = 0; i < member.size(); ++i) {
      const char c = member[i];
      if (in_str) {
        if (c == '\\') {
          ++i;
        } else if (c == quote) {
          in_str = false;
        }
        continue;
      }
      if (c == '"' || c == '\'') {
        in_str = true;
        quote = c;
      } else if (c == '{' || c == '[' || c == '(') {
        ++depth;
      } else if (c == '}' || c == ']' || c == ')') {
        --depth;
      } else if (c == ':' && depth == 0) {
        colon = i;
        break;
      }
    }
    std::string value_text =
        colon == std::string::npos ? member : member.substr(colon + 1);
    if (colon == std::string::npos) {
      // Anonymous positional shape: the member itself is the value.
      value_text = member;
    }
    Value parsed;
    if (!json_value_to_value(value_text, &parsed)) {
      return false;
    }
    expanded.push_back(std::move(parsed));
  }
  *out = std::move(expanded);
  return true;
}

class PlannedExplainExecutor final : public ExecutorBase {
 public:
  PlannedExplainExecutor(Executor inner, Plan plan)
      : plan_(std::move(plan)), inner_(std::move(inner)) {}

  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    return inner_->NextBatch(destination, max_rows);
  }
  void Dump(std::ostream& output, int indent) const override {
    inner_->Dump(output, indent);
  }
  void Explain(std::ostream& output, int indent) const override {
    inner_->Explain(output, indent);
    if (plan_ == nullptr) {
      return;
    }
    const PlanExplainInfo& info = plan_->ExplainInfo();
    if (info.rule_name.empty() && info.physical_operator.empty() &&
        info.required_properties.empty() && info.provided_properties.empty() &&
        info.applied_rules.empty()) {
      return;
    }
    const std::string pad(static_cast<size_t>(std::max(indent, 0)), ' ');
    output << "\n"
           << pad << "Rule: " << info.rule_name << "\n"
           << pad << "Physical operator: " << info.physical_operator << "\n"
           << pad << "Required properties: " << info.required_properties << "\n"
           << pad << "Provided properties: " << info.provided_properties;
    if (!info.applied_rules.empty()) {
      output << "\n" << pad << "Applied rules: " << info.applied_rules;
    }
  }
  Status GetStatus() const override { return inner_->GetStatus(); }
  [[nodiscard]] const Executor& Inner() const noexcept { return inner_; }

 private:
  Plan plan_;
  Executor inner_;
};

}  // namespace

std::vector<ValueType> SchemaColumnTypes(const Schema& schema) {
  std::vector<ValueType> types;
  types.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    types.push_back(schema.GetColumn(i).Type());
  }
  return types;
}

Executor WithPlanExplain(Executor executor, Plan plan,
                         std::vector<ValueType>* output_types) {
  if (executor == nullptr || plan == nullptr) {
    return executor;
  }
  if (output_types != nullptr) {
    // The plan's output schema carries the declared column types; capture
    // them before `plan` moves into the explain wrapper so wire consumers
    // (e.g. pgwire RowDescription) see real types even for empty results.
    *output_types = SchemaColumnTypes(plan->GetSchema());
  }
  return std::make_shared<PlannedExplainExecutor>(std::move(executor),
                                                  std::move(plan));
}

std::optional<ExplainRequest> ParseExplain(std::string_view sql) {
  auto trim = [](std::string_view value) {
    // Skip leading whitespace and SQL comments so a trace header before
    // EXPLAIN does not bypass this Prepare-level string-prefix detection.
    for (;;) {
      while (!value.empty() &&
             std::isspace(static_cast<unsigned char>(value.front()))) {
        value.remove_prefix(1);
      }
      if (value.starts_with("--")) {
        const size_t nl = value.find_first_of("\r\n");
        if (nl == std::string_view::npos) {
          return std::string_view{};
        }
        value.remove_prefix(nl + 1);
        continue;
      }
      if (value.starts_with("/*")) {
        const size_t end = value.find("*/", 2);
        if (end == std::string_view::npos) {
          return std::string_view{};
        }
        value.remove_prefix(end + 2);
        continue;
      }
      break;
    }
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.back()))) {
      value.remove_suffix(1);
    }
    return value;
  };
  auto consume = [&](std::string_view* input, std::string_view keyword) {
    *input = trim(*input);
    if (input->size() < keyword.size()) {
      return false;
    }
    for (size_t i = 0; i < keyword.size(); ++i) {
      if (std::toupper(static_cast<unsigned char>((*input)[i])) != keyword[i]) {
        return false;
      }
    }
    if (input->size() != keyword.size()) {
      const auto next =
          static_cast<unsigned char>((*input)[keyword.size()]);
      // A comment begins a new token just like whitespace or ';', so
      // EXPLAIN/*c*/ANALYZE and EXPLAIN/**/SELECT stay recognisable.
      const bool comment_boundary =
          keyword.size() + 1 < input->size() &&
          ((next == '/' && (*input)[keyword.size() + 1] == '*') ||
           (next == '-' && (*input)[keyword.size() + 1] == '-'));
      if (!std::isspace(next) && next != ';' && !comment_boundary) {
        return false;
      }
    }
    input->remove_prefix(keyword.size());
    return true;
  };

  std::string_view remainder = sql;
  if (!consume(&remainder, "EXPLAIN")) {
    return std::nullopt;
  }
  bool analyze = consume(&remainder, "ANALYZE");
  bool pipeline = consume(&remainder, "PIPELINE");
  if (!analyze && !pipeline) {  // NOLINT(bugprone-branch-clone) reviewed
                                // intentional pattern
    // Also accept the reversed keyword order EXPLAIN PIPELINE ANALYZE.
    analyze = consume(&remainder, "ANALYZE");
  } else if (pipeline && !analyze) {
    analyze = consume(&remainder, "ANALYZE");
  }
  remainder = trim(remainder);
  if (remainder.empty()) {
    return ExplainRequest{
        .analyze = analyze, .pipeline = pipeline, .query = {}};
  }
  return ExplainRequest{
      .analyze = analyze, .pipeline = pipeline, .query = remainder};
}

std::vector<Row> ExplainRows(std::string_view plan) {
  std::vector<Row> rows;
  size_t begin = 0;
  while (begin <= plan.size()) {
    const size_t end = plan.find('\n', begin);
    const std::string_view line = end == std::string_view::npos
                                      ? plan.substr(begin)
                                      : plan.substr(begin, end - begin);
    if (!line.empty()) {
      rows.emplace_back(std::vector<Value>{Value(std::string(line))});
    }
    if (end == std::string_view::npos) {
      break;
    }
    begin = end + 1;
  }
  return rows;
}

std::optional<AnalyzeRequest> ParseAnalyze(std::string_view sql) {
  auto trim = [](std::string_view value) {
    // Skip leading whitespace and SQL comments, mirroring ParseExplain's
    // trim: `ANALYZE/*c*/TABLE t` must recognize both keywords even though
    // consume only removes the keyword bytes themselves.
    for (;;) {
      while (!value.empty() &&
             std::isspace(static_cast<unsigned char>(value.front()))) {
        value.remove_prefix(1);
      }
      if (value.starts_with("--")) {
        const size_t nl = value.find_first_of("\r\n");
        if (nl == std::string_view::npos) {
          return std::string_view{};
        }
        value.remove_prefix(nl + 1);
        continue;
      }
      if (value.starts_with("/*")) {
        const size_t end = value.find("*/", 2);
        if (end == std::string_view::npos) {
          return std::string_view{};
        }
        value.remove_prefix(end + 2);
        continue;
      }
      break;
    }
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.back()))) {
      value.remove_suffix(1);
    }
    return value;
  };
  // Strip trailing comments and every trailing semicolon, not just one: a
  // trace prefix such as `ANALYZE t;;` or `ANALYZE t; --c` must still parse.
  auto strip_tail = [&](std::string_view value) {
    for (;;) {
      value = trim(value);
      // A `--` only opens a trailing line comment when it is outside any
      // quoted identifier (`"t --x"`) and outside a block comment
      // (`/* note -- keep */`). Scan from the front with quote/comment
      // state instead of relying on rfind("--") + preceding whitespace.
      bool in_single = false;
      bool in_double = false;
      bool in_backtick = false;
      bool in_block = false;
      size_t line_comment = std::string_view::npos;
      for (size_t i = 0; i + 1 < value.size(); ++i) {
        const char c = value[i];
        const char next = value[i + 1];
        if (in_block) {
          if (c == '*' && next == '/') {
            in_block = false;
            ++i;
          }
          continue;
        }
        if (in_single || in_double || in_backtick) {
          if (c == '\\') {
            ++i;
            continue;
          }
          if ((in_single && c == '\'') || (in_double && c == '"') ||
              (in_backtick && c == '`')) {
            in_single = in_double = in_backtick = false;
          }
          continue;
        }
        if (c == '\'') {
          in_single = true;
        } else if (c == '"') {
          in_double = true;
        } else if (c == '`') {
          in_backtick = true;
        } else if (c == '/' && next == '*') {
          in_block = true;
          ++i;
        } else if (c == '-' && next == '-') {
          if (line_comment == std::string_view::npos &&
              (i == 0 ||
               std::isspace(static_cast<unsigned char>(value[i - 1])))) {
            line_comment = i;
          }
          // The rest of this line is comment text; skip it.
          while (i + 1 < value.size() && value[i + 1] != '\n') {
            ++i;
          }
        }
      }
      if (line_comment != std::string_view::npos) {
        value.remove_suffix(value.size() - line_comment);
        continue;
      }
      if (value.size() >= 2 && value.back() == '/' &&
          value[value.size() - 2] == '*') {
        const size_t open = value.rfind("/*", value.size() - 2);
        if (open != std::string_view::npos) {
          value.remove_suffix(value.size() - open);
          continue;
        }
      }
      if (!value.empty() && value.back() == ';') {
        value.remove_suffix(1);
        continue;
      }
      break;
    }
    return trim(value);
  };
  auto consume = [&](std::string_view* input, std::string_view keyword) {
    *input = trim(*input);
    if (input->size() < keyword.size()) {
      return false;
    }
    for (size_t i = 0; i < keyword.size(); ++i) {
      if (std::toupper(static_cast<unsigned char>((*input)[i])) != keyword[i]) {
        return false;
      }
    }
    if (input->size() != keyword.size()) {
      const auto next =
          static_cast<unsigned char>((*input)[keyword.size()]);
      // A comment begins a new token just like whitespace or ';', so
      // EXPLAIN/*c*/ANALYZE and EXPLAIN/**/SELECT stay recognisable.
      const bool comment_boundary =
          keyword.size() + 1 < input->size() &&
          ((next == '/' && (*input)[keyword.size() + 1] == '*') ||
           (next == '-' && (*input)[keyword.size() + 1] == '-'));
      if (!std::isspace(next) && next != ';' && !comment_boundary) {
        return false;
      }
    }
    input->remove_prefix(keyword.size());
    return true;
  };

  std::string_view remainder = sql;
  if (!consume(&remainder, "ANALYZE")) {
    return std::nullopt;
  }
  remainder = strip_tail(remainder);
  std::ignore = consume(&remainder, "TABLE");
  remainder = trim(remainder);

  AnalyzeRequest request;
  if (remainder.empty()) {
    return request;
  }

  while (!remainder.empty()) {
    remainder = trim(remainder);
    if (remainder.empty()) {
      break;
    }
    if (std::isalpha(static_cast<unsigned char>(remainder.front())) == 0 &&
        remainder.front() != '_') {
      return std::nullopt;
    }
    size_t length = 1;
    while (length < remainder.size() &&
           (std::isalnum(static_cast<unsigned char>(remainder[length])) != 0 ||
            remainder[length] == '_')) {
      ++length;
    }
    request.tables.emplace_back(remainder.substr(0, length));
    remainder.remove_prefix(length);
    remainder = trim(remainder);
    if (remainder.empty()) {
      break;
    }
    if (remainder.front() != ',') {
      return std::nullopt;
    }
    remainder.remove_prefix(1);
  }
  return request;
}

StatusOr<Executor> ExecuteAnalyze(Database& database, TransactionContext& ctx,
                                  const AnalyzeRequest& request) {
  std::vector<std::string> tables = request.tables;
  if (tables.empty()) {
    tables = database.ListTables(ctx);
  }
  std::vector<Row> rows;
  rows.reserve(tables.size());
  bool any_compacted = false;
  for (const std::string& table : tables) {
    if (database.AutoCompactTableForAnalyze(ctx, table)) {
      any_compacted = true;
    }
    const Status refreshed = database.RefreshStatistics(ctx, table);
    if (refreshed != Status::kSuccess) {
      return refreshed;
    }
    ctx.EraseCachedRelation(table);
    ASSIGN_OR_RETURN(std::shared_ptr<TableStatistics>, stats,
                     ctx.GetStats(table));
    // Compiled-plan validity keys on SchemaEpoch(); without a bump, every
    // cached plan keeps replaying the pre-ANALYZE access path forever.
    database.BumpSchemaEpoch();
    rows.emplace_back(std::vector<Value>{
        Value(std::string("ANALYZE")), Value(std::string(table)),
        Value(static_cast<int64_t>(stats->Rows()))});
  }
  if (any_compacted) {
    RETURN_IF_FAIL(database.WriteCheckpoint().GetStatus());
  }
  return Executor(std::make_shared<ConstantExecutor>(std::move(rows)));
}

std::optional<Executor> ServeCompiledInsert(TransactionContext& ctx,
                                            const CompiledPlan& compiled,
                                            std::vector<Value> parameters) {
  const CompiledPlan::InsertShape& shape = *compiled.insert_shape;
  auto values = std::make_shared<const PreparedValues>(std::move(parameters));
  std::vector<Row> rows;
  rows.reserve(shape.cells.size());
  for (const auto& cells : shape.cells) {
    std::vector<Value> evaluated;
    evaluated.reserve(cells.size());
    for (const Expression& cell : cells) {
      // Slots receive this execution's values; slot-free subtrees are shared
      // immutable nodes, so cloning cost is proportional to literals only.
      StatusOr<Value> value =
          CloneWithPreparedValues(cell, values)->TryEvaluate(Row(), Schema());
      if (!value.HasValue()) {
        // The slow path re-evaluates and surfaces the diagnostic.
        return std::nullopt;
      }
      evaluated.push_back(value.MoveValue());
    }
    if (shape.has_named_columns) {
      // Destination offsets were validated at fill time; replay them.
      std::vector<Value> reordered(shape.schema.ColumnCount());
      // Seed omitted columns with their declared DEFAULT, exactly like the
      // slow INSERT path: only the first execution was applying it, so a
      // replayed statement stored NULL in those columns.
      for (size_t i = 0; i < reordered.size(); ++i) {
        const Column& col = shape.schema.GetColumn(static_cast<slot_t>(i));
        if (col.GetConstraint().ctype == Constraint::kDefault) {
          reordered[i] = col.GetConstraint().value;
        }
      }
      for (size_t i = 0; i < evaluated.size(); ++i) {
        reordered[shape.reorder[i]] = evaluated[i];
      }
      evaluated = std::move(reordered);
    }
    for (size_t i = 0; i < evaluated.size(); ++i) {
      // Same coercion rules as the legacy INSERT path so behavior is
      // identical for every parameter combination.
      const auto& col = shape.schema.GetColumn(i);
      const ValueType expected = col.Type();
      if (ValidateNarrowInteger(col, evaluated[i]) != Status::kSuccess) {
        // The slow path re-validates and surfaces the diagnostic.
        return std::nullopt;
      }
      if (col.IsUnsigned()) {
        if (!evaluated[i].IsNull()) {
          if (evaluated[i].type != ValueType::kInt64) {
            return std::nullopt;
          }
          if (!evaluated[i].IsUnsigned() && evaluated[i].value.int_value < 0) {
            return std::nullopt;
          }
          evaluated[i] = evaluated[i].WithUnsigned();
        }
        continue;
      }
      if (evaluated[i].IsNull() || evaluated[i].type == expected) {
        continue;
      }
      if (expected == ValueType::kDouble &&
          evaluated[i].type == ValueType::kInt64) {
        evaluated[i] =
            evaluated[i].IsUnsigned()
                ? Value(static_cast<double>(
                      static_cast<uint64_t>(evaluated[i].value.int_value)))
                : Value(static_cast<double>(evaluated[i].value.int_value));
        continue;
      }
      if (expected == ValueType::kDate &&
          evaluated[i].type == ValueType::kVarChar) {
        evaluated[i] = Value::Date(evaluated[i].value.varchar_value);
        continue;
      }
      return std::nullopt;  // Legacy path reports the precise error.
    }
    rows.emplace_back(std::move(evaluated));
  }
  StatusOr<std::vector<ForeignKeyCheck>> fk_checks =
      ResolveForeignKeyChecks(ctx, *shape.table);
  if (!fk_checks.HasValue()) {
    return std::nullopt;
  }
  return Executor(std::make_shared<Insert>(
      ctx.txn_, shape.table.get(),
      std::make_shared<ConstantExecutor>(std::move(rows)),
      InsertExecutionMode::kDefault, /*enforce_primary_key=*/false,
      /*upsert_offsets=*/std::vector<size_t>{},
      /*assert_rows_modified=*/-1, fk_checks.MoveValue()));
}

std::vector<Expression> InstallPlanSlots(const Plan& plan,
                                         const std::vector<Value>& parameters) {
  if (!plan || parameters.empty()) {
    return {};
  }
  std::vector<Expression> slots;
  size_t cursor = 0;
  if (!plan->InstallParameterSlots(parameters, &cursor, &slots)) {
    return {};
  }
  std::vector<char> seen(parameters.size(), 0);
  for (const Expression& expression : slots) {
    const auto* slot = dynamic_cast<const ParameterSlot*>(expression.get());
    if (slot == nullptr || slot->Index() >= parameters.size() ||
        slot->GetValue() != parameters[slot->Index()]) {
      return {};
    }
    seen[slot->Index()] = 1;
  }
  if (std::find(seen.begin(), seen.end(), 0) != seen.end()) {
    return {};
  }
  return slots;
}

void RememberSpecializedPlan(const std::string& fingerprint, uint64_t epoch,
                             std::vector<Value> parameters,
                             CompiledPlan::Kind kind, Plan plan,
                             std::shared_ptr<Table> table,
                             const Database* database,
                             const TransactionContext& ctx,
                             std::vector<Expression> slots) {
  if (fingerprint.empty()) {
    return;
  }
  if (!slots.empty() && (plan == nullptr || !plan->ContainsParameterSlots())) {
    slots.clear();
  }
  auto compiled = std::make_shared<CompiledPlan>();
  compiled->kind = kind;
  compiled->database = database;
  compiled->epoch = epoch;
  compiled->opt_bits = CurrentPlanOptToggleBits();
  compiled->parameters = std::move(parameters);
  compiled->parameter_slots = std::move(slots);
  compiled->plan = std::move(plan);
  compiled->table = std::move(table);
  // The plan tree borrows fill-time Tables (and their indexes) whose
  // shared_ptrs live only in ctx.tables_; pin them so cache hits after this
  // transaction ends do not replay freed memory (e.g. subquery scans inside
  // UPDATE/DELETE predicates).
  compiled->retained_tables.reserve(ctx.tables_.size());
  for (const auto& entry : ctx.tables_) {
    compiled->retained_tables.push_back(entry.second);
  }
  // The plan tree borrows fill-time TableStatistics whose shared_ptr lives
  // only in ctx.stats_; pin them so ANALYZE/DROP invalidation cannot leave
  // replayed scans reading freed memory.
  compiled->retained_stats.reserve(ctx.stats_.size());
  for (const auto& entry : ctx.stats_) {
    compiled->retained_stats.push_back(entry.second);
  }
  StoreThreadCompiledPlan(fingerprint, std::move(compiled));
}

namespace {

// Nested per-row array DML (UPDATE ... SET (DELETE/UPDATE/INSERT ...)).
// Each matching row is rewritten in place: the target column holds an ARRAY,
// and every nested item filters / transforms / appends its elements. The
// element variable visible to item predicates is the last component of the
// target path; outer row columns stay reachable through the scope chain so
// correlated references and subqueries keep working.
StatusOr<Executor> ExecuteNestedArrayUpdate(TransactionContext& ctx,
                                            const UpdateStatement& update,
                                            Table* table) {
  const Schema& schema = table->GetSchema();
  struct ResolvedItem {
    size_t offset;
    std::string element_name;
    std::vector<std::string> proto_path;
    const NestedDmlItem* item;
  };
  std::vector<ResolvedItem> resolved;
  resolved.reserve(update.NestedItems().size());
  for (const NestedDmlItem& item : update.NestedItems()) {
    std::vector<std::string> path;
    path.reserve(item.target_path.size() + 1);
    size_t start = 0;
    for (size_t pos = 0; pos <= item.target_path.size(); ++pos) {
      if (pos == item.target_path.size() || item.target_path[pos] == '.') {
        path.push_back(item.target_path.substr(start, pos - start));
        start = pos + 1;
      }
    }
    if (path.empty() || path.front().empty()) {
      return Status(Status::kInvalidArgument,
                    "nested DML target not found: " + item.target_path);
    }
    const int offset = schema.Offset(ColumnName(path.front()));
    if (offset < 0) {
      return Status(Status::kInvalidArgument,
                    "nested DML target not found: " + item.target_path);
    }
    std::vector<std::string> proto_path;
    if (path.size() > 1) {
      proto_path.assign(path.begin() + 1, path.end());
    }
    resolved.push_back(ResolvedItem{.offset = static_cast<size_t>(offset),
                                    .element_name = path.back(),
                                    .proto_path = std::move(proto_path),
                                    .item = &item});
  }

  QueryData query;
  query.from_ = {update.TableName()};
  query.where_ = update.WhereClause() ? update.WhereClause()
                                      : ConstantValueExp(Value(true));
  query.select_.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    query.select_.emplace_back(schema.GetColumn(i).Name().name,
                               ColumnValueExp(schema.GetColumn(i).Name()));
  }
  query.require_row_position_ = true;
  RETURN_IF_FAIL(query.Rewrite(ctx));
  ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
  Executor source = plan->EmitExecutor(ctx);

  int64_t modified_rows = 0;
  Row new_row;
  RowPosition position;
  while (source->Next(&new_row, &position)) {
    // Per-cell array edit state: UPDATE predicates match against the array
    // as of the last DELETE / INSERT (or the original contents), so writes
    // from sibling nested UPDATEs never see one another, while DELETEs take
    // effect immediately in order.
    struct ArrayEditState {
      std::vector<Value> working;
      std::vector<Value> update_baseline;
      // Flags marking elements already rewritten by a nested UPDATE: a
      // second UPDATE touching the same element is a conflict.
      std::vector<bool> update_touched;
      bool loaded{false};
    };
    std::map<size_t, ArrayEditState> edit_state;
    for (const ResolvedItem& entry : resolved) {
      const NestedDmlItem& item = *entry.item;
      Value& cell = new_row.values_[entry.offset];
      const bool proto_target = !entry.proto_path.empty();
      Value array_cell = cell;
      std::string proto_type;
      if (proto_target) {
        if (cell.IsNull() || cell.type != ValueType::kVarChar) {
          return Status(Status::kInvalidArgument,
                        "Cannot execute nested DML on a NULL protocol message");
        }
        proto_type = InferProtoTypeName(
            std::string_view(cell.value.varchar_value), entry.proto_path);
        Value extracted;
        // T3665: read through the FULL path like the SetField side does;
        // passing only .back() extracted a top-level field named after the
        // leaf, so nested repeated fields (msg.a.b) were never found.
        StatusOr<bool> found = TryProtoTextGetFieldPath(
            cell.value.varchar_value, entry.proto_path, &extracted);
        if (!found.HasValue()) {
          return found.GetStatus();
        }
        if (!found.Value() || !extracted.IsArray()) {
          return Status(Status::kInvalidArgument,
                        "nested DML target is not a repeated "
                        "protocol field: " +
                            item.target_path);
        }
        array_cell = std::move(extracted);
      }
      Value& edit_cell = proto_target ? array_cell : cell;
      // The element column's declared type must come from the array's real
      // elements, not a hardcoded INT64: ARRAY<STRING>/<DOUBLE>/<STRUCT> all
      // flowed into an INT64 scope and mis-resolved the element expression.
      ValueType element_type = ValueType::kNull;
      if (!edit_cell.IsNull()) {
        if (edit_cell.IsArray()) {
          for (const Value& element : edit_cell.ArrayElements()) {
            if (!element.IsNull()) {
              element_type = element.type;
              break;
            }
          }
        } else {
          element_type = edit_cell.type;
        }
      }
      const Column element_column(ColumnName("", entry.element_name),
                                  element_type);
      Schema element_schema("", {element_column});
      relational_detail::Scope outer_scope{
          .row = &new_row, .schema = &schema, .outer = nullptr};
      auto eval_on_element = [&](const Expression& expr,
                                 const Value& element) -> StatusOr<Value> {
        if (!expr) {
          return Value(true);
        }
        Row element_row({element});
        relational_detail::Scope element_scope{.row = &element_row,
                                               .schema = &element_schema,
                                               .outer = &outer_scope};
        return relational_detail::TryEvaluate(expr, element_scope, nullptr, ctx,
                                              relational_detail::CteMap{});
      };
      ArrayEditState& state = edit_state[entry.offset];
      auto load_working = [&]() {
        if (!state.loaded) {
          state.working = edit_cell.ArrayElements();
          state.update_baseline = state.working;
          state.update_touched.assign(state.working.size(), false);
          state.loaded = true;
        }
      };
      switch (item.kind) {
        case NestedDmlItem::Kind::kDelete: {
          if (edit_cell.IsNull()) {
            return Status(
                Status::kInvalidArgument,
                "Cannot execute a nested DELETE statement on a NULL array "
                "value");
          }
          if (!edit_cell.IsArray()) {
            return Status(Status::kInvalidArgument,
                          "nested DELETE requires an ARRAY value "
                          "in column " +
                              item.target_path);
          }
          load_working();
          std::vector<Value> kept;
          kept.reserve(state.working.size());
          int64_t touched = 0;
          for (Value& element : state.working) {
            ASSIGN_OR_RETURN(Value, matches,
                             (eval_on_element(item.predicate, element)));
            if (!matches.IsNull() && relational_detail::Truthy(matches)) {
              ++touched;
            } else {
              kept.push_back(std::move(element));
            }
          }
          if (item.assert_rows_modified >= 0 &&
              touched != item.assert_rows_modified) {
            std::ostringstream message;
            message << "ASSERT_ROWS_MODIFIED expected "
                    << item.assert_rows_modified
                    << " array elements modified, but found " << touched;
            return Status(Status::kInvalidArgument, message.str());
          }
          state.working = std::move(kept);
          state.update_baseline = state.working;
          state.update_touched.assign(state.working.size(), false);
          edit_cell =
              Value::Array(state.working, edit_cell.ArrayElementSqlType());
          break;
        }
        case NestedDmlItem::Kind::kUpdate: {
          if (edit_cell.IsNull()) {
            return Status(
                Status::kInvalidArgument,
                "Cannot execute a nested UPDATE statement on a NULL array "
                "value");
          }
          if (!edit_cell.IsArray()) {
            return Status(Status::kInvalidArgument,
                          "nested UPDATE requires an ARRAY value "
                          "in column " +
                              item.target_path);
          }
          load_working();
          int64_t touched = 0;
          for (size_t i = 0; i < state.working.size(); ++i) {
            ASSIGN_OR_RETURN(
                Value, matches,
                (eval_on_element(item.predicate, state.update_baseline[i])));
            if (!matches.IsNull() && relational_detail::Truthy(matches)) {
              if (i < state.update_touched.size() && state.update_touched[i]) {
                return Status(
                    Status::kInvalidArgument,
                    "Attempted to modify an array element with multiple "
                    "nested UPDATE statements");
              }
              ++touched;
              if (i < state.update_touched.size()) {
                state.update_touched[i] = true;
              }
              ASSIGN_OR_RETURN(
                  Value, assigned,
                  (eval_on_element(item.set_value, state.update_baseline[i])));
              state.working[i] = std::move(assigned);
            }
          }
          if (item.assert_rows_modified >= 0 &&
              touched != item.assert_rows_modified) {
            std::ostringstream message;
            message << "ASSERT_ROWS_MODIFIED expected "
                    << item.assert_rows_modified
                    << " array elements modified, but found " << touched;
            return Status(Status::kInvalidArgument, message.str());
          }
          edit_cell =
              Value::Array(state.working, edit_cell.ArrayElementSqlType());
          break;
        }
        case NestedDmlItem::Kind::kInsert: {
          if (edit_cell.IsNull()) {
            return Status(
                Status::kInvalidArgument,
                "Cannot execute a nested INSERT statement on a NULL array "
                "value");
          }
          if (!edit_cell.IsArray()) {
            return Status(Status::kInvalidArgument,
                          "nested INSERT requires an ARRAY value "
                          "in column " +
                              item.target_path);
          }
          load_working();
          int64_t inserted = 0;
          if (item.insert_query != nullptr) {
            StatusOr<relational_detail::Relation> produced_query =
                relational_detail::ExecuteQuery(ctx, *item.insert_query,
                                                nullptr, {});
            if (!produced_query.HasValue()) {
              return Status(
                  Status::kInvalidArgument,
                  std::string(produced_query.GetStatus().GetMessage()));
            }
            relational_detail::Relation produced = produced_query.MoveValue();
            RETURN_IF_FAIL(produced.ForEachRow([&](const Row& row) {
              if (!row.values_.empty()) {
                state.working.push_back(row.values_.front());
                ++inserted;
              }
            }));
          } else {
            for (const auto& values : item.insert_values) {
              if (values.empty()) {
                continue;
              }
              ASSIGN_OR_RETURN(Value, item_value,
                               (relational_detail::TryEvaluate(
                                   values.front(), outer_scope, nullptr, ctx,
                                   relational_detail::CteMap{})));
              state.working.push_back(std::move(item_value));
              ++inserted;
            }
          }
          if (item.assert_rows_modified >= 0 &&
              inserted != item.assert_rows_modified) {
            std::ostringstream message;
            message << "ASSERT_ROWS_MODIFIED expected "
                    << item.assert_rows_modified
                    << " array elements modified, but found " << inserted;
            return Status(Status::kInvalidArgument, message.str());
          }
          state.update_baseline = state.working;
          edit_cell =
              Value::Array(state.working, edit_cell.ArrayElementSqlType());
          break;
        }
      }
      if (proto_target) {
        ASSIGN_OR_RETURN(
            std::optional<std::string>, rewritten,
            TryProtoTextSetField(std::string(cell.value.varchar_value),
                                 entry.proto_path, edit_cell, proto_type));
        if (rewritten.has_value()) {
          cell = Value(std::string(*rewritten));
        }
      }
    }
    StatusOr<RowPosition> updated = table->Update(ctx.txn_, position, new_row);
    if (updated.GetStatus() != Status::kSuccess) {
      // Preserve the storage/MVCC status code so retryable conflicts and
      // no-space errors are distinguishable from schema errors.
      return StatusError(updated.GetStatus().GetCode(),
                         "update failed on table " +
                             std::string(schema.Name()) + ": " +
                             ToString(updated.GetStatus()));
    }
    ++modified_rows;
  }
  // Next()==false reads as EOF, so a scan that failed mid-drain (an overflow
  // inside the WHERE conjuncts, a corrupt page) would otherwise report the
  // rows applied so far as a successful UPDATE.
  RETURN_IF_FAIL(source->GetStatus());
  if (update.HasAssert() && modified_rows != update.AssertRowsModified()) {
    return Status(Status::kInvalidArgument,
                  "ASSERT_ROWS_MODIFIED was specified with " +
                      std::to_string(update.AssertRowsModified()) +
                      " rows, but " + std::to_string(modified_rows) +
                      " rows were modified");
  }
  return Executor(std::make_shared<ConstantExecutor>(
      Row({Value("Update Rows"), Value(modified_rows)})));
}

namespace {

namespace {

std::string EscapeStructKey(std::string_view text) {
  std::string escaped;
  escaped.reserve(text.size() * 2);
  for (const char c : text) {
    switch (c) {
      case '"':
        escaped += "\\\"";
        break;
      case '\\':
        escaped += "\\\\";
        break;
      default:
        escaped.push_back(c);
    }
  }
  return escaped;
}

}  // namespace

// Decodes one struct-member token. Legacy constructor storage embeds nested
// objects/arrays as *quoted* raw JSON (the writer did not escape inner
// quotes); unwrap those so navigation sees real objects.
Value DecodeStructMember(const std::string& text) {
  std::string trimmed = text;
  size_t b = trimmed.find_first_not_of(" \t\r\n");
  if (b == std::string::npos) {
    return {};
  }
  size_t e = trimmed.find_last_not_of(" \t\r\n");
  trimmed = trimmed.substr(b, e - b + 1);
  if (trimmed.size() >= 4 && trimmed.front() == '"' && trimmed.back() == '"') {
    const std::string inner = trimmed.substr(1, trimmed.size() - 2);
    if (!inner.empty() && ((inner.front() == '{' && inner.back() == '}') ||
                           (inner.front() == '[' && inner.back() == ']'))) {
      return Value(std::string(inner));
    }
  }
  Value parsed;
  if (!JsonTextToValue(trimmed, &parsed)) {
    parsed = Value(std::move(trimmed));
  }
  return parsed;
}

// Resolves `segs` member positions inside `json` by name; returns the index
// at every level or an empty optional when the path does not exist here.
std::optional<std::vector<int>> FindStructPathOrdinals(
    const std::string& json, const std::vector<std::string>& segs) {
  std::vector<int> ordinals;
  ordinals.reserve(segs.size());
  std::string current = json;
  for (const std::string& seg : segs) {
    if (current.size() < 2 || current.front() != '{' || current.back() != '}') {
      return std::nullopt;
    }
    const auto members =
        SplitJsonObjectMembers(current.substr(1, current.size() - 2));
    int index = -1;
    size_t ordinal = 0;
    for (const auto& [key, text] : members) {
      if (IdentifierEquals(key, seg)) {
        index = static_cast<int>(ordinal);
        break;
      }
      ++ordinal;
    }
    if (index < 0) {
      return std::nullopt;
    }
    ordinals.push_back(index);
    Value decoded =
        DecodeStructMember(members[static_cast<size_t>(index)].second);
    if (decoded.IsNull()) {
      return std::nullopt;
    }
    if (decoded.type == ValueType::kVarChar &&
        !std::string_view(decoded.value.varchar_value).starts_with("{")) {
      // A STRUCT may contain a protocol message encoded as a TEXT member.
      // The remaining path is resolved by the proto setter rather than by
      // looking for JSON object ordinals.
      ordinals.resize(segs.size(), -1);
      return ordinals;
    }
    current = decoded.type == ValueType::kVarChar
                  ? std::string(decoded.value.varchar_value)
                  : std::string("null");
  }
  return ordinals;
}

// Rewrites `json` by replacing the member addressed by `ordinals` (one index
// per path segment). Missing positions leave the document untouched.
StatusOr<std::optional<std::string>> SetStructPathByOrdinals(
    const std::string& json, const std::vector<std::string>& segs,
    const std::vector<int>& ordinals, size_t depth, const Value& new_value) {
  if (json.size() < 2 || json.front() != '{' || json.back() != '}') {
    // Assigning through a NULL / non-object intermediate.
    return Status(Status::kInvalidArgument, "Cannot set field of NULL STRUCT");
  }
  const auto members = SplitJsonObjectMembers(json.substr(1, json.size() - 2));
  const int index = ordinals[depth];
  if (index < 0 || static_cast<size_t>(index) >= members.size()) {
    return std::nullopt;
  }
  std::string rebuilt = "{";
  bool first = true;
  for (size_t i = 0; i < members.size(); ++i) {
    if (!first) {
      rebuilt += ",";
    }
    first = false;
    rebuilt += "\"";
    rebuilt += EscapeStructKey(members[i].first);
    rebuilt += "\":";
    if (std::cmp_equal(i, index)) {
      if (depth + 1 == segs.size()) {
        rebuilt += EncodeStructMemberJson(new_value);
      } else {
        Value nested_value = DecodeStructMember(members[i].second);
        if (nested_value.type == ValueType::kVarChar &&
            !std::string_view(nested_value.value.varchar_value)
                 .starts_with("{")) {
          std::vector<std::string> proto_path(
              segs.begin() + static_cast<std::ptrdiff_t>(depth + 1),
              segs.end());
          const std::string_view proto_text(nested_value.value.varchar_value);
          const std::string proto_type =
              InferProtoTypeName(proto_text, proto_path);
          ASSIGN_OR_RETURN(std::optional<std::string>, proto,
                           TryProtoTextSetField(proto_text, proto_path,
                                                new_value, proto_type));
          rebuilt += proto.has_value()
                         ? EncodeStructMemberJson(Value(std::string(*proto)))
                         : members[i].second;
        } else {
          ASSIGN_OR_RETURN(
              std::optional<std::string>, nested,
              (SetStructPathByOrdinals(
                  nested_value.IsNull() ? std::string("null")
                  : nested_value.type == ValueType::kVarChar
                      ? std::string(nested_value.value.varchar_value)
                      : std::string("null"),
                  segs, ordinals, depth + 1, new_value)));
          rebuilt += nested.has_value() ? *nested : members[i].second;
        }
      }
    } else {
      rebuilt += members[i].second;
    }
  }
  rebuilt += "}";
  return rebuilt;
}
}  // namespace

// UPDATE with dotted SET targets over STRUCT-typed columns ("SET s.f = ...").
// Struct JSON carries no schema-level field names/positions, so the ordinal
// of each top-level field is resolved against the matched rows themselves;
// rows whose stored keys differ positionally still update correctly.
StatusOr<Executor> ExecuteStructFieldUpdate(TransactionContext& ctx,
                                            const UpdateStatement& update,
                                            Table* table) {
  const Schema& schema = table->GetSchema();
  auto column_index = [&](std::string_view name) -> int {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (IdentifierEquals(schema.GetColumn(i).Name().name, name)) {
        return static_cast<int>(i);
      }
    }
    return -1;
  };
  struct FieldTarget {
    size_t offset;
    std::vector<std::string> segments;
    const Expression* value;
    std::vector<int> ordinals;
  };
  std::vector<FieldTarget> field_targets;
  field_targets.reserve(update.SetClause().size());
  std::vector<std::pair<ColumnName, const Expression*>> plain_targets;
  plain_targets.reserve(update.SetClause().size());
  for (const auto& [target, expression] : update.SetClause()) {
    if (!target.schema.empty() && !target.name.empty()) {
      const int base = column_index(target.schema);
      if (base >= 0) {
        std::vector<std::string> segments;
        segments.reserve(target.name.size() + 1);
        size_t start = 0;
        for (size_t pos = 0; pos <= target.name.size(); ++pos) {
          if (pos == target.name.size() || target.name[pos] == '.') {
            segments.push_back(target.name.substr(start, pos - start));
            start = pos + 1;
          }
        }
        field_targets.push_back(FieldTarget{.offset = static_cast<size_t>(base),
                                            .segments = std::move(segments),
                                            .value = &expression,
                                            .ordinals = {}});
        continue;
      }
    }
    int offset = -1;
    if (!target.name.empty()) {
      offset = column_index(target.name);
    }
    if (offset < 0) {
      return Status(Status::kInvalidArgument,
                    "UPDATE SET target not found: " + target.ToString());
    }
    plain_targets.emplace_back(
        ColumnName(update.TableName(),
                   schema.GetColumn(static_cast<size_t>(offset)).Name().name),
        &expression);
  }
  // A qualifier that names neither the table, the alias, nor a column does
  // not address any target of this table; applying the bare-name match
  // would silently update a different column than the one addressed.
  if (!plain_targets.empty()) {
    return Status(Status::kInvalidArgument,
                  "UPDATE SET target not found: " +
                      plain_targets.front().first.ToString());
  }

  // Dotted references inside the predicate resolve as STRUCT field access
  // against the column named by the first path component.
  Expression where_clause = update.WhereClause();
  if (where_clause) {
    // Alias-qualified references ("x.b" after "UPDATE t AS x") name the
    // scanned table; drop the alias so the struct-field binding below sees
    // the bare column or dotted path.
    const std::string& update_alias = update.Alias();
    if (!update_alias.empty()) {
      std::function<Expression(const Expression&)> bind_update_alias =
          [&](const Expression& expr) -> Expression {
        if (!expr) {
          return expr;
        }
        if (expr->Type() == TypeTag::kColumnValue) {
          const ColumnName& name = expr->AsColumnValue().GetColumnName();
          if (!name.schema.empty() &&
              IdentifierEquals(name.schema, update_alias)) {
            const size_t dot = name.name.find('.');
            if (dot == std::string::npos) {
              return ColumnValueExp(ColumnName(std::string(), name.name));
            }
            return ColumnValueExp(ColumnName(name.name.substr(0, dot),
                                             name.name.substr(dot + 1)));
          }
          return expr;
        }
        std::vector<Expression> children = ExpressionChildren(expr);
        bool changed = false;
        for (Expression& child : children) {
          Expression mapped = bind_update_alias(child);
          changed |= child->ToString() != mapped->ToString();
          child = std::move(mapped);
        }
        return changed ? WithExpressionChildren(expr, std::move(children))
                       : expr;
      };
      where_clause = bind_update_alias(where_clause);
    }
    std::function<Expression(const Expression&)> bind_struct_fields =
        [&](const Expression& expr) -> Expression {
      if (!expr) {
        return expr;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const ColumnName& name = expr->AsColumnValue().GetColumnName();
        if (!name.schema.empty() && name.schema != update.TableName() &&
            !name.name.empty() && name.name != "*") {
          const int base = column_index(name.schema);
          if (base >= 0) {
            return FunctionCallExp(
                "__get_field_safe",
                {ColumnValueExp(ColumnName(
                     update.TableName(),
                     schema.GetColumn(static_cast<size_t>(base)).Name().name)),
                 ConstantValueExp(Value(std::string(name.name)))});
          }
        }
        return expr;
      }
      std::vector<Expression> children = ExpressionChildren(expr);
      bool changed = false;
      for (Expression& child : children) {
        Expression mapped = bind_struct_fields(child);
        changed |= child->ToString() != mapped->ToString();
        child = std::move(mapped);
      }
      return changed ? WithExpressionChildren(expr, std::move(children)) : expr;
    };
    where_clause = bind_struct_fields(where_clause);
  }
  std::vector<std::pair<Row, RowPosition>> pending;
  auto source = table->BeginFullScan(ctx.txn_);
  while (source.IsValid()) {
    Row row = *source;
    relational_detail::Scope scope{
        .row = &row, .schema = &schema, .outer = nullptr};
    bool matches_where = true;
    if (where_clause) {
      ASSIGN_OR_RETURN(
          Value, where_value,
          (relational_detail::TryEvaluate(where_clause, scope, nullptr, ctx,
                                          relational_detail::CteMap{})));
      matches_where = relational_detail::Truthy(where_value);
    }
    if (matches_where) {
      pending.emplace_back(std::move(row), source.Position());
    }
    ++source;
  }

  RETURN_IF_FAIL(source.GetStatus());

  // Resolve each path segment's positional index from any matched row that
  // carries the segment names explicitly (anonymous tuples store fN keys).
  for (auto& target : field_targets) {
    target.ordinals.assign(target.segments.size(), -1);
    for (const auto& [candidate, candidate_position] : pending) {
      const Value& cell = candidate.values_[target.offset];
      if (cell.IsNull() || cell.type != ValueType::kVarChar) {
        continue;
      }
      auto found = FindStructPathOrdinals(std::string(cell.value.varchar_value),
                                          target.segments);
      if (found.has_value()) {
        target.ordinals = *found;
        break;
      }
    }
  }

  int64_t modified_rows = 0;
  for (auto& [new_row, row_position] : pending) {
    // Re-read the physical row before editing it.  The generic executor may
    // reuse its batch-backed VARCHAR storage between calls to Next(); a
    // direct table read gives dotted STRUCT/PROTO updates an owning payload.
    ASSIGN_OR_RETURN(Row, fresh_row, table->Read(ctx.txn_, row_position));
    new_row = std::move(fresh_row);
    for (const auto& target : field_targets) {
      Value& cell = new_row.values_[target.offset];
      // Struct JSON objects start with '{'; proto TEXT payloads never do.
      // Proto cells (and NULLs whose target addresses a modeled proto
      // field) rewrite through the shared dotted-field setter; required-
      // field and enum-member violations surface as errors.
      const bool json_cell = !cell.IsNull() &&
                             cell.type == ValueType::kVarChar &&
                             !cell.value.varchar_value.empty() &&
                             cell.value.varchar_value.front() == '{';
      std::string_view payload =
          !json_cell && !cell.IsNull() && cell.type == ValueType::kVarChar
              ? std::string_view(cell.value.varchar_value)
              : std::string_view();
      const std::string proto_type =
          json_cell ? std::string()
                    : InferProtoTypeName(payload, target.segments);
      const bool proto_cell = !json_cell && !proto_type.empty();
      if (proto_cell) {
        if (cell.IsNull()) {
          return Status(Status::kInvalidArgument,
                        "Cannot set field of NULL `" + proto_type + "`");
        }
        relational_detail::Scope row_scope{
            .row = &new_row, .schema = &schema, .outer = nullptr};
        ASSIGN_OR_RETURN(
            Value, assigned,
            (relational_detail::TryEvaluate(*target.value, row_scope, nullptr,
                                            ctx, relational_detail::CteMap{})));
        RETURN_IF_FAIL(TryValidateEnumFieldValue(
            proto_type,
            target.segments.empty() ? std::string() : target.segments.back(),
            assigned));
        ASSIGN_OR_RETURN(std::optional<std::string>, rewritten,
                         TryProtoTextSetField(payload, target.segments,
                                              assigned, proto_type));
        if (rewritten.has_value()) {
          // Mirror CAST(... AS PROTO): normalize the rewritten TEXT before it
          // is stored, so a wire-decoded payload keeps one canonical spelling
          // instead of being persisted as raw binary in a VARCHAR cell.
          std::string stored = std::move(*rewritten);
          if (auto normalized = NormalizeProtoText(stored);
              normalized.has_value()) {
            stored = std::move(*normalized);
          }
          cell = Value(std::move(stored));
        }
        continue;
      }
      if (cell.IsNull()) {
        return Status(Status::kInvalidArgument,
                      "Cannot set field of NULL STRUCT");
      }
      const std::string text(cell.value.varchar_value);
      relational_detail::Scope row_scope{
          .row = &new_row, .schema = &schema, .outer = nullptr};
      ASSIGN_OR_RETURN(
          Value, assigned,
          (relational_detail::TryEvaluate(*target.value, row_scope, nullptr,
                                          ctx, relational_detail::CteMap{})));
      // Rows may store the same STRUCT members in different orders; resolve
      // the path against this row's own member names first and only fall
      // back to the shared ordinals for rows whose keys are anonymous (fN).
      const std::optional<std::vector<int>> own_ordinals =
          FindStructPathOrdinals(text, target.segments);
      ASSIGN_OR_RETURN(
          std::optional<std::string>, rewritten,
          (SetStructPathByOrdinals(
              text, target.segments,
              own_ordinals.has_value() ? *own_ordinals : target.ordinals, 0,
              assigned)));
      if (rewritten.has_value()) {
        cell = Value(std::move(*rewritten));
      }
    }
    StatusOr<RowPosition> updated =
        table->Update(ctx.txn_, row_position, new_row);
    if (updated.GetStatus() != Status::kSuccess) {
      // Preserve the storage/MVCC status code so retryable conflicts and
      // no-space errors are distinguishable from schema errors.
      return StatusError(updated.GetStatus().GetCode(),
                         "update failed on table " +
                             std::string(schema.Name()) + ": " +
                             ToString(updated.GetStatus()));
    }
    ++modified_rows;
  }
  if (update.HasAssert() && modified_rows != update.AssertRowsModified()) {
    return Status(Status::kInvalidArgument,
                  "ASSERT_ROWS_MODIFIED was specified with " +
                      std::to_string(update.AssertRowsModified()) +
                      " rows, but " + std::to_string(modified_rows) +
                      " rows were modified");
  }
  return Executor(std::make_shared<ConstantExecutor>(
      Row({Value("Update Rows"), Value(modified_rows)})));
}

}  // namespace

StatusOr<Executor> SqlEngine::ExecuteMerge(TransactionContext& ctx,
                                           const MergeStatement& merge) {
  const auto& clauses = merge.Clauses();
  const MergeWhenClause* insert_clause = nullptr;
  const MergeWhenClause* by_source_clause = nullptr;
  for (const MergeWhenClause& clause : clauses) {
    if (clause.match_type == MergeWhenClause::MatchType::kNotMatchedByTarget) {
      if (insert_clause != nullptr) {
        return Status(Status::kInvalidArgument,
                      "MERGE allows at most one WHEN NOT MATCHED [BY TARGET] "
                      "clause");
      }
      insert_clause = &clause;
    } else if (clause.match_type ==
               MergeWhenClause::MatchType::kNotMatchedBySource) {
      if (by_source_clause != nullptr) {
        return Status(Status::kInvalidArgument,
                      "MERGE allows at most one WHEN NOT MATCHED BY SOURCE "
                      "clause");
      }
      by_source_clause = &clause;
    }
  }

  ASSIGN_OR_RETURN(std::shared_ptr<Table>, table,
                   ctx.GetTable(merge.TargetTable()));
  const Schema& schema = table->GetSchema();
  last_dml_table_ = merge.TargetTable();
  const std::string& target_qualifier =
      merge.TargetAlias().empty() ? merge.TargetTable() : merge.TargetAlias();

  // ---- source materialization ------------------------------------------
  ASSIGN_OR_RETURN(
      relational_detail::Relation, source_rel,
      (relational_detail::ExecuteQuery(ctx, *merge.Source(), nullptr, {})));
  std::vector<Row> source_rows;
  RETURN_IF_FAIL(
      source_rel.ForEachRow([&](const Row& row) { source_rows.push_back(row); }));
  const std::string source_alias =
      merge.SourceAlias().empty() ? merge.TargetTable() : merge.SourceAlias();
  const Schema source_schema =
      relational_detail::QualifySchema(source_rel.schema, source_alias);

  auto target_column_index = [&](std::string_view name) -> int {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (IdentifierEquals(schema.GetColumn(i).Name().name, name)) {
        return static_cast<int>(i);
      }
    }
    return -1;
  };
  auto source_column_index = [&](std::string_view name) -> int {
    for (size_t i = 0; i < source_schema.ColumnCount(); ++i) {
      if (IdentifierEquals(source_schema.GetColumn(i).Name().name, name)) {
        return static_cast<int>(i);
      }
    }
    return -1;
  };

  // ---- reference re-binding --------------------------------------------
  // Qualified source references keep their alias (they resolve through the
  // outer Scope during evaluation); target-qualified and bare names bind to
  // the plain target column.  A bare name present in both relations is
  // ambiguous and rejected.
  std::function<StatusOr<Expression>(const Expression&)> rebind =
      [&](const Expression& expr) -> StatusOr<Expression> {
    if (!expr) {
      return expr;
    }
    if (expr->Type() == TypeTag::kColumnValue) {
      const ColumnName& name = expr->AsColumnValue().GetColumnName();
      if (!name.schema.empty()) {
        if (IdentifierEquals(name.schema, source_alias)) {
          return expr;
        }
        if (IdentifierEquals(name.schema, target_qualifier)) {
          return ColumnValueExp(ColumnName("", name.name));
        }
        return expr;  // inner subquery scope; leave untouched
      }
      const int source_index =
          name.name.empty() ? -1 : source_column_index(name.name);
      const int target_index =
          name.name.empty() ? -1 : target_column_index(name.name);
      if (source_index >= 0 && target_index >= 0) {
        return Status(Status::kInvalidArgument,
                      "MERGE condition column " + name.name +
                          " is ambiguous (present in both the target and the "
                          "source)");
      }
      if (source_index >= 0) {
        return ColumnValueExp(ColumnName(source_alias, name.name));
      }
      return expr;
    }
    std::vector<Expression> children = ExpressionChildren(expr);
    bool changed = false;
    for (Expression& child : children) {
      ASSIGN_OR_RETURN(Expression, mapped, rebind(child));
      changed |= child->ToString() != mapped->ToString();
      child = std::move(mapped);
    }
    return changed ? WithExpressionChildren(expr, std::move(children)) : expr;
  };

  Expression on_condition;
  ASSIGN_OR_RETURN(Expression, hv_on_condition, rebind(merge.OnCondition()));
  on_condition = std::move(hv_on_condition);
  std::vector<MergeWhenClause> bound_clauses;
  bound_clauses.reserve(clauses.size());
  for (const MergeWhenClause& clause : clauses) {
    MergeWhenClause bound;
    bound.match_type = clause.match_type;
    if (clause.condition) {
      ASSIGN_OR_RETURN(Expression, bound_condition, rebind(clause.condition));
      bound.condition = std::move(bound_condition);
    }
    bound.action.kind = clause.action.kind;
    bound.action.insert_columns = clause.action.insert_columns;
    for (const auto& [target, value] : clause.action.set_clause) {
      const int target_index = target_column_index(target.name);
      if (target_index < 0) {
        return Status(Status::kNotExists,
                      "MERGE UPDATE SET target not found: " + target.name);
      }
      Expression bound_value;
      ASSIGN_OR_RETURN(Expression, hv_bound_value, rebind(value));
      bound_value = std::move(hv_bound_value);
      bound.action.set_clause.emplace_back(
          ColumnName(
              "",
              schema.GetColumn(static_cast<size_t>(target_index)).Name().name),
          std::move(bound_value));
    }
    for (const Expression& value : clause.action.insert_values) {
      Expression bound_value;
      ASSIGN_OR_RETURN(Expression, hv_insert_value, rebind(value));
      bound_value = std::move(hv_insert_value);
      bound.action.insert_values.push_back(std::move(bound_value));
    }
    bound_clauses.push_back(std::move(bound));
  }

  // ---- equality keys from the ON conjuncts -----------------------------
  std::vector<std::pair<size_t, size_t>> join_keys;  // (target, source)
  {
    std::vector<Expression> conjuncts;
    std::function<void(const Expression&)> collect =
        [&](const Expression& expr) {
          if (expr && expr->Type() == TypeTag::kBinaryExp &&
              expr->AsBinaryExpression().Op() == BinaryOperation::kAnd) {
            collect(expr->AsBinaryExpression().Left());
            collect(expr->AsBinaryExpression().Right());
            return;
          }
          conjuncts.push_back(expr);
        };
    collect(on_condition);
    for (const Expression& conjunct : conjuncts) {
      if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp ||
          conjunct->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
        continue;
      }
      const Expression& left = conjunct->AsBinaryExpression().Left();
      const Expression& right = conjunct->AsBinaryExpression().Right();
      if (left->Type() != TypeTag::kColumnValue ||
          right->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName left_name = left->AsColumnValue().GetColumnName();
      const ColumnName right_name = right->AsColumnValue().GetColumnName();
      const int left_target =
          left_name.schema.empty() ? target_column_index(left_name.name) : -1;
      const int left_source =
          !left_name.schema.empty() &&
                  IdentifierEquals(left_name.schema, source_alias)
              ? source_column_index(left_name.name)
              : -1;
      const int right_target =
          right_name.schema.empty() ? target_column_index(right_name.name) : -1;
      const int right_source =
          !right_name.schema.empty() &&
                  IdentifierEquals(right_name.schema, source_alias)
              ? source_column_index(right_name.name)
              : -1;
      if (left_target >= 0 && right_source >= 0) {
        join_keys.emplace_back(static_cast<size_t>(left_target),
                               static_cast<size_t>(right_source));
      } else if (left_source >= 0 && right_target >= 0) {
        join_keys.emplace_back(static_cast<size_t>(right_target),
                               static_cast<size_t>(left_source));
      }
    }
    // A type-mismatched key makes the hash encoding unequal for values the
    // equality comparison would consider equal; fall back to the linear
    // matcher rather than silently missing matches.
    for (const auto& [target_index, source_index] : join_keys) {
      if (schema.GetColumn(target_index).Type() !=
          source_schema.GetColumn(source_index).Type()) {
        join_keys.clear();
        break;
      }
    }
  }

  std::unordered_map<std::string, std::vector<size_t>> source_index;
  auto composite_key = [](const std::vector<Value>& values) {
    std::string key;
    key.reserve(values.size() * 16);
    for (const Value& value : values) {
      // Use the memcomparable encoding, not AsString(): the latter renders
      // +0.0 and -0.0 differently (MERGE ON d = 0.0 must match -0.0), and
      // text encoding can collide across types. The canonical encoding folds
      // -0/+0, canonicalizes NaN and includes a type tag.
      key += value.EncodeMemcomparableFormat();
    }
    return key;
  };
  const bool use_hash = !join_keys.empty();
  if (use_hash) {
    std::vector<Value> key_values(join_keys.size());
    for (size_t row_index = 0; row_index < source_rows.size(); ++row_index) {
      bool null_key = false;
      for (size_t k = 0; k < join_keys.size(); ++k) {
        const Value& value =
            source_rows[row_index].values_[join_keys[k].second];
        if (value.IsNull()) {
          null_key = true;
        }
        key_values[k] = value;
      }
      if (null_key) {
        continue;  // NULL = NULL is UNKNOWN: never matches
      }
      source_index[composite_key(key_values)].push_back(row_index);
    }
  }

  // ---- classification ---------------------------------------------------
  std::vector<std::pair<RowPosition, Row>> pending_updates;
  std::vector<RowPosition> pending_deletes;
  std::vector<Row> pending_inserts;
  pending_inserts.reserve(source_rows.size());
  std::vector<bool> source_matched(source_rows.size(), false);

  auto eval_truth =
      [&](const Expression& expr,
          const relational_detail::Scope& scope) -> StatusOr<bool> {
    ASSIGN_OR_RETURN(
        Value, value,
        (relational_detail::TryEvaluate(expr, scope, nullptr, ctx,
                                        relational_detail::CteMap{})));
    return relational_detail::Truthy(value);
  };

  auto build_update_row =
      [&](const Row& current, const MergeAction& action,
          const relational_detail::Scope& scope) -> StatusOr<Row> {
    Row updated = current;
    for (const auto& [column, expression] : action.set_clause) {
      const int target_index = target_column_index(column.name);
      if (target_index < 0) {
        return Status(Status::kNotExists,
                      "MERGE UPDATE SET target not found: " + column.name);
      }
      ASSIGN_OR_RETURN(
          Value, value,
          (relational_detail::TryEvaluate(expression, scope, nullptr, ctx,
                                          relational_detail::CteMap{})));
      updated.values_[static_cast<size_t>(target_index)] = std::move(value);
    }
    return updated;
  };

  auto build_insert_row =
      [&](const MergeAction& action,
          const relational_detail::Scope& scope) -> StatusOr<Row> {
    std::vector<Value> values(schema.ColumnCount(), Value());
    std::vector<bool> specified(schema.ColumnCount(), false);
    const auto& columns = action.insert_columns;
    if (!columns.empty() && columns.size() != action.insert_values.size()) {
      return Status(Status::kInvalidArgument,
                    "MERGE INSERT column list and VALUES count differ");
    }
    if (columns.size() > schema.ColumnCount()) {
      return Status(Status::kInvalidArgument,
                    "MERGE INSERT column list exceeds the target schema");
    }
    if (columns.empty() && action.insert_values.size() > schema.ColumnCount()) {
      return Status(Status::kInvalidArgument,
                    "MERGE INSERT value count exceeds the target schema");
    }
    for (size_t i = 0; i < action.insert_values.size(); ++i) {
      size_t offset = i;
      if (!columns.empty()) {
        const int target_index = target_column_index(columns[i]);
        if (target_index < 0) {
          return Status(Status::kNotExists,
                        "MERGE INSERT target column not found: " + columns[i]);
        }
        offset = static_cast<size_t>(target_index);
      }
      ASSIGN_OR_RETURN(Value, value,
                       (relational_detail::TryEvaluate(
                           action.insert_values[i], scope, nullptr, ctx,
                           relational_detail::CteMap{})));
      values[offset] = std::move(value);
      specified[offset] = true;
    }
    // Columns the INSERT clause does not assign take their declared DEFAULT,
    // exactly like the INSERT statement path; columns without one stay NULL.
    for (size_t i = 0; i < values.size(); ++i) {
      if (specified[i]) {
        continue;
      }
      const Column& column = schema.GetColumn(static_cast<slot_t>(i));
      if (column.GetConstraint().ctype == Constraint::kDefault) {
        values[i] = column.GetConstraint().value;
      }
    }
    return Row(std::move(values));
  };

  std::vector<size_t> all_candidates;
  if (!use_hash) {
    all_candidates.reserve(source_rows.size());
    for (size_t i = 0; i < source_rows.size(); ++i) {
      all_candidates.push_back(i);
    }
  }
  auto source = table->BeginFullScan(ctx.txn_);
  while (source.IsValid()) {
    const Row row = *source;
    const RowPosition position = source.Position();
    ++source;
    relational_detail::Scope target_scope{
        .row = &row, .schema = &schema, .outer = nullptr};

    // Candidate source rows for this target row.
    std::span<const size_t> candidates = all_candidates;
    if (use_hash) {
      std::vector<Value> key_values(join_keys.size());
      bool null_key = false;
      for (size_t k = 0; k < join_keys.size(); ++k) {
        const Value& value = row.values_[join_keys[k].first];
        if (value.IsNull()) {
          null_key = true;
        }
        key_values[k] = value;
      }
      if (!null_key) {
        auto found = source_index.find(composite_key(key_values));
        if (found != source_index.end()) {
          candidates = found->second;
        }
      }
    }

    // Matching source rows: ON condition TRUE (source scope chained as the
    // outer of the target scope).
    std::vector<size_t> matches;
    matches.reserve(candidates.size());
    for (size_t row_index : candidates) {
      relational_detail::Scope source_scope{.row = &source_rows[row_index],
                                            .schema = &source_schema,
                                            .outer = nullptr};
      target_scope.outer = &source_scope;
      ASSIGN_OR_RETURN(bool, matched, (eval_truth(on_condition, target_scope)));
      if (matched) {
        matches.push_back(row_index);
      }
    }
    target_scope.outer = nullptr;

    if (matches.empty()) {
      // Candidate for WHEN NOT MATCHED BY SOURCE (target-only scope: source
      // references in its condition surface as evaluation errors).
      if (by_source_clause != nullptr) {
        bool apply = true;
        if (by_source_clause->condition) {
          ASSIGN_OR_RETURN(
              bool, hv_apply,
              (eval_truth(by_source_clause->condition, target_scope)));
          apply = hv_apply;
        }
        if (apply) {
          if (by_source_clause->action.kind == MergeActionKind::kUpdate) {
            ASSIGN_OR_RETURN(Row, updated,
                             (build_update_row(row, by_source_clause->action,
                                               target_scope)));
            pending_updates.emplace_back(position, std::move(updated));
          } else if (by_source_clause->action.kind ==
                     MergeActionKind::kDelete) {
            pending_deletes.push_back(position);
          }
        }
      }
      continue;
    }

    for (size_t row_index : matches) {
      source_matched[row_index] = true;
    }
    // First MATCHED clause whose condition holds for the first matching
    // source row applies; UPDATE/DELETE with several matching source rows is
    // rejected (one action per target row must stay well-defined).
    relational_detail::Scope source_scope{.row = &source_rows[matches[0]],
                                          .schema = &source_schema,
                                          .outer = nullptr};
    target_scope.outer = &source_scope;
    for (const MergeWhenClause& clause : bound_clauses) {
      if (clause.match_type != MergeWhenClause::MatchType::kMatched) {
        continue;
      }
      if (clause.condition) {
        ASSIGN_OR_RETURN(bool, condition,
                         (eval_truth(clause.condition, target_scope)));
        if (!condition) {
          continue;
        }
      }
      if (clause.action.kind == MergeActionKind::kUpdate ||
          clause.action.kind == MergeActionKind::kDelete) {
        if (matches.size() > 1) {
          return Status(
              Status::kInvalidArgument,
              "MERGE attempted to UPDATE or DELETE the same target row more "
              "than once (matched multiple source rows)");
        }
      }
      if (clause.action.kind == MergeActionKind::kUpdate) {
        ASSIGN_OR_RETURN(Row, updated,
                         (build_update_row(row, clause.action, target_scope)));
        pending_updates.emplace_back(position, std::move(updated));
      } else if (clause.action.kind == MergeActionKind::kDelete) {
        pending_deletes.push_back(position);
      }
      break;
    }
    target_scope.outer = nullptr;
  }
  // A failed page read ends the scan like EOF; MERGE must not act on a
  // truncated target (unmatched rows would be inserted as duplicates).
  RETURN_IF_FAIL(source.GetStatus());

  // WHEN NOT MATCHED [BY TARGET]: one source-driven INSERT clause.
  if (insert_clause != nullptr) {
    for (size_t row_index = 0; row_index < source_rows.size(); ++row_index) {
      if (source_matched[row_index]) {
        continue;
      }
      relational_detail::Scope source_scope{.row = &source_rows[row_index],
                                            .schema = &source_schema,
                                            .outer = nullptr};
      if (insert_clause->condition) {
        ASSIGN_OR_RETURN(bool, condition,
                         (eval_truth(insert_clause->condition, source_scope)));
        if (!condition) {
          continue;
        }
      }
      ASSIGN_OR_RETURN(Row, insert_row,
                       (build_insert_row(insert_clause->action, source_scope)));
      pending_inserts.push_back(std::move(insert_row));
    }
  }
  // IsValid()==false can mean EOF or a scan failure; distinguish them before
  // applying anything collected so far, exactly like the UPDATE/DELETE paths.
  RETURN_IF_FAIL(source.GetStatus());

  // ---- application (deletes, then updates, then inserts) ----------------
  ASSIGN_OR_RETURN(std::vector<ReferencedByCheck>, ref_checks,
                   (ResolveReferencedByChecks(database_, ctx, *table)));
  int64_t modified = 0;
  if (!pending_deletes.empty()) {
    for (const ReferencedByCheck& check : ref_checks) {
      ASSIGN_OR_RETURN(
          std::optional<std::string>, referenced,
          FindReferencedParentKey(
              *check.child, ctx.txn_, check.child_offset,
              [&](const std::function<Status(const Value&)>& emit) -> Status {
                for (const RowPosition& position : pending_deletes) {
                  ASSIGN_OR_RETURN(Row, current,
                                   (table->Read(ctx.txn_, position)));
                  if (check.parent_offset < current.values_.size()) {
                    const Value& key = current.values_[check.parent_offset];
                    if (!key.IsNull()) {
                      RETURN_IF_FAIL(emit(key));
                    }
                  }
                }
                return Status::kSuccess;
              },
              // Rows this statement deletes are invisible to the check, the
              // same statement-end semantics Executor::Delete applies
              // (executor/delete.cpp). Without them a self-referencing row
              // that the MERGE removes is still scanned as a live child and
              // rejects the statement with "referenced by" itself, while the
              // identical plain DELETE succeeds.
              [&](const std::function<Status(const RowPosition&)>& emit)
                  -> Status {
                for (const RowPosition& position : pending_deletes) {
                  RETURN_IF_FAIL(emit(position));
                }
                return Status::kSuccess;
              }));
      if (referenced.has_value()) {
        return Status(Status::kConflicts,
                      "MERGE DELETE on table " + std::string(schema.Name()) +
                          " violates foreign key constraint (referenced by " +
                          std::string(check.child->GetSchema().Name()) + ")");
      }
    }
    for (const RowPosition& position : pending_deletes) {
      if (const Status status = table->Delete(ctx.txn_, position);
          status != Status::kSuccess) {
        return Status(Status::kConflicts, "MERGE DELETE failed on table " +
                                              std::string(schema.Name()));
      }
      ++modified;
    }
  }
  ASSIGN_OR_RETURN(std::vector<ForeignKeyCheck>, fk_checks,
                   (ResolveForeignKeyChecks(ctx, *table)));
  std::vector<ForeignKeyCounts> fk_counts;
  fk_counts.reserve(fk_checks.size());
  for (const ForeignKeyCheck& check : fk_checks) {
    ASSIGN_OR_RETURN(
        ForeignKeyCounts, counts,
        (BuildForeignKeyCounts(*check.parent, ctx.txn_, check.parent_offset)));
    fk_counts.push_back(std::move(counts));
  }
  // A self-referencing edge (mgr REFERENCES t(k) on t itself) reads the
  // multiset this statement is changing, so the parent counts pinned at
  // statement start drift out of date the moment the first row lands.
  // Executor::Insert extends the same multiset (executor/insert.cpp
  // note_inserted_row) -- without it a statement inserting (2,1) then
  // (3,2) rejects the forward reference that INSERT INTO t VALUES (2,1),
  // (3,2) accepts -- and an UPDATE that rekeys a parent row drops the old
  // key from the table while the snapshot still counts it. Re-seed the
  // self-referencing multisets between the application phases (deletes →
  // updates → inserts) so every phase reads the table it actually sees.
  // Table handles resolved through the txn cache may be distinct objects
  // for the same table (case-variant lookups), so self-references compare
  // by canonical schema name, not pointer.
  const std::string target_name = std::string(schema.Name());
  const auto is_self_reference = [&target_name](const Table* other) {
    return other != nullptr &&
           std::string(other->GetSchema().Name()) == target_name;
  };
  const auto reseed_self_counts = [&]() -> Status {
    for (size_t c = 0; c < fk_checks.size(); ++c) {
      const ForeignKeyCheck& check = fk_checks[c];
      if (!is_self_reference(check.parent)) {
        continue;
      }
      ASSIGN_OR_RETURN(ForeignKeyCounts, counts,
                       (BuildForeignKeyCounts(*check.parent, ctx.txn_,
                                              check.parent_offset)));
      fk_counts[c] = std::move(counts);
    }
    return Status::kSuccess;
  };
  // Reverse edge, same as Update::Execute (executor/update.cpp): an UPDATE
  // that rekeys a referenced parent row must be rejected while a child row
  // still references its old key. The DELETE clause above already enforces
  // this; without the same check here a MERGE/UPDATE...FROM update leaves
  // dangling children.
  if (!ref_checks.empty() && !pending_updates.empty()) {
    for (const ReferencedByCheck& check : ref_checks) {
      ASSIGN_OR_RETURN(
          std::optional<std::string>, referenced,
          FindReferencedParentKey(
              *check.child, ctx.txn_, check.child_offset,
              [&](const std::function<Status(const Value&)>& emit) -> Status {
                for (const auto& [position, updated] : pending_updates) {
                  ASSIGN_OR_RETURN(Row, current,
                                   (table->Read(ctx.txn_, position)));
                  if (check.parent_offset >= updated.values_.size() ||
                      check.parent_offset >= current.values_.size()) {
                    continue;
                  }
                  const Value& new_key = updated.values_[check.parent_offset];
                  const Value& old_key = current.values_[check.parent_offset];
                  if (new_key == old_key || old_key.IsNull()) {
                    continue;
                  }
                  RETURN_IF_FAIL(emit(old_key));
                }
                return Status::kSuccess;
              },
              [](const std::function<Status(const RowPosition&)>&) -> Status {
                return Status::kSuccess;
              }));
      if (referenced.has_value()) {
        return Status(Status::kConflicts,
                      "MERGE UPDATE on table " + std::string(schema.Name()) +
                          " violates foreign key constraint (referenced by " +
                          std::string(check.child->GetSchema().Name()) + ")");
      }
    }
  }
  for (auto& [position, updated] : pending_updates) {
    for (size_t c = 0; c < fk_checks.size(); ++c) {
      const ForeignKeyCheck& check = fk_checks[c];
      if (check.child_offset >= updated.values_.size()) {
        continue;
      }
      const Value& new_key = updated.values_[check.child_offset];
      ASSIGN_OR_RETURN(bool, viol,
                       (IsForeignKeyViolation(fk_counts[c], new_key)));
      if (viol) {
        return Status(Status::kConflicts,
                      "MERGE UPDATE on table " + std::string(schema.Name()) +
                          " violates foreign key constraint (references " +
                          std::string(check.parent->GetSchema().Name()) + ")");
      }
    }
    if (const StatusOr<RowPosition> new_position =
            table->Update(ctx.txn_, position, updated);
        !new_position.HasValue()) {
      return Status(Status::kConflicts, "MERGE UPDATE failed on table " +
                                            std::string(schema.Name()));
    }
    ++modified;
  }
  // The UPDATE phase may have rekeyed rows of this very table (the
  // self-referencing parent), so the counts the INSERT phase is about to
  // probe were pinned before those writes. Re-seed them from the table as
  // it now stands; without it an INSERT could keep referencing a parent key
  // this statement just removed (and, symmetrically, reject one the UPDATE
  // phase just created). Skipped when nothing changed: the post-delete seed
  // above is already the table's image.
  if (!pending_updates.empty()) {
    RETURN_IF_FAIL(reseed_self_counts());
  }
  for (const Row& insert_row : pending_inserts) {
    for (size_t c = 0; c < fk_checks.size(); ++c) {
      const ForeignKeyCheck& check = fk_checks[c];
      if (check.child_offset >= insert_row.values_.size()) {
        continue;
      }
      const Value& key = insert_row.values_[check.child_offset];
      ASSIGN_OR_RETURN(bool, viol, (IsForeignKeyViolation(fk_counts[c], key)));
      if (viol) {
        return Status(Status::kConflicts,
                      "MERGE INSERT on table " + std::string(schema.Name()) +
                          " violates foreign key constraint (references " +
                          std::string(check.parent->GetSchema().Name()) + ")");
      }
    }
    ASSIGN_OR_RETURN(RowPosition, inserted,
                     (table->Insert(ctx.txn_, insert_row)));
    (void)inserted;
    ++modified;
    // Same as executor/insert.cpp's note_inserted_row: rows this statement
    // inserts extend the self-referencing parent multiset, so a later row of
    // the same statement may reference a key only this one publishes.
    // A non-NULL key only -- NULL keys never match under SQL semantics.
    for (size_t c = 0; c < fk_checks.size(); ++c) {
      const ForeignKeyCheck& check = fk_checks[c];
      if (!is_self_reference(check.parent) ||
          check.parent_offset >= insert_row.values_.size()) {
        continue;
      }
      const Value& key = insert_row.values_[check.parent_offset];
      if (key.IsNull()) {
        continue;
      }
      RETURN_IF_FAIL(fk_counts[c].IncrementKey(key));
    }
  }
  return Executor(std::make_shared<ConstantExecutor>(
      Row({Value(merge.DesugaredFromUpdate() ? "Update Rows" : "Merge Rows"),
           Value(modified)})));
}

// `x op ANY/ALL (SELECT ...)` over a non-correlated subquery: run the inner
// SELECT once at prepare time and fold the comparison into an AND/OR chain
// over the constant candidate list (ANY = OR-chain, ALL = AND-chain; empty
// ANY = FALSE, empty ALL = TRUE; the NOT wrapper is applied last). This is a
// DML-WHERE optimization only. The canonical row loop in
// expression/query_expression.cpp::TryEvaluateQuantifiedComparison additionally
// maps a per-candidate comparison failure to UNKNOWN; shapes where such a
// failure is possible are declined here and keep the legacy row loop.
StatusOr<Expression> SqlEngine::FoldQuantifiedComparison(
    TransactionContext& ctx, const QueryExpression& query) {
  auto inner = std::make_unique<SelectStatement>(*query.Query());
  NestedPrepareState nested(*this);
  StatusOr<Executor> exec = PrepareStatement(ctx, std::move(inner));
  if (!exec.HasValue()) {
    return std::make_shared<QueryExpression>(
        std::make_shared<SelectStatement>(*query.Query()), query.Test(),
        query.Exists(), query.Negated(), query.Op(),
        query.Mode());  // keep legacy route
  }
  std::vector<Value> candidates;
  bool failed = false;
  Row row;
  while (exec.Value()->Next(&row, nullptr)) {
    if (row.values_.empty()) {
      failed = true;
      break;
    }
    candidates.push_back(row[0]);
  }
  if (exec.Value()->GetStatus() != Status::kSuccess) {
    failed = true;
  }
  if (failed) {
    return std::make_shared<QueryExpression>(
        std::make_shared<SelectStatement>(*query.Query()), query.Test(),
        query.Exists(), query.Negated(), query.Op(), query.Mode());
  }
  // The fold's BinaryExpressionExp terms propagate a comparison error
  // instead of turning it into UNKNOWN as the canonical row loop does.
  // Decline the fold for candidate shapes where a comparison can fail:
  // arrays/structs, mixed types, and LIKE with pattern metacharacters.
  std::optional<ValueType> candidate_type;
  for (const Value& candidate : candidates) {
    if (candidate.IsNull()) {
      continue;
    }
    if (candidate.type == ValueType::kArray ||
        (candidate_type.has_value() && *candidate_type != candidate.type)) {
      return std::make_shared<QueryExpression>(
          std::make_shared<SelectStatement>(*query.Query()), query.Test(),
          query.Exists(), query.Negated(), query.Op(), query.Mode());
    }
    candidate_type = candidate.type;
  }
  if (query.Op() == BinaryOperation::kLike ||
      query.Op() == BinaryOperation::kNotLike) {
    for (const Value& candidate : candidates) {
      if (candidate.type != ValueType::kVarChar || candidate.IsNull()) {
        continue;
      }
      const std::string_view pattern = candidate.value.varchar_value;
      if (pattern.find('_') != std::string_view::npos ||
          pattern.find('%') != std::string_view::npos) {
        return std::make_shared<QueryExpression>(
            std::make_shared<SelectStatement>(*query.Query()), query.Test(),
            query.Exists(), query.Negated(), query.Op(), query.Mode());
      }
    }
  }
  dml_fold_applied_ = true;
  if (::tinylamb::platform::GetEnv("TINYLAMB_FOLD_TRACE") != nullptr) {  // NOLINT(cert-err33-c)
    (void)fprintf(stderr, "[fold] quant cands=%zu neg=%d\n", candidates.size(),
                  (int)query.Negated());
  }
  const bool is_all = query.Mode() == QuantifierMode::kAll;
  if (candidates.empty()) {
    const bool value = query.Negated() ? !is_all : is_all;
    return ConstantValueExp(Value(value));
  }
  std::vector<Expression> terms;
  terms.reserve(candidates.size());
  for (const Value& candidate : candidates) {
    terms.push_back(BinaryExpressionExp(query.Test(), query.Op(),
                                        ConstantValueExp(candidate)));
  }
  Expression folded = is_all ? CombineConjuncts(terms)
                             : relational_detail::CombineDisjuncts(terms);
  if (query.Negated()) {
    folded = UnaryExpressionExp(std::move(folded), UnaryOperation::kNot);
  }
  return folded;
}

StatusOr<Expression> SqlEngine::DecorateDmlWhere(TransactionContext& ctx,
                                                 const Expression& expression) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    if (query.Query() && !query.Query()->WithQueries().empty()) {
      return StatusError(
          StatusCode::kNotImplemented,
          "WITH clauses under subquery in DML statement are not supported");
    }
    if (!relational_detail::ContainsOnlyUncorrelatedQueries(ctx, expression,
                                                            {})) {
      return expression;
    }

    if (!query.ArrayResult() && query.Test() &&
        (query.Mode() == QuantifierMode::kAny ||
         query.Mode() == QuantifierMode::kAll)) {
      return FoldQuantifiedComparison(ctx, query);
    }
    if (query.Exists()) {
      // Non-correlated EXISTS: run once, fold to a constant.  A correlated
      // inner cannot be prepared standalone and keeps the legacy route.
      auto inner = std::make_unique<SelectStatement>(*query.Query());
      NestedPrepareState nested(*this);
      StatusOr<Executor> exec = PrepareStatement(ctx, std::move(inner));
      if (exec.HasValue()) {
        Row probe;
        const bool any = exec.Value()->Next(&probe, nullptr);
        if (exec.Value()->GetStatus() != Status::kSuccess) {
          // Preserve the subquery so the checked DML evaluator reports its
          // error. Failure is never proof of an empty EXISTS result.
          return expression;
        }
        dml_fold_applied_ = true;
        return ConstantValueExp(Value(query.Negated() ? !any : any));
      }
      return expression;
    }
    if (!query.Test() && !query.ArrayResult()) {
      // Non-correlated scalar subquery: exactly one row folds to that
      // constant; 0/>1 rows keep the legacy route (which raises the
      // engine's own cardinality error).
      auto inner = std::make_unique<SelectStatement>(*query.Query());
      NestedPrepareState nested(*this);
      StatusOr<Executor> exec = PrepareStatement(ctx, std::move(inner));
      if (exec.HasValue()) {
        std::vector<Value> picked;
        bool failed = false;
        Row row;
        while (exec.Value()->Next(&row, nullptr)) {
          if (row.values_.empty() || picked.size() >= 2) {
            failed = true;
            break;
          }
          picked.push_back(row[0]);
        }
        if (!failed && exec.Value()->GetStatus() == Status::kSuccess &&
            picked.size() == 1) {
          dml_fold_applied_ = true;
          return ConstantValueExp(picked[0]);
        }
      }
      return expression;
    }
    if (!query.Exists() && !query.ArrayResult() && query.Test() &&
        query.Mode() == QuantifierMode::kIn &&
        query.Op() == BinaryOperation::kEquals) {
      auto inner = std::make_unique<SelectStatement>(*query.Query());
      // The nested prepare must not steal the OUTER statement's armed
      // plan-cache candidate (the fill sites read the shared member), and
      // the fold itself must never populate the cache.
      NestedPrepareState nested(*this);
      StatusOr<Executor> exec = PrepareStatement(ctx, std::move(inner));
      if (exec.HasValue()) {
        std::vector<Expression> values;
        bool failed = false;
        Row row;
        while (exec.Value()->Next(&row, nullptr)) {
          if (row.values_.empty()) {
            failed = true;
            break;
          }
          bool as_struct = query.Query()->AsStruct();
          if (!as_struct && row.values_.size() != 1) {
            // `(a, b) IN (SELECT x, y ...)` folds to a struct-JSON IN list
            // when the probe is itself a row value; a scalar probe keeps
            // the single-output-column contract.
            const Expression& probe = query.Test();
            const bool row_value_probe =
                (probe->Type() == TypeTag::kArrayExp &&
                 !probe->AsArrayExpression().Elements().empty()) ||
                (probe->Type() == TypeTag::kFunctionCallExp &&
                 probe->AsFunctionCallExpression().FuncName() ==
                     "__struct_json__") ||
                (probe->Type() == TypeTag::kConstantValue &&
                 probe->AsConstantValue().GetValue().type ==
                     ValueType::kVarChar &&
                 probe->AsConstantValue()
                         .GetValue()
                         .value.varchar_value.size() >= 2 &&
                 probe->AsConstantValue()
                         .GetValue()
                         .value.varchar_value.front() == '{');
            if (!row_value_probe) {
              return StatusError(
                  StatusCode::kInvalidArgument,
                  "Subquery of type IN must have only one output column");
            }
            as_struct = true;
          }
          values.emplace_back(ConstantValueExp(
              relational_detail::ProjectSubqueryRow(row, as_struct, nullptr)));
        }
        if (exec.Value()->GetStatus() != Status::kSuccess) {
          failed = true;
        }
        if (!failed) {
          dml_fold_applied_ = true;
          if (values.empty()) {
            // `x IN ()` is FALSE for every x (NULL included);
            // `x NOT IN ()` is TRUE (GoogleSQL three-valued fold).
            return ConstantValueExp(Value(query.Negated()));
          }
          Expression folded = InExpressionExp(query.Test(), std::move(values));
          if (query.Negated()) {
            folded =
                UnaryExpressionExp(std::move(folded), UnaryOperation::kNot);
          }
          return folded;
        }
      }
      // Correlated or unprepared inner: leave the node for the existing
      // (legacy) route instead of guessing a semantics.
      return expression;
    }
    return expression;
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  bool changed = false;
  for (Expression& child : children) {
    ASSIGN_OR_RETURN(Expression, rewritten, DecorateDmlWhere(ctx, child));
    if (rewritten->ToString() != child->ToString()) {
      child = std::move(rewritten);
      changed = true;
    }
  }
  if (!changed) {
    return expression;
  }
  return WithExpressionChildren(expression, std::move(children));
}

StatusOr<Executor> SqlEngine::ExecuteCreateTable(
    TransactionContext& ctx, const CreateTableStatement& create) {
  last_dml_table_ = create.TableName();
  // GoogleSQL: IF NOT EXISTS turns "name already taken" into a no-op
  // success and OR REPLACE drops the existing table first. The visitor
  // rejects the two flags together, so the checks below never interleave.
  if (create.IfNotExists() || create.OrReplace()) {
    const bool table_exists =
        database_->GetTable(ctx, create.TableName()).HasValue();
    const bool view_exists =
        !table_exists &&
        database_->GetViewDefinition(ctx, create.TableName()).HasValue();
    if (table_exists || view_exists) {
      if (create.IfNotExists()) {
        return Executor(std::make_shared<ConstantExecutor>(
            Row({Value("CREATE TABLE"), Value(0)})));
      }
      // OR REPLACE replaces a physical table; a name held by a view
      // (definition only, no table) keeps failing with the ordinary
      // "already exists" error instead of silently deleting the view.
      if (table_exists) {
        RETURN_IF_FAIL(database_->DropTable(ctx, create.TableName()));
        ctx.EraseCachedRelation(create.TableName());
      }
    }
  }
  if (create.IsAsSelect()) {
    // Materialize through the relational engine so the star-expanded
    // output schema is available: a raw select list still holds the
    // literal "*" directive, whose width/name cannot define the catalog.
    StatusOr<relational_detail::Relation> materialized_query =
        relational_detail::ExecuteQuery(ctx, *create.AsQuery(), nullptr, {});
    if (!materialized_query.HasValue()) {
      return MaterializeFailure(materialized_query.GetStatus(), &last_error_);
    }
    relational_detail::Relation materialized = materialized_query.MoveValue();
    std::vector<Row> rows;
    RETURN_IF_FAIL(materialized.ForEachRow(
        [&rows](const Row& row) { rows.push_back(row); }));
    std::vector<Column> columns;
    columns.reserve(materialized.schema.ColumnCount());
    for (size_t i = 0; i < materialized.schema.ColumnCount(); ++i) {
      const Column& schema_column = materialized.schema.GetColumn(i);
      std::string col_name = schema_column.Name().name;
      if (col_name.empty() || col_name == "*") {
        col_name = "col_" + std::to_string(i);
      }
      ValueType vtype = schema_column.Type();
      if (vtype == ValueType::kNull) {
        vtype = ValueType::kVarChar;
        for (const auto& row : rows) {
          if (i < row.Count() && !row[i].IsNull() &&
              row[i].type != ValueType::kNull) {
            vtype = row[i].type;
            break;
          }
        }
      }
      const Constraint constraint = CompliancePrimaryKeyMode() && i == 0
                                        ? Constraint(Constraint::kPrimaryKey)
                                        : Constraint();
      columns.emplace_back(col_name, vtype, constraint);
      columns.back().SetIntegerBits(schema_column.IntegerBits());
      columns.back().SetUnsigned(schema_column.IsUnsigned());
      columns.back().SetTextDomain(schema_column.GetTextDomain());
      columns.back().SetInterval(schema_column.IsInterval() ||
                                 std::ranges::any_of(rows, [i](const Row& row) {
                                   return i < row.Count() &&
                                          row[i].IsInterval();
                                 }));
      if (std::ranges::any_of(rows, [i](const Row& row) {
            return i < row.Count() && row[i].IsUnsigned();
          })) {
        columns.back().SetUnsigned(true);
      }
    }
    ASSIGN_OR_RETURN(
        Table, table,
        database_->CreateTable(ctx, Schema(create.TableName(), columns)));
    for (auto& row : rows) {
      // Coerce materialized values to the declared column types: the
      // SELECT output may carry narrower types (e.g. INT64 literals in
      // UNION ALL branches under a DOUBLE first row).
      for (size_t i = 0; i < row.Count() && i < columns.size(); ++i) {
        Value& cell = row.values_[i];
        const ValueType target = columns[i].Type();
        if (cell.IsNull() || cell.type == target ||
            target == ValueType::kNull) {
          continue;
        }
        if (target == ValueType::kDouble && cell.type == ValueType::kInt64) {
          cell = Value(static_cast<double>(cell.value.int_value));
        }
      }
      RETURN_IF_FAIL(table.Insert(ctx.txn_, row).GetStatus());
    }
    if (!rows.empty()) {
      table.RebuildLSTableStore(ctx.txn_);
    }
    return Executor(std::make_shared<ConstantExecutor>(Row(
        {Value("CREATE TABLE"), Value(static_cast<int64_t>(rows.size()))})));
  }
  ASSIGN_OR_RETURN(Table, table,
                   database_->CreateTable(
                       ctx, Schema(create.TableName(), create.Columns())));
  return Executor(std::make_shared<ConstantExecutor>(
      Row({Value("CREATE TABLE"), Value(0)})));
}

StatusOr<Executor> SqlEngine::ExecuteInsert(TransactionContext& ctx,
                                            InsertStatement& insert) {
  ASSIGN_OR_RETURN(std::shared_ptr<Table>, table,
                   ctx.GetTable(insert.TableName()));
  last_dml_table_ = insert.TableName();
  std::vector<Row> rows;
  // Column mapping, computed before value evaluation so the DEFAULT
  // marker in VALUES cells can find its target column.
  std::vector<size_t> upsert_offsets;
  if (!insert.Columns().empty()) {
    upsert_offsets.reserve(insert.Columns().size());
    for (const std::string& column : insert.Columns()) {
      // Dedupe by the *resolved* offset, not by a hand-folded spelling:
      // Schema::Offset applies its own case/qualifier rules (e.g. "t.a"
      // and "a" both resolve to the same column), so a separate fold can
      // miss a duplicate target and let reordered[offset] = row[i] drop a
      // value silently.
      const int destination = table->GetSchema().Offset(ColumnName(column));
      if (destination < 0) {
        last_error_ = "unknown INSERT column: " + column;
        return Status::kNotExists;
      }
      if (std::find(upsert_offsets.begin(), upsert_offsets.end(),
                    static_cast<size_t>(destination)) != upsert_offsets.end()) {
        last_error_ = "duplicate INSERT column: " + column;
        return Status::kInvalidArgument;
      }
      upsert_offsets.push_back(static_cast<size_t>(destination));
    }
  }
  const size_t schema_width = table->GetSchema().ColumnCount();
  const auto validate_width = [&](size_t count, bool allow_struct) -> Status {
    if (!insert.Columns().empty()) {
      if (count != upsert_offsets.size()) {
        last_error_ = "INSERT column/value count mismatch";
        return Status::kInvalidArgument;
      }
    } else if (count != schema_width && (!allow_struct || count != 1)) {
      last_error_ = "INSERT value count does not match table schema (got " +
                    std::to_string(count) + ", expected " +
                    std::to_string(schema_width) + ")";
      return Status::kInvalidArgument;
    }
    return Status::kSuccess;
  };
  // Validate every VALUES tuple before folding expressions or binding a
  // DEFAULT marker to the column map. One unqualified expression may be a
  // STRUCT or whole-row NULL and is validated again after expansion.
  for (const auto& value_row : insert.Values()) {
    RETURN_IF_FAIL(validate_width(value_row.size(), /*allow_struct=*/true));
  }
  // VALUES entries fold non-correlated subqueries like the WHERE/SET
  // paths do.
  for (auto& value_row : insert.MutableValues()) {
    for (Expression& value : value_row) {
      ASSIGN_OR_RETURN(Expression, folded, DecorateDmlWhere(ctx, value));
      value = std::move(folded);
    }
  }
  const auto column_default = [&table](size_t column_index) -> Value {
    const Column& column = table->GetSchema().GetColumn(column_index);
    if (column.GetConstraint().ctype == Constraint::kDefault) {
      return column.GetConstraint().value;
    }
    return {};
  };
  if (insert.Query() != nullptr) {
    // A plain projection has a statically known arity. Reject an obvious
    // width mismatch before running the source SELECT, so a volatile or
    // expensive scan is not fully materialized just to fail validation
    // afterwards. Star/set-op/AS STRUCT shapes keep the later check.
    if (insert.Query()->Type() == StatementType::kSelect) {
      const auto* select = insert.Query().get();
      if (!select->AsStruct() && select->UnionAll().empty() &&
          select->GetSetOperationTree() == nullptr) {
        bool statically_sized = true;
        for (const auto& item : select->SelectList()) {
          const Expression* projected = &item.expression;
          if (*projected && (*projected)->Type() == TypeTag::kFunctionCallExp &&
              !(*projected)->AsFunctionCallExpression().Args().empty() &&
              (*projected)->AsFunctionCallExpression().FuncName() ==
                  "__value_table_value") {
            projected =
                &(*projected)->AsFunctionCallExpression().Args().front();
          }
          if (*projected && (*projected)->Type() == TypeTag::kColumnValue &&
              (*projected)->AsColumnValue().GetColumnName().name == "*") {
            statically_sized = false;
            break;
          }
        }
        if (statically_sized) {
          RETURN_IF_FAIL(validate_width(select->SelectList().size(),
                                        /*allow_struct=*/true));
        }
      }
    }
    // INSERT ... SELECT: materialize the source query, then feed its
    // rows through the same column mapping / coercion as VALUES rows.
    plan_cache_fingerprint_.clear();
    plan_cache_parameters_.clear();
    StatusOr<relational_detail::Relation> materialized_query =
        relational_detail::ExecuteQuery(ctx, *insert.Query(), nullptr, {});
    if (!materialized_query.HasValue()) {
      return MaterializeFailure(materialized_query.GetStatus(), &last_error_);
    }
    relational_detail::Relation materialized = materialized_query.MoveValue();
    RETURN_IF_FAIL(materialized.ForEachRow(
        [&rows](Row row) { rows.push_back(std::move(row)); }));
  }
  if (insert.Query() == nullptr) {
    if (insert.Values().empty()) {
      // Keep every INSERT validation failure diagnostic: callers surface
      // LastError(), and the other branches here already report
      // kInvalidArgument for malformed input.
      last_error_ = "INSERT requires at least one VALUES row or a query";
      return Status::kInvalidArgument;
    }
    // Batch INSERT VALUES evaluation; the chunk indices only bound the
    // evaluation loop (all rows are held in `rows` until the insert below).
    // kInsertChunkSize
    constexpr size_t kInsertChunkSize = 64;
    const size_t num_values = insert.Values().size();
    rows.reserve(num_values);
    for (size_t chunk_start = 0; chunk_start < num_values;
         chunk_start += kInsertChunkSize) {
      const size_t chunk_end =
          std::min(chunk_start + kInsertChunkSize, num_values);
      for (size_t v_idx = chunk_start; v_idx < chunk_end; ++v_idx) {
        const auto& values = insert.Values()[v_idx];
        std::vector<Value> row;
        row.reserve(values.size());
        for (size_t c_idx = 0; c_idx < values.size(); ++c_idx) {
          const Expression& value = values[c_idx];
          // The DEFAULT keyword rides a marker function call; the
          // visitor cannot see the target table, so the substitution
          // happens here against the mapped target column.
          if (value->Type() == TypeTag::kFunctionCallExp &&
              value->AsFunctionCallExpression().FuncName() ==
                  "__column_default") {
            // A column without a declared default has the implicit
            // default NULL (BigQuery semantics, pinned by the dml_*
            // compliance cases), so resolution never fails here.
            const size_t target =
                insert.Columns().empty() ? c_idx : upsert_offsets[c_idx];
            row.push_back(column_default(target));
            continue;
          }
          // Use the shared relational evaluator, not the AST-only path: the
          // batch builtin table implements functions (TO_BASE64, FROM_HEX,
          // PARSE_DATE, ARRAY_* ...) that the AST ground truth does not, and a
          // constant VALUES expression must accept every batch-supported
          // builtin regardless of which execution tier evaluates it.
          StatusOr<Value> item = relational_detail::TryEvaluate(
              value, relational_detail::Scope{}, nullptr, ctx,
              relational_detail::CteMap{});
          if (!item.HasValue()) {
            last_error_ = item.GetStatus().GetMessage();
            return item.GetStatus();
          }
          row.push_back(item.MoveValue());
        }
        rows.emplace_back(std::move(row));
      }
    }
  }
  for (auto& row : rows) {
    RETURN_IF_FAIL(validate_width(row.values_.size(), /*allow_struct=*/true));
    if (!insert.Columns().empty()) {
      std::vector<Value> reordered(table->GetSchema().ColumnCount());
      std::vector<bool> specified(table->GetSchema().ColumnCount(), false);
      for (size_t i = 0; i < insert.Columns().size(); ++i) {
        reordered[upsert_offsets[i]] = row[i];
        specified[upsert_offsets[i]] = true;
      }
      // Columns omitted from the target list take their declared
      // DEFAULT; columns without one stay NULL (the historical
      // behavior).
      for (size_t i = 0; i < reordered.size(); ++i) {
        if (!specified[i]) {
          reordered[i] = column_default(i);
        }
      }
      row = Row(std::move(reordered));
    } else if (row.values_.size() == 1 &&
               table->GetSchema().ColumnCount() > 1) {
      // INSERT t VALUES ((1, 'x')) / VALUES (NULL) / (DEFAULT): a single
      // STRUCT (or whole-row NULL) expands across every column.
      std::vector<Value> expanded;
      if (ExpandStructInsertValue(row[0], table->GetSchema().ColumnCount(),
                                  &expanded)) {
        row = Row(std::move(expanded));
      }
    }
    if (insert.Columns().empty()) {
      RETURN_IF_FAIL(
          validate_width(row.values_.size(), /*allow_struct=*/false));
    }
    for (size_t i = 0; i < row.values_.size(); ++i) {
      const auto& col = table->GetSchema().GetColumn(i);
      const ValueType expected = col.Type();
      if (Status st_ni = ValidateNarrowInteger(col, row[i]);
          st_ni != Status::kSuccess) {
        // Preserve the storage/MVCC status code (e.g. kOutOfRange) so the
        // caller can still distinguish retryable errors from input errors.
        last_error_ = st_ni.GetMessage();
        return st_ni;
      }
      if (col.IsUnsigned()) {
        if (!row[i].IsNull()) {
          if (row[i].type != ValueType::kInt64) {
            last_error_ = "INSERT type mismatch for column " + col.Name().name;
            return Status::kInvalidArgument;
          }
          if (!row[i].IsUnsigned() && row[i].value.int_value < 0) {
            last_error_ =
                "assignment out of range for column " + col.Name().name;
            return Status::kInvalidArgument;
          }
          row[i] = row[i].WithUnsigned();
        }
        continue;
      }
      if (row[i].IsNull() || row[i].type == expected) {
        continue;
      }
      if (expected == ValueType::kDouble && row[i].type == ValueType::kInt64) {
        // T3699: unsigned values store two's-complement bits in int_value;
        // a plain cast turned >= 2^63 into a negative double (UINT64_MAX
        // landed as -1.0). Mirror the unsigned-aware branch above.
        row[i] = row[i].IsUnsigned()
                     ? Value(static_cast<double>(
                           static_cast<uint64_t>(row[i].value.int_value)))
                     : Value(static_cast<double>(row[i].value.int_value));
        continue;
      }
      if (expected == ValueType::kDate && row[i].type == ValueType::kVarChar) {
        row[i] = Value::Date(row[i].value.varchar_value);
        continue;
      }
      last_error_ = "INSERT type mismatch for column " + col.Name().name;
      return Status::kInvalidArgument;
    }
  }
  // Compiled-plan fill: build the parametric INSERT artifact. Bindable
  // literals become ParameterSlot placeholders; runtime values are
  // injected per execution, so any literal combination hits.
  // Conflict-handling inserts (modes / PK emulation / row-count asserts)
  // always take the authoritative path so their semantics cannot be
  // lost in a replayed shape.
  if (!plan_cache_fingerprint_.empty() &&
      insert.Mode() == InsertMode::kDefault && !insert.HasAssert() &&
      !CompliancePrimaryKeyMode()) {
    auto shape = std::make_shared<CompiledPlan::InsertShape>();
    bool ok = true;
    size_t slot_cursor = 0;
    shape->cells.reserve(insert.Values().size());
    for (const auto& values : insert.Values()) {
      // A one-value row into a wider table only works through the STRUCT /
      // struct-text expansion above (VALUES ('{"a", 1}')), which the
      // replayed shape does not repeat: the second identical-shape INSERT
      // failed with "row column count does not match table schema".
      if (insert.Columns().empty() && values.size() == 1 &&
          table->GetSchema().ColumnCount() > 1) {
        ok = false;
        break;
      }
      std::vector<Expression> cells;
      cells.reserve(values.size());
      for (const Expression& value : values) {
        // DEFAULT markers must resolve against the target table's
        // catalog (done above); a replayed shape would evaluate the
        // marker function and fail, so those statements stay on the
        // authoritative path.
        if (ContainsNonDeterministicCall(value) ||
            (value->Type() == TypeTag::kFunctionCallExp &&
             value->AsFunctionCallExpression().FuncName() ==
                 "__column_default")) {
          ok = false;
          break;
        }
        cells.push_back(SlotizeLiterals(value, &slot_cursor, &ok));
        if (!ok) {
          break;
        }
      }
      if (!ok) {
        break;
      }
      shape->cells.push_back(std::move(cells));
    }
    // Slot count must match the extracted parameter count exactly;
    // otherwise text-order alignment is broken and caching stays off.
    if (ok && slot_cursor == plan_cache_parameters_.size()) {
      shape->table = table;
      shape->schema = table->GetSchema();
      shape->has_named_columns = !insert.Columns().empty();
      if (shape->has_named_columns) {
        // Offsets were validated above (no duplicates, all known).
        shape->reorder.reserve(insert.Columns().size());
        for (const std::string& column : insert.Columns()) {
          shape->reorder.push_back(
              static_cast<size_t>(shape->schema.Offset(ColumnName(column))));
        }
      }
      auto compiled = std::make_shared<CompiledPlan>();
      compiled->kind = CompiledPlan::Kind::kInsert;
      compiled->database = database_;
      compiled->epoch = database_->SchemaEpoch();
      compiled->opt_bits = CurrentPlanOptToggleBits();
      compiled->parameters = plan_cache_parameters_;
      compiled->insert_shape = std::move(shape);
      // Bug 57: VALUES rows folded to prepare-time constants must not
      // be cached for reuse after the inner tables move on.
      if (!dml_fold_applied_) {
        StoreCompiledPlan(plan_cache_fingerprint_, std::move(compiled));
      }
    }
  }
  InsertExecutionMode exec_mode = InsertExecutionMode::kDefault;
  switch (insert.Mode()) {
    case InsertMode::kDefault:
      exec_mode = InsertExecutionMode::kDefault;
      break;
    case InsertMode::kIgnore:
      exec_mode = InsertExecutionMode::kIgnore;
      break;
    case InsertMode::kUpdate:
      exec_mode = InsertExecutionMode::kUpsert;
      break;
    case InsertMode::kReplace:
      exec_mode = InsertExecutionMode::kReplace;
      break;
  }
  ASSIGN_OR_RETURN(std::vector<ForeignKeyCheck>, fk_checks,
                   (ResolveForeignKeyChecks(ctx, *table)));
  return Executor(std::make_shared<Insert>(
      ctx.txn_, table.get(),
      std::make_shared<ConstantExecutor>(std::move(rows)), exec_mode,
      CompliancePrimaryKeyMode(), upsert_offsets, insert.AssertRowsModified(),
      std::move(fk_checks)));
}

StatusOr<Executor> SqlEngine::ExecuteUpdate(TransactionContext& ctx,
                                            UpdateStatement& update) {
  ASSIGN_OR_RETURN(std::shared_ptr<Table>, table,
                   ctx.GetTable(update.TableName()));
  last_dml_table_ = update.TableName();
  if (!update.HasNestedDml()) {
    // SET values share the WHERE clause's prepare-time subquery fold;
    // a correlated or WITH-bearing inner keeps its legacy route.
    const Schema& update_schema = table->GetSchema();
    for (auto& [target, value] : update.MutableSetClause()) {
      ASSIGN_OR_RETURN(Expression, folded, DecorateDmlWhere(ctx, value));
      value = std::move(folded);
      // SET col = DEFAULT resolves to the column's declared default;
      // columns (and dotted STRUCT fields, which carry no declared
      // default) without one resolve to NULL (BigQuery semantics,
      // pinned by the dml_update* compliance cases).
      if (value->Type() == TypeTag::kFunctionCallExp &&
          value->AsFunctionCallExpression().FuncName() == "__column_default") {
        const int target_offset = update_schema.Offset(ColumnName(target.name));
        value = ConstantValueExp(
            target_offset >= 0 &&
                    update_schema.GetColumn(static_cast<slot_t>(target_offset))
                            .GetConstraint()
                            .ctype == Constraint::kDefault
                ? update_schema.GetColumn(static_cast<slot_t>(target_offset))
                      .GetConstraint()
                      .value
                : Value());
      }
    }
  }
  if (update.HasNestedDml()) {
    // Nested per-row array DML: rows are rewritten element-wise here
    // because array mutations are not expressible as plain projections.
    return ExecuteNestedArrayUpdate(ctx, update, table.get());
  }
  const Schema& schema = table->GetSchema();
  {
    // A SET target qualified by the table name or the UPDATE alias ("t.a",
    // "x.a" after "UPDATE t AS x") addresses a plain column of that table,
    // and a longer path ("x.s.f") addresses a STRUCT field through it.
    // Strip the relation qualifier (case-insensitively) before classifying:
    // comparing it with a raw != against the table name misread both
    // spellings as dotted STRUCT targets, which reported a modified-row
    // count and updated nothing.
    const std::string& update_alias = update.Alias();
    auto strip_relation_qualifier = [&](ColumnName* target) {
      if (target->schema.empty() || target->name.empty()) {
        return;
      }
      if (!IdentifierEquals(target->schema, update.TableName()) &&
          (update_alias.empty() ||
           !IdentifierEquals(target->schema, update_alias))) {
        return;
      }
      std::string name = std::move(target->name);
      const size_t dot = name.find('.');
      if (dot == std::string::npos) {
        *target = ColumnName(std::string(), std::move(name));
        return;
      }
      *target = ColumnName(name.substr(0, dot), name.substr(dot + 1));
    };
    bool dotted_target = false;
    bool plain_target = false;
    for (auto& [target, expression] : update.MutableSetClause()) {
      strip_relation_qualifier(&target);
      if (!target.schema.empty() && !target.name.empty()) {
        dotted_target = true;
      } else {
        plain_target = true;
      }
    }
    // A mixed SET list would silently lose the plain assignments (the
    // dotted path below only rewrites field_targets), so refuse the mixed
    // shape instead of committing a partial update.
    if (dotted_target && plain_target) {
      return Status(
          Status::kInvalidArgument,
          "UPDATE cannot mix dotted STRUCT field targets with plain column "
          "targets in one SET clause");
    }
    if (dotted_target) {
      return ExecuteStructFieldUpdate(ctx, update, table.get());
    }
  }
  std::vector<NamedExpression> output;
  // Value tables (single physical column) allow the FROM alias to stand
  // for the whole row: "UPDATE t AS v SET v = ... WHERE v = ..." binds
  // the alias to that column. Rewritten targets/expressions below.
  std::string value_table_alias = update.Alias();
  if (!value_table_alias.empty() && schema.ColumnCount() == 1) {
    bool alias_is_column = false;
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (IdentifierEquals(schema.GetColumn(i).Name().name,
                           value_table_alias)) {
        alias_is_column = true;
      }
    }
    if (alias_is_column) {
      value_table_alias.clear();
    }
  } else {
    value_table_alias.clear();
  }
  std::vector<std::pair<ColumnName, Expression>> set_clause =
      update.SetClause();
  Expression where_clause = update.WhereClause();
  if (!value_table_alias.empty()) {
    const ColumnName only_column(update.TableName(),
                                 schema.GetColumn(0).Name().name);
    auto matches_alias = [&](const ColumnName& name) {
      return (name.schema.empty() ||
              IdentifierEquals(name.schema, value_table_alias)) &&
             IdentifierEquals(name.name, value_table_alias);
    };
    for (auto& [target, expression] : set_clause) {
      if (matches_alias(target)) {
        target = ColumnName(target.schema, only_column.name);
      }
    }
    if (where_clause) {
      std::function<Expression(const Expression&)> bind_alias =
          [&](const Expression& expr) -> Expression {
        if (!expr) {
          return expr;
        }
        if (expr->Type() == TypeTag::kColumnValue) {
          const ColumnName& name = expr->AsColumnValue().GetColumnName();
          if (matches_alias(name)) {
            return ColumnValueExp(only_column);
          }
          if (!name.schema.empty() &&
              IdentifierEquals(name.schema, value_table_alias)) {
            return ColumnValueExp(ColumnName(update.TableName(), name.name));
          }
          return expr;
        }
        std::vector<Expression> children = ExpressionChildren(expr);
        bool changed = false;
        for (Expression& child : children) {
          Expression mapped = bind_alias(child);
          changed |= child->ToString() != mapped->ToString();
          child = std::move(mapped);
        }
        return changed ? WithExpressionChildren(expr, std::move(children))
                       : expr;
      };
      where_clause = bind_alias(where_clause);
    }
  }
  // Multi-column tables bind alias-qualified predicate references
  // ("UPDATE t AS x ... WHERE x.b = 1") by dropping the alias: it names the
  // scanned table, so the reference is the unqualified column, or a dotted
  // STRUCT field read (first path component promoted) that the
  // bind_struct_fields pass below rewrites.
  if (where_clause && schema.ColumnCount() > 1) {
    const std::string& update_alias = update.Alias();
    if (!update_alias.empty()) {
      std::function<Expression(const Expression&)> bind_update_alias =
          [&](const Expression& expr) -> Expression {
        if (!expr) {
          return expr;
        }
        if (expr->Type() == TypeTag::kColumnValue) {
          const ColumnName& name = expr->AsColumnValue().GetColumnName();
          if (!name.schema.empty() &&
              IdentifierEquals(name.schema, update_alias)) {
            const size_t dot = name.name.find('.');
            if (dot == std::string::npos) {
              return ColumnValueExp(ColumnName(std::string(), name.name));
            }
            return ColumnValueExp(ColumnName(name.name.substr(0, dot),
                                             name.name.substr(dot + 1)));
          }
          return expr;
        }
        std::vector<Expression> children = ExpressionChildren(expr);
        bool changed = false;
        for (Expression& child : children) {
          Expression mapped = bind_update_alias(child);
          changed |= child->ToString() != mapped->ToString();
          child = std::move(mapped);
        }
        return changed ? WithExpressionChildren(expr, std::move(children))
                       : expr;
      };
      where_clause = bind_update_alias(where_clause);
      // SET value expressions may reference the alias as well
      // ("SET x.a = x.b + 1"); rewrite them with the same binding.
      for (auto& [target, expression] : set_clause) {
        expression = bind_update_alias(expression);
      }
    }
  }
  if (!plan_cache_fingerprint_.empty() && !dml_fold_applied_ &&
      !plan_cache_parameters_.empty() && !update.HasAssert()) {
    SqlLiteralSlots literal_slots;
    literal_slots.parameters = &plan_cache_parameters_;
    std::vector<Expression> slotted_sets;
    slotted_sets.reserve(set_clause.size());
    for (const auto& clause : set_clause) {
      slotted_sets.push_back(literal_slots.Apply(clause.second));
    }
    const Expression slotted_where = literal_slots.Apply(where_clause);
    if (literal_slots.Complete()) {
      for (size_t i = 0; i < set_clause.size(); ++i) {
        set_clause[i].second = std::move(slotted_sets[i]);
      }
      where_clause = slotted_where;
    }
  }
  output.reserve(schema.ColumnCount());
  auto column_index = [&](std::string_view name) -> int {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (IdentifierEquals(schema.GetColumn(i).Name().name, name)) {
        return static_cast<int>(i);
      }
    }
    return -1;
  };
  // WHERE predicates may reference struct fields through dotted paths;
  // rewrite those references into get_field(column, "field.path") when
  // the path prefix names a column rather than a relation.  Single-column
  // (value / proto) tables additionally bind unresolved bare names to
  // field reads of that column.
  const bool single_column_table = schema.ColumnCount() == 1;
  if (where_clause) {
    std::function<Expression(const Expression&)> bind_struct_fields =
        [&](const Expression& expr) -> Expression {
      if (!expr) {
        return expr;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const ColumnName& name = expr->AsColumnValue().GetColumnName();
        if (!name.schema.empty() && name.schema != update.TableName()) {
          const int base_column = column_index(name.schema);
          if (base_column >= 0 && !name.name.empty() && name.name != "*") {
            return FunctionCallExp(
                "__get_field_safe",
                {ColumnValueExp(ColumnName(
                     update.TableName(),
                     schema.GetColumn(static_cast<size_t>(base_column))
                         .Name()
                         .name)),
                 ConstantValueExp(Value(std::string(name.name)))});
          }
        }
        if (single_column_table && name.schema.empty() && !name.name.empty() &&
            name.name != "*" &&
            !IdentifierEquals(name.name, schema.GetColumn(0).Name().name)) {
          return FunctionCallExp(
              "__get_field_safe",
              {ColumnValueExp(ColumnName(update.TableName(),
                                         schema.GetColumn(0).Name().name)),
               ConstantValueExp(Value(std::string(name.name)))});
        }
        return expr;
      }
      std::vector<Expression> children = ExpressionChildren(expr);
      bool changed = false;
      for (Expression& child : children) {
        Expression mapped = bind_struct_fields(child);
        changed |= child->ToString() != mapped->ToString();
        child = std::move(mapped);
      }
      return changed ? WithExpressionChildren(expr, std::move(children)) : expr;
    };
    where_clause = bind_struct_fields(where_clause);
  }
  // SET targets may be qualified ("UPDATE t SET t.a = ..."); match the
  // full ColumnName instead of the bare name so qualified assignments are
  // not silently ignored.
  std::vector<bool> applied(set_clause.size(), false);
  // Unmatched targets on single-column (proto) tables address fields of
  // that column's TEXT payload ("SET int64_key_1 = 100").
  std::vector<std::pair<std::string, const Expression*>> proto_sets;
  proto_sets.reserve(applied.size());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const Column& column = schema.GetColumn(i);
    Expression expression = ColumnValueExp(column.Name());
    for (size_t j = 0; j < set_clause.size(); ++j) {
      const ColumnName& target = set_clause[j].first;
      if ((target.name == column.Name().name ||
           IdentifierEquals(target.name, column.Name().name)) &&
          (target.schema.empty() || target.schema == update.TableName())) {
        expression = set_clause[j].second;
        // Narrow / unsigned integer columns re-validate the assigned
        // value through a CAST to the declared width. Decide from the
        // column metadata (as INSERT does via IntegerValueFits), never
        // from substrings of the column NAME: an INT64 column called
        // point32 is not an INT32.
        if (column.Type() == ValueType::kInt64 &&
            (column.IsUnsigned() ||
             (column.IntegerBits() != 0 && column.IntegerBits() < 64))) {
          const unsigned bits =
              column.IntegerBits() == 0 ? 64U : column.IntegerBits();
          const std::string target_type =
              std::string(column.IsUnsigned() ? "UINT" : "INT") +
              std::to_string(bits);
          expression = std::make_shared<CastExpression>(std::move(expression),
                                                        target_type, false);
        }
        applied[j] = true;
        break;
      }
    }
    output.emplace_back(column.Name().name, std::move(expression));
  }
  // A bare name that matches a schema column (case-insensitively). Used by
  // the residual check below to tell a duplicate assignment from a typo.
  const auto find_column = [&schema](const std::string& name) -> int {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (IdentifierEquals(schema.GetColumn(i).Name().name, name)) {
        return static_cast<int>(i);
      }
    }
    return -1;
  };
  for (size_t j = 0; j < applied.size(); ++j) {
    if (applied[j]) {
      continue;
    }
    const ColumnName& target = set_clause[j].first;
    const bool plain_or_local =
        target.schema.empty() || target.schema == update.TableName();
    // The column loop above adopts one SET entry per column (it stops at the
    // first match), so a second assignment to the same column lands here
    // unconsumed. Naming a REAL column that way is a duplicate target, not a
    // missing one: reporting "target not found: a" for an existing column is
    // a second-order lie, and on a single-column table it falls through to
    // the proto path and blames the TEXT payload instead. Say what happened
    // (SQLSTATE 42701 territory) with the matching status code.
    if (plain_or_local && !target.name.empty() && target.name != "*" &&
        find_column(target.name) >= 0) {
      const std::string shown =
          target.schema.empty() ? target.name : target.ToString();
      last_error_ = "UPDATE SET target appears more than once: " + shown;
      return Status::kInvalidArgument;
    }
    if (single_column_table && plain_or_local && !target.name.empty() &&
        target.name != "*") {
      std::string path = target.name;
      if (!target.schema.empty()) {
        path = target.schema + "." + target.name;
      }
      proto_sets.emplace_back(std::move(path), &set_clause[j].second);
      applied[j] = true;
      continue;
    }
    last_error_ =
        "UPDATE SET target not found: " +
        (set_clause[j].first.schema.empty() ? set_clause[j].first.name
                                            : set_clause[j].first.ToString());
    return Status::kNotExists;
  }
  if (!proto_sets.empty()) {
    // Wrap the column read with chained __proto_set calls so each field
    // assignment applies to the payload produced by the previous one.
    for (auto& entry : output) {
      if (!IdentifierEquals(entry.name, schema.GetColumn(0).Name().name)) {
        continue;
      }
      Expression current = std::move(entry.expression);
      for (const auto& [path, value_expr] : proto_sets) {
        current = FunctionCallExp(
            "__proto_set",
            {std::move(current), ConstantValueExp(Value(std::string(path))),
             *value_expr});
      }
      entry.expression = std::move(current);
    }
  }
  QueryData query;
  query.from_ = {update.TableName()};
  query.where_ = where_clause ? where_clause : ConstantValueExp(Value(true));
  query.select_ = std::move(output);
  query.require_row_position_ = true;
  if (where_clause) {
    ASSIGN_OR_RETURN(Expression, decorated_where,
                     DecorateDmlWhere(ctx, where_clause));
    query.where_ = decorated_where;
  }
  RETURN_IF_FAIL(query.Rewrite(ctx));
  ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
  std::vector<Expression> parameter_slots;
  if (!plan_cache_fingerprint_.empty() && !dml_fold_applied_ &&
      !update.HasAssert() && !CompliancePrimaryKeyMode()) {
    parameter_slots = InstallPlanSlots(plan, plan_cache_parameters_);
  }
  if (!update.HasAssert() && !dml_fold_applied_ &&
      !CompliancePrimaryKeyMode()) {
    // Bug 57: a prepare-time subquery fold bakes the inner rows' values
    // into the plan; re-preparing the same text after those rows change
    // must not reuse the specialized plan.
    RememberSpecializedPlan(plan_cache_fingerprint_, database_->SchemaEpoch(),
                            plan_cache_parameters_, CompiledPlan::Kind::kUpdate,
                            plan, table, database_, ctx,
                            std::move(parameter_slots));
  }
  // Compliance primary-key mode: UPDATEs assigning the first column must
  // keep the emulated key unique and duplicate-free.
  bool update_touches_primary_key = false;
  if (CompliancePrimaryKeyMode()) {
    for (const auto& [target, expression] : update.SetClause()) {
      if (schema.Offset(ColumnName(target.name)) == 0) {
        update_touches_primary_key = true;
        break;
      }
    }
  }
  ASSIGN_OR_RETURN(std::vector<ForeignKeyCheck>, fk_checks,
                   (ResolveForeignKeyChecks(ctx, *table)));
  ASSIGN_OR_RETURN(std::vector<ReferencedByCheck>, ref_checks,
                   (ResolveReferencedByChecks(database_, ctx, *table)));
  return Executor(std::make_shared<Update>(
      ctx.txn_, table.get(), plan->EmitExecutor(ctx),
      update.AssertRowsModified(), update_touches_primary_key,
      std::move(fk_checks), std::move(ref_checks)));
}

StatusOr<Executor> SqlEngine::ExecuteDelete(TransactionContext& ctx,
                                            const DeleteStatement& remove) {
  ASSIGN_OR_RETURN(std::shared_ptr<Table>, table,
                   ctx.GetTable(remove.TableName()));
  last_dml_table_ = remove.TableName();
  std::vector<NamedExpression> output;
  const Schema& schema = table->GetSchema();
  output.reserve(schema.ColumnCount());
  // Value tables: bind a FROM alias standing for the whole row to the
  // single physical column ("DELETE t AS v WHERE v = ...").
  Expression where_clause = remove.WhereClause();
  const std::string& delete_alias = remove.Alias();
  if (!delete_alias.empty() && schema.ColumnCount() == 1 &&
      !IdentifierEquals(schema.GetColumn(0).Name().name, delete_alias)) {
    const ColumnName only_column(remove.TableName(),
                                 schema.GetColumn(0).Name().name);
    std::function<Expression(const Expression&)> bind_alias =
        [&](const Expression& expr) -> Expression {
      if (!expr) {
        return expr;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const ColumnName& name = expr->AsColumnValue().GetColumnName();
        if ((name.schema.empty() ||
             IdentifierEquals(name.schema, delete_alias)) &&
            IdentifierEquals(name.name, delete_alias)) {
          return ColumnValueExp(only_column);
        }
        if (!name.schema.empty() &&
            IdentifierEquals(name.schema, delete_alias)) {
          return ColumnValueExp(ColumnName(remove.TableName(), name.name));
        }
        return expr;
      }
      std::vector<Expression> children = ExpressionChildren(expr);
      bool changed = false;
      for (Expression& child : children) {
        Expression mapped = bind_alias(child);
        changed |= child->ToString() != mapped->ToString();
        child = std::move(mapped);
      }
      return changed ? WithExpressionChildren(expr, std::move(children)) : expr;
    };
    where_clause = bind_alias(where_clause);
  }
  // Single-column (proto) value tables: unresolved bare names in the
  // predicate address fields of the only column's TEXT payload.
  if (where_clause && schema.ColumnCount() == 1 &&
      !IdentifierEquals(schema.GetColumn(0).Name().name, delete_alias)) {
    const ColumnName only_column(remove.TableName(),
                                 schema.GetColumn(0).Name().name);
    std::function<Expression(const Expression&)> bind_proto_fields =
        [&](const Expression& expr) -> Expression {
      if (!expr) {
        return expr;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const ColumnName& name = expr->AsColumnValue().GetColumnName();
        if (name.schema.empty() && !name.name.empty() && name.name != "*" &&
            !IdentifierEquals(name.name, schema.GetColumn(0).Name().name)) {
          return FunctionCallExp(
              "__get_field_safe",
              {ColumnValueExp(only_column),
               ConstantValueExp(Value(std::string(name.name)))});
        }
        return expr;
      }
      std::vector<Expression> children = ExpressionChildren(expr);
      bool changed = false;
      for (Expression& child : children) {
        Expression mapped = bind_proto_fields(child);
        changed |= child->ToString() != mapped->ToString();
        child = std::move(mapped);
      }
      return changed ? WithExpressionChildren(expr, std::move(children)) : expr;
    };
    where_clause = bind_proto_fields(where_clause);
  }
  if (!plan_cache_fingerprint_.empty() && !dml_fold_applied_ &&
      !plan_cache_parameters_.empty() && !remove.HasAssert()) {
    SqlLiteralSlots literal_slots;
    literal_slots.parameters = &plan_cache_parameters_;
    const Expression slotted_where = literal_slots.Apply(where_clause);
    if (literal_slots.Complete()) {
      where_clause = slotted_where;
    }
  }
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    output.emplace_back(schema.GetColumn(i).Name());
  }
  QueryData query;
  query.from_ = {remove.TableName()};
  query.where_ = where_clause ? where_clause : ConstantValueExp(Value(true));
  query.select_ = std::move(output);
  query.require_row_position_ = true;
  query.wait_for_write_intent_ = false;
  // ... and FALSE stays FALSE, deliberately. The scan holds the index
  // read guard (the structural-mutation gate) for its whole duration;
  // with a blocking wait, a delivery-style DELETE that races another
  // writer parks INSIDE the guard for as long as the holder needs to
  // finish its remaining statements -- which stalls every concurrent
  // index split behind the gate, stretches those transactions' own row
  // intents, and cascades into escape-hatch timeouts on unrelated hot
  // rows (measured on TPC-C W=1: the engine collapsed to ~12 tps with
  // 5 s wait-limit aborts everywhere). The try-lock policy converts the
  // overlap into an immediate statement-level conflict: the executor
  // unwinds, releases the guard and its intents at once, and the caller
  // retries. UPDATE keeps the waiting default because its conflicts
  // land on short-lived holders (single-row district/customer
  // updates), not on multi-statement delivery transactions.
  if (where_clause) {
    ASSIGN_OR_RETURN(Expression, decorated_where,
                     DecorateDmlWhere(ctx, where_clause));
    query.where_ = decorated_where;
  }
  RETURN_IF_FAIL(query.Rewrite(ctx));
  ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
  std::vector<Expression> parameter_slots;
  if (!plan_cache_fingerprint_.empty() && !dml_fold_applied_ &&
      !remove.HasAssert()) {
    parameter_slots = InstallPlanSlots(plan, plan_cache_parameters_);
  }
  if (!remove.HasAssert() && !dml_fold_applied_) {
    RememberSpecializedPlan(plan_cache_fingerprint_, database_->SchemaEpoch(),
                            plan_cache_parameters_, CompiledPlan::Kind::kDelete,
                            plan, table, database_, ctx,
                            std::move(parameter_slots));
  }
  ASSIGN_OR_RETURN(std::vector<ReferencedByCheck>, ref_checks,
                   (ResolveReferencedByChecks(database_, ctx, *table)));
  return Executor(std::make_shared<DeleteExecutor>(
      ctx.txn_, *table, plan->EmitExecutor(ctx), remove.AssertRowsModified(),
      std::move(ref_checks)));
}

StatusOr<Executor> SqlEngine::ExecuteDropTable(TransactionContext& ctx,
                                               const DropTableStatement& drop) {
  Status drop_status = database_->DropTable(ctx, drop.TableName());
  if (drop.IfExists() && drop_status == Status::kNotExists) {
    drop_status = Status::kSuccess;
  }
  RETURN_IF_FAIL(drop_status);
  // A materialized view is a physical table plus a defining-text record.
  // DROP TABLE must clear both, otherwise the stale definition makes a later
  // CREATE MATERIALIZED VIEW of the same name fail as "already exists" while
  // DROP MATERIALIZED VIEW fails as "does not exist".
  RETURN_IF_FAIL(
      database_->DeleteMaterializedViewDefinition(ctx, drop.TableName()));
  ctx.EraseCachedRelation(drop.TableName());
  return Executor(
      std::make_shared<ConstantExecutor>(Row({Value("DROP TABLE"), Value(0)})));
}

StatusOr<Executor> SqlEngine::ExecuteCreateMaterializedView(
    TransactionContext& ctx, const CreateMaterializedViewStatement& create) {
  last_dml_table_ = create.ViewName();
  plan_cache_fingerprint_.clear();
  plan_cache_parameters_.clear();
  const bool view_exists =
      database_->GetTable(ctx, create.ViewName()).HasValue();
  if (view_exists ||
      database_->GetMaterializedViewDefinition(ctx, create.ViewName())
          .HasValue() ||
      database_->GetViewDefinition(ctx, create.ViewName()).HasValue()) {
    if (create.IfNotExists()) {
      return Executor(std::make_shared<ConstantExecutor>(
          Row({Value("CREATE MATERIALIZED VIEW"), Value(0)})));
    }
    last_error_ = "materialized view " + create.ViewName() + " already exists";
    return Status::kConflicts;
  }
  // Materialize through the relational engine so the star-expanded
  // output schema is available (same contract as CREATE TABLE AS SELECT).
  StatusOr<relational_detail::Relation> materialized_query =
      relational_detail::ExecuteQuery(ctx, *create.Query(), nullptr, {});
  if (!materialized_query.HasValue()) {
    return MaterializeFailure(materialized_query.GetStatus(), &last_error_);
  }
  relational_detail::Relation materialized = materialized_query.MoveValue();
  std::vector<Row> rows;
  RETURN_IF_FAIL(
      materialized.ForEachRow([&rows](const Row& row) { rows.push_back(row); }));
  std::vector<Column> columns;
  columns.reserve(materialized.schema.ColumnCount());
  for (size_t i = 0; i < materialized.schema.ColumnCount(); ++i) {
    const Column& schema_column = materialized.schema.GetColumn(i);
    std::string col_name = schema_column.Name().name;
    if (col_name.empty() || col_name == "*") {
      col_name = "col_" + std::to_string(i);
    }
    ValueType vtype = schema_column.Type();
    if (vtype == ValueType::kNull) {
      vtype = ValueType::kVarChar;
      for (const auto& row : rows) {
        if (i < row.Count() && !row[i].IsNull() &&
            row[i].type != ValueType::kNull) {
          vtype = row[i].type;
          break;
        }
      }
    }
    columns.emplace_back(col_name, vtype, Constraint());
    if (std::ranges::any_of(rows, [i](const Row& row) {
          return i < row.Count() && row[i].IsUnsigned();
        })) {
      columns.back().SetUnsigned(true);
    }
  }
  ASSIGN_OR_RETURN(Table, table,
                   database_->CreateTable(
                       ctx, Schema(create.ViewName(), std::move(columns))));
  for (auto& row : rows) {
    RETURN_IF_FAIL(table.Insert(ctx.txn_, row).GetStatus());
  }
  // Persist the defining text last: a CREATE that failed midway leaves
  // no definition behind, so the name stays creatable.
  RETURN_IF_FAIL(database_->PutMaterializedViewDefinition(
      ctx, create.ViewName(), create.QueryText()));
  return Executor(std::make_shared<ConstantExecutor>(
      Row({Value("CREATE MATERIALIZED VIEW"),
           Value(static_cast<int64_t>(rows.size()))})));
}

StatusOr<Executor> SqlEngine::ExecuteDropMaterializedView(
    TransactionContext& ctx, const DropMaterializedViewStatement& drop) {
  last_dml_table_ = drop.ViewName();
  plan_cache_fingerprint_.clear();
  plan_cache_parameters_.clear();
  StatusOr<std::string> definition =
      database_->GetMaterializedViewDefinition(ctx, drop.ViewName());
  if (!definition.HasValue()) {
    if (drop.IfExists() && definition.GetStatus() == Status::kNotExists) {
      return Executor(std::make_shared<ConstantExecutor>(
          Row({Value("DROP MATERIALIZED VIEW"), Value(0)})));
    }
    // Preserve a real catalog failure (kCorrupt/kIOError/kConflicts) instead
    // of rewriting it into a misleading "does not exist".
    last_error_ = std::string(definition.GetStatus().GetMessage());
    return definition.GetStatus();
  }
  RETURN_IF_FAIL(database_->DropTable(ctx, drop.ViewName()));
  RETURN_IF_FAIL(
      database_->DeleteMaterializedViewDefinition(ctx, drop.ViewName()));
  ctx.EraseCachedRelation(drop.ViewName());
  return Executor(std::make_shared<ConstantExecutor>(
      Row({Value("DROP MATERIALIZED VIEW"), Value(0)})));
}

StatusOr<Executor> SqlEngine::ExecuteCreateView(
    TransactionContext& ctx, const CreateViewStatement& create) {
  last_dml_table_ = create.ViewName();
  plan_cache_fingerprint_.clear();
  plan_cache_parameters_.clear();
  // IF NOT EXISTS is a no-op when the name is already taken (mirroring
  // CREATE TABLE IF NOT EXISTS and the parser's is_if_not_exists flag).
  if (create.IfNotExists()) {
    if (database_->GetTable(ctx, create.ViewName()).HasValue() ||
        database_->GetViewDefinition(ctx, create.ViewName()).HasValue() ||
        database_->GetMaterializedViewDefinition(ctx, create.ViewName())
            .HasValue()) {
      return Executor(std::make_shared<ConstantExecutor>(
          Row({Value("CREATE VIEW"), Value(0)})));
    }
  }
  // Validate the body BEFORE persisting it: a parser failure or a body with
  // no Query node must not leave a name occupied in the catalog with no AST
  // registered (DDL must be all-or-nothing).
  const GoogleSqlParseResult parsed_view =
      GoogleSqlFrontend::Parse(create.QueryText());
  if (!parsed_view.ok) {
    return StatusError(
        StatusCode::kInvalidArgument,
        "CREATE VIEW: cannot parse query body: " + parsed_view.error);
  }
  StatusOr<std::unique_ptr<GoogleSqlAstNode>> view_ast =
      GoogleSqlAstParser::Parse(parsed_view.ast);
  if (!view_ast.HasValue() || view_ast.Value() == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "CREATE VIEW: invalid query body AST");
  }
  const GoogleSqlAstNode* query_node = view_ast.Value()->Child("Query");
  if (query_node == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "CREATE VIEW: query body has no Query node");
  }
  // Issue 2553: SQL UDFs/TVFs exist only in the defining thread's registry
  // and are scoped to that session, but the view definition is cataloged and
  // re-expanded for every reader.  Capturing one leaves a permanently
  // unreadable view for every other session (and for read-worker offloads):
  // reject the capture at creation time instead.  CREATE TEMP VIEW is
  // exempt -- a TEMP view is a session object, exactly like the UDF it may
  // capture.
  if (!create.IsTemp()) {
    if (std::optional<std::string> capture =
            ast_visitor_detail::FirstSessionLocalUdfReference(*query_node);
        capture.has_value()) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "CREATE VIEW: function " + *capture +
              " is a session-local SQL UDF; a persistent view cannot "
              "reference it");
    }
  }
  // Check and persist under the catalog lock before publishing the session
  // definition; a conflicting CREATE must not shadow the existing object.
  const Status stored = database_->PutViewDefinition(
      ctx, create.ViewName(), create.QueryText(), create.OrReplace());
  if (stored == Status::kConflicts) {
    last_error_ = "view " + create.ViewName() + " already exists";
  }
  RETURN_IF_FAIL(stored);
  RegisterViewDefinition(create.ViewName(), *query_node, create.QueryText());
  database_->BumpSchemaEpoch();
  return Executor(std::make_shared<ConstantExecutor>(
      Row({Value("CREATE VIEW"), Value(0)})));
}

StatusOr<Executor> SqlEngine::ExecuteCreateObject(
    TransactionContext& /*ctx*/, const CreateObjectStatement& create) {
  // T2552: run the deferred thread_local registry write (RegisterUdf /
  // session-constant publish) only now, at execute time -- the same
  // contract CREATE VIEW follows.  Rebuilding the ConstantExecutor row
  // from the visitor's result projection keeps the original CREATE's
  // single-cell result (function: unnamed 0; constant: the name row).
  plan_cache_fingerprint_.clear();
  plan_cache_parameters_.clear();
  RETURN_IF_FAIL(create.RunSideEffect());
  std::vector<Value> cells;
  result_column_names_.clear();
  for (const NamedExpression& item : create.ResultProjection()) {
    Value cell;
    if (item.expression && item.expression->Type() == TypeTag::kConstantValue) {
      cell = item.expression->AsConstantValue().GetValue();
    }
    result_column_names_.push_back(item.name);
    cells.push_back(std::move(cell));
  }
  return Executor(std::make_shared<ConstantExecutor>(Row(std::move(cells))));
}

StatusOr<Executor> SqlEngine::ExecuteDropView(TransactionContext& ctx,
                                              const DropViewStatement& drop) {
  last_dml_table_ = drop.ViewName();
  plan_cache_fingerprint_.clear();
  plan_cache_parameters_.clear();
  StatusOr<std::string> definition =
      database_->GetViewDefinition(ctx, drop.ViewName());
  if (!definition.HasValue()) {
    if (drop.IfExists() && definition.GetStatus() == Status::kNotExists) {
      return Executor(std::make_shared<ConstantExecutor>(
          Row({Value("DROP VIEW"), Value(0)})));
    }
    last_error_ = "view " + drop.ViewName() + " does not exist";
    return Status::kNotExists;
  }
  RETURN_IF_FAIL(database_->DeleteViewDefinition(ctx, drop.ViewName()));
  std::string dropped_name = drop.ViewName();
  for (char& c : dropped_name) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  ViewRegistry().erase(dropped_name);
  database_->BumpSchemaEpoch();
  return Executor(
      std::make_shared<ConstantExecutor>(Row({Value("DROP VIEW"), Value(0)})));
}

StatusOr<Executor> SqlEngine::ExecuteCreateIndex(
    TransactionContext& ctx, const CreateIndexStatement& create) {
  ASSIGN_OR_RETURN(std::shared_ptr<Table>, table,
                   ctx.GetTable(create.TableName()));
  const auto lowercase = [](std::string text) {
    for (char& c : text) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return text;
  };
  for (size_t i = 0; i < table->IndexCount(); ++i) {
    if (lowercase(table->GetIndex(i).sc_.name_) == lowercase(create.Name())) {
      if (!create.IfNotExists()) {
        return StatusError(StatusCode::kDuplicates,
                           "index already exists: " + create.Name());
      }
      return Executor(std::make_shared<ConstantExecutor>(
          Row({Value("CREATE INDEX"), Value(0)})));
    }
  }
  std::vector<slot_t> keys, storing;
  std::unordered_set<slot_t> used_keys, used_storing;
  const auto resolve = [&](const std::vector<std::string>& names,
                           std::vector<slot_t>* slots,
                           std::unordered_set<slot_t>& used) -> Status {
    for (const auto& name : names) {
      const int offset = table->GetSchema().Offset(ColumnName("", name));
      if (offset < 0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "unknown index column: " + name);
      }
      if (!used.insert(static_cast<slot_t>(offset)).second) {
        return StatusError(StatusCode::kInvalidArgument,
                           "repeated index column: " + name);
      }
      slots->push_back(static_cast<slot_t>(offset));
    }
    return Status::kSuccess;
  };
  RETURN_IF_FAIL(resolve(create.Keys(), &keys, used_keys));
  RETURN_IF_FAIL(resolve(create.Storing(), &storing, used_storing));
  RETURN_IF_FAIL(database_->CreateIndex(
      ctx, create.TableName(),
      IndexSchema(
          create.Name(), std::move(keys), std::move(storing),
          create.Unique() ? IndexMode::kUnique : IndexMode::kNonUnique)));
  return Executor(std::make_shared<ConstantExecutor>(
      Row({Value("CREATE INDEX"), Value(0)})));
}

StatusOr<Executor> SqlEngine::ExecuteAlterTable(
    TransactionContext& ctx, const AlterTableStatement& alter) {
  const AlterTableAction& action = alter.Action();
  switch (action.kind) {
    case AlterTableAction::Kind::kRenameTo: {
      RETURN_IF_FAIL(
          database_->RenameTable(ctx, alter.TableName(), action.new_name));
      ctx.EraseCachedRelation(alter.TableName());
      return Executor(std::make_shared<ConstantExecutor>(
          Row({Value("ALTER TABLE"), Value(0)})));
    }
    case AlterTableAction::Kind::kRenameColumn: {
      RETURN_IF_FAIL(database_->RenameColumn(
          ctx, alter.TableName(), action.old_column, action.new_name));
      ctx.EraseCachedRelation(alter.TableName());
      return Executor(std::make_shared<ConstantExecutor>(
          Row({Value("ALTER TABLE"), Value(0)})));
    }
  }
  last_error_ = "unsupported ALTER TABLE action";
  return Status::kNotImplemented;
}

StatusOr<Executor> SqlEngine::ExecuteExplain(
    TransactionContext& ctx, std::string_view /*sql*/,
    const ExplainRequest& explain, std::unique_ptr<Statement> statement) {
  if (statement->Type() != StatementType::kSelect) {
    last_error_ = "EXPLAIN currently supports SELECT and WITH queries";
    return Status::kNotImplemented;
  }
  last_statement_type_ = StatementType::kSelect;
  const auto* explain_select = dynamic_cast<const SelectStatement*>(
      statement
          .get());  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
  // CTE names never resolve to catalog tables (materialized CTEs scan shared
  // row cells), so exclude them by name from the statement's own WITH list.
  // Enumerating database_->ListTables() instead was O(all tables), depended on
  // database_ being present, and missed tables created in this transaction.
  std::unordered_set<std::string> explain_cte_names;
  for (const auto& entry : explain_select->WithQueries()) {
    explain_cte_names.insert(AsciiToLower(entry.first));
  }
  std::vector<std::string> explain_base_tables;
  bool explain_has_inner_join_source = false;
  for (const SelectSource& source : explain_select->Sources()) {
    if (source.table.empty() || source.unnest || source.query) {
      continue;
    }
    if (source.join_type == JoinType::kLeft ||
        source.join_type == JoinType::kRight ||
        source.join_type == JoinType::kFull) {
      continue;
    }
    if (explain_cte_names.contains(AsciiToLower(source.table))) {
      continue;
    }
    // Resolve through the transaction context: it sees tables created (but
    // not yet committed) in this transaction and needs no catalog handle.
    ASSIGN_OR_RETURN(bool, exists, ctx.TableExists(source.table));
    if (!exists) {
      continue;
    }
    if (source.join_type == JoinType::kInner) {
      explain_has_inner_join_source = true;
    }
    // Explain() renders the FROM alias, not the physical name, so track the
    // name that can actually appear in the plan body.
    explain_base_tables.push_back(source.alias.empty() ? source.table
                                                       : source.alias);
  }
  OocQueryMetrics ooc_metrics;
  const auto planning_start = std::chrono::steady_clock::now();
  StatusOr<Executor> prepared(Status::kUnknown);
  {
    OocQueryScope ooc_scope(&ooc_metrics);
    // Planning is part of EXPLAIN ANALYZE's statement budget: without a
    // deadline guard here, a complex plan could consume the whole timeout
    // before the drain loop below ever checks it.
    Transaction::ScopedThreadStatementDeadline deadline_guard(
        ctx.txn_.StatementDeadlineNs());
    prepared = PrepareStatement(ctx, std::move(statement));
    if (ctx.txn_.HasStatementTimeout()) {
      RETURN_IF_FAIL(ctx.txn_.CheckStatementTimeout());
    }
  }
  const auto planning_end = std::chrono::steady_clock::now();
  if (!prepared.HasValue()) {
    return prepared.GetStatus();
  }

  uint64_t rows = 0;
  std::chrono::steady_clock::time_point execution_end = planning_end;
  uint64_t initial_hits = 0;
  uint64_t initial_misses = 0;
  PageManager* pm = ctx.txn_.GetPageManager();
  if (pm != nullptr) {
    initial_hits = pm->CacheHits();
    initial_misses = pm->CacheMisses();
  } else if (database_ != nullptr) {
    initial_hits = database_->CacheHits();
    initial_misses = database_->CacheMisses();
  }
  if (explain.analyze) {
    OocQueryScope ooc_scope(&ooc_metrics);
    // P440: the EXPLAIN path returns a raw Executor and never passes through
    // FinishPrepareWithTimeout, so this drain loop used to run the full
    // statement with neither timing nor deadline checks.  Mirror the
    // StatementTimeoutExecutor wrapper: publish the thread-local deadline for
    // deadline-aware operators and check the statement timeout between
    // batches.
    Transaction::ScopedThreadStatementDeadline deadline_guard(
        ctx.txn_.StatementDeadlineNs());
    DataChunk chunk;
    while (prepared.Value()->NextBatch(&chunk) != 0) {
      rows += chunk.Size();
      if (ctx.txn_.HasStatementTimeout()) {
        RETURN_IF_FAIL(ctx.txn_.CheckStatementTimeout());
      }
    }
    // A scan that failed mid-drain must fail the EXPLAIN ANALYZE too;
    // reporting runtime stats for a truncated execution misleads plans.
    RETURN_IF_FAIL(prepared.Value()->GetStatus());
    execution_end = std::chrono::steady_clock::now();
  }
  std::ostringstream output;
  prepared.Value()->Explain(output, 0);
  std::string body = output.str();
  // SQL identifiers are case-insensitive and plan renderings may normalize
  // table names, so compare in lower case when looking for survivors.
  std::string body_lower = AsciiToLower(body);
  if (explain.pipeline) {
    const ExecutorBase* root_exec = prepared.Value().get();
    if (const auto* wrapped =
            dynamic_cast<const PlannedExplainExecutor*>(root_exec)) {
      root_exec = wrapped->Inner().get();
    }
    if (const auto* pipe = dynamic_cast<const PipelineExecutor*>(root_exec)) {
      output << "\n" << pipe->ExplainPipeline();
      // The join-elimination annotation below must look at the complete
      // rendering: a table may be named only in the pipeline explanation.
      body = output.str();
      body_lower = AsciiToLower(body);
    }
  }
  // An INNER-join FROM relation that vanished from a non-empty plan was
  // removed by provably-unused foreign-key join elimination. LEFT-join
  // elimination and full contradiction (EmptyResult) are left unannotated so
  // they keep the "no Join token" plan shape the corpus expects.
  const bool plan_scans_something =
      body.find("FullScan") != std::string::npos ||
      body.find("SeqScan") != std::string::npos ||
      body.find("IndexScan") != std::string::npos ||
      body.find("IndexOnlyScan") != std::string::npos;
  if (explain_has_inner_join_source && plan_scans_something) {
    // A plain substring match confuses `t` with `t2`/`tmp`/`list`; require a
    // whole-identifier match (alphanumeric/underscore boundaries) instead.
    const auto identifier_present = [](std::string_view haystack,
                                       std::string_view identifier) {
      if (identifier.empty()) {
        return false;
      }
      const auto is_ident_char = [](char c) {
        // '$' is a legal identifier byte (same set as the frontend's
        // IsIdentChar); omitting it makes "$t"/"t$col" match "t".
        return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_' ||
               c == '$';
      };
      size_t pos = 0;
      while ((pos = haystack.find(identifier, pos)) != std::string_view::npos) {
        const bool left_ok = pos == 0 || !is_ident_char(haystack[pos - 1]);
        const size_t end = pos + identifier.size();
        const bool right_ok =
            end == haystack.size() || !is_ident_char(haystack[end]);
        if (left_ok && right_ok) {
          return true;
        }
        ++pos;
      }
      return false;
    };
    bool any_survives = false;
    bool any_eliminated = false;
    for (const std::string& table : explain_base_tables) {
      if (identifier_present(body_lower, AsciiToLower(table))) {
        any_survives = true;
      } else {
        any_eliminated = true;
      }
    }
    if (any_eliminated && any_survives) {
      output << "\nJoinElimination";
    }
  }
  if (explain.analyze) {
    output << "\nRuntime: ";
    prepared.Value()->Dump(output, 0);
  }
  const double planning_ms =
      std::chrono::duration<double, std::milli>(planning_end - planning_start)
          .count();
  output << "\nPlanning Time: " << planning_ms << " ms";
  if (explain.analyze) {
    const double execution_ms =
        std::chrono::duration<double, std::milli>(execution_end - planning_end)
            .count();
    output << "\nActual Rows: " << rows << "\nExecution Time: " << execution_ms
           << " ms";
    if (pm != nullptr) {
      const uint64_t hits = pm->CacheHits() - initial_hits;
      const uint64_t misses = pm->CacheMisses() - initial_misses;
      output << "\nBuffer Pool: " << hits << " hits, " << misses << " misses";
    } else if (database_ != nullptr) {
      const uint64_t hits = database_->CacheHits() - initial_hits;
      const uint64_t misses = database_->CacheMisses() - initial_misses;
      output << "\nBuffer Pool: " << hits << " hits, " << misses << " misses";
    }
    const ProcessMemoryBudget& mem = ProcessMemoryBudget::Global();
    const uint64_t rss = platform::CurrentProcessRssBytes();
    const uint64_t total_tracked = mem.TotalUsed();
    const uint64_t unaccounted = rss > total_tracked ? rss - total_tracked : 0;
    const OocTelemetrySnapshot ooc = ooc_metrics.Snapshot();
    output << "\nMemory: tracked=" << total_tracked
           << "B peak_tracked=" << mem.TotalPeak() << "B rss=" << rss
           << "B unaccounted_rss=" << unaccounted
           << "B pools(storage=" << mem.StorageUsed()
           << "B, columnar=" << mem.ColumnarUsed()
           << "B, query=" << mem.QueryUsed() << "B, tx=" << mem.TxUsed()
           << "B)";
    output << "\nOOC: op_peak=" << ooc.op_peak_bytes
           << "B spill_bytes=" << ooc.spill_bytes
           << "B spill_ops=" << ooc.spill_ops
           << " max_depth=" << ooc.max_partition_depth
           << " skew_fallbacks=" << ooc.skew_fallbacks
           << " reclaim_calls=" << ooc.reclaim_calls;
  }
  result_column_names_ = {"QUERY PLAN"};
  return Executor(
      std::make_shared<ConstantExecutor>(ExplainRows(output.str())));
}

}  // namespace tinylamb
