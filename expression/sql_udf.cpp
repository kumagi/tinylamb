/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "expression/sql_udf.hpp"

#include <array>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

#include "expression/expression.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

constexpr int kMaxUdfInvocationDepth = 64;

std::shared_mutex& RegistryMutex() {
  static std::shared_mutex mutex;
  return mutex;
}

std::unordered_map<std::string, SqlScalarFunction>& ScalarRegistry() {
  static std::unordered_map<std::string, SqlScalarFunction> registry;
  return registry;
}

thread_local int tls_udf_depth = 0;

}  // namespace

void RegisterSqlScalarFunction(SqlScalarFunction function) {
  std::unique_lock lock(RegistryMutex());
  ScalarRegistry().insert_or_assign(function.name, std::move(function));
}

std::optional<SqlScalarFunction> FindSqlScalarFunction(
    const std::string_view lower_name) {
  std::shared_lock lock(RegistryMutex());
  const auto found = ScalarRegistry().find(std::string(lower_name));
  if (found == ScalarRegistry().end()) {
    return std::nullopt;
  }
  return found->second;
}

StatusOr<SqlUdfBinding> BindSqlUdfArguments(const SqlScalarFunction& function,
                                            std::vector<Value> arguments) {
  const size_t required = function.RequiredArgs();
  if (arguments.size() < required ||
      arguments.size() > function.params.size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "function " + function.name + " expects between " +
                           std::to_string(required) + " and " +
                           std::to_string(function.params.size()) +
                           " arguments but got " +
                           std::to_string(arguments.size()));
  }
  while (arguments.size() < function.params.size()) {
    const size_t position = arguments.size();
    if (position >= function.defaults.size() || !function.defaults[position]) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "missing required argument to function " + function.name);
    }
    std::vector<Column> scope_columns;
    scope_columns.reserve(position);
    for (size_t i = 0; i < position; ++i) {
      scope_columns.emplace_back(function.params[i], arguments[i].type);
    }
    const Row scope_row(arguments);
    const Schema scope_schema("", std::move(scope_columns));
    ASSIGN_OR_RETURN(Value, evaluated,
                     function.defaults[position]->TryEvaluate(scope_row,
                                                              scope_schema));
    arguments.push_back(std::move(evaluated));
  }
  std::vector<Column> columns;
  columns.reserve(function.params.size());
  for (size_t i = 0; i < function.params.size(); ++i) {
    columns.emplace_back(function.params[i], arguments[i].type);
  }
  return SqlUdfBinding{.row = Row(std::move(arguments)),
                       .schema = Schema("", std::move(columns))};
}

// CheckAvailable() runs before construction (see the UDF path in
// function_call_expression.cpp), so tls_udf_depth counts the active *ancestor*
// invocations at check time and the Nth nested call sees N - 1.  `>=` thus
// rejects only the (kMaxUdfInvocationDepth + 1)-th call: exactly
// kMaxUdfInvocationDepth invocations are permitted.  Keep the check-before-
// increment order in mind before adjusting this comparison.
SqlUdfDepthGuard::SqlUdfDepthGuard() { ++tls_udf_depth; }

Status SqlUdfDepthGuard::CheckAvailable() {
  if (tls_udf_depth >= kMaxUdfInvocationDepth) {
    return StatusError(StatusCode::kInvalidArgument,
                       "SQL UDF invocation depth exceeds " +
                           std::to_string(kMaxUdfInvocationDepth));
  }
  return Status::kSuccess;
}

SqlUdfDepthGuard::~SqlUdfDepthGuard() { --tls_udf_depth; }

int SqlUdfDepthGuard::CurrentDepth() { return tls_udf_depth; }

std::string EncodeStructJson(
    const std::vector<std::pair<std::string, Value>>& fields) {
  auto escape_into = [](std::string& out, std::string_view text) {
    for (const char c : text) {
      if (c == '"' || c == '\\') {
        out.push_back('\\');
        out.push_back(c);
      } else if (static_cast<unsigned char>(c) < 0x20) {
        std::array<char, 8> buf{};
        // Fixed-size escape output can neither fail nor truncate.
        // NOLINTNEXTLINE(cert-err33-c)
        (void)snprintf(buf.data(), buf.size(), "\\u%04x",
                       static_cast<unsigned char>(c));
        out += buf.data();
      } else {
        out.push_back(c);
      }
    }
  };

  std::string json_out = "{";
  // This runs per row for UDF STRUCT binding / equality: pre-size the buffer
  // from the field count so the += chain does not reallocate once per field.
  constexpr size_t kEstimatedFieldBytes = 32;
  if (fields.size() <= (json_out.max_size() - 2) / kEstimatedFieldBytes) {
    json_out.reserve(2 + (fields.size() * kEstimatedFieldBytes));
  }
  bool first = true;
  for (const auto& [name, value] : fields) {
    if (!first) {
      json_out += ",";
    }
    first = false;
    json_out += '"';
    escape_into(json_out, name);
    json_out += "\":";
    if (value.IsNull()) {
      json_out += "null";
      continue;
    }
    std::string text;
    bool is_string = false;
    switch (value.type) {
      case ValueType::kVarChar: {
        text = std::string(value.value.varchar_value);
        // Fold only fields that actually carry a case-insensitive tag;
        // a CI sibling field must not change a binary field's text.
        if (value.IsCaseInsensitive()) {
          text = FoldCaseForCollation(text);
        }
        // Proto value-table cells use the generic VARCHAR channel.  Keep
        // proto text as a quoted payload here: the compliance matcher
        // recognizes the payload as a nested proto, while retaining the
        // outer STRUCT's field boundaries even when repeated members occur.
        is_string = true;
        break;
      }
      case ValueType::kInt64:
        // AsString() respects the unsigned tag (UINT64_MAX must not fold to
        // "-1" in a STRUCT literal's text form).
        text = value.AsString();
        break;
      case ValueType::kDouble:
        // Shortest round-trip representation so whole numbers render as "3"
        // rather than "3.000000" (matching the compliance goldens).
        {
          const double d = value.value.double_value;
          // JSON has no NaN/Infinity literals and std::to_chars emits bare
          // "nan"/"inf"/"-inf". Quote the canonical spellings instead, the
          // same way json_text::ToJsonText and TryEncodeStructMemberJson do,
          // so a STRUCT's canonical text stays parseable JSON.
          if (std::isnan(d)) {
            text = "NaN";
            is_string = true;
          } else if (std::isinf(d)) {
            text = d > 0 ? "Infinity" : "-Infinity";
            is_string = true;
          } else {
            std::array<char, 32> buffer{};
            auto [end, ec] =
                std::to_chars(buffer.data(), buffer.data() + buffer.size(), d);
            if (ec == std::errc()) {
              text = std::string(buffer.data(), end);
            } else {
              text = std::to_string(d);
            }
          }
        }
        break;
      case ValueType::kDate:
        text = value.AsString();
        is_string = true;
        break;
      default:
        text = value.AsString();
        break;
    }
    if (is_string) {
      json_out += '"';
      escape_into(json_out, text);
      json_out += '"';
    } else {
      json_out += text;
    }
  }
  json_out += "}";
  return json_out;
}

}  // namespace tinylamb
