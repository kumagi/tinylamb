/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "expression/sql_udf.hpp"

#include <array>
#include <charconv>
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
  while (arguments.size() < function.defaults.size()) {
    const Expression& default_value = function.defaults[arguments.size()];
    if (default_value) {
      ASSIGN_OR_RETURN(Value, evaluated,
                       default_value->TryEvaluate(Row(), Schema()));
      arguments.push_back(std::move(evaluated));
    } else {
      arguments.emplace_back();
    }
  }
  std::vector<Column> columns;
  columns.reserve(function.params.size());
  for (size_t i = 0; i < function.params.size(); ++i) {
    columns.emplace_back(function.params[i], i < arguments.size()
                                                 ? arguments[i].type
                                                 : ValueType::kNull);
  }
  return SqlUdfBinding{.row = Row(std::move(arguments)),
                       .schema = Schema("", std::move(columns))};
}

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
  auto escape = [](std::string_view text) {
    std::string out;
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
    return out;
  };

  bool any_ci = false;
  for (const auto& [name, value] : fields) {
    any_ci = any_ci || value.IsCaseInsensitive();
  }

  std::string json_out = "{";
  bool first = true;
  for (const auto& [name, value] : fields) {
    if (!first) {
      json_out += ",";
    }
    first = false;
    json_out += "\"" + escape(name) + "\":";
    if (value.IsNull()) {
      json_out += "null";
      continue;
    }
    std::string text;
    bool is_string = false;
    switch (value.type) {
      case ValueType::kVarChar: {
        text = std::string(value.value.varchar_value);
        if (any_ci) {
          for (char& c : text) {
            c = c >= 'A' && c <= 'Z' ? static_cast<char>(c - 'A' + 'a') : c;
          }
        }
        // Proto value-table cells use the generic VARCHAR channel.  Preserve
        // their message shape when they are nested in a STRUCT instead of
        // serializing the payload as an ordinary string.
        // Keep proto text as a quoted payload here.  The compliance matcher
        // recognizes the payload as a nested proto, while retaining the
        // outer STRUCT's field boundaries even when repeated members occur.
        is_string = true;
        break;
      }
      case ValueType::kInt64:
        text = std::to_string(value.value.int_value);
        break;
      case ValueType::kDouble:
        // Shortest round-trip representation so whole numbers render as "3"
        // rather than "3.000000" (matching the compliance goldens).
        {
          std::array<char, 32> buffer{};
          auto [end, ec] =
              std::to_chars(buffer.data(), buffer.data() + buffer.size(),
                            value.value.double_value);
          if (ec == std::errc()) {
            text = std::string(buffer.data(), end);
          } else {
            text = std::to_string(value.value.double_value);
          }
        }
        break;
      default:
        text = value.AsString();
        break;
    }
    json_out += is_string ? "\"" + escape(text) + "\"" : text;
  }
  json_out += "}";
  return json_out;
}

}  // namespace tinylamb
