/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXPRESSION_SQL_UDF_HPP
#define TINYLAMB_EXPRESSION_SQL_UDF_HPP

#include <optional>
#include <string>
#include <vector>

#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

// A materialized SQL scalar UDF created by
//   CREATE [TEMP] FUNCTION name(p1 [, p2 ...]) [RETURNS T] AS (body)
// Parameter references inside `body` are ColumnValue nodes whose column name
// matches a parameter name; invocation evaluates the arguments once, binds the
// resulting values to a synthetic single-row scope and evaluates the body
// against it.  This gives GoogleSQL call semantics: every argument expression
// is evaluated exactly once per invocation, so volatile calls (RAND()) inside
// an argument observe one value at all reference sites.
struct SqlScalarFunction {
  std::string name;                 // lower-cased function name
  std::vector<std::string> params;  // lower-cased parameter names
  // Parallel to `params`; null entries are required parameters, non-null
  // entries are DEFAULT expressions evaluated when the call omits them.
  std::vector<Expression> defaults;
  Expression body;

  [[nodiscard]] size_t RequiredArgs() const {
    size_t required = 0;
    for (const Expression& default_value : defaults) {
      if (!default_value) {
        ++required;
      }
    }
    return required;
  }
};

// Registers or replaces a scalar SQL UDF definition.  Thread-safe.
void RegisterSqlScalarFunction(SqlScalarFunction function);

// Looks a scalar SQL UDF up by lower-cased name.  Thread-safe; returns a copy
// so callers never hold references into the registry across evaluations.
[[nodiscard]] std::optional<SqlScalarFunction> FindSqlScalarFunction(
    std::string_view lower_name);

// Binds evaluated argument values to the parameter names of `function`,
// producing the synthetic single-row scope the body evaluates against.
// Trailing parameters with DEFAULT expressions are padded; wrong argument
// counts raise std::runtime_error.
struct SqlUdfBinding {
  Row row;
  Schema schema;
};

[[nodiscard]] SqlUdfBinding BindSqlUdfArguments(
    const SqlScalarFunction& function, std::vector<Value> arguments);

// RAII invocation-depth accounting shared by every evaluation path so a
// recursive UDF fails with a diagnostic instead of exhausting the C++ stack.
class SqlUdfDepthGuard {
 public:
  SqlUdfDepthGuard();
  ~SqlUdfDepthGuard();
  SqlUdfDepthGuard(const SqlUdfDepthGuard&) = delete;
  SqlUdfDepthGuard& operator=(const SqlUdfDepthGuard&) = delete;
  SqlUdfDepthGuard(SqlUdfDepthGuard&&) = delete;
  SqlUdfDepthGuard& operator=(SqlUdfDepthGuard&&) = delete;

  [[nodiscard]] static int CurrentDepth();
};

// Encodes one struct value as the engine's canonical struct text: a JSON
// object with one entry per field.  Shared by the AST evaluator and the
// relational interpreter so both produce byte-identical output for the
// deferred STRUCT(...) constructor (__struct_json__).
[[nodiscard]] std::string EncodeStructJson(
    const std::vector<std::pair<std::string, Value>>& fields);

}  // namespace tinylamb

#endif  // TINYLAMB_EXPRESSION_SQL_UDF_HPP
