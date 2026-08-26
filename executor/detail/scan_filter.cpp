/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/scan_filter.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "query/statement.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {

BinaryOperation FlipCompare(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kLessThan:
      return BinaryOperation::kGreaterThan;
    case BinaryOperation::kLessThanEquals:
      return BinaryOperation::kGreaterThanEquals;
    case BinaryOperation::kGreaterThan:
      return BinaryOperation::kLessThan;
    case BinaryOperation::kGreaterThanEquals:
      return BinaryOperation::kLessThanEquals;
    default:
      return operation;
  }
}

}  // namespace

bool MatchSimpleCompare(const Row& row, const SimpleComparePredicate& pred) {
  const Value& value = row[pred.column];
  if (value.IsNull() || pred.constant.IsNull()) {
    return false;
  }

  if (pred.int_payload &&
      (value.type == ValueType::kInt64 || value.type == ValueType::kDate)) {
    const int64_t left = value.value.int_value;
    const int64_t right = pred.int_constant;
    switch (pred.op) {
      case BinaryOperation::kEquals:
        return left == right;
      case BinaryOperation::kNotEquals:
        return left != right;
      case BinaryOperation::kLessThan:
        return left < right;
      case BinaryOperation::kLessThanEquals:
        return left <= right;
      case BinaryOperation::kGreaterThan:
        return left > right;
      case BinaryOperation::kGreaterThanEquals:
        return left >= right;
      default:
        return false;
    }
  }
  if (pred.double_payload && value.type == ValueType::kDouble) {
    const double left = value.value.double_value;
    const double right = pred.double_constant;
    switch (pred.op) {
      case BinaryOperation::kEquals:
        return left == right;
      case BinaryOperation::kNotEquals:
        return left != right;
      case BinaryOperation::kLessThan:
        return left < right;
      case BinaryOperation::kLessThanEquals:
        return left <= right;
      case BinaryOperation::kGreaterThan:
        return left > right;
      case BinaryOperation::kGreaterThanEquals:
        return left >= right;
      default:
        return false;
    }
  }

  const auto as_double = [](const Value& v) -> std::optional<double> {
    if (v.type == ValueType::kDouble) {
      return v.value.double_value;
    }
    if (v.type == ValueType::kInt64 || v.type == ValueType::kDate) {
      return static_cast<double>(v.value.int_value);
    }
    return std::nullopt;
  };
  const auto as_int = [](const Value& v) -> std::optional<int64_t> {
    if (v.type == ValueType::kInt64 || v.type == ValueType::kDate) {
      return v.value.int_value;
    }
    return std::nullopt;
  };

  switch (pred.op) {
    case BinaryOperation::kEquals: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value == pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li == *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld == *rd;
      }
      return false;
    }
    case BinaryOperation::kNotEquals: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value != pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li != *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld != *rd;
      }
      return false;
    }
    case BinaryOperation::kLessThan: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value < pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li < *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld < *rd;
      }
      return false;
    }
    case BinaryOperation::kLessThanEquals: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value <= pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li <= *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld <= *rd;
      }
      return false;
    }
    case BinaryOperation::kGreaterThan: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value > pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li > *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld > *rd;
      }
      return false;
    }
    case BinaryOperation::kGreaterThanEquals: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value >= pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li >= *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld >= *rd;
      }
      return false;
    }
    default:
      return false;
  }
}

std::optional<SimpleComparePredicate> TryCompileSimpleCompare(
    const Expression& predicate, const Schema& schema) {
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
    return std::nullopt;
  }
  Expression folded =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(predicate);
  if (!folded || folded->Type() != TypeTag::kBinaryExp) {
    return std::nullopt;
  }
  const BinaryExpression& binary = folded->AsBinaryExpression();
  if (!IsComparison(binary.Op())) {
    return std::nullopt;
  }
  Expression column = binary.Left();
  Expression constant = binary.Right();
  BinaryOperation op = binary.Op();
  if (column->Type() == TypeTag::kConstantValue &&
      constant->Type() == TypeTag::kColumnValue) {
    std::swap(column, constant);
    op = FlipCompare(op);
  }
  if (column->Type() != TypeTag::kColumnValue ||
      constant->Type() != TypeTag::kConstantValue) {
    return std::nullopt;
  }
  const auto offset =
      LocalColumnOffset(schema, column->AsColumnValue().GetColumnName());
  if (!offset) {
    return std::nullopt;
  }
  SimpleComparePredicate compiled;
  compiled.column = static_cast<slot_t>(*offset);
  compiled.op = op;
  compiled.constant = constant->AsConstantValue().GetValue();
  if (compiled.constant.type == ValueType::kInt64 ||
      compiled.constant.type == ValueType::kDate) {
    compiled.int_payload = true;
    compiled.int_constant = compiled.constant.value.int_value;
  } else if (compiled.constant.type == ValueType::kDouble) {
    compiled.double_payload = true;
    compiled.double_constant = compiled.constant.value.double_value;
  }
  return compiled;
}

CompiledScanFilter CompileScanFilter(const std::vector<Expression>& predicates,
                                     const Schema& schema) {
  CompiledScanFilter compiled;
  for (const Expression& predicate : predicates) {
    if (auto simple = TryCompileSimpleCompare(predicate, schema)) {
      compiled.simple.push_back(std::move(*simple));
    } else {
      compiled.residual.push_back(predicate);
    }
  }
  return compiled;
}

bool MatchScanFilter(const Row& row, const Schema& schema,
                     const CompiledScanFilter& filter, const Scope* outer,
                     TransactionContext& context, const CteMap& ctes) {
  for (const SimpleComparePredicate& pred : filter.simple) {
    if (!MatchSimpleCompare(row, pred)) {
      return false;
    }
  }
  if (filter.residual.empty()) {
    return true;
  }
  Scope scope{.row = &row, .schema = &schema, .outer = outer};
  for (const Expression& predicate : filter.residual) {
    if (!Truthy(Evaluate(predicate, scope, nullptr, context, ctes))) {
      return false;
    }
  }
  return true;
}

std::vector<IntegerPeekCompare> BuildIntegerPeeks(
    const CompiledScanFilter& filter, const std::vector<slot_t>* projection,
    const Schema& full_schema) {
  std::vector<IntegerPeekCompare> peeks;
  peeks.reserve(filter.simple.size());
  for (const SimpleComparePredicate& pred : filter.simple) {
    if (!pred.int_payload) {
      continue;
    }
    IntegerPeekCompare peek;
    if (projection != nullptr) {
      if (pred.column >= projection->size()) {
        continue;
      }
      peek.column = (*projection)[pred.column];
    } else {
      peek.column = pred.column;
    }
    if (peek.column >= full_schema.ColumnCount()) {
      continue;
    }
    const ValueType type = full_schema.GetColumn(peek.column).Type();
    if (type != ValueType::kInt64 && type != ValueType::kDate) {
      continue;
    }
    peek.op = pred.op;
    peek.constant = pred.int_constant;
    peeks.push_back(peek);
  }
  return peeks;
}

bool TryParallelTableScan(TransactionContext& context, Table& table,
                          const std::vector<slot_t>* projection,
                          const std::unordered_set<int64_t>* key_filter,
                          std::optional<slot_t> full_key_column,
                          bool filter_during_scan,
                          const CompiledScanFilter* scan_filter,
                          const Schema& result_schema, const Scope* outer,
                          const CteMap& ctes, Relation* result) {
  std::vector<Table::ScanMorsel> morsels =
      table.BuildScanMorsels(context.txn_, 8);
  const size_t workers =
      std::min(static_cast<size_t>(std::thread::hardware_concurrency()),
               std::max<size_t>(1, morsels.size()));
  if (workers <= 1 || morsels.size() < 8) {
    return false;
  }

  std::atomic<size_t> next_morsel{0};
  // Relation (not raw vector) so worker-side rows charge QueryMemoryBudget
  // and spill instead of bypassing the memory contract until the join drains.
  std::vector<Relation> shards(workers);
  std::vector<size_t> shard_seen(workers, 0);
  std::vector<size_t> shard_out(workers, 0);
  std::mutex error_mu;
  std::exception_ptr error;
  std::optional<std::vector<slot_t>> proj_opt;
  if (projection != nullptr) {
    proj_opt = *projection;
  }

  {
    std::vector<std::jthread> threads;
    threads.reserve(workers);
    for (size_t w = 0; w < workers; ++w) {
      threads.emplace_back([&, w] {
        try {
          auto& local = shards[w];
          while (true) {
            const size_t mi = next_morsel.fetch_add(1);
            if (mi >= morsels.size()) {
              break;
            }
            Iterator iterator =
                table.BeginMorselScan(context.txn_, morsels[mi], proj_opt,
                                      key_filter, full_key_column);
            while (iterator.IsValid()) {
              ++shard_seen[w];
              bool matches = true;
              if (filter_during_scan && scan_filter) {
                // Worker threads only evaluate the simple (constant-vs-column)
                // predicates. Residual predicates may contain query
                // expressions whose evaluation touches the shared
                // TransactionContext and its explicit execution runtime,
                // neither of which is safe on worker threads.
                for (const SimpleComparePredicate& pred : scan_filter->simple) {
                  if (!MatchSimpleCompare(*iterator, pred)) {
                    matches = false;
                    break;
                  }
                }
              }
              if (matches) {
                local.AddRow(*iterator);
                ++shard_out[w];
              }
              ++iterator;
            }
          }
        } catch (...) {
          std::scoped_lock lock(error_mu);
          if (!error) {
            error = std::current_exception();
          }
        }
      });
    }
  }
  if (error) {
    std::rethrow_exception(error);
  }
  if (filter_during_scan && scan_filter != nullptr &&
      !scan_filter->residual.empty()) {
    // Re-apply residual predicates on the main thread.
    CompiledScanFilter residual_only;
    residual_only.residual = scan_filter->residual;
    for (size_t w = 0; w < workers; ++w) {
      shards[w].FinishSpill();
      shards[w].ForEachRow([&](const Row& row) {
        if (!MatchScanFilter(row, result_schema, residual_only, outer, context,
                             ctes)) {
          return;
        }
        result->AddRow(row);
      });
      shards[w].ResetContents();
    }
  } else {
    for (size_t w = 0; w < workers; ++w) {
      shards[w].FinishSpill();
      shards[w].ForEachRow([&](const Row& row) { result->AddRow(row); });
      shards[w].ResetContents();
    }
  }
  for (size_t w = 0; w < workers; ++w) {
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->scan_rows += shard_seen[w];
      context.execution_runtime()->scan_values_available +=
          shard_seen[w] * table.GetSchema().ColumnCount();
      context.execution_runtime()->scan_values_decoded +=
          shard_out[w] * result_schema.ColumnCount();
      context.execution_runtime()->scan_output_rows += shard_out[w];
    }
  }
  return true;
}
Relation UnnestValueToRelation(const SelectSource& source,
                               const Value& array_val) {
  Relation result;
  std::string col_name = source.alias.empty() ? "unnest" : source.alias;
  ValueType elem_type = ValueType::kNull;
  std::vector<Value> elements;
  if (array_val.IsArray()) {
    elements = array_val.ArrayElements();
    if (!elements.empty()) {
      elem_type = elements[0].type;
    }
  } else if (!array_val.IsNull()) {
    elements.push_back(array_val);
    elem_type = array_val.type;
  }
  const std::string elem_sql_type =
      array_val.IsArray() ? array_val.ArrayElementSqlType() : "";
  if (elem_sql_type == "BOOL" || elem_sql_type == "BOOLEAN") {
    elem_type = ValueType::kVarChar;
    for (auto& elem : elements) {
      if (!elem.IsNull()) {
        elem =
            Value(std::string(elem.value.int_value != 0 ? "true" : "false"));
      }
    }
  } else if (elem_sql_type == "PROTO") {
    std::vector<Column> cols;
    cols.emplace_back(col_name, ValueType::kVarChar);
    std::vector<std::vector<Value>> field_values(elements.size());
    for (size_t row_idx = 0; row_idx < elements.size(); ++row_idx) {
      if (!elements[row_idx].IsNull() &&
          elements[row_idx].type == ValueType::kVarChar) {
        const std::string text =
            std::string(elements[row_idx].value.varchar_value);
        std::istringstream iss(text);
        std::string field_name_colon, val_str;
        while (iss >> field_name_colon >> val_str) {
          if (field_name_colon.ends_with(':')) {
            std::string f_name =
                field_name_colon.substr(0, field_name_colon.size() - 1);
            if (row_idx == 0) {
              cols.emplace_back(col_name + "." + f_name, ValueType::kInt64);
            }
            int64_t v = 0;
            try {
              v = std::stoll(val_str);
            } catch (...) {
            }
            field_values[row_idx].push_back(Value(v));
          }
        }
      }
    }
    result.schema = Schema("", std::move(cols));
    for (size_t row_idx = 0; row_idx < elements.size(); ++row_idx) {
      std::vector<Value> row_vals;
      row_vals.push_back(std::move(elements[row_idx]));
      for (auto& fv : field_values[row_idx]) {
        row_vals.push_back(std::move(fv));
      }
      result.AddRow(Row(std::move(row_vals)));
    }
    return result;
  }
  bool is_struct_json = !elements.empty() &&
                        elements[0].type == ValueType::kVarChar &&
                        !elements[0].value.varchar_value.empty() &&
                        elements[0].value.varchar_value.front() == '{' &&
                        elements[0].value.varchar_value.back() == '}';
  if (is_struct_json) {
    auto parse_json_obj = [](std::string_view json) {
      std::vector<std::pair<std::string, Value>> fields;
      if (json.size() < 2 || json.front() != '{' || json.back() != '}') {
        return fields;
      }
      std::string_view inner = json.substr(1, json.size() - 2);
      int depth = 0;
      bool in_string = false;
      size_t start = 0;
      std::vector<std::string_view> pairs;
      for (size_t i = 0; i < inner.size(); ++i) {
        char c = inner[i];
        if (in_string) {
          if (c == '\\' && i + 1 < inner.size()) {
            ++i;
            continue;
          }
          if (c == '"') {
            in_string = false;
          }
          continue;
        }
        if (c == '"') {
          in_string = true;
          continue;
        }
        if (c == '{' || c == '[' || c == '(') {
          ++depth;
          continue;
        }
        if (c == '}' || c == ']' || c == ')') {
          if (depth > 0) --depth;
          continue;
        }
        if (c == ',' && depth == 0) {
          pairs.push_back(inner.substr(start, i - start));
          start = i + 1;
        }
      }
      if (start < inner.size()) {
        pairs.push_back(inner.substr(start));
      }
      for (auto p : pairs) {
        while (!p.empty() &&
               std::isspace(static_cast<unsigned char>(p.front()))) {
          p.remove_prefix(1);
        }
        while (!p.empty() &&
               std::isspace(static_cast<unsigned char>(p.back()))) {
          p.remove_suffix(1);
        }
        size_t colon = p.find(':');
        if (colon == std::string_view::npos) {
          continue;
        }
        std::string_view k = p.substr(0, colon);
        std::string_view v = p.substr(colon + 1);
        while (!k.empty() &&
               (k.front() == '"' ||
                std::isspace(static_cast<unsigned char>(k.front())))) {
          k.remove_prefix(1);
        }
        while (!k.empty() &&
               (k.back() == '"' ||
                std::isspace(static_cast<unsigned char>(k.back())))) {
          k.remove_suffix(1);
        }
        while (!v.empty() &&
               std::isspace(static_cast<unsigned char>(v.front()))) {
          v.remove_prefix(1);
        }
        while (!v.empty() &&
               std::isspace(static_cast<unsigned char>(v.back()))) {
          v.remove_suffix(1);
        }
        auto decode_array_text = [](std::string_view body) -> Value {
        // body excludes the outer brackets.  Elements are scalars or nested
        // struct objects; strings are double-quoted.
        auto scalar_token = [](std::string_view t) -> Value {
          while (!t.empty() &&
                 std::isspace(static_cast<unsigned char>(t.front()))) {
            t.remove_prefix(1);
          }
          while (!t.empty() &&
                 std::isspace(static_cast<unsigned char>(t.back()))) {
            t.remove_suffix(1);
          }
          if (t.empty() || t == "null") {
            return {};
          }
          if (t == "true") {
            return Value(int64_t{1});
          }
          if (t == "false") {
            return Value(int64_t{0});
          }
          if (t.front() == '"' && t.back() == '"' && t.size() >= 2) {
            return Value(std::string(t.substr(1, t.size() - 2)));
          }
          if (t.front() == '{' && t.back() == '}') {
            return Value(std::string(t));  // nested struct stays encoded
          }
          int64_t ival = 0;
          auto [ptr, ec] = std::from_chars(t.data(), t.data() + t.size(), ival);
          if (ec == std::errc() && ptr == t.data() + t.size()) {
            return Value(ival);
          }
          double dval = 0.0;
          try {
            dval = std::stod(std::string(t));
            return Value(dval);
          } catch (...) {
            return Value(std::string(t));
          }
        };
        std::vector<Value> elements;
        int nest = 0;
        bool str = false;
        size_t start = 0;
        for (size_t i = 0; i < body.size(); ++i) {
          char c = body[i];
          if (str) {
            if (c == '\\' && i + 1 < body.size()) {
              ++i;
              continue;
            }
            if (c == '"') {
              str = false;
            }
            continue;
          }
          if (c == '"') {
            str = true;
            continue;
          }
          if (c == '{' || c == '[' || c == '(') {
            ++nest;
            continue;
          }
          if (c == '}' || c == ']' || c == ')') {
            if (nest > 0) {
              --nest;
            }
            continue;
          }
          if (c == ',' && nest == 0) {
            elements.push_back(scalar_token(body.substr(start, i - start)));
            start = i + 1;
          }
        }
        if (!body.empty()) {
          elements.push_back(scalar_token(body.substr(start)));
        }
        std::string elem_sql;
        for (const Value& element : elements) {
          if (!element.IsNull()) {
            switch (element.type) {
              case ValueType::kInt64:
                elem_sql = "INT64";
                break;
              case ValueType::kDouble:
                elem_sql = "DOUBLE";
                break;
              case ValueType::kVarChar:
                elem_sql = "STRING";
                break;
              default:
                elem_sql = {};
                break;
            }
            break;
          }
        }
        return Value::Array(std::move(elements),
                            elem_sql.empty() ? "INT64" : elem_sql);
      };
      std::string_view trimmed = v;
      if (trimmed.size() > 6 && trimmed.substr(0, 6) == "ARRAY<") {
        const size_t bracket = trimmed.find('[');
        if (bracket != std::string_view::npos) {
          fields.emplace_back(std::string(k),
                              decode_array_text(trimmed.substr(bracket + 1)));
          continue;
        }
      }
      if (v == "null") {
          fields.emplace_back(std::string(k), Value());
        } else if (v == "true") {
          fields.emplace_back(std::string(k), Value(int64_t{1}));
        } else if (v == "false") {
          fields.emplace_back(std::string(k), Value(int64_t{0}));
        } else if (!v.empty() && v.front() == '"' && v.back() == '"') {
          std::string unquoted(v.substr(1, v.size() - 2));
          fields.emplace_back(std::string(k), Value(std::move(unquoted)));
        } else {
          int64_t ival = 0;
          auto [ptr, ec] =
              std::from_chars(v.data(), v.data() + v.size(), ival);
          if (ec == std::errc() && ptr == v.data() + v.size()) {
            fields.emplace_back(std::string(k), Value(ival));
          } else {
            double dval = 0.0;
            try {
              dval = std::stod(std::string(v));
              fields.emplace_back(std::string(k), Value(dval));
            } catch (...) {
              fields.emplace_back(std::string(k), Value(std::string(v)));
            }
          }
        }
      }
      return fields;
    };

    std::vector<std::vector<std::pair<std::string, Value>>> all_row_fields;
    for (const auto& elem : elements) {
      if (!elem.IsNull() && elem.type == ValueType::kVarChar) {
        all_row_fields.push_back(parse_json_obj(elem.value.varchar_value));
      } else {
        all_row_fields.push_back({});
      }
    }

    std::vector<Column> cols;
    if (!all_row_fields.empty()) {
      for (const auto& [fname, fval] : all_row_fields[0]) {
        ValueType vt = fval.IsNull() ? ValueType::kVarChar : fval.type;
        cols.emplace_back(fname, vt);
      }
    }
    const bool with_offset = !source.offset_alias.empty();
    if (with_offset) {
      cols.emplace_back(source.offset_alias, ValueType::kInt64);
    }
    result.schema = Schema("", std::move(cols));
    for (size_t row_idx = 0; row_idx < all_row_fields.size(); ++row_idx) {
      const auto& row_fields = all_row_fields[row_idx];
      std::vector<Value> row_vals;
      for (const auto& [fname, fval] : row_fields) {
        row_vals.push_back(fval);
      }
      if (with_offset) {
        row_vals.push_back(Value(static_cast<int64_t>(row_idx)));
      }
      result.AddRow(Row(std::move(row_vals)));
    }
    return result;
  }
  std::vector<Column> unnest_cols;
  unnest_cols.emplace_back(col_name, elem_type);
  if (!source.offset_alias.empty()) {
    unnest_cols.emplace_back(source.offset_alias, ValueType::kInt64);
  }
  result.schema = Schema("", std::move(unnest_cols));
  for (size_t row_idx = 0; row_idx < elements.size(); ++row_idx) {
    if (!source.offset_alias.empty()) {
      result.AddRow(Row({std::move(elements[row_idx]),
                         Value(static_cast<int64_t>(row_idx))}));
    } else {
      result.AddRow(Row({std::move(elements[row_idx])}));
    }
  }
  return result;
}

Relation LoadSource(TransactionContext& context, const SelectSource& source,
                    const Scope* outer, const CteMap& ctes,
                    const std::vector<slot_t>* projection,
                    const std::vector<Expression>* scan_predicates,
                    const std::unordered_set<int64_t>* int_key_filter,
                    std::optional<slot_t> int_key_column) {
  Relation result(context.execution_runtime());
  if (source.unnest) {
    const Value array_val =
        Evaluate(source.unnest, Scope{.outer = outer}, nullptr, context, ctes);
    result = UnnestValueToRelation(source, array_val);
  } else if (source.query) {
    result = ExecuteQuery(context, *source.query, outer, ctes);
  } else if (const auto cte = ctes.find(source.table); cte != ctes.end()) {
    const Relation& cte_relation = *cte->second;
    result.schema = cte_relation.schema;
    cte_relation.ForEachRow([&](const Row& row) { result.AddRow(row); });
  } else {
    const bool reusable =
        context.execution_runtime() != nullptr &&
        context.execution_runtime()->reusable_base_relations.contains(
            source.table);
    const std::string cache_key =
        BaseRelationCacheKey(source.table, projection);
    const bool filter_during_scan =
        !reusable && scan_predicates != nullptr && !scan_predicates->empty();
    RelationPtr cached_entry;
    if (reusable) {
      const auto cached =
          context.execution_runtime()->base_relations.find(cache_key);
      if (cached != context.execution_runtime()->base_relations.end()) {
        cached_entry = cached->second;
      }
    }
    if (cached_entry) {
      // Materialize only rows that survive this alias's local predicates so
      // we never deep-copy a multi-million-row cache and filter afterwards.
      Relation& cached_relation = *cached_entry;
      result.schema = cached_relation.schema;
      cached_relation.FinishSpill();
      if (scan_predicates == nullptr || scan_predicates->empty()) {
        cached_relation.ForEachRow([&](const Row& row) {
          if (int_key_filter && int_key_column) {
            const Value& key = row[*int_key_column];
            if (key.IsNull() ||
                !int_key_filter->contains(key.value.int_value)) {
              return;
            }
          }
          result.AddRow(row);
        });
      } else {
        const auto filter_begin = std::chrono::steady_clock::now();
        const CompiledScanFilter scan_filter =
            CompileScanFilter(*scan_predicates, cached_relation.schema);
        auto emit_filtered = [&](const Relation& source_rel) {
          source_rel.ForEachRow([&](const Row& row) {
            if (int_key_filter && int_key_column) {
              const Value& key = row[*int_key_column];
              if (key.IsNull() ||
                  !int_key_filter->contains(key.value.int_value)) {
                return;
              }
            }
            if (MatchScanFilter(row, source_rel.schema, scan_filter, outer,
                                context, ctes)) {
              result.AddRow(row);
            }
          });
        };
        // Query expressions inside residual predicates may re-enter LoadSource
        // for the same cache_key; nested ForEachRow over one SpillFile breaks
        // its read position, so such caches are snapshotted before filtering.
        const bool needs_snapshot = std::ranges::any_of(
            *scan_predicates, [](const Expression& predicate) {
              return ContainsQuery(predicate);
            });
        if (needs_snapshot) {
          Relation snapshot = MaterializeRelation(cached_relation);
          snapshot.FinishSpill();
          emit_filtered(snapshot);
        } else {
          emit_filtered(cached_relation);
        }
        context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
      }
      ++context.execution_runtime()->base_scan_cache_hits;
    } else {
      StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
      if (!table.HasValue()) {
        throw std::runtime_error("table " + source.table + " not found");
      }
      const Schema& table_schema = table.Value()->GetSchema();
      result.schema = projection != nullptr
                          ? ProjectSchema(table_schema, *projection)
                          : table_schema;
      CompiledScanFilter scan_filter;
      if (filter_during_scan) {
        scan_filter = CompileScanFilter(*scan_predicates, result.schema);
      }
      const auto scan_begin = std::chrono::steady_clock::now();
      const auto filter_begin = scan_begin;
      // Prefer skipping full-row decode when an integer key IN-list is active.
      std::optional<slot_t> full_key_column;
      if (int_key_filter != nullptr && int_key_column &&
          projection != nullptr) {
        if (*int_key_column < projection->size()) {
          full_key_column = (*projection)[*int_key_column];
        }
      } else if (int_key_filter != nullptr && int_key_column &&
                 projection == nullptr) {
        full_key_column = int_key_column;
      }
      const bool parallel_ok = TryParallelTableScan(
          context, *table.Value(), projection, int_key_filter, full_key_column,
          filter_during_scan, filter_during_scan ? &scan_filter : nullptr,
          result.schema, outer, ctes, &result);
      if (!parallel_ok) {
        Iterator iterator = [&] {
          if (full_key_column != std::nullopt) {
            if (projection != nullptr) {
              return table.Value()->BeginFullScan(
                  context.txn_, *projection, int_key_filter, *full_key_column);
            }
            return table.Value()->BeginFullScan(context.txn_, int_key_filter,
                                                *full_key_column);
          }
          if (projection != nullptr) {
            return table.Value()->BeginFullScan(context.txn_, *projection);
          }
          return table.Value()->BeginFullScan(context.txn_);
        }();
        while (iterator.IsValid()) {
          if (context.execution_runtime() != nullptr) {
            ++context.execution_runtime()->scan_rows;
            context.execution_runtime()->scan_values_available +=
                table_schema.ColumnCount();
            context.execution_runtime()->scan_values_decoded +=
                result.schema.ColumnCount();
          }
          bool matches = true;
          if (full_key_column == std::nullopt && int_key_filter != nullptr &&
              int_key_column) {
            const Value& key = (*iterator)[*int_key_column];
            if (key.IsNull() ||
                !int_key_filter->contains(key.value.int_value)) {
              matches = false;
              if (context.execution_runtime() != nullptr) {
                ++context.execution_runtime()->key_filter_rejected;
              }
            }
          }
          if (matches && filter_during_scan) {
            matches = MatchScanFilter(*iterator, result.schema, scan_filter,
                                      outer, context, ctes);
          }
          if (matches) {
            result.AddRow(*iterator);
            if (context.execution_runtime() != nullptr) {
              ++context.execution_runtime()->scan_output_rows;
            }
          }
          ++iterator;
        }
      }
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->scan_ms += ElapsedMs(scan_begin);
        if (filter_during_scan) {
          context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
        }
        if (reusable && int_key_filter == nullptr) {
          auto cached = std::make_shared<Relation>(std::move(result));
          context.execution_runtime()->base_relations.emplace(cache_key,
                                                              cached);
          // The contents moved into the cache above; start from a fresh
          // relation before refilling so we never touch a moved-from object.
          result = Relation{};
          result.schema = cached->schema;
          cached->ForEachRow([&](const Row& row) { result.AddRow(row); });
        }
      }
      if (reusable && scan_predicates != nullptr && !scan_predicates->empty()) {
        FilterRelation(context, &result, *scan_predicates, outer, ctes);
      }
    }
  }
  const std::string qualifier =
      source.alias.empty() ? source.table : source.alias;
  // Unnest outputs carry the alias as qualifier too: bare element refs still
  // match by column name, while alias-qualified and alias-as-row references
  // (`s.field`, `SELECT s`) resolve uniformly with table sources.
  if (!qualifier.empty()) {
    result.schema = QualifySchema(result.schema, qualifier);
  }
  result.FinishSpill();
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  return result;
}
bool ContainsQuery(const Expression& expression) {  // NOLINT(misc-no-recursion)
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    return true;
  }
  return std::ranges::any_of(
      ExpressionChildren(expression),
      [](const Expression& child) {  // NOLINT(misc-no-recursion)
        return ContainsQuery(child);
      });
}

std::optional<size_t> LocalColumnOffset(const Schema& schema,
                                        const ColumnName& name) {
  // GoogleSQL identifiers are case-insensitive.
  auto equals = [](std::string_view left, std::string_view right) {
    return left.size() == right.size() &&
           std::equal(left.begin(), left.end(), right.begin(),
                      [](char lhs, char rhs) {
                        return std::tolower(static_cast<unsigned char>(lhs)) ==
                               std::tolower(static_cast<unsigned char>(rhs));
                      });
  };
  std::optional<size_t> match;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const ColumnName& candidate = schema.GetColumn(i).Name();
    if (!equals(candidate.name, name.name)) {
      continue;
    }
    if (!name.schema.empty() && !equals(candidate.schema, name.schema)) {
      continue;
    }
    if (match) {
      return std::nullopt;
    }
    match = i;
  }
  if (!match && name.schema.empty()) {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      const ColumnName& candidate = schema.GetColumn(i).Name();
      if (equals(candidate.schema, name.name)) {
        if (match) {
          return std::nullopt;
        }
        match = i;
      }
    }
  }
  return match;
}
std::vector<Expression> SplitDisjuncts(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (expression && expression->Type() == TypeTag::kBinaryExp &&
      expression->AsBinaryExpression().Op() == BinaryOperation::kOr) {
    std::vector<Expression> result =
        SplitDisjuncts(expression->AsBinaryExpression().Left());
    std::vector<Expression> right =
        SplitDisjuncts(expression->AsBinaryExpression().Right());
    result.insert(result.end(), right.begin(), right.end());
    return result;
  }
  return expression ? std::vector<Expression>{expression}
                    : std::vector<Expression>{};
}

Expression CombineDisjuncts(const std::vector<Expression>& expressions) {
  if (expressions.empty()) {
    return nullptr;
  }
  Expression result = expressions.front();
  for (size_t i = 1; i < expressions.size(); ++i) {
    result = BinaryExpressionExp(result, BinaryOperation::kOr, expressions[i]);
  }
  return result;
}
void FilterRelation(TransactionContext& context, Relation* relation,
                    const std::vector<Expression>& predicates,
                    const Scope* outer, const CteMap& ctes) {
  if (predicates.empty()) {
    return;
  }
  const auto filter_begin = std::chrono::steady_clock::now();
  const CompiledScanFilter scan_filter =
      CompileScanFilter(predicates, relation->schema);
  Relation filtered;
  filtered.schema = relation->schema;
  CopyExecutionStats(&filtered, *relation);
  relation->FinishSpill();
  relation->ForEachRow([&](const Row& row) {
    if (MatchScanFilter(row, relation->schema, scan_filter, outer, context,
                        ctes)) {
      filtered.AddRow(row);
    }
  });
  filtered.FinishSpill();
  *relation = std::move(filtered);
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
  }
}

}  // namespace tinylamb::relational_detail
