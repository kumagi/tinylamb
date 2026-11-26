/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/exception_status.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/proto_text.hpp"
#include "expression/rewrite.hpp"
#include "index/index_scan_iterator.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

Relation UnnestValueToRelation(const SelectSource& source,
                               const Value& array_val) {
  auto trim_part = [](std::string_view text) {
    size_t begin = text.find_first_not_of(" \t");
    if (begin == std::string_view::npos) {
      return std::string();
    }
    size_t end = text.find_last_not_of(" \t");
    return std::string(text.substr(begin, end - begin + 1));
  };
  Relation result;
  std::string col_name = source.alias.empty() ? "unnest" : source.alias;
  ValueType elem_type = ValueType::kNull;
  std::vector<Value> elements;
  if (array_val.IsArray()) {
    elements = array_val.ArrayElements();
    // Infer from the first NON-NULL element: a leading NULL would pin the
    // column to kNull while later rows carry real types — the per-row type
    // flip vectorized consumers reject.
    for (const Value& element : elements) {
      if (!element.IsNull()) {
        elem_type = element.type;
        break;
      }
    }
  } else if (!array_val.IsNull()) {
    elements.push_back(array_val);
    elem_type = array_val.type;
  }
  const std::string elem_sql_type =
      array_val.IsArray() ? array_val.ArrayElementSqlType() : "";
  if (elem_sql_type == "PROTO") {
    std::vector<Column> cols;
    cols.emplace_back(col_name, ValueType::kVarChar);
    std::vector<std::string> field_names;
    for (const Value& element : elements) {
      if (element.IsNull() || element.type != ValueType::kVarChar) {
        continue;
      }
      std::vector<ProtoTextEntry> parsed;
      const std::string text(element.value.varchar_value);
      if (!ParseProtoTextEntries(text, &parsed)) {
        continue;
      }
      for (const ProtoTextEntry& entry : parsed) {
        if (std::ranges::none_of(field_names, [&](const std::string& name) {
              return name == entry.name;
            })) {
          field_names.push_back(entry.name);
        }
      }
    }
    std::vector<std::vector<Value>> field_values(
        elements.size(), std::vector<Value>(field_names.size()));
    for (size_t field_idx = 0; field_idx < field_names.size(); ++field_idx) {
      std::string qualified_name = col_name + "." + field_names[field_idx];
      ValueType type = ValueType::kNull;
      for (size_t row_idx = 0; row_idx < elements.size(); ++row_idx) {
        if (elements[row_idx].IsNull() ||
            elements[row_idx].type != ValueType::kVarChar) {
          continue;
        }
        Value field;
        if (ProtoTextExtractField(elements[row_idx].value.varchar_value,
                                  field_names[field_idx], &field)) {
          field_values[row_idx][field_idx] = field;
          if (!field.IsNull()) {
            type = field.type;
          }
        }
      }
      cols.emplace_back(std::move(qualified_name), type);
    }
    if (!source.offset_alias.empty()) {
      cols.emplace_back(source.offset_alias, ValueType::kInt64);
    }
    result.schema = Schema("", std::move(cols));
    for (size_t row_idx = 0; row_idx < elements.size(); ++row_idx) {
      std::vector<Value> row_vals;
      row_vals.push_back(std::move(elements[row_idx]));
      for (auto& fv : field_values[row_idx]) {
        row_vals.push_back(std::move(fv));
      }
      if (!source.offset_alias.empty()) {
        row_vals.emplace_back(static_cast<int64_t>(row_idx));
      }
      result.AddRow(Row(std::move(row_vals)));
    }
    return result;
  }
  // GoogleSQL UNNEST over an ARRAY<STRUCT> flattens one structural level:
  // the struct's fields become top-level columns (`FROM UNNEST([STRUCT(1 AS
  // y, 2 AS x)])` exposes y and x).  Field names come from the first
  // non-NULL element's encoded JSON keys; NULL elements and later rows with
  // fewer members NULL-fill to the initialized width.
  const bool declared_struct = elem_sql_type.starts_with("STRUCT");
  bool object_shaped = false;
  if (!declared_struct && (elem_sql_type.empty() || elem_sql_type == "INT64")) {
    for (const Value& element : elements) {
      if (element.IsNull()) {
        continue;
      }
      if (element.type == ValueType::kVarChar) {
        const std::string_view text(element.value.varchar_value);
        object_shaped =
            text.size() >= 2 && text.front() == '{' && text.back() == '}';
      }
      break;
    }
  }
  if (declared_struct || object_shaped) {
    // An explicitly aliased UNNEST (`t.struct_arrcol elem`) keeps the whole
    // element reachable under its alias (a NULL element stays NULL); the
    // flattened member columns sit beside it.  An unaliased UNNEST exposes
    // only the member columns.
    const bool keep_element_column =
        !source.alias.empty() && source.alias != "unnest";
    std::vector<std::pair<std::string, ValueType>> fields;
    // Declared element types (`ARRAY<STRUCT<start_day DATE, ...>>`) name
    // fields even when the encoded members use anonymous fN keys, and let
    // textual members coerce back to their declared runtime type.
    std::vector<std::pair<std::string, std::string>> declared_fields;
    if (declared_struct && elem_sql_type.back() == '>' &&
        elem_sql_type.find('<') != std::string::npos) {
      const size_t open = elem_sql_type.find('<');
      std::vector<std::string> parts;
      int bracket = 0;
      std::string current;
      current.reserve(elem_sql_type.size() - open - 1);
      for (size_t i = open + 1; i < elem_sql_type.size(); ++i) {
        const char c = elem_sql_type[i];
        if (c == '<' || c == '(') {
          ++bracket;
        }
        if (c == '>' || c == ')') {
          --bracket;
        }
        if (c == ',' && bracket == 0) {
          parts.push_back(current);
          current.clear();
          continue;
        }
        current.push_back(c);
      }
      if (!current.empty()) {
        parts.push_back(current);
      }
      for (std::string& part : parts) {
        const std::string trimmed = trim_part(part);
        if (trimmed.empty()) {
          continue;
        }
        const size_t space = trimmed.find_first_of(" \t");
        if (space == std::string::npos) {
          declared_fields.emplace_back(std::string(), trimmed);
        } else {
          declared_fields.emplace_back(trimmed.substr(0, space),
                                       trim_part(trimmed.substr(space + 1)));
        }
      }
    }
    struct ParsedElement {
      std::vector<std::pair<size_t, Value>> cells;
      bool null_row{false};
    };
    // Name-aligned flattening: when the declared struct names every field
    // AND the encoded members carry those names, each member is placed at
    // its declared position (a JSON document may serialize members in any
    // order). Anonymous fN encodings and positional members keep the
    // serialized-order behavior.
    const bool declared_named =
        declared_struct && !declared_fields.empty() &&
        std::ranges::none_of(declared_fields,
                             [](const std::pair<std::string, std::string>& f) {
                               return f.first.empty();
                             });
    // Flattening mode, decided from the first parsed element: 1 = place
    // members by declared name, 0 = keep serialized order.
    int align_mode = -1;
    std::vector<ParsedElement> parsed_rows(elements.size());
    for (size_t row_idx = 0; row_idx < elements.size(); ++row_idx) {
      const Value& element = elements[row_idx];
      if (element.IsNull() || element.type != ValueType::kVarChar) {
        parsed_rows[row_idx].null_row = true;
        continue;
      }
      const std::string text(element.value.varchar_value);
      if (text.size() < 2 || text.front() != '{' || text.back() != '}') {
        parsed_rows[row_idx].null_row = true;
        continue;
      }
      const auto members =
          SplitJsonObjectMembers(text.substr(1, text.size() - 2));
      const bool row_named =
          declared_named &&
          std::ranges::all_of(
              members, [&](const std::pair<std::string, std::string>& member) {
                return std::ranges::any_of(
                    declared_fields,
                    [&](const std::pair<std::string, std::string>& field) {
                      return field.first == member.first;
                    });
              });
      if (align_mode < 0) {
        align_mode = row_named ? 1 : 0;
      }
      const bool aligned = align_mode == 1 && row_named;
      for (const auto& [key, member_text] : members) {
        Value parsed;
        if (!JsonTextToValue(member_text, &parsed)) {
          // Non-JSON scalars (e.g. `inf`, `nan` doubles) fall back to
          // numeric then textual interpretation.
          char* parse_end = nullptr;
          const double number = std::strtod(member_text.c_str(), &parse_end);
          if (parse_end != member_text.c_str() && *parse_end == '\0') {
            parsed = Value(number);
          } else {
            parsed = Value(std::string(member_text));
          }
        }
        size_t cell_index = parsed_rows[row_idx].cells.size();
        std::string declared_type;
        if (aligned) {
          const auto fit = std::ranges::find(
              declared_fields, key,
              [](const std::pair<std::string, std::string>& f) {
                return f.first;
              });
          cell_index = static_cast<size_t>(fit - declared_fields.begin());
          declared_type = fit->second;
        } else if (declared_fields.size() == members.size()) {
          declared_type = declared_fields[cell_index].second;
        }
        // Coerce textual members back to declared runtime types: struct
        // serialization stores DATE/TIMESTAMP cells as plain text.
        if (!declared_type.empty() && parsed.type == ValueType::kVarChar) {
          std::string declared_text(parsed.value.varchar_value);
          if ((declared_type == "DATE" || declared_type == "date")) {
            int y = 0, m = 0, d = 0;
            if (sscanf(  // NOLINT(cert-err34-c) - conversion count is checked.
                    declared_text.c_str(), "%d-%d-%d", &y, &m, &d) == 3) {
              parsed = Value::Date(declared_text);
            }
          } else if (declared_type == "TIMESTAMP" ||
                     declared_type == "timestamp" ||
                     declared_type == "DATETIME") {
            int y = 0, mo = 0, d = 0;
            if (sscanf(  // NOLINT(cert-err34-c) - conversion count is checked.
                    declared_text.c_str(), "%d-%d-%d", &y, &mo, &d) == 3) {
              parsed =
                  Value(std::move(declared_text));  // keep canonical text form
            }
          }
        }
        parsed_rows[row_idx].cells.emplace_back(cell_index, std::move(parsed));
      }
      if (align_mode == 1) {
        // Declared names drive the field list; no per-row key discovery.
        continue;
      }
      if (fields.empty()) {
        for (const auto& [key, member_text] : members) {
          fields.emplace_back(key, ValueType::kNull);
        }
        // Prefer declared field names when the arity matches: anonymous
        // tuple literals encode members as f1..fN under a declared type.
        if (declared_fields.size() == fields.size()) {
          for (size_t i = 0; i < declared_fields.size(); ++i) {
            if (!declared_fields[i].first.empty()) {
              fields[i].first = declared_fields[i].first;
            }
          }
        }
      }
    }
    if (align_mode == 1) {
      for (const auto& declared : declared_fields) {
        fields.emplace_back(declared.first, ValueType::kNull);
      }
    }
    // Fix up column types from the flattened cells (first non-NULL value
    // wins, matching the previous representative-row treatment).
    for (const ParsedElement& parsed_row : parsed_rows) {
      for (const auto& [cell_index, cell_value] : parsed_row.cells) {
        if (cell_index < fields.size() && !cell_value.IsNull() &&
            fields[cell_index].second == ValueType::kNull) {
          fields[cell_index].second = cell_value.type;
        }
      }
    }
    std::vector<Column> unnest_cols;
    unnest_cols.reserve(fields.size() + (keep_element_column ? 1 : 0) +
                        (!source.offset_alias.empty() ? 1 : 0));
    for (const auto& [field_name, field_type] : fields) {
      unnest_cols.emplace_back(field_name, field_type);
    }
    if (keep_element_column) {
      unnest_cols.emplace_back(col_name, ValueType::kVarChar);
    }
    if (!source.offset_alias.empty()) {
      unnest_cols.emplace_back(source.offset_alias, ValueType::kInt64);
    }
    result.schema = Schema("", std::move(unnest_cols));
    for (size_t row_idx = 0; row_idx < parsed_rows.size(); ++row_idx) {
      std::vector<Value> row_vals(fields.size(), Value());
      if (!parsed_rows[row_idx].null_row) {
        for (const auto& [cell_index, cell_value] :
             parsed_rows[row_idx].cells) {
          if (cell_index < fields.size()) {
            row_vals[cell_index] = cell_value;
          }
        }
      }
      if (keep_element_column) {
        row_vals.push_back(elements[row_idx]);
      }
      if (!source.offset_alias.empty()) {
        row_vals.emplace_back(static_cast<int64_t>(row_idx));
      }
      result.AddRow(Row(std::move(row_vals)));
    }
    return result;
  }
  // Scalar / untyped elements stay in their single encoded column instead:
  // a NULL element surfaces as a NULL value, and field access resolves via
  // Lookup's dotted-path traversal of the encoding.
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

namespace {
// Forward declaration: defined with the subquery-fold helpers below.
bool ContainsReentrantQueryIn(const Expression& expression,
                              std::string_view table);
}  // namespace

namespace {
// A selective numeric range supplies an enclosure of the visible rows. Keep
// physical table order so floating-point aggregates and stable ties retain
// the full-scan contract. Estimates choose this path, never bound its answer.
StatusOr<bool> TrySelectiveIndexSource(
    TransactionContext& context, const Table& table,
    const TableStatistics& statistics, const Schema& filter_schema,
    const std::vector<slot_t>* projection, const CompiledScanFilter& filter,
    const ScanRuntimeFilter* runtime_filter, const Scope* outer,
    const CteMap& ctes, Relation* result) {
  if (table.IndexCount() == 0 || statistics.Rows() < 1024) {
    return false;
  }
  const bool pure_range =
      !filter.simple.empty() && !filter.compile_decided_reject &&
      filter.prefix_in.empty() && filter.prebuilt_membership.empty() &&
      filter.prebuilt_scalar_compare.empty() &&
      filter.disjunctive_branches.empty() && !filter.needs_unsigned_tagging;
  const auto* exact = (runtime_filter != nullptr) && runtime_filter->enabled()
                          ? runtime_filter->group->ExactKeys()
                          : nullptr;
  const auto local_expression = [&](const auto& self,
                                    const Expression& expression) -> bool {
    if (!expression) {
      return true;
    }
    switch (expression->Type()) {
      case TypeTag::kColumnValue:
      case TypeTag::kConstantValue:
      case TypeTag::kBinaryExp:
      case TypeTag::kUnaryExp:
      case TypeTag::kCaseExp:
      case TypeTag::kInExp:
        return std::ranges::all_of(
            ExpressionChildren(expression),
            [&](const Expression& child) { return self(self, child); });
      default:
        return false;
    }
  };
  const auto local_predicates = [&](const auto& predicates) {
    return std::ranges::all_of(predicates, [&](const Expression& predicate) {
      return local_expression(local_expression, predicate);
    });
  };
  // A literal LIKE over a local string column does not widen the key
  // enclosure. Evaluate it only after restoring heap order, just as on the
  // full scan. Other residuals keep the previous conservative range gate.
  const bool safe_range =
      pure_range &&
      std::ranges::all_of(filter.residual, [&](const Expression& expression) {
        if (!expression || expression->Type() != TypeTag::kBinaryExp) {
          return false;
        }
        const auto& binary = expression->AsBinaryExpression();
        if ((binary.Op() != BinaryOperation::kLike &&
             binary.Op() != BinaryOperation::kNotLike) ||
            !binary.Left() || !binary.Right() ||
            binary.Left()->Type() != TypeTag::kColumnValue ||
            binary.Right()->Type() != TypeTag::kConstantValue) {
          return false;
        }
        const auto offset = LocalColumnOffset(
            filter_schema, binary.Left()->AsColumnValue().GetColumnName());
        return offset &&
               filter_schema.GetColumn(*offset).Type() == ValueType::kVarChar &&
               binary.Right()->AsConstantValue().GetValue().type ==
                   ValueType::kVarChar;
      });
  if ((exact != nullptr) &&
      (exact->size() > 1024 || !local_predicates(filter.residual) ||
       !std::ranges::all_of(filter.disjunctive_branches,
                            [&](const auto& branch) {
                              return local_predicates(branch.residual);
                            }))) {
    exact = nullptr;
  }
  if (!safe_range && (exact == nullptr)) {
    return false;
  }
  const Index* selected = nullptr;
  std::vector<Value> begin, end;
  const std::vector<int64_t>* selected_keys = nullptr;
  double best = 0.05;
  const Schema& schema = table.GetSchema();
  for (size_t i = 0; i < table.IndexCount(); ++i) {
    const Index& index = table.GetIndex(i);
    if (index.sc_.key_.empty() || index.RetainsDeletedEntries()) {
      continue;
    }
    const slot_t key = index.sc_.key_.front();
    const Column& column = schema.GetColumn(key);
    if (column.IsUnsigned() || (column.Type() != ValueType::kInt64 &&
                                column.Type() != ValueType::kDate)) {
      continue;
    }
    if (key >= statistics.Columns()) {
      continue;
    }
    if ((exact != nullptr) &&
        runtime_filter->column < filter_schema.ColumnCount()) {
      const slot_t runtime_column = (projection != nullptr)
                                        ? (*projection)[runtime_filter->column]
                                        : runtime_filter->column;
      if (runtime_column == key && runtime_filter->type == column.Type()) {
        double matches = 0;
        for (int64_t raw : *exact) {
          if (runtime_filter->group->Reject(raw)) {
            continue;
          }
          Value value(raw);
          value.type = column.Type();
          matches += statistics.Column(key).EstimateEqual(value);
        }
        const double fraction =
            matches / static_cast<double>(statistics.Rows());
        if (fraction >= 0 && fraction < best) {
          selected = &index;
          selected_keys = exact;
          best = fraction;
        }
      }
    }
    if (!safe_range) {
      continue;
    }
    std::optional<Value> lower, upper;
    for (const auto& predicate : filter.simple) {
      if (predicate.column_payload ||
          predicate.column >= filter_schema.ColumnCount()) {
        continue;
      }
      const slot_t full_column = (projection != nullptr)
                                     ? (*projection)[predicate.column]
                                     : predicate.column;
      if (full_column != key || predicate.constant.IsNull() ||
          predicate.constant.type != column.Type() ||
          predicate.constant.IsUnsigned()) {
        continue;
      }
      const auto op = predicate.op;
      if (op == BinaryOperation::kEquals ||
          op == BinaryOperation::kGreaterThan ||
          op == BinaryOperation::kGreaterThanEquals) {
        if (!lower ||
            predicate.constant.value.int_value > lower->value.int_value) {
          lower = predicate.constant;
        }
      }
      if (op == BinaryOperation::kEquals || op == BinaryOperation::kLessThan ||
          op == BinaryOperation::kLessThanEquals) {
        if (!upper ||
            predicate.constant.value.int_value < upper->value.int_value) {
          upper = predicate.constant;
        }
      }
    }
    if (!lower && !upper) {
      continue;
    }
    // Cost the enclosure itself; an unrelated predicate must not disguise
    // a broad index range as a selective lookup.
    if (key >= statistics.Columns()) {
      continue;
    }
    const double fraction =
        statistics.Column(key).EstimateRange(lower, true, upper, true) /
        static_cast<double>(statistics.Rows());
    if (fraction >= 0 && fraction < best) {
      selected = &index;
      selected_keys = nullptr;
      begin = lower ? std::vector<Value>{*lower} : std::vector<Value>{};
      end = upper ? std::vector<Value>{*upper} : std::vector<Value>{};
      best = fraction;
    }
  }
  if ((selected == nullptr) ||
      context.txn_.IndexKeysMayBeStale(selected->Root())) {
    return false;
  }
  constexpr size_t kMaxRows = 8192;
  constexpr size_t kBytes = size_t{4} * 1024 * 1024;
  auto charge = TryReserveQueryMemory(kBytes);
  if (!charge) {
    return false;
  }
  auto guard = context.txn_.AcquireIndexReadGuard(selected->Root());
  if (context.txn_.IndexKeysMayBeStale(selected->Root())) {
    return false;
  }
  std::vector<Expression> early_memberships;
  early_memberships.reserve(filter.residual.size());
  if (!filter.needs_unsigned_tagging) {
    for (const auto& expression : filter.residual) {
      if (!expression || expression->Type() != TypeTag::kInExp) {
        continue;
      }
      const auto& membership = expression->AsInExpression();
      if (membership.child_ &&
          membership.child_->Type() == TypeTag::kColumnValue &&
          std::ranges::all_of(membership.list_, [](const Expression& value) {
            return value && value->Type() == TypeTag::kConstantValue;
          })) {
        early_memberships.push_back(expression);
      }
    }
  }
  struct SourceRow {
    RowPosition position;
    Row row;
    size_t page_ordinal{0};
  };
  std::vector<SourceRow> rows;
  rows.reserve(kMaxRows);
  size_t bytes = 0, visited = 0;
  const size_t scans = (selected_keys != nullptr) ? selected_keys->size() : 1;
  for (size_t seek = 0; seek < scans; ++seek) {
    if (selected_keys != nullptr) {
      const int64_t raw = (*selected_keys)[seek];
      if (runtime_filter->group->Reject(raw)) {
        continue;
      }
      Value value(raw);
      value.type = schema.GetColumn(selected->sc_.key_.front()).Type();
      begin = end = std::vector<Value>{value};
    }
    IndexScanIterator iterator(table, *selected, context.txn_, begin, end, true,
                               false, &guard);
    if (projection != nullptr) {
      RETURN_IF_FAIL(iterator.SetProjection(*projection));
    }
    while (iterator.IsValid()) {
      if (++visited > kMaxRows) {
        return false;
      }
      const Row& full_row = *iterator;
      RETURN_IF_FAIL(iterator.GetStatus());
      if (!full_row.IsValid()) {
        ++iterator;
        continue;
      }
      if (full_row.Count() != filter_schema.ColumnCount()) {
        return StatusError(StatusCode::kCorrupt,
                           "indexed source row width mismatch");
      }
      const Row& row = full_row;
      bool matches = true;
      if (!filter.needs_unsigned_tagging) {
        matches =
            std::ranges::all_of(filter.simple, [&](const auto& predicate) {
              return MatchSimpleCompare(row, predicate);
            });
        if (matches && !filter.disjunctive_branches.empty()) {
          matches = std::ranges::any_of(
              filter.disjunctive_branches, [&](const auto& branch) {
                return std::ranges::all_of(branch.simple,
                                           [&](const auto& predicate) {
                                             return MatchSimpleCompare(
                                                 row, predicate);
                                           }) &&
                       std::ranges::all_of(branch.prefix_in,
                                           [&](const auto& predicate) {
                                             return MatchPrefixIn(row,
                                                                  predicate);
                                           }) &&
                       std::ranges::all_of(branch.prebuilt_membership,
                                           [&](const auto& predicate) {
                                             return MatchPrebuiltMembership(
                                                 row, predicate);
                                           });
              });
        }
      }
      for (const auto& membership : early_memberships) {
        if (!matches) {
          break;
        }
        const auto value = membership->TryEvaluate(row, filter_schema);
        // Errors stay on the ordered canonical path. Only a clean rejection
        // can discard a row before restoring physical order.
        if (value.HasValue() &&
            (value.Value().IsNull() || !value.Value().Truthy())) {
          matches = false;
        }
      }
      if (matches && (runtime_filter != nullptr) && runtime_filter->enabled()) {
        const Value& key = row[runtime_filter->column];
        matches = !runtime_filter->Reject(static_cast<int8_t>(key.type),
                                          key.IsNull(), key.value.int_value);
      }
      if (matches) {
        bytes += (row.values_.capacity() * sizeof(Value)) +
                 (sizeof(RowPosition) * 4) + (sizeof(Row) * 2);
        for (const Value& value : row.values_) {
          if (value.type == ValueType::kVarChar) {
            bytes += (2 * value.value.varchar_value.size()) + 32;
          }
        }
        if (bytes > kBytes / 2) {
          return false;
        }
        rows.push_back({iterator.Position(), std::move(*iterator), 0});
      }
      ++iterator;
    }
    RETURN_IF_FAIL(iterator.GetStatus());
  }
  if (rows.size() > 1) {
    std::unordered_map<page_id_t, size_t> page_order;
    Status morsel_status = Status::kSuccess;
    const auto morsels =
        table.BuildScanMorsels(context.txn_, 8, &morsel_status);
    RETURN_IF_FAIL(morsel_status);
    size_t ordinal = 0;
    for (const auto& morsel : morsels) {
      for (page_id_t page : morsel) {
        if ((page_order.size() * 64) + bytes > kBytes) {
          return false;
        }
        page_order.emplace(page, ordinal++);
      }
    }
    for (auto& row : rows) {
      const auto found = page_order.find(row.position.page_id);
      if (found == page_order.end()) {
        return false;
      }
      row.page_ordinal = found->second;
    }
    std::stable_sort(rows.begin(), rows.end(),
                     [](const auto& a, const auto& b) {
                       return a.page_ordinal != b.page_ordinal
                                  ? a.page_ordinal < b.page_ordinal
                                  : a.position.slot < b.position.slot;
                     });
  }
  // The auxiliary reservation remains held until its buffers are destroyed;
  // Relation independently charges (and can spill) the transferred rows.
  Status eval_error{Status::kSuccess};
  size_t emitted = 0;
  for (auto& row : rows) {
    if (MatchScanFilter(row.row, filter_schema, filter, outer, context, ctes,
                        &eval_error)) {
      result->AddRow(std::move(row.row));
      ++emitted;
    }
    RETURN_IF_FAIL(eval_error);
  }
  RETURN_IF_FAIL(result->GetStatus());
  if (auto* runtime = context.execution_runtime()) {
    ++runtime->selective_index_scans;
    runtime->selective_index_rows += visited;
    runtime->scan_rows += visited;
    runtime->scan_values_available += visited * schema.ColumnCount();
    runtime->scan_values_decoded += visited * filter_schema.ColumnCount();
    runtime->scan_output_rows += emitted;
  }
  return true;
}

void MaterializeSingleArenaRow(const BaseRelationArena& arena,
                               const Schema& schema, size_t index, Row* row) {
  const size_t stride = arena.stride;
  const int64_t* cells = arena.cells.data() + (index * stride);
  row->values_.resize(stride);
  for (size_t c = 0; c < stride; ++c) {
    Value& dst = row->values_[c];
    if (arena.IsNull(index, c)) [[unlikely]] {
      if (dst.type != ValueType::kNull) {
        dst = Value();
      }
      continue;
    }
    const ValueType type = arena.slot_types.size() == stride
                               ? arena.slot_types[c]
                               : schema.GetColumn(c).Type();
    if (type == ValueType::kVarChar) {
      const std::string_view sv = arena.Varchar(index, c);
      dst.type = ValueType::kVarChar;
      dst.owned_data.assign(sv);
      dst.value.varchar_value = dst.owned_data;
    } else if (type == ValueType::kDouble) {
      dst.type = ValueType::kDouble;
      dst.value.double_value = std::bit_cast<double>(cells[c]);
    } else {
      dst.type = type;
      dst.value.int_value = cells[c];
      if (type == ValueType::kInt64 && schema.ColumnCount() > c &&
          schema.GetColumn(c).IsUnsigned()) [[unlikely]] {
        dst.SetUnsigned();
      }
    }
  }
}

std::optional<bool> TryMatchSimpleCompareArena(
    const BaseRelationArena& arena, size_t row,
    const SimpleComparePredicate& pred) {
  const size_t stride = arena.stride;
  if (pred.column >= stride || arena.slot_types.size() != stride) {
    return std::nullopt;
  }
  if (pred.column_payload) {
    if (pred.right_column >= stride) {
      return std::nullopt;
    }
    if (arena.IsNull(row, pred.column) ||
        arena.IsNull(row, pred.right_column)) {
      return false;
    }
    const ValueType lt = arena.slot_types[pred.column];
    const ValueType rt = arena.slot_types[pred.right_column];
    const int64_t* row_cells = arena.cells.data() + (row * stride);
    if ((lt == ValueType::kInt64 || lt == ValueType::kDate) &&
        (rt == ValueType::kInt64 || rt == ValueType::kDate)) {
      return ComparePositional(pred.op, row_cells[pred.column],
                               row_cells[pred.right_column]);
    }
    if (lt == ValueType::kDouble && rt == ValueType::kDouble) {
      return ComparePositional(
          pred.op, std::bit_cast<double>(row_cells[pred.column]),
          std::bit_cast<double>(row_cells[pred.right_column]));
    }
    if (lt == ValueType::kVarChar && rt == ValueType::kVarChar) {
      const std::string_view lhs = arena.Varchar(row, pred.column);
      const std::string_view rhs = arena.Varchar(row, pred.right_column);
      if (LooksLikeTimestampText(lhs) && LooksLikeTimestampText(rhs)) {
        return std::nullopt;  // Let the boxed comparator compare instants.
      }
      return ComparePositional(pred.op, lhs, rhs);
    }
    return std::nullopt;
  }
  if (arena.IsNull(row, pred.column) || pred.constant.IsNull()) {
    return false;
  }
  const ValueType vt = arena.slot_types[pred.column];
  const int64_t* row_cells = arena.cells.data() + (row * stride);
  if (pred.int_payload && !pred.constant.IsUnsigned() &&
      (vt == ValueType::kInt64 || vt == ValueType::kDate)) {
    return ComparePositional(pred.op, row_cells[pred.column],
                             pred.int_constant);
  }
  if (pred.double_payload && vt == ValueType::kDouble) {
    return ComparePositional(pred.op,
                             std::bit_cast<double>(row_cells[pred.column]),
                             pred.double_constant);
  }
  if (vt == ValueType::kVarChar && pred.constant.type == ValueType::kVarChar &&
      !pred.constant.IsCaseInsensitive()) {
    const std::string_view l = arena.Varchar(row, pred.column);
    const std::string_view r = pred.constant.value.varchar_value;
    if (pred.op == BinaryOperation::kLike ||
        pred.op == BinaryOperation::kNotLike) {
      const bool matched = FastLikeMatch(l, r);
      return pred.op == BinaryOperation::kLike ? matched : !matched;
    }
    if (pred.plain_string_payload) {
      return ComparePositional(pred.op, l, r);
    }
  }
  return std::nullopt;
}

}  // namespace

bool TryFilterArenaCache(
    TransactionContext& context, const BaseRelationArena& arena,
    const Schema& cached_schema, const CompiledScanFilter* filter,
    const Schema* filter_view, const Scope* outer, const CteMap& ctes,
    const ScanRuntimeFilter* runtime_filter, Relation* result, Status* error) {
  if (arena.rows == 0 || arena.slot_types.size() != arena.stride) {
    return false;
  }
  for (size_t c = 0; c < cached_schema.ColumnCount(); ++c) {
    if (cached_schema.GetColumn(c).IsUnsigned()) {
      return false;
    }
  }
  if (filter != nullptr && filter->compile_decided_reject) {
    return true;
  }
  const bool has_non_simple =
      filter != nullptr &&
      (filter->needs_unsigned_tagging || !filter->prefix_in.empty() ||
       !filter->prebuilt_membership.empty() ||
       !filter->prebuilt_scalar_compare.empty() || !filter->residual.empty() ||
       !filter->disjunctive_branches.empty());
  const WorkerScanFilterPrograms programs =
      (filter != nullptr && has_non_simple && filter_view != nullptr)
          ? CompileWorkerFilter(*filter, *filter_view)
          : WorkerScanFilterPrograms{};
  constexpr size_t kMinParallelFilterRows = 32768;
  constexpr size_t kMaxParallelFilterWorkers = 16;
  static const bool parallel_filter_disabled =
      ::tinylamb::platform::GetEnv(
          "TINYLAMB_DISABLE_PARALLEL_RELATION_FILTER") != nullptr;
  const size_t workers =
      (!parallel_filter_disabled && arena.rows >= kMinParallelFilterRows &&
       (!has_non_simple || programs.complete))
          ? std::min(
                {kMaxParallelFilterWorkers,
                 QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
                 arena.rows / 8192})
          : 1;

  auto row_passes_arena_precheck = [&](size_t r, size_t* key_rej,
                                       size_t* key_null_rej,
                                       bool* all_simple_decided) -> bool {
    if (runtime_filter != nullptr && runtime_filter->enabled()) {
      const slot_t col = runtime_filter->column;
      if (col < arena.stride) {
        const bool is_null = arena.IsNull(r, col);
        const ValueType col_type = arena.slot_types[col];
        const int64_t val = is_null ? 0 : arena.cells[(r * arena.stride) + col];
        if (runtime_filter->Reject(static_cast<int8_t>(col_type), is_null,
                                   val)) {
          ++(*key_rej);
          if (is_null) {
            ++(*key_null_rej);
          }
          return false;
        }
      }
    }
    *all_simple_decided = true;
    if (filter != nullptr) {
      for (const SimpleComparePredicate& pred : filter->simple) {
        const std::optional<bool> verdict =
            TryMatchSimpleCompareArena(arena, r, pred);
        if (!verdict.has_value()) {
          *all_simple_decided = false;
        } else if (!*verdict) {
          return false;
        }
      }
    }
    return true;
  };

  if (workers > 1) {
    std::vector<Relation> shards(workers);
    // Deferred rows record how many accepted rows precede them so the merge
    // below can reinsert them at their source position -- the same protocol
    // TryParallelFilterRows uses.
    struct DeferredRow {
      size_t preceding_rows;
      Row row;
    };
    std::vector<std::vector<DeferredRow>> deferred(workers);
    std::atomic<size_t> key_rejected{0};
    std::atomic<size_t> key_null_rejected{0};
    std::mutex error_mu;
    Status error_ex{Status::kSuccess};
    const size_t chunk_rows = (arena.rows + workers - 1) / workers;
    {
      std::vector<std::jthread> threads;
      threads.reserve(workers);
      for (size_t w = 0; w < workers; ++w) {
        try {
          threads.emplace_back([&, w] {
            try {
              auto& local = shards[w];
              size_t local_key_rej = 0;
              size_t local_key_null_rej = 0;
              size_t accepted_rows = 0;
              // Each worker owns one contiguous range; merging the ranges in
              // worker order then reproduces the serial scan's row order.
              const size_t begin = w * chunk_rows;
              const size_t end = std::min(arena.rows, begin + chunk_rows);
              // Reuse one scratch row: MaterializeSingleArenaRow resizes its
              // values vector, and keeping the allocation avoids one vector
              // (plus VARCHAR payload) free+alloc per accepted row.
              Row row;
              for (size_t r = begin; r < end; ++r) {
                bool all_simple_decided = true;
                if (!row_passes_arena_precheck(r, &local_key_rej,
                                               &local_key_null_rej,
                                               &all_simple_decided)) {
                  continue;
                }
                MaterializeSingleArenaRow(arena, cached_schema, r, &row);
                if (!has_non_simple && all_simple_decided) {
                  ++accepted_rows;
                  local.AddRow(std::move(row));
                  row = Row();
                  continue;
                }
                if (filter != nullptr && filter_view != nullptr &&
                    programs.complete) {
                  switch (MatchScanFilterWorker(row, *filter_view, *filter,
                                                programs)) {
                    case 0:
                      break;
                    case 1:
                      ++accepted_rows;
                      local.AddRow(std::move(row));
                      row = Row();
                      break;
                    default:
                      deferred[w].push_back({accepted_rows, std::move(row)});
                      row = Row();
                      break;
                  }
                } else {
                  deferred[w].push_back({accepted_rows, std::move(row)});
                  row = Row();
                }
              }
              key_rejected.fetch_add(local_key_rej, std::memory_order_relaxed);
              key_null_rejected.fetch_add(local_key_null_rej,
                                          std::memory_order_relaxed);
            } catch (...) {
              // This and the worker-creation catch below take error_mu in
              // disjoint, sequential scopes (no nested acquisition and no
              // second mutex), so the lock order cannot be inverted here.
              std::scoped_lock lock(error_mu);
              if (error_ex == Status::kSuccess) {
                error_ex =
                    StatusFromCurrentException("parallel filter worker failed");
              }
            }
          });
        } catch (...) {
          std::scoped_lock lock(error_mu);
          if (error_ex == Status::kSuccess) {
            error_ex =
                StatusFromCurrentException("parallel filter worker failed");
          }
          break;
        }
      }
    }
    if (error_ex != Status::kSuccess) {
      if (error != nullptr && error->ok()) {
        *error = error_ex;
      }
      return true;
    }
    const auto record_error = [&](Status status) {
      if (error != nullptr && error->ok() && status != Status::kSuccess) {
        *error = std::move(status);
      }
    };
    // Main-thread merge in worker order; deferred rows take the contextual
    // MatchScanFilter path so error semantics are bit-identical to serial,
    // and are reinserted at their source position via preceding_rows.
    for (size_t w = 0; w < workers; ++w) {
      record_error(shards[w].FinishSpill());
      if (deferred[w].empty() && !shards[w].HasSpill()) {
        result->MergeInMemoryShard(std::move(shards[w]));
      } else {
        size_t accepted_rows = 0;
        size_t deferred_index = 0;
        const auto emit_deferred = [&] {
          while (deferred_index < deferred[w].size() &&
                 deferred[w][deferred_index].preceding_rows == accepted_rows) {
            Row& row = deferred[w][deferred_index++].row;
            Status deferred_error{Status::kSuccess};
            if (filter == nullptr ||
                MatchScanFilter(row, *filter_view, *filter, outer, context,
                                ctes, &deferred_error)) {
              result->AddRow(std::move(row));
            }
            record_error(std::move(deferred_error));
          }
        };
        if (shards[w].HasSpill()) {
          record_error(shards[w].ForEachRow([&](const Row& row) {
            emit_deferred();
            result->AddRow(row);
            ++accepted_rows;
          }));
        } else {
          for (Row& row : shards[w].rows) {
            emit_deferred();
            result->AddRow(std::move(row));
            ++accepted_rows;
          }
        }
        emit_deferred();
      }
      deferred[w].clear();
      record_error(shards[w].GetStatus());
      shards[w].ResetContents();
    }
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->key_filter_rejected += key_rejected.load();
      context.execution_runtime()->key_filter_null_rejected +=
          key_null_rejected.load();
    }
    return true;
  }

  size_t key_rej = 0;
  size_t key_null_rej = 0;
  for (size_t r = 0; r < arena.rows; ++r) {
    if (error != nullptr && !error->ok()) {
      break;
    }
    bool all_simple_decided = true;
    if (!row_passes_arena_precheck(r, &key_rej, &key_null_rej,
                                   &all_simple_decided)) {
      continue;
    }
    Row row;
    MaterializeSingleArenaRow(arena, cached_schema, r, &row);
    if (!has_non_simple && all_simple_decided) {
      result->AddRow(std::move(row));
      continue;
    }
    if (filter == nullptr || MatchScanFilter(row, *filter_view, *filter, outer,
                                             context, ctes, error)) {
      result->AddRow(std::move(row));
    }
  }
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->key_filter_rejected += key_rej;
    context.execution_runtime()->key_filter_null_rejected += key_null_rej;
  }
  return true;
}

StatusOr<Relation> LoadSource(TransactionContext& context,
                              const SelectSource& source, const Scope* outer,
                              const CteMap& ctes,
                              const std::vector<slot_t>* projection,
                              const std::vector<Expression>* scan_predicates,
                              const ScanRuntimeFilter* int_runtime_filter,
                              const ScanTupleFilter* tuple_filter,
                              bool columnar_output,
                              const PushedScanProjection* pushed_projection,
                              bool* pushed_projection_applied) {
  if (pushed_projection_applied != nullptr) {
    *pushed_projection_applied = false;
  }
  bool pushed_proj_applied = false;
  const std::vector<Expression>* saved_scan_predicates = scan_predicates;
  if (int_runtime_filter != nullptr && !int_runtime_filter->enabled()) {
    int_runtime_filter = nullptr;
  }
  const ScanRuntimeFilter* saved_int_runtime_filter = int_runtime_filter;
  if (source.sample) {
    scan_predicates = nullptr;
    int_runtime_filter = nullptr;
    tuple_filter = nullptr;
    columnar_output = false;
    pushed_projection = nullptr;
  }
  // Fold row-independent subqueries to constants before filter compilation:
  // the folded compares join the typed SimpleCompare path, and query-free
  // predicates skip the contextual per-row filter and the cached-relation
  // snapshot a residual query would otherwise force.
  std::vector<Expression> folded_predicates;
  if (scan_predicates != nullptr && !scan_predicates->empty()) {
    folded_predicates =
        FoldRowIndependentSubqueries(context, *scan_predicates, outer, ctes);
    if (!folded_predicates.empty()) {
      scan_predicates = &folded_predicates;
    }
  }
  Relation result(context.execution_runtime());
  if (source.unnest) {
    ASSIGN_OR_RETURN(Value, array_val,
                     (TryEvaluate(source.unnest, Scope{.outer = outer}, nullptr,
                                  context, ctes)));
    result = UnnestValueToRelation(source, array_val);
  } else if (source.query) {
    const Relation* cached = nullptr;
    if (outer == nullptr && !source.sample &&
        source.query->WithQueries().empty() &&
        source.query->UnionAll().empty() &&
        !source.query->GetSetOperationTree() && !source.query->Qualify() &&
        source.query->DistinctOn().empty()) {
      cached = ExecuteCachedUncorrelated(context, *source.query, ctes);
    }
    if (cached != nullptr) {
      ASSIGN_OR_RETURN(Relation, materialized, MaterializeRelation(*cached));
      result = std::move(materialized);
    } else {
      ASSIGN_OR_RETURN(Relation, sub,
                       (ExecuteQuery(context, *source.query, outer, ctes,
                                     nullptr, columnar_output)));
      result = std::move(sub);
    }
    // Derived outputs carry bare (or inner-qualified) column names, while
    // outer predicates reference them through this source's alias
    // (QualifyExpression). Without alias qualification, LocalColumnOffset
    // cannot bind `alias.col`, join equalities go unresolved, and the join
    // degrades to a cross product plus filter (TPC-H Q20 at SF1: 800K x
    // derived rows instead of a hash join). Mirror the estimate path
    // (MakeScanNode), which already qualifies derived schemas.
    if (!source.alias.empty()) {
      result.schema = QualifySchema(result.schema, source.alias);
    }
  } else if (const auto cte = ctes.find(source.table); cte != ctes.end()) {
    ASSIGN_OR_RETURN(Relation, materialized,
                     (MaterializeRelation(*cte->second)));
    result = std::move(materialized);
  } else {
    bool reusable =
        tuple_filter == nullptr && context.execution_runtime() != nullptr &&
        context.execution_runtime()->reusable_base_relations.contains(
            source.table);
    const bool reusable_table = reusable;
    const std::string cache_key =
        BaseRelationCacheKey(source.table, projection);
    // Predicates attached to this source are written against the source's
    // alias, but cached/raw table schemas carry the table name (or no
    // qualifier at all).  Evaluate every scan filter against an alias-qualified
    // view of the schema; row layouts are positional so only names change.
    const std::string load_qualifier =
        source.alias.empty() ? source.table : source.alias;
    auto filter_view_of = [&load_qualifier](const Schema& schema) {
      return load_qualifier.empty() ? schema
                                    : QualifySchema(schema, load_qualifier);
    };
    RelationPtr cached_entry;
    SharedBaseRelationCache* const shared_cache =
        context.execution_runtime() != nullptr
            ? context.execution_runtime()->shared_base_cache.get()
            : nullptr;
    struct SharedBaseSlotGuard {
      SharedBaseRelationCache* cache{nullptr};
      std::string key;
      bool active{false};
      SharedBaseSlotGuard(SharedBaseRelationCache* c, std::string k)
          : cache(c), key(std::move(k)) {}
      SharedBaseSlotGuard(const SharedBaseSlotGuard&) = delete;
      SharedBaseSlotGuard& operator=(const SharedBaseSlotGuard&) = delete;
      SharedBaseSlotGuard(SharedBaseSlotGuard&&) = delete;
      SharedBaseSlotGuard& operator=(SharedBaseSlotGuard&&) = delete;
      ~SharedBaseSlotGuard() {
        if (active && cache != nullptr) {
          cache->AbandonBase(key);
        }
      }
    } shared_base_guard(shared_cache, cache_key);
    if (reusable) {
      const auto cached =
          context.execution_runtime()->base_relations.find(cache_key);
      if (cached != context.execution_runtime()->base_relations.end()) {
        cached_entry = cached->second;
      }
      StatusOr<std::shared_ptr<Table>> table_chk =
          context.GetTable(source.table);
      const bool large_lstable =
          table_chk.HasValue() &&
          table_chk.Value()->BackingColumnarRowCount() > 131072;
      if (!cached_entry && !large_lstable && reusable &&
          int_runtime_filter == nullptr && shared_cache != nullptr &&
          !cache_key.empty()) {
        RelationPtr shared_hit;
        if (!shared_cache->AcquireBaseSlot(cache_key, &shared_hit)) {
          if (shared_hit) {
            context.execution_runtime()->base_relations.emplace(cache_key,
                                                                shared_hit);
            cached_entry = std::move(shared_hit);
          }
        } else {
          shared_base_guard.active = true;
        }
      }
      // Direct aggregate scans can publish an empty reuse marker for an
      // in-memory LSTable even when the table has no persisted columnar rows.
      // Without a row arena this is not a materialized empty relation: rescan
      // the table, independent of its backing format or current row count.
      if (cached_entry && cached_entry->rows.empty() &&
          !cached_entry->HasSpill() && !cached_entry->HasColumnarOnly()) {
        const auto arena_it =
            context.execution_runtime()->base_relation_arenas.find(cache_key);
        const bool has_arena =
            arena_it !=
                context.execution_runtime()->base_relation_arenas.end() &&
            arena_it->second != nullptr && arena_it->second->rows > 0;
        if (!has_arena) {
          ++context.execution_runtime()->base_scan_cache_hits;
          cached_entry.reset();
          reusable = false;
        }
      } else if (!cached_entry && large_lstable) {
        auto marker = std::make_shared<Relation>(context.execution_runtime());
        context.execution_runtime()->base_relations.emplace(cache_key,
                                                            std::move(marker));
        reusable = false;
      } else if (!cached_entry && int_runtime_filter != nullptr) {
        reusable = false;
      }
    }
    const bool filter_during_scan =
        !reusable && scan_predicates != nullptr && !scan_predicates->empty();
    if (cached_entry) {
      // Materialize only rows that survive this alias's local predicates so
      // we never deep-copy a multi-million-row cache and filter afterwards.
      Relation& cached_relation = *cached_entry;
      result.schema = cached_relation.schema;
      RETURN_IF_FAIL(cached_relation.FinishSpill());
      RETURN_IF_FAIL(cached_relation.GetStatus());
      const BaseRelationArena* unboxed_arena = nullptr;
      if (cached_relation.rows.empty() && !cached_relation.HasSpill() &&
          context.execution_runtime() != nullptr) {
        const auto arena_it =
            context.execution_runtime()->base_relation_arenas.find(cache_key);
        if (arena_it !=
                context.execution_runtime()->base_relation_arenas.end() &&
            arena_it->second != nullptr && arena_it->second->rows > 0 &&
            arena_it->second->published_rowsless &&
            !arena_it->second->rows_boxed) {
          unboxed_arena = arena_it->second.get();
        }
      }
      if (scan_predicates == nullptr || scan_predicates->empty()) {
        Status key_error{Status::kSuccess};
        if (cached_relation.HasColumnarOnly() &&
            (int_runtime_filter == nullptr || !int_runtime_filter->enabled())) {
          result.SetColumnarTable(cached_relation.columnar,
                                  cached_relation.uniform_cell_types);
        } else if (unboxed_arena != nullptr &&
                   (int_runtime_filter != nullptr &&
                    int_runtime_filter->enabled()) &&
                   TryFilterArenaCache(context, *unboxed_arena,
                                       cached_relation.schema, nullptr, nullptr,
                                       outer, ctes, int_runtime_filter, &result,
                                       &key_error)) {
          RETURN_IF_FAIL(key_error);
        } else {
          if (unboxed_arena != nullptr && unboxed_arena->rows >= 32768) {
            RETURN_IF_FAIL(MaterializeCacheRowsParallel(
                cache_key, &cached_relation, context.execution_runtime()));
          } else {
            MaterializeCacheRowsIfNeeded(cache_key, &cached_relation,
                                         context.execution_runtime());
          }
          const CompiledScanFilter pass_through;
          if (TryParallelFilterRows(context, cached_relation, pass_through,
                                    cached_relation.schema, outer, ctes,
                                    int_runtime_filter, &result, &key_error)) {
            RETURN_IF_FAIL(key_error);
          } else {
            Status iteration = cached_relation.ForEachRow([&](const Row& row) {
              if (int_runtime_filter != nullptr &&
                  int_runtime_filter->enabled() &&
                  int_runtime_filter->column < row.Count()) {
                const Value& key = row[int_runtime_filter->column];
                if (int_runtime_filter->Reject(static_cast<int8_t>(key.type),
                                               key.IsNull(),
                                               key.value.int_value)) {
                  if (context.execution_runtime() != nullptr) {
                    ++context.execution_runtime()->key_filter_rejected;
                    if (key.IsNull()) {
                      ++context.execution_runtime()->key_filter_null_rejected;
                    }
                  }
                  return;
                }
                for (const ScanRuntimeFilter& ext : int_runtime_filter->extra) {
                  if (!ext.enabled() || ext.column >= row.Count()) {
                    continue;
                  }
                  const Value& ek = row[ext.column];
                  if (ext.Reject(static_cast<int8_t>(ek.type), ek.IsNull(),
                                 ek.value.int_value)) {
                    if (context.execution_runtime() != nullptr) {
                      ++context.execution_runtime()->key_filter_rejected;
                    }
                    return;
                  }
                }
              }
              result.AddRow(row);
            });
            RETURN_IF_FAIL(iteration);
          }
        }
      } else {
        const auto filter_begin = std::chrono::steady_clock::now();
        const std::vector<Expression> folded_scan_preds =
            FoldRowIndependentSubqueries(context, *scan_predicates, outer,
                                         ctes);
        if (unboxed_arena != nullptr && unboxed_arena->rows_boxed) {
          unboxed_arena = nullptr;
        }
        const std::vector<Expression>& effective_scan_preds =
            folded_scan_preds.empty() ? *scan_predicates : folded_scan_preds;
        const Schema filter_view = filter_view_of(cached_relation.schema);
        const FilterCompileEnv compile_env{
            .context = &context,
            .outer = outer,
            .ctes = &ctes,
            .input_rows = unboxed_arena != nullptr
                              ? unboxed_arena->rows
                              : cached_relation.TotalRows()};
        const CompiledScanFilter scan_filter =
            CompileScanFilter(effective_scan_preds, filter_view, &compile_env);
        Status eval_error{Status::kSuccess};
        const bool needs_snapshot = std::ranges::any_of(
            *scan_predicates, [&](const Expression& predicate) {
              return ContainsReentrantQueryIn(predicate, source.table);
            });
        if (!needs_snapshot && unboxed_arena != nullptr &&
            TryFilterArenaCache(context, *unboxed_arena, cached_relation.schema,
                                &scan_filter, &filter_view, outer, ctes,
                                int_runtime_filter, &result, &eval_error)) {
          RETURN_IF_FAIL(eval_error);
        } else {
          if (unboxed_arena != nullptr && unboxed_arena->rows >= 32768) {
            RETURN_IF_FAIL(MaterializeCacheRowsParallel(
                cache_key, &cached_relation, context.execution_runtime()));
          } else {
            MaterializeCacheRowsIfNeeded(cache_key, &cached_relation,
                                         context.execution_runtime());
          }
          auto emit_filtered = [&](const Relation& source_rel) -> Status {
            if (TryParallelFilterRows(
                    context, source_rel, scan_filter, filter_view, outer, ctes,
                    int_runtime_filter, &result, &eval_error)) {
              return eval_error;
            }
            Status iteration = source_rel.ForEachRow([&](const Row& row) {
              if (!eval_error.ok()) {
                return;
              }
              if (int_runtime_filter != nullptr &&
                  int_runtime_filter->enabled() &&
                  int_runtime_filter->column < row.Count()) {
                const Value& key = row[int_runtime_filter->column];
                if (int_runtime_filter->Reject(static_cast<int8_t>(key.type),
                                               key.IsNull(),
                                               key.value.int_value)) {
                  if (context.execution_runtime() != nullptr) {
                    ++context.execution_runtime()->key_filter_rejected;
                    if (key.IsNull()) {
                      ++context.execution_runtime()->key_filter_null_rejected;
                    }
                  }
                  return;
                }
                for (const ScanRuntimeFilter& ext : int_runtime_filter->extra) {
                  if (!ext.enabled() || ext.column >= row.Count()) {
                    continue;
                  }
                  const Value& ek = row[ext.column];
                  if (ext.Reject(static_cast<int8_t>(ek.type), ek.IsNull(),
                                 ek.value.int_value)) {
                    if (context.execution_runtime() != nullptr) {
                      ++context.execution_runtime()->key_filter_rejected;
                    }
                    return;
                  }
                }
              }
              if (MatchScanFilter(row, filter_view, scan_filter, outer, context,
                                  ctes, &eval_error)) {
                result.AddRow(row);
              }
            });
            if (iteration != Status::kSuccess) {
              return iteration;
            }
            return source_rel.GetStatus();
          };
          if (needs_snapshot) {
            ASSIGN_OR_RETURN(Relation, snapshot,
                             MaterializeRelation(cached_relation));
            RETURN_IF_FAIL(snapshot.FinishSpill());
            RETURN_IF_FAIL(snapshot.GetStatus());
            RETURN_IF_FAIL(emit_filtered(snapshot));
          } else {
            RETURN_IF_FAIL(emit_filtered(cached_relation));
          }
          RETURN_IF_FAIL(eval_error);
        }
        context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
      }
      ++context.execution_runtime()->base_scan_cache_hits;
    } else {
      StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
      if (!table.HasValue()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "table " + source.table + " not found");
      }
      const Schema& table_schema = table.Value()->GetSchema();
      result.schema = projection != nullptr
                          ? ProjectSchema(table_schema, *projection)
                          : table_schema;
      size_t table_input_rows = 0;
      if (!context.txn_.HasAnyPageVersionChains()) {
        std::shared_ptr<const LSTableStore> lst_store = GetOrBuildLSTableStore(
            context.txn_, *table.Value(), true, projection,
            /*allow_block_lazy=*/true);
        if (lst_store != nullptr && lst_store->uncommitted_owner_txn == 0 &&
            lst_store->layout_generation == table.Value()->LayoutGeneration() &&
            lst_store->first_pid == table.Value()->FirstPageId()) {
          table_input_rows = lst_store->total_rows;
          bool any_promoted = false;
          std::vector<Column> cols;
          cols.reserve(result.schema.ColumnCount());
          for (size_t ci = 0; ci < result.schema.ColumnCount(); ++ci) {
            Column col = result.schema.GetColumn(ci);
            const slot_t full_c = projection != nullptr
                                      ? (*projection)[ci]
                                      : static_cast<slot_t>(ci);
            if (col.GetConstraint().ctype == Constraint::kNothing &&
                full_c < lst_store->column_is_unique_nonnull.size() &&
                lst_store->column_is_unique_nonnull[full_c] != 0) {
              col = Column(col.Name(), col.Type(),
                           Constraint(Constraint::kPrimaryKey));
              any_promoted = true;
            }
            cols.push_back(std::move(col));
          }
          if (any_promoted) {
            result.schema = Schema(result.schema.Name(), std::move(cols));
          }
        }
      }
      if (table_input_rows == 0) {
        if (auto st = context.GetStats(source.table); st.HasValue()) {
          table_input_rows = st.Value()->Rows();
        }
      }
      const Schema filter_view = filter_view_of(result.schema);
      CompiledScanFilter scan_filter;
      if (filter_during_scan) {
        const std::vector<Expression> folded_scan_preds =
            FoldRowIndependentSubqueries(context, *scan_predicates, outer,
                                         ctes);
        const std::vector<Expression>& effective_scan_preds =
            folded_scan_preds.empty() ? *scan_predicates : folded_scan_preds;
        const FilterCompileEnv compile_env{.context = &context,
                                           .outer = outer,
                                           .ctes = &ctes,
                                           .input_rows = table_input_rows};
        scan_filter =
            CompileScanFilter(effective_scan_preds, filter_view, &compile_env);
      }
      const auto scan_begin = std::chrono::steady_clock::now();
      const auto filter_begin = scan_begin;
      // Prefer skipping full-row decode when a dynamic join filter is active.
      std::optional<slot_t> full_key_column;
      if (int_runtime_filter != nullptr && int_runtime_filter->enabled()) {
        if (projection != nullptr) {
          if (int_runtime_filter->column < projection->size()) {
            full_key_column = (*projection)[int_runtime_filter->column];
          }
        } else {
          full_key_column = int_runtime_filter->column;
        }
      }
      std::string keyed_cache_key;
      if (context.execution_runtime() != nullptr && reusable_table &&
          !columnar_output && !context.txn_.HasAnyPageVersionChains() &&
          (scan_predicates == nullptr || scan_predicates->empty()) &&
          full_key_column.has_value() && int_runtime_filter != nullptr &&
          int_runtime_filter->extra.empty() &&
          int_runtime_filter->group != nullptr &&
          int_runtime_filter->group->ExactKeys() != nullptr &&
          int_runtime_filter->group->ExactKeys()->size() <= 8192) {
        const RuntimeFilterGroup& fg = *int_runtime_filter->group;
        const std::vector<int64_t>& exact_keys = *fg.ExactKeys();
        uint64_t h_sum = 0;
        uint64_t h_xor = 0;
        for (int64_t k : exact_keys) {
          auto x = static_cast<uint64_t>(k) + 0x9e3779b97f4a7c15ULL;
          x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
          x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
          const uint64_t m = x ^ (x >> 31);
          h_sum += m;
          h_xor ^= m;
        }
        keyed_cache_key = "__keyed__:" + source.table + ":";
        AppendInteger(keyed_cache_key, *full_key_column);
        keyed_cache_key.push_back(':');
        AppendInteger(keyed_cache_key,
                      static_cast<int>(int_runtime_filter->type));
        keyed_cache_key.push_back(':');
        AppendInteger(keyed_cache_key, exact_keys.size());
        keyed_cache_key.push_back(':');
        AppendInteger(keyed_cache_key, fg.Lo());
        keyed_cache_key.push_back(':');
        AppendInteger(keyed_cache_key, fg.Hi());
        keyed_cache_key.push_back(':');
        AppendInteger(keyed_cache_key, h_sum);
        keyed_cache_key.push_back(':');
        AppendInteger(keyed_cache_key, h_xor);
        // The 64-bit digest can collide across different key sets; embed the
        // key bytes so a cache hit can only mean an identical key set.  Sort
        // first so build order does not split equivalent probes.
        std::vector<int64_t> sorted_keys(exact_keys);
        std::sort(sorted_keys.begin(), sorted_keys.end());
        keyed_cache_key.reserve(
            keyed_cache_key.size() +
            (sorted_keys
                 .size() *       // NOLINT(readability-math-missing-parentheses)
             sizeof(int64_t)));  // NOLINT(readability-math-missing-parentheses)
        for (int64_t k : sorted_keys) {
          // Copy the object representation explicitly instead of treating the
          // int64 object as a char array: a cache key only needs stable bytes
          // within this process, and memcpy keeps the aliasing/alignment
          // contract unambiguous.
          std::array<char, sizeof(k)> k_bytes{};
          std::memcpy(k_bytes.data(), &k, sizeof(k));
          keyed_cache_key.append(k_bytes.data(), k_bytes.size());
        }
        const auto k_it =
            context.execution_runtime()->base_relations.find(keyed_cache_key);
        if (k_it != context.execution_runtime()->base_relations.end() &&
            k_it->second != nullptr && !k_it->second->HasSpill()) {
          const Relation& cached_k = *k_it->second;
          std::vector<slot_t> col_map;
          col_map.reserve(result.schema.ColumnCount());
          bool all_found = true;
          for (size_t ci = 0; ci < result.schema.ColumnCount(); ++ci) {
            const Column& wcol = result.schema.GetColumn(ci);
            std::optional<slot_t> found_ci;
            for (size_t cci = 0; cci < cached_k.schema.ColumnCount(); ++cci) {
              const Column& ccol = cached_k.schema.GetColumn(cci);
              if (ccol.Name().name == wcol.Name().name &&
                  ccol.Type() == wcol.Type()) {
                found_ci = static_cast<slot_t>(cci);
                break;
              }
            }
            if (!found_ci.has_value()) {
              all_found = false;
              break;
            }
            col_map.push_back(*found_ci);
          }
          if (all_found) {
            result.rows.reserve(cached_k.rows.size());
            const size_t ncols = col_map.size();
            for (const Row& src_row : cached_k.rows) {
              Row dst_row;
              dst_row.values_.resize(ncols);
              for (size_t ci = 0; ci < ncols; ++ci) {
                dst_row.values_[ci] = src_row[col_map[ci]];
                if (result.schema.GetColumn(ci).IsUnsigned() &&
                    dst_row.values_[ci].type == ValueType::kInt64) {
                  dst_row.values_[ci].SetUnsigned();
                }
              }
              result.AddRow(std::move(dst_row));
            }
            ++context.execution_runtime()->base_scan_cache_hits;
            if (!source.alias.empty()) {
              result.schema = QualifySchema(result.schema, source.alias);
            } else if (!source.table.empty()) {
              result.schema = QualifySchema(result.schema, source.table);
            }
            return result;
          }
        }
      }
      // The low-level iterator rejects key-filter misses before row decode;
      // the parallel path applies the same rejection inside workers (whose
      // shard counters keep scan accounting; the per-candidate
      // rejected/null-rejected split is recorded shard-side).
      const bool iterator_handles_key_filter = full_key_column.has_value();
      const RuntimeFilterGroup* filter_group =
          iterator_handles_key_filter ? int_runtime_filter->group.get()
                                      : nullptr;
      const ValueType filter_type = iterator_handles_key_filter
                                        ? int_runtime_filter->type
                                        : ValueType::kNull;
      Status eval_error{Status::kSuccess};
      static const bool keyed_parallel_disabled =
          ::tinylamb::platform::GetEnv(
              "TINYLAMB_DISABLE_PARALLEL_KEYED_SCAN") != nullptr;
      bool indexed_ok = false;
      if (!reusable && (filter_during_scan || int_runtime_filter != nullptr)) {
        auto statistics = context.GetStats(source.table);
        if (statistics.HasValue()) {
          ASSIGN_OR_RETURN(bool, used_index,
                           TrySelectiveIndexSource(
                               context, *table.Value(), *statistics.Value(),
                               filter_view, projection, scan_filter,
                               int_runtime_filter, outer, ctes, &result));
          indexed_ok = used_index;
        }
      }
      bool effective_filter_during_scan = filter_during_scan;
      if (int_runtime_filter != nullptr && !int_runtime_filter->extra.empty()) {
        if (indexed_ok) {
          const auto keep_row = [&](const Row& row) {
            return std::ranges::none_of(
                int_runtime_filter->extra, [&](const ScanRuntimeFilter& ext) {
                  if (!ext.enabled() || ext.column >= row.Count()) {
                    return false;
                  }
                  const Value& key = row[ext.column];
                  return ext.Reject(static_cast<int8_t>(key.type), key.IsNull(),
                                    key.value.int_value);
                });
          };
          if (result.HasSpill()) {
            RETURN_IF_FAIL(result.FinishSpill());
            Relation filtered(context.execution_runtime());
            filtered.schema = result.schema;
            RETURN_IF_FAIL(result.ForEachRow([&](const Row& row) {
              if (keep_row(row)) filtered.AddRow(row);
            }));
            RETURN_IF_FAIL(filtered.FinishSpill());
            CopyExecutionStats(&filtered, result);
            result = std::move(filtered);
          } else {
            size_t out = 0;
            for (size_t i = 0; i < result.rows.size(); ++i) {
              if (keep_row(result.rows[i])) {
                if (out != i) {
                  result.rows[out] = std::move(result.rows[i]);
                }
                ++out;
              }
            }
            result.rows.resize(out);
          }
        } else {
          for (const ScanRuntimeFilter& ext : int_runtime_filter->extra) {
            if (!ext.enabled()) {
              continue;
            }
            PrebuiltMembershipPredicate pm;
            pm.column = ext.column;
            pm.negated = false;
            pm.domain_type = ext.type;
            pm.runtime_filter = ext.group;
            pm.existence = true;
            scan_filter.prebuilt_membership.push_back(std::move(pm));
            effective_filter_during_scan = true;
          }
        }
      }
      bool has_unsigned_view = false;
      for (size_t i = 0; i < table_schema.ColumnCount(); ++i) {
        if (table_schema.GetColumn(i).IsUnsigned()) {
          has_unsigned_view = true;
          break;
        }
      }
      if (!has_unsigned_view) {
        for (size_t i = 0; i < filter_view.ColumnCount(); ++i) {
          if (filter_view.GetColumn(i).IsUnsigned()) {
            has_unsigned_view = true;
            break;
          }
        }
      }
      const bool allow_scan_columnar = columnar_output &&
                                       keyed_cache_key.empty() &&
                                       !source.sample && !has_unsigned_view;
      const PushedScanProjection* scan_pushed_proj =
          (allow_scan_columnar && !reusable) ? pushed_projection : nullptr;
      const bool parallel_ok =
          indexed_ok ||
          ((filter_group == nullptr || !keyed_parallel_disabled) &&
           TryParallelTableScan(
               context, *table.Value(), projection, filter_group, filter_type,
               full_key_column, effective_filter_during_scan,
               effective_filter_during_scan ? &scan_filter : nullptr,
               filter_view, outer, ctes, &result, &eval_error, tuple_filter,
               allow_scan_columnar, scan_pushed_proj, &pushed_proj_applied));
      RETURN_IF_FAIL(eval_error);
      if (!parallel_ok) {
        // The canonical row predicate still decides every survivor. Signed
        // INT64/DATE peeks reject known misses before allocating and decoding
        // projected Rows, using the same schema/type guard as typed scans.
        const auto integer_peeks =
            effective_filter_during_scan
                ? BuildIntegerPeeks(scan_filter, projection, table_schema)
                : std::vector<IntegerPeekCompare>{};
        TableScanOptions options;
        if (projection != nullptr) {
          options.projection = *projection;
        }
        options.peek_compares =
            integer_peeks.empty() ? nullptr : &integer_peeks;
        if (iterator_handles_key_filter) {
          options.key_column = full_key_column;
          options.runtime_filter = filter_group;
          options.runtime_filter_type = filter_type;
        }
        Iterator iterator = table.Value()->BeginFullScan(context.txn_, options);
        while (iterator.IsValid()) {
          if (context.execution_runtime() != nullptr) {
            ++context.execution_runtime()->scan_rows;
            context.execution_runtime()->scan_values_available +=
                table_schema.ColumnCount();
            context.execution_runtime()->scan_values_decoded +=
                result.schema.ColumnCount();
          }
          bool matches = true;
          if (!iterator_handles_key_filter && int_runtime_filter != nullptr &&
              int_runtime_filter->enabled() &&
              int_runtime_filter->column < iterator->Count()) {
            const Value& key = (*iterator)[int_runtime_filter->column];
            if (int_runtime_filter->Reject(static_cast<int8_t>(key.type),
                                           key.IsNull(), key.value.int_value)) {
              matches = false;
              if (context.execution_runtime() != nullptr) {
                ++context.execution_runtime()->key_filter_rejected;
                if (key.IsNull()) {
                  ++context.execution_runtime()->key_filter_null_rejected;
                }
              }
            }
          }
          if (matches && effective_filter_during_scan) {
            matches = MatchScanFilter(*iterator, filter_view, scan_filter,
                                      outer, context, ctes, &eval_error);
            RETURN_IF_FAIL(eval_error);
          }
          if (matches) {
            // Move the iterator's row in: all reads of the row happened
            // above, and ++ re-deserializes into a fresh row anyway.  A
            // copy here materialized every scanned row twice.
            result.AddRow(std::move(*iterator));
            if (context.execution_runtime() != nullptr) {
              ++context.execution_runtime()->scan_output_rows;
            }
          }
          ++iterator;
        }
        RETURN_IF_FAIL(iterator.GetStatus());
        RETURN_IF_FAIL(eval_error);
      }
      RETURN_IF_FAIL(result.FinishSpill());
      RETURN_IF_FAIL(result.GetStatus());
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->scan_ms += ElapsedMs(scan_begin);
        if (filter_during_scan) {
          context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
        }
        bool filtered_from_cached = false;
        if (reusable && int_runtime_filter == nullptr) {
          if (result.HasColumnarOnly()) {
            auto cached = std::make_shared<Relation>(nullptr);
            cached->schema = result.schema;
            cached->SetColumnarTable(result.columnar,
                                     result.uniform_cell_types);
            context.execution_runtime()->base_relations.emplace(cache_key,
                                                                cached);
            if (shared_base_guard.active && shared_cache != nullptr) {
              shared_cache->PublishBase(cache_key, cached);
              shared_base_guard.active = false;
            }
          } else {
            auto cached = std::make_shared<Relation>(std::move(result));
            context.execution_runtime()->base_relations.emplace(cache_key,
                                                                cached);
            if (shared_base_guard.active && shared_cache != nullptr) {
              shared_cache->PublishBase(cache_key, cached);
              shared_base_guard.active = false;
            }
            result = Relation(context.execution_runtime());
            result.schema = cached->schema;
            if (scan_predicates != nullptr && !scan_predicates->empty()) {
              const Schema saved_schema = cached->schema;
              const Schema view = filter_view_of(saved_schema);
              const FilterCompileEnv compile_env{
                  .context = &context, .outer = outer, .ctes = &ctes};
              const CompiledScanFilter fill_filter =
                  CompileScanFilter(*scan_predicates, view, &compile_env);
              Status filter_error{Status::kSuccess};
              if (TryParallelFilterRows(context, *cached, fill_filter, view,
                                        outer, ctes, nullptr, &result,
                                        &filter_error, nullptr)) {
                RETURN_IF_FAIL(filter_error);
                RETURN_IF_FAIL(result.FinishSpill());
                RETURN_IF_FAIL(result.GetStatus());
                result.schema = saved_schema;
                filtered_from_cached = true;
              }
            }
            if (!filtered_from_cached) {
              bool copied_parallel = false;
              if (!cached->HasSpill() && cached->rows.size() >= 16384) {
                const CompiledScanFilter pass_through;
                Status copy_error{Status::kSuccess};
                if (TryParallelFilterRows(context, *cached, pass_through,
                                          cached->schema, outer, ctes, nullptr,
                                          &result, &copy_error)) {
                  RETURN_IF_FAIL(copy_error);
                  copied_parallel = true;
                }
              }
              if (!copied_parallel) {
                ASSIGN_OR_RETURN(Relation, cached_copy,
                                 MaterializeRelation(*cached));
                result = std::move(cached_copy);
              }
            }
          }
        } else if (!keyed_cache_key.empty() && !result.HasSpill() &&
                   !result.HasColumnarOnly() && result.rows.size() <= 65536) {
          auto& slot =
              context.execution_runtime()->base_relations[keyed_cache_key];
          if (slot == nullptr ||
              slot->schema.ColumnCount() < result.schema.ColumnCount()) {
            auto cached =
                std::make_shared<Relation>(context.execution_runtime());
            cached->schema = result.schema;
            cached->rows = result.rows;
            slot = std::move(cached);
          }
        }
        if (reusable && !filtered_from_cached && scan_predicates != nullptr &&
            !scan_predicates->empty()) {
          // Evaluate this alias's predicates against the alias-qualified view;
          // the stored schema stays neutral for cache sharing across aliases.
          const Schema saved_schema = result.schema;
          const Schema view = filter_view_of(saved_schema);
          result.schema = view;
          const FilterCompileEnv compile_env{
              .context = &context, .outer = outer, .ctes = &ctes};
          const CompiledScanFilter fill_filter =
              CompileScanFilter(*scan_predicates, view, &compile_env);
          Relation filtered(context.execution_runtime());
          filtered.schema = saved_schema;
          Status filter_error{Status::kSuccess};
          if (TryParallelFilterRows(context, result, fill_filter, view, outer,
                                    ctes, nullptr, &filtered, &filter_error,
                                    &result)) {
            RETURN_IF_FAIL(filter_error);
            RETURN_IF_FAIL(filtered.FinishSpill());
            RETURN_IF_FAIL(filtered.GetStatus());
            result = std::move(filtered);
          } else {
            RETURN_IF_FAIL(FilterRelation(context, &result, *scan_predicates,
                                          outer, ctes));
          }
          result.schema = saved_schema;
        }
      }
    }
  }
  // Values are stored as INT64 bit patterns, while the catalog retains the
  // SQL UINT64 declaration on the column.  Reattach that declaration to the
  // row values before expressions (notably `id < 0`) evaluate them.
  bool has_unsigned_column = false;
  for (size_t i = 0; i < result.schema.ColumnCount(); ++i) {
    has_unsigned_column =
        has_unsigned_column || result.schema.GetColumn(i).IsUnsigned();
  }
  if (has_unsigned_column) {
    RETURN_IF_FAIL(result.EnsureRowsMaterialized());
    RETURN_IF_FAIL(result.FinishSpill());
    RETURN_IF_FAIL(result.GetStatus());
    Relation normalized(context.execution_runtime());
    normalized.schema = result.schema;
    RETURN_IF_FAIL(result.ForEachRow([&](const Row& input) {
      Row row = input;
      for (size_t i = 0; i < row.Count() && i < normalized.schema.ColumnCount();
           ++i) {
        if (normalized.schema.GetColumn(i).IsUnsigned() &&
            row[i].type == ValueType::kInt64 && !row[i].IsUnsigned()) {
          row[i] = row[i].WithUnsigned();
        }
      }
      normalized.AddRow(std::move(row));
    }));
    RETURN_IF_FAIL(normalized.FinishSpill());
    RETURN_IF_FAIL(normalized.GetStatus());
    CopyExecutionStats(&normalized, result);
    result = std::move(normalized);
  }
  if (!pushed_proj_applied) {
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    // Unnest outputs carry the alias as qualifier too: bare element refs still
    // match by column name, while alias-qualified and alias-as-row references
    // (`s.field`, `SELECT s`) resolve uniformly with table sources.
    if (!qualifier.empty()) {
      result.schema = QualifySchema(result.schema, qualifier);
    }
  }
  if (pushed_projection_applied != nullptr) {
    *pushed_projection_applied = pushed_proj_applied;
  }
  if (source.sample) {
    RETURN_IF_FAIL(result.EnsureRowsMaterialized());
    ASSIGN_OR_RETURN(Relation, sampled,
                     (ApplyTableSample(context, source, std::move(result))));
    result = std::move(sampled);
    if (saved_scan_predicates != nullptr && !saved_scan_predicates->empty()) {
      RETURN_IF_FAIL(FilterRelation(context, &result, *saved_scan_predicates,
                                    outer, ctes));
    }
    if (saved_int_runtime_filter != nullptr &&
        saved_int_runtime_filter->enabled()) {
      const slot_t filter_column = saved_int_runtime_filter->column;
      if (filter_column >= result.schema.ColumnCount()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "TABLESAMPLE key filter column is out of range");
      }
      Relation filtered(context.execution_runtime());
      filtered.schema = result.schema;
      CopyExecutionStats(&filtered, result);
      RETURN_IF_FAIL(result.FinishSpill());
      RETURN_IF_FAIL(result.GetStatus());
      RETURN_IF_FAIL(result.ForEachRow([&](const Row& row) {
        const Value& key = row[filter_column];
        if (!saved_int_runtime_filter->Reject(static_cast<int8_t>(key.type),
                                              key.IsNull(),
                                              key.value.int_value)) {
          filtered.AddRow(row);
        } else if (context.execution_runtime() != nullptr) {
          ++context.execution_runtime()->key_filter_rejected;
          if (key.IsNull()) {
            ++context.execution_runtime()->key_filter_null_rejected;
          }
        }
      }));
      RETURN_IF_FAIL(filtered.FinishSpill());
      RETURN_IF_FAIL(filtered.GetStatus());
      result = std::move(filtered);
    }
  }
  if (!columnar_output && result.HasColumnarOnly()) {
    RETURN_IF_FAIL(result.EnsureRowsMaterialized());
  }
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
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

namespace {

// Replaces every row-independent subquery in `expression` with the constant
// value one probe of that subquery returns.  A kQueryExp node folds when all
// queries beneath it are uncorrelated (no outer-scope reference) and carry no
// volatile function: the value is then identical for every row of this
// statement's snapshot -- the same contract the per-row caches
// (uncorrelated_scalar_by_node / uncorrelated_membership_by_node) rely on.
// A node whose evaluation raises keeps its original form, so errors (scalar
// subquery with more than one row, collation conflicts, ...) stay with the
// per-row path, including conjunct-tolerant suppression of sibling errors.
Expression FoldRowIndependentSubqueriesIn(  // NOLINT(misc-no-recursion)
    TransactionContext& context, const Expression& expression,
    const Scope* outer, const CteMap& ctes, bool* changed) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    ExecutionRuntime* runtime = context.execution_runtime();
    // Scalar (Test() == nullptr) and EXISTS probes are row-independent when
    // uncorrelated; quantified probes (`x IN (SELECT ...)` with a row-side
    // Test) and ARRAY builds keep their per-row path here.
    if (runtime != nullptr && query.Test() == nullptr && !query.ArrayResult() &&
        ContainsOnlyUncorrelatedQueries(context, expression, ctes) &&
        !StatementContainsVolatileFunctionCached(runtime, *query.Query())) {
      Scope probe{.outer = outer};
      StatusOr<Value> value =
          TryEvaluate(expression, probe, nullptr, context, ctes);
      if (value.HasValue()) {
        *changed = true;
        return ConstantValueExp(value.MoveValue());
      }
    }
    return expression;
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  bool child_changed = false;
  for (Expression& child : children) {
    Expression folded =
        FoldRowIndependentSubqueriesIn(context, child, outer, ctes, changed);
    if (folded.get() != child.get()) {
      child = std::move(folded);
      child_changed = true;
    }
  }
  if (!child_changed) {
    return expression;
  }
  return WithExpressionChildren(expression, std::move(children));
}

// True when `expression` contains a kQueryExp whose statement scans `table`
// (directly or through nested sources): iterating this table's cached
// relation while such a query re-enters LoadSource for the same cache_key
// would break the spill-file read position, so the caller must snapshot
// first.  Subqueries over other tables (uncorrelated membership or scalar
// probes kept because they carry a row-side Test) iterate their own sources
// and need no copy.
bool ContainsReentrantQueryIn(  // NOLINT(misc-no-recursion)
    const Expression& expression, std::string_view table) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    std::unordered_map<std::string, size_t> counts;
    CountStatementTables(*query.Query(), &counts);
    // Compare via string_view: the map keys are few, so a scan avoids
    // materializing `table` into a std::string on every visited node.
    if (std::ranges::any_of(
            counts, [&](const auto& entry) { return entry.first == table; })) {
      return true;
    }
    return ContainsReentrantQueryIn(query.Test(), table);
  }
  return std::ranges::any_of(
      ExpressionChildren(expression),
      [&](const Expression& child) {  // NOLINT(misc-no-recursion)
        return ContainsReentrantQueryIn(child, table);
      });
}

}  // namespace

std::vector<Expression> FoldRowIndependentSubqueries(
    TransactionContext& context, const std::vector<Expression>& predicates,
    const Scope* outer, const CteMap& ctes) {
  static const bool fold_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_SUBQUERY_FOLD") != nullptr;
  if (fold_disabled || context.execution_runtime() == nullptr ||
      std::ranges::none_of(predicates, [](const Expression& predicate) {
        return ContainsQuery(predicate);
      })) {
    return predicates;
  }
  std::vector<Expression> folded;
  folded.reserve(predicates.size());
  bool changed = false;
  for (const Expression& predicate : predicates) {
    folded.push_back(FoldRowIndependentSubqueriesIn(context, predicate, outer,
                                                    ctes, &changed));
  }
  if (!changed) {
    return predicates;
  }
  return folded;
}

// LocalColumnOffset is defined in executor/detail/columnar_table.cpp.
std::vector<Expression> SplitDisjuncts(const Expression& expression) {
  // T2405: iterative flattening -- this mirror of rewrite.cpp's helper runs
  // on the scan-filter path where no rewrite-depth guard applies at all, so
  // a long OR chain hit the stack directly.  Right is pushed before Left
  // so the stack pops Left first and `parts` keeps the left-to-right leaf
  // order the recursion produced.
  std::vector<Expression> parts;
  std::vector<Expression> pending;
  if (expression) {
    pending.push_back(expression);
  }
  while (!pending.empty()) {
    const Expression node = std::move(pending.back());
    pending.pop_back();
    if (!node) {
      continue;  // a null OR side contributes nothing (old base case)
    }
    if (node->Type() == TypeTag::kBinaryExp &&
        node->AsBinaryExpression().Op() == BinaryOperation::kOr) {
      pending.push_back(node->AsBinaryExpression().Right());
      pending.push_back(node->AsBinaryExpression().Left());
    } else {
      parts.push_back(node);
    }
  }
  return parts;
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
Status FilterRelation(TransactionContext& context, Relation* relation,
                      const std::vector<Expression>& predicates,
                      const Scope* outer, const CteMap& ctes) {
  if (predicates.empty()) {
    return Status::kSuccess;
  }
  const std::vector<Expression> folded_predicates =
      FoldRowIndependentSubqueries(context, predicates, outer, ctes);
  const std::vector<Expression>& effective_predicates =
      folded_predicates.empty() ? predicates : folded_predicates;
  const auto filter_begin = std::chrono::steady_clock::now();
  const FilterCompileEnv compile_env{.context = &context,
                                     .outer = outer,
                                     .ctes = &ctes,
                                     .input_rows = relation->TotalRows(),
                                     .input_relation = relation};
  const CompiledScanFilter scan_filter =
      CompileScanFilter(effective_predicates, relation->schema, &compile_env);
  RETURN_IF_FAIL(relation->FinishSpill());
  RETURN_IF_FAIL(relation->GetStatus());
  if (scan_filter.residual.empty() || scan_filter.residual_bytecode) {
    Relation filtered(context.execution_runtime());
    filtered.schema = relation->schema;
    CopyExecutionStats(&filtered, *relation);
    Status par_error{Status::kSuccess};
    if (TryParallelFilterRows(context, *relation, scan_filter, relation->schema,
                              outer, ctes, nullptr, &filtered, &par_error,
                              relation)) {
      RETURN_IF_FAIL(par_error);
      RETURN_IF_FAIL(filtered.FinishSpill());
      RETURN_IF_FAIL(filtered.GetStatus());
      *relation = std::move(filtered);
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
      }
      return Status::kSuccess;
    }
  } else if (!scan_filter.simple.empty() || !scan_filter.prefix_in.empty() ||
             !scan_filter.prebuilt_membership.empty() ||
             !scan_filter.prebuilt_scalar_compare.empty() ||
             !scan_filter.disjunctive_branches.empty()) {
    CompiledScanFilter non_residual = scan_filter;
    non_residual.residual.clear();
    Relation prefiltered(context.execution_runtime());
    prefiltered.schema = relation->schema;
    CopyExecutionStats(&prefiltered, *relation);
    Status pre_error{Status::kSuccess};
    if (!TryParallelFilterRows(context, *relation, non_residual,
                               relation->schema, outer, ctes, nullptr,
                               &prefiltered, &pre_error, relation)) {
      RETURN_IF_FAIL(relation->EnsureRowsMaterialized());
      ColumnLookupCache pre_lookup;
      Status pre_iter = relation->ForEachRow([&](const Row& row) {
        if (!pre_error.ok()) {
          return;
        }
        if (MatchScanFilter(row, relation->schema, non_residual, outer, context,
                            ctes, &pre_error, &pre_lookup)) {
          prefiltered.AddRow(row);
        }
      });
      RETURN_IF_FAIL(pre_iter);
    }
    RETURN_IF_FAIL(pre_error);
    RETURN_IF_FAIL(prefiltered.FinishSpill());
    RETURN_IF_FAIL(prefiltered.GetStatus());
    *relation = std::move(prefiltered);
  }
  RETURN_IF_FAIL(relation->EnsureRowsMaterialized());
  Relation filtered(context.execution_runtime());
  filtered.schema = relation->schema;
  CopyExecutionStats(&filtered, *relation);
  ColumnLookupCache lookup_cache;
  Status eval_error{Status::kSuccess};
  Status iteration;
  {
    // Register the relation being filtered so a correlated-subquery build
    // triggered by one of the predicates can prune itself to this
    // pipeline's outer key domain (the loop below presents every outer
    // value the probes will ever see).
    ScopedCorrelatedOuter correlated_outer(context, *relation);
    iteration = relation->ForEachRow([&](const Row& row) {
      if (!eval_error.ok()) {
        return;
      }
      if (MatchScanFilter(row, relation->schema, scan_filter, outer, context,
                          ctes, &eval_error, &lookup_cache)) {
        filtered.AddRow(row);
      }
    });
  }
  if (iteration != Status::kSuccess) {
    return iteration;
  }
  RETURN_IF_FAIL(eval_error);
  RETURN_IF_FAIL(filtered.FinishSpill());
  RETURN_IF_FAIL(filtered.GetStatus());
  *relation = std::move(filtered);
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
  }
  return Status::kSuccess;
}

}  // namespace tinylamb::relational_detail
