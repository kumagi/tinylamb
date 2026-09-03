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
#include "expression/function_call_expression.hpp"
#include "expression/proto_text.hpp"
#include "expression/rewrite.hpp"
#include "query/statement.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
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
  // Unsigned integers share the INT64 storage but compare under unsigned
  // semantics in the ground-truth evaluator. The integer fast paths below
  // are all signed, so any unsigned involvement must go through
  // EvaluateBinary (e.g. UINT64_MAX, stored as bit pattern -1, is NOT < 0).
  if ((value.type == ValueType::kInt64 || value.type == ValueType::kDate) &&
      (value.IsUnsigned() || pred.constant.IsUnsigned())) {
    return Binary(pred.op, value, pred.constant).Truthy();
  }

  // The scan fast path must use the same coercions as the full expression
  // evaluator.  In particular, DATE/TIMESTAMP columns commonly receive bare
  // string literals from an IN list; rejecting the type mismatch here would
  // silently filter the matching row before the residual evaluator sees it.
  if (value.type == ValueType::kDate &&
      pred.constant.type == ValueType::kVarChar) {
    try {
      const int64_t rhs = ParseDateDays(pred.constant.value.varchar_value);
      const int64_t lhs = value.value.int_value;
      switch (pred.op) {
        case BinaryOperation::kEquals:
          return lhs == rhs;
        case BinaryOperation::kNotEquals:
          return lhs != rhs;
        case BinaryOperation::kLessThan:
          return lhs < rhs;
        case BinaryOperation::kLessThanEquals:
          return lhs <= rhs;
        case BinaryOperation::kGreaterThan:
          return lhs > rhs;
        case BinaryOperation::kGreaterThanEquals:
          return lhs >= rhs;
        default:
          break;
      }
    } catch (...) {
      return false;
    }
  }
  if (value.type == ValueType::kVarChar &&
      pred.constant.type == ValueType::kVarChar) {
    return Binary(pred.op, value, pred.constant).Truthy();
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

// Compile a single AND expression into a DisjunctiveBranch.
// Splits conjuncts and tries to compile each as SimpleCompare.
CompiledScanFilter::DisjunctiveBranch CompileAndBranch(
    const Expression& and_expr, const Schema& schema) {
  CompiledScanFilter::DisjunctiveBranch branch;
  std::vector<Expression> conjuncts = SplitConjuncts(and_expr);
  for (const Expression& conjunct : conjuncts) {
    if (auto simple = TryCompileSimpleCompare(conjunct, schema)) {
      branch.simple.push_back(std::move(*simple));
    } else {
      branch.residual.push_back(conjunct);
    }
  }
  return branch;
}

CompiledScanFilter CompileScanFilter(const std::vector<Expression>& predicates,
                                     const Schema& schema) {
  CompiledScanFilter compiled;
  for (const Expression& predicate : predicates) {
    if (!predicate) {
      continue;
    }
    // Decompose OR expressions into disjunctive branches.
    // Each OR branch is compiled independently into simple + residual.
    // Only create disjunctive branches when the branch has at least one
    // SimpleCompare predicate AND no query subexpressions.  Otherwise
    // the branch evaluation overhead exceeds the original tree traversal.
    if (predicate->Type() == TypeTag::kBinaryExp &&
        predicate->AsBinaryExpression().Op() == BinaryOperation::kOr) {
      std::vector<Expression> disjuncts =
          SplitDisjuncts(predicate);
      if (disjuncts.size() >= 2 && !ContainsQuery(predicate)) {
        bool all_branches_usable = true;
        std::vector<CompiledScanFilter::DisjunctiveBranch> branches;
        for (const Expression& disjunct : disjuncts) {
          auto branch = CompileAndBranch(disjunct, schema);
          if (branch.simple.empty()) {
            all_branches_usable = false;
            break;
          }
          branches.push_back(std::move(branch));
        }
        if (all_branches_usable) {
          for (auto& b : branches) {
            compiled.disjunctive_branches.push_back(std::move(b));
          }
          continue;
        }
      }
    }
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
  // Stored rows carry INT64 bit patterns for UINT64 columns; the catalog
  // schema retains the unsigned declaration. Reattach it before matching so
  // both the simple fast path and residual evaluation observe the same
  // unsigned-tagged values as post-scan normalization produces.
  const Row* match_row = &row;
  Row tagged;
  bool schema_has_unsigned = false;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    if (schema.GetColumn(i).IsUnsigned()) {
      schema_has_unsigned = true;
      break;
    }
  }
  if (schema_has_unsigned) {
    tagged = row;
    for (size_t i = 0; i < tagged.Size() && i < schema.ColumnCount(); ++i) {
      if (schema.GetColumn(i).IsUnsigned() &&
          tagged[i].type == ValueType::kInt64 && !tagged[i].IsUnsigned()) {
        tagged[i] = tagged[i].WithUnsigned();
      }
    }
    match_row = &tagged;
  }
  // Evaluate top-level simple predicates (conjunction).
  for (const SimpleComparePredicate& pred : filter.simple) {
    if (!MatchSimpleCompare(*match_row, pred)) {
      return false;
    }
  }
  // Evaluate disjunctive branches: at least one branch must fully pass.
  // Each branch has its own SimpleCompare predicates and residual expressions.
  // This avoids recursive expression tree evaluation for OR-of-ANDs patterns
  // (e.g. TPC-H Q20's 3-way brand filter).
  if (!filter.disjunctive_branches.empty()) {
    bool any_branch_passed = false;
    for (const auto& branch : filter.disjunctive_branches) {
      bool branch_ok = true;
      for (const SimpleComparePredicate& pred : branch.simple) {
        if (!MatchSimpleCompare(*match_row, pred)) {
          branch_ok = false;
          break;
        }
      }
      if (branch_ok && !branch.residual.empty()) {
        Scope scope{.row = match_row,
                    .schema = &schema,
                    .outer = outer};
        for (const Expression& predicate : branch.residual) {
          if (!Truthy(
                  Evaluate(predicate, scope, nullptr, context, ctes))) {
            branch_ok = false;
            break;
          }
        }
      }
      if (branch_ok) {
        any_branch_passed = true;
        break;
      }
    }
    if (!any_branch_passed) {
      return false;
    }
  }
  if (filter.residual.empty()) {
    return true;
  }
  Scope scope{.row = match_row, .schema = &schema, .outer = outer};
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
    const Column& schema_column = full_schema.GetColumn(peek.column);
    const ValueType type = schema_column.Type();
    if (type != ValueType::kInt64 && type != ValueType::kDate) {
      continue;
    }
    // Peek pre-filters compare signed integers without rechecking; an
    // unsigned column would mis-filter boundary values (see
    // MatchSimpleCompare), so leave unsigned columns to the exact filter.
    if (schema_column.IsUnsigned()) {
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
    if (!elements.empty()) {
      elem_type = elements[0].type;
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
        row_vals.push_back(Value(static_cast<int64_t>(row_idx)));
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
      std::vector<Value> values;
      bool null_row{false};
    };
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
        // Coerce textual members back to declared runtime types: struct
        // serialization stores DATE/TIMESTAMP cells as plain text.
        if (parsed.type == ValueType::kVarChar &&
            declared_fields.size() == members.size()) {
          const size_t member_index = parsed_rows[row_idx].values.size();
          const std::string& declared_type =
              declared_fields[member_index].second;
          std::string declared_text(parsed.value.varchar_value);
          if ((declared_type == "DATE" || declared_type == "date")) {
            int y = 0, m = 0, d = 0;
            if (sscanf(declared_text.c_str(), "%d-%d-%d", &y, &m, &d) == 3) {
              parsed = Value::Date(declared_text);
            }
          } else if (declared_type == "TIMESTAMP" ||
                     declared_type == "timestamp" ||
                     declared_type == "DATETIME") {
            int y = 0, mo = 0, d = 0;
            if (sscanf(declared_text.c_str(), "%d-%d-%d", &y, &mo, &d) == 3) {
              parsed =
                  Value(std::move(declared_text));  // keep canonical text form
            }
          }
        }
        parsed_rows[row_idx].values.push_back(std::move(parsed));
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
        // Fix up column types from the representative row's values.
        for (size_t i = 0;
             i < parsed_rows[row_idx].values.size() && i < fields.size(); ++i) {
          fields[i].second = parsed_rows[row_idx].values[i].IsNull()
                                 ? ValueType::kNull
                                 : parsed_rows[row_idx].values[i].type;
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
        for (size_t i = 0;
             i < fields.size() && i < parsed_rows[row_idx].values.size(); ++i) {
          row_vals[i] = parsed_rows[row_idx].values[i];
        }
      }
      if (keep_element_column) {
        row_vals.push_back(elements[row_idx]);
      }
      if (!source.offset_alias.empty()) {
        row_vals.push_back(Value(static_cast<int64_t>(row_idx)));
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
              if (context.execution_runtime() != nullptr) {
                ++context.execution_runtime()->key_filter_rejected;
                if (key.IsNull()) {
                  ++context.execution_runtime()->key_filter_null_rejected;
                }
              }
              return;
            }
          }
          result.AddRow(row);
        });
      } else {
        const auto filter_begin = std::chrono::steady_clock::now();
        const Schema filter_view = filter_view_of(cached_relation.schema);
        const CompiledScanFilter scan_filter =
            CompileScanFilter(*scan_predicates, filter_view);
        auto emit_filtered = [&](const Relation& source_rel) {
          source_rel.ForEachRow([&](const Row& row) {
            if (int_key_filter && int_key_column) {
              const Value& key = row[*int_key_column];
              if (key.IsNull() ||
                  !int_key_filter->contains(key.value.int_value)) {
                if (context.execution_runtime() != nullptr) {
                  ++context.execution_runtime()->key_filter_rejected;
                  if (key.IsNull()) {
                    ++context.execution_runtime()->key_filter_null_rejected;
                  }
                }
                return;
              }
            }
            if (MatchScanFilter(row, filter_view, scan_filter, outer, context,
                                ctes)) {
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
      const Schema filter_view = filter_view_of(result.schema);
      CompiledScanFilter scan_filter;
      if (filter_during_scan) {
        scan_filter = CompileScanFilter(*scan_predicates, filter_view);
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
      // Keep key-filter accounting in this path.  The low-level iterator can
      // reject keys before LoadSource sees a row, which is efficient but
      // makes runtime-filter profiles lose the rejected/null-rejected split.
      // Parallel scans use the same low-level fast path, so defer them while
      // profiling an integer key filter and account each candidate below.
      const bool iterator_handles_key_filter =
          full_key_column.has_value() && context.execution_runtime() == nullptr;
      const bool parallel_ok =
          context.execution_runtime() == nullptr || int_key_filter == nullptr
              ? TryParallelTableScan(
                    context, *table.Value(), projection,
                    iterator_handles_key_filter ? int_key_filter : nullptr,
                    iterator_handles_key_filter ? full_key_column
                                                : std::nullopt,
                    filter_during_scan,
                    filter_during_scan ? &scan_filter : nullptr, filter_view,
                    outer, ctes, &result)
              : false;
      if (!parallel_ok) {
        Iterator iterator = [&] {
          if (iterator_handles_key_filter) {
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
          if (!iterator_handles_key_filter && int_key_filter != nullptr &&
              int_key_column) {
            const Value& key = (*iterator)[*int_key_column];
            if (key.IsNull() ||
                !int_key_filter->contains(key.value.int_value)) {
              matches = false;
              if (context.execution_runtime() != nullptr) {
                ++context.execution_runtime()->key_filter_rejected;
                if (key.IsNull()) {
                  ++context.execution_runtime()->key_filter_null_rejected;
                }
              }
            }
          }
          if (matches && filter_during_scan) {
            matches = MatchScanFilter(*iterator, filter_view, scan_filter,
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
        // Evaluate this alias's predicates against the alias-qualified view;
        // the stored schema stays neutral for cache sharing across aliases.
        const Schema saved_schema = result.schema;
        result.schema = filter_view_of(saved_schema);
        FilterRelation(context, &result, *scan_predicates, outer, ctes);
        result.schema = saved_schema;
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
    Relation normalized(context.execution_runtime());
    normalized.schema = result.schema;
    result.ForEachRow([&](const Row& input) {
      Row row = input;
      for (size_t i = 0; i < row.Size() && i < normalized.schema.ColumnCount();
           ++i) {
        if (normalized.schema.GetColumn(i).IsUnsigned() &&
            row[i].type == ValueType::kInt64 && !row[i].IsUnsigned()) {
          row[i] = row[i].WithUnsigned();
        }
      }
      normalized.AddRow(std::move(row));
    });
    CopyExecutionStats(&normalized, result);
    result = std::move(normalized);
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
