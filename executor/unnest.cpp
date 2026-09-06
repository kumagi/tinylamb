/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/unnest.hpp"

#include <cstdio>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "plan/plan.hpp"
#include "query/statement.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

UnnestExecutor::UnnestExecutor(TransactionContext& ctx, Schema child_schema,
                               Executor child, Expression unnest_expr,
                               std::string alias, std::string offset_alias,
                               bool left_outer, Schema output_schema)
    : ctx_(ctx),
      child_schema_(std::move(child_schema)),
      child_(std::move(child)),
      unnest_expr_(std::move(unnest_expr)),
      alias_(std::move(alias)),
      offset_alias_(std::move(offset_alias)),
      left_outer_(left_outer),
      output_schema_(std::move(output_schema)) {}

namespace {

// GoogleSQL coerces array literal elements to the array's declared element
// type (`UNNEST([DATE '2000-01-02', '2000-10-20'])` must yield DATE cells),
// but a surviving mixed DATE/text array surfaces as a per-row type flip that
// vectorized consumers reject (column vector type mismatch).  Normalize
// textual elements back to the declared runtime type at the unnest boundary.
void CoerceElementsToDeclaredType(Value* array_val) {
  if (array_val == nullptr || !array_val->IsArray()) {
    return;
  }
  const std::string& declared = array_val->ArrayElementSqlType();
  if (declared != "DATE") {
    return;
  }
  std::vector<Value> coerced;
  bool changed = false;
  for (const Value& element : array_val->ArrayElements()) {
    if (element.IsNull() || element.type != ValueType::kVarChar) {
      coerced.push_back(element);
      continue;
    }
    const std::string text(element.value.varchar_value);
    int y = 0, m = 0, d = 0;
    // Best-effort gate only; Value::Date() revalidates the parsed text below.
    // NOLINTNEXTLINE(cert-err34-c)
    if (sscanf(text.c_str(), "%d-%d-%d", &y, &m, &d) == 3) {
      coerced.push_back(Value::Date(text));
      changed = true;
    } else {
      coerced.push_back(element);
    }
  }
  if (changed) {
    *array_val = Value::Array(std::move(coerced), declared);
  }
}

// Re-projects one unnest relation onto the plan's declared unnest section.
// The relation shape is inferred per array value (a NULL element decodes as a
// scalar cell while a sibling row flattens struct members), so the width can
// disagree between rows.  Matching the plan schema by (lower-cased) column
// name keeps vectorized consumers that pin the chunk layout on the first row
// from rejecting later rows ("data chunk row width mismatch").
std::vector<Row> AlignRowsToSchema(const Schema& plan_schema,
                                   const Schema& rel_schema,
                                   std::vector<Row> rows) {
  std::vector<Column> columns;
  columns.reserve(rel_schema.ColumnCount());
  for (size_t i = 0; i < rel_schema.ColumnCount(); ++i) {
    columns.emplace_back(
        ColumnName(plan_schema.Name(), rel_schema.GetColumn(i).Name().name),
        rel_schema.GetColumn(i).Type());
  }
  const Schema qualified(plan_schema.Name(), std::move(columns));
  std::vector<int> offsets;
  offsets.reserve(plan_schema.ColumnCount());
  bool identity = plan_schema.ColumnCount() == rel_schema.ColumnCount();
  for (size_t i = 0; i < plan_schema.ColumnCount(); ++i) {
    const int offset = qualified.Offset(plan_schema.GetColumn(i).Name());
    if (offset < 0 || std::cmp_not_equal(offset, i)) {
      identity = false;
    }
    offsets.push_back(offset);
  }
  if (identity) {
    return rows;
  }
  for (Row& row : rows) {
    std::vector<Value> aligned;
    aligned.reserve(offsets.size());
    for (const int offset : offsets) {
      aligned.push_back(offset >= 0 &&
                                static_cast<size_t>(offset) < row.values_.size()
                            ? row[static_cast<size_t>(offset)]
                            : Value());
    }
    row = Row(std::move(aligned));
  }
  return rows;
}

}  // namespace

bool UnnestExecutor::Next(Row* dst, RowPosition* rp) {
  while (true) {
    if (current_row_index_ < current_unnested_rows_.size()) {
      const Row& unnested_row = current_unnested_rows_[current_row_index_++];
      if (current_child_row_.values_.empty()) {
        *dst = unnested_row;
      } else {
        *dst = current_child_row_ + unnested_row;
      }
      if (rp != nullptr) {
        *rp = RowPosition();
      }
      return true;
    }

    if (child_exhausted_) {
      return false;
    }

    current_child_row_ = Row();
    RowPosition child_rp;
    if (!child_->Next(&current_child_row_, &child_rp)) {
      child_exhausted_ = true;
      return false;
    }

    current_unnested_rows_.clear();
    current_row_index_ = 0;

    relational_detail::Scope scope;
    if (!current_child_row_.values_.empty()) {
      scope.row = &current_child_row_;
      scope.schema = &child_schema_;
    }
    relational_detail::CteMap ctes;

    Value array_val =
        relational_detail::Evaluate(unnest_expr_, scope, nullptr, ctx_, ctes);
    CoerceElementsToDeclaredType(&array_val);

    SelectSource source;
    source.alias = alias_.empty() ? "unnest" : alias_;
    source.offset_alias = offset_alias_;
    relational_detail::Relation rel =
        relational_detail::UnnestValueToRelation(source, array_val);
    if (output_schema_.ColumnCount() > 0) {
      rel.rows =
          AlignRowsToSchema(output_schema_, rel.schema, std::move(rel.rows));
    }
    current_unnested_rows_ = std::move(rel.rows);
    if (current_unnested_rows_.empty() && left_outer_) {
      // OUTER unnest: an empty (or NULL) array emits one row of NULLs so the
      // driving row survives with NULL-extended unnest columns.
      const slot_t width = output_schema_.ColumnCount() > 0
                               ? output_schema_.ColumnCount()
                               : rel.schema.ColumnCount();
      std::vector<Value> nulls(width);
      current_unnested_rows_.emplace_back(std::move(nulls));
    }
  }
}

void UnnestExecutor::Dump(std::ostream& o, int indent) const {
  o << "Unnest: " << (unnest_expr_ ? unnest_expr_->ToString() : "");
  if (!alias_.empty()) {
    o << " AS " << alias_;
  }
  if (!offset_alias_.empty()) {
    o << " WITH OFFSET " << offset_alias_;
  }
  o << "\n";
  if (child_) {
    o << Indent(static_cast<size_t>(indent) + 2);
    child_->Dump(o, indent + 2);
  }
}

}  // namespace tinylamb
