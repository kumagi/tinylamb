/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/relation_rename_plan.hpp"

#include <ostream>
#include <sstream>
#include <utility>

#include "common/constants.hpp"
#include "expression/column_value.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"

namespace tinylamb {
namespace {

// Rewrites column qualifiers equal to `from` into `to` inside a requested
// ordering expression so it can be matched against the child's physical
// schema.
Expression TranslateQualifier(const Expression& expression,
                              const std::string& from,
                              const std::string& to) {
  if (!expression ||
      expression->Type() != TypeTag::kColumnValue) {
    return expression;
  }
  ColumnName column = expression->AsColumnValue().GetColumnName();
  if (column.schema != from) return expression;
  column.schema = to;
  return ColumnValueExp(column);
}

}  // namespace

RelationRenamePlan::RelationRenamePlan(Plan src, std::string relation,
                                       std::string physical)
    : src_(std::move(src)),
      relation_(std::move(relation)),
      physical_(std::move(physical)) {
  std::vector<Column> columns;
  const Schema& source = src_->GetSchema();
  columns.reserve(source.ColumnCount());
  for (size_t i = 0; i < source.ColumnCount(); ++i) {
    const ColumnName& column = source.GetColumn(i).Name();
    columns.emplace_back(ColumnName(relation_, column.name));
  }
  renamed_schema_ = Schema("", std::move(columns));
}

Executor RelationRenamePlan::EmitExecutor(TransactionContext& ctx) const {
  // Rows are positional: streaming the child through unchanged is exactly a
  // rename, and row positions survive for UPDATE/DELETE consumers. The
  // wrapper exists so EXPLAIN surfaces the rename boundary.
  return std::make_shared<RelationRenameExecutor>(src_->EmitExecutor(ctx),
                                                  relation_, physical_);
}

void RelationRenameExecutor::Dump(std::ostream& o, int indent) const {
  o << "Rename: " << physical_ << " AS " << relation_ << "\n"
    << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

const Schema& RelationRenamePlan::GetSchema() const { return renamed_schema_; }

bool RelationRenamePlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending) const {
  std::vector<Expression> translated;
  translated.reserve(expressions.size());
  for (const Expression& expression : expressions) {
    translated.push_back(
        TranslateQualifier(expression, relation_, physical_));
  }
  return src_->IsOrderedBy(translated, ascending);
}

void RelationRenamePlan::Dump(std::ostream& o, int indent) const {
  o << "Rename: " << physical_ << " AS " << relation_ << "\n"
    << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

std::string RelationRenamePlan::ToString() const {
  std::ostringstream out;
  out << "Rename: " << physical_ << " AS " << relation_;
  return out.str();
}

}  // namespace tinylamb
