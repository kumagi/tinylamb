/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "index_only_scan_plan.hpp"

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "database/transaction_context.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "index/index.hpp"
#include "index/index_schema.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

bool OrderMatches(const std::vector<ColumnName>& provided, bool scan_ascending,
                  const std::vector<Expression>& expressions,
                  const std::vector<bool>& ascending) {
  if (provided.empty() || expressions.empty() ||
      expressions.size() != ascending.size() ||
      expressions.size() > provided.size()) {
    return false;
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (expressions[i]->Type() != TypeTag::kColumnValue) { return false;
}
    if (expressions[i]->AsColumnValue().GetColumnName().name !=
        provided[i].name) {
      return false;
    }
    if (ascending[i] != scan_ascending) { return false;
}
  }
  return true;
}

Value FirstOrNull(const std::vector<Value>& keys) {
  return keys.empty() ? Value() : keys.front();
}

}  // namespace

IndexOnlyScanPlan::IndexOnlyScanPlan(const Table& table, const Index& index,
                                     const TableStatistics& ts,
                                     std::vector<Value> begin_key,
                                     std::vector<Value> end_key,
                                     bool ascending, Expression where,
                                     std::vector<ColumnName> provided_order)
    : table_(table),
      index_(index),
      stats_(begin_key.empty() && end_key.empty()
                 ? ts
                 : ts.TransformBy(index.sc_.key_[0], FirstOrNull(begin_key),
                                  FirstOrNull(end_key))),
      begin_key_(std::move(begin_key)),
      end_key_(std::move(end_key)),
      ascending_(ascending),
      where_(std::move(where)),
      output_schema_(OutputSchema()),
      provided_order_(std::move(provided_order)) {}

Schema IndexOnlyScanPlan::OutputSchema() const {
  std::vector<Column> cols;
  cols.reserve(index_.sc_.key_.size() + index_.sc_.include_.size());
  for (const auto& key : index_.sc_.key_) {
    cols.push_back(table_.GetSchema().GetColumn(key));
  }
  for (const auto& included : index_.sc_.include_) {
    cols.push_back(table_.GetSchema().GetColumn(included));
  }
  return {"", cols};
}

// EmitExecutor lives in the relational factory (executor/relational_factory.cpp).

size_t IndexOnlyScanPlan::AccessRowCount() const { return EmitRowCount(); }

size_t IndexOnlyScanPlan::EmitRowCount() const {
  if (index_.IsUnique() && begin_key_ == end_key_ &&
      begin_key_.size() == index_.sc_.key_.size()) {
    return 1;
  }
  return stats_.Rows();
}

bool IndexOnlyScanPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending) const {
  return OrderMatches(provided_order_, ascending_, expressions, ascending);
}

void IndexOnlyScanPlan::Dump(std::ostream& o, int /*indent*/) const {
  o << "IndexOnlyScan: " << table_.GetSchema().Name() << " with " << index_
    << " (estimated cost: " << AccessRowCount() << ")";
}

std::string IndexOnlyScanPlan::ToString() const {
  return "IndexOnlyScan: " + std::string(table_.GetSchema().Name()) + " with " +
         index_.sc_.name_ +
         " (estimated cost: " + std::to_string(AccessRowCount()) + ")";
}

}  // namespace tinylamb
