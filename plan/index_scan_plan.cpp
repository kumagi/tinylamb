/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "index_scan_plan.hpp"

#include <cstddef>
#include <memory>

#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "executor/full_scan.hpp"
#include "executor/index_scan.hpp"
#include "executor/selection.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "index/index.hpp"
#include "table/table.hpp"
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
    if (expressions[i]->Type() != TypeTag::kColumnValue) return false;
    const ColumnName& column =
        expressions[i]->AsColumnValue().GetColumnName();
    if (column.name != provided[i].name) return false;
    if (ascending[i] != scan_ascending) return false;
  }
  return true;
}

Value FirstOrNull(const std::vector<Value>& keys) {
  return keys.empty() ? Value() : keys.front();
}

}  // namespace

IndexScanPlan::IndexScanPlan(const Table& table, const Index& index,
                             const TableStatistics& ts,
                             std::vector<Value> begin_key,
                             std::vector<Value> end_key, bool ascending,
                             Expression where,
                             std::vector<ColumnName> provided_order)
    : table_(table),
      index_(index),
      stats_(ts.TransformBy(index.sc_.key_[0], FirstOrNull(begin_key),
                            FirstOrNull(end_key))),
      begin_key_(std::move(begin_key)),
      end_key_(std::move(end_key)),
      ascending_(ascending),
      where_(std::move(where)),
      provided_order_(std::move(provided_order)) {}

Executor IndexScanPlan::EmitExecutor(TransactionContext& ctx) const {
  if (ctx.txn_.RequiresHistoricalRead()) {
    Executor scan = std::make_shared<FullScan>(ctx.txn_, table_);
    return std::make_shared<Selection>(where_, table_.GetSchema(),
                                       std::move(scan));
  }
  return std::make_shared<IndexScan>(ctx.txn_, table_, index_, begin_key_,
                                     end_key_, ascending_, where_, GetSchema());
}

const Schema& IndexScanPlan::GetSchema() const { return table_.GetSchema(); }

size_t IndexScanPlan::AccessRowCount() const { return EmitRowCount(); }

size_t IndexScanPlan::EmitRowCount() const {
  if (index_.IsUnique() && begin_key_ == end_key_ &&
      begin_key_.size() == index_.sc_.key_.size()) {
    return 1;
  }
  return stats_.Rows();
}

bool IndexScanPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                                const std::vector<bool>& ascending) const {
  return OrderMatches(provided_order_, ascending_, expressions, ascending);
}

void IndexScanPlan::Dump(std::ostream& o, int /*indent*/) const {
  o << "IndexScan: " << table_.GetSchema().Name()
    << " (estimated cost: " << AccessRowCount() << ")";
}

std::string IndexScanPlan::ToString() const {
  return "IndexScan: " + std::string(table_.GetSchema().Name()) +
         " (estimated cost: " + std::to_string(AccessRowCount()) + ")";
}

}  // namespace tinylamb
