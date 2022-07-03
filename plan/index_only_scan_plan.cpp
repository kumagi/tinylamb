//
// Created by kumagi on 22/06/19.
//

#include "index_only_scan_plan.hpp"

#include <cmath>
#include <utility>

#include "database/relation_storage.hpp"
#include "database/transaction_context.hpp"
#include "executor/index_only_scan.hpp"
#include "expression/binary_expression.hpp"
#include "index/index.hpp"
#include "table/table.hpp"

namespace tinylamb {

IndexOnlyScanPlan::IndexOnlyScanPlan(const Table& table, const Index& index,
                                     const TableStatistics& ts,
                                     const Value& begin, const Value& end,
                                     bool ascending, Expression where,
                                     std::vector<NamedExpression> select)
    : table_(table),
      index_(index),
      stats_(ts),
      begin_(begin),
      end_(end),
      ascending_(ascending),
      where_(std::move(where)),
      select_(std::move(select)),
      output_schema_(OutputSchema()) {}

Schema IndexOnlyScanPlan::OutputSchema() const {
  std::vector<Column> out;
  for (const auto& col_id : index_.sc_.key_) {
    out.push_back(table_.GetSchema().GetColumn(col_id));
  }
  for (const auto& col_id : index_.sc_.include_) {
    out.push_back(table_.GetSchema().GetColumn(col_id));
  }
  return {"", out};
}

Executor IndexOnlyScanPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<IndexOnlyScan>(ctx.txn_, table_, index_, begin_, end_,
                                         ascending_, where_,
                                         table_.GetSchema());
}

size_t IndexOnlyScanPlan::AccessRowCount() const { return EmitRowCount(); }

size_t IndexOnlyScanPlan::EmitRowCount() const {
  if (index_.IsUnique() && begin_ == end_) {
    return 1;
  }
  return std::ceil(stats_.EstimateCount(index_.sc_.key_[0], begin_, end_));
}

void IndexOnlyScanPlan::Dump(std::ostream& o, int /*indent*/) const {
  o << "IndexOnlyScan: " << table_.GetSchema().Name()
    << " (estimated cost: " << AccessRowCount() << ")";
}

}  // namespace tinylamb