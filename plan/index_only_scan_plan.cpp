/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//
// Created by kumagi on 22/06/19.
//

#include "index_only_scan_plan.hpp"

#include <cmath>
#include <utility>

#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/index_only_scan.hpp"
#include "expression/binary_expression.hpp"
#include "index/index.hpp"
#include "table/table.hpp"

namespace tinylamb {

IndexOnlyScanPlan::IndexOnlyScanPlan(const Table& table, const Index& index,
                                     const TableStatistics& ts,
                                     const Value& begin, const Value& end,
                                     bool ascending, Expression where)
    : table_(table),
      index_(index),
      stats_(ts.TransformBy(index.sc_.key_[0], begin, end)),
      begin_(begin),
      end_(end),
      ascending_(ascending),
      where_(std::move(where)),
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
  o << "IndexOnlyScan: " << table_.GetSchema().Name() << " with " << index_
    << " (estimated cost: " << AccessRowCount() << ")";
}

}  // namespace tinylamb