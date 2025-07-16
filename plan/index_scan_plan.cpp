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

#include "index_scan_plan.hpp"

#include <cmath>
#include <cstddef>
#include <memory>
#include <ostream>
#include <utility>

#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "executor/index_scan.hpp"
#include "expression/expression.hpp"
#include "index/index.hpp"
#include "table/table.hpp"
#include "type/value.hpp"

namespace tinylamb {

IndexScanPlan::IndexScanPlan(const Table& table, const Index& index,
                             const TableStatistics& ts, const Value& begin,
                             const Value& end, bool ascending, Expression where)
    : table_(table),
      index_(index),
      stats_(ts.TransformBy(index.sc_.key_[0], begin, end)),
      begin_(begin),
      end_(end),
      ascending_(ascending),
      where_(std::move(where)) {}

Executor IndexScanPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<IndexScan>(ctx.txn_, table_, index_, begin_, end_,
                                     ascending_, where_, GetSchema());
}

const Schema& IndexScanPlan::GetSchema() const { return table_.GetSchema(); }

size_t IndexScanPlan::AccessRowCount() const { return EmitRowCount() * 2; }

size_t IndexScanPlan::EmitRowCount() const {
  if (index_.IsUnique() && begin_ == end_) {
    return 1;
  }
  return std::ceil(stats_.EstimateCount(index_.sc_.key_[0], begin_, end_));
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