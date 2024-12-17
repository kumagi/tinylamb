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

#include "full_scan_plan.hpp"

#include <cstddef>
#include <memory>
#include <ostream>

#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "executor/full_scan.hpp"
#include "table/table.hpp"

namespace tinylamb {

FullScanPlan::FullScanPlan(const Table& table, const TableStatistics& stats)
    : table_(table), stats_(stats) {}

Executor FullScanPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<FullScan>(ctx.txn_, table_);
}

const Schema& FullScanPlan::GetSchema() const { return table_.GetSchema(); }

size_t FullScanPlan::AccessRowCount() const { return stats_.Rows(); }

size_t FullScanPlan::EmitRowCount() const { return stats_.Rows(); }

void FullScanPlan::Dump(std::ostream& o, int /*indent*/) const {
  o << "FullScan: " << table_.GetSchema().Name()
    << "(estimated cost: " << AccessRowCount() << ")";
}

}  // namespace tinylamb