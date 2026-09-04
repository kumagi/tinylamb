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

#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <ostream>
#include <string>

#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "plan/parallel_thresholds.hpp"
#include "table/table.hpp"

namespace tinylamb {

FullScanPlan::FullScanPlan(const Table& table, const TableStatistics& stats,
                           size_t max_rows)
    : table_(table), stats_(stats), max_rows_(max_rows) {}

FullScanPlan::FullScanPlan(const Table& table, const TableStatistics& stats,
                           std::vector<IntegerPeekCompare> peek_compares,
                           size_t max_rows)
    : table_(table),
      stats_(stats),
      max_rows_(max_rows),
      peek_compares_(std::move(peek_compares)) {}

// EmitExecutor lives in the relational factory
// (executor/relational_factory.cpp).

const Schema& FullScanPlan::GetSchema() const { return table_.GetSchema(); }

size_t FullScanPlan::AccessRowCount() const {
  return std::min(stats_.Rows(), max_rows_);
}

size_t FullScanPlan::EmitRowCount() const {
  return std::min(stats_.Rows(), max_rows_);
}

void FullScanPlan::Dump(std::ostream& o, int /*indent*/) const {
  o << "FullScan: " << table_.GetSchema().Name()
    << "(estimated cost: " << AccessRowCount();
  if (max_rows_ != std::numeric_limits<size_t>::max()) {
    o << ", max rows: " << max_rows_;
  }
  o << ")";
}

std::string FullScanPlan::ToString() const {
  std::string result = "FullScan: " + std::string(table_.GetSchema().Name()) +
                       "(estimated cost: " + std::to_string(AccessRowCount());
  if (max_rows_ != std::numeric_limits<size_t>::max()) {
    result += ", max rows: " + std::to_string(max_rows_);
  }
  return result + ")";
}

}  // namespace tinylamb
