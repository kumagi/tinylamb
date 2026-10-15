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
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "expression/column_value.hpp"
#include "index/index.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/type.hpp"

namespace tinylamb {
namespace {

// The catalog half of the proof: a UNIQUE index whose key is exactly this
// column, on a column the schema declares NOT NULL. CREATE TABLE builds one
// index per PRIMARY KEY / UNIQUE column, so a plain `PRIMARY KEY` table
// qualifies without extra DDL. The NOT NULL declaration is what makes the null
// placement of the reported order vacuous.
bool CatalogKeyCandidate(const Table& table, slot_t column) {
  const Schema& schema = table.GetSchema();
  if (column >= schema.ColumnCount()) {
    return false;
  }
  const Constraint::ConstraintType ctype =
      schema.GetColumn(column).GetConstraint().ctype;
  if (ctype != Constraint::kNotNull && ctype != Constraint::kPrimaryKey) {
    return false;
  }
  for (size_t offset = 0; offset < table.IndexCount(); ++offset) {
    const Index& index = table.GetIndex(offset);
    if (index.IsUnique() && index.sc_.key_.size() == 1 &&
        index.sc_.key_.front() == column) {
      return true;
    }
  }
  return false;
}

// All three halves for one column. Split out so a request for a specific key
// (IsOrderedBy) does not have to take the first qualifying column.
bool ColumnIsClustered(const Table& table, const TableStatistics& stats,
                       slot_t column) {
  // The measurement half of the proof. A zero generation means "never measured
  // here" (never ANALYZEd in this process, a derived statistics object, or a
  // payload written by an older format); a mismatch means the chain moved since
  // the walk. A latched in-place rewrite additionally means an older snapshot
  // can still resolve a pre-image whose value the walk never saw.
  const uint64_t generation = stats.LayoutGeneration();
  if (generation == 0 || generation != table.LayoutGeneration() ||
      table.RowsRewrittenInPlace()) {
    return false;
  }
  if (column >= stats.Columns()) {
    return false;
  }
  // Correlation 1.0 means every adjacent non-NULL pair ANALYZE observed was
  // non-decreasing. With the NOT NULL catalog gate there are no NULLs to skip,
  // so that is exactly "the scan emits non-decreasing values".
  return stats.Column(column).Correlation() >= 1.0 &&
         CatalogKeyCandidate(table, column);
}

}  // namespace

slot_t ClusteredOrderColumn(const Table& table, const TableStatistics& stats) {
  const size_t columns = std::min(
      stats.Columns(), static_cast<size_t>(table.GetSchema().ColumnCount()));
  for (size_t column = 0; column < columns; ++column) {
    if (ColumnIsClustered(table, stats, static_cast<slot_t>(column))) {
      return static_cast<slot_t>(column);
    }
  }
  return kNoClusteredColumn;
}

FullScanPlan::FullScanPlan(const Table& table, TableStatistics stats,
                           size_t max_rows, bool lock_rows,
                           bool wait_for_write_intent)
    : lock_rows_(lock_rows),
      wait_for_write_intent_(wait_for_write_intent),
      table_(table),
      stats_(std::move(stats)),
      max_rows_(max_rows) {}
FullScanPlan::FullScanPlan(const Table& table, TableStatistics stats,
                           std::vector<IntegerPeekCompare> peek_compares,
                           size_t max_rows, bool lock_rows,
                           bool wait_for_write_intent)
    : lock_rows_(lock_rows),
      wait_for_write_intent_(wait_for_write_intent),
      table_(table),
      stats_(std::move(stats)),
      max_rows_(max_rows),
      peek_compares_(std::move(peek_compares)) {}

FullScanPlan::FullScanPlan(const FullScanPlan& base,
                           std::vector<slot_t> projection)
    : lock_rows_(base.lock_rows_),
      wait_for_write_intent_(base.wait_for_write_intent_),
      table_(base.table_),
      stats_(base.stats_),
      max_rows_(base.max_rows_),
      peek_compares_(base.peek_compares_),
      projection_(std::move(projection)) {
  // The narrowed copy is a read-shape plan: keep the base's gates (DML
  // sources, peek compares, max rows) but never carry the clustered-order
  // claim past a projection, since the claiming column may not be emitted.
  std::vector<Column> columns;
  columns.reserve(projection_->size());
  std::vector<ColumnStats> column_stats;
  column_stats.reserve(projection_->size());
  const Schema& source = table_.GetSchema();
  for (const slot_t slot : *projection_) {
    columns.push_back(source.GetColumn(slot));
    column_stats.push_back(base.stats_.Column(slot));
  }
  projected_schema_ = Schema(source.Name(), std::move(columns));
  projected_stats_.Assign(base.stats_.Rows(), std::move(column_stats),
                          base.stats_.LayoutGeneration());
}

bool AnyResolvedTableHasClusteredOrder(const TransactionContext& ctx) {
  return std::ranges::any_of(ctx.tables_, [&](const auto& entry) {
    if (entry.second == nullptr) {
      return false;
    }
    const auto stats = ctx.stats_.find(entry.first);
    if (stats == ctx.stats_.end() || stats->second == nullptr) {
      return false;
    }
    return ClusteredOrderColumn(*entry.second, *stats->second) !=
           kNoClusteredColumn;
  });
}

// EmitExecutor lives in the relational factory
// (executor/relational_factory.cpp). GetSchema/GetStats are defined in the
// header: a narrowed scan presents its projected schema and statistics.

size_t FullScanPlan::AccessRowCount() const {
  return std::min(stats_.Rows(), max_rows_);
}

size_t FullScanPlan::EmitRowCount() const {
  return std::min(stats_.Rows(), max_rows_);
}

slot_t FullScanPlan::ClusteredColumn() const {
  // A narrowed scan does not carry the claim: the claiming column may not be
  // part of the projection, and the proof is cheap to re-derive on a
  // full-width sibling if a later rewrite needs it.
  if (!EmitsChainOrderedScan() || projection_.has_value()) {
    return kNoClusteredColumn;
  }
  return ClusteredOrderColumn(table_, stats_);
}

bool FullScanPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                               const std::vector<bool>& ascending) const {
  if (expressions.empty() || expressions.size() != ascending.size()) {
    return false;
  }
  if (!EmitsChainOrderedScan() || projection_.has_value()) {
    return false;
  }
  // One clustered key is all the chain guarantees, and only forwards: a
  // descending request has no reverse chain walk, and a longer key list is
  // only a prefix match (the caller's IncrementalSort path handles the rest).
  if (expressions.size() != 1 || !ascending.front()) {
    return false;
  }
  const Expression& key = expressions.front();
  if (!key || key->Type() != TypeTag::kColumnValue) {
    return false;
  }
  // Offset resolves the qualified and unqualified spellings of the same column
  // (case-insensitively, the way the rest of the planner does), so the request
  // matches whichever rewrite produced it.
  const int offset =
      table_.GetSchema().Offset(key->AsColumnValue().GetColumnName());
  return offset >= 0 &&
         ColumnIsClustered(table_, stats_, static_cast<slot_t>(offset));
}

bool FullScanPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  // NULL placement needs no check: the catalog gate only accepts a NOT NULL
  // key column, so no row can carry a NULL and NULLS FIRST / NULLS LAST
  // describe the very same sequence.
  static_cast<void>(nulls_first);
  return IsOrderedBy(expressions, ascending);
}

void FullScanPlan::Dump(std::ostream& o, int /*indent*/) const {
  o << "FullScan: " << table_.GetSchema().Name()
    << "(estimated rows: " << AccessRowCount();
  if (projection_.has_value()) {
    o << ", columns=" << projection_->size() << "/"
      << table_.GetSchema().ColumnCount();
  }
  if (max_rows_ != std::numeric_limits<size_t>::max()) {
    o << ", max rows: " << max_rows_;
  }
  const slot_t clustered = ClusteredColumn();
  if (clustered != kNoClusteredColumn) {
    o << ", clustered order: "
      << table_.GetSchema().GetColumn(clustered).Name().name << " ASC";
  }
  o << ")";
}

std::string FullScanPlan::ToString() const {
  std::string result = "FullScan: " + std::string(table_.GetSchema().Name()) +
                       "(estimated rows: " + std::to_string(AccessRowCount());
  if (projection_.has_value()) {
    result += ", columns=" + std::to_string(projection_->size()) + "/" +
              std::to_string(table_.GetSchema().ColumnCount());
  }
  if (max_rows_ != std::numeric_limits<size_t>::max()) {
    result += ", max rows: " + std::to_string(max_rows_);
  }
  const slot_t clustered = ClusteredColumn();
  if (clustered != kNoClusteredColumn) {
    result += ", clustered order: " +
              std::string(table_.GetSchema().GetColumn(clustered).Name().name) +
              " ASC";
  }
  return result + ")";
}

}  // namespace tinylamb
