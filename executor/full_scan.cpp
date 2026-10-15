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

//  Created by kumagi on 2022/02/21.
//

#include "executor/full_scan.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <ostream>
#include <utility>

#include "executor/data_chunk.hpp"
#include "table/table.hpp"

namespace tinylamb {
FullScan::FullScan(Transaction& txn, const Table& table, size_t max_rows,
                   bool lock_rows, bool wait_for_write_intent,
                   std::optional<ColumnName> clustered_order,
                   std::optional<std::vector<slot_t>> projection)
    : table_(&table),
      iter_([&]() {
        TableScanOptions options;
        options.lock_rows = lock_rows;
        options.wait_for_write_intent = wait_for_write_intent;
        options.projection = projection;
        return table_->BeginFullScan(txn, options);
      }()),
      max_rows_(max_rows),
      output_schema_([&]() {
        if (!projection.has_value()) {
          return table.GetSchema();
        }
        std::vector<Column> columns;
        columns.reserve(projection->size());
        for (const slot_t slot : *projection) {
          columns.push_back(table.GetSchema().GetColumn(slot));
        }
        return Schema(table.GetSchema().Name(), std::move(columns));
      }()),
      clustered_order_(std::move(clustered_order)) {}

bool FullScan::AttachRuntimeFilter(
    const std::shared_ptr<const RuntimeFilterGroup>& group, slot_t column,
    ValueType type) {
  if (started_ || group == nullptr || group->empty()) {
    return false;
  }
  const Schema& schema = table_->GetSchema();
  if (column >= schema.ColumnCount()) {
    return false;
  }
  // The filter is built over the build side's int64 key domain. A DATE column
  // holds days-since-epoch in the same field, so the two share the bit domain
  // but not the comparison domain: refuse a type mismatch rather than prune on
  // incomparable values. A group that rejects unconditionally has no domain to
  // match, so the build side's type (unknown when it had no comparable key at
  // all) does not constrain the binding.
  const ValueType declared = schema.GetColumn(column).Type();
  if (declared != ValueType::kInt64 && declared != ValueType::kDate) {
    return false;
  }
  if (type != ValueType::kNull && declared != type) {
    return false;
  }
  runtime_filter_ = group;
  runtime_filter_column_ = column;
  iter_.SetRuntimeFilter(runtime_filter_.get(), runtime_filter_column_, type);
  return true;
}

bool FullScan::Next(Row* dst, RowPosition* rp) {
  started_ = true;
  if (emitted_ >= max_rows_) {
    return false;
  }
  if (!iter_.IsValid()) {
    // Iterator contract (table/iterator_base.hpp): a scan that stops early
    // latches the failure in GetStatus(). Treating it as plain exhaustion
    // would serve a truncated result set as a success.
    const Status status = iter_.GetStatus();
    if (status != Status::kSuccess) {
      return FailWith(status);
    }
    return false;
  }
  *dst = *iter_;
  if (rp != nullptr) {
    *rp = iter_.Position();
  }
  ++iter_;
  ++emitted_;
  return true;
}

size_t FullScan::NextBatch(DataChunk* destination, size_t max_rows) {
  started_ = true;
  const size_t remaining = max_rows_ - std::min(emitted_, max_rows_);
  const size_t batch_limit = std::min(max_rows, remaining);
  destination->Reset(output_schema_, batch_limit);
  while (iter_.IsValid() && emitted_ < max_rows_ &&
         destination->Size() < batch_limit) {
    const RowPosition position = iter_.Position();
    destination->Append(std::move(*iter_), position);
    ++iter_;
    ++emitted_;
  }
  if (!iter_.IsValid() && destination->Size() < batch_limit) {
    // See FullScan::Next: a mid-scan iterator failure is not EOF.
    const Status status = iter_.GetStatus();
    if (status != Status::kSuccess) {
      FailWith(status);
      return 0;
    }
  }
  return destination->Size();
}

void FullScan::Dump(std::ostream& o, int /*indent*/) const {
  o << "FullScan: " << table_->GetSchema().Name();
  if (max_rows_ != std::numeric_limits<size_t>::max()) {
    o << " (max rows: " << max_rows_ << ")";
  }
  // The clustered order this walk delivers. EXPLAIN renders the executor graph
  // for most scan shapes, so the plan's promise is reported here too.
  if (clustered_order_.has_value()) {
    o << " (clustered order: " << clustered_order_->name << " ASC)";
  }
  if (runtime_filter_ != nullptr) {
    o << " (runtime filter: " << runtime_filter_->Describe() << " on column "
      << runtime_filter_column_ << ")";
  }
}
}  // namespace tinylamb
