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

#ifndef TINYLAMB_FULL_SCAN_HPP
#define TINYLAMB_FULL_SCAN_HPP

#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <vector>

#include "common/constants.hpp"
#include "executor/executor_base.hpp"
#include "table/fragment/fragment.hpp"
#include "table/iterator.hpp"
#include "type/column_name.hpp"

namespace tinylamb {
class Table;
class Transaction;
struct TypedCellBatch;
struct Row;
struct RowPosition;

class FullScan : public ExecutorBase {
 public:
  // Non-copyable by design: instances are owned by smart pointers and
  // referenced by raw pointers throughout the executor/graph object web.
  FullScan(const FullScan&) = delete;
  FullScan& operator=(const FullScan&) = delete;
  FullScan(FullScan&&) = delete;
  FullScan& operator=(FullScan&&) = delete;
  // lock_rows marks a DML source scan: the iterator write-intent-locks every
  // occupied slot before resolving it, so yielded rows carry the head
  // (newest committed) image rather than the transaction's snapshot image.
  // clustered_order names the column whose stored order this scan delivers
  // (FullScanPlan::ClusteredColumn); it is reported in EXPLAIN and is empty
  // whenever the scan makes no such promise.
  FullScan(Transaction& txn, const Table& table,
           size_t max_rows = std::numeric_limits<size_t>::max(),
           bool lock_rows = false, bool wait_for_write_intent = true,
           std::optional<ColumnName> clustered_order = std::nullopt,
           std::optional<std::vector<slot_t>> projection = std::nullopt);
  ~FullScan() override = default;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;
  StatusOr<bool> TryFoldTypedBatches(
      const std::function<Status(size_t, const TypedCellBatch&)>& consume);
  [[nodiscard]] StatusOr<ExecResult> ExecuteLSTableFragment(
      const Fragment& fragment, Runtime runtime = Runtime::kNative) const;
  [[nodiscard]] const Schema& OutputSchema() const { return output_schema_; }
  [[nodiscard]] bool WasTypedFolded() const { return typed_folded_; }
  // Accepts a dynamic join filter published by the hash join this scan probes
  // for. FullScan yields whole table rows, so `column` is a table-schema slot
  // and the rejection happens inside the iterator before row decode.
  bool AttachRuntimeFilter(
      const std::shared_ptr<const RuntimeFilterGroup>& group, slot_t column,
      ValueType type) override;

  [[nodiscard]] const std::optional<ColumnName>& ClusteredOrder() const {
    return clustered_order_;
  }

 private:
  Transaction* txn_;
  // Not owned; the executor must not outlive the Table.
  const Table* table_;
  Iterator iter_;
  size_t max_rows_;
  size_t emitted_{0};
  // Set by the first Next/NextBatch: past that point the iterator has already
  // consumed rows, and installing a filter could only prune them retroactively.
  bool started_{false};
  bool typed_folded_{false};
  // Owns the group so the iterator's raw pointer stays valid for its lifetime.
  std::shared_ptr<const RuntimeFilterGroup> runtime_filter_;
  slot_t runtime_filter_column_{0};
  // Owned copy of the schema the projected iterator yields (the table schema
  // when no projection was requested); NextBatch resets chunks to this width.
  Schema output_schema_{Schema("", {})};
  // Diagnostic only: the plan decided the chain walk is in this key's order.
  std::optional<ColumnName> clustered_order_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_FULL_SCAN_HPP
