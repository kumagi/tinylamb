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

#ifndef TINYLAMB_EXECUTOR_BASE_HPP
#define TINYLAMB_EXECUTOR_BASE_HPP

#include <iosfwd>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

#include "common/constants.hpp"
#include "common/runtime_filter.hpp"
#include "executor/data_chunk.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
struct Row;
struct RowPosition;

class ExecutorBase {
 public:
  ExecutorBase() = default;
  ExecutorBase(const ExecutorBase&) = delete;
  ExecutorBase(ExecutorBase&&) = delete;
  ExecutorBase& operator=(const ExecutorBase&) = delete;
  ExecutorBase& operator=(ExecutorBase&&) = delete;
  virtual ~ExecutorBase() = default;
  virtual size_t NextBatch(DataChunk* destination,
                           size_t max_rows = kDefaultVectorSize) = 0;
  bool Next(Row* dst, RowPosition* rp = nullptr);
  // Batch consumption that first drains rows buffered by Next(); callers
  // that may mix row-at-a-time and batch fetching on the same executor
  // must go through this so buffered rows are never skipped. The sticky
  // status remains available via GetStatus(), as on NextBatch().
  size_t NextBatchDraining(DataChunk* destination,
                           size_t max_rows = kDefaultVectorSize);
  virtual void Dump(std::ostream& o, int indent) const = 0;
  virtual void Explain(std::ostream& o, int indent) const { Dump(o, indent); }
  friend std::ostream& operator<<(std::ostream& o, const ExecutorBase& e) {
    e.Dump(o, 0);
    return o;
  }

  // Dynamic ("runtime") join filter pushdown. A hash join materializes its
  // build side first, publishes a
  // sound filter over the build keys here, and only then pulls rows from the
  // probe side: `column` on this executor holds a join key whose value has no
  // counterpart on the build side, so the row cannot produce a match.
  //
  // Returns false when the executor cannot prune (the default, and for any
  // executor whose output is not the probed table); the join then applies the
  // filter itself.  Caller obligation: attach at most once, before the
  // executor has produced its first row. This base implementation is a no-op
  // and does not police the contract; concrete executors that can prune
  // enforce the "before first row" half themselves (e.g. FullScan refuses
  // once started_), while a duplicate attachment before that point may
  // replace the filter. Callers must therefore attach exactly once; a late
  // attachment is ignored rather than corrupting a running probe.
  virtual bool AttachRuntimeFilter(
      const std::shared_ptr<const RuntimeFilterGroup>& group, slot_t column,
      ValueType type) {
    (void)group;
    (void)column;
    (void)type;
    return false;
  }

  // Returns exact visible row count in O(1) without scanning when this executor
  // is an unfiltered clean full-table scan with an intact snapshot store.
  [[nodiscard]] virtual std::optional<uint64_t> TryExactUnfilteredRowCount()
      const {
    return std::nullopt;
  }

  // Sticky execution error. Next()/NextBatch() return false/0 both at EOF
  // and on failure; callers must inspect GetStatus() after exhaustion to
  // tell the two apart. The first recorded error wins (sticky error).
  // Virtual: transparent decorators (RetainedExecutor, IndexSkipScanExecutor)
  // forward to the wrapped executor so a child's error is not reported as a
  // successful exhaustion by the outermost cursor.
  [[nodiscard]] virtual Status GetStatus() const {
    std::scoped_lock lock(status_mu_);
    return status_;
  }
  [[nodiscard]] bool ok() const { return GetStatus() == Status::kSuccess; }

 protected:
  // Records the first error; returns false so callers can write
  // `return FailWith(...)` inside Next().
  bool FailWith(Status status) const {
    // Workers of a shared executor may record failures concurrently. Guard
    // both the check and the write so "first error wins" is atomic and the
    // Status message's shared_ptr refcount is not raced.
    std::scoped_lock lock(status_mu_);
    if (status_ == Status::kSuccess) {
      status_ = std::move(status);
    }
    return false;
  }
  // Copies a child's sticky error when the child stopped on a failure;
  // returns true if a failure was forwarded.  In a bool Next() do not
  // return this value directly (true would claim a row exists) — latch and
  // `return false` instead; in Status-returning materializers use
  // `if (FailWithChildOf(...)) { return GetStatus(); }`.
  bool FailWithChildOf(const ExecutorBase& child) const {
    if (child.GetStatus() != Status::kSuccess) {
      FailWith(child.GetStatus());
      return true;
    }
    return false;
  }

 private:
  mutable std::mutex status_mu_;  // guards status_ (and its message refcount)
  mutable Status status_{Status::kSuccess};
  // T4494: row-at-a-time fetch buffer backing Next().
  DataChunk row_buffer_;
  size_t row_buffer_pos_{0};
};

using Executor = std::shared_ptr<ExecutorBase>;

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_BASE_HPP
