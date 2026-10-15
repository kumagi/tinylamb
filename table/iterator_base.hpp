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

#ifndef TINYLAMB_ITERATOR_BASE_HPP
#define TINYLAMB_ITERATOR_BASE_HPP

#include "common/constants.hpp"
#include "common/runtime_filter.hpp"
#include "page/row_position.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
struct Row;

class IteratorBase {
 public:
  IteratorBase() = default;
  virtual ~IteratorBase() = default;
  IteratorBase(const IteratorBase&) = delete;
  IteratorBase(IteratorBase&&) = delete;
  IteratorBase& operator=(const IteratorBase&) = delete;
  IteratorBase& operator=(IteratorBase&&) = delete;
  [[nodiscard]] virtual bool IsValid() const = 0;
  [[nodiscard]] virtual RowPosition Position() const = 0;
  // No member-wise operator== / operator<=> here: a defaulted version would
  // report every pair of iterators equal.  Concrete iterators that need
  // comparison define their own.
  virtual const Row& operator*() const = 0;
  virtual Row& operator*() = 0;
  const Row* operator->() const { return &operator*(); }
  Row* operator->() { return &operator*(); }
  virtual IteratorBase& operator++() = 0;
  virtual IteratorBase& operator--() = 0;
  // Releases any held page latch without advancing.  Used by morsel scan
  // workers before they may block on a full output queue, so a blocked
  // worker never stands between a writer and its page.
  virtual void DropPageLatch() {}
  // Sticky page-access failure: iterators that cannot surface errors through
  // their return values mark themselves invalid and keep the first failure
  // here. Scans that must distinguish "exhausted" from "corrupt" check this
  // after the loop.
  [[nodiscard]] virtual Status GetStatus() const { return Status::kSuccess; }
  // Installs a dynamic join filter (common/runtime_filter.hpp) on the column
  // this iterator scans, so rows whose key has no counterpart on the join's
  // build side are skipped before row decode. Default is a no-op: only
  // FullScanIterator supports it. Contract: called once, before the first row
  // is produced; the group must outlive the iterator.
  virtual void SetRuntimeFilter(const RuntimeFilterGroup* group, slot_t column,
                                ValueType type) {
    (void)group;
    (void)column;
    (void)type;
  }
  // Typed batch decode (see TypedCellBatch): decodes up to max_rows visible
  // rows into schema-typed cell buffers and returns the row count (0 at
  // exhaustion or when the concrete iterator cannot produce typed batches —
  // callers then keep the Row loop). The iterator advances past the decoded
  // rows; check GetStatus() when 0 arrives before IsValid() turned false.
  virtual size_t DecodeNextTypedBatch(struct TypedCellBatch* batch,
                                      size_t max_rows) {
    (void)batch;
    (void)max_rows;
    return 0;
  }
  virtual void Dump(std::ostream& o, int indent) const = 0;
  friend std::ostream& operator<<(std::ostream& o, const IteratorBase& it) {
    it.Dump(o, 0);
    return o;
  }
};

}  // namespace tinylamb

#endif  // TINYLAMB_ITERATOR_BASE_HPP
