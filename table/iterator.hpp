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

#ifndef TINYLAMB_ITERATOR_HPP
#define TINYLAMB_ITERATOR_HPP

#include <memory>

#include "table/iterator_base.hpp"

namespace tinylamb {
struct Row;
struct TypedCellBatch;

class Iterator {
 public:
  explicit Iterator(IteratorBase* iter) : iter_(iter) {}
  [[nodiscard]] bool IsValid() const { return iter_->IsValid(); }
  // Sticky failure of the underlying scan (IteratorBase contract): callers
  // must distinguish "exhausted" from "IO/corruption failure" via this after
  // IsValid() turns false.
  [[nodiscard]] Status GetStatus() const { return iter_->GetStatus(); }
  [[nodiscard]] RowPosition Position() const { return iter_->Position(); }
  Row* operator->() { return &**iter_; }
  const Row& operator*() const { return **iter_; }
  // Non-const access lets hot scan loops move the row out (the next ++
  // re-deserializes into a fresh row regardless).
  Row& operator*() { return **iter_; }
  Iterator& operator++() {
    ++(*iter_);
    return *this;
  }
  // Forwards to the underlying iterator; a no-op for iterators that cannot
  // prune (see IteratorBase::SetRuntimeFilter).
  void SetRuntimeFilter(const RuntimeFilterGroup* group, slot_t column,
                        ValueType type) {
    iter_->SetRuntimeFilter(group, column, type);
  }
  // Typed batch decode; 0 means exhausted or unsupported (check GetStatus()).
  size_t DecodeNextTypedBatch(TypedCellBatch* batch, size_t max_rows) {
    return iter_->DecodeNextTypedBatch(batch, max_rows);
  }
  Iterator& operator--() {
    --(*iter_);
    return *this;
  }
  // See IteratorBase::DropPageLatch.
  void DropPageLatch() { iter_->DropPageLatch(); }
  friend std::ostream& operator<<(std::ostream& o, const Iterator& it) {
    it.iter_->Dump(o, 0);
    return o;
  }

 private:
  std::unique_ptr<IteratorBase> iter_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_ITERATOR_HPP
