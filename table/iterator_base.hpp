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

#include "page/row_position.hpp"
#include "type/value.hpp"

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
  bool operator==(const IteratorBase&) const = default;
  bool operator<=>(const IteratorBase&) const = default;
  virtual const Row& operator*() const = 0;
  virtual Row& operator*() = 0;
  const Row* operator->() const { return &operator*(); }
  Row* operator->() { return &operator*(); }
  virtual IteratorBase& operator++() = 0;
  virtual IteratorBase& operator--() = 0;
  virtual void Dump(std::ostream& o, int indent) const = 0;
  friend std::ostream& operator<<(std::ostream& o, const IteratorBase& it) {
    it.Dump(o, 0);
    return o;
  }
};

}  // namespace tinylamb

#endif  // TINYLAMB_ITERATOR_BASE_HPP
