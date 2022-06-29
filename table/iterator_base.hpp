//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_ITERATOR_BASE_HPP
#define TINYLAMB_ITERATOR_BASE_HPP

#include "page/row_position.hpp"

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
