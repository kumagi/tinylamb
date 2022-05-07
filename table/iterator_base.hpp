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
  virtual ~IteratorBase() = default;
  [[nodiscard]] virtual bool IsValid() const = 0;
  [[nodiscard]] virtual RowPosition Position() const = 0;
  virtual const Row& operator*() const = 0;
  virtual Row& operator*() = 0;
  const Row* operator->() const { return &operator*(); }
  Row* operator->() { return &operator*(); }
  virtual IteratorBase& operator++() = 0;
  virtual IteratorBase& operator--() = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_ITERATOR_BASE_HPP
