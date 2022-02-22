//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_ITERATOR_BASE_HPP
#define TINYLAMB_ITERATOR_BASE_HPP

namespace tinylamb {
class Row;

class IteratorBase {
 public:
  virtual ~IteratorBase() = default;
  [[nodiscard]] virtual bool IsValid() const = 0;
  virtual const Row& operator*() const = 0;
  virtual Row& operator*() = 0;
  virtual IteratorBase& operator++() = 0;
  virtual IteratorBase& operator--() = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_ITERATOR_BASE_HPP
