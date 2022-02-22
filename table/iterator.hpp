//
// Created by kumagi on 2022/02/23.
//

#ifndef TINYLAMB_ITERATOR_HPP
#define TINYLAMB_ITERATOR_HPP

#include <memory>

#include "table/iterator_base.hpp"

namespace tinylamb {
class Row;

class Iterator {
 public:
  explicit Iterator(IteratorBase* iter) : iter_(iter) {}
  [[nodiscard]] bool IsValid() const { return iter_->IsValid(); }
  const Row& operator*() const { return **iter_; }
  Row& operator*() { return **iter_; }
  Iterator& operator++() {
    ++(*iter_);
    return *this;
  }
  Iterator& operator--() {
    --(*iter_);
    return *this;
  }

 private:
  std::unique_ptr<IteratorBase> iter_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_ITERATOR_HPP
