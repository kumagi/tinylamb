//
// Created by kumagi on 2022/02/09.
//

#ifndef TINYLAMB_B_PLUS_TREE_ITERATOR_HPP
#define TINYLAMB_B_PLUS_TREE_ITERATOR_HPP

#include <string>

#include "common/constants.hpp"

namespace tinylamb {

class BPlusTree;
class Transaction;

class BPlusTreeIterator {
 public:
  BPlusTreeIterator(BPlusTree* tree, Transaction* txn, std::string_view begin,
                    std::string_view end, bool ascending = true);
  std::string_view operator*();
  BPlusTreeIterator& operator++();
  BPlusTreeIterator& operator--();
  [[nodiscard]] bool IsValid() const { return valid_; }

 private:
  BPlusTree* tree_;
  Transaction* txn_;
  page_id_t pid_;
  size_t idx_;
  std::string begin_;
  std::string end_;
  bool valid_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_B_PLUS_TREE_ITERATOR_HPP
