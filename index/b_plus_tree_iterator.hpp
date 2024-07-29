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

#ifndef TINYLAMB_B_PLUS_TREE_ITERATOR_HPP
#define TINYLAMB_B_PLUS_TREE_ITERATOR_HPP

#include <cstddef>
#include <string>
#include <string_view>

#include "common/constants.hpp"

namespace tinylamb {

class BPlusTree;
class Transaction;

class BPlusTreeIterator {
 public:
  BPlusTreeIterator(BPlusTree* tree, Transaction* txn,
                    std::string_view begin = "", std::string_view end = "",
                    bool ascending = true);
  [[nodiscard]] std::string Key() const;
  [[nodiscard]] std::string Value() const;
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
