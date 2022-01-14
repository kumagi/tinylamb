//
// Created by kumagi on 2022/01/15.
//

#ifndef TINYLAMB_LEAF_PAGE_HPP
#define TINYLAMB_LEAF_PAGE_HPP

#include <cassert>
#include <string_view>

#include "constants.hpp"

namespace tinylamb {

class Transaction;

class LeafPage {
 public:
  bool Insert(page_id_t page_id, Transaction& txn, std::string_view key,
              std::string_view value);
  bool Update(page_id_t page_id, Transaction& txn, std::string_view key,
              std::string_view value);
  bool Delete(page_id_t page_id, Transaction& txn, std::string_view key);

  bool Read(page_id_t page_id, Transaction& txn, std::string_view key,
            std::string_view* result);

 private:
  friend class Page;
  friend class std::hash<LeafPage>;

  page_id_t prev_pid_ = 0;
  page_id_t next_pid_ = 0;
  int16_t row_count_ = 0;
  uint16_t free_ptr_ = kPageSize;
  uint16_t free_size_ = kPageSize - sizeof(LeafPage);
};

static_assert(std::is_trivially_destructible<LeafPage>::value == true,
              "RowPage must be trivially destructible");

}  // namespace tinylamb

namespace std {

template <>
class hash<tinylamb::LeafPage> {
 public:
  uint64_t operator()(const tinylamb::LeafPage& r) const;
};

}  // namespace std

#endif  // TINYLAMB_LEAF_PAGE_HPP
