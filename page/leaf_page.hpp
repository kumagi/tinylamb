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
  char* Payload() { return reinterpret_cast<char*>(this); }
  [[nodiscard]] const char* Payload() const {
    return reinterpret_cast<const char*>(this);
  }
  struct RowPointer {
    // Row start position from beginning fom this page.
    uint16_t offset = 0;

    // Physical row size in bytes (required to get exact size for logging).
    uint16_t size = 0;
  };
  RowPointer* Rows();
  std::string_view GetKey(const RowPointer& ptr);
  std::string_view GetValue(const RowPointer& ptr);

 public:
  void Initialize() {
    prev_pid_ = 0;
    next_pid_ = 0;
    row_count_ = 0;
    free_ptr_ = sizeof(LeafPage);
    free_size_ = kPageBodySize - sizeof(LeafPage);
  }

  bool Insert(page_id_t page_id, Transaction& txn, std::string_view key,
              std::string_view value);
  bool Update(page_id_t page_id, Transaction& txn, std::string_view key,
              std::string_view value);
  bool Delete(page_id_t page_id, Transaction& txn, std::string_view key);

  bool Read(page_id_t page_id, Transaction& txn, std::string_view key,
            std::string_view* result);

  void InsertImpl(std::string_view key, std::string_view value);
  void UpdateImpl(std::string_view key, std::string_view value);
  void DeleteImpl(std::string_view key);

  void DeFragment();

 private:
  friend class Page;
  friend class std::hash<LeafPage>;

  page_id_t prev_pid_ = 0;
  page_id_t next_pid_ = 0;
  int16_t row_count_ = 0;
  uint16_t free_ptr_ = kPageSize;
  uint16_t free_size_ = kPageSize - sizeof(LeafPage);
  char data_[0];
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
