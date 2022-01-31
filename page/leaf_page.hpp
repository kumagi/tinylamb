//
// Created by kumagi on 2022/01/15.
//

#ifndef TINYLAMB_LEAF_PAGE_HPP
#define TINYLAMB_LEAF_PAGE_HPP

#include <cassert>
#include <string_view>

#include "common/constants.hpp"

namespace tinylamb {

class Transaction;
class Page;
class InternalPage;

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
  [[nodiscard]] const RowPointer* Rows() const;

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
  bool Read(Transaction& txn, std::string_view key, std::string_view* result);

  [[nodiscard]] std::string_view GetKey(size_t idx) const;
  [[nodiscard]] std::string_view GetValue(size_t idx) const;
  bool LowestKey(Transaction& txn, std::string_view* result);
  bool HighestKey(Transaction& txn, std::string_view* result);
  [[nodiscard]] size_t RowCount() const;

  // Split utils.
  void Split(page_id_t pid, Transaction& txn, Page* target);

  void InsertImpl(std::string_view key, std::string_view value);
  void UpdateImpl(std::string_view key, std::string_view value);
  void DeleteImpl(std::string_view key);

 private:
  void DeFragment();
  void Dump(std::ostream& o, int indent) const;
  [[nodiscard]] size_t Find(std::string_view key) const;

 private:
  friend class Page;
  friend class InternalPage;
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
