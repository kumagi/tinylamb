//
// Created by kumagi on 2022/01/23.
//

#ifndef TINYLAMB_INTERNAL_PAGE_HPP
#define TINYLAMB_INTERNAL_PAGE_HPP

#include <cstdint>
#include <string_view>

#include "common/constants.hpp"

namespace tinylamb {

class Transaction;
class Page;

class InternalPage {
  char* Payload() { return reinterpret_cast<char*>(this); }
  [[nodiscard]] const char* Payload() const {
    return reinterpret_cast<const char*>(this);
  }
  struct RowPointer {
    // Row start position from beginning fom this page.
    bin_size_t offset = 0;

    // Physical row size in bytes (required to get exact size for logging).
    bin_size_t size = 0;
  };
  RowPointer* Rows();
  [[nodiscard]] const RowPointer* Rows() const;

 public:
  void Initialize() {
    lowest_page_ = 0;
    row_count_ = 0;
    free_ptr_ = sizeof(InternalPage);
    free_size_ = kPageBodySize - sizeof(InternalPage);
  }
  bin_size_t RowCount() const;

  void SetLowestValue(page_id_t pid, Transaction& txn, page_id_t value);

  void SetLowestValueImpl(page_id_t value) { lowest_page_ = value; }

  Status Insert(page_id_t pid, Transaction& txn, std::string_view key,
                page_id_t value);

  Status Update(page_id_t pid, Transaction& txn, std::string_view key,
                page_id_t value);

  Status Delete(page_id_t pid, Transaction& txn, std::string_view key);

  Status GetPageForKey(Transaction& txn, std::string_view key,
                       page_id_t* result) const;

  void SplitInto(page_id_t pid, Transaction& txn, std::string_view new_key,
                 Page* right, std::string* middle);

  // Return lowest page_id which may contain the specified |key|.
  [[nodiscard]] bin_size_t SearchToInsert(std::string_view key) const;
  // Return lowest page_id which may contain the specified |key|.
  [[nodiscard]] bin_size_t Search(std::string_view key) const;

  [[nodiscard]] std::string_view GetKey(size_t idx) const;
  [[nodiscard]] page_id_t GetValue(size_t idx) const;

  void InsertImpl(std::string_view key, page_id_t redo);
  void UpdateImpl(std::string_view key, page_id_t redo);
  void DeleteImpl(std::string_view key);

  void Dump(std::ostream& o, int indent) const;

 private:
  friend class std::hash<InternalPage>;
  friend class BPlusTree;

  bin_size_t row_count_ = 0;
  page_id_t lowest_page_ = 0;
  bin_size_t free_ptr_ = sizeof(InternalPage);
  bin_size_t free_size_ = kPageBodySize - sizeof(InternalPage);
  void Defragment();
};

}  // namespace tinylamb

namespace std {

template <>
class hash<tinylamb::InternalPage> {
 public:
  uint64_t operator()(const tinylamb::InternalPage& r) const;
};

}  // namespace std

#endif  // TINYLAMB_INTERNAL_PAGE_HPP
