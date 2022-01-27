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

class InternalPage {
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
    lowest_page_ = 0;
    row_count_ = 0;
    free_ptr_ = sizeof(InternalPage);
    free_size_ = kPageBodySize - sizeof(InternalPage);
  }

  void SetLowestValue(page_id_t pid, page_id_t value);

  void SetTree(page_id_t pid, Transaction& txn, std::string_view key,
               page_id_t left, page_id_t right);

  bool Insert(page_id_t pid, Transaction& txn, std::string_view key,
              page_id_t value);

  bool Update(page_id_t pid, Transaction& txn, std::string_view key,
              page_id_t value);

  bool Delete(page_id_t pid, Transaction& txn, page_id_t target);

  bool GetPageForKey(Transaction& txn, std::string_view key, page_id_t* result);

  // Return lowest page_id which may contain the specified |key|.
  [[nodiscard]] uint16_t SearchToInsert(std::string_view key) const;
  // Return lowest page_id which may contain the specified |key|.
  [[nodiscard]] uint16_t Search(std::string_view key) const;

  [[nodiscard]] std::string_view GetKey(size_t idx) const;
  [[nodiscard]] const page_id_t& GetValue(size_t idx) const;
  [[nodiscard]] page_id_t& GetValue(size_t idx);

  void InsertImpl(std::string_view key, page_id_t pid);
  void DeleteImpl(page_id_t pid);

  void Dump(std::ostream& o, int indent) const;

 private:
  uint16_t row_count_ = 0;
  page_id_t lowest_page_ = 0;
  uint16_t free_ptr_ = sizeof(InternalPage);
  uint16_t free_size_ = kPageBodySize - sizeof(InternalPage);
};

}  // namespace tinylamb

#endif  // TINYLAMB_INTERNAL_PAGE_HPP
