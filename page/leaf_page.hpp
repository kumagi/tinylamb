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
class BranchPage;

class LeafPage {
  char* Payload() { return reinterpret_cast<char*>(rows_); }
  [[nodiscard]] const char* Payload() const {
    return reinterpret_cast<const char*>(rows_);
  }
  struct RowPointer {
    // Row start position from beginning fom this page.
    bin_size_t offset = 0;

    // Physical row size in bytes (required to get exact size for logging).
    bin_size_t size = 0;
  };

 public:
  void Initialize() {
    prev_pid_ = 0;
    next_pid_ = 0;
    row_count_ = 0;
    free_ptr_ = kPageBodySize - OFFSET_OF(LeafPage, rows_);
    free_size_ = kPageBodySize - OFFSET_OF(LeafPage, rows_);
  }

  Status Insert(page_id_t page_id, Transaction& txn, std::string_view key,
                std::string_view value);
  Status Update(page_id_t page_id, Transaction& txn, std::string_view key,
                std::string_view value);
  Status Delete(page_id_t page_id, Transaction& txn, std::string_view key);
  Status Read(page_id_t pid, Transaction& txn, slot_t slot,
              std::string_view* result) const;
  Status ReadKey(page_id_t pid, Transaction& txn, slot_t slot,
                 std::string_view* result) const;
  Status Read(page_id_t pid, Transaction& txn, std::string_view key,
              std::string_view* result) const;

  [[nodiscard]] std::string_view GetKey(size_t idx) const;
  [[nodiscard]] std::string_view GetValue(size_t idx) const;
  Status LowestKey(Transaction& txn, std::string_view* result) const;
  Status HighestKey(Transaction& txn, std::string_view* result) const;
  [[nodiscard]] slot_t RowCount() const;
  Status SetPrevNext(page_id_t pid, Transaction& txn, page_id_t prev,
                     page_id_t next);

  // Split utils.
  void Split(page_id_t pid, Transaction& txn, std::string_view key,
             std::string_view value, Page* right);

  void InsertImpl(std::string_view key, std::string_view value);
  void UpdateImpl(std::string_view key, std::string_view value);
  void DeleteImpl(std::string_view key);
  void SetPrevNextImpl(page_id_t prev, page_id_t next);
  [[nodiscard]] bool SanityCheckForTest() const;

 private:
  void DeFragment();
  void Dump(std::ostream& o, int indent) const;
  [[nodiscard]] size_t Find(std::string_view key) const;

 private:
  friend class BPlusTree;
  friend class BPlusTreeIterator;
  friend class Page;
  friend class BranchPage;
  friend class std::hash<LeafPage>;

  page_id_t prev_pid_ = 0;
  page_id_t next_pid_ = 0;
  slot_t row_count_ = 0;
  bin_size_t free_ptr_ = kPageBodySize - OFFSET_OF(LeafPage, rows_);
  bin_size_t free_size_ = kPageBodySize - OFFSET_OF(LeafPage, rows_);
  RowPointer rows_[0];
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
