//
// Created by kumagi on 2022/01/23.
//

#ifndef TINYLAMB_BRANCH_PAGE_HPP
#define TINYLAMB_BRANCH_PAGE_HPP

#include <cstdint>
#include <string_view>

#include "common/constants.hpp"

namespace tinylamb {

class Transaction;
class Page;
class PageManager;

class BranchPage {
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

 public:
  void Initialize() {
    lowest_page_ = 0;
    row_count_ = 0;
    free_ptr_ = kPageBodySize;
    free_size_ = kPageBodySize - sizeof(BranchPage);
  }
  [[nodiscard]] slot_t RowCount() const;

  void SetLowestValue(page_id_t pid, Transaction& txn, page_id_t value);

  void SetLowestValueImpl(page_id_t value) { lowest_page_ = value; }

  Status AttachLeft(page_id_t pid, Transaction& txn, std::string_view key,
                    page_id_t value);

  Status Insert(page_id_t pid, Transaction& txn, std::string_view key,
                page_id_t value);

  Status Update(page_id_t pid, Transaction& txn, std::string_view key,
                page_id_t value);

  Status Delete(page_id_t pid, Transaction& txn, std::string_view key);

  Status GetPageForKey(Transaction& txn, std::string_view key,
                       page_id_t* result) const;

  Status FindForKey(Transaction& txn, std::string_view key,
                    page_id_t* result) const;

  void SplitInto(page_id_t pid, Transaction& txn, std::string_view key,
                 Page* right, std::string* middle);

  void Merge(page_id_t pid, Transaction& txn, Page* child) const;

  // Return lowest page_id which may contain the specified |key|.
  [[nodiscard]] bin_size_t SearchToInsert(std::string_view key) const;
  // Return lowest page_id which may contain the specified |key|.
  [[nodiscard]] int Search(std::string_view key) const;

  [[nodiscard]] std::string_view GetKey(size_t idx) const;
  [[nodiscard]] page_id_t GetValue(size_t idx) const;

  void InsertImpl(std::string_view key, page_id_t redo);
  void UpdateImpl(std::string_view key, page_id_t redo);
  void DeleteImpl(std::string_view key);

  void Dump(std::ostream& o, int indent) const;
  bool SanityCheckForTest(PageManager* pm) const;

 private:
  friend class std::hash<BranchPage>;
  friend class BPlusTree;
  void DeFragment();

  slot_t row_count_ = 0;
  page_id_t lowest_page_ = 0;
  bin_size_t free_ptr_ = sizeof(BranchPage);
  bin_size_t free_size_ = kPageBodySize - sizeof(BranchPage);
  RowPointer rows_[0];
};

}  // namespace tinylamb

namespace std {

template <>
class hash<tinylamb::BranchPage> {
 public:
  uint64_t operator()(const tinylamb::BranchPage& r) const;
};

}  // namespace std

#endif  // TINYLAMB_BRANCH_PAGE_HPP
