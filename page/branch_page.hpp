//
// Created by kumagi on 2022/01/23.
//

#ifndef TINYLAMB_BRANCH_PAGE_HPP
#define TINYLAMB_BRANCH_PAGE_HPP

#include <cstdint>
#include <string_view>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "foster_pair.hpp"
#include "page/index_key.hpp"
#include "page/row_pointer.hpp"
#include "page_ref.hpp"

namespace tinylamb {

class Transaction;
class Page;
class PageManager;

class BranchPage final {
  char* Payload() { return reinterpret_cast<char*>(this); }
  [[nodiscard]] const char* Payload() const {
    return reinterpret_cast<const char*>(this);
  }

 public:
  void Initialize() {
    lowest_page_ = 0;
    row_count_ = 0;
    free_ptr_ = kPageBodySize;
    free_size_ = kPageBodySize - sizeof(BranchPage);
    rows_[kLowFenceIdx] = kMinusInfinity;
    rows_[kHighFenceIdx] = kPlusInfinity;
    rows_[kFosterIdx] = RowPointer();
  }
  [[nodiscard]] slot_t RowCount() const;

  void SetLowestValue(page_id_t pid, Transaction& txn, page_id_t page);
  page_id_t GetLowestValue(Transaction&) { return lowest_page_; }

  void SetLowestValueImpl(page_id_t value) { lowest_page_ = value; }

  Status Insert(page_id_t pid, Transaction& txn, std::string_view key,
                page_id_t value);

  Status Update(page_id_t pid, Transaction& txn, std::string_view key,
                page_id_t value);

  Status Delete(page_id_t pid, Transaction& txn, std::string_view key);

  StatusOr<page_id_t> GetPageForKey(Transaction& txn,
                                    std::string_view key) const;

  Status SetLowFence(page_id_t pid, Transaction& txn, const IndexKey& lf);
  Status SetHighFence(page_id_t pid, Transaction& txn, const IndexKey& hf);
  void SetLowFenceImpl(const IndexKey& lf);
  void SetHighFenceImpl(const IndexKey& hf);
  [[nodiscard]] IndexKey GetLowFence() const;
  [[nodiscard]] IndexKey GetHighFence() const;
  void SetFence(RowPointer& fence_pos, const IndexKey& new_fence);

  [[nodiscard]] Status SetFoster(page_id_t pid, Transaction& txn,
                                 const FosterPair& foster);
  void SetFosterImpl(const FosterPair& foster);
  [[nodiscard]] StatusOr<FosterPair> GetFoster() const;

  void Split(page_id_t, Transaction& txn, std::string_view key, Page* right,
             std::string* middle);

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

  Status MoveRightToFoster(Transaction& txn, Page& right);
  Status MoveLeftFromFoster(Transaction& txn, Page& right);

 private:
  void UpdateSlotImpl(RowPointer& pos, std::string_view payload);
  [[nodiscard]] std::string_view GetRow(size_t idx) const;
  constexpr static size_t kLowFenceIdx = 0;
  constexpr static size_t kHighFenceIdx = 1;
  constexpr static size_t kFosterIdx = 2;
  constexpr static size_t kExtraIdx = 3;
  friend class std::hash<BranchPage>;
  friend class BPlusTree;
  void DeFragment();

  page_id_t lowest_page_ = 0;
  slot_t row_count_ = 0;
  bin_size_t free_ptr_ = sizeof(BranchPage);
  bin_size_t free_size_ = kPageBodySize - sizeof(BranchPage);
  RowPointer rows_[];
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
