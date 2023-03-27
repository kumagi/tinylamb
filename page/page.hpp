#ifndef TINYLAMB_PAGE_HPP
#define TINYLAMB_PAGE_HPP

#include <unistd.h>

#include <array>
#include <iostream>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "page/branch_page.hpp"
#include "page/free_page.hpp"
#include "page/leaf_page.hpp"
#include "page/meta_page.hpp"
#include "page/row_page.hpp"

namespace tinylamb {

class Page {
 public:
  Page(page_id_t page_id, PageType type);
  void PageInit(page_id_t page_id, PageType type);

  [[nodiscard]] page_id_t PageID() const { return page_id; }
  [[nodiscard]] PageType Type() const { return type; }
  [[nodiscard]] lsn_t PageLSN() const { return page_lsn; }
  [[nodiscard]] lsn_t RecoveryLSN() const { return recovery_lsn; }
  void SetPageLSN(lsn_t lsn) { page_lsn = lsn; }
  void SetRecLSN(lsn_t lsn) { recovery_lsn = std::min(lsn, recovery_lsn); }

  // Meta page manipulations.
  PageRef AllocateNewPage(Transaction& txn, PagePool& pool,
                          PageType new_page_type);
  void DestroyPage(Transaction& txn, Page* target);

  size_t RowCount(Transaction& txn) const;

  // Row page manipulations.
  StatusOr<std::string_view> Read(Transaction& txn, slot_t slot) const;

  StatusOr<slot_t> Insert(Transaction& txn, std::string_view record);

  Status Update(Transaction& txn, slot_t slot, std::string_view row);

  Status Delete(Transaction& txn, slot_t pos);

  [[nodiscard]] slot_t RowCount() const;

  StatusOr<std::string_view> ReadKey(Transaction& txn, slot_t slot) const;

  std::string_view GetKey(slot_t slot) const;
  page_id_t GetPage(slot_t slot) const;

  // Leaf & Branch common manipulation.
  Status SetLowFence(Transaction& txn, const IndexKey& key);
  Status SetHighFence(Transaction& txn, const IndexKey& key);
  [[nodiscard]] IndexKey GetLowFence(Transaction& txn) const;
  [[nodiscard]] IndexKey GetHighFence(Transaction& txn) const;
  [[nodiscard]] Status SetFoster(Transaction& txn, const FosterPair& foster);
  [[nodiscard]] StatusOr<FosterPair> GetFoster(Transaction& txn);
  Status MoveRightToFoster(Transaction& txn, Page& foster);
  Status MoveLeftFromFoster(Transaction& txn, Page& foster);

  void SetLowFenceImpl(const IndexKey& key);
  void SetHighFenceImpl(const IndexKey& key);
  void SetFosterImpl(const FosterPair& foster);

  // Leaf page manipulations.
  Status InsertLeaf(Transaction& txn, std::string_view key,
                    std::string_view value);
  Status Update(Transaction& txn, std::string_view key, std::string_view value);
  Status Delete(Transaction& txn, std::string_view key);
  StatusOr<std::string_view> Read(Transaction& txn, std::string_view key) const;
  StatusOr<std::string_view> LowestKey(Transaction& txn);
  StatusOr<std::string_view> HighestKey(Transaction& txn);

  // Branch page manipulations.
  Status InsertBranch(Transaction& txn, std::string_view key, page_id_t pid);
  Status UpdateBranch(Transaction& txn, std::string_view key, page_id_t pid);
  StatusOr<page_id_t> GetPageForKey(Transaction& txn,
                                    std::string_view key) const;

  void SetLowestValue(Transaction& txn, page_id_t i);
  void SplitInto(Transaction& txn, std::string_view new_key, Page* right,
                 std::string* middle);
  void PageTypeChange(Transaction& txn, PageType new_type);

  // Internal methods exposed for recovery.
  void InsertImpl(std::string_view redo);
  void UpdateImpl(slot_t slot, std::string_view redo);
  void DeleteImpl(slot_t slot);

  void InsertImpl(std::string_view key, std::string_view value);
  void UpdateImpl(std::string_view key, std::string_view value);
  void DeleteImpl(std::string_view key);

  void InsertBranchImpl(std::string_view key, page_id_t pid);
  void UpdateBranchImpl(std::string_view key, page_id_t pid);
  void DeleteBranchImpl(std::string_view key);
  void SetLowestValueBranchImpl(page_id_t lowest_value);
  void PageTypeChangeImpl(PageType new_type);

  void SetChecksum() const;

  [[nodiscard]] bool IsValid() const;
  void* operator new(size_t page_id);
  void operator delete(void* page) noexcept;

  void Dump(std::ostream& o, int indent) const;

  friend std::ostream& operator<<(std::ostream& o, const Page& p) {
    p.Dump(o, 0);
    return o;
  }

  // The ID for this page. This ID is also an offset of this page in file.
  page_id_t page_id = 0;

  // An LSN of the latest log which modified this page.
  lsn_t page_lsn = 0;

  // An LSN of manipulation log which first make this page dirty.
  uint64_t recovery_lsn = 0;
  enum PageType type = PageType::kUnknown;
  mutable uint64_t checksum = 0;
  union PageBody {
    std::array<char, kPageBodySize> dummy_;
    MetaPage meta_page;
    FreePage free_page;
    RowPage row_page;
    LeafPage leaf_page;
    BranchPage branch_page;
    PageBody() : dummy_() {}
  };
  PageBody body;
};

static_assert(std::is_trivially_destructible<Page>::value == true,
              "Page must be trivially destructible");
static_assert(sizeof(Page) == kPageSize,
              "Page size must be equal to kPageSize");

}  // namespace tinylamb

template <>
class std::hash<tinylamb::Page> {
 public:
  uint64_t operator()(const tinylamb::Page& p) const;
};

#endif  // TINYLAMB_PAGE_HPP
