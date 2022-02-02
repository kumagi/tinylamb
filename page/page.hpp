#ifndef TINYLAMB_PAGE_HPP
#define TINYLAMB_PAGE_HPP

#include <tiff.h>
#include <unistd.h>

#include <iostream>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "page/free_page.hpp"
#include "page/internal_page.hpp"
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
  void DestroyPage(Transaction& txn, Page* target, PagePool& pool);

  size_t RowCount(Transaction& txn) const;

  // Row page manipulations.
  Status Read(Transaction& txn, uint16 slot, std::string_view* result) const;
  Status Read(Transaction& txn, std::string_view key, page_id_t* result) const;

  Status Insert(Transaction& txn, std::string_view record, slot_t* slot);

  Status Update(Transaction& txn, slot_t slot, std::string_view row);

  Status Delete(Transaction& txn, slot_t pos);

  [[nodiscard]] size_t RowCount() const;

  Status ReadKey(Transaction& txn, const uint16& slot,
                 std::string_view* result) const;

  std::string_view GetKey(slot_t slot) const;
  page_id_t GetPage(slot_t slot) const;

  // Leaf page manipulations.
  Status Insert(Transaction& txn, std::string_view key, std::string_view value);
  Status Update(Transaction& txn, std::string_view key, std::string_view value);
  Status Delete(Transaction& txn, std::string_view key);
  Status Read(Transaction& txn, std::string_view key, std::string_view* result);
  Status LowestKey(Transaction& txn, std::string_view* result);
  Status HighestKey(Transaction& txn, std::string_view* result);
  void Split(Transaction& txn, Page* right);

  // Internal page manipulations.
  Status Insert(Transaction& txn, std::string_view key, page_id_t pid);
  Status Update(Transaction& txn, std::string_view key, page_id_t pid);
  Status GetPageForKey(Transaction& txn, std::string_view key,
                       page_id_t* page) const;
  void SetLowestValue(Transaction& txn, page_id_t i);
  void SplitInto(Transaction& txn, Page* right, std::string_view* middle);
  Status LowestPage(Transaction& txn, page_id_t* page);

  // Internal methods exposed for recovery.
  void InsertImpl(std::string_view redo);
  void UpdateImpl(slot_t slot, std::string_view redo);
  void DeleteImpl(slot_t slot);

  void InsertImpl(std::string_view key, std::string_view value);
  void UpdateImpl(std::string_view key, std::string_view value);
  void DeleteImpl(std::string_view key);

  void InsertInternalImpl(std::string_view key, page_id_t pid);
  void UpdateInternalImpl(std::string_view key, page_id_t pid);
  void DeleteInternalImpl(std::string_view key);
  void SetLowestValueInternalImpl(page_id_t lowest_value);

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
    char dummy_[kPageBodySize];
    MetaPage meta_page;
    FreePage free_page;
    RowPage row_page;
    LeafPage leaf_page;
    InternalPage internal_page;
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
