#ifndef TINYLAMB_PAGE_HPP
#define TINYLAMB_PAGE_HPP

#include <unistd.h>

#include <iostream>

#include "constants.hpp"
#include "log_message.hpp"
#include "page/free_page.hpp"
#include "page/meta_page.hpp"
#include "page/row_page.hpp"

namespace tinylamb {

class RowPosition;

class Page {
 public:
  Page(page_id_t page_id, PageType type);
  void PageInit(page_id_t page_id, PageType type);

  [[nodiscard]] page_id_t PageId() const { return page_id; }
  [[nodiscard]] PageType Type() const { return type; }
  [[nodiscard]] lsn_t PageLSN() const { return page_lsn; }
  [[nodiscard]] lsn_t RecoveryLSN() const { return recovery_lsn; }
  void SetPageLSN(lsn_t lsn) { page_lsn = lsn; }
  void SetRecLSN(lsn_t lsn) { recovery_lsn = std::min(lsn, recovery_lsn); }

  // Meta page manipulations.
  PageRef AllocateNewPage(Transaction& txn, PagePool& pool,
                          PageType new_page_type);
  void DestroyPage(Transaction& txn, Page* target, PagePool& pool);

  // Row page manipulations.
  bool Read(Transaction& txn, const RowPosition& pos, Row& dst) const;

  bool Insert(Transaction& txn, const Row& record, RowPosition& dst);

  bool Update(Transaction& txn, const RowPosition& pos, const Row& row);

  bool Delete(Transaction& txn, const RowPosition& pos);

  [[nodiscard]] size_t RowCount() const;

  // Internal methods exposed for recovery.
  void InsertImpl(const RowPosition& pos, std::string_view redo);

  void UpdateImpl(const RowPosition& pos, std::string_view redo);

  void DeleteImpl(const RowPosition& pos);

  void SetChecksum() const;

  [[nodiscard]] bool IsValid() const;
  void* operator new(size_t page_id);
  void operator delete(void* page) noexcept;

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
