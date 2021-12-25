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
  Page(size_t page_id, PageType type);
  void PageInit(uint64_t page_id, PageType type);

  [[nodiscard]] uint64_t PageId() const { return page_id; }
  [[nodiscard]] PageType Type() const { return type; }
  [[nodiscard]] uint64_t PageLSN() const { return page_lsn; }
  void SetPageLSN(uint64_t lsn) { page_lsn = lsn; }

  // Meta page.
  PageRef AllocateNewPage(Transaction& txn, PagePool& pool,
                          PageType new_page_type);
  void DestroyPage(Transaction& txn, Page* target, PagePool& pool);

  // Row page.
  bool Read(Transaction& txn, const RowPosition& pos, Row& dst);

  bool Insert(Transaction& txn, const Row& record, RowPosition& dst);

  bool Update(Transaction& txn, const RowPosition& pos, const Row& row);

  bool Delete(Transaction& txn, const RowPosition& pos);

  [[nodiscard]] size_t RowCount() const;

  void InsertImpl(const RowPosition& pos, std::string_view redo);

  void UpdateImpl(const RowPosition& pos, std::string_view redo);

  void DeleteImpl(const RowPosition& pos);

  void SetChecksum() const;

  [[nodiscard]] bool IsValid() const;
  void* operator new(size_t page_id);
  void operator delete(void* page) noexcept;

  uint64_t page_id = 0;
  uint64_t page_lsn = 0;
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
