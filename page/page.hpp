#ifndef TINYLAMB_PAGE_HPP
#define TINYLAMB_PAGE_HPP

#include <unistd.h>

#include <iostream>

#include "constants.hpp"
#include "log_message.hpp"

namespace tinylamb {

class RowPosition;

enum class PageType : uint64_t {
  kUnknown = 0,
  kFreePage,
  kMetaPage,
  kCatalogPage,
  kRowPage,
};

std::ostream& operator<<(std::ostream& o, const PageType& type);

class Page {
 public:
  Page(size_t page_id, PageType type);
  void PageInit(uint64_t page_id, PageType type);

  [[nodiscard]] uint64_t PageId() const { return page_id; }
  [[nodiscard]] PageType Type() const { return type; }
  [[nodiscard]] uint64_t PageLSN() const { return page_lsn; }
  void SetPageLSN(uint64_t lsn) { page_lsn = lsn; }
  char* PageHead() { return reinterpret_cast<char*>(this); }
  const char* PageHead() const { return reinterpret_cast<const char*>(this); }

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
  char page_body[0];
};

const static size_t kPageBodySize = kPageSize - sizeof(Page);

static_assert(std::is_trivially_destructible<Page>::value == true,
              "Page must be trivially destructible");

}  // namespace tinylamb

template <>
class std::hash<tinylamb::Page> {
 public:
  uint64_t operator()(const tinylamb::Page& p) const;
};

#endif  // TINYLAMB_PAGE_HPP
