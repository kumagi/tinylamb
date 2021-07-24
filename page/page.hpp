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
  kFixedLengthRow,
  kVariableRow,
};

std::ostream& operator<<(std::ostream& o, const PageType& type);

class Page {
 public:
  Page(size_t page_id, PageType type);
  void PageInit(uint64_t page_id, PageType type);

  [[nodiscard]] uint64_t PageId() const { return page_id; }
  [[nodiscard]] PageType Type() const { return type; }

  void InsertImpl(std::string_view redo);

  void UpdateImpl(const RowPosition& pos, std::string_view redo);

  void DeleteImpl(const RowPosition& pos);

  void SetChecksum() const;

  [[nodiscard]] bool IsValid() const;
  void* operator new(size_t page_id);
  void operator delete(void* page) noexcept;

  uint64_t page_id = 0;
  uint64_t last_lsn = 0;
  enum PageType type = PageType::kUnknown;
  mutable uint64_t checksum = 0;
  char page_body[0];
};

const static size_t kPageBodySize = kPageSize - sizeof(Page);

}  // namespace tinylamb

template<>
class std::hash<tinylamb::Page> {
 public:
  uint64_t operator()(const tinylamb::Page& p) const;
};

#endif  // TINYLAMB_PAGE_HPP