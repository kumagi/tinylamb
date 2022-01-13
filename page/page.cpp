#include "page/page.hpp"

#include <cstring>
#include <iostream>

#include "free_page.hpp"
#include "page/meta_page.hpp"
#include "page/page_type.hpp"
#include "page/row_page.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"

#define ASSERT_PAGE_TYPE(expected_type)            \
  if (type != (expected_type)) {                   \
    throw std::runtime_error("Invalid page type"); \
  }

namespace tinylamb {

Page::Page(size_t page_id, PageType type) : body() { PageInit(page_id, type); }

void Page::PageInit(uint64_t pid, PageType page_type) {
  memset(this, 0, kPageSize);
  page_id = pid;
  SetPageLSN(0);
  type = page_type;
  recovery_lsn = std::numeric_limits<uint64_t>::max();
  switch (type) {
    case PageType::kUnknown:
      break;
    case PageType::kFreePage:
      body.free_page.Initialize();
      break;
    case PageType::kMetaPage:
      body.meta_page.Initialize();
      break;
    case PageType::kRowPage:
      body.row_page.Initialize();
      break;
  }
}

// Meta page functions.
PageRef Page::AllocateNewPage(Transaction& txn, PagePool& pool,
                              PageType new_page_type) {
  ASSERT_PAGE_TYPE(PageType::kMetaPage)
  PageRef ret = body.meta_page.AllocateNewPage(txn, pool, new_page_type);
  SetPageLSN(txn.PrevLSN());
  SetRecLSN(txn.PrevLSN());
  return std::move(ret);
}

void Page::DestroyPage(Transaction& txn, Page* target, PagePool& pool) {
  ASSERT_PAGE_TYPE(PageType::kMetaPage)
  body.meta_page.DestroyPage(txn, target, pool);
  SetPageLSN(txn.PrevLSN());
  SetRecLSN(txn.PrevLSN());
}

bool Page::Read(Transaction& txn, const RowPosition& pos, Row& dst) const {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  std::string_view payload;
  bool result = body.row_page.Read(PageId(), txn, pos, &payload);
  if (result) {
    dst = Row(payload, pos);
  }
  return result;
}

bool Page::Insert(Transaction& txn, const Row& record, RowPosition& dst) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  bool result = body.row_page.Insert(PageId(), txn, record, dst);
  SetPageLSN(txn.PrevLSN());
  SetRecLSN(txn.PrevLSN());
  return result;
}

bool Page::Update(Transaction& txn, const RowPosition& pos, const Row& row) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  bool result = body.row_page.Update(PageId(), txn, pos, row);
  SetPageLSN(txn.PrevLSN());
  SetRecLSN(txn.PrevLSN());
  return result;
}

bool Page::Delete(Transaction& txn, const RowPosition& pos) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  bool result = body.row_page.Delete(PageId(), txn, pos);
  SetPageLSN(txn.PrevLSN());
  SetRecLSN(txn.PrevLSN());
  return result;
}

size_t Page::RowCount() const {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  return body.row_page.RowCount();
}

void Page::SetChecksum() const { checksum = std::hash<Page>()(*this); }

void Page::InsertImpl(const RowPosition& pos, std::string_view redo) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  body.row_page.InsertRow(redo);
}

void Page::UpdateImpl(const RowPosition& pos, std::string_view redo) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  body.row_page.UpdateRow(pos.slot, redo);
}

void Page::DeleteImpl(const RowPosition& pos) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  body.row_page.DeleteRow(pos.slot);
}

bool Page::IsValid() const { return checksum == std::hash<Page>()(*this); }

void* Page::operator new(size_t) {
  void* ret = new char[kPageSize];
  memset(ret, 0, kPageSize);
  return ret;
}

void Page::operator delete(void* page) noexcept {
  delete[] reinterpret_cast<char*>(page);
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Page>::operator()(const tinylamb::Page& p) const {
  uint64_t header_hash =
      std::hash<uint64_t>()(p.page_id) + std::hash<uint64_t>()(p.PageLSN()) +
      std::hash<uint64_t>()(static_cast<unsigned long>(p.type));
  switch (p.type) {
    case tinylamb::PageType::kFreePage:
      return header_hash + std::hash<tinylamb::FreePage>()(p.body.free_page);
    case tinylamb::PageType::kMetaPage:
      return header_hash + std::hash<tinylamb::MetaPage>()(p.body.meta_page);
    case tinylamb::PageType::kRowPage:
      return header_hash + std::hash<tinylamb::RowPage>()(p.body.row_page);
    default:
      return 0xdeadbeefcafebabe;  // Must be a broken page.
  }
}
