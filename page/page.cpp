#include "page/page.hpp"

#include <cstring>
#include <iostream>

#include "page/free_page.hpp"
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

bool Page::Read(Transaction& txn, const uint16& slot,
                std::string_view* result) const {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  return body.row_page.Read(PageID(), txn, slot, result);
}

bool Page::Insert(Transaction& txn, std::string_view record, uint16_t* slot) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  bool result = body.row_page.Insert(PageID(), txn, record, slot);
  SetPageLSN(txn.PrevLSN());
  SetRecLSN(txn.PrevLSN());
  return result;
}

bool Page::Update(Transaction& txn, uint16_t slot, std::string_view row) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  bool result = body.row_page.Update(PageID(), txn, slot, row);
  SetPageLSN(txn.PrevLSN());
  SetRecLSN(txn.PrevLSN());
  return result;
}

bool Page::Delete(Transaction& txn, const uint16_t pos) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  bool result = body.row_page.Delete(PageID(), txn, pos);
  SetPageLSN(txn.PrevLSN());
  SetRecLSN(txn.PrevLSN());
  return result;
}

size_t Page::RowCount() const {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  return body.row_page.RowCount();
}

// Leaf page manipulations.
bool Page::Insert(Transaction& txn, std::string_view key,
                  std::string_view value) {
  return true;
}

bool Page::Update(Transaction& txn, std::string_view key,
                  std::string_view value) {
  return true;
}
bool Page::Delete(Transaction& txn, std::string_view key) { return true; }
bool Page::Read(Transaction& txn, std::string_view key,
                std::string_view* result) {
  return true;
}

void Page::SetChecksum() const { checksum = std::hash<Page>()(*this); }

void Page::InsertImpl(std::string_view redo) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  body.row_page.InsertRow(redo);
}

void Page::UpdateImpl(uint16_t slot, std::string_view redo) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  body.row_page.UpdateRow(slot, redo);
}

void Page::DeleteImpl(uint16_t slot) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  body.row_page.DeleteRow(slot);
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
