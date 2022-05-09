#include "page/page.hpp"

#include <cstring>
#include <iostream>

#include "page/free_page.hpp"
#include "page/meta_page.hpp"
#include "page/page_type.hpp"
#include "page/row_page.hpp"
#include "transaction/transaction.hpp"

#define ASSERT_PAGE_TYPE(expected_type)            \
  if (type != (expected_type)) {                   \
    throw std::runtime_error("Invalid page type"); \
  }

namespace tinylamb {

Page::Page(page_id_t pid, PageType type) : body() { PageInit(pid, type); }

void Page::PageInit(page_id_t pid, PageType page_type) {
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
    case PageType::kLeafPage:
      body.leaf_page.Initialize();
      break;
    case PageType::kBranchPage:
      body.branch_page.Initialize();
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
  return ret;
}

void Page::DestroyPage(Transaction& txn, Page* target, PagePool& pool) {
  ASSERT_PAGE_TYPE(PageType::kMetaPage)
  body.meta_page.DestroyPage(txn, target, pool);
  SetPageLSN(txn.PrevLSN());
  SetRecLSN(txn.PrevLSN());
}

size_t Page::RowCount(Transaction& txn) const {
  if (type == PageType::kRowPage) {
    return body.row_page.RowCount();
  }
  if (type == PageType::kLeafPage) {
    return body.leaf_page.RowCount();
  } else if (type == PageType::kBranchPage) {
    return body.branch_page.RowCount();
  } else {
    throw std::runtime_error("invalid page type");
  }
}

StatusOr<std::string_view> Page::Read(Transaction& txn, slot_t slot) const {
  if (type == PageType::kRowPage) {
    return body.row_page.Read(PageID(), txn, slot);
  }
  if (type == PageType::kLeafPage) {
    return body.leaf_page.Read(PageID(), txn, slot);
  }
  throw std::runtime_error("invalid page type");
}

std::string_view Page::GetKey(slot_t slot) const {
  if (type == PageType::kLeafPage) {
    return body.leaf_page.GetKey(slot);
  }
  return body.branch_page.GetKey(slot);
}

page_id_t Page::GetPage(slot_t slot) const {
  ASSERT_PAGE_TYPE(PageType::kBranchPage)
  return body.branch_page.GetValue(slot);
}

StatusOr<slot_t> Page::Insert(Transaction& txn, std::string_view record) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  StatusOr<slot_t> result = body.row_page.Insert(PageID(), txn, record);
  if (result.GetStatus() == Status::kSuccess) {
    SetPageLSN(txn.PrevLSN());
    SetRecLSN(txn.PrevLSN());
  }
  return result;
}

Status Page::Update(Transaction& txn, slot_t slot, std::string_view row) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  Status result = body.row_page.Update(PageID(), txn, slot, row);
  if (result == Status::kSuccess) {
    SetPageLSN(txn.PrevLSN());
    SetRecLSN(txn.PrevLSN());
  }
  return result;
}

Status Page::Delete(Transaction& txn, const slot_t pos) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  Status result = body.row_page.Delete(PageID(), txn, pos);
  if (result == Status::kSuccess) {
    SetPageLSN(txn.PrevLSN());
    SetRecLSN(txn.PrevLSN());
  }
  return result;
}

slot_t Page::RowCount() const {
  switch (type) {
    case PageType::kRowPage:
      return body.row_page.RowCount();
    case PageType::kLeafPage:
      return body.leaf_page.RowCount();
    case PageType::kBranchPage:
      return body.branch_page.RowCount();
    default:
      throw std::runtime_error("RowCount is not implemented");
  }
}

StatusOr<std::string_view> Page::ReadKey(Transaction& txn, slot_t slot) const {
  switch (type) {
    case PageType::kRowPage:
      return Status::kUnknown;
    case PageType::kLeafPage:
      return body.leaf_page.ReadKey(PageID(), txn, slot);
    case PageType::kBranchPage:
      return body.branch_page.GetKey(slot);
    default:
      throw std::runtime_error("ReadKey is not implemented");
  }
}

// Leaf page manipulations.
Status Page::InsertLeaf(Transaction& txn, std::string_view key,
                        std::string_view value) {
  ASSERT_PAGE_TYPE(PageType::kLeafPage)
  Status result = body.leaf_page.Insert(PageID(), txn, key, value);
  if (result == Status::kSuccess) {
    SetPageLSN(txn.PrevLSN());
    SetRecLSN(txn.PrevLSN());
  }
  return result;
}

Status Page::Update(Transaction& txn, std::string_view key,
                    std::string_view value) {
  ASSERT_PAGE_TYPE(PageType::kLeafPage)
  Status result = body.leaf_page.Update(PageID(), txn, key, value);
  if (result == Status::kSuccess) {
    SetPageLSN(txn.PrevLSN());
    SetRecLSN(txn.PrevLSN());
  }
  return result;
}

Status Page::Delete(Transaction& txn, std::string_view key) {
  Status result;
  switch (type) {
    case PageType::kLeafPage:
      result = body.leaf_page.Delete(PageID(), txn, key);
      break;
    case PageType::kBranchPage:
      result = body.branch_page.Delete(PageID(), txn, key);
      break;
    default:
      throw std::runtime_error("Invalid page type");
  }
  if (result == Status::kSuccess) {
    SetPageLSN(txn.PrevLSN());
    SetRecLSN(txn.PrevLSN());
  }
  return result;
}

StatusOr<std::string_view> Page::Read(Transaction& txn,
                                      std::string_view key) const {
  ASSERT_PAGE_TYPE(PageType::kLeafPage)
  return body.leaf_page.Read(PageID(), txn, key);
}

StatusOr<std::string_view> Page::LowestKey(Transaction& txn) {
  ASSERT_PAGE_TYPE(PageType::kLeafPage)
  return body.leaf_page.LowestKey(txn);
}

StatusOr<std::string_view> Page::HighestKey(Transaction& txn) {
  ASSERT_PAGE_TYPE(PageType::kLeafPage)
  return body.leaf_page.HighestKey(txn);
}

Status Page::SetPrevNext(Transaction& txn, page_id_t prev, page_id_t next) {
  ASSERT_PAGE_TYPE(PageType::kLeafPage)
  Status s = body.leaf_page.SetPrevNext(PageID(), txn, prev, next);
  if (s == Status::kSuccess) {
    SetPageLSN(txn.PrevLSN());
    SetRecLSN(txn.PrevLSN());
  }
  return s;
}

Status Page::InsertBranch(Transaction& txn, std::string_view key,
                          page_id_t pid) {
  ASSERT_PAGE_TYPE(PageType::kBranchPage)
  Status result = body.branch_page.Insert(PageID(), txn, key, pid);
  if (result == Status::kSuccess) {
    SetPageLSN(txn.PrevLSN());
    SetRecLSN(txn.PrevLSN());
  }
  return result;
}

Status Page::UpdateBranch(Transaction& txn, std::string_view key,
                          page_id_t pid) {
  ASSERT_PAGE_TYPE(PageType::kBranchPage)
  Status result = body.branch_page.Update(PageID(), txn, key, pid);
  if (result == Status::kSuccess) {
    SetPageLSN(txn.PrevLSN());
    SetRecLSN(txn.PrevLSN());
  }
  return result;
}

Status Page::GetPageForKey(Transaction& txn, std::string_view key,
                           page_id_t* page) const {
  ASSERT_PAGE_TYPE(PageType::kBranchPage)
  return body.branch_page.GetPageForKey(txn, key, page);
}

Status Page::FindForKey(Transaction& txn, std::string_view key,
                        page_id_t* page) const {
  ASSERT_PAGE_TYPE(PageType::kBranchPage)
  return body.branch_page.FindForKey(txn, key, page);
}

void Page::SetLowestValue(Transaction& txn, page_id_t v) {
  ASSERT_PAGE_TYPE(PageType::kBranchPage)
  body.branch_page.SetLowestValue(PageID(), txn, v);
  SetPageLSN(txn.PrevLSN());
  SetRecLSN(txn.PrevLSN());
}

void Page::SplitInto(Transaction& txn, std::string_view new_key, Page* right,
                     std::string* middle) {
  ASSERT_PAGE_TYPE(PageType::kBranchPage)
  body.branch_page.SplitInto(PageID(), txn, new_key, right, middle);
}

void Page::Merge(Transaction& txn, Page* page) {
  ASSERT_PAGE_TYPE(PageType::kBranchPage)
  body.branch_page.Merge(PageID(), txn, page);
}

void Page::PageTypeChange(Transaction& txn, PageType new_type) {
  PageTypeChangeImpl(new_type);
  txn.AllocatePageLog(page_id, new_type);
  SetPageLSN(txn.PrevLSN());
  SetRecLSN(txn.PrevLSN());
}

void Page::SetChecksum() const { checksum = std::hash<Page>()(*this); }

void Page::InsertImpl(std::string_view redo) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  body.row_page.InsertRow(redo);
}

void Page::UpdateImpl(slot_t slot, std::string_view redo) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  body.row_page.UpdateRow(slot, redo);
}

void Page::DeleteImpl(slot_t slot) {
  ASSERT_PAGE_TYPE(PageType::kRowPage)
  body.row_page.DeleteRow(slot);
}

void Page::InsertImpl(std::string_view key, std::string_view value) {
  ASSERT_PAGE_TYPE(PageType::kLeafPage)
  body.leaf_page.InsertImpl(key, value);
}

void Page::UpdateImpl(std::string_view key, std::string_view value) {
  ASSERT_PAGE_TYPE(PageType::kLeafPage)
  body.leaf_page.UpdateImpl(key, value);
}

void Page::DeleteImpl(std::string_view key) {
  ASSERT_PAGE_TYPE(PageType::kLeafPage)
  body.leaf_page.DeleteImpl(key);
}

void Page::SetPrevNextImpl(page_id_t prev, page_id_t next) {
  ASSERT_PAGE_TYPE(PageType::kLeafPage)
  body.leaf_page.SetPrevNextImpl(prev, next);
}

void Page::InsertBranchImpl(std::string_view key, page_id_t pid) {
  ASSERT_PAGE_TYPE(PageType::kBranchPage)
  body.branch_page.InsertImpl(key, pid);
}
void Page::UpdateBranchImpl(std::string_view key, page_id_t pid) {
  ASSERT_PAGE_TYPE(PageType::kBranchPage)
  body.branch_page.UpdateImpl(key, pid);
}
void Page::DeleteBranchImpl(std::string_view key) {
  ASSERT_PAGE_TYPE(PageType::kBranchPage)
  body.branch_page.DeleteImpl(key);
}
void Page::SetLowestValueBranchImpl(page_id_t lowest_value) {
  ASSERT_PAGE_TYPE(PageType::kBranchPage)
  body.branch_page.SetLowestValueImpl(lowest_value);
}
void Page::PageTypeChangeImpl(PageType new_type) {
  PageInit(page_id, new_type);
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

void Page::Dump(std::ostream& o, int indent) const {
  o << "PID: " << page_id << " PageLSN: " << page_lsn
    << " RecLSN: " << recovery_lsn << " Type:";
  switch (type) {
    case PageType::kFreePage:
      o << " FreePage ";
      body.free_page.Dump(o, indent);
      break;
    case PageType::kMetaPage:
      o << " MetaPage ";
      body.meta_page.Dump(o, indent);
      break;
    case PageType::kRowPage:
      o << " RowPage ";
      body.row_page.Dump(o, indent);
      break;
    case PageType::kLeafPage:
      o << " LeafPage ";
      body.leaf_page.Dump(o, indent);
      break;
    case PageType::kBranchPage:
      o << " BranchPage ";
      body.branch_page.Dump(o, indent);
      break;
    default:
      break;
  }
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
    case tinylamb::PageType::kLeafPage:
      return header_hash + std::hash<tinylamb::LeafPage>()(p.body.leaf_page);
    case tinylamb::PageType::kBranchPage:
      return header_hash +
             std::hash<tinylamb::BranchPage>()(p.body.branch_page);
    default:
      return 0xdeadbeefcafebabe;  // Must be a broken page.
  }
}
