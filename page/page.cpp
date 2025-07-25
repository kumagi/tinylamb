/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "page/page.hpp"

#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "index_key.hpp"
#include "page/free_page.hpp"
#include "page/meta_page.hpp"
#include "page/page_type.hpp"
#include "page/row_page.hpp"
#include "page_ref.hpp"
#include "transaction/transaction.hpp"

#define ASSERT_PAGE_TYPE(expected_type)            \
  if (type != (expected_type)) {                   \
    throw std::runtime_error("Invalid page type"); \
  }

namespace tinylamb {
Page::Page(page_id_t pid, PageType type) { PageInit(pid, type); }

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

void Page::DestroyPage(Transaction& txn, Page* target) {
  ASSERT_PAGE_TYPE(PageType::kMetaPage)
  body.meta_page.DestroyPage(txn, target);
  SetPageLSN(txn.PrevLSN());
  SetRecLSN(txn.PrevLSN());
}

size_t Page::RowCount(Transaction& /*txn*/) const {
  if (type == PageType::kRowPage) {
    return body.row_page.RowCount();
  }
  if (type == PageType::kLeafPage) {
    return body.leaf_page.RowCount();
  }
  if (type == PageType::kBranchPage) {
    return body.branch_page.RowCount();
  }
  throw std::runtime_error("invalid page type");
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
      throw std::runtime_error("Invalid page type for delete: " +
                               PageTypeString(type));
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

StatusOr<std::string_view> Page::LowestKey(Transaction& txn) const {
  ASSERT_PAGE_TYPE(PageType::kLeafPage)
  return body.leaf_page.LowestKey(txn);
}

StatusOr<std::string_view> Page::HighestKey(Transaction& txn) const {
  ASSERT_PAGE_TYPE(PageType::kLeafPage)
  return body.leaf_page.HighestKey(txn);
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

StatusOr<page_id_t> Page::GetPageForKey(Transaction& txn, std::string_view key,
                                        bool less_than) const {
  ASSERT_PAGE_TYPE(PageType::kBranchPage)
  return body.branch_page.GetPageForKey(txn, key, less_than);
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
  body.branch_page.Split(PageID(), txn, new_key, right, middle);
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

Status Page::SetLowFence(Transaction& txn, const IndexKey& key) {
  Status result = Status::kUnknown;
  switch (type) {
    case PageType::kLeafPage:
      result = body.leaf_page.SetLowFence(PageID(), txn, key);
      break;
    case PageType::kBranchPage:
      result = body.branch_page.SetLowFence(PageID(), txn, key);
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

Status Page::SetHighFence(Transaction& txn, const IndexKey& key) {
  Status result = Status::kUnknown;
  switch (type) {
    case PageType::kLeafPage:
      result = body.leaf_page.SetHighFence(PageID(), txn, key);
      break;
    case PageType::kBranchPage:
      result = body.branch_page.SetHighFence(PageID(), txn, key);
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

IndexKey Page::GetLowFence(Transaction& /*txn*/) const {
  switch (type) {
    case PageType::kLeafPage:
      return body.leaf_page.GetLowFence();
      break;
    case PageType::kBranchPage:
      return body.branch_page.GetLowFence();
      break;
    default:
      throw std::runtime_error("Invalid page type");
  }
}

IndexKey Page::GetHighFence(Transaction& /*txn*/) const {
  switch (type) {
    case PageType::kLeafPage:
      return body.leaf_page.GetHighFence();
      break;
    case PageType::kBranchPage:
      return body.branch_page.GetHighFence();
      break;
    default:
      throw std::runtime_error("Invalid page type");
  }
}

Status Page::SetFoster(Transaction& txn, const FosterPair& foster) {
  Status result = Status::kUnknown;
  switch (type) {
    case PageType::kLeafPage:
      result = body.leaf_page.SetFoster(PageID(), txn, foster);
      break;
    case PageType::kBranchPage:
      result = body.branch_page.SetFoster(PageID(), txn, foster);
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

StatusOr<FosterPair> Page::GetFoster(Transaction& /*txn*/) const {
  switch (type) {
    case PageType::kLeafPage:
      return body.leaf_page.GetFoster();
      break;
    case PageType::kBranchPage:
      return body.branch_page.GetFoster();
      break;
    default:
      throw std::runtime_error("Invalid page type for GetFoster: " +
                               PageTypeString(type));
  }
}

Status Page::MoveRightToFoster(Transaction& txn, Page& foster) {
  switch (type) {
    case PageType::kLeafPage:
      return body.leaf_page.MoveRightToFoster(txn, foster);
    case PageType::kBranchPage:
      return body.branch_page.MoveRightToFoster(txn, foster);
    default:
      throw std::runtime_error("Invalid page type");
  }
  return Status::kSuccess;
}

Status Page::MoveLeftFromFoster(Transaction& txn, Page& foster) {
  switch (type) {
    case PageType::kLeafPage:
      return body.leaf_page.MoveLeftFromFoster(txn, foster);
      break;
    case PageType::kBranchPage:
      return body.branch_page.MoveLeftFromFoster(txn, foster);
      break;
    default:
      throw std::runtime_error("Invalid page type");
  }
  return Status::kSuccess;
}

void Page::SetLowFenceImpl(const IndexKey& key) {
  switch (type) {
    case PageType::kLeafPage:
      body.leaf_page.SetLowFenceImpl(key);
      return;
    case PageType::kBranchPage:
      body.branch_page.SetLowFenceImpl(key);
      return;
    default:
      throw std::runtime_error("Invalid page type");
  }
}

void Page::SetHighFenceImpl(const IndexKey& key) {
  switch (type) {
    case PageType::kLeafPage:
      body.leaf_page.SetHighFenceImpl(key);
      return;
    case PageType::kBranchPage:
      body.branch_page.SetHighFenceImpl(key);
      return;
    default:
      throw std::runtime_error("Invalid page type");
  }
}

void Page::SetFosterImpl(const FosterPair& foster) {
  switch (type) {
    case PageType::kLeafPage:
      body.leaf_page.SetFosterImpl(foster);
      return;
    case PageType::kBranchPage:
      body.branch_page.SetFosterImpl(foster);
      return;
    default:
      throw std::runtime_error("Invalid page type");
  }
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

void* Page::operator new(size_t /*unused*/) {
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
  uint64_t header_hash = std::hash<uint64_t>()(p.page_id) +
                         std::hash<uint64_t>()(p.PageLSN()) +
                         std::hash<uint64_t>()(static_cast<uint64_t>(p.type));
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
