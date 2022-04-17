//
// Created by kumagi on 2022/01/23.
//

#include "branch_page.hpp"

#include "common/serdes.hpp"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

namespace {

std::string OmittedString(std::string_view original, size_t length) {
  if (length < original.length()) {
    std::string omitted_key = std::string(original).substr(0, 8);
    omitted_key +=
        "..(" + std::to_string(original.length() - length + 4) + "bytes)..";
    omitted_key += original.substr(original.length() - 8);
    return omitted_key;
  } else {
    return std::string(original);
  }
}

}  // anonymous namespace

slot_t BranchPage::RowCount() const { return row_count_; }

void BranchPage::SetLowestValue(page_id_t pid, Transaction& txn,
                                page_id_t value) {
  SetLowestValueImpl(value);
  txn.SetLowestLog(pid, value);
}

Status BranchPage::AttachLeft(page_id_t pid, Transaction& txn,
                              std::string_view key, page_id_t value) {
  assert(0 < row_count_);
  const bin_size_t physical_size = SerializeSize(key) + sizeof(value);
  if (free_size_ < physical_size + sizeof(RowPointer)) return Status::kNoSpace;
  if (0 != row_count_ && GetKey(0) == key) return Status::kDuplicates;
  assert(key < GetKey(0));
  InsertImpl(key, lowest_page_);
  txn.InsertBranchLog(pid, key, lowest_page_);
  SetLowestValue(pid, txn, value);
  txn.SetLowestLog(pid, value);
  return Status::kSuccess;
}

Status BranchPage::Insert(page_id_t pid, Transaction& txn, std::string_view key,
                          page_id_t value) {
  const bin_size_t physical_size = SerializeSize(key) + sizeof(value);
  if (free_size_ < physical_size + sizeof(RowPointer)) return Status::kNoSpace;
  size_t pos = SearchToInsert(key);
  if (pos != row_count_ && GetKey(pos) == key) return Status::kDuplicates;
  InsertImpl(key, value);
  txn.InsertBranchLog(pid, key, value);
  return Status::kSuccess;
}

void BranchPage::InsertImpl(std::string_view key, page_id_t pid) {
  const bin_size_t physical_size = SerializeSize(key) + sizeof(page_id_t);
  free_size_ -= physical_size + sizeof(RowPointer);
  if ((Payload() + free_ptr_ - physical_size) <=
      reinterpret_cast<char*>(&rows_[row_count_ + 2])) {
    DeFragment();
  }
  const int pos = SearchToInsert(key);
  ++row_count_;
  assert(physical_size <= free_ptr_);
  free_ptr_ -= physical_size;
  SerializePID(Payload() + free_ptr_, pid);
  SerializeStringView(Payload() + free_ptr_ + sizeof(page_id_t), key);
  memmove(rows_ + pos + 1, rows_ + pos,
          (row_count_ - pos) * sizeof(RowPointer));
  rows_[pos].offset = free_ptr_;
  rows_[pos].size = physical_size;
}

Status BranchPage::Update(page_id_t pid, Transaction& txn, std::string_view key,
                          page_id_t value) {
  const size_t pos = Search(key);
  if (row_count_ < pos || GetKey(pos) != key) return Status::kNotExists;
  txn.UpdateBranchLog(pid, key, GetValue(pos), value);
  UpdateImpl(key, value);
  return Status::kSuccess;
}

void BranchPage::UpdateImpl(std::string_view key, page_id_t pid) {
  memcpy(Payload() + rows_[Search(key)].offset, &pid, sizeof(pid));
}

Status BranchPage::Delete(page_id_t pid, Transaction& txn,
                          std::string_view key) {
  const int pos = Search(key);
  bin_size_t old_value = GetValue(pos);
  DeleteImpl(key);
  txn.DeleteBranchLog(pid, key, old_value);
  return Status::kSuccess;
}

void BranchPage::DeleteImpl(std::string_view key) {
  assert(0 < row_count_);
  int pos = Search(key);
  assert(pos < row_count_);
  if (pos < 0) {
    lowest_page_ = GetValue(0);
    ++pos;
  }
  free_size_ += SerializeSize(GetKey(pos)) + sizeof(page_id_t);
  memmove(rows_ + pos, rows_ + pos + 1,
          sizeof(RowPointer) * (row_count_ - pos));
  --row_count_;
}

Status BranchPage::GetPageForKey(Transaction& txn, std::string_view key,
                                 page_id_t* result) const {
  assert(0 < row_count_);
  if (key < GetKey(0)) {
    *result = lowest_page_;
  } else {
    *result = GetValue(Search(key));
  }
  return Status::kSuccess;
}

Status BranchPage::FindForKey(Transaction& txn, std::string_view key,
                              page_id_t* result) const {
  assert(0 < row_count_);
  size_t pos = Search(key);
  if (RowCount() < pos) return Status::kNotExists;
  if (key == GetKey(pos)) {
    *result = GetValue(pos);
    return Status::kSuccess;
  }
  return Status::kNotExists;
}

void BranchPage::SplitInto(page_id_t pid, Transaction& txn,
                           std::string_view key, Page* right,
                           std::string* middle) {
  const size_t kPayload = kPageBodySize - OFFSET_OF(BranchPage, rows_);
  const size_t kThreshold = kPayload / 2;
  const size_t expected_size =
      SerializeSize(key) + sizeof(page_id_t) + sizeof(RowPointer);
  assert(right->type == PageType::kBranchPage);
  size_t consumed_size = 0;
  size_t pivot = 0;
  while (consumed_size < kThreshold && pivot < row_count_ - 2) {
    consumed_size +=
        SerializeSize(GetKey(pivot++)) + sizeof(page_id_t) + sizeof(RowPointer);
  }
  while (GetKey(pivot) < key && consumed_size < expected_size) {
    pivot++;
    consumed_size +=
        SerializeSize(GetKey(pivot)) + sizeof(page_id_t) + sizeof(RowPointer);
  }
  while (key < GetKey(pivot) && kPayload < consumed_size + expected_size) {
    consumed_size -=
        SerializeSize(GetKey(pivot)) + sizeof(page_id_t) + sizeof(RowPointer);
    pivot--;
  }

  const size_t original_row_count = row_count_;
  *middle = GetKey(pivot);
  right->SetLowestValue(txn, GetValue(pivot));
  for (size_t i = pivot + 1; i < row_count_; ++i) {
    right->InsertBranch(txn, GetKey(i), GetValue(i));
  }
  Page* this_page = GET_PAGE_PTR(this);
  for (size_t i = pivot; i < original_row_count; ++i) {
    this_page->Delete(txn, GetKey(pivot));
  }
  if (right->RowCount() == 0 || right->GetKey(0) <= key) {
    assert(expected_size <= right->body.branch_page.free_size_);
  } else {
    assert(expected_size <= free_size_);
  }
}

void BranchPage::Merge(page_id_t pid, Transaction& txn, Page* child) {
  assert(child->Type() == PageType::kBranchPage);
  assert(child->body.branch_page.row_count_ == 0);
  assert(0 < row_count_);
  const page_id_t new_child = child->body.branch_page.lowest_page_;
  LOG(ERROR) << pid << " and " << new_child << " merge";
}

bin_size_t BranchPage::SearchToInsert(std::string_view key) const {
  int left = -1, right = row_count_;
  while (1 < right - left) {
    const int cur = (left + right) / 2;
    std::string_view cur_key = GetKey(cur);
    if (key < cur_key)
      right = cur;
    else
      left = cur;
  }
  return right;
}

int BranchPage::Search(std::string_view key) const {
  int left = -1, right = row_count_;
  while (1 < right - left) {
    const int cur = (left + right) / 2;
    std::string_view cur_key = GetKey(cur);
    if (key < cur_key)
      right = cur;
    else
      left = cur;
  }
  return left;
}

std::string_view BranchPage::GetKey(size_t idx) const {
  std::string_view key;
  DeserializeStringView(Payload() + rows_[idx].offset + sizeof(page_id_t),
                        &key);
  return key;
}

page_id_t BranchPage::GetValue(size_t idx) const {
  page_id_t result;
  memcpy(&result, Payload() + rows_[idx].offset, sizeof(result));
  return result;
}

void BranchPage::Dump(std::ostream& o, int indent) const {
  o << "Rows: " << row_count_ << " FreeSize: " << free_size_
    << " FreePtr:" << free_ptr_ << " Lowest: " << lowest_page_;
  if (row_count_ == 0) return;
  o << "\n" << Indent(indent + 2) << lowest_page_;
  for (size_t i = 0; i < row_count_; ++i) {
    o << "\n"
      << Indent(indent) << OmittedString(GetKey(i), 20) << "\n"
      << Indent(indent + 2) << GetValue(i);
  }
}

std::string SmallestKey(PageRef&& page) {
  if (page->Type() == PageType::kLeafPage)
    return std::string(page->body.leaf_page.GetKey(0));
  else if (page->Type() == PageType::kBranchPage)
    return std::string(page->body.branch_page.GetKey(0));
  LOG(ERROR) << " for " << page->PageID() << " -> " << page->Type();
  assert(!"invalid page type");
}

std::string BiggestKey(PageRef&& page) {
  if (page->Type() == PageType::kLeafPage) {
    slot_t count = page->body.leaf_page.RowCount();
    return std::string(page->body.leaf_page.GetKey(count - 1));
  } else if (page->Type() == PageType::kBranchPage) {
    slot_t count = page->body.branch_page.RowCount();
    return std::string(page->body.branch_page.GetKey(count - 1));
  }
  assert(!"invalid page type");
}

bool SanityCheck(PageRef&& page, PageManager* pm) {
  if (page->Type() == PageType::kLeafPage)
    return page->body.leaf_page.SanityCheckForTest();
  else if (page->Type() == PageType::kBranchPage)
    return page->body.branch_page.SanityCheckForTest(pm);
  assert(!"invalid page type");
}

bool BranchPage::SanityCheckForTest(PageManager* pm) const {
  bool sanity = SanityCheck(pm->GetPage(lowest_page_), pm);
  if (!sanity) return false;
  if (row_count_ == 0) {
    LOG(FATAL) << "Branch page is empty";
    return false;
  }
  const Page* this_page = GET_CONST_PAGE_PTR(this);
  for (size_t i = 0; i < row_count_ - 1; ++i) {
    if (GetKey(i + 1) < GetKey(i)) return false;
    if (GetValue(i) == 0) {
      LOG(FATAL) << "zero page at " << i;
      Dump(std::cerr, 0);
    }
    std::string smallest = SmallestKey(pm->GetPage(GetValue(i)));
    if (smallest < GetKey(i)) {
      LOG(FATAL) << "Child smallest key is smaller than parent slot: "
                 << smallest << " vs " << GetKey(i);
      return false;
    }
    std::string biggest = BiggestKey(pm->GetPage(GetValue(i)));
    if (GetKey(i + 1) < biggest) {
      LOG(WARN) << *this_page;
      LOG(FATAL) << "Child biggest key is bigger than parent next slot: "
                 << GetKey(i + 1) << " vs " << biggest;
      return false;
    }
    sanity = SanityCheck(pm->GetPage(GetValue(i)), pm);
    if (!sanity) return false;
  }
  return SanityCheck(pm->GetPage(GetValue(row_count_ - 1)), pm);
}

void BranchPage::DeFragment() {
  std::vector<std::string> payloads;
  payloads.reserve(row_count_);
  for (size_t i = 0; i < row_count_; ++i) {
    payloads.emplace_back(Payload() + rows_[i].offset, rows_[i].size);
  }
  bin_size_t offset = kPageBodySize;
  for (size_t i = 0; i < row_count_; ++i) {
    offset -= payloads[i].size();
    rows_[i].offset = offset;
    memcpy(Payload() + offset, payloads[i].data(), payloads[i].size());
  }
  free_ptr_ = offset;
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::BranchPage>::operator()(
    const ::tinylamb::BranchPage& r) const {
  uint64_t ret = 0;
  ret += std::hash<tinylamb::slot_t>()(r.row_count_);
  ret += std::hash<tinylamb::bin_size_t>()(r.free_ptr_);
  ret += std::hash<tinylamb::bin_size_t>()(r.free_size_);
  for (int i = 0; i < r.row_count_; ++i) {
    ret += std::hash<std::string_view>()(r.GetKey(i));
    ret += std::hash<tinylamb::page_id_t>()(r.GetValue(i));
  }
  return ret;
}
