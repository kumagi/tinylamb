//
// Created by kumagi on 2022/01/23.
//

#include "branch_page.hpp"

#include <cstddef>

#include "common/serdes.hpp"
#include "page/foster_pair.hpp"
#include "page/index_key.hpp"
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
  }
  return std::string(original);
}

}  // anonymous namespace

slot_t BranchPage::RowCount() const { return row_count_; }

void BranchPage::SetLowestValue(page_id_t pid, Transaction& txn,
                                page_id_t value) {
  page_id_t old_lowest_value = lowest_page_;
  SetLowestValueImpl(value);
  txn.SetLowestLog(pid, value, old_lowest_value);
}

Status BranchPage::Insert(page_id_t pid, Transaction& txn, std::string_view key,
                          page_id_t value) {
  const bin_size_t physical_size = SerializeSize(key) + sizeof(value);
  if (kPageBodySize / 6 < physical_size) {
    return Status::kTooBigData;
  }
  if (free_size_ <
      physical_size + sizeof(RowPointer) * (row_count_ + kExtraIdx + 1)) {
    return Status::kNoSpace;
  }
  size_t pos = SearchToInsert(key);
  if (pos != row_count_ && GetKey(pos) == key) {
    return Status::kDuplicates;
  }
  InsertImpl(key, value);
  txn.InsertBranchLog(pid, key, value);
  return Status::kSuccess;
}

void BranchPage::InsertImpl(std::string_view key, page_id_t pid) {
  const bin_size_t physical_size = SerializeSize(key) + sizeof(page_id_t);
  free_size_ -= physical_size + sizeof(RowPointer);
  if ((Payload() + free_ptr_ - physical_size) <=
      reinterpret_cast<char*>(&rows_[row_count_ + kExtraIdx + 2])) {
    DeFragment();
  }
  const int pos = SearchToInsert(key);
  ++row_count_;
  assert(physical_size <= free_ptr_);
  free_ptr_ -= physical_size;
  SerializePID(Payload() + free_ptr_, pid);
  SerializeStringView(Payload() + free_ptr_ + sizeof(page_id_t), key);
  memmove(rows_ + kExtraIdx + pos + 1, rows_ + kExtraIdx + pos,
          (row_count_ - pos) * sizeof(RowPointer));
  rows_[kExtraIdx + pos].offset = free_ptr_;
  rows_[kExtraIdx + pos].size = physical_size;
}

Status BranchPage::Update(page_id_t pid, Transaction& txn, std::string_view key,
                          page_id_t value) {
  const bin_size_t physical_size = SerializeSize(key) + sizeof(page_id_t);
  if (kPageBodySize / 6 < physical_size) {
    return Status::kTooBigData;
  }
  const size_t pos = Search(key);
  if (row_count_ < pos || GetKey(pos) != key) {
    return Status::kNotExists;
  }
  if (rows_[pos].size < physical_size &&
      physical_size - rows_[pos].size > free_size_) {
    return Status::kNoSpace;
  }
  txn.UpdateBranchLog(pid, key, value, GetValue(pos));
  UpdateImpl(key, value);
  return Status::kSuccess;
}

void BranchPage::UpdateImpl(std::string_view key, page_id_t pid) {
  memcpy(Payload() + rows_[Search(key) + kExtraIdx].offset, &pid, sizeof(pid));
}

void BranchPage::UpdateSlotImpl(RowPointer& pos, std::string_view payload) {
  assert(payload.size() <= free_size_);
  if (Payload() + free_ptr_ - payload.size() <=
      reinterpret_cast<char*>(&rows_[row_count_ + kExtraIdx + 1])) {
    DeFragment();
  }
  free_ptr_ -= payload.size();
  free_size_ += pos.size;
  free_size_ -= payload.size();
  pos.size = payload.size();
  pos.offset = free_ptr_;
  memcpy(Payload() + pos.offset, payload.data(), payload.size());
}

Status BranchPage::Delete(page_id_t pid, Transaction& txn,
                          std::string_view key) {
  const int pos = Search(key);
  if (pos < 0) {
    page_id_t next_lowest = GetValue(0);
    page_id_t prev_lowest = lowest_page_;
    txn.DeleteBranchLog(pid, GetKey(0), next_lowest);
    txn.SetLowestLog(pid, next_lowest, prev_lowest);
  } else {
    txn.DeleteBranchLog(pid, GetKey(pos), GetValue(pos));
  }
  DeleteImpl(key);
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
  memmove(rows_ + pos + kExtraIdx, rows_ + pos + kExtraIdx + 1,
          sizeof(RowPointer) * (row_count_ - pos));
  --row_count_;
}

StatusOr<page_id_t> BranchPage::GetPageForKey(Transaction& /*txn*/,
                                              std::string_view key) const {
  assert(0 < row_count_);
  if (key < GetKey(0)) {
    return lowest_page_;
  }
  return GetValue(Search(key));
}

void BranchPage::SetFence(RowPointer& fence_pos, const IndexKey& new_fence) {
  if (new_fence.IsMinusInfinity()) {
    fence_pos = kMinusInfinity;
  } else if (new_fence.IsPlusInfinity()) {
    fence_pos = kPlusInfinity;
  } else {
    const size_t physical_size = SerializeSize(new_fence);
    assert(physical_size <= free_size_);
    UpdateSlotImpl(fence_pos, SerializeIndexKey(new_fence));
  }
}

Status BranchPage::SetLowFence(page_id_t pid, Transaction& txn,
                               const IndexKey& lf) {
  if (lf.IsNotInfinity()) {
    const std::string_view new_fence = lf.GetKey().Value();
    const size_t physical_size = SerializeSize(lf);
    if (physical_size > new_fence.size() &&
        free_size_ < physical_size - new_fence.size()) {
      return Status::kNoSpace;
    }
  }
  txn.SetLowFence(pid, lf, GetLowFence());
  SetFence(rows_[kLowFenceIdx], lf);
  return Status::kSuccess;
}

Status BranchPage::SetHighFence(page_id_t pid, Transaction& txn,
                                const IndexKey& hf) {
  if (hf.IsNotInfinity()) {
    const std::string_view new_fence = hf.GetKey().Value();
    const size_t physical_size = SerializeSize(hf);
    if (physical_size > new_fence.size() &&
        free_size_ < physical_size - new_fence.size()) {
      return Status::kNoSpace;
    }
  }
  txn.SetHighFence(pid, hf, GetHighFence());
  SetFence(rows_[kHighFenceIdx], hf);
  return Status::kSuccess;
}

IndexKey BranchPage::GetLowFence() const {
  if (rows_[kLowFenceIdx] == kMinusInfinity) {
    return IndexKey::MinusInfinity();
  }
  return IndexKey::Deserialize(Payload() + rows_[kLowFenceIdx].offset);
}

IndexKey BranchPage::GetHighFence() const {
  if (rows_[kHighFenceIdx] == kPlusInfinity) {
    return IndexKey::PlusInfinity();
  }
  return IndexKey::Deserialize(Payload() + rows_[kHighFenceIdx].offset);
}

void BranchPage::SetLowFenceImpl(const IndexKey& lf) {
  SetFence(rows_[kLowFenceIdx], lf);
}

void BranchPage::SetHighFenceImpl(const IndexKey& hf) {
  SetFence(rows_[kHighFenceIdx], hf);
}

Status BranchPage::SetFoster(page_id_t pid, Transaction& txn,
                             const FosterPair& new_foster) {
  size_t physical_size =
      new_foster.IsEmpty() ? 0
                           : SerializeSize(new_foster.key) + sizeof(page_id_t);
  if (rows_[kFosterIdx].size < physical_size &&
      free_size_ < physical_size - rows_[kFosterIdx].size) {
    return Status::kNoSpace;
  }
  if (auto prev_foster = GetFoster()) {
    txn.SetFoster(pid, new_foster, prev_foster.Value());
  } else {
    txn.SetFoster(pid, new_foster, {});
  }
  SetFosterImpl(new_foster);
  return Status::kSuccess;
}

void BranchPage::SetFosterImpl(const FosterPair& foster) {
  if (foster.IsEmpty()) {
    UpdateSlotImpl(rows_[kFosterIdx], "");
    return;
  }
  bin_size_t physical_size =
      SerializeSize(foster.key) + sizeof(foster.child_pid);
  std::string payload(physical_size, 0);
  SerializeStringView(payload.data(), foster.key);
  SerializePID(payload.data() + SerializeSize(foster.key), foster.child_pid);
  UpdateSlotImpl(rows_[kFosterIdx], payload);
}

StatusOr<FosterPair> BranchPage::GetFoster() const {
  if (rows_[kFosterIdx].size == 0) {
    return Status::kNotExists;
  }
  std::string_view serialized_key;
  page_id_t child = 0;
  size_t offset = DeserializeStringView(Payload() + rows_[kFosterIdx].offset,
                                        &serialized_key);
  DeserializePID(Payload() + rows_[kFosterIdx].offset + offset, &child);
  return FosterPair(serialized_key, child);
}

void BranchPage::Split(page_id_t /*pid*/, Transaction& txn,
                       std::string_view key, Page* right, std::string* middle) {
  const size_t kPayload = kPageBodySize - offsetof(BranchPage, rows_);
  const size_t kThreshold = kPayload / 2;
  const size_t expected_size =
      SerializeSize(key) + sizeof(page_id_t) + sizeof(RowPointer);
  assert(right->type == PageType::kBranchPage);
  size_t consumed_size = 0;
  size_t pivot = 0;
  while (consumed_size < kThreshold && pivot < row_count_ - 2 &&
         pivot < row_count_) {
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

bin_size_t BranchPage::SearchToInsert(std::string_view key) const {
  int left = kExtraIdx - 1;
  int right = row_count_ + static_cast<int>(kExtraIdx);
  while (1 < right - left) {
    const int cur = (left + right) / 2;
    std::string_view cur_key = GetRow(cur);
    if (key < cur_key) {
      right = cur;
    } else {
      left = cur;
    }
  }
  return right - static_cast<int>(kExtraIdx);
}

int BranchPage::Search(std::string_view key) const {
  int left = kExtraIdx - 1;
  int right = row_count_ + static_cast<int>(kExtraIdx);
  while (1 < right - left) {
    const int cur = (left + right) / 2;
    std::string_view cur_key = GetRow(cur);
    if (key < cur_key) {
      right = cur;
    } else {
      left = cur;
    }
  }
  return left - static_cast<int>(kExtraIdx);
}

std::string_view BranchPage::GetKey(size_t idx) const {
  assert(idx < row_count_);
  return GetRow(idx + kExtraIdx);
}

std::string_view BranchPage::GetRow(size_t idx) const {
  std::string_view key;
  assert(idx < row_count_ + kExtraIdx);
  DeserializeStringView(Payload() + rows_[idx].offset + sizeof(page_id_t),
                        &key);
  return key;
}

page_id_t BranchPage::GetValue(size_t idx) const {
  page_id_t result = 0;
  assert(idx < row_count_);
  memcpy(&result, Payload() + rows_[idx + kExtraIdx].offset, sizeof(result));
  return result;
}

void BranchPage::Dump(std::ostream& o, int indent) const {
  o << "Rows: " << row_count_ << " LowFence: " << GetLowFence()
    << " HighFence: " << GetHighFence() << " FreeSize: " << free_size_
    << " FreePtr:" << free_ptr_ << " Lowest: " << lowest_page_;
  if (row_count_ == 0) {
    return;
  }
  o << "\n" << Indent(indent + 2) << lowest_page_;
  for (size_t i = 0; i < row_count_; ++i) {
    o << "\n"
      << Indent(indent) << OmittedString(GetKey(i), 20) << "\n"
      << Indent(indent + 2) << GetValue(i);
  }
  if (0 < rows_[kFosterIdx].size) {
    std::string_view serialized_key;
    page_id_t child = 0;
    size_t offset = DeserializeStringView(Payload() + rows_[kFosterIdx].offset,
                                          &serialized_key);
    DeserializePID(Payload() + rows_[kFosterIdx].offset + offset, &child);
    o << "\n"
      << Indent(indent) << "  FosterKey: " << OmittedString(serialized_key, 20)
      << " -> " << child;
  }
}

std::string SmallestKey(PageRef&& page) {
  if (page->Type() == PageType::kLeafPage) {
    return std::string(page->body.leaf_page.GetKey(0));
  }
  if (page->Type() == PageType::kBranchPage) {
    return std::string(page->body.branch_page.GetKey(0));
  }
  assert(!"invalid page type");
}

std::string BiggestKey(PageRef&& page) {
  if (page->Type() == PageType::kLeafPage) {
    slot_t count = page->body.leaf_page.RowCount();
    return std::string(page->body.leaf_page.GetKey(count - 1));
  }
  if (page->Type() == PageType::kBranchPage) {
    slot_t count = page->body.branch_page.RowCount();
    return std::string(page->body.branch_page.GetKey(count - 1));
  }
  assert(!"invalid page type");
}

bool SanityCheck(PageRef&& page, PageManager* pm) {
  if (page->Type() == PageType::kLeafPage) {
    return page->body.leaf_page.SanityCheckForTest();
  }
  if (page->Type() == PageType::kBranchPage) {
    return page->body.branch_page.SanityCheckForTest(pm);
  }
  assert(!"invalid page type");
}

bool BranchPage::SanityCheckForTest(PageManager* pm) const {
  bool sanity = SanityCheck(pm->GetPage(lowest_page_), pm);
  if (!sanity) {
    return false;
  }
  if (row_count_ == 0) {
    LOG(FATAL) << "Branch page is empty";
    return false;
  }
  const Page* this_page = GET_CONST_PAGE_PTR(this);
  for (size_t i = 0; i < row_count_ - 1; ++i) {
    if (GetKey(i + 1) < GetKey(i)) {
      LOG(FATAL) << "key not ordered";
      return false;
    }
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
    if (!sanity) {
      return false;
    }
  }
  return SanityCheck(pm->GetPage(GetValue(row_count_ - 1)), pm);
}

void BranchPage::DeFragment() {
  std::vector<std::string> payloads;
  payloads.reserve(row_count_);
  for (size_t i = 0; i < row_count_ + kExtraIdx; ++i) {
    payloads.emplace_back(Payload() + rows_[i].offset, rows_[i].size);
  }
  bin_size_t offset = kPageBodySize;
  for (size_t i = 0; i < row_count_ + kExtraIdx; ++i) {
    offset -= payloads[i].size();
    if (0 < rows_[i].size) {
      rows_[i].offset = offset;
    }
    memcpy(Payload() + offset, payloads[i].data(), payloads[i].size());
  }
  free_ptr_ = offset;
}

Status BranchPage::MoveRightToFoster(Transaction& txn, Page& right) {
  assert(right.Type() == PageType::kBranchPage);
  assert(0 < row_count_);
  ASSIGN_OR_CRASH(FosterPair, old_foster, GetFoster());
  assert(old_foster.child_pid == right.PageID());
  std::string move_key(GetKey(row_count_ - 1));
  page_id_t move_value = GetValue(row_count_ - 1);
  RETURN_IF_FAIL(right.InsertBranch(txn, old_foster.key,
                                    right.body.branch_page.lowest_page_));
  right.SetLowestValue(txn, move_value);
  Page* this_page = GET_PAGE_PTR(this);
  STATUS(this_page->Delete(txn, move_key), "Delete must success");
  RETURN_IF_FAIL(
      this_page->SetFoster(txn, FosterPair(move_key, right.page_id)));
  return Status::kSuccess;
}

Status BranchPage::MoveLeftFromFoster(Transaction& txn, Page& right) {
  assert(right.Type() == PageType::kBranchPage);
  assert(0 < right.RowCount());
  Page* this_page = GET_PAGE_PTR(this);
  ASSIGN_OR_CRASH(FosterPair, old_foster, GetFoster());
  assert(old_foster.child_pid == right.PageID());
  std::string move_key(right.GetKey(0));
  if (1 < right.RowCount()) {
    // Move leftmost entry from foster.
    page_id_t move_value = right.body.branch_page.GetValue(0);
    COERCE(this_page->SetFoster(txn, FosterPair(move_key, right.PageID())));
    COERCE(this_page->InsertBranch(txn, old_foster.key,
                                   right.body.branch_page.lowest_page_));
    right.SetLowestValue(txn, move_value);
    COERCE(right.Delete(txn, move_key));
    return Status::kSuccess;
  }
  // Merge foster.
  COERCE(this_page->SetFoster(txn, FosterPair()));
  COERCE(this_page->InsertBranch(txn, old_foster.key,
                                 right.body.branch_page.lowest_page_));
  COERCE(this_page->InsertBranch(txn, right.GetKey(0), right.GetPage(0)));
  COERCE(right.Delete(txn, move_key));
  right.SetLowestValue(txn, 0);
  LOG(WARN) << "merge";
  return Status::kSuccess;
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::BranchPage>::operator()(
    const ::tinylamb::BranchPage& r) const {
  uint64_t ret = 0;
  ret += std::hash<tinylamb::slot_t>()(r.row_count_);
  ret += std::hash<tinylamb::bin_size_t>()(r.free_ptr_);
  ret += std::hash<tinylamb::bin_size_t>()(r.free_size_);
  ret += std::hash<tinylamb::IndexKey>()(r.GetLowFence());
  ret += std::hash<tinylamb::IndexKey>()(r.GetHighFence());
  if (auto foster = r.GetFoster()) {
    ret += std::hash<tinylamb::FosterPair>()(foster.Value());
  }
  for (size_t i = 0; i < r.row_count_; ++i) {
    ret += std::hash<std::string_view>()(r.GetKey(i));
    ret += std::hash<tinylamb::page_id_t>()(r.GetValue(i));
  }
  return ret;
}
