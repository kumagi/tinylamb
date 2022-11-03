//
// Created by kumagi on 2022/01/15.
//

#include "page/leaf_page.hpp"

#include <cstring>

#include "common/debug.hpp"
#include "common/serdes.hpp"
#include "page/index_key.hpp"
#include "page/page.hpp"
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

std::string_view LeafPage::GetKey(size_t idx) const {
  std::string_view ret;
  DeserializeStringView(Payload() + rows_[idx].offset, &ret);
  return ret;
}

std::string_view LeafPage::GetValue(size_t idx) const {
  std::string_view ret;
  DeserializeStringView(
      Payload() + rows_[idx].offset + SerializeSize(GetKey(idx)), &ret);
  return ret;
}

Status LeafPage::Insert(page_id_t page_id, Transaction& txn,
                        std::string_view key, std::string_view value) {
  constexpr size_t kThreshold = kPageBodySize / 6;
  const bin_size_t physical_size = SerializeSize(key) + SerializeSize(value);
  const bin_size_t expected_size = physical_size + sizeof(RowPointer);
  if (kThreshold < expected_size) {
    LOG(INFO) << "too big: " << kThreshold << " << " << expected_size;
    return Status::kTooBigData;
  }
  if (free_size_ < expected_size) {
    return Status::kNoSpace;
  }
  size_t pos = Find(key);
  if (pos != row_count_ && GetKey(pos) == key) {
    return Status::kDuplicates;
  }

  InsertImpl(key, value);
  txn.InsertLeafLog(page_id, key, value);
  return Status::kSuccess;
}

void LeafPage::InsertImpl(std::string_view key, std::string_view value) {
  const bin_size_t physical_size = SerializeSize(key) + SerializeSize(value);
  assert(physical_size + sizeof(RowPointer) <= free_size_);
  if (free_ptr_ <= sizeof(RowPointer) * (row_count_ + 1) + physical_size) {
    DeFragment();
  }
  assert(sizeof(RowPointer) * (row_count_ + 1) + physical_size <= free_ptr_);
  free_size_ -= physical_size + sizeof(RowPointer);
  free_ptr_ -= physical_size;

  bin_size_t write_offset = free_ptr_;
  write_offset += SerializeStringView(Payload() + write_offset, key);
  SerializeStringView(Payload() + write_offset, value);

  size_t pos = Find(key);
  memmove(rows_ + pos + 1, rows_ + pos,
          sizeof(RowPointer) * (row_count_ - pos));
  row_count_++;
  rows_[pos] = {free_ptr_, physical_size};
}

Status LeafPage::Update(page_id_t page_id, Transaction& txn,
                        std::string_view key, std::string_view value) {
  constexpr size_t kThreshold = kPageBodySize / 6;
  const bin_size_t physical_size = SerializeSize(key) + SerializeSize(value);
  if (kThreshold < physical_size) {
    return Status::kTooBigData;
  }
  ASSIGN_OR_RETURN(std::string_view, old_value, Read(page_id, txn, key));
  const bin_size_t old_size = SerializeSize(key) + SerializeSize(old_value);
  if (old_size < physical_size && free_size_ < physical_size - old_size) {
    return Status::kNoSpace;
  }

  txn.UpdateLeafLog(page_id, key, value, old_value);
  UpdateImpl(key, value);
  return Status::kSuccess;
}

void LeafPage::UpdateImpl(std::string_view key, std::string_view redo) {
  const size_t physical_size = SerializeSize(key) + SerializeSize(redo);
  const size_t pos = Find(key);
  if (rows_[pos].size < physical_size) {
    assert(physical_size - rows_[pos].size <= free_size_);
  }
  if (rows_[pos].size < physical_size) {
    if (free_ptr_ <= sizeof(RowPointer) * (row_count_ + 1) + physical_size) {
      free_size_ += rows_[pos].size;
      rows_[pos].size = 0;
      DeFragment();
    }
    assert(sizeof(RowPointer) * (row_count_ + 1) + physical_size <= free_ptr_);
    free_ptr_ -= physical_size;
    rows_[pos].offset = free_ptr_;
  }
  free_size_ += rows_[pos].size;
  free_size_ -= physical_size;
  rows_[pos].size = physical_size;
  bin_size_t write_offset = rows_[pos].offset;
  write_offset += SerializeStringView(Payload() + write_offset, key);
  SerializeStringView(Payload() + write_offset, redo);
}

Status LeafPage::Delete(page_id_t page_id, Transaction& txn,
                        std::string_view key) {
  ASSIGN_OR_RETURN(std::string_view, existing_value, Read(page_id, txn, key));
  size_t pos = Find(key);

  txn.DeleteLeafLog(page_id, key, existing_value);
  DeleteImpl(key);
  assert(SanityCheckForTest());
  return Status::kSuccess;
}

void LeafPage::DeleteImpl(std::string_view key) {
  assert(0 < row_count_);
  size_t pos = Find(key);
  assert(pos < row_count_);
  free_size_ += rows_[pos].size + sizeof(RowPointer);
  memmove(rows_ + pos, rows_ + pos + 1,
          sizeof(RowPointer) * (row_count_ - pos));
  --row_count_;
}

StatusOr<std::string_view> LeafPage::Read(page_id_t /*unused*/,
                                          Transaction& /*unused*/,
                                          slot_t slot) const {
  if (row_count_ <= slot) {
    return Status::kNotExists;
  }
  return GetValue(slot);
}

StatusOr<std::string_view> LeafPage::ReadKey(page_id_t /*unused*/,
                                             Transaction& /*unused*/,
                                             slot_t slot) const {
  if (row_count_ <= slot) {
    return Status::kNotExists;
  }
  return GetKey(slot);
}

StatusOr<std::string_view> LeafPage::Read(page_id_t /*unused*/,
                                          Transaction& /*unused*/,
                                          std::string_view key) const {
  size_t pos = Find(key);
  if (pos < row_count_ && GetKey(pos) == key) {
    return GetValue(pos);
  }
  return Status::kNotExists;
}

StatusOr<std::string_view> LeafPage::LowestKey(Transaction& /*unused*/) const {
  if (row_count_ == 0) {
    return Status::kNotExists;
  }
  return GetKey(0);
}

StatusOr<std::string_view> LeafPage::HighestKey(Transaction& /*unused*/) const {
  if (row_count_ == 0) {
    return Status::kNotExists;
  }
  return GetKey(row_count_ - 1);
}

slot_t LeafPage::RowCount() const { return row_count_; }

void LeafPage::Split(page_id_t pid, Transaction& txn, std::string_view key,
                     std::string_view value, Page* right) {
  const size_t kPayload = kPageBodySize - offsetof(LeafPage, rows_);
  const size_t kThreshold = kPayload / 2;
  const size_t expected_size =
      SerializeSize(key) + SerializeSize(value) + sizeof(RowPointer);
  assert(expected_size < kThreshold);
  assert(right->type == PageType::kLeafPage);
  size_t consumed_size = 0;
  size_t pivot = 0;
  while (consumed_size < kThreshold && pivot < row_count_ - 1) {
    consumed_size += SerializeSize(GetKey(pivot)) +
                     SerializeSize(GetValue(pivot)) + sizeof(RowPointer);
    pivot++;
  }
  while (GetKey(pivot) < key && consumed_size < expected_size) {
    pivot++;
    consumed_size +=
        SerializeSize(GetKey(pivot)) + sizeof(RowPointer) + sizeof(RowPointer);
  }
  while (key < GetKey(pivot) && kPayload < consumed_size + expected_size) {
    consumed_size -=
        SerializeSize(GetKey(pivot)) + sizeof(RowPointer) + sizeof(RowPointer);
    pivot--;
  }

  const size_t original_row_count = row_count_;
  for (size_t i = pivot; i < row_count_; ++i) {
    right->InsertLeaf(txn, GetKey(i), GetValue(i));
  }
  Page* this_page = GET_PAGE_PTR(this);
  for (size_t i = pivot; i < original_row_count; ++i) {
    this_page->Delete(txn, GetKey(pivot));
  }

  if (right->RowCount() == 0 || right->GetKey(0) <= key) {
    assert(expected_size <= right->body.leaf_page.free_size_);
  } else {
    assert(expected_size <= free_size_);
  }
}

void LeafPage::DeFragment() {
  std::vector<std::string> payloads;
  payloads.reserve(row_count_);
  for (size_t i = 0; i < row_count_; ++i) {
    payloads.emplace_back(Payload() + rows_[i].offset, rows_[i].size);
  }
  if (low_fence_ != kMinusInfinity) {
    payloads.emplace_back(Payload() + low_fence_.offset, low_fence_.size);
  }
  if (high_fence_ != kPlusInfinity) {
    payloads.emplace_back(Payload() + high_fence_.offset, high_fence_.size);
  }
  if (foster_.offset != 0) {
    payloads.emplace_back(Payload() + foster_.offset, foster_.size);
  }
  free_ptr_ = kPageBodySize - offsetof(LeafPage, rows_);
  for (size_t i = 0; i < row_count_; ++i) {
    assert(payloads[i].size() <= free_ptr_);
    assert(payloads[i].size() == rows_[i].size);
    free_ptr_ -= payloads[i].size();
    rows_[i].offset = free_ptr_;
    memcpy(Payload() + free_ptr_, payloads[i].data(), payloads[i].size());
  }
  size_t extra_offset = 0;
  if (low_fence_ != kMinusInfinity) {
    assert(low_fence_.size <= free_ptr_);
    free_ptr_ -= low_fence_.size;
    low_fence_.offset = free_ptr_;
    memcpy(Payload() + free_ptr_, payloads[row_count_].data(),
           payloads[row_count_].size());
    extra_offset++;
  }
  if (high_fence_ != kPlusInfinity) {
    assert(high_fence_.size <= free_ptr_);
    free_ptr_ -= high_fence_.size;
    high_fence_.offset = free_ptr_;
    memcpy(Payload() + free_ptr_, payloads[row_count_ + extra_offset].data(),
           payloads[row_count_ + extra_offset].size());
    extra_offset++;
  }
  if (foster_.offset != 0) {
    assert(foster_.size <= free_ptr_);
    free_ptr_ -= foster_.size;
    foster_.offset = free_ptr_;
    memcpy(Payload() + free_ptr_, payloads[row_count_ + extra_offset].data(),
           payloads[row_count_ + extra_offset].size());
    extra_offset++;
  }
}

size_t LeafPage::Find(std::string_view key) const {
  int left = -1;
  int right = row_count_;
  while (1 < right - left) {
    const int cur = (left + right) / 2;
    std::string_view cur_key = GetKey(cur);
    if (cur_key < key) {
      left = cur;
    } else {
      right = cur;
    }
  }
  return right;
}

void LeafPage::SetFence(RowPointer& fence_pos, const IndexKey& new_fence) {
  const size_t physical_size = SerializeSize(new_fence);
  if (free_ptr_ < physical_size) {
    fence_pos = RowPointer();
    DeFragment();
  }
  free_ptr_ -= physical_size;
  free_size_ += fence_pos.size;
  if (new_fence.IsMinusInfinity()) {
    fence_pos = kMinusInfinity;
  } else if (new_fence.IsPlusInfinity()) {
    fence_pos = kPlusInfinity;
  } else {
    ASSIGN_OR_CRASH(std::string_view, key, new_fence.GetKey());
    free_size_ -= physical_size;
    fence_pos.size = physical_size;
    fence_pos.offset = free_ptr_;
    SerializeStringView(Payload() + fence_pos.offset, key);
  }
}

Status LeafPage::SetLowFence(page_id_t pid, Transaction& txn,
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
  SetFence(low_fence_, lf);
  return Status::kSuccess;
}

Status LeafPage::SetHighFence(page_id_t pid, Transaction& txn,
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
  SetFence(high_fence_, hf);
  return Status::kSuccess;
}

void LeafPage::SetLowFenceImpl(const IndexKey& lf) { SetFence(low_fence_, lf); }

void LeafPage::SetHighFenceImpl(const IndexKey& hf) {
  SetFence(high_fence_, hf);
}

IndexKey LeafPage::GetLowFence() const {
  if (low_fence_ == kMinusInfinity) {
    return IndexKey::MinusInfinity();
  }
  assert(low_fence_ != kPlusInfinity);
  std::string_view ret;
  DeserializeStringView(Payload() + low_fence_.offset, &ret);
  return IndexKey(ret);
}

IndexKey LeafPage::GetHighFence() const {
  if (high_fence_ == kPlusInfinity) {
    return IndexKey::PlusInfinity();
  }
  assert(high_fence_ != kMinusInfinity);
  std::string_view ret;
  DeserializeStringView(Payload() + high_fence_.offset, &ret);
  return IndexKey(ret);
}

Status LeafPage::SetFoster(page_id_t pid, Transaction& txn,
                           const FosterPair& new_foster) {
  size_t physical_size = SerializeSize(new_foster.key) + sizeof(page_id_t);
  if (foster_.size < physical_size &&
      free_size_ < physical_size - foster_.size) {
    return Status::kNoSpace;
  }
  StatusOr<FosterPair> prev_foster = GetFoster();
  if (prev_foster.HasValue()) {
    txn.SetFoster(pid, new_foster, prev_foster.Value());
  } else {
    txn.SetFoster(pid, new_foster, FosterPair("", 0));
  }
  SetFosterImpl(new_foster);
  return Status::kSuccess;
}

void LeafPage::SetFosterImpl(const FosterPair& foster) {
  free_size_ += foster_.size;
  if (foster.key.empty()) {
    foster_ = {0, 0};
    return;
  }
  bin_size_t physical_size =
      SerializeSize(foster.key) + sizeof(foster.child_pid);
  if (free_ptr_ <= sizeof(RowPointer) * (row_count_) + physical_size) {
    DeFragment();
  }
  free_ptr_ -= physical_size;
  free_size_ -= physical_size;
  foster_ = {free_ptr_, physical_size};
  size_t offset = SerializeStringView(Payload() + foster_.offset, foster.key);
  SerializePID(Payload() + foster_.offset + offset, foster.child_pid);
}

StatusOr<FosterPair> LeafPage::GetFoster() const {
  if (foster_.size == 0) {
    return Status::kNotExists;
  }
  std::string_view serialized_key;
  page_id_t child;
  size_t offset =
      DeserializeStringView(Payload() + foster_.offset, &serialized_key);
  DeserializePID(Payload() + foster_.offset + offset, &child);
  return FosterPair(serialized_key, child);
}

void LeafPage::Dump(std::ostream& o, int indent) const {
  o << "Rows: " << row_count_ << " LowFence: " << GetLowFence()
    << " HighFence: " << GetHighFence() << " FreeSize: " << free_size_
    << " FreePtr:" << free_ptr_;
  for (size_t i = 0; i < row_count_; ++i) {
    o << "\n"
      << Indent(indent) << OmittedString(GetKey(i), 40) << " :"
      << OmittedString(GetValue(i), 20);
  }
  if (0 < foster_.size) {
    std::string_view serialized_key;
    page_id_t child = 0;
    size_t offset =
        DeserializeStringView(Payload() + foster_.offset, &serialized_key);
    DeserializePID(Payload() + foster_.offset + offset, &child);
    o << "\n"
      << Indent(indent) << "  FosterKey: " << serialized_key << " -> " << child;
  }
}

Status LeafPage::MoveRightToFoster(Transaction& txn, Page& right) {
  assert(right.Type() == PageType::kLeafPage);
  assert(1 < row_count_);
  std::string_view move_key = GetKey(row_count_ - 1);
  std::string_view move_value = GetValue(row_count_ - 1);
  RETURN_IF_FAIL(right.InsertLeaf(txn, move_key, move_value));
  Page* this_page = GET_PAGE_PTR(this);
  RETURN_IF_FAIL(
      this_page->SetFoster(txn, FosterPair(move_key, right.page_id)));
  RETURN_IF_FAIL(this_page->Delete(txn, move_key));
  return Status::kSuccess;
}

Status LeafPage::MoveLeftFromFoster(Transaction& txn, Page& right) {
  assert(right.Type() == PageType::kLeafPage);
  assert(1 < right.RowCount());
  std::string_view move_key = right.GetKey(0);
  std::string_view next_foster_key = right.GetKey(1);
  std::string_view move_value = right.body.leaf_page.GetValue(0);
  Page* this_page = GET_PAGE_PTR(this);
  RETURN_IF_FAIL(this_page->InsertLeaf(txn, move_key, move_value));
  RETURN_IF_FAIL(
      this_page->SetFoster(txn, FosterPair(next_foster_key, right.PageID())));
  RETURN_IF_FAIL(right.Delete(txn, move_key));
  return Status::kSuccess;
}

bool LeafPage::SanityCheckForTest() const {
  for (slot_t i = 0; i < row_count_ - 1; ++i) {
    if (GetKey(i + 1) < GetKey(i)) {
      return false;
    }
  }
  return true;
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::LeafPage>::operator()(
    const ::tinylamb::LeafPage& r) const {
  uint64_t ret = 0x1eaf1eaf;
  ret += std::hash<tinylamb::slot_t>()(r.row_count_);
  ret += std::hash<tinylamb::bin_size_t>()(r.free_ptr_);
  ret += std::hash<tinylamb::bin_size_t>()(r.free_size_);
  ret += std::hash<tinylamb::IndexKey>()(r.GetLowFence());
  ret += std::hash<tinylamb::IndexKey>()(r.GetHighFence());
  tinylamb::StatusOr<tinylamb::FosterPair> foster = r.GetFoster();
  if (foster.HasValue()) {
    ret += std::hash<std::string_view>()(foster.Value().key);
    ret += std::hash<tinylamb::page_id_t>()(foster.Value().child_pid);
  }
  for (int i = 0; i < r.row_count_; ++i) {
    ret += std::hash<std::string_view>()(r.GetKey(i));
    ret += std::hash<std::string_view>()(r.GetValue(i));
  }
  return ret;
}
