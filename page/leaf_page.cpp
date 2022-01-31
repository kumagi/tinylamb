//
// Created by kumagi on 2022/01/15.
//

#include "page/leaf_page.hpp"

#include <cstring>

#include "common/serdes.hpp"
#include "page/page.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

LeafPage::RowPointer* LeafPage::Rows() {
  return reinterpret_cast<RowPointer*>(Payload() + kPageBodySize -
                                       row_count_ * sizeof(RowPointer));
}

const LeafPage::RowPointer* LeafPage::Rows() const {
  return reinterpret_cast<const RowPointer*>(Payload() + kPageBodySize -
                                             row_count_ * sizeof(RowPointer));
}

std::string_view LeafPage::GetKey(size_t idx) const {
  const RowPointer* pos =
      reinterpret_cast<const RowPointer*>(Payload() + kPageBodySize -
                                          row_count_ * sizeof(RowPointer)) +
      idx;
  std::string_view ret;
  DeserializeStringView(Payload() + pos->offset, &ret);
  return ret;
}

std::string_view LeafPage::GetValue(size_t idx) const {
  const RowPointer* pos =
      reinterpret_cast<const RowPointer*>(Payload() + kPageBodySize -
                                          row_count_ * sizeof(RowPointer)) +
      idx;
  std::string_view key = GetKey(idx);
  std::string_view ret;
  DeserializeStringView(
      Payload() + pos->offset + sizeof(bin_size_t) + key.size(), &ret);
  return ret;
}

bool LeafPage::Insert(page_id_t page_id, Transaction& txn, std::string_view key,
                      std::string_view value) {
  const bin_size_t physical_size =
      sizeof(bin_size_t) + key.size() + sizeof(bin_size_t) + value.size();

  size_t pos = Find(key);
  if (free_size_ < physical_size + sizeof(RowPointer) ||  // No space.
      (pos != row_count_ && GetKey(pos) == key)) {        // Already exists.

    return false;
  }

  InsertImpl(key, value);
  txn.InsertLeafLog(page_id, key, value);
  return true;
}

void LeafPage::InsertImpl(std::string_view key, std::string_view value) {
  const bin_size_t physical_size =
      sizeof(bin_size_t) + key.size() + sizeof(bin_size_t) + value.size();
  free_size_ -= physical_size + sizeof(RowPointer);

  if (kPageSize - sizeof(RowPointer) * (row_count_ + 1) <
      free_ptr_ + physical_size) {
    DeFragment();
  }

  size_t pos = Find(key);
  row_count_++;
  RowPointer* rows = Rows();
  memmove(&rows[0], &rows[1], sizeof(RowPointer) * pos);
  rows[pos].offset = free_ptr_;
  rows[pos].size = physical_size;

  free_ptr_ += SerializeStringView(Payload() + free_ptr_, key);
  free_ptr_ += SerializeStringView(Payload() + free_ptr_, value);
}

bool LeafPage::Update(page_id_t page_id, Transaction& txn, std::string_view key,
                      std::string_view value) {
  const size_t physical_size =
      sizeof(bin_size_t) + key.size() + sizeof(bin_size_t) + value.size();

  std::string_view existing_value;
  if (!Read(txn, key, &existing_value)) return false;

  const int size_diff = physical_size - existing_value.size();
  if (free_size_ < size_diff) return false;

  txn.UpdateLeafLog(page_id, key, existing_value, value);
  UpdateImpl(key, value);
  return true;
}

void LeafPage::UpdateImpl(std::string_view key, std::string_view redo) {
  const size_t physical_size =
      sizeof(bin_size_t) + key.size() + sizeof(bin_size_t) + redo.size();
  if (kPageBodySize - sizeof(RowPointer) * row_count_ <
      free_ptr_ + physical_size) {
    DeFragment();
  }
  size_t pos = Find(key);
  RowPointer* rows = Rows();
  rows[pos].offset = free_ptr_;
  rows[pos].size = physical_size;
  free_ptr_ += SerializeStringView(Payload() + free_ptr_, key);
  free_ptr_ += SerializeStringView(Payload() + free_ptr_, redo);
}

bool LeafPage::Delete(page_id_t page_id, Transaction& txn,
                      std::string_view key) {
  std::string_view existing_value;
  if (!Read(txn, key, &existing_value)) return false;

  txn.DeleteLeafLog(page_id, key, existing_value);
  DeleteImpl(key);
  return true;
}

void LeafPage::DeleteImpl(std::string_view key) {
  RowPointer* rows = Rows();
  size_t pos = Find(key);
  free_size_ += rows[pos].size + sizeof(RowPointer);
  memmove(rows + 1, rows, sizeof(RowPointer) * pos);
  --row_count_;
}

bool LeafPage::Read(Transaction& txn, std::string_view key,
                    std::string_view* result = nullptr) {
  if (result) *result = std::string_view();
  size_t pos = Find(key);
  bool found = pos < row_count_ && GetKey(pos) == key;
  if (result && found) *result = GetValue(pos);
  return found;
}

bool LeafPage::LowestKey(Transaction& txn, std::string_view* result) {
  *result = std::string_view();
  if (row_count_ == 0) return false;
  RowPointer* rows = Rows();
  *result = GetKey(0);
  return true;
}

bool LeafPage::HighestKey(Transaction& txn, std::string_view* result) {
  *result = std::string_view();
  if (row_count_ == 0) return false;
  RowPointer* rows = Rows();
  *result = GetKey(row_count_ - 1);
  return true;
}

size_t LeafPage::RowCount() const { return row_count_; }

void LeafPage::Split(page_id_t pid, Transaction& txn, Page* target) {
  // TODO(kumagi): Could be faster with bulk updates.
  for (size_t i = row_count_ / 2; i < row_count_; ++i) {
    target->Insert(txn, GetKey(i), GetValue(i));
  }
  for (size_t i = row_count_ / 2; i < row_count_; ++i) {
    Delete(pid, txn, GetKey(i));
  }
}

void LeafPage::DeFragment() {
  std::vector<std::string> payloads;
  payloads.reserve(row_count_);
  RowPointer* rows = Rows();
  for (size_t i = 0; i < row_count_; ++i) {
    payloads.emplace_back(Payload() + rows[i].offset, rows[i].size);
  }
  bin_size_t offset = sizeof(LeafPage);
  for (size_t i = 0; i < row_count_; ++i) {
    rows[i].offset = offset;
    memcpy(Payload() + offset, payloads[i].data(), payloads[i].size());
    offset += payloads[i].size();
  }
  free_ptr_ = offset;
}

size_t LeafPage::Find(std::string_view key) const {
  int left = -1, right = row_count_;
  while (1 < right - left) {
    const int cur = (left + right) / 2;
    std::string_view cur_key = GetKey(cur);
    if (cur_key < key)
      left = cur;
    else
      right = cur;
  }
  return right;
}

void LeafPage::Dump(std::ostream& o, int indent) const {
  o << "Rows: " << row_count_ << " Prev: " << prev_pid_
    << " Next: " << next_pid_ << " FreeSize: " << free_size_
    << " FreePtr:" << free_ptr_;
  std::string_view out;
  for (size_t i = 0; i < row_count_; ++i) {
    o << "\n" << Indent(indent) << GetKey(i) << ": " << GetValue(i);
  }
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::LeafPage>::operator()(
    const ::tinylamb::LeafPage& r) const {
  uint64_t ret = 0;
  ret += std::hash<tinylamb::page_id_t>()(r.prev_pid_);
  ret += std::hash<tinylamb::page_id_t>()(r.next_pid_);
  ret += std::hash<tinylamb::slot_t>()(r.row_count_);
  ret += std::hash<tinylamb::bin_size_t>()(r.free_ptr_);
  ret += std::hash<tinylamb::bin_size_t>()(r.free_size_);
  for (int i = 0; i < r.row_count_; ++i) {
    ret += std::hash<std::string_view>()(r.GetKey(i));
    ret += std::hash<std::string_view>()(r.GetValue(i));
  }
  return ret;
}
