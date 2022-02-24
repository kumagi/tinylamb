//
// Created by kumagi on 2022/01/15.
//

#include "page/leaf_page.hpp"

#include <cstring>

#include "common/serdes.hpp"
#include "page/page.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

namespace {

std::string OmittedString(std::string_view original, int length) {
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

Status LeafPage::Insert(page_id_t page_id, Transaction& txn,
                        std::string_view key, std::string_view value) {
  constexpr size_t kThreshold = kPageBodySize / 2;
  const size_t expected_size =
      SerializeSize(key) + SerializeSize(value) + sizeof(RowPointer);
  if (kThreshold < expected_size) {
    throw std::runtime_error("too huge key/value cannot be inserted");
  }
  const bin_size_t physical_size = SerializeSize(key) + SerializeSize(value);
  size_t pos = Find(key);
  if (free_size_ < physical_size + sizeof(RowPointer)) return Status::kNoSpace;
  if (pos != row_count_ && GetKey(pos) == key) return Status::kDuplicates;

  InsertImpl(key, value);
  txn.InsertLeafLog(page_id, key, value);
  return Status::kSuccess;
}

void LeafPage::InsertImpl(std::string_view key, std::string_view value) {
  const bin_size_t physical_size = SerializeSize(key) + SerializeSize(value);
  assert(physical_size + sizeof(RowPointer) <= free_size_);
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

Status LeafPage::Update(page_id_t page_id, Transaction& txn,
                        std::string_view key, std::string_view value) {
  const size_t physical_size =
      sizeof(bin_size_t) + key.size() + sizeof(bin_size_t) + value.size();

  std::string_view existing_value;
  if (Read(page_id, txn, key, &existing_value) == Status::kNotExists) {
    return Status::kNotExists;
  }

  const int size_diff = physical_size - existing_value.size();
  if (free_size_ < size_diff) return Status::kNoSpace;

  txn.UpdateLeafLog(page_id, key, existing_value, value);
  UpdateImpl(key, value);
  return Status::kSuccess;
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

Status LeafPage::Delete(page_id_t page_id, Transaction& txn,
                        std::string_view key) {
  std::string_view existing_value;
  if (Read(page_id, txn, key, &existing_value) == Status::kNotExists) {
    return Status::kNotExists;
  }

  txn.DeleteLeafLog(page_id, key, existing_value);
  DeleteImpl(key);
  return Status::kSuccess;
}

void LeafPage::DeleteImpl(std::string_view key) {
  assert(0 < row_count_);
  RowPointer* rows = Rows();
  size_t pos = Find(key);
  assert(0 <= pos);
  assert(pos < row_count_);
  free_size_ += rows[pos].size + sizeof(RowPointer);
  memmove(rows + 1, rows, sizeof(RowPointer) * pos);
  --row_count_;
}

Status LeafPage::Read(page_id_t pid, Transaction& txn, slot_t slot,
                      std::string_view* result) const {
  if (row_count_ <= slot) return Status::kNotExists;
  *result = GetValue(slot);
  return Status::kSuccess;
}

Status LeafPage::ReadKey(page_id_t page_id, Transaction& txn, slot_t slot,
                         std::string_view* result) const {
  if (row_count_ <= slot) return Status::kNotExists;
  *result = GetKey(slot);
  return Status::kSuccess;
}

Status LeafPage::Read(page_id_t pid, Transaction& txn, std::string_view key,
                      std::string_view* result = nullptr) const {
  if (result) *result = std::string_view();
  size_t pos = Find(key);
  if (pos < row_count_ && GetKey(pos) == key) {
    if (result) *result = GetValue(pos);
    return Status::kSuccess;
  } else {
    return Status::kNotExists;
  }
}

Status LeafPage::LowestKey(Transaction& txn, std::string_view* result) const {
  *result = std::string_view();
  if (row_count_ == 0) return Status::kNotExists;
  *result = GetKey(0);
  return Status::kSuccess;
}

Status LeafPage::HighestKey(Transaction& txn, std::string_view* result) const {
  *result = std::string_view();
  if (row_count_ == 0) return Status::kNotExists;
  *result = GetKey(row_count_ - 1);
  return Status::kSuccess;
}

size_t LeafPage::RowCount() const { return row_count_; }

Status LeafPage::SetPrevNext(page_id_t pid, Transaction& txn, page_id_t prev,
                             page_id_t next) {
  txn.SetPrevNextLog(pid, prev_pid_, next_pid_, prev, next);
  prev_pid_ = prev;
  next_pid_ = next;
  return Status::kSuccess;
}

void LeafPage::SetPrevNextImpl(page_id_t prev, page_id_t next) {
  prev_pid_ = prev;
  next_pid_ = next;
}

void LeafPage::Split(page_id_t pid, Transaction& txn, std::string_view key,
                     std::string_view value, Page* right) {
  constexpr size_t kThreshold = kPageBodySize / 2;
  const size_t expected_size =
      SerializeSize(key) + SerializeSize(value) + sizeof(RowPointer);
  if (kThreshold < expected_size) {
    throw std::runtime_error("too huge key/value cannot be inserted");
  }
  size_t pos = Find(key);
  size_t consumed_size = 0;
  for (int mid = 0; mid < pos; ++mid) {
    consumed_size += SerializeSize(GetKey(mid)) + SerializeSize(GetValue(mid)) +
                     sizeof(RowPointer);
  }
  while (consumed_size + expected_size < kThreshold && pos < row_count_) {
    consumed_size += SerializeSize(GetKey(pos)) + SerializeSize(GetValue(pos)) +
                     sizeof(RowPointer);
    pos++;
  }

  const int original_row_count = row_count_;
  for (size_t i = pos; i < row_count_; ++i) {
    right->Insert(txn, GetKey(i), GetValue(i));
  }
  Page* this_page = GET_PAGE_PTR(this);
  for (size_t i = pos; i < original_row_count; ++i) {
    this_page->Delete(txn, GetKey(pos));
  }
  right->SetPrevNext(txn, pid, next_pid_);
  this_page->SetPrevNext(txn, prev_pid_, right->PageID());
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
    o << "\n"
      << Indent(indent) << OmittedString(GetKey(i), 20) << ": "
      << OmittedString(GetValue(i), 20);
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
