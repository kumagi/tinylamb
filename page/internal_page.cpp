//
// Created by kumagi on 2022/01/23.
//

#include "internal_page.hpp"

#include "common/serdes.hpp"
#include "page/page.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

InternalPage::RowPointer* InternalPage::Rows() {
  return reinterpret_cast<RowPointer*>(Payload() + kPageBodySize -
                                       row_count_ * sizeof(RowPointer));
}

const InternalPage::RowPointer* InternalPage::Rows() const {
  return reinterpret_cast<const RowPointer*>(Payload() + kPageBodySize -
                                             row_count_ * sizeof(RowPointer));
}

void InternalPage::SetLowestValue(page_id_t pid, Transaction& txn,
                                  page_id_t value) {
  lowest_page_ = value;
  // txn.SetLowestLog(pid, value);
}

bool InternalPage::Insert(page_id_t pid, Transaction& txn, std::string_view key,
                          page_id_t value) {
  const uint16_t physical_size =
      sizeof(uint16_t) + key.size() + sizeof(uint16_t) + sizeof(page_id_t);

  size_t pos = SearchToInsert(key);
  if (free_size_ < physical_size + sizeof(RowPointer) ||  // No space.
      (pos != row_count_ && GetKey(pos) == key)) {        // Already exists.
    return false;
  }

  InsertImpl(key, value);
  txn.InsertLog(pid, key, value);
  return true;
}

void InternalPage::InsertImpl(std::string_view key, page_id_t pid) {
  const uint16_t inserted_offset = free_ptr_;
  free_ptr_ += Serialize(Payload() + free_ptr_, key);
  free_ptr_ += SerializePID(Payload() + free_ptr_, pid);
  uint16_t insert = SearchToInsert(key);
  RowPointer* rows = Rows();
  memmove(rows - 1, rows, insert * sizeof(RowPointer));
  rows[insert - 1].offset = inserted_offset;
  rows[insert - 1].size = free_ptr_ - inserted_offset;
  ++row_count_;
}

bool InternalPage::Update(page_id_t pid, Transaction& txn, std::string_view key,
                          page_id_t value) {
  return true;
}

bool InternalPage::Delete(page_id_t pid, Transaction& txn,
                          std::string_view key) {
  const uint16_t pos = Search(key);
  if (GetKey(pos) != key) {
    return false;
  }
  uint16_t old_value = GetValue(pos);
  DeleteImpl(key);
  txn.DeleteLog(pid, key, old_value);
  return true;
}

void InternalPage::DeleteImpl(std::string_view key) {
  const uint16_t pos = Search(key);
  free_size_ += GetKey(pos).size() + sizeof(uint16_t) + sizeof(RowPointer);
  RowPointer* rows = Rows();
  memmove(rows + 1, rows, sizeof(RowPointer) * pos);
  row_count_--;
}

bool InternalPage::GetPageForKey(Transaction& txn, std::string_view key,
                                 page_id_t* result) {
  if (row_count_ == 0) return false;
  if (key < GetKey(0)) {
    *result = lowest_page_;
    return true;
  }
  uint16_t slot = Search(key);
  *result = GetValue(slot);
  return true;
}

void InternalPage::SplitInto(page_id_t pid, Transaction& txn, Page* right,
                             std::string_view* middle) {
  const int original_row_count = row_count_;
  int mid = row_count_ / 2;
  assert(1 < mid);
  *middle = GetKey(mid);
  right->SetLowestValue(txn, GetValue(mid));
  for (int i = mid + 1; i < row_count_; ++i) {
    right->Insert(txn, GetKey(i), GetValue(i));
  }
  for (int i = mid; i < original_row_count; ++i) {
    DeleteImpl(GetKey(mid));
  }
}

uint16_t InternalPage::SearchToInsert(std::string_view key) const {
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

uint16_t InternalPage::Search(std::string_view key) const {
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

std::string_view InternalPage::GetKey(size_t idx) const {
  const RowPointer* pos =
      reinterpret_cast<const RowPointer*>(Payload() + kPageBodySize -
                                          row_count_ * sizeof(RowPointer)) +
      idx;
  return Deserialize(Payload() + pos->offset);
}

const page_id_t& InternalPage::GetValue(size_t idx) const {
  const RowPointer* pos =
      reinterpret_cast<const RowPointer*>(Payload() + kPageBodySize -
                                          row_count_ * sizeof(RowPointer)) +
      idx;
  std::string_view key = GetKey(idx);
  return *reinterpret_cast<const page_id_t*>(Payload() + pos->offset +
                                             sizeof(uint16_t) + key.size());
}

page_id_t& InternalPage::GetValue(size_t idx) {
  const RowPointer* pos =
      reinterpret_cast<const RowPointer*>(Payload() + kPageBodySize -
                                          row_count_ * sizeof(RowPointer)) +
      idx;
  std::string_view key = GetKey(idx);
  return *reinterpret_cast<page_id_t*>(Payload() + pos->offset +
                                       sizeof(uint16_t) + key.size());
}

void InternalPage::Dump(std::ostream& o, int indent) const {
  o << "Rows: " << row_count_ << " FreeSize: " << free_size_
    << " FreePtr:" << free_ptr_;
  if (row_count_ == 0) return;
  o << "\n" << Indent(indent + 2) << lowest_page_;
  std::string_view out;
  for (size_t i = 0; i < row_count_; ++i) {
    o << "\n"
      << Indent(indent) << GetKey(i) << "\n"
      << Indent(indent + 2) << GetValue(i);
  }
}

}  // namespace tinylamb