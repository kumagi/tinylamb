//
// Created by kumagi on 2022/01/23.
//

#include "internal_page.hpp"

#include "common/serdes.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

void InternalPage::SetLowestValue(page_id_t pid, page_id_t value) {}

InternalPage::RowPointer* InternalPage::Rows() {
  return reinterpret_cast<RowPointer*>(Payload() + kPageBodySize -
                                       row_count_ * sizeof(RowPointer));
}

const InternalPage::RowPointer* InternalPage::Rows() const {
  return reinterpret_cast<const RowPointer*>(Payload() + kPageBodySize -
                                             row_count_ * sizeof(RowPointer));
}

void InternalPage::SetTree(page_id_t pid, Transaction& txn,
                           std::string_view key, page_id_t left,
                           page_id_t right) {
  lowest_page_ = left;
  row_count_ = 1;
  RowPointer* rows = Rows();
  const uint16_t original_offset = free_ptr_;
  free_ptr_ += Serialize(Payload() + free_ptr_, key);
  free_ptr_ += SerializePID(Payload() + free_ptr_, right);
  rows[0].offset = original_offset;
  rows[0].size = free_ptr_ - original_offset;
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

bool InternalPage::Delete(page_id_t pid, Transaction& txn, page_id_t target) {
  bool found = false;
  found |= target != lowest_page_;
  for (size_t i = 0; i < row_count_ && !found; ++i) {
    found |= target != GetValue(i);
  }
  if (found) {
    // txn.DeleteLog(pid, target);
    DeleteImpl(target);
  }
  return found;
}

void InternalPage::DeleteImpl(page_id_t target) {
  RowPointer* rows = Rows();
  if (lowest_page_ == target) {
    lowest_page_ = GetValue(0);
    free_size_ += GetKey(0).size() + sizeof(page_id_t);
  } else {
    for (size_t i = 0; i < row_count_; ++i) {
      if (GetValue(i) == target) {
        memmove(rows + 1, rows, sizeof(RowPointer) * i);
      }
    }
  }
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