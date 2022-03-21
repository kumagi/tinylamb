//
// Created by kumagi on 2022/01/23.
//

#include "internal_page.hpp"

#include "common/serdes.hpp"
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
  } else {
    return std::string(original);
  }
}

}  // anonymous namespace

slot_t InternalPage::RowCount() const { return row_count_; }

void InternalPage::SetLowestValue(page_id_t pid, Transaction& txn,
                                  page_id_t value) {
  SetLowestValueImpl(value);
  txn.SetLowestLog(pid, value);
}

Status InternalPage::Insert(page_id_t pid, Transaction& txn,
                            std::string_view key, page_id_t value) {
  const bin_size_t physical_size = SerializeSize(key) + sizeof(value);
  if (free_size_ < physical_size + sizeof(RowPointer)) return Status::kNoSpace;
  size_t pos = SearchToInsert(key);
  if (pos != row_count_ && GetKey(pos) == key) return Status::kDuplicates;
  InsertImpl(key, value);
  txn.InsertInternalLog(pid, key, value);
  return Status::kSuccess;
}

void InternalPage::InsertImpl(std::string_view key, page_id_t pid) {
  const bin_size_t physical_size = SerializeSize(key) + sizeof(page_id_t);
  free_size_ -= physical_size + sizeof(RowPointer);
  if (free_ptr_ < sizeof(RowPointer) * (row_count_ + 1) + physical_size) {
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

Status InternalPage::Update(page_id_t pid, Transaction& txn,
                            std::string_view key, page_id_t value) {
  const size_t pos = Search(key);
  if (row_count_ < pos || GetKey(pos) != key) return Status::kNotExists;
  txn.UpdateInternalLog(pid, key, GetValue(pos), value);
  UpdateImpl(key, value);
  return Status::kSuccess;
}

void InternalPage::UpdateImpl(std::string_view key, page_id_t pid) {
  memcpy(Payload() + rows_[Search(key)].offset, &pid, sizeof(pid));
}

Status InternalPage::Delete(page_id_t pid, Transaction& txn,
                            std::string_view key) {
  const bin_size_t pos = Search(key);
  if (GetKey(pos) != key) return Status::kNotExists;
  bin_size_t old_value = GetValue(pos);
  DeleteImpl(key);
  txn.DeleteInternalLog(pid, key, old_value);
  return Status::kSuccess;
}

void InternalPage::DeleteImpl(std::string_view key) {
  assert(0 < row_count_);
  size_t pos = Search(key);
  assert(pos < row_count_);
  free_size_ += GetKey(pos).length() + sizeof(page_id_t);
  memmove(rows_ + pos, rows_ + pos + 1,
          sizeof(RowPointer) * (row_count_ - pos));
  --row_count_;
}

Status InternalPage::GetPageForKey(Transaction& txn, std::string_view key,
                                   page_id_t* result) const {
  assert(0 < row_count_);
  if (row_count_ == 0) return Status::kNotExists;
  if (key < GetKey(0)) {
    *result = lowest_page_;
  } else {
    *result = GetValue(Search(key));
  }
  return Status::kSuccess;
}

void InternalPage::SplitInto(page_id_t pid, Transaction& txn,
                             std::string_view new_key, Page* right,
                             std::string* middle) {
  constexpr size_t kThreshold = kPageBodySize / 2;
  const size_t expected_size =
      SerializeSize(new_key) + sizeof(page_id_t) + sizeof(RowPointer);
  size_t pos = SearchToInsert(new_key);
  size_t consumed_size = 0;
  for (size_t mid = 0; mid < pos; ++mid) {
    consumed_size +=
        SerializeSize(GetKey(mid)) + sizeof(page_id_t) + sizeof(RowPointer);
  }
  while (consumed_size + expected_size < kThreshold && pos < row_count_ - 1) {
    consumed_size +=
        SerializeSize(GetKey(pos)) + sizeof(page_id_t) + sizeof(RowPointer);
    pos++;
  }
  if (pos == row_count_) --pos;
  const size_t original_row_count = row_count_;
  *middle = GetKey(pos);
  right->SetLowestValue(txn, GetValue(pos));
  for (size_t i = pos + 1; i < row_count_; ++i) {
    right->Insert(txn, GetKey(i), GetValue(i));
  }
  Page* this_page = GET_PAGE_PTR(this);
  for (size_t i = pos; i < original_row_count; ++i) {
    this_page->Delete(txn, GetKey(pos));
  }
}

bin_size_t InternalPage::SearchToInsert(std::string_view key) const {
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

bin_size_t InternalPage::Search(std::string_view key) const {
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
  std::string_view key;
  DeserializeStringView(Payload() + rows_[idx].offset + sizeof(page_id_t),
                        &key);
  return key;
}

page_id_t InternalPage::GetValue(size_t idx) const {
  page_id_t result;
  memcpy(&result, Payload() + rows_[idx].offset, sizeof(result));
  return result;
}

void InternalPage::Dump(std::ostream& o, int indent) const {
  o << "Rows: " << row_count_ << " FreeSize: " << free_size_
    << " FreePtr:" << free_ptr_;
  if (row_count_ == 0) return;
  o << "\n" << Indent(indent + 2) << lowest_page_;
  for (size_t i = 0; i < row_count_; ++i) {
    o << "\n"
      << Indent(indent) << OmittedString(GetKey(i), 20) << "\n"
      << Indent(indent + 2) << GetValue(i);
  }
}

void InternalPage::DeFragment() {
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

uint64_t std::hash<tinylamb::InternalPage>::operator()(
    const ::tinylamb::InternalPage& r) const {
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
