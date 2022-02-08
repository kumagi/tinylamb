//
// Created by kumagi on 2022/01/23.
//

#include "internal_page.hpp"

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

InternalPage::RowPointer* InternalPage::Rows() {
  return reinterpret_cast<RowPointer*>(Payload() + kPageBodySize -
                                       row_count_ * sizeof(RowPointer));
}

const InternalPage::RowPointer* InternalPage::Rows() const {
  return reinterpret_cast<const RowPointer*>(Payload() + kPageBodySize -
                                             row_count_ * sizeof(RowPointer));
}

size_t InternalPage::RowCount() const { return row_count_; }

void InternalPage::SetLowestValue(page_id_t pid, Transaction& txn,
                                  page_id_t value) {
  SetLowestValueImpl(value);
  txn.SetLowestLog(pid, value);
}

Status InternalPage::Insert(page_id_t pid, Transaction& txn,
                            std::string_view key, page_id_t value) {
  const bin_size_t physical_size = SerializeSize(key) + sizeof(page_id_t);
  size_t pos = SearchToInsert(key);
  if (free_size_ < physical_size + sizeof(RowPointer)) return Status::kNoSpace;
  if (pos != row_count_ && GetKey(pos) == key) return Status::kDuplicates;

  InsertImpl(key, value);
  txn.InsertInternalLog(pid, key, value);
  return Status::kSuccess;
}

void InternalPage::InsertImpl(std::string_view key, page_id_t pid) {
  const bin_size_t offset = free_ptr_;
  const bin_size_t physical_size =
      sizeof(bin_size_t) + key.size() + sizeof(page_id_t);
  free_size_ -= physical_size + sizeof(RowPointer);

  if (kPageSize - sizeof(RowPointer) * (row_count_ + 1) <
      free_ptr_ + physical_size) {
    DeFragment();
  }
  free_ptr_ += SerializeStringView(Payload() + free_ptr_, key);
  free_ptr_ += SerializePID(Payload() + free_ptr_, pid);
  bin_size_t insert = SearchToInsert(key);
  RowPointer* rows = Rows();
  memmove(rows - 1, rows, insert * sizeof(RowPointer));
  rows[insert - 1].offset = offset;
  rows[insert - 1].size = free_ptr_ - offset;
  ++row_count_;
}

Status InternalPage::Update(page_id_t pid, Transaction& txn,
                            std::string_view key, page_id_t value) {
  const bin_size_t physical_size =
      sizeof(bin_size_t) + key.size() + sizeof(bin_size_t) + sizeof(page_id_t);
  const size_t pos = Search(key);
  if (row_count_ < pos || GetKey(pos) != key) return Status::kNotExists;
  if (free_size_ < physical_size - GetKey(pos).size()) {
    return Status::kNoSpace;
  }

  txn.UpdateInternalLog(pid, key, GetValue(pos), value);
  UpdateImpl(key, value);
  return Status::kSuccess;
}

void InternalPage::UpdateImpl(std::string_view key, page_id_t pid) {
  const bin_size_t pos = Search(key);
  const bin_size_t offset = free_ptr_;
  free_size_ += GetKey(pos).size() - key.size();
  free_ptr_ += SerializeStringView(Payload() + free_ptr_, key);
  free_ptr_ += SerializePID(Payload() + free_ptr_, pid);
  RowPointer* rows = Rows();
  rows[pos].offset = offset;
  rows[pos].size = free_ptr_ - offset;
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
  const bin_size_t pos = Search(key);
  assert(pos < row_count_);
  free_size_ += GetKey(pos).size() + sizeof(bin_size_t) + sizeof(RowPointer);
  RowPointer* rows = Rows();
  memmove(rows + 1, rows, sizeof(RowPointer) * pos);
  row_count_--;
}

Status InternalPage::GetPageForKey(Transaction& txn, std::string_view key,
                                   page_id_t* result) const {
  if (row_count_ == 0) return Status::kNotExists;
  if (key < GetKey(0)) {
    *result = lowest_page_;
    return Status::kSuccess;
  }
  bin_size_t slot = Search(key);
  *result = GetValue(slot);
  return Status::kSuccess;
}

Status InternalPage::LowestPage(Transaction& txn, page_id_t* result) const {
  *result = lowest_page_;
  return Status::kSuccess;
}

void InternalPage::SplitInto(page_id_t pid, Transaction& txn, Page* right,
                             std::string_view* middle) {
  const int original_row_count = row_count_;
  int mid = row_count_ / 2;
  assert(0 < mid);
  *middle = GetKey(mid);
  right->SetLowestValue(txn, GetValue(mid));
  for (int i = mid + 1; i < row_count_; ++i) {
    right->Insert(txn, GetKey(i), GetValue(i));
  }
  Page* this_page = GET_PAGE_PTR(this);
  for (int i = mid; i < original_row_count; ++i) {
    this_page->Delete(txn, GetKey(mid));
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
  const RowPointer* pos =
      reinterpret_cast<const RowPointer*>(Payload() + kPageBodySize -
                                          row_count_ * sizeof(RowPointer)) +
      idx;
  std::string_view key;
  DeserializeStringView(Payload() + pos->offset, &key);
  return key;
}

page_id_t InternalPage::GetValue(size_t idx) const {
  const RowPointer* pos =
      reinterpret_cast<const RowPointer*>(Payload() + kPageBodySize -
                                          row_count_ * sizeof(RowPointer)) +
      idx;
  std::string_view key = GetKey(idx);
  page_id_t result;
  memcpy(&result, Payload() + pos->offset + sizeof(bin_size_t) + key.size(),
         sizeof(result));
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
