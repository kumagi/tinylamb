//
// Created by kumagi on 2022/01/15.
//

#include "page/leaf_page.hpp"

#include <cstring>
#include <stdexcept>

#include "transaction/transaction.hpp"

namespace {

size_t Serialize(char* pos, std::string_view bin) {
  const uint16_t size = bin.size();
  memcpy(pos, &size, sizeof(size));
  memcpy(pos + sizeof(size), bin.data(), bin.size());
  return sizeof(size) + bin.size();
}

std::string_view Deserialize(const char* pos) {
  uint16_t key_length = *reinterpret_cast<const uint16_t*>(pos);
  return {pos + sizeof(uint16_t), key_length};
}

}  // namespace

namespace tinylamb {

LeafPage::RowPointer* LeafPage::Rows() {
  return reinterpret_cast<RowPointer*>(Payload() + kPageBodySize -
                                       row_count_ * sizeof(RowPointer));
}

std::string_view LeafPage::GetKey(const LeafPage::RowPointer& ptr) {
  return Deserialize(Payload() + ptr.offset);
}

std::string_view LeafPage::GetValue(const LeafPage::RowPointer& ptr) {
  std::string_view key = Deserialize(Payload() + ptr.offset);
  return Deserialize(Payload() + ptr.offset + sizeof(uint16_t) + key.size());
}

bool LeafPage::Insert(page_id_t page_id, Transaction& txn, std::string_view key,
                      std::string_view value) {
  const uint16_t physical_size =
      sizeof(uint16_t) + key.size() + sizeof(uint16_t) + value.size();

  if (free_size_ < physical_size + sizeof(RowPointer)) {
    return false;
  }

  // TODO(kumagi): Do binary search to find the data faster.
  RowPointer* rows = Rows();
  for (size_t i = 0; i < row_count_; ++i) {
    std::string_view cur_key = GetKey(rows[i]);
    if (cur_key == key) {
      return false;  // Already exists.
    }
  }

  if (kPageSize - sizeof(RowPointer) * (row_count_ + 1) <
      free_ptr_ + physical_size) {
    DeFragment();
  }

  InsertImpl(key, value);
  txn.InsertLog(page_id, key, value);
  return true;
}

void LeafPage::InsertImpl(std::string_view key, std::string_view value) {
  const uint16_t physical_size =
      sizeof(uint16_t) + key.size() + sizeof(uint16_t) + value.size();
  free_size_ -= physical_size + sizeof(RowPointer);

  row_count_++;
  RowPointer* rows = Rows();
  rows[0].offset = free_ptr_;
  rows[0].size = physical_size;

  free_ptr_ += Serialize(Payload() + free_ptr_, key);
  free_ptr_ += Serialize(Payload() + free_ptr_, value);
}

bool LeafPage::Update(page_id_t page_id, Transaction& txn, std::string_view key,
                      std::string_view value) {
  const int physical_size =
      sizeof(uint16_t) + key.size() + sizeof(uint16_t) + value.size();
  // TODO(kumagi): Do binary search to find the data faster.
  std::string_view existing_value;
  bool exists = Read(page_id, txn, key, &existing_value);
  if (!exists) {
    return false;
  }

  const int size_diff = physical_size - existing_value.size();
  if (free_size_ < size_diff) {
    // No enough space.
    return false;
  }
  if (kPageBodySize - sizeof(RowPointer) * row_count_ <
      free_ptr_ + physical_size) {
    DeFragment();
  }
  txn.UpdateLog(page_id, key, existing_value, value);
  UpdateImpl(key, value);
  return true;
}

void LeafPage::UpdateImpl(std::string_view key, std::string_view redo) {
  const int physical_size =
      sizeof(uint16_t) + key.size() + sizeof(uint16_t) + redo.size();
  RowPointer* rows = Rows();
  for (size_t i = 0; i < row_count_; ++i) {
    std::string_view cur_key = GetKey(rows[i]);
    if (cur_key == key) {
      rows[i].offset = free_ptr_;
      rows[i].size = physical_size;
      free_ptr_ += Serialize(Payload() + free_ptr_, key);
      free_ptr_ += Serialize(Payload() + free_ptr_, redo);
      return;
    }
  }
}

bool LeafPage::Delete(page_id_t page_id, Transaction& txn,
                      std::string_view key) {
  std::string_view existing_value;
  bool exists = Read(page_id, txn, key, &existing_value);
  if (!exists) {
    return false;
  }

  txn.DeleteLog(page_id, key, existing_value);
  DeleteImpl(key);
  return true;
}

void LeafPage::DeleteImpl(std::string_view key) {
  RowPointer* rows = Rows();
  for (size_t i = 0; i < row_count_; ++i) {
    std::string_view cur_key = GetKey(rows[i]);
    if (cur_key == key) {
      free_size_ += rows[i].size + sizeof(RowPointer);
      memmove(&rows[1], &rows[0], sizeof(RowPointer) * i);

      --row_count_;
      return;
    }
  }
}

bool LeafPage::Read(page_id_t page_id, Transaction& txn, std::string_view key,
                    std::string_view* result) {
  *result = std::string_view();
  // TODO(kumagi): Do binary search to find the data faster.
  RowPointer* rows = Rows();
  for (size_t i = 0; i < row_count_; ++i) {
    std::string_view cur_key = GetKey(rows[i]);
    if (cur_key == key) {
      *result = GetValue(rows[i]);
      return true;
    }
  }
  return false;
}

void LeafPage::DeFragment() {
  std::vector<std::string> payloads;
  payloads.reserve(row_count_);
  RowPointer* rows = Rows();
  for (size_t i = 0; i < row_count_; ++i) {
    payloads.emplace_back(Payload() + rows[i].offset, rows[i].size);
  }
  uint16_t offset = sizeof(LeafPage);
  for (size_t i = 0; i < row_count_; ++i) {
    rows[i].offset = offset;
    memcpy(Payload() + offset, payloads[i].data(), payloads[i].size());
    offset += payloads[i].size();
  }
  free_ptr_ = offset;
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::LeafPage>::operator()(
    const ::tinylamb::LeafPage& r) const {
  return 0;
}
