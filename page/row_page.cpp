#include "row_page.hpp"

#include "recovery/log_record.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

bool RowPage::Read(page_id_t page_id, Transaction& txn, uint16_t slot,
                   std::string_view* dst) const {
  if (!txn.AddReadSet(
          RowPosition(page_id, slot)) ||  // Failed by read conflict.
      row_count_ <= slot) {               // Specified slot is out of array.
    return false;
  }
  *dst = GetRow(slot);
  return true;
}

/*
 *           = before =
 * +-------------------------------+
 * | RowPointer(0, 0) |            |
 * +-------------------------------+
 * |                               |
 * +-------------------------------+
 *                                 ^ free_ptr_
 *           = after =
 * *-------------------------------+
 * | RowPointer(PosX, Size) |      |
 * +-------------------------------+
 * |                      | Record |
 * +-------------------------------+
 *                        ^ PosX == free_ptr_
 */
bool RowPage::Insert(page_id_t page_id, Transaction& txn,
                     std::string_view record, uint16_t* dst) {
  if (free_size_ + sizeof(RowPointer) < record.size()) {
    // There is no enough space.
    return false;
  }
  if (Payload() + free_ptr_ <=
      reinterpret_cast<char*>(&data_[row_count_ + 1]) + record.size()) {
    DeFragment();
  }
  assert(reinterpret_cast<char*>(&data_[row_count_ + 1]) + record.size() <
         Payload() + free_ptr_);
  *dst = row_count_;
  const RowPosition pos(page_id, *dst);
  if (!txn.AddWriteSet(pos)) {
    // Lock conflict.
    return false;
  }
  InsertRow(record);
  txn.InsertLog(page_id, *dst, record);
  return true;
}

uint16_t RowPage::InsertRow(std::string_view new_row) {
  assert(new_row.size() <= std::numeric_limits<uint16_t>::max());
  free_size_ -= new_row.size() + sizeof(RowPointer);
  free_ptr_ -= new_row.size();
  data_[row_count_].offset = free_ptr_;
  data_[row_count_].size = new_row.size();
  memcpy(Payload() + free_ptr_, new_row.data(), new_row.size());
  return row_count_++;
}

bool RowPage::Update(page_id_t page_id, Transaction& txn, uint16_t slot,
                     std::string_view record) {
  assert(slot <= row_count_);
  std::string_view prev_row = GetRow(slot);
  if (prev_row.size() < record.size() &&
      free_size_ < record.size() - prev_row.size()) {
    // No enough space in this page.
    return false;
  }
  RowPosition pos(page_id, slot);
  if (!txn.AddWriteSet(pos)) {
    // Write lock conflict.
    return false;
  }
  txn.UpdateLog(page_id, slot, prev_row, record);
  UpdateRow(pos.slot, record);
  return true;
}

void RowPage::UpdateRow(uint16_t slot, std::string_view record) {
  std::string_view prev_row = GetRow(slot);
  free_size_ -= prev_row.size();
  free_size_ += record.size();
  assert(0 <= free_size_);
  data_[slot].size = record.size();
  if (record.size() <= prev_row.size()) {
    // There is enough space, just overwrite.
  } else {
    // Use new field.
    if (Payload() + free_ptr_ - record.size() <=
        reinterpret_cast<char*>(&data_[row_count_])) {
      DeFragment();
    }
    free_ptr_ -= record.size();
    data_[slot].offset = free_ptr_;
  }
  memcpy(Payload() + data_[slot].offset, record.data(), record.size());
}

bool RowPage::Delete(page_id_t page_id, Transaction& txn, uint16_t slot) {
  assert(slot <= row_count_);
  txn.DeleteLog(page_id, slot, GetRow(slot));
  DeleteRow(slot);
  return true;
}

void RowPage::DeleteRow(uint16_t slot) {
  row_count_--;
  free_size_ += data_[slot].size;
  std::swap(data_[slot], data_[row_count_]);
  data_[row_count_].size = data_[row_count_].offset = 0;
}

size_t RowPage::RowCount() const { return row_count_; }

void RowPage::DeFragment() {
  // FIXME: replace it with inplace one?
  std::vector<std::string> tmp_buffer;
  tmp_buffer.reserve(row_count_);
  for (size_t i = 0; i < row_count_; ++i) {
    std::string_view r = GetRow(i);
    tmp_buffer.emplace_back(r);
  }
  free_ptr_ = kPageBodySize;
  for (size_t i = 0; i < row_count_; ++i) {
    free_ptr_ -= tmp_buffer[i].size();
    memcpy(Payload() + free_ptr_, tmp_buffer[i].data(), tmp_buffer[i].size());
    data_[i].offset = free_ptr_;
  }
}

void RowPage::Dump(std::ostream& o, int indent) const {
  o << "Rows: " << row_count_ << " Prev: " << prev_page_id_
    << " Next: " << next_page_id_ << " FreeSize: " << free_size_
    << " FreePtr:" << free_ptr_;
  for (size_t i = 0; i < row_count_; ++i) {
    o << "\n" << Indent(indent) << i << ": " << GetRow(i);
  }
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::RowPage>::operator()(
    const tinylamb::RowPage& r) const {
  uint64_t ret = 0;
  ret += std::hash<tinylamb::page_id_t>()(r.prev_page_id_);
  ret += std::hash<tinylamb::page_id_t>()(r.next_page_id_);
  ret += std::hash<int16_t>()(r.row_count_);
  ret += std::hash<int16_t>()(r.free_ptr_);
  ret += std::hash<int16_t>()(r.free_size_);
  for (int i = 0; i < r.row_count_; ++i) {
    ret += std::hash<std::string_view>()(r.GetRow(i));
  }
  return ret;
}
