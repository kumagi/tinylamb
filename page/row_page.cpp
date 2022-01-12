#include "row_page.hpp"

#include "recovery/log_record.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

bool RowPage::Read(page_id_t page_id, Transaction& txn, const RowPosition& pos,
                   std::string_view* dst) {
  if (!txn.AddReadSet(pos) ||    // Failed by read conflict.
      pos.page_id != page_id ||  // Invalid page ID specified.
      row_count_ <= pos.slot) {  // Specified slot is out of array.
    return false;
  }
  *dst = GetRow(pos.slot);
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
bool RowPage::Insert(page_id_t page_id, Transaction& txn, const Row& record,
                     RowPosition& dst) {
  if (free_size_ + sizeof(RowPointer) < record.Size()) {
    // There is no enough space.
    return false;
  }
  if (Payload() + free_ptr_ <=
      reinterpret_cast<char*>(&data_[row_count_ + 1]) + record.Size()) {
    DeFragment();
  }
  assert(reinterpret_cast<char*>(&data_[row_count_ + 1]) + record.Size() <
         Payload() + free_ptr_);
  dst = RowPosition(page_id, row_count_);
  if (!txn.AddWriteSet(dst)) {
    // Lock conflict.
    return false;
  }
  dst.slot = InsertRow(record.data);
  dst.page_id = page_id;
  txn.InsertLog(dst, record.data);
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

bool RowPage::Update(page_id_t page_id, Transaction& txn, const RowPosition& pos,
                     const Row& new_row) {
  assert(pos.page_id == page_id);
  assert(pos.slot <= row_count_);
  std::string_view prev_row = GetRow(pos.slot);
  if (prev_row.size() < new_row.data.size() &&
      free_size_ < new_row.data.size() - prev_row.size()) {
    // No enough space in this page.
    return false;
  }
  if (!txn.AddWriteSet(pos)) {
    // Write lock conflict.
    return false;
  }
  txn.UpdateLog(pos, prev_row, new_row.data);
  UpdateRow(pos.slot, new_row.data);
  return true;
}

void RowPage::UpdateRow(int slot, std::string_view new_row) {
  std::string_view prev_row = GetRow(slot);
  free_size_ -= prev_row.size();
  free_size_ += new_row.size();
  assert(0 <= free_size_);
  data_[slot].size = new_row.size();
  if (new_row.size() <= prev_row.size()) {
    // There is enough space, just overwrite.
  } else {
    // Use new field.
    if (Payload() + free_ptr_ - new_row.size() <=
        reinterpret_cast<char*>(&data_[row_count_])) {
      DeFragment();
    }
    free_ptr_ -= new_row.size();
    data_[slot].offset = free_ptr_;
  }
  memcpy(Payload() + data_[slot].offset, new_row.data(), new_row.size());
}

bool RowPage::Delete(page_id_t page_id, Transaction& txn,
                     const RowPosition& pos) {
  assert(pos.page_id == page_id);
  assert(pos.slot <= row_count_);
  txn.DeleteLog(pos, GetRow(pos.slot));
  DeleteRow(pos.slot);
  return true;
}

void RowPage::DeleteRow(int slot) {
  row_count_--;
  free_size_ += data_[slot].size;
  std::swap(data_[slot], data_[row_count_]);
  data_[row_count_].size = data_[row_count_].offset = 0;
}

size_t RowPage::RowCount() const { return row_count_; }

void RowPage::DeFragment() {
  // FIXME: replace it with O(1) memory consumption?
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
