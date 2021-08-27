#include "row_page.hpp"

#include "recovery/log_record.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

bool RowPage::Read(Transaction& txn, const RowPosition& pos, Row& dst) {
  if (!txn.AddReadSet(pos) ||     // Failed by read conflict.
      pos.page_id != PageId() ||  // Invalid page ID specified.
      row_count_ <= pos.slot) {   // Specified slot is out of array.
    return false;
  }
  dst = GetRow(pos.slot);
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
bool RowPage::Insert(Transaction& txn, const Row& record, RowPosition& dst) {
  if (free_size_ + sizeof(RowPointer) < record.Size()) {
    // There is no enough space.
    return false;
  }
  if (PageHead() + free_ptr_ - record.Size() <=
      reinterpret_cast<char*>(&data_[row_count_ + 1])) {
    DeFragment();
  }
  dst = RowPosition(PageId(), row_count_);
  if (!txn.AddWriteSet(dst)) {
    // Lock conflict.
    return false;
  }
  dst.slot = InsertRow(record.data);
  dst.page_id = PageId();
  txn.InsertLog(dst, record.data);
  last_lsn = txn.PrevLSN();
  return true;
}

uint16_t RowPage::InsertRow(std::string_view new_row) {
  assert(new_row.size() <= std::numeric_limits<uint16_t>::max());
  free_size_ -= new_row.size() + sizeof(RowPointer);
  free_ptr_ -= new_row.size();
  data_[row_count_].offset = free_ptr_;
  data_[row_count_].size = new_row.size();
  memcpy(PageHead() + free_ptr_, new_row.data(), new_row.size());
  return row_count_++;
}

bool RowPage::Update(Transaction& txn, const RowPosition& pos,
                     const Row& new_row) {
  assert(pos.page_id == PageId());
  assert(pos.slot <= row_count_);
  Row prev_row = GetRow(pos.slot);
  if (prev_row.Size() < new_row.data.size() &&
      free_size_ < new_row.data.size() - prev_row.data.size()) {
    // No enough space in this page.
    return false;
  }
  if (!txn.AddWriteSet(pos)) {
    return false;
  }
  txn.UpdateLog(pos, new_row.data, prev_row.data);
  UpdateRow(pos.slot, new_row.data);
  last_lsn = txn.PrevLSN();
  return true;
}

void RowPage::UpdateRow(int slot, std::string_view new_row) {
  Row prev_row = GetRow(slot);
  free_size_ -= prev_row.Size();
  free_size_ += new_row.size();
  assert(0 <= free_size_);
  data_[slot].size = new_row.size();
  if (new_row.size() <= prev_row.Size()) {
    // There is enough space, just overwrite.
  } else {
    // Use new field.
    if (PageHead() + free_ptr_ - new_row.size() <=
        reinterpret_cast<char*>(&data_[row_count_])) {
      DeFragment();
    }
    free_ptr_ -= new_row.size();
    data_[slot].offset = free_ptr_;
  }
  memcpy(PageHead() + data_[slot].offset, new_row.data(), new_row.size());
}

bool RowPage::Delete(Transaction& txn, const RowPosition& pos) {
  assert(pos.page_id == PageId());
  assert(pos.slot <= row_count_);
  DeleteRow(pos.slot);
  txn.DeleteLog(pos, GetRow(pos.slot).data);
  return true;
}

void RowPage::DeleteRow(int slot) {
  row_count_--;
  free_size_ += data_[slot].size;
  std::swap(data_[slot], data_[row_count_]);
  data_[row_count_].size = data_[row_count_].offset = 0;
}

size_t RowPage::RowCount() const { return row_count_; }

void DeFragment() {
  LOG(FATAL) << "de-fragment not implemented";
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::RowPage>::operator()(
    const tinylamb::RowPage& r) const {
  uint64_t ret = 0;
  ret += std::hash<uint64_t>()(r.prev_page_id_);
  ret += std::hash<uint64_t>()(r.next_page_id_);
  ret += std::hash<int16_t>()(r.row_count_);
  ret += std::hash<int16_t>()(r.free_ptr_);
  ret += std::hash<int16_t>()(r.free_size_);
  for (int i = 0; i < r.row_count_; ++i) {
    ret += std::hash<tinylamb::Row>()(r.GetRow(i));
  }
  return ret;
}
