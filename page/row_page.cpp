#include "row_page.hpp"

#include "recovery/log_record.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

StatusOr<std::string_view> RowPage::Read(page_id_t page_id, Transaction& txn,
                                         slot_t slot) const {
  if (!txn.AddReadSet(RowPosition(page_id, slot))) {
    return Status::kConflicts;
  }
  if (row_max_ <= slot || rows_[slot].offset == 0) {
    return Status::kNotExists;
  }
  return GetRow(slot);
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
StatusOr<slot_t> RowPage::Insert(page_id_t page_id, Transaction& txn,
                                 std::string_view record) {
  if (free_size_ <= record.size() + sizeof(RowPointer)) return Status::kNoSpace;
  // Scan the first vacant slot.
  slot_t result = InsertRow(record);
  if (!txn.AddWriteSet(RowPosition(page_id, result))) {
    return Status::kConflicts;
  }
  txn.InsertLog(page_id, result, record);
  return result;
}

slot_t RowPage::InsertRow(std::string_view new_row) {
  assert(new_row.size() <= std::numeric_limits<slot_t>::max());
  if (Payload() + free_ptr_ <=
      reinterpret_cast<char*>(&rows_[row_count_ + 1]) + new_row.size()) {
    DeFragment();
  }
  slot_t slot = 0;
  for (; slot <= row_max_; ++slot) {
    if (rows_[slot].offset == 0) break;
  }

  assert(reinterpret_cast<char*>(&rows_[slot + 1]) + new_row.size() <
         Payload() + free_ptr_);
  free_size_ -= new_row.size() + sizeof(RowPointer);
  free_ptr_ -= new_row.size();
  rows_[slot].offset = free_ptr_;
  rows_[slot].size = new_row.size();
  memcpy(Payload() + free_ptr_, new_row.data(), new_row.size());
  row_count_++;
  row_max_ = std::max(row_max_, row_count_);
  return slot;
}

Status RowPage::Update(page_id_t page_id, Transaction& txn, slot_t slot,
                       std::string_view record) {
  if (row_max_ <= slot || rows_[slot].offset == 0) return Status::kNotExists;
  std::string_view prev_row = GetRow(slot);
  if (prev_row.size() < record.size() &&
      free_size_ < record.size() - prev_row.size()) {
    return Status::kNoSpace;
  }
  RowPosition pos(page_id, slot);
  if (!txn.AddWriteSet(pos)) {
    LOG(ERROR) << "cannot add write-set";
    return Status::kConflicts;
  }
  txn.UpdateLog(page_id, slot, prev_row, record);
  UpdateRow(pos.slot, record);
  return Status::kSuccess;
}

void RowPage::UpdateRow(slot_t slot, std::string_view record) {
  std::string_view prev_row = GetRow(slot);
  if (record.size() <= prev_row.size()) {
    // There is already enough space, just overwrite.
  } else {
    // Use new field.
    if (Payload() + free_ptr_ - record.size() <=
        reinterpret_cast<char*>(&rows_[row_count_])) {
      rows_[slot].size = 0;
      DeFragment();
    }
    free_ptr_ -= record.size();
    rows_[slot].offset = free_ptr_;
  }
  free_size_ += prev_row.size();
  free_size_ -= record.size();
  rows_[slot].size = record.size();
  memcpy(Payload() + rows_[slot].offset, record.data(), record.size());
}

Status RowPage::Delete(page_id_t page_id, Transaction& txn, slot_t slot) {
  if (row_max_ <= slot || rows_[slot].offset == 0) return Status::kNotExists;
  RowPosition pos(page_id, slot);
  if (!txn.AddWriteSet(pos)) return Status::kConflicts;
  txn.DeleteLog(page_id, slot, GetRow(slot));
  DeleteRow(slot);
  return Status::kSuccess;
}

void RowPage::DeleteRow(slot_t slot) {
  row_count_--;
  free_size_ += rows_[slot].size;
  rows_[slot].size = rows_[slot].offset = 0;
  while (slot == row_max_ - 1 && rows_[slot].offset == 0) {
    row_max_--;
  }
}

slot_t RowPage::RowCount() const { return row_count_; }

void RowPage::DeFragment() {
  // FIXME: replace it with inplace one?
  std::vector<std::string> tmp_buffer;
  tmp_buffer.reserve(row_max_);
  for (size_t i = 0; i < row_max_; ++i) {
    if (rows_[i].offset == 0) {
      tmp_buffer.emplace_back("");
    } else {
      tmp_buffer.emplace_back(GetRow(i));
    }
  }
  free_ptr_ = kPageBodySize;
  for (size_t i = 0; i < row_max_; ++i) {
    if (rows_[i].offset == 0) continue;

    free_ptr_ -= tmp_buffer[i].size();
    rows_[i].offset = free_ptr_;
    memcpy(Payload() + free_ptr_, tmp_buffer[i].data(), tmp_buffer[i].size());
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
  ret += std::hash<tinylamb::slot_t>()(r.row_max_);
  ret += std::hash<tinylamb::slot_t>()(r.row_count_);
  ret += std::hash<tinylamb::bin_size_t>()(r.free_ptr_);
  ret += std::hash<tinylamb::bin_size_t>()(r.free_size_);
  for (int i = 0; i < r.row_count_; ++i) {
    ret += std::hash<std::string_view>()(r.GetRow(i));
  }
  return ret;
}
