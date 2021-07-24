#include "row_page.hpp"

#include "recovery/log_record.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

bool RowPage::Read(Transaction& txn, const RowPosition& pos,
                   const Schema& schema, Row& dst) {
  if (pos.page_id != PageId() || row_count_ <= pos.slot) {
    return false;
  }
  char* physical_pos = data_ + schema.FixedRowSize() * pos.slot;
  dst.Read(physical_pos, schema.FixedRowSize(), pos);
  return true;
}

bool RowPage::Insert(Transaction& txn, const Row& record, const Schema& schema,
                     RowPosition& dst) {
  dst = RowPosition(PageId(), row_count_);
  if (!txn.AddWriteSet(dst)) {
    return false;
  }
  std::string_view payload(record.Data(), record.Size());
  InsertRow(payload);
  txn.InsertLog(dst, payload);
  last_lsn = txn.PrevLSN();
  return true;
}

void RowPage::InsertRow(std::string_view redo_log) {
  assert(row_count_ <= MaxSlot());
  memcpy(data_ + row_size_ * row_count_, redo_log.data(), redo_log.size());
  row_count_++;
}

bool RowPage::Update(Transaction& txn, const RowPosition& pos,
                     const Row& new_row, const Schema& schema) {
  if (!txn.AddWriteSet(pos)) {
    return false;
  }
  Row target_row(data_ + row_size_ * pos.slot, row_size_, pos);
  std::string_view undo_log(target_row.Data(), target_row.Size());
  std::string_view redo_log(new_row.Data(), new_row.Size());
  txn.UpdateLog(pos, redo_log, undo_log);
  UpdateImpl(pos, redo_log);
  last_lsn = txn.PrevLSN();
  return true;
}

void RowPage::UpdateRow(const RowPosition& pos, std::string_view redo_log) {
  memcpy(data_ + row_size_ * pos.slot, redo_log.data(), redo_log.size());
}

bool RowPage::Delete(Transaction& txn, const RowPosition& pos,
                     const Schema& schema) {
  RowPosition final_row_pos(PageId(), row_count_ - 1);
  if (!txn.AddWriteSet(pos) || !txn.AddWriteSet(final_row_pos)) {
    return false;
  }
  txn.DeleteLog(pos, std::string_view(data_ + row_size_ * pos.slot, row_size_));
  DeleteRow(pos);
  return true;
}

void RowPage::DeleteRow(const RowPosition& pos) {
  memcpy(data_ + row_size_ * pos.slot, data_ + row_size_ * (row_count_ - 1),
         row_size_);
  row_count_--;
}

size_t RowPage::RowCount() const { return row_count_; }

}  // namespace tinylamb

uint64_t std::hash<tinylamb::RowPage>::operator()(
    const tinylamb::RowPage& r) const {
  uint64_t ret = 0;
  for (size_t i = 0; i < r.row_count_; ++i) {
    const char* physical_pos = r.data_ + i * r.row_size_;
    ret += std::hash<std::string_view>()(
        std::string_view(physical_pos, r.row_size_));
  }
  return ret;
}
