#include "row_page.hpp"

#include "recovery/log_record.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

bool RowPage::Read(Transaction& txn, const RowPosition& pos,
                   const Schema& schema, Row& dst) {
  if (pos.page_id != PageId() || Metadata().row_count <= pos.slot) {
    return false;
  }
  char* physical_pos = PageData() + schema.FixedRowSize() * pos.slot;
  dst.Read(physical_pos, schema.FixedRowSize(), pos);
  return true;
}

bool RowPage::Insert(Transaction& txn, const Row& record, const Schema& schema,
                     RowPosition& dst) {
  auto& header = Metadata();
  dst = RowPosition(PageId(), header.row_count);
  if (!txn.AddWriteSet(dst)) {
    return false;
  }
  uint16_t target_slot = header.row_count;
  const size_t row_size = schema.FixedRowSize();
  Row inserted_row(PageData() + row_size * target_slot, schema.FixedRowSize(),
                   dst);
  inserted_row.Write(schema, record);
  txn.InsertLog(dst, std::string_view(record.Data(), record.Size()));
  header.row_count++;
  Header().last_lsn = txn.PrevLSN();
  return true;
}

bool RowPage::Update(Transaction& txn, const RowPosition& pos,
                     const Row& new_row, const Schema& schema) {
  auto& header = Metadata();
  if (!txn.AddWriteSet(pos)) {
    return false;
  }
  Row target_row(PageData() + header.row_size * pos.slot, header.row_size, pos);
  txn.UpdateLog(pos, std::string_view(new_row.Data(), new_row.Size()),
                std::string_view(target_row.Data(), target_row.Size()));
  target_row.Write(schema, new_row);
  Header().last_lsn = txn.PrevLSN();
  return true;
}

bool RowPage::Delete(Transaction& txn, const RowPosition& pos,
                     const Schema& schema) {
  auto& header = Metadata();
  RowPosition final_row_pos(PageId(), header.row_count - 1);
  if (!txn.AddWriteSet(pos) || !txn.AddWriteSet(final_row_pos)) {
    return false;
  }
  Row final_row(PageData() + header.row_size * final_row_pos.slot,
                header.row_size, final_row_pos);
  char* target_ptr = PageData() + header.row_size * pos.slot;
  Row target_row(target_ptr, header.row_size, pos);
  txn.DeleteLog(pos, std::string_view(target_ptr, header.row_size));
  target_row.Write(schema, final_row);
  header.row_count--;
  return true;
}

uint64_t RowPage::CalcChecksum() const {
  const auto& header = Metadata();
  uint64_t ret = 0;
  for (size_t i = 0; i < header.row_count; ++i) {
    const char* physical_pos = PageData() + i * header.row_size;
    ret += std::hash<std::string_view>()(
        std::string_view(physical_pos, header.row_size));
  }
  return ret;
}

}  // namespace tinylamb