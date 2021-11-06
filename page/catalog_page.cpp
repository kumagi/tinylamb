#include "catalog_page.hpp"

#include <cstring>
#include <transaction/transaction.hpp>

#include "page.hpp"
#include "type/schema.hpp"

namespace tinylamb {

void CatalogPage::Initialize() {
  prev_page_id = 0;
  next_page_id = 0;
  free_list_head = 0;
  slot_count = 0;
  payload_begin = kPageSize;
}

RowPosition CatalogPage::AddSchema(Transaction& txn, const Schema& schema) {
  const uint16_t expected_size = schema.Size();
  const uint16_t next_begin = payload_begin - expected_size;
  if (next_begin < (slot_count + 1) * sizeof(uint16_t)) {
    // No enough space to insert a new schema.
    return RowPosition();
  } else {
    uint16_t new_slot = slot_count;
    RowPosition pos(PageId(), new_slot);
    if (!txn.AddWriteSet(RowPosition(pos))) {
      return RowPosition();
    }
    txn.AddWriteSet(RowPosition(PageId(), new_slot));
    InsertSchema(schema.Data());
    txn.InsertLog(pos, schema.Data());
    SetPageLSN(txn.PrevLSN());
    return RowPosition(PageId(), new_slot);
  }
}

void CatalogPage::InsertSchema(std::string_view schema_data) {
  payload_begin -= schema_data.size();
  slot[slot_count++] = payload_begin;
  memcpy(reinterpret_cast<char*>(this) + payload_begin, schema_data.data(),
         schema_data.size());
}
void CatalogPage::UpdateSchema(const RowPosition& pos,
                               std::string_view schema_data) {
  throw std::runtime_error("not implemented CatalogPage::UpdateSchema");
}

void CatalogPage::DeleteSchema(const RowPosition& pos) {
  throw std::runtime_error("not implemented CatalogPage::DeleteSchema");
}

[[nodiscard]] Schema CatalogPage::Read(Transaction& txn,
                                       const RowPosition& pos) {
  assert(pos.page_id == PageId());
  assert(pos.slot < slot_count);
  txn.AddReadSet(pos);
  return Schema(reinterpret_cast<char*>(this) + slot[pos.slot]);
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::CatalogPage>::operator()(
    const tinylamb::CatalogPage& c) const {
  static constexpr uint64_t kCatalogChecksumSalt = 0xca1a6;
  uint64_t result = kCatalogChecksumSalt;
  result += std::hash<uint64_t>()(c.prev_page_id);
  result += std::hash<uint64_t>()(c.next_page_id);
  result += std::hash<uint16_t>()(c.free_list_head);
  result += std::hash<uint16_t>()(c.slot_count);
  result += std::hash<uint16_t>()(c.payload_begin);
  for (size_t i = 0; i < c.SlotCount(); ++i) {
    result += std::hash<tinylamb::Schema>()(c.Slot(i));
  }
  return result;
}
