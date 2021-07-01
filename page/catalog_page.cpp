#include "catalog_page.hpp"

#include <cstring>
#include <transaction/transaction.hpp>

#include "page.hpp"
#include "type/schema.hpp"

namespace tinylamb {

void CatalogPageHeader::Initialize() {
  prev_page_id = 0;
  next_page_id = 0;
  free_list_head = 0;
  slot_count = 0;
  payload_begin = kPageSize;
}

uint64_t CatalogPageHeader::CalcChecksum() const {
  uint64_t result = std::hash<uint64_t>()(prev_page_id);
  result += std::hash<uint64_t>()(next_page_id);
  result += std::hash<uint16_t>()(free_list_head);
  result += std::hash<uint16_t>()(slot_count);
  result += std::hash<uint16_t>()(payload_begin);
  return result;
}
void CatalogPage::Initialize() {
  Metadata().Initialize();
}

RowPosition CatalogPage::AddSchema(Transaction& txn, const Schema& schema) {
  CatalogPageHeader& header = Metadata();
  const uint16_t expected_size = schema.Size();
  const uint16_t next_begin = header.payload_begin - expected_size;
  if (next_begin <
      sizeof(header) + (header.slot_count + 1) * sizeof(uint16_t)) {
    // No enough space to insert new schema.
    std::cerr << "no space\n";
    return RowPosition();
  } else {
    uint16_t new_slot = header.slot_count;
    RowPosition pos(PageId(), new_slot);
    if (!txn.AddWriteSet(RowPosition(pos))) {
      return RowPosition();
    }
    header.slot[new_slot] = header.payload_begin - expected_size;
    header.slot_count++;
    header.payload_begin -= expected_size;
    Schema& new_schema =
        *reinterpret_cast<Schema*>(Data() + header.payload_begin);
    memcpy(Data() + next_begin, schema.Data(), schema.Size());
    txn.InsertLog(pos, std::string_view(schema.Data(), schema.Size()));
    Header().last_lsn = txn.PrevLSN();
    return RowPosition(PageId(), new_slot);
  }
}

[[nodiscard]] uint64_t CatalogPage::CalcChecksum() const {
  constexpr uint64_t kCatalogChecksumSalt = 0xca1a6;
  const auto& header = Metadata();
  uint64_t result = header.CalcChecksum() + kCatalogChecksumSalt;
  for (size_t i = 0; i < SlotCount(); ++i) {
    result +=
        Schema::Read(const_cast<char*>(Data() + header.slot[i])).CalcChecksum();
  }
  return result;
}

[[nodiscard]] Schema CatalogPage::Read(Transaction& txn,
                                       const RowPosition& pos) {
  assert(pos.page_id == PageId());
  const CatalogPageHeader& header = Metadata();
  assert(pos.slot <= header.slot_count);
  txn.AddReadSet(pos);
  return Schema::Read(Data() + header.slot[pos.slot]);
}

[[nodiscard]] uint64_t CatalogPage::NextPageID() const {
  return Metadata().next_page_id;
}
[[nodiscard]] uint16_t CatalogPage::SlotCount() const {
  return Metadata().slot_count;
}

}  // namespace tinylamb
