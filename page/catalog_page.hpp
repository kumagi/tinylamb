#ifndef TINYLAMB_CATALOG_PAGE_HPP
#define TINYLAMB_CATALOG_PAGE_HPP

#include "page.hpp"
#include "type/schema.hpp"

namespace tinylamb {

struct Transaction;

struct CatalogPageHeader {
  MAPPING_ONLY(CatalogPageHeader);
  void Initialize();
  uint64_t prev_page_id = 0;
  uint64_t next_page_id = 0;
  uint16_t free_list_head = 0;
  uint16_t slot_count = 0;
  uint16_t payload_begin = kPageSize;
  uint16_t slot[0] = {};

  uint64_t CalcChecksum() const;
  friend std::ostream& operator<<(std::ostream& o, const CatalogPageHeader& c) {
    o << "prev: " << c.prev_page_id << " next: " << c.next_page_id
      << " free_head: " << c.free_list_head << " slot_count: " << c.slot_count
      << " payload_begin: " << c.payload_begin;
    return o;
  }
};

class CatalogPage : public Page {
  MAPPING_ONLY(CatalogPage);

 public:
  void Initialize();

  RowPosition AddSchema(Transaction& txn, const Schema& schema);

  [[nodiscard]] uint64_t CalcChecksum() const;

  [[nodiscard]] Schema Read(Transaction& txn, const RowPosition& pos);

  friend std::ostream& operator<<(std::ostream& o, const CatalogPage& c) {
    o << "CatalogPage: " << c.Metadata() << " (" << c.SlotCount() << "){";
    const CatalogPageHeader& header = c.Metadata();
    for (uint16_t i = 0; i < c.SlotCount(); ++i) {
      o << Schema::Read(const_cast<char*>(c.Data() + header.slot[i]));
    }
    o << "}";
    return o;
  }
  [[nodiscard]] uint64_t NextPageID() const;
  [[nodiscard]] uint16_t SlotCount() const;

 private:
  [[nodiscard]] CatalogPageHeader& Metadata() {
    return BodyAs<CatalogPageHeader>();
  }
  [[nodiscard]] const CatalogPageHeader& Metadata() const {
    return BodyAs<CatalogPageHeader>();
  }
};

}  // namespace tinylamb

#endif  // TINYLAMB_CATALOG_PAGE_HPP
