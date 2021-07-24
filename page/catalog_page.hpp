#ifndef TINYLAMB_CATALOG_PAGE_HPP
#define TINYLAMB_CATALOG_PAGE_HPP

#include "page.hpp"
#include "type/schema.hpp"

namespace tinylamb {

struct Transaction;
class Catalog;

class CatalogPage : public Page {
  friend class std::hash<tinylamb::CatalogPage>;
 public:
  void Initialize();

  RowPosition AddSchema(Transaction& txn, const Schema& schema);

  [[nodiscard]] Schema Read(Transaction& txn, const RowPosition& pos);

  friend std::ostream& operator<<(std::ostream& o, const CatalogPage& c) {
    o << "CatalogPage: prev: " << c.prev_page_id << " next: " << c.next_page_id
      << " free_head: " << c.free_list_head << " slot_count: " << c.slot_count
      << " payload_begin: " << c.payload_begin << " (" << c.SlotCount() << "){";
    for (uint16_t i = 0; i < c.SlotCount(); ++i) {
      o << c.Slot(i);
    }
    o << "}";
    return o;
  }

  [[nodiscard]] uint64_t NextPageID() const { return next_page_id; }
  [[nodiscard]] uint16_t SlotCount() const { return slot_count; }
  Schema Slot(uint8_t idx) const {
    return Schema(reinterpret_cast<const char*>(this) + slot[idx]);
  }

 private:
  friend class Page;
  friend class Catalog;
  void InsertSchema(std::string_view schema_data);
  void UpdateSchema(const RowPosition& pos, std::string_view schema_data);
  void DeleteSchema(const RowPosition& pos);

  uint64_t prev_page_id = 0;
  uint64_t next_page_id = 0;
  uint16_t free_list_head = 0;
  uint16_t slot_count = 0;
  uint16_t payload_begin = kPageSize;
  uint16_t slot[0];
};

}  // namespace tinylamb

namespace std {
template <>
class hash<tinylamb::CatalogPage> {
 public:
  uint64_t operator()(const tinylamb::CatalogPage& c) const;
};

}  // namespace std

#endif  // TINYLAMB_CATALOG_PAGE_HPP
