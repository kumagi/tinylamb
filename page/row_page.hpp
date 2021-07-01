#ifndef TINYLAMB_ROW_PAGE_HPP
#define TINYLAMB_ROW_PAGE_HPP

#include <cassert>

#include "../type/row.hpp"
#include "../type/schema.hpp"
#include "page.hpp"
#include "row_position.hpp"
#include "type/catalog.hpp"

namespace tinylamb {

struct RowPageHeader {
  MAPPING_ONLY(RowPageHeader);
  void Initialize() {
    prev_page_id = 0;
    next_page_id = 0;
    row_count = 0;
    row_size = 0;
  }
  uint64_t prev_page_id = 0;
  uint64_t next_page_id = 0;
  int16_t row_count = 0;
  uint16_t row_size = 0;
};

struct RowPage : public Page {
  MAPPING_ONLY(RowPage);
  RowPageHeader& Metadata() { return BodyAs<RowPageHeader>(); }
  void Initialize() {
    Metadata().Initialize();
  }
  [[nodiscard]] const RowPageHeader& Metadata() const {
    return BodyAs<const RowPageHeader>();
  }
  char* PageData() {
    return reinterpret_cast<char*>(this) + sizeof(PageHeader) +
           sizeof(RowPageHeader);
  }
  [[nodiscard]] const char* PageData() const {
    return reinterpret_cast<const char*>(this) + sizeof(PageHeader) +
           sizeof(RowPageHeader);
  }

  char* RowPageBody() {
    return payload + sizeof(PageHeader) + sizeof(RowPageHeader);
  }

  bool Read(Transaction& txn, const RowPosition& pos, const Schema& schema,
            Row& dst);

  bool Insert(Transaction& txn, const Row& record, const Schema& schema,
              RowPosition& dst);

  bool Update(Transaction& txn, const RowPosition& pos, const Row& row,
              const Schema& schema);

  bool Delete(Transaction& txn, const RowPosition& pos, const Schema& schema);

  [[nodiscard]] uint64_t CalcChecksum() const;
};
static_assert(sizeof(RowPage) == sizeof(Page));

}  // namespace tinylamb

#endif  // TINYLAMB_ROW_PAGE_HPP
