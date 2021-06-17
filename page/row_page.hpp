#ifndef TINYLAMB_FIXED_LENGTH_PAGE_HPP
#define TINYLAMB_FIXED_LENGTH_PAGE_HPP

#include <cassert>

#include "../type/row.hpp"
#include "../type/schema.hpp"
#include "page.hpp"
#include "row_position.hpp"
#include "type/catalog.hpp"

namespace tinylamb {

struct FixedLengthPageHeader {
  MAPPING_ONLY(FixedLengthPageHeader);
  void Init() {
    prev_page_id = 0;
    next_page_id = 0;
    row_count = 0;
    free_list_head = 0;
  }
  uint64_t prev_page_id = 0;
  uint64_t next_page_id = 0;
  int16_t row_count = 0;
  uint16_t free_list_head = 0;
};

struct FixedLengthPage : public Page {
  MAPPING_ONLY(FixedLengthPage);
  void Init(uint64_t page_id) {
    memset(Data(), 0, kPageSize);

  }
  FixedLengthPageHeader& Metadata() {
    return Body<FixedLengthPageHeader>();
  }
  const FixedLengthPageHeader& Metadata() const {
    return Body<const FixedLengthPageHeader>();
  }
  static PayloadSize() {
    return kPageSize - sizeof(PageHeader) - sizeof(FixedLengthPageHeader);
  }

  Row Read(const RowPosition& pos, const Schema& schema) {
    assert(pos.page_id == PageId());
    return ret(&body[schema.RowSize() * pos.slot], RowSize(), pos);
  }

  // Make sure you have the lock of this row before invoke this function.
  RowPosition Insert(const Row& record, const Schema& schema) {
    const uint16_t next_pos = Metadata().row_count * schema.RowSize();
    if (PayloadSize() < next_pos + record.length) {
      // TODO: reuse deleted row space.
      return RowPosition();  // No enough space.
    }
    memcpy(Data() + next_pos, record.data, record.length);
    return RowPosition(PageId(), Header().row_count++);
  }

  // Make sure you have the lock of this row before invoke this function.
  bool Update(const Row& target, const Schema& schema) {
    assert(target.pos.page_id == PageId());
    memccpy(Data() + schema.RowSize() * target.pos.slot, target.data, target.length);
    return true;
  }

  bool Delete(const RowPosition& target) {
    // TODO: implement.
  }
};

static_assert(sizeof(FixedLengthPage) == sizeof(Page));

} // namespace tinylamb

#endif  // TINYLAMB_FIXED_LENGTH_PAGE_HPP
