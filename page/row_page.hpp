#ifndef TINYLAMB_ROW_PAGE_HPP
#define TINYLAMB_ROW_PAGE_HPP

#include <cassert>

#include "page/page.hpp"
#include "page/row_position.hpp"
#include "type/catalog.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class Recovery;

/*  Page Layout
 * +----------------------------------------------------+
 * | Page Header | Row Page Header | data_[] | (Tuples) |
 * +----------------------------------------------------+
 * | (cont. Tuples)                                     |
 * +----------------------------------------------------+
 * | (cont. Tuples)                             | Tuple |
 * +----------------------------------------------------+
 *                                     free_ptr_^
 */

class RowPage : public Page {
 public:
  void Initialize() {
    prev_page_id_ = 0;
    next_page_id_ = 0;
    row_count_ = 0;
    free_ptr_ = kPageSize;
    free_size_ = kPageSize - sizeof(RowPage);
    memset(data_, 0, kBodySize);
  }

  bool Read(Transaction& txn, const RowPosition& pos, Row& dst);

  bool Insert(Transaction& txn, const Row& record, RowPosition& dst);

  bool Update(Transaction& txn, const RowPosition& pos, const Row& row);

  bool Delete(Transaction& txn, const RowPosition& pos);

  size_t RowCount() const;

  uint16_t FreePtrForTest() const { return free_ptr_; }
  uint16_t FreeSizeForTest() const { return free_size_; }

  void DeFragment();

  struct RowPointer {
    // Row start position from beginning fom this page.
    uint16_t offset;

    // Physical row size in bytes (required to get exact size for logging).
    int16_t size;
  };
  uint64_t prev_page_id_ = 0;
  uint64_t next_page_id_ = 0;
  int16_t row_count_ = 0;
  uint16_t free_ptr_ = kPageSize;
  uint16_t free_size_ = kPageSize - sizeof(RowPage);
  RowPointer data_[0];
  constexpr static size_t kBodySize =
      kPageBodySize - sizeof(prev_page_id_) - sizeof(next_page_id_) -
      sizeof(row_count_) - sizeof(free_ptr_) - sizeof(free_size_);

  friend class Page;
  friend class Catalog;
  friend class std::hash<RowPage>;

  Row GetRow(uint16_t slot) const {
    return Row(
        std::string_view(PageHead() + data_[slot].offset, data_[slot].size),
        RowPosition(PageId(), slot));
  }

  uint16_t InsertRow(std::string_view new_row);

  void UpdateRow(int slot, std::string_view new_row);

  void DeleteRow(int slot);
};

static_assert(std::is_trivially_destructible<RowPage>::value == true,
              "RowPage must be trivially destructible");

}  // namespace tinylamb

namespace std {

template <>
class hash<tinylamb::RowPage> {
 public:
  uint64_t operator()(const tinylamb::RowPage& r) const;
};

}  // namespace std

#endif  // TINYLAMB_ROW_PAGE_HPP
