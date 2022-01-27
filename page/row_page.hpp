#ifndef TINYLAMB_ROW_PAGE_HPP
#define TINYLAMB_ROW_PAGE_HPP

#include <cassert>
#include <string_view>

#include "common/constants.hpp"

namespace tinylamb {

class RecoveryManager;
class Transaction;

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

class RowPage {
 private:
  char* Payload() { return reinterpret_cast<char*>(this); }
  [[nodiscard]] const char* Payload() const {
    return reinterpret_cast<const char*>(this);
  }

 public:
  void Initialize() {
    prev_page_id_ = 0;
    next_page_id_ = 0;
    row_count_ = 0;
    free_ptr_ = kPageBodySize;
    free_size_ = kPageBodySize - sizeof(RowPage);
    // memset(data_, 0, kBodySize);
  }

  bool Read(page_id_t page_id, Transaction& txn, uint16_t slot,
            std::string_view* dst) const;

  bool Insert(page_id_t page_id, Transaction& txn, std::string_view record,
              uint16_t* dst);

  bool Update(page_id_t page_id, Transaction& txn, uint16_t slot,
              std::string_view record);

  bool Delete(page_id_t page_id, Transaction& txn, uint16_t slot);

  [[nodiscard]] size_t RowCount() const;

  [[nodiscard]] uint16_t FreePtrForTest() const { return free_ptr_; }
  [[nodiscard]] uint16_t FreeSizeForTest() const { return free_size_; }

  void DeFragment();

  struct RowPointer {
    // Row start position from beginning fom this page.
    uint16_t offset = 0;

    // Physical row size in bytes (required to get exact size for logging).
    int16_t size = 0;
  };

  friend class Page;
  friend class std::hash<RowPage>;

  [[nodiscard]] std::string_view GetRow(uint16_t slot) const {
    return {Payload() + data_[slot].offset,
            static_cast<size_t>(data_[slot].size)};
  }

  uint16_t InsertRow(std::string_view new_row);

  void UpdateRow(uint16_t slot, std::string_view record);

  void DeleteRow(uint16_t slot);

  page_id_t prev_page_id_ = 0;
  page_id_t next_page_id_ = 0;
  int16_t row_count_ = 0;
  uint16_t free_ptr_ = kPageSize;
  uint16_t free_size_ = kPageSize - sizeof(RowPage);
  RowPointer data_[0];
  void Dump(std::ostream& o, int indent) const;
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
