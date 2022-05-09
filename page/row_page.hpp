#ifndef TINYLAMB_ROW_PAGE_HPP
#define TINYLAMB_ROW_PAGE_HPP

#include <cassert>
#include <string_view>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"

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

  StatusOr<std::string_view> Read(page_id_t page_id, Transaction& txn, slot_t slot) const;

  StatusOr<slot_t> Insert(page_id_t page_id, Transaction& txn, std::string_view record);

  Status Update(page_id_t page_id, Transaction& txn, slot_t slot,
                std::string_view record);

  Status Delete(page_id_t page_id, Transaction& txn, slot_t slot);

  [[nodiscard]] slot_t RowCount() const;

  [[nodiscard]] bin_size_t FreePtrForTest() const { return free_ptr_; }
  [[nodiscard]] bin_size_t FreeSizeForTest() const { return free_size_; }

  void DeFragment();

  struct RowPointer {
    // Row start position from beginning fom this page.
    bin_size_t offset = 0;

    // Physical row size in bytes (required to get exact size for logging).
    bin_size_t size = 0;
  };

  friend class Page;
  friend class std::hash<RowPage>;

  [[nodiscard]] std::string_view GetRow(slot_t slot) const {
    return {Payload() + rows_[slot].offset,
            static_cast<size_t>(rows_[slot].size)};
  }

  bin_size_t InsertRow(std::string_view new_row);

  void UpdateRow(slot_t slot, std::string_view record);

  void DeleteRow(slot_t slot);

  page_id_t prev_page_id_ = 0;
  page_id_t next_page_id_ = 0;
  slot_t row_max_ = 0;
  slot_t row_count_ = 0;
  bin_size_t free_ptr_ = kPageSize;
  bin_size_t free_size_ = kPageSize - sizeof(RowPage);
  RowPointer rows_[0];
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
