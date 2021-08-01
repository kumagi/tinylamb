#ifndef TINYLAMB_ROW_PAGE_HPP
#define TINYLAMB_ROW_PAGE_HPP

#include <cassert>

#include "../type/row.hpp"
#include "../type/schema.hpp"
#include "page.hpp"
#include "row_position.hpp"
#include "type/catalog.hpp"

namespace tinylamb {

class Recovery;

class RowPage : public Page {
 public:
  void Initialize() {
    LOG(DEBUG) << "initialize row page";
    prev_page_id_ = 0;
    next_page_id_ = 0;
    row_count_ = 0;
    row_size_ = 0;
  }

  bool Read(Transaction& txn, const RowPosition& pos, const Schema& schema,
            Row& dst);

  bool Insert(Transaction& txn, const Row& record, const Schema& schema,
              RowPosition& dst);

  bool Update(Transaction& txn, const RowPosition& pos, const Row& row,
              const Schema& schema);

  bool Delete(Transaction& txn, const RowPosition& pos, const Schema& schema);

  uint16_t MaxSlot() const {
    return (reinterpret_cast<const char*>(this) + kPageSize - data_) / row_size_;
  }

  size_t RowCount() const;

 private:
  uint64_t prev_page_id_ = 0;
  uint64_t next_page_id_ = 0;
  int16_t row_count_ = 0;
  uint16_t row_size_ = 0;
  char data_[0];

  friend class Page;
  friend class Catalog;
  friend class std::hash<RowPage>;
  void InsertRow(std::string_view redo_log);

  void UpdateRow(const RowPosition& pos, std::string_view redo_log);

  void DeleteRow(const RowPosition& pos);
};

}  // namespace tinylamb

namespace std {

template<>
class hash<tinylamb::RowPage> {
 public:
  uint64_t operator()(const tinylamb::RowPage& r) const;
};

}

#endif  // TINYLAMB_ROW_PAGE_HPP
