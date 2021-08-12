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
    prev_page_id_ = 0;
    next_page_id_ = 0;
    row_count_ = 0;
  }

  bool Read(Transaction& txn, const RowPosition& pos, const Schema& schema,
            Row& dst);

  bool Insert(Transaction& txn, const Row& record, const Schema& schema,
              RowPosition& dst);

  bool Update(Transaction& txn, const RowPosition& pos, const Row& row,
              const Schema& schema);

  bool Delete(Transaction& txn, const RowPosition& pos, const Schema& schema);

  size_t RowCount() const;

 private:
  uint64_t prev_page_id_ = 0;
  uint64_t next_page_id_ = 0;
  int16_t row_count_ = 0;
  char data_[0];
  constexpr static size_t kBodySize = kPageBodySize - sizeof(prev_page_id_) -
                                      sizeof(next_page_id_) -
                                      sizeof(row_count_);

  friend class Page;
  friend class Catalog;
  friend class std::hash<RowPage>;
  void InsertRow(std::string_view redo_log);

  void UpdateRow(const RowPosition& pos, std::string_view redo_log);

  void DeleteRow(const RowPosition& pos, size_t row_size);
};

}  // namespace tinylamb

namespace std {

template <>
class hash<tinylamb::RowPage> {
 public:
  uint64_t operator()(const tinylamb::RowPage& r) const;
};

}  // namespace std

#endif  // TINYLAMB_ROW_PAGE_HPP
