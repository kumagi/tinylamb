#ifndef TINYLAMB_CATALOG_HPP
#define TINYLAMB_CATALOG_HPP

#include <bits/unique_ptr.h>

#include <cstdint>
#include <ostream>
#include <string_view>

#include "common/constants.hpp"
#include "table/b_plus_tree.hpp"

namespace tinylamb {

class PageManager;
class Table;
class Transaction;
class Schema;

class Catalog {
  static constexpr uint64_t kCatalogPageId = 1;

 public:
  explicit Catalog(page_id_t pid, PageManager* pm)
      : root_(pid), pm_(pm), bpt_(pid, pm) {}

  void InitializeIfNeeded(Transaction& txn);

  Status CreateTable(Transaction& txn, const Schema& schema);

  Status GetTable(Transaction& txn, std::string_view schema_name, Table* tbl);

  [[maybe_unused]] void DebugDump(Transaction& txn, std::ostream& o);

 private:
  page_id_t root_;
  PageManager* pm_;
  BPlusTree bpt_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CATALOG_HPP
