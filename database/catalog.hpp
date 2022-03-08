#ifndef TINYLAMB_CATALOG_HPP
#define TINYLAMB_CATALOG_HPP

#include <bits/unique_ptr.h>

#include <cstdint>
#include <ostream>
#include <string_view>

#include "common/constants.hpp"
#include "index/b_plus_tree.hpp"

namespace tinylamb {

class PageManager;
class Table;
class Transaction;
class Schema;
class TableStatistics;

class Catalog {
  static constexpr uint64_t kCatalogPageId = 1;

 public:
  explicit Catalog(page_id_t table_root, page_id_t stats_root, PageManager* pm)
      : table_root_(table_root),
        stats_root_(stats_root),
        pm_(pm),
        tables_(table_root_, pm),
        stats_(stats_root_, pm) {}

  void InitializeIfNeeded(Transaction& txn);

  Status CreateTable(Transaction& txn, const Schema& schema);

  Status GetTable(Transaction& txn, std::string_view schema_name, Table* tbl);

  [[maybe_unused]] void DebugDump(Transaction& txn, std::ostream& o);

  Status GetStatistics(Transaction& txn, std::string_view schema_name,
                       TableStatistics* ts);
  Status UpdateStatistics(Transaction& txn, std::string_view schema_name,
                          const TableStatistics& ts);
  Status RefreshStatistics(Transaction& txn, std::string_view schema_name);

 private:
  page_id_t table_root_;
  page_id_t stats_root_;
  PageManager* pm_;
  BPlusTree tables_;
  BPlusTree stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CATALOG_HPP
