#include "catalog.hpp"

#include <cstdint>
#include <sstream>
#include <string>

#include "common/encoder.hpp"
#include "common/log_message.hpp"
// #include "page/catalog_page.hpp"
#include "index/b_plus_tree_iterator.hpp"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/row_page.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

void Catalog::InitializeIfNeeded(Transaction& txn) {
  PageRef table_root_page = pm_->GetPage(table_root_);
  if (!table_root_page.IsValid()) {
    PageRef table_root = pm_->AllocateNewPage(txn, PageType::kLeafPage);
    table_root_ = table_root->PageID();
    tables_ = BPlusTree(table_root_, pm_);
  }
  PageRef stats_root_page = pm_->GetPage(stats_root_);
  if (!stats_root_page.IsValid()) {
    PageRef stats_root = pm_->AllocateNewPage(txn, PageType::kLeafPage);
    stats_root_ = stats_root->PageID();
    stats_ = BPlusTree(stats_root_, pm_);
  }
}

Status Catalog::CreateTable(Transaction& txn, const Schema& schema) {
  std::string_view val;
  if (tables_.Read(txn, schema.Name(), &val) != Status::kNotExists) {
    return Status::kConflicts;
  }
  PageRef table_page = pm_->AllocateNewPage(txn, PageType::kRowPage);
  Table new_table(pm_, schema, table_page->PageID(), {});
  TableStatistics new_stat(schema);

  std::stringstream ss;
  Encoder arc(ss);
  arc << new_table;
  Status s = tables_.Insert(txn, schema.Name(), ss.str());
  if (s == Status::kConflicts) return s;

  s = stats_.Insert(txn, std::string(schema.Name()), "");
  if (s == Status::kConflicts) return s;
  return Status::kSuccess;
}

Status Catalog::GetTable(Transaction& txn, std::string_view schema_name,
                         Table* tbl) {
  std::string_view val;
  Status s = tables_.Read(txn, schema_name, &val);
  if (s != Status::kSuccess) return Status::kNotExists;
  Table t(pm_);
  std::string v(val);
  std::stringstream ss(v);
  Decoder ext(ss);
  ext >> *tbl;
  tbl->pm_ = pm_;
  return Status::kSuccess;
}

[[maybe_unused]] void Catalog::DebugDump(Transaction& txn, std::ostream& o) {
  // FIXME(kumagi): The btree also has statistics entry.
  BPlusTreeIterator iter = tables_.Begin(txn);
  while (iter.IsValid()) {
    std::string schema(*iter);
    std::stringstream ss(schema);
    Schema sc;
    Decoder ext(ss);
    ext >> sc;
    o << sc << "\n";
    ++iter;
  }
}

Status Catalog::GetStatistics(Transaction& txn, std::string_view schema_name,
                              TableStatistics* ts) {
  std::string_view val;
  Status s = stats_.Read(txn, std::string(schema_name), &val);
  if (s != Status::kSuccess) return s;
  std::string v(val);
  std::stringstream ss(v);
  Decoder ext(ss);
  ext >> *ts;
  return Status::kSuccess;
}

Status Catalog::UpdateStatistics(Transaction& txn, std::string_view schema_name,
                                 const TableStatistics& ts) {
  std::stringstream ss;
  Encoder enc(ss);
  enc << ts;
  std::string encoded_stat = ss.str();
  return stats_.Update(txn, std::string(schema_name), encoded_stat);
}

Status Catalog::RefreshStatistics(Transaction& txn,
                                  std::string_view schema_name) {
  Table tbl(pm_);
  Status s = GetTable(txn, schema_name, &tbl);
  if (s != Status::kSuccess) return s;
  TableStatistics ts(tbl.GetSchema());
  s = ts.Update(txn, tbl);
  if (s != Status::kSuccess) return s;
  s = UpdateStatistics(txn, schema_name, ts);
  return s;
}

}  // namespace tinylamb
