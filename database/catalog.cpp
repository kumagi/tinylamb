#include "catalog.hpp"

#include <cstdint>
#include <sstream>
#include <string>

#include "common/encoder.hpp"
#include "common/log_message.hpp"
// #include "page/catalog_page.hpp"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/row_page.hpp"
#include "table/b_plus_tree_iterator.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

void Catalog::InitializeIfNeeded(Transaction& txn) {
  PageRef root_page = pm_->GetPage(root_);
  if (!root_page.IsValid()) {
    PageRef root = pm_->AllocateNewPage(txn, PageType::kLeafPage);
    root_ = root->PageID();
    bpt_ = BPlusTree(root_, pm_);
  }
}

Status Catalog::CreateTable(Transaction& txn, const Schema& schema) {
  std::string_view val;
  if (bpt_.Read(txn, schema.Name(), &val) != Status::kNotExists) {
    return Status::kConflicts;
  }
  PageRef table_page = pm_->AllocateNewPage(txn, PageType::kRowPage);
  Table new_table(pm_, schema, table_page->PageID(), {});

  std::stringstream ss;
  Encoder arc(ss);
  arc << new_table;
  Status s = bpt_.Insert(txn, schema.Name(), ss.str());
  if (s == Status::kConflicts) return s;
  return Status::kSuccess;
}

Status Catalog::GetTable(Transaction& txn, std::string_view schema_name,
                         Table* tbl) {
  std::string_view val;
  Status s = bpt_.Read(txn, schema_name, &val);
  if (s != Status::kSuccess) return Status::kNotExists;
  Table t(pm_);
  std::string v(val);
  std::stringstream ss(v);
  Decoder ext(ss);
  ext >> *tbl;
  return Status::kSuccess;
}

[[maybe_unused]] void Catalog::DebugDump(Transaction& txn, std::ostream& o) {
  BPlusTreeIterator iter = bpt_.Begin(txn);
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

}  // namespace tinylamb
