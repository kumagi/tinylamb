#include "relation_storage.hpp"

#include "common/encoder.hpp"
#include "database/page_storage.hpp"
#include "index/b_plus_tree_iterator.hpp"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

constexpr int kTableRoot = 1;
constexpr int kStatsRoot = 2;

RelationStorage::RelationStorage(std::string_view dbname)
    : catalog_(kTableRoot), statistics_(kStatsRoot), storage_(dbname) {
  auto txn = Begin();
  {
    PageRef tables_root = storage_.pm_.GetPage(kTableRoot);
    if (!tables_root.IsValid()) {
      tables_root->PageInit(kTableRoot, PageType::kLeafPage);
    }
  }
  {
    PageRef stats_root = storage_.pm_.GetPage(kStatsRoot);
    if (!stats_root.IsValid()) {
      stats_root->PageInit(kTableRoot, PageType::kLeafPage);
    }
  }
  if (txn.PreCommit() != Status::kSuccess) {
    LOG(FATAL) << "Failed to initialize relations";
    exit(1);
  }
}

template <typename Serializable>
std::string Serialize(const Serializable& from) {
  std::stringstream ss;
  Encoder arc(ss);
  arc << from;
  return ss.str();
}
template <typename Deserializable>
void Deserialize(std::string_view from, Deserializable& dst) {
  std::string v(from);
  std::stringstream ss(v);
  Decoder ext(ss);
  ext >> dst;
}

Status RelationStorage::CreateTable(Transaction& txn, const Schema& schema) {
  std::string_view val;
  if (catalog_.Read(txn, schema.Name(), &val) != Status::kNotExists) {
    return Status::kConflicts;
  }
  PageRef table_page = storage_.pm_.AllocateNewPage(txn, PageType::kRowPage);
  Table new_table(schema, table_page->PageID());
  TableStatistics new_stat(schema);

  Status s = catalog_.Insert(txn, schema.Name(), Serialize(new_table));
  if (s != Status::kSuccess) {
    return s;
  }
  return statistics_.Insert(txn, std::string(schema.Name()),
                            Serialize(new_stat));
}

Status RelationStorage::CreateIndex(Transaction& txn,
                                    std::string_view schema_name,
                                    const IndexSchema& idx) {
  Table tbl;
  Status s = GetTable(txn, schema_name, &tbl);
  if (s != Status::kSuccess) {
    return s;
  }
  return tbl.CreateIndex(txn, idx);
}

Status RelationStorage::GetTable(Transaction& txn, std::string_view schema_name,
                                 Table* tbl) {
  std::string_view val;
  Status s = catalog_.Read(txn, schema_name, &val);
  if (s != Status::kSuccess) {
    return Status::kNotExists;
  }
  Deserialize(val, *tbl);
  return Status::kSuccess;
}

[[maybe_unused]] void RelationStorage::DebugDump(Transaction& txn,
                                                 std::ostream& o) {
  // FIXME(kumagi): The btree also has statistics entry.
  BPlusTreeIterator iter = catalog_.Begin(txn);
  while (iter.IsValid()) {
    Schema sc;
    Deserialize(iter.Value(), sc);
    o << sc << "\n";
    ++iter;
  }
}

Status RelationStorage::GetStatistics(Transaction& txn,
                                      std::string_view schema_name,
                                      TableStatistics* ts) {
  std::string_view val;
  Status s = statistics_.Read(txn, std::string(schema_name), &val);
  if (s != Status::kSuccess) {
    return s;
  }
  Deserialize(val, *ts);
  return Status::kSuccess;
}

Status RelationStorage::UpdateStatistics(Transaction& txn,
                                         std::string_view schema_name,
                                         const TableStatistics& ts) {
  return statistics_.Update(txn, std::string(schema_name), Serialize(ts));
}

Status RelationStorage::RefreshStatistics(Transaction& txn,
                                          std::string_view schema_name) {
  Table tbl;
  Status s = GetTable(txn, schema_name, &tbl);
  if (s != Status::kSuccess) {
    return s;
  }
  TableStatistics ts(tbl.GetSchema());
  s = ts.Update(txn, tbl);
  if (s != Status::kSuccess) {
    return s;
  }
  s = UpdateStatistics(txn, schema_name, ts);
  return s;
}

}  // namespace tinylamb
