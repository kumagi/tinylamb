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

constexpr int kDefaultTableRoot = 1;
constexpr int kDefaultStatisticsRoot = 2;

RelationStorage::RelationStorage(std::string_view dbname)
    : catalog_(kDefaultTableRoot),
      statistics_(kDefaultStatisticsRoot),
      storage_(dbname) {
  auto txn = Begin();
  catalog_ = BPlusTree(txn, kDefaultTableRoot);
  statistics_ = BPlusTree(txn, kDefaultStatisticsRoot);
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
  if (catalog_.Read(txn, schema.Name()).GetStatus() != Status::kNotExists) {
    return Status::kConflicts;
  }
  PageRef table_page = storage_.pm_.AllocateNewPage(txn, PageType::kRowPage);
  Table new_table(schema, table_page->PageID());
  TableStatistics new_stat(schema);

  RETURN_IF_FAIL(catalog_.Insert(txn, schema.Name(), Serialize(new_table)));
  return statistics_.Insert(txn, std::string(schema.Name()),
                            Serialize(new_stat));
}

Status RelationStorage::CreateIndex(Transaction& txn,
                                    std::string_view schema_name,
                                    const IndexSchema& idx) {
  ASSIGN_OR_RETURN(Table, tbl, GetTable(txn, schema_name));
  RETURN_IF_FAIL(tbl.CreateIndex(txn, idx));
  return catalog_.Update(txn, schema_name, Serialize(tbl));
}

StatusOr<Table> RelationStorage::GetTable(Transaction& txn,
                                          std::string_view schema_name) {
  ASSIGN_OR_RETURN(std::string_view, val, catalog_.Read(txn, schema_name));
  Table tbl;
  Deserialize(val, tbl);
  return tbl;
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

StatusOr<TableStatistics> RelationStorage::GetStatistics(
    Transaction& txn, std::string_view schema_name) {
  ASSIGN_OR_RETURN(std::string_view, val, statistics_.Read(txn, schema_name));
  ASSIGN_OR_RETURN(Table, tbl, GetTable(txn, schema_name));
  TableStatistics ts(tbl.schema_);
  Deserialize(val, ts);
  return ts;
}

Status RelationStorage::UpdateStatistics(Transaction& txn,
                                         std::string_view schema_name,
                                         const TableStatistics& ts) {
  return statistics_.Update(txn, std::string(schema_name), Serialize(ts));
}

Status RelationStorage::RefreshStatistics(Transaction& txn,
                                          std::string_view schema_name) {
  ASSIGN_OR_RETURN(Table, tbl, GetTable(txn, schema_name));
  ASSIGN_OR_RETURN(TableStatistics, stats, GetStatistics(txn, schema_name));
  RETURN_IF_FAIL(stats.Update(txn, tbl));
  return UpdateStatistics(txn, schema_name, stats);
}

}  // namespace tinylamb
