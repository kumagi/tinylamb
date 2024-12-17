/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "database.hpp"

#include <cstdio>
#include <cstdlib>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>

#include "common/constants.hpp"
#include "common/encoder.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "database/page_storage.hpp"
#include "index/b_plus_tree_iterator.hpp"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction.hpp"
#include "type/column.hpp"
#include "type/schema.hpp"

namespace tinylamb {

constexpr int kDefaultTableRoot = 1;
constexpr int kDefaultStatisticsRoot = 2;

Database::Database(std::string_view dbname)
    : catalog_(kDefaultTableRoot),
      statistics_(kDefaultStatisticsRoot),
      storage_(dbname) {
  auto ctx = BeginContext();
  catalog_ = BPlusTree(ctx.txn_, kDefaultTableRoot);
  statistics_ = BPlusTree(ctx.txn_, kDefaultStatisticsRoot);
  if (ctx.txn_.PreCommit() != Status::kSuccess) {
    LOG(FATAL) << "Failed to initialize relations";
    exit(1);
  }
}

template <typename Serializable>
static std::string Serialize(const Serializable& from) {
  std::stringstream ss;
  Encoder arc(ss);
  arc << from;
  return ss.str();
}

template <typename Deserializable>
static void Deserialize(std::string_view from, Deserializable& dst) {
  std::string v(from);
  std::stringstream ss(v);
  Decoder ext(ss);
  ext >> dst;
}

StatusOr<Table> Database::CreateTable(TransactionContext& ctx,
                                      const Schema& schema) {
  if (catalog_.Read(ctx.txn_, schema.Name()).GetStatus() !=
      Status::kNotExists) {
    return Status::kConflicts;
  }
  PageRef table_page =
      storage_.pm_.AllocateNewPage(ctx.txn_, PageType::kRowPage);
  Table new_table(schema, table_page->PageID());
  TableStatistics new_stat(schema);

  // Prepare index for primary-key and unique-key.
  for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
    const Column& col = schema.GetColumn(i);
    if (col.GetConstraint().IsUnique()) {
      std::stringstream idx_name_stream;
      idx_name_stream << schema.Name() << "|" << col.Name();
      IndexSchema new_idx(idx_name_stream.str(), {i}, {});
      new_table.CreateIndex(ctx.txn_, new_idx);
    }
  }

  RETURN_IF_FAIL(
      catalog_.Insert(ctx.txn_, schema.Name(), Serialize(new_table)));
  RETURN_IF_FAIL(statistics_.Insert(ctx.txn_, std::string(schema.Name()),
                                    Serialize(new_stat)));
  return new_table;
}

Status Database::CreateIndex(TransactionContext& ctx,
                             std::string_view schema_name,
                             const IndexSchema& idx) {
  ASSIGN_OR_RETURN(Table, tbl, GetTable(ctx, schema_name));
  RETURN_IF_FAIL(tbl.CreateIndex(ctx.txn_, idx));
  return catalog_.Update(ctx.txn_, schema_name, Serialize(tbl));
}

StatusOr<Table> Database::GetTable(TransactionContext& ctx,
                                   std::string_view schema_name) {
  ASSIGN_OR_RETURN(std::string_view, val, catalog_.Read(ctx.txn_, schema_name));
  Table tbl;
  Deserialize(val, tbl);
  return tbl;
}

[[maybe_unused]] void Database::DebugDump(Transaction& txn, std::ostream& o) {
  // FIXME(kumagi): The btree also has statistics entry.
  BPlusTreeIterator iter = catalog_.Begin(txn);
  while (iter.IsValid()) {
    Schema sc;
    Deserialize(iter.Value(), sc);
    o << sc << "\n";
    ++iter;
  }
}

StatusOr<TableStatistics> Database::GetStatistics(
    TransactionContext& ctx, std::string_view schema_name) {
  ASSIGN_OR_RETURN(std::string_view, val,
                   statistics_.Read(ctx.txn_, schema_name));
  ASSIGN_OR_RETURN(Table, tbl, GetTable(ctx, schema_name));
  TableStatistics ts(tbl.schema_);
  Deserialize(val, ts);
  return ts;
}

Status Database::UpdateStatistics(TransactionContext& ctx,
                                  std::string_view schema_name,
                                  const TableStatistics& ts) {
  return statistics_.Update(ctx.txn_, std::string(schema_name), Serialize(ts));
}

Status Database::RefreshStatistics(TransactionContext& ctx,
                                   std::string_view schema_name) {
  ASSIGN_OR_RETURN(Table, tbl, GetTable(ctx, schema_name));
  ASSIGN_OR_RETURN(TableStatistics, stats, GetStatistics(ctx, schema_name));
  RETURN_IF_FAIL(stats.Update(ctx.txn_, tbl));
  return UpdateStatistics(ctx, schema_name, stats);
}

void Database::EmulateCrash() { storage_.DiscardAllUpdates(); }

void Database::DeleteAll() {
  EmulateCrash();
  std::ignore = std::remove(storage_.DBName().c_str());
  std::ignore = std::remove(storage_.LogName().c_str());
  std::ignore = std::remove(storage_.MasterRecordName().c_str());
}

}  // namespace tinylamb
