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

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "common/constants.hpp"
#include "common/decoder.hpp"
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
#include "type/function.hpp"
#include "type/schema.hpp"

namespace tinylamb {

constexpr int kDefaultTableRoot = 1;
constexpr int kDefaultStatisticsRoot = 2;
constexpr int kDefaultFunctionRoot = 3;

Database::Database(std::string_view dbname)
    : catalog_(kDefaultTableRoot),
      statistics_(kDefaultStatisticsRoot),
      functions_(kDefaultFunctionRoot),
      storage_(dbname) {
  auto ctx = BeginContext();
  catalog_ = BPlusTree(ctx.txn_, kDefaultTableRoot);
  statistics_ = BPlusTree(ctx.txn_, kDefaultStatisticsRoot);
  functions_ = BPlusTree(ctx.txn_, kDefaultFunctionRoot);
  if (ctx.txn_.PreCommit() != Status::kSuccess) {
    LOG(FATAL) << "Failed to initialize relations";
    exit(1);
  }
}

std::ostream& operator<<(std::ostream& o, const Database& db) {
  o << "Database(storage=" << db.storage_
    << ", catalogs=<BPlusTree; use DebugDump(txn, o) for details>)";
  return o;
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

namespace {

constexpr uint64_t kStatisticsMetaMagic = 0x544C53544D455441ULL;  // TLSTMETA

std::string StatisticsColumnKey(std::string_view table, slot_t column) {
  std::string key;
  key.reserve(table.size() + 3);
  key.append(table);
  key.push_back('\0');
  key.push_back(static_cast<char>((column >> 8) & 0xff));
  key.push_back(static_cast<char>(column & 0xff));
  return key;
}

std::string EncodeStatisticsMeta(size_t rows) {
  std::stringstream stream;
  Encoder encoder(stream);
  encoder << kStatisticsMetaMagic << static_cast<uint64_t>(rows);
  return stream.str();
}

uint64_t PeekUint64(std::string_view payload) {
  if (payload.size() < sizeof(uint64_t)) {
    return 0;
  }
  std::string copy(payload);
  std::stringstream stream(copy);
  Decoder decoder(stream);
  uint64_t value = 0;
  decoder >> value;
  return value;
}

Status UpsertStatistics(BPlusTree& tree, Transaction& txn, std::string_view key,
                        std::string_view value) {
  const Status updated = tree.Update(txn, key, value);
  if (updated == Status::kNotExists) {
    return tree.Insert(txn, key, value);
  }
  return updated;
}

Status WriteSplitStatistics(BPlusTree& tree, Transaction& txn,
                            std::string_view table_name,
                            const TableStatistics& stats) {
  RETURN_IF_FAIL(UpsertStatistics(tree, txn, table_name,
                                  EncodeStatisticsMeta(stats.Rows())));
  for (slot_t column = 0; column < stats.Columns(); ++column) {
    RETURN_IF_FAIL(UpsertStatistics(tree, txn,
                                    StatisticsColumnKey(table_name, column),
                                    Serialize(stats.Column(column))));
  }
  return Status::kSuccess;
}

}  // namespace

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
  // CreateIndex full-scans the table and reacquires this page latch.
  table_page.PageUnlock();

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
  RETURN_IF_FAIL(
      WriteSplitStatistics(statistics_, ctx.txn_, schema.Name(), new_stat));
  return new_table;
}

Status Database::DropTable(TransactionContext& ctx,
                           std::string_view schema_name) {
  ASSIGN_OR_RETURN(Table, tbl, GetTable(ctx, schema_name));
  const Schema& schema = tbl.GetSchema();
  for (slot_t column = 0; column < schema.ColumnCount(); ++column) {
    const Status deleted = statistics_.Delete(
        ctx.txn_, StatisticsColumnKey(schema_name, column));
    if (deleted != Status::kSuccess && deleted != Status::kNotExists) {
      return deleted;
    }
  }
  {
    const Status deleted = statistics_.Delete(ctx.txn_, schema_name);
    if (deleted != Status::kSuccess && deleted != Status::kNotExists) {
      return deleted;
    }
  }
  return catalog_.Delete(ctx.txn_, schema_name);
}

Status Database::CreateIndex(TransactionContext& ctx,
                             std::string_view schema_name,
                             const IndexSchema& idx) {
  ASSIGN_OR_RETURN(Table, tbl, GetTable(ctx, schema_name));
  RETURN_IF_FAIL(tbl.CreateIndex(ctx.txn_, idx));
  return catalog_.Update(ctx.txn_, schema_name, Serialize(tbl));
}

StatusOr<Function> Database::GetOrAddFunction(TransactionContext& ctx,
                                              std::string_view function_name,
                                              int argument_count) {
  StatusOr<std::string_view> val = functions_.Read(ctx.txn_, function_name);
  if (val.GetStatus() != Status::kSuccess &&
      val.GetStatus() != Status::kNotExists) {
    return val.GetStatus();
  }
  if (val.GetStatus() == Status::kNotExists) {
    Function new_func{std::string(function_name), argument_count};
    RETURN_IF_FAIL(
        functions_.Insert(ctx.txn_, function_name, Serialize(new_func)));
    return new_func;
  }
  Function func;
  Deserialize(val.Value(), func);
  return func;
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
  ASSIGN_OR_RETURN(Table, tbl, GetTable(ctx, schema_name));
  ASSIGN_OR_RETURN(std::string_view, meta,
                   statistics_.Read(ctx.txn_, schema_name));
  TableStatistics ts(tbl.GetSchema());
  if (PeekUint64(meta) != kStatisticsMetaMagic) {
    Deserialize(meta, ts);
    return ts;
  }
  uint64_t magic = 0;
  uint64_t rows = 0;
  {
    std::string copy(meta);
    std::stringstream stream(copy);
    Decoder decoder(stream);
    decoder >> magic >> rows;
  }
  const Schema& schema = tbl.GetSchema();
  std::vector<ColumnStats> columns;
  columns.reserve(schema.ColumnCount());
  for (slot_t column = 0; column < schema.ColumnCount(); ++column) {
    StatusOr<std::string_view> payload =
        statistics_.Read(ctx.txn_, StatisticsColumnKey(schema_name, column));
    if (!payload.HasValue()) {
      columns.emplace_back(schema.GetColumn(column).Type());
      continue;
    }
    ColumnStats stats;
    Deserialize(payload.Value(), stats);
    columns.push_back(std::move(stats));
  }
  ts.Assign(rows, std::move(columns));
  return ts;
}

Status Database::UpdateStatistics(TransactionContext& ctx,
                                  std::string_view schema_name,
                                  const TableStatistics& ts) {
  return WriteSplitStatistics(statistics_, ctx.txn_, schema_name, ts);
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
