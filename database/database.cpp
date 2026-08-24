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
#include <cstring>
#include <memory>
#include <mutex>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
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

constexpr page_id_t kDefaultTableRoot = 1;
constexpr page_id_t kDefaultStatisticsRoot = 2;
constexpr page_id_t kDefaultFunctionRoot = 3;

Database::Database(std::string_view dbname, size_t wal_sync_ms)
    : catalog_(kDefaultTableRoot),
      statistics_(kDefaultStatisticsRoot),
      functions_(kDefaultFunctionRoot),
      storage_(dbname, wal_sync_ms) {
  auto ctx = BeginContext();
  catalog_ = BPlusTree(ctx.txn_, kDefaultTableRoot);
  statistics_ = BPlusTree(ctx.txn_, kDefaultStatisticsRoot);
  functions_ = BPlusTree(ctx.txn_, kDefaultFunctionRoot);
  if (ctx.txn_.PreCommit() != Status::kSuccess) {
    // Let the embedder decide how to fail (the CLI main catches this);
    // exit(1) here would skip Logger fsync and PagePool cleanup.
    throw std::runtime_error("Failed to initialize relations: " +
                             std::string(dbname));
  }
}

std::ostream& operator<<(std::ostream& o, const Database& db) {
  o << "Database(storage=" << db.storage_
    << ", catalogs=<BPlusTree; use DebugDump(txn, o) for details>)";
  return o;
}

namespace {

template <typename Serializable>
std::string Serialize(const Serializable& from) {
  std::stringstream ss;
  Encoder arc(ss);
  arc << from;
  return ss.str();
}

// Returns false when the payload was truncated/corrupt; a partially decoded
// object must never leak out as a valid catalog entry.
template <typename Deserializable>
bool Deserialize(std::string_view from, Deserializable* dst) {
  std::string v(from);
  std::stringstream ss(v);
  Decoder ext(ss);
  ext >> *dst;
  // eofbit alone is fine (payload fully consumed); failbit/badbit mean a
  // short read or an oversized length field.
  return !ss.fail();
}

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
  uint64_t value = 0;
  DeserializeU64(payload.data(), &value);
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
  for (size_t column = 0; column < stats.Columns(); ++column) {
    RETURN_IF_FAIL(
        UpsertStatistics(tree, txn, StatisticsColumnKey(table_name,
                                                        static_cast<slot_t>(column)),
                         Serialize(stats.Column(column))));
  }
  return Status::kSuccess;
}

}  // namespace

StatusOr<Table> Database::CreateTable(TransactionContext& ctx,
                                      const Schema& schema) {
  // Serializes the existence check with the catalog insert within this
  // process. B+tree inserts reject duplicates, but by the time Insert fails
  // the table page (and any unique indexes) are already allocated and
  // cannot be reclaimed -- kSystemDestroyPage redo is unimplemented, so a
  // lost race would leak pages permanently. Close the race before the
  // allocation instead of compensating afterwards.
  std::scoped_lock lock(catalog_mu_);
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
  // Compiled plans may reference the previous catalog shape; drop them.
  BumpSchemaEpoch();
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
  RETURN_IF_FAIL(catalog_.Delete(ctx.txn_, schema_name));
  // KNOWN LIMITATION: the table's row/index pages stay allocated (space
  // leak). Reclaiming them needs kSystemDestroyPage redo support in the
  // recovery manager -- do NOT wire PageManager::DestroyPage here until that
  // lands, or a crash after DROP would make the next startup fail.
  // Invalidate cached images so later lookups observe the drop.
  ctx.tables_.erase(std::string(schema_name));
  ctx.stats_.erase(std::string(schema_name));
  BumpSchemaEpoch();
  return Status::kSuccess;
}

Status Database::CreateIndex(TransactionContext& ctx,
                             std::string_view schema_name,
                             const IndexSchema& idx) {
  ASSIGN_OR_RETURN(Table, tbl, GetTable(ctx, schema_name));
  RETURN_IF_FAIL(tbl.CreateIndex(ctx.txn_, idx));
  RETURN_IF_FAIL(catalog_.Update(ctx.txn_, schema_name, Serialize(tbl)));
  // Refresh the cached image so later inserts maintain the new index.
  ctx.tables_[std::string(schema_name)] =
      std::make_shared<Table>(std::move(tbl));
  // New access paths may change plan choices; invalidate compiled plans.
  BumpSchemaEpoch();
  return Status::kSuccess;
}

StatusOr<Function> Database::GetOrAddFunction(TransactionContext& ctx,
                                              std::string_view function_name,
                                              int argument_count) {
  // Same TOCTOU shape as CreateTable: hold the catalog mutex across the
  // Read→Insert pair so concurrent registrations cannot both take the
  // insert path.
  std::scoped_lock lock(catalog_mu_);
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
  if (!Deserialize(val.Value(), &func)) {
    return Status::kCorrupt;
  }
  return func;
}

StatusOr<Table> Database::GetTable(TransactionContext& ctx,
                                   std::string_view schema_name) {
  ASSIGN_OR_RETURN(std::string_view, val, catalog_.Read(ctx.txn_, schema_name));
  Table tbl;
  if (!Deserialize(val, &tbl)) {
    // A corrupt catalog entry must abort the lookup instead of handing out
    // a garbage schema.
    return Status::kCorrupt;
  }
  return tbl;
}

std::vector<std::string> Database::ListTables(TransactionContext& ctx) {
  std::vector<std::string> names;
  for (BPlusTreeIterator iter = catalog_.Begin(ctx.txn_); iter.IsValid();
       ++iter) {
    names.push_back(iter.Key());
  }
  return names;
}

[[maybe_unused]] void Database::DebugDump(Transaction& txn, std::ostream& o) {
  // FIXME(kumagi): The btree also has statistics entry.
  BPlusTreeIterator iter = catalog_.Begin(txn);
  while (iter.IsValid()) {
    Table tbl;
    if (!Deserialize(iter.Value(), &tbl)) {
      o << "(corrupt catalog entry)\n";
      ++iter;
      continue;
    }
    o << tbl << "\n";
    ++iter;
  }
}

StatusOr<TableStatistics> Database::GetStatistics(
    TransactionContext& ctx, std::string_view schema_name) {
  // Statistics are immutable between schema/statistics epochs. Query plans
  // ask for them on every fresh TransactionContext; reading the split
  // catalog records (and decoding the table schema first) repeatedly made
  // OLTP prepare spend as much time in metadata as in planning. Keep the
  // estimate cache thread-local so lookup is lock-free. A rare DDL/ANALYZE
  // race can only use a conservative old estimate for one epoch; execution
  // never depends on statistics for correctness.
  struct ThreadStatsCache {
    const Database* owner{nullptr};
    uint64_t epoch{0};
    std::unordered_map<std::string, TableStatistics> entries;
  };
  thread_local ThreadStatsCache cache;
  const uint64_t epoch = SchemaEpoch();
  if (cache.owner != this || cache.epoch != epoch) {
    cache.owner = this;
    cache.epoch = epoch;
    cache.entries.clear();
  }
  const std::string name(schema_name);
  if (const auto found = cache.entries.find(name);
      found != cache.entries.end()) {
    return found->second;
  }
  ASSIGN_OR_RETURN(Table, tbl, GetTable(ctx, schema_name));
  ASSIGN_OR_RETURN(std::string_view, meta,
                   statistics_.Read(ctx.txn_, schema_name));
  TableStatistics ts(tbl.GetSchema());
  if (PeekUint64(meta) != kStatisticsMetaMagic) {
    if (!Deserialize(meta, &ts)) {
      return Status::kCorrupt;
    }
    cache.entries.emplace(name, ts);
    return ts;
  }
  uint64_t magic = 0;
  uint64_t rows = 0;
  {
    std::string copy(meta);
    std::stringstream stream(copy);
    Decoder decoder(stream);
    decoder >> magic >> rows;
    if (stream.fail()) {
      return Status::kCorrupt;
    }
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
    if (!Deserialize(payload.Value(), &stats)) {
      return Status::kCorrupt;
    }
    columns.push_back(std::move(stats));
  }
  ts.Assign(rows, std::move(columns));
  cache.entries.emplace(name, ts);
  return ts;
}

Status Database::UpdateStatistics(TransactionContext& ctx,
                                  std::string_view schema_name,
                                  const TableStatistics& ts) {
  const Status updated =
      WriteSplitStatistics(statistics_, ctx.txn_, schema_name, ts);
  if (updated == Status::kSuccess) {
    // ANALYZE (and any other refresh) changes cost inputs for compiled plans.
    BumpSchemaEpoch();
  }
  return updated;
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
