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

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <ostream>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#ifdef __GLIBC__
#include <malloc.h>
#endif

#include "common/constants.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/serdes.hpp"
#include "common/status_or.hpp"
#include "database/page_storage.hpp"
#include "index/lsm_index.hpp"
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

namespace {

#ifdef __GLIBC__
// Engine heap tuning.  Join/sort/aggregation row stores repeatedly grow and
// free multi-hundred-MB blocks; with the default glibc thresholds every such
// cycle bounced through mmap/munmap (or heap trims via mprotect), costing
// kernel time and fresh page faults on each regrow.  Raising both thresholds
// keeps those blocks in the brk heap where freed space is recycled in place.
// Mirrors the tuning PostgreSQL applies for the same reason.  Sanitizer and
// non-glibc builds replace malloc entirely and ignore mallopt.
struct MallocTuning {
  MallocTuning() {
    mallopt(M_MMAP_THRESHOLD, 512 << 20);
    mallopt(M_TRIM_THRESHOLD, 512 << 20);
  }
};
// Intentional static-initializer side effect: applies the heap tuning
// thresholds once at process start, before any big block churns.
const MallocTuning g_malloc_tuning;  // NOLINT(cert-err58-cpp)
#endif

// Base names of every database this process opened through Create().
// Guards RemoveCreatedDatabaseFiles(); see the header comment.
std::mutex created_databases_mutex;
std::unordered_set<std::string> created_databases;

// GoogleSQL identifiers are case-insensitive; table-name comparison folds
// ASCII case the same way GetTable()'s catalog fallback does.
bool EqualsIgnoreCase(std::string_view left, std::string_view right) {
  return left.size() == right.size() &&
         std::equal(left.begin(), left.end(), right.begin(),
                    [](char a, char b) {
                      return std::tolower(static_cast<unsigned char>(a)) ==
                             std::tolower(static_cast<unsigned char>(b));
                    });
}

template <typename Cache>
void EraseCaseVariants(Cache& cache, std::string_view name) {
  std::erase_if(cache, [&](const auto& entry) {
    return EqualsIgnoreCase(entry.first, name);
  });
}

}  // namespace

StatusOr<std::unique_ptr<Database>> Database::Create(std::string_view dbname,
                                                     size_t wal_sync_ms,
                                                     bool preallocate_new_wal) {
  ASSIGN_OR_RETURN(
      std::unique_ptr<PageStorage>, storage,
      PageStorage::Create(dbname, wal_sync_ms, preallocate_new_wal));
  {
    const std::scoped_lock lock(created_databases_mutex);
    created_databases.insert(std::string(dbname));
  }
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
  auto database = std::unique_ptr<Database>(
      new Database(dbname, std::move(storage)));  // NOLINT
  auto ctx = database->BeginContext();
  ASSIGN_OR_RETURN(BPlusTree, catalog,
                   BPlusTree::Open(ctx.txn_, kDefaultTableRoot));
  database->catalog_ = catalog;
  ASSIGN_OR_RETURN(BPlusTree, statistics,
                   BPlusTree::Open(ctx.txn_, kDefaultStatisticsRoot));
  database->statistics_ = statistics;
  ASSIGN_OR_RETURN(BPlusTree, functions,
                   BPlusTree::Open(ctx.txn_, kDefaultFunctionRoot));
  database->functions_ = functions;
  RETURN_IF_FAIL(ctx.txn_.PreCommit());
  return database;
}

Database::Database(std::string_view /*dbname*/,
                   std::unique_ptr<PageStorage> storage)
    : catalog_(kDefaultTableRoot),
      statistics_(kDefaultStatisticsRoot),
      functions_(kDefaultFunctionRoot),
      storage_(std::move(storage)) {}

uint64_t Database::CacheHits() const noexcept {
  return storage_ && storage_->pm_ ? storage_->pm_->CacheHits() : 0;
}

uint64_t Database::CacheMisses() const noexcept {
  return storage_ && storage_->pm_ ? storage_->pm_->CacheMisses() : 0;
}

size_t Database::PinnedPageCount() const noexcept {
  return storage_ && storage_->pm_ ? storage_->pm_->PinnedPageCount() : 0;
}

std::ostream& operator<<(std::ostream& o, const Database& db) {
  o << "Database(storage=" << *db.storage_
    << ", catalogs=<BPlusTree; use DebugDump(txn, o) for details>)";
  return o;
}

namespace {

using TableDropHookState = std::shared_ptr<std::atomic<bool>>;

class TableDropFinishHook final : public TransactionFinishHook {
 public:
  TableDropFinishHook(std::shared_ptr<TableIncarnation> incarnation,
                      TableDropHookState state)
      : incarnation_(std::move(incarnation)), state_(std::move(state)) {}

  void OnTransactionFinished(TransactionStatus status) noexcept override {
    if (incarnation_ == nullptr ||
        !state_->exchange(false, std::memory_order_acq_rel)) {
      return;
    }
    if (status == TransactionStatus::kCommitted) {
      incarnation_->CommitDrop();
    } else if (status == TransactionStatus::kAborted) {
      incarnation_->AbortDrop();
    }
  }

 private:
  std::shared_ptr<TableIncarnation> incarnation_;
  TableDropHookState state_;
};

class TableDropRollback {
 public:
  TableDropRollback(Transaction* txn,
                    std::shared_ptr<TableIncarnation> incarnation,
                    TableDropHookState state)
      : txn_(txn),
        incarnation_(std::move(incarnation)),
        state_(std::move(state)) {}
  TableDropRollback(const TableDropRollback&) = delete;
  TableDropRollback& operator=(const TableDropRollback&) = delete;
  TableDropRollback(TableDropRollback&&) = delete;
  TableDropRollback& operator=(TableDropRollback&&) = delete;
  ~TableDropRollback() {
    if (active_) {
      txn_->MarkFailed();
      state_->store(false, std::memory_order_release);
      incarnation_->AbortDrop();
    }
  }
  void Dismiss() { active_ = false; }

 private:
  Transaction* txn_;
  std::shared_ptr<TableIncarnation> incarnation_;
  TableDropHookState state_;
  bool active_{true};
};

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
  try {
    ext >> *dst;
  } catch (const std::exception&) {
    // Corrupt catalog/statistics payloads (bad version, truncated legacy
    // image) must surface as `false`, not escape the StatusOr API.
    return false;
  }
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

// {magic, rows, layout_generation}. The generation is the clustering-proof
// stamp (TableStatistics::LayoutGeneration); readers tolerate its absence so a
// record written before v3 keeps working, and it decodes to 0 = "no proof".
std::string EncodeStatisticsMeta(size_t rows, uint64_t layout_generation) {
  std::stringstream stream;
  Encoder encoder(stream);
  encoder << kStatisticsMetaMagic << static_cast<uint64_t>(rows)
          << layout_generation;
  return stream.str();
}

uint64_t PeekUint64(std::string_view payload) {
  if (payload.size() < sizeof(uint64_t)) {
    return 0;
  }
  uint64_t value = 0;
  // Deserializes exactly sizeof(uint64_t) bytes; the size guard above proves
  // the buffer is large enough.
  // NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
  DeserializeU64(payload.data(), &value);
  return value;
}

Status UpsertStatistics(BPlusTree& tree, Transaction& txn, std::string_view key,
                        std::string_view value) {
  Status updated = tree.Update(txn, key, value);
  if (updated == Status::kNotExists) {
    return tree.Insert(txn, key, value);
  }
  return updated;
}

Status WriteSplitStatistics(BPlusTree& tree, Transaction& txn,
                            std::string_view table_name,
                            const TableStatistics& stats) {
  RETURN_IF_FAIL(UpsertStatistics(
      tree, txn, table_name,
      EncodeStatisticsMeta(stats.Rows(), stats.LayoutGeneration())));
  for (size_t column = 0; column < stats.Columns(); ++column) {
    RETURN_IF_FAIL(UpsertStatistics(
        tree, txn, StatisticsColumnKey(table_name, static_cast<slot_t>(column)),
        Serialize(stats.Column(column))));
  }
  return Status::kSuccess;
}

}  // namespace

StatusOr<Table> Database::CreateTable(TransactionContext& ctx,
                                      const Schema& schema) {
  // Serializes the existence check with the catalog insert within this
  // process. B+tree inserts reject duplicates; by the time Insert fails the
  // table page is already allocated, so close the race before the allocation
  // instead of compensating afterwards. (D3 made page reclaim crash-safe,
  // but the pre-check avoids allocating a page per losing CREATE TABLE.)
  std::scoped_lock lock(catalog_mu_);
  // Existence must be checked case-insensitively: GetTable() resolves names
  // with a case-insensitive fallback, so an exact-match-only check here let
  // "CREATE TABLE foo" succeed while "Foo" already existed, creating an
  // ambiguous catalog.
  {
    auto read_result = catalog_.Read(ctx.txn_, schema.Name());
    bool exists = read_result.HasValue();
    if (!exists && read_result.GetStatus() == Status::kNotExists) {
      const std::string wanted(schema.Name());
      for (BPlusTreeIterator iter = catalog_.Begin(ctx.txn_); iter.IsValid();
           ++iter) {
        const std::string& name = iter.Key();
        if (name.size() == wanted.size() &&
            std::equal(name.begin(), name.end(), wanted.begin(),
                       [](char a, char b) {
                         return std::tolower(static_cast<unsigned char>(a)) ==
                                std::tolower(static_cast<unsigned char>(b));
                       })) {
          exists = true;
          break;
        }
      }
    }
    if (exists) {
      return Status::kConflicts;
    }
    const Status read_status = read_result.GetStatus();
    if (read_status != Status::kNotExists) {
      // A corrupt or unreadable catalog entry is not "table already
      // exists"; surface the real failure instead of a misleading
      // kConflicts.
      return read_status;
    }
  }
  // FOREIGN KEY validation: every kForeign column must name an existing
  // parent table/column of the same type.  A self-reference resolves
  // against the schema being created (the table does not exist yet).
  // Uniqueness of the parent column is intentionally not required:
  // enforcement is an existence check, which stays well-defined with
  // duplicate parent keys.
  for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
    const Column& child = schema.GetColumn(i);
    const Constraint& constraint = child.GetConstraint();
    if (constraint.ctype != Constraint::kForeign) {
      continue;
    }
    const std::string child_name = child.Name().name;
    std::string parent_table;
    std::vector<std::string> parent_columns;
    if (!ParseForeignKeyReference(constraint, &parent_table, &parent_columns) ||
        parent_columns.size() != 1) {
      return Status(
          StatusCode::kInvalidArgument,
          "FOREIGN KEY on column " + child_name + " names an unknown or " +
              "multi-column reference; only single-column references of "
              "the form parent(col) are supported");
    }
    const Schema* parent_schema = &schema;
    Table parent_table_obj;
    if (!EqualsIgnoreCase(parent_table, schema.Name())) {
      StatusOr<Table> parent_result = GetTable(ctx, parent_table);
      if (!parent_result.HasValue()) {
        if (parent_result.GetStatus() == Status::kNotExists) {
          std::string message = "FOREIGN KEY on column " + child_name;
          message += " references missing table ";
          message += parent_table;
          return Status(StatusCode::kInvalidArgument, message);
        }
        return parent_result.GetStatus();
      }
      parent_table_obj = parent_result.MoveValue();
      parent_schema = &parent_table_obj.GetSchema();
    }
    const int parent_offset =
        parent_schema->Offset(ColumnName("", parent_columns[0]));
    if (parent_offset < 0) {
      std::string message = "FOREIGN KEY on column " + child_name;
      message += " references missing column ";
      message += parent_table;
      message += ".";
      message += parent_columns[0];
      return Status(StatusCode::kInvalidArgument, message);
    }
    const Column& parent_column =
        parent_schema->GetColumn(static_cast<size_t>(parent_offset));
    if (parent_column.Type() != child.Type()) {
      std::string message = "FOREIGN KEY on column " + child_name;
      message += " has a type mismatch with ";
      message += parent_table;
      message += ".";
      message += parent_columns[0];
      return Status(StatusCode::kInvalidArgument, message);
    }
  }
  ASSIGN_OR_RETURN(
      PageRef, table_page,
      storage_->pm_->AllocateNewPage(ctx.txn_, PageType::kRowPage));
  Table new_table(schema, table_page->PageID(), SchemaEpoch());
  TableStatistics new_stat(schema);
  // CreateIndex full-scans the table and reacquires this page latch.
  table_page.PageUnlock();
  // A failure after the allocation must not leak the fresh table page or
  // any already-built index tree: unwind them the way DropTable would.  No
  // rows were inserted, so the row-page chain is the single initial page.
  bool catalog_inserted = false;
  const auto unwind_allocation = [&](Status failure) -> Status {
    // After the catalog row landed, DropTable-equivalent unwinding must also
    // remove the catalog entry and any statistics rows that were written,
    // otherwise the catalog keeps serving a table whose storage is back on
    // the free list.
    if (catalog_inserted) {
      std::ignore = catalog_.Delete(ctx.txn_, schema.Name());
      for (slot_t column = 0; column < schema.ColumnCount(); ++column) {
        std::ignore = statistics_.Delete(
            ctx.txn_, StatisticsColumnKey(schema.Name(), column));
      }
      std::ignore = statistics_.Delete(ctx.txn_, schema.Name());
    }
    for (size_t i = 0; i < new_table.IndexCount(); ++i) {
      std::ignore = LsmIndex(new_table.GetIndex(i).Root()).Destroy(ctx.txn_);
    }
    {
      auto page = storage_->pm_->GetPage(new_table.first_pid_);
      if (page.HasValue()) {
        storage_->pm_->DestroyPage(ctx.txn_, &*page.Value());
        page.Value().PageUnlock();
      }
    }
    return failure;
  };

  // Prepare index for primary-key and unique-key.
  for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
    const Column& col = schema.GetColumn(i);
    if (col.GetConstraint().IsUnique()) {
      std::stringstream idx_name_stream;
      idx_name_stream << schema.Name() << "|" << col.Name();
      IndexSchema new_idx(idx_name_stream.str(), {i}, {});
      // A failed unique-index build (oversized key, conflicts) must fail the
      // CREATE TABLE instead of silently registering an unconstrained table.
      const Status index_status = new_table.CreateIndex(ctx.txn_, new_idx);
      if (index_status != Status::kSuccess) {
        return unwind_allocation(index_status);
      }
    }
  }

  {
    const Status insert_status =
        catalog_.Insert(ctx.txn_, schema.Name(), Serialize(new_table));
    if (insert_status != Status::kSuccess) {
      return unwind_allocation(insert_status);
    }
    catalog_inserted = true;
  }
  {
    const Status stats_status =
        WriteSplitStatistics(statistics_, ctx.txn_, schema.Name(), new_stat);
    if (stats_status != Status::kSuccess) {
      return unwind_allocation(stats_status);
    }
  }
  {
    std::scoped_lock incarnation_lock(table_incarnations_mu_);
    table_incarnations_[std::string(schema.Name())] =
        new_table.incarnation_state_;
  }
  // Compiled plans may reference the previous catalog shape; drop them.
  BumpSchemaEpoch();
  return new_table;
}

Status Database::DropTable(TransactionContext& ctx,
                           std::string_view schema_name) {
  std::string canonical;
  std::shared_ptr<TableIncarnation> incarnation;
  std::unique_ptr<TableDropRollback> rollback;
  {
    std::scoped_lock lock(catalog_mu_);
    ASSIGN_OR_RETURN(Table, tbl, GetTable(ctx, schema_name));
    canonical = tbl.GetSchema().Name();
    for (BPlusTreeIterator iter = catalog_.Begin(ctx.txn_); iter.IsValid();
         ++iter) {
      if (EqualsIgnoreCase(iter.Key(), canonical)) {
        continue;
      }
      Table other;
      if (!Deserialize(iter.Value(), &other)) {
        return Status::kCorrupt;
      }
      const Schema& other_schema = other.GetSchema();
      for (slot_t i = 0; i < other_schema.ColumnCount(); ++i) {
        const Constraint& constraint =
            other_schema.GetColumn(i).GetConstraint();
        if (constraint.ctype != Constraint::kForeign) {
          continue;
        }
        std::string parent_table;
        std::vector<std::string> parent_columns;
        if (!ParseForeignKeyReference(constraint, &parent_table,
                                      &parent_columns)) {
          continue;
        }
        if (EqualsIgnoreCase(parent_table, canonical)) {
          return StatusError(StatusCode::kConflicts,
                             "cannot drop table " + canonical +
                                 " because table " +
                                 std::string(other_schema.Name()) +
                                 " has a FOREIGN KEY referencing it");
        }
      }
    }
    incarnation = tbl.incarnation_state_;
    if (incarnation == nullptr || !incarnation->BeginDrop()) {
      return Status::kConflicts;
    }
    auto hook_state = std::make_shared<std::atomic<bool>>(true);
    rollback =
        std::make_unique<TableDropRollback>(&ctx.txn_, incarnation, hook_state);
    ctx.txn_.AddFinishHook(
        std::make_shared<TableDropFinishHook>(incarnation, hook_state));
    ctx.txn_.MarkTableDrop();
    BumpSchemaEpoch();
  }
  if (!incarnation->WaitForQuiescence(ctx.txn_.ID())) {
    return Status::kConflicts;
  }
  ASSIGN_OR_RETURN(Table, preflight, GetTable(ctx, canonical));
  RETURN_IF_FAIL(storage_->pm_->WaitForTableTailQuiescence(preflight.first_pid_,
                                                           ctx.txn_.ID()));

  std::scoped_lock lock(catalog_mu_);
  ASSIGN_OR_RETURN(Table, current, GetTable(ctx, canonical));
  if (current.incarnation_state_ != incarnation) {
    return Status::kConflicts;
  }
  const Schema& schema = current.GetSchema();
  std::vector<page_id_t> index_roots;
  index_roots.reserve(current.IndexCount());
  for (size_t index = 0; index < current.IndexCount(); ++index) {
    index_roots.push_back(current.GetIndex(index).Root());
  }
  auto index_write_guards = ctx.txn_.AcquireIndexWriteGuards(index_roots);
  for (slot_t column = 0; column < schema.ColumnCount(); ++column) {
    const Status deleted =
        statistics_.Delete(ctx.txn_, StatisticsColumnKey(canonical, column));
    if (deleted != Status::kSuccess && deleted != Status::kNotExists) {
      return deleted;
    }
  }
  {
    const Status deleted = statistics_.Delete(ctx.txn_, canonical);
    if (deleted != Status::kSuccess && deleted != Status::kNotExists) {
      return deleted;
    }
  }
  RETURN_IF_FAIL(catalog_.Delete(ctx.txn_, canonical));
  page_id_t pid = current.first_pid_;
  while (pid != 0) {
    ASSIGN_OR_RETURN(PageRef, page, storage_->pm_->GetPage(pid));
    if (!page.IsValid()) {
      break;
    }
    const page_id_t next = page->body.row_page.next_page_id_;
    RETURN_IF_FAIL(storage_->pm_->DestroyPage(ctx.txn_, &*page));
    page.PageUnlock();
    pid = next;
  }
  for (size_t i = 0; i < current.IndexCount(); ++i) {
    RETURN_IF_FAIL(LsmIndex(current.GetIndex(i).Root()).Destroy(ctx.txn_));
  }
  storage_->pm_->ForgetTableTail(current.first_pid_);
  EraseCaseVariants(ctx.tables_, canonical);
  EraseCaseVariants(ctx.stats_, canonical);
  rollback->Dismiss();
  return Status::kSuccess;
}

std::string MaterializedViewKey(std::string_view name) {
  std::string key(Database::kMaterializedViewKeyPrefix);
  for (char c : name) {
    key.push_back(
        static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  return key;
}

Status Database::PutMaterializedViewDefinition(TransactionContext& ctx,
                                               std::string_view name,
                                               std::string_view query_text) {
  std::scoped_lock lock(catalog_mu_);
  const std::string key = MaterializedViewKey(name);
  const Status existing = functions_.Delete(ctx.txn_, key);
  if (existing != Status::kSuccess && existing != Status::kNotExists) {
    return existing;
  }
  return functions_.Insert(ctx.txn_, key, std::string(query_text));
}

StatusOr<std::string> Database::GetMaterializedViewDefinition(
    TransactionContext& ctx, std::string_view name) {
  StatusOr<std::string> value =
      functions_.Read(ctx.txn_, MaterializedViewKey(name));
  if (value.GetStatus() != Status::kSuccess &&
      value.GetStatus() != Status::kNotExists) {
    return value.GetStatus();
  }
  if (value.GetStatus() == Status::kNotExists) {
    return Status::kNotExists;
  }
  return value.MoveValue();
}

Status Database::DeleteMaterializedViewDefinition(TransactionContext& ctx,
                                                  std::string_view name) {
  std::scoped_lock lock(catalog_mu_);
  const Status deleted = functions_.Delete(ctx.txn_, MaterializedViewKey(name));
  if (deleted != Status::kSuccess && deleted != Status::kNotExists) {
    return deleted;
  }
  return Status::kSuccess;
}

std::string ViewKey(std::string_view name) {
  std::string key(Database::kViewKeyPrefix);
  for (char c : name) {
    key.push_back(
        static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  return key;
}

// The name index is one record: newline-joined lowercased view names.
// Views are DDL-rare, so a read-modify-write under catalog_mu_ is cheap and
// avoids needing a range-scan primitive on the functions tree.
StatusOr<std::vector<std::string>> DecodeViewIndex(std::string_view record) {
  std::vector<std::string> names;
  size_t start = 0;
  while (start <= record.size()) {
    const size_t newline = record.find('\n', start);
    const std::string_view piece = newline == std::string_view::npos
                                       ? record.substr(start)
                                       : record.substr(start, newline - start);
    if (!piece.empty()) {
      names.emplace_back(piece);
    }
    if (newline == std::string_view::npos) {
      break;
    }
    start = newline + 1;
  }
  return names;
}

std::string EncodeViewIndex(const std::vector<std::string>& names) {
  std::string record;
  for (const std::string& name : names) {
    record += name;
    record += '\n';
  }
  return record;
}

Status WriteViewIndex(BPlusTree& functions, TransactionContext& ctx,
                      const std::vector<std::string>& names) {
  const Status deleted =
      functions.Delete(ctx.txn_, std::string(Database::kViewIndexKey));
  if (deleted != Status::kSuccess && deleted != Status::kNotExists) {
    return deleted;
  }
  return functions.Insert(ctx.txn_, std::string(Database::kViewIndexKey),
                          EncodeViewIndex(names));
}

Status Database::PutViewDefinition(TransactionContext& ctx,
                                   std::string_view name,
                                   std::string_view query_text) {
  std::scoped_lock lock(catalog_mu_);
  const std::string key = ViewKey(name);
  const Status existing = functions_.Delete(ctx.txn_, key);
  if (existing != Status::kSuccess && existing != Status::kNotExists) {
    return existing;
  }
  const Status inserted =
      functions_.Insert(ctx.txn_, key, std::string(query_text));
  if (inserted != Status::kSuccess) {
    return inserted;
  }
  std::string lowered(name);
  for (char& c : lowered) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  std::vector<std::string> names;
  StatusOr<std::string> index =
      functions_.Read(ctx.txn_, std::string(kViewIndexKey));
  if (index.HasValue()) {
    StatusOr<std::vector<std::string>> decoded = DecodeViewIndex(index.Value());
    if (decoded.HasValue()) {
      names = std::move(decoded.Value());
    }
  }
  if (std::ranges::find(names, lowered) == names.end()) {
    names.push_back(std::move(lowered));
  }
  return WriteViewIndex(functions_, ctx, names);
}

StatusOr<std::string> Database::GetViewDefinition(TransactionContext& ctx,
                                                  std::string_view name) {
  StatusOr<std::string> value = functions_.Read(ctx.txn_, ViewKey(name));
  if (value.GetStatus() != Status::kSuccess &&
      value.GetStatus() != Status::kNotExists) {
    return value.GetStatus();
  }
  if (value.GetStatus() == Status::kNotExists) {
    return Status::kNotExists;
  }
  return value.MoveValue();
}

Status Database::DeleteViewDefinition(TransactionContext& ctx,
                                      std::string_view name) {
  std::scoped_lock lock(catalog_mu_);
  const Status deleted = functions_.Delete(ctx.txn_, ViewKey(name));
  if (deleted != Status::kSuccess && deleted != Status::kNotExists) {
    return deleted;
  }
  std::string lowered(name);
  for (char& c : lowered) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  StatusOr<std::string> index =
      functions_.Read(ctx.txn_, std::string(kViewIndexKey));
  if (!index.HasValue()) {
    return Status::kSuccess;
  }
  StatusOr<std::vector<std::string>> decoded = DecodeViewIndex(index.Value());
  if (!decoded.HasValue()) {
    return Status::kSuccess;
  }
  std::erase(decoded.Value(), lowered);
  return WriteViewIndex(functions_, ctx, decoded.Value());
}

StatusOr<std::vector<std::string>> Database::ListViewDefinitions(
    TransactionContext& ctx) {
  StatusOr<std::string> index =
      functions_.Read(ctx.txn_, std::string(kViewIndexKey));
  if (index.GetStatus() == Status::kNotExists) {
    return std::vector<std::string>{};
  }
  if (!index.HasValue()) {
    return index.GetStatus();
  }
  return DecodeViewIndex(index.Value());
}

Status Database::RenameTable(TransactionContext& ctx,
                             std::string_view from_name,
                             std::string_view to_name) {
  std::scoped_lock lock(catalog_mu_);
  ASSIGN_OR_RETURN(Table, tbl, GetTable(ctx, from_name));
  const std::string canonical(tbl.GetSchema().Name());
  const std::string target(to_name);
  if (target.empty()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "RENAME TO requires a non-empty name");
  }
  if (EqualsIgnoreCase(target, canonical)) {
    // Renaming to the same (case-variant) name keeps the canonical spelling
    // stable; nothing to move.
    return Status::kSuccess;
  }
  // Target existence must be checked case-insensitively (same reasoning as
  // CreateTable).
  for (BPlusTreeIterator iter = catalog_.Begin(ctx.txn_); iter.IsValid();
       ++iter) {
    const std::string& name = iter.Key();
    if (name.size() == target.size() &&
        std::equal(name.begin(), name.end(), target.begin(),
                   [](char a, char b) {
                     return std::tolower(static_cast<unsigned char>(a)) ==
                            std::tolower(static_cast<unsigned char>(b));
                   })) {
      return StatusError(StatusCode::kConflicts,
                         "table " + target + " already exists");
    }
  }
  // A FOREIGN KEY stores its parent table name as a
  // string; renaming the parent would silently dangle it (DropTable refuses
  // for the same reason).
  for (BPlusTreeIterator iter = catalog_.Begin(ctx.txn_); iter.IsValid();
       ++iter) {
    Table other;
    if (!Deserialize(iter.Value(), &other)) {
      return Status::kCorrupt;
    }
    const Schema& other_schema = other.GetSchema();
    for (slot_t i = 0; i < other_schema.ColumnCount(); ++i) {
      const Constraint& constraint = other_schema.GetColumn(i).GetConstraint();
      if (constraint.ctype != Constraint::kForeign) {
        continue;
      }
      std::string parent_table;
      std::vector<std::string> parent_columns;
      if (!ParseForeignKeyReference(constraint, &parent_table,
                                    &parent_columns)) {
        continue;
      }
      if (EqualsIgnoreCase(parent_table, canonical)) {
        return StatusError(StatusCode::kConflicts,
                           "cannot rename table " + canonical +
                               " because table " +
                               std::string(other_schema.Name()) +
                               " has a FOREIGN KEY referencing it");
      }
    }
  }
  Table renamed = tbl;
  renamed.schema_ = tbl.GetSchema().WithName(target);
  // Resolve statistics while the old catalog name still exists. Looking
  // them up after deleting it made migration depend on a warm thread cache.
  ASSIGN_OR_RETURN(TableStatistics, renamed_stats,
                   GetStatistics(ctx, canonical));
  {
    std::scoped_lock incarnation_lock(table_incarnations_mu_);
    auto state = table_incarnations_.find(canonical);
    if (state != table_incarnations_.end()) {
      table_incarnations_[target] = state->second;
      table_incarnations_.erase(state);
    }
  }
  RETURN_IF_FAIL(catalog_.Insert(ctx.txn_, target, Serialize(renamed)));
  RETURN_IF_FAIL(catalog_.Delete(ctx.txn_, canonical));
  // Statistics entries are keyed by the canonical table name; carry them to
  // the new name so ANALYZE results survive a rename.
  {
    RETURN_IF_FAIL(
        WriteSplitStatistics(statistics_, ctx.txn_, target, renamed_stats));
    for (size_t column = 0; column < renamed_stats.Columns(); ++column) {
      const Status deleted = statistics_.Delete(
          ctx.txn_,
          StatisticsColumnKey(canonical, static_cast<slot_t>(column)));
      if (deleted != Status::kSuccess && deleted != Status::kNotExists) {
        return deleted;
      }
    }
    const Status deleted = statistics_.Delete(ctx.txn_, canonical);
    if (deleted != Status::kSuccess && deleted != Status::kNotExists) {
      return deleted;
    }
  }
  EraseCaseVariants(ctx.tables_, canonical);
  EraseCaseVariants(ctx.stats_, canonical);
  EraseCaseVariants(ctx.tables_, target);
  EraseCaseVariants(ctx.stats_, target);
  BumpSchemaEpoch();
  return Status::kSuccess;
}

Status Database::RenameColumn(TransactionContext& ctx,
                              std::string_view table_name,
                              std::string_view old_column,
                              std::string_view new_column) {
  std::scoped_lock lock(catalog_mu_);
  ASSIGN_OR_RETURN(Table, tbl, GetTable(ctx, table_name));
  const std::string canonical(tbl.GetSchema().Name());
  const ColumnName wanted("", old_column);
  const int offset = tbl.GetSchema().Offset(wanted);
  if (offset < 0) {
    return StatusError(
        StatusCode::kNotExists,
        "column " + std::string(old_column) + " not found in " + canonical);
  }
  for (size_t i = 0; i < tbl.GetSchema().ColumnCount(); ++i) {
    if (std::cmp_equal(i, offset)) {
      continue;
    }
    if (EqualsIgnoreCase(tbl.GetSchema().GetColumn(i).Name().name,
                         new_column)) {
      return StatusError(StatusCode::kConflicts,
                         "column " + std::string(new_column) +
                             " already exists in " + canonical);
    }
  }
  // Reference payloads name parent columns. Match RenameTable's policy:
  // reject a rename that would leave another constraint dangling.
  for (BPlusTreeIterator iter = catalog_.Begin(ctx.txn_); iter.IsValid();
       ++iter) {
    Table other;
    if (!Deserialize(iter.Value(), &other)) {
      return Status::kCorrupt;
    }
    const Schema& other_schema = other.GetSchema();
    for (size_t i = 0; i < other_schema.ColumnCount(); ++i) {
      const Column& column = other_schema.GetColumn(i);
      std::string parent_table;
      std::vector<std::string> parent_columns;
      if (!ParseForeignKeyReference(column.GetConstraint(), &parent_table,
                                    &parent_columns) ||
          !EqualsIgnoreCase(parent_table, canonical)) {
        continue;
      }
      if (!EqualsIgnoreCase(old_column, new_column) &&
          (parent_columns.empty() ||
           std::ranges::any_of(parent_columns, [&](const std::string& name) {
             return EqualsIgnoreCase(name, old_column);
           }))) {
        return StatusError(StatusCode::kConflicts,
                           "cannot rename column " + std::string(old_column) +
                               " because a FOREIGN KEY references it");
      }
    }
  }
  Table renamed = tbl;
  renamed.schema_ = tbl.GetSchema().WithRenamedColumn(
      static_cast<size_t>(offset), new_column);
  RETURN_IF_FAIL(catalog_.Update(ctx.txn_, canonical, Serialize(renamed)));
  EraseCaseVariants(ctx.tables_, canonical);
  EraseCaseVariants(ctx.stats_, canonical);
  BumpSchemaEpoch();
  return Status::kSuccess;
}

Status Database::CreateIndex(TransactionContext& ctx,
                             std::string_view schema_name,
                             const IndexSchema& idx) {
  std::scoped_lock lock(catalog_mu_);
  ASSIGN_OR_RETURN(Table, tbl, GetTable(ctx, schema_name));
  // Serialize catalog/cache updates under the canonical name: a differently
  // cased reference used to write a second, unreachable catalog entry.
  const std::string canonical(tbl.GetSchema().Name());
  tbl.incarnation_ = SchemaEpoch();
  RETURN_IF_FAIL(tbl.CreateIndex(ctx.txn_, idx));
  RETURN_IF_FAIL(catalog_.Update(ctx.txn_, canonical, Serialize(tbl)));
  // Refresh the cached image so later inserts maintain the new index.
  EraseCaseVariants(ctx.tables_, canonical);
  ctx.tables_[canonical] = std::make_shared<Table>(std::move(tbl));
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
  StatusOr<std::string> val = functions_.Read(ctx.txn_, function_name);
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
  const auto bind_incarnation = [&](Table& table) {
    std::scoped_lock incarnation_lock(table_incarnations_mu_);
    auto& state = table_incarnations_[std::string(table.GetSchema().Name())];
    if (state == nullptr) {
      state = std::make_shared<TableIncarnation>();
    }
    table.SetIncarnationState(state);
  };
  auto read_result = catalog_.Read(ctx.txn_, schema_name);
  if (read_result.HasValue()) {
    Table tbl;
    if (!Deserialize(read_result.Value(), &tbl)) {
      // A corrupt catalog entry must abort the lookup instead of handing
      // out a garbage schema.
      return Status::kCorrupt;
    }
    bind_incarnation(tbl);
    return tbl;
  }
  // GoogleSQL identifiers are case-insensitive; fall back to a
  // case-insensitive scan of the catalog before giving up.
  for (BPlusTreeIterator iter = catalog_.Begin(ctx.txn_); iter.IsValid();
       ++iter) {
    const std::string& name = iter.Key();
    if (name.size() == schema_name.size() &&
        std::equal(name.begin(), name.end(), schema_name.begin(),
                   [](char a, char b) {
                     return std::tolower(static_cast<unsigned char>(a)) ==
                            std::tolower(static_cast<unsigned char>(b));
                   })) {
      Table tbl;
      if (!Deserialize(iter.Value(), &tbl)) {
        return Status::kCorrupt;
      }
      bind_incarnation(tbl);
      return tbl;
    }
  }
  return read_result.GetStatus();
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
  // Statistics live under the canonical (declared) table name; a differently
  // cased reference used to miss the record and fail the whole lookup.
  const std::string canonical(tbl.GetSchema().Name());
  ASSIGN_OR_RETURN(std::string, meta, statistics_.Read(ctx.txn_, canonical));
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
  uint64_t layout_generation = 0;
  {
    std::stringstream stream(meta);
    Decoder decoder(stream);
    decoder >> magic >> rows;
    if (stream.fail()) {
      return Status::kCorrupt;
    }
    // Pre-v3 records stop after `rows`: they certify no physical order.
    if (!decoder.AtEnd()) {
      decoder >> layout_generation;
      if (stream.fail()) {
        return Status::kCorrupt;
      }
    }
  }
  const Schema& schema = tbl.GetSchema();
  std::vector<ColumnStats> columns;
  columns.reserve(schema.ColumnCount());
  for (slot_t column = 0; column < schema.ColumnCount(); ++column) {
    // Column keys live under the canonical name too; using the caller's raw
    // case missed every column record for a differently-cased reference and
    // silently substituted empty ColumnStats.
    StatusOr<std::string> payload =
        statistics_.Read(ctx.txn_, StatisticsColumnKey(canonical, column));
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
  ts.Assign(rows, std::move(columns), layout_generation);
  cache.entries.emplace(name, ts);
  return ts;
}

Status Database::UpdateStatistics(TransactionContext& ctx,
                                  std::string_view schema_name,
                                  const TableStatistics& ts) {
  // Statistics keys follow the canonical (declared) table name, exactly like
  // the readers (GetStatistics) and DropTable; writing under the caller's
  // raw spelling left the refresh invisible for differently-cased references.
  ASSIGN_OR_RETURN(Table, tbl, GetTable(ctx, schema_name));
  const std::string canonical(tbl.GetSchema().Name());
  const Status updated =
      WriteSplitStatistics(statistics_, ctx.txn_, canonical, ts);
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

void Database::EmulateCrash() { storage_->DiscardAllUpdates(); }

StatusOr<lsn_t> Database::WriteCheckpoint() {
  return storage_->cm_->WriteCheckpoint();
}

namespace {
// Removes the WAL segment series `<wal_base>.<index>` for one database,
// where `wal_base` is the database's `.log` path. Segments are contiguous
// from index 0 (recycling only ever trims a prefix), so the first missing
// file ends the sweep.
void RemoveWalSegmentFiles(const std::filesystem::path& wal_base) {
  const std::filesystem::path directory = wal_base.has_parent_path()
                                              ? wal_base.parent_path()
                                              : std::filesystem::path(".");
  const std::string prefix = wal_base.filename().string() + ".";
  std::error_code ec;
  std::filesystem::directory_iterator it(directory, ec);
  if (ec) {
    return;
  }
  for (const std::filesystem::directory_entry& entry : it) {
    const std::string filename = entry.path().filename().string();
    if (!filename.starts_with(prefix)) {
      continue;
    }
    const std::string suffix = filename.substr(prefix.size());
    if (!suffix.empty() && std::ranges::all_of(suffix, [](char c) {
          return c >= '0' && c <= '9';
        })) {
      std::filesystem::remove(entry.path(), ec);
    }
  }
}
}  // namespace

void Database::RemoveCreatedDatabaseFiles() {
  std::unordered_set<std::string> names;
  {
    const std::scoped_lock lock(created_databases_mutex);
    names = created_databases;
  }
  for (const std::string& name : names) {
    // Same file set as DeleteAll(): the page file, the WAL segment series,
    // the checkpoint marker and its stranded temp sibling. Missing entries
    // are the norm (not every database reaches every stage) and simply
    // ignored.
    std::ignore = std::remove((name + ".db").c_str());
    std::ignore = std::remove((name + ".db.ckpt").c_str());
    std::ignore = std::remove((name + ".db.ckpt.tmp").c_str());
    std::ignore = std::remove((name + ".log").c_str());
    std::ignore = std::remove((name + ".log.meta").c_str());
    std::ignore = std::remove((name + ".log.meta.tmp").c_str());
    RemoveWalSegmentFiles(name + ".log");
    std::ignore = std::remove((name + ".last_checkpoint").c_str());
    std::ignore = std::remove((name + ".last_checkpoint.tmp").c_str());
  }
}

void Database::DeleteAll() {
  if (storage_ == nullptr) {
    return;
  }
  EmulateCrash();
  const std::string database_name = storage_->DBName();
  const std::string log_name = storage_->LogName();
  const std::string master_record_name = storage_->MasterRecordName();
  storage_.reset();
  std::ignore = std::remove(database_name.c_str());
  std::ignore = std::remove((database_name + ".ckpt").c_str());
  std::ignore = std::remove((database_name + ".ckpt.tmp").c_str());
  std::ignore = std::remove(log_name.c_str());
  std::ignore = std::remove((log_name + ".meta").c_str());
  std::ignore = std::remove((log_name + ".meta.tmp").c_str());
  RemoveWalSegmentFiles(log_name);
  std::ignore = std::remove(master_record_name.c_str());
  std::ignore = std::remove((master_record_name + ".tmp").c_str());
}

}  // namespace tinylamb
