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
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <map>
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
#include "table/tlcs_file.hpp"
#include "transaction/transaction.hpp"
#include "type/column.hpp"
#include "type/function.hpp"
#include "type/schema.hpp"

namespace tinylamb {

constexpr page_id_t kDefaultTableRoot = 1;
constexpr page_id_t kDefaultStatisticsRoot = 2;
constexpr page_id_t kDefaultFunctionRoot = 3;

namespace {

bool AutoTlcsEnabled() noexcept {
  const char* env = std::getenv("TINYLAMB_AUTO_TLCS");
  if (env == nullptr || *env == '\0') {
    return true;
  }
  const std::string_view v(env);
  return v != "0" && v != "false" && v != "FALSE" && v != "off" && v != "OFF" &&
         v != "no" && v != "NO";
}

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
  // Register only after every fallible step succeeded: a failed Create() must
  // not leave a name behind that RemoveCreatedDatabaseFiles() keeps deleting,
  // potentially unlinking files a later process created under the same name.
  {
    const std::scoped_lock lock(created_databases_mutex);
    created_databases.insert(std::string(dbname));
  }
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
  return storage_ && storage_->pm_ ? PageManager::PinnedPageCount() : 0;
}

std::ostream& operator<<(std::ostream& o, const Database& db) {
  o << "Database(storage=" << *db.storage_
    << ", catalogs=<BPlusTree; use DebugDump(txn, o) for details>)";
  return o;
}

namespace {

std::string TlcsPathForTable(const std::string& db_file_path,
                             page_id_t first_pid) {
  return db_file_path + ".tlcs." + std::to_string(first_pid);
}

using TableDropHookState = std::shared_ptr<std::atomic<bool>>;

class TableDropFinishHook final : public TransactionFinishHook {
 public:
  TableDropFinishHook(std::shared_ptr<TableIncarnation> incarnation,
                      TableDropHookState state, std::string tlcs_path)
      : incarnation_(std::move(incarnation)),
        state_(std::move(state)),
        tlcs_path_(std::move(tlcs_path)) {}

  void OnTransactionFinished(TransactionStatus status) noexcept override {
    if (incarnation_ == nullptr ||
        !state_->exchange(false, std::memory_order_acq_rel)) {
      return;
    }
    if (status == TransactionStatus::kCommitted) {
      incarnation_->CommitDrop();
      incarnation_->SetBackingTLCSStoreRaw(nullptr);
      incarnation_->SetLSTableStoreRaw(nullptr);
      incarnation_->SetProjectedLSTableStoreRaw(nullptr);
      if (!tlcs_path_.empty()) {
        std::error_code ec;
        std::filesystem::remove(tlcs_path_, ec);
      }
    } else if (status == TransactionStatus::kAborted) {
      incarnation_->AbortDrop();
    }
  }

 private:
  std::shared_ptr<TableIncarnation> incarnation_;
  TableDropHookState state_;
  std::string tlcs_path_;
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
  // stringstream copies its input into its own buffer; build it directly
  // from the view instead of allocating an intermediate std::string.
  std::stringstream ss;
  ss.write(from.data(), static_cast<std::streamsize>(from.size()));
  ss.seekg(0);
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

constexpr uint64_t kStatisticsMetaMagic =
    0x544C53544D455441ULL;  // "TLSTMETA" (historical; do not change)

std::string StatisticsColumnKey(std::string_view table, slot_t column) {
  // slot_t is a 16-bit column id, so the two-byte big-endian suffix below is
  // exhaustive: there is no addressable column >= 65536 whose statistics key
  // could wrap onto an existing column's key.
  static_assert(sizeof(slot_t) == 2,
                "statistics key encodes a 16-bit column id");
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
// NOTE: unlike the TLSTATS payload this meta record has no explicit version;
// readers infer the presence of layout_generation from AtEnd(). Adding further
// fields requires a version bump here or old readers will silently drop them.
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
  // process. unique LSM index lookups reject duplicates; by the time Insert
  // fails the table page is already allocated, so close the race before the
  // allocation instead of compensating afterwards. (D3 made page reclaim
  // crash-safe, but the pre-check avoids allocating a page per losing CREATE
  // TABLE.)
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
      return StatusError(
          StatusCode::kConflicts,
          "table " + std::string(schema.Name()) + " already exists");
    }
    const Status read_status = read_result.GetStatus();
    if (read_status != Status::kNotExists) {
      // A corrupt or unreadable catalog entry is not "table already
      // exists"; surface the real failure instead of a misleading
      // kConflicts.
      return read_status;
    }
  }
  const auto view = GetViewDefinition(ctx, schema.Name());
  if (view.HasValue()) {
    return StatusError(
        StatusCode::kConflicts,
        "view " + std::string(schema.Name()) + " already exists");
  }
  if (view.GetStatus() != Status::kNotExists) {
    return view.GetStatus();
  }
  // A materialized view also owns its name in the functions store; FROM
  // resolution checks ViewRegistry first, so shadowing it would silently
  // hide the view.
  const auto mv = GetMaterializedViewDefinition(ctx, schema.Name());
  if (mv.HasValue()) {
    return StatusError(
        StatusCode::kConflicts,
        "materialized view " + std::string(schema.Name()) + " already exists");
  }
  if (mv.GetStatus() != Status::kNotExists) {
    return mv.GetStatus();
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
      // DROP has already checked for referencing tables when it changes
      // the incarnation to drop-pending and releases catalog_mu_ to wait.
      // Do not register a new reference during that wait.
      if (!parent_table_obj.AcquireLease(ctx.txn_.ID()).HasValue()) {
        return StatusError(
            StatusCode::kConflicts,
            "FOREIGN KEY references a table being dropped: " + parent_table);
      }
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
  {
    std::scoped_lock attach_lock(tlcs_attach_mu_);
    tlcs_probed_pids_.erase(new_table.first_pid_);
  }
  // A failure after the allocation must not leak the fresh table page or
  // any already-built index tree: unwind them the way DropTable would.  No
  // rows were inserted, so the row-page chain is the single initial page.
  bool catalog_inserted = false;
  const auto unwind_allocation = [&](Status failure) -> Status {
    const auto record_cleanup = [&](std::string_view operation,
                                    const Status& status) {
      if (status != Status::kSuccess) {
        LOG(WARN) << "CreateTable compensation " << operation
                  << " failed: " << status;
        ctx.txn_.MarkFailed();
      }
    };
    // Keep attempting every independent compensation. If any fails, prevent
    // commit so transaction abort can undo the original allocations and writes.
    if (catalog_inserted) {
      record_cleanup("catalog", catalog_.Delete(ctx.txn_, schema.Name()));
      for (slot_t column = 0; column < schema.ColumnCount(); ++column) {
        const Status status = statistics_.Delete(
            ctx.txn_, StatisticsColumnKey(schema.Name(), column));
        if (status != Status::kNotExists) {
          record_cleanup("column statistics", status);
        }
      }
      const Status status = statistics_.Delete(ctx.txn_, schema.Name());
      if (status != Status::kNotExists) {
        record_cleanup("table statistics", status);
      }
    }
    for (size_t i = 0; i < new_table.IndexCount(); ++i) {
      record_cleanup("index",
                     LsmIndex(new_table.GetIndex(i).Root()).Destroy(ctx.txn_));
    }
    auto page = storage_->pm_->GetPage(new_table.first_pid_);
    if (page.HasValue()) {
      // DestroyPage() takes blocks_mu_; drop the exclusive page latch from
      // GetPage() first to keep the documented lock order. This page was just
      // allocated by this txn and is not published to other writers.
      Page* target = &*page.Value();
      page.Value().PageUnlock();
      record_cleanup("table page",
                     storage_->pm_->DestroyPage(ctx.txn_, target));
    } else {
      record_cleanup("table page lookup", page.GetStatus());
    }
    return failure;
  };

  // Prepare index for primary-key and unique-key.
  for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
    const Column& col = schema.GetColumn(i);
    if (col.GetConstraint().IsUnique()) {
      // This is a descriptive label scoped to the owning table's catalog
      // entry (it is not a standalone catalog key): two tables whose
      // "table|column" strings happen to collide still keep separate indexes.
      std::string index_name(schema.Name());
      index_name.push_back('|');
      index_name += col.Name().ToString();
      IndexSchema new_idx(index_name, {i}, {});
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
  // The catalog write is transaction-local until commit: an abort rolls it
  // back without rewinding the epoch, so the txn must be flagged dirty for
  // the abort path to invalidate any plan/cache stamped from this view.
  ctx.txn_.MarkCatalogDirty();
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
    const std::string tlcs_path =
        tbl.FirstPageId() != 0
            ? TlcsPathForTable(storage_->DBName(), tbl.FirstPageId())
            : std::string();
    auto hook_state = std::make_shared<std::atomic<bool>>(true);
    rollback =
        std::make_unique<TableDropRollback>(&ctx.txn_, incarnation, hook_state);
    ctx.txn_.AddFinishHook(std::make_shared<TableDropFinishHook>(
        incarnation, hook_state, tlcs_path));
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
  std::unordered_set<page_id_t> seen;
  while (pid != 0) {
    // A corrupted next-page link can make the chain loop forever on the DDL
    // thread (which runs on the event loop), so refuse to follow a revisit.
    if (!seen.insert(pid).second) {
      return StatusError(StatusCode::kCorrupt,
                         "cycle detected while dropping table chain");
    }
    ASSIGN_OR_RETURN(PageRef, page, storage_->pm_->GetPage(pid));
    if (!page.IsValid()) {
      break;
    }
    const page_id_t next = page->body.row_page.next_page_id_;
    // GetPage() latches the page exclusively by default, while DestroyPage()
    // takes blocks_mu_. Release the latch first so we never hold a page latch
    // while acquiring blocks_mu_ (docs/lock_order.md). The quiescence checks
    // above keep other writers off this chain, and the PageRef still owns the
    // page while the raw pointer is passed to DestroyPage().
    Page* target = &*page;
    page.PageUnlock();
    RETURN_IF_FAIL(storage_->pm_->DestroyPage(ctx.txn_, target));
    pid = next;
  }
  for (size_t i = 0; i < current.IndexCount(); ++i) {
    RETURN_IF_FAIL(LsmIndex(current.GetIndex(i).Root()).Destroy(ctx.txn_));
  }
  storage_->pm_->ForgetTableTail(current.first_pid_);
  {
    std::scoped_lock attach_lock(tlcs_attach_mu_);
    tlcs_probed_pids_.erase(current.first_pid_);
  }
  EraseCaseVariants(ctx.tables_, canonical);
  EraseCaseVariants(ctx.stats_, canonical);
  rollback->Dismiss();
  return Status::kSuccess;
}

namespace {

std::string MaterializedViewKey(std::string_view name) {
  std::string key(Database::kMaterializedViewKeyPrefix);
  key.reserve(key.size() + name.size());
  for (char c : name) {
    key.push_back(
        static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  return key;
}

}  // namespace

Status Database::PutMaterializedViewDefinition(TransactionContext& ctx,
                                               std::string_view name,
                                               std::string_view query_text) {
  std::scoped_lock lock(catalog_mu_);
  const std::string key = MaterializedViewKey(name);
  Status existing = functions_.Delete(ctx.txn_, key);
  if (existing != Status::kSuccess && existing != Status::kNotExists) {
    return existing;
  }
  RETURN_IF_FAIL(functions_.Insert(ctx.txn_, key, std::string(query_text)));
  // The definition write is buffered in this txn until commit; an abort still
  // needs to invalidate templates/plans stamped from this catalog view.
  ctx.txn_.MarkCatalogDirty();
  BumpSchemaEpoch();
  return Status::kSuccess;
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
  Status deleted = functions_.Delete(ctx.txn_, MaterializedViewKey(name));
  if (deleted != Status::kSuccess && deleted != Status::kNotExists) {
    return deleted;
  }
  // Mirror PutMaterializedViewDefinition: stale definitions must not survive
  // in epoch-keyed view registration or compiled-plan caches.
  ctx.txn_.MarkCatalogDirty();
  BumpSchemaEpoch();
  return Status::kSuccess;
}

namespace {

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
  names.reserve(record.size());
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
  Status deleted =
      functions.Delete(ctx.txn_, std::string(Database::kViewIndexKey));
  if (deleted != Status::kSuccess && deleted != Status::kNotExists) {
    return deleted;
  }
  return functions.Insert(ctx.txn_, std::string(Database::kViewIndexKey),
                          EncodeViewIndex(names));
}

}  // namespace

Status Database::PutViewDefinition(TransactionContext& ctx,
                                   std::string_view name,
                                   std::string_view query_text,
                                   bool replace_existing) {
  std::scoped_lock lock(catalog_mu_);
  const auto table = GetTable(ctx, name);
  if (table.HasValue()) {
    return StatusError(StatusCode::kConflicts,
                       "table " + std::string(name) + " already exists");
  }
  if (table.GetStatus() != Status::kNotExists) {
    return table.GetStatus();
  }
  const auto matview = GetMaterializedViewDefinition(ctx, name);
  if (matview.HasValue()) {
    return StatusError(
        StatusCode::kConflicts,
        "materialized view " + std::string(name) + " already exists");
  }
  if (matview.GetStatus() != Status::kNotExists) {
    return matview.GetStatus();
  }
  const std::string key = ViewKey(name);
  const auto existing_view = functions_.Read(ctx.txn_, key);
  if (existing_view.HasValue()) {
    if (!replace_existing) {
      return StatusError(StatusCode::kConflicts,
                         "view " + std::string(name) + " already exists");
    }
  } else if (existing_view.GetStatus() != Status::kNotExists) {
    return existing_view.GetStatus();
  }
  std::string lowered(name);
  for (char& c : lowered) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  std::vector<std::string> names;
  StatusOr<std::string> index =
      functions_.Read(ctx.txn_, std::string(kViewIndexKey));
  if (index.HasValue()) {
    ASSIGN_OR_RETURN(std::vector<std::string>, decoded,
                     DecodeViewIndex(index.Value()));
    names = std::move(decoded);
  } else if (index.GetStatus() != Status::kNotExists) {
    return index.GetStatus();
  }
  Status existing = functions_.Delete(ctx.txn_, key);
  if (existing != Status::kSuccess && existing != Status::kNotExists) {
    return existing;
  }
  Status inserted = functions_.Insert(ctx.txn_, key, std::string(query_text));
  if (inserted != Status::kSuccess) {
    return inserted;
  }
  if (std::ranges::find(names, lowered) == names.end()) {
    names.push_back(std::move(lowered));
  }
  Status stored = WriteViewIndex(functions_, ctx, names);
  if (stored == Status::kSuccess) {
    // T485: the buffered catalog write is visible to this txn only until
    // commit; an Abort must invalidate caches stamped from it.
    ctx.txn_.MarkCatalogDirty();
  }
  return stored;
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
  std::string lowered(name);
  for (char& c : lowered) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  StatusOr<std::string> index =
      functions_.Read(ctx.txn_, std::string(kViewIndexKey));
  if (!index.HasValue() && index.GetStatus() != Status::kNotExists) {
    return index.GetStatus();
  }
  std::vector<std::string> names;
  if (index.HasValue()) {
    ASSIGN_OR_RETURN(std::vector<std::string>, decoded,
                     DecodeViewIndex(index.Value()));
    names = std::move(decoded);
  }
  Status deleted = functions_.Delete(ctx.txn_, ViewKey(name));
  if (deleted != Status::kSuccess && deleted != Status::kNotExists) {
    return deleted;
  }
  std::erase(names, lowered);
  Status stored = WriteViewIndex(functions_, ctx, names);
  if (stored == Status::kSuccess) {
    // T485: the buffered catalog write is visible to this txn only until
    // commit; an Abort must invalidate caches stamped from it.
    ctx.txn_.MarkCatalogDirty();
  }
  return stored;
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
  // Views and materialized views share the object-name namespace with tables
  // during FROM resolution (ViewRegistry is consulted first).
  const auto view = GetViewDefinition(ctx, target);
  if (view.HasValue()) {
    return StatusError(StatusCode::kConflicts,
                       "view " + target + " already exists");
  }
  if (view.GetStatus() != Status::kNotExists) {
    return view.GetStatus();
  }
  const auto matview = GetMaterializedViewDefinition(ctx, target);
  if (matview.HasValue()) {
    return StatusError(StatusCode::kConflicts,
                       "materialized view " + target + " already exists");
  }
  if (matview.GetStatus() != Status::kNotExists) {
    return matview.GetStatus();
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
  // A materialized view is a table plus a defining-text record under the old
  // name. Keep them in sync: migrate the definition with the rename so DROP
  // MATERIALIZED VIEW keeps working on the new name and a new object can take
  // the old name.
  StatusOr<std::string> mv_definition =
      GetMaterializedViewDefinition(ctx, canonical);
  if (mv_definition.GetStatus() != Status::kSuccess &&
      mv_definition.GetStatus() != Status::kNotExists) {
    return mv_definition.GetStatus();
  }
  const bool has_mv_definition = mv_definition.HasValue();
  const std::string mv_text =
      has_mv_definition ? std::move(mv_definition.Value()) : std::string();
  RETURN_IF_FAIL(catalog_.Insert(ctx.txn_, target, Serialize(renamed)));
  RETURN_IF_FAIL(catalog_.Delete(ctx.txn_, canonical));
  if (has_mv_definition) {
    // Directly use functions_ here: the Put/Delete helpers take catalog_mu_,
    // which RenameTable already holds (non-recursive -> deadlock).
    RETURN_IF_FAIL(
        functions_.Insert(ctx.txn_, MaterializedViewKey(target), mv_text));
    const Status removed_mv =
        functions_.Delete(ctx.txn_, MaterializedViewKey(canonical));
    if (removed_mv != Status::kSuccess && removed_mv != Status::kNotExists) {
      return removed_mv;
    }
  }
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
  // This map is procedural (not transactional): only rebind the incarnation
  // after every fallible catalog/statistics update has succeeded, so a failure
  // cannot leave the new name bound while the WAL rolls the catalog back.
  {
    std::scoped_lock incarnation_lock(table_incarnations_mu_);
    auto state = table_incarnations_.find(canonical);
    if (state != table_incarnations_.end()) {
      table_incarnations_[target] = state->second;
      table_incarnations_.erase(state);
    }
  }
  EraseCaseVariants(ctx.tables_, canonical);
  EraseCaseVariants(ctx.stats_, canonical);
  EraseCaseVariants(ctx.tables_, target);
  EraseCaseVariants(ctx.stats_, target);
  // Like CreateIndex: the catalog/stats rename is buffered in this txn. If it
  // aborts, the epoch stays advanced while the catalog reverts to the old
  // name, so plans built with the new name must be invalidated on abort.
  ctx.txn_.MarkCatalogDirty();
  BumpSchemaEpoch();
  return Status::kSuccess;
}

Status Database::RenameColumn(TransactionContext& ctx,
                              std::string_view table_name,
                              std::string_view old_column,
                              std::string_view new_column) {
  if (new_column.empty()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "RENAME COLUMN requires a non-empty new name");
  }
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
  ctx.txn_.MarkCatalogDirty();
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
  ctx.txn_.MarkCatalogDirty();
  // Refresh the cached image so later inserts maintain the new index.
  EraseCaseVariants(ctx.tables_, canonical);
  ctx.tables_[canonical] = std::make_shared<Table>(std::move(tbl));
  // New access paths may change plan choices; invalidate compiled plans.
  BumpSchemaEpoch();
  return Status::kSuccess;
}

// #1100: see declaration.  Locking is kept strictly inside the per-database
// incarnation map; TableIncarnation::IsLive takes its own lock afterwards
// (docs/lock_order.md: no nesting between the two).
bool Database::IsCurrentIncarnation(
    std::string_view canonical_name,
    const std::shared_ptr<TableIncarnation>& state) const {
  if (state == nullptr) {
    return false;
  }
  bool bound = false;
  {
    std::scoped_lock incarnation_lock(table_incarnations_mu_);
    auto it = table_incarnations_.find(canonical_name);
    bound = it != table_incarnations_.end() && it->second == state;
  }
  return bound && state->IsLive();
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
    // A new signature can change plan validity and the thread-local UDF
    // registry; invalidate caches stamped from the pre-registration epoch.
    ctx.txn_.MarkCatalogDirty();
    BumpSchemaEpoch();
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
    {
      std::scoped_lock incarnation_lock(table_incarnations_mu_);
      auto it = table_incarnations_.find(table.GetSchema().Name());
      if (it == table_incarnations_.end()) {
        it = table_incarnations_
                 .emplace(std::string(table.GetSchema().Name()), nullptr)
                 .first;
      }
      auto& state = it->second;
      if (state == nullptr) {
        state = std::make_shared<TableIncarnation>();
      }
      table.SetIncarnationState(state);
    }
    MaybeAttachBackingTlcs(table);
  };
  auto read_result = catalog_.ReadVisible(ctx.txn_, schema_name);
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
  if (read_result.GetStatus() != Status::kNotExists) {
    return read_result.GetStatus();
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
      iter.EnableStableLeafCache();
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
  // Note: catalog_ is walked here; statistics live in a separate tree.
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
  struct NameHash {
    using is_transparent = void;
    size_t operator()(std::string_view name) const noexcept {
      return std::hash<std::string_view>{}(name);
    }
  };
  struct ThreadStatsCache {
    const Database* owner{nullptr};
    uint64_t epoch{0};
    // The read below goes through ctx.txn_'s snapshot. Keying only on the
    // live epoch lets a stale-snapshot txn plant its old statistics under a
    // fresh epoch, which later txns would then keep reading.
    uint64_t snapshot{0};
    std::unordered_map<std::string, TableStatistics, NameHash, std::equal_to<>>
        entries;
  };
  thread_local ThreadStatsCache cache;
  const uint64_t epoch = SchemaEpoch();
  const uint64_t snapshot = ctx.txn_.SnapshotTimestamp();
  if (cache.owner != this || cache.epoch != epoch ||
      cache.snapshot != snapshot) {
    cache.owner = this;
    cache.epoch = epoch;
    cache.snapshot = snapshot;
    cache.entries.clear();
  }
  if (const auto found = cache.entries.find(schema_name);
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
    cache.entries.emplace(schema_name, ts);
    return ts;
  }
  uint64_t magic = 0;
  uint64_t rows = 0;
  uint64_t layout_generation = 0;
  {
    std::istringstream stream(std::move(meta));
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
  cache.entries.emplace(schema_name, ts);
  return ts;
}

Status Database::UpdateStatistics(TransactionContext& ctx,
                                  std::string_view schema_name,
                                  const TableStatistics& ts) {
  std::scoped_lock lock(catalog_mu_);
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
    // The statistics write is buffered in the caller's transaction until
    // commit. An abort restores the old statistics, so caches stamped with
    // the new epoch must be invalidated via the catalog-dirty rollback path
    // (same contract as CreateIndex / catalog writes).
    ctx.txn_.MarkCatalogDirty();
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

Database::~Database() {
  std::vector<std::shared_ptr<TableIncarnation>> incarnations;
  {
    std::scoped_lock incarnation_lock(table_incarnations_mu_);
    for (auto& [name, incarnation] : table_incarnations_) {
      if (incarnation != nullptr) {
        incarnations.push_back(incarnation);
      }
    }
  }
  // Release the registry lock before paying the (potentially large) cost of
  // unmapping/returning columnar stores; scans keep their own references.
  for (auto& incarnation : incarnations) {
    incarnation->DropColumnarCaches();
  }
}

void Database::EmulateCrash() { storage_->DiscardAllUpdates(); }

StatusOr<lsn_t> Database::WriteCheckpoint() {
  if (AutoTlcsEnabled() && storage_ != nullptr && storage_->pm_ != nullptr &&
      !auto_tlcs_in_progress_.exchange(true, std::memory_order_acq_rel)) {
    struct ResetGuard {
      std::atomic<bool>& flag;
      ~ResetGuard() { flag.store(false, std::memory_order_release); }
    } guard{auto_tlcs_in_progress_};
    TransactionContext ctx = BeginContext();
    ctx.txn_.SweepCommittedVersionChains();
    if (!ctx.txn_.HasAnyPageVersionChains()) {
      const std::vector<std::string> tables = ListTables(ctx);
      for (const std::string& name : tables) {
        if (ctx.txn_.HasAnyPageVersionChains()) {
          break;
        }
        (void)TryAutoCompactTable(ctx, name);
      }
    }
    (void)ctx.PreCommit();
  }
  return storage_->cm_->WriteCheckpoint();
}

namespace {

bool StubChainMatchesTlcs(PageManager& pm, page_id_t first_pid,
                          const TlcsFile& tlcs) {
  if (first_pid == 0 || first_pid != tlcs.FirstPageId() ||
      tlcs.BlockCount() == 0) {
    return false;
  }
  page_id_t pid = first_pid;
  uint64_t blocks_seen = 0;
  uint64_t rows_seen = 0;
  std::unordered_set<page_id_t> seen;
  seen.reserve(tlcs.BlockCount());
  while (pid != 0) {
    if (!seen.insert(pid).second || blocks_seen >= tlcs.BlockCount()) {
      return false;
    }
    StatusOr<PageRef> page = pm.GetPage(pid, /*shared=*/true);
    if (!page.HasValue() || page.Value().IsNull() ||
        page.Value()->Type() != PageType::kRowPage) {
      return false;
    }
    const RowPage& rp = page.Value()->body.row_page;
    if (!rp.IsColumnarStub()) {
      return false;
    }
    const page_id_t next_pid = rp.next_page_id_;
    const slot_t rc = rp.RowCount();
    page.Value().PageUnlock();
    rows_seen += rc;
    ++blocks_seen;
    pid = next_pid;
  }
  return blocks_seen == tlcs.BlockCount() && rows_seen == tlcs.TotalRows();
}

}  // namespace

void Database::MaybeAttachBackingTlcs(Table& table) {
  const page_id_t first_pid = table.FirstPageId();
  if (first_pid == 0 || table.GetBackingTlcsStore() != nullptr ||
      storage_ == nullptr || storage_->pm_ == nullptr) {
    return;
  }
  std::scoped_lock attach_lock(tlcs_attach_mu_);
  if (table.GetBackingTlcsStore() != nullptr ||
      tlcs_probed_pids_.contains(first_pid)) {
    return;
  }
  bool is_stub = false;
  {
    StatusOr<PageRef> page = storage_->pm_->GetPage(first_pid, /*shared=*/true);
    if (!page.HasValue() || page.Value().IsNull() ||
        page.Value()->Type() != PageType::kRowPage) {
      return;
    }
    is_stub = page.Value()->body.row_page.IsColumnarStub();
    // Release the page latch before inspecting or mapping `.tlcs` files.
    page.Value().PageUnlock();
  }
  if (is_stub) {
    const std::string path = TlcsPathForTable(storage_->DBName(), first_pid);
    std::error_code ec;
    if (std::filesystem::exists(path, ec) && !ec) {
      StatusOr<std::shared_ptr<TlcsFile>> opened =
          TlcsFile::Open(path, table.GetSchema());
      if (opened.HasValue()) {
        std::shared_ptr<TlcsFile> tlcs = opened.MoveValue();
        const uint64_t gen = table.LayoutGeneration();
        if (gen != 0 && !table.RowsRewrittenInPlace() &&
            StubChainMatchesTlcs(*storage_->pm_, first_pid, *tlcs) &&
            table.LayoutGeneration() == gen) {
          tlcs->BindLayoutGeneration(gen);
        }
        table.SetBackingTLCSStore(std::move(tlcs));
      } else {
        LOG(WARN) << "Failed to open backing tlcs file " << path << ": "
                  << opened.GetStatus();
      }
    }
  }
  tlcs_probed_pids_.insert(first_pid);
}

Status Database::CompactTableToTlcs(TransactionContext& ctx,
                                    std::string_view schema_name,
                                    bool write_checkpoint,
                                    bool refresh_statistics,
                                    bool non_blocking_quiescence) {
  ASSIGN_OR_RETURN(Table, tbl, GetTable(ctx, schema_name));
  if (tbl.FirstPageId() == 0) {
    return Status::kNotExists;
  }
  const std::shared_ptr<TableIncarnation> incarnation = tbl.incarnation_state_;
  if (incarnation == nullptr) {
    return Status::kInvalidArgument;
  }

  std::shared_ptr<const TlcsFile> existing_tlcs = tbl.GetBackingTlcsStore();
  if (existing_tlcs != nullptr &&
      existing_tlcs->FormatVersion() >= TlcsFile::kFormatVersion) {
    if (existing_tlcs->LayoutGeneration() == tbl.LayoutGeneration() &&
        existing_tlcs->FirstPageId() == tbl.FirstPageId()) {
      return Status::kSuccess;
    }
    bool all_stub_pages = true;
    size_t chain_count = 0;
    std::unordered_set<page_id_t> seen;
    page_id_t pid = tbl.FirstPageId();
    while (pid != 0) {
      if (!seen.insert(pid).second) {
        return StatusError(StatusCode::kCorrupt,
                           "cycle detected while inspecting table chain");
      }
      ASSIGN_OR_RETURN(PageRef, page,
                       storage_->pm_->GetPage(pid, /*shared=*/true));
      if (page.IsNull() || page->Type() != PageType::kRowPage) {
        return StatusError(StatusCode::kCorrupt,
                           "invalid page type during tlcs compaction check");
      }
      const RowPage& rp = page->body.row_page;
      const page_id_t next_pid = rp.next_page_id_;
      if (!rp.IsColumnarStub()) {
        if (existing_tlcs->TotalRows() != 0 || pid != tbl.FirstPageId() ||
            next_pid != 0 || rp.RowMax() != 0) {
          all_stub_pages = false;
          break;
        }
      }
      ++chain_count;
      pid = next_pid;
    }
    if (all_stub_pages &&
        ((existing_tlcs->TotalRows() == 0 && chain_count == 1) ||
         chain_count == existing_tlcs->BlockCount())) {
      return Status::kSuccess;
    }
  }

  const uint64_t before_gen = tbl.LayoutGeneration();
  const std::string final_path =
      TlcsPathForTable(storage_->DBName(), tbl.FirstPageId());
  const std::string temp_path = final_path + ".tmp";

  if (ctx.txn_.IsReadOnly() && tbl.IndexCount() > 0) {
    return StatusError(StatusCode::kInvalidArgument,
                       "cannot compact table with secondary indexes in a "
                       "read-only transaction");
  }
  if (non_blocking_quiescence && ctx.txn_.HasAnyPageVersionChains()) {
    return Status::kConflicts;
  }

  ASSIGN_OR_RETURN(
      PendingTlcsCommit, pending,
      WriteTlcsTempFile(ctx.txn_, tbl, existing_tlcs.get(), temp_path));

  struct ResumeTableGuard {
    std::shared_ptr<TableIncarnation> state;
    bool active{false};
    ResumeTableGuard(std::shared_ptr<TableIncarnation> s, bool a)
        : state(std::move(s)), active(a) {}
    ResumeTableGuard(const ResumeTableGuard&) = delete;
    ResumeTableGuard& operator=(const ResumeTableGuard&) = delete;
    ResumeTableGuard(ResumeTableGuard&&) = delete;
    ResumeTableGuard& operator=(ResumeTableGuard&&) = delete;
    ~ResumeTableGuard() {
      if (active && state != nullptr) {
        state->AbortDrop();
      }
    }
  };
  if (!incarnation->BeginDrop()) {
    return Status::kConflicts;
  }
  ResumeTableGuard resume_guard(incarnation, true);

  if (!incarnation->WaitForQuiescence(ctx.txn_.ID(), non_blocking_quiescence)) {
    return Status::kConflicts;
  }
  if (tbl.LayoutGeneration() != before_gen) {
    return Status::kConflicts;
  }
  RETURN_IF_FAIL(storage_->pm_->WaitForTableTailQuiescence(tbl.FirstPageId(),
                                                           ctx.txn_.ID()));
  if (tbl.LayoutGeneration() != before_gen) {
    return Status::kConflicts;
  }
  // P356/426: a stub serves every row as one current TLCS image.  Demoting a
  // page that still carries registered MVCC version chains -- an open
  // snapshot, or a rewrite by a writer that quiescence did not drain (an
  // idle-between-statements multi-statement transaction holds no lease) --
  // would erase that history and make pinned reads jump to the current
  // image.  Refuse this compaction round instead; the next one runs once the
  // chains retire.
  for (page_id_t chain_pid : pending.old_pids) {
    if (ctx.txn_.HasPageVersionChains(chain_pid)) {
      return Status::kConflicts;
    }
  }

  auto rebuild_indexes = [&](Transaction& index_txn,
                             const TlcsFile& tlcs) -> Status {
    const size_t num_indexes = tbl.IndexCount();
    if (num_indexes == 0) {
      return Status::kSuccess;
    }
    std::vector<std::map<std::string, std::string>> single_maps(num_indexes);
    std::vector<std::map<std::string, std::vector<IndexValueType>>> multi_maps(
        num_indexes);
    RETURN_IF_FAIL(tlcs.ForEachBlockRow(
        tbl.GetSchema(),
        [&](page_id_t pid, slot_t slot, const Row& row) -> Status {
          const RowPosition pos(pid, slot);
          for (size_t i = 0; i < num_indexes; ++i) {
            const Index& idx = tbl.GetIndex(i);
            const bool key_has_null = std::ranges::any_of(
                idx.sc_.key_,
                [&row](const slot_t col) { return row[col].IsNull(); });
            std::vector<Value> include_vals;
            include_vals.reserve(idx.sc_.include_.size());
            for (const slot_t col : idx.sc_.include_) {
              include_vals.push_back(row[col]);
            }
            const std::string key = idx.GenerateKey(row);
            IndexValueType entry{pos, Row(std::move(include_vals))};
            if (idx.StoresSingleValue() && !key_has_null) {
              single_maps[i].emplace(key, Encode(entry));
            } else {
              multi_maps[i][key].push_back(std::move(entry));
            }
          }
          return Status::kSuccess;
        }));
    for (size_t i = 0; i < num_indexes; ++i) {
      for (auto& [k, vec] : multi_maps[i]) {
        single_maps[i].emplace(k, Encode(vec));
      }
      multi_maps[i].clear();
    }

    if (index_txn.HasForeignIndexReservations()) {
      // T3894: a kConflicts drain timeout used to be discarded with (void).
      // Rebuilding the whole key set while a foreign transaction still holds
      // an index-undo reservation races that txn's commit/abort replay and
      // can drop entries or leave ghosts behind, so fail the compaction
      // (the ResumeTableGuard above unwinds it) instead of rebuilding
      // on a stale canonical set.
      RETURN_IF_FAIL(index_txn.WaitAllIndexReservationsDrain(
          std::chrono::milliseconds(250)));
    }
    std::vector<page_id_t> index_roots;
    index_roots.reserve(num_indexes);
    for (size_t i = 0; i < num_indexes; ++i) {
      index_roots.push_back(tbl.GetIndex(i).Root());
    }
    auto index_guards = index_txn.AcquireIndexWriteGuards(index_roots);
    for (size_t i = 0; i < num_indexes; ++i) {
      LsmIndex lsm(tbl.GetIndex(i).Root());
      LsmIndexIterator existing = lsm.Begin(index_txn, "", "");
      auto tit = single_maps[i].begin();
      while (existing.IsValid() && tit != single_maps[i].end()) {
        const std::string cur_key = existing.Key();
        if (cur_key < tit->first) {
          RETURN_IF_FAIL(lsm.Delete(index_txn, cur_key));
          ++existing;
        } else if (tit->first < cur_key) {
          RETURN_IF_FAIL(lsm.Insert(index_txn, tit->first, tit->second));
          ++tit;
        } else {
          if (existing.Value() != tit->second) {
            RETURN_IF_FAIL(lsm.Update(index_txn, cur_key, tit->second));
          }
          ++existing;
          ++tit;
        }
      }
      while (existing.IsValid()) {
        RETURN_IF_FAIL(lsm.Delete(index_txn, existing.Key()));
        ++existing;
      }
      while (tit != single_maps[i].end()) {
        RETURN_IF_FAIL(lsm.Insert(index_txn, tit->first, tit->second));
        ++tit;
      }
      single_maps[i].clear();
    }
    return Status::kSuccess;
  };

  const size_t target_count =
      std::max<size_t>(1, pending.block_row_counts.size());
  lsn_t compaction_lsn = pending.max_chain_lsn;
  std::shared_ptr<TlcsFile> opened;
  if (target_count <= pending.old_pids.size()) {
    const std::vector<page_id_t> planned_stub_pids(
        pending.old_pids.begin(),
        pending.old_pids.begin() + static_cast<ptrdiff_t>(target_count));
    RETURN_IF_FAIL(
        FinalizeTlcsTempFile(pending, planned_stub_pids, before_gen + 1));

    const bool use_internal_txn =
        ctx.txn_.IsReadOnly() || !ctx.txn_.HasLogRecord();
    if (use_internal_txn) {
      TransactionContext compact_ctx = BeginContext();
      if (tbl.IndexCount() > 0) {
        ASSIGN_OR_RETURN(std::shared_ptr<TlcsFile>, temp_tlcs,
                         TlcsFile::Open(pending.temp_path, tbl.GetSchema()));
        RETURN_IF_FAIL(rebuild_indexes(compact_ctx.txn_, *temp_tlcs));
      }
      for (size_t j = target_count; j < pending.old_pids.size(); ++j) {
        RETURN_IF_FAIL(
            compact_ctx.txn_.DestroyPageLog(pending.old_pids[j]).GetStatus());
      }
      if (compact_ctx.txn_.HasLogRecord()) {
        RETURN_IF_FAIL(compact_ctx.txn_.PreCommit());
      }
    } else {
      if (tbl.IndexCount() > 0) {
        ASSIGN_OR_RETURN(std::shared_ptr<TlcsFile>, temp_tlcs,
                         TlcsFile::Open(pending.temp_path, tbl.GetSchema()));
        RETURN_IF_FAIL(rebuild_indexes(ctx.txn_, *temp_tlcs));
      }
      for (size_t j = target_count; j < pending.old_pids.size(); ++j) {
        RETURN_IF_FAIL(
            ctx.txn_.DestroyPageLog(pending.old_pids[j]).GetStatus());
      }
    }

    compaction_lsn = std::max(compaction_lsn, storage_->logger_->BufferedLSN());
    ASSIGN_OR_RETURN(std::vector<page_id_t>, stub_pids,
                     storage_->pm_->ReplaceTableChainWithColumnarStubs(
                         tbl.FirstPageId(), pending.old_pids,
                         pending.block_row_counts, compaction_lsn));
    (void)stub_pids;
    incarnation->BumpLayoutGeneration();

    // Release any old mmap/store reservation before mapping the new `.tlcs`
    // file.
    tbl.SetBackingTLCSStore(nullptr);
    existing_tlcs.reset();

    // The page chain already points at the stubs: the finalized temp file is
    // now the only row-image copy. A rename or reopen failure must keep it
    // on disk for recovery instead of deleting it through the destructor.
    if (Status rename_st = RenameFinalizedTlcsTemp(pending, final_path);
        !rename_st.ok()) {
      pending.KeepTempFileOnDisk();
      return rename_st;
    }
    auto opened_or = TlcsFile::Open(final_path, tbl.GetSchema());
    if (!opened_or.HasValue()) {
      pending.KeepTempFileOnDisk();
      return opened_or.GetStatus();
    }
    opened = opened_or.MoveValue();
  } else {
    compaction_lsn = std::max(compaction_lsn, storage_->logger_->BufferedLSN());
    ASSIGN_OR_RETURN(std::vector<page_id_t>, stub_pids,
                     storage_->pm_->ReplaceTableChainWithColumnarStubs(
                         tbl.FirstPageId(), pending.old_pids,
                         pending.block_row_counts, compaction_lsn));
    incarnation->BumpLayoutGeneration();
    const uint64_t final_gen = incarnation->LayoutGeneration();
    if (Status finalize_st =
            FinalizeTlcsFile(pending, stub_pids, final_gen, final_path);
        !finalize_st.ok()) {
      pending.KeepTempFileOnDisk();
      return finalize_st;
    }

    tbl.SetBackingTLCSStore(nullptr);
    existing_tlcs.reset();
    auto opened_or = TlcsFile::Open(final_path, tbl.GetSchema());
    if (!opened_or.HasValue()) {
      pending.KeepTempFileOnDisk();
      return opened_or.GetStatus();
    }
    opened = opened_or.MoveValue();
    if (tbl.IndexCount() > 0) {
      TransactionContext compact_ctx = BeginContext();
      Transaction& idx_txn = (ctx.txn_.IsReadOnly() || !ctx.txn_.HasLogRecord())
                                 ? compact_ctx.txn_
                                 : ctx.txn_;
      RETURN_IF_FAIL(rebuild_indexes(idx_txn, *opened));
      if (&idx_txn == &compact_ctx.txn_ && compact_ctx.txn_.HasLogRecord()) {
        RETURN_IF_FAIL(compact_ctx.txn_.PreCommit());
      }
    }
  }

  tbl.SetBackingTLCSStore(opened);
  {
    std::scoped_lock attach_lock(tlcs_attach_mu_);
    tlcs_probed_pids_.insert(tbl.FirstPageId());
  }

  incarnation->AbortDrop();
  resume_guard.active = false;

  if (write_checkpoint) {
    RETURN_IF_FAIL(storage_->cm_->WriteCheckpoint().GetStatus());
  }

  if (refresh_statistics && !ctx.txn_.IsReadOnly()) {
    RETURN_IF_FAIL(RefreshStatistics(ctx, schema_name));
  } else {
    BumpSchemaEpoch();
  }
  return Status::kSuccess;
}

bool Database::TryAutoCompactTable(TransactionContext& ctx,
                                   std::string_view schema_name) {
  if (storage_ == nullptr || storage_->pm_ == nullptr ||
      !ctx.txn_.WriteSetEmpty() || ctx.txn_.HasLogRecord()) {
    return false;
  }
  ctx.txn_.SweepCommittedVersionChains();
  if (ctx.txn_.HasAnyPageVersionChains()) {
    return false;
  }
  auto tbl_or = GetTable(ctx, schema_name);
  if (!tbl_or.HasValue()) {
    return false;
  }
  const Table& tbl = tbl_or.Value();
  const page_id_t first_pid = tbl.FirstPageId();
  if (first_pid == 0 || !LSTableStore::SupportsSchema(tbl.GetSchema())) {
    return false;
  }
  if (const auto existing = tbl.GetBackingTlcsStore();
      existing != nullptr &&
      existing->LayoutGeneration() == tbl.LayoutGeneration() &&
      existing->FirstPageId() == first_pid) {
    return false;
  }
  {
    StatusOr<PageRef> first_page =
        storage_->pm_->GetPage(first_pid, /*shared=*/true);
    if (!first_page.HasValue() || first_page.Value().IsNull() ||
        first_page.Value()->Type() != PageType::kRowPage) {
      return false;
    }
    const RowPage& rp = first_page.Value()->body.row_page;
    const bool empty_uncompacted =
        !rp.IsColumnarStub() && rp.next_page_id_ == 0 && rp.RowMax() == 0;
    if (rp.IsColumnarStub() || empty_uncompacted) {
      return false;
    }
  }
  const Status st = CompactTableToTlcs(
      ctx, schema_name, /*write_checkpoint=*/false,
      /*refresh_statistics=*/false, /*non_blocking_quiescence=*/true);
  if (st == Status::kSuccess) {
    return true;
  }
  if (st != Status::kConflicts && st != Status::kNotImplemented &&
      st != Status::kInvalidArgument && st != Status::kMemoryLimitExceeded) {
    LOG(WARN) << "Auto-TLCS skipped for " << schema_name << ": " << st;
  }
  return false;
}

bool Database::AutoCompactTableForAnalyze(TransactionContext& ctx,
                                          std::string_view schema_name) {
  const char* analyze_tlcs = std::getenv("TINYLAMB_ANALYZE_AUTO_TLCS");
  if (analyze_tlcs == nullptr || analyze_tlcs[0] == '\0' ||
      analyze_tlcs[0] == '0') {
    return false;
  }
  if (!AutoTlcsEnabled()) {
    return false;
  }
  return TryAutoCompactTable(ctx, schema_name);
}

Status Database::CompactAllTablesToTlcs() {
  if (storage_ == nullptr || storage_->pm_ == nullptr) {
    return Status::kInvalidArgument;
  }
  std::vector<std::string> tables;
  {
    TransactionContext list_ctx = BeginContext();
    tables = ListTables(list_ctx);
    RETURN_IF_FAIL(list_ctx.PreCommit());
  }
  std::vector<std::string> compacted;
  for (const std::string& name : tables) {
    TransactionContext ctx = BeginContext();
    const Status st = CompactTableToTlcs(ctx, name, /*write_checkpoint=*/false,
                                         /*refresh_statistics=*/false,
                                         /*non_blocking_quiescence=*/false);
    if (st == Status::kNotImplemented) {
      (void)ctx.PreCommit();
      continue;
    }
    RETURN_IF_FAIL(st);
    RETURN_IF_FAIL(ctx.PreCommit());
    compacted.push_back(name);
  }
  RETURN_IF_FAIL(storage_->cm_->WriteCheckpoint().GetStatus());
  for (const std::string& name : compacted) {
    TransactionContext sctx = BeginContext();
    RETURN_IF_FAIL(RefreshStatistics(sctx, name));
    RETURN_IF_FAIL(sctx.PreCommit());
  }
  return Status::kSuccess;
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

namespace {
void RemoveCompanionStorageFiles(const std::string& db_file_path) {
  std::ignore = std::remove((db_file_path + ".blocks").c_str());
  std::ignore = std::remove((db_file_path + ".lsm").c_str());
  const std::filesystem::path p(db_file_path);
  const std::filesystem::path parent =
      p.parent_path().empty() ? std::filesystem::path(".") : p.parent_path();
  const std::string base_blocks = p.filename().string() + ".blocks.";
  const std::string base_lsm = p.filename().string() + ".lsm.";
  const std::string base_tlcs = p.filename().string() + ".tlcs";
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(parent, ec)) {
    if (ec) {
      break;
    }
    const std::string fname = entry.path().filename().string();
    if (fname.starts_with(base_blocks) || fname.starts_with(base_lsm) ||
        fname.starts_with(base_tlcs)) {
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
    // Same file set as DeleteAll(): the page file, companion block/tlcs
    // files, the WAL segment series, the checkpoint marker and its stranded
    // temp sibling. Missing entries are the norm and simply ignored.
    std::ignore = std::remove((name + ".db").c_str());
    // The snapshot temp is <db>.tmp (page_manager.cpp); the old .db.ckpt
    // names are kept only to clean up files from retired versions.
    std::ignore = std::remove((name + ".tmp").c_str());
    std::ignore = std::remove((name + ".db.ckpt").c_str());
    std::ignore = std::remove((name + ".db.ckpt.tmp").c_str());
    RemoveCompanionStorageFiles(name + ".db");
    RemoveCompanionStorageFiles(name);
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
  std::ignore = std::remove((database_name + ".tmp").c_str());
  std::ignore = std::remove((database_name + ".ckpt").c_str());
  std::ignore = std::remove((database_name + ".ckpt.tmp").c_str());
  RemoveCompanionStorageFiles(database_name);
  if (database_name.ends_with(".db")) {
    RemoveCompanionStorageFiles(
        database_name.substr(0, database_name.size() - 3));
  }
  std::ignore = std::remove(log_name.c_str());
  std::ignore = std::remove((log_name + ".meta").c_str());
  std::ignore = std::remove((log_name + ".meta.tmp").c_str());
  RemoveWalSegmentFiles(log_name);
  std::ignore = std::remove(master_record_name.c_str());
  std::ignore = std::remove((master_record_name + ".tmp").c_str());
}

}  // namespace tinylamb
