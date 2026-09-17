/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_FUZZ_SCOPED_DB_HPP
#define TINYLAMB_FUZZ_SCOPED_DB_HPP

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "index/index_schema.hpp"
#include "table/table.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// Throwaway database for one fuzz iteration: deletes the .db/.log/master
// files when the scope exits.  Without this every iteration strands a fresh
// file pair next to the fuzzer's working directory; long sweeps eventually
// exhaust the disk (observed: "Disk quota exceeded" turning Database::Create
// into a LOG(FATAL) abort, which libFuzzer reports as a bogus crash).
class ScopedDb {
 public:
  explicit ScopedDb(std::string_view prefix)
      : name_(std::string(prefix) + "-" + RandomString(8)) {
    StatusOr<std::unique_ptr<Database>> created = Database::Create(name_);
    if (created.HasValue()) {
      db_ = created.MoveValue();
    }
  }

  ScopedDb(const ScopedDb&) = delete;
  ScopedDb& operator=(const ScopedDb&) = delete;
  ScopedDb(ScopedDb&&) = delete;
  ScopedDb& operator=(ScopedDb&&) = delete;

  ~ScopedDb() {
    if (db_ != nullptr) {
      db_->DeleteAll();
    }
    db_.reset();
  }

  // Emulates a crash mid-session: discard every unflushed pool image and
  // reopen the same database files so recovery (REDO + loser UNDO) runs
  // against exactly what reached the disk.  The old Database must be torn
  // down before Create so no shutdown-time writeback resurrects images
  // that never made it out of the pool.
  void CrashAndReopen() {
    if (db_ != nullptr) {
      db_->EmulateCrash();
    }
    db_.reset();
    db_ = Database::Create(name_).MoveValue();
  }

  [[nodiscard]] Database* operator->() const { return db_.get(); }
  [[nodiscard]] Database& operator*() const { return *db_; }
  [[nodiscard]] Database* get() const { return db_.get(); }
  [[nodiscard]] explicit operator bool() const { return db_ != nullptr; }

 private:
  std::string name_;
  std::unique_ptr<Database> db_;
};

// The SQL frontend has no CREATE INDEX syntax, so fuzzers carry index specs
// as text ("CREATE [UNIQUE] INDEX n ON t(cols)[ INCLUDE(cols)];") and apply
// them through the catalog API here. Keeping the spec textual preserves the
// self-contained .test regression format.
inline bool ApplyIndexSpec(Database& db, TransactionContext& ctx,
                           std::string_view ddl) {
  IndexMode mode = IndexMode::kNonUnique;
  if (ddl.starts_with("CREATE UNIQUE INDEX ")) {
    mode = IndexMode::kUnique;
    ddl.remove_prefix(std::string_view("CREATE UNIQUE INDEX ").size());
  } else if (ddl.starts_with("CREATE INDEX ")) {
    ddl.remove_prefix(std::string_view("CREATE INDEX ").size());
  } else {
    return false;
  }
  const size_t on = ddl.find(" ON ");
  const size_t open =
      on == std::string_view::npos ? std::string_view::npos : ddl.find('(', on);
  if (on == std::string_view::npos || open == std::string_view::npos) {
    return false;
  }
  const std::string idx_name(ddl.substr(0, on));
  const std::string table(ddl.substr(on + 4, open - (on + 4)));
  auto col_names = [](std::string_view list) {
    std::vector<std::string> cols;
    size_t p = 0;
    while (p < list.size()) {
      const size_t comma = list.find(',', p);
      cols.emplace_back(list.substr(p, comma == std::string_view::npos
                                           ? std::string_view::npos
                                           : comma - p));
      if (comma == std::string_view::npos) {
        break;
      }
      p = comma + 1;
    }
    return cols;
  };
  const size_t close = ddl.find(')', open);
  if (close == std::string_view::npos) {
    return false;
  }
  std::vector<std::string> key_cols =
      col_names(ddl.substr(open + 1, close - open - 1));
  std::vector<std::string> include_cols;
  static constexpr std::string_view kIncludeKw = " INCLUDE(";
  if (const size_t inc = ddl.find(kIncludeKw, close);
      inc != std::string_view::npos) {
    const size_t inc_open = inc + kIncludeKw.size() - 1;
    const size_t inc_close = ddl.find(')', inc_open);
    if (inc_close == std::string_view::npos) {
      return false;
    }
    include_cols =
        col_names(ddl.substr(inc_open + 1, inc_close - inc_open - 1));
  }
  StatusOr<Table> tbl = db.GetTable(ctx, table);
  if (!tbl.HasValue()) {
    return false;
  }
  auto slots_of = [&tbl](const std::vector<std::string>& cols,
                         std::vector<slot_t>* slots) {
    for (const std::string& c : cols) {
      const int off = tbl.Value().GetSchema().Offset(ColumnName(c));
      if (off < 0) {
        return false;
      }
      slots->push_back(static_cast<slot_t>(off));
    }
    return true;
  };
  std::vector<slot_t> key;
  std::vector<slot_t> include;
  if (key_cols.empty() || !slots_of(key_cols, &key) ||
      !slots_of(include_cols, &include)) {
    return false;
  }
  return db.CreateIndex(ctx, table, IndexSchema(idx_name, key, include, mode))
      .ok();
}

}  // namespace tinylamb

#endif  // TINYLAMB_FUZZ_SCOPED_DB_HPP
